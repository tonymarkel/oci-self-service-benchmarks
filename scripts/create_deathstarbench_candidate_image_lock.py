#!/usr/bin/env python3
"""Bind Hotel/Media candidate publications to prepared inputs and registry bytes.

This deliberately does not use the Social Network runtime lock validator.
Published images, native build receipts, and offline inspection are not cloud,
dataset, measurement, denied-edge, cleanup, or anonymous layer-pull qualification.
All output release gates remain false. Receipts are operator/CI observations,
not cryptographically authenticated build provenance or reproducible rebuilds.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Callable, Mapping
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_workload_contract import distributed_workload_profile as get_workload_profile
from scripts.deathstarbench_artifact_common import (
    PreparationError, UPSTREAM_REPOSITORY, UPSTREAM_REVISION, sha256, tree_sha256,
)
from scripts import prepare_deathstarbench_candidate_load_driver as driver
from scripts import prepare_deathstarbench_hotel_images as hotel
from scripts import prepare_deathstarbench_media_images as media
from scripts import prepare_deathstarbench_load_driver as social_driver
from scripts.create_deathstarbench_image_lock import ImageLockError, _repository


SCHEMA = "candidate-publication-v1"
LOCK_SCHEMA = "candidate-artifact-lock-v1"
PLATFORMS = ("linux/amd64", "linux/arm64")
ARCHITECTURES = {"linux/amd64": "x86_64", "linux/arm64": "aarch64"}
CUSTOM_KEYS = {
    "hotel_reservation": {"app": "hotel-reservation"},
    "media_microservices": {"app": "media-microservices", "frontend": "nginx-web-server"},
}
SHA = re.compile(r"[0-9a-f]{64}\Z")
INDEX_TYPES = {"application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json"}
MANIFEST_TYPES = {"application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json"}
CONFIG_TYPES = {"application/vnd.oci.image.config.v1+json", "application/vnd.docker.container.image.v1+json"}


class CandidateImageLockError(RuntimeError):
    """A candidate input, receipt, or registry byte identity did not match."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CandidateImageLockError(message)


def _equal(value: Any, expected: Any, label: str) -> None:
    # bool is a subclass of int: schema_version=True must not pass as 1.
    def same(left, right):
        if type(left) is not type(right):
            return False
        if isinstance(right, dict):
            return left.keys() == right.keys() and all(same(left[key], right[key]) for key in right)
        if isinstance(right, list):
            return len(left) == len(right) and all(same(a, b) for a, b in zip(left, right, strict=True))
        return left == right
    _require(same(value, expected), f"Conflicting {label}.")


def _mapping(value: Any, label: str, *, keys: set[str] | None = None) -> dict:
    _require(isinstance(value, dict), f"{label} must be a JSON object.")
    if keys is not None:
        _require(set(value) == keys, f"{label} has incomplete or unexpected fields.")
    return value


def _hex(value: Any, label: str) -> str:
    _require(isinstance(value, str) and bool(SHA.fullmatch(value)) and value != "0" * 64,
             f"{label} must be a non-placeholder SHA-256.")
    return value


def _digest(value: Any, label: str) -> str:
    _require(isinstance(value, str) and value.startswith("sha256:"), f"Invalid {label} digest.")
    _hex(value[7:], label)
    return value


def _reference(value: Any, label: str) -> tuple[str, str]:
    _require(isinstance(value, str) and value.count("@") == 1, f"{label} must be digest-pinned.")
    repository, digest = value.split("@")
    try:
        canonical = _repository(repository, label=label)
    except ImageLockError as exc:
        raise CandidateImageLockError(str(exc)) from exc
    _require(repository == canonical and not canonical.split("/", 1)[0].endswith(".invalid"),
             f"{label} must use a canonical non-placeholder repository without a tag.")
    return repository, _digest(digest, label)


def _json_bytes(raw: bytes, label: str, *, digest: str | None = None, size: int | None = None) -> dict:
    _require(isinstance(raw, bytes), f"{label} inspection must return original bytes.")
    if digest is not None:
        _equal("sha256:" + hashlib.sha256(raw).hexdigest(), digest, f"{label} raw byte digest")
    if size is not None:
        _equal(len(raw), size, f"{label} descriptor size")
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result
    def reject_constant(value):
        raise ValueError(f"Non-JSON number: {value}")
    try:
        value = json.loads(raw, object_pairs_hook=unique_object, parse_constant=reject_constant)
    except (UnicodeError, ValueError) as exc:
        raise CandidateImageLockError(f"Invalid {label} JSON.") from exc
    return _mapping(value, label)


def _read_document(path: Path, label: str) -> dict:
    _require(path.is_file() and not path.is_symlink(), f"Missing or unsafe {label}.")
    try:
        return _json_bytes(path.read_bytes(), label)
    except OSError as exc:
        raise CandidateImageLockError(f"Cannot read {label}.") from exc


def _file(root: Path, relative: Any, label: str) -> Path:
    _require(isinstance(relative, str), f"Invalid {label} path.")
    path = PurePosixPath(relative)
    _require(relative not in ("", ".") and path.as_posix() == relative
             and not path.is_absolute() and not ({"..", ".git"} & set(path.parts))
             and "\\" not in relative and "\0" not in relative, f"Unsafe {label} path.")
    result = root / path
    for parent in (result, *result.parents):
        if parent == root:
            break
        _require(not parent.is_symlink(), f"Symlink is forbidden for {label}.")
    _require(result.exists(), f"Missing {label}.")
    return result


def _file_hash(root: Path, relative: str, expected: Any, label: str) -> str:
    expected = _hex(expected, label)
    path = _file(root, relative, label)
    _require(path.is_file(), f"{label} is not a regular file.")
    _equal(sha256(path), expected, f"{label} byte identity")
    return expected


def _tree_hash(root: Path, relative: str, expected: Any, label: str) -> str:
    expected = _hex(expected, label)
    path = _file(root, relative, label)
    _require(path.is_dir(), f"{label} is not a directory.")
    try:
        found = tree_sha256(path)
    except (PreparationError, OSError) as exc:
        raise CandidateImageLockError(f"Unsafe {label} inventory.") from exc
    _equal(found, expected, f"{label} tree identity")
    return expected


def _common_preparation(document: dict, workload_id: str, label: str) -> None:
    for key, expected in {
        "schema_version": 1, "workload_id": workload_id, "candidate_only": True,
        "released": False, "upstream_revision": UPSTREAM_REVISION,
        "upstream_repository": UPSTREAM_REPOSITORY,
    }.items():
        _equal(document.get(key), expected, f"{label} {key}")


def _workload_preparation(root: Path, workload_id: str) -> tuple[dict, dict]:
    _require(root.is_dir() and not root.is_symlink(), "Workload preparation root must be a regular directory.")
    document = _read_document(root / "context-manifest.json", "workload preparation manifest")
    _common_preparation(document, workload_id, "workload preparation")
    _equal(document.get("runtime_git_clone"), False, "runtime Git cloning")
    _equal(document.get("byte_reproducible_rebuild"), False, "rebuild qualification")
    contexts = _mapping(document.get("contexts"), "workload contexts", keys=set(CUSTOM_KEYS[workload_id]))
    identities = {}
    for name, value in contexts.items():
        context = _mapping(value, f"{name} context")
        _equal(context.get("path"), name, f"{name} context path")
        _equal(context.get("dockerfile"), "Dockerfile.candidate", f"{name} recipe path")
        identities[name] = {
            "context_sha256": _tree_hash(root, name, context.get("context_sha256"), name),
            "recipe_sha256": sha256(_file(root, f"{name}/Dockerfile.candidate", f"{name} recipe")),
            "deathstarbench_license_sha256": _file_hash(root, f"{name}/LICENSE.deathstarbench", hotel.UPSTREAM_ANCHORS["LICENSE"], f"{name} license"),
        }
        for key in ("recipe_sha256", "license_sha256"):
            if key in context:
                _equal(context[key], identities[name]["recipe_sha256" if key == "recipe_sha256" else "deathstarbench_license_sha256"], f"{name} {key}")
    if workload_id == "hotel_reservation":
        for key, expected in {"patch_revision": hotel.PATCH_REVISION, "recipe_revision": hotel.RECIPE_REVISION,
                              "dataset_revision": hotel.DATASET_REVISION, "native_platforms": list(PLATFORMS),
                              "runtime_build_tools": False, "runtime_demo_private_keys": False,
                              "build_network_required": False}.items():
            _equal(document.get(key), expected, f"Hotel {key}")
        inventory = document.get("source_inventory")
        _require(isinstance(inventory, list) and bool(inventory), "Missing Hotel source inventory.")
        names = []
        for item in inventory:
            item = _mapping(item, "Hotel inventory entry", keys={"path", "sha256"})
            _require(isinstance(item["path"], str), "Invalid Hotel source path.")
            path = PurePosixPath(item["path"])
            _require(path.as_posix() == item["path"] and not path.is_absolute()
                     and not ({"..", ".git"} & set(path.parts)) and "\\" not in item["path"]
                     and "\0" not in item["path"]
                     and (item["path"] == "LICENSE" or item["path"].startswith("hotelReservation/")),
                     "Unsafe Hotel source inventory path.")
            names.append(item["path"])
            _hex(item["sha256"], "Hotel original source")
        _equal(names, sorted(set(names)), "Hotel source inventory ordering")
        inventory_digest = hashlib.sha256(json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        _equal(document.get("source_inventory_sha256"), inventory_digest, "Hotel source inventory hash")
        original = {item["path"]: item["sha256"] for item in inventory}
        for path, expected in hotel.UPSTREAM_ANCHORS.items():
            _equal(original.get(path), expected, f"Hotel source anchor {path}")
        patches = _mapping(document.get("source_patches"), "Hotel source patches", keys={
            "hotelReservation/cmd/user/db.go", "hotelReservation/services/frontend/server.go", "hotelReservation/services/reservation/server.go"})
        for path, digest in original.items():
            relative = path.removeprefix("hotelReservation/")
            copied = path.startswith("hotelReservation/") and (
                relative in hotel.APP_FILES or relative.split("/", 1)[0] in hotel.APP_DIRECTORIES)
            if copied and path not in patches:
                _file_hash(root, "app/" + relative, digest, "Hotel unchanged source " + relative)
        for path, receipt in patches.items():
            receipt = _mapping(receipt, "Hotel source patch", keys={"original_sha256", "patched_sha256"})
            _equal(receipt["original_sha256"], hotel.UPSTREAM_ANCHORS[path], f"Hotel original patch {path}")
            _file_hash(root, "app/" + path.removeprefix("hotelReservation/"), receipt["patched_sha256"], "Hotel patched source")
        fixtures = _mapping(document.get("semantic_test_fixtures"), "Hotel semantic fixtures", keys={
            "services/reservation/candidate_availability_test.go", "services/frontend/candidate_resolver_test.go", "cmd/user/candidate_seed_test.go"})
        for asset, path in (("availability_test.go", "services/reservation/candidate_availability_test.go"),
                            ("frontend_resolver_test.go", "services/frontend/candidate_resolver_test.go"),
                            ("user_seed_test.go", "cmd/user/candidate_seed_test.go")):
            _equal(fixtures[path], sha256(hotel.ASSET_ROOT / asset), "Hotel semantic test source")
            _file_hash(root, "app/" + path, fixtures[path], "Hotel semantic test fixture")
        _equal(document.get("correction_asset_sha256"), sha256(hotel.ASSET_ROOT / "availability.go"), "Hotel correction asset")
        _equal((_file(root, "app/Dockerfile.candidate", "Hotel recipe")).read_bytes(), hotel._dockerfile().encode("utf-8"), "Hotel generated recipe")
        software = _mapping(document.get("software"), "Hotel software")
        for key, expected in {"go_version": hotel.GO_VERSION, "builder": hotel.GO_BUILDER,
                              "builder_platforms": hotel.GO_PLATFORMS, "runtime_base": "scratch"}.items():
            _equal(software.get(key), expected, f"Hotel software {key}")
        for relative in ("go.mod", "go.sum"):
            _file_hash(root, "app/" + relative, software.get(relative.replace(".", "_") + "_sha256"), relative)
        _tree_hash(root, "app/vendor", software.get("vendor_inventory_sha256"), "Hotel vendor")
        _file_hash(root, "app/config.json", document.get("config_sha256"), "Hotel config")
        licenses = _mapping(document.get("license_identities"), "Hotel licenses")
        _equal(licenses.get("deathstarbench_license_sha256"), hotel.UPSTREAM_ANCHORS["LICENSE"], "Hotel upstream license")
        _equal(licenses.get("go_license_from_immutable_builder"), True, "Hotel Go license source")
        vendors = licenses.get("vendored_licenses")
        _require(isinstance(vendors, list) and bool(vendors), "Missing Hotel vendor licenses.")
        seen = set()
        for entry in vendors:
            entry = _mapping(entry, "Hotel vendor license", keys={"path", "sha256"})
            _require(isinstance(entry["path"], str) and entry["path"] not in seen, "Duplicate vendor license.")
            seen.add(entry["path"])
            for subtree in ("app/vendor/", "app/licenses/vendor/"):
                _file_hash(root, subtree + entry["path"], entry["sha256"], "Hotel vendor license")
        expected_vendors = {
            path.relative_to(root / "app/vendor").as_posix()
            for path in (root / "app/vendor").rglob("*")
            if path.is_file() and path.name.upper().startswith(("LICENSE", "LICENCE", "COPYING", "NOTICE", "COPYRIGHT"))
        }
        _require(seen == expected_vendors, "Incomplete Hotel vendored license inventory.")
    else:
        _equal(document.get("preparation_revision"), media.PREPARATION_REVISION, "Media preparation revision")
        _equal(document.get("namespace"), media.NAMESPACE, "Media namespace")
        _equal(document.get("original_anchor_sha256"), media.UPSTREAM_ANCHORS, "Media original anchors")
        _hex(document.get("original_source_tree_sha256"), "Media original source tree")
        _equal(document.get("runtime_source_baked_into_images"), True, "Media baked source")
        for key, expected in {"immutable_base_images_verified": True, "build_source_archives_checksum_pinned": True,
                              "build_network_required": True, "native_build_required": True, "portable_cpu_build": True,
                              "frontend_runtime_writable_paths": ["/tmp"],
                              "base_images": {"ubuntu_xenial": {"reference": media.UBUNTU_BASE, "platform_digests": dict(media.UBUNTU_PLATFORM_DIGESTS)}}}.items():
            _equal(document.get(key), expected, f"Media software {key}")
        for name, context in contexts.items():
            _file_hash(root, f"{name}/build/source-lock.json", context.get("build_source_lock_sha256"), f"Media {name} source lock")
            _tree_hash(root, f"{name}/build", context.get("build_inputs_sha256"), f"Media {name} build inputs")
            source_lock = _read_document(root / name / "build/source-lock.json", "Media build source lock")
            _equal(source_lock, {"schema_version": 1, "candidate_only": True,
                                 "sources": {key: {"url": value[0], "sha256": value[1]} for key, value in media.BUILD_SOURCES.items()}},
                   "Media build source lock contents")
            for asset in ("build-dependencies.sh", "build-openresty.sh"):
                _file_hash(root, f"{name}/build/{asset}", sha256(media.BUILD_ASSETS / asset), f"Media {name} approved build asset")
            expected_env = "".join(f"{key.upper()}_URL='{url}'\n{key.upper()}_SHA256='{digest}'\n"
                                   for key, (url, digest) in sorted(media.BUILD_SOURCES.items()))
            _equal((_file(root, f"{name}/build/source.env", "Media build environment")).read_bytes(), expected_env.encode("utf-8"),
                   "Media generated build environment")
            identities[name]["build_source_lock_sha256"] = context["build_source_lock_sha256"]
            identities[name]["build_inputs_sha256"] = context["build_inputs_sha256"]
        receipts = _mapping(document.get("prepared_source_receipts"), "Media source receipts", keys=set(media.PATCHED_SOURCE_PATHS))
        for name, (original, prepared) in media.PATCHED_SOURCE_PATHS.items():
            receipt = _mapping(receipts[name], "Media patch receipt", keys={"original_path", "original_sha256", "prepared_path", "prepared_sha256"})
            _equal(receipt["original_path"], "mediaMicroservices/" + original, f"Media original {name}")
            _equal(receipt["original_sha256"], media.UPSTREAM_ANCHORS[receipt["original_path"]], f"Media original {name} SHA")
            _equal(receipt["prepared_path"], prepared, f"Media prepared {name}")
            _file_hash(root, prepared, receipt["prepared_sha256"], f"Media patched {name}")
        initializer = _mapping(document.get("initializer"), "Media initializer")
        for key, expected in {"path": "initializer", "revision": media.INITIALIZER_REVISION,
                              "executed": False, "dataset_ready": False, "requires_journal_and_semantic_readiness": True}.items():
            _equal(initializer.get(key), expected, f"Media initializer {key}")
        _tree_hash(root, "initializer", initializer.get("context_sha256"), "Media initializer")
        _file_hash(root, "initializer/LICENSE.deathstarbench", initializer.get("license_sha256"), "Media initializer license")
        _equal(initializer.get("license_sha256"), hotel.UPSTREAM_ANCHORS["LICENSE"], "Media initializer upstream license")
        datasets = _mapping(initializer.get("datasets"), "Media dataset identities", keys={"casts.json", "movies.json"})
        for name, digest in datasets.items():
            _equal(digest, media.UPSTREAM_ANCHORS["mediaMicroservices/datasets/tmdb/" + name], f"Media dataset {name} source")
            _file_hash(root, "initializer/datasets/tmdb/" + name, digest, f"Media dataset {name}")
    return document, identities


def _driver_preparation(root: Path, workload_id: str) -> tuple[dict, dict]:
    _require(root.is_dir() and not root.is_symlink(), "Driver preparation root must be a regular directory.")
    document = _read_document(root / "context-manifest.json", "driver preparation manifest")
    _common_preparation(document, workload_id, "driver preparation")
    profile = get_workload_profile(workload_id)
    for key, expected in {
        "candidate_schema": "candidate-driver-v1", "measurement_qualified": False,
        "load_driver_revision": profile.load_driver_revision, "measurement_revision": profile.measurement_revision,
        "measurement_method": driver.MEASUREMENT_METHOD, "architecture": "x86_64", "platform": "linux/amd64",
        "base_image": social_driver.ROCKY_LINUX_9_MINIMAL_AMD64, "context_path": "context",
        "wrk2_tree_git_sha": social_driver.WRK2_TREE_GIT_SHA, "wrk2_source_hash_method": "legacy-social-tree-v1",
        "luajit_revision": social_driver.LUAJIT_REVISION, "luasocket_source_url": social_driver.LUASOCKET_URL,
        "luasocket_source_sha256": social_driver.LUASOCKET_SOURCE_SHA256,
        "original_request_script_path": profile.request_script_path,
        "original_request_script_sha256": profile.request_script_sha256,
        "seed_base": driver.SEED_BASE, "seed_method": driver.SEED_METHOD,
        "frontend_node_port": 8080, "prepared_target_anchor_count": 1,
        "runtime_source_network_required": False, "attestation_network_required": False,
        "benchmark_private_network_required": True, "runtime_build_tools": False,
        "runtime_user": "65532:65532", "runtime_read_only": True, "runtime_drop_capabilities": ["ALL"],
        "runtime_no_new_privileges": True, "runtime_tmpfs": "/tmp:rw,noexec,nosuid,nodev,size=64m",
    }.items():
        _equal(document.get(key), expected, f"driver {key}")
    _tree_hash(root, "context", document.get("context_sha256"), "driver context")
    for relative, field in (("original-request.lua", "original_request_script_sha256"), ("request.lua", "prepared_request_script_sha256"),
                            ("metrics.lua", "metrics_script_sha256"), ("entrypoint", "entrypoint_sha256"),
                            (social_driver.LUASOCKET_FILENAME, "luasocket_source_sha256")):
        _file_hash(root, "context/" + relative, document.get(field), "driver " + relative)
    try:
        request = (_file(root, "context/original-request.lua", "original driver script")).read_text(encoding="utf-8")
        prepared = driver.prepare_request_script(workload_id, request)
        metrics = (f'local candidate_workload = "{workload_id}"\n'
                   f'local candidate_seed_base = {driver.SEED_BASE}\n'
                   + (driver.ASSET_ROOT / "candidate-metrics.lua").read_text(encoding="utf-8"))
        _equal((_file(root, "context/request.lua", "prepared driver script")).read_bytes(), prepared.encode("utf-8"), "generated request script")
        _equal((_file(root, "context/metrics.lua", "driver metrics")).read_bytes(), metrics.encode("utf-8"), "generated metrics script")
        _equal((_file(root, "context/entrypoint", "driver entrypoint")).read_bytes(), (driver.ASSET_ROOT / "candidate-entrypoint").read_bytes(), "generated driver entrypoint")
        recipe = driver._dockerfile(profile, document["prepared_request_script_sha256"], document["metrics_script_sha256"], document["entrypoint_sha256"])
        _equal((_file(root, "context/Dockerfile", "driver recipe")).read_bytes(), recipe.encode("utf-8"), "generated driver recipe")
    except (OSError, UnicodeError, PreparationError) as exc:
        raise CandidateImageLockError("Driver preparation cannot be reproduced from its audited request/metrics/entrypoint assets.") from exc
    _equal(social_driver._tree_sha256(_file(root, "context/wrk2", "wrk2 source")), document.get("wrk2_source_sha256"), "driver wrk2 source")
    _hex(document.get("wrk2_source_sha256"), "driver wrk2 source")
    _tree_hash(root, "context/wrk2/deps/luajit", document.get("luajit_source_sha256"), "driver LuaJIT source")
    licenses = _mapping(document.get("licenses_sha256"), "driver licenses", keys={"DeathStarBench-LICENSE", "wrk2-LICENSE", "LuaJIT-COPYRIGHT", "LuaSocket-LICENSE"})
    for name, digest in licenses.items():
        _file_hash(root, "context/licenses/" + name, digest, "driver license " + name)
    _equal(licenses["DeathStarBench-LICENSE"], hotel.UPSTREAM_ANCHORS["LICENSE"], "driver DeathStarBench license")
    _equal(licenses["LuaSocket-LICENSE"], social_driver.LUASOCKET_LICENSE_SHA256, "driver LuaSocket license")
    _file_hash(root, "context/wrk2/LICENSE", licenses["wrk2-LICENSE"], "driver original wrk2 license")
    _file_hash(root, "context/wrk2/deps/luajit/COPYRIGHT", licenses["LuaJIT-COPYRIGHT"], "driver original LuaJIT license")
    return document, {"context_sha256": document["context_sha256"],
                      "recipe_sha256": sha256(_file(root, "context/Dockerfile", "driver recipe")),
                      "licenses_sha256": dict(licenses)}


ATTESTATION_FIELDS = (
    "schema", "measurement_qualified", "workload", "architecture", "platform", "revision",
    "upstream_revision", "context_sha256", "wrk_binary_sha256", "wrk2_source_sha256", "luajit_revision",
    "luasocket_source_sha256", "original_request_script_sha256", "request_script_sha256",
    "metrics_script_sha256", "entrypoint_sha256", "seed_base",
)


def parse_load_driver_attestation(value: str, context_manifest: Mapping[str, Any]) -> dict[str, str]:
    """Bind one exact candidate marker; a Social marker cannot qualify a candidate."""
    _require(isinstance(value, str), "Driver attestation must be text.")
    lines = value.splitlines()
    _require(len(lines) == 1 and lines[0].startswith("CANDIDATE_DSB_LOAD_DRIVER_ARTIFACT "), "Expected exactly one candidate driver marker.")
    tokens = lines[0].split(" ")
    _require(len(tokens) == len(ATTESTATION_FIELDS) + 1, "Unexpected driver attestation fields.")
    found = {}
    for name, token in zip(ATTESTATION_FIELDS, tokens[1:], strict=True):
        key, separator, item = token.partition("=")
        _require(key == name and separator == "=" and bool(item), "Malformed or reordered driver attestation.")
        found[name] = item
    expected = {
        "schema": "candidate-driver-v1", "measurement_qualified": "0", "workload": context_manifest["workload_id"],
        "architecture": "x86_64", "platform": "linux/amd64", "revision": context_manifest["load_driver_revision"],
        "upstream_revision": UPSTREAM_REVISION, "context_sha256": context_manifest["context_sha256"],
        "wrk2_source_sha256": context_manifest["wrk2_source_sha256"], "luajit_revision": social_driver.LUAJIT_REVISION,
        "luasocket_source_sha256": social_driver.LUASOCKET_SOURCE_SHA256,
        "original_request_script_sha256": context_manifest["original_request_script_sha256"],
        "request_script_sha256": context_manifest["prepared_request_script_sha256"],
        "metrics_script_sha256": context_manifest["metrics_script_sha256"], "entrypoint_sha256": context_manifest["entrypoint_sha256"],
        "seed_base": str(driver.SEED_BASE),
    }
    for name, item in expected.items():
        _equal(found[name], item, f"driver attestation {name}")
    _hex(found["wrk_binary_sha256"], "attested wrk2 binary")
    return found


def _descriptor(value: Any, label: str, media_types: set[str]) -> dict:
    value = _mapping(value, label)
    _require(value.get("mediaType") in media_types, f"Unexpected {label} media type.")
    _digest(value.get("digest"), label)
    _require(type(value.get("size")) is int and value["size"] > 0, f"Invalid {label} size.")
    return value


def verify_image_index(index_image: str, platform_images: Mapping[str, str], *,
                       inspector: Callable[[str], bytes], blob_inspector: Callable[[str, str], bytes],
                       allow_other_platforms: bool = False) -> dict:
    """Verify original index/manifest/config bytes and native config architecture."""
    repository, digest = _reference(index_image, "published index")
    index = _json_bytes(inspector(index_image), "published index", digest=digest)
    _equal(index.get("schemaVersion"), 2, "published index schema")
    _require(index.get("mediaType") in INDEX_TYPES and isinstance(index.get("manifests"), list), "Expected an OCI/Docker image index.")
    descriptors = {}
    for item in index["manifests"]:
        item = _descriptor(item, "index manifest", MANIFEST_TYPES)
        platform = _mapping(item.get("platform"), "index platform")
        key = f"{platform.get('os')}/{platform.get('architecture')}"
        if key == "unknown/unknown":
            annotations = _mapping(item.get("annotations"), "attestation annotations")
            _equal(annotations.get("vnd.docker.reference.type"), "attestation-manifest", "non-runtime index attestation")
            continue
        if key not in platform_images and allow_other_platforms:
            continue
        _require(key in platform_images and key not in descriptors, "Unexpected or duplicate runtime platform.")
        _require(platform.get("variant") in (None, "v8") if key == "linux/arm64" else platform.get("variant") is None,
                 "Unsupported index architecture variant.")
        descriptors[key] = item
    _require(set(descriptors) == set(platform_images), "Missing runtime platform manifests.")
    _require(len({value["digest"] for value in descriptors.values()}) == len(descriptors), "Platforms must not share one manifest digest.")
    result = {}
    for platform, reference in platform_images.items():
        item = descriptors[platform]
        _equal(reference, repository + "@" + item["digest"], f"{platform} selected manifest")
        manifest = _json_bytes(inspector(reference), platform + " manifest", digest=item["digest"], size=item["size"])
        _equal(manifest.get("schemaVersion"), 2, "platform manifest schema")
        _require(manifest.get("mediaType") in MANIFEST_TYPES and "manifests" not in manifest, "Selected image is not a platform manifest.")
        config = _descriptor(manifest.get("config"), "image config", CONFIG_TYPES)
        _require(isinstance(manifest.get("layers"), list) and bool(manifest["layers"]), "Platform image layers are missing.")
        for layer in manifest["layers"]:
            layer = _mapping(layer, "image layer")
            _digest(layer.get("digest"), "image layer")
            _require(type(layer.get("size")) is int and layer["size"] > 0, "Invalid layer size.")
        image_config = _json_bytes(blob_inspector(repository, config["digest"]), "image config", digest=config["digest"], size=config["size"])
        _equal(image_config.get("os"), "linux", f"{platform} config OS")
        _equal(image_config.get("architecture"), platform.split("/")[1], f"{platform} config architecture")
        result[platform] = {"image": reference, "manifest_digest": item["digest"], "config_digest": config["digest"],
                            "os": "linux", "architecture": image_config["architecture"]}
    return {"index_image": index_image, "platforms": result}


def _build_receipt(receipt: Any, workload_id: str, context_name: str, platform: str, image: dict, identity: dict,
                   inspector: Callable[[str], bytes], blob_inspector: Callable[[str, str], bytes]) -> dict:
    receipt = _mapping(receipt, "native build receipt", keys={"schema_version", "workload_id", "context_name", "platform", "context_sha256", "recipe_sha256", "runner_architecture", "qemu_used", "published_index_image"})
    for key, expected in {"schema_version": 1, "workload_id": workload_id, "context_name": context_name,
                          "platform": platform, "context_sha256": identity["context_sha256"], "recipe_sha256": identity["recipe_sha256"],
                          "runner_architecture": ARCHITECTURES[platform], "qemu_used": False}.items():
        _equal(receipt.get(key), expected, f"native build {key}")
    original = verify_image_index(receipt["published_index_image"], {platform: image["image"]},
                                  inspector=inspector, blob_inspector=blob_inspector)
    _equal(original["platforms"][platform]["config_digest"], image["config_digest"], "native build platform config")
    return dict(receipt)


def _artifact_receipt(receipt: dict, workload_id: str, runtime_key: str, platform: str, context: Path) -> None:
    from scripts.smoke_deathstarbench_candidate_images import _baked_files, HOTEL_PROGRAMS, MEDIA_PROGRAMS, MEDIA_DEPENDENCY_LICENSES, inspection_plan
    artifacts = _mapping(receipt.get("artifact_identities"), "smoke artifact identities", keys={
        "baked_file_sha256", "dependency_license_sha256", "elf_artifacts", "required_executables", "runtime_clone_detected", "private_key_detected"})
    for key in ("runtime_clone_detected", "private_key_detected"):
        _equal(artifacts.get(key), False, f"smoke artifact {key}")
    try:
        expected_files = _baked_files(workload_id, runtime_key, context)
    except PreparationError as exc:
        raise CandidateImageLockError("Smoke files cannot bind the prepared context.") from exc
    files = _mapping(artifacts.get("baked_file_sha256"), "smoke baked files")
    keys = set(expected_files) | ({"/usr/share/licenses/go/LICENSE"} if runtime_key == "hotel-reservation" else set())
    _require(set(files) == keys, "Smoke baked-file inventory is incomplete or unexpected.")
    for name, path in expected_files.items():
        _equal(_hex(files[name], "baked file"), sha256(path), f"smoke baked file {name}")
    if runtime_key == "hotel-reservation":
        _hex(files["/usr/share/licenses/go/LICENSE"], "in-image Go license")
    # Notices copied from verified build archives are in-image identities, not
    # context-file equality proofs. Keep them separate from source-baked files.
    notices = _mapping(artifacts.get("dependency_license_sha256"), "in-image dependency notices")
    expected_dependencies = set(MEDIA_DEPENDENCY_LICENSES.get(runtime_key, ()))
    observed_dependencies = set()
    prefix = "/usr/share/licenses/deathstarbench/dependencies/"
    for path, digest in notices.items():
        _require(isinstance(path, str) and path.startswith(prefix) and PurePosixPath(path).as_posix() == path
                 and ".." not in PurePosixPath(path).parts and "\\" not in path and "\0" not in path,
                 "Invalid dependency notice path.")
        suffix = path.removeprefix(prefix)
        _require("/" in suffix and bool(suffix.split("/", 1)[1]), "Missing dependency notice filename.")
        observed_dependencies.add(suffix.split("/", 1)[0])
        _hex(digest, "in-image dependency notice")
    _require(observed_dependencies == expected_dependencies, "Incomplete or unexpected in-image dependency notice inventory.")
    expected_required = (["/go/bin/" + name for name in HOTEL_PROGRAMS] if runtime_key == "hotel-reservation" else
                         ["/usr/local/bin/" + name for name in MEDIA_PROGRAMS] if runtime_key == "media-microservices" else
                         ["/usr/local/openresty/nginx/sbin/nginx"] if runtime_key == "nginx-web-server" else
                         ["/opt/deathstarbench-candidate-driver/bin/wrk"])
    _equal(artifacts.get("required_executables"), expected_required, "smoke required executable inventory")
    elves = _mapping(artifacts.get("elf_artifacts"), "smoke ELF artifacts")
    _require(bool(elves), "Smoke ELF artifact inventory is missing.")
    for path, elf in elves.items():
        _require(isinstance(path, str) and path.startswith("/") and PurePosixPath(path).as_posix() == path
                 and ".." not in PurePosixPath(path).parts and "\\" not in path and "\0" not in path,
                 "Invalid in-image ELF path.")
        elf = _mapping(elf, "smoke ELF identity", keys={"sha256", "elf_class", "elf_type", "machine", "architecture", "interpreter", "dynamic_dependencies", "runnable", "static"})
        _hex(elf["sha256"], "smoke ELF bytes")
        for key, expected in {"elf_class": 64, "machine": 62 if platform == "linux/amd64" else 183,
                              "architecture": ARCHITECTURES[platform]}.items():
            _equal(elf[key], expected, f"smoke ELF {key}")
        _require(type(elf["elf_type"]) is int and elf["elf_type"] in (1, 2, 3), "Invalid ELF object type.")
        _require(elf["interpreter"] is None or (isinstance(elf["interpreter"], str) and elf["interpreter"].startswith("/")), "Invalid ELF interpreter.")
        for key in ("dynamic_dependencies", "runnable", "static"):
            _require(type(elf[key]) is bool, f"Invalid ELF {key}.")
        _equal(elf["runnable"], elf["elf_type"] in (2, 3), "ELF runnable state")
        _equal(elf["static"], elf["runnable"] and elf["interpreter"] is None and not elf["dynamic_dependencies"], "ELF static state")
    for path in expected_required:
        _require(path in elves and elves[path]["runnable"], "Smoke required executable is missing/not runnable.")
        if runtime_key == "hotel-reservation":
            _equal(elves[path]["static"], True, "scratch Hotel executable linkage")
    methods = set(receipt["methods"])
    expected_methods = {"stopped-container-export-no-host-extraction", "all-ELF64-native-architecture",
                        "baked-config-script-license-hashes", "runtime-clone-and-private-key-screen"}
    if runtime_key == "hotel-reservation":
        expected_methods.add("scratch-static-linkage-no-service-startup")
    elif any(elf["dynamic_dependencies"] for elf in elves.values()):
        expected_methods.add("native-hardened-ldd-all-dynamic-ELFs")
    if runtime_key == "nginx-web-server":
        expected_methods.update({"baked-nginx-config-syntax-with-explicit-mock-DNS", "baked-Lua-Thrift-module-loading-no-service-methods"})
    if runtime_key == "load-driver":
        expected_methods.add("candidate-driver-offline-hardened-attestation")
    _require(expected_methods <= methods, "Missing required native image smoke methods.")
    executions = receipt.get("inspection_executions")
    _require(isinstance(executions, list), "Missing image inspection execution receipts.")
    for execution in executions:
        execution = _mapping(execution, "image inspection execution", keys={"command", "stdout_sha256", "stdout", "mock_dns"})
        command = execution["command"]
        _require(isinstance(command, list) and bool(command) and all(isinstance(arg, str) and bool(arg) for arg in command), "Invalid inspection command receipt.")
        _require(isinstance(execution["stdout"], str), "Inspection stdout must be text.")
        _equal(_hex(execution["stdout_sha256"], "inspection stdout"), hashlib.sha256(execution["stdout"].encode()).hexdigest(), "inspection stdout bytes")
        _mapping(execution["mock_dns"], "inspection mock DNS")
    plan = inspection_plan(workload_id, runtime_key, context, artifacts)
    _require(len(executions) == len(plan), "Missing or unexpected required inspection execution receipts.")
    for execution, (command, hosts) in zip(executions, plan, strict=True):
        _equal(execution["command"], command, "required native inspection command")
        _equal(execution["mock_dns"], {host: "127.0.0.1" for host in hosts}, "required inspection mock DNS")
    if runtime_key == "load-driver":
        required_command = ["/opt/deathstarbench-candidate-driver/bin/entrypoint", "attest"]
        matching = [execution for execution in executions if execution["command"] == required_command]
        _require(len(matching) == 1, "Exactly one executed driver attestation receipt is required.")
        _equal(matching[0]["stdout"], receipt.get("load_driver_attestation"), "driver attestation execution stdout")
    else:
        _equal(receipt.get("load_driver_attestation"), None, "non-driver attestation")


def _smoke_receipt(receipt: Any, workload_id: str, runtime_key: str, platform: str, image: dict, identity: dict, context: Path) -> dict:
    receipt = _mapping(receipt, "candidate image smoke receipt")
    for key, expected in {"schema_version": 1, "candidate_schema": "candidate-image-smoke-v1", "workload_id": workload_id,
                          "image_key": runtime_key, "image": image["image"], "platform": platform,
                          "architecture": ARCHITECTURES[platform], "config_digest": image["config_digest"],
                          "context_sha256": identity["context_sha256"], "candidate_only": True, "released": False,
                          "runtime_semantics_qualified": False}.items():
        _equal(receipt.get(key), expected, f"image smoke {key}")
    controls = _mapping(receipt.get("smoke_execution"), "smoke execution")
    for key, expected in {"passed": True, "network": "none", "read_only": True, "user": "65532:65532",
                          "drop_capabilities": ["ALL"], "no_new_privileges": True}.items():
        _equal(controls.get(key), expected, f"smoke execution {key}")
    methods = receipt.get("methods")
    _require(isinstance(methods, list) and bool(methods) and all(isinstance(item, str) and bool(item) for item in methods), "Missing smoke methods.")
    native = _mapping(receipt.get("native_builder"), "smoke native builder", keys={"platform", "architecture", "qemu_used"})
    for key, expected in {"platform": platform, "architecture": ARCHITECTURES[platform], "qemu_used": False}.items():
        _equal(native.get(key), expected, f"smoke native builder {key}")
    config = _mapping(receipt.get("image_config"), "smoke config", keys={"os", "architecture", "config_digest"})
    for key, expected in {"os": "linux", "architecture": platform.split("/")[1], "config_digest": image["config_digest"]}.items():
        _equal(config.get(key), expected, f"smoke image config {key}")
    _artifact_receipt(receipt, workload_id, runtime_key, platform, context)
    return dict(receipt)


def _driver_binary_attestation(receipt: dict, driver_manifest: dict) -> dict:
    attestation = parse_load_driver_attestation(receipt.get("load_driver_attestation"), driver_manifest)
    identity = receipt["artifact_identities"]["elf_artifacts"]["/opt/deathstarbench-candidate-driver/bin/wrk"]
    _equal(attestation["wrk_binary_sha256"], identity["sha256"], "driver attestation inspected binary hash")
    return attestation


def preflight_native_candidate(workload_id: str, workload_preparation: Path, driver_preparation: Path,
                               records: list[dict], *, inspector: Callable[[str], bytes],
                               blob_inspector: Callable[[str, str], bytes]) -> dict:
    """Read-only full native evidence verification, before assembling/pushing indexes."""
    _require(workload_id in CUSTOM_KEYS, "Only Hotel/Media candidates are supported.")
    # Per-call caches are safe for content-addressed inputs, reduce registry
    # traffic, and never retain authentication/state across parallel workloads.
    inspector = lru_cache(maxsize=128)(inspector)
    blob_inspector = lru_cache(maxsize=128)(blob_inspector)
    _, contexts = _workload_preparation(workload_preparation, workload_id)
    driver_manifest, driver_identity = _driver_preparation(driver_preparation, workload_id)
    _require(isinstance(records, list) and len(records) == 2, "Both native platform records are required.")
    platforms = {}
    for record in records:
        record = _mapping(record, "native publication record", keys={"schema_version", "candidate_schema", "workload_id", "platform", "images", "workload_preparation_manifest_sha256", "driver_preparation_manifest_sha256"})
        platform = record["platform"]
        _require(isinstance(platform, str) and platform in PLATFORMS and platform not in platforms, "Duplicate or unsupported native record platform.")
        for key, expected in {"schema_version": 1, "candidate_schema": "candidate-native-publication-v1", "workload_id": workload_id,
                              "workload_preparation_manifest_sha256": sha256(workload_preparation / "context-manifest.json"),
                              "driver_preparation_manifest_sha256": sha256(driver_preparation / "context-manifest.json")}.items():
            _equal(record.get(key), expected, f"native publication {key}")
        expected_keys = set(contexts) | ({"load_driver"} if platform == "linux/amd64" else set())
        images = _mapping(record["images"], "native publication images", keys=expected_keys)
        verified_images = {}
        for name, supplied in images.items():
            supplied = _mapping(supplied, "native image receipt", keys={"image", "build_receipt", "smoke_receipt"})
            build = _mapping(supplied["build_receipt"], "native build receipt")
            image = verify_image_index(build.get("published_index_image"), {platform: supplied["image"]}, inspector=inspector, blob_inspector=blob_inspector)["platforms"][platform]
            identity = driver_identity if name == "load_driver" else contexts[name]
            key = "load-driver" if name == "load_driver" else CUSTOM_KEYS[workload_id][name]
            context = driver_preparation / "context" if name == "load_driver" else workload_preparation / name
            image["native_build_receipt"] = _build_receipt(build, workload_id, name, platform, image, identity, inspector, blob_inspector)
            image["smoke_receipt"] = _smoke_receipt(supplied["smoke_receipt"], workload_id, key, platform, image, identity, context)
            if name == "load_driver":
                image["driver_attestation"] = _driver_binary_attestation(image["smoke_receipt"], driver_manifest)
            verified_images[name] = image
        platforms[platform] = verified_images
    return platforms


def validate_support_evidence(workload_id: str, evidence: dict, *, inspector: Callable[[str], bytes],
                              blob_inspector: Callable[[str, str], bytes]) -> dict:
    """Read-only backing-image evidence preflight; tags identify candidates only.

    The fixed requested candidate tag must currently resolve to its exact
    recorded index. Config metadata is not an executed binary-version proof.
    Only amd64 is required for the fixed database/cache/control support roles.
    """
    _require(workload_id in CUSTOM_KEYS, "Only Hotel/Media candidates are supported.")
    from scripts.inspect_deathstarbench_support_images import SUPPORT_IMAGES, VERSION_ENV
    profile = get_workload_profile(workload_id)
    keys = set(profile.required_image_keys) - set(CUSTOM_KEYS[workload_id].values())
    supplied = _mapping(evidence, "support images", keys=keys)
    inspector = lru_cache(maxsize=128)(inspector)
    blob_inspector = lru_cache(maxsize=128)(blob_inspector)
    images = {}
    for key, value in supplied.items():
        value = _mapping(value, f"support {key}", keys={"requested_image", "index_image", "platforms"})
        _equal(value["requested_image"], SUPPORT_IMAGES[workload_id][key], f"support {key} candidate source")
        repository, digest = _reference(value["index_image"], f"support {key} index")
        _equal(repository, _repository(value["requested_image"], label=key), f"support {key} repository")
        _json_bytes(inspector(value["requested_image"]), f"support {key} candidate source", digest=digest)
        platforms = _mapping(value["platforms"], f"support {key} platforms", keys={"linux/amd64"})
        for item in platforms.values():
            _mapping(item, f"support {key} platform", keys={"image"})
        verified = verify_image_index(value["index_image"], {platform: item["image"] for platform, item in platforms.items()},
                                      inspector=inspector, blob_inspector=blob_inspector, allow_other_platforms=True)
        config = _json_bytes(blob_inspector(repository, verified["platforms"]["linux/amd64"]["config_digest"]), "support version metadata")
        metadata = _mapping(config.get("config", {}), "support image config")
        environment = {}
        env_entries = metadata.get("Env")
        _require(env_entries is None or isinstance(env_entries, list), "Invalid support config environment list.")
        for entry in env_entries or []:
            _require(isinstance(entry, str) and "=" in entry, "Invalid support config environment.")
            name, value_env = entry.split("=", 1)
            _require(name not in environment, "Duplicate support config environment variable.")
            environment[name] = value_env
        version = environment.get(VERSION_ENV.get(key))
        source = "image-config-environment" if version is not None else None
        labels = metadata.get("Labels")
        labels = {} if labels is None else _mapping(labels, "support config labels")
        if version is None and "org.opencontainers.image.version" in labels:
            _require(isinstance(labels["org.opencontainers.image.version"], str), "Invalid support version label.")
            version = labels["org.opencontainers.image.version"].removeprefix("v")
            source = "image-config-label"
        candidate_version = SUPPORT_IMAGES[workload_id][key].rsplit(":", 1)[1]
        if version is not None:
            _equal(version, candidate_version, f"support {key} observed version metadata")
        images[key] = {**verified, "requested_image": SUPPORT_IMAGES[workload_id][key], "candidate_version": candidate_version,
                       "observed_version_metadata": version, "version_metadata_source": source,
                       "binary_version_executed": False, "native_build_claimed": False, "runtime_qualified": False}
    return images


def create_candidate_lock(workload_id: str, workload_preparation: Path, driver_preparation: Path,
                          publication_evidence: dict, *, inspector: Callable[[str], bytes],
                          blob_inspector: Callable[[str, str], bytes]) -> dict:
    """Produce an unreleased artifact receipt, never a deployable public lock."""
    _require(workload_id in CUSTOM_KEYS, "Only Hotel/Media candidates are supported.")
    inspector = lru_cache(maxsize=128)(inspector)
    blob_inspector = lru_cache(maxsize=128)(blob_inspector)
    profile = get_workload_profile(workload_id)
    workload, contexts = _workload_preparation(workload_preparation, workload_id)
    driver_manifest, driver_identity = _driver_preparation(driver_preparation, workload_id)
    evidence = _mapping(publication_evidence, "publication evidence", keys={
        "schema_version", "candidate_schema", "workload_id", "upstream_revision", "workload_revision", "image_set_revision",
        "workload_preparation_manifest_sha256", "driver_preparation_manifest_sha256", "custom_images", "load_driver", "support_images"})
    for key, expected in {"schema_version": 1, "candidate_schema": SCHEMA, "workload_id": workload_id,
                          "upstream_revision": UPSTREAM_REVISION, "workload_revision": profile.workload_revision,
                          "image_set_revision": profile.image_set_revision,
                          "workload_preparation_manifest_sha256": sha256(workload_preparation / "context-manifest.json"),
                          "driver_preparation_manifest_sha256": sha256(driver_preparation / "context-manifest.json")}.items():
        _equal(evidence.get(key), expected, f"publication {key}")
    custom = _mapping(evidence["custom_images"], "custom images", keys=set(CUSTOM_KEYS[workload_id]))
    images = {}
    for context_name, image_evidence in (*custom.items(), ("load_driver", evidence["load_driver"])):
        image_evidence = _mapping(image_evidence, "built image evidence", keys={"index_image", "platforms"})
        platforms = _mapping(image_evidence["platforms"], "built image platforms", keys={"linux/amd64"} if context_name == "load_driver" else set(PLATFORMS))
        for item in platforms.values():
            _mapping(item, "built platform evidence", keys={"image", "build_receipt", "smoke_receipt"})
        verified = verify_image_index(image_evidence["index_image"], {key: value["image"] for key, value in platforms.items()}, inspector=inspector, blob_inspector=blob_inspector)
        runtime_key = "load-driver" if context_name == "load_driver" else CUSTOM_KEYS[workload_id][context_name]
        identity = driver_identity if context_name == "load_driver" else contexts[context_name]
        for platform, item in verified["platforms"].items():
            supplied = platforms[platform]
            item["native_build_receipt"] = _build_receipt(supplied["build_receipt"], workload_id, context_name, platform, item, identity, inspector, blob_inspector)
            context = driver_preparation / "context" if context_name == "load_driver" else workload_preparation / context_name
            item["smoke_receipt"] = _smoke_receipt(supplied["smoke_receipt"], workload_id, runtime_key, platform, item, identity, context)
            if context_name == "load_driver":
                item["driver_attestation"] = _driver_binary_attestation(item["smoke_receipt"], driver_manifest)
        images[runtime_key] = {**verified, "prepared_identity": identity}
    images.update(validate_support_evidence(workload_id, evidence["support_images"], inspector=inspector, blob_inspector=blob_inspector))
    return {
        "schema_version": 1, "candidate_schema": LOCK_SCHEMA, "candidate_only": True, "released": False,
        "runtime_qualified": False, "measurement_qualified": False, "anonymous_pull_qualified": False,
        "workload_id": workload_id, "upstream_revision": UPSTREAM_REVISION, "workload_revision": profile.workload_revision,
        "image_set_revision": profile.image_set_revision, "load_driver_revision": profile.load_driver_revision,
        "publication_evidence_sha256": hashlib.sha256(json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "preparation": {"workload": workload, "load_driver": driver_manifest,
                        "workload_manifest_sha256": evidence["workload_preparation_manifest_sha256"],
                        "driver_manifest_sha256": evidence["driver_preparation_manifest_sha256"]},
        "images": images,
        "qualification": {
            "publication": "raw-index-platform-manifest-config-bytes-verified-v1",
            "native_build": "operator-ci-receipts-not-signed-provenance-v1",
            "anonymous_pull": {"qualified": False, "required_method": "isolated-client-with-empty-auth-layer-pull-and-smoke-v1"},
            "dataset_ready": False, "cloud_runtime_ready": False, "denied_edges_ready": False, "cleanup_ready": False,
        },
        "limitations": [
            "This is candidate artifact evidence, not the public runtime lock schema or a release gate.",
            "Raw manifest/config retrieval does not prove anonymous layer pulls; authenticated build/pull receipts cannot imply public access.",
            "Build/inspection receipts are CI/operator observations, not signed provenance or byte-reproducible rebuild proof.",
            "Offline ELF/config/driver inspection does not attest service dependencies, dataset readiness, or semantic workload results.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", choices=tuple(CUSTOM_KEYS), required=True)
    parser.add_argument("--workload-preparation", type=Path, required=True)
    parser.add_argument("--driver-preparation", type=Path, required=True)
    parser.add_argument("--publication", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    from scripts.deathstarbench_candidate_registry import inspect_manifest, inspect_blob
    result = create_candidate_lock(arguments.workload, arguments.workload_preparation, arguments.driver_preparation,
                                   _read_document(arguments.publication, "publication evidence"),
                                   inspector=inspect_manifest, blob_inspector=inspect_blob)
    output = arguments.output.resolve()
    for root in (arguments.workload_preparation.resolve(), arguments.driver_preparation.resolve()):
        _require(root != output and root not in output.parents, "Lock output must not mutate a prepared input tree.")
    # Never overwrite an existing lock or symlink. These are generated artifacts,
    # not repository edits, and failures leave the original inputs untouched.
    try:
        with arguments.output.open("x", encoding="utf-8") as stream:
            stream.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    except OSError as exc:
        raise CandidateImageLockError("Cannot create fresh candidate lock output.") from exc
    print(arguments.output)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (CandidateImageLockError, PreparationError) as error:
        print(f"candidate-image-lock: {error}", file=sys.stderr)
        raise SystemExit(1)
