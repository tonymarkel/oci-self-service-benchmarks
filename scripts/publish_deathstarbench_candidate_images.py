#!/usr/bin/env python3
"""Orchestrate manual native Hotel/Media candidate publication, never release.

Preparation stays offline. Registry mutation occurs only in the explicit
assemble command, and only for uniquely named GHCR candidates. Native records
retain their original pushed indexes when dual-platform indexes are assembled.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_workload_contract import distributed_workload_profile
from scripts.deathstarbench_artifact_common import PreparationError, UPSTREAM_REVISION, sha256, tree_sha256
from scripts.create_deathstarbench_candidate_image_lock import CUSTOM_KEYS, create_candidate_lock, CandidateImageLockError
from scripts.deathstarbench_candidate_registry import DIGEST, digest_bytes, inspect_manifest, inspect_blob, resolve_platform
from scripts.inspect_deathstarbench_support_images import write_receipt
from scripts.prepare_deathstarbench_candidate_load_driver import prepare_context
from scripts.prepare_deathstarbench_hotel_images import prepare_contexts as prepare_hotel
from scripts.prepare_deathstarbench_media_images import prepare_contexts as prepare_media
from scripts.smoke_deathstarbench_candidate_images import smoke_image

WORKLOADS = tuple(CUSTOM_KEYS)
PLATFORMS = {"amd64": ("linux/amd64", "x86_64", "ubuntu-24.04"),
             "arm64": ("linux/arm64", "aarch64", "ubuntu-24.04-arm")}
SLUGS = {"hotel_reservation": "hotel", "media_microservices": "media"}


def plan(selection: str) -> dict:
    if selection not in (*WORKLOADS, "all"):
        raise PreparationError("Unsupported candidate workload selection.")
    workloads = list(WORKLOADS) if selection == "all" else [selection]
    return {"workloads": workloads, "native": {"include": [
        {"workload": workload, "architecture": arch, "runner": values[2]}
        for workload in workloads for arch, values in PLATFORMS.items()]}}


def candidate_name(owner: str, workload: str, context: str, run_id: str, attempt: str) -> str:
    owner = owner.lower()
    if not re.fullmatch(r"[a-z0-9][a-z0-9_.-]*", owner) or workload not in WORKLOADS:
        raise PreparationError("Invalid publication owner or workload.")
    if context not in (*CUSTOM_KEYS[workload], "load_driver") or not all(re.fullmatch(r"[1-9][0-9]*", s) for s in (run_id, attempt)):
        raise PreparationError("Invalid candidate context or unique run identity.")
    return f"ghcr.io/{owner}/deathstarbench-{SLUGS[workload]}-{context.replace('_', '-')}:dsb-6ecb097-{run_id}-{attempt}"


def prepared(root: Path, workload: str) -> tuple[Path, Path, dict, dict]:
    app, driver = root / "workload", root / "driver"
    return app, driver, json.loads((app / "context-manifest.json").read_bytes()), json.loads((driver / "context-manifest.json").read_bytes())


def prepare(workload: str, upstream: Path, rock: Path, root: Path) -> None:
    if workload not in WORKLOADS or root.exists() or root.is_symlink():
        raise PreparationError("Preparation requires a supported workload and fresh output directory.")
    root.mkdir(parents=True)
    (prepare_hotel if workload == "hotel_reservation" else prepare_media)(upstream, root / "workload")
    prepare_context(workload, upstream, rock, root / "driver")


def native_record(workload: str, architecture: str, root: Path, indexes: dict[str, str]) -> dict:
    if workload not in WORKLOADS or architecture not in PLATFORMS:
        raise PreparationError("Unsupported native candidate record.")
    target, machine, _ = PLATFORMS[architecture]
    actual = {"arm64": "aarch64", "amd64": "x86_64"}.get(platform.machine(), platform.machine())
    if platform.system() != "Linux" or actual != machine:
        raise PreparationError("Publication records require matching native Linux builders.")
    keys = set(CUSTOM_KEYS[workload]) | ({"load_driver"} if architecture == "amd64" else set())
    if set(indexes) != keys:
        raise PreparationError("Native build inventory is incomplete or crosses support roles.")
    app, driver, app_manifest, driver_manifest = prepared(root, workload)
    images = {}
    for name, index in indexes.items():
        resolved = resolve_platform(index, target)
        context = driver / "context" if name == "load_driver" else app / name
        recipe = context / ("Dockerfile" if name == "load_driver" else "Dockerfile.candidate")
        context_hash = tree_sha256(context)
        declared = driver_manifest["context_sha256"] if name == "load_driver" else app_manifest["contexts"][name]["context_sha256"]
        if context_hash != declared:
            raise PreparationError("Prepared context changed before publication recording.")
        subprocess.run(["docker", "pull", "--platform", target, resolved["image"]], check=True, timeout=600)
        smoke = smoke_image(resolved["image"], workload_id=workload,
                            image_key="load-driver" if name == "load_driver" else CUSTOM_KEYS[workload][name],
                            architecture=machine, context=context)
        images[name] = {"image": resolved["image"], "build_receipt": {
            "schema_version": 1, "workload_id": workload, "context_name": name,
            "platform": target, "context_sha256": context_hash, "recipe_sha256": sha256(recipe),
            "runner_architecture": machine, "qemu_used": False,
            "published_index_image": resolved["index_image"]}, "smoke_receipt": smoke}
    return {"schema_version": 1, "candidate_schema": "candidate-native-publication-v1",
            "workload_id": workload, "platform": target, "images": images,
            "workload_preparation_manifest_sha256": sha256(app / "context-manifest.json"),
            "driver_preparation_manifest_sha256": sha256(driver / "context-manifest.json")}


def validate_records(workload: str, root: Path, records: list[dict]) -> dict:
    app, driver, _, _ = prepared(root, workload)
    by_platform = {}
    for record in records:
        target = record.get("platform")
        if target not in {v[0] for v in PLATFORMS.values()} or target in by_platform:
            raise PreparationError("Missing, duplicate, or invalid native platform records.")
        expected_keys = set(CUSTOM_KEYS[workload]) | ({"load_driver"} if target == "linux/amd64" else set())
        expected = {"schema_version": 1, "candidate_schema": "candidate-native-publication-v1", "workload_id": workload,
                    "workload_preparation_manifest_sha256": sha256(app / "context-manifest.json"),
                    "driver_preparation_manifest_sha256": sha256(driver / "context-manifest.json")}
        if any(type(record.get(k)) is not type(v) or record.get(k) != v for k, v in expected.items()) or set(record.get("images", {})) != expected_keys:
            raise PreparationError("Native records differ from regenerated source preparation.")
        by_platform[target] = record
    if set(by_platform) != {v[0] for v in PLATFORMS.values()}:
        raise PreparationError("Both native architectures must succeed before index assembly.")
    return by_platform


def assemble(workload: str, root: Path, records: list[dict], support: dict, owner: str,
             run_id: str, attempt: str, output: Path) -> dict:
    if output.exists() or output.is_symlink():
        raise PreparationError("Publication output must be a fresh directory.")
    platforms = validate_records(workload, root, records)
    app, driver, _, _ = prepared(root, workload)
    output.mkdir(parents=True)
    custom = {}
    for name in CUSTOM_KEYS[workload]:
        tagged = candidate_name(owner, workload, name, run_id, attempt)
        sources = [platforms[p]["images"][name]["build_receipt"]["published_index_image"] for p in platforms]
        # Check original native evidence and repository boundaries before push.
        repository = tagged.split(":", 1)[0]
        for source in sources:
            if source.split("@", 1)[0] != repository:
                raise PreparationError("Native indexes must belong to the intended candidate repository.")
            inspect_manifest(source)
        metadata = output / f"{name}-index-metadata.json"
        subprocess.run(["docker", "buildx", "imagetools", "create", "--tag", tagged,
                        "--metadata-file", str(metadata), *sources], check=True, timeout=600)
        descriptor = json.loads(metadata.read_bytes())["containerimage.descriptor"]
        digest = descriptor["digest"]
        if not isinstance(digest, str) or not DIGEST.fullmatch(digest):
            raise PreparationError("Index assembly did not return a valid SHA-256 descriptor.")
        index = repository + "@" + digest
        raw = inspect_manifest(index)
        if digest_bytes(raw) != digest or len(raw) != descriptor["size"]:
            raise PreparationError("Published index does not match assembly metadata.")
        custom[name] = {"index_image": index, "platforms": {p: record["images"][name] for p, record in platforms.items()}}
    driver_image = platforms["linux/amd64"]["images"]["load_driver"]
    profile = distributed_workload_profile(workload)
    support_images = {}
    for key, item in support["workloads"][workload]["images"].items():
        support_images[key] = {"requested_image": item["requested_image"], "index_image": item["index_image"],
                               "platforms": {"linux/amd64": {"image": item["image"]}}}
    evidence = {"schema_version": 1, "candidate_schema": "candidate-publication-v1", "workload_id": workload,
                "upstream_revision": UPSTREAM_REVISION, "workload_revision": profile.workload_revision,
                "image_set_revision": profile.image_set_revision,
                "workload_preparation_manifest_sha256": sha256(app / "context-manifest.json"),
                "driver_preparation_manifest_sha256": sha256(driver / "context-manifest.json"),
                "custom_images": custom, "load_driver": {"index_image": driver_image["build_receipt"]["published_index_image"],
                    "platforms": {"linux/amd64": driver_image}}, "support_images": support_images}
    lock = create_candidate_lock(workload, app, driver, evidence, inspector=inspect_manifest, blob_inspector=inspect_blob)
    write_receipt(output / "publication.json", evidence)
    write_receipt(output / "candidate-image-lock.json", lock)
    return lock


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    planning = commands.add_parser("plan")
    planning.add_argument("--selection", choices=(*WORKLOADS, "all"), required=True)
    preparation = commands.add_parser("prepare")
    recording = commands.add_parser("record-native")
    merging = commands.add_parser("assemble")
    for cmd in (preparation, recording, merging):
        cmd.add_argument("--workload", choices=WORKLOADS, required=True)
        cmd.add_argument("--root", type=Path, required=True)
    preparation.add_argument("--upstream", type=Path, required=True)
    preparation.add_argument("--luasocket-rock", type=Path, required=True)
    recording.add_argument("--architecture", choices=tuple(PLATFORMS), required=True)
    recording.add_argument("--app-index", required=True)
    recording.add_argument("--frontend-index")
    recording.add_argument("--driver-index")
    recording.add_argument("--output", type=Path, required=True)
    merging.add_argument("--records", type=Path, nargs=2, required=True)
    merging.add_argument("--support", type=Path, required=True)
    merging.add_argument("--owner", required=True)
    merging.add_argument("--run-id", required=True)
    merging.add_argument("--attempt", required=True)
    merging.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "plan":
        print(json.dumps(plan(args.selection), separators=(",", ":")))
    elif args.command == "prepare":
        prepare(args.workload, args.upstream, args.luasocket_rock, args.root)
    elif args.command == "record-native":
        images = {"app": args.app_index}
        if args.frontend_index:
            images["frontend"] = args.frontend_index
        if args.driver_index:
            images["load_driver"] = args.driver_index
        write_receipt(args.output, native_record(args.workload, args.architecture, args.root, images))
    else:
        assemble(args.workload, args.root, [json.loads(p.read_bytes()) for p in args.records],
                 json.loads(args.support.read_bytes()), args.owner, args.run_id, args.attempt, args.output)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (PreparationError, CandidateImageLockError, subprocess.SubprocessError, OSError, ValueError, KeyError) as error:
        print(f"candidate-publication: {error}", file=sys.stderr)
        raise SystemExit(1)
