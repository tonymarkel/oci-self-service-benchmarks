#!/usr/bin/env python3
"""Create the deployable DeathStarBench image lock from registry indexes.

The final JSON intentionally contains only the strict workload-runtime schema.
Build context identity and rebuild limitations remain separate build artifacts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any


UPSTREAM_REVISION = "6ecb09706140f8730b5385c08f1386c654c3c526"
WORKLOAD_REVISION = "social-network-6ecb097-workload-v1"
IMAGE_SET_REVISION = "social-network-images-v1"
REQUIRED_PLATFORMS = ("linux/amd64", "linux/arm64")
THIRD_PARTY_IMAGES = {
    "mongo": "docker.io/library/mongo:4.4.6",
    "redis": "docker.io/library/redis:7.2.4",
    "memcached": "docker.io/library/memcached:1.6.26",
    "jaeger": "docker.io/jaegertracing/all-in-one:1.57.0",
}
CUSTOM_IMAGE_NAMES = ("app", "frontend", "media_frontend")
CUSTOM_RUNTIME_KEYS = {
    "app": "social-network-microservices",
    "frontend": "nginx-thrift",
    "media_frontend": "media-frontend",
}
THIRD_PARTY_RUNTIME_KEYS = {
    "mongo": "mongodb",
    "redis": "redis",
    "memcached": "memcached",
    "jaeger": "jaeger",
}
RUNTIME_IMAGE_KEYS = (
    "jaeger",
    "media-frontend",
    "memcached",
    "mongodb",
    "nginx-thrift",
    "redis",
    "social-network-microservices",
)
DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
REPOSITORY_PATTERN = re.compile(
    r"(?=.{1,440}\Z)(?:[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/)+"
    r"[a-z0-9]+(?:[._/-][a-z0-9]+)*\Z"
)


class ImageLockError(RuntimeError):
    """Raised when an image index cannot produce a complete exact lock."""


def inspect_raw(reference: str) -> bytes:
    """Return the registry's raw OCI index for ``reference`` using Buildx."""
    try:
        result = subprocess.run(
            ["docker", "buildx", "imagetools", "inspect", "--raw", reference],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as exc:
        raise ImageLockError("Docker Buildx is required to inspect image indexes.") from exc
    except subprocess.CalledProcessError as exc:
        detail = exc.stderr.decode("utf-8", errors="replace").strip()
        raise ImageLockError(f"Unable to inspect {reference}: {detail}") from exc
    return result.stdout


def _valid_digest(value: object, *, label: str) -> str:
    digest = str(value or "")
    if not DIGEST_PATTERN.fullmatch(digest):
        raise ImageLockError(f"{label} does not have an exact SHA-256 digest.")
    return digest


def _repository(reference: str, *, label: str) -> str:
    """Return a lower-case repository with both digest and tag removed."""
    if not isinstance(reference, str) or any(
        character.isspace() for character in reference
    ):
        raise ImageLockError(f"{label} image reference is invalid.")
    without_digest = reference.split("@", 1)[0]
    final_slash = without_digest.rfind("/")
    final_colon = without_digest.rfind(":")
    if final_colon > final_slash:
        without_digest = without_digest[:final_colon]
    if not REPOSITORY_PATTERN.fullmatch(without_digest):
        raise ImageLockError(f"{label} image repository is not canonical.")
    return without_digest


def platform_digests(raw_index: bytes, *, reference: str) -> dict[str, str]:
    try:
        document = json.loads(raw_index)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ImageLockError(f"{reference} did not return a valid OCI index.") from exc

    manifests = document.get("manifests") if isinstance(document, dict) else None
    if not isinstance(manifests, list):
        raise ImageLockError(f"{reference} is not a multi-platform image index.")

    found: dict[str, str] = {}
    for descriptor in manifests:
        if not isinstance(descriptor, dict):
            continue
        platform = descriptor.get("platform")
        if not isinstance(platform, dict):
            continue
        key = f"{platform.get('os')}/{platform.get('architecture')}"
        if key not in REQUIRED_PLATFORMS:
            # BuildKit provenance and SBOM attestations use unknown/unknown.
            continue
        digest = _valid_digest(
            descriptor.get("digest"), label=f"{reference} {key} manifest"
        )
        if key in found:
            raise ImageLockError(f"{reference} contains duplicate {key} manifests.")
        found[key] = digest

    missing = [platform for platform in REQUIRED_PLATFORMS if platform not in found]
    if missing:
        raise ImageLockError(
            f"{reference} is missing required platform manifests: {', '.join(missing)}."
        )
    return {platform: found[platform] for platform in REQUIRED_PLATFORMS}


def _load_context_manifest(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ImageLockError("The context manifest is missing or invalid.") from exc
    if not isinstance(document, dict):
        raise ImageLockError("The context manifest must be a JSON object.")
    if document.get("schema_version") != 1:
        raise ImageLockError("The context manifest schema is unsupported.")
    if document.get("candidate_only") is not True:
        raise ImageLockError("The context manifest must describe a candidate build.")
    if document.get("upstream_revision") != UPSTREAM_REVISION:
        raise ImageLockError("The context manifest has a conflicting upstream revision.")
    if document.get("runtime_git_clone") is not False:
        raise ImageLockError("The candidate must prohibit runtime Git clones.")
    if document.get("runtime_source_baked_into_images") is not True:
        raise ImageLockError("The candidate must bake source assets into its images.")
    if document.get("third_party_images") != THIRD_PARTY_IMAGES:
        raise ImageLockError("The context manifest has conflicting support images.")
    contexts = document.get("contexts")
    if not isinstance(contexts, dict) or set(contexts) != set(CUSTOM_IMAGE_NAMES):
        raise ImageLockError("The context manifest has incomplete custom images.")
    return document


def _resolved_references(
    name: str,
    source: str,
    inspector: Callable[[str], bytes],
    *,
    require_index_digest: bool,
) -> dict[str, str]:
    if require_index_digest:
        if source.count("@") != 1:
            raise ImageLockError(
                f"The custom {name} image must include its pushed index digest."
            )
        _valid_digest(source.rsplit("@", 1)[1], label=f"custom {name} image index")
    repository = _repository(source, label=name)
    digests = platform_digests(inspector(source), reference=source)
    return {
        platform: f"{repository}@{digest}"
        for platform, digest in digests.items()
    }


def _runtime_lock(
    custom: Mapping[str, Mapping[str, str]],
    third_party: Mapping[str, Mapping[str, str]],
) -> dict[str, Any]:
    platforms: dict[str, Any] = {}
    for platform in REQUIRED_PLATFORMS:
        images: dict[str, str] = {}
        for build_name, runtime_key in CUSTOM_RUNTIME_KEYS.items():
            images[runtime_key] = custom[build_name][platform]
        for build_name, runtime_key in THIRD_PARTY_RUNTIME_KEYS.items():
            images[runtime_key] = third_party[build_name][platform]
        if set(images) != set(RUNTIME_IMAGE_KEYS):
            raise ImageLockError("The runtime image-key mapping is incomplete.")
        platforms[platform] = {"images": images}
    return {
        "schema_version": 1,
        "workload_revision": WORKLOAD_REVISION,
        "image_set_revision": IMAGE_SET_REVISION,
        "upstream_revision": UPSTREAM_REVISION,
        "released": False,
        "platforms": platforms,
    }


def _validate_runtime_contract(lock: Mapping[str, Any]) -> None:
    """Use the deployment module as the authoritative schema validator."""
    repository_root = Path(__file__).resolve().parents[1]
    inserted = False
    if str(repository_root) not in sys.path:
        sys.path.insert(0, str(repository_root))
        inserted = True
    try:
        from app.deathstarbench_k3s_workload import (
            WorkloadBundleError,
            validate_image_lock,
        )

        try:
            validate_image_lock(lock)
        except WorkloadBundleError as exc:
            raise ImageLockError(
                f"Generated image lock violates the workload contract: {exc}"
            ) from exc
    finally:
        if inserted:
            sys.path.remove(str(repository_root))


def _synthetic_custom_references() -> dict[str, dict[str, str]]:
    """Create distinct valid references for a pre-push schema dry run."""
    values: dict[str, dict[str, str]] = {}
    for build_name, runtime_key in CUSTOM_RUNTIME_KEYS.items():
        values[build_name] = {}
        for platform in REQUIRED_PLATFORMS:
            digest = hashlib.sha256(f"{runtime_key}:{platform}".encode()).hexdigest()
            values[build_name][platform] = (
                f"registry.invalid/deathstarbench/{runtime_key}@sha256:{digest}"
            )
    return values


def preflight_third_party(
    context_manifest: Path,
    *,
    inspector: Callable[[str], bytes] = inspect_raw,
) -> dict[str, Any]:
    """Resolve support images and dry-run the strict runtime schema pre-push."""
    _load_context_manifest(context_manifest)
    resolved = {
        name: _resolved_references(
            name, source, inspector, require_index_digest=False
        )
        for name, source in THIRD_PARTY_IMAGES.items()
    }
    _validate_runtime_contract(_runtime_lock(_synthetic_custom_references(), resolved))
    return {
        "schema_version": 1,
        "upstream_revision": UPSTREAM_REVISION,
        "runtime_schema_preflight": True,
        "sources": THIRD_PARTY_IMAGES,
        "platforms": {
            platform: {
                "images": {
                    THIRD_PARTY_RUNTIME_KEYS[name]: resolved[name][platform]
                    for name in THIRD_PARTY_IMAGES
                }
            }
            for platform in REQUIRED_PLATFORMS
        },
    }


def _load_third_party_preflight(path: Path) -> dict[str, dict[str, str]]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ImageLockError("The support-image preflight is missing or invalid.") from exc
    expected_fields = {
        "schema_version",
        "upstream_revision",
        "runtime_schema_preflight",
        "sources",
        "platforms",
    }
    if not isinstance(document, dict) or set(document) != expected_fields:
        raise ImageLockError("The support-image preflight schema is invalid.")
    if (
        document["schema_version"] != 1
        or document["upstream_revision"] != UPSTREAM_REVISION
        or document["runtime_schema_preflight"] is not True
        or document["sources"] != THIRD_PARTY_IMAGES
    ):
        raise ImageLockError("The support-image preflight identity is invalid.")
    platforms = document["platforms"]
    if not isinstance(platforms, dict) or set(platforms) != set(REQUIRED_PLATFORMS):
        raise ImageLockError("The support-image preflight platforms are invalid.")

    resolved = {name: {} for name in THIRD_PARTY_IMAGES}
    expected_runtime_keys = set(THIRD_PARTY_RUNTIME_KEYS.values())
    inverse_keys = {value: key for key, value in THIRD_PARTY_RUNTIME_KEYS.items()}
    for platform in REQUIRED_PLATFORMS:
        platform_value = platforms[platform]
        if not isinstance(platform_value, dict) or set(platform_value) != {"images"}:
            raise ImageLockError("A support-image preflight platform is invalid.")
        images = platform_value["images"]
        if not isinstance(images, dict) or set(images) != expected_runtime_keys:
            raise ImageLockError("The support-image preflight keys are invalid.")
        for runtime_key, reference in images.items():
            repository, separator, digest = str(reference).partition("@")
            source_name = inverse_keys[runtime_key]
            if (
                separator != "@"
                or repository != _repository(
                    THIRD_PARTY_IMAGES[source_name], label=source_name
                )
                or not DIGEST_PATTERN.fullmatch(digest)
            ):
                raise ImageLockError("A support-image preflight reference is invalid.")
            resolved[source_name][platform] = str(reference)
    return resolved


def build_lock(
    context_manifest: Path,
    custom_images: Mapping[str, str],
    *,
    inspector: Callable[[str], bytes] = inspect_raw,
    third_party_preflight: Path | None = None,
) -> dict[str, Any]:
    _load_context_manifest(context_manifest)
    if set(custom_images) != set(CUSTOM_IMAGE_NAMES):
        raise ImageLockError("Exactly three custom image references are required.")

    custom = {
        name: _resolved_references(
            name, custom_images[name], inspector, require_index_digest=True
        )
        for name in CUSTOM_IMAGE_NAMES
    }
    if third_party_preflight is None:
        third_party = {
            name: _resolved_references(
                name, source, inspector, require_index_digest=False
            )
            for name, source in THIRD_PARTY_IMAGES.items()
        }
    else:
        third_party = _load_third_party_preflight(third_party_preflight)

    lock = _runtime_lock(custom, third_party)
    _validate_runtime_contract(lock)
    return lock


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--preflight-third-party", action="store_true")
    parser.add_argument("--third-party-preflight", type=Path)
    parser.add_argument("--app-image")
    parser.add_argument("--frontend-image")
    parser.add_argument("--media-frontend-image")
    return parser


def main() -> int:
    parser = _parser()
    arguments = parser.parse_args()
    custom_arguments = (
        arguments.app_image,
        arguments.frontend_image,
        arguments.media_frontend_image,
    )
    if arguments.preflight_third_party:
        if any(custom_arguments) or arguments.third_party_preflight is not None:
            parser.error("The support-image preflight does not accept custom images.")
        document = preflight_third_party(arguments.context_manifest)
    else:
        if not all(custom_arguments):
            parser.error("All three custom image references are required.")
        document = build_lock(
            arguments.context_manifest,
            {
                "app": arguments.app_image,
                "frontend": arguments.frontend_image,
                "media_frontend": arguments.media_frontend_image,
            },
            third_party_preflight=arguments.third_party_preflight,
        )

    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(arguments.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
