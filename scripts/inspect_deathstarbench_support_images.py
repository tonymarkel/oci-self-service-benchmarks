#!/usr/bin/env python3
"""Inspect immutable x86 backing-image candidates, read-only registry access.

This records registry bytes and config provenance, not executed versions,
MongoDB CPU compatibility, readiness, live semantics, or release qualification.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.deathstarbench_artifact_common import PreparationError
from scripts.deathstarbench_candidate_registry import resolve_platform


SUPPORT_IMAGES = {
    "hotel_reservation": {
        "mongodb": "docker.io/library/mongo:5.0.31",
        "consul": "docker.io/hashicorp/consul:1.20.5",
        "jaeger": "docker.io/jaegertracing/all-in-one:1.57.0",
        "memcached": "docker.io/library/memcached:1.6.26",
    },
    "media_microservices": {
        "mongodb": "docker.io/library/mongo:4.4.6",
        "redis": "docker.io/library/redis:7.2.4",
        "jaeger": "docker.io/jaegertracing/all-in-one:1.57.0",
        "memcached": "docker.io/library/memcached:1.6.26",
    },
}
VERSION_ENV = {"mongodb": "MONGO_VERSION", "redis": "REDIS_VERSION", "memcached": "MEMCACHED_VERSION", "consul": "CONSUL_VERSION"}


def inventory_support_images() -> dict[str, object]:
    cache = {}
    workloads = {}
    for workload, images in SUPPORT_IMAGES.items():
        result = {}
        for key, reference in images.items():
            if reference not in cache:
                cache[reference] = resolve_platform(reference, "linux/amd64")
            verified = cache[reference]
            config = verified["config"].get("config", {})
            expected_version = reference.rsplit(":", 1)[1]
            environment = dict(e.split("=", 1) for e in config.get("Env", []) if "=" in e)
            labels = config.get("Labels") or {}
            env_name = VERSION_ENV.get(key)
            observed_version = environment.get(env_name) if env_name else None
            version_source = "image-config-environment" if observed_version is not None else None
            if observed_version is None and "org.opencontainers.image.version" in labels:
                observed_version = labels["org.opencontainers.image.version"].removeprefix("v")
                version_source = "image-config-label"
            if observed_version is not None and observed_version != expected_version:
                raise PreparationError("Backing image version metadata differs from its explicit candidate version.")
            result[key] = {
                "requested_image": reference, "image": verified["image"], "index_image": verified["index_image"],
                "platform": "linux/amd64", "architecture": "x86_64",
                "config_digest": verified["config_digest"], "manifest_digest": verified["manifest_digest"],
                "image_config": verified["config"], "manifest": verified["manifest"],
                "candidate_version": expected_version,
                "observed_version_metadata": observed_version, "version_metadata_source": version_source,
                "binary_version_executed": False, "runtime_qualified": False,
            }
        workloads[workload] = {"images": result}
    return {"schema_version": 1, "candidate_schema": "candidate-support-inventory-v1",
            "candidate_only": True, "released": False, "runtime_semantics_qualified": False,
            "inspection_method": "readonly-registry-index-platform-config-byte-hashes-v1",
            "support_roles_platform": "linux/amd64", "workloads": workloads,
            "limitations": ["Version metadata is not an executed binary-version proof.",
                            "MongoDB 5.0 CPU-feature requirements and every backing-image command/readiness need native live qualification."]}


def write_receipt(path: Path, document: object) -> None:
    if os.path.lexists(path) or not path.parent.is_dir():
        raise PreparationError("Receipt path must be new, with an existing parent.")
    payload = json.dumps(document, indent=2, sort_keys=True) + "\n"
    with path.open("x", encoding="utf-8") as stream:
        stream.write(payload)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    write_receipt(arguments.output, inventory_support_images())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
