#!/usr/bin/env python3
"""Read-only Hotel/Media artifact adoption and run-bound storage/render preflight.

No provider, SSH, Docker, apply, dataset, measurement, or cleanup commands are
invoked. Preparation trees must have been regenerated from the audited sources.
Raw registry manifests/configs are verified; this is not an anonymous layer-pull
or cloud runtime qualification. JSON on stdout is the immutable run snapshot.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_candidate_artifacts import adopt_candidate_artifacts
from app.deathstarbench_candidate_runtime import (
    create_candidate_runtime_plan,
    read_candidate_runtime_snapshot,
    validate_candidate_runtime_snapshot,
)
from scripts.deathstarbench_candidate_registry import inspect_manifest, inspect_blob


def argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workload', choices=('hotel_reservation', 'media_microservices'), required=True)
    parser.add_argument('--image-lock', type=Path, required=True)
    parser.add_argument('--publication', type=Path, required=True)
    parser.add_argument('--workload-preparation', type=Path, required=True)
    parser.add_argument('--driver-preparation', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--provider', choices=('aws', 'azure', 'gcp', 'oci'), required=True)
    parser.add_argument('--topology-fingerprint', required=True)
    parser.add_argument('--application-architecture', choices=('x86_64', 'aarch64'), required=True)
    parser.add_argument('--load-generator-ip', required=True)
    parser.add_argument('--database-filesystem-uuid', required=True)
    parser.add_argument('--verify-snapshot', type=Path, help='Require an existing snapshot to match this fresh preflight exactly.')
    return parser


def preflight(arguments, *, inspector=inspect_manifest, blob_inspector=inspect_blob):
    artifacts = adopt_candidate_artifacts(
        arguments.workload, arguments.image_lock, arguments.publication,
        workload_preparation=arguments.workload_preparation,
        driver_preparation=arguments.driver_preparation,
        inspector=inspector, blob_inspector=blob_inspector,
    )
    plan = create_candidate_runtime_plan(
        artifacts, run_id=arguments.run_id, provider=arguments.provider,
        topology_fingerprint=arguments.topology_fingerprint,
        application_architecture=arguments.application_architecture,
        load_generator_ip=arguments.load_generator_ip,
        filesystem_uuid=arguments.database_filesystem_uuid,
    )
    if arguments.verify_snapshot is not None:
        validate_candidate_runtime_snapshot(
            read_candidate_runtime_snapshot(arguments.verify_snapshot), plan,
        )
    return plan.snapshot()


def main() -> int:
    parser = argument_parser()
    arguments = parser.parse_args()
    try:
        result = preflight(arguments)
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
