#!/usr/bin/env python3
"""Reconstruct startup-safe Hotel sources and compare offline binary metadata.

No registry, publication, cloud, database or application executor is available.
Even matching native metadata is not image provenance or live readiness proof.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_hotel_initialization import parse_hotel_dataset_attestation
from scripts import prepare_deathstarbench_hotel_startup as preparation
from scripts import deathstarbench_hotel_initialization_artifact_common as source_boundary
from scripts import validate_deathstarbench_hotel_initialization as baseline
from scripts.deathstarbench_artifact_common import PreparationError, candidate_output


COMMANDS = preparation.APPLICATIONS
MAX_METADATA_BYTES = 16 * 1024
_ATTESTATION_FALSE = (
    'released', 'initialization_executed', 'database_executed', 'application_started',
    'application_start_allowed', 'application_ready', 'global_readonly_verification_completed',
    'initializer_authorized', 'runtime_authorized', 'qualification_authority',
    'database_execution_verified', 'anonymous_pull_qualified',
    'dataset_qualified', 'runtime_qualified', 'measurement_qualified', 'cloud_qualified', 'public_release',
)


def _require(condition, message):
    if not condition:
        raise PreparationError(message)


def _validate_preparation(files):
    """Local consistency only; full source provenance also requires reconstruction."""
    try:
        value = baseline._json(files.read('context-manifest.json'))
        _require(type(value) is dict and type(value.get('schema_version')) is int
                 and value['schema_version'] == 1
                 and value.get('candidate_schema') == preparation.PREPARATION_SCHEMA
                 and value.get('candidate_only') is True
                 and value.get('recipe_revision') == preparation.RECIPE_REVISION
                 and value.get('patch_revision') == preparation.PATCH_REVISION
                 and value.get('policy_revision') == preparation.POLICY_REVISION
                 and value.get('workload_id') == 'hotel_reservation'
                 and value.get('dataset_revision') == preparation.DATASET_REVISION
                 and value.get('startup_policy') == preparation.startup_policy()
                 and all(value.get(name) is False for name in preparation.FALSE_FLAGS),
                 'Startup preparation scope, revision or closed gates differ.')
        _require(value.get('contexts') == {'app': {'path': 'app', 'dockerfile': 'Dockerfile.candidate',
                 'context_sha256': files.tree_digest('app')}}, 'Startup application context differs.')
        for name, field in (
            ('guarded-base-preparation.json', 'guarded_base_preparation_sha256'),
            ('legacy-base-preparation.json', 'legacy_base_preparation_sha256'),
            ('app/config.json', 'config_sha256'),
            ('app/cmd/hotel-initialize/main.go', 'retained_initializer_source_sha256'),
        ):
            _require(files.digest(name) == value.get(field), 'Retained startup input identity differs.')
        _require(value.get('startup_asset_sha256') == {
            name: files.digest('app/' + target) for name, target in preparation.ASSET_TARGETS.items()
        }, 'Startup application assets differ.')
        files.verify()
        return value
    except (KeyError, TypeError, ValueError, RecursionError) as exc:
        raise PreparationError('Startup preparation receipt is malformed.') from exc


def validate_preparation(root: Path, upstream: Path) -> dict:
    """Independently regenerate every source/receipt/mode, not caller-provided hashes."""
    root = source_boundary._directory(root, 'Startup Hotel preparation')
    upstream = source_boundary._directory(upstream, 'Audited upstream')
    _require(root != upstream and root not in upstream.parents and upstream not in root.parents,
             'Startup preparation overlaps audited source.')
    source_boundary._pinned_upstream(upstream)
    files_type = preparation.guarded._GuardedFiles
    with files_type(root) as supplied, files_type(upstream / 'hotelReservation') as original, \
            files_type(upstream, capture=False) as source_root:
        source_root.read('LICENSE')
        manifest = _validate_preparation(supplied)
        before = supplied.tree_digest()
        with tempfile.TemporaryDirectory(prefix='dsb-hotel-startup-reconstruction-') as directory:
            regenerated = Path(directory).resolve() / 'regenerated'
            expected = preparation.prepare_contexts(upstream, regenerated)
            with files_type(regenerated) as trusted:
                _require(manifest == expected and before == trusted.tree_digest(),
                         'Startup evidence differs from independently regenerated audited source.')
                trusted.verify()
        original.verify()
        source_root.read('LICENSE')
        source_boundary._pinned_upstream(upstream)
        supplied.verify()
        _require(supplied.tree_digest() == before, 'Startup evidence changed during reconstruction.')
        result = {
            'manifest': manifest, 'manifest_sha256': supplied.digest('context-manifest.json'),
            'prepared_tree_sha256': before, 'context_sha256': supplied.tree_digest('app'),
            'recipe_sha256': supplied.digest('app/Dockerfile.candidate'),
            'config_sha256': supplied.digest('app/config.json'),
            'source_fingerprint': manifest['seed_source']['source_fingerprint'],
            'recipe_revision': preparation.RECIPE_REVISION,
        }
        supplied.verify()
        return result


def _summary():
    return {'schema_version': 1, 'candidate_only': True,
            **{name: False for name in preparation.FALSE_FLAGS},
            'recipe_revision': preparation.RECIPE_REVISION, 'policy_revision': preparation.POLICY_REVISION}


def _write(destination, name, value):
    with preparation.guarded._GuardedFiles(destination, writable=True) as files:
        files.write(name, json.dumps(value, sort_keys=True, indent=2) + '\n', new=True)
        files.verify()


def prepare(upstream: Path, output: Path):
    with candidate_output(upstream, output, protected_paths=(preparation.ASSET_ROOT, Path(__file__))) as destination:
        rounds = []
        for name in ('first', 'second'):
            target = destination / name
            manifest = preparation.prepare_contexts(upstream, target)
            with preparation.guarded._GuardedFiles(target) as files:
                _require(_validate_preparation(files) == manifest, 'Serialized startup source differs.')
                rounds.append({'manifest_sha256': files.digest('context-manifest.json'),
                               'prepared_tree_sha256': files.tree_digest()})
                files.verify()
        _require(rounds[0] == rounds[1], 'Startup preparation is not deterministic.')
        value = {**_summary(), 'scope': 'offline-startup-source-preparation-only',
                 'source_preparation_passed': True, 'independent_preparation_rounds': 2,
                 'artifact_receipts': rounds[0]}
        _write(destination, 'preparation-validation.json', value)
        return value


def verify(root: Path, upstream: Path, evidence: Path, output: Path):
    identity = validate_preparation(root, upstream)
    expected_files = {name + '.json' for name in (*COMMANDS, 'hotel-initialize')}
    with preparation.guarded._GuardedFiles(evidence, capture=False,
                                           max_file_bytes=MAX_METADATA_BYTES) as native:
        with native.directory() as descriptor:
            _require(set(os.listdir(descriptor)) == expected_files,
                     'Expected exactly ten startup and one baseline initializer metadata files.')
        native.capture()
        _require(set(native.files) == expected_files and set(native.directories) == {()},
                 'Startup binary metadata inventory differs.')
        original = parse_hotel_dataset_attestation(native.read('hotel-initialize.json'),
                                                  expected_source_fingerprint=identity['source_fingerprint'])
        receipts = {'hotel-initialize.json': native.digest('hotel-initialize.json')}
        for component in COMMANDS:
            filename = component + '.json'
            raw = native.read(filename)
            _require(0 < len(raw) <= MAX_METADATA_BYTES, 'Startup binary metadata exceeds bound.')
            observed = baseline._json(raw)
            expected = {
                'schema_version': 1, 'candidate_schema': 'hotel-guarded-startup-source-attestation-v1',
                'recipe_revision': preparation.RECIPE_REVISION, 'policy_revision': preparation.POLICY_REVISION,
                'component': component, 'config_sha256': 'sha256:' + identity['config_sha256'],
                'dataset_fingerprint': original['dataset_fingerprint'],
                'source_fingerprint': identity['source_fingerprint'], 'candidate_only': True,
                'scope': 'offline-source-contract-only-no-live-readiness-or-execution-authority',
                **{name: False for name in _ATTESTATION_FALSE},
            }
            # Canonical JSON keeps false/zero and true/one type aliases distinct.
            _require(json.dumps(observed, sort_keys=True, allow_nan=False) ==
                     json.dumps(expected, sort_keys=True, allow_nan=False),
                     'Startup metadata differs from exact compiled policy and unchanged baseline.')
            receipts[filename] = hashlib.sha256(raw).hexdigest()
        native.verify()
    value = {**_summary(), 'scope': 'offline-binary-metadata-consistency-not-image-or-runtime-provenance',
             'source_preparation_sha256': identity['manifest_sha256'],
             'source_fingerprint': identity['source_fingerprint'],
             'dataset_fingerprint': original['dataset_fingerprint'],
             'startup_attestation_count': len(COMMANDS), 'baseline_initializer_attestation_count': 1,
             'binary_metadata_consistent': True, 'execution_proven_by_json_alone': False,
             'binary_attestation_sha256': receipts}
    with candidate_output(root, output, protected_paths=(upstream, evidence, Path(__file__))) as destination:
        _write(destination, 'native-metadata-validation.json', value)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest='mode', required=True)
    source = modes.add_parser('prepare')
    source.add_argument('--upstream', type=Path, required=True)
    source.add_argument('--output', type=Path, required=True)
    native = modes.add_parser('verify')
    native.add_argument('--preparation', type=Path, required=True)
    native.add_argument('--upstream', type=Path, required=True)
    native.add_argument('--native-evidence', type=Path, required=True)
    native.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    value = (prepare(args.upstream, args.output) if args.mode == 'prepare'
             else verify(args.preparation, args.upstream, args.native_evidence, args.output))
    print(json.dumps(value, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
