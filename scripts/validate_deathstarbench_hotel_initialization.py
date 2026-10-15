#!/usr/bin/env python3
"""Check additive Hotel preparation and offline binary metadata, never a runtime.

The CI caller executes the pinned Go compiler with no network. This script
checks the resulting bounded JSON against its exact prepared source context;
JSON alone is not proof of binary execution, image provenance or Mongo safety.
It has no publication, adoption, controller, database or release capability.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_hotel_initialization import (
    MAX_ATTESTATION_BYTES, parse_hotel_dataset_attestation,
    validate_hotel_initialization_policy,
)
from scripts import prepare_deathstarbench_hotel_initialization as preparation
from scripts import deathstarbench_hotel_seed_source as seed
from scripts.deathstarbench_artifact_common import (
    PreparationError, UPSTREAM_REVISION, candidate_output,
)


COMMANDS = tuple(sorted((*preparation.APPLICATIONS, 'hotel-initialize')))
_FALSE_FLAGS = (
    'released', 'images_built', 'images_published', 'database_executed',
    'application_start_allowed', 'runtime_qualified', 'measurement_qualified',
    'current_candidate_factory_accepts_recipe',
)


def _require(condition, message):
    if not condition:
        raise PreparationError(message)


def _read(files, relative):
    raw = files.read(relative)
    _require(0 < len(raw) <= MAX_ATTESTATION_BYTES, 'Expected bounded evidence file.')
    return raw


def _json(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            _require(key not in value, 'Duplicate evidence JSON field.')
            value[key] = item
        return value
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(PreparationError('Nonfinite evidence JSON.')))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise PreparationError('Invalid evidence JSON.') from exc


def _source_fingerprint():
    identity = {
        'generator_revision': seed.GENERATOR_REVISION,
        'arithmetic_policy': seed.arithmetic_policy(),
        'source_files': seed.PREPARED_SOURCE_SHA256,
        'collections': [
            {'component': component, 'database': component + '-db', 'name': name,
             'documents_variable': rows, 'expected_documents': count}
            for component in seed.COMPONENTS for name, rows, count in seed.COLLECTION_SPECS[component]
        ],
    }
    raw = (json.dumps(identity, sort_keys=True, separators=(',', ':')) + '\n').encode()
    return identity, 'sha256:' + hashlib.sha256(raw).hexdigest()


def _validate_preparation(files) -> dict:
    _require(('app',) in files.directories, 'Expected real prepared application directory.')
    raw = _read(files, 'context-manifest.json')
    value = _json(raw)
    _require(isinstance(value, dict) and value.get('schema_version') == 1
             and type(value['schema_version']) is int
             and value.get('candidate_schema') == preparation.PREPARATION_SCHEMA
             and value.get('candidate_only') is True
             and value.get('workload_id') == 'hotel_reservation'
             and value.get('upstream_revision') == UPSTREAM_REVISION
             and value.get('recipe_revision') == preparation.RECIPE_REVISION
             and value.get('patch_revision') == preparation.PATCH_REVISION
             and value.get('dataset_revision') == preparation.DATASET_REVISION
             and value.get('initialization_revision') == preparation.CONTRACT_REVISION
             and all(value.get(flag) is False for flag in _FALSE_FLAGS),
             'Hotel preparation scope, revision or closed gates differ.')
    validate_hotel_initialization_policy(value.get('initialization_policy'))
    _require(value.get('contexts') == {'app': {'path': 'app', 'dockerfile': 'Dockerfile.candidate',
             'context_sha256': files.tree_digest('app')}}, 'Prepared Hotel context bytes differ.')
    _require(files.digest('base-preparation.json') == value.get('legacy_base_preparation_sha256'),
             'Retained v1 input receipt differs.')
    identity, source_fingerprint = _source_fingerprint()
    receipts = value.get('seed_source')
    _require(isinstance(receipts, dict) and receipts.get('source_fingerprint_input') == identity
             and receipts.get('source_fingerprint') == source_fingerprint
             and receipts.get('generator_revision') == seed.GENERATOR_REVISION
             and receipts.get('arithmetic_policy') == seed.arithmetic_policy()
             and type(receipts.get('schema_version')) is int and receipts['schema_version'] == 1
             and receipts.get('candidate_only') is True and receipts.get('released') is False
             and receipts.get('qualification_authority') is False,
             'Compiled Hotel seed source binding differs.')
    generated = receipts.get('generated_source_sha256')
    expected = {'initialization/catalog/catalog.go',
                *(f'initialization/seeddata/{component}/seed.go' for component in seed.COMPONENTS)}
    _require(isinstance(generated, dict) and set(generated) == expected,
             'Generated Hotel catalog inventory differs.')
    for name, digest in generated.items():
        _require(hashlib.sha256(_read(files, 'app/' + name)).hexdigest() == digest,
                 'Generated Hotel catalog source bytes differ.')
    return value


def validate_preparation(root: Path) -> dict:
    with preparation._GuardedFiles(root) as files:
        value = _validate_preparation(files)
        files.verify()
        return value


def _write_summary(destination, name, value):
    with preparation._GuardedFiles(destination, writable=True) as owned:
        owned.write(name, json.dumps(value, sort_keys=True, indent=2) + '\n', new=True)
        owned.verify()


def _summary():
    return {'schema_version': 1, 'candidate_only': True, 'released': False,
            'recipe_revision': preparation.RECIPE_REVISION,
            'images_built': False, 'images_published': False,
            'database_executed': False, 'dataset_qualified': False,
            'application_start_allowed': False, 'runtime_qualified': False,
            'measurement_qualified': False, 'public_release': False}


def prepare(upstream: Path, output: Path) -> dict:
    with candidate_output(upstream, output, protected_paths=(preparation.ASSET_ROOT, Path(__file__))) as destination:
        rounds = []
        for name in ('first', 'second'):
            target = destination / name
            manifest = preparation.prepare_contexts(upstream, target)
            with preparation._GuardedFiles(target) as files:
                _require(_validate_preparation(files) == manifest, 'Serialized Hotel preparation differs.')
                rounds.append({'manifest_sha256': files.digest('context-manifest.json'),
                               'prepared_tree_sha256': files.tree_digest()})
                files.verify()
        _require(rounds[0] == rounds[1], 'New Hotel preparation is not deterministic.')
        value = {**_summary(), 'scope': 'source-preparation-only', 'source_preparation_passed': True,
                 'independent_preparation_rounds': 2, 'artifact_receipts': rounds[0]}
        _write_summary(destination, 'preparation-validation.json', value)
        return value


def verify(root: Path, evidence: Path, output: Path) -> dict:
    with preparation._GuardedFiles(root) as source, \
            preparation._GuardedFiles(evidence, capture=False, max_file_bytes=MAX_ATTESTATION_BYTES) as native:
        manifest = _validate_preparation(source)
        source_fingerprint = manifest['seed_source']['source_fingerprint']
        expected = {name + '.json' for name in COMMANDS}
        with native.directory() as descriptor:
            _require(set(os.listdir(descriptor)) == expected, 'Expected exactly eleven binary attestation files.')
        native.capture()
        _require(set(native.files) == expected and set(native.directories) == {()},
                 'Expected exactly eleven binary attestation files.')
        receipts, dataset = {}, None
        for command in COMMANDS:
            name = command + '.json'
            raw = _read(native, name)
            observed = parse_hotel_dataset_attestation(raw, expected_source_fingerprint=source_fingerprint)
            _require(dataset is None or observed == dataset, 'Hotel binaries attest different datasets.')
            dataset = observed
            receipts[name] = hashlib.sha256(raw).hexdigest()
        source_preparation_sha256 = source.digest('context-manifest.json')
        source.verify()
        native.verify()
    value = {**_summary(), 'scope': 'offline-binary-metadata-consistency-only-no-mongodb',
             'source_preparation_sha256': source_preparation_sha256,
             'source_fingerprint': source_fingerprint, 'dataset_fingerprint': dataset['dataset_fingerprint'],
             'database_count': 8, 'collection_count': 11, 'document_count': 873,
             'binary_attestation_count': len(receipts), 'binary_metadata_consistent': True,
             'binary_attestation_sha256': receipts,
             'execution_proven_by_json_alone': False}
    with candidate_output(root, output, protected_paths=(evidence, Path(__file__))) as destination:
        _write_summary(destination, 'native-metadata-validation.json', value)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest='mode', required=True)
    source = modes.add_parser('prepare')
    source.add_argument('--upstream', type=Path, required=True)
    source.add_argument('--output', type=Path, required=True)
    native = modes.add_parser('verify')
    native.add_argument('--preparation', type=Path, required=True)
    native.add_argument('--native-evidence', type=Path, required=True)
    native.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    value = (prepare(args.upstream, args.output) if args.mode == 'prepare'
             else verify(args.preparation, args.native_evidence, args.output))
    print(json.dumps(value, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
