#!/usr/bin/env python3
"""Prepare additive startup-safe Hotel sources, never build or start services.

The published initialization recipe remains unchanged. This separate context
cannot be adopted by either existing image factory or private live operator.
All output is candidate source evidence, not a startup or readiness capability.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import prepare_deathstarbench_hotel_initialization as guarded
from scripts import deathstarbench_hotel_startup_core as core
from scripts import deathstarbench_hotel_startup_services as services
from scripts.deathstarbench_artifact_common import PreparationError, candidate_output, replace_exact


ASSET_ROOT = Path(__file__).resolve().parent / 'assets/deathstarbench/hotel-startup-v1'
PREPARATION_SCHEMA = 'hotel-guarded-startup-preparation-v1'
RECIPE_REVISION = 'hotel-reservation-guarded-startup-native-go-scratch-candidate-v1'
PATCH_REVISION = 'hotel-reservation-guarded-startup-source-candidate-v1'
POLICY_REVISION = 'hotel-guarded-startup-source-policy-v1'
APPLICATIONS = guarded.APPLICATIONS
DATASET_REVISION = guarded.DATASET_REVISION
GO_TEST_PACKAGES = (
    './initialization/...', './startup', './registry', './cmd/...',
    './services/geo', './services/user', './services/recommendation',
    './services/attractions', './services/reservation', './services/frontend',
)
ASSET_TARGETS = {
    'startup/startup.go': 'startup/startup.go',
    'startup/startup_test.go': 'startup/startup_test.go',
    'initialization/startup_snapshot.go': 'initialization/startup_snapshot.go',
    'initialization/startup_snapshot_test.go': 'initialization/startup_snapshot_test.go',
    'registry/registry.go': 'registry/registry.go',
    'registry/registry_test.go': 'registry/registry_test.go',
    **{f'services/{name}/{filename}': f'services/{name}/{filename}'
       for name in ('geo', 'user', 'recommendation', 'attractions')
       for filename in ('preload.go', 'preload_test.go')},
}
FALSE_FLAGS = (
    'released', 'qualification_authority', 'images_built', 'images_published',
    'database_executed', 'initialization_executed', 'application_start_allowed',
    'dataset_qualified', 'runtime_qualified', 'measurement_qualified',
    'cloud_qualified', 'public_release', 'current_candidate_factory_accepts_recipe',
    'current_guarded_factory_accepts_recipe',
)


def startup_policy():
    return {
        'revision': POLICY_REVISION,
        'scope': 'offline-startup-source-checks-not-live-readiness-or-authority',
        'runtime_arguments_allowed': [],
        'runtime_tls_environment': 'false',
        'jaeger_sample_ratio': 'unset-pinned-default-0.01-or-exact-0.01',
        'config': 'exact-baked-sha256-bounded-regular-file-before-network',
        'required_identity_environment': [
            'HOTEL_CANDIDATE_RUN_ID', 'HOTEL_CANDIDATE_CONTRACT_SHA256',
            'HOTEL_CANDIDATE_STARTUP_ATTEMPT_ID', 'HOTEL_CANDIDATE_COMPONENT',
            'HOTEL_CANDIDATE_POD_UID', 'HOTEL_CANDIDATE_POD_IP',
        ],
        'identity_scope': 'syntax-and-explicit-selectors-not-live-pod-or-cluster-proof',
        'preload': 'bounded-detached-rows-from-the-same-exact-verified-own-store-snapshot',
        'startup_database_writes_allowed': False,
        'cinemas': 'intentionally-absent-verified-empty-not-new-seed-data',
        'registration': 'explicit-private-ip-run-attempt-component-pod-id-one-get-at-most-one-put-shared-deadline',
        'registration_duplicate_check': 'read-before-write-not-an-atomic-distributed-claim',
        'registration_retry_allowed': False,
        'automatic_reregistration_allowed': False,
        'discovery_health_or_restart_qualified': False,
        'metadata_attestation': 'compiled-policy-and-dataset-only-not-runtime-or-image-provenance',
        'application_start_authority': 'none-new-publication-adoption-and-live-controller-required',
    }


def _dockerfile():
    value = replace_exact(guarded._dockerfile(), guarded.RECIPE_REVISION, RECIPE_REVISION,
                          label='separate startup image recipe')
    value = replace_exact(value, 'COPY initialization/ initialization/\n',
                          'COPY initialization/ initialization/\nCOPY startup/ startup/\n',
                          label='additive startup package build input')
    value = replace_exact(value, 'ENV CGO_ENABLED=0 GO111MODULE=on ',
                          'ENV TLS=false CGO_ENABLED=0 GO111MODULE=on ',
                          label='startup offline native test TLS guard')
    old = ' '.join((*guarded.legacy.GO_TEST_PACKAGES, './initialization/...', './cmd/...'))
    return replace_exact(value, old, ' '.join(GO_TEST_PACKAGES),
                         label='startup source offline native tests')


def _take_guarded_output(owned):
    # Work relative to held directory descriptors; do not reopen a moved or
    # caller-replaced source through an unchecked absolute path.
    with owned.directory() as root:
        if set(os.listdir(root)) != {'base'}:
            raise PreparationError('Unexpected startup preparation root inventory.')
    with owned.directory(('base',)) as source:
        expected = {'app', 'context-manifest.json', 'base-preparation.json'}
        if set(os.listdir(source)) != expected:
            raise PreparationError('Unexpected guarded preparation input inventory.')
        for name, target, kind in (
            ('app', 'app', stat.S_ISDIR),
            ('context-manifest.json', 'guarded-base-preparation.json', stat.S_ISREG),
            ('base-preparation.json', 'legacy-base-preparation.json', stat.S_ISREG),
        ):
            if not kind(os.stat(name, dir_fd=source, follow_symlinks=False).st_mode):
                raise PreparationError('Guarded startup input is not the expected real file or directory.')
            os.rename(name, target, src_dir_fd=source, dst_dir_fd=owned.fd)
    os.rmdir('base', dir_fd=owned.fd)
    owned.directories = {(): owned.directories[()]}
    owned.capture()


def _check_base(owned, manifest):
    if (not isinstance(manifest, dict)
            or type(manifest.get('schema_version')) is not int or manifest['schema_version'] != 1
            or manifest.get('candidate_schema') != guarded.PREPARATION_SCHEMA
            or manifest.get('candidate_only') is not True
            or manifest.get('workload_id') != 'hotel_reservation'
            or manifest.get('recipe_revision') != guarded.RECIPE_REVISION
            or manifest.get('dataset_revision') != guarded.DATASET_REVISION
            or any(manifest.get(flag) is not False for flag in (
                'released', 'images_built', 'images_published', 'database_executed',
                'application_start_allowed', 'runtime_qualified', 'measurement_qualified',
                'current_candidate_factory_accepts_recipe'))):
        raise PreparationError('Guarded startup base recipe or closed gates differ.')
    try:
        valid = (json.loads(owned.read('guarded-base-preparation.json')) == manifest
                 and owned.tree_digest('app') == manifest['contexts']['app']['context_sha256']
                 and owned.digest('app/config.json') == manifest['config_sha256']
                 and owned.digest('legacy-base-preparation.json') == manifest['legacy_base_preparation_sha256'])
    except (KeyError, TypeError, ValueError, RecursionError) as exc:
        raise PreparationError('Guarded preparation input receipt is malformed.') from exc
    if not valid:
        raise PreparationError('Guarded preparation changed before startup adoption.')
    for name in ('guarded-base-preparation.json', 'legacy-base-preparation.json'):
        owned.write(name, owned.read(name))


def _patch(owned, relative, replacement, patches):
    before = owned.read('app/' + relative)
    owned.write('app/' + relative, replacement)
    patches[relative] = {
        'original_sha256': hashlib.sha256(before).hexdigest(),
        'patched_sha256': owned.digest('app/' + relative),
    }


def prepare_contexts(upstream: Path, output: Path) -> dict:
    protected = (ASSET_ROOT, Path(__file__), Path(core.__file__), Path(services.__file__))
    with candidate_output(upstream, output, protected_paths=protected) as destination, \
            guarded._GuardedFiles(destination, capture=False, writable=True) as owned, \
            guarded._GuardedFiles(ASSET_ROOT) as assets:
        base = guarded.prepare_contexts(upstream, destination / 'base')
        _take_guarded_output(owned)
        _check_base(owned, base)
        original_initializer = owned.digest('app/cmd/hotel-initialize/main.go')
        patches = {}
        for component in APPLICATIONS:
            relative = f'cmd/{component}/main.go'
            source = owned.read('app/' + relative).decode('utf-8')
            _patch(owned, relative, core.patch_main(source, component, base['config_sha256']), patches)
        relative = 'tls/options.go'
        _patch(owned, relative, core.patch_tls(owned.read('app/' + relative).decode('utf-8')), patches)
        for relative in services.SERVICE_PATHS:
            source = owned.read('app/' + relative).decode('utf-8')
            _patch(owned, relative, services.patch_service(relative, source), patches)
        expected_directories = {()}
        for relative in ASSET_TARGETS:
            parts = guarded._relative(relative)
            expected_directories.update(parts[:index] for index in range(1, len(parts)))
        actual_files = {name for name in assets.files
                        if not (Path(name).name == '.DS_Store' and len(Path(name).parts) <= 3)}
        if set(assets.directories) != expected_directories or actual_files != set(ASSET_TARGETS):
            raise PreparationError('Startup source asset inventory differs from the exact reviewed files.')
        asset_hashes = {}
        for relative, target in sorted(ASSET_TARGETS.items()):
            if guarded._relative(relative) != guarded._relative(target):
                raise PreparationError('Startup assets require exact confined application-relative targets.')
            raw = assets.read(relative)
            asset_hashes[relative] = hashlib.sha256(raw).hexdigest()
            if target == 'registry/registry.go':
                _patch(owned, target, raw, patches)
            else:
                owned.write('app/' + target, raw, new=True)
        owned.write('app/Dockerfile.candidate', _dockerfile())
        if (owned.digest('app/cmd/hotel-initialize/main.go') != original_initializer
                or owned.digest('app/config.json') != base['config_sha256']):
            raise PreparationError('Startup preparation changed the retained initializer or baked config.')
        assets.verify()
        owned.verify()
        manifest = {
            'schema_version': 1, 'candidate_schema': PREPARATION_SCHEMA,
            'candidate_only': True, **{flag: False for flag in FALSE_FLAGS},
            'workload_id': 'hotel_reservation', 'upstream_revision': base['upstream_revision'],
            'patch_revision': PATCH_REVISION, 'recipe_revision': RECIPE_REVISION,
            'policy_revision': POLICY_REVISION, 'startup_policy': startup_policy(),
            'dataset_revision': DATASET_REVISION,
            'initialization_revision': base['initialization_revision'],
            'initialization_policy': base['initialization_policy'], 'seed_source': base['seed_source'],
            'guarded_base_preparation_sha256': owned.digest('guarded-base-preparation.json'),
            'guarded_base_context_sha256': base['contexts']['app']['context_sha256'],
            'legacy_base_preparation_sha256': owned.digest('legacy-base-preparation.json'),
            'retained_initializer_source_sha256': original_initializer,
            'source_patches': patches, 'startup_asset_sha256': asset_hashes,
            'contexts': {'app': {'path': 'app', 'dockerfile': 'Dockerfile.candidate',
                                 'context_sha256': owned.tree_digest('app')}},
            'software': base['software'], 'license_identities': base['license_identities'],
            'config_sha256': base['config_sha256'], 'native_platforms': base['native_platforms'],
            'release_blockers': [
                'Separate native publication, image/source adoption and anonymous full-byte verification are required.',
                'A new live cluster/storage/Pod-bound startup controller and response-loss cleanup qualification are required.',
                'Startup environment and metadata are selectors, not proof of live Pod or initialization authority.',
                'Consul registration checks are not atomic claims or health/restart/discovery qualification.',
                'Existing initialization metadata attests only its unchanged baseline, not these startup source changes.',
                'Application semantic readiness, denied edges, atomic reservation/authentication, warmup and measurement remain unqualified.',
                'Media and all public dispatch/release paths remain unchanged and closed for these candidates.',
            ],
        }
        owned.write('context-manifest.json', json.dumps(manifest, sort_keys=True, indent=2) + '\n', new=True)
        assets.verify()
        owned.verify()
        return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--upstream', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare_contexts(args.upstream, args.output), sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
