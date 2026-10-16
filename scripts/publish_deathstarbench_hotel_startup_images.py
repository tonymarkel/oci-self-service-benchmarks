#!/usr/bin/env python3
"""Explicit native startup-Hotel publication, never application startup or release.

The old Hotel publication is freshly adopted rather than republished. Both
native app receipts, their registry bytes, and that complete baseline must pass
before the first multi-platform index write. Anonymous native pull evidence is
separate from artifact adoption and does not open any runtime or public gate.
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

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_candidate_artifacts import adopt_candidate_artifacts
from app.deathstarbench_hotel_initialization import DATASET_REVISION
from scripts.prepare_deathstarbench_hotel_startup import RECIPE_REVISION, POLICY_REVISION
from scripts.create_deathstarbench_candidate_image_lock import _read_document
from scripts.deathstarbench_artifact_common import (
    PreparationError, UPSTREAM_REVISION, candidate_output, sha256, tree_sha256,
)
from scripts.deathstarbench_candidate_registry import (
    DIGEST, digest_bytes, inspect_blob, inspect_manifest, resolve_platform,
)
from scripts.deathstarbench_hotel_startup_artifact_common import (
    BASELINE_RUN, BASELINE_ARTIFACT, BASELINE_LOCK_SHA256, BASELINE_PUBLICATION_SHA256,
    validate_startup_preparation, verify_baseline_files,
)
from scripts import smoke_deathstarbench_hotel_initialization_images as shared_smoke
from scripts.inspect_deathstarbench_support_images import write_receipt
from scripts.prepare_deathstarbench_candidate_load_driver import prepare_context
from scripts.prepare_deathstarbench_hotel_images import prepare_contexts as prepare_legacy
from scripts.prepare_deathstarbench_hotel_startup import prepare_contexts as prepare_startup


PUBLICATION_SCHEMA = 'hotel-guarded-startup-publication-v1'
NATIVE_SCHEMA = 'hotel-guarded-startup-native-publication-v1'
BUILD_SCHEMA = 'hotel-guarded-startup-native-build-v1'
PLATFORMS = {
    'amd64': ('linux/amd64', 'x86_64', 'ubuntu-24.04'),
    'arm64': ('linux/arm64', 'aarch64', 'ubuntu-24.04-arm'),
}


def plan() -> dict:
    return {'native': {'include': [
        {'architecture': architecture, 'runner': values[2]}
        for architecture, values in PLATFORMS.items()
    ]}}


def candidate_name(owner: str, run_id: str, attempt: str) -> str:
    owner = owner.lower()
    if (not re.fullmatch(r'[a-z0-9][a-z0-9_.-]*', owner)
            or not all(re.fullmatch(r'[1-9][0-9]*', value) for value in (run_id, attempt))):
        raise PreparationError('Invalid startup Hotel owner or unique run identity.')
    return f'ghcr.io/{owner}/deathstarbench-hotel-app:guard-startup-6ecb097-{run_id}-{attempt}'


def prepare(upstream: Path, rock: Path, root: Path) -> None:
    with candidate_output(upstream, root, protected_paths=(rock, Path(__file__))) as owned:
        prepare_legacy(upstream, owned / 'baseline-workload')
        prepare_context('hotel_reservation', upstream, rock, owned / 'baseline-driver')
        prepare_startup(upstream, owned / 'startup')


def prepared(root: Path) -> tuple[Path, dict]:
    preparation = root / 'startup'
    manifest = _read_document(preparation / 'context-manifest.json', 'startup Hotel preparation')
    return preparation, manifest


def baseline(root: Path, evidence: Path):
    """Only the exact reviewed v1 files, with complete fresh source/OCI checks."""
    lock_path, publication_path = verify_baseline_files(evidence)
    binding = adopt_candidate_artifacts(
        'hotel_reservation', lock_path, publication_path,
        workload_preparation=root / 'baseline-workload',
        driver_preparation=root / 'baseline-driver',
        inspector=inspect_manifest, blob_inspector=inspect_blob,
    )
    metadata = binding.metadata()
    if (metadata['candidate_lock_sha256'] != BASELINE_LOCK_SHA256
            or metadata['publication_document_sha256'] != BASELINE_PUBLICATION_SHA256):
        raise PreparationError('Reused Hotel artifacts differ from the exact reviewed baseline.')
    return binding


def _native(architecture: str) -> tuple[str, str]:
    if architecture not in PLATFORMS:
        raise PreparationError('Unsupported startup Hotel native architecture.')
    target, machine, _ = PLATFORMS[architecture]
    actual = {'arm64': 'aarch64', 'amd64': 'x86_64'}.get(platform.machine(), platform.machine())
    if platform.system() != 'Linux' or actual != machine:
        raise PreparationError('Startup Hotel evidence requires a matching native Linux host.')
    return target, machine


def _identity(root: Path) -> dict:
    preparation, manifest = prepared(root)
    return {
        'schema_version': 1, 'workload_id': 'hotel_reservation',
        'recipe_revision': RECIPE_REVISION, 'dataset_revision': DATASET_REVISION,
        'policy_revision': POLICY_REVISION, 'config_sha256': manifest['config_sha256'],
        'generator_revision': manifest['seed_source']['generator_revision'],
        'source_fingerprint': manifest['seed_source']['source_fingerprint'],
        'preparation_manifest_sha256': sha256(preparation / 'context-manifest.json'),
    }


def native_record(architecture: str, root: Path, upstream: Path, index: str) -> dict:
    from scripts.smoke_deathstarbench_hotel_startup_images import smoke_startup_hotel_image
    target, machine = _native(architecture)
    # A changed/forged preparation must fail before resolution or image pulls,
    # not merely at the later assembled-lock boundary.
    validate_startup_preparation(root / 'startup', upstream)
    preparation, manifest = prepared(root)
    context = preparation / 'app'
    context_hash = tree_sha256(context)
    if context_hash != manifest['contexts']['app']['context_sha256']:
        raise PreparationError('Startup Hotel context changed before publication recording.')
    resolved = resolve_platform(index, target)
    subprocess.run(['docker', 'pull', '--platform', target, resolved['image']], check=True, timeout=600)
    smoke = smoke_startup_hotel_image(resolved['image'], architecture=machine, preparation=preparation)
    identity = _identity(root)
    return {
        **identity, 'candidate_schema': NATIVE_SCHEMA, 'platform': target,
        'image': resolved['image'],
        'build_receipt': {
            **identity, 'candidate_schema': BUILD_SCHEMA, 'context_name': 'app',
            'platform': target, 'context_sha256': context_hash,
            'recipe_sha256': sha256(context / 'Dockerfile.candidate'),
            'runner_architecture': machine, 'qemu_used': False,
            'published_index_image': resolved['index_image'],
        },
        'smoke_receipt': smoke,
    }


def validate_records(root: Path, records: list[dict]) -> dict:
    expected = _identity(root)
    required = set(expected) | {'candidate_schema', 'platform', 'image', 'build_receipt', 'smoke_receipt'}
    by_platform = {}
    for record in records:
        target = record.get('platform')
        if (set(record) != required or target not in {v[0] for v in PLATFORMS.values()}
                or target in by_platform or record.get('candidate_schema') != NATIVE_SCHEMA
                or any(type(record.get(key)) is not type(value) or record.get(key) != value
                       for key, value in expected.items())):
            raise PreparationError('Guarded native receipts must match regenerated source on both platforms.')
        by_platform[target] = record
    if set(by_platform) != {v[0] for v in PLATFORMS.values()}:
        raise PreparationError('Both native startup Hotel architectures must succeed before assembly.')
    return by_platform


def assemble(root: Path, upstream: Path, records: list[dict], baseline_evidence: Path,
             owner: str, run_id: str, attempt: str, output: Path) -> dict:
    from scripts.create_deathstarbench_hotel_startup_image_lock import (
        create_startup_hotel_lock, preflight_native_startup_hotel,
    )
    if output.exists() or output.is_symlink():
        raise PreparationError('Guarded publication output must be fresh.')
    protected = (root.resolve(), upstream.resolve(), baseline_evidence.resolve())
    resolved = output.resolve()
    if any(resolved == value or resolved.is_relative_to(value) or value.is_relative_to(resolved)
           for value in protected):
        raise PreparationError('Guarded publication output must not overlap any source/evidence input.')
    validate_startup_preparation(root / 'startup', upstream)
    platforms = validate_records(root, records)
    preparation, _ = prepared(root)
    reused = baseline(root, baseline_evidence)
    # Complete source, baseline, native manifest/config/smoke checks before ANY index mutation.
    preflight_native_startup_hotel(
        preparation, upstream, records, reused, inspector=inspect_manifest, blob_inspector=inspect_blob,
    )
    tagged = candidate_name(owner, run_id, attempt)
    repository = tagged.split(':', 1)[0]
    sources = [platforms[value[0]]['build_receipt']['published_index_image']
               for value in PLATFORMS.values()]
    for source in sources:
        if (source.split('@', 1)[0] != repository or '@' not in source
                or not DIGEST.fullmatch(source.split('@', 1)[1])):
            raise PreparationError('Original native startup indexes must remain in the intended Hotel repository.')
    output.mkdir(parents=True)
    # Keep intent plus native input identities if a subsequent registry write/verification fails.
    write_receipt(output / 'publication-attempt.json', {
        'candidate_only': True, 'application_start_allowed': False, 'database_executed': False,
        'runtime_qualified': False, 'measurement_qualified': False,
        'public_release': False, 'tag': tagged, 'original_native_indexes': sources,
        'baseline_fingerprint': reused.fingerprint,
    })
    metadata_path = output / 'app-index-metadata.json'
    subprocess.run(['docker', 'buildx', 'imagetools', 'create', '--tag', tagged,
                    '--metadata-file', str(metadata_path), *sources], check=True, timeout=600)
    descriptor = _read_document(metadata_path, 'startup index assembly metadata')['containerimage.descriptor']
    digest = descriptor['digest']
    if not isinstance(digest, str) or not DIGEST.fullmatch(digest) or type(descriptor.get('size')) is not int:
        raise PreparationError('Guarded index assembly returned an invalid descriptor.')
    index = repository + '@' + digest
    raw = inspect_manifest(index)
    if digest_bytes(raw) != digest or len(raw) != descriptor['size']:
        raise PreparationError('Published startup index does not match exact assembly metadata.')
    identity = _identity(root)
    evidence = {
        **identity, 'candidate_schema': PUBLICATION_SCHEMA, 'upstream_revision': UPSTREAM_REVISION,
        'baseline_fingerprint': reused.fingerprint,
        'app': {'index_image': index, 'platforms': {
            target: {key: record[key] for key in ('image', 'build_receipt', 'smoke_receipt')}
            for target, record in platforms.items()
        }},
    }
    lock = create_startup_hotel_lock(
        preparation, upstream, evidence, reused, inspector=inspect_manifest, blob_inspector=inspect_blob,
    )
    write_receipt(output / 'publication.json', evidence)
    write_receipt(output / 'candidate-image-lock.json', lock)
    return lock


def anonymous_record(architecture: str, root: Path, upstream: Path,
                     baseline_evidence: Path, publication: Path) -> dict:
    from app.deathstarbench_hotel_startup_artifacts import adopt_startup_hotel_artifacts
    from scripts.smoke_deathstarbench_hotel_startup_images import smoke_startup_hotel_image
    target, machine = _native(architecture)
    config_value = os.environ.get('DOCKER_CONFIG')
    if (any(os.environ.get(key) for key in ('REGISTRY_TOKEN', 'REGISTRY_USERNAME', 'DOCKER_AUTH_CONFIG'))
            or not config_value):
        raise PreparationError('Anonymous startup Hotel proof requires explicit empty Docker config and no credentials.')
    config = Path(config_value)
    if not config.is_absolute() or config.is_symlink() or not config.is_dir() or any(config.iterdir()):
        raise PreparationError('Anonymous Docker config must be a fresh empty real absolute directory.')
    validate_startup_preparation(root / 'startup', upstream)
    reused = baseline(root, baseline_evidence)
    preparation, _ = prepared(root)
    adopted = adopt_startup_hotel_artifacts(
        publication / 'candidate-image-lock.json', publication / 'publication.json',
        preparation=preparation, upstream=upstream, baseline=reused,
        inspector=inspect_manifest, blob_inspector=inspect_blob,
    )
    # Use the factory's detached immutable image, never reopen mutable evidence
    # after adoption to choose a different executable at the last moment.
    selected = adopted.app['platforms'][target]
    subprocess.run(['docker', 'pull', '--platform', target, selected['image']], check=True, timeout=600)
    smoke = smoke_startup_hotel_image(selected['image'], architecture=machine, preparation=preparation)
    if (smoke['dataset_fingerprint'] != selected['smoke_receipt']['dataset_fingerprint']
            or shared_smoke._canonical(smoke['startup_attestations']) !=
            shared_smoke._canonical(selected['smoke_receipt']['startup_attestations'])):
        raise PreparationError('Anonymous native smoke differs from adopted compiled dataset/startup identity.')
    return {
        'schema_version': 1, 'candidate_schema': 'hotel-guarded-startup-anonymous-native-v1',
        'candidate_only': True, 'platform': target, 'image': selected['image'],
        'baseline_fingerprint': reused.fingerprint, 'artifact_adoption': adopted.metadata(),
        'empty_docker_config': True, 'registry_credentials_supplied': False,
        'app_native_anonymous_pull_and_no_database_smoke': True, 'smoke_receipt': smoke,
        'scope': 'fresh-empty-docker-client-config-exact-native-app-pull-and-no-database-smoke-only',
        'all_layers_independently_downloaded': False,
        'proxy_or_host_credential_independence_qualified': False,
        'anonymous_pull_qualified': False, 'support_or_driver_layers_pulled': False,
        'database_executed': False, 'application_start_allowed': False,
        'initialization_executed': False, 'application_started': False, 'application_ready': False,
        'initializer_authorized': False, 'runtime_authorized': False, 'qualification_authority': False,
        'dataset_qualified': False, 'cloud_qualified': False, 'released': False,
        'runtime_qualified': False, 'measurement_qualified': False, 'public_release': False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('plan')
    preparing = commands.add_parser('prepare')
    recording = commands.add_parser('record-native')
    baselining = commands.add_parser('baseline')
    assembling = commands.add_parser('assemble')
    anonymous = commands.add_parser('anonymous-native')
    for command in (preparing, recording, baselining, assembling, anonymous):
        command.add_argument('--root', type=Path, required=True)
    for command in (preparing, recording, baselining, assembling, anonymous):
        command.add_argument('--upstream', type=Path, required=True)
    preparing.add_argument('--luasocket-rock', type=Path, required=True)
    for command in (baselining, assembling, anonymous):
        command.add_argument('--baseline', type=Path, required=True)
    for command in (recording, baselining, assembling, anonymous):
        command.add_argument('--output', type=Path, required=True)
    for command in (recording, anonymous):
        command.add_argument('--architecture', choices=tuple(PLATFORMS), required=True)
    recording.add_argument('--app-index', required=True)
    assembling.add_argument('--records', nargs=2, type=Path, required=True)
    for name in ('owner', 'run-id', 'attempt'):
        assembling.add_argument('--' + name, required=True)
    anonymous.add_argument('--publication', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'plan':
        print(json.dumps(plan(), separators=(',', ':')))
    elif args.command == 'prepare':
        prepare(args.upstream, args.luasocket_rock, args.root)
    elif args.command == 'record-native':
        write_receipt(args.output, native_record(args.architecture, args.root, args.upstream, args.app_index))
    elif args.command == 'baseline':
        validate_startup_preparation(args.root / 'startup', args.upstream)
        write_receipt(args.output, baseline(args.root, args.baseline).metadata())
    elif args.command == 'assemble':
        assemble(args.root, args.upstream, [_read_document(path, 'startup native record') for path in args.records],
                 args.baseline, args.owner, args.run_id, args.attempt, args.output)
    else:
        write_receipt(args.output, anonymous_record(
            args.architecture, args.root, args.upstream, args.baseline, args.publication,
        ))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (PreparationError, ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f'startup-hotel-publication: {error}', file=sys.stderr)
        raise SystemExit(1)
