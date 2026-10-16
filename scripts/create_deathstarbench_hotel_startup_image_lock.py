#!/usr/bin/env python3
"""Distinct inert startup-safe Hotel artifact evidence, never runtime authority.

Fresh source reconstruction and raw registry manifest/config bytes are checked
on every call. Native smoke is an observation, not signed build provenance,
database verification, service readiness or permission to start applications.
"""

from __future__ import annotations

from functools import lru_cache, wraps
from pathlib import Path

from app.deathstarbench_candidate_artifacts import CandidateArtifactBinding, CandidateArtifactError
from scripts import create_deathstarbench_candidate_image_lock as legacy
from scripts import create_deathstarbench_hotel_initialization_image_lock as guarded
from scripts import prepare_deathstarbench_hotel_startup as preparation_module
from scripts.deathstarbench_artifact_common import PreparationError
from scripts.deathstarbench_hotel_seed_source import GENERATOR_REVISION


SCHEMA = 'hotel-guarded-startup-publication-v1'
NATIVE_SCHEMA = 'hotel-guarded-startup-native-publication-v1'
BUILD_SCHEMA = 'hotel-guarded-startup-native-build-v1'
LOCK_SCHEMA = 'hotel-guarded-startup-artifact-lock-v1'
WORKLOAD = 'hotel_reservation'
RECIPE_REVISION = preparation_module.RECIPE_REVISION
DATASET_REVISION = preparation_module.DATASET_REVISION
POLICY_REVISION = preparation_module.POLICY_REVISION
PLATFORMS = legacy.PLATFORMS
FALSE_FLAGS = tuple(sorted((set(preparation_module.FALSE_FLAGS) - {'images_built', 'images_published'}) | {
    'initializer_authorized', 'runtime_authorized', 'anonymous_pull_qualified',
    'application_started', 'application_ready', 'global_readonly_verification_completed',
    'database_execution_verified',
}))


class StartupHotelImageLockError(ValueError):
    """Startup source, native, baseline or immutable registry bytes differ."""


def _require(condition, message):
    if not condition:
        raise StartupHotelImageLockError(message)


def _equal(value, expected, label):
    try:
        guarded._equal(value, expected, label)
    except guarded.GuardedHotelImageLockError as exc:
        raise StartupHotelImageLockError(str(exc)) from exc


def _object(value, label, keys=None):
    _require(type(value) is dict, label + ' must be a JSON object.')
    if keys is not None:
        _require(set(value) == set(keys), label + ' fields differ.')
    return value


def _canonical(value):
    try:
        return guarded._canonical(value)
    except guarded.GuardedHotelImageLockError as exc:
        raise StartupHotelImageLockError(str(exc)) from exc


def _detached(value):
    import json
    return json.loads(_canonical(value))


def _digest(value):
    import hashlib
    return hashlib.sha256(_canonical(value)).hexdigest()


def _guard_errors(function):
    @wraps(function)
    def validated(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (legacy.CandidateImageLockError, guarded.GuardedHotelImageLockError,
                CandidateArtifactError, PreparationError, OSError, UnicodeError,
                TypeError, KeyError, ValueError, RecursionError) as exc:
            if isinstance(exc, StartupHotelImageLockError):
                raise
            raise StartupHotelImageLockError('Complete startup Hotel artifact validation failed: ' + str(exc)) from exc
    return validated


def _preparation(preparation: Path, upstream: Path):
    from scripts.deathstarbench_hotel_startup_artifact_common import validate_startup_preparation
    return validate_startup_preparation(Path(preparation), Path(upstream))


def _identity(prepared, baseline):
    return {
        'schema_version': 1, 'workload_id': WORKLOAD,
        'recipe_revision': RECIPE_REVISION, 'dataset_revision': DATASET_REVISION,
        'policy_revision': POLICY_REVISION, 'generator_revision': GENERATOR_REVISION,
        'source_fingerprint': prepared['source_fingerprint'],
        'preparation_manifest_sha256': prepared['manifest_sha256'],
        'config_sha256': prepared['config_sha256'],
    }


def _validate_baseline(baseline, *, inspector, blob_inspector):
    # Reuse only the exact original factory capability and fixed x86 support/
    # driver bytes. Guarded/startup bindings or serialized JSON are not inputs.
    return guarded._validate_baseline(baseline, inspector=inspector, blob_inspector=blob_inspector)


def _app_repository(reference, baseline):
    guarded._app_repository(reference, baseline)


def _native_image(supplied, platform, prepared, preparation, baseline, *, inspector, blob_inspector):
    _object(supplied, 'startup platform evidence', {'image', 'build_receipt', 'smoke_receipt'})
    _app_repository(supplied['image'], baseline)
    receipt = _object(supplied['build_receipt'], 'startup native build', {
        *_identity(prepared, baseline), 'candidate_schema', 'context_name', 'platform',
        'context_sha256', 'recipe_sha256', 'runner_architecture', 'qemu_used', 'published_index_image',
    })
    for name, expected in {
        **_identity(prepared, baseline), 'candidate_schema': BUILD_SCHEMA,
        'context_name': 'app', 'platform': platform,
        'context_sha256': prepared['context_sha256'], 'recipe_sha256': prepared['recipe_sha256'],
        'runner_architecture': legacy.ARCHITECTURES[platform], 'qemu_used': False,
    }.items():
        _equal(receipt[name], expected, 'startup native build ' + name)
    _app_repository(receipt['published_index_image'], baseline)
    image = legacy.verify_image_index(
        receipt['published_index_image'], {platform: supplied['image']},
        inspector=inspector, blob_inspector=blob_inspector,
    )['platforms'][platform]
    from scripts import smoke_deathstarbench_hotel_startup_images as smoke_validation
    repository, _ = legacy._reference(image['image'], 'startup native image')
    actual_config = legacy._json_bytes(
        blob_inspector(repository, image['config_digest']), 'startup native image configuration',
        digest=image['config_digest'],
    )
    actual_runtime = smoke_validation._runtime_config(actual_config.get('config'))
    smoke = smoke_validation.validate_startup_hotel_smoke_receipt(
        supplied['smoke_receipt'], image=image['image'], platform=platform,
        config_digest=image['config_digest'], preparation=Path(preparation),
    )
    _equal(smoke['image_config']['runtime'], actual_runtime,
           'startup smoke and fresh raw registry runtime configuration')
    # Bind the smoke's later local preparation observation to the independent
    # reconstruction captured before registry I/O, not merely the seed hash.
    # A self-consistently rehashed source change must not cross that interval.
    for name, expected in {
        **{key: value for key, value in _identity(prepared, baseline).items()
           if key not in ('schema_version', 'workload_id', 'generator_revision')},
        'context_sha256': prepared['context_sha256'],
    }.items():
        _equal(smoke[name], expected, 'startup native smoke reconstructed ' + name)
    image['native_build_receipt'] = _detached(receipt)
    image['smoke_receipt'] = _detached(smoke)
    return image


def _native_records(prepared, preparation, records, baseline, *, inspector, blob_inspector):
    _require(type(records) is list and len(records) == 2, 'Both native startup Hotel records are required.')
    platforms = {}
    for record in records:
        _object(record, 'startup native publication', {
            *_identity(prepared, baseline), 'candidate_schema', 'platform', 'image', 'build_receipt', 'smoke_receipt',
        })
        platform = record['platform']
        _require(type(platform) is str and platform in PLATFORMS and platform not in platforms,
                 'Duplicate or unsupported native startup platform.')
        for name, expected in {**_identity(prepared, baseline), 'candidate_schema': NATIVE_SCHEMA}.items():
            _equal(record[name], expected, 'startup native publication ' + name)
        platforms[platform] = _native_image(
            {name: record[name] for name in ('image', 'build_receipt', 'smoke_receipt')},
            platform, prepared, preparation, baseline, inspector=inspector, blob_inspector=blob_inspector,
        )
    for field in ('dataset_attestation', 'startup_attestations'):
        _equal(platforms['linux/amd64']['smoke_receipt'][field],
               platforms['linux/arm64']['smoke_receipt'][field],
               'exact source-bound native startup ' + field + ' equality')
    return platforms


@_guard_errors
def preflight_native_startup_hotel(preparation: Path, upstream: Path, records: list,
                                   baseline: CandidateArtifactBinding, *, inspector, blob_inspector) -> dict:
    """Completely check both native records before any assembled-index write."""
    _canonical(records)
    prepared = _preparation(preparation, upstream)
    inspector, blob_inspector = lru_cache(maxsize=128)(inspector), lru_cache(maxsize=128)(blob_inspector)
    _validate_baseline(baseline, inspector=inspector, blob_inspector=blob_inspector)
    return _native_records(prepared, preparation, records, baseline,
                           inspector=inspector, blob_inspector=blob_inspector)


@_guard_errors
def create_startup_hotel_lock(preparation: Path, upstream: Path, publication: dict,
                              baseline: CandidateArtifactBinding, *, inspector, blob_inspector) -> dict:
    """Return inert artifact evidence only after source/native/registry checks."""
    _canonical(publication)
    prepared = _preparation(preparation, upstream)  # Must precede ANY registry I/O.
    _require(type(baseline) is CandidateArtifactBinding,
             'A freshly adopted original Hotel CandidateArtifactBinding is required.')
    baseline.__post_init__()
    _object(publication, 'startup Hotel publication', {
        *_identity(prepared, baseline), 'candidate_schema', 'upstream_revision', 'baseline_fingerprint', 'app',
    })
    for name, expected in {
        **_identity(prepared, baseline), 'candidate_schema': SCHEMA,
        'upstream_revision': legacy.UPSTREAM_REVISION, 'baseline_fingerprint': baseline.fingerprint,
    }.items():
        _equal(publication[name], expected, 'startup Hotel publication ' + name)
    inspector, blob_inspector = lru_cache(maxsize=128)(inspector), lru_cache(maxsize=128)(blob_inspector)
    reused = _validate_baseline(baseline, inspector=inspector, blob_inspector=blob_inspector)
    app = _object(publication['app'], 'startup Hotel app', {'index_image', 'platforms'})
    _app_repository(app['index_image'], baseline)
    _object(app['platforms'], 'startup Hotel app platforms', set(PLATFORMS))
    records = [{**_identity(prepared, baseline), 'candidate_schema': NATIVE_SCHEMA,
                'platform': platform, **app['platforms'][platform]} for platform in PLATFORMS]
    native = _native_records(prepared, preparation, records, baseline,
                             inspector=inspector, blob_inspector=blob_inspector)
    assembled = legacy.verify_image_index(
        app['index_image'], {platform: native[platform]['image'] for platform in PLATFORMS},
        inspector=inspector, blob_inspector=blob_inspector,
    )
    for platform in PLATFORMS:
        _equal(assembled['platforms'][platform],
               {name: native[platform][name] for name in assembled['platforms'][platform]},
               'startup assembled membership and native config ' + platform)
    return {
        **_identity(prepared, baseline), 'candidate_schema': LOCK_SCHEMA,
        'candidate_only': True, 'upstream_revision': legacy.UPSTREAM_REVISION,
        'publication_evidence_sha256': _digest(publication),
        'preparation': {name: prepared[name] for name in (
            'manifest_sha256', 'prepared_tree_sha256', 'context_sha256', 'recipe_sha256',
            'config_sha256', 'source_fingerprint', 'recipe_revision',
        )},
        'baseline': reused, 'app': {'index_image': assembled['index_image'], 'platforms': native},
        'dataset_attestation': _detached(native['linux/amd64']['smoke_receipt']['dataset_attestation']),
        'startup_attestations': _detached(native['linux/amd64']['smoke_receipt']['startup_attestations']),
        **{name: False for name in FALSE_FLAGS},
        'scope': 'source-and-native-startup-image-evidence-only-no-database-or-runtime-authority',
        'limitations': [
            'Native receipts are observations, not signed provenance or byte-reproducible rebuild proof.',
            'No database initialization, live application service, readiness check or benchmark was executed.',
            'Original support and driver bytes are reused, not rebuilt or requalified for this recipe.',
            'Manifest/config access is not anonymous layer-pull, cloud, recovery or public release qualification.',
        ],
    }
