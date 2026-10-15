#!/usr/bin/env python3
"""Read-only, source-bound evidence for the separately initialized Hotel image.

This is not the legacy candidate runtime lock. It cannot authorize database
initialization, application startup, a runtime plan, or any public benchmark.
Native smoke receipts are CI observations, not signed build provenance.
"""

from __future__ import annotations

import hashlib
import json
from functools import lru_cache, wraps
from pathlib import Path
from typing import Any

from app.deathstarbench_candidate_artifacts import CandidateArtifactBinding, CandidateArtifactError, _strict_json_values
from app.deathstarbench_hotel_initialization import DATASET_REVISION, RECIPE_REVISION
from scripts import create_deathstarbench_candidate_image_lock as legacy
from scripts.deathstarbench_hotel_seed_source import GENERATOR_REVISION
from scripts.deathstarbench_artifact_common import PreparationError


SCHEMA = 'hotel-guarded-initialization-publication-v1'
NATIVE_SCHEMA = 'hotel-guarded-initialization-native-publication-v1'
BUILD_SCHEMA = 'hotel-guarded-initialization-native-build-v1'
LOCK_SCHEMA = 'hotel-guarded-initialization-artifact-lock-v1'
WORKLOAD = 'hotel_reservation'
PLATFORMS = legacy.PLATFORMS
MAX_DOCUMENT_BYTES = 16 * 1024 * 1024


class GuardedHotelImageLockError(ValueError):
    """Guarded source, native execution, baseline, or registry evidence differs."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise GuardedHotelImageLockError(message)


def _equal(value, expected, label):
    try:
        legacy._equal(value, expected, label)
    except legacy.CandidateImageLockError as exc:
        raise GuardedHotelImageLockError(str(exc)) from exc


def _object(value, label, keys=None):
    _require(type(value) is dict, label + ' must be a JSON object.')
    if keys is not None:
        _require(set(value) == set(keys), label + ' fields differ.')
    return value


def _canonical(value) -> bytes:
    def json_types(item):
        if type(item) is dict:
            _require(all(type(key) is str for key in item), 'Guarded JSON object keys must be strings.')
            for child in item.values():
                json_types(child)
        elif type(item) is list:
            for child in item:
                json_types(child)
        else:
            _require(item is None or type(item) in (str, bool, int, float),
                     'Guarded Hotel evidence contains a non-JSON value.')
    try:
        json_types(value)
        _strict_json_values(value)
        raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')
        _require(0 < len(raw) <= MAX_DOCUMENT_BYTES, 'Guarded Hotel evidence exceeds its bounded document size.')
        return raw
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        if isinstance(exc, GuardedHotelImageLockError):
            raise
        raise GuardedHotelImageLockError('Guarded Hotel evidence must be bounded finite UTF-8 JSON.') from exc


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _detached(value):
    return json.loads(_canonical(value))


def _guard_errors(function):
    @wraps(function)
    def guarded(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (legacy.CandidateImageLockError, CandidateArtifactError, PreparationError,
                OSError, UnicodeError, TypeError, KeyError, ValueError, RecursionError) as exc:
            if isinstance(exc, GuardedHotelImageLockError):
                raise
            raise GuardedHotelImageLockError('Complete guarded Hotel artifact validation failed: ' + str(exc)) from exc
    return guarded


def _preparation(preparation: Path, upstream: Path) -> dict:
    # Full deterministic regeneration must fail before any registry access.
    from scripts.deathstarbench_hotel_initialization_artifact_common import validate_guarded_preparation
    return validate_guarded_preparation(Path(preparation), Path(upstream))


def _identity(prepared: dict, baseline: CandidateArtifactBinding) -> dict:
    return {
        'schema_version': 1, 'workload_id': WORKLOAD,
        'recipe_revision': RECIPE_REVISION, 'dataset_revision': DATASET_REVISION,
        'generator_revision': GENERATOR_REVISION,
        'source_fingerprint': prepared['source_fingerprint'],
        'preparation_manifest_sha256': prepared['manifest_sha256'],
    }


def _validate_baseline(baseline: CandidateArtifactBinding, *, inspector, blob_inspector) -> dict:
    _require(type(baseline) is CandidateArtifactBinding,
             'A freshly adopted original Hotel CandidateArtifactBinding is required, not serialized evidence.')
    baseline.__post_init__()  # Recheck its factory proof and all immutable fields.
    _equal(baseline.workload_id, WORKLOAD, 'original Hotel baseline workload')
    metadata = baseline.metadata()
    _equal(metadata['candidate_schema'], 'candidate-runtime-artifact-adoption-v1', 'baseline adoption schema')
    for flag in ('released', 'runtime_qualified', 'measurement_qualified', 'anonymous_pull_qualified'):
        _equal(metadata[flag], False, 'baseline ' + flag)
    bindings = metadata['image_bindings']
    required = set(legacy.get_workload_profile(WORKLOAD).required_image_keys) | {'load-driver'}
    _equal(set(bindings), required, 'complete original Hotel baseline assets')
    reused = {}
    for name in sorted(required - {'hotel-reservation'}):
        value = bindings[name]
        _equal(set(value['platforms']), {'linux/amd64'}, 'fixed x86 baseline ' + name)
        verified = legacy.verify_image_index(
            value['index_image'], {'linux/amd64': value['platforms']['linux/amd64']['image']},
            inspector=inspector, blob_inspector=blob_inspector,
            allow_other_platforms=name != 'load-driver',
        )
        _equal(verified['platforms'], value['platforms'], 'fresh reused baseline bytes ' + name)
        reused[name] = _detached(value)
    return {
        'binding_fingerprint': baseline.fingerprint,
        'candidate_lock_sha256': metadata['candidate_lock_sha256'],
        'publication_document_sha256': metadata['publication_document_sha256'],
        'preparation_identity_sha256': metadata['preparation_identity_sha256'],
        'workload_preparation_manifest_sha256': metadata['workload_preparation_manifest_sha256'],
        'driver_preparation_manifest_sha256': metadata['driver_preparation_manifest_sha256'],
        'scope': 'fresh-original-adoption-and-reused-x86-registry-bytes-only-no-cloud-identity',
        'images': reused,
    }


def _app_repository(reference, baseline):
    repository, _ = legacy._reference(reference, 'guarded Hotel application')
    old = baseline.image_bindings['hotel-reservation']
    old_repository, _ = legacy._reference(old['index_image'], 'original Hotel application')
    _equal(repository, old_repository, 'dedicated existing Hotel application repository')
    _require(reference != old['index_image'] and all(reference != value['image'] for value in old['platforms'].values()),
             'The original Hotel application image cannot be relabeled as the guarded recipe.')


def _native_image(supplied, platform, prepared, preparation, baseline, *, inspector, blob_inspector):
    _object(supplied, 'guarded platform evidence', {'image', 'build_receipt', 'smoke_receipt'})
    _app_repository(supplied['image'], baseline)
    receipt = _object(supplied['build_receipt'], 'guarded native build', {
        'schema_version', 'candidate_schema', 'workload_id', 'context_name', 'platform',
        'recipe_revision', 'dataset_revision', 'generator_revision', 'source_fingerprint',
        'preparation_manifest_sha256', 'context_sha256', 'recipe_sha256',
        'runner_architecture', 'qemu_used', 'published_index_image',
    })
    for name, expected in {
        **_identity(prepared, baseline), 'candidate_schema': BUILD_SCHEMA,
        'context_name': 'app', 'platform': platform,
        'context_sha256': prepared['context_sha256'], 'recipe_sha256': prepared['recipe_sha256'],
        'runner_architecture': legacy.ARCHITECTURES[platform], 'qemu_used': False,
    }.items():
        _equal(receipt.get(name), expected, 'guarded native build ' + name)
    _app_repository(receipt['published_index_image'], baseline)
    image = legacy.verify_image_index(
        receipt['published_index_image'], {platform: supplied['image']},
        inspector=inspector, blob_inspector=blob_inspector,
    )['platforms'][platform]
    from scripts import smoke_deathstarbench_hotel_initialization_images as smoke_validation
    repository, _ = legacy._reference(image['image'], 'guarded native image')
    actual_config = legacy._json_bytes(
        blob_inspector(repository, image['config_digest']), 'guarded native image configuration',
        digest=image['config_digest'],
    )
    actual_runtime = smoke_validation._runtime_config(actual_config.get('config'))
    smoke = smoke_validation.validate_guarded_hotel_smoke_receipt(
        supplied['smoke_receipt'], image=image['image'], platform=platform,
        config_digest=image['config_digest'], preparation=Path(preparation),
    )
    _equal(smoke['image_config']['runtime'], actual_runtime,
           'native smoke runtime configuration and fresh raw registry configuration')
    _equal(smoke['source_fingerprint'], prepared['source_fingerprint'], 'native smoke source identity')
    image['native_build_receipt'] = _detached(receipt)
    image['smoke_receipt'] = _detached(smoke)
    return image


def _native_records(prepared, preparation, records, baseline, *, inspector, blob_inspector):
    _require(type(records) is list and len(records) == 2, 'Both native guarded Hotel records are required.')
    platforms = {}
    for record in records:
        _object(record, 'guarded native publication', {
            'schema_version', 'candidate_schema', 'workload_id', 'platform', 'recipe_revision',
            'dataset_revision', 'generator_revision', 'source_fingerprint',
            'preparation_manifest_sha256', 'image', 'build_receipt', 'smoke_receipt',
        })
        platform = record['platform']
        _require(platform in PLATFORMS and platform not in platforms, 'Duplicate or unsupported native Hotel platform.')
        for name, expected in {**_identity(prepared, baseline), 'candidate_schema': NATIVE_SCHEMA}.items():
            _equal(record[name], expected, 'guarded native publication ' + name)
        platforms[platform] = _native_image(
            {name: record[name] for name in ('image', 'build_receipt', 'smoke_receipt')},
            platform, prepared, preparation, baseline, inspector=inspector, blob_inspector=blob_inspector,
        )
    _equal(platforms['linux/amd64']['smoke_receipt']['dataset_attestation'],
           platforms['linux/arm64']['smoke_receipt']['dataset_attestation'],
           'exact source-bound native dataset equality (not rounded comparison results)')
    return platforms


@_guard_errors
def preflight_native_guarded_hotel(preparation: Path, upstream: Path, records: list,
                                    baseline: CandidateArtifactBinding, *, inspector, blob_inspector) -> dict:
    """Fully check two native records before the assembled index is published."""
    _canonical(records)
    prepared = _preparation(preparation, upstream)
    inspector, blob_inspector = lru_cache(maxsize=128)(inspector), lru_cache(maxsize=128)(blob_inspector)
    _validate_baseline(baseline, inspector=inspector, blob_inspector=blob_inspector)
    return _native_records(prepared, preparation, records, baseline, inspector=inspector, blob_inspector=blob_inspector)


@_guard_errors
def create_guarded_hotel_lock(preparation: Path, upstream: Path, publication: dict,
                              baseline: CandidateArtifactBinding, *, inspector, blob_inspector) -> dict:
    """Return inert evidence after complete source, native, and byte checks."""
    _canonical(publication)
    prepared = _preparation(preparation, upstream)
    _require(type(baseline) is CandidateArtifactBinding,
             'A freshly adopted original Hotel CandidateArtifactBinding is required, not serialized evidence.')
    baseline.__post_init__()
    _object(publication, 'guarded Hotel publication', {
        'schema_version', 'candidate_schema', 'workload_id', 'upstream_revision',
        'recipe_revision', 'dataset_revision', 'generator_revision', 'source_fingerprint',
        'preparation_manifest_sha256', 'baseline_fingerprint', 'app',
    })
    for name, expected in {
        **_identity(prepared, baseline), 'candidate_schema': SCHEMA,
        'upstream_revision': legacy.UPSTREAM_REVISION, 'baseline_fingerprint': baseline.fingerprint,
    }.items():
        _equal(publication[name], expected, 'guarded Hotel publication ' + name)
    inspector, blob_inspector = lru_cache(maxsize=128)(inspector), lru_cache(maxsize=128)(blob_inspector)
    reused = _validate_baseline(baseline, inspector=inspector, blob_inspector=blob_inspector)
    app = _object(publication['app'], 'guarded Hotel app', {'index_image', 'platforms'})
    _app_repository(app['index_image'], baseline)
    _object(app['platforms'], 'guarded Hotel app platforms', set(PLATFORMS))
    records = [{**_identity(prepared, baseline), 'candidate_schema': NATIVE_SCHEMA,
                'platform': platform, **app['platforms'][platform]} for platform in PLATFORMS]
    native = _native_records(prepared, preparation, records, baseline, inspector=inspector, blob_inspector=blob_inspector)
    assembled = legacy.verify_image_index(
        app['index_image'], {platform: native[platform]['image'] for platform in PLATFORMS},
        inspector=inspector, blob_inspector=blob_inspector,
    )
    for platform in PLATFORMS:
        _equal(assembled['platforms'][platform], {name: native[platform][name] for name in assembled['platforms'][platform]},
               'assembled membership and native config ' + platform)
    return {
        **_identity(prepared, baseline), 'candidate_schema': LOCK_SCHEMA,
        'candidate_only': True, 'released': False, 'upstream_revision': legacy.UPSTREAM_REVISION,
        'publication_evidence_sha256': _digest(publication),
        'preparation': {name: prepared[name] for name in (
            'manifest_sha256', 'prepared_tree_sha256', 'context_sha256', 'recipe_sha256',
            'config_sha256', 'source_fingerprint', 'recipe_revision',
        )},
        'baseline': reused,
        'app': {'index_image': assembled['index_image'], 'platforms': native},
        'dataset_attestation': _detached(native['linux/amd64']['smoke_receipt']['dataset_attestation']),
        **{name: False for name in (
            'initializer_authorized', 'runtime_authorized', 'initialization_executed', 'database_executed',
            'application_start_allowed', 'dataset_qualified', 'runtime_qualified',
            'measurement_qualified', 'cloud_qualified', 'public_release', 'anonymous_pull_qualified',
        )},
        'scope': 'source-and-native-image-evidence-only-no-database-or-runtime-authority',
        'limitations': [
            'Native receipts are CI observations, not signed provenance or byte-reproducible rebuild proof.',
            'No database initializer, controller transition, application service, or benchmark was executed.',
            'Original support and driver bytes are reused, not rebuilt or requalified for the new runtime.',
            'Manifest/config access is not anonymous layer-pull, cloud, recovery, or public release qualification.',
        ],
    }
