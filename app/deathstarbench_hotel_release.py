"""Checked-in runtime inputs for the experimental synthetic Hotel benchmark.

This small deployment lock reuses published immutable images. It is separate
from historical qualification receipts and the original private controller.
It requires no local source checkout or expired workflow artifact at run time.
"""
from __future__ import annotations

import json
from pathlib import Path
import re

from .deathstarbench_candidate_workload import validate_candidate_images

IMAGE_LOCK_FILENAME = 'deathstarbench-hotel-image-lock.json'
CHECKED_IN_IMAGE_LOCK = Path(__file__).resolve().parents[1] / 'docs/qualification/deathstarbench-hotel-ui-images-v1.json'
_IMAGE = re.compile(r'[a-z0-9][a-z0-9./_-]*@sha256:[a-f0-9]{64}\Z')
_SHA = re.compile(r'[a-f0-9]{64}\Z')


def validate_hotel_runtime_lock(value) -> dict:
    if not isinstance(value, dict):
        raise ValueError('Hotel runtime lock must be an object.')
    lock = json.loads(json.dumps(value, allow_nan=False))
    if (type(lock.get('schema_version')) is not int or lock['schema_version'] != 1
            or lock.get('workload_id') != 'hotel_reservation'
            or lock.get('experimental') is not True
            or lock.get('runtime_revision') != 'hotel-synthetic-k3s-v1'
            or lock.get('measurement_revision') != 'hotel-synthetic-mixed-k3s-v1'):
        raise ValueError('Hotel runtime identity is invalid.')
    validate_candidate_images('hotel_reservation', lock.get('candidate_images'))
    for field in ('upstream_revision', 'workload_revision'):
        if lock.get(field) != lock['candidate_images'][field]:
            raise ValueError('Hotel source and workload revisions differ.')
    platforms = lock.get('application_images')
    if not isinstance(platforms, dict) or set(platforms) != {'linux/amd64', 'linux/arm64'}:
        raise ValueError('Hotel requires both guarded application architectures.')
    references = [*platforms.values(), lock.get('load_generator_image')]
    if any(not isinstance(ref, str) or not _IMAGE.fullmatch(ref) or ref.endswith('0' * 64)
           for ref in references) or len(set(references)) != len(references):
        raise ValueError('Hotel requires distinct immutable image digests.')
    for field in ('initializer_sha256', 'load_script_sha256', 'load_driver_context_sha256',
                  'dataset_manifest_sha256', 'wrk_binary_sha256'):
        if not isinstance(lock.get(field), str) or not _SHA.fullmatch(lock[field]):
            raise ValueError('Hotel runtime input digest is invalid: ' + field)
    if lock.get('dataset_fingerprint') != 'sha256:' + lock['dataset_manifest_sha256']:
        raise ValueError('Hotel dataset fingerprints differ.')
    for field in ('dataset_revision', 'load_driver_revision'):
        if not isinstance(lock.get(field), str) or not lock[field].strip():
            raise ValueError('Hotel runtime revision is missing: ' + field)
    return lock


def load_hotel_runtime_lock(source: Path = CHECKED_IN_IMAGE_LOCK) -> dict:
    return validate_hotel_runtime_lock(json.loads(Path(source).read_text()))
