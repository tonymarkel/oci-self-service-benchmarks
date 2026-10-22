"""Published Media images and the synthetic compose-review dataset contract."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

from .deathstarbench_candidate_workload import validate_candidate_images


IMAGE_LOCK_FILENAME = 'deathstarbench-media-image-lock.json'
CHECKED_IN_IMAGE_LOCK = Path(__file__).resolve().parents[1] / 'docs/qualification/deathstarbench-media-ui-images-v1.json'
INITIALIZER_PATH = Path(__file__).resolve().parent / 'guests/media_seed.py'
MOVIE_TITLES_PATH = Path(__file__).resolve().parent / 'manifests/deathstarbench/media-synthetic-v1/movie-titles.json'
DATASET_MANIFEST = {
    'schema_version': 1, 'workload_id': 'media_microservices',
    'revision': 'media-synthetic-users-driver-titles-v1',
    'users': {'count': 1000, 'first_index': 1, 'username_prefix': 'username_',
              'password_prefix': 'password_', 'first_name_prefix': 'first_name_',
              'last_name_prefix': 'last_name_'},
    'movies': {'count': 995, 'first_index': 1, 'movie_id_prefix': 'movie_id_',
               'id_mapping': 'first-title-index', 'source_title_count': 1000},
    'tmdb_records': 0, 'initial_reviews': 0, 'canary_reviews': 1,
}
_IMAGE = re.compile(r'[a-z0-9][a-z0-9./_-]*@sha256:[a-f0-9]{64}\Z')
_SHA = re.compile(r'[a-f0-9]{64}\Z')


def dataset_manifest_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _initializer_sha256():
    try:
        return hashlib.sha256(INITIALIZER_PATH.read_bytes()).hexdigest()
    except OSError as exc:
        raise ValueError('Media initializer is unavailable.') from exc


def dataset_manifest():
    try:
        payload = MOVIE_TITLES_PATH.read_bytes()
        if hashlib.sha256(payload).hexdigest() != 'f1b5763e894e6fd96fab2c358aaf6cbd0b3faa92a61b8a5347edb70d85c866ac':
            raise ValueError('Media driver title list differs from the published source.')
        titles = json.loads(payload)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise ValueError('Media driver title list is unavailable or invalid.') from exc
    manifest = json.loads(json.dumps(DATASET_MANIFEST))
    manifest['movies']['titles'] = titles
    return manifest


def validate_media_runtime_lock(value) -> dict:
    if not isinstance(value, dict):
        raise ValueError('Media runtime lock must be an object.')
    lock = json.loads(json.dumps(value, allow_nan=False))
    if (type(lock.get('schema_version')) is not int or lock['schema_version'] != 1
            or lock.get('workload_id') != 'media_microservices' or lock.get('experimental') is not True
            or lock.get('runtime_revision') != 'media-synthetic-k3s-v1'
            or lock.get('measurement_revision') != 'media-synthetic-compose-k3s-v1'):
        raise ValueError('Media runtime identity is invalid.')
    validate_candidate_images('media_microservices', lock.get('candidate_images'))
    for field in ('upstream_revision', 'workload_revision'):
        if lock.get(field) != lock['candidate_images'][field]:
            raise ValueError('Media source and workload revisions differ.')
    image = lock.get('load_generator_image')
    if not isinstance(image, str) or not _IMAGE.fullmatch(image) or image.endswith('0' * 64):
        raise ValueError('Media requires an immutable load-generator image digest.')
    for field in ('initializer_sha256', 'dataset_manifest_sha256', 'load_script_sha256',
                  'load_driver_context_sha256', 'wrk_binary_sha256'):
        if not isinstance(lock.get(field), str) or not _SHA.fullmatch(lock[field]):
            raise ValueError('Media runtime input digest is invalid: ' + field)
    expected_dataset = dataset_manifest()
    if (lock.get('dataset_manifest') != expected_dataset
            or lock.get('dataset_revision') != DATASET_MANIFEST['revision']
            or lock['dataset_manifest_sha256'] != dataset_manifest_digest(expected_dataset)
            or lock.get('dataset_fingerprint') != 'sha256:' + lock['dataset_manifest_sha256']):
        raise ValueError('Media synthetic dataset identity differs.')
    if lock['initializer_sha256'] != _initializer_sha256():
        raise ValueError('Media initializer bytes differ from the run image lock.')
    if not isinstance(lock.get('load_driver_revision'), str) or not lock['load_driver_revision'].strip():
        raise ValueError('Media load-driver revision is missing.')
    return lock


def load_media_runtime_lock(source: Path = CHECKED_IN_IMAGE_LOCK) -> dict:
    return validate_media_runtime_lock(json.loads(Path(source).read_text()))
