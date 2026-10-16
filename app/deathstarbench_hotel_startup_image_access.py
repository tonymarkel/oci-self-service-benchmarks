"""Fresh anonymous full-byte access for the distinct startup Hotel artifacts.

Only the unchanged guarded image reader's transport and OCI descriptor helpers
are shared. Its artifact context, qualification factory and capability proof
are not accepted or adapted. This module neither executes images nor creates
infrastructure, and saved observations never become startup/runtime authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
import re
import threading
import weakref

from . import deathstarbench_hotel_image_access as shared
from . import deathstarbench_hotel_startup_artifacts as adoption


ACCESS_SCHEMA = 'hotel-guarded-startup-image-access-v1'
ACCESS_METHOD = shared.ACCESS_METHOD
_SUPPORT = frozenset({'consul', 'jaeger', 'memcached', 'mongodb'})
_PLATFORMS = ('linux/amd64', 'linux/arm64')
_TRUST_ENVIRONMENT = (
    'SSL_CERT_FILE', 'SSL_CERT_DIR', 'REQUESTS_CA_BUNDLE', 'CURL_CA_BUNDLE',
    'PYTHONHTTPSVERIFY', 'SSLKEYLOGFILE', 'OPENSSL_CONF',
)
_CLOSED_FLAGS = tuple(sorted(set(adoption.validation.FALSE_FLAGS) | set(shared._CLOSED_FLAGS) | {
    'application_started', 'application_ready', 'global_readonly_verification_completed',
    'database_execution_verified',
}))


class StartupHotelImageAccessError(ValueError):
    """Exact, complete anonymous startup Hotel access could not be proven."""


def _require(condition, message):
    if not condition:
        raise StartupHotelImageAccessError(message)


def _trust_environment():
    _require(not any(os.environ.get(name) for name in _TRUST_ENVIRONMENT),
             'Startup Hotel byte access requires absent TLS trust and key-log overrides.')


def _context(artifacts):
    """Detach the original startup proof's complete source and image identity."""
    _require(type(artifacts) is adoption.StartupHotelArtifactBinding,
             'An exact adopted startup Hotel artifact binding is required, not a legacy binding or summary.')
    try:
        artifacts.__post_init__()
        metadata = artifacts.metadata()
        _require(metadata['candidate_schema'] == adoption.ADOPTION_SCHEMA,
                 'The adopted artifact schema is not the distinct startup Hotel recipe.')
        for name, expected in (
            ('recipe_revision', adoption.validation.RECIPE_REVISION),
            ('policy_revision', adoption.validation.POLICY_REVISION),
            ('dataset_revision', adoption.validation.DATASET_REVISION),
        ):
            _require(type(metadata[name]) is str and metadata[name] == expected,
                     'The adopted startup Hotel source policy differs from this qualification recipe.')
        _require(all(getattr(artifacts, name) is False and metadata[name] is False
                     for name in adoption.validation.FALSE_FLAGS),
                 'Every adopted startup Hotel execution and release gate must remain closed.')
        shared._digest(artifacts.fingerprint)
        shared._digest(metadata['source_fingerprint'])
        shared._digest(artifacts.dataset_attestation['dataset_fingerprint'])
        _require(artifacts.dataset_attestation['source_fingerprint'] == metadata['source_fingerprint'],
                 'The adopted startup Hotel dataset is crossed with another compiled source.')
        config = metadata['config_sha256']
        _require(isinstance(config, str) and re.fullmatch(r'[0-9a-f]{64}', config)
                 and config != '0' * 64, 'The adopted startup Hotel configuration digest is invalid.')
        _require(set(artifacts.app['platforms']) == set(_PLATFORMS),
                 'Both native startup Hotel application architectures are required.')
        _require(set(artifacts.reused_images) == _SUPPORT | {'load-driver'},
                 'The exact four support images and x86 load driver are required.')
        images = {}
        for name, item in [('hotel-reservation', artifacts.app), *sorted(artifacts.reused_images.items())]:
            platforms = _PLATFORMS if name == 'hotel-reservation' else ('linux/amd64',)
            _require(set(item['platforms']) == set(platforms),
                     'A startup Hotel image has a crossed platform inventory.')
            index_registry, index_repository, _ = shared._reference(item['index_image'])
            _require(index_registry == ('docker.io' if name in _SUPPORT else 'ghcr.io'),
                     'The startup Hotel image registry differs from its reviewed role.')
            for platform in platforms:
                bound = item['platforms'][platform]
                registry, repository, manifest_digest = shared._reference(bound['image'])
                _require((registry, repository) == (index_registry, index_repository)
                         and bound['manifest_digest'] == manifest_digest and bound['os'] == 'linux'
                         and bound['architecture'] == platform.split('/')[1],
                         'A startup Hotel platform image is crossed with another index or architecture.')
                shared._digest(bound['config_digest'])
                key = name + '@' + platform if name == 'hotel-reservation' else name
                images[key] = {'image': bound['image'], 'index_image': item['index_image'],
                               'platform': platform, 'config_digest': bound['config_digest']}
        _require(len(images) == 7 and len({item['image'] for item in images.values()}) == 7,
                 'Startup Hotel access requires seven distinct exact platform images.')
        return {
            'workload_id': 'hotel_reservation', 'artifact_adoption_schema': adoption.ADOPTION_SCHEMA,
            'artifact_fingerprint': artifacts.fingerprint,
            **{name: metadata[name] for name in (
                'recipe_revision', 'policy_revision', 'dataset_revision', 'generator_revision',
                'source_fingerprint', 'config_sha256',
            )},
            'dataset_fingerprint': artifacts.dataset_attestation['dataset_fingerprint'],
            'startup_attestations_fingerprint': shared._fingerprint(metadata['startup_attestations']),
            'images': dict(sorted(images.items())),
        }
    except (ValueError, KeyError, TypeError, AttributeError, RecursionError) as exc:
        if isinstance(exc, StartupHotelImageAccessError):
            raise
        raise StartupHotelImageAccessError(
            'Startup Hotel artifact identity is incomplete, changed, or crossed.') from None


@dataclass(frozen=True, slots=True)
class _AccessProof:
    metadata_json: str
    fingerprint: str
    pid: int


_PROOFS = weakref.WeakKeyDictionary()
_PROOFS_LOCK = threading.Lock()


class StartupHotelImageAccess:
    """Factory-only process-local byte capability, never execution authority."""

    __slots__ = ('_metadata_json', '_fingerprint', '__weakref__')

    def __new__(cls, *args, **kwargs):
        raise StartupHotelImageAccessError('Startup Hotel access requires fresh anonymous full-byte qualification.')

    def __setattr__(self, name, value):
        raise StartupHotelImageAccessError('Startup Hotel image access capabilities are immutable.')

    def __reduce_ex__(self, protocol):
        raise StartupHotelImageAccessError('Startup Hotel access cannot be copied or serialized as authority.')

    @property
    def fingerprint(self):
        return self._fingerprint

    def metadata(self):
        proof = _authority(self)
        return {**json.loads(proof.metadata_json), 'fingerprint': proof.fingerprint}


def _authority(value):
    _require(type(value) is StartupHotelImageAccess,
             'An original startup Hotel image access capability is required.')
    with _PROOFS_LOCK:
        proof = _PROOFS.get(value)
    _require(proof is not None and proof.pid == os.getpid(),
             'A summary, copied, or other-process startup access receipt has no authority.')
    _require(value._metadata_json == proof.metadata_json and value._fingerprint == proof.fingerprint,
             'Startup Hotel access capability fields changed after qualification.')
    return proof


def require_startup_hotel_image_access(value, artifacts):
    """Require the original process-local capability for exactly these artifacts."""
    proof = _authority(value)
    _require(json.loads(proof.metadata_json)['identity'] == _context(artifacts),
             'Startup Hotel image access is crossed with another artifact binding.')
    return value


def _stream_images(client, identity):
    """Use only the old reader's transport/descriptor primitives, never its proof."""
    selected, indexes = shared._selected_descriptors(client, identity)
    results, declared_bytes = {}, sum(item['bytes'] for item in indexes.values())
    for key, expected in identity['images'].items():
        registry, repository, manifest_digest = shared._reference(expected['image'])
        descriptor = selected[key]
        raw = client.manifest(registry, repository, manifest_digest, size=descriptor['size'])
        manifest = shared._json(raw)
        _require(type(manifest.get('schemaVersion')) is int and manifest['schemaVersion'] == 2
                 and manifest.get('mediaType') == descriptor['mediaType']
                 and set(manifest) <= {'schemaVersion', 'mediaType', 'config', 'layers', 'annotations'},
                 'Startup Hotel access requires an exact platform image manifest, not an index or artifact.')
        if 'annotations' in manifest:
            shared._annotations(manifest['annotations'])
        config = shared._descriptor(manifest.get('config'), shared._CONFIG_TYPES, shared.MAX_DOCUMENT_BYTES)
        _require(config['digest'] == expected['config_digest'],
                 'A startup Hotel image config differs from the adopted artifact.')
        layers = manifest.get('layers')
        _require(isinstance(layers, list) and 0 < len(layers) <= shared.MAX_LAYERS,
                 'The startup Hotel layer inventory is empty or exceeds its bound.')
        layers = [shared._descriptor(item, shared._LAYER_TYPES, shared.MAX_LAYER_BYTES) for item in layers]
        declared_bytes += len(raw) + config['size'] + sum(item['size'] for item in layers)
        _require(declared_bytes <= shared.MAX_TOTAL_BYTES,
                 'Declared startup Hotel image bytes exceed the aggregate safety bound.')
        configuration = shared._json(client.blob(registry, repository, config, retain=True))
        _require(configuration.get('os') == 'linux'
                 and configuration.get('architecture') == expected['platform'].split('/')[1],
                 'A startup Hotel image config differs from its exact adopted native platform.')
        rootfs = configuration.get('rootfs')
        _require(isinstance(rootfs, dict) and rootfs.get('type') == 'layers'
                 and isinstance(rootfs.get('diff_ids'), list) and len(rootfs['diff_ids']) == len(layers),
                 'A startup Hotel rootfs layer inventory differs from its platform manifest.')
        for digest in rootfs['diff_ids']:
            shared._digest(digest)
        for layer in layers:
            client.blob(registry, repository, layer)
        results[key] = {
            'image': expected['image'], 'platform': expected['platform'],
            'index_image': expected['index_image'], 'manifest': descriptor,
            'config': config, 'layers': layers,
            'compressed_layer_bytes': sum(item['size'] for item in layers),
            'all_descriptor_bodies_sha256_and_size_verified': True,
        }
    _require(set(results) == set(identity['images']) and type(client.downloaded_bytes) is int
             and client.downloaded_bytes > 0,
             'Startup Hotel access did not verify the complete positive byte inventory.')
    return indexes, results


def qualify_startup_hotel_image_access(artifacts):
    """Freshly stream every bound startup image byte with fixed anonymous policy.

    There are no caller-supplied transports, credentials, endpoints, limits or
    cached results. A partial, malformed, denied, redirected, truncated or
    changed result aborts the whole attempt without issuing a capability.
    """
    _trust_environment()
    identity = _context(artifacts)
    try:
        client = shared._AnonymousClient()
        indexes, images = _stream_images(client, identity)
    except shared.GuardedHotelImageAccessError as exc:
        # Shared transport errors are already bounded and redact URL/token/body
        # material. Keep the distinct new public exception type at this boundary.
        raise StartupHotelImageAccessError(str(exc)) from None
    _trust_environment()
    _require(identity == _context(artifacts),
             'Startup Hotel artifact identity changed during anonymous full-byte verification.')
    metadata = {
        'schema_version': 1, 'candidate_schema': ACCESS_SCHEMA, 'method': ACCESS_METHOD,
        'candidate_only': True, **dict.fromkeys(_CLOSED_FLAGS, False),
        'anonymous_all_bound_image_bytes_verified': True,
        'scope': 'exact-startup-hotel-both-app-platforms-four-x86-support-images-and-x86-driver-bytes-only',
        'identity': identity, 'indexes': indexes, 'images': images,
        'observed_at': datetime.now(timezone.utc).isoformat(), 'downloaded_bytes': client.downloaded_bytes,
        'client': {
            'registry_credentials_used': False, 'docker_config_loaded': False, 'netrc_loaded': False,
            'cookies_used': False, 'environment_proxy_credentials_used': False,
            'local_image_cache_used': False, 'file_writes_performed': False,
            'anonymous_repository_scoped_bearer_tokens': True,
            'tokens_forwarded_to_redirect_targets': False,
        },
        'limits': {
            'document_bytes': shared.MAX_DOCUMENT_BYTES, 'token_bytes': shared.MAX_TOKEN_BYTES,
            'layer_bytes': shared.MAX_LAYER_BYTES, 'total_bytes': shared.MAX_TOTAL_BYTES,
            'layers_per_image': shared.MAX_LAYERS, 'index_entries': shared.MAX_INDEX_ENTRIES,
            'socket_timeout_seconds': shared.REQUEST_TIMEOUT_SECONDS,
            'total_timeout_seconds': shared.TOTAL_TIMEOUT_SECONDS, 'redirects_per_blob': shared.MAX_REDIRECTS,
        },
        'limitations': [
            'This verifies complete compressed-byte access, not a Docker/CRI pull, binary smoke, or runtime qualification.',
            'No image, application, initializer, database, Kubernetes, or benchmark was executed.',
            'Startup image access does not authorize a legacy guarded initializer or any application startup controller.',
            'Every attempt or resume needs a fresh factory capability; saved JSON is historical evidence only.',
        ],
    }
    encoded = shared._canonical(metadata)
    value = object.__new__(StartupHotelImageAccess)
    object.__setattr__(value, '_metadata_json', encoded)
    object.__setattr__(value, '_fingerprint', shared._fingerprint(metadata))
    with _PROOFS_LOCK:
        _PROOFS[value] = _AccessProof(encoded, value._fingerprint, os.getpid())
    return value
