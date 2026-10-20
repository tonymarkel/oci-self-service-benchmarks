"""Fresh anonymous full-byte access to the seven bound guarded Hotel images.

This is a separate artifact capability, not a runtime/initializer capability.
No Docker daemon, registry credentials, environment proxies, netrc, cookies,
image cache, file writes or container execution are used. This module creates
no cloud resources. An explicit private SSH TCP-egress context may change the
network path without changing HTTPS validation; its owner records that path
and independently owns and cleans up its relay.
Historical JSON is evidence only; each attempt must qualify its own capability.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
import os
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import weakref

from .deathstarbench_hotel_initialization_artifacts import GuardedHotelArtifactBinding
from scripts.deathstarbench_registry_egress import https_handler


ACCESS_SCHEMA = 'hotel-guarded-initialization-image-access-v1'
ACCESS_METHOD = 'isolated-anonymous-registry-v2-full-index-manifest-config-compressed-layer-bodies-v1'
MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
MAX_TOKEN_BYTES = 64 * 1024
MAX_LAYER_BYTES = 2 * 1024 * 1024 * 1024
MAX_TOTAL_BYTES = 8 * 1024 * 1024 * 1024
MAX_LAYERS = 128
MAX_INDEX_ENTRIES = 256
CHUNK_BYTES = 64 * 1024
REQUEST_TIMEOUT_SECONDS = 30
TOTAL_TIMEOUT_SECONDS = 900
MAX_REDIRECTS = 3
_SUPPORT = frozenset({'consul', 'jaeger', 'memcached', 'mongodb'})
_PLATFORMS = ('linux/amd64', 'linux/arm64')
_DIGEST = re.compile(r'sha256:[0-9a-f]{64}\Z')
_REFERENCE = re.compile(r'(docker\.io|ghcr\.io)/([a-z0-9][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)+)@(sha256:[0-9a-f]{64})\Z')
_INDEX_TYPES = frozenset({'application/vnd.docker.distribution.manifest.list.v2+json',
                          'application/vnd.oci.image.index.v1+json'})
_MANIFEST_TYPES = frozenset({'application/vnd.docker.distribution.manifest.v2+json',
                             'application/vnd.oci.image.manifest.v1+json'})
_CONFIG_TYPES = frozenset({'application/vnd.docker.container.image.v1+json',
                           'application/vnd.oci.image.config.v1+json'})
_LAYER_TYPES = frozenset({'application/vnd.docker.image.rootfs.diff.tar.gzip',
                          'application/vnd.oci.image.layer.v1.tar',
                          'application/vnd.oci.image.layer.v1.tar+gzip',
                          'application/vnd.oci.image.layer.v1.tar+zstd'})
# Exact, reviewed HTTPS endpoints only, not wildcard domains. Docker endpoints:
# https://docs.docker.com/desktop/enterprise/allow-list/
# GitHub explicitly documents ghcr.io and pkg-containers.githubusercontent.com:
# https://docs.github.com/en/actions/reference/runners/github-hosted-runners
# Public containers can be read anonymously:
# https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry
_REGISTRIES = {
    'docker.io': {
        'host': 'registry-1.docker.io', 'realm': 'https://auth.docker.io/token',
        'service': 'registry.docker.io', 'blob_hosts': frozenset({
            'registry-1.docker.io', 'production.cloudflare.docker.com',
            'production.cloudfront.docker.com',
            'docker-images-prod.6aa30f8b08e16409b46e0173d6de2f56.r2.cloudflarestorage.com',
            'docker-images-prod.s3.dualstack.us-east-1.amazonaws.com',
        }),
    },
    'ghcr.io': {
        'host': 'ghcr.io', 'realm': 'https://ghcr.io/token', 'service': 'ghcr.io',
        'blob_hosts': frozenset({'ghcr.io', 'pkg-containers.githubusercontent.com'}),
    },
}
_CLOSED_FLAGS = (
    'released', 'initializer_authorized', 'runtime_authorized', 'initialization_executed',
    'database_executed', 'application_start_allowed', 'dataset_qualified', 'runtime_qualified',
    'measurement_qualified', 'cloud_qualified', 'public_release', 'anonymous_pull_qualified',
    'anonymous_layer_pull_qualified', 'image_execution_performed',
)


class GuardedHotelImageAccessError(ValueError):
    """Exact, complete anonymous Hotel image access could not be proven."""


def _require(condition, message):
    if not condition:
        raise GuardedHotelImageAccessError(message)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n'


def _fingerprint(value):
    return 'sha256:' + hashlib.sha256(_canonical(value).encode()).hexdigest()


def _digest(value):
    _require(isinstance(value, str) and _DIGEST.fullmatch(value) and value != 'sha256:' + '0' * 64,
             'A non-placeholder SHA-256 descriptor is required.')
    return value


def _json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            _require(key not in result, 'Registry JSON repeats an object field.')
            result[key] = value
        return result

    def constant(_value):
        raise GuardedHotelImageAccessError('Registry JSON contains a non-finite value.')

    try:
        _require(isinstance(raw, bytes) and not raw.startswith(b'\xef\xbb\xbf'),
                 'Registry JSON must be UTF-8 bytes without a BOM.')
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=unique, parse_constant=constant)
        pending = [(value, 0)]
        while pending:
            item, depth = pending.pop()
            _require(depth <= 64, 'Registry JSON exceeds its depth bound.')
            if isinstance(item, dict):
                for key, child in item.items():
                    key.encode('utf-8')
                    pending.append((child, depth + 1))
            elif isinstance(item, list):
                pending.extend((child, depth + 1) for child in item)
            elif isinstance(item, str):
                item.encode('utf-8')
            elif isinstance(item, float):
                _require(math.isfinite(item), 'Registry JSON contains a non-finite value.')
        _require(isinstance(value, dict), 'Registry JSON must contain an object.')
        return value
    except (UnicodeError, ValueError, RecursionError) as exc:
        if isinstance(exc, GuardedHotelImageAccessError):
            raise
        raise GuardedHotelImageAccessError('Registry JSON is invalid.') from None


def _reference(value):
    match = _REFERENCE.fullmatch(value) if isinstance(value, str) else None
    _require(match is not None, 'An exact adopted Docker Hub/GHCR digest reference is required.')
    registry, repository, digest = match.groups()
    _require(len(repository) <= 255, 'Registry repository exceeds its bound.')
    _digest(digest)
    return registry, repository, digest


def _url(value, hosts):
    try:
        _require(isinstance(value, str) and 0 < len(value) <= 16384,
                 'Registry URL is outside the reviewed anonymous HTTPS endpoints.')
        parsed = urllib.parse.urlsplit(value)
        _require(parsed.scheme == 'https' and parsed.hostname in hosts
                 and parsed.netloc in {parsed.hostname, parsed.hostname + ':443'}
                 and parsed.port in (None, 443) and parsed.username is None and parsed.password is None
                 and not parsed.fragment and not any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)
                 and '\\' not in value and value.isascii(),
                 'Registry URL is outside the reviewed anonymous HTTPS endpoints.')
        return parsed
    except (ValueError, TypeError):
        raise GuardedHotelImageAccessError('Registry URL is invalid.') from None


def _header(headers, name, default=None):
    # urllib's Message/HTTPMessage retains duplicate headers. Ambiguous auth,
    # redirect, length, encoding, or digest headers are never interpreted.
    values = headers.get_all(name, [])
    _require(len(values) <= 1, 'Registry response repeats a security-relevant header.')
    return values[0] if values else default


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


class _AnonymousClient:
    """Only freshly obtained exact-repository anonymous Bearer tokens exist."""

    def __init__(self):
        self.deadline = time.monotonic() + TOTAL_TIMEOUT_SECONDS
        tls = ssl.create_default_context()
        _require(tls.verify_mode == ssl.CERT_REQUIRED and tls.check_hostname,
                 'Anonymous Hotel access requires verified TLS and hostnames.')
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), _NoRedirect(), https_handler(tls))
        self.tokens = {}
        self.downloaded_bytes = 0

    def _timeout(self):
        remaining = self.deadline - time.monotonic()
        _require(remaining > 0, 'Anonymous Hotel image access exceeded its total time bound.')
        return min(REQUEST_TIMEOUT_SECONDS, remaining)

    def _token(self, registry, repository, challenge):
        policy = _REGISTRIES[registry]
        scheme, _, parameters = str(challenge or '').partition(' ')
        _require(scheme.casefold() == 'bearer', 'Registry omitted its anonymous Bearer challenge.')
        _require(0 < len(parameters) <= 4096 and parameters.isascii() and '\\' not in parameters
                 and not any(ord(c) < 32 or ord(c) == 127 for c in parameters),
                 'Registry returned an invalid token challenge.')
        fields = {}
        for parameter in urllib.request.parse_http_list(parameters):
            key, separator, value = parameter.partition('=')
            _require(separator and key in {'realm', 'service', 'scope'} and key not in fields
                     and len(value) >= 2 and value[0] == value[-1] == '"' and '\\' not in value,
                     'Registry returned an invalid token challenge.')
            fields[key] = value[1:-1]
        _require(fields == {'realm': policy['realm'], 'service': policy['service'], 'scope': f'repository:{repository}:pull'},
                 'Registry token realm, service, or repository scope differs from the exact pull.')
        query = urllib.parse.urlencode({'service': policy['service'], 'scope': fields['scope']})
        response = self._response(policy['realm'] + '?' + query, registry, None, blob=False, token_request=True)
        raw, _ = self._read(response, MAX_TOKEN_BYTES, retain=True)
        document = _json(raw)
        token = document.get('token') or document.get('access_token')
        _require(isinstance(token, str) and 0 < len(token) <= 16384
                 and re.fullmatch(r'[A-Za-z0-9._~+/=-]+', token), 'Registry returned an invalid anonymous token.')
        if document.get('token') is not None and document.get('access_token') is not None:
            _require(document['token'] == document['access_token'], 'Registry returned conflicting anonymous tokens.')
        self.tokens[registry, repository] = token

    def _response(self, url, registry, repository, *, blob, token_request=False):
        policy = _REGISTRIES[registry]
        initial = url
        token_host = urllib.parse.urlsplit(policy['realm']).hostname
        _url(url, {token_host} if token_request else {policy['host']})
        used_token = (registry, repository) in self.tokens
        redirects = 0
        while True:
            parsed = _url(url, policy['blob_hosts'] if blob else {token_host if token_request else policy['host']})
            request_headers = {'User-Agent': 'DeathStarBench-guarded-Hotel-anonymous/1', 'Accept-Encoding': 'identity',
                'Accept': ', '.join(sorted(_INDEX_TYPES | _MANIFEST_TYPES)) if '/manifests/' in parsed.path else 'application/octet-stream'}
            # Never forward even an anonymous token after a redirect, including
            # a same-host redirect or a CDN redirect back to the registry.
            if not token_request and redirects == 0 and (registry, repository) in self.tokens:
                request_headers['Authorization'] = 'Bearer ' + self.tokens[registry, repository]
            request = urllib.request.Request(url, headers=request_headers, method='GET')
            try:
                response = self.opener.open(request, timeout=self._timeout())
            except urllib.error.HTTPError as exc:
                try:
                    status = exc.code
                    challenge = _header(exc.headers, 'WWW-Authenticate')
                    location = _header(exc.headers, 'Location')
                finally:
                    exc.close()
                if status == 401 and repository is not None and not used_token and redirects == 0 and url == initial:
                    self._token(registry, repository, challenge)
                    used_token = True
                    continue
                if status in {301, 302, 303, 307, 308} and blob and redirects < MAX_REDIRECTS and isinstance(location, str):
                    url = urllib.parse.urljoin(url, location)
                    _url(url, policy['blob_hosts'])
                    redirects += 1
                    continue
                raise GuardedHotelImageAccessError(
                    f'Anonymous registry access was denied, unavailable, or redirected unsafely (HTTP {status}).') from None
            except GuardedHotelImageAccessError:
                raise
            except (OSError, urllib.error.URLError, http.client.HTTPException, ValueError):
                raise GuardedHotelImageAccessError('Anonymous registry access failed.') from None
            try:
                _require(type(response.status) is int and response.status == 200 and response.geturl() == url,
                         'Registry response is not the exact complete requested GET.')
                encoding = _header(response.headers, 'Content-Encoding', 'identity')
                _require(isinstance(encoding, str) and encoding.casefold() == 'identity',
                         'Registry response applies an unapproved content encoding.')
            except BaseException:
                response.close()
                raise
            return response

    def _read(self, response, limit, *, retain, expected_digest=None, expected_size=None):
        chunks, observed, digest = [], 0, hashlib.sha256()
        try:
            length = _header(response.headers, 'Content-Length')
            if length is not None:
                _require(isinstance(length, str) and re.fullmatch(r'0|[1-9][0-9]*', length),
                         'Registry Content-Length is invalid.')
                _require(len(length) <= 20 and int(length) <= limit
                         and (expected_size is None or int(length) == expected_size),
                         'Registry Content-Length differs from the descriptor or safety bound.')
            declared_digest = _header(response.headers, 'Docker-Content-Digest')
            if declared_digest is not None and expected_digest is not None:
                _require(declared_digest == expected_digest, 'Registry digest header differs from the pinned descriptor.')
            while True:
                self._timeout()
                count = min(CHUNK_BYTES, limit - observed + 1)
                # HTTPResponse.read(n) may loop internally while a peer slowly
                # dribbles bytes, hiding the aggregate deadline. read1(n) does
                # at most one underlying buffered read, so every received chunk
                # returns here for the monotonic deadline/aggregate checks.
                chunk = response.read1(count)
                self._timeout()
                _require(isinstance(chunk, bytes) and len(chunk) <= count, 'Registry response body is not bounded bytes.')
                if not chunk:
                    break
                observed += len(chunk)
                self.downloaded_bytes += len(chunk)
                _require(observed <= limit and self.downloaded_bytes <= MAX_TOTAL_BYTES,
                         'Registry body exceeds the per-object or total byte bound.')
                digest.update(chunk)
                if retain:
                    chunks.append(chunk)
            _require(expected_size is None or observed == expected_size, 'Registry body size differs from its exact descriptor.')
            _require(length is None or observed == int(length), 'Registry body size differs from its Content-Length.')
            _require(expected_digest is None or 'sha256:' + digest.hexdigest() == expected_digest,
                     'Registry body SHA-256 differs from its exact descriptor.')
            return b''.join(chunks) if retain else None, observed
        except (OSError, urllib.error.URLError, http.client.HTTPException, ValueError) as exc:
            if isinstance(exc, GuardedHotelImageAccessError):
                raise
            raise GuardedHotelImageAccessError('Anonymous registry body verification failed.') from None
        finally:
            response.close()

    def manifest(self, registry, repository, digest, *, size=None):
        response = self._response(f'https://{_REGISTRIES[registry]["host"]}/v2/{repository}/manifests/{digest}',
                                  registry, repository, blob=False)
        return self._read(response, MAX_DOCUMENT_BYTES, retain=True, expected_digest=digest, expected_size=size)[0]

    def blob(self, registry, repository, descriptor, *, retain=False):
        response = self._response(f'https://{_REGISTRIES[registry]["host"]}/v2/{repository}/blobs/{descriptor["digest"]}',
                                  registry, repository, blob=True)
        return self._read(response, descriptor['size'], retain=retain,
                          expected_digest=descriptor['digest'], expected_size=descriptor['size'])[0]


def _annotations(value):
    _require(isinstance(value, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in value.items()),
             'Registry annotations must be strings.')


def _descriptor(value, media_types, limit, *, indexed=False):
    allowed = {'mediaType', 'digest', 'size', 'annotations'} | ({'platform'} if indexed else set())
    _require(isinstance(value, dict) and {'mediaType', 'digest', 'size'} <= set(value) <= allowed,
             'A registry descriptor has unexpected or incomplete fields/URLs.')
    _digest(value['digest'])
    _require(isinstance(value['mediaType'], str) and value['mediaType'] in media_types
             and type(value['size']) is int and 0 < value['size'] <= limit,
             'A registry descriptor has an unsupported type or unsafe size.')
    if 'annotations' in value:
        _annotations(value['annotations'])
    return {key: value[key] for key in ('mediaType', 'digest', 'size')}


def _context(artifacts):
    _require(type(artifacts) is GuardedHotelArtifactBinding,
             'An exact adopted guarded Hotel artifact binding is required, not a saved summary or legacy plan.')
    try:
        artifacts.__post_init__()
        _digest(artifacts.fingerprint)
        _require(set(artifacts.app['platforms']) == set(_PLATFORMS), 'Both guarded Hotel architectures are required.')
        _require(set(artifacts.reused_images) == _SUPPORT | {'load-driver'}, 'The exact four support images and driver are required.')
        images = {}
        for name, item in [('hotel-reservation', artifacts.app), *sorted(artifacts.reused_images.items())]:
            platforms = _PLATFORMS if name == 'hotel-reservation' else ('linux/amd64',)
            _require(set(item['platforms']) == set(platforms), 'An image has a crossed platform inventory.')
            index_registry, index_repository, _ = _reference(item['index_image'])
            _require(index_registry == ('docker.io' if name in _SUPPORT else 'ghcr.io'),
                     'Image registry differs from its reviewed application/support/driver role.')
            for platform in platforms:
                bound = item['platforms'][platform]
                registry, repository, manifest_digest = _reference(bound['image'])
                _require((registry, repository) == (index_registry, index_repository)
                         and bound['manifest_digest'] == manifest_digest and bound['os'] == 'linux'
                         and bound['architecture'] == platform.split('/')[1],
                         'An adopted platform image is crossed with another index or architecture.')
                _digest(bound['config_digest'])
                key = name + '@' + platform if name == 'hotel-reservation' else name
                images[key] = {'image': bound['image'], 'index_image': item['index_image'],
                               'platform': platform, 'config_digest': bound['config_digest']}
        _require(len(images) == 7 and len({item['image'] for item in images.values()}) == 7,
                 'Guarded Hotel access requires seven distinct exact platform images.')
        return {'workload_id': 'hotel_reservation', 'artifact_fingerprint': artifacts.fingerprint,
                'images': dict(sorted(images.items()))}
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        if isinstance(exc, GuardedHotelImageAccessError):
            raise
        raise GuardedHotelImageAccessError('Guarded Hotel artifact identity is incomplete, changed, or crossed.') from None


def _selected_descriptors(client, identity):
    groups = {}
    for key, expected in identity['images'].items():
        groups.setdefault(expected['index_image'], {})[key] = expected
    selected = {}
    index_results = {}
    for index_image, expected_images in sorted(groups.items()):
        registry, repository, digest = _reference(index_image)
        raw = client.manifest(registry, repository, digest)
        index = _json(raw)
        _require(type(index.get('schemaVersion')) is int and index['schemaVersion'] == 2
                 and isinstance(index.get('mediaType'), str) and index['mediaType'] in _INDEX_TYPES
                 and set(index) <= {'schemaVersion', 'mediaType', 'manifests', 'annotations'},
                 'An adopted image index has an unsupported schema or fields.')
        if 'annotations' in index:
            _annotations(index['annotations'])
        entries = index.get('manifests')
        _require(isinstance(entries, list) and 0 < len(entries) <= MAX_INDEX_ENTRIES,
                 'An adopted index has an empty or excessive manifest inventory.')
        for entry in entries:
            descriptor = _descriptor(entry, _MANIFEST_TYPES, MAX_DOCUMENT_BYTES, indexed=True)
            platform = entry.get('platform')
            _require(isinstance(platform, dict) and isinstance(platform.get('os'), str)
                     and isinstance(platform.get('architecture'), str), 'An index platform is missing.')
            for key, expected in expected_images.items():
                if expected['platform'] != platform['os'] + '/' + platform['architecture']:
                    continue
                _require(key not in selected and set(platform) <= {'os', 'architecture', 'variant'}
                         and (platform.get('variant') in (None, 'v8') if platform['architecture'] == 'arm64'
                              else platform.get('variant') is None), 'An index repeats or changes a selected native platform.')
                _require(expected['image'] == registry + '/' + repository + '@' + descriptor['digest'],
                         'Index platform descriptor differs from the exact adopted image.')
                selected[key] = descriptor
        _require(set(expected_images) <= set(selected), 'An adopted index omitted a bound platform image.')
        index_results[index_image] = {'digest': digest, 'bytes': len(raw), 'all_index_body_sha256_verified': True}
    _require(set(selected) == set(identity['images']), 'The selected index image inventory differs from its artifact binding.')
    return selected, index_results


@dataclass(frozen=True, slots=True)
class _AccessProof:
    metadata_json: str
    fingerprint: str
    pid: int


_PROOFS = weakref.WeakKeyDictionary()
_PROOFS_LOCK = threading.Lock()


class GuardedHotelImageAccess:
    """Immutable factory-only process-local access capability, not a release."""

    __slots__ = ('_metadata_json', '_fingerprint', '__weakref__')

    def __new__(cls, *args, **kwargs):
        raise GuardedHotelImageAccessError('Guarded Hotel access requires fresh anonymous full-byte qualification.')

    def __setattr__(self, name, value):
        raise GuardedHotelImageAccessError('Guarded Hotel image access capabilities are immutable.')

    def __reduce_ex__(self, protocol):
        raise GuardedHotelImageAccessError('Guarded Hotel access capabilities cannot be copied or serialized as authority.')

    @property
    def fingerprint(self):
        return self._fingerprint

    def metadata(self):
        proof = _authority(self)
        return {**json.loads(proof.metadata_json), 'fingerprint': proof.fingerprint}


def _authority(value):
    _require(type(value) is GuardedHotelImageAccess, 'An original guarded Hotel image access capability is required.')
    with _PROOFS_LOCK:
        proof = _PROOFS.get(value)
    _require(proof is not None and proof.pid == os.getpid(),
             'A summary, copied, or other-process access receipt has no authority.')
    _require(value._metadata_json == proof.metadata_json and value._fingerprint == proof.fingerprint,
             'Guarded Hotel access capability fields changed after qualification.')
    return proof


def require_guarded_hotel_image_access(value, artifacts):
    """Require a fresh original capability bound to precisely these artifacts."""
    proof = _authority(value)
    _require(json.loads(proof.metadata_json)['identity'] == _context(artifacts),
             'Guarded Hotel image access is crossed with another artifact binding.')
    return value


def qualify_guarded_hotel_image_access(artifacts):
    """Stream every bound compressed image byte anonymously; no partial result.

    All limits and trusted endpoints are fixed policy. A denial, rate limit,
    unreviewed redirect, malformed input, truncation, or digest mismatch aborts
    the whole attempt. No caller transport, credential or policy overrides exist.
    """
    identity = _context(artifacts)  # Reject crossed evidence before any network.
    client = _AnonymousClient()
    selected, indexes = _selected_descriptors(client, identity)
    results, declared_bytes = {}, sum(item['bytes'] for item in indexes.values())
    for key, expected in identity['images'].items():
        registry, repository, manifest_digest = _reference(expected['image'])
        descriptor = selected[key]
        raw = client.manifest(registry, repository, manifest_digest, size=descriptor['size'])
        manifest = _json(raw)
        _require(type(manifest.get('schemaVersion')) is int and manifest['schemaVersion'] == 2
                 and manifest.get('mediaType') == descriptor['mediaType']
                 and set(manifest) <= {'schemaVersion', 'mediaType', 'config', 'layers', 'annotations'},
                 'Guarded Hotel access requires an exact platform image manifest, not an index/artifact.')
        if 'annotations' in manifest:
            _annotations(manifest['annotations'])
        config = _descriptor(manifest.get('config'), _CONFIG_TYPES, MAX_DOCUMENT_BYTES)
        _require(config['digest'] == expected['config_digest'], 'Image config differs from the adopted artifact.')
        layers = manifest.get('layers')
        _require(isinstance(layers, list) and 0 < len(layers) <= MAX_LAYERS,
                 'Image layer inventory is empty or exceeds its bound.')
        layers = [_descriptor(item, _LAYER_TYPES, MAX_LAYER_BYTES) for item in layers]
        declared_bytes += len(raw) + config['size'] + sum(item['size'] for item in layers)
        _require(declared_bytes <= MAX_TOTAL_BYTES, 'Declared Hotel image bytes exceed the aggregate safety bound.')
        configuration = _json(client.blob(registry, repository, config, retain=True))
        _require(configuration.get('os') == 'linux' and configuration.get('architecture') == expected['platform'].split('/')[1],
                 'Image config does not describe its exact adopted native platform.')
        rootfs = configuration.get('rootfs')
        _require(isinstance(rootfs, dict) and rootfs.get('type') == 'layers'
                 and isinstance(rootfs.get('diff_ids'), list) and len(rootfs['diff_ids']) == len(layers),
                 'Image rootfs layer inventory differs from its platform manifest.')
        for digest in rootfs['diff_ids']:
            _digest(digest)
        for layer in layers:
            client.blob(registry, repository, layer)
        results[key] = {'image': expected['image'], 'platform': expected['platform'],
                        'index_image': expected['index_image'], 'manifest': descriptor,
                        'config': config, 'layers': layers,
                        'compressed_layer_bytes': sum(item['size'] for item in layers),
                        'all_descriptor_bodies_sha256_and_size_verified': True}
    # Recheck the original frozen proof after all potentially slow I/O.
    _require(identity == _context(artifacts), 'Guarded Hotel artifact identity changed during anonymous verification.')
    metadata = {'schema_version': 1, 'candidate_schema': ACCESS_SCHEMA, 'method': ACCESS_METHOD,
                'candidate_only': True, **dict.fromkeys(_CLOSED_FLAGS, False),
                'anonymous_all_bound_image_bytes_verified': True,
                'scope': 'exact-guarded-hotel-both-app-platforms-four-x86-support-images-and-x86-driver-bytes-only',
                'identity': identity, 'indexes': indexes, 'images': results,
                'observed_at': datetime.now(timezone.utc).isoformat(), 'downloaded_bytes': client.downloaded_bytes,
                'client': {'registry_credentials_used': False, 'docker_config_loaded': False, 'netrc_loaded': False,
                           'cookies_used': False, 'environment_proxy_credentials_used': False,
                           'local_image_cache_used': False, 'file_writes_performed': False,
                           'anonymous_repository_scoped_bearer_tokens': True,
                           'tokens_forwarded_to_redirect_targets': False},
                'limits': {'document_bytes': MAX_DOCUMENT_BYTES, 'token_bytes': MAX_TOKEN_BYTES,
                           'layer_bytes': MAX_LAYER_BYTES, 'total_bytes': MAX_TOTAL_BYTES,
                           'layers_per_image': MAX_LAYERS, 'index_entries': MAX_INDEX_ENTRIES,
                           'socket_timeout_seconds': REQUEST_TIMEOUT_SECONDS,
                           'total_timeout_seconds': TOTAL_TIMEOUT_SECONDS, 'redirects_per_blob': MAX_REDIRECTS},
                'limitations': ['This is complete compressed-byte access, not a Docker/CRI pull, binary smoke, or runtime qualification.',
                                'No image, application, initializer, database, Kubernetes, or benchmark was executed.',
                                'Every attempt/resume needs a fresh factory capability; saved JSON is historical evidence only.']}
    encoded = _canonical(metadata)
    value = object.__new__(GuardedHotelImageAccess)
    object.__setattr__(value, '_metadata_json', encoded)
    object.__setattr__(value, '_fingerprint', _fingerprint(metadata))
    with _PROOFS_LOCK:
        _PROOFS[value] = _AccessProof(encoded, value._fingerprint, os.getpid())
    return value
