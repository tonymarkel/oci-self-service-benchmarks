"""Fresh anonymous byte access for support-only Hotel/Media staging.

This does not run images, pull them into a runtime, or qualify anonymous access
to the paused applications/frontends/load drivers. It streams the exact four
amd64 Docker Hub support images' config and every compressed layer body without
registry credentials, local image caches, proxy credentials, or file writes.
The returned process-local capability is not restorable from serialized data.
Every operator attempt/resume must obtain a fresh capability.
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

from .deathstarbench_candidate_runtime import CandidateRuntimePlan
from .deathstarbench_candidate_stage import CandidateSupportStageBundle, validate_candidate_support_stage
from scripts.deathstarbench_registry_egress import https_handler


ACCESS_SCHEMA = 'candidate-support-image-access-v1'
ACCESS_METHOD = 'anonymous-registry-v2-full-config-and-compressed-layer-bodies-v1'
MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
MAX_TOKEN_BYTES = 64 * 1024
MAX_LAYER_BYTES = 2 * 1024 * 1024 * 1024
MAX_TOTAL_BYTES = 8 * 1024 * 1024 * 1024
MAX_LAYERS = 128
CHUNK_BYTES = 64 * 1024
REQUEST_TIMEOUT_SECONDS = 30
TOTAL_TIMEOUT_SECONDS = 900
MAX_REDIRECTS = 3
_REGISTRY_HOST = 'registry-1.docker.io'
_TOKEN_REALM = 'https://auth.docker.io/token'
# Docker's documented image-download endpoints. New destinations require a
# reviewed update; never permit arbitrary manifest URLs or private endpoints.
# https://docs.docker.com/desktop/enterprise/allow-list/
# https://github.com/docker/docs/issues/21960
_BLOB_HOSTS = frozenset({
    _REGISTRY_HOST,
    'production.cloudflare.docker.com',
    'production.cloudfront.docker.com',
    'docker-images-prod.6aa30f8b08e16409b46e0173d6de2f56.r2.cloudflarestorage.com',
})
_MANIFEST_TYPES = frozenset({
    'application/vnd.docker.distribution.manifest.v2+json',
    'application/vnd.oci.image.manifest.v1+json',
})
_CONFIG_TYPES = frozenset({
    'application/vnd.docker.container.image.v1+json',
    'application/vnd.oci.image.config.v1+json',
})
_LAYER_TYPES = frozenset({
    'application/vnd.docker.image.rootfs.diff.tar.gzip',
    'application/vnd.oci.image.layer.v1.tar',
    'application/vnd.oci.image.layer.v1.tar+gzip',
    'application/vnd.oci.image.layer.v1.tar+zstd',
})
_DIGEST = re.compile(r'sha256:[0-9a-f]{64}\Z')
_REFERENCE = re.compile(r'docker\.io/([a-z0-9][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)+)@(sha256:[0-9a-f]{64})\Z')


class CandidateImageAccessError(ValueError):
    """Support-only anonymous access or its exact capability cannot be proven."""


def _require(condition, message):
    if not condition:
        raise CandidateImageAccessError(message)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n'


def _fingerprint(value):
    return 'sha256:' + hashlib.sha256(_canonical(value).encode()).hexdigest()


def _json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            _require(key not in result, 'Registry JSON repeats an object field.')
            result[key] = value
        return result

    def constant(_value):
        raise CandidateImageAccessError('Registry JSON contains a non-finite value.')

    try:
        _require(isinstance(raw, bytes) and not raw.startswith(b'\xef\xbb\xbf'), 'Registry JSON must be UTF-8 bytes without a BOM.')
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
        if isinstance(exc, CandidateImageAccessError):
            raise
        raise CandidateImageAccessError('Registry JSON is invalid.') from None


def _digest(value):
    _require(isinstance(value, str) and _DIGEST.fullmatch(value) and value != 'sha256:' + '0' * 64,
             'A non-placeholder SHA-256 descriptor is required.')
    return value


def _url(value, hosts):
    try:
        parsed = urllib.parse.urlsplit(value)
        _require(parsed.scheme == 'https' and parsed.hostname in hosts and parsed.port in (None, 443)
                 and parsed.username is None and parsed.password is None and not parsed.fragment
                 and not any(c.isspace() for c in value) and '\\' not in value,
                 'Registry URL is outside the reviewed anonymous HTTPS endpoints.')
    except (ValueError, TypeError):
        raise CandidateImageAccessError('Registry URL is invalid.') from None
    return parsed


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


class _AnonymousClient:
    """No Docker config, registry environment auth, cookie jar, or netrc input."""

    def __init__(self):
        self.deadline = time.monotonic() + TOTAL_TIMEOUT_SECONDS
        # Explicitly disable environment proxy credentials as well as implicit
        # redirects; never inherit an installed/default authenticated opener.
        tls = ssl.create_default_context()
        _require(tls.verify_mode == ssl.CERT_REQUIRED and tls.check_hostname,
                 'Anonymous support access requires verified TLS and hostnames.')
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), _NoRedirect(), https_handler(tls))
        self.tokens = {}
        self.downloaded_bytes = 0

    def _timeout(self):
        remaining = self.deadline - time.monotonic()
        _require(remaining > 0, 'Anonymous support-image access exceeded its total time bound.')
        return min(REQUEST_TIMEOUT_SECONDS, remaining)

    def _token(self, repository, challenge):
        scheme, _, parameters = str(challenge or '').partition(' ')
        _require(scheme.casefold() == 'bearer', 'Docker Hub omitted its anonymous Bearer challenge.')
        fields = {}
        for parameter in urllib.request.parse_http_list(parameters):
            key, separator, value = parameter.partition('=')
            _require(separator and key in {'realm', 'service', 'scope'} and key not in fields
                     and len(value) >= 2 and value[0] == value[-1] == '"' and '\\' not in value,
                     'Docker Hub returned an invalid token challenge.')
            fields[key] = value[1:-1]
        _require(fields == {'realm': _TOKEN_REALM, 'service': 'registry.docker.io', 'scope': f'repository:{repository}:pull'},
                 'Docker Hub token realm, service, or repository scope differs from the exact pull.')
        query = urllib.parse.urlencode({'service': 'registry.docker.io', 'scope': fields['scope']})
        response = self._response(_TOKEN_REALM + '?' + query, None, blob=False, token_request=True)
        raw, _ = self._read(response, MAX_TOKEN_BYTES, retain=True)
        document = _json(raw)
        token = document.get('token') or document.get('access_token')
        _require(isinstance(token, str) and 0 < len(token) <= 16384
                 and re.fullmatch(r'[A-Za-z0-9._~+/=-]+', token), 'Docker Hub returned an invalid anonymous token.')
        if document.get('token') is not None and document.get('access_token') is not None:
            _require(document['token'] == document['access_token'], 'Docker Hub returned conflicting anonymous tokens.')
        self.tokens[repository] = token

    def _response(self, url, repository, *, blob, token_request=False):
        initial = url
        _url(initial, {'auth.docker.io'} if token_request else {_REGISTRY_HOST})
        used_token = repository in self.tokens
        redirects = 0
        while True:
            parsed = _url(url, _BLOB_HOSTS if blob else ({'auth.docker.io'} if token_request else {_REGISTRY_HOST}))
            headers = {'User-Agent': 'DeathStarBench-candidate-support-anonymous/1', 'Accept-Encoding': 'identity',
                       'Accept': ', '.join(sorted(_MANIFEST_TYPES)) if '/manifests/' in parsed.path else 'application/octet-stream'}
            # A token is obtained without credentials for precisely this repo;
            # it is never forwarded to a redirect target, even the same host.
            if repository in self.tokens and url == initial and redirects == 0 and not token_request:
                headers['Authorization'] = 'Bearer ' + self.tokens[repository]
            request = urllib.request.Request(url, headers=headers, method='GET')
            try:
                response = self.opener.open(request, timeout=self._timeout())
            except urllib.error.HTTPError as exc:
                status, challenge, location = exc.code, exc.headers.get('WWW-Authenticate'), exc.headers.get('Location')
                exc.close()
                if status == 401 and repository is not None and not used_token and url == initial:
                    self._token(repository, challenge)
                    used_token = True
                    continue
                if status in {301, 302, 303, 307, 308} and blob and redirects < MAX_REDIRECTS and isinstance(location, str):
                    url = urllib.parse.urljoin(url, location)
                    _url(url, _BLOB_HOSTS)
                    redirects += 1
                    continue
                raise CandidateImageAccessError(f'Anonymous registry access was denied, unavailable, or redirected unsafely (HTTP {status}).') from None
            except CandidateImageAccessError:
                raise
            except (OSError, urllib.error.URLError, http.client.HTTPException, ValueError):
                # Exception strings may contain signed CDN URLs. Never expose
                # those URLs, response bodies, or bearer tokens in receipts.
                raise CandidateImageAccessError('Anonymous registry access failed.') from None
            try:
                _require(response.status == 200 and response.geturl() == url,
                         'Registry response is not the exact complete requested GET.')
                _require(response.headers.get('Content-Encoding', 'identity').casefold() == 'identity',
                         'Registry response applies an unapproved content encoding.')
            except BaseException:
                response.close()
                raise
            return response

    def _read(self, response, limit, *, retain, expected_digest=None, expected_size=None):
        chunks, observed, digest = [], 0, hashlib.sha256()
        try:
            length = response.headers.get('Content-Length')
            if length is not None:
                _require(isinstance(length, str) and re.fullmatch(r'0|[1-9][0-9]*', length), 'Registry Content-Length is invalid.')
                _require(int(length) <= limit and (expected_size is None or int(length) == expected_size),
                         'Registry Content-Length differs from the descriptor or safety bound.')
            declared_digest = response.headers.get('Docker-Content-Digest')
            if declared_digest is not None and expected_digest is not None:
                _require(declared_digest == expected_digest, 'Registry digest header differs from the pinned descriptor.')
            while True:
                self._timeout()
                chunk = response.read(min(CHUNK_BYTES, limit - observed + 1))
                self._timeout()
                _require(isinstance(chunk, bytes), 'Registry response body is not bytes.')
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
            observed_digest = 'sha256:' + digest.hexdigest()
            _require(expected_digest is None or observed_digest == expected_digest,
                     'Registry body SHA-256 differs from its exact descriptor.')
            return b''.join(chunks) if retain else None, observed
        except (OSError, urllib.error.URLError, http.client.HTTPException, ValueError) as exc:
            if isinstance(exc, CandidateImageAccessError):
                raise
            raise CandidateImageAccessError('Anonymous registry body verification failed.') from None
        finally:
            response.close()

    def manifest(self, repository, digest):
        response = self._response(f'https://{_REGISTRY_HOST}/v2/{repository}/manifests/{digest}', repository, blob=False)
        return self._read(response, MAX_DOCUMENT_BYTES, retain=True, expected_digest=digest)[0]

    def blob(self, repository, descriptor, *, retain=False):
        response = self._response(f'https://{_REGISTRY_HOST}/v2/{repository}/blobs/{descriptor["digest"]}', repository, blob=True)
        return self._read(response, descriptor['size'], retain=retain,
                          expected_digest=descriptor['digest'], expected_size=descriptor['size'])[0]


def _descriptor(value, media_types, limit):
    _require(isinstance(value, dict) and {'mediaType', 'digest', 'size'} <= set(value)
             and not set(value) - {'mediaType', 'digest', 'size', 'annotations'},
             'A registry descriptor has unexpected or incomplete fields/URLs.')
    _digest(value['digest'])
    _require(isinstance(value['mediaType'], str) and value['mediaType'] in media_types
             and type(value['size']) is int and 0 < value['size'] <= limit,
             'A registry descriptor has an unsupported type or unsafe size.')
    _require('annotations' not in value or (isinstance(value['annotations'], dict)
             and all(isinstance(k, str) and isinstance(v, str) for k, v in value['annotations'].items())),
             'Registry descriptor annotations must be strings.')
    return {key: value[key] for key in ('mediaType', 'digest', 'size')}


def _context(plan, stage):
    _require(type(plan) is CandidateRuntimePlan and type(stage) is CandidateSupportStageBundle,
             'Exact typed candidate plan and support stage are required.')
    try:
        stage = validate_candidate_support_stage(plan, stage)
        from .deathstarbench_candidate_workload import validate_candidate_assets
        components, _ = validate_candidate_assets(plan.artifacts.workload_id)
        keys = {item['name']: item['image_key'] for item in components if item['role'] != 'application'}
        images = {}
        for item in json.loads(stage.canonical_json)['items']:
            if item['kind'] != 'Deployment' or item['metadata']['name'] not in keys:
                continue
            _require(item['spec']['replicas'] == 1, 'Every active support deployment must have one replica.')
            key = keys[item['metadata']['name']]
            bound = plan.artifacts.image_bindings[key]['platforms']['linux/amd64']
            reference = item['spec']['template']['spec']['containers'][0]['image']
            _require(reference == bound['image'] and _REFERENCE.fullmatch(reference), 'Support image must be the exact adopted Docker Hub platform digest.')
            _require(bound['os'] == 'linux' and bound['architecture'] == 'amd64', 'Support image must be native Linux amd64.')
            _digest(bound['config_digest'])
            images[key] = {'image': reference, 'platform': 'linux/amd64', 'config_digest': bound['config_digest']}
        _require(len(images) == 4 and len({value['image'] for value in images.values()}) == 4,
                 'Support-only access requires the exact four unique backing images.')
        return {'run_id': plan.run_id, 'provider': plan.provider, 'workload_id': stage.workload_id,
                'artifact_fingerprint': plan.artifacts.fingerprint, 'snapshot_fingerprint': plan.fingerprint,
                'stage_fingerprint': stage.fingerprint, 'stage_manifest_sha256': stage.rendered_manifest_sha256,
                'active_support_components': sorted(keys), 'support_images': dict(sorted(images.items()))}
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        if isinstance(exc, CandidateImageAccessError):
            raise
        raise CandidateImageAccessError('Support image access cannot adopt a crossed plan/stage/artifact identity.') from None


@dataclass(frozen=True, slots=True)
class _AccessProof:
    metadata_json: str
    fingerprint: str
    pid: int


_PROOFS = weakref.WeakKeyDictionary()
_PROOFS_LOCK = threading.Lock()


class CandidateSupportImageAccess:
    """Original immutable process-local access proof, not a release authority."""

    __slots__ = ('_metadata_json', '_fingerprint', '__weakref__')

    def __new__(cls, *args, **kwargs):
        raise CandidateImageAccessError('Support access capabilities require fresh anonymous qualification.')

    def __setattr__(self, name, value):
        raise CandidateImageAccessError('Support access capabilities are immutable.')

    @property
    def fingerprint(self):
        return self._fingerprint

    def metadata(self):
        proof = _authority(self)
        return {**json.loads(proof.metadata_json), 'fingerprint': proof.fingerprint}


def _authority(value):
    _require(type(value) is CandidateSupportImageAccess, 'An original support-image access capability is required.')
    with _PROOFS_LOCK:
        proof = _PROOFS.get(value)
    _require(proof is not None and proof.pid == os.getpid(), 'A summary, copied, or other-process access receipt has no authority.')
    _require(value._metadata_json == proof.metadata_json and value._fingerprint == proof.fingerprint,
             'Support access capability fields changed after qualification.')
    return proof


def require_candidate_support_image_access(value, plan, stage):
    """Check an original fresh-attempt capability, never a saved JSON receipt."""
    proof = _authority(value)
    _require(json.loads(proof.metadata_json)['identity'] == _context(plan, stage),
             'Support access capability is crossed with another run/stage/artifact binding.')
    return value


def qualify_candidate_support_image_access(plan, stage):
    """Stream complete anonymous support bytes; never run any image or write files.

    Limits are fixed policy, not caller-provided overrides. HTTP 429/denials,
    unreviewed redirects, TLS errors and byte/time bounds fail this attempt;
    no partial capability is returned. Resume must call this factory again.
    """
    identity = _context(plan, stage)
    client = _AnonymousClient()
    results = {}
    declared_bytes = 0
    for key, expected in identity['support_images'].items():
        repository, manifest_digest = _REFERENCE.fullmatch(expected['image']).groups()
        raw = client.manifest(repository, manifest_digest)
        manifest = _json(raw)
        _require(type(manifest.get('schemaVersion')) is int and manifest['schemaVersion'] == 2
                 and isinstance(manifest.get('mediaType'), str)
                 and manifest['mediaType'] in _MANIFEST_TYPES and 'manifests' not in manifest,
                 'Support access requires a platform image manifest, not an index.')
        config = _descriptor(manifest.get('config'), _CONFIG_TYPES, MAX_DOCUMENT_BYTES)
        _require(config['digest'] == expected['config_digest'], 'Support config differs from the adopted artifact.')
        layers = manifest.get('layers')
        _require(isinstance(layers, list) and 0 < len(layers) <= MAX_LAYERS, 'Support layer inventory is empty or exceeds its bound.')
        layers = [_descriptor(item, _LAYER_TYPES, MAX_LAYER_BYTES) for item in layers]
        declared_bytes += len(raw) + config['size'] + sum(item['size'] for item in layers)
        _require(declared_bytes <= MAX_TOTAL_BYTES, 'Declared support image bytes exceed the aggregate safety bound.')
        configuration = _json(client.blob(repository, config, retain=True))
        _require(configuration.get('os') == 'linux' and configuration.get('architecture') == 'amd64',
                 'Support config does not describe native Linux amd64.')
        rootfs = configuration.get('rootfs')
        _require(isinstance(rootfs, dict) and rootfs.get('type') == 'layers'
                 and isinstance(rootfs.get('diff_ids'), list) and len(rootfs['diff_ids']) == len(layers),
                 'Support rootfs layer inventory differs from its platform manifest.')
        for digest in rootfs['diff_ids']:
            _digest(digest)
        for descriptor in layers:
            client.blob(repository, descriptor)
        results[key] = {'image': expected['image'], 'platform': 'linux/amd64',
                        'manifest_digest': manifest_digest, 'manifest_bytes': len(raw),
                        'config': config, 'layers': layers,
                        'compressed_layer_bytes': sum(item['size'] for item in layers),
                        'all_descriptor_bodies_sha256_verified': True}
    metadata = {'schema_version': 1, 'candidate_schema': ACCESS_SCHEMA, 'method': ACCESS_METHOD,
                'candidate_only': True, 'released': False, 'runtime_qualified': False,
                'measurement_qualified': False, 'anonymous_pull_qualified': False,
                'anonymous_layer_pull_qualified': False, 'application_start_allowed': False,
                'anonymous_support_bytes_verified': True, 'support_image_execution_performed': False,
                'scope': 'active-support-images-only-no-runtime-or-application-qualification',
                'identity': identity, 'images': results,
                'observed_at': datetime.now(timezone.utc).isoformat(),
                'downloaded_bytes': client.downloaded_bytes,
                'client': {'registry_credentials_used': False, 'docker_config_loaded': False,
                           'environment_proxy_credentials_used': False, 'local_image_cache_used': False,
                           'anonymous_repository_scoped_bearer_tokens': True},
                'limits': {'document_bytes': MAX_DOCUMENT_BYTES, 'layer_bytes': MAX_LAYER_BYTES,
                           'total_bytes': MAX_TOTAL_BYTES, 'layers_per_image': MAX_LAYERS,
                           'socket_timeout_seconds': REQUEST_TIMEOUT_SECONDS, 'total_timeout_seconds': TOTAL_TIMEOUT_SECONDS},
                'limitations': ['This is complete compressed-byte access, not a Docker/CRI image pull or a native binary smoke.',
                                'No application/frontend/driver bytes or execution are qualified by this support-only proof.',
                                'The operator must obtain a fresh factory capability on every attempt/resume; saved JSON is historical evidence.']}
    encoded = _canonical(metadata)
    value = object.__new__(CandidateSupportImageAccess)
    object.__setattr__(value, '_metadata_json', encoded)
    object.__setattr__(value, '_fingerprint', _fingerprint(metadata))
    with _PROOFS_LOCK:
        _PROOFS[value] = _AccessProof(encoded, value._fingerprint, os.getpid())
    return value
