"""Explicit private-qualifier routing through operator-owned SSH TCP forwards.

This changes only where the process opens TCP sockets for eight exact public
registry/auth/CDN HTTPS endpoints. The original urllib HTTPS connections still
perform their own TLS handshake, hostname/certificate checks, HTTP requests,
redirect validation, byte limits, and SHA-256 verification. No HTTP proxy,
registry credential, alternate trust anchor, cached bytes, or deserialized
capability is accepted here. Other cloud/API socket destinations are unchanged.

The outer operator must create, authenticate, own, and clean up the SSH helper
and its loopback forwards. A listening loopback port is not proof of SSH or
cloud ownership; this module deliberately does not claim to verify either.
Install this context only in a standalone private qualification CLI, never the
application server. It is context-bound, exclusive, PID-bound, and one-use.
Only handlers explicitly obtained with https_handler() use these forwards;
global sockets, default urllib openers, and cloud SDK transports are untouched.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import socket
import ssl
import stat
import threading
import urllib.request
import weakref


SCHEMA = 'registry-ssh-tcp-egress-observation-v1'
TRANSPORT = 'operator-owned-ssh-tcp-forward'
HOSTS = frozenset({
    'registry-1.docker.io', 'auth.docker.io',
    'production.cloudflare.docker.com', 'production.cloudfront.docker.com',
    'docker-images-prod.6aa30f8b08e16409b46e0173d6de2f56.r2.cloudflarestorage.com',
    'ghcr.io', 'pkg-containers.githubusercontent.com',
    'docker-images-prod.s3.dualstack.us-east-1.amazonaws.com',
})
MAX_CONFIG_BYTES = 16 * 1024
_FORBIDDEN_ENVIRONMENT = (
    'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
    'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy',
    'SSL_CERT_FILE', 'SSL_CERT_DIR', 'REQUESTS_CA_BUNDLE', 'CURL_CA_BUNDLE',
    'PYTHONHTTPSVERIFY', 'SSLKEYLOGFILE', 'OPENSSL_CONF',
    'REGISTRY_TOKEN', 'REGISTRY_USERNAME', 'DOCKER_AUTH_CONFIG',
)
_PROOFS = weakref.WeakKeyDictionary()
_PROOFS_LOCK = threading.Lock()
_ACTIVE_LOCK = threading.Lock()
_MODULE_PID = os.getpid()
_CURRENT = ContextVar('deathstarbench_private_registry_egress', default=None)


class RegistryEgressError(ValueError):
    """A private egress mapping or its process scope cannot be safely used."""


def _require(condition, message):
    if not condition:
        raise RegistryEgressError(message)


def _signature(details):
    return (details.st_dev, details.st_ino, details.st_uid, details.st_mode,
            details.st_nlink, details.st_size, details.st_mtime_ns, details.st_ctime_ns)


def _read(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        _require(stat.S_ISREG(before.st_mode) and before.st_uid == os.getuid()
                 and stat.S_IMODE(before.st_mode) == 0o600 and before.st_nlink == 1
                 and 0 < before.st_size <= MAX_CONFIG_BYTES,
                 'Registry egress configuration must be an owned private bounded single-link regular file.')
        raw = bytearray()
        while len(raw) <= MAX_CONFIG_BYTES:
            chunk = os.read(descriptor, min(65536, MAX_CONFIG_BYTES + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        after = os.fstat(descriptor)
        named = os.stat(path, follow_symlinks=False)
        _require(_signature(before) == _signature(after) == _signature(named)
                 and len(raw) == before.st_size,
                 'Registry egress configuration changed while reading.')
        return bytes(raw), _signature(before)
    finally:
        os.close(descriptor)


def _json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, 'Registry egress JSON repeats a field.')
            result[key] = value
        return result

    try:
        _require(not raw.startswith(b'\xef\xbb\xbf'), 'Registry egress JSON must not contain a BOM.')
        return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(RegistryEgressError('Nonfinite egress JSON.')))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise RegistryEgressError('Registry egress configuration is not strict finite UTF-8 JSON.') from exc


def _routes(value):
    _require(type(value) is dict and set(value) == {'schema_version', 'transport', 'forwards'}
             and type(value['schema_version']) is int and value['schema_version'] == 1
             and value['transport'] == TRANSPORT and type(value['forwards']) is dict
             and set(value['forwards']) == HOSTS,
             'Registry egress requires the exact schema and every reviewed registry/auth/CDN host.')
    result = []
    for host in sorted(HOSTS):
        endpoint = value['forwards'][host]
        _require(type(endpoint) is dict and set(endpoint) == {'host', 'port'}
                 and endpoint['host'] == '127.0.0.1' and type(endpoint['host']) is str
                 and type(endpoint['port']) is int and 1 <= endpoint['port'] <= 65535,
                 'Each registry forward requires one literal IPv4 loopback address and integer TCP port.')
        result.append((host, endpoint['port']))
    _require(len({port for _host, port in result}) == len(result),
             'Registry hosts must not share a loopback forward endpoint.')
    return tuple(result)


@dataclass(frozen=True, slots=True)
class _Proof:
    pid: int
    path: Path
    signature: tuple
    digest: str
    routes: tuple


class RegistryEgressMapping(Mapping):
    """Factory-only immutable route selectors; not cloud/SSH authority."""

    __slots__ = ('_routes', '_path', '_digest', '_pid', '__weakref__')
    __hash__ = object.__hash__
    __eq__ = object.__eq__

    def __new__(cls, *args, **kwargs):
        raise RegistryEgressError('Load the exact private egress configuration with load_mapping().')

    def __setattr__(self, name, value):
        raise RegistryEgressError('Registry egress mappings are immutable.')

    def __getitem__(self, host):
        for name, port in self._routes:
            if name == host:
                return ('127.0.0.1', port)
        raise KeyError(host)

    def __iter__(self):
        return (host for host, _port in self._routes)

    def __len__(self):
        return len(self._routes)


def load_mapping(path):
    """Read all eight routes from an exact owned 0600 file, without writes."""
    _require(os.getpid() == _MODULE_PID, 'Registry egress cannot be loaded in a forked process.')
    path = Path(path).absolute()
    _require('..' not in path.parts and path.parent.resolve(strict=True) == path.parent,
             'Registry egress configuration requires a canonical real parent directory.')
    raw, signature = _read(path)
    routes = _routes(_json(raw))
    proof = _Proof(os.getpid(), path, signature, hashlib.sha256(raw).hexdigest(), routes)
    result = object.__new__(RegistryEgressMapping)
    for name, value in (('_routes', routes), ('_path', path), ('_digest', proof.digest), ('_pid', proof.pid)):
        object.__setattr__(result, name, value)
    with _PROOFS_LOCK:
        _PROOFS[result] = [proof, False]
    return result


def _checked(mapping):
    _require(os.getpid() == _MODULE_PID, 'Registry egress cannot be used in a forked process.')
    _require(type(mapping) is RegistryEgressMapping, 'An original loaded registry egress mapping is required.')
    with _PROOFS_LOCK:
        state = _PROOFS.get(mapping)
        _require(state is not None, 'Registry egress mapping has no original factory proof.')
        proof = state[0]
    _require(os.getpid() == proof.pid == mapping._pid and mapping._routes == proof.routes
             and mapping._path == proof.path and mapping._digest == proof.digest,
             'Registry egress mapping changed or belongs to another process.')
    _require(proof.path.parent.resolve(strict=True) == proof.path.parent,
             'Registry egress configuration parent changed.')
    raw, signature = _read(proof.path)
    _require(signature == proof.signature and hashlib.sha256(raw).hexdigest() == proof.digest
             and _routes(_json(raw)) == proof.routes,
             'Registry egress configuration changed after loading.')
    return proof


def _environment():
    _require(not any(os.environ.get(name) for name in _FORBIDDEN_ENVIRONMENT),
             'Private registry egress refuses environment proxy, trust, logging or registry-auth overrides.')
    return tuple((name, os.environ.get(name)) for name in _FORBIDDEN_ENVIRONMENT)


class RegistryEgressSession:
    """Detached observations only; cannot activate a route or image capability."""

    __slots__ = ('_snapshot',)

    def __new__(cls, *args, **kwargs):
        raise RegistryEgressError('Registry egress sessions are created by their context only.')

    def __setattr__(self, name, value):
        raise RegistryEgressError('Registry egress sessions are immutable.')

    def metadata(self):
        return self._snapshot()


class _RoutedHTTPSConnection(http.client.HTTPSConnection):
    """Change only this connection's TCP factory, never its TLS hostname."""

    def __init__(self, host, *args, egress_state, **kwargs):
        egress_state.check()
        super().__init__(host, *args, **kwargs)
        _require(type(self.host) is str and self.host in HOSTS
                 and type(self.port) is int and self.port == 443,
                 'Private registry HTTPS handlers accept only the eight exact reviewed hosts on port 443.')
        self._egress_state = egress_state
        self._create_connection = egress_state.connect

    def connect(self):
        self._egress_state.check()
        _require(self._tunnel_host is None, 'HTTP proxy tunnels are not permitted in private registry egress.')
        return super().connect()


class _RoutedHTTPSHandler(urllib.request.HTTPSHandler, urllib.request.ProxyHandler):
    """HTTPS plus an explicit empty proxy policy for this one opener only.

    Being a ProxyHandler also tells urllib.build_opener not to add an ambient
    environment/macOS proxy handler. Its empty proxy mapping adds no HTTP proxy
    methods; HTTPS still uses the ordinary do_open/HTTPSConnection machinery.
    """

    def __init__(self, state, context=None):
        if context is None:
            context = ssl.create_default_context()
        urllib.request.ProxyHandler.__init__(self, proxies={})
        urllib.request.HTTPSHandler.__init__(self, context=context)
        _require(self._context.verify_mode == ssl.CERT_REQUIRED and self._context.check_hostname is True,
                 'Private registry egress requires certificate and hostname verification.')
        self._state = state

    def https_open(self, request):
        self._state.check()
        _require(getattr(request, '_tunnel_host', None) is None,
                 'An ambient HTTP proxy cannot be combined with private registry egress.')

        def connection(host, **kwargs):
            return _RoutedHTTPSConnection(host, egress_state=self._state, **kwargs)

        return self.do_open(connection, request, context=self._context)


def https_handler(context=None):
    """Return ordinary HTTPS by default, or one explicit scoped SSH transport.

    The returned active handler cannot be used after leaving its original
    context or from another process/context. No global opener is installed.
    """
    state = _CURRENT.get()
    if state is None:
        return urllib.request.HTTPSHandler(context=context)
    state.check()
    return _RoutedHTTPSHandler(state, context=context)


@contextmanager
def registry_egress(mapping):
    """Route one private CLI scope through pre-existing SSH loopback forwards.

    Known endpoint failures never fall back to direct access. A scope cannot be
    nested, shared with another concurrent scope, reentered, reused, or used
    after fork. Its ContextVar is reset even on BaseException. Global sockets,
    openers, OS networking, trust settings, authentication, and files do not change.
    """
    proof = _checked(mapping)
    _require(_ACTIVE_LOCK.acquire(blocking=False), 'Another registry egress context is already active.')
    try:
        environment = _environment()
        with _PROOFS_LOCK:
            state = _PROOFS.get(mapping)
            _require(state is not None and state[0] == proof and state[1] is False,
                     'Registry egress mappings are one-use; obtain a fresh reviewed configuration.')
            state[1] = True
        original = socket.create_connection
        _require(callable(original), 'The original TCP connection function is unavailable.')
        ports = dict(proof.routes)
        counters_lock = threading.Lock()
        attempts = dict.fromkeys(HOSTS, 0)
        successes = dict.fromkeys(HOSTS, 0)
        active = True
        failure_type = None
        state = type('_RegistryEgressState', (), {})()

        def check():
            _require(active and os.getpid() == proof.pid,
                     'Registry egress context is closed or belongs to another process.')
            _require(_CURRENT.get() is state, 'Registry egress belongs to another execution context.')
            _checked(mapping)
            _require(_environment() == environment, 'Registry egress environment changed during use.')

        def connect(address, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, source_address=None, *, all_errors=False):
            check()
            host, port = address
            _require(type(host) is str and host in HOSTS and type(port) is int and port == 443,
                     'Private registry TCP forwarding requires an exact reviewed HTTPS endpoint.')
            with counters_lock:
                attempts[host] += 1
            options = {'timeout': timeout, 'source_address': source_address}
            if all_errors:
                options['all_errors'] = all_errors
            connection = original(('127.0.0.1', ports[host]), **options)
            try:
                check()
            except BaseException:
                connection.close()
                raise
            with counters_lock:
                successes[host] += 1
            return connection

        def snapshot():
            if active:
                check()
            with counters_lock:
                value = {
                    'schema_version': 1, 'candidate_schema': SCHEMA,
                    'private_qualification_only': True, 'transport': TRANSPORT,
                    'network_scope': 'explicit-ssh-tcp-egress-not-direct-network',
                    'configuration_sha256': 'sha256:' + proof.digest,
                    'forwards': {host: {'host': '127.0.0.1', 'port': ports[host]} for host in sorted(HOSTS)},
                    'connection_attempts': dict(sorted(attempts.items())),
                    'connection_successes': dict(sorted(successes.items())),
                    'ssh_tcp_routing_used': any(successes.values()), 'active': active,
                    'failure_type': failure_type,
                    'tls_hostname_and_certificate_checks_unchanged': True,
                    'global_socket_routing_changed': False, 'global_urllib_opener_installed': False,
                    'tls_terminated_by_this_module': False, 'http_headers_read_by_this_module': False,
                    'ssh_identity_verified_by_this_module': False,
                    'cloud_credentials_copied_to_relay_by_this_module': False,
                    'cached_registry_bytes_accepted': False, 'serialized_capabilities_restored': False,
                }
            return json.loads(json.dumps(value, sort_keys=True, allow_nan=False))

        session = object.__new__(RegistryEgressSession)
        object.__setattr__(session, '_snapshot', snapshot)
        state.check, state.connect = check, connect
        context_token = _CURRENT.set(state)
        try:
            check()
            yield session
            check()
        except BaseException as exc:
            name = type(exc).__name__
            failure_type = name if re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', name) else 'RegistryEgressError'
            raise
        finally:
            active = False
            _CURRENT.reset(context_token)
    finally:
        # No global socket or opener was installed, including on failures.
        _ACTIVE_LOCK.release()
