"""Synthetic registry bytes; no network, Docker, database, or cloud execution."""

import copy
from dataclasses import replace
from email.message import Message
import hashlib
import http.client
import io
import json
import os
import pickle
import ssl
import unittest
from unittest import mock
import urllib.error
import urllib.parse

from app import deathstarbench_hotel_image_access as access
from tests import test_deathstarbench_candidate_image_lock as image_fixtures
from tests import test_deathstarbench_hotel_initialization_artifacts as adoption_fixtures


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def digest(value):
    return 'sha256:' + hashlib.sha256(value).hexdigest()


def headers(**values):
    result = Message()
    for key, value in values.items():
        result[key.replace('_', '-')] = value
    return result


class Response:
    def __init__(self, url, body):
        self.url, self.body, self.offset = url, body, 0
        self.status = 200
        self.headers = headers(Content_Length=str(len(body)))
        self.closed = False
        self.read_sizes = []

    def geturl(self):
        return self.url

    def read(self, count):
        self.read_sizes.append(count)
        value = self.body[self.offset:self.offset + count]
        self.offset += len(value)
        return value

    def read1(self, count):
        return self.read(count)

    def close(self):
        self.closed = True


class Registry:
    def __init__(self, fixture):
        self.fixture = fixture
        self.calls, self.responses = [], []
        self.challenge = None
        self.redirect = None
        self.response_mutation = None
        self.body_mutation = None
        self.http_status = None
        self.token_mutation = None
        self.implicit_redirect = False

    def open(self, request, *, timeout):
        url = request.full_url
        parsed = urllib.parse.urlsplit(url)
        request_headers = {key.lower(): value for key, value in request.header_items()}
        self.calls.append((url, request.get_method(), request_headers, timeout))
        if self.http_status is not None:
            raise urllib.error.HTTPError(url, self.http_status, 'synthetic unavailable', headers(), io.BytesIO())
        is_token = (parsed.hostname == 'auth.docker.io' or parsed.hostname == 'ghcr.io' and parsed.path == '/token')
        if is_token:
            query = urllib.parse.parse_qs(parsed.query)
            repository = query['scope'][0].removeprefix('repository:').removesuffix(':pull')
            registry = 'docker.io' if parsed.hostname == 'auth.docker.io' else 'ghcr.io'
            token = 'anonymous.' + hashlib.sha256((registry + '/' + repository).encode()).hexdigest()
            document = {'token': token}
            if self.token_mutation:
                self.token_mutation(document)
            body = encoded(document)
        else:
            registry = 'ghcr.io' if parsed.hostname in {'ghcr.io', 'pkg-containers.githubusercontent.com'} else 'docker.io'
            if parsed.hostname in {'registry-1.docker.io', 'ghcr.io'}:
                repository, kind, selector = parsed.path.removeprefix('/v2/').rsplit('/', 2)
                if 'authorization' not in request_headers:
                    policy = access._REGISTRIES[registry]
                    challenge = f'Bearer realm="{policy["realm"]}",service="{policy["service"]}",scope="repository:{repository}:pull"'
                    if self.challenge is not None:
                        challenge = self.challenge(challenge)
                    raise urllib.error.HTTPError(url, 401, 'anonymous challenge', headers(WWW_Authenticate=challenge), io.BytesIO())
                token = 'Bearer anonymous.' + hashlib.sha256((registry + '/' + repository).encode()).hexdigest()
                if request_headers['authorization'] != token:
                    raise AssertionError('Wrong repository-scoped anonymous token')
                if self.redirect is not None:
                    target = self.redirect(registry, repository, kind, selector)
                    if target:
                        raise urllib.error.HTTPError(url, 307, 'synthetic CDN redirect', headers(Location=target), io.BytesIO())
            else:
                repository, kind, selector = parsed.path.removeprefix('/fixture/').split('/', 2)
                repository = repository.replace('!', '/')
            if kind == 'manifests':
                body = self.fixture.manifests[registry + '/' + repository + '@' + selector]
            elif kind == 'blobs':
                body = self.fixture.blobs[registry + '/' + repository, selector]
            else:
                raise AssertionError('Unexpected synthetic registry endpoint')
        if self.body_mutation:
            body = self.body_mutation(url, body)
        response = Response(url, body)
        if self.implicit_redirect:
            response.url = 'https://attacker.example/secret?TOKEN'
        if self.response_mutation:
            self.response_mutation(response)
        self.responses.append(response)
        return response


class GuardedHotelImageAccessTests(unittest.TestCase):
    def case(self, *, variant='one', layer_change=None, config_change=None):
        fixture = adoption_fixtures.GuardedHotelArtifactsTests()
        self.addCleanup(fixture.doCleanups)

        def full_image(registry, repository, platforms, runtime=None):
            descriptors, images, configs, native = [], {}, {}, {}
            for platform in platforms:
                # A shared digest in different images is intentionally streamed
                # again for every bound reference, never satisfied by a cache.
                layer_body = (b'SYNTHETIC-NOT-A-RUNNABLE-LAYER:' + variant.encode()) * 2000
                registry.blobs[repository, digest(layer_body)] = layer_body
                layer = {'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip',
                         'digest': digest(layer_body), 'size': len(layer_body)}
                if layer_change:
                    layer_change(layer)
                config = {'os': 'linux', 'architecture': platform.split('/')[1],
                          'config': runtime if runtime is not None else {'Labels': {'fixture': repository}},
                          'rootfs': {'type': 'layers', 'diff_ids': [digest(layer_body)]}}
                if config_change:
                    config_change(config)
                config_body = encoded(config)
                configs[platform] = digest(config_body)
                registry.blobs[repository, digest(config_body)] = config_body
                manifest = encoded({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                    'config': {'mediaType': 'application/vnd.oci.image.config.v1+json', 'digest': digest(config_body), 'size': len(config_body)},
                    'layers': [layer]})
                images[platform] = repository + '@' + digest(manifest)
                registry.manifests[images[platform]] = manifest
                descriptor = {'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                              'digest': digest(manifest), 'size': len(manifest),
                              'platform': {'os': 'linux', 'architecture': platform.split('/')[1]}}
                descriptors.append(descriptor)
                raw = encoded({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.index.v1+json', 'manifests': [descriptor]})
                native[platform] = repository + '@' + digest(raw)
                registry.manifests[native[platform]] = raw
            raw = encoded({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.index.v1+json', 'manifests': descriptors})
            index = repository + '@' + digest(raw)
            registry.manifests[index] = raw
            return index, images, configs, native

        with mock.patch.object(image_fixtures.RegistryFixture, 'image', new=full_image):
            data = fixture.fixture()
        publication = data['publication']
        app = publication['app']
        repository = app['index_image'].split('@')[0]
        runtime = app['platforms']['linux/amd64']['smoke_receipt']['image_config']['runtime']
        index, images, configs, native = full_image(data['registry'], repository, access._PLATFORMS, runtime)
        app['index_image'] = index
        for platform in access._PLATFORMS:
            app['platforms'][platform]['image'] = images[platform]
            app['platforms'][platform]['build_receipt']['published_index_image'] = native[platform]
        binding = fixture.adopt(data)
        return binding, Registry(data['registry'])

    def qualify(self, binding, registry):
        with mock.patch.object(access.urllib.request, 'build_opener', return_value=registry):
            return access.qualify_guarded_hotel_image_access(binding)

    def test_all_seven_exact_images_are_freshly_streamed_including_shared_blobs(self):
        binding, registry = self.case()
        capability = self.qualify(binding, registry)
        metadata = capability.metadata()
        self.assertIs(access.require_guarded_hotel_image_access(capability, binding), capability)
        expected_keys = {'hotel-reservation@linux/amd64', 'hotel-reservation@linux/arm64',
                         'consul', 'jaeger', 'memcached', 'mongodb', 'load-driver'}
        self.assertEqual(set(metadata['images']), expected_keys)
        self.assertEqual(len(metadata['indexes']), 6)
        self.assertEqual(len([call for call in registry.calls if '/blobs/' in call[0]]), 14)
        self.assertEqual(len([call for call in registry.calls if '/manifests/' in call[0] and 'authorization' in call[2]]), 13)
        self.assertTrue(metadata['anonymous_all_bound_image_bytes_verified'])
        self.assertTrue(all(metadata[flag] is False for flag in access._CLOSED_FLAGS))
        self.assertFalse(binding.application_start_allowed)
        for key, item in metadata['images'].items():
            self.assertTrue(item['all_descriptor_bodies_sha256_and_size_verified'])
            self.assertEqual(item['platform'], 'linux/arm64' if key.endswith('@linux/arm64') else 'linux/amd64')
        self.assertTrue(all(item.closed for item in registry.responses))
        self.assertTrue(all(count <= access.CHUNK_BYTES for item in registry.responses for count in item.read_sizes))
        self.assertGreater(metadata['downloaded_bytes'], sum(item['compressed_layer_bytes'] for item in metadata['images'].values()))

    def test_every_factory_call_reads_all_bytes_again_without_docker_or_files(self):
        binding, registry = self.case()
        with mock.patch('builtins.open', side_effect=AssertionError('No credential or file reads')), \
                mock.patch('subprocess.run', side_effect=AssertionError('No Docker/CLI')):
            first = self.qualify(binding, registry)
            first_calls = len(registry.calls)
            second = self.qualify(binding, registry)
        self.assertEqual(len(registry.calls), first_calls * 2)
        self.assertIsNot(first, second)
        self.assertNotEqual(first.fingerprint, second.fingerprint)

    def test_registry_environment_proxy_and_default_openers_cannot_supply_credentials(self):
        binding, registry = self.case()
        with mock.patch.dict(os.environ, {'REGISTRY_TOKEN': 'DO-NOT-USE', 'GH_TOKEN': 'DO-NOT-USE',
                                        'REGISTRY_USERNAME': 'private-user', 'DOCKER_CONFIG': '/private/credentials',
                                        'HTTPS_PROXY': 'https://user:password@private-proxy', 'NETRC': '/private/netrc'}), \
                mock.patch.object(access.urllib.request, 'ProxyHandler', wraps=access.urllib.request.ProxyHandler) as proxies, \
                mock.patch.object(access.urllib.request, 'urlopen', side_effect=AssertionError('No shared opener')):
            metadata = self.qualify(binding, registry).metadata()
        proxies.assert_called_once_with({})
        self.assertNotIn('DO-NOT-USE', repr(registry.calls))
        self.assertNotIn('private-user', repr(registry.calls))
        for url, method, request_headers, timeout in registry.calls:
            self.assertEqual(method, 'GET')
            self.assertGreater(timeout, 0)
            self.assertLessEqual(timeout, access.REQUEST_TIMEOUT_SECONDS)
            self.assertNotIn('cookie', request_headers)
            self.assertNotIn('proxy-authorization', request_headers)
            if '/token?' in url:
                self.assertNotIn('authorization', request_headers)
        for key, value in metadata['client'].items():
            self.assertIs(value, key == 'anonymous_repository_scoped_bearer_tokens')

    def test_safe_both_registry_cdns_never_receive_tokens_or_leak_signed_queries(self):
        binding, registry = self.case()
        def redirect(registry_name, repository, kind, selector):
            if kind != 'blobs':
                return None
            host = 'pkg-containers.githubusercontent.com' if registry_name == 'ghcr.io' else 'production.cloudflare.docker.com'
            return 'https://' + host + '/fixture/' + repository.replace('/', '!') + '/blobs/' + selector + '?secret=NEVER-RETAIN'
        registry.redirect = redirect
        metadata = self.qualify(binding, registry).metadata()
        cdn_calls = [call for call in registry.calls if '/fixture/' in call[0]]
        self.assertEqual(len(cdn_calls), 14)
        self.assertTrue(all('authorization' not in call[2] for call in cdn_calls))
        self.assertNotIn('NEVER-RETAIN', json.dumps(metadata))

    def test_strict_realm_service_scope_basic_duplicate_and_escaped_challenges(self):
        binding, original = self.case()
        changes = [lambda v: v.replace('realm="https://', 'realm="http://'),
                   lambda v: v.replace('realm="https://', 'realm="https://attacker.example/'),
                   lambda v: v.replace('service="', 'service="wrong-'),
                   lambda v: v.replace(':pull', ':push'),
                   lambda v: v.replace('scope="repository:', 'scope="repository:foreign/'),
                   lambda v: v + ',service="duplicate"', lambda v: v.replace('Bearer ', 'Basic '),
                   lambda v: v + ',extra="value"', lambda v: v.replace('scope="', 'scope="\\')]
        for change in changes:
            registry = Registry(original.fixture)
            registry.challenge = change
            with self.subTest(change=change), self.assertRaises(access.GuardedHotelImageAccessError):
                self.qualify(binding, registry)
            self.assertFalse(any('/token?' in call[0] for call in registry.calls))

    def test_malformed_conflicting_or_credential_like_token_bodies_fail(self):
        binding, original = self.case()
        mutations = [lambda doc: doc.update(token='secret\r\nX-Credential: value'),
                     lambda doc: doc.update(token=''), lambda doc: doc.update(token=['not-string']),
                     lambda doc: doc.update(access_token='conflicting'),
                     lambda doc: doc.update(token='x' * 16385)]
        for mutation in mutations:
            registry = Registry(original.fixture)
            registry.token_mutation = mutation
            with self.subTest(mutation=mutation), self.assertRaises(access.GuardedHotelImageAccessError):
                self.qualify(binding, registry)

    def test_unreviewed_cross_registry_and_unsafe_redirects_are_not_followed(self):
        binding, original = self.case()
        destinations = ['http://pkg-containers.githubusercontent.com/blob', 'https://attacker.example/blob',
                        'https://127.0.0.1/blob', 'https://pkg-containers.githubusercontent.com:444/blob',
                        'https://user:secret@pkg-containers.githubusercontent.com/blob',
                        'https://pkg-containers.githubusercontent.com/blob#fragment',
                        'https://pkg-containers.githubusercontent.com.evil.example/blob',
                        'https://pkg-containers.githubusercontent.com/blob\n',
                        'https://ghcr.io\\@attacker.example/blob']
        for destination in destinations:
            registry = Registry(original.fixture)
            registry.redirect = lambda *_args: destination
            with self.subTest(destination=destination), self.assertRaises(access.GuardedHotelImageAccessError):
                self.qualify(binding, registry)
            self.assertFalse(any(call[0] == destination for call in registry.calls))
        # Even a separately reviewed Docker endpoint is not a GHCR blob target.
        registry = Registry(original.fixture)
        registry.redirect = lambda reg, _repo, kind, _sha: 'https://production.cloudflare.docker.com/blob' if reg == 'ghcr.io' and kind == 'blobs' else None
        with self.assertRaises(access.GuardedHotelImageAccessError):
            self.qualify(binding, registry)
        self.assertFalse(any('production.cloudflare.docker.com' in call[0] for call in registry.calls))

    def test_manifest_and_token_redirects_and_implicit_redirects_fail_closed(self):
        binding, original = self.case()
        registry = Registry(original.fixture)
        registry.redirect = lambda reg, _repo, kind, _sha: 'https://' + access._REGISTRIES[reg]['host'] + '/unexpected' if kind == 'manifests' else None
        with self.assertRaises(access.GuardedHotelImageAccessError):
            self.qualify(binding, registry)
        self.assertFalse(any('/unexpected' in call[0] for call in registry.calls))
        registry = Registry(original.fixture)
        registry.implicit_redirect = True
        with self.assertRaises(access.GuardedHotelImageAccessError):
            self.qualify(binding, registry)
        self.assertTrue(all(item.closed for item in registry.responses))
        self.assertIsNone(access._NoRedirect().redirect_request(None, None, 307, '', None, 'https://attacker.example/'))

    def test_index_manifest_config_layer_corruption_and_truncation_abort_whole_attempt(self):
        binding, original = self.case()
        indexes = {item['index_image'] for item in access._context(binding)['images'].values()}
        index_digests = {item.split('@')[1] for item in indexes}
        for kind in ('index', 'manifest', 'config', 'layer', 'truncated-layer'):
            registry = Registry(original.fixture)
            def corrupt(url, body):
                selected = ('/manifests/' in url and url.rsplit('/', 1)[1] in index_digests if kind == 'index' else
                            '/manifests/' in url and url.rsplit('/', 1)[1] not in index_digests if kind == 'manifest' else
                            '/blobs/' in url and body.startswith(b'{') if kind == 'config' else
                            '/blobs/' in url and not body.startswith(b'{'))
                return (body[:-1] if kind == 'truncated-layer' else body[:-1] + b'!') if selected else body
            registry.body_mutation = corrupt
            with self.subTest(kind=kind), self.assertRaises(access.GuardedHotelImageAccessError):
                self.qualify(binding, registry)
            self.assertTrue(all(item.closed for item in registry.responses))

    def test_security_headers_partial_status_and_ambiguous_headers_fail_closed(self):
        binding, original = self.case()
        mutations = [lambda r: r.headers.__setitem__('Content-Encoding', 'gzip'),
                     lambda r: r.headers.replace_header('Content-Length', '-1'),
                     lambda r: r.headers.replace_header('Content-Length', '0' * 30),
                     lambda r: r.headers.replace_header('Content-Length', str(access.MAX_TOTAL_BYTES + 1)),
                     lambda r: r.headers.__setitem__('Content-Length', str(len(r.body))),
                     lambda r: r.headers.__setitem__('Content-Encoding', 'identity') or r.headers.__setitem__('Content-Encoding', 'identity'),
                     lambda r: r.headers.__setitem__('Docker-Content-Digest', 'sha256:' + 'e' * 64),
                     lambda r: setattr(r, 'status', 206)]
        for change in mutations:
            registry = Registry(original.fixture)
            # A token has no adopted descriptor digest, so test body headers on
            # exact content responses rather than the legitimate token body.
            registry.response_mutation = lambda r: change(r) if '/token?' not in r.url else None
            with self.subTest(change=change), self.assertRaises(access.GuardedHotelImageAccessError):
                self.qualify(binding, registry)
            self.assertTrue(all(item.closed for item in registry.responses))

    def test_layer_document_total_count_index_and_time_bounds_are_fixed(self):
        binding, original = self.case()
        for constant, limit in (('MAX_DOCUMENT_BYTES', 16), ('MAX_TOKEN_BYTES', 2), ('MAX_LAYER_BYTES', 16),
                                ('MAX_TOTAL_BYTES', 16), ('MAX_LAYERS', 0), ('MAX_INDEX_ENTRIES', 0)):
            with self.subTest(constant=constant), mock.patch.object(access, constant, limit), self.assertRaises(access.GuardedHotelImageAccessError):
                self.qualify(binding, Registry(original.fixture))
        with mock.patch.object(access.time, 'monotonic', side_effect=[1, 10000]), self.assertRaisesRegex(access.GuardedHotelImageAccessError, 'time bound'):
            self.qualify(binding, original)

    def test_huge_chunk_without_content_length_is_not_retained(self):
        binding, registry = self.case()
        def huge(response):
            if '/token?' in response.url:
                return
            del response.headers['Content-Length']
            response.read = lambda count: b'x' * (count + 1)
        registry.response_mutation = huge
        with self.assertRaisesRegex(access.GuardedHotelImageAccessError, 'bounded bytes'):
            self.qualify(binding, registry)
        self.assertTrue(all(item.closed for item in registry.responses))

    def test_foreign_layers_urls_unsupported_types_and_rootfs_fail(self):
        for change in (lambda layer: layer.update(urls=['https://attacker.example/foreign']),
                       lambda layer: layer.update(mediaType='application/vnd.docker.image.rootfs.foreign.diff.tar.gzip')):
            binding, registry = self.case(layer_change=change)
            with self.subTest(change=change), self.assertRaises(access.GuardedHotelImageAccessError):
                self.qualify(binding, registry)
        for change in (lambda config: config['rootfs'].update(diff_ids=[]),
                       lambda config: config['rootfs'].update(diff_ids=['sha256:' + '0' * 64]),
                       lambda config: config['rootfs'].update(type='wrong')):
            binding, registry = self.case(config_change=change)
            with self.subTest(change=change), self.assertRaises(access.GuardedHotelImageAccessError):
                self.qualify(binding, registry)

    def test_failure_messages_redact_network_urls_tokens_and_bodies(self):
        binding, original = self.case()
        for status in (403, 404, 429, 500):
            registry = Registry(original.fixture)
            registry.http_status = status
            with self.subTest(status=status), self.assertRaises(access.GuardedHotelImageAccessError) as raised:
                self.qualify(binding, registry)
            self.assertNotIn('https://', str(raised.exception))
        for error in (urllib.error.URLError('https://signed.example/?TOKEN'),
                      http.client.BadStatusLine('https://signed.example/?TOKEN')):
            with mock.patch.object(access.urllib.request, 'build_opener') as opener:
                opener.return_value.open.side_effect = error
                with self.assertRaises(access.GuardedHotelImageAccessError) as raised:
                    access.qualify_guarded_hotel_image_access(binding)
            self.assertNotIn('TOKEN', str(raised.exception))

    def test_process_local_capability_cannot_restore_copy_cross_or_mutate(self):
        binding, registry = self.case()
        capability = self.qualify(binding, registry)
        with self.assertRaises(access.GuardedHotelImageAccessError):
            access.GuardedHotelImageAccess()
        for value in (capability.metadata(), object.__new__(access.GuardedHotelImageAccess), None):
            with self.subTest(value=value), self.assertRaises(access.GuardedHotelImageAccessError):
                access.require_guarded_hotel_image_access(value, binding)
        for copier in (copy.copy, copy.deepcopy, pickle.dumps):
            with self.subTest(copier=copier), self.assertRaises((TypeError, access.GuardedHotelImageAccessError)):
                copier(capability)
        with self.assertRaises(TypeError):
            replace(capability, _fingerprint='changed')
        with self.assertRaises(access.GuardedHotelImageAccessError):
            capability._fingerprint = 'changed'
        other, _ = self.case(variant='other')
        with self.assertRaisesRegex(access.GuardedHotelImageAccessError, 'crossed'):
            access.require_guarded_hotel_image_access(capability, other)
        with mock.patch.object(access.os, 'getpid', return_value=os.getpid() + 1), self.assertRaises(access.GuardedHotelImageAccessError):
            capability.metadata()
        detached = capability.metadata()
        detached['images'].clear()
        self.assertEqual(len(capability.metadata()['images']), 7)
        object.__setattr__(capability, '_metadata_json', '{}')
        with self.assertRaisesRegex(access.GuardedHotelImageAccessError, 'fields changed'):
            access.require_guarded_hotel_image_access(capability, binding)

    def test_invalid_crossed_or_duplicate_artifacts_are_rejected_before_network(self):
        binding, registry = self.case()
        for value in (binding.metadata(), binding.reused_images, None):
            with self.subTest(value=value), self.assertRaises(access.GuardedHotelImageAccessError):
                self.qualify(value, registry)
        self.assertEqual(registry.calls, [])
        # Bypass Python's frozen assignment only to model an adversarial caller;
        # the original adoption proof must detect tampering before any I/O.
        object.__setattr__(binding, 'reused_images', {'load-driver': binding.reused_images['load-driver']})
        with self.assertRaises(access.GuardedHotelImageAccessError):
            self.qualify(binding, registry)
        self.assertEqual(registry.calls, [])

    def test_rehashed_bad_index_descriptor_inventory_is_still_rejected(self):
        binding, original = self.case()
        base_identity = access._context(binding)
        index_image = base_identity['images']['hotel-reservation@linux/amd64']['index_image']
        mutations = [lambda doc: doc.update(schemaVersion=True), lambda doc: doc.update(mediaType=[]),
                     lambda doc: doc['manifests'].append(copy.deepcopy(doc['manifests'][0])),
                     lambda doc: doc['manifests'].pop(),
                     lambda doc: doc['manifests'][0].update(digest='sha256:' + 'a' * 64),
                     lambda doc: doc['manifests'][0].update(urls=['https://attacker.example/image']),
                     lambda doc: doc['manifests'][0]['platform'].update(variant='v9'),
                     lambda doc: doc['manifests'][0].update(size=True)]
        for mutation in mutations:
            document = json.loads(original.fixture.manifests[index_image])
            mutation(document)
            raw = encoded(document)
            changed_index = index_image.split('@')[0] + '@' + digest(raw)
            source = copy.copy(original.fixture)
            source.manifests = {**original.fixture.manifests, changed_index: raw}
            identity = copy.deepcopy(base_identity)
            for item in identity['images'].values():
                if item['index_image'] == index_image:
                    item['index_image'] = changed_index
            # Directly test the index parser with a fully rehashed synthetic
            # input. No adoption/capability authority is replaced or returned.
            registry = Registry(source)
            with self.subTest(mutation=mutation), \
                    mock.patch.object(access.urllib.request, 'build_opener', return_value=registry), \
                    self.assertRaises(access.GuardedHotelImageAccessError):
                access._selected_descriptors(access._AnonymousClient(), identity)

    def test_token_redirects_duplicate_challenges_and_incomplete_reads_are_closed(self):
        client = access._AnonymousClient()
        for destination in ('https://attacker.example/token', 'https://ghcr.io/unexpected'):
            client.opener = mock.Mock()
            client.opener.open.side_effect = urllib.error.HTTPError('https://ghcr.io/token', 307, '',
                                                                    headers(Location=destination), io.BytesIO())
            with self.subTest(destination=destination), self.assertRaises(access.GuardedHotelImageAccessError):
                client._response('https://ghcr.io/token?service=ghcr.io', 'ghcr.io', None, blob=False, token_request=True)
            self.assertEqual(client.opener.open.call_count, 1)
        duplicated = headers(WWW_Authenticate='Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:example/hotel:pull"')
        duplicated['WWW-Authenticate'] = duplicated['WWW-Authenticate']
        client.opener = mock.Mock()
        client.opener.open.side_effect = urllib.error.HTTPError('https://ghcr.io/v2/example/hotel/manifests/sha256:abc',
                                                                401, '', duplicated, io.BytesIO())
        with self.assertRaisesRegex(access.GuardedHotelImageAccessError, 'repeats'):
            client._response('https://ghcr.io/v2/example/hotel/manifests/sha256:abc', 'ghcr.io', 'example/hotel', blob=False)
        self.assertEqual(client.opener.open.call_count, 1)
        for error in (http.client.IncompleteRead(b'secret', 100), OSError('https://signed.example/?TOKEN')):
            response = Response('https://ghcr.io/v2/example/hotel/blobs/sha256:abc', b'body')
            response.read1 = mock.Mock(side_effect=error)
            with self.subTest(error=type(error)), self.assertRaises(access.GuardedHotelImageAccessError) as raised:
                client._read(response, 10, retain=False)
            self.assertTrue(response.closed)
            self.assertNotIn('secret', str(raised.exception))
            self.assertNotIn('TOKEN', str(raised.exception))

    def test_deadline_is_rechecked_after_each_single_read_without_internal_fill_loop(self):
        response = Response('https://ghcr.io/v2/example/hotel/blobs/sha256:abc', b'body')
        response.read = mock.Mock(side_effect=AssertionError('Do not use potentially looping read()'))
        response.read1 = mock.Mock(return_value=b'b')
        with mock.patch.object(access.time, 'monotonic', side_effect=[1, 2, 10000]):
            client = access._AnonymousClient()
            with self.assertRaisesRegex(access.GuardedHotelImageAccessError, 'time bound'):
                client._read(response, 4, retain=False)
        response.read.assert_not_called()
        response.read1.assert_called_once_with(5)
        self.assertTrue(response.closed)

    def test_strict_json_descriptors_urls_and_tls(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}', b'\xef\xbb\xbf{}',
                    b'{"x":"\\ud800"}', b'[]', b'{"x":' + b'[' * 70 + b'0' + b']' * 70 + b'}'):
            with self.subTest(raw=raw), self.assertRaises(access.GuardedHotelImageAccessError):
                access._json(raw)
        for descriptor in ({'mediaType': [], 'size': 1, 'digest': 'sha256:' + 'a' * 64},
                           {'mediaType': next(iter(access._LAYER_TYPES)), 'size': True, 'digest': 'sha256:' + 'a' * 64},
                           {'mediaType': next(iter(access._LAYER_TYPES)), 'size': 1, 'digest': 'sha256:' + '0' * 64}):
            with self.subTest(descriptor=descriptor), self.assertRaises(access.GuardedHotelImageAccessError):
                access._descriptor(descriptor, access._LAYER_TYPES, access.MAX_LAYER_BYTES)
        for value in ('ghcr.io/example/image:latest', 'https://ghcr.io/example/image', 'ghcr.io/example/image@sha256:' + '0' * 64):
            with self.subTest(value=value), self.assertRaises(access.GuardedHotelImageAccessError):
                access._reference(value)
        context = mock.Mock(verify_mode=ssl.CERT_NONE, check_hostname=False)
        with mock.patch.object(access.ssl, 'create_default_context', return_value=context), self.assertRaises(access.GuardedHotelImageAccessError):
            access._AnonymousClient()


if __name__ == '__main__':
    unittest.main()
