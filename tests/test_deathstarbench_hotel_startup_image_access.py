"""Real synthetic OCI bytes for the distinct startup access boundary.

Source/native fixtures are explicit unit substitutes; adoption and each raw
index, manifest, config and compressed-layer hash/length check remain real.
No network, cloud, database, Docker daemon or application is executed.
"""

import copy
from dataclasses import replace
import json
import os
import pickle
import unittest
from unittest import mock
import urllib.error

from app import deathstarbench_hotel_image_access as shared
from app import deathstarbench_hotel_initialization_operator as old_operator
from app import deathstarbench_hotel_runtime as old_runtime
from app import deathstarbench_hotel_startup_artifacts as adoption
from app import deathstarbench_hotel_startup_image_access as access
from tests import test_deathstarbench_candidate_image_lock as image_fixtures
from tests import test_deathstarbench_hotel_image_access as old_fixtures
from tests.test_deathstarbench_hotel_image_access import Registry, digest, encoded
from tests.test_deathstarbench_hotel_startup_image_lock import StartupHotelFixtureMixin


class StartupHotelImageAccessTests(StartupHotelFixtureMixin, unittest.TestCase):
    def case(self, *, variant='one', layer_change=None, config_change=None):
        def full_image(registry, repository, platforms, runtime=None):
            descriptors, images, configs, native = [], {}, {}, {}
            for platform in platforms:
                # Identical shared bodies still have to be streamed once per
                # bound image, including both native application manifests.
                body = (b'SYNTHETIC-STARTUP-NOT-RUNNABLE:' + variant.encode()) * 2000
                registry.blobs[repository, digest(body)] = body
                layer = {'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip',
                         'digest': digest(body), 'size': len(body)}
                if layer_change:
                    layer_change(layer)
                configuration = {'os': 'linux', 'architecture': platform.split('/')[1],
                                 'config': runtime if runtime is not None else {'Labels': {'fixture': repository}},
                                 'rootfs': {'type': 'layers', 'diff_ids': [digest(body)]}}
                if config_change:
                    config_change(configuration)
                config_raw = encoded(configuration)
                configs[platform] = digest(config_raw)
                registry.blobs[repository, digest(config_raw)] = config_raw
                raw = encoded({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                    'config': {'mediaType': 'application/vnd.oci.image.config.v1+json',
                               'digest': digest(config_raw), 'size': len(config_raw)}, 'layers': [layer]})
                images[platform] = repository + '@' + digest(raw)
                registry.manifests[images[platform]] = raw
                descriptor = {'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                              'digest': digest(raw), 'size': len(raw),
                              'platform': {'os': 'linux', 'architecture': platform.split('/')[1]}}
                descriptors.append(descriptor)
                single = encoded({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.index.v1+json',
                                  'manifests': [descriptor]})
                native[platform] = repository + '@' + digest(single)
                registry.manifests[native[platform]] = single
            raw = encoded({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.index.v1+json',
                           'manifests': descriptors})
            index = repository + '@' + digest(raw)
            registry.manifests[index] = raw
            return index, images, configs, native

        with mock.patch.object(image_fixtures.RegistryFixture, 'image', new=full_image):
            data = self.fixture()
        app = data['publication']['app']
        runtime = app['platforms']['linux/amd64']['smoke_receipt']['image_config']['runtime']
        index, images, _configs, native = full_image(
            data['registry'], app['index_image'].split('@')[0], shared._PLATFORMS, runtime)
        app['index_image'] = index
        for platform in shared._PLATFORMS:
            app['platforms'][platform]['image'] = images[platform]
            app['platforms'][platform]['build_receipt']['published_index_image'] = native[platform]
        self.save(data)
        with self.evidence(data):
            binding = adoption.adopt_startup_hotel_artifacts(
                data['lock_path'], data['publication_path'], preparation=data['preparation'],
                upstream=data['upstream'], baseline=data['baseline'], inspector=data['registry'].inspect,
                blob_inspector=data['registry'].blob)
        return binding, Registry(data['registry'])

    def qualify(self, binding, registry):
        with mock.patch.object(shared.urllib.request, 'build_opener', return_value=registry):
            return access.qualify_startup_hotel_image_access(binding)

    def test_exact_seven_platform_bodies_including_shared_layers_are_verified(self):
        binding, registry = self.case()
        capability = self.qualify(binding, registry)
        metadata = capability.metadata()
        self.assertIs(type(capability), access.StartupHotelImageAccess)
        self.assertIs(access.require_startup_hotel_image_access(capability, binding), capability)
        self.assertEqual(set(metadata['images']), {'hotel-reservation@linux/amd64',
            'hotel-reservation@linux/arm64', 'consul', 'jaeger', 'memcached', 'mongodb', 'load-driver'})
        self.assertEqual(len(metadata['indexes']), 6)
        self.assertEqual(len([call for call in registry.calls if '/blobs/' in call[0]]), 14)
        self.assertEqual(len([call for call in registry.calls if '/manifests/' in call[0]
                              and 'authorization' in call[2]]), 13)
        self.assertTrue(metadata['anonymous_all_bound_image_bytes_verified'])
        for flag in access._CLOSED_FLAGS:
            self.assertIs(metadata[flag], False, flag)
        for flag in ('application_start_allowed', 'application_started', 'application_ready',
                     'initializer_authorized', 'runtime_authorized', 'runtime_qualified', 'public_release'):
            self.assertIs(metadata[flag], False, flag)
        self.assertEqual(metadata['identity']['artifact_fingerprint'], binding.fingerprint)
        for key, result in metadata['images'].items():
            platform = 'linux/arm64' if key.endswith('@linux/arm64') else 'linux/amd64'
            self.assertEqual(result['platform'], platform)
            source = binding.app if key.startswith('hotel-reservation@') else binding.reused_images[key]
            self.assertEqual(result['image'], source['platforms'][platform]['image'])
            self.assertEqual(result['index_image'], source['index_image'])
            self.assertTrue(result['all_descriptor_bodies_sha256_and_size_verified'])
        self.assertTrue(all(response.closed for response in registry.responses))
        self.assertTrue(all(size <= shared.CHUNK_BYTES for response in registry.responses for size in response.read_sizes))

    def test_each_factory_attempt_repeats_all_bytes_without_files_or_subprocesses(self):
        binding, registry = self.case()
        with mock.patch('builtins.open', side_effect=AssertionError('No credential/file access')), \
                mock.patch('subprocess.run', side_effect=AssertionError('No Docker/CLI')):
            first = self.qualify(binding, registry)
            count = len(registry.calls)
            second = self.qualify(binding, registry)
        self.assertEqual(len(registry.calls), count * 2)
        self.assertIsNot(first, second)
        self.assertNotEqual(first.fingerprint, second.fingerprint)

    def test_metadata_contains_exact_startup_recipe_not_old_guarded_identity(self):
        binding, registry = self.case()
        identity = self.qualify(binding, registry).metadata()['identity']
        original = binding.metadata()
        for name in ('recipe_revision', 'policy_revision', 'source_fingerprint', 'config_sha256'):
            self.assertEqual(identity[name], original[name], name)
        self.assertNotEqual(access.ACCESS_SCHEMA, shared.ACCESS_SCHEMA)

    def test_saved_sources_metadata_wrong_artifacts_and_old_binding_fail_before_network(self):
        binding, registry = self.case()
        helper = old_fixtures.GuardedHotelImageAccessTests()
        self.addCleanup(helper.doCleanups)
        old_binding, _ = helper.case()
        for supplied in (binding.metadata(), binding.metadata()['preparation'], binding.reused_images,
                         old_binding, object(), None):
            with self.subTest(supplied=type(supplied)), self.assertRaises(access.StartupHotelImageAccessError):
                self.qualify(supplied, registry)
        self.assertEqual(registry.calls, [])

    def test_tampered_original_artifact_proof_fails_before_network(self):
        for field, bad in (('reused_images', {}), ('app', {}), ('startup_attestations', {}),
                           ('fingerprint', 'sha256:' + 'f' * 64), ('_metadata_json', '{}')):
            binding, registry = self.case()
            object.__setattr__(binding, field, bad)
            with self.subTest(field=field), self.assertRaises(access.StartupHotelImageAccessError):
                self.qualify(binding, registry)
            self.assertEqual(registry.calls, [])

    def test_artifact_is_rechecked_after_all_slow_body_io(self):
        binding, registry = self.case()
        registry.response_mutation = lambda response: object.__setattr__(binding, '_metadata_json', '{}')
        with self.assertRaises(access.StartupHotelImageAccessError):
            self.qualify(binding, registry)
        self.assertTrue(registry.calls)

    def test_original_factory_capability_cannot_restore_copy_serialize_or_change(self):
        binding, registry = self.case()
        capability = self.qualify(binding, registry)
        with self.assertRaises(access.StartupHotelImageAccessError):
            access.StartupHotelImageAccess()
        for supplied in (capability.metadata(), object.__new__(access.StartupHotelImageAccess), None):
            with self.subTest(supplied=type(supplied)), self.assertRaises(access.StartupHotelImageAccessError):
                access.require_startup_hotel_image_access(supplied, binding)
        for copier in (copy.copy, copy.deepcopy, pickle.dumps):
            with self.subTest(copier=copier), self.assertRaises((TypeError, access.StartupHotelImageAccessError)):
                copier(capability)
        with self.assertRaises(TypeError):
            replace(capability, _fingerprint='changed')
        with self.assertRaises(access.StartupHotelImageAccessError):
            capability._fingerprint = 'changed'
        with mock.patch.object(access.os, 'getpid', return_value=os.getpid() + 1), \
                self.assertRaises(access.StartupHotelImageAccessError):
            capability.metadata()
        detached = capability.metadata()
        detached['images'].clear()
        self.assertEqual(len(capability.metadata()['images']), 7)
        object.__setattr__(capability, '_metadata_json', '{}')
        with self.assertRaises(access.StartupHotelImageAccessError):
            access.require_startup_hotel_image_access(capability, binding)

    def test_access_capability_is_bound_to_exact_adoption_and_cannot_cross_old_factories(self):
        binding, registry = self.case()
        capability = self.qualify(binding, registry)
        other, _ = self.case(variant='different-reviewed-bytes')
        with self.assertRaises(access.StartupHotelImageAccessError):
            access.require_startup_hotel_image_access(capability, other)
        helper = old_fixtures.GuardedHotelImageAccessTests()
        self.addCleanup(helper.doCleanups)
        old_binding, old_registry = helper.case()
        old_capability = helper.qualify(old_binding, old_registry)
        with self.assertRaises(shared.GuardedHotelImageAccessError):
            shared.require_guarded_hotel_image_access(capability, old_binding)
        with self.assertRaises(access.StartupHotelImageAccessError):
            access.require_startup_hotel_image_access(old_capability, binding)
        for supplied in (binding, capability):
            with self.subTest(supplied=type(supplied)), self.assertRaises(old_runtime.GuardedHotelRuntimeError):
                old_runtime._checked_inputs(supplied, None, None)
            with self.subTest(supplied=type(supplied)), self.assertRaises(old_operator.HotelInitializationOperatorError):
                old_operator.make_hotel_initialization_pin('0123456789ab', {}, supplied)

    def test_corrupt_and_truncated_each_body_kind_abort_without_receipt(self):
        for part in ('index', 'manifest', 'config', 'layer'):
            for mutation in (lambda raw: raw[:-1], lambda raw: raw[:-1] + bytes([raw[-1] ^ 1])):
                binding, registry = self.case()
                app = binding.app
                manifest_ref = app['platforms']['linux/amd64']['image']
                document = json.loads(registry.fixture.manifests[manifest_ref])
                selector = {'index': app['index_image'].split('@')[1],
                            'manifest': manifest_ref.split('@')[1],
                            'config': document['config']['digest'], 'layer': document['layers'][0]['digest']}[part]
                registry.body_mutation = lambda url, body: mutation(body) if url.endswith('/' + selector) else body
                with self.subTest(part=part, mutation=mutation), self.assertRaises(access.StartupHotelImageAccessError):
                    self.qualify(binding, registry)

    def test_shared_security_guards_still_apply_to_startup_downloads(self):
        changes = [lambda r: setattr(r, 'http_status', 206),
                   lambda r: setattr(r, 'implicit_redirect', True),
                   lambda r: setattr(r, 'challenge', lambda value: value.replace(':pull', ':push')),
                   lambda r: setattr(r, 'redirect', lambda *args: 'https://attacker.example/?secret=REDACT'),
                   lambda r: setattr(r, 'response_mutation', lambda response: response.headers.add_header('Content-Length', '1'))]
        for change in changes:
            binding, registry = self.case()
            change(registry)
            with self.subTest(change=change), self.assertRaises(access.StartupHotelImageAccessError):
                self.qualify(binding, registry)

    def test_allowed_cdn_redirects_drop_tokens_and_signed_urls_are_not_retained(self):
        binding, registry = self.case()
        def redirect(registry_name, repository, kind, selector):
            if kind != 'blobs':
                return None
            host = 'pkg-containers.githubusercontent.com' if registry_name == 'ghcr.io' else 'production.cloudflare.docker.com'
            return 'https://' + host + '/fixture/' + repository.replace('/', '!') + '/blobs/' + selector + '?secret=REDACT'
        registry.redirect = redirect
        metadata = self.qualify(binding, registry).metadata()
        redirected = [call for call in registry.calls if '/fixture/' in call[0]]
        self.assertEqual(len(redirected), 14)
        self.assertTrue(all('authorization' not in call[2] for call in redirected))
        self.assertNotIn('REDACT', json.dumps(metadata))

    def test_current_fixed_byte_and_time_bounds_are_not_bypassable(self):
        for field, value in (('MAX_TOTAL_BYTES', 10), ('MAX_DOCUMENT_BYTES', 10),
                             ('MAX_LAYER_BYTES', 10), ('MAX_LAYERS', 0), ('MAX_INDEX_ENTRIES', 0),
                             ('TOTAL_TIMEOUT_SECONDS', -1)):
            binding, registry = self.case()
            with self.subTest(field=field), mock.patch.object(shared, field, value), \
                    self.assertRaises(access.StartupHotelImageAccessError):
                self.qualify(binding, registry)

    def test_ambient_credentials_proxies_and_shared_openers_do_not_enter_requests(self):
        binding, registry = self.case()
        with mock.patch.dict(os.environ, {'GH_TOKEN': 'NEVER-USE', 'REGISTRY_TOKEN': 'NEVER-USE',
                'HTTPS_PROXY': 'https://private:NEVER-USE@proxy', 'NETRC': '/private/netrc'}), \
                mock.patch.object(shared.urllib.request, 'ProxyHandler', wraps=shared.urllib.request.ProxyHandler) as proxies, \
                mock.patch.object(shared.urllib.request, 'urlopen', side_effect=AssertionError('No shared opener')):
            self.qualify(binding, registry)
        proxies.assert_called_once_with({})
        self.assertNotIn('NEVER-USE', repr(registry.calls))
        self.assertTrue(all('cookie' not in call[2] and 'proxy-authorization' not in call[2] for call in registry.calls))

    def test_network_failures_cannot_leak_exception_urls_or_tokens(self):
        binding, _ = self.case()
        for error in (OSError('https://private.example/?TOKEN'), urllib.error.URLError('Bearer TOKEN')):
            with mock.patch.object(shared.urllib.request, 'build_opener') as opener:
                opener.return_value.open.side_effect = error
                with self.subTest(error=type(error)), self.assertRaises(access.StartupHotelImageAccessError) as raised:
                    access.qualify_startup_hotel_image_access(binding)
            self.assertNotIn('TOKEN', str(raised.exception))
            self.assertNotIn('private.example', str(raised.exception))

    def test_tls_trust_and_key_logging_overrides_refuse_before_transport(self):
        binding, _ = self.case()
        for name in access._TRUST_ENVIRONMENT:
            with mock.patch.dict(os.environ, {name: 'PRIVATE-OVERRIDE'}, clear=True), \
                    mock.patch.object(shared.urllib.request, 'build_opener') as opener, \
                    self.subTest(name=name), self.assertRaises(access.StartupHotelImageAccessError) as raised:
                access.qualify_startup_hotel_image_access(binding)
            opener.assert_not_called()
            self.assertNotIn('PRIVATE-OVERRIDE', str(raised.exception))

    def test_tls_override_introduced_during_body_io_prevents_capability(self):
        binding, registry = self.case()
        with mock.patch.dict(os.environ, {}, clear=True):
            registry.response_mutation = lambda response: os.environ.update(SSLKEYLOGFILE='/private/NEVER-WRITE')
            with self.assertRaises(access.StartupHotelImageAccessError) as raised:
                self.qualify(binding, registry)
        self.assertNotIn('NEVER-WRITE', str(raised.exception))


if __name__ == '__main__':
    unittest.main()
