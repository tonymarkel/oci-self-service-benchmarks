"""Synthetic registry bodies only: no actual pulls, containers, or cloud actions."""

import copy
from dataclasses import replace
from email.message import Message
import hashlib
import http.client
import io
import json
import os
import unittest
from unittest import mock
import urllib.error
import urllib.parse

from app import deathstarbench_candidate_image_access as access
from app.deathstarbench_candidate_runtime import create_candidate_runtime_plan
from app.deathstarbench_candidate_stage import build_candidate_support_stage
from tests import test_deathstarbench_candidate_artifacts as artifacts
from tests import test_deathstarbench_candidate_image_lock as image_fixtures


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
    def __init__(self, url, body, *, extra_headers=None):
        self.url, self.body, self.offset = url, body, 0
        self.status = 200
        self.headers = headers(Content_Length=str(len(body)))
        for key, value in (extra_headers or {}).items():
            self.headers[key] = value
        self.closed = False
        self.read_sizes = []

    def geturl(self):
        return self.url

    def read(self, count):
        self.read_sizes.append(count)
        result = self.body[self.offset:self.offset + count]
        self.offset += len(result)
        return result

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

    def open(self, request, *, timeout):
        url = request.full_url
        parsed = urllib.parse.urlsplit(url)
        request_headers = {key.lower(): value for key, value in request.header_items()}
        self.calls.append((url, request.get_method(), request_headers, timeout))
        if self.http_status is not None:
            raise urllib.error.HTTPError(url, self.http_status, 'synthetic registry failure', headers(), io.BytesIO())
        if parsed.hostname == 'auth.docker.io':
            query = urllib.parse.parse_qs(parsed.query)
            repository = query['scope'][0].removeprefix('repository:').removesuffix(':pull')
            body = encoded({'token': 'public.' + hashlib.sha256(repository.encode()).hexdigest()})
        else:
            if parsed.hostname != 'registry-1.docker.io':
                repository, kind, selector = parsed.path.removeprefix('/fixture/').split('/', 2)
                repository = repository.replace('!', '/')
            else:
                repository, kind, selector = parsed.path.removeprefix('/v2/').rsplit('/', 2)
                if 'authorization' not in request_headers:
                    challenge = f'Bearer realm="{access._TOKEN_REALM}",service="registry.docker.io",scope="repository:{repository}:pull"'
                    if self.challenge is not None:
                        challenge = self.challenge(challenge)
                    raise urllib.error.HTTPError(url, 401, 'anonymous challenge', headers(WWW_Authenticate=challenge), io.BytesIO())
                expected = 'Bearer public.' + hashlib.sha256(repository.encode()).hexdigest()
                if request_headers['authorization'] != expected:
                    raise AssertionError('Wrong repository-scoped anonymous bearer')
                if self.redirect is not None and kind == 'blobs':
                    target = self.redirect(repository, selector)
                    if target is not None:
                        raise urllib.error.HTTPError(url, 307, 'synthetic CDN redirect', headers(Location=target), io.BytesIO())
            if kind == 'manifests':
                body = self.fixture.manifests['docker.io/' + repository + '@' + selector]
            elif kind == 'blobs':
                body = self.fixture.blobs['docker.io/' + repository, selector]
            else:
                raise AssertionError('Unexpected request kind')
        if self.body_mutation is not None:
            body = self.body_mutation(url, body)
        response = Response(url, body)
        if self.response_mutation is not None:
            self.response_mutation(response)
        self.responses.append(response)
        return response


class CandidateSupportImageAccessTests(unittest.TestCase):
    def case(self, workload='hotel_reservation', *, layer_change=None, config_change=None):
        fixture = artifacts.CandidateArtifactAdoptionTests()
        self.addCleanup(fixture.doCleanups)
        original_image = image_fixtures.RegistryFixture.image

        def with_support_bodies(registry, repository, platforms):
            if not repository.startswith('docker.io/'):
                return original_image(registry, repository, platforms)
            descriptors, images, configs, native = [], {}, {}, {}
            for platform in platforms:
                layer_body = (b'SYNTHETIC-NOT-A-RUNNABLE-LAYER:' + repository.encode()) * 3000
                layer = {'digest': digest(layer_body), 'size': len(layer_body),
                         'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip'}
                configuration = {'os': 'linux', 'architecture': platform.split('/')[1],
                                 'rootfs': {'type': 'layers', 'diff_ids': [digest(layer_body)]}}
                if config_change is not None:
                    config_change(configuration)
                config_body = encoded(configuration)
                config_digest = digest(config_body)
                registry.blobs[repository, config_digest] = config_body
                registry.blobs[repository, layer['digest']] = layer_body
                if layer_change is not None:
                    layer_change(layer)
                manifest = {'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                            'config': {'mediaType': 'application/vnd.oci.image.config.v1+json',
                                       'digest': config_digest, 'size': len(config_body)}, 'layers': [layer]}
                body = encoded(manifest)
                image = repository + '@' + digest(body)
                registry.manifests[image] = body
                images[platform], configs[platform] = image, config_digest
                descriptor = {'mediaType': manifest['mediaType'], 'digest': digest(body), 'size': len(body),
                              'platform': {'os': 'linux', 'architecture': platform.split('/')[1]}}
                descriptors.append(descriptor)
                index_body = encoded({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.index.v1+json', 'manifests': [descriptor]})
                native[platform] = repository + '@' + digest(index_body)
                registry.manifests[native[platform]] = index_body
            body = encoded({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.index.v1+json', 'manifests': descriptors})
            index = repository + '@' + digest(body)
            registry.manifests[index] = body
            return index, images, configs, native

        with mock.patch.object(image_fixtures.RegistryFixture, 'image', new=with_support_bodies):
            source = fixture.fixture(workload)
            binding = fixture.adopt(source)
        plan = create_candidate_runtime_plan(binding, run_id='a1b2c3d4e5f6', provider='azure',
                    topology_fingerprint='sha256:' + '1' * 64, application_architecture='aarch64',
                    load_generator_ip='10.2.3.4', filesystem_uuid='12345678-1234-4234-8234-123456789abc')
        return plan, build_candidate_support_stage(plan), Registry(source['case'].registry)

    def qualify(self, plan, stage, registry):
        with mock.patch.object(access.urllib.request, 'build_opener', return_value=registry):
            return access.qualify_candidate_support_image_access(plan, stage)

    def test_both_workloads_fetch_every_exact_support_body_without_app_or_driver_access(self):
        for workload, components in (('hotel_reservation', 14), ('media_microservices', 19)):
            with self.subTest(workload=workload):
                plan, stage, registry = self.case(workload)
                proof = self.qualify(plan, stage, registry)
                metadata = proof.metadata()
                self.assertIs(access.require_candidate_support_image_access(proof, plan, stage), proof)
                self.assertEqual(metadata['fingerprint'], proof.fingerprint)
                self.assertEqual(len(metadata['identity']['active_support_components']), components)
                self.assertEqual(set(metadata['images']), {'mongodb', 'jaeger', 'memcached', 'consul' if workload == 'hotel_reservation' else 'redis'})
                self.assertTrue(metadata['anonymous_support_bytes_verified'])
                for key in ('released', 'runtime_qualified', 'measurement_qualified', 'anonymous_pull_qualified',
                            'anonymous_layer_pull_qualified', 'application_start_allowed', 'support_image_execution_performed'):
                    self.assertIs(metadata[key], False)
                self.assertFalse(plan.artifacts.anonymous_pull_qualified)
                self.assertFalse(stage.metadata()['anonymous_pull_qualified'])
                self.assertEqual(len([call for call in registry.calls if '/blobs/' in call[0]]), 8)
                self.assertTrue(all(call[1] == 'GET' and 'ghcr.io' not in call[0] and '/tags/' not in call[0] for call in registry.calls))
                self.assertTrue(all(response.closed for response in registry.responses))
                self.assertTrue(all(size <= access.CHUNK_BYTES for response in registry.responses for size in response.read_sizes))
                self.assertGreater(metadata['downloaded_bytes'], sum(item['compressed_layer_bytes'] for item in metadata['images'].values()))

    def test_environment_registry_and_proxy_credentials_are_never_used(self):
        plan, stage, registry = self.case()
        with mock.patch.dict(os.environ, {'REGISTRY_TOKEN': 'DO-NOT-USE', 'REGISTRY_USERNAME': 'private-user',
                                        'DOCKER_CONFIG': '/private/credentials', 'HTTPS_PROXY': 'https://user:password@private-proxy'}), \
             mock.patch.object(access.urllib.request, 'ProxyHandler', wraps=access.urllib.request.ProxyHandler) as proxies:
            proof = self.qualify(plan, stage, registry)
        proxies.assert_called_once_with({})
        self.assertNotIn('DO-NOT-USE', repr(registry.calls))
        self.assertNotIn('private-user', repr(registry.calls))
        for url, _method, request_headers, _timeout in registry.calls:
            if 'auth.docker.io' in url:
                self.assertNotIn('authorization', request_headers)
        self.assertTrue(all(value is False for key, value in proof.metadata()['client'].items() if key != 'anonymous_repository_scoped_bearer_tokens'))

    def test_safe_blob_cdn_redirect_strips_authorization_and_never_retains_signed_url(self):
        plan, stage, registry = self.case()
        registry.redirect = lambda repo, selector: 'https://production.cloudflare.docker.com/fixture/' + repo.replace('/', '!') + '/blobs/' + selector + '?secret-signed-query=DO-NOT-RETAIN'
        proof = self.qualify(plan, stage, registry)
        cdn = [call for call in registry.calls if 'production.cloudflare.docker.com' in call[0]]
        self.assertEqual(len(cdn), 8)
        self.assertTrue(all('authorization' not in call[2] for call in cdn))
        self.assertNotIn('DO-NOT-RETAIN', json.dumps(proof.metadata()))

    def test_s3_config_and_layers_complete_original_support_factory(self):
        plan, stage, registry = self.case()
        registry.redirect = lambda repo, selector: (
            'https://docker-images-prod.s3.dualstack.us-east-1.amazonaws.com/fixture/'
            + repo.replace('/', '!') + '/blobs/' + selector + '?synthetic-signature=S3-DO-NOT-RETAIN')
        try:
            proof = self.qualify(plan, stage, registry)
        except access.CandidateImageAccessError as exc:
            self.fail('Exact S3 config/layer redirect was rejected: ' + str(exc))
        self.assertIs(access.require_candidate_support_image_access(proof, plan, stage), proof)
        metadata = proof.metadata()
        responses = [r for r in registry.responses if urllib.parse.urlsplit(r.url).hostname ==
                     'docker-images-prod.s3.dualstack.us-east-1.amazonaws.com']
        self.assertEqual(len(responses), 8)
        self.assertEqual(sum(r.body.startswith(b'{') for r in responses), 4)
        for response in responses:
            self.assertEqual(response.offset, len(response.body))
            self.assertTrue(response.closed)
            self.assertEqual(digest(response.body), urllib.parse.urlsplit(response.url).path.rsplit('/', 1)[1])
        self.assertEqual(metadata['downloaded_bytes'], sum(len(r.body) for r in registry.responses))
        self.assertEqual(sum(item['compressed_layer_bytes'] for item in metadata['images'].values()),
                         sum(len(r.body) for r in responses if not r.body.startswith(b'{')))
        self.assertTrue(all('authorization' not in call[2] for call in registry.calls if '/fixture/' in call[0]))
        for forbidden in ('https://', 'synthetic-signature', 'S3-DO-NOT-RETAIN', 'public.'):
            self.assertNotIn(forbidden, json.dumps(metadata))

    def test_s3_nearby_hosts_region_and_wrong_port_are_not_followed(self):
        plan, stage, original = self.case()
        for host in ('docker-images-prod.s3.dualstack.us-east-1.amazonaws.com.evil.example',
                     'docker-images-prod.s3.dualstack.us-west-2.amazonaws.com',
                     'docker-images-prod.s3.dualstack.us-east-1.amazonaws.com:444'):
            registry = Registry(original.fixture)
            destination = 'https://' + host + '/blob?synthetic-signature=DO-NOT-RETAIN'
            registry.redirect = lambda _repo, _selector: destination
            with self.subTest(host=host), self.assertRaises(access.CandidateImageAccessError):
                self.qualify(plan, stage, registry)
            self.assertFalse(any(call[0] == destination for call in registry.calls))

    def test_s3_config_and_layer_corruption_and_truncation_still_fail(self):
        plan, stage, original = self.case()
        for part in ('config', 'layer'):
            for truncate in (False, True):
                registry = Registry(original.fixture)
                registry.redirect = lambda repo, selector: (
                    'https://docker-images-prod.s3.dualstack.us-east-1.amazonaws.com/fixture/'
                    + repo.replace('/', '!') + '/blobs/' + selector)
                def corrupt(url, body):
                    selected = (urllib.parse.urlsplit(url).hostname ==
                                'docker-images-prod.s3.dualstack.us-east-1.amazonaws.com'
                                and body.startswith(b'{') == (part == 'config'))
                    return (body[:-1] if truncate else body[:-1] + b'!') if selected else body
                registry.body_mutation = corrupt
                with self.subTest(part=part, truncate=truncate), self.assertRaisesRegex(
                        access.CandidateImageAccessError, 'SHA-256|Content-Length|body size'):
                    self.qualify(plan, stage, registry)
                self.assertTrue(any('/fixture/' in r.url for r in registry.responses))
                self.assertTrue(all(r.closed for r in registry.responses))

    def test_bad_realm_service_scope_duplicate_and_basic_challenges_fail(self):
        plan, stage, original = self.case()
        changes = [lambda value: value.replace('auth.docker.io', 'attacker.example'),
                   lambda value: value.replace('registry.docker.io', 'wrong-service'),
                   lambda value: value.replace(':pull', ':push'),
                   lambda value: value.replace('scope="repository:', 'scope="repository:foreign/'),
                   lambda value: value + ',realm="' + access._TOKEN_REALM + '"',
                   lambda value: value.replace('Bearer ', 'Basic ')]
        for change in changes:
            registry = Registry(original.fixture)
            # Use the original binding's exact deterministic synthetic bodies.
            registry.challenge = change
            with self.subTest(change=change), self.assertRaises(access.CandidateImageAccessError):
                self.qualify(plan, stage, registry)
            self.assertFalse(any('auth.docker.io' in call[0] for call in registry.calls))

    def test_unsafe_and_unreviewed_redirects_fail_without_following_them(self):
        plan, stage, original = self.case()
        destinations = ['http://production.cloudflare.docker.com/blob', 'https://attacker.example/blob',
                        'https://127.0.0.1/blob', 'https://production.cloudflare.docker.com:444/blob',
                        'https://user:secret@production.cloudflare.docker.com/blob',
                        'https://production.cloudflare.docker.com/blob#fragment']
        for destination in destinations:
            registry = Registry(original.fixture)
            registry.redirect = lambda _repo, _selector: destination
            with self.subTest(destination=destination), self.assertRaises(access.CandidateImageAccessError):
                self.qualify(plan, stage, registry)
            self.assertFalse(any(call[0] == destination for call in registry.calls))

    def test_manifest_config_and_layer_corruption_or_truncation_never_returns_capability(self):
        plan, stage, original = self.case()
        for kind in ('manifest', 'config', 'layer', 'truncated-layer'):
            registry = Registry(original.fixture)
            def corrupt(url, body):
                selected = ('/manifests/' in url if kind == 'manifest' else
                            '/blobs/' in url and body.startswith(b'{') if kind == 'config' else
                            '/blobs/' in url and not body.startswith(b'{'))
                return (body[:-1] if kind == 'truncated-layer' else body[:-1] + b'!') if selected else body
            registry.body_mutation = corrupt
            with self.subTest(kind=kind), self.assertRaises(access.CandidateImageAccessError):
                self.qualify(plan, stage, registry)
            self.assertTrue(all(item.closed for item in registry.responses))

    def test_content_encoding_length_digest_header_and_partial_status_are_rejected(self):
        plan, stage, original = self.case()
        changes = [lambda response: response.headers.__setitem__('Content-Encoding', 'gzip'),
                   lambda response: response.headers.replace_header('Content-Length', '-1'),
                   lambda response: response.headers.replace_header('Content-Length', str(access.MAX_TOTAL_BYTES + 1)),
                   lambda response: response.headers.__setitem__('Docker-Content-Digest', 'sha256:' + 'e' * 64),
                   lambda response: setattr(response, 'status', 206)]
        for change in changes:
            registry = Registry(original.fixture)
            registry.response_mutation = change
            with self.subTest(change=change), self.assertRaises(access.CandidateImageAccessError):
                self.qualify(plan, stage, registry)

    def test_time_document_layer_total_and_count_bounds_fail_closed(self):
        plan, stage, registry = self.case()
        for constant, limit in (('MAX_DOCUMENT_BYTES', 16), ('MAX_LAYER_BYTES', 16), ('MAX_TOTAL_BYTES', 16), ('MAX_LAYERS', 0)):
            with self.subTest(constant=constant), mock.patch.object(access, constant, limit), self.assertRaises(access.CandidateImageAccessError):
                self.qualify(plan, stage, Registry(registry.fixture))
        with mock.patch.object(access.time, 'monotonic', side_effect=[1, 10000]), self.assertRaisesRegex(access.CandidateImageAccessError, 'time bound'):
            self.qualify(plan, stage, registry)

    def test_unsupported_foreign_layer_urls_and_bad_rootfs_are_rejected(self):
        changes = [lambda layer: layer.update(urls=['https://attacker.example/foreign']),
                   lambda layer: layer.update(mediaType='application/vnd.docker.image.rootfs.foreign.diff.tar.gzip')]
        for change in changes:
            plan, stage, registry = self.case(layer_change=change)
            with self.subTest(change=change), self.assertRaises(access.CandidateImageAccessError):
                self.qualify(plan, stage, registry)
        plan, stage, registry = self.case(config_change=lambda config: config['rootfs'].update(diff_ids=[]))
        with self.assertRaisesRegex(access.CandidateImageAccessError, 'rootfs'):
            self.qualify(plan, stage, registry)

    def test_network_denial_rate_limit_and_unavailable_fail_redacted(self):
        plan, stage, original = self.case()
        for status in (403, 404, 429, 500):
            registry = Registry(original.fixture)
            registry.http_status = status
            with self.subTest(status=status), self.assertRaises(access.CandidateImageAccessError) as raised:
                self.qualify(plan, stage, registry)
            self.assertNotIn('https://', str(raised.exception))
        with mock.patch.object(access.urllib.request, 'build_opener') as constructor:
            constructor.return_value.open.side_effect = urllib.error.URLError('https://secret.example/signed?secret=TOKEN')
            with self.assertRaises(access.CandidateImageAccessError) as raised:
                access.qualify_candidate_support_image_access(plan, stage)
        self.assertNotIn('TOKEN', str(raised.exception))
        with mock.patch.object(access.urllib.request, 'build_opener') as constructor:
            constructor.return_value.open.side_effect = http.client.BadStatusLine('https://secret.example/signed?secret=TOKEN')
            with self.assertRaises(access.CandidateImageAccessError) as raised:
                access.qualify_candidate_support_image_access(plan, stage)
        self.assertNotIn('TOKEN', str(raised.exception))

    def test_capability_cannot_be_constructed_restored_copied_changed_or_crossed(self):
        plan, stage, registry = self.case()
        proof = self.qualify(plan, stage, registry)
        with self.assertRaises(access.CandidateImageAccessError):
            access.CandidateSupportImageAccess()
        for value in (proof.metadata(), object.__new__(access.CandidateSupportImageAccess), None):
            with self.assertRaises(access.CandidateImageAccessError):
                access.require_candidate_support_image_access(value, plan, stage)
        with self.assertRaises((TypeError, access.CandidateImageAccessError)):
            copy.copy(proof)
        with self.assertRaises(TypeError):
            replace(proof, _fingerprint='changed')
        with self.assertRaises(access.CandidateImageAccessError):
            proof._fingerprint = 'changed'
        other = create_candidate_runtime_plan(plan.artifacts, run_id='123456abcdef', provider=plan.provider,
                    topology_fingerprint=plan.topology_fingerprint, application_architecture=plan.bundle.application_architecture,
                    load_generator_ip='10.2.3.4', filesystem_uuid=plan.storage.filesystem_uuid)
        with self.assertRaisesRegex(access.CandidateImageAccessError, 'crossed'):
            access.require_candidate_support_image_access(proof, other, build_candidate_support_stage(other))
        metadata = proof.metadata()
        metadata['anonymous_pull_qualified'] = True
        self.assertFalse(proof.metadata()['anonymous_pull_qualified'])
        with mock.patch.object(access.os, 'getpid', return_value=os.getpid() + 1), self.assertRaises(access.CandidateImageAccessError):
            access.require_candidate_support_image_access(proof, plan, stage)
        original = proof._metadata_json
        object.__setattr__(proof, '_metadata_json', original.replace('"released":false', '"released":true'))
        with self.assertRaisesRegex(access.CandidateImageAccessError, 'fields changed'):
            access.require_candidate_support_image_access(proof, plan, stage)

    def test_crossed_or_started_stage_is_rejected_before_network(self):
        plan, stage, registry = self.case()
        other_plan, other_stage, _ = self.case('media_microservices')
        with self.assertRaises(access.CandidateImageAccessError):
            self.qualify(plan, other_stage, registry)
        self.assertEqual(registry.calls, [])
        object.__setattr__(stage, 'canonical_json', stage.canonical_json.replace('"replicas":0', '"replicas":1'))
        with self.assertRaises(access.CandidateImageAccessError):
            self.qualify(plan, stage, registry)
        self.assertEqual(registry.calls, [])

    def test_strict_json_and_descriptor_shapes_fail_closed(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}', b'\xef\xbb\xbf{}', b'{"x":"\\ud800"}', b'[]'):
            with self.subTest(raw=raw), self.assertRaises(access.CandidateImageAccessError):
                access._json(raw)
        for item in ({'mediaType': [], 'size': 1, 'digest': 'sha256:' + 'a' * 64},
                     {'mediaType': next(iter(access._LAYER_TYPES)), 'size': True, 'digest': 'sha256:' + 'a' * 64}):
            with self.assertRaises(access.CandidateImageAccessError):
                access._descriptor(item, access._LAYER_TYPES, access.MAX_LAYER_BYTES)


if __name__ == '__main__':
    unittest.main()
