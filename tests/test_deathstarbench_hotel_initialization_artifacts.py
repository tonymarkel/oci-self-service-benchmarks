"""Synthetic guarded evidence tests; no image, database, or cloud execution.

Only independently tested full source reconstruction and native smoke parsing
are replaced. Original adoption proof and all manifest/config byte checks run.
"""

from contextlib import contextmanager
import copy
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
from pathlib import Path
import unittest
from unittest import mock

from app import deathstarbench_hotel_initialization_artifacts as adoption
from app.deathstarbench_candidate_artifacts import CandidateArtifactBinding
from scripts import create_deathstarbench_hotel_initialization_image_lock as factory
from scripts import smoke_deathstarbench_hotel_initialization_images as smoke
from scripts.deathstarbench_artifact_common import PreparationError
from tests import test_deathstarbench_candidate_artifacts as baseline_fixtures
from tests.test_deathstarbench_candidate_image_lock import encoded, digest, write
from tests.test_deathstarbench_hotel_initialization import metadata_fixture, SOURCE


class GuardedHotelArtifactsTests(unittest.TestCase):
    def fixture(self, *, runtime_overrides=None):
        original = baseline_fixtures.CandidateArtifactAdoptionTests()
        data = original.fixture()
        self.addCleanup(original.doCleanups)
        baseline = original.adopt(data)
        registry = data['case'].registry
        repository = baseline.image_bindings['hotel-reservation']['index_image'].split('@')[0]
        descriptors, images, native, configs = [], {}, {}, {}
        runtime = {'User': '65532:65532', 'WorkingDir': '/workspace', 'Cmd': ['/go/bin/frontend'],
                   'Env': list(smoke.ENVIRONMENT)}
        for platform in factory.PLATFORMS:
            config = encoded({'architecture': platform.split('/')[1], 'os': 'linux',
                              'config': {**runtime, **(runtime_overrides or {})}})
            configs[platform] = digest(config)
            registry.blobs[repository, digest(config)] = config
            manifest = encoded({'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                'config': {'mediaType': 'application/vnd.oci.image.config.v1+json', 'digest': digest(config), 'size': len(config)},
                'layers': [{'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip', 'digest': 'sha256:' + 'd' * 64, 'size': 456}]})
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
        prepared = {'manifest': {'synthetic-fixture-only': True},
                    **{name: hashlib.sha256(name.encode()).hexdigest() for name in (
                        'manifest_sha256', 'prepared_tree_sha256', 'context_sha256', 'recipe_sha256', 'config_sha256')},
                    'source_fingerprint': SOURCE, 'recipe_revision': factory.RECIPE_REVISION}
        common = factory._identity(prepared, baseline)
        platforms = {}
        for platform in factory.PLATFORMS:
            platforms[platform] = {'image': images[platform], 'build_receipt': {
                **common, 'candidate_schema': factory.BUILD_SCHEMA,
                'context_name': 'app', 'platform': platform,
                'context_sha256': prepared['context_sha256'], 'recipe_sha256': prepared['recipe_sha256'],
                'runner_architecture': factory.legacy.ARCHITECTURES[platform], 'qemu_used': False,
                'published_index_image': native[platform],
            }, 'smoke_receipt': {'synthetic-fixture-only': True, 'source_fingerprint': SOURCE,
                                 'image_config': {'runtime': copy.deepcopy(runtime)},
                                 'dataset_attestation': metadata_fixture()}}
        publication = {**common, 'candidate_schema': factory.SCHEMA, 'upstream_revision': factory.legacy.UPSTREAM_REVISION,
                       'baseline_fingerprint': baseline.fingerprint, 'app': {'index_image': index, 'platforms': platforms}}
        root = data['case'].workload_path.parent
        return {'baseline': baseline, 'registry': registry, 'prepared': prepared,
                'preparation': data['case'].workload_path, 'upstream': root / 'synthetic-upstream',
                'publication': publication, 'lock_path': root / 'guarded-lock.json',
                'publication_path': root / 'guarded-publication.json', 'configs': configs}

    @contextmanager
    def evidence(self, data):
        # Explicit unit-only substitutes; full source/ELF/command smoke tests
        # live in their own suites and real pinned CI publication workflow.
        with mock.patch.object(factory, '_preparation', return_value=copy.deepcopy(data['prepared'])) as prepared, \
                mock.patch.object(smoke, 'validate_guarded_hotel_smoke_receipt', side_effect=lambda receipt, **kwargs: copy.deepcopy(receipt)) as native:
            yield prepared, native

    def lock(self, data, **changes):
        parameters = dict(inspector=data['registry'].inspect, blob_inspector=data['registry'].blob)
        parameters.update(changes)
        with self.evidence(data):
            return factory.create_guarded_hotel_lock(data['preparation'], data['upstream'],
                                                     data['publication'], data['baseline'], **parameters)

    def adopt(self, data, **changes):
        lock = self.lock(data)
        write(data['lock_path'], encoded(lock))
        write(data['publication_path'], encoded(data['publication']))
        return self.adopt_saved(data, **changes)

    def adopt_saved(self, data, **changes):
        parameters = dict(preparation=data['preparation'], upstream=data['upstream'], baseline=data['baseline'],
                          inspector=data['registry'].inspect, blob_inspector=data['registry'].blob)
        parameters.update(changes)
        with self.evidence(data):
            return adoption.adopt_guarded_hotel_artifacts(data['lock_path'], data['publication_path'], **parameters)

    def test_distinct_source_bound_lock_reuses_only_exact_x86_assets(self):
        data = self.fixture()
        inspect, blob = mock.Mock(wraps=data['registry'].inspect), mock.Mock(wraps=data['registry'].blob)
        lock = self.lock(data, inspector=inspect, blob_inspector=blob)
        self.assertEqual(lock['candidate_schema'], factory.LOCK_SCHEMA)
        self.assertEqual(lock['recipe_revision'], factory.RECIPE_REVISION)
        self.assertEqual(lock['generator_revision'], factory.GENERATOR_REVISION)
        self.assertEqual(lock['dataset_revision'], factory.DATASET_REVISION)
        self.assertEqual(lock['source_fingerprint'], SOURCE)
        self.assertNotIn('hotel-reservation', lock['baseline']['images'])
        self.assertIn('load-driver', lock['baseline']['images'])
        for name, value in lock['baseline']['images'].items():
            self.assertEqual(value, data['baseline'].metadata()['image_bindings'][name])
            self.assertEqual(set(value['platforms']), {'linux/amd64'})
            self.assertIn(mock.call(value['index_image']), inspect.call_args_list)
            item = value['platforms']['linux/amd64']
            self.assertIn(mock.call(item['image'].split('@')[0], item['config_digest']), blob.call_args_list)
        for platform in factory.PLATFORMS:
            self.assertEqual(lock['app']['platforms'][platform]['config_digest'], data['configs'][platform])
        for name in ('initializer_authorized', 'runtime_authorized', 'initialization_executed', 'database_executed',
                     'application_start_allowed', 'dataset_qualified', 'runtime_qualified',
                     'measurement_qualified', 'cloud_qualified', 'public_release', 'anonymous_pull_qualified', 'released'):
            self.assertIs(lock[name], False)

    def test_native_preflight_matches_factory_before_any_assembled_index_access(self):
        data = self.fixture()
        records = [{**factory._identity(data['prepared'], data['baseline']), 'candidate_schema': factory.NATIVE_SCHEMA,
                    'platform': platform, **value} for platform, value in data['publication']['app']['platforms'].items()]
        inspect = mock.Mock(wraps=data['registry'].inspect)
        with self.evidence(data):
            native = factory.preflight_native_guarded_hotel(data['preparation'], data['upstream'], records, data['baseline'],
                                                            inspector=inspect, blob_inspector=data['registry'].blob)
        self.assertEqual(native, self.lock(data)['app']['platforms'])
        self.assertNotIn(mock.call(data['publication']['app']['index_image']), inspect.call_args_list)

    def test_adoption_is_separate_immutable_not_runtime_authority(self):
        data = self.fixture()
        binding = self.adopt(data)
        self.assertIsInstance(binding, adoption.GuardedHotelArtifactBinding)
        self.assertNotIsInstance(binding, CandidateArtifactBinding)
        self.assertFalse(hasattr(binding, 'runtime_manifest'))
        self.assertFalse(hasattr(binding, 'runtime_plan'))
        self.assertFalse(hasattr(type(binding), 'from_metadata'))
        metadata = binding.metadata()
        self.assertEqual(metadata['candidate_schema'], adoption.ADOPTION_SCHEMA)
        for name in ('initializer_authorized', 'runtime_authorized', 'initialization_executed', 'database_executed',
                     'application_start_allowed', 'dataset_qualified', 'runtime_qualified',
                     'measurement_qualified', 'cloud_qualified', 'public_release', 'anonymous_pull_qualified', 'released'):
            self.assertIs(getattr(binding, name), False)
            self.assertIs(metadata[name], False)
        with self.assertRaises(FrozenInstanceError):
            binding.released = True
        with self.assertRaises(TypeError):
            binding.app['platforms']['linux/amd64']['image'] = 'changed'
        metadata['dataset_attestation']['document_count'] = 999
        self.assertEqual(binding.dataset_attestation['document_count'], 873)
        self.assertEqual(replace(binding).metadata(), binding.metadata())
        for change in ({'app': {}}, {'reused_images': {}}, {'dataset_attestation': {}}, {'fingerprint': 'sha256:' + 'a' * 64},
                       {'_metadata_json': '{}'}, {'_proof': object()}):
            with self.subTest(change=change), self.assertRaises(adoption.GuardedHotelArtifactError):
                replace(binding, **change)
        with self.assertRaises(TypeError):
            replace(binding._proof, metadata_json='{}')
        with self.assertRaises(adoption.GuardedHotelArtifactError):
            binding._proof.metadata_json = '{}'

    def test_source_failure_occurs_before_registry_even_for_rehashed_evidence(self):
        data = self.fixture()
        inspect, blob = mock.Mock(), mock.Mock()
        with mock.patch.object(factory, '_preparation', side_effect=PreparationError('source reconstructed differently')):
            with self.assertRaisesRegex(factory.GuardedHotelImageLockError, 'source reconstructed'):
                factory.create_guarded_hotel_lock(data['preparation'], data['upstream'], data['publication'], data['baseline'],
                                                  inspector=inspect, blob_inspector=blob)
        inspect.assert_not_called()
        blob.assert_not_called()

    def test_serialized_or_media_or_forged_baseline_is_not_authority(self):
        data = self.fixture()
        for baseline in (data['baseline'].metadata(), object(), None):
            inspect, blob = mock.Mock(), mock.Mock()
            with self.evidence(data), self.assertRaises(factory.GuardedHotelImageLockError):
                factory.create_guarded_hotel_lock(data['preparation'], data['upstream'], data['publication'], baseline,
                                                  inspector=inspect, blob_inspector=blob)
            inspect.assert_not_called()
            blob.assert_not_called()
        original = baseline_fixtures.CandidateArtifactAdoptionTests()
        media_data = original.fixture('media_microservices')
        self.addCleanup(original.doCleanups)
        media = original.adopt(media_data)
        data['publication']['baseline_fingerprint'] = media.fingerprint
        inspect, blob = mock.Mock(), mock.Mock()
        with self.evidence(data), self.assertRaisesRegex(factory.GuardedHotelImageLockError, 'baseline workload'):
            factory.create_guarded_hotel_lock(data['preparation'], data['upstream'], data['publication'], media,
                                              inspector=inspect, blob_inspector=blob)
        inspect.assert_not_called()
        blob.assert_not_called()

    def test_old_app_platform_cannot_be_reinterpreted_as_guarded(self):
        data = self.fixture()
        data['publication']['app']['platforms']['linux/amd64']['image'] = data['baseline'].image_bindings['hotel-reservation']['platforms']['linux/amd64']['image']
        with self.assertRaisesRegex(factory.GuardedHotelImageLockError, 'original Hotel application'):
            self.lock(data)

    def test_mutated_native_identity_and_architecture_never_pass(self):
        for field, bad in [('schema_version', True), ('candidate_schema', 'candidate-native-publication-v1'),
                           ('context_sha256', 'a' * 64), ('recipe_sha256', 'b' * 64), ('source_fingerprint', 'sha256:' + 'f' * 64),
                           ('generator_revision', 'old'), ('dataset_revision', 'old'), ('recipe_revision', 'old'),
                           ('preparation_manifest_sha256', 'c' * 64), ('runner_architecture', 'aarch64'), ('qemu_used', True),
                           ('unexpected', False)]:
            data = self.fixture()
            data['publication']['app']['platforms']['linux/amd64']['build_receipt'][field] = bad
            with self.subTest(field=field), self.assertRaises(factory.GuardedHotelImageLockError):
                self.lock(data)

    def test_complete_dataset_attestations_must_match_exactly_between_architectures(self):
        data = self.fixture()
        data['publication']['app']['platforms']['linux/arm64']['smoke_receipt']['dataset_attestation']['collections'][0]['content_sha256'] = 'sha256:' + 'f' * 64
        with self.assertRaisesRegex(factory.GuardedHotelImageLockError, 'exact source-bound native dataset equality'):
            self.lock(data)

    def test_tampered_new_or_reused_registry_bytes_are_rejected(self):
        for target in ('new-index', 'new-manifest', 'new-config', 'old-driver-index', 'old-support-config'):
            data = self.fixture()
            pub, registry = data['publication'], data['registry']
            if target == 'new-index':
                registry.manifests[pub['app']['index_image']] += b' '
            elif target == 'new-manifest':
                registry.manifests[pub['app']['platforms']['linux/arm64']['image']] += b' '
            elif target == 'new-config':
                key = (pub['app']['index_image'].split('@')[0], data['configs']['linux/amd64'])
                registry.blobs[key] += b' '
            elif target == 'old-driver-index':
                registry.manifests[data['baseline'].image_bindings['load-driver']['index_image']] += b' '
            else:
                support = next(value for name, value in data['baseline'].image_bindings.items() if name not in ('hotel-reservation', 'load-driver'))
                key = (support['index_image'].split('@')[0], support['platforms']['linux/amd64']['config_digest'])
                registry.blobs[key] += b' '
            with self.subTest(target=target), self.assertRaises(factory.GuardedHotelImageLockError):
                self.lock(data)

    def test_actual_rehashed_runtime_config_is_not_replaced_by_self_reported_safe_receipt(self):
        for overrides in ({'Env': [*smoke.ENVIRONMENT, 'HOTEL_CANDIDATE_RUN_ID=123456789abc']},
                          {'Cmd': ['/bin/unexpected']}, {'Healthcheck': {'Test': ['CMD', '/bin/unexpected']}},
                          {'Entrypoint': ['/bin/unexpected']}, {'Volumes': {'/data': {}}},
                          {'User': '0:0'}, {'WorkingDir': '/other'}, {'AttachStdout': False}):
            # Every registry byte hash, descriptor and reference is genuinely
            # recomputed by fixture construction; only the receipt lies.
            data = self.fixture(runtime_overrides=overrides)
            with self.subTest(overrides=overrides), self.assertRaises(factory.GuardedHotelImageLockError):
                self.lock(data)

    def test_unknown_publication_fields_old_schema_nonfinite_and_boolean_versions_fail(self):
        for change in ('unknown', 'old-schema', 'nonfinite', 'boolean-version', 'crossed-baseline',
                       'nonstring-key', 'nonjson-set', 'surrogate-string'):
            data = self.fixture()
            if change == 'unknown':
                data['publication']['runtime_qualified'] = True
            elif change == 'old-schema':
                data['publication']['candidate_schema'] = 'candidate-publication-v1'
            elif change == 'nonfinite':
                data['publication']['schema_version'] = float('inf')
            elif change == 'boolean-version':
                data['publication']['schema_version'] = True
            elif change == 'nonstring-key':
                data['publication'][123] = 'not-json-key'
            elif change == 'nonjson-set':
                data['publication']['source_fingerprint'] = {'not-json'}
            elif change == 'surrogate-string':
                data['publication']['source_fingerprint'] = '\ud800'
            else:
                data['publication']['baseline_fingerprint'] = 'sha256:' + 'f' * 64
            with self.subTest(change=change), self.assertRaises(factory.GuardedHotelImageLockError):
                self.lock(data)

    def test_adoption_rejects_any_rehashed_lock_field_or_gate_promotion(self):
        for field in ('initializer_authorized', 'runtime_qualified', 'dataset_qualified', 'public_release',
                      'scope', 'dataset_attestation', 'baseline', 'unexpected'):
            data = self.fixture()
            lock = self.lock(data)
            lock[field] = True
            write(data['lock_path'], encoded(lock))
            write(data['publication_path'], encoded(data['publication']))
            with self.subTest(field=field), self.assertRaises(adoption.GuardedHotelArtifactError):
                self.adopt_saved(data)

    def test_adoption_strict_json_rejects_duplicate_overflow_bom_and_links(self):
        for raw in (b'{"schema_version":1,"schema_version":1}', b'{"number":1e999}', b'\xef\xbb\xbf{}', b'{"s":"\\ud800"}'):
            data = self.fixture()
            write(data['lock_path'], raw)
            write(data['publication_path'], encoded(data['publication']))
            with self.subTest(raw=raw), self.assertRaises(adoption.GuardedHotelArtifactError):
                self.adopt_saved(data)
        data = self.fixture()
        self.adopt(data)
        data['lock_path'].unlink()
        data['lock_path'].symlink_to(data['publication_path'])
        with self.assertRaises(adoption.GuardedHotelArtifactError):
            self.adopt_saved(data)


if __name__ == '__main__':
    unittest.main()
