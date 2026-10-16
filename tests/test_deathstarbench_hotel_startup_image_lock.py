"""Synthetic immutable-registry tests, not real build/runtime qualification.

Only full source reconstruction and native smoke parsing are unit substitutes;
the original baseline capability and all raw OCI byte checks remain real.
"""

from contextlib import contextmanager
import copy
import unittest
from unittest import mock

from scripts import create_deathstarbench_hotel_startup_image_lock as factory
from scripts.deathstarbench_artifact_common import PreparationError
from tests import test_deathstarbench_hotel_initialization_artifacts as original
from tests import test_deathstarbench_candidate_artifacts as candidate_fixtures
from tests.test_deathstarbench_candidate_image_lock import encoded, write


class StartupHotelFixtureMixin:
    def fixture(self, *, runtime_overrides=None):
        helper = original.GuardedHotelArtifactsTests()
        data = helper.fixture(runtime_overrides=runtime_overrides)
        self.addCleanup(helper.doCleanups)
        data['prepared']['recipe_revision'] = factory.RECIPE_REVISION
        common = factory._identity(data['prepared'], data['baseline'])
        publication = data['publication']
        for name in tuple(publication):
            if name not in ('app', 'baseline_fingerprint', 'upstream_revision'):
                del publication[name]
        publication.update(common, candidate_schema=factory.SCHEMA)
        for platform, value in publication['app']['platforms'].items():
            build = value['build_receipt']
            build.update(common, candidate_schema=factory.BUILD_SCHEMA)
            value['smoke_receipt']['startup_attestations'] = {
                component: {'component': component, 'policy_revision': factory.POLICY_REVISION,
                            'config_sha256': 'sha256:' + common['config_sha256']}
                for component in factory.preparation_module.APPLICATIONS
            }
            value['smoke_receipt'].update({
                key: value for key, value in common.items()
                if key not in ('schema_version', 'workload_id', 'generator_revision')
            }, context_sha256=data['prepared']['context_sha256'])
        root = data['preparation'].parent
        data.update(lock_path=root / 'startup-lock.json', publication_path=root / 'startup-publication.json')
        return data

    @contextmanager
    def evidence(self, data):
        from scripts import smoke_deathstarbench_hotel_startup_images as smoke
        with mock.patch.object(factory, '_preparation', return_value=copy.deepcopy(data['prepared'])) as source, \
                mock.patch.object(smoke, 'validate_startup_hotel_smoke_receipt',
                                  side_effect=lambda receipt, **kwargs: copy.deepcopy(receipt)) as native:
            yield source, native

    def lock(self, data, **changes):
        parameters = {'inspector': data['registry'].inspect, 'blob_inspector': data['registry'].blob}
        parameters.update(changes)
        with self.evidence(data):
            return factory.create_startup_hotel_lock(data['preparation'], data['upstream'],
                                                     data['publication'], data['baseline'], **parameters)

    def records(self, data):
        return [{**factory._identity(data['prepared'], data['baseline']),
                 'candidate_schema': factory.NATIVE_SCHEMA, 'platform': platform, **value}
                for platform, value in data['publication']['app']['platforms'].items()]

    def save(self, data):
        write(data['lock_path'], encoded(self.lock(data)))
        write(data['publication_path'], encoded(data['publication']))


class StartupHotelImageLockTests(StartupHotelFixtureMixin, unittest.TestCase):
    def test_distinct_lock_exact_identity_and_original_reused_bytes(self):
        data = self.fixture()
        inspected = mock.Mock(wraps=data['registry'].inspect)
        blobs = mock.Mock(wraps=data['registry'].blob)
        value = self.lock(data, inspector=inspected, blob_inspector=blobs)
        self.assertEqual(value['candidate_schema'], factory.LOCK_SCHEMA)
        self.assertEqual(value['policy_revision'], factory.POLICY_REVISION)
        self.assertEqual(value['config_sha256'], data['prepared']['config_sha256'])
        self.assertEqual(value['recipe_revision'], factory.RECIPE_REVISION)
        self.assertEqual(set(value['startup_attestations']), set(factory.preparation_module.APPLICATIONS))
        self.assertNotIn('hotel-reservation', value['baseline']['images'])
        self.assertIn('load-driver', value['baseline']['images'])
        for name, image in value['baseline']['images'].items():
            self.assertEqual(image, data['baseline'].metadata()['image_bindings'][name])
            self.assertEqual(set(image['platforms']), {'linux/amd64'})
            self.assertIn(mock.call(image['index_image']), inspected.call_args_list)
            member = image['platforms']['linux/amd64']
            self.assertIn(mock.call(member['image'].split('@')[0], member['config_digest']), blobs.call_args_list)
        for flag in factory.FALSE_FLAGS:
            self.assertIs(value[flag], False, flag)
        self.assertNotIn('images_built', value)
        self.assertNotIn('images_published', value)

    def test_preflight_rechecks_both_native_records_before_assembled_index(self):
        data = self.fixture()
        inspected = mock.Mock(wraps=data['registry'].inspect)
        with self.evidence(data):
            result = factory.preflight_native_startup_hotel(
                data['preparation'], data['upstream'], self.records(data), data['baseline'],
                inspector=inspected, blob_inspector=data['registry'].blob,
            )
        self.assertEqual(result, self.lock(data)['app']['platforms'])
        self.assertNotIn(mock.call(data['publication']['app']['index_image']), inspected.call_args_list)

    def test_source_failure_before_any_registry_observation_in_both_entrypoints(self):
        data = self.fixture()
        for preflight in (False, True):
            inspect, blob = mock.Mock(), mock.Mock()
            with self.subTest(preflight=preflight), \
                    mock.patch.object(factory, '_preparation', side_effect=PreparationError('independent source mismatch')):
                with self.assertRaisesRegex(factory.StartupHotelImageLockError, 'source mismatch'):
                    if preflight:
                        factory.preflight_native_startup_hotel(
                            data['preparation'], data['upstream'], self.records(data), data['baseline'],
                            inspector=inspect, blob_inspector=blob)
                    else:
                        factory.create_startup_hotel_lock(
                            data['preparation'], data['upstream'], data['publication'], data['baseline'],
                            inspector=inspect, blob_inspector=blob)
            inspect.assert_not_called()
            blob.assert_not_called()

    def test_serialized_or_wrong_workload_baseline_rejected_before_registry(self):
        data = self.fixture()
        for baseline in (data['baseline'].metadata(), object(), None):
            inspect, blob = mock.Mock(), mock.Mock()
            with self.evidence(data), self.assertRaises(factory.StartupHotelImageLockError):
                factory.create_startup_hotel_lock(
                    data['preparation'], data['upstream'], data['publication'], baseline,
                    inspector=inspect, blob_inspector=blob)
            inspect.assert_not_called()
            blob.assert_not_called()
        helper = candidate_fixtures.CandidateArtifactAdoptionTests()
        self.addCleanup(helper.doCleanups)
        wrong = helper.adopt(helper.fixture('media_microservices'))
        data['publication']['baseline_fingerprint'] = wrong.fingerprint
        inspect, blob = mock.Mock(), mock.Mock()
        with self.evidence(data), self.assertRaisesRegex(factory.StartupHotelImageLockError, 'baseline workload'):
            factory.create_startup_hotel_lock(
                data['preparation'], data['upstream'], data['publication'], wrong,
                inspector=inspect, blob_inspector=blob)
        inspect.assert_not_called()
        blob.assert_not_called()

    def test_guarded_binding_is_not_an_original_baseline_or_startup_substitute(self):
        helper = original.GuardedHotelArtifactsTests()
        self.addCleanup(helper.doCleanups)
        guarded = helper.adopt(helper.fixture())
        data = self.fixture()
        inspect, blob = mock.Mock(), mock.Mock()
        with self.evidence(data), self.assertRaises(factory.StartupHotelImageLockError):
            factory.create_startup_hotel_lock(
                data['preparation'], data['upstream'], data['publication'], guarded,
                inspector=inspect, blob_inspector=blob)
        inspect.assert_not_called()
        blob.assert_not_called()

    def test_every_publication_identity_field_is_exact_typed(self):
        for field, bad in (
            ('schema_version', True), ('candidate_schema', factory.guarded.SCHEMA),
            ('recipe_revision', factory.guarded.RECIPE_REVISION), ('policy_revision', 'other'),
            ('config_sha256', 'f' * 64), ('source_fingerprint', 'sha256:' + 'f' * 64),
            ('preparation_manifest_sha256', 'a' * 64), ('dataset_revision', 'other'),
            ('generator_revision', 'other'), ('upstream_revision', 'f' * 40),
            ('baseline_fingerprint', 'sha256:' + 'f' * 64), ('unexpected', False),
        ):
            data = self.fixture()
            data['publication'][field] = bad
            with self.subTest(field=field), self.assertRaises(factory.StartupHotelImageLockError):
                self.lock(data)

    def test_every_native_build_identity_field_is_exact_typed(self):
        for field, bad in (
            ('schema_version', True), ('candidate_schema', factory.guarded.BUILD_SCHEMA),
            ('policy_revision', 'other'), ('config_sha256', 'a' * 64),
            ('context_sha256', 'f' * 64), ('recipe_sha256', 'b' * 64),
            ('runner_architecture', 'aarch64'), ('qemu_used', 0),
            ('recipe_revision', factory.guarded.RECIPE_REVISION), ('unexpected', False),
        ):
            data = self.fixture()
            data['publication']['app']['platforms']['linux/amd64']['build_receipt'][field] = bad
            with self.subTest(field=field), self.assertRaises(factory.StartupHotelImageLockError):
                self.lock(data)

    def test_duplicate_missing_or_old_native_records_never_preflight(self):
        data = self.fixture()
        records = self.records(data)
        variants = [[], records[:1], [records[0], records[0]], records + records[:1]]
        old = copy.deepcopy(records)
        old[0]['candidate_schema'] = factory.guarded.NATIVE_SCHEMA
        variants.append(old)
        for records in variants:
            with self.subTest(records=len(records)), self.evidence(data), \
                    self.assertRaises(factory.StartupHotelImageLockError):
                factory.preflight_native_startup_hotel(
                    data['preparation'], data['upstream'], records, data['baseline'],
                    inspector=data['registry'].inspect, blob_inspector=data['registry'].blob)

    def test_complete_cross_architecture_dataset_and_startup_maps_must_match(self):
        for field in ('dataset_attestation', 'startup_attestations'):
            data = self.fixture()
            data['publication']['app']['platforms']['linux/arm64']['smoke_receipt'][field]['tampered'] = True
            with self.subTest(field=field), self.assertRaisesRegex(factory.StartupHotelImageLockError, 'equality'):
                self.lock(data)

    def test_later_local_smoke_source_observation_must_match_original_reconstruction(self):
        for field in ('context_sha256', 'preparation_manifest_sha256', 'policy_revision', 'config_sha256',
                      'recipe_revision', 'dataset_revision', 'source_fingerprint'):
            data = self.fixture()
            data['publication']['app']['platforms']['linux/amd64']['smoke_receipt'][field] = 'changed'
            with self.subTest(field=field), self.assertRaisesRegex(factory.StartupHotelImageLockError, 'reconstructed'):
                self.lock(data)

    def test_original_application_image_cannot_be_relabelled_as_startup(self):
        data = self.fixture()
        data['publication']['app']['platforms']['linux/amd64']['image'] = \
            data['baseline'].image_bindings['hotel-reservation']['platforms']['linux/amd64']['image']
        with self.assertRaisesRegex(factory.StartupHotelImageLockError, 'original Hotel application'):
            self.lock(data)

    def test_fresh_new_and_reused_manifest_config_hashes_are_not_self_reported(self):
        for target in ('new-index', 'new-manifest', 'new-config', 'old-driver-index', 'old-support-config'):
            data = self.fixture()
            pub, registry = data['publication'], data['registry']
            if target == 'new-index':
                registry.manifests[pub['app']['index_image']] += b' '
            elif target == 'new-manifest':
                registry.manifests[pub['app']['platforms']['linux/arm64']['image']] += b' '
            elif target == 'new-config':
                registry.blobs[pub['app']['index_image'].split('@')[0], data['configs']['linux/amd64']] += b' '
            elif target == 'old-driver-index':
                registry.manifests[data['baseline'].image_bindings['load-driver']['index_image']] += b' '
            else:
                image = next(value for name, value in data['baseline'].image_bindings.items()
                             if name not in ('hotel-reservation', 'load-driver'))
                registry.blobs[image['index_image'].split('@')[0], image['platforms']['linux/amd64']['config_digest']] += b' '
            with self.subTest(target=target), self.assertRaises(factory.StartupHotelImageLockError):
                self.lock(data)

    def test_genuinely_rehashed_unsafe_raw_runtime_config_cannot_hide_behind_receipt(self):
        for override in ({'Env': ['TLS=true']}, {'Cmd': ['/bin/other']}, {'User': '0:0'},
                         {'WorkingDir': '/other'}, {'Entrypoint': ['/bin/other']},
                         {'Healthcheck': {'Test': ['CMD', '/bin/other']}}, {'Volumes': {'/data': {}}}):
            data = self.fixture(runtime_overrides=override)
            with self.subTest(override=override), self.assertRaises(factory.StartupHotelImageLockError):
                self.lock(data)

    def test_nonfinite_nonjson_and_unexpected_publication_values_fail_closed(self):
        for value in (float('nan'), float('inf'), object(), {1: 'not-json-key'}):
            data = self.fixture()
            data['publication']['unexpected'] = value
            with self.subTest(value=type(value)), self.assertRaises(factory.StartupHotelImageLockError):
                self.lock(data)

    def test_detached_lock_cannot_mutate_input_evidence(self):
        data = self.fixture()
        before = copy.deepcopy(data['publication'])
        value = self.lock(data)
        value['app']['platforms']['linux/amd64']['native_build_receipt']['config_sha256'] = 'changed'
        value['startup_attestations']['frontend']['component'] = 'changed'
        value['baseline']['images'].clear()
        self.assertEqual(data['publication'], before)
        self.assertTrue(self.lock(data)['baseline']['images'])
