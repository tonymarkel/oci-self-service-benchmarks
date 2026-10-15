"""Synthetic offline identities: no live cluster, database, or execution proof."""

from dataclasses import FrozenInstanceError, replace
import unittest
from unittest import mock

from app import deathstarbench_hotel_runtime as runtime
from app.deathstarbench_candidate_runtime import create_candidate_runtime_plan
from app.deathstarbench_candidate_stage import build_candidate_support_stage
from tests.test_deathstarbench_candidate_image_lock import encoded, write
from tests import test_deathstarbench_candidate_artifacts as baseline_fixtures
from tests import test_deathstarbench_hotel_initialization_artifacts as guarded_fixtures


RUN = '123456abcdef'
FS_UUID = '12345678-1234-1234-1234-123456789abc'
ATTEMPT = '87654321-4321-4321-4321-cba987654321'
TOPOLOGY = 'sha256:' + '1' * 64


def uid(number):
    return '00000000-0000-0000-0000-' + str(number).zfill(12)


class GuardedHotelRuntimeTests(unittest.TestCase):
    def setUp(self):
        # Only the separately tested preparation/native smoke boundaries are
        # stubbed by this existing synthetic helper. Both adoption proofs and
        # all immutable manifest/config/baseline comparisons still execute.
        self.helper = guarded_fixtures.GuardedHotelArtifactsTests()
        self.addCleanup(self.helper.doCleanups)
        self.data = self.helper.fixture()
        self.artifacts = self.helper.adopt(self.data)
        self.support_plan = self.support()
        self.support_stage = build_candidate_support_stage(self.support_plan)

    def support(self, **overrides):
        options = dict(run_id=RUN, provider='azure', topology_fingerprint=TOPOLOGY,
                       application_architecture='aarch64', load_generator_ip='10.240.2.10',
                       filesystem_uuid=FS_UUID)
        options.update(overrides)
        return create_candidate_runtime_plan(self.data['baseline'], **options)

    def plan(self, **overrides):
        options = dict(artifacts=self.artifacts, support_plan=self.support_plan,
                       support_stage=self.support_stage, initialization_attempt_id=ATTEMPT)
        options.update(overrides)
        return runtime.create_guarded_hotel_runtime_plan(**options)

    def databases(self):
        return tuple(runtime.GuardedHotelDatabaseObservation(
            component=component, pvc_name=component, pvc_uid=uid(10 + index * 3),
            pv_name=runtime.NAMESPACE + '-' + component, pv_uid=uid(11 + index * 3),
            pod_name=component + '-deadbeef-abc12', pod_uid=uid(12 + index * 3),
        ) for index, component in enumerate(runtime._DATABASES))

    def observation(self, plan=None, **overrides):
        plan = plan or self.plan()
        options = dict(server_ca_sha256='sha256:' + '2' * 64, kube_system_namespace_uid=uid(1),
                       namespace_uid=uid(2), database_node_name='database.internal', database_node_uid=uid(3),
                       database_filesystem_uuid=FS_UUID, initializer_pod_name=plan.initializer_pod_name,
                       initializer_pod_uid=uid(4), databases=self.databases())
        options.update(overrides)
        return runtime.observe_guarded_hotel_cluster_identity(plan, **options)

    def test_exact_original_support_and_new_guarded_recipe_are_separate(self):
        plan = self.plan()
        saved = plan.snapshot()
        self.assertEqual(saved['candidate_schema'], runtime.SNAPSHOT_SCHEMA)
        self.assertEqual(saved['runtime_revision'], runtime.RUNTIME_REVISION)
        self.assertEqual(saved['support_plan_fingerprint'], self.support_plan.fingerprint)
        self.assertEqual(saved['support_stage_fingerprint'], self.support_stage.fingerprint)
        self.assertEqual(saved['original_artifact_fingerprint'], self.data['baseline'].fingerprint)
        self.assertEqual(saved['guarded_artifact_fingerprint'], self.artifacts.fingerprint)
        self.assertEqual(saved['dataset_revision'], runtime.DATASET_REVISION)
        self.assertEqual(saved['recipe_revision'], runtime.RECIPE_REVISION)
        self.assertEqual(saved['reused_images'], self.artifacts.metadata()['reused_images'])
        self.assertEqual(plan.application_image, self.artifacts.app['platforms']['linux/arm64']['image'])
        self.assertNotEqual(plan.application_image, self.data['baseline'].image_bindings['hotel-reservation']['platforms']['linux/arm64']['image'])
        self.assertEqual(plan.run_id, RUN)
        self.assertEqual(plan.provider, 'azure')
        self.assertEqual(plan.topology_fingerprint, TOPOLOGY)
        self.assertEqual(plan.namespace, runtime.NAMESPACE)
        self.assertEqual(plan.storage, self.support_plan.storage)
        self.assertEqual(plan.application_architecture, 'aarch64')
        self.assertIsNone(saved.get('namespace_uid'))
        self.assertFalse(saved['live_identity_observed'])
        self.assertTrue(all(value is False for value in saved['qualification'].values()))
        self.assertFalse(plan.initializer_authorized)
        self.assertFalse(plan.application_start_allowed)
        self.assertFalse(plan.runtime_authorized)
        self.assertFalse(plan.released)
        self.assertFalse(hasattr(plan, 'runtime_manifest'))
        self.assertFalse(hasattr(type(plan), 'from_metadata'))
        self.assertEqual(runtime.validate_guarded_hotel_runtime_plan(plan), plan)
        self.assertEqual(runtime.validate_guarded_hotel_runtime_snapshot(saved, plan), saved)

    def test_stable_single_attempt_names_and_existing_go_environment_only(self):
        plan = self.plan()
        self.assertEqual(plan.initializer_pod_name, 'hotel-initialize-' + ATTEMPT.replace('-', ''))
        self.assertEqual(plan.verifier_pod_name, 'hotel-verify-' + ATTEMPT.replace('-', ''))
        self.assertLessEqual(len(plan.initializer_pod_name), 63)
        self.assertNotEqual(plan.initializer_pod_name, plan.verifier_pod_name)
        environment = plan.initialization_environment()
        self.assertEqual(environment, {
            'HOTEL_CANDIDATE_RUN_ID': RUN,
            'HOTEL_CANDIDATE_CONTRACT_SHA256': self.artifacts.dataset_attestation['dataset_fingerprint'],
        })
        self.assertEqual(plan.snapshot()['policy']['go_identity_scope'],
                         'run-and-dataset-only-not-cluster-storage-pod-or-attempt')
        environment['HOTEL_CANDIDATE_RUN_ID'] = 'changed'
        self.assertEqual(plan.initialization_environment()['HOTEL_CANDIDATE_RUN_ID'], RUN)

    def test_restart_retry_replay_and_replacement_are_cleanup_only(self):
        policy = self.plan().snapshot()['policy']
        self.assertEqual(policy['initialization_attempts'], 1)
        self.assertEqual(policy['initializer_restart_policy'], 'Never')
        for flag in ('initialization_replay_allowed', 'automatic_retry_allowed',
                     'reset_allowed', 'repair_allowed', 'upsert_allowed', 'live_observation_is_authority'):
            self.assertIs(policy[flag], False)
        self.assertEqual(policy['interrupted_or_ambiguous_state'], 'cleanup-only-no-initializer-replay')
        self.assertEqual(policy['replaced_cluster_namespace_storage_or_pod'], 'cleanup-only-new-run-required')
        self.assertIn('forbidden', policy['application_start'])

    def test_all_run_scope_and_attempt_changes_recompute_identity(self):
        original = self.plan()
        for options in ({'run_id': 'abcdef123456'}, {'provider': 'aws'},
                        {'topology_fingerprint': 'sha256:' + '3' * 64},
                        {'application_architecture': 'x86_64'}, {'load_generator_ip': '10.240.2.11'},
                        {'filesystem_uuid': uid(99)}):
            support = self.support(**options)
            with self.subTest(options=options):
                changed = self.plan(support_plan=support, support_stage=build_candidate_support_stage(support))
                self.assertNotEqual(original.fingerprint, changed.fingerprint)
                with self.assertRaises(runtime.GuardedHotelRuntimeError):
                    runtime.validate_guarded_hotel_runtime_snapshot(original.snapshot(), changed)
        changed = self.plan(initialization_attempt_id=uid(99))
        self.assertNotEqual(original.fingerprint, changed.fingerprint)
        self.assertNotEqual(original.initializer_pod_name, changed.initializer_pod_name)

    def test_crossed_stages_wrong_baseline_and_serialized_inputs_fail(self):
        other = self.support(run_id='abcdef123456')
        for options in ({'support_plan': other}, {'support_stage': build_candidate_support_stage(other)},
                        {'artifacts': self.artifacts.metadata()}, {'artifacts': self.data['baseline']},
                        {'support_plan': self.support_plan.snapshot()}, {'support_stage': self.support_stage.metadata()},
                        {'artifacts': object()}, {'support_stage': None}):
            with self.subTest(options=options), self.assertRaises(runtime.GuardedHotelRuntimeError):
                self.plan(**options)
        original = baseline_fixtures.CandidateArtifactAdoptionTests()
        self.addCleanup(original.doCleanups)
        data = original.fixture()
        # Exact same JSON has different retained original document bytes. A
        # genuine fresh baseline proof must still agree with the guarded proof.
        write(data['publication_path'], encoded(data['publication']) + b'\n')
        other_baseline = original.adopt(data)
        self.assertNotEqual(self.data['baseline'].fingerprint, other_baseline.fingerprint)
        support = create_candidate_runtime_plan(
            other_baseline, run_id=RUN, provider='azure', topology_fingerprint=TOPOLOGY,
            application_architecture='aarch64', load_generator_ip='10.240.2.10', filesystem_uuid=FS_UUID,
        )
        with self.assertRaisesRegex(runtime.GuardedHotelRuntimeError, 'same freshly adopted original baseline'):
            self.plan(support_plan=support, support_stage=build_candidate_support_stage(support))

    def test_unadopted_objects_and_inconsistent_dataclass_replacement_fail(self):
        plan = self.plan()
        bogus = object.__new__(runtime.GuardedHotelArtifactBinding)
        with self.assertRaises(runtime.GuardedHotelRuntimeError):
            self.plan(artifacts=bogus)
        for fields in ({'fingerprint': 'sha256:' + 'f' * 64}, {'snapshot_json': '{}'},
                       {'initialization_attempt_id': uid(98)}, {'support_stage': object()},
                       {'support_plan': self.support(run_id='abcdef123456')}, {'artifacts': {}}):
            with self.subTest(fields=fields), self.assertRaises(runtime.GuardedHotelRuntimeError):
                replace(plan, **fields)
        with self.assertRaises(FrozenInstanceError):
            plan.fingerprint = 'changed'
        self.assertEqual(replace(plan), plan)

    def test_invalid_attempt_id_and_saved_snapshot_cannot_promote(self):
        for attempt in ('', None, True, '00000000-0000-0000-0000-000000000000',
                        ATTEMPT.upper(), ATTEMPT + '\n', 'a' * 1024):
            with self.subTest(attempt=attempt), self.assertRaises(runtime.GuardedHotelRuntimeError):
                self.plan(initialization_attempt_id=attempt)
        plan = self.plan()
        for change in ('unknown', 'released', 'qualification', 'dataset_fingerprint', 'initialization_attempt_id'):
            saved = plan.snapshot()
            saved[change] = True
            with self.subTest(change=change), self.assertRaises(runtime.GuardedHotelRuntimeError):
                runtime.validate_guarded_hotel_runtime_snapshot(saved, plan)
        saved = plan.snapshot()
        saved['storage']['database_names'].clear()
        self.assertEqual(len(plan.snapshot()['storage']['database_names']), 8)
        for value in (plan.snapshot(), None, object()):
            with self.assertRaises(runtime.GuardedHotelRuntimeError):
                runtime.validate_guarded_hotel_runtime_plan(value)

    def test_observations_require_all_real_identifier_inputs_but_are_not_proof(self):
        plan = self.plan()
        observed = self.observation(plan)
        metadata = observed.metadata()
        self.assertEqual(metadata['candidate_schema'], runtime.OBSERVATION_SCHEMA)
        self.assertEqual(metadata['plan_fingerprint'], plan.fingerprint)
        self.assertEqual(metadata['namespace_uid'], uid(2))
        self.assertEqual(metadata['cluster_identity_kind'], 'kube-system-namespace-uid-and-server-ca-sha256')
        self.assertEqual(metadata['kube_system_namespace_uid'], uid(1))
        self.assertEqual(metadata['database_filesystem_uuid'], FS_UUID)
        self.assertEqual(metadata['storage_contract_sha256'], plan.storage.sha256)
        self.assertEqual(metadata['databases'], [item.metadata() for item in self.databases()])
        self.assertEqual(metadata['initializer_pod_name'], plan.initializer_pod_name)
        for flag in ('live_api_executed', 'live_identity_verified', 'freshness_verified',
                     'initializer_authorized', 'application_start_allowed', 'released'):
            self.assertIs(metadata[flag], False)
        self.assertTrue(all(value is False for value in metadata['qualification'].values()))
        self.assertEqual(runtime.validate_guarded_hotel_cluster_observation(plan, observed), observed)
        self.assertEqual(runtime.validate_saved_guarded_hotel_cluster_observation(plan, metadata), metadata)
        self.assertFalse(hasattr(type(observed), 'from_metadata'))
        with self.assertRaises(FrozenInstanceError):
            observed.namespace_uid = uid(99)
        metadata['databases'].clear()
        self.assertEqual(len(observed.metadata()['databases']), 8)

    def test_each_cluster_storage_and_object_change_has_a_different_identity(self):
        original = self.observation()
        for options in ({'server_ca_sha256': 'sha256:' + '4' * 64}, {'kube_system_namespace_uid': uid(98)},
                        {'namespace_uid': uid(98)}, {'database_node_name': 'database2'},
                        {'database_node_uid': uid(98)}, {'initializer_pod_uid': uid(98)}):
            with self.subTest(options=options):
                changed = self.observation(**options)
                self.assertNotEqual(original.fingerprint, changed.fingerprint)
        for field in ('pvc_uid', 'pv_uid', 'pod_uid', 'pod_name'):
            rows = list(self.databases())
            rows[0] = replace(rows[0], **{field: rows[0].component + '-new-pod' if field == 'pod_name' else uid(98)})
            changed = self.observation(databases=tuple(rows))
            self.assertNotEqual(original.fingerprint, changed.fingerprint)
        changed_plan = self.plan(initialization_attempt_id=uid(99))
        with self.assertRaises(runtime.GuardedHotelRuntimeError):
            runtime.validate_guarded_hotel_cluster_observation(changed_plan, original)

    def test_crossed_unknown_missing_or_reused_kubernetes_objects_rejected(self):
        rows = self.databases()
        for options in ({'database_filesystem_uuid': uid(99)}, {'initializer_pod_name': 'wrong'},
                        {'kube_system_namespace_uid': uid(2)}, {'namespace_uid': uid(3)},
                        {'database_node_uid': rows[0].pvc_uid}, {'initializer_pod_uid': rows[0].pod_uid},
                        {'databases': rows[:-1]}, {'databases': tuple(reversed(rows))},
                        {'databases': [*rows]}, {'databases': tuple(item.metadata() for item in rows)},
                        {'server_ca_sha256': 'sha256:' + '0' * 64}, {'server_ca_sha256': 'bad'},
                        {'database_node_name': ''}, {'database_node_name': 'Database'},
                        {'database_node_name': 'db\n'}):
            with self.subTest(options=options), self.assertRaises(runtime.GuardedHotelRuntimeError):
                self.observation(**options)
        for field in ('component', 'pvc_name', 'pv_name', 'pod_name', 'pvc_uid', 'pv_uid', 'pod_uid'):
            with self.subTest(field=field), self.assertRaises(runtime.GuardedHotelRuntimeError):
                replace(rows[0], **{field: 'wrong'})
        for field in ('kube_system_namespace_uid', 'namespace_uid', 'database_node_uid',
                      'database_filesystem_uuid', 'initializer_pod_uid'):
            with self.subTest(field=field), self.assertRaises(runtime.GuardedHotelRuntimeError):
                self.observation(**{field: '00000000-0000-0000-0000-000000000000'})

    def test_observation_replacements_and_serialized_replay_do_not_gain_authority(self):
        observed = self.observation()
        for fields in ({'fingerprint': 'sha256:' + 'f' * 64}, {'metadata_json': '{}'},
                       {'namespace_uid': uid(98)}, {'databases': ()}, {'plan': self.plan(initialization_attempt_id=uid(99))}):
            with self.subTest(fields=fields), self.assertRaises(runtime.GuardedHotelRuntimeError):
                replace(observed, **fields)
        for value in (observed.metadata(), None, object()):
            with self.assertRaises(runtime.GuardedHotelRuntimeError):
                runtime.validate_guarded_hotel_cluster_observation(observed.plan, value)
        self.assertEqual(replace(observed), observed)

    def test_saved_observation_checks_schema_without_restoring_authority(self):
        observed = self.observation()
        for field, value in (
            ('schema_version', True), ('candidate_schema', 'other'), ('released', True),
            ('initializer_authorized', True), ('live_identity_verified', True),
            ('freshness_verified', True), ('application_start_allowed', True),
            ('run_id', 'abcdef123456'), ('plan_fingerprint', 'sha256:' + 'f' * 64),
            ('initialization_attempt_id', uid(99)), ('unknown', False),
        ):
            metadata = observed.metadata()
            metadata[field] = value
            with self.subTest(field=field), self.assertRaises(runtime.GuardedHotelRuntimeError):
                runtime.validate_saved_guarded_hotel_cluster_observation(observed.plan, metadata)
        metadata = observed.metadata()
        del metadata['namespace_uid']
        with self.assertRaises(runtime.GuardedHotelRuntimeError):
            runtime.validate_saved_guarded_hotel_cluster_observation(observed.plan, metadata)
        metadata = observed.metadata()
        metadata['databases'][0]['extra'] = False
        with self.assertRaises(runtime.GuardedHotelRuntimeError):
            runtime.validate_saved_guarded_hotel_cluster_observation(observed.plan, metadata)

    def test_offline_factories_never_execute_network_or_subprocess(self):
        with mock.patch('socket.socket', side_effect=AssertionError('network')), \
                mock.patch('subprocess.run', side_effect=AssertionError('execution')), \
                mock.patch('builtins.open', side_effect=AssertionError('filesystem')):
            # Source asset verification uses pathlib read_bytes/read_text,
            # explicitly allowed here for deterministic audited offline input.
            plan = self.plan()
            observed = self.observation(plan)
            self.assertFalse(observed.metadata()['live_api_executed'])


if __name__ == '__main__':
    unittest.main()
