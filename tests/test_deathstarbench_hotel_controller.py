"""Controller sequencing with real leases/latches and simulated external I/O."""

import copy
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from app import deathstarbench_hotel_controller as controller
from app import deathstarbench_hotel_initialization_journal as journal
from app.deathstarbench_hotel_initialization_stage import build_guarded_hotel_initialization_stage
from app.run_lease import acquire_run_lease
from tests import test_deathstarbench_hotel_runtime as fixtures


class HotelControllerTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.fixture = fixtures.GuardedHotelRuntimeTests()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.plan = self.fixture.plan()
        self.stage = build_guarded_hotel_initialization_stage(self.plan)
        self.observation = self.fixture.observation()
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory())) / '.candidate-support'
        self.directory = self.root / self.plan.run_id
        self.directory.mkdir(mode=0o700, parents=True)
        self.lease = self.stack.enter_context(acquire_run_lease(self.root, self.plan.run_id, 'controller-test'))
        self.identity = {'role_nodes': {'control': 'control', 'database': 'database.internal',
                                      'application': 'app', 'cache': 'cache'},
                         'server_ca_sha256': '2' * 64,
                         'database_volume': {'filesystem_uuid': fixtures.FS_UUID}}
        self.cluster = SimpleNamespace(host=lambda role: SimpleNamespace(host_key=role, jump_host_key=None))
        self.support = {'state': 'support_ready_applications_paused',
                        'phase_receipts': {}, 'support_attestation': {'inventory': {'items': []}}}
        self.job = {'id': self.plan.run_id, 'plan': {'provider': 'azure'}, 'results': [],
                    'resources': {controller.SUPPORT_STAGE_JOURNAL_KEY: copy.deepcopy(self.support)},
                    'candidate_support_pin': {'run_id': self.plan.run_id, 'provider_pin': {'provider': 'azure'}}}
        self.audit_identity = {'run': self.plan.run_id, 'owned': True}
        self.access = SimpleNamespace(fingerprint='sha256:' + 'e' * 64)
        self.calls, self.persisted, self.checkpoints = [], [], []
        self.released = []
        self.fail_release = None
        self.on_persist = None
        self.on_observe = None
        self.on_audit = None
        self.global_complete = False
        self.reads = 0
        self.pending_forever = False
        self.patches = {
            '_context': lambda *args: (self.cluster, self.plan.support_plan, self.plan.support_stage, copy.deepcopy(self.identity)),
            'validate_candidate_support_journal': lambda value, *args: copy.deepcopy(value),
            'qualify_guarded_hotel_image_access': lambda artifacts: self.access,
            'require_guarded_hotel_image_access': lambda value, artifacts: value,
            'candidate_database_storage_observation_command': lambda storage: 'storage',
            'parse_candidate_database_storage_observation': lambda output, storage: {'read_only': True},
        }
        for name, value in self.patches.items():
            self.stack.enter_context(mock.patch.object(controller, name, side_effect=value))
        self.stack.enter_context(mock.patch.object(journal, 'require_guarded_hotel_image_access',
                                                  side_effect=lambda value, artifacts: value))
        self.stack.enter_context(mock.patch.object(controller.cluster_runtime, '_database_volume_attestation_command',
                                                  return_value='volume'))
        self.stack.enter_context(mock.patch.object(controller.cluster_runtime, '_parse_plan_database_volume_attestation',
                                                  return_value=copy.deepcopy(self.identity['database_volume'])))
        self.stack.enter_context(mock.patch.object(controller.cluster_runtime, '_same_database_volume_identity',
                                                  side_effect=lambda a, b: a == b))
        self.live = SimpleNamespace(
            hotel_initialization_observe_command=lambda *args, include_initialization=True: 'observe' if include_initialization else 'before',
            parse_hotel_initialization_observation=lambda output, *args, **kwargs: json.loads(output),
            hotel_initialization_create_command=lambda *args: 'create',
            hotel_initialization_release_command=lambda plan, stage, roles, observation, mode: 'release:' + mode,
            hotel_initialization_logs_command=lambda plan, stage, mode, uid: 'logs:' + mode,
        )
        self.stack.enter_context(mock.patch.object(controller, 'live', self.live))

    def observe(self, include=True):
        self.reads += 1
        pods = {}
        for mode, uid in [('initialize-empty', self.observation.initializer_pod_uid), ('verify', fixtures.uid(99))]:
            done = mode in self.released and not self.pending_forever
            pods[mode] = {'uid': uid, 'spec': self.stage.pod(mode)['spec'],
                          'metadata': {'uid': uid}, 'phase': 'Succeeded' if done else 'Pending',
                          'exit_code': 0 if done else None, 'restart_count': 0,
                          'container_id': ('containerd://' + mode) if done else None,
                          'started_at': '2026-10-20T15:00:00Z' if done else None,
                          'image_id': self.plan.application_image if done else None}
            if mode in self.released:
                pods[mode]['spec'].pop('schedulingGates')
        result = {
            'cluster_observation': self.observation.metadata() if include else None,
            'verifier_pod_uid': fixtures.uid(99) if include else None,
            'initialization_pods': pods if include else {},
            'continuity': {'server_ca_sha256': 'sha256:' + '2' * 64,
                           'kube_system_namespace_uid': fixtures.uid(1),
                           'objects': [], 'support_containers': [], 'endpoint_graph': [],
                           'initialization_pods': {k: v['uid'] for k, v in pods.items()} if include else {},
                           'initialization_objects': {}},
            'support_attestation': {'inventory': {'items': []}},
        }
        if self.on_observe:
            self.on_observe(result)
        return result

    def execute(self, job, command, **kwargs):
        self.calls.append(command)
        if command in ('observe', 'before'):
            return json.dumps(self.observe(command == 'observe'))
        if command.startswith('release:'):
            mode = command.split(':', 1)[1]
            # Releasing without a durable latch is the production regression
            # this simulated boundary detects.
            self.assertTrue((self.directory / journal.LATCHES[mode]).is_file())
            self.assertNotIn(mode, self.released)
            self.released.append(mode)
            if self.fail_release == mode:
                raise OSError('lost gate-release response')
        if command.startswith('logs:'):
            mode = command.split(':', 1)[1]
            return json.dumps({'candidate_only': True, 'released': False,
                              'application_start_allowed': False, 'runtime_qualified': False,
                              'measurement_qualified': False, 'run_id': self.plan.run_id,
                              'contract_sha256': self.plan.snapshot()['dataset_fingerprint'],
                              'mode': mode, 'database_verification_completed': True})
        return ''

    def persist(self, job):
        if self.on_persist:
            self.on_persist(job)
        self.persisted.append(copy.deepcopy(job[controller.STATE_KEY]))

    def audit(self, job, lease):
        if self.on_audit:
            self.on_audit()
        return {'schema_version': 1, 'candidate_only': True, 'read_only': True,
                'run_id': self.plan.run_id, 'provider': self.plan.provider,
                'cloud_identity': copy.deepcopy(self.audit_identity)}

    def run_controller(self, **kwargs):
        return controller.run_guarded_hotel_initialization(
            self.job, self.fixture.artifacts, baseline_artifacts=self.fixture.data['baseline'],
            initialization_attempt_id=fixtures.ATTEMPT, lease=self.lease,
            execute=self.execute, persist=self.persist, fresh_audit=self.audit,
            checkpoint=kwargs.pop('checkpoint', self.checkpoints.append), **kwargs)

    def test_success_requires_two_distinct_latched_pods_and_global_verification(self):
        result = self.run_controller()
        self.assertEqual(self.released, ['initialize-empty', 'verify'])
        self.assertEqual(result['state'], 'initialized-and-globally-verified-applications-paused')
        self.assertIs(result['database_execution_verified'], True)
        self.assertIs(result['global_readonly_verification_completed'], True)
        self.assertIs(result['application_start_allowed'], False)
        self.assertIs(result['public_release'], False)
        self.assertIs(result['measurement_qualified'], False)
        self.assertEqual(result['journal']['event_count'], 6)
        self.assertEqual(self.calls.count('create'), 1)
        self.assertLess(self.calls.index('logs:initialize-empty'), self.calls.index('release:verify'))
        self.assertEqual(self.job['results'], [])

    def test_lost_release_response_never_replays_or_dispatches_verifier(self):
        self.fail_release = 'initialize-empty'
        with self.assertRaisesRegex(OSError, 'lost gate'):
            self.run_controller()
        self.assertEqual(self.released, ['initialize-empty'])
        self.assertEqual(self.job[controller.STATE_KEY]['state'], 'failed-cleanup-only')
        before = list(self.calls)
        with self.assertRaisesRegex(ValueError, 'cleanup'):
            self.run_controller()
        self.assertEqual(self.calls, before)

    def test_ssh_transport_must_not_retry_one_shot_remote_commands(self):
        original = self.execute
        def execute(job, command, **kwargs):
            self.assertEqual(kwargs.get('transport_attempts'), 1)
            self.assertEqual(kwargs.get('transport_retry_delay_seconds'), 0)
            return original(job, command, **kwargs)
        self.execute = execute
        self.run_controller()
        self.assertEqual(self.released, ['initialize-empty', 'verify'])

    def test_existing_latch_without_state_is_cleanup_only_before_external_calls(self):
        (self.directory / journal.LATCHES['initialize-empty']).write_bytes(b'{')
        with self.assertRaisesRegex(ValueError, 'cleanup'):
            self.run_controller()
        self.assertEqual(self.calls, [])

    def test_failed_intent_persistence_never_releases_gate(self):
        def fail(job):
            if job[controller.STATE_KEY]['state'] == 'initialize-intent-recorded':
                raise OSError('disk full')
        self.on_persist = fail
        with self.assertRaisesRegex(OSError, 'disk full'):
            self.run_controller()
        self.assertEqual(self.released, [])
        self.assertTrue((self.directory / journal.LATCHES['initialize-empty']).exists())

    def test_interruption_after_durable_intent_is_cleanup_only(self):
        def checkpoint(name):
            if name == 'initializer-intent':
                raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.run_controller(checkpoint=checkpoint)
        self.assertTrue((self.directory / journal.LATCHES['initialize-empty']).exists())
        self.assertEqual(self.released, [])
        self.assertEqual(self.job[controller.STATE_KEY]['state'], 'failed-cleanup-only')

    def test_live_identity_replacement_before_release_is_refused(self):
        def change(result):
            if self.calls.count('observe') >= 2:
                result['continuity']['kube_system_namespace_uid'] = fixtures.uid(97)
        self.on_observe = change
        with self.assertRaisesRegex(ValueError, 'identity|continuity'):
            self.run_controller()
        self.assertEqual(self.released, [])

    def test_checkpoint_cannot_delete_the_durable_dispatch_latch(self):
        def mutate(name):
            if name == 'initializer-intent':
                (self.directory / journal.LATCHES['initialize-empty']).unlink()
        with self.assertRaises(ValueError):
            self.run_controller(checkpoint=mutate)
        self.assertEqual(self.released, [])

    def test_checkpoint_cannot_insert_a_competing_verifier_latch(self):
        def mutate(name):
            if name == 'initializer-intent':
                source = self.directory / journal.LATCHES['initialize-empty']
                target = self.directory / journal.LATCHES['verify']
                value = json.loads(source.read_text())
                value['intent']['mode'] = 'verify'
                value['intent']['pod_uid'] = fixtures.uid(99)
                target.write_bytes(journal._canonical(value))
                target.chmod(0o600)
        with self.assertRaises(ValueError):
            self.run_controller(checkpoint=mutate)
        self.assertEqual(self.released, [])

    def test_same_pod_uid_cannot_replace_its_container_after_completion(self):
        completed = []
        def mutate(result):
            if 'initialize-empty' in self.released:
                completed.append(True)
                if len(completed) > 1:
                    result['initialization_pods']['initialize-empty']['container_id'] = 'containerd://replacement'
        self.on_observe = mutate
        with self.assertRaisesRegex(ValueError, 'container|execution'):
            self.run_controller()
        self.assertEqual(self.released, ['initialize-empty'])
        self.assertNotIn('logs:initialize-empty', self.calls)

    def test_cloud_owner_drift_blocks_create(self):
        audits = []
        def change():
            audits.append(True)
            if len(audits) > 1:
                self.audit_identity['owned'] = 'changed'
        self.on_audit = change
        with self.assertRaisesRegex(ValueError, 'cloud'):
            self.run_controller()
        self.assertNotIn('create', self.calls)

    def test_callback_cannot_change_plan_or_evidence_to_gain_execution(self):
        def mutate(job):
            job['plan']['provider'] = 'gcp'
        self.on_persist = mutate
        with self.assertRaisesRegex(ValueError, 'binding|plan'):
            self.run_controller()
        self.assertEqual(self.calls, [])

    def test_timeout_is_bounded_and_cannot_dispatch_verifier(self):
        self.pending_forever = True
        clock = iter(range(0, 10000, 100))
        with mock.patch.object(controller.time, 'monotonic', side_effect=lambda: next(clock)), \
                mock.patch.object(controller.time, 'sleep'), self.assertRaises(TimeoutError):
            self.run_controller(timeout_seconds=1)
        self.assertEqual(self.released, ['initialize-empty'])
        self.assertIs(self.job[controller.STATE_KEY]['global_readonly_verification_completed'], False)

    def test_success_observed_after_deadline_is_not_accepted(self):
        clock = iter([0, 0, 2] + list(range(3, 1000)))
        with mock.patch.object(controller.time, 'monotonic', side_effect=lambda: next(clock)), \
                self.assertRaises(TimeoutError):
            self.run_controller(timeout_seconds=1)
        self.assertEqual(self.released, ['initialize-empty'])
        self.assertNotIn('logs:initialize-empty', self.calls)

    def test_boolean_and_unbounded_timeouts_fail_before_external_work(self):
        for value in [True, 0, -1, 3601, '480']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.run_controller(timeout_seconds=value)
        self.assertEqual(self.calls, [])

    def use_live_parser(self):
        from app import deathstarbench_hotel_live as actual
        from tests.test_deathstarbench_hotel_live import HotelLiveTests, ROLE_NODES
        case = HotelLiveTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.assertEqual(case.plan.fingerprint, self.plan.fingerprint)
        self.identity['role_nodes'] = ROLE_NODES
        baseline = actual.parse_hotel_initialization_observation(
            json.dumps(case.document(False)), self.plan, self.stage, ROLE_NODES,
            include_initialization=False)
        self.support['support_attestation'] = baseline['support_attestation']
        self.job['resources'][controller.SUPPORT_STAGE_JOURNAL_KEY] = copy.deepcopy(self.support)
        self.live.parse_hotel_initialization_observation = actual.parse_hotel_initialization_observation

        def document(include=True):
            value = case.document(include)
            for item in value['inventory']['items']:
                if item['kind'] != 'Pod' or item['metadata']['name'] not in (
                        self.plan.initializer_pod_name, self.plan.verifier_pod_name):
                    continue
                mode = item['spec']['containers'][0]['args'][0]
                if mode not in self.released:
                    continue
                item['spec'].pop('schedulingGates')
                item['spec']['nodeName'] = ROLE_NODES['application']
                item['status'] = {
                    'phase': 'Succeeded', 'containerStatuses': [{
                        'name': 'hotel-initialize', 'image': self.plan.application_image,
                        'imageID': 'docker-pullable://' + self.plan.application_image,
                        'containerID': 'containerd://' + ('a' if mode == 'initialize-empty' else 'b') * 64,
                        'restartCount': 0, 'ready': False, 'started': False, 'lastState': {},
                        'state': {'terminated': {'exitCode': 0, 'reason': 'Completed',
                                  'startedAt': '2026-10-20T15:00:00Z',
                                  'finishedAt': '2026-10-20T15:00:01Z'}},
                    }],
                }
            return value
        self.observe = document
        return case

    def test_real_inventory_parser_and_durable_controller_complete_together(self):
        self.use_live_parser()
        result = self.run_controller()
        self.assertIs(result['database_execution_verified'], True)
        self.assertEqual(result['journal']['event_count'], 6)
        self.assertEqual(set(result['container_executions']), {'initialize-empty', 'verify'})
        self.assertEqual(len(result['last_observation']['cluster_observation']['databases']), 8)

    def test_real_endpoint_replacement_refuses_controller_before_creation(self):
        case = self.use_live_parser()
        from tests.test_deathstarbench_candidate_k3s import objects, identity
        objects(case.inventory, 'EndpointSlice')[0]['endpoints'][0]['targetRef']['uid'] = identity('Pod', 'replacement')
        with self.assertRaisesRegex(ValueError, 'target'):
            self.run_controller()
        self.assertNotIn('create', self.calls)
        self.assertEqual(self.released, [])


if __name__ == '__main__':
    unittest.main()
