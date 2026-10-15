"""Synthetic SSH orchestration with real private leases and one-shot journals.

The separate live Kubernetes adapter tests validate complete API inventories and
admission defaults. This module stubs that adapter and the existing support
facade, but executes real artifact/plan derivation, cloud-audit and guest-storage
parsers, lease checks, exclusive dispatch latches, and strict Go output checks.
No test here claims a live cloud, database, image pull, or Kubernetes execution.
"""

from contextlib import ExitStack
import copy
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from app import deathstarbench_hotel_initialization_journal as journal
from app import deathstarbench_hotel_live_controller as controller
from app.deathstarbench_candidate_storage_observation import OBSERVATION_MARKER
from app.run_lease import acquire_run_lease, RunLeaseError
from tests import test_deathstarbench_candidate_storage_observation as storage_fixtures
from tests import test_deathstarbench_hotel_runtime as fixtures


class GuardedHotelLiveControllerTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.fixture = fixtures.GuardedHotelRuntimeTests()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.plan = self.fixture.plan()
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory())) / '.candidate-support'
        root.mkdir(mode=0o700)
        self.directory = root / self.plan.run_id
        self.directory.mkdir(mode=0o700)
        self.lease = acquire_run_lease(root, self.plan.run_id, 'hotel-controller-test')
        self.addCleanup(self.lease.release)
        self.access = SimpleNamespace(fingerprint='sha256:' + 'e' * 64)
        self.cloud_identity = {'provider_create_uid': fixtures.uid(1000)}
        self.volume = {
            'lun': 0, 'device_link': '/dev/disk/azure/data/by-lun/0',
            'filesystem_uuid': fixtures.FS_UUID,
            'mount_point': '/var/lib/deathstarbench/database', 'filesystem': 'xfs',
        }
        self.roles = {role: role + '.internal' for role in ('control', 'database', 'cache', 'application')}
        self.context = {
            'role_nodes': self.roles, 'server_ca_sha256': '2' * 64,
            'database_volume': copy.deepcopy(self.volume),
        }
        self.cluster = SimpleNamespace(
            provider='azure',
            host=lambda role: SimpleNamespace(host_key=role, jump_host_key=None),
        )
        self.inventory = {'apiVersion': 'v1', 'kind': 'List', 'items': [{
            'kind': 'Namespace', 'metadata': {
                'name': self.plan.namespace, 'uid': fixtures.uid(2),
            },
        }, {
            'kind': 'PersistentVolume', 'metadata': {
                'name': self.fixture.databases()[0].pv_name, 'uid': self.fixture.databases()[0].pv_uid,
            },
        }]}
        self.ready = {
            'state': controller.support.READY_STATE,
            'support_attestation': {'inventory': copy.deepcopy(self.inventory), 'support_pods': []},
        }
        self.job = {
            'id': self.plan.run_id, '_persist_state': False, 'results': [],
            'plan': {'provider': 'azure'},
            'candidate_support_pin': {
                'run_id': self.plan.run_id, 'provider_pin': {'provider': 'azure'},
            },
            'hotel_initialization_pin': {'guarded_artifact_fingerprint': self.fixture.artifacts.fingerprint},
            'resources': {controller.support.SUPPORT_STAGE_JOURNAL_KEY: copy.deepcopy(self.ready)},
        }
        self.saved = []
        self.calls = []
        self.dispatches = []
        self.checkpoints = []
        self.pods = {}
        self.audit_count = 0
        self.volume_count = 0
        self.storage_count = 0
        self.before_persist = None
        self.before_audit = None
        self.before_volume = None
        self.before_storage = None
        self.before_observation = None
        self.lose_response_at = None
        self.finished_exit_code = 0
        self.output_changes = {}
        self.created_states = {}

        for module in (controller, journal):
            self.stack.enter_context(mock.patch.object(
                module, 'require_guarded_hotel_image_access', return_value=self.access))
        self.stack.enter_context(mock.patch.object(
            controller.support, '_context', side_effect=lambda *_args: (
                self.cluster, self.fixture.support_plan, self.fixture.support_stage, self.context)))
        self.stack.enter_context(mock.patch.object(
            controller.support, 'validate_candidate_support_journal', side_effect=lambda value, *_args: copy.deepcopy(value)))
        # The import is deliberately replaced even when the real adapter is
        # available: these are controller-ordering tests, not API parser tests.
        self.adapter = SimpleNamespace(
            guarded_hotel_observation_command=self.command,
            parse_guarded_hotel_live_observation=self.parse_observation,
        )
        self.stack.enter_context(mock.patch.dict(sys.modules, {
            'app.deathstarbench_hotel_live_k3s': self.adapter,
        }))

    def command(self, plan, stage, roles, ca, *, operation='observe', mode=None, expected_identity=None):
        self.assertEqual(plan.fingerprint, self.plan.fingerprint)
        self.assertEqual(stage.plan_fingerprint, self.plan.fingerprint)
        self.assertEqual(roles, self.roles)
        self.assertEqual(ca, '2' * 64)
        return 'hotel-adapter:' + json.dumps({'operation': operation, 'mode': mode})

    def parse_observation(self, output, plan, stage, roles, ca, *, expected_identity=None):
        return json.loads(output)

    def output(self, mode):
        return json.dumps({
            'candidate_only': True, 'released': False, 'application_start_allowed': False,
            'runtime_qualified': False, 'measurement_qualified': False,
            'run_id': self.plan.run_id,
            'contract_sha256': self.plan.snapshot()['dataset_fingerprint'],
            'mode': mode, 'database_verification_completed': True, **self.output_changes,
        })

    def observation(self):
        return {
            'identity': {'cluster_uid': fixtures.uid(1), 'support_containers': []},
            'namespace_uid': fixtures.uid(2), 'kube_system_namespace_uid': fixtures.uid(1),
            'nodes': [{'role': 'database', 'name': 'database.internal', 'uid': fixtures.uid(3)}],
            'databases': [row.metadata() for row in self.fixture.databases()],
            'pods': copy.deepcopy(self.pods),
            'support_inventory': copy.deepcopy(self.inventory),
            'logs': {mode: row['logs'] for mode, row in self.pods.items() if 'logs' in row},
        }

    def assert_dispatch_is_durable(self, mode):
        latch_path = self.directory / journal.LATCHES[mode]
        self.assertTrue(latch_path.is_file())
        self.assertEqual(latch_path.stat().st_mode & 0o777, 0o600)
        latch = json.loads(latch_path.read_bytes())
        self.assertEqual(latch['identity']['plan_fingerprint'], self.plan.fingerprint)
        self.assertEqual(latch['intent']['mode'], mode)
        self.assertEqual(latch['intent']['pod_uid'], self.pods[mode]['uid'])
        self.assertEqual(latch['intent']['image'], self.plan.application_image)
        self.assertEqual(latch['intent']['image_access_fingerprint'], self.access.fingerprint)
        index = 2 if mode == 'initialize-empty' else 4
        event = json.loads((self.directory / (journal.PREFIX + str(index).zfill(2) + '.json')).read_bytes())
        self.assertEqual(event['payload'], latch['intent'])
        self.assertEqual(self.job[controller.JOB_KEY]['state'], mode + '-dispatch-intent')
        self.assertEqual(self.saved[-1][controller.JOB_KEY]['state'], mode + '-dispatch-intent')

    def execute(self, job, command, **kwargs):
        self.assertIs(job, self.job)
        self.calls.append((command, kwargs))
        if 'AZURE_DSB_DATABASE_VOLUME' in command:
            self.assertEqual(kwargs['host_key'], 'database')
            self.volume_count += 1
            uuid = self.volume['filesystem_uuid']
            if self.before_volume is not None:
                uuid = self.before_volume(self.volume_count, uuid)
            return (
                'AZURE_DSB_DATABASE_VOLUME lun=0 device_link=/dev/disk/azure/data/by-lun/0 '
                f'uuid={uuid} mount_point=/var/lib/deathstarbench/database filesystem=xfs\n'
            )
        if OBSERVATION_MARKER in command:
            self.assertEqual(kwargs['host_key'], 'database')
            self.storage_count += 1
            changes = self.before_storage(self.storage_count) if self.before_storage is not None else {}
            return storage_fixtures.marker(self.plan.storage, **changes) + '\n'
        self.assertEqual(kwargs['host_key'], 'control')
        self.assertTrue(command.startswith('hotel-adapter:'))
        action = json.loads(command.split(':', 1)[1])
        operation, mode = action['operation'], action['mode']
        if self.before_observation is not None:
            self.before_observation(operation, mode)
        if operation == 'create':
            self.assertFalse(self.pods)
            self.pods = {
                mode: {'uid': fixtures.uid(4 if mode == 'initialize-empty' else 99),
                       'state': self.created_states.get(mode, 'gated')}
                for mode in ('initialize-empty', 'verify')
            }
        elif operation == 'release':
            self.assert_dispatch_is_durable(mode)
            self.assertEqual(self.pods[mode]['state'], 'gated')
            self.dispatches.append(mode)
            self.pods[mode]['state'] = 'running'
            if self.lose_response_at == mode:
                raise TimeoutError('simulated gate-release response loss after the request')
        elif operation == 'wait':
            self.assertEqual(self.pods[mode]['state'], 'running')
            self.pods[mode].update(
                state='terminated', exit_code=self.finished_exit_code, logs=self.output(mode))
        else:
            self.assertEqual(operation, 'observe')
        return json.dumps(self.observation())

    def persist(self, job):
        if self.before_persist is not None:
            self.before_persist(job)
        self.saved.append(copy.deepcopy(job))

    def fresh_audit(self, job, lease):
        self.assertIs(job, self.job)
        self.assertIs(lease, self.lease)
        self.audit_count += 1
        identity = copy.deepcopy(self.cloud_identity)
        if self.before_audit is not None:
            identity = self.before_audit(self.audit_count, identity)
        return {
            'schema_version': 1, 'candidate_only': True, 'read_only': True,
            'run_id': self.plan.run_id, 'provider': 'azure', 'cloud_identity': identity,
        }

    def run_controller(self, **overrides):
        options = {
            'baseline_artifacts': self.fixture.data['baseline'], 'lease': self.lease,
            'execute': self.execute, 'persist': self.persist, 'image_access': self.access,
            'fresh_audit': self.fresh_audit, 'expected_cloud_identity': self.cloud_identity,
            'initialization_attempt_id': fixtures.ATTEMPT,
            'checkpoint': lambda name, _job: self.checkpoints.append(name),
        }
        options.update(overrides)
        return controller.prepare_guarded_hotel_initialization(self.job, self.fixture.artifacts, **options)

    def test_success_has_two_ordered_durable_dispatches_and_never_starts_apps(self):
        result = self.run_controller()
        self.assertEqual(self.dispatches, ['initialize-empty', 'verify'])
        self.assertEqual(self.checkpoints, [
            'initialize-response-returned', 'initialize-dispatched', 'initialize-verified',
            'verify-response-returned', 'verify-dispatched', 'verify-verified'])
        self.assertEqual(result['state'], controller.SUCCESS)
        self.assertTrue(result['initialization_executed'])
        self.assertTrue(result['database_execution_verified'])
        self.assertTrue(result['global_readonly_verification_completed'])
        self.assertTrue(result['all_application_replicas_zero'])
        self.assertTrue(result['applications_paused'])
        self.assertEqual(self.audit_count, 4)
        self.assertEqual(self.volume_count, 4)
        self.assertEqual(self.storage_count, 4)
        for flag in ('released', 'application_start_allowed', 'runtime_qualified', 'measurement_qualified',
                     'cloud_qualified', 'public_release', 'public_dispatch_enabled'):
            self.assertIs(result[flag], False)
        self.assertEqual(result['journal']['event_count'], 6)
        self.assertEqual(result['journal']['dispatch_latches'], ['initialize-empty', 'verify'])
        self.assertFalse(result['journal']['journal_is_execution_permission'])
        self.assertTrue(all(result['journal'][flag] is False for flag in journal.CLOSED))
        result['journal']['events'].clear()
        self.assertEqual(self.job[controller.JOB_KEY]['journal']['event_count'], 6)
        self.assertEqual(len(self.job[controller.JOB_KEY]['journal']['events']), 6)

    def test_lost_initializer_response_never_replays_or_dispatches_verifier(self):
        self.lose_response_at = 'initialize-empty'
        with self.assertRaises(TimeoutError):
            self.run_controller()
        self.assertEqual(self.dispatches, ['initialize-empty'])
        self.assertEqual(self.job[controller.JOB_KEY]['state'], 'failed-cleanup-only')
        self.assertEqual(self.job[controller.JOB_KEY]['journal']['state'], 'initialize-intent-recorded')
        self.assertIsNone(self.job[controller.JOB_KEY]['initialization_executed'])
        self.assertTrue((self.directory / journal.LATCHES['initialize-empty']).exists())
        self.assertFalse((self.directory / journal.LATCHES['verify']).exists())
        calls = len(self.calls)
        with self.assertRaisesRegex(controller.GuardedHotelControllerError, 'cleanup-only'):
            self.run_controller()
        # Durable journal evidence still refuses a new invocation even if an
        # untrusted caller discards the detached in-memory outcome dictionary.
        self.job.pop(controller.JOB_KEY)
        with self.assertRaisesRegex(controller.GuardedHotelControllerError, 'cleanup-only'):
            self.run_controller(initialization_attempt_id=fixtures.uid(777))
        self.assertEqual(len(self.calls), calls)
        self.assertEqual(self.dispatches, ['initialize-empty'])

    def test_latch_persistence_response_loss_prevents_any_remote_dispatch(self):
        original = journal.GuardedHotelInitializationJournal._exclusive

        def lose_intent_event(store, directory, name, value):
            if name == journal.PREFIX + '02.json':
                raise OSError('simulated lost intent-event write response after the exclusive latch')
            return original(store, directory, name, value)

        with mock.patch.object(journal.GuardedHotelInitializationJournal, '_exclusive', lose_intent_event):
            with self.assertRaises(OSError):
                self.run_controller()
        self.assertFalse(self.dispatches)
        self.assertTrue((self.directory / journal.LATCHES['initialize-empty']).exists())
        self.assertFalse((self.directory / journal.LATCHES['verify']).exists())
        calls = len(self.calls)
        self.job.pop(controller.JOB_KEY)
        with self.assertRaisesRegex(controller.GuardedHotelControllerError, 'cleanup-only'):
            self.run_controller()
        self.assertEqual(len(self.calls), calls)

    def test_lost_verifier_response_retains_both_latches_without_claiming_global_success(self):
        self.lose_response_at = 'verify'
        with self.assertRaises(TimeoutError):
            self.run_controller()
        self.assertEqual(self.dispatches, ['initialize-empty', 'verify'])
        value = self.job[controller.JOB_KEY]
        self.assertEqual(value['state'], 'failed-cleanup-only')
        self.assertTrue(value['initialization_executed'])
        self.assertFalse(value['global_readonly_verification_completed'])
        self.assertEqual(value['journal']['state'], 'verify-intent-recorded')
        self.assertTrue(all((self.directory / name).exists() for name in journal.LATCHES.values()))

    def test_fresh_cloud_drift_before_first_dispatch_leaves_both_pods_gated(self):
        self.before_audit = lambda count, value: {'provider_create_uid': fixtures.uid(1001)} if count == 2 else value
        with self.assertRaisesRegex(controller.GuardedHotelControllerError, 'cloud create identities changed'):
            self.run_controller()
        self.assertFalse(self.dispatches)
        self.assertEqual({pod['state'] for pod in self.pods.values()}, {'gated'})
        self.assertFalse(any((self.directory / name).exists() for name in journal.LATCHES.values()))

    def test_fresh_cloud_drift_between_initializer_and_verifier_blocks_verify(self):
        self.before_audit = lambda count, value: {'provider_create_uid': fixtures.uid(1001)} if count == 3 else value
        with self.assertRaises(controller.GuardedHotelControllerError):
            self.run_controller()
        self.assertEqual(self.dispatches, ['initialize-empty'])
        self.assertEqual(self.pods['verify']['state'], 'gated')
        self.assertFalse((self.directory / journal.LATCHES['verify']).exists())

    def test_fresh_database_filesystem_drift_blocks_dispatch(self):
        self.before_volume = lambda count, value: fixtures.uid(888) if count == 2 else value
        with self.assertRaisesRegex(controller.GuardedHotelControllerError, 'database volume changed'):
            self.run_controller()
        self.assertFalse(self.dispatches)
        self.assertFalse((self.directory / journal.LATCHES['initialize-empty']).exists())

    def test_fresh_storage_ownership_marker_drift_blocks_dispatch(self):
        self.before_storage = lambda count: {'run': 'fedcba654321'} if count == 2 else {}
        with self.assertRaisesRegex(ValueError, 'another run, workload or disk'):
            self.run_controller()
        self.assertFalse(self.dispatches)
        self.assertFalse((self.directory / journal.LATCHES['initialize-empty']).exists())

    def test_replaced_support_object_is_rejected_before_gated_pods_are_created(self):
        def replace_object(operation, _mode):
            if operation == 'observe' and not self.pods:
                self.inventory['items'][1]['metadata']['uid'] = fixtures.uid(888)
        self.before_observation = replace_object
        with self.assertRaisesRegex(controller.GuardedHotelControllerError, 'Support objects were replaced'):
            self.run_controller()
        self.assertFalse(self.pods)
        self.assertFalse(self.dispatches)

    def test_callback_mutation_after_durable_intent_is_caught_before_remote_patch(self):
        def mutate(job):
            if job[controller.JOB_KEY]['state'] == 'initialize-empty-dispatch-intent':
                job['plan']['provider'] = 'aws'
        self.before_persist = mutate
        with self.assertRaisesRegex(controller.GuardedHotelControllerError, 'immutable operator inputs changed'):
            self.run_controller()
        self.assertTrue((self.directory / journal.LATCHES['initialize-empty']).exists())
        self.assertFalse(self.dispatches)
        self.assertFalse((self.directory / journal.LATCHES['verify']).exists())

    def test_callback_support_journal_mutation_after_intent_is_caught(self):
        def mutate(job):
            if job[controller.JOB_KEY]['state'] == 'initialize-empty-dispatch-intent':
                job['resources'][controller.support.SUPPORT_STAGE_JOURNAL_KEY]['state'] = 'changed'
        self.before_persist = mutate
        with self.assertRaisesRegex(controller.GuardedHotelControllerError, 'support or SSH identity changed'):
            self.run_controller()
        self.assertTrue((self.directory / journal.LATCHES['initialize-empty']).exists())
        self.assertFalse(self.dispatches)

    def test_released_lease_after_durable_intent_is_caught_before_patch(self):
        def release(job):
            if job[controller.JOB_KEY]['state'] == 'initialize-empty-dispatch-intent':
                self.lease.release()
        self.before_persist = release
        with self.assertRaises(RunLeaseError):
            self.run_controller()
        self.assertTrue((self.directory / journal.LATCHES['initialize-empty']).exists())
        self.assertFalse(self.dispatches)

    def test_successful_transport_with_inexact_go_output_cannot_dispatch_verifier(self):
        self.output_changes = {'database_verification_completed': False}
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.run_controller()
        self.assertEqual(self.dispatches, ['initialize-empty'])
        self.assertEqual(self.job[controller.JOB_KEY]['state'], 'failed-cleanup-only')
        self.assertFalse((self.directory / journal.LATCHES['verify']).exists())

    def test_boolean_exit_code_is_not_a_successful_integer_status(self):
        self.finished_exit_code = False
        with self.assertRaises(controller.GuardedHotelControllerError):
            self.run_controller()
        self.assertEqual(self.dispatches, ['initialize-empty'])
        self.assertFalse((self.directory / journal.LATCHES['verify']).exists())

    def test_admitted_pod_without_its_gate_cannot_be_dispatched(self):
        self.created_states['initialize-empty'] = 'running'
        with self.assertRaisesRegex(controller.GuardedHotelControllerError, 'must remain blocked'):
            self.run_controller()
        self.assertFalse(self.dispatches)
        self.assertFalse((self.directory / journal.LATCHES['initialize-empty']).exists())

    def test_public_history_or_preexisting_results_are_rejected_before_remote_calls(self):
        self.job['_persist_state'] = True
        with self.assertRaises(controller.GuardedHotelControllerError):
            self.run_controller()
        self.job['_persist_state'] = False
        self.job['results'] = [{'status': 'success'}]
        with self.assertRaises(controller.GuardedHotelControllerError):
            self.run_controller()
        self.assertFalse(self.calls)
        self.assertFalse(list(self.directory.glob(journal.PREFIX + '*')))


if __name__ == '__main__':
    unittest.main()
