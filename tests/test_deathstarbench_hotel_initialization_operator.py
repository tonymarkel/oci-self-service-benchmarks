"""Lifecycle simulations only: no cloud, registry, Kubernetes or MongoDB calls."""

import copy
from contextlib import redirect_stderr
from io import StringIO
import os
from pathlib import Path
import signal
import tempfile
import unittest
from unittest.mock import patch

from app import deathstarbench_candidate_operator as support
from app import deathstarbench_hotel_initialization_operator as operator
from app.run_lease import acquire_run_lease, require_acquired_run_lease
from scripts import qualify_deathstarbench_hotel_initialization as cli
from scripts.deathstarbench_candidate_operator_state import CandidateStateError
from scripts.deathstarbench_hotel_initialization_operator_state import HotelInitializationStateStore
from tests import test_deathstarbench_hotel_initialization_artifacts as fixtures


RUN = '123456abcdef'
ATTEMPT = '87654321-4321-4321-4321-cba987654321'


class SimulatedCancellation(BaseException):
    pass


class HotelInitializationOperatorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name) / '.candidate-support'
        self.directory.mkdir(mode=0o700)
        (self.directory / RUN).mkdir(mode=0o700)
        self.lease = acquire_run_lease(self.directory, RUN, 'candidate-support-operator')
        self.addCleanup(self.lease.release)
        fixture = fixtures.GuardedHotelArtifactsTests()
        self.addCleanup(fixture.doCleanups)
        self.data = fixture.fixture()
        self.guarded = fixture.adopt(self.data)
        self.artifacts = self.data['baseline']
        plan = {'provider': 'aws', 'benchmarks': ['deathstarbench'], 'deathstarbench': {'workload': 'social_network'}}
        pin = {
            'schema_version': 1, 'candidate_schema': support.PIN_SCHEMA,
            'candidate_only': True, 'released': False, 'run_id': RUN,
            'workload_id': 'hotel_reservation', 'artifact_fingerprint': self.artifacts.fingerprint,
            'artifacts': self.artifacts.metadata(), 'infrastructure_plan': plan,
            'provider_pin': {'provider': 'aws', 'account_id': 'synthetic-fixture'},
            'ssh_public_key_sha256': '1' * 64,
        }
        pin['fingerprint'] = support._fingerprint(pin)
        self.pin = pin
        self.hotel_pin = operator.make_hotel_initialization_pin(RUN, pin, self.guarded, attempt_id=ATTEMPT)
        self.job = {
            'id': RUN, 'resources': {}, 'results': [], '_persist_state': False,
            'plan': copy.deepcopy(plan), support.PIN_KEY: copy.deepcopy(pin),
            operator.PIN_KEY: copy.deepcopy(self.hotel_pin),
        }
        self.calls, self.saved = [], []
        self.cloud_identity = {'immutable_create_id': 'synthetic-vm-create-1'}
        validation = patch.object(support, '_validated_support', side_effect=lambda job, artifacts, value, access: copy.deepcopy(value))
        validation.start()
        self.addCleanup(validation.stop)

    def persist(self, job, lease):
        require_acquired_run_lease(lease, RUN)
        self.saved.append(copy.deepcopy(job))

    def provision(self, job, lease):
        self.calls.append('provision')
        self.assertEqual(job[operator.OPERATOR_JOURNAL_KEY]['state'], 'provisioning')
        job['resources']['synthetic_owned_cloud'] = True

    def audit(self, job, lease):
        self.calls.append('audit')
        return {'schema_version': 1, 'candidate_only': True, 'read_only': True,
                'run_id': RUN, 'provider': 'aws', 'cloud_identity': copy.deepcopy(self.cloud_identity)}

    def runtime(self, job, lease):
        self.calls.append('runtime')

    def access(self, job, lease):
        self.calls.append('access')
        return {
            'schema_version': 1, 'candidate_schema': 'candidate-support-image-access-v1',
            'scope': 'active-support-images-only-no-runtime-or-application-qualification',
            'candidate_only': True, 'anonymous_support_bytes_verified': True,
            'support_image_execution_performed': False,
            **{name: False for name in ('released', 'runtime_qualified', 'measurement_qualified',
                                       'anonymous_pull_qualified', 'anonymous_layer_pull_qualified', 'application_start_allowed')},
            'identity': {'run_id': RUN, 'provider': 'aws', 'workload_id': 'hotel_reservation',
                         'artifact_fingerprint': self.artifacts.fingerprint},
        }

    def stage(self, job, lease):
        self.calls.append('stage')
        value = {'state': support.READY_STATE, 'fixture_only': True, 'candidate_only': True, 'released': False}
        job['resources'][support.SUPPORT_STAGE_JOURNAL_KEY] = value
        return value

    def initialize(self, job, lease):
        self.calls.append('initialize')
        self.assertEqual(job[operator.OPERATOR_JOURNAL_KEY]['state'], 'initializing')
        self.assertIn(support.SUPPORT_STAGE_JOURNAL_KEY, job['resources'])
        return {
            'schema_version': 1, 'candidate_schema': 'hotel-guarded-live-initialization-outcome-v1',
            'candidate_only': True, 'released': False, 'run_id': RUN,
            'initialization_attempt_id': ATTEMPT, 'guarded_artifact_fingerprint': self.guarded.fingerprint,
            'state': 'initialized-and-globally-verified-applications-paused', 'initialization_executed': True,
            'database_execution_verified': True, 'global_readonly_verification_completed': True,
            'applications_paused': True, 'all_application_replicas_zero': True,
            **{name: False for name in ('application_start_allowed', 'runtime_qualified', 'measurement_qualified',
                                       'cloud_qualified', 'public_release', 'public_dispatch_enabled')},
        }

    def destroy(self, job, lease):
        self.calls.append('destroy')
        self.assertNotIn(support.SUPPORT_STAGE_JOURNAL_KEY, job['resources'])
        job['resources'].clear()
        return {'schema_version': 1, 'candidate_schema': support.CLEANUP_PROOF_SCHEMA,
                'candidate_only': True, 'run_id': RUN, 'provider': 'aws',
                'cleanup_complete': True, 'independent_absence_proved': True,
                'synthetic_fixture_not_live_cloud_proof': True}

    def run_operator(self, **overrides):
        kwargs = dict(
            lease=self.lease, pin=self.pin, initialization_pin=self.hotel_pin,
            artifacts=self.artifacts, guarded_artifacts=self.guarded, persist=self.persist,
            provision=self.provision, fresh_audit=self.audit, prepare_runtime=self.runtime,
            support_image_access=self.access, stage=self.stage, initialize=self.initialize,
            destroy_and_prove=self.destroy, interrupt_exceptions=(KeyboardInterrupt, SimulatedCancellation),
        )
        kwargs.update(overrides)
        return operator.run_guarded_hotel_initialization_operator(self.job, **kwargs)

    def test_actual_initialization_callback_is_after_strict_support_and_before_durable_archive_cleanup(self):
        result = self.run_operator()
        self.assertEqual(self.calls, ['provision', 'audit', 'runtime', 'audit', 'access', 'stage', 'initialize', 'destroy'])
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(result.support_stage_complete and result.initialization_complete and result.cleanup_complete)
        self.assertNotIn(support.OPERATOR_JOURNAL_KEY, self.job)
        self.assertFalse(any(self.job[operator.OPERATOR_JOURNAL_KEY]['scope']['qualification'].values()))
        self.assertIs(self.job[operator.OPERATOR_JOURNAL_KEY]['scope']['operator_started_applications'], False)
        self.assertTrue(any(support.STAGE_ARCHIVE_KEY in value and support.SUPPORT_STAGE_JOURNAL_KEY in value['resources']
                            for value in self.saved))
        snapshot = result.initialization_snapshot
        snapshot['run_id'] = 'crossed'
        self.assertEqual(result.initialization_snapshot['run_id'], RUN)
        require_acquired_run_lease(self.lease, RUN)

    def test_each_phase_failure_still_cleans_and_never_runs_later_initialization(self):
        for phase in ('provision', 'fresh_audit', 'prepare_runtime', 'support_image_access', 'stage', 'initialize'):
            with self.subTest(phase=phase):
                self.job.pop(operator.OPERATOR_JOURNAL_KEY, None)
                self.job.pop(support.STAGE_ARCHIVE_KEY, None)
                self.job['resources'].clear()
                self.calls.clear()
                def fail(job, lease):
                    self.calls.append('failed-' + phase)
                    raise RuntimeError('simulated phase failure')
                result = self.run_operator(**{phase: fail})
                self.assertIn('destroy', self.calls)
                self.assertNotIn('initialize', self.calls)
                self.assertFalse(result.initialization_complete)
                self.assertTrue(result.cleanup_complete)
                self.assertEqual(result.exit_code, 1)

    def test_cleanup_only_has_no_source_registry_ssh_or_initializer_dependency(self):
        self.job[operator.OPERATOR_JOURNAL_KEY] = operator._new_record(self.hotel_pin)
        self.job['resources']['synthetic_owned_cloud'] = True
        result = self.run_operator(cleanup_only=True, artifacts=None, guarded_artifacts=None,
                                   provision=None, fresh_audit=None, prepare_runtime=None,
                                   support_image_access=None, stage=None, initialize=None)
        self.assertEqual(self.calls, ['destroy'])
        self.assertEqual(result.exit_code, 0)
        self.assertFalse(result.initialization_complete)

    def test_existing_history_cannot_be_replayed_even_before_initializer_intent(self):
        self.job[operator.OPERATOR_JOURNAL_KEY] = operator._new_record(self.hotel_pin)
        with self.assertRaises(operator.HotelInitializationOperatorError):
            self.run_operator()
        self.assertEqual(self.calls, [])

    def test_mutation_response_loss_cleans_without_repeat_or_success_claim(self):
        dispatches = []
        def lost_response(job, lease):
            dispatches.append(1)
            raise SimulatedCancellation('response lost after initializer gate release')
        result = self.run_operator(initialize=lost_response)
        self.assertEqual(dispatches, [1])
        self.assertFalse(result.initialization_complete)
        self.assertTrue(result.cleanup_complete)
        self.assertEqual(result.exit_code, 130)

    def test_success_metadata_cannot_promote_release_or_use_boolean_number_aliases(self):
        for key, invalid in (('schema_version', True), ('released', True),
                             ('global_readonly_verification_completed', 1), ('application_start_allowed', True)):
            with self.subTest(key=key):
                self.job.pop(operator.OPERATOR_JOURNAL_KEY, None)
                self.job.pop(support.STAGE_ARCHIVE_KEY, None)
                self.job['resources'].clear()
                def bad(job, lease):
                    value = self.initialize(job, lease)
                    value[key] = invalid
                    return value
                result = self.run_operator(initialize=bad)
                self.assertEqual(result.exit_code, 1)
                self.assertFalse(result.initialization_complete)
                self.assertTrue(result.cleanup_complete)

    def test_fresh_cloud_identity_drift_stops_before_image_or_initializer_work(self):
        def drift(job, lease):
            self.cloud_identity['immutable_create_id'] = 'same-name-replacement'
        result = self.run_operator(prepare_runtime=drift)
        self.assertEqual(result.exit_code, 1)
        self.assertNotIn('access', self.calls)
        self.assertNotIn('initialize', self.calls)
        self.assertIn('destroy', self.calls)

    def test_cleanup_proof_failure_is_not_a_successful_exit(self):
        result = self.run_operator(destroy_and_prove=lambda job, lease: True)
        self.assertTrue(result.initialization_complete)
        self.assertFalse(result.cleanup_complete)
        self.assertEqual(result.exit_code, 2)

    def test_repeated_cleanup_signals_are_deferred_and_preserve_nonzero_status(self):
        def signalled(job, lease):
            os.kill(os.getpid(), signal.SIGTERM)
            os.kill(os.getpid(), signal.SIGINT)
            return self.destroy(job, lease)
        result = self.run_operator(destroy_and_prove=signalled)
        self.assertTrue(result.cleanup_complete)
        self.assertEqual(result.exit_code, 128 + signal.SIGTERM)

    def test_immutable_attempt_pin_cannot_change_during_callback_or_recovery(self):
        self.job[operator.PIN_KEY]['initialization_attempt_id'] = '87654321-4321-4321-4321-cba987654322'
        with self.assertRaises(operator.HotelInitializationOperatorError):
            self.run_operator()
        self.assertEqual(self.calls, [])

    def test_private_state_store_independently_enforces_attempt_file_on_save_load(self):
        store = HotelInitializationStateStore(self.directory)
        store.publish_pin(self.pin, self.lease)
        store.publish_hotel_pin(self.hotel_pin, self.lease)
        store.save(self.job, self.lease)
        self.assertEqual(store.load(RUN, self.lease)[operator.PIN_KEY], self.hotel_pin)
        with self.assertRaises(CandidateStateError):
            store.publish_hotel_pin(self.hotel_pin, self.lease)
        crossed = copy.deepcopy(self.job)
        crossed[operator.PIN_KEY]['initialization_attempt_id'] = '87654321-4321-4321-4321-cba987654322'
        with self.assertRaises(CandidateStateError):
            store.save(crossed, self.lease)
        self.assertEqual(store.load(RUN, self.lease)[operator.PIN_KEY], self.hotel_pin)

    def test_cli_cleanup_refuses_plan_source_ssh_and_provider_overrides_before_any_callbacks(self):
        for option in ('--plan', '--guarded-lock', '--ssh-env-file', '--aws-image-pin'):
            args = cli._parser().parse_args(['--cleanup-only', RUN, option, 'ignored'])
            with self.assertRaises(ValueError):
                cli._validate_arguments(args)
        with self.assertRaises(SystemExit), redirect_stderr(StringIO()):
            cli._parser().parse_args(['--resume', RUN])
        args = cli._parser().parse_args(['--cleanup-only', RUN])
        cli._validate_arguments(args)

    def test_cli_response_loss_checkpoint_is_durable_and_non_replaying(self):
        args = cli._parser().parse_args(['--interrupt-after', 'initialize-response-returned'])
        saved = []
        with self.assertRaises(cli.application.RunCancelled):
            cli._checkpoint(args, self.job, 'initialize-response-returned', lambda value: saved.append(copy.deepcopy(value)))
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]['hotel_initialization_interruption']['recovery_policy'], 'cleanup-only-never-replay')


if __name__ == '__main__':
    unittest.main()
