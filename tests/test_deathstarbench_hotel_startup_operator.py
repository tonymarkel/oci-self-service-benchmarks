"""Lifecycle simulations using original factories; no cloud qualification."""
import copy
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import signal
import tempfile
import unittest

from app import deathstarbench_candidate_deployment as deployment
from app import deathstarbench_candidate_k3s as k3s
from app import deathstarbench_candidate_operator as support
from app import deathstarbench_distributed as distributed
from app import deathstarbench_hotel_initialization_operator as initialization
from app import deathstarbench_hotel_startup_operator as operator
from app import deathstarbench_hotel_startup_ownership as ownership
from app.deathstarbench_candidate_storage import parse_candidate_database_storage_attestation
from app.deathstarbench_candidate_storage_observation import parse_candidate_database_storage_observation
from app.deathstarbench_hotel_runtime import GuardedHotelRuntimePlan
from app.deathstarbench_hotel_initialization_stage import GuardedHotelInitializationStage
from app.run_lease import acquire_run_lease, require_acquired_run_lease
from tests import test_deathstarbench_distributed_runtime as cloud_fixture
from tests import test_deathstarbench_hotel_live_k3s as init_fixture
from tests import test_deathstarbench_candidate_storage as storage
from tests import test_deathstarbench_candidate_storage_observation as storage_read
from tests.test_deathstarbench_hotel_startup_ownership import OriginalArtifactsMixin, RUN, INITIALIZATION, STARTUP


class StartupOperatorTests(OriginalArtifactsMixin, unittest.TestCase):
    def setUp(self):
        self.job = self.new_job()
        self.calls, self.saved = [], []
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / '.candidate-support'
        root.mkdir(mode=0o700)
        (root / RUN).mkdir(mode=0o700)
        self.lease = acquire_run_lease(root, RUN, 'startup-operator-test')
        self.addCleanup(self.lease.release)
        self.archive_failed = False
        self.cloud = {'create_uid': 'offline-original-create'}

    def persist(self, job, lease):
        self.assertIs(lease, self.lease)
        require_acquired_run_lease(lease, RUN)
        if job[operator.OPERATOR_JOURNAL_KEY]['state'] == 'accepted':
            self.calls.append('intent')
            self.assertIs(job[ownership.CHECKPOINT_KEY]['contract_recorded'], False)
        if 'hotel_startup_archive' in job and self.archive_failed:
            raise OSError('https://registry.invalid/?token=SIGNED_SECRET')
        self.saved.append(ownership.project_hotel_startup_job(job))

    def provision(self, job, lease):
        self.calls.append('provision')
        # Simulated provider creates ownership; Task6 validates real schemas.
        job[ownership.CHECKPOINT_KEY] = ownership.update_hotel_startup_ownership_checkpoint(
            job[ownership.CHECKPOINT_KEY], self.provider_projection())
        job['resources'].update(copy.deepcopy(cloud_fixture.candidate_job()['resources']))
        from app.providers import azure
        job['resources']['azure_resource_group_tags'] = azure._tags(RUN, 'resource-group')

    def audit(self, job, lease):
        self.calls.append('audit')
        return {'schema_version': 1, 'candidate_only': True, 'read_only': True,
                'run_id': RUN, 'provider': 'azure', 'cloud_identity': copy.deepcopy(self.cloud)}

    def runtime(self, job, lease):
        self.calls.append('runtime')
        remote = cloud_fixture.FakeRemoteExecutor(
            ca_sha256=hashlib.sha256(b'OFFLINE-CA-NOT-A-REAL-CERTIFICATE').hexdigest())
        distributed.prepare_azure_distributed_k3s_candidate(job, execute=remote)

    def access(self, job, lease):
        self.calls.append('access')
        _, plan, stage, _ = deployment._context(job, self.baseline, distributed.distributed_k3s_candidate_plan)
        return {'schema_version': 1, 'candidate_schema': 'candidate-support-image-access-v1',
                'scope': 'active-support-images-only-no-runtime-or-application-qualification',
                'candidate_only': True, 'anonymous_support_bytes_verified': True,
                'support_image_execution_performed': False,
                **{key: False for key in ('released', 'runtime_qualified', 'measurement_qualified',
                   'anonymous_pull_qualified', 'anonymous_layer_pull_qualified', 'application_start_allowed')},
                'identity': {'run_id': RUN, 'provider': 'azure', 'workload_id': 'hotel_reservation',
                    'artifact_fingerprint': self.baseline.fingerprint, 'snapshot_fingerprint': plan.fingerprint,
                    'stage_fingerprint': stage.fingerprint, 'stage_manifest_sha256': stage.rendered_manifest_sha256}}

    def stage(self, job, lease):
        self.calls.append('support')
        _, plan, stage, context = deployment._context(job, self.baseline, distributed.distributed_k3s_candidate_plan)
        inventory = init_fixture.support_fixtures.live_inventory(stage)
        ready = deployment._new_journal(plan, stage, context)
        ready.update(state=deployment.READY_STATE, applied_phases=[p for p, _ in stage.phases],
            storage_preparation=parse_candidate_database_storage_attestation(storage.marker(plan.storage), plan.storage),
            storage_observation=parse_candidate_database_storage_observation(storage_read.marker(plan.storage), plan.storage),
            support_attestation=k3s._support_receipt(inventory, k3s._validated_context(plan, stage, context['role_nodes'])))
        ready['phase_receipts'] = {p: k3s._reconciliation_receipt(inventory,
            k3s._phase_context(plan, stage, p, context['role_nodes'])) for p, _ in stage.phases}
        job['resources'][support.SUPPORT_STAGE_JOURNAL_KEY] = ready
        return ready

    def initialize(self, job, lease):
        self.calls.append('initialize')
        self.live = {'schema_version': 1, 'candidate_schema': 'hotel-guarded-live-initialization-outcome-v1',
            'candidate_only': True, 'released': False, 'run_id': RUN,
            'initialization_attempt_id': INITIALIZATION,
            'guarded_artifact_fingerprint': self.guarded_artifacts.fingerprint,
            'state': 'initialized-and-globally-verified-applications-paused',
            **{key: True for key in ('initialization_executed', 'database_execution_verified',
                'global_readonly_verification_completed', 'applications_paused', 'all_application_replicas_zero')},
            **{key: False for key in ('application_start_allowed', 'runtime_qualified', 'measurement_qualified',
                'cloud_qualified', 'public_release', 'public_dispatch_enabled')},
            'last_observation': {'pod': {'status': 'https://registry.invalid/?token=SIGNED_SECRET'}}}
        job['guarded_hotel_initialization'] = self.live
        return self.live

    def startup(self, job, lease, *, initialization_plan, initialization_stage):
        self.calls.append('startup')
        self.assertIs(type(initialization_plan), GuardedHotelRuntimePlan)
        self.assertIs(type(initialization_stage), GuardedHotelInitializationStage)
        self.assertEqual(initialization_plan.initialization_attempt_id, INITIALIZATION)
        self.assertIs(initialization_plan.artifacts, self.guarded_artifacts)
        self.assertIs(job['guarded_hotel_initialization'], self.live)
        self.assertIs(lease, self.lease)
        self.assertEqual(job[support.PIN_KEY], self.pin)
        self.assertEqual(job[initialization.PIN_KEY], self.initialization_pin)
        self.assertEqual(job[operator.PIN_KEY], self.startup_pin)
        self.assertNotEqual(INITIALIZATION, STARTUP)
        value = {'schema_version': 1, 'candidate_schema': 'hotel-live-startup-outcome-v1',
            'candidate_only': True, 'run_id': RUN, 'startup_attempt_id': STARTUP,
            'initialization_attempt_id': INITIALIZATION,
            'state': 'startup-and-service-identities-observed',
            'startup_execution_observed': True, 'service_identity_observed': True,
            **{name: False for name in ('released', 'application_start_allowed', 'application_started',
                'application_ready', 'startup_executed', 'initializer_authorized', 'runtime_authorized',
                'startup_authorized', 'runtime_qualified', 'measurement_qualified', 'dataset_qualified',
                'cloud_qualified', 'public_release', 'public_dispatch_enabled', 'semantic_correctness_observed')}}
        job['hotel_application_startup'] = value
        return copy.deepcopy(value)

    def destroy(self, job, lease):
        self.calls.append('destroy')
        self.assertIs(lease, self.lease)
        require_acquired_run_lease(lease, RUN)
        # External boundary simulation only. Actual file/SDK mandatory ordering
        # belongs to Task6, not this fake's claim of publication.
        seal = ownership.make_hotel_startup_cleanup_ownership(job, support_pin=self.pin,
            initialization_pin=self.initialization_pin, startup_pin=self.startup_pin)
        self.assertEqual(seal['absence_targets']['group']['id'], '/subscriptions/offline/groups/original')
        if seal['audits']:
            self.assertEqual(seal['audits'][0]['cloud_identity'], {'create_uid': 'offline-original-create'})
        self.calls.append('absence')
        return {'schema_version': 1, 'candidate_schema': support.CLEANUP_PROOF_SCHEMA,
            'candidate_only': True, 'run_id': RUN, 'provider': 'azure',
            'cleanup_complete': True, 'independent_absence_proved': True}

    def run_operator(self, **overrides):
        callbacks = dict(lease=self.lease, pin=self.pin, initialization_pin=self.initialization_pin,
            startup_pin=self.startup_pin, persist=self.persist, provision=self.provision,
            fresh_audit=self.audit, prepare_runtime=self.runtime, support_image_access=self.access,
            stage=self.stage, initialize=self.initialize, startup=self.startup, destroy_and_prove=self.destroy,
            artifacts=self.baseline, guarded_artifacts=self.guarded_artifacts, startup_artifacts=self.startup_artifacts)
        callbacks.update(overrides)
        return operator.run_hotel_startup_operator(self.job, **callbacks)

    def test_one_original_execution_lease_covers_every_phase_and_teardown(self):
        result = self.run_operator()
        self.assertEqual(self.calls, ['intent', 'provision', 'audit', 'runtime', 'audit', 'access',
                                     'support', 'initialize', 'startup', 'destroy', 'absence'])
        self.assertEqual((result.execution_outcome, result.cleanup_outcome), ('completed', 'completed'))
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(self.job[operator.PIN_KEY], self.startup_pin)
        self.assertIs(self.job['guarded_hotel_initialization'], self.live)
        self.assertNotIn('SIGNED_SECRET', str(self.job['hotel_startup_archive']))
        require_acquired_run_lease(self.lease, RUN)

    def test_failure_is_sticky_even_if_destroy_succeeds(self):
        def fail(job, lease):
            self.calls.append('failed-initialize')
            raise RuntimeError('https://registry.invalid/?token=SIGNED_SECRET')
        result = self.run_operator(initialize=fail)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'completed')
        self.assertEqual(self.calls.count('startup'), 0)
        self.assertNotIn('SIGNED_SECRET', str(result))

    def test_archive_failure_does_not_suppress_independently_owned_teardown(self):
        self.archive_failed = True
        result = self.run_operator()
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'completed')
        self.assertIn('absence', self.calls)

    def test_mandatory_publication_failure_blocks_provider_deletion_and_fails_cleanup(self):
        def publication_fails(job, lease):
            raise OSError('seal publication failed')
        result = self.run_operator(destroy_and_prove=publication_fails)
        self.assertEqual(result.cleanup_outcome, 'failed')
        self.assertNotIn('destroy', self.calls)
        self.assertNotEqual(result.exit_code, 0)

    def test_corrupt_execution_history_does_not_disable_intact_cleanup_binding(self):
        def corrupt(job, lease):
            job[operator.OPERATOR_JOURNAL_KEY] = {'corrupt': True}
            raise RuntimeError('failed execution custody')
        result = self.run_operator(initialize=corrupt)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'completed')
        self.assertNotIn('startup', self.calls)

    def test_missing_checkpoint_blocks_cleanup(self):
        def corrupt(job, lease):
            job.pop(ownership.CHECKPOINT_KEY)
            raise RuntimeError('ownership lost')
        result = self.run_operator(initialize=corrupt)
        self.assertEqual(result.cleanup_outcome, 'failed')
        self.assertNotIn('destroy', self.calls)

    def test_prior_history_refuses_every_execution_callback(self):
        self.job[operator.OPERATOR_JOURNAL_KEY] = {'state': 'accepted'}
        with self.assertRaises(ValueError):
            self.run_operator()
        self.assertEqual(self.calls, [])

    def test_precontract_failure_is_not_required_not_fake_independent_absence(self):
        def fail(job, lease):
            raise RuntimeError('read-only preflight failure before contract authorization')
        result = self.run_operator(provision=fail)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'not_required')
        self.assertIsNone(result.cleanup_error)
        self.assertNotEqual(result.exit_code, 0)
        self.assertNotIn('destroy', self.calls)
        self.assertEqual(self.job['hotel_startup_cleanup_not_required']['reason'],
                         'provider-create-contract-never-authorized')

    def test_initial_checkpoint_publication_failure_cannot_claim_not_required(self):
        def fail(job, lease):
            raise OSError('initial checkpoint installation uncertain')
        result = self.run_operator(persist=fail)
        self.assertEqual(result.cleanup_outcome, 'failed')
        self.assertNotIn('destroy', self.calls)
        self.assertNotIn('hotel_startup_cleanup_not_required', self.job)

    def test_lower_initializer_detached_return_keeps_original_live_record_custody(self):
        # Real lower controller returns _copy(current), not job[JOB_KEY] itself.
        # Catches an identity check that would reject the actual API contract.
        def initialize(job, lease):
            return copy.deepcopy(self.initialize(job, lease))
        result = self.run_operator(initialize=initialize)
        self.assertEqual(result.execution_outcome, 'completed')
        self.assertIs(self.job['guarded_hotel_initialization'], self.live)
        self.assertIn('startup', self.calls)

    def test_archive_retains_safe_actual_startup_controller_key_and_no_raw_payload(self):
        def startup(job, lease, **kwargs):
            value = self.startup(job, lease, **kwargs)
            job['hotel_application_startup'] = {**value, 'last_observation': {
                'http': 'https://registry.invalid/?token=SIGNED_SECRET'}}
            return copy.deepcopy(job['hotel_application_startup'])
        result = self.run_operator(startup=startup)
        self.assertEqual(result.execution_outcome, 'completed')
        self.assertIs(self.job['hotel_startup_archive']['hotel_application_startup']['startup_execution_observed'], True)
        self.assertNotIn('SIGNED_SECRET', str(self.job['hotel_startup_archive']))

    def test_persisted_execution_failure_remains_failed_after_completed_cleanup(self):
        # Catches a cleanup_complete journal erasing execution failure in history.
        def fail(job, lease):
            raise RuntimeError('application startup failed')
        result = self.run_operator(startup=fail)
        self.assertEqual(result.execution_outcome, 'failed')
        final = self.saved[-1][operator.OPERATOR_JOURNAL_KEY]
        self.assertEqual(final['execution_outcome'], 'failed')
        self.assertEqual(final['cleanup_outcome'], 'completed')
        self.assertEqual(final['error']['code'], 'execution-failed')

    def test_lost_original_artifact_capability_stops_execution_but_not_cleanup(self):
        # Catches accidentally requiring the failed executable source proof for
        # independent cleanup. The real factory proof is invalidated temporarily.
        original = self.guarded_artifacts._proof
        def fail(job, lease):
            self.initialize(job, lease)
            object.__setattr__(self.guarded_artifacts, '_proof', None)
            return copy.deepcopy(self.live)
        try:
            result = self.run_operator(initialize=fail)
            self.assertEqual(result.execution_outcome, 'failed')
            self.assertEqual(result.cleanup_outcome, 'completed')
            self.assertNotIn('startup', self.calls)
        finally:
            object.__setattr__(self.guarded_artifacts, '_proof', original)

    def test_crossed_immutable_pin_blocks_cleanup_even_with_self_consistent_hash(self):
        def corrupt(job, lease):
            pin = job[operator.PIN_KEY]
            pin['startup_attempt_id'] = '87654321-4321-4321-8321-cba987654323'
            pin['fingerprint'] = support._fingerprint({key: value for key, value in pin.items() if key != 'fingerprint'})
            raise RuntimeError('crossed pin')
        result = self.run_operator(initialize=corrupt)
        self.assertEqual(result.cleanup_outcome, 'failed')
        self.assertNotIn('destroy', self.calls)

    def test_cloud_identity_drift_stops_before_support_or_initializer(self):
        def drift(job, lease):
            self.runtime(job, lease)
            self.cloud['create_uid'] = 'same-name-replacement'
        result = self.run_operator(prepare_runtime=drift)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'completed')
        self.assertNotIn('access', self.calls)
        self.assertNotIn('initialize', self.calls)

    def test_initialization_and_startup_success_cannot_use_boolean_number_aliases(self):
        def invalid(job, lease):
            value = self.initialize(job, lease)
            value['global_readonly_verification_completed'] = 1
            return value
        result = self.run_operator(initialize=invalid)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertNotIn('startup', self.calls)
        self.assertEqual(result.cleanup_outcome, 'completed')

    def test_startup_completion_cannot_cross_the_original_initialization_attempt(self):
        # Catches accepting a completion receipt for another initializer attempt.
        def crossed(job, lease, **kwargs):
            value = self.startup(job, lease, **kwargs)
            value['initialization_attempt_id'] = '87654321-4321-4321-8321-cba987654323'
            job['hotel_application_startup'] = copy.deepcopy(value)
            return value
        result = self.run_operator(startup=crossed)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'completed')

    def test_cleanup_signals_are_deferred_under_original_lease_and_fail_execution(self):
        def signalled(job, lease):
            os.kill(os.getpid(), signal.SIGTERM)
            os.kill(os.getpid(), signal.SIGINT)
            return self.destroy(job, lease)
        result = self.run_operator(destroy_and_prove=signalled)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'completed')
        self.assertEqual(result.exit_code, 128 + signal.SIGTERM)
        self.assertEqual(self.saved[-1][operator.OPERATOR_JOURNAL_KEY]['execution_outcome'], 'failed')

    def test_final_evidence_failure_cannot_produce_success_after_proved_cleanup(self):
        def persist(job, lease):
            if job.get(operator.OPERATOR_JOURNAL_KEY, {}).get('state') == 'cleanup_complete':
                raise OSError('final evidence unavailable')
            self.persist(job, lease)
        result = self.run_operator(persist=persist)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'completed')
        self.assertNotEqual(result.exit_code, 0)

    def test_cleanup_context_exit_failure_fails_execution_and_corrects_terminal_history(self):
        # Catches a post-yield runner failure being hidden by completed absence.
        @contextmanager
        def exit_fails():
            yield {'number': None}
            raise RuntimeError('https://registry.invalid/?token=SIGNED_SECRET')
        result = self.run_operator(cleanup_signal_context=exit_fails)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'completed')
        self.assertNotEqual(result.exit_code, 0)
        self.assertEqual(self.saved[-1][operator.OPERATOR_JOURNAL_KEY]['execution_outcome'], 'failed')
        self.assertEqual(self.saved[-1][operator.OPERATOR_JOURNAL_KEY]['cleanup_outcome'], 'completed')
        self.assertEqual(self.calls.count('destroy'), 1)
        self.assertEqual(self.calls.count('absence'), 1)
        self.assertNotIn('SIGNED_SECRET', str(result))
        require_acquired_run_lease(self.lease, RUN)

    def test_cleanup_context_exit_interruption_fails_execution_and_saved_terminal_record(self):
        # Catches interruption while restoring the cleanup context's handlers.
        @contextmanager
        def interrupted_exit():
            yield {'number': None}
            raise KeyboardInterrupt()
        result = self.run_operator(cleanup_signal_context=interrupted_exit)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'completed')
        self.assertEqual(result.exit_code, 130)
        self.assertIs(result.error['interrupted'], True)
        self.assertEqual(self.saved[-1][operator.OPERATOR_JOURNAL_KEY]['execution_outcome'], 'failed')
        self.assertIs(self.saved[-1][operator.OPERATOR_JOURNAL_KEY]['error']['interrupted'], True)
        self.assertEqual(self.calls.count('destroy'), 1)

    def test_signal_during_final_persist_corrects_saved_execution_outcome(self):
        # The real deferred signal arrives AFTER the evidence callback saves
        # its snapshot, not during destroy as in the earlier signal coverage.
        delivered = False
        def persist(job, lease):
            nonlocal delivered
            self.persist(job, lease)
            if (job.get(operator.OPERATOR_JOURNAL_KEY, {}).get('state') == 'cleanup_complete'
                    and not delivered):
                delivered = True
                os.kill(os.getpid(), signal.SIGTERM)
        result = self.run_operator(persist=persist)
        self.assertTrue(delivered)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'completed')
        self.assertEqual(result.exit_code, 128 + signal.SIGTERM)
        self.assertEqual(self.saved[-1][operator.OPERATOR_JOURNAL_KEY]['execution_outcome'], 'failed')
        self.assertIs(self.saved[-1][operator.OPERATOR_JOURNAL_KEY]['error']['interrupted'], True)
        self.assertEqual(self.calls.count('destroy'), 1)
        require_acquired_run_lease(self.lease, RUN)

    def test_failed_corrective_terminal_publication_never_returns_success_or_repeats_teardown(self):
        # Publication failure cannot repair the old durable snapshot; execution
        # must nevertheless remain failed rather than trusting that snapshot.
        @contextmanager
        def exit_fails():
            yield {'number': None}
            raise RuntimeError('runner context exit failed')
        corrections = []
        def persist(job, lease):
            record = job.get(operator.OPERATOR_JOURNAL_KEY, {})
            if record.get('state') == 'cleanup_complete' and record.get('execution_outcome') == 'failed':
                corrections.append(1)
                raise OSError('https://registry.invalid/?token=SIGNED_SECRET')
            self.persist(job, lease)
        result = self.run_operator(persist=persist, cleanup_signal_context=exit_fails)
        self.assertEqual(result.execution_outcome, 'failed')
        self.assertEqual(result.cleanup_outcome, 'completed')
        self.assertNotEqual(result.exit_code, 0)
        self.assertEqual(corrections, [1])
        self.assertEqual(self.job[operator.OPERATOR_JOURNAL_KEY]['execution_outcome'], 'failed')
        self.assertEqual(self.job[operator.OPERATOR_JOURNAL_KEY]['cleanup_outcome'], 'completed')
        self.assertEqual(self.saved[-1][operator.OPERATOR_JOURNAL_KEY]['execution_outcome'], 'completed')
        self.assertEqual(self.calls.count('destroy'), 1)
        self.assertEqual(self.calls.count('absence'), 1)
        self.assertNotIn('SIGNED_SECRET', str(result))
