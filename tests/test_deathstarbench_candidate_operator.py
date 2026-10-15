"""Lifecycle simulations only: callbacks never contact cloud, registry or SSH.

Artifact adoption uses existing synthetic registry fixtures. Native support
stage validation is separately tested; these tests mock that validator when
exercising operator ordering, durability and cleanup failure paths.
"""

import copy
from contextlib import contextmanager
import json
import os
from pathlib import Path
import signal
import tempfile
import unittest
from unittest.mock import patch

from app import deathstarbench_candidate_operator as operator
from app.deathstarbench_candidate_deployment import SUPPORT_STAGE_JOURNAL_KEY
from app.run_lease import acquire_run_lease, require_acquired_run_lease
from tests import test_deathstarbench_candidate_artifacts as artifact_fixtures


RUN = '123456abcdef'


class SimulatedCancellation(BaseException):
    pass


class CandidateOperatorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name) / '.candidate-support'
        self.directory.mkdir()
        (self.directory / RUN).mkdir()
        self.lease = acquire_run_lease(self.directory, RUN, 'candidate-support-operator')
        self.addCleanup(self.lease.release)
        fixtures = artifact_fixtures.CandidateArtifactAdoptionTests()
        data = fixtures.fixture()
        self.addCleanup(fixtures.doCleanups)
        self.artifacts = fixtures.adopt(data)
        plan = {'provider': 'aws', 'benchmarks': ['deathstarbench'], 'deathstarbench': {'workload': 'social_network'}}
        pin = {
            'schema_version': 1, 'candidate_schema': operator.PIN_SCHEMA,
            'candidate_only': True, 'released': False, 'run_id': RUN,
            'workload_id': self.artifacts.workload_id, 'artifact_fingerprint': self.artifacts.fingerprint,
            'artifacts': self.artifacts.metadata(), 'infrastructure_plan': plan,
            'provider_pin': {'provider': 'aws', 'account_id': 'synthetic-fixture'},
            'ssh_public_key_sha256': '1' * 64,
        }
        pin['fingerprint'] = operator._fingerprint(pin)
        self.pin = pin
        self.job = {'id': RUN, 'resources': {}, 'results': [], '_persist_state': False,
                    'plan': copy.deepcopy(plan), operator.PIN_KEY: copy.deepcopy(pin)}
        self.calls, self.saved = [], []
        self.cloud_identity = {'immutable_create_id': 'synthetic-vm-create-1'}
        self.validation = patch.object(operator, '_validated_support', side_effect=lambda job, artifacts, value, access: copy.deepcopy(value))
        self.validation.start()
        self.addCleanup(self.validation.stop)

    def persist(self, job, lease):
        require_acquired_run_lease(lease, RUN)
        self.saved.append(copy.deepcopy(job))

    def provision(self, job, lease):
        self.calls.append('provision')
        self.assertEqual(job[operator.OPERATOR_JOURNAL_KEY]['state'], 'provisioning')
        job['resources']['synthetic_owned_cloud'] = True

    def audit(self, job, lease):
        self.calls.append('audit')
        return {'schema_version': 1, 'candidate_schema': 'synthetic-cloud-audit',
                'candidate_only': True, 'read_only': True, 'released': False,
                'run_id': RUN, 'provider': 'aws', 'cloud_identity': copy.deepcopy(self.cloud_identity)}

    def runtime(self, job, lease):
        self.calls.append('runtime')
        job['resources']['synthetic_runtime'] = 'cluster-ready'

    def access(self, job, lease):
        self.calls.append('access')
        return {'schema_version': 1, 'candidate_schema': 'candidate-support-image-access-v1',
                'scope': 'active-support-images-only-no-runtime-or-application-qualification',
                'candidate_only': True, 'released': False, 'runtime_qualified': False,
                'measurement_qualified': False, 'anonymous_pull_qualified': False,
                'anonymous_layer_pull_qualified': False, 'application_start_allowed': False,
                'anonymous_support_bytes_verified': True, 'support_image_execution_performed': False,
                'identity': {'run_id': RUN, 'provider': 'aws', 'workload_id': self.pin['workload_id'],
                             'artifact_fingerprint': self.artifacts.fingerprint}}

    def stage(self, job, lease):
        self.calls.append('stage')
        value = {'candidate_only': True, 'released': False, 'fixture_only': True,
                 'state': operator.READY_STATE, 'operator_started_applications': False,
                 'operator_initialized_dataset': False, 'run_id': RUN}
        job['resources'][SUPPORT_STAGE_JOURNAL_KEY] = value
        return value

    def destroy(self, job, lease):
        self.calls.append('destroy')
        self.assertNotIn(SUPPORT_STAGE_JOURNAL_KEY, job['resources'])
        job['resources'].clear()
        return {'schema_version': 1, 'candidate_schema': operator.CLEANUP_PROOF_SCHEMA,
                'candidate_only': True, 'run_id': RUN, 'provider': 'aws',
                'cleanup_complete': True, 'independent_absence_proved': True,
                'synthetic_fixture_not_live_cloud_proof': True}

    def run_operator(self, **changes):
        options = dict(lease=self.lease, pin=self.pin, artifacts=self.artifacts, persist=self.persist,
                       provision=self.provision, fresh_audit=self.audit, prepare_runtime=self.runtime,
                       support_image_access=self.access, stage=self.stage, destroy_and_prove=self.destroy,
                       interrupt_exceptions=(KeyboardInterrupt, SimulatedCancellation))
        options.update(changes)
        return operator.run_candidate_support_operator(self.job, **options)

    def resumable(self, state='staging_support'):
        self.job['resources']['synthetic_owned_cloud'] = True
        record = operator._new_record(self.pin)
        record.update(state=state, cloud_identity=copy.deepcopy(self.cloud_identity))
        self.job[operator.OPERATOR_JOURNAL_KEY] = record

    def test_order_scope_and_durable_archive_precede_exact_key_removal(self):
        result = self.run_operator()
        self.assertEqual(self.calls, ['provision', 'audit', 'runtime', 'audit', 'access', 'stage', 'destroy'])
        self.assertEqual(result.exit_code, 0)
        self.assertTrue(result.support_stage_complete and result.cleanup_complete)
        self.assertTrue(all(value is False for value in self.job[operator.OPERATOR_JOURNAL_KEY]['scope']['qualification'].values()))
        self.assertIs(self.job[operator.OPERATOR_JOURNAL_KEY]['scope']['operator_started_applications'], False)
        self.assertIs(self.job[operator.OPERATOR_JOURNAL_KEY]['scope']['operator_initialized_dataset'], False)
        self.assertIs(self.job[operator.OPERATOR_JOURNAL_KEY]['scope']['operator_executed_benchmark'], False)
        self.assertNotIn('applications_started', self.job[operator.OPERATOR_JOURNAL_KEY]['scope'])
        self.assertNotIn('dataset_initialized', self.job[operator.OPERATOR_JOURNAL_KEY]['scope'])
        archive = self.job[operator.STAGE_ARCHIVE_KEY]
        self.assertFalse(archive['qualification_authority'])
        self.assertEqual(archive['payload_sha256'], operator._fingerprint(archive['payload']))
        archived_before_removal = [saved for saved in self.saved if operator.STAGE_ARCHIVE_KEY in saved
                                   and SUPPORT_STAGE_JOURNAL_KEY in saved['resources']]
        self.assertTrue(archived_before_removal)
        snapshot = result.support_snapshot
        snapshot['run_id'] = 'crossed'
        self.assertEqual(result.support_snapshot['run_id'], RUN)
        require_acquired_run_lease(self.lease, RUN)

    def test_each_phase_failure_still_attempts_cleanup_without_false_support_success(self):
        for phase in ('provision', 'fresh_audit', 'prepare_runtime', 'support_image_access', 'stage'):
            with self.subTest(phase=phase):
                self.job.pop(operator.OPERATOR_JOURNAL_KEY, None)
                self.job.pop(operator.STAGE_ARCHIVE_KEY, None)
                self.job['resources'].clear()
                self.calls.clear()
                def fail(job, lease):
                    self.calls.append('failed-' + phase)
                    raise RuntimeError('simulated ' + phase)
                result = self.run_operator(**{phase: fail})
                self.assertIn('destroy', self.calls)
                self.assertFalse(result.support_stage_complete)
                self.assertTrue(result.cleanup_complete)
                self.assertEqual(result.exit_code, 1)

    def test_cloud_create_identity_drift_before_stage_cleans_but_never_runs_access(self):
        def runtime(job, lease):
            self.runtime(job, lease)
            self.cloud_identity = {'immutable_create_id': 'same-name-replacement'}
        result = self.run_operator(prepare_runtime=runtime)
        self.assertEqual(result.exit_code, 1)
        self.assertNotIn('access', self.calls)
        self.assertIn('destroy', self.calls)

    def test_resume_is_audit_only_and_refuses_preownership_audit_failure_or_drift(self):
        self.resumable()
        result = self.run_operator(resume=True)
        self.assertEqual(self.calls, ['audit', 'runtime', 'audit', 'access', 'stage', 'destroy'])
        self.assertEqual(result.exit_code, 0)
        for drift in (False, True):
            self.resumable()
            self.calls.clear()
            def failed(job, lease):
                if not drift:
                    raise RuntimeError('missing role resource')
                value = self.audit(job, lease)
                value['cloud_identity'] = {'immutable_create_id': 'foreign-replacement'}
                return value
            with self.assertRaises(operator.CandidateOperatorRefusal):
                self.run_operator(resume=True, fresh_audit=failed)
            self.assertNotIn('destroy', self.calls)

    def test_cleanup_only_has_no_registry_artifact_runtime_or_ssh_dependency(self):
        self.resumable(state='cleanup_failed')
        result = self.run_operator(cleanup_only=True, artifacts=None, provision=None,
                                   fresh_audit=None, prepare_runtime=None, support_image_access=None, stage=None)
        self.assertEqual(self.calls, ['destroy'])
        self.assertEqual(result.exit_code, 0)
        self.assertFalse(result.support_stage_complete)
        self.assertTrue(result.cleanup_complete)

    def test_teardown_resume_and_foreign_or_public_state_refuse_without_any_callbacks(self):
        self.resumable(state='cleanup_pending')
        with self.assertRaises(operator.CandidateOperatorRefusal):
            self.run_operator(resume=True)
        self.assertEqual(self.calls, [])
        self.job.pop(operator.OPERATOR_JOURNAL_KEY)
        self.job['resources'].clear()
        self.job['_persist_state'] = True
        with self.assertRaises(operator.CandidateOperatorRefusal):
            self.run_operator()
        self.assertEqual(self.calls, [])

    def test_boolean_or_local_status_is_not_independent_cleanup_proof(self):
        result = self.run_operator(destroy_and_prove=lambda job, lease: True)
        self.assertTrue(result.support_stage_complete)
        self.assertFalse(result.cleanup_complete)
        self.assertEqual(result.exit_code, 2)

    def test_cleanup_proof_does_not_promote_global_runtime_qualification(self):
        def promoted(job, lease):
            value = self.destroy(job, lease)
            value['runtime_qualified'] = True
            return value
        result = self.run_operator(destroy_and_prove=promoted)
        self.assertEqual(result.exit_code, 2)
        self.assertFalse(result.cleanup_complete)

    def test_cancel_and_repeated_cleanup_interruption_preserve_support_scope(self):
        def cancel(job, lease):
            raise SimulatedCancellation('operator cancelled')
        attempts = []
        def interrupted_cleanup(job, lease):
            attempts.append(1)
            if len(attempts) == 1:
                raise KeyboardInterrupt('first cleanup interruption')
            return self.destroy(job, lease)
        result = self.run_operator(stage=cancel, destroy_and_prove=interrupted_cleanup)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(result.exit_code, 130)
        self.assertTrue(result.cleanup_complete)

    def test_real_repeated_cleanup_signals_are_deferred_until_proof_finishes(self):
        def signalled(job, lease):
            os.kill(os.getpid(), signal.SIGINT)
            os.kill(os.getpid(), signal.SIGTERM)
            return self.destroy(job, lease)
        result = self.run_operator(destroy_and_prove=signalled)
        self.assertEqual(result.exit_code, 128 + signal.SIGINT)
        self.assertTrue(result.cleanup_complete)
        self.assertIs(self.job[operator.OPERATOR_JOURNAL_KEY]['failure']['interrupted'], True)

    def test_signal_during_final_persistence_is_recorded_while_still_deferred(self):
        injected = []
        def persist(job, lease):
            if job[operator.OPERATOR_JOURNAL_KEY]['state'] == 'cleanup_complete' and not injected:
                injected.append(True)
                os.kill(os.getpid(), signal.SIGTERM)
            self.persist(job, lease)
        result = self.run_operator(persist=persist)
        self.assertEqual(result.exit_code, 128 + signal.SIGTERM)
        self.assertTrue(result.cleanup_complete)
        self.assertIs(self.job[operator.OPERATOR_JOURNAL_KEY]['failure']['interrupted'], True)

    def test_unknown_resource_keys_are_not_removed_to_make_cleanup_pass(self):
        def stage(job, lease):
            value = self.stage(job, lease)
            job['resources']['unknown_foreign_resource'] = {'id': 'do-not-delete'}
            return value
        def refuse(job, lease):
            self.assertEqual(job['resources']['unknown_foreign_resource'], {'id': 'do-not-delete'})
            raise RuntimeError('foreign key prevents terminal predicate')
        result = self.run_operator(stage=stage, destroy_and_prove=refuse)
        self.assertFalse(result.cleanup_complete)
        self.assertIn('unknown_foreign_resource', self.job['resources'])

    def test_archive_persistence_failure_retains_native_stage_key_and_still_attempts_destroy(self):
        called = []
        def fail_archive(job, lease):
            if operator.STAGE_ARCHIVE_KEY in job:
                raise OSError('archive was not durable')
            self.persist(job, lease)
        def cleanup(job, lease):
            called.append(1)
            self.assertIn(SUPPORT_STAGE_JOURNAL_KEY, job['resources'])
            raise RuntimeError('cleanup attempted with evidence retained')
        result = self.run_operator(persist=fail_archive, destroy_and_prove=cleanup)
        self.assertTrue(called)
        self.assertFalse(result.cleanup_complete)
        self.assertIn(SUPPORT_STAGE_JOURNAL_KEY, self.job['resources'])

    def test_lease_loss_blocks_further_callbacks_and_never_claims_cleanup(self):
        def runtime(job, lease):
            self.calls.append('runtime-released-lease')
            lease.release()
        result = self.run_operator(prepare_runtime=runtime)
        self.assertEqual(result.exit_code, 2)
        self.assertNotIn('stage', self.calls)
        self.assertNotIn('destroy', self.calls)

    def test_initial_intent_persistence_failure_precedes_all_cloud_callbacks(self):
        def unavailable(job, lease):
            raise OSError('initial private state not durable')
        with self.assertRaises(OSError):
            self.run_operator(persist=unavailable)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.job['resources'], {})

    def test_native_ready_validator_failure_cannot_become_operator_success(self):
        with patch.object(operator, '_validated_support', side_effect=ValueError('native stage identity drift')) as validate:
            result = self.run_operator()
        validate.assert_called_once()
        self.assertFalse(result.support_stage_complete)
        self.assertEqual(result.exit_code, 1)
        self.assertTrue(result.cleanup_complete)

    def test_archive_collision_preserves_both_existing_archive_and_native_key(self):
        original = {'unvalidated_existing_history': True}
        self.job[operator.STAGE_ARCHIVE_KEY] = copy.deepcopy(original)
        def refuse(job, lease):
            self.calls.append('destroy-with-history-retained')
            self.assertIn(SUPPORT_STAGE_JOURNAL_KEY, job['resources'])
            raise RuntimeError('terminal gate requires exact stage archival')
        result = self.run_operator(destroy_and_prove=refuse)
        self.assertFalse(result.cleanup_complete)
        self.assertEqual(self.job[operator.STAGE_ARCHIVE_KEY], original)
        self.assertIn(SUPPORT_STAGE_JOURNAL_KEY, self.job['resources'])
        self.assertIn('destroy-with-history-retained', self.calls)

    def test_cleanup_only_preserves_original_stage_failure_without_replaying_it(self):
        self.resumable(state='cleanup_failed')
        original = {'type': 'PreviousStageFailure', 'message': 'original interruption', 'interrupted': True}
        self.job[operator.OPERATOR_JOURNAL_KEY]['failure'] = copy.deepcopy(original)
        result = self.run_operator(cleanup_only=True, artifacts=None)
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(self.calls, ['destroy'])
        self.assertEqual(self.job[operator.OPERATOR_JOURNAL_KEY]['failure'], original)

    def test_crossed_access_scope_and_promoted_audit_flags_never_reach_stage(self):
        for mode in ('access', 'audit'):
            self.job.pop(operator.OPERATOR_JOURNAL_KEY, None)
            self.job.pop(operator.STAGE_ARCHIVE_KEY, None)
            self.job['resources'].clear()
            self.calls.clear()
            if mode == 'access':
                def changed(job, lease):
                    value = self.access(job, lease)
                    value['anonymous_layer_pull_qualified'] = True
                    return value
                options = {'support_image_access': changed}
            else:
                def changed(job, lease):
                    value = self.audit(job, lease)
                    value['runtime_qualified'] = True
                    return value
                options = {'fresh_audit': changed}
            result = self.run_operator(**options)
            self.assertFalse(result.support_stage_complete)
            self.assertNotIn('stage', self.calls)
            self.assertIn('destroy', self.calls)


if __name__ == '__main__':
    unittest.main()
