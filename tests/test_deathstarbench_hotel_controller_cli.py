"""Private Hotel CLI admission and durable state, without live cloud or SSH."""

from contextlib import ExitStack, redirect_stdout, redirect_stderr
import copy
import hashlib
import importlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from types import SimpleNamespace

from app import deathstarbench_candidate_operator as operator
from scripts import qualify_deathstarbench_candidate_support as support
from scripts.deathstarbench_candidate_operator_state import CandidateStateError, CandidateStateStore
from tests.test_deathstarbench_candidate_operator_cli import plan_values


class HotelInitializationCliTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.store = CandidateStateStore(root / '.candidate-support')

    def cli(self):
        name = 'scripts.qualify_deathstarbench_hotel_initialization'
        self.assertIsNotNone(importlib.util.find_spec(name), 'Private Hotel initialization CLI is required')
        return importlib.import_module(name)

    def test_only_new_or_cleanup_modes_and_hotel_are_admitted(self):
        cli = self.cli()
        for arguments in (['--resume', '0123456789ab'], ['--workload', 'media_microservices']):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                cli._parser().parse_args(arguments)

    def test_cleanup_rejects_guarded_input_overrides_before_reading_state(self):
        cli = self.cli()
        for flag in ('--guarded-image-lock', '--guarded-publication', '--guarded-preparation', '--upstream'):
            args = cli._parser().parse_args(['--cleanup-only', '0123456789ab', flag, '/missing'])
            with self.assertRaises(ValueError), mock.patch.object(self.store, 'acquire', side_effect=AssertionError('state acquired')):
                cli._run(args, store=self.store)

    def test_new_requires_complete_artifacts_before_state_or_cloud(self):
        cli = self.cli()
        args = cli._parser().parse_args(['--plan', '/missing'])
        with self.assertRaises(ValueError), mock.patch.object(self.store, 'allocate', side_effect=AssertionError('allocated')):
            cli._run(args, store=self.store)

    def saved_run(self):
        from tests import test_deathstarbench_candidate_artifacts as fixtures
        from tests.test_azure_provider import PUBLIC_KEY
        fixture = fixtures.CandidateArtifactAdoptionTests()
        self.addCleanup(fixture.doCleanups)
        binding = fixture.adopt(fixture.fixture())
        ssh = {'private_key': 'PRIVATE-TEST', 'public_key': PUBLIC_KEY, 'passphrase': None}
        plan = support._infrastructure_plan(plan_values(), ssh)
        run_id, lease = self.store.allocate()
        pin = support._new_pin(run_id, plan, {'schema_version': 1, 'provider': 'azure'}, binding, ssh)
        job = {'id': run_id, 'plan': pin['infrastructure_plan'], 'candidate_support_pin': pin,
               'resources': {}, 'results': [], 'events': [], 'status': 'queued'}
        job[operator.OPERATOR_JOURNAL_KEY] = operator._new_record(pin)
        job[operator.OPERATOR_JOURNAL_KEY]['state'] = 'cleanup_failed'
        self.store.publish_pin(pin, lease)
        self.store.save(job, lease)
        lease.release()
        return run_id

    def test_cleanup_has_no_source_registry_ssh_or_controller_dependency(self):
        cli = self.cli()
        run_id = self.saved_run()
        def destroy(job, lease):
            self.store.require_lease(run_id, lease)
            self.assertNotIn('_key', job)
            return {'schema_version': 1, 'candidate_schema': operator.CLEANUP_PROOF_SCHEMA,
                    'candidate_only': True, 'run_id': run_id, 'provider': 'azure',
                    'cleanup_complete': True, 'independent_absence_proved': True}
        cloud = mock.Mock(destroy_and_prove=destroy)
        with mock.patch.object(support, 'CandidateCloud', return_value=cloud), \
                mock.patch.object(support, '_adopt', side_effect=AssertionError('registry')), \
                mock.patch.object(support.shared, '_ssh_material', side_effect=AssertionError('SSH material')), \
                mock.patch.object(support.application, 'ssh', side_effect=AssertionError('SSH execution')), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(cli._run(cli._parser().parse_args(['--cleanup-only', run_id]), store=self.store), 0)
        with self.store.acquire(run_id) as lease:
            saved = self.store.load(run_id, lease)
        self.assertTrue(saved[operator.OPERATOR_JOURNAL_KEY]['cleanup_complete'])
        self.assertFalse(saved[operator.OPERATOR_JOURNAL_KEY]['scope']['public_dispatch_enabled'])
        self.assertEqual(saved['results'], [])

    def test_immutable_initialization_pin_is_enforced_on_load_and_cannot_be_replaced(self):
        run_id = self.saved_run()
        with self.store.acquire(run_id) as lease:
            job = self.store.load(run_id, lease)
            pin = {'run_id': run_id, 'initialization_attempt_id': '12345678-1234-4234-8234-123456789abc'}
            self.store.publish_hotel_initialization_pin(pin, lease)
            job['guarded_hotel_initialization_pin'] = copy.deepcopy(pin)
            self.store.save(job, lease)
            original = (self.store.directory(run_id) / 'state.json').read_bytes()
            job['guarded_hotel_initialization_pin']['initialization_attempt_id'] = 'changed'
            self.store.save(job, lease)  # Evidence persistence is not execution authority.
            with self.assertRaises(CandidateStateError):
                self.store.load(run_id, lease)
            (self.store.directory(run_id) / 'state.json').write_bytes(original)
            with self.assertRaises(CandidateStateError):
                self.store.publish_hotel_initialization_pin(pin, lease)
            (self.store.directory(run_id) / 'hotel-initialization-pin.json').unlink()
            with self.assertRaises(CandidateStateError):
                self.store.load(run_id, lease)

    def test_legacy_initialization_records_are_refused_without_migration(self):
        run_id = self.saved_run()
        directory = self.store.directory(run_id)
        path = directory / 'state.json'
        saved = json.loads(path.read_text())
        pin = {'run_id': run_id, 'initialization_attempt_id': '12345678-1234-4234-8234-123456789abc'}
        saved['hotel_initialization_pin'] = pin
        raw = (json.dumps(saved, sort_keys=True) + '\n').encode()
        path.write_bytes(raw)
        with self.store.acquire(run_id) as lease:
            for pin_file_exists in (False, True):
                if pin_file_exists:
                    self.store.publish_hotel_initialization_pin(pin, lease)
                with self.subTest(pin_file_exists=pin_file_exists), \
                        self.assertRaisesRegex(CandidateStateError, 'Legacy Hotel initialization'):
                    self.store.load(run_id, lease)
                self.assertEqual(path.read_bytes(), raw)

    def test_exact_guarded_input_bytes_are_saved_with_original_immutability_rules(self):
        run_id = self.saved_run()
        source = self.store.root.parent / 'guarded.json'
        raw = b'{ "guarded" : true }\n\n'
        source.write_bytes(raw)
        with self.store.acquire(run_id) as lease:
            for filename in ('guarded-image-lock.json', 'guarded-publication.json'):
                saved = self.store.publish_artifact_input(run_id, filename, source, hashlib.sha256(raw).hexdigest(), lease)
                self.assertEqual(saved.read_bytes(), raw)
            source.write_bytes(b'{}\n')
            with self.assertRaises(CandidateStateError):
                self.store.publish_artifact_input(run_id, 'guarded-image-lock.json', source, hashlib.sha256(raw).hexdigest(), lease)

    def test_existing_support_resume_refuses_a_guarded_attempt_without_mutating_it(self):
        run_id = self.saved_run()
        with self.store.acquire(run_id) as lease:
            job = self.store.load(run_id, lease)
            pin = {'run_id': run_id, 'initialization_attempt_id': '12345678-1234-4234-8234-123456789abc'}
            self.store.publish_hotel_initialization_pin(pin, lease)
            job['guarded_hotel_initialization_pin'] = pin
            self.store.save(job, lease)
        before = (self.store.directory(run_id) / 'state.json').read_bytes()
        with mock.patch.object(support.shared, '_ssh_material', side_effect=AssertionError('SSH read')), \
                self.assertRaisesRegex(ValueError, 'cleanup-only'):
            support._run(support._parser().parse_args(['--resume', run_id]), store=self.store)
        self.assertEqual((self.store.directory(run_id) / 'state.json').read_bytes(), before)

    def new_run(self):
        """Use existing synthetic source validators with real artifact adoption."""
        from tests import test_deathstarbench_candidate_artifacts as baseline_fixtures
        from tests import test_deathstarbench_hotel_initialization_artifacts as guarded_fixtures
        from tests.test_azure_provider import PUBLIC_KEY
        from scripts import deathstarbench_candidate_registry as registry
        baseline_fixture = baseline_fixtures.CandidateArtifactAdoptionTests()
        guarded_fixture = guarded_fixtures.GuardedHotelArtifactsTests()
        self.addCleanup(baseline_fixture.doCleanups)
        self.addCleanup(guarded_fixture.doCleanups)
        original = baseline_fixture.fixture()
        guarded = guarded_fixture.fixture()
        guarded_fixture.adopt(guarded)
        self.stack.enter_context(baseline_fixture.prepared_inputs(original['prepared'], original['inputs']))
        self.stack.enter_context(guarded_fixture.evidence(guarded))
        self.stack.enter_context(mock.patch.object(registry, 'inspect_manifest', side_effect=guarded['registry'].inspect))
        self.stack.enter_context(mock.patch.object(registry, 'inspect_blob', side_effect=guarded['registry'].blob))
        self.stack.enter_context(mock.patch.object(support.shared, '_ssh_material', return_value={
            'private_key': 'PRIVATE-TEST-ONLY', 'public_key': PUBLIC_KEY, 'passphrase': 'PASSPHRASE-TEST-ONLY'}))
        plan = self.store.root.parent / 'plan.json'
        plan.write_text(json.dumps(plan_values()))
        args = self.cli()._parser().parse_args([
            '--plan', str(plan), '--image-lock', str(original['lock_path']),
            '--publication', str(original['publication_path']),
            '--workload-preparation', str(original['case'].workload_path),
            '--driver-preparation', str(original['case'].driver_path),
            '--guarded-image-lock', str(guarded['lock_path']),
            '--guarded-publication', str(guarded['publication_path']),
            '--guarded-preparation', str(guarded['preparation']), '--upstream', str(guarded['upstream']),
        ])
        return args

    def test_new_run_persists_exact_inputs_and_attempt_before_cloud_and_keeps_failure_nonzero(self):
        cli = self.cli()
        args = self.new_run()
        observed = {}
        def provision(job, lease):
            self.store.require_lease(job['id'], lease)
            directory = self.store.directory(job['id'])
            saved = self.store.load(job['id'], lease)
            observed.update(saved)
            for filename, source in (
                ('candidate-image-lock.json', args.image_lock), ('publication.json', args.publication),
                ('guarded-image-lock.json', args.guarded_image_lock), ('guarded-publication.json', args.guarded_publication),
            ):
                self.assertEqual((directory / filename).read_bytes(), source.read_bytes())
            pin = json.loads((directory / 'hotel-initialization-pin.json').read_text())
            self.assertEqual(pin, saved['guarded_hotel_initialization_pin'])
            self.assertRegex(pin['initialization_attempt_id'], r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
            self.assertFalse(pin['application_start_allowed'])
            raise RuntimeError('Synthetic cloud provisioning failed')
        def destroy(job, lease):
            return {'schema_version': 1, 'candidate_schema': operator.CLEANUP_PROOF_SCHEMA,
                    'candidate_only': True, 'run_id': job['id'], 'provider': 'azure',
                    'cleanup_complete': True, 'independent_absence_proved': True}
        with mock.patch.object(support, 'CandidateCloud', return_value=mock.Mock(
                provision=provision, destroy_and_prove=destroy)), redirect_stdout(io.StringIO()):
            self.assertEqual(cli._run(args, store=self.store), 1)
        self.assertTrue(observed)
        directory = self.store.directory(observed['id'])
        self.assertNotIn('PRIVATE-TEST-ONLY', (directory / 'state.json').read_text())
        self.assertNotIn('PASSPHRASE-TEST-ONLY', (directory / 'state.json').read_text())
        with self.store.acquire(observed['id']) as lease:
            final = self.store.load(observed['id'], lease)
        self.assertTrue(final[operator.OPERATOR_JOURNAL_KEY]['cleanup_complete'])
        self.assertIn('Synthetic cloud provisioning failed', final[operator.OPERATOR_JOURNAL_KEY]['failure']['message'])

    def test_changed_guarded_input_is_refused_before_cloud_construction(self):
        cli = self.cli()
        args = self.new_run()
        args.guarded_image_lock.write_text('{}\n')
        with mock.patch.object(support, 'CandidateCloud', side_effect=AssertionError('cloud constructed')), \
                self.assertRaises(ValueError):
            cli._run(args, store=self.store)
        self.assertEqual(list(self.store.root.iterdir()), [])

    def test_input_change_after_adoption_is_refused_before_provisioning(self):
        from scripts import deathstarbench_candidate_registry as registry
        cli = self.cli()
        args = self.new_run()
        original_inspect = registry.inspect_manifest.side_effect
        guarded_image = json.loads(args.guarded_publication.read_text())['app']['index_image']
        def change_after_read(reference):
            if reference == guarded_image:
                args.guarded_image_lock.write_bytes(args.guarded_image_lock.read_bytes() + b'\n')
            return original_inspect(reference)
        with mock.patch.object(registry, 'inspect_manifest', side_effect=change_after_read), \
                mock.patch.object(support, 'CandidateCloud', side_effect=AssertionError('cloud constructed')), \
                self.assertRaisesRegex(CandidateStateError, 'changed after full adoption'):
            cli._run(args, store=self.store)

    def controller_failure_cleanup(self, failure, expected_exit):
        """Keep CLI, operator, leases and persistence real across stage failure.

        The independently tested support preparation facade supplies synthetic
        ready evidence; the controller call supplies its injected failure. This
        isolates the cleanup-owning composition from remote Kubernetes work.
        """
        from app import deathstarbench_candidate_image_access as image_access
        from app import deathstarbench_hotel_controller as controller
        from scripts import deathstarbench_candidate_operator_state as state
        cli = self.cli()
        args = self.new_run()
        observed = []
        audit_identity = {'synthetic-create-id': 'original'}

        def provision(job, lease):
            self.store.require_lease(job['id'], lease)
            job['resources']['synthetic-cloud-owner'] = copy.deepcopy(audit_identity)
            observed.append('provision')

        def audit(job, lease):
            self.store.require_lease(job['id'], lease)
            return {'schema_version': 1, 'candidate_only': True, 'read_only': True,
                    'run_id': job['id'], 'provider': 'azure', 'cloud_identity': audit_identity}

        def support_access(job, bundle):
            pin = job['candidate_support_pin']
            receipt = {
                'schema_version': 1, 'candidate_schema': 'candidate-support-image-access-v1',
                'scope': 'active-support-images-only-no-runtime-or-application-qualification',
                'candidate_only': True, 'released': False, 'runtime_qualified': False,
                'measurement_qualified': False, 'anonymous_pull_qualified': False,
                'anonymous_layer_pull_qualified': False, 'application_start_allowed': False,
                'anonymous_support_bytes_verified': True, 'support_image_execution_performed': False,
                'identity': {'run_id': job['id'], 'provider': 'azure', 'workload_id': 'hotel_reservation',
                             'artifact_fingerprint': pin['artifact_fingerprint']},
            }
            return SimpleNamespace(metadata=lambda: copy.deepcopy(receipt))

        def stage(job, artifacts, *, lease, execute, persist, emit):
            self.store.require_lease(job['id'], lease)
            ready = {'candidate_only': True, 'released': False,
                     'state': 'support_ready_applications_paused', 'run_id': job['id'],
                     'synthetic_support_evidence': True, 'application_start_allowed': False}
            job['resources'][operator.SUPPORT_STAGE_JOURNAL_KEY] = ready
            persist(job)
            observed.append('support-ready')
            return ready

        def fail_controller(job, artifacts, *, baseline_artifacts, initialization_attempt_id,
                            lease, execute, persist, fresh_audit, checkpoint):
            self.store.require_lease(job['id'], lease)
            self.assertEqual(initialization_attempt_id,
                             job['guarded_hotel_initialization_pin']['initialization_attempt_id'])
            self.assertEqual(baseline_artifacts.fingerprint,
                             job['candidate_support_pin']['artifact_fingerprint'])
            artifacts.__post_init__()
            self.assertEqual(job['resources'][operator.SUPPORT_STAGE_JOURNAL_KEY]['state'],
                             'support_ready_applications_paused')
            observed.append('controller-entered')
            job['guarded_hotel_initialization'] = {
                'state': ('initialized-and-globally-verified-applications-paused' if failure == 'success' else
                          'initializer-intent-recorded' if failure.startswith('pin-checkpoint-') else
                          'injected-persist-failure' if failure == 'persistence' else 'failed-cleanup-only'),
                'run_id': job['id'], 'initialization_attempt_id': initialization_attempt_id,
                'guarded_artifact_fingerprint': artifacts.fingerprint,
                'initialization_executed': True if failure == 'success' else None,
                'database_execution_verified': failure == 'success',
                'global_readonly_verification_completed': failure == 'success',
                'application_start_allowed': False,
                'measurement_qualified': False, 'public_release': False,
            }
            persist(job)
            if failure.startswith('pin-checkpoint-'):
                execute(job, 'SYNTHETIC-HOTEL-GATE-RELEASE')
                self.fail('Gate release continued after Hotel pin damage')
            if failure == 'success':
                return copy.deepcopy(job['guarded_hotel_initialization'])
            if failure == 'interrupt':
                raise KeyboardInterrupt('Injected initialization interruption')
            raise OSError('Injected controller transport failure')

        def destroy(job, lease):
            self.store.require_lease(job['id'], lease)
            self.assertEqual(observed, ['provision', 'support-ready', 'controller-entered'])
            self.assertNotIn(operator.SUPPORT_STAGE_JOURNAL_KEY, job['resources'])
            self.assertTrue(job[operator.STAGE_ARCHIVE_KEY]['payload']['synthetic_support_evidence'])
            self.assertIn('guarded_hotel_initialization', job)
            observed.append('destroy')
            job['resources'].clear()
            return {'schema_version': 1, 'candidate_schema': operator.CLEANUP_PROOF_SCHEMA,
                    'candidate_only': True, 'run_id': job['id'], 'provider': 'azure',
                    'cleanup_complete': True, 'independent_absence_proved': True}

        original_write = state._atomic_write
        failed_write = []
        def write(path, value):
            if (failure == 'persistence' and path.name == 'state.json' and not failed_write
                    and value.get('guarded_hotel_initialization', {}).get('state') == 'injected-persist-failure'):
                failed_write.append(path)
                raise OSError('Injected initialization persistence failure')
            result = original_write(path, value)
            if (failure.startswith('pin-checkpoint-') and path.name == 'state.json' and not failed_write
                    and value.get('guarded_hotel_initialization', {}).get('state') == 'initializer-intent-recorded'):
                failed_write.append(path)
                immutable = path.parent / 'hotel-initialization-pin.json'
                if failure == 'pin-checkpoint-deleted':
                    immutable.unlink()
                else:
                    altered = json.loads(immutable.read_text())
                    altered['initialization_attempt_id'] = '12345678-1234-4234-8234-123456789abc'
                    immutable.write_text(json.dumps(altered))
            return result

        cloud = SimpleNamespace(provision=provision, audit=audit, destroy_and_prove=destroy)
        with mock.patch.object(support, 'CandidateCloud', return_value=cloud), \
                mock.patch.object(support.runtime, 'prepare_distributed_k3s_candidate'), \
                mock.patch.object(support, '_context', side_effect=lambda job, *_args: (None, job, None, None)), \
                mock.patch.object(image_access, 'qualify_candidate_support_image_access', side_effect=support_access), \
                mock.patch.object(image_access, 'require_candidate_support_image_access'), \
                mock.patch.object(support, 'prepare_candidate_support_stage', side_effect=stage), \
                mock.patch.object(operator, '_validated_support', side_effect=lambda job, artifacts, value, access: copy.deepcopy(value)), \
                mock.patch.object(controller, 'run_guarded_hotel_initialization', side_effect=fail_controller), \
                mock.patch.object(state, '_atomic_write', side_effect=write), \
                mock.patch.object(support.application, 'ssh', side_effect=AssertionError('unexpected gate release')), \
                redirect_stdout(io.StringIO()) as stdout:
            self.assertEqual(cli._run(args, store=self.store), expected_exit)
        self.assertEqual(observed, ['provision', 'support-ready', 'controller-entered', 'destroy'])
        if failure == 'persistence':
            self.assertEqual(len(failed_write), 1)
        run_directory, = self.store.root.iterdir()
        with self.store.acquire(run_directory.name) as lease:
            if failure.startswith('pin-checkpoint-'):
                with self.assertRaises(CandidateStateError):
                    self.store.load(run_directory.name, lease)
                saved = json.loads((run_directory / 'state.json').read_text())
            else:
                saved = self.store.load(run_directory.name, lease)
        journal = saved[operator.OPERATOR_JOURNAL_KEY]
        self.assertTrue(journal['cleanup_complete'])
        self.assertEqual(journal['support_stage_complete'], failure == 'success')
        if failure == 'success':
            self.assertIsNone(journal['failure'])
        else:
            self.assertEqual(journal['failure']['type'],
                             'KeyboardInterrupt' if failure == 'interrupt' else
                             'CandidateOperatorRefusal' if failure.startswith('pin-checkpoint-') else 'OSError')
            self.assertEqual(journal['failure']['interrupted'], failure == 'interrupt')
        self.assertIsNone(journal['cleanup_failure'])
        self.assertFalse(saved['guarded_hotel_initialization']['application_start_allowed'])
        self.assertEqual(saved['results'], [])
        self.assertEqual(journal['scope']['scope'], 'support-stage-and-guarded-hotel-initialization-applications-paused')
        self.assertNotIn('operator_initialized_dataset', journal['scope'])
        self.assertEqual(journal['scope']['dataset_execution_evidence_key'], 'guarded_hotel_initialization')
        self.assertTrue(stdout.getvalue().startswith('Private guarded Hotel initialization candidate '))
        outcome = json.loads(stdout.getvalue().split('\n', 1)[1])
        self.assertEqual(outcome['operation_scope'], journal['scope'])
        self.assertEqual(outcome['hotel_initialization']['completion'],
                         'completed' if failure == 'success' else
                         'unknown' if failure == 'persistence' or failure.startswith('pin-checkpoint-') else 'failed')
        self.assertEqual(outcome['hotel_initialization']['initialization_executed'],
                         True if failure == 'success' else None)
        self.assertFalse(outcome['hotel_initialization']['application_start_allowed'])
        self.assertFalse(outcome['hotel_initialization']['measurement_qualified'])
        if failure == 'success':
            self.assertEqual(outcome['support_snapshot']['state'], 'support_ready_applications_paused')
        return run_directory.name, copy.deepcopy(journal['scope'])

    def test_controller_transport_failure_runs_cleanup_and_returns_nonzero(self):
        self.controller_failure_cleanup('transport', 1)

    def test_controller_interrupt_runs_cleanup_and_preserves_interrupt_exit(self):
        self.controller_failure_cleanup('interrupt', 130)

    def test_controller_persistence_failure_runs_cleanup_and_returns_nonzero(self):
        self.controller_failure_cleanup('persistence', 1)

    def test_initializer_intent_pin_deletion_stops_before_gate_release_and_still_cleans(self):
        self.controller_failure_cleanup('pin-checkpoint-deleted', 1)

    def test_initializer_intent_pin_change_stops_before_gate_release_and_still_cleans(self):
        self.controller_failure_cleanup('pin-checkpoint-changed', 1)

    def test_controller_success_reports_initialization_and_retains_support_component_identity(self):
        self.controller_failure_cleanup('success', 0)

    def test_guarded_cleanup_recovers_same_scope_without_runtime_artifacts(self):
        run_id, scope = self.controller_failure_cleanup('success', 0)
        def destroy(job, lease):
            self.store.require_lease(run_id, lease)
            self.assertEqual(job[operator.OPERATOR_JOURNAL_KEY]['scope'], scope)
            return {'schema_version': 1, 'candidate_schema': operator.CLEANUP_PROOF_SCHEMA,
                    'candidate_only': True, 'run_id': run_id, 'provider': 'azure',
                    'cleanup_complete': True, 'independent_absence_proved': True}
        with mock.patch.object(support, 'CandidateCloud', return_value=mock.Mock(destroy_and_prove=destroy)), \
                mock.patch.object(support, '_adopt', side_effect=AssertionError('registry adoption')), \
                mock.patch.object(support.shared, '_ssh_material', side_effect=AssertionError('SSH material')), \
                redirect_stdout(io.StringIO()) as stdout:
            self.assertEqual(self.cli()._run(self.cli()._parser().parse_args(['--cleanup-only', run_id]), store=self.store), 0)
        outcome = json.loads(stdout.getvalue().split('\n', 1)[1])
        self.assertEqual(outcome['operation_scope'], scope)
        self.assertEqual(outcome['hotel_initialization']['completion'], 'completed')

    def test_guarded_scope_requires_matching_immutable_pin_before_any_cleanup_callback(self):
        run_id, _scope = self.controller_failure_cleanup('success', 0)
        with self.store.acquire(run_id) as lease:
            job = self.store.load(run_id, lease)
            job['_persist_state'] = False
            before = (self.store.directory(run_id) / 'state.json').read_bytes()
            for enabled, mutate in ((False, False), (True, True)):
                candidate = copy.deepcopy(job)
                if mutate:
                    candidate['guarded_hotel_initialization_pin']['support_pin_fingerprint'] = 'sha256:' + 'f' * 64
                with self.subTest(enabled=enabled), self.assertRaises(operator.CandidateOperatorRefusal):
                    operator.run_candidate_support_operator(
                        candidate, lease=lease, pin=candidate['candidate_support_pin'], cleanup_only=True,
                        guarded_hotel_initialization=enabled, persist=lambda *_args: self.fail('state mutation'),
                        destroy_and_prove=lambda *_args: self.fail('cloud cleanup before pin validation'),
                    )
            self.assertEqual((self.store.directory(run_id) / 'state.json').read_bytes(), before)

    def test_support_outcome_metadata_remains_unchanged_by_default(self):
        run_id = self.saved_run()
        def destroy(job, lease):
            return {'schema_version': 1, 'candidate_schema': operator.CLEANUP_PROOF_SCHEMA,
                    'candidate_only': True, 'run_id': run_id, 'provider': 'azure',
                    'cleanup_complete': True, 'independent_absence_proved': True}
        with self.store.acquire(run_id) as lease:
            job = self.store.load(run_id, lease)
            job['_persist_state'] = False
            outcome = operator.run_candidate_support_operator(
                job, lease=lease, pin=job['candidate_support_pin'], cleanup_only=True,
                persist=lambda value, held: self.store.save(value, held), destroy_and_prove=destroy,
            )
        self.assertEqual(set(outcome.metadata()), {
            'exit_code', 'support_stage_complete', 'cleanup_complete', 'support_snapshot',
            'cleanup_proof', 'failure', 'cleanup_failure',
        })
        self.assertEqual(job[operator.OPERATOR_JOURNAL_KEY]['scope']['scope'], 'support-stage-only-applications-paused')
        self.assertIs(job[operator.OPERATOR_JOURNAL_KEY]['scope']['operator_initialized_dataset'], False)

    def test_hotel_scope_cannot_be_requested_for_an_unpinned_support_record(self):
        run_id = self.saved_run()
        with self.store.acquire(run_id) as lease:
            job = self.store.load(run_id, lease)
            job['_persist_state'] = False
            with self.assertRaisesRegex(operator.CandidateOperatorRefusal, 'matching immutable initialization pin'):
                operator.run_candidate_support_operator(
                    job, lease=lease, pin=job['candidate_support_pin'], cleanup_only=True,
                    guarded_hotel_initialization=True, persist=lambda *_args: self.fail('state mutation'),
                    destroy_and_prove=lambda *_args: self.fail('cloud callback'),
                )

    def test_hotel_pin_damage_after_provision_stops_execution_and_still_attempts_exact_cleanup(self):
        for damage in ('deleted', 'invalid-json', 'changed'):
            with self.subTest(damage=damage):
                args = self.new_run()
                observed = []
                accepted = {}
                def provision(job, lease):
                    self.store.require_lease(job['id'], lease)
                    accepted.update(copy.deepcopy(job['candidate_support_pin']))
                    job['resources']['synthetic-exact-cloud-id'] = 'owned-instance-1'
                    observed.append('provision')
                    path = self.store.directory(job['id']) / 'hotel-initialization-pin.json'
                    if damage == 'deleted':
                        path.unlink()
                    elif damage == 'invalid-json':
                        path.write_bytes(b'{broken')
                    else:
                        altered = json.loads(path.read_text())
                        altered['initialization_attempt_id'] = '12345678-1234-4234-8234-123456789abc'
                        path.write_text(json.dumps(altered))
                def destroy(job, lease):
                    self.store.require_lease(job['id'], lease)
                    self.assertEqual(job['candidate_support_pin'], accepted)
                    self.assertEqual(job['resources']['synthetic-exact-cloud-id'], 'owned-instance-1')
                    self.assertNotIn('guarded_hotel_initialization', job)
                    observed.append('destroy')
                    return {'schema_version': 1, 'candidate_schema': operator.CLEANUP_PROOF_SCHEMA,
                            'candidate_only': True, 'run_id': job['id'], 'provider': 'azure',
                            'cleanup_complete': True, 'independent_absence_proved': True}
                cloud = SimpleNamespace(provision=provision, destroy_and_prove=destroy,
                                        audit=lambda *_args: self.fail('execution continued after pin damage'))
                with mock.patch.object(support, 'CandidateCloud', return_value=cloud), \
                        mock.patch.object(support.application, 'ssh', side_effect=AssertionError('SSH after pin damage')), \
                        redirect_stdout(io.StringIO()) as stdout:
                    self.assertNotEqual(self.cli()._run(args, store=self.store), 0)
                self.assertEqual(observed, ['provision', 'destroy'])
                outcome = json.loads(stdout.getvalue().split('\n', 1)[1])
                self.assertEqual(outcome['hotel_initialization']['completion'], 'not-started')

    def test_real_cloud_cleanup_persists_original_ownership_and_deletes_after_hotel_pin_damage(self):
        from scripts.deathstarbench_candidate_provider import CandidateCloud, CLEANUP_SOURCE_KEY
        from scripts import deathstarbench_candidate_cloud_audit as audit
        for damage in ('deleted', 'invalid-json', 'changed', 'job-key-deleted'):
            with self.subTest(damage=damage):
                args = self.new_run()
                observed, retained = [], {}
                def cloud_factory(plan, provider_pin, *, persist, emit, oci_config_file):
                    cloud = CandidateCloud(plan, provider_pin, persist=persist, emit=emit)
                    def provision(job, lease):
                        self.store.require_lease(job['id'], lease)
                        group = 'owned-original-group-' + job['id']
                        job['resources']['azure_resource_group_name'] = group
                        retained.update(run_id=job['id'], group=group,
                                        support_pin=copy.deepcopy(job['candidate_support_pin']))
                        path = self.store.directory(job['id']) / 'hotel-initialization-pin.json'
                        if damage == 'deleted':
                            path.unlink()
                        elif damage == 'invalid-json':
                            path.write_bytes(b'{broken')
                        elif damage == 'changed':
                            value = json.loads(path.read_text())
                            value['initialization_attempt_id'] = '12345678-1234-4234-8234-123456789abc'
                            path.write_text(json.dumps(value))
                        else:
                            del job['guarded_hotel_initialization_pin']
                        retained['damaged_bytes'] = path.read_bytes() if path.exists() else None
                        observed.append('provision')
                    cloud.provision = provision
                    cloud.audit = lambda *_args: self.fail('execution after pin damage')
                    return cloud
                def sdk_delete(job, *, persist, emit):
                    # The real cloud facade must save original GET/delete
                    # targets before crossing the destructive SDK boundary.
                    directory = self.store.directory(job['id'])
                    saved = json.loads((directory / 'state.json').read_text())
                    source = saved[CLEANUP_SOURCE_KEY]
                    self.assertEqual(source['resources']['azure_resource_group_name'], retained['group'])
                    self.assertEqual(job['resources']['azure_resource_group_name'], retained['group'])
                    self.assertEqual(job['candidate_support_pin'], retained['support_pin'])
                    self.assertNotIn('guarded_hotel_initialization', job)
                    observed.append('sdk-delete')
                    job['resources'].clear()
                    job['resources'][support.application.DISTRIBUTED_PROVIDER_TERMINAL_KEY] = (
                        support.application.distributed_provider_terminal_marker('azure', job['id']))
                    persist(job)
                def absent(original):
                    self.assertEqual(original['resources']['azure_resource_group_name'], retained['group'])
                    observed.append('absence-audit')
                    return {'independent_absence': True}
                with mock.patch.object(support, 'CandidateCloud', side_effect=cloud_factory), \
                        mock.patch.object(support.application.azure_provider, 'destroy_resources', side_effect=sdk_delete), \
                        mock.patch.object(audit, 'audit_azure_absent', side_effect=absent), \
                        mock.patch.object(support.application, 'ssh', side_effect=AssertionError('SSH after pin damage')), \
                        redirect_stdout(io.StringIO()) as stdout:
                    self.assertEqual(self.cli()._run(args, store=self.store), 1)
                self.assertEqual(observed, ['provision', 'sdk-delete', 'absence-audit'])
                outcome = json.loads(stdout.getvalue().split('\n', 1)[1])
                self.assertTrue(outcome['cleanup_complete'])
                self.assertEqual(outcome['hotel_initialization']['completion'], 'not-started')
                directory = self.store.directory(retained['run_id'])
                immutable = directory / 'hotel-initialization-pin.json'
                self.assertEqual(immutable.read_bytes() if immutable.exists() else None, retained['damaged_bytes'])
                saved = json.loads((directory / 'state.json').read_text())
                self.assertTrue(saved[operator.OPERATOR_JOURNAL_KEY]['cleanup_complete'])


if __name__ == '__main__':
    unittest.main()
