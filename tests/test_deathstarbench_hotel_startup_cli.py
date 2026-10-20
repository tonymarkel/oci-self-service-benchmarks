"""Offline CLI integration with original factories and real private custody."""
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import tempfile
import unittest
from unittest import mock

from app import deathstarbench_candidate_artifacts as baseline_adoption
from app import deathstarbench_hotel_initialization_artifacts as guarded_adoption
from app import deathstarbench_hotel_startup_artifacts as startup_adoption
from app import deathstarbench_hotel_startup_operator as operator
from app.run_lease import require_acquired_run_lease
from scripts import qualify_deathstarbench_hotel_startup as cli
from scripts.deathstarbench_hotel_startup_operator_state import HotelStartupStateStore
from tests import test_deathstarbench_candidate_artifacts as baseline_fixtures
from tests import test_deathstarbench_hotel_initialization_artifacts as guarded_fixtures
from tests import test_deathstarbench_hotel_startup_image_access as startup_fixtures
from tests.test_deathstarbench_candidate_operator_cli import plan_values, PUBLIC_KEY


class StartupCliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.helper = startup_fixtures.StartupHotelImageAccessTests()
        cls.addClassCleanup(cls.helper.doCleanups)
        base_original = baseline_fixtures.CandidateArtifactAdoptionTests.fixture
        guard_original = guarded_fixtures.GuardedHotelArtifactsTests.fixture
        startup_original = cls.helper.fixture
        def baseline_capture(helper, *args, **kwargs):
            data = base_original(helper, *args, **kwargs)
            cls.base_data = data
            return data
        def guard_capture(helper, **kwargs):
            data = guard_original(helper, **kwargs)
            cls.guard_data = {**data, 'publication': copy.deepcopy(data['publication']),
                              'prepared': copy.deepcopy(data['prepared'])}
            return data
        def startup_capture(**kwargs):
            cls.start_data = startup_original(**kwargs)
            return cls.start_data
        with mock.patch.object(baseline_fixtures.CandidateArtifactAdoptionTests, 'fixture', baseline_capture), \
                mock.patch.object(guarded_fixtures.GuardedHotelArtifactsTests, 'fixture', guard_capture), \
                mock.patch.object(cls.helper, 'fixture', startup_capture):
            _binding, cls.registry = cls.helper.case()
        # Both recipes have the same runtime config but distinct source/build
        # identities. Give guarded publication real synthetic layer bodies too.
        start_app = cls.start_data['publication']['app']
        guard_app = cls.guard_data['publication']['app']
        guard_app['index_image'] = start_app['index_image']
        for platform, row in guard_app['platforms'].items():
            row['image'] = start_app['platforms'][platform]['image']
            row['build_receipt']['published_index_image'] = start_app['platforms'][platform]['build_receipt']['published_index_image']
        cls.guard_helper = guarded_fixtures.GuardedHotelArtifactsTests()
        cls.addClassCleanup(cls.guard_helper.doCleanups)
        cls.guard_helper.adopt(cls.guard_data)
        cls.base_helper = baseline_fixtures.CandidateArtifactAdoptionTests()

    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.store = HotelStartupStateStore(self.root / '.candidate-support')
        self.plan = self.root / 'plan.json'
        self.plan.write_text(json.dumps(plan_values()))
        self.output, self.errors = io.StringIO(), io.StringIO()
        self.stack.enter_context(redirect_stdout(self.output))
        self.stack.enter_context(redirect_stderr(self.errors))

    def args(self, *extra):
        case = self.base_data['case']
        values = {'plan': self.plan, 'image-lock': self.base_data['lock_path'],
            'publication': self.base_data['publication_path'], 'workload-preparation': case.workload_path,
            'driver-preparation': case.driver_path, 'guarded-lock': self.guard_data['lock_path'],
            'guarded-publication': self.guard_data['publication_path'], 'guarded-preparation': self.guard_data['preparation'],
            'startup-lock': self.start_data['lock_path'], 'startup-publication': self.start_data['publication_path'],
            'startup-preparation': self.start_data['preparation'], 'upstream': self.start_data['upstream']}
        return cli._parser().parse_args([part for key, value in values.items() for part in ('--' + key, str(value))] + list(extra))

    def boundaries(self):
        from scripts import deathstarbench_candidate_registry as registry
        from app import deathstarbench_hotel_image_access as access
        self.stack.enter_context(self.base_helper.prepared_inputs(self.base_data['prepared'], self.base_data['inputs']))
        self.stack.enter_context(self.guard_helper.evidence(self.guard_data))
        self.stack.enter_context(self.helper.evidence(self.start_data))
        self.stack.enter_context(mock.patch.object(cli, 'validate_guarded_preparation', return_value=self.guard_data['prepared']))
        self.stack.enter_context(mock.patch.object(cli, 'validate_startup_preparation', return_value=self.start_data['prepared']))
        self.stack.enter_context(mock.patch.object(registry, 'inspect_manifest', side_effect=self.start_data['registry'].inspect))
        self.stack.enter_context(mock.patch.object(registry, 'inspect_blob', side_effect=self.start_data['registry'].blob))
        self.stack.enter_context(mock.patch.object(access.urllib.request, 'build_opener', return_value=self.registry))
        self.stack.enter_context(mock.patch.object(cli.shared, '_ssh_material', return_value={
            'private_key': 'OFFLINE-PRIVATE-KEY', 'passphrase': 'OFFLINE-PASSPHRASE', 'public_key': PUBLIC_KEY}))

    def test_fresh_adoption_and_same_baseline_bind_all_artifacts(self):
        # Catches skipping fresh factories, crossing baseline or publishing only
        # normalized inputs. Real outer operator records pre-create failure.
        self.boundaries()
        original_adopt = cli.support._adopt
        original_guarded, original_startup = cli.adopt_guarded_hotel_artifacts, cli.adopt_startup_hotel_artifacts
        observed = {}
        def baseline(*args, **kwargs):
            observed['baseline'] = original_adopt(*args, **kwargs)
            return observed['baseline']
        def guarded(*args, **kwargs):
            self.assertIs(kwargs['baseline'], observed['baseline'])
            return original_guarded(*args, **kwargs)
        def startup(*args, **kwargs):
            self.assertIs(kwargs['baseline'], observed['baseline'])
            return original_startup(*args, **kwargs)
        with mock.patch.object(cli.support, '_adopt', side_effect=baseline), \
                mock.patch.object(cli, 'adopt_guarded_hotel_artifacts', side_effect=guarded), \
                mock.patch.object(cli, 'adopt_startup_hotel_artifacts', side_effect=startup), \
                mock.patch.object(cli.CandidateCloud, 'provision', side_effect=RuntimeError('SIGNED_SECRET')):
            self.assertNotEqual(cli._run(self.args(), store=self.store), 0)
        directory, = self.store.root.iterdir()
        with self.store.acquire(directory.name) as lease:
            saved = self.store.load(directory.name, lease)
            baseline = saved['candidate_support_pin']['artifact_fingerprint']
            self.assertEqual(saved['hotel_initialization_pin']['guarded_artifacts']['baseline']['binding_fingerprint'], baseline)
            self.assertEqual(saved['hotel_startup_pin']['startup_artifacts']['baseline']['binding_fingerprint'], baseline)
            self.assertEqual(saved['hotel_startup_operator']['execution_outcome'], 'failed')
            self.assertEqual(saved['hotel_startup_operator']['cleanup_outcome'], 'not_required')
            for filename, source in (
                ('candidate-image-lock.json', self.base_data['lock_path']), ('publication.json', self.base_data['publication_path']),
                ('guarded-image-lock.json', self.guard_data['lock_path']), ('guarded-publication.json', self.guard_data['publication_path']),
                ('startup-image-lock.json', self.start_data['lock_path']), ('startup-publication.json', self.start_data['publication_path'])):
                self.assertEqual((directory / filename).read_bytes(), source.read_bytes())
            self.assertNotIn('SIGNED_SECRET', (directory / 'state.json').read_text())
            self.assertNotIn('OFFLINE-PRIVATE', (directory / 'state.json').read_text())
            from app.deathstarbench_hotel_startup_image_access import qualify_startup_hotel_image_access
            with self.assertRaises(ValueError):
                qualify_startup_hotel_image_access(saved['hotel_startup_pin']['startup_artifacts'])

    def test_existing_run_cannot_execute(self):
        # Catches adding any old-run execution path before argument validation.
        self.assertNotIn('--resume', cli._parser().format_help())
        args = self.args('--run-id', '123456abcdef')
        with mock.patch.object(cli.shared, '_ssh_material', side_effect=AssertionError('SSH')), \
                mock.patch.object(self.store, 'allocate', side_effect=AssertionError('allocation')), self.assertRaises(ValueError):
            cli._run(args, store=self.store)
        self.assertEqual(list(self.store.root.iterdir()), [])
        for option in ('--resume', '--recover', '--interrupt-after'):
            with self.assertRaises(SystemExit):
                cli._parser().parse_args([option, '123456abcdef'])

    def test_native_create_and_teardown_keep_failed_fresh_execution_failed(self):
        # Catches hashing a stripped SSH comment instead of the exact native
        # contract key, skipping strict adapter selection or clearing failure.
        from types import SimpleNamespace
        from tests.test_aws_hotel_startup_ownership import StrictEC2
        from tests.test_aws_deathstarbench_lifecycle import candidate_plan, PIN, PUBLIC_KEY as AWS_KEY
        from tests.test_aws_provider import FakeSession
        from scripts import deathstarbench_candidate_provider as provider
        self.boundaries()
        self.plan.write_text(json.dumps({**candidate_plan(), 'availability_domain': 'us-east-2a', 'ocpus': 8, 'memory_gb': 32,
                                        'destroy_after_completion': True}))
        image_pin = self.root / 'aws-images.json'
        image_pin.write_text(json.dumps({'schema_version': 1, 'provider': 'aws',
                                        'support_image': PIN, 'application_image': PIN}))
        ec2 = StrictEC2()
        ec2.application_capacity = (8, 32768)
        session = FakeSession(ec2=ec2, sts=SimpleNamespace(get_caller_identity=lambda: {'Account': '123456789012'}))
        lease_seen = []
        def fail_runtime(job, **kwargs):
            held = job['_candidate_ssh_lease']
            self.store.require_lease(job['id'], held)
            lease_seen.append(held)
            raise RuntimeError('SIGNED_SECRET')
        with mock.patch.object(provider.aws, '_session', return_value=session), \
                mock.patch.object(cli.shared, '_ssh_material', return_value={
                    'private_key': 'OFFLINE-PRIVATE-KEY', 'passphrase': None, 'public_key': AWS_KEY + ' allowed-aws-comment'}), \
                mock.patch.object(cli.runtime, 'prepare_distributed_k3s_candidate', side_effect=fail_runtime):
            self.assertEqual(cli._run(self.args('--aws-image-pin', str(image_pin)), store=self.store), 1)
        self.assertEqual(len(lease_seen), 1)
        directory, = self.store.root.iterdir()
        with self.store.acquire(directory.name) as lease:
            saved = self.store.load(directory.name, lease)
            self.assertEqual(saved[operator.OPERATOR_JOURNAL_KEY]['execution_outcome'], 'failed')
            self.assertEqual(saved[operator.OPERATOR_JOURNAL_KEY]['cleanup_outcome'], 'completed')
            self.assertEqual(saved['candidate_support_pin']['ssh_public_key_sha256'],
                             hashlib.sha256((AWS_KEY + ' allowed-aws-comment').encode()).hexdigest())
            self.assertEqual(saved['candidate_support_pin']['provider_pin']['ssh_public_key_sha256'],
                             hashlib.sha256(' '.join(AWS_KEY.split()[:2]).encode()).hexdigest())
            self.assertIsNotNone(self.store.checkpoint(directory.name, lease)['seal_fingerprint'])
        self.assertTrue(any(name == 'terminate_instances' for name, _ in ec2.calls))
        self.assertNotIn('SIGNED_SECRET', (directory / 'state.json').read_text())

    def completed_fresh_boundary(self, *, signal_at_exit=False, signal_during_output=False, audit_fault=None):
        """Real operator/store/native lifecycle; only remote application IO is fake."""
        from types import SimpleNamespace
        from tests.test_aws_hotel_startup_ownership import StrictEC2
        from tests.test_aws_deathstarbench_lifecycle import candidate_plan, PIN, PUBLIC_KEY as AWS_KEY
        from tests.test_aws_provider import FakeSession
        from tests.test_deathstarbench_distributed_runtime import FakeRemoteExecutor
        from tests.test_deathstarbench_hotel_startup_operator import StartupOperatorTests
        from app import deathstarbench_hotel_live_controller as initializer
        from app import deathstarbench_hotel_startup_controller as startup
        from app.deathstarbench_hotel_image_access import require_guarded_hotel_image_access
        from app.deathstarbench_hotel_startup_image_access import require_startup_hotel_image_access
        from scripts import deathstarbench_candidate_provider as provider
        self.boundaries()
        self.plan.write_text(json.dumps({**candidate_plan(), 'availability_domain': 'us-east-2a',
            'ocpus': 8, 'memory_gb': 32, 'destroy_after_completion': True}))
        image_pin = self.root / 'aws-images.json'
        image_pin.write_text(json.dumps({'schema_version': 1, 'provider': 'aws',
                                        'support_image': PIN, 'application_image': PIN}))
        ec2 = StrictEC2()
        ec2.application_capacity = (8, 32768)
        session = FakeSession(ec2=ec2, sts=SimpleNamespace(get_caller_identity=lambda: {'Account': '123456789012'}))
        phase = {'active': False, 'output_active': [], 'output_signal': False, 'outcomes': [],
                 'destroy_calls': 0, 'startup_calls': 0, 'initialize_calls': 0}
        original_handler = signal.getsignal(signal.SIGTERM)
        remote = FakeRemoteExecutor(ca_sha256=hashlib.sha256(b'OFFLINE-CA-NOT-A-REAL-CERTIFICATE').hexdigest())
        def execute(job, command, **kwargs):
            if 'AWS_DSB_WORKLOAD_STORAGE' in command:
                return ('AWS_DSB_WORKLOAD_STORAGE uuid=11111111-2222-3333-4444-555555555555 '
                        'root=/var/lib/deathstarbench/database/mongodb databases=6\n')
            if 'AWS_DSB_DATABASE_VOLUME' in command:
                volume = cli.runtime.distributed_k3s_candidate_plan(job['resources']).database_volume_id
                return (f'AWS_DSB_DATABASE_VOLUME volume_id={volume} '
                    f'device_link=/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{volume.replace("-", "")} '
                    'uuid=11111111-2222-3333-4444-555555555555 '
                    'mount_point=/var/lib/deathstarbench/database filesystem=xfs\n')
            return remote(job, command, **kwargs)
        def support_stage(job, artifacts, **kwargs):
            from tests import test_deathstarbench_candidate_k3s as kubernetes
            helper = StartupOperatorTests()
            helper.calls, helper.baseline = [], artifacts
            context = cli._context(job, artifacts, cli.runtime.distributed_k3s_candidate_plan)[3]
            inventory = kubernetes.live_inventory
            with mock.patch.object(kubernetes, 'live_inventory',
                    side_effect=lambda stage: inventory(stage, context['role_nodes'])):
                return helper.stage(job, kwargs['lease'])
        def initialize(job, artifacts, **kwargs):
            phase['initialize_calls'] += 1
            require_guarded_hotel_image_access(kwargs['image_access'], artifacts)
            self.assertIs(artifacts, phase['guarded'])
            self.assertIs(kwargs['baseline_artifacts'], phase['baseline'])
            self.assertIs(kwargs['lease'], phase['lease'])
            self.assertEqual(kwargs['expected_cloud_identity'], kwargs['fresh_audit'](job, kwargs['lease'])['cloud_identity'])
            self.assertEqual(kwargs['initialization_attempt_id'], job['hotel_initialization_pin']['initialization_attempt_id'])
            value = {'schema_version': 1, 'candidate_schema': 'hotel-guarded-live-initialization-outcome-v1',
                'candidate_only': True, 'released': False, 'run_id': job['id'],
                'initialization_attempt_id': job['hotel_initialization_pin']['initialization_attempt_id'],
                'guarded_artifact_fingerprint': artifacts.fingerprint,
                'state': 'initialized-and-globally-verified-applications-paused',
                **{key: True for key in ('initialization_executed', 'database_execution_verified',
                    'global_readonly_verification_completed', 'applications_paused', 'all_application_replicas_zero')},
                **{key: False for key in ('application_start_allowed', 'runtime_qualified', 'measurement_qualified',
                    'cloud_qualified', 'public_release', 'public_dispatch_enabled')}}
            job['guarded_hotel_initialization'] = value
            return copy.deepcopy(value)
        def start(job, artifacts, **kwargs):
            phase['startup_calls'] += 1
            require_startup_hotel_image_access(kwargs['image_access'], artifacts)
            self.assertIs(artifacts, phase['startup'])
            self.assertIs(kwargs['baseline_artifacts'], phase['baseline'])
            self.assertIs(kwargs['lease'], phase['lease'])
            self.assertIs(kwargs['initialization_plan'].artifacts, phase['guarded'])
            self.assertEqual(kwargs['initialization_stage'].plan, kwargs['initialization_plan'])
            self.assertEqual(kwargs['initialization_plan'].initialization_attempt_id,
                             job['hotel_initialization_pin']['initialization_attempt_id'])
            self.assertEqual(kwargs['startup_attempt_id'], job['hotel_startup_pin']['startup_attempt_id'])
            self.assertEqual(kwargs['expected_cloud_identity'], kwargs['fresh_audit'](job, kwargs['lease'])['cloud_identity'])
            value = {'schema_version': 1, 'candidate_schema': 'hotel-live-startup-outcome-v1',
                'candidate_only': True, 'run_id': job['id'],
                'startup_attempt_id': job['hotel_startup_pin']['startup_attempt_id'],
                'initialization_attempt_id': job['hotel_initialization_pin']['initialization_attempt_id'],
                'state': 'startup-and-service-identities-observed',
                'startup_execution_observed': True, 'service_identity_observed': True,
                **{key: False for key in ('released', 'application_start_allowed', 'application_started',
                    'application_ready', 'startup_executed', 'initializer_authorized', 'runtime_authorized',
                    'startup_authorized', 'runtime_qualified', 'measurement_qualified', 'dataset_qualified',
                    'cloud_qualified', 'public_release', 'public_dispatch_enabled', 'semantic_correctness_observed')}}
            job['hotel_application_startup'] = value
            return copy.deepcopy(value)
        original_operator = cli.run_hotel_startup_operator
        def observe_operator(job, **kwargs):
            self.assertIs(kwargs['lease'], job['_candidate_ssh_lease'])
            require_acquired_run_lease(kwargs['lease'], job['id'])
            phase.update(job=job, lease=kwargs['lease'], baseline=kwargs['artifacts'],
                         guarded=kwargs['guarded_artifacts'], startup=kwargs['startup_artifacts'])
            self.assertIs(type(phase['baseline']), baseline_adoption.CandidateArtifactBinding)
            self.assertIs(type(phase['guarded']), guarded_adoption.GuardedHotelArtifactBinding)
            self.assertIs(type(phase['startup']), startup_adoption.StartupHotelArtifactBinding)
            def trace(name, callback):
                def invoke(*args, **options):
                    try:
                        return callback(*args, **options)
                    except BaseException as exc:
                        phase['callback_failure'] = name
                        raise
                return invoke
            for name in ('provision', 'fresh_audit', 'prepare_runtime', 'support_image_access', 'stage', 'initialize', 'startup'):
                kwargs[name] = trace(name, kwargs[name])
            result = original_operator(job, **kwargs)
            if audit_fault is None:
                self.assertEqual((result.execution_outcome, result.cleanup_outcome, result.exit_code),
                                 ('completed', 'completed', 0), repr((result, phase.get('callback_failure'))))
            phase['outcomes'].append(result)
            return result
        original_destroy = cli.CandidateCloud.destroy_and_prove
        def destroy(cloud, job, lease):
            self.assertIs(lease, phase['lease'])
            phase['destroy_calls'] += 1
            return original_destroy(cloud, job, lease)
        original_signals = cli.support._signals
        @contextmanager
        def terminal_boundary(job):
            with original_signals(job) as contexts:
                phase['active'] = True
                yield contexts
                self.store.require_lease(job['id'], phase['lease'])
                self.assertEqual(len(phase['outcomes']), 1)
                if signal_at_exit:
                    os.kill(os.getpid(), signal.SIGTERM)
            phase['active'] = False
        class FinalOutput(io.StringIO):
            def write(stream, value):
                if '"mode": "fresh"' in value:
                    self.store.require_lease(phase['job']['id'], phase['lease'])
                    phase['output_active'].append(phase['active'])
                    if phase['active'] and signal_during_output:
                        phase['output_signal'] = True
                        os.kill(os.getpid(), signal.SIGTERM)
                    if not phase['active']:
                        self.assertEqual(signal.getsignal(signal.SIGTERM), original_handler)
                return super().write(value)
        output = FinalOutput()
        if audit_fault is not None:
            from app import deathstarbench_hotel_startup_ownership as ownership
            from scripts.deathstarbench_candidate_operator_state import _atomic_write
            from scripts.deathstarbench_hotel_startup_operator_state import CHECKPOINT_FILE
            expected_identity = cli._expected_cloud_identity
            def corrupt_audit(store, run_id, lease):
                checkpoint = store.checkpoint(run_id, lease)
                self.assertTrue(checkpoint['audits'])
                if audit_fault == 'missing':
                    checkpoint['audits'] = []
                else:
                    audit = checkpoint['audits'][-1]
                    audit['cloud_identity']['run_id'] = '000000000000'
                    audit['cloud_identity_sha256'] = ownership._fingerprint(audit['cloud_identity'])
                checkpoint['fingerprint'] = ownership._fingerprint({key: value for key, value in checkpoint.items() if key != 'fingerprint'})
                _atomic_write(store.require_lease(run_id, lease) / CHECKPOINT_FILE, checkpoint)
                return expected_identity(store, run_id, lease)
            self.stack.enter_context(mock.patch.object(cli, '_expected_cloud_identity', side_effect=corrupt_audit))
        with mock.patch.object(provider.aws, '_session', return_value=session), \
                mock.patch.object(cli.shared, '_ssh_material', return_value={
                    'private_key': 'OFFLINE-PRIVATE', 'passphrase': None, 'public_key': AWS_KEY}), \
                mock.patch.object(cli.application, 'ssh', side_effect=execute), \
                mock.patch.object(cli, 'prepare_candidate_support_stage', side_effect=support_stage), \
                mock.patch.object(initializer, 'prepare_guarded_hotel_initialization', side_effect=initialize), \
                mock.patch.object(startup, 'prepare_startup_hotel_applications', side_effect=start), \
                mock.patch.object(cli, 'run_hotel_startup_operator', side_effect=observe_operator), \
                mock.patch.object(cli.CandidateCloud, 'destroy_and_prove', destroy), \
                mock.patch.object(cli.support, '_signals', terminal_boundary), redirect_stdout(output):
            result = cli._run(self.args('--aws-image-pin', str(image_pin)), store=self.store)
        self.assertEqual((phase['destroy_calls'], phase['initialize_calls'], phase['startup_calls']),
                         (1, 1, 1) if audit_fault is None else (1, 0, 0))
        diagnostics, = [json.loads(line) for line in output.getvalue().splitlines() if line.startswith('{')]
        return result, diagnostics, phase

    def test_fresh_late_signal_after_completed_operator_fails_final_diagnostics(self):
        # Catches ignoring a real signal captured after genuine completed
        # execution/cleanup but before the outer handler context is restored.
        result, diagnostics, phase = self.completed_fresh_boundary(signal_at_exit=True)
        self.assertEqual(result, 143)
        self.assertEqual(diagnostics['execution_outcome'], 'failed')
        self.assertEqual(diagnostics['cleanup_outcome'], 'completed')
        self.assertIs(diagnostics['error']['interrupted'], True)
        self.assertEqual(diagnostics['error']['code'], 'execution-interrupted')
        self.assertEqual(phase['output_active'], [False])

    def test_fresh_final_output_is_after_signal_handler_restoration(self):
        # Catches emitting authoritative success while a captured/deferred
        # signal can still be silently observed during that output operation.
        result, diagnostics, phase = self.completed_fresh_boundary(signal_during_output=True)
        self.assertEqual(phase['output_active'], [False])
        self.assertFalse(phase['output_signal'])
        self.assertEqual((result, diagnostics['execution_outcome']), (0, 'completed'))

    def test_fresh_late_signal_preserves_cleanup_failure_exit_and_error_codes(self):
        # Narrow CLI outcome-policy case: a late signal must mark interruption
        # without replacing prior static errors or cleanup failure's exit 2.
        self.boundaries()
        failed = operator.HotelStartupOperatorOutcome('failed', 'failed',
            {'code': 'evidence-publication-failed', 'interrupted': False},
            {'code': 'cleanup-failed', 'interrupted': False}, 2)
        def failed_operator(job, **kwargs):
            with kwargs['cleanup_signal_context']():
                pass
            return failed
        original_signals = cli.support._signals
        @contextmanager
        def signal_at_exit(job):
            with original_signals(job) as contexts:
                yield contexts
                os.kill(os.getpid(), signal.SIGINT)
        with mock.patch.object(cli, 'run_hotel_startup_operator', side_effect=failed_operator), \
                mock.patch.object(cli.support, '_signals', signal_at_exit):
            self.assertEqual(cli._run(self.args(), store=self.store), 2)
        diagnostics, = [json.loads(line) for line in self.output.getvalue().splitlines() if line.startswith('{')]
        self.assertEqual(diagnostics['error'], {'code': 'evidence-publication-failed', 'interrupted': True})
        self.assertEqual(diagnostics['cleanup_error'], {'code': 'cleanup-failed', 'interrupted': False})
        self.assertEqual((diagnostics['execution_outcome'], diagnostics['cleanup_outcome'], diagnostics['exit_code']),
                         ('failed', 'failed', 2))

    def test_missing_retained_audit_refuses_before_lower_dispatch(self):
        # Catches treating missing durable original identity as optional or
        # falling back to a journal/saved completion before lower execution.
        result, diagnostics, _ = self.completed_fresh_boundary(audit_fault='missing')
        self.assertNotEqual(result, 0)
        self.assertEqual(diagnostics['execution_outcome'], 'failed')

    def test_crossed_retained_audit_refuses_before_lower_dispatch(self):
        # Catches trusting a rehashed foreign audit instead of native original
        # corroboration on the actual checkpoint read.
        result, diagnostics, _ = self.completed_fresh_boundary(audit_fault='crossed')
        self.assertNotEqual(result, 0)
        self.assertEqual(diagnostics['execution_outcome'], 'failed')

    def test_input_change_after_original_adoption_refuses_before_provider(self):
        # Catches deferring exact input custody until after a create gate.
        self.boundaries()
        path = self.start_data['publication_path']
        original = path.read_bytes()
        real_pin = cli.make_hotel_startup_pin
        def change(*args, **kwargs):
            pin = real_pin(*args, **kwargs)
            path.write_bytes(original + b' ')
            return pin
        try:
            with mock.patch.object(cli, 'make_hotel_startup_pin', side_effect=change), \
                    mock.patch.object(cli.CandidateCloud, 'provision', side_effect=AssertionError('create')), self.assertRaises(ValueError):
                cli._run(self.args(), store=self.store)
        finally:
            path.write_bytes(original)
        directory, = self.store.root.iterdir()
        with self.store.acquire(directory.name):
            self.assertFalse((directory / 'state.json').exists())

    def test_changed_native_key_comment_or_pin_refuses_before_create(self):
        # Catches accepting native public-key bytes changed after pinning or
        # trusting a live replacement pin instead of the published original.
        from types import SimpleNamespace
        from tests.test_aws_hotel_startup_ownership import StrictEC2
        from tests.test_aws_deathstarbench_lifecycle import candidate_plan, PIN, PUBLIC_KEY as AWS_KEY
        from tests.test_aws_provider import FakeSession
        from scripts import deathstarbench_candidate_provider as provider
        self.boundaries()
        self.plan.write_text(json.dumps({**candidate_plan(), 'availability_domain': 'us-east-2a',
            'ocpus': 8, 'memory_gb': 32, 'destroy_after_completion': True}))
        image_pin = self.root / 'aws-images.json'
        image_pin.write_text(json.dumps({'schema_version': 1, 'provider': 'aws',
                                        'support_image': PIN, 'application_image': PIN}))
        original = cli.CandidateCloud.provision
        for change in ('comment', 'pin'):
            ec2 = StrictEC2()
            ec2.application_capacity = (8, 32768)
            session = FakeSession(ec2=ec2, sts=SimpleNamespace(get_caller_identity=lambda: {'Account': '123456789012'}))
            def crossed(cloud, job, lease):
                if change == 'comment':
                    job['_public_key'] += ' changed-after-pin'
                else:
                    job['candidate_support_pin']['ssh_public_key_sha256'] = '0' * 64
                return original(cloud, job, lease)
            with self.subTest(change=change), mock.patch.object(provider.aws, '_session', return_value=session), \
                    mock.patch.object(cli.shared, '_ssh_material', return_value={
                        'private_key': 'OFFLINE-PRIVATE', 'passphrase': None, 'public_key': AWS_KEY}), \
                    mock.patch.object(cli.CandidateCloud, 'provision', crossed):
                self.assertNotEqual(cli._run(self.args('--aws-image-pin', str(image_pin)), store=self.store), 0)
            self.assertFalse(any(name.startswith(('create_', 'run_instances', 'import_key_pair')) for name, _ in ec2.calls))

    def test_malformed_modes_refuse_before_external_side_effects(self):
        # Catches processing transport/SSH/source before rejecting mixed modes.
        cases = [[], ['--destroy-only'], ['--run-id', '123456abcdef'],
                 ['--destroy-only', '--run-id', '../bad']]
        for field in (*cli._ARTIFACT_OPTIONS, *cli._PROVIDER_OPTIONS, 'plan', 'ssh_env_file', 'registry_egress'):
            cases.append(['--destroy-only', '--run-id', '123456abcdef', '--' + field.replace('_', '-'),
                          'x86_64' if field == 'oci_application_architecture' else 'forbidden'])
        for values in cases:
            with self.subTest(values=values), \
                    mock.patch.object(cli, '_run_private', side_effect=AssertionError('side effect')), self.assertRaises(ValueError):
                cli._run(cli._parser().parse_args(values), store=self.store)

    def test_source_preflight_refuses_before_registry_ssh_or_allocation(self):
        # Catches trusting retained preparation summaries before fresh source.
        self.boundaries()
        for name in ('validate_guarded_preparation', 'validate_startup_preparation'):
            with self.subTest(name=name), mock.patch.object(cli, name, side_effect=ValueError('bad source')), \
                    mock.patch.object(cli.shared, '_ssh_material', side_effect=AssertionError('SSH')), \
                    mock.patch.object(cli.support, '_adopt', side_effect=AssertionError('registry')), self.assertRaises(ValueError):
                cli._run(self.args(), store=self.store)
        self.assertEqual(list(self.store.root.iterdir()), [])

    def test_malformed_original_locks_refuse_before_allocation(self):
        # Catches bypassing any original adopter with merely saved metadata.
        self.boundaries()
        for data in (self.base_data, self.guard_data, self.start_data):
            path = data['lock_path']
            original = path.read_bytes()
            try:
                path.write_bytes(b'{"schema_version":1,"schema_version":1}')
                with self.subTest(path=path.name), \
                        mock.patch.object(self.store, 'allocate', side_effect=AssertionError('allocation')), self.assertRaises(ValueError):
                    cli._run(self.args(), store=self.store)
            finally:
                path.write_bytes(original)
        self.assertEqual(list(self.store.root.iterdir()), [])

    def test_actual_lower_callback_handoff_uses_original_capabilities_and_lease(self):
        # Catches nesting cleanup owner, wrong callback signature, crossed
        # attempt UUIDs/baseline or replacing a capability with saved metadata.
        result, diagnostics, phase = self.completed_fresh_boundary()
        self.assertEqual((result, diagnostics['execution_outcome'], diagnostics['cleanup_outcome']),
                         (0, 'completed', 'completed'))
        self.assertEqual(len(phase['outcomes']), 1)

    def destroy_fixture(self):
        from tests.test_deathstarbench_hotel_candidate_provider import StartupCandidateProviderTests
        helper = StartupCandidateProviderTests()
        helper.setUpClass()
        self.addCleanup(helper.doClassCleanups)
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        helper.cloud.provision(helper.job, helper.lease)
        helper.job[operator.OPERATOR_JOURNAL_KEY] = {
            'schema_version': 1, 'candidate_schema': operator.OPERATOR_SCHEMA,
            'candidate_only': True, 'released': False, 'run_id': helper.job['id'],
            'pin_fingerprint': helper.job['hotel_startup_pin']['fingerprint'],
            'initialization_attempt_id': helper.job['hotel_initialization_pin']['initialization_attempt_id'],
            'startup_attempt_id': helper.job['hotel_startup_pin']['startup_attempt_id'],
            'state': 'cleanup_failed', 'execution_outcome': 'failed', 'cleanup_outcome': 'failed',
            'error': {'code': 'execution-failed', 'interrupted': False}, 'cleanup_error': None}
        helper.store.save(helper.job, helper.lease)
        helper.lease.release()
        return helper

    def test_destroy_only_has_zero_registry_ssh_kubernetes_calls(self):
        # Catches requiring history, source or executable capabilities to delete.
        helper = self.destroy_fixture()
        directory = helper.store.directory(helper.job['id'])
        (directory / 'state.json').write_bytes(b'corrupt execution history')
        from app import deathstarbench_hotel_startup_controller as controller
        with mock.patch('urllib.request.build_opener', side_effect=AssertionError('registry')) as registry, \
                mock.patch.object(cli.support, '_adopt', side_effect=AssertionError('adoption')), \
                mock.patch.object(cli.shared, '_ssh_material', side_effect=AssertionError('SSH material')), \
                mock.patch.object(cli.application, 'ssh', side_effect=AssertionError('SSH')) as ssh, \
                mock.patch.object(controller, 'prepare_startup_hotel_applications', side_effect=AssertionError('Kubernetes')) as startup, \
                mock.patch.object(cli, 'run_hotel_startup_operator', side_effect=AssertionError('execution')):
            result = cli._run(cli._parser().parse_args(['--destroy-only', '--run-id', helper.job['id']]), store=helper.store)
        self.assertEqual((registry.call_count, ssh.call_count, startup.call_count), (0, 0, 0))
        self.assertEqual(result, 0)
        self.assertEqual((directory / 'state.json').read_bytes(), b'corrupt execution history')
        self.assertIn('"execution_outcome": "not-evaluated"', self.output.getvalue())

    def test_destroy_preserves_failed_execution(self):
        # Catches promoting a failed startup after successful strict teardown.
        helper = self.destroy_fixture()
        history = helper.store.directory(helper.job['id']) / 'state.json'
        before = history.read_bytes()
        self.assertEqual(cli._run(cli._parser().parse_args(['--destroy-only', '--run-id', helper.job['id']]), store=helper.store), 0)
        self.assertEqual(history.read_bytes(), before)
        self.assertEqual(json.loads(before)[operator.OPERATOR_JOURNAL_KEY]['execution_outcome'], 'failed')

    def test_destroy_not_required_or_interrupted_is_nonzero(self):
        # Catches treating no obligation or signal-deferred cleanup as success.
        self.boundaries()
        with mock.patch.object(cli.CandidateCloud, 'provision', side_effect=RuntimeError('precreate')):
            cli._run(self.args(), store=self.store)
        directory, = self.store.root.iterdir()
        args = cli._parser().parse_args(['--destroy-only', '--run-id', directory.name])
        self.assertEqual(cli._run(args, store=self.store), 2)
        helper = self.destroy_fixture()
        from scripts.deathstarbench_candidate_provider import CandidateCloud
        real_destroy = CandidateCloud.destroy_and_prove
        def interrupt(cloud, job, lease):
            os.kill(os.getpid(), signal.SIGTERM)
            return real_destroy(cloud, job, lease)
        with mock.patch.object(CandidateCloud, 'destroy_and_prove', interrupt):
            self.assertEqual(cli._run(cli._parser().parse_args(['--destroy-only', '--run-id', helper.job['id']]), store=helper.store), 143)

    def test_destroy_observes_signal_at_signal_context_exit(self):
        # Catches sampling deferred signal state before the final context exit.
        helper = self.destroy_fixture()
        original_signals = cli.support._signals
        @contextmanager
        def terminal_signal(job):
            with original_signals(job) as contexts:
                yield contexts
                os.kill(os.getpid(), signal.SIGTERM)
        with mock.patch.object(cli.support, '_signals', terminal_signal):
            self.assertEqual(cli._run(cli._parser().parse_args(['--destroy-only', '--run-id', helper.job['id']]), store=helper.store), 143)

    def test_destroy_refuses_incomplete_or_promoted_cleanup_proofs(self):
        # Catches returning zero from a truthy flag, wrong identity or a public
        # gate promotion instead of the exact strict absence proof.
        self.boundaries()
        with mock.patch.object(cli.CandidateCloud, 'provision', side_effect=RuntimeError('before-create')):
            cli._run(self.args(), store=self.store)
        directory, = self.store.root.iterdir()
        args = cli._parser().parse_args(['--destroy-only', '--run-id', directory.name])
        proof = {'schema_version': 1, 'candidate_schema': 'candidate-support-cleanup-proof-v1',
                 'candidate_only': True, 'run_id': directory.name, 'provider': 'azure',
                 'cleanup_complete': True, 'independent_absence_proved': True}
        for changed in ({'cleanup_complete': 1}, {'independent_absence_proved': False},
                        {'run_id': '000000000000'}, {'provider': 'gcp'}, {'measurement_qualified': True}):
            with self.subTest(changed=changed), mock.patch.object(cli.CandidateCloud, 'destroy_and_prove', return_value={**proof, **changed}):
                self.assertEqual(cli._run(args, store=self.store), 2)

    def test_unsafe_sdk_config_exception_text_is_withheld(self):
        # Catches legacy OCI config/SDK text leaking through new CLI diagnostics.
        with mock.patch.object(cli, '_run', side_effect=RuntimeError('https://signed.invalid/?token=SECRET')):
            self.assertEqual(cli.main([]), 2)
        self.assertNotIn('SECRET', self.errors.getvalue())
        self.assertNotIn('RuntimeError', self.errors.getvalue())

    def test_preoperator_cancellation_is_static_and_nonzero(self):
        # Catches leaking BaseException-derived cooperative cancellation before
        # the outer lifecycle owner has started.
        self.boundaries()
        with mock.patch.object(cli, 'STATE_ROOT', self.store.root), \
                mock.patch.object(cli.shared, '_ssh_material', side_effect=cli.application.RunCancelled('SECRET')):
            args = self.args()
            with mock.patch.object(cli, '_parser') as parser:
                parser.return_value.parse_args.return_value = args
                self.assertEqual(cli.main([]), 130)
        self.assertNotIn('SECRET', self.errors.getvalue())

    def test_non_oci_destroy_rejects_auth_config_before_sdk(self):
        helper = self.destroy_fixture()
        with mock.patch.object(cli.CandidateCloud, 'clients', side_effect=AssertionError('SDK')):
            with self.assertRaises(ValueError):
                cli._run(cli._parser().parse_args(['--destroy-only', '--run-id', helper.job['id'],
                    '--oci-config-file', 'private-auth-only']), store=helper.store)

    def test_oci_auth_config_keeps_pinned_scope_and_contains_unsafe_sdk_errors(self):
        # Catches treating auth input as provider scope, retaining config data,
        # or leaking the legacy config loader's arbitrary exception text.
        from tests.test_deathstarbench_hotel_candidate_provider import NativeStartupProviderTests
        from scripts import deathstarbench_candidate_provider as provider
        helper = NativeStartupProviderTests()
        helper.setUpClass()
        self.addCleanup(helper.doClassCleanups)
        job, cloud, store, lease, clients, _ = helper.build('oci')
        self.addCleanup(helper.doCleanups)
        cloud.provision(job, lease)
        lease.release()
        args = cli._parser().parse_args(['--destroy-only', '--run-id', job['id'],
                                        '--oci-config-file', 'private-auth-only'])
        with mock.patch.object(cli, 'STATE_ROOT', store.root), mock.patch.object(cli, '_parser') as parser, \
                mock.patch.object(provider.legacy_pins, '_oci_clients', side_effect=RuntimeError('SIGNED_SECRET_CONFIG')):
            parser.return_value.parse_args.return_value = args
            self.assertEqual(cli.main([]), 2)
        self.assertNotIn('SIGNED_SECRET_CONFIG', self.output.getvalue() + self.errors.getvalue())
        original_pin = copy.deepcopy(job['candidate_support_pin']['provider_pin'])
        def authenticate(options, pin):
            self.assertEqual(options.oci_config_file, Path('private-auth-only'))
            self.assertEqual(options.oci_profile, original_pin['profile'])
            self.assertEqual(pin, original_pin)
            return clients
        with mock.patch.object(provider.legacy_pins, '_oci_clients', side_effect=authenticate):
            self.assertEqual(cli._run(args, store=store), 0)
        for path in store.directory(job['id']).glob('*.json'):
            self.assertNotIn('private-auth-only', path.read_text())

    def test_explicit_registry_transport_scopes_fresh_lifecycle_and_restores(self):
        # Catches bypassing the existing registry-only context or leaking it
        # beyond the invocation on an exception.
        from scripts import deathstarbench_registry_egress as transport
        events = []
        mapping = object()
        @contextmanager
        def scoped(value):
            self.assertIs(value, mapping)
            events.append('enter')
            try:
                yield mapping
            finally:
                events.append('exit')
        def execute(args, *, store):
            self.assertEqual(events, ['enter'])
            raise KeyboardInterrupt()
        args = self.args('--registry-egress', 'private-mapping.json')
        with mock.patch.object(transport, 'load_mapping', return_value=mapping), \
                mock.patch.object(transport, 'registry_egress', scoped), \
                mock.patch.object(cli, '_run_private', execute), self.assertRaises(KeyboardInterrupt):
            cli._run(args, store=self.store)
        self.assertEqual(events, ['enter', 'exit'])


if __name__ == '__main__':
    unittest.main()
