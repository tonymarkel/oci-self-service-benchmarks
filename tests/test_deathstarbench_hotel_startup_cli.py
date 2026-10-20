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
        from app import deathstarbench_hotel_live_controller as initializer
        from app import deathstarbench_hotel_startup_controller as startup
        from app.deathstarbench_hotel_image_access import require_guarded_hotel_image_access
        from app.deathstarbench_hotel_startup_image_access import require_startup_hotel_image_access
        from app.deathstarbench_hotel_runtime import create_guarded_hotel_runtime_plan
        from app.deathstarbench_hotel_initialization_stage import build_guarded_hotel_initialization_stage
        from tests.test_deathstarbench_hotel_startup_operator import StartupOperatorTests
        from tests import test_deathstarbench_distributed_runtime as cloud_fixture
        self.boundaries()
        handoff = {}
        def lower_initialize(job, artifacts, **kwargs):
            require_acquired_run_lease(kwargs['lease'], job['id'])
            require_guarded_hotel_image_access(kwargs['image_access'], artifacts)
            self.assertIs(artifacts, handoff['guarded'])
            self.assertIs(kwargs['baseline_artifacts'], handoff['baseline'])
            self.assertEqual(kwargs['initialization_attempt_id'], job['hotel_initialization_pin']['initialization_attempt_id'])
            job['guarded_hotel_initialization'] = {'independent': 'initializer'}
            return copy.deepcopy(job['guarded_hotel_initialization'])
        def lower_startup(job, artifacts, **kwargs):
            require_startup_hotel_image_access(kwargs['image_access'], artifacts)
            self.assertIs(artifacts, handoff['startup'])
            self.assertIs(kwargs['baseline_artifacts'], handoff['baseline'])
            self.assertIs(kwargs['lease'], handoff['lease'])
            self.assertIs(kwargs['initialization_plan'], handoff['plan'])
            self.assertIs(kwargs['initialization_stage'], handoff['stage'])
            self.assertEqual(kwargs['startup_attempt_id'], job['hotel_startup_pin']['startup_attempt_id'])
            self.assertEqual(job['guarded_hotel_initialization'], {'independent': 'initializer'})
            handoff['completed'] = True
            return {'independent': 'startup'}
        def drive(job, **callbacks):
            handoff.update(baseline=callbacks['artifacts'], guarded=callbacks['guarded_artifacts'],
                           startup=callbacks['startup_artifacts'], lease=callbacks['lease'])
            self.assertIs(type(handoff['baseline']), baseline_adoption.CandidateArtifactBinding)
            self.assertIs(type(handoff['guarded']), guarded_adoption.GuardedHotelArtifactBinding)
            self.assertIs(type(handoff['startup']), startup_adoption.StartupHotelArtifactBinding)
            job['resources'] = copy.deepcopy(cloud_fixture.candidate_job()['resources'])
            from app.providers import azure
            job['resources']['azure_resource_group_tags'] = azure._tags(job['id'], 'resource-group')
            helper = StartupOperatorTests()
            helper.calls = []
            helper.baseline = callbacks['artifacts']
            handoff['support_helper'] = helper
            helper.runtime(job, callbacks['lease'])
            access = callbacks['support_image_access'](job, callbacks['lease'])
            self.assertIs(access['anonymous_support_bytes_verified'], True)
            callbacks['stage'](job, callbacks['lease'])
            job[operator.OPERATOR_JOURNAL_KEY] = {'cloud_identity': {'offline': 'original'}}
            self.assertEqual(callbacks['initialize'](job, callbacks['lease']), {'independent': 'initializer'})
            _, support_plan, support_stage, _ = cli._context(job, callbacks['artifacts'], cli.runtime.distributed_k3s_candidate_plan)
            plan = create_guarded_hotel_runtime_plan(callbacks['guarded_artifacts'],
                support_plan, support_stage,
                initialization_attempt_id=job['hotel_initialization_pin']['initialization_attempt_id'])
            stage = build_guarded_hotel_initialization_stage(plan)
            handoff.update(plan=plan, stage=stage)
            self.assertEqual(callbacks['startup'](job, callbacks['lease'], initialization_plan=plan,
                initialization_stage=stage), {'independent': 'startup'})
            return operator.HotelStartupOperatorOutcome('failed', 'not_required', {'code': 'execution-failed', 'interrupted': False}, None, 1)
        # This is a narrow CLI callback wiring test. The preceding test executes
        # the real outer operator; operator regression suites cover its phases.
        def lower_support(job, artifacts, **kwargs):
            self.assertIs(artifacts, handoff['baseline'])
            self.assertIs(kwargs['lease'], handoff['lease'])
            return handoff['support_helper'].stage(job, kwargs['lease'])
        with mock.patch.object(cli, 'run_hotel_startup_operator', side_effect=drive), \
                mock.patch.object(cli, 'prepare_candidate_support_stage', side_effect=lower_support), \
                mock.patch.object(initializer, 'prepare_guarded_hotel_initialization', side_effect=lower_initialize), \
                mock.patch.object(startup, 'prepare_startup_hotel_applications', side_effect=lower_startup):
            self.assertEqual(cli._run(self.args(), store=self.store), 1)
        self.assertTrue(handoff['completed'])

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
