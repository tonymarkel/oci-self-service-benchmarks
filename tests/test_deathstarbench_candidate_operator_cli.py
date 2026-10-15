"""Private operator CLI/state wiring only; no real registry, SSH or SDK calls."""

from contextlib import ExitStack, redirect_stdout
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock

from app import deathstarbench_candidate_operator as core
from app.models import BenchmarkPlan
from app.run_lease import RunLeaseError
from scripts import qualify_deathstarbench_candidate_support as cli
from scripts.deathstarbench_candidate_operator_state import (
    CandidateStateError, CandidateStateStore, _atomic_write_raw, canonical,
)
from scripts.deathstarbench_candidate_provider import CandidateCloud
from scripts.deathstarbench_candidate_provider import CLEANUP_SOURCE_KEY
from tests import test_deathstarbench_candidate_artifacts as artifacts
from tests.test_azure_provider import PUBLIC_KEY


def plan_values(provider='azure'):
    return {
        'provider': provider, 'region': 'eastus2' if provider == 'azure' else 'us-east1' if provider == 'gcp' else 'us-east-1',
        'azure_subscription_id': '11111111-2222-3333-4444-555555555555', 'azure_zone': '1',
        'gcp_project_id': 'candidate-project', 'gcp_zone': 'us-east1-b',
        'shape': 'Standard_D8s_v5', 'ocpus': 8, 'memory_gb': 32,
        'storage': {'additional_volume': False},
        'benchmarks': ['deathstarbench'], 'destroy_after_completion': True,
        'deathstarbench': {'topology_id': cli.DISTRIBUTED_TIERED_TOPOLOGY_ID,
                          'runtime_id': cli.K3S_RUNTIME_ID, 'workload': 'social_network'},
    }


class CandidatePrivateStateTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.store = CandidateStateStore(root / '.candidate-support')
        self.run_id, self.lease = self.store.allocate()
        self.stack.enter_context(self.lease)
        self.pin = {'run_id': self.run_id, 'infrastructure_plan': {'provider': 'azure'}}
        self.job = {'id': self.run_id, 'candidate_support_pin': copy.deepcopy(self.pin),
                    'plan': copy.deepcopy(self.pin['infrastructure_plan']), 'resources': {}, 'results': [],
                    'status': 'queued', '_key': 'PRIVATE-KEY-TEST-ONLY', '_passphrase': 'PASSWORD-TEST-ONLY'}
        self.store.publish_pin(self.pin, self.lease)

    def test_private_round_trip_excludes_process_secrets_and_public_history(self):
        self.store.save(self.job, self.lease)
        directory = self.store.directory(self.run_id)
        saved = self.store.load(self.run_id, self.lease)
        self.assertEqual(saved['id'], self.run_id)
        self.assertNotIn('_key', saved)
        self.assertNotIn('PRIVATE-KEY-TEST-ONLY', (directory / 'state.json').read_text())
        self.assertNotIn('PASSWORD-TEST-ONLY', (directory / 'state.json').read_text())
        for name in ('state.json', 'pin.json'):
            self.assertEqual(stat.S_IMODE((directory / name).stat().st_mode), 0o600)
        self.assertEqual(list(self.store.root.parent.glob('*/state.json')), [])

    def test_copied_released_or_other_store_lease_is_not_authority(self):
        with self.assertRaises(RunLeaseError):
            self.store.save(self.job, copy.copy(self.lease))
        other = CandidateStateStore(self.store.root.parent / 'other-store')
        (other.root / self.run_id).mkdir(mode=0o700)
        with self.assertRaises(CandidateStateError):
            other.require_lease(self.run_id, self.lease)
        self.lease.release()
        with self.assertRaises(RunLeaseError):
            self.store.save(self.job, self.lease)

    def test_pin_and_plan_changes_are_refused_without_overwriting_saved_state(self):
        self.store.save(self.job, self.lease)
        before = (self.store.directory(self.run_id) / 'state.json').read_bytes()
        for key in ('candidate_support_pin', 'plan'):
            changed = copy.deepcopy(self.job)
            changed[key]['injected'] = True
            with self.assertRaises(CandidateStateError):
                self.store.save(changed, self.lease)
        self.assertEqual((self.store.directory(self.run_id) / 'state.json').read_bytes(), before)
        with self.assertRaises(CandidateStateError):
            self.store.publish_pin(self.pin, self.lease)

    def test_symlink_mode_hardlink_fifo_and_duplicate_state_are_rejected(self):
        self.store.save(self.job, self.lease)
        directory = self.store.directory(self.run_id)
        path = directory / 'state.json'
        original = path.read_bytes()
        path.chmod(0o644)
        with self.assertRaises(CandidateStateError):
            self.store.load(self.run_id, self.lease)
        path.chmod(0o600)
        linked = directory / 'linked.json'
        os.link(path, linked)
        with self.assertRaises(CandidateStateError):
            self.store.load(self.run_id, self.lease)
        linked.unlink()
        path.unlink()
        path.symlink_to(directory / 'pin.json')
        with self.assertRaises(CandidateStateError):
            self.store.load(self.run_id, self.lease)
        path.unlink()
        os.mkfifo(path, 0o600)
        with self.assertRaises(CandidateStateError):
            self.store.load(self.run_id, self.lease)
        path.unlink()
        _atomic_write_raw(path, b'{"id":"first","id":"second"}')
        with self.assertRaises(CandidateStateError):
            self.store.load(self.run_id, self.lease)
        path.unlink()
        _atomic_write_raw(path, original)

    def test_artifact_copies_preserve_exact_bytes_and_refuse_post_adoption_change(self):
        source = self.store.root.parent / 'source.json'
        raw = b'{ "schema_version" : 1 }\n\n'
        _atomic_write_raw(source, raw)
        digest = hashlib.sha256(raw).hexdigest()
        saved = self.store.publish_artifact_input(self.run_id, 'publication.json', source, digest, self.lease)
        self.assertEqual(saved.read_bytes(), raw)
        _atomic_write_raw(source, b'{"schema_version":2}\n')
        with self.assertRaises(CandidateStateError):
            self.store.publish_artifact_input(self.run_id, 'publication.json', source, digest, self.lease)
        self.assertEqual(saved.read_bytes(), raw)

    def test_private_ssh_known_hosts_use_original_directory_and_acquired_capability(self):
        job = {**self.job, '_persist_state': False, '_candidate_ssh_lease': self.lease,
               '_candidate_ssh_directory': self.store.directory(self.run_id)}
        with mock.patch.object(cli.application, 'RUNS', self.store.root.parent):
            path = cli.application._ssh_known_hosts_path(job)
            self.assertEqual(path, self.store.directory(self.run_id) / 'known_hosts')
            self.assertFalse((self.store.root.parent / self.run_id).exists())
            changed = {**job, '_candidate_ssh_lease': copy.copy(self.lease)}
            with self.assertRaises(RunLeaseError):
                cli.application._ssh_known_hosts_path(changed)
            path.unlink()
            path.symlink_to(self.store.directory(self.run_id) / 'pin.json')
            with self.assertRaises(OSError):
                cli.application._ssh_known_hosts_path(job)


class CandidateOperatorCliTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.store = CandidateStateStore(root / '.candidate-support')
        fixture = artifacts.CandidateArtifactAdoptionTests()
        self.addCleanup(fixture.doCleanups)
        self.binding = fixture.adopt(fixture.fixture())
        self.ssh = {'private_key': 'PRIVATE-TEST-ONLY', 'public_key': PUBLIC_KEY, 'passphrase': None}
        self.plan = cli._infrastructure_plan(plan_values(), self.ssh)

    def saved_run(self, provider='azure'):
        run_id, lease = self.store.allocate()
        plan = self.plan if provider == 'azure' else cli._infrastructure_plan(plan_values(provider), self.ssh)
        pin = cli._new_pin(run_id, plan, {'schema_version': 1, 'provider': provider}, self.binding, self.ssh)
        job = {'id': run_id, 'plan': pin['infrastructure_plan'], 'candidate_support_pin': pin,
               'resources': {}, 'results': [], 'events': [], 'status': 'queued'}
        job[core.OPERATOR_JOURNAL_KEY] = core._new_record(pin)
        job[core.OPERATOR_JOURNAL_KEY]['state'] = 'cleanup_failed'
        self.store.publish_pin(pin, lease)
        self.store.save(job, lease)
        lease.release()
        return run_id

    def test_cleanup_only_needs_no_ssh_artifacts_or_registry_and_never_releases_ui(self):
        run_id = self.saved_run()
        def destroy(job, held):
            self.store.require_lease(run_id, held)
            self.assertNotIn('_key', job)
            self.assertIs(job['_persist_state'], False)
            return {'schema_version': 1, 'candidate_schema': core.CLEANUP_PROOF_SCHEMA,
                    'candidate_only': True, 'run_id': run_id, 'provider': 'azure',
                    'cleanup_complete': True, 'independent_absence_proved': True}
        cloud = mock.Mock(destroy_and_prove=destroy)
        with mock.patch.object(cli, 'CandidateCloud', return_value=cloud), \
             mock.patch.object(cli, '_adopt', side_effect=AssertionError('No registry during cleanup')), \
             mock.patch.object(cli.shared, '_ssh_material', side_effect=AssertionError('No SSH during cleanup')), \
             mock.patch.object(cli.application, 'ssh', side_effect=AssertionError('No guest commands during cleanup')), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(cli._run(cli._parser().parse_args(['--cleanup-only', run_id]), store=self.store), 0)
        cloud.provision.assert_not_called()
        cloud.audit.assert_not_called()
        cloud.reconcile_aws_capacity_rejection.assert_not_called()
        with self.store.acquire(run_id) as lease:
            saved = self.store.load(run_id, lease)
        self.assertTrue(saved[core.OPERATOR_JOURNAL_KEY]['cleanup_complete'])
        self.assertFalse(saved[core.OPERATOR_JOURNAL_KEY]['scope']['public_dispatch_enabled'])
        self.assertEqual(saved['results'], [])

    def test_aws_rejection_reconciliation_is_explicit_cleanup_only_and_precedes_normal_cleanup(self):
        run_id = self.saved_run('aws')
        order = []
        event_id = '11111111-2222-4333-8444-555555555555'
        def reconcile(job, held, *, event_id):
            self.store.require_lease(run_id, held)
            self.assertEqual(event_id, '11111111-2222-4333-8444-555555555555')
            order.append('reconcile')
        def destroy(job, held):
            self.store.require_lease(run_id, held)
            self.assertEqual(order, ['reconcile'])
            order.append('destroy')
            return {'schema_version': 1, 'candidate_schema': core.CLEANUP_PROOF_SCHEMA,
                    'candidate_only': True, 'run_id': run_id, 'provider': 'aws',
                    'cleanup_complete': True, 'independent_absence_proved': True}
        cloud = mock.Mock(destroy_and_prove=destroy, reconcile_aws_capacity_rejection=reconcile)
        with mock.patch.object(cli, 'CandidateCloud', return_value=cloud), \
             mock.patch.object(cli, '_adopt', side_effect=AssertionError('No registry')), \
             mock.patch.object(cli.shared, '_ssh_material', side_effect=AssertionError('No SSH')), \
             redirect_stdout(io.StringIO()) as output:
            args = cli._parser().parse_args(['--cleanup-only', run_id, '--reconcile-aws-capacity-rejection', event_id])
            self.assertEqual(cli._run(args, store=self.store), 0)
        self.assertEqual(order, ['reconcile', 'destroy'])
        self.assertNotIn('cloudtrail_event', output.getvalue())
        cloud.provision.assert_not_called()

    def test_aws_rejection_flag_is_refused_for_resume_new_and_nonaws_before_cloud(self):
        event_id = '11111111-2222-4333-8444-555555555555'
        for mode in ([], ['--resume', '0123456789ab']):
            with mock.patch.object(self.store, 'acquire') as acquire, self.assertRaisesRegex(ValueError, 'cleanup-only'):
                args = cli._parser().parse_args([*mode, '--reconcile-aws-capacity-rejection', event_id])
                cli._run(args, store=self.store)
            acquire.assert_not_called()
        run_id = self.saved_run()
        with mock.patch.object(cli, 'CandidateCloud') as cloud, self.assertRaisesRegex(ValueError, 'AWS-only'):
            args = cli._parser().parse_args(['--cleanup-only', run_id, '--reconcile-aws-capacity-rejection', event_id])
            cli._run(args, store=self.store)
        cloud.assert_not_called()

    def test_cleanup_carrier_has_only_nonsecret_placeholders_outside_saved_plan(self):
        restored = cli._infrastructure_plan(self.plan.model_dump(exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'}))
        self.assertEqual(restored.ssh_private_key, 'cleanup-only-no-ssh')
        self.assertEqual(restored.model_dump(exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'}),
                         self.plan.model_dump(exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'}))

    def test_model_validation_never_reports_injected_private_input(self):
        secret = {**self.ssh, 'private_key': 'SECRET-PRIVATE-INPUT', 'passphrase': 'SECRET-PASSPHRASE'}
        with self.assertRaises(ValueError) as caught:
            cli._infrastructure_plan({**plan_values(), 'benchmarks': ['unknown']}, secret)
        self.assertNotIn(secret['private_key'], str(caught.exception))
        self.assertNotIn(secret['passphrase'], str(caught.exception))
        self.assertIn('withheld', str(caught.exception))

    def test_aws_oci_provider_pins_preserve_integral_capacity_and_oci_tags(self):
        from tests.test_aws_oci_deathstarbench_live_qualification import AwsOciDistributedQualificationTests
        fixture = AwsOciDistributedQualificationTests()
        for provider in ('aws', 'oci'):
            pin = fixture.aws_pin() if provider == 'aws' else fixture.oci_pin()
            args = cli._parser().parse_args([])
            plan = cli._infrastructure_plan({**plan_values(provider),
                'region': pin['region'], 'shape': pin['application_shape'],
                'availability_domain': pin.get('availability_zone', pin.get('availability_domain')),
                'compartment_id': pin.get('compartment_id'), 'ocpus': pin['application_vcpus'],
                'oci_defined_tags': pin.get('defined_tags', {}),
            }, self.ssh)
            if provider == 'oci':
                args.oci_application_architecture = pin['application_architecture']
                args.oci_application_image_id = pin['application_image_id']
                args.oci_support_image_id = pin['support_image_id']
            with mock.patch.object(cli.pins, '_read_aws_image_pin', return_value={
                    'support_image': fixture.image(), 'application_image': fixture.image()}), \
                 mock.patch.object(cli.pins, '_oci_config', return_value={'region': pin['region']}):
                actual = cli._provider_pin(args, plan, self.ssh)
            self.assertIs(type(actual['application_vcpus']), int)
            self.assertEqual(actual['application_vcpus'], plan.ocpus)
            if provider == 'oci':
                self.assertEqual(actual['defined_tags'], plan.oci_defined_tags)

    def test_recovery_provider_overrides_and_cleanup_artifact_override_fail_before_acquire(self):
        for flags in (['--cleanup-only', '0123456789ab', '--image-lock', 'wrong.json'],
                      ['--resume', '0123456789ab', '--oci-profile', 'OTHER'],
                      ['--resume', '0123456789ab', '--aws-image-pin', 'wrong.json'],
                      ['--resume', '0123456789ab', '--plan', 'wrong.json']):
            with mock.patch.object(self.store, 'acquire') as acquire, self.assertRaises(ValueError):
                cli._run(cli._parser().parse_args(flags), store=self.store)
            acquire.assert_not_called()

    def test_carrier_rejects_other_benchmarks_secrets_storage_and_retained_resources(self):
        for change in ({'benchmarks': ['fio']}, {'destroy_after_completion': False},
                       {'storage': {'additional_volume': True}}, {'ssh_private_key': 'DO-NOT-SAVE'}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                cli._infrastructure_plan({**plan_values(), **change}, self.ssh)

    def test_provider_guard_and_azure_create_identity_precede_delete(self):
        run_id = self.saved_run()
        with self.store.acquire(run_id) as lease:
            job = self.store.load(run_id, lease)
            job[core.OPERATOR_JOURNAL_KEY]['cloud_identity'] = {'opaque': 'original'}
            cloud = CandidateCloud(self.plan, job['candidate_support_pin']['provider_pin'], persist=lambda _job: None, emit=lambda *_args: None)
            from scripts import deathstarbench_candidate_cloud_audit as audit
            with mock.patch.object(audit, 'require_azure_cleanup_identity', side_effect=ValueError('replaced')) as guard, \
                 mock.patch.object(cli.application.azure_provider, 'destroy_resources') as destroy:
                with self.assertRaisesRegex(ValueError, 'replaced'):
                    cloud.destroy_and_prove(job, lease)
                guard.assert_called_once()
                destroy.assert_not_called()
            job['plan']['region'] = 'wrong'
            with self.assertRaisesRegex(ValueError, 'Infrastructure plan'):
                cloud.audit(job, lease)

    def test_durable_original_graph_survives_graph_clear_and_failed_absence_probe(self):
        run_id = self.saved_run()
        with self.store.acquire(run_id) as lease:
            job = self.store.load(run_id, lease)
            job['resources'] = {'original-exact-graph': {'opaque_id': 'original'}}
            snapshots = []
            def persist(value):
                self.store.save(value, lease)
                snapshots.append(copy.deepcopy(value))
            cloud = CandidateCloud(self.plan, job['candidate_support_pin']['provider_pin'], persist=persist, emit=lambda *_args: None)
            from scripts import deathstarbench_candidate_cloud_audit as audit
            def clear_graph(value, **_kwargs):
                self.assertEqual(snapshots[-1][CLEANUP_SOURCE_KEY]['resources'], {'original-exact-graph': {'opaque_id': 'original'}})
                value['resources'] = {'terminal-marker': True}
                persist(value)
            with mock.patch.object(cli.application.azure_provider, 'destroy_resources', side_effect=clear_graph), \
                 mock.patch.object(audit, 'audit_azure_absent', side_effect=RuntimeError('transient GET outage')), \
                 mock.patch.object(cli.application, '_cleared_distributed_provider_contract', return_value=True):
                with self.assertRaisesRegex(RuntimeError, 'GET outage'):
                    cloud.destroy_and_prove(job, lease)
            reloaded = self.store.load(run_id, lease)
            with mock.patch.object(cli.application.azure_provider, 'destroy_resources'), \
                 mock.patch.object(audit, 'audit_azure_absent', return_value={'state': 'absent'}) as absent, \
                 mock.patch.object(cli.application, '_cleared_distributed_provider_contract', return_value=True):
                proof = cloud.destroy_and_prove(reloaded, lease)
            self.assertTrue(proof['independent_absence_proved'])
            self.assertEqual(absent.call_args.args[0]['resources'], {'original-exact-graph': {'opaque_id': 'original'}})


if __name__ == '__main__':
    unittest.main()
