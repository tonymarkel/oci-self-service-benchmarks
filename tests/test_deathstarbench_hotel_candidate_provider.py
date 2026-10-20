"""Synthetic provider wiring only; never a cloud or deletion qualification."""

import copy
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from app import deathstarbench_candidate_operator as support
from app import deathstarbench_hotel_initialization_operator as hotel
from app.models import BenchmarkPlan
from app.run_lease import acquire_run_lease
from scripts import deathstarbench_candidate_cloud_audit as audit
from scripts import deathstarbench_candidate_provider as provider
from tests import test_deathstarbench_hotel_initialization_artifacts as artifacts
from tests.test_deathstarbench_candidate_operator_cli import plan_values


RUN = '123456abcdef'


class HotelCandidateProviderTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory())) / '.candidate-support'
        root.mkdir(mode=0o700)
        (root / RUN).mkdir(mode=0o700)
        self.lease = acquire_run_lease(root, RUN, 'hotel-provider-test')
        self.addCleanup(self.lease.release)
        helper = artifacts.GuardedHotelArtifactsTests()
        self.addCleanup(helper.doCleanups)
        data = helper.fixture()
        self.guarded = helper.adopt(data)
        self.baseline = data['baseline']

    def fixture(self, cloud_provider='azure', *, with_identity=True):
        plan = BenchmarkPlan.model_validate({**plan_values(cloud_provider),
            'ssh_private_key': 'synthetic-only', 'ssh_public_key': 'synthetic-only'})
        saved_plan = plan.model_dump(exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'})
        pin = {
            'schema_version': 1, 'candidate_schema': support.PIN_SCHEMA,
            'candidate_only': True, 'released': False, 'run_id': RUN,
            'workload_id': 'hotel_reservation', 'artifact_fingerprint': self.baseline.fingerprint,
            'artifacts': self.baseline.metadata(), 'infrastructure_plan': saved_plan,
            'provider_pin': {'provider': cloud_provider}, 'ssh_public_key_sha256': '1' * 64,
        }
        pin['fingerprint'] = support._fingerprint(pin)
        hotel_pin = hotel.make_hotel_initialization_pin(RUN, pin, self.guarded)
        record = hotel._new_record(hotel_pin)
        identity = {
            'revision': audit.REVISION, 'provider': cloud_provider, 'run_id': RUN,
            'plan_sha256': audit._sha(saved_plan), 'account': {'fixture_only': 'no-live-identity'},
            'resources': [],
        }
        resources = {'provider': cloud_provider, cloud_provider + '_distributed_candidate': True}
        if cloud_provider == 'gcp':
            for index, prefix in enumerate(provider.gcp.DISTRIBUTED_RESOURCE_PREFIXES, 1):
                link = 'https://www.googleapis.com/compute/v1/projects/candidate-project/global/networks/' + prefix
                identity['resources'].append({'prefix': prefix, 'id': str(index), 'self_link': link})
                resources.update({prefix + '_id': str(index), prefix + '_self_link': link})
        if with_identity:
            record.update(state='initializing', cloud_identity=identity, audits=[{
                'schema_version': 1, 'candidate_only': True, 'read_only': True,
                'run_id': RUN, 'provider': cloud_provider, 'cloud_identity': copy.deepcopy(identity),
            }])
        job = {
            'id': RUN, '_persist_state': False, 'results': [], 'plan': saved_plan,
            'resources': resources, support.PIN_KEY: pin, hotel.PIN_KEY: hotel_pin,
            hotel.OPERATOR_JOURNAL_KEY: record,
        }
        saved = []
        cloud = provider.CandidateCloud(plan, pin['provider_pin'],
            persist=lambda value: saved.append(copy.deepcopy(value)), emit=lambda *_args: None,
            operator_journal_key=hotel.OPERATOR_JOURNAL_KEY)
        return job, cloud, saved

    def cleanup(self, job, cloud, *, guard_error=None):
        adapter = provider.azure if cloud.plan.provider == 'azure' else provider.gcp
        absent_name = 'audit_' + cloud.plan.provider + '_absent'
        with mock.patch.object(audit, 'require_azure_cleanup_identity', side_effect=guard_error) as guard, \
             mock.patch.object(adapter, 'destroy_resources') as destroy, \
             mock.patch.object(audit, absent_name, return_value={'fixture_only': True}), \
             mock.patch.object(provider.application, '_cleared_distributed_provider_contract', return_value=True):
            if guard_error is not None:
                with self.assertRaisesRegex(ValueError, 'replacement'):
                    cloud.destroy_and_prove(job, self.lease)
                destroy.assert_not_called()
            else:
                result = cloud.destroy_and_prove(job, self.lease)
                self.assertTrue(result['cleanup_complete'])
                destroy.assert_called_once()
            return guard

    def test_only_known_operator_selector_keys_are_accepted(self):
        for value in ('arbitrary-field', None, True):
            with self.assertRaises(ValueError):
                provider.CandidateCloud(None, {}, persist=None, emit=None, operator_journal_key=value)

    def test_hotel_selects_its_own_audit_not_legacy_decoy_before_azure_delete(self):
        job, cloud, saved = self.fixture()
        job[support.OPERATOR_JOURNAL_KEY] = {'cloud_identity': {'legacy-decoy': True}}
        expected = copy.deepcopy(job[hotel.OPERATOR_JOURNAL_KEY]['cloud_identity'])
        guard = self.cleanup(job, cloud)
        self.assertEqual(guard.call_args.args[1], expected)
        self.assertEqual(saved[0][provider.CLEANUP_SOURCE_KEY]['cloud_identity'], expected)

    def test_azure_opaque_identity_drift_refuses_before_delete(self):
        job, cloud, _saved = self.fixture()
        guard = self.cleanup(job, cloud, guard_error=ValueError('same-name replacement'))
        guard.assert_called_once()

    def test_crossed_run_pin_or_audit_refuses_before_archive_or_azure_delete(self):
        for change in ('run', 'audit', 'pin'):
            with self.subTest(change=change):
                job, cloud, saved = self.fixture()
                if change == 'run':
                    job[hotel.OPERATOR_JOURNAL_KEY]['cloud_identity']['run_id'] = 'fedcba654321'
                    job[hotel.OPERATOR_JOURNAL_KEY]['audits'][0]['cloud_identity']['run_id'] = 'fedcba654321'
                elif change == 'audit':
                    job[hotel.OPERATOR_JOURNAL_KEY]['audits'][0]['read_only'] = False
                else:
                    job[hotel.PIN_KEY]['support_pin_fingerprint'] = 'sha256:' + '0' * 64
                with mock.patch.object(provider.azure, 'destroy_resources') as destroy, self.assertRaises(ValueError):
                    cloud.destroy_and_prove(job, self.lease)
                destroy.assert_not_called()
                self.assertEqual(saved, [])
                self.assertNotIn(provider.CLEANUP_SOURCE_KEY, job)

    def test_gcp_audited_numeric_graph_is_preserved_and_existing_delete_guard_used(self):
        job, cloud, saved = self.fixture('gcp')
        guard = self.cleanup(job, cloud)
        guard.assert_not_called()
        self.assertEqual(saved[0][provider.CLEANUP_SOURCE_KEY]['cloud_identity'],
                         job[hotel.OPERATOR_JOURNAL_KEY]['cloud_identity'])

    def test_gcp_numeric_or_link_drift_refuses_before_archive_or_delete(self):
        for field in ('id', 'self_link'):
            with self.subTest(field=field):
                job, cloud, saved = self.fixture('gcp')
                prefix = provider.gcp.DISTRIBUTED_RESOURCE_PREFIXES[0]
                job['resources'][prefix + '_' + field] = '999' if field == 'id' else 'projects/other/global/networks/replacement'
                with mock.patch.object(provider.gcp, 'destroy_resources') as destroy, self.assertRaises(ValueError):
                    cloud.destroy_and_prove(job, self.lease)
                destroy.assert_not_called()
                self.assertEqual(saved, [])
                self.assertNotIn(provider.CLEANUP_SOURCE_KEY, job)

    def test_retained_cleanup_source_cannot_cross_the_original_audit(self):
        job, cloud, _saved = self.fixture()
        self.cleanup(job, cloud)
        source = job[provider.CLEANUP_SOURCE_KEY]
        source['cloud_identity']['account'] = {'crossed': 'replacement'}
        source['fingerprint'] = provider.fingerprint({key: value for key, value in source.items() if key != 'fingerprint'})
        with mock.patch.object(provider.azure, 'destroy_resources') as destroy, self.assertRaisesRegex(ValueError, 'crossed'):
            cloud.destroy_and_prove(job, self.lease)
        destroy.assert_not_called()

    def test_audit_mutation_during_archive_persistence_refuses_before_delete(self):
        job, cloud, _saved = self.fixture()
        def mutate(value):
            identity = value[hotel.OPERATOR_JOURNAL_KEY]['cloud_identity']
            identity['account'] = {'replacement': True}
            value[hotel.OPERATOR_JOURNAL_KEY]['audits'][0]['cloud_identity'] = copy.deepcopy(identity)
        cloud.persist = mutate
        with mock.patch.object(provider.azure, 'destroy_resources') as destroy, self.assertRaisesRegex(ValueError, 'changed during'):
            cloud.destroy_and_prove(job, self.lease)
        destroy.assert_not_called()

    def test_partial_provision_without_ready_audit_retains_exact_unaudited_cleanup(self):
        job, cloud, saved = self.fixture(with_identity=False)
        guard = self.cleanup(job, cloud)
        guard.assert_not_called()
        self.assertIsNone(saved[0][provider.CLEANUP_SOURCE_KEY]['cloud_identity'])


from tests.test_deathstarbench_hotel_startup_ownership import OriginalArtifactsMixin, INITIALIZATION, STARTUP


class StartupCandidateProviderTests(OriginalArtifactsMixin, unittest.TestCase):
    def setUp(self):
        import hashlib
        from app import deathstarbench_hotel_startup_operator as startup
        from app import deathstarbench_hotel_startup_ownership as ownership
        from scripts.deathstarbench_hotel_startup_operator_state import HotelStartupStateStore
        from tests.test_aws_hotel_startup_ownership import StrictEC2
        from tests.test_aws_deathstarbench_lifecycle import PIN, PUBLIC_KEY, candidate_plan, FakeSession, SimpleNamespace
        PUBLIC_KEY = ' '.join(PUBLIC_KEY.split()[:2]) + getattr(self, 'public_key_comment', ' benchmark')
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = HotelStartupStateStore(Path(temporary.name) / 'private')
        (self.store.root / RUN).mkdir(mode=0o700)
        self.lease = self.store.acquire(RUN)
        self.addCleanup(self.lease.release)
        self.plan_model = BenchmarkPlan.model_validate({**candidate_plan(),
            'ocpus': 8, 'memory_gb': 32,
            'ssh_private_key': 'offline-only', 'ssh_public_key': PUBLIC_KEY})
        saved = self.plan_model.model_dump(exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'})
        pin = copy.deepcopy(self.pin)
        pin.update(infrastructure_plan=saved, provider_pin={'provider': 'aws', 'support_image': PIN, 'application_image': PIN},
                   ssh_public_key_sha256=hashlib.sha256(PUBLIC_KEY.encode()).hexdigest())
        pin['fingerprint'] = support._fingerprint({key: value for key, value in pin.items() if key != 'fingerprint'})
        initialization_pin = hotel.make_hotel_initialization_pin(RUN, pin, self.guarded_artifacts, attempt_id=INITIALIZATION)
        startup_pin = startup.make_hotel_startup_pin(RUN, pin, initialization_pin, self.startup_artifacts, startup_attempt_id=STARTUP)
        self.job = {'id': RUN, '_persist_state': False, '_public_key': PUBLIC_KEY, 'results': [],
                    'plan': saved, 'resources': {}, support.PIN_KEY: pin, hotel.PIN_KEY: initialization_pin,
                    startup.PIN_KEY: startup_pin}
        self.job[ownership.CHECKPOINT_KEY] = ownership.make_hotel_startup_ownership_checkpoint(self.job,
            support_pin=pin, initialization_pin=initialization_pin, startup_pin=startup_pin)
        self.store.publish_pin(pin, self.lease)
        self.store.publish_hotel_pin(initialization_pin, self.lease)
        self.store.publish_startup_pin(startup_pin, self.lease)
        self.store.save(self.job, self.lease)
        self.ec2 = StrictEC2()
        self.ec2.application_capacity = (8, 32768)
        self.session = FakeSession(ec2=self.ec2, sts=SimpleNamespace(get_caller_identity=lambda: {'Account': '123456789012'}))
        patch = mock.patch.object(provider.aws, '_session', return_value=self.session)
        patch.start()
        self.addCleanup(patch.stop)
        self.cloud = provider.CandidateCloud(self.plan_model, pin['provider_pin'],
            persist=lambda job: self.store.save(job, self.lease), emit=lambda *_: None,
            operator_journal_key='hotel_startup_operator')

    def test_startup_destroy_publishes_exact_targets_before_delete(self):
        from scripts.deathstarbench_hotel_startup_operator_state import OWNERSHIP_FILE
        from app import deathstarbench_hotel_startup_ownership as ownership
        from scripts import deathstarbench_hotel_startup_operator_state as state
        import os
        import stat
        self.cloud.provision(self.job, self.lease)
        receipt = self.cloud.audit(self.job, self.lease)
        self.job[ownership.CHECKPOINT_KEY] = ownership.record_hotel_startup_ownership_audit(self.job[ownership.CHECKPOINT_KEY], receipt)
        self.store.save(self.job, self.lease)
        self.job[support.OPERATOR_JOURNAL_KEY] = {'cloud_identity': {'legacy-decoy': True}}
        events = []
        def before(name, args):
            if name.startswith(('stop_', 'terminate_', 'delete_', 'revoke_', 'detach_', 'disassociate_', 'release_')):
                source = self.store.cleanup_ownership(RUN, self.lease)
                self.assertGreater(len(source['known_identities']), 21)
                events.extend(('publish-cleanup-source', 'delete'))
        self.ec2.before = before
        real_link, real_fsync = os.link, os.fsync
        def fail_link(source, target, **kwargs):
            if Path(target).name == OWNERSHIP_FILE:
                raise OSError('seal installation unavailable')
            return real_link(source, target, **kwargs)
        with mock.patch.object(state.os, 'link', side_effect=fail_link), self.assertRaises(OSError):
            self.cloud.destroy_and_prove(self.job, self.lease)
        self.assertEqual(events, [])
        seal_path = self.store.root / RUN / OWNERSHIP_FILE
        def fail_directory_sync(fd):
            if seal_path.exists() and stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError('seal directory sync unavailable')
            return real_fsync(fd)
        with mock.patch.object(state.os, 'fsync', side_effect=fail_directory_sync), self.assertRaises(OSError):
            self.cloud.destroy_and_prove(self.job, self.lease)
        self.assertEqual(events, [])
        self.assertIsNone(self.store.checkpoint(RUN, self.lease)['seal_fingerprint'])
        loaded = self.store.load_for_destroy(RUN, self.lease)
        self.assertEqual(loaded[ownership.CHECKPOINT_KEY]['seal_fingerprint'], self.store.cleanup_ownership(RUN, self.lease)['fingerprint'])
        # The startup adapter never restores an execution journal. Explicitly
        # exercise the real lower-level quiesce-capable path with the seal in
        # custody to prove stop ordering at the external SDK boundary too.
        probe = {**loaded, 'resources': copy.deepcopy(loaded['resources'])}
        probe['resources'][provider.aws.DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {'state': 'measurement_started'}
        provider.aws.quiesce_distributed_deathstarbench_load_generator(probe, strict_cleanup=True,
            expected_cleanup_identity=self.cloud._startup_aws_boundary(self.store.cleanup_ownership(RUN, self.lease)))
        self.assertTrue(any(name == 'stop_instances' for name, _ in self.ec2.calls))
        self.assertTrue(self.cloud.destroy_and_prove(self.job, self.lease)['independent_absence_proved'])
        self.assertLess(events.index('publish-cleanup-source'), events.index('delete'))
        self.assertTrue((self.store.root / RUN / OWNERSHIP_FILE).is_file())
        source = self.store.cleanup_ownership(RUN, self.lease)
        original_targets = copy.deepcopy(source['absence_targets'])
        loaded = self.store.load_for_destroy(RUN, self.lease)
        self.assertTrue(self.cloud.destroy_and_prove(loaded, self.lease)['cleanup_complete'])
        self.assertEqual(self.store.cleanup_ownership(RUN, self.lease)['absence_targets'], original_targets)

    def test_intact_precontract_cleanup_is_not_required_without_sdk_access(self):
        before = list(self.ec2.calls)
        result = self.cloud.destroy_and_prove(self.job, self.lease)
        self.assertEqual(result['candidate_schema'], 'hotel-startup-cleanup-not-required-v1')
        self.assertEqual(self.ec2.calls, before)

    def test_history_failure_after_create_contract_keeps_cleanup_authority_without_allocation(self):
        from scripts import deathstarbench_hotel_startup_operator_state as state
        with mock.patch.object(state, 'project_hotel_startup_job', side_effect=ValueError('history unavailable')):
            with self.assertRaises(ValueError):
                self.cloud.provision(self.job, self.lease)
            loaded = self.store.load_for_destroy(RUN, self.lease)
            self.assertTrue(loaded['hotel_startup_owned_graph']['contract_recorded'])
            self.assertEqual(loaded['hotel_startup_owned_graph']['create_attempts'], {})
            result = self.cloud.destroy_and_prove(loaded, self.lease)
            self.assertTrue(result['cleanup_complete'])
            self.assertNotIn('reason', result)
        self.assertFalse(any(name.startswith(('create_', 'run_instances', 'import_key_pair')) for name, _ in self.ec2.calls))

    def test_missing_or_crossed_ownership_refuses(self):
        self.cloud.provision(self.job, self.lease)
        path = self.store.root / RUN / 'hotel-startup-owned-graph.json'
        path.write_text('{}')
        before = len(self.ec2.calls)
        with self.assertRaises(ValueError):
            self.cloud.destroy_and_prove(self.job, self.lease)
        self.assertEqual(self.ec2.calls[before:], [])

    def test_disk_load_refuses_dropped_aws_id_with_retained_original_target(self):
        # A valid checksum must not hide a native graph/independent-ID conflict.
        from app import deathstarbench_hotel_startup_ownership as ownership
        self.cloud.provision(self.job, self.lease)
        path = self.store.root / RUN / 'hotel-startup-owned-graph.json'
        original = json.loads(path.read_text())
        target = copy.deepcopy(original['known_identities']['aws-key'])
        self.assertTrue(target['id'].startswith('key-'))
        before = list(self.ec2.calls)
        for status in ('deleted', 'planned'):
            with self.subTest(status=status):
                checkpoint = copy.deepcopy(original)
                checkpoint['resources'][provider.aws.AWS_DSB_GRAPH_KEY]['graph']['key'] = {'status': status}
                checkpoint['fingerprint'] = provider.fingerprint({
                    key: value for key, value in checkpoint.items() if key != 'fingerprint'})
                path.write_text(provider.canonical(checkpoint))
                checked = ownership.validate_hotel_startup_ownership_checkpoint(
                    json.loads(path.read_text()), run_id=RUN, provider='aws')
                self.assertEqual(checked['known_identities']['aws-key'], target)
                self.assertEqual(checked['absence_targets']['aws-key'], target)
                with self.assertRaises(ValueError):
                    self.store.load_for_destroy(RUN, self.lease)
                with self.assertRaises(ValueError):
                    self.cloud.destroy_and_prove(self.job, self.lease)
                self.assertEqual(self.ec2.calls, before)
        path.write_text(provider.canonical(original))

    def test_partial_provision_uses_create_identity_not_fake_readiness(self):
        from botocore.exceptions import ClientError
        def reject(kind, request):
            if kind == 'instance':
                raise ClientError({'Error': {'Code': 'InsufficientInstanceCapacity'}}, 'RunInstances')
        self.ec2.cloud.on_create = reject
        with self.assertRaises(ClientError):
            self.cloud.provision(self.job, self.lease)
        self.ec2.cloud.on_create = None
        (self.store.root / RUN / 'state.json').write_bytes(b'corrupt execution history')
        loaded = self.store.load_for_destroy(RUN, self.lease)
        self.assertEqual(loaded['hotel_startup_owned_graph']['audits'], [])
        self.assertIn('aws-instance-control', loaded['hotel_startup_owned_graph']['create_attempts'])
        self.assertTrue(self.cloud.destroy_and_prove(loaded, self.lease)['cleanup_complete'])
        terminal = self.store.load_for_destroy(RUN, self.lease)['hotel_startup_owned_graph']
        graph = terminal['resources'][provider.aws.AWS_DSB_GRAPH_KEY]['graph']
        self.assertEqual(graph['key']['id'], terminal['known_identities']['aws-key']['id'])
        self.assertEqual(graph['key']['status'], 'deleted')
        for key in ('instance-control', 'instance-load-generator'):
            self.assertEqual(graph[key], {'status': 'deleted'})
            self.assertNotIn('aws-' + key, terminal['known_identities'])
        self.assertIn('aws-instance-control', terminal['create_attempts'])
        self.assertNotIn('aws-instance-load-generator', terminal['create_attempts'])


class AwsPublicKeyStartupTests(OriginalArtifactsMixin, unittest.TestCase):
    setUp = StartupCandidateProviderTests.setUp
    public_key_comment = ' operator@example.test'

    def test_exact_original_native_public_key_comment_remains_private(self):
        self.cloud.provision(self.job, self.lease)
        checkpoint = self.store.checkpoint(RUN, self.lease)
        self.assertTrue(checkpoint['create_contract']['public_key'].endswith(self.public_key_comment))
        history = (self.store.root / RUN / 'state.json').read_text()
        self.assertNotIn('operator@example.test', history)
        self.assertNotIn('offline-only', history)
        self.assertNotIn('offline-only', (self.store.root / RUN / 'hotel-startup-owned-graph.json').read_text())
        contract = self.job['resources'][provider.aws.AWS_DSB_GRAPH_KEY]['contract']
        contract['public_key'] += '-changed'
        with self.assertRaises(ValueError):
            self.store.save(self.job, self.lease)
        before = len(self.ec2.calls)
        with self.assertRaises(ValueError):
            provider.aws.provision_distributed_deathstarbench_candidate({'id': RUN, 'resources': {}}, self.plan_model,
                public_key='not-an-ssh-key', support_image=self.cloud.pin['support_image'],
                application_image=self.cloud.pin['application_image'], strict_create_identity=True)
        self.assertEqual(len(self.ec2.calls), before)


class NativeStartupProviderTests(OriginalArtifactsMixin, unittest.TestCase):
    def seal_current(self, job, store, lease):
        from app import deathstarbench_hotel_startup_ownership as ownership
        store.save_cleanup_progress(job, lease, finalizing=True)
        seal = ownership.make_hotel_startup_cleanup_ownership(job, support_pin=job[support.PIN_KEY],
            initialization_pin=job[hotel.PIN_KEY], startup_pin=job['hotel_startup_pin'])
        store.publish_cleanup_ownership(RUN, seal, lease)
        probe = store.load_for_destroy(RUN, lease)
        probe['resources'][provider.gcp.DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {'state': 'measurement_started'}
        return probe

    def audit_and_record(self, job, cloud, store, lease):
        from app import deathstarbench_hotel_startup_ownership as ownership
        receipt = cloud.audit(job, lease)
        job[ownership.CHECKPOINT_KEY] = ownership.record_hotel_startup_ownership_audit(job[ownership.CHECKPOINT_KEY], receipt)
        store.save(job, lease)
        return receipt

    def azure_ready(self, job, plan, clients):
        from uuid import uuid5, NAMESPACE_URL
        from tests.test_deathstarbench_candidate_cloud_audit import AzureInventory
        from tests import test_azure_provider as fake
        group = job['resources']['azure_resource_group_name']
        get_vm = clients['virtual_machines'].get
        clients['virtual_machines'].get = lambda group, name, **kwargs: get_vm(group, name)
        clients['resource_groups'].items[group]['properties'] = {'provisioningState': 'Succeeded'}
        for key in fake.ResourceInventory.TOP_LEVEL_KEYS:
            for value in clients[key].items.values():
                value.setdefault('properties', {})['provisioningState'] = 'Succeeded'
        for (_, name), vm in clients['virtual_machines'].items.items():
            properties = vm['properties']
            properties['vmId'] = str(uuid5(NAMESPACE_URL, name))
            properties['instanceView'] = {'statuses': [{'code': 'PowerState/running'}]}
            os_disk = properties['storageProfile']['osDisk']
            disk_id = provider.azure._expected_id(fake.SUBSCRIPTION_ID, group, 'Microsoft.Compute/disks/' + os_disk['name'])
            os_disk['managedDisk']['id'] = disk_id
            clients['disks'].items[(group, os_disk['name'])] = {
                'id': disk_id, 'name': os_disk['name'], 'location': plan.region,
                'zones': [plan.azure_zone], 'sku': {'name': 'Premium_LRS'}, 'managedBy': vm['id'],
                'properties': {'diskSizeGB': 50, 'osType': 'Linux', 'diskState': 'Attached',
                    'uniqueId': str(uuid5(NAMESPACE_URL, disk_id)), 'provisioningState': 'Succeeded'}}
            nic_name = properties['networkProfile']['networkInterfaces'][0]['id'].rsplit('/', 1)[-1]
            clients['network_interfaces'].items[(group, nic_name)]['properties']['virtualMachine'] = {'id': vm['id']}
            for disk in properties['storageProfile']['dataDisks']:
                actual = clients['disks'].items[(group, disk['name'])]
                actual['managedBy'] = vm['id']
                actual['properties'].update(diskState='Attached', uniqueId=str(uuid5(NAMESPACE_URL, actual['id'])))
        for value in clients['subnets'].items.values():
            value['properties']['provisioningState'] = 'Succeeded'
        clients['subnets'].list = lambda group, vnet: [copy.deepcopy(value)
            for (saved_group, saved_vnet, _), value in clients['subnets'].items.items()
            if (group, vnet) == (saved_group, saved_vnet)]
        clients['resources'] = AzureInventory(clients)

    def build(self, kind):
        import hashlib
        from app import deathstarbench_hotel_startup_operator as startup
        from app import deathstarbench_hotel_startup_ownership as ownership
        from scripts.deathstarbench_hotel_startup_operator_state import HotelStartupStateStore
        if kind == 'gcp':
            from tests.test_gcp_hotel_startup_ownership import strict_clients, candidate_plan, PUBLIC_KEY
            clients, timeline = strict_clients()
            raw = {**candidate_plan(), 'shape': 'n2-standard-4', 'ocpus': 4, 'memory_gb': 16}
            pin_values = {'provider': 'gcp'}
        elif kind == 'azure':
            from tests import test_azure_deathstarbench_lifecycle as az_fixture
            from tests.test_azure_provider import PUBLIC_KEY
            fixture = az_fixture.AzureDeathStarBenchLifecycleTests()
            raw, clients = fixture._distributed_plan(), fixture._distributed_clients()
            raw['shape'] = raw['azure_vm_size']
            clients['subscriptions'].get = lambda subscription, **kwargs: {
                'subscriptionId': subscription, 'tenantId': clients['subscriptions'].accounts[0]['tenantId'], 'state': 'Enabled'}
            pin_values = {'provider': 'azure'}
            timeline = []
        else:
            from tests.test_oci_hotel_startup_ownership import StrictCloud, INPUTS
            native = StrictCloud()
            clients, timeline = native.clients, native.calls
            self.native = native
            PUBLIC_KEY = INPUTS['public_key']
            raw = {**plan_values('oci'), 'region': INPUTS['region'], 'shape': INPUTS['shape'],
                   'ocpus': 4, 'memory_gb': 32, 'availability_domain': INPUTS['availability_domain']}
            pin_values = {'provider': 'oci', 'profile': 'DEFAULT', 'compartment_id': INPUTS['compartment_id'],
                'availability_domain': INPUTS['availability_domain'], 'region': INPUTS['region'],
                'application_shape': INPUTS['shape'], 'application_architecture': 'x86_64',
                'application_vcpus': 4, 'application_memory_gib': 32, 'application_image_id': INPUTS['application_image_id'],
                'support_image_id': INPUTS['support_image_id'], 'defined_tags': {},
                'ssh_public_key_sha256': provider.legacy_pins._public_key_digest(PUBLIC_KEY)}
        plan = BenchmarkPlan.model_validate({**raw, 'ssh_private_key': 'offline-only', 'ssh_public_key': PUBLIC_KEY})
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        store = HotelStartupStateStore(Path(temporary.name) / 'private')
        (store.root / RUN).mkdir(mode=0o700)
        lease = store.acquire(RUN)
        self.addCleanup(lease.release)
        saved = plan.model_dump(exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'})
        pin = copy.deepcopy(self.pin)
        pin.update(infrastructure_plan=saved, provider_pin=pin_values, ssh_public_key_sha256=hashlib.sha256(PUBLIC_KEY.encode()).hexdigest())
        pin['fingerprint'] = support._fingerprint({k: v for k, v in pin.items() if k != 'fingerprint'})
        initialization_pin = hotel.make_hotel_initialization_pin(RUN, pin, self.guarded_artifacts, attempt_id=INITIALIZATION)
        startup_pin = startup.make_hotel_startup_pin(RUN, pin, initialization_pin, self.startup_artifacts, startup_attempt_id=STARTUP)
        job = {'id': RUN, '_persist_state': False, '_public_key': PUBLIC_KEY, 'results': [], 'plan': saved, 'resources': {},
               support.PIN_KEY: pin, hotel.PIN_KEY: initialization_pin, startup.PIN_KEY: startup_pin}
        job[ownership.CHECKPOINT_KEY] = ownership.make_hotel_startup_ownership_checkpoint(job, support_pin=pin,
            initialization_pin=initialization_pin, startup_pin=startup_pin)
        store.publish_pin(pin, lease)
        store.publish_hotel_pin(initialization_pin, lease)
        store.publish_startup_pin(startup_pin, lease)
        store.save(job, lease)
        cloud = provider.CandidateCloud(plan, pin_values, persist=lambda value: store.save(value, lease), emit=lambda *_: None,
                                        operator_journal_key='hotel_startup_operator')
        if kind == 'oci':
            cloud._clients = clients
        else:
            module = getattr(provider, kind)
            runtime = module._runtime
            patch = mock.patch.object(module, '_runtime', side_effect=lambda selected, credential=None, supplied=None: runtime(selected, credential, supplied or clients))
            patch.start()
            self.addCleanup(patch.stop)
        return job, cloud, store, lease, clients, timeline

    def test_gcp_sealed_names_survive_clear_and_independent_absence_uses_all_twenty(self):
        job, cloud, store, lease, clients, timeline = self.build('gcp')
        cloud.provision(job, lease)
        from tests.test_deathstarbench_candidate_cloud_audit import ExactImages
        for disk in clients['disks'].items.values():
            disk['status'] = 'READY'
        resources = job['resources']
        clients['disks'].items[resources['gcp_dsb_database_data_disk_name']]['users'] = [
            clients['instances'].items[resources['gcp_dsb_database_instance_name']]['self_link']]
        clients['images'] = ExactImages(clients['images'].images)
        self.audit_and_record(job, cloud, store, lease)
        observed = []
        provider.gcp.prepare_distributed_candidate_cleanup(job)
        probe = self.seal_current(job, store, lease)
        original_stop = clients['instances'].stop
        def stop(request):
            observed.append(store.cleanup_ownership(RUN, lease)['fingerprint'])
            return original_stop(request)
        clients['instances'].stop = stop
        self.assertTrue(provider.gcp.quiesce_distributed_deathstarbench_load_generator(probe, clients=clients))
        self.assertEqual(len(observed), 1)
        for name in ('networks', 'subnetworks', 'routers', 'firewalls', 'instances', 'disks'):
            service = clients[name]
            original = service.delete
            def delete(request, *, original=original):
                observed.append(store.cleanup_ownership(RUN, lease)['fingerprint'])
                return original(request)
            service.delete = delete
        result = cloud.destroy_and_prove(job, lease)
        self.assertTrue(observed)
        self.assertEqual(len(result['provider_absence']['expected_names']), 20)
        self.assertEqual(len(store.cleanup_ownership(RUN, lease)['absence_targets']), 20)
        (store.root / RUN / 'state.json').write_bytes(b'corrupt history')
        loaded = store.load_for_destroy(RUN, lease)
        self.assertTrue(cloud.destroy_and_prove(loaded, lease)['independent_absence_proved'])

    def test_oci_implicit_snapshots_are_durable_before_real_mutation(self):
        job, cloud, store, lease, clients, timeline = self.build('oci')
        cloud.provision(job, lease)
        self.audit_and_record(job, cloud, store, lease)
        events = []
        def mutation(name):
            events.append(store.cleanup_ownership(RUN, lease)['fingerprint'])
        self.native.on_mutation = mutation
        provider.oci.prepare_distributed_candidate_cleanup(job, clients=clients)
        probe = self.seal_current(job, store, lease)
        original_action = clients['compute'].instance_action
        def action(identity, value, **kwargs):
            mutation('instance_action')
            return original_action(identity, value, **kwargs)
        clients['compute'].instance_action = action
        self.assertTrue(provider.oci.quiesce_distributed_deathstarbench_load_generator(probe, clients=clients, ownership_snapshot='startup-v1'))
        self.assertEqual(len(events), 1)
        self.assertTrue(cloud.destroy_and_prove(job, lease)['cleanup_complete'])
        self.assertTrue(events)
        self.assertGreater(len(store.cleanup_ownership(RUN, lease)['known_identities']), 21)

    def test_azure_seals_before_actual_deallocation_and_group_delete_then_proves_containment(self):
        from app import deathstarbench_hotel_startup_ownership as ownership
        from tests.test_azure_provider import Operation
        job, cloud, store, lease, clients, timeline = self.build('azure')
        cloud.provision(job, lease)
        self.azure_ready(job, cloud.plan, clients)
        self.audit_and_record(job, cloud, store, lease)
        checkpoint = store.checkpoint(RUN, lease)
        incarnations = {key: row for key, row in checkpoint['known_identities'].items() if key.endswith('_incarnation')}
        self.assertEqual(len(incarnations), 11)
        self.assertFalse(set(incarnations) & set(checkpoint['absence_targets']))
        self.assertTrue(set(incarnations) <= set(checkpoint['audits'][0]['known_identities']))
        events = []
        vm = next(iter(clients['virtual_machines'].items.values()))
        original_uuid = vm['properties']['vmId']
        vm['properties']['vmId'] = 'ffffffff-ffff-4fff-8fff-ffffffffffff'
        with self.assertRaises(ValueError):
            cloud.destroy_and_prove(job, lease)
        self.assertFalse((store.root / RUN / 'hotel-startup-cleanup-ownership.json').exists())
        self.assertEqual(clients['resource_groups'].delete_calls, [])
        vm['properties']['vmId'] = original_uuid
        # Exercise native deallocation explicitly, without teaching startup
        # history loading to restore a performance execution journal.
        provider.azure.prepare_distributed_candidate_cleanup(job)
        store.save_cleanup_progress(job, lease, finalizing=True)
        seal = ownership.make_hotel_startup_cleanup_ownership(job, support_pin=job[support.PIN_KEY],
            initialization_pin=job[hotel.PIN_KEY], startup_pin=job['hotel_startup_pin'])
        store.publish_cleanup_ownership(RUN, seal, lease)
        def deallocate(group, name):
            events.append(('deallocate', store.cleanup_ownership(RUN, lease)['fingerprint']))
            return Operation()
        clients['virtual_machines'].begin_deallocate = deallocate
        manifest, _ = provider.azure._load_topology_inventory_contract(job['resources'])
        provider.azure._best_effort_deallocate_distributed_load_generator(clients,
            {**job['resources'], provider.azure.DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {'state': 'measurement_started'}},
            subscription_id=job['resources']['azure_subscription_id'],
            resource_group=job['resources']['azure_resource_group_name'], manifest=manifest)
        original = clients['resource_groups'].begin_delete
        def delete(name):
            events.append(('delete', store.cleanup_ownership(RUN, lease)['fingerprint']))
            return original(name)
        clients['resource_groups'].begin_delete = delete
        result = cloud.destroy_and_prove(job, lease)
        self.assertEqual([event[0] for event in events], ['deallocate', 'delete'])
        self.assertEqual(set(store.cleanup_ownership(RUN, lease)['absence_targets']), {'azure_resource_group'})
        self.assertEqual(result['provider_absence']['absence_passes'], 2)


if __name__ == '__main__':
    unittest.main()
