"""Synthetic SDK graph tests; these do not contact or qualify a live cloud."""

import copy
import json
import sys
import unittest
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid5

from app.models import BenchmarkPlan
from app.providers import azure, gcp
from scripts import deathstarbench_candidate_cloud_audit as audit
from tests import test_azure_deathstarbench_lifecycle as azure_lifecycle
from tests import test_azure_provider as azure_fakes
from tests import test_gcp_provider as gcp_fakes

# The existing lifecycle fixture uses its historical discovery-mode import.
sys.modules.setdefault('test_gcp_provider', gcp_fakes)
from tests import test_gcp_deathstarbench_lifecycle as gcp_lifecycle  # noqa: E402


RUN_ID = '0123456789ab'
SSH_FIELDS = {'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'}


class ReadOnlyService:
    """Every call made during an audit must be an SDK read, not a fake write."""

    ALLOWED = {'get', 'list', 'list_by_resource_group'}

    def __init__(self, service, name, calls):
        self.service, self.name, self.calls = service, name, calls

    def __getattr__(self, key):
        value = getattr(self.service, key)
        if not callable(value):
            return value
        if key not in self.ALLOWED:
            def forbidden(*args, **kwargs):
                raise AssertionError(f'Audit attempted mutation {self.name}.{key}')
            return forbidden
        def read(*args, **kwargs):
            self.calls.append((self.name, key, copy.deepcopy(args), copy.deepcopy(kwargs)))
            # Legacy in-memory Azure fake ignores SDK expand. Its stored VM
            # fixture already contains the requested instance-view response.
            if self.name == 'virtual_machines':
                kwargs.pop('expand', None)
            return value(*args, **kwargs)
        return read


def _wrap(clients):
    calls = []
    return {key: ReadOnlyService(value, key, calls) for key, value in clients.items()}, calls


def _model(raw):
    raw = dict(raw)
    if raw.get('provider') == 'azure':
        raw['shape'] = raw['azure_vm_size']
    return BenchmarkPlan.model_validate({**raw, 'ssh_private_key': 'private-test-only',
                                        'ssh_public_key': azure_fakes.PUBLIC_KEY})


class AzureInventory:
    def __init__(self, clients):
        self.clients = clients
        self.foreign = []

    def list_by_resource_group(self, group):
        return [copy.deepcopy(value) for key in azure_fakes.ResourceInventory.TOP_LEVEL_KEYS
                for (saved_group, _), value in self.clients[key].items.items()
                if saved_group == group] + copy.deepcopy(self.foreign)


def azure_fixture(on_persist=None):
    fixture = azure_lifecycle.AzureDeathStarBenchLifecycleTests()
    raw = fixture._distributed_plan()
    plan = _model(raw)
    clients = fixture._distributed_clients()
    clients['subscriptions'].get = lambda subscription, **kwargs: {
        'subscriptionId': subscription,
        'tenantId': clients['subscriptions'].accounts[0]['tenantId'], 'state': 'Enabled'}
    job = {'id': RUN_ID, 'resources': {}, 'plan': plan.model_dump(exclude=SSH_FIELDS)}
    azure.provision_distributed_deathstarbench_candidate(job, plan,
        public_key=azure_fakes.PUBLIC_KEY, clients=clients, persist=on_persist)
    group = job['resources']['azure_resource_group_name']
    clients['resource_groups'].items[group]['properties'] = {'provisioningState': 'Succeeded'}
    for key in azure_fakes.ResourceInventory.TOP_LEVEL_KEYS:
        for value in clients[key].items.values():
            value.setdefault('properties', {})['provisioningState'] = 'Succeeded'
    for (_, name), vm in clients['virtual_machines'].items.items():
        properties = vm['properties']
        properties['vmId'] = str(uuid5(NAMESPACE_URL, name))
        properties['instanceView'] = {'statuses': [{'code': 'ProvisioningState/succeeded'}, {'code': 'PowerState/running'}]}
        os_profile = properties['storageProfile']['osDisk']
        disk_id = azure._expected_id(azure_fakes.SUBSCRIPTION_ID, group,
                                    'Microsoft.Compute/disks/' + os_profile['name'])
        os_profile['managedDisk']['id'] = disk_id
        clients['disks'].items[(group, os_profile['name'])] = {
            'id': disk_id, 'name': os_profile['name'], 'location': plan.region,
            'zones': [plan.azure_zone], 'sku': {'name': 'Premium_LRS'}, 'managedBy': vm['id'],
            'properties': {'diskSizeGB': 50, 'osType': 'Linux', 'diskState': 'Attached',
                           'uniqueId': str(uuid5(NAMESPACE_URL, disk_id)), 'provisioningState': 'Succeeded'}}
        nic_id = properties['networkProfile']['networkInterfaces'][0]['id']
        nic_name = nic_id.rsplit('/', 1)[-1]
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
    return job, plan, clients


class ExactImages:
    def __init__(self, images):
        self.images = copy.deepcopy(images)

    def get(self, request):
        if request['project'] != gcp.IMAGE_PROJECT:
            raise gcp_fakes.NotFound('wrong image project')
        for image in self.images.values():
            if image['name'] == request['image']:
                return copy.deepcopy(image)
        raise gcp_fakes.NotFound('image')


def gcp_fixture(on_persist=None):
    clients, _ = gcp_lifecycle.candidate_clients()
    raw = gcp_lifecycle.candidate_plan()
    raw.update(shape='n2-standard-4', ocpus=4, memory_gb=16)
    plan = _model(raw)
    job = {'id': RUN_ID, 'resources': {}, 'plan': plan.model_dump(exclude=SSH_FIELDS)}
    gcp.provision_distributed_deathstarbench_candidate(job, plan,
        public_key=gcp_fakes.PUBLIC_KEY, clients=clients, persist=on_persist)
    for disk in clients['disks'].items.values():
        disk['status'] = 'READY'
    database_name = job['resources']['gcp_dsb_database_instance_name']
    clients['disks'].items[job['resources']['gcp_dsb_database_data_disk_name']]['users'] = [
        clients['instances'].items[database_name]['self_link']]
    clients['images'] = ExactImages(clients['images'].images)
    return job, plan, clients


class CandidateCloudAuditTests(unittest.TestCase):
    def test_known_provider_keys_accept_every_partial_writer_and_cleanup_transition(self):
        for make, provider in ((azure_fixture, azure), (gcp_fixture, gcp)):
            with self.subTest(provider=provider.__name__):
                seen = []
                def persisted(job):
                    audit.require_known_candidate_resource_keys(job)
                    seen.append(copy.deepcopy(job['resources']))
                job, _, raw = make(on_persist=persisted)
                self.assertGreater(len(seen), 20)
                # Synthetic fixture teardown executes real provider writers,
                # never a real SDK/cloud. Include deletion request IDs and
                # response-loss lifecycle suffixes not present in ready state.
                provider.destroy_resources(job, clients=raw, persist=persisted)
                self.assertGreater(len(seen), 25)

    def test_unknown_provider_metadata_refused_before_any_sdk_call(self):
        cases = (
            (azure_fixture, audit.audit_azure_ready, (
                'azure_foreign_resource', 'azure_dsb_application_instance_backup_volume_id',
                'azure_peer_instance_id', 'azure_dsb_database_instance_public_ip_id',
                'azure_dsb_application_instance_os_disk_id', 'gcp_project_id')),
            (gcp_fixture, audit.audit_gcp_ready, (
                'gcp_foreign_resource', 'gcp_dsb_application_instance_backup_volume_id',
                'gcp_peer_instance_id', 'gcp_dsb_router_foreign_request_id', 'azure_subscription_id')),
        )
        for make, ready, fields in cases:
            for field in fields:
                with self.subTest(field=field):
                    job, plan, raw = make()
                    job['resources'][field] = {'id': 'do-not-forget'}
                    before = copy.deepcopy(job)
                    clients, calls = _wrap(raw)
                    with self.assertRaisesRegex(ValueError, 'unknown provider resource metadata'):
                        ready(job, plan, clients=clients)
                    self.assertEqual(calls, [])
                    self.assertEqual(job, before)

    def test_non_provider_unknown_keys_are_preserved_not_authorized(self):
        for make in (azure_fixture, gcp_fixture):
            job, _, _ = make()
            job['resources']['unknown_foreign_resource'] = {'id': 'preserve'}
            audit.require_known_candidate_resource_keys(job)
            self.assertEqual(job['resources']['unknown_foreign_resource'], {'id': 'preserve'})

    def test_azure_fresh_subscription_sdk_get_retains_tenant_and_cannot_hide_404(self):
        from azure.core.credentials import AccessToken
        from azure.core.pipeline.transport import HttpResponse, HttpTransport
        from azure.mgmt.subscription import SubscriptionClient

        class Response(HttpResponse):
            def __init__(self, request, payload):
                super().__init__(request, None)
                self.status_code = 200
                self.headers = {'content-type': 'application/json'}
                self.payload = payload

            def body(self):
                return json.dumps(self.payload).encode()

        class Transport(HttpTransport):
            def __init__(self, payload):
                self.payload, self.calls = payload, []

            def open(self):
                pass

            def close(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.close()

            def send(self, request, **kwargs):
                self.calls.append((request.method, request.url))
                return Response(request, self.payload)

        job, plan, raw = azure_fixture()
        transport = Transport({'subscriptionId': azure_fakes.SUBSCRIPTION_ID, 'tenantId': 'tenant-1', 'state': 'Enabled'})
        credential = SimpleNamespace(get_token=lambda *args, **kwargs: AccessToken('synthetic-offline-token', 4102444800))
        with SubscriptionClient(credential, transport=transport) as sdk:
            raw['subscription_audit'] = sdk.subscriptions
            audit.audit_azure_ready(job, plan, clients=_wrap(raw)[0])
            self.assertTrue(transport.calls)
            self.assertTrue(all(method == 'GET' and 'api-version=2022-12-01' in url for method, url in transport.calls))
            transport.payload['tenantId'] = 'different-live-tenant'
            with self.assertRaisesRegex(ValueError, 'fresh Azure subscription/tenant'):
                audit.audit_azure_ready(job, plan, clients=_wrap(raw)[0])
        raw.pop('subscription_audit')
        raw['resource_groups'].items.clear()
        def inaccessible(*args, **kwargs):
            raise azure_fakes.NotFound('subscription not accessible')
        raw['subscriptions'].get = inaccessible
        with self.assertRaises(azure_fakes.NotFound):
            audit.audit_azure_absent(job, clients=_wrap(raw)[0])

    def test_azure_real_sdk_models_match_wire_and_flattened_properties(self):
        from azure.mgmt.compute import models as compute_models
        from azure.mgmt.network import models as network_models

        job, plan, raw = azure_fixture()
        baseline = audit.audit_azure_ready(job, plan, clients=_wrap(raw)[0])
        classes = {
            'virtual_machines': compute_models.VirtualMachine,
            'disks': compute_models.Disk,
            'network_interfaces': network_models.NetworkInterface,
            'network_security_groups': network_models.NetworkSecurityGroup,
            'virtual_networks': network_models.VirtualNetwork,
            'public_ip_addresses': network_models.PublicIPAddress,
            'nat_gateways': network_models.NatGateway,
            'subnets': network_models.Subnet,
        }
        # Simulate deserialized GET replies rather than only JSON mappings.
        for name, cls in classes.items():
            service = raw[name]
            original = service.get
            service.get = lambda *args, _read=original, _cls=cls, **kwargs: _cls(_read(*args, **kwargs))
        result = audit.audit_azure_ready(job, plan, clients=_wrap(raw)[0])
        self.assertEqual(result, baseline)

    def test_azure_complete_graph_stable_identity_and_no_mutations(self):
        job, plan, raw = azure_fixture()
        before = copy.deepcopy(job)
        cloud_before = {key: copy.deepcopy(value.items) for key, value in raw.items() if hasattr(value, 'items') and not callable(value.items)}
        clients, calls = _wrap(raw)
        result = audit.audit_azure_ready(job, plan, clients=clients)
        second = audit.audit_azure_ready(job, plan, clients=clients)
        self.assertEqual(result, second)
        self.assertEqual(result['cloud_identity_sha256'], audit._sha(result['cloud_identity']))
        self.assertEqual(len(result['roles']), 5)
        self.assertEqual(len(result['resources']), 26)
        self.assertTrue(all(result['roles'][role]['unique_id'] for role in result['roles']))
        self.assertFalse(result['public_ready'])
        self.assertEqual(job, before)
        self.assertEqual(cloud_before, {key: copy.deepcopy(value.items) for key, value in raw.items() if hasattr(value, 'items') and not callable(value.items)})
        self.assertTrue(calls)
        self.assertTrue(all(method in ReadOnlyService.ALLOWED for _, method, _, _ in calls))
        self.assertTrue(any(name == 'virtual_machines' and kwargs.get('expand') == 'instanceView' for name, _, _, kwargs in calls))

    def test_gcp_complete_graph_stable_identity_and_no_mutations(self):
        job, plan, raw = gcp_fixture()
        before = copy.deepcopy(job)
        cloud_before = {key: copy.deepcopy(value.items) for key, value in raw.items()
                        if isinstance(value, gcp_fakes.ResourceService)}
        clients, calls = _wrap(raw)
        result = audit.audit_gcp_ready(job, plan, clients=clients)
        self.assertEqual(result, audit.audit_gcp_ready(job, plan, clients=clients))
        self.assertEqual(len(result['resources']), 20)
        self.assertEqual(len(result['roles']), 5)
        self.assertFalse(result['measurement_qualified'])
        self.assertEqual(result['cloud_identity_sha256'], audit._sha(result['cloud_identity']))
        self.assertEqual(job, before)
        self.assertEqual(cloud_before, {key: copy.deepcopy(value.items) for key, value in raw.items()
                                       if isinstance(value, gcp_fakes.ResourceService)})
        self.assertTrue(all(method == 'get' for _, method, _, _ in calls))

    def test_exact_saved_plan_and_source_guard(self):
        for make, function in ((azure_fixture, audit.audit_azure_ready), (gcp_fixture, audit.audit_gcp_ready)):
            with self.subTest(provider=function.__name__):
                job, plan, raw = make()
                clients, _ = _wrap(raw)
                job['plan']['deathstarbench']['request_rate'] += 1
                with self.assertRaisesRegex(ValueError, 'exact saved plan'):
                    function(job, plan, clients=clients)
                job['plan'] = plan.model_dump(exclude=SSH_FIELDS)
                job['resources'][audit.DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY] = {'state': 'ready'}
                with self.assertRaisesRegex(ValueError, 'journal'):
                    function(job, plan, clients=clients)

    def test_azure_missing_unready_foreign_and_drift_fail_closed(self):
        def change_vm(raw, job, field, value):
            name = job['resources']['azure_dsb_application_instance_name']
            raw['virtual_machines'].items[(job['resources']['azure_resource_group_name'], name)]['properties'][field] = value
        mutations = [
            lambda raw, job: change_vm(raw, job, 'instanceView', {'statuses': [{'code': 'PowerState/deallocated'}]}),
            lambda raw, job: change_vm(raw, job, 'vmId', '00000000-0000-0000-0000-000000000000'),
            lambda raw, job: raw['subscriptions'].accounts.clear(),
            lambda raw, job: raw['subscriptions'].accounts[0].update(tenantId='other'),
            lambda raw, job: raw['resources'].foreign.append({'id': '/foreign'}),
            lambda raw, job: raw['disks'].items.clear(),
            lambda raw, job: job['resources'].update(private_ip='10.240.1.99'),
        ]
        for number, mutation in enumerate(mutations):
            with self.subTest(case=number):
                job, plan, raw = azure_fixture()
                mutation(raw, job)
                clients, _ = _wrap(raw)
                with self.assertRaises((ValueError, RuntimeError)):
                    audit.audit_azure_ready(job, plan, clients=clients)

    def test_gcp_missing_unready_foreign_and_drift_fail_closed(self):
        def disk(raw, job):
            return raw['disks'].items[job['resources']['gcp_dsb_database_data_disk_name']]
        mutations = [
            lambda raw, job: raw['projects'].__dict__.update(get_value={'id': '123'}),
            lambda raw, job: raw['instances'].items[job['resources']['gcp_dsb_application_instance_name']].update(status='TERMINATED'),
            lambda raw, job: raw['networks'].items.clear(),
            lambda raw, job: disk(raw, job).update(users=[]),
            lambda raw, job: disk(raw, job).update(status='CREATING'),
            lambda raw, job: disk(raw, job).update(labels={'managed-by': 'someone-else'}),
            lambda raw, job: job['resources'].update(public_ip='198.51.100.222'),
            lambda raw, job: next(iter(raw['images'].images.values())).update(id='9002'),
            lambda raw, job: next(iter(raw['images'].images.values())).update(status='DEPRECATED'),
        ]
        for number, mutation in enumerate(mutations):
            with self.subTest(case=number):
                job, plan, raw = gcp_fixture()
                mutation(raw, job)
                clients, _ = _wrap(raw)
                with self.assertRaises((ValueError, RuntimeError)):
                    audit.audit_gcp_ready(job, plan, clients=clients)

    def test_azure_opaque_identity_is_pinned_for_stopped_cleanup_and_replacements_refused(self):
        job, plan, raw = azure_fixture()
        clients, _ = _wrap(raw)
        pinned = audit.audit_azure_ready(job, plan, clients=clients)['cloud_identity']
        vm = next(iter(raw['virtual_machines'].items.values()))
        vm['properties']['instanceView']['statuses'] = [{'code': 'PowerState/deallocated'}]
        result = audit.require_azure_cleanup_identity(job, pinned, clients=clients)
        self.assertTrue(result['cleanup_identity_matches'])
        vm['properties']['vmId'] = str(uuid5(NAMESPACE_URL, 'replacement'))
        with self.assertRaisesRegex(ValueError, 'replaced'):
            audit.require_azure_cleanup_identity(job, pinned, clients=clients)

    def test_azure_new_same_name_vm_or_disk_changes_identity_even_with_matching_arm_ids(self):
        for kind, key in (('virtual_machines', 'vmId'), ('disks', 'uniqueId')):
            with self.subTest(kind=kind):
                job, plan, raw = azure_fixture()
                clients, _ = _wrap(raw)
                first = audit.audit_azure_ready(job, plan, clients=clients)
                next(iter(raw[kind].items.values()))['properties'][key] = str(uuid5(NAMESPACE_URL, 'replacement'))
                second = audit.audit_azure_ready(job, plan, clients=clients)
                self.assertNotEqual(first['cloud_identity_sha256'], second['cloud_identity_sha256'])
                with self.assertRaisesRegex(ValueError, 'replaced'):
                    audit.require_azure_cleanup_identity(job, first['cloud_identity'], clients=clients)

    def test_azure_unexpected_subnet_or_boolean_confusion_rejected(self):
        job, plan, raw = azure_fixture()
        group = job['resources']['azure_resource_group_name']
        vnet = job['resources']['azure_dsb_vnet_name']
        raw['subnets'].items[(group, vnet, 'foreign')] = {'id': '/foreign', 'name': 'foreign'}
        with self.assertRaisesRegex(ValueError, 'subnets'):
            audit.audit_azure_ready(job, plan, clients=_wrap(raw)[0])
        raw['subnets'].items.pop((group, vnet, 'foreign'))
        next(iter(raw['network_interfaces'].items.values()))['properties']['enableIPForwarding'] = 0
        with self.assertRaisesRegex(ValueError, 'boolean'):
            audit.audit_azure_ready(job, plan, clients=_wrap(raw)[0])

    def test_gcp_saved_immutable_id_and_disk_profile_and_image_source_rejected(self):
        cases = [
            ('gcp_dsb_network', lambda item: item.update(id='99999')),
            ('gcp_dsb_database_data_disk', lambda item: item.update(size_gb=101)),
            ('gcp_dsb_application_boot_disk', lambda item: item.pop('source_image', None)),
            ('gcp_dsb_application_instance', lambda item: item['network_interfaces'][0]['access_configs'][0].update(nat_ip='198.51.100.200')),
        ]
        for prefix, mutate in cases:
            with self.subTest(prefix=prefix):
                job, plan, raw = gcp_fixture()
                service, _, _ = gcp._resource_get_spec(prefix, job['resources'], raw)
                mutate(service.items[job['resources'][prefix + '_name']])
                with self.assertRaises((ValueError, RuntimeError)):
                    audit.audit_gcp_ready(job, plan, clients=_wrap(raw)[0])

    def test_gcp_saved_and_live_profile_drift_cannot_override_topology(self):
        job, plan, raw = gcp_fixture()
        prefix = 'gcp_dsb_database_data_disk'
        job['resources'][prefix + '_size_gb'] = 200
        raw['disks'].items[job['resources'][prefix + '_name']]['size_gb'] = 200
        with self.assertRaisesRegex(ValueError, 'differs from topology'):
            audit.audit_gcp_ready(job, plan, clients=_wrap(raw)[0])

    def test_azure_cleanup_missing_children_allowed_not_missing_identity(self):
        job, plan, raw = azure_fixture()
        clients, _ = _wrap(raw)
        pinned = audit.audit_azure_ready(job, plan, clients=clients)['cloud_identity']
        raw['virtual_machines'].items.clear()
        raw['disks'].items.clear()
        result = audit.require_azure_cleanup_identity(job, pinned, clients=clients)
        self.assertTrue(all(not item['present'] for item in result['resources']))
        pinned['resources'][0]['id'] = '/foreign'
        with self.assertRaisesRegex(ValueError, 'ARM identity'):
            audit.require_azure_cleanup_identity(job, pinned, clients=clients)

    def test_azure_absence_requires_two_gets_and_original_scope(self):
        job, _, raw = azure_fixture()
        raw['resource_groups'].items.clear()
        clients, calls = _wrap(raw)
        before = copy.deepcopy(job)
        result = audit.audit_azure_absent(job, clients=clients)
        self.assertEqual(result['absence_passes'], 2)
        self.assertEqual(len([call for call in calls if call[0] == 'resource_groups']), 2)
        self.assertEqual(job, before)
        raw['subscriptions'].accounts[0]['tenantId'] = 'wrong'
        with self.assertRaisesRegex(ValueError, 'tenant'):
            audit.audit_azure_absent(job, clients=clients)

    def test_gcp_absence_reads_original_deterministic_names_after_partial_creation(self):
        job, _, raw = gcp_fixture()
        for service in raw.values():
            if isinstance(service, gcp_fakes.ResourceService):
                service.items.clear()
        # Flat identity wasn't saved after the initial request, but the
        # pre-request atomic deterministic allowlist still establishes scope.
        for prefix in gcp.DISTRIBUTED_RESOURCE_PREFIXES:
            job['resources'].pop(prefix + '_name', None)
            job['resources'].pop(prefix + '_id', None)
            job['resources'].pop(prefix + '_self_link', None)
        before = copy.deepcopy(job)
        clients, calls = _wrap(raw)
        result = audit.audit_gcp_absent(job, clients=clients)
        self.assertEqual(result['absence_passes'], 2)
        self.assertEqual(len(calls), 41)  # Project plus 20 resources twice.
        self.assertEqual(job, before)

    def test_absence_presence_replacement_and_inaccessible_are_not_absent(self):
        for make, function, key in ((azure_fixture, audit.audit_azure_absent, 'resource_groups'),
                                    (gcp_fixture, audit.audit_gcp_absent, 'networks')):
            with self.subTest(provider=function.__name__):
                job, _, raw = make()
                clients, _ = _wrap(raw)
                with self.assertRaisesRegex(ValueError, 'still exists'):
                    function(job, clients=clients)
                if key == 'networks':
                    for service in raw.values():
                        if isinstance(service, gcp_fakes.ResourceService):
                            service.items.clear()
                def denied(*args, **kwargs):
                    raise azure_fakes.ApiError(403, 'denied')
                raw[key].get = denied
                with self.assertRaises(azure_fakes.ApiError):
                    function(job, clients=clients)


if __name__ == '__main__':
    unittest.main()
