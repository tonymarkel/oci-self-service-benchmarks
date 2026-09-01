import base64
import copy
import unittest

from app.providers import azure


SUBSCRIPTION_ID = '11111111-2222-3333-4444-555555555555'
PUBLIC_KEY = (
    'ssh-ed25519 '
    'AAAAC3NzaC1lZDI1NTE5AAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
)


class NotFound(Exception):
    status_code = 404


class ApiError(Exception):
    def __init__(self, status_code, message='api error'):
        super().__init__(message)
        self.status_code = status_code


class Operation:
    def __init__(self, *, callback=None, error=None):
        self.callback = callback
        self.error = error

    def result(self):
        result = self.callback() if self.callback else None
        if self.error:
            raise self.error
        return result


class Subscriptions:
    def __init__(self):
        self.accounts = [{
            'id': SUBSCRIPTION_ID,
            'name': 'Benchmark Subscription',
            'tenantId': 'tenant-1',
            'state': 'Enabled',
            'isDefault': True,
        }]
        self.locations = [
            {'name': 'eastus2', 'region_type': 'Physical'},
            {'name': 'westus3', 'metadata': {'regionType': 'Physical'}},
            {'name': 'edge-site', 'region_type': 'Logical'},
        ]

    def list(self):
        return copy.deepcopy(self.accounts)

    def list_locations(self, subscription_id):
        if subscription_id != SUBSCRIPTION_ID:
            raise NotFound(subscription_id)
        return copy.deepcopy(self.locations)


class Providers:
    def __init__(self):
        self.states = {
            'Microsoft.Compute': 'Registered',
            'Microsoft.Network': 'Registered',
        }

    def get(self, namespace):
        return {'registration_state': self.states[namespace]}


def capability(name, value):
    return {'name': name, 'value': str(value)}


def vm_sku(
    name,
    architecture,
    vcpus,
    memory_gb,
    *,
    zones=('1', '2'),
    restrictions=(),
    premium_v2=True,
    gpus=0,
):
    return {
        'name': name,
        'resource_type': 'virtualMachines',
        'tier': 'Standard',
        'family': name.split('_', 1)[-1].split('_', 1)[0],
        'locations': ['eastus2'],
        'location_info': [{'location': 'eastus2', 'zones': list(zones)}],
        'capabilities': [
            capability('CpuArchitectureType', architecture),
            capability('vCPUs', vcpus),
            capability('MemoryGB', memory_gb),
            capability('GPUs', gpus),
            capability('PremiumIO', 'True'),
            capability('PremiumIOV2Supported', str(premium_v2)),
            capability('AcceleratedNetworkingEnabled', 'True'),
            capability('MaxDataDiskCount', 8),
            capability('MaxNetworkBandwidth', 12500),
        ],
        'restrictions': list(restrictions),
    }


class ResourceSkus:
    def __init__(self):
        self.calls = []
        self.items = [
            vm_sku('Standard_D8pls_v6', 'Arm64', 8, 16),
            vm_sku('Standard_D2as_v7', 'x64', 2, 8),
            vm_sku(
                'Standard_D4s_v6', 'x64', 4, 16,
                restrictions=[{
                    'type': 'Location',
                    'values': ['eastus2'],
                    'reason_code': 'QuotaId',
                }],
            ),
            vm_sku('Standard_NC4as_T4_v3', 'x64', 4, 28, gpus=1),
            {
                'name': 'PremiumV2_LRS',
                'resource_type': 'disks',
                'locations': ['eastus2'],
                'location_info': [{
                    'location': 'eastus2',
                    'zones': ['1', '2'],
                }],
                'capabilities': [],
                'restrictions': [],
            },
        ]

    def list(self, filter=None):
        self.calls.append(filter)
        return copy.deepcopy(self.items)


class Images:
    def __init__(self):
        self.list_calls = []
        self.get_calls = []
        self.versions = {
            'rockylinux-x86_64': ['9.5.20250101', '9.5.20260115'],
            'rockylinux-aarch64': ['9.5.20250102', '9.5.20260201'],
        }

    def list(self, region, publisher, offer, sku):
        self.list_calls.append((region, publisher, offer, sku))
        return [{'name': value} for value in self.versions.get(offer, ())]

    def get(self, region, publisher, offer, sku, version):
        self.get_calls.append((region, publisher, offer, sku, version))
        return {
            'id': (
                f'/subscriptions/{SUBSCRIPTION_ID}/providers/Microsoft.Compute/'
                f'locations/{region}/publishers/{publisher}/artifactTypes/'
                f'vmImage/offers/{offer}/skus/{sku}/versions/{version}'
            ),
            'name': version,
        }


class MarketplaceTerms:
    def __init__(self):
        self.accepted = True
        self.calls = []

    def show(self, urn):
        self.calls.append(urn)
        return {'accepted': self.accepted}


class ResourceGroups:
    def __init__(self):
        self.items = {}
        self.create_calls = []
        self.delete_calls = []
        self.on_create = None
        self.create_error_after_store = None
        self.delete_error = None
        self.delete_response_lost = False

    def _id(self, name):
        return f'/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{name}'

    def get(self, name):
        if name not in self.items:
            raise NotFound(name)
        return copy.deepcopy(self.items[name])

    def create_or_update(self, name, body):
        self.create_calls.append((name, copy.deepcopy(body)))
        if self.on_create:
            self.on_create(name, body)
        self.items[name] = {
            'id': self._id(name),
            'name': name,
            'location': body['location'],
            'tags': copy.deepcopy(body['tags']),
        }
        if self.create_error_after_store:
            raise self.create_error_after_store
        return copy.deepcopy(self.items[name])

    def begin_delete(self, name):
        self.delete_calls.append(name)

        def delete():
            if self.delete_response_lost:
                self.items.pop(name, None)
                raise ApiError(503, 'polling response lost')
            if self.delete_error:
                raise self.delete_error
            self.items.pop(name, None)

        return Operation(callback=delete)


class ArmResources:
    def __init__(self, resource_groups, namespace, collection):
        self.resource_groups = resource_groups
        self.namespace = namespace
        self.collection = collection
        self.items = {}
        self.calls = []
        self.counter = 0

    def _id(self, group, name):
        return (
            f'/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{group}/'
            f'providers/{self.namespace}/{self.collection}/{name}'
        )

    def begin_create_or_update(self, group, name, body):
        self.calls.append((group, name, copy.deepcopy(body)))

        def create():
            if group not in self.resource_groups.items:
                raise NotFound(group)
            self.counter += 1
            resource = copy.deepcopy(body)
            resource.update({'id': self._id(group, name), 'name': name})
            if self.collection == 'publicIPAddresses':
                resource['properties']['ipAddress'] = (
                    f'198.51.100.{20 + self.counter}'
                )
            if self.collection == 'networkInterfaces':
                configurations = copy.deepcopy(
                    body['properties']['ipConfigurations']
                )
                configurations[0]['properties']['privateIPAddress'] = (
                    f'10.42.1.{10 + self.counter}'
                )
                resource['properties']['ipConfigurations'] = configurations
            self.items[(group, name)] = resource
            return copy.deepcopy(resource)

        return Operation(callback=create)

    def get(self, group, name):
        key = (group, name)
        if key not in self.items:
            raise NotFound(name)
        return copy.deepcopy(self.items[key])


class Subnets(ArmResources):
    def __init__(self, resource_groups):
        super().__init__(
            resource_groups,
            'Microsoft.Network',
            'virtualNetworks/placeholder/subnets',
        )

    def begin_create_or_update(self, group, vnet, name, body):
        self.calls.append((group, vnet, name, copy.deepcopy(body)))

        def create():
            resource = copy.deepcopy(body)
            resource.update({
                'id': (
                    f'/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{group}/'
                    f'providers/Microsoft.Network/virtualNetworks/{vnet}/'
                    f'subnets/{name}'
                ),
                'name': name,
            })
            self.items[(group, vnet, name)] = resource
            return copy.deepcopy(resource)

        return Operation(callback=create)


class ResourceInventory:
    TOP_LEVEL_KEYS = (
        'network_security_groups',
        'virtual_networks',
        'public_ip_addresses',
        'network_interfaces',
        'disks',
        'virtual_machines',
    )

    def __init__(self, clients):
        self.clients = clients
        self.foreign = []
        self.calls = []

    def list_by_resource_group(self, group):
        self.calls.append(group)
        items = []
        for key in self.TOP_LEVEL_KEYS:
            service = self.clients[key]
            items.extend(
                copy.deepcopy(value)
                for (saved_group, _), value in service.items.items()
                if saved_group == group
            )
        # Azure materializes the named VM OS disk as a top-level managed disk.
        for (saved_group, name), vm in self.clients['virtual_machines'].items.items():
            if saved_group != group:
                continue
            disk_name = vm['properties']['storageProfile']['osDisk']['name']
            items.append({
                'id': (
                    f'/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{group}/'
                    f'providers/Microsoft.Compute/disks/{disk_name}'
                ),
                'name': disk_name,
            })
        items.extend(copy.deepcopy(self.foreign))
        return items


def fake_clients():
    groups = ResourceGroups()
    clients = {
        'subscriptions': Subscriptions(),
        'providers': Providers(),
        'resource_groups': groups,
        'resource_skus': ResourceSkus(),
        'virtual_machine_images': Images(),
        'marketplace_terms': MarketplaceTerms(),
        'network_security_groups': ArmResources(
            groups, 'Microsoft.Network', 'networkSecurityGroups'
        ),
        'virtual_networks': ArmResources(
            groups, 'Microsoft.Network', 'virtualNetworks'
        ),
        'subnets': Subnets(groups),
        'public_ip_addresses': ArmResources(
            groups, 'Microsoft.Network', 'publicIPAddresses'
        ),
        'network_interfaces': ArmResources(
            groups, 'Microsoft.Network', 'networkInterfaces'
        ),
        'disks': ArmResources(groups, 'Microsoft.Compute', 'disks'),
        'virtual_machines': ArmResources(
            groups, 'Microsoft.Compute', 'virtualMachines'
        ),
    }
    clients['resources'] = ResourceInventory(clients)
    return clients


def full_plan():
    return {
        'provider': 'azure',
        'azure_subscription_id': SUBSCRIPTION_ID,
        'region': 'eastus2',
        'azure_zone': '1',
        'azure_vm_size': 'Standard_D8pls_v6',
        'ocpus': 8,
        'memory_gb': 16,
        'benchmarks': ['sysbench', 'stream', 'fio', 'iperf3', 'apachebench'],
        'sysbench': {'workloads': ['cpu', 'fileio']},
        'iperf3': {'protocols': ['tcp', 'udp']},
        'phoronix': {'profiles': []},
        'storage': {
            'boot_size_gb': 100,
            'additional_volume': True,
            'additional_size_gb': 1024,
        },
    }


class AzureDiscoveryTests(unittest.TestCase):
    def test_value_reads_sdk_camel_case_mapping_before_attributes(self):
        class SdkModel(dict):
            registration_state = 'Wrong attribute value'

        provider = SdkModel(registrationState='Registered')

        self.assertEqual(
            azure._value(provider, 'registration_state'),
            'Registered',
        )

        restriction = SdkModel(values=['1'])
        self.assertEqual(azure._value(restriction, 'values'), ['1'])
        self.assertEqual(azure._value(SdkModel(), 'values', ()), ())

    def test_bootstrap_uses_default_subscription_and_physical_regions(self):
        clients = fake_clients()

        result = azure.bootstrap(clients=clients)

        self.assertEqual(result['subscription_id'], SUBSCRIPTION_ID)
        self.assertEqual(result['subscription_name'], 'Benchmark Subscription')
        self.assertEqual(result['tenant_id'], 'tenant-1')
        self.assertEqual(result['default_region'], 'eastus2')
        self.assertEqual(result['regions'], ['eastus2', 'westus3'])
        self.assertEqual(result['subscriptions'][0]['id'], SUBSCRIPTION_ID)

    def test_bootstrap_requires_registered_compute_and_network(self):
        clients = fake_clients()
        clients['providers'].states['Microsoft.Network'] = 'NotRegistered'

        with self.assertRaisesRegex(RuntimeError, 'Microsoft.Network'):
            azure.bootstrap(SUBSCRIPTION_ID, clients=clients)

    def test_placement_and_sizes_filter_subscription_restrictions_and_gpus(self):
        clients = fake_clients()

        placement = azure.placement(
            SUBSCRIPTION_ID, 'eastus2', clients=clients
        )
        sizes = azure.vm_sizes(
            SUBSCRIPTION_ID, 'eastus2', '1', clients=clients
        )

        self.assertEqual(
            [item['name'] for item in placement['availability_zones']],
            ['1', '2'],
        )
        self.assertEqual(
            [item['vm_size'] for item in sizes['items']],
            ['Standard_D2as_v7', 'Standard_D8pls_v6'],
        )
        arm = sizes['items'][1]
        self.assertEqual(arm['name'], 'Standard_D8pls_v6')
        self.assertEqual(arm['vcpus'], 8)
        self.assertEqual(arm['memory_gb'], 16)
        self.assertEqual(arm['architecture'], 'arm64')
        self.assertEqual(arm['network_bandwidth_gbps'], 12.5)
        self.assertTrue(all(
            call == "location eq 'eastus2'"
            for call in clients['resource_skus'].calls
        ))

    def test_documented_loadgen_bandwidth_fills_unusable_live_capability(self):
        for live_value in (None, '0', 'not-a-number'):
            with self.subTest(live_value=live_value):
                clients = fake_clients()
                loadgen_sku = next(
                    item for item in clients['resource_skus'].items
                    if item.get('name') == 'Standard_D2as_v7'
                )
                loadgen_sku['capabilities'] = [
                    item for item in loadgen_sku['capabilities']
                    if item['name'] != 'MaxNetworkBandwidth'
                ]
                if live_value is not None:
                    loadgen_sku['capabilities'].append(
                        capability('MaxNetworkBandwidth', live_value)
                    )

                sizes = azure.vm_sizes(
                    SUBSCRIPTION_ID, 'eastus2', '1', clients=clients
                )
                loadgen = next(
                    item for item in sizes['items']
                    if item['vm_size'] == 'Standard_D2as_v7'
                )

                self.assertEqual(loadgen['network_bandwidth_gbps'], 16.0)
                self.assertEqual(loadgen['network_capacity_kind'], 'maximum')

    def test_positive_live_loadgen_bandwidth_overrides_documented_fallback(self):
        clients = fake_clients()

        sizes = azure.vm_sizes(
            SUBSCRIPTION_ID, 'eastus2', '1', clients=clients
        )
        loadgen = next(
            item for item in sizes['items']
            if item['vm_size'] == 'Standard_D2as_v7'
        )

        self.assertEqual(loadgen['network_bandwidth_gbps'], 12.5)
        self.assertEqual(loadgen['network_capacity_kind'], 'maximum')

    def test_documented_bandwidth_is_not_applied_to_other_sizes(self):
        clients = fake_clients()
        arm_sku = next(
            item for item in clients['resource_skus'].items
            if item.get('name') == 'Standard_D8pls_v6'
        )
        arm_sku['capabilities'] = [
            item for item in arm_sku['capabilities']
            if item['name'] != 'MaxNetworkBandwidth'
        ]

        sizes = azure.vm_sizes(
            SUBSCRIPTION_ID, 'eastus2', '1', clients=clients
        )
        arm = next(
            item for item in sizes['items']
            if item['vm_size'] == 'Standard_D8pls_v6'
        )

        self.assertNotIn('network_bandwidth_gbps', arm)
        self.assertNotIn('network_capacity_kind', arm)

    def test_zone_restriction_is_subscription_aware(self):
        clients = fake_clients()
        clients['resource_skus'].items[0]['restrictions'] = [{
            'type': 'Zone',
            'restriction_info': {'zones': ['2']},
            'reason_code': 'NotAvailableForSubscription',
        }]

        zone_one = azure.vm_sizes(
            SUBSCRIPTION_ID, 'eastus2', '1', clients=clients
        )
        zone_two = azure.vm_sizes(
            SUBSCRIPTION_ID, 'eastus2', '2', clients=clients
        )

        self.assertIn(
            'Standard_D8pls_v6', [item['vm_size'] for item in zone_one['items']]
        )
        self.assertNotIn(
            'Standard_D8pls_v6', [item['vm_size'] for item in zone_two['items']]
        )

    def test_image_resolution_pins_latest_architecture_specific_version(self):
        clients = fake_clients()

        image = azure.resolve_rocky_linux_9_image(
            SUBSCRIPTION_ID, 'eastus2', 'aarch64', clients=clients
        )

        self.assertEqual(image['architecture'], 'arm64')
        self.assertEqual(image['offer'], 'rockylinux-aarch64')
        self.assertEqual(image['sku'], 'rockylinux-aarch64-9')
        self.assertEqual(image['version'], '9.5.20260201')
        self.assertEqual(
            image['urn'],
            'resf:rockylinux-aarch64:rockylinux-aarch64-9:9.5.20260201',
        )
        self.assertNotEqual(image['image_reference']['version'], 'latest')


class AzureProvisionTests(unittest.TestCase):
    def test_udp_only_security_rule_keeps_tcp_control_listener(self):
        rules = azure._security_rules(('udp',), ())
        by_name = {rule['name']: rule for rule in rules}

        self.assertEqual(
            by_name['allow-iperf-tcp']['properties']['protocol'], 'Tcp'
        )
        self.assertEqual(
            by_name['allow-iperf-tcp']['properties']['destinationPortRange'],
            '5201',
        )
        self.assertEqual(
            by_name['allow-iperf-udp']['properties']['protocol'], 'Udp'
        )

    def test_full_arm_plan_builds_tagged_stack_storage_peer_and_x86_loadgen(self):
        clients = fake_clients()
        loadgen_sku = next(
            item for item in clients['resource_skus'].items
            if item.get('name') == 'Standard_D2as_v7'
        )
        loadgen_sku['capabilities'] = [
            item for item in loadgen_sku['capabilities']
            if item['name'] != 'MaxNetworkBandwidth'
        ]
        job = {'id': 'abc123', 'resources': {}}
        persisted = []

        resources = azure.provision(
            job,
            full_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
            persist=lambda current: persisted.append(copy.deepcopy(current)),
        )

        self.assertEqual(resources['provider'], 'azure')
        self.assertEqual(resources['azure_resource_group_name'], 'benchmark-abc123')
        self.assertFalse(resources['azure_resource_group_create_ambiguous'])
        self.assertEqual(resources['architecture'], 'arm64')
        self.assertEqual(resources['loadgen_architecture'], 'x86_64')
        self.assertEqual(resources['loadgen_shape'], 'Standard_D2as_v7')
        self.assertEqual(resources['loadgen_network_bandwidth_gbps'], 16.0)
        self.assertEqual(resources['loadgen_network_capacity_kind'], 'maximum')
        self.assertTrue(resources['public_ip'].startswith('198.51.100.'))
        self.assertTrue(resources['private_ip'].startswith('10.42.1.'))
        self.assertTrue(resources['peer_private_ip'].startswith('10.42.1.'))
        self.assertEqual(resources['azure_data_disk_lun'], 0)
        self.assertEqual(resources['azure_data_disk_type'], 'PremiumV2_LRS')

        first_group_anchor = next(
            state['resources'] for state in persisted
            if state['resources'].get('azure_resource_group_create_ambiguous') is True
            and state['resources'].get('azure_resource_group_name')
        )
        self.assertEqual(
            first_group_anchor['azure_resource_group_expected_id'].lower(),
            (
                f'/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/'
                'benchmark-abc123'
            ).lower(),
        )

        group_call = clients['resource_groups'].create_calls[0]
        self.assertEqual(group_call[1]['tags']['managed-by'], azure.MANAGED_BY)
        pip_bodies = [call[2] for call in clients['public_ip_addresses'].calls]
        self.assertEqual(len(pip_bodies), 3)
        self.assertTrue(all(body['sku']['name'] == 'Standard' for body in pip_bodies))
        self.assertTrue(all(body['zones'] == ['1'] for body in pip_bodies))

        disk_body = clients['disks'].calls[0][2]
        self.assertEqual(disk_body['sku']['name'], 'PremiumV2_LRS')
        self.assertEqual(disk_body['properties']['diskIOPSReadWrite'], 3000)
        self.assertEqual(disk_body['properties']['diskMBpsReadWrite'], 125)
        self.assertEqual(disk_body['properties']['networkAccessPolicy'], 'DenyAll')

        vm_calls = clients['virtual_machines'].calls
        self.assertEqual(len(vm_calls), 3)
        runner_body = next(body for _, name, body in vm_calls if name.endswith('-runner'))
        loadgen_body = next(body for _, name, body in vm_calls if name.endswith('-loadgen'))
        peer_body = next(body for _, name, body in vm_calls if name.endswith('-iperf-peer'))
        self.assertEqual(
            runner_body['properties']['hardwareProfile']['vmSize'],
            'Standard_D8pls_v6',
        )
        self.assertEqual(
            loadgen_body['properties']['hardwareProfile']['vmSize'],
            'Standard_D2as_v7',
        )
        self.assertIn('rockylinux-aarch64', runner_body['plan']['product'])
        self.assertIn('rockylinux-x86_64', loadgen_body['plan']['product'])
        self.assertEqual(
            runner_body['properties']['storageProfile']['dataDisks'][0][
                'managedDisk'
            ]['id'],
            resources['azure_data_disk_id'],
        )
        peer_script = base64.b64decode(
            peer_body['properties']['osProfile']['customData']
        ).decode()
        self.assertIn('Azure benchmark iperf3 server', peer_script)

        nsg_rules = clients['network_security_groups'].calls[0][2][
            'properties'
        ]['securityRules']
        ssh_rule = next(rule for rule in nsg_rules if rule['name'] == 'allow-ssh')
        self.assertEqual(
            ssh_rule['properties']['sourceAddressPrefix'], '0.0.0.0/0'
        )
        self.assertIn('allow-iperf-tcp', {rule['name'] for rule in nsg_rules})
        self.assertIn('allow-iperf-udp', {rule['name'] for rule in nsg_rules})
        self.assertIn('allow-web-loadgen', {rule['name'] for rule in nsg_rules})

    def test_each_web_benchmark_uses_standard_d2as_v7_loadgen(self):
        for benchmark in ('apachebench', 'deathstarbench'):
            with self.subTest(benchmark=benchmark):
                clients = fake_clients()
                plan = full_plan()
                plan['benchmarks'] = [benchmark]
                plan['storage']['additional_volume'] = False

                resources = azure.provision(
                    {'id': f'web-{benchmark}', 'resources': {}},
                    plan,
                    public_key=PUBLIC_KEY,
                    clients=clients,
                )

                self.assertEqual(
                    resources['loadgen_shape'],
                    'Standard_D2as_v7',
                )
                loadgen_body = next(
                    body
                    for _, name, body in clients['virtual_machines'].calls
                    if name.endswith('-loadgen')
                )
                self.assertEqual(
                    loadgen_body['properties']['hardwareProfile']['vmSize'],
                    'Standard_D2as_v7',
                )

    def test_request_bodies_deserialize_with_pinned_azure_sdk_wire_models(self):
        """Catch snake_case bodies that the modern SDK would send unchanged."""
        from azure.mgmt.compute.models import Disk, VirtualMachine
        from azure.mgmt.network.models import (
            NetworkInterface,
            NetworkSecurityGroup,
            PublicIPAddress,
            Subnet,
            VirtualNetwork,
        )

        clients = fake_clients()
        azure.provision(
            {'id': 'schema1', 'resources': {}},
            full_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        nsg = NetworkSecurityGroup._deserialize(
            clients['network_security_groups'].calls[0][2], {}
        )
        vnet = VirtualNetwork._deserialize(
            clients['virtual_networks'].calls[0][2], {}
        )
        subnet = Subnet._deserialize(clients['subnets'].calls[0][3], {})
        public_ip = PublicIPAddress._deserialize(
            clients['public_ip_addresses'].calls[0][2], {}
        )
        nic = NetworkInterface._deserialize(
            clients['network_interfaces'].calls[0][2], {}
        )
        disk = Disk._deserialize(clients['disks'].calls[0][2], {})
        vm = VirtualMachine._deserialize(
            clients['virtual_machines'].calls[0][2], {}
        )

        self.assertEqual(
            nsg.properties.security_rules[0].properties.destination_port_range,
            '22',
        )
        self.assertEqual(
            vnet.properties.address_space.address_prefixes, ['10.42.0.0/16']
        )
        self.assertEqual(subnet.properties.address_prefix, '10.42.1.0/24')
        self.assertEqual(public_ip.properties.public_ip_allocation_method, 'Static')
        self.assertEqual(
            nic.properties.ip_configurations[0].properties.subnet.id,
            clients['subnets'].items[
                ('benchmark-schema1', 'benchmark-schema1-vnet', 'benchmark-subnet')
            ]['id'],
        )
        self.assertEqual(disk.properties.disk_iops_read_write, 3000)
        self.assertEqual(disk.properties.disk_m_bps_read_write, 125)
        self.assertEqual(
            vm.properties.hardware_profile.vm_size, 'Standard_D8pls_v6'
        )
        self.assertEqual(
            vm.properties.storage_profile.data_disks[0].managed_disk.id,
            clients['disks'].calls[0][2].get('id',
                clients['disks'].items[
                    ('benchmark-schema1', 'benchmark-schema1-data')
                ]['id']
            ),
        )

    def test_group_create_response_loss_reconciles_exact_owned_group(self):
        clients = fake_clients()
        clients['resource_groups'].create_error_after_store = ApiError(503)
        plan = full_plan()
        plan['benchmarks'] = ['stream']
        plan['storage']['additional_volume'] = False
        job = {'id': 'response1', 'resources': {}}

        resources = azure.provision(
            job, plan, public_key=PUBLIC_KEY, clients=clients
        )

        self.assertFalse(resources['azure_resource_group_create_ambiguous'])
        self.assertEqual(resources['azure_resource_group_name'], 'benchmark-response1')

    def test_existing_same_name_group_with_wrong_tags_fails_closed(self):
        clients = fake_clients()
        clients['resource_groups'].items['benchmark-collision'] = {
            'id': (
                f'/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/'
                'benchmark-collision'
            ),
            'name': 'benchmark-collision',
            'location': 'eastus2',
            'tags': {'managed-by': 'someone-else'},
        }
        plan = full_plan()
        plan['benchmarks'] = ['stream']
        plan['storage']['additional_volume'] = False

        with self.assertRaisesRegex(RuntimeError, 'ownership tags'):
            azure.provision(
                {'id': 'collision', 'resources': {}},
                plan,
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertEqual(clients['network_security_groups'].calls, [])

    def test_validation_failure_happens_before_first_cloud_write(self):
        clients = fake_clients()
        plan = full_plan()
        plan['iperf3']['protocols'] = ['sctp']

        with self.assertRaisesRegex(ValueError, 'TCP and UDP only'):
            azure.provision(
                {'id': 'badproto', 'resources': {}},
                plan,
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertEqual(clients['resource_groups'].create_calls, [])
        self.assertEqual(clients['network_security_groups'].calls, [])

    def test_unaccepted_marketplace_terms_fail_before_first_cloud_write(self):
        clients = fake_clients()
        clients['marketplace_terms'].accepted = False
        plan = full_plan()
        plan['benchmarks'] = ['stream']
        plan['storage']['additional_volume'] = False

        with self.assertRaisesRegex(RuntimeError, 'terms accept'):
            azure.provision(
                {'id': 'terms', 'resources': {}},
                plan,
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertEqual(clients['resource_groups'].create_calls, [])

    def test_unavailable_premium_v2_fails_before_first_cloud_write(self):
        clients = fake_clients()
        clients['resource_skus'].items = [
            item for item in clients['resource_skus'].items
            if item.get('name') != 'PremiumV2_LRS'
        ]

        with self.assertRaisesRegex(ValueError, 'Premium SSD v2 is unavailable'):
            azure.provision(
                {'id': 'nodisk', 'resources': {}},
                full_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertEqual(clients['resource_groups'].create_calls, [])


class AzureCleanupTests(unittest.TestCase):
    def _provision_minimal(self):
        clients = fake_clients()
        plan = full_plan()
        plan['benchmarks'] = ['stream']
        plan['storage']['additional_volume'] = False
        job = {'id': 'cleanup1', 'resources': {}}
        azure.provision(job, plan, public_key=PUBLIC_KEY, clients=clients)
        return clients, job

    def test_cleanup_verifies_and_deletes_exact_group_idempotently(self):
        clients, job = self._provision_minimal()

        azure.destroy_resources(job, clients=clients)
        azure.destroy_resources(job, clients=clients)

        self.assertEqual(
            clients['resource_groups'].delete_calls, ['benchmark-cleanup1']
        )
        self.assertEqual(job['status'], 'destroyed')
        self.assertNotIn('azure_resource_group_name', job['resources'])
        self.assertNotIn('public_ip', job['resources'])

    def test_cleanup_refuses_changed_ownership_tags_without_mutation(self):
        clients, job = self._provision_minimal()
        clients['resource_groups'].items['benchmark-cleanup1']['tags'][
            'benchmark-job'
        ] = 'another-job'

        with self.assertRaisesRegex(RuntimeError, 'ownership tags'):
            azure.destroy_resources(job, clients=clients)

        self.assertEqual(clients['resource_groups'].delete_calls, [])
        self.assertIn('azure_resource_group_name', job['resources'])

    def test_cleanup_refuses_cross_subscription_group_identity(self):
        clients, job = self._provision_minimal()
        clients['resource_groups'].items['benchmark-cleanup1']['id'] = (
            '/subscriptions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/'
            'resourceGroups/benchmark-cleanup1'
        )

        with self.assertRaisesRegex(RuntimeError, 'immutable'):
            azure.destroy_resources(job, clients=clients)

        self.assertEqual(clients['resource_groups'].delete_calls, [])

    def test_cleanup_refuses_foreign_top_level_resource_in_owned_group(self):
        clients, job = self._provision_minimal()
        clients['resources'].foreign.append({
            'id': (
                f'/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/'
                'benchmark-cleanup1/providers/Microsoft.Storage/'
                'storageAccounts/manualdata'
            ),
            'name': 'manualdata',
        })

        with self.assertRaisesRegex(RuntimeError, 'outside this run'):
            azure.destroy_resources(job, clients=clients)

        self.assertEqual(clients['resource_groups'].delete_calls, [])
        self.assertIn('azure_resource_group_name', job['resources'])

    def test_cleanup_retains_contract_when_delete_does_not_prove_absence(self):
        clients, job = self._provision_minimal()
        clients['resource_groups'].delete_error = ApiError(503)

        with self.assertRaises(ApiError):
            azure.destroy_resources(job, clients=clients)

        self.assertIn('azure_resource_group_name', job['resources'])
        self.assertTrue(job['resources']['azure_resource_group_delete_ambiguous'])

    def test_cleanup_accepts_lost_delete_response_only_after_exact_absence(self):
        clients, job = self._provision_minimal()
        clients['resource_groups'].delete_response_lost = True

        azure.destroy_resources(job, clients=clients)

        self.assertNotIn('azure_resource_group_name', job['resources'])
        self.assertEqual(job['status'], 'destroyed')

    def test_cleanup_refuses_orphan_child_contract_without_group_anchor(self):
        clients = fake_clients()
        job = {
            'id': 'orphan',
            'resources': {'azure_instance_id': '/some/resource'},
        }

        with self.assertRaisesRegex(RuntimeError, 'ownership anchor'):
            azure.destroy_resources(job, clients=clients)

        self.assertEqual(clients['resource_groups'].delete_calls, [])


if __name__ == '__main__':
    unittest.main()
