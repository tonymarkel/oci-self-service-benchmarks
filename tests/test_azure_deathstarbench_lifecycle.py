import copy
import unittest

from app.deathstarbench_contract import (
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    K3S_RUNTIME_ID,
    K3S_RUNTIME_JOURNAL_KEY,
    PODMAN_COMPOSE_RUNTIME_ID,
    SINGLE_HOST_TOPOLOGY_ID,
)
from app.providers import azure
from app.deathstarbench_distributed import azure_k3s_candidate_plan
from app.resource_inventory import (
    ResourceInventoryError,
    load_role_node_inventory,
)
from tests.test_azure_provider import (
    ApiError,
    NotFound,
    Operation,
    PUBLIC_KEY,
    SUBSCRIPTION_ID,
    fake_clients,
    full_plan,
    vm_sku,
)


class AzureDeathStarBenchLifecycleTests(unittest.TestCase):
    def _compact_plan(self):
        plan = full_plan()
        plan['benchmarks'] = ['deathstarbench']
        plan['deathstarbench'] = {
            'topology_id': SINGLE_HOST_TOPOLOGY_ID,
            'runtime_id': PODMAN_COMPOSE_RUNTIME_ID,
        }
        plan['storage']['additional_volume'] = False
        return plan

    def _distributed_plan(self):
        plan = full_plan()
        plan['benchmarks'] = ['deathstarbench']
        plan['deathstarbench'] = {
            'topology_id': DISTRIBUTED_TIERED_TOPOLOGY_ID,
            'runtime_id': K3S_RUNTIME_ID,
            'workload': 'social_network',
        }
        plan['storage']['additional_volume'] = False
        return plan

    def _distributed_clients(self):
        clients = fake_clients()
        clients['resource_skus'].items.append(
            vm_sku('Standard_D4as_v7', 'x64', 4, 16)
        )
        return clients

    def _provision_distributed(self, job_id='dsblifecycle'):
        clients = self._distributed_clients()
        job = {'id': job_id, 'resources': {}}
        azure.provision_distributed_deathstarbench_candidate(
            job,
            self._distributed_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
        )
        return clients, job

    def test_compact_provision_dual_writes_full_inventory_before_group_put(self):
        clients = fake_clients()
        job = {'id': 'dsbcompact', 'resources': {}}
        state_at_first_write = []
        clients['resource_groups'].on_create = lambda _name, _body: (
            state_at_first_write.append(copy.deepcopy(job['resources']))
        )

        resources = azure.provision(
            job,
            self._compact_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        self.assertEqual(len(state_at_first_write), 1)
        planned = state_at_first_write[0]
        self.assertIn(azure.DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY, planned)
        self.assertIn(azure.DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY, planned)
        planned_inventory = load_role_node_inventory(planned)
        self.assertEqual(
            tuple(node.key for node in planned_inventory.nodes),
            ('load-generator', 'runner'),
        )
        self.assertTrue(all(
            node.lifecycle_status == 'planned'
            for node in planned_inventory.nodes
        ))
        self.assertEqual(
            planned_inventory.node('runner').provider_resource_id.casefold(),
            planned['azure_instance_expected_id'].casefold(),
        )
        self.assertEqual(
            planned_inventory.node(
                'load-generator'
            ).provider_resource_id.casefold(),
            planned['azure_loadgen_instance_expected_id'].casefold(),
        )

        inventory = load_role_node_inventory(resources)
        self.assertTrue(all(
            node.lifecycle_status == 'running' for node in inventory.nodes
        ))
        self.assertEqual(
            inventory.node('runner').private_addresses,
            (resources['private_ip'],),
        )
        self.assertEqual(
            inventory.node('load-generator').private_addresses,
            (resources['loadgen_private_ip'],),
        )
        self.assertEqual(
            clients['subnets'].calls[0][3]['properties'][
                'privateEndpointNetworkPolicies'
            ],
            'Disabled',
        )

    def test_distributed_candidate_creates_exact_five_node_azure_topology(self):
        clients = self._distributed_clients()
        job = {'id': 'dsbtopology', 'resources': {}}
        state_at_first_write = []
        clients['resource_groups'].on_create = lambda _name, _body: (
            state_at_first_write.append(copy.deepcopy(job['resources']))
        )

        resources = azure.provision_distributed_deathstarbench_candidate(
            job,
            self._distributed_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        self.assertEqual(len(state_at_first_write), 1)
        planned = state_at_first_write[0]
        planned_inventory = load_role_node_inventory(planned)
        self.assertEqual(
            tuple(node.key for node in planned_inventory.nodes),
            ('application', 'cache', 'control', 'database', 'load-generator'),
        )
        self.assertTrue(all(
            node.lifecycle_status == 'planned'
            and node.provider_resource_id
            and node.provider_resource_name
            and node.private_addresses
            and node.shape
            and node.architecture
            and node.capacity_class
            for node in planned_inventory.nodes
        ))
        self.assertEqual(
            planned_inventory.topology_fingerprint,
            planned[azure.DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY],
        )
        planned_database_storage = planned_inventory.node(
            'database'
        ).storage_resource('database-data')
        self.assertEqual(planned_database_storage.lifecycle_status, 'planned')
        self.assertEqual(planned_database_storage.size_gb, 100)
        self.assertEqual(planned_database_storage.provisioned_iops, 3000)
        self.assertEqual(
            planned_database_storage.provisioned_throughput_mibps, 125
        )

        expected_order = (
            'control',
            'database',
            'cache',
            'application',
            'loadgen',
        )
        vm_calls = clients['virtual_machines'].calls
        self.assertEqual(
            tuple(name.rsplit('-', 1)[-1] for _, name, _ in vm_calls),
            expected_order,
        )
        self.assertEqual(
            tuple(
                body['properties']['hardwareProfile']['vmSize']
                for _, _, body in vm_calls
            ),
            (
                'Standard_D2as_v7',
                'Standard_D4as_v7',
                'Standard_D2as_v7',
                'Standard_D8pls_v6',
                'Standard_D2as_v7',
            ),
        )

        nic_calls = clients['network_interfaces'].calls
        expected_private = {
            f'benchmark-dsbtopology-{key}': value
            for key, value in azure.DISTRIBUTED_PRIVATE_ADDRESSES.items()
        }
        expected_private['benchmark-dsbtopology-loadgen'] = (
            expected_private.pop('benchmark-dsbtopology-load-generator')
        )
        self.assertEqual(len(nic_calls), 5)
        for _, nic_name, body in nic_calls:
            vm_name = nic_name.removesuffix('-nic')
            properties = body['properties']['ipConfigurations'][0]['properties']
            self.assertEqual(
                properties['privateIPAddress'], expected_private[vm_name]
            )
            self.assertEqual(properties['privateIPAllocationMethod'], 'Static')
            has_public_ip = 'publicIPAddress' in properties
            self.assertEqual(
                has_public_ip,
                vm_name.rsplit('-', 1)[-1]
                in {'control', 'application', 'loadgen'},
            )
        public_ip_names = tuple(
            name for _, name, _ in clients['public_ip_addresses'].calls
        )
        self.assertEqual(
            tuple(
                name.removesuffix('-pip').rsplit('-', 1)[-1]
                for name in public_ip_names
                if not name.endswith('-cluster-nat-pip')
            ),
            ('control', 'application', 'loadgen'),
        )
        self.assertIn('benchmark-dsbtopology-cluster-nat-pip', public_ip_names)
        self.assertEqual(len(clients['nat_gateways'].calls), 1)
        _, nat_name, nat_body = clients['nat_gateways'].calls[0]
        self.assertEqual(nat_name, 'benchmark-dsbtopology-cluster-nat')
        self.assertEqual(nat_body['sku']['name'], 'Standard')
        self.assertEqual(nat_body['zones'], ['1'])
        self.assertEqual(
            nat_body['properties']['publicIpAddresses'][0]['id'].casefold(),
            resources[
                'azure_dsb_cluster_nat_public_ip_expected_id'
            ].casefold(),
        )
        subnet_bodies = {
            name: body for _, _, name, body in clients['subnets'].calls
        }
        cluster_subnet = subnet_bodies['cluster-subnet']['properties']
        loadgen_subnet = subnet_bodies['loadgen-subnet']['properties']
        self.assertFalse(cluster_subnet['defaultOutboundAccess'])
        self.assertFalse(loadgen_subnet['defaultOutboundAccess'])
        self.assertEqual(
            cluster_subnet['privateEndpointNetworkPolicies'], 'Enabled'
        )
        self.assertEqual(
            loadgen_subnet['privateEndpointNetworkPolicies'], 'Enabled'
        )
        self.assertEqual(
            cluster_subnet['natGateway']['id'].casefold(),
            resources[
                'azure_dsb_cluster_nat_gateway_expected_id'
            ].casefold(),
        )
        self.assertNotIn('natGateway', loadgen_subnet)

        self.assertEqual(len(clients['disks'].calls), 1)
        _, disk_name, disk_body = clients['disks'].calls[0]
        self.assertEqual(disk_name, 'benchmark-dsbtopology-database-data')
        self.assertEqual(disk_body['sku']['name'], 'PremiumV2_LRS')
        self.assertEqual(disk_body['properties']['diskSizeGB'], 100)
        self.assertEqual(disk_body['properties']['diskIOPSReadWrite'], 3000)
        self.assertEqual(disk_body['properties']['diskMBpsReadWrite'], 125)
        self.assertEqual(disk_body['properties']['networkAccessPolicy'], 'DenyAll')
        database_vm = next(
            body for _, name, body in vm_calls if name.endswith('-database')
        )
        self.assertEqual(
            database_vm['properties']['storageProfile']['dataDisks'][0][
                'managedDisk'
            ]['id'].casefold(),
            resources['azure_dsb_database_data_disk_expected_id'].casefold(),
        )

        inventory = load_role_node_inventory(resources)
        self.assertTrue(all(
            node.lifecycle_status == 'running' for node in inventory.nodes
        ))
        public_nodes = tuple(
            node.key for node in inventory.nodes if node.public_addresses
        )
        self.assertEqual(
            public_nodes, ('application', 'control', 'load-generator')
        )
        self.assertEqual(
            inventory.node('database').storage_resource(
                'database-data'
            ).lifecycle_status,
            'running',
        )

        nsg_calls = {
            name: body
            for _, name, body in clients['network_security_groups'].calls
        }
        cluster_rules = {
            rule['name']: rule['properties']
            for rule in nsg_calls[
                'benchmark-dsbtopology-cluster-nsg'
            ]['properties']['securityRules']
        }
        self.assertEqual(
            cluster_rules['allow-k3s-control-plane'][
                'destinationPortRange'
            ],
            '6443',
        )
        self.assertEqual(
            cluster_rules['allow-k3s-overlay']['destinationPortRange'],
            '8472',
        )
        self.assertNotIn('allow-k3s-kubelet', cluster_rules)
        self.assertEqual(
            cluster_rules['allow-loadgen-frontend']['sourceAddressPrefix'],
            azure.DISTRIBUTED_PRIVATE_ADDRESSES['load-generator'],
        )
        self.assertEqual(
            cluster_rules['allow-loadgen-frontend']['destinationPortRange'],
            '8080',
        )
        self.assertEqual(
            cluster_rules['deny-unlisted-vnet-inbound']['access'], 'Deny'
        )
        self.assertEqual(
            cluster_rules['deny-unlisted-vnet-inbound']['priority'], 4000
        )
        loadgen_rules = {
            rule['name']: rule['properties']
            for rule in nsg_calls[
                'benchmark-dsbtopology-loadgen-nsg'
            ]['properties']['securityRules']
        }
        self.assertEqual(
            set(loadgen_rules),
            {'allow-public-ssh', 'deny-unlisted-vnet-inbound'},
        )

    def test_distributed_candidate_cleanup_clears_nested_contract_state(self):
        clients, job = self._provision_distributed('dsbcleanup')
        job['resources'][K3S_RUNTIME_JOURNAL_KEY] = {
            'state': 'cluster_ready',
        }
        job['resources'][DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY] = {
            'state': 'workload_ready',
        }

        azure.destroy_resources(job, clients=clients)

        self.assertEqual(
            clients['resource_groups'].delete_calls,
            ['benchmark-dsbcleanup'],
        )
        self.assertEqual(job['status'], 'destroyed')
        self.assertNotIn(
            azure.DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY, job['resources']
        )
        self.assertNotIn(
            azure.DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY, job['resources']
        )
        self.assertNotIn(azure.ROLE_NODE_INVENTORY_KEY, job['resources'])
        self.assertNotIn(K3S_RUNTIME_JOURNAL_KEY, job['resources'])
        self.assertNotIn(
            DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
            job['resources'],
        )
        self.assertFalse(any(
            key.startswith('azure_') for key in job['resources']
        ))
        for key in (
            'instance_id',
            'public_ip',
            'private_ip',
            'loadgen_instance_id',
            'loadgen_public_ip',
            'loadgen_private_ip',
        ):
            self.assertNotIn(key, job['resources'])

    def test_cleanup_clears_legacy_orphaned_runtime_journal(self):
        job = {
            'id': 'dsborphanedjournal',
            'resources': {
                K3S_RUNTIME_JOURNAL_KEY: {'state': 'cluster_ready'},
                DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY: {
                    'state': 'workload_ready',
                },
            },
        }

        azure.destroy_resources(job, clients={})

        self.assertEqual(job['resources'], {})
        self.assertEqual(job['status'], 'destroyed')

    def test_provider_candidate_is_accepted_by_runtime_plan_contract(self):
        _clients, job = self._provision_distributed('dsbruntimeplan')

        runtime_plan = azure_k3s_candidate_plan(job['resources'])

        self.assertEqual(
            tuple(host.key for host in runtime_plan.hosts),
            ('control', 'database', 'cache', 'application'),
        )
        self.assertEqual(
            runtime_plan.host('database').jump_host_key,
            'azure_dsb_control_public_ip',
        )

    def test_distributed_cleanup_rejects_tampered_nested_inventory(self):
        clients, job = self._provision_distributed('dsbtamper')
        inventory = job['resources'][azure.ROLE_NODE_INVENTORY_KEY]
        application = next(
            node for node in inventory['nodes'] if node['key'] == 'application'
        )
        application['provider_resource_id'] = (
            f'/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/'
            'benchmark-dsbtamper/providers/Microsoft.Compute/'
            'virtualMachines/not-the-planned-application'
        )

        with self.assertRaisesRegex(
            RuntimeError, 'conflicts with its persisted provider identity'
        ):
            azure.destroy_resources(job, clients=clients)

        self.assertEqual(clients['resource_groups'].delete_calls, [])
        self.assertIn(azure.ROLE_NODE_INVENTORY_KEY, job['resources'])
        self.assertIn('azure_resource_group_name', job['resources'])

    def test_mid_vm_response_loss_reconciles_and_cleanup_recovers(self):
        clients = self._distributed_clients()
        virtual_machines = clients['virtual_machines']
        original_begin = virtual_machines.begin_create_or_update

        def begin_with_lost_cache_response(group, name, body):
            operation = original_begin(group, name, body)
            if not name.endswith('-cache'):
                return operation

            def create_then_lose_response():
                operation.result()
                raise ApiError(503, 'lost VM create response')

            return Operation(callback=create_then_lose_response)

        virtual_machines.begin_create_or_update = begin_with_lost_cache_response
        job = {'id': 'dsbresponse', 'resources': {}}

        azure.provision_distributed_deathstarbench_candidate(
            job,
            self._distributed_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        inventory = load_role_node_inventory(job['resources'])
        self.assertEqual(
            inventory.node('cache').lifecycle_status,
            'running',
        )
        self.assertEqual(
            inventory.node('control').lifecycle_status,
            'running',
        )
        self.assertEqual(
            inventory.node('database').lifecycle_status,
            'running',
        )
        self.assertIn('azure_resource_group_name', job['resources'])

        azure.destroy_resources(job, clients=clients)

        self.assertEqual(job['status'], 'destroyed')
        self.assertNotIn(azure.ROLE_NODE_INVENTORY_KEY, job['resources'])
        self.assertEqual(
            clients['resource_groups'].delete_calls,
            ['benchmark-dsbresponse'],
        )

    def test_mid_vm_failure_is_ambiguous_but_group_cleanup_recovers(self):
        clients = self._distributed_clients()
        virtual_machines = clients['virtual_machines']
        original_begin = virtual_machines.begin_create_or_update

        def begin_with_cache_failure(group, name, body):
            if name.endswith('-cache'):
                return Operation(error=ApiError(503, 'VM create unavailable'))
            return original_begin(group, name, body)

        virtual_machines.begin_create_or_update = begin_with_cache_failure
        job = {'id': 'dsbfailure', 'resources': {}}

        with self.assertRaisesRegex(ApiError, 'VM create unavailable'):
            azure.provision_distributed_deathstarbench_candidate(
                job,
                self._distributed_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        inventory = load_role_node_inventory(job['resources'])
        self.assertEqual(
            inventory.node('cache').lifecycle_status,
            'create_ambiguous',
        )
        self.assertEqual(
            inventory.node('control').lifecycle_status,
            'running',
        )

        azure.destroy_resources(job, clients=clients)

        self.assertEqual(job['status'], 'destroyed')
        self.assertNotIn(azure.ROLE_NODE_INVENTORY_KEY, job['resources'])

    def test_ambiguous_group_create_waits_for_delayed_visibility(self):
        clients = self._distributed_clients()
        groups = clients['resource_groups']
        groups.create_error_after_store = ApiError(503, 'lost group response')
        original_get = groups.get
        hidden_reads = {'remaining': 2}

        def delayed_get(name):
            if name in groups.items and hidden_reads['remaining']:
                hidden_reads['remaining'] -= 1
                raise NotFound(name)
            return original_get(name)

        groups.get = delayed_get
        job = {'id': 'dsbdelayed', 'resources': {}}

        with self.assertRaisesRegex(ApiError, 'lost group response'):
            azure.provision_distributed_deathstarbench_candidate(
                job,
                self._distributed_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertTrue(
            job['resources']['azure_resource_group_create_ambiguous']
        )
        self.assertIn(azure.ROLE_NODE_INVENTORY_KEY, job['resources'])

        azure.destroy_resources(
            job,
            clients=clients,
            absence_retry_delay_seconds=0,
        )

        self.assertEqual(groups.delete_calls, ['benchmark-dsbdelayed'])
        self.assertEqual(job['status'], 'destroyed')
        self.assertNotIn(azure.ROLE_NODE_INVENTORY_KEY, job['resources'])

    def test_candidate_retry_reuses_pins_without_reputting_children(self):
        clients, job = self._provision_distributed('dsbretry')
        initial_call_counts = {
            key: len(clients[key].calls)
            for key in (
                'network_security_groups',
                'virtual_networks',
                'subnets',
                'public_ip_addresses',
                'nat_gateways',
                'network_interfaces',
                'disks',
                'virtual_machines',
            )
        }
        saved_application_version = job['resources'][
            'azure_dsb_application_image_version'
        ]
        saved_support_version = job['resources'][
            'azure_dsb_support_image_version'
        ]
        clients['virtual_machine_images'].versions[
            'rockylinux-aarch64'
        ].append('9.9.20990101')
        clients['virtual_machine_images'].versions[
            'rockylinux-x86_64'
        ].append('9.9.20990101')

        azure.provision_distributed_deathstarbench_candidate(
            job,
            self._distributed_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        self.assertEqual(
            {
                key: len(clients[key].calls)
                for key in initial_call_counts
            },
            initial_call_counts,
        )
        self.assertEqual(
            job['resources']['azure_dsb_application_image_version'],
            saved_application_version,
        )
        self.assertEqual(
            job['resources']['azure_dsb_support_image_version'],
            saved_support_version,
        )

    def test_candidate_retry_rejects_live_nsg_drift_without_put(self):
        clients, job = self._provision_distributed('dsbdrift')
        service = clients['network_security_groups']
        key = ('benchmark-dsbdrift', 'benchmark-dsbdrift-cluster-nsg')
        service.items[key]['properties']['securityRules'][0]['properties'][
            'priority'
        ] = 999
        call_count = len(service.calls)

        with self.assertRaisesRegex(RuntimeError, 'configuration differs'):
            azure.provision_distributed_deathstarbench_candidate(
                job,
                self._distributed_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertEqual(len(service.calls), call_count)
        self.assertEqual(
            service.items[key]['properties']['securityRules'][0][
                'properties'
            ]['priority'],
            999,
        )

    def test_candidate_retry_rejects_live_subnet_drift_without_put(self):
        clients, job = self._provision_distributed('dsbsubnetdrift')
        service = clients['subnets']
        key = (
            'benchmark-dsbsubnetdrift',
            'benchmark-dsbsubnetdrift-dsb-vnet',
            'cluster-subnet',
        )
        service.items[key]['properties']['addressPrefix'] = '10.250.1.0/24'
        call_count = len(service.calls)

        with self.assertRaisesRegex(RuntimeError, 'configuration'):
            azure.provision_distributed_deathstarbench_candidate(
                job,
                self._distributed_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertEqual(len(service.calls), call_count)
        self.assertEqual(
            service.items[key]['properties']['addressPrefix'],
            '10.250.1.0/24',
        )

    def test_candidate_rejects_false_or_unknown_application_premium_io(self):
        for premium_io in ('False', None):
            with self.subTest(premium_io=premium_io):
                clients = self._distributed_clients()
                application_sku = next(
                    item for item in clients['resource_skus'].items
                    if item.get('name') == 'Standard_D8pls_v6'
                )
                application_sku['capabilities'] = [
                    capability
                    for capability in application_sku['capabilities']
                    if capability['name'] != 'PremiumIO'
                ]
                if premium_io is not None:
                    application_sku['capabilities'].append({
                        'name': 'PremiumIO',
                        'value': premium_io,
                    })
                job = {'id': 'dsbpremiumio', 'resources': {}}

                with self.assertRaisesRegex(
                    ValueError,
                    r'Premium_LRS OS-disk support.*application',
                ):
                    azure.provision_distributed_deathstarbench_candidate(
                        job,
                        self._distributed_plan(),
                        public_key=PUBLIC_KEY,
                        clients=clients,
                    )

                self.assertEqual(clients['resource_groups'].create_calls, [])
                self.assertEqual(job['resources'], {})

    def test_candidate_requires_compute_registration_before_group_put(self):
        for state in ('NotRegistered', ''):
            with self.subTest(state=state):
                clients = self._distributed_clients()
                clients['providers'].states['Microsoft.Compute'] = state
                job = {'id': 'dsbregistration', 'resources': {}}

                with self.assertRaisesRegex(
                    RuntimeError,
                    r'required Azure resource providers.*Microsoft.Compute',
                ):
                    azure.provision_distributed_deathstarbench_candidate(
                        job,
                        self._distributed_plan(),
                        public_key=PUBLIC_KEY,
                        clients=clients,
                    )

                self.assertEqual(clients['resource_groups'].create_calls, [])
                self.assertEqual(job['resources'], {})

    def test_candidate_rejects_explicit_false_database_premium_io_v2(self):
        clients = self._distributed_clients()
        database_sku = next(
            item for item in clients['resource_skus'].items
            if item.get('name') == 'Standard_D4as_v7'
        )
        database_sku['capabilities'] = [
            capability
            for capability in database_sku['capabilities']
            if capability['name'] != 'PremiumIOV2Supported'
        ]
        database_sku['capabilities'].append({
            'name': 'PremiumIOV2Supported',
            'value': 'False',
        })
        job = {'id': 'dsbpremiumiov2', 'resources': {}}

        with self.assertRaisesRegex(
            ValueError,
            r'reports that Azure Premium SSD v2 is unsupported',
        ):
            azure.provision_distributed_deathstarbench_candidate(
                job,
                self._distributed_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertEqual(clients['resource_groups'].create_calls, [])
        self.assertEqual(job['resources'], {})

    def test_candidate_accepts_omitted_database_premium_io_v2_capability(self):
        clients = self._distributed_clients()
        database_sku = next(
            item for item in clients['resource_skus'].items
            if item.get('name') == 'Standard_D4as_v7'
        )
        database_sku['capabilities'] = [
            capability
            for capability in database_sku['capabilities']
            if capability['name'] != 'PremiumIOV2Supported'
        ]
        job = {'id': 'dsbpremiumunknown', 'resources': {}}

        resources = azure.provision_distributed_deathstarbench_candidate(
            job,
            self._distributed_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        self.assertEqual(resources['azure_database_disk_type'], 'PremiumV2_LRS')
        self.assertTrue(clients['resource_groups'].create_calls)

    def test_candidate_rejects_mixed_plan_and_foreign_job_state(self):
        clients = self._distributed_clients()
        wrong_provider = self._distributed_plan()
        wrong_provider['provider'] = 'aws'
        with self.assertRaisesRegex(ValueError, 'requires an Azure plan'):
            azure.provision_distributed_deathstarbench_candidate(
                {'id': 'dsbwrongprovider', 'resources': {}},
                wrong_provider,
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        mixed = self._distributed_plan()
        mixed['benchmarks'].append('stream')
        with self.assertRaisesRegex(ValueError, 'only selected benchmark'):
            azure.provision_distributed_deathstarbench_candidate(
                {'id': 'dsbmixed', 'resources': {}},
                mixed,
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        for workload in (None, 'hotel_reservation'):
            with self.subTest(workload=workload):
                wrong_workload = self._distributed_plan()
                if workload is None:
                    wrong_workload['deathstarbench'].pop('workload')
                else:
                    wrong_workload['deathstarbench']['workload'] = workload
                with self.assertRaisesRegex(
                    ValueError,
                    'exact DeathStarBench Social Network workload',
                ):
                    azure.provision_distributed_deathstarbench_candidate(
                        {'id': 'dsbwrongworkload', 'resources': {}},
                        wrong_workload,
                        public_key=PUBLIC_KEY,
                        clients=clients,
                    )
                self.assertEqual(
                    clients['resource_groups'].create_calls,
                    [],
                )

        foreign_job = {
            'id': 'dsbforeign',
            'resources': {'provider': 'aws', 'aws_instance_id': 'i-foreign'},
        }
        with self.assertRaisesRegex(
            ResourceInventoryError,
            'pre-existing resource state',
        ):
            azure.provision_distributed_deathstarbench_candidate(
                foreign_job,
                self._distributed_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertEqual(clients['resource_groups'].create_calls, [])

    def test_candidate_requires_all_child_operations_before_group_put(self):
        cases = (
            ('missing-service', 'managed disks'),
            ('missing-operation', 'network interfaces'),
            ('missing-cleanup-inventory', 'resource-group resource inventory'),
            ('missing-group-delete', 'resource groups'),
        )
        for case, expected_label in cases:
            with self.subTest(case=case):
                clients = self._distributed_clients()
                if case == 'missing-service':
                    clients.pop('disks')
                elif case == 'missing-operation':
                    clients[
                        'network_interfaces'
                    ].begin_create_or_update = None
                elif case == 'missing-cleanup-inventory':
                    clients.pop('resources')
                else:
                    clients['resource_groups'].begin_delete = None
                job = {'id': 'dsbservices', 'resources': {}}

                with self.assertRaisesRegex(RuntimeError, expected_label):
                    azure.provision_distributed_deathstarbench_candidate(
                        job,
                        self._distributed_plan(),
                        public_key=PUBLIC_KEY,
                        clients=clients,
                    )

                self.assertEqual(clients['resource_groups'].create_calls, [])
                self.assertEqual(job['resources'], {})

    def test_cleanup_rejects_expected_id_outside_active_topology(self):
        clients, job = self._provision_distributed('dsbinapplicable')
        group_name = job['resources']['azure_resource_group_name']
        peer_name = f'{group_name}-iperf-peer'
        job['resources'].update({
            'azure_peer_instance_name': peer_name,
            'azure_peer_instance_expected_id': (
                f'/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/'
                f'{group_name}/providers/Microsoft.Compute/'
                f'virtualMachines/{peer_name}'
            ),
        })

        with self.assertRaisesRegex(
            RuntimeError,
            'resource identities that are not part of the active topology',
        ):
            azure.destroy_resources(job, clients=clients)

        self.assertEqual(clients['resource_groups'].delete_calls, [])
        self.assertIn('azure_resource_group_name', job['resources'])

    def test_candidate_resume_rejects_inapplicable_identity_before_put(self):
        clients, job = self._provision_distributed('dsbresumeallowlist')
        group_name = job['resources']['azure_resource_group_name']
        peer_name = f'{group_name}-iperf-peer'
        job['resources'].update({
            'azure_peer_instance_name': peer_name,
            'azure_peer_instance_expected_id': (
                f'/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/'
                f'{group_name}/providers/Microsoft.Compute/'
                f'virtualMachines/{peer_name}'
            ),
        })
        nat_service = clients['nat_gateways']
        nat_name = f'{group_name}-cluster-nat'
        nat_service.items.pop((group_name, nat_name))
        put_count = len(nat_service.calls)

        with self.assertRaisesRegex(
            ResourceInventoryError,
            'resource identities that are not part of the active topology',
        ):
            azure.provision_distributed_deathstarbench_candidate(
                job,
                self._distributed_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertEqual(len(nat_service.calls), put_count)
        self.assertNotIn((group_name, nat_name), nat_service.items)

    def test_cleanup_rejects_altered_applicable_identity_name_or_type(self):
        for case in ('name', 'resource-type'):
            with self.subTest(case=case):
                clients, job = self._provision_distributed(
                    f'dsbidentity{case.replace("-", "")}'
                )
                prefix = 'azure_dsb_control_instance'
                if case == 'name':
                    job['resources'][f'{prefix}_name'] += '-changed'
                else:
                    expected_id_key = f'{prefix}_expected_id'
                    job['resources'][expected_id_key] = job['resources'][
                        expected_id_key
                    ].replace('/virtualMachines/', '/disks/')

                with self.assertRaisesRegex(
                    RuntimeError,
                    'conflicts with its exact deterministic name or resource type',
                ):
                    azure.destroy_resources(job, clients=clients)

                self.assertEqual(clients['resource_groups'].delete_calls, [])
                self.assertIn('azure_resource_group_name', job['resources'])

    def test_cleanup_rechecks_initial_absence_when_create_is_unambiguous(self):
        clients, job = self._provision_distributed('dsbvisibility')
        self.assertFalse(
            job['resources']['azure_resource_group_create_ambiguous']
        )
        groups = clients['resource_groups']
        original_get = groups.get
        reads = {'count': 0}

        def transient_initial_absence(name):
            reads['count'] += 1
            if reads['count'] <= 2:
                raise NotFound(name)
            return original_get(name)

        groups.get = transient_initial_absence

        azure.destroy_resources(
            job,
            clients=clients,
            absence_retry_attempts=4,
            absence_retry_delay_seconds=0,
        )

        self.assertGreaterEqual(reads['count'], 6)
        self.assertEqual(groups.delete_calls, ['benchmark-dsbvisibility'])
        self.assertEqual(job['status'], 'destroyed')

    def test_cleanup_rechecks_transient_predelete_absence(self):
        clients, job = self._provision_distributed('dsbpredeletevisibility')
        groups = clients['resource_groups']
        original_get = groups.get
        reads = {'count': 0}

        def transient_predelete_absence(name):
            reads['count'] += 1
            if reads['count'] == 2:
                raise NotFound(name)
            return original_get(name)

        groups.get = transient_predelete_absence

        azure.destroy_resources(
            job,
            clients=clients,
            absence_retry_attempts=4,
            absence_retry_delay_seconds=0,
        )

        self.assertGreaterEqual(reads['count'], 5)
        self.assertEqual(
            groups.delete_calls,
            ['benchmark-dsbpredeletevisibility'],
        )
        self.assertEqual(job['status'], 'destroyed')

    def test_candidate_sdk_models_match_every_request_projection(self):
        from azure.mgmt.compute.models import Disk, VirtualMachine
        from azure.mgmt.network.models import (
            NatGateway,
            NetworkInterface,
            NetworkSecurityGroup,
            PublicIPAddress,
            Subnet,
            VirtualNetwork,
        )

        clients, _ = self._provision_distributed('dsbsdkprojection')
        request_sets = (
            (
                clients['network_security_groups'].calls,
                2,
                NetworkSecurityGroup,
            ),
            (clients['virtual_networks'].calls, 2, VirtualNetwork),
            (clients['public_ip_addresses'].calls, 2, PublicIPAddress),
            (clients['nat_gateways'].calls, 2, NatGateway),
            (clients['network_interfaces'].calls, 2, NetworkInterface),
            (clients['disks'].calls, 2, Disk),
            (clients['virtual_machines'].calls, 2, VirtualMachine),
            (clients['subnets'].calls, 3, Subnet),
        )
        for calls, body_index, model_class in request_sets:
            with self.subTest(model=model_class.__name__):
                self.assertTrue(calls)
                for call in calls:
                    body = call[body_index]
                    observed = model_class._deserialize(body, {})
                    self.assertTrue(
                        azure._matches_observed_spec(observed, body),
                        f'{model_class.__name__} did not preserve {body!r}',
                    )

    def test_normal_provision_rejects_unreleased_distributed_contract(self):
        clients = self._distributed_clients()
        job = {'id': 'dsbgate', 'resources': {}}

        with self.assertRaisesRegex(ValueError, 'not released yet'):
            azure.provision(
                job,
                self._distributed_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertEqual(job['resources'], {})
        self.assertEqual(clients['resource_groups'].create_calls, [])
        self.assertEqual(clients['resource_skus'].calls, [])


if __name__ == '__main__':
    unittest.main()
