import copy
import unittest
from unittest.mock import patch

from app.deathstarbench_contract import (
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    K3S_RUNTIME_ID,
)
from app.deathstarbench_distributed import gcp_k3s_candidate_plan
from app.providers import gcp
from app.resource_inventory import load_role_node_inventory
from test_gcp_provider import (
    ApiError,
    ImageFamilyService,
    MachineTypeService,
    NotFound,
    Operation,
    PUBLIC_KEY,
    ResourceService,
    StaticService,
)


class DistributedResourceService(ResourceService):
    NAME_FIELDS = {**ResourceService.NAME_FIELDS, 'router': 'router'}
    BODY_FIELDS = {**ResourceService.BODY_FIELDS, 'router': 'router_resource'}

    def _self_link(self, request, name):
        if self.kind == 'router':
            return (
                'https://compute.googleapis.com/compute/v1/projects/'
                f'{request["project"]}/regions/{request["region"]}/routers/{name}'
            )
        return super()._self_link(request, name)

    def insert(self, request):
        if self.kind != 'instance':
            return super().insert(request)
        self.timeline.append(('insert', self.kind, copy.deepcopy(request)))
        body = copy.deepcopy(request['instance_resource'])
        name = body['name']
        if self.insert_error:
            raise self.insert_error
        if name in self.insert_errors_by_name:
            raise self.insert_errors_by_name[name]
        if name in self.items:
            raise ApiError(409, f'{name} already exists')
        body['id'] = str(1000 + len(self.items))
        body['self_link'] = self._self_link(request, name)
        interface = body['network_interfaces'][0]
        interface.setdefault('network_ip', f'10.240.1.{10 + len(self.items)}')
        if interface['access_configs']:
            interface['access_configs'][0]['nat_ip'] = (
                f'198.51.100.{20 + len(self.items)}'
            )
        attached = []
        for disk in body['disks']:
            actual = {
                key: copy.deepcopy(value)
                for key, value in disk.items()
                if key != 'initialize_params'
            }
            if disk.get('boot'):
                initialize = disk['initialize_params']
                actual['source'] = self._self_link(
                    request, disk['device_name']
                ).replace('/instances/', '/disks/')
                self.disk_service.items[disk['device_name']] = {
                    'name': disk['device_name'],
                    'id': str(8000 + len(self.disk_service.items)),
                    'self_link': actual['source'],
                    'description': initialize['description'],
                    'size_gb': initialize['disk_size_gb'],
                    'type_': initialize['disk_type'],
                    'provisioned_iops': initialize.get('provisioned_iops'),
                    'provisioned_throughput': initialize.get(
                        'provisioned_throughput'
                    ),
                    'labels': copy.deepcopy(initialize['labels']),
                    'source_image': initialize['source_image'],
                    'source_image_id': self._image_id(
                        initialize['source_image']
                    ),
                    'users': [body['self_link']],
                }
            attached.append(actual)
        body['disks'] = attached
        self.items[name] = body
        return Operation()

    def patch(self, request):
        if self.kind != 'router':
            raise AssertionError(f'Unexpected {self.kind} patch')
        self.timeline.append(('patch', self.kind, copy.deepcopy(request)))
        name = request['router']
        if name not in self.items:
            raise NotFound(name)
        body = copy.deepcopy(request['router_resource'])
        body['id'] = self.items[name]['id']
        body['self_link'] = self.items[name]['self_link']
        self.items[name] = body
        return Operation()


def candidate_clients():
    timeline = []
    machines = [
        {
            'name': 'n2-standard-2',
            'id': '2002',
            'self_link': (
                'projects/p/zones/us-east1-b/machineTypes/n2-standard-2'
            ),
            'guest_cpus': 2,
            'memory_mb': 8192,
            'architecture': 'X86_64',
            'is_shared_cpu': False,
        },
        {
            'name': 'n2-standard-4',
            'id': '2004',
            'self_link': (
                'projects/p/zones/us-east1-b/machineTypes/n2-standard-4'
            ),
            'guest_cpus': 4,
            'memory_mb': 16384,
            'architecture': 'X86_64',
            'is_shared_cpu': False,
        },
    ]
    image = {
        'name': 'rocky-linux-9-v20260901',
        'id': '9001',
        'self_link': (
            'https://compute.googleapis.com/compute/v1/projects/'
            'rocky-linux-cloud/global/images/'
            'rocky-linux-9-v20260901'
        ),
        'status': 'READY',
        'architecture': 'X86_64',
    }
    clients = {
        'projects': StaticService(get={'id': '3977225790263303761'}),
        'regions': StaticService(listed=[{'name': 'us-east1', 'status': 'UP'}]),
        'zones': StaticService(listed=[{
            'name': 'us-east1-b',
            'id': '101',
            'status': 'UP',
            'region': 'projects/p/regions/us-east1',
        }]),
        'machine_types': MachineTypeService(machines),
        'images': ImageFamilyService({'rocky-linux-9': image}),
        'networks': DistributedResourceService('network', timeline),
        'subnetworks': DistributedResourceService('subnet', timeline),
        'routers': DistributedResourceService('router', timeline),
        'firewalls': DistributedResourceService('firewall', timeline),
        'instances': DistributedResourceService('instance', timeline),
        'disks': DistributedResourceService('disk', timeline),
    }
    clients['instances'].disk_service = clients['disks']
    for service in clients.values():
        if isinstance(service, ResourceService):
            service.clients = clients
    return clients, timeline


def candidate_plan():
    return {
        'provider': 'gcp',
        'gcp_project_id': 'p',
        'region': 'us-east1',
        'gcp_zone': 'us-east1-b',
        'shape': 'n2-standard-2',
        'ocpus': 2,
        'memory_gb': 8,
        'benchmarks': ['deathstarbench'],
        'deathstarbench': {
            'workload': 'social_network',
            'topology_id': DISTRIBUTED_TIERED_TOPOLOGY_ID,
            'runtime_id': K3S_RUNTIME_ID,
        },
        'storage': {'additional_volume': False},
    }


class GcpDistributedDeathStarBenchLifecycleTests(unittest.TestCase):
    def test_real_compute_message_accepts_static_private_address(self):
        from google.cloud import compute_v1

        body = gcp._instance_resource(
            {'types': compute_v1},
            job_id='gcpdsb00',
            name='benchmark-gcpdsb00-dsb-control',
            role='control',
            machine_type=(
                'projects/p/zones/us-east1-b/machineTypes/n2-standard-2'
            ),
            image={
                'self_link': (
                    'projects/rocky-linux-cloud/global/images/'
                    'rocky-linux-9-v20260901'
                ),
            },
            project_id='p',
            zone='us-east1-b',
            network='projects/p/global/networks/benchmark-gcpdsb00-dsb',
            subnet=(
                'projects/p/regions/us-east1/subnetworks/'
                'benchmark-gcpdsb00-dsb-cluster'
            ),
            network_tags=('benchmark-gcpdsb00-dsb-control',),
            boot_size_gb=50,
            public_key=PUBLIC_KEY,
            private_ip_address='10.240.1.10',
        )

        self.assertEqual(
            body.network_interfaces[0].network_i_p,
            '10.240.1.10',
        )

    def test_pd_ssd_reported_baseline_is_not_a_provisioned_setting(self):
        from google.cloud import compute_v1

        expected = gcp._data_disk_resource(
            {'types': compute_v1},
            'gcpdsb00',
            'benchmark-gcpdsb00-dsb-database-data',
            'p',
            'us-east1-b',
            100,
            role='database-data',
            disk_type='pd-ssd',
        )
        actual = {
            'name': 'benchmark-gcpdsb00-dsb-database-data',
            'description': expected.description,
            'size_gb': 100,
            'type_': 'projects/p/zones/us-east1-b/diskTypes/pd-ssd',
            'labels': dict(expected.labels),
            'provisioned_iops': 6000,
            'provisioned_throughput': 240,
        }

        gcp._assert_insert_resource_matches(
            'gcp_dsb_database_data_disk',
            actual,
            expected,
            project_id='p',
            zone='us-east1-b',
        )

    def test_c4a_arm_application_keeps_four_support_roles_on_x86_n2(self):
        clients, _ = candidate_clients()
        c4a = {
            'name': 'c4a-standard-4',
            'id': '4004',
            'self_link': (
                'projects/p/zones/us-east1-b/machineTypes/c4a-standard-4'
            ),
            'guest_cpus': 4,
            'memory_mb': 16384,
            'architecture': 'ARM64',
            'is_shared_cpu': False,
        }
        machines = list(clients['machine_types'].machines.values()) + [c4a]
        clients['machine_types'] = MachineTypeService(machines)
        arm_image = {
            'name': 'rocky-linux-9-arm64-v20260901',
            'id': '9401',
            'self_link': (
                'https://compute.googleapis.com/compute/v1/projects/'
                'rocky-linux-cloud/global/images/'
                'rocky-linux-9-arm64-v20260901'
            ),
            'status': 'READY',
            'architecture': 'ARM64',
        }
        x86_image = clients['images'].images['rocky-linux-9']
        clients['images'] = ImageFamilyService({
            'rocky-linux-9': x86_image,
            'rocky-linux-9-arm64': arm_image,
        })
        for service in clients.values():
            if isinstance(service, ResourceService):
                service.clients = clients
        selected = candidate_plan()
        selected.update({
            'shape': 'c4a-standard-4',
            'ocpus': 4,
            'memory_gb': 16,
        })
        job = {'id': 'gcpc4a01', 'resources': {}}

        resources = gcp.provision_distributed_deathstarbench_candidate(
            job,
            selected,
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        application = clients['instances'].items[
            'benchmark-gcpc4a01-dsb-application'
        ]
        self.assertEqual(
            application['machine_type'].rsplit('/', 1)[-1],
            'c4a-standard-4',
        )
        self.assertEqual(application['network_interfaces'][0]['nic_type'], 'GVNIC')
        self.assertEqual(
            clients['disks'].items[
                'benchmark-gcpc4a01-dsb-application-boot'
            ]['type_'].rsplit('/', 1)[-1],
            'hyperdisk-balanced',
        )
        for role, expected_shape in gcp.DISTRIBUTED_ROLE_MACHINE_TYPES.items():
            instance = clients['instances'].items[
                f'benchmark-gcpc4a01-dsb-{role}'
            ]
            self.assertEqual(
                instance['machine_type'].rsplit('/', 1)[-1], expected_shape
            )
            self.assertEqual(
                resources[f'gcp_dsb_{role.replace("-", "_")}_architecture'],
                'x86_64',
            )
        self.assertEqual(resources['architecture'], 'arm64')
        self.assertEqual(
            resources['gcp_dsb_application_image_id'], '9401'
        )
        self.assertEqual(resources['gcp_dsb_support_image_id'], '9001')

    def test_provisions_exact_five_role_graph_and_inventory(self):
        clients, timeline = candidate_clients()
        job = {'id': 'gcpdsb01', 'resources': {}}

        resources = gcp.provision_distributed_deathstarbench_candidate(
            job,
            candidate_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        self.assertEqual(len(clients['instances'].items), 5)
        for role, address in gcp.DISTRIBUTED_PRIVATE_ADDRESSES.items():
            prefix = gcp.DISTRIBUTED_NODE_PREFIXES[role]
            self.assertEqual(resources[f'{prefix}_private_ip'], address)
            self.assertTrue(resources[f'{prefix}_id'])
            self.assertTrue(resources[f'{prefix}_self_link'])
        self.assertIsNone(resources['gcp_dsb_database_instance_public_ip'])
        self.assertIsNone(resources['gcp_dsb_cache_instance_public_ip'])
        self.assertTrue(resources['gcp_dsb_control_instance_public_ip'])
        self.assertTrue(resources['gcp_dsb_application_instance_public_ip'])
        self.assertTrue(resources['gcp_dsb_load_generator_instance_public_ip'])
        self.assertEqual(
            resources['gcp_dsb_database_disk_device'],
            '/dev/disk/by-id/google-benchmark-gcpdsb01-dsb-database-data',
        )
        database_vm = clients['instances'].items[
            'benchmark-gcpdsb01-dsb-database'
        ]
        self.assertEqual(
            database_vm['network_interfaces'][0]['network_i_p'],
            gcp.DISTRIBUTED_PRIVATE_ADDRESSES['database'],
        )
        self.assertIn(
            'benchmark-gcpdsb01-dsb-database-data',
            {disk['device_name'] for disk in database_vm['disks']},
        )
        router = clients['routers'].items['benchmark-gcpdsb01-dsb-router']
        self.assertEqual(
            router['nats'][0]['source_subnetwork_ip_ranges_to_nat'],
            'LIST_OF_SUBNETWORKS',
        )
        router_insert = next(
            event for event in timeline
            if event[0:2] == ('insert', 'router')
        )
        self.assertNotIn('nats', router_insert[2]['router_resource'])
        self.assertTrue(any(
            event[0:2] == ('patch', 'router') for event in timeline
        ))
        self.assertEqual(len(clients['firewalls'].items), 5)
        inventory = load_role_node_inventory(resources)
        self.assertEqual(
            tuple(node.key for node in inventory.nodes),
            ('application', 'cache', 'control', 'database', 'load-generator'),
        )
        self.assertTrue(all(
            node.lifecycle_status == 'running' for node in inventory.nodes
        ))
        self.assertEqual(
            inventory.node('database').storage_resource(
                'database-data'
            ).provisioned_iops,
            3000,
        )
        first_insert = next(event for event in timeline if event[0] == 'insert')
        self.assertEqual(first_insert[1], 'network')
        self.assertTrue(resources['gcp_distributed_candidate'])
        runtime_plan = gcp_k3s_candidate_plan(resources)
        self.assertEqual(runtime_plan.provider, 'gcp')
        self.assertEqual(
            runtime_plan.database_device_name,
            resources['gcp_dsb_database_data_disk_name'],
        )

    def test_cleanup_is_verified_complete_and_idempotent(self):
        clients, timeline = candidate_clients()
        job = {'id': 'gcpdsb02', 'resources': {}}
        gcp.provision_distributed_deathstarbench_candidate(
            job,
            candidate_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
        )
        timeline.clear()

        with patch.object(gcp, 'RECONCILIATION_DELAY_SECONDS', 0):
            gcp.destroy_resources(job, clients=clients)
            gcp.destroy_resources(job, clients=clients)

        self.assertEqual(job['status'], 'destroyed')
        self.assertFalse(clients['instances'].items)
        self.assertFalse(clients['disks'].items)
        self.assertFalse(clients['firewalls'].items)
        self.assertFalse(clients['routers'].items)
        self.assertFalse(clients['subnetworks'].items)
        self.assertFalse(clients['networks'].items)
        self.assertNotIn('gcp_distributed_candidate', job['resources'])
        self.assertFalse(any(
            key.startswith('gcp_') for key in job['resources']
        ))
        delete_kinds = [event[1] for event in timeline if event[0] == 'delete']
        self.assertEqual(delete_kinds[:5], ['instance'] * 5)
        self.assertEqual(delete_kinds[-1], 'network')

    def test_resume_rejects_same_name_resource_with_new_numeric_id(self):
        clients, _ = candidate_clients()
        job = {'id': 'gcpdsb10', 'resources': {}}
        gcp.provision_distributed_deathstarbench_candidate(
            job,
            candidate_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
        )
        network_name = 'benchmark-gcpdsb10-dsb'
        recorded_id = job['resources']['gcp_dsb_network_id']
        clients['networks'].items[network_name]['id'] = '999999'

        with self.assertRaisesRegex(RuntimeError, 'not recorded ID'):
            gcp.provision_distributed_deathstarbench_candidate(
                job,
                candidate_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertEqual(
            job['resources']['gcp_dsb_network_id'],
            recorded_id,
        )

    def test_response_lost_vm_without_visibility_fails_cleanup_closed(self):
        clients, _ = candidate_clients()
        application = 'benchmark-gcpdsb03-dsb-application'
        clients['instances'].insert_errors_by_name[application] = ApiError(
            408, 'response outcome unknown'
        )
        job = {'id': 'gcpdsb03', 'resources': {}}

        with self.assertRaises(ApiError):
            gcp.provision_distributed_deathstarbench_candidate(
                job,
                candidate_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )
        request_id = job['resources'][
            'gcp_dsb_application_instance_request_id'
        ]
        with patch.object(gcp, 'RECONCILIATION_DELAY_SECONDS', 0):
            with self.assertRaisesRegex(RuntimeError, 'cannot yet prove'):
                gcp.destroy_resources(job, clients=clients)

        self.assertEqual(
            job['resources']['gcp_dsb_application_instance_request_id'],
            request_id,
        )
        self.assertTrue(clients['instances'].items)
        self.assertIn('role_node_inventory', job['resources'])

    def test_failure_before_first_vm_cleans_unattached_database_disk(self):
        clients, _ = candidate_clients()
        control = 'benchmark-gcpdsb06-dsb-control'
        clients['instances'].insert_errors_by_name[control] = ApiError(
            400,
            'launch rejected',
        )
        job = {'id': 'gcpdsb06', 'resources': {}}

        with self.assertRaises(ApiError):
            gcp.provision_distributed_deathstarbench_candidate(
                job,
                candidate_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )
        with patch.object(gcp, 'RECONCILIATION_DELAY_SECONDS', 0):
            gcp.destroy_resources(job, clients=clients)

        self.assertEqual(job['status'], 'destroyed')
        self.assertFalse(clients['disks'].items)
        self.assertFalse(any(
            key.startswith('gcp_') for key in job['resources']
        ))

    def test_confirmed_early_network_rejections_remain_cleanup_safe(self):
        cases = (
            ('gcpdsb07', 'networks', 'benchmark-gcpdsb07-dsb'),
            (
                'gcpdsb08',
                'subnetworks',
                'benchmark-gcpdsb08-dsb-cluster',
            ),
            (
                'gcpdsb09',
                'subnetworks',
                'benchmark-gcpdsb09-dsb-loadgen',
            ),
        )
        for job_id, service_key, resource_name in cases:
            with self.subTest(resource_name=resource_name):
                clients, _ = candidate_clients()
                clients[service_key].insert_errors_by_name[resource_name] = (
                    ApiError(400, 'create rejected')
                )
                job = {'id': job_id, 'resources': {}}

                with self.assertRaises(ApiError):
                    gcp.provision_distributed_deathstarbench_candidate(
                        job,
                        candidate_plan(),
                        public_key=PUBLIC_KEY,
                        clients=clients,
                    )
                with patch.object(gcp, 'RECONCILIATION_DELAY_SECONDS', 0):
                    gcp.destroy_resources(job, clients=clients)

                self.assertEqual(job['status'], 'destroyed')
                self.assertFalse(any(
                    key.startswith('gcp_') for key in job['resources']
                ))

    def test_unknown_inventory_schema_blocks_cleanup_before_delete(self):
        clients, timeline = candidate_clients()
        job = {'id': 'gcpdsb04', 'resources': {}}
        gcp.provision_distributed_deathstarbench_candidate(
            job,
            candidate_plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
        )
        job['resources']['role_node_inventory']['schema_version'] = 999
        timeline.clear()

        with self.assertRaisesRegex(RuntimeError, 'ownership inventory is invalid'):
            gcp.destroy_resources(job, clients=clients)

        self.assertFalse(any(event[0] == 'delete' for event in timeline))

    def test_ordinary_provider_rejects_unreleased_distributed_runtime(self):
        clients, timeline = candidate_clients()
        job = {'id': 'gcpdsb05', 'resources': {}}

        with self.assertRaisesRegex(ValueError, 'not released'):
            gcp.provision(
                job,
                candidate_plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertFalse(any(event[0] == 'insert' for event in timeline))


if __name__ == '__main__':
    unittest.main()
