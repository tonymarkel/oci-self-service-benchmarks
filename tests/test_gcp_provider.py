import copy
import json
import unittest
import uuid
from unittest.mock import patch

from app.providers import gcp


PUBLIC_KEY = (
    'ssh-ed25519 '
    'AAAAC3NzaC1lZDI1NTE5AAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
)


class NotFound(Exception):
    status_code = 404


class ApiError(Exception):
    def __init__(self, status_code, message='api error', *, reason=None):
        super().__init__(message)
        self.status_code = status_code
        self.reason = reason


class Operation:
    def __init__(self, error=None, on_result=None):
        self.error = error
        self.on_result = on_result
        self.completed = False

    def result(self, timeout=None):
        if self.on_result:
            callback, self.on_result = self.on_result, None
            callback()
        self.completed = True
        if self.error:
            raise self.error
        return None

    def done(self):
        return self.completed


class StaticService:
    def __init__(self, *, get=None, listed=(), family=None):
        self.get_value = get
        self.listed = list(listed)
        self.family = family

    def get(self, request):
        return copy.deepcopy(self.get_value)

    def list(self, request):
        return copy.deepcopy(self.listed)

    def get_from_family(self, request):
        return copy.deepcopy(self.family)


class MachineTypeService(StaticService):
    def __init__(self, machines):
        self.machines = {
            machine['name']: copy.deepcopy(machine) for machine in machines
        }
        super().__init__(listed=machines)

    def get(self, request):
        name = request['machine_type']
        if name not in self.machines:
            raise NotFound(name)
        return copy.deepcopy(self.machines[name])


class ImageFamilyService(StaticService):
    def __init__(self, images):
        self.images = {
            family: copy.deepcopy(image) for family, image in images.items()
        }
        self.historical_images = []
        self.calls = []
        super().__init__()

    def get_from_family(self, request):
        self.calls.append(copy.deepcopy(request))
        family = request['family']
        if family not in self.images:
            raise NotFound(family)
        return copy.deepcopy(self.images[family])


class ResourceService:
    NAME_FIELDS = {
        'network': 'network',
        'subnet': 'subnetwork',
        'firewall': 'firewall',
        'instance': 'instance',
        'disk': 'disk',
    }
    BODY_FIELDS = {
        'network': 'network_resource',
        'subnet': 'subnetwork_resource',
        'firewall': 'firewall_resource',
        'instance': 'instance_resource',
        'disk': 'disk_resource',
    }

    def __init__(self, kind, timeline):
        self.kind = kind
        self.timeline = timeline
        self.items = {}
        self.insert_error = None
        self.insert_errors_by_name = {}
        self.delete_errors_by_name = {}
        self.get_after_insert_error = None
        self.get_misses_by_name = {}
        self.disk_service = None
        self.clients = None
        self.on_delete = None

    def _image_id(self, source_image):
        service = (self.clients or {}).get('images')
        candidates = []
        family = getattr(service, 'family', None)
        if family:
            candidates.append(family)
        candidates.extend(
            getattr(service, 'images', {}).values()
        )
        candidates.extend(getattr(service, 'historical_images', ()))
        for image in candidates:
            if gcp._link_equal(image.get('self_link'), source_image):
                return str(image.get('id') or '')
        return ''

    def _self_link(self, request, name):
        project = request['project']
        if self.kind in {'network', 'firewall'}:
            return f'https://compute.googleapis.com/compute/v1/projects/{project}/global/{self.kind}s/{name}'
        if self.kind == 'subnet':
            return f'https://compute.googleapis.com/compute/v1/projects/{project}/regions/{request["region"]}/subnetworks/{name}'
        return f'https://compute.googleapis.com/compute/v1/projects/{project}/zones/{request["zone"]}/{self.kind}s/{name}'

    def insert(self, request):
        self.timeline.append(('insert', self.kind, copy.deepcopy(request)))
        body = copy.deepcopy(request[self.BODY_FIELDS[self.kind]])
        name = body['name']
        if self.insert_error:
            raise self.insert_error
        if name in self.insert_errors_by_name:
            raise self.insert_errors_by_name[name]
        if name in self.items:
            raise ApiError(409, f'{name} already exists')
        body['id'] = str(1000 + len(self.items))
        body['self_link'] = self._self_link(request, name)
        if self.kind == 'instance':
            interfaces = body['network_interfaces']
            interfaces[0]['network_ip'] = f'10.42.1.{2 + len(self.items)}'
            interfaces[0]['access_configs'][0]['nat_ip'] = (
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
                    if self.disk_service is not None:
                        self.disk_service.items[disk['device_name']] = {
                            'name': disk['device_name'],
                            'id': str(8000 + len(self.disk_service.items)),
                            'self_link': actual['source'],
                            'description': initialize['description'],
                            'size_gb': initialize['disk_size_gb'],
                            'type_': initialize['disk_type'],
                            'provisioned_iops': initialize.get(
                                'provisioned_iops'
                            ),
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

    def get(self, request):
        name = request[self.NAME_FIELDS[self.kind]]
        remaining_misses = self.get_misses_by_name.get(name, 0)
        if remaining_misses:
            self.get_misses_by_name[name] = remaining_misses - 1
            raise NotFound(name)
        if self.get_after_insert_error:
            error = self.get_after_insert_error
            self.get_after_insert_error = None
            raise error
        if name not in self.items:
            raise NotFound(name)
        return copy.deepcopy(self.items[name])

    def delete(self, request):
        name = request[self.NAME_FIELDS[self.kind]]
        self.timeline.append(('delete', self.kind, name, copy.deepcopy(request)))
        if name in self.delete_errors_by_name:
            raise self.delete_errors_by_name[name]
        if name not in self.items:
            raise NotFound(name)
        if self.kind == 'instance' and self.disk_service is not None:
            for disk in self.items[name].get('disks', ()):
                if disk.get('auto_delete'):
                    disk_name = disk.get('source', '').rstrip('/').rsplit('/', 1)[-1]
                    self.disk_service.items.pop(disk_name, None)
        if self.on_delete:
            self.on_delete(name)
        del self.items[name]
        return Operation()


def clients_and_timeline(machine=None, image=None):
    timeline = []
    machine = machine or {
        'name': 'n2-standard-2',
        'id': '2002',
        'self_link': (
            'https://compute.googleapis.com/compute/v1/projects/p/zones/'
            'us-east1-b/machineTypes/n2-standard-2'
        ),
        'guest_cpus': 2,
        'memory_mb': 8192,
        'architecture': 'X86_64',
        'is_shared_cpu': False,
    }
    image = image or {
        'name': 'rocky-linux-9-v20260801',
        'id': '9001',
        'self_link': (
            'https://compute.googleapis.com/compute/v1/projects/'
            'rocky-linux-cloud/global/images/rocky-linux-9-v20260801'
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
        'machine_types': StaticService(get=machine, listed=[machine]),
        'images': StaticService(family=image),
        'networks': ResourceService('network', timeline),
        'subnetworks': ResourceService('subnet', timeline),
        'firewalls': ResourceService('firewall', timeline),
        'instances': ResourceService('instance', timeline),
        'disks': ResourceService('disk', timeline),
    }
    clients['instances'].disk_service = clients['disks']
    for service in clients.values():
        if isinstance(service, ResourceService):
            service.clients = clients
    return clients, timeline


def plan(**overrides):
    value = {
        'provider': 'gcp',
        'gcp_project_id': 'p',
        'region': 'us-east1',
        'gcp_zone': 'us-east1-b',
        'shape': 'n2-standard-2',
        'ocpus': 2,
        'memory_gb': 8,
        'benchmarks': ['sysbench'],
        'sysbench': {'workloads': ['cpu']},
        'storage': {'additional_volume': False},
    }
    value.update(overrides)
    return value


class GcpDiscoveryTests(unittest.TestCase):
    def test_machine_types_filter_non_pd_and_high_risk_variants(self):
        items = [
            {
                'name': name,
                'guest_cpus': 4,
                'memory_mb': 16384,
                'architecture': architecture,
                **extra,
            }
            for name, architecture, extra in (
                ('n2-standard-4', 'X86_64', {}),
                ('t2a-standard-4', 'ARM64', {}),
                ('c4a-standard-4', 'ARM64', {}),
                ('c4a-standard-4-lssd', 'ARM64', {}),
                ('c4a-standard-8-metal', 'ARM64', {}),
                ('c4a-standard-16', 'X86_64', {}),
                ('n4-standard-4', 'X86_64', {}),
                ('h4d-standard-192', 'X86_64', {}),
                ('n2-highmem-224-metal', 'X86_64', {}),
                ('c3-standard-8-lssd', 'X86_64', {}),
                ('n2-standard-4-gpu', 'X86_64', {'accelerators': [{}]}),
            )
        ]
        clients, _ = clients_and_timeline()
        clients['machine_types'] = StaticService(listed=items)

        response = gcp.machine_types('p', 'us-east1-b', clients=clients)

        self.assertEqual(
            [item['machine_type'] for item in response['items']],
            ['c4a-standard-4', 'n2-standard-4', 't2a-standard-4'],
        )
        c4a = response['items'][0]
        self.assertEqual(c4a['architecture'], 'arm64')
        self.assertEqual(c4a['disk_type'], 'hyperdisk-balanced')
        self.assertEqual(c4a['disk_interface'], 'NVME')
        self.assertEqual(c4a['network_interface_type'], 'GVNIC')
        self.assertEqual(c4a['disk_provisioned_iops'], 3000)
        self.assertEqual(c4a['disk_provisioned_throughput_mibps'], 140)
        self.assertEqual(response['items'][2]['architecture'], 'arm64')

    def test_image_identity_must_be_complete(self):
        clients, _ = clients_and_timeline()
        clients['images'] = StaticService(family={
            'name': 'rocky-linux-9-v1',
            'id': '1',
            'self_link': '',
            'status': 'READY',
            'architecture': 'X86_64',
        })

        with self.assertRaisesRegex(RuntimeError, 'incomplete immutable identity'):
            gcp.latest_rocky_linux_9_image('x86_64', clients=clients)


class GcpProvisionTests(unittest.TestCase):
    def test_partial_persisted_image_contract_fails_before_cloud_insert(self):
        clients, timeline = clients_and_timeline()
        job = {
            'id': 'partial1',
            'resources': {'image_id': '9001'},
        }

        with self.assertRaisesRegex(RuntimeError, 'image identity is incomplete'):
            gcp.provision(
                job,
                plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertFalse(any(event[0] == 'insert' for event in timeline))

    def test_sctp_firewall_uses_protocol_132_without_port_bounds(self):
        clients, _ = clients_and_timeline()

        firewall = gcp._firewall_resource(
            clients,
            'a1b2c3d4',
            'benchmark-a1b2c3d4-iperf',
            'projects/p/global/networks/benchmark-a1b2c3d4',
            role='iperf-firewall',
            target_tag='benchmark-a1b2c3d4-iperf-peer',
            source_tags=('benchmark-a1b2c3d4-runner',),
            protocols=('sctp',),
        )

        self.assertEqual(
            firewall['allowed'],
            [
                {'I_p_protocol': 'tcp', 'ports': ['5201']},
                {'I_p_protocol': '132'},
            ],
        )

    def test_web_firewall_ports_are_selection_scoped(self):
        clients, _ = clients_and_timeline()
        cases = (
            (('apachebench',), ['80']),
            (('deathstarbench',), ['5000', '8080']),
            (
                ('apachebench', 'deathstarbench'),
                ['80', '5000', '8080'],
            ),
        )
        for selected, ports in cases:
            with self.subTest(selected=selected):
                firewall = gcp._firewall_resource(
                    clients,
                    'a1b2c3d4',
                    'benchmark-a1b2c3d4-web',
                    'projects/p/global/networks/benchmark-a1b2c3d4',
                    role='web-firewall',
                    target_tag='benchmark-a1b2c3d4-runner',
                    source_tags=('benchmark-a1b2c3d4-loadgen',),
                    web_benchmarks=selected,
                )
                self.assertEqual(
                    firewall['allowed'],
                    [{'I_p_protocol': 'tcp', 'ports': ports}],
                )
                self.assertEqual(
                    firewall['source_tags'],
                    ['benchmark-a1b2c3d4-loadgen'],
                )
                self.assertEqual(
                    firewall['target_tags'],
                    ['benchmark-a1b2c3d4-runner'],
                )

    def test_web_selection_provisions_fixed_isolated_load_generator(self):
        clients, timeline = clients_and_timeline()
        job = {'id': 'web12345', 'resources': {}}

        resources = gcp.provision(
            job,
            plan(benchmarks=['apachebench', 'deathstarbench']),
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        self.assertEqual(
            set(clients['instances'].items),
            {
                'benchmark-web12345-runner',
                'benchmark-web12345-loadgen',
            },
        )
        self.assertNotIn('benchmark-web12345-iperf-peer', clients['instances'].items)
        loadgen = clients['instances'].items['benchmark-web12345-loadgen']
        runner = clients['instances'].items['benchmark-web12345-runner']
        self.assertEqual(
            loadgen['machine_type'].rsplit('/', 1)[-1],
            'n2-standard-2',
        )
        self.assertEqual(
            loadgen['labels']['benchmark-role'],
            'load-generator',
        )
        self.assertEqual(loadgen['service_accounts'], [])
        self.assertEqual(
            loadgen['network_interfaces'][0]['subnetwork'],
            runner['network_interfaces'][0]['subnetwork'],
        )
        metadata = {
            item['key']: item['value']
            for item in loadgen['metadata']['items']
        }
        self.assertEqual(metadata['block-project-ssh-keys'], 'TRUE')
        self.assertEqual(metadata['enable-oslogin'], 'FALSE')
        self.assertTrue(metadata['ssh-keys'].startswith(
            'benchmark:ssh-ed25519 '
        ))
        boot = clients['disks'].items['benchmark-web12345-loadgen-boot']
        self.assertEqual(boot['type_'].rsplit('/', 1)[-1], 'pd-balanced')
        self.assertEqual(boot['size_gb'], 20)
        self.assertEqual(boot['labels']['benchmark-role'], 'load-generator-boot')

        web = clients['firewalls'].items['benchmark-web12345-web']
        self.assertEqual(web['source_tags'], ['benchmark-web12345-loadgen'])
        self.assertEqual(web['target_tags'], ['benchmark-web12345-runner'])
        self.assertEqual(
            web['allowed'],
            [{
                'I_p_protocol': 'tcp',
                'ports': ['80', '5000', '8080'],
            }],
        )
        ssh = clients['firewalls'].items['benchmark-web12345-ssh']
        self.assertEqual(
            set(ssh['target_tags']),
            {
                'benchmark-web12345-runner',
                'benchmark-web12345-loadgen',
            },
        )
        self.assertEqual(resources['loadgen_shape'], 'n2-standard-2')
        self.assertEqual(resources['loadgen_vcpus'], 2)
        self.assertEqual(resources['loadgen_memory_gb'], 8)
        self.assertEqual(resources['loadgen_architecture'], 'x86_64')
        self.assertTrue(resources['loadgen_public_ip'])
        self.assertTrue(resources['loadgen_private_ip'])
        self.assertEqual(resources['loadgen_network_bandwidth_gbps'], 10)
        self.assertEqual(resources['loadgen_network_capacity_kind'], 'maximum')
        self.assertEqual(resources['gcp_loadgen_boot_disk_type'], 'pd-balanced')
        uuid.UUID(resources['gcp_loadgen_instance_request_id'])
        self.assertEqual(
            resources['gcp_loadgen_boot_disk_request_id'],
            resources['gcp_loadgen_instance_request_id'],
        )
        loadgen_inserts = [
            event for event in timeline
            if event[0] == 'insert'
            and event[1] == 'instance'
            and event[2]['instance_resource']['name']
            == 'benchmark-web12345-loadgen'
        ]
        self.assertEqual(len(loadgen_inserts), 1)

    def test_web_loadgen_unavailable_fails_before_first_insert(self):
        clients, timeline = clients_and_timeline()
        target = {
            'name': 'e2-standard-2',
            'id': '2202',
            'self_link': (
                'projects/p/zones/us-east1-b/machineTypes/e2-standard-2'
            ),
            'guest_cpus': 2,
            'memory_mb': 8192,
            'architecture': 'X86_64',
            'is_shared_cpu': False,
        }
        clients['machine_types'] = MachineTypeService([target])
        job = {'id': 'nowebn2', 'resources': {}}

        with self.assertRaisesRegex(ValueError, 'unavailable'):
            gcp.provision(
                job,
                plan(
                    shape='e2-standard-2',
                    benchmarks=['apachebench'],
                ),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertFalse(any(event[0] == 'insert' for event in timeline))

    def test_fixed_n2_loadgen_infers_x86_when_api_omits_architecture(self):
        machine = {
            'name': 'n2-standard-2',
            'id': '2002',
            'self_link': (
                'projects/p/zones/us-east1-b/machineTypes/n2-standard-2'
            ),
            'guest_cpus': 2,
            'memory_mb': 8192,
            'is_shared_cpu': False,
        }
        clients, _ = clients_and_timeline()
        clients['machine_types'] = MachineTypeService([machine])

        details = gcp._loadgen_machine_details(
            'p', 'us-east1-b', clients
        )

        self.assertEqual(details['architecture'], 'x86_64')
        self.assertEqual(details['machine_type'], 'n2-standard-2')
        self.assertEqual(details['disk_type'], 'pd-balanced')

    def test_fixed_n2_loadgen_rejects_explicit_arm_architecture(self):
        machine = {
            'name': 'n2-standard-2',
            'id': '2002',
            'self_link': (
                'projects/p/zones/us-east1-b/machineTypes/n2-standard-2'
            ),
            'guest_cpus': 2,
            'memory_mb': 8192,
            'architecture': 'ARM64',
            'is_shared_cpu': False,
        }
        clients, _ = clients_and_timeline()
        clients['machine_types'] = MachineTypeService([machine])

        with self.assertRaisesRegex(ValueError, 'metadata is incompatible'):
            gcp._loadgen_machine_details('p', 'us-east1-b', clients)

    def test_c4a_web_target_keeps_loadgen_on_x86_pd_balanced(self):
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
        n2 = {
            'name': 'n2-standard-2',
            'id': '2002',
            'self_link': (
                'projects/p/zones/us-east1-b/machineTypes/n2-standard-2'
            ),
            'guest_cpus': 2,
            'memory_mb': 8192,
            'architecture': 'X86_64',
            'is_shared_cpu': False,
        }
        arm_image = {
            'name': 'rocky-linux-9-arm64-v20260813',
            'id': '9401',
            'self_link': (
                'projects/rocky-linux-cloud/global/images/'
                'rocky-linux-9-arm64-v20260813'
            ),
            'status': 'READY',
            'architecture': 'ARM64',
        }
        x86_image = {
            'name': 'rocky-linux-9-v20260813',
            'id': '9002',
            'self_link': (
                'projects/rocky-linux-cloud/global/images/'
                'rocky-linux-9-v20260813'
            ),
            'status': 'READY',
            'architecture': 'X86_64',
        }
        clients, _ = clients_and_timeline(machine=c4a, image=arm_image)
        clients['machine_types'] = MachineTypeService([c4a, n2])
        clients['images'] = ImageFamilyService({
            'rocky-linux-9-arm64': arm_image,
            'rocky-linux-9': x86_image,
        })
        job = {'id': 'c4aweb99', 'resources': {}}

        resources = gcp.provision(
            job,
            plan(
                shape='c4a-standard-4',
                ocpus=4,
                memory_gb=16,
                benchmarks=['deathstarbench'],
            ),
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        runner_boot = clients['disks'].items[
            'benchmark-c4aweb99-runner-boot'
        ]
        loadgen_boot = clients['disks'].items[
            'benchmark-c4aweb99-loadgen-boot'
        ]
        self.assertEqual(
            runner_boot['type_'].rsplit('/', 1)[-1],
            'hyperdisk-balanced',
        )
        self.assertEqual(
            loadgen_boot['type_'].rsplit('/', 1)[-1],
            'pd-balanced',
        )
        self.assertIsNone(loadgen_boot['provisioned_iops'])
        self.assertIsNone(loadgen_boot['provisioned_throughput'])
        loadgen = clients['instances'].items['benchmark-c4aweb99-loadgen']
        self.assertEqual(
            loadgen['machine_type'].rsplit('/', 1)[-1],
            'n2-standard-2',
        )
        self.assertNotIn('nic_type', loadgen['network_interfaces'][0])
        self.assertEqual(resources['architecture'], 'arm64')
        self.assertEqual(resources['loadgen_architecture'], 'x86_64')
        self.assertEqual(resources['loadgen_image_id'], '9002')

    def test_retry_pins_runner_and_loadgen_images_across_family_rollover(self):
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
        n2 = {
            'name': 'n2-standard-2',
            'id': '2002',
            'self_link': (
                'projects/p/zones/us-east1-b/machineTypes/n2-standard-2'
            ),
            'guest_cpus': 2,
            'memory_mb': 8192,
            'architecture': 'X86_64',
            'is_shared_cpu': False,
        }
        old_arm = {
            'name': 'rocky-linux-9-arm64-v1',
            'id': '9401',
            'self_link': (
                'projects/rocky-linux-cloud/global/images/'
                'rocky-linux-9-arm64-v1'
            ),
            'status': 'READY',
            'architecture': 'ARM64',
        }
        old_x86 = {
            'name': 'rocky-linux-9-v1',
            'id': '9001',
            'self_link': (
                'projects/rocky-linux-cloud/global/images/rocky-linux-9-v1'
            ),
            'status': 'READY',
            'architecture': 'X86_64',
        }
        clients, timeline = clients_and_timeline(machine=c4a, image=old_arm)
        clients['machine_types'] = MachineTypeService([c4a, n2])
        clients['images'] = ImageFamilyService({
            'rocky-linux-9-arm64': old_arm,
            'rocky-linux-9': old_x86,
        })
        loadgen_name = 'benchmark-rollover-loadgen'
        clients['instances'].insert_errors_by_name[loadgen_name] = ApiError(
            408, 'response outcome unknown'
        )
        job = {'id': 'rollover', 'resources': {}}
        persisted = []
        selected = plan(
            shape='c4a-standard-4',
            ocpus=4,
            memory_gb=16,
            benchmarks=['apachebench'],
        )

        with self.assertRaises(ApiError):
            gcp.provision(
                job,
                selected,
                public_key=PUBLIC_KEY,
                clients=clients,
                persist=lambda current: persisted.append(
                    copy.deepcopy(current)
                ),
            )

        self.assertEqual(job['resources']['image_id'], '9401')
        self.assertEqual(job['resources']['gcp_loadgen_image_id'], '9001')
        self.assertEqual(job['resources']['loadgen_image_id'], '9001')
        restarted = json.loads(json.dumps(persisted[-1]))
        new_arm = {
            **old_arm,
            'name': 'rocky-linux-9-arm64-v2',
            'id': '9499',
            'self_link': (
                'projects/rocky-linux-cloud/global/images/'
                'rocky-linux-9-arm64-v2'
            ),
        }
        new_x86 = {
            **old_x86,
            'name': 'rocky-linux-9-v2',
            'id': '9999',
            'self_link': (
                'projects/rocky-linux-cloud/global/images/rocky-linux-9-v2'
            ),
        }
        rolled = ImageFamilyService({
            'rocky-linux-9-arm64': new_arm,
            'rocky-linux-9': new_x86,
        })
        rolled.historical_images = [old_arm, old_x86]
        clients['images'] = rolled
        del clients['instances'].insert_errors_by_name[loadgen_name]
        timeline.clear()

        resources = gcp.provision(
            restarted,
            selected,
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        self.assertEqual(rolled.calls, [])
        self.assertEqual(resources['image_id'], '9401')
        self.assertEqual(resources['gcp_loadgen_image_id'], '9001')
        self.assertEqual(
            clients['disks'].items['benchmark-rollover-runner-boot'][
                'source_image_id'
            ],
            '9401',
        )
        self.assertEqual(
            clients['disks'].items['benchmark-rollover-loadgen-boot'][
                'source_image_id'
            ],
            '9001',
        )

    def test_exact_name_retry_adopts_owned_loadgen_with_same_request_id(self):
        clients, timeline = clients_and_timeline()
        job = {'id': 'adoptweb', 'resources': {}}
        selected = plan(benchmarks=['apachebench'])
        gcp.provision(
            job,
            selected,
            public_key=PUBLIC_KEY,
            clients=clients,
        )
        request_id = job['resources']['gcp_loadgen_instance_request_id']
        timeline.clear()

        gcp.provision(
            job,
            selected,
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        retry = next(
            event for event in timeline
            if event[0] == 'insert'
            and event[1] == 'instance'
            and event[2]['instance_resource']['name']
            == 'benchmark-adoptweb-loadgen'
        )
        self.assertEqual(retry[2]['request_id'], request_id)
        self.assertFalse(
            job['resources']['gcp_loadgen_instance_create_ambiguous']
        )
        self.assertEqual(
            len([
                name for name in clients['instances'].items
                if name == 'benchmark-adoptweb-loadgen'
            ]),
            1,
        )

    def test_adoption_rejects_foreign_project_same_name_boot_disk(self):
        clients, timeline = clients_and_timeline()
        job = {'id': 'foreign1', 'resources': {}}
        selected = plan()
        gcp.provision(
            job,
            selected,
            public_key=PUBLIC_KEY,
            clients=clients,
        )
        runner = clients['instances'].items['benchmark-foreign1-runner']
        runner['disks'][0]['source'] = (
            'projects/foreign/zones/us-east1-b/disks/'
            'benchmark-foreign1-runner-boot'
        )
        restarted = json.loads(json.dumps(job))
        timeline.clear()

        with self.assertRaisesRegex(RuntimeError, 'disk .* source differs'):
            gcp.provision(
                restarted,
                selected,
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertFalse(any(event[0] == 'delete' for event in timeline))

    def test_adoption_polls_then_rejects_wrong_boot_source_image_id(self):
        clients, timeline = clients_and_timeline()
        job = {'id': 'badimage', 'resources': {}}
        selected = plan()
        gcp.provision(
            job,
            selected,
            public_key=PUBLIC_KEY,
            clients=clients,
        )
        boot_name = 'benchmark-badimage-runner-boot'
        clients['disks'].items[boot_name]['source_image_id'] = 'foreign-image'
        clients['disks'].get_misses_by_name[boot_name] = 1
        restarted = json.loads(json.dumps(job))
        timeline.clear()

        with (
            patch.object(gcp.time, 'sleep') as sleep,
            self.assertRaisesRegex(RuntimeError, 'source image ID'),
        ):
            gcp.provision(
                restarted,
                selected,
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertGreaterEqual(sleep.call_count, 1)
        self.assertFalse(any(event[0] == 'delete' for event in timeline))

    def test_provision_builds_isolated_pd_balanced_graph(self):
        clients, timeline = clients_and_timeline()
        job = {'id': 'a1b2c3d4', 'resources': {}}
        persisted = []
        selected = plan(
            benchmarks=['fio', 'iperf3'],
            iperf3={'protocols': ['tcp', 'udp']},
            storage={'additional_volume': True, 'additional_size_gb': 100},
        )

        resources = gcp.provision(
            job,
            selected,
            public_key=PUBLIC_KEY,
            clients=clients,
            persist=lambda current: persisted.append(copy.deepcopy(current['resources'])),
        )

        self.assertEqual(resources['gcp_compute_project_id'], '3977225790263303761')
        self.assertEqual(resources['gcp_data_disk_type'], 'pd-balanced')
        self.assertEqual(resources['gcp_data_disk_size_gb'], 100)
        self.assertEqual(resources['gcp_data_device_name'], 'benchmark-a1b2c3d4-data')
        first_insert = next(index for index, item in enumerate(timeline) if item[0] == 'insert')
        self.assertGreaterEqual(first_insert, 0)
        self.assertTrue(any(
            state.get('gcp_resource_prefix') == 'benchmark-a1b2c3d4'
            and state.get('gcp_compute_project_id') == '3977225790263303761'
            for state in persisted
        ))
        for state in persisted:
            if state.get('gcp_runner_boot_disk_name'):
                self.assertEqual(
                    state.get('gcp_instance_name'),
                    'benchmark-a1b2c3d4-runner',
                )
                uuid.UUID(state['gcp_instance_request_id'])
                self.assertEqual(
                    state.get('gcp_runner_boot_disk_request_id'),
                    state.get('gcp_instance_request_id'),
                )
            if state.get('gcp_peer_boot_disk_name'):
                self.assertEqual(
                    state.get('gcp_peer_instance_name'),
                    'benchmark-a1b2c3d4-iperf-peer',
                )
                uuid.UUID(state['gcp_peer_instance_request_id'])
                self.assertEqual(
                    state.get('gcp_peer_boot_disk_request_id'),
                    state.get('gcp_peer_instance_request_id'),
                )
        for event in timeline:
            if event[0] == 'insert':
                uuid.UUID(event[2]['request_id'])

        network = clients['networks'].items['benchmark-a1b2c3d4']
        self.assertNotIn('labels', network)
        self.assertFalse(network['auto_create_subnetworks'])
        data = clients['disks'].items['benchmark-a1b2c3d4-data']
        self.assertEqual(data['type_'].rsplit('/', 1)[-1], 'pd-balanced')
        self.assertEqual(data['labels']['benchmark-role'], 'data')

        runner = clients['instances'].items['benchmark-a1b2c3d4-runner']
        self.assertEqual(runner['service_accounts'], [])
        metadata = {item['key']: item['value'] for item in runner['metadata']['items']}
        self.assertEqual(metadata['block-project-ssh-keys'], 'TRUE')
        self.assertEqual(metadata['enable-oslogin'], 'FALSE')
        self.assertTrue(metadata['ssh-keys'].startswith('benchmark:ssh-ed25519 '))
        self.assertEqual(len(runner['disks']), 2)
        self.assertTrue(runner['disks'][0]['auto_delete'])
        self.assertFalse(runner['disks'][1]['auto_delete'])

        peer = clients['instances'].items['benchmark-a1b2c3d4-iperf-peer']
        peer_metadata = {
            item['key']: item['value'] for item in peer['metadata']['items']
        }
        self.assertEqual(peer_metadata['block-project-ssh-keys'], 'TRUE')
        self.assertEqual(peer_metadata['enable-oslogin'], 'FALSE')
        self.assertIn('firewall-cmd --permanent --add-port=5201/tcp', peer_metadata['startup-script'])
        self.assertIn('firewall-cmd --permanent --add-port=5201/udp', peer_metadata['startup-script'])
        self.assertEqual(peer['machine_type'], runner['machine_type'])
        self.assertEqual(len(peer['disks']), 1)
        runner_boot = clients['disks'].items['benchmark-a1b2c3d4-runner-boot']
        peer_boot = clients['disks'].items['benchmark-a1b2c3d4-iperf-peer-boot']
        self.assertIn('benchmark-role=runner-boot', runner_boot['description'])
        self.assertIn('benchmark-role=iperf-peer-boot', peer_boot['description'])

    def test_c4a_uses_hyperdisk_nvme_and_gvnic_for_the_full_graph(self):
        machine = {
            'name': 'c4a-standard-4',
            'id': '4004',
            'self_link': (
                'https://compute.googleapis.com/compute/v1/projects/p/zones/'
                'us-east1-b/machineTypes/c4a-standard-4'
            ),
            'guest_cpus': 4,
            'memory_mb': 16384,
            'architecture': 'ARM64',
            'is_shared_cpu': False,
        }
        image = {
            'name': 'rocky-linux-9-arm64-v20260813',
            'id': '9401',
            'self_link': (
                'https://compute.googleapis.com/compute/v1/projects/'
                'rocky-linux-cloud/global/images/'
                'rocky-linux-9-arm64-v20260813'
            ),
            'status': 'READY',
            'architecture': 'ARM64',
        }
        clients, _ = clients_and_timeline(machine=machine, image=image)
        job = {'id': 'c4a4c4a4', 'resources': {}}

        resources = gcp.provision(
            job,
            plan(
                shape='c4a-standard-4',
                ocpus=4,
                memory_gb=16,
                benchmarks=['fio', 'iperf3'],
                iperf3={'protocols': ['tcp']},
                storage={
                    'additional_volume': True,
                    'additional_size_gb': 100,
                },
            ),
            public_key=PUBLIC_KEY,
            clients=clients,
        )

        self.assertEqual(resources['architecture'], 'arm64')
        self.assertEqual(resources['gcp_boot_disk_type'], 'hyperdisk-balanced')
        self.assertEqual(resources['gcp_data_disk_type'], 'hyperdisk-balanced')
        self.assertEqual(resources['gcp_boot_disk_provisioned_iops'], 3000)
        self.assertEqual(
            resources['gcp_boot_disk_provisioned_throughput_mibps'], 140
        )
        self.assertEqual(resources['gcp_data_disk_provisioned_iops'], 3000)
        self.assertEqual(
            resources['gcp_data_disk_provisioned_throughput_mibps'], 140
        )
        self.assertEqual(resources['gcp_disk_interface'], 'NVME')
        self.assertEqual(resources['gcp_network_interface_type'], 'GVNIC')

        for name in (
            'benchmark-c4a4c4a4-runner-boot',
            'benchmark-c4a4c4a4-iperf-peer-boot',
            'benchmark-c4a4c4a4-data',
        ):
            disk = clients['disks'].items[name]
            self.assertEqual(
                disk['type_'].rsplit('/', 1)[-1], 'hyperdisk-balanced'
            )
            self.assertEqual(disk['provisioned_iops'], 3000)
            self.assertEqual(disk['provisioned_throughput'], 140)

        for name in (
            'benchmark-c4a4c4a4-runner',
            'benchmark-c4a4c4a4-iperf-peer',
        ):
            instance = clients['instances'].items[name]
            self.assertEqual(
                instance['network_interfaces'][0]['nic_type'], 'GVNIC'
            )
            self.assertTrue(all(
                disk['interface'] == 'NVME' for disk in instance['disks']
            ))

    def test_c4a_instance_collision_with_scsi_disk_is_not_adopted(self):
        clients, _ = clients_and_timeline()
        service = clients['instances']
        expected = gcp._instance_resource(
            clients,
            job_id='abc12345',
            name='benchmark-abc12345-runner',
            role='runner',
            machine_type=(
                'projects/p/zones/us-east1-b/machineTypes/c4a-standard-4'
            ),
            image={'self_link': 'projects/rocky/global/images/rocky-arm'},
            project_id='p',
            zone='us-east1-b',
            network='projects/p/global/networks/benchmark-abc12345',
            subnet=(
                'projects/p/regions/us-east1/subnetworks/benchmark-abc12345'
            ),
            network_tag='benchmark-abc12345-runner',
            boot_size_gb=100,
            disk_type='hyperdisk-balanced',
            disk_interface='NVME',
            network_interface_type='GVNIC',
            disk_provisioned_iops=3000,
            disk_provisioned_throughput_mibps=140,
            public_key=PUBLIC_KEY,
        )
        service.insert({
            'project': 'p',
            'zone': 'us-east1-b',
            'instance_resource': expected,
            'request_id': '11111111-1111-4111-8111-111111111111',
        })
        service.items['benchmark-abc12345-runner']['disks'][0][
            'interface'
        ] = 'SCSI'
        service.insert_error = ApiError(409, 'already exists')

        with self.assertRaisesRegex(RuntimeError, 'interface differs'):
            gcp._create_named_resource(
                {'id': 'abc12345', 'resources': {}},
                None,
                prefix='gcp_instance',
                name='benchmark-abc12345-runner',
                client=service,
                clients=clients,
                insert_request_type='InsertInstanceRequest',
                insert_values={
                    'project': 'p',
                    'zone': 'us-east1-b',
                    'instance_resource': expected,
                },
                get_request_type='GetInstanceRequest',
                get_values={
                    'project': 'p',
                    'zone': 'us-east1-b',
                    'instance': 'benchmark-abc12345-runner',
                },
            )

    def test_kernel_memory_rejection_happens_before_first_insert(self):
        machine = {
            'name': 'e2-small',
            'id': '2',
            'self_link': 'projects/p/zones/us-east1-b/machineTypes/e2-small',
            'guest_cpus': 2,
            'memory_mb': 2048,
            'architecture': 'X86_64',
        }
        clients, timeline = clients_and_timeline(machine=machine)
        job = {'id': 'feed1234', 'resources': {}}
        selected = plan(
            shape='e2-small',
            ocpus=2,
            memory_gb=2,
            benchmarks=['phoronix'],
            phoronix={'profiles': ['build_linux_kernel']},
        )

        with self.assertRaisesRegex(ValueError, 'at least 4 GiB'):
            gcp.provision(
                job,
                selected,
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertFalse(any(item[0] == 'insert' for item in timeline))

    def test_visibility_lag_after_accepted_insert_keeps_contract(self):
        timeline = []
        service = ResourceService('network', timeline)
        service.get_after_insert_error = NotFound('visibility lag')
        job = {'id': 'abc12345', 'resources': {}}
        clients = {'networks': service}

        with self.assertRaises(NotFound):
            gcp._create_named_resource(
                job,
                None,
                prefix='gcp_network',
                name='benchmark-abc12345',
                client=service,
                clients=clients,
                insert_request_type='InsertNetworkRequest',
                insert_values={
                    'project': 'p',
                    'network_resource': gcp._network_resource(
                        clients, 'abc12345', 'benchmark-abc12345'
                    ),
                },
                get_request_type='GetNetworkRequest',
                get_values={'project': 'p', 'network': 'benchmark-abc12345'},
            )

        self.assertTrue(job['resources']['gcp_network_create_ambiguous'])
        self.assertIn('gcp_network_request_id', job['resources'])
        self.assertNotIn('gcp_network_id', job['resources'])

    def test_completed_insert_error_clears_consumed_request_id(self):
        timeline = []
        service = ResourceService('network', timeline)
        original_insert = service.insert
        requests = []

        def insert(request):
            requests.append(copy.deepcopy(request))
            if len(requests) == 1:
                return Operation(ApiError(
                    400,
                    'texte localise',
                    reason='RESOURCE_NOT_READY',
                ))
            return original_insert(request)

        service.insert = insert
        job = {'id': 'abc12345', 'resources': {}}
        clients = {'networks': service}
        values = dict(
            prefix='gcp_network',
            name='benchmark-abc12345',
            client=service,
            clients=clients,
            insert_request_type='InsertNetworkRequest',
            insert_values={
                'project': 'p',
                'network_resource': gcp._network_resource(
                    clients, 'abc12345', 'benchmark-abc12345'
                ),
            },
            get_request_type='GetNetworkRequest',
            get_values={'project': 'p', 'network': 'benchmark-abc12345'},
        )

        with (
            patch.object(gcp.time, 'sleep'),
            self.assertRaises(ApiError),
        ):
            gcp._create_named_resource(job, None, **values)

        self.assertNotIn('gcp_network_request_id', job['resources'])
        gcp._create_named_resource(job, None, **values)

        self.assertEqual(len(requests), 2)
        self.assertNotEqual(
            requests[0]['request_id'],
            requests[1]['request_id'],
        )
        self.assertFalse(job['resources']['gcp_network_create_ambiguous'])

    def test_timeout_throttle_and_cancellation_keep_create_contract(self):
        for status in (408, 429, 499):
            with self.subTest(status=status):
                timeline = []
                service = ResourceService('network', timeline)
                service.insert_error = ApiError(status, 'ambiguous transport outcome')
                job = {'id': 'abc12345', 'resources': {}}
                clients = {'networks': service}

                with self.assertRaises(ApiError):
                    gcp._create_named_resource(
                        job,
                        None,
                        prefix='gcp_network',
                        name='benchmark-abc12345',
                        client=service,
                        clients=clients,
                        insert_request_type='InsertNetworkRequest',
                        insert_values={
                            'project': 'p',
                            'network_resource': gcp._network_resource(
                                clients, 'abc12345', 'benchmark-abc12345'
                            ),
                        },
                        get_request_type='GetNetworkRequest',
                        get_values={
                            'project': 'p',
                            'network': 'benchmark-abc12345',
                        },
                    )

                self.assertTrue(
                    job['resources']['gcp_network_create_ambiguous']
                )
                uuid.UUID(job['resources']['gcp_network_request_id'])

    def test_instance_collision_with_foreign_nic_is_not_adopted(self):
        clients, timeline = clients_and_timeline()
        service = clients['instances']
        expected = gcp._instance_resource(
            clients,
            job_id='abc12345',
            name='benchmark-abc12345-runner',
            role='runner',
            machine_type=(
                'projects/p/zones/us-east1-b/machineTypes/n2-standard-2'
            ),
            image={'self_link': 'projects/rocky/global/images/rocky'},
            project_id='p',
            zone='us-east1-b',
            network='projects/p/global/networks/benchmark-abc12345',
            subnet=(
                'projects/p/regions/us-east1/subnetworks/benchmark-abc12345'
            ),
            network_tag='benchmark-abc12345-runner',
            boot_size_gb=100,
            public_key=PUBLIC_KEY,
        )
        request = {
            'project': 'p',
            'zone': 'us-east1-b',
            'instance_resource': expected,
            'request_id': '11111111-1111-4111-8111-111111111111',
        }
        service.insert(request)
        service.items['benchmark-abc12345-runner']['network_interfaces'][0][
            'network'
        ] = 'projects/foreign/global/networks/benchmark-abc12345'
        service.insert_error = ApiError(409, 'already exists')
        job = {'id': 'abc12345', 'resources': {}}

        with self.assertRaisesRegex(RuntimeError, 'NIC network differs'):
            gcp._create_named_resource(
                job,
                None,
                prefix='gcp_instance',
                name='benchmark-abc12345-runner',
                client=service,
                clients=clients,
                insert_request_type='InsertInstanceRequest',
                insert_values={
                    'project': 'p',
                    'zone': 'us-east1-b',
                    'instance_resource': expected,
                },
                get_request_type='GetInstanceRequest',
                get_values={
                    'project': 'p',
                    'zone': 'us-east1-b',
                    'instance': 'benchmark-abc12345-runner',
                },
            )

    def test_server_default_nic_type_is_accepted_when_request_omits_it(self):
        clients, timeline = clients_and_timeline()
        expected = gcp._instance_resource(
            clients,
            job_id='abc12345',
            name='benchmark-abc12345-runner',
            role='runner',
            machine_type=(
                'projects/p/zones/us-east1-b/machineTypes/t2a-standard-4'
            ),
            image={'self_link': 'projects/rocky/global/images/rocky-arm'},
            project_id='p',
            zone='us-east1-b',
            network='projects/p/global/networks/benchmark-abc12345',
            subnet=(
                'projects/p/regions/us-east1/subnetworks/benchmark-abc12345'
            ),
            network_tag='benchmark-abc12345-runner',
            boot_size_gb=100,
            public_key=PUBLIC_KEY,
        )
        service = clients['instances']
        service.insert({
            'project': 'p',
            'zone': 'us-east1-b',
            'instance_resource': expected,
            'request_id': '11111111-1111-4111-8111-111111111111',
        })
        actual = service.items['benchmark-abc12345-runner']
        actual['network_interfaces'][0]['nic_type'] = 'GVNIC'

        gcp._assert_insert_resource_matches(
            'gcp_instance',
            actual,
            expected,
            project_id='p',
            zone='us-east1-b',
        )

    def test_h3_explicitly_requests_gvnic(self):
        clients, _ = clients_and_timeline()
        instance = gcp._instance_resource(
            clients,
            job_id='abc12345',
            name='benchmark-abc12345-runner',
            role='runner',
            machine_type='projects/p/zones/us-east1-b/machineTypes/h3-standard-88',
            image={'self_link': 'projects/rocky/global/images/rocky'},
            project_id='p',
            zone='us-east1-b',
            network='projects/p/global/networks/benchmark-abc12345',
            subnet='projects/p/regions/us-east1/subnetworks/benchmark-abc12345',
            network_tag='benchmark-abc12345-runner',
            boot_size_gb=100,
            public_key=PUBLIC_KEY,
        )

        self.assertEqual(instance['network_interfaces'][0]['nic_type'], 'GVNIC')
        self.assertEqual(
            instance['scheduling']['on_host_maintenance'], 'TERMINATE'
        )

    def test_confirmed_runner_rejection_clears_implicit_boot_contract(self):
        clients, timeline = clients_and_timeline()
        clients['instances'].insert_errors_by_name[
            'benchmark-abc12345-runner'
        ] = ApiError(400, 'invalid runner')
        job = {'id': 'abc12345', 'resources': {}, 'status': 'provisioning'}

        with self.assertRaises(ApiError):
            gcp.provision(
                job,
                plan(),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertNotIn('gcp_instance_name', job['resources'])
        self.assertNotIn('gcp_runner_boot_disk_name', job['resources'])
        gcp.destroy_resources(job, clients=clients)
        self.assertEqual(job['status'], 'destroyed')

    def test_confirmed_peer_rejection_clears_implicit_boot_contract(self):
        clients, timeline = clients_and_timeline()
        clients['instances'].insert_errors_by_name[
            'benchmark-abc12345-iperf-peer'
        ] = ApiError(403, 'peer denied')
        job = {'id': 'abc12345', 'resources': {}, 'status': 'provisioning'}

        with self.assertRaises(ApiError):
            gcp.provision(
                job,
                plan(
                    benchmarks=['iperf3'],
                    iperf3={'protocols': ['tcp']},
                ),
                public_key=PUBLIC_KEY,
                clients=clients,
            )

        self.assertNotIn('gcp_peer_instance_name', job['resources'])
        self.assertNotIn('gcp_peer_boot_disk_name', job['resources'])
        gcp.destroy_resources(job, clients=clients)
        self.assertEqual(job['status'], 'destroyed')

    def test_resumed_ambiguous_runner_rejection_retains_contract(self):
        clients, _ = clients_and_timeline()
        runner_name = 'benchmark-abc12345-runner'
        clients['instances'].insert_errors_by_name[runner_name] = ApiError(
            408, 'response lost'
        )
        job = {'id': 'abc12345', 'resources': {}, 'status': 'provisioning'}

        with self.assertRaises(ApiError):
            gcp.provision(
                job, plan(), public_key=PUBLIC_KEY, clients=clients
            )
        request_id = job['resources']['gcp_instance_request_id']
        clients['instances'].insert_errors_by_name[runner_name] = ApiError(
            403, 'permission changed'
        )

        with self.assertRaises(ApiError):
            gcp.provision(
                job, plan(), public_key=PUBLIC_KEY, clients=clients
            )

        self.assertEqual(job['resources']['gcp_instance_name'], runner_name)
        self.assertEqual(
            job['resources']['gcp_instance_request_id'], request_id
        )
        self.assertTrue(job['resources']['gcp_instance_create_ambiguous'])
        self.assertIn('gcp_runner_boot_disk_name', job['resources'])

    def test_resumed_confirmed_network_rejection_retains_known_identity(self):
        clients, _ = clients_and_timeline()
        job = {'id': 'abc12345', 'resources': {}, 'status': 'provisioning'}
        clients['instances'].insert_errors_by_name[
            'benchmark-abc12345-runner'
        ] = ApiError(408, 'stop after base resources')
        with self.assertRaises(ApiError):
            gcp.provision(
                job, plan(), public_key=PUBLIC_KEY, clients=clients
            )
        network_id = job['resources']['gcp_network_id']
        network_link = job['resources']['gcp_network_self_link']
        clients['networks'].insert_error = ApiError(
            403, 'permission changed'
        )

        with self.assertRaises(ApiError):
            gcp.provision(
                job, plan(), public_key=PUBLIC_KEY, clients=clients
            )

        self.assertEqual(job['resources']['gcp_network_id'], network_id)
        self.assertEqual(
            job['resources']['gcp_network_self_link'], network_link
        )
        self.assertEqual(
            job['resources']['gcp_network_name'], 'benchmark-abc12345'
        )

    def test_resumed_confirmed_runner_rejection_retains_known_identity(self):
        clients, _ = clients_and_timeline()
        job = {'id': 'abc12345', 'resources': {}, 'status': 'provisioning'}
        gcp.provision(
            job, plan(), public_key=PUBLIC_KEY, clients=clients
        )
        runner_id = job['resources']['gcp_instance_id']
        runner_link = job['resources']['gcp_instance_self_link']
        clients['instances'].insert_errors_by_name[
            'benchmark-abc12345-runner'
        ] = ApiError(403, 'permission changed')

        with self.assertRaises(ApiError):
            gcp.provision(
                job, plan(), public_key=PUBLIC_KEY, clients=clients
            )

        self.assertEqual(job['resources']['gcp_instance_id'], runner_id)
        self.assertEqual(
            job['resources']['gcp_instance_self_link'], runner_link
        )
        self.assertIn('gcp_runner_boot_disk_name', job['resources'])


class GcpCleanupTests(unittest.TestCase):
    def provisioned(self):
        clients, timeline = clients_and_timeline()
        job = {'id': 'deadbeef', 'resources': {}, 'status': 'provisioning'}
        gcp.provision(
            job,
            plan(
                benchmarks=['fio', 'iperf3'],
                iperf3={'protocols': ['tcp']},
                storage={'additional_volume': True, 'additional_size_gb': 100},
            ),
            public_key=PUBLIC_KEY,
            clients=clients,
        )
        timeline.clear()
        return job, clients, timeline

    def c4a_provisioned(self):
        machine = {
            'name': 'c4a-standard-4',
            'id': '4004',
            'self_link': (
                'projects/p/zones/us-east1-b/machineTypes/c4a-standard-4'
            ),
            'guest_cpus': 4,
            'memory_mb': 16384,
            'architecture': 'ARM64',
        }
        image = {
            'name': 'rocky-linux-9-arm64-v20260813',
            'id': '9401',
            'self_link': (
                'projects/rocky-linux-cloud/global/images/'
                'rocky-linux-9-arm64-v20260813'
            ),
            'status': 'READY',
            'architecture': 'ARM64',
        }
        clients, timeline = clients_and_timeline(machine=machine, image=image)
        job = {'id': 'c4ac1ean', 'resources': {}, 'status': 'provisioning'}
        gcp.provision(
            job,
            plan(
                shape='c4a-standard-4',
                ocpus=4,
                memory_gb=16,
                benchmarks=['fio', 'iperf3'],
                iperf3={'protocols': ['tcp']},
                storage={
                    'additional_volume': True,
                    'additional_size_gb': 100,
                },
            ),
            public_key=PUBLIC_KEY,
            clients=clients,
        )
        timeline.clear()
        return job, clients, timeline

    def web_provisioned(self):
        clients, timeline = clients_and_timeline()
        job = {'id': 'webclean', 'resources': {}, 'status': 'provisioning'}
        gcp.provision(
            job,
            plan(benchmarks=['apachebench', 'deathstarbench']),
            public_key=PUBLIC_KEY,
            clients=clients,
        )
        timeline.clear()
        return job, clients, timeline

    def test_web_cleanup_deletes_loadgen_and_boot_before_parent_network(self):
        job, clients, timeline = self.web_provisioned()

        gcp.destroy_resources(job, clients=clients)

        instance_deletes = [
            event[2]
            for event in timeline
            if event[:2] == ('delete', 'instance')
        ]
        self.assertEqual(
            instance_deletes,
            [
                'benchmark-webclean-loadgen',
                'benchmark-webclean-runner',
            ],
        )
        deleted_firewalls = [
            event[2]
            for event in timeline
            if event[:2] == ('delete', 'firewall')
        ]
        self.assertIn('benchmark-webclean-web', deleted_firewalls)
        network_delete = next(
            index for index, event in enumerate(timeline)
            if event[:3] == ('delete', 'network', 'benchmark-webclean')
        )
        web_delete = next(
            index for index, event in enumerate(timeline)
            if event[:3] == (
                'delete',
                'firewall',
                'benchmark-webclean-web',
            )
        )
        self.assertLess(web_delete, network_delete)
        self.assertNotIn('gcp_loadgen_instance_name', job['resources'])
        self.assertNotIn('gcp_loadgen_boot_disk_name', job['resources'])
        self.assertNotIn('loadgen_public_ip', job['resources'])
        self.assertEqual(job['status'], 'destroyed')

    def test_cleanup_verifies_and_deletes_children_before_network(self):
        job, clients, timeline = self.provisioned()

        gcp.destroy_resources(job, clients=clients)

        deleted = [item[1] for item in timeline if item[0] == 'delete']
        self.assertEqual(deleted[0:2], ['instance', 'instance'])
        self.assertEqual(deleted[-1], 'network')
        self.assertLess(deleted.index('disk'), deleted.index('subnet'))
        self.assertEqual(job['status'], 'destroyed')
        self.assertNotIn('gcp_resource_prefix', job['resources'])

    def test_partial_delete_retries_with_same_uuid_after_independent_cleanup(self):
        job, clients, timeline = self.provisioned()
        data_name = 'benchmark-deadbeef-data'
        clients['disks'].delete_errors_by_name[data_name] = ApiError(
            403, 'temporary operator policy block'
        )

        with self.assertRaisesRegex(RuntimeError, 'data disk'):
            gcp.destroy_resources(job, clients=clients)

        first_data_delete = next(
            event for event in timeline
            if event[:3] == ('delete', 'disk', data_name)
        )
        first_request_id = first_data_delete[3]['request_id']
        self.assertIn('gcp_data_disk_name', job['resources'])
        self.assertNotIn('gcp_ssh_firewall_name', job['resources'])
        self.assertNotIn('gcp_network_name', job['resources'])
        del clients['disks'].delete_errors_by_name[data_name]

        gcp.destroy_resources(job, clients=clients)

        data_deletes = [
            event for event in timeline
            if event[:3] == ('delete', 'disk', data_name)
        ]
        self.assertEqual(len(data_deletes), 2)
        self.assertEqual(data_deletes[1][3]['request_id'], first_request_id)
        self.assertEqual(job['status'], 'destroyed')

    def test_restart_reconciles_late_boot_after_parent_contract_is_cleared(self):
        clients, timeline = clients_and_timeline()
        job = {'id': 'lateboot', 'resources': {}, 'status': 'provisioning'}
        provisioned_states = []
        gcp.provision(
            job,
            plan(),
            public_key=PUBLIC_KEY,
            clients=clients,
            persist=lambda current: provisioned_states.append(
                copy.deepcopy(current)
            ),
        )
        job = json.loads(json.dumps(provisioned_states[-1]))
        resources = job['resources']
        boot_name = resources['gcp_runner_boot_disk_name']
        parent_request_id = resources['gcp_instance_request_id']
        boot_template = copy.deepcopy(clients['disks'].items.pop(boot_name))
        resources.pop('gcp_runner_boot_disk_id', None)
        resources.pop('gcp_runner_boot_disk_self_link', None)
        resources['gcp_runner_boot_disk_create_ambiguous'] = True
        network_name = resources['gcp_network_name']
        clients['networks'].delete_errors_by_name[network_name] = ApiError(
            403, 'policy denied'
        )
        timeline.clear()
        cleanup_states = []

        with self.assertRaises(RuntimeError):
            gcp.destroy_resources(
                job,
                clients=clients,
                persist=lambda current: cleanup_states.append(
                    copy.deepcopy(current)
                ),
            )

        self.assertNotIn('gcp_instance_name', job['resources'])
        self.assertEqual(
            job['resources']['gcp_runner_boot_disk_request_id'],
            parent_request_id,
        )
        restarted = json.loads(json.dumps(cleanup_states[-1]))
        clients['disks'].items[boot_name] = boot_template
        del clients['networks'].delete_errors_by_name[network_name]
        timeline.clear()

        gcp.destroy_resources(restarted, clients=clients)

        self.assertTrue(any(
            event[:3] == ('delete', 'disk', boot_name)
            for event in timeline
        ))
        self.assertEqual(restarted['status'], 'destroyed')

    def test_completed_delete_error_rotates_request_id_before_retry(self):
        job, clients, _ = self.provisioned()
        service = clients['disks']
        original_delete = service.delete
        requests = []

        def delete(request):
            requests.append(copy.deepcopy(request))
            if len(requests) == 1:
                return Operation(ApiError(
                    400,
                    'localized text with no retry words',
                    reason='RESOURCE_IN_USE_BY_ANOTHER_RESOURCE',
                ))
            return original_delete(request)

        service.delete = delete
        with patch.object(gcp.time, 'sleep'):
            gcp._delete_resource(
                job,
                None,
                'gcp_data_disk',
                clients,
            )

        self.assertEqual(len(requests), 2)
        self.assertNotEqual(
            requests[0]['request_id'],
            requests[1]['request_id'],
        )

    def test_transport_ambiguous_delete_replays_same_request_id(self):
        job, clients, _ = self.provisioned()
        service = clients['disks']
        original_delete = service.delete
        requests = []

        def delete(request):
            requests.append(copy.deepcopy(request))
            if len(requests) == 1:
                raise ApiError(503, 'nicht verfuegbar')
            return original_delete(request)

        service.delete = delete
        with patch.object(gcp.time, 'sleep'):
            gcp._delete_resource(
                job,
                None,
                'gcp_data_disk',
                clients,
            )

        self.assertEqual(len(requests), 2)
        self.assertEqual(
            requests[0]['request_id'],
            requests[1]['request_id'],
        )

    def test_semantic_delete_retry_rejects_same_name_replacement(self):
        job, clients, _ = self.provisioned()
        service = clients['disks']
        disk_name = job['resources']['gcp_data_disk_name']
        requests = []

        def replace_disk():
            service.items[disk_name]['id'] = 'foreign-replacement'

        def delete(request):
            requests.append(copy.deepcopy(request))
            return Operation(
                ApiError(
                    400,
                    'ressource occupee',
                    reason='RESOURCE_IN_USE_BY_ANOTHER_RESOURCE',
                ),
                on_result=replace_disk,
            )

        service.delete = delete
        with (
            patch.object(gcp.time, 'sleep'),
            self.assertRaisesRegex(RuntimeError, 'numeric ID'),
        ):
            gcp._delete_resource(
                job,
                None,
                'gcp_data_disk',
                clients,
            )

        self.assertEqual(len(requests), 1)
        self.assertIn(disk_name, service.items)
        self.assertNotIn(
            'gcp_data_disk_delete_request_id',
            job['resources'],
        )

    def test_delete_retry_classification_uses_codes_not_messages(self):
        self.assertTrue(gcp._delete_retryable(ApiError(
            503, 'vollstaendig lokalisierte fehlermeldung'
        )))
        self.assertTrue(gcp._delete_retryable(ApiError(
            400,
            'aucun indice textuel',
            reason='RESOURCE_NOT_READY',
        )))
        self.assertFalse(gcp._delete_retryable(ApiError(
            403,
            'temporary backend quota rate limit in use',
        )))

    def test_cleanup_rejects_recreated_numeric_id_before_delete(self):
        job, clients, timeline = self.provisioned()
        clients['networks'].items['benchmark-deadbeef']['id'] = 'different'

        with self.assertRaisesRegex(RuntimeError, 'numeric ID'):
            gcp.destroy_resources(job, clients=clients)

        self.assertFalse(any(item[0] == 'delete' for item in timeline))

    def test_cleanup_rejects_unexpected_instance_disk_before_delete(self):
        job, clients, timeline = self.provisioned()
        runner = clients['instances'].items['benchmark-deadbeef-runner']
        runner['disks'].append({
            'device_name': 'foreign',
            'source': 'projects/p/zones/us-east1-b/disks/foreign',
            'boot': False,
            'auto_delete': True,
        })

        with self.assertRaisesRegex(RuntimeError, 'unexpected attached disk set'):
            gcp.destroy_resources(job, clients=clients)

        self.assertFalse(any(item[0] == 'delete' for item in timeline))

    def test_cleanup_rejects_foreign_project_same_name_boot_before_delete(self):
        job, clients, timeline = self.provisioned()
        runner = clients['instances'].items['benchmark-deadbeef-runner']
        boot_name = 'benchmark-deadbeef-runner-boot'
        boot = next(disk for disk in runner['disks'] if disk['boot'])
        boot['source'] = (
            f'projects/foreign/zones/us-east1-b/disks/{boot_name}'
        )
        restarted = json.loads(json.dumps(job))

        with self.assertRaisesRegex(RuntimeError, 'unsafe attachment'):
            gcp.destroy_resources(restarted, clients=clients)

        self.assertFalse(any(item[0] == 'delete' for item in timeline))

    def test_cleanup_rejects_foreign_project_boot_resource_self_link(self):
        job, clients, timeline = self.provisioned()
        boot_name = 'benchmark-deadbeef-runner-boot'
        boot = clients['disks'].items[boot_name]
        boot['self_link'] = (
            f'projects/foreign/zones/us-east1-b/disks/{boot_name}'
        )
        job['resources'].pop('gcp_runner_boot_disk_id', None)
        job['resources'].pop('gcp_runner_boot_disk_self_link', None)
        job['resources']['gcp_runner_boot_disk_create_ambiguous'] = True
        restarted = json.loads(json.dumps(job))

        with self.assertRaisesRegex(RuntimeError, 'different project or zone'):
            gcp.destroy_resources(restarted, clients=clients)

        self.assertFalse(any(item[0] == 'delete' for item in timeline))

    def test_cleanup_rejects_foreign_project_boot_disk_user(self):
        job, clients, timeline = self.provisioned()
        boot_name = 'benchmark-deadbeef-runner-boot'
        clients['disks'].items[boot_name]['users'] = [
            'projects/foreign/zones/us-east1-b/instances/'
            'benchmark-deadbeef-runner'
        ]
        restarted = json.loads(json.dumps(job))

        with self.assertRaisesRegex(RuntimeError, 'unexpected instance'):
            gcp.destroy_resources(restarted, clients=clients)

        self.assertFalse(any(item[0] == 'delete' for item in timeline))

    def test_c4a_cleanup_rejects_hyperdisk_performance_drift(self):
        job, clients, timeline = self.c4a_provisioned()
        data = clients['disks'].items['benchmark-c4ac1ean-data']
        data['provisioned_iops'] = 4000

        with self.assertRaisesRegex(RuntimeError, 'provisioned IOPS changed'):
            gcp.destroy_resources(job, clients=clients)

        self.assertFalse(any(item[0] == 'delete' for item in timeline))

    def test_c4a_cleanup_rejects_nic_and_disk_interface_drift(self):
        for field, value, message in (
            ('nic', 'VIRTIO_NET', 'network interface type'),
            ('disk', 'SCSI', 'disk interface'),
        ):
            with self.subTest(field=field):
                job, clients, timeline = self.c4a_provisioned()
                runner = clients['instances'].items[
                    'benchmark-c4ac1ean-runner'
                ]
                if field == 'nic':
                    runner['network_interfaces'][0]['nic_type'] = value
                else:
                    runner['disks'][0]['interface'] = value

                with self.assertRaisesRegex(RuntimeError, message):
                    gcp.destroy_resources(job, clients=clients)

                self.assertFalse(any(item[0] == 'delete' for item in timeline))

    def test_response_lost_c4a_data_disk_replays_exact_hyperdisk_profile(self):
        clients, timeline = clients_and_timeline()
        request_id = '11111111-1111-4111-8111-111111111111'
        job = {
            'id': 'facefeed',
            'status': 'failed',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': 'benchmark-facefeed',
                'gcp_zone': 'us-east1-b',
                'gcp_data_disk_name': 'benchmark-facefeed-data',
                'gcp_data_disk_request_id': request_id,
                'gcp_data_disk_create_ambiguous': True,
                'gcp_data_disk_type': 'hyperdisk-balanced',
                'gcp_data_disk_size_gb': 100,
                'gcp_data_device_name': 'benchmark-facefeed-data',
                'gcp_data_disk_provisioned_iops': 3000,
                'gcp_data_disk_provisioned_throughput_mibps': 140,
            },
        }

        gcp.destroy_resources(job, clients=clients)

        insert = next(
            event for event in timeline if event[:2] == ('insert', 'disk')
        )
        self.assertEqual(insert[2]['request_id'], request_id)
        body = insert[2]['disk_resource']
        self.assertEqual(
            body['type_'].rsplit('/', 1)[-1], 'hyperdisk-balanced'
        )
        self.assertEqual(body['provisioned_iops'], 3000)
        self.assertEqual(body['provisioned_throughput'], 140)
        self.assertEqual(job['status'], 'destroyed')

    def test_cleanup_rejects_cross_project_same_name_parent(self):
        job, clients, timeline = self.provisioned()
        subnet = clients['subnetworks'].items['benchmark-deadbeef']
        subnet['network'] = (
            'projects/foreign/global/networks/benchmark-deadbeef'
        )

        with self.assertRaisesRegex(RuntimeError, 'different VPC'):
            gcp.destroy_resources(job, clients=clients)

        self.assertFalse(any(item[0] == 'delete' for item in timeline))

    def test_preflight_404_clears_contract_without_name_only_delete(self):
        clients, timeline = clients_and_timeline()
        job = {
            'id': 'facefeed',
            'status': 'failed',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': 'benchmark-facefeed',
                'gcp_network_name': 'benchmark-facefeed',
                'gcp_network_id': '123',
                'gcp_network_self_link': (
                    'projects/p/global/networks/benchmark-facefeed'
                ),
                'gcp_network_create_ambiguous': False,
            },
        }

        gcp.destroy_resources(job, clients=clients)

        self.assertFalse(any(item[0] == 'delete' for item in timeline))
        self.assertEqual(job['status'], 'destroyed')

    def test_replacement_between_preflight_and_delete_is_rejected(self):
        clients, timeline = clients_and_timeline()
        network = gcp._network_resource(
            clients, 'facefeed', 'benchmark-facefeed'
        )
        network.update({
            'id': '123',
            'self_link': 'projects/p/global/networks/benchmark-facefeed',
        })
        service = clients['networks']
        service.items['benchmark-facefeed'] = network
        original_get = service.get
        calls = 0

        def replacing_get(request):
            nonlocal calls
            calls += 1
            result = original_get(request)
            if calls == 2:
                result['id'] = 'replacement'
            return result

        service.get = replacing_get
        job = {
            'id': 'facefeed',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': 'benchmark-facefeed',
                'gcp_network_name': 'benchmark-facefeed',
                'gcp_network_id': '123',
                'gcp_network_self_link': network['self_link'],
                'gcp_network_create_ambiguous': False,
            },
        }

        with self.assertRaisesRegex(RuntimeError, 'numeric ID'):
            gcp.destroy_resources(job, clients=clients)

        self.assertFalse(any(item[0] == 'delete' for item in timeline))

    def test_response_lost_network_is_replayed_then_deleted(self):
        clients, timeline = clients_and_timeline()
        request_id = '11111111-1111-4111-8111-111111111111'
        job = {
            'id': 'facefeed',
            'status': 'failed',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': 'benchmark-facefeed',
                'gcp_network_name': 'benchmark-facefeed',
                'gcp_network_request_id': request_id,
                'gcp_network_create_ambiguous': True,
            },
        }
        snapshots = []

        gcp.destroy_resources(
            job,
            clients=clients,
            persist=lambda current: snapshots.append(
                copy.deepcopy(current['resources'])
            ),
        )

        inserts = [item for item in timeline if item[:2] == ('insert', 'network')]
        self.assertEqual(len(inserts), 1)
        self.assertEqual(inserts[0][2]['request_id'], request_id)
        self.assertTrue(any(
            state.get('gcp_network_id')
            and state.get('gcp_network_create_ambiguous') is False
            for state in snapshots
        ))
        self.assertEqual(job['status'], 'destroyed')

    def test_cleanup_replay_clears_completed_failed_insert_contract(self):
        clients, timeline = clients_and_timeline()
        original_request_id = '11111111-1111-4111-8111-111111111111'
        requests = []

        def failed_insert(request):
            requests.append(copy.deepcopy(request))
            return Operation(ApiError(
                400,
                'kein englischer fehlertext',
                reason='RESOURCE_NOT_READY',
            ))

        clients['networks'].insert = failed_insert
        job = {
            'id': 'facefeed',
            'status': 'failed',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': 'benchmark-facefeed',
                'gcp_network_name': 'benchmark-facefeed',
                'gcp_network_request_id': original_request_id,
                'gcp_network_create_ambiguous': True,
            },
        }

        with patch.object(gcp.time, 'sleep'):
            gcp.destroy_resources(job, clients=clients)

        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]['request_id'], original_request_id)
        self.assertNotIn('gcp_network_request_id', job['resources'])
        self.assertNotIn('gcp_network_name', job['resources'])
        self.assertEqual(job['status'], 'destroyed')

    def test_cleanup_requires_compute_project_id_before_cloud_delete(self):
        clients, timeline = clients_and_timeline()
        job = {
            'id': 'facefeed',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_resource_prefix': 'benchmark-facefeed',
            },
        }

        with self.assertRaisesRegex(RuntimeError, 'Compute project ID is missing'):
            gcp.destroy_resources(job, clients=clients)

        self.assertFalse(any(item[0] == 'delete' for item in timeline))

    def test_ambiguous_contract_requires_persisted_request_id(self):
        clients, timeline = clients_and_timeline()
        job = {
            'id': 'facefeed',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': 'benchmark-facefeed',
                'gcp_network_name': 'benchmark-facefeed',
                'gcp_network_create_ambiguous': True,
            },
        }

        with self.assertRaisesRegex(RuntimeError, 'request ID'):
            gcp.destroy_resources(job, clients=clients)

        self.assertFalse(any(item[0] == 'delete' for item in timeline))

    def test_replay_rejection_retains_original_ambiguity_contract(self):
        clients, timeline = clients_and_timeline()
        clients['networks'].insert_error = ApiError(403, 'permission changed')
        request_id = '11111111-1111-4111-8111-111111111111'
        job = {
            'id': 'facefeed',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': 'benchmark-facefeed',
                'gcp_network_name': 'benchmark-facefeed',
                'gcp_network_request_id': request_id,
                'gcp_network_create_ambiguous': True,
            },
        }

        with self.assertRaisesRegex(RuntimeError, 'Unable to reconcile'):
            gcp.destroy_resources(job, clients=clients)

        self.assertEqual(
            job['resources']['gcp_network_name'], 'benchmark-facefeed'
        )
        self.assertEqual(job['resources']['gcp_network_request_id'], request_id)
        self.assertTrue(job['resources']['gcp_network_create_ambiguous'])

    def test_precreate_data_metadata_without_name_is_cleared(self):
        clients, timeline = clients_and_timeline()
        job = {
            'id': 'facefeed',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': 'benchmark-facefeed',
                'gcp_data_disk_type': 'pd-balanced',
                'gcp_data_disk_size_gb': 100,
                'gcp_data_device_name': 'benchmark-facefeed-data',
            },
        }

        gcp.destroy_resources(job, clients=clients)

        self.assertEqual(job['status'], 'destroyed')
        self.assertNotIn('gcp_data_disk_type', job['resources'])

    def test_restart_after_network_absence_proof_finishes_boot_contract(self):
        clients, timeline = clients_and_timeline()
        job = {
            'id': 'facefeed',
            'status': 'destroying',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': 'benchmark-facefeed',
                'gcp_zone': 'us-east1-b',
                'gcp_network_absence_confirmed': True,
                'gcp_runner_boot_disk_name': (
                    'benchmark-facefeed-runner-boot'
                ),
                'gcp_runner_boot_disk_create_ambiguous': True,
            },
        }

        with patch.object(gcp.time, 'sleep'):
            gcp.destroy_resources(job, clients=clients)

        self.assertEqual(job['status'], 'destroyed')
        self.assertNotIn('gcp_runner_boot_disk_name', job['resources'])
        self.assertNotIn('gcp_network_absence_confirmed', job['resources'])

    def test_ambiguous_boot_visibility_is_polled_before_cleanup(self):
        clients, timeline = clients_and_timeline()
        name = 'benchmark-facefeed-runner-boot'
        clients['disks'].items[name] = {
            'name': name,
            'id': '8080',
            'self_link': f'projects/p/zones/us-east1-b/disks/{name}',
            'description': gcp._description('facefeed', 'runner-boot'),
            'size_gb': 100,
            'type_': 'projects/p/zones/us-east1-b/diskTypes/pd-balanced',
            'labels': gcp._labels('facefeed', 'runner-boot'),
            'users': [],
        }
        clients['disks'].get_misses_by_name[name] = 2
        job = {
            'id': 'facefeed',
            'status': 'destroying',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': 'benchmark-facefeed',
                'gcp_zone': 'us-east1-b',
                'gcp_network_absence_confirmed': True,
                'gcp_runner_boot_disk_name': name,
                'gcp_runner_boot_disk_create_ambiguous': True,
            },
        }

        with patch.object(gcp.time, 'sleep') as sleep:
            gcp.destroy_resources(job, clients=clients)

        self.assertGreaterEqual(sleep.call_count, 1)
        self.assertTrue(any(
            event[:3] == ('delete', 'disk', name) for event in timeline
        ))
        self.assertEqual(job['status'], 'destroyed')

    def test_post_network_sweep_polls_for_late_boot_disk(self):
        clients, timeline = clients_and_timeline()
        network_name = 'benchmark-facefeed'
        network = gcp._network_resource(
            clients, 'facefeed', network_name
        )
        network.update({
            'id': '123',
            'self_link': f'projects/p/global/networks/{network_name}',
        })
        clients['networks'].items[network_name] = network
        boot_name = 'benchmark-facefeed-runner-boot'

        def materialize_boot(_):
            clients['disks'].items[boot_name] = {
                'name': boot_name,
                'id': '8080',
                'self_link': (
                    f'projects/p/zones/us-east1-b/disks/{boot_name}'
                ),
                'description': gcp._description(
                    'facefeed', 'runner-boot'
                ),
                'size_gb': 100,
                'type_': (
                    'projects/p/zones/us-east1-b/diskTypes/pd-balanced'
                ),
                'labels': gcp._labels('facefeed', 'runner-boot'),
                'users': [],
            }
            clients['disks'].get_misses_by_name[boot_name] = 2

        clients['networks'].on_delete = materialize_boot
        request_id = '11111111-1111-4111-8111-111111111111'
        job = {
            'id': 'facefeed',
            'status': 'destroying',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': network_name,
                'gcp_zone': 'us-east1-b',
                'gcp_network_name': network_name,
                'gcp_network_id': '123',
                'gcp_network_self_link': network['self_link'],
                'gcp_network_create_ambiguous': False,
                'gcp_instance_name': 'benchmark-facefeed-runner',
                'gcp_instance_request_id': request_id,
                'gcp_instance_create_ambiguous': True,
                'gcp_runner_boot_disk_name': boot_name,
                'gcp_runner_boot_disk_create_ambiguous': True,
            },
        }

        with patch.object(gcp.time, 'sleep') as sleep:
            gcp.destroy_resources(job, clients=clients)

        self.assertGreaterEqual(sleep.call_count, 1)
        self.assertTrue(any(
            event[:3] == ('delete', 'disk', boot_name)
            for event in timeline
        ))
        self.assertEqual(job['status'], 'destroyed')

    def test_post_network_boot_replacement_is_not_deleted(self):
        clients, timeline = clients_and_timeline()
        network_name = 'benchmark-facefeed'
        network = gcp._network_resource(
            clients, 'facefeed', network_name
        )
        network.update({
            'id': '123',
            'self_link': f'projects/p/global/networks/{network_name}',
        })
        clients['networks'].items[network_name] = network
        boot_name = 'benchmark-facefeed-runner-boot'

        def materialize_then_replace(_):
            disk_service = clients['disks']
            disk_service.items[boot_name] = {
                'name': boot_name,
                'id': '8080',
                'self_link': (
                    f'projects/p/zones/us-east1-b/disks/{boot_name}'
                ),
                'description': gcp._description(
                    'facefeed', 'runner-boot'
                ),
                'size_gb': 100,
                'type_': (
                    'projects/p/zones/us-east1-b/diskTypes/pd-balanced'
                ),
                'labels': gcp._labels('facefeed', 'runner-boot'),
                'users': [],
            }
            original_get = disk_service.get
            calls = 0

            def replacing_get(request):
                nonlocal calls
                calls += 1
                result = original_get(request)
                if calls == 2:
                    result['id'] = 'replacement'
                return result

            disk_service.get = replacing_get

        clients['networks'].on_delete = materialize_then_replace
        request_id = '11111111-1111-4111-8111-111111111111'
        job = {
            'id': 'facefeed',
            'status': 'destroying',
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'p',
                'gcp_compute_project_id': '3977225790263303761',
                'gcp_resource_prefix': network_name,
                'gcp_zone': 'us-east1-b',
                'gcp_network_name': network_name,
                'gcp_network_id': '123',
                'gcp_network_self_link': network['self_link'],
                'gcp_network_create_ambiguous': False,
                'gcp_instance_name': 'benchmark-facefeed-runner',
                'gcp_instance_request_id': request_id,
                'gcp_instance_create_ambiguous': True,
                'gcp_runner_boot_disk_name': boot_name,
                'gcp_runner_boot_disk_create_ambiguous': True,
            },
        }

        with (
            patch.object(gcp.time, 'sleep'),
            self.assertRaisesRegex(RuntimeError, 'numeric ID'),
        ):
            gcp.destroy_resources(job, clients=clients)

        self.assertFalse(any(
            event[:3] == ('delete', 'disk', boot_name)
            for event in timeline
        ))
        self.assertIn('gcp_runner_boot_disk_name', job['resources'])


if __name__ == '__main__':
    unittest.main()
