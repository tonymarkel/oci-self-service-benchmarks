import copy
from dataclasses import replace
import hashlib
import json
import re
import subprocess
from types import SimpleNamespace
import unittest
from unittest import mock

from app.deathstarbench_contract import (
    DISTRIBUTED_IMAGE_SET_REVISION,
    DISTRIBUTED_RUNTIME_REVISION,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    DISTRIBUTED_WORKLOAD_REVISION,
    K3S_RUNTIME_ID,
)
from app.deathstarbench_distributed import (
    DISTRIBUTED_NETWORK_QUALIFICATION_KEY,
    NETWORK_POLICY_POSITIVE_ATTEMPT_LIMIT,
    NETWORK_POLICY_POSITIVE_RETRY_INTERVAL_SECONDS,
    NETWORK_POLICY_PROBE_DEADLINE_SECONDS,
    NETWORK_POLICY_PROBE_POD,
    NETWORK_POLICY_PROBE_SERVICE,
    NETWORK_POLICY_PROBE_SETTLE_SECONDS,
    NETWORK_POLICY_POSITIVE_CONTROL_POD,
    NETWORK_QUALIFICATION_SCHEMA_VERSION,
    NETWORK_TCP_PROBE_TIMEOUT_SECONDS,
    RUNTIME_JOURNAL_KEY,
    WORKLOAD_JOURNAL_KEY,
    AzureK3sCandidatePlan,
    DistributedRuntimeError,
    _prepare_distributed_k3s_candidate,
    _network_policy_positive_control_command,
    _network_policy_probe_command,
    _network_policy_probe_cleanup_command,
    _network_policy_probe_manifest,
    azure_k3s_candidate_plan,
    aws_k3s_candidate_plan,
    distributed_k3s_candidate_plan,
    gcp_k3s_candidate_plan,
    parse_database_volume_attestation,
    parse_aws_database_volume_attestation,
    parse_gcp_database_volume_attestation,
    parse_oci_database_volume_attestation,
    parse_database_workload_storage_attestation,
    prepare_azure_distributed_k3s_candidate,
    prepare_azure_distributed_social_network_candidate,
    prepare_gcp_distributed_k3s_candidate,
    prepare_gcp_distributed_social_network_candidate,
    oci_k3s_candidate_plan,
    qualify_distributed_network_paths,
)
from app.deathstarbench_k3s_workload import (
    IMAGE_LOCK_SCHEMA_VERSION,
    REQUIRED_IMAGE_KEYS,
    UPSTREAM_REVISION,
    WorkloadBundleError,
    render_social_network_bundle,
)
from app.deathstarbench_topology import build_topology_manifest
from app.k3s_runtime import K3S_AGENT_TOKEN_FILE
from app.main import RunCancelled, run_deathstarbench
from app.remote_execution import SSHCommandError
from app.resource_inventory import persist_role_node_inventory


PRIVATE_ADDRESSES = {
    'control': '10.240.1.10',
    'database': '10.240.1.11',
    'cache': '10.240.1.12',
    'application': '10.240.1.13',
    'load-generator': '10.240.2.10',
}
SHAPES = {
    'control': 'Standard_D2as_v7',
    'database': 'Standard_D4as_v7',
    'cache': 'Standard_D2as_v7',
    'application': 'Standard_D8ps_v6',
    'load-generator': 'Standard_D2as_v7',
}
ARCHITECTURES = {
    'control': 'x86_64',
    'database': 'x86_64',
    'cache': 'x86_64',
    'application': 'arm64',
    'load-generator': 'x86_64',
}


def candidate_image_lock():
    return {
        'schema_version': IMAGE_LOCK_SCHEMA_VERSION,
        'workload_revision': DISTRIBUTED_WORKLOAD_REVISION,
        'image_set_revision': DISTRIBUTED_IMAGE_SET_REVISION,
        'upstream_revision': UPSTREAM_REVISION,
        'released': False,
        'platforms': {
            platform: {
                'images': {
                    key: (
                        f'registry.example/deathstarbench/{key}@sha256:'
                        + hashlib.sha256(
                            f'{platform}:{key}'.encode()
                        ).hexdigest()
                    )
                    for key in REQUIRED_IMAGE_KEYS
                },
            }
            for platform in ('linux/amd64', 'linux/arm64')
        },
    }


def candidate_job():
    manifest = build_topology_manifest(
        DISTRIBUTED_TIERED_TOPOLOGY_ID,
        K3S_RUNTIME_ID,
        selected_shape=SHAPES['application'],
        selected_architecture='arm64',
    )
    group_root = '/subscriptions/sub/resourceGroups/benchmark-runtime'
    inventory = manifest.planned_inventory('azure')
    updated_nodes = []
    resources = {
        'provider': 'azure',
        'azure_distributed_candidate': True,
        'ssh_user': 'benchmark',
        'azure_database_disk_type': 'PremiumV2_LRS',
        'azure_database_disk_size_gb': 100.0,
        'azure_database_disk_iops': 3000,
        'azure_database_disk_throughput_mibps': 125.0,
        'azure_database_disk_device': '/dev/disk/azure/scsi1/lun0',
        'deathstarbench_topology_manifest': manifest.as_dict(),
        'deathstarbench_topology_fingerprint': manifest.fingerprint,
    }
    for node in inventory.nodes:
        key = node.key
        normalized = key.replace('-', '_')
        prefix = f'azure_dsb_{normalized}_instance'
        name = f'dsb-{key.replace("load-generator", "loadgen")}'
        node_id = f'{group_root}/providers/Microsoft.Compute/virtualMachines/{name}'
        public = (
            f'203.0.113.{10 + len(updated_nodes)}'
            if key in {'control', 'application', 'load-generator'}
            else None
        )
        storage_items = []
        for storage in node.storage:
            disk_name = 'dsb-database-data'
            disk_id = (
                f'{group_root}/providers/Microsoft.Compute/disks/{disk_name}'
            )
            storage_items.append(replace(
                storage,
                provider_resource_id=disk_id,
                provider_resource_name=disk_name,
                device='/dev/disk/azure/scsi1/lun0',
                lifecycle_status='running',
            ))
            resources.update({
                'azure_dsb_database_data_disk_id': disk_id,
                'azure_dsb_database_data_disk_expected_id': disk_id,
                'azure_dsb_database_data_disk_name': disk_name,
            })
        updated_nodes.append(replace(
            node,
            provider_resource_id=node_id,
            provider_resource_name=name,
            public_addresses=(public,) if public else (),
            private_addresses=(PRIVATE_ADDRESSES[key],),
            shape=SHAPES[key],
            architecture=ARCHITECTURES[key],
            storage=tuple(storage_items),
            lifecycle_status='running',
        ))
        resources.update({
            f'{prefix}_name': name,
            f'{prefix}_id': node_id,
            f'{prefix}_expected_id': node_id,
            f'azure_dsb_{normalized}_private_ip': PRIVATE_ADDRESSES[key],
            f'azure_dsb_{normalized}_public_ip': public,
        })
    inventory = replace(inventory, nodes=tuple(updated_nodes))
    persist_role_node_inventory(resources, inventory)
    return {'id': 'abc123def456', 'resources': resources, 'results': []}


def gcp_candidate_job():
    shapes = {
        'control': 'n2-standard-2',
        'database': 'n2-standard-4',
        'cache': 'n2-standard-2',
        'application': 'c4a-standard-8',
        'load-generator': 'n2-standard-2',
    }
    manifest = build_topology_manifest(
        DISTRIBUTED_TIERED_TOPOLOGY_ID,
        K3S_RUNTIME_ID,
        selected_shape=shapes['application'],
        selected_architecture='arm64',
    )
    project_id = 'benchmark-project'
    zone = 'us-central1-a'
    inventory = manifest.planned_inventory('gcp')
    updated_nodes = []
    resources = {
        'provider': 'gcp',
        'gcp_distributed_candidate': True,
        'gcp_project_id': project_id,
        'gcp_zone': zone,
        'availability_zone': zone,
        'ssh_user': 'benchmark',
        'deathstarbench_topology_manifest': manifest.as_dict(),
        'deathstarbench_topology_fingerprint': manifest.fingerprint,
        'gcp_dsb_application_image_id': '3001',
        'gcp_dsb_application_image_name': 'rocky-linux-9-arm64-test',
        'gcp_dsb_application_image_self_link': (
            'https://www.googleapis.com/compute/v1/projects/'
            'rocky-linux-cloud/global/images/rocky-linux-9-arm64-test'
        ),
        'gcp_dsb_application_image_family': 'rocky-linux-9-arm64',
        'gcp_dsb_application_image_architecture': 'arm64',
        'gcp_dsb_support_image_id': '3002',
        'gcp_dsb_support_image_name': 'rocky-linux-9-test',
        'gcp_dsb_support_image_self_link': (
            'https://www.googleapis.com/compute/v1/projects/'
            'rocky-linux-cloud/global/images/rocky-linux-9-test'
        ),
        'gcp_dsb_support_image_family': 'rocky-linux-9',
        'gcp_dsb_support_image_architecture': 'x86_64',
    }
    for index, node in enumerate(inventory.nodes, start=1):
        key = node.key
        normalized = key.replace('-', '_')
        prefix = f'gcp_dsb_{normalized}_instance'
        name = f'benchmark-{key.replace("load-generator", "loadgen")}'
        resource_id = str(1000 + index)
        public = (
            f'203.0.113.{20 + index}'
            if key in {'control', 'application', 'load-generator'}
            else None
        )
        storage_items = []
        for storage in node.storage:
            disk_name = 'benchmark-database-data'
            disk_id = '2001'
            device = f'/dev/disk/by-id/google-{disk_name}'
            storage_items.append(replace(
                storage,
                provider_resource_id=disk_id,
                provider_resource_name=disk_name,
                device=device,
                lifecycle_status='running',
            ))
            resources.update({
                'gcp_dsb_database_data_disk_name': disk_name,
                'gcp_dsb_database_data_disk_id': disk_id,
                'gcp_dsb_database_data_disk_self_link': (
                    f'https://www.googleapis.com/compute/v1/projects/'
                    f'{project_id}/zones/{zone}/disks/{disk_name}'
                ),
                'gcp_dsb_database_disk_device': device,
                'gcp_dsb_database_data_disk_type': 'pd-ssd',
                'gcp_dsb_database_data_disk_size_gb': 100,
                'gcp_dsb_database_data_disk_provisioned_iops': None,
                'gcp_dsb_database_data_disk_provisioned_throughput_mibps': None,
            })
        architecture = 'arm64' if key == 'application' else 'x86_64'
        updated_nodes.append(replace(
            node,
            provider_resource_id=resource_id,
            provider_resource_name=name,
            public_addresses=(public,) if public else (),
            private_addresses=(PRIVATE_ADDRESSES[key],),
            zone=zone,
            shape=shapes[key],
            architecture=architecture,
            storage=tuple(storage_items),
            lifecycle_status='running',
        ))
        resources.update({
            f'{prefix}_name': name,
            f'{prefix}_id': resource_id,
            f'{prefix}_self_link': (
                f'https://www.googleapis.com/compute/v1/projects/{project_id}'
                f'/zones/{zone}/instances/{name}'
            ),
            f'gcp_dsb_{normalized}_public_ip': public,
            f'gcp_dsb_{normalized}_private_ip': PRIVATE_ADDRESSES[key],
            f'gcp_dsb_{normalized}_machine_type': shapes[key],
            f'gcp_dsb_{normalized}_architecture': architecture,
        })
    inventory = replace(inventory, nodes=tuple(updated_nodes))
    persist_role_node_inventory(resources, inventory)
    return {'id': 'abc123def456', 'resources': resources, 'results': []}


class FakeRemoteExecutor:
    def __init__(self, fail_at=None, *, ca_sha256=None, filesystem_uuid=None):
        self.calls = []
        self.fail_at = fail_at
        self.ca_sha256 = ca_sha256 or ('a' * 64)
        self.filesystem_uuid = (
            filesystem_uuid
            or '11111111-2222-3333-4444-555555555555'
        )
        self.secure_token = f'K10{self.ca_sha256}::agent-password'

    def __call__(self, job, command, **kwargs):
        self.calls.append((command, kwargs))
        if self.fail_at is not None and len(self.calls) == self.fail_at:
            raise RuntimeError('injected remote failure')
        if '/var/lib/rancher/k3s/server/agent-token' in command:
            return self.secure_token + '\n'
        if '/var/lib/rancher/k3s/server/tls/server-ca.crt' in command:
            return self.ca_sha256 + '\n'
        if 'AZURE_DSB_WORKLOAD_STORAGE' in command:
            return (
                'AZURE_DSB_WORKLOAD_STORAGE '
                f'uuid={self.filesystem_uuid} '
                'root=/var/lib/deathstarbench/database/mongodb databases=6\n'
            )
        if 'AZURE_DSB_DATABASE_VOLUME' in command:
            return (
                'AZURE_DSB_DATABASE_VOLUME lun=0 '
                'device_link=/dev/disk/azure/data/by-lun/0 '
                f'uuid={self.filesystem_uuid} '
                'mount_point=/var/lib/deathstarbench/database '
                'filesystem=xfs\n'
            )
        if 'deployments.apps,services,persistentvolumes' in command:
            return '{"apiVersion":"v1","items":[],"kind":"List"}'
        return ''


class GcpFakeRemoteExecutor(FakeRemoteExecutor):
    def __call__(self, job, command, **kwargs):
        if 'GCP_DSB_WORKLOAD_STORAGE' in command:
            self.calls.append((command, kwargs))
            return (
                'GCP_DSB_WORKLOAD_STORAGE '
                f'uuid={self.filesystem_uuid} '
                'root=/var/lib/deathstarbench/database/mongodb databases=6\n'
            )
        if 'GCP_DSB_DATABASE_VOLUME' in command:
            self.calls.append((command, kwargs))
            return (
                'GCP_DSB_DATABASE_VOLUME '
                'device_name=benchmark-database-data '
                'device_link=/dev/disk/by-id/google-benchmark-database-data '
                f'uuid={self.filesystem_uuid} '
                'mount_point=/var/lib/deathstarbench/database '
                'filesystem=xfs\n'
            )
        return super().__call__(job, command, **kwargs)


class NetworkQualificationExecutor:
    """Model the deterministic probe lifecycle and return exact markers."""

    MARKER_RE = re.compile(
        r"'(DISTRIBUTED_DSB_(?:NETWORK_PATH|POSITIVE_CONTROL|"
        r"DEFAULT_DENY(?:_CLEANUP)?) [^']+)'"
    )

    def __init__(
        self,
        *,
        fail_on=None,
        duplicate_on=None,
        fail_final_cleanup=False,
        initial_probe=None,
        initial_positive_probe=None,
        negative_mode='drop',
        fail_positive_attempt=None,
    ):
        self.calls = []
        self.fail_on = fail_on
        self.duplicate_on = duplicate_on
        self.fail_final_cleanup = fail_final_cleanup
        self.cleanup_count = 0
        self.delete_count = 0
        self.probe_state = initial_probe
        self.positive_probe_state = initial_positive_probe
        self.negative_mode = negative_mode
        self.fail_positive_attempt = fail_positive_attempt
        self.positive_attempt_count = 0
        self.negative_attempt_count = 0

    def __call__(self, job, command, **kwargs):
        self.calls.append((command, kwargs))
        markers = self.MARKER_RE.findall(command)
        if not markers:
            raise AssertionError('network qualification sent an unknown command')
        marker = markers[0]
        if (
            'DISTRIBUTED_DSB_DEFAULT_DENY ' in marker
            and 'CLEANUP' not in marker
        ):
            self.negative_attempt_count += 1
            if self.negative_mode == 'reject':
                marker = next(
                    value for value in markers if 'block_mode=reject' in value
                )
            elif self.negative_mode == 'success':
                self.probe_state = 'exact'
                raise RuntimeError('The default-deny probe reached Redis.')
            elif self.negative_mode == 'invalid_exit':
                self.probe_state = 'exact'
                raise RuntimeError(
                    'The default-deny probe exit status is invalid.'
                )
            elif self.negative_mode == 'invalid_reason':
                self.probe_state = 'exact'
                raise RuntimeError(
                    'The default-deny probe termination reason is invalid.'
                )
            elif self.negative_mode != 'drop':
                raise AssertionError('unsupported negative probe mode')
        if 'DISTRIBUTED_DSB_DEFAULT_DENY_CLEANUP ' in marker:
            self.cleanup_count += 1
            if self.fail_final_cleanup and self.cleanup_count == 4:
                raise RuntimeError('injected final cleanup failure')
            positive = f'pod={NETWORK_POLICY_POSITIVE_CONTROL_POD} ' in marker
            state_name = 'positive_probe_state' if positive else 'probe_state'
            state = getattr(self, state_name)
            if state == 'foreign':
                raise RuntimeError(
                    'Refusing to delete a foreign network qualification probe Pod.'
                )
            if state == 'exact':
                self.delete_count += 1
                setattr(self, state_name, None)
        elif 'DISTRIBUTED_DSB_POSITIVE_CONTROL ' in marker:
            self.positive_attempt_count += 1
            self.positive_probe_state = 'exact'
            if self.fail_positive_attempt == self.positive_attempt_count:
                raise RuntimeError('injected positive control failure')
        elif 'DISTRIBUTED_DSB_DEFAULT_DENY ' in marker:
            self.probe_state = 'exact'
        if self.fail_on is not None and self.fail_on in marker:
            raise RuntimeError('injected network probe failure')
        if self.duplicate_on is not None and self.duplicate_on in marker:
            return f'{marker}\n{marker}\n'
        return marker + '\n'


def candidate_workload_attestation(job, lock):
    plan = distributed_k3s_candidate_plan(job['resources'])
    bundle = render_social_network_bundle(
        lock,
        plan.host('application').architecture,
        plan.load_generator_private_ip,
    )
    roles = set(bundle.expected_component_placement.values())
    return {
        'schema_version': 1,
        'namespace': bundle.namespace,
        'workload_revision': DISTRIBUTED_WORKLOAD_REVISION,
        'image_set_revision': DISTRIBUTED_IMAGE_SET_REVISION,
        'architecture': bundle.application_architecture,
        'platform': bundle.platform,
        'load_generator_cidr': bundle.load_generator_cidr,
        'image_lock_fingerprint': bundle.image_lock_fingerprint,
        'bundle_fingerprint': bundle.rendered_manifest_sha256,
        'component_count': len(bundle.expected_component_placement),
        'pod_nodes': tuple(sorted(
            plan.host(role).node_name for role in roles
        )),
    }


class DistributedRuntimePlanTests(unittest.TestCase):
    def test_legacy_azure_plan_constructor_remains_source_compatible(self):
        current = azure_k3s_candidate_plan(candidate_job()['resources'])

        legacy = AzureK3sCandidatePlan(
            current.topology_fingerprint,
            current.hosts,
            current.load_generator,
            current.load_generator_private_ip,
        )

        self.assertEqual(legacy.provider, 'azure')
        self.assertEqual(legacy.expected_nodes, current.expected_nodes)

    def test_plan_uses_exact_private_bastion_routes_and_mixed_architecture(self):
        job = candidate_job()

        plan = azure_k3s_candidate_plan(job['resources'])

        self.assertEqual(
            tuple(host.key for host in plan.hosts),
            ('control', 'database', 'cache', 'application'),
        )
        self.assertEqual(plan.host('control').jump_host_key, None)
        self.assertEqual(
            plan.load_generator_private_ip,
            PRIVATE_ADDRESSES['load-generator'],
        )
        self.assertEqual(plan.load_generator.key, 'load-generator')
        self.assertEqual(plan.load_generator.role, 'load_generator')
        self.assertEqual(plan.load_generator.architecture, 'x86_64')
        self.assertEqual(
            plan.load_generator.host_key,
            'azure_dsb_load_generator_public_ip',
        )
        self.assertIsNone(plan.load_generator.jump_host_key)
        for key in ('database', 'cache', 'application'):
            with self.subTest(key=key):
                self.assertEqual(
                    plan.host(key).jump_host_key,
                    'azure_dsb_control_public_ip',
                )
                self.assertEqual(
                    plan.host(key).host_key,
                    f'azure_dsb_{key}_private_ip',
                )
        self.assertEqual(plan.host('application').architecture, 'aarch64')

    def test_plan_rejects_tampered_identity_address_and_lifecycle(self):
        cases = []
        job = candidate_job()
        job['resources']['azure_dsb_cache_private_ip'] = '10.240.1.99'
        cases.append(job)
        job = candidate_job()
        job['resources']['azure_dsb_control_instance_expected_id'] += '-other'
        cases.append(job)
        job = candidate_job()
        inventory = job['resources']['role_node_inventory']
        next(
            node for node in inventory['nodes'] if node['key'] == 'database'
        )['lifecycle_status'] = 'creating'
        cases.append(job)
        job = candidate_job()
        job['resources']['deathstarbench_topology_fingerprint'] = 'sha256:' + '0' * 64
        cases.append(job)

        for tampered in cases:
            with self.subTest(resources=tampered['resources']):
                with self.assertRaises(DistributedRuntimeError):
                    azure_k3s_candidate_plan(tampered['resources'])

    def test_plan_normalizes_azure_resource_id_case(self):
        job = candidate_job()
        resources = job['resources']
        for node in resources['role_node_inventory']['nodes']:
            node['provider_resource_id'] = node[
                'provider_resource_id'
            ].upper()
            normalized = node['key'].replace('-', '_')
            prefix = f'azure_dsb_{normalized}_instance'
            resources[f'{prefix}_id'] = resources[f'{prefix}_id'].swapcase()
            resources[f'{prefix}_expected_id'] = resources[
                f'{prefix}_expected_id'
            ].upper()
            for storage in node['storage']:
                storage['provider_resource_id'] = storage[
                    'provider_resource_id'
                ].upper()
                resources['azure_dsb_database_data_disk_id'] = resources[
                    'azure_dsb_database_data_disk_id'
                ].swapcase()

        plan = azure_k3s_candidate_plan(resources)

        self.assertEqual(plan.host('control').role, 'control')

    def test_plan_rejects_ssh_user_and_k3s_name_before_remote_work(self):
        job = candidate_job()
        job['resources']['ssh_user'] = 'root'
        with self.assertRaisesRegex(DistributedRuntimeError, 'SSH user'):
            azure_k3s_candidate_plan(job['resources'])

        job = candidate_job()
        resources = job['resources']
        control = next(
            node for node in resources['role_node_inventory']['nodes']
            if node['key'] == 'control'
        )
        old_name = control['provider_resource_name']
        new_name = 'invalid_control_name'
        new_id = control['provider_resource_id'].replace(old_name, new_name)
        control['provider_resource_name'] = new_name
        control['provider_resource_id'] = new_id
        resources['azure_dsb_control_instance_name'] = new_name
        resources['azure_dsb_control_instance_id'] = new_id
        resources['azure_dsb_control_instance_expected_id'] = new_id

        with self.assertRaisesRegex(
            DistributedRuntimeError,
            'K3s node identity is invalid',
        ):
            azure_k3s_candidate_plan(resources)

    def test_plan_rejects_tampered_destructive_storage_contract(self):
        mutations = {
            'device': '/dev/sdz',
            'size_gb': 101.0,
            'provisioned_iops': 3001,
            'provisioned_throughput_mibps': 126.0,
            'ephemeral': True,
        }
        for field, value in mutations.items():
            with self.subTest(field=field):
                job = candidate_job()
                database = next(
                    node
                    for node in job['resources']['role_node_inventory']['nodes']
                    if node['key'] == 'database'
                )
                database['storage'][0][field] = value

                with self.assertRaisesRegex(
                    DistributedRuntimeError,
                    'destructive-storage contract',
                ):
                    azure_k3s_candidate_plan(job['resources'])

        job = candidate_job()
        job['resources']['azure_database_disk_iops'] = 9000
        with self.assertRaisesRegex(
            DistributedRuntimeError,
            'destructive-storage contract',
        ):
            azure_k3s_candidate_plan(job['resources'])

        job = candidate_job()
        database = next(
            node
            for node in job['resources']['role_node_inventory']['nodes']
            if node['key'] == 'database'
        )
        extra = copy.deepcopy(database['storage'][0])
        extra['key'] = 'unexpected-data'
        database['storage'].append(extra)
        with self.assertRaisesRegex(
            DistributedRuntimeError,
            'persistent-storage contract is incomplete',
        ):
            azure_k3s_candidate_plan(job['resources'])


class GcpDistributedRuntimeTests(unittest.TestCase):
    def test_gcp_plan_uses_same_roles_with_strict_gce_disk_and_routes(self):
        job = gcp_candidate_job()

        plan = gcp_k3s_candidate_plan(job['resources'])

        self.assertEqual(plan.provider, 'gcp')
        self.assertEqual(
            tuple(host.key for host in plan.hosts),
            ('control', 'database', 'cache', 'application'),
        )
        self.assertEqual(plan.database_device_name, 'benchmark-database-data')
        self.assertEqual(
            plan.host('database').jump_host_key,
            'gcp_dsb_control_public_ip',
        )
        self.assertEqual(
            plan.host('application').host_key,
            'gcp_dsb_application_private_ip',
        )
        self.assertEqual(
            plan.host('application').jump_host_key,
            'gcp_dsb_control_public_ip',
        )
        self.assertEqual(
            plan.load_generator.host_key,
            'gcp_dsb_load_generator_public_ip',
        )
        self.assertEqual(plan.host('application').architecture, 'aarch64')
        self.assertEqual(distributed_k3s_candidate_plan(job['resources']), plan)

    def test_gcp_plan_rejects_numeric_identity_and_stable_device_drift(self):
        job = gcp_candidate_job()
        job['resources']['gcp_dsb_cache_instance_id'] = '9999'
        with self.assertRaisesRegex(DistributedRuntimeError, 'identity conflicts'):
            gcp_k3s_candidate_plan(job['resources'])

        job = gcp_candidate_job()
        database = next(
            node
            for node in job['resources']['role_node_inventory']['nodes']
            if node['key'] == 'database'
        )
        database['storage'][0]['device'] = '/dev/sdb'
        with self.assertRaisesRegex(
            DistributedRuntimeError,
            'destructive-storage contract',
        ):
            gcp_k3s_candidate_plan(job['resources'])

    def test_gcp_volume_parser_binds_name_link_and_uuid(self):
        output = (
            'GCP_DSB_DATABASE_VOLUME device_name=benchmark-database-data '
            'device_link=/dev/disk/by-id/google-benchmark-database-data '
            'uuid=11111111-2222-3333-4444-555555555555 '
            'mount_point=/var/lib/deathstarbench/database filesystem=xfs\n'
        )

        parsed = parse_gcp_database_volume_attestation(
            output,
            expected_device_name='benchmark-database-data',
        )

        self.assertEqual(parsed['device_name'], 'benchmark-database-data')
        self.assertNotIn('lun', parsed)
        with self.assertRaises(DistributedRuntimeError):
            parse_gcp_database_volume_attestation(
                output.replace('benchmark-database-data', 'another-disk'),
                expected_device_name='benchmark-database-data',
            )

    def test_gcp_runs_identical_k3s_and_workload_orchestration(self):
        job = gcp_candidate_job()
        runtime_executor = GcpFakeRemoteExecutor()

        runtime = prepare_gcp_distributed_k3s_candidate(
            job,
            execute=runtime_executor,
        )

        self.assertEqual(runtime['state'], 'cluster_ready')
        self.assertEqual(
            runtime['database_volume']['device_name'],
            'benchmark-database-data',
        )
        self.assertTrue(any(
            'GCP_DSB_DATABASE_VOLUME' in command
            for command, _ in runtime_executor.calls
        ))
        lock = candidate_image_lock()
        attestation = candidate_workload_attestation(job, lock)
        workload_executor = GcpFakeRemoteExecutor()
        with mock.patch(
            'app.deathstarbench_distributed.parse_workload_attestation',
            return_value=attestation,
        ):
            workload = prepare_gcp_distributed_social_network_candidate(
                job,
                lock,
                execute=workload_executor,
            )

        self.assertEqual(workload['state'], 'workload_ready')
        self.assertTrue(any(
            'GCP_DSB_WORKLOAD_STORAGE' in command
            for command, _ in workload_executor.calls
        ))
        payload_calls = [
            kwargs for _, kwargs in workload_executor.calls
            if 'stdin_text' in kwargs
        ]
        self.assertEqual(
            len(payload_calls),
            len(render_social_network_bundle(
                lock,
                'arm64',
                PRIVATE_ADDRESSES['load-generator'],
            ).phase_names),
        )


def projected_runtime(provider):
    prefix = f'{provider}_dsb'
    private_addresses = {
        'control': '10.240.1.10',
        'database': '10.240.3.11',
        'cache': '10.240.3.12',
        'application': '10.240.1.13',
        'load-generator': '10.240.2.10',
    }
    resources = {'provider': provider}
    nodes = {}
    for index, (key, role) in enumerate((
        ('control', 'control'),
        ('database', 'database'),
        ('cache', 'cache'),
        ('application', 'application'),
        ('load-generator', 'load_generator'),
    ), start=10):
        normalized = key.replace('-', '_')
        direct = key in {'control', 'load-generator'}
        host_key = (
            f'{prefix}_{normalized}_public_ip'
            if direct
            else f'{prefix}_{normalized}_private_ip'
        )
        public_ip = f'198.51.100.{index}' if key in {
            'control', 'application', 'load-generator'
        } else None
        resources[f'{prefix}_{normalized}_private_ip'] = private_addresses[key]
        resources[f'{prefix}_{normalized}_public_ip'] = public_ip
        nodes[key] = {
            'key': key,
            'role': role,
            'node_name': f'dsb-{key}',
            'instance_id': f'{provider}-instance-{key}',
            'shape': 'candidate-shape',
            'architecture': 'x86_64',
            'private_ip': private_addresses[key],
            'public_ip': public_ip,
            'host_key': host_key,
            'jump_host_key': (
                None if direct else f'{prefix}_control_public_ip'
            ),
        }
    projection = {
        'provider': provider,
        'topology_fingerprint': 'sha256:' + 'a' * 64,
        'region': 'us-east-1' if provider == 'aws' else 'us-ashburn-1',
        'database_volume_id': (
            'vol-0123456789abcdef0'
            if provider == 'aws'
            else 'ocid1.volume.oc1.iad.example'
        ),
        'database_device': (
            '/dev/oracleoci/oraclevdb' if provider == 'oci' else None
        ),
        'nodes': nodes,
    }
    return resources, projection


class AwsOciDistributedRuntimeTests(unittest.TestCase):
    def test_aws_and_oci_plan_dispatch_preserves_guest_and_route_contracts(self):
        cases = (
            (
                'aws',
                aws_k3s_candidate_plan,
                'app.providers.aws.distributed_deathstarbench_candidate_projection',
                'rocky_linux_9',
            ),
            (
                'oci',
                oci_k3s_candidate_plan,
                'app.providers.oci.distributed_deathstarbench_candidate_projection',
                'oracle_linux_9',
            ),
        )
        for provider, factory, target, guest_os in cases:
            with self.subTest(provider=provider):
                resources, projection = projected_runtime(provider)
                with mock.patch(target, return_value=projection):
                    plan = factory(resources)
                    dispatched = distributed_k3s_candidate_plan(resources)

                self.assertEqual(dispatched, plan)
                self.assertEqual(plan.provider, provider)
                self.assertEqual(plan.guest_os, guest_os)
                self.assertEqual(plan.region, projection['region'])
                self.assertEqual(
                    plan.database_volume_id,
                    projection['database_volume_id'],
                )
                self.assertEqual(
                    plan.host('database').jump_host_key,
                    f'{provider}_dsb_control_public_ip',
                )
                self.assertEqual(
                    plan.load_generator.host_key,
                    f'{provider}_dsb_load_generator_public_ip',
                )

    def test_aws_and_oci_volume_attestations_bind_exact_provider_identity(self):
        filesystem_uuid = '11111111-2222-3333-4444-555555555555'
        aws_volume = 'vol-0123456789abcdef0'
        aws_output = (
            f'AWS_DSB_DATABASE_VOLUME volume_id={aws_volume} '
            'device_link=/dev/disk/by-id/'
            'nvme-Amazon_Elastic_Block_Store_vol0123456789abcdef0 '
            f'uuid={filesystem_uuid} '
            'mount_point=/var/lib/deathstarbench/database filesystem=xfs\n'
        )
        oci_volume = 'ocid1.volume.oc1.iad.example'
        oci_output = (
            f'OCI_DSB_DATABASE_VOLUME volume_id={oci_volume} '
            'device_link=/dev/oracleoci/oraclevdb '
            f'uuid={filesystem_uuid} '
            'mount_point=/var/lib/deathstarbench/database filesystem=xfs\n'
        )

        self.assertEqual(
            parse_aws_database_volume_attestation(
                aws_output,
                expected_volume_id=aws_volume,
            )['volume_id'],
            aws_volume,
        )
        self.assertEqual(
            parse_oci_database_volume_attestation(
                oci_output,
                expected_volume_id=oci_volume,
            )['device_link'],
            '/dev/oracleoci/oraclevdb',
        )
        with self.assertRaises(DistributedRuntimeError):
            parse_aws_database_volume_attestation(
                aws_output.replace(aws_volume, 'vol-11111111111111111'),
                expected_volume_id=aws_volume,
            )
        with self.assertRaises(DistributedRuntimeError):
            parse_oci_database_volume_attestation(
                oci_output.replace('oraclevdb', 'oraclevdc'),
                expected_volume_id=oci_volume,
            )

    def test_provider_workload_storage_markers_are_not_interchangeable(self):
        filesystem_uuid = '11111111-2222-3333-4444-555555555555'
        for provider, marker in (
            ('aws', 'AWS_DSB_WORKLOAD_STORAGE'),
            ('oci', 'OCI_DSB_WORKLOAD_STORAGE'),
        ):
            output = (
                f'{marker} uuid={filesystem_uuid} '
                'root=/var/lib/deathstarbench/database/mongodb databases=6\n'
            )
            self.assertEqual(
                parse_database_workload_storage_attestation(
                    output,
                    expected_filesystem_uuid=filesystem_uuid,
                    provider=provider,
                )['database_count'],
                6,
            )
            with self.assertRaises(DistributedRuntimeError):
                parse_database_workload_storage_attestation(
                    output,
                    expected_filesystem_uuid=filesystem_uuid,
                    provider='oci' if provider == 'aws' else 'aws',
                )

    def test_prepare_selects_explicit_guest_adapter_before_remote_work(self):
        for provider, expected_command in (
            ('aws', 'prepare-rocky'),
            ('oci', 'prepare-oracle'),
        ):
            resources, projection = projected_runtime(provider)
            plan_factory = mock.Mock()
            target = (
                'app.providers.aws.distributed_deathstarbench_candidate_projection'
                if provider == 'aws'
                else 'app.providers.oci.distributed_deathstarbench_candidate_projection'
            )
            with mock.patch(target, return_value=projection):
                plan_factory.return_value = distributed_k3s_candidate_plan(resources)
            observed = []

            def stop_after_first(_job, command, **_kwargs):
                observed.append(command)
                raise RuntimeError('stop after guest adapter selection')

            job = {'resources': resources}
            with (
                mock.patch(
                    'app.deathstarbench_distributed.rocky_host_prepare_command',
                    return_value='prepare-rocky',
                ),
                mock.patch(
                    'app.deathstarbench_distributed.oracle_linux_host_prepare_command',
                    return_value='prepare-oracle',
                ),
                self.assertRaisesRegex(RuntimeError, 'stop after'),
            ):
                _prepare_distributed_k3s_candidate(
                    job,
                    plan_factory=plan_factory,
                    execute=stop_after_first,
                )

            self.assertEqual(observed, [expected_command])

    def test_oracle_host_prepare_retries_one_ssh_failure(self):
        resources, projection = projected_runtime('oci')
        plan_factory = mock.Mock()
        with mock.patch(
            'app.providers.oci.distributed_deathstarbench_candidate_projection',
            return_value=projection,
        ):
            plan_factory.return_value = distributed_k3s_candidate_plan(resources)
        observed = []
        events = []

        def fail_once_then_stop(_job, command, **_kwargs):
            observed.append(command)
            if command == 'prepare-oracle' and observed.count(command) == 1:
                raise SSHCommandError('first-boot convergence failure')
            if command != 'prepare-oracle':
                raise RuntimeError('stop after host preparation')
            return ''

        with (
            mock.patch(
                'app.deathstarbench_distributed.oracle_linux_host_prepare_command',
                return_value='prepare-oracle',
            ),
            self.assertRaisesRegex(RuntimeError, 'stop after host preparation'),
        ):
            _prepare_distributed_k3s_candidate(
                {'resources': resources},
                plan_factory=plan_factory,
                execute=fail_once_then_stop,
                emit=lambda _job, label, message: events.append((label, message)),
            )

        self.assertEqual(observed[:2], ['prepare-oracle', 'prepare-oracle'])
        self.assertEqual(
            [message for _label, message in events if 'retrying once' in message],
            [
                'Oracle Linux host preparation did not converge on the first '
                'attempt for dsb-control; retrying once.'
            ],
        )

    def test_oracle_host_prepare_propagates_second_ssh_failure(self):
        resources, projection = projected_runtime('oci')
        plan_factory = mock.Mock()
        with mock.patch(
            'app.providers.oci.distributed_deathstarbench_candidate_projection',
            return_value=projection,
        ):
            plan_factory.return_value = distributed_k3s_candidate_plan(resources)
        observed = []

        def always_fail(_job, command, **_kwargs):
            observed.append(command)
            raise SSHCommandError('persistent host preparation failure')

        with (
            mock.patch(
                'app.deathstarbench_distributed.oracle_linux_host_prepare_command',
                return_value='prepare-oracle',
            ),
            self.assertRaisesRegex(
                SSHCommandError,
                'persistent host preparation failure',
            ),
        ):
            _prepare_distributed_k3s_candidate(
                {'resources': resources},
                plan_factory=plan_factory,
                execute=always_fail,
            )

        self.assertEqual(observed, ['prepare-oracle', 'prepare-oracle'])

    def test_rocky_host_prepare_does_not_retry_ssh_failure(self):
        resources, projection = projected_runtime('aws')
        plan_factory = mock.Mock()
        with mock.patch(
            'app.providers.aws.distributed_deathstarbench_candidate_projection',
            return_value=projection,
        ):
            plan_factory.return_value = distributed_k3s_candidate_plan(resources)
        observed = []

        def fail(_job, command, **_kwargs):
            observed.append(command)
            raise SSHCommandError('rocky preparation failure')

        with (
            mock.patch(
                'app.deathstarbench_distributed.rocky_host_prepare_command',
                return_value='prepare-rocky',
            ),
            self.assertRaisesRegex(SSHCommandError, 'rocky preparation failure'),
        ):
            _prepare_distributed_k3s_candidate(
                {'resources': resources},
                plan_factory=plan_factory,
                execute=fail,
            )

        self.assertEqual(observed, ['prepare-rocky'])


class DistributedRuntimeOrchestrationTests(unittest.TestCase):
    def test_cluster_bootstrap_is_ordered_attested_and_secret_safe(self):
        job = candidate_job()
        execute = FakeRemoteExecutor()
        events = []
        persisted = []

        journal = prepare_azure_distributed_k3s_candidate(
            job,
            execute=execute,
            emit=lambda _job, label, message: events.append((label, message)),
            persist=lambda current: persisted.append(
                dict(current['resources'][RUNTIME_JOURNAL_KEY])
            ),
        )

        self.assertEqual(journal['state'], 'cluster_ready')
        self.assertEqual(
            journal['prepared_hosts'],
            ['control', 'database', 'cache', 'application'],
        )
        self.assertEqual(
            journal['joined_agents'],
            ['database', 'cache', 'application'],
        )
        self.assertEqual(journal['server_ca_sha256'], 'a' * 64)
        self.assertEqual(
            journal['database_volume']['device_link'],
            '/dev/disk/azure/data/by-lun/0',
        )
        self.assertGreater(len(persisted), 8)
        self.assertEqual(persisted[0]['state'], 'preparing_hosts')
        self.assertEqual(persisted[-1]['state'], 'cluster_ready')

        host_keys = [kwargs['host_key'] for _, kwargs in execute.calls]
        self.assertEqual(host_keys[:3], ['azure_dsb_control_public_ip'] * 3)
        jumped = [
            kwargs
            for _, kwargs in execute.calls
            if kwargs['host_key'] in {
                'azure_dsb_database_private_ip',
                'azure_dsb_cache_private_ip',
                'azure_dsb_application_private_ip',
            }
        ]
        self.assertTrue(jumped)
        self.assertTrue(all(
            item['jump_host_key'] == 'azure_dsb_control_public_ip'
            for item in jumped
        ))
        database_commands = [
            command
            for command, kwargs in execute.calls
            if kwargs['host_key'] == 'azure_dsb_database_private_ip'
        ]
        self.assertTrue(any(
            '/var/lib/deathstarbench/database' in command
            for command in database_commands
        ))
        self.assertTrue(any(
            'kubectl get nodes' in command for command, _ in execute.calls
        ))
        self.assertTrue(any(
            'deployment/coredns' in command for command, _ in execute.calls
        ))
        self.assertTrue(any(
            'patch deployment coredns --type=merge' in command
            for command, _ in execute.calls
        ))
        token_reads = [
            kwargs
            for command, kwargs in execute.calls
            if '/var/lib/rancher/k3s/server/agent-token' in command
        ]
        self.assertEqual(token_reads, [{'timeout': 120,
                                       'host_key': 'azure_dsb_control_public_ip',
                                       'sensitive_output': True}])

        secret_calls = [
            (command, kwargs)
            for command, kwargs in execute.calls
            if 'secret_stdin' in kwargs
        ]
        self.assertEqual(len(secret_calls), 3)
        for command, kwargs in secret_calls:
            self.assertIn(K3S_AGENT_TOKEN_FILE, command)
            self.assertEqual(kwargs['secret_stdin'], execute.secure_token)
            self.assertNotIn(execute.secure_token, command)
        serialized = repr(job) + repr(events) + repr(persisted)
        self.assertNotIn(execute.secure_token, serialized)

    def test_failure_leaves_nonsecret_progress_and_stops(self):
        job = candidate_job()
        execute = FakeRemoteExecutor(fail_at=5)

        with self.assertRaisesRegex(RuntimeError, 'injected'):
            prepare_azure_distributed_k3s_candidate(job, execute=execute)

        journal = job['resources'][RUNTIME_JOURNAL_KEY]
        self.assertEqual(journal['state'], 'preparing_hosts')
        self.assertEqual(journal['prepared_hosts'], ['control'])
        self.assertNotIn(execute.secure_token, repr(job))
        self.assertEqual(len(execute.calls), 5)

    def test_partial_retry_preserves_progress_until_revalidation_succeeds(self):
        job = candidate_job()
        with self.assertRaises(RuntimeError):
            prepare_azure_distributed_k3s_candidate(
                job,
                execute=FakeRemoteExecutor(fail_at=5),
            )
        prior = copy.deepcopy(job['resources'][RUNTIME_JOURNAL_KEY])
        persisted = []

        journal = prepare_azure_distributed_k3s_candidate(
            job,
            execute=FakeRemoteExecutor(),
            persist=lambda current: persisted.append(copy.deepcopy(
                current['resources'][RUNTIME_JOURNAL_KEY]
            )),
        )

        self.assertEqual(prior['prepared_hosts'], ['control'])
        self.assertEqual(journal['state'], 'cluster_ready')
        self.assertEqual(persisted[0]['state'], 'hosts_prepared')

    def test_completed_retry_rejects_changed_database_or_server_identity(self):
        for changed, message in (
            ({'filesystem_uuid': 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'},
             'database volume identity changed'),
            ({'ca_sha256': 'b' * 64}, 'server CA fingerprint changed'),
        ):
            with self.subTest(changed=changed):
                job = candidate_job()
                prepare_azure_distributed_k3s_candidate(
                    job,
                    execute=FakeRemoteExecutor(),
                )
                prior = copy.deepcopy(
                    job['resources'][RUNTIME_JOURNAL_KEY]
                )

                with self.assertRaisesRegex(DistributedRuntimeError, message):
                    prepare_azure_distributed_k3s_candidate(
                        job,
                        execute=FakeRemoteExecutor(**changed),
                    )

                self.assertEqual(
                    job['resources'][RUNTIME_JOURNAL_KEY],
                    prior,
                )

    def test_cancellation_propagates_without_erasing_prior_journal(self):
        job = candidate_job()
        prepare_azure_distributed_k3s_candidate(
            job,
            execute=FakeRemoteExecutor(),
        )
        prior = copy.deepcopy(job['resources'][RUNTIME_JOURNAL_KEY])

        class CancellingExecutor(FakeRemoteExecutor):
            def __call__(self, job, command, **kwargs):
                self.calls.append((command, kwargs))
                raise RunCancelled('cancelled by test')

        with self.assertRaises(RunCancelled):
            prepare_azure_distributed_k3s_candidate(
                job,
                execute=CancellingExecutor(),
            )

        self.assertEqual(job['resources'][RUNTIME_JOURNAL_KEY], prior)

    def test_conflicting_existing_journal_fails_before_remote_work(self):
        job = candidate_job()
        job['resources'][RUNTIME_JOURNAL_KEY] = {
            'schema_version': 99,
            'runtime_revision': DISTRIBUTED_RUNTIME_REVISION,
            'topology_fingerprint': job['resources'][
                'deathstarbench_topology_fingerprint'
            ],
            'state': 'cluster_ready',
            'prepared_hosts': list(PRIVATE_ADDRESSES),
            'joined_agents': list(PRIVATE_ADDRESSES)[1:4],
            'server_ca_sha256': 'a' * 64,
            'database_volume': None,
        }
        execute = FakeRemoteExecutor()

        with self.assertRaises(DistributedRuntimeError):
            prepare_azure_distributed_k3s_candidate(job, execute=execute)

        self.assertEqual(execute.calls, [])

    def test_database_volume_attestation_is_strict(self):
        output = (
            'AZURE_DSB_DATABASE_VOLUME lun=0 '
            'device_link=/dev/disk/azure/scsi1/lun0 '
            'uuid=AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE '
            'mount_point=/var/lib/deathstarbench/database filesystem=xfs\n'
        )

        parsed = parse_database_volume_attestation(output)

        self.assertEqual(
            parsed['filesystem_uuid'],
            'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
        )
        with self.assertRaises(DistributedRuntimeError):
            parse_database_volume_attestation(
                output.replace('scsi1/lun0', 'scsi1/lun1')
            )

    def test_database_workload_storage_attestation_is_uuid_bound(self):
        filesystem_uuid = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
        output = (
            'AZURE_DSB_WORKLOAD_STORAGE '
            f'uuid={filesystem_uuid.upper()} '
            'root=/var/lib/deathstarbench/database/mongodb databases=6\n'
        )

        parsed = parse_database_workload_storage_attestation(
            output,
            expected_filesystem_uuid=filesystem_uuid,
        )

        self.assertEqual(parsed['filesystem_uuid'], filesystem_uuid)
        self.assertEqual(parsed['database_count'], 6)
        with self.assertRaises(DistributedRuntimeError):
            parse_database_workload_storage_attestation(
                output.replace('databases=6', 'databases=5'),
                expected_filesystem_uuid=filesystem_uuid,
            )
        with self.assertRaises(DistributedRuntimeError):
            parse_database_workload_storage_attestation(
                output,
                expected_filesystem_uuid=(
                    '11111111-2222-3333-4444-555555555555'
                ),
            )

    def test_compact_runner_fails_closed_for_distributed_contract(self):
        plan = SimpleNamespace(
            deathstarbench=SimpleNamespace(
                topology_id=DISTRIBUTED_TIERED_TOPOLOGY_ID,
                runtime_id=K3S_RUNTIME_ID,
                workload='social_network',
            )
        )

        with self.assertRaisesRegex(RuntimeError, 'qualification path'):
            run_deathstarbench({'resources': {}, 'results': []}, plan)


class DistributedWorkloadOrchestrationTests(unittest.TestCase):
    def ready_cluster_job(self):
        job = candidate_job()
        prepare_azure_distributed_k3s_candidate(
            job,
            execute=FakeRemoteExecutor(),
        )
        return job

    def test_workload_is_phased_stdin_only_attested_and_result_free(self):
        job = self.ready_cluster_job()
        lock = candidate_image_lock()
        execute = FakeRemoteExecutor()
        events = []
        persisted = []
        attestation = candidate_workload_attestation(job, lock)

        with mock.patch(
            'app.deathstarbench_distributed.parse_workload_attestation',
            return_value=attestation,
        ):
            journal = prepare_azure_distributed_social_network_candidate(
                job,
                lock,
                execute=execute,
                emit=lambda _job, label, message: events.append(
                    (label, message)
                ),
                persist=lambda current: persisted.append(copy.deepcopy(
                    current['resources'][WORKLOAD_JOURNAL_KEY]
                )),
            )

        bundle = render_social_network_bundle(lock, 'arm64', '10.240.2.10')
        self.assertEqual(journal['state'], 'workload_ready')
        self.assertEqual(
            journal['applied_phases'],
            list(bundle.phase_names),
        )
        self.assertEqual(
            journal['database_storage']['filesystem_uuid'],
            '11111111-2222-3333-4444-555555555555',
        )
        self.assertEqual(
            journal['workload_attestation']['pod_nodes'],
            ['dsb-application', 'dsb-cache', 'dsb-control', 'dsb-database'],
        )
        self.assertEqual(job['results'], [])
        self.assertEqual(persisted[0]['state'], 'preparing_storage')
        self.assertEqual(persisted[-1]['state'], 'workload_ready')

        payload_calls = [
            (command, kwargs)
            for command, kwargs in execute.calls
            if 'stdin_text' in kwargs
        ]
        self.assertEqual(len(payload_calls), len(bundle.phase_names))
        for (command, kwargs), (phase, payload) in zip(
            payload_calls,
            zip(bundle.phase_names, bundle.phase_payloads, strict=True),
            strict=True,
        ):
            self.assertEqual(kwargs['stdin_text'], payload)
            self.assertIn(bundle.phase_sha256(phase), command)
            self.assertNotIn(payload, command)
            self.assertNotIn('registry.example', command)
            self.assertEqual(
                kwargs['host_key'],
                'azure_dsb_control_public_ip',
            )
        serialized_events = repr(events)
        self.assertNotIn('registry.example', serialized_events)
        self.assertNotIn('@sha256:', serialized_events)

        ca_calls = [
            (command, kwargs)
            for command, kwargs in execute.calls
            if '/var/lib/rancher/k3s/server/tls/server-ca.crt' in command
        ]
        self.assertEqual(len(ca_calls), 1)
        ca_command, ca_kwargs = ca_calls[0]
        self.assertIn('sudo sha256sum', ca_command)
        self.assertNotIn('/server/agent-token', ca_command)
        self.assertEqual(ca_kwargs, {
            'timeout': 120,
            'host_key': 'azure_dsb_control_public_ip',
        })

        volume_calls = [
            (command, kwargs)
            for command, kwargs in execute.calls
            if 'AZURE_DSB_DATABASE_VOLUME' in command
        ]
        self.assertEqual(len(volume_calls), 1)
        volume_command, volume_kwargs = volume_calls[0]
        self.assertIn(
            'EXPECTED_UUID=11111111-2222-3333-4444-555555555555',
            volume_command,
        )
        self.assertIn('grep -Fxq "$EXPECTED_FSTAB" /etc/fstab', volume_command)
        self.assertNotIn('mkfs', volume_command)
        self.assertNotIn('sudo mount', volume_command)
        self.assertNotIn('FSTAB_TMP=', volume_command)
        self.assertEqual(volume_kwargs, {
            'timeout': 300,
            'host_key': 'azure_dsb_database_private_ip',
            'jump_host_key': 'azure_dsb_control_public_ip',
        })

    def test_workload_rejects_server_ca_drift_before_storage_or_apply(self):
        job = self.ready_cluster_job()
        execute = FakeRemoteExecutor(ca_sha256='b' * 64)

        with self.assertRaisesRegex(
            DistributedRuntimeError,
            'server CA identity changed',
        ):
            prepare_azure_distributed_social_network_candidate(
                job,
                candidate_image_lock(),
                execute=execute,
            )

        self.assertTrue(any(
            '/var/lib/rancher/k3s/server/tls/server-ca.crt' in command
            for command, _ in execute.calls
        ))
        self.assertFalse(any(
            'AZURE_DSB_DATABASE_VOLUME' in command
            or 'AZURE_DSB_WORKLOAD_STORAGE' in command
            or 'stdin_text' in kwargs
            for command, kwargs in execute.calls
        ))
        self.assertEqual(job['results'], [])

    def test_apply_failure_leaves_write_ahead_phase_and_retry_converges(self):
        job = self.ready_cluster_job()
        lock = candidate_image_lock()

        class FailSecondPhase(FakeRemoteExecutor):
            def __init__(self):
                super().__init__()
                self.apply_count = 0

            def __call__(self, job, command, **kwargs):
                if 'stdin_text' in kwargs:
                    self.apply_count += 1
                    if self.apply_count == 2:
                        self.calls.append((command, kwargs))
                        raise RuntimeError('injected workload apply failure')
                return super().__call__(job, command, **kwargs)

        with self.assertRaisesRegex(RuntimeError, 'workload apply failure'):
            prepare_azure_distributed_social_network_candidate(
                job,
                lock,
                execute=FailSecondPhase(),
            )

        journal = copy.deepcopy(job['resources'][WORKLOAD_JOURNAL_KEY])
        self.assertEqual(journal['state'], 'applying_storage')
        self.assertEqual(journal['applied_phases'], ['namespace'])
        attestation = candidate_workload_attestation(job, lock)
        retry = FakeRemoteExecutor()
        with mock.patch(
            'app.deathstarbench_distributed.parse_workload_attestation',
            return_value=attestation,
        ):
            completed = prepare_azure_distributed_social_network_candidate(
                job,
                lock,
                execute=retry,
            )

        self.assertEqual(completed['state'], 'workload_ready')
        self.assertEqual(
            len([kwargs for _, kwargs in retry.calls if 'stdin_text' in kwargs]),
            len(render_social_network_bundle(
                lock, 'arm64', '10.240.2.10'
            ).phase_names) - 1,
        )

    def test_changed_image_lock_is_rejected_before_remote_retry(self):
        job = self.ready_cluster_job()
        lock = candidate_image_lock()
        attestation = candidate_workload_attestation(job, lock)
        with mock.patch(
            'app.deathstarbench_distributed.parse_workload_attestation',
            return_value=attestation,
        ):
            prepare_azure_distributed_social_network_candidate(
                job,
                lock,
                execute=FakeRemoteExecutor(),
            )
        prior = copy.deepcopy(job['resources'][WORKLOAD_JOURNAL_KEY])
        changed = copy.deepcopy(lock)
        changed['platforms']['linux/arm64']['images'][
            'social-network-microservices'
        ] = (
            'registry.example/deathstarbench/social-network-microservices'
            '@sha256:' + '0' * 64
        )
        execute = FakeRemoteExecutor()

        with self.assertRaisesRegex(
            DistributedRuntimeError,
            'journal conflicts',
        ):
            prepare_azure_distributed_social_network_candidate(
                job,
                changed,
                execute=execute,
            )

        self.assertEqual(execute.calls, [])
        self.assertEqual(job['resources'][WORKLOAD_JOURNAL_KEY], prior)

    def test_workload_requires_cluster_ready_before_remote_work(self):
        job = candidate_job()
        execute = FakeRemoteExecutor()

        with self.assertRaisesRegex(
            DistributedRuntimeError,
            'cluster_ready',
        ):
            prepare_azure_distributed_social_network_candidate(
                job,
                candidate_image_lock(),
                execute=execute,
            )

        self.assertEqual(execute.calls, [])
        self.assertNotIn(WORKLOAD_JOURNAL_KEY, job['resources'])

    def test_readiness_drift_leaves_applied_journal_without_result(self):
        job = self.ready_cluster_job()
        lock = candidate_image_lock()

        with mock.patch(
            'app.deathstarbench_distributed.parse_workload_attestation',
            side_effect=WorkloadBundleError('policy drift'),
        ):
            with self.assertRaisesRegex(
                DistributedRuntimeError,
                'failed attestation',
            ):
                prepare_azure_distributed_social_network_candidate(
                    job,
                    lock,
                    execute=FakeRemoteExecutor(),
                )

        bundle = render_social_network_bundle(lock, 'arm64', '10.240.2.10')
        journal = job['resources'][WORKLOAD_JOURNAL_KEY]
        self.assertEqual(journal['state'], 'workloads_applied')
        self.assertEqual(journal['applied_phases'], list(bundle.phase_names))
        self.assertIsNone(journal['workload_attestation'])
        self.assertEqual(job['results'], [])

    def test_ready_retry_skips_applied_phases_and_restores_attestation(self):
        job = self.ready_cluster_job()
        lock = candidate_image_lock()
        attestation = candidate_workload_attestation(job, lock)
        with mock.patch(
            'app.deathstarbench_distributed.parse_workload_attestation',
            return_value=attestation,
        ):
            prepare_azure_distributed_social_network_candidate(
                job,
                lock,
                execute=FakeRemoteExecutor(),
            )

        prior_ready = copy.deepcopy(
            job['resources'][WORKLOAD_JOURNAL_KEY]
        )
        persisted = []
        retry = FakeRemoteExecutor()
        with mock.patch(
            'app.deathstarbench_distributed.parse_workload_attestation',
            return_value=attestation,
        ):
            completed = prepare_azure_distributed_social_network_candidate(
                job,
                lock,
                execute=retry,
                persist=lambda current: persisted.append(copy.deepcopy(
                    current['resources'][WORKLOAD_JOURNAL_KEY]
                )),
            )

        bundle = render_social_network_bundle(lock, 'arm64', '10.240.2.10')
        self.assertFalse(any(
            'stdin_text' in kwargs for _, kwargs in retry.calls
        ))
        self.assertGreaterEqual(len(persisted), 2)
        self.assertEqual(persisted[0]['state'], 'workloads_applied')
        self.assertEqual(
            persisted[0]['applied_phases'],
            list(bundle.phase_names),
        )
        self.assertIsNone(persisted[0]['workload_attestation'])
        self.assertTrue(all(
            entry['state'] != 'workload_ready'
            for entry in persisted[:-1]
        ))
        self.assertEqual(persisted[-1], completed)
        self.assertEqual(completed['state'], 'workload_ready')
        self.assertEqual(
            completed['workload_attestation'],
            prior_ready['workload_attestation'],
        )

    def test_ready_retry_readiness_failure_leaves_demoted_journal(self):
        job = self.ready_cluster_job()
        lock = candidate_image_lock()
        attestation = candidate_workload_attestation(job, lock)
        with mock.patch(
            'app.deathstarbench_distributed.parse_workload_attestation',
            return_value=attestation,
        ):
            prepare_azure_distributed_social_network_candidate(
                job,
                lock,
                execute=FakeRemoteExecutor(),
            )

        retry = FakeRemoteExecutor()
        with mock.patch(
            'app.deathstarbench_distributed.parse_workload_attestation',
            side_effect=WorkloadBundleError('retry readiness drift'),
        ):
            with self.assertRaisesRegex(
                DistributedRuntimeError,
                'failed attestation',
            ):
                prepare_azure_distributed_social_network_candidate(
                    job,
                    lock,
                    execute=retry,
                )

        bundle = render_social_network_bundle(lock, 'arm64', '10.240.2.10')
        self.assertFalse(any(
            'stdin_text' in kwargs for _, kwargs in retry.calls
        ))
        journal = job['resources'][WORKLOAD_JOURNAL_KEY]
        self.assertEqual(journal['state'], 'workloads_applied')
        self.assertEqual(journal['applied_phases'], list(bundle.phase_names))
        self.assertIsNone(journal['workload_attestation'])

    def test_completed_retry_cancellation_leaves_demoted_journal(self):
        job = self.ready_cluster_job()
        lock = candidate_image_lock()
        attestation = candidate_workload_attestation(job, lock)
        with mock.patch(
            'app.deathstarbench_distributed.parse_workload_attestation',
            return_value=attestation,
        ):
            prepare_azure_distributed_social_network_candidate(
                job,
                lock,
                execute=FakeRemoteExecutor(),
            )
        prior = copy.deepcopy(job['resources'][WORKLOAD_JOURNAL_KEY])
        persisted = []

        class CancellingExecutor(FakeRemoteExecutor):
            def __call__(self, job, command, **kwargs):
                self.calls.append((command, kwargs))
                raise RunCancelled('cancelled by test')

        with self.assertRaises(RunCancelled):
            prepare_azure_distributed_social_network_candidate(
                job,
                lock,
                execute=CancellingExecutor(),
                persist=lambda current: persisted.append(copy.deepcopy(
                    current['resources'][WORKLOAD_JOURNAL_KEY]
                )),
            )

        bundle = render_social_network_bundle(lock, 'arm64', '10.240.2.10')
        journal = job['resources'][WORKLOAD_JOURNAL_KEY]
        self.assertEqual(journal['state'], 'workloads_applied')
        self.assertEqual(journal['applied_phases'], list(bundle.phase_names))
        self.assertEqual(journal['database_storage'], prior['database_storage'])
        self.assertIsNone(journal['workload_attestation'])
        self.assertEqual(persisted, [journal])
        self.assertEqual(job['results'], [])


class DistributedNetworkQualificationTests(unittest.TestCase):
    def ready_workload_job(self, *, provider='azure'):
        lock = candidate_image_lock()
        if provider == 'azure':
            job = candidate_job()
            runtime_execute = FakeRemoteExecutor()
            prepare_runtime = prepare_azure_distributed_k3s_candidate
            prepare_workload = prepare_azure_distributed_social_network_candidate
        elif provider == 'gcp':
            job = gcp_candidate_job()
            runtime_execute = GcpFakeRemoteExecutor()
            prepare_runtime = prepare_gcp_distributed_k3s_candidate
            prepare_workload = prepare_gcp_distributed_social_network_candidate
        else:
            raise AssertionError(f'unsupported test provider {provider}')
        prepare_runtime(job, execute=runtime_execute)
        attestation = candidate_workload_attestation(job, lock)
        with mock.patch(
            'app.deathstarbench_distributed.parse_workload_attestation',
            return_value=attestation,
        ):
            prepare_workload(
                job,
                lock,
                execute=(
                    GcpFakeRemoteExecutor()
                    if provider == 'gcp'
                    else FakeRemoteExecutor()
                ),
            )
        return job, lock

    def test_qualification_is_bounded_digest_pinned_and_residue_free(self):
        job, lock = self.ready_workload_job()
        prior_job = copy.deepcopy(job)
        execute = NetworkQualificationExecutor()

        attestation = qualify_distributed_network_paths(
            job,
            lock,
            execute=execute,
        )

        self.assertEqual(job, prior_job)
        self.assertEqual(
            DISTRIBUTED_NETWORK_QUALIFICATION_KEY,
            'deathstarbench_distributed_network_qualification',
        )
        self.assertNotIn(DISTRIBUTED_NETWORK_QUALIFICATION_KEY, job['resources'])
        self.assertEqual(
            set(attestation),
            {
                'schema_version',
                'provider',
                'runtime_revision',
                'topology_fingerprint',
                'workload_revision',
                'image_set_revision',
                'image_lock_fingerprint',
                'namespace',
                'required_path',
                'forbidden_paths',
                'policy_positive_controls',
                'default_deny_path',
            },
        )
        self.assertEqual(
            attestation['schema_version'],
            NETWORK_QUALIFICATION_SCHEMA_VERSION,
        )
        self.assertEqual(NETWORK_QUALIFICATION_SCHEMA_VERSION, 3)
        self.assertEqual(attestation['provider'], 'azure')
        self.assertEqual(attestation['required_path'], {
            'source_role': 'load-generator',
            'destination_role': 'application',
            'protocol': 'TCP',
            'port': 8080,
            'outcome': 'connected',
            'timeout_seconds': NETWORK_TCP_PROBE_TIMEOUT_SECONDS,
        })
        self.assertEqual(
            [
                (path['destination_role'], path['port'], path['outcome'])
                for path in attestation['forbidden_paths']
            ],
            [
                ('control', 6443, 'blocked'),
                ('database', 22, 'blocked'),
                ('cache', 22, 'blocked'),
            ],
        )
        for path in attestation['forbidden_paths']:
            self.assertEqual(path['source_role'], 'load-generator')
            self.assertEqual(path['protocol'], 'TCP')
            self.assertEqual(
                path['timeout_seconds'],
                NETWORK_TCP_PROBE_TIMEOUT_SECONDS,
            )
        image_reference = lock['platforms']['linux/amd64']['images']['redis']
        image_digest = image_reference.rsplit('@', 1)[1]
        positive_control = {
            'source_kind': 'Pod',
            'source_name': NETWORK_POLICY_POSITIVE_CONTROL_POD,
            'source_node': 'dsb-cache',
            'source_node_role': 'cache',
            'source_architecture': 'x86_64',
            'source_policy_component': 'home-timeline-service',
            'probe_image_digest': image_digest,
            'destination_service': NETWORK_POLICY_PROBE_SERVICE,
            'protocol': 'TCP',
            'port': 6379,
            'outcome': 'connected',
            'response': 'PONG',
            'attempt_limit': NETWORK_POLICY_POSITIVE_ATTEMPT_LIMIT,
            'retry_interval_seconds': (
                NETWORK_POLICY_POSITIVE_RETRY_INTERVAL_SECONDS
            ),
            'deadline_seconds': NETWORK_POLICY_PROBE_DEADLINE_SECONDS,
            'cleanup_confirmed': True,
        }
        self.assertEqual(attestation['policy_positive_controls'], [
            {'position': 'before_negative', **positive_control},
            {'position': 'after_negative', **positive_control},
        ])
        self.assertEqual(attestation['default_deny_path'], {
            'source_kind': 'Pod',
            'source_name': NETWORK_POLICY_PROBE_POD,
            'source_node': 'dsb-cache',
            'source_node_role': 'cache',
            'source_architecture': 'x86_64',
            'probe_image_digest': image_digest,
            'destination_service': NETWORK_POLICY_PROBE_SERVICE,
            'protocol': 'TCP',
            'port': 6379,
            'outcome': 'blocked',
            'block_mode': 'drop',
            'settle_seconds': NETWORK_POLICY_PROBE_SETTLE_SECONDS,
            'deadline_seconds': NETWORK_POLICY_PROBE_DEADLINE_SECONDS,
            'cleanup_confirmed': True,
        })
        serialized = json.dumps(attestation, sort_keys=True)
        self.assertNotIn('registry.example', serialized)
        for address in PRIVATE_ADDRESSES.values():
            self.assertNotIn(address, serialized)

        self.assertEqual(len(execute.calls), 12)
        cleanup_calls = [
            (command, kwargs)
            for command, kwargs in execute.calls
            if 'DISTRIBUTED_DSB_DEFAULT_DENY_CLEANUP' in command
        ]
        self.assertEqual(len(cleanup_calls), 5)
        self.assertEqual(execute.calls[:2], cleanup_calls[:2])
        self.assertEqual(execute.calls[-2:], cleanup_calls[-2:])
        for command, kwargs in cleanup_calls:
            self.assertIn('--ignore-not-found=true', command)
            self.assertIn('--ignore-not-found=true -o name', command)
            self.assertIn('EXPECTED_IDENTITY=', command)
            self.assertIn('{.metadata.uid}', command)
            self.assertIn('"kind":"DeleteOptions"', command)
            self.assertIn('"preconditions":{"uid":"%s"}', command)
            self.assertIn('UID-preconditioned', command)
            self.assertIn('--request DELETE', command)
            self.assertIn('--noproxy "*"', command)
            self.assertIn(
                '/var/lib/rancher/k3s/server/tls/client-admin.crt',
                command,
            )
            self.assertNotIn(' delete pod ', command)
            self.assertIn(
                'deathstarbench\\.io/qualification-probe',
                command,
            )
            self.assertIn(
                'deathstarbench\\.io/image-lock-fingerprint',
                command,
            )
            self.assertIn(
                'deathstarbench\\.io/image-set-revision',
                command,
            )
            self.assertIn(
                'deathstarbench\\.io/workload-revision',
                command,
            )
            self.assertIn('dsb-cache', command)
            self.assertIn('redis-default-deny-probe', command)
            self.assertIn(image_reference, command)
            self.assertIn(
                'Refusing to delete a foreign network qualification probe Pod.',
                command,
            )
            self.assertLess(
                command.index('test "$ACTUAL_IDENTITY" = "$EXPECTED_IDENTITY"'),
                command.index('DELETE_STATUS=$(sudo curl'),
            )
            self.assertEqual(kwargs, {
                'timeout': 120,
                'host_key': 'azure_dsb_control_public_ip',
            })

        tcp_calls = [
            (command, kwargs)
            for command, kwargs in execute.calls
            if 'DISTRIBUTED_DSB_NETWORK_PATH' in command
        ]
        self.assertEqual(len(tcp_calls), 4)
        for command, kwargs in tcp_calls:
            self.assertIn(
                f'timeout --foreground --signal=TERM '
                f'{NETWORK_TCP_PROBE_TIMEOUT_SECONDS}s',
                command,
            )
            self.assertIn('/dev/tcp/10.240.1.', command)
            self.assertEqual(kwargs, {
                'timeout': 15,
                'host_key': 'azure_dsb_load_generator_public_ip',
            })
            self.assertNotIn('secret_stdin', kwargs)
        self.assertIn('destination=application', tcp_calls[0][0])
        self.assertIn('expectation=allow', tcp_calls[0][0])
        for command, _ in tcp_calls[1:]:
            self.assertIn('expectation=deny', command)
            self.assertIn('case "$STATUS" in 1|124)', command)

        positive_calls = [
            (command, kwargs)
            for command, kwargs in execute.calls
            if 'DISTRIBUTED_DSB_POSITIVE_CONTROL ' in command
        ]
        self.assertEqual(len(positive_calls), 2)
        self.assertEqual(positive_calls[0], positive_calls[1])
        positive_command, positive_kwargs = positive_calls[0]
        self.assertEqual(positive_kwargs['host_key'], 'azure_dsb_control_public_ip')
        self.assertEqual(positive_kwargs['timeout'], 90)
        self.assertNotIn('secret_stdin', positive_kwargs)
        self.assertIn('test "$PHASE" = Succeeded', positive_command)
        self.assertIn('test "$POD_LOG" = PONG', positive_command)
        self.assertIn('test "$EXIT_CODE" = 0', positive_command)
        self.assertIn('test "$ACTUAL_IMAGE" = "$EXPECTED_IMAGE"', positive_command)
        self.assertIn('test -n "$STARTED_AT"', positive_command)
        self.assertIn('test -n "$FINISHED_AT"', positive_command)
        self.assertIn('test "$RESTARTS" = 0', positive_command)
        self.assertIn('exceeded its observation bound', positive_command)
        self.assertIn('tail -c 2048', positive_command)
        positive_manifest = json.loads(positive_kwargs['stdin_text'])
        self.assertEqual(
            positive_manifest['metadata']['name'],
            NETWORK_POLICY_POSITIVE_CONTROL_POD,
        )
        self.assertEqual(
            positive_manifest['metadata']['labels'],
            {
                'deathstarbench.io/qualification-probe': 'allowed-control',
                'app.kubernetes.io/component': 'home-timeline-service',
            },
        )
        self.assertNotIn(
            'app.kubernetes.io/name',
            positive_manifest['metadata']['labels'],
        )
        self.assertEqual(positive_manifest['spec']['nodeName'], 'dsb-cache')
        self.assertEqual(
            positive_manifest['spec']['readinessGates'],
            [{'conditionType': 'deathstarbench.io/positive-control-ready'}],
        )
        self.assertEqual(
            positive_manifest['spec']['containers'][0]['image'],
            image_reference,
        )
        positive_probe_command = (
            positive_manifest['spec']['containers'][0]['command']
        )
        self.assertEqual(positive_probe_command[:2], ['/bin/sh', '-c'])
        self.assertEqual(len(positive_probe_command), 3)
        positive_probe_script = positive_probe_command[2]
        subprocess.run(
            ['sh', '-n'],
            input=positive_probe_script,
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertIn(
            f'while test "$ATTEMPT" -le '
            f'{NETWORK_POLICY_POSITIVE_ATTEMPT_LIMIT}',
            positive_probe_script,
        )
        self.assertIn(
            f'sleep {NETWORK_POLICY_POSITIVE_RETRY_INTERVAL_SECONDS}',
            positive_probe_script,
        )
        self.assertIn(
            f'-h {NETWORK_POLICY_PROBE_SERVICE} -p 6379 PING',
            positive_probe_script,
        )
        self.assertIn('test "$LAST_RESPONSE" = PONG', positive_probe_script)
        self.assertIn('printf "PONG\\n"; exit 0', positive_probe_script)
        self.assertIn(
            'DISTRIBUTED_DSB_POSITIVE_CONTROL_FAILED',
            positive_probe_script,
        )

        pod_calls = [
            (command, kwargs)
            for command, kwargs in execute.calls
            if 'DISTRIBUTED_DSB_DEFAULT_DENY ' in command
            and 'CLEANUP' not in command
        ]
        self.assertEqual(len(pod_calls), 1)
        pod_command, pod_kwargs = pod_calls[0]
        self.assertEqual(
            pod_kwargs['host_key'],
            'azure_dsb_control_public_ip',
        )
        self.assertEqual(pod_kwargs['timeout'], 90)
        self.assertNotIn('secret_stdin', pod_kwargs)
        self.assertIn(image_reference, pod_command)
        self.assertIn('for ATTEMPT in $(seq 1 40)', pod_command)
        self.assertIn('--request-timeout=2s', pod_command)
        self.assertIn('DeadlineExceeded:*) BLOCK_MODE=drop', pod_command)
        self.assertIn(':Error) test "$EXIT_CODE" = 1', pod_command)
        self.assertIn('test "$ACTUAL_IMAGE" = "$EXPECTED_IMAGE"', pod_command)
        self.assertIn('test -n "$STARTED_AT"', pod_command)
        self.assertIn('test -n "$FINISHED_AT"', pod_command)
        self.assertIn('test "$RESTARTS" = 0', pod_command)
        self.assertIn('exit status is invalid', pod_command)
        self.assertIn('termination reason is invalid', pod_command)
        self.assertIn('exceeded its observation bound', pod_command)
        self.assertIn('block_mode=drop', pod_command)
        self.assertIn('block_mode=reject', pod_command)
        manifest = json.loads(pod_kwargs['stdin_text'])
        self.assertEqual(manifest['kind'], 'Pod')
        self.assertEqual(manifest['metadata']['name'], NETWORK_POLICY_PROBE_POD)
        self.assertEqual(
            manifest['metadata']['namespace'],
            'deathstarbench-social',
        )
        spec = manifest['spec']
        self.assertEqual(spec['nodeName'], 'dsb-cache')
        self.assertEqual(
            spec['activeDeadlineSeconds'],
            NETWORK_POLICY_PROBE_DEADLINE_SECONDS,
        )
        self.assertFalse(spec['automountServiceAccountToken'])
        self.assertEqual(spec['restartPolicy'], 'Never')
        self.assertEqual(len(spec['containers']), 1)
        container = spec['containers'][0]
        self.assertEqual(container['image'], image_reference)
        self.assertRegex(container['image'], r'@sha256:[0-9a-f]{64}$')
        self.assertEqual(container['command'], [
            '/bin/sh',
            '-c',
            f'sleep {NETWORK_POLICY_PROBE_SETTLE_SECONDS}; exec redis-cli '
            f'-h {NETWORK_POLICY_PROBE_SERVICE} -p 6379 PING',
        ])
        positive_indexes = [
            index for index, (command, _) in enumerate(execute.calls)
            if 'DISTRIBUTED_DSB_POSITIVE_CONTROL ' in command
        ]
        negative_index = next(
            index for index, (command, _) in enumerate(execute.calls)
            if 'DISTRIBUTED_DSB_DEFAULT_DENY ' in command
            and 'CLEANUP' not in command
        )
        between_cleanup_index = next(
            index for index in range(positive_indexes[0] + 1, negative_index)
            if 'DISTRIBUTED_DSB_DEFAULT_DENY_CLEANUP' in execute.calls[index][0]
        )
        self.assertLess(positive_indexes[0], between_cleanup_index)
        self.assertLess(between_cleanup_index, negative_index)
        self.assertLess(negative_index, positive_indexes[1])
        self.assertIsNone(execute.probe_state)
        self.assertIsNone(execute.positive_probe_state)
        self.assertEqual(execute.delete_count, 3)

    def test_exact_interrupted_probe_is_adopted_for_cleanup(self):
        job, lock = self.ready_workload_job()
        execute = NetworkQualificationExecutor(initial_probe='exact')

        attestation = qualify_distributed_network_paths(
            job,
            lock,
            execute=execute,
        )

        self.assertTrue(attestation['default_deny_path']['cleanup_confirmed'])
        self.assertTrue(all(
            control['cleanup_confirmed']
            for control in attestation['policy_positive_controls']
        ))
        self.assertEqual(execute.cleanup_count, 5)
        self.assertEqual(execute.delete_count, 4)
        self.assertIsNone(execute.probe_state)
        self.assertIsNone(execute.positive_probe_state)

    def test_first_positive_control_failure_prevents_negative_probe(self):
        job, lock = self.ready_workload_job()
        execute = NetworkQualificationExecutor(
            fail_positive_attempt=1,
        )

        with self.assertRaisesRegex(RuntimeError, 'positive control failure'):
            qualify_distributed_network_paths(
                job,
                lock,
                execute=execute,
            )

        self.assertFalse(any(
            'DISTRIBUTED_DSB_DEFAULT_DENY pod=' in command
            for command, _ in execute.calls
        ))
        self.assertEqual(execute.positive_attempt_count, 1)
        self.assertEqual(execute.cleanup_count, 4)
        self.assertIsNone(execute.probe_state)
        self.assertIsNone(execute.positive_probe_state)

    def test_second_positive_control_failure_rejects_negative_attestation(self):
        job, lock = self.ready_workload_job()
        execute = NetworkQualificationExecutor(fail_positive_attempt=2)

        with self.assertRaisesRegex(RuntimeError, 'positive control failure'):
            qualify_distributed_network_paths(
                job,
                lock,
                execute=execute,
            )

        self.assertEqual(execute.negative_attempt_count, 1)
        self.assertEqual(execute.positive_attempt_count, 2)
        self.assertEqual(execute.cleanup_count, 5)
        self.assertIsNone(execute.probe_state)
        self.assertIsNone(execute.positive_probe_state)

    def test_reject_is_an_accepted_default_deny_result(self):
        job, lock = self.ready_workload_job()
        execute = NetworkQualificationExecutor(negative_mode='reject')

        attestation = qualify_distributed_network_paths(
            job,
            lock,
            execute=execute,
        )

        self.assertEqual(attestation['default_deny_path']['block_mode'], 'reject')
        self.assertEqual(
            [control['position'] for control in attestation['policy_positive_controls']],
            ['before_negative', 'after_negative'],
        )

    def test_negative_probe_success_is_bracketed_then_rejected(self):
        job, lock = self.ready_workload_job()
        execute = NetworkQualificationExecutor(negative_mode='success')

        with self.assertRaisesRegex(RuntimeError, 'reached Redis'):
            qualify_distributed_network_paths(
                job,
                lock,
                execute=execute,
            )

        self.assertEqual(execute.negative_attempt_count, 1)
        self.assertEqual(execute.positive_attempt_count, 2)
        self.assertEqual(execute.cleanup_count, 5)
        self.assertIsNone(execute.probe_state)
        self.assertIsNone(execute.positive_probe_state)

    def test_invalid_negative_exit_or_reason_is_bracketed_then_rejected(self):
        job, lock = self.ready_workload_job()
        for mode, message in (
            ('invalid_exit', 'exit status is invalid'),
            ('invalid_reason', 'termination reason is invalid'),
        ):
            with self.subTest(mode=mode):
                execute = NetworkQualificationExecutor(negative_mode=mode)
                with self.assertRaisesRegex(RuntimeError, message):
                    qualify_distributed_network_paths(
                        copy.deepcopy(job),
                        lock,
                        execute=execute,
                    )
                self.assertEqual(execute.negative_attempt_count, 1)
                self.assertEqual(execute.positive_attempt_count, 2)
                self.assertEqual(execute.cleanup_count, 5)
                self.assertIsNone(execute.probe_state)
                self.assertIsNone(execute.positive_probe_state)

    def test_cleanup_delete_is_bound_to_the_validated_pod_uid(self):
        command, marker = _network_policy_probe_cleanup_command(
            cache_node_name='dsb-cache',
            image_reference=(
                'registry.example/deathstarbench/redis@sha256:' + 'b' * 64
            ),
            image_lock_fingerprint='sha256:' + 'a' * 64,
        )

        self.assertIn(NETWORK_POLICY_PROBE_POD, marker)
        self.assertIn('{.metadata.uid}', command)
        self.assertIn(
            '"preconditions":{"uid":"%s"}',
            command,
        )
        self.assertIn('--request DELETE', command)
        self.assertIn(
            'test "$CURRENT_UID" = "$POD_UID"',
            command,
        )
        self.assertIn(
            'A foreign Pod replaced the network probe during cleanup.',
            command,
        )
        self.assertNotIn(' delete pod ', command)
        self.assertLess(
            command.index('ACTUAL_IDENTITY=${OBSERVED_IDENTITY#*|}'),
            command.index('DELETE_STATUS=$(sudo curl'),
        )

    def test_generated_network_probe_commands_are_valid_bash(self):
        image_reference = (
            'registry.example/deathstarbench/redis@sha256:' + 'b' * 64
        )
        image_digest = image_reference.rsplit('@', 1)[1]
        fingerprint = 'sha256:' + 'a' * 64
        positive_manifest = _network_policy_probe_manifest(
            cache_node_name='dsb-cache',
            image_reference=image_reference,
            image_lock_fingerprint=fingerprint,
            allowed=True,
        )
        negative_manifest = _network_policy_probe_manifest(
            cache_node_name='dsb-cache',
            image_reference=image_reference,
            image_lock_fingerprint=fingerprint,
        )
        positive_command, _ = _network_policy_positive_control_command(
            manifest_sha256=hashlib.sha256(
                positive_manifest.encode('utf-8')
            ).hexdigest(),
            cache_node_name='dsb-cache',
            image_reference=image_reference,
            image_digest=image_digest,
        )
        negative_command, _ = _network_policy_probe_command(
            manifest_sha256=hashlib.sha256(
                negative_manifest.encode('utf-8')
            ).hexdigest(),
            cache_node_name='dsb-cache',
            image_reference=image_reference,
            image_digest=image_digest,
        )
        negative_cleanup, _ = _network_policy_probe_cleanup_command(
            cache_node_name='dsb-cache',
            image_reference=image_reference,
            image_lock_fingerprint=fingerprint,
        )
        positive_cleanup, _ = _network_policy_probe_cleanup_command(
            cache_node_name='dsb-cache',
            image_reference=image_reference,
            image_lock_fingerprint=fingerprint,
            allowed=True,
        )

        for command in (
            positive_command,
            negative_command,
            negative_cleanup,
            positive_cleanup,
        ):
            with self.subTest(command=command[:80]):
                subprocess.run(
                    ['bash', '-n', '-c', command],
                    check=True,
                    capture_output=True,
                    text=True,
                )

    def test_foreign_probe_name_collision_refuses_without_delete(self):
        job, lock = self.ready_workload_job()
        execute = NetworkQualificationExecutor(initial_probe='foreign')

        with self.assertRaisesRegex(
            RuntimeError,
            'Refusing to delete a foreign network qualification probe Pod',
        ):
            qualify_distributed_network_paths(
                job,
                lock,
                execute=execute,
            )

        self.assertEqual(execute.cleanup_count, 4)
        self.assertEqual(execute.delete_count, 0)
        self.assertEqual(execute.probe_state, 'foreign')
        self.assertTrue(all(
            'DISTRIBUTED_DSB_DEFAULT_DENY_CLEANUP' in command
            for command, _ in execute.calls
        ))

    def test_identical_probe_contract_dispatches_through_gcp_plan(self):
        job, lock = self.ready_workload_job(provider='gcp')
        execute = NetworkQualificationExecutor()

        attestation = qualify_distributed_network_paths(
            job,
            lock,
            execute=execute,
        )

        self.assertEqual(attestation['provider'], 'gcp')
        self.assertEqual(
            [path['destination_role'] for path in attestation['forbidden_paths']],
            ['control', 'database', 'cache'],
        )
        tcp_calls = [
            kwargs
            for command, kwargs in execute.calls
            if 'DISTRIBUTED_DSB_NETWORK_PATH' in command
        ]
        self.assertEqual(tcp_calls, [
            {'timeout': 15, 'host_key': 'gcp_dsb_load_generator_public_ip'},
        ] * 4)
        control_calls = [
            kwargs
            for command, kwargs in execute.calls
            if 'DISTRIBUTED_DSB_DEFAULT_DENY' in command
        ]
        self.assertEqual(len(control_calls), 6)
        self.assertTrue(all(
            kwargs['host_key'] == 'gcp_dsb_control_public_ip'
            for kwargs in control_calls
        ))
        positive_calls = [
            kwargs
            for command, kwargs in execute.calls
            if 'DISTRIBUTED_DSB_POSITIVE_CONTROL' in command
        ]
        self.assertEqual(positive_calls, [{
            'timeout': 90,
            'stdin_text': mock.ANY,
            'host_key': 'gcp_dsb_control_public_ip',
        }] * 2)

    def test_requires_exact_workload_ready_before_any_remote_probe(self):
        job = candidate_job()
        prepare_azure_distributed_k3s_candidate(
            job,
            execute=FakeRemoteExecutor(),
        )
        execute = NetworkQualificationExecutor()

        with self.assertRaisesRegex(
            DistributedRuntimeError,
            'must be workload_ready',
        ):
            qualify_distributed_network_paths(
                job,
                candidate_image_lock(),
                execute=execute,
            )

        self.assertEqual(execute.calls, [])

    def test_changed_or_unpinned_lock_fails_before_any_remote_probe(self):
        job, lock = self.ready_workload_job()
        changed = copy.deepcopy(lock)
        changed['platforms']['linux/amd64']['images']['redis'] = (
            'registry.example/deathstarbench/redis:latest'
        )
        execute = NetworkQualificationExecutor()

        with self.assertRaisesRegex(
            DistributedRuntimeError,
            'workload bundle is invalid',
        ):
            qualify_distributed_network_paths(
                job,
                changed,
                execute=execute,
            )

        self.assertEqual(execute.calls, [])

    def test_underlay_failure_still_confirms_probe_pod_absence_in_finally(self):
        job, lock = self.ready_workload_job()
        execute = NetworkQualificationExecutor(fail_on='destination=database')

        with self.assertRaisesRegex(RuntimeError, 'network probe failure'):
            qualify_distributed_network_paths(
                job,
                lock,
                execute=execute,
            )

        cleanup_calls = [
            call for call in execute.calls
            if 'DISTRIBUTED_DSB_DEFAULT_DENY_CLEANUP' in call[0]
        ]
        self.assertEqual(len(cleanup_calls), 4)
        self.assertEqual(execute.calls[-1], cleanup_calls[-1])
        self.assertFalse(any(
            'stdin_text' in kwargs for _, kwargs in execute.calls
        ))

    def test_pod_probe_failure_is_cleaned_and_confirmed_absent(self):
        job, lock = self.ready_workload_job()
        execute = NetworkQualificationExecutor(
            fail_on='DISTRIBUTED_DSB_DEFAULT_DENY pod=',
        )

        with self.assertRaisesRegex(RuntimeError, 'network probe failure'):
            qualify_distributed_network_paths(
                job,
                lock,
                execute=execute,
            )

        self.assertEqual(sum(
            'stdin_text' in kwargs for _, kwargs in execute.calls
        ), 3)
        self.assertIn(
            'DISTRIBUTED_DSB_DEFAULT_DENY_CLEANUP',
            execute.calls[-1][0],
        )
        self.assertEqual(execute.positive_attempt_count, 2)
        self.assertEqual(execute.cleanup_count, 5)

    def test_duplicate_probe_marker_is_rejected_then_cleanup_runs(self):
        job, lock = self.ready_workload_job()
        execute = NetworkQualificationExecutor(
            duplicate_on='destination=control',
        )

        with self.assertRaisesRegex(
            DistributedRuntimeError,
            'one exact marker',
        ):
            qualify_distributed_network_paths(
                job,
                lock,
                execute=execute,
            )

        self.assertEqual(execute.cleanup_count, 4)
        self.assertIn(
            'DISTRIBUTED_DSB_DEFAULT_DENY_CLEANUP',
            execute.calls[-1][0],
        )

    def test_final_absence_proof_is_mandatory(self):
        job, lock = self.ready_workload_job()
        execute = NetworkQualificationExecutor(fail_final_cleanup=True)

        with self.assertRaisesRegex(RuntimeError, 'final cleanup failure'):
            qualify_distributed_network_paths(
                job,
                lock,
                execute=execute,
            )

        self.assertEqual(execute.cleanup_count, 5)
        self.assertIn(
            'DISTRIBUTED_DSB_DEFAULT_DENY_CLEANUP',
            execute.calls[-1][0],
        )


if __name__ == '__main__':
    unittest.main()
