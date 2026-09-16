import copy
from dataclasses import replace
import hashlib
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
    RUNTIME_JOURNAL_KEY,
    WORKLOAD_JOURNAL_KEY,
    DistributedRuntimeError,
    azure_k3s_candidate_plan,
    parse_database_volume_attestation,
    parse_database_workload_storage_attestation,
    prepare_azure_distributed_k3s_candidate,
    prepare_azure_distributed_social_network_candidate,
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


def candidate_workload_attestation(job, lock):
    plan = azure_k3s_candidate_plan(job['resources'])
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


if __name__ == '__main__':
    unittest.main()
