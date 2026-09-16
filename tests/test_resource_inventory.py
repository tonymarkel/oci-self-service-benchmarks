import copy
import unittest
from types import SimpleNamespace

from app.resource_inventory import (
    ROLE_NODE_INVENTORY_KEY,
    ResourceInventoryError,
    RoleNode,
    RoleNodeInventory,
    StorageResource,
    inventory_from_legacy_resources,
    load_role_node_inventory,
    persist_role_node_inventory,
    upsert_role_node,
)


class RoleNodeInventoryTests(unittest.TestCase):
    def test_serialization_is_canonical_and_round_trips_idempotently(self):
        data = StorageResource(
            key='data',
            kind='block_volume',
            provider_resource_id='vol-2',
            size_gb=500,
            provisioned_iops=6000,
            provisioned_throughput_mibps=250,
            ephemeral=False,
            lifecycle_status='running',
        )
        boot = StorageResource(
            key='boot',
            kind='boot_volume',
            provider_resource_id='vol-1',
            size_gb=100,
            ephemeral=False,
        )
        application = RoleNode(
            key='application-1',
            role='application',
            provider_resource_id='i-app',
            provider_resource_name='benchmark-app-1',
            public_addresses=('2001:0db8::1', '198.51.100.20'),
            private_addresses=('10.0.0.20', '10.0.0.20'),
            zone='us-east-1a',
            shape='m8i.2xlarge',
            architecture='x86_64',
            capacity_class='application_8vcpu_32gib_v1',
            storage=(data, boot),
            lifecycle_status='running',
        )
        loadgen = RoleNode(
            key='load-generator',
            role='load_generator',
            provider_resource_id='i-loadgen',
            private_addresses=('10.0.0.10',),
            lifecycle_status='creating',
        )

        inventory = RoleNodeInventory(
            provider='aws',
            topology_fingerprint='sha256:' + ('a' * 64),
            nodes=(loadgen, application),
        )
        reverse_order = RoleNodeInventory(
            provider='aws',
            topology_fingerprint='sha256:' + ('a' * 64),
            nodes=(application, loadgen),
        )

        self.assertEqual(
            [node.key for node in inventory.nodes],
            ['application-1', 'load-generator'],
        )
        self.assertEqual(
            inventory.node('application-1').public_addresses,
            ('198.51.100.20', '2001:db8::1'),
        )
        self.assertEqual(
            [item.key for item in inventory.node('application-1').storage],
            ['boot', 'data'],
        )
        self.assertEqual(inventory.canonical_json(), reverse_order.canonical_json())

        serialized = inventory.as_dict()
        round_trip = RoleNodeInventory.from_dict(serialized)
        self.assertEqual(round_trip, inventory)
        self.assertEqual(round_trip.as_dict(), serialized)
        self.assertEqual(round_trip.canonical_json(), inventory.canonical_json())

        # Callers receive ordinary JSON containers, not aliases into the
        # immutable inventory object.
        serialized['nodes'][0]['private_addresses'].append('10.0.0.99')
        self.assertEqual(
            inventory.node('application-1').private_addresses,
            ('10.0.0.20',),
        )

    def test_upsert_is_copy_on_write_idempotent_and_preserves_identity(self):
        planned_storage = StorageResource(
            key='data',
            kind='block_volume',
            size_gb=100,
            lifecycle_status='planned',
        )
        planned = RoleNode(
            key='database-1',
            role='database',
            shape='r8i.large',
            capacity_class='database_2vcpu_16gib_v1',
            storage=(planned_storage,),
            lifecycle_status='planned',
        )
        created_storage = StorageResource(
            key='data',
            kind='block_volume',
            provider_resource_id='vol-database',
            lifecycle_status='running',
        )
        running = RoleNode(
            key='database-1',
            role='database',
            provider_resource_id='i-database',
            private_addresses=('10.0.2.10',),
            shape='r8i.large',
            capacity_class='database_2vcpu_16gib_v1',
            storage=(created_storage,),
            lifecycle_status='running',
        )
        original = RoleNodeInventory(
            provider='aws',
            topology_fingerprint='sha256:' + ('b' * 64),
            nodes=(planned,),
        )

        updated = original.upsert(running)
        sparse_update = RoleNode(
            key='database-1',
            role='database',
            lifecycle_status='stopping',
        )
        stopping = updated.upsert(sparse_update)

        self.assertEqual(original.node('database-1'), planned)
        self.assertEqual(
            updated.node('database-1').provider_resource_id,
            'i-database',
        )
        self.assertEqual(
            updated.node('database-1').storage_resource('data').size_gb,
            100,
        )
        self.assertEqual(
            updated.node('database-1').storage_resource('data').provider_resource_id,
            'vol-database',
        )
        self.assertEqual(stopping.node('database-1').lifecycle_status, 'stopping')
        self.assertEqual(
            stopping.node('database-1').provider_resource_id,
            'i-database',
        )
        self.assertEqual(stopping.topology_fingerprint, original.topology_fingerprint)
        self.assertEqual(updated.upsert(running), updated)

    def test_upsert_rejects_role_or_provider_identity_replacement(self):
        original = RoleNodeInventory(provider='aws', nodes=(RoleNode(
            key='database-1',
            role='database',
            provider_resource_id='i-original',
            provider_resource_name='database-original',
            lifecycle_status='running',
        ),))

        for update, message in (
            (
                RoleNode(key='database-1', role='cache'),
                'role cannot be replaced',
            ),
            (
                RoleNode(
                    key='database-1',
                    role='database',
                    provider_resource_id='i-replacement',
                ),
                'provider_resource_id cannot be replaced',
            ),
            (
                RoleNode(
                    key='database-1',
                    role='database',
                    provider_resource_name='database-replacement',
                ),
                'provider_resource_name cannot be replaced',
            ),
        ):
            with self.subTest(update=update):
                with self.assertRaisesRegex(ResourceInventoryError, message):
                    original.upsert(update)

    def test_storage_upsert_preserves_resources_and_rejects_replacement(self):
        data = StorageResource(
            key='data',
            kind='block_volume',
            provider_resource_id='vol-data',
            size_gb=100,
            lifecycle_status='running',
        )
        logs = StorageResource(
            key='logs',
            kind='block_volume',
            provider_resource_id='vol-logs',
            lifecycle_status='running',
        )
        node = RoleNode(
            key='database-1',
            role='database',
            storage=(data, logs),
            lifecycle_status='running',
        )
        update = RoleNode(
            key='database-1',
            role='database',
            storage=(StorageResource(
                key='data',
                kind='block_volume',
                provider_resource_id='vol-data',
                device='/dev/nvme1n1',
                lifecycle_status='running',
            ),),
            lifecycle_status='running',
        )

        merged = RoleNodeInventory(nodes=(node,)).upsert(update).node('database-1')
        self.assertEqual([item.key for item in merged.storage], ['data', 'logs'])
        self.assertEqual(merged.storage_resource('data').size_gb, 100)
        self.assertEqual(merged.storage_resource('data').device, '/dev/nvme1n1')

        for replacement, message in (
            (
                StorageResource(
                    key='data',
                    kind='block_volume',
                    provider_resource_id='vol-replacement',
                ),
                'provider_resource_id cannot be replaced',
            ),
            (
                StorageResource(
                    key='data',
                    kind='block_volume',
                    provider_resource_id='vol-data',
                    device='/dev/nvme2n1',
                ),
                'device cannot be replaced',
            ),
            (
                StorageResource(
                    key='data',
                    kind='local_nvme',
                    provider_resource_id='vol-data',
                ),
                'kind cannot be replaced',
            ),
        ):
            with self.subTest(replacement=replacement):
                with self.assertRaisesRegex(ResourceInventoryError, message):
                    merged.upsert_storage(replacement)

    def test_remove_requires_never_created_planned_or_confirmed_deleted(self):
        planned = RoleNode(
            key='planned',
            role='application',
            storage=(StorageResource(
                key='data',
                kind='block_volume',
                lifecycle_status='planned',
            ),),
            lifecycle_status='planned',
        )
        running = RoleNode(
            key='running',
            role='application',
            provider_resource_id='i-running',
            lifecycle_status='running',
        )
        deleted = RoleNode(
            key='deleted',
            role='application',
            provider_resource_id='i-deleted',
            storage=(StorageResource(
                key='data',
                kind='block_volume',
                provider_resource_id='vol-deleted',
                lifecycle_status='deleted',
            ),),
            lifecycle_status='deleted',
        )
        inventory = RoleNodeInventory(nodes=(planned, running, deleted))

        without_planned = inventory.without('planned')
        without_deleted = without_planned.without('deleted')
        self.assertIsNone(without_planned.node('planned'))
        self.assertIsNone(without_deleted.node('deleted'))
        self.assertEqual(planned.without_storage('data').storage, ())
        self.assertEqual(deleted.without_storage('data').storage, ())
        self.assertEqual(without_deleted.without('missing'), without_deleted)
        with self.assertRaisesRegex(ResourceInventoryError, 'confirmed deleted'):
            inventory.without('running')

        attached = StorageResource(
            key='attached',
            kind='block_volume',
            provider_resource_id='vol-attached',
            lifecycle_status='running',
        )
        node = RoleNode(
            key='database-1',
            role='database',
            storage=(attached,),
            lifecycle_status='deleted',
        )
        with self.assertRaisesRegex(ResourceInventoryError, 'confirmed deleted'):
            node.without_storage('attached')
        with self.assertRaisesRegex(ResourceInventoryError, 'confirmed deleted'):
            RoleNodeInventory(nodes=(node,)).without('database-1')

    def test_validation_rejects_ambiguous_or_unsafe_inventory(self):
        node = RoleNode(key='cache-1', role='cache')
        with self.assertRaisesRegex(ResourceInventoryError, 'keys must be unique'):
            RoleNodeInventory(nodes=(node, node))
        with self.assertRaisesRegex(ResourceInventoryError, 'invalid IP address'):
            RoleNode(
                key='cache-1',
                role='cache',
                private_addresses=('database.internal',),
            )
        with self.assertRaisesRegex(ResourceInventoryError, 'mount_point'):
            StorageResource(
                key='data',
                kind='block_volume',
                mount_point='/data/../tmp',
            )
        with self.assertRaisesRegex(ResourceInventoryError, 'positive integer'):
            StorageResource(
                key='data',
                kind='block_volume',
                provisioned_iops=1.5,
            )
        with self.assertRaisesRegex(ResourceInventoryError, 'must be one of'):
            RoleNode(
                key='cache-1',
                role='cache',
                lifecycle_status='provider_native_ready',
            )
        with self.assertRaisesRegex(ResourceInventoryError, 'capacity_class'):
            RoleNode(
                key='cache-1',
                role='cache',
                capacity_class='Cache Class',
            )
        with self.assertRaisesRegex(ResourceInventoryError, 'topology_fingerprint'):
            RoleNodeInventory(topology_fingerprint='sha256:not-a-digest')

    def test_persisted_schema_fails_closed_instead_of_guessing_from_legacy(self):
        legacy = {
            'provider': 'aws',
            'instance_id': 'i-must-not-be-imported',
            ROLE_NODE_INVENTORY_KEY: {
                'schema_version': 2,
                'provider': 'aws',
                'topology_fingerprint': None,
                'nodes': [],
            },
        }
        with self.assertRaisesRegex(ResourceInventoryError, 'schema version'):
            load_role_node_inventory(legacy)

        inventory = RoleNodeInventory(provider='aws')
        serialized = inventory.as_dict()
        serialized['unrecognized_cleanup_contract'] = []
        with self.assertRaisesRegex(ResourceInventoryError, 'unknown'):
            RoleNodeInventory.from_dict(serialized)

        missing_fingerprint_field = inventory.as_dict()
        del missing_fingerprint_field['topology_fingerprint']
        with self.assertRaisesRegex(ResourceInventoryError, 'missing'):
            RoleNodeInventory.from_dict(missing_fingerprint_field)

        node_document = RoleNode(
            key='cache-1',
            role='cache',
        ).as_dict()
        del node_document['capacity_class']
        with self.assertRaisesRegex(ResourceInventoryError, 'missing'):
            RoleNode.from_dict(node_document)

        conflicting = {
            'provider': 'gcp',
            ROLE_NODE_INVENTORY_KEY: inventory.as_dict(),
        }
        with self.assertRaisesRegex(ResourceInventoryError, 'provider conflicts'):
            load_role_node_inventory(conflicting)

    def test_helpers_persist_only_validated_nested_contract(self):
        resources = {'provider': 'aws'}
        inventory = RoleNodeInventory(provider='aws')

        persisted = persist_role_node_inventory(resources, inventory)
        self.assertIs(resources[ROLE_NODE_INVENTORY_KEY], persisted)
        self.assertEqual(load_role_node_inventory(resources), inventory)

        node = RoleNode(
            key='frontend-1',
            role='frontend',
            lifecycle_status='planned',
        )
        updated = upsert_role_node(resources, node)
        self.assertEqual(updated.node('frontend-1'), node)
        self.assertEqual(load_role_node_inventory(resources), updated)


class LegacyRoleNodeInventoryTests(unittest.TestCase):
    def test_imports_aws_runner_loadgen_and_selected_storage_without_mutation(self):
        resources = {
            'provider': 'aws',
            'instance_id': 'i-runner',
            'aws_instance_id': 'i-runner',
            'public_ip': '203.0.113.10',
            'private_ip': '10.42.0.10',
            'instance_type': 'c8id.8xlarge',
            'architecture': 'x86_64',
            'availability_zone': 'us-east-1a',
            'loadgen_instance_id': 'i-loadgen',
            'loadgen_public_ip': '203.0.113.20',
            'loadgen_private_ip': '10.42.0.20',
            'loadgen_shape': 'm7i.large',
            'loadgen_architecture': 'x86_64',
            'aws_loadgen_availability_zone': 'us-east-1a',
            'aws_data_volume_id': 'vol-data',
            'aws_data_volume_size_gb': 1024,
            'aws_data_volume_iops': 6000,
            'aws_data_volume_throughput_mibps': 250,
            'benchmark_storage_target': {
                'storage_target_kind': 'attached_block_volume',
                'storage_target_capacity_bytes': 1024**4,
                'storage_target_model': 'Amazon Elastic Block Store',
                'storage_target_mount_point': '/data',
                'storage_target_filesystem': 'xfs',
            },
        }
        original = copy.deepcopy(resources)

        inventory = inventory_from_legacy_resources(resources)

        self.assertEqual(resources, original)
        self.assertEqual(inventory.provider, 'aws')
        runner = inventory.node('runner')
        self.assertEqual(runner.role, 'runner')
        self.assertEqual(runner.provider_resource_id, 'i-runner')
        self.assertEqual(runner.public_addresses, ('203.0.113.10',))
        self.assertEqual(runner.private_addresses, ('10.42.0.10',))
        self.assertEqual(runner.shape, 'c8id.8xlarge')
        self.assertEqual(runner.zone, 'us-east-1a')
        self.assertEqual(len(runner.storage), 1)
        storage = runner.storage[0]
        self.assertEqual(storage.provider_resource_id, 'vol-data')
        self.assertEqual(storage.size_gb, 1024)
        self.assertEqual(storage.provisioned_iops, 6000)
        self.assertFalse(storage.ephemeral)

        loadgen = inventory.node('load-generator')
        self.assertEqual(loadgen.role, 'load_generator')
        self.assertEqual(loadgen.provider_resource_id, 'i-loadgen')
        self.assertEqual(loadgen.shape, 'm7i.large')
        self.assertEqual(loadgen.zone, 'us-east-1a')

    def test_imports_provider_names_and_a_planned_legacy_runner(self):
        resources = {
            'provider': 'gcp',
            'gcp_instance_name': 'benchmark-example-runner',
            'gcp_loadgen_instance_name': 'benchmark-example-loadgen',
            'gcp_machine_type': 'c4a-standard-4',
            'gcp_zone': 'us-central1-a',
            'architecture': 'arm64',
            'loadgen_shape': 'n2-standard-2',
            'loadgen_architecture': 'x86_64',
        }
        inventory = inventory_from_legacy_resources(resources)

        self.assertEqual(
            inventory.node('runner').provider_resource_name,
            'benchmark-example-runner',
        )
        self.assertEqual(inventory.node('runner').shape, 'c4a-standard-4')
        self.assertEqual(inventory.node('runner').zone, 'us-central1-a')
        self.assertEqual(
            inventory.node('load-generator').provider_resource_name,
            'benchmark-example-loadgen',
        )

        planned = inventory_from_legacy_resources(
            {},
            plan=SimpleNamespace(
                provider='oci',
                shape='VM.Standard.E5.Flex',
                availability_domain='AD-1',
            ),
        )
        self.assertEqual(planned.provider, 'oci')
        self.assertEqual(planned.node('runner').lifecycle_status, 'unknown')
        self.assertEqual(planned.node('runner').shape, 'VM.Standard.E5.Flex')
        self.assertEqual(planned.node('runner').zone, 'AD-1')

    def test_local_nvme_descriptor_is_imported_as_ephemeral_storage(self):
        inventory = inventory_from_legacy_resources({
            'provider': 'oci',
            'instance_id': 'ocid1.instance.example',
            'shape': 'BM.DenseIO.E5.128',
            'benchmark_storage_target': {
                'storage_target_kind': 'instance_local_nvme',
                'storage_target_capacity_bytes': 6_800 * 1024**3,
                'storage_target_model': 'OCI NVMe SSD',
                'storage_target_mount_point': '/benchmark-local',
                'storage_target_filesystem': 'xfs',
            },
        })

        storage = inventory.node('runner').storage[0]
        self.assertTrue(storage.ephemeral)
        self.assertIsNone(storage.provider_resource_id)
        self.assertEqual(storage.kind, 'instance_local_nvme')
        self.assertEqual(storage.size_gb, 6800)

    def test_imports_gcp_provisioned_disk_performance_fields(self):
        inventory = inventory_from_legacy_resources({
            'provider': 'gcp',
            'gcp_instance_name': 'benchmark-runner',
            'gcp_machine_type': 'c4-standard-4',
            'gcp_data_disk_name': 'benchmark-data',
            'gcp_data_disk_size_gb': 500,
            'gcp_data_disk_provisioned_iops': 12000,
            'gcp_data_disk_provisioned_throughput_mibps': 500,
            'gcp_data_disk_iops': 1,
            'gcp_data_disk_throughput_mibps': 1,
        })

        storage = inventory.node('runner').storage_resource('benchmark')
        self.assertEqual(storage.provider_resource_name, 'benchmark-data')
        self.assertEqual(storage.provisioned_iops, 12000)
        self.assertEqual(storage.provisioned_throughput_mibps, 500)


if __name__ == '__main__':
    unittest.main()
