import copy
import unittest
from dataclasses import replace
from types import SimpleNamespace

from app.deathstarbench_topology import (
    BENCHMARK_INGRESS_CHANNEL,
    CAPACITY_CONTRACTS,
    CACHE_CAPACITY_CLASS,
    CACHE_DATA_CHANNEL,
    CONTROL_CAPACITY_CLASS,
    DATABASE_CAPACITY_CLASS,
    DATABASE_DATA_CHANNEL,
    DATABASE_STORAGE_CLASS,
    DISTRIBUTED_TIERED_NODE_KEYS,
    K3S_CONTROL_PLANE_CHANNEL,
    LOAD_GENERATOR_CAPACITY_CLASS,
    SINGLE_HOST_NODE_KEYS,
    TopologyManifestError,
    build_deathstarbench_topology,
    build_topology_manifest,
)


class DeathStarBenchTopologyManifestTests(unittest.TestCase):
    def test_single_host_plan_matches_current_runner_and_load_generator(self):
        manifest = build_topology_manifest(
            'single_host_v1',
            'podman_compose_v1',
            selected_shape='c8id.8xlarge',
            selected_architecture='x86_64',
        )

        self.assertEqual(
            tuple(node.key for node in manifest.nodes),
            SINGLE_HOST_NODE_KEYS,
        )
        self.assertEqual(
            tuple(node.role for node in manifest.nodes),
            ('runner', 'load_generator'),
        )
        self.assertEqual(manifest.nodes[0].selected_shape, 'c8id.8xlarge')
        self.assertIsNone(manifest.nodes[1].selected_shape)
        self.assertEqual(
            manifest.nodes[1].capacity.key,
            LOAD_GENERATOR_CAPACITY_CLASS,
        )
        self.assertTrue(
            manifest.network_policy.allows(
                'load-generator',
                'runner',
                BENCHMARK_INGRESS_CHANNEL,
            )
        )
        self.assertFalse(manifest.network_policy.allows('runner', 'load-generator'))

        inventory = manifest.planned_inventory('aws')
        self.assertEqual(inventory.provider, 'aws')
        self.assertEqual(inventory.node('runner').shape, 'c8id.8xlarge')
        self.assertEqual(
            inventory.node('load-generator').lifecycle_status,
            'planned',
        )

    def test_distributed_plan_has_exact_deterministic_roles_and_order(self):
        manifest = build_topology_manifest(
            'distributed_tiered_v1',
            'k3s_v1',
            selected_shape='VM.Standard.A2.Flex',
            selected_architecture='aarch64',
        )

        self.assertEqual(
            tuple(node.key for node in manifest.nodes),
            DISTRIBUTED_TIERED_NODE_KEYS,
        )
        self.assertEqual(
            tuple(node.role for node in manifest.nodes),
            (
                'control',
                'database',
                'cache',
                'application',
                'load_generator',
            ),
        )
        application = manifest.nodes[3]
        self.assertEqual(application.selected_shape, 'VM.Standard.A2.Flex')
        self.assertEqual(application.architecture, 'arm64')
        self.assertEqual(
            manifest.creation_order,
            ('control', 'database', 'cache', 'application', 'load-generator'),
        )
        self.assertEqual(
            manifest.deletion_order,
            ('load-generator', 'application', 'cache', 'database', 'control'),
        )

        expected_support_classes = {
            'load-generator': LOAD_GENERATOR_CAPACITY_CLASS,
            'control': CONTROL_CAPACITY_CLASS,
            'cache': CACHE_CAPACITY_CLASS,
            'database': DATABASE_CAPACITY_CLASS,
        }
        for node in manifest.nodes:
            if node.key == 'application':
                continue
            self.assertIsNone(node.selected_shape)
            self.assertEqual(node.capacity.key, expected_support_classes[node.key])
            self.assertTrue(node.capacity.key.endswith('_v1'))

    def test_database_uses_normalized_persistent_storage_contract(self):
        manifest = build_topology_manifest(
            'distributed_tiered_v1',
            'k3s_v1',
        )
        database = next(node for node in manifest.nodes if node.key == 'database')

        self.assertEqual(len(database.storage), 1)
        storage = database.storage[0]
        self.assertEqual(storage.capacity_class, DATABASE_STORAGE_CLASS)
        self.assertEqual(storage.size_gib, 100)
        self.assertEqual(storage.minimum_iops, 3000)
        self.assertEqual(storage.minimum_throughput_mibps, 125)
        self.assertEqual(storage.filesystem, 'xfs')

        inventory_storage = manifest.planned_inventory().node('database').storage[0]
        self.assertEqual(inventory_storage.kind, 'persistent_block_volume')
        self.assertFalse(inventory_storage.ephemeral)
        self.assertEqual(inventory_storage.lifecycle_status, 'planned')
        self.assertIsNone(inventory_storage.provider_resource_id)

    def test_reachability_matrix_is_closed_and_role_scoped(self):
        manifest = build_topology_manifest(
            'distributed_tiered_v1',
            'k3s_v1',
        )
        matrix = manifest.reachability_matrix

        self.assertEqual(tuple(matrix), DISTRIBUTED_TIERED_NODE_KEYS)
        self.assertEqual(
            tuple(matrix['load-generator']),
            DISTRIBUTED_TIERED_NODE_KEYS,
        )
        self.assertIn(
            BENCHMARK_INGRESS_CHANNEL,
            matrix['load-generator']['application'],
        )
        self.assertEqual(matrix['load-generator']['database'], ())
        self.assertIn(CACHE_DATA_CHANNEL, matrix['application']['cache'])
        self.assertIn(DATABASE_DATA_CHANNEL, matrix['application']['database'])
        self.assertIn(
            K3S_CONTROL_PLANE_CHANNEL,
            matrix['database']['control'],
        )
        self.assertNotIn(
            'k3s_node_metrics_v1',
            matrix['application']['database'],
        )
        channels = {
            channel.key: channel
            for channel in manifest.network_policy.channels
        }
        self.assertEqual(channels[K3S_CONTROL_PLANE_CHANNEL].ports, (6443,))
        self.assertEqual(channels[BENCHMARK_INGRESS_CHANNEL].ports, (8080,))
        self.assertEqual(matrix['application']['load-generator'], ())

    def test_fingerprint_and_serialization_are_deterministic_and_strict(self):
        first = build_topology_manifest(
            'distributed_tiered_v1',
            'k3s_v1',
            selected_shape='c4a-standard-8',
            selected_architecture='arm64',
        )
        second = build_topology_manifest(
            'distributed_tiered_v1',
            'k3s_v1',
            selected_shape='c4a-standard-8',
            selected_architecture='aarch64',
        )

        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertRegex(first.fingerprint, r'^sha256:[0-9a-f]{64}$')
        self.assertEqual(first.canonical_json(), second.canonical_json())
        self.assertEqual(type(first).from_dict(first.as_dict()), first)

        changed = copy.deepcopy(first.as_dict())
        changed['nodes'][3]['selected_shape'] = 'c4a-standard-16'
        with self.assertRaisesRegex(TopologyManifestError, 'fingerprint'):
            type(first).from_dict(changed)

        unknown = copy.deepcopy(first.as_dict())
        unknown['provider_hint'] = 'gcp'
        with self.assertRaisesRegex(TopologyManifestError, 'unknown'):
            type(first).from_dict(unknown)

    def test_released_contract_fingerprints_are_golden(self):
        compact = build_topology_manifest(
            'single_host_v1',
            'podman_compose_v1',
            selected_shape='c8id.8xlarge',
            selected_architecture='x86_64',
        )
        distributed = build_topology_manifest(
            'distributed_tiered_v1',
            'k3s_v1',
            selected_shape='c4a-standard-8',
            selected_architecture='arm64',
        )

        self.assertEqual(
            compact.fingerprint,
            'sha256:5aef0a2bff832828475f562a900c3c1e498e1332c595a6c53c2ab5d52a8fca33',
        )
        self.assertEqual(
            distributed.fingerprint,
            'sha256:1687ad84d1fe7e8748727604f5ce7607d820e2cd3d3b675b603f724b61a14a1e',
        )

    def test_plan_builder_accepts_models_and_persisted_mappings(self):
        object_plan = SimpleNamespace(
            shape='Standard_D8ps_v6',
            architecture='aarch64',
            deathstarbench=SimpleNamespace(
                topology_id='distributed_tiered_v1',
                runtime_id='k3s_v1',
            ),
        )
        mapping_plan = {
            'shape': 'Standard_D8ps_v6',
            'architecture': 'arm64',
            'deathstarbench': {
                'topology_id': 'distributed_tiered_v1',
                'runtime_id': 'k3s_v1',
            },
        }

        self.assertEqual(
            build_deathstarbench_topology(object_plan),
            build_deathstarbench_topology(mapping_plan),
        )

    def test_support_shapes_and_node_order_cannot_be_silently_changed(self):
        manifest = build_topology_manifest(
            'distributed_tiered_v1',
            'k3s_v1',
        )
        nodes = list(manifest.nodes)
        nodes[1] = replace(nodes[1], selected_shape='Standard_D2as_v7')
        with self.assertRaisesRegex(TopologyManifestError, 'abstract capacity'):
            replace(manifest, nodes=tuple(nodes))

        with self.assertRaisesRegex(TopologyManifestError, 'keys or order'):
            replace(manifest, nodes=tuple(reversed(manifest.nodes)))

        with self.assertRaises(TypeError):
            CAPACITY_CONTRACTS['unversioned'] = manifest.nodes[0].capacity

    def test_unknown_topology_runtime_pairs_fail_closed(self):
        with self.assertRaisesRegex(TopologyManifestError, 'Unsupported'):
            build_topology_manifest('distributed_tiered_v2', 'k3s_v1')
        with self.assertRaisesRegex(TopologyManifestError, 'Unsupported'):
            build_topology_manifest('distributed_tiered_v1', 'k3s_stable')

    def test_explicit_invalid_persisted_ids_do_not_default_to_compact(self):
        for topology_id, runtime_id in (
            ('', 'podman_compose_v1'),
            (False, 'podman_compose_v1'),
            ('single_host_v1', 0),
        ):
            with self.subTest(
                topology_id=topology_id,
                runtime_id=runtime_id,
            ):
                with self.assertRaises(TopologyManifestError):
                    build_deathstarbench_topology({
                        'deathstarbench': {
                            'topology_id': topology_id,
                            'runtime_id': runtime_id,
                        },
                    })


if __name__ == '__main__':
    unittest.main()
