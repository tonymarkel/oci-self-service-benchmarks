import unittest
from unittest.mock import patch

from app import main
from app.resource_inventory import (
    ROLE_NODE_INVENTORY_KEY,
    RoleNode,
    RoleNodeInventory,
    StorageResource,
)


def job_with(inventory):
    return {
        'plan': {'provider': 'aws'},
        'resources': {ROLE_NODE_INVENTORY_KEY: inventory.as_dict()},
    }


class RoleNodeInventoryRecoveryTests(unittest.TestCase):
    def test_planned_inventory_without_a_cloud_identity_is_not_recoverable(self):
        job = job_with(RoleNodeInventory(
            provider='aws',
            nodes=(RoleNode(
                key='application-1',
                role='application',
                shape='c8i.2xlarge',
                lifecycle_status='planned',
            ),),
        ))

        self.assertFalse(main.has_recoverable_resources(job))

    def test_ambiguous_or_identified_nodes_are_recoverable(self):
        for node in (
            RoleNode(
                key='application-1',
                role='application',
                lifecycle_status='create_ambiguous',
            ),
            RoleNode(
                key='application-1',
                role='application',
                provider_resource_id='i-123',
                lifecycle_status='unknown',
            ),
            RoleNode(
                key='application-1',
                role='application',
                private_addresses=('10.0.0.10',),
                lifecycle_status='unknown',
            ),
        ):
            with self.subTest(node=node):
                self.assertTrue(main.has_recoverable_resources(job_with(
                    RoleNodeInventory(provider='aws', nodes=(node,))
                )))

    def test_attached_or_ambiguous_storage_is_recoverable(self):
        for storage in (
            StorageResource(
                key='database',
                kind='block_volume',
                provider_resource_id='vol-123',
            ),
            StorageResource(
                key='database',
                kind='block_volume',
                lifecycle_status='create_ambiguous',
            ),
        ):
            with self.subTest(storage=storage):
                inventory = RoleNodeInventory(
                    provider='aws',
                    nodes=(RoleNode(
                        key='database-1',
                        role='database',
                        storage=(storage,),
                        lifecycle_status='planned',
                    ),),
                )
                self.assertTrue(main.has_recoverable_resources(
                    job_with(inventory)
                ))

    def test_deleted_inventory_is_not_recoverable_without_provider_anchors(self):
        inventory = RoleNodeInventory(
            provider='aws',
            nodes=(RoleNode(
                key='application-1',
                role='application',
                lifecycle_status='deleted',
            ),),
        )

        self.assertFalse(main.has_recoverable_resources(job_with(inventory)))

    def test_unknown_inventory_schema_fails_closed(self):
        job = {
            'plan': {'provider': 'aws'},
            'resources': {
                ROLE_NODE_INVENTORY_KEY: {
                    'schema_version': 999,
                    'provider': 'aws',
                    'nodes': [],
                },
            },
        }

        self.assertTrue(main.has_recoverable_resources(job))
        self.assertTrue(main.has_recoverable_resources_for_any_provider(job))

    def test_terminal_distributed_tombstones_override_retained_audit_identity(self):
        cases = (
            (
                'aws',
                {
                    main.aws_provider.AWS_DSB_GRAPH_KEY: {'retained': 'audit'},
                    'aws_distributed_candidate': True,
                    'aws_account_id': '123456789012',
                    'aws_dsb_control_public_ip': '198.51.100.10',
                },
                main.aws_provider,
                'distributed_deathstarbench_candidate_deleted',
            ),
            (
                'oci',
                {
                    main.oci_provider.CONTRACT_KEY: {'retained': 'audit'},
                    'oci_dsb_control_public_ip': '198.51.100.20',
                    'oci_dsb_database_volume_id': 'ocid1.volume.example',
                },
                main.oci_provider,
                'distributed_candidate_is_deleted',
            ),
        )
        for provider, resources, module, predicate in cases:
            with self.subTest(provider=provider), patch.object(
                module,
                predicate,
                return_value=True,
            ) as terminal:
                job = {
                    'status': 'destroyed',
                    'plan': {'provider': provider},
                    'resources': resources,
                }

                self.assertFalse(main.has_recoverable_resources(job))
                self.assertFalse(
                    main.has_recoverable_resources_for_any_provider(job)
                )
                self.assertGreaterEqual(terminal.call_count, 2)

    def test_nonterminal_or_malformed_distributed_graphs_fail_closed(self):
        cases = (
            (
                'aws',
                {main.aws_provider.AWS_DSB_GRAPH_KEY: {}},
                main.aws_provider,
                'distributed_deathstarbench_candidate_deleted',
            ),
            (
                'oci',
                {main.oci_provider.CONTRACT_KEY: {}},
                main.oci_provider,
                'distributed_candidate_is_deleted',
            ),
        )
        for provider, resources, module, predicate in cases:
            with self.subTest(provider=provider), patch.object(
                module,
                predicate,
                return_value=False,
            ):
                job = {
                    'status': 'destroyed',
                    'plan': {'provider': provider},
                    'resources': resources,
                }

                self.assertTrue(main.has_recoverable_resources(job))
                self.assertTrue(
                    main.has_recoverable_resources_for_any_provider(job)
                )

    def test_distributed_candidate_signals_survive_missing_contracts(self):
        cases = (
            (
                'aws',
                {
                    'provider': 'aws',
                    'deathstarbench': {
                        'topology_id': 'distributed_tiered_v1',
                    },
                },
                {},
                main.aws_provider,
                'distributed_deathstarbench_candidate_deleted',
            ),
            (
                'aws',
                {'provider': 'aws'},
                {'aws_distributed_candidate': False},
                main.aws_provider,
                'distributed_deathstarbench_candidate_deleted',
            ),
            (
                'aws',
                {'provider': 'aws'},
                {'aws_dsb_control_public_ip': '198.51.100.10'},
                main.aws_provider,
                'distributed_deathstarbench_candidate_deleted',
            ),
            (
                'oci',
                {
                    'provider': 'oci',
                    'deathstarbench': {'runtime_id': 'k3s_v1'},
                },
                {},
                main.oci_provider,
                'distributed_candidate_is_deleted',
            ),
            (
                'oci',
                {'provider': 'oci'},
                {'oci_distributed_candidate': False},
                main.oci_provider,
                'distributed_candidate_is_deleted',
            ),
            (
                'oci',
                {'provider': 'oci'},
                {'oci_dsb_control_public_ip': '198.51.100.20'},
                main.oci_provider,
                'distributed_candidate_is_deleted',
            ),
        )
        for provider, plan, resources, module, predicate in cases:
            with self.subTest(provider=provider, resources=resources), patch.object(
                module,
                predicate,
                return_value=False,
            ) as terminal:
                job = {
                    'status': 'destroyed',
                    'cleanup_error': None,
                    'plan': plan,
                    'resources': resources,
                }

                self.assertTrue(main.has_recoverable_resources(job))
                self.assertTrue(
                    main.has_recoverable_resources_for_any_provider(job)
                )
                self.assertGreaterEqual(terminal.call_count, 2)

    def test_unknown_provider_prefixed_keys_are_not_candidate_aliases(self):
        cases = (
            ('aws', {'aws_dsb_unknown': 'retained'}),
            ('oci', {'oci_dsb_unknown': 'retained'}),
        )
        for provider, resources in cases:
            with self.subTest(provider=provider):
                job = {
                    'status': 'destroyed',
                    'cleanup_error': None,
                    'plan': {'provider': provider},
                    'resources': resources,
                }

                self.assertFalse(main.has_recoverable_resources(job))
                self.assertFalse(
                    main.has_recoverable_resources_for_any_provider(job)
                )

    def test_compact_oci_identity_is_not_a_distributed_alias_signal(self):
        job = {
            'status': 'destroyed',
            'cleanup_error': None,
            'plan': {'provider': 'oci'},
            'resources': {
                'oci_compartment_id': 'ocid1.compartment.oc1..compact',
                'oci_availability_domain': 'test:US-ASHBURN-AD-1',
            },
        }

        self.assertEqual(
            main._distributed_candidate_providers(job),
            frozenset(),
        )
        self.assertIsNone(
            main._distributed_candidate_local_terminal_state(job)
        )

    def test_distributed_provider_mismatch_and_mixed_graph_fail_closed(self):
        mismatched = {
            'status': 'destroyed',
            'plan': {'provider': 'oci'},
            'resources': {
                main.aws_provider.AWS_DSB_GRAPH_KEY: {},
                'aws_distributed_candidate': True,
            },
        }
        mixed = {
            'status': 'destroyed',
            'plan': {'provider': 'aws'},
            'resources': {
                main.aws_provider.AWS_DSB_GRAPH_KEY: {},
                main.oci_provider.CONTRACT_KEY: {},
            },
        }

        for job in (mismatched, mixed):
            with self.subTest(resources=tuple(job['resources'])):
                self.assertTrue(main.has_recoverable_resources(job))
                self.assertTrue(
                    main.has_recoverable_resources_for_any_provider(job)
                )


if __name__ == '__main__':
    unittest.main()
