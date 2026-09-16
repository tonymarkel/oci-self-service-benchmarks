import unittest

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


if __name__ == '__main__':
    unittest.main()
