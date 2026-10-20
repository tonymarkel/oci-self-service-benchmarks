"""Read-only startup preflight before existing Azure destructive engine."""
import unittest
from app.providers import azure
from tests import test_azure_deathstarbench_lifecycle as fixture_module
from tests.test_azure_provider import PUBLIC_KEY


class AzureStartupOwnershipTests(unittest.TestCase):
    def setUp(self):
        fixture = fixture_module.AzureDeathStarBenchLifecycleTests()
        self.plan = fixture._distributed_plan()
        self.clients = fixture._distributed_clients()
        self.job = {'id': '123456abcdef', 'resources': {}, 'plan': self.plan}
        azure.provision_distributed_deathstarbench_candidate(self.job, self.plan, public_key=PUBLIC_KEY,
            clients=self.clients, persist=lambda _: None)

    def test_preflight_reads_exact_group_and_children_without_delete(self):
        azure.prepare_distributed_candidate_cleanup(self.job, clients=self.clients)
        self.assertEqual(self.clients['resource_groups'].delete_calls, [])

    def test_foreign_inventory_refuses_before_group_delete(self):
        self.clients['resources'].foreign.append({'id': '/subscriptions/foreign/resourceGroups/other/providers/Any/thing',
            'type': 'Any/thing', 'name': 'foreign'})
        with self.assertRaises((ValueError, RuntimeError)):
            azure.prepare_distributed_candidate_cleanup(self.job, clients=self.clients)
        self.assertEqual(self.clients['resource_groups'].delete_calls, [])
