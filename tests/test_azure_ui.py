import unittest
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / 'app/static/index.html').read_text()
JAVASCRIPT = (ROOT / 'app/static/app.js').read_text()
HISTORY = (ROOT / 'app/static/history.js').read_text()


class ElementIndex(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements = {}

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        element_id = attributes.get('id')
        if element_id:
            self.elements[element_id] = (tag, attributes)


class AzureUiTests(unittest.TestCase):
    def test_plan_has_subscription_and_zone_controls(self):
        parser = ElementIndex()
        parser.feed(INDEX)

        self.assertEqual(parser.elements['azureSubscription'][0], 'input')
        self.assertEqual(parser.elements['azureZone'][0], 'select')
        self.assertIn('hidden', parser.elements['azureSubscriptionField'][1])
        self.assertIn('hidden', parser.elements['azureZoneField'][1])
        self.assertIn('active Azure CLI subscription', INDEX)

    def test_frontend_uses_only_the_provider_scoped_discovery_routes(self):
        self.assertIn('/api/providers/azure/bootstrap', JAVASCRIPT)
        self.assertIn(
            '/api/providers/azure/placement?subscription_id=',
            JAVASCRIPT,
        )
        self.assertIn('/api/providers/azure/vm-sizes', JAVASCRIPT)
        self.assertIn("query.set('subscription_id', azureSubscription)", JAVASCRIPT)
        self.assertIn("query.set('zone', azureZone)", JAVASCRIPT)

    def test_azure_mode_uses_cli_rocky_linux_and_fixed_vm_capacity(self):
        self.assertIn('Runs locally with Azure CLI', JAVASCRIPT)
        self.assertIn('Azure runs use a pinned Rocky Linux 9 image.', JAVASCRIPT)
        self.assertIn('Azure VM Size', JAVASCRIPT)
        self.assertIn('Search VM sizes available in this zone', JAVASCRIPT)
        self.assertIn('connect as benchmark', JAVASCRIPT)
        self.assertIn('const providerFixedCapacity = fixedCapacity || azure;', JAVASCRIPT)
        self.assertIn("$('#ocpus').readOnly = fixedCapacity || azure", JAVASCRIPT)
        self.assertIn("$('#memory').readOnly = fixedCapacity || azure", JAVASCRIPT)
        self.assertIn(
            'Select a valid Azure VM size from the searchable list.',
            JAVASCRIPT,
        )
        self.assertIn("setProviderOnlyElement('#azureSubscriptionField', azure)", JAVASCRIPT)
        self.assertIn("setProviderOnlyElement('#azureZoneField', azure)", JAVASCRIPT)

    def test_azure_copy_explains_storage_network_and_cleanup_contracts(self):
        self.assertIn('Premium SSD v2', INDEX)
        self.assertIn('3,000 IOPS and 125 MiB/s throughput', INDEX)
        self.assertIn("$('#azureStorageHint').hidden = !azure", JAVASCRIPT)
        self.assertIn('private Azure VNet address', JAVASCRIPT)
        self.assertIn('dedicated resource group', JAVASCRIPT)
        self.assertIn('SSH TCP/22 is open to 0.0.0.0/0', JAVASCRIPT)

    def test_payload_and_rerun_preserve_subscription_and_zone(self):
        self.assertIn(
            "azure_subscription_id: azure ? $('#azureSubscription').value.trim() : null",
            JAVASCRIPT,
        )
        self.assertIn(
            "azure_zone: azure ? $('#azureZone').value : null",
            JAVASCRIPT,
        )
        self.assertIn(
            "$('#azureSubscription').value = plan.azure_subscription_id || ''",
            JAVASCRIPT,
        )
        self.assertIn(
            "$('#azureZone').value = plan.azure_zone || ''",
            JAVASCRIPT,
        )
        self.assertIn("} else if (provider === 'azure') {", JAVASCRIPT)

    def test_provider_capabilities_remain_registry_driven(self):
        for capability in (
            'benchmarks',
            'llm_benchmarks',
            'sysbench_workloads',
            'iperf3_protocols',
        ):
            self.assertIn(
                f"providerCapabilitySet('{capability}')",
                JAVASCRIPT,
            )
        self.assertNotIn('azureSupportedBenchmarks', JAVASCRIPT)
        self.assertNotIn('azureSupportedIperf3Protocols', JAVASCRIPT)

    def test_history_labels_azure_fields_and_capacity(self):
        self.assertIn("azure: 'Azure'", HISTORY)
        self.assertIn("runField(run, 'azure_subscription_id')", HISTORY)
        self.assertIn("runField(run, 'azure_zone')", HISTORY)
        self.assertIn("provider === 'azure'", HISTORY)
        self.assertIn("? 'VM size'", HISTORY)
        self.assertIn("provider === 'aws' || provider === 'gcp' || provider === 'azure'", HISTORY)

    def test_subscription_identity_is_not_mistaken_for_a_resource(self):
        for metadata_key in (
            'azure_image_id',
            'azure_peer_image_id',
            'azure_subscription_id',
            'azure_tenant_id',
        ):
            self.assertIn(f"'{metadata_key}'", JAVASCRIPT)
        self.assertIn("azure: 'benchmark'", JAVASCRIPT)


if __name__ == '__main__':
    unittest.main()
