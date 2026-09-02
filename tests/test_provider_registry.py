import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app import main
from app.providers.registry import (
    ProviderOperationUnavailableError,
    UnsupportedProviderError,
    dispatch_provider_operation,
    provider_adapter,
    provider_id,
    providers,
)


class ProviderRegistryTests(unittest.TestCase):
    def test_legacy_plan_without_provider_resolves_to_oci(self):
        self.assertEqual(provider_id({}), 'oci')
        self.assertEqual(provider_id(SimpleNamespace()), 'oci')
        self.assertEqual(provider_adapter({}).short_name, 'OCI')

    def test_explicit_unknown_or_empty_provider_fails_closed(self):
        for plan in (
            {'provider': ''},
            {'provider': None},
            SimpleNamespace(provider='alicloud'),
        ):
            with self.subTest(plan=plan):
                with self.assertRaisesRegex(
                    UnsupportedProviderError,
                    'Unsupported cloud provider',
                ):
                    provider_adapter(plan)

    def test_registered_provider_without_operation_handler_is_rejected(self):
        with self.assertRaisesRegex(
            ProviderOperationUnavailableError,
            'does not implement the benchmark operation',
        ):
            dispatch_provider_operation(
                'aws',
                'benchmark',
                {'oci': lambda: 'must-not-run'},
            )

    def test_provider_api_is_backwards_compatible_and_exposes_capabilities(self):
        response = main.providers()
        items = response['items']

        self.assertEqual(
            [(item['id'], item['name']) for item in items],
            [
                ('oci', 'Oracle Cloud Infrastructure'),
                ('aws', 'Amazon Web Services'),
                ('gcp', 'Google Cloud'),
                ('azure', 'Microsoft Azure'),
            ],
        )
        aws = items[1]
        self.assertEqual(aws['short_name'], 'AWS')
        self.assertEqual(aws['ssh_user'], 'ec2-user')
        self.assertEqual(
            aws['capabilities']['benchmarks'],
            [
                'deathstarbench',
                'apachebench',
                'sysbench',
                'stream',
                'phoronix',
                'iperf3',
                'fio',
            ],
        )
        self.assertEqual(
            aws['capabilities']['iperf3_protocols'],
            ['tcp', 'udp', 'sctp'],
        )
        self.assertTrue(aws['capabilities']['additional_volume'])
        self.assertEqual(
            aws['capabilities']['llm_benchmarks'],
            ['llama_bench'],
        )
        gcp = items[2]
        self.assertEqual(gcp['short_name'], 'GCP')
        self.assertEqual(gcp['ssh_user'], 'benchmark')
        self.assertEqual(
            gcp['capabilities']['benchmarks'],
            aws['capabilities']['benchmarks'],
        )
        self.assertEqual(
            gcp['capabilities']['iperf3_protocols'],
            ['tcp', 'udp', 'sctp'],
        )
        self.assertEqual(
            gcp['capabilities']['llm_benchmarks'],
            ['llama_bench'],
        )
        self.assertTrue(gcp['capabilities']['additional_volume'])
        azure = items[3]
        self.assertEqual(azure['short_name'], 'Azure')
        self.assertEqual(azure['ssh_user'], 'benchmark')
        self.assertEqual(
            azure['capabilities']['benchmarks'],
            aws['capabilities']['benchmarks'],
        )
        self.assertEqual(
            azure['capabilities']['iperf3_protocols'],
            ['tcp', 'udp'],
        )
        self.assertEqual(
            azure['capabilities']['llm_benchmarks'],
            ['llama_bench'],
        )
        self.assertTrue(azure['capabilities']['additional_volume'])

    def test_frontend_loads_provider_options_and_support_from_the_registry(self):
        javascript = (
            Path(__file__).resolve().parents[1]
            / 'app/static/app.js'
        ).read_text()

        self.assertIn("api('/api/providers')", javascript)
        self.assertIn('renderProviders(providerCatalog.items || [])', javascript)
        for capability in (
            'benchmarks',
            'llm_benchmarks',
            'sysbench_workloads',
            'iperf3_protocols',
        ):
            self.assertIn(
                f"providerCapabilitySet('{capability}')",
                javascript,
            )
        self.assertNotIn('awsSupportedBenchmarks', javascript)

    def test_registry_provider_ids_are_unique(self):
        registered = providers()
        self.assertEqual(
            len({provider.id for provider in registered}),
            len(registered),
        )


class ProviderDispatchTests(unittest.TestCase):
    def test_legacy_provision_dispatches_explicitly_to_oci(self):
        plan = SimpleNamespace()
        job = {'resources': {}}

        with (
            patch.object(main, 'provision_oci', return_value='oci') as oci,
            patch.object(main.aws_provider, 'provision') as aws,
        ):
            result = main.provision(job, plan)

        self.assertEqual(result, 'oci')
        oci.assert_called_once_with(job, plan)
        aws.assert_not_called()

    def test_unknown_provider_cannot_fall_through_to_oci_operation(self):
        plan = SimpleNamespace(provider='alicloud')
        job = {'plan': {'provider': 'alicloud'}, 'resources': {}}

        with (
            patch.object(main, 'provision_oci') as provision_oci,
            patch.object(main.aws_provider, 'provision') as provision_aws,
            patch.object(main.azure_provider, 'provision') as provision_azure,
            patch.object(main, 'run_oci_benchmarks') as run_oci,
            patch.object(main, 'run_aws_benchmarks') as run_aws,
            patch.object(main, 'run_azure_benchmarks') as run_azure,
            patch.object(main, 'destroy_oci_resources') as destroy_oci,
            patch.object(main.aws_provider, 'destroy_resources') as destroy_aws,
            patch.object(
                main.azure_provider,
                'destroy_resources',
            ) as destroy_azure,
        ):
            for operation in (
                lambda: main.provision(job, plan),
                lambda: main.run_benchmarks(job, plan),
                lambda: main.destroy_resources(job),
            ):
                with self.assertRaisesRegex(
                    UnsupportedProviderError,
                    "'alicloud'",
                ):
                    operation()

        for handler in (
            provision_oci,
            provision_aws,
            provision_azure,
            run_oci,
            run_aws,
            run_azure,
            destroy_oci,
            destroy_aws,
            destroy_azure,
        ):
            handler.assert_not_called()


if __name__ == '__main__':
    unittest.main()
