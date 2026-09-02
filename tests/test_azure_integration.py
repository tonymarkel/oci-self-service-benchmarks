import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app import main


class AzureDiscoveryRouteTests(unittest.TestCase):
    def test_discovery_routes_are_provider_scoped_and_uncached(self):
        bootstrap = {
            'subscription_id': 'subscription-id',
            'default_region': 'eastus2',
            'regions': ['eastus2'],
        }
        placement = {'availability_zones': [{'name': '1'}]}
        sizes = {'items': [{'vm_size': 'Standard_D8ps_v6'}]}
        with (
            patch.object(
                main.azure_provider,
                'bootstrap',
                return_value=bootstrap,
            ) as boot,
            patch.object(
                main.azure_provider,
                'placement',
                return_value=placement,
            ) as place,
            patch.object(
                main.azure_provider,
                'vm_sizes',
                return_value=sizes,
            ) as vm_sizes,
        ):
            responses = (
                main.azure_bootstrap('subscription-id'),
                main.azure_placement('subscription-id', 'eastus2'),
                main.azure_vm_sizes('subscription-id', 'eastus2', '1'),
            )

        self.assertEqual(
            [response.headers['cache-control'] for response in responses],
            ['no-store', 'no-store', 'no-store'],
        )
        self.assertEqual(json.loads(responses[0].body), bootstrap)
        self.assertEqual(json.loads(responses[1].body), placement)
        self.assertEqual(json.loads(responses[2].body), sizes)
        boot.assert_called_once_with(
            subscription_id='subscription-id',
            default_region='eastus2',
        )
        place.assert_called_once_with(
            subscription_id='subscription-id',
            region='eastus2',
        )
        vm_sizes.assert_called_once_with(
            subscription_id='subscription-id',
            region='eastus2',
            zone='1',
        )

    def test_authentication_errors_point_to_azure_cli(self):
        error = main.azure_api_error(
            'read Azure subscriptions',
            RuntimeError('credential unavailable'),
        )

        self.assertEqual(error.status_code, 400)
        self.assertIn('az login', error.detail)
        self.assertEqual(error.headers['Cache-Control'], 'no-store')


class AzureLifecycleDispatchTests(unittest.TestCase):
    def test_provision_passes_only_public_key_and_state_callbacks(self):
        plan = SimpleNamespace(provider='azure')
        job = {
            'resources': {},
            '_public_key': 'ssh-ed25519 public',
            '_key': 'private',
        }
        with patch.object(
            main.azure_provider,
            'provision',
            return_value={'provider': 'azure'},
        ) as provision:
            result = main.provision(job, plan)

        self.assertEqual(result, {'provider': 'azure'})
        provision.assert_called_once_with(
            job,
            plan,
            public_key='ssh-ed25519 public',
            emit=main.event,
            persist=main.persist_job_state,
        )
        self.assertNotIn('private', repr(provision.call_args.kwargs))

    def test_benchmark_and_destroy_dispatch_only_to_azure(self):
        plan = SimpleNamespace(provider='azure')
        job = {'plan': {'provider': 'azure'}, 'resources': {}}
        with (
            patch.object(
                main,
                'run_azure_benchmarks',
                return_value='benchmarked',
            ) as benchmark,
            patch.object(
                main.azure_provider,
                'destroy_resources',
                return_value='destroyed',
            ) as destroy,
        ):
            self.assertEqual(main.run_benchmarks(job, plan), 'benchmarked')
            self.assertEqual(main.destroy_resources(job), 'destroyed')

        benchmark.assert_called_once_with(job, plan)
        destroy.assert_called_once_with(
            job,
            emit=main.event,
            persist=main.persist_job_state,
            preserve_status=False,
        )

    def test_saved_resource_group_contract_is_recoverable(self):
        job = {
            'plan': {'provider': 'azure'},
            'resources': {
                'azure_subscription_id': 'subscription-id',
                'azure_resource_group_name': 'benchmark-abc123',
                'azure_resource_group_tags': {
                    'managed-by': 'oci-self-service-benchmarks',
                    'benchmark-job': 'abc123',
                    'benchmark-role': 'resource-group',
                },
            },
        }

        self.assertTrue(main.has_recoverable_resources(job))

    def test_data_mount_uses_only_the_persisted_azure_lun(self):
        job = {
            'resources': {
                'provider': 'azure',
                'azure_data_disk_lun': 0,
                'ssh_user': 'benchmark',
            },
        }
        with (
            patch.object(
                main.rocky_linux,
                'azure_data_volume_mount_command',
                return_value='safe-mount',
            ) as command,
            patch.object(main, 'ssh', return_value='mounted') as ssh,
            patch.object(main, 'event'),
        ):
            main.mount_data_volume(job)

        command.assert_called_once_with(lun=0, owner='benchmark')
        ssh.assert_called_once_with(job, 'safe-mount', timeout=300)

    def test_runtime_metadata_records_azure_disk_and_placement(self):
        plan = SimpleNamespace(
            azure_subscription_id='subscription-id',
            region='eastus2',
            azure_zone='1',
            shape='Standard_D8ps_v6',
            ocpus=8,
            memory_gb=32,
        )
        job = {
            'resources': {
                'architecture': 'arm64',
                'image_id': '/images/rocky-9',
                'image_name': (
                    'resf:rockylinux-aarch64:rockylinux-aarch64-9:9.6'
                ),
                'azure_resource_group_name': 'benchmark-run',
                'azure_data_disk_type': 'PremiumV2_LRS',
                'azure_data_disk_size_gb': 100,
                'azure_data_disk_iops': 3000,
                'azure_data_disk_throughput_mibps': 125,
            },
        }

        metadata = main.azure_runtime_environment(job, plan)

        self.assertEqual(metadata['provider'], 'Azure')
        self.assertEqual(metadata['vm_size'], 'Standard_D8ps_v6')
        self.assertEqual(metadata['architecture'], 'arm64')
        self.assertEqual(metadata['data_volume_type'], 'PremiumV2_LRS')
        self.assertEqual(metadata['data_volume_provisioned_iops'], 3000)


if __name__ == '__main__':
    unittest.main()
