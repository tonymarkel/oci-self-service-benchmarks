import unittest
from pathlib import Path
from unittest.mock import patch

from app import main
from app.models import BenchmarkPlan, StorageOptions


ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / 'app/static/index.html').read_text()
JAVASCRIPT = (ROOT / 'app/static/app.js').read_text()


def c4a_plan(**overrides):
    values = {
        'provider': 'gcp',
        'gcp_project_id': 'benchmark-project',
        'gcp_zone': 'us-east1-b',
        'region': 'us-east1',
        'shape': 'c4a-standard-4',
        'ocpus': 4,
        'memory_gb': 16,
        'ssh_private_key': 'private',
        'ssh_public_key': 'ssh-ed25519 AAAATEST',
        'storage': {
            'boot_size_gb': 100,
            'additional_volume': True,
            'additional_size_gb': 100,
        },
        'benchmarks': ['fio'],
    }
    values.update(overrides)
    return BenchmarkPlan(**values)


def gcp_job(plan, **resource_overrides):
    resources = {
        'provider': 'gcp',
        'gcp_project_id': plan.gcp_project_id,
        'gcp_zone': plan.gcp_zone,
        'public_ip': '198.51.100.20',
        'private_ip': '10.42.1.2',
        'ssh_user': 'benchmark',
        'architecture': 'arm64',
        'image_id': 'rocky-linux-9-arm64-v20260813',
        'image_name': 'rocky-linux-9-arm64-v20260813',
    }
    resources.update(resource_overrides)
    return {
        'id': 'gcp-c4a-runtime',
        'plan': plan.model_dump(
            exclude={
                'ssh_private_key',
                'ssh_public_key',
                'ssh_key_passphrase',
            }
        ),
        'events': [],
        'resources': resources,
        'results': [],
    }


class GcpC4aPlanContractTests(unittest.TestCase):
    def test_c4a_uses_the_existing_provider_neutral_storage_plan(self):
        plan = c4a_plan()

        self.assertEqual(plan.shape, 'c4a-standard-4')
        self.assertEqual(plan.storage.boot_size_gb, 100)
        self.assertEqual(plan.storage.additional_size_gb, 100)
        self.assertNotIn('disk_type', StorageOptions.model_fields)
        self.assertNotIn('provisioned_iops', StorageOptions.model_fields)
        self.assertNotIn('provisioned_throughput_mibps', StorageOptions.model_fields)


class GcpC4aUiContractTests(unittest.TestCase):
    def test_form_describes_machine_derived_c4a_storage_and_networking(self):
        self.assertIn('<script src="/static/app.js?v=33"></script>', INDEX)
        self.assertIn('C4A uses Hyperdisk Balanced', INDEX)
        self.assertIn('aria-live="polite"', INDEX)
        self.assertIn('function updateGcpStorageHint(shape = null)', JAVASCRIPT)
        self.assertIn("shape.disk_type === 'hyperdisk-balanced'", JAVASCRIPT)
        self.assertIn('3,000 IOPS and 140 MiB/s', JAVASCRIPT)
        self.assertIn('shape.network_interface_type', JAVASCRIPT)
        self.assertIn('the provisioned values in every benchmark result', JAVASCRIPT)
        self.assertIn('const GCP_C4A_DATA_SIZE_GB = 100', JAVASCRIPT)
        self.assertIn("dataSize.dataset.userEdited !== 'true'", JAVASCRIPT)
        self.assertIn("dataSize.dataset.gcpC4aDefaultApplied = 'true'", JAVASCRIPT)
        self.assertIn('remaining regional C4A vCPU and Hyperdisk', JAVASCRIPT)

    def test_discovery_metadata_is_visible_after_type_selection(self):
        selected_start = JAVASCRIPT.index('function updateSelectedShape()')
        selected_end = JAVASCRIPT.index(
            '// A datalist choice emits',
            selected_start,
        )
        selected_shape = JAVASCRIPT[selected_start:selected_end]

        self.assertIn('shape.ocpus', selected_shape)
        self.assertIn('shape.memory_gb', selected_shape)
        self.assertIn('shape.architecture', selected_shape)
        self.assertIn('gcpDiskLabel(shape.disk_type)', selected_shape)
        self.assertIn(
            'gcpNetworkInterfaceLabel(shape.network_interface_type)',
            selected_shape,
        )


class GcpC4aRuntimeMetadataTests(unittest.TestCase):
    def test_hyperdisk_and_gvnic_contract_is_recorded_in_every_result(self):
        plan = c4a_plan()
        job = gcp_job(
            plan,
            gcp_network_interface_type='GVNIC',
            gcp_disk_interface='NVME',
            gcp_boot_disk_type='hyperdisk-balanced',
            gcp_boot_disk_provisioned_iops=3000,
            gcp_boot_disk_provisioned_throughput_mibps=140,
            gcp_data_device_name='benchmark-gcp-c4a-runtime-data',
            gcp_data_disk_type='hyperdisk-balanced',
            gcp_data_disk_size_gb=100,
            gcp_data_disk_provisioned_iops=3000,
            gcp_data_disk_provisioned_throughput_mibps=140,
        )
        target_metadata = {
            'storage_target_contract': 'v1',
            'storage_target_kind': 'provisioned_data_volume',
            'storage_target_mount_point': '/data',
        }

        with (
            patch.object(main, 'ssh', return_value='ready'),
            patch.object(main, 'mount_data_volume') as mount,
            patch.object(
                main,
                'prepare_storage_benchmark_target',
                return_value=('/data', target_metadata),
            ) as prepare_target,
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_gcp_benchmarks(job, plan)

        mount.assert_not_called()
        prepare_target.assert_called_once()
        execute.assert_called_once()
        metadata = execute.call_args.kwargs['metadata']
        self.assertEqual(metadata['network_interface_type'], 'GVNIC')
        self.assertEqual(metadata['disk_interface'], 'NVME')
        self.assertEqual(metadata['boot_volume_type'], 'hyperdisk-balanced')
        self.assertEqual(metadata['boot_volume_provisioned_iops'], 3000)
        self.assertEqual(
            metadata['boot_volume_provisioned_throughput_mibps'],
            140,
        )
        self.assertEqual(metadata['data_volume_type'], 'hyperdisk-balanced')
        self.assertEqual(metadata['data_volume_provisioned_iops'], 3000)
        self.assertEqual(
            metadata['data_volume_provisioned_throughput_mibps'],
            140,
        )
        self.assertEqual(metadata['storage_target_contract'], 'v1')
        self.assertEqual(
            metadata['storage_target_kind'],
            'provisioned_data_volume',
        )

    def test_pd_balanced_runtime_keeps_optional_hyperdisk_fields_absent(self):
        plan = c4a_plan(
            shape='t2a-standard-4',
            storage={'additional_volume': False},
            benchmarks=['sysbench'],
            sysbench={'workloads': ['cpu']},
        )
        job = gcp_job(
            plan,
            gcp_network_interface_type='VIRTIO_NET',
            gcp_boot_disk_type='pd-balanced',
        )

        with (
            patch.object(main, 'ssh', return_value='ready'),
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_gcp_benchmarks(job, plan)

        metadata = execute.call_args.kwargs['metadata']
        self.assertEqual(metadata['network_interface_type'], 'VIRTIO_NET')
        self.assertNotIn('disk_interface', metadata)
        self.assertEqual(metadata['boot_volume_type'], 'pd-balanced')
        self.assertNotIn('boot_volume_provisioned_iops', metadata)
        self.assertNotIn('data_volume_provisioned_iops', metadata)

    def test_phoronix_results_receive_the_same_c4a_environment(self):
        plan = c4a_plan(
            benchmarks=['phoronix'],
            phoronix={'profiles': ['compress_7zip']},
        )
        job = gcp_job(
            plan,
            gcp_network_interface_type='GVNIC',
            gcp_disk_interface='NVME',
            gcp_boot_disk_type='hyperdisk-balanced',
            gcp_boot_disk_provisioned_iops=3000,
            gcp_boot_disk_provisioned_throughput_mibps=140,
            gcp_data_disk_type='hyperdisk-balanced',
            gcp_data_disk_size_gb=100,
            gcp_data_disk_provisioned_iops=3000,
            gcp_data_disk_provisioned_throughput_mibps=140,
        )

        with (
            patch.object(main, 'ssh', side_effect=['prepared', 'aarch64']),
            patch.object(main, 'execute_benchmark') as execute,
        ):
            failures = main.run_phoronix_profiles(job, plan)

        self.assertEqual(failures, ())
        execute.assert_called_once()
        metadata = execute.call_args.kwargs['metadata']
        self.assertEqual(metadata['network_interface_type'], 'GVNIC')
        self.assertEqual(metadata['disk_interface'], 'NVME')
        self.assertEqual(metadata['boot_volume_type'], 'hyperdisk-balanced')
        self.assertEqual(metadata['data_volume_type'], 'hyperdisk-balanced')
        self.assertEqual(metadata['data_volume_provisioned_iops'], 3000)
        self.assertEqual(
            metadata['data_volume_provisioned_throughput_mibps'],
            140,
        )


if __name__ == '__main__':
    unittest.main()
