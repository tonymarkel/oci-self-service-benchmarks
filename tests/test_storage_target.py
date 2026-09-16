import base64
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app import main, storage_target


def encoded(value):
    return base64.b64encode(value.encode()).decode()


def local_success_output(provider='aws'):
    return (
        'local preparation diagnostics\n'
        'OCI_BENCH_STORAGE_TARGET_V1 '
        f'provider={provider} '
        f'model_b64={encoded("Provider Local NVMe")} '
        'capacity_bytes=1900000000000 '
        'mount_point=/benchmark-local filesystem=xfs\n'
    )


def local_fallback_output(provider='aws'):
    return (
        'OCI_BENCH_STORAGE_TARGET_FALLBACK_V1 '
        f'provider={provider} reason=candidate_count_mismatch\n'
    )


def mounted_descriptor_output():
    return (
        'OCI_BENCH_MOUNTED_STORAGE_TARGET_V1 '
        f'model_b64={encoded("Cloud Block Volume")} '
        'capacity_bytes=268435456000 mount_point=/data '
        'filesystem=xfs transport=nvme\n'
    )


class StorageTargetCommandTests(unittest.TestCase):
    def assert_valid_bash(self, command):
        with tempfile.NamedTemporaryFile(mode='w') as script:
            script.write(command)
            script.flush()
            subprocess.run(
                ['bash', '-n', script.name],
                check=True,
                capture_output=True,
                text=True,
            )

    def test_provider_probes_require_exact_local_identity(self):
        commands = {
            provider: storage_target.local_nvme_prepare_command(
                provider,
                2,
                3800,
            )
            for provider in ('aws', 'oci', 'azure', 'gcp')
        }

        self.assertIn(
            'Amazon EC2 NVMe Instance Storage',
            commands['aws'],
        )
        self.assertIn(
            'MODEL=$(lsblk -dno MODEL "$LINK"',
            commands['aws'],
        )
        self.assertEqual(commands['aws'].count('lsblk -dno MODEL'), 2)
        self.assertNotIn('lsblk -dnro MODEL', commands['aws'])
        self.assertIn('/dev/disk/azure/local/by-serial/*', commands['azure'])
        self.assertIn('/dev/disk/azure/local/by-index/*', commands['azure'])
        self.assertNotIn('/dev/disk/azure/resource', commands['azure'])
        self.assertIn(
            '/dev/disk/by-id/google-local-nvme-ssd-*',
            commands['gcp'],
        )
        self.assertIn('EXPECTED_COUNT=2', commands['oci'])
        self.assertIn('for LINK in /dev/nvme*n1', commands['oci'])
        for provider, command in commands.items():
            with self.subTest(provider=provider):
                self.assertIn('candidate_count_mismatch', command)
                self.assertIn('capacity_mismatch', command)
                self.assertIn('MINIMUM_BYTES=8589934592', command)
                self.assertIn(
                    'ROOT_ROWS=$(lsblk -srnpo NAME "$ROOT_DEVICE"',
                    command,
                )
                self.assertIn('ROOT_DEVICES+=("$ROOT_MEMBER")', command)
                self.assertNotIn('head -n 1 | tr -d "[:space:]"; ', command)
                self.assertIn('readonly_probe_failed', command)
                self.assertIn('device_has_partitions', command)
                self.assertIn('partition_probe_failed', command)
                self.assertIn('device_has_holders', command)
                self.assertIn('holders_probe_failed', command)
                self.assertIn('device_is_mounted', command)
                self.assertIn('mount_probe_failed', command)
                self.assertIn('device_is_swap', command)
                self.assertIn('swap_probe_failed', command)
                self.assertIn(
                    'SWAPS=$(swapon --show=NAME --noheadings --raw '
                    '2>/dev/null)',
                    command,
                )
                self.assertEqual(command.count('swapon --show=NAME'), 1)
                self.assertNotIn(
                    'swapon --noheadings --raw --output NAME',
                    command,
                )
                self.assertNotIn('--output NAME', command)
                self.assertIn('device_has_signatures', command)
                self.assertIn('signature_probe_failed', command)
                self.assertIn('mount_point_probe_failed', command)
                self.assertIn('[ -L "$MOUNT_POINT" ]', command)
                self.assertIn('sudo mkfs.xfs "$DEVICE"', command)
                self.assertNotIn('sudo mkfs.xfs -f "$DEVICE"', command)
                self.assertIn('sudo mount "$DEVICE" "$MOUNT_POINT"', command)
                self.assertNotIn('/etc/fstab', command)
                self.assert_valid_bash(command)

    def test_zero_control_plane_count_falls_back_before_discovery(self):
        command = storage_target.local_nvme_prepare_command('oci', 0)

        fallback = command.index('fallback no_control_plane_local_nvme')
        discovery = command.index('for LINK in /dev/nvme*n1')
        self.assertLess(fallback, discovery)
        self.assert_valid_bash(command)
        completed = subprocess.run(
            ['bash', '-c', command],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIsNone(
            storage_target.parse_local_nvme_prepare_output(completed.stdout)
        )

    def test_command_inputs_are_strictly_validated(self):
        for provider in ('unknown', 'aws; id'):
            with self.subTest(provider=provider), self.assertRaisesRegex(
                ValueError,
                'provider',
            ):
                storage_target.local_nvme_prepare_command(provider, 1)
        for count in (-1, True, 1.5, '1'):
            with self.subTest(count=count), self.assertRaisesRegex(
                ValueError,
                'count',
            ):
                storage_target.local_nvme_prepare_command('aws', count)
        for size in (0, -1, float('nan'), True, 'large'):
            with self.subTest(size=size), self.assertRaisesRegex(
                ValueError,
                'capacity',
            ):
                storage_target.local_nvme_prepare_command('aws', 1, size)
        for mount_point in (
            'data', '/', '//data', '/data//nested', '/data/../tmp',
            '/data;id', ' /data',
        ):
            with self.subTest(mount_point=mount_point), self.assertRaisesRegex(
                ValueError,
                'mount point',
            ):
                storage_target.local_nvme_prepare_command(
                    'aws',
                    1,
                    mount_point=mount_point,
                )

    def test_mounted_descriptor_is_read_only_and_report_safe(self):
        command = storage_target.mounted_storage_descriptor_command('/data')

        self.assertIn('findmnt -rn -o SOURCE', command)
        self.assertIn('findmnt -rn -o FSTYPE', command)
        self.assertIn('CAPACITY_BYTES=$(lsblk -bdnro SIZE', command)
        self.assertIn('MODEL=$(lsblk -dno MODEL "$DEVICE"', command)
        self.assertEqual(command.count('lsblk -dno MODEL'), 1)
        self.assertNotIn('lsblk -dnro MODEL', command)
        self.assertNotIn('mkfs', command)
        self.assertNotIn(' mount ', command)
        self.assertNotIn('/etc/fstab', command)
        self.assert_valid_bash(command)


class OciLocalNvmeProfileTests(unittest.TestCase):
    def test_shape_config_requires_finite_nvme_count_capacity_and_description(self):
        profile = main._oci_local_nvme_storage_summary(SimpleNamespace(
            local_disks=2,
            local_disks_total_size_in_gbs=13_600.0,
            local_disk_description='2 locally attached NVMe SSDs',
        ))

        self.assertEqual(profile, {
            'local_nvme_supported': True,
            'local_nvme_disk_count': 2,
            'local_nvme_disk_size_gb': 6_800.0,
            'local_nvme_total_size_gb': 13_600.0,
        })

        for values in (
            {'local_disks': 0, 'local_disks_total_size_in_gbs': 13_600.0},
            {'local_disks': 2, 'local_disks_total_size_in_gbs': 0},
            {'local_disks': 2, 'local_disks_total_size_in_gbs': float('inf')},
            {'local_disks': 2, 'local_disks_total_size_in_gbs': float('nan')},
            {'local_disks': 2, 'local_disks_total_size_in_gbs': 13_600.0,
             'local_disk_description': 'local SSDs'},
        ):
            shape_config = SimpleNamespace(**{
                'local_disk_description': 'NVMe local storage',
                **values,
            })
            with self.subTest(values=values):
                self.assertFalse(
                    main._oci_local_nvme_storage_summary(
                        shape_config
                    )['local_nvme_supported']
                )


class StorageTargetParserTests(unittest.TestCase):
    def test_local_success_returns_exact_report_schema(self):
        output = (
            'mkfs diagnostics\n'
            'OCI_BENCH_STORAGE_TARGET_V1 provider=aws '
            f'model_b64={encoded("Amazon EC2 NVMe Instance Storage")} '
            'capacity_bytes=1900000000000 '
            'mount_point=/benchmark-local filesystem=xfs\n'
        )

        descriptor = storage_target.parse_local_nvme_prepare_output(output)
        parsed_descriptor, fallback_reason = (
            storage_target.parse_local_nvme_prepare_result(output)
        )

        self.assertEqual(descriptor, {
            'storage_target_contract': 'v1',
            'storage_target_policy': (
                'prefer_verified_instance_local_nvme_'
                'else_additional_volume_v1'
            ),
            'storage_target_kind': 'instance_local_nvme',
            'storage_target_verification': (
                'provider_attested_guest_verified_v1'
            ),
            'storage_target_transport': 'nvme',
            'storage_target_model': 'Amazon EC2 NVMe Instance Storage',
            'storage_target_capacity_bytes': 1900000000000,
            'storage_target_device_count': 1,
            'storage_target_layout': 'single_device_v1',
            'storage_target_filesystem': 'xfs',
            'storage_target_mount_point': '/benchmark-local',
        })
        self.assertEqual(parsed_descriptor, descriptor)
        self.assertIsNone(fallback_reason)
        self.assertNotIn('storage_target_device', descriptor)
        self.assertNotIn('storage_target_device_link', descriptor)
        self.assertNotIn('storage_target_serial', descriptor)
        self.assertNotIn('storage_target_uuid', descriptor)

    def test_safe_fallback_returns_none(self):
        output = (
            'OCI_BENCH_STORAGE_TARGET_FALLBACK_V1 '
            'provider=azure reason=candidate_count_mismatch\n'
        )

        self.assertIsNone(
            storage_target.parse_local_nvme_prepare_output(output)
        )

        self.assertEqual(
            storage_target.parse_local_nvme_prepare_result(output),
            (None, 'candidate_count_mismatch'),
        )

    def test_fallback_reason_must_be_safe_before_it_can_be_logged(self):
        output = (
            'OCI_BENCH_STORAGE_TARGET_FALLBACK_V1 '
            'provider=aws reason=candidate-count-mismatch\n'
        )

        with self.assertRaisesRegex(ValueError, 'fallback reason'):
            storage_target.parse_local_nvme_prepare_result(output)

    def test_mounted_descriptor_normalizes_fallback_metadata(self):
        output = (
            'OCI_BENCH_MOUNTED_STORAGE_TARGET_V1 '
            f'model_b64={encoded("Amazon Elastic Block Store")} '
            'capacity_bytes=268435456000 mount_point=/data '
            'filesystem=xfs transport=nvme\n'
        )

        descriptor = storage_target.parse_mounted_storage_descriptor_output(
            output
        )

        self.assertEqual(
            descriptor['storage_target_kind'],
            'provisioned_data_volume',
        )
        self.assertEqual(
            descriptor['storage_target_verification'],
            'manifest_bound_guest_verified_v1',
        )
        self.assertEqual(descriptor['storage_target_transport'], 'nvme')
        self.assertEqual(
            descriptor['storage_target_model'],
            'Amazon Elastic Block Store',
        )
        self.assertEqual(descriptor['storage_target_device_count'], 1)
        self.assertNotIn('storage_target_device', descriptor)

    def test_parsers_reject_missing_duplicate_and_malformed_markers(self):
        success = (
            'OCI_BENCH_STORAGE_TARGET_V1 provider=oci '
            f'model_b64={encoded("OCI local NVMe")} '
            'capacity_bytes=8589934592 mount_point=/benchmark-local '
            'filesystem=xfs'
        )
        invalid = (
            '',
            success + '\n' + success,
            success.replace('capacity_bytes=8589934592', 'capacity_bytes=1'),
            success.replace('filesystem=xfs', 'filesystem=ext4'),
            success.replace('model_b64=', 'model_b64=***'),
            success + ' raw_device=/dev/nvme0n1',
        )
        for output in invalid:
            with self.subTest(output=output), self.assertRaises(ValueError):
                storage_target.parse_local_nvme_prepare_output(output)

    def test_model_text_is_safely_normalized(self):
        model = encoded('Cloud\nDisk <unsafe>')
        output = (
            'OCI_BENCH_MOUNTED_STORAGE_TARGET_V1 '
            f'model_b64={model} '
            'capacity_bytes=1000 mount_point=/data '
            'filesystem=xfs transport=paravirtualized'
        )

        descriptor = storage_target.parse_mounted_storage_descriptor_output(
            output
        )

        self.assertEqual(descriptor['storage_target_model'], 'Cloud Disk unsafe')


class StorageBenchmarkTargetSelectionTests(unittest.TestCase):
    @staticmethod
    def plan(provider='aws', additional_volume=True):
        return SimpleNamespace(
            provider=provider,
            storage=SimpleNamespace(additional_volume=additional_volume),
        )

    @staticmethod
    def job(provider='aws', **resources):
        values = {'provider': provider}
        values.update(resources)
        return {
            'id': 'storage-target-selection',
            'events': [],
            'results': [],
            'resources': values,
        }

    def test_verified_local_success_skips_data_mount_and_persists_descriptor(self):
        job = self.job(
            local_nvme_supported=True,
            local_nvme_disk_count=1,
            local_nvme_total_size_gb=1900,
        )

        with (
            patch.object(
                main,
                'ssh',
                return_value=local_success_output(),
            ) as ssh,
            patch.object(main, 'mount_data_volume') as mount,
        ):
            directory, descriptor = main.prepare_storage_benchmark_target(
                job,
                self.plan(),
                {'fio'},
            )

        self.assertEqual(directory, '/benchmark-local')
        self.assertEqual(
            descriptor['storage_target_kind'],
            'instance_local_nvme',
        )
        self.assertEqual(descriptor['storage_target_contract'], 'v1')
        self.assertEqual(
            job['resources']['benchmark_storage_target'],
            descriptor,
        )
        mount.assert_not_called()
        ssh.assert_called_once()
        self.assertEqual(ssh.call_args.kwargs['transport_attempts'], 1)
        self.assertIn('EXPECTED_COUNT=1', ssh.call_args.args[1])
        self.assertIn('/benchmark-local', ssh.call_args.args[1])
        self.assertTrue(any(
            'unused fallback' in item['message']
            for item in job['events']
        ))

    def test_zero_or_unsafe_local_profile_uses_data_without_local_probe(self):
        invalid_profiles = (
            {},
            {
                'local_nvme_supported': False,
                'local_nvme_disk_count': 1,
                'local_nvme_total_size_gb': 1900,
            },
            {
                'local_nvme_supported': True,
                'local_nvme_disk_count': 0,
                'local_nvme_total_size_gb': 1900,
            },
            {
                'local_nvme_supported': True,
                'local_nvme_disk_count': True,
                'local_nvme_total_size_gb': 1900,
            },
            {
                'local_nvme_supported': True,
                'local_nvme_disk_count': 1,
                'local_nvme_total_size_gb': 'unknown',
            },
            {
                'local_nvme_supported': True,
                'local_nvme_disk_count': 1,
                'local_nvme_total_size_gb': float('inf'),
            },
        )
        descriptor_command = storage_target.mounted_storage_descriptor_command(
            '/data'
        )

        for profile in invalid_profiles:
            with self.subTest(profile=profile):
                job = self.job(**profile)
                with (
                    patch.object(
                        main,
                        'ssh',
                        return_value=mounted_descriptor_output(),
                    ) as ssh,
                    patch.object(main, 'mount_data_volume') as mount,
                ):
                    directory, descriptor = (
                        main.prepare_storage_benchmark_target(
                            job,
                            self.plan(),
                            {'sysbench_fileio'},
                        )
                    )

                self.assertEqual(directory, '/data')
                self.assertEqual(
                    descriptor['storage_target_kind'],
                    'provisioned_data_volume',
                )
                mount.assert_called_once_with(job)
                ssh.assert_called_once_with(
                    job,
                    descriptor_command,
                    timeout=60,
                )
                self.assertEqual(
                    job['resources']['benchmark_storage_target'],
                    descriptor,
                )

    def test_local_probe_fallback_then_uses_data_descriptor(self):
        job = self.job(
            local_nvme_supported=True,
            local_nvme_disk_count=1,
            local_nvme_total_size_gb=1900,
        )

        with (
            patch.object(
                main,
                'ssh',
                side_effect=[
                    local_fallback_output(),
                    mounted_descriptor_output(),
                ],
            ) as ssh,
            patch.object(main, 'mount_data_volume') as mount,
        ):
            directory, descriptor = main.prepare_storage_benchmark_target(
                job,
                self.plan(),
                {'fio'},
            )

        self.assertEqual(directory, '/data')
        self.assertEqual(
            descriptor['storage_target_kind'],
            'provisioned_data_volume',
        )
        mount.assert_called_once_with(job)
        self.assertEqual(ssh.call_count, 2)
        self.assertIn('EXPECTED_COUNT=1', ssh.call_args_list[0].args[1])
        self.assertEqual(
            ssh.call_args_list[1].args[1],
            storage_target.mounted_storage_descriptor_command('/data'),
        )
        self.assertTrue(any(
            'did not pass the fail-closed' in item['message']
            for item in job['events']
        ))
        self.assertTrue(any(
            'guest_reason=candidate_count_mismatch' in item['message']
            for item in job['events']
        ))

    def test_malformed_local_probe_fails_closed_before_data_mount(self):
        job = self.job(
            local_nvme_supported=True,
            local_nvme_disk_count=1,
            local_nvme_total_size_gb=1900,
        )

        with (
            patch.object(main, 'ssh', return_value='malformed output'),
            patch.object(main, 'mount_data_volume') as mount,
            self.assertRaisesRegex(ValueError, 'exactly one result marker'),
        ):
            main.prepare_storage_benchmark_target(
                job,
                self.plan(),
                {'fio'},
            )

        mount.assert_not_called()
        self.assertNotIn('benchmark_storage_target', job['resources'])

    def test_oci_data_mount_uses_exact_manifest_device(self):
        job = self.job(
            'oci',
            oci_data_volume_device='/dev/oracleoci/oraclevdb',
        )

        with (
            patch.object(
                main,
                'oci_data_volume_mount_command',
                return_value='exact-oci-mount',
            ) as command,
            patch.object(main, 'ssh', return_value='mounted') as ssh,
        ):
            main.mount_data_volume(job)

        command.assert_called_once_with('/dev/oracleoci/oraclevdb')
        ssh.assert_called_once_with(job, 'exact-oci-mount', timeout=300)

    def test_oci_missing_manifest_device_refuses_guest_inspection(self):
        job = self.job('oci')

        with (
            patch.object(main, 'ssh') as ssh,
            self.assertRaisesRegex(RuntimeError, 'device is missing'),
        ):
            main.mount_data_volume(job)

        ssh.assert_not_called()

    def test_oci_mount_command_is_bound_to_validated_exact_device(self):
        command = main.oci_data_volume_mount_command(
            '/dev/oracleoci/oraclevdb'
        )

        self.assertIn(
            'EXPECTED_DEVICE=/dev/oracleoci/oraclevdb',
            command,
        )
        self.assertIn('resolves to the root disk', command)
        self.assertIn('not a whole disk', command)
        self.assertNotIn(
            'for device in $(lsblk',
            command,
        )
        with self.assertRaisesRegex(ValueError, 'path is invalid'):
            main.oci_data_volume_mount_command('/dev/../etc/passwd')


if __name__ == '__main__':
    unittest.main()
