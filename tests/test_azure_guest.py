import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from types import SimpleNamespace

from app import phoronix
from app.guests import rocky_linux, web


class AzureRockyLinuxGuestTests(unittest.TestCase):
    @staticmethod
    def assert_valid_bash(command):
        subprocess.run(
            ['bash', '-n'],
            input=command,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_selection_accepts_azure_surface_but_rejects_sctp(self):
        rocky_linux.validate_benchmark_selection(
            [
                'apachebench',
                'deathstarbench',
                'sysbench',
                'stream',
                'fio',
                'iperf3',
                'phoronix',
            ],
            sysbench_workloads=['cpu', 'memory', 'fileio'],
            iperf3_protocols=['tcp', 'udp'],
            phoronix_profiles=phoronix.PROFILES,
            llm_benchmarks=['llama_bench'],
            provider='azure',
        )
        self.assertEqual(
            rocky_linux.supported_iperf3_protocols('azure'),
            frozenset({'tcp', 'udp'}),
        )

        with self.assertRaisesRegex(ValueError, 'not supported on Azure.*sctp'):
            rocky_linux.validate_benchmark_selection(
                ['iperf3'],
                iperf3_protocols=['sctp'],
                provider='azure',
            )
        with self.assertRaisesRegex(ValueError, 'not supported on Azure.*sctp'):
            rocky_linux.installation_steps(
                ['iperf3'],
                iperf3_protocols=['sctp'],
                provider='azure',
            )
        with self.assertRaisesRegex(ValueError, 'Azure.*unsupported.*sctp'):
            rocky_linux.iperf3_peer_startup_script(
                ['tcp', 'sctp'],
                provider='azure',
            )

    def test_readiness_is_provider_aware_and_uses_rocky_repositories(self):
        hosts = rocky_linux.benchmark_readiness_hosts(
            ['stream', 'phoronix'],
            llm_benchmarks=['llama_bench'],
        )
        command = rocky_linux.readiness_command(
            'arm64',
            hosts,
            provider='azure',
        )

        self.assertIn(
            'The Azure benchmark guest must be Rocky Linux 9',
            command,
        )
        self.assertIn('EXPECTED_ARCH=aarch64', command)
        self.assertIn('mirrors.rockylinux.org', command)
        self.assertIn('openbenchmarking.org', command)
        self.assertIn('huggingface.co', command)
        self.assertNotIn('cdn.amazonlinux.com', command)
        self.assert_valid_bash(command)

    def test_azure_installation_uses_shared_rocky_packages(self):
        steps = rocky_linux.installation_steps(
            ['sysbench', 'fio', 'iperf3'],
            sysbench_workloads=['fileio'],
            iperf3_protocols=['tcp', 'udp'],
            additional_volume=True,
            provider='azure',
        )

        combined = '\n'.join(step.command for step in steps)
        self.assertIn('install ', combined)
        self.assertIn('fio', combined)
        self.assertIn('iperf3', combined)
        self.assertIn('xfsprogs', combined)
        self.assertNotIn('lksctp-tools', combined)
        self.assertNotIn(' install curl', combined)
        for step in steps:
            self.assert_valid_bash(step.command)

    def test_lun_mount_uses_only_exact_azure_links_and_persists_uuid(self):
        command = rocky_linux.azure_data_volume_mount_command(
            lun=7,
            owner='benchmark',
        )

        self.assertIn('LUN=7', command)
        self.assertIn('/dev/disk/azure/data/by-lun/$LUN', command)
        self.assertIn('/dev/disk/azure/scsi1/lun$LUN', command)
        self.assertNotIn('/dev/disk/azure/*', command)
        self.assertIn(
            'Refusing to format or mount the Azure boot disk',
            command,
        )
        self.assertIn('sudo mkfs.xfs "$DEVICE"', command)
        self.assertNotIn('mkfs.xfs -f', command)
        self.assertIn(
            'UUID=%s /data xfs discard,nofail 0 2 '
            '# cloud-benchmark-data',
            command,
        )
        self.assertIn('AZURE_DATA_VOLUME lun=%s device_link=%s uuid=%s', command)
        self.assertLess(command.index('MOUNTED_SOURCE='), command.index('FSTAB_TMP='))
        self.assert_valid_bash(command)

    def test_lun_mount_rejects_unsafe_values(self):
        for lun in (-1, 64, True, '0'):
            with self.subTest(lun=lun), self.assertRaisesRegex(
                ValueError,
                'LUN',
            ):
                rocky_linux.azure_data_volume_mount_command(lun=lun)
        with self.assertRaisesRegex(ValueError, 'owner'):
            rocky_linux.azure_data_volume_mount_command(owner='root')

    def test_deathstar_database_mount_is_fixed_to_lun_zero_and_runtime_path(self):
        command = (
            rocky_linux.azure_deathstarbench_database_volume_mount_command()
        )

        self.assertIn('LUN=0', command)
        self.assertIn(
            'MOUNT_POINT=/var/lib/deathstarbench/database',
            command,
        )
        self.assertIn('/dev/disk/azure/data/by-lun/$LUN', command)
        self.assertIn('/dev/disk/azure/scsi1/lun$LUN', command)
        self.assertNotIn('/dev/disk/azure/*', command)
        self.assertIn(
            'Refusing to format or mount the Azure boot disk',
            command,
        )
        self.assertIn('ROOT_ANCESTRY=$(lsblk -srnpo NAME "$ROOT_DEVICE")', command)
        self.assertIn('lsblk -dnro TYPE "$DEVICE"', command)
        self.assertIn('DEVICE_TREE=$(lsblk -nrpo NAME "$DEVICE")', command)
        self.assertIn('CHILD_COUNT=$(printf', command)
        self.assertIn('DEVICE_MOUNTS=$(lsblk -dnro MOUNTPOINTS', command)
        self.assertIn(
            'Azure LUN 0 links resolve to different block devices',
            command,
        )
        self.assertIn(
            'The DeathStarBench database mount point is not a trusted '
            'directory',
            command,
        )
        self.assertIn('sudo mkfs.xfs "$DEVICE"', command)
        self.assertNotIn('mkfs.xfs -f', command)
        self.assertIn(
            'UUID=%s /var/lib/deathstarbench/database xfs '
            'discard,nofail 0 2 # cloud-benchmark-dsb-database',
            command,
        )
        self.assertIn(
            'A different filesystem is already mounted at '
            '/var/lib/deathstarbench/database',
            command,
        )
        self.assertIn(
            'Refusing to replace a non-benchmark DeathStarBench database '
            'fstab entry',
            command,
        )
        self.assertIn('sudo chown root:root "$MOUNT_POINT"', command)
        self.assertIn('sudo chmod 0755 "$MOUNT_POINT"', command)
        self.assertIn('sudo restorecon -F "$MOUNT_POINT"', command)
        self.assertNotIn('restorecon -RF "$MOUNT_POINT"', command)
        self.assertNotIn('sudo chmod 0775 "$MOUNT_POINT"', command)
        self.assertIn(
            'AZURE_DSB_DATABASE_VOLUME lun=0 device_link=%s uuid=%s',
            command,
        )
        self.assertLess(
            command.index('MOUNTED_SOURCE='),
            command.index('FSTAB_TMP='),
        )
        self.assert_valid_bash(command)

    def test_deathstar_database_mount_contract_cannot_be_redirected(self):
        with self.assertRaises(TypeError):
            rocky_linux.azure_deathstarbench_database_volume_mount_command(
                lun=1
            )
        with self.assertRaises(TypeError):
            rocky_linux.azure_deathstarbench_database_volume_mount_command(
                mount_point='/data'
            )

    def test_deathstar_database_volume_attestation_is_uuid_bound_and_read_only(self):
        filesystem_uuid = '11111111-2222-3333-4444-555555555555'

        command = (
            rocky_linux.azure_deathstarbench_database_volume_attestation_command(
                filesystem_uuid.upper()
            )
        )

        self.assertIn(f'EXPECTED_UUID={filesystem_uuid}', command)
        self.assertIn('LUN=0', command)
        self.assertIn(
            'MOUNT_POINT=/var/lib/deathstarbench/database',
            command,
        )
        self.assertIn('/dev/disk/azure/data/by-lun/$LUN', command)
        self.assertIn('/dev/disk/azure/scsi1/lun$LUN', command)
        self.assertNotIn('/dev/disk/azure/*', command)
        self.assertIn(
            'Azure LUN 0 links resolve to different block devices',
            command,
        )
        self.assertIn('lsblk -dnro TYPE "$DEVICE"', command)
        self.assertIn('ROOT_ANCESTRY=$(lsblk -srnpo NAME "$ROOT_DEVICE")', command)
        self.assertIn('sudo blkid -s TYPE -o value "$DEVICE"', command)
        self.assertIn('sudo blkid -s UUID -o value "$DEVICE"', command)
        self.assertIn(
            'sudo findmnt -rn -o UUID --mountpoint "$MOUNT_POINT"',
            command,
        )
        self.assertIn(
            'sudo findmnt -rn -o FSTYPE --mountpoint "$MOUNT_POINT"',
            command,
        )
        self.assertIn(
            'UUID=$EXPECTED_UUID /var/lib/deathstarbench/database xfs '
            'discard,nofail 0 2 # cloud-benchmark-dsb-database',
            command,
        )
        self.assertIn('grep -Fxq "$EXPECTED_FSTAB" /etc/fstab', command)
        self.assertIn(
            'AZURE_DSB_DATABASE_VOLUME lun=0 device_link=%s uuid=%s',
            command,
        )

        for mutating_fragment in (
            'mkfs',
            'sudo mount',
            'FSTAB_TMP=',
            'sudo install -m 0644',
            'sudo sed -i',
            'sudo tee',
            'chown',
            'chmod',
            'restorecon',
        ):
            with self.subTest(mutating_fragment=mutating_fragment):
                self.assertNotIn(mutating_fragment, command)
        self.assert_valid_bash(command)

    def test_deathstar_database_volume_attestation_rejects_non_uuid_identity(self):
        for value in (
            '',
            'not-a-uuid',
            '../disk',
            True,
            '11111111-2222-3333-4444-55555555555g',
        ):
            with self.subTest(value=value), self.assertRaisesRegex(
                ValueError,
                'filesystem UUID',
            ):
                rocky_linux.azure_deathstarbench_database_volume_attestation_command(
                    value
                )

    def test_database_workload_storage_is_uuid_bound_owned_and_selinux_labeled(self):
        filesystem_uuid = '11111111-2222-3333-4444-555555555555'

        command = (
            rocky_linux.azure_deathstarbench_database_workload_storage_command(
                filesystem_uuid
            )
        )

        self.assertIn(f'EXPECTED_UUID={filesystem_uuid}', command)
        self.assertIn(
            'MOUNT_POINT=/var/lib/deathstarbench/database',
            command,
        )
        self.assertIn('.deathstarbench-workload-storage-v1', command)
        for database in (
            'media-mongodb',
            'post-storage-mongodb',
            'social-graph-mongodb',
            'url-shorten-mongodb',
            'user-mongodb',
            'user-timeline-mongodb',
        ):
            self.assertIn(database, command)
        self.assertIn('semanage fcontext -a -t container_file_t', command)
        self.assertIn('restorecon -R -x "$MOUNT_POINT"', command)
        self.assertIn('Refusing to relabel database storage containing', command)
        self.assertIn('EXPECTED_DATABASES=', command)
        self.assertIn('ACTUAL_DATABASES=', command)
        self.assertIn('DATABASE_COUNT=', command)
        self.assertIn(
            'database workload root contains an unexpected directory inventory',
            command,
        )
        self.assertIn(':container_file_t:', command)
        self.assertNotIn('rm -rf', command)
        self.assertNotIn('mkfs', command)
        self.assertNotIn('mount "$MOUNT_POINT"', command)
        self.assert_valid_bash(command)

    def test_database_workload_storage_rejects_non_uuid_identity(self):
        for value in ('', 'not-a-uuid', '../disk', True):
            with self.subTest(value=value), self.assertRaisesRegex(
                ValueError,
                'filesystem UUID',
            ):
                rocky_linux.azure_deathstarbench_database_workload_storage_command(
                    value
                )

    def test_database_workload_storage_shell_is_idempotent_and_fails_closed(self):
        filesystem_uuid = '11111111-2222-3333-4444-555555555555'

        with tempfile.TemporaryDirectory() as directory:
            fixture_root = Path(directory).resolve()
            mount_point = fixture_root / 'database'
            mount_point.mkdir()
            tools = fixture_root / 'bin'
            tools.mkdir()

            def executable(name, body):
                path = tools / name
                path.write_text(body)
                path.chmod(0o755)

            executable('sudo', '#!/bin/sh\nexec "$@"\n')
            executable(
                'install',
                '#!/bin/sh\n'
                'directory=false\nmode=\n'
                'while [ "$#" -gt 0 ]; do\n'
                '  case "$1" in\n'
                '    -d) directory=true; shift;;\n'
                '    -o|-g) shift 2;;\n'
                '    -m) mode="$2"; shift 2;;\n'
                '    *) break;;\n'
                '  esac\n'
                'done\n'
                'if [ "$directory" = true ]; then\n'
                '  exec /usr/bin/install -d -m "$mode" "$@"\n'
                'fi\n'
                'exec /usr/bin/install -m "$mode" "$@"\n',
            )
            executable(
                'findmnt',
                '#!/bin/sh\n'
                'case "$*" in\n'
                '  *"-o FSTYPE --mountpoint"*) printf "xfs\\n";;\n'
                '  *"-o UUID --mountpoint"*) printf "%s\\n" "$MOCK_UUID";;\n'
                '  *"-o TARGET"*)\n'
                '    printf "%s\\n" "$MOCK_MOUNT_POINT"\n'
                '    if [ -n "${MOCK_NESTED_MOUNT:-}" ]; then\n'
                '      printf "%s\\n" "$MOCK_NESTED_MOUNT"\n'
                '    fi;;\n'
                '  *) exit 2;;\n'
                'esac\n',
            )
            executable(
                'find',
                '#!/bin/sh\n'
                'root="$1"\n'
                'for entry in "$root"/*; do\n'
                '  [ -e "$entry" ] || continue\n'
                '  basename "$entry"\n'
                'done\n',
            )
            executable(
                'stat',
                '#!/bin/sh\n'
                'case "$*" in\n'
                '  *"%U:%G:%a"*) printf "root:root:600\\n";;\n'
                '  *"%C"*) printf "system_u:object_r:container_file_t:s0\\n";;\n'
                '  *) exit 2;;\n'
                'esac\n',
            )
            executable('semanage', '#!/bin/sh\nexit 0\n')
            executable('restorecon', '#!/bin/sh\nexit 0\n')

            command = (
                rocky_linux.azure_deathstarbench_database_workload_storage_command(
                    filesystem_uuid
                ).replace(
                    '/var/lib/deathstarbench/database',
                    str(mount_point),
                )
            )
            environment = os.environ.copy()
            environment.update({
                'PATH': f'{tools}{os.pathsep}{environment["PATH"]}',
                'MOCK_MOUNT_POINT': str(mount_point),
                'MOCK_UUID': filesystem_uuid,
            })

            def run(**environment_overrides):
                current_environment = {**environment, **environment_overrides}
                return subprocess.run(
                    ['bash', '-c', command],
                    capture_output=True,
                    text=True,
                    env=current_environment,
                    check=False,
                )

            first = run()
            self.assertEqual(first.returncode, 0, first.stderr)
            self.assertIn('databases=6', first.stdout)
            repeated = run()
            self.assertEqual(repeated.returncode, 0, repeated.stderr)

            nested = run(
                MOCK_NESTED_MOUNT=str(mount_point / 'mongodb' / 'nested')
            )
            self.assertNotEqual(nested.returncode, 0)
            self.assertIn('nested mount', nested.stderr)

            unexpected = mount_point / 'mongodb' / 'unexpected'
            unexpected.mkdir()
            extra = run()
            self.assertNotEqual(extra.returncode, 0)
            self.assertIn('unexpected directory inventory', extra.stderr)
            unexpected.rmdir()

            marker = mount_point / '.deathstarbench-workload-storage-v1'
            marker.write_text('foreign ownership\n')
            drift = run()
            self.assertNotEqual(drift.returncode, 0)
            self.assertIn('ownership marker changed', drift.stderr)

    def test_azure_peer_startup_supports_only_tcp_and_udp(self):
        script = rocky_linux.iperf3_peer_startup_script(
            ['tcp', 'udp'],
            provider='azure',
        )

        self.assertIn('Description=Azure benchmark iperf3 server', script)
        self.assertIn('--add-port=5201/tcp', script)
        self.assertIn('--add-port=5201/udp', script)
        self.assertNotIn('5201/sctp', script)
        self.assertNotIn('lksctp-tools', script)
        self.assert_valid_bash(script)


class AzureWebGuestTests(unittest.TestCase):
    def test_web_readiness_and_packages_use_rocky_contract(self):
        command = web.readiness_command(
            'azure',
            'apachebench',
            'service',
            region='eastus',
            expected_architecture='aarch64',
        )
        target = web.apachebench_install_steps('azure', 'service')[0].command
        loadgen = web.apachebench_install_steps('azure', 'loadgen')[0].command

        self.assertIn('The Azure benchmark guest must be Rocky Linux 9', command)
        self.assertIn('mirrors.rockylinux.org', command)
        self.assertIn('EXPECTED_ARCH=aarch64', command)
        self.assertIn('install firewalld httpd', target)
        self.assertIn('install httpd-tools time', loadgen)

    def test_deathstar_uses_crb_epel_and_packaged_compose(self):
        steps = web.deathstarbench_install_steps('azure', 'service')
        combined = '\n'.join(step.command for step in steps)

        self.assertIn('install git podman python3 python3-pyyaml', combined)
        self.assertIn('config-manager --set-enabled crb', combined)
        self.assertIn('install epel-release', combined)
        self.assertIn('install podman-compose', combined)
        self.assertNotIn(web.PODMAN_COMPOSE_URL, combined)
        self.assertEqual(
            web.podman_compose_path('azure'),
            '/usr/bin/podman-compose',
        )
        for step in steps:
            AzureRockyLinuxGuestTests.assert_valid_bash(step.command)

    def test_runtime_metadata_uses_azure_vnet_and_vcpu_vocabulary(self):
        plan = SimpleNamespace(
            provider='azure',
            region='eastus',
            shape='Standard_D8pls_v6',
            ocpus=8,
            memory_gb=16,
        )
        resources = {
            'private_ip': '10.42.1.10',
            'architecture': 'aarch64',
            'vcpu': 8,
            'loadgen_private_ip': '10.42.1.20',
            'loadgen_shape': 'Standard_D4s_v6',
            'loadgen_architecture': 'x86_64',
            'loadgen_vcpus': 4,
            'loadgen_memory_gb': 8,
            'loadgen_network_bandwidth_gbps': 2.0,
        }

        metadata = web.runtime_metadata(
            plan,
            resources,
            service_architecture='aarch64',
            loadgen_architecture='x86_64',
        )

        self.assertEqual(metadata['provider'], 'Azure')
        self.assertEqual(metadata['region'], 'eastus')
        self.assertEqual(metadata['service_vcpus'], 8)
        self.assertEqual(metadata['load_generator_vcpus'], 4)
        self.assertEqual(metadata['traffic_path'], 'Azure private VNet address')
        self.assertNotIn('service_ocpus', metadata)


if __name__ == '__main__':
    unittest.main()
