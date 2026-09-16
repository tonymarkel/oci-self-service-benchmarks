import subprocess
import unittest
from datetime import datetime, timezone

from app import phoronix
from app.guests import amazon_linux, rocky_linux


class RockyLinuxGuestContractTests(unittest.TestCase):
    @staticmethod
    def assert_valid_bash(command):
        subprocess.run(
            ['bash', '-n'],
            input=command,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_image_families_cover_x86_64_and_arm64(self):
        self.assertEqual(
            rocky_linux.image_family('X86_64'),
            'rocky-linux-9',
        )
        self.assertEqual(
            rocky_linux.image_family('ARM64'),
            'rocky-linux-9-arm64',
        )
        self.assertEqual(
            rocky_linux.image_family_uri('amd64'),
            'projects/rocky-linux-cloud/global/images/family/rocky-linux-9',
        )
        self.assertEqual(
            rocky_linux.image_family_uri('aarch64'),
            'projects/rocky-linux-cloud/global/images/family/'
            'rocky-linux-9-arm64',
        )
        with self.assertRaisesRegex(ValueError, 'architecture is unsupported'):
            rocky_linux.image_family('riscv64')

    def test_ssh_metadata_uses_an_explicit_isolated_account(self):
        expiration = datetime(2026, 8, 20, 18, tzinfo=timezone.utc)

        items = rocky_linux.ssh_metadata_items(
            'ssh-ed25519 QUFBQQ== local-key-comment',
            expires_at=expiration,
        )

        self.assertEqual(
            items,
            (
                {
                    'key': 'ssh-keys',
                    'value': (
                        'benchmark:ssh-ed25519 QUFBQQ== google-ssh '
                        '{"userName":"benchmark",'
                        '"expireOn":"2026-08-20T18:00:00+0000"}'
                    ),
                },
                {'key': 'block-project-ssh-keys', 'value': 'TRUE'},
                {'key': 'enable-oslogin', 'value': 'FALSE'},
            ),
        )

    def test_ssh_metadata_rejects_unsafe_input(self):
        with self.assertRaisesRegex(ValueError, 'exactly one key'):
            rocky_linux.ssh_metadata_items(
                'ssh-ed25519 QUFBQQ==\nssh-ed25519 QUFBQQ=='
            )
        with self.assertRaisesRegex(ValueError, 'supported OpenSSH'):
            rocky_linux.ssh_metadata_items('ssh-dss QUFBQQ==')
        with self.assertRaisesRegex(ValueError, 'invalid or unsafe'):
            rocky_linux.ssh_metadata_items(
                'ssh-ed25519 QUFBQQ==',
                username='root',
            )
        with self.assertRaisesRegex(ValueError, 'timezone-aware'):
            rocky_linux.ssh_metadata_items(
                'ssh-ed25519 QUFBQQ==',
                expires_at=datetime(2026, 8, 20, 18),
            )

    def test_validation_accepts_the_full_gcp_guest_surface(self):
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
            iperf3_protocols=['tcp', 'udp', 'sctp'],
            phoronix_profiles=phoronix.PROFILES,
            llm_benchmarks=['llama_bench'],
        )

    def test_validation_rejects_unsupported_or_incomplete_selections(self):
        with self.assertRaisesRegex(ValueError, 'not supported on GCP'):
            rocky_linux.validate_benchmark_selection(['unknown'])
        with self.assertRaisesRegex(ValueError, 'Sysbench workload'):
            rocky_linux.validate_benchmark_selection(['sysbench'])
        with self.assertRaisesRegex(ValueError, 'Phoronix profile'):
            rocky_linux.validate_benchmark_selection(['phoronix'])
        with self.assertRaisesRegex(ValueError, 'iperf3 protocol'):
            rocky_linux.validate_benchmark_selection(['iperf3'])
        with self.assertRaisesRegex(ValueError, 'not supported on GCP'):
            rocky_linux.validate_benchmark_selection(
                ['iperf3'],
                iperf3_protocols=['dccp'],
            )
        with self.assertRaisesRegex(ValueError, 'LLM benchmark'):
            rocky_linux.validate_benchmark_selection(
                ['fio'],
                llm_benchmarks=['unknown_llm'],
            )

    def test_readiness_is_bounded_and_selection_aware(self):
        hosts = rocky_linux.benchmark_readiness_hosts([
            'sysbench_fileio',
            'stream',
            'phoronix',
        ], llm_benchmarks=['llama_bench'])
        self.assertEqual(hosts, rocky_linux.DEFAULT_READINESS_HOSTS)

        command = rocky_linux.readiness_command('ARM64', hosts)

        self.assertIn('The GCP benchmark guest must be Rocky Linux 9', command)
        self.assertIn('EXPECTED_ARCH=aarch64', command)
        self.assertIn('mirrors.rockylinux.org', command)
        self.assertIn('openbenchmarking.org', command)
        self.assertIn('huggingface.co', command)
        self.assertIn('for attempt in $(seq 1 24)', command)
        self.assertIn('if [ "$attempt" -lt 24 ]; then sleep 5; fi', command)
        self.assertIn('sudo timeout 60 dnf -q', command)
        self.assertIn('command -v curl', command)
        self.assertNotIn('cdn.amazonlinux.com', command)
        self.assert_valid_bash(command)

    def test_readiness_and_dnf_reject_shell_injection(self):
        with self.assertRaisesRegex(ValueError, 'hostname'):
            rocky_linux.readiness_command('x86_64', ['github.com; id'])
        with self.assertRaisesRegex(ValueError, 'package'):
            rocky_linux.dnf_install_command(['gcc; id'])

    def test_packages_cover_all_supported_workloads_on_stock_rocky(self):
        packages = rocky_linux.benchmark_packages(
            ['sysbench', 'stream', 'fio', 'iperf3', 'phoronix'],
            sysbench_workloads=['cpu', 'memory', 'fileio'],
            iperf3_protocols=['sctp'],
            phoronix_profiles=phoronix.PROFILES,
            llm_benchmarks=['llama_bench'],
            additional_volume=True,
        )

        for package in (
            'fio',
            'iperf3',
            'lksctp-tools',
            'xfsprogs',
            'cmake',
            'gcc-c++',
            'git',
            'libaio-devel',
            'libgomp',
            'php-cli',
            'perl-core',
            'elfutils-libelf-devel',
        ):
            self.assertIn(package, packages)
        self.assertNotIn('curl', packages)
        self.assertNotIn('php8.2-cli', packages)
        self.assertEqual(
            rocky_linux.rocky_phoronix_packages(phoronix.PROFILES),
            phoronix.required_packages(phoronix.PROFILES),
        )

    def test_llama_installs_and_verifies_one_coherent_gcc_toolset(self):
        packages = rocky_linux.benchmark_packages(
            [],
            llm_benchmarks=['llama_bench'],
        )
        self.assertTrue(rocky_linux.LLAMA_TOOLSET_PACKAGES <= packages)
        self.assertIn('gcc-toolset-15-gcc', packages)
        self.assertIn('gcc-toolset-15-gcc-c++', packages)
        self.assertIn('gcc-toolset-15-binutils', packages)
        self.assertIn('gcc-toolset-15-runtime', packages)

        steps = rocky_linux.installation_steps(
            [],
            llm_benchmarks=['llama_bench'],
        )
        self.assertEqual(
            [step.name for step in steps],
            [
                'Rocky Linux benchmark prerequisites',
                'GCC Toolset 15 for llama.cpp',
            ],
        )
        install_command, verify_command = (
            step.command for step in steps
        )
        for package in rocky_linux.LLAMA_TOOLSET_PACKAGES:
            self.assertIn(package, install_command)
        self.assertIn(
            f'source {rocky_linux.LLAMA_TOOLSET_ENABLE}',
            verify_command,
        )
        self.assertIn('for TOOL in gcc g++ as ld', verify_command)
        self.assertIn('g++ -print-prog-name=as', verify_command)
        self.assertIn(
            rocky_linux.LLAMA_TOOLSET_BIN_DIRECTORY,
            verify_command,
        )
        self.assertIn(
            rocky_linux.LLAMA_TOOLSET_ROOT_DIRECTORY,
            verify_command,
        )
        for step in steps:
            self.assert_valid_bash(step.command)

    def test_installation_steps_install_prerequisites_then_pinned_sysbench(self):
        steps = rocky_linux.installation_steps(
            ['sysbench', 'fio'],
            sysbench_workloads=['fileio'],
            additional_volume=True,
        )

        self.assertEqual(
            [step.name for step in steps],
            [
                'Rocky Linux benchmark prerequisites',
                f'sysbench {amazon_linux.SYSBENCH_VERSION}',
            ],
        )
        self.assertIn('sudo timeout 480 dnf -y', steps[0].command)
        self.assertIn('fio', steps[0].command)
        self.assertIn('xfsprogs', steps[0].command)
        self.assertIn(amazon_linux.SYSBENCH_SOURCE_SHA256, steps[1].command)
        for step in steps:
            self.assert_valid_bash(step.command)

    def test_mount_uses_exact_gce_identity_and_persists_uuid(self):
        command = rocky_linux.data_volume_mount_command('benchmark-data')

        self.assertIn(
            'DEVICE_LINK=/dev/disk/by-id/google-benchmark-data',
            command,
        )
        self.assertNotIn('/dev/disk/by-id/google-*', command)
        self.assertIn('Refusing to format or mount the GCP boot disk', command)
        self.assertIn('sudo mkfs.xfs "$DEVICE"', command)
        self.assertNotIn('mkfs.xfs -f', command)
        self.assertIn(
            'UUID=%s /data xfs discard,nofail 0 2 '
            '# cloud-benchmark-data',
            command,
        )
        self.assertIn('findmnt -rn -o UUID', command)
        self.assertIn('GCP_DATA_VOLUME device_link=%s uuid=%s', command)
        self.assertLess(command.index('MOUNTED_SOURCE='), command.index('FSTAB_TMP='))
        self.assert_valid_bash(command)

    def test_mount_rejects_unsafe_device_names_and_owners(self):
        with self.assertRaisesRegex(ValueError, 'device name'):
            rocky_linux.data_volume_mount_command('benchmark-data; id')
        with self.assertRaisesRegex(ValueError, 'owner'):
            rocky_linux.data_volume_mount_command(owner='root')

    def test_peer_startup_supports_tcp_udp_and_sctp(self):
        script = rocky_linux.iperf3_peer_startup_script(
            ['tcp', 'udp', 'sctp']
        )

        self.assertIn('dnf -y', script)
        self.assertIn('--add-port=5201/tcp', script)
        self.assertIn('--add-port=5201/udp', script)
        self.assertIn('--add-port=5201/sctp', script)
        self.assertIn('lksctp-tools', script)
        self.assertIn('kernel-modules-extra-${KERNEL_RELEASE}', script)
        self.assertIn('modprobe sctp', script)
        self.assertIn('/proc/net/sctp', script)
        self.assertNotIn('sudo ', script)
        self.assertIn('ExecStart=/usr/bin/iperf3 -s', script)
        self.assertIn('systemctl enable --now iperf3-server.service', script)
        self.assert_valid_bash(script)

        tcp_only = rocky_linux.iperf3_peer_startup_script(['tcp'])
        self.assertNotIn('5201/udp', tcp_only)
        self.assertNotIn('5201/sctp', tcp_only)
        self.assertNotIn('lksctp-tools', tcp_only)
        self.assert_valid_bash(tcp_only)
        with self.assertRaisesRegex(ValueError, 'unsupported'):
            rocky_linux.iperf3_peer_startup_script(['dccp'])

    def test_benchmark_commands_and_parsers_reuse_strict_result_contracts(self):
        commands = rocky_linux.benchmark_commands(4)

        self.assertEqual(
            set(commands),
            {
                'sysbench_cpu',
                'sysbench_memory',
                'sysbench_fileio',
                'fio',
                'stream',
            },
        )
        self.assertIn('--threads=4', commands['sysbench_cpu'][1])
        self.assertIn('cd /data', commands['sysbench_fileio'][1])
        self.assertIn('--directory=/data', commands['fio'][1])
        self.assertIs(
            rocky_linux.parse_fio_output,
            amazon_linux.parse_fio_output,
        )
        self.assertIs(
            rocky_linux.parse_stream_output,
            amazon_linux.parse_stream_output,
        )
        for _name, command in commands.values():
            self.assert_valid_bash(command)

        local_commands = rocky_linux.benchmark_commands(
            4,
            storage_directory='/benchmark-local',
        )
        self.assertIn(
            'cd /benchmark-local',
            local_commands['sysbench_fileio'][1],
        )
        self.assertIn(
            '--directory=/benchmark-local',
            local_commands['fio'][1],
        )


if __name__ == '__main__':
    unittest.main()
