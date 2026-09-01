import subprocess
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
