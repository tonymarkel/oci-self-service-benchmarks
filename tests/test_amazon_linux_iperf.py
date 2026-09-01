import subprocess
import tempfile
import unittest

from app.guests import amazon_linux


class AmazonLinuxIperfTests(unittest.TestCase):
    def test_validation_accepts_tcp_udp_and_sctp(self):
        amazon_linux.validate_benchmark_selection(
            {'iperf3'},
            iperf3_protocols=('tcp', 'udp', 'sctp'),
        )

        with self.assertRaisesRegex(ValueError, 'not supported on AWS'):
            amazon_linux.validate_benchmark_selection(
                {'iperf3'},
                iperf3_protocols=('dccp',),
            )

    def test_runner_package_plan_installs_iperf3_without_oci_repositories(self):
        packages = amazon_linux.benchmark_packages({'iperf3'})
        steps = amazon_linux.installation_steps({'iperf3'})

        self.assertEqual(packages, {'iperf3'})
        self.assertEqual(len(steps), 1)
        self.assertIn('install iperf3', steps[0].command)
        self.assertNotIn('ol9_', steps[0].command)

        sctp_packages = amazon_linux.benchmark_packages(
            {'iperf3'},
            iperf3_protocols={'sctp'},
        )
        sctp_steps = amazon_linux.installation_steps(
            {'iperf3'},
            iperf3_protocols={'sctp'},
        )
        self.assertEqual(sctp_packages, {'iperf3', 'lksctp-tools'})
        self.assertEqual(len(sctp_steps), 2)
        self.assertIn('lksctp-tools', sctp_steps[0].command)
        self.assertIn('SCTP kernel support', sctp_steps[1].name)
        self.assertIn(
            'kernel${KERNEL_SERIES}-modules-extra-${KERNEL_RELEASE}',
            sctp_steps[1].command,
        )
        self.assertIn('sudo modprobe sctp', sctp_steps[1].command)
        self.assertIn('/proc/net/sctp', sctp_steps[1].command)
        self.assertIn('iperf3 --help', sctp_steps[1].command)
        subprocess.run(
            ['bash', '-n'],
            input=sctp_steps[1].command,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_peer_cloud_init_is_al2023_specific_retried_and_protocol_scoped(self):
        tcp = amazon_linux.iperf3_peer_cloud_init(('tcp',))
        udp = amazon_linux.iperf3_peer_cloud_init(('tcp', 'udp'))
        sctp = amazon_linux.iperf3_peer_cloud_init(('sctp',))

        for script in (tcp, udp, sctp):
            self.assertIn('timeout 480 dnf -y', script)
            self.assertIn('for attempt in $(seq 1 3)', script)
            self.assertIn('systemctl enable --now iperf3-server.service', script)
            self.assertIn('systemctl is-active --quiet', script)
            self.assertIn('Description=AWS benchmark iperf3 server', script)
            self.assertNotIn('ol9_', script)
            with tempfile.NamedTemporaryFile(mode='w') as file:
                file.write(script)
                file.flush()
                subprocess.run(
                    ['bash', '-n', file.name],
                    check=True,
                    capture_output=True,
                    text=True,
                )

        self.assertIn('5201/tcp', tcp)
        self.assertNotIn('5201/udp', tcp)
        self.assertNotIn('5201/sctp', tcp)
        self.assertNotIn('lksctp-tools', tcp)
        self.assertIn('5201/udp', udp)
        self.assertNotIn('5201/sctp', udp)
        self.assertIn('lksctp-tools', sctp)
        self.assertIn('modprobe sctp', sctp)
        self.assertIn('5201/sctp', sctp)
        self.assertNotIn('sudo ', sctp)

        with self.assertRaisesRegex(ValueError, 'unsupported'):
            amazon_linux.iperf3_peer_cloud_init(('dccp',))


if __name__ == '__main__':
    unittest.main()
