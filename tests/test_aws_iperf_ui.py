import unittest
from pathlib import Path

from app.models import BenchmarkPlan
from app.providers.registry import provider_adapter


ROOT = Path(__file__).resolve().parents[1]
JAVASCRIPT = (ROOT / 'app/static/app.js').read_text()


def values(**overrides):
    plan = {
        'provider': 'aws',
        'aws_profile': 'default',
        'region': 'us-east-2',
        'shape': 'm7i.large',
        'ocpus': 2,
        'memory_gb': 8,
        'ssh_private_key': 'private',
        'ssh_public_key': 'ssh-ed25519 AAAATEST',
        'storage': {'additional_volume': False},
        'benchmarks': ['iperf3'],
        'iperf3': {'protocols': ['tcp', 'udp']},
    }
    plan.update(overrides)
    return plan


class AwsIperfPlanAndUiTests(unittest.TestCase):
    def test_plan_accepts_tcp_udp_and_sctp(self):
        plan = BenchmarkPlan(**values(
            iperf3={'protocols': ['tcp', 'udp', 'sctp']},
        ))

        self.assertEqual(plan.iperf3.protocols, ['tcp', 'udp', 'sctp'])

    def test_ui_enables_all_iperf_protocols_on_aws(self):
        capabilities = provider_adapter('aws').capabilities
        self.assertIn('iperf3', capabilities.benchmark_ids)
        self.assertEqual(
            capabilities.iperf3_protocols,
            ('tcp', 'udp', 'sctp'),
        )
        self.assertIn(
            "providerCapabilitySet('iperf3_protocols')",
            JAVASCRIPT,
        )
        self.assertNotIn('awsSupportedIperf3Protocols', JAVASCRIPT)
        self.assertIn('same-type EC2 instance', JAVASCRIPT)
        self.assertIn('TCP, UDP, and SCTP tests', JAVASCRIPT)


if __name__ == '__main__':
    unittest.main()
