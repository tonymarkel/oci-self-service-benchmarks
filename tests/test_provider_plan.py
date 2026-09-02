import unittest

from pydantic import ValidationError

from app.models import BenchmarkPlan


def plan_values(**overrides):
    values = {
        'region': 'us-ashburn-1',
        'shape': 'VM.Standard.E5.Flex',
        'ssh_private_key': 'private-key',
        'ssh_public_key': 'ssh-ed25519 public-key',
        'benchmarks': ['sysbench'],
        'sysbench': {'workloads': ['cpu']},
    }
    values.update(overrides)
    return values


class ProviderPlanTests(unittest.TestCase):
    def test_legacy_plan_defaults_to_oci(self):
        plan = BenchmarkPlan(**plan_values())

        self.assertEqual(plan.provider, 'oci')
        self.assertEqual(plan.aws_profile, 'default')
        self.assertTrue(plan.storage.additional_volume)

    def test_oci_rejects_benchmarks_outside_its_advertised_capabilities(self):
        with self.assertRaisesRegex(
            ValidationError,
            'OCI does not support the selected benchmark: unreleased',
        ):
            BenchmarkPlan(**plan_values(benchmarks=['unreleased']))

    def test_oci_rejects_llm_ids_outside_its_advertised_capabilities(self):
        with self.assertRaisesRegex(
            ValidationError,
            'OCI does not support the selected LLM benchmark: unreleased_llm',
        ):
            BenchmarkPlan(**plan_values(
                benchmarks=[],
                llm_benchmarks=['unreleased_llm'],
            ))

    def test_aws_plan_uses_safe_storage_default(self):
        values = plan_values(
            provider='aws',
            region='us-east-2',
            shape='m7i.2xlarge',
        )
        plan = BenchmarkPlan(**values)

        self.assertEqual(plan.provider, 'aws')
        self.assertEqual(plan.aws_profile, 'default')
        self.assertFalse(plan.storage.additional_volume)

    def test_aws_instance_type_can_populate_compatibility_shape(self):
        values = plan_values(
            provider='aws',
            region='us-east-2',
            shape=None,
            instance_type='m7i.2xlarge',
        )
        plan = BenchmarkPlan(**values)

        self.assertEqual(plan.shape, 'm7i.2xlarge')

    def test_aws_accepts_cpu_memory_benchmark_subset(self):
        plan = BenchmarkPlan(**plan_values(
            provider='aws',
            region='us-east-2',
            shape='m7i.2xlarge',
            benchmarks=['sysbench', 'stream', 'phoronix'],
            sysbench={'workloads': ['cpu', 'memory']},
            phoronix={'profiles': ['openssl', 'tinymembench']},
        ))

        self.assertEqual(plan.benchmarks, ['sysbench', 'stream', 'phoronix'])

    def test_aws_accepts_web_network_and_llm_parity_surface(self):
        plan = BenchmarkPlan(**plan_values(
            provider='aws',
            region='us-east-2',
            shape='m7i.2xlarge',
            benchmarks=['deathstarbench', 'apachebench', 'iperf3'],
            iperf3={'protocols': ['sctp']},
            llm_benchmarks=['llama_bench'],
            memory_gb=32,
        ))

        self.assertEqual(plan.iperf3.protocols, ['sctp'])
        self.assertEqual(plan.llm_benchmarks, ['llama_bench'])

    def test_aws_rejects_unknown_benchmark(self):
        with self.assertRaisesRegex(
            ValidationError,
            'does not support the selected benchmark: unreleased',
        ):
            BenchmarkPlan(**plan_values(
                provider='aws',
                region='us-east-2',
                shape='m7i.2xlarge',
                benchmarks=['unreleased'],
                memory_gb=32,
            ))

    def test_aws_storage_benchmarks_require_and_accept_a_data_volume(self):
        with self.assertRaisesRegex(ValidationError, 'additional /data volume'):
            BenchmarkPlan(**plan_values(
                provider='aws',
                region='us-east-2',
                shape='m7i.2xlarge',
                sysbench={'workloads': ['fileio']},
                storage={'additional_volume': False},
            ))

        plan = BenchmarkPlan(**plan_values(
            provider='aws',
            region='us-east-2',
            shape='m7i.2xlarge',
            benchmarks=['sysbench', 'fio'],
            sysbench={'workloads': ['fileio']},
            storage={
                'additional_volume': True,
                'additional_size_gb': 250,
            },
        ))

        self.assertTrue(plan.storage.additional_volume)
        self.assertEqual(plan.storage.additional_size_gb, 250)
        self.assertEqual(plan.benchmarks, ['sysbench', 'fio'])

    def test_aws_accepts_llama_as_the_only_selection(self):
        plan = BenchmarkPlan(**plan_values(
            provider='aws',
            region='us-east-2',
            shape='m7i.2xlarge',
            benchmarks=[],
            llm_benchmarks=['llama_bench'],
        ))

        self.assertEqual(plan.llm_benchmarks, ['llama_bench'])

    def test_gcp_plan_requires_project_and_zone(self):
        with self.assertRaisesRegex(ValidationError, 'project ID is required'):
            BenchmarkPlan(**plan_values(
                provider='gcp',
                region='us-east1',
                shape='n2-standard-8',
            ))
        with self.assertRaisesRegex(ValidationError, 'zone is required'):
            BenchmarkPlan(**plan_values(
                provider='gcp',
                gcp_project_id='benchmark-project',
                region='us-east1',
                shape='n2-standard-8',
            ))

    def test_gcp_uses_safe_storage_default_and_machine_type_alias(self):
        values = plan_values(
            provider='gcp',
            gcp_project_id='benchmark-project',
            gcp_zone='us-east1-b',
            region='us-east1',
            shape=None,
            machine_type='n2-standard-8',
        )
        plan = BenchmarkPlan(**values)

        self.assertEqual(plan.shape, 'n2-standard-8')
        self.assertFalse(plan.storage.additional_volume)

    def test_gcp_accepts_the_curated_compute_benchmark_matrix(self):
        plan = BenchmarkPlan(**plan_values(
            provider='gcp',
            gcp_project_id='benchmark-project',
            gcp_zone='us-east1-b',
            region='us-east1',
            shape='n2-standard-8',
            benchmarks=['sysbench', 'stream', 'fio', 'iperf3', 'phoronix'],
            sysbench={'workloads': ['cpu', 'memory', 'fileio']},
            iperf3={'protocols': ['tcp', 'udp']},
            phoronix={'profiles': ['openssl', 'tinymembench']},
            storage={
                'additional_volume': True,
                'additional_size_gb': 250,
            },
        ))

        self.assertEqual(plan.provider, 'gcp')
        self.assertEqual(plan.iperf3.protocols, ['tcp', 'udp'])

    def test_gcp_accepts_web_sctp_and_llm_parity_surface(self):
        gcp = {
            'provider': 'gcp',
            'gcp_project_id': 'benchmark-project',
            'gcp_zone': 'us-east1-b',
            'region': 'us-east1',
            'shape': 'n2-standard-8',
        }
        plan = BenchmarkPlan(**plan_values(
            **gcp,
            benchmarks=['deathstarbench', 'apachebench', 'iperf3'],
            iperf3={'protocols': ['sctp']},
            llm_benchmarks=['llama_bench'],
        ))

        self.assertEqual(plan.iperf3.protocols, ['sctp'])
        self.assertEqual(plan.llm_benchmarks, ['llama_bench'])

    def test_gcp_rejects_unknown_benchmark(self):
        gcp = {
            'provider': 'gcp',
            'gcp_project_id': 'benchmark-project',
            'gcp_zone': 'us-east1-b',
            'region': 'us-east1',
            'shape': 'n2-standard-8',
        }
        with self.assertRaisesRegex(
            ValidationError,
            'does not support the selected benchmark: unreleased',
        ):
            BenchmarkPlan(**plan_values(
                **gcp,
                benchmarks=['unreleased'],
            ))

    def test_azure_requires_subscription_and_zone(self):
        with self.assertRaisesRegex(
            ValidationError,
            'subscription ID is required',
        ):
            BenchmarkPlan(**plan_values(
                provider='azure',
                region='eastus2',
                shape='Standard_D8s_v6',
            ))
        with self.assertRaisesRegex(ValidationError, 'zone is required'):
            BenchmarkPlan(**plan_values(
                provider='azure',
                azure_subscription_id='00000000-0000-0000-0000-000000000000',
                region='eastus2',
                shape='Standard_D8s_v6',
            ))

    def test_azure_uses_safe_storage_default_and_vm_size_alias(self):
        plan = BenchmarkPlan(**plan_values(
            provider='azure',
            azure_subscription_id='00000000-0000-0000-0000-000000000000',
            azure_zone='1',
            region='eastus2',
            shape=None,
            vm_size='Standard_D8ps_v6',
        ))

        self.assertEqual(plan.shape, 'Standard_D8ps_v6')
        self.assertFalse(plan.storage.additional_volume)

    def test_azure_accepts_full_surface_except_sctp(self):
        azure = {
            'provider': 'azure',
            'azure_subscription_id': (
                '00000000-0000-0000-0000-000000000000'
            ),
            'azure_zone': '1',
            'region': 'eastus2',
            'shape': 'Standard_D8ps_v6',
        }
        plan = BenchmarkPlan(**plan_values(
            **azure,
            benchmarks=[
                'deathstarbench',
                'apachebench',
                'sysbench',
                'stream',
                'fio',
                'iperf3',
                'phoronix',
            ],
            sysbench={'workloads': ['cpu', 'memory', 'fileio']},
            iperf3={'protocols': ['tcp', 'udp']},
            storage={'additional_volume': True},
            llm_benchmarks=['llama_bench'],
        ))

        self.assertEqual(plan.provider, 'azure')
        self.assertEqual(plan.iperf3.protocols, ['tcp', 'udp'])

        with self.assertRaisesRegex(ValidationError, 'does not support.*SCTP'):
            BenchmarkPlan(**plan_values(
                **azure,
                benchmarks=['iperf3'],
                iperf3={'protocols': ['sctp']},
            ))


if __name__ == '__main__':
    unittest.main()
