import unittest
from unittest.mock import patch

from app import main
from app.models import BenchmarkPlan


def oci_plan(**overrides):
    values = {
        'region': 'us-ashburn-1',
        'shape': 'VM.Standard.A1.Flex',
        'ocpus': 2,
        'memory_gb': 16,
        'ssh_private_key': 'private',
        'ssh_public_key': 'public',
        'benchmarks': ['sysbench', 'stream', 'fio'],
        'sysbench': {'workloads': ['cpu', 'memory', 'fileio']},
        'storage': {
            'additional_volume': True,
            'additional_size_gb': 100,
        },
    }
    values.update(overrides)
    return BenchmarkPlan(**values)


def oci_job():
    return {
        'id': 'oci-structured-results',
        'events': [],
        'results': [],
        'resources': {
            'provider': 'oci',
            'architecture': 'aarch64',
            'image_id': 'ocid1.image.oc1..oraclelinux9',
            'image_name': 'Oracle-Linux-9.6-2026.08.01-0',
        },
    }


class OciStructuredResultParityTests(unittest.TestCase):
    def test_core_workloads_use_observed_cpu_and_shared_strict_contracts(self):
        plan = oci_plan()
        job = oci_job()

        def probe(_job, command, **kwargs):
            self.assertEqual(kwargs['timeout'], 60)
            if command == main.llama_cpp.architecture_command():
                return 'aarch64\n'
            if command == main.llama_cpp.logical_cpu_count_command():
                return '4\n'
            self.fail(f'Unexpected SSH command: {command}')

        with (
            patch.object(main, 'wait_for_guest_readiness') as readiness,
            patch.object(main, 'install_benchmark_tools'),
            patch.object(main, 'mount_data_volume') as mount,
            patch.object(main, 'ssh', side_effect=probe) as ssh,
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_oci_benchmarks(job, plan)

        readiness.assert_called_once()
        readiness_hosts = readiness.call_args.kwargs['required_hosts']
        self.assertIn('github.com', readiness_hosts)
        self.assertIn('codeload.github.com', readiness_hosts)
        self.assertIn('raw.githubusercontent.com', readiness_hosts)
        mount.assert_called_once_with(job)
        self.assertEqual(
            [call.args[1] for call in ssh.call_args_list],
            ['uname -m', 'nproc'],
        )

        calls = {call.args[1]: call for call in execute.call_args_list}
        self.assertEqual(
            set(calls),
            {
                'sysbench_cpu',
                'sysbench_memory',
                'sysbench_fileio',
                'stream',
                'fio',
            },
        )
        expected_commands = main.amazon_linux.benchmark_commands(4)
        for benchmark_id, call in calls.items():
            with self.subTest(benchmark_id=benchmark_id):
                self.assertEqual(
                    call.args[2:4],
                    expected_commands[benchmark_id],
                )
                self.assertEqual(
                    call.kwargs['metadata'],
                    {
                        'provider': 'OCI',
                        'region': 'us-ashburn-1',
                        'shape': 'VM.Standard.A1.Flex',
                        'ocpus': 2.0,
                        'memory_gb': 16.0,
                        'architecture': 'aarch64',
                        'logical_cpu_count': 4,
                        'portable_result_contract': 'v1',
                        'image_id': 'ocid1.image.oc1..oraclelinux9',
                        'image_name': 'Oracle-Linux-9.6-2026.08.01-0',
                    },
                )

        self.assertIs(
            calls['sysbench_cpu'].kwargs['parser'],
            main.amazon_linux.parse_sysbench_cpu_output,
        )
        self.assertIs(
            calls['sysbench_memory'].kwargs['parser'],
            main.amazon_linux.parse_sysbench_memory_output,
        )
        self.assertIs(
            calls['sysbench_fileio'].kwargs['parser'],
            main.amazon_linux.parse_sysbench_fileio_output,
        )
        self.assertIs(
            calls['fio'].kwargs['parser'],
            main.amazon_linux.parse_fio_output,
        )
        self.assertIsNone(calls['fio'].kwargs['output_limit'])
        with patch.object(
            main.amazon_linux,
            'parse_stream_output',
            return_value={'triad_mb_per_second': 1},
        ) as stream_parser:
            calls['stream'].kwargs['parser']('STREAM output')
        stream_parser.assert_called_once_with(
            'STREAM output',
            expected_threads=4,
        )

    def test_oracle_install_uses_pinned_sysbench_without_full_curl_rpm(self):
        plan = oci_plan()
        selected = main.expanded_benchmark_ids(plan)
        job = oci_job()

        with (
            patch.object(main, 'dnf_install') as dnf_install,
            patch.object(
                main,
                'ssh',
                return_value=f'sysbench {main.amazon_linux.SYSBENCH_VERSION}',
            ) as ssh,
        ):
            main.install_benchmark_tools(job, plan, selected)

        expected_packages = {
            *main.amazon_linux.STREAM_PACKAGES,
            *main.amazon_linux.SYSBENCH_BUILD_PACKAGES,
            'fio',
        }
        expected_packages.discard('curl')
        dnf_install.assert_called_once_with(job, expected_packages)
        self.assertNotIn('curl', dnf_install.call_args.args[1])
        self.assertNotIn('oracle-epel-release-el9', dnf_install.call_args.args[1])
        ssh.assert_called_once_with(
            job,
            main.amazon_linux.sysbench_install_command(),
            timeout=1800,
            include_stderr=True,
        )

    def test_architecture_mismatch_stops_before_any_benchmark(self):
        plan = oci_plan(
            benchmarks=['stream'],
            sysbench={'workloads': []},
            storage={'additional_volume': False},
        )
        job = oci_job()

        with (
            patch.object(main, 'wait_for_guest_readiness'),
            patch.object(main, 'install_benchmark_tools'),
            patch.object(
                main,
                'ssh',
                side_effect=['x86_64\n', '4\n'],
            ),
            patch.object(main, 'execute_benchmark') as execute,
            self.assertRaisesRegex(RuntimeError, 'does not match'),
        ):
            main.run_oci_benchmarks(job, plan)

        execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
