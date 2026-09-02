import unittest
from types import SimpleNamespace
from unittest.mock import patch

from pydantic import ValidationError

from app import main
from app.models import ApacheBenchOptions, BenchmarkPlan


def options(**overrides):
    values = {
        'workloads': ['new_connections', 'keep_alive'],
        'request_count': 1000,
        'concurrency': 10,
        'response_size_kib': 64,
        'warmup_requests': 10,
        'trials': 2,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def plan(**overrides):
    values = {
        'region': 'us-ashburn-1',
        'shape': 'VM.Standard.A1.Flex',
        'ocpus': 8,
        'memory_gb': 32,
        'benchmarks': ['apachebench'],
        'llm_benchmarks': [],
        'apachebench': options(),
        'sysbench': SimpleNamespace(workloads=['cpu']),
        'iperf3': SimpleNamespace(protocols=['tcp']),
        'phoronix': SimpleNamespace(profiles=['compress_7zip']),
        'storage': SimpleNamespace(additional_volume=False),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def job(**resource_overrides):
    resources = {
        'private_ip': '10.42.1.10',
        'public_ip': '203.0.113.10',
        'loadgen_private_ip': '10.42.1.20',
        'loadgen_public_ip': '203.0.113.20',
        'loadgen_shape': 'VM.Standard.E5.Flex',
        'loadgen_network_bandwidth_gbps': 2.0,
    }
    resources.update(resource_overrides)
    return {
        'id': 'apachebench-unit',
        'events': [],
        'results': [],
        'resources': resources,
    }


class ApacheBenchNetworkTests(unittest.TestCase):
    def test_catalog_exposes_both_connection_modes(self):
        self.assertEqual(
            {
                workload['id']
                for workload in main.catalog()['apachebench_workloads']
            },
            {'new_connections', 'keep_alive'},
        )

    def test_http_ingress_is_private_subnet_only_and_conditional(self):
        ordinary = main.benchmark_security_rules()
        apachebench = main.benchmark_security_rules(
            include_apachebench=True,
        )

        def port_80_rules(rules):
            return [
                rule
                for rule in rules
                if rule.tcp_options
                and rule.tcp_options.destination_port_range.min == 80
                and rule.tcp_options.destination_port_range.max == 80
            ]

        self.assertEqual(port_80_rules(ordinary), [])
        rules = port_80_rules(apachebench)
        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0].source, '10.42.1.0/24')
        self.assertEqual(rules[0].protocol, '6')

    def test_apachebench_and_deathstar_share_load_generator_requirement(self):
        self.assertTrue(main.plan_uses_load_generator(plan()))
        self.assertTrue(
            main.plan_uses_load_generator(
                plan(benchmarks=['deathstarbench'])
            )
        )
        self.assertTrue(
            main.plan_uses_load_generator(
                plan(benchmarks=['apachebench', 'deathstarbench'])
            )
        )
        self.assertFalse(
            main.plan_uses_load_generator(plan(benchmarks=['stream']))
        )

    def test_load_generator_bandwidth_uses_and_clamps_oci_shape_metadata(self):
        flexible = SimpleNamespace(
            shape='VM.Standard.E5.Flex',
            networking_bandwidth_in_gbps=None,
            networking_bandwidth_options=SimpleNamespace(
                default_per_ocpu_in_gbps=1.0,
                min_in_gbps=1.0,
                max_in_gbps=40.0,
            ),
        )
        fixed = SimpleNamespace(
            shape='VM.Standard3.Flex',
            networking_bandwidth_in_gbps=8.0,
            networking_bandwidth_options=None,
        )

        self.assertEqual(main.configured_network_bandwidth_gbps(flexible, 2), 2.0)
        self.assertEqual(main.configured_network_bandwidth_gbps(flexible, 100), 40.0)
        self.assertEqual(main.configured_network_bandwidth_gbps(fixed, 2), 8.0)


class ApacheBenchModelTests(unittest.TestCase):
    def test_defaults_match_the_plan_form(self):
        value = ApacheBenchOptions()

        self.assertEqual(
            value.workloads,
            ['new_connections', 'keep_alive'],
        )
        self.assertEqual(value.request_count, 500000)
        self.assertEqual(value.concurrency, 100)
        self.assertEqual(value.response_size_kib, 64)
        self.assertEqual(value.warmup_requests, 10000)
        self.assertEqual(value.trials, 3)

    def test_rejects_duplicates_and_concurrency_above_request_count(self):
        with self.assertRaisesRegex(ValidationError, 'must be unique'):
            ApacheBenchOptions(
                workloads=['keep_alive', 'keep_alive'],
            )
        with self.assertRaisesRegex(ValidationError, 'cannot exceed'):
            ApacheBenchOptions(request_count=10, concurrency=11)

    def test_selected_benchmark_requires_a_connection_mode(self):
        with self.assertRaisesRegex(ValidationError, 'connection mode'):
            BenchmarkPlan(
                region='us-ashburn-1',
                shape='VM.Standard.E5.Flex',
                ssh_private_key='private',
                ssh_public_key='public',
                benchmarks=['apachebench'],
                apachebench={'workloads': []},
            )


class ApacheBenchOrchestrationTests(unittest.TestCase):
    def test_large_warmup_gets_its_own_request_based_timeout(self):
        self.assertGreater(
            main.apachebench_request_timeout(10_000_000),
            main.apachebench_request_timeout(1000),
        )

    def test_runs_every_trial_on_load_generator_and_aggregates_per_mode(self):
        benchmark_plan = plan()
        target_job = job()
        calls = []

        def fake_ssh(
            _job,
            command,
            timeout=1800,
            host_key='public_ip',
            include_stderr=False,
        ):
            calls.append((host_key, command, timeout, include_stderr))
            if command == 'uname -m':
                architecture = (
                    'x86_64'
                    if host_key == 'loadgen_public_ip'
                    else 'aarch64'
                )
                if include_stderr:
                    return (
                        f'{architecture}\n** The server may need to be '
                        'upgraded. See https://openssh.com/pq.html'
                    )
                return architecture
            return command

        def parsed(output):
            if output.startswith('warm-'):
                return {'requests_per_second': 900.0}
            trial = int(output.rsplit('-', 1)[1])
            return {
                'requests_per_second': 1000.0 + trial,
                'failed_requests': 0,
                'non_2xx_responses': 0,
                'p50_ms': 1.0,
                'p90_ms': 2.0,
                'p95_ms': 3.0,
                'p99_ms': 4.0,
                'load_generator_capacity_used_percent': 50.0,
            }

        def aggregate(trials):
            return {
                'mean_requests_per_second': sum(
                    trial['requests_per_second'] for trial in trials
                ) / len(trials),
                'total_failed_requests': sum(
                    trial['failed_requests'] for trial in trials
                ),
            }

        with (
            patch.object(main, 'wait_for_guest_readiness'),
            patch.object(main, 'dnf_install'),
            patch.object(main, 'ssh', side_effect=fake_ssh),
            patch.object(
                main,
                'apachebench_target_prepare_command',
                side_effect=lambda size, source: f'target-{size}-{source}',
            ),
            patch.object(
                main,
                'apachebench_loadgen_prepare_command',
                return_value='loadgen-prepare',
            ),
            patch.object(
                main,
                'apachebench_readiness_command',
                side_effect=lambda target, size: f'ready-{target}-{size}',
            ),
            patch.object(
                main,
                'apachebench_warmup_command',
                side_effect=lambda mode, target, _options: f'warm-{mode}-{target}',
            ),
            patch.object(
                main,
                'apachebench_command',
                side_effect=lambda mode, target, _options, trial: (
                    f'run-{mode}-{target}-{trial}'
                ),
            ),
            patch.object(
                main,
                'apachebench_workload',
                side_effect=lambda mode: {
                    'name': mode.replace('_', ' ').title(),
                },
            ),
            patch.object(main, 'parse_apachebench_output', side_effect=parsed),
            patch.object(main, 'record_apachebench_network_capacity'),
            patch.object(main, 'validate_apachebench_metrics'),
            patch.object(
                main,
                'aggregate_apachebench_trials',
                side_effect=aggregate,
            ),
            patch.object(
                main,
                'apachebench_metadata',
                side_effect=lambda mode, _options: {'connection_mode': mode},
            ),
        ):
            failures = main.run_apachebench(target_job, benchmark_plan)

        self.assertEqual(failures, ())
        self.assertEqual(
            [result['id'] for result in target_job['results']],
            ['apachebench_new_connections', 'apachebench_keep_alive'],
        )
        for result in target_job['results']:
            self.assertEqual(result['status'], 'completed')
            self.assertEqual(len(result['trial_metrics']), 2)
            self.assertEqual(result['metrics']['mean_requests_per_second'], 1001.5)
            self.assertEqual(result['metadata']['traffic_path'], 'OCI private VCN address')
            self.assertEqual(result['metadata']['service_architecture'], 'aarch64')
            self.assertEqual(
                result['metadata']['load_generator_architecture'],
                'x86_64',
            )
            self.assertIn('--- Trial 1 of 2 ---', result['output'])
            self.assertIn('--- Trial 2 of 2 ---', result['output'])

        architecture_calls = [
            call for call in calls if call[1] == 'uname -m'
        ]
        self.assertEqual(len(architecture_calls), 2)
        self.assertTrue(all(not call[3] for call in architecture_calls))

        load_commands = [
            (host, command)
            for host, command, _, _ in calls
            if command.startswith(('warm-', 'run-'))
        ]
        self.assertEqual(len(load_commands), 6)
        self.assertTrue(
            all(host == 'loadgen_public_ip' for host, _ in load_commands)
        )
        self.assertTrue(
            all('10.42.1.10' in command for _, command in load_commands)
        )
        target_prepare = next(
            command for _, command, _, _ in calls
            if command.startswith('target-')
        )
        self.assertIn('10.42.1.20', target_prepare)
        self.assertIn(
            ('public_ip', 'sudo systemctl stop httpd >/dev/null 2>&1 || true'),
            [(host, command) for host, command, _, _ in calls],
        )

    def test_invalid_private_addresses_create_results_instead_of_aborting(self):
        target_job = job(private_ip='not-an-ip')

        with patch.object(
            main,
            'apachebench_workload',
            side_effect=lambda mode: {'name': mode},
        ):
            failures = main.run_apachebench(target_job, plan())

        self.assertEqual(len(failures), 2)
        self.assertEqual(len(target_job['results']), 2)
        self.assertTrue(
            all(result['status'] == 'failed' for result in target_job['results'])
        )
        self.assertIn('valid private', target_job['results'][0]['error'])

    def test_parser_failure_preserves_raw_trial_output(self):
        benchmark_plan = plan(
            apachebench=options(
                workloads=['new_connections'],
                warmup_requests=0,
                trials=1,
            )
        )
        target_job = job()

        def fake_ssh(
            _job,
            command,
            timeout=1800,
            host_key='public_ip',
            include_stderr=False,
        ):
            if command == 'uname -m':
                return 'x86_64'
            if command == 'measured-command':
                return 'otherwise useful raw ApacheBench output'
            return 'ok'

        with (
            patch.object(main, 'wait_for_guest_readiness'),
            patch.object(main, 'dnf_install'),
            patch.object(main, 'ssh', side_effect=fake_ssh),
            patch.object(main, 'apachebench_target_prepare_command', return_value='target'),
            patch.object(main, 'apachebench_loadgen_prepare_command', return_value='loadgen'),
            patch.object(main, 'apachebench_readiness_command', return_value='ready'),
            patch.object(main, 'apachebench_command', return_value='measured-command'),
            patch.object(main, 'apachebench_workload', return_value={'name': 'New Connections'}),
            patch.object(main, 'apachebench_metadata', return_value={}),
            patch.object(
                main,
                'parse_apachebench_output',
                side_effect=ValueError('missing percentile marker'),
            ),
        ):
            failures = main.run_apachebench(target_job, benchmark_plan)

        self.assertEqual(len(failures), 1)
        result = target_job['results'][0]
        self.assertEqual(result['status'], 'failed')
        self.assertIn('otherwise useful raw ApacheBench output', result['output'])
        self.assertEqual(result['trial_metrics'], [])

    def test_retained_apachebench_target_is_left_running(self):
        benchmark_plan = plan(
            destroy_after_completion=False,
            apachebench=options(
                workloads=['new_connections'],
                warmup_requests=0,
                trials=1,
            ),
        )
        target_job = job()
        calls = []

        def fake_ssh(
            _job,
            command,
            timeout=1800,
            host_key='public_ip',
            include_stderr=False,
        ):
            calls.append(command)
            return 'x86_64' if command == 'uname -m' else command

        with (
            patch.object(main, 'wait_for_guest_readiness'),
            patch.object(main, 'dnf_install'),
            patch.object(main, 'ssh', side_effect=fake_ssh),
            patch.object(main, 'apachebench_target_prepare_command', return_value='target'),
            patch.object(main, 'apachebench_loadgen_prepare_command', return_value='loadgen'),
            patch.object(main, 'apachebench_readiness_command', return_value='ready'),
            patch.object(main, 'apachebench_command', return_value='run'),
            patch.object(
                main,
                'apachebench_workload',
                return_value={'name': 'New Connections'},
            ),
            patch.object(main, 'apachebench_metadata', return_value={}),
            patch.object(
                main,
                'parse_apachebench_output',
                return_value={'requests_per_second': 1000.0},
            ),
            patch.object(main, 'record_apachebench_network_capacity'),
            patch.object(main, 'validate_apachebench_metrics'),
            patch.object(
                main,
                'aggregate_apachebench_trials',
                return_value={'mean_requests_per_second': 1000.0},
            ),
        ):
            failures = main.run_apachebench(target_job, benchmark_plan)

        self.assertEqual(failures, ())
        self.assertNotIn(
            'sudo systemctl stop httpd >/dev/null 2>&1 || true',
            calls,
        )
        self.assertTrue(any(
            event['stage'] == 'ApacheBench retained'
            for event in target_job['events']
        ))

    def test_first_mode_failure_does_not_skip_the_second_mode(self):
        benchmark_plan = plan(
            apachebench=options(warmup_requests=0, trials=1)
        )
        target_job = job()

        def fake_ssh(
            _job,
            command,
            timeout=1800,
            host_key='public_ip',
            include_stderr=False,
        ):
            if command == 'uname -m':
                return 'x86_64'
            return command

        def parse(output):
            if output.startswith('run-new_connections'):
                raise ValueError('invalid first mode')
            return {
                'requests_per_second': 1000.0,
                'keep_alive_requests': 999,
            }

        with (
            patch.object(main, 'wait_for_guest_readiness'),
            patch.object(main, 'dnf_install'),
            patch.object(main, 'ssh', side_effect=fake_ssh),
            patch.object(main, 'apachebench_target_prepare_command', return_value='target'),
            patch.object(main, 'apachebench_loadgen_prepare_command', return_value='loadgen'),
            patch.object(main, 'apachebench_readiness_command', return_value='ready'),
            patch.object(
                main,
                'apachebench_command',
                side_effect=lambda mode, target, _options, trial: (
                    f'run-{mode}-{target}-{trial}'
                ),
            ),
            patch.object(
                main,
                'apachebench_workload',
                side_effect=lambda mode: {
                    'name': mode,
                    'keep_alive': mode == 'keep_alive',
                },
            ),
            patch.object(main, 'apachebench_metadata', return_value={}),
            patch.object(main, 'parse_apachebench_output', side_effect=parse),
            patch.object(main, 'record_apachebench_network_capacity'),
            patch.object(main, 'validate_apachebench_metrics'),
            patch.object(
                main,
                'aggregate_apachebench_trials',
                return_value={'mean_requests_per_second': 1000.0},
            ),
        ):
            failures = main.run_apachebench(target_job, benchmark_plan)

        self.assertEqual(len(failures), 1)
        self.assertEqual(
            [result['status'] for result in target_job['results']],
            ['failed', 'completed'],
        )

    def test_setup_failure_marks_each_mode_and_continues_deathstar(self):
        benchmark_plan = plan(
            benchmarks=['apachebench', 'deathstarbench'],
        )
        target_job = job()

        def fail_install(*_args, **_kwargs):
            raise main.SSHCommandError('dnf failed', output='package output')

        with (
            patch.object(main, 'wait_for_guest_readiness'),
            patch.object(main, 'dnf_install', side_effect=fail_install),
            patch.object(
                main,
                'apachebench_workload',
                side_effect=lambda mode: {'name': mode},
            ),
        ):
            failures = main.run_apachebench(target_job, benchmark_plan)

        self.assertEqual(len(failures), 2)
        self.assertEqual(len(target_job['results']), 2)
        self.assertTrue(
            all(result['status'] == 'failed' for result in target_job['results'])
        )
        self.assertTrue(
            all(
                'package output' in result['output']
                for result in target_job['results']
            )
        )

    def test_apachebench_runs_before_deathstar_and_failure_is_deferred(self):
        benchmark_plan = plan(
            benchmarks=['apachebench', 'deathstarbench'],
        )
        target_job = job()
        order = []

        def run_apache(*_args):
            order.append('apachebench')
            return ('ApacheBench failed',)

        def run_deathstar(*_args):
            order.append('deathstarbench')

        with (
            patch.object(main, 'wait_for_guest_readiness'),
            patch.object(main, 'install_benchmark_tools'),
            patch.object(main, 'run_apachebench', side_effect=run_apache),
            patch.object(main, 'run_deathstarbench', side_effect=run_deathstar),
        ):
            with self.assertRaisesRegex(RuntimeError, 'ApacheBench failed'):
                main.run_benchmarks(target_job, benchmark_plan)

        self.assertEqual(order, ['apachebench', 'deathstarbench'])


if __name__ == '__main__':
    unittest.main()
