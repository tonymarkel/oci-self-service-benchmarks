import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from starlette.requests import Request
from starlette.responses import PlainTextResponse

from app import main
from app.models import BenchmarkPlan


def aws_plan(**overrides):
    values = {
        'provider': 'aws',
        'aws_profile': 'default',
        'region': 'us-east-2',
        'shape': 't3.micro',
        'ocpus': 2,
        'memory_gb': 1,
        'ssh_private_key': 'private',
        'ssh_public_key': 'ssh-ed25519 AAAATEST',
        'storage': {'additional_volume': False},
        'benchmarks': ['sysbench'],
        'sysbench': {'workloads': ['cpu']},
    }
    values.update(overrides)
    return BenchmarkPlan(**values)


class AwsMainDispatchTests(unittest.TestCase):
    @staticmethod
    def request(
        path,
        host,
        *,
        method='GET',
        host_header='127.0.0.1:8000',
        origin=None,
    ):
        headers = [(b'host', host_header.encode())]
        if origin is not None:
            headers.append((b'origin', origin.encode()))
        return Request({
            'type': 'http',
            'http_version': '1.1',
            'method': method,
            'scheme': 'http',
            'path': path,
            'raw_path': path.encode(),
            'query_string': b'',
            'headers': headers,
            'client': (host, 50000),
            'server': ('127.0.0.1', 8000),
        })

    def test_cloud_api_is_available_only_over_loopback(self):
        calls = []

        async def downstream(_request):
            calls.append(True)
            return PlainTextResponse('ok')

        denied = asyncio.run(main.require_loopback_for_api(
            self.request('/api/jobs', '203.0.113.10'),
            downstream,
        ))
        allowed = asyncio.run(main.require_loopback_for_api(
            self.request('/api/jobs', '127.0.0.1'),
            downstream,
        ))
        static = asyncio.run(main.require_loopback_for_api(
            self.request('/static/app.js', '203.0.113.10'),
            downstream,
        ))

        self.assertEqual(denied.status_code, 403)
        self.assertEqual(denied.headers['cache-control'], 'no-store')
        self.assertEqual(allowed.status_code, 200)
        self.assertEqual(static.status_code, 200)
        self.assertEqual(len(calls), 2)

    def test_cloud_api_rejects_dns_rebinding_and_cross_origin_mutations(self):
        calls = []

        async def downstream(_request):
            calls.append(True)
            return PlainTextResponse('ok')

        rebound = asyncio.run(main.require_loopback_for_api(
            self.request(
                '/api/config/ssh-defaults',
                '127.0.0.1',
                host_header='attacker.example:8000',
            ),
            downstream,
        ))
        missing_origin = asyncio.run(main.require_loopback_for_api(
            self.request('/api/jobs', '127.0.0.1', method='POST'),
            downstream,
        ))
        cross_origin = asyncio.run(main.require_loopback_for_api(
            self.request(
                '/api/jobs',
                '127.0.0.1',
                method='POST',
                origin='http://localhost:8000',
            ),
            downstream,
        ))
        same_origin = asyncio.run(main.require_loopback_for_api(
            self.request(
                '/api/jobs',
                '127.0.0.1',
                method='POST',
                origin='http://127.0.0.1:8000',
            ),
            downstream,
        ))

        for denied in (rebound, missing_origin, cross_origin):
            self.assertEqual(denied.status_code, 403)
            self.assertEqual(denied.headers['cache-control'], 'no-store')
        self.assertEqual(same_origin.status_code, 200)
        self.assertEqual(len(calls), 1)

    def test_cloud_api_allows_ipv6_localhost_and_starlette_test_clients(self):
        calls = []

        async def downstream(_request):
            calls.append(True)
            return PlainTextResponse('ok')

        ipv6 = asyncio.run(main.require_loopback_for_api(
            self.request(
                '/api/catalog',
                '::1',
                host_header='[::1]:8000',
            ),
            downstream,
        ))
        localhost = asyncio.run(main.require_loopback_for_api(
            self.request(
                '/api/catalog',
                '127.0.0.1',
                host_header='localhost:8000',
            ),
            downstream,
        ))
        testclient = asyncio.run(main.require_loopback_for_api(
            self.request(
                '/api/jobs',
                'testclient',
                method='POST',
                host_header='testserver',
            ),
            downstream,
        ))

        self.assertEqual(ipv6.status_code, 200)
        self.assertEqual(localhost.status_code, 200)
        self.assertEqual(testclient.status_code, 200)
        self.assertEqual(len(calls), 3)

    def test_provision_dispatches_to_aws_with_only_the_public_key(self):
        plan = aws_plan()
        job = {
            'id': 'aws-dispatch',
            '_public_key': 'ssh-ed25519 AAAATEST',
            'resources': {},
        }

        with patch.object(main.aws_provider, 'provision') as provision:
            main.provision(job, plan)

        provision.assert_called_once_with(
            job,
            plan,
            public_key='ssh-ed25519 AAAATEST',
            emit=main.event,
            persist=main.persist_job_state,
        )

    def test_cleanup_dispatches_from_the_persisted_provider(self):
        job = {
            'id': 'aws-cleanup',
            'plan': {
                'provider': 'aws',
                'aws_profile': 'default',
                'region': 'us-east-2',
            },
            'resources': {},
        }

        with patch.object(main.aws_provider, 'destroy_resources') as cleanup:
            main.destroy_resources(job, preserve_status=True)

        cleanup.assert_called_once_with(
            job,
            emit=main.event,
            persist=main.persist_job_state,
            preserve_status=True,
        )

    def test_aws_run_uses_amazon_linux_setup_and_only_selected_command(self):
        plan = aws_plan()
        job = {
            'id': 'aws-run',
            'plan': plan.model_dump(
                exclude={
                    'ssh_private_key',
                    'ssh_public_key',
                    'ssh_key_passphrase',
                }
            ),
            'events': [],
            'resources': {
                'public_ip': '198.51.100.10',
                'ssh_user': 'ec2-user',
                'architecture': 'x86_64',
                'image_id': 'ami-123',
                'image_name': 'al2023-test',
            },
            'results': [],
        }

        with (
            patch.object(main, 'ssh', return_value='ready') as ssh,
            patch.object(main, 'execute_benchmark') as execute,
            patch.object(main, 'wait_for_guest_readiness') as oci_readiness,
            patch.object(main, 'install_benchmark_tools') as oci_install,
        ):
            main.run_benchmarks(job, plan)

        self.assertGreaterEqual(ssh.call_count, 3)
        self.assertIn('Amazon Linux', job['events'][0]['message'])
        oci_readiness.assert_not_called()
        oci_install.assert_not_called()
        execute.assert_called_once()
        args, kwargs = execute.call_args
        self.assertEqual(args[1:3], ('sysbench_cpu', 'Sysbench — CPU'))
        self.assertIn('--threads=2', args[3])
        self.assertIs(
            kwargs['parser'],
            main.amazon_linux.parse_sysbench_cpu_output,
        )
        self.assertEqual(kwargs['metadata']['provider'], 'AWS')
        self.assertEqual(kwargs['metadata']['image_id'], 'ami-123')
        readiness_command = ssh.call_args_list[0].args[1]
        self.assertIn('codeload.github.com', readiness_command)
        self.assertNotIn('raw.githubusercontent.com', readiness_command)
        self.assertNotIn('openbenchmarking.org', readiness_command)

    def test_aws_memory_and_stream_runs_are_wired_to_strict_parsers(self):
        plan = aws_plan(
            benchmarks=['sysbench', 'stream'],
            sysbench={'workloads': ['memory']},
        )
        job = {
            'id': 'aws-parsers',
            'events': [],
            'resources': {
                'public_ip': '198.51.100.10',
                'ssh_user': 'ec2-user',
                'architecture': 'x86_64',
            },
            'results': [],
        }

        with (
            patch.object(main, 'ssh', return_value='ready') as ssh,
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_benchmarks(job, plan)

        calls = {call.args[1]: call for call in execute.call_args_list}
        self.assertEqual(set(calls), {'sysbench_memory', 'stream'})
        self.assertIs(
            calls['sysbench_memory'].kwargs['parser'],
            main.amazon_linux.parse_sysbench_memory_output,
        )
        self.assertTrue(callable(calls['stream'].kwargs['parser']))
        readiness_command = ssh.call_args_list[0].args[1]
        self.assertIn('codeload.github.com', readiness_command)
        self.assertIn('raw.githubusercontent.com', readiness_command)
        self.assertNotIn('openbenchmarking.org', readiness_command)

    def test_encrypted_key_askpass_cannot_answer_remote_password_prompts(self):
        job = {
            'id': 'ssh-security-test',
            '_key': 'encrypted-private-key',
            '_passphrase': 'test-passphrase',
            'resources': {
                'public_ip': '198.51.100.10',
                'ssh_user': 'ec2-user',
            },
        }
        captured = {}

        def popen(args, **kwargs):
            captured['args'] = args
            captured['env'] = kwargs['env']
            captured['askpass'] = Path(kwargs['env']['SSH_ASKPASS']).read_text()

            class Process:
                pid = 12345
                returncode = 0

                def poll(self):
                    return self.returncode

                def communicate(self, timeout=None):
                    return 'ok\n', ''

            return Process()

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', side_effect=popen),
            ):
                output = main.ssh(job, 'true')
                known_hosts = Path(directory) / job['id'] / 'known_hosts'
                self.assertTrue(known_hosts.exists())
                self.assertEqual(known_hosts.stat().st_mode & 0o777, 0o600)

        self.assertEqual(output, 'ok\n')
        arguments = captured['args']
        for option in (
            'PasswordAuthentication=no',
            'KbdInteractiveAuthentication=no',
            'PreferredAuthentications=publickey',
            'IdentitiesOnly=yes',
            'ForwardAgent=no',
            'ClearAllForwardings=yes',
            'IdentityAgent=none',
            'StrictHostKeyChecking=accept-new',
            'GlobalKnownHostsFile=/dev/null',
            'ServerAliveInterval=30',
            'ServerAliveCountMax=6',
        ):
            self.assertIn(option, arguments)
        self.assertIn('-F', arguments)
        self.assertEqual(arguments[arguments.index('-F') + 1], '/dev/null')
        known_hosts_options = [
            argument for argument in arguments
            if argument.startswith('UserKnownHostsFile=')
        ]
        self.assertEqual(len(known_hosts_options), 1)
        self.assertTrue(known_hosts_options[0].endswith(
            '/ssh-security-test/known_hosts'
        ))
        self.assertNotIn('StrictHostKeyChecking=no', arguments)
        self.assertNotIn('BatchMode=yes', arguments)
        self.assertNotIn('test-passphrase', arguments)
        self.assertEqual(
            captured['env']['OCI_BENCHMARK_SSH_PASSPHRASE'],
            'test-passphrase',
        )
        self.assertIn('$OCI_BENCHMARK_SSH_PASSPHRASE', captured['askpass'])
        self.assertNotIn('test-passphrase', captured['askpass'])

    def test_ssh_reuses_one_known_hosts_file_for_every_job_connection(self):
        job = {
            'id': 'stable-known-hosts',
            '_key': 'private-key',
            '_passphrase': None,
            'resources': {
                'public_ip': '198.51.100.10',
                'ssh_user': 'ec2-user',
            },
        }
        calls = []

        def popen(args, **_kwargs):
            calls.append(args)

            class Process:
                pid = 12345
                returncode = 0

                def poll(self):
                    return self.returncode

                def communicate(self, timeout=None):
                    return 'ok\n', ''

            return Process()

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', side_effect=popen),
            ):
                main.ssh(job, 'first')
                main.ssh(job, 'second')
            expected = (
                f'UserKnownHostsFile='
                f'{Path(directory) / job["id"] / "known_hosts"}'
            )

        self.assertEqual(len(calls), 2)
        self.assertIn(expected, calls[0])
        self.assertIn(expected, calls[1])

    def test_ssh_secret_stdin_uses_only_a_bounded_process_pipe(self):
        job = {
            'id': 'ssh-secret-stdin',
            '_key': 'private-key',
            '_passphrase': None,
            'resources': {
                'public_ip': '198.51.100.10',
                'ssh_user': 'ec2-user',
            },
        }
        secret = 'K10example::agent:fixed-secret-value'
        captured = {}

        class SecretPipe:
            def write(self, value):
                captured['secret'] = value

            def close(self):
                captured['closed'] = True

        class Process:
            pid = 12345
            returncode = 0
            stdin = SecretPipe()

            def poll(self):
                return self.returncode

            def communicate(self, timeout=None):
                return 'ok\n', ''

        def popen(args, **kwargs):
            captured['args'] = args
            captured['kwargs'] = kwargs
            return Process()

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', side_effect=popen),
            ):
                self.assertEqual(
                    main.ssh(job, 'read-secret', secret_stdin=secret),
                    'ok\n',
                )

        self.assertIs(captured['kwargs']['stdin'], main.subprocess.PIPE)
        self.assertEqual(captured['secret'], secret)
        self.assertTrue(captured['closed'])
        self.assertNotIn(secret, captured['args'])
        self.assertNotIn(secret, captured['kwargs']['env'].values())
        with self.assertRaisesRegex(ValueError, 'at most 4096 bytes'):
            main.ssh(job, 'read-secret', secret_stdin='x' * 4097)


class AwsDiscoveryRouteTests(unittest.TestCase):
    def test_all_aws_discovery_responses_disable_http_caching(self):
        cases = (
            (
                main.aws_provider,
                'bootstrap',
                main.aws_bootstrap,
                {'account_id': '123456789012', 'regions': ['us-east-2']},
            ),
            (
                main.aws_provider,
                'placement',
                main.aws_placement,
                {'availability_zones': [{'name': 'us-east-2a'}]},
            ),
            (
                main.aws_provider,
                'instance_types',
                main.aws_instance_types,
                {'items': [{'instance_type': 'm7i.large'}]},
            ),
        )
        for provider, method_name, route, payload in cases:
            with self.subTest(route=route.__name__):
                with patch.object(provider, method_name, return_value=payload):
                    response = route()

                self.assertEqual(response.headers['cache-control'], 'no-store')
                self.assertEqual(json.loads(response.body), payload)

    def test_aws_discovery_errors_also_disable_http_caching(self):
        with (
            patch.object(
                main.aws_provider,
                'bootstrap',
                side_effect=RuntimeError('profile unavailable'),
            ),
            self.assertRaises(main.HTTPException) as raised,
        ):
            main.aws_bootstrap()

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(
            raised.exception.headers['Cache-Control'],
            'no-store',
        )


class BenchmarkFailureAccountingTests(unittest.TestCase):
    @staticmethod
    def job():
        return {
            'id': 'failed-benchmark',
            'events': [],
            'resources': {'public_ip': '198.51.100.10'},
            'results': [],
        }

    def test_silent_ssh_failure_is_recorded_exactly_once(self):
        job = self.job()
        error = main.SSHCommandError(
            'SSH command timed out.',
            output='',
        )

        with (
            patch.object(main, 'ssh', side_effect=error),
            self.assertRaisesRegex(RuntimeError, 'Example failed'),
        ):
            main.execute_benchmark(job, 'example', 'Example', 'benchmark')

        self.assertEqual(len(job['results']), 1)
        self.assertEqual(job['results'][0]['status'], 'failed')
        self.assertEqual(job['results'][0]['output'], '')
        self.assertEqual(main.benchmark_status(job), 'failed')

    def test_benchmark_transport_retry_policy_is_forwarded_to_ssh(self):
        job = self.job()

        with patch.object(main, 'ssh', return_value='measured\n') as ssh:
            main.execute_benchmark(
                job,
                'example',
                'Example',
                'benchmark',
                transport_attempts=1,
            )

        self.assertEqual(ssh.call_count, 1)
        self.assertEqual(ssh.call_args.kwargs['transport_attempts'], 1)

    def test_empty_success_output_is_rejected_and_recorded_once(self):
        job = self.job()

        with (
            patch.object(main, 'ssh', return_value='  \n'),
            self.assertRaisesRegex(RuntimeError, 'without producing'),
        ):
            main.execute_benchmark(job, 'example', 'Example', 'benchmark')

        self.assertEqual(len(job['results']), 1)
        self.assertEqual(job['results'][0]['status'], 'failed')
        self.assertEqual(main.benchmark_status(job), 'failed')

    def test_any_parser_exception_is_recorded_exactly_once(self):
        job = self.job()

        def broken_parser(_output):
            raise RuntimeError('parser bug')

        with (
            patch.object(main, 'ssh', return_value='benchmark output\n'),
            self.assertRaisesRegex(RuntimeError, 'invalid result'),
        ):
            main.execute_benchmark(
                job,
                'example',
                'Example',
                'benchmark',
                parser=broken_parser,
            )

        self.assertEqual(len(job['results']), 1)
        self.assertEqual(job['results'][0]['status'], 'failed')
        self.assertEqual(job['results'][0]['error'], 'parser bug')
        self.assertEqual(main.benchmark_status(job), 'failed')


class DurableStateTests(unittest.TestCase):
    def test_destroy_preserves_interrupted_partial_benchmark_outcome(self):
        job = {
            'id': 'interrupted-partial-aws',
            'status': 'testing',
            'plan': {
                'provider': 'aws',
                'aws_profile': 'default',
                'region': 'us-east-2',
                'shape': 't3.micro',
                'benchmarks': ['sysbench', 'stream'],
            },
            'events': [],
            'resources': {'aws_vpc_id': 'vpc-123'},
            'results': [{
                'id': 'sysbench_cpu',
                'name': 'Sysbench — CPU',
                'status': 'completed',
            }],
            'created_at': main.now(),
            'updated_at': main.now(),
        }
        scheduled = []

        def create_task(coroutine):
            scheduled.append(coroutine)
            coroutine.close()
            task = SimpleNamespace(exception=lambda: None)
            task.add_done_callback = lambda callback: callback(task)
            return task

        with tempfile.TemporaryDirectory() as directory:
            run_directory = Path(directory) / job['id']
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.dict(main.jobs, {}, clear=True),
                patch.object(main.asyncio, 'create_task', side_effect=create_task),
            ):
                main.persist_job_state(job)
                self.assertEqual(
                    main.read_json_file(run_directory / 'state.json', {})[
                        'benchmark_status'
                    ],
                    'pending',
                )

                response = asyncio.run(main.destroy(job['id']))
                destroying = main.read_json_file(
                    run_directory / 'state.json',
                    {},
                )
                resumed = main.load_persisted_job(job['id'])
                resumed['status'] = 'destroyed'
                main.persist_job_state(resumed)
                summary = main.run_summary(run_directory)

        self.assertEqual(response, {'status': 'destroying'})
        self.assertTrue(destroying['benchmark_interrupted'])
        self.assertEqual(destroying['benchmark_status'], 'interrupted')
        self.assertEqual(summary['status'], 'destroyed')
        self.assertEqual(summary['benchmark_status'], 'interrupted')
        self.assertEqual(len(scheduled), 1)

    def test_benchmark_status_requires_every_selected_result(self):
        result_ids = [
            'sysbench_cpu',
            'sysbench_memory',
            'sysbench_fileio',
            'stream',
            'fio',
            'iperf_tcp',
            'iperf_udp',
            'phoronix_compress_7zip',
            'phoronix_openssl',
            'phoronix_build_linux_kernel',
        ]
        job = {
            'status': 'testing',
            'plan': {
                'provider': 'aws',
                'benchmarks': [
                    'sysbench',
                    'stream',
                    'fio',
                    'iperf3',
                    'phoronix',
                ],
                'sysbench': {'workloads': ['cpu', 'memory', 'fileio']},
                'iperf3': {'protocols': ['tcp', 'udp']},
                'phoronix': {'profiles': [
                    'compress_7zip',
                    'openssl',
                    'build_linux_kernel',
                    'tinymembench',
                ]},
            },
            'results': [
                {'id': result_id, 'status': 'completed'}
                for result_id in result_ids
            ],
        }

        self.assertEqual(main.benchmark_status(job), 'pending')

        job['results'].append({
            'id': 'phoronix_tinymembench',
            'status': 'completed',
        })
        self.assertEqual(main.benchmark_status(job), 'complete')
        for status in ('reporting', 'destroying', 'destroyed'):
            job['status'] = status
            self.assertEqual(main.benchmark_status(job), 'complete')

        job['results'].pop()
        job['error'] = 'The app stopped before Tinymembench completed.'
        self.assertEqual(main.benchmark_status(job), 'failed')

    def test_genuine_report_failure_is_not_migrated_to_completed(self):
        saved = {
            'status': 'failed',
            'benchmark_status': 'complete',
            'error': 'Unable to write report.html.',
            'events': [{
                'stage': 'Report warning',
                'message': 'Unable to write report.html.',
            }],
        }

        normalized = main.normalize_saved_job_state(saved)

        self.assertEqual(normalized['status'], 'failed')
        self.assertEqual(normalized['error'], 'Unable to write report.html.')
        self.assertNotIn('lifecycle_warning', normalized)

    def test_legacy_cleanup_failure_is_still_migrated(self):
        saved = {
            'status': 'failed',
            'benchmark_status': 'complete',
            'error': 'Unable to delete VPC.',
            'events': [{
                'stage': 'Cleanup warning',
                'message': 'Unable to delete VPC.',
            }],
        }

        normalized = main.normalize_saved_job_state(saved)

        self.assertEqual(normalized['status'], 'cleanup_failed')
        self.assertIsNone(normalized['error'])
        self.assertEqual(normalized['cleanup_error'], 'Unable to delete VPC.')

    def test_response_lost_aws_create_is_recoverable_without_an_id(self):
        job = {
            'id': 'response-lost-vpc-create',
            'status': 'provisioning',
            'plan': {
                'provider': 'aws',
                'aws_profile': 'default',
                'region': 'us-east-2',
                'shape': 't3.micro',
            },
            'events': [],
            'resources': {
                'provider': 'aws',
                'aws_profile': 'default',
                'aws_account_id': '123456789012',
                'region': 'us-east-2',
            },
            'results': [],
            'created_at': main.now(),
            'updated_at': main.now(),
        }

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.dict(main.jobs, {}, clear=True),
            ):
                main.persist_job_state(job)
                status = main.job_status(job['id'])
                summary = main.run_summary(Path(directory) / job['id'])

        self.assertEqual(status['status'], 'interrupted')
        self.assertTrue(status['recoverable'])
        self.assertTrue(summary['recoverable'])

    def test_destroyed_pre_result_failure_summarizes_as_failed(self):
        job = {
            'id': 'failed-before-results-aws',
            'status': 'destroyed',
            'error': 'Amazon Linux package installation failed.',
            'plan': {
                'provider': 'aws',
                'aws_profile': 'default',
                'region': 'us-east-2',
                'shape': 't3.micro',
            },
            'events': [],
            'resources': {},
            'results': [],
            'created_at': main.now(),
            'updated_at': main.now(),
        }

        with tempfile.TemporaryDirectory() as directory:
            with patch.object(main, 'RUNS', Path(directory)):
                main.persist_job_state(job)
                summary = main.run_summary(Path(directory) / job['id'])

        self.assertEqual(summary['status'], 'destroyed')
        self.assertEqual(summary['benchmark_status'], 'failed')

    def test_destroy_resumes_persisted_destroying_cleanup_after_restart(self):
        job = {
            'id': 'resume-destroying-aws',
            'status': 'destroying',
            'plan': {
                'provider': 'aws',
                'aws_profile': 'default',
                'region': 'us-east-2',
                'shape': 't3.micro',
            },
            'events': [],
            'resources': {'aws_vpc_id': 'vpc-123'},
            'results': [],
            'created_at': main.now(),
            'updated_at': main.now(),
        }
        scheduled = []

        def create_task(coroutine):
            scheduled.append(coroutine)
            coroutine.close()
            task = SimpleNamespace(exception=lambda: None)
            task.add_done_callback = lambda callback: callback(task)
            return task

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.dict(main.jobs, {}, clear=True),
                patch.object(main.asyncio, 'create_task', side_effect=create_task),
            ):
                main.persist_job_state(job)
                response = asyncio.run(main.destroy(job['id']))
                saved = main.read_json_file(
                    Path(directory) / job['id'] / 'state.json',
                    {},
                )

        self.assertEqual(response, {'status': 'destroying'})
        self.assertEqual(saved['status'], 'destroying')
        self.assertEqual(len(scheduled), 1)

    def test_state_is_atomic_and_available_before_a_report_exists(self):
        job = {
            'id': 'partial-provision',
            'status': 'provisioning',
            'plan': {
                'provider': 'aws',
                'aws_profile': 'default',
                'region': 'us-east-2',
                'shape': 't3.micro',
            },
            'events': [],
            'resources': {'aws_vpc_id': 'vpc-123'},
            'results': [],
            'created_at': main.now(),
            'updated_at': main.now(),
        }
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(main, 'RUNS', Path(directory)):
                main.persist_job_state(job)
                run_directory = Path(directory) / job['id']
                saved = main.read_json_file(run_directory / 'state.json', {})

        self.assertEqual(saved['status'], 'provisioning')
        self.assertEqual(saved['resources']['aws_vpc_id'], 'vpc-123')
        self.assertFalse(any(run_directory.glob('.state-*.tmp')))

    def test_restart_exposes_recorded_resources_as_recoverable(self):
        job = {
            'id': 'interrupted-aws',
            'status': 'provisioning',
            'plan': {
                'provider': 'aws',
                'aws_profile': 'default',
                'region': 'us-east-2',
                'shape': 't3.micro',
            },
            'events': [],
            'resources': {'aws_vpc_id': 'vpc-123'},
            'results': [],
        }
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.dict(main.jobs, {}, clear=True),
            ):
                main.persist_job_state(job)
                response = main.job_status(job['id'])

        self.assertEqual(response['status'], 'interrupted')
        self.assertTrue(response['recoverable'])
        self.assertFalse(response['report_ready'])
        self.assertNotIn('_persist_state', response)

    def test_state_only_run_is_discoverable_and_selected_as_latest(self):
        job = {
            'id': 'interrupted-history-aws',
            'status': 'testing',
            'plan': {
                'provider': 'aws',
                'aws_profile': 'default',
                'region': 'us-east-2',
                'shape': 't3.micro',
                'ocpus': 2,
                'memory_gb': 1,
                'benchmarks': ['sysbench'],
            },
            'events': [{
                'at': '2026-08-17T12:00:00+00:00',
                'stage': 'Test',
                'message': 'Running Sysbench.',
            }],
            'resources': {'aws_vpc_id': 'vpc-123'},
            'results': [],
            'created_at': '2026-08-17T11:59:00+00:00',
            'updated_at': '2026-08-17T12:00:00+00:00',
        }
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(main, 'RUNS', Path(directory)):
                main.persist_job_state(job)
                summaries = main.saved_report_summaries()
                latest = main.latest_report()

        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0]['id'], job['id'])
        self.assertEqual(summaries[0]['status'], 'interrupted')
        self.assertFalse(summaries[0]['report_ready'])
        self.assertTrue(summaries[0]['recoverable'])
        self.assertEqual(summaries[0]['provider'], 'aws')
        self.assertEqual(latest, {'id': job['id']})


if __name__ == '__main__':
    unittest.main()
