import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app import main
from app.guests import web
from app.models import BenchmarkPlan


def llama_toolchain_output(*, toolset_version=None):
    prefix = (
        f'/opt/rh/gcc-toolset-{toolset_version}/root/usr/bin'
        if toolset_version
        else '/usr/bin'
    )
    compiler_version = {
        15: '15.2.1',
        12: '12.3.1',
    }.get(toolset_version, '11.5.0')
    assembler_version = {
        15: '2.44',
        12: '2.38',
    }.get(toolset_version, '2.35.2')
    return (
        f'LLAMA_COMPILER_PATH={prefix}/gcc\n'
        f'LLAMA_COMPILER_VERSION={compiler_version}\n'
        f'LLAMA_CXX_COMPILER_PATH={prefix}/g++\n'
        f'LLAMA_CXX_COMPILER_VERSION={compiler_version}\n'
        f'LLAMA_ASSEMBLER_PATH={prefix}/as\n'
        f'LLAMA_ASSEMBLER_VERSION=GNU assembler version {assembler_version}\n'
    )


def web_options(**overrides):
    values = {
        'workloads': ['new_connections'],
        'request_count': 100,
        'concurrency': 10,
        'response_size_kib': 1,
        'warmup_requests': 0,
        'trials': 1,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def deathstar_options(**overrides):
    values = {
        'workload': 'hotel_reservation',
        'warmup_seconds': 1,
        'duration_seconds': 1,
        'threads': 1,
        'connections': 1,
        'request_rate': 1,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def shared_plan(provider, benchmarks):
    return SimpleNamespace(
        provider=provider,
        region={
            'oci': 'us-ashburn-1',
            'aws': 'us-east-2',
            'gcp': 'us-east1',
        }[provider],
        shape={
            'oci': 'VM.Standard.A1.Flex',
            'aws': 'c7g.4xlarge',
            'gcp': 'c4a-standard-16',
        }[provider],
        ocpus=16,
        memory_gb=32,
        benchmarks=list(benchmarks),
        llm_benchmarks=[],
        apachebench=web_options(),
        deathstarbench=deathstar_options(),
        sysbench=SimpleNamespace(workloads=['cpu']),
        iperf3=SimpleNamespace(protocols=['tcp']),
        phoronix=SimpleNamespace(profiles=['compress_7zip']),
        storage=SimpleNamespace(additional_volume=False),
        destroy_after_completion=True,
    )


def shared_job(provider):
    return {
        'id': f'{provider}-web-runtime',
        'events': [],
        'results': [],
        'resources': {
            'provider': provider,
            'public_ip': '203.0.113.10',
            'private_ip': '10.42.1.10',
            'architecture': 'aarch64',
            'loadgen_public_ip': '203.0.113.20',
            'loadgen_private_ip': '10.42.1.20',
            'loadgen_shape': {
                'oci': 'VM.Standard.E5.Flex',
                'aws': 'c7i.xlarge',
                'gcp': 'n2-custom-4-8192',
            }[provider],
            'loadgen_architecture': 'x86_64',
            'loadgen_vcpus': 4,
            'loadgen_memory_gb': 8,
            'loadgen_network_bandwidth_gbps': 2.0,
        },
    }


class WebGuestContractTests(unittest.TestCase):
    def test_provider_readiness_and_apache_packages_are_distribution_specific(self):
        aws_readiness = web.readiness_command(
            'aws', 'apachebench', 'service', region='us-east-2'
        )
        gcp_readiness = web.readiness_command(
            'gcp',
            'apachebench',
            'service',
            region='us-east1',
            expected_architecture='aarch64',
        )
        aws_target = web.apachebench_install_steps('aws', 'service')[0].command
        gcp_loadgen = web.apachebench_install_steps('gcp', 'loadgen')[0].command

        self.assertIn('cdn.amazonlinux.com', aws_readiness)
        self.assertIn('mirrors.rockylinux.org', gcp_readiness)
        self.assertIn('EXPECTED_ARCH=aarch64', gcp_readiness)
        self.assertIn('install firewalld httpd', aws_target)
        self.assertIn('install httpd-tools time', gcp_loadgen)
        self.assertNotIn('ol9_', aws_target + gcp_loadgen)

    def test_aws_deathstar_uses_spal_and_checksum_pinned_compose(self):
        steps = web.deathstarbench_install_steps('aws', 'service')
        combined = '\n'.join(step.command for step in steps)

        self.assertIn(web.MINIMUM_SPAL_SYSTEM_RELEASE, combined)
        self.assertIn('install spal-release', combined)
        self.assertIn('install podman python3-dotenv', combined)
        self.assertIn(web.PODMAN_COMPOSE_URL, combined)
        self.assertIn(web.PODMAN_COMPOSE_SHA256, combined)
        self.assertIn('/usr/local/bin/podman-compose', combined)
        self.assertNotIn(' install curl', combined)
        for step in steps:
            subprocess.run(
                ['bash', '-n'],
                input=step.command,
                check=True,
                capture_output=True,
                text=True,
            )

    def test_gcp_deathstar_uses_crb_epel_and_packaged_compose(self):
        steps = web.deathstarbench_install_steps('gcp', 'service')
        combined = '\n'.join(step.command for step in steps)

        self.assertIn('config-manager --set-enabled crb', combined)
        self.assertIn('install epel-release', combined)
        self.assertIn('install podman-compose', combined)
        self.assertNotIn(web.PODMAN_COMPOSE_URL, combined)
        self.assertEqual(web.podman_compose_path('gcp'), '/usr/bin/podman-compose')

    def test_runtime_metadata_uses_provider_vcpu_and_private_path_vocabulary(self):
        plan = shared_plan('aws', ['apachebench'])
        metadata = web.runtime_metadata(
            plan,
            shared_job('aws')['resources'],
            service_architecture='aarch64',
            loadgen_architecture='x86_64',
        )

        self.assertEqual(metadata['provider'], 'AWS')
        self.assertEqual(metadata['service_vcpus'], 16)
        self.assertEqual(metadata['load_generator_vcpus'], 4)
        self.assertEqual(metadata['traffic_path'], 'AWS private VPC address')
        self.assertNotIn('service_ocpus', metadata)


class SharedWebRunnerTests(unittest.TestCase):
    APACHE_METRICS = {
        'requests_per_second': 1000.0,
        'failed_requests': 0,
        'non_2xx_responses': 0,
        'p50_ms': 1.0,
        'p90_ms': 2.0,
        'p95_ms': 3.0,
        'p99_ms': 4.0,
        'load_generator_capacity_used_percent': 25.0,
    }
    WRK_METRICS = (
        'Sent 100 requests\n'
        'OCI_DSB_METRICS duration_seconds=1.000000 total_requests=100 '
        'throughput_requests_per_second=100.000000 errors=0 '
        'error_rate_percent=0.000000 socket_errors=0 connect_errors=0 '
        'read_errors=0 write_errors=0 timeout_errors=0 p50_ms=1.000000 '
        'p95_ms=2.000000 p99_ms=3.000000\n'
    )

    def test_apachebench_preprovision_failures_keep_static_metadata(self):
        plan = shared_plan('aws', ['apachebench'])
        plan.apachebench = web_options(
            workloads=['new_connections', 'keep_alive'],
        )
        job = shared_job('aws')
        for key in (
            'private_ip',
            'architecture',
            'loadgen_architecture',
        ):
            job['resources'].pop(key, None)

        failures = main.run_apachebench(job, plan)

        self.assertEqual(len(failures), 2)
        self.assertEqual(
            {result['id'] for result in job['results']},
            {'apachebench_new_connections', 'apachebench_keep_alive'},
        )
        for result in job['results']:
            metadata = result['metadata']
            self.assertEqual(metadata['provider'], 'AWS')
            self.assertEqual(metadata['service_shape'], 'c7g.4xlarge')
            self.assertEqual(metadata['traffic_path'], 'AWS private VPC address')
            self.assertEqual(metadata['request_count_per_trial'], 100)
            self.assertEqual(metadata['trial_count'], 1)
            self.assertNotIn('service_architecture', metadata)
            self.assertNotIn('load_generator_architecture', metadata)

    def test_deathstar_preprovision_failure_keeps_static_metadata(self):
        plan = shared_plan('aws', ['deathstarbench'])
        job = shared_job('aws')
        for key in (
            'loadgen_public_ip',
            'architecture',
            'loadgen_architecture',
        ):
            job['resources'].pop(key, None)

        with self.assertRaisesRegex(
            RuntimeError,
            'DeathStarBench provisioning did not produce',
        ):
            main.run_deathstarbench(job, plan)

        self.assertEqual(len(job['results']), 1)
        result = job['results'][0]
        self.assertEqual(result['status'], 'failed')
        metadata = result['metadata']
        self.assertEqual(metadata['provider'], 'AWS')
        self.assertEqual(metadata['service_shape'], 'c7g.4xlarge')
        self.assertEqual(metadata['traffic_path'], 'AWS private VPC address')
        self.assertEqual(metadata['workload'], 'Hotel Reservation')
        self.assertEqual(metadata['duration_seconds'], 1)
        self.assertEqual(metadata['requested_requests_per_second'], 1)
        self.assertNotIn('service_architecture', metadata)
        self.assertNotIn('load_generator_architecture', metadata)
        self.assertNotIn('container_runtime_details', metadata)

    def test_apachebench_uses_canonical_aws_loadgen_and_provider_metadata(self):
        plan = shared_plan('aws', ['apachebench'])
        job = shared_job('aws')

        def fake_ssh(
            _job,
            command,
            timeout=1800,
            host_key='public_ip',
            include_stderr=False,
        ):
            if command == 'uname -m':
                return 'x86_64' if host_key == 'loadgen_public_ip' else 'aarch64'
            if command == 'measured':
                return 'apache output'
            return 'ok'

        with (
            patch.object(main, 'wait_for_web_guest_readiness') as readiness,
            patch.object(main, 'install_apachebench_guest') as install,
            patch.object(main, 'ssh', side_effect=fake_ssh),
            patch.object(main, 'apachebench_target_prepare_command', return_value='target'),
            patch.object(main, 'apachebench_loadgen_prepare_command', return_value='loadgen'),
            patch.object(main, 'apachebench_readiness_command', return_value='ready'),
            patch.object(main, 'apachebench_command', return_value='measured'),
            patch.object(main, 'parse_apachebench_output', return_value=dict(self.APACHE_METRICS)),
            patch.object(main, 'record_apachebench_network_capacity'),
            patch.object(main, 'validate_apachebench_metrics'),
            patch.object(main, 'aggregate_apachebench_trials', return_value={'mean_requests_per_second': 1000.0}),
        ):
            failures = main.run_apachebench(job, plan)

        self.assertEqual(failures, ())
        self.assertEqual(readiness.call_count, 2)
        self.assertEqual(install.call_count, 2)
        metadata = job['results'][0]['metadata']
        self.assertEqual(metadata['traffic_path'], 'AWS private VPC address')
        self.assertEqual(metadata['load_generator_vcpus'], 4)

    def test_deathstar_warmup_and_aws_compose_path_are_executed(self):
        plan = shared_plan('aws', ['deathstarbench'])
        job = shared_job('aws')
        calls = []

        def fake_ssh(
            _job,
            command,
            timeout=1800,
            host_key='public_ip',
            include_stderr=False,
        ):
            calls.append((host_key, command))
            if command == 'uname -m':
                return 'x86_64' if host_key == 'loadgen_public_ip' else 'aarch64'
            if 'podman info --format' in command:
                return 'rootless=false networkBackend=netavark graphDriver=overlay'
            if '/wrk2/wrk -D exp' in command:
                return self.WRK_METRICS
            return 'ok'

        with (
            patch.object(main, 'wait_for_web_guest_readiness'),
            patch.object(main, 'install_deathstarbench_guest'),
            patch.object(main, 'ssh', side_effect=fake_ssh),
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_deathstarbench(job, plan)

        commands = '\n'.join(command for _, command in calls)
        self.assertIn('sudo /usr/local/bin/podman-compose', commands)
        self.assertIn('source address=10.42.1.20/32', commands)
        self.assertTrue(any(
            item['stage'] == 'DeathStarBench warm-up'
            and 'Warm-up achieved' in item['message']
            for item in job['events']
        ))
        execute.assert_called_once()
        self.assertEqual(
            execute.call_args.kwargs['metadata']['traffic_path'],
            'AWS private VPC address',
        )

    def test_cloud_runners_call_apache_then_deathstar_and_defer_apache_failure(self):
        for provider, runner in (
            ('aws', main.run_aws_benchmarks),
            ('gcp', main.run_gcp_benchmarks),
        ):
            with self.subTest(provider=provider):
                values = {
                    'provider': provider,
                    'region': 'us-east-2' if provider == 'aws' else 'us-east1',
                    'shape': 'm7i.xlarge' if provider == 'aws' else 'n2-standard-4',
                    'ocpus': 4,
                    'memory_gb': 16,
                    'ssh_private_key': 'private',
                    'ssh_public_key': 'ssh-ed25519 AAAATEST',
                    'storage': {'additional_volume': False},
                    'benchmarks': ['apachebench', 'deathstarbench'],
                }
                if provider == 'aws':
                    values['aws_profile'] = 'default'
                else:
                    values.update({
                        'gcp_project_id': 'benchmark-project',
                        'gcp_zone': 'us-east1-b',
                    })
                plan = BenchmarkPlan(**values)
                job = shared_job(provider)
                order = []

                def run_apache(*_args):
                    order.append('apachebench')
                    return ('ApacheBench failed',)

                def run_deathstar(*_args):
                    order.append('deathstarbench')

                with (
                    patch.object(main, 'ssh', return_value='ready'),
                    patch.object(main, 'run_apachebench', side_effect=run_apache),
                    patch.object(main, 'run_deathstarbench', side_effect=run_deathstar),
                    self.assertRaisesRegex(RuntimeError, 'ApacheBench failed'),
                ):
                    runner(job, plan)

                self.assertEqual(order, ['apachebench', 'deathstarbench'])


class SharedLlamaAndSctpRunnerTests(unittest.TestCase):
    def test_aws_and_gcp_llama_use_shared_command_parser_and_metadata(self):
        for provider, runner in (
            ('aws', main.run_aws_benchmarks),
            ('gcp', main.run_gcp_benchmarks),
        ):
            with self.subTest(provider=provider):
                values = {
                    'provider': provider,
                    'region': 'us-east-2' if provider == 'aws' else 'us-east1',
                    'shape': 'c7i.xlarge' if provider == 'aws' else 'n2-standard-4',
                    'ocpus': 4,
                    'memory_gb': 8,
                    'ssh_private_key': 'private',
                    'ssh_public_key': 'ssh-ed25519 AAAATEST',
                    'storage': {'additional_volume': False},
                    'benchmarks': [],
                    'llm_benchmarks': ['llama_bench'],
                }
                if provider == 'aws':
                    values['aws_profile'] = 'default'
                else:
                    values.update({
                        'gcp_project_id': 'benchmark-project',
                        'gcp_zone': 'us-east1-b',
                    })
                plan = BenchmarkPlan(**values)
                job = shared_job(provider)
                job['resources']['architecture'] = 'x86_64'
                toolset_enable = (
                    main.rocky_linux.LLAMA_TOOLSET_ENABLE
                    if provider == 'gcp'
                    else None
                )
                toolchain_command = main.llama_cpp.toolchain_probe_command(
                    toolset_enable=toolset_enable
                )

                def fake_ssh(_job, command, **_kwargs):
                    if command == 'nproc':
                        return '4\n'
                    if command == toolchain_command:
                        return llama_toolchain_output(
                            toolset_version=15 if provider == 'gcp' else None
                        )
                    return 'ready'

                with (
                    patch.object(main, 'ssh', side_effect=fake_ssh) as ssh,
                    patch.object(main, 'execute_benchmark') as execute,
                ):
                    runner(job, plan)

                llama_call = next(
                    call for call in execute.call_args_list
                    if call.args[1] == 'llama_bench'
                )
                self.assertIn(main.llama_cpp.LLAMA_CPP_REVISION, llama_call.args[3])
                self.assertNotIn(main.LLAMA_TOOLSET_ENABLE, llama_call.args[3])
                if provider == 'gcp':
                    self.assertIn(
                        main.rocky_linux.LLAMA_TOOLSET_ENABLE,
                        llama_call.args[3],
                    )
                else:
                    self.assertNotIn(
                        main.rocky_linux.LLAMA_TOOLSET_ENABLE,
                        llama_call.args[3],
                    )
                self.assertIn('THREADS=4;', llama_call.args[3])
                self.assertNotIn('THREADS=$(nproc)', llama_call.args[3])
                self.assertEqual(
                    llama_call.kwargs['metadata']['model_sha256'],
                    main.llama_cpp.MODEL_SHA256,
                )
                self.assertEqual(
                    llama_call.kwargs['metadata']['logical_cpu_count'],
                    4,
                )
                self.assertEqual(
                    llama_call.kwargs['metadata']['llama_compiler_version'],
                    '15.2.1' if provider == 'gcp' else '11.5.0',
                )
                self.assertEqual(
                    llama_call.kwargs['metadata'][
                        'llama_cxx_compiler_version'
                    ],
                    '15.2.1' if provider == 'gcp' else '11.5.0',
                )
                self.assertFalse(
                    llama_call.kwargs['metadata']['llama_native_optimization']
                )
                self.assertEqual(
                    llama_call.kwargs['metadata']['llama_cpu_build_profile'],
                    main.llama_cpp.X86_64_PORTABLE_CPU_PROFILE,
                )
                self.assertIsNone(llama_call.kwargs['output_limit'])
                self.assertFalse(llama_call.kwargs['include_stderr'])
                self.assertEqual(llama_call.kwargs['transport_attempts'], 1)
                with patch.object(
                    main.llama_cpp,
                    'parse_output',
                    return_value={'ok': 1},
                ) as parser:
                    llama_call.kwargs['parser']('[]')
                parser.assert_called_once_with('[]', expected_threads=4)
                self.assertTrue(any(
                    'huggingface.co' in call.args[1]
                    for call in ssh.call_args_list
                ))
                self.assertEqual(ssh.call_args_list[-2].args[1], 'nproc')
                self.assertEqual(
                    ssh.call_args_list[-1].args[1],
                    toolchain_command,
                )
                self.assertFalse(
                    ssh.call_args_list[-1].kwargs['include_stderr']
                )

    def test_both_rocky_cloud_runners_select_gcc_toolset_15_for_llama(self):
        plan = SimpleNamespace(
            benchmarks=[],
            llm_benchmarks=['llama_bench'],
            sysbench=SimpleNamespace(workloads=[]),
            iperf3=SimpleNamespace(protocols=[]),
            phoronix=SimpleNamespace(profiles=[]),
            storage=SimpleNamespace(additional_volume=False),
            ocpus=4,
        )
        for provider_name in ('GCP', 'Azure'):
            with self.subTest(provider=provider_name):
                job = {
                    'id': f'{provider_name.lower()}-llama',
                    'events': [],
                    'results': [],
                    'resources': {'architecture': 'x86_64'},
                }
                with (
                    patch.object(main, 'ssh', return_value='ready') as ssh,
                    patch.object(main, 'run_llama_benchmark') as run_llama,
                ):
                    main.run_rocky_linux_benchmarks(
                        job,
                        plan,
                        provider_name=provider_name,
                        environment={'provider': provider_name},
                    )

                run_llama.assert_called_once_with(
                    job,
                    metadata={'provider': provider_name},
                    toolset_enable=main.rocky_linux.LLAMA_TOOLSET_ENABLE,
                )
                guest_commands = [call.args[1] for call in ssh.call_args_list]
                self.assertTrue(any(
                    all(
                        package in command
                        for package in main.rocky_linux.LLAMA_TOOLSET_PACKAGES
                    )
                    for command in guest_commands
                ))
                self.assertTrue(any(
                    f'source {main.rocky_linux.LLAMA_TOOLSET_ENABLE}' in command
                    and 'for TOOL in gcc g++ as ld' in command
                    and 'g++ -print-prog-name=as' in command
                    for command in guest_commands
                ))

    def test_oci_llama_uses_observed_x86_smt_and_arm_thread_counts(self):
        cases = (
            ('VM.Standard.E5.Flex', 'x86_64', 2, '4\n', 4),
            ('VM.Standard.A1.Flex', 'aarch64', 2, '2\n', 2),
        )
        for shape, architecture, ocpus, probe_output, expected in cases:
            with self.subTest(shape=shape):
                plan = shared_plan('oci', [])
                plan.shape = shape
                plan.ocpus = ocpus
                plan.llm_benchmarks = ['llama_bench']
                job = shared_job('oci')
                job['resources'].pop('architecture')
                job['resources']['image_id'] = 'ocid1.image.oc1..runner'
                job['resources']['image_name'] = 'Oracle-Linux-9-runner'

                def fake_ssh(_job, command, **_kwargs):
                    if command == 'uname -m':
                        return f'{architecture}\n'
                    if command == 'nproc':
                        return probe_output
                    if command == main.llama_cpp.toolchain_probe_command(
                        toolset_enable=main.LLAMA_TOOLSET_ENABLE
                    ):
                        return llama_toolchain_output(toolset_version=12)
                    return 'ready'

                with (
                    patch.object(main, 'wait_for_guest_readiness'),
                    patch.object(main, 'install_benchmark_tools'),
                    patch.object(main, 'ssh', side_effect=fake_ssh) as ssh,
                    patch.object(main, 'execute_benchmark') as execute,
                ):
                    main.run_oci_benchmarks(job, plan)

                self.assertEqual(
                    [call.args[1] for call in ssh.call_args_list],
                    [
                        'uname -m',
                        'nproc',
                        main.llama_cpp.toolchain_probe_command(
                            toolset_enable=main.LLAMA_TOOLSET_ENABLE
                        ),
                    ],
                )
                for call in ssh.call_args_list:
                    self.assertEqual(call.kwargs['timeout'], 60)
                llama_call = execute.call_args
                self.assertIn(f'THREADS={expected};', llama_call.args[3])
                self.assertEqual(llama_call.kwargs['metadata']['provider'], 'OCI')
                self.assertEqual(llama_call.kwargs['metadata']['shape'], shape)
                self.assertEqual(llama_call.kwargs['metadata']['ocpus'], ocpus)
                self.assertEqual(
                    llama_call.kwargs['metadata']['architecture'],
                    architecture,
                )
                self.assertEqual(
                    llama_call.kwargs['metadata']['image_id'],
                    'ocid1.image.oc1..runner',
                )
                self.assertEqual(
                    llama_call.kwargs['metadata']['logical_cpu_count'],
                    expected,
                )
                self.assertEqual(
                    llama_call.kwargs['metadata']['llama_compiler_version'],
                    '12.3.1',
                )
                self.assertEqual(
                    llama_call.kwargs['metadata']['llama_toolset_enable'],
                    main.LLAMA_TOOLSET_ENABLE,
                )
                if architecture == 'x86_64':
                    self.assertFalse(
                        llama_call.kwargs['metadata'][
                            'llama_native_optimization'
                        ]
                    )
                    self.assertIn('-DGGML_NATIVE=OFF', llama_call.args[3])
                    self.assertNotIn('-DGGML_NATIVE=ON', llama_call.args[3])
                    expected_profile = (
                        main.llama_cpp.X86_64_PORTABLE_CPU_PROFILE
                    )
                else:
                    self.assertTrue(
                        llama_call.kwargs['metadata'][
                            'llama_native_optimization'
                        ]
                    )
                    self.assertIn('-DGGML_NATIVE=ON', llama_call.args[3])
                    self.assertNotIn('-DGGML_NATIVE=OFF', llama_call.args[3])
                    expected_profile = main.llama_cpp.AARCH64_NATIVE_CPU_PROFILE
                self.assertEqual(
                    llama_call.kwargs['metadata']['llama_cpu_build_profile'],
                    expected_profile,
                )
                with patch.object(
                    main.llama_cpp,
                    'parse_output',
                    return_value={'ok': 1},
                ) as parser:
                    llama_call.kwargs['parser']('[]')
                parser.assert_called_once_with(
                    '[]',
                    expected_threads=expected,
                )

    def test_llama_rejects_a_malformed_logical_cpu_probe_before_execution(self):
        job = shared_job('oci')

        def fake_ssh(_job, command, **_kwargs):
            return 'x86_64\n' if command == 'uname -m' else '4 CPUs\n'

        with (
            patch.object(main, 'ssh', side_effect=fake_ssh),
            patch.object(main, 'execute_benchmark') as execute,
            self.assertRaisesRegex(RuntimeError, 'positive integer'),
        ):
            main.run_llama_benchmark(job)

        execute.assert_not_called()
        self.assertEqual(len(job['results']), 1)
        self.assertEqual(job['results'][0]['id'], 'llama_bench')
        self.assertEqual(job['results'][0]['status'], 'failed')
        self.assertIn('4 CPUs', job['results'][0]['output'])
        self.assertEqual(
            job['results'][0]['metadata']['model_sha256'],
            main.llama_cpp.MODEL_SHA256,
        )

    def test_llama_rejects_an_unverifiable_toolchain_before_building(self):
        job = shared_job('oci')

        def fake_ssh(_job, command, **_kwargs):
            if command == 'uname -m':
                return 'aarch64\n'
            if command == 'nproc':
                return '4\n'
            return 'not toolchain provenance\n'

        with (
            patch.object(main, 'ssh', side_effect=fake_ssh),
            patch.object(main, 'execute_benchmark') as execute,
            self.assertRaisesRegex(RuntimeError, 'build-toolchain probe'),
        ):
            main.run_llama_benchmark(job)

        execute.assert_not_called()
        self.assertEqual(job['results'][0]['status'], 'failed')
        self.assertIn('Build-toolchain probe failed', job['results'][0]['error'])
        self.assertIn('not toolchain provenance', job['results'][0]['output'])
        self.assertEqual(
            job['results'][0]['metadata']['logical_cpu_count'],
            4,
        )

    def test_oci_sctp_result_uses_the_strict_protocol_parser(self):
        plan = shared_plan('oci', ['iperf3'])
        plan.iperf3 = SimpleNamespace(protocols=['sctp'])
        job = shared_job('oci')
        job['resources']['peer_private_ip'] = '10.42.2.10'

        with (
            patch.object(main, 'wait_for_guest_readiness'),
            patch.object(main, 'install_benchmark_tools'),
            patch.object(main, 'wait_for_iperf_peer') as wait_peer,
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_oci_benchmarks(job, plan)

        wait_peer.assert_called_once_with(job, ('sctp',))
        sctp_call = next(
            call for call in execute.call_args_list
            if call.args[1] == 'iperf_sctp'
        )
        with patch.object(
            main,
            'parse_iperf3_output',
            return_value={'protocol': 'SCTP'},
        ) as parser:
            sctp_call.kwargs['parser']('{}')
        parser.assert_called_once_with('{}', expected_protocol='sctp')


if __name__ == '__main__':
    unittest.main()
