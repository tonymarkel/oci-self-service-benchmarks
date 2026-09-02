import tempfile
import unittest
from pathlib import Path
from unittest.mock import call, patch

from app import main
from app.models import BenchmarkPlan


def gcp_plan(**overrides):
    values = {
        'provider': 'gcp',
        'gcp_project_id': 'benchmark-project',
        'gcp_zone': 'us-east1-b',
        'region': 'us-east1',
        'shape': 'n2-standard-2',
        'ocpus': 2,
        'memory_gb': 8,
        'ssh_private_key': 'private',
        'ssh_public_key': 'ssh-ed25519 AAAATEST',
        'storage': {'additional_volume': False},
        'benchmarks': ['sysbench'],
        'sysbench': {'workloads': ['cpu']},
    }
    values.update(overrides)
    return BenchmarkPlan(**values)


def gcp_job(plan=None, **resource_overrides):
    plan = plan or gcp_plan()
    resources = {
        'provider': 'gcp',
        'gcp_project_id': plan.gcp_project_id,
        'region': plan.region,
        'gcp_zone': plan.gcp_zone,
        'gcp_machine_type': plan.shape,
        'public_ip': '198.51.100.20',
        'private_ip': '10.42.1.2',
        'ssh_user': 'benchmark',
        'architecture': 'x86_64',
        'image_id': 'rocky-linux-9-v20260801',
        'image_name': 'rocky-linux-9-v20260801',
    }
    resources.update(resource_overrides)
    return {
        'id': 'gcp-integration',
        'plan': plan.model_dump(
            exclude={
                'ssh_private_key',
                'ssh_public_key',
                'ssh_key_passphrase',
            }
        ),
        'events': [],
        'resources': resources,
        'results': [],
    }


class GcpMainDispatchTests(unittest.TestCase):
    def test_provision_dispatches_to_gcp_with_only_the_public_key(self):
        plan = gcp_plan()
        job = {
            'id': 'gcp-dispatch',
            '_public_key': 'ssh-ed25519 AAAATEST',
            'resources': {},
        }

        with patch.object(main.gcp_provider, 'provision') as provision:
            main.provision(job, plan)

        provision.assert_called_once_with(
            job,
            plan,
            public_key='ssh-ed25519 AAAATEST',
            emit=main.event,
            persist=main.persist_job_state,
        )

    def test_cleanup_dispatches_from_the_persisted_gcp_provider(self):
        job = {
            'id': 'gcp-cleanup',
            'plan': {
                'provider': 'gcp',
                'gcp_project_id': 'benchmark-project',
                'gcp_zone': 'us-east1-b',
                'region': 'us-east1',
            },
            'resources': {},
        }

        with patch.object(main.gcp_provider, 'destroy_resources') as cleanup:
            main.destroy_resources(job, preserve_status=True)

        cleanup.assert_called_once_with(
            job,
            emit=main.event,
            persist=main.persist_job_state,
            preserve_status=True,
        )

    def test_response_lost_gcp_create_contract_is_recoverable(self):
        job = {
            'plan': {'provider': 'gcp'},
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'benchmark-project',
                'gcp_resource_prefix': 'benchmark-response-lost',
            },
        }

        self.assertTrue(main.has_recoverable_resources(job))
        job['resources'].pop('gcp_resource_prefix')
        self.assertFalse(main.has_recoverable_resources(job))

    def test_persisted_gcp_ownership_contract_survives_app_restart(self):
        job = {
            'id': 'gcp-response-lost',
            'status': 'provisioning',
            'plan': {
                'provider': 'gcp',
                'gcp_project_id': 'benchmark-project',
                'gcp_zone': 'us-east1-b',
                'region': 'us-east1',
                'shape': 'n2-standard-2',
            },
            'events': [],
            'resources': {
                'provider': 'gcp',
                'gcp_project_id': 'benchmark-project',
                'gcp_resource_prefix': 'benchmark-gcp-response-lost',
            },
            'results': [],
            'created_at': main.now(),
            'updated_at': main.now(),
        }

        with tempfile.TemporaryDirectory() as directory:
            run_directory = Path(directory) / job['id']
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.dict(main.jobs, {}, clear=True),
            ):
                main.persist_job_state(job)
                status = main.job_status(job['id'])
                summary = main.run_summary(run_directory)

        self.assertEqual(status['status'], 'interrupted')
        self.assertTrue(status['recoverable'])
        self.assertTrue(summary['recoverable'])
        self.assertEqual(summary['provider'], 'gcp')


class GcpSshReadinessTests(unittest.TestCase):
    @staticmethod
    def job():
        return {
            'id': 'gcp-ssh-readiness',
            'events': [],
            'resources': {
                'public_ip': '198.51.100.20',
                'ssh_user': 'benchmark',
            },
        }

    def test_transport_probe_retries_only_connection_failures(self):
        job = self.job()
        unavailable = main.SSHCommandError(
            'SSH command failed: connection refused',
            output='connection refused',
            returncode=255,
        )

        with (
            patch.object(
                main,
                'ssh',
                side_effect=[unavailable, unavailable, ''],
            ) as ssh,
            patch.object(main.time, 'sleep') as sleep,
        ):
            main.wait_for_ssh_transport(
                job,
                attempts=3,
                retry_delay_seconds=5,
            )

        self.assertEqual(ssh.call_count, 3)
        for probe in ssh.call_args_list:
            self.assertEqual(probe.args[1], 'true')
            self.assertEqual(probe.kwargs['transport_attempts'], 1)
            self.assertEqual(probe.kwargs['timeout'], 30)
        self.assertEqual(sleep.call_args_list, [call(5.0), call(5.0)])
        self.assertEqual(job['events'][-1]['message'], 'Guest SSH transport is ready.')

    def test_transport_probe_does_not_retry_a_remote_command_failure(self):
        job = self.job()
        remote_failure = main.SSHCommandError(
            'SSH command failed: permission denied',
            output='permission denied',
            returncode=1,
        )

        with (
            patch.object(main, 'ssh', side_effect=remote_failure) as ssh,
            patch.object(main.time, 'sleep') as sleep,
            self.assertRaisesRegex(main.SSHCommandError, 'permission denied'),
        ):
            main.wait_for_ssh_transport(job)

        ssh.assert_called_once()
        sleep.assert_not_called()


class GcpBenchmarkRuntimeTests(unittest.TestCase):
    def test_gcp_cpu_run_uses_rocky_setup_and_strict_parser(self):
        plan = gcp_plan()
        job = gcp_job(plan)

        with (
            patch.object(main, 'ssh', return_value='ready') as ssh,
            patch.object(main, 'execute_benchmark') as execute,
            patch.object(main, 'wait_for_guest_readiness') as oci_readiness,
            patch.object(main, 'install_benchmark_tools') as oci_install,
        ):
            main.run_benchmarks(job, plan)

        self.assertGreaterEqual(ssh.call_count, 4)
        self.assertTrue(any(
            'Rocky Linux 9' in item['message'] for item in job['events']
        ))
        oci_readiness.assert_not_called()
        oci_install.assert_not_called()
        execute.assert_called_once()
        args, kwargs = execute.call_args
        self.assertEqual(args[1:3], ('sysbench_cpu', 'Sysbench — CPU'))
        self.assertIn('--threads=2', args[3])
        self.assertIs(
            kwargs['parser'],
            main.rocky_linux.parse_sysbench_cpu_output,
        )
        self.assertEqual(kwargs['metadata']['provider'], 'GCP')
        self.assertEqual(kwargs['metadata']['project_id'], 'benchmark-project')
        self.assertEqual(kwargs['metadata']['zone'], 'us-east1-b')
        self.assertEqual(
            kwargs['metadata']['image_id'],
            'rocky-linux-9-v20260801',
        )
        self.assertEqual(ssh.call_args_list[0].args[1], 'true')
        readiness_command = ssh.call_args_list[1].args[1]
        self.assertEqual(
            ssh.call_args_list[1].kwargs['transport_attempts'],
            1,
        )
        self.assertIn('EXPECTED_ARCH=x86_64', readiness_command)
        self.assertIn('codeload.github.com', readiness_command)
        self.assertNotIn('openbenchmarking.org', readiness_command)

    def test_gcp_arm_readiness_uses_the_persisted_machine_architecture(self):
        plan = gcp_plan(
            shape='t2a-standard-2',
            ocpus=2,
            memory_gb=8,
            benchmarks=['stream'],
        )
        job = gcp_job(plan, architecture='arm64')

        with (
            patch.object(main, 'ssh', return_value='ready') as ssh,
            patch.object(main, 'execute_benchmark'),
        ):
            main.run_gcp_benchmarks(job, plan)

        readiness_command = ssh.call_args_list[1].args[1]
        self.assertIn('EXPECTED_ARCH=aarch64', readiness_command)

    def test_gcp_memory_and_stream_use_strict_parsers(self):
        plan = gcp_plan(
            benchmarks=['sysbench', 'stream'],
            sysbench={'workloads': ['memory']},
        )
        job = gcp_job(plan)

        with (
            patch.object(main, 'ssh', return_value='ready'),
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_gcp_benchmarks(job, plan)

        calls = {call.args[1]: call for call in execute.call_args_list}
        self.assertEqual(set(calls), {'sysbench_memory', 'stream'})
        self.assertIs(
            calls['sysbench_memory'].kwargs['parser'],
            main.rocky_linux.parse_sysbench_memory_output,
        )
        self.assertTrue(callable(calls['stream'].kwargs['parser']))
        with patch.object(
            main.rocky_linux,
            'parse_stream_output',
            return_value={'copy_mib_per_second': 1},
        ) as parser:
            calls['stream'].kwargs['parser']('STREAM output')
        parser.assert_called_once_with('STREAM output', expected_threads=2)

    def test_gcp_missing_architecture_fails_before_any_guest_write(self):
        plan = gcp_plan()
        job = gcp_job(plan)
        job['resources'].pop('architecture')

        with (
            patch.object(main, 'ssh') as ssh,
            self.assertRaisesRegex(RuntimeError, 'architecture is missing'),
        ):
            main.run_gcp_benchmarks(job, plan)

        ssh.assert_not_called()

    def test_gcp_exact_data_disk_is_mounted_without_disk_scanning(self):
        plan = gcp_plan(
            storage={
                'additional_volume': True,
                'additional_size_gb': 100,
            },
            benchmarks=['fio'],
        )
        job = gcp_job(
            plan,
            gcp_data_device_name='benchmark-data-gcp-integration',
        )

        with (
            patch.object(
                main.rocky_linux,
                'data_volume_mount_command',
                return_value='exact-gcp-mount',
            ) as mount_command,
            patch.object(main, 'ssh', return_value='mounted') as ssh,
        ):
            main.mount_data_volume(job)

        mount_command.assert_called_once_with(
            'benchmark-data-gcp-integration',
            owner='benchmark',
        )
        ssh.assert_called_once_with(job, 'exact-gcp-mount', timeout=300)

    def test_gcp_missing_data_device_name_refuses_to_inspect_guest_disks(self):
        job = gcp_job()

        with (
            patch.object(main, 'ssh') as ssh,
            self.assertRaisesRegex(RuntimeError, 'device name is missing'),
        ):
            main.mount_data_volume(job)

        ssh.assert_not_called()

    def test_gcp_fio_and_sysbench_fileio_use_data_volume_and_parsers(self):
        plan = gcp_plan(
            storage={
                'additional_volume': True,
                'additional_size_gb': 100,
            },
            benchmarks=['sysbench', 'fio'],
            sysbench={'workloads': ['fileio']},
        )
        job = gcp_job(
            plan,
            gcp_data_device_name='benchmark-data-gcp-integration',
            gcp_data_disk_type='pd-balanced',
            gcp_data_disk_size_gb=100,
        )

        with (
            patch.object(main, 'ssh', return_value='ready'),
            patch.object(main, 'mount_data_volume') as mount,
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_gcp_benchmarks(job, plan)

        mount.assert_called_once_with(job)
        calls = {call.args[1]: call for call in execute.call_args_list}
        self.assertEqual(set(calls), {'fio', 'sysbench_fileio'})
        self.assertIn('--directory=/data', calls['fio'].args[3])
        self.assertIn('cd /data', calls['sysbench_fileio'].args[3])
        self.assertIs(
            calls['fio'].kwargs['parser'],
            main.rocky_linux.parse_fio_output,
        )
        self.assertIs(
            calls['sysbench_fileio'].kwargs['parser'],
            main.rocky_linux.parse_sysbench_fileio_output,
        )
        self.assertEqual(
            calls['fio'].kwargs['metadata']['data_volume_type'],
            'pd-balanced',
        )

    def test_gcp_iperf_uses_same_zone_private_peer_for_tcp_and_udp(self):
        plan = gcp_plan(
            benchmarks=['iperf3'],
            iperf3={'protocols': ['tcp', 'udp']},
        )
        job = gcp_job(
            plan,
            peer_private_ip='10.42.1.3',
            gcp_peer_machine_type=plan.shape,
            gcp_peer_image_id='rocky-linux-9-v20260801',
        )

        with (
            patch.object(main, 'ssh', return_value='ready'),
            patch.object(main, 'wait_for_iperf_peer') as wait_for_peer,
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_gcp_benchmarks(job, plan)

        wait_for_peer.assert_called_once_with(
            job,
            ('tcp', 'udp'),
            verify_tcp_udp=True,
        )
        calls = {call.args[1]: call for call in execute.call_args_list}
        self.assertEqual(set(calls), {'iperf_tcp', 'iperf_udp'})
        self.assertIn('-c 10.42.1.3', calls['iperf_tcp'].args[3])
        self.assertIn('-u -b 0', calls['iperf_udp'].args[3])
        for call in calls.values():
            self.assertEqual(
                call.kwargs['metadata']['traffic_path'],
                'GCP private VPC address',
            )
            self.assertEqual(
                call.kwargs['metadata']['peer_private_ip'],
                '10.42.1.3',
            )
        self.assertTrue(any(
            'same-zone GCP peer' in item['message']
            for item in job['events']
        ))

    def test_gcp_phoronix_profiles_run_after_guest_preparation(self):
        plan = gcp_plan(
            benchmarks=['phoronix'],
            phoronix={'profiles': ['compress_7zip', 'openssl']},
        )
        job = gcp_job(plan)

        with (
            patch.object(main, 'ssh', return_value='ready') as ssh,
            patch.object(main, 'run_phoronix_profiles', return_value=()) as run,
        ):
            main.run_gcp_benchmarks(job, plan)

        run.assert_called_once_with(job, plan)
        self.assertIn('openbenchmarking.org', ssh.call_args_list[1].args[1])

    def test_gcp_phoronix_failures_are_deferred_then_reported(self):
        plan = gcp_plan(
            benchmarks=['phoronix'],
            phoronix={'profiles': ['openssl']},
        )
        job = gcp_job(plan)

        with (
            patch.object(main, 'ssh', return_value='ready'),
            patch.object(
                main,
                'run_phoronix_profiles',
                return_value=('OpenSSL failed',),
            ),
            self.assertRaisesRegex(RuntimeError, 'OpenSSL failed'),
        ):
            main.run_gcp_benchmarks(job, plan)


if __name__ == '__main__':
    unittest.main()
