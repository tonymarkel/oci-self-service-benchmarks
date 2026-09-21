import asyncio
import json
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app import main


def active_job(job_id='ca11ce11ca10', *, status='testing'):
    cancel = threading.Event()
    return {
        'id': job_id,
        'status': status,
        'plan': {
            'provider': 'aws',
            'aws_profile': 'default',
            'region': 'us-east-2',
            'benchmarks': ['deathstarbench'],
        },
        '_key': 'private-key',
        '_passphrase': None,
        '_persist_state': True,
        '_cancel_event': cancel,
        'events': [],
        'resources': {
            'aws_account_id': '123456789012',
            'aws_vpc_id': 'vpc-owned',
            'public_ip': '198.51.100.10',
            'ssh_user': 'ec2-user',
        },
        'results': [],
        'created_at': main.now(),
        'updated_at': main.now(),
    }


class LiveRunCancellationTests(unittest.TestCase):
    def tearDown(self):
        main.jobs.clear()
        main.job_tasks.clear()
        main.job_cancel_events.clear()
        with main.job_processes_lock:
            main.job_processes.clear()

    def test_live_destroy_signals_supervisor_and_is_idempotent(self):
        async def scenario(directory):
            blocker = asyncio.Event()
            supervisor = asyncio.create_task(blocker.wait())
            job = active_job()
            main.jobs[job['id']] = job
            main.job_tasks[job['id']] = supervisor
            main.job_cancel_events[job['id']] = job['_cancel_event']
            try:
                with (
                    patch.object(main, 'RUNS', Path(directory)),
                    patch.object(
                        main.asyncio,
                        'create_task',
                        side_effect=AssertionError(
                            'live stop must not launch parallel cleanup'
                        ),
                    ),
                ):
                    first = await main.destroy(job['id'])
                    second = await main.destroy(job['id'])
                    saved = json.loads(
                        (Path(directory) / job['id'] / 'state.json').read_text()
                    )
            finally:
                supervisor.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await supervisor
            return first, second, job, saved

        with tempfile.TemporaryDirectory() as directory:
            first, second, job, saved = asyncio.run(scenario(directory))

        self.assertEqual(first, {'status': 'cancelling'})
        self.assertEqual(second, {'status': 'cancelling'})
        self.assertTrue(job['_cancel_event'].is_set())
        self.assertTrue(job['benchmark_interrupted'])
        self.assertEqual(job['status'], 'cancelling')
        self.assertEqual(
            [item['stage'] for item in job['events']],
            ['Stop requested'],
        )
        self.assertEqual(saved['status'], 'cancelling')
        self.assertTrue(saved['benchmark_interrupted'])
        self.assertEqual(saved['benchmark_status'], 'interrupted')
        self.assertTrue(saved['cancel_requested_at'])

    def test_supervisor_serially_cleans_once_after_cancelled_provision(self):
        async def scenario(directory):
            job = active_job(status='queued')
            cleanup_calls = []

            def provision(_job, _plan):
                job['_cancel_event'].set()

            def cleanup(target):
                cleanup_calls.append(target['status'])
                target['status'] = 'destroyed'
                main.persist_job_state(target)

            plan = SimpleNamespace(destroy_after_completion=False)
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(
                    main,
                    'provider_adapter',
                    return_value=SimpleNamespace(
                        provisioning_message='Provisioning test resources.'
                    ),
                ),
                patch.object(main, 'provision', side_effect=provision),
                patch.object(main, 'run_benchmarks') as benchmarks,
                patch.object(main, 'destroy_with_status', side_effect=cleanup),
            ):
                await main.run_job(job, plan)
            return job, cleanup_calls, benchmarks

        with tempfile.TemporaryDirectory() as directory:
            job, cleanup_calls, benchmarks = asyncio.run(scenario(directory))

        self.assertEqual(cleanup_calls, ['cleanup_pending'])
        self.assertEqual(job['status'], 'destroyed')
        self.assertTrue(job['benchmark_interrupted'])
        self.assertIsNone(job.get('error'))
        self.assertNotIn('_cancel_event', job)
        benchmarks.assert_not_called()

    def test_stop_during_reporting_preserves_completed_benchmark_outcome(self):
        async def scenario(directory):
            job = active_job(job_id='ca11ce11ca12', status='queued')
            report_started = threading.Event()
            release_report = threading.Event()
            cleanup_calls = []

            def benchmarks(target, _plan):
                target['results'].append({
                    'id': 'deathstarbench',
                    'name': 'DeathStarBench',
                    'status': 'completed',
                })

            def report(_job):
                report_started.set()
                self.assertTrue(release_report.wait(2))

            def cleanup(target):
                cleanup_calls.append(target['status'])
                target['status'] = 'destroyed'

            plan = SimpleNamespace(destroy_after_completion=False)
            main.jobs[job['id']] = job
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(
                    main,
                    'provider_adapter',
                    return_value=SimpleNamespace(
                        provisioning_message='Provisioning test resources.'
                    ),
                ),
                patch.object(main, 'provision'),
                patch.object(main, 'run_benchmarks', side_effect=benchmarks),
                patch.object(main, 'make_report', side_effect=report),
                patch.object(main, 'destroy_with_status', side_effect=cleanup),
            ):
                supervisor = asyncio.create_task(main.run_job(job, plan))
                main.retain_job_task(
                    job['id'],
                    supervisor,
                    cancel_event=job['_cancel_event'],
                )
                self.assertTrue(
                    await asyncio.to_thread(report_started.wait, 2)
                )
                response = await main.destroy(job['id'])
                release_report.set()
                await supervisor
            return job, response, cleanup_calls

        with tempfile.TemporaryDirectory() as directory:
            job, response, cleanup_calls = asyncio.run(scenario(directory))

        self.assertEqual(response, {'status': 'cancelling'})
        self.assertEqual(cleanup_calls, ['cleanup_pending'])
        self.assertEqual(job['status'], 'destroyed')
        self.assertFalse(job.get('benchmark_interrupted', False))
        self.assertEqual(main.benchmark_status(job), 'complete')

    def test_stop_during_auto_cleanup_does_not_race_or_mark_interrupted(self):
        async def scenario(directory):
            job = active_job(job_id='c1ea0a11ca13', status='queued')
            cleanup_started = threading.Event()
            release_cleanup = threading.Event()
            cleanup_calls = []

            def benchmarks(target, _plan):
                target['results'].append({
                    'id': 'deathstarbench',
                    'name': 'DeathStarBench',
                    'status': 'completed',
                })

            def cleanup(target):
                cleanup_calls.append(target['status'])
                cleanup_started.set()
                self.assertTrue(release_cleanup.wait(2))
                target['status'] = 'destroyed'

            plan = SimpleNamespace(destroy_after_completion=True)
            main.jobs[job['id']] = job
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(
                    main,
                    'provider_adapter',
                    return_value=SimpleNamespace(
                        provisioning_message='Provisioning test resources.'
                    ),
                ),
                patch.object(main, 'provision'),
                patch.object(main, 'run_benchmarks', side_effect=benchmarks),
                patch.object(main, 'make_report'),
                patch.object(main, 'destroy_with_status', side_effect=cleanup),
            ):
                supervisor = asyncio.create_task(main.run_job(job, plan))
                main.retain_job_task(
                    job['id'],
                    supervisor,
                    cancel_event=job['_cancel_event'],
                )
                self.assertTrue(
                    await asyncio.to_thread(cleanup_started.wait, 2)
                )
                response = await main.destroy(job['id'])
                release_cleanup.set()
                await supervisor
            return job, response, cleanup_calls

        with tempfile.TemporaryDirectory() as directory:
            job, response, cleanup_calls = asyncio.run(scenario(directory))

        self.assertEqual(response, {'status': 'cleanup_pending'})
        self.assertEqual(cleanup_calls, ['cleanup_pending'])
        self.assertEqual(job['status'], 'destroyed')
        self.assertFalse(job.get('benchmark_interrupted', False))
        self.assertEqual(main.benchmark_status(job), 'complete')

    def test_ssh_process_is_terminated_when_cancellation_is_signalled(self):
        job = active_job(job_id='ca11ce55aa14')
        cancel = job['_cancel_event']
        captured = {}

        class Process:
            pid = 4242
            returncode = None

            def poll(self):
                return self.returncode

            def communicate(self, timeout=None):
                if self.returncode is not None:
                    return 'partial output\n', ''
                cancel.set()
                raise subprocess.TimeoutExpired('ssh', timeout)

        process = Process()

        def popen(args, **kwargs):
            captured['args'] = args
            captured['kwargs'] = kwargs
            return process

        def terminate(target):
            self.assertIs(target, process)
            target.returncode = -15

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', side_effect=popen),
                patch.object(
                    main,
                    'signal_process_termination',
                    side_effect=terminate,
                ) as terminate_process,
            ):
                with self.assertRaises(main.RunCancelled):
                    main.ssh(job, 'long-running-command', timeout=60)

        terminate_process.assert_called_once_with(process)
        self.assertTrue(captured['kwargs']['start_new_session'])
        self.assertIs(captured['kwargs']['stdout'], subprocess.PIPE)
        self.assertIs(captured['kwargs']['stderr'], subprocess.PIPE)
        self.assertNotIn(job['id'], main.job_processes)

    def test_unexpected_communicate_error_cannot_orphan_ssh_process(self):
        job = active_job(job_id='b00b1e55aa15')

        class Process:
            pid = 4343
            returncode = None
            communicate_calls = 0
            wait_calls = 0

            def poll(self):
                return self.returncode

            def communicate(self, timeout=None):
                self.communicate_calls += 1
                raise OSError('broken local SSH pipe')

            def wait(self, timeout=None):
                self.wait_calls += 1
                self.returncode = -15
                return self.returncode

        process = Process()

        def terminate(target):
            self.assertIs(target, process)

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', return_value=process),
                patch.object(
                    main,
                    'signal_process_termination',
                    side_effect=terminate,
                ) as terminate_process,
                self.assertRaisesRegex(OSError, 'broken local SSH pipe'),
            ):
                main.ssh(job, 'long-running-command', timeout=60)

        terminate_process.assert_called_once_with(process)
        self.assertEqual(process.communicate_calls, 2)
        self.assertEqual(process.wait_calls, 1)
        self.assertEqual(process.returncode, -15)
        self.assertNotIn(job['id'], main.job_processes)

    def test_first_boot_retry_wait_is_cancellation_aware(self):
        job = active_job(job_id='ca11cead1e16')

        def unavailable(*_args, **_kwargs):
            job['_cancel_event'].set()
            raise main.SSHCommandError(
                'connection refused',
                output='connection refused',
                returncode=255,
            )

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main, 'ssh', side_effect=unavailable),
                patch.object(main.time, 'sleep') as sleep,
                self.assertRaises(main.RunCancelled),
            ):
                main.wait_for_ssh_transport(
                    job,
                    attempts=2,
                    retry_delay_seconds=30,
                )

        sleep.assert_not_called()

    def test_persisted_cancelling_job_recovers_as_interrupted_and_destroys(self):
        job = active_job(job_id='ca11ce11ca11', status='cancelling')
        job['benchmark_interrupted'] = True
        job['cancel_requested_at'] = main.now()

        with tempfile.TemporaryDirectory() as directory:
            run_directory = Path(directory) / job['id']
            with patch.object(main, 'RUNS', Path(directory)):
                main.persist_job_state(job)
                main.jobs.clear()
                status = main.job_status(job['id'])
                summary = main.run_summary(run_directory)
                with patch.object(main, 'destroy_with_status'):
                    response = asyncio.run(main.destroy(job['id']))
                saved = json.loads((run_directory / 'state.json').read_text())

        self.assertEqual(status['status'], 'interrupted')
        self.assertEqual(status['benchmark_status'], 'interrupted')
        self.assertTrue(status['recoverable'])
        self.assertFalse(status['live'])
        self.assertEqual(summary['status'], 'interrupted')
        self.assertEqual(summary['benchmark_status'], 'interrupted')
        self.assertEqual(response, {'status': 'destroying'})
        self.assertEqual(saved['status'], 'destroying')

    def test_direct_cleanup_task_is_retained_and_repeated_post_is_serial(self):
        async def scenario(directory):
            job = active_job(job_id='e7a1edc1ea17', status='complete')
            job['results'] = [{
                'id': 'deathstarbench',
                'name': 'DeathStarBench',
                'status': 'completed',
            }]
            cleanup_started = threading.Event()
            release_cleanup = threading.Event()
            cleanup_calls = []

            def cleanup(target):
                cleanup_calls.append(target['status'])
                cleanup_started.set()
                self.assertTrue(release_cleanup.wait(2))
                target['status'] = 'destroyed'

            main.jobs[job['id']] = job
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main, 'destroy_with_status', side_effect=cleanup),
            ):
                first = await main.destroy(job['id'])
                cleanup_task = main.job_tasks.get(job['id'])
                self.assertIsNotNone(cleanup_task)
                self.assertTrue(
                    await asyncio.to_thread(cleanup_started.wait, 2)
                )
                second = await main.destroy(job['id'])
                release_cleanup.set()
                await cleanup_task
                await asyncio.sleep(0)
            return job, main.jobs[job['id']], first, second, cleanup_calls

        with tempfile.TemporaryDirectory() as directory:
            original_job, managed_job, first, second, cleanup_calls = asyncio.run(
                scenario(directory)
            )

        self.assertEqual(first, {'status': 'destroying'})
        self.assertEqual(second, {'status': 'destroying'})
        self.assertEqual(cleanup_calls, ['destroying'])
        self.assertEqual(original_job['status'], 'complete')
        self.assertEqual(managed_job['status'], 'destroyed')
        self.assertNotIn(original_job['id'], main.job_tasks)

    def test_live_status_always_reports_recoverability(self):
        job = active_job(job_id='11feaec0ab18')
        main.jobs[job['id']] = job

        status = main.job_status(job['id'])

        self.assertTrue(status['live'])
        self.assertTrue(status['recoverable'])

    def test_terminal_supervisor_is_joined_without_falsifying_outcome(self):
        async def scenario(directory):
            job = active_job(job_id='7eaa1a1ace19', status='complete')
            job['results'] = [{
                'id': 'deathstarbench',
                'name': 'DeathStarBench',
                'status': 'completed',
            }]

            async def finishing_supervisor():
                await asyncio.sleep(0)

            supervisor = asyncio.create_task(finishing_supervisor())
            main.jobs[job['id']] = job
            main.job_tasks[job['id']] = supervisor

            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main, 'destroy_with_status'),
            ):
                response = await main.destroy(job['id'])
                cleanup_task = main.job_tasks[job['id']]
                await cleanup_task
                await asyncio.sleep(0)
            return job, main.jobs[job['id']], response

        with tempfile.TemporaryDirectory() as directory:
            original_job, managed_job, response = asyncio.run(scenario(directory))

        self.assertEqual(response, {'status': 'destroying'})
        self.assertEqual(original_job['status'], 'complete')
        self.assertEqual(managed_job['status'], 'destroying')
        self.assertFalse(managed_job.get('benchmark_interrupted', False))
        self.assertFalse(original_job['_cancel_event'].is_set())


if __name__ == '__main__':
    unittest.main()
