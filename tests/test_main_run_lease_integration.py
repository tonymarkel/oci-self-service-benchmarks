import asyncio
import json
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from app import main
from app.run_lease import (
    RUN_LEASE_FILENAME,
    acquire_run_lease,
    inspect_run_lease,
)


RUN_ID = 'a1b2c3d4e5f6'


def write_saved_run(root, *, status='testing'):
    directory = root / RUN_ID
    directory.mkdir()
    plan = {
        'provider': 'azure',
        'region': 'eastus',
        'benchmarks': [],
        'llm_benchmarks': [],
    }
    state = {
        'id': RUN_ID,
        'status': status,
        'error': None,
        'cleanup_error': None,
        'benchmark_status': 'pending',
        'created_at': '2026-09-20T12:00:00+00:00',
        'updated_at': '2026-09-20T12:01:00+00:00',
        'plan': plan,
        'events': [],
        'resources': {},
        'results': [],
    }
    (directory / 'plan.json').write_text(json.dumps(plan))
    (directory / 'state.json').write_text(json.dumps(state, indent=2))
    return directory


class MainRunLeaseIntegrationTests(unittest.TestCase):
    def setUp(self):
        main.jobs.clear()
        main.job_tasks.clear()
        main.job_cancel_events.clear()
        with main.job_processes_lock:
            main.job_processes.clear()

    tearDown = setUp

    def test_normal_api_run_holds_lease_until_supervisor_finishes(self):
        async def scenario(root):
            provision_started = threading.Event()
            release_provision = threading.Event()

            def provision(_job, _plan):
                provision_started.set()
                self.assertTrue(release_provision.wait(3))

            def run_benchmarks(job, _plan):
                job['results'].append({
                    'id': 'lease-test',
                    'status': 'completed',
                })

            sanitized_plan = {
                'provider': 'aws',
                'region': 'us-east-2',
                'benchmarks': [],
                'llm_benchmarks': [],
                'destroy_after_completion': False,
            }
            plan = SimpleNamespace(
                benchmarks=[],
                ssh_private_key='private',
                ssh_public_key='public',
                ssh_key_passphrase=None,
                destroy_after_completion=False,
                model_dump=lambda **_kwargs: dict(sanitized_plan),
            )
            adapter = SimpleNamespace(
                id='aws',
                short_name='AWS',
                provisioning_message='Provisioning test infrastructure.',
            )
            with (
                patch.object(main, 'RUNS', root),
                patch.object(main, 'provider_adapter', return_value=adapter),
                patch.object(
                    main,
                    'derive_public_key',
                    return_value='ssh-ed25519 public',
                ),
                patch.object(
                    main,
                    'normalize_public_key',
                    return_value='ssh-ed25519 public',
                ),
                patch.object(main, 'provision', side_effect=provision),
                patch.object(
                    main,
                    'run_benchmarks',
                    side_effect=run_benchmarks,
                ),
                patch.object(main, 'make_report'),
            ):
                response = await main.create_job(plan)
                job_id = response['id']
                supervisor = main.job_tasks[job_id]
                self.assertTrue(
                    await asyncio.to_thread(provision_started.wait, 3)
                )
                held_while_running = inspect_run_lease(root, job_id)

                # Model an independent app process: it has only the persisted
                # artifacts and shared flock, not this process's registries.
                with (
                    patch.dict(main.jobs, {}, clear=True),
                    patch.dict(main.job_tasks, {}, clear=True),
                    patch.dict(main.job_cancel_events, {}, clear=True),
                ):
                    external_status = main.job_status(job_id)
                    with self.assertRaises(HTTPException) as raised:
                        await main.destroy(job_id)

                release_provision.set()
                await supervisor
                await asyncio.sleep(0)
                held_after_finish = inspect_run_lease(root, job_id)
                final_state = main.load_persisted_job(job_id)
            return (
                held_while_running,
                external_status,
                raised.exception.status_code,
                held_after_finish,
                final_state,
            )

        with tempfile.TemporaryDirectory() as temporary:
            result = asyncio.run(scenario(Path(temporary)))

        held, status, destroy_status, released, final_state = result
        self.assertTrue(held.held)
        self.assertEqual(held.owner.owner_label, 'web-run')
        self.assertEqual(status['status'], 'provisioning')
        self.assertFalse(status['live'])
        self.assertEqual(destroy_status, 409)
        self.assertFalse(released.held)
        self.assertEqual(final_state['status'], 'complete')

    def test_cancelled_normal_supervisor_waits_for_mutating_worker_before_unlock(self):
        async def scenario(root):
            directory = write_saved_run(root, status='queued')
            worker_started = threading.Event()
            release_worker = threading.Event()

            def provision(_job, _plan):
                worker_started.set()
                self.assertTrue(release_worker.wait(3))

            plan = SimpleNamespace(destroy_after_completion=False)
            adapter = SimpleNamespace(
                provisioning_message='Provisioning test infrastructure.'
            )
            lease = acquire_run_lease(root, RUN_ID, 'web-run')
            with (
                patch.object(main, 'RUNS', root),
                patch.object(main, 'provider_adapter', return_value=adapter),
                patch.object(main, 'provision', side_effect=provision),
                patch.object(main, 'run_benchmarks'),
                patch.object(main, 'make_report'),
            ):
                job = main.load_persisted_job(RUN_ID)
                job['_cancel_event'] = threading.Event()
                supervisor = asyncio.create_task(
                    main.run_job(job, plan, run_lease=lease)
                )
                self.assertTrue(
                    await asyncio.to_thread(worker_started.wait, 3)
                )
                supervisor.cancel()
                await asyncio.sleep(0)
                held_while_worker_blocked = inspect_run_lease(
                    root,
                    RUN_ID,
                ).held
                task_done_while_worker_blocked = supervisor.done()
                release_worker.set()
                with self.assertRaises(asyncio.CancelledError):
                    await supervisor
                held_after_worker = inspect_run_lease(root, RUN_ID).held
                state_exists = (directory / 'state.json').is_file()
            return (
                held_while_worker_blocked,
                task_done_while_worker_blocked,
                held_after_worker,
                state_exists,
            )

        with tempfile.TemporaryDirectory() as temporary:
            result = asyncio.run(scenario(Path(temporary)))

        held_during, task_done, held_after, state_exists = result
        self.assertTrue(held_during)
        self.assertFalse(task_done)
        self.assertFalse(held_after)
        self.assertTrue(state_exists)

    def test_held_external_lease_preserves_persisted_active_status(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = write_saved_run(root)
            state_before = (directory / 'state.json').read_bytes()
            with (
                patch.object(main, 'RUNS', root),
                acquire_run_lease(root, RUN_ID, 'azure-qualifier'),
            ):
                status = main.job_status(RUN_ID)
                summary = main.run_summary(directory)

            self.assertEqual(
                (directory / 'state.json').read_bytes(),
                state_before,
            )

        self.assertEqual(status['status'], 'testing')
        self.assertFalse(status['live'])
        self.assertEqual(summary['status'], 'testing')

    def test_definitively_unheld_active_run_is_still_reported_interrupted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = write_saved_run(root)
            with patch.object(main, 'RUNS', root):
                status = main.job_status(RUN_ID)
                summary = main.run_summary(directory)

        self.assertEqual(status['status'], 'interrupted')
        self.assertEqual(summary['status'], 'interrupted')

    def test_held_lease_rejects_persisted_destroy_without_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = write_saved_run(root)
            state_before = (directory / 'state.json').read_bytes()
            with (
                patch.object(main, 'RUNS', root),
                patch.object(main, 'destroy_with_status') as cleanup,
                acquire_run_lease(root, RUN_ID, 'azure-qualifier'),
                self.assertRaises(HTTPException) as raised,
            ):
                asyncio.run(main.destroy(RUN_ID))

            self.assertEqual(
                (directory / 'state.json').read_bytes(),
                state_before,
            )

        self.assertEqual(raised.exception.status_code, 409)
        self.assertNotIn(RUN_ID, main.jobs)
        self.assertNotIn(RUN_ID, main.job_tasks)
        cleanup.assert_not_called()

    def test_ambiguous_lease_rejects_persisted_destroy_without_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = write_saved_run(root)
            lease_path = directory / RUN_LEASE_FILENAME
            lease_path.write_text('not valid lease metadata')
            lease_path.chmod(0o600)
            state_before = (directory / 'state.json').read_bytes()
            with (
                patch.object(main, 'RUNS', root),
                patch.object(main, 'destroy_with_status') as cleanup,
                self.assertRaises(HTTPException) as raised,
            ):
                status = main.job_status(RUN_ID)
                summary = main.run_summary(directory)
                asyncio.run(main.destroy(RUN_ID))

            self.assertEqual(
                (directory / 'state.json').read_bytes(),
                state_before,
            )

        self.assertEqual(raised.exception.status_code, 409)
        self.assertTrue(status['ownership_unknown'])
        self.assertIn('could not be verified', status['ownership_message'])
        self.assertTrue(summary['ownership_unknown'])
        self.assertNotIn(RUN_ID, main.jobs)
        self.assertNotIn(RUN_ID, main.job_tasks)
        cleanup.assert_not_called()

    def test_direct_cleanup_holds_lease_until_background_worker_finishes(self):
        async def scenario(root):
            write_saved_run(root, status='complete')
            cleanup_started = threading.Event()
            release_cleanup = threading.Event()
            observed = []

            def cleanup(job):
                observed.append(inspect_run_lease(root, RUN_ID).held)
                cleanup_started.set()
                self.assertTrue(release_cleanup.wait(2))
                observed.append(inspect_run_lease(root, RUN_ID).held)
                job['status'] = 'destroyed'
                main.persist_job_state(job)

            with (
                patch.object(main, 'RUNS', root),
                patch.object(main, 'destroy_with_status', side_effect=cleanup),
            ):
                first = await main.destroy(RUN_ID)
                task = main.job_tasks[RUN_ID]
                self.assertTrue(
                    await asyncio.to_thread(cleanup_started.wait, 2)
                )
                held_during_cleanup = inspect_run_lease(root, RUN_ID).held
                second = await main.destroy(RUN_ID)
                release_cleanup.set()
                await task
                await asyncio.sleep(0)
                held_after_cleanup = inspect_run_lease(root, RUN_ID).held
            return first, second, observed, held_during_cleanup, held_after_cleanup

        with tempfile.TemporaryDirectory() as temporary:
            result = asyncio.run(scenario(Path(temporary)))

        first, second, observed, held_during, held_after = result
        self.assertEqual(first, {'status': 'destroying'})
        self.assertEqual(second, {'status': 'destroying'})
        self.assertEqual(observed, [True, True])
        self.assertTrue(held_during)
        self.assertFalse(held_after)

    def test_cancelling_waiter_does_not_release_running_cleanup_lease(self):
        async def scenario(root):
            write_saved_run(root, status='complete')
            cleanup_started = threading.Event()
            release_cleanup = threading.Event()
            cleanup_finished = threading.Event()
            cleanup_calls = []

            def cleanup(job):
                cleanup_calls.append(job['status'])
                try:
                    cleanup_started.set()
                    self.assertTrue(release_cleanup.wait(2))
                    job['status'] = 'destroyed'
                    main.persist_job_state(job)
                finally:
                    cleanup_finished.set()

            with (
                patch.object(main, 'RUNS', root),
                patch.object(main, 'destroy_with_status', side_effect=cleanup),
            ):
                await main.destroy(RUN_ID)
                task = main.job_tasks[RUN_ID]
                self.assertTrue(
                    await asyncio.to_thread(cleanup_started.wait, 2)
                )
                task.cancel()
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                held_after_cancel = inspect_run_lease(root, RUN_ID).held
                task_done_while_worker_blocked = task.done()
                try:
                    repeated = ('response', await main.destroy(RUN_ID))
                except HTTPException as exc:
                    repeated = ('http', exc.status_code)
                release_cleanup.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertTrue(
                    await asyncio.to_thread(cleanup_finished.wait, 2)
                )
                await asyncio.sleep(0)
                held_after_worker = inspect_run_lease(root, RUN_ID).held
            return (
                held_after_cancel,
                task_done_while_worker_blocked,
                repeated,
                held_after_worker,
                cleanup_calls,
            )

        with tempfile.TemporaryDirectory() as temporary:
            result = asyncio.run(scenario(Path(temporary)))

        (
            held_after_cancel,
            _task_done,
            repeated,
            held_after_worker,
            cleanup_calls,
        ) = result
        self.assertTrue(held_after_cancel)
        self.assertTrue(
            repeated == ('response', {'status': 'destroying'})
            or repeated == ('http', 409),
            repeated,
        )
        self.assertFalse(held_after_worker)
        self.assertEqual(cleanup_calls, ['destroying'])

    def test_executor_callback_failure_does_not_release_submitted_worker_lease(self):
        async def scenario(root):
            write_saved_run(root, status='complete')
            captured = {}

            class SubmittedFuture:
                def add_done_callback(self, _callback):
                    raise RuntimeError('injected callback registration failure')

            def submit(_executor, function, job, lease):
                captured['function'] = function
                captured['job'] = job
                captured['lease'] = lease
                return SubmittedFuture()

            loop = asyncio.get_running_loop()
            with (
                patch.object(main, 'RUNS', root),
                patch.object(loop, 'run_in_executor', side_effect=submit),
                self.assertRaisesRegex(RuntimeError, 'callback registration'),
            ):
                await main.destroy(RUN_ID)
            held_after_submission = inspect_run_lease(root, RUN_ID).held
            captured['lease'].release()
            held_after_worker_release = inspect_run_lease(root, RUN_ID).held
            return held_after_submission, held_after_worker_release, captured

        with tempfile.TemporaryDirectory() as temporary:
            result = asyncio.run(scenario(Path(temporary)))

        held_after_submission, held_after_release, captured = result
        self.assertTrue(held_after_submission)
        self.assertFalse(held_after_release)
        self.assertIs(captured['function'], main.destroy_with_status_under_lease)
        self.assertEqual(captured['job']['status'], 'destroying')

    def test_failed_recovered_cleanup_retry_reacquires_lease(self):
        async def first_attempt(root):
            write_saved_run(root, status='complete')

            def fail_cleanup(job):
                job['status'] = 'cleanup_failed'
                job['cleanup_error'] = 'injected cleanup failure'
                main.persist_job_state(job)

            with (
                patch.object(main, 'RUNS', root),
                patch.object(
                    main,
                    'destroy_with_status',
                    side_effect=fail_cleanup,
                ) as cleanup,
            ):
                await main.destroy(RUN_ID)
                task = main.job_tasks[RUN_ID]
                await task
                await asyncio.sleep(0)
                first_calls = cleanup.call_count
            return first_calls

        async def conflicting_retry(root):
            with (
                patch.object(main, 'RUNS', root),
                patch.object(main, 'destroy_with_status') as cleanup,
                acquire_run_lease(root, RUN_ID, 'qualification-cli'),
                self.assertRaises(HTTPException) as raised,
            ):
                await main.destroy(RUN_ID)
            return raised.exception.status_code, cleanup.call_count

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first_calls = asyncio.run(first_attempt(root))
            status_code, retry_calls = asyncio.run(conflicting_retry(root))

        self.assertEqual(first_calls, 1)
        self.assertEqual(status_code, 409)
        self.assertEqual(retry_calls, 0)

    def test_persisted_destroy_reloads_manifest_after_acquiring_lease(self):
        async def scenario(root, directory):
            actual_load = main.load_persisted_job
            load_calls = 0
            cleaned_revisions = []

            def load(job_id):
                nonlocal load_calls
                load_calls += 1
                loaded = actual_load(job_id)
                if load_calls == 1:
                    state_path = directory / 'state.json'
                    state = json.loads(state_path.read_text())
                    state['resources']['revision'] = 'fresh-under-lease'
                    state_path.write_text(json.dumps(state, indent=2))
                return loaded

            def cleanup(job):
                cleaned_revisions.append(job['resources'].get('revision'))
                job['status'] = 'destroyed'
                main.persist_job_state(job)

            with (
                patch.object(main, 'RUNS', root),
                patch.object(main, 'load_persisted_job', side_effect=load),
                patch.object(main, 'destroy_with_status', side_effect=cleanup),
            ):
                response = await main.destroy(RUN_ID)
                task = main.job_tasks[RUN_ID]
                await task
                await asyncio.sleep(0)
            return response, load_calls, cleaned_revisions

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = write_saved_run(root, status='complete')
            result = asyncio.run(scenario(root, directory))

        response, load_calls, cleaned_revisions = result
        self.assertEqual(response, {'status': 'destroying'})
        self.assertEqual(load_calls, 2)
        self.assertEqual(cleaned_revisions, ['fresh-under-lease'])

    def test_saved_run_deletion_fails_closed_for_held_or_ambiguous_lease(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = write_saved_run(root, status='destroyed')
            with patch.object(main, 'RUNS', root):
                with acquire_run_lease(
                    root,
                    RUN_ID,
                    'azure-qualifier',
                ):
                    self.assertFalse(
                        main.saved_run_can_be_deleted(directory)
                    )

                self.assertTrue(main.saved_run_can_be_deleted(directory))
                lease_path = directory / RUN_LEASE_FILENAME
                lease_path.write_text('{"schema_version":')
                lease_path.chmod(0o600)
                self.assertFalse(main.saved_run_can_be_deleted(directory))


if __name__ == '__main__':
    unittest.main()
