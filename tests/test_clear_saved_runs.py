import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

from app import main
from app.models import ClearSavedRunsRequest
from app.run_lease import (
    LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME,
    RUN_LEASE_FILENAME,
    RunLeaseHeldError,
    acquire_run_lease,
)


def saved_state(
    root,
    run_id,
    status,
    *,
    resources=None,
    plan=None,
):
    directory = root / run_id
    directory.mkdir()
    state = {
        'id': run_id,
        'status': status,
        'error': None,
        'cleanup_error': None,
        'benchmark_status': 'complete' if status == 'complete' else 'failed',
        'created_at': '2026-09-01T12:00:00+00:00',
        'updated_at': '2026-09-01T12:01:00+00:00',
        'plan': plan or {},
        'events': [],
        'resources': resources or {},
        'results': [],
    }
    (directory / 'state.json').write_text(json.dumps(state))
    return directory, state


def response_json(response):
    return json.loads(response.body.decode())


class ClearSavedRunsTests(unittest.TestCase):
    def setUp(self):
        main.jobs.clear()
        main.job_tasks.clear()
        main.job_cancel_events.clear()
        with main.job_processes_lock:
            main.job_processes.clear()

    tearDown = setUp

    def test_html_shells_are_not_cached_across_asset_contract_changes(self):
        for response in (main.home(), main.history()):
            with self.subTest(path=response.path):
                self.assertEqual(response.headers['cache-control'], 'no-store')

    def test_confirmation_is_required_by_the_typed_delete_route(self):
        with self.assertRaises(ValidationError):
            ClearSavedRunsRequest.model_validate({})
        with self.assertRaises(ValidationError):
            ClearSavedRunsRequest.model_validate({'confirmed': False})
        with self.assertRaises(ValidationError):
            ClearSavedRunsRequest.model_validate({'confirmed': 1})
        self.assertTrue(
            ClearSavedRunsRequest.model_validate({'confirmed': True}).confirmed
        )

        route = next(
            route
            for route in main.app.routes
            if route.path == '/api/reports' and 'DELETE' in route.methods
        )
        self.assertEqual(
            route.body_field.field_info.annotation,
            ClearSavedRunsRequest,
        )
        with self.assertRaises(HTTPException) as raised:
            main.clear_saved_runs(
                ClearSavedRunsRequest.model_construct(confirmed=False)
            )
        self.assertEqual(raised.exception.status_code, 422)

    def test_deletes_only_safe_finalized_history_and_terminal_registries(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            destroyed, destroyed_job = saved_state(
                root,
                'aaaaaaaaaaaa',
                'destroyed',
                resources={'aws_account_id': '123456789012'},
                plan={'provider': 'aws'},
            )
            reported, _ = saved_state(root, 'bbbbbbbbbbbb', 'reported')
            failed, _ = saved_state(root, 'cccccccccccc', 'failed')
            legacy = root / 'dddddddddddd'
            legacy.mkdir()
            (legacy / 'report.html').write_text('<p>legacy</p>')

            main.jobs['aaaaaaaaaaaa'] = destroyed_job
            main.job_tasks['aaaaaaaaaaaa'] = SimpleNamespace(
                done=lambda: True
            )
            main.job_cancel_events['aaaaaaaaaaaa'] = object()
            with main.job_processes_lock:
                main.job_processes['aaaaaaaaaaaa'] = set()

            with (
                patch.object(main, 'RUNS', root),
                patch.object(
                    main,
                    'destroy_resources',
                    side_effect=AssertionError(
                        'clearing history must not invoke cloud cleanup'
                    ),
                ),
            ):
                response = main.clear_saved_runs(
                    ClearSavedRunsRequest(confirmed=True)
                )
            payload = response_json(response)

            self.assertEqual(response.headers['cache-control'], 'no-store')
            self.assertEqual(payload, {
                'deleted_count': 4,
                'preserved_count': 0,
                'preserved_run_ids': [],
            })
            self.assertTrue(root.is_dir())
            for directory in (destroyed, reported, failed, legacy):
                self.assertFalse(directory.exists())
            self.assertNotIn('aaaaaaaaaaaa', main.jobs)
            self.assertNotIn('aaaaaaaaaaaa', main.job_tasks)
            self.assertNotIn('aaaaaaaaaaaa', main.job_cancel_events)
            self.assertNotIn('aaaaaaaaaaaa', main.job_processes)

    def test_preserves_every_run_that_may_still_need_lifecycle_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directories = []
            for run_id, status in (
                ('111111111111', 'queued'),
                ('222222222222', 'complete'),
                ('333333333333', 'cleanup_failed'),
                ('444444444444', 'interrupted'),
                ('555555555555', 'unknown'),
            ):
                directory, _ = saved_state(root, run_id, status)
                directories.append(directory)

            corrupt = root / '666666666666'
            corrupt.mkdir()
            (corrupt / 'state.json').write_text('{not valid json')
            directories.append(corrupt)

            recoverable, _ = saved_state(
                root,
                '777777777777',
                'failed',
                resources={'aws_account_id': '123456789012'},
                plan={'provider': 'aws'},
            )
            directories.append(recoverable)

            mismatched_provider, _ = saved_state(
                root,
                'abababababab',
                'failed',
                resources={
                    'gcp_project_id': 'benchmark-project',
                    'gcp_resource_prefix': 'bench-abababababab',
                },
                plan={'provider': 'oci'},
            )
            directories.append(mismatched_provider)

            active_task, _ = saved_state(root, '888888888888', 'destroyed')
            active_process, _ = saved_state(root, '999999999999', 'destroyed')
            directories.extend((active_task, active_process))
            main.job_tasks['888888888888'] = SimpleNamespace(
                done=lambda: False
            )
            process = object()
            with main.job_processes_lock:
                main.job_processes['999999999999'] = {process}

            with patch.object(main, 'RUNS', root):
                payload = response_json(main.clear_saved_runs(
                    ClearSavedRunsRequest(confirmed=True)
                ))

            expected_ids = [directory.name for directory in directories]
            self.assertEqual(payload, {
                'deleted_count': 0,
                'preserved_count': len(expected_ids),
                'preserved_run_ids': sorted(expected_ids),
            })
            self.assertTrue(all(directory.exists() for directory in directories))
            self.assertIn('888888888888', main.job_tasks)
            self.assertIn(process, main.job_processes['999999999999'])

    def test_empty_archive_is_an_idempotent_no_op(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(
            main,
            'RUNS',
            Path(temporary),
        ):
            first = response_json(main.clear_saved_runs(
                ClearSavedRunsRequest(confirmed=True)
            ))
            second = response_json(main.clear_saved_runs(
                ClearSavedRunsRequest(confirmed=True)
            ))

        expected = {
            'deleted_count': 0,
            'preserved_count': 0,
            'preserved_run_ids': [],
        }
        self.assertEqual(first, expected)
        self.assertEqual(second, expected)

    def test_noncanonical_symlinked_and_artifact_free_paths_are_untouched(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            noncanonical = root / 'notes'
            noncanonical.mkdir()
            (noncanonical / 'state.json').write_text('{}')
            uppercase = root / 'ABCDEFABCDEF'
            uppercase.mkdir()
            (uppercase / 'report.html').write_text('report')
            artifact_free = root / 'aaaaaaaaaaaa'
            artifact_free.mkdir()
            (artifact_free / 'plan.json').write_text('{}')
            symlink = root / 'bbbbbbbbbbbb'
            symlink.symlink_to(noncanonical, target_is_directory=True)

            with patch.object(main, 'RUNS', root):
                payload = response_json(main.clear_saved_runs(
                    ClearSavedRunsRequest(confirmed=True)
                ))

            self.assertEqual(payload, {
                'deleted_count': 0,
                'preserved_count': 0,
                'preserved_run_ids': [],
            })
            for path in (noncanonical, uppercase, artifact_free, symlink):
                self.assertTrue(path.exists())
            self.assertTrue(symlink.is_symlink())

    def test_partial_delete_stays_quarantined_after_lock_inode_is_removed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory, job = saved_state(root, 'aaaaaaaaaaaa', 'destroyed')
            main.jobs[job['id']] = job

            def partial_delete(quarantine):
                (quarantine / RUN_LEASE_FILENAME).unlink()
                (
                    quarantine
                    / LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME
                ).unlink()
                raise PermissionError('read only')

            with (
                patch.object(main, 'RUNS', root),
                patch.object(
                    main.shutil,
                    'rmtree',
                    side_effect=partial_delete,
                ),
            ):
                payload = response_json(main.clear_saved_runs(
                    ClearSavedRunsRequest(confirmed=True)
                ))

            self.assertEqual(payload, {
                'deleted_count': 0,
                'preserved_count': 1,
                'preserved_run_ids': ['aaaaaaaaaaaa'],
            })
            self.assertFalse(directory.exists())
            quarantines = list(root.glob('.deleting-aaaaaaaaaaaa-*'))
            self.assertEqual(len(quarantines), 1)
            self.assertTrue((quarantines[0] / 'state.json').is_file())
            self.assertNotIn(job['id'], main.jobs)

    def test_clear_acquires_lease_before_final_revalidation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory, _ = saved_state(root, 'aaaaaaaaaaaa', 'destroyed')
            real_eligibility = main.saved_run_can_be_deleted
            observed_conflict = []

            def eligibility(path, *, lease_held=False):
                if lease_held:
                    with self.assertRaises(RunLeaseHeldError):
                        acquire_run_lease(
                            root,
                            path.name,
                            'qualification-cli',
                        )
                    observed_conflict.append(path.name)
                return real_eligibility(path, lease_held=lease_held)

            with (
                patch.object(main, 'RUNS', root),
                patch.object(
                    main,
                    'saved_run_can_be_deleted',
                    side_effect=eligibility,
                ),
            ):
                payload = response_json(main.clear_saved_runs(
                    ClearSavedRunsRequest(confirmed=True)
                ))

            self.assertEqual(observed_conflict, ['aaaaaaaaaaaa'])
            self.assertEqual(payload['deleted_count'], 1)
            self.assertFalse(directory.exists())

    def test_clear_preserves_run_owned_by_another_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory, _ = saved_state(root, 'aaaaaaaaaaaa', 'destroyed')
            with (
                patch.object(main, 'RUNS', root),
                acquire_run_lease(root, 'aaaaaaaaaaaa', 'qualification-cli'),
            ):
                payload = response_json(main.clear_saved_runs(
                    ClearSavedRunsRequest(confirmed=True)
                ))

            self.assertEqual(payload, {
                'deleted_count': 0,
                'preserved_count': 1,
                'preserved_run_ids': ['aaaaaaaaaaaa'],
            })
            self.assertTrue(directory.exists())


if __name__ == '__main__':
    unittest.main()
