import asyncio
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app import main


PROVIDERS = ('aws', 'azure', 'gcp', 'oci')


def exact_public_job(provider, *, job_id='abababababab', resources=None):
    job = {
        'id': job_id,
        'status': 'cleanup_pending',
        'error': None,
        'cleanup_error': None,
        'created_at': '2026-10-01T12:00:00+00:00',
        'updated_at': '2026-10-01T12:00:00+00:00',
        'plan': {
            'provider': provider,
            'benchmarks': ['deathstarbench'],
            'llm_benchmarks': [],
            'deathstarbench': {
                'topology_id': 'distributed_tiered_v1',
                'runtime_id': 'k3s_v1',
                'workload': 'social_network',
            },
        },
        'events': [],
        'resources': {} if resources is None else resources,
        'results': [],
        '_persist_state': True,
    }
    if resources is None:
        job['_distributed_empty_preflight_proof'] = (
            main._DISTRIBUTED_LIVE_EMPTY_PREFLIGHT_PROOF
        )
    return job


class DistributedPreflightTerminalTests(unittest.TestCase):
    def tearDown(self):
        main.jobs.clear()
        main.job_tasks.clear()
        main.job_cancel_events.clear()
        with main.job_processes_lock:
            main.job_processes.clear()

    def test_exact_empty_cleanup_persists_terminal_proof_without_dispatch(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as temporary:
                runs = Path(temporary)
                job = exact_public_job(provider)
                with (
                    patch.object(main, 'RUNS', runs),
                    patch.object(main, 'dispatch_provider_operation') as dispatch,
                ):
                    result = main.destroy_resources(job)

                marker = main._distributed_preflight_terminal_marker(
                    provider,
                    job['id'],
                )
                self.assertEqual(result, {
                    main.DISTRIBUTED_PREFLIGHT_TERMINAL_KEY: marker,
                })
                self.assertEqual(job['status'], 'destroyed')
                self.assertNotIn('cleanup_error', job)
                dispatch.assert_not_called()

                state = json.loads(
                    (runs / job['id'] / 'state.json').read_text()
                )
                self.assertEqual(state['status'], 'destroyed')
                self.assertEqual(state['resources'], job['resources'])
                self.assertTrue(
                    main._distributed_preflight_terminal_state(job, provider)
                )
                self.assertTrue(
                    main._distributed_candidate_local_terminal_state(job)
                )
                self.assertFalse(main.has_recoverable_resources(job))
                self.assertFalse(
                    main.has_recoverable_resources_for_any_provider(job)
                )

    def test_persisted_terminal_proof_replay_is_idempotent(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as temporary:
                runs = Path(temporary)
                job = exact_public_job(provider)
                with patch.object(main, 'RUNS', runs):
                    main.destroy_resources(job)
                    state_path = runs / job['id'] / 'state.json'
                    original_state = json.loads(state_path.read_text())
                    recovered = main.load_persisted_job(job['id'])
                    with patch.object(
                        main,
                        'dispatch_provider_operation',
                    ) as dispatch:
                        replay = main.destroy_resources(recovered)

                dispatch.assert_not_called()
                self.assertEqual(replay, job['resources'])
                self.assertEqual(recovered['status'], 'destroyed')
                self.assertEqual(recovered['events'], job['events'])
                self.assertEqual(
                    json.loads(state_path.read_text()),
                    original_state,
                )
                self.assertFalse(main.has_recoverable_resources(recovered))

    def test_preserve_status_still_persists_the_absence_proof(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as temporary:
                runs = Path(temporary)
                job = exact_public_job(provider)
                with (
                    patch.object(main, 'RUNS', runs),
                    patch.object(main, 'dispatch_provider_operation') as dispatch,
                ):
                    main.destroy_resources(job, preserve_status=True)

                dispatch.assert_not_called()
                self.assertEqual(job['status'], 'cleanup_pending')
                self.assertEqual(job['events'], [])
                state = json.loads(
                    (runs / job['id'] / 'state.json').read_text()
                )
                self.assertEqual(state['status'], 'cleanup_pending')
                self.assertEqual(state['resources'], job['resources'])
                self.assertTrue(
                    main._distributed_preflight_terminal_state(job, provider)
                )

    def test_corrupt_terminal_proof_is_rejected_and_recoverable(self):
        for provider in PROVIDERS:
            with self.subTest(provider=provider):
                job = exact_public_job(
                    provider,
                    resources={
                        main.DISTRIBUTED_PREFLIGHT_TERMINAL_KEY: {
                            **main._distributed_preflight_terminal_marker(
                                provider,
                                job_id='abababababab',
                            ),
                            'cloud_mutation_started': True,
                        },
                    },
                )
                job['status'] = 'destroyed'

                self.assertFalse(
                    main._distributed_preflight_terminal_state(job, provider)
                )
                self.assertFalse(
                    main._distributed_candidate_local_terminal_state(job)
                )
                self.assertTrue(main.has_recoverable_resources(job))
                self.assertTrue(
                    main.has_recoverable_resources_for_any_provider(job)
                )
                with self.assertRaises(HTTPException) as proof_error:
                    main.require_destroyed_run_terminal_proof(job)
                self.assertEqual(proof_error.exception.status_code, 409)
                with (
                    patch.object(main, 'dispatch_provider_operation') as dispatch,
                    self.assertRaisesRegex(
                        RuntimeError,
                        'no complete persisted ownership contract',
                    ),
                ):
                    main.destroy_resources(job)
                dispatch.assert_not_called()

    def test_retained_live_preflight_failure_transfers_proof_under_lease(self):
        async def cleanup_live_job(job):
            main.persist_job_state(job)
            main.jobs[job['id']] = job
            response = await main.destroy(job['id'])
            cleanup_task = main.job_tasks[job['id']]
            await cleanup_task
            await asyncio.sleep(0)
            return response, main.jobs[job['id']]

        with tempfile.TemporaryDirectory() as temporary, patch.object(
            main,
            'RUNS',
            Path(temporary),
        ), patch.object(main, 'dispatch_provider_operation') as dispatch:
            for index, provider in enumerate(PROVIDERS, start=1):
                with self.subTest(provider=provider):
                    job = exact_public_job(
                        provider,
                        job_id=f'00000000000{index}',
                    )
                    job['status'] = 'failed'
                    job['plan']['destroy_after_completion'] = False

                    response, cleaned = asyncio.run(cleanup_live_job(job))

                    self.assertEqual(response, {'status': 'destroying'})
                    self.assertEqual(cleaned['status'], 'destroyed')
                    self.assertTrue(
                        main._distributed_preflight_terminal_state(
                            cleaned,
                            provider,
                        )
                    )
                    self.assertNotIn(
                        '_distributed_empty_preflight_proof',
                        json.loads(
                            (
                                Path(temporary)
                                / job['id']
                                / 'state.json'
                            ).read_text()
                        ),
                    )
            dispatch.assert_not_called()

    def test_recovered_empty_and_copied_live_proofs_never_transfer(self):
        provider = 'aws'
        job_id = '121212121212'
        live = exact_public_job(provider, job_id=job_id)
        persisted = copy.deepcopy(live)
        persisted.pop('_distributed_empty_preflight_proof', None)

        # A disk-recovered document has no process-only source object.
        self.assertFalse(main._transfer_live_distributed_preflight_proof(
            job_id,
            None,
            persisted,
        ))
        self.assertNotIn('_distributed_empty_preflight_proof', persisted)

        main.jobs[job_id] = live
        copied_live = dict(live)
        mismatch_cases = {
            'copied_registry_object': (copied_live, copy.deepcopy(persisted)),
            'copied_job_id': (
                live,
                {**copy.deepcopy(persisted), 'id': '343434343434'},
            ),
            'copied_plan': (
                live,
                {
                    **copy.deepcopy(persisted),
                    'plan': {
                        **copy.deepcopy(persisted['plan']),
                        'provider': 'oci',
                    },
                },
            ),
            'live_resource_evidence': (
                {**live, 'resources': {'unexpected': True}},
                copy.deepcopy(persisted),
            ),
            'persisted_resource_evidence': (
                live,
                {**copy.deepcopy(persisted), 'resources': {'unexpected': True}},
            ),
        }
        for label, (source, target) in mismatch_cases.items():
            with self.subTest(label=label):
                self.assertFalse(
                    main._transfer_live_distributed_preflight_proof(
                        job_id,
                        source,
                        target,
                    )
                )
                self.assertNotIn(
                    '_distributed_empty_preflight_proof',
                    target,
                )

        # Even the exact empty persisted document remains untrusted after the
        # process-local source is gone.
        main.jobs.clear()
        recovered = copy.deepcopy(persisted)
        with (
            patch.object(main, 'dispatch_provider_operation') as recovered_dispatch,
            self.assertRaisesRegex(
                RuntimeError,
                'no complete persisted ownership contract',
            ),
        ):
            main.destroy_resources(recovered)
        recovered_dispatch.assert_not_called()


if __name__ == '__main__':
    unittest.main()
