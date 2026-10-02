import copy
import unittest
from unittest.mock import patch

from fastapi import HTTPException

from app import main
from app.deathstarbench_contract import (
    DISTRIBUTED_PROVIDER_TERMINAL_KEY,
    distributed_provider_terminal_marker,
)


PROVIDERS = ('azure', 'gcp')


def exact_public_job(
    provider,
    *,
    job_id='abababababab',
    status='destroyed',
    resources=None,
):
    return {
        'id': job_id,
        'status': status,
        'error': None,
        'cleanup_error': None,
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
    }


def terminal_resources(provider, job_id):
    return {
        DISTRIBUTED_PROVIDER_TERMINAL_KEY:
            distributed_provider_terminal_marker(provider, job_id),
    }


class DistributedProviderTerminalTests(unittest.TestCase):
    def assert_recoverable_and_untrusted(self, job):
        provider = job['plan']['provider']
        self.assertFalse(
            main._cleared_distributed_provider_contract(job, provider)
        )
        self.assertFalse(
            main._distributed_candidate_local_terminal_state(job)
        )
        self.assertTrue(main.has_recoverable_resources(job))
        self.assertTrue(main.has_recoverable_resources_for_any_provider(job))
        with self.assertRaises(HTTPException) as error:
            main.require_destroyed_run_terminal_proof(job)
        self.assertEqual(error.exception.status_code, 409)

    def test_destroyed_empty_or_truncated_resources_fail_closed(self):
        for provider in PROVIDERS:
            incomplete_resources = (
                {},
                {'provider': provider},
                {
                    DISTRIBUTED_PROVIDER_TERMINAL_KEY: {
                        'schema_version': 1,
                        'provider': provider,
                    },
                },
            )
            for resources in incomplete_resources:
                with self.subTest(provider=provider, resources=resources):
                    self.assert_recoverable_and_untrusted(
                        exact_public_job(provider, resources=resources)
                    )

    def test_recovered_empty_resources_cannot_claim_preflight_absence(self):
        for provider in PROVIDERS:
            job = exact_public_job(
                provider,
                status='cleanup_pending',
                resources={},
            )

            with self.subTest(provider=provider), patch.object(
                main,
                'dispatch_provider_operation',
            ) as dispatch, self.assertRaisesRegex(
                RuntimeError,
                'no complete persisted ownership contract',
            ):
                main.destroy_resources(job)

            dispatch.assert_not_called()
            self.assertEqual(job['resources'], {})
            self.assertEqual(job['status'], 'cleanup_pending')

    def test_exact_run_bound_terminal_marker_is_trusted(self):
        for provider in PROVIDERS:
            job_id = f'{provider}terminal'
            job = exact_public_job(
                provider,
                job_id=job_id,
                resources=terminal_resources(provider, job_id),
            )

            with self.subTest(provider=provider):
                self.assertTrue(
                    main._cleared_distributed_provider_contract(job, provider)
                )
                self.assertTrue(
                    main._distributed_candidate_local_terminal_state(job)
                )
                self.assertFalse(main.has_recoverable_resources(job))
                self.assertFalse(
                    main.has_recoverable_resources_for_any_provider(job)
                )
                main.require_destroyed_run_terminal_proof(job)

    def test_copied_or_corrupt_terminal_markers_fail_closed(self):
        for provider in PROVIDERS:
            job_id = f'{provider}terminal'
            exact = distributed_provider_terminal_marker(provider, job_id)
            corruptions = {
                'copied_run': distributed_provider_terminal_marker(
                    provider,
                    'different-run',
                ),
                'wrong_provider': distributed_provider_terminal_marker(
                    'gcp' if provider == 'azure' else 'azure',
                    job_id,
                ),
                'wrong_schema': {**exact, 'schema_version': 2},
                'wrong_topology': {**exact, 'topology_id': 'single_host_v1'},
                'wrong_runtime': {**exact, 'runtime_id': 'podman_compose_v1'},
                'cleanup_not_complete': {
                    **exact,
                    'cloud_cleanup_completed': False,
                },
            }
            for corruption, marker in corruptions.items():
                with self.subTest(provider=provider, corruption=corruption):
                    job = exact_public_job(
                        provider,
                        job_id=job_id,
                        resources={DISTRIBUTED_PROVIDER_TERMINAL_KEY: marker},
                    )
                    self.assert_recoverable_and_untrusted(job)

    def test_copied_preflight_marker_fails_closed(self):
        for provider in PROVIDERS:
            job_id = f'{provider}terminal'
            job = exact_public_job(
                provider,
                job_id=job_id,
                resources={
                    main.DISTRIBUTED_PREFLIGHT_TERMINAL_KEY:
                        main._distributed_preflight_terminal_marker(
                            provider,
                            'different-run',
                        ),
                },
            )

            with self.subTest(provider=provider):
                self.assert_recoverable_and_untrusted(job)

    def test_marker_presence_survives_damaged_plan_classification(self):
        for provider in PROVIDERS:
            job_id = f'{provider}terminal'
            markers = (
                {
                    DISTRIBUTED_PROVIDER_TERMINAL_KEY:
                        distributed_provider_terminal_marker(provider, job_id),
                },
                {
                    main.DISTRIBUTED_PREFLIGHT_TERMINAL_KEY:
                        main._distributed_preflight_terminal_marker(
                            provider,
                            job_id,
                        ),
                },
                {
                    DISTRIBUTED_PROVIDER_TERMINAL_KEY: {
                        'schema_version': 1,
                        'provider': 'unknown-cloud',
                    },
                },
            )
            for resources in markers:
                with self.subTest(provider=provider, resources=resources):
                    job = exact_public_job(
                        provider,
                        job_id=job_id,
                        resources=resources,
                    )
                    job['plan']['deathstarbench'] = {}
                    self.assertFalse(
                        main._distributed_candidate_local_terminal_state(job)
                    )
                    self.assertTrue(main.has_recoverable_resources(job))
                    self.assertTrue(
                        main.has_recoverable_resources_for_any_provider(job)
                    )

    def test_common_distributed_evidence_survives_damaged_plan(self):
        for provider in PROVIDERS:
            for evidence_key in main.DISTRIBUTED_CANDIDATE_EVIDENCE_KEYS:
                with self.subTest(
                    provider=provider,
                    evidence_key=evidence_key,
                ):
                    job = exact_public_job(
                        provider,
                        resources={evidence_key: {}},
                    )
                    job['plan']['deathstarbench'] = {}
                    self.assertEqual(
                        main._distributed_candidate_providers(job),
                        frozenset({provider}),
                    )
                    self.assertFalse(
                        main._distributed_candidate_local_terminal_state(job)
                    )
                    self.assertTrue(main.has_recoverable_resources(job))
                    self.assertTrue(
                        main.has_recoverable_resources_for_any_provider(job)
                    )

    def test_terminal_marker_rejects_extra_ownership_or_unknown_state(self):
        provider_owned_keys = {
            'azure': ('azure_subscription_id', '/subscriptions/example'),
            'gcp': ('gcp_project', 'example-project'),
        }
        for provider in PROVIDERS:
            job_id = f'{provider}terminal'
            ownership_key, ownership_value = provider_owned_keys[provider]
            extras = (
                {ownership_key: ownership_value},
                {'unrecognized_resource_id': 'resource-123'},
            )
            for extra in extras:
                with self.subTest(provider=provider, extra=extra):
                    resources = terminal_resources(provider, job_id)
                    resources.update(extra)
                    self.assert_recoverable_and_untrusted(exact_public_job(
                        provider,
                        job_id=job_id,
                        resources=resources,
                    ))

    def test_terminal_marker_replay_finalizes_without_provider_dispatch(self):
        for provider in PROVIDERS:
            job_id = f'{provider}terminal'
            job = exact_public_job(
                provider,
                job_id=job_id,
                status='cleanup_pending',
                resources=terminal_resources(provider, job_id),
            )
            original_resources = copy.deepcopy(job['resources'])

            with self.subTest(provider=provider), patch.object(
                main,
                'dispatch_provider_operation',
            ) as dispatch:
                result = main.destroy_resources(job)
                replay = main.destroy_resources(job)

                dispatch.assert_not_called()
                self.assertEqual(result, original_resources)
                self.assertEqual(replay, original_resources)
                self.assertEqual(job['resources'], original_resources)
                self.assertEqual(job['status'], 'destroyed')
                self.assertIsNone(job.get('cleanup_error'))


if __name__ == '__main__':
    unittest.main()
