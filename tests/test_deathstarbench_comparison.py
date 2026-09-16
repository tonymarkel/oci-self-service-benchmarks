import unittest

from app import comparison
from app.deathstarbench_contract import (
    DISTRIBUTED_TIERED_PROFILE,
    SINGLE_HOST_PROFILE,
    runtime_profile,
)
from app.models import (
    DEATHSTARBENCH_DISTRIBUTED_TIERED_TOPOLOGY_ID,
    DEATHSTARBENCH_K3S_RUNTIME_ID,
    DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID,
    DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID,
)


UPSTREAM_REVISION = '6ecb09706140f8730b5385c08f1386c654c3c526'


def plan(*, distributed=False):
    deathstarbench = {
        'workload': 'social_network',
        'warmup_seconds': 30,
        'duration_seconds': 60,
        'threads': 4,
        'connections': 64,
        'request_rate': 100,
    }
    if distributed:
        deathstarbench.update({
            'topology_id': DEATHSTARBENCH_DISTRIBUTED_TIERED_TOPOLOGY_ID,
            'runtime_id': DEATHSTARBENCH_K3S_RUNTIME_ID,
        })
    return {
        'provider': 'aws',
        'region': 'us-east-2',
        'shape': 'c7i.2xlarge',
        'ocpus': 8,
        'memory_gb': 16,
        'benchmarks': ['deathstarbench'],
        'llm_benchmarks': [],
        'deathstarbench': deathstarbench,
    }


def metadata(
    *,
    topology_id=DEATHSTARBENCH_DISTRIBUTED_TIERED_TOPOLOGY_ID,
    runtime_id=DEATHSTARBENCH_K3S_RUNTIME_ID,
    runtime_revision='v1.33.4+k3s1',
    topology_revision=None,
    service_placement_revision=None,
):
    profile = runtime_profile(topology_id, runtime_id)
    return {
        'upstream_revision': UPSTREAM_REVISION,
        'topology_id': topology_id,
        'topology_revision': (
            topology_revision or profile.topology_revision
        ),
        'runtime_id': runtime_id,
        'runtime_revision': runtime_revision,
        'service_placement_revision': (
            service_placement_revision or profile.placement_revision
        ),
    }


def document(run_id, *, runtime_revision='v1.33.4+k3s1', value=10.0):
    return comparison.build_results_artifact({
        'id': run_id,
        'status': 'destroyed',
        'benchmark_status': 'complete',
        'plan': plan(distributed=True),
        'results': [{
            'id': 'deathstarbench',
            'name': 'DeathStarBench — Social Network',
            'status': 'completed',
            'metadata': metadata(runtime_revision=runtime_revision),
            'metrics': {'p95_ms': value},
        }],
    })


class DeathStarBenchComparisonContractTests(unittest.TestCase):
    def test_historical_result_normalizes_to_single_host_runtime(self):
        historical = comparison.workload_fingerprint(
            'deathstarbench',
            plan(),
            {'upstream_revision': UPSTREAM_REVISION},
        )
        explicit = comparison.workload_fingerprint(
            'deathstarbench',
            plan(),
            metadata(
                topology_id=DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID,
                runtime_id=DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID,
                runtime_revision=(
                    comparison.DEATHSTARBENCH_LEGACY_RUNTIME_REVISION
                ),
            ),
        )

        self.assertFalse(historical['contract_unknown'])
        self.assertEqual(historical['fingerprint'], explicit['fingerprint'])
        self.assertEqual(
            historical['contract']['settings']['topology_id'],
            DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID,
        )
        self.assertEqual(
            historical['contract']['settings']['runtime_id'],
            DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID,
        )
        self.assertEqual(
            historical['contract']['settings']['runtime_revision'],
            comparison.DEATHSTARBENCH_LEGACY_RUNTIME_REVISION,
        )
        self.assertEqual(
            historical['contract']['settings']['topology_revision'],
            SINGLE_HOST_PROFILE.topology_revision,
        )
        self.assertEqual(
            historical['contract']['settings'][
                'service_placement_revision'
            ],
            SINGLE_HOST_PROFILE.placement_revision,
        )

    def test_distributed_runtime_requires_complete_attestation(self):
        missing = comparison.workload_fingerprint(
            'deathstarbench',
            plan(distributed=True),
            {'upstream_revision': UPSTREAM_REVISION},
        )
        partial = comparison.workload_fingerprint(
            'deathstarbench',
            plan(distributed=True),
            {
                'upstream_revision': UPSTREAM_REVISION,
                'topology_id': DEATHSTARBENCH_DISTRIBUTED_TIERED_TOPOLOGY_ID,
                'runtime_id': DEATHSTARBENCH_K3S_RUNTIME_ID,
            },
        )

        for contract in (missing, partial):
            with self.subTest(issues=contract['issues']):
                self.assertTrue(contract['contract_unknown'])
                self.assertIsNone(contract['fingerprint'])
                self.assertTrue(any(
                    'runtime' in issue.lower()
                    for issue in contract['issues']
                ))

    def test_every_partially_attested_result_fails_closed(self):
        complete = metadata()
        marker_keys = (
            'topology_id',
            'topology_revision',
            'runtime_id',
            'runtime_revision',
            'service_placement_revision',
        )

        for missing_key in marker_keys:
            with self.subTest(missing_key=missing_key):
                partial = dict(complete)
                partial.pop(missing_key)
                contract = comparison.workload_fingerprint(
                    'deathstarbench',
                    plan(distributed=True),
                    partial,
                )

                self.assertTrue(contract['contract_unknown'])
                self.assertIsNone(contract['fingerprint'])
                self.assertTrue(any(
                    'complete DeathStarBench execution attestation' in issue
                    and missing_key in issue
                    for issue in contract['issues']
                ))

    def test_topology_runtime_and_revision_are_workload_identity(self):
        single_host = comparison.workload_fingerprint(
            'deathstarbench',
            plan(),
            {'upstream_revision': UPSTREAM_REVISION},
        )
        distributed = comparison.workload_fingerprint(
            'deathstarbench',
            plan(distributed=True),
            metadata(),
        )

        differences = comparison.explain_workload_mismatch(
            single_host,
            distributed,
        )

        self.assertNotEqual(
            single_host['fingerprint'],
            distributed['fingerprint'],
        )
        self.assertEqual(
            {
                'settings.topology_id',
                'settings.topology_revision',
                'settings.runtime_id',
                'settings.runtime_revision',
                'settings.service_placement_revision',
            },
            {
                difference['path']
                for difference in differences
                if difference['path'].startswith('settings.')
                and difference['path'] in {
                    'settings.topology_id',
                    'settings.topology_revision',
                    'settings.runtime_id',
                    'settings.runtime_revision',
                    'settings.service_placement_revision',
                }
            },
        )

    def test_registered_topology_and_placement_revisions_are_required(self):
        cases = (
            (
                'topology_revision',
                {'topology_revision': 'distributed-tiered-placement-v999'},
                'topology revision',
            ),
            (
                'service_placement_revision',
                {'service_placement_revision': 'frontend-everywhere-v999'},
                'service placement revision',
            ),
        )

        for label, overrides, expected_issue in cases:
            with self.subTest(label=label):
                contract = comparison.workload_fingerprint(
                    'deathstarbench',
                    plan(distributed=True),
                    metadata(**overrides),
                )

                self.assertTrue(contract['contract_unknown'])
                self.assertIsNone(contract['fingerprint'])
                self.assertTrue(any(
                    expected_issue in issue.lower()
                    and 'registered topology/runtime profile' in issue
                    for issue in contract['issues']
                ))

    def test_historical_fully_attested_runtime_revision_remains_comparable(self):
        historical_runtime_revision = 'v1.33.4+k3s1'
        contract = comparison.workload_fingerprint(
            'deathstarbench',
            plan(distributed=True),
            metadata(runtime_revision=historical_runtime_revision),
        )

        self.assertFalse(contract['contract_unknown'])
        self.assertIsNotNone(contract['fingerprint'])
        self.assertNotEqual(
            historical_runtime_revision,
            DISTRIBUTED_TIERED_PROFILE.runtime_revision,
        )
        self.assertEqual(
            contract['contract']['settings']['runtime_revision'],
            historical_runtime_revision,
        )

    def test_fully_attested_runtime_revision_must_be_nonempty(self):
        for invalid_revision in ('', '   ', None):
            with self.subTest(runtime_revision=invalid_revision):
                attestation = metadata()
                attestation['runtime_revision'] = invalid_revision
                contract = comparison.workload_fingerprint(
                    'deathstarbench',
                    plan(distributed=True),
                    attestation,
                )

                self.assertTrue(contract['contract_unknown'])
                self.assertIsNone(contract['fingerprint'])
                self.assertTrue(any(
                    'exact DeathStarBench runtime revision' in issue
                    for issue in contract['issues']
                ))

    def test_reported_execution_identity_must_match_plan(self):
        contract = comparison.workload_fingerprint(
            'deathstarbench',
            plan(distributed=True),
            metadata(
                topology_id=DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID,
                runtime_id=DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID,
                runtime_revision=(
                    comparison.DEATHSTARBENCH_LEGACY_RUNTIME_REVISION
                ),
            ),
        )

        self.assertTrue(contract['contract_unknown'])
        self.assertIsNone(contract['fingerprint'])
        self.assertTrue(any(
            'does not match the plan' in issue
            for issue in contract['issues']
        ))

    def test_same_distributed_identity_builds_comparison_chart(self):
        payload = comparison.build_comparison_payload([
            document('run-a', value=10.0),
            document('run-b', value=12.0),
        ])

        self.assertEqual(payload['mismatches'], [])
        self.assertEqual(payload['excluded'], [])
        self.assertEqual(len(payload['charts']), 1)
        self.assertEqual(payload['charts'][0]['metric_id'], 'p95_ms')
        self.assertEqual(
            [item['run_id'] for item in payload['charts'][0]['values']],
            ['run-a', 'run-b'],
        )

    def test_different_runtime_revisions_do_not_compare_automatically(self):
        payload = comparison.build_comparison_payload([
            document('run-a', runtime_revision='v1.33.4+k3s1'),
            document('run-b', runtime_revision='v1.34.1+k3s1'),
        ])

        self.assertEqual(payload['charts'], [])
        self.assertEqual(len(payload['mismatches']), 1)
        self.assertEqual(
            {
                difference['path']
                for difference in payload['mismatches'][0]['differences']
            },
            {'settings.runtime_revision'},
        )
        self.assertEqual(
            {item['run_id'] for item in payload['excluded']},
            {'run-a', 'run-b'},
        )


if __name__ == '__main__':
    unittest.main()
