import unittest

from pydantic import ValidationError

from app.models import (
    BenchmarkPlan,
    DEATHSTARBENCH_DEFAULT_RUNTIME_ID,
    DEATHSTARBENCH_DEFAULT_TOPOLOGY_ID,
    DEATHSTARBENCH_DISTRIBUTED_TIERED_TOPOLOGY_ID,
    DEATHSTARBENCH_K3S_RUNTIME_ID,
    DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID,
    DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID,
    DeathStarBenchOptions,
)


def legacy_plan_payload(**overrides):
    payload = {
        'region': 'us-ashburn-1',
        'shape': 'VM.Standard.E5.Flex',
        'memory_gb': 32,
        'ssh_private_key': 'private-key',
        'ssh_public_key': 'ssh-rsa public-key',
        'benchmarks': ['deathstarbench'],
        'deathstarbench': {
            'workload': 'social_network',
            'warmup_seconds': 45,
        },
    }
    payload.update(overrides)
    return payload


class DeathStarBenchTopologyModelTests(unittest.TestCase):
    def test_existing_options_default_to_versioned_single_host_contract(self):
        options = DeathStarBenchOptions(workload='social_network')

        self.assertEqual(
            options.topology_id,
            DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID,
        )
        self.assertEqual(
            options.runtime_id,
            DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID,
        )
        self.assertEqual(
            DEATHSTARBENCH_DEFAULT_TOPOLOGY_ID,
            DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID,
        )
        self.assertEqual(
            DEATHSTARBENCH_DEFAULT_RUNTIME_ID,
            DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID,
        )

    def test_existing_benchmark_plan_payload_remains_valid(self):
        plan = BenchmarkPlan.model_validate(legacy_plan_payload())

        self.assertEqual(plan.deathstarbench.workload, 'social_network')
        self.assertEqual(plan.deathstarbench.warmup_seconds, 45)
        self.assertEqual(
            plan.deathstarbench.topology_id,
            DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID,
        )
        self.assertEqual(
            plan.deathstarbench.runtime_id,
            DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID,
        )

    def test_distributed_topology_requires_k3s_runtime_contract(self):
        options = DeathStarBenchOptions(
            topology_id=DEATHSTARBENCH_DISTRIBUTED_TIERED_TOPOLOGY_ID,
            runtime_id=DEATHSTARBENCH_K3S_RUNTIME_ID,
        )

        self.assertEqual(
            options.model_dump()['topology_id'],
            'distributed_tiered_v1',
        )
        self.assertEqual(options.model_dump()['runtime_id'], 'k3s_v1')

    def test_unknown_versioned_identifiers_are_rejected(self):
        for field, value in (
            ('topology_id', 'single_host_v2'),
            ('runtime_id', 'k3s_stable'),
        ):
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValidationError):
                    DeathStarBenchOptions(**{field: value})

    def test_crossed_topology_runtime_pairs_are_rejected(self):
        for topology_id, runtime_id in (
            (
                DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID,
                DEATHSTARBENCH_K3S_RUNTIME_ID,
            ),
            (
                DEATHSTARBENCH_DISTRIBUTED_TIERED_TOPOLOGY_ID,
                DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID,
            ),
        ):
            with self.subTest(
                topology_id=topology_id,
                runtime_id=runtime_id,
            ):
                with self.assertRaisesRegex(
                    ValidationError,
                    'must be a supported versioned pair',
                ):
                    DeathStarBenchOptions(
                        topology_id=topology_id,
                        runtime_id=runtime_id,
                    )

    def test_contract_identifiers_are_immutable_after_validation(self):
        options = DeathStarBenchOptions()

        with self.assertRaisesRegex(ValidationError, 'Field is frozen'):
            options.topology_id = DEATHSTARBENCH_DISTRIBUTED_TIERED_TOPOLOGY_ID
        with self.assertRaisesRegex(ValidationError, 'Field is frozen'):
            options.runtime_id = DEATHSTARBENCH_K3S_RUNTIME_ID

        self.assertEqual(
            (options.topology_id, options.runtime_id),
            (
                DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID,
                DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID,
            ),
        )


if __name__ == '__main__':
    unittest.main()
