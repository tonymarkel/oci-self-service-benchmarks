import unittest
import asyncio
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from app import main
from app.deathstarbench_contract import (
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DISTRIBUTED_IMAGE_SET_REVISION,
    DISTRIBUTED_RUNTIME_REVISION,
    DISTRIBUTED_TIERED_PROFILE,
    DISTRIBUTED_WORKLOAD_REVISION,
    K3S_VERSION,
    RUNTIME_PROFILES,
    SINGLE_HOST_PROFILE,
    SINGLE_HOST_RUNTIME_REVISION,
    require_released_runtime,
    runtime_profile,
)
from app.models import BenchmarkPlan


def distributed_plan():
    return BenchmarkPlan(
        region='us-ashburn-1',
        shape='VM.Standard.E5.Flex',
        memory_gb=32,
        ssh_private_key='private-key',
        ssh_public_key='ssh-rsa public-key',
        storage={'additional_volume': False},
        benchmarks=['deathstarbench'],
        deathstarbench={
            'topology_id': 'distributed_tiered_v1',
            'runtime_id': 'k3s_v1',
            'workload': 'social_network',
        },
    )


class DeathStarBenchRuntimeContractTests(unittest.TestCase):
    def test_single_host_profile_is_released_and_auditable(self):
        profile = runtime_profile('single_host_v1', 'podman_compose_v1')

        self.assertIs(profile, SINGLE_HOST_PROFILE)
        self.assertTrue(profile.released)
        self.assertEqual(profile.node_count, 2)
        self.assertEqual(
            profile.runtime_revision,
            SINGLE_HOST_RUNTIME_REVISION,
        )
        self.assertEqual(
            profile.metadata()['service_placement_revision'],
            'all-workload-services-on-runner-v1',
        )

    def test_distributed_profile_pins_exact_k3s_candidate(self):
        profile = runtime_profile('distributed_tiered_v1', 'k3s_v1')

        self.assertIs(profile, DISTRIBUTED_TIERED_PROFILE)
        self.assertTrue(profile.released)
        self.assertRegex(K3S_VERSION, r'^v\d+\.\d+\.\d+\+k3s\d+$')
        self.assertIn(K3S_VERSION.removeprefix('v').replace('+', '-'), DISTRIBUTED_RUNTIME_REVISION)
        self.assertTrue(DISTRIBUTED_RUNTIME_REVISION.endswith('-v5'))
        self.assertEqual(
            DISTRIBUTED_WORKLOAD_REVISION,
            'social-network-6ecb097-workload-v1',
        )
        self.assertEqual(
            DISTRIBUTED_IMAGE_SET_REVISION,
            'social-network-images-v1',
        )
        self.assertEqual(
            DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
            'deathstarbench_workload_state_v1',
        )
        self.assertEqual(
            profile.roles,
            ('control', 'database', 'cache', 'application', 'load_generator'),
        )

    def test_unknown_profiles_fail_closed_and_released_profiles_resolve(self):
        with self.assertRaisesRegex(ValueError, 'Unsupported'):
            runtime_profile('single_host_v2', 'podman_compose_v1')

        self.assertIs(
            require_released_runtime('distributed_tiered_v1', 'k3s_v1'),
            DISTRIBUTED_TIERED_PROFILE,
        )

        self.assertIs(
            require_released_runtime('single_host_v1', 'podman_compose_v1'),
            SINGLE_HOST_PROFILE,
        )

    def test_profiles_are_immutable(self):
        with self.assertRaises(FrozenInstanceError):
            SINGLE_HOST_PROFILE.node_count = 3
        with self.assertRaises(TypeError):
            RUNTIME_PROFILES[('single_host_v2', 'podman_compose_v1')] = (
                SINGLE_HOST_PROFILE
            )

    def test_released_runtime_routes_to_public_distributed_dispatch(self):
        plan = distributed_plan()

        with (
            patch.object(
                main,
                'load_run_distributed_image_lock',
                return_value={'released': True},
            ),
            patch.object(
                main,
                'provision_public_distributed_deathstarbench',
                return_value='distributed',
            ) as distributed,
            patch.object(main, 'dispatch_provider_operation') as dispatch,
        ):
            self.assertEqual(
                main.provision(
                    {'id': '123456789abc', 'resources': {}},
                    plan,
                ),
                'distributed',
            )

        dispatch.assert_not_called()
        distributed.assert_called_once()

    def test_direct_legacy_plan_defaults_to_the_released_compact_contract(self):
        plan = SimpleNamespace(
            provider='aws',
            benchmarks=['deathstarbench'],
            deathstarbench=SimpleNamespace(workload='social_network'),
        )

        with patch.object(
            main,
            'dispatch_provider_operation',
            return_value='dispatched',
        ) as dispatch:
            self.assertEqual(
                main.provision({'resources': {}}, plan),
                'dispatched',
            )

        dispatch.assert_called_once()

    def test_job_api_checks_coordinated_bundle_before_key_validation(self):
        plan = distributed_plan()

        with (
            patch.object(
                main,
                'preflight_distributed_deathstarbench_release',
                side_effect=main.PublicDistributedLifecycleError(
                    'coordinated release marker',
                ),
            ) as preflight,
            patch.object(main, 'derive_public_key') as derive,
        ):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(main.create_job(plan))

        self.assertEqual(raised.exception.status_code, 422)
        self.assertEqual(
            raised.exception.detail,
            'coordinated release marker',
        )
        preflight.assert_called_once_with()
        derive.assert_not_called()


if __name__ == '__main__':
    unittest.main()
