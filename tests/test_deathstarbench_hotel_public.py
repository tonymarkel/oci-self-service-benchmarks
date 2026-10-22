"""Hotel's normal application path retains its own immutable runtime inputs."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from app import deathstarbench_hotel_release as hotel
from app import deathstarbench_public as public
from app import main
from tests.test_deathstarbench_public_lifecycle import distributed_plan


class HotelPublicTests(unittest.TestCase):
    def plan(self):
        plan = distributed_plan('oci')
        plan['deathstarbench']['workload'] = 'hotel_reservation'
        return plan

    def test_real_lock_has_guarded_platforms_and_synthetic_dataset(self):
        lock = hotel.load_hotel_runtime_lock()
        self.assertIs(lock['experimental'], True)
        self.assertEqual(lock['workload_id'], 'hotel_reservation')
        self.assertEqual(set(lock['application_images']), {'linux/amd64', 'linux/arm64'})
        self.assertEqual(lock['dataset_fingerprint'], 'sha256:7d1e07da21efdf9df9cf2062f09546b8eb153c03af69be9e2cb4a635956d6681')
        self.assertNotEqual(lock['application_images']['linux/amd64'], lock['candidate_images']['platforms']['linux/amd64']['images']['hotel-reservation'])
        self.assertNotIn('dataset_nodes_sha256', lock)

    def test_malformed_and_crossed_locks_fail_before_dispatch(self):
        lock = hotel.load_hotel_runtime_lock()
        for change in (
            lambda v: v.update(workload_id='social_network'),
            lambda v: v.update(schema_version=True),
            lambda v: v.update(dataset_fingerprint='unknown'),
            lambda v: v.update(load_generator_image='image:latest'),
            lambda v: v['application_images'].update({'linux/amd64': 'image:latest'}),
            lambda v: v['application_images'].pop('linux/arm64'),
            lambda v: v['candidate_images'].update(workload_id='media_microservices'),
        ):
            changed = copy.deepcopy(lock)
            change(changed)
            with self.subTest(change=change), self.assertRaises(ValueError):
                hotel.validate_hotel_runtime_lock(changed)

    def test_hotel_snapshot_is_separate_and_remains_bound_to_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / '123456789abc'
            run.mkdir()
            target = public.snapshot_distributed_image_lock(run, workload='hotel_reservation')
            self.assertEqual(target.name, hotel.IMAGE_LOCK_FILENAME)
            self.assertFalse((run / public.IMAGE_LOCK_FILENAME).exists())
            self.assertEqual(public.load_run_distributed_image_lock(root, run.name, workload='hotel_reservation'), hotel.load_hotel_runtime_lock())
            with self.assertRaises(public.PublicDistributedLifecycleError):
                public.snapshot_distributed_image_lock(run, workload='hotel_reservation')

    def test_hotel_dispatch_uses_hotel_runtime_and_never_social_initializer(self):
        job = {'id': '123456789abc', 'resources': {}, 'results': []}
        lock = hotel.load_hotel_runtime_lock()
        with patch.object(public, 'run_hotel_benchmark', return_value={'status': 'completed'}) as run, \
                patch.object(public, 'prepare_distributed_k3s_candidate') as bootstrap, \
                patch.object(public, 'prepare_distributed_social_network_candidate') as social:
            result = public.run_distributed_deathstarbench(job, self.plan(), lock, execute=Mock(), emit=Mock(), persist=Mock())
        self.assertEqual(result['status'], 'completed')
        bootstrap.assert_called_once()
        run.assert_called_once()
        social.assert_not_called()

    def test_hotel_refuses_social_image_snapshot(self):
        lock = public.preflight_distributed_deathstarbench_release()
        with self.assertRaises(ValueError), patch.object(public, 'prepare_distributed_k3s_candidate') as bootstrap:
            public.run_distributed_deathstarbench({}, self.plan(), lock, execute=Mock(), emit=Mock(), persist=Mock())
        bootstrap.assert_not_called()

    def test_catalog_workloads_are_available_independently(self):
        for available in ({'social_network'}, {'hotel_reservation'},
                          {'social_network', 'hotel_reservation'}, {'media_microservices'},
                          {'social_network', 'media_microservices'},
                          {'hotel_reservation', 'media_microservices'},
                          {'social_network', 'hotel_reservation', 'media_microservices'}, set()):
            def preflight(*, workload='social_network'):
                if workload not in available:
                    raise public.PublicDistributedLifecycleError('Unavailable test lock')
                return {}
            with self.subTest(available=available), patch.object(
                    main, 'preflight_distributed_deathstarbench_release', side_effect=preflight):
                catalog = main.catalog()
                topology = next(item for item in catalog['deathstarbench_topologies']
                                if item['topology_id'] == 'distributed_tiered_v1')
                self.assertEqual(set(topology['workloads']), available)
                self.assertEqual(topology['released'], bool(available))


if __name__ == '__main__':
    unittest.main()
