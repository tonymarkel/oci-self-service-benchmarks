"""Media public runs retain workload-specific inputs and shared infrastructure."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from app import deathstarbench_media_release as media
from app import deathstarbench_public as public
from tests.test_deathstarbench_public_lifecycle import distributed_plan


class MediaPublicTests(unittest.TestCase):
    def plan(self):
        plan = distributed_plan('oci')
        plan['deathstarbench']['workload'] = 'media_microservices'
        return plan

    def test_snapshot_stays_bound_to_run_without_replacing_other_workloads(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / '123456789abc'
            run.mkdir()
            source = root / 'media-source.json'
            source.write_bytes(media.CHECKED_IN_IMAGE_LOCK.read_bytes())
            expected = media.load_media_runtime_lock()
            social = public.snapshot_distributed_image_lock(run)
            hotel = public.snapshot_distributed_image_lock(run, workload='hotel_reservation')
            target = public.snapshot_distributed_image_lock(run, source=source, workload='media_microservices')
            self.assertEqual(target.name, 'deathstarbench-media-image-lock.json')
            self.assertEqual(len({social, hotel, target}), 3)
            source.write_text('{}')
            self.assertEqual(public.load_run_distributed_image_lock(
                root, run.name, workload='media_microservices'), expected)
            with self.assertRaises(public.PublicDistributedLifecycleError):
                public.snapshot_distributed_image_lock(run, workload='media_microservices')

    def test_shared_bootstrap_runs_once_before_media_dispatch(self):
        job = {'id': '123456789abc', 'resources': {}, 'results': []}
        lock = media.load_media_runtime_lock()
        order = []
        def benchmark(actual_job, actual_plan, actual_lock, **callbacks):
            self.assertIs(actual_job, job)
            self.assertEqual(actual_plan, self.plan())
            self.assertEqual(actual_lock, lock)
            order.append('media')
            return {'status': 'completed', 'workload_id': 'media_microservices'}
        with patch.object(public, 'run_media_benchmark', side_effect=benchmark), \
                patch.object(public, 'prepare_distributed_k3s_candidate', side_effect=lambda *a, **k: order.append('k3s')), \
                patch.object(public, 'run_hotel_benchmark', side_effect=AssertionError('Hotel runtime dispatched')), \
                patch.object(public, 'prepare_distributed_social_network_candidate', side_effect=AssertionError('Social initializer dispatched')):
            result = public.run_distributed_deathstarbench(
                job, self.plan(), lock, execute=Mock(), emit=Mock(), persist=Mock())
        self.assertEqual(order, ['k3s', 'media'])
        self.assertEqual(result, {'status': 'completed', 'workload_id': 'media_microservices'})

    def test_crossed_or_malformed_lock_fails_before_bootstrap(self):
        invalid = copy.deepcopy(media.load_media_runtime_lock())
        invalid['workload_id'] = 'hotel_reservation'
        locks = [invalid, public.preflight_distributed_deathstarbench_release(),
                 public.preflight_distributed_deathstarbench_release(workload='hotel_reservation')]
        for lock in locks:
            with self.subTest(workload=lock.get('workload_id', 'social_network')), \
                    patch.object(public, 'prepare_distributed_k3s_candidate', side_effect=AssertionError('Bootstrap reached')):
                with self.assertRaises(ValueError):
                    public.run_distributed_deathstarbench(
                        {}, self.plan(), lock, execute=Mock(), emit=Mock(), persist=Mock())


if __name__ == '__main__':
    unittest.main()
