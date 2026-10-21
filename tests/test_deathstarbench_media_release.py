import copy
import hashlib
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from app import deathstarbench_media_release as media


class MediaReleaseTests(unittest.TestCase):
    def test_published_lock_uses_native_apps_and_actual_driver(self):
        lock = media.load_media_runtime_lock()
        self.assertEqual(lock['workload_id'], 'media_microservices')
        self.assertEqual(lock['dataset_manifest']['users']['count'], 1000)
        self.assertEqual(lock['dataset_manifest']['movies']['count'], 995)
        self.assertEqual(len(lock['dataset_manifest']['movies']['titles']), 1000)
        self.assertEqual(lock['dataset_manifest']['tmdb_records'], 0)
        self.assertEqual(lock['initializer_sha256'], hashlib.sha256(media.INITIALIZER_PATH.read_bytes()).hexdigest())
        self.assertNotEqual(lock['candidate_images']['platforms']['linux/amd64']['images']['media-microservices'],
                            lock['candidate_images']['platforms']['linux/arm64']['images']['media-microservices'])

    def test_malformed_crossed_or_changed_inputs_are_rejected(self):
        original = media.load_media_runtime_lock()
        for change in (
            lambda value: value.update(workload_id='hotel_reservation'),
            lambda value: value.update(schema_version=True),
            lambda value: value.update(load_generator_image='media:latest'),
            lambda value: value.update(initializer_sha256='0' * 64),
            lambda value: value.update(dataset_manifest_sha256='0' * 64),
            lambda value: value['dataset_manifest']['users'].update(count=999),
            lambda value: value['candidate_images'].update(workload_id='hotel_reservation'),
        ):
            value = copy.deepcopy(original)
            change(value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                media.validate_media_runtime_lock(value)

    def test_initializer_byte_drift_is_rejected_before_cloud_calls(self):
        lock = media.load_media_runtime_lock()
        with patch.object(media, '_initializer_sha256', return_value='a' * 64), self.assertRaisesRegex(ValueError, 'initializer'):
            media.validate_media_runtime_lock(lock)

    def test_missing_media_assets_hide_only_media_from_catalog(self):
        from app import main
        for field in ('MOVIE_TITLES_PATH', 'INITIALIZER_PATH'):
            with self.subTest(field=field), patch.object(media, field, Path('/nonexistent-media-test-asset')):
                available = {item['id'] for item in main.catalog()['deathstarbench_workloads']}
                self.assertIn('hotel_reservation', available)
                self.assertIn('social_network', available)
                self.assertNotIn('media_microservices', available)
