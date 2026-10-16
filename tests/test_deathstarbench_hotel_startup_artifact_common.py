"""The startup artifact source boundary delegates complete reconstruction."""

from pathlib import Path
import hashlib
import unittest
from unittest import mock

from scripts import deathstarbench_hotel_startup_artifact_common as common
from scripts import deathstarbench_hotel_initialization_artifact_common as baseline
from scripts.deathstarbench_artifact_common import PreparationError
from tests import test_deathstarbench_hotel_startup_validation as fixtures


class StartupHotelArtifactCommonTests(unittest.TestCase):
    def test_distinct_complete_program_inventory_and_original_input_selectors(self):
        self.assertEqual(common.PROGRAMS, tuple(sorted((*common.preparation.APPLICATIONS, 'hotel-initialize'))))
        self.assertEqual(len(common.PROGRAMS), 11)
        self.assertEqual(common.PLATFORMS, ('linux/amd64', 'linux/arm64'))
        for name in ('BASELINE_ARTIFACT', 'BASELINE_RUN', 'BASELINE_LOCK_SHA256', 'BASELINE_PUBLICATION_SHA256'):
            self.assertEqual(getattr(common, name), getattr(baseline, name))
        self.assertIs(common.verify_baseline_files, baseline.verify_baseline_files)

    def test_source_boundary_requires_both_full_inputs_without_hash_only_shortcut(self):
        expected = {'fixture': True}
        with mock.patch.object(common.validation, 'validate_preparation', return_value=expected) as reconstruct:
            self.assertIs(common.validate_startup_preparation(Path('/synthetic/prepared'), Path('/synthetic/upstream')), expected)
        reconstruct.assert_called_once_with(Path('/synthetic/prepared'), Path('/synthetic/upstream'))

    def test_real_reconstruction_checks_source_tree_and_rejects_rehashed_drift(self):
        helper = fixtures.HotelStartupValidationTests()
        self.addCleanup(helper.doCleanups)
        helper.setUp()
        identity = common.validate_startup_preparation(helper.prepared, helper.upstream)
        self.assertEqual(identity['config_sha256'], helper.manifest['config_sha256'])
        source = helper.prepared / 'app/startup/startup.go'
        source.write_bytes(source.read_bytes() + b'\n// rehashed untrusted drift\n')
        helper.rehash_manifest(startup_asset_sha256={
            name: hashlib.sha256((helper.prepared / 'app' / target).read_bytes()).hexdigest()
            for name, target in common.preparation.ASSET_TARGETS.items()
        })
        with self.assertRaises(PreparationError):
            common.validate_startup_preparation(helper.prepared, helper.upstream)
