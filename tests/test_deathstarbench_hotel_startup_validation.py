"""Synthetic source reconstruction and offline metadata consistency checks.

Only the audited Git-source check and the preparation fixture's source patchers
are substituted. These tests neither execute Go nor claim image/runtime proof.
"""

import copy
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from app.deathstarbench_hotel_initialization import HotelInitializationError
from scripts import validate_deathstarbench_hotel_startup as validation
from scripts.deathstarbench_artifact_common import PreparationError, sha256, tree_sha256
from tests import test_deathstarbench_hotel_startup_preparation as fixtures
from tests.test_deathstarbench_hotel_initialization import fingerprint, metadata_fixture


class HotelStartupValidationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.HotelStartupPreparationTests()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.root = self.fixture.root
        self.upstream = self.fixture.upstream
        self.prepared, self.manifest = self.fixture.prepare('prepared')
        self.evidence = self.root / 'native'
        self.evidence.mkdir()
        self.pin = patch.object(validation.source_boundary, '_pinned_upstream', side_effect=lambda path: path)
        self.pin.start()
        self.addCleanup(self.pin.stop)
        self.original = metadata_fixture()
        self.original.pop('dataset_fingerprint')
        self.original['source_fingerprint'] = self.manifest['seed_source']['source_fingerprint']
        self.original['dataset_fingerprint'] = fingerprint(self.original)
        self.metadata = {}
        for component in validation.COMMANDS:
            self.metadata[component] = {
                'schema_version': 1, 'candidate_schema': 'hotel-guarded-startup-source-attestation-v1',
                'recipe_revision': validation.preparation.RECIPE_REVISION,
                'policy_revision': validation.preparation.POLICY_REVISION,
                'component': component,
                'config_sha256': 'sha256:' + self.manifest['config_sha256'],
                'dataset_fingerprint': self.original['dataset_fingerprint'],
                'source_fingerprint': self.original['source_fingerprint'], 'candidate_only': True,
                'scope': 'offline-source-contract-only-no-live-readiness-or-execution-authority',
                **{name: False for name in validation._ATTESTATION_FALSE},
            }
            self.write_metadata(component, self.metadata[component])
        self.write_metadata('hotel-initialize', self.original)

    def write_metadata(self, component, value):
        path = self.evidence / (component + '.json')
        path.write_bytes(value if isinstance(value, bytes) else fixtures.canonical(value))
        path.chmod(0o644)

    def write_manifest(self, manifest):
        (self.prepared / 'context-manifest.json').write_bytes(fixtures.canonical(manifest))

    def rehash_manifest(self, **changes):
        manifest = json.loads((self.prepared / 'context-manifest.json').read_bytes())
        manifest.update(changes)
        manifest['contexts']['app']['context_sha256'] = tree_sha256(self.prepared / 'app')
        self.write_manifest(manifest)
        return manifest

    def validate(self):
        return validation.validate_preparation(self.prepared, self.upstream)

    def verify(self, name='verified'):
        return validation.verify(self.prepared, self.upstream, self.evidence, self.root / name)

    def assert_closed(self, value):
        self.assertIs(value['candidate_only'], True)
        for name in validation.preparation.FALSE_FLAGS:
            self.assertIs(value[name], False, name)

    def test_independent_reconstruction_matches_complete_source_and_preserves_inputs(self):
        before = tree_sha256(self.prepared), tree_sha256(self.upstream)
        result = self.validate()
        self.assertEqual(result['manifest'], self.manifest)
        self.assertEqual(result['manifest_sha256'], sha256(self.prepared / 'context-manifest.json'))
        self.assertEqual(result['prepared_tree_sha256'], before[0])
        self.assertEqual(result['context_sha256'], tree_sha256(self.prepared / 'app'))
        self.assertEqual(result['recipe_sha256'], sha256(self.prepared / 'app/Dockerfile.candidate'))
        self.assertEqual(result['config_sha256'], self.manifest['config_sha256'])
        self.assertEqual(result['source_fingerprint'], self.original['source_fingerprint'])
        self.assertEqual(result['recipe_revision'], validation.preparation.RECIPE_REVISION)
        self.assertEqual(before, (tree_sha256(self.prepared), tree_sha256(self.upstream)))
        result['manifest']['startup_policy']['runtime_arguments_allowed'].append('not-authority')
        self.assertEqual(json.loads((self.prepared / 'context-manifest.json').read_bytes()), self.manifest)

    def test_self_consistently_rehashed_app_source_cannot_replace_independent_reconstruction(self):
        relative = 'cmd/frontend/main.go'
        path = self.prepared / 'app' / relative
        path.write_bytes(path.read_bytes() + b'// unreviewed behavior\n')
        changed = self.rehash_manifest()
        changed['source_patches'][relative]['patched_sha256'] = sha256(path)
        self.write_manifest(changed)
        with validation.preparation.guarded._GuardedFiles(self.prepared) as local:
            self.assertEqual(validation._validate_preparation(local), changed)
        with self.assertRaisesRegex(PreparationError, 'independently regenerated'):
            self.validate()

    def test_rehashed_mode_license_or_retained_base_receipt_drift_still_fails_reconstruction(self):
        cases = ('mode', 'license', 'guarded-base-preparation.json', 'legacy-base-preparation.json')
        original_manifest = (self.prepared / 'context-manifest.json').read_bytes()
        for kind in cases:
            with self.subTest(kind=kind):
                if kind == 'mode':
                    path = self.prepared / 'app/cmd/frontend/main.go'
                    original = path.read_bytes()
                    path.chmod(0o600)
                    self.rehash_manifest()
                elif kind == 'license':
                    path = self.prepared / 'app/LICENSE.deathstarbench'
                    original = None
                    path.write_text('unreviewed license inventory\n')
                    self.rehash_manifest()
                else:
                    path = self.prepared / kind
                    original = path.read_bytes()
                    path.write_bytes(original + b'\n')
                    field = ('guarded_base_preparation_sha256' if kind.startswith('guarded')
                             else 'legacy_base_preparation_sha256')
                    self.rehash_manifest(**{field: sha256(path)})
                with validation.preparation.guarded._GuardedFiles(self.prepared) as local:
                    validation._validate_preparation(local)
                with self.assertRaisesRegex(PreparationError, 'independently regenerated'):
                    self.validate()
                if original is None:
                    path.unlink()
                else:
                    path.write_bytes(original)
                    path.chmod(0o644)
                (self.prepared / 'context-manifest.json').write_bytes(original_manifest)

    def test_source_or_license_or_supplied_evidence_changes_during_reconstruction_are_detected(self):
        original_prepare = validation.preparation.prepare_contexts
        for target in (self.upstream / 'hotelReservation/sentinel', self.upstream / 'LICENSE',
                       self.prepared / 'app/cmd/frontend/main.go'):
            original = target.read_bytes()
            def drift(upstream, output):
                result = original_prepare(upstream, output)
                target.write_bytes(original + b'// changed during reconstruction\n')
                return result
            with self.subTest(target=target), patch.object(validation.preparation, 'prepare_contexts', side_effect=drift):
                with self.assertRaises(PreparationError):
                    self.validate()
                self.assertIn(b'changed during reconstruction', target.read_bytes())
            target.write_bytes(original)

    def test_invalid_pinned_source_fails_before_any_regeneration(self):
        with patch.object(validation.source_boundary, '_pinned_upstream', side_effect=PreparationError('wrong source')), \
                patch.object(validation.preparation, 'prepare_contexts') as regenerate:
            with self.assertRaisesRegex(PreparationError, 'wrong source'):
                self.validate()
            regenerate.assert_not_called()

    def test_source_revalidation_failure_after_regeneration_is_not_success(self):
        with patch.object(validation.source_boundary, '_pinned_upstream',
                          side_effect=[self.upstream, PreparationError('source identity drift')]):
            with self.assertRaisesRegex(PreparationError, 'source identity drift'):
                self.validate()

    def test_overlapping_source_and_preparation_directories_are_refused_before_regeneration(self):
        for supplied, upstream in ((self.upstream, self.upstream),
                                   (self.upstream / 'hotelReservation', self.upstream),
                                   (self.root, self.upstream)):
            with self.subTest(supplied=supplied, upstream=upstream), \
                    patch.object(validation.preparation, 'prepare_contexts') as regenerate:
                with self.assertRaisesRegex(PreparationError, 'overlaps'):
                    validation.validate_preparation(supplied, upstream)
                regenerate.assert_not_called()

    def test_root_links_leaf_links_and_hardlinks_are_refused_without_touching_foreign_files(self):
        alias = self.root / 'alias'
        alias.symlink_to(self.prepared)
        with self.assertRaises(PreparationError):
            validation.validate_preparation(alias, self.upstream)
        source_alias = self.root / 'source-alias'
        source_alias.symlink_to(self.upstream)
        with self.assertRaises(PreparationError):
            validation.validate_preparation(self.prepared, source_alias)
        target = self.prepared / 'app/cmd/frontend/main.go'
        original = target.read_bytes()
        foreign = self.root / 'foreign.go'
        foreign.write_text('foreign bytes must survive\n')
        for kind in ('symlink', 'hardlink'):
            target.unlink()
            if kind == 'symlink':
                target.symlink_to(foreign)
            else:
                os.link(foreign, target)
            with self.subTest(kind=kind), self.assertRaises(PreparationError):
                self.validate()
            self.assertEqual(foreign.read_text(), 'foreign bytes must survive\n')
            target.unlink()
            target.write_bytes(original)

    def test_local_receipt_rejects_promoted_gates_and_boolean_schema_aliases(self):
        for field, value in [('schema_version', True), ('candidate_only', 1),
                             ('recipe_revision', validation.preparation.guarded.RECIPE_REVISION),
                             *[(name, True) for name in validation.preparation.FALSE_FLAGS],
                             *[(name, 0) for name in validation.preparation.FALSE_FLAGS]]:
            changed = copy.deepcopy(self.manifest)
            changed[field] = value
            self.write_manifest(changed)
            with self.subTest(field=field, value=value), \
                    validation.preparation.guarded._GuardedFiles(self.prepared) as files:
                with self.assertRaises(PreparationError):
                    validation._validate_preparation(files)
        self.write_manifest(self.manifest)

    def test_local_receipt_rejects_duplicate_nonfinite_nonutf8_and_nonobject_json(self):
        original = (self.prepared / 'context-manifest.json').read_bytes()
        malformed = (b'', b'[]', b'null', b'\xff', original + b'{}',
                     original.replace(b'"schema_version": 1', b'"schema_version": 1, "schema_version": 1'),
                     original.replace(b'"schema_version": 1', b'"schema_version": NaN'))
        for raw in malformed:
            (self.prepared / 'context-manifest.json').write_bytes(raw)
            with self.subTest(raw=raw[:40]), validation.preparation.guarded._GuardedFiles(self.prepared) as files:
                with self.assertRaises(PreparationError):
                    validation._validate_preparation(files)
        (self.prepared / 'context-manifest.json').write_bytes(original)

    def test_two_preparation_rounds_are_identical_and_summary_has_no_execution_authority(self):
        destination = self.root / 'twice'
        before = tree_sha256(self.upstream), tree_sha256(self.fixture.assets)
        result = validation.prepare(self.upstream, destination)
        self.assert_closed(result)
        self.assertEqual(result['independent_preparation_rounds'], 2)
        self.assertIs(result['source_preparation_passed'], True)
        self.assertEqual(tree_sha256(destination / 'first'), tree_sha256(destination / 'second'))
        self.assertEqual(result['artifact_receipts']['manifest_sha256'],
                         sha256(destination / 'first/context-manifest.json'))
        self.assertEqual(json.loads((destination / 'preparation-validation.json').read_bytes()), result)
        self.assertEqual(before, (tree_sha256(self.upstream), tree_sha256(self.fixture.assets)))

    def test_nondeterministic_round_failure_cleans_only_new_output(self):
        original_prepare = validation.preparation.prepare_contexts
        def vary(upstream, output):
            manifest = original_prepare(upstream, output)
            if output.name == 'second':
                manifest['unreviewed-round-drift'] = True
                (output / 'context-manifest.json').write_bytes(fixtures.canonical(manifest))
            return manifest
        destination = self.root / 'nondeterministic'
        with patch.object(validation.preparation, 'prepare_contexts', side_effect=vary):
            with self.assertRaisesRegex(PreparationError, 'not deterministic'):
                validation.prepare(self.upstream, destination)
        self.assertFalse(destination.exists())
        self.assertTrue(self.prepared.is_dir())
        self.assertEqual((self.upstream / 'LICENSE').read_text(), 'synthetic pinned license\n')

    def test_all_eleven_metadata_files_match_but_cannot_assert_execution_or_startup(self):
        for flag in ('initializer_authorized', 'runtime_authorized', 'qualification_authority',
                     'database_execution_verified', 'anonymous_pull_qualified'):
            self.assertIn(flag, validation._ATTESTATION_FALSE)
            self.assertTrue(all(value[flag] is False for value in self.metadata.values()), flag)
        before = tree_sha256(self.prepared), tree_sha256(self.upstream), tree_sha256(self.evidence)
        result = self.verify()
        self.assert_closed(result)
        self.assertEqual(result['startup_attestation_count'], 10)
        self.assertEqual(result['baseline_initializer_attestation_count'], 1)
        self.assertIs(result['binary_metadata_consistent'], True)
        self.assertIs(result['execution_proven_by_json_alone'], False)
        self.assertEqual(result['source_fingerprint'], self.original['source_fingerprint'])
        self.assertEqual(result['dataset_fingerprint'], self.original['dataset_fingerprint'])
        self.assertEqual(result['binary_attestation_sha256'], {
            path.name: sha256(path) for path in self.evidence.iterdir()})
        self.assertEqual(before, (tree_sha256(self.prepared), tree_sha256(self.upstream), tree_sha256(self.evidence)))
        self.assertEqual(json.loads((self.root / 'verified/native-metadata-validation.json').read_bytes()), result)

    def test_metadata_rejects_missing_extra_duplicate_nonfinite_and_boolean_aliases(self):
        component = validation.COMMANDS[0]
        good = copy.deepcopy(self.metadata[component])
        mutations = [
            lambda value: value.pop('component'), lambda value: value.update(unreviewed=True),
            lambda value: value.update(schema_version=True), lambda value: value.update(candidate_only=1),
            lambda value: value.update(database_executed=0), lambda value: value.update(application_ready=True),
            lambda value: value.update(component='frontend' if component != 'frontend' else 'user'),
            lambda value: value.update(config_sha256='sha256:' + 'd' * 64),
            lambda value: value.update(source_fingerprint='sha256:' + 'e' * 64),
            lambda value: value.update(dataset_fingerprint='sha256:' + 'f' * 64),
            lambda value: value.update(recipe_revision=validation.preparation.guarded.RECIPE_REVISION),
        ]
        for number, mutate in enumerate(mutations):
            changed = copy.deepcopy(good)
            mutate(changed)
            self.write_metadata(component, changed)
            with self.subTest(case=number), self.assertRaises(PreparationError):
                self.verify()
            self.assertFalse((self.root / 'verified').exists())
        original = fixtures.canonical(good)
        malformed = (b'', b'[]', b'null', b'\xff', original + b'{}',
                     original.replace(b'"schema_version": 1', b'"schema_version": 1, "schema_version": 1'),
                     original.replace(b'"schema_version": 1', b'"schema_version": NaN'))
        for raw in malformed:
            self.write_metadata(component, raw)
            with self.subTest(raw=raw[:40]), self.assertRaises(PreparationError):
                self.verify()
        self.write_metadata(component, good)

    def test_every_startup_attestation_execution_flag_must_be_exact_false(self):
        component = validation.COMMANDS[0]
        for flag in validation._ATTESTATION_FALSE:
            changed = copy.deepcopy(self.metadata[component])
            changed[flag] = True
            self.write_metadata(component, changed)
            with self.subTest(flag=flag), self.assertRaises(PreparationError):
                self.verify()
        self.write_metadata(component, self.metadata[component])

    def test_old_initializer_metadata_must_retain_unchanged_recipe_source_and_dataset(self):
        for field, value in (('recipe_revision', validation.preparation.RECIPE_REVISION),
                             ('source_fingerprint', 'sha256:' + 'e' * 64),
                             ('document_count', 874), ('application_start_allowed', True)):
            changed = copy.deepcopy(self.original)
            changed[field] = value
            if field == 'source_fingerprint':
                changed.pop('dataset_fingerprint')
                changed['dataset_fingerprint'] = fingerprint(changed)
            self.write_metadata('hotel-initialize', changed)
            with self.subTest(field=field), self.assertRaises((PreparationError, HotelInitializationError)):
                self.verify()
        self.write_metadata('hotel-initialize', self.original)

    def test_exact_metadata_file_inventory_forbids_missing_extra_or_subdirectory_inputs(self):
        selected = self.evidence / (validation.COMMANDS[0] + '.json')
        original = selected.read_bytes()
        selected.unlink()
        with self.assertRaisesRegex(PreparationError, 'exactly ten'):
            self.verify()
        selected.write_bytes(original)
        unexpected = self.evidence / 'unexpected.json'
        unexpected.write_text('{}\n')
        with self.assertRaisesRegex(PreparationError, 'exactly ten'):
            self.verify()
        unexpected.unlink()
        selected.unlink()
        selected.mkdir()
        with self.assertRaises(PreparationError):
            self.verify()

    def test_metadata_symlink_hardlink_and_oversized_files_are_refused(self):
        selected = self.evidence / (validation.COMMANDS[0] + '.json')
        original = selected.read_bytes()
        foreign = self.root / 'foreign-metadata.json'
        foreign.write_bytes(original)
        for kind in ('symlink', 'hardlink', 'oversized'):
            selected.unlink()
            if kind == 'symlink':
                selected.symlink_to(foreign)
            elif kind == 'hardlink':
                os.link(foreign, selected)
            else:
                selected.write_bytes(b' ' * (validation.MAX_METADATA_BYTES + 1))
            with self.subTest(kind=kind), self.assertRaises(PreparationError):
                self.verify()
            self.assertEqual(foreign.read_bytes(), original)

    def test_existing_output_and_overlapping_input_destinations_are_preserved(self):
        existing = self.root / 'existing'
        existing.mkdir()
        (existing / 'sentinel').write_text('keep existing output\n')
        before = tree_sha256(self.prepared), tree_sha256(self.upstream), tree_sha256(self.evidence)
        for output in (existing, self.prepared / 'overlap', self.evidence / 'overlap', self.upstream / 'overlap'):
            with self.subTest(output=output), self.assertRaises(PreparationError):
                validation.verify(self.prepared, self.upstream, self.evidence, output)
        self.assertEqual((existing / 'sentinel').read_text(), 'keep existing output\n')
        self.assertEqual(before, (tree_sha256(self.prepared), tree_sha256(self.upstream), tree_sha256(self.evidence)))

    def test_validator_uses_no_network_or_external_process_and_old_recipes_stay_separate(self):
        with patch('socket.create_connection', side_effect=AssertionError('network forbidden')), \
                patch('subprocess.run', side_effect=AssertionError('process forbidden')):
            result = self.verify()
        self.assert_closed(result)
        self.assertEqual(validation.preparation.guarded.RECIPE_REVISION,
                         'hotel-reservation-guarded-init-native-go-scratch-candidate-v1')
        self.assertNotIn('COPY startup/', validation.preparation.guarded._dockerfile())


if __name__ == '__main__':
    unittest.main()
