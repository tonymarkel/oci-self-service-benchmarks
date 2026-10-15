"""Synthetic metadata/receipt checks, not execution or dataset-content proof."""

import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

from app.deathstarbench_hotel_initialization import HotelInitializationError, hotel_initialization_policy
from scripts import validate_deathstarbench_hotel_initialization as validation
from scripts.deathstarbench_artifact_common import PreparationError, sha256, tree_sha256
from tests.test_deathstarbench_hotel_initialization import fingerprint, metadata_fixture


class HotelInitializationValidationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='hotel-native-metadata-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.prepared = self.root / 'prepared'
        self.manifest = self.make_preparation(self.prepared)
        self.evidence = self.root / 'evidence'
        self.evidence.mkdir()
        self.dataset = metadata_fixture()
        self.dataset['source_fingerprint'] = validation._source_fingerprint()[1]
        self.dataset['dataset_fingerprint'] = fingerprint({key: value for key, value in self.dataset.items()
                                                         if key != 'dataset_fingerprint'})
        for command in validation.COMMANDS:
            self.write(self.evidence / (command + '.json'), self.dataset)

    def write(self, path, value):
        path.write_text(json.dumps(value, sort_keys=True) + '\n')

    def make_preparation(self, root):
        root.mkdir()
        app = root / 'app'
        app.mkdir()
        generated = {}
        for relative in ('initialization/catalog/catalog.go',
                         *(f'initialization/seeddata/{component}/seed.go' for component in validation.seed.COMPONENTS)):
            path = app / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('// Synthetic receipt-validation fixture, not Go code.\n')
            generated[relative] = sha256(path)
        self.write(root / 'base-preparation.json', {'synthetic': True})
        identity, source_fingerprint = validation._source_fingerprint()
        manifest = {**validation._summary(), 'workload_id': 'hotel_reservation',
                    'candidate_schema': validation.preparation.PREPARATION_SCHEMA,
                    'upstream_revision': validation.UPSTREAM_REVISION,
                    'patch_revision': validation.preparation.PATCH_REVISION,
                    'dataset_revision': validation.preparation.DATASET_REVISION,
                    'initialization_revision': validation.preparation.CONTRACT_REVISION,
                    'current_candidate_factory_accepts_recipe': False,
                    'initialization_policy': hotel_initialization_policy(),
                    'legacy_base_preparation_sha256': sha256(root / 'base-preparation.json'),
                    'contexts': {'app': {'path': 'app', 'dockerfile': 'Dockerfile.candidate',
                                         'context_sha256': tree_sha256(app)}},
                    'seed_source': {'source_fingerprint_input': identity, 'source_fingerprint': source_fingerprint,
                                    'generator_revision': validation.seed.GENERATOR_REVISION,
                                    'qualification_authority': False, 'generated_source_sha256': generated}}
        self.write(root / 'context-manifest.json', manifest)
        return manifest

    def verify(self):
        return validation.verify(self.prepared, self.evidence, self.root / 'summary')

    def test_eleven_consistent_source_bound_metadata_outputs_do_not_grant_runtime_authority(self):
        value = self.verify()
        self.assertEqual(value['binary_attestation_count'], 11)
        self.assertTrue(value['binary_metadata_consistent'])
        self.assertFalse(value['execution_proven_by_json_alone'])
        self.assertEqual(value['document_count'], 873)
        self.assertEqual(value['dataset_fingerprint'], self.dataset['dataset_fingerprint'])
        for key in ('released', 'images_built', 'images_published', 'database_executed',
                    'dataset_qualified', 'application_start_allowed', 'runtime_qualified',
                    'measurement_qualified', 'public_release'):
            self.assertIs(value[key], False)
        self.assertEqual(len(value['binary_attestation_sha256']), 11)
        self.assertEqual(json.loads((self.root / 'summary/native-metadata-validation.json').read_text()), value)

    def test_missing_or_extra_binary_metadata_is_refused_without_output(self):
        for name in ('extra.json', 'frontend.json'):
            with self.subTest(name=name):
                if name == 'extra.json':
                    self.write(self.evidence / name, {})
                else:
                    (self.evidence / name).unlink()
                with self.assertRaises(PreparationError):
                    self.verify()
                self.assertFalse((self.root / 'summary').exists())
                if name == 'extra.json':
                    (self.evidence / name).unlink()

    def test_crossed_valid_metadata_from_one_binary_is_refused(self):
        changed = copy.deepcopy(self.dataset)
        changed['collections'][0]['content_sha256'] = 'sha256:' + 'd' * 64
        changed['dataset_fingerprint'] = fingerprint({key: value for key, value in changed.items()
                                                     if key != 'dataset_fingerprint'})
        self.write(self.evidence / 'frontend.json', changed)
        with self.assertRaisesRegex(PreparationError, 'different datasets'):
            self.verify()
        self.assertFalse((self.root / 'summary').exists())

    def test_malformed_duplicate_or_promoted_metadata_is_refused(self):
        path = self.evidence / 'frontend.json'
        for raw in (b'{"candidate_only":true,"candidate_only":true}', b'not JSON',
                    json.dumps({**self.dataset, 'runtime_qualified': True}).encode(),
                    json.dumps({**self.dataset, 'source_fingerprint': 'sha256:' + 'f' * 64}).encode()):
            with self.subTest(raw=raw[:50]):
                path.write_bytes(raw)
                with self.assertRaises(HotelInitializationError):
                    self.verify()
                self.assertFalse((self.root / 'summary').exists())

    def test_links_hardlinks_special_and_oversized_files_are_refused(self):
        path = self.evidence / 'frontend.json'
        original = path.read_bytes()
        path.unlink()
        path.symlink_to(self.evidence / 'search.json')
        with self.assertRaises(PreparationError):
            self.verify()
        path.unlink()
        os.link(self.evidence / 'search.json', path)
        with self.assertRaises(PreparationError):
            self.verify()
        path.unlink()
        path.write_bytes(b'x' * (validation.MAX_ATTESTATION_BYTES + 1))
        with self.assertRaises(PreparationError):
            self.verify()
        path.unlink()
        os.mkfifo(path)
        with self.assertRaises(PreparationError):
            self.verify()
        path.unlink()
        path.write_bytes(original)
        self.assertTrue(self.verify()['binary_metadata_consistent'])

    def test_source_context_manifest_or_source_identity_drift_is_refused(self):
        path = self.prepared / 'context-manifest.json'
        mutations = (
            lambda value: value.update(current_candidate_factory_accepts_recipe=True),
            lambda value: value.update(candidate_schema='old-v1-preparation'),
            lambda value: value['seed_source'].update(source_fingerprint='sha256:' + 'f' * 64),
            lambda value: value['seed_source']['source_fingerprint_input'].update(generator_revision='foreign'),
            lambda value: value['seed_source']['generated_source_sha256'].pop('initialization/catalog/catalog.go'),
            lambda value: value.update(contexts={'app': {'path': '../escape'}}),
        )
        for mutate in mutations:
            value = copy.deepcopy(self.manifest)
            mutate(value)
            self.write(path, value)
            with self.assertRaises(PreparationError):
                self.verify()
        self.write(path, self.manifest)
        (self.prepared / 'app/initialization/catalog/catalog.go').write_text('changed')
        with self.assertRaises(PreparationError):
            self.verify()

    def test_existing_or_overlapping_output_is_preserved(self):
        output = self.root / 'summary'
        output.mkdir()
        (output / 'sentinel').write_text('keep')
        with self.assertRaises(PreparationError):
            self.verify()
        self.assertEqual((output / 'sentinel').read_text(), 'keep')
        with self.assertRaises(PreparationError):
            validation.verify(self.prepared, self.evidence, self.evidence / 'overlap')
        self.assertFalse((self.evidence / 'overlap').exists())

    def test_twice_preparation_detects_determinism_and_removes_only_new_owned_failure(self):
        upstream = self.root / 'upstream'
        upstream.mkdir()
        with patch.object(validation.preparation, 'prepare_contexts', side_effect=lambda _source, target: self.make_preparation(target)):
            value = validation.prepare(upstream, self.root / 'twice')
        self.assertEqual(value['independent_preparation_rounds'], 2)
        self.assertTrue(value['source_preparation_passed'])
        def changed(_source, target):
            value = self.make_preparation(target)
            if target.name == 'second':
                (target / 'extra').write_text('nondeterministic')
            return value
        with patch.object(validation.preparation, 'prepare_contexts', side_effect=changed):
            with self.assertRaisesRegex(PreparationError, 'not deterministic'):
                validation.prepare(upstream, self.root / 'failed')
        self.assertFalse((self.root / 'failed').exists())
        self.assertTrue(upstream.is_dir())

    def test_required_ci_runs_new_source_and_all_binary_checks_without_database_or_publication(self):
        text = (Path(__file__).resolve().parents[1] / '.github/workflows/ci.yml').read_text()
        workflow = yaml.safe_load(text)
        job = workflow['jobs']['candidate-artifacts']
        self.assertIn('candidate-artifacts', workflow['jobs']['required']['needs'])
        runs = [step.get('run', '') for step in job['steps']]
        native = next(run for run in runs if 'mkdir .build/hotel-initialization-attestations' in run)
        self.assertIn('validate_deathstarbench_hotel_initialization.py prepare', '\n'.join(runs))
        self.assertIn('validate_deathstarbench_hotel_initialization.py verify', native)
        for guard in ('--network=none', '--read-only', '--cap-drop=ALL', '--user "$(id -u):$(id -g)"',
                      'GOPROXY=off', 'GOSUMDB=off', 'GOTOOLCHAIN=local',
                      'go test -mod=vendor -vet=off ./initialization/... ./cmd/...',
                      'go build -mod=vendor -trimpath', 'cd /tmp',
                      '--candidate-initialization-attestation', 'test "$code" -eq 2'):
            self.assertIn(guard, native)
        for command in validation.COMMANDS:
            self.assertIn(command, native)
        self.assertNotIn('initialize-empty', native)
        self.assertNotIn('HOTEL_CANDIDATE_RUN_ID', native)
        self.assertNotIn('docker push', native)
        self.assertNotIn('packages: write', text)
        upload = next(step for step in job['steps'] if step.get('uses', '').startswith('actions/upload-artifact@'))
        self.assertIn('.build/hotel-initialization-native/native-metadata-validation.json', upload['with']['path'])


if __name__ == '__main__':
    unittest.main()
