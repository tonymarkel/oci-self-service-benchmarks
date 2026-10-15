"""Synthetic reconstruction/continuity tests; real Git inputs are checked separately."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import deathstarbench_hotel_initialization_artifact_common as common
from scripts.deathstarbench_artifact_common import PreparationError, tree_sha256


class GuardedHotelSourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='guarded-hotel-source-test-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.upstream = self.root / 'upstream'
        (self.upstream / 'hotelReservation').mkdir(parents=True)
        (self.upstream / 'hotelReservation/source.go').write_text('pinned original source fixture\n')
        (self.upstream / 'LICENSE').write_text('pinned license fixture\n')
        self.prepared = self.root / 'prepared'
        self.make(self.prepared)
        guards = (
            patch.object(common, '_pinned_upstream', side_effect=lambda path: path),
            patch.object(common.preparation, 'prepare_contexts', side_effect=lambda _source, output: self.make(output)),
            patch.object(common.offline, '_validate_preparation', side_effect=lambda files: json.loads(files.read('context-manifest.json'))),
        )
        for guard in guards:
            guard.start()
            self.addCleanup(guard.stop)

    def make(self, root):
        (root / 'app').mkdir(parents=True)
        (root / 'app/config.json').write_text('{"fixture":true}\n')
        (root / 'app/Dockerfile.candidate').write_text('# synthetic recipe fixture, not an executable build\n')
        (root / 'app/source.go').write_text('synthetic generated source fixture\n')
        (root / 'base-preparation.json').write_text('{"synthetic_input":true}\n')
        manifest = {'synthetic': True, 'seed_source': {'source_fingerprint': 'sha256:' + '1' * 64},
                    'contexts': {'app': {'context_sha256': tree_sha256(root / 'app')}}}
        (root / 'context-manifest.json').write_text(json.dumps(manifest, sort_keys=True) + '\n')
        for path in (root, *root.rglob('*')):
            path.chmod(0o755 if path.is_dir() else 0o644)
        return manifest

    def validate(self):
        return common.validate_guarded_preparation(self.prepared, self.upstream)

    def test_exact_complete_reconstruction_is_required_and_inputs_are_unchanged(self):
        before = tree_sha256(self.prepared), tree_sha256(self.upstream)
        result = self.validate()
        self.assertEqual(result['context_sha256'], tree_sha256(self.prepared / 'app'))
        self.assertEqual(result['prepared_tree_sha256'], before[0])
        self.assertEqual(result['recipe_sha256'], hashlib.sha256((self.prepared / 'app/Dockerfile.candidate').read_bytes()).hexdigest())
        self.assertEqual(result['recipe_revision'], common.preparation.RECIPE_REVISION)
        self.assertEqual(before, (tree_sha256(self.prepared), tree_sha256(self.upstream)))

    def test_rehashed_malicious_context_or_extra_file_or_mode_drift_does_not_match_reconstruction(self):
        path = self.prepared / 'app/source.go'
        for kind in ('source', 'extra', 'mode'):
            with self.subTest(kind=kind):
                original = path.read_bytes()
                if kind == 'source':
                    path.write_text('changed but self-consistently rehashed\n')
                elif kind == 'extra':
                    (self.prepared / 'unrecorded').write_text('extra')
                else:
                    path.chmod(0o600)
                document = json.loads((self.prepared / 'context-manifest.json').read_text())
                document['contexts']['app']['context_sha256'] = tree_sha256(self.prepared / 'app')
                (self.prepared / 'context-manifest.json').write_text(json.dumps(document, sort_keys=True) + '\n')
                with self.assertRaisesRegex(PreparationError, 'independently regenerated'):
                    self.validate()
                path.write_bytes(original)
                path.chmod(0o644)
                if kind == 'extra':
                    (self.prepared / 'unrecorded').unlink()

    def test_root_links_leaf_links_and_hardlinks_fail_without_reconstruction(self):
        alias = self.root / 'alias'
        alias.symlink_to(self.prepared)
        with self.assertRaises(PreparationError):
            common.validate_guarded_preparation(alias, self.upstream)
        path = self.prepared / 'app/source.go'
        path.unlink()
        path.symlink_to(self.upstream / 'LICENSE')
        with self.assertRaises(PreparationError):
            self.validate()
        path.unlink()
        os.link(self.upstream / 'LICENSE', path)
        with self.assertRaises(PreparationError):
            self.validate()

    def test_source_license_and_evidence_drift_during_reconstruction_is_detected(self):
        for kind in ('source', 'license', 'evidence'):
            with self.subTest(kind=kind):
                target = {'source': self.upstream / 'hotelReservation/source.go',
                          'license': self.upstream / 'LICENSE',
                          'evidence': self.prepared / 'app/source.go'}[kind]
                original = target.read_bytes()
                def drift(_source, output):
                    expected = self.make(output)
                    target.write_bytes(original + b'changed while reconstructing\n')
                    return expected
                with patch.object(common.preparation, 'prepare_contexts', side_effect=drift):
                    with self.assertRaises(PreparationError):
                        self.validate()
                target.write_bytes(original)

    def test_invalid_pinned_upstream_fails_before_reconstruction(self):
        with patch.object(common, '_pinned_upstream', side_effect=PreparationError('wrong Git bytes')), \
                patch.object(common.preparation, 'prepare_contexts') as regenerate:
            with self.assertRaises(PreparationError):
                self.validate()
            regenerate.assert_not_called()

    def test_overlapping_inputs_are_rejected(self):
        with self.assertRaises(PreparationError):
            common.validate_guarded_preparation(self.upstream / 'hotelReservation', self.upstream)

    def test_reviewed_baseline_hashes_select_inputs_but_do_not_adopt_or_repair_them(self):
        baseline = self.root / 'baseline'
        baseline.mkdir()
        (baseline / 'candidate-image-lock.json').write_text('{"fixture": "lock"}\n')
        (baseline / 'publication.json').write_text('{"fixture": "publication"}\n')
        digest = lambda name: hashlib.sha256((baseline / name).read_bytes()).hexdigest()
        with patch.object(common, 'BASELINE_LOCK_SHA256', digest('candidate-image-lock.json')), \
                patch.object(common, 'BASELINE_PUBLICATION_SHA256', digest('publication.json')):
            self.assertEqual(common.verify_baseline_files(baseline),
                             (baseline / 'candidate-image-lock.json', baseline / 'publication.json'))
            (baseline / 'publication.json').write_text('changed')
            with self.assertRaises(PreparationError):
                common.verify_baseline_files(baseline)
            self.assertEqual((baseline / 'publication.json').read_text(), 'changed')


if __name__ == '__main__':
    unittest.main()
