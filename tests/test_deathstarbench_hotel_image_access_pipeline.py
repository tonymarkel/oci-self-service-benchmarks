"""Read-only qualification orchestration tests; network boundaries are mocked."""

from contextlib import contextmanager, redirect_stderr, redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest import mock

from scripts import qualify_deathstarbench_hotel_image_access as pipeline
from scripts.deathstarbench_artifact_common import PreparationError, UPSTREAM_REVISION
from tests.test_deathstarbench_hotel_initialization_workflow import job, step


ROOT = Path(__file__).resolve().parents[1]


class GuardedHotelImageAccessPipelineTests(unittest.TestCase):
    def setUp(self):
        self.environment = mock.patch.dict(os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def fixture(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        base = Path(temporary.name).resolve()
        paths = {name: base / name for name in ('upstream', 'baseline', 'guarded', 'sources')}
        for path in paths.values():
            path.mkdir()
        paths['root'] = base / 'prepared'
        paths['output'] = base / 'access'
        paths['rock'] = paths['sources'] / 'luasocket-3.1.0-1.src.rock'
        paths['rock'].write_bytes(b'unit-only-pinned-rock')
        paths['baseline'].joinpath('candidate-image-lock.json').write_bytes(b'unit-only-old-lock')
        paths['baseline'].joinpath('publication.json').write_bytes(b'unit-only-old-publication')
        paths['guarded'].joinpath('candidate-image-lock.json').write_bytes(b'unit-only-new-lock')
        paths['guarded'].joinpath('publication.json').write_bytes(b'unit-only-new-publication')
        return paths

    @contextmanager
    def boundaries(self, paths):
        events = []
        baseline, adopted, access = object(), object(), mock.Mock()
        receipt = {'candidate_only': True, 'anonymous_all_bound_image_bytes_verified': True,
                   'runtime_qualified': False, 'application_start_allowed': False, 'public_release': False,
                   'scope': 'synthetic-unit-only-never-live-proof'}
        access.metadata.side_effect = lambda: dict(receipt)

        def prepare(upstream, rock, root):
            events.append('prepare-complete-source')
            root.mkdir()
            (root / 'guarded').mkdir()

        def validate(*args):
            events.append('independent-source-reconstruction')

        def old(*args):
            events.append('fresh-original-adoption')
            return baseline

        def new(*args, **kwargs):
            events.append('fresh-guarded-adoption')
            self.assertIs(kwargs['baseline'], baseline)
            return adopted

        def qualify(value):
            events.append('all-seven-platform-bytes')
            self.assertIs(value, adopted)
            return access

        def require(value, value_artifacts):
            events.append('process-local-capability-check')
            self.assertIs(value, access)
            self.assertIs(value_artifacts, adopted)

        patches = [
            mock.patch.object(pipeline, 'LUASOCKET_SOURCE_SHA256', hashlib.sha256(paths['rock'].read_bytes()).hexdigest()),
            mock.patch.object(pipeline, 'GUARDED_LOCK_SHA256', hashlib.sha256(paths['guarded'].joinpath('candidate-image-lock.json').read_bytes()).hexdigest()),
            mock.patch.object(pipeline, 'GUARDED_PUBLICATION_SHA256', hashlib.sha256(paths['guarded'].joinpath('publication.json').read_bytes()).hexdigest()),
            mock.patch.object(pipeline.publication, 'prepare', side_effect=prepare),
            mock.patch.object(pipeline, 'validate_guarded_preparation', side_effect=validate),
            mock.patch.object(pipeline.publication, 'baseline', side_effect=old),
            mock.patch.object(pipeline, 'adopt_guarded_hotel_artifacts', side_effect=new),
            mock.patch('app.deathstarbench_hotel_image_access.qualify_guarded_hotel_image_access', side_effect=qualify),
            mock.patch('app.deathstarbench_hotel_image_access.require_guarded_hotel_image_access', side_effect=require),
        ]
        started = [item.start() for item in patches]
        try:
            yield {'events': events, 'receipt': receipt, 'prepare': started[3], 'validate': started[4],
                   'baseline': started[5], 'adopt': started[6], 'qualify': started[7], 'require': started[8]}
        finally:
            for item in reversed(patches):
                item.stop()

    def run_qualification(self, paths):
        return pipeline.qualify(paths['upstream'], paths['rock'], paths['root'],
                                paths['baseline'], paths['guarded'])

    def arguments(self, paths):
        return [item for key, path in (('upstream', paths['upstream']), ('luasocket-rock', paths['rock']),
                                      ('root', paths['root']), ('baseline', paths['baseline']),
                                      ('publication', paths['guarded']), ('output', paths['output']))
                for item in ('--' + key, str(path))]

    def test_complete_source_then_fresh_both_adoptions_then_full_bytes_then_local_capability(self):
        paths = self.fixture()
        with self.boundaries(paths) as observed:
            receipt = self.run_qualification(paths)
        self.assertEqual(observed['events'], ['prepare-complete-source', 'independent-source-reconstruction',
                         'fresh-original-adoption', 'fresh-guarded-adoption', 'all-seven-platform-bytes',
                         'process-local-capability-check'])
        self.assertEqual(receipt, observed['receipt'])
        self.assertEqual(set(receipt), set(observed['receipt']))
        for forbidden in ('run_id', 'cluster', 'namespace_uid', 'pod_uid', 'runtime_plan', 'snapshot'):
            self.assertNotIn(forbidden, receipt)
        observed['adopt'].assert_called_once_with(
            paths['guarded'] / 'candidate-image-lock.json', paths['guarded'] / 'publication.json',
            preparation=paths['root'] / 'guarded', upstream=paths['upstream'],
            baseline=mock.ANY, inspector=pipeline.publication.inspect_manifest,
            blob_inspector=pipeline.publication.inspect_blob,
        )

    def test_source_failure_precedes_all_registry_adoption_and_layer_requests(self):
        paths = self.fixture()
        with self.boundaries(paths) as observed:
            observed['validate'].side_effect = PreparationError('source reconstructed differently')
            with self.assertRaisesRegex(PreparationError, 'source reconstructed'):
                self.run_qualification(paths)
            for name in ('baseline', 'adopt', 'qualify', 'require'):
                observed[name].assert_not_called()

    def test_reviewed_guarded_file_rehash_does_not_replace_exact_review_selector(self):
        for filename in ('candidate-image-lock.json', 'publication.json'):
            paths = self.fixture()
            with self.boundaries(paths) as observed:
                paths['guarded'].joinpath(filename).write_bytes(b'{"rehashed":"self-consistent-but-not-reviewed"}')
                with self.assertRaisesRegex(PreparationError, 'file identity differs'):
                    self.run_qualification(paths)
                for name in ('prepare', 'baseline', 'adopt', 'qualify'):
                    observed[name].assert_not_called()

    def test_original_baseline_still_checks_exact_historical_files_before_registry(self):
        paths = self.fixture()
        real_baseline = pipeline.publication.baseline
        with self.boundaries(paths) as observed, \
                mock.patch.object(pipeline.publication, 'adopt_candidate_artifacts') as registry_adopt:
            observed['baseline'].side_effect = real_baseline
            with self.assertRaisesRegex(PreparationError, 'baseline file identity differs'):
                self.run_qualification(paths)
            registry_adopt.assert_not_called()
            observed['adopt'].assert_not_called()
            observed['qualify'].assert_not_called()

    def test_evidence_change_during_adoption_is_rejected_before_layer_requests(self):
        paths = self.fixture()
        with self.boundaries(paths) as observed:
            original = observed['adopt'].side_effect

            def changed(*args, **kwargs):
                value = original(*args, **kwargs)
                paths['guarded'].joinpath('publication.json').write_bytes(b'changed-after-adoption')
                return value

            observed['adopt'].side_effect = changed
            with self.assertRaises(PreparationError):
                self.run_qualification(paths)
            observed['qualify'].assert_not_called()

    def test_pinned_rock_drift_or_links_fail_before_source_or_registry(self):
        for mode in ('wrong-hash', 'symlink', 'hardlink'):
            paths = self.fixture()
            with self.boundaries(paths) as observed:
                if mode == 'wrong-hash':
                    paths['rock'].write_bytes(b'not-pinned')
                else:
                    original = paths['rock'].with_name('original.rock')
                    paths['rock'].rename(original)
                    if mode == 'symlink':
                        paths['rock'].symlink_to(original)
                    else:
                        os.link(original, paths['rock'])
                with self.subTest(mode=mode), self.assertRaises(PreparationError):
                    self.run_qualification(paths)
                observed['prepare'].assert_not_called()
                observed['baseline'].assert_not_called()

    def test_unsafe_or_overlapping_inputs_are_preserved_without_registry(self):
        for mode in ('root-exists', 'root-symlink', 'root-overlap', 'linked-evidence', 'linked-lock', 'hardlinked-lock'):
            paths = self.fixture()
            with self.boundaries(paths) as observed:
                if mode == 'root-exists':
                    paths['root'].mkdir()
                elif mode == 'root-symlink':
                    paths['root'].symlink_to(paths['upstream'], target_is_directory=True)
                elif mode == 'root-overlap':
                    paths['root'] = paths['guarded'] / 'nested'
                elif mode == 'linked-evidence':
                    link = paths['guarded'].with_name('evidence-link')
                    link.symlink_to(paths['guarded'], target_is_directory=True)
                    paths['guarded'] = link
                else:
                    path = paths['guarded'] / 'candidate-image-lock.json'
                    original = path.with_name('original-lock.json')
                    path.rename(original)
                    if mode == 'linked-lock':
                        path.symlink_to(original)
                    else:
                        os.link(original, path)
                with self.subTest(mode=mode), self.assertRaises(PreparationError):
                    self.run_qualification(paths)
                observed['prepare'].assert_not_called()
                observed['baseline'].assert_not_called()

    def test_credentials_proxies_and_trust_overrides_refuse_before_any_input_or_network(self):
        for name in pipeline._EMPTY_ENVIRONMENT:
            with mock.patch.dict(os.environ, {name: 'secret-or-ambient-setting'}, clear=True), \
                    mock.patch.object(pipeline, '_directory') as directory, \
                    self.subTest(name=name), self.assertRaisesRegex(PreparationError, 'absent credentials'):
                pipeline.qualify(Path('unused'), Path('unused'), Path('unused'), Path('unused'), Path('unused'))
            directory.assert_not_called()

    def test_cli_only_writes_detached_access_metadata_and_does_not_overwrite_existing_output(self):
        paths = self.fixture()
        with self.boundaries(paths) as observed, redirect_stdout(io.StringIO()):
            self.assertEqual(pipeline.main(self.arguments(paths)), 0)
        output = paths['output'] / 'image-access.json'
        self.assertEqual(json.loads(output.read_bytes()), observed['receipt'])
        before = output.read_bytes()
        with mock.patch.object(pipeline, 'qualify') as qualify, redirect_stderr(io.StringIO()):
            self.assertEqual(pipeline.main(self.arguments(paths)), 1)
        qualify.assert_not_called()
        self.assertEqual(output.read_bytes(), before)
        self.assertEqual(sorted(path.name for path in paths['output'].iterdir()), ['image-access.json'])

    def test_pre_adoption_failure_retains_only_bounded_sanitized_error_not_message(self):
        paths = self.fixture()
        secret = 'Bearer secret-token https://signed.example/blob?token=private'
        stderr = io.StringIO()
        with self.boundaries(paths) as observed, redirect_stderr(stderr):
            observed['validate'].side_effect = ValueError(secret)
            self.assertEqual(pipeline.main(self.arguments(paths)), 1)
        path = paths['output'] / 'qualification-failure.json'
        raw = path.read_text()
        self.assertLess(len(raw), 4096)
        self.assertNotIn(secret, raw + stderr.getvalue())
        self.assertNotIn('signed.example', raw + stderr.getvalue())
        value = json.loads(raw)
        self.assertEqual(value['error_type'], 'ValueError')
        for flag in ('qualification_completed', 'initializer_authorized', 'initialization_executed',
                     'database_executed', 'application_start_allowed', 'runtime_qualified',
                     'measurement_qualified', 'cloud_qualified', 'public_release', 'released'):
            self.assertIs(value[flag], False)
        observed['baseline'].assert_not_called()

    def test_output_cannot_overlap_or_replace_sources_or_preparation(self):
        for key in ('root', 'upstream', 'baseline', 'guarded'):
            paths = self.fixture()
            paths['output'] = paths[key] if key == 'root' else paths[key] / 'new-output'
            with mock.patch.object(pipeline, 'qualify') as qualify, redirect_stderr(io.StringIO()):
                self.assertEqual(pipeline.main(self.arguments(paths)), 1)
            qualify.assert_not_called()
            self.assertFalse(paths['output'].exists())

    def test_historical_selector_constants_are_exact_reviewed_inputs_only(self):
        self.assertEqual(pipeline.BASELINE_RUN, 38414696606)
        self.assertEqual(pipeline.BASELINE_ARTIFACT, 'candidate-lock-hotel_reservation-38414696606-1')
        self.assertEqual(pipeline.GUARDED_RUN, 38767705438)
        self.assertEqual(pipeline.GUARDED_ARTIFACT, 'hotel-guarded-lock-38767705438-1')
        self.assertEqual(pipeline.GUARDED_LOCK_SHA256, 'ddb175985a0555adca5d2457198a289f588aa847301c95dd47e1f5ae4796fee6')
        self.assertEqual(pipeline.GUARDED_PUBLICATION_SHA256, '2bfcf18580a4a2092896816ffa4eb2a451d97a69d31b22a263ec4c4548d6701b')


class GuardedHotelImageAccessWorkflowTests(unittest.TestCase):
    def test_explicit_manual_route_native_readonly_permissions_pinned_actions_no_runtime_or_publish(self):
        wrapper = (ROOT / '.github/workflows/deathstarbench-social-images.yml').read_text()
        body = job(wrapper, 'preflight-guarded-hotel')
        self.assertIn("if: github.event.inputs.publication == 'hotel-runtime-preflight'", body)
        self.assertIn('uses: ./.github/workflows/deathstarbench-hotel-runtime-preflight.yml', body)
        self.assertIn('packages: read', body)
        self.assertNotIn('packages: write', body)
        self.assertIn('        default: load-driver', wrapper)
        workflow = (ROOT / '.github/workflows/deathstarbench-hotel-runtime-preflight.yml').read_text()
        trigger = workflow.split('\npermissions:', 1)[0]
        self.assertIn('  workflow_call:', trigger)
        self.assertNotRegex(trigger, r'(?m)^  (?:push|pull_request|workflow_dispatch|schedule):')
        self.assertIn('runs-on: ubuntu-24.04', workflow)
        self.assertIn('timeout-minutes: 60', workflow)
        self.assertIn('packages: read', workflow)
        self.assertNotIn('packages: write', workflow)
        self.assertNotIn('id-token:', workflow)
        self.assertNotIn('secrets: inherit', workflow)
        self.assertNotRegex(workflow, r'(?i)(?:docker/|setup-qemu|buildx|docker\s+(?:run|pull|push|login)|kubectl|terraform|\bk3s\s|\bhelm\s)')
        for action in re.findall(r'(?m)^\s+(?:- )?uses: (\S+)', workflow):
            self.assertRegex(action, r'^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}$')
        self.assertEqual(workflow.count('persist-credentials: false'), 2)
        self.assertIn('ref: ' + UPSTREAM_REVISION, workflow)
        self.assertIn('submodules: recursive', workflow)
        for run, artifact in ((pipeline.BASELINE_RUN, pipeline.BASELINE_ARTIFACT),
                              (pipeline.GUARDED_RUN, pipeline.GUARDED_ARTIFACT)):
            self.assertIn('run-id: ' + str(run), workflow)
            self.assertIn('name: ' + artifact, workflow)
        execution = step(workflow, 'Fresh source reconstruction and anonymous exact indexes manifests configs and every layer')
        self.assertNotIn('github.token', execution)
        self.assertNotIn('secrets.', execution)
        for name in pipeline._EMPTY_ENVIRONMENT:
            self.assertIn(name, execution)
        self.assertIn('scripts/qualify_deathstarbench_hotel_image_access.py', execution)
        self.assertIn('--upstream .build/upstream', execution)
        self.assertIn('--output .build/access', execution)
        self.assertIn('retention-days: 90', workflow)
        self.assertIn('if-no-files-found: error', workflow)
        self.assertIn('if: always()', step(workflow, 'Retain a bounded sanitized failure record if an earlier prerequisite failed'))
        self.assertIn('if: always()', step(workflow, 'Preserve historical byte-access observations (never a runtime capability)'))
        self.assertIn('Hotel and Media remain unreleased', workflow)


if __name__ == '__main__':
    unittest.main()
