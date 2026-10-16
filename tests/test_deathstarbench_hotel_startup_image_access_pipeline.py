"""Startup full-byte qualification sequencing with external edges mocked."""

from contextlib import contextmanager, ExitStack, redirect_stderr, redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import qualify_deathstarbench_hotel_startup_image_access as pipeline
from scripts.deathstarbench_artifact_common import PreparationError


class StartupHotelImageAccessPipelineTests(unittest.TestCase):
    def setUp(self):
        environment = mock.patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def fixture(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        base = Path(temporary.name).resolve()
        paths = {name: base / name for name in ('upstream', 'baseline', 'startup', 'sources')}
        for path in paths.values():
            path.mkdir()
        paths.update(root=base / 'prepared', output=base / 'access',
                     rock=paths['sources'] / 'luasocket-3.1.0-1.src.rock')
        paths['rock'].write_bytes(b'unit-only-pinned-rock')
        for name in ('baseline', 'startup'):
            paths[name].joinpath('candidate-image-lock.json').write_bytes(('unit-only-' + name + '-lock').encode())
            paths[name].joinpath('publication.json').write_bytes(('unit-only-' + name + '-publication').encode())
        return paths

    @contextmanager
    def boundaries(self, paths):
        events = []
        baseline, adopted, access = object(), object(), mock.Mock()
        receipt = {'candidate_only': True, 'anonymous_all_bound_image_bytes_verified': True,
                   'runtime_qualified': False, 'application_start_allowed': False, 'public_release': False,
                   'scope': 'synthetic-unit-only-not-live-authority'}
        access.metadata.side_effect = lambda: dict(receipt)

        def prepare(upstream, rock, root):
            events.append('prepare-complete-source')
            root.mkdir()
            (root / 'startup').mkdir()

        def validate(*args):
            events.append('independent-source-reconstruction')
            return {'independent-source': 'same-exact-tree-before-and-after'}

        def old(*args):
            events.append('fresh-original-adoption')
            return baseline

        def new(*args, **kwargs):
            events.append('fresh-startup-adoption')
            self.assertIs(kwargs['baseline'], baseline)
            return adopted

        def qualify(value):
            events.append('all-seven-platform-bytes')
            self.assertIs(value, adopted)
            return access

        def require(value, artifacts):
            events.append('process-local-capability-check')
            self.assertIs(value, access)
            self.assertIs(artifacts, adopted)

        patches = {
            'rock_hash': mock.patch.object(pipeline, 'LUASOCKET_SOURCE_SHA256', hashlib.sha256(paths['rock'].read_bytes()).hexdigest()),
            'lock_hash': mock.patch.object(pipeline, 'STARTUP_LOCK_SHA256', hashlib.sha256(paths['startup'].joinpath('candidate-image-lock.json').read_bytes()).hexdigest()),
            'publication_hash': mock.patch.object(pipeline, 'STARTUP_PUBLICATION_SHA256', hashlib.sha256(paths['startup'].joinpath('publication.json').read_bytes()).hexdigest()),
            'prepare': mock.patch.object(pipeline.publication, 'prepare', side_effect=prepare),
            'validate': mock.patch.object(pipeline, 'validate_startup_preparation', side_effect=validate),
            'baseline': mock.patch.object(pipeline.publication, 'baseline', side_effect=old),
            'adopt': mock.patch.object(pipeline, 'adopt_startup_hotel_artifacts', side_effect=new),
            'qualify': mock.patch('app.deathstarbench_hotel_startup_image_access.qualify_startup_hotel_image_access', side_effect=qualify),
            'require': mock.patch('app.deathstarbench_hotel_startup_image_access.require_startup_hotel_image_access', side_effect=require),
        }
        with ExitStack() as stack:
            observed = {name: stack.enter_context(patch) for name, patch in patches.items()}
            yield {**observed, 'events': events, 'receipt': receipt, 'access': access}

    def run_qualification(self, paths):
        return pipeline.qualify(paths['upstream'], paths['rock'], paths['root'], paths['baseline'], paths['startup'])

    def arguments(self, paths):
        return [item for key, path in (('upstream', paths['upstream']), ('luasocket-rock', paths['rock']),
                                      ('root', paths['root']), ('baseline', paths['baseline']),
                                      ('publication', paths['startup']), ('output', paths['output']))
                for item in ('--' + key, str(path))]

    def test_full_source_then_fresh_both_adoptions_then_all_bytes_then_source_recheck(self):
        paths = self.fixture()
        with self.boundaries(paths) as observed:
            receipt = self.run_qualification(paths)
        self.assertEqual(observed['events'], ['prepare-complete-source', 'independent-source-reconstruction',
            'fresh-original-adoption', 'fresh-startup-adoption', 'all-seven-platform-bytes',
            'process-local-capability-check', 'independent-source-reconstruction'])
        self.assertEqual(receipt, observed['receipt'])
        self.assertEqual(set(receipt), set(observed['receipt']))
        observed['validate'].assert_has_calls([mock.call(paths['root'] / 'startup', paths['upstream'])] * 2)
        observed['adopt'].assert_called_once_with(
            paths['startup'] / 'candidate-image-lock.json', paths['startup'] / 'publication.json',
            preparation=paths['root'] / 'startup', upstream=paths['upstream'], baseline=mock.ANY,
            inspector=pipeline.publication.inspect_manifest, blob_inspector=pipeline.publication.inspect_blob)
        for forbidden in ('run_id', 'cluster', 'namespace_uid', 'pod_uid', 'runtime_plan', 'snapshot'):
            self.assertNotIn(forbidden, receipt)

    def test_source_failure_precedes_every_registry_adoption_and_body_request(self):
        paths = self.fixture()
        with self.boundaries(paths) as observed:
            observed['validate'].side_effect = PreparationError('independently reconstructed source mismatch')
            with self.assertRaisesRegex(PreparationError, 'source mismatch'):
                self.run_qualification(paths)
            for name in ('baseline', 'adopt', 'qualify', 'require'):
                observed[name].assert_not_called()

    def test_byte_receipt_does_not_substitute_for_final_independent_source_identity(self):
        for response in (PreparationError('source changed during slow registry reads'), {'independent-source': 'different'}):
            paths = self.fixture()
            with self.boundaries(paths) as observed:
                observed['validate'].side_effect = [{'independent-source': 'original'}, response]
                with self.subTest(response=type(response)), self.assertRaises(PreparationError):
                    self.run_qualification(paths)
                observed['qualify'].assert_called_once()
                observed['require'].assert_called_once()
                observed['access'].metadata.assert_not_called()

    def test_reviewed_new_artifact_hashes_cannot_be_self_rehashed_or_substituted(self):
        for filename in ('candidate-image-lock.json', 'publication.json'):
            paths = self.fixture()
            with self.boundaries(paths) as observed:
                paths['startup'].joinpath(filename).write_bytes(b'{"self-consistent":"not-reviewed-publication"}')
                with self.subTest(filename=filename), self.assertRaises(PreparationError):
                    self.run_qualification(paths)
                for name in ('prepare', 'validate', 'baseline', 'adopt', 'qualify'):
                    observed[name].assert_not_called()

    def test_original_baseline_still_checks_its_reviewed_files_before_registry(self):
        paths = self.fixture()
        real_baseline = pipeline.publication.baseline
        with self.boundaries(paths) as observed, mock.patch.object(pipeline.publication, 'adopt_candidate_artifacts') as registry:
            observed['baseline'].side_effect = real_baseline
            with self.assertRaisesRegex(PreparationError, 'baseline file identity differs'):
                self.run_qualification(paths)
            registry.assert_not_called()
            observed['adopt'].assert_not_called()
            observed['qualify'].assert_not_called()

    def test_input_drift_during_adoption_refuses_before_full_bytes(self):
        paths = self.fixture()
        with self.boundaries(paths) as observed:
            original = observed['adopt'].side_effect
            def changed(*args, **kwargs):
                result = original(*args, **kwargs)
                paths['startup'].joinpath('publication.json').write_bytes(b'changed-during-adoption')
                return result
            observed['adopt'].side_effect = changed
            with self.assertRaises(PreparationError):
                self.run_qualification(paths)
            observed['qualify'].assert_not_called()

    def test_input_or_pinned_source_drift_during_all_bytes_never_returns_authority(self):
        for target in ('rock', 'startup', 'baseline'):
            paths = self.fixture()
            with self.boundaries(paths) as observed:
                original = observed['qualify'].side_effect
                def changed(*args):
                    result = original(*args)
                    path = paths['rock'] if target == 'rock' else paths[target] / 'publication.json'
                    path.write_bytes(b'changed-after-original-proof')
                    return result
                observed['qualify'].side_effect = changed
                with self.subTest(target=target), self.assertRaises(PreparationError):
                    self.run_qualification(paths)
                observed['access'].metadata.assert_not_called()

    def test_pinned_source_wrong_bytes_symlink_and_hardlink_fail_without_source_or_registry(self):
        for mode in ('wrong-bytes', 'symlink', 'hardlink'):
            paths = self.fixture()
            with self.boundaries(paths) as observed:
                if mode == 'wrong-bytes':
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

    def test_unsafe_input_links_and_existing_or_overlapping_root_are_preserved(self):
        for mode in ('root-exists', 'root-symlink', 'root-overlap', 'linked-evidence', 'linked-lock', 'hardlinked-lock'):
            paths = self.fixture()
            with self.boundaries(paths) as observed:
                if mode == 'root-exists':
                    paths['root'].mkdir()
                elif mode == 'root-symlink':
                    paths['root'].symlink_to(paths['upstream'], target_is_directory=True)
                elif mode == 'root-overlap':
                    paths['root'] = paths['startup'] / 'nested'
                elif mode == 'linked-evidence':
                    linked = paths['startup'].with_name('linked-startup')
                    linked.symlink_to(paths['startup'], target_is_directory=True)
                    paths['startup'] = linked
                else:
                    path = paths['startup'] / 'candidate-image-lock.json'
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

    def test_ambient_auth_proxy_and_trust_overrides_refuse_before_inputs_or_network(self):
        for name in pipeline._EMPTY_ENVIRONMENT:
            with mock.patch.dict(os.environ, {name: 'NEVER-USE'}, clear=True), \
                    mock.patch.object(pipeline, '_directory') as directory, \
                    self.subTest(name=name), self.assertRaises(PreparationError):
                pipeline.qualify(Path('unused'), Path('unused'), Path('unused'), Path('unused'), Path('unused'))
            directory.assert_not_called()

    def test_environment_rechecked_before_each_registry_stage_and_before_result(self):
        for stage, forbidden_next in (('prepare', 'baseline'), ('baseline', 'adopt'), ('adopt', 'qualify'), ('qualify', None)):
            paths = self.fixture()
            with self.boundaries(paths) as observed:
                original = observed[stage].side_effect
                def introduced(*args, **kwargs):
                    result = original(*args, **kwargs)
                    os.environ['GH_TOKEN'] = 'credential-introduced-after-initial-check'
                    return result
                observed[stage].side_effect = introduced
                with self.subTest(stage=stage), self.assertRaises(PreparationError):
                    self.run_qualification(paths)
                if forbidden_next:
                    observed[forbidden_next].assert_not_called()
                observed['access'].metadata.assert_not_called()
            os.environ.pop('GH_TOKEN', None)

    def test_cli_writes_only_historical_metadata_and_preserves_existing_output(self):
        paths = self.fixture()
        with self.boundaries(paths) as observed, redirect_stdout(io.StringIO()):
            self.assertEqual(pipeline.main(self.arguments(paths)), 0)
        path = paths['output'] / 'image-access.json'
        self.assertEqual(json.loads(path.read_bytes()), observed['receipt'])
        original = path.read_bytes()
        with mock.patch.object(pipeline, 'qualify') as qualify, redirect_stderr(io.StringIO()):
            self.assertEqual(pipeline.main(self.arguments(paths)), 1)
        qualify.assert_not_called()
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual([item.name for item in paths['output'].iterdir()], ['image-access.json'])

    def test_failure_output_is_bounded_sanitized_and_never_runtime_or_source_authority(self):
        paths = self.fixture()
        secret = 'Bearer PRIVATE_TOKEN https://signed.private/?TOKEN=secret'
        stderr = io.StringIO()
        with self.boundaries(paths) as observed, redirect_stderr(stderr):
            observed['validate'].side_effect = ValueError(secret)
            self.assertEqual(pipeline.main(self.arguments(paths)), 1)
        raw = (paths['output'] / 'qualification-failure.json').read_text()
        self.assertLess(len(raw), 4096)
        for forbidden in ('PRIVATE_TOKEN', 'signed.private', 'TOKEN=secret'):
            self.assertNotIn(forbidden, raw + stderr.getvalue())
        failure = json.loads(raw)
        self.assertEqual(failure['candidate_schema'], 'hotel-startup-image-access-qualification-failure-v1')
        self.assertEqual(failure['error_type'], 'ValueError')
        self.assertIs(failure['qualification_completed'], False)
        for flag in pipeline._FAILURE_FALSE_FLAGS:
            self.assertIs(failure[flag], False, flag)
        observed['baseline'].assert_not_called()

    def test_output_never_overlaps_input_or_preparation_and_failure_does_not_overwrite(self):
        for target in ('root', 'upstream', 'baseline', 'startup'):
            paths = self.fixture()
            paths['output'] = paths[target] if target == 'root' else paths[target] / 'new-output'
            with mock.patch.object(pipeline, 'qualify') as qualify, redirect_stderr(io.StringIO()):
                self.assertEqual(pipeline.main(self.arguments(paths)), 1)
            qualify.assert_not_called()
            self.assertFalse(paths['output'].exists())

    def test_reviewed_selector_constants_identify_new_publication_not_old_guarded_runtime(self):
        self.assertEqual(pipeline.BASELINE_RUN, 38414696606)
        self.assertEqual(pipeline.BASELINE_ARTIFACT, 'candidate-lock-hotel_reservation-38414696606-1')
        self.assertEqual(pipeline.STARTUP_RUN, 38909780616)
        self.assertEqual(pipeline.STARTUP_ARTIFACT, 'hotel-startup-lock-38909780616-1')
        self.assertEqual(pipeline.STARTUP_LOCK_SHA256, 'e22515f1c0f5c99ea08aed42e47e9c56476edcc67615555f075e19fc1066f057')
        self.assertEqual(pipeline.STARTUP_PUBLICATION_SHA256, 'd3d1b192c6590123b13f2e4457b38bac8cb2c956baf2cf374efbf12fdc4d2f40')


if __name__ == '__main__':
    unittest.main()
