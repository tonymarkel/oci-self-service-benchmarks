import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import publish_deathstarbench_hotel_initialization_images as publish
from scripts import create_deathstarbench_hotel_initialization_image_lock as factory
from scripts import smoke_deathstarbench_hotel_initialization_images as smoke_module
from app import deathstarbench_hotel_initialization_artifacts as adoption
from scripts.deathstarbench_artifact_common import PreparationError, sha256, tree_sha256


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = 'ghcr.io/example/deathstarbench-hotel-app'


class GuardedHotelPublicationTests(unittest.TestCase):
    def setUp(self):
        # These are orchestration unit fixtures, not source reconstruction or
        # native execution evidence. Dedicated source/factory tests exercise
        # the real full pinned-input reconstruction boundary.
        patcher = mock.patch.object(publish, 'validate_guarded_preparation')
        self.source_validation = patcher.start()
        self.addCleanup(patcher.stop)

    def _prepared(self, root):
        guarded = root / 'guarded'
        context = guarded / 'app'
        context.mkdir(parents=True)
        (context / 'Dockerfile.candidate').write_text('FROM scratch\n')
        (guarded / 'context-manifest.json').write_text(json.dumps({
            'seed_source': {'generator_revision': 'generator-fixture', 'source_fingerprint': 'sha256:' + '1' * 64},
            'contexts': {'app': {'context_sha256': tree_sha256(context)}},
        }))
        records = []
        for architecture, values in publish.PLATFORMS.items():
            records.append({
                **publish._identity(root), 'candidate_schema': publish.NATIVE_SCHEMA,
                'platform': values[0], 'image': REPOSITORY + '@sha256:' + ('a' if architecture == 'amd64' else 'b') * 64,
                'build_receipt': {'published_index_image': REPOSITORY + '@sha256:' + ('c' if architecture == 'amd64' else 'd') * 64},
                'smoke_receipt': {'dataset_fingerprint': 'sha256:' + '2' * 64},
            })
        return records

    def _baseline(self):
        result = mock.Mock(fingerprint='sha256:' + 'e' * 64)
        result.metadata.return_value = {
            'candidate_lock_sha256': publish.BASELINE_LOCK_SHA256,
            'publication_document_sha256': publish.BASELINE_PUBLICATION_SHA256,
        }
        return result

    def test_native_matrix_and_unique_existing_repository_tags(self):
        native = publish.plan()['native']['include']
        self.assertEqual(native, [
            {'architecture': 'amd64', 'runner': 'ubuntu-24.04'},
            {'architecture': 'arm64', 'runner': 'ubuntu-24.04-arm'},
        ])
        tag = publish.candidate_name('Example', '123', '2')
        self.assertEqual(tag, REPOSITORY + ':guard-init-6ecb097-123-2')
        self.assertNotIn(':dsb-', tag)
        for owner, run_id, attempt in [('bad/owner', '1', '1'), ('example', '0', '1'),
                                       ('example', '$(id)', '1'), ('example', '1', '0')]:
            with self.subTest(owner=owner, run_id=run_id, attempt=attempt), self.assertRaises(PreparationError):
                publish.candidate_name(owner, run_id, attempt)

    def test_preparation_keeps_distinct_old_and_new_contexts_without_driver_build(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            upstream = parent / 'upstream'
            upstream.mkdir()
            rock = parent / 'source.rock'
            rock.write_text('fixture')
            output = parent / 'prepared'
            with mock.patch.object(publish, 'prepare_legacy') as old, \
                    mock.patch.object(publish, 'prepare_context') as driver, \
                    mock.patch.object(publish, 'prepare_guarded') as guarded, \
                    mock.patch.object(publish.subprocess, 'run') as run:
                publish.prepare(upstream, rock, output)
                old.assert_called_once_with(upstream, output.resolve() / 'baseline-workload')
                driver.assert_called_once_with('hotel_reservation', upstream, rock, output.resolve() / 'baseline-driver')
                guarded.assert_called_once_with(upstream, output.resolve() / 'guarded')
                run.assert_not_called()
            sentinel = output / 'keep'
            sentinel.write_text('mine')
            with self.assertRaises(PreparationError):
                publish.prepare(upstream, rock, output)
            self.assertEqual(sentinel.read_text(), 'mine')

    def test_baseline_requires_exact_pinned_files_and_full_fresh_adoption(self):
        binding = self._baseline()
        with mock.patch.object(publish, 'verify_baseline_files', return_value=(Path('lock'), Path('publication'))) as verify, \
                mock.patch.object(publish, 'adopt_candidate_artifacts', return_value=binding) as adopt:
            self.assertIs(publish.baseline(Path('prepared'), Path('baseline')), binding)
            verify.assert_called_once_with(Path('baseline'))
            self.assertEqual(adopt.call_args.args, ('hotel_reservation', Path('lock'), Path('publication')))
            self.assertEqual(adopt.call_args.kwargs['workload_preparation'], Path('prepared/baseline-workload'))
            binding.metadata.return_value['candidate_lock_sha256'] = 'f' * 64
            with self.assertRaises(PreparationError):
                publish.baseline(Path('prepared'), Path('baseline'))

    def test_both_native_source_identities_are_required_and_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid = self._prepared(root)
            original = copy.deepcopy(valid)
            result = publish.validate_records(root, valid)
            self.assertEqual(valid, original)
            self.assertIs(result['linux/arm64'], valid[1])
            wrong = copy.deepcopy(valid)
            wrong[0]['schema_version'] = True
            extra = copy.deepcopy(valid)
            extra[1]['images'] = {}
            for records in (valid[:1], [valid[0], valid[0]], wrong, extra):
                with self.subTest(records=records), self.assertRaises(PreparationError):
                    publish.validate_records(root, records)
            (root / 'guarded/context-manifest.json').write_text('{}')
            with self.assertRaises((PreparationError, KeyError)):
                publish.validate_records(root, valid)

    def test_native_host_mismatch_fails_before_pull_or_smoke(self):
        with mock.patch.object(publish.platform, 'system', return_value='Linux'), \
                mock.patch.object(publish.platform, 'machine', return_value='x86_64'), \
                mock.patch.object(publish.subprocess, 'run') as run, \
                mock.patch.object(smoke_module, 'smoke_guarded_hotel_image') as smoke:
            with self.assertRaises(PreparationError):
                publish.native_record('arm64', Path('unused'), Path('unused'), 'unused')
            run.assert_not_called()
            smoke.assert_not_called()

    def test_native_record_binds_original_index_exact_platform_and_new_recipe(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._prepared(root)
            selected = {'image': REPOSITORY + '@sha256:' + 'a' * 64,
                        'index_image': REPOSITORY + '@sha256:' + 'c' * 64}
            with mock.patch.object(publish.platform, 'system', return_value='Linux'), \
                    mock.patch.object(publish.platform, 'machine', return_value='aarch64'), \
                    mock.patch.object(publish, 'resolve_platform', return_value=selected), \
                    mock.patch.object(publish.subprocess, 'run') as run, \
                    mock.patch.object(smoke_module, 'smoke_guarded_hotel_image', return_value={'proof': 'mock'}) as smoke:
                record = publish.native_record('arm64', root, root.parent / 'upstream', selected['index_image'])
                self.assertEqual(record['image'], selected['image'])
                build = record['build_receipt']
                self.assertEqual(build['published_index_image'], selected['index_image'])
                self.assertEqual(build['recipe_revision'], publish.RECIPE_REVISION)
                self.assertEqual(build['recipe_sha256'], sha256(root / 'guarded/app/Dockerfile.candidate'))
                self.assertFalse(build['qemu_used'])
                self.assertEqual(run.call_args.args[0], ['docker', 'pull', '--platform', 'linux/arm64', selected['image']])
                smoke.assert_called_once_with(selected['image'], architecture='aarch64', preparation=root / 'guarded')
                self.source_validation.assert_called_once_with(root / 'guarded', root.parent / 'upstream')

    def test_changed_guarded_source_fails_before_every_registry_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self._prepared(root)
            upstream = parent / 'upstream'
            evidence = parent / 'baseline'
            config = parent / 'docker-config'
            config.mkdir()
            self.source_validation.side_effect = PreparationError('changed source')
            operations = (
                lambda: publish.native_record('amd64', root, upstream, REPOSITORY + '@sha256:' + 'a' * 64),
                lambda: publish.assemble(root, upstream, records, evidence, 'example', '1', '1', parent / 'output'),
                lambda: publish.anonymous_record('amd64', root, upstream, evidence, parent / 'publication'),
            )
            with mock.patch.dict(os.environ, {'DOCKER_CONFIG': str(config)}, clear=True), \
                    mock.patch.object(publish, '_native', return_value=('linux/amd64', 'x86_64')), \
                    mock.patch.object(publish, 'baseline') as baseline, \
                    mock.patch.object(publish, 'resolve_platform') as resolve, \
                    mock.patch.object(publish, 'inspect_manifest') as manifests, \
                    mock.patch.object(publish, 'inspect_blob') as blobs, \
                    mock.patch.object(publish.subprocess, 'run') as run:
                for operation in operations:
                    with self.assertRaisesRegex(PreparationError, 'changed source'):
                        operation()
                for observer in (baseline, resolve, manifests, blobs, run):
                    observer.assert_not_called()
            self.assertEqual(self.source_validation.call_count, 3)
            self.assertFalse((parent / 'output').exists())

    def test_failed_complete_preflight_never_creates_output_or_mutates_registry(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self._prepared(root)
            output = parent / 'output'
            with mock.patch.object(publish, 'baseline', return_value=self._baseline()), \
                    mock.patch.object(factory, 'preflight_native_guarded_hotel', side_effect=ValueError('bad evidence')) as preflight, \
                    mock.patch.object(publish.subprocess, 'run') as run:
                with self.assertRaises(ValueError):
                    publish.assemble(root, parent / 'upstream', records, parent / 'baseline', 'example', '1', '1', output)
                preflight.assert_called_once()
                run.assert_not_called()
            self.assertFalse(output.exists())

    def test_repository_boundary_rejects_original_index_before_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self._prepared(root)
            records[1]['build_receipt']['published_index_image'] = 'ghcr.io/other/app@sha256:' + 'd' * 64
            with mock.patch.object(publish, 'baseline', return_value=self._baseline()), \
                    mock.patch.object(factory, 'preflight_native_guarded_hotel'), \
                    mock.patch.object(publish.subprocess, 'run') as run:
                with self.assertRaises(PreparationError):
                    publish.assemble(root, parent / 'upstream', records, parent / 'baseline', 'example', '1', '1', parent / 'output')
                run.assert_not_called()

    def test_index_assembly_retains_original_native_receipts_and_partial_intent(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self._prepared(root)
            original = copy.deepcopy(records)
            output = parent / 'output'
            raw = b'assembled-index-fixture'
            digest = 'sha256:' + hashlib.sha256(raw).hexdigest()

            def assemble_command(command, **_kwargs):
                path = Path(command[command.index('--metadata-file') + 1])
                path.write_text(json.dumps({'containerimage.descriptor': {'digest': digest, 'size': len(raw)}}))

            with mock.patch.object(publish, 'baseline', return_value=self._baseline()), \
                    mock.patch.object(factory, 'preflight_native_guarded_hotel') as preflight, \
                    mock.patch.object(factory, 'create_guarded_hotel_lock', return_value={'closed': True}) as create, \
                    mock.patch.object(publish, 'inspect_manifest', return_value=raw), \
                    mock.patch.object(publish.subprocess, 'run', side_effect=assemble_command) as run:
                lock = publish.assemble(root, parent / 'upstream', records, parent / 'baseline', 'example', '1', '1', output)
                self.assertEqual(lock, {'closed': True})
                self.assertEqual(records, original)
                self.assertEqual(run.call_count, 1)
                preflight.assert_called_once()
                evidence = create.call_args.args[2]
                self.assertEqual(evidence['candidate_schema'], publish.PUBLICATION_SCHEMA)
                self.assertEqual(evidence['app']['platforms']['linux/amd64']['build_receipt'], original[0]['build_receipt'])
                self.assertEqual(evidence['app']['index_image'], REPOSITORY + '@' + digest)
                self.assertTrue((output / 'publication-attempt.json').is_file())
                self.assertTrue((output / 'candidate-image-lock.json').is_file())
            partial = parent / 'partial'
            with mock.patch.object(publish, 'baseline', return_value=self._baseline()), \
                    mock.patch.object(factory, 'preflight_native_guarded_hotel'), \
                    mock.patch.object(publish.subprocess, 'run', side_effect=OSError('registry down')):
                with self.assertRaises(OSError):
                    publish.assemble(root, parent / 'upstream', records, parent / 'baseline', 'example', '1', '2', partial)
            self.assertTrue((partial / 'publication-attempt.json').is_file())
            self.assertFalse((partial / 'candidate-image-lock.json').exists())

    def test_anonymous_credentials_or_nonempty_config_fail_before_adoption(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory)
            with mock.patch.object(publish, '_native', return_value=('linux/amd64', 'x86_64')), \
                    mock.patch.object(publish, 'baseline') as baseline:
                for environment in ({}, {'DOCKER_CONFIG': str(config), 'REGISTRY_TOKEN': 'secret'},
                                    {'DOCKER_CONFIG': 'relative'}, {'DOCKER_CONFIG': str(config), 'DOCKER_AUTH_CONFIG': '{}'}):
                    with self.subTest(environment=environment), mock.patch.dict(os.environ, environment, clear=True), self.assertRaises(PreparationError):
                        publish.anonymous_record('amd64', Path('unused'), Path('unused'), Path('unused'), Path('unused'))
                (config / 'config.json').write_text('{}')
                with mock.patch.dict(os.environ, {'DOCKER_CONFIG': str(config)}, clear=True), self.assertRaises(PreparationError):
                    publish.anonymous_record('amd64', Path('unused'), Path('unused'), Path('unused'), Path('unused'))
                baseline.assert_not_called()

    def test_anonymous_proof_is_separate_native_app_scope_not_runtime_release(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self._prepared(root)
            publication = parent / 'publication'
            publication.mkdir()
            (publication / 'publication.json').write_text(json.dumps({'app': {'platforms': {'linux/amd64': records[0]}}}))
            config = parent / 'docker-config'
            config.mkdir()
            adopted = mock.Mock()
            adopted.app = {'platforms': {'linux/amd64': records[0]}}
            adopted.metadata.return_value = {'runtime_qualified': False}
            with mock.patch.dict(os.environ, {'DOCKER_CONFIG': str(config)}, clear=True), \
                    mock.patch.object(publish, '_native', return_value=('linux/amd64', 'x86_64')), \
                    mock.patch.object(publish, 'baseline', return_value=self._baseline()), \
                    mock.patch.object(adoption, 'adopt_guarded_hotel_artifacts', return_value=adopted) as adopt, \
                    mock.patch.object(publish.subprocess, 'run') as run, \
                    mock.patch.object(smoke_module, 'smoke_guarded_hotel_image', return_value=records[0]['smoke_receipt']):
                proof = publish.anonymous_record('amd64', root, parent / 'upstream', parent / 'baseline', publication)
                self.assertTrue(proof['app_native_anonymous_pull_and_no_database_smoke'])
                for key in ('anonymous_pull_qualified', 'support_or_driver_layers_pulled', 'database_executed',
                            'application_start_allowed', 'runtime_qualified', 'measurement_qualified', 'public_release',
                            'all_layers_independently_downloaded', 'proxy_or_host_credential_independence_qualified'):
                    self.assertIs(proof[key], False)
                adopt.assert_called_once()
                self.assertEqual(run.call_args.args[0][-1], records[0]['image'])

    def test_called_workflow_manual_native_pinned_and_anonymous_jobs_read_only(self):
        text = (ROOT / '.github/workflows/deathstarbench-hotel-initialization-images.yml').read_text()
        trigger = text.split('permissions:', 1)[0]
        self.assertIn('workflow_call:', trigger)
        for forbidden in ('workflow_dispatch:', 'pull_request:', 'push:'):
            self.assertNotIn(forbidden, trigger)
        for forbidden in ('setup-qemu', 'id-token:', 'secrets: inherit'):
            self.assertNotIn(forbidden, text)
        self.assertIn('runner: ubuntu-24.04-arm', text)
        self.assertEqual(text.count('uses: docker/build-push-action@'), 1)
        self.assertIn('provenance: mode=max', text)
        self.assertIn('sbom: true', text)
        self.assertIn('run-id: 38414696606', text)
        self.assertLess(text.index('Freshly adopt'), text.index('Publish only the native'))
        self.assertIn('if: always()', text)
        anonymous = text.split('  anonymous-native:', 1)[1]
        self.assertIn('packages: read', anonymous)
        self.assertNotIn('packages: write', anonymous)
        self.assertNotIn('docker/login-action', anonymous)
        self.assertNotIn('REGISTRY_TOKEN:', anonymous)
        self.assertIn('DOCKER_CONFIG=$(mktemp -d)', anonymous)
        self.assertIn('unset REGISTRY_TOKEN REGISTRY_USERNAME DOCKER_AUTH_CONFIG', anonymous)
        self.assertIn('Hotel and Media remain unreleased', text)
        for line in text.splitlines():
            if 'uses:' in line:
                self.assertRegex(line.split('@', 1)[1].split()[0], r'^[0-9a-f]{40}$')


if __name__ == '__main__':
    unittest.main()
