"""Synthetic startup image orchestration checks, never registry/build authority."""

import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from app import deathstarbench_hotel_startup_artifacts as adoption
from app.deathstarbench_candidate_artifacts import _freeze, _thaw
from scripts import create_deathstarbench_hotel_startup_image_lock as factory
from scripts import publish_deathstarbench_hotel_startup_images as publish
from scripts import smoke_deathstarbench_hotel_startup_images as smoke_module
from scripts.deathstarbench_artifact_common import PreparationError, sha256, tree_sha256


REPOSITORY = 'ghcr.io/example/deathstarbench-hotel-app'


class StartupHotelPublicationTests(unittest.TestCase):
    def setUp(self):
        # Complete source reconstruction and raw OCI/smoke validation have
        # separate suites. These mocks isolate mutation/observation ordering.
        patcher = mock.patch.object(publish, 'validate_startup_preparation')
        self.source_validation = patcher.start()
        self.addCleanup(patcher.stop)

    def prepared(self, root):
        target = root / 'startup'
        context = target / 'app'
        context.mkdir(parents=True)
        (context / 'Dockerfile.candidate').write_text('FROM scratch\n')
        (target / 'context-manifest.json').write_text(json.dumps({
            'config_sha256': '7' * 64,
            'seed_source': {'generator_revision': 'fixture-generator',
                            'source_fingerprint': 'sha256:' + '1' * 64},
            'contexts': {'app': {'context_sha256': tree_sha256(context)}},
        }))
        records = []
        for architecture, values in publish.PLATFORMS.items():
            records.append({
                **publish._identity(root), 'candidate_schema': publish.NATIVE_SCHEMA,
                'platform': values[0],
                'image': REPOSITORY + '@sha256:' + ('a' if architecture == 'amd64' else 'b') * 64,
                'build_receipt': {'published_index_image': REPOSITORY + '@sha256:'
                                  + ('c' if architecture == 'amd64' else 'd') * 64},
                'smoke_receipt': {'dataset_fingerprint': 'sha256:' + '2' * 64,
                                 'startup_attestations': {'frontend': {
                                     'component': 'frontend', 'policy_revision': publish.POLICY_REVISION,
                                     'config_sha256': 'sha256:' + '7' * 64,
                                     'database_execution_verified': False}}},
            })
        return records

    def baseline(self):
        binding = mock.Mock(fingerprint='sha256:' + 'e' * 64)
        binding.metadata.return_value = {
            'candidate_lock_sha256': publish.BASELINE_LOCK_SHA256,
            'publication_document_sha256': publish.BASELINE_PUBLICATION_SHA256,
        }
        return binding

    def test_distinct_startup_schemas_native_matrix_and_unique_existing_repository_tags(self):
        self.assertEqual(publish.plan()['native']['include'], [
            {'architecture': 'amd64', 'runner': 'ubuntu-24.04'},
            {'architecture': 'arm64', 'runner': 'ubuntu-24.04-arm'},
        ])
        self.assertEqual(publish.candidate_name('Example', '123', '2'),
                         REPOSITORY + ':guard-startup-6ecb097-123-2')
        for name, suffix in [('PUBLICATION_SCHEMA', 'publication'),
                             ('NATIVE_SCHEMA', 'native-publication'), ('BUILD_SCHEMA', 'native-build')]:
            self.assertEqual(getattr(publish, name), 'hotel-guarded-startup-' + suffix + '-v1')
        for owner, run_id, attempt in [('bad/owner', '1', '1'), ('example', '0', '1'),
                                       ('example', '$(id)', '1'), ('example', '1', '0')]:
            with self.subTest(owner=owner, run_id=run_id, attempt=attempt), self.assertRaises(PreparationError):
                publish.candidate_name(owner, run_id, attempt)

    def test_preparation_has_separate_original_and_startup_contexts_without_build(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            upstream = parent / 'upstream'
            upstream.mkdir()
            rock = parent / 'source.rock'
            rock.write_text('fixture')
            output = parent / 'prepared'
            with mock.patch.object(publish, 'prepare_legacy') as old, \
                    mock.patch.object(publish, 'prepare_context') as driver, \
                    mock.patch.object(publish, 'prepare_startup') as startup, \
                    mock.patch.object(publish.subprocess, 'run') as run:
                publish.prepare(upstream, rock, output)
                old.assert_called_once_with(upstream, output.resolve() / 'baseline-workload')
                driver.assert_called_once_with('hotel_reservation', upstream, rock,
                                               output.resolve() / 'baseline-driver')
                startup.assert_called_once_with(upstream, output.resolve() / 'startup')
                run.assert_not_called()
            sentinel = output / 'keep'
            sentinel.write_text('mine')
            with self.assertRaises(PreparationError):
                publish.prepare(upstream, rock, output)
            self.assertEqual(sentinel.read_text(), 'mine')

    def test_original_baseline_files_are_freshly_adopted_not_old_guarded_authority(self):
        binding = self.baseline()
        with mock.patch.object(publish, 'verify_baseline_files', return_value=(Path('lock'), Path('publication'))) as verify, \
                mock.patch.object(publish, 'adopt_candidate_artifacts', return_value=binding) as adopt:
            self.assertIs(publish.baseline(Path('prepared'), Path('baseline')), binding)
            verify.assert_called_once_with(Path('baseline'))
            self.assertEqual(adopt.call_args.args, ('hotel_reservation', Path('lock'), Path('publication')))
            self.assertEqual(adopt.call_args.kwargs['workload_preparation'], Path('prepared/baseline-workload'))
            self.assertEqual(adopt.call_args.kwargs['driver_preparation'], Path('prepared/baseline-driver'))
            for field in ('candidate_lock_sha256', 'publication_document_sha256'):
                original = binding.metadata.return_value[field]
                binding.metadata.return_value[field] = 'f' * 64
                with self.subTest(field=field), self.assertRaises(PreparationError):
                    publish.baseline(Path('prepared'), Path('baseline'))
                binding.metadata.return_value[field] = original

    def test_records_require_both_native_platforms_and_exact_new_source_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid = self.prepared(root)
            saved = copy.deepcopy(valid)
            result = publish.validate_records(root, valid)
            self.assertIs(result['linux/arm64'], valid[1])
            self.assertEqual(saved, valid)
            mutations = [
                lambda values: values.pop(),
                lambda values: values.__setitem__(1, copy.deepcopy(values[0])),
                lambda values: values[0].update(schema_version=True),
                lambda values: values[0].update(candidate_schema='hotel-guarded-initialization-native-publication-v1'),
                lambda values: values[1].update(recipe_revision='legacy'),
                lambda values: values[1].update(extra='not permitted'),
                lambda values: values[1].update(source_fingerprint='sha256:' + '9' * 64),
            ]
            for mutate in mutations:
                observed = copy.deepcopy(valid)
                mutate(observed)
                with self.subTest(mutation=mutate), self.assertRaises(PreparationError):
                    publish.validate_records(root, observed)

    def test_mismatched_native_host_fails_before_registry_pull_smoke_or_source(self):
        with mock.patch.object(publish.platform, 'system', return_value='Linux'), \
                mock.patch.object(publish.platform, 'machine', return_value='x86_64'), \
                mock.patch.object(publish, 'resolve_platform') as resolve, \
                mock.patch.object(publish.subprocess, 'run') as run, \
                mock.patch.object(smoke_module, 'smoke_startup_hotel_image') as smoke:
            with self.assertRaises(PreparationError):
                publish.native_record('arm64', Path('unused'), Path('unused'), 'unused')
            resolve.assert_not_called()
            run.assert_not_called()
            smoke.assert_not_called()
            self.source_validation.assert_not_called()

    def test_native_receipt_binds_pulled_manifest_original_index_and_startup_recipe(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.prepared(root)
            selected = {'image': REPOSITORY + '@sha256:' + 'a' * 64,
                        'index_image': REPOSITORY + '@sha256:' + 'c' * 64}
            with mock.patch.object(publish, '_native', return_value=('linux/arm64', 'aarch64')), \
                    mock.patch.object(publish, 'resolve_platform', return_value=selected), \
                    mock.patch.object(publish.subprocess, 'run') as run, \
                    mock.patch.object(smoke_module, 'smoke_startup_hotel_image', return_value={'proof': 'mock'}) as smoke:
                record = publish.native_record('arm64', root, root.parent / 'upstream', selected['index_image'])
                self.assertEqual(record['image'], selected['image'])
                self.assertEqual(record['build_receipt']['published_index_image'], selected['index_image'])
                self.assertEqual(record['build_receipt']['recipe_revision'], publish.RECIPE_REVISION)
                self.assertEqual(record['build_receipt']['recipe_sha256'], sha256(root / 'startup/app/Dockerfile.candidate'))
                self.assertIs(record['build_receipt']['qemu_used'], False)
                run.assert_called_once_with(['docker', 'pull', '--platform', 'linux/arm64', selected['image']],
                                            check=True, timeout=600)
                smoke.assert_called_once_with(selected['image'], architecture='aarch64', preparation=root / 'startup')
                self.source_validation.assert_called_once_with(root / 'startup', root.parent / 'upstream')

    def test_changed_source_prevents_every_registry_observation_pull_and_output(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self.prepared(root)
            docker_config = parent / 'docker-config'
            docker_config.mkdir()
            self.source_validation.side_effect = PreparationError('changed source')
            operations = (
                lambda: publish.native_record('amd64', root, parent / 'upstream', REPOSITORY + '@sha256:' + 'a' * 64),
                lambda: publish.assemble(root, parent / 'upstream', records, parent / 'baseline',
                                         'example', '1', '1', parent / 'output'),
                lambda: publish.anonymous_record('amd64', root, parent / 'upstream',
                                                 parent / 'baseline', parent / 'publication'),
            )
            with mock.patch.dict(os.environ, {'DOCKER_CONFIG': str(docker_config)}, clear=True), \
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

    def test_local_context_drift_is_refused_before_resolution_even_after_source_boundary_returns(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.prepared(root)
            (root / 'startup/app/Dockerfile.candidate').write_text('FROM hostile\n')
            with mock.patch.object(publish, '_native', return_value=('linux/amd64', 'x86_64')), \
                    mock.patch.object(publish, 'resolve_platform') as resolve, \
                    mock.patch.object(publish.subprocess, 'run') as run:
                with self.assertRaises(PreparationError):
                    publish.native_record('amd64', root, root.parent / 'upstream', 'unused')
                self.source_validation.assert_called_once()
                resolve.assert_not_called()
                run.assert_not_called()

    def test_full_preflight_and_repository_boundaries_precede_index_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self.prepared(root)
            output = parent / 'output'
            with mock.patch.object(publish, 'baseline', return_value=self.baseline()), \
                    mock.patch.object(factory, 'preflight_native_startup_hotel', side_effect=ValueError('bad evidence')) as preflight, \
                    mock.patch.object(publish.subprocess, 'run') as run:
                with self.assertRaisesRegex(ValueError, 'bad evidence'):
                    publish.assemble(root, parent / 'upstream', records, parent / 'baseline',
                                     'example', '1', '1', output)
                preflight.assert_called_once()
                run.assert_not_called()
            self.assertFalse(output.exists())
            for reference in ('ghcr.io/other/app@sha256:' + 'd' * 64, REPOSITORY + ':mutable'):
                changed = copy.deepcopy(records)
                changed[1]['build_receipt']['published_index_image'] = reference
                with self.subTest(reference=reference), \
                        mock.patch.object(publish, 'baseline', return_value=self.baseline()), \
                        mock.patch.object(factory, 'preflight_native_startup_hotel'), \
                        mock.patch.object(publish.subprocess, 'run') as run:
                    with self.assertRaises(PreparationError):
                        publish.assemble(root, parent / 'upstream', changed, parent / 'baseline',
                                         'example', '1', '1', output)
                    run.assert_not_called()
                self.assertFalse(output.exists())

    def test_publication_preserves_original_receipts_and_partial_mutation_intent(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self.prepared(root)
            original = copy.deepcopy(records)
            output = parent / 'output'
            raw = b'assembled-startup-index'
            digest = 'sha256:' + hashlib.sha256(raw).hexdigest()

            def command(arguments, **_kwargs):
                metadata = Path(arguments[arguments.index('--metadata-file') + 1])
                metadata.write_text(json.dumps({'containerimage.descriptor': {'digest': digest, 'size': len(raw)}}))

            with mock.patch.object(publish, 'baseline', return_value=self.baseline()), \
                    mock.patch.object(factory, 'preflight_native_startup_hotel') as preflight, \
                    mock.patch.object(factory, 'create_startup_hotel_lock', return_value={'closed': True}) as create, \
                    mock.patch.object(publish, 'inspect_manifest', return_value=raw), \
                    mock.patch.object(publish.subprocess, 'run', side_effect=command) as run:
                self.assertEqual(publish.assemble(root, parent / 'upstream', records, parent / 'baseline',
                                                  'example', '1', '1', output), {'closed': True})
                preflight.assert_called_once()
                run.assert_called_once()
                evidence = create.call_args.args[2]
                self.assertEqual(evidence['candidate_schema'], publish.PUBLICATION_SCHEMA)
                self.assertEqual(evidence['app']['index_image'], REPOSITORY + '@' + digest)
                self.assertEqual(evidence['app']['platforms']['linux/amd64']['build_receipt'], original[0]['build_receipt'])
                self.assertEqual(records, original)
                self.assertTrue((output / 'publication-attempt.json').is_file())
                self.assertTrue((output / 'candidate-image-lock.json').is_file())
            partial = parent / 'partial'
            with mock.patch.object(publish, 'baseline', return_value=self.baseline()), \
                    mock.patch.object(factory, 'preflight_native_startup_hotel'), \
                    mock.patch.object(publish.subprocess, 'run', side_effect=OSError('registry down')):
                with self.assertRaises(OSError):
                    publish.assemble(root, parent / 'upstream', records, parent / 'baseline',
                                     'example', '1', '2', partial)
            self.assertTrue((partial / 'publication-attempt.json').is_file())
            self.assertFalse((partial / 'candidate-image-lock.json').exists())

    def test_fresh_nonoverlapping_assembly_output_is_required_before_registry_or_write(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self.prepared(root)
            upstream = parent / 'upstream'
            upstream.mkdir()
            evidence = parent / 'baseline'
            evidence.mkdir()
            existing = parent / 'existing'
            existing.mkdir()
            sentinel = existing / 'keep'
            sentinel.write_text('mine')
            with mock.patch.object(publish, 'baseline') as baseline, \
                    mock.patch.object(publish.subprocess, 'run') as run:
                for output in (existing, root / 'new', upstream / 'new', evidence / 'new', parent):
                    with self.subTest(output=output), self.assertRaises(PreparationError):
                        publish.assemble(root, upstream, records, evidence, 'example', '1', '1', output)
                baseline.assert_not_called()
                run.assert_not_called()
            self.assertEqual(sentinel.read_text(), 'mine')

    def test_assembled_raw_bytes_must_match_exact_mutation_descriptor_before_lock_is_written(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self.prepared(root)
            raw = b'exact-assembled-index-bytes'
            descriptor = {'digest': 'sha256:' + hashlib.sha256(raw).hexdigest(), 'size': len(raw)}
            for number, bad in enumerate(('digest', 'size', 'boolean-size')):
                output = parent / ('output-' + str(number))

                def command(arguments, **_kwargs):
                    value = copy.deepcopy(descriptor)
                    if bad == 'digest':
                        value['digest'] = 'sha256:' + 'f' * 64
                    elif bad == 'size':
                        value['size'] += 1
                    else:
                        value['size'] = True
                    metadata = Path(arguments[arguments.index('--metadata-file') + 1])
                    metadata.write_text(json.dumps({'containerimage.descriptor': value}))

                with self.subTest(bad=bad), \
                        mock.patch.object(publish, 'baseline', return_value=self.baseline()), \
                        mock.patch.object(factory, 'preflight_native_startup_hotel'), \
                        mock.patch.object(factory, 'create_startup_hotel_lock') as create, \
                        mock.patch.object(publish, 'inspect_manifest', return_value=raw), \
                        mock.patch.object(publish.subprocess, 'run', side_effect=command):
                    with self.assertRaises(PreparationError):
                        publish.assemble(root, parent / 'upstream', records, parent / 'baseline',
                                         'example', '1', str(number + 1), output)
                    create.assert_not_called()
                self.assertTrue((output / 'publication-attempt.json').is_file())
                self.assertFalse((output / 'candidate-image-lock.json').exists())

    def test_anonymous_polluted_or_missing_client_config_fails_before_adoption(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory)
            with mock.patch.object(publish, '_native', return_value=('linux/amd64', 'x86_64')), \
                    mock.patch.object(publish, 'baseline') as baseline:
                environments = ({}, {'DOCKER_CONFIG': str(config), 'REGISTRY_TOKEN': 'secret'},
                                {'DOCKER_CONFIG': str(config), 'REGISTRY_USERNAME': 'user'},
                                {'DOCKER_CONFIG': 'relative'}, {'DOCKER_CONFIG': str(config), 'DOCKER_AUTH_CONFIG': '{}'})
                for environment in environments:
                    with self.subTest(environment=environment), mock.patch.dict(os.environ, environment, clear=True), \
                            self.assertRaises(PreparationError):
                        publish.anonymous_record('amd64', Path('unused'), Path('unused'), Path('unused'), Path('unused'))
                (config / 'config.json').write_text('{}')
                with mock.patch.dict(os.environ, {'DOCKER_CONFIG': str(config)}, clear=True), self.assertRaises(PreparationError):
                    publish.anonymous_record('amd64', Path('unused'), Path('unused'), Path('unused'), Path('unused'))
                baseline.assert_not_called()
            self.source_validation.assert_not_called()

    def test_anonymous_selection_uses_detached_adoption_not_mutable_publication_reread(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self.prepared(root)
            publication = parent / 'publication'
            publication.mkdir()
            mutable = publication / 'publication.json'
            mutable.write_text('{}')
            config = parent / 'docker-config'
            config.mkdir()
            selected = copy.deepcopy(records[0])
            adopted = mock.Mock()
            adopted.app = _freeze({'platforms': {'linux/amd64': selected}})
            with self.assertRaises(TypeError):
                adopted.app['platforms']['linux/amd64']['smoke_receipt']['startup_attestations']['frontend']['component'] = 'changed'
            adopted.metadata.return_value = {'runtime_qualified': False}

            def change_after_adoption(*_args, **_kwargs):
                mutable.write_text(json.dumps({'app': {'platforms': {'linux/amd64': {
                    'image': 'ghcr.io/foreign/mutable:latest'}}}}))
                return adopted

            with mock.patch.dict(os.environ, {'DOCKER_CONFIG': str(config)}, clear=True), \
                    mock.patch.object(publish, '_native', return_value=('linux/amd64', 'x86_64')), \
                    mock.patch.object(publish, 'baseline', return_value=self.baseline()), \
                    mock.patch.object(adoption, 'adopt_startup_hotel_artifacts', side_effect=change_after_adoption) as adopt, \
                    mock.patch.object(publish.subprocess, 'run') as run, \
                    mock.patch.object(smoke_module, 'smoke_startup_hotel_image', return_value=selected['smoke_receipt']):
                proof = publish.anonymous_record('amd64', root, parent / 'upstream', parent / 'baseline', publication)
                adopt.assert_called_once()
                self.assertEqual(run.call_args.args[0][-1], selected['image'])
                self.assertEqual(proof['image'], selected['image'])
                self.assertTrue(proof['app_native_anonymous_pull_and_no_database_smoke'])
                self.assertTrue(proof['empty_docker_config'])
                self.assertIs(proof['registry_credentials_supplied'], False)
                for flag in ('anonymous_pull_qualified', 'support_or_driver_layers_pulled', 'database_executed',
                             'application_start_allowed', 'runtime_qualified', 'measurement_qualified', 'public_release',
                             'all_layers_independently_downloaded', 'proxy_or_host_credential_independence_qualified'):
                    self.assertIs(proof[flag], False)
                self.assertEqual(proof['scope'], 'fresh-empty-docker-client-config-exact-native-app-pull-and-no-database-smoke-only')

    def test_anonymous_failed_adoption_never_pulls_and_changed_compiled_dataset_never_qualifies(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            records = self.prepared(root)
            config = parent / 'docker-config'
            config.mkdir()
            adopted = mock.Mock()
            adopted.app = _freeze({'platforms': {'linux/amd64': copy.deepcopy(records[0])}})
            with mock.patch.dict(os.environ, {'DOCKER_CONFIG': str(config)}, clear=True), \
                    mock.patch.object(publish, '_native', return_value=('linux/amd64', 'x86_64')), \
                    mock.patch.object(publish, 'baseline', return_value=self.baseline()), \
                    mock.patch.object(adoption, 'adopt_startup_hotel_artifacts', side_effect=ValueError('bad adoption')) as adopt, \
                    mock.patch.object(publish.subprocess, 'run') as run, \
                    mock.patch.object(smoke_module, 'smoke_startup_hotel_image') as smoke:
                with self.assertRaisesRegex(ValueError, 'bad adoption'):
                    publish.anonymous_record('amd64', root, parent / 'upstream', parent / 'baseline', parent / 'publication')
                run.assert_not_called()
                smoke.assert_not_called()
                adopt.side_effect = None
                adopt.return_value = adopted
                for mismatch in ('dataset_fingerprint', 'startup_attestations'):
                    changed = copy.deepcopy(records[0]['smoke_receipt'])
                    changed[mismatch] = ('sha256:' + '9' * 64 if mismatch == 'dataset_fingerprint'
                                         else {'geo': {'component': 'frontend'}})
                    smoke.return_value = changed
                    with self.subTest(mismatch=mismatch), self.assertRaises(PreparationError):
                        publish.anonymous_record('amd64', root, parent / 'upstream',
                                                 parent / 'baseline', parent / 'publication')
                self.assertEqual(run.call_count, 2)
                self.assertEqual(smoke.call_count, 2)

    def test_anonymous_real_factory_binding_accepts_frozen_metadata_but_refuses_identity_or_flag_drift(self):
        # Keep the existing real adoption fixture's exact proof/freeze behavior:
        # only source reconstruction and ELF execution are unit substitutes.
        # This integration caught a real failure that plain dict mocks missed.
        from tests.test_deathstarbench_hotel_startup_artifacts import StartupHotelArtifactTests
        helper = StartupHotelArtifactTests()
        self.addCleanup(helper.doCleanups)
        data = helper.fixture()
        for platform in data['publication']['app']['platforms'].values():
            platform['smoke_receipt']['dataset_fingerprint'] = platform['smoke_receipt']['dataset_attestation']['dataset_fingerprint']
        binding = helper.adopt(data)
        self.assertIs(type(binding), adoption.StartupHotelArtifactBinding)
        original = _thaw(binding.app['platforms']['linux/amd64']['smoke_receipt'])
        self.assertEqual(set(original['startup_attestations']), set(factory.preparation_module.APPLICATIONS))
        with self.assertRaises(TypeError):
            binding.app['platforms']['linux/amd64']['smoke_receipt']['startup_attestations']['frontend']['component'] = 'changed'
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / 'prepared'
            self.prepared(root)
            config = parent / 'docker-config'
            config.mkdir()
            with mock.patch.dict(os.environ, {'DOCKER_CONFIG': str(config)}, clear=True), \
                    mock.patch.object(publish, '_native', return_value=('linux/amd64', 'x86_64')), \
                    mock.patch.object(publish, 'baseline', return_value=data['baseline']), \
                    mock.patch.object(adoption, 'adopt_startup_hotel_artifacts', return_value=binding), \
                    mock.patch.object(publish.subprocess, 'run') as run, \
                    mock.patch.object(smoke_module, 'smoke_startup_hotel_image', return_value=copy.deepcopy(original)) as smoke:
                proof = publish.anonymous_record('amd64', root, parent / 'upstream',
                                                parent / 'baseline', parent / 'publication')
                self.assertEqual(proof['artifact_adoption']['fingerprint'], binding.fingerprint)
                self.assertEqual(proof['baseline_fingerprint'], data['baseline'].fingerprint)
                self.assertEqual(proof['smoke_receipt']['startup_attestations'], original['startup_attestations'])
                for field, value in (
                    ('component', 'geo'), ('policy_revision', 'different-policy'),
                    ('config_sha256', 'sha256:' + '9' * 64),
                    ('database_execution_verified', True), ('namespace', 'different-run'),
                    ('unit', 'different-unit'),
                ):
                    changed = copy.deepcopy(original)
                    changed['startup_attestations']['frontend'][field] = value
                    self.assertEqual(changed['dataset_attestation'], original['dataset_attestation'])
                    smoke.return_value = changed
                    with self.subTest(field=field), self.assertRaises(PreparationError):
                        publish.anonymous_record('amd64', root, parent / 'upstream',
                                                 parent / 'baseline', parent / 'publication')
                self.assertEqual(run.call_count, 7)
                self.assertEqual(smoke.call_count, 7)
                # Comparison must not thaw or otherwise mutate the capability.
                self.assertEqual(_thaw(binding.app['platforms']['linux/amd64']['smoke_receipt']), original)


if __name__ == '__main__':
    unittest.main()
