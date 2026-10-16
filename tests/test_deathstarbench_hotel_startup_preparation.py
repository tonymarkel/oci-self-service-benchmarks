"""Synthetic additive startup preparation checks, never runtime qualification.

The native CI job separately prepares the audited upstream source and executes
the real Go helpers. These fixtures isolate filesystem, receipt and preservation
contracts; their mocked source patchers do not attest application behavior.
"""

import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app.deathstarbench_hotel_initialization import hotel_initialization_policy
from scripts import create_deathstarbench_candidate_image_lock as original_factory
from scripts import prepare_deathstarbench_hotel_startup as preparation
from scripts import validate_deathstarbench_hotel_initialization as guarded_validation
from scripts.deathstarbench_artifact_common import PreparationError, sha256, tree_sha256


def canonical(value):
    return (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()


def normalized_modes(root):
    for path in (root, *root.rglob('*')):
        path.chmod(0o755 if path.is_dir() else 0o644)


class HotelStartupPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='hotel-startup-preparation-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.upstream = self.root / 'upstream'
        (self.upstream / 'hotelReservation').mkdir(parents=True)
        (self.upstream / 'LICENSE').write_text('synthetic pinned license\n')
        (self.upstream / 'hotelReservation/sentinel').write_text('preserve upstream\n')
        self.assets = self.root / 'assets'
        self.assets.mkdir()
        self.asset_targets = dict(preparation.ASSET_TARGETS)
        for name in self.asset_targets:
            path = self.assets / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('package fixture\n// synthetic ' + name + '\n')
        normalized_modes(self.assets)
        self.base_receipts = []
        self.base_sources = []
        self.base_change = None
        for guard in (
            patch.object(preparation, 'ASSET_ROOT', self.assets),
            patch.object(preparation.guarded, 'prepare_contexts', side_effect=self.guarded),
            patch.object(preparation.core, 'patch_main', side_effect=self.patch_main),
            patch.object(preparation.core, 'patch_tls', side_effect=self.patch_tls),
            patch.object(preparation.services, 'patch_service', side_effect=self.patch_service),
        ):
            guard.start()
            self.addCleanup(guard.stop)

    def patch_main(self, text, component, config_sha256):
        self.assertIn(component, preparation.APPLICATIONS)
        self.assertEqual(len(config_sha256), 64)
        return text + '// synthetic startup-main patch: ' + component + '\n'

    def patch_service(self, relative, text):
        self.assertIn(relative, preparation.services.SERVICE_PATHS)
        return text + '// synthetic startup-service patch: ' + relative + '\n'

    def patch_tls(self, text):
        return text + '// synthetic startup TLS-init patch\n'

    def guarded(self, upstream, destination):
        self.assertEqual(upstream, self.upstream)
        app = destination / 'app'
        app.mkdir(parents=True)
        sources = {}
        startup_patches = {}
        for component in preparation.APPLICATIONS:
            sources['cmd/' + component + '/main.go'] = (
                'package main\n// synthetic guarded main: ' + component + '\n')
        for component in preparation.guarded.COMPONENTS:
            sources['cmd/' + component + '/db.go'] = (
                'package main\n// own-store read-only verification: ' + component + '\n')
        for name in preparation.services.SERVICE_PATHS:
            sources[name] = 'package fixture\n// synthetic original service: ' + name + '\n'
        sources.update({
            'registry/registry.go': 'package registry\n// retained guarded registry\n',
            'tls/options.go': 'package tls\n// retained guarded TLS initialization\n',
            'cmd/hotel-initialize/main.go': 'package main\n// immutable guarded initializer\n',
            'config.json': '{"fixture":"baked-config"}\n',
            'Dockerfile.candidate': preparation.guarded._dockerfile(),
            'initialization/dataset.go': 'package initialization\n// immutable dataset\n',
            'initialization/protocol.go': 'package initialization\n// immutable protocol\n',
        })
        source_input, fingerprint = guarded_validation._source_fingerprint()
        generated = {
            'initialization/catalog/catalog.go': 'package catalog\n// immutable synthetic catalog\n',
            **{'initialization/seeddata/' + component + '/seed.go':
               'package seeddata\n// immutable seed: ' + component + '\n'
               for component in preparation.guarded.COMPONENTS},
        }
        sources.update(generated)
        for name, text in sources.items():
            path = app / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        normalized_modes(destination)
        for component in preparation.APPLICATIONS:
            name = 'cmd/' + component + '/main.go'
            startup_patches[name] = {
                'original_sha256': hashlib.sha256(('original ' + component).encode()).hexdigest(),
                'patched_sha256': sha256(app / name),
            }
        for component in preparation.guarded.COMPONENTS:
            name = 'cmd/' + component + '/db.go'
            startup_patches[name] = {
                'original_sha256': hashlib.sha256(('original db ' + component).encode()).hexdigest(),
                'patched_sha256': sha256(app / name),
            }
        legacy = {
            'schema_version': 1, 'candidate_only': True, 'released': False,
            'recipe_revision': preparation.guarded.legacy.RECIPE_REVISION,
            'source_inventory': [{'path': 'hotelReservation/' + name, 'sha256': sha256(app / name)}
                                 for name in sorted(sources)],
        }
        (destination / 'base-preparation.json').write_bytes(canonical(legacy))
        receipt = {
            'schema_version': 1,
            'candidate_schema': preparation.guarded.PREPARATION_SCHEMA,
            'candidate_only': True, 'released': False, 'workload_id': 'hotel_reservation',
            'upstream_revision': preparation.guarded.legacy.UPSTREAM_REVISION,
            'recipe_revision': preparation.guarded.RECIPE_REVISION,
            'patch_revision': preparation.guarded.PATCH_REVISION,
            'dataset_revision': preparation.guarded.DATASET_REVISION,
            'initialization_revision': preparation.guarded.CONTRACT_REVISION,
            'initialization_policy': hotel_initialization_policy(),
            'legacy_base_preparation_sha256': sha256(destination / 'base-preparation.json'),
            'legacy_base_context_sha256': '1' * 64,
            'seed_source': {
                'schema_version': 1, 'candidate_only': True, 'released': False,
                'qualification_authority': False,
                'generator_revision': source_input['generator_revision'],
                'arithmetic_policy': source_input['arithmetic_policy'],
                'source_fingerprint_input': source_input,
                'source_fingerprint': fingerprint,
                'generated_source_sha256': {
                    name: hashlib.sha256(text.encode()).hexdigest() for name, text in generated.items()},
            },
            'startup_source_patches': startup_patches,
            'initializer_source_sha256': sha256(app / 'cmd/hotel-initialize/main.go'),
            'contexts': {'app': {'path': 'app', 'dockerfile': 'Dockerfile.candidate',
                                 'context_sha256': tree_sha256(app)}},
            'software': {'go_version': '1.21.13'},
            'license_identities': {'fixture': True},
            'config_sha256': sha256(app / 'config.json'),
            'native_platforms': ['linux/amd64', 'linux/arm64'],
            **{key: False for key in guarded_validation._FALSE_FLAGS},
        }
        (destination / 'context-manifest.json').write_bytes(canonical(receipt))
        normalized_modes(destination)
        self.base_receipts.append(copy.deepcopy(receipt))
        self.base_sources.append(copy.deepcopy(sources))
        if self.base_change is not None:
            self.base_change(destination, receipt)
        return receipt

    def prepare(self, name='output'):
        target = self.root / name
        return target, preparation.prepare_contexts(self.upstream, target)

    def test_separate_recipe_is_deterministic_and_all_execution_gates_stay_closed(self):
        first, receipt = self.prepare('first')
        second, repeated = self.prepare('second')
        self.assertEqual(receipt, repeated)
        self.assertEqual(tree_sha256(first), tree_sha256(second))
        self.assertEqual(receipt['candidate_schema'], 'hotel-guarded-startup-preparation-v1')
        self.assertEqual(receipt['recipe_revision'],
                         'hotel-reservation-guarded-startup-native-go-scratch-candidate-v1')
        self.assertEqual(receipt['patch_revision'], 'hotel-reservation-guarded-startup-source-candidate-v1')
        self.assertTrue(receipt['candidate_only'])
        for key in ('released', 'qualification_authority', 'images_built', 'images_published',
                    'database_executed', 'initialization_executed', 'dataset_qualified',
                    'application_start_allowed', 'runtime_qualified', 'measurement_qualified',
                    'cloud_qualified', 'public_release', 'current_candidate_factory_accepts_recipe',
                    'current_guarded_factory_accepts_recipe'):
            self.assertIs(receipt[key], False, key)
        self.assertEqual(receipt['policy_revision'], 'hotel-guarded-startup-source-policy-v1')
        self.assertEqual(receipt['startup_policy'], preparation.startup_policy())
        self.assertEqual(receipt['contexts']['app']['context_sha256'], tree_sha256(first / 'app'))
        self.assertEqual(json.loads((first / 'context-manifest.json').read_bytes()), receipt)
        self.assertFalse((first / 'base').exists())

    def test_source_preparation_does_not_execute_network_cloud_or_subprocesses(self):
        with patch('socket.create_connection', side_effect=AssertionError('network forbidden')), \
                patch('subprocess.run', side_effect=AssertionError('process forbidden')):
            _target, receipt = self.prepare()
        self.assertIs(receipt['qualification_authority'], False)

    def test_canonical_output_bytes_and_modes_do_not_depend_on_umask(self):
        first, receipt = self.prepare('first')
        previous = os.umask(0o077)
        try:
            second, repeated = self.prepare('restricted')
        finally:
            os.umask(previous)
        self.assertEqual(receipt, repeated)
        self.assertEqual(tree_sha256(first), tree_sha256(second))
        for path in (second, *second.rglob('*')):
            self.assertEqual(path.stat().st_mode & 0o777, 0o755 if path.is_dir() else 0o644)

    def test_complete_guarded_and_legacy_receipts_are_retained_with_exact_hashes(self):
        target, receipt = self.prepare()
        self.assertEqual(json.loads((target / 'guarded-base-preparation.json').read_bytes()), self.base_receipts[0])
        self.assertEqual(receipt['guarded_base_preparation_sha256'], sha256(target / 'guarded-base-preparation.json'))
        self.assertEqual(receipt['legacy_base_preparation_sha256'], sha256(target / 'legacy-base-preparation.json'))
        self.assertEqual(receipt['seed_source'], self.base_receipts[0]['seed_source'])
        self.assertEqual(receipt['dataset_revision'], self.base_receipts[0]['dataset_revision'])
        self.assertEqual(receipt['retained_initializer_source_sha256'], self.base_receipts[0]['initializer_source_sha256'])
        self.assertEqual(sha256(target / 'app/cmd/hotel-initialize/main.go'), receipt['retained_initializer_source_sha256'])
        for name, digest in receipt['seed_source']['generated_source_sha256'].items():
            self.assertEqual(sha256(target / 'app' / name), digest)

    def test_source_patch_receipt_binds_exact_original_and_patched_inventory(self):
        target, receipt = self.prepare()
        expected = {
            *(f'cmd/{component}/main.go' for component in preparation.APPLICATIONS),
            *preparation.services.SERVICE_PATHS, 'tls/options.go', 'registry/registry.go',
        }
        self.assertEqual(set(receipt['source_patches']), expected)
        for relative, row in receipt['source_patches'].items():
            self.assertEqual(set(row), {'original_sha256', 'patched_sha256'})
            self.assertEqual(row['original_sha256'],
                             hashlib.sha256(self.base_sources[0][relative].encode()).hexdigest())
            self.assertEqual(row['patched_sha256'], sha256(target / 'app' / relative))
            self.assertNotEqual(row['original_sha256'], row['patched_sha256'])
        self.assertEqual(receipt['startup_asset_sha256'], {
            source: sha256(self.assets / source) for source in self.asset_targets})
        for source, relative in self.asset_targets.items():
            self.assertEqual((target / 'app' / relative).read_bytes(), (self.assets / source).read_bytes())

    def test_original_legacy_and_guarded_recipes_and_defaults_are_unchanged(self):
        self.assertEqual(preparation.guarded.legacy.RECIPE_REVISION,
                         'hotel-reservation-native-go-scratch-candidate-v1')
        self.assertEqual(preparation.guarded.RECIPE_REVISION,
                         'hotel-reservation-guarded-init-native-go-scratch-candidate-v1')
        self.assertNotIn('./startup', preparation.guarded._dockerfile())
        current = preparation._dockerfile()
        self.assertIn(preparation.RECIPE_REVISION, current)
        self.assertIn('USER 65532:65532', current)
        self.assertIn('RUN --network=none', current)

    def test_new_preparation_cannot_pass_existing_original_or_guarded_factories(self):
        target, _receipt = self.prepare()
        with self.assertRaises(original_factory.CandidateImageLockError):
            original_factory._workload_preparation(target, 'hotel_reservation')
        with preparation.guarded._GuardedFiles(target) as files:
            with self.assertRaisesRegex(PreparationError, 'scope, revision or closed gates'):
                guarded_validation._validate_preparation(files)

    def test_existing_output_and_protected_inputs_are_preserved(self):
        output = self.root / 'existing'
        output.mkdir()
        (output / 'sentinel').write_text('preserve existing\n')
        before = tree_sha256(self.upstream), tree_sha256(self.assets)
        for target in (output, self.upstream / 'overlap', self.assets / 'overlap'):
            with self.subTest(target=target), self.assertRaises(PreparationError):
                preparation.prepare_contexts(self.upstream, target)
        self.assertEqual((output / 'sentinel').read_text(), 'preserve existing\n')
        self.assertEqual(before, (tree_sha256(self.upstream), tree_sha256(self.assets)))

    def test_corrupt_guarded_context_config_receipt_or_initializer_is_refused(self):
        for name in ('app/config.json', 'app/cmd/hotel-initialize/main.go',
                     'app/initialization/catalog/catalog.go', 'base-preparation.json', 'context-manifest.json'):
            with self.subTest(name=name):
                self.base_change = lambda root, _receipt: (root / name).write_text('corrupt guarded input\n')
                with self.assertRaises(PreparationError):
                    self.prepare()
                self.assertFalse((self.root / 'output').exists())
                self.assertEqual((self.upstream / 'hotelReservation/sentinel').read_text(), 'preserve upstream\n')

    def test_base_recipe_identity_and_boolean_gate_aliases_are_refused(self):
        changes = (
            ('schema_version', True), ('candidate_schema', preparation.PREPARATION_SCHEMA),
            ('candidate_only', 1), ('workload_id', 'media_microservices'),
            ('recipe_revision', preparation.RECIPE_REVISION), ('dataset_revision', 'foreign-dataset'),
            ('released', True), ('database_executed', 0), ('application_start_allowed', True),
        )
        for key, value in changes:
            def alter(root, receipt):
                receipt[key] = value
                (root / 'context-manifest.json').write_bytes(canonical(receipt))
            self.base_change = alter
            with self.subTest(key=key, value=value), patch.object(preparation.core, 'patch_main') as apply_source:
                with self.assertRaises(PreparationError):
                    self.prepare()
                apply_source.assert_not_called()
                self.assertFalse((self.root / 'output').exists())

    def test_changed_dataset_or_initializer_during_source_patching_is_refused(self):
        for relative in ('initialization/catalog/catalog.go', 'cmd/hotel-initialize/main.go'):
            def alter(text, component, config_sha256):
                target = self.root / 'output/app' / relative
                target.write_bytes(target.read_bytes() + b'// concurrent mutation\n')
                return self.patch_main(text, component, config_sha256)
            with self.subTest(relative=relative), patch.object(preparation.core, 'patch_main', side_effect=alter):
                with self.assertRaises(PreparationError):
                    self.prepare()
                self.assertFalse((self.root / 'output').exists())

    def test_missing_unexpected_nested_or_unsafe_asset_inventory_is_refused(self):
        selected = next(iter(self.asset_targets))
        original = (self.assets / selected).read_bytes()
        (self.assets / selected).unlink()
        with self.assertRaises(PreparationError):
            self.prepare()
        (self.assets / selected).write_bytes(original)
        for name in ('unknown.go', 'startup/unexpected.go', 'startup/nested/unexpected.go'):
            with self.subTest(name=name):
                path = self.assets / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('package hostile\n')
                with self.assertRaises(PreparationError):
                    self.prepare()
                self.assertFalse((self.root / 'output').exists())
                path.unlink()
                if name.startswith('startup/nested/'):
                    path.parent.rmdir()

    def test_asset_target_path_traversal_is_refused_without_creating_external_files(self):
        source = next(iter(self.asset_targets))
        for target in ('../escape.go', '/escape.go', 'startup/../escape.go',
                       'startup//escape.go', 'startup/./escape.go', 'startup/bad\\escape.go'):
            mapping = {**self.asset_targets, source: target}
            with self.subTest(target=target), patch.object(preparation, 'ASSET_TARGETS', mapping):
                with self.assertRaises(PreparationError):
                    self.prepare()
                self.assertFalse((self.root / 'output').exists())
                self.assertFalse((self.root / 'escape.go').exists())

    def test_guarded_input_links_and_hardlinks_fail_before_any_source_patch(self):
        foreign = self.root / 'foreign-guarded.go'
        foreign.write_text('foreign guarded source\n')
        for kind in ('symlink', 'hardlink'):
            def substitute(root, _receipt):
                path = root / 'app/cmd/attractions/main.go'
                path.unlink()
                if kind == 'symlink':
                    path.symlink_to(foreign)
                else:
                    os.link(foreign, path)
            self.base_change = substitute
            with self.subTest(kind=kind), patch.object(preparation.core, 'patch_main') as apply_source:
                with self.assertRaises(PreparationError):
                    self.prepare()
                apply_source.assert_not_called()
                self.assertFalse((self.root / 'output').exists())
                self.assertEqual(foreign.read_text(), 'foreign guarded source\n')

    def test_assets_reject_symlinks_and_hardlinks_without_touching_targets(self):
        selected = self.assets / next(iter(self.asset_targets))
        original = selected.read_bytes()
        foreign = self.root / 'foreign.go'
        foreign.write_text('foreign source must survive\n')
        for kind in ('symlink', 'hardlink'):
            with self.subTest(kind=kind):
                selected.unlink()
                if kind == 'symlink':
                    selected.symlink_to(foreign)
                else:
                    os.link(foreign, selected)
                with self.assertRaises(PreparationError):
                    self.prepare()
                self.assertEqual(foreign.read_text(), 'foreign source must survive\n')
                self.assertFalse((self.root / 'output').exists())
                selected.unlink()
                selected.write_bytes(original)

    def test_main_patch_failure_only_removes_new_owned_output(self):
        with patch.object(preparation.core, 'patch_main', side_effect=PreparationError('exact main anchor drift')):
            with self.assertRaisesRegex(PreparationError, 'anchor drift'):
                self.prepare()
        self.assertFalse((self.root / 'output').exists())
        self.assertTrue(self.upstream.is_dir())
        self.assertTrue(self.assets.is_dir())

    def test_main_change_after_guarded_read_is_never_overwritten(self):
        def change(text, component, config_sha256):
            path = self.root / 'output/app/cmd' / component / 'main.go'
            path.write_text(text + '// changed after guarded read\n')
            return self.patch_main(text, component, config_sha256)
        with patch.object(preparation.core, 'patch_main', side_effect=change):
            with self.assertRaises(PreparationError):
                self.prepare()
        self.assertFalse((self.root / 'output').exists())

    def test_source_file_substitution_cannot_follow_symlink_or_hardlink(self):
        foreign = self.root / 'foreign-main.go'
        foreign.write_text('foreign main must survive\n')
        for kind in ('symlink', 'hardlink'):
            def substitute(text, component, config_sha256):
                path = self.root / 'output/app/cmd' / component / 'main.go'
                path.unlink()
                if kind == 'symlink':
                    path.symlink_to(foreign)
                else:
                    os.link(foreign, path)
                return self.patch_main(text, component, config_sha256)
            with self.subTest(kind=kind), patch.object(preparation.core, 'patch_main', side_effect=substitute):
                with self.assertRaises(PreparationError):
                    self.prepare()
                self.assertFalse((self.root / 'output').exists())
                self.assertEqual(foreign.read_text(), 'foreign main must survive\n')

    def test_source_parent_substitution_never_writes_to_foreign_directory(self):
        foreign = self.root / 'foreign-directory'
        foreign.mkdir()
        (foreign / 'main.go').write_text('foreign main\n')
        def substitute(text, component, config_sha256):
            parent = self.root / 'output/app/cmd' / component
            parent.rename(parent.with_name('held-' + component))
            parent.symlink_to(foreign, target_is_directory=True)
            return self.patch_main(text, component, config_sha256)
        with patch.object(preparation.core, 'patch_main', side_effect=substitute):
            with self.assertRaises(PreparationError):
                self.prepare()
        self.assertFalse((self.root / 'output').exists())
        self.assertEqual((foreign / 'main.go').read_text(), 'foreign main\n')

    def test_replaced_output_root_is_preserved_and_never_used_for_writes(self):
        foreign = self.root / 'foreign-root'
        foreign.mkdir()
        (foreign / 'sentinel').write_text('preserve foreign root\n')
        def substitute(text, component, config_sha256):
            output = self.root / 'output'
            output.rename(self.root / 'held-output')
            output.symlink_to(foreign, target_is_directory=True)
            return self.patch_main(text, component, config_sha256)
        with patch.object(preparation.core, 'patch_main', side_effect=substitute):
            with self.assertRaisesRegex(PreparationError, 'replacement preserved'):
                self.prepare()
        self.assertTrue((self.root / 'output').is_symlink())
        self.assertEqual(list(foreign.iterdir()), [foreign / 'sentinel'])
        self.assertEqual((foreign / 'sentinel').read_text(), 'preserve foreign root\n')


if __name__ == '__main__':
    unittest.main()
