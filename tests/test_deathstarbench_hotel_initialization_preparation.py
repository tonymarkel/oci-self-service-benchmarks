"""Synthetic preparation failures; real pinned inputs are exercised in CI."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import prepare_deathstarbench_hotel_initialization as preparation
from scripts.deathstarbench_artifact_common import PreparationError, sha256, tree_sha256


MAIN = '''package main
import (
    "os"
)
func main() {
    tune.Init()
    _ = os.Stdout
}
'''


class HotelInitializationPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='hotel-init-preparation-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.upstream = self.root / 'upstream'
        self.upstream.mkdir()
        self.assets = self.root / 'assets'
        (self.assets / 'initialization').mkdir(parents=True)
        (self.assets / 'initialization/guard.go').write_text('package initialization\n')
        self.generated = {'initialization/catalog/catalog.go': 'package catalog\n'}
        self.receipts = {'source_fingerprint': 'sha256:' + '1' * 64,
                         'generated_source_sha256': {name: hashlib.sha256(text.encode()).hexdigest()
                                                     for name, text in self.generated.items()}}
        for target, value in (
            ('ASSET_ROOT', self.assets),
            ('seed_source_receipts', lambda _: self.receipts),
            ('generate_seed_sources', lambda _: self.generated),
        ):
            guard = patch.object(preparation, target, value)
            guard.start()
            self.addCleanup(guard.stop)
        guard = patch.object(preparation.legacy, 'prepare_contexts', side_effect=self.legacy)
        guard.start()
        self.addCleanup(guard.stop)

    def legacy(self, upstream, destination):
        app = destination / 'app'
        app.mkdir(parents=True)
        inventory = []
        for component in preparation.APPLICATIONS:
            path = app / f'cmd/{component}/main.go'
            path.parent.mkdir(parents=True)
            path.write_text(MAIN)
            inventory.append({'path': f'hotelReservation/cmd/{component}/main.go', 'sha256': sha256(path)})
            if component in preparation.COMPONENTS:
                (path.parent / 'db.go').write_text('package main\n// original startup seed fixture\n')
        (app / 'config.json').write_text('{}\n')
        # Mirror v1's canonical context modes, not the caller's umask.
        for path in (destination, app, *app.rglob('*')):
            path.chmod(0o755 if path.is_dir() else 0o644)
        result = {
            'schema_version': 1, 'candidate_only': True, 'released': False,
            'source_inventory': inventory,
            'contexts': {'app': {'context_sha256': tree_sha256(app)}},
            'software': {'go_version': '1.21.13'}, 'license_identities': {'fixture': True},
            'config_sha256': sha256(app / 'config.json'),
            'native_platforms': ['linux/amd64', 'linux/arm64'],
        }
        (destination / 'context-manifest.json').write_text(json.dumps(result, sort_keys=True) + '\n')
        return result

    def prepare(self, name='output'):
        destination = self.root / name
        return destination, preparation.prepare_contexts(self.upstream, destination)

    def test_new_recipe_is_deterministic_and_cannot_claim_current_adoption_or_runtime(self):
        first, receipt = self.prepare('first')
        second, repeated = self.prepare('second')
        self.assertEqual(receipt, repeated)
        self.assertEqual(tree_sha256(first), tree_sha256(second))
        self.assertEqual(receipt['contexts']['app']['context_sha256'], tree_sha256(first / 'app'))
        self.assertEqual(receipt['candidate_schema'], 'hotel-guarded-initialization-preparation-v1')
        self.assertTrue(receipt['candidate_only'])
        for field in ('released', 'images_built', 'images_published', 'database_executed',
                      'application_start_allowed', 'runtime_qualified', 'measurement_qualified',
                      'current_candidate_factory_accepts_recipe'):
            self.assertIs(receipt[field], False)
        self.assertTrue((first / 'base-preparation.json').is_file())
        self.assertFalse((first / 'base').exists())
        self.assertEqual(len(receipt['startup_source_patches']), 18)

    def test_prepared_modes_and_hashes_do_not_depend_on_callers_umask(self):
        first, receipt = self.prepare('first')
        previous = os.umask(0o077)
        try:
            second, repeated = self.prepare('restricted')
        finally:
            os.umask(previous)
        self.assertEqual(receipt, repeated)
        self.assertEqual(tree_sha256(first), tree_sha256(second))
        self.assertEqual((second / 'app/initialization').stat().st_mode & 0o777, 0o755)
        self.assertEqual((second / 'app/cmd/hotel-initialize').stat().st_mode & 0o777, 0o755)

    def test_guarded_whole_root_hash_matches_common_canonical_tree_hash(self):
        destination, _ = self.prepare()
        # Prefix-like siblings exercise full POSIX path ordering, not tuples.
        (destination / 'a-b').write_bytes(b'first')
        (destination / 'a').mkdir()
        (destination / 'a/z').write_bytes(b'second')
        with preparation._GuardedFiles(destination) as owned:
            self.assertEqual(owned.tree_digest(), tree_sha256(destination))
            self.assertEqual(owned.tree_digest('app'), tree_sha256(destination / 'app'))

    def test_all_application_mains_attest_before_tune_config_or_identity(self):
        destination, _ = self.prepare()
        for component in preparation.APPLICATIONS:
            source = (destination / f'app/cmd/{component}/main.go').read_text()
            self.assertLess(source.index('initialization.Attest('), source.index('IdentityFromEnv'))
            self.assertLess(source.index('IdentityFromEnv'), source.index('tune.Init'))
            self.assertIn('catalog.SourceFingerprint', source)
            self.assertIn('identity.ContractSHA256 != dataset.Fingerprint()', source)

    def test_ordinary_database_startup_has_only_bounded_own_store_verification(self):
        destination, _ = self.prepare()
        for component in preparation.COMPONENTS:
            source = (destination / f'app/cmd/{component}/db.go').read_text()
            self.assertIn(f'ConnectAndVerify(ctx, "{component}"', source)
            self.assertIn('30*time.Second', source)
            for forbidden in ('InsertMany', 'InsertOne', 'UpdateOne', 'Delete', 'Drop', '.Initialize('):
                self.assertNotIn(forbidden, source)
        self.assertIn('func seededUsername', (destination / 'app/cmd/user/db.go').read_text())

    def test_initializer_requires_exact_mode_identity_and_pinned_configuration_before_store_access(self):
        source = preparation._initializer('f' * 64)
        self.assertLess(source.index('initialization.Attest('), source.index('os.Open('))
        self.assertLess(source.index('IdentityFromEnv'), source.index('os.Open('))
        self.assertLess(source.index('sha256.Sum256(raw)'), source.index('OpenMongoStores('))
        self.assertIn('os.Args[1] != "initialize-empty" && os.Args[1] != "verify"', source)
        self.assertIn('io.LimitReader(file, 65537)', source)
        self.assertIn('address != "mongodb-"+component+":27017"', source)
        self.assertIn('5*time.Minute', source)
        self.assertIn('"application_start_allowed": false', source)
        self.assertNotIn('os.Getenv("MONGO', source)

    def test_original_preparation_recipe_constants_and_default_build_remain_unchanged(self):
        self.assertEqual(preparation.legacy.RECIPE_REVISION, 'hotel-reservation-native-go-scratch-candidate-v1')
        self.assertEqual(preparation.legacy.DATASET_REVISION, 'hotel-reservation-decimal-user-seed-candidate-v1')
        self.assertNotIn('COPY initialization/', preparation.legacy._dockerfile())
        revised = preparation._dockerfile()
        self.assertIn(preparation.RECIPE_REVISION, revised)
        self.assertIn('COPY initialization/', revised)
        self.assertIn('./initialization/... ./cmd/...', revised)
        self.assertIn('RUN --network=none', revised)
        self.assertIn('USER 65532:65532', revised)

    def test_existing_output_and_protected_input_are_preserved(self):
        output = self.root / 'existing'
        output.mkdir()
        (output / 'sentinel').write_text('keep')
        with self.assertRaises(PreparationError):
            preparation.prepare_contexts(self.upstream, output)
        self.assertEqual((output / 'sentinel').read_text(), 'keep')
        with self.assertRaises(PreparationError):
            preparation.prepare_contexts(self.upstream, self.upstream / 'overlap')
        self.assertFalse((self.upstream / 'overlap').exists())

    def test_generated_hash_drift_or_path_escape_removes_only_new_owned_output(self):
        with patch.object(preparation, 'generate_seed_sources', return_value={'initialization/catalog/catalog.go': 'changed'}):
            with self.assertRaisesRegex(PreparationError, 'changed between validation and generation'):
                self.prepare()
        for name in ('../escape.go', '/escape.go', 'initialization/../escape.go',
                     'initialization//escape.go', 'initialization/./escape.go',
                     'initialization/escape\\bad.go', 'other/escape.go'):
            generated = {name: 'package escape\n'}
            receipts = {**self.receipts, 'generated_source_sha256': {
                name: hashlib.sha256(generated[name].encode()).hexdigest()}}
            with self.subTest(name=name), \
                    patch.object(preparation, 'generate_seed_sources', return_value=generated), \
                    patch.object(preparation, 'seed_source_receipts', return_value=receipts):
                with self.assertRaisesRegex(PreparationError, 'Unsafe generated Hotel initialization path'):
                    self.prepare()
            self.assertFalse((self.root / 'output').exists())
            self.assertFalse((self.root / 'escape.go').exists())
            self.assertTrue(self.upstream.is_dir())

    def test_asset_symlinks_are_not_copied_and_failed_output_is_recoverably_scoped(self):
        (self.assets / 'initialization/foreign.go').symlink_to(self.assets / 'initialization/guard.go')
        with self.assertRaises(PreparationError):
            self.prepare()
        self.assertFalse((self.root / 'output').exists())
        self.assertTrue((self.assets / 'initialization/guard.go').is_file())

    def test_main_anchor_drift_is_rejected(self):
        for text in ('package main\n', MAIN + '\nfunc main() {\n}\n'):
            with self.assertRaises(PreparationError):
                preparation._main(text)

    def test_main_bytes_are_not_reread_after_validated_hash(self):
        with patch.object(Path, 'read_text', side_effect=AssertionError('unverified text reread')):
            _, receipt = self.prepare()
        self.assertEqual(len(receipt['startup_source_patches']), 18)

    def test_main_changed_between_read_and_patch_is_not_overwritten(self):
        original_main = preparation._main

        def change(text):
            path = self.root / 'output/app/cmd/attractions/main.go'
            path.write_text(MAIN + '// changed after guarded read\n')
            return original_main(text)

        with patch.object(preparation, '_main', side_effect=change):
            with self.assertRaisesRegex(PreparationError, 'output file was replaced'):
                self.prepare()
        self.assertFalse((self.root / 'output').exists())

    def test_output_file_substitution_cannot_follow_symlink_or_hardlink(self):
        foreign = self.root / 'foreign.go'
        foreign.write_text('foreign content must remain unchanged\n')
        original_main = preparation._main
        for kind in ('symlink', 'hardlink'):
            def replace(text):
                path = self.root / 'output/app/cmd/attractions/main.go'
                path.unlink()
                if kind == 'symlink':
                    path.symlink_to(foreign)
                else:
                    os.link(foreign, path)
                return original_main(text)

            with self.subTest(kind=kind), patch.object(preparation, '_main', side_effect=replace):
                with self.assertRaises(PreparationError):
                    self.prepare()
            self.assertEqual(foreign.read_text(), 'foreign content must remain unchanged\n')
            self.assertFalse((self.root / 'output').exists())

    def test_output_parent_substitution_never_writes_to_foreign_directory(self):
        foreign = self.root / 'foreign-directory'
        foreign.mkdir()
        (foreign / 'main.go').write_text('foreign source\n')
        original_main = preparation._main

        def replace(text):
            parent = self.root / 'output/app/cmd/attractions'
            parent.rename(parent.with_name('held-attractions'))
            parent.symlink_to(foreign, target_is_directory=True)
            return original_main(text)

        with patch.object(preparation, '_main', side_effect=replace):
            with self.assertRaisesRegex(PreparationError, 'directory identity or type changed'):
                self.prepare()
        self.assertEqual((foreign / 'main.go').read_text(), 'foreign source\n')
        self.assertFalse((self.root / 'output').exists())

    def test_replaced_output_root_is_preserved_and_never_used_for_writes(self):
        foreign = self.root / 'foreign-root'
        foreign.mkdir()
        (foreign / 'sentinel').write_text('keep')
        original_main = preparation._main

        def replace(text):
            output = self.root / 'output'
            output.rename(self.root / 'held-output')
            output.symlink_to(foreign, target_is_directory=True)
            return original_main(text)

        with patch.object(preparation, '_main', side_effect=replace):
            with self.assertRaisesRegex(PreparationError, 'cleanup refused and replacement preserved'):
                self.prepare()
        self.assertTrue((self.root / 'output').is_symlink())
        self.assertTrue((self.root / 'held-output/app/cmd/attractions/main.go').is_file())
        self.assertEqual(list(foreign.iterdir()), [foreign / 'sentinel'])
        self.assertEqual((foreign / 'sentinel').read_text(), 'keep')

    def test_stable_asset_read_rejects_in_place_change_during_read(self):
        source = self.assets / 'initialization/guard.go'
        with preparation._GuardedFiles(self.assets) as assets:
            real_fdopen = preparation.os.fdopen

            def change(descriptor, *args, **kwargs):
                stream = real_fdopen(descriptor, *args, **kwargs)
                source.write_text('package changed\n')
                return stream

            with patch.object(preparation.os, 'fdopen', side_effect=change):
                with self.assertRaisesRegex(PreparationError, 'changed while reading'):
                    assets.read('initialization/guard.go')

    def test_stable_asset_read_rejects_symlink_substitution_during_open(self):
        source = self.assets / 'initialization/guard.go'
        foreign = self.root / 'foreign-input.go'
        foreign.write_text('package foreign\n')
        with preparation._GuardedFiles(self.assets) as assets:
            real_open = preparation.os.open

            def replace(path, *args, **kwargs):
                if path == 'guard.go':
                    source.unlink()
                    source.symlink_to(foreign)
                return real_open(path, *args, **kwargs)

            with patch.object(preparation.os, 'open', side_effect=replace):
                with self.assertRaises(PreparationError):
                    assets.read('initialization/guard.go')
        self.assertEqual(foreign.read_text(), 'package foreign\n')

    def test_retained_inputs_changed_before_final_manifest_are_rejected(self):
        original_initializer = preparation._initializer

        def change(config_hash):
            (self.root / 'output/app/config.json').write_text('{"foreign":"config"}\n')
            return original_initializer(config_hash)

        with patch.object(preparation, '_initializer', side_effect=change):
            with self.assertRaisesRegex(PreparationError, 'input identity, type or size changed'):
                self.prepare()
        self.assertFalse((self.root / 'output').exists())

    def test_generated_sources_cannot_silently_overlap_library_assets(self):
        generated = {'initialization/guard.go': 'package generated\n'}
        receipts = {**self.receipts, 'generated_source_sha256': {
            name: hashlib.sha256(text.encode()).hexdigest() for name, text in generated.items()}}
        with patch.object(preparation, 'generate_seed_sources', return_value=generated), \
                patch.object(preparation, 'seed_source_receipts', return_value=receipts):
            with self.assertRaisesRegex(PreparationError, 'output paths overlap'):
                self.prepare()
        self.assertFalse((self.root / 'output').exists())


if __name__ == '__main__':
    unittest.main()
