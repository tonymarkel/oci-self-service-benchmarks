"""Portable synthetic extraction fixtures; production anchors stay unmocked in CI preparation.

These small fixtures test source boundaries, not the actual dataset contents or
cardinalities. Only their trusted SHA table is patched inside each unit test.
The required candidate preparation CI also invokes the generator on all eight
complete real, pinned prepared source files with production hashes unchanged.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import deathstarbench_hotel_seed_source as seed
from scripts import prepare_deathstarbench_hotel_images as legacy


PRODUCTION_ANCHORS = dict(seed.PREPARED_SOURCE_SHA256)
EXPECTED = (
    ('attractions', 'hotels', 'newPoints', 6),
    ('attractions', 'museums', 'newMuseums', 6),
    ('attractions', 'restaurants', 'newRestaurants', 6),
    ('geo', 'geo', 'newPoints', 80),
    ('profile', 'hotels', 'newProfiles', 80),
    ('rate', 'inventory', 'newRatePlans', 27),
    ('recommendation', 'recommendation', 'newHotels', 80),
    ('reservation', 'number', 'newNumbers', 80),
    ('reservation', 'reservation', 'newReservations', 1),
    ('review', 'reviews', 'newReviews', 6),
    ('user', 'user', 'newUsers', 501),
)


def fixture(component):
    declarations = 'type seedRow struct {\n\tValue float32 `bson:"value"`\n}\n'
    construction = ''.join(
        f'\t{rows} := []interface{{}}{{\n\t\t&seedRow{{37.7835 + float32(7)/500.0*3}},\n\t}}\n\n'
        for current, _name, rows, _count in EXPECTED if current == component)
    if component == 'user':
        declarations = (
            'type User struct { Name string; Password string }\n\n'
            'func seededUsername(suffix string) string {\n'
            '\treturn fmt.Sprintf("Cornell_%s", suffix)\n}\n'
        )
        construction = (
            '\tnewUsers := []interface{}{}\n'
            '\tfor i := 0; i <= 0; i++ {\n'
            '\t\tsuffix := strconv.Itoa(i)\n'
            '\t\tsum := sha256.Sum256([]byte(suffix))\n'
            '\t\tnewUsers = append(newUsers, User{seededUsername(suffix), fmt.Sprintf("%x", sum)})\n\t}\n'
        )
    if component in {'geo', 'profile', 'recommendation'}:
        kind = 'float32' if component == 'profile' else 'float64'
        construction += (
            '\tfor i := 7; i <= 80; i++ {\n'
            f'\t\tlat := 37.7835 + {kind}(i)/500.0*3\n'
            f'\t\tlon := -122.41 + {kind}(i)/500.0*4\n'
            '\t\t_ = lat\n\t\t_ = lon\n\t}\n'
        )
    generating = '' if component in {'attractions', 'review'} else seed._GENERATING
    if component == 'reservation':
        targets = (
            '\tdatabase := client.Database("reservation-db")\n'
            '\tnumCollection := database.Collection("number")\n'
            '\tresCollection := database.Collection("reservation")\n'
            '\t_, err = numCollection.InsertMany(context.TODO(), newNumbers)\n'
            '\t_, err = resCollection.InsertMany(context.TODO(), newReservations)\n'
        )
    else:
        names = {'hotels': 'collectionH', 'museums': 'collectionM', 'restaurants': 'collectionR'}
        targets = ''.join(
            f'\t{names[name] if component == "attractions" else "collection"} := client.Database("{component}-db").Collection("{name}")\n'
            f'\t_, err = {names[name] if component == "attractions" else "collection"}.InsertMany(context.TODO(), {rows})\n'
            for current, name, rows, _count in EXPECTED if current == component)
    return (seed._PREFIX + '\t"context"\n\t"fmt"\n)\n\n' + declarations + '\n'
            + seed._FUNCTION + generating + '\n' + construction + seed._URI + targets
            + '\treturn client, func() {}\n}\n')


class HotelSeedSourceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='hotel-pure-seed-source-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.app = self.root / 'app'
        self.trusted = {}
        for component in seed.COMMANDS:
            directory = self.app / 'cmd' / component
            directory.mkdir(parents=True)
            if component in seed.COMPONENTS:
                relative = f'cmd/{component}/db.go'
                raw = fixture(component).encode()
                (self.app / relative).write_bytes(raw)
                self.trusted[relative] = hashlib.sha256(raw).hexdigest()
        patch = mock.patch.object(seed, 'PREPARED_SOURCE_SHA256', self.trusted)
        patch.start()
        self.addCleanup(patch.stop)

    def update(self, component, text, *, trust=False):
        relative = f'cmd/{component}/db.go'
        raw = text.encode()
        (self.app / relative).write_bytes(raw)
        if trust:
            self.trusted[relative] = hashlib.sha256(raw).hexdigest()

    def test_production_anchors_are_exact_complete_sources_not_fixture_hashes(self):
        self.assertEqual(len(PRODUCTION_ANCHORS), 8)
        for path, digest in PRODUCTION_ANCHORS.items():
            self.assertRegex(digest, r'^[0-9a-f]{64}$')
            if path != 'cmd/user/db.go':
                self.assertEqual(digest, legacy.UPSTREAM_ANCHORS['hotelReservation/' + path])
        self.assertEqual(PRODUCTION_ANCHORS['cmd/user/db.go'],
                         'eb0244f1390b9277d2f2ebfb83f140f638cb8d431505c8faac4d43ef62ffa4a7')
        self.assertNotEqual(PRODUCTION_ANCHORS['cmd/user/db.go'], legacy.UPSTREAM_ANCHORS['hotelReservation/cmd/user/db.go'])
        with mock.patch.object(seed, 'PREPARED_SOURCE_SHA256', PRODUCTION_ANCHORS):
            with self.assertRaisesRegex(seed.PreparationError, 'SHA-256 drifted'):
                seed.generate_seed_sources(self.app)

    def test_nine_deterministic_relative_outputs_preserve_typed_construction_and_decimal_user(self):
        first = seed.generate_seed_sources(self.app)
        self.assertEqual(first, seed.generate_seed_sources(self.app))
        self.assertEqual(list(first), sorted(first))
        self.assertEqual(set(first), {'initialization/catalog/catalog.go'} | {
            f'initialization/seeddata/{component}/seed.go' for component in seed.COMPONENTS})
        for relative, source in first.items():
            self.assertFalse(Path(relative).is_absolute())
            self.assertNotIn('..', Path(relative).parts)
            self.assertNotIn('mongodb://', source)
            self.assertNotIn('InsertMany(', source)
            self.assertNotIn('log.Info', source)
            self.assertNotIn('mongo.Connect', source)
        geo = first['initialization/seeddata/geo/seed.go']
        self.assertIn('Value float32 `bson:"value"`', geo)
        self.assertIn('&seedRow{37.7835 + float32(7)/500.0*3}', geo)
        user = first['initialization/seeddata/user/seed.go']
        self.assertIn('return fmt.Sprintf("Cornell_%s", suffix)', user)
        self.assertIn('sha256.Sum256([]byte(suffix))', user)
        self.assertIn('fmt.Sprintf("%x", sum)', user)
        self.assertNotIn('Cornell_%x', user)

    def test_exact_eleven_target_rows_and_expected_counts_are_source_metadata_not_content_claims(self):
        generated = seed.generate_seed_sources(self.app)
        receipts = seed.seed_source_receipts(self.app)
        entries = receipts['source_fingerprint_input']['collections']
        observed = [(item['component'], item['name'], item['documents_variable'], item['expected_documents']) for item in entries]
        self.assertEqual(observed, list(EXPECTED))
        self.assertEqual(sum(item['expected_documents'] for item in entries), 873)
        self.assertEqual(len(entries), 11)
        for component, name, rows, _count in EXPECTED:
            source = generated[f'initialization/seeddata/{component}/seed.go']
            self.assertIn(f'{{Component: "{component}", Database: "{component}-db", Name: "{name}", Documents: {rows}}}', source)
        self.assertIs(receipts['candidate_only'], True)
        self.assertIs(receipts['released'], False)
        self.assertIs(receipts['qualification_authority'], False)
        self.assertNotIn('dataset_fingerprint', receipts)
        self.assertNotIn('dataset_ready', receipts)

    def test_source_fingerprint_is_non_circular_and_catalog_exports_expected_api_shape(self):
        generated = seed.generate_seed_sources(self.app)
        receipts = seed.seed_source_receipts(self.app)
        inputs = receipts['source_fingerprint_input']
        raw = (json.dumps(inputs, sort_keys=True, separators=(',', ':')) + '\n').encode()
        fingerprint = 'sha256:' + hashlib.sha256(raw).hexdigest()
        self.assertEqual(receipts['source_fingerprint'], fingerprint)
        self.assertNotIn('generated_source_sha256', inputs)
        catalog = generated['initialization/catalog/catalog.go']
        self.assertIn(f'const SourceFingerprint = "{fingerprint}"', catalog)
        self.assertIn('func Collections() []initialization.Collection', catalog)
        self.assertIn('make([]initialization.Collection, 0, 11)', catalog)
        positions = [catalog.index(f'collections = append(collections, {component}.Collections()...)') for component in seed.COMPONENTS]
        self.assertEqual(positions, sorted(positions))
        self.assertEqual(receipts['generated_source_sha256'], {
            relative: hashlib.sha256(source.encode()).hexdigest() for relative, source in generated.items()})
        self.assertEqual(inputs['source_files'], self.trusted)
        self.assertEqual(inputs['arithmetic_policy'], seed.arithmetic_policy())
        self.assertEqual(receipts['arithmetic_policy'], inputs['arithmetic_policy'])
        self.assertEqual(inputs['generator_revision'], 'hotel-typed-seed-source-candidate-v2')

    def test_exact_six_typed_rounding_barriers_are_bound_without_comparison_rounding(self):
        sources = seed.generate_seed_sources(self.app)
        receipts = seed.seed_source_receipts(self.app)
        policy = seed.arithmetic_policy()
        self.assertEqual(policy['revision'], 'hotel-seed-explicit-ieee754-operation-rounding-v1')
        self.assertEqual(policy['rule'], 'explicit-target-type-conversions-after-division-product-and-sum')
        self.assertEqual(len(policy['expressions']), 6)
        self.assertEqual([(item['component'], item['variable'], item['float_type']) for item in policy['expressions']], [
            ('geo', 'lat', 'float64'), ('geo', 'lon', 'float64'),
            ('profile', 'lat', 'float32'), ('profile', 'lon', 'float32'),
            ('recommendation', 'lat', 'float64'), ('recommendation', 'lon', 'float64'),
        ])
        for item in policy['expressions']:
            source = sources[f'initialization/seeddata/{item["component"]}/seed.go']
            kind = item['float_type']
            base, scale = ('37.7835', '3') if item['variable'] == 'lat' else ('-122.41', '4')
            expected = f'{kind}({kind}({base}) + {kind}({kind}({kind}(i) / {kind}(500.0)) * {kind}({scale})))'
            self.assertEqual(item['replacement'], expected)
            self.assertEqual(source.count(f'\t\t{item["variable"]} := {expected}'), 1)
            self.assertNotIn(f'{item["variable"]} := {item["original"]}', source)
            self.assertIn(f'{item["variable"]} := {item["original"]}', (self.app / f'cmd/{item["component"]}/db.go').read_text())
        for path, receipt in receipts['source_files'].items():
            changed = path.split('/')[1] in {'geo', 'profile', 'recommendation'}
            self.assertEqual(receipt['original_construction_sha256'] != receipt['generated_construction_sha256'], changed)
        # Original typed row literals and non-coordinate arithmetic stay exact;
        # no row re-encoding, decimal quantization or EJSON tolerance is added.
        self.assertIn('&seedRow{37.7835 + float32(7)/500.0*3}', sources['initialization/seeddata/geo/seed.go'])
        self.assertNotIn('math.Round', '\n'.join(sources.values()))

    def test_arithmetic_policy_is_detached_and_semantic_revision_changes_source_binding(self):
        initial = seed.seed_source_receipts(self.app)
        policy = seed.arithmetic_policy()
        policy['expressions'][0]['float_type'] = 'wrong'
        self.assertEqual(seed.arithmetic_policy()['expressions'][0]['float_type'], 'float64')
        with mock.patch.object(seed, 'ARITHMETIC_POLICY_REVISION', 'changed-test-only-arithmetic-policy'):
            changed = seed.seed_source_receipts(self.app)
        self.assertNotEqual(initial['source_fingerprint'], changed['source_fingerprint'])
        self.assertEqual(initial['source_fingerprint_input']['source_files'], changed['source_fingerprint_input']['source_files'])

    def test_missing_duplicate_wrong_type_or_extra_coordinate_assignments_refuse(self):
        original = fixture('geo')
        lat = '\t\tlat := 37.7835 + float64(i)/500.0*3\n'
        lon = '\t\tlon := -122.41 + float64(i)/500.0*4\n'
        mutations = (
            original.replace(lat, ''),
            original.replace(lat, lat + lat),
            original.replace('lat := 37.7835 + float64(i)', 'lat := 37.7835 + float32(i)'),
            original.replace('float64(i)/500.0*3', 'float64(i)/500.0*2'),
            original.replace(lat + lon, lon + lat),
            original.replace(lat, lat + '\t\tlat += 1\n'),
            original.replace(lat, lat + '\t\tlon = 4\n'),
        )
        for text in mutations:
            with self.subTest(mutation=text[-100:]):
                self.update('geo', text, trust=True)
                with self.assertRaisesRegex(seed.PreparationError, 'arithmetic inventory/type drifted'):
                    seed.generate_seed_sources(self.app)
        self.update('geo', original, trust=True)
        unknown = fixture('review').replace(seed._URI, '\t\tlat := 37.0\n' + seed._URI)
        self.update('review', unknown, trust=True)
        with self.assertRaisesRegex(seed.PreparationError, 'arithmetic inventory/type drifted'):
            seed.generate_seed_sources(self.app)

    def test_only_explicit_required_imports_are_retained(self):
        generated = seed.generate_seed_sources(self.app)
        expected = {'attractions': (), 'review': (), 'geo': ('strconv',), 'recommendation': ('strconv',),
                    'reservation': ('strconv',), 'profile': ('fmt', 'strconv'), 'rate': ('fmt', 'strconv'),
                    'user': ('crypto/sha256', 'fmt', 'strconv')}
        for component, imports in expected.items():
            source = generated[f'initialization/seeddata/{component}/seed.go']
            observed = re.findall(r'^\t"([^"]+)"$', source, flags=re.M)
            self.assertEqual(observed, [*imports, seed.MODULE + '/initialization'])

    def test_generator_never_writes_or_executes_and_caller_mutation_is_not_retained(self):
        before = {p.relative_to(self.app).as_posix(): p.read_bytes() for p in self.app.rglob('*') if p.is_file()}
        original_open = seed.os.open
        def read_only(path, flags, *args, **kwargs):
            self.assertFalse(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC))
            return original_open(path, flags, *args, **kwargs)
        with mock.patch.object(seed.os, 'open', side_effect=read_only), \
             mock.patch.object(subprocess, 'run', side_effect=AssertionError('No Go or other execution')):
            generated = seed.generate_seed_sources(self.app)
            receipts = seed.seed_source_receipts(self.app)
        after = {p.relative_to(self.app).as_posix(): p.read_bytes() for p in self.app.rglob('*') if p.is_file()}
        self.assertEqual(before, after)
        generated.clear()
        receipts['source_fingerprint_input']['collections'][0]['expected_documents'] = 999
        self.assertEqual(sum(item['expected_documents'] for item in seed.seed_source_receipts(self.app)['source_fingerprint_input']['collections']), 873)

    def test_complete_file_hash_rejects_drift_even_outside_extracted_prefix(self):
        for component in seed.COMPONENTS:
            with self.subTest(component=component):
                self.update(component, fixture(component) + '// post-body drift\n')
                with self.assertRaisesRegex(seed.PreparationError, 'SHA-256 drifted'):
                    seed.generate_seed_sources(self.app)
                self.update(component, fixture(component))

    def test_extraction_anchors_refuse_truncation_duplicates_and_effects_even_with_test_only_trusted_hash(self):
        original = fixture('geo')
        mutations = (
            original[:original.index(seed._URI)],
            original.replace(seed._URI, seed._URI + seed._URI),
            original.replace('package main', 'package other'),
            original.replace(seed._FUNCTION, 'func initializeDatabase(url string) {\n'),
            original.replace(seed._GENERATING, ''),
            original.replace('newPoints := []interface{}{', 'otherRows := []interface{}{'),
            original.replace('type seedRow', 'var seedRow'),
            original.replace('"geo-db"', '"unknown-db"'),
            original.replace('context.TODO(), newPoints', 'context.TODO(), otherRows'),
            original.replace('\tnewPoints :=', '\tlog.Fatal().Msg("effect")\n\tnewPoints :='),
        )
        for text in mutations:
            with self.subTest(mutation=text[:60]):
                self.update('geo', text, trust=True)
                with self.assertRaises(seed.PreparationError):
                    seed.generate_seed_sources(self.app)
        self.update('geo', original, trust=True)

    def test_unpatched_or_changed_decimal_username_helper_is_refused(self):
        for text in (fixture('user').replace('Cornell_%s', 'Cornell_%x'),
                     fixture('user').replace('seededUsername(suffix)', 'fmt.Sprint(suffix)')):
            self.update('user', text, trust=True)
            with self.assertRaisesRegex(seed.PreparationError, 'decimal user seed helper'):
                seed.generate_seed_sources(self.app)

    def test_unknown_missing_commands_and_unexpected_seeder_fail_closed(self):
        unknown = self.app / 'cmd/unknown'
        unknown.mkdir()
        with self.assertRaisesRegex(seed.PreparationError, 'inventory'):
            seed.generate_seed_sources(self.app)
        unknown.rmdir()
        extra = self.app / 'cmd/frontend/db.go'
        extra.write_text('unknown seeder\n')
        with self.assertRaisesRegex(seed.PreparationError, 'Unexpected Hotel seeder'):
            seed.generate_seed_sources(self.app)
        extra.unlink()
        (self.app / 'cmd/search').rmdir()
        with self.assertRaisesRegex(seed.PreparationError, 'inventory'):
            seed.generate_seed_sources(self.app)

    def test_leaf_directory_root_links_hardlinks_and_special_files_are_refused(self):
        leaf = self.app / 'cmd/geo/db.go'
        real = self.root / 'saved-db.go'
        leaf.rename(real)
        leaf.symlink_to(real)
        with self.assertRaises(seed.PreparationError):
            seed.generate_seed_sources(self.app)
        leaf.unlink()
        os.link(real, leaf)
        with self.assertRaises(seed.PreparationError):
            seed.generate_seed_sources(self.app)
        leaf.unlink()
        os.mkfifo(leaf)
        with self.assertRaises(seed.PreparationError):
            seed.generate_seed_sources(self.app)
        leaf.unlink()
        real.rename(leaf)
        component = self.app / 'cmd/geo'
        saved = self.root / 'saved-geo'
        component.rename(saved)
        component.symlink_to(saved, target_is_directory=True)
        with self.assertRaises(seed.PreparationError):
            seed.generate_seed_sources(self.app)
        root_link = self.root / 'app-link'
        root_link.symlink_to(self.app, target_is_directory=True)
        with self.assertRaises(seed.PreparationError):
            seed.generate_seed_sources(root_link)

    def test_missing_oversized_and_traversal_inputs_are_refused(self):
        leaf = self.app / 'cmd/geo/db.go'
        leaf.write_bytes(b'x' * (seed.MAX_SOURCE_BYTES + 1))
        with self.assertRaises(seed.PreparationError):
            seed.generate_seed_sources(self.app)
        leaf.unlink()
        with self.assertRaises(seed.PreparationError):
            seed.generate_seed_sources(self.app)
        with self.assertRaisesRegex(seed.PreparationError, 'traversal'):
            seed.generate_seed_sources(self.app / '../app')

    def test_renamed_leaf_during_open_is_not_adopted_as_stable_source(self):
        leaf = self.app / 'cmd/geo/db.go'
        saved = self.root / 'opened-original.go'
        original_open = seed.os.open
        def replace_after_open(path, flags, *args, **kwargs):
            descriptor = original_open(path, flags, *args, **kwargs)
            if Path(path) == leaf:
                leaf.rename(saved)
                leaf.write_text(fixture('geo'))
            return descriptor
        with mock.patch.object(seed.os, 'open', side_effect=replace_after_open):
            with self.assertRaisesRegex(seed.PreparationError, 'changed while reading'):
                seed.generate_seed_sources(self.app)


if __name__ == '__main__':
    unittest.main()
