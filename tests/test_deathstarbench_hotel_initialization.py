"""Synthetic offline metadata tests; no Go, database, image or cloud execution."""

import copy
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
import unittest

from app import deathstarbench_hotel_initialization as hotel


SOURCE = 'sha256:' + 'a' * 64


def fingerprint(value):
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
    return 'sha256:' + hashlib.sha256(encoded.encode('utf-8')).hexdigest()


def metadata_fixture():
    # These digest-shaped placeholders test metadata validation only. They do
    # not assert the existence, provenance or correct content of any dataset.
    result = {
        'schema_version': 1, 'candidate_schema': 'hotel-initialization-dataset-v1',
        'candidate_only': True,
        'contract_revision': 'hotel-separate-initialization-candidate-v1',
        'recipe_revision': 'hotel-reservation-guarded-init-native-go-scratch-candidate-v1',
        'dataset_revision': 'hotel-reservation-deterministic-baseline-candidate-v1',
        'source_fingerprint': SOURCE,
        'canonicalization': 'bson-canonical-extjson-recursive-sorted-v1',
        'deterministic_id': 'sha256-canonical-logical-key-v1',
        'database_count': 8, 'collection_count': 11, 'document_count': 873,
        'collections': [{
            'component': component, 'database': component + '-db', 'name': name,
            'count': count, 'logical_key_fields': list(keys),
            'content_sha256': 'sha256:' + 'b' * 64,
            'logical_keys_sha256': 'sha256:' + 'c' * 64,
        } for component, name, count, keys in hotel._COLLECTIONS],
        'scope': 'offline-dataset-construction-only-no-mongodb-execution',
        'initialization_executed': False, 'dataset_qualified': False,
        'runtime_qualified': False, 'measurement_qualified': False,
        'application_start_allowed': False, 'public_release': False,
    }
    return {**result, 'dataset_fingerprint': fingerprint(result)}


class HotelInitializationContractTests(unittest.TestCase):
    def test_static_contract_is_exact_and_does_not_invent_dataset_hashes(self):
        value = hotel.hotel_initialization_policy()
        self.assertEqual(value['database_count'], 8)
        self.assertEqual(value['collection_count'], 11)
        self.assertEqual(value['document_count'], 873)
        self.assertEqual(len(value['stores']), 8)
        collections = [item for store in value['stores'] for item in store['collections']]
        self.assertEqual(len(collections), 11)
        self.assertEqual(sum(item['expected_count'] for item in collections), 873)
        self.assertTrue(all(item['expected_count'] > 0 for item in collections))
        self.assertTrue(all(item is False for item in value['qualification'].values()))
        self.assertFalse(value['runtime_authority'])
        self.assertFalse(value['executor_implemented'])
        self.assertFalse(value['application_start_allowed'])
        self.assertFalse(value['public_ui_enabled'])
        self.assertNotIn('dataset_fingerprint', value)
        self.assertNotIn('source_fingerprint', value)
        self.assertEqual(value['contract_fingerprint'], fingerprint({key: item for key, item in value.items() if key != 'contract_fingerprint'}))

    def test_audited_key_fields_include_composite_and_non_hotel_ids(self):
        rows = {(component, name): (count, keys) for component, name, count, keys in hotel._COLLECTIONS}
        self.assertEqual(rows['profile', 'hotels'], (80, ('id',)))
        self.assertEqual(rows['user', 'user'], (501, ('username',)))
        self.assertEqual(rows['attractions', 'restaurants'], (6, ('restaurantId',)))
        self.assertEqual(rows['attractions', 'museums'], (6, ('museumId',)))
        self.assertEqual(rows['review', 'reviews'], (6, ('reviewId',)))
        self.assertEqual(rows['rate', 'inventory'][1], ('hotelId', 'code', 'inDate', 'outDate', 'roomType.code'))
        self.assertEqual(rows['reservation', 'reservation'][1], ('hotelId', 'customerName', 'inDate', 'outDate'))

    def test_immutable_contract_replace_cannot_cross_revision_or_collection_identity(self):
        contract = hotel.HotelInitializationContract()
        self.assertEqual(contract.sha256, contract.fingerprint.removeprefix('sha256:'))
        with self.assertRaises(FrozenInstanceError):
            contract.dataset_revision = 'old'
        for changes in (
            {'contract_revision': 'hotel-other'}, {'recipe_revision': 'old'},
            {'dataset_revision': 'old'}, {'collection_specs': ()},
            {'collection_specs': list(hotel._COLLECTIONS)},
            {'collection_specs': tuple(reversed(hotel._COLLECTIONS))},
        ):
            with self.subTest(changes=changes), self.assertRaises(hotel.HotelInitializationError):
                replace(contract, **changes)
        aliased = list(hotel._COLLECTIONS)
        component, name, _count, keys = aliased[8]
        aliased[8] = (component, name, True, keys)  # True == 1 must not pass.
        with self.assertRaises(hotel.HotelInitializationError):
            replace(contract, collection_specs=tuple(aliased))

    def test_policy_is_detached_and_strict_with_no_replay_or_authority(self):
        original = hotel.hotel_initialization_policy()
        copy_value = hotel.validate_hotel_initialization_policy(original)
        copy_value['stores'][0]['collections'][0]['expected_count'] = 0
        self.assertEqual(original, hotel.hotel_initialization_policy())
        policy = original['policy']
        self.assertEqual(policy['initialization'], 'explicit-init-only')
        self.assertEqual(policy['normal_start'], 'read-only-verify-then-connect')
        self.assertEqual(policy['incomplete_or_foreign_state'], 'cleanup-only')
        self.assertTrue(policy['controller_wide_prestart_verification_required'])
        self.assertFalse(any(policy[field] for field in ('replay_allowed', 'reset_allowed', 'upsert_allowed', 'postwarmup_epoch_qualified')))
        mutations = [
            lambda value: value.update(application_start_allowed=True),
            lambda value: value.update(extra='unknown'),
            lambda value: value['qualification'].update(dataset=True),
            lambda value: value['stores'][0]['collections'][0].update(expected_count=0),
            lambda value: value['policy'].update(replay_allowed=True),
        ]
        for mutate in mutations:
            value = copy.deepcopy(original)
            mutate(value)
            with self.assertRaises(hotel.HotelInitializationError):
                hotel.validate_hotel_initialization_policy(value)

    def test_valid_go_shaped_metadata_is_evidence_only_and_detached(self):
        value = metadata_fixture()
        result = hotel.validate_hotel_dataset_attestation(value, expected_source_fingerprint=SOURCE,
                                                         expected_dataset_fingerprint=value['dataset_fingerprint'])
        self.assertEqual(value, result)
        result['collections'][0]['count'] = 0
        self.assertEqual(value['collections'][0]['count'], 6)
        self.assertTrue(all(value[flag] is False for flag in hotel._FLAGS))

    def test_source_binding_is_mandatory_exact_nonzero_and_part_of_dataset_hash(self):
        value = metadata_fixture()
        for source in (None, '', 'a' * 64, 'sha256:' + '0' * 64, 'sha256:' + 'A' * 64,
                       'sha256:' + 'd' * 64):
            with self.subTest(source=source), self.assertRaises(hotel.HotelInitializationError):
                hotel.validate_hotel_dataset_attestation(value, expected_source_fingerprint=source)
        value['source_fingerprint'] = 'sha256:' + 'd' * 64
        with self.assertRaisesRegex(hotel.HotelInitializationError, 'metadata fingerprint'):
            hotel.validate_hotel_dataset_attestation(value, expected_source_fingerprint=value['source_fingerprint'])

    def test_unknown_missing_old_artifact_and_gate_promotion_are_rejected(self):
        mutations = [
            lambda value: value.update(extra='foreign'),
            lambda value: value.pop('source_fingerprint'),
            lambda value: value.update(recipe_revision='hotel-reservation-native-go-scratch-candidate-v1'),
            lambda value: value.update(dataset_revision='hotel-reservation-decimal-user-seed-candidate-v1'),
            lambda value: value.update(candidate_schema='candidate-runtime-preflight-v1'),
            lambda value: value.update(scope='runtime-qualified'),
            lambda value: value.update(schema_version=True),
            lambda value: value.update(database_count=8.0),
            lambda value: value.update(document_count=0),
            lambda value: value.update(candidate_only=False),
        ] + [lambda value, flag=flag: value.update({flag: True}) for flag in hotel._FLAGS]
        for number, mutate in enumerate(mutations):
            value = metadata_fixture()
            mutate(value)
            with self.subTest(case=number), self.assertRaises(hotel.HotelInitializationError):
                hotel.validate_hotel_dataset_attestation(value, expected_source_fingerprint=SOURCE)
        with self.assertRaises(hotel.HotelInitializationError):
            hotel.validate_hotel_dataset_attestation(object(), expected_source_fingerprint=SOURCE)

    def test_collection_set_order_counts_and_logical_keys_are_exact(self):
        mutations = [
            lambda rows: rows.pop(), lambda rows: rows.append(copy.deepcopy(rows[0])),
            lambda rows: rows.reverse(), lambda rows: rows.__setitem__(1, copy.deepcopy(rows[0])),
            lambda rows: rows[0].update(count=0), lambda rows: rows[8].update(count=True),
            lambda rows: rows[0].update(count=6.0), lambda rows: rows[0].update(database='foreign-db'),
            lambda rows: rows[0].update(component='social_network'),
            lambda rows: rows[0].update(logical_key_fields=['id']),
            lambda rows: rows[5]['logical_key_fields'].reverse(),
            lambda rows: rows[0].update(logical_key_fields=('hotelId',)),
            lambda rows: rows[0].update(other='unapproved'),
        ]
        for number, mutate in enumerate(mutations):
            value = metadata_fixture()
            mutate(value['collections'])
            with self.subTest(case=number), self.assertRaises(hotel.HotelInitializationError):
                hotel.validate_hotel_dataset_attestation(value, expected_source_fingerprint=SOURCE)

    def test_hash_format_metadata_integrity_and_external_fingerprint_are_exact(self):
        for field in ('content_sha256', 'logical_keys_sha256'):
            for digest in ('sha256:' + '0' * 64, 'b' * 64, 'sha256:' + 'B' * 64, 'sha256:' + 'b' * 63, None):
                value = metadata_fixture()
                value['collections'][0][field] = digest
                with self.subTest(field=field, digest=digest), self.assertRaises(hotel.HotelInitializationError):
                    hotel.validate_hotel_dataset_attestation(value, expected_source_fingerprint=SOURCE)
        value = metadata_fixture()
        value['collections'][0]['content_sha256'] = 'sha256:' + 'd' * 64
        with self.assertRaisesRegex(hotel.HotelInitializationError, 'metadata fingerprint'):
            hotel.validate_hotel_dataset_attestation(value, expected_source_fingerprint=SOURCE)
        value = metadata_fixture()
        with self.assertRaisesRegex(hotel.HotelInitializationError, 'externally pinned'):
            hotel.validate_hotel_dataset_attestation(value, expected_source_fingerprint=SOURCE,
                                                     expected_dataset_fingerprint='sha256:' + 'd' * 64)

    def test_canonicalization_has_no_newline_ascii_or_html_escaping(self):
        sample = {'z': '<&é>', 'a': {'b': 1, 'a': 2}}
        self.assertEqual(hotel._canonical(sample), '{"a":{"a":2,"b":1},"z":"<&é>"}')
        self.assertEqual(hotel._fingerprint(sample), fingerprint(sample))

    def test_parse_exact_bounded_utf8_json_and_reject_duplicates_and_junk(self):
        value = metadata_fixture()
        encoded = json.dumps(value).encode('utf-8')
        for text in (encoded, encoded.decode(), b'\n' + encoded + b'\n'):
            self.assertEqual(hotel.parse_hotel_dataset_attestation(text, expected_source_fingerprint=SOURCE), value)
        malformed = [
            b'', b'\xff', b'\xef\xbb\xbf' + encoded, encoded + b'{}', b'prefix\n' + encoded,
            encoded.replace(b'"schema_version": 1', b'"schema_version": 1, "schema_version": 1', 1),
            encoded.replace(b'"count": 6', b'"count": 6, "count": 6', 1),
            encoded.replace(b'"document_count": 873', b'"document_count": NaN', 1),
            b'{"count":' + b'9' * 5000 + b'}',
            b' ' * (hotel.MAX_ATTESTATION_BYTES + 1), [], None,
        ]
        for number, text in enumerate(malformed):
            with self.subTest(case=number), self.assertRaises(hotel.HotelInitializationError):
                hotel.parse_hotel_dataset_attestation(text, expected_source_fingerprint=SOURCE)


if __name__ == '__main__':
    unittest.main()
