"""Offline Hotel seed policy and Go-generated dataset evidence, not authority.

The static contract deliberately has no dataset-content digest. Only execution
of the new source-bound Go catalog can produce that evidence. These validators
check its structure and internal fingerprints; they do not execute or authorize
MongoDB writes, image pulls, applications, Kubernetes, or public dispatch. No
runtime plan or old CandidateArtifactBinding promotion API exists here.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any


CONTRACT_REVISION = 'hotel-separate-initialization-candidate-v1'
RECIPE_REVISION = 'hotel-reservation-guarded-init-native-go-scratch-candidate-v1'
DATASET_REVISION = 'hotel-reservation-deterministic-rounded-baseline-candidate-v1'
DATASET_SCHEMA = 'hotel-initialization-dataset-v1'
CANONICALIZATION = 'bson-canonical-extjson-recursive-sorted-v1'
DETERMINISTIC_ID = 'sha256-canonical-logical-key-v1'
DATASET_SCOPE = 'offline-dataset-construction-only-no-mongodb-execution'
MAX_ATTESTATION_BYTES = 1024 * 1024
_DIGEST = re.compile(r'sha256:[0-9a-f]{64}\Z')

# Audited BSON field names from the eight cmd/*/db.go startup generators.
# These are collection/logical-key specifications, NOT hashes of their data.
_COLLECTIONS = (
    ('attractions', 'hotels', 6, ('hotelId',)),
    ('attractions', 'museums', 6, ('museumId',)),
    ('attractions', 'restaurants', 6, ('restaurantId',)),
    ('geo', 'geo', 80, ('hotelId',)),
    ('profile', 'hotels', 80, ('id',)),
    ('rate', 'inventory', 27, ('hotelId', 'code', 'inDate', 'outDate', 'roomType.code')),
    ('recommendation', 'recommendation', 80, ('hotelId',)),
    ('reservation', 'number', 80, ('hotelId',)),
    ('reservation', 'reservation', 1, ('hotelId', 'customerName', 'inDate', 'outDate')),
    ('review', 'reviews', 6, ('reviewId',)),
    ('user', 'user', 501, ('username',)),
)
COMPONENTS = tuple(sorted({item[0] for item in _COLLECTIONS}))
_FLAGS = (
    'initialization_executed', 'dataset_qualified', 'runtime_qualified',
    'measurement_qualified', 'application_start_allowed', 'public_release',
)


class HotelInitializationError(ValueError):
    """Hotel initialization metadata is crossed, malformed, or overclaims."""


def _require(condition, message):
    if not condition:
        raise HotelInitializationError(message)


def _canonical(value: Any) -> str:
    # Match Go Metadata exactly: recursive key sorting, UTF-8, no HTML/ASCII
    # escaping, no whitespace and no trailing newline. Metadata has no floats.
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False,
                          separators=(',', ':'), allow_nan=False)
    except (TypeError, ValueError, RecursionError) as exc:
        raise HotelInitializationError('Hotel evidence must be finite JSON.') from exc


def _fingerprint(value) -> str:
    try:
        return 'sha256:' + hashlib.sha256(_canonical(value).encode('utf-8')).hexdigest()
    except UnicodeError as exc:
        raise HotelInitializationError('Hotel evidence must be valid UTF-8.') from exc


def _digest(value, label):
    _require(isinstance(value, str) and _DIGEST.fullmatch(value)
             and value != 'sha256:' + '0' * 64, f'{label} requires a nonzero canonical SHA-256.')
    return value


def _fields(value, keys, label):
    _require(isinstance(value, Mapping) and set(value) == set(keys), f'{label} schema differs.')


def _closed_gates():
    return {key: False for key in (
        'anonymous_layer_pull', 'cloud_runtime', 'dataset', 'measurement',
        'denied_edges', 'cleanup', 'public_release',
    )}


@dataclass(frozen=True, slots=True)
class HotelInitializationContract:
    """Immutable source-preparation policy; never a runtime capability."""

    contract_revision: str = CONTRACT_REVISION
    recipe_revision: str = RECIPE_REVISION
    dataset_revision: str = DATASET_REVISION
    collection_specs: tuple = _COLLECTIONS

    def __post_init__(self):
        _require((self.contract_revision, self.recipe_revision, self.dataset_revision)
                 == (CONTRACT_REVISION, RECIPE_REVISION, DATASET_REVISION), 'Hotel contract revisions differ.')
        _require(type(self.collection_specs) is tuple and self.collection_specs == _COLLECTIONS,
                 'Hotel collection specifications differ from the audited baseline.')
        _require(len(self.collection_specs) == 11 and len(COMPONENTS) == 8
                 and all(type(row[2]) is int and row[2] > 0 for row in self.collection_specs)
                 and sum(row[2] for row in self.collection_specs) == 873,
                 'Hotel baseline must have eight stores, eleven nonempty collections and 873 documents.')

    def _identity(self):
        self.__post_init__()
        stores = [{
            'component': component, 'mongodb_component': 'mongodb-' + component,
            'database': component + '-db',
            'collections': [{'name': name, 'expected_count': count, 'logical_key_fields': list(keys)}
                            for owner, name, count, keys in self.collection_specs if owner == component],
        } for component in COMPONENTS]
        return {
            'schema_version': 1, 'candidate_schema': 'hotel-initialization-source-contract-v1',
            'candidate_only': True, 'released': False, 'workload_id': 'hotel_reservation',
            'scope': 'offline-source-preparation-only-no-runtime-authority',
            'contract_revision': self.contract_revision, 'recipe_revision': self.recipe_revision,
            'dataset_revision': self.dataset_revision, 'database_count': 8,
            'collection_count': 11, 'document_count': 873, 'stores': stores,
            'policy': {
                'initialization': 'explicit-init-only', 'normal_start': 'read-only-verify-then-connect',
                'claims': 'per-store-atomic-create-before-any-write',
                'incomplete_or_foreign_state': 'cleanup-only', 'replay_allowed': False,
                'reset_allowed': False, 'upsert_allowed': False,
                'controller_wide_prestart_verification_required': True,
                'postwarmup_epoch_qualified': False,
            },
            'runtime_authority': False, 'executor_implemented': False,
            'application_start_allowed': False, 'public_ui_enabled': False,
            'qualification': _closed_gates(),
            'future_execution_requires': [
                'complete-new-recipe-and-source-preparation-manifest',
                'actual-go-produced-source-bound-dataset-fingerprint',
                'new-immutable-image-publication-and-fresh-access-evidence',
                'separate-run-cluster-storage-and-initialization-authority',
            ],
        }

    @property
    def fingerprint(self):
        return _fingerprint(self._identity())

    @property
    def sha256(self):
        return self.fingerprint.removeprefix('sha256:')

    def metadata(self):
        return {**self._identity(), 'contract_fingerprint': self.fingerprint}


def hotel_initialization_policy() -> dict:
    """Return a detached static policy; no content fingerprint is invented."""
    return HotelInitializationContract().metadata()


def validate_hotel_initialization_policy(value) -> dict:
    expected = hotel_initialization_policy()
    _fields(value, expected, 'Hotel source policy')
    _require(_canonical(value) == _canonical(expected), 'Hotel source policy or closed gates differ.')
    return json.loads(_canonical(expected))


def validate_hotel_dataset_attestation(value, *, expected_source_fingerprint: str,
                                       expected_dataset_fingerprint: str | None = None) -> dict:
    """Check Go catalog evidence, not raw data, native execution or provenance.

    Collection content hashes cannot be recomputed without the typed documents.
    A future runtime boundary must separately verify the complete new recipe,
    source preparation and actual dataset evidence; this API grants no ability
    to promote an existing CandidateArtifactBinding or run a database command.
    """
    source = _digest(expected_source_fingerprint, 'Expected compiled source fingerprint')
    if expected_dataset_fingerprint is not None:
        _digest(expected_dataset_fingerprint, 'Expected dataset fingerprint')
    expected = {
        'schema_version': 1, 'candidate_schema': DATASET_SCHEMA, 'candidate_only': True,
        'contract_revision': CONTRACT_REVISION, 'recipe_revision': RECIPE_REVISION,
        'dataset_revision': DATASET_REVISION, 'canonicalization': CANONICALIZATION,
        'deterministic_id': DETERMINISTIC_ID, 'database_count': 8,
        'collection_count': 11, 'document_count': 873, 'scope': DATASET_SCOPE,
        **{flag: False for flag in _FLAGS},
    }
    _fields(value, {*expected, 'source_fingerprint', 'dataset_fingerprint', 'collections'}, 'Hotel dataset evidence')
    _require(all(_canonical(value[key]) == _canonical(item) for key, item in expected.items()),
             'Hotel dataset revisions, counts, evidence scope or closed gates differ.')
    _require(_digest(value['source_fingerprint'], 'Compiled source fingerprint') == source,
             'Hotel dataset compiled source binding differs.')
    rows = value['collections']
    _require(isinstance(rows, list) and len(rows) == 11, 'Hotel dataset must attest exactly eleven collections.')
    for row, (component, name, count, keys) in zip(rows, _COLLECTIONS):
        _fields(row, {'component', 'database', 'name', 'count', 'logical_key_fields',
                      'content_sha256', 'logical_keys_sha256'}, 'Hotel collection evidence')
        _require(row['component'] == component and row['database'] == component + '-db'
                 and row['name'] == name and type(row['count']) is int and row['count'] == count
                 and isinstance(row['logical_key_fields'], list) and row['logical_key_fields'] == list(keys),
                 'Hotel collection identity, order, positive count or logical keys differ.')
        _digest(row['content_sha256'], 'Collection content hash')
        _digest(row['logical_keys_sha256'], 'Collection logical-key hash')
    observed = _digest(value['dataset_fingerprint'], 'Dataset fingerprint')
    identity = {key: item for key, item in value.items() if key != 'dataset_fingerprint'}
    _require(observed == _fingerprint(identity), 'Hotel dataset metadata fingerprint differs.')
    _require(expected_dataset_fingerprint is None or observed == expected_dataset_fingerprint,
             'Hotel dataset differs from the externally pinned fingerprint.')
    return json.loads(_canonical(value))


def parse_hotel_dataset_attestation(output: str | bytes, *, expected_source_fingerprint: str,
                                    expected_dataset_fingerprint: str | None = None) -> dict:
    """Parse one bounded duplicate-free UTF-8 Go JSON result, with no wrapper."""
    def pairs(items):
        result = {}
        for key, item in items:
            _require(key not in result, 'Hotel dataset JSON contains a duplicate field.')
            result[key] = item
        return result
    try:
        raw = output.encode('utf-8') if isinstance(output, str) else output
        _require(isinstance(raw, bytes) and 0 < len(raw) <= MAX_ATTESTATION_BYTES,
                 'Hotel dataset JSON must be bounded UTF-8 bytes.')
        _require(not raw.startswith(b'\xef\xbb\xbf'), 'Hotel dataset JSON cannot contain a BOM.')
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                           parse_constant=lambda _value: (_ for _ in ()).throw(HotelInitializationError('Non-finite Hotel JSON.')))
    except HotelInitializationError:
        raise
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise HotelInitializationError('Hotel dataset JSON is invalid.') from exc
    return validate_hotel_dataset_attestation(value, expected_source_fingerprint=expected_source_fingerprint,
                                               expected_dataset_fingerprint=expected_dataset_fingerprint)
