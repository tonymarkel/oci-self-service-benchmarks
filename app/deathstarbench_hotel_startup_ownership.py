"""Pure, bounded startup ownership integrity and detached diagnostic projection.

The provider adapter MUST validate its finite native graph/create schemas and
fresh exact-target relationships before supplying a projection. Neither this
module's unkeyed digests nor caller-supplied dictionaries are cloud authority.
File custody, atomic checkpointing and exclusive seal publication belong to the
private startup store; strict SDK preflight/deletion belongs to its adapter.
"""
from __future__ import annotations

from collections.abc import Mapping
import json
import re

from . import deathstarbench_candidate_operator as support
from . import deathstarbench_hotel_initialization_operator as initialization
from .deathstarbench_candidate_runtime import _canonical, _fingerprint

CHECKPOINT_KEY = 'hotel_startup_owned_graph'
CHECKPOINT_SCHEMA = 'hotel-startup-owned-graph-v1'
OWNERSHIP_SCHEMA = 'hotel-startup-cleanup-ownership-v1'
TARGET_SCHEMA = 'hotel-startup-owned-target-v1'
ATTEMPT_SCHEMA = 'hotel-startup-create-attempt-v1'
NOT_REQUIRED_KEY = 'hotel_startup_cleanup_not_required'
NOT_REQUIRED_SCHEMA = 'hotel-startup-cleanup-not-required-v1'
_MAX_BYTES = 4 * 1024 * 1024
_DIGEST = re.compile(r'sha256:[0-9a-f]{64}\Z')
_IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9:/._-]{0,2047}\Z|/[A-Za-z0-9:/._-]{1,2047}\Z')
_KEY = re.compile(r'[a-z][a-z0-9_-]{0,127}\Z')
_SCOPE_KEYS = frozenset({'account_id', 'region', 'subscription_id', 'tenant_id', 'project_id',
                         'project_number', 'compute_project_id', 'compartment_id', 'availability_domain', 'zone'})
_ROOTS = frozenset({'run_id', 'provider', 'plan_sha256', 'support_pin_fingerprint',
                   'initialization_pin_fingerprint', 'startup_pin_fingerprint'})
_PROJECTION = frozenset({'provider_scope', 'create_contract', 'resources', 'known_identities',
                        'absence_targets', 'create_attempts', 'unresolved_creates', 'audits'})


class HotelStartupOwnershipError(ValueError):
    """Missing or conflicting original ownership refuses cleanup."""


def _require(condition, message='Startup ownership identity or schema differs.'):
    if not condition:
        raise HotelStartupOwnershipError(message)


def _copy(value):
    try:
        encoded = _canonical(value)
        _require(len(encoded.encode()) <= _MAX_BYTES, 'Startup ownership exceeds its byte bound.')
        return json.loads(encoded)
    except (TypeError, ValueError, RecursionError):
        raise HotelStartupOwnershipError('Startup ownership requires bounded finite JSON.') from None


def _digest(value):
    return type(value) is str and _DIGEST.fullmatch(value) is not None and not value.endswith('0' * 64)


def _identifier(value):
    return type(value) is str and _IDENTIFIER.fullmatch(value) is not None


def _scope(value, *, empty=False):
    _require(type(value) is dict and set(value) <= _SCOPE_KEYS and (empty or bool(value)))
    _require(all(_identifier(item) for item in value.values()))


def _targets(value, *, absence=False):
    _require(type(value) is dict and len(value) <= 512)
    for key, row in value.items():
        _require(type(key) is str and _KEY.fullmatch(key) is not None)
        _require(type(row) is dict and set(row) == {'candidate_schema', 'kind', 'id', 'name', 'scope', 'parent_id',
                                                   'self_link', 'incarnation_id'})
        _require(row['candidate_schema'] == TARGET_SCHEMA
                 and type(row['kind']) is str and _KEY.fullmatch(row['kind']) is not None
                 and (_identifier(row['id']) or (absence and row['id'] is None))
                 and _identifier(row['name'])
                 and (row['parent_id'] is None or _identifier(row['parent_id']))
                 and (row['self_link'] is None or _identifier(row['self_link']))
                 and (row['incarnation_id'] is None or (type(row['incarnation_id']) is str
                      and initialization._UUID.fullmatch(row['incarnation_id']) is not None
                      and row['incarnation_id'] != '00000000-0000-0000-0000-000000000000')))
        _scope(row['scope'])


def _audit(value, run_id, provider):
    _require(type(value) is dict and set(value) == {
        'schema_version', 'run_id', 'provider', 'cloud_identity', 'cloud_identity_sha256', 'known_identities'})
    _require(type(value['schema_version']) is int and value['schema_version'] == 1
             and value['run_id'] == run_id and value['provider'] == provider
             and type(value['cloud_identity']) is dict and bool(value['cloud_identity'])
             and _digest(value['cloud_identity_sha256'])
             and value['cloud_identity_sha256'] == _fingerprint(value['cloud_identity']))
    _targets(value['known_identities'])


def _attempts(value):
    _require(type(value) is dict and len(value) <= 512)
    for key, row in value.items():
        _require(type(key) is str and _KEY.fullmatch(key) is not None
                 and type(row) is dict and set(row) == {'candidate_schema', 'kind', 'name', 'scope', 'request_id'})
        _require(row['candidate_schema'] == ATTEMPT_SCHEMA
                 and type(row['kind']) is str and _KEY.fullmatch(row['kind']) is not None
                 and _identifier(row['name']) and (row['request_id'] is None or (
                     type(row['request_id']) is str and initialization._UUID.fullmatch(row['request_id']) is not None
                     and row['request_id'] != '00000000-0000-0000-0000-000000000000')))
        _scope(row['scope'])


def _projection(value, *, run_id, provider, recorded):
    _require(type(value) is dict and set(value) == _PROJECTION)
    _scope(value['provider_scope'], empty=not recorded)
    _require(type(value['resources']) is dict
             and type(value['known_identities']) is dict
             and type(value['absence_targets']) is dict
             and type(value['unresolved_creates']) is list and len(value['unresolved_creates']) <= 512
             and all(type(key) is str and _KEY.fullmatch(key) for key in value['unresolved_creates'])
             and len(set(value['unresolved_creates'])) == len(value['unresolved_creates'])
             and type(value['audits']) is list and len(value['audits']) <= 64)
    _targets(value['known_identities'])
    _targets(value['absence_targets'], absence=True)
    _attempts(value['create_attempts'])
    for key in ('known_identities', 'absence_targets', 'create_attempts'):
        for row in value[key].values():
            _require(all(row['scope'].get(name) == item for name, item in value['provider_scope'].items()),
                     'A typed target or attempt crossed its independent provider scope.')
    for audit in value['audits']:
        _audit(audit, run_id, provider)
    if recorded:
        _require(type(value['create_contract']) is dict and bool(value['create_contract']))
    else:
        _require(value['create_contract'] is None and not any(value[key] for key in _PROJECTION - {'create_contract'}))
    _copy(value)  # Native provider graph semantics are deliberately NOT inferred here.


def _roots(job, support_pin, initialization_pin, startup_pin):
    from .deathstarbench_hotel_startup_operator import PIN_KEY, validate_hotel_startup_pin
    provider = support._validate_pin(job, support_pin, None, cleanup_only=True)
    initialization.validate_hotel_initialization_pin(initialization_pin, support_pin, cleanup_only=True)
    validate_hotel_startup_pin(startup_pin, support_pin, initialization_pin)
    _require(_canonical(job.get(initialization.PIN_KEY)) == _canonical(initialization_pin)
             and _canonical(job.get(PIN_KEY)) == _canonical(startup_pin))
    return {'run_id': job['id'], 'provider': provider, 'plan_sha256': _fingerprint(job['plan']),
            'support_pin_fingerprint': support_pin['fingerprint'],
            'initialization_pin_fingerprint': initialization_pin['fingerprint'],
            'startup_pin_fingerprint': startup_pin['fingerprint']}


def _validate(value, run_id, provider, *, seal):
    value = _copy(value)
    expected = _ROOTS | _PROJECTION | {'schema_version', 'candidate_schema', 'fingerprint'}
    if not seal:
        expected |= {'contract_recorded', 'seal_fingerprint'}
    _require(type(value) is dict and set(value) == expected)
    _require(type(value['schema_version']) is int and value['schema_version'] == 1
             and value['candidate_schema'] == (OWNERSHIP_SCHEMA if seal else CHECKPOINT_SCHEMA)
             and type(run_id) is str and re.fullmatch(r'[0-9a-f]{12}', run_id) is not None
             and provider in {'aws', 'azure', 'gcp', 'oci'}
             and value['run_id'] == run_id and value['provider'] == provider
             and all(_digest(value[key]) for key in _ROOTS - {'run_id', 'provider'})
             and _digest(value['fingerprint'])
             and value['fingerprint'] == _fingerprint({k: v for k, v in value.items() if k != 'fingerprint'}))
    if not seal:
        _require(type(value['contract_recorded']) is bool
                 and (value['seal_fingerprint'] is None or _digest(value['seal_fingerprint'])))
    _projection({k: value[k] for k in _PROJECTION}, run_id=run_id, provider=provider,
                recorded=True if seal else value['contract_recorded'])
    if seal:
        _require(not value['unresolved_creates'], 'Unresolved original create identity blocks cleanup.')
    return value


def validate_hotel_startup_ownership_checkpoint(value, *, run_id, provider):
    return _validate(value, run_id, provider, seal=False)


def validate_hotel_startup_cleanup_ownership(value: dict, *, run_id: str, provider: str) -> dict:
    return _validate(value, run_id, provider, seal=True)


def _signed(value):
    return {**value, 'fingerprint': _fingerprint(value)}


def make_hotel_startup_ownership_checkpoint(job, *, support_pin, initialization_pin, startup_pin):
    roots = _roots(job, support_pin, initialization_pin, startup_pin)
    value = {**roots, 'schema_version': 1, 'candidate_schema': CHECKPOINT_SCHEMA,
             'contract_recorded': False, 'seal_fingerprint': None,
             'provider_scope': {}, 'create_contract': None, 'resources': {},
             'known_identities': {}, 'absence_targets': {}, 'create_attempts': {}, 'unresolved_creates': [], 'audits': []}
    return validate_hotel_startup_ownership_checkpoint(_signed(value), run_id=roots['run_id'], provider=roots['provider'])


def bind_hotel_startup_ownership_checkpoint(job, *, support_pin, initialization_pin, startup_pin):
    roots = _roots(job, support_pin, initialization_pin, startup_pin)
    value = validate_hotel_startup_ownership_checkpoint(job.get(CHECKPOINT_KEY),
        run_id=roots['run_id'], provider=roots['provider'])
    _require(all(value[key] == item for key, item in roots.items()))
    return value


def update_hotel_startup_ownership_checkpoint(value, projection):
    """Retain facts, not old mutable graph state. Caller validates native schemas.

    Dropping an unresolved intent is only legal when the provider has proved
    rejection or supplied its original allocation ID; that provider-specific
    transition check belongs to Task6 before this function is called.
    """
    old = validate_hotel_startup_ownership_checkpoint(value, run_id=value.get('run_id'), provider=value.get('provider'))
    projection = _copy(projection)
    _projection(projection, run_id=old['run_id'], provider=old['provider'], recorded=True)
    if old['contract_recorded']:
        for key in ('provider_scope', 'create_contract'):
            _require(_canonical(old[key]) == _canonical(projection[key]), 'Original create contract or scope changed.')
    for key in ('known_identities', 'absence_targets', 'create_attempts'):
        for name, row in old[key].items():
            _require(name not in projection[key] or _canonical(row) == _canonical(projection[key][name]),
                     'Original allocation identity or absence target was replaced.')
        if old['seal_fingerprint'] is not None:
            _require(set(projection[key]) <= set(old[key]), 'Sealed target scope cannot expand.')
        projection[key] = {**old[key], **projection[key]}
    audits = old['audits'][:]
    for audit in projection['audits']:
        if audit not in audits:
            _require(old['seal_fingerprint'] is None, 'Sealed audit evidence cannot expand.')
            audits.append(audit)
    projection['audits'] = audits
    if old['seal_fingerprint'] is not None:
        _require(not projection['unresolved_creates'], 'Sealed cleanup cannot introduce unresolved creation.')
    new = {**old, **projection, 'contract_recorded': True}
    new.pop('fingerprint')
    return validate_hotel_startup_ownership_checkpoint(_signed(new), run_id=old['run_id'], provider=old['provider'])


def record_hotel_startup_ownership_audit(value, audit):
    """Attach an already provider-validated audit's safe identity envelope.

    Only the actual provider-validated safe cloud_identity body is retained,
    never an arbitrary observation/status/error body. Its finite native schema
    and relation to retained targets MUST be validated by the adapter first.
    """
    old = validate_hotel_startup_ownership_checkpoint(value, run_id=value.get('run_id'), provider=value.get('provider'))
    _require(old['contract_recorded'] and old['seal_fingerprint'] is None)
    _require(isinstance(audit, Mapping) and type(audit.get('schema_version')) is int
             and audit['schema_version'] == 1 and audit.get('candidate_only') is True
             and audit.get('read_only') is True and audit.get('run_id') == old['run_id']
             and audit.get('provider') == old['provider']
             and type(audit.get('cloud_identity')) is dict and bool(audit['cloud_identity']))
    envelope = {'schema_version': 1, 'run_id': old['run_id'], 'provider': old['provider'],
                'cloud_identity': _copy(audit['cloud_identity']),
                'cloud_identity_sha256': _fingerprint(audit['cloud_identity']),
                'known_identities': old['known_identities']}
    projection = {key: old[key] for key in _PROJECTION}
    projection['audits'] = [envelope]
    return update_hotel_startup_ownership_checkpoint(old, projection)


def make_hotel_startup_cleanup_ownership(job: dict, *, support_pin: dict,
                                         initialization_pin: dict, startup_pin: dict) -> dict:
    checkpoint = bind_hotel_startup_ownership_checkpoint(job, support_pin=support_pin,
        initialization_pin=initialization_pin, startup_pin=startup_pin)
    _require(checkpoint['contract_recorded'], 'No original provider create contract was recorded.')
    _require(checkpoint['seal_fingerprint'] is None, 'Reuse the original published seal; never reconstruct it from progress.')
    value = {key: checkpoint[key] for key in _ROOTS | _PROJECTION}
    value.update(schema_version=1, candidate_schema=OWNERSHIP_SCHEMA)
    return validate_hotel_startup_cleanup_ownership(_signed(value), run_id=checkpoint['run_id'], provider=checkpoint['provider'])


def seal_hotel_startup_ownership_checkpoint(value, seal):
    old = validate_hotel_startup_ownership_checkpoint(value, run_id=value.get('run_id'), provider=value.get('provider'))
    seal = validate_hotel_startup_cleanup_ownership(seal, run_id=old['run_id'], provider=old['provider'])
    _require(all(old[key] == seal[key] for key in _ROOTS))
    if old['seal_fingerprint'] is not None:
        _require(old['seal_fingerprint'] == seal['fingerprint'], 'The immutable cleanup seal cannot be replaced.')
        _require(all(old[key] == seal[key] for key in ('provider_scope', 'create_contract', 'known_identities',
                                                     'absence_targets', 'create_attempts', 'audits')))
        return old
    _require(old['contract_recorded'] and all(old[key] == seal[key] for key in _PROJECTION))
    new = {**old, 'seal_fingerprint': seal['fingerprint']}
    new.pop('fingerprint')
    return _signed(new)


def validate_hotel_startup_cleanup_not_required(value, checkpoint):
    """Local never-authorized-create disposition, explicitly NOT absence proof."""
    _require(type(checkpoint) is dict)
    checkpoint = validate_hotel_startup_ownership_checkpoint(checkpoint,
        run_id=checkpoint.get('run_id'), provider=checkpoint.get('provider'))
    _require(not checkpoint['contract_recorded'] and checkpoint['seal_fingerprint'] is None)
    _projection({key: checkpoint[key] for key in _PROJECTION}, run_id=checkpoint['run_id'],
                provider=checkpoint['provider'], recorded=False)
    _require(type(value) is dict and set(value) == _ROOTS | {
        'schema_version', 'candidate_schema', 'ownership_checkpoint_fingerprint', 'reason', 'fingerprint'})
    _require(type(value['schema_version']) is int and value['schema_version'] == 1
             and value['candidate_schema'] == NOT_REQUIRED_SCHEMA
             and all(value[key] == checkpoint[key] for key in _ROOTS)
             and value['ownership_checkpoint_fingerprint'] == checkpoint['fingerprint']
             and value['reason'] == 'provider-create-contract-never-authorized'
             and value['fingerprint'] == _fingerprint({k: v for k, v in value.items() if k != 'fingerprint'}))
    return _copy(value)


def make_hotel_startup_cleanup_not_required(job, *, support_pin, initialization_pin, startup_pin):
    checkpoint = bind_hotel_startup_ownership_checkpoint(job, support_pin=support_pin,
        initialization_pin=initialization_pin, startup_pin=startup_pin)
    value = {key: checkpoint[key] for key in _ROOTS}
    value.update(schema_version=1, candidate_schema=NOT_REQUIRED_SCHEMA,
        ownership_checkpoint_fingerprint=checkpoint['fingerprint'], reason='provider-create-contract-never-authorized')
    return validate_hotel_startup_cleanup_not_required(_signed(value), checkpoint)


_SAFE_BOOLS = frozenset({'candidate_only', 'released', 'initialization_executed', 'database_execution_verified',
    'global_readonly_verification_completed', 'all_application_replicas_zero', 'applications_paused',
    'startup_execution_observed', 'service_identity_observed', 'application_start_allowed', 'runtime_qualified',
    'measurement_qualified', 'cloud_qualified', 'public_release', 'public_dispatch_enabled'})
_SAFE_IDENTITIES = frozenset({'run_id', 'initialization_attempt_id', 'startup_attempt_id', 'pin_fingerprint',
    'guarded_artifact_fingerprint', 'startup_artifact_fingerprint', 'plan_fingerprint', 'stage_fingerprint',
    'snapshot_fingerprint'})
_SAFE_STATES = frozenset({'accepted', 'provisioning', 'auditing_cloud', 'preparing_runtime', 'auditing_runtime',
    'checking_support_access', 'staging_support', 'support_observed', 'initializing', 'initialization_observed',
    'starting', 'startup_observed', 'cleanup_pending', 'cleaning', 'cleanup_complete', 'cleanup_failed',
    'cleanup_not_required',
    'initialized-and-globally-verified-applications-paused', 'startup-and-service-identities-observed',
    'failed-cleanup-only', 'support_ready_applications_paused'})


def _safe_record(record):
    if not isinstance(record, Mapping):
        return {}
    result = {}
    for key in _SAFE_BOOLS:
        if type(record.get(key)) is bool:
            result[key] = record[key]
    for key in _SAFE_IDENTITIES:
        value = record.get(key)
        if type(value) is str and (_digest(value) or re.fullmatch(r'[0-9a-f]{12}|[0-9a-f-]{36}', value)):
            result[key] = value
    if type(record.get('schema_version')) is int and record['schema_version'] == 1:
        result['schema_version'] = 1
    schema = record.get('candidate_schema')
    if type(schema) is str and schema in {'hotel-guarded-live-initialization-outcome-v1', 'hotel-live-startup-outcome-v1',
                  'hotel-startup-operator-v1', 'candidate-support-stage-journal-v1'}:
        result['candidate_schema'] = schema
    if type(record.get('state')) is str and record['state'] in _SAFE_STATES:
        result['state'] = record['state']
    for key, allowed in (('execution_outcome', {'completed', 'failed'}),
                         ('cleanup_outcome', {'completed', 'failed', 'not_required'})):
        value = record.get(key)
        if type(value) is str and value in allowed:
            result[key] = value
    for key in ('error', 'cleanup_error'):
        value = record.get(key)
        if value is None:
            result[key] = None
        elif (type(value) is dict and set(value) == {'code', 'interrupted'}
              and type(value['code']) is str and value['code'] in {
                  'execution-failed', 'evidence-publication-failed', 'execution-stopped-before-create',
                  'execution-interrupted', 'cleanup-failed'} and type(value['interrupted']) is bool):
            result[key] = dict(value)
    return result


def project_hotel_startup_job(job):
    """Detached diagnostic allowlist, never a recoverable execution capability.

    Unknown and nested carrier copies are omitted, not recursively trusted.
    Native resources live in independently validated ownership, not state.json.
    Raw statuses/logs/HTTP/error strings are never copied or hashed here.
    """
    from .deathstarbench_hotel_startup_operator import PIN_KEY, OPERATOR_JOURNAL_KEY
    result = {'id': job['id'], 'resources': {}, 'results': []}
    for key in ('plan', support.PIN_KEY, initialization.PIN_KEY, PIN_KEY):
        if key in job:
            result[key] = _copy(job[key])
    for key in ('guarded_hotel_initialization', 'hotel_application_startup', OPERATOR_JOURNAL_KEY):
        if key in job:
            result[key] = _safe_record(job[key])
    ready = job.get('resources', {}).get(support.SUPPORT_STAGE_JOURNAL_KEY)
    if ready is not None:
        result['resources'][support.SUPPORT_STAGE_JOURNAL_KEY] = _safe_record(ready)
    if 'hotel_startup_archive' in job:
        archive = job['hotel_startup_archive']
        result['hotel_startup_archive'] = {key: _safe_record(archive.get(key))
            for key in ('guarded_hotel_initialization', 'hotel_application_startup', OPERATOR_JOURNAL_KEY,
                        support.SUPPORT_STAGE_JOURNAL_KEY)}
    if NOT_REQUIRED_KEY in job:
        result[NOT_REQUIRED_KEY] = validate_hotel_startup_cleanup_not_required(
            job[NOT_REQUIRED_KEY], job.get(CHECKPOINT_KEY))
    return _copy(result)
