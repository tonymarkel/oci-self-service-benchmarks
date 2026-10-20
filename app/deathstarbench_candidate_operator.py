"""Private, cleanup-owning support-stage operator orchestration.

Provider callbacks own provisioning, fresh read-only cloud audits and exact
terminal deletion proof. This boundary owns durable intent, the acquired lease,
phase sequencing and evidence preservation. A separately pinned private Hotel
extension may initialize its dataset. No path dispatches a public run, starts
an application or produces benchmark results.
"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from contextlib import contextmanager
from dataclasses import dataclass
import json
import re
import signal
import threading
from typing import Any

from .deathstarbench_candidate_artifacts import CandidateArtifactBinding
from .deathstarbench_candidate_deployment import (
    JOURNAL_SCHEMA as SUPPORT_JOURNAL_SCHEMA,
    READY_STATE,
    SUPPORT_STAGE_JOURNAL_KEY,
)
from .deathstarbench_candidate_runtime import _canonical, _closed_gates, _fingerprint
from .deathstarbench_workload_contract import distributed_workload_profile
from .run_lease import RunLease, require_acquired_run_lease


PIN_KEY = 'candidate_support_pin'
OPERATOR_JOURNAL_KEY = 'candidate_support_operator'
STAGE_ARCHIVE_KEY = 'support_stage_archive'
PIN_SCHEMA = 'candidate-support-operator-pin-v1'
OPERATOR_SCHEMA = 'candidate-support-operator-v1'
ARCHIVE_SCHEMA = 'candidate-support-stage-archive-v1'
CLEANUP_PROOF_SCHEMA = 'candidate-support-cleanup-proof-v1'
HOTEL_PIN_KEY = 'guarded_hotel_initialization_pin'
HOTEL_STATE_KEY = 'guarded_hotel_initialization'
_SHA256 = re.compile(r'sha256:[0-9a-f]{64}\Z')
_STATES = frozenset({
    'accepted', 'provisioning', 'auditing_cloud', 'preparing_runtime',
    'auditing_runtime', 'checking_support_access', 'staging_support',
    'support_observed', 'cleanup_pending', 'cleaning', 'cleanup_complete', 'cleanup_failed',
})
_TEARDOWN = frozenset({'cleanup_pending', 'cleaning', 'cleanup_complete', 'cleanup_failed'})
_MAX_EVIDENCE_BYTES = 64 * 1024 * 1024


class CandidateOperatorError(ValueError):
    """Candidate operator ownership, sequencing or evidence is invalid."""


class CandidateOperatorRefusal(CandidateOperatorError):
    """Preflight/recovery was refused before taking cleanup ownership."""


def _copy(value):
    encoded = _canonical(value).encode('utf-8')
    if len(encoded) > _MAX_EVIDENCE_BYTES:
        raise CandidateOperatorError('Candidate operator evidence exceeds its size bound.')
    return json.loads(encoded)


def _scope(guarded_hotel_initialization=False):
    value = {
        'scope': 'support-stage-only-applications-paused',
        'application_start_allowed': False,
        'operator_started_applications': False, 'operator_initialized_dataset': False,
        'operator_executed_benchmark': False, 'public_dispatch_enabled': False,
        'cleanup_policy': 'always-attempt-exact-owned-infrastructure-destruction',
        'qualification': _closed_gates(),
    }
    if guarded_hotel_initialization:
        value['scope'] = 'support-stage-and-guarded-hotel-initialization-applications-paused'
        del value['operator_initialized_dataset']
        value['dataset_execution_evidence_key'] = HOTEL_STATE_KEY
    return value


def _hotel_operation_pin(job, pin, lease, enabled):
    """Validate immutable operation identity, never reconstruct runtime authority."""
    supplied = job.get(HOTEL_PIN_KEY)
    if not enabled:
        if supplied is not None:
            raise CandidateOperatorRefusal('A pinned Hotel operation requires its explicit initialization scope.')
        return None
    from scripts.deathstarbench_candidate_operator_state import _read
    try:
        saved = _read(lease.path.parent / 'hotel-initialization-pin.json')
        if (pin['workload_id'] != 'hotel_reservation' or not isinstance(supplied, Mapping)
                or _canonical(saved) != _canonical(supplied)
                or supplied.get('candidate_schema') != 'hotel-guarded-initialization-operator-pin-v1'
                or type(supplied.get('schema_version')) is not int or supplied['schema_version'] != 1
                or supplied.get('candidate_only') is not True or supplied.get('released') is not False
                or supplied.get('run_id') != pin['run_id']
                or supplied.get('support_pin_fingerprint') != pin['fingerprint']
                or supplied.get('baseline_artifact_fingerprint') != pin['artifact_fingerprint']
                or not isinstance(supplied.get('initialization_attempt_id'), str)
                or not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}',
                                    supplied['initialization_attempt_id'])
                or any(supplied.get(key) is not False for key in (
                    'application_start_allowed', 'runtime_qualified', 'measurement_qualified', 'public_release'))
                or supplied.get('fingerprint') != _fingerprint({key: value for key, value in supplied.items() if key != 'fingerprint'})):
            raise ValueError('Guarded Hotel operation pin differs.')
        metadata = supplied['guarded_artifacts']
        if (metadata['workload_id'] != 'hotel_reservation'
                or metadata['baseline']['binding_fingerprint'] != pin['artifact_fingerprint']
                or metadata['fingerprint'] != supplied['guarded_artifact_fingerprint']
                or metadata['fingerprint'] != _fingerprint({key: value for key, value in metadata.items() if key != 'fingerprint'})):
            raise ValueError('Guarded Hotel artifact pin differs.')
        return _copy(supplied)
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise CandidateOperatorRefusal('The explicit Hotel scope requires its matching immutable initialization pin.') from exc


def _hotel_summary(job, pin):
    """Report retained execution evidence without promoting qualification gates."""
    record = job.get(HOTEL_STATE_KEY)
    identity_matches = isinstance(record, Mapping) and all(record.get(key) == expected for key, expected in (
        ('run_id', pin['run_id']), ('initialization_attempt_id', pin['initialization_attempt_id']),
        ('guarded_artifact_fingerprint', pin['guarded_artifact_fingerprint']),
    ))
    initialized = record.get('initialization_executed') if identity_matches else False if record is None else None
    initialized = initialized if type(initialized) is bool else None
    complete = (identity_matches
                and record.get('state') == 'initialized-and-globally-verified-applications-paused'
                and initialized is True and record.get('database_execution_verified') is True
                and record.get('global_readonly_verification_completed') is True)
    completion = ('completed' if complete else 'not-started' if record is None else
                  'failed' if identity_matches and record.get('state') == 'failed-cleanup-only' else 'unknown')
    return {
        'completion': completion, 'evidence_key': HOTEL_STATE_KEY,
        'initialization_executed': initialized,
        'database_execution_verified': complete,
        'global_readonly_verification_completed': complete,
        'application_start_allowed': False, 'runtime_qualified': False,
        'measurement_qualified': False, 'public_release': False,
    }


def _error(exc, interrupted=False):
    return {'type': type(exc).__name__, 'message': str(exc)[:2048], 'interrupted': bool(interrupted)}


def _validate_pin(job, pin, artifacts, *, cleanup_only):
    if not isinstance(job, MutableMapping) or not isinstance(job.get('resources'), MutableMapping):
        raise CandidateOperatorRefusal('A private mutable candidate job/resources mapping is required.')
    if job.get('_persist_state') is not False or job.get('results') not in (None, []):
        raise CandidateOperatorRefusal('Candidate operators must stay outside public persistence/results.')
    keys = {
        'schema_version', 'candidate_schema', 'candidate_only', 'released',
        'run_id', 'workload_id', 'artifact_fingerprint', 'artifacts',
        'infrastructure_plan', 'provider_pin', 'ssh_public_key_sha256', 'fingerprint',
    }
    if not isinstance(pin, Mapping) or set(pin) != keys:
        raise CandidateOperatorRefusal('Candidate operator pin schema is invalid.')
    if (type(pin['schema_version']) is not int or pin['schema_version'] != 1
            or pin['candidate_schema'] != PIN_SCHEMA or pin['candidate_only'] is not True
            or pin['released'] is not False or pin['run_id'] != job.get('id')):
        raise CandidateOperatorRefusal('Candidate operator pin ownership/release state differs.')
    if _canonical(pin) != _canonical(job.get(PIN_KEY)) or _canonical(pin['infrastructure_plan']) != _canonical(job.get('plan')):
        raise CandidateOperatorRefusal('Private pin or infrastructure carrier plan differs.')
    provider_pin = pin['provider_pin']
    provider = pin['infrastructure_plan'].get('provider') if isinstance(pin['infrastructure_plan'], Mapping) else None
    if provider not in {'aws', 'azure', 'gcp', 'oci'} or not isinstance(provider_pin, Mapping) or provider_pin.get('provider') != provider:
        raise CandidateOperatorRefusal('Candidate provider pin differs from its carrier plan.')
    if any(key in pin['infrastructure_plan'] for key in ('ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase')):
        raise CandidateOperatorRefusal('SSH material must not enter the sanitized carrier plan.')
    for digest in ('fingerprint', 'artifact_fingerprint'):
        if not isinstance(pin[digest], str) or not _SHA256.fullmatch(pin[digest]) or pin[digest].endswith('0' * 64):
            raise CandidateOperatorRefusal('Candidate pin requires exact nonzero fingerprints.')
    if pin['fingerprint'] != _fingerprint({key: pin[key] for key in pin if key != 'fingerprint'}):
        raise CandidateOperatorRefusal('Candidate pin fingerprint differs from its complete contents.')
    key_digest = pin['ssh_public_key_sha256']
    if not isinstance(key_digest, str) or not re.fullmatch(r'[0-9a-f]{64}', key_digest) or key_digest == '0' * 64:
        raise CandidateOperatorRefusal('Candidate pin requires its exact nonzero SSH public-key digest.')
    try:
        profile = distributed_workload_profile(pin['workload_id'])
    except (TypeError, ValueError) as exc:
        raise CandidateOperatorRefusal('Candidate workload identity is invalid.') from exc
    if profile.released or pin['workload_id'] not in {'hotel_reservation', 'media_microservices'}:
        raise CandidateOperatorRefusal('Only unreleased Hotel/Media support operators are allowed.')
    if not cleanup_only:
        if not isinstance(artifacts, CandidateArtifactBinding):
            raise CandidateOperatorRefusal('Fresh factory-adopted candidate artifacts are required.')
        artifacts.__post_init__()
        if (artifacts.workload_id != pin['workload_id'] or artifacts.fingerprint != pin['artifact_fingerprint']
                or _canonical(artifacts.metadata()) != _canonical(pin['artifacts'])):
            raise CandidateOperatorRefusal('Fresh artifact adoption differs from the immutable operator pin.')
    return provider


def _new_record(pin, *, guarded_hotel_initialization=False):
    return {
        'schema_version': 1, 'candidate_schema': OPERATOR_SCHEMA,
        'candidate_only': True, 'released': False,
        'run_id': pin['run_id'], 'workload_id': pin['workload_id'],
        'pin_fingerprint': pin['fingerprint'], 'scope': _scope(guarded_hotel_initialization),
        'attempt': 1, 'state': 'accepted', 'cloud_identity': None,
        'audits': [], 'support_image_access': None, 'support_snapshot': None,
        'support_stage_complete': False, 'cleanup_complete': False,
        'cleanup_proof': None, 'failure': None, 'cleanup_failure': None,
    }


def validate_candidate_operator_record(value, pin, *, guarded_hotel_initialization=None):
    """Validate retained history; it cannot substitute for fresh audit/access."""
    if guarded_hotel_initialization is None:
        guarded_hotel_initialization = (isinstance(value, Mapping)
                                        and _canonical(value.get('scope')) == _canonical(_scope(True)))
    if guarded_hotel_initialization and pin['workload_id'] != 'hotel_reservation':
        raise CandidateOperatorRefusal('Guarded initialization scope is Hotel-only.')
    expected = _new_record(pin, guarded_hotel_initialization=guarded_hotel_initialization)
    dynamic = set(expected) - {
        'schema_version', 'candidate_schema', 'candidate_only', 'released',
        'run_id', 'workload_id', 'pin_fingerprint', 'scope',
    }
    if not isinstance(value, Mapping) or set(value) != set(expected) or any(
        _canonical(value[key]) != _canonical(expected[key]) for key in set(expected) - dynamic
    ):
        raise CandidateOperatorRefusal('Retained operator intent/scope differs from its pin.')
    if (type(value['attempt']) is not int or value['attempt'] < 1
            or not isinstance(value['state'], str) or value['state'] not in _STATES or not isinstance(value['audits'], list)
            or type(value['support_stage_complete']) is not bool or type(value['cleanup_complete']) is not bool):
        raise CandidateOperatorRefusal('Retained operator progress is invalid.')
    if value['cloud_identity'] is not None and (not isinstance(value['cloud_identity'], Mapping) or not value['cloud_identity']):
        raise CandidateOperatorRefusal('Retained cloud identity is invalid.')
    if value['support_stage_complete'] and not isinstance(value['support_snapshot'], Mapping):
        raise CandidateOperatorRefusal('Retained support success lacks its historical snapshot.')
    if value['cleanup_complete']:
        if value['state'] != 'cleanup_complete':
            raise CandidateOperatorRefusal('Cleanup success is retained outside the complete state.')
        _cleanup_proof(value['cleanup_proof'], pin)
    elif value['cleanup_proof'] is not None:
        raise CandidateOperatorRefusal('Uncompleted cleanup retains a success proof.')
    return _copy(dict(value))


def _audit(value, pin):
    if (not isinstance(value, Mapping) or type(value.get('schema_version')) is not int
            or value['schema_version'] != 1 or value.get('candidate_only') is not True
            or value.get('read_only') is not True or value.get('run_id') != pin['run_id']
            or value.get('provider') != pin['provider_pin']['provider']
            or not isinstance(value.get('cloud_identity'), Mapping) or not value['cloud_identity']):
        raise CandidateOperatorError('Fresh read-only cloud audit lacks its exact run/provider identity.')
    for flag in ('released', 'runtime_qualified', 'measurement_qualified', 'public_release'):
        if value.get(flag, False) is not False:
            raise CandidateOperatorError('A cloud audit cannot promote benchmark qualification.')
    return _copy(dict(value))


def _access(value, pin):
    if (not isinstance(value, Mapping) or type(value.get('schema_version')) is not int or value['schema_version'] != 1
            or value.get('candidate_schema') != 'candidate-support-image-access-v1'
            or value.get('scope') != 'active-support-images-only-no-runtime-or-application-qualification'):
        raise CandidateOperatorError('A fresh support-only image-access receipt is required.')
    identity = value.get('identity')
    if not isinstance(identity, Mapping) or any(identity.get(key) != expected for key, expected in (
        ('run_id', pin['run_id']), ('provider', pin['provider_pin']['provider']),
        ('workload_id', pin['workload_id']), ('artifact_fingerprint', pin['artifact_fingerprint']),
    )):
        raise CandidateOperatorError('Support image access crossed its operator identity.')
    for flag in ('released', 'runtime_qualified', 'measurement_qualified', 'anonymous_pull_qualified',
                 'anonymous_layer_pull_qualified', 'application_start_allowed'):
        if value.get(flag) is not False:
            raise CandidateOperatorError('Support byte access cannot promote application/runtime qualification.')
    if (value.get('candidate_only') is not True or value.get('anonymous_support_bytes_verified') is not True
            or value.get('support_image_execution_performed') is not False):
        raise CandidateOperatorError('Support access must prove bytes only, without execution.')
    return _copy(dict(value))


def _validated_support(job, artifacts, value, access):
    # The stage facade validates complete live receipts, not just a ready label.
    from . import deathstarbench_candidate_deployment as deployment
    from .deathstarbench_distributed import distributed_k3s_candidate_plan
    _cluster, plan, stage, identity = deployment._context(job, artifacts, distributed_k3s_candidate_plan)
    result = deployment.validate_candidate_support_journal(value, plan, stage, identity)
    if (result['state'] != READY_STATE or result['candidate_schema'] != SUPPORT_JOURNAL_SCHEMA
            or _canonical(result) != _canonical(job['resources'].get(SUPPORT_STAGE_JOURNAL_KEY))):
        raise CandidateOperatorError('Support success is not the exact persisted ready stage journal.')
    access_identity = access['identity']
    if (access_identity.get('snapshot_fingerprint') != plan.fingerprint
            or access_identity.get('stage_fingerprint') != stage.fingerprint
            or access_identity.get('stage_manifest_sha256') != stage.rendered_manifest_sha256):
        raise CandidateOperatorError('Support image access differs from the staged runtime identity.')
    return _copy(result)


def _cleanup_proof(value, pin):
    if (not isinstance(value, Mapping) or type(value.get('schema_version')) is not int
            or value['schema_version'] != 1 or value.get('candidate_schema') != CLEANUP_PROOF_SCHEMA
            or value.get('candidate_only') is not True or value.get('run_id') != pin['run_id']
            or value.get('provider') != pin['provider_pin']['provider']
            or value.get('cleanup_complete') is not True or value.get('independent_absence_proved') is not True):
        raise CandidateOperatorError('Cleanup requires exact independent terminal-absence proof, not local status.')
    for flag in ('released', 'runtime_qualified', 'measurement_qualified', 'public_release',
                 'anonymous_pull_qualified', 'anonymous_layer_pull_qualified', 'application_start_allowed'):
        if value.get(flag, False) is not False:
            raise CandidateOperatorError('Cleanup proof cannot promote application or benchmark qualification.')
    return _copy(dict(value))


@contextmanager
def _defer_cleanup_signals():
    """A repeated cooperative signal must not abort exact owned cleanup."""
    state = {'number': None}
    if threading.current_thread() is not threading.main_thread():
        yield state
        return
    previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
    def defer(number, _frame):
        if state['number'] is None:
            state['number'] = number
    try:
        for number in previous:
            signal.signal(number, defer)
        yield state
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


@dataclass(frozen=True, slots=True)
class CandidateOperatorOutcome:
    exit_code: int
    support_stage_complete: bool
    cleanup_complete: bool
    _support_json: str | None
    _cleanup_json: str | None
    failure: str | None
    cleanup_failure: str | None
    _hotel_json: str | None = None

    @property
    def support_snapshot(self):
        return json.loads(self._support_json) if self._support_json is not None else None

    @property
    def cleanup_proof(self):
        return json.loads(self._cleanup_json) if self._cleanup_json is not None else None

    def metadata(self):
        value = {key: getattr(self, key) for key in (
            'exit_code', 'support_stage_complete', 'cleanup_complete', 'support_snapshot',
            'cleanup_proof', 'failure', 'cleanup_failure',
        )}
        if self._hotel_json is not None:
            value.update(operation_scope=_scope(True), hotel_initialization=json.loads(self._hotel_json))
        return value


def run_candidate_support_operator(
    job, *, lease: RunLease, pin, persist, provision=None, fresh_audit=None,
    prepare_runtime=None, support_image_access=None, stage=None, destroy_and_prove,
    artifacts=None, resume=False, cleanup_only=False, cleanup_signal_context=None,
    interrupt_exceptions=(KeyboardInterrupt,), guarded_hotel_initialization=False,
) -> CandidateOperatorOutcome:
    """Run support staging with one lease, or perform registry/SSH-free cleanup.

    A refused recovery (including its initial fresh cloud audit) does not own
    cleanup. Once new provisioning starts or resume ownership is accepted,
    every return attempts exact cleanup. SIGKILL/power loss cannot run finally;
    durable phase intent and provider inventory remain for explicit recovery.
    """
    if type(resume) is not bool or type(cleanup_only) is not bool or (resume and cleanup_only):
        raise CandidateOperatorRefusal('Choose exactly one new, resume or cleanup-only mode.')
    if type(guarded_hotel_initialization) is not bool or (guarded_hotel_initialization and resume):
        raise CandidateOperatorRefusal('Guarded Hotel operations require explicit new or cleanup-only scope.')
    if (not isinstance(interrupt_exceptions, tuple) or not interrupt_exceptions
            or any(not isinstance(item, type) or not issubclass(item, BaseException) for item in interrupt_exceptions)):
        raise CandidateOperatorRefusal('Interruption exception types must be explicit BaseException subclasses.')
    provider = _validate_pin(job, pin, artifacts, cleanup_only=cleanup_only)
    require_acquired_run_lease(lease, pin['run_id'])
    hotel_pin = _hotel_operation_pin(job, pin, lease, guarded_hotel_initialization)
    if lease.path.parent.parent.name != '.candidate-support':
        raise CandidateOperatorRefusal('Candidate operators require the private candidate state namespace.')
    if not callable(persist) or not callable(destroy_and_prove):
        raise CandidateOperatorRefusal('Durable persistence and independent cleanup proof callbacks are required.')
    if not cleanup_only and not all(callable(item) for item in (fresh_audit, prepare_runtime, support_image_access, stage)):
        raise CandidateOperatorRefusal('Fresh cloud/runtime/access/stage callbacks are required.')
    if not resume and not cleanup_only and not callable(provision):
        raise CandidateOperatorRefusal('New candidates require an explicit provisioning callback.')
    pin = _copy(dict(pin))
    original = job.get(OPERATOR_JOURNAL_KEY)
    if resume or cleanup_only:
        current = validate_candidate_operator_record(original, pin, guarded_hotel_initialization=guarded_hotel_initialization)
        if not cleanup_only and current['state'] in _TEARDOWN:
            raise CandidateOperatorRefusal('Teardown has begun; only cleanup-only recovery is permitted.')
    else:
        if original is not None or job['resources']:
            raise CandidateOperatorRefusal('A new operator cannot adopt existing state or resources.')
        current = None
    expected_record = current
    teardown = False

    def binding():
        require_acquired_run_lease(lease, pin['run_id'])
        _validate_pin(job, pin, artifacts, cleanup_only=cleanup_only)
        if not teardown and _canonical(_hotel_operation_pin(job, pin, lease, guarded_hotel_initialization)) != _canonical(hotel_pin):
            raise CandidateOperatorError('Guarded Hotel operation identity changed outside its durable lifecycle.')
        if _canonical(job.get(OPERATOR_JOURNAL_KEY)) != _canonical(expected_record):
            raise CandidateOperatorError('Operator intent changed outside its durable lifecycle.')

    def callback(function):
        binding()
        result = function(job, lease)
        binding()
        return result

    def write(**changes):
        nonlocal current, expected_record
        binding()
        candidate = {**(current or _new_record(pin, guarded_hotel_initialization=guarded_hotel_initialization)), **changes}
        candidate = validate_candidate_operator_record(candidate, pin, guarded_hotel_initialization=guarded_hotel_initialization)
        current = expected_record = candidate
        job[OPERATOR_JOURNAL_KEY] = _copy(candidate)
        callback(persist)

    # Recovery refuses before accepting cleanup ownership. The audit is cloud
    # read-only; it may publish validated local aliases through its callback.
    initial_audit = None
    if resume and not cleanup_only:
        try:
            initial_audit = _audit(callback(fresh_audit), pin)
            if current['cloud_identity'] is not None and _canonical(current['cloud_identity']) != _canonical(initial_audit['cloud_identity']):
                raise CandidateOperatorRefusal('Resumed immutable cloud create identities changed.')
        except BaseException as exc:
            raise CandidateOperatorRefusal('Fresh recovery ownership audit was refused; use explicit cleanup-only.') from exc
    if current is None:
        # A persistence failure here precedes provisioning; no cleanup authority
        # has been accepted and no cloud callback has been invoked.
        write()
    elif not cleanup_only:
        write(attempt=current['attempt'] + 1, state='accepted', failure=None,
              cleanup_failure=None, cleanup_complete=False, cleanup_proof=None,
              support_stage_complete=False, support_snapshot=None, support_image_access=None,
              audits=[])

    failure = None
    cleanup_failure = None
    support = None
    cleanup = None
    interrupted = False
    signal_state = None
    try:
        if not cleanup_only:
            if not resume:
                write(state='provisioning')
                callback(provision)
                write(state='auditing_cloud')
                initial_audit = _audit(callback(fresh_audit), pin)
            write(state='preparing_runtime', cloud_identity=initial_audit['cloud_identity'], audits=[initial_audit])
            callback(prepare_runtime)
            write(state='auditing_runtime')
            after_runtime = _audit(callback(fresh_audit), pin)
            if _canonical(after_runtime['cloud_identity']) != _canonical(current['cloud_identity']):
                raise CandidateOperatorError('Cloud create identities drifted across runtime preparation.')
            write(state='checking_support_access', audits=[*current['audits'], after_runtime])
            access = _access(callback(support_image_access), pin)
            write(state='staging_support', support_image_access=access)
            staged = callback(stage)
            if guarded_hotel_initialization and _hotel_summary(job, hotel_pin)['completion'] != 'completed':
                raise CandidateOperatorError('Guarded Hotel staging lacks completed initialization and global verification evidence.')
            support = _validated_support(job, artifacts, staged, access)
            write(state='support_observed', support_stage_complete=True, support_snapshot=support)
    except BaseException as exc:
        interrupted = isinstance(exc, interrupt_exceptions)
        failure = _error(exc, interrupted)
    finally:
        # Hotel-only evidence damage forbids further execution, but cannot
        # revoke cleanup of the original support/cloud identity already owned.
        teardown = True
        # Evidence archival is deliberately independent of fresh registry/SSH
        # prerequisites, and runs under signal deferral with exact cleanup.
        try:
            with (cleanup_signal_context or _defer_cleanup_signals)() as signal_state:
                try:
                    write(state='cleanup_pending', failure=(current['failure'] if cleanup_only else failure),
                          attempt=current['attempt'] + (1 if cleanup_only else 0),
                          cleanup_complete=False, cleanup_proof=None, cleanup_failure=None)
                    raw = job['resources'].get(SUPPORT_STAGE_JOURNAL_KEY)
                    if SUPPORT_STAGE_JOURNAL_KEY in job['resources']:
                        payload = _copy(raw)
                        archive = {
                            'schema_version': 1, 'candidate_schema': ARCHIVE_SCHEMA,
                            'candidate_only': True, 'run_id': pin['run_id'],
                            'source_resource_key': SUPPORT_STAGE_JOURNAL_KEY,
                            'qualification_authority': False,
                            'payload_sha256': _fingerprint(payload), 'payload': payload,
                        }
                        previous = job.get(STAGE_ARCHIVE_KEY)
                        if previous is not None and _canonical(previous) != _canonical(archive):
                            raise CandidateOperatorError('Existing stage archive differs; evidence will not be overwritten.')
                        binding()
                        job[STAGE_ARCHIVE_KEY] = archive
                        callback(persist)  # Journal still present until archive is durable.
                        if (_canonical(job['resources'].get(SUPPORT_STAGE_JOURNAL_KEY)) != _canonical(payload)
                                or _canonical(job.get(STAGE_ARCHIVE_KEY)) != _canonical(archive)):
                            raise CandidateOperatorError('Stage evidence changed during archival persistence.')
                        binding()
                        del job['resources'][SUPPORT_STAGE_JOURNAL_KEY]
                        callback(persist)
                except BaseException as exc:
                    cleanup_failure = _error(exc, isinstance(exc, interrupt_exceptions))
                # Even a damaged evidence artifact must not suppress the exact
                # provider cleanup attempt. Unknown resource keys stay intact.
                for attempt in range(2):
                    try:
                        write(state='cleaning')
                    except BaseException as exc:
                        cleanup_failure = cleanup_failure or _error(exc, isinstance(exc, interrupt_exceptions))
                    try:
                        # A failed history write is not permission to discard
                        # the stage key, but it must not suppress this cleanup
                        # attempt. callback() still requires acquired authority
                        # and unchanged immutable intent before any cloud call.
                        cleanup = _cleanup_proof(callback(destroy_and_prove), pin)
                        break
                    except BaseException as exc:
                        cancellation = isinstance(exc, interrupt_exceptions)
                        if cancellation:
                            interrupted = True
                            failure = failure or _error(exc, True)
                        if not cancellation or attempt == 1:
                            cleanup_failure = cleanup_failure or _error(exc, cancellation)
                            break
                complete = cleanup is not None and cleanup_failure is None
                number = signal_state.get('number') if isinstance(signal_state, Mapping) else None
                if number in (signal.SIGINT, signal.SIGTERM):
                    interrupted = True
                    failure = failure or {'type': 'OperatorSignal', 'message': f'Signal {number} deferred during cleanup.', 'interrupted': True}
                write(state='cleanup_complete' if complete else 'cleanup_failed', cleanup_complete=complete,
                      cleanup_proof=cleanup if complete else None, cleanup_failure=cleanup_failure,
                      failure=(current['failure'] if cleanup_only and failure is None else failure))
                # A first signal may arrive during the final atomic save. Record
                # it once more while deferral is still active; subsequent
                # signals retain that same first number and cannot abort this.
                number = signal_state.get('number') if isinstance(signal_state, Mapping) else None
                if number in (signal.SIGINT, signal.SIGTERM) and failure is None:
                    interrupted = True
                    failure = {'type': 'OperatorSignal', 'message': f'Signal {number} deferred during cleanup.', 'interrupted': True}
                    write(failure=failure)
        except BaseException as exc:
            cleanup_failure = cleanup_failure or _error(exc, isinstance(exc, interrupt_exceptions))
    number = signal_state.get('number') if isinstance(signal_state, Mapping) else None
    if number in (signal.SIGINT, signal.SIGTERM):
        interrupted = True
        failure = failure or {'type': 'OperatorSignal', 'message': f'Signal {number} deferred during cleanup.', 'interrupted': True}
    complete = cleanup is not None and cleanup_failure is None
    exit_code = 2 if not complete else (128 + number if number in (signal.SIGINT, signal.SIGTERM) else
                                        130 if interrupted else 1 if failure is not None else 0)
    return CandidateOperatorOutcome(
        exit_code, support is not None, complete,
        _canonical(support) if support is not None else None,
        _canonical(cleanup) if complete else None,
        None if failure is None else failure['type'] + ': ' + failure['message'],
        None if cleanup_failure is None else cleanup_failure['type'] + ': ' + cleanup_failure['message'],
        _canonical(_hotel_summary(job, hotel_pin)) if guarded_hotel_initialization else None,
    )
