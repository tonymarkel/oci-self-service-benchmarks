"""Private, cleanup-owning one-shot Hotel initialization orchestration.

This is deliberately separate from the unchanged support-only operator. Its
narrow success means one initializer and a global read-only verifier completed
while every application remained paused; it is not benchmark qualification.
Any recovery is cleanup-only. Retained progress is evidence, never permission
to issue an initializer, remove a Pod gate, or repeat a lost response.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
import re
import signal
import uuid

from . import deathstarbench_candidate_operator as support
from .deathstarbench_candidate_runtime import _canonical, _closed_gates, _fingerprint
from .deathstarbench_hotel_initialization_artifacts import GuardedHotelArtifactBinding
from .run_lease import require_acquired_run_lease


PIN_KEY = 'hotel_initialization_pin'
PIN_SCHEMA = 'hotel-guarded-initialization-operator-pin-v1'
OPERATOR_JOURNAL_KEY = 'hotel_initialization_operator'
OPERATOR_SCHEMA = 'hotel-guarded-initialization-operator-v1'
_UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')
_STATES = frozenset({
    'accepted', 'provisioning', 'auditing_cloud', 'preparing_runtime',
    'auditing_runtime', 'checking_support_access', 'staging_support',
    'support_observed', 'initializing', 'initialization_observed',
    'cleanup_pending', 'cleaning', 'cleanup_complete', 'cleanup_failed',
})


class HotelInitializationOperatorError(ValueError):
    """Private Hotel lifecycle identity or history is invalid."""


def make_hotel_initialization_pin(run_id, support_pin, artifacts, *, attempt_id=None):
    """Bind a fresh attempt to guarded evidence and the original support pin."""
    if type(artifacts) is not GuardedHotelArtifactBinding:
        raise HotelInitializationOperatorError('Fresh guarded Hotel artifact adoption is required.')
    artifacts.__post_init__()
    if (not isinstance(support_pin, Mapping) or support_pin.get('run_id') != run_id
            or support_pin.get('workload_id') != 'hotel_reservation'
            or support_pin.get('fingerprint') != _fingerprint({key: value for key, value in support_pin.items() if key != 'fingerprint'})
            or artifacts.metadata()['baseline']['binding_fingerprint'] != support_pin.get('artifact_fingerprint')):
        raise HotelInitializationOperatorError('Guarded Hotel evidence crossed its original support pin.')
    attempt_id = str(uuid.uuid4()) if attempt_id is None else attempt_id
    if (type(attempt_id) is not str or _UUID.fullmatch(attempt_id) is None
            or attempt_id == '00000000-0000-0000-0000-000000000000'):
        raise HotelInitializationOperatorError('A nonzero canonical initialization attempt UUID is required.')
    value = {
        'schema_version': 1, 'candidate_schema': PIN_SCHEMA, 'candidate_only': True,
        'released': False, 'run_id': run_id, 'workload_id': 'hotel_reservation',
        'support_pin_fingerprint': support_pin['fingerprint'],
        'guarded_artifact_fingerprint': artifacts.fingerprint,
        'guarded_artifacts': artifacts.metadata(), 'initialization_attempt_id': attempt_id,
        'recovery_policy': 'cleanup-only-never-replay',
    }
    return {**value, 'fingerprint': _fingerprint(value)}


def validate_hotel_initialization_pin(value, support_pin, artifacts=None, *, cleanup_only=False):
    keys = {
        'schema_version', 'candidate_schema', 'candidate_only', 'released', 'run_id',
        'workload_id', 'support_pin_fingerprint', 'guarded_artifact_fingerprint',
        'guarded_artifacts', 'initialization_attempt_id', 'recovery_policy', 'fingerprint',
    }
    if (not isinstance(value, Mapping) or set(value) != keys
            or type(value['schema_version']) is not int or value['schema_version'] != 1
            or value['candidate_schema'] != PIN_SCHEMA or value['candidate_only'] is not True
            or value['released'] is not False or value['run_id'] != support_pin['run_id']
            or value['workload_id'] != 'hotel_reservation'
            or support_pin['workload_id'] != 'hotel_reservation'
            or value['support_pin_fingerprint'] != support_pin['fingerprint']
            or value['recovery_policy'] != 'cleanup-only-never-replay'
            or type(value['initialization_attempt_id']) is not str
            or _UUID.fullmatch(value['initialization_attempt_id']) is None
            or value['initialization_attempt_id'] == '00000000-0000-0000-0000-000000000000'
            or value['fingerprint'] != _fingerprint({key: item for key, item in value.items() if key != 'fingerprint'})):
        raise HotelInitializationOperatorError('Immutable guarded Hotel initialization pin differs.')
    adopted = value['guarded_artifacts']
    if (not isinstance(adopted, Mapping) or adopted.get('fingerprint') != value['guarded_artifact_fingerprint']
            or adopted.get('workload_id') != 'hotel_reservation' or adopted.get('released') is not False
            or not isinstance(adopted.get('baseline'), Mapping)
            or adopted['baseline'].get('binding_fingerprint') != support_pin['artifact_fingerprint']):
        raise HotelInitializationOperatorError('Retained guarded Hotel artifact identity differs.')
    if not cleanup_only:
        if type(artifacts) is not GuardedHotelArtifactBinding:
            raise HotelInitializationOperatorError('Fresh guarded Hotel adoption is required before live work.')
        artifacts.__post_init__()
        if (_canonical(artifacts.metadata()) != _canonical(adopted)
                or artifacts.fingerprint != value['guarded_artifact_fingerprint']):
            raise HotelInitializationOperatorError('Fresh guarded Hotel adoption differs from the immutable pin.')
    return support._copy(dict(value))


def _scope():
    return {
        'scope': 'one-shot-hotel-initialize-and-global-readonly-verify-applications-paused',
        'application_start_allowed': False, 'operator_started_applications': False,
        'operator_executed_benchmark': False, 'public_dispatch_enabled': False,
        'cleanup_policy': 'always-attempt-exact-owned-infrastructure-destruction',
        'recovery_policy': 'cleanup-only-never-replay', 'qualification': _closed_gates(),
    }


def _new_record(pin):
    return {
        'schema_version': 1, 'candidate_schema': OPERATOR_SCHEMA, 'candidate_only': True,
        'released': False, 'run_id': pin['run_id'], 'pin_fingerprint': pin['fingerprint'],
        'initialization_attempt_id': pin['initialization_attempt_id'], 'scope': _scope(),
        'attempt': 1, 'state': 'accepted', 'cloud_identity': None, 'audits': [],
        'support_image_access': None, 'support_snapshot': None, 'support_stage_complete': False,
        'initialization_snapshot': None, 'initialization_complete': False,
        'cleanup_complete': False, 'cleanup_proof': None, 'failure': None, 'cleanup_failure': None,
    }


def _initialization(value, pin):
    if not isinstance(value, Mapping):
        raise HotelInitializationOperatorError('A live guarded Hotel initialization observation is required.')
    expected = {
        'schema_version': 1, 'candidate_only': True, 'released': False,
        'run_id': pin['run_id'], 'initialization_attempt_id': pin['initialization_attempt_id'],
        'guarded_artifact_fingerprint': pin['guarded_artifact_fingerprint'],
        'candidate_schema': 'hotel-guarded-live-initialization-outcome-v1',
        'state': 'initialized-and-globally-verified-applications-paused', 'initialization_executed': True,
        'database_execution_verified': True, 'global_readonly_verification_completed': True,
        'all_application_replicas_zero': True, 'applications_paused': True,
        'application_start_allowed': False, 'runtime_qualified': False,
        'measurement_qualified': False, 'cloud_qualified': False,
        'public_release': False, 'public_dispatch_enabled': False,
    }
    if any(type(value.get(key)) is not type(item) or value[key] != item for key, item in expected.items()):
        raise HotelInitializationOperatorError('Guarded Hotel completion lacks exact paused, non-released live observations.')
    return support._copy(dict(value))


def validate_hotel_initialization_operator_record(value, pin, support_pin):
    """Validate historical evidence only; this never grants execution authority."""
    expected = _new_record(pin)
    fixed = {'schema_version', 'candidate_schema', 'candidate_only', 'released', 'run_id',
             'pin_fingerprint', 'initialization_attempt_id', 'scope'}
    if (not isinstance(value, Mapping) or set(value) != set(expected)
            or any(_canonical(value[key]) != _canonical(expected[key]) for key in fixed)
            or type(value['attempt']) is not int or value['attempt'] < 1
            or type(value['state']) is not str or value['state'] not in _STATES
            or not isinstance(value['audits'], list)
            or any(type(value[key]) is not bool for key in ('support_stage_complete', 'initialization_complete', 'cleanup_complete'))):
        raise HotelInitializationOperatorError('Retained Hotel lifecycle identity or state differs.')
    if value['support_stage_complete'] and not isinstance(value['support_snapshot'], Mapping):
        raise HotelInitializationOperatorError('Retained Hotel support success lacks its historical snapshot.')
    if value['initialization_complete']:
        _initialization(value['initialization_snapshot'], pin)
    elif value['initialization_snapshot'] is not None:
        raise HotelInitializationOperatorError('Incomplete Hotel initialization retains a success snapshot.')
    if value['cleanup_complete']:
        if value['state'] != 'cleanup_complete':
            raise HotelInitializationOperatorError('Hotel cleanup success is outside its complete state.')
        support._cleanup_proof(value['cleanup_proof'], support_pin)
    elif value['cleanup_proof'] is not None:
        raise HotelInitializationOperatorError('Incomplete Hotel cleanup retains a success proof.')
    return support._copy(dict(value))


@dataclass(frozen=True, slots=True)
class HotelInitializationOperatorOutcome:
    exit_code: int
    support_stage_complete: bool
    initialization_complete: bool
    cleanup_complete: bool
    _initialization_json: str | None
    _cleanup_json: str | None
    failure: str | None
    cleanup_failure: str | None

    @property
    def initialization_snapshot(self):
        return json.loads(self._initialization_json) if self._initialization_json is not None else None

    @property
    def cleanup_proof(self):
        return json.loads(self._cleanup_json) if self._cleanup_json is not None else None

    def metadata(self):
        return {key: getattr(self, key) for key in (
            'exit_code', 'support_stage_complete', 'initialization_complete', 'cleanup_complete',
            'initialization_snapshot', 'cleanup_proof', 'failure', 'cleanup_failure',
        )}


def run_guarded_hotel_initialization_operator(
    job, *, lease, pin, initialization_pin, persist, provision=None, fresh_audit=None,
    prepare_runtime=None, support_image_access=None, stage=None, initialize=None,
    destroy_and_prove, artifacts=None, guarded_artifacts=None, cleanup_only=False,
    cleanup_signal_context=None, interrupt_exceptions=(KeyboardInterrupt,),
) -> HotelInitializationOperatorOutcome:
    """Initialize exactly one fresh attempt, or perform registry/SSH-free cleanup.

    There is intentionally no resume mode. A crash or lost gate-release response
    retains the exact attempt and run-wide latches for explicit cleanup-only.
    """
    if type(cleanup_only) is not bool:
        raise HotelInitializationOperatorError('Cleanup-only mode must be an explicit Boolean.')
    if (not isinstance(interrupt_exceptions, tuple) or not interrupt_exceptions
            or any(not isinstance(item, type) or not issubclass(item, BaseException) for item in interrupt_exceptions)):
        raise HotelInitializationOperatorError('Explicit interruption exception classes are required.')
    support._validate_pin(job, pin, artifacts, cleanup_only=cleanup_only)
    initialization_pin = validate_hotel_initialization_pin(initialization_pin, pin, guarded_artifacts, cleanup_only=cleanup_only)
    if _canonical(job.get(PIN_KEY)) != _canonical(initialization_pin):
        raise HotelInitializationOperatorError('Job initialization pin differs from its immutable input.')
    require_acquired_run_lease(lease, pin['run_id'])
    if lease.path.parent.parent.name != '.candidate-support':
        raise HotelInitializationOperatorError('Hotel operators require the private candidate state namespace.')
    if not callable(persist) or not callable(destroy_and_prove):
        raise HotelInitializationOperatorError('Durable persistence and exact cleanup callbacks are required.')
    if not cleanup_only and not all(callable(item) for item in (
        provision, fresh_audit, prepare_runtime, support_image_access, stage, initialize,
    )):
        raise HotelInitializationOperatorError('Fresh provisioning, support and live initialization callbacks are required.')
    pin = support._copy(dict(pin))
    original = job.get(OPERATOR_JOURNAL_KEY)
    if cleanup_only:
        current = validate_hotel_initialization_operator_record(original, initialization_pin, pin)
    else:
        if original is not None or job['resources'] or support.OPERATOR_JOURNAL_KEY in job:
            raise HotelInitializationOperatorError('A new Hotel initializer cannot adopt existing resources or operator history.')
        current = None
    expected_record = current

    def binding():
        require_acquired_run_lease(lease, pin['run_id'])
        support._validate_pin(job, pin, artifacts, cleanup_only=cleanup_only)
        validate_hotel_initialization_pin(job.get(PIN_KEY), pin, guarded_artifacts, cleanup_only=cleanup_only)
        if (_canonical(job.get(PIN_KEY)) != _canonical(initialization_pin)
                or _canonical(job.get(OPERATOR_JOURNAL_KEY)) != _canonical(expected_record)):
            raise HotelInitializationOperatorError('Hotel operator intent changed outside its durable lifecycle.')

    def callback(function):
        binding()
        result = function(job, lease)
        binding()
        return result

    def write(**changes):
        nonlocal current, expected_record
        binding()
        current = expected_record = validate_hotel_initialization_operator_record(
            {**(current or _new_record(initialization_pin)), **changes}, initialization_pin, pin)
        job[OPERATOR_JOURNAL_KEY] = support._copy(current)
        callback(persist)

    if current is None:
        write()  # No cleanup ownership is accepted until initial intent is durable.
    failure = cleanup_failure = initialized = cleanup = None
    support_complete = interrupted = False
    signal_state = None
    try:
        if not cleanup_only:
            write(state='provisioning')
            callback(provision)
            write(state='auditing_cloud')
            initial_audit = support._audit(callback(fresh_audit), pin)
            write(state='preparing_runtime', cloud_identity=initial_audit['cloud_identity'], audits=[initial_audit])
            callback(prepare_runtime)
            write(state='auditing_runtime')
            after_runtime = support._audit(callback(fresh_audit), pin)
            if _canonical(initial_audit['cloud_identity']) != _canonical(after_runtime['cloud_identity']):
                raise HotelInitializationOperatorError('Cloud create identities drifted across runtime preparation.')
            write(state='checking_support_access', audits=[initial_audit, after_runtime])
            access = support._access(callback(support_image_access), pin)
            write(state='staging_support', support_image_access=access)
            ready = support._validated_support(job, artifacts, callback(stage), access)
            write(state='support_observed', support_stage_complete=True, support_snapshot=ready)
            support_complete = True
            write(state='initializing')
            initialized = _initialization(callback(initialize), initialization_pin)
            write(state='initialization_observed', initialization_complete=True, initialization_snapshot=initialized)
    except BaseException as exc:
        interrupted = isinstance(exc, interrupt_exceptions)
        failure = support._error(exc, interrupted)
    finally:
        try:
            with (cleanup_signal_context or support._defer_cleanup_signals)() as signal_state:
                try:
                    write(state='cleanup_pending', failure=(current['failure'] if cleanup_only else failure),
                          attempt=current['attempt'] + (1 if cleanup_only else 0),
                          cleanup_complete=False, cleanup_proof=None, cleanup_failure=None)
                    if support.SUPPORT_STAGE_JOURNAL_KEY in job['resources']:
                        payload = support._copy(job['resources'][support.SUPPORT_STAGE_JOURNAL_KEY])
                        archive = {
                            'schema_version': 1, 'candidate_schema': support.ARCHIVE_SCHEMA,
                            'candidate_only': True, 'run_id': pin['run_id'],
                            'source_resource_key': support.SUPPORT_STAGE_JOURNAL_KEY,
                            'qualification_authority': False, 'payload_sha256': _fingerprint(payload), 'payload': payload,
                        }
                        previous = job.get(support.STAGE_ARCHIVE_KEY)
                        if previous is not None and _canonical(previous) != _canonical(archive):
                            raise HotelInitializationOperatorError('A differing support archive cannot be overwritten.')
                        binding()
                        job[support.STAGE_ARCHIVE_KEY] = archive
                        callback(persist)
                        if (_canonical(job['resources'].get(support.SUPPORT_STAGE_JOURNAL_KEY)) != _canonical(payload)
                                or _canonical(job.get(support.STAGE_ARCHIVE_KEY)) != _canonical(archive)):
                            raise HotelInitializationOperatorError('Support evidence changed during archival.')
                        binding()
                        del job['resources'][support.SUPPORT_STAGE_JOURNAL_KEY]
                        callback(persist)
                except BaseException as exc:
                    cleanup_failure = support._error(exc, isinstance(exc, interrupt_exceptions))
                for attempt in range(2):
                    try:
                        write(state='cleaning')
                    except BaseException as exc:
                        cleanup_failure = cleanup_failure or support._error(exc, isinstance(exc, interrupt_exceptions))
                    try:
                        cleanup = support._cleanup_proof(callback(destroy_and_prove), pin)
                        break
                    except BaseException as exc:
                        cancellation = isinstance(exc, interrupt_exceptions)
                        if cancellation:
                            interrupted = True
                            failure = failure or support._error(exc, True)
                        if not cancellation or attempt == 1:
                            cleanup_failure = cleanup_failure or support._error(exc, cancellation)
                            break
                complete = cleanup is not None and cleanup_failure is None
                number = signal_state.get('number') if isinstance(signal_state, Mapping) else None
                if number in (signal.SIGINT, signal.SIGTERM):
                    interrupted = True
                    failure = failure or {'type': 'OperatorSignal', 'message': f'Signal {number} deferred during cleanup.', 'interrupted': True}
                write(state='cleanup_complete' if complete else 'cleanup_failed', cleanup_complete=complete,
                      cleanup_proof=cleanup if complete else None, cleanup_failure=cleanup_failure,
                      failure=(current['failure'] if cleanup_only and failure is None else failure))
                number = signal_state.get('number') if isinstance(signal_state, Mapping) else None
                if number in (signal.SIGINT, signal.SIGTERM) and failure is None:
                    interrupted = True
                    failure = {'type': 'OperatorSignal', 'message': f'Signal {number} deferred during cleanup.', 'interrupted': True}
                    write(failure=failure)
        except BaseException as exc:
            cleanup_failure = cleanup_failure or support._error(exc, isinstance(exc, interrupt_exceptions))
    number = signal_state.get('number') if isinstance(signal_state, Mapping) else None
    complete = cleanup is not None and cleanup_failure is None
    exit_code = 2 if not complete else (128 + number if number in (signal.SIGINT, signal.SIGTERM) else
                                        130 if interrupted else 1 if failure is not None else 0)
    return HotelInitializationOperatorOutcome(
        exit_code, support_complete, initialized is not None, complete,
        _canonical(initialized) if initialized is not None else None,
        _canonical(cleanup) if complete else None,
        None if failure is None else failure['type'] + ': ' + failure['message'],
        None if cleanup_failure is None else cleanup_failure['type'] + ': ' + cleanup_failure['message'],
    )
