"""One original leased Hotel lifecycle; execution and cleanup fail separately.

The initialize callback wraps prepare_guarded_hotel_initialization, NEVER its
cleanup-owning operator. startup(job, lease, *, initialization_plan,
initialization_stage) wraps the startup controller. The strict provider adapter
owns read-only preflight, durable exclusive ownership seal/readback BEFORE any
quiesce/revoke/detach/delete, and independent absence. Its persistence path must
checkpoint native ownership before safe history, independently during cleanup.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import re
import signal

from . import deathstarbench_candidate_operator as support
from . import deathstarbench_candidate_deployment as deployment
from . import deathstarbench_distributed as distributed
from . import deathstarbench_hotel_initialization_operator as initialization
from . import deathstarbench_hotel_startup_ownership as ownership
from .deathstarbench_candidate_runtime import _canonical, _fingerprint
from .deathstarbench_hotel_initialization_stage import build_guarded_hotel_initialization_stage
from .deathstarbench_hotel_runtime import create_guarded_hotel_runtime_plan
from .deathstarbench_hotel_startup_artifacts import StartupHotelArtifactBinding
from .run_lease import require_acquired_run_lease

PIN_KEY = 'hotel_startup_pin'
PIN_SCHEMA = 'hotel-startup-operator-pin-v1'
OPERATOR_JOURNAL_KEY = 'hotel_startup_operator'
OPERATOR_SCHEMA = 'hotel-startup-operator-v1'
ARCHIVE_KEY = 'hotel_startup_archive'
_PIN_KEYS = frozenset({'schema_version', 'candidate_schema', 'candidate_only', 'released', 'run_id',
    'workload_id', 'support_pin_fingerprint', 'initialization_pin_fingerprint', 'startup_artifact_fingerprint',
    'startup_artifacts', 'initialization_attempt_id', 'startup_attempt_id', 'recovery_policy', 'fingerprint'})


class HotelStartupOperatorError(ValueError):
    """Startup identity or lifecycle custody is invalid."""


def _require(condition, message='Hotel startup immutable identity or custody differs.'):
    if not condition:
        raise HotelStartupOperatorError(message)


def _support_pin(support_pin):
    _require(isinstance(support_pin, Mapping))
    job = {'id': support_pin.get('run_id'), 'plan': support_pin.get('infrastructure_plan'),
           'resources': {}, '_persist_state': False, 'results': [], support.PIN_KEY: support_pin}
    support._validate_pin(job, support_pin, None, cleanup_only=True)
    _require(support_pin['workload_id'] == 'hotel_reservation'
             and type(job['id']) is str and re.fullmatch(r'[0-9a-f]{12}', job['id']) is not None)


def make_hotel_startup_pin(run_id: str, support_pin: dict, initialization_pin: dict,
                           artifacts, *, startup_attempt_id: str) -> dict:
    _support_pin(support_pin)
    initialization.validate_hotel_initialization_pin(initialization_pin, support_pin, cleanup_only=True)
    _require(type(artifacts) is StartupHotelArtifactBinding, 'Fresh startup Hotel artifact adoption is required.')
    artifacts.__post_init__()
    value = {'schema_version': 1, 'candidate_schema': PIN_SCHEMA, 'candidate_only': True,
        'released': False, 'run_id': run_id, 'workload_id': 'hotel_reservation',
        'support_pin_fingerprint': support_pin['fingerprint'],
        'initialization_pin_fingerprint': initialization_pin['fingerprint'],
        'startup_artifact_fingerprint': artifacts.fingerprint, 'startup_artifacts': artifacts.metadata(),
        'initialization_attempt_id': initialization_pin['initialization_attempt_id'],
        'startup_attempt_id': startup_attempt_id, 'recovery_policy': 'cleanup-only-never-replay'}
    return validate_hotel_startup_pin({**value, 'fingerprint': _fingerprint(value)},
        support_pin, initialization_pin, artifacts)


def validate_hotel_startup_pin(value: dict, support_pin: dict, initialization_pin: dict, artifacts=None) -> dict:
    """Validate identity; artifacts=None is metadata-only, not execution access."""
    _support_pin(support_pin)
    initialization.validate_hotel_initialization_pin(initialization_pin, support_pin, cleanup_only=True)
    _require(isinstance(value, Mapping) and set(value) == _PIN_KEYS)
    _require(type(value['schema_version']) is int and value['schema_version'] == 1
             and value['candidate_schema'] == PIN_SCHEMA and value['candidate_only'] is True
             and value['released'] is False and value['workload_id'] == 'hotel_reservation'
             and value['run_id'] == support_pin['run_id']
             and value['support_pin_fingerprint'] == support_pin['fingerprint']
             and value['initialization_pin_fingerprint'] == initialization_pin['fingerprint']
             and value['initialization_attempt_id'] == initialization_pin['initialization_attempt_id']
             and value['recovery_policy'] == 'cleanup-only-never-replay'
             and type(value['startup_attempt_id']) is str
             and initialization._UUID.fullmatch(value['startup_attempt_id']) is not None
             and value['startup_attempt_id'] not in ('00000000-0000-0000-0000-000000000000', value['initialization_attempt_id'])
             and ownership._digest(value['startup_artifact_fingerprint'])
             and value['fingerprint'] == _fingerprint({k: v for k, v in value.items() if k != 'fingerprint'}))
    adopted = value['startup_artifacts']
    from .deathstarbench_hotel_startup_artifacts import ADOPTION_SCHEMA
    _require(isinstance(adopted, Mapping) and adopted.get('candidate_schema') == ADOPTION_SCHEMA
             and adopted.get('fingerprint') == value['startup_artifact_fingerprint']
             and adopted.get('workload_id') == 'hotel_reservation' and adopted.get('released') is False
             and isinstance(adopted.get('baseline'), Mapping)
             and adopted['baseline'].get('binding_fingerprint') == support_pin['artifact_fingerprint'])
    if artifacts is not None:
        _require(type(artifacts) is StartupHotelArtifactBinding, 'Original startup artifact capability is required.')
        artifacts.__post_init__()
        _require(artifacts.fingerprint == value['startup_artifact_fingerprint']
                 and _canonical(artifacts.metadata()) == _canonical(adopted))
    return ownership._copy(dict(value))


@dataclass(frozen=True, slots=True)
class HotelStartupOperatorOutcome:
    execution_outcome: str
    cleanup_outcome: str
    error: dict | None
    cleanup_error: dict | None
    exit_code: int


def _error(code, exc=None):
    # Class names and exception text are untrusted too. Never stringify them.
    return {'code': code, 'interrupted': isinstance(exc, (KeyboardInterrupt, SystemExit))}


def _startup(value, pin):
    expected = {'schema_version': 1, 'candidate_schema': 'hotel-live-startup-outcome-v1',
        'candidate_only': True, 'run_id': pin['run_id'], 'startup_attempt_id': pin['startup_attempt_id'],
        'initialization_attempt_id': pin['initialization_attempt_id'],
        'state': 'startup-and-service-identities-observed', 'startup_execution_observed': True,
        'service_identity_observed': True}
    _require(isinstance(value, Mapping) and all(type(value.get(k)) is type(v) and value[k] == v for k, v in expected.items()),
             'Startup lacks the exact original completed controller observation.')
    for key in ('released', 'application_start_allowed', 'application_started', 'application_ready',
                'startup_executed', 'initializer_authorized', 'runtime_authorized', 'startup_authorized',
                'runtime_qualified', 'measurement_qualified', 'dataset_qualified', 'cloud_qualified',
                'public_release', 'public_dispatch_enabled', 'semantic_correctness_observed'):
        _require(value.get(key) is False)


def run_hotel_startup_operator(job, *, lease, pin, initialization_pin, startup_pin,
    persist, provision, fresh_audit, prepare_runtime, support_image_access, stage,
    initialize, startup, destroy_and_prove, artifacts, guarded_artifacts,
    startup_artifacts, cleanup_signal_context=None) -> HotelStartupOperatorOutcome:
    """Execute once, never resume; always attempt independently sound teardown."""
    support._validate_pin(job, pin, artifacts, cleanup_only=False)
    initialization.validate_hotel_initialization_pin(initialization_pin, pin, guarded_artifacts)
    startup_pin = validate_hotel_startup_pin(startup_pin, pin, initialization_pin, startup_artifacts)
    require_acquired_run_lease(lease, pin['run_id'])
    _require(lease.path.parent.parent.name == '.candidate-support')
    _require(all(callable(f) for f in (persist, provision, fresh_audit, prepare_runtime,
        support_image_access, stage, initialize, startup, destroy_and_prove)))
    _require(not job['resources'] and not any(k in job for k in (
        OPERATOR_JOURNAL_KEY, support.OPERATOR_JOURNAL_KEY, initialization.OPERATOR_JOURNAL_KEY,
        ownership.CHECKPOINT_KEY, 'guarded_hotel_initialization', 'hotel_application_startup', ARCHIVE_KEY,
        ownership.NOT_REQUIRED_KEY)),
        'A fresh startup invocation cannot replay old resources, intent or history.')
    pin, initialization_pin = ownership._copy(pin), ownership._copy(initialization_pin)
    # Metadata roots validate independently of source capabilities and history.
    roots = ownership._roots(job, pin, initialization_pin, startup_pin)
    current = None

    def cleanup_binding():
        require_acquired_run_lease(lease, roots['run_id'])
        return ownership.bind_hotel_startup_ownership_checkpoint(job, support_pin=pin,
            initialization_pin=initialization_pin, startup_pin=startup_pin)

    def execution_binding():
        cleanup_binding()
        support._validate_pin(job, pin, artifacts, cleanup_only=False)
        initialization.validate_hotel_initialization_pin(job.get(initialization.PIN_KEY), pin, guarded_artifacts)
        validate_hotel_startup_pin(job.get(PIN_KEY), pin, initialization_pin, startup_artifacts)
        _require(_canonical(job.get(OPERATOR_JOURNAL_KEY)) == _canonical(current),
                 'Execution journal changed outside its original lifecycle.')

    def callback(function, **kwargs):
        execution_binding()
        result = function(job, lease, **kwargs)
        execution_binding()
        return result

    def write(state):
        nonlocal current
        execution_binding()
        current = {**(current or {'schema_version': 1, 'candidate_schema': OPERATOR_SCHEMA,
            'candidate_only': True, 'released': False, 'run_id': roots['run_id'],
            'pin_fingerprint': startup_pin['fingerprint'],
            'initialization_attempt_id': initialization_pin['initialization_attempt_id'],
            'startup_attempt_id': startup_pin['startup_attempt_id']}), 'state': state}
        job[OPERATOR_JOURNAL_KEY] = ownership._copy(current)
        callback(persist)

    job[ownership.CHECKPOINT_KEY] = ownership.make_hotel_startup_ownership_checkpoint(
        job, support_pin=pin, initialization_pin=initialization_pin, startup_pin=startup_pin)
    failure = cleanup_failure = None
    cleanup_complete = False
    accepted_persisted = not_required = False
    signals = None

    def latch_terminal_failure(code, exc):
        nonlocal failure
        observed = _error(code, exc)
        failure = {**(failure or observed),
            'interrupted': bool(observed['interrupted'] or (failure and failure['interrupted']))}

    def observe_terminal_signal():
        nonlocal failure
        number = signals.get('number') if isinstance(signals, Mapping) else None
        if number in (signal.SIGINT, signal.SIGTERM):
            failure = {**(failure or {'code': 'execution-interrupted'}), 'interrupted': True}
        return number

    def terminal_record():
        return {**(current or {}),
            'state': 'cleanup_complete' if cleanup_complete else 'cleanup_not_required' if not_required else 'cleanup_failed',
            'execution_outcome': 'failed' if failure else 'completed',
            'cleanup_outcome': 'completed' if cleanup_complete else 'not_required' if not_required else 'failed',
            'error': failure, 'cleanup_error': cleanup_failure}

    try:
        write('accepted')  # Store checkpoints this before allowing any provision.
        accepted_persisted = True
        write('provisioning')
        callback(provision)
        write('auditing_cloud')
        audit = support._audit(callback(fresh_audit), pin)
        job[ownership.CHECKPOINT_KEY] = ownership.record_hotel_startup_ownership_audit(cleanup_binding(), audit)
        write('preparing_runtime')
        callback(prepare_runtime)
        write('auditing_runtime')
        after_runtime = support._audit(callback(fresh_audit), pin)
        _require(_canonical(audit['cloud_identity']) == _canonical(after_runtime['cloud_identity']),
                 'Original cloud identity changed during runtime preparation.')
        job[ownership.CHECKPOINT_KEY] = ownership.record_hotel_startup_ownership_audit(cleanup_binding(), after_runtime)
        write('checking_support_access')
        access = support._access(callback(support_image_access), pin)
        write('staging_support')
        support._validated_support(job, artifacts, callback(stage), access)
        write('support_observed')
        write('initializing')
        initialized = callback(initialize)
        initialization._initialization(initialized, initialization_pin)
        _require(_canonical(job.get('guarded_hotel_initialization')) == _canonical(initialized),
                 'Startup requires the retained original lower initializer result, not a replayed copy.')
        write('initialization_observed')
        # Re-derive only offline identity after the actual lower callback. This
        # does not infer execution/completion, which Task4 independently proves.
        _, support_plan, support_stage, _ = deployment._context(
            job, artifacts, distributed.distributed_k3s_candidate_plan)
        initialization_plan = create_guarded_hotel_runtime_plan(guarded_artifacts,
            support_plan, support_stage, initialization_attempt_id=initialization_pin['initialization_attempt_id'])
        initialization_stage = build_guarded_hotel_initialization_stage(initialization_plan)
        write('starting')
        started = callback(startup, initialization_plan=initialization_plan,
                           initialization_stage=initialization_stage)
        _startup(started, startup_pin)
        _require(_canonical(job.get('hotel_application_startup')) == _canonical(started),
                 'Startup result differs from the retained original controller record.')
        write('startup_observed')
    except BaseException as exc:
        failure = _error('execution-failed', exc)

    # Stop all execution callbacks permanently. Cleanup has no artifact adoption,
    # source-file, registry, Kubernetes or execution-history dependency.
    try:
        with (cleanup_signal_context or support._defer_cleanup_signals)() as signals:
            try:
                cleanup_binding()
                projected = ownership.project_hotel_startup_job(job)
                archive = {key: projected.get(key, {}) for key in (
                    'guarded_hotel_initialization', 'hotel_application_startup', OPERATOR_JOURNAL_KEY)}
                archive[support.SUPPORT_STAGE_JOURNAL_KEY] = projected['resources'].get(support.SUPPORT_STAGE_JOURNAL_KEY, {})
                job[ARCHIVE_KEY] = archive
                persist(job, lease)
                cleanup_binding()
            except BaseException as exc:
                failure = failure or _error('evidence-publication-failed', exc)
            try:
                checkpoint = cleanup_binding()
                if not checkpoint['contract_recorded']:
                    _require(accepted_persisted, 'Initial checkpoint publication was not confirmed.')
                    job[ownership.NOT_REQUIRED_KEY] = ownership.make_hotel_startup_cleanup_not_required(
                        job, support_pin=pin, initialization_pin=initialization_pin, startup_pin=startup_pin)
                    not_required = True
                    failure = failure or _error('execution-stopped-before-create')
                else:
                    _require(not checkpoint['unresolved_creates'],
                             'Unresolved original ownership blocks provider cleanup.')
                    proof = destroy_and_prove(job, lease)
                    cleanup_binding()
                    support._cleanup_proof(proof, pin)
                    cleanup_complete = True
            except BaseException as exc:
                cleanup_failure = _error('cleanup-failed', exc)
            # Final diagnostics are best-effort only for cleanup, mandatory for
            # execution success. Never rerun the failed serializer to delete.
            try:
                cleanup_binding()
                observe_terminal_signal()
                job[OPERATOR_JOURNAL_KEY] = terminal_record()
                persist(job, lease)
                cleanup_binding()
            except BaseException as exc:
                latch_terminal_failure('evidence-publication-failed', exc)
    except BaseException as exc:
        latch_terminal_failure('execution-failed', exc)
        cleanup_failure = cleanup_failure or _error('cleanup-failed', exc)
    number = observe_terminal_signal()
    # Context exit and the first terminal persistence may themselves fail or
    # observe a deferred signal. Reconcile once, only AFTER both boundaries.
    # This never re-enters teardown or makes evidence a deletion prerequisite.
    try:
        settled = terminal_record()
        if _canonical(job.get(OPERATOR_JOURNAL_KEY)) != _canonical(settled):
            job[OPERATOR_JOURNAL_KEY] = settled
            cleanup_binding()
            persist(job, lease)
            cleanup_binding()
    except BaseException as exc:
        latch_terminal_failure('evidence-publication-failed', exc)
        # Publication can remain unavailable: preserve failed live/returned
        # outcome, but do not loop or pretend a stale durable row was fixed.
        job[OPERATOR_JOURNAL_KEY] = terminal_record()
    exit_code = (2 if not cleanup_complete and not not_required else 128 + number if number in (signal.SIGINT, signal.SIGTERM)
                 else 130 if failure and failure['interrupted'] else 1 if failure else 0)
    return HotelStartupOperatorOutcome('failed' if failure else 'completed',
        'completed' if cleanup_complete else 'not_required' if not_required else 'failed', failure, cleanup_failure, exit_code)
