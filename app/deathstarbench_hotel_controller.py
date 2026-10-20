"""One-shot Hotel initialization inside an existing cleanup-owning operator.

This controller never resumes an attempt, provisions resources, or starts
application services. Its caller owns exact infrastructure destruction on every
exit. Local journals remain historical evidence, not reusable permission.
"""

from __future__ import annotations

import json
import time

from . import deathstarbench_distributed as cluster_runtime
from . import deathstarbench_hotel_live as live
from .deathstarbench_candidate_deployment import (
    SUPPORT_STAGE_JOURNAL_KEY, READY_STATE, _context,
    validate_candidate_support_journal,
)
from .deathstarbench_candidate_operator import _audit
from .deathstarbench_candidate_storage_observation import (
    candidate_database_storage_observation_command,
    parse_candidate_database_storage_observation,
)
from .deathstarbench_hotel_image_access import (
    qualify_guarded_hotel_image_access, require_guarded_hotel_image_access,
)
from .deathstarbench_hotel_initialization_artifacts import GuardedHotelArtifactBinding
from .deathstarbench_hotel_initialization_journal import (
    GuardedHotelInitializationJournal, PREFIX, LATCHES,
)
from .deathstarbench_hotel_initialization_stage import (
    build_guarded_hotel_initialization_stage, SCHEDULING_GATE,
)
from .deathstarbench_hotel_runtime import (
    create_guarded_hotel_runtime_plan, observe_guarded_hotel_cluster_identity,
    GuardedHotelDatabaseObservation, validate_saved_guarded_hotel_cluster_observation,
)
from .run_lease import require_acquired_run_lease


STATE_KEY = 'guarded_hotel_initialization'
SCHEMA = 'hotel-guarded-live-initialization-v1'


class HotelControllerError(ValueError):
    """The attempt cannot proceed; its owner must perform cleanup."""


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _copy(value):
    return json.loads(_canonical(value))


def _require(condition, message):
    if not condition:
        raise HotelControllerError(message)


def _base_continuity(observation):
    return {key: value for key, value in observation['continuity'].items()
            if key not in ('initialization_pods', 'initialization_objects')}


def _support_continuity(previous, current):
    """Do not adopt replaced support resources between staging and init."""
    def objects(receipt):
        return {(item['kind'], item['metadata'].get('namespace'), item['metadata']['name']): item
                for item in receipt['inventory']['items']}
    old, new = objects(previous), objects(current)
    for key, item in old.items():
        actual = new.get(key)
        _require(actual is not None and actual['metadata']['uid'] == item['metadata']['uid'],
                 'A support resource identity changed after its completed stage.')
        if key[0] == 'Pod':
            def containers(pod):
                return sorted((row['name'], row['imageID'], row['containerID'], row['restartCount'])
                              for row in pod['status']['containerStatuses'])
            _require(containers(item) == containers(actual),
                     'A support container changed after its completed stage.')


def _typed_observation(value, plan):
    saved = validate_saved_guarded_hotel_cluster_observation(plan, value)
    return observe_guarded_hotel_cluster_identity(
        plan, **{name: saved[name] for name in (
            'server_ca_sha256', 'kube_system_namespace_uid', 'namespace_uid',
            'database_node_name', 'database_node_uid', 'database_filesystem_uuid',
            'initializer_pod_name', 'initializer_pod_uid',
        )}, databases=tuple(GuardedHotelDatabaseObservation(**row) for row in saved['databases']),
    )


def run_guarded_hotel_initialization(
    job, artifacts, *, baseline_artifacts, initialization_attempt_id, lease,
    execute, persist, fresh_audit, checkpoint=None,
    timeout_seconds=480,
) -> dict:
    """Initialize then globally verify once; propagate failures to cleanup.

    ``execute`` is the existing SSH transport, ``fresh_audit(job, lease)`` is
    the provider SDK ownership audit, and ``persist(job)`` durably saves state.
    Only fresh in-process artifact/access objects can reach dispatch. A
    checkpoint callback may interrupt for qualification; it grants no authority.
    """
    _require(type(timeout_seconds) is int and 1 <= timeout_seconds <= 3600,
             'A bounded integer timeout of 1..3600 seconds is required.')
    _require(all(callable(item) for item in (execute, persist, fresh_audit))
             and (checkpoint is None or callable(checkpoint)),
             'Execution, durable persistence and fresh cloud auditing are required.')
    _require(type(artifacts) is GuardedHotelArtifactBinding,
             'A freshly adopted guarded Hotel artifact is required.')
    artifacts.__post_init__()
    require_acquired_run_lease(lease, job.get('id'))
    _require(lease.path.parent.parent.name == '.candidate-support',
             'Hotel initialization requires the private candidate state namespace.')
    _require(STATE_KEY not in job, 'An existing Hotel attempt requires cleanup-only recovery.')
    _require(not any(name.startswith(PREFIX) or name in LATCHES.values()
                     for name in (path.name for path in lease.path.parent.iterdir())),
             'Existing Hotel journal files require cleanup-only recovery.')
    cluster, support_plan, support_stage, identity = _context(
        job, baseline_artifacts, cluster_runtime.distributed_k3s_candidate_plan)
    support = validate_candidate_support_journal(
        job['resources'].get(SUPPORT_STAGE_JOURNAL_KEY), support_plan, support_stage, identity)
    _require(support['state'] == READY_STATE, 'Hotel initialization requires completed paused support staging.')
    plan = create_guarded_hotel_runtime_plan(
        artifacts, support_plan, support_stage, initialization_attempt_id=initialization_attempt_id)
    stage = build_guarded_hotel_initialization_stage(plan)
    roles = identity['role_nodes']
    original_plan = _canonical(job['plan'])
    original_pin = _canonical(job['candidate_support_pin'])
    current = None
    intent = None
    journal_expected = None
    initial_cloud = None
    pinned = None
    executions = {}

    def binding():
        require_acquired_run_lease(lease, plan.run_id)
        _require(original_plan == _canonical(job['plan'])
                 and original_pin == _canonical(job['candidate_support_pin']),
                 'The Hotel provider/plan binding changed during a callback.')
        _, now_plan, now_stage, now_identity = _context(
            job, baseline_artifacts, cluster_runtime.distributed_k3s_candidate_plan)
        _require(now_plan.fingerprint == support_plan.fingerprint
                 and now_stage.fingerprint == support_stage.fingerprint
                 and _canonical(now_identity) == _canonical(identity)
                 and _canonical(job['resources'].get(SUPPORT_STAGE_JOURNAL_KEY)) == _canonical(support),
                 'The Hotel support/runtime/SSH binding changed during a callback.')
        _require(_canonical(job.get(STATE_KEY)) == _canonical(current),
                 'The Hotel controller evidence changed outside its lifecycle.')
        if intent is not None and journal_expected is not None:
            _require(_canonical(intent.metadata()) == _canonical(journal_expected),
                     'The durable Hotel journal or dispatch latches changed during a callback.')

    def checked(function, *args, **kwargs):
        binding()
        result = function(*args, **kwargs)
        binding()
        return result

    def write(**changes):
        nonlocal current
        binding()
        current = {**(current or {
            'schema_version': 1, 'candidate_schema': SCHEMA,
            'candidate_only': True, 'released': False, 'run_id': plan.run_id,
            'initialization_attempt_id': initialization_attempt_id,
            'plan_fingerprint': plan.fingerprint, 'stage_fingerprint': stage.fingerprint,
            'guarded_artifact_fingerprint': artifacts.fingerprint,
            'resume_policy': 'cleanup-only-never-replay',
            'application_start_allowed': False, 'public_dispatch_enabled': False,
            'runtime_qualified': False, 'measurement_qualified': False,
            'cloud_qualified': False, 'public_release': False,
            'initialization_executed': False, 'database_execution_verified': False,
            'global_readonly_verification_completed': False,
        }), **changes}
        job[STATE_KEY] = _copy(current)
        checked(persist, job)

    def single_attempt(value, command, **kwargs):
        # The shared SSH transport defaults to retrying exit 255. A lost
        # response may mean the gate was already released, so even transport
        # failures must return to cleanup instead of dispatching twice.
        return execute(value, command, transport_attempts=1,
                       transport_retry_delay_seconds=0, **kwargs)

    def remote(role, command, timeout=180):
        return checked(cluster_runtime._execute, single_attempt, job, cluster.host(role),
                       command, timeout=timeout)

    def save_intent(**changes):
        nonlocal journal_expected
        # Called only immediately after our own synchronous journal operation.
        # Every later callback must preserve this exact history and latch set.
        journal_expected = intent.metadata()
        write(journal=journal_expected, **changes)

    def audit():
        nonlocal initial_cloud
        result = _audit(checked(fresh_audit, job, lease), job['candidate_support_pin'])
        cloud = result['cloud_identity']
        prior = job.get('candidate_support_operator', {}).get('cloud_identity')
        if initial_cloud is None:
            _require(prior is None or _canonical(prior) == _canonical(cloud),
                     'Fresh cloud ownership differs from the support operator.')
            initial_cloud = _copy(cloud)
        _require(_canonical(initial_cloud) == _canonical(cloud),
                 'Fresh cloud identity changed during Hotel initialization.')
        write(last_cloud_audit=result)

    def storage():
        volume = cluster_runtime._parse_plan_database_volume_attestation(
            remote('database', cluster_runtime._database_volume_attestation_command(
                cluster, plan.storage.filesystem_uuid), timeout=300), cluster)
        _require(cluster_runtime._same_database_volume_identity(identity['database_volume'], volume),
                 'The exact provider database volume identity changed.')
        observed = parse_candidate_database_storage_observation(
            remote('database', candidate_database_storage_observation_command(plan.storage), timeout=300),
            plan.storage)
        write(last_storage_observation=observed)

    def observe(include=True, *, timeout=180):
        output = remote('control', live.hotel_initialization_observe_command(
            plan, stage, roles, include_initialization=include), timeout=timeout)
        result = live.parse_hotel_initialization_observation(
            output, plan, stage, roles, include_initialization=include)
        _require(result['continuity']['server_ca_sha256'] == 'sha256:' + identity['server_ca_sha256'],
                 'The live Kubernetes server CA differs from the prepared cluster.')
        _support_continuity(support['support_attestation'], result['support_attestation'])
        if pinned is not None:
            _require(_canonical(result['continuity']) == _canonical(pinned['continuity']),
                     'Live cluster, endpoint, storage or Pod identity continuity changed.')
        for mode, pod in result['initialization_pods'].items():
            execution = {key: pod[key] for key in ('container_id', 'started_at')}
            if mode in executions:
                _require(_canonical(execution) == _canonical(executions[mode]),
                         'Hotel container execution identity changed within the same Pod.')
            elif execution['container_id'] is not None:
                _require(isinstance(execution['started_at'], str) and bool(execution['started_at']),
                         'Hotel container execution is missing its start identity.')
                executions[mode] = execution
        write(last_observation=result, container_executions=_copy(executions))
        return result

    def mark(name):
        if checkpoint is not None:
            checked(checkpoint, name)

    def completed(observation, mode):
        pod = observation['initialization_pods'][mode]
        _require(pod['phase'] != 'Failed' and
                 (pod['exit_code'] is None or type(pod['exit_code']) is int and pod['exit_code'] == 0),
                 'Hotel initializer/verifier exited unsuccessfully; cleanup is required.')
        return pod['phase'] == 'Succeeded' and type(pod['exit_code']) is int and pod['exit_code'] == 0

    try:
        write(state='preflight')
        # Establish ownership before registry work or the first SSH command.
        audit()
        access = checked(qualify_guarded_hotel_image_access, artifacts)
        require_guarded_hotel_image_access(access, artifacts)
        write(image_access_fingerprint=access.fingerprint)
        intent = GuardedHotelInitializationJournal(plan, stage, lease=lease)
        save_intent()
        audit()
        storage()
        before = observe(False)
        write(state='creating-blocked-pods')
        remote('control', live.hotel_initialization_create_command(plan, stage, roles, before), timeout=300)
        # A lost creation response propagates to cleanup without another create.
        audit()
        storage()
        blocked = observe()
        _require(_canonical(_base_continuity(before)) == _canonical(_base_continuity(blocked)),
                 'Live support identity changed while creating blocked initialization Pods.')
        pinned = _copy(blocked)
        intent.record_blocked_pods(_typed_observation(blocked['cluster_observation'], plan),
                                   verifier_pod_uid=blocked['verifier_pod_uid'])
        save_intent(state='blocked-pods-recorded')
        for mode, prefix in (('initialize-empty', 'initializer'), ('verify', 'verifier')):
            audit()
            storage()
            observed = observe()
            pod = observed['initialization_pods'][mode]
            _require(pod['spec'].get('schedulingGates') == [{'name': SCHEDULING_GATE}]
                     and pod['phase'] == 'Pending' and pod['exit_code'] is None,
                     'The exact one-shot Pod must remain blocked before dispatch.')
            if mode == 'initialize-empty':
                verifier = observed['initialization_pods']['verify']
                _require(verifier['spec'].get('schedulingGates') == [{'name': SCHEDULING_GATE}]
                         and verifier['phase'] == 'Pending' and verifier['exit_code'] is None,
                         'The verifier was dispatched before initialization.')
            else:
                _require(completed(observed, 'initialize-empty'),
                         'Global verification requires the same successful initializer Pod.')
            require_guarded_hotel_image_access(access, artifacts)
            intent.record_dispatch_intent(mode, image_access=access)
            state = 'initialize-intent-recorded' if mode == 'initialize-empty' else 'verify-intent-recorded'
            save_intent(state=state,
                        **({'initialization_executed': None} if mode == 'initialize-empty' else {}))
            mark(prefix + '-intent')
            remote('control', live.hotel_initialization_release_command(
                plan, stage, roles, observed, mode))
            write(state=prefix + '-dispatched')
            deadline = time.monotonic() + timeout_seconds
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError('Hotel one-shot Pod did not complete within its observation deadline.')
                observed = observe(timeout=min(180, max(1, int(remaining))))
                if time.monotonic() >= deadline:
                    raise TimeoutError('Hotel one-shot Pod did not complete within its observation deadline.')
                if completed(observed, mode):
                    break
                time.sleep(min(1, max(0, deadline - time.monotonic())))
            audit()
            storage()
            observed = observe()
            _require(completed(observed, mode), 'Completed Hotel Pod evidence changed before log collection.')
            uid = observed['initialization_pods'][mode]['uid']
            output = remote('control', live.hotel_initialization_logs_command(plan, stage, mode, uid))
            after = observe()
            _require(completed(after, mode), 'Completed Hotel Pod evidence changed after log collection.')
            intent.record_exit_observation(mode, pod_uid=uid, exit_code=0, output=output)
            save_intent(state=prefix + '-exit-recorded',
                        **({'initialization_executed': True} if mode == 'initialize-empty' else {}))
            mark(prefix + '-exit')
        audit()
        storage()
        last = observe()
        _require(all(completed(last, mode) for mode in ('initialize-empty', 'verify')),
                 'Both unchanged Hotel Pods must retain successful completion.')
        write(state='initialized-and-globally-verified-applications-paused',
              database_execution_verified=True, global_readonly_verification_completed=True,
              applications_paused=True, all_application_replicas_zero=True,
              journal=intent.metadata())
        return _copy(current)
    except BaseException as exc:
        # Never erase/repair latches. If persistence or lease ownership was
        # lost, preserve the existing conservative intent and original error.
        if current is not None:
            try:
                write(state='failed-cleanup-only', failure_type=type(exc).__name__,
                      database_execution_verified=False, global_readonly_verification_completed=False)
            except BaseException:
                pass
        raise
