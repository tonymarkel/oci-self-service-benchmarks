"""Private, one-shot Hotel initialization, never application/benchmark release.

The caller owns the cloud lifecycle and must retain the lease through cleanup.
Every remote mutation follows fresh cloud/storage/API observations. Durable
exclusive intent precedes each single gate-removal request; uncertain responses
are cleanup-only, including after the database may already have been seeded.
"""

from __future__ import annotations

from collections.abc import Mapping
import json
import os

from . import deathstarbench_candidate_deployment as support
from . import deathstarbench_distributed as cluster_runtime
from .deathstarbench_candidate_k3s import _object_uids
from .deathstarbench_candidate_operator import _audit
from .deathstarbench_candidate_runtime import _canonical
from .deathstarbench_candidate_storage_observation import (
    candidate_database_storage_observation_command,
    parse_candidate_database_storage_observation,
)
from .deathstarbench_hotel_image_access import require_guarded_hotel_image_access
from .deathstarbench_hotel_initialization_journal import (
    GuardedHotelInitializationJournal, PREFIX, LATCHES,
)
from .deathstarbench_hotel_initialization_stage import build_guarded_hotel_initialization_stage
from .deathstarbench_hotel_runtime import (
    create_guarded_hotel_runtime_plan, observe_guarded_hotel_cluster_identity,
    GuardedHotelDatabaseObservation,
)
from .run_lease import require_acquired_run_lease


JOB_KEY = 'guarded_hotel_initialization'
SCHEMA = 'hotel-guarded-live-initialization-outcome-v1'
SUCCESS = 'initialized-and-globally-verified-applications-paused'


class GuardedHotelControllerError(ValueError):
    """Uncertain ownership, identity, execution or evidence requires cleanup."""


def _require(condition, message):
    if not condition:
        raise GuardedHotelControllerError(message)


def _copy(value):
    return json.loads(_canonical(value))


def prepare_guarded_hotel_initialization(
    job, guarded_artifacts, *, baseline_artifacts, lease, execute, persist,
    image_access, fresh_audit, expected_cloud_identity, initialization_attempt_id,
    emit=None, checkpoint=None,
):
    """Initialize once and globally verify, leaving all ten apps at zero.

    New invocations only. Neither a saved outcome nor even an unlatched old
    journal permits recovery dispatch. Use the cleanup-owning operator for
    every live invocation, including explicit cleanup-only after hard exits.
    """
    # Import lazily so the old support-only module never depends on this path.
    from .deathstarbench_hotel_live_k3s import (
        guarded_hotel_observation_command, parse_guarded_hotel_live_observation,
    )

    require_acquired_run_lease(lease, job.get('id'))
    _require(lease.path.parent.parent.name == '.candidate-support'
             and job.get('_persist_state') is False and job.get('results') in (None, []),
             'Hotel initialization requires the exact private leased candidate namespace.')
    _require(all(callable(callback) for callback in (execute, persist, fresh_audit)),
             'Durable persistence, SSH and fresh cloud auditing are required.')
    _require(JOB_KEY not in job and not any(
        name.startswith(PREFIX) or name in LATCHES.values()
        for name in os.listdir(lease.path.parent)),
        'A previous Hotel controller attempt requires cleanup-only, never replay.')
    cluster, original_plan, original_stage, context = support._context(
        job, baseline_artifacts, cluster_runtime.distributed_k3s_candidate_plan)
    ready = support.validate_candidate_support_journal(
        job['resources'].get(support.SUPPORT_STAGE_JOURNAL_KEY),
        original_plan, original_stage, context)
    _require(ready['state'] == support.READY_STATE,
             'Hotel initialization requires the exact completed support-only stage.')
    plan = create_guarded_hotel_runtime_plan(
        guarded_artifacts, original_plan, original_stage,
        initialization_attempt_id=initialization_attempt_id)
    stage = build_guarded_hotel_initialization_stage(plan)
    require_guarded_hotel_image_access(image_access, guarded_artifacts)
    _require(isinstance(expected_cloud_identity, Mapping) and bool(expected_cloud_identity),
             'An exact independently audited cloud create identity is required.')
    cloud_identity = _copy(dict(expected_cloud_identity))
    pinned_inputs = _canonical({key: job.get(key) for key in (
        'candidate_support_pin', 'hotel_initialization_pin', 'plan')})
    original_context = _canonical(context)
    original_support = _canonical(ready)
    current = None

    def binding():
        require_acquired_run_lease(lease, plan.run_id)
        _require(_canonical({key: job.get(key) for key in (
            'candidate_support_pin', 'hotel_initialization_pin', 'plan')}) == pinned_inputs,
            'Hotel immutable operator inputs changed during a callback.')
        fresh_cluster, fresh_plan, fresh_stage, fresh_context = support._context(
            job, baseline_artifacts, cluster_runtime.distributed_k3s_candidate_plan)
        _require(fresh_plan.fingerprint == original_plan.fingerprint
                 and fresh_stage.fingerprint == original_stage.fingerprint
                 and _canonical(fresh_context) == original_context
                 and _canonical(job['resources'].get(support.SUPPORT_STAGE_JOURNAL_KEY)) == original_support,
                 'Hotel provider, runtime, support or SSH identity changed during a callback.')
        _require(_canonical(job.get(JOB_KEY)) == _canonical(current),
                 'Hotel live-controller evidence changed outside its lifecycle.')
        require_guarded_hotel_image_access(image_access, guarded_artifacts)
        return fresh_cluster

    def callback(function, *arguments):
        binding()
        result = function(*arguments)
        binding()
        return result

    def write(**changes):
        nonlocal current
        binding()
        current = _copy({**(current or {}), **changes})
        job[JOB_KEY] = _copy(current)
        callback(persist, job)

    def event(message):
        if emit is not None:
            callback(emit, job, 'Guarded Hotel initialization', message)

    def remote(role, command, *, timeout=600):
        fresh_cluster = binding()
        result = cluster_runtime._execute(execute, job, fresh_cluster.host(role),
                                          command, timeout=timeout)
        binding()
        return result

    def prerequisites():
        audited = _audit(callback(fresh_audit, job, lease), job['candidate_support_pin'])
        _require(_canonical(audited['cloud_identity']) == _canonical(cloud_identity),
                 'Fresh cloud create identities changed before Hotel execution.')
        output = remote('database', cluster_runtime._database_volume_attestation_command(
            cluster, plan.storage.filesystem_uuid), timeout=300)
        volume = cluster_runtime._parse_plan_database_volume_attestation(output, cluster)
        _require(cluster_runtime._same_database_volume_identity(context['database_volume'], volume),
                 'The provider database volume changed before Hotel execution.')
        output = remote('database', candidate_database_storage_observation_command(plan.storage), timeout=300)
        storage_observation = parse_candidate_database_storage_observation(output, plan.storage)
        write(last_cloud_audit=audited, last_storage_observation=storage_observation)

    identity = None
    latest = None

    def observe(operation='observe', mode=None):
        nonlocal latest
        kwargs = dict(operation=operation, mode=mode, expected_identity=identity)
        output = remote('control', guarded_hotel_observation_command(
            plan, stage, context['role_nodes'], context['server_ca_sha256'], **kwargs))
        if operation == 'release':
            checkpoint_at('initialize-response-returned' if mode == 'initialize-empty'
                          else 'verify-response-returned')
        latest = parse_guarded_hotel_live_observation(
            output, plan, stage, context['role_nodes'], context['server_ca_sha256'],
            expected_identity=identity)
        write(last_observation=latest)
        return latest

    def checkpoint_at(name):
        if checkpoint is not None:
            callback(checkpoint, name, job)

    journal = GuardedHotelInitializationJournal(plan, stage, lease=lease)
    write(schema_version=1, candidate_schema=SCHEMA, candidate_only=True, released=False,
          run_id=plan.run_id, initialization_attempt_id=initialization_attempt_id,
          plan_fingerprint=plan.fingerprint, stage_fingerprint=stage.fingerprint,
          guarded_artifact_fingerprint=guarded_artifacts.fingerprint,
          state='preflight', initialization_executed=False, database_execution_verified=False,
          global_readonly_verification_completed=False, applications_paused=True,
          all_application_replicas_zero=True, application_start_allowed=False,
          runtime_qualified=False, measurement_qualified=False, cloud_qualified=False,
          public_release=False, public_dispatch_enabled=False,
          resume_policy='cleanup-only-no-replay', journal=journal.metadata())
    try:
        prerequisites()
        before = observe()
        previous_uids = _object_uids(ready['support_attestation']['inventory'])
        now_uids = _object_uids(before['support_inventory'])
        _require(all(now_uids.get(key) == value for key, value in previous_uids.items()),
                 'Support objects were replaced after support readiness.')
        _require(_canonical(before['identity']['support_containers'])
                 == _canonical(ready['support_attestation']['support_pods']),
                 'Support containers restarted or changed after support readiness.')
        _require(not before['pods'], 'Initialization Pods must not preexist this one-shot attempt.')
        identity = before['identity']
        write(state='creating-gated-pods')
        # Exact create only; a lost response is never reconciled into a retry.
        created = observe('create')
        identity = created['identity']
        _require(set(created['pods']) == {'initialize-empty', 'verify'}
                 and all(row['state'] == 'gated' for row in created['pods'].values()),
                 'Both exact standalone Pods must remain blocked before dispatch.')
        database_node = next(row for row in created['nodes'] if row['role'] == 'database')
        db_fields = ('component', 'pvc_name', 'pvc_uid', 'pv_name', 'pv_uid', 'pod_name', 'pod_uid')
        cluster_observation = observe_guarded_hotel_cluster_identity(
            plan, server_ca_sha256='sha256:' + context['server_ca_sha256'],
            kube_system_namespace_uid=created['kube_system_namespace_uid'],
            namespace_uid=created['namespace_uid'], database_node_name=database_node['name'],
            database_node_uid=database_node['uid'], database_filesystem_uuid=plan.storage.filesystem_uuid,
            initializer_pod_name=plan.initializer_pod_name,
            initializer_pod_uid=created['pods']['initialize-empty']['uid'],
            databases=tuple(GuardedHotelDatabaseObservation(**{key: row[key] for key in db_fields})
                            for row in created['databases']))
        journal.record_blocked_pods(cluster_observation, verifier_pod_uid=created['pods']['verify']['uid'])
        write(state='gated-pods-pinned', journal=journal.metadata())
        for mode in ('initialize-empty', 'verify'):
            prerequisites()
            blocked = observe()
            _require(blocked['pods'][mode]['state'] == 'gated',
                     'The one-shot Pod is no longer blocked before dispatch intent.')
            journal.record_dispatch_intent(mode, image_access=image_access)
            write(state=mode + '-dispatch-intent', journal=journal.metadata(),
                  initialization_executed=None if mode == 'initialize-empty' else True)
            # One remote request, never retry after SSH/API response loss.
            observe('release', mode)
            checkpoint_at('initialize-dispatched' if mode == 'initialize-empty' else 'verify-dispatched')
            finished = observe('wait', mode)
            row = finished['pods'][mode]
            _require(row['state'] == 'terminated' and type(row['exit_code']) is int and row['exit_code'] == 0,
                     'The exact one-shot Pod did not produce a verified successful termination.')
            journal.record_exit_observation(mode, pod_uid=row['uid'], exit_code=row['exit_code'],
                                            output=finished['logs'][mode])
            write(state=mode + '-verified', journal=journal.metadata(), initialization_executed=True,
                  database_execution_verified=True,
                  global_readonly_verification_completed=mode == 'verify')
            checkpoint_at('initialize-verified' if mode == 'initialize-empty' else 'verify-verified')
        prerequisites()
        observe()
        write(state=SUCCESS)
        event('One-shot Hotel initialization and separate global read-only verification completed; all application replicas remain zero. Infrastructure will now be destroyed.')
        return _copy(current)
    except BaseException as exc:
        # Keep exclusive journal files and exact Pod identities, even when the
        # transport did not return. The outer operator must still attempt cleanup.
        try:
            write(state='failed-cleanup-only', failure_type=type(exc).__name__)
        except BaseException:
            pass
        raise
