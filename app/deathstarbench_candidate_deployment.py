"""Leased, retry-aware support staging for unreleased Hotel/Media candidates.

The caller owns provider provisioning, fresh cloud ownership checks and exact
cleanup. This internal operation consumes an already prepared K3s cluster. It
never starts application services, initializers, load drivers, or measurement,
and is deliberately absent from public benchmark dispatch.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import asdict
import json
import re
from typing import Any

from . import deathstarbench_distributed as cluster_runtime
from .deathstarbench_candidate_artifacts import CandidateArtifactBinding
from .deathstarbench_candidate_runtime import (
    CandidateRuntimePlan, _canonical, _closed_gates, _fingerprint,
    create_candidate_runtime_plan,
)
from .deathstarbench_candidate_stage import (
    CandidateSupportStageBundle, build_candidate_support_stage,
)
from .deathstarbench_candidate_storage import (
    candidate_database_storage_prepare_command,
    parse_candidate_database_storage_attestation,
    validate_candidate_database_storage_attestation,
)
from .deathstarbench_candidate_storage_observation import (
    candidate_database_storage_observation_command,
    parse_candidate_database_storage_observation,
    validate_saved_candidate_database_storage_observation,
)
from .deathstarbench_candidate_k3s import (
    candidate_phase_apply_command,
    candidate_phase_reconciliation_command,
    parse_candidate_phase_reconciliation,
    validate_saved_candidate_phase_reconciliation,
    candidate_support_readiness_command,
    parse_candidate_support_attestation,
    validate_saved_candidate_support_attestation,
)
from .deathstarbench_contract import DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
from .k3s_runtime import (
    cluster_nodes_readiness_command, system_services_readiness_command,
    server_ca_sha256_read_command,
)
from .resource_inventory import ROLE_NODE_INVENTORY_KEY
from .run_lease import RunLease, RunLeaseError, require_acquired_run_lease


SUPPORT_STAGE_JOURNAL_KEY = 'deathstarbench_candidate_support_stage'
JOURNAL_SCHEMA = 'candidate-support-stage-journal-v1'
READY_STATE = 'support_ready_applications_paused'
_SHA256 = re.compile(r'[0-9a-f]{64}\Z')
_STATES = frozenset({
    'preflight_only', 'preparing_storage', 'storage_prepared', 'revalidating',
    'applying_phase', 'reconciling_phase', 'attesting_support', READY_STATE,
})


class CandidateDeploymentError(ValueError):
    """The support stage lacks exact ownership, sequencing, or live evidence."""


def _require_lease(job: Mapping[str, Any], lease: RunLease):
    try:
        require_acquired_run_lease(lease, job.get('id'))
    except RunLeaseError as exc:
        raise CandidateDeploymentError('An acquired, still-held lease for this exact persisted run is required.') from exc


def _provider_run_binding(job, resources, provider):
    """Bind persisted provider ownership to the job, not just its topology.

    These are locally validated contracts, not fresh provider API evidence. The
    caller must still audit real cloud tags/resources before SSH dispatch.
    """
    run_id = job.get('id')
    if provider == 'azure':
        from .providers import azure
        actual = resources.get('azure_resource_group_tags')
        expected = azure._tags(run_id, 'resource-group')
        if not isinstance(actual, Mapping) or _canonical(dict(actual)) != _canonical(expected):
            raise CandidateDeploymentError('Azure resource-group owner tags do not belong to this run.')
        identity = {'kind': 'azure-resource-group-tags', 'value': dict(actual)}
    elif provider == 'gcp':
        from .providers import gcp
        prefix = resources.get('gcp_resource_prefix')
        if prefix != gcp._resource_name(run_id, 'dsb'):
            raise CandidateDeploymentError('GCP candidate resource prefix does not belong to this run.')
        identity = {'kind': 'gcp-resource-prefix', 'value': prefix}
    elif provider == 'aws':
        from .providers import aws
        state = resources.get(aws.AWS_DSB_GRAPH_KEY)
        contract = state.get('contract') if isinstance(state, Mapping) else None
        if not isinstance(contract, Mapping) or contract.get('job_id') != run_id:
            raise CandidateDeploymentError('AWS candidate graph contract does not belong to this run.')
        identity = {'kind': 'aws-graph-contract', 'value': run_id}
    elif provider == 'oci':
        from .providers import oci
        contract = resources.get(oci.CONTRACT_KEY)
        if not isinstance(contract, Mapping) or contract.get('job_id') != run_id:
            raise CandidateDeploymentError('OCI candidate infrastructure contract does not belong to this run.')
        identity = {'kind': 'oci-infrastructure-contract', 'value': run_id}
    else:
        raise CandidateDeploymentError('Support staging requires an exact supported provider owner.')
    return {'run_id': run_id, **identity}


def _context(job, artifacts, plan_factory):
    if not isinstance(job, MutableMapping) or not isinstance(job.get('resources'), MutableMapping):
        raise CandidateDeploymentError('A mutable persisted candidate job/resource mapping is required.')
    resources = job['resources']
    # A released Social workload or one-shot execution must never become a
    # support-stage candidate through a new artifact argument.
    if any(resources.get(key) is not None for key in (
        cluster_runtime.WORKLOAD_JOURNAL_KEY,
        DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    )) or job.get('results') not in (None, []):
        raise CandidateDeploymentError('Existing workload execution/results cannot be adopted into support staging.')
    cluster = plan_factory(resources)
    provider_owner = _provider_run_binding(job, resources, cluster.provider)
    runtime = resources.get(cluster_runtime.RUNTIME_JOURNAL_KEY)
    cluster_runtime._validate_existing_journal(runtime, cluster)
    if (
        not isinstance(runtime, Mapping)
        or type(runtime.get('schema_version')) is not int
        or runtime.get('state') != 'cluster_ready'
        or not isinstance(runtime.get('server_ca_sha256'), str)
        or not _SHA256.fullmatch(runtime['server_ca_sha256'])
        or runtime['server_ca_sha256'] == '0' * 64
    ):
        raise CandidateDeploymentError('The exact pinned K3s runtime must be cluster_ready.')
    database_volume = dict(runtime['database_volume'])
    if 'lun' in database_volume and type(database_volume['lun']) is not int:
        raise CandidateDeploymentError('The provider database LUN must be an integer, not a Boolean alias.')
    database_volume['filesystem_uuid'] = database_volume['filesystem_uuid'].casefold()
    plan = create_candidate_runtime_plan(
        artifacts, run_id=job['id'], provider=cluster.provider,
        topology_fingerprint=cluster.topology_fingerprint,
        application_architecture=cluster.host('application').architecture,
        load_generator_ip=cluster.load_generator_private_ip,
        filesystem_uuid=database_volume['filesystem_uuid'],
    )
    stage = build_candidate_support_stage(plan)
    roles = {host.role: host.node_name for host in cluster.hosts}
    hosts = [asdict(host) for host in cluster.hosts] + [asdict(cluster.load_generator)]
    route_keys = {host.host_key for host in (*cluster.hosts, cluster.load_generator)}
    route_keys.update(host.jump_host_key for host in (*cluster.hosts, cluster.load_generator) if host.jump_host_key is not None)
    identity = {
        'provider': cluster.provider,
        'provider_run_owner': provider_owner,
        'topology_fingerprint': cluster.topology_fingerprint,
        'role_node_inventory_sha256': _fingerprint(resources[ROLE_NODE_INVENTORY_KEY]),
        'hosts': hosts, 'role_nodes': roles,
        'ssh_routes': {key: resources.get(key) for key in sorted(route_keys)},
        'ssh_user': resources.get('ssh_user'),
        'server_ca_sha256': runtime['server_ca_sha256'],
        'database_volume': database_volume,
    }
    return cluster, plan, stage, identity


def _new_journal(plan, stage, identity):
    return {
        'schema_version': 1, 'candidate_schema': JOURNAL_SCHEMA,
        'candidate_only': True, 'released': False,
        'run_id': plan.run_id, 'workload_id': plan.artifacts.workload_id,
        'namespace': plan.bundle.namespace,
        'snapshot': plan.snapshot(), 'snapshot_fingerprint': plan.fingerprint,
        'stage': stage.metadata(), 'cluster_identity': json.loads(_canonical(identity)),
        'state': 'preflight_only', 'applied_phases': [], 'pending_phase': None,
        'phase_receipts': {}, 'pending_receipt': None,
        'storage_preparation': None, 'storage_observation': None,
        'support_attestation': None,
        'qualification': _closed_gates(),
    }


def _require_completed_resource_uids(receipts, observation):
    """Preserve all completed static identities across phases and final proof."""
    inventory = observation.get('inventory') if isinstance(observation, Mapping) else None
    if not isinstance(inventory, Mapping) or not isinstance(inventory.get('items'), list):
        raise CandidateDeploymentError('UID continuity requires the validated Kubernetes inventory.')
    current = {
        (item['kind'], item['metadata'].get('namespace'), item['metadata']['name']): item['metadata']['uid']
        for item in inventory['items']
    }
    for receipt in receipts.values():
        for item in receipt['resources']:
            key = (item['kind'], item['namespace'], item['name'])
            if current.get(key) != item['uid']:
                raise CandidateDeploymentError('A completed support resource was replaced or disappeared before the next phase/readiness.')


def validate_candidate_support_journal(value, plan, stage, identity):
    """Restore historical evidence only; live readiness is always re-attested."""
    if not isinstance(value, Mapping):
        raise CandidateDeploymentError('The support-stage journal must be an object.')
    expected = _new_journal(plan, stage, identity)
    dynamic = {
        'state', 'applied_phases', 'pending_phase', 'phase_receipts',
        'pending_receipt', 'storage_preparation', 'storage_observation', 'support_attestation',
    }
    if set(value) != set(expected) or any(
        _canonical(value[key]) != _canonical(expected[key]) for key in set(expected) - dynamic
    ):
        raise CandidateDeploymentError('Support-stage ownership/snapshot/cluster identity drifted.')
    phase_names = tuple(name for name, _ in stage.phases)
    state, applied, pending = value['state'], value['applied_phases'], value['pending_phase']
    if not isinstance(state, str) or state not in _STATES:
        raise CandidateDeploymentError('Support-stage state is invalid.')
    if not isinstance(applied, list) or tuple(applied) != phase_names[:len(applied)]:
        raise CandidateDeploymentError('Support-stage phases are not the exact ordered prefix.')
    if pending is not None and (len(applied) == len(phase_names) or pending != phase_names[len(applied)]):
        raise CandidateDeploymentError('Support-stage pending phase conflicts with its ordered prefix.')
    if (pending is None) != (value['pending_receipt'] is None):
        raise CandidateDeploymentError('A pending phase and its pre-dispatch receipt must remain paired.')
    roles = identity['role_nodes']
    receipts = value['phase_receipts']
    if not isinstance(receipts, Mapping) or set(receipts) != set(applied):
        raise CandidateDeploymentError('Completed support phases lack their exact reconciliation receipts.')
    for phase in applied:
        receipt = validate_saved_candidate_phase_reconciliation(receipts[phase], plan, stage, phase, roles)
        if receipt['complete'] is not True:
            raise CandidateDeploymentError('A completed support phase has incomplete evidence.')
        if receipt['namespace_uid'] != receipts['namespace']['namespace_uid']:
            raise CandidateDeploymentError('Completed phases crossed their namespace UID continuity.')
    if value['pending_receipt'] is not None:
        if pending is None:
            raise CandidateDeploymentError('A pending receipt has no pending phase.')
        receipt = validate_saved_candidate_phase_reconciliation(value['pending_receipt'], plan, stage, pending, roles)
        if applied and receipt['namespace_uid'] != receipts['namespace']['namespace_uid']:
            raise CandidateDeploymentError('Pending phase crossed the completed namespace UID.')
    preparation = value['storage_preparation']
    if preparation is not None:
        validate_candidate_database_storage_attestation(preparation, plan.storage)
    observation = value['storage_observation']
    if observation is not None:
        validate_saved_candidate_database_storage_observation(observation, plan.storage)
    if state in {'preflight_only', 'preparing_storage'}:
        if applied or pending is not None or receipts or preparation is not None or observation is not None:
            raise CandidateDeploymentError('Support-stage progress exists before storage preparation.')
    elif preparation is None:
        raise CandidateDeploymentError('Support staging lacks its initial full-budget storage receipt.')
    if state == 'storage_prepared' and (applied or pending is not None):
        raise CandidateDeploymentError('Storage-only state contains workload progress.')
    if state in {'applying_phase', 'reconciling_phase'} and (pending is None or observation is None):
        raise CandidateDeploymentError('A phase mutation/reconciliation lacks its live storage observation.')
    if state in {'attesting_support', READY_STATE} and (tuple(applied) != phase_names or pending is not None or observation is None):
        raise CandidateDeploymentError('Support readiness lacks all completed phases and storage observation.')
    attestation = value['support_attestation']
    if state == READY_STATE:
        attestation = validate_saved_candidate_support_attestation(attestation, plan, stage, roles)
        if attestation['namespace_uid'] != receipts['namespace']['namespace_uid']:
            raise CandidateDeploymentError('Support readiness crossed the completed namespace UID.')
        _require_completed_resource_uids(receipts, attestation)
    elif attestation is not None:
        raise CandidateDeploymentError('Support readiness is retained outside the ready state.')
    return json.loads(_canonical(dict(value)))


def _same_resource_identity(prior, current, *, allow_additions=False):
    if prior['namespace_uid'] is not None and prior['namespace_uid'] != current['namespace_uid']:
        raise CandidateDeploymentError('The run-owned namespace was replaced or disappeared.')
    previous = {(item['kind'], item['namespace'], item['name']): item['uid'] for item in prior['resources']}
    observed = {(item['kind'], item['namespace'], item['name']): item['uid'] for item in current['resources']}
    if (not allow_additions and set(previous) != set(observed)) or any(observed.get(key) != uid for key, uid in previous.items()):
        raise CandidateDeploymentError('Previously observed support resources were replaced or disappeared.')


def prepare_candidate_support_stage(
    job: MutableMapping[str, Any], artifacts: CandidateArtifactBinding, *,
    lease: RunLease, execute: Callable[..., str],
    persist: Callable[[MutableMapping[str, Any]], Any],
    emit: Callable[[MutableMapping[str, Any], str, str], Any] | None = None,
    _plan_factory=cluster_runtime.distributed_k3s_candidate_plan,
) -> dict[str, Any]:
    """Prepare support services only, with durable phase/response-loss handling.

    The caller must retain the run lease until this synchronous operation and
    its cleanup policy finish. It must independently audit cloud ownership;
    persisted provider inventory and this SSH boundary are not a cloud audit.
    """
    _require_lease(job, lease)
    if not callable(persist):
        raise CandidateDeploymentError('Durable persistence is required before any remote work.')
    cluster, plan, stage, identity = _context(job, artifacts, _plan_factory)
    existing = job['resources'].get(SUPPORT_STAGE_JOURNAL_KEY)
    current = _new_journal(plan, stage, identity) if existing is None else validate_candidate_support_journal(existing, plan, stage, identity)
    journal_bound = existing is not None

    def check_binding():
        _require_lease(job, lease)
        fresh_cluster, fresh_plan, fresh_stage, fresh_identity = _context(job, artifacts, _plan_factory)
        if fresh_plan.fingerprint != plan.fingerprint or fresh_stage.fingerprint != stage.fingerprint or _canonical(fresh_identity) != _canonical(identity):
            raise CandidateDeploymentError('Provider/runtime/SSH binding changed during support staging.')
        if journal_bound and _canonical(job['resources'].get(SUPPORT_STAGE_JOURNAL_KEY)) != _canonical(current):
            raise CandidateDeploymentError('The support-stage journal was removed or changed during a callback.')
        return fresh_cluster

    def write(**changes):
        nonlocal current, journal_bound
        check_binding()
        candidate = {**current, **changes}
        current = validate_candidate_support_journal(candidate, plan, stage, identity)
        job['resources'][SUPPORT_STAGE_JOURNAL_KEY] = json.loads(_canonical(current))
        journal_bound = True
        persist(job)
        check_binding()

    def remote(key, command, *, timeout, stdin_text=None):
        fresh_cluster = check_binding()
        result = cluster_runtime._execute(execute, job, fresh_cluster.host(key), command, timeout=timeout, stdin_text=stdin_text)
        check_binding()
        return result

    def event(message):
        if emit is not None:
            emit(job, 'Candidate support stage', message)

    roles = identity['role_nodes']

    def reconcile(phase):
        output = remote('control', candidate_phase_reconciliation_command(plan, stage, phase, roles), timeout=300)
        return parse_candidate_phase_reconciliation(output, plan, stage, phase, roles)

    # A completed journal is historical. Revoke ready before the first SSH,
    # including transport failure/cancellation during cluster re-attestation.
    if existing is not None and current['state'] not in {'preflight_only', 'preparing_storage'}:
        write(state='revalidating', storage_observation=None, support_attestation=None)
    else:
        write()
    event('Re-attesting the exact K3s cluster, CA, and database volume.')
    remote('control', cluster_nodes_readiness_command(cluster.expected_nodes), timeout=420)
    remote('control', system_services_readiness_command(cluster.host('control').node_name), timeout=420)
    ca = remote('control', server_ca_sha256_read_command(), timeout=120).strip()
    if ca != identity['server_ca_sha256']:
        raise CandidateDeploymentError('The K3s CA changed before support-stage mutation.')
    volume_output = remote('database', cluster_runtime._database_volume_attestation_command(cluster, plan.storage.filesystem_uuid), timeout=300)
    volume = cluster_runtime._parse_plan_database_volume_attestation(volume_output, cluster)
    if not cluster_runtime._same_database_volume_identity(identity['database_volume'], volume):
        raise CandidateDeploymentError('The provider database volume changed before support staging.')
    # Guard foreign/started application Kubernetes state before *any* storage
    # mutation, not merely before applying the first manifest phase.
    namespace_before = reconcile('namespace')
    if 'namespace' in current['phase_receipts']:
        _same_resource_identity(current['phase_receipts']['namespace'], namespace_before)
    elif namespace_before['namespace_uid'] is not None and current['pending_phase'] != 'namespace':
        raise CandidateDeploymentError('An unjournaled existing namespace cannot be adopted into support staging.')
    if current['storage_preparation'] is None:
        if namespace_before['namespace_uid'] is not None:
            raise CandidateDeploymentError('Initial storage preparation cannot be replayed beneath an existing stage namespace.')
        write(state='preparing_storage')
        event('Preparing the eight run-owned candidate MongoDB directories; applications remain blocked.')
        output = remote('database', candidate_database_storage_prepare_command(plan.storage), timeout=300)
        preparation = parse_candidate_database_storage_attestation(output, plan.storage)
        write(state='storage_prepared', storage_preparation=preparation)
    # Never replay preparation once support pods may have opened the databases.
    observed_output = remote('database', candidate_database_storage_observation_command(plan.storage), timeout=300)
    observation = parse_candidate_database_storage_observation(observed_output, plan.storage)
    write(storage_observation=observation)
    for phase in tuple(current['applied_phases']):
        receipt = reconcile(phase)
        if receipt['complete'] is not True:
            raise CandidateDeploymentError('A previously completed support phase is no longer complete.')
        _same_resource_identity(current['phase_receipts'][phase], receipt)
    for phase, payload in stage.phases:
        if phase in current['applied_phases']:
            continue
        before = reconcile(phase)
        _require_completed_resource_uids(current['phase_receipts'], before)
        if current['pending_receipt'] is not None:
            _same_resource_identity(current['pending_receipt'], before, allow_additions=True)
        write(state='reconciling_phase', pending_phase=phase, pending_receipt=before)
        if before['complete'] is not True:
            write(state='applying_phase')
            event(f'Applying candidate support-stage phase {phase}; application replicas remain zero.')
            remote('control', candidate_phase_apply_command(plan, stage, phase, roles), timeout=600, stdin_text=payload)
            after = reconcile(phase)
            _require_completed_resource_uids(current['phase_receipts'], after)
            _same_resource_identity(before, after, allow_additions=True)
            if after['complete'] is not True:
                raise CandidateDeploymentError('Support-phase apply did not produce complete reconciled state.')
        else:
            after = before
        receipts = {**current['phase_receipts'], phase: after}
        applied = [*current['applied_phases'], phase]
        next_state = 'attesting_support' if len(applied) == len(stage.phases) else 'revalidating'
        write(state=next_state, applied_phases=applied, pending_phase=None, pending_receipt=None, phase_receipts=receipts)
    write(state='attesting_support')
    event('Attesting support pods and proving every application remains paused.')
    output = remote('control', candidate_support_readiness_command(plan, stage, roles), timeout=2100)
    attestation = parse_candidate_support_attestation(output, plan, stage, roles)
    write(state=READY_STATE, support_attestation=attestation)
    event('Candidate support services are ready and applications are currently paused; this operation did not start applications, initializers, or benchmarks.')
    return json.loads(_canonical(current))
