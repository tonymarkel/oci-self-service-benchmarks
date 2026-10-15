"""Offline guarded Hotel identity contracts, not initialization authority.

The original candidate support plan remains unchanged and has all application
replicas stopped. This separate identity binds its exact support stage to the
newly adopted guarded image and deterministic dataset. Neither a plan nor an
observation executes Kubernetes/MongoDB, verifies live ownership, authorizes an
initializer, or allows application startup. There is no serialized adoption.

The existing Go image accepts only run ID and dataset fingerprint. It does not
enforce the richer cluster/storage/Pod identity recorded here; that requires a
separate, future live observation and controller boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
from typing import Any, Mapping

from .deathstarbench_candidate_artifacts import CandidateArtifactBinding
from .deathstarbench_candidate_runtime import CandidateRuntimePlan
from .deathstarbench_candidate_stage import (
    CandidateSupportStageBundle, validate_candidate_support_stage,
)
from .deathstarbench_hotel_initialization import (
    COMPONENTS, DATASET_REVISION, RECIPE_REVISION,
    _canonical, _digest, _fingerprint, validate_hotel_dataset_attestation,
)
from .deathstarbench_hotel_initialization_artifacts import (
    ADOPTION_SCHEMA, GuardedHotelArtifactBinding,
)


SNAPSHOT_SCHEMA = 'hotel-guarded-runtime-preflight-v1'
OBSERVATION_SCHEMA = 'hotel-guarded-cluster-observation-v1'
RUNTIME_REVISION = 'hotel-guarded-initialization-runtime-identity-candidate-v1'
NAMESPACE = 'deathstarbench-hotel'
_UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')
_DNS_LABEL = re.compile(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z')
_PLATFORMS = {'x86_64': 'linux/amd64', 'aarch64': 'linux/arm64'}
_DATABASES = tuple('mongodb-' + component for component in COMPONENTS)


class GuardedHotelRuntimeError(ValueError):
    """Guarded Hotel identities are malformed, crossed, or overclaim authority."""


def _require(condition, message):
    if not condition:
        raise GuardedHotelRuntimeError(message)


def _uuid(value, label):
    _require(type(value) is str and _UUID.fullmatch(value) is not None
             and value != '00000000-0000-0000-0000-000000000000',
             label + ' requires a nonzero canonical lowercase UUID.')
    return value


def _name(value, label, *, subdomain=False):
    _require(type(value) is str and len(value) <= (253 if subdomain else 63)
             and all(_DNS_LABEL.fullmatch(part) is not None for part in value.split('.'))
             and (subdomain or '.' not in value), label + ' must be a bounded canonical Kubernetes name.')
    return value


def _closed_gates():
    return {name: False for name in (
        'anonymous_layer_pull', 'live_cluster_identity', 'initializer_authorized',
        'initialization_executed', 'dataset', 'application_start', 'cloud_runtime',
        'measurement', 'denied_edges', 'cleanup', 'public_release',
    )}


def _checked_inputs(artifacts, support_plan, support_stage):
    _require(type(artifacts) is GuardedHotelArtifactBinding,
             'A freshly adopted, separate guarded Hotel artifact binding is required.')
    _require(type(support_plan) is CandidateRuntimePlan
             and type(support_plan.artifacts) is CandidateArtifactBinding,
             'An exact original candidate support runtime plan is required.')
    _require(type(support_stage) is CandidateSupportStageBundle,
             'An exact original support-only stage is required.')
    try:
        # Recheck factory proofs even though all retained objects are frozen.
        artifacts.__post_init__()
        support_plan.artifacts.__post_init__()
        support_plan.__post_init__()
        checked_stage = validate_candidate_support_stage(support_plan, support_stage)
        adopted = artifacts.metadata()
        original = support_plan.artifacts.metadata()
        _require(artifacts.workload_id == support_plan.artifacts.workload_id == 'hotel_reservation',
                 'Guarded Hotel cannot adopt Media or Social support identities.')
        _require(adopted['candidate_schema'] == ADOPTION_SCHEMA
                 and adopted['recipe_revision'] == RECIPE_REVISION
                 and adopted['dataset_revision'] == DATASET_REVISION,
                 'Guarded Hotel artifact recipe or dataset revisions differ.')
        _require(adopted['baseline']['binding_fingerprint'] == support_plan.artifacts.fingerprint,
                 'Guarded Hotel and support plan require the same freshly adopted original baseline.')
        reused = {key: value for key, value in original['image_bindings'].items()
                  if key != 'hotel-reservation'}
        _require(_canonical(adopted['reused_images']) == _canonical(reused),
                 'Guarded Hotel support/driver images differ from the original support plan.')
        _require(support_plan.storage.namespace == support_plan.bundle.namespace == checked_stage.namespace == NAMESPACE,
                 'Guarded Hotel requires the exact audited namespace.')
        _require(support_plan.storage.database_names == _DATABASES,
                 'Guarded Hotel requires the exact eight audited database directories.')
        _require(support_plan.bundle.application_architecture in _PLATFORMS,
                 'Guarded Hotel requires an exact supported application architecture.')
        validate_hotel_dataset_attestation(
            adopted['dataset_attestation'], expected_source_fingerprint=adopted['source_fingerprint'],
        )
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        if isinstance(exc, GuardedHotelRuntimeError):
            raise
        raise GuardedHotelRuntimeError('Guarded Hotel cannot adopt crossed support/artifact evidence.') from exc
    return checked_stage, adopted


def _snapshot_identity(artifacts, support_plan, support_stage, attempt):
    stage, adopted = _checked_inputs(artifacts, support_plan, support_stage)
    _uuid(attempt, 'Initialization attempt identity')
    platform = _PLATFORMS[support_plan.bundle.application_architecture]
    selected = adopted['app']['platforms'][platform]
    return {
        'schema_version': 1, 'candidate_schema': SNAPSHOT_SCHEMA,
        'runtime_revision': RUNTIME_REVISION, 'candidate_only': True, 'released': False,
        'scope': 'offline-guarded-image-and-support-identity-only-no-execution-authority',
        'workload_id': 'hotel_reservation', 'run_id': support_plan.run_id,
        'provider': support_plan.provider, 'topology_fingerprint': support_plan.topology_fingerprint,
        'namespace': NAMESPACE, 'application_architecture': support_plan.bundle.application_architecture,
        'support_plan_fingerprint': support_plan.fingerprint,
        'support_stage_fingerprint': stage.fingerprint,
        'support_stage_manifest_sha256': stage.rendered_manifest_sha256,
        'original_artifact_fingerprint': support_plan.artifacts.fingerprint,
        'guarded_artifact_fingerprint': artifacts.fingerprint,
        'recipe_revision': adopted['recipe_revision'], 'generator_revision': adopted['generator_revision'],
        'dataset_revision': adopted['dataset_revision'], 'source_fingerprint': adopted['source_fingerprint'],
        'dataset_fingerprint': adopted['dataset_attestation']['dataset_fingerprint'],
        'application_platform': platform, 'application_image': selected['image'],
        'application_config_digest': selected['config_digest'],
        'application_index_image': adopted['app']['index_image'],
        'reused_images': adopted['reused_images'], 'storage': support_plan.storage.metadata(),
        'initialization_attempt_id': attempt,
        'initializer_pod_name': 'hotel-initialize-' + attempt.replace('-', ''),
        'verifier_pod_name': 'hotel-verify-' + attempt.replace('-', ''),
        'live_identity_observed': False,
        'required_live_identity': [
            'server-ca-sha256', 'kube-system-namespace-uid', 'workload-namespace-uid',
            'database-node-name-and-uid', 'database-filesystem-uuid',
            'eight-bound-pvc-pv-and-database-pod-identities', 'initializer-pod-name-and-uid',
        ],
        'policy': {
            'initialization_attempts': 1, 'initializer_restart_policy': 'Never',
            'initialization_replay_allowed': False, 'automatic_retry_allowed': False,
            'reset_allowed': False, 'repair_allowed': False, 'upsert_allowed': False,
            'interrupted_or_ambiguous_state': 'cleanup-only-no-initializer-replay',
            'replaced_cluster_namespace_storage_or_pod': 'cleanup-only-new-run-required',
            'application_start': 'forbidden-until-separate-global-readonly-verification',
            'go_identity_scope': 'run-and-dataset-only-not-cluster-storage-pod-or-attempt',
            'live_observation_is_authority': False,
        },
        'qualification': _closed_gates(),
    }


@dataclass(frozen=True, slots=True)
class GuardedHotelRuntimePlan:
    """Immutable offline identity, never an initialization/start capability."""

    artifacts: GuardedHotelArtifactBinding = field(repr=False)
    support_plan: CandidateRuntimePlan = field(repr=False)
    support_stage: CandidateSupportStageBundle = field(repr=False)
    initialization_attempt_id: str
    snapshot_json: str = field(repr=False)
    fingerprint: str
    candidate_only: bool = field(init=False, default=True)
    released: bool = field(init=False, default=False)
    initializer_authorized: bool = field(init=False, default=False)
    runtime_authorized: bool = field(init=False, default=False)
    application_start_allowed: bool = field(init=False, default=False)

    def __post_init__(self):
        expected = _snapshot_identity(self.artifacts, self.support_plan, self.support_stage,
                                      self.initialization_attempt_id)
        _require(self.snapshot_json == _canonical(expected) and self.fingerprint == _fingerprint(expected),
                 'Guarded Hotel plan conflicts with its exact offline derivation.')
        _require(self.candidate_only is True and all(getattr(self, flag) is False for flag in (
            'released', 'initializer_authorized', 'runtime_authorized', 'application_start_allowed',
        )), 'Guarded Hotel plan cannot promote any execution gate.')

    @property
    def run_id(self):
        return self.support_plan.run_id

    @property
    def provider(self):
        return self.support_plan.provider

    @property
    def topology_fingerprint(self):
        return self.support_plan.topology_fingerprint

    @property
    def namespace(self):
        return NAMESPACE

    @property
    def application_architecture(self):
        return self.support_plan.bundle.application_architecture

    @property
    def storage(self):
        return self.support_plan.storage

    @property
    def application_image(self):
        return self.snapshot()['application_image']

    @property
    def initializer_pod_name(self):
        return 'hotel-initialize-' + self.initialization_attempt_id.replace('-', '')

    @property
    def verifier_pod_name(self):
        return 'hotel-verify-' + self.initialization_attempt_id.replace('-', '')

    def snapshot(self) -> dict[str, Any]:
        return json.loads(self.snapshot_json)

    def initialization_environment(self) -> dict[str, str]:
        """Existing image identity fields only; these do not authorize execution."""
        return {
            'HOTEL_CANDIDATE_RUN_ID': self.run_id,
            'HOTEL_CANDIDATE_CONTRACT_SHA256': self.snapshot()['dataset_fingerprint'],
        }


def create_guarded_hotel_runtime_plan(
    artifacts: GuardedHotelArtifactBinding,
    support_plan: CandidateRuntimePlan,
    support_stage: CandidateSupportStageBundle,
    *, initialization_attempt_id: str,
) -> GuardedHotelRuntimePlan:
    """Bind a new image to an exact stopped support stage, without promotion."""
    identity = _snapshot_identity(artifacts, support_plan, support_stage, initialization_attempt_id)
    return GuardedHotelRuntimePlan(artifacts, support_plan, support_stage, initialization_attempt_id,
                                   _canonical(identity), _fingerprint(identity))


def validate_guarded_hotel_runtime_plan(plan: GuardedHotelRuntimePlan) -> GuardedHotelRuntimePlan:
    """Reconstruct a typed offline plan; serialized identities are not accepted."""
    _require(type(plan) is GuardedHotelRuntimePlan, 'An exact guarded Hotel runtime plan is required.')
    checked = create_guarded_hotel_runtime_plan(
        plan.artifacts, plan.support_plan, plan.support_stage,
        initialization_attempt_id=plan.initialization_attempt_id,
    )
    _require(plan == checked, 'Guarded Hotel runtime plan conflicts with its offline derivation.')
    return checked


def validate_guarded_hotel_runtime_snapshot(value, plan: GuardedHotelRuntimePlan) -> dict[str, Any]:
    """Compare saved identity only against a freshly constructed typed plan."""
    _require(type(plan) is GuardedHotelRuntimePlan, 'An exact guarded Hotel plan is required.')
    plan.__post_init__()
    try:
        _require(isinstance(value, Mapping) and _canonical(dict(value)) == plan.snapshot_json,
                 'Saved guarded Hotel identity differs from fresh offline preflight.')
    except (TypeError, ValueError, RecursionError) as exc:
        if isinstance(exc, GuardedHotelRuntimeError):
            raise
        raise GuardedHotelRuntimeError('Saved guarded Hotel identity is not finite JSON.') from exc
    return plan.snapshot()


@dataclass(frozen=True, slots=True)
class GuardedHotelDatabaseObservation:
    """Caller-observed object identifiers; syntax validation is not live proof."""

    component: str
    pvc_name: str
    pvc_uid: str
    pv_name: str
    pv_uid: str
    pod_name: str
    pod_uid: str

    def __post_init__(self):
        _require(type(self.component) is str and self.component in _DATABASES,
                 'Hotel database observation requires an audited database component.')
        _name(self.pvc_name, 'Database PVC name')
        _name(self.pv_name, 'Database PV name')
        _require(self.pvc_name == self.component and self.pv_name == NAMESPACE + '-' + self.component,
                 'Hotel PVC/PV names differ from the exact audited storage binding.')
        _name(self.pod_name, 'Database Pod name')
        _require(self.pod_name.startswith(self.component + '-'),
                 'Hotel database Pod name must belong to its audited Deployment name.')
        for label, value in (('PVC UID', self.pvc_uid), ('PV UID', self.pv_uid), ('database Pod UID', self.pod_uid)):
            _uuid(value, label)
        _require(len({self.pvc_uid, self.pv_uid, self.pod_uid}) == 3,
                 'Hotel database object UIDs must be distinct.')

    def metadata(self):
        return {name: getattr(self, name) for name in (
            'component', 'pvc_name', 'pvc_uid', 'pv_name', 'pv_uid', 'pod_name', 'pod_uid',
        )}


def _observation_identity(plan, server_ca_sha256, kube_system_namespace_uid, namespace_uid,
                          database_node_name, database_node_uid, database_filesystem_uuid,
                          initializer_pod_name, initializer_pod_uid, databases):
    _require(type(plan) is GuardedHotelRuntimePlan, 'An exact guarded Hotel runtime plan is required.')
    plan.__post_init__()
    try:
        _digest(server_ca_sha256, 'Kubernetes server CA content digest')
    except ValueError as exc:
        raise GuardedHotelRuntimeError('A canonical nonzero Kubernetes server CA digest is required.') from exc
    for name, value in (
        ('kube-system Namespace UID', kube_system_namespace_uid), ('workload Namespace UID', namespace_uid),
        ('database Node UID', database_node_uid), ('database filesystem UUID', database_filesystem_uuid),
        ('initializer Pod UID', initializer_pod_uid),
    ):
        _uuid(value, name)
    _name(database_node_name, 'Database Node name', subdomain=True)
    _require(database_filesystem_uuid == plan.storage.filesystem_uuid,
             'Observed database filesystem differs from the original run storage contract.')
    _require(initializer_pod_name == plan.initializer_pod_name,
             'Observed initializer Pod name differs from the exact single attempt identity.')
    _require(type(databases) is tuple and len(databases) == 8
             and all(type(item) is GuardedHotelDatabaseObservation for item in databases),
             'Exactly eight typed ordered Hotel database observations are required.')
    for item in databases:
        item.__post_init__()
    _require(tuple(item.component for item in databases) == _DATABASES,
             'Hotel database observations must have the complete canonical component inventory/order.')
    all_uids = [kube_system_namespace_uid, namespace_uid, database_node_uid, initializer_pod_uid]
    all_uids.extend(value for item in databases for value in (item.pvc_uid, item.pv_uid, item.pod_uid))
    _require(len(set(all_uids)) == len(all_uids), 'Observed Kubernetes object UIDs cannot be reused or crossed.')
    _require(len({item.pod_name for item in databases} | {initializer_pod_name}) == 9,
             'Observed database and initializer Pod names must be distinct.')
    return {
        'schema_version': 1, 'candidate_schema': OBSERVATION_SCHEMA,
        'candidate_only': True, 'released': False,
        'scope': 'caller-supplied-identifiers-only-not-live-ownership-or-initialization-authority',
        'plan_fingerprint': plan.fingerprint, 'run_id': plan.run_id, 'provider': plan.provider,
        'topology_fingerprint': plan.topology_fingerprint, 'namespace': plan.namespace,
        'server_ca_sha256': server_ca_sha256,
        'cluster_identity_kind': 'kube-system-namespace-uid-and-server-ca-sha256',
        'kube_system_namespace_uid': kube_system_namespace_uid, 'namespace_uid': namespace_uid,
        'database_node_name': database_node_name, 'database_node_uid': database_node_uid,
        'database_filesystem_uuid': database_filesystem_uuid,
        'storage_contract_sha256': plan.storage.sha256,
        'initializer_pod_name': initializer_pod_name, 'initializer_pod_uid': initializer_pod_uid,
        'initialization_attempt_id': plan.initialization_attempt_id,
        'databases': [item.metadata() for item in databases],
        'live_api_executed': False, 'live_identity_verified': False, 'freshness_verified': False,
        'initializer_authorized': False, 'application_start_allowed': False,
        'qualification': _closed_gates(),
    }


@dataclass(frozen=True, slots=True)
class GuardedHotelClusterObservation:
    """Immutable unverified observation, not a live execution capability."""

    plan: GuardedHotelRuntimePlan = field(repr=False)
    server_ca_sha256: str
    kube_system_namespace_uid: str
    namespace_uid: str
    database_node_name: str
    database_node_uid: str
    database_filesystem_uuid: str
    initializer_pod_name: str
    initializer_pod_uid: str
    databases: tuple[GuardedHotelDatabaseObservation, ...]
    metadata_json: str = field(repr=False)
    fingerprint: str

    def __post_init__(self):
        identity = _observation_identity(
            self.plan, self.server_ca_sha256, self.kube_system_namespace_uid, self.namespace_uid,
            self.database_node_name, self.database_node_uid, self.database_filesystem_uuid,
            self.initializer_pod_name, self.initializer_pod_uid, self.databases,
        )
        _require(self.metadata_json == _canonical(identity) and self.fingerprint == _fingerprint(identity),
                 'Hotel cluster observation conflicts with its exact unverified identity.')

    def metadata(self):
        return json.loads(self.metadata_json)


def observe_guarded_hotel_cluster_identity(
    plan: GuardedHotelRuntimePlan, *, server_ca_sha256: str,
    kube_system_namespace_uid: str, namespace_uid: str,
    database_node_name: str, database_node_uid: str, database_filesystem_uuid: str,
    initializer_pod_name: str, initializer_pod_uid: str,
    databases: tuple[GuardedHotelDatabaseObservation, ...],
) -> GuardedHotelClusterObservation:
    """Record explicit identifiers without pretending to observe Kubernetes.

    This function performs no API/network calls or commands. It reconstructs
    the audited local support assets as part of checking its offline plan.
    Callers must not
    treat syntax-correct UIDs, this fingerprint, or retained metadata as fresh
    live ownership verification or as permission to run/redo initialization.
    """
    values = (plan, server_ca_sha256, kube_system_namespace_uid, namespace_uid,
              database_node_name, database_node_uid, database_filesystem_uuid,
              initializer_pod_name, initializer_pod_uid, databases)
    identity = _observation_identity(*values)
    return GuardedHotelClusterObservation(*values, _canonical(identity), _fingerprint(identity))


def validate_guarded_hotel_cluster_observation(
    plan: GuardedHotelRuntimePlan, observation: GuardedHotelClusterObservation,
) -> GuardedHotelClusterObservation:
    """Check exact identity against a fresh plan, without verifying live state."""
    _require(type(observation) is GuardedHotelClusterObservation,
             'An exact typed unverified Hotel cluster observation is required.')
    observation.__post_init__()
    checked = observe_guarded_hotel_cluster_identity(
        plan, server_ca_sha256=observation.server_ca_sha256,
        kube_system_namespace_uid=observation.kube_system_namespace_uid,
        namespace_uid=observation.namespace_uid, database_node_name=observation.database_node_name,
        database_node_uid=observation.database_node_uid,
        database_filesystem_uuid=observation.database_filesystem_uuid,
        initializer_pod_name=observation.initializer_pod_name, initializer_pod_uid=observation.initializer_pod_uid,
        databases=observation.databases,
    )
    _require(observation.metadata_json == checked.metadata_json and observation.fingerprint == checked.fingerprint,
             'Hotel cluster observation belongs to a different offline plan.')
    return checked


def validate_saved_guarded_hotel_cluster_observation(
    plan: GuardedHotelRuntimePlan, metadata: Mapping[str, Any],
) -> dict[str, Any]:
    """Check historical unverified data only; never restore live authority.

    The result is a detached dictionary with every execution/freshness gate
    still false, not a typed execution context or a fresh observation.
    """
    _require(type(metadata) is dict and set(metadata) == {
        'schema_version', 'candidate_schema', 'candidate_only', 'released', 'scope',
        'plan_fingerprint', 'run_id', 'provider', 'topology_fingerprint', 'namespace',
        'server_ca_sha256', 'cluster_identity_kind', 'kube_system_namespace_uid',
        'namespace_uid', 'database_node_name', 'database_node_uid', 'database_filesystem_uuid',
        'storage_contract_sha256', 'initializer_pod_name', 'initializer_pod_uid',
        'initialization_attempt_id', 'databases', 'live_api_executed', 'live_identity_verified',
        'freshness_verified', 'initializer_authorized', 'application_start_allowed', 'qualification',
    }, 'Saved Hotel cluster observation fields differ.')
    rows = metadata['databases']
    fields = {'component', 'pvc_name', 'pvc_uid', 'pv_name', 'pv_uid', 'pod_name', 'pod_uid'}
    _require(type(rows) is list and len(rows) == 8
             and all(type(item) is dict and set(item) == fields for item in rows),
             'Saved Hotel database object observations differ.')
    checked = observe_guarded_hotel_cluster_identity(
        plan, server_ca_sha256=metadata['server_ca_sha256'],
        kube_system_namespace_uid=metadata['kube_system_namespace_uid'],
        namespace_uid=metadata['namespace_uid'], database_node_name=metadata['database_node_name'],
        database_node_uid=metadata['database_node_uid'], database_filesystem_uuid=metadata['database_filesystem_uuid'],
        initializer_pod_name=metadata['initializer_pod_name'], initializer_pod_uid=metadata['initializer_pod_uid'],
        databases=tuple(GuardedHotelDatabaseObservation(**item) for item in rows),
    )
    try:
        _require(_canonical(metadata) == checked.metadata_json,
                 'Saved Hotel observation conflicts with its exact unverified identity.')
    except (TypeError, ValueError, RecursionError) as exc:
        if isinstance(exc, GuardedHotelRuntimeError):
            raise
        raise GuardedHotelRuntimeError('Saved Hotel observation is not finite bounded identity data.') from exc
    return checked.metadata()
