"""Offline identity for startup Hotel images and the unchanged paused support.

This is not the guarded initialization runtime, a cluster observation, or an
execution capability. The startup image-access proof must remain original and
process-local. An initialization attempt identifier records identity only: it
does not assert initialization completed, restore a saved receipt's authority,
or permit any initializer, application, Kubernetes or cloud operation.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import json
import re
from typing import Any

from .deathstarbench_candidate_artifacts import CandidateArtifactBinding
from .deathstarbench_candidate_runtime import (
    CandidateRuntimePlan, _canonical, _fingerprint,
)
from .deathstarbench_candidate_stage import (
    CandidateSupportStageBundle, validate_candidate_support_stage,
)
from .deathstarbench_candidate_storage import CandidateStorageContract
from .deathstarbench_candidate_workload import CandidateWorkloadBundle
from .deathstarbench_hotel_initialization import (
    COMPONENTS as DATABASE_COMPONENTS, validate_hotel_dataset_attestation,
)
from .deathstarbench_hotel_startup_artifacts import (
    ADOPTION_SCHEMA, StartupHotelArtifactBinding,
)
from .deathstarbench_hotel_startup_image_access import (
    StartupHotelImageAccess, require_startup_hotel_image_access,
)
from scripts import prepare_deathstarbench_hotel_startup as preparation


SNAPSHOT_SCHEMA = 'hotel-guarded-startup-runtime-preflight-v1'
RUNTIME_REVISION = 'hotel-guarded-startup-runtime-identity-candidate-v1'
NAMESPACE = 'deathstarbench-hotel'
APPLICATIONS = tuple(preparation.APPLICATIONS)
_DATABASES = tuple('mongodb-' + component for component in DATABASE_COMPONENTS)
_PLATFORMS = {'x86_64': 'linux/amd64', 'aarch64': 'linux/arm64'}
_UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')
_CLOSED_FLAGS = (
    'released', 'qualification_authority', 'initializer_authorized', 'runtime_authorized',
    'startup_authorized', 'initialization_executed', 'initialization_complete',
    'database_executed', 'database_execution_verified',
    'global_readonly_verification_completed', 'application_start_allowed',
    'application_started', 'application_ready', 'startup_executed',
    'dataset_qualified', 'runtime_qualified', 'measurement_qualified', 'cloud_qualified',
    'anonymous_pull_qualified', 'anonymous_layer_pull_qualified', 'image_execution_performed',
    'current_candidate_factory_accepts_recipe', 'current_guarded_factory_accepts_recipe',
    'public_release', 'public_dispatch_enabled',
    'live_api_executed', 'live_identity_observed', 'live_identity_verified', 'freshness_verified',
)


class StartupHotelRuntimeError(ValueError):
    """The startup identity is crossed, stale, or claims execution authority."""


def _require(condition, message):
    if not condition:
        raise StartupHotelRuntimeError(message)


def _uuid(value, label):
    _require(type(value) is str and _UUID.fullmatch(value) is not None
             and value != '00000000-0000-0000-0000-000000000000',
             label + ' requires a nonzero canonical lowercase UUID.')
    return value


def _component(value):
    _require(type(value) is str and value in APPLICATIONS,
             'An exact audited startup Hotel application component is required.')
    return value


def _pod_name(component, attempt):
    component = _component(component)
    _uuid(attempt, 'Startup attempt identity')
    name = 'hotel-start-' + component + '-' + attempt.replace('-', '')
    _require(len(name) <= 63, 'A startup Hotel Pod name exceeds the Kubernetes name bound.')
    return name


def _environment(run_id, dataset_fingerprint, attempt, component):
    values = {
        'HOTEL_CANDIDATE_RUN_ID': {'value': run_id},
        'HOTEL_CANDIDATE_CONTRACT_SHA256': {'value': dataset_fingerprint},
        'HOTEL_CANDIDATE_STARTUP_ATTEMPT_ID': {'value': attempt},
        'HOTEL_CANDIDATE_COMPONENT': {'value': _component(component)},
        'HOTEL_CANDIDATE_POD_UID': {
            'valueFrom': {'fieldRef': {'apiVersion': 'v1', 'fieldPath': 'metadata.uid'}},
        },
        'HOTEL_CANDIDATE_POD_IP': {
            'valueFrom': {'fieldRef': {'apiVersion': 'v1', 'fieldPath': 'status.podIP'}},
        },
        'TLS': {'value': 'false'},
    }
    return [{'name': name, **values[name]} for name in sorted(values)]


def _closed_gates():
    return {name: False for name in (
        'support', 'live_cluster_identity', 'initializer_authorized', 'initialization_executed',
        'global_readonly_verification', 'admission', 'continuity', 'one_shot_journal',
        'startup_dispatch', 'application_start',
        'service_discovery', 'semantic_readiness', 'restart_recovery', 'dataset',
        'cloud_runtime', 'measurement', 'denied_edges', 'cleanup', 'public_release',
    )}


def _checked_inputs(artifacts, access, support_plan, support_stage):
    _require(type(artifacts) is StartupHotelArtifactBinding,
             'A freshly adopted, distinct startup Hotel artifact binding is required.')
    _require(type(access) is StartupHotelImageAccess,
             'The original process-local startup Hotel byte-access capability is required.')
    _require(type(support_plan) is CandidateRuntimePlan
             and type(getattr(support_plan, 'artifacts', None)) is CandidateArtifactBinding,
             'An exact original candidate support runtime plan is required.')
    _require(type(support_stage) is CandidateSupportStageBundle,
             'An exact original paused candidate support stage is required.')
    _require(type(getattr(support_plan, 'bundle', None)) is CandidateWorkloadBundle
             and type(getattr(support_plan, 'storage', None)) is CandidateStorageContract,
             'Exact original candidate render and storage contracts are required.')
    try:
        # No network is used here. Revalidate original factory proofs and the
        # complete local support derivation, not only caller-supplied digests.
        artifacts.__post_init__()
        require_startup_hotel_image_access(access, artifacts)
        support_plan.artifacts.__post_init__()
        support_plan.__post_init__()
        stage = validate_candidate_support_stage(support_plan, support_stage)
        adopted = artifacts.metadata()
        original = support_plan.artifacts.metadata()
        _require(artifacts.workload_id == support_plan.artifacts.workload_id == 'hotel_reservation',
                 'Startup Hotel cannot adopt Media or Social support identities.')
        _require(adopted['candidate_schema'] == ADOPTION_SCHEMA
                 and adopted['recipe_revision'] == preparation.RECIPE_REVISION
                 and adopted['policy_revision'] == preparation.POLICY_REVISION
                 and adopted['dataset_revision'] == preparation.DATASET_REVISION,
                 'Startup Hotel artifact recipe, policy or dataset revisions differ.')
        _require(adopted['baseline']['binding_fingerprint'] == support_plan.artifacts.fingerprint,
                 'Startup Hotel and support plan require the same freshly adopted original baseline.')
        reused = {name: value for name, value in original['image_bindings'].items()
                  if name != 'hotel-reservation'}
        _require(_canonical(adopted['reused_images']) == _canonical(reused),
                 'Startup Hotel support and driver images differ from the original support plan.')
        _require(support_plan.storage.namespace == support_plan.bundle.namespace == stage.namespace == NAMESPACE
                 and support_plan.storage.run_id == support_plan.run_id
                 and support_plan.storage.workload_id == 'hotel_reservation'
                 and support_plan.storage.database_names == _DATABASES,
                 'Startup Hotel requires the exact run namespace and eight audited database directories.')
        _require(support_plan.bundle.application_architecture in _PLATFORMS,
                 'Startup Hotel requires an exact supported application architecture.')
        _require(stage.blocked_application_components == APPLICATIONS,
                 'The original support stage must keep all ten audited Hotel applications paused.')
        _require(set(adopted['startup_attestations']) == set(APPLICATIONS)
                 and all(adopted['startup_attestations'][name]['component'] == name for name in APPLICATIONS),
                 'Startup Hotel requires the exact ten compiled application attestations.')
        validate_hotel_dataset_attestation(
            adopted['dataset_attestation'], expected_source_fingerprint=adopted['source_fingerprint'],
        )
        # Recheck the same capability after local source/asset derivations.
        require_startup_hotel_image_access(access, artifacts)
    except (ValueError, KeyError, TypeError, AttributeError, RecursionError) as exc:
        if isinstance(exc, StartupHotelRuntimeError):
            raise
        raise StartupHotelRuntimeError('Startup Hotel cannot adopt crossed support, image or artifact evidence.') from None
    return stage, adopted


def _snapshot_identity(artifacts, access, support_plan, support_stage,
                       initialization_attempt_id, startup_attempt_id):
    stage, adopted = _checked_inputs(artifacts, access, support_plan, support_stage)
    _uuid(initialization_attempt_id, 'Initialization attempt identity')
    _uuid(startup_attempt_id, 'Startup attempt identity')
    _require(initialization_attempt_id != startup_attempt_id,
             'Initialization and startup attempts require distinct identities.')
    platform = _PLATFORMS[support_plan.bundle.application_architecture]
    selected = adopted['app']['platforms'][platform]
    return {
        'schema_version': 1, 'candidate_schema': SNAPSHOT_SCHEMA,
        'runtime_revision': RUNTIME_REVISION, 'candidate_only': True,
        **dict.fromkeys(_CLOSED_FLAGS, False),
        'scope': 'offline-startup-image-and-paused-support-identity-only-no-execution-authority',
        'workload_id': 'hotel_reservation', 'run_id': support_plan.run_id,
        'provider': support_plan.provider, 'topology_fingerprint': support_plan.topology_fingerprint,
        'namespace': NAMESPACE, 'application_architecture': support_plan.bundle.application_architecture,
        'support_plan_fingerprint': support_plan.fingerprint,
        'support_stage_fingerprint': stage.fingerprint,
        'support_stage_manifest_sha256': stage.rendered_manifest_sha256,
        'support_manifest_source_sha256': stage.manifest_source_sha256,
        'support_image_manifest_sha256': stage.image_manifest_sha256,
        'original_artifact_fingerprint': support_plan.artifacts.fingerprint,
        'startup_artifact_fingerprint': artifacts.fingerprint,
        'startup_image_access_fingerprint': access.fingerprint,
        **{name: adopted[name] for name in (
            'recipe_revision', 'policy_revision', 'generator_revision', 'dataset_revision',
            'source_fingerprint', 'config_sha256', 'dataset_attestation', 'startup_attestations',
        )},
        'dataset_fingerprint': adopted['dataset_attestation']['dataset_fingerprint'],
        'application_platform': platform, 'application_image': selected['image'],
        'application_config_digest': selected['config_digest'],
        'application_index_image': adopted['app']['index_image'],
        'reused_images': adopted['reused_images'], 'storage': support_plan.storage.metadata(),
        'expected_component_placement': dict(support_plan.bundle.expected_component_placement),
        'load_generator_cidr': support_plan.bundle.load_generator_cidr,
        'initialization_attempt_id': initialization_attempt_id,
        'startup_attempt_id': startup_attempt_id,
        'applications': [{
            'component': component, 'pod_name': _pod_name(component, startup_attempt_id),
            'environment': _environment(support_plan.run_id, adopted['dataset_attestation']['dataset_fingerprint'],
                                        startup_attempt_id, component),
        } for component in APPLICATIONS],
        'required_live_identity': [
            'server-ca-sha256', 'kube-system-namespace-uid', 'workload-namespace-uid',
            'all-five-role-node-names-and-uids', 'database-filesystem-uuid',
            'eight-bound-pvc-pv-and-database-pod-identities',
            'fresh-global-readonly-verifier-pod-uid-and-exact-completed-dataset-observation',
            'ten-startup-pod-names-uids-private-ips-node-bindings-and-container-image-identities',
            'admission-mutator-absence-and-exact-admitted-startup-specs',
            'run-attempt-pod-bound-unique-consul-catalog-and-semantic-service-readiness',
        ],
        'remaining_gates': [
            'fresh-owned-cloud-support-and-cluster-observation',
            'separate-compatible-initialization-and-fresh-global-readonly-dataset-verification',
            'exact-admitted-pod-specs-and-admission-mutator-absence',
            'live-cluster-namespace-storage-database-and-pod-identity-continuity',
            'leased-durable-one-shot-startup-journal-and-uncertain-dispatch-handling',
            'cleanup-only-recovery-and-proven-owned-infrastructure-destruction',
            'service-discovery-preload-semantic-readiness-and-restart-recovery',
            'authentication-reservation-state-denied-edges-and-measurement-qualification',
            'public-release-and-ui-review',
        ],
        'policy': {
            'stage_shape': 'ten-standalone-scheduling-gated-pods-no-deployments',
            'startup_attempts': 1, 'startup_pod_restart_policy': 'Never',
            'initialization_attempt_is_completion_proof': False,
            'startup_attempt_is_dispatch_authority': False,
            'saved_initialization_receipt_is_authority': False,
            'automatic_retry_allowed': False, 'application_restart_allowed': False,
            'initialization_replay_allowed': False, 'reset_allowed': False,
            'repair_allowed': False, 'upsert_allowed': False,
            'interrupted_or_ambiguous_state': 'cleanup-only-no-dispatch-or-replay',
            'replaced_cluster_namespace_storage_or_pod': 'cleanup-only-new-run-required',
            'application_start': 'forbidden-until-separate-fresh-live-verification-and-leased-controller',
            'image_access_scope': 'complete-compressed-byte-access-only-not-runtime-or-startup-authority',
            'go_identity_scope': 'run-dataset-attempt-component-pod-uid-and-private-pod-ip-assertions-only',
            'runtime_arguments': [], 'tls': 'false', 'baked_configuration_override_allowed': False,
            'live_observation_is_authority': False,
        },
        'qualification': _closed_gates(),
    }


@dataclass(frozen=True, slots=True)
class StartupHotelRuntimePlan:
    """Immutable offline startup identity, never initialization/start permission."""

    artifacts: StartupHotelArtifactBinding = field(repr=False)
    access: StartupHotelImageAccess = field(repr=False)
    support_plan: CandidateRuntimePlan = field(repr=False)
    support_stage: CandidateSupportStageBundle = field(repr=False)
    initialization_attempt_id: str
    startup_attempt_id: str
    snapshot_json: str = field(repr=False)
    fingerprint: str
    candidate_only: bool = field(init=False, default=True)
    released: bool = field(init=False, default=False)
    qualification_authority: bool = field(init=False, default=False)
    initializer_authorized: bool = field(init=False, default=False)
    runtime_authorized: bool = field(init=False, default=False)
    startup_authorized: bool = field(init=False, default=False)
    initialization_executed: bool = field(init=False, default=False)
    initialization_complete: bool = field(init=False, default=False)
    database_executed: bool = field(init=False, default=False)
    database_execution_verified: bool = field(init=False, default=False)
    global_readonly_verification_completed: bool = field(init=False, default=False)
    application_start_allowed: bool = field(init=False, default=False)
    application_started: bool = field(init=False, default=False)
    application_ready: bool = field(init=False, default=False)
    startup_executed: bool = field(init=False, default=False)
    dataset_qualified: bool = field(init=False, default=False)
    runtime_qualified: bool = field(init=False, default=False)
    measurement_qualified: bool = field(init=False, default=False)
    cloud_qualified: bool = field(init=False, default=False)
    anonymous_pull_qualified: bool = field(init=False, default=False)
    anonymous_layer_pull_qualified: bool = field(init=False, default=False)
    image_execution_performed: bool = field(init=False, default=False)
    current_candidate_factory_accepts_recipe: bool = field(init=False, default=False)
    current_guarded_factory_accepts_recipe: bool = field(init=False, default=False)
    public_release: bool = field(init=False, default=False)
    public_dispatch_enabled: bool = field(init=False, default=False)
    live_api_executed: bool = field(init=False, default=False)
    live_identity_observed: bool = field(init=False, default=False)
    live_identity_verified: bool = field(init=False, default=False)
    freshness_verified: bool = field(init=False, default=False)

    def __post_init__(self):
        try:
            expected = _snapshot_identity(
                self.artifacts, self.access, self.support_plan, self.support_stage,
                self.initialization_attempt_id, self.startup_attempt_id,
            )
            _require(self.snapshot_json == _canonical(expected) and self.fingerprint == _fingerprint(expected),
                     'Startup Hotel plan conflicts with its exact offline derivation.')
            _require(self.candidate_only is True
                     and all(getattr(self, name) is False for name in _CLOSED_FLAGS),
                     'A startup Hotel plan cannot promote execution or qualification gates.')
        except (ValueError, KeyError, TypeError, AttributeError, RecursionError) as exc:
            if isinstance(exc, StartupHotelRuntimeError):
                raise
            raise StartupHotelRuntimeError('Startup Hotel plan identity is malformed or crossed.') from None

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

    def application_pod_name(self, component):
        return _pod_name(component, self.startup_attempt_id)

    def startup_environment(self, component):
        value = self.snapshot()
        return _environment(value['run_id'], value['dataset_fingerprint'], value['startup_attempt_id'], component)

    def snapshot(self) -> dict[str, Any]:
        return json.loads(self.snapshot_json)

    def metadata(self) -> dict[str, Any]:
        return self.snapshot()


def create_startup_hotel_runtime_plan(
    artifacts: StartupHotelArtifactBinding, access: StartupHotelImageAccess,
    support_plan: CandidateRuntimePlan, support_stage: CandidateSupportStageBundle, *,
    initialization_attempt_id: str, startup_attempt_id: str,
) -> StartupHotelRuntimePlan:
    """Bind exact original proofs offline without observing or executing a run."""
    identity = _snapshot_identity(artifacts, access, support_plan, support_stage,
                                  initialization_attempt_id, startup_attempt_id)
    return StartupHotelRuntimePlan(
        artifacts, access, support_plan, support_stage, initialization_attempt_id, startup_attempt_id,
        _canonical(identity), _fingerprint(identity),
    )


def validate_startup_hotel_runtime_plan(plan: StartupHotelRuntimePlan) -> StartupHotelRuntimePlan:
    """Fully revalidate and return the original frozen plan, never saved proofs."""
    _require(type(plan) is StartupHotelRuntimePlan, 'An exact startup Hotel runtime plan is required.')
    try:
        plan.__post_init__()
        return plan
    except (ValueError, KeyError, TypeError, AttributeError, RecursionError) as exc:
        if isinstance(exc, StartupHotelRuntimeError):
            raise
        raise StartupHotelRuntimeError('Startup Hotel runtime plan is incomplete, changed, or crossed.') from None


def validate_startup_hotel_runtime_snapshot(value, plan: StartupHotelRuntimePlan) -> dict[str, Any]:
    """Compare historical identity against fresh original proofs, never adopt it."""
    checked = validate_startup_hotel_runtime_plan(plan)
    try:
        _require(isinstance(value, Mapping) and _canonical(dict(value)) == checked.snapshot_json,
                 'Saved startup Hotel identity differs from fresh offline preflight.')
    except (ValueError, TypeError, RecursionError) as exc:
        if isinstance(exc, StartupHotelRuntimeError):
            raise
        raise StartupHotelRuntimeError('Saved startup Hotel identity is not finite bounded JSON.') from None
    return checked.snapshot()
