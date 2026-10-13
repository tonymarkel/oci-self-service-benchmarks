"""Plan-bound support-only K3s manifests with application startup forbidden.

Hotel startup seeds MongoDB and is not retry-safe initialization. Media's TMDB
dataset rights and initialization remain unqualified. This distinct stage can
declare support deployments, but every application/frontend has replicas=0.
It never changes the offline final renderer or grants a release/start gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from typing import TYPE_CHECKING, Any

from .deathstarbench_candidate_workload import validate_candidate_assets
from .deathstarbench_workload_contract import distributed_workload_profile

if TYPE_CHECKING:
    from .deathstarbench_candidate_runtime import CandidateRuntimePlan


STAGE_REVISION = 'candidate-support-stage-v1'
STAGE_SCHEMA = 'candidate-support-stage-bundle-v1'
PHASE_NAMES = ('namespace', 'storage', 'policies', 'services', 'support-workloads', 'blocked-applications')
RUN_ANNOTATION = 'deathstarbench.io/candidate-run-id'
PLAN_ANNOTATION = 'deathstarbench.io/candidate-runtime-fingerprint'
REVISION_ANNOTATION = 'deathstarbench.io/candidate-stage-revision'
STAGE_ANNOTATION = 'deathstarbench.io/candidate-stage-fingerprint'
START_ANNOTATION = 'deathstarbench.io/application-start-allowed'


class CandidateStageError(ValueError):
    """A support stage is stale, crossed, or would allow application startup."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n'


def _fingerprint(value: Any) -> str:
    return 'sha256:' + hashlib.sha256(_canonical(value).encode('utf-8')).hexdigest()


def _checked_plan(plan):
    # Import locally: the run bridge may itself import this separate stage API.
    from .deathstarbench_candidate_runtime import CandidateRuntimePlan
    if type(plan) is not CandidateRuntimePlan:
        raise CandidateStageError('An exact validated candidate runtime plan is required.')
    try:
        return CandidateRuntimePlan(
            plan.run_id, plan.provider, plan.topology_fingerprint, plan.artifacts,
            plan.bundle, plan.storage, plan.snapshot_json, plan.fingerprint,
        )
    except (ValueError, TypeError, AttributeError) as exc:
        raise CandidateStageError('Candidate support stage cannot adopt a crossed runtime plan.') from exc


def _derive_stage(plan) -> dict[str, Any]:
    components, _ = validate_candidate_assets(plan.artifacts.workload_id)
    profile = distributed_workload_profile(plan.artifacts.workload_id)
    roles = {item['name']: item['role'] for item in components}
    blocked = tuple(sorted(name for name, role in roles.items() if role == 'application'))
    support = tuple(sorted(name for name, role in roles.items() if role != 'application'))
    identity = {
        'schema_version': 1, 'candidate_schema': STAGE_SCHEMA, 'revision': STAGE_REVISION,
        'candidate_only': True, 'released': False, 'application_start_allowed': False,
        'initialization_qualified': False, 'runtime_qualified': False,
        'measurement_qualified': False, 'dataset_ready': False,
        'dataset_rights_cleared': False, 'anonymous_pull_qualified': False,
        'run_id': plan.run_id, 'provider': plan.provider,
        'workload_id': profile.workload_id, 'namespace': profile.namespace,
        'plan_fingerprint': plan.fingerprint, 'topology_fingerprint': plan.topology_fingerprint,
        'artifact_fingerprint': plan.artifacts.fingerprint,
        'manifest_source_sha256': plan.bundle.manifest_source_sha256,
        'final_manifest_sha256': plan.bundle.rendered_manifest_sha256,
        'image_manifest_sha256': plan.bundle.image_manifest_sha256,
        'application_architecture': plan.bundle.application_architecture,
        'load_generator_cidr': plan.bundle.load_generator_cidr,
        'active_support_components': list(support), 'blocked_application_components': list(blocked),
        'readiness_scope': 'support-only-no-application-or-dataset-readiness',
        'startup_barriers': ([
            'Eight Hotel startup programs unconditionally seed MongoDB; guarded initialization and restart semantics are not qualified.',
            'All Hotel applications and its frontend remain stopped; startup cannot be treated as declarative retry-safe work.',
        ] if profile.workload_id == 'hotel_reservation' else [
            'Media TMDB dataset rights need documented clearance or a reviewed versioned replacement.',
            'All Media applications and its frontend remain stopped; dataset initialization is not authorized or qualified.',
        ]),
    }
    # The identity fingerprint is computed before annotations, not from a
    # payload that contains its own digest. The exact payload gets a separate
    # rendered_manifest_sha256 and both values are required by the stage guard.
    fingerprint = _fingerprint(identity)
    owners = {
        RUN_ANNOTATION: plan.run_id, PLAN_ANNOTATION: plan.fingerprint,
        REVISION_ANNOTATION: STAGE_REVISION, STAGE_ANNOTATION: fingerprint,
        START_ANNOTATION: 'false',
    }
    groups = {name: [] for name in PHASE_NAMES}
    final = json.loads(plan.bundle.canonical_json)
    if set(final) != {'apiVersion', 'kind', 'items'} or final['apiVersion'] != 'v1' or final['kind'] != 'List':
        raise CandidateStageError('The candidate final render is not an exact Kubernetes List.')
    deployment_names = []
    for item in final['items']:
        kind = item['kind']
        item['metadata'].setdefault('annotations', {}).update(owners)
        if kind == 'Deployment':
            name = item['metadata']['name']
            if name not in roles:
                raise CandidateStageError('The stage includes an unaudited deployment.')
            deployment_names.append(name)
            pod = item['spec']['template']
            pod.setdefault('metadata', {}).setdefault('annotations', {}).update(owners)
            if 'initContainers' in pod['spec'] or item['spec'].get('paused'):
                raise CandidateStageError('Initialization/paused-controller behavior is not part of the support stage.')
            application = roles[name] == 'application'
            item['spec']['replicas'] = 0 if application else 1
            groups['blocked-applications' if application else 'support-workloads'].append(item)
        elif kind == 'Namespace':
            groups['namespace'].append(item)
        elif kind in ('PersistentVolume', 'PersistentVolumeClaim'):
            groups['storage'].append(item)
        elif kind == 'NetworkPolicy':
            groups['policies'].append(item)
        elif kind == 'Service':
            groups['services'].append(item)
        else:
            # No Pod, Job, CronJob, autoscaler, or extra controller can bypass
            # the application replica gate or mutate/initialize the dataset.
            raise CandidateStageError('Unexpected resource kind in the support stage.')
    if sorted(deployment_names) != sorted(roles) or len(deployment_names) != len(roles):
        raise CandidateStageError('Candidate support-stage deployment inventory is incomplete or duplicated.')
    for values in groups.values():
        values.sort(key=lambda item: (item['kind'], item['metadata']['name']))
    phase_payloads = tuple(_canonical({'apiVersion': 'v1', 'kind': 'List', 'items': groups[name]}) for name in PHASE_NAMES)
    canonical_json = _canonical({'apiVersion': 'v1', 'kind': 'List',
                                 'items': [item for name in PHASE_NAMES for item in groups[name]]})
    rendered_sha256 = 'sha256:' + hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()
    return {
        'workload_id': profile.workload_id, 'run_id': plan.run_id, 'namespace': profile.namespace,
        'plan_fingerprint': plan.fingerprint, 'fingerprint': fingerprint,
        'rendered_manifest_sha256': rendered_sha256,
        'final_manifest_sha256': plan.bundle.rendered_manifest_sha256,
        'manifest_source_sha256': plan.bundle.manifest_source_sha256,
        'image_manifest_sha256': plan.bundle.image_manifest_sha256,
        'application_architecture': plan.bundle.application_architecture,
        'load_generator_cidr': plan.bundle.load_generator_cidr,
        'canonical_json': canonical_json, 'phase_payloads': phase_payloads,
        'active_support_components': support, 'blocked_application_components': blocked,
        '_metadata_json': _canonical({**identity, 'fingerprint': fingerprint,
                                      'rendered_manifest_sha256': rendered_sha256}),
    }


@dataclass(frozen=True, slots=True)
class CandidateSupportStageBundle:
    plan: CandidateRuntimePlan = field(repr=False)
    workload_id: str
    run_id: str
    namespace: str
    plan_fingerprint: str
    fingerprint: str
    rendered_manifest_sha256: str
    final_manifest_sha256: str
    manifest_source_sha256: str
    image_manifest_sha256: str
    application_architecture: str
    load_generator_cidr: str
    canonical_json: str
    phase_payloads: tuple[str, ...]
    active_support_components: tuple[str, ...]
    blocked_application_components: tuple[str, ...]
    _metadata_json: str = field(repr=False)
    revision: str = field(init=False, default=STAGE_REVISION)
    candidate_only: bool = field(init=False, default=True)
    application_start_allowed: bool = field(init=False, default=False)
    initialization_qualified: bool = field(init=False, default=False)
    released: bool = field(init=False, default=False)

    def __post_init__(self):
        checked = _checked_plan(self.plan)
        expected = _derive_stage(checked)
        for name, value in expected.items():
            if type(getattr(self, name)) is not type(value) or getattr(self, name) != value:
                raise CandidateStageError('Candidate stage conflicts with its exact support-only derivation: ' + name)
        object.__setattr__(self, 'plan', checked)

    @property
    def phases(self) -> tuple[tuple[str, str], ...]:
        return tuple(zip(PHASE_NAMES, self.phase_payloads, strict=True))

    def metadata(self) -> dict[str, Any]:
        return json.loads(self._metadata_json)


def build_candidate_support_stage(plan: CandidateRuntimePlan) -> CandidateSupportStageBundle:
    """Derive a separate staged manifest; never start an application or seed."""
    checked = _checked_plan(plan)
    return CandidateSupportStageBundle(plan=checked, **_derive_stage(checked))


def validate_candidate_support_stage(
    plan: CandidateRuntimePlan, stage: CandidateSupportStageBundle,
) -> CandidateSupportStageBundle:
    """Return a fresh stage only when plan, stage identity, and bytes all match."""
    checked = build_candidate_support_stage(plan)
    if type(stage) is not CandidateSupportStageBundle or stage != checked:
        raise CandidateStageError('Candidate support stage is crossed or stale; application startup stays forbidden.')
    return checked
