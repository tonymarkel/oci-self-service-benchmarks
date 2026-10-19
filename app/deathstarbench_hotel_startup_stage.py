"""Offline, scheduling-gated Hotel application Pods, never startup authority.

The original support stage remains unchanged and its application Deployments
stay at zero replicas. This distinct additive List contains only ten standalone
Pods. Every Pod remains unschedulable until a separate future leased controller
has verified live initialization, admission, ownership and dispatch continuity.
There is no executor, gate-release patch, retry or semantic-readiness claim here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from typing import Any

from .deathstarbench_candidate_workload import validate_candidate_assets
from .deathstarbench_workload_contract import distributed_workload_profile


STAGE_REVISION = 'hotel-guarded-startup-stage-candidate-v1'
STAGE_SCHEMA = 'hotel-guarded-startup-stage-v1'
SCHEDULING_GATE = 'deathstarbench.io/hotel-startup-controller'
APPLICATIONS = (
    'attractions', 'frontend', 'geo', 'profile', 'rate', 'recommendation',
    'reservation', 'review', 'search', 'user',
)
_GATES = (
    'released', 'qualification_authority', 'initializer_authorized', 'runtime_authorized',
    'startup_authorized', 'initialization_executed', 'initialization_complete',
    'database_executed', 'database_execution_verified',
    'global_readonly_verification_completed', 'application_start_allowed',
    'application_started', 'application_ready', 'startup_executed', 'dataset_qualified', 'runtime_qualified',
    'measurement_qualified', 'cloud_qualified', 'public_release', 'anonymous_pull_qualified',
    'public_dispatch_enabled', 'live_identity_observed', 'live_identity_verified', 'freshness_verified',
)


class StartupHotelStartupStageError(ValueError):
    """A startup stage is crossed, changed or would broaden offline rendering."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n'


def _fingerprint(value: Any) -> str:
    return 'sha256:' + hashlib.sha256(_canonical(value).encode('utf-8')).hexdigest()


def _checked_plan(plan):
    # This type is intentionally not accepted by existing initialization or
    # support-stage factories, and those factories are not relaxed here.
    from .deathstarbench_hotel_startup_runtime import (
        StartupHotelRuntimePlan, validate_startup_hotel_runtime_plan,
    )
    if type(plan) is not StartupHotelRuntimePlan:
        raise StartupHotelStartupStageError('An exact startup Hotel runtime identity plan is required.')
    try:
        return validate_startup_hotel_runtime_plan(plan)
    except (ValueError, TypeError, AttributeError, KeyError, RecursionError) as exc:
        raise StartupHotelStartupStageError('Startup Hotel runtime identity is crossed or stale.') from exc


def _applications():
    """Read only the checksum-audited original command/port inventory."""
    try:
        components, _edges = validate_candidate_assets('hotel_reservation')
        selected = tuple(item for item in components if item['role'] == 'application')
        if tuple(item['name'] for item in selected) != APPLICATIONS or any(
                item['image_key'] != 'hotel-reservation'
                or item['command'] != ['/go/bin/' + item['name']]
                or item['storage_subdir'] is not None for item in selected):
            raise StartupHotelStartupStageError('The exact ten audited Hotel applications are required.')
        return selected
    except (ValueError, TypeError, AttributeError, KeyError, RecursionError) as exc:
        if isinstance(exc, StartupHotelStartupStageError):
            raise
        raise StartupHotelStartupStageError('Audited Hotel application inventory differs.') from exc


def _derive(plan):
    profile = distributed_workload_profile('hotel_reservation')
    components = _applications()
    snapshot = plan.snapshot()
    common_labels = {
        'deathstarbench.io/workload': profile.workload_label,
        'deathstarbench.io/role': 'application',
        'deathstarbench.io/candidate-run-id': plan.run_id,
        'deathstarbench.io/hotel-startup': 'guarded-v1',
        'deathstarbench.io/hotel-startup-attempt': plan.startup_attempt_id,
    }
    annotations = {
        'deathstarbench.io/candidate-run-id': plan.run_id,
        'deathstarbench.io/hotel-startup-runtime-fingerprint': plan.fingerprint,
        'deathstarbench.io/hotel-startup-stage-revision': STAGE_REVISION,
        'deathstarbench.io/hotel-initialization-attempt': plan.initialization_attempt_id,
        'deathstarbench.io/hotel-startup-attempt': plan.startup_attempt_id,
        'deathstarbench.io/application-start-allowed': 'false',
        'deathstarbench.io/architecture': plan.application_architecture,
        'deathstarbench.io/image-reference': plan.application_image,
        'deathstarbench.io/hotel-startup-config-sha256': snapshot['config_sha256'],
        'deathstarbench.io/hotel-startup-image-config-digest': snapshot['application_config_digest'],
    }
    pods = []
    for component in components:
        name = component['name']
        pods.append({
            'apiVersion': 'v1', 'kind': 'Pod',
            'metadata': {
                'name': plan.application_pod_name(name), 'namespace': plan.namespace,
                'labels': {**common_labels, 'app.kubernetes.io/component': name},
                'annotations': dict(annotations),
            },
            'spec': {
                'schedulingGates': [{'name': SCHEDULING_GATE}],
                'restartPolicy': 'Never', 'activeDeadlineSeconds': 900,
                'terminationGracePeriodSeconds': 10,
                'automountServiceAccountToken': False, 'enableServiceLinks': False,
                'dnsPolicy': 'ClusterFirst', 'schedulerName': 'default-scheduler',
                'hostNetwork': False, 'hostPID': False, 'hostIPC': False,
                'shareProcessNamespace': False,
                'nodeSelector': {
                    'deathstarbench.io/role': 'application',
                    'kubernetes.io/arch': 'arm64' if plan.application_architecture == 'aarch64' else 'amd64',
                },
                'securityContext': {
                    'runAsNonRoot': True, 'runAsUser': 65532, 'runAsGroup': 65532,
                    'seccompProfile': {'type': 'RuntimeDefault'},
                },
                'containers': [{
                    'name': name, 'image': plan.application_image, 'imagePullPolicy': 'Always',
                    'workingDir': '/workspace', 'command': list(component['command']), 'args': [],
                    # Never inherit old TLS=0 or arbitrary endpoint settings.
                    # The validated plan supplies exact run/dataset/attempt and
                    # component literals plus Pod UID/IP Downward API fields.
                    'env': plan.startup_environment(name),
                    'ports': [{
                        'name': port['name'], 'containerPort': port['container_port'],
                        'protocol': port['protocol'],
                    } for port in component['ports']],
                    'securityContext': {
                        'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                        'capabilities': {'drop': ['ALL']},
                    },
                    # The original audited renderer declares no application
                    # resources. These modest explicit bounds are only for a
                    # future short qualification, never a benchmark profile.
                    'resources': {
                        'requests': {'cpu': '100m', 'memory': '64Mi'},
                        'limits': {'cpu': '1', 'memory': '256Mi'},
                    },
                }],
            },
        })

    identity = {
        'schema_version': 1, 'candidate_schema': STAGE_SCHEMA, 'revision': STAGE_REVISION,
        'candidate_only': True, **{flag: False for flag in _GATES},
        'run_id': plan.run_id, 'provider': plan.provider, 'namespace': plan.namespace,
        'plan_fingerprint': plan.fingerprint,
        'initialization_attempt_id': plan.initialization_attempt_id,
        'startup_attempt_id': plan.startup_attempt_id,
        'application_image': plan.application_image,
        'application_architecture': plan.application_architecture,
        **{name: snapshot[name] for name in (
            'support_plan_fingerprint', 'support_stage_fingerprint', 'support_stage_manifest_sha256',
            'original_artifact_fingerprint', 'startup_artifact_fingerprint',
            'startup_image_access_fingerprint', 'recipe_revision', 'policy_revision',
            'dataset_revision', 'config_sha256', 'application_config_digest', 'application_index_image',
        )},
        'source_fingerprint': snapshot['source_fingerprint'],
        'dataset_fingerprint': snapshot['dataset_fingerprint'],
        'pod_names': {component['name']: pod['metadata']['name']
                      for component, pod in zip(components, pods, strict=True)},
        'application_components': list(APPLICATIONS), 'application_pod_count': 10,
        'scheduling_gate': SCHEDULING_GATE, 'restart_policy': 'Never',
        'active_deadline_seconds': 900,
        'scope': 'offline-ten-gated-standalone-startup-qualification-pods-only',
        'execution_authority': 'none-separate-leased-live-controller-and-durable-dispatch-required',
        'live_initialization_verified': False, 'semantic_readiness_verified': False,
        'gate_release_patch_available': False, 'automatic_retry_allowed': False,
        'original_support_stage_modified': False,
        'deployments_included': 0, 'jobs_included': 0, 'services_included': 0,
        'network_policies_included': 0, 'storage_resources_included': 0, 'rbac_resources_included': 0,
        'resource_scope': 'bounded-qualification-only-not-released-measurement-capacity',
        'readiness_scope': 'none-no-tcp-or-static-page-readiness-substitute',
        'recovery': 'never-replay-on-response-loss-pod-replacement-or-uncertainty-cleanup-only',
    }
    fingerprint = _fingerprint(identity)
    for pod in pods:
        pod['metadata']['annotations']['deathstarbench.io/hotel-startup-stage-fingerprint'] = fingerprint
    payload = _canonical({'apiVersion': 'v1', 'kind': 'List', 'items': pods})
    manifest_sha256 = 'sha256:' + hashlib.sha256(payload.encode('utf-8')).hexdigest()
    return {
        'run_id': plan.run_id, 'namespace': plan.namespace, 'plan_fingerprint': plan.fingerprint,
        'startup_attempt_id': plan.startup_attempt_id, 'fingerprint': fingerprint,
        'canonical_json': payload, 'rendered_manifest_sha256': manifest_sha256,
        '_metadata_json': _canonical({**identity, 'fingerprint': fingerprint,
                                     'rendered_manifest_sha256': manifest_sha256}),
    }


@dataclass(frozen=True, slots=True)
class StartupHotelStartupStage:
    """Exact immutable offline Pod List, not permission to create or start it."""

    plan: Any = field(repr=False)
    run_id: str
    namespace: str
    plan_fingerprint: str
    startup_attempt_id: str
    fingerprint: str
    canonical_json: str
    rendered_manifest_sha256: str
    _metadata_json: str = field(repr=False)
    revision: str = field(init=False, default=STAGE_REVISION)
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
    public_release: bool = field(init=False, default=False)
    anonymous_pull_qualified: bool = field(init=False, default=False)
    public_dispatch_enabled: bool = field(init=False, default=False)
    live_identity_observed: bool = field(init=False, default=False)
    live_identity_verified: bool = field(init=False, default=False)
    freshness_verified: bool = field(init=False, default=False)

    def __post_init__(self):
        try:
            checked = _checked_plan(self.plan)
            if type(self.revision) is not str or self.revision != STAGE_REVISION or self.candidate_only is not True or any(
                    getattr(self, flag) is not False for flag in _GATES):
                raise StartupHotelStartupStageError('Offline startup stages cannot authorize execution or release.')
            expected = _derive(checked)
            for name, value in expected.items():
                if type(getattr(self, name)) is not type(value) or getattr(self, name) != value:
                    raise StartupHotelStartupStageError('Startup Hotel stage differs from exact offline derivation: ' + name)
            object.__setattr__(self, 'plan', checked)
        except (ValueError, TypeError, AttributeError, KeyError, RecursionError) as exc:
            if isinstance(exc, StartupHotelStartupStageError):
                raise
            raise StartupHotelStartupStageError('Startup Hotel stage is incomplete, changed or crossed.') from None

    def metadata(self) -> dict:
        self.__post_init__()
        return json.loads(self._metadata_json)

    def pod(self, component: str) -> dict:
        if type(component) is not str or component not in APPLICATIONS:
            raise StartupHotelStartupStageError('Only the exact ten audited Hotel applications are allowed.')
        self.__post_init__()
        name = self.plan.application_pod_name(component)
        return next(item for item in json.loads(self.canonical_json)['items']
                    if item['metadata']['name'] == name)


def build_startup_hotel_startup_stage(plan) -> StartupHotelStartupStage:
    """Render ten gated Pods without executing commands or contacting an API."""
    checked = _checked_plan(plan)
    return StartupHotelStartupStage(plan=checked, **_derive(checked))


def validate_startup_hotel_startup_stage(plan, stage) -> StartupHotelStartupStage:
    """Reconstruct the entire closed-world Pod List, not merely its hashes."""
    if type(stage) is not StartupHotelStartupStage:
        raise StartupHotelStartupStageError('An exact separate startup Hotel stage is required.')
    stage.__post_init__()
    expected = build_startup_hotel_startup_stage(plan)
    if stage != expected:
        raise StartupHotelStartupStageError('Startup Hotel stage is crossed or stale.')
    return expected
