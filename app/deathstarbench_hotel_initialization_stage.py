"""Offline, scheduling-gated Hotel initialization manifests, never authority.

Both one-shot Pods remain unscheduled. Rendering a Pod or a JSON Patch template
does not authorize creating it, removing its gate, running an initializer, or
starting any application. A future leased controller must durably bind the
observed Pod/namespace/cluster/storage identities and consume its dispatch latch
before considering these templates. There is no executor in this module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import re
from typing import Any


STAGE_REVISION = 'hotel-guarded-initialization-stage-candidate-v1'
STAGE_SCHEMA = 'hotel-guarded-initialization-stage-v1'
SCHEDULING_GATE = 'deathstarbench.io/hotel-initialization-controller'
MODES = ('initialize-empty', 'verify')
COMPONENTS = ('attractions', 'geo', 'profile', 'rate', 'recommendation', 'reservation', 'review', 'user')
_UID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')
_GATES = (
    'initializer_authorized', 'runtime_authorized', 'initialization_executed',
    'database_executed', 'application_start_allowed', 'dataset_qualified',
    'runtime_qualified', 'measurement_qualified', 'cloud_qualified',
    'anonymous_pull_qualified', 'public_release', 'released',
)


class GuardedHotelInitializationStageError(ValueError):
    """An offline initialization stage is crossed or would broaden execution."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n'


def _fingerprint(value: Any) -> str:
    return 'sha256:' + hashlib.sha256(_canonical(value).encode()).hexdigest()


def _checked_plan(plan):
    # Late imports preserve the artifact-only boundary and avoid render cycles.
    from .deathstarbench_hotel_runtime import (
        GuardedHotelRuntimePlan, validate_guarded_hotel_runtime_plan,
    )
    if type(plan) is not GuardedHotelRuntimePlan:
        raise GuardedHotelInitializationStageError('An exact guarded Hotel runtime preflight is required.')
    try:
        return validate_guarded_hotel_runtime_plan(plan)
    except (ValueError, TypeError, AttributeError) as exc:
        raise GuardedHotelInitializationStageError('Guarded Hotel runtime preflight is crossed or stale.') from exc


def _derive(plan):
    namespace = plan.namespace
    common = {
        'deathstarbench.io/workload': 'hotel-reservation-v1',
        'deathstarbench.io/candidate-run-id': plan.run_id,
        'deathstarbench.io/hotel-initialization': 'guarded-v1',
        'deathstarbench.io/hotel-initialization-attempt': plan.initialization_attempt_id,
    }
    annotations = {
        'deathstarbench.io/candidate-run-id': plan.run_id,
        'deathstarbench.io/hotel-runtime-fingerprint': plan.fingerprint,
        'deathstarbench.io/hotel-initialization-stage-revision': STAGE_REVISION,
        'deathstarbench.io/hotel-initialization-attempt': plan.initialization_attempt_id,
        'deathstarbench.io/application-start-allowed': 'false',
    }

    def metadata(name, *, mode=None):
        labels = dict(common)
        if mode is not None:
            labels.update({'app.kubernetes.io/component': 'hotel-initialize' if mode == 'initialize-empty' else 'hotel-verify',
                           'deathstarbench.io/role': 'application'})
        return {'name': name, 'namespace': namespace, 'labels': labels,
                'annotations': dict(annotations)}

    pods = []
    for mode in MODES:
        name = plan.initializer_pod_name if mode == 'initialize-empty' else plan.verifier_pod_name
        pods.append({
            'apiVersion': 'v1', 'kind': 'Pod', 'metadata': metadata(name, mode=mode),
            'spec': {
                'schedulingGates': [{'name': SCHEDULING_GATE}],
                'restartPolicy': 'Never', 'activeDeadlineSeconds': 420,
                'terminationGracePeriodSeconds': 10,
                'automountServiceAccountToken': False, 'enableServiceLinks': False,
                'dnsPolicy': 'ClusterFirst', 'schedulerName': 'default-scheduler',
                'hostNetwork': False, 'hostPID': False, 'hostIPC': False,
                'shareProcessNamespace': False,
                'nodeSelector': {'deathstarbench.io/role': 'application',
                                 'kubernetes.io/arch': 'arm64' if plan.application_architecture == 'aarch64' else 'amd64'},
                'securityContext': {'runAsNonRoot': True, 'runAsUser': 65532, 'runAsGroup': 65532,
                                    'seccompProfile': {'type': 'RuntimeDefault'}},
                'containers': [{
                    'name': 'hotel-initialize', 'image': plan.application_image,
                    'imagePullPolicy': 'Always', 'workingDir': '/workspace',
                    'command': ['/go/bin/hotel-initialize'], 'args': [mode],
                    'env': [{'name': key, 'value': value}
                            for key, value in sorted(plan.initialization_environment().items())],
                    'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                                        'capabilities': {'drop': ['ALL']}},
                    'resources': {'requests': {'cpu': '100m', 'memory': '64Mi'},
                                  'limits': {'cpu': '1', 'memory': '256Mi'}},
                }],
            },
        })

    init_selector = {'matchLabels': dict(common)}
    mongo_selectors = [{
        'matchLabels': {'deathstarbench.io/workload': 'hotel-reservation-v1',
                        'app.kubernetes.io/component': 'mongodb-' + component},
    } for component in COMPONENTS]
    egress = [{
        'to': [{'podSelector': selector}], 'ports': [{'port': 27017, 'protocol': 'TCP'}],
    } for selector in mongo_selectors]
    egress.append({
        'to': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'kube-system'}},
                'podSelector': {'matchLabels': {'k8s-app': 'kube-dns'}}}],
        'ports': [{'port': 53, 'protocol': 'TCP'}, {'port': 53, 'protocol': 'UDP'}],
    })
    policies = [{
        'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
        'metadata': metadata('hotel-initialization-egress-and-deny-ingress'),
        'spec': {'podSelector': init_selector, 'policyTypes': ['Ingress', 'Egress'],
                 'ingress': [], 'egress': egress},
    }]
    for component, selector in zip(COMPONENTS, mongo_selectors, strict=True):
        policies.append({
            'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
            'metadata': metadata('hotel-initialization-to-mongodb-' + component),
            'spec': {'podSelector': selector, 'policyTypes': ['Ingress'],
                     'ingress': [{'from': [{'podSelector': init_selector}],
                                  'ports': [{'port': 27017, 'protocol': 'TCP'}]}]},
        })
    identity = {
        'schema_version': 1, 'candidate_schema': STAGE_SCHEMA, 'revision': STAGE_REVISION,
        'candidate_only': True, **{name: False for name in _GATES},
        'run_id': plan.run_id, 'namespace': namespace, 'plan_fingerprint': plan.fingerprint,
        'initialization_attempt_id': plan.initialization_attempt_id,
        'application_image': plan.application_image, 'application_architecture': plan.application_architecture,
        'pod_names': {mode: pod['metadata']['name'] for mode, pod in zip(MODES, pods, strict=True)},
        'scheduling_gate': SCHEDULING_GATE,
        'scope': 'offline-gated-initialize-empty-and-global-readonly-verify-only',
        'execution_authority': 'none-future-leased-controller-with-durable-single-use-dispatch-required',
        'recovery': 'never-replay-initialization-after-dispatch-intent-even-if-dataset-is-complete',
        'application_deployments_included': 0, 'database_count': 8,
    }
    fingerprint = _fingerprint(identity)
    for item in (*pods, *policies):
        item['metadata']['annotations']['deathstarbench.io/hotel-initialization-stage-fingerprint'] = fingerprint
    payload = _canonical({'apiVersion': 'v1', 'kind': 'List', 'items': [*policies, *pods]})
    metadata_json = _canonical({**identity, 'fingerprint': fingerprint,
                                'rendered_manifest_sha256': 'sha256:' + hashlib.sha256(payload.encode()).hexdigest()})
    return {'run_id': plan.run_id, 'namespace': namespace, 'plan_fingerprint': plan.fingerprint,
            'fingerprint': fingerprint, 'canonical_json': payload,
            'rendered_manifest_sha256': 'sha256:' + hashlib.sha256(payload.encode()).hexdigest(),
            '_metadata_json': metadata_json}


@dataclass(frozen=True, slots=True)
class GuardedHotelInitializationStage:
    plan: Any = field(repr=False)
    run_id: str
    namespace: str
    plan_fingerprint: str
    fingerprint: str
    canonical_json: str
    rendered_manifest_sha256: str
    _metadata_json: str = field(repr=False)
    candidate_only: bool = field(init=False, default=True)
    initializer_authorized: bool = field(init=False, default=False)
    application_start_allowed: bool = field(init=False, default=False)
    released: bool = field(init=False, default=False)

    def __post_init__(self):
        checked = _checked_plan(self.plan)
        if (self.candidate_only is not True or any(getattr(self, name) is not False
                for name in ('initializer_authorized', 'application_start_allowed', 'released'))):
            raise GuardedHotelInitializationStageError('Offline initialization stages cannot authorize execution.')
        expected = _derive(checked)
        for name, value in expected.items():
            if type(getattr(self, name)) is not type(value) or getattr(self, name) != value:
                raise GuardedHotelInitializationStageError('Guarded initialization stage differs from exact derivation: ' + name)
        object.__setattr__(self, 'plan', checked)

    def metadata(self) -> dict:
        return json.loads(self._metadata_json)

    def pod(self, mode: str) -> dict:
        if type(mode) is not str or mode not in MODES:
            raise GuardedHotelInitializationStageError('Only exact initialize-empty and verify Pod modes are allowed.')
        name = self.metadata()['pod_names'][mode]
        return next(item for item in json.loads(self.canonical_json)['items']
                    if item['kind'] == 'Pod' and item['metadata']['name'] == name)


def build_guarded_hotel_initialization_stage(plan) -> GuardedHotelInitializationStage:
    """Render two gated Pods and nine policies without commands or API/network calls."""
    checked = _checked_plan(plan)
    return GuardedHotelInitializationStage(plan=checked, **_derive(checked))


def validate_guarded_hotel_initialization_stage(plan, stage) -> GuardedHotelInitializationStage:
    expected = build_guarded_hotel_initialization_stage(plan)
    if type(stage) is not GuardedHotelInitializationStage or stage != expected:
        raise GuardedHotelInitializationStageError('Guarded initialization stage is crossed or stale.')
    return expected


def guarded_hotel_scheduling_gate_release_patch(plan, stage, mode: str, pod_uid: str) -> list[dict]:
    """Return an OFFLINE strict patch template, never execution permission.

    A live controller must first validate admission defaults and the complete
    observed Pod, persist the UID and single-use dispatch latch, and check live
    cluster/namespace/storage/support continuity. The strict whole-spec test is
    intentionally fail-closed on admission mutations; this module does not
    silently permit defaults or emit kubectl/shell execution commands.
    """
    checked = validate_guarded_hotel_initialization_stage(plan, stage)
    if (type(pod_uid) is not str or not _UID.fullmatch(pod_uid)
            or pod_uid == '00000000-0000-0000-0000-000000000000'):
        raise GuardedHotelInitializationStageError('An exact nonzero observed Pod UID is required.')
    pod = checked.pod(mode)
    return [
        {'op': 'test', 'path': '/metadata/uid', 'value': pod_uid},
        {'op': 'test', 'path': '/metadata/name', 'value': pod['metadata']['name']},
        {'op': 'test', 'path': '/metadata/namespace', 'value': checked.namespace},
        {'op': 'test', 'path': '/metadata/labels', 'value': pod['metadata']['labels']},
        {'op': 'test', 'path': '/metadata/annotations', 'value': pod['metadata']['annotations']},
        {'op': 'test', 'path': '/spec', 'value': pod['spec']},
        {'op': 'remove', 'path': '/spec/schedulingGates'},
    ]
