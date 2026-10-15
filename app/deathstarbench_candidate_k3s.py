"""Guarded K3s commands and observations for support-only candidates.

Hotel and Media application/frontends remain at zero replicas: Hotel's current
startup seeds MongoDB and must not run during this checkpoint. These commands
do not initialize data, scale applications, delete/prune resources, or authorize
public execution. Kubernetes multi-object apply is not transactional; callers
must persist intent and reconcile exact object UIDs after response loss.
Apply fails closed on replacement of any already observed object, including
support Pods/controllers; transparent restart recovery is not qualified here.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from datetime import datetime
import hashlib
import inspect
import ipaddress
import json
import re
import shlex
from typing import Any
import zlib

from .deathstarbench_candidate_runtime import CandidateRuntimePlan
from .deathstarbench_candidate_stage import (
    CandidateSupportStageBundle, validate_candidate_support_stage,
)
from .k3s_runtime import K3S_BINARY


OBSERVATION_SCHEMA = 'candidate-k3s-observation-v1'
SUPPORT_SCHEMA = 'candidate-k3s-support-attestation-v1'
RECONCILIATION_SCHEMA = 'candidate-k3s-phase-reconciliation-v1'
MAX_JSON_BYTES = 4 * 1024 * 1024
_ROLES = ('application', 'cache', 'control', 'database')
_OWNERSHIP_KEYS = (
    'deathstarbench.io/candidate-run-id',
    'deathstarbench.io/candidate-runtime-fingerprint',
    'deathstarbench.io/candidate-stage-revision',
    'deathstarbench.io/candidate-stage-fingerprint',
    'deathstarbench.io/application-start-allowed',
)


class CandidateK3sError(ValueError):
    """Candidate K3s evidence cannot prove the exact paused-app scope."""


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _strict_json(raw):
    if isinstance(raw, bytes):
        if len(raw) > MAX_JSON_BYTES:
            raise CandidateK3sError('Candidate K3s JSON exceeds its bound.')
        try:
            raw = raw.decode('utf-8')
        except UnicodeError as exc:
            raise CandidateK3sError('Candidate K3s JSON is not UTF-8.') from exc
    try:
        bounded_text = isinstance(raw, str) and len(raw.encode('utf-8')) <= MAX_JSON_BYTES
    except UnicodeError as exc:
        raise CandidateK3sError('Candidate K3s JSON is not valid UTF-8 text.') from exc
    if not bounded_text:
        raise CandidateK3sError('Candidate K3s JSON must be bounded text.')

    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise CandidateK3sError('Candidate K3s JSON contains duplicate keys.')
            result[key] = value
        return result

    def constant(_value):
        raise CandidateK3sError('Candidate K3s JSON contains a non-finite value.')

    try:
        result = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
        pending = [(result, 0)]
        while pending:
            value, depth = pending.pop()
            if depth > 64:
                raise CandidateK3sError('Candidate K3s JSON exceeds its nesting bound.')
            if isinstance(value, dict):
                pending.extend((child, depth + 1) for child in (*value.keys(), *value.values()))
            elif isinstance(value, list):
                pending.extend((child, depth + 1) for child in value)
            elif isinstance(value, str):
                value.encode('utf-8')
        _canonical(result).encode('utf-8')
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        if isinstance(exc, CandidateK3sError):
            raise
        raise CandidateK3sError('Candidate K3s JSON is invalid.') from exc
    return result


def _mapping(value, label):
    if not isinstance(value, Mapping):
        raise CandidateK3sError(f'{label} must be an object.')
    return value


def _array(value, label):
    if not isinstance(value, list):
        raise CandidateK3sError(f'{label} must be an array.')
    return value


def _uid(value, label):
    if (
        not isinstance(value, str)
        or not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', value)
        or value == '00000000-0000-0000-0000-000000000000'
    ):
        raise CandidateK3sError(f'{label} UID is invalid.')
    return value


def _integer(value, label, minimum=0):
    if type(value) is not int or not minimum <= value <= 2**31 - 1:
        raise CandidateK3sError(f'{label} must be a bounded integer.')
    return value


def _timestamp(value):
    if (
        not isinstance(value, str) or len(value) > 40
        or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})', value)
    ):
        raise CandidateK3sError('Candidate running timestamp is invalid.')
    try:
        if datetime.fromisoformat(value.replace('Z', '+00:00')).tzinfo is None:
            raise CandidateK3sError('Candidate running timestamp lacks a timezone.')
    except ValueError as exc:
        raise CandidateK3sError('Candidate running timestamp is invalid.') from exc


def _subset(actual, expected, label):
    """Compare submitted fields, with narrow Kubernetes omission defaults."""
    if isinstance(expected, Mapping):
        actual = _mapping(actual, label)
        if not expected and actual:
            raise CandidateK3sError(f'{label} empty selector/object drifted.')
        for key, value in expected.items():
            if key not in actual:
                if key == 'env' and value == [] and label.endswith(('container', 'containers[0]')):
                    continue
                if key == 'storageClassName' and value == '' and label.startswith('PersistentVolume/'):
                    continue
                if key in {'ingress', 'egress'} and value == [] and label.startswith('NetworkPolicy/'):
                    continue
                raise CandidateK3sError(f'{label} is missing {key}.')
            _subset(actual[key], value, f'{label}.{key}')
    elif isinstance(expected, list):
        actual = _array(actual, label)
        if len(actual) != len(expected):
            raise CandidateK3sError(f'{label} array inventory drifted.')
        for index, (left, right) in enumerate(zip(actual, expected, strict=True)):
            _subset(left, right, f'{label}[{index}]')
    elif type(actual) is not type(expected) or actual != expected:
        raise CandidateK3sError(f'{label} drifted.')


def _metadata(actual, expected, label, *, uid_required=True):
    actual = _mapping(actual, f'{label} metadata')
    _subset(actual, expected, f'{label} metadata')
    if actual.get('deletionTimestamp') is not None:
        raise CandidateK3sError(f'{label} is being deleted.')
    if uid_required:
        _uid(actual.get('uid'), label)
    annotations = _mapping(actual.get('annotations', {}), f'{label} annotations')
    expected_annotations = expected.get('annotations', {})
    for key in _OWNERSHIP_KEYS:
        if annotations.get(key) != expected_annotations.get(key):
            raise CandidateK3sError(f'{label} ownership annotation drifted.')
    if 'labels' in expected:
        labels = dict(_mapping(actual.get('labels'), f'{label} labels'))
        if label.startswith(('Pod/', 'ReplicaSet/')):
            labels.pop('pod-template-hash', None)
        if labels != expected['labels']:
            raise CandidateK3sError(f'{label} exact label inventory drifted.')
    return actual


def _owner(actual, kind, name, uid, label):
    expected = {
        'apiVersion': 'apps/v1', 'kind': kind, 'name': name, 'uid': uid,
        'controller': True, 'blockOwnerDeletion': True,
    }
    if _canonical(actual) != _canonical([expected]):
        raise CandidateK3sError(f'{label} controller owner reference drifted.')


def _default_fields(actual, expected, defaults, label, dynamic=()):
    """Reject fields outside the submission and narrowly documented defaults."""
    for key in set(actual) - set(expected) - set(dynamic):
        if key not in defaults or not any(_canonical(actual[key]) == _canonical(value) for value in defaults[key]):
            raise CandidateK3sError(f'{label} has unapproved field {key}.')


def _pod_spec(actual, expected, label, *, pod=False):
    actual = dict(_mapping(actual, f'{label} spec'))
    expected = dict(expected)
    tolerations = actual.pop('tolerations', [])
    expected_tolerations = expected.pop('tolerations', [])
    allowed_defaults = [
        {'key': f'node.kubernetes.io/{key}', 'operator': 'Exists', 'effect': 'NoExecute', 'tolerationSeconds': 300}
        for key in ('not-ready', 'unreachable')
    ] if pod else []
    retained = []
    for item in _array(tolerations, f'{label} tolerations'):
        if any(_canonical(item) == _canonical(default) for default in allowed_defaults):
            continue
        retained.append(item)
    if _canonical(retained) != _canonical(expected_tolerations):
        raise CandidateK3sError(f'{label} tolerations drifted.')
    _subset(actual, expected, f'{label} spec')
    _default_fields(actual, expected, {
        'restartPolicy': ('Always',), 'dnsPolicy': ('ClusterFirst',),
        'schedulerName': ('default-scheduler',), 'securityContext': (None, {}),
        'serviceAccountName': ('default',), 'serviceAccount': ('default',),
        'preemptionPolicy': ('PreemptLowerPriority',), 'priority': (0,),
        'imagePullSecrets': (None, []), 'initContainers': (None, []),
        'ephemeralContainers': (None, []), 'hostNetwork': (False,),
        'hostPID': (False,), 'hostIPC': (False,), 'shareProcessNamespace': (False,),
        'affinity': (None, {}), 'hostAliases': (None, []), 'dnsConfig': (None, {}),
        'schedulingGates': (None, []), 'resourceClaims': (None, []),
        'topologySpreadConstraints': (None, []), 'volumes': (None, []),
    }, f'{label} spec', dynamic=('nodeName',) if pod else ())
    for key in ('initContainers', 'ephemeralContainers', 'imagePullSecrets'):
        if actual.get(key) not in (None, []):
            raise CandidateK3sError(f'{label} has unapproved {key}.')
    for key in ('hostNetwork', 'hostPID', 'hostIPC', 'shareProcessNamespace'):
        if actual.get(key, False) is not False:
            raise CandidateK3sError(f'{label} has unapproved host/process access.')
    if actual.get('securityContext') not in (None, {}):
        raise CandidateK3sError(f'{label} has an unapproved security context.')
    if actual.get('serviceAccountName', 'default') != 'default':
        raise CandidateK3sError(f'{label} uses an unapproved service account.')
    if actual.get('restartPolicy', 'Always') != 'Always' or actual.get('dnsPolicy', 'ClusterFirst') != 'ClusterFirst':
        raise CandidateK3sError(f'{label} restart or DNS policy drifted.')
    if actual.get('volumes', []) != expected.get('volumes', []):
        raise CandidateK3sError(f'{label} volume inventory drifted.')
    if actual.get('nodeSelector') != expected.get('nodeSelector'):
        raise CandidateK3sError(f'{label} placement selectors drifted.')
    for key in ('affinity', 'hostAliases', 'dnsConfig', 'schedulingGates', 'resourceClaims', 'topologySpreadConstraints'):
        if actual.get(key) not in (None, {}, []):
            raise CandidateK3sError(f'{label} contains unapproved {key}.')
    containers = _array(actual.get('containers'), f'{label} containers')
    if len(containers) != 1:
        raise CandidateK3sError(f'{label} must contain exactly one locked container.')
    observed = containers[0]
    approved = expected['containers'][0]
    _subset(observed, approved, f'{label} container')
    _default_fields(observed, approved, {
        'terminationMessagePath': ('/dev/termination-log',),
        'terminationMessagePolicy': ('File',), 'resources': (None, {}),
        'securityContext': (None, {}), 'stdin': (False,), 'stdinOnce': (False,), 'tty': (False,),
        'envFrom': (None, []), 'lifecycle': (None, {}), 'livenessProbe': (None, {}),
        'startupProbe': (None, {}), 'volumeDevices': (None, []), 'args': (None, []),
        'workingDir': (None, ''), 'command': (None, []),
    }, f'{label} container')
    probe = _mapping(observed.get('readinessProbe'), f'{label} readiness probe')
    _default_fields(probe, approved['readinessProbe'], {'initialDelaySeconds': (0,), 'successThreshold': (1,)}, f'{label} readiness probe')
    socket = _mapping(probe.get('tcpSocket'), f'{label} readiness TCP socket')
    _default_fields(socket, approved['readinessProbe']['tcpSocket'], {'host': ('',)}, f'{label} readiness TCP socket')
    for key in ('resources', 'securityContext'):
        if observed.get(key) not in (None, {}):
            raise CandidateK3sError(f'{label} container has unapproved {key}.')
    for key in ('envFrom', 'lifecycle', 'livenessProbe', 'startupProbe'):
        if observed.get(key) not in (None, [], {}):
            raise CandidateK3sError(f'{label} container has unapproved {key}.')
    for key in ('args', 'command', 'workingDir', 'volumeDevices'):
        if key not in approved and observed.get(key) not in (None, [], ''):
            raise CandidateK3sError(f'{label} container has unapproved {key}.')
    for port in observed.get('ports', []):
        if port.get('hostPort', 0) != 0 or port.get('hostIP', '') != '':
            raise CandidateK3sError(f'{label} container exposes a host port.')
        if set(port) - {'name', 'containerPort', 'protocol', 'hostPort', 'hostIP'}:
            raise CandidateK3sError(f'{label} container port has an unapproved field.')
    for item in observed.get('env', []):
        if set(item) != {'name', 'value'}:
            raise CandidateK3sError(f'{label} container environment source drifted.')


def _status_counts(resource, target, label, *, complete):
    status = _mapping(resource.get('status', {}), f'{label} status')
    generation = _integer(resource['metadata'].get('generation'), f'{label} generation', 1)
    observed_generation = _integer(status.get('observedGeneration', 0), f'{label} observed generation')
    if complete and observed_generation != generation:
        raise CandidateK3sError(f'{label} has not observed its exact generation.')
    if target == 0:
        for key in ('replicas', 'readyReplicas', 'availableReplicas', 'updatedReplicas', 'unavailableReplicas', 'terminatingReplicas'):
            if _integer(status.get(key, 0), f'{label} {key}') != 0:
                raise CandidateK3sError(f'{label} application has nonzero replica status.')
    elif complete:
        for key in ('replicas', 'readyReplicas', 'availableReplicas'):
            if _integer(status.get(key), f'{label} {key}') != 1:
                raise CandidateK3sError(f'{label} support is not exactly one ready replica.')
        if resource['kind'] == 'Deployment' and _integer(status.get('updatedReplicas'), f'{label} updated replicas') != 1:
            raise CandidateK3sError(f'{label} support deployment is not updated.')
        if _integer(status.get('unavailableReplicas', 0), f'{label} unavailable replicas') != 0:
            raise CandidateK3sError(f'{label} support has unavailable replicas.')


def _nodes(by_name, context):
    if set(by_name) != set(context['role_nodes'].values()):
        raise CandidateK3sError('Candidate node inventory differs from the exact role mapping.')
    result = []
    for role, name in sorted(context['role_nodes'].items()):
        node = by_name[name]
        if node.get('apiVersion') != 'v1' or node.get('spec', {}).get('unschedulable', False) is not False:
            raise CandidateK3sError(f'Node/{name} API or scheduling state drifted.')
        metadata = _mapping(node.get('metadata'), 'Node metadata')
        uid = _uid(metadata.get('uid'), f'Node/{name}')
        if metadata.get('deletionTimestamp') is not None:
            raise CandidateK3sError('Candidate node is being deleted.')
        labels = _mapping(metadata.get('labels'), 'Node labels')
        architecture = context['application_arch'] if role == 'application' else 'amd64'
        if any(labels.get(key) != value for key, value in {
            'deathstarbench.io/role': role, 'kubernetes.io/arch': architecture, 'kubernetes.io/os': 'linux',
        }.items()):
            raise CandidateK3sError(f'Node/{name} role or architecture drifted.')
        status = _mapping(node.get('status'), 'Node status')
        if _mapping(status.get('nodeInfo'), 'Node runtime info').get('architecture') != architecture:
            raise CandidateK3sError(f'Node/{name} runtime architecture drifted.')
        ready = [item for item in _array(status.get('conditions'), 'Node conditions') if _mapping(item, 'Node condition').get('type') == 'Ready']
        if len(ready) != 1 or ready[0].get('status') != 'True':
            raise CandidateK3sError(f'Node/{name} is not Ready.')
        result.append({'role': role, 'name': name, 'uid': uid, 'architecture': architecture})
    return result


def _system_object(item, namespace):
    """Only the two Kubernetes-created default namespace objects are allowed."""
    metadata = _mapping(item.get('metadata'), 'System object metadata')
    if item.get('apiVersion') != 'v1' or metadata.get('namespace') != namespace or metadata.get('deletionTimestamp') is not None:
        raise CandidateK3sError('Default namespace object identity drifted.')
    _uid(metadata.get('uid'), 'Default namespace object')
    if item['kind'] == 'ServiceAccount' and metadata.get('name') == 'default':
        if item.get('secrets') not in (None, []) or item.get('imagePullSecrets') not in (None, []):
            raise CandidateK3sError('Default service account contains unexpected credentials.')
        return True
    if item['kind'] == 'ConfigMap' and metadata.get('name') == 'kube-root-ca.crt':
        data = _mapping(item.get('data'), 'Kubernetes root CA ConfigMap data')
        if set(data) != {'ca.crt'} or not isinstance(data['ca.crt'], str) or not data['ca.crt'].startswith('-----BEGIN CERTIFICATE-----'):
            raise CandidateK3sError('Default root CA ConfigMap has unexpected data.')
        if item.get('binaryData') not in (None, {}):
            raise CandidateK3sError('Default root CA ConfigMap has unexpected binary data.')
        return True
    return False


def _validate_inventory(inventory, context, *, complete):
    document = _mapping(inventory, 'Candidate Kubernetes inventory')
    if set(document) - {'apiVersion', 'kind', 'metadata', 'items'} or document.get('apiVersion') != 'v1' or document.get('kind') != 'List':
        raise CandidateK3sError('Candidate inventory must be an exact Kubernetes List.')
    items = _array(document.get('items'), 'Candidate Kubernetes items')
    if len(items) > 1024:
        raise CandidateK3sError('Candidate Kubernetes inventory is too large.')
    expected = {}
    for item in context['manifest']['items']:
        expected.setdefault(item['kind'], {})[item['metadata']['name']] = item
    by_kind = {}
    identities = set()
    for item in items:
        item = _mapping(item, 'Candidate Kubernetes object')
        kind = item.get('kind')
        metadata = _mapping(item.get('metadata'), 'Candidate Kubernetes metadata')
        name = metadata.get('name')
        if not isinstance(kind, str) or not isinstance(name, str) or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', name):
            raise CandidateK3sError('Candidate Kubernetes object identity is invalid.')
        namespace = metadata.get('namespace')
        if kind in {'Namespace', 'PersistentVolume', 'Node'}:
            if namespace is not None:
                raise CandidateK3sError('Cluster-scoped candidate object has a namespace.')
        elif namespace != context['namespace']:
            raise CandidateK3sError('Candidate namespaced object crossed its namespace.')
        identity = (kind, name, namespace)
        if identity in identities:
            raise CandidateK3sError('Candidate Kubernetes inventory repeats an object.')
        identities.add(identity)
        if kind in {'ServiceAccount', 'ConfigMap'} and _system_object(item, context['namespace']):
            continue
        if kind not in {*expected, 'Pod', 'ReplicaSet', 'Node'}:
            raise CandidateK3sError('Candidate namespace contains an unexpected resource kind.')
        if kind in expected and name not in expected[kind]:
            raise CandidateK3sError(f'Candidate {kind} inventory contains an unexpected object.')
        by_kind.setdefault(kind, {})[name] = item
    nodes = _nodes(by_kind.get('Node', {}), context)
    namespaces = by_kind.get('Namespace', {})
    if not namespaces:
        if complete or any(kind != 'Node' for kind in by_kind):
            raise CandidateK3sError('Candidate namespace is missing or has orphaned objects.')
        return {'namespace_uid': None, 'nodes': nodes, 'support_pods': [], 'blocked_applications': []}
    namespace = namespaces[context['namespace']]
    namespace_metadata = _metadata(namespace.get('metadata'), expected['Namespace'][context['namespace']]['metadata'], 'Namespace')
    if namespace.get('status', {}).get('phase') != 'Active':
        raise CandidateK3sError('Candidate namespace is not Active.')
    for kind, objects in expected.items():
        if complete and set(by_kind.get(kind, {})) != set(objects):
            raise CandidateK3sError(f'Candidate {kind} inventory is incomplete.')
        for name, observed in by_kind.get(kind, {}).items():
            approved = objects[name]
            label = f'{kind}/{name}'
            if observed.get('apiVersion') != approved['apiVersion']:
                raise CandidateK3sError(f'{label} API version drifted.')
            _metadata(observed.get('metadata'), approved['metadata'], label)
            if observed['metadata'].get('ownerReferences') not in (None, []):
                raise CandidateK3sError(f'{label} has an unexpected owner.')
            if kind == 'Namespace':
                if observed.get('spec', {}).get('finalizers', ['kubernetes']) != ['kubernetes']:
                    raise CandidateK3sError('Candidate namespace finalizers drifted.')
                continue
            _subset(observed.get('spec'), approved['spec'], label + ' spec')
            if kind == 'Deployment':
                spec = observed['spec']
                _default_fields(spec, approved['spec'], {
                    'progressDeadlineSeconds': (600,), 'minReadySeconds': (0,), 'paused': (False,),
                }, label + ' spec')
                component = name
                role = context['placement'][component]
                replicas = 0 if role == 'application' else 1
                if type(spec.get('replicas')) is not int or spec['replicas'] != replicas or spec.get('paused', False) is not False:
                    raise CandidateK3sError(f'{label} replica/paused contract drifted.')
                if spec.get('selector') != approved['spec']['selector']:
                    raise CandidateK3sError(f'{label} exact selector inventory drifted.')
                _pod_spec(spec['template']['spec'], approved['spec']['template']['spec'], label)
                _metadata(spec['template']['metadata'], approved['spec']['template']['metadata'], label + ' template', uid_required=False)
                _status_counts(observed, replicas, label, complete=complete)
            elif kind == 'NetworkPolicy':
                spec = dict(observed['spec'])
                for key in ('ingress', 'egress'):
                    if approved['spec'].get(key) == [] and key not in spec:
                        spec[key] = []
                if _canonical(spec) != _canonical(approved['spec']):
                    raise CandidateK3sError(f'{label} exact policy graph drifted.')
            elif kind == 'Service':
                spec = observed['spec']
                _default_fields(spec, approved['spec'], {
                    'ipFamilyPolicy': ('SingleStack',), 'sessionAffinity': ('None',),
                    'internalTrafficPolicy': ('Cluster',), 'externalIPs': (None, []),
                    'externalName': (None, ''), 'publishNotReadyAddresses': (False,),
                    'healthCheckNodePort': (0,),
                }, label + ' spec', dynamic=('clusterIP', 'clusterIPs', 'ipFamilies'))
                if spec.get('externalIPs') not in (None, []) or spec.get('externalName') not in (None, ''):
                    raise CandidateK3sError(f'{label} exposes an unapproved external path.')
                if spec.get('publishNotReadyAddresses', False) is not False or spec.get('sessionAffinity', 'None') != 'None':
                    raise CandidateK3sError(f'{label} service behavior drifted.')
                for port, approved_port in zip(spec['ports'], approved['spec']['ports'], strict=True):
                    if 'nodePort' not in approved_port and port.get('nodePort', 0) != 0:
                        raise CandidateK3sError(f'{label} exposes an unexpected NodePort.')
                if complete:
                    try:
                        cluster_ip = ipaddress.ip_address(spec.get('clusterIP'))
                    except ValueError as exc:
                        raise CandidateK3sError(f'{label} ClusterIP is invalid.') from exc
                    if cluster_ip not in ipaddress.ip_network('10.43.0.0/16') or str(cluster_ip) != spec['clusterIP']:
                        raise CandidateK3sError(f'{label} ClusterIP is outside the pinned K3s network.')
                    if spec.get('clusterIPs', [str(cluster_ip)]) != [str(cluster_ip)] or spec.get('ipFamilies', ['IPv4']) != ['IPv4']:
                        raise CandidateK3sError(f'{label} address-family inventory drifted.')
            elif kind in {'PersistentVolume', 'PersistentVolumeClaim'}:
                if 'volume.beta.kubernetes.io/storage-class' in observed['metadata'].get('annotations', {}):
                    raise CandidateK3sError(f'{label} has an unexpected legacy storage class.')
                spec = observed['spec']
                if spec.get('storageClassName', '') != '' or spec.get('volumeMode', 'Filesystem') != 'Filesystem':
                    raise CandidateK3sError(f'{label} storage binding mode drifted.')
                if complete and observed.get('status', {}).get('phase') != 'Bound':
                    raise CandidateK3sError(f'{label} is not Bound.')
                if kind == 'PersistentVolume':
                    if spec.get('hostPath') != approved['spec']['hostPath']:
                        raise CandidateK3sError(f'{label} host path drifted.')
                    if set(spec) - set(approved['spec']) - {'claimRef'}:
                        raise CandidateK3sError(f'{label} has unapproved storage fields.')
                    claim_reference = _mapping(spec.get('claimRef'), f'{label} claim reference')
                    _default_fields(claim_reference, approved['spec']['claimRef'], {
                        'apiVersion': ('v1',), 'kind': ('PersistentVolumeClaim',),
                    }, f'{label} claim reference', dynamic=('uid', 'resourceVersion'))
                    if 'uid' in claim_reference:
                        _uid(claim_reference['uid'], f'{label} claim reference')
                    claim_name = approved['spec']['claimRef']['name']
                    pvc = by_kind.get('PersistentVolumeClaim', {}).get(claim_name)
                    if complete and (pvc is None or spec.get('claimRef', {}).get('uid') != pvc['metadata'].get('uid')):
                        raise CandidateK3sError(f'{label} PVC UID binding drifted.')
                else:
                    _default_fields(spec, approved['spec'], {
                        'volumeMode': ('Filesystem',), 'dataSource': (None,), 'dataSourceRef': (None,),
                        'selector': (None, {}), 'volumeAttributesClassName': (None,),
                    }, label + ' spec')
                    if spec.get('resources') != approved['spec']['resources']:
                        raise CandidateK3sError(f'{label} request/limit inventory drifted.')
                    if spec.get('dataSource') is not None or spec.get('dataSourceRef') is not None or spec.get('selector') not in (None, {}):
                        raise CandidateK3sError(f'{label} has an unapproved data source or selector.')
                    if complete and observed.get('status', {}).get('capacity') != {'storage': '10Gi'}:
                        raise CandidateK3sError(f'{label} bound capacity drifted.')
    deployments = by_kind.get('Deployment', {})
    replicasets = {}
    for name, rs in by_kind.get('ReplicaSet', {}).items():
        metadata = _mapping(rs.get('metadata'), f'ReplicaSet/{name} metadata')
        labels = _mapping(metadata.get('labels'), 'ReplicaSet labels')
        component = labels.get('app.kubernetes.io/component')
        if component not in deployments or component in replicasets:
            raise CandidateK3sError('Candidate ReplicaSet component inventory drifted.')
        deployment = deployments[component]
        approved_template = expected['Deployment'][component]['spec']['template']
        _metadata(metadata, {
            'namespace': context['namespace'], 'labels': approved_template['metadata']['labels'],
            # The Deployment controller copies Deployment annotations onto
            # ReplicaSet metadata; only its pod template inherits template-only
            # architecture/image annotations.
            'annotations': expected['Deployment'][component]['metadata']['annotations'],
        }, f'ReplicaSet/{name}')
        _owner(metadata.get('ownerReferences'), 'Deployment', component, deployment['metadata']['uid'], f'ReplicaSet/{name}')
        spec = _mapping(rs.get('spec'), f'ReplicaSet/{name} spec')
        _default_fields(spec, {'replicas': None, 'selector': None, 'template': None}, {'minReadySeconds': (0,)}, f'ReplicaSet/{name} spec')
        desired = 0 if context['placement'][component] == 'application' else 1
        if type(spec.get('replicas')) is not int or spec['replicas'] != desired:
            raise CandidateK3sError('Candidate ReplicaSet replicas drifted.')
        template = _mapping(spec.get('template'), 'ReplicaSet template')
        _metadata(template.get('metadata'), approved_template['metadata'], f'ReplicaSet/{name} template', uid_required=False)
        _pod_spec(template.get('spec'), approved_template['spec'], f'ReplicaSet/{name}')
        selector = _mapping(spec.get('selector'), 'ReplicaSet selector')
        selector_labels = dict(_mapping(selector.get('matchLabels'), 'ReplicaSet selector labels'))
        pod_hash = selector_labels.pop('pod-template-hash', None)
        if not isinstance(pod_hash, str) or not re.fullmatch(r'[a-z0-9]{1,16}', pod_hash):
            raise CandidateK3sError('Candidate ReplicaSet template hash is invalid.')
        if selector_labels != approved_template['metadata']['labels'] or template['metadata']['labels'].get('pod-template-hash') != pod_hash or labels.get('pod-template-hash') != pod_hash:
            raise CandidateK3sError('Candidate ReplicaSet selector/template hash drifted.')
        if not name.startswith(component + '-') or selector.get('matchExpressions') not in (None, []):
            raise CandidateK3sError('Candidate ReplicaSet name/selector drifted.')
        _status_counts(rs, desired, f'ReplicaSet/{name}', complete=complete)
        replicasets[component] = rs
    support_pods = {}
    for name, pod in by_kind.get('Pod', {}).items():
        metadata = _mapping(pod.get('metadata'), f'Pod/{name} metadata')
        component = metadata.get('labels', {}).get('app.kubernetes.io/component')
        if component not in deployments or component in support_pods:
            raise CandidateK3sError('Candidate Pod component inventory drifted.')
        if context['placement'][component] == 'application':
            raise CandidateK3sError('Candidate application Pod exists before initialization is authorized.')
        rs = replicasets.get(component)
        if rs is None:
            raise CandidateK3sError('Candidate support Pod has no exact ReplicaSet controller.')
        template = expected['Deployment'][component]['spec']['template']
        _metadata(metadata, {'namespace': context['namespace'], **template['metadata']}, f'Pod/{name}')
        _owner(metadata.get('ownerReferences'), 'ReplicaSet', rs['metadata']['name'], rs['metadata']['uid'], f'Pod/{name}')
        if metadata.get('labels', {}).get('pod-template-hash') != rs['spec']['selector']['matchLabels']['pod-template-hash'] or not name.startswith(rs['metadata']['name'] + '-'):
            raise CandidateK3sError('Candidate support Pod template/name drifted.')
        spec = _mapping(pod.get('spec'), f'Pod/{name} spec')
        _pod_spec(spec, template['spec'], f'Pod/{name}', pod=True)
        role = context['placement'][component]
        if spec.get('nodeName') not in ({context['role_nodes'][role]} if complete else {None, '', context['role_nodes'][role]}):
            raise CandidateK3sError('Candidate support Pod is on the wrong exact role node.')
        status = _mapping(pod.get('status', {}), f'Pod/{name} status')
        statuses = status.get('containerStatuses', [])
        _array(statuses, 'Candidate container statuses')
        if complete:
            ready = [item for item in _array(status.get('conditions'), 'Candidate Pod conditions') if _mapping(item, 'Candidate Pod condition').get('type') == 'Ready']
            if status.get('phase') != 'Running' or len(ready) != 1 or ready[0].get('status') != 'True' or len(statuses) != 1:
                raise CandidateK3sError('Candidate support Pod is not exactly Running and Ready.')
        if len(statuses) > 1 or status.get('initContainerStatuses') not in (None, []) or status.get('ephemeralContainerStatuses') not in (None, []):
            raise CandidateK3sError('Candidate support Pod status contains unexpected containers.')
        if statuses:
            container_status = _mapping(statuses[0], 'Candidate container status')
            expected_image = template['spec']['containers'][0]['image']
            image_id = container_status.get('imageID')
            if image_id not in ({expected_image, 'docker-pullable://' + expected_image} if complete else {None, '', expected_image, 'docker-pullable://' + expected_image}):
                raise CandidateK3sError('Candidate support runtime imageID differs from its platform digest.')
            if container_status.get('name') != component:
                raise CandidateK3sError('Candidate support container status name drifted.')
            restarts = _integer(container_status.get('restartCount'), 'Candidate support restart count')
            if complete:
                state = _mapping(container_status.get('state'), 'Candidate running state')
                image = container_status.get('image')
                if container_status.get('ready') is not True or container_status.get('started', True) is not True or set(state) != {'running'}:
                    raise CandidateK3sError('Candidate support container is not Ready and running.')
                _timestamp(_mapping(state['running'], 'Candidate running state').get('startedAt'))
                if not isinstance(image, str) or not image or len(image) > 512 or not re.fullmatch(r'containerd://[0-9a-f]{64}', container_status.get('containerID', '')):
                    raise CandidateK3sError('Candidate support container runtime identity is invalid.')
                _mapping(container_status.get('lastState', {}), 'Candidate support last state')
                support_pods[component] = {
                    'component': component, 'role': role, 'node_name': spec['nodeName'],
                    'pod_name': name, 'pod_uid': metadata['uid'],
                    'replicaset_name': rs['metadata']['name'], 'replicaset_uid': rs['metadata']['uid'],
                    'deployment_uid': deployments[component]['metadata']['uid'],
                    'container_id': container_status['containerID'], 'image': image, 'image_id': image_id,
                    'restart_count': restarts, 'state': dict(state),
                    'last_state': dict(container_status.get('lastState', {})),
                }
            else:
                support_pods[component] = {}
        elif complete:
            raise CandidateK3sError('Candidate support Pod has no running container status.')
        else:
            support_pods[component] = {}
    support_names = {name for name, role in context['placement'].items() if role != 'application'}
    if complete and (set(support_pods) != support_names or not support_pods or not support_names <= set(replicasets)):
        raise CandidateK3sError('Candidate support inventory is incomplete; zero Pods cannot be ready.')
    blocked = []
    for component, role in sorted(context['placement'].items()):
        if role == 'application' and component in deployments:
            blocked.append({
                'component': component, 'deployment_uid': deployments[component]['metadata']['uid'],
                'replicaset_uid': replicasets[component]['metadata']['uid'] if component in replicasets else None,
                'replicas': 0,
            })
    return {
        'namespace_uid': namespace_metadata['uid'], 'nodes': nodes,
        'support_pods': [support_pods[name] for name in sorted(support_pods)],
        'blocked_applications': blocked,
    }


def _checked_inventory(inventory, context, *, complete):
    """Normalize malformed nested API shapes into the fail-closed boundary."""
    try:
        return _validate_inventory(inventory, context, complete=complete)
    except CandidateK3sError:
        raise
    except (TypeError, AttributeError, KeyError, IndexError, StopIteration, UnicodeError) as exc:
        raise CandidateK3sError('Candidate Kubernetes inventory has malformed nested fields.') from exc


def _object_uids(inventory):
    return {
        (item['kind'], item['metadata']['name'], item['metadata'].get('namespace')): item['metadata']['uid']
        for item in inventory['items']
    }


def _require_uid_continuity(previous, current):
    now = _object_uids(current)
    for identity, uid in previous.items():
        if now.get(identity) != uid:
            raise CandidateK3sError('Candidate existing object UID changed or disappeared before/after mutation.')


def _validated_context(plan, stage, expected_role_nodes):
    if not isinstance(plan, CandidateRuntimePlan) or not isinstance(stage, CandidateSupportStageBundle):
        raise CandidateK3sError('An explicit candidate run plan and support stage are required.')
    try:
        stage = validate_candidate_support_stage(plan, stage)
    except ValueError as exc:
        raise CandidateK3sError(f'Candidate support stage cannot be validated: {exc}') from exc
    roles = _mapping(expected_role_nodes, 'Expected candidate role nodes')
    if set(roles) != set(_ROLES):
        raise CandidateK3sError('Candidate role mapping requires exactly four K3s roles.')
    values = {}
    for role, name in roles.items():
        if (
            not isinstance(name, str) or len(name) > 253
            or any(not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', label) for label in name.split('.'))
        ):
            raise CandidateK3sError('Candidate node names must be canonical DNS names.')
        values[role] = name
    if len(set(values.values())) != 4:
        raise CandidateK3sError('Candidate role nodes must be distinct.')
    return {
        'run_id': plan.run_id, 'snapshot_fingerprint': plan.fingerprint,
        'stage_fingerprint': stage.fingerprint, 'namespace': stage.namespace,
        'workload_id': stage.workload_id, 'role_nodes': dict(sorted(values.items())),
        'application_arch': 'arm64' if plan.bundle.application_architecture == 'aarch64' else 'amd64',
        'manifest': _strict_json(stage.canonical_json),
        'placement': dict(plan.bundle.expected_component_placement),
        'stage_manifest_sha256': stage.rendered_manifest_sha256,
        'phase_payloads': {name: payload for name, payload in stage.phases},
    }


def _phase_context(plan, stage, phase, expected_role_nodes):
    context = _validated_context(plan, stage, expected_role_nodes)
    if not isinstance(phase, str) or phase not in context['phase_payloads']:
        raise CandidateK3sError('Candidate stage phase is invalid.')
    context['phase'] = phase
    context['phase_sha256'] = hashlib.sha256(context['phase_payloads'][phase].encode()).hexdigest()
    return context


def _observation(output, context, mode):
    document = _mapping(_strict_json(output), 'Candidate observation')
    expected = {
        'schema_version': 1, 'candidate_schema': OBSERVATION_SCHEMA,
        'candidate_only': True, 'released': False, 'runtime_qualified': False,
        'mode': mode, 'run_id': context['run_id'],
        'snapshot_fingerprint': context['snapshot_fingerprint'],
        'stage_fingerprint': context['stage_fingerprint'], 'namespace': context['namespace'],
        'role_nodes': context['role_nodes'],
        'phase': context.get('phase'),
    }
    if set(document) != set(expected) | {'inventory'}:
        raise CandidateK3sError('Candidate observation envelope schema is invalid.')
    if _canonical({key: document[key] for key in expected}) != _canonical(expected):
        raise CandidateK3sError('Candidate observation crossed its run/stage/node identity.')
    return document['inventory']


def _receipt_identity(context, inventory, schema):
    return {
        'schema_version': 1, 'candidate_schema': schema,
        'candidate_only': True, 'released': False, 'runtime_qualified': False,
        # This describes the observed inventory, not a historical assertion
        # that an external operator never started applications or seeded data.
        'applications_paused': True, 'application_pods_observed': 0, 'dataset_qualified': False,
        'run_id': context['run_id'], 'snapshot_fingerprint': context['snapshot_fingerprint'],
        'stage_fingerprint': context['stage_fingerprint'],
        'stage_manifest_sha256': context['stage_manifest_sha256'],
        'namespace': context['namespace'], 'role_nodes': dict(context['role_nodes']),
        'inventory_sha256': hashlib.sha256(_canonical(inventory).encode()).hexdigest(),
        'inventory': json.loads(_canonical(inventory)),
    }


def _support_receipt(inventory, context):
    normalized = _checked_inventory(inventory, context, complete=True)
    return {
        **_receipt_identity(context, inventory, SUPPORT_SCHEMA),
        'state': 'support_ready_applications_paused', **normalized,
    }


def parse_candidate_support_attestation(output, plan, stage, expected_role_nodes):
    """Prove support Pods only; zero application Pods never mean workload-ready."""
    context = _validated_context(plan, stage, expected_role_nodes)
    return _support_receipt(_observation(output, context, 'support-readiness'), context)


def validate_saved_candidate_support_attestation(value, plan, stage, expected_role_nodes):
    """Restore historical support proof; this is not a fresh execution authority."""
    context = _validated_context(plan, stage, expected_role_nodes)
    value = _mapping(value, 'Saved support attestation')
    rebuilt = _support_receipt(_mapping(value.get('inventory'), 'Saved Kubernetes inventory'), context)
    if _canonical(value) != _canonical(rebuilt):
        raise CandidateK3sError('Saved candidate support attestation fields drifted.')
    return rebuilt


def _reconciliation_receipt(inventory, context):
    normalized = _checked_inventory(inventory, context, complete=False)
    desired = _strict_json(context['phase_payloads'][context['phase']])['items']
    desired_ids = {(item['kind'], item['metadata']['name'], item['metadata'].get('namespace')) for item in desired}
    resources = []
    for item in inventory['items']:
        metadata = item['metadata']
        identity = (item['kind'], metadata['name'], metadata.get('namespace'))
        if identity in desired_ids:
            resources.append({
                'kind': identity[0], 'name': identity[1], 'namespace': identity[2],
                'uid': metadata['uid'],
            })
    resources.sort(key=lambda item: (item['kind'], item['name'], item['namespace'] or ''))
    return {
        **_receipt_identity(context, inventory, RECONCILIATION_SCHEMA),
        'phase': context['phase'], 'phase_sha256': context['phase_sha256'],
        'complete': len(resources) == len(desired_ids),
        'namespace_uid': normalized['namespace_uid'], 'resources': resources,
    }


def parse_candidate_phase_reconciliation(output, plan, stage, phase, expected_role_nodes):
    """Record a valid owned subset after response loss, never infer apply success."""
    context = _phase_context(plan, stage, phase, expected_role_nodes)
    return _reconciliation_receipt(_observation(output, context, 'phase-reconciliation'), context)


def validate_saved_candidate_phase_reconciliation(value, plan, stage, phase, expected_role_nodes):
    context = _phase_context(plan, stage, phase, expected_role_nodes)
    value = _mapping(value, 'Saved candidate phase reconciliation')
    rebuilt = _reconciliation_receipt(_mapping(value.get('inventory'), 'Saved Kubernetes inventory'), context)
    if _canonical(value) != _canonical(rebuilt):
        raise CandidateK3sError('Saved candidate phase reconciliation fields drifted.')
    return rebuilt


_REMOTE_OPERATIONS = r'''
def kubectl(arguments, timeout=60, allow_absent=False, no_json=False):
    completed = subprocess.run([K3S_BINARY, 'kubectl', *arguments], capture_output=True, timeout=timeout)
    if completed.returncode != 0:
        raise CandidateK3sError('Candidate kubectl operation failed; no success is inferred.')
    if no_json:
        return None
    if allow_absent and not completed.stdout.strip():
        return None
    return _strict_json(completed.stdout)

def collect():
    items = []
    namespace = kubectl(['get', 'namespace', CONTEXT['namespace'], '--ignore-not-found', '-o', 'json'], allow_absent=True)
    if namespace is not None:
        items.append(namespace)
        resources = (
            'deployments.apps,services,persistentvolumeclaims,networkpolicies.networking.k8s.io,'
            'replicasets.apps,pods,statefulsets.apps,daemonsets.apps,jobs.batch,cronjobs.batch,'
            'replicationcontrollers,ingresses.networking.k8s.io,horizontalpodautoscalers.autoscaling,'
            'secrets,serviceaccounts,configmaps,roles.rbac.authorization.k8s.io,rolebindings.rbac.authorization.k8s.io'
        )
        namespaced = kubectl(['-n', CONTEXT['namespace'], 'get', resources, '-o', 'json'])
        items.extend(_array(namespaced.get('items'), 'Observed namespace objects'))
    pvs = kubectl(['get', 'persistentvolumes', '-o', 'json'])
    expected_pvs = {item['metadata']['name'] for item in CONTEXT['manifest']['items'] if item['kind'] == 'PersistentVolume'}
    workload_label = next(item['metadata']['labels']['deathstarbench.io/workload'] for item in CONTEXT['manifest']['items'] if item['kind'] == 'Namespace')
    for item in _array(pvs.get('items'), 'Observed PV objects'):
        metadata = _mapping(item.get('metadata'), 'Observed PV metadata')
        claim = _mapping(item.get('spec', {}).get('claimRef', {}), 'Observed PV claim')
        annotations = _mapping(metadata.get('annotations') or {}, 'Observed PV annotations')
        labels = _mapping(metadata.get('labels') or {}, 'Observed PV labels')
        if (metadata.get('name') in expected_pvs
            or str(metadata.get('name', '')).startswith(CONTEXT['namespace'] + '-')
            or claim.get('namespace') == CONTEXT['namespace']
            or annotations.get('deathstarbench.io/candidate-run-id') == CONTEXT['run_id']
            or labels.get('deathstarbench.io/workload') == workload_label):
            items.append(item)
    nodes = kubectl(['get', 'nodes', *sorted(CONTEXT['role_nodes'].values()), '-o', 'json'])
    items.extend(_array(nodes.get('items'), 'Observed K3s nodes'))
    inventory = {'apiVersion': 'v1', 'kind': 'List', 'items': items}
    _checked_inventory(inventory, CONTEXT, complete=False)
    return inventory

def observation(inventory, mode):
    result = {
        'schema_version': 1, 'candidate_schema': OBSERVATION_SCHEMA,
        'candidate_only': True, 'released': False, 'runtime_qualified': False,
        'mode': mode, 'run_id': CONTEXT['run_id'],
        'snapshot_fingerprint': CONTEXT['snapshot_fingerprint'],
        'stage_fingerprint': CONTEXT['stage_fingerprint'], 'namespace': CONTEXT['namespace'],
        'role_nodes': CONTEXT['role_nodes'], 'phase': CONTEXT.get('phase'),
        'inventory': inventory,
    }
    print(_canonical(result))

if OPERATION == 'reconcile':
    observation(collect(), 'phase-reconciliation')
elif OPERATION == 'readiness':
    before = collect()
    if not any(item.get('kind') == 'Namespace' for item in before['items']):
        raise CandidateK3sError('Candidate namespace is missing before support readiness.')
    support = sorted(name for name, role in CONTEXT['placement'].items() if role != 'application')
    kubectl(['-n', CONTEXT['namespace'], 'wait', '--for=condition=Available', *('deployment/' + name for name in support), '--timeout=' + str(TIMEOUT_SECONDS) + 's'], timeout=TIMEOUT_SECONDS + 15, no_json=True)
    selector = 'deathstarbench.io/workload=' + next(item['metadata']['labels']['deathstarbench.io/workload'] for item in CONTEXT['manifest']['items'] if item['kind'] == 'Deployment') + ',deathstarbench.io/role in (control,database,cache)'
    kubectl(['-n', CONTEXT['namespace'], 'wait', '--for=condition=Ready', 'pod', '-l', selector, '--timeout=' + str(TIMEOUT_SECONDS) + 's'], timeout=TIMEOUT_SECONDS + 15, no_json=True)
    final = collect()
    _checked_inventory(final, CONTEXT, complete=True)
    observation(final, 'support-readiness')
elif OPERATION == 'apply':
    payload = sys.stdin.buffer.read(MAX_JSON_BYTES + 1)
    if len(payload) > MAX_JSON_BYTES or hashlib.sha256(payload).hexdigest() != CONTEXT['phase_sha256']:
        raise CandidateK3sError('Candidate phase stdin does not match its exact SHA-256.')
    if _canonical(_strict_json(payload)) != _canonical(_strict_json(CONTEXT['phase_payloads'][CONTEXT['phase']])):
        raise CandidateK3sError('Candidate phase stdin differs from the validated support stage.')
    if os.geteuid() != 0:
        raise CandidateK3sError('Candidate K3s apply requires root ownership.')
    directory = '/run/deathstarbench-candidate'
    try:
        os.mkdir(directory, 0o700)
    except FileExistsError:
        pass
    details = os.lstat(directory)
    if not stat.S_ISDIR(details.st_mode) or stat.S_IMODE(details.st_mode) != 0o700 or details.st_uid != 0 or details.st_gid != 0 or os.path.realpath(directory) != directory:
        raise CandidateK3sError('Candidate namespace lock directory is unsafe.')
    path = directory + '/' + CONTEXT['namespace'] + '.lock'
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode) or details.st_nlink != 1 or details.st_uid != 0 or details.st_gid != 0 or stat.S_IMODE(details.st_mode) != 0o600:
            raise CandidateK3sError('Candidate namespace lock file is unsafe.')
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        before = collect()
        existing_uids = _object_uids(before)
        namespace = next((item for item in before['items'] if item['kind'] == 'Namespace'), None)
        namespace_uid = namespace['metadata']['uid'] if namespace is not None else None
        phase = CONTEXT['phase']
        if namespace is None and phase != 'namespace':
            raise CandidateK3sError('Candidate namespace must exist before other phases.')
        if namespace is None:
            completed = subprocess.run([K3S_BINARY, 'kubectl', 'create', '--field-manager=deathstarbench-candidate', '-f', '-'], input=payload, capture_output=True, timeout=120)
            if completed.returncode != 0:
                # Conflict is not evidence of our ownership. A fresh collector
                # must prove every binding before treating creation as observed.
                after = collect()
                _require_uid_continuity(existing_uids, after)
                if not any(item['kind'] == 'Namespace' for item in after['items']):
                    raise CandidateK3sError('Candidate namespace creation did not complete.')
            else:
                _require_uid_continuity(existing_uids, collect())
        else:
            fresh = collect()
            _require_uid_continuity(existing_uids, fresh)
            fresh_namespace = next(item for item in fresh['items'] if item['kind'] == 'Namespace')
            if fresh_namespace['metadata']['uid'] != namespace_uid:
                raise CandidateK3sError('Candidate namespace UID changed before mutation.')
            dry = subprocess.run([K3S_BINARY, 'kubectl', 'apply', '--server-side', '--dry-run=server', '--validate=strict', '--field-manager=deathstarbench-candidate', '-f', '-'], input=payload, capture_output=True, timeout=120)
            if dry.returncode != 0:
                raise CandidateK3sError('Candidate server dry-run rejected the guarded phase.')
            fresh = collect()
            _require_uid_continuity(existing_uids, fresh)
            fresh_namespace = next(item for item in fresh['items'] if item['kind'] == 'Namespace')
            if fresh_namespace['metadata']['uid'] != namespace_uid:
                raise CandidateK3sError('Candidate namespace UID changed after dry-run.')
            applied = subprocess.run([K3S_BINARY, 'kubectl', 'apply', '--server-side', '--validate=strict', '--field-manager=deathstarbench-candidate', '-f', '-'], input=payload, capture_output=True, timeout=120)
            if applied.returncode != 0:
                raise CandidateK3sError('Candidate phase apply did not complete; reconcile before retry.')
        after = collect()
        _require_uid_continuity(existing_uids, after)
        observation(after, 'phase-reconciliation')
    finally:
        os.close(descriptor)
else:
    raise CandidateK3sError('Unknown candidate K3s operation.')
'''


def _remote_command(context, operation, timeout_seconds=120):
    # Embed only stdlib validation helpers. Compressed immutable desired state
    # avoids Linux's single-argument size limit while still hashing stdin bytes.
    helpers = '\n\n'.join(inspect.getsource(function) for function in (
        CandidateK3sError, _canonical, _strict_json, _mapping, _array, _uid,
        _integer, _timestamp, _subset, _metadata, _owner, _default_fields, _pod_spec, _status_counts, _nodes,
        _system_object, _validate_inventory, _checked_inventory, _object_uids, _require_uid_continuity,
    ))
    encoded = base64.b64encode(zlib.compress(_canonical(context).encode(), 9)).decode()
    script = (
        'from __future__ import annotations\n'
        'import base64, fcntl, hashlib, ipaddress, json, os, re, stat, subprocess, sys, zlib\n'
        'from collections.abc import Mapping\n'
        'from datetime import datetime\n'
        f'MAX_JSON_BYTES={MAX_JSON_BYTES}\n'
        f'OBSERVATION_SCHEMA={OBSERVATION_SCHEMA!r}\n'
        f'_OWNERSHIP_KEYS={_OWNERSHIP_KEYS!r}\n'
        f'K3S_BINARY={K3S_BINARY!r}\n'
        f'OPERATION={operation!r}\nTIMEOUT_SECONDS={timeout_seconds}\n'
        + helpers + '\n'
        + f"compressed=base64.b64decode({encoded!r}, validate=True)\n"
        + 'decoder=zlib.decompressobj()\n'
        + 'raw=decoder.decompress(compressed, MAX_JSON_BYTES + 1)\n'
        + "if len(raw)>MAX_JSON_BYTES or not decoder.eof or decoder.unused_data: raise CandidateK3sError('Candidate command context is invalid.')\n"
        + 'CONTEXT=_strict_json(raw)\n'
        + _REMOTE_OPERATIONS
    )
    return 'sudo python3 -c ' + shlex.quote(script)


def candidate_phase_apply_command(plan, stage, phase, expected_role_nodes):
    """Apply one exact stdin phase under guarded ownership, never prune/delete."""
    return _remote_command(_phase_context(plan, stage, phase, expected_role_nodes), 'apply')


def candidate_phase_reconciliation_command(plan, stage, phase, expected_role_nodes):
    """Read-only reconciliation: no waits, filesystem writes, apply, or scale."""
    return _remote_command(_phase_context(plan, stage, phase, expected_role_nodes), 'reconcile')


def candidate_support_readiness_command(plan, stage, expected_role_nodes, timeout_seconds=120):
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 600:
        raise CandidateK3sError('Candidate support readiness timeout must be 1-600 seconds.')
    return _remote_command(_validated_context(plan, stage, expected_role_nodes), 'readiness', timeout_seconds)
