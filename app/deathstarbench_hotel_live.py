"""Fresh, fail-closed Hotel initialization inventory and one-shot guest commands.

These helpers provide bounded observations and exact API mutations. The leased
controller owns authorization, journal latches, deadlines, and cleanup.
"""
from __future__ import annotations

import base64
import hashlib
import inspect
import ipaddress
import json
import re
import shlex
import zlib
from collections.abc import Mapping
from datetime import datetime

from . import deathstarbench_candidate_k3s as support
from .deathstarbench_hotel_initialization_stage import (
    MODES, SCHEDULING_GATE, validate_guarded_hotel_initialization_stage,
)
from .deathstarbench_hotel_runtime import (
    GuardedHotelDatabaseObservation, observe_guarded_hotel_cluster_identity,
)

HotelInitializationLiveError = support.CandidateK3sError
CandidateK3sError = support.CandidateK3sError
MAX_JSON_BYTES = support.MAX_JSON_BYTES
for _name in ('_canonical', '_strict_json', '_mapping', '_array', '_uid', '_integer',
              '_timestamp', '_subset', '_default_fields', '_checked_inventory'):
    globals()[_name] = getattr(support, _name)


def _fail(message):
    raise CandidateK3sError('Hotel initialization: ' + message)


def _exact(actual, expected, label):
    if _canonical(actual) != _canonical(expected):
        _fail(label + ' differs from the exact approved value.')


def _hotel_metadata(actual, expected, label):
    actual = _mapping(actual, label + ' metadata')
    _uid(actual.get('uid'), label)
    for key in ('name', 'namespace', 'labels', 'annotations'):
        _exact(actual.get(key), expected.get(key), label + ' ' + key)
    if actual.get('ownerReferences') not in (None, []) or actual.get('deletionTimestamp') is not None or actual.get('finalizers') not in (None, []):
        _fail(label + ' has foreign ownership, deletion or finalizers.')
    _default_fields(actual, expected, {}, label + ' metadata', dynamic=(
        'uid', 'resourceVersion', 'creationTimestamp', 'generation', 'managedFields',
    ))
    if not isinstance(actual.get('resourceVersion'), str) or not re.fullmatch(r'[1-9][0-9]*', actual['resourceVersion']):
        _fail(label + ' has no exact resourceVersion.')


def _hotel_pod(pod, approved, context):
    label = 'Pod/' + approved['metadata']['name']
    if set(pod) - {'kind', 'apiVersion', 'metadata', 'spec', 'status'}:
        _fail(label + ' has unapproved object fields.')
    _exact(pod.get('apiVersion'), 'v1', label + ' apiVersion')
    _hotel_metadata(pod.get('metadata'), approved['metadata'], label)
    actual = _mapping(pod.get('spec'), label + ' spec')
    expected = json.loads(_canonical(approved['spec']))
    gates = actual.get('schedulingGates', [])
    if gates not in ([], [{'name': context['scheduling_gate']}]):
        _fail(label + ' scheduling gates drifted.')
    blocked = bool(gates)
    if blocked:
        if actual.get('nodeName') not in (None, ''):
            _fail(label + ' was scheduled while gated.')
    else:
        expected.pop('schedulingGates')
        if actual.get('nodeName') not in (None, '', context['role_nodes']['application']):
            _fail(label + ' has the wrong application node.')
    # Compare the complete submitted fields recursively, then reject all extra
    # fields except these narrow Kubernetes admission/API defaults.
    compared = dict(actual)
    for key in ('hostNetwork', 'hostPID', 'hostIPC'):
        # These three bools are omitempty in the PodSpec API. Pointer bools,
        # including shareProcessNamespace/automountServiceAccountToken, remain
        # explicit and mandatory in the admitted spec.
        compared.setdefault(key, False)
    _subset(compared, expected, label + ' spec')
    _default_fields(actual, expected, {
        'serviceAccountName': ('default',), 'serviceAccount': ('default',),
        'priority': (0,), 'preemptionPolicy': ('PreemptLowerPriority',),
        'schedulingGates': ([],),
    }, label + ' spec', dynamic=('containers', 'tolerations', 'nodeName'))
    tolerations = actual.get('tolerations', [])
    allowed = [{'key': 'node.kubernetes.io/' + key, 'operator': 'Exists', 'effect': 'NoExecute', 'tolerationSeconds': 300}
               for key in ('not-ready', 'unreachable')]
    if not isinstance(tolerations, list) or len(tolerations) > 2 or len({_canonical(x) for x in tolerations}) != len(tolerations) or any(x not in allowed for x in tolerations):
        _fail(label + ' has unapproved tolerations.')
    for key in ('securityContext', 'nodeSelector'):
        _exact(actual.get(key), expected[key], label + ' ' + key)
    containers = _array(actual.get('containers'), label + ' containers')
    if len(containers) != 1:
        _fail(label + ' must contain one exact container.')
    container, locked = containers[0], expected['containers'][0]
    _default_fields(container, locked, {'terminationMessagePath': ('/dev/termination-log',),
                    'terminationMessagePolicy': ('File',)}, label + ' container')
    for key, value in locked.items():
        _exact(container.get(key), value, label + ' container ' + key)
    status = _mapping(pod.get('status', {}), label + ' status')
    phase = status.get('phase')
    if phase not in ('Pending', 'Running', 'Succeeded', 'Failed'):
        _fail(label + ' has an unknown phase.')
    if status.get('initContainerStatuses') not in (None, []) or status.get('ephemeralContainerStatuses') not in (None, []):
        _fail(label + ' has foreign container execution.')
    statuses = _array(status.get('containerStatuses', []), label + ' container status')
    if blocked and (phase != 'Pending' or statuses or status.get('podIP') or status.get('podIPs') or status.get('hostIP') or status.get('hostIPs') or status.get('startTime')):
        _fail(label + ' executed or acquired a node/IP while gated.')
    for condition in _array(status.get('conditions', []), label + ' conditions'):
        if blocked and _mapping(condition, label + ' condition').get('status') == 'True' and condition.get('type') in ('PodScheduled', 'Ready', 'ContainersReady', 'PodReadyToStartContainers'):
            _fail(label + ' claims scheduling/execution while gated.')
    if len(statuses) > 1 or (phase != 'Pending' and not statuses):
        _fail(label + ' has inconsistent execution status.')
    image_id, exit_code, restart_count, container_id, started_at = None, None, 0, None, None
    if statuses:
        current = _mapping(statuses[0], label + ' container status')
        if current.get('name') != 'hotel-initialize' or current.get('restartCount') != 0 or type(current.get('restartCount')) is not int or current.get('lastState', {}) != {}:
            _fail(label + ' restarted or contains unexpected container history.')
        state = _mapping(current.get('state'), label + ' container state')
        if len(state) != 1 or next(iter(state)) not in ('waiting', 'running', 'terminated'):
            _fail(label + ' has inconsistent container state.')
        image_id = current.get('imageID')
        if current.get('image') not in (context['application_image'], context['application_config_digest']):
            _fail(label + ' status image differs from the platform image.')
        if 'waiting' in state:
            if phase != 'Pending' or image_id not in (None, '', context['application_image'], 'docker-pullable://' + context['application_image']) or current.get('containerID') not in (None, ''):
                _fail(label + ' waiting state conflicts with phase or execution identity.')
        else:
            if actual.get('nodeName') != context['role_nodes']['application'] or image_id not in (context['application_image'], 'docker-pullable://' + context['application_image']) or not re.fullmatch(r'containerd://[0-9a-f]{64}', current.get('containerID', '')):
                _fail(label + ' runtime identity or application placement drifted.')
            container_id = current['containerID']
            if 'running' in state:
                _timestamp(_mapping(state['running'], label + ' running').get('startedAt'))
                started_at = state['running']['startedAt']
                if phase != 'Running':
                    _fail(label + ' running container conflicts with phase.')
            else:
                terminated = _mapping(state['terminated'], label + ' terminated')
                exit_code = _integer(terminated.get('exitCode'), label + ' exit code')
                _timestamp(terminated.get('startedAt'))
                started_at = terminated['startedAt']
                _timestamp(terminated.get('finishedAt'))
                start = datetime.fromisoformat(terminated['startedAt'].replace('Z', '+00:00'))
                finish = datetime.fromisoformat(terminated['finishedAt'].replace('Z', '+00:00'))
                if finish < start or current.get('ready') is not False or current.get('started', False) is not False:
                    _fail(label + ' terminated state is inconsistent.')
                if terminated.get('containerID', container_id) != container_id or (exit_code == 0 and terminated.get('signal', 0) != 0):
                    _fail(label + ' terminated identity or signal conflicts with its status.')
                if phase not in ('Running', 'Succeeded', 'Failed') or (phase == 'Succeeded' and exit_code != 0) or (phase == 'Failed' and exit_code == 0):
                    _fail(label + ' terminal phase conflicts with exit code.')
        if phase in ('Succeeded', 'Failed') and 'terminated' not in state:
            _fail(label + ' terminal phase has no terminated container.')
    return {'uid': pod['metadata']['uid'], 'spec': json.loads(_canonical(actual)),
            'metadata': json.loads(_canonical(pod['metadata'])), 'phase': phase,
            'exit_code': exit_code, 'image_id': image_id, 'restart_count': restart_count,
            'container_id': container_id, 'started_at': started_at}


def _endpoint_graph(slices, items, context):
    services = {p['metadata']['name']: p for p in items if p['kind'] == 'Service'}
    pods = {p['metadata']['labels']['app.kubernetes.io/component']: p for p in items if p['kind'] == 'Pod'}
    graph, seen_services = [], set()
    for item in slices:
        metadata = _mapping(item.get('metadata'), 'EndpointSlice metadata')
        name = metadata.get('name')
        _uid(metadata.get('uid'), 'EndpointSlice')
        if item.get('apiVersion') != 'discovery.k8s.io/v1' or metadata.get('namespace') != context['namespace'] or metadata.get('deletionTimestamp') is not None:
            _fail('EndpointSlice scope/API/deletion drifted.')
        labels = _mapping(metadata.get('labels'), 'EndpointSlice labels')
        if metadata.get('finalizers') not in (None, []):
            _fail('EndpointSlice has foreign finalizers.')
        service_name = labels.get('kubernetes.io/service-name')
        if service_name not in services or service_name in seen_services or not isinstance(name, str) or not name.startswith(service_name + '-'):
            _fail('EndpointSlice has foreign, duplicate or invalid service ownership.')
        seen_services.add(service_name)
        service = services[service_name]
        authority_labels = {'kubernetes.io/service-name': service_name, 'endpointslice.kubernetes.io/managed-by': 'endpointslice-controller.k8s.io'}
        if labels not in (authority_labels, {**service['metadata']['labels'], **authority_labels}):
            _fail('EndpointSlice labels broaden endpoint authority.')
        _exact(metadata.get('ownerReferences'), [{'apiVersion': 'v1', 'kind': 'Service', 'name': service_name,
                'uid': service['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}], 'EndpointSlice Service owner')
        if item.get('addressType') != 'IPv4' or set(item) - {'apiVersion', 'kind', 'metadata', 'addressType', 'ports', 'endpoints'}:
            _fail('EndpointSlice has unapproved address type or fields.')
        endpoints = _array([] if item.get('endpoints') is None else item['endpoints'], 'EndpointSlice endpoints')
        expected_pod = pods.get(service_name)
        if len(endpoints) != (1 if expected_pod is not None else 0):
            _fail('EndpointSlice endpoint count differs from the exact support Pod.')
        ports = _array([] if item.get('ports') is None else item['ports'], 'EndpointSlice ports')
        expected_ports = [{'name': p.get('name', ''), 'protocol': p.get('protocol', 'TCP'), 'port': p.get('targetPort', p['port'])}
                          for p in service['spec']['ports']]
        if endpoints or ports:
            compared_ports = json.loads(_canonical(ports))
            for actual_port in compared_ports:
                # Empty names are an API default, and confer no additional
                # route authority. All numeric ports/protocols remain exact.
                if actual_port.get('name', '') == '':
                    matching = [p for p in expected_ports if p['port'] == actual_port.get('port') and p['protocol'] == actual_port.get('protocol')]
                    if len(matching) == 1:
                        actual_port['name'] = matching[0]['name']
            _exact(sorted(compared_ports, key=_canonical), sorted(expected_ports, key=_canonical), 'EndpointSlice ports')
            ports = sorted(compared_ports, key=_canonical)
        if endpoints:
            endpoint = _mapping(endpoints[0], 'EndpointSlice endpoint')
            ip = expected_pod.get('status', {}).get('podIP')
            try:
                if not isinstance(ip, str) or str(ipaddress.IPv4Address(ip)) != ip:
                    _fail('Support Pod has no canonical actual IPv4 address.')
            except ipaddress.AddressValueError as exc:
                raise CandidateK3sError('Support Pod IP is invalid.') from exc
            _exact(endpoint.get('addresses'), [ip], 'EndpointSlice addresses')
            ref = _mapping(endpoint.get('targetRef'), 'EndpointSlice Pod reference')
            expected_ref = {'kind': 'Pod', 'namespace': context['namespace'], 'name': expected_pod['metadata']['name'], 'uid': expected_pod['metadata']['uid']}
            if ref.get('apiVersion', 'v1') != 'v1' or {k: v for k, v in ref.items() if k != 'apiVersion'} != expected_ref:
                _fail('EndpointSlice target is not the exact database/support Pod UID.')
            conditions = _mapping(endpoint.get('conditions'), 'EndpointSlice conditions')
            if conditions.get('ready') is not True or conditions.get('serving', True) is not True or conditions.get('terminating', False) is not False or set(conditions) - {'ready', 'serving', 'terminating'}:
                _fail('EndpointSlice is not exactly ready and nonterminating.')
            if set(endpoint) - {'addresses', 'conditions', 'targetRef', 'nodeName', 'zone'} or endpoint.get('nodeName', expected_pod['spec']['nodeName']) != expected_pod['spec']['nodeName']:
                _fail('EndpointSlice contains foreign routing authority.')
        graph.append({'service_name': service_name, 'service_uid': service['metadata']['uid'], 'slice_name': name,
                      'slice_uid': metadata['uid'], 'ports': ports, 'endpoints': endpoints})
    required = {name for name in services if name.startswith('mongodb-')}
    if not required <= seen_services:
        _fail('MongoDB EndpointSlice inventory is incomplete.')
    return sorted(graph, key=lambda row: row['service_name'])


def _release_preconditions(checked, mode, context):
    pods = checked['initialization_pods']
    target = pods[mode]
    if target['spec'].get('schedulingGates') != [{'name': context['scheduling_gate']}] or target['phase'] != 'Pending':
        _fail('Target is not exactly gated/Pending; replay is forbidden.')
    if mode == 'initialize-empty':
        verifier = pods['verify']
        if verifier['spec'].get('schedulingGates') != [{'name': context['scheduling_gate']}] or verifier['phase'] != 'Pending':
            _fail('Verifier must remain gated/Pending before initialization dispatch.')
    elif mode == 'verify':
        initializer = pods['initialize-empty']
        if initializer['phase'] != 'Succeeded' or initializer['exit_code'] != 0:
            _fail('Initializer must have succeeded before verifier dispatch.')
    else:
        _fail('Unknown initialization mode.')


def _validate_hotel_document(document, context):
    expected_fields = {'schema_version', 'plan_fingerprint', 'stage_fingerprint', 'role_nodes', 'include_initialization',
                       'inventory', 'server_ca_sha256', 'kube_system_namespace_uid'}
    if not isinstance(document, dict) or set(document) != expected_fields:
        _fail('Observation fields differ.')
    for key in ('schema_version', 'plan_fingerprint', 'stage_fingerprint', 'role_nodes', 'include_initialization'):
        _exact(document[key], context[key], 'observation ' + key)
    if not isinstance(document['server_ca_sha256'], str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', document['server_ca_sha256']) or document['server_ca_sha256'] == 'sha256:' + '0' * 64:
        _fail('Server CA digest is invalid.')
    _uid(document['kube_system_namespace_uid'], 'kube-system Namespace')
    inventory = _mapping(document['inventory'], 'Hotel inventory')
    if inventory.get('kind') != 'List' or inventory.get('apiVersion') != 'v1' or set(inventory) - {'kind', 'apiVersion', 'metadata', 'items'}:
        _fail('Inventory is not an exact Kubernetes List.')
    items = _array(inventory.get('items'), 'Hotel objects')
    if len(items) > 1024:
        _fail('Inventory is oversized.')
    desired = {(x['kind'], x['metadata']['name']): x for x in context['initialization_manifest']['items']}
    base, init_objects, slices, identities, all_uids = [], {}, [], set(), set()
    for item in items:
        item = _mapping(item, 'Hotel object')
        metadata = _mapping(item.get('metadata'), 'Hotel object metadata')
        key = (item.get('kind'), metadata.get('name'))
        identity = (key[0], key[1], metadata.get('namespace'))
        _uid(metadata.get('uid'), 'Hotel object')
        if identity in identities or metadata['uid'] in all_uids:
            _fail('Inventory repeats an object identity or UID.')
        identities.add(identity)
        all_uids.add(metadata['uid'])
        if key in desired:
            if not context['include_initialization']:
                _fail('Initialization objects already exist before one-shot creation.')
            init_objects[key] = item
        elif key[0] == 'EndpointSlice':
            slices.append(item)
        else:
            base.append(item)
    if document['kube_system_namespace_uid'] in all_uids:
        _fail('kube-system Namespace UID is reused.')
    normalized = _checked_inventory({'kind': 'List', 'apiVersion': 'v1', 'items': base}, context['support_context'], complete=True)
    for pod in normalized['support_pods']:
        if pod['restart_count'] != 0 or pod['last_state'] != {}:
            _fail('Support container restarted or has prior execution history.')
        locked_image = pod['image_id'].removeprefix('docker-pullable://')
        if pod['image'] not in (locked_image, context['support_config_digests'][locked_image]):
            _fail('Support status image differs from its exact image digest.')
    graph = _endpoint_graph(slices, base, context)
    observations = {}
    if context['include_initialization']:
        if set(init_objects) != set(desired):
            _fail('Initialization policy/Pod inventory is incomplete.')
        for key, approved in desired.items():
            item = init_objects[key]
            if key[0] == 'Pod':
                mode = approved['spec']['containers'][0]['args'][0]
                observations[mode] = _hotel_pod(item, approved, context)
            else:
                _exact(item.get('apiVersion'), approved['apiVersion'], 'Initialization policy apiVersion')
                _hotel_metadata(item.get('metadata'), approved['metadata'], 'Initialization policy')
                actual_spec = dict(_mapping(item.get('spec'), 'Initialization policy spec'))
                expected_spec = dict(approved['spec'])
                for omitted in ('ingress', 'egress'):
                    if expected_spec.get(omitted) == [] and omitted not in actual_spec:
                        actual_spec[omitted] = []
                _exact(actual_spec, expected_spec, 'Initialization NetworkPolicy')
    objects = sorted([{'kind': x['kind'], 'namespace': x['metadata'].get('namespace'), 'name': x['metadata']['name'], 'uid': x['metadata']['uid']}
                      for x in [*base, *slices]], key=lambda row: (row['kind'], row['namespace'] or '', row['name']))
    containers = [{key: pod[key] for key in ('component', 'pod_uid', 'container_id', 'restart_count', 'image_id')}
                  for pod in normalized['support_pods']]
    return {'initialization_pods': observations, 'support_normalized': normalized,
            'base_inventory': {'kind': 'List', 'apiVersion': 'v1', 'items': base},
            'continuity': {'server_ca_sha256': document['server_ca_sha256'],
                'kube_system_namespace_uid': document['kube_system_namespace_uid'],
                'objects': objects, 'support_containers': containers, 'endpoint_graph': graph,
                'initialization_pods': {mode: pod['uid'] for mode, pod in observations.items()},
                'initialization_objects': {key[1]: item['metadata']['uid'] for key, item in init_objects.items()}}}


def _context(plan, stage, role_nodes, include=True):
    validate_guarded_hotel_initialization_stage(plan, stage)
    if type(include) is not bool:
        _fail('include_initialization must be a Boolean.')
    support_context = support._validated_context(plan.support_plan, plan.support_stage, role_nodes)
    return {'schema_version': 1, 'plan_fingerprint': plan.fingerprint, 'stage_fingerprint': stage.fingerprint,
            'role_nodes': support_context['role_nodes'], 'include_initialization': include,
            'namespace': plan.namespace, 'application_image': plan.application_image,
            'application_config_digest': plan.snapshot()['application_config_digest'],
            'support_config_digests': {platform['image']: platform['config_digest']
                for binding in plan.support_plan.artifacts.image_bindings.values()
                for platform in binding['platforms'].values()},
            'scheduling_gate': SCHEDULING_GATE, 'initialization_manifest': json.loads(stage.canonical_json),
            'support_context': support_context}


def parse_hotel_initialization_observation(output, plan, stage, role_nodes, *, include_initialization=True):
    context = _context(plan, stage, role_nodes, include_initialization)
    document = _strict_json(output)
    try:
        checked = _validate_hotel_document(document, context)
    except (KeyError, TypeError, AttributeError, IndexError, RecursionError) as exc:
        raise CandidateK3sError('Hotel initialization observation has malformed fields.') from exc
    proof = support._support_receipt(checked['base_inventory'], context['support_context'])
    observation, verifier_uid = None, None
    if include_initialization:
        base = checked['base_inventory']['items']
        by_key = {(x['kind'], x['metadata']['name']): x for x in base}
        rows = []
        for component in plan.storage.database_names:
            pod = next(x for x in base if x['kind'] == 'Pod' and x['metadata']['labels']['app.kubernetes.io/component'] == component)
            pvc = by_key['PersistentVolumeClaim', component]
            pv = by_key['PersistentVolume', pvc['spec']['volumeName']]
            rows.append(GuardedHotelDatabaseObservation(component=component, pvc_name=component, pvc_uid=pvc['metadata']['uid'],
                pv_name=pv['metadata']['name'], pv_uid=pv['metadata']['uid'], pod_name=pod['metadata']['name'], pod_uid=pod['metadata']['uid']))
        node = by_key['Node', role_nodes['database']]
        observation = observe_guarded_hotel_cluster_identity(plan, server_ca_sha256=document['server_ca_sha256'],
            kube_system_namespace_uid=document['kube_system_namespace_uid'], namespace_uid=checked['support_normalized']['namespace_uid'],
            database_node_name=role_nodes['database'], database_node_uid=node['metadata']['uid'], database_filesystem_uuid=plan.storage.filesystem_uuid,
            initializer_pod_name=plan.initializer_pod_name, initializer_pod_uid=checked['initialization_pods']['initialize-empty']['uid'], databases=tuple(rows)).metadata()
        verifier_uid = checked['initialization_pods']['verify']['uid']
    return {'cluster_observation': observation, 'verifier_pod_uid': verifier_uid,
            'initialization_pods': checked['initialization_pods'], 'continuity': checked['continuity'], 'support_attestation': proof}


# Freeze helper source at import, never inspect the checkout during dispatch.
_REMOTE_HELPERS = support._REMOTE_HELPERS + '\n\n' + '\n\n'.join(inspect.getsource(function) for function in (
    _fail, _exact, _hotel_metadata, _hotel_pod, _endpoint_graph, _release_preconditions, _validate_hotel_document,
))

_REMOTE_OPERATIONS = r'''
def kubectl(arguments):
    completed = subprocess.run([K3S_BINARY, 'kubectl', '--request-timeout=20s', *arguments], capture_output=True, timeout=30)
    if completed.returncode != 0:
        _fail('kubectl operation failed; no success is inferred.')
    return _strict_json(completed.stdout)

def collect():
    items = [kubectl(['get', 'namespace', CONTEXT['namespace'], '-o', 'json'])]
    resources = ('deployments.apps,services,persistentvolumeclaims,networkpolicies.networking.k8s.io,'
        'replicasets.apps,pods,statefulsets.apps,daemonsets.apps,jobs.batch,cronjobs.batch,'
        'replicationcontrollers,ingresses.networking.k8s.io,horizontalpodautoscalers.autoscaling,'
        'secrets,serviceaccounts,configmaps,roles.rbac.authorization.k8s.io,rolebindings.rbac.authorization.k8s.io,'
        'endpointslices.discovery.k8s.io')
    items.extend(_array(kubectl(['-n', CONTEXT['namespace'], 'get', resources, '-o', 'json']).get('items'), 'Namespaced objects'))
    expected_pvs = {x['metadata']['name'] for x in CONTEXT['support_context']['manifest']['items'] if x['kind'] == 'PersistentVolume'}
    for item in _array(kubectl(['get', 'persistentvolumes', '-o', 'json']).get('items'), 'PersistentVolumes'):
        metadata = _mapping(item.get('metadata'), 'PV metadata')
        claim = _mapping(item.get('spec', {}).get('claimRef', {}), 'PV claim')
        labels, annotations = metadata.get('labels') or {}, metadata.get('annotations') or {}
        if (metadata.get('name') in expected_pvs or str(metadata.get('name', '')).startswith(CONTEXT['namespace'] + '-')
            or claim.get('namespace') == CONTEXT['namespace'] or annotations.get('deathstarbench.io/candidate-run-id') == CONTEXT['support_context']['run_id']
            or labels.get('deathstarbench.io/workload') == 'hotel-reservation-v1'):
            items.append(item)
    items.extend(_array(kubectl(['get', 'nodes', *sorted(CONTEXT['role_nodes'].values()), '-o', 'json']).get('items'), 'Nodes'))
    kube_system = kubectl(['get', 'namespace', 'kube-system', '-o', 'json'])
    if kube_system.get('kind') != 'Namespace' or kube_system.get('apiVersion') != 'v1' or kube_system.get('metadata', {}).get('name') != 'kube-system' or kube_system.get('metadata', {}).get('deletionTimestamp') is not None:
        _fail('kube-system Namespace identity is invalid.')
    with open('/var/lib/rancher/k3s/server/tls/server-ca.crt', 'rb') as stream:
        certificate = stream.read(65537)
    if not certificate or len(certificate) > 65536 or not certificate.startswith(b'-----BEGIN CERTIFICATE-----'):
        _fail('K3s server CA file is invalid or oversized.')
    document = {key: CONTEXT[key] for key in ('schema_version', 'plan_fingerprint', 'stage_fingerprint', 'role_nodes', 'include_initialization')}
    document.update(inventory={'apiVersion': 'v1', 'kind': 'List', 'items': items},
                    server_ca_sha256='sha256:' + hashlib.sha256(certificate).hexdigest(),
                    kube_system_namespace_uid=kube_system['metadata']['uid'])
    return document

if OPERATION == 'observe':
    document = collect()
    _validate_hotel_document(document, CONTEXT)
    print(_canonical(document))
elif OPERATION == 'create':
    before = collect()
    before_checked = _validate_hotel_document(before, CONTEXT)
    _exact(before_checked['continuity'], EXPECTED_CONTINUITY, 'Fresh pre-create continuity')
    payload = _canonical(CONTEXT['initialization_manifest']).encode()
    completed = subprocess.run([K3S_BINARY, 'kubectl', '--request-timeout=20s', '-n', CONTEXT['namespace'],
        'create', '--validate=strict', '--field-manager=deathstarbench-hotel-initialization', '-f', '-'], input=payload, capture_output=True, timeout=60)
    if completed.returncode != 0:
        _fail('One-shot creation failed; never retry creation or infer ownership from conflicts.')
    CONTEXT['include_initialization'] = True
    after = collect()
    after_checked = _validate_hotel_document(after, CONTEXT)
    _exact({key: value for key, value in before_checked['continuity'].items() if key not in ('initialization_pods', 'initialization_objects')},
           {key: value for key, value in after_checked['continuity'].items() if key not in ('initialization_pods', 'initialization_objects')}, 'Creation support continuity')
    print(_canonical(after))
elif OPERATION == 'release':
    before = collect()
    checked = _validate_hotel_document(before, CONTEXT)
    _exact(checked['continuity'], EXPECTED_CONTINUITY, 'Fresh pre-release continuity')
    for mode, pins in EXPECTED_EXECUTION.items():
        for key, value in pins.items():
            _exact(checked['initialization_pods'][mode][key], value, 'Fresh execution identity ' + mode + ' ' + key)
    _release_preconditions(checked, MODE, CONTEXT)
    pod = checked['initialization_pods'][MODE]
    if pod['spec'].get('schedulingGates') != [{'name': CONTEXT['scheduling_gate']}]:
        _fail('Pod gate is absent; no replay is permitted.')
    metadata = pod['metadata']
    patch = [{'op': 'test', 'path': '/metadata/' + key, 'value': metadata[key]}
             for key in ('uid', 'resourceVersion', 'name', 'namespace', 'labels', 'annotations')]
    patch.extend([{'op': 'test', 'path': '/spec', 'value': pod['spec']}, {'op': 'remove', 'path': '/spec/schedulingGates'}])
    kubectl(['-n', CONTEXT['namespace'], 'patch', 'pod', metadata['name'], '--type=json', '-p', _canonical(patch), '-o', 'json'])
    after = collect()
    after_checked = _validate_hotel_document(after, CONTEXT)
    _exact(after_checked['continuity'], EXPECTED_CONTINUITY, 'Post-release continuity')
    print(_canonical(after))
elif OPERATION == 'logs':
    pod = kubectl(['-n', NAMESPACE, 'get', 'pod', POD_NAME, '-o', 'json'])
    if pod.get('kind') != 'Pod' or pod.get('metadata', {}).get('uid') != POD_UID or pod['metadata'].get('name') != POD_NAME or pod['metadata'].get('namespace') != NAMESPACE or pod['metadata'].get('deletionTimestamp') is not None:
        _fail('Log Pod UID/scope differs from the exact observed Pod.')
    completed = subprocess.run([K3S_BINARY, 'kubectl', '--request-timeout=20s', '-n', NAMESPACE, 'logs', POD_NAME,
        '-c', 'hotel-initialize', '--limit-bytes=4097', '--tail=-1'], capture_output=True, timeout=30)
    if completed.returncode != 0 or len(completed.stdout) > 4096:
        _fail('Pod logs failed or exceeded their bound.')
    # A second read detects replacement across the separate logs request.
    after = kubectl(['-n', NAMESPACE, 'get', 'pod', POD_NAME, '-o', 'json'])
    if after.get('metadata', {}).get('uid') != POD_UID or after['metadata'].get('deletionTimestamp') is not None:
        _fail('Log Pod was replaced or deleted during collection.')
    sys.stdout.buffer.write(completed.stdout)
else:
    _fail('Unknown remote operation.')
'''


def _command(context, operation, *, continuity=None, mode=None, log_identity=None, execution=None):
    payload = {'context': context, 'continuity': continuity, 'execution': execution}
    encoded = base64.b64encode(zlib.compress(_canonical(payload).encode(), 9)).decode()
    script = ('from __future__ import annotations\n'
        'import base64, hashlib, ipaddress, json, re, subprocess, sys, zlib\n'
        'from collections.abc import Mapping\nfrom datetime import datetime\n'
        f'MAX_JSON_BYTES={MAX_JSON_BYTES}\n_OWNERSHIP_KEYS={support._OWNERSHIP_KEYS!r}\n'
        f'K3S_BINARY={support.K3S_BINARY!r}\nOPERATION={operation!r}\n'
        + _REMOTE_HELPERS + '\n'
        + f'COMMAND=_strict_json(zlib.decompress(base64.b64decode({encoded!r}, validate=True)))\n'
        + "CONTEXT=COMMAND['context']\nEXPECTED_CONTINUITY=COMMAND['continuity']\nEXPECTED_EXECUTION=COMMAND['execution']\n"
        + f'MODE={mode!r}\n'
        + (f'NAMESPACE={log_identity[0]!r}\nPOD_NAME={log_identity[1]!r}\nPOD_UID={log_identity[2]!r}\n' if log_identity else '')
        + _REMOTE_OPERATIONS)
    # Python 3.9 supports removeprefix; no strict zip or host-only syntax.
    return 'sudo python3 -c ' + shlex.quote(script)


def hotel_initialization_observe_command(plan, stage, role_nodes, *, include_initialization=True):
    return _command(_context(plan, stage, role_nodes, include_initialization), 'observe')


def hotel_initialization_create_command(plan, stage, role_nodes, observation):
    context = _context(plan, stage, role_nodes, False)
    if not isinstance(observation, dict) or observation.get('initialization_pods') != {} or not isinstance(observation.get('continuity'), dict):
        _fail('Creation requires a freshly validated pre-create observation.')
    continuity = _strict_json(_canonical(observation['continuity']))
    if continuity.get('initialization_pods') != {} or continuity.get('initialization_objects') != {}:
        _fail('Creation observation contains prior initialization objects.')
    return _command(context, 'create', continuity=continuity)


def hotel_initialization_release_command(plan, stage, role_nodes, observation, mode):
    context = _context(plan, stage, role_nodes)
    if mode not in MODES or not isinstance(observation, dict) or set(observation) != {'cluster_observation', 'verifier_pod_uid', 'initialization_pods', 'continuity', 'support_attestation'}:
        _fail('Release requires the exact full live observation and mode.')
    # Parent must freshly audit and persist its latch before dispatching. Guest
    # independently recollects and validates every support/endpoint/Pod binding.
    continuity = _strict_json(_canonical(observation['continuity']))
    if observation['initialization_pods'].get(mode, {}).get('spec', {}).get('schedulingGates') != [{'name': SCHEDULING_GATE}]:
        _fail('Release gate is absent; replay is forbidden.')
    execution = {observed_mode: {key: row[key] for key in ('container_id', 'started_at') if row.get(key)}
                 for observed_mode, row in observation['initialization_pods'].items()}
    return _command(context, 'release', continuity=continuity, mode=mode, execution=execution)


def hotel_initialization_logs_command(plan, stage, mode, pod_uid):
    validate_guarded_hotel_initialization_stage(plan, stage)
    if mode not in MODES:
        _fail('Logs require an exact initialization mode.')
    _uid(pod_uid, 'Log Pod')
    return _command({}, 'logs', log_identity=(plan.namespace, stage.pod(mode)['metadata']['name'], pod_uid))
