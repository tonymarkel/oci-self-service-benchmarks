"""Private, one-shot Hotel initialization commands; never application startup.

The old support validator remains unchanged. This boundary removes only the
eleven exact initialization objects and EndpointSlices, validates the entire
remaining paused support graph with that validator, and then proves every
MongoDB Service resolves to its exact ready, UID-bound database Pod. Retained
receipts describe observations, not dispatch authority or retry permission.
"""

from __future__ import annotations

import base64
from datetime import datetime
import hashlib
import inspect
import ipaddress
import json
import re
import shlex
import zlib

from . import deathstarbench_candidate_k3s as support
from .deathstarbench_hotel_initialization_stage import (
    COMPONENTS, MODES, SCHEDULING_GATE, validate_guarded_hotel_initialization_stage,
)
from .deathstarbench_hotel_runtime import validate_guarded_hotel_runtime_plan
from .k3s_runtime import K3S_BINARY


SCHEMA = 'hotel-guarded-live-k3s-observation-v1'
MAX_JSON_BYTES = support.MAX_JSON_BYTES
ADMISSION_GROUP = 'admissionregistration.k8s.io'
MUTATOR_KINDS = {
    'mutatingwebhookconfigurations': 'MutatingWebhookConfiguration',
    'mutatingadmissionpolicies': 'MutatingAdmissionPolicy',
    'mutatingadmissionpolicybindings': 'MutatingAdmissionPolicyBinding',
}
CandidateK3sError = support.CandidateK3sError
_canonical = support._canonical
_strict_json = support._strict_json
_mapping = support._mapping
_array = support._array
_uid = support._uid
_integer = support._integer
_timestamp = support._timestamp
_subset = support._subset
_default_fields = support._default_fields
_checked_inventory = support._checked_inventory


def _exact(left, right, label):
    if _canonical(left) != _canonical(right):
        raise CandidateK3sError(label + ' drifted.')


def _admission_versions(group):
    group = _mapping(group, 'Hotel admission API group')
    if group.get('apiVersion') != 'v1' or group.get('kind') != 'APIGroup' or group.get('name') != ADMISSION_GROUP:
        raise CandidateK3sError('Hotel admission API group discovery is invalid.')
    versions = []
    for item in _array(group.get('versions'), 'Hotel admission served API versions'):
        item = _mapping(item, 'Hotel admission API version')
        version = item.get('version')
        if (set(item) != {'groupVersion', 'version'} or not isinstance(version, str)
                or not re.fullmatch(r'v[1-9][0-9]{0,2}(?:(?:alpha|beta)[1-9][0-9]{0,2})?', version)
                or item.get('groupVersion') != ADMISSION_GROUP + '/' + version or version in versions):
            raise CandidateK3sError('Hotel admission served API version is invalid.')
        versions.append(version)
    if not 1 <= len(versions) <= 8 or 'v1' not in versions or group.get('preferredVersion') not in group['versions']:
        raise CandidateK3sError('Hotel admission API versions are incomplete.')
    return versions


def _admission_mutator_absence(value):
    """Require successful discovery and empty lists, never a swallowed 403/404.

    Policies are feature-gated across Kubernetes versions. Their absence is
    accepted only when every version advertised by this live API group has
    been discovered and no version serves that collection. List metadata is
    retained in observations but excluded from stable identity: its global
    etcd resourceVersion changes when unrelated Pod status is updated.
    """
    value = _mapping(value, 'Hotel admission mutator evidence')
    if set(value) != {'api_group', 'discovery', 'inventories'}:
        raise CandidateK3sError('Hotel admission mutator evidence schema drifted.')
    versions = _admission_versions(value['api_group'])
    discovery = _mapping(value['discovery'], 'Hotel admission API discovery')
    if set(discovery) != {ADMISSION_GROUP + '/' + version for version in versions}:
        raise CandidateK3sError('Hotel admission API discovery omitted a served version.')
    expected_paths = {}
    served_resources = set()
    for group_version, document in discovery.items():
        document = _mapping(document, 'Hotel admission API resource list')
        if document.get('apiVersion') != 'v1' or document.get('kind') != 'APIResourceList' or document.get('groupVersion') != group_version:
            raise CandidateK3sError('Hotel admission API resource discovery is invalid.')
        found = set()
        subresources = set()
        for resource in _array(document.get('resources'), 'Hotel admission API resources'):
            resource = _mapping(resource, 'Hotel admission API resource')
            name = resource.get('name')
            if not isinstance(name, str) or not re.fullmatch(r'[a-z]+(?:/[a-z]+)?', name) or name in found:
                raise CandidateK3sError('Hotel admission API resource inventory is invalid.')
            found.add(name)
            base = name.split('/')[0]
            if base.startswith('mutating') or str(resource.get('kind', '')).startswith('Mutating'):
                if base not in MUTATOR_KINDS or resource.get('kind') != MUTATOR_KINDS[base] or resource.get('namespaced') is not False:
                    raise CandidateK3sError('Hotel API serves an unapproved admission mutator kind.')
                if '/' in name:
                    subresources.add(base)
                else:
                    if 'list' not in _array(resource.get('verbs'), 'Hotel mutator API verbs'):
                        raise CandidateK3sError('Hotel mutator API cannot be independently listed.')
                    path = group_version + '/' + name
                    expected_paths[path] = MUTATOR_KINDS[base] + 'List'
                    served_resources.add(base)
        if not subresources <= found:
            raise CandidateK3sError('Hotel admission mutator subresource lacks its collection.')
    if ADMISSION_GROUP + '/v1/mutatingwebhookconfigurations' not in expected_paths:
        raise CandidateK3sError('Hotel stable mutating webhook API is missing from discovery.')
    inventories = _mapping(value['inventories'], 'Hotel admission mutator inventories')
    if set(inventories) != set(expected_paths):
        raise CandidateK3sError('Hotel admission mutator collection was not successfully inspected.')
    for path, kind in expected_paths.items():
        document = _mapping(inventories[path], 'Hotel admission mutator list')
        if document.get('kind') != kind or document.get('apiVersion') != path.rsplit('/', 1)[0]:
            raise CandidateK3sError('Hotel admission mutator list API identity drifted.')
        metadata = _mapping(document.get('metadata'), 'Hotel admission mutator list metadata')
        if (not isinstance(metadata.get('resourceVersion'), str) or not metadata['resourceVersion']
                or len(metadata['resourceVersion']) > 128 or metadata.get('continue') not in (None, '')
                or metadata.get('remainingItemCount') is not None and (
                    type(metadata['remainingItemCount']) is not int or metadata['remainingItemCount'] != 0)):
            raise CandidateK3sError('Hotel admission mutator list is incomplete or paginated.')
        if _array(document.get('items'), 'Hotel admission mutators') != []:
            raise CandidateK3sError('Hotel initialization forbids all dynamic admission mutators.')
    return {'api_group': ADMISSION_GROUP, 'served_versions': sorted(versions),
            'inspected_collections': sorted(expected_paths), 'mutator_count': 0,
            'unserved_resources': sorted(set(MUTATOR_KINDS) - served_resources)}


def _initialization_pod(pod, approved, context):
    label = 'Hotel initialization Pod/' + approved['metadata']['name']
    if pod.get('apiVersion') != 'v1' or pod.get('kind') != 'Pod':
        raise CandidateK3sError(label + ' API identity drifted.')
    metadata = _mapping(pod.get('metadata'), label + ' metadata')
    _uid(metadata.get('uid'), label)
    resource_version = metadata.get('resourceVersion')
    if not isinstance(resource_version, str) or not re.fullmatch(r'[1-9][0-9]{0,31}', resource_version):
        raise CandidateK3sError(label + ' resourceVersion is invalid.')
    for key in ('name', 'namespace', 'labels', 'annotations'):
        _exact(metadata.get(key), approved['metadata'][key], label + ' ' + key)
    if metadata.get('deletionTimestamp') is not None or metadata.get('ownerReferences') not in (None, []):
        raise CandidateK3sError(label + ' is deleting or has an unexpected owner.')
    if metadata.get('finalizers') not in (None, []):
        raise CandidateK3sError(label + ' has unapproved finalizers.')
    if 'generateName' in metadata:
        raise CandidateK3sError(label + ' must have an exact standalone name, never generateName.')
    actual = dict(_mapping(pod.get('spec'), label + ' spec'))
    desired = dict(approved['spec'])
    # PodSpec's false host booleans are Go omitempty defaults. Normalize only
    # this validation copy; the atomic JSON Patch still tests the untouched
    # complete admitted spec, including the exact presence/absence of fields.
    for field in ('hostNetwork', 'hostPID', 'hostIPC', 'shareProcessNamespace'):
        if field not in actual and desired.get(field) is False:
            actual[field] = False
    gates = actual.pop('schedulingGates', [])
    desired.pop('schedulingGates')
    if gates not in ([], [{'name': SCHEDULING_GATE}]):
        raise CandidateK3sError(label + ' scheduling gates drifted.')
    gated = gates != []
    node = actual.pop('nodeName', None)
    if (gated and node not in (None, '')) or (not gated and node not in (None, '', context['role_nodes']['application'])):
        raise CandidateK3sError(label + ' exact application node binding drifted.')
    tolerations = actual.pop('tolerations', [])
    allowed = [
        {'key': 'node.kubernetes.io/' + key, 'operator': 'Exists',
         'effect': 'NoExecute', 'tolerationSeconds': 300}
        for key in ('not-ready', 'unreachable')
    ]
    if len(tolerations) > 2 or len({_canonical(item) for item in tolerations}) != len(tolerations):
        raise CandidateK3sError(label + ' toleration inventory drifted.')
    if any(not any(_canonical(item) == _canonical(default) for default in allowed) for item in tolerations):
        raise CandidateK3sError(label + ' has unapproved tolerations.')
    _subset(actual, desired, label + ' spec')
    _default_fields(actual, desired, {
        'serviceAccountName': ('default',), 'serviceAccount': ('default',),
        'preemptionPolicy': ('PreemptLowerPriority',), 'priority': (0,),
        'imagePullSecrets': (None, []), 'initContainers': (None, []),
        'ephemeralContainers': (None, []), 'volumes': (None, []),
        'affinity': (None, {}), 'hostAliases': (None, []), 'dnsConfig': (None, {}),
        'resourceClaims': (None, []), 'topologySpreadConstraints': (None, []),
    }, label + ' spec')
    container = _mapping(actual['containers'][0], label + ' container')
    _default_fields(container, desired['containers'][0], {
        'terminationMessagePath': ('/dev/termination-log',), 'terminationMessagePolicy': ('File',),
        'stdin': (False,), 'stdinOnce': (False,), 'tty': (False,),
    }, label + ' container')
    # _subset validates submitted fields, but nested unexpected fields would
    # otherwise allow admission-time environment/security/resource changes.
    for key in ('securityContext', 'resources', 'env'):
        _exact(container[key], desired['containers'][0][key], label + ' container ' + key)
    _exact(actual['securityContext'], desired['securityContext'], label + ' security context')
    _exact(actual['nodeSelector'], desired['nodeSelector'], label + ' node selector')
    status = _mapping(pod.get('status', {}), label + ' status')
    if status.get('initContainerStatuses') not in (None, []) or status.get('ephemeralContainerStatuses') not in (None, []):
        raise CandidateK3sError(label + ' has unexpected container statuses.')
    statuses = _array(status.get('containerStatuses', []), label + ' container statuses')
    if len(statuses) > 1:
        raise CandidateK3sError(label + ' has extra containers.')
    result = {'name': metadata['name'], 'uid': metadata['uid'], 'state': 'gated' if gated else 'pending',
              'spec': json.loads(_canonical(pod['spec'])), 'pod': json.loads(_canonical(pod)),
              'node_name': node, 'exit_code': None, 'container_id': None, 'image_id': None}
    if gated:
        if statuses or status.get('phase') not in (None, 'Pending') or status.get('podIP') not in (None, ''):
            raise CandidateK3sError(label + ' gated Pod has already executed.')
        return result
    if not statuses:
        if status.get('phase') not in (None, 'Pending'):
            raise CandidateK3sError(label + ' has no matching container state.')
        return result
    observed = _mapping(statuses[0], label + ' container status')
    if observed.get('name') != 'hotel-initialize' or type(observed.get('restartCount')) is not int or observed['restartCount'] != 0:
        raise CandidateK3sError(label + ' container name or restart count drifted.')
    if observed.get('lastState') != {}:
        raise CandidateK3sError(label + ' has a prior container execution.')
    state = _mapping(observed.get('state'), label + ' container state')
    if len(state) != 1 or not set(state) <= {'waiting', 'running', 'terminated'}:
        raise CandidateK3sError(label + ' container execution state is ambiguous.')
    if 'waiting' in state:
        if status.get('phase') != 'Pending' or observed.get('containerID') not in (None, ''):
            raise CandidateK3sError(label + ' waiting container has an execution identity.')
        return result
    image = context['application_image']
    if node != context['role_nodes']['application'] or observed.get('imageID') not in (image, 'docker-pullable://' + image):
        raise CandidateK3sError(label + ' execution node or platform imageID drifted.')
    container_id = observed.get('containerID', '')
    if not isinstance(container_id, str) or not re.fullmatch(r'containerd://[0-9a-f]{64}', container_id):
        raise CandidateK3sError(label + ' container runtime identity is invalid.')
    result.update(container_id=container_id, image_id=observed['imageID'])
    if 'running' in state:
        if status.get('phase') != 'Running':
            raise CandidateK3sError(label + ' running phase drifted.')
        _timestamp(_mapping(state['running'], label + ' running state').get('startedAt'))
        result['state'] = 'running'
    else:
        ended = _mapping(state['terminated'], label + ' terminated state')
        _timestamp(ended.get('startedAt'))
        _timestamp(ended.get('finishedAt'))
        if datetime.fromisoformat(ended['finishedAt'].replace('Z', '+00:00')) < datetime.fromisoformat(ended['startedAt'].replace('Z', '+00:00')):
            raise CandidateK3sError(label + ' termination precedes its execution.')
        exit_code = _integer(ended.get('exitCode'), label + ' exit code')
        if _integer(ended.get('signal', 0), label + ' termination signal') != 0 or (exit_code == 0 and ended.get('reason') != 'Completed'):
            raise CandidateK3sError(label + ' termination reason or signal is not successful one-shot completion.')
        if ended.get('containerID', container_id) != container_id:
            raise CandidateK3sError(label + ' terminated container ID drifted.')
        phase = status.get('phase')
        # Kubelet mergePodStatus can retain a nonterminal phase while publishing
        # terminated container status. This is pending evidence, never success.
        if phase in ('Pending', 'Running'):
            return result
        if phase != ('Succeeded' if exit_code == 0 else 'Failed'):
            safe_phase = (phase if isinstance(phase, str) and phase in ('Succeeded', 'Failed', 'Unknown')
                          else '<missing>' if phase is None else '<invalid>')
            raise CandidateK3sError(label + ' terminal phase and exit status disagree '
                                   + '(phase=' + safe_phase + ', exit_code=' + str(exit_code) + ').')
        result.update(state='terminated', exit_code=exit_code)
    return result


def _endpoint_ports(value, service_name):
    """Compare complete port maps without depending on controller list order."""
    label = 'Hotel EndpointSlice ports for ' + service_name
    ports = _array(value, label)
    if not ports:
        raise CandidateK3sError(label + ' must be nonempty.')
    names = set()
    for port in ports:
        port = _mapping(port, label)
        if (set(port) != {'name', 'port', 'protocol'} or not isinstance(port['name'], str)
                or not isinstance(port['protocol'], str) or type(port['port']) is not int
                or not 1 <= port['port'] <= 65535 or port['name'] in names):
            raise CandidateK3sError(label + ' drifted.')
        names.add(port['name'])
    return sorted(ports, key=lambda port: port['name'])


def _endpoint_graph(slices, inventory, normalized, context):
    services = {item['metadata']['name']: item for item in inventory['items'] if item['kind'] == 'Service'}
    pods = {item['metadata']['name']: item for item in inventory['items'] if item['kind'] == 'Pod'}
    ready = {item['component']: item for item in normalized['support_pods']}
    observed = {}
    for item in slices:
        if item.get('apiVersion') != 'discovery.k8s.io/v1' or item.get('addressType') != 'IPv4':
            raise CandidateK3sError('Hotel EndpointSlice API/address family drifted.')
        metadata = _mapping(item.get('metadata'), 'Hotel EndpointSlice metadata')
        _uid(metadata.get('uid'), 'Hotel EndpointSlice')
        labels = _mapping(metadata.get('labels'), 'Hotel EndpointSlice labels')
        name = labels.get('kubernetes.io/service-name')
        if name not in services or name in observed or metadata.get('namespace') != context['namespace'] or metadata.get('deletionTimestamp') is not None:
            raise CandidateK3sError('Hotel EndpointSlice service inventory drifted.')
        if labels.get('endpointslice.kubernetes.io/managed-by') != 'endpointslice-controller.k8s.io':
            raise CandidateK3sError('Hotel EndpointSlice manager drifted.')
        service = services[name]
        owners = [{'apiVersion': 'v1', 'kind': 'Service', 'name': name, 'uid': service['metadata']['uid'],
                   'controller': True, 'blockOwnerDeletion': True}]
        _exact(metadata.get('ownerReferences'), owners, 'Hotel EndpointSlice Service owner')
        if name not in ready:
            # The pinned controller creates an empty placeholder for a Service
            # with no Pods. Protobuf storage can round-trip [] to JSON null.
            for field in ('ports', 'endpoints'):
                value = item.get(field)
                if value is not None and (not isinstance(value, list) or value):
                    raise CandidateK3sError('Paused Hotel application ' + name + ' has nonempty or invalid EndpointSlice ' + field + '.')
            observed[name] = {'service_uid': service['metadata']['uid'], 'endpoint_slice_uid': metadata['uid'], 'pod_ip': None}
            continue
        expected_ports = [{'name': port.get('name', ''), 'port': port['targetPort'], 'protocol': port.get('protocol', 'TCP')}
                          for port in service['spec']['ports']]
        _exact(_endpoint_ports(item.get('ports'), name), _endpoint_ports(expected_ports, name),
               'Hotel EndpointSlice ports for ' + name)
        endpoints = _array(item.get('endpoints'), 'Hotel EndpointSlice endpoints for ' + name)
        support_pod = ready[name]
        pod = pods[support_pod['pod_name']]
        pod_ip = pod.get('status', {}).get('podIP')
        try:
            address = ipaddress.ip_address(pod_ip)
        except ValueError as exc:
            raise CandidateK3sError('Hotel support Pod IP is missing or invalid.') from exc
        if address not in ipaddress.ip_network('10.42.0.0/16') or str(address) != pod_ip:
            raise CandidateK3sError('Hotel support Pod IP is outside the pinned K3s network.')
        if pod.get('status', {}).get('podIPs', [{'ip': pod_ip}]) != [{'ip': pod_ip}]:
            raise CandidateK3sError('Hotel support Pod IP inventory drifted.')
        if len(endpoints) != 1:
            raise CandidateK3sError('Hotel support Service must resolve to exactly one Pod.')
        endpoint = _mapping(endpoints[0], 'Hotel EndpointSlice endpoint')
        if endpoint.get('addresses') != [pod_ip] or endpoint.get('nodeName') != support_pod['node_name']:
            raise CandidateK3sError('Hotel EndpointSlice address/node differs from its support Pod.')
        conditions = _mapping(endpoint.get('conditions'), 'Hotel endpoint conditions')
        if conditions.get('ready') is not True or conditions.get('serving', True) is not True or conditions.get('terminating', False) is not False:
            raise CandidateK3sError('Hotel support endpoint is not exclusively ready and serving.')
        target = _mapping(endpoint.get('targetRef'), 'Hotel endpoint target')
        if any(target.get(key) != value for key, value in {
            'kind': 'Pod', 'namespace': context['namespace'], 'name': support_pod['pod_name'], 'uid': support_pod['pod_uid'],
        }.items()) or target.get('apiVersion', 'v1') != 'v1':
            raise CandidateK3sError('Hotel EndpointSlice target Pod UID binding drifted.')
        observed[name] = {'service_uid': service['metadata']['uid'], 'endpoint_slice_uid': metadata['uid'], 'pod_ip': pod_ip}
    if not set(ready) <= set(observed):
        raise CandidateK3sError('Hotel ready support Service is missing its EndpointSlice.')
    ips = [observed[name]['pod_ip'] for name in ready]
    if len(set(ips)) != len(ips):
        raise CandidateK3sError('Hotel support Pods reuse an IP address.')
    return observed


def _live_inventory(inventory, context):
    document = _mapping(inventory, 'Hotel live inventory')
    if set(document) - {'apiVersion', 'kind', 'metadata', 'items'} or document.get('apiVersion') != 'v1' or document.get('kind') != 'List':
        raise CandidateK3sError('Hotel inventory must be an exact Kubernetes List.')
    expected = {(item['kind'], item['metadata']['name']): item for item in context['initialization_manifest']['items']}
    additions, slices, remainder, identities = {}, [], [], set()
    for item in _array(document.get('items'), 'Hotel live objects'):
        metadata = _mapping(item.get('metadata'), 'Hotel live object metadata')
        key = (item.get('kind'), metadata.get('name'))
        if key in identities:
            raise CandidateK3sError('Hotel live inventory repeats an object.')
        identities.add(key)
        if key in expected:
            additions[key] = item
        elif item.get('kind') == 'EndpointSlice':
            slices.append(item)
        else:
            remainder.append(item)
    if additions and set(additions) != set(expected):
        raise CandidateK3sError('Hotel initialization creation is partial; cleanup only.')
    filtered = {'apiVersion': 'v1', 'kind': 'List', 'items': remainder}
    normalized = _checked_inventory(filtered, context, complete=True)
    if any(item['restart_count'] != 0 or item['last_state'] for item in normalized['support_pods']):
        raise CandidateK3sError('Hotel support container restarted; initialization is unsafe.')
    graph = _endpoint_graph(slices, filtered, normalized, context)
    pods = {}
    for key, item in additions.items():
        approved = expected[key]
        if key[0] == 'Pod':
            mode = approved['spec']['containers'][0]['args'][0]
            pods[mode] = _initialization_pod(item, approved, context)
        else:
            if item.get('apiVersion') != approved['apiVersion']:
                raise CandidateK3sError('Hotel initialization policy API drifted.')
            for field in ('name', 'namespace', 'labels', 'annotations'):
                _exact(item['metadata'].get(field), approved['metadata'][field], 'Hotel initialization policy ' + field)
            _uid(item['metadata'].get('uid'), 'Hotel initialization policy')
            if item['metadata'].get('deletionTimestamp') is not None or item['metadata'].get('ownerReferences') not in (None, []):
                raise CandidateK3sError('Hotel initialization policy ownership drifted.')
            spec = dict(_mapping(item.get('spec'), 'Hotel initialization policy spec'))
            for field in ('ingress', 'egress'):
                if approved['spec'].get(field) == [] and field not in spec:
                    spec[field] = []
            _exact(spec, approved['spec'], 'Hotel initialization policy graph')
    by_kind = {(item['kind'], item['metadata']['name']): item for item in remainder}
    databases = []
    for component in COMPONENTS:
        name = 'mongodb-' + component
        support_pod = next(item for item in normalized['support_pods'] if item['component'] == name)
        pvc = by_kind['PersistentVolumeClaim', name]
        pv = by_kind['PersistentVolume', context['namespace'] + '-' + name]
        databases.append({'component': name, 'pvc_name': name, 'pvc_uid': pvc['metadata']['uid'],
                          'pv_name': pv['metadata']['name'], 'pv_uid': pv['metadata']['uid'],
                          'pod_name': support_pod['pod_name'], 'pod_uid': support_pod['pod_uid'], **graph[name]})
    object_uids = sorted([{'kind': item['kind'], 'name': item['metadata']['name'],
                          'namespace': item['metadata'].get('namespace'), 'uid': item['metadata']['uid']}
                         for item in document['items']], key=lambda item: (item['kind'], item['name']))
    uid_values = [item['uid'] for item in object_uids]
    if len(set(uid_values)) != len(uid_values):
        raise CandidateK3sError('Hotel Kubernetes objects reuse a UID.')
    identity = {'objects': object_uids, 'support_containers': normalized['support_pods'], 'endpoints': graph}
    return {'inventory': json.loads(_canonical(inventory)), 'support_inventory': filtered,
            'namespace_uid': normalized['namespace_uid'], 'nodes': normalized['nodes'],
            'databases': databases, 'pods': pods, 'identity': identity}


def _continuity(previous, current):
    if previous is None:
        return
    previous = _mapping(previous, 'Expected Hotel live identity')
    if set(previous) != set(current):
        raise CandidateK3sError('Hotel identity schema drifted.')
    for key in ('server_ca_sha256', 'kube_system_namespace_uid', 'namespace_uid', 'support_containers', 'endpoints', 'admission_mutators'):
        _exact(previous[key], current[key], 'Hotel live ' + key)
    old = {(item['kind'], item['name'], item['namespace']): item['uid'] for item in previous['objects']}
    now = {(item['kind'], item['name'], item['namespace']): item['uid'] for item in current['objects']}
    if any(now.get(key) != uid for key, uid in old.items()):
        raise CandidateK3sError('Hotel object UID disappeared or was replaced.')


def _context(plan, stage, role_nodes, expected_ca_sha256):
    plan = validate_guarded_hotel_runtime_plan(plan)
    stage = validate_guarded_hotel_initialization_stage(plan, stage)
    if not isinstance(expected_ca_sha256, str) or not re.fullmatch(r'(?:sha256:)?[0-9a-f]{64}', expected_ca_sha256):
        raise CandidateK3sError('Hotel expected server CA digest is invalid.')
    ca = expected_ca_sha256.removeprefix('sha256:')
    if ca == '0' * 64:
        raise CandidateK3sError('Hotel expected server CA digest must be nonzero.')
    return {**support._validated_context(plan.support_plan, plan.support_stage, role_nodes),
            'hotel_plan_fingerprint': plan.fingerprint, 'hotel_stage_fingerprint': stage.fingerprint,
            'initialization_manifest': _strict_json(stage.canonical_json),
            'application_image': plan.application_image, 'server_ca_sha256': 'sha256:' + ca}


def _parse_document(document, context, expected_identity=None):
    expected = {'schema_version': 1, 'candidate_schema': SCHEMA, 'candidate_only': True,
                'released': False, 'application_start_allowed': False,
                'run_id': context['run_id'], 'plan_fingerprint': context['hotel_plan_fingerprint'],
                'stage_fingerprint': context['hotel_stage_fingerprint'],
                'server_ca_sha256': context['server_ca_sha256'], 'role_nodes': context['role_nodes']}
    if set(document) != set(expected) | {'inventory', 'kube_system_namespace_uid', 'logs', 'admission_mutators'}:
        raise CandidateK3sError('Hotel live observation envelope schema drifted.')
    _exact({key: document[key] for key in expected}, expected, 'Hotel live observation identity')
    _uid(document['kube_system_namespace_uid'], 'Hotel kube-system namespace')
    result = _live_inventory(document['inventory'], context)
    result.update(server_ca_sha256=document['server_ca_sha256'],
                  kube_system_namespace_uid=document['kube_system_namespace_uid'])
    result['identity'].update(server_ca_sha256=result['server_ca_sha256'],
                              kube_system_namespace_uid=result['kube_system_namespace_uid'], namespace_uid=result['namespace_uid'])
    result['identity']['admission_mutators'] = _admission_mutator_absence(document['admission_mutators'])
    result['admission_mutators'] = json.loads(_canonical(document['admission_mutators']))
    if result['kube_system_namespace_uid'] in {item['uid'] for item in result['identity']['objects']}:
        raise CandidateK3sError('Hotel kube-system UID aliases another object.')
    logs = _mapping(document['logs'], 'Hotel initializer logs')
    if set(logs) - set(MODES):
        raise CandidateK3sError('Hotel initializer logs contain an unknown mode.')
    for mode, output in logs.items():
        if result['pods'].get(mode, {}).get('state') != 'terminated' or not isinstance(output, str) or len(output.encode()) > 4096:
            raise CandidateK3sError('Hotel terminal logs are not bounded to an exact terminated Pod.')
    result['logs'] = dict(logs)
    _continuity(expected_identity, result['identity'])
    return result


def parse_guarded_hotel_live_observation(output, plan, stage, role_nodes, expected_ca_sha256, *, expected_identity=None):
    """Validate fresh output; returned dictionaries never authorize replay."""
    try:
        return _parse_document(_mapping(_strict_json(output), 'Hotel live observation'),
                               _context(plan, stage, role_nodes, expected_ca_sha256), expected_identity)
    except CandidateK3sError:
        raise
    except (TypeError, AttributeError, KeyError, IndexError, StopIteration, UnicodeError) as exc:
        raise CandidateK3sError('Hotel live observation has malformed nested fields.') from exc


_REMOTE_OPERATIONS = r'''
def kubectl(arguments, timeout=60):
    result = subprocess.run([K3S_BINARY, 'kubectl', *arguments], capture_output=True, timeout=timeout)
    if result.returncode != 0:
        raise CandidateK3sError('Hotel kubectl operation failed; no retry or success is inferred.')
    return _strict_json(result.stdout)

def admission_mutators():
    group = kubectl(['get', '--raw', '/apis/' + ADMISSION_GROUP])
    evidence = {'api_group': group, 'discovery': {}, 'inventories': {}}
    for version in _admission_versions(group):
        group_version = ADMISSION_GROUP + '/' + version
        resources = kubectl(['get', '--raw', '/apis/' + group_version])
        evidence['discovery'][group_version] = resources
        for resource in _array(resources.get('resources'), 'Hotel admission API resources'):
            name = resource.get('name', '')
            if isinstance(name, str) and name.split('/')[0] in MUTATOR_KINDS and '/' not in name:
                path = group_version + '/' + name
                evidence['inventories'][path] = kubectl(['get', '--raw', '/apis/' + path])
    _admission_mutator_absence(evidence)
    return evidence

def collect():
    ca = 'sha256:' + hashlib.sha256(open('/var/lib/rancher/k3s/server/tls/server-ca.crt', 'rb').read(65537)).hexdigest()
    if ca != CONTEXT['server_ca_sha256']:
        raise CandidateK3sError('Hotel live server CA changed.')
    admission = admission_mutators()
    system = kubectl(['get', 'namespace', 'kube-system', '-o', 'json'])
    _uid(system.get('metadata', {}).get('uid'), 'Hotel kube-system namespace')
    namespace = kubectl(['get', 'namespace', CONTEXT['namespace'], '-o', 'json'])
    resources = ('deployments.apps,services,persistentvolumeclaims,networkpolicies.networking.k8s.io,'
        'replicasets.apps,pods,statefulsets.apps,daemonsets.apps,jobs.batch,cronjobs.batch,'
        'replicationcontrollers,ingresses.networking.k8s.io,horizontalpodautoscalers.autoscaling,'
        'secrets,serviceaccounts,configmaps,roles.rbac.authorization.k8s.io,rolebindings.rbac.authorization.k8s.io,'
        'endpointslices.discovery.k8s.io')
    items = [namespace, *_array(kubectl(['-n', CONTEXT['namespace'], 'get', resources, '-o', 'json']).get('items'), 'Hotel namespaced objects')]
    pvs = kubectl(['get', 'persistentvolumes', '-o', 'json'])
    expected_pvs = {item['metadata']['name'] for item in CONTEXT['manifest']['items'] if item['kind'] == 'PersistentVolume'}
    for item in _array(pvs.get('items'), 'Hotel PV objects'):
        metadata = _mapping(item.get('metadata'), 'Hotel PV metadata')
        claim = _mapping(item.get('spec', {}).get('claimRef', {}), 'Hotel PV claim')
        if (metadata.get('name') in expected_pvs or str(metadata.get('name', '')).startswith(CONTEXT['namespace'] + '-')
            or claim.get('namespace') == CONTEXT['namespace']
            or metadata.get('annotations', {}).get('deathstarbench.io/candidate-run-id') == CONTEXT['run_id']
            or metadata.get('labels', {}).get('deathstarbench.io/workload') == 'hotel-reservation-v1'):
            items.append(item)
    items.extend(_array(kubectl(['get', 'nodes', *sorted(CONTEXT['role_nodes'].values()), '-o', 'json']).get('items'), 'Hotel nodes'))
    document = {'schema_version': 1, 'candidate_schema': SCHEMA, 'candidate_only': True,
        'released': False, 'application_start_allowed': False, 'run_id': CONTEXT['run_id'],
        'plan_fingerprint': CONTEXT['hotel_plan_fingerprint'], 'stage_fingerprint': CONTEXT['hotel_stage_fingerprint'],
        'server_ca_sha256': ca, 'role_nodes': CONTEXT['role_nodes'],
        'kube_system_namespace_uid': system['metadata']['uid'],
        'inventory': {'apiVersion': 'v1', 'kind': 'List', 'items': items}, 'logs': {}, 'admission_mutators': admission}
    checked = _parse_document(document, CONTEXT)
    for mode, pod in checked['pods'].items():
        if pod['state'] == 'terminated':
            # Recheck the UID immediately before logs; named replacement is
            # rejected again in the full post-log observation below.
            current = kubectl(['-n', CONTEXT['namespace'], 'get', 'pod', pod['name'], '-o', 'json'])
            if current['metadata']['uid'] != pod['uid']:
                raise CandidateK3sError('Hotel terminal Pod replaced before logs.')
            result = subprocess.run([K3S_BINARY, 'kubectl', '-n', CONTEXT['namespace'], 'logs', pod['name'], '-c', 'hotel-initialize', '--limit-bytes=4097'], capture_output=True, timeout=30)
            if result.returncode != 0 or len(result.stdout) > 4096:
                raise CandidateK3sError('Hotel terminal log retrieval failed or exceeded its bound.')
            document['logs'][mode] = result.stdout.decode('utf-8')
            current = kubectl(['-n', CONTEXT['namespace'], 'get', 'pod', pod['name'], '-o', 'json'])
            if current['metadata']['uid'] != pod['uid'] or _canonical(current['spec']) != _canonical(pod['spec']):
                raise CandidateK3sError('Hotel terminal Pod changed during logs.')
    _parse_document(document, CONTEXT)
    return document

before = collect()
checked = _parse_document(before, CONTEXT, EXPECTED_IDENTITY)
if OPERATION == 'create':
    if checked['pods'] or any(item['metadata']['name'] in {approved['metadata']['name'] for approved in CONTEXT['initialization_manifest']['items']} for item in before['inventory']['items']):
        raise CandidateK3sError('Hotel initialization objects already exist; creation is never replayed.')
    payload = _canonical(CONTEXT['initialization_manifest']).encode()
    _exact(_admission_mutator_absence(admission_mutators()), checked['identity']['admission_mutators'], 'Hotel pre-create admission absence')
    completed = subprocess.run([K3S_BINARY, 'kubectl', 'create', '--validate=strict', '--field-manager=deathstarbench-hotel-initialization', '-f', '-'], input=payload, capture_output=True, timeout=120)
    if completed.returncode != 0:
        raise CandidateK3sError('Hotel initialization creation failed or was partial; cleanup only.')
    after = collect()
    checked_after = _parse_document(after, CONTEXT, checked['identity'])
    if set(checked_after['pods']) != set(MODES) or any(pod['state'] != 'gated' for pod in checked_after['pods'].values()):
        raise CandidateK3sError('Hotel initialization creation did not leave both exact Pods gated.')
    print(_canonical(after))
elif OPERATION == 'release':
    if EXPECTED_IDENTITY is None or set(checked['pods']) != set(MODES):
        raise CandidateK3sError('Hotel dispatch requires the exact durable blocked Pod identity.')
    selected = checked['pods'][MODE]
    if selected['state'] != 'gated':
        raise CandidateK3sError('Hotel dispatch gate already removed; replay forbidden.')
    if MODE == 'initialize-empty' and checked['pods']['verify']['state'] != 'gated':
        raise CandidateK3sError('Hotel verifier is not gated before initialization.')
    if MODE == 'verify' and not (checked['pods']['initialize-empty']['state'] == 'terminated' and checked['pods']['initialize-empty']['exit_code'] == 0):
        raise CandidateK3sError('Hotel global verifier requires exact successful initializer termination.')
    pod = selected['pod']
    patch = [{'op': 'test', 'path': '/metadata/' + key, 'value': pod['metadata'][key]}
             for key in ('uid', 'resourceVersion', 'name', 'namespace', 'labels', 'annotations')]
    patch.extend([{'op': 'test', 'path': '/spec', 'value': pod['spec']}, {'op': 'remove', 'path': '/spec/schedulingGates'}])
    # Exactly one patch invocation. Ambiguous transport/API response is never
    # retried, recreated, or inferred from a later successful-looking dataset.
    _exact(_admission_mutator_absence(admission_mutators()), checked['identity']['admission_mutators'], 'Hotel pre-release admission absence')
    completed = subprocess.run([K3S_BINARY, 'kubectl', '-n', CONTEXT['namespace'], 'patch', 'pod', selected['name'], '--type=json', '-p', _canonical(patch), '-o', 'json'], capture_output=True, timeout=60)
    if completed.returncode != 0:
        raise CandidateK3sError('Hotel one-shot gate release failed or is ambiguous; cleanup only.')
    patched = _strict_json(completed.stdout)
    if patched.get('metadata', {}).get('uid') != selected['uid']:
        raise CandidateK3sError('Hotel gate release returned a different Pod UID.')
    after = collect()
    _parse_document(after, CONTEXT, checked['identity'])
    print(_canonical(after))
elif OPERATION == 'wait':
    if EXPECTED_IDENTITY is None or MODE not in checked['pods'] or checked['pods'][MODE]['state'] == 'gated':
        raise CandidateK3sError('Hotel terminal wait requires an already dispatched exact Pod.')
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while checked['pods'][MODE]['state'] != 'terminated':
        if time.monotonic() >= deadline:
            raise CandidateK3sError('Hotel one-shot initialization deadline exceeded; cleanup only.')
        time.sleep(2)
        before = collect()
        checked = _parse_document(before, CONTEXT, EXPECTED_IDENTITY)
        if checked['pods'][MODE]['state'] == 'gated':
            raise CandidateK3sError('Hotel dispatched Pod unexpectedly regained a gate.')
    print(_canonical(before))
elif OPERATION == 'observe':
    print(_canonical(before))
else:
    raise CandidateK3sError('Unknown private Hotel operation.')
'''


_REMOTE_HELPERS = support._REMOTE_HELPERS + '\n\n' + '\n\n'.join(inspect.getsource(function) for function in (
    _exact, _admission_versions, _admission_mutator_absence, _initialization_pod, _endpoint_ports, _endpoint_graph, _live_inventory, _continuity, _parse_document,
))


def guarded_hotel_observation_command(plan, stage, role_nodes, expected_ca_sha256, *,
                                      operation='observe', mode=None, expected_identity=None, timeout_seconds=450):
    """Build a private command; only the leased controller may dispatch it.

    No command restores a consumed dispatch capability or starts application
    services. Creation and gate removal use one API write without retries.
    """
    context = _context(plan, stage, role_nodes, expected_ca_sha256)
    if operation not in ('observe', 'create', 'release', 'wait') or (mode not in MODES if operation in ('release', 'wait') else mode is not None):
        raise CandidateK3sError('Private Hotel command operation/mode is invalid.')
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 600:
        raise CandidateK3sError('Private Hotel timeout must be 1-600 seconds.')
    if operation in ('create', 'release', 'wait') and expected_identity is None:
        raise CandidateK3sError('Private Hotel mutations/waits require a prior live identity.')
    encoded = base64.b64encode(zlib.compress(_canonical({'context': context, 'expected_identity': expected_identity}).encode(), 9)).decode()
    script = (
        'from __future__ import annotations\n'
        'import base64, hashlib, ipaddress, json, os, re, subprocess, sys, time, zlib\n'
        'from collections.abc import Mapping\nfrom datetime import datetime\n'
        f'MAX_JSON_BYTES={MAX_JSON_BYTES}\nSCHEMA={SCHEMA!r}\n'
        f'ADMISSION_GROUP={ADMISSION_GROUP!r}\nMUTATOR_KINDS={MUTATOR_KINDS!r}\n'
        f'_OWNERSHIP_KEYS={support._OWNERSHIP_KEYS!r}\nK3S_BINARY={K3S_BINARY!r}\n'
        f'SCHEDULING_GATE={SCHEDULING_GATE!r}\nCOMPONENTS={COMPONENTS!r}\nMODES={MODES!r}\n'
        f'OPERATION={operation!r}\nMODE={mode!r}\nTIMEOUT_SECONDS={timeout_seconds}\n'
        + _REMOTE_HELPERS + '\n'
        + f'compressed=base64.b64decode({encoded!r}, validate=True)\n'
        + 'decoder=zlib.decompressobj()\nraw=decoder.decompress(compressed, MAX_JSON_BYTES + 1)\n'
        + "if len(raw)>MAX_JSON_BYTES or not decoder.eof or decoder.unused_data: raise CandidateK3sError('Invalid Hotel command context.')\n"
        + "payload=_strict_json(raw)\nCONTEXT=payload['context']\nEXPECTED_IDENTITY=payload['expected_identity']\n"
        + _REMOTE_OPERATIONS
    )
    return 'sudo python3 -c ' + shlex.quote(script)
