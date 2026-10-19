"""Read-only exact startup identity joins, never dispatch or readiness authority.

Observation schemas (version 1): CRI_SCHEMA has run_id, startup_attempt_id,
plan_fingerprint and entries: [{component, node_name, inspect, inspecti}]. The
last two fields are unmodified single-ID ``k3s crictl ... -o json`` objects.
Consul CONSUL_SCHEMA has the same identity fields plus agent_services (raw
GET /v1/agent/services), catalog_services (raw GET /v1/catalog/services), and
catalog_entries (service-name -> raw GET /v1/catalog/service/<name> list).
The observer must query complete, unfiltered, non-ACL-truncated inventories.
It must use the exact pinned Consul execution, no redirects, and fresh reads.

CRI node_name is the trusted observer's exact node-routing binding: CRI does
not supply a node name. Raw labels bind the execution to its Pod. Fields are
from the pinned K3s dependencies, not invented architecture/digest summaries:
https://github.com/k3s-io/k3s/blob/v1.36.4%2Bk3s1/go.mod
https://github.com/k3s-io/containerd/blob/v2.3.4-k3s1.36/internal/cri/server/container_status.go
https://github.com/k3s-io/containerd/blob/v2.3.4-k3s1.36/internal/cri/server/images/image_status.go
https://github.com/k3s-io/cri-tools/blob/v1.36.0-k3s1/cmd/crictl/image.go

Pod results retain original resource_version/admitted_metadata/admitted_spec,
latest observed_resource_version/spec/pod, and normalized execution fields.
Only gate removal and exact application nodeName binding can change the spec
after release; metadata remains pinned except resourceVersion and the exact
generation 1 -> 2 consequence of gate removal. Scheduler binding does not
increment generation. This rule is sourced from upstream Kubernetes v1.36.4
(not a live/fork observation); unexpected fork behavior fails closed:
https://github.com/kubernetes/kubernetes/blob/v1.36.4/pkg/registry/core/pod/strategy.go
https://github.com/kubernetes/kubernetes/blob/v1.36.4/pkg/registry/core/pod/storage/storage.go
Endpoint
inputs are raw support Pods and component -> normalized startup Pod results.
Consul execution is an independently support-validated normalized record
(component, role, pod_name, pod_uid, pod_ip, node_name, container_id, image,
image_id). The unchanged support command uses hostname=Pod name and dc1;
intrinsic consul is catalog-only, has an empty ServiceAddress, and port 8300.
Consul node UUID is unrelated to Kubernetes Pod UID and is never equated to it.
The pinned Consul dev default advertises Node Address=127.0.0.1, not the HTTP
Pod IP. Trusted observer routing and exact Pod hostname bind this local agent:
https://github.com/hashicorp/consul/blob/v1.20.5/agent/config/default.go

Endpoint/Consul validators default to strict complete joins. Explicit
allow_pending=True permits only validated asynchronous publication lag, never
execution/readiness authority. Endpoint rows then add publication_pending;
Consul adds a sorted publication_pending backend component list and excludes
partial joins or Pending-Pod publications from backend_registrations. Missing
Pod IP/node permits only absent publication, never adopting a presented row.
Callers must separately reject regression after previously confirmed joins.
Pinned-version endpoint conditions/placeholder source:
https://github.com/kubernetes/kubernetes/blob/v1.36.4/staging/src/k8s.io/endpointslice/utils.go
https://github.com/kubernetes/kubernetes/blob/v1.36.4/staging/src/k8s.io/endpointslice/reconciler.go
"""

from __future__ import annotations

from functools import wraps
import ipaddress
import json
import re

from . import deathstarbench_candidate_k3s as support
from .deathstarbench_candidate_workload import validate_candidate_assets
from .deathstarbench_hotel_startup_runtime import (
    APPLICATIONS, NAMESPACE, StartupHotelRuntimePlan, validate_startup_hotel_runtime_plan,
)
from .deathstarbench_hotel_startup_stage import SCHEDULING_GATE


CRI_SCHEMA = 'hotel-startup-cri-evidence-v1'
CONSUL_SCHEMA = 'hotel-startup-consul-evidence-v1'
MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
_CID = re.compile(r'containerd://[0-9a-f]{64}\Z')


class StartupHotelIdentityError(ValueError):
    """A startup observation is malformed, crossed, mutated or stale."""


def _require(condition, message):
    if not condition:
        raise StartupHotelIdentityError(message)


def _boundary(function):
    @wraps(function)
    def checked(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (ValueError, KeyError, TypeError, AttributeError, IndexError, RecursionError) as exc:
            if isinstance(exc, StartupHotelIdentityError):
                raise
            raise StartupHotelIdentityError('Startup Hotel identity evidence is malformed or crossed.') from None
    return checked


def _copy(value):
    encoded = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    _require(len(encoded.encode()) <= MAX_EVIDENCE_BYTES, 'Startup Hotel evidence exceeds its bound.')
    return json.loads(encoded)


def _map(value):
    _require(type(value) is dict, 'An exact identity object is required.')
    return value


def _list(value):
    _require(type(value) is list, 'An exact identity array is required.')
    return value


def _same(actual, expected, label):
    equal = (actual == expected if type(actual) is set and type(expected) is set else
             support._canonical(actual) == support._canonical(expected))
    _require(equal, label + ' drifted.')


def _ip(value):
    address = ipaddress.ip_address(value)
    _require(type(value) is str and str(address) == value
             and address in ipaddress.ip_network('10.42.0.0/16'), 'The exact private K3s Pod IP is required.')
    return value


def _publication_binding_known(pod):
    """Validate every currently known binding; missing facts prove no publication."""
    address, node = pod.get('pod_ip'), pod.get('node_name')
    if address not in (None, ''):
        _ip(address)
    if node not in (None, ''):
        _require(type(node) is str and bool(node), 'Publication Pod node binding is malformed.')
    return address not in (None, '') and node not in (None, '')


def _defaults(actual, desired, defaults):
    for name in set(actual) - set(desired):
        _require(name in defaults and any(support._canonical(actual[name]) == support._canonical(item)
                                         for item in defaults[name]), 'An unapproved admitted field exists.')
        actual.pop(name)


def _submitted_spec(spec, approved):
    """Normalize only enumerated API defaults, never the retained admitted copy."""
    actual, desired = _copy(_map(spec)), _copy(_map(approved))
    actual.pop('schedulingGates', None)
    desired.pop('schedulingGates')
    actual.pop('nodeName', None)
    tolerations = _list(actual.pop('tolerations', []))
    allowed = [{'key': 'node.kubernetes.io/' + key, 'operator': 'Exists',
                'effect': 'NoExecute', 'tolerationSeconds': 300}
               for key in ('not-ready', 'unreachable')]
    _require(len(tolerations) <= 2 and len({support._canonical(t) for t in tolerations}) == len(tolerations)
             and all(t in allowed for t in tolerations), 'Unapproved startup Pod tolerations.')
    for name in ('hostNetwork', 'hostPID', 'hostIPC', 'shareProcessNamespace'):
        if name not in actual and desired.get(name) is False:
            actual[name] = False
    _defaults(actual, desired, {
        'serviceAccountName': ('default',), 'serviceAccount': ('default',),
        'preemptionPolicy': ('PreemptLowerPriority',), 'priority': (0,),
        'imagePullSecrets': (None, []), 'initContainers': (None, []),
        'ephemeralContainers': (None, []), 'volumes': (None, []),
        'affinity': (None, {}), 'hostAliases': (None, []), 'dnsConfig': (None, {}),
        'resourceClaims': (None, []), 'topologySpreadConstraints': (None, []),
    })
    containers = _list(actual.get('containers'))
    _require(len(containers) == 1, 'Exactly one startup application container is required.')
    container = _map(containers[0])
    if 'args' not in container and desired['containers'][0]['args'] == []:
        container['args'] = []
    _defaults(container, desired['containers'][0], {
        'terminationMessagePath': ('/dev/termination-log',), 'terminationMessagePolicy': ('File',),
        'stdin': (False,), 'stdinOnce': (False,), 'tty': (False,),
    })
    _same(actual, desired, 'Complete startup Pod specification')


@_boundary
def validate_startup_hotel_admitted_pod(pod: dict, approved: dict, role_nodes: dict[str, str], *,
                                       pinned: dict | None = None, released: bool = False) -> dict:
    """Pin exact admission and allow only explicitly released scheduling changes."""
    pod, approved = _copy(_map(pod)), _copy(_map(approved))
    _require(type(released) is bool and (not released or pinned is not None),
             'Release requires the original admitted Pod pin.')
    _require(pod.get('apiVersion') == approved.get('apiVersion') == 'v1'
             and pod.get('kind') == approved.get('kind') == 'Pod', 'Startup Pod API identity drifted.')
    metadata, expected = _map(pod.get('metadata')), _map(approved.get('metadata'))
    support._uid(metadata.get('uid'), 'Startup Pod')
    version = metadata.get('resourceVersion')
    _require(type(version) is str and re.fullmatch(r'[1-9][0-9]{0,31}', version),
             'Startup Pod resourceVersion is invalid.')
    _require(type(metadata.get('generation')) is int and metadata['generation'] == (2 if released else 1),
             'Startup Pod generation differs from exact creation/gate-removal transition.')
    for key in ('name', 'namespace', 'labels', 'annotations'):
        _same(metadata.get(key), expected[key], 'Startup Pod ' + key)
    _require(metadata['namespace'] == NAMESPACE and metadata.get('deletionTimestamp') is None
             and metadata.get('ownerReferences') in (None, []) and metadata.get('finalizers') in (None, [])
             and 'generateName' not in metadata, 'Startup Pod must remain standalone and nondeleting.')
    _require(set(metadata) <= {'name', 'namespace', 'labels', 'annotations', 'uid', 'resourceVersion',
                              'creationTimestamp', 'managedFields', 'generation', 'deletionTimestamp',
                              'ownerReferences', 'finalizers'}, 'Unknown admitted Pod metadata.')
    component = expected['labels']['app.kubernetes.io/component']
    _require(component in APPLICATIONS, 'An exact startup component is required.')
    spec = _map(pod.get('spec'))
    gates = spec.get('schedulingGates', [])
    _same(gates, [] if released else [{'name': SCHEDULING_GATE}], 'Startup scheduling gate')
    node = spec.get('nodeName')
    application_node = _map(role_nodes)['application']
    _require(type(application_node) is str and bool(application_node)
             and node in ((None, '', application_node) if released else (None, '')),
             'Startup Pod exact application node binding drifted.')
    _submitted_spec(spec, approved['spec'])
    original_metadata, original_spec = metadata, spec
    if pinned is not None:
        pinned = _map(pinned)
        original_metadata, original_spec = pinned['admitted_metadata'], pinned['admitted_spec']
        expected_metadata = {k: v for k, v in original_metadata.items() if k != 'resourceVersion'}
        _require(expected_metadata['generation'] == 1, 'Original admitted generation must remain one.')
        if released:
            expected_metadata['generation'] = 2
        _same({k: v for k, v in metadata.items() if k != 'resourceVersion'}, expected_metadata,
              'Pinned admitted Pod metadata')
        comparable = _copy(original_spec)
        if released:
            comparable.pop('schedulingGates', None)
            if 'schedulingGates' in spec:
                comparable['schedulingGates'] = []
            if 'nodeName' in spec:
                comparable['nodeName'] = node
        _same(spec, comparable, 'Pinned complete admitted Pod specification')
        _same(metadata['uid'], pinned['pod_uid'], 'Pinned Pod UID')
        _require(not pinned.get('released', False) or released, 'Startup gate release cannot be reversed.')
    result = {'component': component, 'pod_name': metadata['name'], 'pod_uid': metadata['uid'],
              'resource_version': original_metadata['resourceVersion'], 'observed_resource_version': version,
              'admitted_metadata': _copy(original_metadata), 'admitted_spec': _copy(original_spec),
              'spec': _copy(spec), 'pod': pod, 'released': released,
              'node_name': node, 'pod_ip': None, 'container_id': None, 'image_id': None,
              'started_at': None,
              'image': approved['spec']['containers'][0]['image'], 'state': 'pending' if released else 'gated'}
    status = _map(pod.get('status', {}))
    _require(status.get('initContainerStatuses') in (None, [])
             and status.get('ephemeralContainerStatuses') in (None, []), 'Unexpected startup container status.')
    statuses = _list(status.get('containerStatuses', []))
    _require(len(statuses) <= 1, 'Extra startup container executions.')
    if not released:
        _require(not statuses and status.get('phase') in (None, 'Pending')
                 and status.get('podIP') in (None, '') and status.get('podIPs') in (None, []),
                 'Gated startup Pod already executed.')
    else:
        if status.get('podIP') not in (None, ''):
            result['pod_ip'] = _ip(status['podIP'])
            _same(status.get('podIPs', [{'ip': result['pod_ip']}]), [{'ip': result['pod_ip']}], 'Pod IP inventory')
        _require(status.get('phase') in (None, 'Pending', 'Running'), 'Startup Pod failed or terminated.')
        if statuses:
            container = _map(statuses[0])
            _require(container.get('name') == component and type(container.get('restartCount')) is int
                     and container['restartCount'] == 0 and container.get('lastState') == {},
                     'Startup container name, restart count or prior execution drifted.')
            state = _map(container.get('state'))
            _require(len(state) == 1 and set(state) <= {'waiting', 'running'}, 'Startup execution failed or is ambiguous.')
            if 'running' in state:
                image = result['image']
                # Deliberately the same strict matcher as existing validators;
                # a CRI config digest is not a Kubernetes platform imageID.
                _require(container.get('imageID') in (image, 'docker-pullable://' + image)
                         and container.get('image') == image, 'Startup Pod platform imageID drifted.')
                _require(node == application_node and result['pod_ip'] is not None
                         and status.get('phase') == 'Running'
                         and type(container.get('containerID')) is str
                         and _CID.fullmatch(container['containerID']), 'Startup execution binding is incomplete.')
                support._timestamp(_map(state['running']).get('startedAt'))
                result.update(state='running', container_id=container['containerID'], image_id=container['imageID'],
                              started_at=state['running']['startedAt'])
            else:
                _require(status.get('phase') == 'Pending' and container.get('containerID') in (None, ''),
                         'Waiting startup container has already executed.')
        else:
            _require(status.get('phase') in (None, 'Pending'), 'Running startup Pod lacks its container execution.')
    if pinned is not None:
        for key in ('pod_ip', 'container_id', 'image_id', 'node_name', 'started_at'):
            if pinned.get(key) not in (None, ''):
                _same(result[key], pinned[key], 'Pinned startup ' + key)
        if pinned.get('state') == 'running':
            _same(result['state'], 'running', 'Pinned running execution')
    return result


@_boundary
def validate_startup_hotel_endpoints(slices: list[dict], services: list[dict],
                                   support_pods: list[dict], startup_pods: dict[str, dict], *,
                                   allow_pending: bool = False) -> dict:
    """Join each complete Service inventory to one exact live Pod or an empty placeholder."""
    _require(type(allow_pending) is bool, 'Publication pending mode must be explicit boolean.')
    slices, services, support_pods, startup_pods = _copy([slices, services, support_pods, startup_pods])
    components, _ = validate_candidate_assets('hotel_reservation')
    support_names = {item['name'] for item in components if item['role'] != 'application'}
    targets, target_labels, pending_eligible, bound = {}, {}, set(), {}
    for pod in _list(support_pods):
        metadata = _map(pod['metadata'])
        component = metadata['labels']['app.kubernetes.io/component']
        _require(component not in targets and component in support_names
                 and pod.get('kind') == 'Pod' and pod.get('apiVersion') == 'v1'
                 and metadata['namespace'] == NAMESPACE and metadata.get('deletionTimestamp') is None,
                 'Support endpoint Pod inventory drifted.')
        status = _map(pod['status'])
        targets[component] = {'pod_name': metadata['name'], 'pod_uid': metadata['uid'],
                              'pod_ip': _ip(status['podIP']), 'node_name': pod['spec']['nodeName']}
        target_labels[component] = metadata['labels']
        _same(status.get('podIPs', [{'ip': status['podIP']}]), [{'ip': status['podIP']}], 'Support Pod IP inventory')
    _same(set(targets), support_names, 'Complete support endpoint Pod inventory')
    for component, pod in _map(startup_pods).items():
        _require(component in APPLICATIONS and pod['component'] == component, 'Startup endpoint component drifted.')
        if allow_pending and pod.get('released') is True and pod.get('state') in ('pending', 'running'):
            pending_eligible.add(component)
            bound[component] = _publication_binding_known(pod)
            _require(pod['state'] != 'running' or bound[component],
                     'Running endpoint Pod lacks a proven IP/node binding.')
        if pod.get('state') == 'running' or component in pending_eligible:
            targets[component] = _map(pod)
            target_labels[component] = pod['pod']['metadata']['labels']
    service_map = {}
    for service in _list(services):
        metadata = _map(service['metadata'])
        name = metadata['name']
        support._uid(metadata['uid'], 'Startup Service')
        _require(name not in service_map and service.get('kind') == 'Service' and service.get('apiVersion') == 'v1'
                 and metadata.get('namespace') == NAMESPACE and metadata.get('deletionTimestamp') is None,
                 'Startup Service inventory drifted.')
        service_map[name] = service
    _same(set(service_map), set(targets) | set(APPLICATIONS), 'Complete startup Service inventory')
    for component, target in targets.items():
        support._uid(target['pod_uid'], 'Endpoint Pod')
        _require(target.get('pod_ip') not in (None, '') or component in pending_eligible
                 and target.get('state') == 'pending', 'Endpoint Pod IP is missing.')
    known_ips = [_ip(p['pod_ip']) for p in targets.values() if p.get('pod_ip') not in (None, '')]
    _require(len({p['pod_uid'] for p in targets.values()}) == len(targets)
             and len(set(known_ips)) == len(known_ips), 'Pod UID/IP inventory is not unique.')
    result, slice_uids = {}, set()
    for item in _list(slices):
        metadata = _map(item['metadata'])
        labels = _map(metadata['labels'])
        name = labels['kubernetes.io/service-name']
        support._uid(metadata['uid'], 'Startup EndpointSlice')
        _require(name in service_map and name not in result and metadata['uid'] not in slice_uids
                 and item.get('apiVersion') == 'discovery.k8s.io/v1' and item.get('kind') == 'EndpointSlice'
                 and item.get('addressType') == 'IPv4' and metadata.get('namespace') == NAMESPACE
                 and metadata.get('deletionTimestamp') is None
                 and labels.get('endpointslice.kubernetes.io/managed-by') == 'endpointslice-controller.k8s.io',
                 'Startup EndpointSlice inventory, manager or address family drifted.')
        slice_uids.add(metadata['uid'])
        service = service_map[name]
        _same(metadata.get('ownerReferences'), [{'apiVersion': 'v1', 'kind': 'Service', 'name': name,
              'uid': service['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}],
              'EndpointSlice exact Service owner')
        row = {'service_uid': service['metadata']['uid'], 'endpoint_slice_uid': metadata['uid'],
               'pod_uid': None, 'pod_ip': None, 'pod_name': None, 'node_name': None}
        if allow_pending:
            row['publication_pending'] = False
        if name not in targets:
            _require(item.get('endpoints') in (None, []) and item.get('ports') in (None, []),
                     'A not-running startup component has an endpoint.')
        else:
            target = targets[name]
            selector = _map(service['spec'].get('selector'))
            _require(bool(selector) and all(target_labels[name].get(key) == value for key, value in selector.items()),
                     'Service selector differs from its exact endpoint Pod.')
            support._uid(target['pod_uid'], 'Endpoint Pod')
            expected_ports = [{'name': p.get('name', ''), 'port': p['targetPort'], 'protocol': p.get('protocol', 'TCP')}
                              for p in service['spec']['ports']]
            empty_pending = name in pending_eligible and item.get('endpoints') in (None, [])
            if not empty_pending or item.get('ports') not in (None, []):
                ports = _list(item.get('ports'))
                _require(all(type(p) is dict and set(p) == {'name', 'port', 'protocol'}
                             and type(p['port']) is int for p in ports), 'EndpointSlice ports are malformed.')
                _same(sorted(ports, key=lambda p: p['name']), sorted(expected_ports, key=lambda p: p['name']),
                      'EndpointSlice complete port inventory')
            if empty_pending:
                row['publication_pending'] = True
            else:
                endpoints = _list(item.get('endpoints'))
                _require(len(endpoints) == 1, 'Exactly one live endpoint per Service is required.')
                if name in pending_eligible:
                    _require(bound[name], 'Presented endpoint lacks a proven Pod IP/node binding.')
                endpoint = _map(endpoints[0])
                _same(endpoint.get('addresses'), [target['pod_ip']], 'Endpoint exact Pod IP')
                _same(endpoint.get('nodeName'), target['node_name'], 'Endpoint exact Pod node')
                conditions = _map(endpoint.get('conditions'))
                if name in pending_eligible and conditions.get('ready') is False:
                    _same(conditions, {'ready': False, 'serving': False, 'terminating': False},
                          'Exact nonterminating pending endpoint conditions')
                    row['publication_pending'] = True
                else:
                    _require(conditions.get('ready') is True and conditions.get('serving', True) is True
                             and conditions.get('terminating', False) is False, 'Endpoint is not ready and serving.')
                reference = _copy(_map(endpoint.get('targetRef')))
                reference.setdefault('apiVersion', 'v1')
                _same(reference, {'apiVersion': 'v1', 'kind': 'Pod', 'namespace': NAMESPACE,
                                 'name': target['pod_name'], 'uid': target['pod_uid']}, 'Endpoint exact Pod target')
                row.update({key: target[key] for key in ('pod_uid', 'pod_ip', 'pod_name', 'node_name')})
                if name in pending_eligible and target['state'] != 'running':
                    row['publication_pending'] = True
        result[name] = row
    _same(set(result), set(service_map), 'Complete EndpointSlice inventory')
    return result


def _observation(evidence, plan, schema, fields):
    plan = validate_startup_hotel_runtime_plan(plan)
    value = _copy(_map(evidence))
    _same(set(value), {'schema_version', 'candidate_schema', 'run_id', 'startup_attempt_id',
                       'plan_fingerprint'} | set(fields), 'Startup observation schema')
    _require(type(value['schema_version']) is int and value['schema_version'] == 1
             and value['candidate_schema'] == schema and value['run_id'] == plan.run_id
             and value['startup_attempt_id'] == plan.startup_attempt_id
             and value['plan_fingerprint'] == plan.fingerprint, 'Startup observation plan identity drifted.')
    return value, plan.snapshot()


def _pods(pods, plan):
    pods = _copy(_map(pods))
    for component, pod in pods.items():
        _require(component in APPLICATIONS and pod['component'] == component
                 and pod['pod_name'] == plan.application_pod_name(component)
                 and pod['image'] == plan.application_image, 'Startup execution differs from its exact plan.')
        support._uid(pod['pod_uid'], 'Startup execution Pod')
        if pod['state'] == 'running':
            _ip(pod['pod_ip'])
            _require(type(pod['container_id']) is str and _CID.fullmatch(pod['container_id'])
                     and pod['image_id'] in (plan.application_image, 'docker-pullable://' + plan.application_image)
                     and type(pod['node_name']) is str and pod['node_name'], 'Startup execution identity is incomplete.')
    return pods


@_boundary
def validate_startup_hotel_cri_evidence(evidence: dict, plan: StartupHotelRuntimePlan,
                                      pods: dict[str, dict]) -> dict:
    """Join raw container/config/manifest/native-platform evidence to every running Pod."""
    evidence, snapshot = _observation(evidence, plan, CRI_SCHEMA, {'entries'})
    pods = _pods(pods, plan)
    expected = {name for name, pod in pods.items() if pod['state'] == 'running'}
    result = {}
    config = snapshot['application_config_digest']
    architecture = {'aarch64': 'arm64', 'x86_64': 'amd64'}[plan.application_architecture]
    for entry in _list(evidence['entries']):
        _same(set(entry), {'component', 'node_name', 'inspect', 'inspecti'}, 'CRI entry schema')
        component = entry['component']
        _require(component in expected and component not in result, 'Extra or duplicate CRI execution.')
        pod = pods[component]
        _same(entry['node_name'], pod['node_name'], 'CRI routed node')
        status = _map(entry['inspect']['status'])
        _same(status.get('id'), pod['container_id'].removeprefix('containerd://'), 'CRI container ID')
        _same(status.get('metadata'), {'name': component, 'attempt': 0}, 'CRI component and execution attempt')
        _require(status.get('state') == 'CONTAINER_RUNNING', 'CRI container is not running.')
        labels = _map(status.get('labels'))
        for key, wanted in {'io.kubernetes.pod.uid': pod['pod_uid'], 'io.kubernetes.pod.name': pod['pod_name'],
                            'io.kubernetes.pod.namespace': plan.namespace,
                            'io.kubernetes.container.name': component}.items():
            _same(labels.get(key), wanted, 'CRI raw Pod binding')
        _same(status.get('imageRef'), plan.application_image, 'CRI selected platform manifest')
        _same(status.get('imageId'), config, 'CRI executed config digest')
        image_status = _map(entry['inspecti']['status'])
        _same(image_status.get('id'), config, 'CRI inspected config digest')
        digests = _list(image_status.get('repoDigests'))
        _require(plan.application_image in digests and all(type(d) is str for d in digests)
                 and len(set(digests)) == len(digests), 'CRI image lacks the exact platform manifest.')
        image_spec = _map(entry['inspecti']['info']['imageSpec'])
        _same(image_spec.get('os'), 'linux', 'CRI native operating system')
        _same(image_spec.get('architecture'), architecture, 'CRI native architecture')
        result[component] = {key: pod[key] for key in ('pod_name', 'pod_uid', 'node_name', 'container_id')}
        result[component].update(manifest_image=plan.application_image, config_digest=config,
                                 os='linux', architecture=architecture)
    _same(set(result), expected, 'Complete CRI running execution inventory')
    _require(len({p['container_id'] for p in result.values()}) == len(result), 'Duplicate CRI container ID.')
    return result


@_boundary
def validate_startup_hotel_consul_identity(evidence: dict, plan: StartupHotelRuntimePlan,
                                         pods: dict[str, dict], started_components: tuple[str, ...],
                                         consul_execution: dict, *, allow_pending: bool = False) -> dict:
    """Require exact backend IDs and one separately bound intrinsic Consul server."""
    _require(type(allow_pending) is bool, 'Publication pending mode must be explicit boolean.')
    evidence, snapshot = _observation(evidence, plan, CONSUL_SCHEMA,
                                      {'agent_services', 'catalog_services', 'catalog_entries'})
    pods = _pods(pods, plan)
    _require(type(started_components) is tuple and len(set(started_components)) == len(started_components)
             and all(name in APPLICATIONS and name in pods
                     and (not allow_pending or pods[name].get('released') is True)
                     and (pods[name]['state'] == 'running' or allow_pending and pods[name]['state'] == 'pending'
                          and pods[name].get('released') is True)
                     for name in started_components), 'Started component execution inventory drifted.')
    execution = _map(consul_execution)
    support._uid(execution['pod_uid'], 'Consul execution Pod')
    _ip(execution['pod_ip'])
    support_image = plan.support_plan.artifacts.image_bindings['consul']['platforms']['linux/amd64']['image']
    _require(execution.get('component') == 'consul' and execution.get('role') == 'control'
             and type(execution.get('pod_name')) is str and bool(execution['pod_name'])
             and type(execution.get('node_name')) is str and bool(execution['node_name'])
             and type(execution.get('container_id')) is str and _CID.fullmatch(execution['container_id'])
             and execution.get('image') == support_image
             and execution.get('image_id') in (support_image, 'docker-pullable://' + support_image),
             'The exact independently validated Consul support execution is required.')
    backends = set(started_components) - {'frontend'}
    catalog_names = {'consul'} | {'srv-' + component for component in backends}
    catalog, entries = _map(evidence['catalog_services']), _map(evidence['catalog_entries'])
    if allow_pending:
        _require('consul' in catalog and 'consul' in entries and set(catalog) <= catalog_names
                 and set(entries) <= catalog_names, 'Unexpected or missing intrinsic Consul catalog inventory.')
    else:
        _same(set(catalog), catalog_names, 'Complete Consul catalog service inventory')
        _same(set(entries), catalog_names, 'Complete Consul catalog entries')
    for tags in catalog.values():
        _same(tags, [], 'Consul catalog service tags')
    intrinsic = _list(entries['consul'])
    _require(len(intrinsic) == 1, 'Exactly one intrinsic Consul server is required.')
    intrinsic = _map(intrinsic[0])
    node = {'Node': execution['pod_name'], 'Address': '127.0.0.1', 'Datacenter': 'dc1'}
    for key, wanted in {**node, 'ServiceID': 'consul', 'ServiceName': 'consul',
                        'ServiceAddress': '', 'ServicePort': 8300}.items():
        _same(intrinsic.get(key), wanted, 'Intrinsic Consul server binding')
    _require(type(intrinsic.get('ServicePort')) is int and intrinsic.get('ServiceKind', '') == ''
             and intrinsic.get('ServiceTags') in (None, []), 'Intrinsic Consul port, kind or tags drifted.')
    registrations, pending = {}, []
    ports = {'profile': 8081, 'search': 8082, 'geo': 8083, 'rate': 8084, 'recommendation': 8085,
             'user': 8086, 'reservation': 8087, 'review': 8088, 'attractions': 8089}
    agent = _map(evidence['agent_services'])
    expected_ids = set()
    for component in sorted(backends):
        pod = pods[component]
        service_id = 'hotel-' + plan.run_id + '-' + plan.startup_attempt_id + '-' + component + '-' + pod['pod_uid']
        expected_ids.add(service_id)
        meta = {'run_id': plan.run_id, 'startup_attempt_id': plan.startup_attempt_id,
                'component': component, 'pod_uid': pod['pod_uid'], 'contract_sha256': snapshot['dataset_fingerprint']}
        agent_present = service_id in agent
        rows = _list(entries.get('srv-' + component, []) if allow_pending else entries['srv-' + component])
        _require(len(rows) <= 1 if allow_pending else len(rows) == 1,
                 'Exactly one catalog registration per backend is required.')
        if allow_pending:
            binding_known = _publication_binding_known(pod)
            _require(not (agent_present or 'srv-' + component in catalog or rows) or binding_known,
                     'Presented registration lacks a proven Pod IP/node binding.')
        if agent_present or not allow_pending:
            service = _map(agent.get(service_id))
            for key, wanted in {'ID': service_id, 'Service': 'srv-' + component, 'Address': pod['pod_ip'],
                                'Port': ports[component], 'Meta': meta}.items():
                _same(service.get(key), wanted, 'Consul exact agent registration')
            _require(type(service.get('Port')) is int, 'Consul backend port is malformed.')
            _require(service.get('Tags') in (None, []) and service.get('Kind', '') == '', 'Unexpected Consul service tags/kind.')
        if rows:
            row = _map(rows[0])
            for key, wanted in {**node, 'ServiceID': service_id, 'ServiceName': 'srv-' + component,
                                'ServiceAddress': pod['pod_ip'], 'ServicePort': ports[component], 'ServiceMeta': meta}.items():
                _same(row.get(key), wanted, 'Consul exact backend catalog binding')
            _require(type(row.get('ServicePort')) is int and row.get('ServiceTags') in (None, [])
                     and row.get('ServiceKind', '') == '', 'Unexpected backend catalog port/tags/kind.')
        if allow_pending and (not agent_present or not rows or 'srv-' + component not in catalog
                              or pod['state'] != 'running'):
            pending.append(component)
        else:
            registrations[component] = {'service_id': service_id, 'pod_uid': pod['pod_uid'],
                                         'pod_ip': pod['pod_ip'], 'port': ports[component]}
    if allow_pending:
        _require(set(agent) <= expected_ids, 'Unexpected Consul agent registration inventory.')
    else:
        _same(set(agent), expected_ids, 'Complete Consul agent registration inventory')
    result = {'backend_registrations': registrations, 'frontend_registrations': 0,
            'intrinsic_consul': {**{key: execution[key] for key in
                                   ('pod_name', 'pod_uid', 'pod_ip', 'node_name', 'container_id')},
                                'service_id': 'consul', 'port': 8300, 'datacenter': 'dc1',
                                'catalog_node_address': '127.0.0.1'}}
    if allow_pending:
        result['publication_pending'] = pending
    return result
