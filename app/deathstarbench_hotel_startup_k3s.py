"""Bounded read-only Hotel startup evidence, never execution authority.

Only the leased controller can guarantee dispatch routing and ordering. Retain
admission pins in memory, acknowledge each release, then execute Kubernetes ->
application-node CRI -> final Kubernetes. Pass the first identity to the final
parser. Dispatcher must share one <=60-second triplet deadline, passing a
shrinking remainder to each command, within the original <=900-second startup
window. The join additionally caps actual triplet wire output at 4 MiB.
Neither serialized observations nor these pure joins restore a lease.
Raw Pod/status dictionaries below are internal only, never history-safe.
"""
from __future__ import annotations

import base64
import inspect
import json
import math
import re
import shlex
import zlib

from . import deathstarbench_candidate_k3s as support
from . import deathstarbench_hotel_live_k3s as initialization
from . import deathstarbench_hotel_startup_identity as identity
from .deathstarbench_hotel_startup_runtime import validate_startup_hotel_runtime_plan
from .deathstarbench_hotel_startup_stage import validate_startup_hotel_startup_stage
from .k3s_runtime import K3S_BINARY

MAX_JSON_BYTES = 4 * 1024 * 1024
SCHEMA = 'hotel-startup-kubernetes-observation-v1'
CRI_OBSERVATION_SCHEMA = 'hotel-startup-cri-observation-v1'
POD_PINNING_PROJECTION = 'hotel-startup-pod-pin-managed-fields-elided-v1'
CandidateK3sError = support.CandidateK3sError
_strict_json = support._strict_json
_canonical = support._canonical
_uid = support._uid

# Every collection is individually discovered and completely listed. No label
# selector or pre-filtered multi-kind kubectl List may stand in for these reads.
COLLECTIONS = (
    ('v1', 'namespaces', 'Namespace', False), ('v1', 'nodes', 'Node', False),
    ('v1', 'persistentvolumes', 'PersistentVolume', False),
    *(( 'v1', name, kind, True) for name, kind in (
        ('pods', 'Pod'), ('services', 'Service'), ('persistentvolumeclaims', 'PersistentVolumeClaim'),
        ('replicationcontrollers', 'ReplicationController'), ('podtemplates', 'PodTemplate'),
        ('resourcequotas', 'ResourceQuota'), ('limitranges', 'LimitRange'),
        ('secrets', 'Secret'), ('serviceaccounts', 'ServiceAccount'), ('configmaps', 'ConfigMap'))),
    *(( 'apps/v1', name, kind, True) for name, kind in (
        ('deployments', 'Deployment'), ('replicasets', 'ReplicaSet'),
        ('statefulsets', 'StatefulSet'), ('daemonsets', 'DaemonSet'))),
    ('batch/v1', 'jobs', 'Job', True), ('batch/v1', 'cronjobs', 'CronJob', True),
    ('networking.k8s.io/v1', 'networkpolicies', 'NetworkPolicy', True),
    ('networking.k8s.io/v1', 'ingresses', 'Ingress', True),
    ('autoscaling/v2', 'horizontalpodautoscalers', 'HorizontalPodAutoscaler', True),
    ('rbac.authorization.k8s.io/v1', 'roles', 'Role', True),
    ('rbac.authorization.k8s.io/v1', 'rolebindings', 'RoleBinding', True),
    ('discovery.k8s.io/v1', 'endpointslices', 'EndpointSlice', True),
)


def _require(value, message):
    if not value:
        raise CandidateK3sError(message)


def _byte_count(value):
    _require(type(value) is int and 0 < value <= MAX_JSON_BYTES,
             'Retained raw output byte count is invalid.')
    return value


def _collection_path(group, resource, namespaced, namespace):
    root = '/api/v1' if group == 'v1' else '/apis/' + group
    return root + ('/namespaces/' + namespace if namespaced else '') + '/' + resource


def _envelope(plan, stage, initialization_plan, initialization_stage, roles, ca):
    return {'schema_version': 1, 'candidate_schema': SCHEMA, 'run_id': plan.run_id,
            'startup_attempt_id': plan.startup_attempt_id, 'plan_fingerprint': plan.fingerprint,
            'stage_fingerprint': stage.fingerprint, 'initialization_plan_fingerprint': initialization_plan.fingerprint,
            'initialization_stage_fingerprint': initialization_stage.fingerprint,
            'role_nodes': roles, 'server_ca_sha256': ca}


def _context(plan, stage, initialization_plan, initialization_stage, roles, ca):
    plan = validate_startup_hotel_runtime_plan(plan)
    stage = validate_startup_hotel_startup_stage(plan, stage)
    context = initialization._context(initialization_plan, initialization_stage, roles, ca)
    _require(plan.support_plan == initialization_plan.support_plan
             and plan.support_stage == initialization_plan.support_stage
             and plan.initialization_attempt_id == initialization_plan.initialization_attempt_id
             and plan.snapshot()['dataset_fingerprint'] == initialization_plan.snapshot()['dataset_fingerprint'],
             'Startup and initializer must retain the same original support and dataset.')
    return context


def _complete_list(document, group, kind):
    _require(isinstance(document, dict) and document.get('apiVersion') == group
             and document.get('kind') == kind + 'List', 'Collection API identity is invalid.')
    meta = document.get('metadata', {})
    _require(isinstance(meta, dict) and isinstance(meta.get('resourceVersion'), str)
             and 0 < len(meta['resourceVersion']) <= 128 and meta.get('continue') in (None, '')
             and (meta.get('remainingItemCount') is None or type(meta['remainingItemCount']) is int
                  and meta['remainingItemCount'] == 0), 'Collection is missing complete list evidence.')
    items = document.get('items')
    _require(isinstance(items, list), 'Collection items are missing.')
    keys = set()
    for item in items:
        _require(isinstance(item, dict) and item.get('apiVersion') == group and item.get('kind') == kind,
                 'Collection contains an unexpected object API identity.')
        meta = item.get('metadata', {})
        key = (meta.get('namespace'), meta.get('name'))
        _require(isinstance(key[1], str) and key not in keys, 'Collection repeats an object.')
        keys.add(key)
    return items


def _collections(document, context):
    discovery, collections = document['discovery'], document['collections']
    _require(set(discovery) == {c[0] for c in COLLECTIONS}, 'Discovery omitted a required API version.')
    _require(set(collections) == {_collection_path(g, r, n, context['namespace']) for g, r, k, n in COLLECTIONS},
             'A required collection was missing or duplicated.')
    items, system = [], None
    for group, resource, kind, namespaced in COLLECTIONS:
        api = discovery[group]
        _require(api.get('apiVersion') == 'v1' and api.get('kind') == 'APIResourceList'
                 and api.get('groupVersion') == group, 'Resource discovery identity is invalid.')
        resources = api.get('resources', [])
        _require(isinstance(resources, list) and len({r['name'] for r in resources}) == len(resources),
                 'Discovery repeats a resource.')
        found = [r for r in resources if r['name'] == resource]
        _require(len(found) == 1 and found[0].get('kind') == kind
                 and found[0].get('namespaced') is namespaced and 'list' in found[0].get('verbs', []),
                 'Required collection was not successfully discovered.')
        listed = _complete_list(collections[_collection_path(group, resource, namespaced, context['namespace'])], group, kind)
        for item in listed:
            meta = item['metadata']
            if namespaced:
                _require(meta.get('namespace') == context['namespace'], 'Collection crossed its namespace.')
            else:
                _require(meta.get('namespace') is None, 'Cluster collection contains namespaced objects.')
            if kind == 'Namespace':
                if meta['name'] == 'kube-system': system = item
                if meta['name'] != context['namespace']: continue
            if kind == 'PersistentVolume':
                # Preserve the complete list as evidence; validate every PV
                # associated by any existing audited support-owner criterion.
                if not _associated_pv(item, context): continue
            items.append(item)
    _require(system is not None, 'The kube-system namespace identity is missing.')
    _uid(system['metadata'].get('uid'), 'Startup kube-system namespace')
    _require(system['metadata'].get('deletionTimestamp') is None, 'kube-system is deleting.')
    return items, system['metadata']['uid']


def _associated_pv(item, context):
    meta, spec = item['metadata'], item.get('spec', {})
    return (meta['name'] in {p['metadata']['name'] for p in context['manifest']['items'] if p['kind'] == 'PersistentVolume'}
            or meta['name'].startswith(context['namespace'] + '-')
            or spec.get('claimRef', {}).get('namespace') == context['namespace']
            or meta.get('annotations', {}).get('deathstarbench.io/candidate-run-id') == context['run_id']
            or meta.get('labels', {}).get('deathstarbench.io/workload') == 'hotel-reservation-v1')


def _key(item):
    return item['kind'], item['metadata'].get('namespace'), item['metadata']['name']


def _execution_anchor(pods):
    fields = ('pod_name', 'pod_uid', 'node_name', 'pod_ip', 'container_id', 'image', 'image_id', 'started_at', 'state')
    return {name: {field: pod[field] for field in fields} for name, pod in pods.items()}


def _services(items, context):
    """Tighten nested fields the historical support boundary checks as subsets."""
    approved = {p['metadata']['name']: p['spec'] for p in context['manifest']['items'] if p['kind'] == 'Service'}
    for service in (p for p in items if p['kind'] == 'Service'):
        desired, actual = approved[service['metadata']['name']], service['spec']
        _require(_canonical(actual.get('selector')) == _canonical(desired['selector']),
                 'Original Service selector inventory drifted.')
        ports = json.loads(_canonical(actual['ports']))
        _require(len(ports) == len(desired['ports']), 'Original Service port count drifted.')
        for port, original in zip(ports, desired['ports']):
            # The existing audited support boundary enumerates unset nodePort
            # as integer zero. No other nested API extension is normalized.
            if 'nodePort' not in original and 'nodePort' in port:
                _require(type(port['nodePort']) is int and port['nodePort'] == 0,
                         'Original Service exposes an unexpected NodePort.')
                del port['nodePort']
        _require(_canonical(ports) == _canonical(desired['ports']), 'Original Service port graph drifted.')


def parse_startup_hotel_kubernetes_observation(output, plan, stage, initialization_plan,
        initialization_stage, role_nodes, expected_ca_sha256, *, expected_identity=None):
    """Internal raw observation. expected_identity is trusted in-memory custody.

    The controller alone may change a retained startup pin's released flag after
    successful one-shot gate removal. The parser never infers that authority.
    """
    try:
        context = _context(plan, stage, initialization_plan, initialization_stage, role_nodes, expected_ca_sha256)
        document = _strict_json(output)
        raw_bytes = len(output) if isinstance(output, bytes) else len(output.encode('utf-8'))
        _byte_count(raw_bytes)
        first_raw_bytes = (expected_identity or {}).get('raw_output_bytes')
        if first_raw_bytes is not None:
            _byte_count(first_raw_bytes)
        _require((expected_identity or {}).get('execution_anchor') is None or first_raw_bytes is not None,
                 'The retained first Kubernetes byte count is missing.')
        envelope = _envelope(plan, stage, initialization_plan, initialization_stage, context['role_nodes'], context['server_ca_sha256'])
        _require(set(document) == set(envelope) | {'discovery', 'collections', 'admission_mutators', 'consul', 'consul_execution'},
                 'Startup observation envelope schema drifted.')
        initialization._exact({k: document[k] for k in envelope}, envelope, 'Startup observation envelope')
        items, system_uid = _collections(document, context)
        mutators = initialization._admission_mutator_absence(document['admission_mutators'])
        expected_init = {_key(p): p for p in json.loads(initialization_stage.canonical_json)['items']}
        expected_startup = {_key(stage.pod(c)): c for c in stage.metadata()['application_components']}
        additions, applications, slices, remainder = {}, {}, [], []
        pins = (expected_identity or {}).get('startup_pods', {})
        for item in items:
            key = _key(item)
            if key in expected_init:
                _require(key not in additions, 'Repeated initialization object.')
                additions[key] = item
            elif key in expected_startup:
                component = expected_startup[key]
                _require(component not in applications, 'Repeated startup object.')
                pin = pins.get(component)
                # Preserve the raw collection evidence. Only the detached pin
                # representation elides server field-manager bookkeeping, as
                # ordinary kubectl JSON printing does by default. No status,
                # spec, annotation or other metadata field is projected away.
                pinned_pod = json.loads(_canonical(item))
                pinned_pod['metadata'].pop('managedFields', None)
                applications[component] = identity.validate_startup_hotel_admitted_pod(pinned_pod, stage.pod(component),
                    role_nodes, pinned=pin, released=pin.get('released', False) if pin else False)
            elif item['kind'] == 'EndpointSlice': slices.append(item)
            else: remainder.append(item)
        _require(not additions or set(additions) == set(expected_init), 'Initializer objects are partial.')
        _require(not applications or set(applications) == set(expected_startup.values()), 'Startup objects are partial.')
        normalized = support._checked_inventory({'apiVersion': 'v1', 'kind': 'List', 'items': remainder}, context, complete=True)
        _services(remainder, context)
        _require(all(p['restart_count'] == 0 and not p['last_state'] for p in normalized['support_pods']),
                 'Support execution restarted.')
        initializers = {}
        for key, item in additions.items():
            approved = expected_init[key]
            if item['kind'] == 'Pod':
                mode = approved['spec']['containers'][0]['args'][0]
                initializers[mode] = initialization._initialization_pod(item, approved, context)
            else:
                support._metadata(item['metadata'], approved['metadata'], 'Initializer policy')
                for field in ('name', 'namespace', 'labels', 'annotations'):
                    initialization._exact(item['metadata'].get(field), approved['metadata'][field],
                                          'Initializer policy ' + field)
                _require(item.get('apiVersion') == approved['apiVersion'] and item['metadata'].get('ownerReferences') in (None, [])
                         and item['metadata'].get('finalizers') in (None, []), 'Initializer policy ownership drifted.')
                spec = dict(item['spec'])
                for field in ('ingress', 'egress'):
                    if approved['spec'].get(field) == []: spec.setdefault(field, [])
                initialization._exact(spec, approved['spec'], 'Initializer policy')
        support_pods = [p for p in remainder if p['kind'] == 'Pod']
        endpoints = identity.validate_startup_hotel_endpoints(slices, [s for s in remainder if s['kind'] == 'Service'],
                                                            support_pods, applications, allow_pending=True)
        consul_execution = dict(next(p for p in normalized['support_pods'] if p['component'] == 'consul'))
        consul_pod = next(p for p in support_pods if p['metadata']['uid'] == consul_execution['pod_uid'])
        consul_execution['pod_ip'] = consul_pod['status']['podIP']
        # Wire routing assertion is exact and checked against independently
        # support-validated execution, not a caller-supplied HTTP URL.
        binding = {k: consul_execution[k] for k in ('component', 'role', 'pod_name', 'pod_uid', 'pod_ip', 'node_name', 'container_id', 'image', 'image_id')}
        initialization._exact(document['consul_execution'], binding, 'Consul request execution binding')
        released = tuple(c for c, p in applications.items() if p['released'])
        running = sorted(c for c, p in applications.items() if p['state'] == 'running')
        consul = identity.validate_startup_hotel_consul_identity(document['consul'], plan, applications, released,
                                                               binding, allow_pending=True)
        pending = {'endpoints': sorted(c for c in released if endpoints[c]['publication_pending']),
                   'consul': consul['publication_pending']}
        confirmed = {'endpoints': sorted(c for c in running if not endpoints[c]['publication_pending']),
                     'consul': sorted(consul['backend_registrations'])}
        previous_confirmed = (expected_identity or {}).get('publication_confirmed', {})
        for channel in ('endpoints', 'consul'):
            _require(set(previous_confirmed.get(channel, [])) <= set(confirmed[channel]),
                     'Previously confirmed startup publication regressed.')
        objects = sorted([{'kind': p['kind'], 'name': p['metadata']['name'],
                           'namespace': p['metadata'].get('namespace'), 'uid': p['metadata']['uid']} for p in items],
                         key=lambda p: (p['kind'], p['namespace'] or '', p['name']))
        uids = [p['uid'] for p in objects] + [system_uid]
        _require(len(uids) == len(set(uids)), 'Kubernetes objects alias UIDs.')
        stable = {'objects': objects, 'support_containers': normalized['support_pods'], 'nodes': normalized['nodes'],
                  'pod_pinning_projection': POD_PINNING_PROJECTION,
                  'support_endpoints': {name: endpoint for name, endpoint in endpoints.items()
                                        if name not in stage.metadata()['application_components']},
                  'namespace_uid': normalized['namespace_uid'], 'kube_system_namespace_uid': system_uid,
                  'server_ca_sha256': context['server_ca_sha256'], 'admission_mutators': mutators}
        if expected_identity and 'objects' in expected_identity:
            for field in set(stable) - {'objects'}:
                initialization._exact(stable[field], expected_identity[field], 'Startup retained ' + field)
            now = {(p['kind'], p['namespace'], p['name']): p['uid'] for p in objects}
            _require(all(now.get((p['kind'], p['namespace'], p['name'])) == p['uid'] for p in expected_identity['objects']),
                     'Retained Kubernetes object disappeared or changed UID.')
        stable.update(startup_pods=applications, execution_anchor=_execution_anchor(applications),
                      publication_confirmed=confirmed, raw_output_bytes=raw_bytes)
        return {'startup_execution_observed': False, 'plan_fingerprint': plan.fingerprint,
                'startup_pods': applications, 'initialization_pods': initializers, 'endpoints': endpoints,
                'released_components': sorted(released), 'running_components': running,
                'publication_pending': pending, 'publication_confirmed': confirmed,
                'raw_output_bytes': raw_bytes, 'first_kubernetes_raw_output_bytes': first_raw_bytes,
                'consul': consul, 'identity': stable,
                'first_kubernetes_anchor': json.loads(_canonical((expected_identity or {}).get('execution_anchor')))}
    except ValueError:
        raise
    except (KeyError, TypeError, AttributeError, IndexError, StopIteration) as exc:
        raise CandidateK3sError('Malformed bounded startup observation.') from exc


def validate_startup_hotel_live_observation(kubernetes_observation, cri_output, plan, *, started_components):
    """Join trusted first -> CRI -> final reads, without granting capabilities."""
    validate_startup_hotel_runtime_plan(plan)
    current = kubernetes_observation
    _require(current.get('plan_fingerprint') == plan.fingerprint and current.get('startup_execution_observed') is False,
             'An exact Kubernetes-only intermediate is required.')
    pods = current['startup_pods']
    first = current.get('first_kubernetes_anchor')
    final_anchor = _execution_anchor(pods)
    _require(isinstance(first, dict) and set(first) == set(final_anchor), 'First Kubernetes Pod inventory is missing or changed.')
    for component, original in first.items():
        final = final_anchor[component]
        _require(isinstance(original, dict) and set(original) == set(final), 'First Kubernetes anchor schema drifted.')
        if original['state'] in ('running', 'gated'):
            initialization._exact(original, final, 'First/final pinned Kubernetes execution')
        else:
            _require(original['state'] == 'pending' and final['state'] in ('pending', 'running'),
                     'Pending execution regressed or became unknown.')
            for field, value in original.items():
                if field != 'state' and value not in (None, ''):
                    initialization._exact(value, final[field], 'First/final known Pod identity')
    _require(type(started_components) is tuple and set(started_components) == {c for c, p in pods.items() if p['released']}
             and len(started_components) == len(set(started_components)), 'Started execution inventory is incomplete.')
    wire = _strict_json(cri_output)
    cri_bytes = len(cri_output) if isinstance(cri_output, bytes) else len(cri_output.encode('utf-8'))
    first_bytes = _byte_count(current.get('first_kubernetes_raw_output_bytes'))
    final_bytes = _byte_count(current.get('raw_output_bytes'))
    _require(final_bytes == _byte_count(current['identity'].get('raw_output_bytes')),
             'Current Kubernetes output byte custody drifted.')
    _require(first_bytes + _byte_count(cri_bytes) + final_bytes <= MAX_JSON_BYTES,
             'Cumulative startup observation raw output exceeds its bound.')
    _require(isinstance(wire, dict) and set(wire) == {'schema_version', 'candidate_schema', 'application_node',
                                                   'first_kubernetes_anchor', 'evidence'}
             and type(wire['schema_version']) is int and wire['schema_version'] == 1
             and wire['candidate_schema'] == CRI_OBSERVATION_SCHEMA, 'CRI observation envelope drifted.')
    initialization._exact(wire['first_kubernetes_anchor'], current['first_kubernetes_anchor'], 'CRI first Kubernetes anchor')
    application_node = next(n['name'] for n in current['identity']['nodes'] if n['role'] == 'application')
    _require(wire['application_node'] == application_node, 'CRI observer application routing drifted.')
    # These are the actual parser-retained first observation fields, not an
    # inferred or synthetic CRI execution. Only the component key is restored.
    first_pods = {name: dict(anchor, component=name) for name, anchor in first.items()}
    cri = identity.validate_startup_hotel_cri_evidence(wire['evidence'], plan, first_pods)
    proven = sorted(cri)
    execution_pending = sorted(set(current['running_components']) - set(proven))
    result = dict(current)
    result.update(startup_execution_observed=bool(proven) and not execution_pending, cri=cri,
                  observed_running_components=proven, observed_running_count=len(proven),
                  execution_pending_components=execution_pending)
    return result


def _deadline(value):
    _require(type(value) in (int, float) and math.isfinite(value) and value > 0,
             'A positive finite remaining startup deadline is required.')
    return min(60.0, value)


def _script(context, seconds, operations):
    encoded = base64.b64encode(zlib.compress(_canonical(context).encode(), 9)).decode()
    return ('from __future__ import annotations\n'
            'import base64, hashlib, ipaddress, json, os, re, selectors, signal, subprocess, sys, time, urllib.request, zlib\n'
            'from collections.abc import Mapping\n'
            'from datetime import datetime\n'
            f'MAX_JSON_BYTES={MAX_JSON_BYTES}\nK3S_BINARY={K3S_BINARY!r}\n'
            f'_OWNERSHIP_KEYS={support._OWNERSHIP_KEYS!r}\n'
            + support._REMOTE_HELPERS + '\n'
            + f'CONTEXT=_strict_json(zlib.decompress(base64.b64decode({encoded!r})))\n'
            + f'WINDOW={seconds!r}\n' + _REMOTE_BOUNDS + '\n' + operations)


_REMOTE_BOUNDS = r'''
END = time.monotonic() + WINDOW
USED = 0
def remaining():
    value = END - time.monotonic()
    if value <= 0: raise CandidateK3sError('Startup observation deadline exceeded.')
    return min(15.0, value)
def charge(raw):
    global USED
    USED += len(raw)
    if USED > MAX_JSON_BYTES: raise CandidateK3sError('Startup raw evidence exceeds its bound.')
    return raw
def alarm(signum, frame):
    raise CandidateK3sError('Startup observation window exceeded.')
signal.signal(signal.SIGALRM, alarm)
signal.setitimer(signal.ITIMER_REAL, WINDOW)
def run(arguments):
    limit = remaining()
    signal.setitimer(signal.ITIMER_REAL, limit)
    until = time.monotonic() + limit
    process = subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    raw = bytearray()
    try:
        while selector.get_map():
            left = min(until, END) - time.monotonic()
            if left <= 0: raise CandidateK3sError('Startup request deadline exceeded.')
            events = selector.select(left)
            if not events: raise CandidateK3sError('Startup request timed out.')
            for key, event in events:
                chunk = os.read(key.fileobj.fileno(), min(65536, MAX_JSON_BYTES - USED + 1))
                if not chunk: selector.unregister(key.fileobj)
                else: raw.extend(charge(chunk))
        if process.wait(timeout=max(0.001, min(until, END)-time.monotonic())) != 0:
            raise CandidateK3sError('Startup command failed; no empty inventory inferred.')
        remaining()
        return _strict_json(bytes(raw))
    finally:
        selector.close()
        if process.poll() is None: process.kill()
        process.wait()
        process.stdout.close()
        signal.setitimer(signal.ITIMER_REAL, max(0.000001, END - time.monotonic()))
def kubectl(path):
    return run([K3S_BINARY, 'kubectl', 'get', '--raw', path])
def emit(document):
    remaining()
    output = _canonical(document).encode()
    if len(output) > MAX_JSON_BYTES: raise CandidateK3sError('Startup output exceeds its bound.')
    print(output.decode())
'''


def startup_hotel_cri_observation_command(plan, pods, *, deadline_remaining_seconds):
    validate_startup_hotel_runtime_plan(plan)
    pods = identity._pods(pods, plan)
    running = {c: p for c, p in pods.items() if p['state'] == 'running'}
    nodes = {p['node_name'] for p in running.values()}
    _require(len(nodes) == 1, 'CRI requires a nonempty running inventory on one exact application node.')
    context = {'pods': {c: {k: p[k] for k in ('node_name', 'container_id')} for c, p in running.items()},
               'config_digest': plan.snapshot()['application_config_digest'],
               'anchor': _execution_anchor(pods), 'application_node': next(iter(nodes)),
               'envelope': {'schema_version': 1, 'candidate_schema': identity.CRI_SCHEMA,
                            'run_id': plan.run_id, 'startup_attempt_id': plan.startup_attempt_id,
                            'plan_fingerprint': plan.fingerprint}}
    operations = r'''
document = dict(CONTEXT['envelope'], entries=[])
for component, pod in CONTEXT['pods'].items():
    inspected = run([K3S_BINARY, 'crictl', 'inspect', '-o', 'json', pod['container_id'].removeprefix('containerd://')])
    if inspected['status']['imageId'] != CONTEXT['config_digest']:
        raise CandidateK3sError('CRI executed image config differs from the pinned plan.')
    image = run([K3S_BINARY, 'crictl', 'inspecti', '-o', 'json', CONTEXT['config_digest']])
    document['entries'].append({'component': component, 'node_name': pod['node_name'], 'inspect': inspected, 'inspecti': image})
emit({'schema_version': 1, 'candidate_schema': 'hotel-startup-cri-observation-v1',
      'application_node': CONTEXT['application_node'], 'first_kubernetes_anchor': CONTEXT['anchor'], 'evidence': document})
'''
    # The controller routes this command via its audited application RuntimeHost.
    # K3s --node-name need not equal the OS hostname. Never infer host identity.
    return 'sudo python3 -c ' + shlex.quote(_script(context, _deadline(deadline_remaining_seconds), operations))


def startup_hotel_observation_command(plan, stage, initialization_plan, initialization_stage,
        role_nodes, expected_ca_sha256, *, deadline_remaining_seconds):
    context = _context(plan, stage, initialization_plan, initialization_stage, role_nodes, expected_ca_sha256)
    context['envelope'] = _envelope(plan, stage, initialization_plan, initialization_stage,
                                    context['role_nodes'], context['server_ca_sha256'])
    context['excluded'] = [list(_key(p)) for p in json.loads(stage.canonical_json)['items']
                           + json.loads(initialization_stage.canonical_json)['items']]
    operations = (f'COLLECTIONS={COLLECTIONS!r}\nADMISSION_GROUP={initialization.ADMISSION_GROUP!r}\n'
                  f'MUTATOR_KINDS={initialization.MUTATOR_KINDS!r}\n'
                  + _REMOTE_STARTUP_HELPERS + _REMOTE_KUBERNETES)
    return 'sudo python3 -c ' + shlex.quote(_script(context, _deadline(deadline_remaining_seconds), operations))


_REMOTE_KUBERNETES = r'''
with open('/var/lib/rancher/k3s/server/tls/server-ca.crt', 'rb') as certificate:
    ca_bytes = certificate.read(65537)
if len(ca_bytes) > 65536: raise CandidateK3sError('Startup server CA exceeds its bound.')
ca = 'sha256:' + hashlib.sha256(ca_bytes).hexdigest()
if ca != CONTEXT['server_ca_sha256']: raise CandidateK3sError('Startup server CA changed.')
document = dict(CONTEXT['envelope'], discovery={}, collections={})
for group in sorted({c[0] for c in COLLECTIONS}):
    document['discovery'][group] = kubectl('/api/v1' if group == 'v1' else '/apis/' + group)
for group, resource, kind, namespaced in COLLECTIONS:
    if kind == 'Pod': continue
    path = _collection_path(group, resource, namespaced, CONTEXT['namespace'])
    document['collections'][path] = kubectl(path)
group = kubectl('/apis/' + ADMISSION_GROUP)
admission = {'api_group': group, 'discovery': {}, 'inventories': {}}
for version in _admission_versions(group):
    group_version = ADMISSION_GROUP + '/' + version
    resources = kubectl('/apis/' + group_version)
    admission['discovery'][group_version] = resources
    for resource in resources['resources']:
        if resource['name'] in MUTATOR_KINDS:
            path = group_version + '/' + resource['name']
            admission['inventories'][path] = kubectl('/apis/' + path)
_admission_mutator_absence(admission)
document['admission_mutators'] = admission
# A complete first Pods list is needed to bind the exact Consul execution.
pods_path = '/api/v1/namespaces/' + CONTEXT['namespace'] + '/pods'
document['collections'][pods_path] = kubectl(pods_path)
items, system_uid = _collections(document, CONTEXT)
excluded = {tuple(k) for k in CONTEXT['excluded']}
support_items = [p for p in items if _key(p) not in excluded and p['kind'] != 'EndpointSlice']
validated = _checked_inventory({'apiVersion': 'v1', 'kind': 'List', 'items': support_items}, CONTEXT, complete=True)
_services(support_items, CONTEXT)
execution = next(p for p in validated['support_pods'] if p['component'] == 'consul')
pod = next(p for p in support_items if p['kind'] == 'Pod' and p['metadata']['uid'] == execution['pod_uid'])
execution['pod_ip'] = pod['status']['podIP']
address = ipaddress.ip_address(execution['pod_ip'])
if address not in ipaddress.ip_network('10.42.0.0/16'): raise CandidateK3sError('Consul HTTP Pod address drifted.')
binding = {k: execution[k] for k in ('component', 'role', 'pod_name', 'pod_uid', 'pod_ip', 'node_name', 'container_id', 'image', 'image_id')}
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs): return None
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
def consul(path):
    request = urllib.request.Request('http://' + str(address) + ':8500/v1/' + path,
                                     headers={'Accept': 'application/json', 'Cache-Control': 'no-cache'})
    limit = remaining()
    signal.setitimer(signal.ITIMER_REAL, limit)
    try:
        with opener.open(request, timeout=limit) as response:
            headers_raw = str(response.headers).encode('utf-8')
            if len(headers_raw) > 16384: raise CandidateK3sError('Consul response headers exceed their bound.')
            charge(headers_raw)
            if any(len(response.headers.get_all(name, [])) > 1 for name in
                   ('Content-Type', 'Content-Length', 'X-Consul-Results-Filtered-By-ACLs')):
                raise CandidateK3sError('Consul response repeats a completeness header.')
            if (response.status != 200 or response.headers.get_content_type() != 'application/json'
                or response.headers.get('X-Consul-Results-Filtered-By-ACLs', 'false').lower() != 'false'):
                raise CandidateK3sError('Consul request is partial, redirected or invalid.')
            raw = response.read(MAX_JSON_BYTES - USED + 1)
            charge(raw)
            remaining()
            return _strict_json(raw)
    finally:
        signal.setitimer(signal.ITIMER_REAL, max(0.000001, END - time.monotonic()))
agent, catalog = consul('agent/services'), consul('catalog/services')
entries = {}
for name in catalog:
    if not re.fullmatch(r'(?:consul|srv-(?:attractions|geo|profile|rate|recommendation|reservation|review|search|user))', name):
        raise CandidateK3sError('Unexpected Consul catalog service.')
    entries[name] = consul('catalog/service/' + name)
current = kubectl('/api/v1/namespaces/' + CONTEXT['namespace'] + '/pods/' + execution['pod_name'])
if (current['metadata']['uid'] != pod['metadata']['uid'] or _canonical(current['spec']) != _canonical(pod['spec'])
    or current['status']['podIP'] != str(address)
    or _canonical(current['status']['containerStatuses']) != _canonical(pod['status']['containerStatuses'])):
    raise CandidateK3sError('Consul execution changed during collection.')
document['consul_execution'] = binding
document['consul'] = {k: CONTEXT['envelope'][k] for k in ('run_id', 'startup_attempt_id', 'plan_fingerprint')}
document['consul'].update(schema_version=1, candidate_schema='hotel-startup-consul-evidence-v1',
                         agent_services=agent, catalog_services=catalog, catalog_entries=entries)
# Publication reads precede the final complete Pods read. This reduces the
# ordinary publication-before-Pod-status race but is not an atomic snapshot.
document['collections'][pods_path] = kubectl(pods_path)
emit(document)
'''


# Match the support boundary's capture-on-import rule: a later checkout/edit
# must not cause stale function line numbers to splice different remote code.
_REMOTE_STARTUP_HELPERS = '\n'.join(inspect.getsource(function) for function in (
    _require, _collection_path, _complete_list, _associated_pv, _collections, _key, _services,
    initialization._admission_versions, initialization._admission_mutator_absence,
))
