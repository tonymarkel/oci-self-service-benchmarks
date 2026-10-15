"""Synthetic live-controller protocol tests; never cloud qualification."""

import copy
from contextlib import redirect_stdout
import hashlib
import io
import json
import shlex
from types import SimpleNamespace
import unittest
from unittest.mock import mock_open, patch

from app import deathstarbench_hotel_live_k3s as live
from app.deathstarbench_hotel_initialization_stage import build_guarded_hotel_initialization_stage
from tests import test_deathstarbench_candidate_k3s as support_fixtures
from tests import test_deathstarbench_hotel_runtime as runtime_fixtures


ROLES = support_fixtures.ROLE_NODES
CA = 'sha256:' + '2' * 64


def admission_evidence(*, policies=True):
    group = live.ADMISSION_GROUP
    versions = ['v1', 'v1beta1'] if policies else ['v1']
    result = {'api_group': {'apiVersion': 'v1', 'kind': 'APIGroup', 'name': group,
                           'versions': [{'version': version, 'groupVersion': group + '/' + version} for version in versions],
                           'preferredVersion': {'version': 'v1', 'groupVersion': group + '/v1'}},
              'discovery': {}, 'inventories': {}}
    for version in versions:
        group_version = group + '/' + version
        names = ['mutatingwebhookconfigurations'] if version == 'v1' else ['mutatingadmissionpolicies', 'mutatingadmissionpolicybindings']
        result['discovery'][group_version] = {
            'apiVersion': 'v1', 'kind': 'APIResourceList', 'groupVersion': group_version,
            'resources': [{'name': name, 'kind': live.MUTATOR_KINDS[name], 'namespaced': False,
                           'singularName': '', 'verbs': ['get', 'list', 'watch', 'create', 'update', 'patch', 'delete']}
                          for name in names]}
        for name in names:
            result['inventories'][group_version + '/' + name] = {
                'apiVersion': group_version, 'kind': live.MUTATOR_KINDS[name] + 'List',
                'metadata': {'resourceVersion': '123'}, 'items': []}
    return result


def inventory(plan, stage, *, created=False):
    result = support_fixtures.live_inventory(plan.support_stage)
    services = {item['metadata']['name']: item for item in result['items'] if item['kind'] == 'Service'}
    pods = [item for item in result['items'] if item['kind'] == 'Pod']
    for index, pod in enumerate(pods):
        name = pod['metadata']['labels']['app.kubernetes.io/component']
        address = '10.42.1.' + str(index + 1)
        pod['status'].update(podIP=address, podIPs=[{'ip': address}])
        service = services[name]
        slice_name = name + '-abcde'
        result['items'].append({
            'apiVersion': 'discovery.k8s.io/v1', 'kind': 'EndpointSlice', 'addressType': 'IPv4',
            'metadata': {'name': slice_name, 'namespace': stage.namespace,
                         'uid': support_fixtures.identity('EndpointSlice', slice_name),
                         'labels': {'kubernetes.io/service-name': name,
                                    'endpointslice.kubernetes.io/managed-by': 'endpointslice-controller.k8s.io'},
                         'ownerReferences': [{'apiVersion': 'v1', 'kind': 'Service', 'name': name,
                                              'uid': service['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}]},
            'ports': sorted([{'name': port.get('name', ''), 'port': port['targetPort'], 'protocol': port.get('protocol', 'TCP')}
                             for port in service['spec']['ports']], key=lambda port: port['name']),
            'endpoints': [{'addresses': [address], 'conditions': {'ready': True, 'serving': True, 'terminating': False},
                           'nodeName': pod['spec']['nodeName'], 'targetRef': {'kind': 'Pod', 'namespace': stage.namespace,
                                                                          'name': pod['metadata']['name'], 'uid': pod['metadata']['uid']}}],
        })
    support_names = {pod['metadata']['labels']['app.kubernetes.io/component'] for pod in pods}
    for index, name in enumerate(sorted(set(services) - support_names)):
        service = services[name]
        slice_name = name + '-abcde'
        # The real controller creates all ten zero-Pod placeholders. Repeated
        # fields may return as null after the API's protobuf storage round trip.
        empty = None if index % 2 else []
        result['items'].append({
            'apiVersion': 'discovery.k8s.io/v1', 'kind': 'EndpointSlice', 'addressType': 'IPv4',
            'metadata': {'name': slice_name, 'namespace': stage.namespace,
                         'uid': support_fixtures.identity('EndpointSlice', slice_name),
                         'labels': {'kubernetes.io/service-name': name,
                                    'endpointslice.kubernetes.io/managed-by': 'endpointslice-controller.k8s.io'},
                         'ownerReferences': [{'apiVersion': 'v1', 'kind': 'Service', 'name': name,
                                              'uid': service['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}]},
            'ports': copy.deepcopy(empty), 'endpoints': copy.deepcopy(empty),
        })
    if created:
        for item in json.loads(stage.canonical_json)['items']:
            item['metadata']['uid'] = support_fixtures.identity(item['kind'], item['metadata']['name'])
            if item['kind'] == 'Pod':
                item['metadata']['resourceVersion'] = '123'
                item['spec'].update(serviceAccountName='default', serviceAccount='default', priority=0,
                                    preemptionPolicy='PreemptLowerPriority', tolerations=[
                                        {'key': 'node.kubernetes.io/' + key, 'operator': 'Exists',
                                         'effect': 'NoExecute', 'tolerationSeconds': 300}
                                        for key in ('not-ready', 'unreachable')])
                item['spec']['containers'][0].update(terminationMessagePath='/dev/termination-log', terminationMessagePolicy='File')
                item['status'] = {'phase': 'Pending', 'conditions': [{'type': 'PodScheduled', 'status': 'False', 'reason': 'SchedulingGated'}]}
            result['items'].append(item)
    return result


def endpoint_slice(data, service_name):
    return next(item for item in data['items'] if item['kind'] == 'EndpointSlice'
                and item['metadata']['labels']['kubernetes.io/service-name'] == service_name)


def document(plan, stage, data, *, logs=None):
    return {'schema_version': 1, 'candidate_schema': live.SCHEMA, 'candidate_only': True,
            'released': False, 'application_start_allowed': False,
            'run_id': plan.run_id, 'plan_fingerprint': plan.fingerprint, 'stage_fingerprint': stage.fingerprint,
            'server_ca_sha256': CA, 'role_nodes': ROLES, 'inventory': data, 'logs': logs or {},
            'kube_system_namespace_uid': support_fixtures.identity('Namespace', 'kube-system'),
            'admission_mutators': admission_evidence()}


def initialization_pod(data, mode):
    return next(item for item in data['items'] if item['kind'] == 'Pod'
                and item['spec']['containers'][0].get('args') == [mode]
                and item['spec']['containers'][0]['name'] == 'hotel-initialize')


def run_pod(data, mode, *, state='terminated', exit_code=0):
    pod = initialization_pod(data, mode)
    pod['spec'].pop('schedulingGates')
    pod['spec']['nodeName'] = ROLES['application']
    image = pod['spec']['containers'][0]['image']
    cid = 'containerd://' + hashlib.sha256(mode.encode()).hexdigest()
    execution = {'startedAt': '2026-10-15T15:00:00Z'}
    if state == 'terminated':
        execution.update(finishedAt='2026-10-15T15:00:15Z', exitCode=exit_code, containerID=cid,
                         reason='Completed' if exit_code == 0 else 'Error', signal=0)
    pod['status'] = {'phase': ('Succeeded' if exit_code == 0 else 'Failed') if state == 'terminated' else 'Running',
                     'containerStatuses': [{'name': 'hotel-initialize', 'restartCount': 0,
                                            'image': image, 'imageID': 'docker-pullable://' + image,
                                            'containerID': cid, 'lastState': {}, 'state': {state: execution}}]}
    return pod


class GuardedHotelLiveK3sTests(unittest.TestCase):
    def setUp(self):
        helper = runtime_fixtures.GuardedHotelRuntimeTests()
        self.addCleanup(helper.doCleanups)
        helper.setUp()
        self.plan = helper.plan()
        self.stage = build_guarded_hotel_initialization_stage(self.plan)

    def parse(self, data, *, expected=None, logs=None):
        return live.parse_guarded_hotel_live_observation(json.dumps(document(self.plan, self.stage, data, logs=logs)),
            self.plan, self.stage, ROLES, CA, expected_identity=expected)

    def test_full_support_graph_before_create_and_exact_gated_additions(self):
        before = self.parse(inventory(self.plan, self.stage))
        self.assertEqual(before['pods'], {})
        after = self.parse(inventory(self.plan, self.stage, created=True), expected=before['identity'])
        self.assertEqual(set(after['pods']), {'initialize-empty', 'verify'})
        self.assertTrue(all(pod['state'] == 'gated' for pod in after['pods'].values()))
        self.assertEqual(len(after['databases']), 8)
        for database in after['databases']:
            self.assertTrue(database['pod_ip'].startswith('10.42.'))
            self.assertIn('endpoint_slice_uid', database)
        self.assertEqual(len(after['support_inventory']['items']), len(before['support_inventory']['items']))
        self.assertEqual(sum(item['pod_ip'] is None for item in after['identity']['endpoints'].values()), 10)

    def test_paused_placeholders_normalize_only_empty_wire_representations(self):
        missing = object()
        for created in (False, True):
            data = inventory(self.plan, self.stage, created=created)
            baseline = self.parse(data)['identity']
            for ports in (None, [], missing):
                for endpoints in (None, [], missing):
                    with self.subTest(created=created, ports=ports, endpoints=endpoints):
                        current = copy.deepcopy(data)
                        selected = endpoint_slice(current, 'frontend')
                        for field, value in (('ports', ports), ('endpoints', endpoints)):
                            if value is missing:
                                selected.pop(field)
                            else:
                                selected[field] = copy.deepcopy(value)
                        self.assertEqual(self.parse(current, expected=baseline)['identity'], baseline)

    def test_paused_placeholders_reject_nonempty_or_wrong_typed_fields(self):
        for field in ('ports', 'endpoints'):
            for value in (False, 0, '', {}, [None], [{'unexpected': 'data'}]):
                with self.subTest(field=field, value=value):
                    data = inventory(self.plan, self.stage)
                    endpoint_slice(data, 'frontend')[field] = value
                    with self.assertRaisesRegex(live.CandidateK3sError, 'frontend'):
                        self.parse(data)
        data = inventory(self.plan, self.stage)
        service = next(item for item in data['items'] if item['kind'] == 'Service' and item['metadata']['name'] == 'frontend')
        endpoint_slice(data, 'frontend')['ports'] = [
            {'name': port['name'], 'port': port['targetPort'], 'protocol': port['protocol']}
            for port in service['spec']['ports']]
        with self.assertRaisesRegex(live.CandidateK3sError, 'frontend'):
            self.parse(data)

    def test_paused_placeholder_authority_and_uid_continuity_remain_strict(self):
        original = inventory(self.plan, self.stage)
        baseline = self.parse(original)['identity']
        for mutation in ('owner', 'manager', 'namespace', 'duplicate', 'sliceuid'):
            with self.subTest(mutation=mutation):
                data = copy.deepcopy(original)
                selected = endpoint_slice(data, 'frontend')
                if mutation == 'owner':
                    selected['metadata']['ownerReferences'][0]['uid'] = support_fixtures.identity('Service', 'wrong')
                elif mutation == 'manager':
                    selected['metadata']['labels']['endpointslice.kubernetes.io/managed-by'] = 'manual'
                elif mutation == 'namespace':
                    selected['metadata']['namespace'] = 'default'
                elif mutation == 'duplicate':
                    data['items'].append(copy.deepcopy(selected))
                else:
                    selected['metadata']['uid'] = support_fixtures.identity('EndpointSlice', 'replacement')
                with self.assertRaises(live.CandidateK3sError):
                    self.parse(data, expected=baseline)

    def test_active_multiport_order_is_semantically_equivalent(self):
        data = inventory(self.plan, self.stage, created=True)
        selected = endpoint_slice(data, 'jaeger')
        self.assertEqual(len(selected['ports']), 2)
        baseline = self.parse(data)['identity']
        selected['ports'].reverse()
        self.assertEqual(self.parse(data, expected=baseline)['identity'], baseline)

    def test_active_ports_reject_missing_extra_duplicate_or_wrong_typed_data(self):
        mutations = [
            lambda s: s.pop('ports'),
            lambda s: s.update(ports=None),
            lambda s: s.update(ports=[]),
            lambda s: s.update(ports=False),
            lambda s: s['ports'].pop(),
            lambda s: s['ports'].append(copy.deepcopy(s['ports'][0])),
            lambda s: s['ports'].append({'name': 'extra', 'port': 1234, 'protocol': 'TCP'}),
            lambda s: s['ports'][0].update(name='wrong'),
            lambda s: s['ports'][0].update(name=None),
            lambda s: s['ports'][0].update(port=1234),
            lambda s: s['ports'][0].update(port=True),
            lambda s: s['ports'][0].update(port='16686'),
            lambda s: s['ports'][0].update(port=16686.0),
            lambda s: s['ports'][0].update(port=0),
            lambda s: s['ports'][0].update(port=65536),
            lambda s: s['ports'][0].update(protocol='UDP'),
            lambda s: s['ports'][0].update(protocol=None),
            lambda s: s['ports'][0].pop('name'),
            lambda s: s['ports'][0].update(appProtocol='unknown'),
            lambda s: s['ports'].__setitem__(0, None),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                data = inventory(self.plan, self.stage)
                mutate(endpoint_slice(data, 'jaeger'))
                with self.assertRaisesRegex(live.CandidateK3sError, 'jaeger'):
                    self.parse(data)

    def test_active_support_endpoints_do_not_inherit_empty_placeholder_tolerance(self):
        for value in (None, [], False, {}, ''):
            with self.subTest(value=value):
                data = inventory(self.plan, self.stage)
                endpoint_slice(data, 'mongodb-geo')['endpoints'] = value
                with self.assertRaises(live.CandidateK3sError):
                    self.parse(data)

    def test_running_and_terminal_one_shot_state_and_bounded_logs(self):
        data = inventory(self.plan, self.stage, created=True)
        baseline = self.parse(data)['identity']
        run_pod(data, 'initialize-empty', state='running')
        self.assertEqual(self.parse(data, expected=baseline)['pods']['initialize-empty']['state'], 'running')
        run_pod_copy = initialization_pod(data, 'initialize-empty')
        run_pod_copy['spec']['schedulingGates'] = [{'name': live.SCHEDULING_GATE}]
        run_pod(data, 'initialize-empty')
        after = self.parse(data, expected=baseline, logs={'initialize-empty': '{}\n'})
        self.assertEqual(after['pods']['initialize-empty']['exit_code'], 0)
        self.assertEqual(after['logs']['initialize-empty'], '{}\n')

    def test_failed_exit_is_observed_not_success_or_retry(self):
        data = inventory(self.plan, self.stage, created=True)
        run_pod(data, 'initialize-empty', exit_code=2)
        self.assertEqual(self.parse(data)['pods']['initialize-empty']['exit_code'], 2)

    def test_terminated_container_with_nonterminal_pod_phase_is_pending_not_success(self):
        for mode in ('initialize-empty', 'verify'):
            for phase in ('Pending', 'Running'):
                for exit_code in (0, 2):
                    with self.subTest(mode=mode, phase=phase, exit_code=exit_code):
                        data = inventory(self.plan, self.stage, created=True)
                        baseline = self.parse(data)['identity']
                        pod = run_pod(data, mode, exit_code=exit_code)
                        pod['status']['phase'] = phase
                        observed = self.parse(data, expected=baseline)['pods'][mode]
                        self.assertEqual(observed['state'], 'pending')
                        self.assertIsNone(observed['exit_code'])
                        self.assertEqual(observed['pod'], pod)
                        self.assertEqual(observed['container_id'], pod['status']['containerStatuses'][0]['containerID'])
                        self.assertEqual(observed['image_id'], pod['status']['containerStatuses'][0]['imageID'])
                        pod['status']['phase'] = 'Succeeded' if exit_code == 0 else 'Failed'
                        completed = self.parse(data, expected=baseline)['pods'][mode]
                        self.assertEqual(completed['state'], 'terminated')
                        self.assertEqual(completed['exit_code'], exit_code)
                        self.assertEqual(completed['uid'], observed['uid'])

    def test_terminal_phase_contradictions_remain_hard_failures_with_safe_diagnostics(self):
        for phase, exit_code, diagnostic in (
                ('Succeeded', 2, 'Succeeded'), ('Failed', 0, 'Failed'),
                ('Unknown', 0, 'Unknown'), (None, 0, '<missing>'),
                (False, 0, '<invalid>'), ([], 0, '<invalid>'),
                ('untrusted-secret-message', 0, '<invalid>')):
            with self.subTest(phase=phase, exit_code=exit_code):
                data = inventory(self.plan, self.stage, created=True)
                pod = run_pod(data, 'initialize-empty', exit_code=exit_code)
                pod['status']['phase'] = phase
                with self.assertRaises(live.CandidateK3sError) as caught:
                    self.parse(data)
                message = str(caught.exception)
                self.assertIn('Pod/' + pod['metadata']['name'], message)
                self.assertIn('phase=' + diagnostic, message)
                self.assertIn('exit_code=' + str(exit_code), message)
                self.assertNotIn('untrusted-secret-message', message)

    def test_nonterminal_phase_does_not_bypass_any_termination_validation(self):
        baseline = self.parse(inventory(self.plan, self.stage, created=True))['identity']
        mutations = [
            lambda p, c, t: p['metadata'].update(uid=support_fixtures.identity('Pod', 'replacement')),
            lambda p, c, t: p['spec'].update(nodeName=ROLES['database']),
            lambda p, c, t: c.update(imageID='wrong-image'),
            lambda p, c, t: c.update(restartCount=1),
            lambda p, c, t: c.update(lastState={'terminated': {'exitCode': 0}}),
            lambda p, c, t: c.update(containerID='containerd://bad'),
            lambda p, c, t: t.update(containerID='containerd://' + '9' * 64),
            lambda p, c, t: t.update(startedAt='invalid'),
            lambda p, c, t: t.update(finishedAt='invalid'),
            lambda p, c, t: t.update(finishedAt='2026-10-15T14:59:59Z'),
            lambda p, c, t: t.update(exitCode=True),
            lambda p, c, t: t.update(signal=9),
            lambda p, c, t: t.update(signal=True),
            lambda p, c, t: t.update(reason='Error'),
        ]
        for phase in ('Pending', 'Running'):
            for index, mutate in enumerate(mutations):
                with self.subTest(phase=phase, mutation=index):
                    data = inventory(self.plan, self.stage, created=True)
                    pod = run_pod(data, 'initialize-empty')
                    pod['status']['phase'] = phase
                    container = pod['status']['containerStatuses'][0]
                    mutate(pod, container, container['state']['terminated'])
                    with self.assertRaises(live.CandidateK3sError):
                        self.parse(data, expected=baseline)

    def test_unsafe_admission_mutations_rejected(self):
        mutations = [
            lambda p: p['spec'].update(nodeName=ROLES['application']),
            lambda p: p['spec'].update(hostNetwork=True),
            lambda p: p['spec'].update(volumes=[{'name': 'host', 'hostPath': {'path': '/'}}]),
            lambda p: p['spec'].update(automountServiceAccountToken=True),
            lambda p: p['spec'].update(initContainers=[{'name': 'evil', 'image': 'bad'}]),
            lambda p: p['spec'].update(ephemeralContainers=[{'name': 'evil', 'image': 'bad'}]),
            lambda p: p['spec'].update(serviceAccountName='privileged'),
            lambda p: p['spec']['containers'].append(copy.deepcopy(p['spec']['containers'][0])),
            lambda p: p['spec']['containers'][0].update(envFrom=[{'secretRef': {'name': 'secret'}}]),
            lambda p: p['spec']['containers'][0]['env'][0].update(valueFrom={'secretKeyRef': {'name': 'x', 'key': 'y'}}),
            lambda p: p['spec']['containers'][0]['securityContext'].update(privileged=True),
            lambda p: p['spec']['securityContext'].update(sysctls=[{'name': 'x', 'value': '1'}]),
            lambda p: p['spec']['containers'][0]['resources']['limits'].update(cpu='2'),
            lambda p: p['spec'].update(nodeSelector={'kubernetes.io/arch': 'arm64'}),
            lambda p: p['metadata'].update(ownerReferences=[{'kind': 'Job'}]),
            lambda p: p['metadata']['annotations'].update(extra='unknown'),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                data = inventory(self.plan, self.stage, created=True)
                mutate(initialization_pod(data, 'initialize-empty'))
                with self.assertRaises(live.CandidateK3sError):
                    self.parse(data)

    def test_omitted_false_host_defaults_preserve_exact_admitted_patch_spec(self):
        data = inventory(self.plan, self.stage, created=True)
        pod = initialization_pod(data, 'initialize-empty')
        for field in ('hostNetwork', 'hostPID', 'hostIPC', 'shareProcessNamespace'):
            pod['spec'].pop(field)
        observed = self.parse(data)['pods']['initialize-empty']
        self.assertEqual(observed['spec'], pod['spec'])
        self.assertNotIn('hostNetwork', observed['spec'])
        pod['spec']['hostNetwork'] = True
        with self.assertRaises(live.CandidateK3sError):
            self.parse(data)

    def test_missing_partial_duplicate_or_extra_objects_rejected(self):
        for mode in ('partial', 'duplicate', 'unknown'):
            with self.subTest(mode=mode):
                data = inventory(self.plan, self.stage, created=True)
                if mode == 'partial':
                    data['items'].remove(initialization_pod(data, 'verify'))
                elif mode == 'duplicate':
                    data['items'].append(copy.deepcopy(initialization_pod(data, 'verify')))
                else:
                    extra = copy.deepcopy(initialization_pod(data, 'verify'))
                    extra['metadata']['name'] = 'unexpected'
                    data['items'].append(extra)
                with self.assertRaises(live.CandidateK3sError):
                    self.parse(data)

    def test_endpoint_service_pod_graph_mutations_rejected(self):
        mutations = [
            lambda s: s['metadata']['ownerReferences'][0].update(uid=support_fixtures.identity('Service', 'wrong')),
            lambda s: s['endpoints'][0]['targetRef'].update(uid=support_fixtures.identity('Pod', 'wrong')),
            lambda s: s['endpoints'][0]['targetRef'].update(namespace='default'),
            lambda s: s['endpoints'][0].update(addresses=['10.42.99.99']),
            lambda s: s['endpoints'][0].update(nodeName=ROLES['application']),
            lambda s: s['endpoints'][0]['conditions'].update(ready=False),
            lambda s: s['endpoints'][0]['conditions'].update(terminating=True),
            lambda s: s['ports'][0].update(port=1234),
            lambda s: s['metadata']['labels'].update({'endpointslice.kubernetes.io/managed-by': 'manual'}),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                data = inventory(self.plan, self.stage)
                selected = next(item for item in data['items'] if item['kind'] == 'EndpointSlice' and item['metadata']['labels']['kubernetes.io/service-name'] == 'mongodb-geo')
                mutate(selected)
                with self.assertRaises(live.CandidateK3sError):
                    self.parse(data)

    def test_support_restart_application_start_and_replaced_identity_rejected(self):
        data = inventory(self.plan, self.stage, created=True)
        expected = self.parse(data)['identity']
        for mutation in ('restart', 'application', 'poduid', 'endpointuid'):
            with self.subTest(mutation=mutation):
                current = copy.deepcopy(data)
                if mutation == 'restart':
                    next(item for item in current['items'] if item['kind'] == 'Pod' and 'schedulingGates' not in item['spec'])['status']['containerStatuses'][0]['restartCount'] = 1
                elif mutation == 'application':
                    next(item for item in current['items'] if item['kind'] == 'Deployment' and item['spec']['replicas'] == 0)['spec']['replicas'] = 1
                elif mutation == 'poduid':
                    initialization_pod(current, 'verify')['metadata']['uid'] = support_fixtures.identity('Pod', 'replacement')
                else:
                    next(item for item in current['items'] if item['kind'] == 'EndpointSlice')['metadata']['uid'] = support_fixtures.identity('EndpointSlice', 'replacement')
                with self.assertRaises(live.CandidateK3sError):
                    self.parse(current, expected=expected)

    def test_runtime_status_restart_image_node_and_container_id_rejected(self):
        for mutation in ('restart', 'image', 'node', 'cid', 'laststate'):
            with self.subTest(mutation=mutation):
                data = inventory(self.plan, self.stage, created=True)
                pod = run_pod(data, 'initialize-empty')
                status = pod['status']['containerStatuses'][0]
                if mutation == 'restart': status['restartCount'] = True
                elif mutation == 'image': status['imageID'] = 'wrong'
                elif mutation == 'node': pod['spec']['nodeName'] = ROLES['database']
                elif mutation == 'cid': status['containerID'] = 'containerd://bad'
                else: status['lastState'] = {'terminated': {'exitCode': 0}}
                with self.assertRaises(live.CandidateK3sError):
                    self.parse(data)

    def test_admission_policy_unserved_only_with_complete_successful_discovery(self):
        original = document(self.plan, self.stage, inventory(self.plan, self.stage))
        original['admission_mutators'] = admission_evidence(policies=False)
        observed = live.parse_guarded_hotel_live_observation(json.dumps(original), self.plan, self.stage, ROLES, CA)
        self.assertEqual(observed['identity']['admission_mutators']['unserved_resources'],
                         ['mutatingadmissionpolicies', 'mutatingadmissionpolicybindings'])
        self.assertEqual(observed['identity']['admission_mutators']['mutator_count'], 0)
        for mutation in ('missing-version', 'missing-list', 'unknown-mutator', 'active-webhook', 'active-policy', 'active-binding'):
            with self.subTest(mutation=mutation):
                current = copy.deepcopy(original)
                evidence = current['admission_mutators'] = admission_evidence()
                if mutation == 'missing-version':
                    evidence['discovery'].pop(live.ADMISSION_GROUP + '/v1beta1')
                elif mutation == 'missing-list':
                    evidence['inventories'].pop(live.ADMISSION_GROUP + '/v1beta1/mutatingadmissionpolicies')
                elif mutation == 'unknown-mutator':
                    evidence['discovery'][live.ADMISSION_GROUP + '/v1']['resources'].append(
                        {'name': 'mutatingfuturepolicies', 'kind': 'FutureMutator', 'namespaced': False, 'verbs': ['list']})
                else:
                    name = {'active-webhook': 'mutatingwebhookconfigurations', 'active-policy': 'mutatingadmissionpolicies',
                            'active-binding': 'mutatingadmissionpolicybindings'}[mutation]
                    selected = next(value for key, value in evidence['inventories'].items() if key.endswith('/' + name))
                    selected['items'] = [{'metadata': {'name': 'hostile'}}]
                with self.assertRaises(live.CandidateK3sError):
                    live.parse_guarded_hotel_live_observation(json.dumps(current), self.plan, self.stage, ROLES, CA)

    def test_empty_mutator_list_revision_changes_do_not_change_stable_identity(self):
        original = document(self.plan, self.stage, inventory(self.plan, self.stage))
        before = live.parse_guarded_hotel_live_observation(json.dumps(original), self.plan, self.stage, ROLES, CA)
        for item in original['admission_mutators']['inventories'].values():
            item['metadata']['resourceVersion'] = '999'
        after = live.parse_guarded_hotel_live_observation(json.dumps(original), self.plan, self.stage, ROLES, CA,
            expected_identity=before['identity'])
        self.assertEqual(after['identity'], before['identity'])
        self.assertNotEqual(after['admission_mutators'], before['admission_mutators'])

    def test_embedded_hostile_mutator_or_forbidden_discovery_prevents_all_writes(self):
        ca_bytes = b'synthetic-certificate-only'
        ca = 'sha256:' + hashlib.sha256(ca_bytes).hexdigest()
        for operation, mutation in (('create', 'webhook'), ('release', 'policy'), ('release', 'binding'), ('create', 'forbidden-discovery')):
            with self.subTest(operation=operation, mutation=mutation):
                data = inventory(self.plan, self.stage, created=operation == 'release')
                original = document(self.plan, self.stage, data)
                original['server_ca_sha256'] = ca
                expected = live.parse_guarded_hotel_live_observation(json.dumps(original), self.plan, self.stage, ROLES, ca)['identity']
                hostile = admission_evidence()
                if mutation != 'forbidden-discovery':
                    name = {'webhook': 'mutatingwebhookconfigurations', 'policy': 'mutatingadmissionpolicies',
                            'binding': 'mutatingadmissionpolicybindings'}[mutation]
                    next(value for key, value in hostile['inventories'].items() if key.endswith('/' + name))['items'] = [{'metadata': {'name': 'hostile'}}]
                calls = []

                def kubectl(arguments, **kwargs):
                    calls.append(arguments)
                    words = arguments[2:]
                    self.assertEqual(words[:2], ['get', '--raw'])
                    path = words[2].removeprefix('/apis/')
                    if mutation == 'forbidden-discovery' and path.endswith('/v1beta1'):
                        return SimpleNamespace(returncode=1, stdout=b'', stderr=b'Forbidden')
                    result = hostile['api_group'] if path == live.ADMISSION_GROUP else (
                        hostile['discovery'][path] if path in hostile['discovery'] else hostile['inventories'][path])
                    return SimpleNamespace(returncode=0, stdout=json.dumps(result).encode(), stderr=b'')

                command = live.guarded_hotel_observation_command(self.plan, self.stage, ROLES, ca,
                    operation=operation, mode='initialize-empty' if operation == 'release' else None, expected_identity=expected)
                with patch('subprocess.run', side_effect=kubectl), patch('builtins.open', mock_open(read_data=ca_bytes)):
                    with self.assertRaisesRegex(ValueError, 'admission mutators|kubectl operation failed'):
                        exec(shlex.split(command)[3], {})
                self.assertTrue(calls)
                self.assertTrue(all(arguments[2:4] == ['get', '--raw'] for arguments in calls))

    def test_command_self_contained_and_exact_one_patch_without_apply_or_scale(self):
        observed = self.parse(inventory(self.plan, self.stage, created=True))
        for operation, mode in (('observe', None), ('create', None), ('release', 'initialize-empty'), ('wait', 'verify')):
            command = live.guarded_hotel_observation_command(self.plan, self.stage, ROLES, CA,
                operation=operation, mode=mode, expected_identity=observed['identity'])
            words = shlex.split(command)
            self.assertEqual(words[:3], ['sudo', 'python3', '-c'])
            compile(words[3], '<guarded-hotel-command>', 'exec')
            self.assertNotIn("'apply'", words[3])
            self.assertNotIn("'scale'", words[3])
            self.assertEqual(words[3].count("'--type=json'"), 1)
            self.assertIn("'path': '/spec'", words[3])
        with self.assertRaises(live.CandidateK3sError):
            live.guarded_hotel_observation_command(self.plan, self.stage, ROLES, CA, operation='release', mode='initialize-empty')

    def _check_embedded_release(self, phase=None):
        data = inventory(self.plan, self.stage, created=True)
        ca_bytes = b'synthetic-certificate-only'
        ca = 'sha256:' + hashlib.sha256(ca_bytes).hexdigest()
        original = document(self.plan, self.stage, data)
        original['server_ca_sha256'] = ca
        expected = live.parse_guarded_hotel_live_observation(json.dumps(original), self.plan, self.stage, ROLES, ca)['identity']
        command = live.guarded_hotel_observation_command(self.plan, self.stage, ROLES, ca,
            operation='release', mode='initialize-empty', expected_identity=expected)
        calls = []

        def kubectl(arguments, **kwargs):
            calls.append(arguments)
            words = arguments[2:]
            if words[:2] == ['get', '--raw']:
                path = words[2].removeprefix('/apis/')
                evidence = original['admission_mutators']
                result = evidence['api_group'] if path == live.ADMISSION_GROUP else (
                    evidence['discovery'][path] if path in evidence['discovery'] else evidence['inventories'][path])
            elif 'patch' in words:
                operations = json.loads(words[words.index('-p') + 1])
                pod = initialization_pod(data, 'initialize-empty')
                tests = {item['path']: item['value'] for item in operations if item['op'] == 'test'}
                self.assertEqual(tests['/metadata/uid'], pod['metadata']['uid'])
                self.assertEqual(tests['/metadata/resourceVersion'], pod['metadata']['resourceVersion'])
                self.assertEqual(tests['/spec'], pod['spec'])
                self.assertEqual(operations[-1], {'op': 'remove', 'path': '/spec/schedulingGates'})
                if phase is None:
                    pod['spec'].pop('schedulingGates')
                else:
                    run_pod(data, 'initialize-empty')
                    pod['status']['phase'] = phase
                result = pod
            elif words[:3] == ['get', 'namespace', 'kube-system']:
                result = {'metadata': {'uid': original['kube_system_namespace_uid']}}
            elif words[:2] == ['get', 'namespace']:
                result = next(item for item in data['items'] if item['kind'] == 'Namespace')
            elif words[:2] == ['get', 'persistentvolumes']:
                result = {'items': [item for item in data['items'] if item['kind'] == 'PersistentVolume']}
            elif words[:2] == ['get', 'nodes']:
                result = {'items': [item for item in data['items'] if item['kind'] == 'Node']}
            elif words[:3] == ['-n', self.stage.namespace, 'get']:
                result = {'items': [item for item in data['items'] if item['kind'] not in ('Namespace', 'PersistentVolume', 'Node')]}
            else:
                self.fail('Unexpected kubectl invocation: ' + repr(words))
            return SimpleNamespace(returncode=0, stdout=json.dumps(result).encode(), stderr=b'')

        output = io.StringIO()
        with patch('subprocess.run', side_effect=kubectl), patch('builtins.open', mock_open(read_data=ca_bytes)), redirect_stdout(output):
            exec(shlex.split(command)[3], {})
        observed = live.parse_guarded_hotel_live_observation(output.getvalue(), self.plan, self.stage, ROLES, ca, expected_identity=expected)
        self.assertEqual(observed['pods']['initialize-empty']['state'], 'pending')
        self.assertIsNone(observed['pods']['initialize-empty']['exit_code'])
        if phase is not None:
            self.assertEqual(observed['pods']['initialize-empty']['pod']['status']['phase'], phase)
            self.assertEqual(observed['pods']['initialize-empty']['pod']['status']['containerStatuses'][0]['state']['terminated']['exitCode'], 0)
        self.assertEqual(sum('patch' in arguments for arguments in calls), 1)

    def test_embedded_release_validates_fresh_graph_and_sends_one_exact_atomic_patch(self):
        self._check_embedded_release()

    def test_embedded_release_accepts_only_pending_evidence_for_terminal_phase_lag(self):
        for phase in ('Pending', 'Running'):
            with self.subTest(phase=phase):
                self._check_embedded_release(phase)

    def test_embedded_wait_for_phase_convergence_is_readonly_and_bounded(self):
        for persistent in (False, True):
            with self.subTest(persistent=persistent):
                data = inventory(self.plan, self.stage, created=True)
                pod = run_pod(data, 'initialize-empty')
                pod['status']['phase'] = 'Running'
                ca_bytes = b'synthetic-certificate-only'
                ca = 'sha256:' + hashlib.sha256(ca_bytes).hexdigest()
                original = document(self.plan, self.stage, data)
                original['server_ca_sha256'] = ca
                expected = live.parse_guarded_hotel_live_observation(
                    json.dumps(original), self.plan, self.stage, ROLES, ca)['identity']
                command = live.guarded_hotel_observation_command(self.plan, self.stage, ROLES, ca,
                    operation='wait', mode='initialize-empty', expected_identity=expected, timeout_seconds=3)
                calls = []
                inventory_reads = []

                def kubectl(arguments, **kwargs):
                    calls.append(arguments)
                    words = arguments[2:]
                    self.assertNotIn('patch', words)
                    self.assertNotIn('create', words)
                    if words[:2] == ['get', '--raw']:
                        path = words[2].removeprefix('/apis/')
                        evidence = original['admission_mutators']
                        result = evidence['api_group'] if path == live.ADMISSION_GROUP else (
                            evidence['discovery'][path] if path in evidence['discovery'] else evidence['inventories'][path])
                    elif words[:3] == ['get', 'namespace', 'kube-system']:
                        result = {'metadata': {'uid': original['kube_system_namespace_uid']}}
                    elif words[:2] == ['get', 'namespace']:
                        result = next(item for item in data['items'] if item['kind'] == 'Namespace')
                    elif words[:2] == ['get', 'persistentvolumes']:
                        result = {'items': [item for item in data['items'] if item['kind'] == 'PersistentVolume']}
                    elif words[:2] == ['get', 'nodes']:
                        result = {'items': [item for item in data['items'] if item['kind'] == 'Node']}
                    elif words[:4] == ['-n', self.stage.namespace, 'get', 'pod']:
                        result = pod
                    elif words[:3] == ['-n', self.stage.namespace, 'logs']:
                        return SimpleNamespace(returncode=0, stdout=b'{}\n', stderr=b'')
                    elif words[:3] == ['-n', self.stage.namespace, 'get']:
                        inventory_reads.append(1)
                        if not persistent and len(inventory_reads) == 3:
                            pod['status']['phase'] = 'Succeeded'
                        result = {'items': [item for item in data['items'] if item['kind'] not in ('Namespace', 'PersistentVolume', 'Node')]}
                    else:
                        self.fail('Unexpected read-only kubectl invocation: ' + repr(words))
                    return SimpleNamespace(returncode=0, stdout=json.dumps(result).encode(), stderr=b'')

                output = io.StringIO()
                with patch('subprocess.run', side_effect=kubectl), patch('builtins.open', mock_open(read_data=ca_bytes)), \
                        patch('time.monotonic', side_effect=[0, 1, 2, 3]), patch('time.sleep') as sleep, redirect_stdout(output):
                    if persistent:
                        with self.assertRaisesRegex(ValueError, 'deadline exceeded; cleanup only'):
                            exec(shlex.split(command)[3], {})
                    else:
                        exec(shlex.split(command)[3], {})
                self.assertEqual(len(inventory_reads), 3)
                self.assertEqual(sleep.call_count, 2)
                self.assertTrue(all('patch' not in arguments and 'create' not in arguments for arguments in calls))
                if persistent:
                    self.assertEqual(output.getvalue(), '')
                else:
                    observed = live.parse_guarded_hotel_live_observation(
                        output.getvalue(), self.plan, self.stage, ROLES, ca, expected_identity=expected)
                    self.assertEqual(observed['pods']['initialize-empty']['state'], 'terminated')
                    self.assertEqual(observed['pods']['initialize-empty']['exit_code'], 0)
