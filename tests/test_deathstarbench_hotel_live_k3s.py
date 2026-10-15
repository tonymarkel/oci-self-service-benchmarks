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
            'ports': [{'name': port.get('name', ''), 'port': port['targetPort'], 'protocol': port.get('protocol', 'TCP')}
                      for port in service['spec']['ports']],
            'endpoints': [{'addresses': [address], 'conditions': {'ready': True, 'serving': True, 'terminating': False},
                           'nodeName': pod['spec']['nodeName'], 'targetRef': {'kind': 'Pod', 'namespace': stage.namespace,
                                                                          'name': pod['metadata']['name'], 'uid': pod['metadata']['uid']}}],
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


def document(plan, stage, data, *, logs=None):
    return {'schema_version': 1, 'candidate_schema': live.SCHEMA, 'candidate_only': True,
            'released': False, 'application_start_allowed': False,
            'run_id': plan.run_id, 'plan_fingerprint': plan.fingerprint, 'stage_fingerprint': stage.fingerprint,
            'server_ca_sha256': CA, 'role_nodes': ROLES, 'inventory': data, 'logs': logs or {},
            'kube_system_namespace_uid': support_fixtures.identity('Namespace', 'kube-system')}


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

    def test_embedded_release_validates_fresh_graph_and_sends_one_exact_atomic_patch(self):
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
            if 'patch' in words:
                operations = json.loads(words[words.index('-p') + 1])
                pod = initialization_pod(data, 'initialize-empty')
                tests = {item['path']: item['value'] for item in operations if item['op'] == 'test'}
                self.assertEqual(tests['/metadata/uid'], pod['metadata']['uid'])
                self.assertEqual(tests['/metadata/resourceVersion'], pod['metadata']['resourceVersion'])
                self.assertEqual(tests['/spec'], pod['spec'])
                self.assertEqual(operations[-1], {'op': 'remove', 'path': '/spec/schedulingGates'})
                pod['spec'].pop('schedulingGates')
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
        self.assertEqual(sum('patch' in arguments for arguments in calls), 1)
