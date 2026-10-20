"""Synthetic live-boundary behavior; no cloud or private run data."""
import copy
from contextlib import redirect_stdout
import io
import json
import shlex
from types import SimpleNamespace
import unittest
from unittest.mock import patch, mock_open

from app import deathstarbench_hotel_initialization_stage as staging
from app import deathstarbench_hotel_runtime as runtime
from tests.test_deathstarbench_candidate_k3s import ROLE_NODES, identity, live_inventory, objects
from tests import test_deathstarbench_hotel_runtime as runtime_fixtures


class HotelLiveTests(unittest.TestCase):
    def setUp(self):
        self.fixture = runtime_fixtures.GuardedHotelRuntimeTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.plan = self.fixture.plan()
        self.stage = staging.build_guarded_hotel_initialization_stage(self.plan)
        self.inventory = live_inventory(self.fixture.support_stage)
        for index, pod in enumerate(objects(self.inventory, 'Pod')):
            pod['status']['podIP'] = '10.42.1.' + str(index + 2)
        for service in objects(self.inventory, 'Service'):
            name = service['metadata']['name']
            if not name.startswith('mongodb-'):
                continue
            pod = next(p for p in objects(self.inventory, 'Pod') if p['metadata']['labels']['app.kubernetes.io/component'] == name)
            self.inventory['items'].append({
                'apiVersion': 'discovery.k8s.io/v1', 'kind': 'EndpointSlice',
                'metadata': {'name': name + '-abc12', 'namespace': self.plan.namespace,
                             'uid': identity('EndpointSlice', name),
                             'labels': {'kubernetes.io/service-name': name, 'endpointslice.kubernetes.io/managed-by': 'endpointslice-controller.k8s.io'},
                             'ownerReferences': [{'apiVersion': 'v1', 'kind': 'Service', 'name': name,
                                                  'uid': service['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}]},
                'addressType': 'IPv4', 'ports': [{'name': '', 'protocol': 'TCP', 'port': 27017}],
                'endpoints': [{'addresses': [pod['status']['podIP']], 'conditions': {'ready': True, 'serving': True, 'terminating': False},
                               'nodeName': ROLE_NODES['database'], 'targetRef': {'kind': 'Pod', 'namespace': self.plan.namespace,
                               'name': pod['metadata']['name'], 'uid': pod['metadata']['uid']}}]})
        for item in json.loads(self.stage.canonical_json)['items']:
            item['metadata'].update(uid=identity(item['kind'], item['metadata']['name']), resourceVersion='5')
            if item['kind'] == 'Pod':
                item['status'] = {'phase': 'Pending'}
            self.inventory['items'].append(item)

    def document(self, include=True):
        inventory = copy.deepcopy(self.inventory)
        if not include:
            names = {p['metadata']['name'] for p in json.loads(self.stage.canonical_json)['items']}
            inventory['items'] = [p for p in inventory['items'] if p['metadata']['name'] not in names]
        return {'schema_version': 1, 'plan_fingerprint': self.plan.fingerprint, 'stage_fingerprint': self.stage.fingerprint,
                'role_nodes': ROLE_NODES, 'include_initialization': include, 'inventory': inventory,
                'server_ca_sha256': 'sha256:' + '2' * 64, 'kube_system_namespace_uid': identity('Namespace', 'kube-system')}

    def parse(self, document=None, include=True):
        from app import deathstarbench_hotel_live as live
        return live.parse_hotel_initialization_observation(json.dumps(document or self.document(include)), self.plan, self.stage, ROLE_NODES, include_initialization=include)

    def test_complete_gated_inventory_and_support_only_observation(self):
        result = self.parse()
        runtime.validate_saved_guarded_hotel_cluster_observation(self.plan, result['cluster_observation'])
        self.assertEqual(set(result['initialization_pods']), {'initialize-empty', 'verify'})
        self.assertEqual(result['initialization_pods']['verify']['phase'], 'Pending')
        self.assertEqual(self.parse(include=False)['initialization_pods'], {})

    def test_unexpected_objects_endpoint_mutation_and_support_restart_fail(self):
        mutations = [
            lambda d: d['inventory']['items'].append({'kind': 'Secret', 'apiVersion': 'v1', 'metadata': {'name': 'foreign', 'namespace': self.plan.namespace, 'uid': identity('Secret', 'foreign')}}),
            lambda d: objects(d['inventory'], 'EndpointSlice')[0]['endpoints'][0]['targetRef'].update(uid=identity('Pod', 'foreign')),
            lambda d: objects(d['inventory'], 'EndpointSlice')[0]['endpoints'][0]['addresses'].append('10.42.99.9'),
            lambda d: objects(d['inventory'], 'EndpointSlice')[0]['ports'][0].update(port=80),
            lambda d: objects(d['inventory'], 'Pod')[0]['status']['containerStatuses'][0].update(restartCount=1),
            lambda d: objects(d['inventory'], 'Pod')[-1]['spec'].update(volumes=[{'name': 'injected', 'emptyDir': {}}]),
            lambda d: objects(d['inventory'], 'Pod')[-1]['spec'].update(nodeName=ROLE_NODES['application']),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                document = self.document()
                mutate(document)
                with self.assertRaises(ValueError):
                    self.parse(document)

    def test_known_admission_defaults_and_terminal_transition(self):
        document = self.document()
        pod = objects(document['inventory'], 'Pod')[-1]
        pod['spec'].update(serviceAccountName='default', serviceAccount='default', priority=0, preemptionPolicy='PreemptLowerPriority',
                           tolerations=[{'key': 'node.kubernetes.io/' + key, 'operator': 'Exists', 'effect': 'NoExecute', 'tolerationSeconds': 300} for key in ('not-ready', 'unreachable')])
        pod['spec']['containers'][0].update(terminationMessagePath='/dev/termination-log', terminationMessagePolicy='File')
        self.parse(document)
        pod['spec'].pop('schedulingGates')
        pod['spec']['nodeName'] = ROLE_NODES['application']
        image = self.plan.application_image
        pod['status'] = {'phase': 'Running', 'containerStatuses': [{'name': 'hotel-initialize', 'image': image, 'imageID': 'docker-pullable://' + image,
                          'containerID': 'containerd://' + 'a' * 64, 'restartCount': 0, 'ready': False, 'started': False, 'lastState': {},
                          'state': {'terminated': {'exitCode': 0, 'reason': 'Completed', 'startedAt': '2026-10-14T12:00:00Z', 'finishedAt': '2026-10-14T12:00:01Z'}}}]}
        pod['status']['containerStatuses'][0]['image'] = self.plan.snapshot()['application_config_digest']
        self.assertEqual(self.parse(document)['initialization_pods']['verify']['exit_code'], 0)
        self.assertEqual(self.parse(document)['initialization_pods']['verify'].get('container_id'), 'containerd://' + 'a' * 64)
        pod['status']['containerStatuses'][0]['state']['terminated']['finishedAt'] = '2026-10-14T12:00:00.100Z'
        self.parse(document)
        pod['status']['phase'] = 'Succeeded'
        self.parse(document)
        for mutation in ({'containerID': 'containerd://' + 'b' * 64}, {'signal': 9}):
            changed = copy.deepcopy(document)
            objects(changed['inventory'], 'Pod')[-1]['status']['containerStatuses'][0]['state']['terminated'].update(mutation)
            with self.assertRaises(ValueError):
                self.parse(changed)
        pod['status']['containerStatuses'][0]['state']['terminated']['exitCode'] = 1
        with self.assertRaises(ValueError):
            self.parse(document)

    def test_known_api_omission_of_false_host_flags_is_safe(self):
        document = self.document()
        for pod in objects(document['inventory'], 'Pod')[-2:]:
            for key in ('hostNetwork', 'hostPID', 'hostIPC'):
                pod['spec'].pop(key)
        result = self.parse(document)
        self.assertNotIn('hostNetwork', result['initialization_pods']['verify']['spec'])

    def test_support_cri_status_can_report_exact_bound_config_digest(self):
        document = self.document()
        configurations = {platform['image']: platform['config_digest']
                          for binding in self.plan.support_plan.artifacts.image_bindings.values()
                          for platform in binding['platforms'].values()}
        for pod in objects(document['inventory'], 'Pod')[:-2]:
            pod['status']['containerStatuses'][0]['image'] = configurations[pod['spec']['containers'][0]['image']]
        self.parse(document)
        objects(document['inventory'], 'Pod')[0]['status']['containerStatuses'][0]['image'] = 'sha256:' + 'f' * 64
        with self.assertRaises(ValueError):
            self.parse(document)

    def test_commands_capture_source_and_use_create_and_atomic_release(self):
        from app import deathstarbench_hotel_live as live
        observed = self.parse()
        with patch.object(live.inspect, 'getsource', side_effect=AssertionError('source reread')):
            commands = [live.hotel_initialization_observe_command(self.plan, self.stage, ROLE_NODES),
                        live.hotel_initialization_create_command(self.plan, self.stage, ROLE_NODES, self.parse(include=False)),
                        live.hotel_initialization_release_command(self.plan, self.stage, ROLE_NODES, observed, 'initialize-empty'),
                        live.hotel_initialization_logs_command(self.plan, self.stage, 'verify', observed['verifier_pod_uid'])]
        for command in commands:
            self.assertEqual(shlex.split(command)[:3], ['sudo', 'python3', '-c'])
            self.assertLess(len(command.encode()), 128 * 1024)
            compile(shlex.split(command)[3], '<remote>', 'exec')
        release = shlex.split(commands[2])[3]
        self.assertIn('resourceVersion', release)
        self.assertIn("'test'", release)
        self.assertIn('--limit-bytes=4097', shlex.split(commands[3])[3])

    def execute(self, command, document, *, replace_before_patch=False, replace_after_logs=False, logs=b'{"ok":true}\n'):
        records = []
        current = copy.deepcopy(document['inventory'])
        ca = b'-----BEGIN CERTIFICATE-----\nsynthetic-only\n-----END CERTIFICATE-----\n'

        def run(args, **kwargs):
            records.append((args, kwargs))
            if 'logs' in args:
                if replace_after_logs:
                    next(p for p in current['items'] if p['metadata']['name'] == args[args.index('logs') + 1])['metadata']['uid'] = identity('Pod', 'replaced')
                return SimpleNamespace(returncode=0, stdout=logs, stderr=b'')
            if 'patch' in args:
                pod = next(p for p in current['items'] if p['metadata']['name'] == args[args.index('pod') + 1])
                if replace_before_patch:
                    pod['metadata']['resourceVersion'] = '6'
                for operation in json.loads(args[args.index('-p') + 1]):
                    parts = operation['path'].split('/')[1:]
                    value = pod
                    for part in parts[:-1]:
                        value = value[part]
                    if operation['op'] == 'test' and value.get(parts[-1]) != operation['value']:
                        return SimpleNamespace(returncode=1, stdout=b'', stderr=b'conflict')
                    if operation['op'] == 'remove':
                        del value[parts[-1]]
                return SimpleNamespace(returncode=0, stdout=json.dumps(pod).encode(), stderr=b'')
            if 'create' in args:
                for item in json.loads(kwargs['input'])['items']:
                    item['metadata'].update(uid=identity(item['kind'], item['metadata']['name']), resourceVersion='5')
                    if item['kind'] == 'Pod':
                        item['status'] = {'phase': 'Pending'}
                    current['items'].append(item)
                return SimpleNamespace(returncode=0, stdout=b'created', stderr=b'')
            resource = args[args.index('get') + 1]
            if resource == 'namespace':
                name = args[args.index('namespace') + 1]
                value = ({'kind': 'Namespace', 'apiVersion': 'v1', 'metadata': {'name': 'kube-system', 'uid': document['kube_system_namespace_uid']}}
                         if name == 'kube-system' else objects(current, 'Namespace')[0])
            elif resource == 'pod':
                value = next(p for p in current['items'] if p['metadata']['name'] == args[args.index('pod') + 1])
            elif resource == 'persistentvolumes':
                value = {'items': objects(current, 'PersistentVolume')}
            elif resource == 'nodes':
                value = {'items': objects(current, 'Node')}
            else:
                value = {'items': [p for p in current['items'] if p['metadata'].get('namespace') == self.plan.namespace]}
            return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode(), stderr=b'')

        output = io.BytesIO()
        stream = io.TextIOWrapper(output, encoding='utf-8')
        namespace = {}
        with patch('subprocess.run', side_effect=run), patch('builtins.open', mock_open(read_data=ca)), redirect_stdout(stream):
            exec(compile(shlex.split(command)[3], '<synthetic-guest>', 'exec'), namespace)
        stream.flush()
        return records, output.getvalue().decode()

    def test_remote_observe_create_and_release_validate_real_collected_inventory(self):
        from app import deathstarbench_hotel_live as live
        command = live.hotel_initialization_observe_command(self.plan, self.stage, ROLE_NODES)
        records, output = self.execute(command, self.document())
        observed = self.parse(json.loads(output))
        self.assertTrue(all('--request-timeout=20s' in args for args, _ in records))
        command = live.hotel_initialization_release_command(self.plan, self.stage, ROLE_NODES, observed, 'initialize-empty')
        records, output = self.execute(command, json.loads(output))
        self.assertEqual(sum('patch' in args for args, _ in records), 1)
        self.assertNotIn('schedulingGates', self.parse(json.loads(output))['initialization_pods']['initialize-empty']['spec'])
        with self.assertRaises(ValueError):
            self.execute(command, json.loads(output))
        _, before_output = self.execute(live.hotel_initialization_observe_command(self.plan, self.stage, ROLE_NODES, include_initialization=False), self.document(False))
        before = self.parse(json.loads(before_output), include=False)
        command = live.hotel_initialization_create_command(self.plan, self.stage, ROLE_NODES, before)
        records, output = self.execute(command, json.loads(before_output))
        self.assertEqual(sum('create' in args for args, _ in records), 1)
        self.assertFalse(any('apply' in args for args, _ in records))
        self.parse(json.loads(output))
        crossed = copy.deepcopy(before)
        crossed['continuity']['server_ca_sha256'] = 'sha256:' + 'f' * 64
        crossed_command = live.hotel_initialization_create_command(self.plan, self.stage, ROLE_NODES, crossed)
        with self.assertRaises(ValueError):
            self.execute(crossed_command, json.loads(before_output))

    def test_endpoint_slice_may_copy_exact_validated_service_labels(self):
        document = self.document()
        for endpoint in objects(document['inventory'], 'EndpointSlice'):
            name = endpoint['metadata']['labels']['kubernetes.io/service-name']
            service = next(p for p in objects(document['inventory'], 'Service') if p['metadata']['name'] == name)
            endpoint['metadata']['labels'].update(service['metadata']['labels'])
        self.parse(document)

    def test_empty_application_slice_and_support_port_order_use_api_defaults(self):
        document = self.document()
        services = objects(document['inventory'], 'Service')
        pods = {p['metadata']['labels']['app.kubernetes.io/component']: p for p in objects(document['inventory'], 'Pod')
                if p['metadata']['name'] not in (self.plan.initializer_pod_name, self.plan.verifier_pod_name)}
        application = next(s for s in services if s['metadata']['name'] not in pods)
        for service in (application, next(s for s in services if s['metadata']['name'] == 'jaeger')):
            name = service['metadata']['name']
            item = copy.deepcopy(objects(document['inventory'], 'EndpointSlice')[0])
            item['metadata'].update(name=name + '-abc12', uid=identity('EndpointSlice', name), labels={
                **service['metadata']['labels'], 'kubernetes.io/service-name': name,
                'endpointslice.kubernetes.io/managed-by': 'endpointslice-controller.k8s.io'})
            item['metadata']['ownerReferences'][0].update(name=name, uid=service['metadata']['uid'])
            if name in pods:
                pod = pods[name]
                item['ports'] = [{'name': p['name'], 'port': p['targetPort'], 'protocol': p['protocol']} for p in reversed(service['spec']['ports'])]
                item['endpoints'][0].update(addresses=[pod['status']['podIP']], nodeName=pod['spec']['nodeName'],
                    targetRef={'apiVersion': 'v1', 'kind': 'Pod', 'namespace': self.plan.namespace,
                               'name': pod['metadata']['name'], 'uid': pod['metadata']['uid']})
            else:
                item['endpoints'] = None
                item['ports'] = None
            document['inventory']['items'].append(item)
        self.parse(document)

    def test_atomic_release_refuses_resource_version_race_and_logs_are_bounded(self):
        from app import deathstarbench_hotel_live as live
        _, output = self.execute(live.hotel_initialization_observe_command(self.plan, self.stage, ROLE_NODES), self.document())
        observed = self.parse(json.loads(output))
        command = live.hotel_initialization_release_command(self.plan, self.stage, ROLE_NODES, observed, 'initialize-empty')
        with self.assertRaises(ValueError):
            self.execute(command, json.loads(output), replace_before_patch=True)
        command = live.hotel_initialization_logs_command(self.plan, self.stage, 'verify', observed['verifier_pod_uid'])
        _, logs = self.execute(command, self.document())
        self.assertEqual(logs, '{"ok":true}\n')
        for options in ({'replace_after_logs': True}, {'logs': b'x' * 4097}):
            with self.assertRaises(ValueError):
                self.execute(command, self.document(), **options)

    def test_gated_pod_cannot_report_scheduled_condition_and_slice_cannot_be_finalized(self):
        for mutation in ({'conditions': [{'type': 'PodScheduled', 'status': 'True'}]},
                         {'conditions': [{'type': 'PodReadyToStartContainers', 'status': 'True'}]},
                         {'hostIPs': [{'ip': '10.0.0.2'}]}, {'startTime': '2026-10-14T12:00:00Z'}):
            document = self.document()
            objects(document['inventory'], 'Pod')[-1]['status'].update(mutation)
            with self.assertRaises(ValueError):
                self.parse(document)
        document = self.document()
        objects(document['inventory'], 'EndpointSlice')[0]['metadata']['finalizers'] = ['foreign.invalid/finalizer']
        with self.assertRaises(ValueError):
            self.parse(document)

    def test_remote_release_rechecks_other_pod_phase_and_gate(self):
        from app import deathstarbench_hotel_live as live
        _, output = self.execute(live.hotel_initialization_observe_command(self.plan, self.stage, ROLE_NODES), self.document())
        observed = self.parse(json.loads(output))
        command = live.hotel_initialization_release_command(self.plan, self.stage, ROLE_NODES, observed, 'initialize-empty')
        document = json.loads(output)
        verifier = next(p for p in objects(document['inventory'], 'Pod') if p['metadata']['name'] == self.plan.verifier_pod_name)
        verifier['spec'].pop('schedulingGates')
        with self.assertRaises(ValueError):
            self.execute(command, document)
        command = live.hotel_initialization_release_command(self.plan, self.stage, ROLE_NODES, observed, 'verify')
        with self.assertRaises(ValueError):
            self.execute(command, json.loads(output))

    def test_remote_verifier_release_pins_observed_initializer_execution(self):
        from app import deathstarbench_hotel_live as live
        document = self.document()
        initializer = next(p for p in objects(document['inventory'], 'Pod') if p['metadata']['name'] == self.plan.initializer_pod_name)
        initializer['spec'].pop('schedulingGates')
        initializer['spec']['nodeName'] = ROLE_NODES['application']
        image = self.plan.application_image
        initializer['status'] = {'phase': 'Succeeded', 'containerStatuses': [{'name': 'hotel-initialize', 'image': image,
            'imageID': image, 'containerID': 'containerd://' + 'a' * 64, 'restartCount': 0, 'ready': False, 'lastState': {},
            'state': {'terminated': {'exitCode': 0, 'startedAt': '2026-10-14T12:00:00Z', 'finishedAt': '2026-10-14T12:00:01Z'}}}]}
        _, output = self.execute(live.hotel_initialization_observe_command(self.plan, self.stage, ROLE_NODES), document)
        observed = self.parse(json.loads(output))
        command = live.hotel_initialization_release_command(self.plan, self.stage, ROLE_NODES, observed, 'verify')
        self.execute(command, json.loads(output))
        document = json.loads(output)
        initializer = next(p for p in objects(document['inventory'], 'Pod') if p['metadata']['name'] == self.plan.initializer_pod_name)
        initializer['status']['containerStatuses'][0]['containerID'] = 'containerd://' + 'b' * 64
        with self.assertRaises(ValueError):
            self.execute(command, document)
