"""Offline generated-observer protocol tests, not live qualification."""
import copy
from contextlib import redirect_stdout
from email.message import Message
import hashlib
import io
import json
import shlex
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from app import deathstarbench_hotel_startup_k3s as live
from app.deathstarbench_hotel_initialization_stage import build_guarded_hotel_initialization_stage
from app.deathstarbench_hotel_runtime import create_guarded_hotel_runtime_plan
from tests import test_deathstarbench_hotel_initialization_artifacts as guarded
from tests import test_deathstarbench_hotel_live_k3s as init_fixtures
from tests import test_deathstarbench_hotel_startup_identity as identity_fixtures
from tests.test_deathstarbench_hotel_startup_runtime import StartupRuntimeFixtureMixin

ROLES = identity_fixtures.ROLES


class StartupObservationTests(StartupRuntimeFixtureMixin, unittest.TestCase):
    admitted = identity_fixtures.StartupIdentityTests.admitted
    execution = identity_fixtures.StartupIdentityTests.execution
    executions = identity_fixtures.StartupIdentityTests.executions
    cri = identity_fixtures.StartupIdentityTests.cri
    network = identity_fixtures.StartupIdentityTests.network
    consul = identity_fixtures.StartupIdentityTests.consul

    def setUp(self):
        original = guarded.GuardedHotelArtifactsTests.fixture
        captured = {}
        def capture(helper, **kwargs):
            data = original(helper, **kwargs)
            captured['artifact'] = helper.adopt(data)
            return data
        with patch.object(guarded.GuardedHotelArtifactsTests, 'fixture', capture):
            super().setUp()
        from app.deathstarbench_hotel_startup_stage import build_startup_hotel_startup_stage
        self.runtime = self.plan()
        self.stage = build_startup_hotel_startup_stage(self.runtime)
        self.initialization_plan = create_guarded_hotel_runtime_plan(
            captured['artifact'], self.support_plan, self.support_stage,
            initialization_attempt_id=self.runtime.initialization_attempt_id)
        self.initialization_stage = build_guarded_hotel_initialization_stage(self.initialization_plan)

    def wire(self, *, running=False):
        pods = self.executions() if running else {
            name: live.identity.validate_startup_hotel_admitted_pod(self.admitted(name), self.stage.pod(name), ROLES)
            for name in self.stage.metadata()['application_components']}
        slices, services, support, execution = self.network(pods if running else {})
        inventory = init_fixtures.inventory(self.initialization_plan, self.initialization_stage, created=True)
        inventory['items'] = [p for p in inventory['items'] if p['kind'] not in ('EndpointSlice',)
                              and not (p['kind'] == 'Pod' and p['metadata']['labels'].get('deathstarbench.io/role') != 'application')]
        inventory['items'].extend(support + slices + [p['pod'] for p in pods.values()])
        discoveries, lists = {}, {}
        for group, resource, kind, namespaced in live.COLLECTIONS:
            discoveries.setdefault(group, {'apiVersion': 'v1', 'kind': 'APIResourceList',
                'groupVersion': group, 'resources': []})['resources'].append({
                    'name': resource, 'kind': kind, 'namespaced': namespaced, 'verbs': ['get', 'list']})
            path = live._collection_path(group, resource, namespaced, self.runtime.namespace)
            items = [p for p in inventory['items'] if p['kind'] == kind]
            if kind == 'Namespace':
                items.append({'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
                    'name': 'kube-system', 'uid': init_fixtures.support_fixtures.identity('Namespace', 'kube-system')}})
            lists[path] = {'apiVersion': group, 'kind': kind + 'List',
                           'metadata': {'resourceVersion': '123', 'continue': '', 'remainingItemCount': 0}, 'items': items}
        return {**live._envelope(self.runtime, self.stage, self.initialization_plan,
                                self.initialization_stage, ROLES, init_fixtures.CA),
                'discovery': discoveries, 'collections': lists,
                'admission_mutators': init_fixtures.admission_evidence(),
                'consul': self.consul(pods, execution, tuple(pods) if running else ()),
                'consul_execution': execution}, pods

    def parse(self, wire, expected=None):
        return live.parse_startup_hotel_kubernetes_observation(json.dumps(wire), self.runtime, self.stage,
            self.initialization_plan, self.initialization_stage, ROLES, init_fixtures.CA, expected_identity=expected)

    def test_every_required_collection_has_discovery_and_complete_list_evidence(self):
        # Catches treating missing/denied/truncated collections as empty inventories.
        wire, _ = self.wire()
        observed = self.parse(wire)
        self.assertFalse(observed['startup_execution_observed'])
        for path in wire['collections']:
            for mutation in ('missing', 'continue', 'remaining', 'version', 'duplicate'):
                changed = copy.deepcopy(wire)
                collection = changed['collections'][path]
                if mutation == 'missing': del changed['collections'][path]
                elif mutation == 'duplicate': collection['items'] *= 2 if collection['items'] else 1; collection['items'].append({'kind': 'Foreign'})
                else: collection['metadata'][{'continue': 'continue', 'remaining': 'remainingItemCount', 'version': 'resourceVersion'}[mutation]] = {'continue': 'next', 'remaining': 1, 'version': ''}[mutation]
                with self.subTest(path=path, mutation=mutation), self.assertRaises(ValueError): self.parse(changed)
        for key in ('discovery', 'admission_mutators'):
            changed = copy.deepcopy(wire); changed[key] = {}
            with self.assertRaises(ValueError): self.parse(changed)
        with self.assertRaises(ValueError):
            live.parse_startup_hotel_kubernetes_observation(' ' * (4 * 1024 * 1024 + 1), self.runtime,
                self.stage, self.initialization_plan, self.initialization_stage, ROLES, init_fixtures.CA)

    def test_real_application_endpoints_are_validated_not_filtered(self):
        # Catches feeding invented empty application slices into the support-only observer.
        wire, pods = self.wire(running=True)
        _, gated = self.wire()
        for name in gated: gated[name]['released'] = True
        observed = self.parse(wire, {'startup_pods': gated})
        self.assertEqual(observed['endpoints']['geo']['pod_uid'], pods['geo']['pod_uid'])
        changed = copy.deepcopy(wire)
        slices = next(v['items'] for v in changed['collections'].values() if v['kind'] == 'EndpointSliceList')
        next(p for p in slices if p['metadata']['labels']['kubernetes.io/service-name'] == 'geo')['endpoints'][0]['targetRef']['uid'] = 'wrong'
        with self.assertRaises(ValueError): self.parse(changed, {'startup_pods': gated})

    def test_cri_is_collected_on_application_node_and_rejoined(self):
        # Catches claiming complete proof from Kubernetes alone or accepting replaced execution.
        wire, pods = self.wire(running=True)
        _, gated = self.wire()
        for pod in gated.values(): pod['released'] = True
        first = self.parse(wire, {'startup_pods': gated})
        self.assertFalse(first['startup_execution_observed'])
        final = self.parse(wire, first['identity'])
        result = live.validate_startup_hotel_live_observation(final, json.dumps(self.cri_wire(pods)), self.runtime,
                                                             started_components=tuple(pods))
        self.assertTrue(result['startup_execution_observed'])
        cri_host = result['cri']['geo']['node_name']
        self.assertEqual(cri_host, ROLES['application'])
        for field in ('raw_output_bytes', 'first_kubernetes_raw_output_bytes'):
            for invalid in (True, 0, -1, 4 * 1024 * 1024 + 1, None):
                changed = copy.deepcopy(final); changed[field] = invalid
                with self.subTest(count=field, invalid=invalid), self.assertRaises(ValueError):
                    live.validate_startup_hotel_live_observation(changed, json.dumps(self.cri_wire(pods)),
                                                                self.runtime, started_components=tuple(pods))
        for mutate in ('container_id', 'pod_uid', 'pod_ip', 'started_at'):
            changed = copy.deepcopy(final); changed['first_kubernetes_anchor']['geo'][mutate] = 'replaced'
            with self.subTest(field=mutate), self.assertRaises(ValueError):
                live.validate_startup_hotel_live_observation(changed, json.dumps(self.cri_wire(pods)), self.runtime,
                                                             started_components=tuple(pods))
        del final['first_kubernetes_anchor']
        with self.assertRaises(ValueError):
            live.validate_startup_hotel_live_observation(final, json.dumps(self.cri_wire(pods)), self.runtime, started_components=tuple(pods))

    def cri_wire(self, pods):
        return {'schema_version': 1, 'candidate_schema': 'hotel-startup-cri-observation-v1',
                'application_node': ROLES['application'],
                'first_kubernetes_anchor': live._execution_anchor(pods),
                'evidence': self.cri({c: p for c, p in pods.items() if p['state'] == 'running'})}

    def test_new_execution_between_cri_and_final_read_is_incomplete_not_recovery(self):
        # Catches rejecting healthy Pending->Running progress or claiming CRI for an uninspected execution.
        final_wire, _ = self.wire(running=True)
        first_wire = copy.deepcopy(final_wire)
        pods = next(v['items'] for v in first_wire['collections'].values() if v['kind'] == 'PodList')
        geo = next(p for p in pods if p['metadata']['name'] == self.runtime.application_pod_name('geo'))
        geo['status'] = {'phase': 'Pending', 'containerStatuses': []}
        geo['spec'].pop('nodeName')
        self.lag(first_wire)
        _, gated = self.wire()
        for pod in gated.values(): pod['released'] = True
        first = self.parse(first_wire, {'startup_pods': gated})
        final = self.parse(final_wire, first['identity'])
        result = live.validate_startup_hotel_live_observation(final, json.dumps(self.cri_wire(first['startup_pods'])),
                                                             self.runtime, started_components=tuple(gated))
        self.assertFalse(result['startup_execution_observed'])
        self.assertEqual(result['execution_pending_components'], ['geo'])
        self.assertEqual(result['observed_running_count'], 9)
        self.assertNotIn('geo', result['cri'])
        complete_final = self.parse(final_wire, final['identity'])
        complete = live.validate_startup_hotel_live_observation(complete_final,
            json.dumps(self.cri_wire(final['startup_pods'])), self.runtime, started_components=tuple(gated))
        self.assertTrue(complete['startup_execution_observed'])
        self.assertEqual(complete['observed_running_count'], 10)
        for mutation in ('uid', 'container', 'node', 'ip'):
            changed = copy.deepcopy(final_wire)
            current = next(p for v in changed['collections'].values() for p in v['items']
                           if p['kind'] == 'Pod' and p['metadata']['name'] == self.runtime.application_pod_name('user'))
            if mutation == 'uid': current['metadata']['uid'] = '00000000-0000-4000-8000-000000000999'
            elif mutation == 'container': current['status']['containerStatuses'][0]['containerID'] = 'containerd://' + 'f' * 64
            elif mutation == 'node': current['spec']['nodeName'] = ROLES['control']
            else: current['status'].update(podIP='10.42.1.250', podIPs=[{'ip': '10.42.1.250'}])
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.parse(changed, first['identity'])

    def test_cumulative_triplet_raw_output_is_bounded_not_only_each_command(self):
        # Catches independently bounded outputs collectively exceeding the raw evidence budget.
        wire, pods = self.wire(running=True)
        _, gated = self.wire()
        for pod in gated.values(): pod['released'] = True
        raw = json.dumps(wire)
        padded = raw + ' ' * (2 * 1024 * 1024 - len(raw.encode()))
        first = live.parse_startup_hotel_kubernetes_observation(padded, self.runtime, self.stage,
            self.initialization_plan, self.initialization_stage, ROLES, init_fixtures.CA,
            expected_identity={'startup_pods': gated})
        final = live.parse_startup_hotel_kubernetes_observation(padded, self.runtime, self.stage,
            self.initialization_plan, self.initialization_stage, ROLES, init_fixtures.CA,
            expected_identity=first['identity'])
        self.assertEqual(first['identity']['raw_output_bytes'], 2 * 1024 * 1024)
        self.assertEqual(final['first_kubernetes_raw_output_bytes'], 2 * 1024 * 1024)
        with self.assertRaises(ValueError):
            live.validate_startup_hotel_live_observation(final, json.dumps(self.cri_wire(pods)), self.runtime,
                                                        started_components=tuple(pods))

    def lag(self, wire, component='geo'):
        slices = next(v['items'] for v in wire['collections'].values() if v['kind'] == 'EndpointSliceList')
        endpoint = next(p for p in slices if p['metadata']['labels']['kubernetes.io/service-name'] == component)
        endpoint.update(ports=[], endpoints=[])
        wire['consul']['agent_services'] = {k: v for k, v in wire['consul']['agent_services'].items()
                                            if v['Service'] != 'srv-' + component}
        wire['consul']['catalog_services'].pop('srv-' + component, None)
        wire['consul']['catalog_entries'].pop('srv-' + component, None)

    def test_lag_does_not_hide_hostile_publication_or_erase_confirmed_identity(self):
        # Catches generic pending exception suppression and readiness regression recovery.
        wire, _ = self.wire(running=True)
        _, gated = self.wire()
        for pod in gated.values(): pod['released'] = True
        lagged = copy.deepcopy(wire); self.lag(lagged)
        pending = self.parse(lagged, {'startup_pods': gated})
        self.assertEqual(pending['publication_pending'], {'endpoints': ['geo'], 'consul': ['geo']})
        self.assertNotIn('geo', pending['publication_confirmed']['endpoints'])
        for channel in ('endpoints', 'consul'):
            hostile = copy.deepcopy(lagged)
            if channel == 'endpoints':
                slices = next(v['items'] for v in hostile['collections'].values() if v['kind'] == 'EndpointSliceList')
                next(s for s in slices if s['metadata']['labels']['kubernetes.io/service-name'] == 'user')['endpoints'][0]['targetRef']['uid'] = 'foreign'
            else:
                next(v for v in hostile['consul']['agent_services'].values() if v['Service'] == 'srv-user')['Address'] = '10.42.1.250'
            with self.subTest(channel=channel), self.assertRaises(ValueError):
                self.parse(hostile, {'startup_pods': gated})
        complete = self.parse(wire, pending['identity'])
        self.assertIn('geo', complete['publication_confirmed']['endpoints'])
        self.assertIn('geo', complete['publication_confirmed']['consul'])
        for channel in ('endpoints', 'consul'):
            regressed = copy.deepcopy(lagged)
            if channel == 'endpoints': regressed['consul'] = copy.deepcopy(wire['consul'])
            else:
                for path, collection in wire['collections'].items():
                    if collection['kind'] == 'EndpointSliceList': regressed['collections'][path] = copy.deepcopy(collection)
            with self.subTest(regression=channel), self.assertRaises(ValueError):
                self.parse(regressed, complete['identity'])

    def test_pending_pod_publication_is_not_running_execution(self):
        # Catches promoting exact early publication into container execution proof.
        wire, _ = self.wire(running=True)
        _, gated = self.wire()
        for pod in gated.values(): pod['released'] = True
        pods = next(v['items'] for v in wire['collections'].values() if v['kind'] == 'PodList')
        pod = next(p for p in pods if p['metadata']['name'] == self.runtime.application_pod_name('geo'))
        pod['status'].update(phase='Pending', containerStatuses=[])
        slices = next(v['items'] for v in wire['collections'].values() if v['kind'] == 'EndpointSliceList')
        next(s for s in slices if s['metadata']['labels']['kubernetes.io/service-name'] == 'geo')['endpoints'][0]['conditions'] = {
            'ready': False, 'serving': False, 'terminating': False}
        observed = self.parse(wire, {'startup_pods': gated})
        self.assertIn('geo', observed['released_components'])
        self.assertNotIn('geo', observed['running_components'])
        self.assertEqual(observed['publication_pending'], {'endpoints': ['geo'], 'consul': ['geo']})
        self.assertNotIn('geo', observed['consul']['backend_registrations'])

    def test_original_service_selector_and_port_graph_cannot_be_augmented(self):
        # Catches subset support validation admitting an additional behavior-changing selector/port field.
        wire, _ = self.wire()
        defaulted = copy.deepcopy(wire)
        services = next(v['items'] for v in defaulted['collections'].values() if v['kind'] == 'ServiceList')
        next(s for s in services if s['metadata']['name'] == 'consul')['spec']['ports'][0]['nodePort'] = 0
        self.assertFalse(self.parse(defaulted)['startup_execution_observed'])
        for field in ('selector', 'appProtocol'):
            changed = copy.deepcopy(wire)
            services = next(v['items'] for v in changed['collections'].values() if v['kind'] == 'ServiceList')
            service = next(s for s in services if s['metadata']['name'] == 'consul')
            if field == 'selector':
                pods = next(v['items'] for v in changed['collections'].values() if v['kind'] == 'PodList')
                pod = next(p for p in pods if p['metadata']['name'] == changed['consul_execution']['pod_name'])
                key = next(k for k in pod['metadata']['labels'] if k not in service['spec']['selector'])
                service['spec']['selector'][key] = pod['metadata']['labels'][key]
            else: service['spec']['ports'][0]['appProtocol'] = 'foreign'
            with self.subTest(field=field), self.assertRaises(ValueError): self.parse(changed)

    def test_initializer_policy_metadata_matches_exact_original_factory(self):
        # Catches accidentally broadening the initializer's exact metadata contract to support subsets.
        wire, _ = self.wire()
        self.assertFalse(self.parse(wire)['startup_execution_observed'])
        approved = next(p for p in json.loads(self.initialization_stage.canonical_json)['items']
                        if p['kind'] == 'NetworkPolicy')
        for field in ('name', 'namespace', 'labels', 'annotations'):
            changed = copy.deepcopy(wire)
            policy = next(p for v in changed['collections'].values() for p in v['items']
                          if p['kind'] == 'NetworkPolicy' and p['metadata']['name'] == approved['metadata']['name'])
            if field in ('name', 'namespace'): policy['metadata'][field] += '-foreign'
            else: policy['metadata'][field]['foreign'] = 'unapproved'
            with self.subTest(field=field), self.assertRaises(ValueError): self.parse(changed)

    def test_managed_fields_projection_preserves_every_other_pod_pin(self):
        # Catches pinning mutable field-manager bookkeeping or eliding real admitted identity/spec fields.
        wire, _ = self.wire()
        pod = next(p for v in wire['collections'].values() for p in v['items']
                   if p['kind'] == 'Pod' and p['metadata']['name'] == self.runtime.application_pod_name('geo'))
        pod['metadata']['managedFields'] = [{'manager': 'kubectl', 'operation': 'Update', 'apiVersion': 'v1',
            'fieldsType': 'FieldsV1', 'fieldsV1': {'f:spec': {'f:schedulingGates': {}}}}]
        first = self.parse(wire)
        final_wire = copy.deepcopy(wire)
        current = next(p for v in final_wire['collections'].values() for p in v['items']
                       if p['kind'] == 'Pod' and p['metadata']['name'] == pod['metadata']['name'])
        current['metadata']['managedFields'].append({'manager': 'kubelet', 'operation': 'Update', 'apiVersion': 'v1',
            'fieldsType': 'FieldsV1', 'fieldsV1': {'f:status': {}}, 'subresource': 'status'})
        current['metadata']['resourceVersion'] = '456'
        current['status']['message'] = 'Preserve this raw status field.'
        final = self.parse(final_wire, first['identity'])
        self.assertNotIn('managedFields', final['startup_pods']['geo']['admitted_metadata'])
        self.assertNotIn('managedFields', final['startup_pods']['geo']['pod']['metadata'])
        self.assertEqual(final['startup_pods']['geo']['pod']['status']['message'], 'Preserve this raw status field.')
        self.assertEqual(len(current['metadata']['managedFields']), 2)
        for mutation in ('annotation', 'spec', 'projection'):
            changed, previous = copy.deepcopy(final_wire), copy.deepcopy(first['identity'])
            target = next(p for v in changed['collections'].values() for p in v['items']
                          if p['kind'] == 'Pod' and p['metadata']['name'] == pod['metadata']['name'])
            if mutation == 'annotation': target['metadata']['annotations']['foreign'] = 'value'
            elif mutation == 'spec': target['spec']['containers'][0]['resources']['limits']['memory'] = '1Gi'
            else: previous['pod_pinning_projection'] = 'incompatible-pinning-projection'
            with self.subTest(mutation=mutation), self.assertRaises(ValueError): self.parse(changed, previous)

    def execute_command(self, command, responses, *, consul=None, replace=None, http_mutation=None):
        """Real generated script and real pipes; fake only kubectl/CRI/HTTP boundaries."""
        calls, requests, timers = [], [], []
        real_popen = subprocess.Popen
        ca = b'OFFLINE-CA-NOT-A-REAL-CERTIFICATE'
        def popen(arguments, **kwargs):
            calls.append(arguments)
            key = arguments[-1]
            raw = responses[key]
            code = 0
            if replace:
                raw, code = replace(key, raw)
            if not isinstance(raw, bytes): raw = json.dumps(raw).encode()
            if len(raw) > 100000:
                with tempfile.TemporaryFile() as payload:
                    payload.write(raw)
                    payload.seek(0)
                    return real_popen([sys.executable, '-c',
                        'import sys;sys.stdout.buffer.write(sys.stdin.buffer.read());sys.exit(int(sys.argv[1]))',
                        str(code)], stdin=payload, **kwargs)
            return real_popen([sys.executable, '-c',
                'import sys;sys.stdout.buffer.write(bytes.fromhex(sys.argv[1]));sys.exit(int(sys.argv[2]))',
                raw.hex(), str(code)], **kwargs)
        class Response(io.BytesIO):
            def __init__(self, data, filtered='false'):
                super().__init__(json.dumps(data).encode())
                self.status = 200
                self.headers = Message()
                self.headers['Content-Type'] = 'application/json'
                self.headers['X-Consul-Results-Filtered-By-ACLs'] = filtered
        class Opener:
            def open(_self, request, timeout):
                requests.append((request.full_url, timeout, dict(request.header_items())))
                key = request.full_url.split('/v1/')[1]
                value = consul[key]
                response = value if isinstance(value, Response) else Response(value)
                if http_mutation: http_mutation(response)
                return response
        output = io.StringIO()
        previous_alarm = signal.getsignal(signal.SIGALRM)
        try:
            with patch('subprocess.Popen', popen), patch('builtins.open', return_value=io.BytesIO(ca)), \
                 patch('urllib.request.build_opener', return_value=Opener()), \
                 patch('signal.setitimer', side_effect=lambda *args: timers.append(args)), redirect_stdout(output):
                exec(shlex.split(command)[-1], {'__name__': '__main__'})
        finally:
            signal.signal(signal.SIGALRM, previous_alarm)
        return json.loads(output.getvalue()), calls, requests, timers

    def remote_fixture(self, deadline=10):
        wire, _ = self.wire()
        ca = 'sha256:' + hashlib.sha256(b'OFFLINE-CA-NOT-A-REAL-CERTIFICATE').hexdigest()
        responses = dict(wire['collections'])
        responses.update({('/api/v1' if group == 'v1' else '/apis/' + group): document
                          for group, document in wire['discovery'].items()})
        admission = wire['admission_mutators']
        responses['/apis/admissionregistration.k8s.io'] = admission['api_group']
        responses.update({'/apis/' + path: doc for path, doc in admission['discovery'].items()})
        responses.update({'/apis/' + path: doc for path, doc in admission['inventories'].items()})
        pod = next(p for v in wire['collections'].values() for p in v['items']
                   if p['kind'] == 'Pod' and p['metadata']['name'] == wire['consul_execution']['pod_name'])
        responses['/api/v1/namespaces/' + self.runtime.namespace + '/pods/' + pod['metadata']['name']] = pod
        consul = {'agent/services': wire['consul']['agent_services'],
                  'catalog/services': wire['consul']['catalog_services'],
                  **{'catalog/service/' + k: v for k, v in wire['consul']['catalog_entries'].items()}}
        command = live.startup_hotel_observation_command(self.runtime, self.stage, self.initialization_plan,
            self.initialization_stage, ROLES, ca, deadline_remaining_seconds=deadline)
        return command, responses, consul, ca

    def test_generated_collector_executes_all_discovery_lists_and_bounded_consul_requests(self):
        # Catches a collector that declares list proof without actually performing every read.
        command, responses, consul, ca = self.remote_fixture()
        for pod in responses['/api/v1/namespaces/deathstarbench-hotel/pods']['items']:
            pod['metadata']['managedFields'] = [{'manager': 'real-server-bookkeeping'}]
        wire, calls, requests, timers = self.execute_command(command, responses, consul=consul)
        observed = live.parse_startup_hotel_kubernetes_observation(json.dumps(wire), self.runtime,
            self.stage, self.initialization_plan, self.initialization_stage, ROLES, ca)
        self.assertFalse(observed['startup_execution_observed'])
        self.assertTrue(all('managedFields' in pod['metadata']
                            for pod in wire['collections']['/api/v1/namespaces/deathstarbench-hotel/pods']['items']))
        self.assertEqual({c[-1] for c in calls}, set(responses))
        self.assertEqual(timers[0], (signal.ITIMER_REAL, 10.0))
        self.assertEqual({r[0] for r in requests}, {
            'http://' + wire['consul_execution']['pod_ip'] + ':8500/v1/' + path for path in consul})
        self.assertTrue(all(0 < timeout <= 10 for _, timeout, _ in requests))
        self.assertTrue(all(headers['Cache-control'] == 'no-cache' for _, _, headers in requests))
        self.assertEqual(calls[-1][-1], '/api/v1/namespaces/deathstarbench-hotel/pods')
        for mutation in ('denied', 'duplicate', 'truncated', 'oversized'):
            def replace(path, raw):
                if path != '/api/v1/namespaces/deathstarbench-hotel/podtemplates': return raw, 0
                if mutation == 'denied': return b'Forbidden', 1
                if mutation == 'duplicate': return b'{"items":[],"items":[]}', 0
                if mutation == 'oversized': return b' ' * (4 * 1024 * 1024 + 1), 0
                changed = copy.deepcopy(raw); changed['metadata']['continue'] = 'next'
                return changed, 0
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.execute_command(command, responses, consul=consul, replace=replace)

    def test_generated_observation_does_not_reread_later_checkout_source(self):
        # Catches helper generation splicing a changed checkout or depending on source that disappeared.
        _, responses, consul, ca = self.remote_fixture()
        with patch('inspect.getsource', side_effect=OSError('source no longer available')):
            command = live.startup_hotel_observation_command(self.runtime, self.stage, self.initialization_plan,
                self.initialization_stage, ROLES, ca, deadline_remaining_seconds=60)
        wire, _, _, _ = self.execute_command(command, responses, consul=consul)
        observed = live.parse_startup_hotel_kubernetes_observation(json.dumps(wire), self.runtime,
            self.stage, self.initialization_plan, self.initialization_stage, ROLES, ca)
        self.assertEqual(observed['consul']['intrinsic_consul']['pod_uid'], wire['consul_execution']['pod_uid'])

    def test_generated_cri_uses_exact_raw_ids_on_the_routed_application_node(self):
        # Catches inspecting a summary/config alias instead of the exact raw single-container IDs.
        pods = self.executions()
        evidence = self.cri(pods)
        responses = {p['inspect']['status']['id']: p['inspect'] for p in evidence['entries']}
        responses[self.runtime.snapshot()['application_config_digest']] = evidence['entries'][0]['inspecti']
        command = live.startup_hotel_cri_observation_command(self.runtime, pods, deadline_remaining_seconds=900)
        wire, calls, _, timers = self.execute_command(command, responses)
        result = live.identity.validate_startup_hotel_cri_evidence(wire['evidence'], self.runtime, pods)
        self.assertEqual(len(result), 10)
        self.assertEqual({entry['node_name'] for entry in wire['evidence']['entries']}, {ROLES['application']})
        self.assertEqual(wire['application_node'], ROLES['application'])
        self.assertEqual(wire['first_kubernetes_anchor']['geo']['container_id'], pods['geo']['container_id'])
        self.assertEqual(len(calls), 20)
        self.assertEqual(timers[0], (signal.ITIMER_REAL, 60.0))
        self.assertTrue(all(timer == (signal.ITIMER_REAL, 15.0) for timer in timers[1::2]))

    def test_generated_http_rejects_partial_redirect_and_acl_filtered_responses(self):
        # Catches accepting a non-complete HTTP response as authoritative registration absence.
        command, responses, consul, _ = self.remote_fixture(deadline=900)
        _, _, requests, timers = self.execute_command(command, responses, consul=consul)
        self.assertEqual(timers[0], (signal.ITIMER_REAL, 60.0))
        self.assertTrue(all(timeout == 15 for _, timeout, _ in requests))
        for status in (206, 301, 302, 403):
            with self.subTest(status=status), self.assertRaises(ValueError):
                self.execute_command(command, responses, consul=consul,
                    http_mutation=lambda response: setattr(response, 'status', status))
        def filtered(response):
            response.headers.replace_header('X-Consul-Results-Filtered-By-ACLs', 'true')
        with self.assertRaises(ValueError):
            self.execute_command(command, responses, consul=consul, http_mutation=filtered)
        for mutation in ('duplicate', 'oversize'):
            def bad_headers(response):
                if mutation == 'duplicate': response.headers['X-Consul-Results-Filtered-By-ACLs'] = 'false'
                else: response.headers['X-Extra'] = 'x' * 16385
            with self.subTest(headers=mutation), self.assertRaises(ValueError):
                self.execute_command(command, responses, consul=consul, http_mutation=bad_headers)

    def test_generated_output_envelope_is_independently_bounded(self):
        # Catches checking only raw subprocess totals while emitting an oversized outer envelope.
        pods = {'geo': self.execution()[1]}
        entry = self.cri(pods)['entries'][0]
        responses = {entry['inspect']['status']['id']: entry['inspect'],
                     self.runtime.snapshot()['application_config_digest']: entry['inspecti']}
        entry['inspecti']['info']['padding'] = ''
        size = sum(len(json.dumps(raw).encode()) for raw in responses.values())
        entry['inspecti']['info']['padding'] = 'x' * (4 * 1024 * 1024 - 1 - size)
        command = live.startup_hotel_cri_observation_command(self.runtime, pods, deadline_remaining_seconds=60)
        with self.assertRaisesRegex(ValueError, 'Startup output exceeds'):
            self.execute_command(command, responses)

    def test_generated_request_and_window_expiration_fail_without_output(self):
        # Catches an expired/short remaining deadline silently using the normal 60-second window.
        pods = self.executions()
        for deadline in (0, -1, float('nan'), float('inf'), True):
            with self.subTest(deadline=deadline), self.assertRaises(ValueError):
                live.startup_hotel_cri_observation_command(self.runtime, pods, deadline_remaining_seconds=deadline)
        command = live.startup_hotel_cri_observation_command(self.runtime, pods, deadline_remaining_seconds=0.001)
        evidence = self.cri(pods)
        responses = {p['inspect']['status']['id']: p['inspect'] for p in evidence['entries']}
        with self.assertRaises(ValueError): self.execute_command(command, responses)
