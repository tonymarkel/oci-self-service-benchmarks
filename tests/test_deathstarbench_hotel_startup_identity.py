"""Exact startup identity joins against genuine offline factory fixtures."""

import copy
import hashlib
import unittest

from app import deathstarbench_hotel_startup_identity as identity
from app import deathstarbench_hotel_startup_stage as staging
from tests import test_deathstarbench_candidate_k3s as support_fixtures
from tests.test_deathstarbench_hotel_startup_runtime import (
    RUN, STARTUP_ATTEMPT, StartupRuntimeFixtureMixin, uid,
)


ROLES = support_fixtures.ROLE_NODES
PORTS = {'attractions': 8089, 'geo': 8083, 'profile': 8081, 'rate': 8084,
         'recommendation': 8085, 'reservation': 8087, 'review': 8088,
         'search': 8082, 'user': 8086}


class StartupIdentityTests(StartupRuntimeFixtureMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.runtime = self.plan()
        self.stage = staging.build_startup_hotel_startup_stage(self.runtime)

    def admitted(self, component='geo'):
        pod = self.stage.pod(component)
        pod['metadata'].update(uid=support_fixtures.identity('Pod', pod['metadata']['name']),
                               resourceVersion='123', generation=1, creationTimestamp='2026-10-19T12:00:00Z')
        pod['spec'].update(serviceAccountName='default', priority=0,
                           preemptionPolicy='PreemptLowerPriority', tolerations=[
                               {'key': 'node.kubernetes.io/' + key, 'operator': 'Exists',
                                'effect': 'NoExecute', 'tolerationSeconds': 300}
                               for key in ('not-ready', 'unreachable')])
        pod['spec']['containers'][0].update(terminationMessagePath='/dev/termination-log',
                                            terminationMessagePolicy='File')
        pod['status'] = {'phase': 'Pending'}
        return pod

    def execution(self, component='geo', number=20):
        pod = self.admitted(component)
        pinned = identity.validate_startup_hotel_admitted_pod(pod, self.stage.pod(component), ROLES)
        pod['metadata'].update(resourceVersion='124', generation=2)
        pod['spec'].pop('schedulingGates')
        pod['spec']['nodeName'] = ROLES['application']
        pod['status'] = {
            'phase': 'Running', 'podIP': '10.42.1.' + str(number),
            'podIPs': [{'ip': '10.42.1.' + str(number)}],
            'containerStatuses': [{
                'name': component, 'image': self.runtime.application_image,
                'imageID': 'docker-pullable://' + self.runtime.application_image,
                'containerID': 'containerd://' + hashlib.sha256(component.encode()).hexdigest(),
                'restartCount': 0, 'ready': True, 'started': True, 'lastState': {},
                'state': {'running': {'startedAt': '2026-10-19T12:00:01Z'}},
            }],
        }
        result = identity.validate_startup_hotel_admitted_pod(
            pod, self.stage.pod(component), ROLES, pinned=pinned, released=True)
        return pod, result

    def executions(self):
        return {name: self.execution(name, number)[1]
                for number, name in enumerate(staging.APPLICATIONS, 20)}

    def pending(self, *, address='10.42.1.22', node=ROLES['application']):
        pod = self.admitted('geo')
        pinned = identity.validate_startup_hotel_admitted_pod(pod, self.stage.pod('geo'), ROLES)
        pod['metadata'].update(resourceVersion='124', generation=2)
        pod['spec'].pop('schedulingGates')
        if node is not None:
            pod['spec']['nodeName'] = node
        if address is not None:
            pod['status'].update(podIP=address, podIPs=[{'ip': address}])
        return identity.validate_startup_hotel_admitted_pod(
            pod, self.stage.pod('geo'), ROLES, pinned=pinned, released=True)

    def cri(self, pods, architecture='arm64'):
        config = self.runtime.snapshot()['application_config_digest']
        entries = []
        for name, pod in pods.items():
            entries.append({'component': name, 'node_name': pod['node_name'],
                'inspect': {'status': {
                    'id': pod['container_id'].removeprefix('containerd://'),
                    'metadata': {'name': name, 'attempt': 0}, 'state': 'CONTAINER_RUNNING',
                    'createdAt': '2026-10-19T12:00:00Z', 'startedAt': '2026-10-19T12:00:01Z',
                    'finishedAt': '0001-01-01T00:00:00Z', 'exitCode': 0,
                    'image': {'image': self.runtime.application_image},
                    'imageRef': self.runtime.application_image, 'imageId': config,
                    'labels': {'io.kubernetes.pod.uid': pod['pod_uid'],
                               'io.kubernetes.pod.name': pod['pod_name'],
                               'io.kubernetes.pod.namespace': self.runtime.namespace,
                               'io.kubernetes.container.name': name},
                    'annotations': {}, 'mounts': [], 'logPath': '/var/log/pods/synthetic/0.log'},
                    'info': {'sandboxID': 'a' * 64, 'pid': 1234, 'removing': False}},
                'inspecti': {'status': {'id': config, 'repoTags': [],
                    'repoDigests': [self.runtime.application_image], 'size': '12345',
                    'uid': {'value': '65532'}, 'username': '', 'pinned': False},
                    'info': {'chainID': 'sha256:' + 'b' * 64,
                             'imageSpec': {'os': 'linux', 'architecture': architecture,
                                           'config': {'User': '65532:65532'},
                                           'rootfs': {'type': 'layers', 'diff_ids': []}}}}})
        return {'schema_version': 1, 'candidate_schema': 'hotel-startup-cri-evidence-v1',
                'run_id': RUN, 'startup_attempt_id': STARTUP_ATTEMPT,
                'plan_fingerprint': self.runtime.fingerprint, 'entries': entries}

    def network(self, startup):
        inventory = support_fixtures.live_inventory(self.support_stage)
        services = [item for item in inventory['items'] if item['kind'] == 'Service']
        support = [item for item in inventory['items'] if item['kind'] == 'Pod']
        by_component = {}
        for number, pod in enumerate(support, 1):
            pod['status'].update(podIP='10.42.2.' + str(number),
                                 podIPs=[{'ip': '10.42.2.' + str(number)}])
            by_component[pod['metadata']['labels']['app.kubernetes.io/component']] = {
                'pod_name': pod['metadata']['name'], 'pod_uid': pod['metadata']['uid'],
                'pod_ip': pod['status']['podIP'], 'node_name': pod['spec']['nodeName']}
        by_component.update(startup)
        slices = []
        for service in services:
            name = service['metadata']['name']
            target = by_component.get(name)
            slices.append({'apiVersion': 'discovery.k8s.io/v1', 'kind': 'EndpointSlice',
                'addressType': 'IPv4', 'metadata': {
                    'name': name + '-slice', 'namespace': self.runtime.namespace,
                    'uid': support_fixtures.identity('EndpointSlice', name),
                    'labels': {'kubernetes.io/service-name': name,
                              'endpointslice.kubernetes.io/managed-by': 'endpointslice-controller.k8s.io'},
                    'ownerReferences': [{'apiVersion': 'v1', 'kind': 'Service', 'name': name,
                        'uid': service['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}]},
                'ports': [{'name': p.get('name', ''), 'port': p['targetPort'],
                           'protocol': p.get('protocol', 'TCP')} for p in service['spec']['ports']] if target else [],
                'endpoints': [{'addresses': [target['pod_ip']], 'nodeName': target['node_name'],
                    'conditions': {'ready': True, 'serving': True, 'terminating': False},
                    'targetRef': {'kind': 'Pod', 'namespace': self.runtime.namespace,
                                  'name': target['pod_name'], 'uid': target['pod_uid']}}] if target else []})
        consul = next(p for p in support if p['metadata']['labels']['app.kubernetes.io/component'] == 'consul')
        execution = {**by_component['consul'], 'component': 'consul', 'role': 'control',
                     'container_id': consul['status']['containerStatuses'][0]['containerID'],
                     'image': consul['spec']['containers'][0]['image'],
                     'image_id': consul['status']['containerStatuses'][0]['imageID']}
        return slices, services, support, execution

    def consul(self, pods, execution, started=None):
        started = tuple(pods) if started is None else started
        node = {'Node': execution['pod_name'], 'Address': '127.0.0.1', 'Datacenter': 'dc1'}
        agent, catalog = {}, {'consul': [{**node, 'ServiceID': 'consul', 'ServiceName': 'consul',
                                        'ServiceAddress': '', 'ServicePort': 8300,
                                        'ServiceTags': [], 'ServiceMeta': {'version': '1.20.5'}}]}
        for component in started:
            if component == 'frontend':
                continue
            pod = pods[component]
            service_id = 'hotel-' + RUN + '-' + STARTUP_ATTEMPT + '-' + component + '-' + pod['pod_uid']
            meta = {'run_id': RUN, 'startup_attempt_id': STARTUP_ATTEMPT, 'component': component,
                    'pod_uid': pod['pod_uid'], 'contract_sha256': self.runtime.snapshot()['dataset_fingerprint']}
            agent[service_id] = {'ID': service_id, 'Service': 'srv-' + component,
                'Address': pod['pod_ip'], 'Port': PORTS[component], 'Meta': meta, 'Tags': []}
            catalog['srv-' + component] = [{**node, 'ServiceID': service_id,
                'ServiceName': 'srv-' + component, 'ServiceAddress': pod['pod_ip'],
                'ServicePort': PORTS[component], 'ServiceMeta': meta, 'ServiceTags': []}]
        return {'schema_version': 1, 'candidate_schema': 'hotel-startup-consul-evidence-v1',
                'run_id': RUN, 'startup_attempt_id': STARTUP_ATTEMPT,
                'plan_fingerprint': self.runtime.fingerprint, 'agent_services': agent,
                'catalog_services': {name: [] for name in catalog}, 'catalog_entries': catalog}

    def test_status_only_version_change_preserves_pinned_spec(self):
        # Catches re-pinning mutable spec/UID or treating ordinary status RV updates as replacement.
        pod = self.admitted()
        pinned = identity.validate_startup_hotel_admitted_pod(pod, self.stage.pod('geo'), ROLES)
        pod['metadata']['resourceVersion'] = '456'
        pod['status']['conditions'] = [{'type': 'PodScheduled', 'status': 'False'}]
        observed = identity.validate_startup_hotel_admitted_pod(pod, self.stage.pod('geo'), ROLES, pinned=pinned)
        self.assertEqual(observed['spec'], pinned['spec'])
        self.assertEqual(observed['pod_uid'], pinned['pod_uid'])
        self.assertEqual(observed['resource_version'], '123')
        self.assertEqual(observed['observed_resource_version'], '456')
        for mutate in (
            lambda p: p['metadata'].update(uid=uid(99)),
            lambda p: p['metadata']['annotations'].update(injected='value'),
            lambda p: p['spec']['containers'][0]['env'].append({'name': 'INJECTED', 'value': 'yes'}),
            lambda p: p['spec'].update(volumes=[{'name': 'token', 'projected': {'sources': []}}]),
            lambda p: p['spec']['containers'].append({'name': 'sidecar', 'image': 'other'}),
            lambda p: p['spec'].update(automountServiceAccountToken=True),
            lambda p: p['spec']['containers'][0].update(image='other'),
            lambda p: p['spec']['nodeSelector'].update({'deathstarbench.io/role': 'control'}),
            lambda p: p['spec'].update(nodeName=ROLES['application']),
        ):
            changed = copy.deepcopy(pod)
            mutate(changed)
            with self.subTest(mutation=mutate), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_admitted_pod(changed, self.stage.pod('geo'), ROLES, pinned=pinned)
        running, observed = self.execution()
        self.assertEqual(observed['node_name'], 'dsb-application')
        self.assertEqual(observed['admitted_spec'], pinned['spec'])
        running['spec']['nodeName'] = ROLES['control']
        with self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_admitted_pod(running, self.stage.pod('geo'), ROLES, pinned=pinned, released=True)

    def test_initial_admission_and_execution_fail_closed(self):
        # Catches admission injections even on the first observation and acceptance of failed/restarted execution.
        for mutate in (
            lambda p: p['spec']['containers'][0].update(readinessProbe={'tcpSocket': {'port': 8083}}),
            lambda p: p['spec'].update(initContainers=[{'name': 'init'}]),
            lambda p: p['spec'].update(tolerations=[{'operator': 'Exists'}]),
            lambda p: p['spec']['securityContext'].update(runAsUser=0),
            lambda p: p['spec']['containers'][0]['resources']['limits'].update(memory='1Gi'),
            lambda p: p['status'].update(podIPs=[{'ip': '10.42.1.90'}]),
        ):
            pod = self.admitted()
            mutate(pod)
            with self.subTest(mutation=mutate), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_admitted_pod(pod, self.stage.pod('geo'), ROLES)
        pod, pinned = self.execution()
        for mutate in (
            lambda p: p['status']['containerStatuses'][0].update(restartCount=1),
            lambda p: p['status']['containerStatuses'][0].update(lastState={'terminated': {'exitCode': 1}}),
            lambda p: p['status']['containerStatuses'][0].update(imageID=self.runtime.snapshot()['application_config_digest']),
            lambda p: p['status']['containerStatuses'][0].update(containerID='containerd://' + 'f' * 64),
            lambda p: p['status']['containerStatuses'][0]['state']['running'].update(startedAt='2026-10-19T12:00:02Z'),
            lambda p: p['status'].update(podIP='10.42.1.99'),
            lambda p: p['status']['containerStatuses'][0].update(state={'terminated': {'exitCode': 1}}),
        ):
            changed = copy.deepcopy(pod)
            mutate(changed)
            with self.subTest(mutation=mutate), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_admitted_pod(changed, self.stage.pod('geo'), ROLES, pinned=pinned, released=True)

    def test_native_x86_execution_uses_amd64_not_application_arm64(self):
        # Catches assuming every application runs on arm64 while support remains fixed amd64.
        self.support_plan = self.support(application_architecture='x86_64')
        from app.deathstarbench_candidate_stage import build_candidate_support_stage
        self.support_stage = build_candidate_support_stage(self.support_plan)
        self.runtime = self.plan()
        self.stage = staging.build_startup_hotel_startup_stage(self.runtime)
        pods = self.executions()
        result = identity.validate_startup_hotel_cri_evidence(self.cri(pods, 'amd64'), self.runtime, pods)
        self.assertEqual(result['geo']['architecture'], 'amd64')
        with self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_cri_evidence(self.cri(pods, 'arm64'), self.runtime, pods)

    def test_generation_changes_once_only_for_exact_gate_removal(self):
        # Catches rejecting the API-owned release increment or accepting arbitrary metadata generations.
        pod = self.admitted()
        pod['metadata']['generation'] = 1
        pinned = identity.validate_startup_hotel_admitted_pod(pod, self.stage.pod('geo'), ROLES)
        pod['spec'].pop('schedulingGates')
        pod['metadata'].update(generation=2, resourceVersion='124')
        released = identity.validate_startup_hotel_admitted_pod(
            pod, self.stage.pod('geo'), ROLES, pinned=pinned, released=True)
        self.assertEqual(released['admitted_metadata']['generation'], 1)
        self.assertEqual(released['pod']['metadata']['generation'], 2)
        pod['spec']['nodeName'] = ROLES['application']
        bound = identity.validate_startup_hotel_admitted_pod(
            pod, self.stage.pod('geo'), ROLES, pinned=released, released=True)
        self.assertEqual(bound['pod']['metadata']['generation'], 2)
        for generation in (1, 3, True, None):
            changed = copy.deepcopy(pod)
            changed['metadata']['generation'] = generation
            with self.subTest(generation=generation), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_admitted_pod(
                    changed, self.stage.pod('geo'), ROLES, pinned=pinned, released=True)
        gated = self.admitted()
        gated['metadata']['generation'] = 2
        with self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_admitted_pod(gated, self.stage.pod('geo'), ROLES, pinned=pinned)

    def test_endpoint_inventory_rejects_foreign_support_and_service_selector(self):
        # Catches accepting foreign support objects merely because an added Service/Slice agrees with them.
        pods = self.executions()
        slices, services, support, _ = self.network(pods)
        service = next(s for s in services if s['metadata']['name'] == 'geo')
        service['spec']['selector'] = {'app.kubernetes.io/component': 'user'}
        with self.subTest(selector=True), self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_endpoints(slices, services, support, pods)
        slices, services, support, _ = self.network(pods)
        foreign = copy.deepcopy(support[0])
        foreign['metadata'].update(name='foreign', uid=uid(991))
        foreign['metadata']['labels']['app.kubernetes.io/component'] = 'foreign'
        foreign['status'].update(podIP='10.42.2.250', podIPs=[{'ip': '10.42.2.250'}])
        service = copy.deepcopy(services[0])
        service['metadata'].update(name='foreign', uid=uid(992))
        endpoint = copy.deepcopy(slices[0])
        endpoint['metadata'].update(name='foreign-slice', uid=uid(993))
        endpoint['metadata']['labels']['kubernetes.io/service-name'] = 'foreign'
        endpoint['metadata']['ownerReferences'][0].update(name='foreign', uid=uid(992))
        endpoint['endpoints'] = [{'addresses': ['10.42.2.250'], 'nodeName': foreign['spec']['nodeName'],
            'conditions': {'ready': True}, 'targetRef': {'kind': 'Pod', 'namespace': self.runtime.namespace,
                'name': 'foreign', 'uid': uid(991)}}]
        with self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_endpoints(slices + [endpoint], services + [service], support + [foreign], pods)

    def test_endpoint_owner_target_and_native_cri_must_agree(self):
        # Catches label-only endpoints and manifest-only CRI evidence that belongs to another native execution.
        pods = self.executions()
        slices, services, support, _ = self.network(pods)
        result = identity.validate_startup_hotel_endpoints(slices, services, support, pods)
        self.assertEqual(result['geo']['pod_uid'], pods['geo']['pod_uid'])
        self.assertEqual(result['frontend']['pod_ip'], pods['frontend']['pod_ip'])
        for mutate in (
            lambda s: s['metadata']['ownerReferences'][0].update(uid=uid(99)),
            lambda s: s['endpoints'][0]['targetRef'].update(uid=uid(99)),
            lambda s: s['endpoints'][0].update(addresses=['10.42.1.99']),
            lambda s: s['endpoints'][0].update(nodeName=ROLES['control']),
            lambda s: s['ports'][0].update(port=9999),
            lambda s: s['endpoints'].append(copy.deepcopy(s['endpoints'][0])),
        ):
            changed = copy.deepcopy(slices)
            mutate(next(s for s in changed if s['metadata']['labels']['kubernetes.io/service-name'] == 'geo'))
            with self.subTest(mutation=mutate), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_endpoints(changed, services, support, pods)
        for changed in (slices[:-1], slices + [copy.deepcopy(slices[0])]):
            with self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_endpoints(changed, services, support, pods)
        evidence = self.cri(pods)
        result = identity.validate_startup_hotel_cri_evidence(evidence, self.runtime, pods)
        self.assertEqual(result['geo']['config_digest'], self.runtime.snapshot()['application_config_digest'])
        self.assertEqual(result['geo']['architecture'], 'arm64')
        for mutate in (
            lambda e: e.update(node_name=ROLES['control']),
            lambda e: e['inspect']['status']['labels'].update({'io.kubernetes.pod.uid': uid(99)}),
            lambda e: e['inspect']['status'].update(id='f' * 64),
            lambda e: e['inspect']['status'].update(imageRef=self.runtime.snapshot()['application_index_image']),
            lambda e: e['inspect']['status'].update(imageId='sha256:' + 'f' * 64),
            lambda e: e['inspecti']['status'].update(id='sha256:' + 'f' * 64),
            lambda e: e['inspecti']['status'].update(repoDigests=[]),
            lambda e: e['inspecti']['info']['imageSpec'].update(architecture='amd64'),
            lambda e: e['inspecti']['info']['imageSpec'].update(os='windows'),
            lambda e: e['inspecti'].pop('info'),
        ):
            changed = copy.deepcopy(evidence)
            mutate(changed['entries'][0])
            with self.subTest(mutation=mutate), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_cri_evidence(changed, self.runtime, pods)
        for changed in (dict(evidence, entries=evidence['entries'][:-1]),
                        dict(evidence, entries=evidence['entries'] + [evidence['entries'][0]])):
            with self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_cri_evidence(changed, self.runtime, pods)

    def test_consul_intrinsic_entry_is_not_a_blanket_exception(self):
        # Catches stale/extra registrations hidden by an exemption for every non-srv Consul entry.
        pods = self.executions()
        _, _, _, execution = self.network(pods)
        evidence = self.consul(pods, execution)
        result = identity.validate_startup_hotel_consul_identity(evidence, self.runtime, pods, tuple(pods), execution)
        backend_registrations = result['backend_registrations']
        self.assertEqual(len(backend_registrations), 9)
        self.assertEqual(result['frontend_registrations'], 0)
        self.assertEqual(result['intrinsic_consul']['port'], 8300)
        for mutate in (
            lambda e: e['catalog_services'].update(foreign=[]),
            lambda e: e['catalog_entries']['consul'][0].update(ServicePort=8500),
            lambda e: e['catalog_entries']['consul'][0].update(Address='10.42.2.99'),
            lambda e: e['catalog_entries']['consul'][0].update(Node='foreign'),
            lambda e: e['catalog_entries']['consul'].append(copy.deepcopy(e['catalog_entries']['consul'][0])),
            lambda e: e['catalog_entries']['srv-geo'][0]['ServiceMeta'].update(startup_attempt_id=uid(99)),
            lambda e: e['catalog_entries']['srv-geo'][0].update(ServiceAddress='10.42.1.99'),
            lambda e: e['catalog_entries']['srv-geo'].append(copy.deepcopy(e['catalog_entries']['srv-geo'][0])),
            lambda e: e['agent_services'].update(consul={'ID': 'consul', 'Service': 'consul'}),
            lambda e: e['agent_services'].update(frontend={'ID': 'frontend', 'Service': 'srv-frontend'}),
            lambda e: e['catalog_entries'].pop('srv-geo'),
        ):
            changed = copy.deepcopy(evidence)
            mutate(changed)
            with self.subTest(mutation=mutate), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_consul_identity(changed, self.runtime, pods, tuple(pods), execution)
        with self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_consul_identity(evidence, self.runtime, pods, ('geo',), execution)

    def test_partial_catalog_is_exact_for_only_started_backends(self):
        # Catches treating not-yet-started applications as registered or requiring all nine too early.
        pods = self.executions()
        _, _, _, execution = self.network(pods)
        for started, count in (((), 0), (('geo',), 1), (('geo', 'frontend'), 1)):
            with self.subTest(started=started):
                result = identity.validate_startup_hotel_consul_identity(
                    self.consul(pods, execution, started), self.runtime, pods, started, execution)
                self.assertEqual(len(result['backend_registrations']), count)

    def test_consul_dev_loopback_catalog_binds_exact_support_execution(self):
        # Catches conflating the Consul dev agent's advertised node address with its HTTP Pod IP.
        pods = self.executions()
        _, _, _, execution = self.network(pods)
        evidence = self.consul(pods, execution)
        for rows in evidence['catalog_entries'].values():
            rows[0]['Address'] = '127.0.0.1'
        result = identity.validate_startup_hotel_consul_identity(
            evidence, self.runtime, pods, tuple(pods), execution)
        self.assertEqual(result['intrinsic_consul']['pod_ip'], execution['pod_ip'])
        self.assertEqual(result['intrinsic_consul']['catalog_node_address'], '127.0.0.1')
        self.assertEqual(result['backend_registrations']['geo']['pod_ip'], '10.42.1.22')
        for mutate in (
            lambda e: e['catalog_entries']['consul'][0].update(Address=execution['pod_ip']),
            lambda e: e['catalog_entries']['srv-geo'][0].update(Address=execution['pod_ip']),
            lambda e: e['catalog_entries']['consul'][0].update(ServiceKind='connect-proxy'),
            lambda e: e['catalog_entries']['consul'][0].update(ServiceTags=['foreign']),
        ):
            changed = copy.deepcopy(evidence)
            mutate(changed)
            with self.subTest(mutation=mutate), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_consul_identity(changed, self.runtime, pods, tuple(pods), execution)

    def test_saved_or_forged_plan_and_malformed_inputs_are_not_identity(self):
        # Catches accepting historical metadata as original factory proof or leaking raw schema exceptions.
        for plan in (self.runtime.snapshot(), object.__new__(type(self.runtime)), None):
            with self.subTest(plan=type(plan)), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_cri_evidence({}, plan, {})
        for pod in (None, {}, {'metadata': []}, {'spec': {'containers': [None]}}):
            with self.subTest(pod=pod), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_admitted_pod(pod, self.stage.pod('geo'), ROLES)

    def test_endpoint_publication_lag_requires_explicit_opt_in(self):
        # Catches failing healthy controller lag or silently weakening strict default success/output.
        pods = self.executions()
        slices, services, support, _ = self.network(pods)
        strict = identity.validate_startup_hotel_endpoints(slices, services, support, pods)
        observed = identity.validate_startup_hotel_endpoints(slices, services, support, pods, allow_pending=True)
        self.assertTrue(all(row['publication_pending'] is False for row in observed.values()))
        self.assertEqual({name: {k: v for k, v in row.items() if k != 'publication_pending'}
                          for name, row in observed.items()}, strict)
        for form in ('null', 'empty', 'full-ports', 'not-ready'):
            changed = copy.deepcopy(slices)
            geo = next(s for s in changed if s['metadata']['labels']['kubernetes.io/service-name'] == 'geo')
            if form == 'not-ready':
                geo['endpoints'][0]['conditions'] = {'ready': False, 'serving': False, 'terminating': False}
            else:
                geo['endpoints'] = None if form == 'null' else []
                if form != 'full-ports':
                    geo['ports'] = None if form == 'null' else []
            with self.subTest(form=form):
                with self.assertRaises(identity.StartupHotelIdentityError):
                    identity.validate_startup_hotel_endpoints(changed, services, support, pods)
                observed = identity.validate_startup_hotel_endpoints(
                    changed, services, support, pods, allow_pending=True)
                self.assertIs(observed['geo']['publication_pending'], True)
                self.assertIs(observed['user']['publication_pending'], False)
                self.assertEqual(observed['geo']['pod_uid'], pods['geo']['pod_uid'] if form == 'not-ready' else None)

    def test_pending_endpoint_does_not_hide_present_drift_or_support_failure(self):
        # Catches returning on the first pending component before validating hostile evidence elsewhere.
        pods = self.executions()
        slices, services, support, _ = self.network(pods)
        mutations = (
            ('geo', lambda s: s['metadata']['ownerReferences'][0].update(uid=uid(99))),
            ('geo', lambda s: s.update(ports=[{'name': 'grpc', 'port': 9999, 'protocol': 'TCP'}])),
            ('user', lambda s: s['endpoints'][0]['targetRef'].update(uid=uid(99))),
            ('mongodb-user', lambda s: s['endpoints'][0]['conditions'].update(ready=False, serving=False)),
            ('mongodb-user', lambda s: s.update(endpoints=[], ports=[])),
        )
        for name, mutate in mutations:
            changed = copy.deepcopy(slices)
            geo = next(s for s in changed if s['metadata']['labels']['kubernetes.io/service-name'] == 'geo')
            geo.update(ports=[], endpoints=[])
            mutate(next(s for s in changed if s['metadata']['labels']['kubernetes.io/service-name'] == name))
            with self.subTest(component=name), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_endpoints(changed, services, support, pods, allow_pending=True)
        for mutate in (
            lambda s: s['endpoints'][0].update(addresses=['10.42.1.99']),
            lambda s: s['endpoints'][0]['targetRef'].update(uid=uid(99)),
            lambda s: s['endpoints'][0]['conditions'].update(terminating=True),
            lambda s: s['endpoints'][0]['conditions'].update(serving=True),
            lambda s: s['endpoints'][0]['conditions'].pop('serving'),
        ):
            changed = copy.deepcopy(slices)
            geo = next(s for s in changed if s['metadata']['labels']['kubernetes.io/service-name'] == 'geo')
            geo['endpoints'][0]['conditions'] = {'ready': False, 'serving': False, 'terminating': False}
            mutate(geo)
            with self.subTest(mutation=mutate), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_endpoints(changed, services, support, pods, allow_pending=True)
        changed = copy.deepcopy(slices)
        changed[0].update(ports=[], endpoints=[])
        with self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_endpoints(changed + [copy.deepcopy(changed[-1])], services, support, pods,
                                                      allow_pending=True)

    def test_pending_assigned_pod_publication_never_proves_running(self):
        # Catches treating an API Pending Pod with assigned IP and publication as proven execution/readiness.
        pods = self.executions()
        pods['geo'] = self.pending()
        slices, services, support, execution = self.network(pods)
        geo = next(s for s in slices if s['metadata']['labels']['kubernetes.io/service-name'] == 'geo')
        for ready in (False, True):
            geo['endpoints'][0]['conditions'] = {'ready': ready, 'serving': ready, 'terminating': False}
            with self.subTest(ready=ready):
                result = identity.validate_startup_hotel_endpoints(slices, services, support, pods, allow_pending=True)
                self.assertIs(result['geo']['publication_pending'], True)
                self.assertEqual(result['geo']['pod_uid'], pods['geo']['pod_uid'])
        evidence = self.consul(pods, execution)
        with self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_consul_identity(evidence, self.runtime, pods, tuple(pods), execution)
        result = identity.validate_startup_hotel_consul_identity(
            evidence, self.runtime, pods, tuple(pods), execution, allow_pending=True)
        self.assertEqual(result['publication_pending'], ['geo'])
        self.assertNotIn('geo', result['backend_registrations'])
        self.assertEqual(len(result['backend_registrations']), 8)

    def test_unknown_pending_pod_identity_allows_only_absent_publication(self):
        # Catches adopting an endpoint/registration address before this Pod's IP/node can be proven.
        pods = self.executions()
        slices, services, support, execution = self.network(pods)
        complete = self.consul(pods, execution)
        for address, node in ((None, None), ('10.42.1.22', None), (None, ROLES['application'])):
            pods['geo'] = self.pending(address=address, node=node)
            changed = copy.deepcopy(slices)
            geo = next(s for s in changed if s['metadata']['labels']['kubernetes.io/service-name'] == 'geo')
            geo.update(endpoints=[], ports=[])
            with self.subTest(address=address, node=node):
                result = identity.validate_startup_hotel_endpoints(changed, services, support, pods, allow_pending=True)
                self.assertIs(result['geo']['publication_pending'], True)
                absent = copy.deepcopy(complete)
                service_id = next(key for key, value in absent['agent_services'].items() if value['Service'] == 'srv-geo')
                absent['agent_services'].pop(service_id)
                absent['catalog_services'].pop('srv-geo')
                absent['catalog_entries'].pop('srv-geo')
                result = identity.validate_startup_hotel_consul_identity(
                    absent, self.runtime, pods, tuple(pods), execution, allow_pending=True)
                self.assertEqual(result['publication_pending'], ['geo'])
                with self.assertRaises(identity.StartupHotelIdentityError):
                    identity.validate_startup_hotel_endpoints(slices, services, support, pods, allow_pending=True)
                with self.assertRaises(identity.StartupHotelIdentityError):
                    identity.validate_startup_hotel_consul_identity(
                        complete, self.runtime, pods, tuple(pods), execution, allow_pending=True)

    def test_consul_publication_channels_can_lag_independently_without_promotion(self):
        # Catches requiring agent and catalog snapshots to update atomically, or promoting a partial join.
        pods = self.executions()
        _, _, _, execution = self.network(pods)
        evidence = self.consul(pods, execution)
        strict = identity.validate_startup_hotel_consul_identity(evidence, self.runtime, pods, tuple(pods), execution)
        observed = identity.validate_startup_hotel_consul_identity(
            evidence, self.runtime, pods, tuple(pods), execution, allow_pending=True)
        self.assertEqual(observed.pop('publication_pending'), [])
        self.assertEqual(observed, strict)
        service_id = next(key for key, value in evidence['agent_services'].items() if value['Service'] == 'srv-geo')
        for form in ('missing-agent', 'missing-catalog', 'empty-catalog', 'missing-catalog-name', 'all-missing'):
            changed = copy.deepcopy(evidence)
            if form in ('missing-agent', 'all-missing'):
                changed['agent_services'].pop(service_id)
            if form in ('missing-catalog', 'all-missing'):
                changed['catalog_entries'].pop('srv-geo')
            if form in ('missing-catalog-name', 'all-missing'):
                changed['catalog_services'].pop('srv-geo')
            if form == 'empty-catalog':
                changed['catalog_entries']['srv-geo'] = []
            with self.subTest(form=form):
                with self.assertRaises(identity.StartupHotelIdentityError):
                    identity.validate_startup_hotel_consul_identity(changed, self.runtime, pods, tuple(pods), execution)
                result = identity.validate_startup_hotel_consul_identity(
                    changed, self.runtime, pods, tuple(pods), execution, allow_pending=True)
                self.assertEqual(result['publication_pending'], ['geo'])
                self.assertNotIn('geo', result['backend_registrations'])
                self.assertEqual(len(result['backend_registrations']), 8)
        absent = copy.deepcopy(evidence)
        absent['agent_services'] = {}
        absent['catalog_services'] = {'consul': []}
        absent['catalog_entries'] = {'consul': evidence['catalog_entries']['consul']}
        result = identity.validate_startup_hotel_consul_identity(
            absent, self.runtime, pods, tuple(pods), execution, allow_pending=True)
        self.assertEqual(result['publication_pending'], [
            'attractions', 'geo', 'profile', 'rate', 'recommendation', 'reservation', 'review', 'search', 'user'])
        self.assertEqual(result['backend_registrations'], {})

    def test_consul_lag_does_not_hide_any_present_hostile_identity(self):
        # Catches missing-publication shortcuts that ignore stale, foreign, malformed or intrinsic entries.
        pods = self.executions()
        _, _, _, execution = self.network(pods)
        evidence = self.consul(pods, execution)
        geo_id = next(key for key, value in evidence['agent_services'].items() if value['Service'] == 'srv-geo')
        user_id = next(key for key, value in evidence['agent_services'].items() if value['Service'] == 'srv-user')
        for mutate in (
            lambda e: e['agent_services'][user_id]['Meta'].update(pod_uid=uid(99)),
            lambda e: e['catalog_entries']['srv-user'][0].update(ServiceAddress='10.42.1.99'),
            lambda e: e['catalog_entries']['srv-user'].append(copy.deepcopy(e['catalog_entries']['srv-user'][0])),
            lambda e: e['catalog_entries']['srv-geo'][0].update(ServicePort=9999),
            lambda e: e['catalog_services'].update(foreign=[]),
            lambda e: e['agent_services'].update(foreign={'ID': 'foreign', 'Service': 'foreign'}),
            lambda e: e['catalog_entries'].update(foreign=[]),
            lambda e: e['catalog_entries']['consul'].clear(),
            lambda e: e['catalog_services'].pop('consul'),
            lambda e: e['catalog_entries']['consul'][0].update(ServicePort=8500),
        ):
            changed = copy.deepcopy(evidence)
            changed['agent_services'].pop(geo_id)
            mutate(changed)
            with self.subTest(mutation=mutate), self.assertRaises(identity.StartupHotelIdentityError):
                identity.validate_startup_hotel_consul_identity(
                    changed, self.runtime, pods, tuple(pods), execution, allow_pending=True)
        agent_only = copy.deepcopy(evidence)
        agent_only['catalog_entries'].pop('srv-geo')
        agent_only['agent_services'][geo_id]['Meta']['startup_attempt_id'] = uid(99)
        with self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_consul_identity(
                agent_only, self.runtime, pods, tuple(pods), execution, allow_pending=True)

    def test_pending_mode_preserves_running_identity_and_release_requirements(self):
        # Catches treating missing IP on purported Running execution or an unreleased registration as healthy lag.
        pods = self.executions()
        slices, services, support, execution = self.network(pods)
        bad_pods = copy.deepcopy(pods)
        bad_pods['geo']['pod_ip'] = None
        changed = copy.deepcopy(slices)
        geo = next(s for s in changed if s['metadata']['labels']['kubernetes.io/service-name'] == 'geo')
        geo['endpoints'][0]['addresses'] = [None]
        with self.subTest(strict=True), self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_endpoints(changed, services, support, bad_pods)
        geo.update(ports=[], endpoints=[])
        with self.subTest(running=True), self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_endpoints(changed, services, support, bad_pods, allow_pending=True)
        evidence = self.consul(pods, execution)
        geo_id = next(key for key, value in evidence['agent_services'].items() if value['Service'] == 'srv-geo')
        evidence['agent_services'].pop(geo_id)
        bad_pods = copy.deepcopy(pods)
        bad_pods['geo']['released'] = False
        with self.subTest(unreleased=True), self.assertRaises(identity.StartupHotelIdentityError):
            identity.validate_startup_hotel_consul_identity(
                evidence, self.runtime, bad_pods, tuple(pods), execution, allow_pending=True)


if __name__ == '__main__':
    unittest.main()
