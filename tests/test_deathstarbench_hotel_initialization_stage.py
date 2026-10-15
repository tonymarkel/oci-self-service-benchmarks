"""Offline gated Hotel render tests; no cluster, Pod, MongoDB, or executor."""

import copy
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
import unittest

from app import deathstarbench_hotel_initialization_stage as stage
from app.deathstarbench_candidate_runtime import create_candidate_runtime_plan
from app.deathstarbench_candidate_stage import build_candidate_support_stage
from app.deathstarbench_hotel_runtime import create_guarded_hotel_runtime_plan
from tests import test_deathstarbench_hotel_initialization_artifacts as artifact_fixtures


ATTEMPT = '12345678-1234-4234-8234-123456789abc'
UID = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'


class GuardedHotelInitializationStageTests(unittest.TestCase):
    def plan(self, architecture='aarch64', attempt=ATTEMPT):
        fixture = artifact_fixtures.GuardedHotelArtifactsTests()
        self.addCleanup(fixture.doCleanups)
        data = fixture.fixture()
        guarded = fixture.adopt(data)
        support = create_candidate_runtime_plan(
            data['baseline'], run_id='a1b2c3d4e5f6', provider='oci',
            topology_fingerprint='sha256:' + '1' * 64,
            application_architecture=architecture, load_generator_ip='10.2.3.4',
            filesystem_uuid='12345678-1234-4234-8234-123456789abc',
        )
        return create_guarded_hotel_runtime_plan(
            guarded, support, build_candidate_support_stage(support),
            initialization_attempt_id=attempt,
        )

    def test_exact_two_standalone_gated_never_restart_pods_no_job_or_service(self):
        for architecture in ('x86_64', 'aarch64'):
            plan = self.plan(architecture)
            bundle = stage.build_guarded_hotel_initialization_stage(plan)
            items = json.loads(bundle.canonical_json)['items']
            self.assertEqual(len(items), 11)
            self.assertEqual(sum(item['kind'] == 'Pod' for item in items), 2)
            self.assertEqual(sum(item['kind'] == 'NetworkPolicy' for item in items), 9)
            self.assertTrue(all(item['kind'] in {'Pod', 'NetworkPolicy'} for item in items))
            self.assertEqual({item['metadata']['namespace'] for item in items}, {'deathstarbench-hotel'})
            for mode in stage.MODES:
                pod = bundle.pod(mode)
                spec = pod['spec']
                self.assertEqual(spec['schedulingGates'], [{'name': stage.SCHEDULING_GATE}])
                self.assertEqual(spec['restartPolicy'], 'Never')
                self.assertEqual(spec['activeDeadlineSeconds'], 420)
                self.assertEqual(spec['terminationGracePeriodSeconds'], 10)
                self.assertEqual(spec['nodeSelector'], {
                    'deathstarbench.io/role': 'application',
                    'kubernetes.io/arch': 'arm64' if architecture == 'aarch64' else 'amd64',
                })
                self.assertEqual(len(spec['containers']), 1)
                container = spec['containers'][0]
                self.assertEqual(container['image'], plan.application_image)
                self.assertEqual(container['imagePullPolicy'], 'Always')
                self.assertEqual(container['command'], ['/go/bin/hotel-initialize'])
                self.assertEqual(container['args'], [mode])
                self.assertEqual(container['workingDir'], '/workspace')
                self.assertEqual(container['env'], [{'name': key, 'value': value}
                                 for key, value in sorted(plan.initialization_environment().items())])
                self.assertEqual({item['name'] for item in container['env']}, {
                    'HOTEL_CANDIDATE_RUN_ID', 'HOTEL_CANDIDATE_CONTRACT_SHA256',
                })
                for forbidden in ('volumes', 'initContainers', 'ephemeralContainers', 'imagePullSecrets',
                                  'hostAliases', 'affinity', 'serviceAccount', 'serviceAccountName'):
                    self.assertNotIn(forbidden, spec)
                self.assertNotIn('volumeMounts', container)
                self.assertNotIn('ports', container)
                self.assertNotIn('livenessProbe', container)
                self.assertNotIn('readinessProbe', container)

    def test_rootless_readonly_no_token_or_privileged_access_and_bounded_resources(self):
        bundle = stage.build_guarded_hotel_initialization_stage(self.plan())
        for mode in stage.MODES:
            spec = bundle.pod(mode)['spec']
            for flag in ('automountServiceAccountToken', 'enableServiceLinks', 'hostNetwork', 'hostPID',
                         'hostIPC', 'shareProcessNamespace'):
                self.assertIs(spec[flag], False)
            self.assertEqual(spec['securityContext'], {
                'runAsNonRoot': True, 'runAsUser': 65532, 'runAsGroup': 65532,
                'seccompProfile': {'type': 'RuntimeDefault'},
            })
            self.assertEqual(spec['containers'][0]['securityContext'], {
                'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                'capabilities': {'drop': ['ALL']},
            })
            self.assertEqual(spec['containers'][0]['resources'], {
                'requests': {'cpu': '100m', 'memory': '64Mi'},
                'limits': {'cpu': '1', 'memory': '256Mi'},
            })

    def test_only_exact_eight_mongo_and_coredns_edges_with_matching_ingress(self):
        bundle = stage.build_guarded_hotel_initialization_stage(self.plan())
        policies = [item for item in json.loads(bundle.canonical_json)['items'] if item['kind'] == 'NetworkPolicy']
        outbound = next(item for item in policies if item['metadata']['name'] == 'hotel-initialization-egress-and-deny-ingress')
        spec = outbound['spec']
        self.assertEqual(spec['policyTypes'], ['Ingress', 'Egress'])
        self.assertEqual(spec['ingress'], [])
        self.assertEqual(len(spec['egress']), 9)
        mongo = spec['egress'][:8]
        self.assertEqual([item['to'][0]['podSelector']['matchLabels']['app.kubernetes.io/component'] for item in mongo],
                         ['mongodb-' + component for component in stage.COMPONENTS])
        for rule in mongo:
            self.assertEqual(set(rule), {'to', 'ports'})
            self.assertEqual(len(rule['to']), 1)
            self.assertEqual(set(rule['to'][0]), {'podSelector'})
            self.assertEqual(rule['ports'], [{'port': 27017, 'protocol': 'TCP'}])
        self.assertEqual(spec['egress'][-1], {
            'to': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'kube-system'}},
                    'podSelector': {'matchLabels': {'k8s-app': 'kube-dns'}}}],
            'ports': [{'port': 53, 'protocol': 'TCP'}, {'port': 53, 'protocol': 'UDP'}],
        })
        inbound = [item for item in policies if item is not outbound]
        self.assertEqual(len(inbound), 8)
        for component, policy in zip(stage.COMPONENTS, inbound, strict=True):
            self.assertEqual(policy['spec']['podSelector'], mongo[stage.COMPONENTS.index(component)]['to'][0]['podSelector'])
            self.assertEqual(policy['spec']['policyTypes'], ['Ingress'])
            self.assertEqual(policy['spec']['ingress'], [{'from': [{'podSelector': spec['podSelector']}],
                                                         'ports': [{'port': 27017, 'protocol': 'TCP'}]}])
        encoded = json.dumps([item['spec'] for item in policies])
        for forbidden in ('ipBlock', 'consul', 'jaeger', 'frontend', 'memcached', '0.0.0.0', 'namespaceSelector": {}'):
            self.assertNotIn(forbidden, encoded)

    def test_original_support_stage_and_all_release_and_start_flags_remain_unchanged(self):
        plan = self.plan()
        original = plan.support_stage.canonical_json
        bundle = stage.build_guarded_hotel_initialization_stage(plan)
        self.assertEqual(plan.support_stage.canonical_json, original)
        for item in json.loads(original)['items']:
            if item['kind'] == 'Deployment' and item['metadata']['name'] in plan.support_stage.blocked_application_components:
                self.assertEqual(item['spec']['replicas'], 0)
        metadata = bundle.metadata()
        for name in stage._GATES:
            self.assertIs(metadata[name], False)
        self.assertIs(bundle.application_start_allowed, False)
        self.assertIs(bundle.initializer_authorized, False)
        self.assertIs(bundle.released, False)
        self.assertIs(bundle.candidate_only, True)
        self.assertEqual(metadata['application_deployments_included'], 0)
        self.assertIn('offline', metadata['scope'])
        self.assertIn('never-replay', metadata['recovery'])
        self.assertNotEqual(bundle.fingerprint, plan.fingerprint)
        self.assertEqual(bundle.rendered_manifest_sha256,
                         'sha256:' + hashlib.sha256(bundle.canonical_json.encode()).hexdigest())

    def test_immutable_rederivation_and_copy_safe_accessors(self):
        plan = self.plan()
        bundle = stage.build_guarded_hotel_initialization_stage(plan)
        self.assertEqual(stage.build_guarded_hotel_initialization_stage(plan), bundle)
        self.assertEqual(stage.validate_guarded_hotel_initialization_stage(plan, bundle), bundle)
        self.assertEqual(replace(bundle), bundle)
        with self.assertRaises(FrozenInstanceError):
            bundle.canonical_json = '{}'
        pod = bundle.pod('initialize-empty')
        pod['spec']['containers'][0]['args'] = ['verify']
        metadata = bundle.metadata()
        metadata['application_start_allowed'] = True
        self.assertEqual(bundle.pod('initialize-empty')['spec']['containers'][0]['args'], ['initialize-empty'])
        self.assertIs(bundle.metadata()['application_start_allowed'], False)
        for fields in ({'run_id': '112233445566'}, {'namespace': 'default'}, {'fingerprint': 'sha256:' + 'a' * 64},
                       {'plan_fingerprint': plan.support_plan.fingerprint}, {'_metadata_json': '{}'},
                       {'rendered_manifest_sha256': plan.support_stage.rendered_manifest_sha256}):
            with self.subTest(fields=fields), self.assertRaises(stage.GuardedHotelInitializationStageError):
                replace(bundle, **fields)

    def test_extra_pod_job_network_env_args_shape_and_image_cannot_be_rehashed_into_stage(self):
        plan = self.plan()
        bundle = stage.build_guarded_hotel_initialization_stage(plan)
        original = json.loads(bundle.canonical_json)
        pod_index = next(index for index, item in enumerate(original['items']) if item['kind'] == 'Pod')
        changes = (
            lambda doc: doc['items'].append(copy.deepcopy(doc['items'][pod_index])),
            lambda doc: doc['items'].append({'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': 'replay'}}),
            lambda doc: doc['items'][0]['spec']['egress'].append({'to': [{'ipBlock': {'cidr': '0.0.0.0/0'}}]}),
            lambda doc: doc['items'][pod_index]['spec']['containers'][0]['env'].append({'name': 'WRITE_URI', 'value': 'other'}),
            lambda doc: doc['items'][pod_index]['spec']['containers'][0].update(args=['initialize-empty', '--retry']),
            lambda doc: doc['items'][pod_index]['spec']['containers'][0].update(image=plan.support_plan.artifacts.runtime_manifest['platforms']['linux/amd64']['images']['hotel-reservation']),
            lambda doc: doc['items'][pod_index]['spec']['nodeSelector'].update({'kubernetes.io/arch': 'amd64'}),
            lambda doc: doc['items'][pod_index]['spec'].update(restartPolicy='OnFailure'),
            lambda doc: doc['items'][pod_index]['spec'].pop('schedulingGates'),
        )
        for change in changes:
            changed = copy.deepcopy(original)
            change(changed)
            payload = stage._canonical(changed)
            with self.subTest(change=change), self.assertRaises(stage.GuardedHotelInitializationStageError):
                replace(bundle, canonical_json=payload,
                        rendered_manifest_sha256='sha256:' + hashlib.sha256(payload.encode()).hexdigest())

    def test_cross_plan_or_serialized_stage_cannot_be_adopted(self):
        plan = self.plan()
        bundle = stage.build_guarded_hotel_initialization_stage(plan)
        for other in (self.plan(architecture='x86_64'), self.plan(attempt='aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee')):
            self.assertNotEqual(stage.build_guarded_hotel_initialization_stage(other).fingerprint, bundle.fingerprint)
            with self.assertRaises(stage.GuardedHotelInitializationStageError):
                stage.validate_guarded_hotel_initialization_stage(other, bundle)
        for value in (None, {}, plan.snapshot(), plan.support_plan, plan.support_stage, plan.artifacts):
            with self.subTest(value=type(value)), self.assertRaises(stage.GuardedHotelInitializationStageError):
                stage.build_guarded_hotel_initialization_stage(value)
        for value in (None, {}, bundle.metadata(), plan.support_stage):
            with self.subTest(value=type(value)), self.assertRaises(stage.GuardedHotelInitializationStageError):
                stage.validate_guarded_hotel_initialization_stage(plan, value)
        for mode in (None, True, '', 'initialize', 'verify ', 'repair', 'reset', 'initialize-empty --retry'):
            with self.subTest(mode=mode), self.assertRaises(stage.GuardedHotelInitializationStageError):
                bundle.pod(mode)

    def test_offline_release_patch_has_exact_uid_and_whole_spec_tests_before_single_gate_removal(self):
        plan = self.plan()
        bundle = stage.build_guarded_hotel_initialization_stage(plan)
        for mode in stage.MODES:
            pod = bundle.pod(mode)
            patch = stage.guarded_hotel_scheduling_gate_release_patch(plan, bundle, mode, UID)
            self.assertEqual(patch[0], {'op': 'test', 'path': '/metadata/uid', 'value': UID})
            self.assertEqual(patch[-1], {'op': 'remove', 'path': '/spec/schedulingGates'})
            self.assertTrue(all(item['op'] == 'test' for item in patch[:-1]))
            tests = {item['path']: item['value'] for item in patch[:-1]}
            self.assertEqual(tests['/spec'], pod['spec'])
            self.assertEqual(tests['/metadata/name'], pod['metadata']['name'])
            self.assertEqual(tests['/metadata/namespace'], 'deathstarbench-hotel')
            self.assertEqual(tests['/metadata/labels'], pod['metadata']['labels'])
            self.assertEqual(tests['/metadata/annotations'], pod['metadata']['annotations'])
            patch[0]['value'] = 'not-retained'
            self.assertEqual(stage.guarded_hotel_scheduling_gate_release_patch(plan, bundle, mode, UID)[0]['value'], UID)
        for uid in (None, True, '', '00000000-0000-0000-0000-000000000000', UID.upper(), 'aaaa', UID + '/other'):
            with self.subTest(uid=uid), self.assertRaises(stage.GuardedHotelInitializationStageError):
                stage.guarded_hotel_scheduling_gate_release_patch(plan, bundle, 'initialize-empty', uid)


if __name__ == '__main__':
    unittest.main()
