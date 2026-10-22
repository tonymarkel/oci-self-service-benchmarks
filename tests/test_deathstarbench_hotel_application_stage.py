"""Guarded application staging is deterministic and grants no live authority."""

import copy
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
import unittest

from app import deathstarbench_hotel_application_stage as stage
from tests import test_deathstarbench_hotel_initialization_stage as fixtures


class GuardedHotelApplicationStageTests(unittest.TestCase):
    def plan(self, architecture='x86_64', attempt=fixtures.ATTEMPT):
        fixture = fixtures.GuardedHotelInitializationStageTests()
        self.addCleanup(fixture.doCleanups)
        return fixture.plan(architecture, attempt)

    def test_ten_gated_services_use_guarded_platform_image_and_dataset_identity(self):
        for architecture in ('x86_64', 'aarch64'):
            plan = self.plan(architecture)
            bundle = stage.build_guarded_hotel_application_stage(plan)
            pods = json.loads(bundle.canonical_json)['items']
            self.assertEqual(len(pods), 10)
            self.assertEqual({pod['kind'] for pod in pods}, {'Pod'})
            self.assertEqual({pod['metadata']['namespace'] for pod in pods}, {plan.namespace})
            self.assertEqual({pod['spec']['containers'][0]['name'] for pod in pods}, {
                'attractions', 'frontend', 'geo', 'profile', 'rate',
                'recommendation', 'reservation', 'review', 'search', 'user',
            })
            self.assertEqual(len({pod['metadata']['name'] for pod in pods}), 10)
            for pod in pods:
                spec = pod['spec']
                container = spec['containers'][0]
                component = container['name']
                self.assertEqual(spec['schedulingGates'], [{'name': stage.SCHEDULING_GATE}])
                self.assertEqual(spec['restartPolicy'], 'Never')
                self.assertEqual(spec['activeDeadlineSeconds'], 900)
                self.assertEqual(spec['nodeSelector'], {
                    'deathstarbench.io/role': 'application',
                    'kubernetes.io/arch': 'amd64' if architecture == 'x86_64' else 'arm64',
                })
                self.assertEqual(container['image'], plan.application_image)
                self.assertEqual(container['imagePullPolicy'], 'Always')
                self.assertEqual(container['command'], ['/go/bin/' + component])
                self.assertEqual(container['workingDir'], '/workspace')
                env = {item['name']: item['value'] for item in container['env']}
                self.assertEqual(len(env), len(container['env']))
                self.assertEqual(env['HOTEL_CANDIDATE_RUN_ID'], plan.run_id)
                self.assertEqual(env['HOTEL_CANDIDATE_CONTRACT_SHA256'], plan.snapshot()['dataset_fingerprint'])
                self.assertEqual(env['TLS'], '0')
                self.assertNotIn('args', container)
                self.assertNotIn('ownerReferences', pod['metadata'])
                self.assertNotIn('pod-template-hash', pod['metadata']['labels'])
                self.assertNotIn('deathstarbench.io/hotel-initialization', pod['metadata']['labels'])

    def test_services_and_network_policies_route_without_changing_support_stage(self):
        plan = self.plan()
        before = plan.support_stage.canonical_json
        bundle = stage.build_guarded_hotel_application_stage(plan)
        support = json.loads(before)['items']
        services = {item['metadata']['name']: item for item in support if item['kind'] == 'Service'}
        deployments = {item['metadata']['name']: item for item in support if item['kind'] == 'Deployment'}
        policies = {item['metadata']['name']: item for item in support if item['kind'] == 'NetworkPolicy'}
        for component in bundle.components:
            pod = bundle.pod(component)
            labels = pod['metadata']['labels']
            selectors = [services[component]['spec']['selector'],
                         policies['allow-egress-from-' + component]['spec']['podSelector']['matchLabels']]
            for selector in selectors:
                self.assertTrue(all(labels.get(key) == value for key, value in selector.items()))
            self.assertEqual(deployments[component]['spec']['replicas'], 0)
            original = deployments[component]['spec']['template']['spec']['containers'][0]
            actual = pod['spec']['containers'][0]
            self.assertEqual(actual['ports'], original['ports'])
            self.assertEqual(actual['readinessProbe'], original['readinessProbe'])
            self.assertTrue(all(item in actual['env'] for item in original['env']))
            # Deployment-generated ReplicaSets additionally select their hash;
            # standalone Pods must never carry that controller-owned label.
            replica_selector = {**deployments[component]['spec']['selector']['matchLabels'],
                                'pod-template-hash': 'abc123'}
            self.assertFalse(all(labels.get(key) == value for key, value in replica_selector.items()))
        self.assertEqual(plan.support_stage.canonical_json, before)

    def test_backend_search_frontend_groups_respect_audited_dependencies(self):
        plan = self.plan()
        bundle = stage.build_guarded_hotel_application_stage(plan)
        groups = bundle.startup_groups
        self.assertEqual(groups, (
            ('attractions', 'geo', 'profile', 'rate', 'recommendation', 'reservation', 'review', 'user'),
            ('search',), ('frontend',),
        ))
        self.assertEqual(tuple(sorted(name for group in groups for name in group)), bundle.components)
        metadata = bundle.metadata()
        self.assertEqual(metadata['startup_groups'], [list(group) for group in groups])
        self.assertEqual(metadata['consul_services'], {
            'attractions': {'name': 'srv-attractions', 'port': 8089},
            'geo': {'name': 'srv-geo', 'port': 8083},
            'profile': {'name': 'srv-profile', 'port': 8081},
            'rate': {'name': 'srv-rate', 'port': 8084},
            'recommendation': {'name': 'srv-recommendation', 'port': 8085},
            'reservation': {'name': 'srv-reservation', 'port': 8087},
            'review': {'name': 'srv-review', 'port': 8088},
            'search': {'name': 'srv-search', 'port': 8082},
            'user': {'name': 'srv-user', 'port': 8086},
        })
        self.assertIn('not-semantic-readiness', metadata['readiness_scope'])

    def test_pods_are_rootless_bounded_and_cannot_mount_or_restart(self):
        bundle = stage.build_guarded_hotel_application_stage(self.plan())
        for component in bundle.components:
            spec = bundle.pod(component)['spec']
            for flag in ('automountServiceAccountToken', 'enableServiceLinks', 'hostNetwork',
                         'hostPID', 'hostIPC', 'shareProcessNamespace'):
                self.assertIs(spec[flag], False)
            self.assertEqual(spec['securityContext'], {
                'runAsNonRoot': True, 'runAsUser': 65532, 'runAsGroup': 65532,
                'seccompProfile': {'type': 'RuntimeDefault'},
            })
            container = spec['containers'][0]
            self.assertEqual(container['securityContext'], {
                'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                'capabilities': {'drop': ['ALL']},
            })
            self.assertEqual(container['resources'], {
                'requests': {'cpu': '100m', 'memory': '64Mi'},
                'limits': {'cpu': '1', 'memory': '512Mi'},
            })
            for key in ('volumes', 'initContainers', 'ephemeralContainers', 'imagePullSecrets'):
                self.assertNotIn(key, spec)
            for key in ('volumeMounts', 'livenessProbe', 'startupProbe', 'lifecycle'):
                self.assertNotIn(key, container)

    def test_render_and_metadata_never_claim_execution_or_reuse_authority(self):
        plan = self.plan()
        bundle = stage.build_guarded_hotel_application_stage(plan)
        metadata = bundle.metadata()
        self.assertIs(bundle.candidate_only, True)
        self.assertIs(bundle.application_start_allowed, False)
        self.assertIs(bundle.released, False)
        for flag in ('application_start_allowed', 'initialization_verified', 'runtime_qualified',
                     'measurement_qualified', 'cloud_qualified', 'public_release', 'released'):
            self.assertIs(metadata[flag], False)
        self.assertEqual(metadata['execution_authority'], 'none')
        self.assertEqual(metadata['recovery'], 'cleanup-only-no-restart-or-replay')
        self.assertEqual(metadata['plan_fingerprint'], plan.fingerprint)
        self.assertEqual(metadata['initialization_attempt_id'], plan.initialization_attempt_id)
        for pod in json.loads(bundle.canonical_json)['items']:
            annotations = pod['metadata']['annotations']
            self.assertEqual(annotations['deathstarbench.io/application-start-allowed'], 'false')
            self.assertEqual(annotations['deathstarbench.io/hotel-runtime-fingerprint'], plan.fingerprint)
            self.assertEqual(annotations['deathstarbench.io/hotel-application-stage-fingerprint'], bundle.fingerprint)

    def test_frozen_exact_derivation_and_copy_safe_accessors(self):
        plan = self.plan()
        bundle = stage.build_guarded_hotel_application_stage(plan)
        self.assertEqual(stage.validate_guarded_hotel_application_stage(plan, bundle), bundle)
        self.assertEqual(replace(bundle), bundle)
        with self.assertRaises(FrozenInstanceError):
            bundle.canonical_json = '{}'
        pod = bundle.pod('frontend')
        pod['spec']['containers'][0]['image'] = 'untrusted:latest'
        bundle.metadata()['consul_services']['user']['port'] = 1
        self.assertEqual(bundle.pod('frontend')['spec']['containers'][0]['image'], plan.application_image)
        self.assertEqual(bundle.metadata()['consul_services']['user']['port'], 8086)
        for field, value in (
            ('run_id', '112233445566'), ('namespace', 'default'), ('_metadata_json', '{}'),
            ('plan_fingerprint', plan.support_plan.fingerprint), ('fingerprint', 'sha256:' + '0' * 64),
            ('components', ('frontend',)), ('startup_groups', (('frontend',),)),
        ):
            with self.subTest(field=field), self.assertRaises(stage.GuardedHotelApplicationStageError):
                replace(bundle, **{field: value})

    def test_cross_plan_serialized_or_tampered_inputs_are_refused(self):
        plan = self.plan()
        bundle = stage.build_guarded_hotel_application_stage(plan)
        for value in (None, {}, plan.snapshot(), plan.support_plan, plan.support_stage):
            with self.subTest(value=type(value)), self.assertRaises(stage.GuardedHotelApplicationStageError):
                stage.build_guarded_hotel_application_stage(value)
        for value in (None, {}, bundle.metadata(), plan.support_stage):
            with self.subTest(value=type(value)), self.assertRaises(stage.GuardedHotelApplicationStageError):
                stage.validate_guarded_hotel_application_stage(plan, value)
        for other in (self.plan('aarch64'), self.plan(attempt=fixtures.UID)):
            with self.assertRaises(stage.GuardedHotelApplicationStageError):
                stage.validate_guarded_hotel_application_stage(other, bundle)
            other_names = {stage.build_guarded_hotel_application_stage(other).pod(c)['metadata']['name']
                           for c in bundle.components}
            if other.initialization_attempt_id != plan.initialization_attempt_id:
                self.assertTrue(other_names.isdisjoint({bundle.pod(c)['metadata']['name'] for c in bundle.components}))
        for component in (None, True, '', 'mongodb-user', 'hotel-initialize', 'Frontend', 'user --repair'):
            with self.subTest(component=component), self.assertRaises(stage.GuardedHotelApplicationStageError):
                bundle.pod(component)
        corrupted = copy.copy(plan)
        object.__setattr__(corrupted, 'application_start_allowed', True)
        with self.assertRaises(stage.GuardedHotelApplicationStageError):
            stage.build_guarded_hotel_application_stage(corrupted)

    def test_rehashing_altered_pods_cannot_bypass_derivation(self):
        bundle = stage.build_guarded_hotel_application_stage(self.plan())
        def container(doc):
            return doc['items'][0]['spec']['containers'][0]
        changes = (
            lambda d: d['items'].append(copy.deepcopy(d['items'][0])),
            lambda d: d['items'][0]['spec'].pop('schedulingGates'),
            lambda d: d['items'][0]['spec'].update(restartPolicy='Always'),
            lambda d: d['items'][0]['spec'].update(hostNetwork=True),
            lambda d: d['items'][0]['spec']['nodeSelector'].update({'kubernetes.io/arch': 'arm64'}),
            lambda d: d['items'][0]['metadata']['labels'].update({'pod-template-hash': 'abc'}),
            lambda d: container(d).update(image='other:latest'),
            lambda d: container(d).update(command=['/go/bin/hotel-initialize']),
            lambda d: container(d)['env'].append({'name': 'WRITE_URI', 'value': 'foreign'}),
            lambda d: container(d)['resources']['limits'].update(memory='64Gi'),
        )
        for change in changes:
            doc = json.loads(bundle.canonical_json)
            change(doc)
            payload = json.dumps(doc, sort_keys=True, separators=(',', ':')) + '\n'
            with self.subTest(change=change), self.assertRaises(stage.GuardedHotelApplicationStageError):
                replace(bundle, canonical_json=payload,
                        rendered_manifest_sha256='sha256:' + hashlib.sha256(payload.encode()).hexdigest())

    def test_revalidation_rejects_boolean_aliases_in_bypassed_frozen_fields(self):
        plan = self.plan()
        bundle = stage.build_guarded_hotel_application_stage(plan)
        for field, value in (('application_start_allowed', 0), ('released', 0), ('candidate_only', 1)):
            changed = copy.copy(bundle)
            object.__setattr__(changed, field, value)
            with self.subTest(field=field), self.assertRaises(stage.GuardedHotelApplicationStageError):
                stage.validate_guarded_hotel_application_stage(plan, changed)

    def test_plan_flags_require_exact_booleans_even_after_frozen_field_bypass(self):
        plan = self.plan()
        for field, value in (('application_start_allowed', 0), ('initializer_authorized', 0),
                             ('runtime_authorized', 0), ('released', 0), ('candidate_only', 1)):
            changed = copy.copy(plan)
            object.__setattr__(changed, field, value)
            with self.subTest(field=field), self.assertRaises(stage.GuardedHotelApplicationStageError):
                stage.build_guarded_hotel_application_stage(changed)


if __name__ == '__main__':
    unittest.main()
