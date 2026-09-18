import copy
import hashlib
import json
from pathlib import Path
from types import MappingProxyType
import unittest

from app.deathstarbench_contract import (
    DISTRIBUTED_IMAGE_SET_REVISION,
    DISTRIBUTED_WORKLOAD_REVISION,
)
from app.deathstarbench_k3s_workload import (
    ASSET_DIRECTORY,
    AUDITED_COMPONENT_EDGES,
    CPP_ENTRYPOINTS,
    DATABASE_ROOT,
    EXPECTED_COMPONENTS,
    EXPECTED_NETWORK_POLICIES,
    FRONTEND_COMPONENT,
    FRONTEND_NODE_PORT,
    IMAGE_LOCK_SCHEMA_VERSION,
    MEMCACHED_COMPONENTS,
    MONGODB_COMPONENTS,
    NAMESPACE,
    REDIS_COMPONENTS,
    REQUIRED_IMAGE_KEYS,
    UPSTREAM_REVISION,
    RenderedSocialNetworkBundle,
    WorkloadBundleError,
    parse_workload_attestation,
    render_social_network_bundle,
    render_workload_bundle,
    render_workload_documents,
    render_workload_json,
    validate_image_lock,
    workload_apply_command,
    workload_readiness_command,
)


def image_lock():
    platforms = {}
    for platform in ('linux/amd64', 'linux/arm64'):
        platforms[platform] = {
            'images': {
                key: (
                    f'registry.example/deathstarbench/{key}@sha256:'
                    + hashlib.sha256(f'{platform}:{key}'.encode()).hexdigest()
                )
                for key in REQUIRED_IMAGE_KEYS
            },
        }
    return {
        'schema_version': IMAGE_LOCK_SCHEMA_VERSION,
        'workload_revision': DISTRIBUTED_WORKLOAD_REVISION,
        'image_set_revision': DISTRIBUTED_IMAGE_SET_REVISION,
        'upstream_revision': UPSTREAM_REVISION,
        'released': False,
        'platforms': platforms,
    }


def phase_items(payload):
    parsed = json.loads(payload)
    assert parsed['apiVersion'] == 'v1'
    assert parsed['kind'] == 'List'
    return parsed['items']


def by_kind(bundle):
    values = {}
    for value in json.loads(bundle.canonical_json)['items']:
        values.setdefault(value['kind'], {})[value['metadata']['name']] = value
    return values


def fake_attestation(bundle):
    rendered = by_kind(bundle)
    items = []
    for kind in (
        'Deployment',
        'Service',
        'PersistentVolume',
        'PersistentVolumeClaim',
        'NetworkPolicy',
    ):
        for value in rendered[kind].values():
            item = copy.deepcopy(value)
            if kind == 'Deployment':
                item['status'] = {
                    'availableReplicas': 1,
                    'readyReplicas': 1,
                    'updatedReplicas': 1,
                }
            elif kind in {'PersistentVolume', 'PersistentVolumeClaim'}:
                item['status'] = {'phase': 'Bound'}
            items.append(item)

    for name, deployment in rendered['Deployment'].items():
        template = deployment['spec']['template']
        image = template['spec']['containers'][0]['image']
        role = template['spec']['nodeSelector']['deathstarbench.io/role']
        items.append({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'labels': copy.deepcopy(template['metadata']['labels']),
                'name': f'{name}-candidate-pod',
                'namespace': NAMESPACE,
            },
            'spec': {
                'containers': copy.deepcopy(template['spec']['containers']),
                'nodeName': f'dsb-{role}',
                'nodeSelector': {'deathstarbench.io/role': role},
            },
            'status': {
                'conditions': [{'status': 'True', 'type': 'Ready'}],
                'containerStatuses': [{
                    'image': image,
                    'imageID': f'docker-pullable://{image}',
                    'name': name,
                    'ready': True,
                }],
                'phase': 'Running',
            },
        })
    return {'apiVersion': 'v1', 'items': items, 'kind': 'List'}


class ImageLockTests(unittest.TestCase):
    def test_exact_two_platform_digest_lock_is_accepted(self):
        validated = validate_image_lock(image_lock())

        self.assertEqual(set(validated.platforms), {'linux/amd64', 'linux/arm64'})
        self.assertRegex(validated.fingerprint, r'^sha256:[0-9a-f]{64}$')
        self.assertIn(
            '@sha256:',
            validated.image('aarch64', 'social-network-microservices'),
        )
        with self.assertRaises(TypeError):
            validated.platforms['linux/amd64']['redis'] = 'mutable'

    def test_tags_latest_missing_platforms_and_unknown_fields_fail_closed(self):
        cases = []

        tagged = image_lock()
        tagged['platforms']['linux/amd64']['images']['redis'] = (
            'registry.example/deathstarbench/redis:7.2'
        )
        cases.append(tagged)

        latest_with_digest = image_lock()
        latest_with_digest['platforms']['linux/amd64']['images']['redis'] = (
            'registry.example/deathstarbench/redis:latest@sha256:' + 'a' * 64
        )
        cases.append(latest_with_digest)

        missing_platform = image_lock()
        del missing_platform['platforms']['linux/arm64']
        cases.append(missing_platform)

        extra = image_lock()
        extra['channel'] = 'stable'
        cases.append(extra)

        released = image_lock()
        released['released'] = True
        cases.append(released)

        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(WorkloadBundleError):
                    validate_image_lock(value)

    def test_platforms_require_same_repository_and_distinct_references(self):
        mismatch = image_lock()
        mismatch['platforms']['linux/arm64']['images']['redis'] = (
            'registry.example/other/redis@sha256:' + 'a' * 64
        )
        with self.assertRaisesRegex(WorkloadBundleError, 'same repository'):
            validate_image_lock(mismatch)

        duplicate = image_lock()
        duplicate['platforms']['linux/amd64']['images']['jaeger'] = (
            duplicate['platforms']['linux/amd64']['images']['redis']
        )
        with self.assertRaisesRegex(WorkloadBundleError, 'distinct'):
            validate_image_lock(duplicate)


class WorkloadRenderTests(unittest.TestCase):
    def setUp(self):
        self.lock = image_lock()
        self.bundle = render_social_network_bundle(
            self.lock,
            'aarch64',
            '10.240.2.10',
        )

    def test_checked_in_assets_are_unreleased_and_contain_no_image_digests(self):
        self.assertEqual(
            {path.name for path in ASSET_DIRECTORY.iterdir()},
            {'components.json', 'network-policies.json'},
        )
        combined = ''.join(
            path.read_text()
            for path in sorted(ASSET_DIRECTORY.iterdir())
            if path.is_file()
        )
        self.assertIn('"released": false', combined)
        self.assertNotIn('@sha256:', combined)
        self.assertNotIn(':latest', combined)
        self.assertNotIn('server.key', combined)
        self.assertNotIn('server.pem', combined)

    def test_render_is_canonical_deterministic_and_split_into_fixed_phases(self):
        again = render_social_network_bundle(
            copy.deepcopy(self.lock),
            'arm64',
            '10.240.2.10',
        )

        self.assertIsInstance(self.bundle, RenderedSocialNetworkBundle)
        self.assertEqual(self.bundle, again)
        self.assertEqual(self.bundle.namespace, NAMESPACE)
        self.assertEqual(self.bundle.application_architecture, 'aarch64')
        self.assertEqual(self.bundle.load_generator_cidr, '10.240.2.10/32')
        self.assertRegex(
            self.bundle.manifest_source_sha256,
            r'^sha256:[0-9a-f]{64}$',
        )
        self.assertRegex(
            self.bundle.rendered_manifest_sha256,
            r'^sha256:[0-9a-f]{64}$',
        )
        self.assertEqual(
            [len(phase_items(value)) for value in self.bundle.phase_payloads],
            [1, 12, 39, 27, 27],
        )
        self.assertEqual(
            [item['kind'] for item in phase_items(self.bundle.namespace_manifest)],
            ['Namespace'],
        )
        self.assertEqual(
            {item['kind'] for item in phase_items(self.bundle.storage)},
            {'PersistentVolume', 'PersistentVolumeClaim'},
        )
        self.assertEqual(
            self.bundle.phase_names,
            ('namespace', 'storage', 'policies', 'services', 'workloads'),
        )
        self.assertEqual(
            self.bundle.phases,
            tuple(zip(
                self.bundle.phase_names,
                self.bundle.phase_payloads,
                strict=True,
            )),
        )
        self.assertEqual(len(json.loads(self.bundle.canonical_json)['items']), 106)
        for payload in (*self.bundle.phase_payloads, self.bundle.canonical_json):
            self.assertEqual(
                payload,
                json.dumps(
                    json.loads(payload),
                    sort_keys=True,
                    separators=(',', ':'),
                ) + '\n',
            )
        self.assertEqual(
            self.bundle.phase_sha256('namespace'),
            hashlib.sha256(self.bundle.namespace_manifest.encode()).hexdigest(),
        )
        self.assertEqual(
            self.bundle.phase_payload('storage'),
            self.bundle.storage,
        )
        with self.assertRaises(WorkloadBundleError):
            self.bundle.phase_payload('foundation')
        self.assertEqual(
            render_workload_json(self.lock, 'arm64', '10.240.2.10/32'),
            self.bundle.canonical_json,
        )
        self.assertEqual(
            render_workload_documents(self.lock, 'arm64', '10.240.2.10/32'),
            self.bundle.document_json,
        )

    def test_exact_upstream_inventory_and_placement_are_rendered(self):
        manifests = by_kind(self.bundle)

        self.assertEqual(set(manifests['Deployment']), EXPECTED_COMPONENTS)
        self.assertEqual(len(CPP_ENTRYPOINTS), 11)
        self.assertEqual(len(REDIS_COMPONENTS), 3)
        self.assertEqual(len(MEMCACHED_COMPONENTS), 4)
        self.assertEqual(len(MONGODB_COMPONENTS), 6)
        self.assertEqual(
            dict(self.bundle.expected_component_placement),
            {
                name: (
                    'application'
                    if name in {*CPP_ENTRYPOINTS, 'nginx-thrift', 'media-frontend'}
                    else 'cache'
                    if name in REDIS_COMPONENTS | MEMCACHED_COMPONENTS
                    else 'database'
                    if name in MONGODB_COMPONENTS
                    else 'control'
                )
                for name in EXPECTED_COMPONENTS
            },
        )
        self.assertIsInstance(
            self.bundle.expected_component_placement,
            MappingProxyType,
        )
        for name, deployment in manifests['Deployment'].items():
            pod_spec = deployment['spec']['template']['spec']
            container = pod_spec['containers'][0]
            self.assertEqual(
                pod_spec['nodeSelector'],
                {'deathstarbench.io/role': self.bundle.expected_component_placement[name]},
            )
            self.assertNotIn('resources', container)
            self.assertNotIn('initContainers', pod_spec)
            self.assertIn('@sha256:', container['image'])
            self.assertNotIn(':latest', container['image'])
        self.assertIn(
            {'effect': 'NoSchedule',
             'key': 'node-role.kubernetes.io/control-plane',
             'operator': 'Equal',
             'value': 'true'},
            manifests['Deployment']['jaeger-agent']['spec']['template']['spec'][
                'tolerations'
            ],
        )

    def test_arm_application_uses_arm_images_but_support_roles_stay_amd64(self):
        arm = self.bundle
        x86 = render_social_network_bundle(
            self.lock,
            'x86_64',
            '10.240.2.10',
        )
        arm_deployments = by_kind(arm)['Deployment']
        x86_deployments = by_kind(x86)['Deployment']
        arm_images = self.lock['platforms']['linux/arm64']['images']
        amd_images = self.lock['platforms']['linux/amd64']['images']

        for name in EXPECTED_COMPONENTS:
            arm_container = arm_deployments[name]['spec']['template']['spec'][
                'containers'
            ][0]
            x86_container = x86_deployments[name]['spec']['template']['spec'][
                'containers'
            ][0]
            role = arm.expected_component_placement[name]
            image_key = next(
                key
                for key, value in (
                    ('social-network-microservices', CPP_ENTRYPOINTS),
                    ('redis', REDIS_COMPONENTS),
                    ('memcached', MEMCACHED_COMPONENTS),
                    ('mongodb', MONGODB_COMPONENTS),
                    ('nginx-thrift', {'nginx-thrift'}),
                    ('media-frontend', {'media-frontend'}),
                    ('jaeger', {'jaeger-agent'}),
                )
                if name in value
            )
            if role == 'application':
                self.assertEqual(arm_container['image'], arm_images[image_key])
                self.assertNotEqual(arm_container['image'], x86_container['image'])
                expected_architecture = 'aarch64'
            else:
                self.assertEqual(arm_container['image'], amd_images[image_key])
                self.assertEqual(arm_container['image'], x86_container['image'])
                expected_architecture = 'x86_64'
            self.assertEqual(
                arm_deployments[name]['metadata']['annotations'][
                    'deathstarbench.io/architecture'
                ],
                expected_architecture,
            )

    def test_load_generator_change_modifies_only_policy_phase(self):
        changed = render_social_network_bundle(
            self.lock,
            'aarch64',
            '10.240.2.11',
        )

        self.assertEqual(self.bundle.namespace_manifest, changed.namespace_manifest)
        self.assertEqual(self.bundle.storage, changed.storage)
        self.assertEqual(self.bundle.services, changed.services)
        self.assertEqual(self.bundle.workloads, changed.workloads)
        self.assertNotEqual(self.bundle.policies, changed.policies)
        before = by_kind(self.bundle)['NetworkPolicy']
        after = by_kind(changed)['NetworkPolicy']
        changed_names = {
            name for name in before if before[name] != after[name]
        }
        self.assertEqual(changed_names, {'allow-load-generator-frontend'})

    def test_render_rejects_non_host_cidr_and_wrapper_rejects_cidr_input(self):
        for cidr in ('10.240.2.0/24', '2001:db8::1/128', '10.240.2.10'):
            with self.subTest(cidr=cidr):
                with self.assertRaises(WorkloadBundleError):
                    render_workload_bundle(self.lock, 'x86_64', cidr)
        with self.assertRaises(WorkloadBundleError):
            render_social_network_bundle(
                self.lock,
                'x86_64',
                '10.240.2.10/32',
            )

    def test_frontend_ports_and_database_storage_are_exact(self):
        manifests = by_kind(self.bundle)
        frontend = manifests['Service'][FRONTEND_COMPONENT]
        self.assertEqual(frontend['spec']['type'], 'NodePort')
        self.assertEqual(frontend['spec']['externalTrafficPolicy'], 'Local')
        self.assertEqual(frontend['spec']['ports'], [{
            'name': 'http',
            'nodePort': 8080,
            'port': 8080,
            'protocol': 'TCP',
            'targetPort': 8080,
        }])
        self.assertEqual(
            manifests['Service']['media-frontend']['spec']['ports'][0],
            {
                'name': 'http-media',
                'port': 8081,
                'protocol': 'TCP',
                'targetPort': 8080,
            },
        )

        for name in MONGODB_COMPONENTS:
            pv = manifests['PersistentVolume'][f'dsb-{name}']
            pvc = manifests['PersistentVolumeClaim'][name]
            deployment = manifests['Deployment'][name]
            self.assertEqual(
                pv['spec']['hostPath'],
                {'path': f'{DATABASE_ROOT}/{name}', 'type': 'Directory'},
            )
            self.assertEqual(
                pv['spec']['persistentVolumeReclaimPolicy'],
                'Retain',
            )
            expression = pv['spec']['nodeAffinity']['required'][
                'nodeSelectorTerms'
            ][0]['matchExpressions'][0]
            self.assertEqual(
                expression,
                {
                    'key': 'deathstarbench.io/role',
                    'operator': 'In',
                    'values': ['database'],
                },
            )
            self.assertEqual(pvc['spec']['volumeName'], f'dsb-{name}')
            self.assertEqual(
                deployment['spec']['template']['spec']['volumes'],
                [{
                    'name': 'database-data',
                    'persistentVolumeClaim': {'claimName': name},
                }],
            )

    def test_network_policies_default_deny_and_allow_only_required_ports(self):
        policies = by_kind(self.bundle)['NetworkPolicy']
        self.assertEqual(set(policies), EXPECTED_NETWORK_POLICIES)
        self.assertEqual(
            policies['default-deny-all']['spec'],
            {
                'egress': [],
                'ingress': [],
                'podSelector': {},
                'policyTypes': ['Ingress', 'Egress'],
            },
        )
        dns = policies['allow-dns-egress']['spec']['egress'][0]
        self.assertEqual(
            {(value['protocol'], value['port']) for value in dns['ports']},
            {('TCP', 53), ('UDP', 53)},
        )
        self.assertEqual(
            dns['to'],
            [{
                'namespaceSelector': {
                    'matchLabels': {'kubernetes.io/metadata.name': 'kube-system'}
                },
                'podSelector': {'matchLabels': {'k8s-app': 'kube-dns'}},
            }],
        )
        ingress = policies['allow-load-generator-frontend']['spec']
        self.assertEqual(
            ingress['podSelector']['matchLabels'][
                'app.kubernetes.io/component'
            ],
            FRONTEND_COMPONENT,
        )
        self.assertEqual(
            ingress['ingress'][0]['from'],
            [{'ipBlock': {'cidr': '10.240.2.10/32'}}],
        )
        self.assertEqual(
            ingress['ingress'][0]['ports'],
            [{'port': FRONTEND_NODE_PORT, 'protocol': 'TCP'}],
        )
        observed_egress = set()
        for source, _, _, _ in AUDITED_COMPONENT_EDGES:
            policy = policies[f'allow-egress-from-{source}']
            for rule in policy['spec']['egress']:
                peer = rule['to'][0]['podSelector']['matchLabels'][
                    'app.kubernetes.io/component'
                ]
                port = rule['ports'][0]
                observed_egress.add(
                    (source, peer, port['protocol'], port['port'])
                )
        observed_ingress = set()
        for _, destination, _, _ in AUDITED_COMPONENT_EDGES:
            policy = policies[f'allow-ingress-to-{destination}']
            for rule in policy['spec']['ingress']:
                peer = rule['from'][0]['podSelector']['matchLabels'][
                    'app.kubernetes.io/component'
                ]
                port = rule['ports'][0]
                observed_ingress.add(
                    (peer, destination, port['protocol'], port['port'])
                )
        self.assertEqual(observed_egress, AUDITED_COMPONENT_EDGES)
        self.assertEqual(observed_ingress, AUDITED_COMPONENT_EDGES)
        self.assertNotIn('"deathstarbench.io/role": "cache"', json.dumps(
            {
                name: policy
                for name, policy in policies.items()
                if name.startswith('allow-egress-from-')
            }
        ))


class WorkloadCommandTests(unittest.TestCase):
    def setUp(self):
        self.bundle = render_social_network_bundle(
            image_lock(),
            'x86_64',
            '10.240.2.10',
        )

    def test_apply_reads_payload_from_stdin_and_checks_hash_before_apply(self):
        expected = self.bundle.phase_sha256('namespace')
        command = workload_apply_command(expected)

        self.assertIn(f'EXPECTED_SHA256={expected}', command)
        self.assertIn('cat > "$MANIFEST"', command)
        self.assertIn('sha256sum "$MANIFEST"', command)
        self.assertIn('apply --dry-run=server', command)
        self.assertIn('--field-manager=deathstarbench-benchmark', command)
        self.assertLess(
            command.index('test "$ACTUAL_SHA256"'),
            command.index('apply --dry-run=server'),
        )
        self.assertNotIn(self.bundle.namespace_manifest, command)
        self.assertNotIn('registry.example', command)
        self.assertNotIn('base64', command)
        self.assertNotIn('curl', command)

        for invalid in ('sha256:' + expected, 'A' * 64, '0' * 63, None):
            with self.subTest(invalid=invalid):
                with self.assertRaises(WorkloadBundleError):
                    workload_apply_command(invalid)

    def test_readiness_is_bounded_and_emits_labeled_json_inventory(self):
        command = workload_readiness_command(self.bundle, timeout_seconds=321)

        self.assertIn('--for=condition=Available deployment --all', command)
        self.assertIn('--for=condition=Ready pod', command)
        self.assertEqual(command.count('--timeout=321s'), 2)
        self.assertIn('deathstarbench.io/workload=social-network-v1', command)
        self.assertIn('persistentvolumes', command)
        self.assertTrue(command.endswith('-o json'))
        self.assertNotIn('registry.example', command)
        with self.assertRaises(WorkloadBundleError):
            workload_readiness_command(self.bundle, timeout_seconds=0)
        with self.assertRaises(WorkloadBundleError):
            workload_readiness_command(object())


class WorkloadAttestationTests(unittest.TestCase):
    def setUp(self):
        self.bundle = render_social_network_bundle(
            image_lock(),
            'aarch64',
            '10.240.2.10',
        )
        self.attestation = fake_attestation(self.bundle)

    def test_exact_live_state_is_accepted(self):
        result = parse_workload_attestation(
            json.dumps(self.attestation),
            self.bundle,
        )

        self.assertEqual(result['namespace'], NAMESPACE)
        self.assertEqual(result['component_count'], 27)
        self.assertEqual(result['architecture'], 'aarch64')
        self.assertEqual(result['load_generator_cidr'], '10.240.2.10/32')
        self.assertEqual(
            result['bundle_fingerprint'],
            self.bundle.rendered_manifest_sha256,
        )
        self.assertEqual(
            result['pod_nodes'],
            ('dsb-application', 'dsb-cache', 'dsb-control', 'dsb-database'),
        )

    def test_attestation_rejects_image_storage_policy_and_pod_drift(self):
        mutations = []

        image_drift = copy.deepcopy(self.attestation)
        deployment = next(
            item for item in image_drift['items']
            if item['kind'] == 'Deployment'
        )
        deployment['spec']['template']['spec']['containers'][0]['image'] = (
            'registry.example/foreign/image@sha256:' + 'f' * 64
        )
        mutations.append(image_drift)

        storage_drift = copy.deepcopy(self.attestation)
        pv = next(
            item for item in storage_drift['items']
            if item['kind'] == 'PersistentVolume'
        )
        pv['spec']['hostPath']['path'] = '/tmp/database'
        mutations.append(storage_drift)

        policy_drift = copy.deepcopy(self.attestation)
        policy_drift['items'] = [
            item for item in policy_drift['items']
            if not (
                item['kind'] == 'NetworkPolicy'
                and item['metadata']['name'] == 'default-deny-all'
            )
        ]
        mutations.append(policy_drift)

        broadened_policy = copy.deepcopy(self.attestation)
        policy = next(
            item for item in broadened_policy['items']
            if item['kind'] == 'NetworkPolicy'
            and item['metadata']['name'] == 'allow-ingress-to-user-mongodb'
        )
        policy['spec']['ingress'][0]['ports'].append({
            'port': 27018,
            'protocol': 'TCP',
        })
        mutations.append(broadened_policy)

        pod_drift = copy.deepcopy(self.attestation)
        pod = next(item for item in pod_drift['items'] if item['kind'] == 'Pod')
        pod['spec']['nodeSelector'] = {'deathstarbench.io/role': 'database'}
        mutations.append(pod_drift)

        for mutation in mutations:
            with self.subTest(mutation=mutation):
                with self.assertRaises(WorkloadBundleError):
                    parse_workload_attestation(json.dumps(mutation), self.bundle)

    def test_attestation_rejects_extra_resources_and_non_json(self):
        extra = copy.deepcopy(self.attestation)
        extra['items'].append(copy.deepcopy(extra['items'][-1]))
        extra['items'][-1]['metadata']['name'] = 'unexpected-pod'
        with self.assertRaises(WorkloadBundleError):
            parse_workload_attestation(json.dumps(extra), self.bundle)
        with self.assertRaises(WorkloadBundleError):
            parse_workload_attestation('not-json', self.bundle)
        with self.assertRaises(WorkloadBundleError):
            parse_workload_attestation(json.dumps(self.attestation), object())


if __name__ == '__main__':
    unittest.main()
