"""Offline candidate render tests. Example digests are not release evidence."""

import copy
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
from types import MappingProxyType
import unittest
from unittest.mock import patch

from app import deathstarbench_candidate_workload as candidate
from app.deathstarbench_k3s_workload import (
    render_social_network_bundle,
    require_released_distributed_bundle,
    workload_readiness_command,
)
from app.deathstarbench_public import (
    CHECKED_IN_IMAGE_LOCK,
    validate_distributed_deathstarbench_plan,
)
from app.deathstarbench_workload_contract import distributed_workload_profile
from app.models import BenchmarkPlan


CANDIDATES = ('hotel_reservation', 'media_microservices')


def candidate_images(workload_id):
    profile = distributed_workload_profile(workload_id)
    return {
        'schema_version': candidate.CANDIDATE_IMAGE_SCHEMA_VERSION,
        'workload_id': workload_id,
        'workload_revision': profile.workload_revision,
        'image_set_revision': profile.image_set_revision,
        'upstream_revision': candidate.UPSTREAM_REVISION,
        'released': False,
        'platforms': {
            platform: {'images': {
                key: f'registry.example/{workload_id}/{key}@sha256:' + hashlib.sha256(
                    f'offline-test:{workload_id}:{platform}:{key}'.encode()
                ).hexdigest()
                for key in profile.required_image_keys
            }}
            for platform in ('linux/amd64', 'linux/arm64')
        },
    }


def render(workload_id, architecture='aarch64', ip='10.240.2.10'):
    return candidate.render_candidate_workload_bundle(
        workload_id, candidate_images(workload_id), architecture, ip,
    )


def inventory(bundle):
    by_kind = {}
    for item in json.loads(bundle.canonical_json)['items']:
        by_kind.setdefault(item['kind'], {})[item['metadata']['name']] = item
    return by_kind


class CandidateImageTests(unittest.TestCase):
    def test_exact_platforms_and_read_only_refs(self):
        for workload_id in CANDIDATES:
            with self.subTest(workload=workload_id):
                images = candidate.validate_candidate_images(workload_id, candidate_images(workload_id))
                self.assertEqual(set(images), {'linux/amd64', 'linux/arm64'})
                with self.assertRaises(TypeError):
                    images['linux/amd64']['mongodb'] = 'mutable'
                self.assertNotEqual(images['linux/amd64']['mongodb'], images['linux/arm64']['mongodb'])

    def test_crossed_workload_or_release_identities_are_rejected(self):
        for workload_id in CANDIDATES:
            for field, value in (
                ('workload_id', 'social_network'),
                ('workload_revision', 'social-network-6ecb097-workload-v1'),
                ('image_set_revision', 'social-network-images-v1'),
                ('upstream_revision', '0' * 40),
                ('schema_version', True), ('released', True), ('released', 0),
            ):
                with self.subTest(workload=workload_id, field=field, value=value):
                    manifest = candidate_images(workload_id)
                    manifest[field] = value
                    with self.assertRaises(candidate.CandidateWorkloadError):
                        candidate.validate_candidate_images(workload_id, manifest)

    def test_tags_placeholders_missing_or_reused_images_are_rejected(self):
        for workload_id in CANDIDATES:
            valid = candidate_images(workload_id)
            cases = []
            for reference in ('registry.example/mongodb:latest', 'registry.example/mongodb@sha256:' + '0' * 64, None):
                item = copy.deepcopy(valid)
                item['platforms']['linux/amd64']['images']['mongodb'] = reference
                cases.append(item)
            item = copy.deepcopy(valid)
            del item['platforms']['linux/arm64']
            cases.append(item)
            item = copy.deepcopy(valid)
            item['platforms']['linux/amd64']['images']['extra'] = 'unqualified'
            cases.append(item)
            item = copy.deepcopy(valid)
            item['platforms']['linux/arm64']['images']['mongodb'] = item['platforms']['linux/amd64']['images']['mongodb']
            cases.append(item)
            item = copy.deepcopy(valid)
            item['platforms']['linux/arm64']['images']['mongodb'] = 'other.example/mongodb@sha256:' + '1' * 64
            cases.append(item)
            item = copy.deepcopy(valid)
            item['extra'] = True
            cases.extend((item, None, []))
            for index, invalid in enumerate(cases):
                with self.subTest(workload=workload_id, case=index):
                    with self.assertRaises(candidate.CandidateWorkloadError):
                        candidate.validate_candidate_images(workload_id, invalid)


class CandidateAssetTests(unittest.TestCase):
    def test_audited_assets_have_complete_component_and_dependency_inventory(self):
        for workload_id, counts in (('hotel_reservation', (24, 41)), ('media_microservices', (32, 49))):
            with self.subTest(workload=workload_id):
                components, edges = candidate.validate_candidate_assets(workload_id)
                self.assertEqual((len(components), len(edges)), counts)
                self.assertEqual([item['name'] for item in components], sorted(distributed_workload_profile(workload_id).expected_component_placement))

    def test_any_byte_drift_rejects_without_inventing_new_audit_identity(self):
        profile = distributed_workload_profile('hotel_reservation')
        payload = (profile.asset_directory / 'components.json').read_bytes()
        with patch.object(Path, 'read_bytes', return_value=payload + b' '):
            with self.assertRaisesRegex(candidate.CandidateWorkloadError, 'changed'):
                candidate.validate_candidate_assets('hotel_reservation')

    def test_profile_release_flag_cannot_be_bypassed_by_assets(self):
        profile = distributed_workload_profile('hotel_reservation')
        component_asset = json.loads((profile.asset_directory / 'components.json').read_text())
        policy_asset = json.loads((profile.asset_directory / 'network-policies.json').read_text())
        component_asset['released'] = True
        with patch.object(candidate, '_read_asset', side_effect=[component_asset, policy_asset]):
            with self.assertRaisesRegex(candidate.CandidateWorkloadError, 'release state'):
                candidate.validate_candidate_assets('hotel_reservation')

    def test_invalid_schema_role_storage_command_and_frontend_are_rejected(self):
        profile = distributed_workload_profile('hotel_reservation')
        original = json.loads((profile.asset_directory / 'components.json').read_text())
        policies = json.loads((profile.asset_directory / 'network-policies.json').read_text())
        variants = []
        bad = copy.deepcopy(original)
        bad['components'][0]['role'] = 'control'
        variants.append(bad)
        bad = copy.deepcopy(original)
        bad['components'][0]['command'] = ['bash', '-c', 'unqualified']
        variants.append(bad)
        bad = copy.deepcopy(original)
        bad['components'][0]['storage_subdir'] = '../../etc'
        variants.append(bad)
        bad = copy.deepcopy(original)
        bad['components'][0]['ports'][0]['container_port'] = True
        variants.append(bad)
        bad = copy.deepcopy(original)
        next(item for item in bad['components'] if item['name'] == 'frontend')['ports'][0]['container_port'] = 8080
        variants.append(bad)
        bad = copy.deepcopy(original)
        bad['components'].append(copy.deepcopy(bad['components'][0]))
        variants.append(bad)
        bad = copy.deepcopy(original)
        bad['schema_version'] = True
        variants.append(bad)
        for index, invalid in enumerate(variants):
            with self.subTest(case=index):
                with patch.object(candidate, '_read_asset', side_effect=[invalid, policies]):
                    with self.assertRaises(candidate.CandidateWorkloadError):
                        candidate.validate_candidate_assets('hotel_reservation')

    def test_invalid_dependency_port_or_unordered_edges_are_rejected(self):
        profile = distributed_workload_profile('hotel_reservation')
        components = json.loads((profile.asset_directory / 'components.json').read_text())
        policies = json.loads((profile.asset_directory / 'network-policies.json').read_text())
        variants = []
        bad = copy.deepcopy(policies)
        bad['edges'][0]['port'] = 22
        variants.append(bad)
        bad = copy.deepcopy(policies)
        bad['edges'][0]['destination'] = 'foreign'
        variants.append(bad)
        bad = copy.deepcopy(policies)
        bad['edges'].reverse()
        variants.append(bad)
        bad = copy.deepcopy(policies)
        bad['edges'].append(copy.deepcopy(bad['edges'][-1]))
        variants.append(bad)
        for index, invalid in enumerate(variants):
            with self.subTest(case=index):
                with patch.object(candidate, '_read_asset', side_effect=[components, invalid]):
                    with self.assertRaises(candidate.CandidateWorkloadError):
                        candidate.validate_candidate_assets('hotel_reservation')


class CandidateRenderTests(unittest.TestCase):
    def test_both_architectures_have_exact_placement_and_locked_images(self):
        for workload_id in CANDIDATES:
            for architecture in ('x86_64', 'aarch64'):
                with self.subTest(workload=workload_id, architecture=architecture):
                    bundle = render(workload_id, architecture)
                    self.assertFalse(bundle.released)
                    self.assertEqual(bundle.namespace, distributed_workload_profile(workload_id).namespace)
                    by_kind = inventory(bundle)
                    self.assertEqual(set(by_kind['Deployment']), set(bundle.expected_component_placement))
                    images = candidate_images(workload_id)['platforms']
                    components, _edges = candidate.validate_candidate_assets(workload_id)
                    for component in components:
                        template = by_kind['Deployment'][component['name']]['spec']['template']
                        spec = template['spec']
                        expected_arch = architecture if component['role'] == 'application' else 'x86_64'
                        platform = 'linux/arm64' if expected_arch == 'aarch64' else 'linux/amd64'
                        self.assertEqual(spec['containers'][0]['image'], images[platform]['images'][component['image_key']])
                        self.assertEqual(spec['nodeSelector']['deathstarbench.io/role'], component['role'])
                        self.assertEqual(spec['nodeSelector']['kubernetes.io/arch'], 'arm64' if expected_arch == 'aarch64' else 'amd64')
                        self.assertFalse(spec['automountServiceAccountToken'])
                        self.assertFalse(spec['enableServiceLinks'])
                        self.assertNotIn('initContainers', spec)
                        self.assertNotIn('hostNetwork', spec)
                        self.assertEqual(bool(spec.get('tolerations')), component['role'] == 'control')

    def test_frontend_maps_existing_cloud_port_to_each_workload_pod_port(self):
        for workload_id in CANDIDATES:
            with self.subTest(workload=workload_id):
                profile = distributed_workload_profile(workload_id)
                by_kind = inventory(render(workload_id))
                frontend = by_kind['Service'][profile.frontend_component]['spec']
                self.assertEqual(frontend['externalTrafficPolicy'], 'Local')
                self.assertEqual(frontend['ports'][0]['nodePort'], 8080)
                self.assertEqual(frontend['ports'][0]['targetPort'], profile.frontend_container_port)
                self.assertEqual(frontend['ports'][0]['port'], 8080)
                self.assertEqual(sum(item['spec']['type'] == 'NodePort' for item in by_kind['Service'].values()), 1)
                ingress = by_kind['NetworkPolicy']['allow-load-generator-frontend']['spec']['ingress']
                self.assertEqual(ingress, [{
                    'from': [{'ipBlock': {'cidr': '10.240.2.10/32'}}],
                    'ports': [{'port': profile.frontend_container_port, 'protocol': 'TCP'}],
                }])

    def test_static_storage_stays_on_database_disk_and_within_capacity(self):
        for workload_id in CANDIDATES:
            with self.subTest(workload=workload_id):
                by_kind = inventory(render(workload_id))
                self.assertEqual(len(by_kind['PersistentVolume']), 8)
                self.assertEqual(len(by_kind['PersistentVolumeClaim']), 8)
                for volume in by_kind['PersistentVolume'].values():
                    spec = volume['spec']
                    self.assertEqual(spec['capacity'], {'storage': '10Gi'})
                    self.assertEqual(spec['persistentVolumeReclaimPolicy'], 'Retain')
                    self.assertEqual(spec['storageClassName'], '')
                    self.assertTrue(spec['hostPath']['path'].startswith(candidate.DATABASE_ROOT + '/'))
                    self.assertEqual(spec['hostPath']['type'], 'Directory')
                    self.assertEqual(spec['nodeAffinity']['required']['nodeSelectorTerms'][0]['matchExpressions'][0]['values'], ['database'])
                    self.assertNotIn('namespace', volume['metadata'])
                    claim = by_kind['PersistentVolumeClaim'][spec['claimRef']['name']]['spec']
                    self.assertEqual(claim['volumeName'], volume['metadata']['name'])

    def test_network_policies_are_exact_bidirectional_dependencies_and_dns(self):
        for workload_id in CANDIDATES:
            with self.subTest(workload=workload_id):
                policies = inventory(render(workload_id))['NetworkPolicy']
                _components, edges = candidate.validate_candidate_assets(workload_id)
                egress, ingress = set(), set()
                for name, policy in policies.items():
                    spec = policy['spec']
                    if name.startswith('allow-egress-from-'):
                        source = spec['podSelector']['matchLabels']['app.kubernetes.io/component']
                        for rule in spec['egress']:
                            target = rule['to'][0]['podSelector']['matchLabels']['app.kubernetes.io/component']
                            egress.add((source, target, rule['ports'][0]['protocol'], rule['ports'][0]['port']))
                    elif name.startswith('allow-ingress-to-'):
                        target = spec['podSelector']['matchLabels']['app.kubernetes.io/component']
                        for rule in spec['ingress']:
                            source = rule['from'][0]['podSelector']['matchLabels']['app.kubernetes.io/component']
                            ingress.add((source, target, rule['ports'][0]['protocol'], rule['ports'][0]['port']))
                self.assertEqual(egress, set(edges))
                self.assertEqual(ingress, set(edges))
                self.assertEqual(policies['default-deny-all']['spec'], {
                    'podSelector': {}, 'policyTypes': ['Ingress', 'Egress'], 'ingress': [], 'egress': [],
                })
                dns = policies['allow-dns-egress']['spec']['egress'][0]
                self.assertEqual(len(dns['to']), 1)
                self.assertEqual(set(dns['to'][0]), {'namespaceSelector', 'podSelector'})
                self.assertEqual(dns['ports'], [{'port': 53, 'protocol': 'TCP'}, {'port': 53, 'protocol': 'UDP'}])

    def test_fingerprints_and_phase_order_are_deterministic(self):
        for workload_id in CANDIDATES:
            with self.subTest(workload=workload_id):
                first, second = render(workload_id), render(workload_id)
                self.assertEqual(first, second)
                self.assertEqual([phase for phase, _payload in first.phases], ['namespace', 'storage', 'policies', 'services', 'workloads'])
                self.assertEqual(first.rendered_manifest_sha256, 'sha256:' + hashlib.sha256(first.canonical_json.encode()).hexdigest())
                self.assertNotEqual(first.rendered_manifest_sha256, render(workload_id, 'x86_64').rendered_manifest_sha256)
                self.assertNotEqual(first.rendered_manifest_sha256, render(workload_id, ip='10.240.2.11').rendered_manifest_sha256)

    def test_invalid_architecture_ip_and_released_workload_are_rejected(self):
        for architecture in ('unknown', '', 'x86_64;echo bad', None, [], {}):
            with self.subTest(architecture=architecture):
                with self.assertRaises(candidate.CandidateWorkloadError):
                    render('hotel_reservation', architecture)
        for ip in (
            '0.0.0.0/0', '10.240.2.10/32', '8.8.8.8', '127.0.0.1',
            '0.0.0.0', '::1', '10.240.2.10 ', '224.1.1.1',
            '169.254.169.254', '192.0.2.1', '198.51.100.7',
            '203.0.113.8', '240.0.0.1',
        ):
            with self.subTest(ip=ip):
                with self.assertRaises(candidate.CandidateWorkloadError):
                    render('hotel_reservation', ip=ip)
        for workload_id in ('social_network', 'unknown', '', None):
            with self.subTest(workload=workload_id):
                with self.assertRaises(candidate.CandidateWorkloadError):
                    candidate.render_candidate_workload_bundle(workload_id, {}, 'x86_64', '10.240.2.10')

    def test_all_rfc1918_ranges_and_architecture_aliases_are_accepted(self):
        for ip in ('10.1.2.3', '172.16.0.1', '172.31.255.254', '192.168.0.1'):
            with self.subTest(ip=ip):
                self.assertEqual(render('hotel_reservation', ip=ip).load_generator_cidr, f'{ip}/32')
        for alias, canonical in (('amd64', 'x86_64'), ('arm64', 'aarch64')):
            with self.subTest(alias=alias):
                self.assertEqual(render('hotel_reservation', alias), render('hotel_reservation', canonical))

    def test_nested_read_only_image_mappings_render_with_identical_fingerprint(self):
        for workload_id in CANDIDATES:
            with self.subTest(workload=workload_id):
                manifest = candidate_images(workload_id)
                manifest['platforms'] = MappingProxyType({
                    platform: MappingProxyType({'images': MappingProxyType(value['images'])})
                    for platform, value in manifest['platforms'].items()
                })
                bundle = candidate.render_candidate_workload_bundle(
                    workload_id, MappingProxyType(manifest), 'aarch64', '10.240.2.10',
                )
                self.assertEqual(bundle, render(workload_id))

    def test_image_identity_uses_same_validated_snapshot_as_rendered_references(self):
        manifest = candidate_images('hotel_reservation')
        original_validator = candidate.validate_candidate_images

        def validate_then_mutate(workload_id, value):
            references = original_validator(workload_id, value)
            value['platforms']['linux/arm64']['images']['hotel-reservation'] = 'mutated.example/unqualified:latest'
            return references

        with patch.object(candidate, 'validate_candidate_images', side_effect=validate_then_mutate):
            bundle = candidate.render_candidate_workload_bundle(
                'hotel_reservation', manifest, 'aarch64', '10.240.2.10',
            )
        self.assertEqual(bundle, render('hotel_reservation'))
        self.assertNotIn('mutated.example', bundle.canonical_json)

    def test_parallel_interleaving_does_not_change_workload_state_or_social_identity(self):
        social_images = json.loads(CHECKED_IN_IMAGE_LOCK.read_text())
        before = render_social_network_bundle(social_images, 'aarch64', '10.240.2.10')
        jobs = list(CANDIDATES) * 12
        with ThreadPoolExecutor(max_workers=4) as pool:
            bundles = list(pool.map(render, jobs))
        for workload_id, bundle in zip(jobs, bundles, strict=True):
            self.assertEqual(bundle, render(workload_id))
            self.assertEqual(bundle.namespace, distributed_workload_profile(workload_id).namespace)
        after = render_social_network_bundle(social_images, 'aarch64', '10.240.2.10')
        self.assertEqual(before, after)
        self.assertEqual(require_released_distributed_bundle(social_images).released, True)

    def test_candidate_preview_cannot_be_used_as_released_bundle(self):
        for workload_id in CANDIDATES:
            with self.subTest(workload=workload_id):
                with self.assertRaises(ValueError):
                    workload_readiness_command(render(workload_id))
                with self.assertRaises(ValueError):
                    require_released_distributed_bundle(candidate_images(workload_id))

    def test_new_candidates_stay_rejected_by_public_plan_and_lifecycle(self):
        for provider in ('aws', 'gcp', 'azure', 'oci'):
            for workload_id in CANDIDATES:
                with self.subTest(provider=provider, workload=workload_id):
                    plan = {
                        'provider': provider, 'region': 'test-region-1',
                        'ssh_private_key': 'offline-test-private-key',
                        'ssh_public_key': 'offline-test-public-key',
                        'shape': 'VM.Standard.E5.Flex' if provider == 'oci' else 'test-shape',
                        'ocpus': 4, 'memory_gb': 16, 'benchmarks': ['deathstarbench'],
                        'deathstarbench': {'topology_id': 'distributed_tiered_v1', 'runtime_id': 'k3s_v1', 'workload': workload_id},
                    }
                    with self.assertRaisesRegex(ValueError, 'Social Network'):
                        BenchmarkPlan.model_validate(plan)
                    with self.assertRaisesRegex(ValueError, 'exact Social Network'):
                        validate_distributed_deathstarbench_plan(plan)


if __name__ == '__main__':
    unittest.main()
