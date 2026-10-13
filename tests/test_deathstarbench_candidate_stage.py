"""Support-only staging tests; they start no pods and initialize no dataset."""

from dataclasses import FrozenInstanceError, replace
import hashlib
import json
import unittest

from app import deathstarbench_candidate_stage as stage
from app.deathstarbench_candidate_runtime import create_candidate_runtime_plan
from app.deathstarbench_candidate_workload import validate_candidate_assets
from tests import test_deathstarbench_candidate_artifacts as artifact_fixtures


def documents(bundle):
    return json.loads(bundle.canonical_json)['items']


class CandidateSupportStageTests(unittest.TestCase):
    def plan(self, workload='hotel_reservation', architecture='aarch64', run_id='a1b2c3d4e5f6'):
        fixture = artifact_fixtures.CandidateArtifactAdoptionTests()
        self.addCleanup(fixture.doCleanups)
        artifacts = fixture.adopt(fixture.fixture(workload))
        return create_candidate_runtime_plan(
            artifacts, run_id=run_id, provider='oci', topology_fingerprint='sha256:' + '1' * 64,
            application_architecture=architecture, load_generator_ip='10.2.3.4',
            filesystem_uuid='12345678-1234-4234-8234-123456789abc',
        )

    def test_exact_role_inventory_blocks_every_application_and_starts_only_support(self):
        for workload, expected_app, expected_support in (('hotel_reservation', 10, 14), ('media_microservices', 13, 19)):
            for architecture in ('x86_64', 'aarch64'):
                with self.subTest(workload=workload, architecture=architecture):
                    plan = self.plan(workload, architecture)
                    bundle = stage.build_candidate_support_stage(plan)
                    components, _ = validate_candidate_assets(workload)
                    roles = {item['name']: item['role'] for item in components}
                    deployment_docs = [item for item in documents(bundle) if item['kind'] == 'Deployment']
                    self.assertEqual(len(bundle.blocked_application_components), expected_app)
                    self.assertEqual(len(bundle.active_support_components), expected_support)
                    self.assertEqual(set(item['metadata']['name'] for item in deployment_docs), set(roles))
                    for item in deployment_docs:
                        name = item['metadata']['name']
                        self.assertIs(type(item['spec']['replicas']), int)
                        self.assertEqual(item['spec']['replicas'], 0 if roles[name] == 'application' else 1)
                        self.assertNotIn('paused', item['spec'])
                        self.assertNotIn('initContainers', item['spec']['template']['spec'])
                        arch = item['spec']['template']['spec']['nodeSelector']['kubernetes.io/arch']
                        self.assertEqual(arch, 'arm64' if roles[name] == 'application' and architecture == 'aarch64' else 'amd64')
                    self.assertTrue(all(item['kind'] in {'Namespace', 'PersistentVolume', 'PersistentVolumeClaim', 'NetworkPolicy', 'Service', 'Deployment'}
                                        for item in documents(bundle)))

    def test_stage_ownership_binds_all_resource_metadata_and_pod_templates(self):
        plan = self.plan('media_microservices')
        bundle = stage.build_candidate_support_stage(plan)
        expected = {stage.RUN_ANNOTATION: plan.run_id, stage.PLAN_ANNOTATION: plan.fingerprint,
                    stage.REVISION_ANNOTATION: stage.STAGE_REVISION, stage.STAGE_ANNOTATION: bundle.fingerprint,
                    stage.START_ANNOTATION: 'false'}
        for item in documents(bundle):
            for key, value in expected.items():
                self.assertEqual(item['metadata']['annotations'][key], value)
                if item['kind'] == 'Deployment':
                    self.assertEqual(item['spec']['template']['metadata']['annotations'][key], value)
        self.assertEqual(bundle.rendered_manifest_sha256, 'sha256:' + hashlib.sha256(bundle.canonical_json.encode()).hexdigest())
        self.assertNotEqual(bundle.fingerprint, bundle.rendered_manifest_sha256)

    def test_six_exact_phases_partition_the_payload_with_no_starting_applications(self):
        plan = self.plan()
        bundle = stage.build_candidate_support_stage(plan)
        self.assertEqual(tuple(name for name, _ in bundle.phases), stage.PHASE_NAMES)
        merged = []
        for name, payload in bundle.phases:
            value = json.loads(payload)
            self.assertEqual(set(value), {'apiVersion', 'kind', 'items'})
            self.assertEqual(value['kind'], 'List')
            merged.extend(value['items'])
            if name in ('support-workloads', 'blocked-applications'):
                self.assertTrue(value['items'])
                self.assertTrue(all(item['kind'] == 'Deployment' for item in value['items']))
                self.assertTrue(all(item['spec']['replicas'] == (1 if name == 'support-workloads' else 0) for item in value['items']))
        self.assertEqual(merged, documents(bundle))
        self.assertEqual(len(merged), len({(item['kind'], item['metadata']['name']) for item in merged}))

    def test_stage_does_not_mutate_or_reuse_the_offline_final_manifest_identity(self):
        plan = self.plan()
        final_json, final_hash, final_phases = plan.bundle.canonical_json, plan.bundle.rendered_manifest_sha256, plan.bundle.phases
        bundle = stage.build_candidate_support_stage(plan)
        self.assertEqual(plan.bundle.canonical_json, final_json)
        self.assertEqual(plan.bundle.rendered_manifest_sha256, final_hash)
        self.assertEqual(plan.bundle.phases, final_phases)
        self.assertTrue(all(item['spec']['replicas'] == 1 for item in documents(plan.bundle) if item['kind'] == 'Deployment'))
        self.assertEqual(bundle.final_manifest_sha256, final_hash)
        self.assertNotEqual(bundle.rendered_manifest_sha256, final_hash)
        self.assertNotEqual(bundle.fingerprint, plan.fingerprint)
        self.assertEqual(bundle.image_manifest_sha256, plan.bundle.image_manifest_sha256)
        self.assertEqual(bundle.manifest_source_sha256, plan.bundle.manifest_source_sha256)

    def test_stage_metadata_is_copy_safe_and_keeps_startup_rights_and_release_gates_closed(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            bundle = stage.build_candidate_support_stage(self.plan(workload))
            metadata = bundle.metadata()
            for key in ('application_start_allowed', 'initialization_qualified', 'runtime_qualified', 'measurement_qualified',
                        'dataset_ready', 'dataset_rights_cleared', 'anonymous_pull_qualified', 'released'):
                self.assertIs(metadata[key], False)
            self.assertIs(bundle.application_start_allowed, False)
            self.assertIs(bundle.initialization_qualified, False)
            self.assertIs(bundle.released, False)
            self.assertIs(bundle.candidate_only, True)
            self.assertIn('unconditionally seed MongoDB' if workload == 'hotel_reservation' else 'TMDB dataset rights',
                          metadata['startup_barriers'][0])
            metadata['blocked_application_components'].clear()
            metadata['application_start_allowed'] = True
            self.assertNotEqual(metadata, bundle.metadata())
            with self.assertRaises(FrozenInstanceError):
                bundle.application_start_allowed = True

    def test_stage_is_deterministic_but_isolated_across_runs_workloads_and_architectures(self):
        first_plan = self.plan()
        first = stage.build_candidate_support_stage(first_plan)
        self.assertEqual(stage.build_candidate_support_stage(first_plan), first)
        for plan in (self.plan(run_id='112233445566'), self.plan('media_microservices'), self.plan(architecture='x86_64')):
            other = stage.build_candidate_support_stage(plan)
            self.assertNotEqual(other.fingerprint, first.fingerprint)
            self.assertNotEqual(other.rendered_manifest_sha256, first.rendered_manifest_sha256)
            with self.assertRaises(stage.CandidateStageError):
                stage.validate_candidate_support_stage(plan, first)
        self.assertEqual(stage.validate_candidate_support_stage(first_plan, first), first)

    def test_tampered_stage_fields_replica_gate_and_final_bundle_substitution_are_rejected(self):
        plan = self.plan()
        bundle = stage.build_candidate_support_stage(plan)
        changed = json.loads(bundle.canonical_json)
        app = next(item for item in changed['items'] if item['kind'] == 'Deployment' and item['metadata']['name'] in bundle.blocked_application_components)
        app['spec']['replicas'] = 1
        altered = stage._canonical(changed)
        for fields in (
            {'canonical_json': altered}, {'canonical_json': plan.bundle.canonical_json},
            {'phase_payloads': plan.bundle.phase_payloads}, {'run_id': '112233445566'},
            {'workload_id': 'social_network'}, {'fingerprint': plan.fingerprint},
            {'rendered_manifest_sha256': plan.bundle.rendered_manifest_sha256},
            {'blocked_application_components': ()}, {'active_support_components': ()},
            {'_metadata_json': '{}'}, {'canonical_json': altered, 'rendered_manifest_sha256': 'sha256:' + hashlib.sha256(altered.encode()).hexdigest()},
        ):
            with self.subTest(fields=tuple(fields)), self.assertRaises(stage.CandidateStageError):
                replace(bundle, **fields)
        with self.assertRaises(stage.CandidateStageError):
            stage.validate_candidate_support_stage(plan, plan.bundle)

    def test_untypeable_or_crossed_runtime_plans_fail_before_stage_build(self):
        plan = self.plan()
        for value in (None, {}, plan.snapshot(), plan.bundle):
            with self.subTest(value=type(value).__name__), self.assertRaises(stage.CandidateStageError):
                stage.build_candidate_support_stage(value)


if __name__ == '__main__':
    unittest.main()
