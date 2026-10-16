"""Offline startup identities backed by actual synthetic adoption/access proofs.

Fixtures stub only the previously tested source/native boundaries, and route
anonymous HTTPS to deterministic raw OCI bytes. No live cloud, Kubernetes,
database, initializer, application or benchmark is executed.
"""

from dataclasses import FrozenInstanceError, replace
import json
import os
from pathlib import Path
import subprocess
import unittest
from unittest import mock

from app import deathstarbench_hotel_image_access as old_access
from app import deathstarbench_hotel_initialization_operator as old_operator
from app import deathstarbench_hotel_runtime as old_runtime
from app import deathstarbench_hotel_startup_image_access as access_boundary
from app import deathstarbench_hotel_startup_runtime as runtime
from app.deathstarbench_candidate_runtime import create_candidate_runtime_plan
from app.deathstarbench_candidate_stage import build_candidate_support_stage
from tests import test_deathstarbench_candidate_artifacts as candidate_fixtures
from tests import test_deathstarbench_hotel_image_access as old_access_fixtures
from tests import test_deathstarbench_hotel_startup_image_access as access_fixtures


RUN = '123456abcdef'
FS_UUID = '12345678-1234-4234-8234-123456789abc'
INITIALIZATION_ATTEMPT = '87654321-4321-4321-8321-cba987654321'
STARTUP_ATTEMPT = '87654321-4321-4321-8321-cba987654322'
TOPOLOGY = 'sha256:' + '1' * 64


def uid(number):
    return '00000000-0000-4000-8000-' + str(number).zfill(12)


class StartupRuntimeFixtureMixin:
    def setUp(self):
        helper = access_fixtures.StartupHotelImageAccessTests()
        self.addCleanup(helper.doCleanups)
        original_fixture = helper.fixture
        captured = {}
        def fixture(*args, **kwargs):
            data = original_fixture(*args, **kwargs)
            captured.update(data)
            return data
        # Capture, never substitute, the exact original adoption used by the
        # startup proof. A separately created baseline has different bytes.
        with mock.patch.object(helper, 'fixture', side_effect=fixture):
            self.artifacts, registry = helper.case()
        self.data = captured
        self.access_helper, self.registry = helper, registry
        self.image_access = helper.qualify(self.artifacts, registry)
        self.support_plan = self.support()
        self.support_stage = build_candidate_support_stage(self.support_plan)

    def support(self, **overrides):
        options = dict(run_id=RUN, provider='azure', topology_fingerprint=TOPOLOGY,
                       application_architecture='aarch64', load_generator_ip='10.240.2.10',
                       filesystem_uuid=FS_UUID)
        options.update(overrides)
        return create_candidate_runtime_plan(self.data['baseline'], **options)

    def plan(self, **overrides):
        options = dict(artifacts=self.artifacts, access=self.image_access,
                       support_plan=self.support_plan, support_stage=self.support_stage,
                       initialization_attempt_id=INITIALIZATION_ATTEMPT,
                       startup_attempt_id=STARTUP_ATTEMPT)
        options.update(overrides)
        return runtime.create_startup_hotel_runtime_plan(**options)


class StartupHotelRuntimeTests(StartupRuntimeFixtureMixin, unittest.TestCase):
    def test_distinct_plan_binds_real_startup_original_support_and_access_proofs(self):
        plan = self.plan()
        saved = plan.snapshot()
        self.assertIs(type(plan), runtime.StartupHotelRuntimePlan)
        self.assertEqual(saved['candidate_schema'], runtime.SNAPSHOT_SCHEMA)
        self.assertEqual(saved['runtime_revision'], runtime.RUNTIME_REVISION)
        self.assertEqual(saved['startup_artifact_fingerprint'], self.artifacts.fingerprint)
        self.assertEqual(saved['startup_image_access_fingerprint'], self.image_access.fingerprint)
        self.assertEqual(saved['original_artifact_fingerprint'], self.data['baseline'].fingerprint)
        self.assertEqual(saved['support_plan_fingerprint'], self.support_plan.fingerprint)
        self.assertEqual(saved['support_stage_fingerprint'], self.support_stage.fingerprint)
        self.assertEqual(plan.application_image, self.artifacts.app['platforms']['linux/arm64']['image'])
        self.assertNotEqual(plan.application_image, self.data['baseline'].image_bindings['hotel-reservation']['platforms']['linux/arm64']['image'])
        self.assertEqual(plan.run_id, RUN)
        self.assertEqual(plan.provider, 'azure')
        self.assertEqual(plan.namespace, runtime.NAMESPACE)
        self.assertEqual(plan.storage, self.support_plan.storage)
        self.assertEqual(plan.application_architecture, 'aarch64')
        self.assertEqual(plan.topology_fingerprint, TOPOLOGY)
        self.assertEqual(runtime.validate_startup_hotel_runtime_plan(plan), plan)
        self.assertEqual(runtime.validate_startup_hotel_runtime_snapshot(saved, plan), saved)
        self.assertEqual(plan.metadata(), saved)

    def test_every_execution_readiness_release_and_qualification_gate_remains_false(self):
        plan = self.plan()
        saved = plan.snapshot()
        for name in runtime._CLOSED_FLAGS:
            self.assertIs(saved[name], False, name)
        self.assertTrue(all(value is False for value in saved['qualification'].values()))
        for name in ('initializer_authorized', 'runtime_authorized', 'application_start_allowed',
                     'application_started', 'application_ready', 'released'):
            self.assertIs(saved[name], False, name)
        for name in ('namespace_uid', 'pod_uid', 'cluster_uid', 'initialization_completion'):
            self.assertNotIn(name, saved)
        for name in ('from_metadata', 'from_snapshot', 'runtime_manifest', 'apply', 'execute', 'ungate'):
            self.assertFalse(hasattr(plan, name), name)

    def test_explicit_seven_variable_environment_is_exact_and_copy_safe_for_all_components(self):
        plan = self.plan()
        expected_names = {'HOTEL_CANDIDATE_RUN_ID', 'HOTEL_CANDIDATE_CONTRACT_SHA256',
            'HOTEL_CANDIDATE_STARTUP_ATTEMPT_ID', 'HOTEL_CANDIDATE_COMPONENT',
            'HOTEL_CANDIDATE_POD_UID', 'HOTEL_CANDIDATE_POD_IP', 'TLS'}
        for component in runtime.APPLICATIONS:
            environment = plan.startup_environment(component)
            self.assertEqual([item['name'] for item in environment], sorted(expected_names))
            env = {item['name']: item for item in environment}
            self.assertEqual(env['HOTEL_CANDIDATE_RUN_ID'], {'name': 'HOTEL_CANDIDATE_RUN_ID', 'value': RUN})
            self.assertEqual(env['HOTEL_CANDIDATE_CONTRACT_SHA256']['value'], self.artifacts.dataset_attestation['dataset_fingerprint'])
            self.assertEqual(env['HOTEL_CANDIDATE_STARTUP_ATTEMPT_ID']['value'], STARTUP_ATTEMPT)
            self.assertEqual(env['HOTEL_CANDIDATE_COMPONENT']['value'], component)
            self.assertEqual(env['TLS'], {'name': 'TLS', 'value': 'false'})
            for name, field in (('HOTEL_CANDIDATE_POD_UID', 'metadata.uid'), ('HOTEL_CANDIDATE_POD_IP', 'status.podIP')):
                self.assertEqual(env[name], {'name': name, 'valueFrom': {'fieldRef': {'apiVersion': 'v1', 'fieldPath': field}}})
            environment.clear()
            self.assertEqual(len(plan.startup_environment(component)), 7)

    def test_ten_attempt_bound_pod_names_are_canonical_stable_and_distinct(self):
        plan = self.plan()
        names = {component: plan.application_pod_name(component) for component in runtime.APPLICATIONS}
        self.assertEqual(len(names), 10)
        self.assertEqual(len(set(names.values())), 10)
        for component, name in names.items():
            self.assertEqual(name, 'hotel-start-' + component + '-' + STARTUP_ATTEMPT.replace('-', ''))
            self.assertLessEqual(len(name), 63)
            self.assertEqual(self.plan().application_pod_name(component), name)
        for value in ('', None, True, 'mongodb-user', 'frontend\n', 'Frontend', 'unknown'):
            with self.subTest(value=value), self.assertRaises(runtime.StartupHotelRuntimeError):
                plan.startup_environment(value)
            with self.subTest(value=value), self.assertRaises(runtime.StartupHotelRuntimeError):
                plan.application_pod_name(value)

    def test_canonical_nonzero_distinct_attempt_ids_are_required(self):
        invalid = ('', None, True, 1, '00000000-0000-0000-0000-000000000000',
                   STARTUP_ATTEMPT.upper(), STARTUP_ATTEMPT + '\n', 'a' * 1024)
        for field in ('initialization_attempt_id', 'startup_attempt_id'):
            for value in invalid:
                with self.subTest(field=field, value=value), self.assertRaises(runtime.StartupHotelRuntimeError):
                    self.plan(**{field: value})
        with self.assertRaises(runtime.StartupHotelRuntimeError):
            self.plan(startup_attempt_id=INITIALIZATION_ATTEMPT)

    def test_run_provider_architecture_storage_topology_and_both_attempts_change_identity(self):
        original = self.plan()
        scopes = ({'run_id': 'abcdef123456'}, {'provider': 'aws'}, {'provider': 'gcp'}, {'provider': 'oci'},
                  {'topology_fingerprint': 'sha256:' + '3' * 64}, {'application_architecture': 'x86_64'},
                  {'load_generator_ip': '10.240.2.11'}, {'filesystem_uuid': uid(98)})
        for options in scopes:
            support = self.support(**options)
            changed = self.plan(support_plan=support, support_stage=build_candidate_support_stage(support))
            with self.subTest(options=options):
                self.assertNotEqual(original.fingerprint, changed.fingerprint)
                with self.assertRaises(runtime.StartupHotelRuntimeError):
                    runtime.validate_startup_hotel_runtime_snapshot(original.snapshot(), changed)
        for field in ('initialization_attempt_id', 'startup_attempt_id'):
            changed = self.plan(**{field: uid(99)})
            self.assertNotEqual(original.fingerprint, changed.fingerprint)
            with self.assertRaises(runtime.StartupHotelRuntimeError):
                runtime.validate_startup_hotel_runtime_snapshot(original.snapshot(), changed)

    def test_original_support_stage_is_unchanged_and_all_ten_apps_stay_zero(self):
        original = (self.support_plan.snapshot_json, self.support_stage.canonical_json,
                    self.support_stage.fingerprint, self.support_stage.metadata())
        self.plan()
        self.assertEqual(original, (self.support_plan.snapshot_json, self.support_stage.canonical_json,
                                   self.support_stage.fingerprint, self.support_stage.metadata()))
        blocked = self.support_stage.blocked_application_components
        self.assertEqual(set(blocked), set(runtime.APPLICATIONS))
        documents = json.loads(self.support_stage.canonical_json)['items']
        self.assertEqual(len([item for item in documents if item['kind'] == 'Deployment'
                              and item['metadata']['name'] in blocked and item['spec']['replicas'] == 0]), 10)

    def test_crossed_or_saved_support_inputs_fail_before_any_stage_can_be_derived(self):
        other = self.support(run_id='abcdef123456')
        for options in ({'support_plan': other}, {'support_stage': build_candidate_support_stage(other)},
                        {'support_plan': self.support_plan.snapshot()}, {'support_stage': self.support_stage.metadata()},
                        {'support_plan': None}, {'support_stage': None}):
            with self.subTest(options=tuple(options)), self.assertRaises(runtime.StartupHotelRuntimeError):
                self.plan(**options)

    def test_media_and_wrong_original_baseline_cannot_supply_hotel_support(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            helper = candidate_fixtures.CandidateArtifactAdoptionTests()
            self.addCleanup(helper.doCleanups)
            data = helper.fixture(workload)
            baseline = helper.adopt(data)
            support = create_candidate_runtime_plan(baseline, run_id=RUN, provider='azure', topology_fingerprint=TOPOLOGY,
                application_architecture='aarch64', load_generator_ip='10.240.2.10', filesystem_uuid=FS_UUID)
            with self.subTest(workload=workload), self.assertRaises(runtime.StartupHotelRuntimeError):
                self.plan(support_plan=support, support_stage=build_candidate_support_stage(support))

    def test_old_artifacts_access_and_saved_source_or_completion_json_are_not_inputs(self):
        helper = old_access_fixtures.GuardedHotelImageAccessTests()
        self.addCleanup(helper.doCleanups)
        old_artifacts, registry = helper.case()
        old_capability = helper.qualify(old_artifacts, registry)
        for value in (old_artifacts, self.data['baseline'], self.artifacts.metadata(), None, object()):
            with self.subTest(artifact=type(value)), self.assertRaises(runtime.StartupHotelRuntimeError):
                self.plan(artifacts=value)
        for value in (old_capability, self.image_access.metadata(), {'initialization_completed': True},
                      object.__new__(access_boundary.StartupHotelImageAccess), None):
            with self.subTest(access=type(value)), self.assertRaises(runtime.StartupHotelRuntimeError):
                self.plan(access=value)

    def test_access_capability_cannot_be_crossed_tampered_or_inherited_by_another_pid(self):
        other, registry = self.access_helper.case(variant='different-source-publication-bytes')
        other_access = self.access_helper.qualify(other, registry)
        with self.assertRaises(runtime.StartupHotelRuntimeError):
            self.plan(access=other_access)
        with mock.patch.object(access_boundary.os, 'getpid', return_value=os.getpid() + 1), \
                self.assertRaises(runtime.StartupHotelRuntimeError):
            self.plan()
        object.__setattr__(self.image_access, '_metadata_json', '{}')
        with self.assertRaises(runtime.StartupHotelRuntimeError):
            self.plan()

    def test_uninitialized_exact_types_do_not_bypass_factory_proofs_or_leak_raw_errors(self):
        for field, value in (('artifacts', self.artifacts), ('access', self.image_access),
                             ('support_plan', self.support_plan), ('support_stage', self.support_stage)):
            forged = object.__new__(type(value))
            with self.subTest(field=field), self.assertRaises(runtime.StartupHotelRuntimeError):
                self.plan(**{field: forged})
        forged = object.__new__(runtime.StartupHotelRuntimePlan)
        with self.assertRaises(runtime.StartupHotelRuntimeError):
            runtime.validate_startup_hotel_runtime_plan(forged)

    def test_new_fresh_access_receipt_changes_plan_identity_even_for_same_artifacts(self):
        original = self.plan()
        another = self.access_helper.qualify(self.artifacts, self.registry)
        changed = self.plan(access=another)
        self.assertNotEqual(original.fingerprint, changed.fingerprint)
        self.assertEqual(original.application_image, changed.application_image)
        with self.assertRaises(runtime.StartupHotelRuntimeError):
            runtime.validate_startup_hotel_runtime_snapshot(original.snapshot(), changed)

    def test_frozen_plan_and_detached_accessors_cannot_promote_or_change_identity(self):
        plan = self.plan()
        original = plan.snapshot()
        saved = plan.snapshot()
        saved['applications'].clear()
        saved['qualification']['application_start'] = True
        self.assertEqual(plan.snapshot(), original)
        self.assertEqual(plan.metadata(), original)
        with self.assertRaises(FrozenInstanceError):
            plan.fingerprint = 'changed'
        self.assertEqual(replace(plan), plan)
        for changes in ({'fingerprint': 'sha256:' + 'f' * 64}, {'snapshot_json': '{}'},
                        {'initialization_attempt_id': uid(98)}, {'startup_attempt_id': uid(99)},
                        {'support_plan': self.support(run_id='abcdef123456')}, {'support_stage': object()}):
            with self.subTest(changes=tuple(changes)), self.assertRaises(runtime.StartupHotelRuntimeError):
                replace(plan, **changes)

    def test_rehashed_or_mutated_saved_snapshot_is_never_execution_or_completion_authority(self):
        plan = self.plan()
        for field, value in (('released', True), ('schema_version', True), ('application_start_allowed', 1),
                             ('startup_attempt_id', uid(99)), ('initialization_completed', True),
                             ('global_readonly_verification_completed', True), ('unknown', False)):
            saved = plan.snapshot()
            saved[field] = value
            with self.subTest(field=field), self.assertRaises(runtime.StartupHotelRuntimeError):
                runtime.validate_startup_hotel_runtime_snapshot(saved, plan)
        for value in (plan.snapshot(), None, object()):
            with self.assertRaises(runtime.StartupHotelRuntimeError):
                runtime.validate_startup_hotel_runtime_plan(value)
        object.__setattr__(plan, 'snapshot_json', '{}')
        with self.assertRaises(runtime.StartupHotelRuntimeError):
            runtime.validate_startup_hotel_runtime_plan(plan)

    def test_self_consistent_rehashed_snapshots_cannot_replace_derived_runtime_identity(self):
        plan = self.plan()
        mutations = {
            'image': lambda value: value.update(application_image='ghcr.io/example/unreviewed:latest'),
            'source': lambda value: value.update(source_fingerprint='sha256:' + 'e' * 64),
            'config': lambda value: value.update(config_sha256='e' * 64),
            'access': lambda value: value.update(startup_image_access_fingerprint='sha256:' + 'e' * 64),
            'storage': lambda value: value['storage'].update(filesystem_uuid=uid(98)),
            'pod': lambda value: value['applications'][0].update(pod_name='unbound-pod'),
            'env': lambda value: value['applications'][0]['environment'].append({'name': 'TLS', 'value': 'true'}),
            'complete': lambda value: value.update(initialization_complete=True),
        }
        for name, mutation in mutations.items():
            value = plan.snapshot()
            mutation(value)
            with self.subTest(name=name), self.assertRaises(runtime.StartupHotelRuntimeError):
                replace(plan, snapshot_json=runtime._canonical(value), fingerprint=runtime._fingerprint(value))

    def test_bypassed_frozen_gate_mutations_and_boolean_aliases_fail_original_validation(self):
        plan = self.plan()
        for field in ('initializer_authorized', 'application_start_allowed', 'startup_authorized',
                      'runtime_authorized', 'initialization_complete', 'released'):
            for value in (True, 0):
                object.__setattr__(plan, field, value)
                with self.subTest(field=field, value=value), self.assertRaises(runtime.StartupHotelRuntimeError):
                    runtime.validate_startup_hotel_runtime_plan(plan)
                object.__setattr__(plan, field, False)

    def test_existing_guarded_runtime_operator_and_image_access_reject_new_plan(self):
        plan = self.plan()
        with self.assertRaises(old_runtime.GuardedHotelRuntimeError):
            old_runtime.validate_guarded_hotel_runtime_plan(plan)
        with self.assertRaises(old_access.GuardedHotelImageAccessError):
            old_access.require_guarded_hotel_image_access(self.image_access, self.artifacts)
        with self.assertRaises(old_operator.HotelInitializationOperatorError):
            old_operator.make_hotel_initialization_pin(RUN, {}, plan)

    def test_factory_and_validation_are_offline_and_never_write_files(self):
        with mock.patch.object(access_boundary.shared.urllib.request, 'build_opener', side_effect=AssertionError('No network')), \
                mock.patch.object(subprocess, 'run', side_effect=AssertionError('No subprocess')), \
                mock.patch.object(subprocess, 'Popen', side_effect=AssertionError('No subprocess')), \
                mock.patch.object(Path, 'write_text', side_effect=AssertionError('No file writes')), \
                mock.patch.object(Path, 'write_bytes', side_effect=AssertionError('No file writes')), \
                mock.patch.object(os, 'mkdir', side_effect=AssertionError('No filesystem allocation')):
            plan = self.plan()
            runtime.validate_startup_hotel_runtime_plan(plan)
            runtime.validate_startup_hotel_runtime_snapshot(plan.snapshot(), plan)


if __name__ == '__main__':
    unittest.main()
