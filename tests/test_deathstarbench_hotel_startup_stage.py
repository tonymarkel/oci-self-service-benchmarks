"""Closed-world offline startup Pods: deliberately no live dispatch authority."""

import copy
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import unittest
from unittest import mock

from app import deathstarbench_candidate_stage as support_boundary
from app import deathstarbench_hotel_startup_image_access as access_boundary
from app import deathstarbench_hotel_startup_runtime as runtime
from app import deathstarbench_hotel_startup_stage as stage
from app.deathstarbench_candidate_workload import validate_candidate_assets
from app.deathstarbench_workload_contract import distributed_workload_profile
from tests.test_deathstarbench_hotel_startup_runtime import (
    INITIALIZATION_ATTEMPT, RUN, STARTUP_ATTEMPT, StartupRuntimeFixtureMixin, uid,
    traced_validation,
)


def documents(bundle):
    return json.loads(bundle.canonical_json)['items']


class StartupHotelStartupStageTests(StartupRuntimeFixtureMixin, unittest.TestCase):
    def bundle(self, plan=None):
        return stage.build_startup_hotel_startup_stage(plan or self.plan())

    def test_public_validation_bounds_full_derivation_and_validates_both_plan_arguments(self):
        # One full stage derivation plus independent embedded/supplied runtime
        # validations bounds CPU work without caching across public operations.
        plan = self.plan()
        equal_plan = replace(plan)
        bundle = self.bundle(plan)
        for _ in range(2):
            checked, counts = traced_validation(
                lambda: stage.validate_startup_hotel_startup_stage(equal_plan, bundle),
                snapshot=runtime._snapshot_identity, stage=stage._derive,
                access=runtime.require_startup_hotel_image_access)
            self.assertEqual(counts, {'snapshot': 2, 'stage': 1, 'access': 4})
            self.assertIs(checked, bundle)
            self.assertIs(checked.plan, plan)
            self.assertIs(checked.plan.access, self.image_access)
        for operation in (bundle.metadata, lambda: bundle.pod('frontend')):
            _, counts = traced_validation(operation, snapshot=runtime._snapshot_identity, stage=stage._derive)
            self.assertEqual(counts, {'snapshot': 1, 'stage': 1})

    def test_repeat_public_validation_rechecks_supplied_and_embedded_plan_independently(self):
        # Catches using the embedded valid plan to skip validation of an equal
        # supplied object, or accepting a genuinely valid but crossed plan.
        plan = self.plan()
        supplied = replace(plan)
        bundle = self.bundle(plan)
        stage.validate_startup_hotel_startup_stage(supplied, bundle)
        for target in (supplied, plan):
            object.__setattr__(target, 'candidate_only', False)
            try:
                with self.subTest(target='supplied' if target is supplied else 'embedded'), \
                        self.assertRaisesRegex(stage.StartupHotelStartupStageError, 'crossed or stale'):
                    stage.validate_startup_hotel_startup_stage(supplied, bundle)
            finally:
                object.__setattr__(target, 'candidate_only', True)
        crossed = self.plan(startup_attempt_id=uid(98))
        with self.assertRaisesRegex(stage.StartupHotelStartupStageError, 'crossed or stale'):
            stage.validate_startup_hotel_startup_stage(crossed, bundle)

    def test_repeat_public_validation_and_accessors_recheck_changed_stage_and_capability(self):
        # Catches retaining a public-boundary validation across later mutations;
        # metadata and Pod access must keep full validation, not trust JSON hashes.
        plan = self.plan()
        bundle = self.bundle(plan)
        stage.validate_startup_hotel_startup_stage(plan, bundle)
        mutations = [(bundle, 'canonical_json', '{}'), (bundle, '_metadata_json', '{}'),
                     (bundle, 'candidate_only', False), (bundle, 'revision', 'changed'),
                     (self.image_access, '_metadata_json', '{}')]
        mutations += [(bundle, flag, value) for flag in stage._GATES for value in (True, 0)]
        for target, field, value in mutations:
            original = getattr(target, field)
            try:
                object.__setattr__(target, field, value)
                for operation in (lambda: stage.validate_startup_hotel_startup_stage(plan, bundle),
                                  bundle.metadata, lambda: bundle.pod('frontend')):
                    with self.subTest(field=field, value=value, operation=operation), \
                            self.assertRaises(stage.StartupHotelStartupStageError):
                        operation()
            finally:
                object.__setattr__(target, field, original)
        # Detached output remains harmless even when validators return originals.
        bundle.metadata()['pod_names'].clear()
        bundle.pod('frontend')['spec']['schedulingGates'].clear()
        self.assertIs(stage.validate_startup_hotel_startup_stage(plan, bundle), bundle)
        self.assertEqual(len(bundle.metadata()['pod_names']), 10)
        self.assertEqual(bundle.pod('frontend')['spec']['schedulingGates'], [{'name': stage.SCHEDULING_GATE}])

    def test_repeat_public_validation_and_accessors_recheck_source_asset_bytes(self):
        # Catches caching either a public stage validation or an accessor result
        # after the checksum-audited application inventory changes on disk.
        plan = self.plan()
        bundle = self.bundle(plan)
        stage.validate_startup_hotel_startup_stage(plan, bundle)
        target = distributed_workload_profile('hotel_reservation').asset_directory / 'components.json'
        read_bytes = Path.read_bytes
        def changed(path):
            payload = read_bytes(path)
            return payload + b'\n' if path == target else payload
        with mock.patch.object(Path, 'read_bytes', new=changed):
            for operation in (lambda: stage.validate_startup_hotel_startup_stage(plan, bundle),
                              bundle.metadata, lambda: bundle.pod('frontend')):
                with self.subTest(operation=operation), self.assertRaises(stage.StartupHotelStartupStageError):
                    operation()
        self.assertIs(stage.validate_startup_hotel_startup_stage(plan, bundle), bundle)

    def test_exact_ten_standalone_pods_use_native_new_images_and_audited_commands_ports(self):
        plan = self.plan()
        bundle = self.bundle(plan)
        payload = json.loads(bundle.canonical_json)
        self.assertEqual(set(payload), {'apiVersion', 'kind', 'items'})
        self.assertEqual(payload['apiVersion'], 'v1')
        self.assertEqual(payload['kind'], 'List')
        pods = payload['items']
        self.assertEqual(len(pods), 10)
        self.assertTrue(all(item['kind'] == 'Pod' for item in pods))
        self.assertEqual(len({item['metadata']['name'] for item in pods}), 10)
        components, _edges = validate_candidate_assets('hotel_reservation')
        applications = {item['name']: item for item in components if item['role'] == 'application'}
        self.assertEqual(set(applications), set(stage.APPLICATIONS))
        for name, source in applications.items():
            pod = bundle.pod(name)
            self.assertEqual(pod['metadata']['name'], plan.application_pod_name(name))
            self.assertEqual(pod['metadata']['namespace'], plan.namespace)
            self.assertNotIn('ownerReferences', pod['metadata'])
            self.assertNotIn('generateName', pod['metadata'])
            self.assertEqual(len(pod['spec']['containers']), 1)
            container = pod['spec']['containers'][0]
            self.assertEqual(container['name'], name)
            self.assertEqual(container['image'], plan.application_image)
            self.assertEqual(container['imagePullPolicy'], 'Always')
            self.assertEqual(container['command'], source['command'])
            self.assertEqual(container['args'], [])
            self.assertEqual(container['workingDir'], '/workspace')
            self.assertEqual(container['ports'], [{'name': port['name'], 'containerPort': port['container_port'],
                'protocol': port['protocol']} for port in source['ports']])

    def test_all_pods_remain_scheduling_gated_never_restart_and_have_bounded_deadlines(self):
        bundle = self.bundle()
        for pod in documents(bundle):
            spec = pod['spec']
            self.assertEqual(spec['schedulingGates'], [{'name': stage.SCHEDULING_GATE}])
            self.assertEqual(spec['restartPolicy'], 'Never')
            self.assertIs(type(spec['activeDeadlineSeconds']), int)
            self.assertEqual(spec['activeDeadlineSeconds'], 900)
            self.assertEqual(spec['terminationGracePeriodSeconds'], 10)
            self.assertFalse(any(name in spec for name in ('initContainers', 'ephemeralContainers', 'volumes',
                'serviceAccountName', 'imagePullSecrets', 'nodeName', 'readinessGates')))
            self.assertFalse(any(name in pod for name in ('status', 'replicas', 'selector', 'template')))
            container = spec['containers'][0]
            self.assertFalse(any(name in container for name in ('livenessProbe', 'readinessProbe', 'startupProbe',
                'lifecycle', 'envFrom', 'volumeMounts', 'volumeDevices', 'stdin', 'tty')))
        for name in ('apply', 'execute', 'ungate', 'gate_release_patch', 'retry', 'from_metadata'):
            self.assertFalse(hasattr(bundle, name), name)

    def test_rootless_read_only_no_credentials_or_host_namespaces_are_exact(self):
        for pod in documents(self.bundle()):
            spec = pod['spec']
            for name in ('automountServiceAccountToken', 'enableServiceLinks', 'hostNetwork',
                         'hostPID', 'hostIPC', 'shareProcessNamespace'):
                self.assertIs(spec[name], False, name)
            self.assertEqual(spec['dnsPolicy'], 'ClusterFirst')
            self.assertEqual(spec['schedulerName'], 'default-scheduler')
            self.assertEqual(spec['securityContext'], {'runAsNonRoot': True, 'runAsUser': 65532,
                'runAsGroup': 65532, 'seccompProfile': {'type': 'RuntimeDefault'}})
            self.assertEqual(spec['containers'][0]['securityContext'], {'allowPrivilegeEscalation': False,
                'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']}})

    def test_exact_seven_environment_fields_include_only_downward_uid_ip_and_tls_false(self):
        plan = self.plan()
        bundle = self.bundle(plan)
        for name in stage.APPLICATIONS:
            environment = bundle.pod(name)['spec']['containers'][0]['env']
            self.assertEqual(environment, plan.startup_environment(name))
            self.assertEqual(len(environment), 7)
            self.assertEqual([entry['name'] for entry in environment], sorted(entry['name'] for entry in environment))
            literals = {entry['name']: entry['value'] for entry in environment if 'value' in entry}
            self.assertEqual(literals['TLS'], 'false')
            self.assertEqual(literals['HOTEL_CANDIDATE_RUN_ID'], RUN)
            self.assertEqual(literals['HOTEL_CANDIDATE_COMPONENT'], name)
            self.assertEqual(literals['HOTEL_CANDIDATE_STARTUP_ATTEMPT_ID'], STARTUP_ATTEMPT)
            for entry in environment:
                if 'valueFrom' in entry:
                    self.assertEqual(set(entry['valueFrom']), {'fieldRef'})
                    self.assertIn(entry['valueFrom']['fieldRef']['fieldPath'], ('metadata.uid', 'status.podIP'))

    def test_both_native_architectures_are_application_only_and_original_support_stays_unchanged(self):
        original = self.support_stage.canonical_json
        for architecture, expected in (('aarch64', 'arm64'), ('x86_64', 'amd64')):
            support = self.support(application_architecture=architecture)
            support_stage = support_boundary.build_candidate_support_stage(support)
            before = support_stage.canonical_json
            plan = self.plan(support_plan=support, support_stage=support_stage)
            bundle = self.bundle(plan)
            for pod in documents(bundle):
                self.assertEqual(pod['spec']['nodeSelector'], {'deathstarbench.io/role': 'application',
                                                               'kubernetes.io/arch': expected})
                self.assertEqual(pod['spec']['containers'][0]['image'], self.artifacts.app['platforms']['linux/' + expected]['image'])
            self.assertEqual(support_stage.canonical_json, before)
            blocked = support_stage.blocked_application_components
            self.assertEqual(len([item for item in documents(support_stage) if item['kind'] == 'Deployment'
                                  and item['metadata']['name'] in blocked and item['spec']['replicas'] == 0]), 10)
        self.assertEqual(self.support_stage.canonical_json, original)

    def test_service_matching_labels_retained_and_all_ownership_annotations_bound(self):
        plan = self.plan()
        bundle = self.bundle(plan)
        services = {item['metadata']['name']: item for item in documents(self.support_stage) if item['kind'] == 'Service'}
        expected_annotations = {'deathstarbench.io/candidate-run-id': RUN,
            'deathstarbench.io/hotel-startup-runtime-fingerprint': plan.fingerprint,
            'deathstarbench.io/hotel-startup-stage-revision': stage.STAGE_REVISION,
            'deathstarbench.io/hotel-initialization-attempt': INITIALIZATION_ATTEMPT,
            'deathstarbench.io/hotel-startup-attempt': STARTUP_ATTEMPT,
            'deathstarbench.io/application-start-allowed': 'false',
            'deathstarbench.io/architecture': plan.application_architecture,
            'deathstarbench.io/image-reference': plan.application_image,
            'deathstarbench.io/hotel-startup-stage-fingerprint': bundle.fingerprint}
        for name in stage.APPLICATIONS:
            pod = bundle.pod(name)
            labels = pod['metadata']['labels']
            self.assertEqual(labels['deathstarbench.io/workload'], 'hotel-reservation-v1')
            self.assertEqual(labels['app.kubernetes.io/component'], name)
            self.assertEqual(labels['deathstarbench.io/role'], 'application')
            self.assertEqual(labels['deathstarbench.io/candidate-run-id'], RUN)
            self.assertEqual(labels['deathstarbench.io/hotel-startup-attempt'], STARTUP_ATTEMPT)
            for key, value in services[name]['spec']['selector'].items():
                self.assertEqual(labels[key], value)
            for key, value in expected_annotations.items():
                self.assertEqual(pod['metadata']['annotations'][key], value)

    def test_metadata_has_no_live_readiness_or_release_authority_and_no_network_delta(self):
        plan = self.plan()
        bundle = self.bundle(plan)
        metadata = bundle.metadata()
        self.assertEqual(metadata['candidate_schema'], stage.STAGE_SCHEMA)
        self.assertEqual(metadata['revision'], stage.STAGE_REVISION)
        self.assertEqual(metadata['plan_fingerprint'], plan.fingerprint)
        self.assertEqual(metadata['application_components'], list(stage.APPLICATIONS))
        self.assertEqual(metadata['application_pod_count'], 10)
        self.assertEqual(metadata['pod_names'], {name: plan.application_pod_name(name) for name in stage.APPLICATIONS})
        self.assertEqual(metadata['initialization_attempt_id'], INITIALIZATION_ATTEMPT)
        self.assertEqual(metadata['startup_attempt_id'], STARTUP_ATTEMPT)
        for flag in stage._GATES:
            self.assertIs(metadata[flag], False, flag)
            self.assertIs(getattr(bundle, flag), False, flag)
        for flag in ('live_initialization_verified', 'semantic_readiness_verified', 'gate_release_patch_available',
                     'automatic_retry_allowed', 'original_support_stage_modified'):
            self.assertIs(metadata[flag], False, flag)
        for count in ('deployments_included', 'jobs_included', 'services_included', 'network_policies_included',
                      'storage_resources_included', 'rbac_resources_included'):
            self.assertIs(type(metadata[count]), int)
            self.assertEqual(metadata[count], 0)
        self.assertIn('no-tcp', metadata['readiness_scope'])
        self.assertIn('cleanup-only', metadata['recovery'])
        self.assertEqual(bundle.rendered_manifest_sha256, 'sha256:' + hashlib.sha256(bundle.canonical_json.encode()).hexdigest())
        self.assertNotEqual(bundle.fingerprint, bundle.rendered_manifest_sha256)

    def test_resources_are_explicit_bounded_qualification_not_measurement_settings(self):
        bundle = self.bundle()
        for pod in documents(bundle):
            self.assertEqual(pod['spec']['containers'][0]['resources'], {
                'requests': {'cpu': '100m', 'memory': '64Mi'}, 'limits': {'cpu': '1', 'memory': '256Mi'}})
        self.assertIn('qualification-only', bundle.metadata()['resource_scope'])

    def test_detached_pod_and_metadata_accessors_and_immutable_stage(self):
        bundle = self.bundle()
        original = bundle.canonical_json
        pod = bundle.pod('frontend')
        pod['spec']['schedulingGates'].clear()
        pod['metadata']['labels'].clear()
        bundle.metadata()['pod_names'].clear()
        self.assertEqual(bundle.canonical_json, original)
        self.assertEqual(len(bundle.metadata()['pod_names']), 10)
        self.assertEqual(bundle.pod('frontend')['spec']['schedulingGates'], [{'name': stage.SCHEDULING_GATE}])
        with self.assertRaises(FrozenInstanceError):
            bundle.application_start_allowed = True
        self.assertEqual(replace(bundle), bundle)
        for name in ('', None, True, 'mongodb-user', 'Frontend', 'frontend\n'):
            with self.subTest(name=name), self.assertRaises(stage.StartupHotelStartupStageError):
                bundle.pod(name)

    def test_render_is_deterministic_and_crossed_scope_or_attempts_cannot_validate(self):
        plan = self.plan()
        bundle = self.bundle(plan)
        self.assertEqual(self.bundle(plan), bundle)
        self.assertEqual(stage.validate_startup_hotel_startup_stage(plan, bundle), bundle)
        for changes in ({'initialization_attempt_id': uid(97)}, {'startup_attempt_id': uid(98)}):
            other = self.plan(**changes)
            self.assertNotEqual(self.bundle(other).fingerprint, bundle.fingerprint)
            with self.assertRaises(stage.StartupHotelStartupStageError):
                stage.validate_startup_hotel_startup_stage(other, bundle)
        support = self.support(run_id='abcdef123456')
        other = self.plan(support_plan=support, support_stage=support_boundary.build_candidate_support_stage(support))
        with self.assertRaises(stage.StartupHotelStartupStageError):
            stage.validate_startup_hotel_startup_stage(other, bundle)

    def test_fully_rehashed_unsafe_payloads_fail_exact_reconstruction_not_just_hash_checks(self):
        bundle = self.bundle()
        def container(document):
            return document['items'][0]['spec']['containers'][0]
        def podspec(document):
            return document['items'][0]['spec']
        mutations = {
            'ungate': lambda doc: podspec(doc).update(schedulingGates=[]),
            'wrong-gate': lambda doc: podspec(doc).update(schedulingGates=[{'name': 'attacker.example/start'}]),
            'restart': lambda doc: podspec(doc).update(restartPolicy='Always'),
            'deadline': lambda doc: podspec(doc).update(activeDeadlineSeconds=0),
            'bool-deadline': lambda doc: podspec(doc).update(activeDeadlineSeconds=True),
            'live-node': lambda doc: podspec(doc).update(nodeName='selected-node'),
            'wrong-arch': lambda doc: podspec(doc)['nodeSelector'].update({'kubernetes.io/arch': 'amd64'}),
            'probe': lambda doc: container(doc).update(readinessProbe={'tcpSocket': {'port': 8089}}),
            'static-page-probe': lambda doc: container(doc).update(livenessProbe={'httpGet': {'path': '/', 'port': 5000}}),
            'startup-probe': lambda doc: container(doc).update(startupProbe={'tcpSocket': {'port': 8089}}),
            'runtime-args': lambda doc: container(doc).update(args=['--mongoaddr=attacker:27017']),
            'other-command': lambda doc: container(doc).update(command=['/bin/sh']),
            'other-image': lambda doc: container(doc).update(image='ghcr.io/example/unreviewed:latest'),
            'old-tls': lambda doc: container(doc)['env'].append({'name': 'TLS', 'value': '0'}),
            'extra-env': lambda doc: container(doc)['env'].append({'name': 'MONGO_ADDR', 'value': 'attacker:27017'}),
            'missing-env': lambda doc: container(doc)['env'].pop(),
            'literal-pod-id': lambda doc: container(doc).update(env=[{'name': 'HOTEL_CANDIDATE_POD_UID', 'value': uid(1)}]),
            'secret-env': lambda doc: container(doc).update(envFrom=[{'secretRef': {'name': 'credentials'}}]),
            'configmap-volume': lambda doc: podspec(doc).update(volumes=[{'name': 'config', 'configMap': {'name': 'unsafe'}}]),
            'host-volume': lambda doc: podspec(doc).update(volumes=[{'name': 'root', 'hostPath': {'path': '/'}}]),
            'service-account': lambda doc: podspec(doc).update(serviceAccountName='admin'),
            'token': lambda doc: podspec(doc).update(automountServiceAccountToken=True),
            'registry-secret': lambda doc: podspec(doc).update(imagePullSecrets=[{'name': 'registry'}]),
            'host-network': lambda doc: podspec(doc).update(hostNetwork=True),
            'host-pid': lambda doc: podspec(doc).update(hostPID=True),
            'root': lambda doc: podspec(doc)['securityContext'].update(runAsUser=0),
            'privilege': lambda doc: container(doc)['securityContext'].update(privileged=True),
            'writable-root': lambda doc: container(doc)['securityContext'].update(readOnlyRootFilesystem=False),
            'capability': lambda doc: container(doc)['securityContext'].update(capabilities={'add': ['SYS_ADMIN']}),
            'port': lambda doc: container(doc)['ports'][0].update(containerPort=22),
            'host-port': lambda doc: container(doc)['ports'][0].update(hostPort=8089),
            'namespace': lambda doc: doc['items'][0]['metadata'].update(namespace='default'),
            'labels': lambda doc: doc['items'][0]['metadata']['labels'].update({'app.kubernetes.io/component': 'frontend'}),
            'owners': lambda doc: doc['items'][0]['metadata'].update(ownerReferences=[{'kind': 'Deployment', 'name': 'retrying-owner'}]),
            'annotation': lambda doc: doc['items'][0]['metadata']['annotations'].update({'deathstarbench.io/application-start-allowed': 'true'}),
            'sidecar': lambda doc: podspec(doc)['containers'].append(copy.deepcopy(container(doc))),
            'init': lambda doc: podspec(doc).update(initContainers=[copy.deepcopy(container(doc))]),
            'extra-pod': lambda doc: doc['items'].append(copy.deepcopy(doc['items'][0])),
            'missing-pod': lambda doc: doc['items'].pop(),
            'job': lambda doc: doc['items'][0].update(kind='Job'),
            'network-policy': lambda doc: doc['items'].append({'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy'}),
            'rbac': lambda doc: doc['items'].append({'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'ClusterRoleBinding'}),
        }
        for name, mutation in mutations.items():
            altered = json.loads(bundle.canonical_json)
            mutation(altered)
            payload = stage._canonical(altered)
            rendered = 'sha256:' + hashlib.sha256(payload.encode()).hexdigest()
            metadata = bundle.metadata()
            metadata['rendered_manifest_sha256'] = rendered
            with self.subTest(name=name), self.assertRaises(stage.StartupHotelStartupStageError):
                replace(bundle, canonical_json=payload, rendered_manifest_sha256=rendered,
                        _metadata_json=stage._canonical(metadata))

    def test_metadata_rehash_or_false_gate_alias_cannot_promote_and_object_bypass_is_detected(self):
        bundle = self.bundle()
        for field in ('released', 'application_start_allowed', 'runtime_authorized', 'semantic_readiness_verified', 'gate_release_patch_available'):
            metadata = bundle.metadata()
            metadata[field] = True
            metadata['fingerprint'] = stage._fingerprint({key: value for key, value in metadata.items()
                if key not in ('fingerprint', 'rendered_manifest_sha256')})
            with self.subTest(field=field), self.assertRaises(stage.StartupHotelStartupStageError):
                replace(bundle, _metadata_json=stage._canonical(metadata), fingerprint=metadata['fingerprint'])
        for field, value in (('run_id', 'abcdef123456'), ('namespace', 'default'), ('startup_attempt_id', uid(98)),
                             ('fingerprint', 'sha256:' + 'f' * 64), ('plan_fingerprint', self.support_plan.fingerprint),
                             ('_metadata_json', '{}'), ('plan', self.support_plan)):
            with self.subTest(field=field), self.assertRaises(stage.StartupHotelStartupStageError):
                replace(bundle, **{field: value})
        object.__setattr__(bundle, 'canonical_json', stage._canonical({'apiVersion': 'v1', 'kind': 'List', 'items': []}))
        with self.assertRaises(stage.StartupHotelStartupStageError):
            bundle.metadata()
        with self.assertRaises(stage.StartupHotelStartupStageError):
            bundle.pod('frontend')

    def test_old_support_stage_plan_or_saved_json_cannot_be_adopted_as_startup(self):
        plan = self.plan()
        bundle = self.bundle(plan)
        for value in (None, {}, plan.snapshot(), self.support_plan, self.support_stage, self.image_access):
            with self.subTest(value=type(value)), self.assertRaises(stage.StartupHotelStartupStageError):
                stage.build_startup_hotel_startup_stage(value)
        for value in (None, bundle.metadata(), self.support_stage, self.support_plan.bundle):
            with self.subTest(value=type(value)), self.assertRaises(stage.StartupHotelStartupStageError):
                stage.validate_startup_hotel_startup_stage(plan, value)
        with self.assertRaises(support_boundary.CandidateStageError):
            support_boundary.validate_candidate_support_stage(self.support_plan, bundle)

    def test_changed_fresh_access_or_runtime_fields_are_rejected_before_render(self):
        plan = self.plan()
        object.__setattr__(self.image_access, '_metadata_json', '{}')
        with self.assertRaises(stage.StartupHotelStartupStageError):
            self.bundle(plan)

    def test_original_pid_is_required_again_for_metadata_pod_and_saved_bundle_validation(self):
        plan = self.plan()
        bundle = self.bundle(plan)
        with mock.patch.object(access_boundary.os, 'getpid', return_value=os.getpid() + 1):
            with self.assertRaises(stage.StartupHotelStartupStageError):
                bundle.metadata()
            with self.assertRaises(stage.StartupHotelStartupStageError):
                bundle.pod('frontend')
            with self.assertRaises(stage.StartupHotelStartupStageError):
                stage.validate_startup_hotel_startup_stage(plan, bundle)

    def test_direct_frozen_gate_bypasses_and_uninitialized_exact_stage_never_validate(self):
        plan = self.plan()
        bundle = self.bundle(plan)
        for flag in stage._GATES:
            object.__setattr__(bundle, flag, True)
            with self.subTest(flag=flag), self.assertRaises(stage.StartupHotelStartupStageError):
                stage.validate_startup_hotel_startup_stage(plan, bundle)
            object.__setattr__(bundle, flag, False)
        object.__setattr__(bundle, 'application_start_allowed', 0)
        with self.assertRaises(stage.StartupHotelStartupStageError):
            bundle.metadata()
        forged = object.__new__(stage.StartupHotelStartupStage)
        with self.assertRaises(stage.StartupHotelStartupStageError):
            stage.validate_startup_hotel_startup_stage(plan, forged)

    def test_render_and_validation_perform_no_network_subprocess_or_file_writes(self):
        plan = self.plan()
        with mock.patch.object(access_boundary.shared.urllib.request, 'build_opener', side_effect=AssertionError('No network')), \
                mock.patch.object(subprocess, 'run', side_effect=AssertionError('No subprocess')), \
                mock.patch.object(subprocess, 'Popen', side_effect=AssertionError('No subprocess')), \
                mock.patch.object(Path, 'write_text', side_effect=AssertionError('No file writes')), \
                mock.patch.object(Path, 'write_bytes', side_effect=AssertionError('No file writes')), \
                mock.patch.object(os, 'mkdir', side_effect=AssertionError('No filesystem allocation')):
            bundle = self.bundle(plan)
            stage.validate_startup_hotel_startup_stage(plan, bundle)
            bundle.metadata()
            bundle.pod('frontend')


if __name__ == '__main__':
    unittest.main()
