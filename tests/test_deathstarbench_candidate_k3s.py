"""Synthetic K3s support observations: never live qualification evidence."""

from contextlib import redirect_stdout
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from app import deathstarbench_candidate_k3s as k3s
from app import deathstarbench_candidate_stage as staging
from tests import test_deathstarbench_candidate_stage as stage_fixtures


ROLE_NODES = {role: f'dsb-{role}' for role in ('application', 'cache', 'control', 'database')}


def identity(kind, name):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f'synthetic-k3s-only:{kind}:{name}'))


def live_inventory(stage, role_nodes=ROLE_NODES, *, application_replicasets=True):
    """Mirror controller/default behavior without executing any Kubernetes API."""
    items = copy.deepcopy(json.loads(stage.canonical_json)['items'])
    uid_by_kind_name = {(item['kind'], item['metadata']['name']): identity(item['kind'], item['metadata']['name']) for item in items}
    extra = []
    for index, item in enumerate(items):
        kind, name = item['kind'], item['metadata']['name']
        item['metadata']['uid'] = uid_by_kind_name[kind, name]
        if kind == 'Namespace':
            item['spec'] = {'finalizers': ['kubernetes']}
            item['status'] = {'phase': 'Active'}
        elif kind == 'PersistentVolume':
            item['status'] = {'phase': 'Bound'}
            item['spec']['claimRef'].update({
                'uid': uid_by_kind_name['PersistentVolumeClaim', item['spec']['claimRef']['name']],
                'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim', 'resourceVersion': '1',
            })
            # StorageClassName is omitempty on PV, but not PVC.
            item['spec'].pop('storageClassName')
        elif kind == 'PersistentVolumeClaim':
            item['status'] = {'phase': 'Bound', 'capacity': {'storage': '10Gi'}, 'accessModes': ['ReadWriteOnce']}
            item['spec']['volumeMode'] = 'Filesystem'
        elif kind == 'Service':
            item['spec'].update({'clusterIP': f'10.43.1.{index + 1}', 'clusterIPs': [f'10.43.1.{index + 1}'], 'ipFamilies': ['IPv4']})
        elif kind == 'NetworkPolicy':
            for key in ('ingress', 'egress'):
                if item['spec'].get(key) == []:
                    item['spec'].pop(key)
        elif kind == 'Deployment':
            item['metadata']['generation'] = 1
            replicas = item['spec']['replicas']
            item['status'] = {'observedGeneration': 1, 'replicas': replicas, 'readyReplicas': replicas,
                              'availableReplicas': replicas, 'updatedReplicas': replicas}
            pod_hash = '6c9cf79f4f'
            rs_name = name + '-' + pod_hash
            template = copy.deepcopy(item['spec']['template'])
            template['metadata']['labels']['pod-template-hash'] = pod_hash
            for container in template['spec']['containers']:
                if container.get('env') == []:
                    container.pop('env')
            if replicas == 0 and not application_replicasets:
                continue
            rs = {
                'apiVersion': 'apps/v1', 'kind': 'ReplicaSet',
                'metadata': {
                    'name': rs_name, 'namespace': stage.namespace, 'uid': identity('ReplicaSet', rs_name), 'generation': 1,
                    'labels': copy.deepcopy(template['metadata']['labels']),
                    # Controller copies Deployment annotations onto RS metadata.
                    'annotations': {**item['metadata']['annotations'], 'deployment.kubernetes.io/revision': '1'},
                    'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': name,
                                         'uid': item['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}],
                },
                'spec': {'replicas': replicas, 'selector': {'matchLabels': copy.deepcopy(template['metadata']['labels'])},
                         'template': copy.deepcopy(template)},
                'status': {'observedGeneration': 1, 'replicas': replicas, 'readyReplicas': replicas, 'availableReplicas': replicas},
            }
            extra.append(rs)
            if replicas == 0:
                continue
            role = template['spec']['nodeSelector']['deathstarbench.io/role']
            pod_name = rs_name + '-abcde'
            pod_spec = copy.deepcopy(template['spec'])
            pod_spec['nodeName'] = role_nodes[role]
            pod_spec.setdefault('tolerations', []).extend([
                {'key': f'node.kubernetes.io/{key}', 'operator': 'Exists', 'effect': 'NoExecute', 'tolerationSeconds': 300}
                for key in ('not-ready', 'unreachable')
            ])
            image = pod_spec['containers'][0]['image']
            extra.append({
                'apiVersion': 'v1', 'kind': 'Pod',
                'metadata': {
                    'name': pod_name, 'namespace': stage.namespace, 'uid': identity('Pod', pod_name),
                    **copy.deepcopy(template['metadata']),
                    'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': rs_name,
                                         'uid': rs['metadata']['uid'], 'controller': True, 'blockOwnerDeletion': True}],
                },
                'spec': pod_spec,
                'status': {
                    'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}],
                    'containerStatuses': [{
                        'name': name, 'image': image, 'imageID': 'docker-pullable://' + image,
                        'ready': True, 'started': True, 'restartCount': 0,
                        'containerID': 'containerd://' + hashlib.sha256(name.encode()).hexdigest(),
                        'state': {'running': {'startedAt': '2026-10-14T12:00:00Z'}}, 'lastState': {},
                    }],
                },
            })
    for role, name in role_nodes.items():
        architecture = 'arm64' if role == 'application' and stage.application_architecture == 'aarch64' else 'amd64'
        extra.append({
            'apiVersion': 'v1', 'kind': 'Node',
            'metadata': {'name': name, 'uid': identity('Node', name), 'labels': {
                'deathstarbench.io/role': role, 'kubernetes.io/arch': architecture, 'kubernetes.io/os': 'linux',
            }},
            'status': {'nodeInfo': {'architecture': architecture}, 'conditions': [{'type': 'Ready', 'status': 'True'}]},
        })
    return {'apiVersion': 'v1', 'kind': 'List', 'items': items + extra}


def observation(plan, stage, inventory, *, phase=None, mode='support-readiness', roles=ROLE_NODES):
    return json.dumps({
        'schema_version': 1, 'candidate_schema': k3s.OBSERVATION_SCHEMA,
        'candidate_only': True, 'released': False, 'runtime_qualified': False,
        'mode': mode, 'run_id': plan.run_id, 'snapshot_fingerprint': plan.fingerprint,
        'stage_fingerprint': stage.fingerprint, 'namespace': stage.namespace,
        'role_nodes': roles, 'phase': phase, 'inventory': inventory,
    })


def objects(inventory, kind):
    return [item for item in inventory['items'] if item['kind'] == kind]


class _CandidateCaseMixin:
    def case(self, workload='hotel_reservation', architecture='aarch64'):
        helper = stage_fixtures.CandidateSupportStageTests()
        self.addCleanup(helper.doCleanups)
        plan = helper.plan(workload, architecture)
        stage = staging.build_candidate_support_stage(plan)
        return plan, stage, live_inventory(stage)

    def parse(self, plan, stage, inventory):
        return k3s.parse_candidate_support_attestation(observation(plan, stage, inventory), plan, stage, ROLE_NODES)


class CandidateK3sTests(_CandidateCaseMixin, unittest.TestCase):
    def test_both_workloads_architectures_have_support_only_proof_and_exact_controllers(self):
        for workload, support_count, app_count in (('hotel_reservation', 14, 10), ('media_microservices', 19, 13)):
            for architecture in ('x86_64', 'aarch64'):
                with self.subTest(workload=workload, architecture=architecture):
                    plan, stage, inventory = self.case(workload, architecture)
                    proof = self.parse(plan, stage, inventory)
                    self.assertEqual(proof['state'], 'support_ready_applications_paused')
                    self.assertEqual(len(proof['support_pods']), support_count)
                    self.assertEqual(len(proof['blocked_applications']), app_count)
                    self.assertTrue(proof['candidate_only'])
                    for field in ('released', 'runtime_qualified', 'dataset_qualified'):
                        self.assertIs(proof[field], False)
                    self.assertIs(proof['applications_paused'], True)
                    self.assertEqual(proof['application_pods_observed'], 0)
                    self.assertEqual(proof['namespace_uid'], identity('Namespace', stage.namespace))
                    for pod in proof['support_pods']:
                        self.assertEqual(pod['node_name'], ROLE_NODES[pod['role']])
                        self.assertEqual(pod['restart_count'], 0)
                        self.assertTrue(pod['image_id'].startswith('docker-pullable://'))
                        self.assertIn('startedAt', pod['state']['running'])
                    self.assertEqual(k3s.validate_saved_candidate_support_attestation(proof, plan, stage, ROLE_NODES), proof)

    def test_application_zero_status_without_zero_replicaset_is_valid_not_app_ready(self):
        plan, stage, _ = self.case()
        inventory = live_inventory(stage, application_replicasets=False)
        proof = self.parse(plan, stage, inventory)
        self.assertTrue(all(item['replicaset_uid'] is None for item in proof['blocked_applications']))
        self.assertTrue(proof['applications_paused'])

    def test_known_api_defaults_and_omitted_empty_env_are_not_false_drift(self):
        plan, stage, inventory = self.case()
        for item in inventory['items']:
            if item['kind'] in {'Deployment', 'ReplicaSet', 'Pod'}:
                spec = item['spec'] if item['kind'] == 'Pod' else item['spec']['template']['spec']
                spec.update({'restartPolicy': 'Always', 'dnsPolicy': 'ClusterFirst',
                             'schedulerName': 'default-scheduler', 'securityContext': {}})
                if item['kind'] == 'Pod':
                    spec.update({'serviceAccountName': 'default', 'serviceAccount': 'default',
                                 'preemptionPolicy': 'PreemptLowerPriority', 'priority': 0})
                container = spec['containers'][0]
                if container.get('env') == []:
                    container.pop('env')
                container.update({'terminationMessagePath': '/dev/termination-log', 'terminationMessagePolicy': 'File', 'resources': {}})
                container['readinessProbe'].update({'initialDelaySeconds': 0, 'successThreshold': 1})
            elif item['kind'] == 'Service':
                item['spec'].update({'sessionAffinity': 'None', 'ipFamilyPolicy': 'SingleStack', 'internalTrafficPolicy': 'Cluster'})
        self.parse(plan, stage, inventory)

    def test_unknown_defaults_probe_actions_and_service_account_volumes_fail(self):
        plan, stage, original = self.case()
        mutations = [
            lambda inv: objects(inv, 'Pod')[0]['spec'].update({'priorityClassName': 'unapproved'}),
            lambda inv: objects(inv, 'Pod')[0]['spec']['containers'][0]['readinessProbe'].update({'exec': {'command': ['false']}}),
            lambda inv: objects(inv, 'Pod')[0]['spec']['containers'][0]['readinessProbe']['tcpSocket'].update({'host': 'external.example'}),
            lambda inv: objects(inv, 'Pod')[0]['spec'].setdefault('volumes', []).append({'name': 'token', 'secret': {'secretName': 'default-token'}}),
            lambda inv: objects(inv, 'Deployment')[0]['spec'].update({'minReadySeconds': 10}),
            lambda inv: objects(inv, 'Service')[0]['spec'].update({'externalIPs': ['192.0.2.1']}),
        ]
        for index, mutate in enumerate(mutations):
            inventory = copy.deepcopy(original)
            mutate(inventory)
            with self.subTest(index=index), self.assertRaises(k3s.CandidateK3sError):
                self.parse(plan, stage, inventory)

    def test_zero_or_missing_support_pods_never_pass_readiness(self):
        plan, stage, original = self.case()
        for removed in (objects(original, 'Pod'), objects(original, 'Pod')[:1]):
            inventory = copy.deepcopy(original)
            names = {item['metadata']['name'] for item in removed}
            inventory['items'] = [item for item in inventory['items'] if not (item['kind'] == 'Pod' and item['metadata']['name'] in names)]
            with self.assertRaisesRegex(k3s.CandidateK3sError, 'support inventory'):
                self.parse(plan, stage, inventory)

    def test_application_deployment_replicas_status_bool_and_pod_bypass_fail(self):
        plan, stage, original = self.case()
        application = next(item for item in objects(original, 'Deployment') if item['spec']['replicas'] == 0)
        for field, value in (('spec.replicas', 1), ('spec.replicas', False), ('status.replicas', 1),
                             ('status.updatedReplicas', 1), ('status.availableReplicas', 1), ('spec.paused', True)):
            inventory = copy.deepcopy(original)
            item = next(item for item in objects(inventory, 'Deployment') if item['metadata']['name'] == application['metadata']['name'])
            section, name = field.split('.')
            item[section][name] = value
            with self.subTest(field=field, value=value), self.assertRaises(k3s.CandidateK3sError):
                self.parse(plan, stage, inventory)
        inventory = copy.deepcopy(original)
        pod = copy.deepcopy(objects(inventory, 'Pod')[0])
        pod['metadata']['name'] = 'unexpected-application-pod'
        pod['metadata']['labels']['app.kubernetes.io/component'] = application['metadata']['name']
        inventory['items'].append(pod)
        with self.assertRaisesRegex(k3s.CandidateK3sError, 'application Pod'):
            self.parse(plan, stage, inventory)

    def test_static_inventory_ownership_policy_storage_and_hotel_nodeport_drift_fail(self):
        plan, stage, original = self.case()
        mutations = [
            lambda inv: objects(inv, 'Namespace')[0]['metadata']['annotations'].update({staging.RUN_ANNOTATION: 'abcdef123456'}),
            lambda inv: objects(inv, 'Deployment')[0]['metadata'].update({'deletionTimestamp': '2026-10-14T12:00:00Z'}),
            lambda inv: objects(inv, 'PersistentVolume')[0]['spec']['hostPath'].update({'path': '/data'}),
            lambda inv: objects(inv, 'PersistentVolume')[0]['spec']['claimRef'].update({'uid': identity('foreign', 'claim')}),
            lambda inv: objects(inv, 'PersistentVolumeClaim')[0]['status'].update({'phase': 'Pending'}),
            lambda inv: objects(inv, 'PersistentVolumeClaim')[0]['status'].update({'capacity': {'storage': '1Gi'}}),
            lambda inv: next(item for item in objects(inv, 'Service') if item['metadata']['name'] == 'frontend')['spec']['ports'][0].update({'targetPort': 8080}),
            lambda inv: objects(inv, 'NetworkPolicy')[0]['spec'].update({'podSelector': {'matchLabels': {'foreign': 'selector'}}}),
            lambda inv: inv['items'].append(copy.deepcopy(objects(inv, 'Service')[0])),
            lambda inv: inv['items'].pop(0),
        ]
        for index, mutate in enumerate(mutations):
            inventory = copy.deepcopy(original)
            mutate(inventory)
            with self.subTest(index=index), self.assertRaises(k3s.CandidateK3sError):
                self.parse(plan, stage, inventory)

    def test_pod_controller_image_node_architecture_and_restart_evidence_fail_if_crossed(self):
        plan, stage, original = self.case()
        mutations = [
            lambda inv: objects(inv, 'Pod')[0]['metadata']['ownerReferences'][0].update({'uid': identity('foreign', 'controller')}),
            lambda inv: objects(inv, 'ReplicaSet')[0]['metadata']['ownerReferences'][0].update({'controller': 1}),
            lambda inv: objects(inv, 'Pod')[0]['spec'].update({'nodeName': ROLE_NODES['application']}),
            lambda inv: objects(inv, 'Node')[0]['metadata']['labels'].update({'kubernetes.io/arch': 'amd64'}),
            lambda inv: objects(inv, 'Pod')[0]['status']['containerStatuses'][0].update({'imageID': 'docker-pullable://registry.example/foreign@sha256:' + '1' * 64}),
            lambda inv: objects(inv, 'Pod')[0]['status']['containerStatuses'][0].update({'restartCount': True}),
            lambda inv: objects(inv, 'Pod')[0]['status']['containerStatuses'][0]['state']['running'].update({'startedAt': ''}),
            lambda inv: objects(inv, 'Pod')[0]['status']['containerStatuses'][0].update({'ready': False}),
            lambda inv: objects(inv, 'Pod')[0]['spec'].update({'initContainers': [{'name': 'unsafe'}]}),
            lambda inv: objects(inv, 'Pod')[0]['spec']['containers'].append(copy.deepcopy(objects(inv, 'Pod')[0]['spec']['containers'][0])),
            lambda inv: objects(inv, 'Pod')[0]['spec']['containers'][0].update({'securityContext': {'privileged': True}}),
        ]
        for index, mutate in enumerate(mutations):
            inventory = copy.deepcopy(original)
            mutate(inventory)
            with self.subTest(index=index), self.assertRaises(k3s.CandidateK3sError):
                self.parse(plan, stage, inventory)

    def test_restarts_are_observations_not_erased_or_qualified_away(self):
        plan, stage, inventory = self.case()
        item = objects(inventory, 'Pod')[0]
        item['status']['containerStatuses'][0]['restartCount'] = 3
        item['status']['containerStatuses'][0]['lastState'] = {'terminated': {'exitCode': 1, 'reason': 'Error'}}
        proof = self.parse(plan, stage, inventory)
        recorded = next(pod for pod in proof['support_pods'] if pod['pod_name'] == item['metadata']['name'])
        self.assertEqual(recorded['restart_count'], 3)
        self.assertEqual(recorded['last_state'], {'terminated': {'exitCode': 1, 'reason': 'Error'}})

    def test_default_kubernetes_namespace_objects_are_narrow_not_secret_authorization(self):
        plan, stage, inventory = self.case()
        inventory['items'].extend([
            {'apiVersion': 'v1', 'kind': 'ServiceAccount', 'metadata': {'name': 'default', 'namespace': stage.namespace, 'uid': identity('ServiceAccount', 'default')}},
            {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'kube-root-ca.crt', 'namespace': stage.namespace, 'uid': identity('ConfigMap', 'rootca')}, 'data': {'ca.crt': '-----BEGIN CERTIFICATE-----\nsynthetic-test-only\n'}},
        ])
        self.parse(plan, stage, inventory)
        inventory['items'].append({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'foreign', 'namespace': stage.namespace, 'uid': identity('Secret', 'foreign')}})
        with self.assertRaisesRegex(k3s.CandidateK3sError, 'unexpected resource kind'):
            self.parse(plan, stage, inventory)

    def test_strict_json_and_envelope_crossing_are_rejected(self):
        plan, stage, inventory = self.case()
        valid = observation(plan, stage, inventory)
        bad = [valid.replace('"schema_version": 1', '"schema_version": true', 1),
               valid.replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1', 1),
               valid.replace(plan.run_id, 'abcdef123456'), valid + '{}', b'\xff',
               json.dumps({'inventory': inventory}), valid.replace('"released": false', '"released": true', 1)]
        for output in bad:
            with self.subTest(output=str(output)[:60]), self.assertRaises(k3s.CandidateK3sError):
                k3s.parse_candidate_support_attestation(output, plan, stage, ROLE_NODES)
        malformed = json.loads(valid)
        malformed['inventory']['items'][0]['metadata']['untrusted'] = '\ud800'
        with self.assertRaises(k3s.CandidateK3sError):
            k3s.parse_candidate_support_attestation(json.dumps(malformed), plan, stage, ROLE_NODES)

    def test_saved_proof_raw_inventory_and_normalized_fields_are_revalidated(self):
        plan, stage, inventory = self.case()
        proof = self.parse(plan, stage, inventory)
        for key, value in (('runtime_qualified', True), ('state', 'workload_ready'), ('namespace_uid', identity('foreign', 'namespace')),
                           ('inventory_sha256', '0' * 64), ('extra', True)):
            changed = copy.deepcopy(proof)
            changed[key] = value
            with self.subTest(key=key), self.assertRaises(k3s.CandidateK3sError):
                k3s.validate_saved_candidate_support_attestation(changed, plan, stage, ROLE_NODES)
        changed = copy.deepcopy(proof)
        changed['support_pods'][0]['restart_count'] = 99
        with self.assertRaises(k3s.CandidateK3sError):
            k3s.validate_saved_candidate_support_attestation(changed, plan, stage, ROLE_NODES)

    def test_complete_and_partial_phase_receipts_preserve_exact_uids(self):
        plan, stage, inventory = self.case()
        for phase, payload in stage.phases:
            with self.subTest(phase=phase):
                proof = k3s.parse_candidate_phase_reconciliation(
                    observation(plan, stage, inventory, phase=phase, mode='phase-reconciliation'),
                    plan, stage, phase, ROLE_NODES,
                )
                self.assertTrue(proof['complete'])
                self.assertEqual(len(proof['resources']), len(json.loads(payload)['items']))
                self.assertEqual(k3s.validate_saved_candidate_phase_reconciliation(proof, plan, stage, phase, ROLE_NODES), proof)
        partial = copy.deepcopy(inventory)
        selected = objects(partial, 'PersistentVolume')[0]
        partial['items'].remove(selected)
        proof = k3s.parse_candidate_phase_reconciliation(
            observation(plan, stage, partial, phase='storage', mode='phase-reconciliation'), plan, stage, 'storage', ROLE_NODES,
        )
        self.assertFalse(proof['complete'])
        self.assertEqual(len(proof['resources']), 15)

    def test_missing_namespace_first_phase_is_empty_partial_but_orphan_pv_fails(self):
        plan, stage, inventory = self.case()
        empty = {'apiVersion': 'v1', 'kind': 'List', 'items': objects(inventory, 'Node')}
        proof = k3s.parse_candidate_phase_reconciliation(
            observation(plan, stage, empty, phase='namespace', mode='phase-reconciliation'), plan, stage, 'namespace', ROLE_NODES,
        )
        self.assertFalse(proof['complete'])
        self.assertIsNone(proof['namespace_uid'])
        self.assertEqual(proof['resources'], [])
        empty['items'].append(objects(inventory, 'PersistentVolume')[0])
        with self.assertRaisesRegex(k3s.CandidateK3sError, 'orphaned'):
            k3s.parse_candidate_phase_reconciliation(
                observation(plan, stage, empty, phase='namespace', mode='phase-reconciliation'), plan, stage, 'namespace', ROLE_NODES,
            )

    def test_bad_role_mapping_phase_and_timeout_are_rejected(self):
        plan, stage, _ = self.case()
        bad_roles = [{}, {**ROLE_NODES, 'load_generator': 'load'}, {**ROLE_NODES, 'cache': ROLE_NODES['database']},
                     {**ROLE_NODES, 'control': 'bad node'}, {**ROLE_NODES, 'control': 'a' * 64}]
        for roles in bad_roles:
            with self.assertRaises(k3s.CandidateK3sError):
                k3s.candidate_support_readiness_command(plan, stage, roles)
        with self.assertRaises(k3s.CandidateK3sError):
            k3s.candidate_phase_apply_command(plan, stage, 'initialize', ROLE_NODES)
        for timeout in (True, 0, 601):
            with self.assertRaises(k3s.CandidateK3sError):
                k3s.candidate_support_readiness_command(plan, stage, ROLE_NODES, timeout)

    @unittest.skipUnless(shutil.which('bash'), 'Bash syntax validation is required')
    def test_generated_commands_compile_bound_context_with_no_prune_delete_or_force(self):
        plan, stage, _ = self.case()
        commands = [k3s.candidate_support_readiness_command(plan, stage, ROLE_NODES)]
        for phase, _ in stage.phases:
            commands.extend([k3s.candidate_phase_apply_command(plan, stage, phase, ROLE_NODES),
                             k3s.candidate_phase_reconciliation_command(plan, stage, phase, ROLE_NODES)])
        for command in commands:
            argv = shlex.split(command)
            self.assertEqual(argv[:3], ['sudo', 'python3', '-c'])
            compile(argv[3], '<candidate-remote>', 'exec')
            checked = subprocess.run([shutil.which('bash'), '-n', '-c', command], capture_output=True, text=True)
            self.assertEqual(checked.returncode, 0, checked.stderr)
            self.assertLess(len(command.encode()), 120000)
            self.assertNotIn('--force-conflicts', command)
            self.assertNotIn('--prune', command)
            self.assertNotIn("'delete'", command)

    def test_remote_helpers_are_frozen_at_module_import(self):
        plan, stage, _ = self.case()
        with patch.object(k3s.inspect, 'getsource', side_effect=AssertionError('runtime source reread')):
            command = k3s.candidate_support_readiness_command(plan, stage, ROLE_NODES)
        compile(shlex.split(command)[3], '<frozen-candidate-remote>', 'exec')


class CandidateRemoteSimulationTests(_CandidateCaseMixin, unittest.TestCase):
    """Execute generated Python with a fake API and a private test lock path."""

    def execute_remote(self, command, inventory, *, payload=b'', conflict=False, change_uid_after_dry=False, change_service_uid_after_dry=False, legacy_zip=False):
        records = []
        self.last_remote_calls = records
        current = copy.deepcopy(inventory)
        source = shlex.split(command)[3]
        with tempfile.TemporaryDirectory() as temporary:
            lock_directory = str(Path(temporary).resolve() / 'namespace-locks')
            source = source.replace('/run/deathstarbench-candidate', lock_directory)
            real_lstat, real_fstat = os.lstat, os.fstat

            def root_stat(details):
                return SimpleNamespace(st_mode=details.st_mode, st_nlink=details.st_nlink, st_uid=0, st_gid=0)

            def run(argv, **kwargs):
                args = argv[2:]
                records.append(args)
                if 'wait' in args:
                    return SimpleNamespace(returncode=0, stdout=b'condition met\n')
                if 'get' in args:
                    index = args.index('get')
                    resource = args[index + 1]
                    if resource == 'namespace':
                        names = objects(current, 'Namespace')
                        document = names[0] if names else None
                    elif resource == 'persistentvolumes':
                        document = {'apiVersion': 'v1', 'kind': 'List', 'items': objects(current, 'PersistentVolume')}
                    elif resource == 'nodes':
                        document = {'apiVersion': 'v1', 'kind': 'List', 'items': objects(current, 'Node')}
                    else:
                        document = {'apiVersion': 'v1', 'kind': 'List', 'items': [item for item in current['items'] if item['kind'] not in {'Node', 'Namespace', 'PersistentVolume'}]}
                    return SimpleNamespace(returncode=0, stdout=json.dumps(document).encode() if document is not None else b'')
                if 'create' in args:
                    item = json.loads(kwargs['input'])['items'][0]
                    item['metadata']['uid'] = identity('Namespace', item['metadata']['name'])
                    item['status'] = {'phase': 'Active'}
                    if conflict:
                        item['metadata']['annotations'][staging.RUN_ANNOTATION] = 'abcdef123456'
                    current['items'].append(item)
                    return SimpleNamespace(returncode=1 if conflict else 0, stdout=b'')
                if 'apply' in args:
                    if '--dry-run=server' in args and change_uid_after_dry:
                        objects(current, 'Namespace')[0]['metadata']['uid'] = identity('recreated', 'namespace')
                    if '--dry-run=server' in args and change_service_uid_after_dry:
                        objects(current, 'Service')[0]['metadata']['uid'] = identity('recreated', 'service')
                    return SimpleNamespace(returncode=0, stdout=b'')
                raise AssertionError(args)

            captured = io.StringIO()
            try:
                with patch.object(subprocess, 'run', side_effect=run), patch.object(os, 'geteuid', return_value=0), \
                     patch.object(os, 'lstat', side_effect=lambda path: root_stat(real_lstat(path))), \
                     patch.object(os, 'fstat', side_effect=lambda descriptor: root_stat(real_fstat(descriptor))), \
                     patch('sys.stdin', SimpleNamespace(buffer=io.BytesIO(payload))), redirect_stdout(captured):
                    namespace = {'__name__': '__candidate_remote__'}
                    if legacy_zip:
                        # Match Python 3.9's no-keyword zip signature even when
                        # this regression runs on the newer CI interpreters.
                        namespace['zip'] = lambda *values: zip(*values)
                    exec(compile(source, '<simulated-candidate-remote>', 'exec'), namespace)
            except k3s.CandidateK3sError:
                raise
            except ValueError as exc:
                # The embedded class is intentionally independent of installed
                # app modules, but failures retain the same semantic boundary.
                raise k3s.CandidateK3sError(str(exc)) from exc
        return records, captured.getvalue()

    def test_guest_python39_zip_keeps_exact_inventory_checks(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            with self.subTest(workload=workload):
                plan, stage, inventory = self.case(workload)
                for command in (
                    k3s.candidate_phase_reconciliation_command(plan, stage, 'services', ROLE_NODES),
                    k3s.candidate_support_readiness_command(plan, stage, ROLE_NODES),
                ):
                    records, output = self.execute_remote(command, inventory, legacy_zip=True)
                    self.assertTrue(records)
                    self.assertTrue(json.loads(output)['inventory']['items'])
                    drifted = copy.deepcopy(inventory)
                    objects(drifted, 'Service')[0]['spec']['ports'].append({'port': 12345})
                    with self.assertRaisesRegex(k3s.CandidateK3sError, 'array inventory drifted'):
                        self.execute_remote(command, drifted, legacy_zip=True)

    def test_readonly_reconciliation_and_support_waits_execute_no_mutations(self):
        plan, stage, inventory = self.case()
        command = k3s.candidate_phase_reconciliation_command(plan, stage, 'storage', ROLE_NODES)
        records, output = self.execute_remote(command, inventory)
        self.assertTrue(all('get' in args for args in records))
        self.assertTrue(k3s.parse_candidate_phase_reconciliation(output, plan, stage, 'storage', ROLE_NODES)['complete'])
        records, output = self.execute_remote(k3s.candidate_support_readiness_command(plan, stage, ROLE_NODES), inventory)
        waits = [args for args in records if 'wait' in args]
        self.assertEqual(len(waits), 2)
        self.assertTrue(all('deployment/' + name in waits[0] for name in stage.active_support_components))
        self.assertTrue(all('deployment/' + name not in waits[0] for name in stage.blocked_application_components))
        self.assertFalse(any('apply' in args or 'create' in args for args in records))
        self.assertEqual(self.parse(plan, stage, json.loads(output)['inventory'])['state'], 'support_ready_applications_paused')

    def test_guarded_apply_hashes_stdin_and_emits_readonly_phase_receipt(self):
        plan, stage, inventory = self.case()
        phase = 'blocked-applications'
        payload = dict(stage.phases)[phase].encode()
        command = k3s.candidate_phase_apply_command(plan, stage, phase, ROLE_NODES)
        records, output = self.execute_remote(command, inventory, payload=payload)
        applies = [args for args in records if 'apply' in args]
        self.assertEqual(len(applies), 2)
        self.assertIn('--dry-run=server', applies[0])
        self.assertNotIn('--dry-run=server', applies[1])
        self.assertTrue(k3s.parse_candidate_phase_reconciliation(output, plan, stage, phase, ROLE_NODES)['complete'])
        with self.assertRaisesRegex(k3s.CandidateK3sError, 'SHA-256'):
            self.execute_remote(command, inventory, payload=payload + b' ')
        self.assertEqual(self.last_remote_calls, [])

    def test_foreign_namespace_or_started_app_refuses_before_apply(self):
        plan, stage, original = self.case()
        command = k3s.candidate_phase_apply_command(plan, stage, 'services', ROLE_NODES)
        for mutate in (
            lambda inv: objects(inv, 'Namespace')[0]['metadata']['annotations'].update({staging.RUN_ANNOTATION: 'abcdef123456'}),
            lambda inv: next(item for item in objects(inv, 'Deployment') if item['spec']['replicas'] == 0)['spec'].update({'replicas': 1}),
        ):
            inventory = copy.deepcopy(original)
            mutate(inventory)
            with self.assertRaises(k3s.CandidateK3sError):
                self.execute_remote(command, inventory, payload=dict(stage.phases)['services'].encode())
            self.assertFalse(any('apply' in args or 'create' in args for args in self.last_remote_calls))

    def test_namespace_create_conflict_cannot_take_over_foreign_owner(self):
        plan, stage, original = self.case()
        inventory = {'apiVersion': 'v1', 'kind': 'List', 'items': objects(original, 'Node')}
        command = k3s.candidate_phase_apply_command(plan, stage, 'namespace', ROLE_NODES)
        records, output = self.execute_remote(command, inventory, payload=dict(stage.phases)['namespace'].encode())
        self.assertTrue(any('create' in args for args in records))
        self.assertFalse(any('apply' in args for args in records))
        self.assertTrue(k3s.parse_candidate_phase_reconciliation(output, plan, stage, 'namespace', ROLE_NODES)['complete'])
        with self.assertRaises(k3s.CandidateK3sError):
            self.execute_remote(command, inventory, payload=dict(stage.phases)['namespace'].encode(), conflict=True)
        self.assertFalse(any('apply' in args for args in self.last_remote_calls))

    def test_namespace_uid_change_after_dryrun_refuses_actual_apply(self):
        plan, stage, inventory = self.case()
        command = k3s.candidate_phase_apply_command(plan, stage, 'services', ROLE_NODES)
        with self.assertRaisesRegex(k3s.CandidateK3sError, 'UID changed'):
            self.execute_remote(command, inventory, payload=dict(stage.phases)['services'].encode(), change_uid_after_dry=True)
        applies = [args for args in self.last_remote_calls if 'apply' in args]
        self.assertEqual(len(applies), 1)
        self.assertIn('--dry-run=server', applies[0])

    def test_same_owned_service_recreated_during_dryrun_refuses_actual_apply(self):
        plan, stage, inventory = self.case()
        command = k3s.candidate_phase_apply_command(plan, stage, 'services', ROLE_NODES)
        with self.assertRaisesRegex(k3s.CandidateK3sError, 'object UID changed'):
            self.execute_remote(command, inventory, payload=dict(stage.phases)['services'].encode(), change_service_uid_after_dry=True)
        applies = [args for args in self.last_remote_calls if 'apply' in args]
        self.assertEqual(len(applies), 1)
        self.assertIn('--dry-run=server', applies[0])

    def test_unknown_candidate_pv_cannot_hide_behind_other_namespace_or_name(self):
        plan, stage, inventory = self.case()
        extra = copy.deepcopy(objects(inventory, 'PersistentVolume')[0])
        extra['metadata']['name'] = 'unexpected-owned-volume'
        extra['metadata']['uid'] = identity('foreign', 'volume')
        extra['spec']['claimRef']['namespace'] = 'some-other-namespace'
        inventory['items'].append(extra)
        command = k3s.candidate_phase_apply_command(plan, stage, 'services', ROLE_NODES)
        with self.assertRaisesRegex(k3s.CandidateK3sError, 'unexpected object'):
            self.execute_remote(command, inventory, payload=dict(stage.phases)['services'].encode())
        self.assertFalse(any('apply' in args or 'create' in args for args in self.last_remote_calls))


if __name__ == '__main__':
    unittest.main()
