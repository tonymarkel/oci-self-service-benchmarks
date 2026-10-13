"""Simulated SSH/state failure paths; no live cloud or Kubernetes claims.

Real artifact binding, stage derivation, provider inventory parsing, run leases,
storage parsers and deployment journals execute. Kubernetes command/attestation
boundaries are stubbed here and separately tested with full API inventories.
"""

from contextlib import ExitStack
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from app import deathstarbench_candidate_deployment as deploy
from app import deathstarbench_distributed as cluster
from app.deathstarbench_contract import DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
from app.run_lease import RunLease, acquire_run_lease
from tests import test_deathstarbench_candidate_artifacts as artifact_fixtures
from tests import test_deathstarbench_candidate_storage as storage_fixtures
from tests import test_deathstarbench_candidate_storage_observation as observation_fixtures
from tests import test_deathstarbench_distributed_runtime as cluster_fixtures


class SimulatedStageSSH:
    def __init__(self, plan, stage, provider='azure'):
        self.plan, self.stage = plan, stage
        self.calls = []
        self.objects = {}
        self.namespace_uid = None
        self.fail_before = None
        self.lose_response_at = None
        self.partial_at = None
        self.mutate = None
        self.guard_failure = False
        self.base = cluster_fixtures.GcpFakeRemoteExecutor() if provider == 'gcp' else cluster_fixtures.FakeRemoteExecutor()

    def receipt(self, phase):
        resources = self.objects.get(phase, [])
        desired = json.loads(dict(self.stage.phases)[phase])['items']
        return {
            'phase': phase, 'complete': len(resources) == len(desired),
            'namespace_uid': self.namespace_uid, 'resources': copy.deepcopy(resources),
            'run_id': self.plan.run_id, 'snapshot_fingerprint': self.plan.fingerprint,
            'stage_fingerprint': self.stage.fingerprint, 'candidate_only': True,
            'released': False,
            'inventory': self.inventory(),
        }

    def inventory(self):
        return {'apiVersion': 'v1', 'kind': 'List', 'items': [{
            'kind': item['kind'], 'metadata': {
                'name': item['name'], 'namespace': item['namespace'], 'uid': item['uid'],
            },
        } for resources in self.objects.values() for item in resources]}

    def identities(self, phase):
        return sorted(({
            'kind': item['kind'], 'name': item['metadata']['name'],
            'namespace': item['metadata'].get('namespace'),
            'uid': 'uid-' + item['kind'] + '-' + item['metadata']['name'],
        } for item in json.loads(dict(self.stage.phases)[phase])['items']), key=lambda item: (item['kind'], item['name']))

    def __call__(self, job, command, **kwargs):
        self.calls.append((command, kwargs))
        if self.fail_before is not None and self.fail_before(command):
            raise RuntimeError('simulated transport failure')
        if command.startswith('reconcile:'):
            if self.guard_failure:
                raise ValueError('simulated foreign/started-application namespace')
            return json.dumps(self.receipt(command.split(':', 1)[1]))
        if command.startswith('apply:'):
            phase = command.split(':', 1)[1]
            self.assert_payload(phase, kwargs)
            self.objects[phase] = self.identities(phase)
            if phase == 'namespace':
                self.namespace_uid = self.objects[phase][0]['uid']
            if self.partial_at == phase:
                self.objects[phase] = self.objects[phase][:max(1, len(self.objects[phase]) // 2)]
                self.partial_at = None
                raise RuntimeError('simulated partial apply response loss')
            if self.lose_response_at == phase:
                self.lose_response_at = None
                raise RuntimeError('simulated successful apply response loss')
            if self.mutate is not None:
                self.mutate(job)
                self.mutate = None
            return ''
        if command == 'support-ready':
            return json.dumps({
                'run_id': self.plan.run_id, 'snapshot_fingerprint': self.plan.fingerprint,
                'stage_fingerprint': self.stage.fingerprint,
                'namespace_uid': self.namespace_uid,
                'scope': 'support-only', 'application_start_allowed': False,
                'blocked_application_components': list(self.stage.blocked_application_components),
                'candidate_only': True, 'released': False,
                'inventory': self.inventory(),
            })
        if 'CANDIDATE_DSB_DATABASE_STORAGE_OBSERVATION' in command:
            return observation_fixtures.marker(self.plan.storage) + '\n'
        if 'CANDIDATE_DSB_DATABASE_STORAGE' in command:
            return storage_fixtures.marker(self.plan.storage) + '\n'
        return self.base(job, command, **kwargs)

    def assert_payload(self, phase, kwargs):
        if kwargs.get('stdin_text') != dict(self.stage.phases)[phase]:
            raise AssertionError('Only the exact stage phase may be sent to SSH')
        for item in json.loads(kwargs['stdin_text'])['items']:
            if item['kind'] == 'Deployment':
                role = item['metadata']['labels']['deathstarbench.io/role']
                if item['spec']['replicas'] != (0 if role == 'application' else 1):
                    raise AssertionError('An application start crossed the stage barrier')


class CandidateSupportDeploymentTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.root = root
        self.fixture = artifact_fixtures.CandidateArtifactAdoptionTests()
        self.addCleanup(self.fixture.doCleanups)
        self.persisted = []

        def saved_phase(value, plan, stage, phase, roles):
            if not isinstance(value, dict) or value.get('phase') != phase or value.get('run_id') != plan.run_id or value.get('snapshot_fingerprint') != plan.fingerprint or value.get('stage_fingerprint') != stage.fingerprint or type(value.get('complete')) is not bool:
                raise ValueError('simulated phase evidence schema drift')
            if value.get('candidate_only') is not True or value.get('released') is not False:
                raise ValueError('simulated gate promotion')
            return copy.deepcopy(value)

        def saved_ready(value, plan, stage, roles):
            if not isinstance(value, dict) or value.get('run_id') != plan.run_id or value.get('snapshot_fingerprint') != plan.fingerprint or value.get('stage_fingerprint') != stage.fingerprint or value.get('application_start_allowed') is not False:
                raise ValueError('simulated support readiness identity drift')
            return copy.deepcopy(value)

        patches = {
            'candidate_phase_apply_command': lambda plan, stage, phase, roles: 'apply:' + phase,
            'candidate_phase_reconciliation_command': lambda plan, stage, phase, roles: 'reconcile:' + phase,
            'parse_candidate_phase_reconciliation': lambda output, plan, stage, phase, roles: saved_phase(json.loads(output), plan, stage, phase, roles),
            'validate_saved_candidate_phase_reconciliation': saved_phase,
            'candidate_support_readiness_command': lambda plan, stage, roles: 'support-ready',
            'parse_candidate_support_attestation': lambda output, plan, stage, roles: saved_ready(json.loads(output), plan, stage, roles),
            'validate_saved_candidate_support_attestation': saved_ready,
        }
        for name, value in patches.items():
            self.stack.enter_context(mock.patch.object(deploy, name, side_effect=value))

    def setup_run(self, workload='hotel_reservation', provider='azure'):
        job = cluster_fixtures.candidate_job() if provider == 'azure' else cluster_fixtures.gcp_candidate_job()
        if provider == 'azure':
            from app.providers import azure
            job['resources']['azure_resource_group_tags'] = azure._tags(job['id'], 'resource-group')
            cluster.prepare_azure_distributed_k3s_candidate(job, execute=cluster_fixtures.FakeRemoteExecutor())
        else:
            from app.providers import gcp
            job['resources']['gcp_resource_prefix'] = gcp._resource_name(job['id'], 'dsb')
            cluster.prepare_gcp_distributed_k3s_candidate(job, execute=cluster_fixtures.GcpFakeRemoteExecutor())
        artifacts = self.fixture.adopt(self.fixture.fixture(workload))
        _, plan, stage, identity = deploy._context(job, artifacts, cluster.distributed_k3s_candidate_plan)
        run_dir = self.root / job['id']
        run_dir.mkdir(exist_ok=True)
        lease = self.stack.enter_context(acquire_run_lease(self.root, job['id'], owner_label='candidate-stage-tests'))
        remote = SimulatedStageSSH(plan, stage, provider)
        return job, artifacts, plan, stage, identity, lease, remote

    def persist(self, job):
        self.persisted.append(copy.deepcopy(job['resources'][deploy.SUPPORT_STAGE_JOURNAL_KEY]))

    def run_stage(self, job, artifacts, lease, remote, **kwargs):
        return deploy.prepare_candidate_support_stage(job, artifacts, lease=lease, execute=remote, persist=kwargs.pop('persist', self.persist), **kwargs)

    def test_both_workloads_stage_support_only_and_retain_exact_run_cluster_identity(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            job, artifacts, plan, stage, identity, lease, remote = self.setup_run(workload)
            journal = self.run_stage(job, artifacts, lease, remote)
            self.assertEqual(journal['state'], deploy.READY_STATE)
            self.assertEqual(journal['applied_phases'], [name for name, _ in stage.phases])
            self.assertEqual(journal['cluster_identity'], identity)
            self.assertEqual(journal['snapshot_fingerprint'], plan.fingerprint)
            self.assertEqual(journal['storage_preparation']['database_count'], 8)
            self.assertEqual(journal['storage_observation']['available_bytes'], 0)
            self.assertTrue(all(value is False for value in journal['qualification'].values()))
            self.assertEqual(job['results'], [])
            self.assertNotIn(cluster.WORKLOAD_JOURNAL_KEY, job['resources'])
            self.assertEqual(len([command for command, _ in remote.calls if command.startswith('apply:')]), 6)
            self.assertEqual(deploy.validate_candidate_support_journal(journal, plan, stage, identity), journal)
            lease.release()

    def test_gcp_uses_provider_disk_and_routes_without_social_storage_or_apps(self):
        job, artifacts, plan, stage, _, lease, remote = self.setup_run(provider='gcp')
        journal = self.run_stage(job, artifacts, lease, remote)
        self.assertEqual(journal['snapshot']['provider'], 'gcp')
        self.assertTrue(any('GCP_DSB_DATABASE_VOLUME' in command for command, _ in remote.calls))
        self.assertFalse(any('DSB_WORKLOAD_STORAGE' in command for command, _ in remote.calls))
        self.assertTrue(all(kwargs['host_key'].startswith('gcp_dsb_') for _, kwargs in remote.calls))

    def test_end_to_end_journal_with_real_k3s_parsers_and_generated_commands(self):
        from app import deathstarbench_candidate_k3s as k3s
        from tests import test_deathstarbench_candidate_k3s as api_fixtures

        job, artifacts, plan, stage, identity, lease, remote = self.setup_run('media_microservices')
        roles = identity['role_nodes']
        full = api_fixtures.live_inventory(stage, roles)
        commands = []

        def rendered(generator, prefix):
            def generate(*args):
                commands.append(generator(*args))
                return prefix + (args[2] if prefix != 'support-ready' else '')
            return generate

        def current_inventory():
            static = {(item['kind'], item['name']) for resources in remote.objects.values() for item in resources}
            deployments = {name for kind, name in static if kind == 'Deployment'}
            items = []
            for item in full['items']:
                kind, metadata = item['kind'], item['metadata']
                if kind == 'Node' or (kind, metadata['name']) in static:
                    items.append(copy.deepcopy(item))
                elif kind in {'Pod', 'ReplicaSet'} and metadata['labels']['app.kubernetes.io/component'] in deployments:
                    items.append(copy.deepcopy(item))
            return {'apiVersion': 'v1', 'kind': 'List', 'items': items}

        remote.identities = lambda phase: sorted(({
            'kind': item['kind'], 'name': item['metadata']['name'],
            'namespace': item['metadata'].get('namespace'),
            'uid': api_fixtures.identity(item['kind'], item['metadata']['name']),
        } for item in json.loads(dict(stage.phases)[phase])['items']), key=lambda item: (item['kind'], item['name']))
        original_execute = remote.__call__

        def execute(job, command, **kwargs):
            if command.startswith('reconcile:'):
                remote.calls.append((command, kwargs))
                return api_fixtures.observation(plan, stage, current_inventory(), phase=command.split(':', 1)[1], mode='phase-reconciliation', roles=roles)
            if command == 'support-ready':
                remote.calls.append((command, kwargs))
                return api_fixtures.observation(plan, stage, current_inventory(), roles=roles)
            return original_execute(job, command, **kwargs)

        for name in ('parse_candidate_phase_reconciliation', 'validate_saved_candidate_phase_reconciliation',
                     'parse_candidate_support_attestation', 'validate_saved_candidate_support_attestation'):
            self.stack.enter_context(mock.patch.object(deploy, name, getattr(k3s, name)))
        self.stack.enter_context(mock.patch.object(deploy, 'candidate_phase_apply_command', side_effect=rendered(k3s.candidate_phase_apply_command, 'apply:')))
        self.stack.enter_context(mock.patch.object(deploy, 'candidate_phase_reconciliation_command', side_effect=rendered(k3s.candidate_phase_reconciliation_command, 'reconcile:')))
        self.stack.enter_context(mock.patch.object(deploy, 'candidate_support_readiness_command', side_effect=rendered(k3s.candidate_support_readiness_command, 'support-ready')))
        journal = self.run_stage(job, artifacts, lease, execute)
        self.assertEqual(journal['state'], deploy.READY_STATE)
        self.assertEqual(len(journal['support_attestation']['support_pods']), 19)
        self.assertEqual(len(journal['support_attestation']['blocked_applications']), 13)
        self.assertEqual(journal['support_attestation']['application_pods_observed'], 0)
        self.assertFalse(journal['support_attestation']['dataset_qualified'])
        self.assertTrue(commands)
        self.assertEqual(deploy.validate_candidate_support_journal(journal, plan, stage, identity), journal)

    def test_second_invocation_revokes_ready_and_only_observes_existing_storage(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        first = self.run_stage(job, artifacts, lease, remote)
        self.persisted.clear()
        remote.calls.clear()
        second = self.run_stage(job, artifacts, lease, remote)
        self.assertEqual(self.persisted[0]['state'], 'revalidating')
        self.assertIsNone(self.persisted[0]['support_attestation'])
        self.assertIsNone(self.persisted[0]['storage_observation'])
        self.assertEqual(second['phase_receipts'], first['phase_receipts'])
        self.assertFalse(any(command.startswith('apply:') for command, _ in remote.calls))
        self.assertFalse(any('CANDIDATE_DSB_DATABASE_STORAGE schema=' in command for command, _ in remote.calls))
        self.assertTrue(any('STORAGE_OBSERVATION' in command for command, _ in remote.calls))

    def test_retry_transport_failure_never_leaves_stale_ready_state(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        self.run_stage(job, artifacts, lease, remote)
        remote.fail_before = lambda command: True
        with self.assertRaisesRegex(RuntimeError, 'transport'):
            self.run_stage(job, artifacts, lease, remote)
        journal = job['resources'][deploy.SUPPORT_STAGE_JOURNAL_KEY]
        self.assertEqual(journal['state'], 'revalidating')
        self.assertIsNone(journal['support_attestation'])
        self.assertIsNone(journal['storage_observation'])

    def test_success_response_loss_is_reconciled_without_reapplying_phase(self):
        for phase in ('namespace', 'support-workloads'):
            job, artifacts, _, _, _, lease, remote = self.setup_run()
            remote.lose_response_at = phase
            with self.assertRaisesRegex(RuntimeError, 'response loss'):
                self.run_stage(job, artifacts, lease, remote)
            self.assertEqual(job['resources'][deploy.SUPPORT_STAGE_JOURNAL_KEY]['pending_phase'], phase)
            count_before = sum(command == 'apply:' + phase for command, _ in remote.calls)
            result = self.run_stage(job, artifacts, lease, remote)
            self.assertEqual(result['state'], deploy.READY_STATE)
            self.assertEqual(sum(command == 'apply:' + phase for command, _ in remote.calls), count_before)
            lease.release()

    def test_partial_response_loss_requires_same_uids_then_applies_only_missing_phase(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        remote.partial_at = 'storage'
        with self.assertRaisesRegex(RuntimeError, 'partial'):
            self.run_stage(job, artifacts, lease, remote)
        pending = job['resources'][deploy.SUPPORT_STAGE_JOURNAL_KEY]['pending_receipt']
        self.assertEqual(pending['resources'], [])  # Durable pre-dispatch state.
        self.run_stage(job, artifacts, lease, remote)
        self.assertEqual(sum(command == 'apply:storage' for command, _ in remote.calls), 2)
        self.assertEqual(sum(command == 'apply:namespace' for command, _ in remote.calls), 1)

    def test_pending_phase_cannot_erase_its_pre_dispatch_identity_receipt(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        remote.lose_response_at = 'namespace'
        with self.assertRaises(RuntimeError):
            self.run_stage(job, artifacts, lease, remote)
        job['resources'][deploy.SUPPORT_STAGE_JOURNAL_KEY]['pending_receipt'] = None
        remote.calls.clear()
        with self.assertRaisesRegex(deploy.CandidateDeploymentError, 'paired'):
            self.run_stage(job, artifacts, lease, remote)
        self.assertEqual(remote.calls, [])

    def test_completed_or_observed_partial_object_replacement_is_refused(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        self.run_stage(job, artifacts, lease, remote)
        remote.objects['storage'][0]['uid'] += '-replaced'
        remote.calls.clear()
        with self.assertRaisesRegex(deploy.CandidateDeploymentError, 'replaced'):
            self.run_stage(job, artifacts, lease, remote)
        self.assertFalse(any(command.startswith('apply:') for command, _ in remote.calls))

    def test_replacement_after_phase_receipt_is_refused_before_final_ready(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        original = remote.__call__
        def replace_before_readiness(job, command, **kwargs):
            if command == 'support-ready':
                remote.objects['storage'][0]['uid'] += '-replaced'
            return original(job, command, **kwargs)
        with self.assertRaisesRegex(deploy.CandidateDeploymentError, 'replaced'):
            self.run_stage(job, artifacts, lease, replace_before_readiness)
        self.assertEqual(job['resources'][deploy.SUPPORT_STAGE_JOURNAL_KEY]['state'], 'attesting_support')
        self.assertIsNone(job['resources'][deploy.SUPPORT_STAGE_JOURNAL_KEY]['support_attestation'])

    def test_completed_static_replacement_between_phases_prevents_next_apply(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        original = remote.__call__
        def replace_before_policies(job, command, **kwargs):
            if command == 'reconcile:policies':
                remote.objects['storage'][0]['uid'] += '-replaced'
            return original(job, command, **kwargs)
        with self.assertRaisesRegex(deploy.CandidateDeploymentError, 'replaced'):
            self.run_stage(job, artifacts, lease, replace_before_policies)
        self.assertFalse(any(command == 'apply:policies' for command, _ in remote.calls))

    def test_crossed_workload_or_promoted_journal_fails_before_ssh(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        self.run_stage(job, artifacts, lease, remote)
        other = self.fixture.adopt(self.fixture.fixture('media_microservices'))
        remote.calls.clear()
        with self.assertRaises(deploy.CandidateDeploymentError):
            self.run_stage(job, other, lease, remote)
        self.assertEqual(remote.calls, [])
        job['resources'][deploy.SUPPORT_STAGE_JOURNAL_KEY]['qualification']['public_release'] = True
        with self.assertRaises(deploy.CandidateDeploymentError):
            self.run_stage(job, artifacts, lease, remote)
        self.assertEqual(remote.calls, [])

    def test_copied_provider_inventory_cannot_change_its_owning_run(self):
        for provider in ('azure', 'gcp'):
            job, artifacts, _, _, _, lease, remote = self.setup_run(provider=provider)
            copied = copy.deepcopy(job)
            copied['id'] = '123456abcdef'
            other_dir = self.root / copied['id']
            other_dir.mkdir(exist_ok=True)
            other_lease = self.stack.enter_context(acquire_run_lease(self.root, copied['id'], owner_label='candidate-stage-tests'))
            with self.assertRaisesRegex(deploy.CandidateDeploymentError, 'this run'):
                self.run_stage(copied, artifacts, other_lease, remote)
            self.assertEqual(remote.calls, [])
            lease.release()
            other_lease.release()

    def test_all_provider_owner_contracts_bind_the_exact_job_id(self):
        from app.providers import aws, azure, gcp, oci
        run_id = '123456abcdef'
        resources = {
            'azure_resource_group_tags': azure._tags(run_id, 'resource-group'),
            'gcp_resource_prefix': gcp._resource_name(run_id, 'dsb'),
            aws.AWS_DSB_GRAPH_KEY: {'contract': {'job_id': run_id}},
            oci.CONTRACT_KEY: {'job_id': run_id},
        }
        for provider in ('aws', 'azure', 'gcp', 'oci'):
            binding = deploy._provider_run_binding({'id': run_id}, resources, provider)
            self.assertEqual(binding['run_id'], run_id)
            with self.assertRaisesRegex(deploy.CandidateDeploymentError, 'this run'):
                deploy._provider_run_binding({'id': 'abcdef123456'}, resources, provider)

    def test_fabricated_or_copied_lease_cannot_authorize_persistence_or_ssh(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        fabricated = RunLease(
            file_descriptor=-1, legacy_file_descriptor=-1,
            path=lease.path, owner=lease.owner,
        )
        for unacquired in (fabricated, copy.copy(lease)):
            with self.assertRaisesRegex(deploy.CandidateDeploymentError, 'still-held lease'):
                self.run_stage(job, artifacts, unacquired, remote)
        self.assertEqual(remote.calls, [])
        self.assertEqual(self.persisted, [])

    def test_release_during_ssh_prevents_further_work_and_ready_persistence(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        def release_after_ssh(job, command, **kwargs):
            result = remote(job, command, **kwargs)
            lease.release()
            return result
        with self.assertRaisesRegex(deploy.CandidateDeploymentError, 'still-held lease'):
            self.run_stage(job, artifacts, lease, release_after_ssh)
        self.assertEqual(len(remote.calls), 1)
        self.assertEqual(len(self.persisted), 1)
        self.assertEqual(self.persisted[0]['state'], 'preflight_only')

    def test_wrong_or_released_lease_and_missing_persistence_fail_before_ssh(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        with self.assertRaises(deploy.CandidateDeploymentError):
            self.run_stage(job, artifacts, lease, remote, persist=None)
        lease.release()
        with self.assertRaises(deploy.CandidateDeploymentError):
            self.run_stage(job, artifacts, lease, remote)
        self.assertEqual(remote.calls, [])

    def test_existing_execution_and_boolean_runtime_schema_are_rejected_before_ssh(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {}
        with self.assertRaises(deploy.CandidateDeploymentError):
            self.run_stage(job, artifacts, lease, remote)
        job['resources'].pop(DEATHSTARBENCH_EXECUTION_JOURNAL_KEY)
        job['resources'][cluster.RUNTIME_JOURNAL_KEY]['schema_version'] = True
        with self.assertRaises(deploy.CandidateDeploymentError):
            self.run_stage(job, artifacts, lease, remote)
        self.assertEqual(remote.calls, [])

    def test_namespace_guard_failure_precedes_storage_or_manifest_mutation(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        remote.guard_failure = True
        with self.assertRaisesRegex(ValueError, 'foreign'):
            self.run_stage(job, artifacts, lease, remote)
        self.assertFalse(any(command.startswith('apply:') or 'CANDIDATE_DSB_DATABASE_STORAGE schema=' in command for command, _ in remote.calls))

    def test_inventory_and_route_drift_mid_phase_stops_before_next_mutation(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        remote.mutate = lambda job: job['resources'].update(azure_dsb_control_public_ip='203.0.113.250')
        with self.assertRaises(ValueError):
            self.run_stage(job, artifacts, lease, remote)
        self.assertEqual([command for command, _ in remote.calls if command.startswith('apply:')], ['apply:namespace'])

    def test_persist_failure_records_intent_but_never_dispatches_that_phase(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        def failed_persist(job):
            self.persist(job)
            if job['resources'][deploy.SUPPORT_STAGE_JOURNAL_KEY]['state'] == 'applying_phase':
                raise RuntimeError('durability failure')
        with self.assertRaisesRegex(RuntimeError, 'durability'):
            self.run_stage(job, artifacts, lease, remote, persist=failed_persist)
        self.assertFalse(any(command.startswith('apply:') for command, _ in remote.calls))

    def test_persist_callback_cannot_erase_or_replace_the_required_intent(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        def erased_persist(job):
            job['resources'].pop(deploy.SUPPORT_STAGE_JOURNAL_KEY)
        with self.assertRaisesRegex(deploy.CandidateDeploymentError, 'journal was removed or changed'):
            self.run_stage(job, artifacts, lease, remote, persist=erased_persist)
        self.assertEqual(remote.calls, [])
        def changed_persist(job):
            job['resources'][deploy.SUPPORT_STAGE_JOURNAL_KEY]['state'] = 'revalidating'
        with self.assertRaisesRegex(deploy.CandidateDeploymentError, 'journal was removed or changed'):
            self.run_stage(job, artifacts, lease, remote, persist=changed_persist)
        self.assertEqual(remote.calls, [])

    def test_ssh_callback_cannot_erase_the_phase_journal(self):
        job, artifacts, _, _, _, lease, remote = self.setup_run()
        def erase_after_ssh(job, command, **kwargs):
            result = remote(job, command, **kwargs)
            job['resources'].pop(deploy.SUPPORT_STAGE_JOURNAL_KEY)
            return result
        with self.assertRaisesRegex(deploy.CandidateDeploymentError, 'journal was removed or changed'):
            self.run_stage(job, artifacts, lease, erase_after_ssh)
        self.assertEqual(len(remote.calls), 1)
