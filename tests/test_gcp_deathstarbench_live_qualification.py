from __future__ import annotations

import argparse
from pathlib import Path
import threading
import unittest
from unittest import mock

from app.deathstarbench_contract import (
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    K3S_RUNTIME_JOURNAL_KEY,
)
from scripts import qualify_gcp_deathstarbench_distributed as qualification


class GcpDistributedQualificationTests(unittest.TestCase):
    def args(self, **overrides):
        values = {
            'resume': None,
            'cleanup_only': None,
            'image_lock': Path('/candidate/lock.json'),
            'project_id': 'benchmark-project',
            'region': 'us-east1',
            'zone': 'us-east1-b',
            'application_machine_type': 'c4a-standard-8',
            'application_vcpus': 8,
            'application_memory_gib': 32,
            'measure': False,
            'interrupt_at': None,
            'interrupt_mode': None,
            'warmup_seconds': 30,
            'duration_seconds': 60,
            'threads': 4,
            'connections': 4,
            'request_rate': 100,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def ssh(self):
        return {
            'private_key': 'PRIVATE',
            'public_key': 'ssh-ed25519 QUFBQQ== benchmark',
            'passphrase': None,
        }

    def job(self, plan=None):
        if plan is None:
            plan = qualification._plan(self.args(), self.ssh())
        return {
            'id': 'abc123def456',
            'plan': plan.model_dump(
                exclude={
                    'ssh_private_key',
                    'ssh_public_key',
                    'ssh_key_passphrase',
                }
            ),
            '_key': 'PRIVATE',
            '_public_key': self.ssh()['public_key'],
            '_passphrase': None,
            '_cancel_event': threading.Event(),
            'status': 'queued',
            'events': [],
            'resources': {},
            'results': [],
            'created_at': '2026-09-21T00:00:00+00:00',
            'updated_at': '2026-09-21T00:00:00+00:00',
        }

    def test_plan_uses_exact_gcp_distributed_contract(self):
        plan = qualification._plan(self.args(), self.ssh())

        self.assertEqual(plan.provider, 'gcp')
        self.assertEqual(plan.gcp_project_id, 'benchmark-project')
        self.assertEqual(plan.gcp_zone, 'us-east1-b')
        self.assertEqual(plan.shape, 'c4a-standard-8')
        self.assertEqual(
            plan.deathstarbench.topology_id,
            'distributed_tiered_v1',
        )
        self.assertEqual(plan.deathstarbench.runtime_id, 'k3s_v1')
        self.assertEqual(plan.deathstarbench.workload, 'social_network')
        self.assertEqual(plan.deathstarbench.connections, 4)
        self.assertEqual(plan.benchmarks, ['deathstarbench'])

    def test_plan_rejects_missing_project_mismatched_zone_and_bad_load(self):
        with self.assertRaisesRegex(
            qualification.QualificationError,
            'project-id',
        ):
            qualification._plan(self.args(project_id=''), self.ssh())
        with self.assertRaisesRegex(
            qualification.QualificationError,
            'zone must belong',
        ):
            qualification._plan(
                self.args(region='us-east1', zone='us-central1-a'),
                self.ssh(),
            )
        with self.assertRaisesRegex(
            qualification.QualificationError,
            'evenly divisible',
        ):
            qualification._plan(self.args(connections=5), self.ssh())

    def test_candidate_validation_rejects_wrong_provider_or_marker(self):
        job = self.job()
        wrong = {**job, 'plan': {**job['plan'], 'provider': 'azure'}}
        with self.assertRaisesRegex(
            qualification.QualificationError,
            'not a GCP distributed',
        ):
            qualification._validate_candidate_job(
                wrong,
                job_id=wrong['id'],
            )

        job['resources'] = {
            'provider': 'gcp',
            'gcp_project_id': 'benchmark-project',
            'gcp_resource_prefix': 'benchmark-abc123def456',
        }
        with self.assertRaisesRegex(
            qualification.QualificationError,
            'ownership markers',
        ):
            qualification._validate_candidate_job(job, job_id=job['id'])

        job['resources']['gcp_distributed_candidate'] = True
        qualification._validate_candidate_job(job, job_id=job['id'])

    def test_main_gcp_hooks_remain_operator_only_and_provider_strict(self):
        plan = qualification._plan(self.args(), self.ssh())
        azure_plan = plan.model_copy(update={'provider': 'azure'})

        with self.assertRaisesRegex(ValueError, 'requires GCP'):
            qualification.application.prepare_gcp_distributed_deathstarbench_candidate_runtime(
                self.job(plan),
                azure_plan,
            )

        with mock.patch.object(
            qualification.application,
            'prepare_gcp_distributed_k3s_candidate',
            return_value={'state': 'cluster_ready'},
        ) as prepare:
            result = qualification.application.prepare_gcp_distributed_deathstarbench_candidate_runtime(
                self.job(plan),
                plan,
            )

        self.assertEqual(result, {'state': 'cluster_ready'})
        self.assertIs(prepare.call_args.kwargs['execute'], qualification.application.ssh)
        self.assertIs(prepare.call_args.kwargs['emit'], qualification.application.event)

    def test_main_gcp_measurement_hook_uses_gcp_strict_wrapper(self):
        plan = qualification._plan(self.args(measure=True), self.ssh())
        job = self.job(plan)
        image_lock = {'schema_version': 1}

        with mock.patch.object(
            qualification.application,
            'run_gcp_distributed_social_network_measurement',
            return_value={'id': 'deathstarbench'},
        ) as runner:
            result = qualification.application.run_gcp_distributed_deathstarbench_candidate_measurement(
                job,
                plan,
                image_lock,
            )

        self.assertEqual(result, {'id': 'deathstarbench'})
        self.assertEqual(runner.call_args.args[:2], (job, image_lock))
        self.assertEqual(runner.call_args.args[2], plan.deathstarbench)
        self.assertIs(runner.call_args.kwargs['execute'], qualification.application.ssh)

    def test_end_to_end_orchestration_reuses_generic_runtime_and_cleans(self):
        plan = qualification._plan(self.args(), self.ssh())
        job = self.job(plan)
        image_lock = {'schema_version': 1}

        def provision(candidate, *_args, **_kwargs):
            candidate['resources'].update({
                'provider': 'gcp',
                'gcp_distributed_candidate': True,
            })

        def runtime(candidate, **_kwargs):
            candidate['resources'][K3S_RUNTIME_JOURNAL_KEY] = {
                'state': 'cluster_ready'
            }

        def workload(candidate, _lock, **_kwargs):
            candidate['resources'][DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY] = {
                'state': 'workload_ready'
            }

        with (
            mock.patch.object(
                qualification.gcp,
                'provision_distributed_deathstarbench_candidate',
                side_effect=provision,
            ) as provisioner,
            mock.patch.object(
                qualification,
                'prepare_distributed_k3s_candidate',
                side_effect=runtime,
            ) as runtime_prepare,
            mock.patch.object(
                qualification,
                'prepare_distributed_social_network_candidate',
                side_effect=workload,
            ) as workload_prepare,
            mock.patch.object(
                qualification,
                '_cleanup',
                return_value=True,
            ) as cleanup,
            mock.patch.object(
                qualification.shared,
                '_read_interruption_evidence',
                return_value=None,
            ),
            mock.patch.object(qualification.shared, '_event'),
            mock.patch.object(qualification.shared, '_set_status'),
        ):
            result = qualification._run_qualification(
                job,
                lambda: (plan, self.ssh(), image_lock),
            )

        self.assertEqual(result, 0)
        provisioner.assert_called_once()
        runtime_prepare.assert_called_once()
        workload_prepare.assert_called_once()
        cleanup.assert_called_once_with(job)

    def test_measurement_uses_same_generic_runner_and_strict_gate(self):
        plan = qualification._plan(self.args(measure=True), self.ssh())
        job = self.job(plan)
        image_lock = {'schema_version': 1}
        measured_result = {'id': 'deathstarbench', 'status': 'completed'}

        def provision(candidate, *_args, **_kwargs):
            candidate['resources'].update({
                'provider': 'gcp',
                'gcp_distributed_candidate': True,
            })

        def runtime(candidate, **_kwargs):
            candidate['resources'][K3S_RUNTIME_JOURNAL_KEY] = {
                'state': 'cluster_ready'
            }

        def workload(candidate, _lock, **_kwargs):
            candidate['resources'][DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY] = {
                'state': 'workload_ready'
            }

        with (
            mock.patch.object(
                qualification.gcp,
                'provision_distributed_deathstarbench_candidate',
                side_effect=provision,
            ),
            mock.patch.object(
                qualification,
                'prepare_distributed_k3s_candidate',
                side_effect=runtime,
            ),
            mock.patch.object(
                qualification,
                'prepare_distributed_social_network_candidate',
                side_effect=workload,
            ),
            mock.patch.object(
                qualification,
                'run_distributed_social_network_measurement',
                return_value=measured_result,
            ) as measure,
            mock.patch.object(
                qualification.shared,
                '_require_qualified_measurement_result',
            ) as result_gate,
            mock.patch.object(
                qualification.shared,
                '_write_and_require_report',
            ) as report_gate,
            mock.patch.object(
                qualification,
                '_cleanup',
                return_value=True,
            ),
            mock.patch.object(
                qualification.shared,
                '_read_interruption_evidence',
                return_value=None,
            ),
            mock.patch.object(qualification.shared, '_event'),
            mock.patch.object(qualification.shared, '_set_status'),
        ):
            result = qualification._run_qualification(
                job,
                lambda: (plan, self.ssh(), image_lock),
                measure=True,
            )

        self.assertEqual(result, 0)
        measure.assert_called_once()
        self.assertEqual(measure.call_args.args[2], plan.deathstarbench)
        result_gate.assert_called_once_with(job, plan, measured_result)
        report_gate.assert_called_once_with(job)

    def test_failure_still_attempts_cleanup(self):
        plan = qualification._plan(self.args(), self.ssh())
        job = self.job(plan)

        with (
            mock.patch.object(
                qualification.gcp,
                'provision_distributed_deathstarbench_candidate',
                side_effect=RuntimeError('quota denied'),
            ),
            mock.patch.object(
                qualification,
                '_cleanup',
                return_value=True,
            ) as cleanup,
            mock.patch.object(
                qualification.shared,
                '_read_interruption_evidence',
                return_value=None,
            ),
            mock.patch.object(qualification.shared, '_set_status'),
        ):
            result = qualification._run_qualification(
                job,
                lambda: (plan, self.ssh(), {'schema_version': 1}),
            )

        self.assertEqual(result, 1)
        cleanup.assert_called_once_with(job)
        self.assertIn('quota denied', job['error'])

    def test_cleanup_fails_closed_when_gcp_ownership_remains(self):
        job = self.job()
        job['resources'] = {
            'provider': 'gcp',
            'gcp_distributed_candidate': True,
        }

        def incomplete_cleanup(candidate):
            candidate['status'] = 'destroyed'

        with (
            mock.patch.object(
                qualification.application,
                'destroy_with_status',
                side_effect=incomplete_cleanup,
            ),
            mock.patch.object(
                qualification.application,
                'has_recoverable_resources',
                return_value=False,
            ),
            mock.patch.object(qualification.shared, '_set_status'),
            mock.patch.object(qualification.shared, '_event'),
        ):
            self.assertFalse(qualification._cleanup(job))

    def test_main_requires_lock_before_new_cloud_run(self):
        with mock.patch.object(qualification, '_qualify') as qualify:
            result = qualification.main([
                '--project-id',
                'benchmark-project',
            ])

        self.assertEqual(result, 2)
        qualify.assert_not_called()


if __name__ == '__main__':
    unittest.main()
