from __future__ import annotations

import argparse
from contextlib import nullcontext
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from app.deathstarbench_contract import (
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    K3S_RUNTIME_JOURNAL_KEY,
)
from app.run_lease import inspect_run_lease
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
            'inject_initializer_response_loss': False,
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

    def test_initializer_response_loss_cli_validation(self):
        missing_measurement = qualification._parser().parse_args((
            '--image-lock',
            'lock.json',
            '--inject-initializer-response-loss',
        ))
        with self.assertRaisesRegex(
            qualification.QualificationError,
            'requires --measure',
        ):
            qualification.shared._validate_interruption_options(
                missing_measurement
            )

        hard_checkpoint = qualification._parser().parse_args((
            '--image-lock',
            'lock.json',
            '--measure',
            '--inject-initializer-response-loss',
            '--interrupt-at',
            'initialization_started',
            '--interrupt-mode',
            'hard',
        ))
        with self.assertRaisesRegex(
            qualification.QualificationError,
            'cannot be combined',
        ):
            qualification.shared._validate_interruption_options(
                hard_checkpoint
            )

        graceful_checkpoint = qualification._parser().parse_args((
            '--image-lock',
            'lock.json',
            '--measure',
            '--inject-initializer-response-loss',
            '--interrupt-at',
            'initialization_started',
        ))
        with self.assertRaisesRegex(
            qualification.QualificationError,
            'cannot be combined',
        ):
            qualification.shared._validate_interruption_options(
                graceful_checkpoint
            )

        enabled = qualification._parser().parse_args((
            '--image-lock',
            'lock.json',
            '--measure',
            '--inject-initializer-response-loss',
        ))
        qualification.shared._validate_interruption_options(enabled)
        self.assertTrue(enabled.inject_initializer_response_loss)

        disabled = qualification._parser().parse_args((
            '--image-lock',
            'lock.json',
            '--measure',
        ))
        self.assertFalse(disabled.inject_initializer_response_loss)

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

    def test_measurement_resume_accepts_only_safe_pre_init_states(self):
        for state in (None, *sorted(qualification.SAFE_MEASUREMENT_RESUME_STATES)):
            with self.subTest(state=state):
                job = self.job()
                job['resources'] = {
                    'provider': 'gcp',
                    'gcp_distributed_candidate': True,
                }
                if state is not None:
                    job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
                        'state': state,
                    }

                qualification._require_resumable(job, measure=True)

    def test_measurement_resume_rejects_post_init_and_unknown_states(self):
        unsafe_states = sorted(
            qualification.shared.MEASUREMENT_JOURNAL_STATES
            - qualification.SAFE_MEASUREMENT_RESUME_STATES
        ) + ['invented_state']
        for state in unsafe_states:
            with self.subTest(state=state):
                job = self.job()
                job['resources'] = {
                    'provider': 'gcp',
                    'gcp_distributed_candidate': True,
                    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {'state': state},
                }

                with self.assertRaisesRegex(
                    qualification.QualificationError,
                    'not safe to resume',
                ):
                    qualification._require_resumable(job, measure=True)

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

    def test_network_attestation_is_persisted_once_and_drift_is_refused(self):
        job = self.job()
        image_lock = {'schema_version': 1}
        original = {'schema_version': 1, 'provider': 'gcp'}
        drifted = {'schema_version': 1, 'provider': 'azure'}

        with (
            mock.patch.object(
                qualification,
                'qualify_distributed_network_paths',
                side_effect=(original, dict(original), drifted),
            ) as network_gate,
            mock.patch.object(qualification.shared, '_persist') as persist,
        ):
            self.assertEqual(
                qualification._qualify_and_persist_network_paths(
                    job,
                    image_lock,
                ),
                original,
            )
            self.assertEqual(
                qualification._qualify_and_persist_network_paths(
                    job,
                    image_lock,
                ),
                original,
            )
            with self.assertRaisesRegex(
                qualification.QualificationError,
                'evidence drifted',
            ):
                qualification._qualify_and_persist_network_paths(
                    job,
                    image_lock,
                )

        self.assertEqual(network_gate.call_count, 3)
        persist.assert_called_once_with(job)
        self.assertEqual(
            job['resources'][
                qualification.DISTRIBUTED_NETWORK_QUALIFICATION_KEY
            ],
            original,
        )

    def test_end_to_end_orchestration_reuses_generic_runtime_and_cleans(self):
        plan = qualification._plan(self.args(), self.ssh())
        job = self.job(plan)
        image_lock = {'schema_version': 1}
        network_attestation = {'schema_version': 1, 'provider': 'gcp'}

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
                'qualify_distributed_network_paths',
                return_value=network_attestation,
            ) as network_gate,
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
        network_gate.assert_called_once_with(
            job,
            image_lock,
            execute=qualification.application.ssh,
            emit=mock.ANY,
        )
        self.assertEqual(
            job['resources'][
                qualification.DISTRIBUTED_NETWORK_QUALIFICATION_KEY
            ],
            network_attestation,
        )
        cleanup.assert_called_once_with(job)

    def test_measurement_uses_same_generic_runner_and_strict_gate(self):
        plan = qualification._plan(self.args(measure=True), self.ssh())
        job = self.job(plan)
        image_lock = {'schema_version': 1}
        measured_result = {'id': 'deathstarbench', 'status': 'completed'}
        network_attestation = {'schema_version': 1, 'provider': 'gcp'}
        operation_order = []

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

        def qualify_network(candidate, actual_lock, **_kwargs):
            self.assertIs(candidate, job)
            self.assertIs(actual_lock, image_lock)
            self.assertEqual(
                candidate['resources'][K3S_RUNTIME_JOURNAL_KEY]['state'],
                'cluster_ready',
            )
            self.assertEqual(
                candidate['resources'][DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY][
                    'state'
                ],
                'workload_ready',
            )
            operation_order.append('network_gate')
            return network_attestation

        def measure(candidate, *_args, **_kwargs):
            self.assertEqual(
                candidate['resources'][
                    qualification.DISTRIBUTED_NETWORK_QUALIFICATION_KEY
                ],
                network_attestation,
            )
            operation_order.append('measurement')
            return measured_result

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
                'qualify_distributed_network_paths',
                side_effect=qualify_network,
            ),
            mock.patch.object(
                qualification,
                'run_distributed_social_network_measurement',
                side_effect=measure,
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
                inject_initializer_response_loss=True,
            )

        self.assertEqual(result, 0)
        self.assertEqual(operation_order, ['network_gate', 'measurement'])
        measure.assert_called_once()
        self.assertEqual(measure.call_args.args[2], plan.deathstarbench)
        self.assertIs(
            measure.call_args.kwargs[
                'qualification_inject_initializer_response_loss'
            ],
            True,
        )
        result_gate.assert_called_once_with(job, plan, measured_result)
        report_gate.assert_called_once_with(job)

    def test_qualify_forwards_initializer_response_loss_to_run(self):
        args = self.args(
            measure=True,
            inject_initializer_response_loss=True,
        )
        plan = qualification._plan(args, self.ssh())
        job = self.job(plan)
        image_lock = {'schema_version': 1}

        with (
            mock.patch.object(
                qualification.shared,
                '_ssh_material',
                return_value=self.ssh(),
            ),
            mock.patch.object(
                qualification.shared,
                '_read_image_lock',
                return_value=image_lock,
            ),
            mock.patch.object(
                qualification.shared,
                '_preflight_anonymous_ghcr_images',
            ),
            mock.patch.object(qualification, '_plan', return_value=plan),
            mock.patch.object(
                qualification.shared,
                '_new_job',
                return_value=(job, nullcontext()),
            ),
            mock.patch.object(
                qualification,
                '_run_qualification',
                return_value=0,
            ) as run,
        ):
            outcome = qualification._qualify(args)

        self.assertEqual(outcome, 0)
        run.assert_called_once()
        self.assertIs(
            run.call_args.kwargs['inject_initializer_response_loss'],
            True,
        )

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

    def test_real_hard_exit_persists_evidence_and_releases_lease(self):
        job_id = 'ab12cd34ef56'
        child_source = """
import sys
from pathlib import Path
from app.deathstarbench_contract import DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
from scripts import qualify_gcp_deathstarbench_distributed as qualification

runs = Path(sys.argv[1])
job_id = sys.argv[2]
qualification.application.RUNS = runs
job = {
    'id': job_id,
    'resources': {
        DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
            'state': 'load_generator_ready',
        },
    },
}
with qualification._exclusive_job_lock(job_id):
    callback = qualification.shared._qualification_checkpoint_callback(
        interrupt_at='load_generator_ready',
        interrupt_mode='hard',
    )
    callback(job, 'load_generator_ready')
raise AssertionError('hard exit unexpectedly returned')
"""
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job_id).mkdir()
            completed = subprocess.run(
                [sys.executable, '-c', child_source, str(runs), job_id],
                cwd=qualification.PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            with mock.patch.object(qualification.application, 'RUNS', runs):
                evidence = qualification.shared._read_interruption_evidence(
                    job_id
                )
                status_after_exit = inspect_run_lease(runs, job_id)
                with qualification._exclusive_job_lock(job_id):
                    status_after_reacquire = inspect_run_lease(runs, job_id)

        self.assertEqual(
            completed.returncode,
            qualification.shared.HARD_INTERRUPTION_EXIT_CODE,
            completed.stderr,
        )
        self.assertEqual(evidence['source'], 'checkpoint')
        self.assertEqual(evidence['mode'], 'hard')
        self.assertEqual(evidence['checkpoint'], 'load_generator_ready')
        self.assertEqual(
            evidence['cleanup_outcome'],
            'not_attempted_process_exit',
        )
        self.assertFalse(status_after_exit.held)
        self.assertTrue(status_after_reacquire.held)

    def test_hard_exit_evidence_is_finalized_by_cleanup_only(self):
        job = self.job()
        job['resources'] = {
            'provider': 'gcp',
            'gcp_distributed_candidate': True,
            DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                'state': 'measurement_started',
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with (
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(
                    qualification,
                    '_load_candidate_job',
                    return_value=job,
                ) as loader,
                mock.patch.object(
                    qualification,
                    '_exclusive_job_lock',
                    side_effect=lambda _job_id: nullcontext(),
                ),
                mock.patch.object(
                    qualification,
                    '_cleanup',
                    return_value=True,
                ) as cleanup,
            ):
                qualification.shared._record_checkpoint_interruption(
                    job,
                    'measurement_started',
                    mode='hard',
                )
                outcome = qualification._cleanup_only(job['id'])
                evidence = qualification.shared._read_interruption_evidence(
                    job['id']
                )

        self.assertEqual(outcome, 0)
        self.assertEqual(loader.call_count, 2)
        cleanup.assert_called_once_with(job)
        self.assertEqual(evidence['source'], 'checkpoint')
        self.assertEqual(evidence['checkpoint'], 'measurement_started')
        self.assertEqual(evidence['cleanup_outcome'], 'completed')
        self.assertEqual(evidence['recovery_outcome'], 'cleanup_completed')

    def test_cleanup_only_signals_preserve_ownership_and_origin_evidence(self):
        immutable_origin_fields = (
            'source',
            'mode',
            'checkpoint',
            'signal',
            'execution_state',
            'replay_decision',
            'requested_at',
        )
        for signum in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=signal.Signals(signum).name):
                job = self.job()
                expected_resources = {
                    'provider': 'gcp',
                    'gcp_distributed_candidate': True,
                    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                        'state': 'measurement_started',
                    },
                }
                job['resources'] = expected_resources.copy()
                cleanup_started = threading.Event()
                release_cleanup = threading.Event()
                operation_finished = threading.Event()

                def interrupted_cleanup(_job):
                    cleanup_started.set()
                    release_cleanup.wait(30)
                    self.fail(
                        'the signal must unwind a blocked GCP cleanup-only wait'
                    )

                def interrupt_blocked_cleanup():
                    if not cleanup_started.wait(5):
                        return
                    os.kill(os.getpid(), signum)
                    if not operation_finished.wait(3):
                        release_cleanup.set()

                interrupter = threading.Thread(
                    target=interrupt_blocked_cleanup,
                    daemon=True,
                )

                with tempfile.TemporaryDirectory() as temporary:
                    runs = Path(temporary)
                    (runs / job['id']).mkdir()
                    with (
                        mock.patch.object(
                            qualification.application,
                            'RUNS',
                            runs,
                        ),
                        mock.patch.object(
                            qualification,
                            '_load_candidate_job',
                            return_value=job,
                        ) as loader,
                        mock.patch.object(
                            qualification,
                            '_exclusive_job_lock',
                            side_effect=lambda _job_id: nullcontext(),
                        ),
                        mock.patch.object(
                            qualification,
                            '_cleanup',
                            side_effect=interrupted_cleanup,
                        ) as cleanup,
                        mock.patch.object(qualification.shared, '_persist'),
                    ):
                        qualification.shared._record_checkpoint_interruption(
                            job,
                            'measurement_started',
                            mode='hard',
                        )
                        origin = (
                            qualification.shared._read_interruption_evidence(
                                job['id']
                            )
                        )
                        started_at = time.monotonic()
                        interrupter.start()
                        try:
                            outcome = qualification._cleanup_only(job['id'])
                        finally:
                            operation_finished.set()
                            release_cleanup.set()
                            interrupter.join(timeout=5)
                        elapsed = time.monotonic() - started_at
                        evidence = (
                            qualification.shared._read_interruption_evidence(
                                job['id']
                            )
                        )

                self.assertEqual(outcome, 2)
                self.assertLess(elapsed, 2)
                self.assertFalse(interrupter.is_alive())
                self.assertEqual(loader.call_count, 2)
                cleanup.assert_called_once_with(job)
                self.assertEqual(job['status'], 'cleanup_failed')
                self.assertEqual(job['resources'], expected_resources)
                self.assertTrue(job['benchmark_interrupted'])
                for field in immutable_origin_fields:
                    self.assertEqual(evidence[field], origin[field])
                self.assertEqual(
                    evidence['subsequent_signal'],
                    signal.Signals(signum).name,
                )
                self.assertEqual(evidence['cleanup_outcome'], 'failed')
                self.assertEqual(evidence['recovery_outcome'], 'cleanup_failed')

    def test_cleanup_only_reconciles_signal_during_final_evidence_update(self):
        job = self.job()
        job['resources'] = {
            'provider': 'gcp',
            'gcp_distributed_candidate': True,
            DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                'state': 'measurement_started',
            },
        }
        immutable_origin_fields = (
            'source',
            'mode',
            'checkpoint',
            'signal',
            'execution_state',
            'replay_decision',
            'requested_at',
        )
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with mock.patch.object(qualification.application, 'RUNS', runs):
                qualification.shared._record_checkpoint_interruption(
                    job,
                    'measurement_started',
                    mode='hard',
                )
                origin = qualification.shared._read_interruption_evidence(
                    job['id']
                )
                real_update = (
                    qualification.shared._update_interruption_evidence
                )
                update_calls = 0

                def interrupt_final_update(*args, **kwargs):
                    nonlocal update_calls
                    update_calls += 1
                    if update_calls == 2:
                        os.kill(os.getpid(), signal.SIGTERM)
                        raise qualification.application.RunCancelled(
                            'injected final evidence interruption'
                        )
                    return real_update(*args, **kwargs)

                with (
                    mock.patch.object(
                        qualification,
                        '_load_candidate_job',
                        return_value=job,
                    ),
                    mock.patch.object(
                        qualification,
                        '_exclusive_job_lock',
                        side_effect=lambda _job_id: nullcontext(),
                    ),
                    mock.patch.object(
                        qualification,
                        '_cleanup',
                        return_value=True,
                    ),
                    mock.patch.object(qualification.shared, '_persist'),
                    mock.patch.object(
                        qualification.shared,
                        '_update_interruption_evidence',
                        side_effect=interrupt_final_update,
                    ),
                ):
                    outcome = qualification._cleanup_only(job['id'])
                evidence = qualification.shared._read_interruption_evidence(
                    job['id']
                )

        self.assertEqual(outcome, 128 + signal.SIGTERM)
        self.assertGreaterEqual(update_calls, 3)
        for field in immutable_origin_fields:
            self.assertEqual(evidence[field], origin[field])
        self.assertEqual(evidence['subsequent_signal'], 'SIGTERM')
        self.assertEqual(evidence['cleanup_outcome'], 'completed')
        self.assertEqual(evidence['recovery_outcome'], 'cleanup_completed')

    def test_run_reconciles_signal_during_final_evidence_read(self):
        plan = qualification._plan(self.args(), self.ssh())
        job = self.job(plan)
        job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'load_generator_ready',
        }
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

        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with mock.patch.object(qualification.application, 'RUNS', runs):
                qualification.shared._record_checkpoint_interruption(
                    job,
                    'load_generator_ready',
                    mode='hard',
                )
                origin = qualification.shared._read_interruption_evidence(
                    job['id']
                )
                real_read = qualification.shared._read_interruption_evidence
                read_calls = 0

                def interrupt_final_read(job_id):
                    nonlocal read_calls
                    read_calls += 1
                    if read_calls == 2:
                        os.kill(os.getpid(), signal.SIGINT)
                        raise qualification.application.RunCancelled(
                            'injected final evidence interruption'
                        )
                    return real_read(job_id)

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
                        'qualify_distributed_network_paths',
                        return_value={
                            'schema_version': 1,
                            'provider': 'gcp',
                        },
                    ),
                    mock.patch.object(qualification, '_cleanup', return_value=True),
                    mock.patch.object(qualification.shared, '_event'),
                    mock.patch.object(qualification.shared, '_set_status'),
                    mock.patch.object(qualification.shared, '_persist'),
                    mock.patch.object(
                        qualification.shared,
                        '_read_interruption_evidence',
                        side_effect=interrupt_final_read,
                    ),
                ):
                    outcome = qualification._run_qualification(
                        job,
                        lambda: (plan, self.ssh(), image_lock),
                    )
                evidence = qualification.shared._read_interruption_evidence(
                    job['id']
                )

        self.assertEqual(outcome, 128 + signal.SIGINT)
        self.assertGreaterEqual(read_calls, 4)
        for field in (
            'source',
            'mode',
            'checkpoint',
            'signal',
            'execution_state',
            'replay_decision',
            'requested_at',
        ):
            self.assertEqual(evidence[field], origin[field])
        self.assertEqual(evidence['subsequent_signal'], 'SIGINT')
        self.assertEqual(evidence['cleanup_outcome'], 'completed')

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

    def test_cleanup_ignores_retained_network_qualification_evidence(self):
        job = self.job()
        job['resources'] = {
            'provider': 'gcp',
            qualification.DISTRIBUTED_NETWORK_QUALIFICATION_KEY: {
                'schema_version': 1,
                'provider': 'gcp',
            },
        }

        def destroy(candidate):
            candidate['status'] = 'destroyed'

        with (
            mock.patch.object(
                qualification.application,
                'destroy_with_status',
                side_effect=destroy,
            ),
            mock.patch.object(
                qualification.application,
                'has_recoverable_resources',
                return_value=False,
            ),
            mock.patch.object(qualification.shared, '_set_status'),
            mock.patch.object(qualification.shared, '_event'),
        ):
            self.assertTrue(qualification._cleanup(job))

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
