#!/usr/bin/env python3
"""Run the distributed DeathStarBench Azure qualification safely.

This is an operator-only harness.  It persists the complete ownership
contract before Azure writes, deploys the candidate topology/runtime, and by
default stops at the attested ``workload_ready`` boundary.  ``--measure``
additionally runs the one-shot dataset and load qualification, writes its
report, and only then attempts exact resource-group cleanup.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import signal
import sys
from typing import Any, Callable, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import main as application
from app.deathstarbench_contract import (
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    K3S_RUNTIME_ID,
    K3S_RUNTIME_JOURNAL_KEY,
)
from app.deathstarbench_distributed import (
    DISTRIBUTED_NETWORK_QUALIFICATION_KEY,
    prepare_azure_distributed_k3s_candidate,
    prepare_azure_distributed_social_network_candidate,
    qualify_distributed_network_paths,
)
from app.models import BenchmarkPlan
from app.providers import azure
from app.resource_inventory import ROLE_NODE_INVENTORY_KEY
from app.run_lease import LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME
from scripts import qualify_deathstarbench_distributed_shared as shared


QualificationError = shared.QualificationError
LOCK_FILENAME = shared.LOCK_FILENAME
# Retain this name for callers/tests that used the harness-specific constant,
# while the process-wide run lease holds it as a migration guard for older
# qualifier processes.
QUALIFICATION_LOCK_FILENAME = LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME
INTERRUPTION_EVIDENCE_FILENAME = shared.INTERRUPTION_EVIDENCE_FILENAME
INTERRUPTION_EVIDENCE_SCHEMA_VERSION = shared.INTERRUPTION_EVIDENCE_SCHEMA_VERSION
MAX_INTERRUPTION_EVIDENCE_BYTES = shared.MAX_INTERRUPTION_EVIDENCE_BYTES
HARD_INTERRUPTION_EXIT_CODE = shared.HARD_INTERRUPTION_EXIT_CODE
INTERRUPTION_CHECKPOINTS = shared.INTERRUPTION_CHECKPOINTS
INTERRUPTION_MODES = shared.INTERRUPTION_MODES
GHCR_HOST = shared.GHCR_HOST
GHCR_RESPONSE_LIMIT_BYTES = shared.GHCR_RESPONSE_LIMIT_BYTES
GHCR_TIMEOUT_SECONDS = shared.GHCR_TIMEOUT_SECONDS
GHCR_IMAGE_KEYS = shared.GHCR_IMAGE_KEYS
GHCR_MANIFEST_ACCEPT = shared.GHCR_MANIFEST_ACCEPT
RESUME_TEARDOWN_STATUSES = shared.RESUME_TEARDOWN_STATUSES
CLEANUP_OWNERSHIP_KEYS = frozenset({
    'azure_resource_group_name',
    'azure_resource_group_expected_id',
    'azure_resource_group_tags',
    ROLE_NODE_INVENTORY_KEY,
    azure.DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
    azure.DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY,
    K3S_RUNTIME_JOURNAL_KEY,
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
})
# DISTRIBUTED_NETWORK_QUALIFICATION_KEY is deliberately absent: its retained
# attestation is non-cloud evidence and must not make exact cleanup look
# incomplete after every Azure-owned resource has been proved absent.
WORKLOAD_OPTION_DEFAULTS = dict(shared.WORKLOAD_OPTION_DEFAULTS)
SAFE_MEASUREMENT_RESUME_STATES = shared.SAFE_MEASUREMENT_RESUME_STATES
MEASUREMENT_JOURNAL_STATES = shared.MEASUREMENT_JOURNAL_STATES


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--resume',
        metavar='JOB_ID',
        help='Resume one persisted candidate from its exact ownership journal.',
    )
    mode.add_argument(
        '--cleanup-only',
        metavar='JOB_ID',
        help='Destroy one persisted candidate without using SSH credentials.',
    )
    parser.add_argument(
        '--image-lock',
        type=Path,
        help=(
            'Exact image-lock artifact from the candidate publication workflow. '
            'Required for a new run; optional on resume when the run copy exists.'
        ),
    )
    parser.add_argument(
        '--ssh-env-file',
        type=Path,
        help=(
            'Optional .env file containing benchmark SSH key paths. Useful '
            'when the qualification script runs from an isolated worktree.'
        ),
    )
    parser.add_argument(
        '--subscription-id',
        default=os.getenv('AZURE_SUBSCRIPTION_ID'),
        help='Azure subscription ID (or AZURE_SUBSCRIPTION_ID).',
    )
    parser.add_argument('--region', default='eastus2')
    parser.add_argument('--zone', default='1')
    parser.add_argument('--application-size', default='Standard_D8ps_v6')
    parser.add_argument('--application-vcpus', type=int, default=8)
    parser.add_argument('--application-memory-gib', type=float, default=32)
    parser.add_argument(
        '--measure',
        action='store_true',
        help=(
            'After workload readiness, initialize the one-shot dataset, run '
            'wrk2, and require a complete saved report before cleanup.'
        ),
    )
    parser.add_argument(
        '--interrupt-at',
        choices=INTERRUPTION_CHECKPOINTS,
        help=(
            'Qualification-only failure injection immediately after the named '
            'measurement state is durably journaled. Requires --measure.'
        ),
    )
    parser.add_argument(
        '--interrupt-mode',
        choices=INTERRUPTION_MODES,
        help=(
            'Use cooperative cleanup (graceful) or intentionally terminate '
            'the harness process (hard). Requires --measure and --interrupt-at; '
            'the safe default with --interrupt-at is graceful.'
        ),
    )
    parser.add_argument(
        '--inject-initializer-response-loss',
        action='store_true',
        help=(
            'Qualification-only simulation of a lost initializer response '
            'after the durable remote dispatch is accepted; reconciliation '
            'remains read-only. Requires --measure.'
        ),
    )
    parser.add_argument(
        '--warmup-seconds',
        type=int,
        default=WORKLOAD_OPTION_DEFAULTS['warmup_seconds'],
    )
    parser.add_argument(
        '--duration-seconds',
        type=int,
        default=WORKLOAD_OPTION_DEFAULTS['duration_seconds'],
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=WORKLOAD_OPTION_DEFAULTS['threads'],
    )
    parser.add_argument(
        '--connections',
        type=int,
        default=WORKLOAD_OPTION_DEFAULTS['connections'],
    )
    parser.add_argument(
        '--request-rate',
        type=int,
        default=WORKLOAD_OPTION_DEFAULTS['request_rate'],
    )
    return parser


_read_image_lock = shared._read_image_lock
_RejectRegistryRedirects = shared._RejectRegistryRedirects
_anonymous_registry_open = shared._anonymous_registry_open
_require_ghcr_https_url = shared._require_ghcr_https_url
_read_bounded_response = shared._read_bounded_response
_request_bounded = shared._request_bounded
_bearer_token_url = shared._bearer_token_url
_anonymous_bearer_token = shared._anonymous_bearer_token
_preflight_one_ghcr_manifest = shared._preflight_one_ghcr_manifest
_preflight_anonymous_ghcr_images = shared._preflight_anonymous_ghcr_images
_ssh_material = shared._ssh_material


def _plan(
    args: argparse.Namespace,
    ssh: Mapping[str, str | None],
) -> BenchmarkPlan:
    subscription_id = str(args.subscription_id or '').strip()
    if not subscription_id:
        raise QualificationError(
            'Pass --subscription-id or set AZURE_SUBSCRIPTION_ID.'
        )
    threads = args.threads
    connections = args.connections
    request_rate = args.request_rate
    if threads > 0 and (
        connections < threads
        or request_rate < threads
        or connections % threads
        or request_rate % threads
    ):
        raise QualificationError(
            'Qualification connections and request rate must each be at least '
            'and evenly divisible by the wrk2 thread count.'
        )
    return BenchmarkPlan(
        provider='azure',
        azure_subscription_id=subscription_id,
        azure_zone=str(args.zone).strip(),
        region=str(args.region).strip(),
        shape=str(args.application_size).strip(),
        ocpus=args.application_vcpus,
        memory_gb=args.application_memory_gib,
        ssh_private_key=str(ssh['private_key']),
        ssh_public_key=str(ssh['public_key']),
        ssh_key_passphrase=ssh['passphrase'],
        security={'mode': 'none'},
        networking='paravirtualized',
        storage={'additional_volume': False},
        deathstarbench={
            'topology_id': DISTRIBUTED_TIERED_TOPOLOGY_ID,
            'runtime_id': K3S_RUNTIME_ID,
            'workload': 'social_network',
            'warmup_seconds': args.warmup_seconds,
            'duration_seconds': args.duration_seconds,
            'threads': args.threads,
            'connections': args.connections,
            'request_rate': args.request_rate,
        },
        benchmarks=['deathstarbench'],
        destroy_after_completion=True,
    )


_event = shared._event
_persist = shared._persist
_set_status = shared._set_status
_validate_interruption_options = shared._validate_interruption_options
_effective_interrupt_mode = shared._effective_interrupt_mode
_execution_state = shared._execution_state
_replay_decision = shared._replay_decision
_evidence_path = shared._evidence_path
_validated_interruption_evidence = shared._validated_interruption_evidence
_serialize_interruption_evidence = shared._serialize_interruption_evidence
_write_interruption_evidence = shared._write_interruption_evidence
_read_interruption_evidence = shared._read_interruption_evidence
_update_interruption_evidence = shared._update_interruption_evidence
_record_checkpoint_interruption = shared._record_checkpoint_interruption
_record_signal_interruption = shared._record_signal_interruption
_finalize_interruption_evidence_signal_aware = (
    shared._finalize_interruption_evidence_signal_aware
)
_mark_interrupted_unless_benchmark_is_terminal = (
    shared._mark_interrupted_unless_benchmark_is_terminal
)
_hard_exit = shared._hard_exit
_cooperative_signal_handlers = shared._cooperative_signal_handlers
_acquire_job_lease = shared._acquire_job_lease


def _qualification_checkpoint_callback(
    *,
    interrupt_at: str,
    interrupt_mode: str,
) -> Callable[[dict[str, Any], str], None]:
    return shared._qualification_checkpoint_callback(
        interrupt_at=interrupt_at,
        interrupt_mode=interrupt_mode,
        record_checkpoint=(
            lambda job, state, *, mode: _record_checkpoint_interruption(
                job,
                state,
                mode=mode,
            )
        ),
        hard_exit=lambda exit_code: _hard_exit(exit_code),
    )


def _new_job(
    plan: BenchmarkPlan,
    ssh: Mapping[str, str | None],
    image_lock: Mapping[str, Any],
):
    return shared._new_job(
        plan,
        ssh,
        image_lock,
        persist=lambda job: _persist(job),
    )


def _validate_candidate_job(job: Mapping[str, Any], *, job_id: str) -> None:
    """Fail closed before this harness mutates or destroys a saved run."""

    plan = job.get('plan')
    deathstarbench = plan.get('deathstarbench') if isinstance(plan, dict) else None
    expected = {
        'topology_id': DISTRIBUTED_TIERED_TOPOLOGY_ID,
        'runtime_id': K3S_RUNTIME_ID,
        'workload': 'social_network',
    }
    if (
        not isinstance(plan, dict)
        or plan.get('provider') != 'azure'
        or not isinstance(deathstarbench, dict)
        or any(deathstarbench.get(key) != value for key, value in expected.items())
    ):
        raise QualificationError(
            f'Saved job {job_id} is not an Azure distributed Social Network '
            'K3s qualification candidate.'
        )
    if application.has_recoverable_resources(job):
        resources = job.get('resources')
        if (
            not isinstance(resources, dict)
            or resources.get('provider') != 'azure'
            or resources.get('azure_distributed_candidate') is not True
        ):
            raise QualificationError(
                f'Saved job {job_id} has recoverable resources without the '
                'exact Azure distributed-candidate ownership markers.'
            )


def _load_candidate_job(job_id: str) -> dict[str, Any]:
    if not application.RUN_ID_PATTERN.fullmatch(job_id):
        raise QualificationError('Qualification job IDs must be 12 lowercase hex digits.')
    job = application.load_persisted_job(job_id)
    if job is None:
        raise QualificationError(f'No persisted qualification job {job_id!r} exists.')
    _validate_candidate_job(job, job_id=job_id)
    return job


_exclusive_job_lock = shared._exclusive_job_lock


_resume_job = shared._resume_job


def _require_resumable(
    job: Mapping[str, Any],
    *,
    measure: bool = False,
) -> None:
    evidence = _read_interruption_evidence(str(job['id']))
    if evidence is not None:
        if evidence['subsequent_signal'] is None:
            evidence_state = evidence['execution_state']
            replay_decision = evidence['replay_decision']
        else:
            evidence_state = evidence['subsequent_signal_execution_state']
            replay_decision = evidence['subsequent_signal_replay_decision']
        current_state = _execution_state(job)
        if replay_decision != 'resume_allowed':
            raise QualificationError(
                f'Qualification job {job["id"]} retained interruption '
                'evidence requires cleanup-only recovery.'
            )
        if current_state != evidence_state:
            raise QualificationError(
                f'Qualification job {job["id"]} current measurement state '
                f'{current_state!r} does not exactly match retained '
                f'interruption evidence state {evidence_state!r}; use '
                '--cleanup-only instead.'
            )
    shared._require_resumable(
        job,
        provider_name='Azure',
        measure=measure,
    )


_require_resume_workload_settings = shared._require_resume_workload_settings
_finite_number = shared._finite_number
_require_qualified_metrics = shared._require_qualified_metrics
_require_qualified_measurement_result = shared._require_qualified_measurement_result


def _write_and_require_report(job: dict[str, Any]) -> None:
    shared._write_and_require_report(
        job,
        set_status=lambda current, status: _set_status(current, status),
        emit=lambda current, stage, message: _event(current, stage, message),
    )


def _qualify_and_persist_network_paths(
    job: dict[str, Any],
    image_lock: Mapping[str, Any],
) -> dict[str, Any]:
    """Run the live network gate and durably retain its immutable evidence."""

    attestation = qualify_distributed_network_paths(
        job,
        image_lock,
        execute=application.ssh,
        emit=_event,
    )
    if not isinstance(attestation, dict):
        raise QualificationError(
            'Distributed network qualification returned malformed evidence.'
        )
    resources = job.get('resources')
    if not isinstance(resources, dict):
        raise QualificationError(
            'Distributed network qualification lost mutable resource state.'
        )
    if DISTRIBUTED_NETWORK_QUALIFICATION_KEY in resources:
        if resources[DISTRIBUTED_NETWORK_QUALIFICATION_KEY] != attestation:
            raise QualificationError(
                'Persisted distributed network qualification evidence drifted '
                'from the live attestation.'
            )
        return attestation
    resources[DISTRIBUTED_NETWORK_QUALIFICATION_KEY] = attestation
    _persist(job)
    return attestation


def _cleanup(job: dict[str, Any]) -> bool:
    if not job.get('resources'):
        _set_status(job, 'destroyed')
        return True
    _set_status(job, 'destroying')
    _event(job, 'Destroy', 'Removing the live Azure qualification resources.')
    application.destroy_with_status(job)
    resources = job.get('resources')
    ownership_keys = (
        sorted(CLEANUP_OWNERSHIP_KEYS.intersection(resources))
        if isinstance(resources, dict)
        else ['<malformed resources>']
    )
    if (
        job.get('status') != 'destroyed'
        or application.has_recoverable_resources(job)
        or ownership_keys
    ):
        print(
            f'Cleanup did not finish for job {job["id"]}: '
            f'{job.get("cleanup_error") or "recoverable ownership contract retained"}'
            + (f' ({", ".join(ownership_keys)})' if ownership_keys else ''),
            file=sys.stderr,
            flush=True,
        )
        return False
    print(f'Azure cleanup proved complete for job {job["id"]}.', flush=True)
    return True


def _cleanup_only(job_id: str) -> int:
    job_id = str(job_id)
    if not application.RUN_ID_PATTERN.fullmatch(job_id):
        raise QualificationError(
            'Qualification job IDs must be 12 lowercase hex digits.'
        )
    with _exclusive_job_lock(job_id):
        job = _load_candidate_job(job_id)
        evidence_error: Exception | None = None
        try:
            if _read_interruption_evidence(job_id) is not None:
                _update_interruption_evidence(
                    job,
                    recovery_outcome='cleanup_only_started',
                )
        except Exception as exc:
            # Retained operator evidence is not part of cloud ownership. Never
            # let a damaged evidence artifact prevent exact cleanup.
            evidence_error = exc
            print(
                f'Unable to update interruption recovery evidence: {exc}',
                file=sys.stderr,
                flush=True,
            )
        cleanup_complete = _cleanup(job)
        try:
            if _read_interruption_evidence(job_id) is not None:
                _update_interruption_evidence(
                    job,
                    cleanup_outcome=(
                        'completed' if cleanup_complete else 'failed'
                    ),
                    recovery_outcome=(
                        'cleanup_completed'
                        if cleanup_complete
                        else 'cleanup_failed'
                    ),
                )
        except Exception as exc:
            evidence_error = evidence_error or exc
            print(
                f'Unable to finalize interruption recovery evidence: {exc}',
                file=sys.stderr,
                flush=True,
            )
        return 0 if cleanup_complete and evidence_error is None else 2


def _lock_for_run(
    requested_path: Path | None,
    job: Mapping[str, Any],
) -> dict[str, Any]:
    return shared._lock_for_run(
        requested_path,
        job,
        read_image_lock=lambda path: _read_image_lock(path),
    )


def _run_qualification(
    job: dict[str, Any],
    setup: Callable[
        [],
        tuple[BenchmarkPlan, Mapping[str, str | None], Mapping[str, Any]],
    ],
    *,
    measure: bool = False,
    interrupt_at: str | None = None,
    interrupt_mode: str | None = None,
    inject_initializer_response_loss: bool = False,
    recovery_attempt: bool = False,
) -> int:
    failure: BaseException | None = None
    qualified = False
    setup_complete = False
    cleanup_complete = False
    with _cooperative_signal_handlers(job) as signal_state:
        try:
            application.raise_if_cancelled(job)
            existing_evidence = _read_interruption_evidence(str(job['id']))
            if (
                recovery_attempt
                and interrupt_at is not None
                and existing_evidence is not None
            ):
                raise QualificationError(
                    'A recovery with retained interruption evidence cannot '
                    'inject another checkpoint; resume without --interrupt-at '
                    'or use --cleanup-only.'
                )
            if recovery_attempt and existing_evidence is not None:
                _update_interruption_evidence(
                    job,
                    recovery_outcome='resume_started',
                )
            plan, ssh, image_lock = setup()
            setup_complete = True
            application.raise_if_cancelled(job)
            if measure:
                job['_persist_results_artifact'] = True
            _set_status(job, 'provisioning')
            application.raise_if_cancelled(job)
            azure.provision_distributed_deathstarbench_candidate(
                job,
                plan,
                public_key=str(ssh['public_key']),
                emit=_event,
                persist=_persist,
            )
            application.raise_if_cancelled(job)
            _set_status(job, 'testing')
            prepare_azure_distributed_k3s_candidate(
                job,
                execute=application.ssh,
                emit=_event,
                persist=_persist,
            )
            application.raise_if_cancelled(job)
            prepare_azure_distributed_social_network_candidate(
                job,
                image_lock,
                execute=application.ssh,
                emit=_event,
                persist=_persist,
            )
            application.raise_if_cancelled(job)
            resources = job['resources']
            runtime = resources.get(K3S_RUNTIME_JOURNAL_KEY) or {}
            workload = resources.get(DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY) or {}
            if (
                runtime.get('state') != 'cluster_ready'
                or workload.get('state') != 'workload_ready'
            ):
                raise QualificationError(
                    'Azure candidate did not reach both exact readiness boundaries.'
                )
            application.raise_if_cancelled(job)
            _qualify_and_persist_network_paths(job, image_lock)
            application.raise_if_cancelled(job)
            if measure:
                measurement_kwargs = {}
                if interrupt_at is not None:
                    if interrupt_mode not in INTERRUPTION_MODES:
                        raise QualificationError(
                            'An interruption checkpoint requires a valid mode.'
                        )
                    measurement_kwargs['qualification_checkpoint'] = (
                        _qualification_checkpoint_callback(
                            interrupt_at=interrupt_at,
                            interrupt_mode=interrupt_mode,
                        )
                    )
                if inject_initializer_response_loss:
                    measurement_kwargs[
                        'qualification_inject_initializer_response_loss'
                    ] = True
                application.raise_if_cancelled(job)
                result = (
                    application.run_azure_distributed_deathstarbench_candidate_measurement(
                        job,
                        plan,
                        image_lock,
                        **measurement_kwargs,
                    )
                )
                application.raise_if_cancelled(job)
                _require_qualified_measurement_result(job, plan, result)
                application.raise_if_cancelled(job)
                _write_and_require_report(job)
                application.raise_if_cancelled(job)
            qualified = True
            _event(
                job,
                'Qualified',
                (
                    'Azure K3s, Social Network workload, one-shot measurement, and '
                    'saved report passed qualification.'
                    if measure
                    else 'Azure K3s and Social Network workload reached exact '
                    'workload_ready.'
                ),
            )
        except BaseException as exc:  # cleanup must also run for Ctrl-C
            failure = exc
            failure_label = f'{type(exc).__name__}: {exc}'
            job['error'] = failure_label
            if isinstance(exc, (KeyboardInterrupt, application.RunCancelled)):
                _mark_interrupted_unless_benchmark_is_terminal(job)
            _set_status(job, 'cleanup_pending')
            print(
                f'Qualification failed: {failure_label}',
                file=sys.stderr,
                flush=True,
            )
        finally:
            signum = signal_state['number']
            if signum is not None:
                signal_outcome = _mark_interrupted_unless_benchmark_is_terminal(
                    job
                )
                if signal_outcome not in {'complete', 'failed'}:
                    # A signal is the terminal cause for a still-pending
                    # benchmark even if the interrupted SDK call surfaced an
                    # ordinary exception while unwinding.
                    job['error'] = None
                if failure is None:
                    failure = application.RunCancelled(
                        f'Qualification received {signal.Signals(signum).name}.'
                    )
                    _set_status(job, 'cleanup_pending')
                else:
                    _persist(job)
                try:
                    _record_signal_interruption(job, signum)
                    signal_state['evidence_recorded'] = True
                except BaseException as evidence_error:
                    if failure is None:
                        failure = evidence_error
                    print(
                        f'Unable to retain signal interruption evidence: '
                        f'{evidence_error}',
                        file=sys.stderr,
                        flush=True,
                    )
            try:
                cleanup_complete = _cleanup(job)
            except application.RunCancelled as cleanup_interruption:
                # A first signal during an Azure cleanup poller must also be
                # able to return control promptly.  The persisted ownership
                # contract remains intact so --cleanup-only can retry safely.
                cleanup_complete = False
                if failure is None:
                    failure = cleanup_interruption
                job['cleanup_error'] = (
                    'Azure cleanup was interrupted by an operating-system '
                    'signal; recoverable ownership was retained for '
                    '--cleanup-only.'
                )
                _set_status(job, 'cleanup_failed')
                print(job['cleanup_error'], file=sys.stderr, flush=True)

            # A signal may arrive while cloud cleanup itself is running. It is
            # still recorded after cleanup without performing file I/O in the
            # Python signal handler.
            signal_state['raise_cancel'] = False
            final_signum = signal_state['number']
            if final_signum is not None and signum is None:
                _mark_interrupted_unless_benchmark_is_terminal(job)
                if failure is None:
                    failure = application.RunCancelled(
                        f'Qualification received '
                        f'{signal.Signals(final_signum).name} during cleanup.'
                    )
                try:
                    _record_signal_interruption(job, final_signum)
                    signal_state['evidence_recorded'] = True
                except BaseException as evidence_error:
                    if failure is None:
                        failure = evidence_error
                    print(
                        f'Unable to retain signal interruption evidence: '
                        f'{evidence_error}',
                        file=sys.stderr,
                        flush=True,
                    )
                _persist(job)
            try:
                recovery_outcome = None
                if recovery_attempt:
                    if qualified:
                        recovery_outcome = 'resume_completed'
                    elif not setup_complete:
                        recovery_outcome = 'resume_refused'
                    else:
                        recovery_outcome = 'resume_failed'
                _finalize_interruption_evidence_signal_aware(
                    job,
                    signal_state,
                    cleanup_outcome=(
                        'completed' if cleanup_complete else 'failed'
                    ),
                    recovery_outcome=recovery_outcome,
                )
            except BaseException as evidence_error:
                if failure is None:
                    failure = evidence_error
                print(
                    f'Unable to finalize interruption evidence: {evidence_error}',
                    file=sys.stderr,
                    flush=True,
                )

    if not cleanup_complete:
        return 2
    signum = signal_state['number']
    if signum is not None:
        return 128 + signum
    if failure is not None:
        if isinstance(failure, (KeyboardInterrupt, application.RunCancelled)):
            return 130
        return 1
    if not qualified:
        return 1
    print(
        f'Qualification succeeded and all Azure resources were destroyed '
        f'(job {job["id"]}).',
        flush=True,
    )
    return 0


def _qualify(args: argparse.Namespace) -> int:
    interrupt_at = getattr(args, 'interrupt_at', None)
    interrupt_mode = _effective_interrupt_mode(args)
    inject_initializer_response_loss = getattr(
        args,
        'inject_initializer_response_loss',
        False,
    )
    if args.resume:
        job_id = str(args.resume)
        if not application.RUN_ID_PATTERN.fullmatch(job_id):
            raise QualificationError(
                'Qualification job IDs must be 12 lowercase hex digits.'
            )
        print(f'Azure qualification job: {job_id}', flush=True)

        with _exclusive_job_lock(job_id):
            try:
                job = _load_candidate_job(job_id)

                # Complete every resume preflight before entering the runner,
                # whose finally block intentionally owns cleanup after cloud
                # work begins. A refused resume must leave the retained Azure
                # graph untouched for an explicit --cleanup-only decision.
                _require_resumable(job, measure=args.measure)
                if (
                    interrupt_at is not None
                    and _read_interruption_evidence(job_id) is not None
                ):
                    raise QualificationError(
                        'A recovery with retained interruption evidence cannot '
                        'inject another checkpoint.'
                    )
                _require_resume_workload_settings(job, args)
                ssh = _ssh_material(args.ssh_env_file)
                _job, plan = _resume_job(job, ssh)
                image_lock = _lock_for_run(args.image_lock, job)
                _preflight_anonymous_ghcr_images(image_lock)
            except QualificationError:
                # This updater reloads canonical evidence and is a no-op when
                # none exists. Preserve the interruption origin and cleanup
                # fields; only finalize the refused recovery attempt.
                _update_interruption_evidence(
                    {'id': job_id},
                    recovery_outcome='resume_refused',
                )
                raise

            def resume_setup():
                return plan, ssh, image_lock

            return _run_qualification(
                job,
                resume_setup,
                measure=args.measure,
                interrupt_at=interrupt_at,
                interrupt_mode=interrupt_mode,
                inject_initializer_response_loss=(
                    inject_initializer_response_loss
                ),
                recovery_attempt=True,
            )

    # A new run performs every local/registry validation before its ownership
    # journal exists. No Azure write is possible until the job is persisted and
    # entered into the cleanup-guaranteed lifecycle below.
    ssh = _ssh_material(args.ssh_env_file)
    image_lock = _read_image_lock(args.image_lock)
    _preflight_anonymous_ghcr_images(image_lock)
    plan = _plan(args, ssh)
    job, lease = _new_job(plan, ssh, image_lock)
    print(f'Azure qualification job: {job["id"]}', flush=True)

    def new_setup():
        return plan, ssh, image_lock

    with lease:
        return _run_qualification(
            job,
            new_setup,
            measure=args.measure,
            interrupt_at=interrupt_at,
            interrupt_mode=interrupt_mode,
            inject_initializer_response_loss=inject_initializer_response_loss,
        )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        _validate_interruption_options(args)
        if args.cleanup_only:
            return _cleanup_only(args.cleanup_only)
        if not args.resume and args.image_lock is None:
            raise QualificationError('--image-lock is required for a new run.')
        return _qualify(args)
    except QualificationError as exc:
        print(f'Qualification refused: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
