#!/usr/bin/env python3
"""Run the unreleased distributed DeathStarBench GCP qualification safely.

The harness creates the exact five-role Compute Engine candidate, installs the
same pinned K3s runtime and immutable Social Network bundle used by Azure, and
always attempts inventory-verified cleanup.  It remains operator-only and is
never dispatched by the public API/UI release path.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import os
from pathlib import Path
import signal
import sys
from typing import Any, Callable, Iterator, Mapping


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
    prepare_distributed_k3s_candidate,
    prepare_distributed_social_network_candidate,
)
from app.deathstarbench_distributed_measurement import (
    run_distributed_social_network_measurement,
)
from app.models import BenchmarkPlan
from app.providers import gcp
from app.resource_inventory import ROLE_NODE_INVENTORY_KEY
from scripts import qualify_azure_deathstarbench_distributed as shared


QualificationError = shared.QualificationError
LOCK_FILENAME = shared.LOCK_FILENAME
INTERRUPTION_CHECKPOINTS = shared.INTERRUPTION_CHECKPOINTS
INTERRUPTION_MODES = shared.INTERRUPTION_MODES
WORKLOAD_OPTION_DEFAULTS = dict(shared.WORKLOAD_OPTION_DEFAULTS)
SAFE_MEASUREMENT_RESUME_STATES = shared.SAFE_MEASUREMENT_RESUME_STATES

_CLEANUP_METADATA_KEYS = frozenset({
    ROLE_NODE_INVENTORY_KEY,
    gcp.DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
    gcp.DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY,
    K3S_RUNTIME_JOURNAL_KEY,
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
})


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
        '--project-id',
        default=(
            os.getenv('GOOGLE_CLOUD_PROJECT')
            or os.getenv('GCLOUD_PROJECT')
        ),
        help='Google Cloud project ID (or GOOGLE_CLOUD_PROJECT).',
    )
    parser.add_argument('--region', default='us-east1')
    parser.add_argument('--zone', default='us-east1-b')
    parser.add_argument('--application-machine-type', default='c4a-standard-8')
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
            'the harness process (hard). Requires --measure and --interrupt-at.'
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


def _plan(
    args: argparse.Namespace,
    ssh: Mapping[str, str | None],
) -> BenchmarkPlan:
    project_id = str(args.project_id or '').strip()
    if not project_id:
        raise QualificationError(
            'Pass --project-id or set GOOGLE_CLOUD_PROJECT.'
        )
    region = str(args.region or '').strip()
    zone = str(args.zone or '').strip()
    if not region or not zone or not zone.startswith(region + '-'):
        raise QualificationError(
            'The GCP qualification zone must belong to the selected region.'
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
        provider='gcp',
        gcp_project_id=project_id,
        gcp_zone=zone,
        region=region,
        shape=str(args.application_machine_type).strip(),
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


def _validate_candidate_job(job: Mapping[str, Any], *, job_id: str) -> None:
    """Fail closed before this harness mutates or destroys a saved run."""

    plan = job.get('plan')
    options = plan.get('deathstarbench') if isinstance(plan, dict) else None
    expected = {
        'topology_id': DISTRIBUTED_TIERED_TOPOLOGY_ID,
        'runtime_id': K3S_RUNTIME_ID,
        'workload': 'social_network',
    }
    if (
        not isinstance(plan, dict)
        or plan.get('provider') != 'gcp'
        or not isinstance(options, dict)
        or any(options.get(key) != value for key, value in expected.items())
    ):
        raise QualificationError(
            f'Saved job {job_id} is not a GCP distributed Social Network '
            'K3s qualification candidate.'
        )
    if application.has_recoverable_resources(job):
        resources = job.get('resources')
        if (
            not isinstance(resources, dict)
            or resources.get('provider') != 'gcp'
            or resources.get('gcp_distributed_candidate') is not True
        ):
            raise QualificationError(
                f'Saved job {job_id} has recoverable resources without the '
                'exact GCP distributed-candidate ownership markers.'
            )


def _load_candidate_job(job_id: str) -> dict[str, Any]:
    if not application.RUN_ID_PATTERN.fullmatch(job_id):
        raise QualificationError(
            'Qualification job IDs must be 12 lowercase hex digits.'
        )
    job = application.load_persisted_job(job_id)
    if job is None:
        raise QualificationError(
            f'No persisted qualification job {job_id!r} exists.'
        )
    _validate_candidate_job(job, job_id=job_id)
    return job


@contextmanager
def _exclusive_job_lock(job_id: str) -> Iterator[None]:
    lease = shared._acquire_job_lease(job_id)
    with lease:
        yield


def _require_resumable(
    job: Mapping[str, Any],
    *,
    measure: bool = False,
) -> None:
    job_id = str(job['id'])
    if job.get('status') in shared.RESUME_TEARDOWN_STATUSES:
        raise QualificationError(
            f'Qualification job {job_id} is already in '
            f'{job.get("status")!r}; use --cleanup-only {job_id} instead.'
        )
    if not job.get('resources'):
        raise QualificationError(
            f'Qualification job {job_id} has no recoverable GCP resources.'
        )
    resources = job.get('resources')
    execution = (
        resources.get(DEATHSTARBENCH_EXECUTION_JOURNAL_KEY)
        if isinstance(resources, Mapping)
        else None
    )
    if execution is None:
        return
    if not measure:
        raise QualificationError(
            f'Qualification job {job_id} already has a one-shot measurement '
            'journal; refusing to resume it as workload-only.'
        )
    state = execution.get('state') if isinstance(execution, Mapping) else None
    if state not in SAFE_MEASUREMENT_RESUME_STATES:
        raise QualificationError(
            f'Qualification job {job_id} measurement state {state!r} is not '
            'safe to resume; use --cleanup-only instead.'
        )


def _cleanup(job: dict[str, Any]) -> bool:
    if not job.get('resources'):
        shared._set_status(job, 'destroyed')
        return True
    shared._set_status(job, 'destroying')
    shared._event(job, 'Destroy', 'Removing the live GCP qualification resources.')
    application.destroy_with_status(job)
    resources = job.get('resources')
    ownership_keys = []
    if isinstance(resources, dict):
        ownership_keys = sorted(
            key
            for key in resources
            if key.startswith('gcp_') or key in _CLEANUP_METADATA_KEYS
        )
    else:
        ownership_keys = ['<malformed resources>']
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
    print(f'GCP cleanup proved complete for job {job["id"]}.', flush=True)
    return True


def _cleanup_only(job_id: str) -> int:
    _load_candidate_job(job_id)
    with _exclusive_job_lock(job_id):
        job = _load_candidate_job(job_id)
        evidence_error: Exception | None = None
        try:
            if shared._read_interruption_evidence(job_id) is not None:
                shared._update_interruption_evidence(
                    job,
                    recovery_outcome='cleanup_only_started',
                )
        except Exception as exc:
            evidence_error = exc
            print(
                f'Unable to update interruption recovery evidence: {exc}',
                file=sys.stderr,
                flush=True,
            )
        cleanup_complete = _cleanup(job)
        try:
            if shared._read_interruption_evidence(job_id) is not None:
                shared._update_interruption_evidence(
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
    recovery_attempt: bool = False,
) -> int:
    failure: BaseException | None = None
    qualified = False
    setup_complete = False
    cleanup_complete = False
    with shared._cooperative_signal_handlers(job) as signal_state:
        try:
            application.raise_if_cancelled(job)
            existing_evidence = shared._read_interruption_evidence(
                str(job['id'])
            )
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
                shared._update_interruption_evidence(
                    job,
                    recovery_outcome='resume_started',
                )
            plan, ssh, image_lock = setup()
            setup_complete = True
            application.raise_if_cancelled(job)
            if measure:
                job['_persist_results_artifact'] = True
            shared._set_status(job, 'provisioning')
            gcp.provision_distributed_deathstarbench_candidate(
                job,
                plan,
                public_key=str(ssh['public_key']),
                emit=shared._event,
                persist=shared._persist,
            )
            application.raise_if_cancelled(job)
            shared._set_status(job, 'testing')
            prepare_distributed_k3s_candidate(
                job,
                execute=application.ssh,
                emit=shared._event,
                persist=shared._persist,
            )
            application.raise_if_cancelled(job)
            prepare_distributed_social_network_candidate(
                job,
                image_lock,
                execute=application.ssh,
                emit=shared._event,
                persist=shared._persist,
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
                    'GCP candidate did not reach both exact readiness boundaries.'
                )
            if measure:
                measurement_kwargs = {}
                if interrupt_at is not None:
                    if interrupt_mode not in INTERRUPTION_MODES:
                        raise QualificationError(
                            'An interruption checkpoint requires a valid mode.'
                        )
                    measurement_kwargs['qualification_checkpoint'] = (
                        shared._qualification_checkpoint_callback(
                            interrupt_at=interrupt_at,
                            interrupt_mode=interrupt_mode,
                        )
                    )
                result = run_distributed_social_network_measurement(
                    job,
                    image_lock,
                    plan.deathstarbench,
                    execute=application.ssh,
                    emit=shared._event,
                    persist=shared._persist,
                    **measurement_kwargs,
                )
                application.raise_if_cancelled(job)
                shared._require_qualified_measurement_result(job, plan, result)
                shared._write_and_require_report(job)
                application.raise_if_cancelled(job)
            qualified = True
            shared._event(
                job,
                'Qualified',
                (
                    'GCP K3s, Social Network workload, one-shot measurement, '
                    'and saved report passed qualification.'
                    if measure
                    else 'GCP K3s and Social Network workload reached exact '
                    'workload_ready.'
                ),
            )
        except BaseException as exc:
            failure = exc
            failure_label = f'{type(exc).__name__}: {exc}'
            job['error'] = failure_label
            if isinstance(exc, (KeyboardInterrupt, application.RunCancelled)):
                shared._mark_interrupted_unless_benchmark_is_terminal(job)
            shared._set_status(job, 'cleanup_pending')
            print(
                f'Qualification failed: {failure_label}',
                file=sys.stderr,
                flush=True,
            )
        finally:
            signum = signal_state['number']
            if signum is not None:
                outcome = shared._mark_interrupted_unless_benchmark_is_terminal(
                    job
                )
                if outcome not in {'complete', 'failed'}:
                    job['error'] = None
                if failure is None:
                    failure = application.RunCancelled(
                        f'Qualification received {signal.Signals(signum).name}.'
                    )
                    shared._set_status(job, 'cleanup_pending')
                else:
                    shared._persist(job)
                try:
                    shared._record_signal_interruption(job, signum)
                except BaseException as evidence_error:
                    if failure is None:
                        failure = evidence_error
                    print(
                        'Unable to retain signal interruption evidence: '
                        f'{evidence_error}',
                        file=sys.stderr,
                        flush=True,
                    )
            try:
                cleanup_complete = _cleanup(job)
            except application.RunCancelled as cleanup_interruption:
                cleanup_complete = False
                if failure is None:
                    failure = cleanup_interruption
                job['cleanup_error'] = (
                    'GCP cleanup was interrupted by an operating-system '
                    'signal; recoverable ownership was retained for '
                    '--cleanup-only.'
                )
                shared._set_status(job, 'cleanup_failed')
                print(job['cleanup_error'], file=sys.stderr, flush=True)

            final_signum = signal_state['number']
            if final_signum is not None and signum is None:
                shared._mark_interrupted_unless_benchmark_is_terminal(job)
                if failure is None:
                    failure = application.RunCancelled(
                        'Qualification received '
                        f'{signal.Signals(final_signum).name} during cleanup.'
                    )
                try:
                    shared._record_signal_interruption(job, final_signum)
                except BaseException as evidence_error:
                    if failure is None:
                        failure = evidence_error
                    print(
                        'Unable to retain signal interruption evidence: '
                        f'{evidence_error}',
                        file=sys.stderr,
                        flush=True,
                    )
                shared._persist(job)
            try:
                existing_evidence = shared._read_interruption_evidence(
                    str(job['id'])
                )
                if existing_evidence is not None:
                    recovery_outcome = None
                    if recovery_attempt:
                        if qualified:
                            recovery_outcome = 'resume_completed'
                        elif not setup_complete:
                            recovery_outcome = 'resume_refused'
                        else:
                            recovery_outcome = 'resume_failed'
                    shared._update_interruption_evidence(
                        job,
                        cleanup_outcome=(
                            'completed' if cleanup_complete else 'failed'
                        ),
                        recovery_outcome=recovery_outcome,
                    )
            except BaseException as evidence_error:
                if failure is None:
                    failure = evidence_error
                print(
                    'Unable to finalize interruption evidence: '
                    f'{evidence_error}',
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
        f'Qualification succeeded and all GCP resources were destroyed '
        f'(job {job["id"]}).',
        flush=True,
    )
    return 0


def _qualify(args: argparse.Namespace) -> int:
    interrupt_at = getattr(args, 'interrupt_at', None)
    interrupt_mode = shared._effective_interrupt_mode(args)
    if args.resume:
        preliminary = _load_candidate_job(args.resume)
        job_id = str(preliminary['id'])
        print(f'GCP qualification job: {job_id}', flush=True)
        with _exclusive_job_lock(job_id):
            job = _load_candidate_job(job_id)

            def resume_setup():
                _require_resumable(job, measure=args.measure)
                shared._require_resume_workload_settings(job, args)
                ssh = shared._ssh_material()
                _job, plan = shared._resume_job(job, ssh)
                image_lock = shared._lock_for_run(args.image_lock, job)
                shared._preflight_anonymous_ghcr_images(image_lock)
                return plan, ssh, image_lock

            return _run_qualification(
                job,
                resume_setup,
                measure=args.measure,
                interrupt_at=interrupt_at,
                interrupt_mode=interrupt_mode,
                recovery_attempt=True,
            )

    ssh = shared._ssh_material()
    image_lock = shared._read_image_lock(args.image_lock)
    shared._preflight_anonymous_ghcr_images(image_lock)
    plan = _plan(args, ssh)
    job, lease = shared._new_job(plan, ssh, image_lock)
    print(f'GCP qualification job: {job["id"]}', flush=True)

    def new_setup():
        return plan, ssh, image_lock

    with lease:
        return _run_qualification(
            job,
            new_setup,
            measure=args.measure,
            interrupt_at=interrupt_at,
            interrupt_mode=interrupt_mode,
        )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        shared._validate_interruption_options(args)
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
