#!/usr/bin/env python3
"""Privately initialize and verify Hotel once, then destroy all owned resources.

Creates five billable role VMs using the existing infrastructure-only carrier.
Every Hotel application stays at zero replicas. This is neither a benchmark nor
a public run. No resume is supported: interruption or a lost mutation response
requires explicit cleanup-only using the retained immutable attempt identity.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import threading

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import main as application
from app import deathstarbench_distributed as runtime
from app.deathstarbench_candidate_artifacts import _read_input
from app.deathstarbench_candidate_deployment import _context, prepare_candidate_support_stage
from app.deathstarbench_hotel_initialization_artifacts import adopt_guarded_hotel_artifacts
from app.deathstarbench_hotel_initialization_operator import (
    OPERATOR_JOURNAL_KEY, PIN_KEY, make_hotel_initialization_pin,
    run_guarded_hotel_initialization_operator,
)
from app.run_lease import require_acquired_run_lease
from scripts import qualify_deathstarbench_candidate_support as support
from scripts import qualify_deathstarbench_distributed_shared as shared
from scripts import qualify_aws_oci_deathstarbench_distributed as pins
from scripts.deathstarbench_candidate_provider import CandidateCloud
from scripts.deathstarbench_hotel_initialization_operator_state import HotelInitializationStateStore
from scripts.deathstarbench_hotel_initialization_artifact_common import validate_guarded_preparation


STATE_ROOT = application.RUNS / '.candidate-support'
_ARTIFACT_OPTIONS = (
    'image_lock', 'publication', 'workload_preparation', 'driver_preparation',
    'guarded_lock', 'guarded_publication', 'guarded_preparation', 'upstream',
)
_PROVIDER_OPTIONS = (
    'aws_image_pin', 'oci_profile', 'oci_application_architecture',
    'oci_application_image_id', 'oci_support_image_id', 'oci_defined_tags_json',
)


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cleanup-only', metavar='RUN_ID')
    parser.add_argument('--plan', type=Path, help='Exact five-role infrastructure carrier JSON, without SSH keys.')
    for name in _ARTIFACT_OPTIONS:
        parser.add_argument('--' + name.replace('_', '-'), type=Path)
    parser.add_argument('--ssh-env-file', type=Path)
    parser.add_argument('--aws-image-pin', type=Path)
    parser.add_argument('--oci-config-file', type=Path)
    parser.add_argument('--oci-profile')
    parser.add_argument('--oci-application-architecture', choices=('x86_64', 'arm64'))
    parser.add_argument('--oci-application-image-id')
    parser.add_argument('--oci-support-image-id')
    parser.add_argument('--oci-defined-tags-json', type=Path)
    parser.add_argument('--reconcile-aws-capacity-rejection', metavar='CLOUDTRAIL_EVENT_ID')
    parser.add_argument('--interrupt-after', choices=(
        'cluster-ready', 'support-ready', 'initialize-response-returned', 'initialize-dispatched',
        'initialize-verified', 'verify-response-returned', 'verify-dispatched', 'verify-verified',
    ), help='Qualification-only lost-response/interruption checkpoint; never retried.')
    parser.add_argument('--interrupt-mode', choices=('graceful', 'hard'), default='graceful')
    return parser


def _validate_arguments(args):
    if args.interrupt_after is None and args.interrupt_mode != 'graceful':
        raise ValueError('--interrupt-mode requires --interrupt-after.')
    if args.reconcile_aws_capacity_rejection and not args.cleanup_only:
        raise ValueError('AWS rejected-capacity reconciliation requires cleanup-only.')
    if args.cleanup_only:
        if (args.plan is not None or args.ssh_env_file is not None or args.interrupt_after is not None
                or any(getattr(args, name) is not None for name in (*_ARTIFACT_OPTIONS, *_PROVIDER_OPTIONS))):
            raise ValueError('Cleanup-only accepts no plan, artifact, SSH, provider pin or interruption overrides.')
    elif args.plan is None or any(getattr(args, name) is None for name in _ARTIFACT_OPTIONS):
        raise ValueError('A fresh Hotel run requires its plan, both exact artifact evidence pairs and all complete prepared source trees.')


def _checkpoint(args, job, name, persist):
    if args.interrupt_after != name:
        return
    job['hotel_initialization_interruption'] = {
        'schema_version': 1, 'run_id': job['id'], 'checkpoint': name,
        'mode': args.interrupt_mode, 'recorded_at': application.now(),
        'recovery_policy': 'cleanup-only-never-replay',
        'application_start_allowed': False, 'measurement_qualified': False,
    }
    persist(job)
    if args.interrupt_mode == 'hard':
        os._exit(shared.HARD_INTERRUPTION_EXIT_CODE)
    raise application.RunCancelled('Requested private Hotel initialization qualification interruption; do not replay.')


def _run(args, *, store=None):
    from app.deathstarbench_candidate_image_access import (
        qualify_candidate_support_image_access, require_candidate_support_image_access,
    )
    from app.deathstarbench_hotel_image_access import qualify_guarded_hotel_image_access

    _validate_arguments(args)
    store = store or HotelInitializationStateStore(STATE_ROOT)
    if args.cleanup_only:
        run_id = args.cleanup_only
        lease = store.acquire(run_id)
        try:
            job = store.load(run_id, lease)
            pin = job['candidate_support_pin']
            hotel_pin = job[PIN_KEY]
            plan = support._infrastructure_plan(job['plan'])
            if args.reconcile_aws_capacity_rejection and plan.provider != 'aws':
                raise ValueError('Capacity-rejection reconciliation is AWS-only.')
            ssh = artifacts = guarded = None
            provider_pin = pin['provider_pin']
        except BaseException:
            lease.release()
            raise
    else:
        # Independently regenerate guarded source before any baseline registry
        # inspection, not merely before downloading the guarded image layers.
        validate_guarded_preparation(args.guarded_preparation, args.upstream)
        ssh = shared._ssh_material(args.ssh_env_file)
        plan = support._infrastructure_plan(_read_input(args.plan, 'infrastructure plan')[0], ssh)
        provider_pin = support._provider_pin(args, plan, ssh)
        args.workload = 'hotel_reservation'
        artifacts = support._adopt(args)
        guarded = adopt_guarded_hotel_artifacts(
            args.guarded_lock, args.guarded_publication, preparation=args.guarded_preparation,
            upstream=args.upstream, baseline=artifacts,
        )
        run_id, lease = store.allocate()
        try:
            pin = support._new_pin(run_id, plan, provider_pin, artifacts, ssh)
            hotel_pin = make_hotel_initialization_pin(run_id, pin, guarded)
            timestamp = application.now()
            job = {
                'id': run_id, 'plan': pin['infrastructure_plan'], 'candidate_support_pin': pin,
                PIN_KEY: hotel_pin, 'status': 'queued', 'events': [], 'resources': {},
                'results': [], 'created_at': timestamp, 'updated_at': timestamp,
            }
            store.publish_pin(pin, lease)
            store.publish_hotel_pin(hotel_pin, lease)
            store.save(job, lease)
            baseline_metadata, guarded_metadata = artifacts.metadata(), guarded.metadata()
            for filename, source, digest in (
                ('candidate-image-lock.json', args.image_lock, baseline_metadata['candidate_lock_sha256']),
                ('publication.json', args.publication, baseline_metadata['publication_document_sha256']),
            ):
                store.publish_artifact_input(run_id, filename, source, digest, lease)
            for filename, source, digest in (
                ('guarded-image-lock.json', args.guarded_lock, guarded_metadata['candidate_lock_sha256']),
                ('guarded-publication.json', args.guarded_publication, guarded_metadata['publication_document_sha256']),
            ):
                store.publish_guarded_artifact_input(run_id, filename, source, digest, lease)
        except BaseException:
            lease.release()
            raise

    with lease:
        job.update(_persist_state=False, _persist_results_artifact=False, _cancel_event=threading.Event())
        if ssh is not None:
            job.update(_key=ssh['private_key'], _passphrase=ssh['passphrase'], _public_key=ssh['public_key'])
            job.update(_candidate_ssh_lease=lease, _candidate_ssh_directory=store.directory(run_id))

        def persist(value):
            value['updated_at'] = application.now()
            store.save(value, lease)

        def emit(value, stage, message):
            value['events'].append({'at': application.now(), 'stage': stage, 'message': message})
            persist(value)

        def execute(value, command, **kwargs):
            store.require_lease(value['id'], lease)
            application.raise_if_cancelled(value)
            result = application.ssh(value, command, **kwargs)
            store.require_lease(value['id'], lease)
            return result

        cloud = CandidateCloud(plan, provider_pin, persist=persist, emit=emit, oci_config_file=args.oci_config_file)
        support_access_capability = None

        def prepare_runtime(value, held):
            require_acquired_run_lease(held, run_id)
            runtime.prepare_distributed_k3s_candidate(value, execute=execute, persist=persist, emit=emit)
            _checkpoint(args, value, 'cluster-ready', persist)

        def support_access(value, held):
            nonlocal support_access_capability
            require_acquired_run_lease(held, run_id)
            candidate, bundle = _context(value, artifacts, runtime.distributed_k3s_candidate_plan)[1:3]
            support_access_capability = qualify_candidate_support_image_access(candidate, bundle)
            return support_access_capability.metadata()

        def stage(value, held):
            candidate, bundle = _context(value, artifacts, runtime.distributed_k3s_candidate_plan)[1:3]
            require_candidate_support_image_access(support_access_capability, candidate, bundle)
            result = prepare_candidate_support_stage(value, artifacts, lease=held, execute=execute, persist=persist, emit=emit)
            _checkpoint(args, value, 'support-ready', persist)
            return result

        def initialize(value, held):
            from app.deathstarbench_hotel_live_controller import prepare_guarded_hotel_initialization
            # This full-byte capability is freshly obtained in this process and
            # is never restored from the retained JSON evidence files.
            access = qualify_guarded_hotel_image_access(guarded)
            return prepare_guarded_hotel_initialization(
                value, guarded, baseline_artifacts=artifacts, lease=held, execute=execute,
                persist=persist, image_access=access, fresh_audit=cloud.audit,
                expected_cloud_identity=value[OPERATOR_JOURNAL_KEY]['cloud_identity'],
                initialization_attempt_id=hotel_pin['initialization_attempt_id'], emit=emit,
                checkpoint=lambda name, current: _checkpoint(args, current, name, persist),
            )

        print(f'Private Hotel initialization {run_id}: {plan.provider}; state {store.directory(run_id)}', flush=True)
        with support._signals(job) as (_signal_state, cleanup_signals):
            if args.reconcile_aws_capacity_rejection:
                cloud.reconcile_aws_capacity_rejection(job, lease, event_id=args.reconcile_aws_capacity_rejection)
            outcome = run_guarded_hotel_initialization_operator(
                job, lease=lease, pin=pin, initialization_pin=hotel_pin,
                artifacts=artifacts, guarded_artifacts=guarded, persist=lambda value, _held: persist(value),
                provision=cloud.provision, fresh_audit=cloud.audit, prepare_runtime=prepare_runtime,
                support_image_access=support_access, stage=stage, initialize=initialize,
                destroy_and_prove=cloud.destroy_and_prove, cleanup_only=bool(args.cleanup_only),
                cleanup_signal_context=cleanup_signals,
                interrupt_exceptions=(KeyboardInterrupt, application.RunCancelled),
            )
            print(json.dumps(outcome.metadata(), indent=2, sort_keys=True), flush=True)
            return outcome.exit_code


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        return _run(args)
    except (ValueError, OSError, RuntimeError) as exc:
        print(f'Hotel initialization operator refused: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
