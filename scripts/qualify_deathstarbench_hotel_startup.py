#!/usr/bin/env python3
"""Privately initialize and start Hotel once, then destroy exact-owned resources.

Fresh mode creates five billable role VMs and a fresh dataset. Startup-only
observations are not performance qualification or public release. There is no
execution replay: an old identity is accepted only with --destroy-only.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
from pathlib import Path
import re
import signal
import sys
import threading
import uuid
import json

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import main as application
from app import deathstarbench_distributed as runtime
from app import deathstarbench_candidate_operator as support_operator
from app.deathstarbench_candidate_artifacts import _read_input
from app.deathstarbench_candidate_deployment import _context, prepare_candidate_support_stage
from app.deathstarbench_hotel_initialization_artifacts import adopt_guarded_hotel_artifacts
from app.deathstarbench_hotel_initialization_operator import make_hotel_initialization_pin
from app.deathstarbench_hotel_startup_artifacts import adopt_startup_hotel_artifacts
from app.deathstarbench_hotel_startup_operator import (
    OPERATOR_JOURNAL_KEY, make_hotel_startup_pin, run_hotel_startup_operator,
)
from app.deathstarbench_hotel_startup_ownership import (
    CHECKPOINT_KEY, validate_hotel_startup_cleanup_not_required,
)
from app.run_lease import require_acquired_run_lease
from scripts import qualify_deathstarbench_candidate_support as support
from scripts import qualify_deathstarbench_distributed_shared as shared
from scripts.deathstarbench_candidate_provider import CandidateCloud
from scripts.deathstarbench_hotel_startup_operator_state import HotelStartupStateStore
from scripts.deathstarbench_hotel_initialization_artifact_common import validate_guarded_preparation
from scripts.deathstarbench_hotel_startup_artifact_common import validate_startup_preparation

STATE_ROOT = application.RUNS / '.candidate-support'
_ARTIFACT_OPTIONS = (
    'image_lock', 'publication', 'workload_preparation', 'driver_preparation',
    'guarded_lock', 'guarded_publication', 'guarded_preparation',
    'startup_lock', 'startup_publication', 'startup_preparation', 'upstream',
)
_PROVIDER_OPTIONS = (
    'aws_image_pin', 'oci_profile', 'oci_application_architecture',
    'oci_application_image_id', 'oci_support_image_id', 'oci_defined_tags_json',
)


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--destroy-only', action='store_true')
    parser.add_argument('--run-id', help='Existing private identity; destroy-only, never execution.')
    parser.add_argument('--plan', type=Path, help='Exact destroy-after five-role infrastructure carrier, without SSH keys.')
    for name in _ARTIFACT_OPTIONS:
        parser.add_argument('--' + name.replace('_', '-'), type=Path)
    parser.add_argument('--ssh-env-file', type=Path)
    parser.add_argument('--registry-egress', type=Path,
                        help='Explicit private registry HTTPS forwarding; does not change TLS trust or cloud SDK traffic.')
    parser.add_argument('--aws-image-pin', type=Path)
    parser.add_argument('--oci-config-file', type=Path, help='Local OCI SDK authentication only; never overrides pinned scope.')
    parser.add_argument('--oci-profile')
    parser.add_argument('--oci-application-architecture', choices=('x86_64', 'arm64'))
    parser.add_argument('--oci-application-image-id')
    parser.add_argument('--oci-support-image-id')
    parser.add_argument('--oci-defined-tags-json', type=Path)
    return parser


def _validate_arguments(args):
    if args.destroy_only:
        if (not isinstance(args.run_id, str) or re.fullmatch(r'[0-9a-f]{12}', args.run_id) is None
                or any(getattr(args, name) is not None for name in (
                    'plan', 'ssh_env_file', 'registry_egress', *_ARTIFACT_OPTIONS, *_PROVIDER_OPTIONS))):
            raise ValueError('Destroy-only requires one exact private run ID and no execution or provider-selector overrides.')
    elif (args.run_id is not None or args.plan is None
            or any(getattr(args, name) is None for name in _ARTIFACT_OPTIONS)):
        raise ValueError('Fresh startup requires all three original evidence pairs, complete source preparations and a new identity.')


def _run(args, *, store=None):
    _validate_arguments(args)
    if args.registry_egress is None:
        return _run_private(args, store=store)
    from scripts.deathstarbench_registry_egress import load_mapping, registry_egress
    with registry_egress(load_mapping(args.registry_egress)):
        return _run_private(args, store=store)


def _destroy(args, store):
    # No history, source, registry, SSH or Kubernetes is opened by this path.
    with store.acquire(args.run_id) as lease:
        job = store.load_for_destroy(args.run_id, lease)
        pin = job['candidate_support_pin']
        plan = support._infrastructure_plan(job['plan'])
        if args.oci_config_file is not None and plan.provider != 'oci':
            raise ValueError('OCI authentication config requires an independently pinned OCI cleanup carrier.')
        job.update(_persist_state=False, _persist_results_artifact=False,
                   _cancel_event=threading.Event())
        cloud = CandidateCloud(plan, pin['provider_pin'],
            persist=lambda value: store.save_cleanup_progress(value, lease), emit=lambda *_: None,
            oci_config_file=args.oci_config_file, operator_journal_key=OPERATOR_JOURNAL_KEY)
        cleanup = 'failed'
        interrupted = False
        with support._signals(job) as (signals, cleanup_signals):
            with cleanup_signals():
                try:
                    proof = cloud.destroy_and_prove(job, lease)
                    if proof.get('candidate_schema') == 'hotel-startup-cleanup-not-required-v1':
                        validate_hotel_startup_cleanup_not_required(proof, job[CHECKPOINT_KEY])
                        cleanup = 'not_required'
                    else:
                        support_operator._cleanup_proof(proof, pin)
                        cleanup = 'completed'
                except (Exception, KeyboardInterrupt, application.RunCancelled) as exc:
                    interrupted = isinstance(exc, (KeyboardInterrupt, application.RunCancelled))
        number = signals.get('number')
        interrupted = interrupted or number in (signal.SIGINT, signal.SIGTERM)
        if number in (signal.SIGINT, signal.SIGTERM):
            code = 128 + int(number)
        elif interrupted:
            code = 130
        else:
            code = 0 if cleanup == 'completed' else 2
        print(json.dumps({'mode': 'destroy-only', 'execution_outcome': 'not-evaluated',
            'cleanup_outcome': cleanup, 'interrupted': interrupted, 'exit_code': code}, sort_keys=True), flush=True)
        return code


def _run_private(args, *, store=None):
    _validate_arguments(args)
    store = store or HotelStartupStateStore(STATE_ROOT)
    if args.destroy_only:
        return _destroy(args, store)

    # Fail closed on both independent source reconstructions before registry,
    # SSH custody, allocation or any cloud SDK boundary.
    validate_guarded_preparation(args.guarded_preparation, args.upstream)
    validate_startup_preparation(args.startup_preparation, args.upstream)
    ssh = shared._ssh_material(args.ssh_env_file)
    plan = support._infrastructure_plan(_read_input(args.plan, 'infrastructure plan')[0], ssh)
    if args.oci_config_file is not None and plan.provider != 'oci':
        raise ValueError('OCI authentication config requires an OCI infrastructure carrier.')
    provider_pin = support._provider_pin(args, plan, ssh)
    args.workload = 'hotel_reservation'
    artifacts = support._adopt(args)
    guarded = adopt_guarded_hotel_artifacts(args.guarded_lock, args.guarded_publication,
        preparation=args.guarded_preparation, upstream=args.upstream, baseline=artifacts)
    startup_artifacts = adopt_startup_hotel_artifacts(args.startup_lock, args.startup_publication,
        preparation=args.startup_preparation, upstream=args.upstream, baseline=artifacts)
    run_id, lease = store.allocate()
    try:
        pin = support._new_pin(run_id, plan, provider_pin, artifacts, ssh)
        # Startup native ownership binds the exact configured key bytes,
        # including native-valid comments. The nested legacy provider pin
        # independently retains its normalized key-material digest.
        pin['ssh_public_key_sha256'] = hashlib.sha256(ssh['public_key'].encode('utf-8')).hexdigest()
        pin['fingerprint'] = support.fingerprint({key: value for key, value in pin.items() if key != 'fingerprint'})
        initialization_pin = make_hotel_initialization_pin(run_id, pin, guarded)
        startup_pin = make_hotel_startup_pin(run_id, pin, initialization_pin, startup_artifacts,
                                            startup_attempt_id=str(uuid.uuid4()))
        timestamp = application.now()
        job = {'id': run_id, 'plan': pin['infrastructure_plan'], 'candidate_support_pin': pin,
            'hotel_initialization_pin': initialization_pin, 'hotel_startup_pin': startup_pin,
            'status': 'queued', 'events': [], 'resources': {}, 'results': [],
            'created_at': timestamp, 'updated_at': timestamp,
            '_persist_state': False, '_persist_results_artifact': False, '_cancel_event': threading.Event()}
        store.publish_pin(pin, lease)
        store.publish_hotel_pin(initialization_pin, lease)
        store.publish_startup_pin(startup_pin, lease)
        for binding, publisher, inputs in (
            (artifacts, store.publish_artifact_input, (
                ('candidate-image-lock.json', args.image_lock), ('publication.json', args.publication))),
            (guarded, store.publish_guarded_artifact_input, (
                ('guarded-image-lock.json', args.guarded_lock), ('guarded-publication.json', args.guarded_publication))),
            (startup_artifacts, store.publish_startup_artifact_input, (
                ('startup-image-lock.json', args.startup_lock), ('startup-publication.json', args.startup_publication))),
        ):
            metadata = binding.metadata()
            for (filename, source), key in zip(inputs, ('candidate_lock_sha256', 'publication_document_sha256')):
                publisher(run_id, filename, source, metadata[key], lease)
        # The outer operator creates its first checkpoint and accepted history;
        # pre-saving would turn this fresh invocation into forbidden replay.
    except BaseException:
        lease.release()
        raise

    with lease:
        job.update(_key=ssh['private_key'], _passphrase=ssh['passphrase'], _public_key=ssh['public_key'],
                   _candidate_ssh_lease=lease, _candidate_ssh_directory=store.directory(run_id))
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
            application.raise_if_cancelled(value)
            return result

        cloud = CandidateCloud(plan, provider_pin, persist=persist, emit=emit,
            oci_config_file=args.oci_config_file, operator_journal_key=OPERATOR_JOURNAL_KEY)
        from app.deathstarbench_candidate_image_access import (
            qualify_candidate_support_image_access, require_candidate_support_image_access,
        )
        support_capability = None

        def prepare_runtime(value, held):
            require_acquired_run_lease(held, run_id)
            runtime.prepare_distributed_k3s_candidate(value, execute=execute, persist=persist, emit=emit)

        def support_access(value, held):
            nonlocal support_capability
            require_acquired_run_lease(held, run_id)
            candidate, bundle = _context(value, artifacts, runtime.distributed_k3s_candidate_plan)[1:3]
            support_capability = qualify_candidate_support_image_access(candidate, bundle)
            return support_capability.metadata()

        def stage(value, held):
            require_acquired_run_lease(held, run_id)
            candidate, bundle = _context(value, artifacts, runtime.distributed_k3s_candidate_plan)[1:3]
            require_candidate_support_image_access(support_capability, candidate, bundle)
            return prepare_candidate_support_stage(value, artifacts, lease=held, execute=execute, persist=persist, emit=emit)

        def initialize(value, held):
            from app.deathstarbench_hotel_image_access import qualify_guarded_hotel_image_access
            from app.deathstarbench_hotel_live_controller import prepare_guarded_hotel_initialization
            require_acquired_run_lease(held, run_id)
            return prepare_guarded_hotel_initialization(value, guarded, baseline_artifacts=artifacts,
                lease=held, execute=execute, persist=persist, image_access=qualify_guarded_hotel_image_access(guarded),
                fresh_audit=cloud.audit, expected_cloud_identity=value[OPERATOR_JOURNAL_KEY]['cloud_identity'],
                initialization_attempt_id=initialization_pin['initialization_attempt_id'], emit=emit)

        def startup(value, held, *, initialization_plan, initialization_stage):
            from app.deathstarbench_hotel_startup_image_access import qualify_startup_hotel_image_access
            from app.deathstarbench_hotel_startup_controller import prepare_startup_hotel_applications
            require_acquired_run_lease(held, run_id)
            return prepare_startup_hotel_applications(value, startup_artifacts, baseline_artifacts=artifacts,
                initialization_plan=initialization_plan, initialization_stage=initialization_stage,
                lease=held, execute=execute, persist=persist, image_access=qualify_startup_hotel_image_access(startup_artifacts),
                fresh_audit=cloud.audit, expected_cloud_identity=value[OPERATOR_JOURNAL_KEY]['cloud_identity'],
                startup_attempt_id=startup_pin['startup_attempt_id'], emit=emit)

        print(f'Private Hotel startup {run_id}: {plan.provider}; not performance qualification', flush=True)
        with support._signals(job) as (_signals, cleanup_signals):
            outcome = run_hotel_startup_operator(job, lease=lease, pin=pin,
                initialization_pin=initialization_pin, startup_pin=startup_pin,
                persist=lambda value, held: persist(value), provision=cloud.provision, fresh_audit=cloud.audit,
                prepare_runtime=prepare_runtime, support_image_access=support_access, stage=stage,
                initialize=initialize, startup=startup, destroy_and_prove=cloud.destroy_and_prove,
                artifacts=artifacts, guarded_artifacts=guarded, startup_artifacts=startup_artifacts,
                cleanup_signal_context=cleanup_signals)
            print(json.dumps({'mode': 'fresh', **asdict(outcome)}, sort_keys=True), flush=True)
            return outcome.exit_code


def main(argv=None) -> int:
    args = _parser().parse_args(argv)
    try:
        return _run(args)
    except (KeyboardInterrupt, application.RunCancelled):
        print('Hotel startup refused: interrupted.', file=sys.stderr)
        return 130
    except Exception:
        # Legacy SDK/config/SSH errors can include secrets and signed URLs.
        # Never interpolate exception text or dynamic class names here.
        print('Hotel startup refused: private qualification or cleanup failed.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
