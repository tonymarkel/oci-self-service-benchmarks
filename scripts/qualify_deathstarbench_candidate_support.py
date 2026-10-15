#!/usr/bin/env python3
"""Privately qualify Hotel/Media backing services, then destroy owned resources.

This operator creates five billable role VMs. The supplied BenchmarkPlan is an
infrastructure-only carrier for the existing five-role contract, not a Social
Network execution request. No application, dataset, load driver, measurement or
public UI dispatch is permitted. State and leases are kept outside web history.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import math
import os
from pathlib import Path
import sys
import threading
from pydantic import ValidationError

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import main as application
from app import deathstarbench_distributed as runtime
from app.deathstarbench_candidate_artifacts import adopt_candidate_artifacts, _read_input
from app.deathstarbench_candidate_deployment import _context, prepare_candidate_support_stage
from app.deathstarbench_contract import DISTRIBUTED_TIERED_TOPOLOGY_ID, K3S_RUNTIME_ID
from app.models import BenchmarkPlan
from app.run_lease import require_acquired_run_lease
from scripts import qualify_aws_oci_deathstarbench_distributed as pins
from scripts import qualify_deathstarbench_distributed_shared as shared
from scripts.deathstarbench_candidate_operator_state import CandidateStateStore, canonical, fingerprint
from scripts.deathstarbench_candidate_provider import CandidateCloud


PIN_SCHEMA = 'candidate-support-operator-pin-v1'
STATE_ROOT = application.RUNS / '.candidate-support'


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--resume', metavar='RUN_ID')
    mode.add_argument('--cleanup-only', metavar='RUN_ID')
    parser.add_argument('--plan', type=Path, help='Exact five-role infrastructure BenchmarkPlan JSON for a new run.')
    parser.add_argument('--workload', choices=('hotel_reservation', 'media_microservices'))
    parser.add_argument('--image-lock', type=Path)
    parser.add_argument('--publication', type=Path)
    parser.add_argument('--workload-preparation', type=Path)
    parser.add_argument('--driver-preparation', type=Path)
    parser.add_argument('--ssh-env-file', type=Path)
    parser.add_argument('--aws-image-pin', type=Path)
    parser.add_argument('--oci-config-file', type=Path)
    parser.add_argument('--oci-profile')
    parser.add_argument('--oci-application-architecture', choices=('x86_64', 'arm64'))
    parser.add_argument('--oci-application-image-id')
    parser.add_argument('--oci-support-image-id')
    parser.add_argument('--oci-defined-tags-json', type=Path)
    parser.add_argument('--interrupt-after', choices=('cluster-ready', 'support-ready'),
                        help='Qualification-only interruption after the durable named checkpoint.')
    parser.add_argument('--interrupt-mode', choices=('graceful', 'hard'), default='graceful')
    return parser


def _infrastructure_plan(values, ssh=None):
    values = dict(values)
    forbidden = {'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'}
    if forbidden.intersection(values):
        raise ValueError('Keep SSH material out of infrastructure JSON; use --ssh-env-file.')
    # The typed carrier requires these fields even for SDK-only deletion. Its
    # cleanup placeholders are never persisted and never passed to SSH.
    material = ssh or {'private_key': 'cleanup-only-no-ssh', 'public_key': 'cleanup-only-no-ssh', 'passphrase': None}
    values.update(ssh_private_key=material['private_key'], ssh_public_key=material['public_key'],
                  ssh_key_passphrase=material['passphrase'])
    try:
        plan = BenchmarkPlan.model_validate(values)
    except ValidationError:
        # Pydantic model-level errors can include the entire input mapping,
        # including the in-memory private key/passphrase injected above.
        raise ValueError('Infrastructure plan is invalid; SSH input details are withheld.') from None
    if (tuple(plan.benchmarks) != ('deathstarbench',) or plan.llm_benchmarks
            or plan.deathstarbench.topology_id != DISTRIBUTED_TIERED_TOPOLOGY_ID
            or plan.deathstarbench.runtime_id != K3S_RUNTIME_ID
            or plan.deathstarbench.workload != 'social_network'
            or plan.destroy_after_completion is not True
            or plan.storage.additional_volume):
        raise ValueError('Only the exact destroy-after five-role infrastructure carrier is accepted.')
    return plan


def _provider_pin(args, plan, ssh):
    if plan.provider in ('azure', 'gcp'):
        return {
            'schema_version': 1, 'provider': plan.provider,
            'region': plan.region,
            'account_selector': plan.azure_subscription_id if plan.provider == 'azure' else plan.gcp_project_id,
            'zone': plan.azure_zone if plan.provider == 'azure' else plan.gcp_zone,
        }
    capacity = plan.ocpus
    if not math.isfinite(capacity) or capacity != int(capacity):
        raise ValueError('This provider operator requires integral application CPU capacity.')
    options = argparse.Namespace(
        provider=plan.provider, region=plan.region, availability_zone=plan.availability_domain,
        application_shape=plan.shape, application_vcpus=int(capacity),
        application_memory_gib=plan.memory_gb, aws_profile=plan.aws_profile,
        aws_image_pin=args.aws_image_pin, oci_profile=args.oci_profile or 'DEFAULT',
        oci_config_file=args.oci_config_file, oci_compartment_id=plan.compartment_id,
        oci_application_architecture=args.oci_application_architecture,
        oci_application_image_id=args.oci_application_image_id,
        oci_support_image_id=args.oci_support_image_id,
        oci_defined_tags_json=args.oci_defined_tags_json,
        **shared.WORKLOAD_OPTION_DEFAULTS,
    )
    value = pins._new_provider_pin(options, ssh)
    if plan.provider == 'oci':
        expected_tags = pins._validate_oci_defined_tags(plan.oci_defined_tags)
        if args.oci_defined_tags_json is not None and canonical(value['defined_tags']) != canonical(expected_tags):
            raise ValueError('OCI defined-tag input must equal the exact infrastructure plan tags.')
        value['defined_tags'] = expected_tags
        value = pins._validate_provider_pin(value)
    return value


def _new_pin(run_id, plan, provider_pin, artifacts, ssh):
    value = {
        'schema_version': 1, 'candidate_schema': PIN_SCHEMA,
        'candidate_only': True, 'released': False, 'run_id': run_id,
        'workload_id': artifacts.workload_id, 'artifact_fingerprint': artifacts.fingerprint,
        'artifacts': artifacts.metadata(),
        'infrastructure_plan': plan.model_dump(exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'}),
        'provider_pin': provider_pin,
        'ssh_public_key_sha256': pins._public_key_digest(ssh['public_key']),
    }
    return {**value, 'fingerprint': fingerprint(value)}


def _adopt(args, *, workload=None, lock=None, publication=None):
    if (args.workload_preparation is None or args.driver_preparation is None
            or (lock or args.image_lock) is None or (publication or args.publication) is None):
        raise ValueError('Fresh artifact adoption requires exact lock/publication and both complete prepared trees.')
    return adopt_candidate_artifacts(
        workload or args.workload, lock or args.image_lock, publication or args.publication,
        workload_preparation=args.workload_preparation, driver_preparation=args.driver_preparation,
    )


@contextmanager
def _signals(job):
    """Defer repeated cleanup signals while preserving their nonzero outcome."""
    with shared._cooperative_signal_handlers(job) as state:
        @contextmanager
        def cleanup_signals():
            state['raise_cancel'] = False
            try:
                yield state
            finally:
                # No signal can turn a successful cleanup into process success.
                state['raise_cancel'] = False
        yield state, cleanup_signals


def _checkpoint(args, job, name, persist):
    if args.interrupt_after != name:
        return
    job['candidate_support_interruption'] = {
        'schema_version': 1, 'run_id': job['id'], 'checkpoint': name,
        'mode': args.interrupt_mode, 'recorded_at': application.now(),
        'application_start_allowed': False, 'measurement_qualified': False,
    }
    persist(job)
    if args.interrupt_mode == 'hard':
        os._exit(shared.HARD_INTERRUPTION_EXIT_CODE)
    raise application.RunCancelled('Requested support-stage qualification interruption.')


def _run(args, *, store=None):
    from app.deathstarbench_candidate_operator import run_candidate_support_operator
    from app.deathstarbench_candidate_image_access import (
        qualify_candidate_support_image_access, require_candidate_support_image_access,
    )
    if args.interrupt_after is None and args.interrupt_mode != 'graceful':
        raise ValueError('--interrupt-mode requires --interrupt-after.')
    if args.cleanup_only and (args.interrupt_after or args.plan or args.workload or args.image_lock or args.publication
                              or args.workload_preparation or args.driver_preparation):
        raise ValueError('Cleanup-only does not accept artifact, workload, plan or interruption overrides.')
    if args.resume and args.plan is not None:
        raise ValueError('Resume uses the exact saved infrastructure plan, never a replacement.')
    if (args.resume or args.cleanup_only) and any(getattr(args, key) is not None for key in (
        'aws_image_pin', 'oci_profile', 'oci_application_architecture', 'oci_application_image_id',
        'oci_support_image_id', 'oci_defined_tags_json',
    )):
        raise ValueError('Recovery uses the immutable saved provider pin; provider overrides are refused.')
    store = store or CandidateStateStore(STATE_ROOT)
    recovering = args.cleanup_only or args.resume
    lease = None
    if recovering:
        run_id = recovering
        lease = store.acquire(run_id)
        try:
            job = store.load(run_id, lease)
            pin = job['candidate_support_pin']
            if args.workload is not None and args.workload != pin['workload_id']:
                raise ValueError('Resume workload differs from its immutable candidate pin.')
            ssh = None if args.cleanup_only else shared._ssh_material(args.ssh_env_file)
            plan = _infrastructure_plan(job['plan'], ssh)
            if ssh is not None and pins._public_key_digest(ssh['public_key']) != pin['ssh_public_key_sha256']:
                raise ValueError('Resume SSH key differs from the original candidate pin.')
            artifacts = None
            if not args.cleanup_only:
                directory = store.directory(run_id)
                artifacts = _adopt(args, workload=pin['workload_id'],
                                   lock=args.image_lock or directory / 'candidate-image-lock.json',
                                   publication=args.publication or directory / 'publication.json')
            provider_pin = pin['provider_pin']
        except BaseException:
            lease.release()
            raise
    else:
        if args.plan is None or args.workload is None:
            raise ValueError('A new support qualification requires --plan and --workload.')
        ssh = shared._ssh_material(args.ssh_env_file)
        plan = _infrastructure_plan(_read_input(args.plan, 'infrastructure plan')[0], ssh)
        provider_pin = _provider_pin(args, plan, ssh)
        artifacts = _adopt(args)
        run_id, lease = store.allocate()
        try:
            pin = _new_pin(run_id, plan, provider_pin, artifacts, ssh)
            timestamp = application.now()
            job = {
                'id': run_id, 'plan': pin['infrastructure_plan'], 'candidate_support_pin': pin,
                'status': 'queued', 'events': [], 'resources': {}, 'results': [],
                'created_at': timestamp, 'updated_at': timestamp,
            }
            store.publish_pin(pin, lease)
            store.save(job, lease)
            metadata = artifacts.metadata()
            store.publish_artifact_input(run_id, 'candidate-image-lock.json', args.image_lock,
                                         metadata['candidate_lock_sha256'], lease)
            store.publish_artifact_input(run_id, 'publication.json', args.publication,
                                         metadata['publication_document_sha256'], lease)
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
        access = None

        def context():
            return _context(job, artifacts, runtime.distributed_k3s_candidate_plan)[1:3]

        def prepare_runtime(value, held):
            require_acquired_run_lease(held, run_id)
            runtime.prepare_distributed_k3s_candidate(value, execute=execute, persist=persist, emit=emit)
            _checkpoint(args, value, 'cluster-ready', persist)

        def support_access(value, held):
            nonlocal access
            require_acquired_run_lease(held, run_id)
            candidate, stage = context()
            access = qualify_candidate_support_image_access(candidate, stage)
            return access.metadata()

        def stage(value, held):
            candidate, bundle = context()
            require_candidate_support_image_access(access, candidate, bundle)
            result = prepare_candidate_support_stage(value, artifacts, lease=held, execute=execute, persist=persist, emit=emit)
            _checkpoint(args, value, 'support-ready', persist)
            return result

        print(f'Private support candidate {run_id}: {plan.provider}/{pin["workload_id"]}; state {store.directory(run_id)}', flush=True)
        with _signals(job) as (signal_state, cleanup_signals):
            outcome = run_candidate_support_operator(
                job, lease=lease, pin=pin, artifacts=artifacts, persist=lambda value, _held: persist(value),
                provision=cloud.provision, fresh_audit=cloud.audit, prepare_runtime=prepare_runtime,
                support_image_access=support_access, stage=stage,
                destroy_and_prove=cloud.destroy_and_prove, resume=bool(args.resume),
                cleanup_only=bool(args.cleanup_only), cleanup_signal_context=cleanup_signals,
                interrupt_exceptions=(KeyboardInterrupt, application.RunCancelled),
            )
            print(json.dumps(outcome.metadata(), indent=2, sort_keys=True), flush=True)
            return outcome.exit_code


def main(argv=None):
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        return _run(args)
    except (ValueError, OSError, RuntimeError) as exc:
        print(f'Candidate support operator refused: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
