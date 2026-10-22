#!/usr/bin/env python3
"""Privately initialize and verify Hotel once, then destroy owned infrastructure.

Applications stay paused and public dispatch remains closed. New runs require
complete fresh adoption of the original and guarded artifacts. Recovery only
destroys infrastructure; it never resumes initialization or requires sources,
registry access, or SSH credentials.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import uuid

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_hotel_initialization_artifacts import adopt_guarded_hotel_artifacts
from scripts import qualify_deathstarbench_candidate_support as support
from scripts.deathstarbench_candidate_operator_state import fingerprint


PIN_SCHEMA = 'hotel-guarded-initialization-operator-pin-v1'
CHECKPOINTS = ('cluster-ready', 'support-ready', 'initializer-intent',
               'initializer-exit', 'verifier-intent')


def _parser():
    parser = support._parser(allow_resume=False, workloads=('hotel_reservation',),
                             interruption_checkpoints=CHECKPOINTS)
    parser.description = __doc__
    parser.add_argument('--guarded-image-lock', type=Path)
    parser.add_argument('--guarded-publication', type=Path)
    parser.add_argument('--guarded-preparation', type=Path)
    parser.add_argument('--upstream', type=Path)
    return parser


def _run(args, *, store=None):
    guarded_inputs = ('guarded_image_lock', 'guarded_publication', 'guarded_preparation', 'upstream')
    if args.resume:
        raise ValueError('Hotel initialization cannot resume; use --cleanup-only.')
    if args.cleanup_only:
        if any(getattr(args, name) is not None for name in guarded_inputs):
            raise ValueError('Cleanup-only does not accept guarded artifact or source overrides.')
        return support._run(args, store=store)
    if args.plan is None or any(getattr(args, name) is None for name in guarded_inputs + (
            'image_lock', 'publication', 'workload_preparation', 'driver_preparation')):
        raise ValueError('A new Hotel initialization requires --plan, both exact locks/publications, '
                         'both original prepared trees, --guarded-preparation and --upstream.')
    if args.workload not in (None, 'hotel_reservation'):
        raise ValueError('Only Hotel initialization is permitted.')
    if args.interrupt_after is None and args.interrupt_mode != 'graceful':
        raise ValueError('--interrupt-mode requires --interrupt-after.')
    if args.reconcile_aws_capacity_rejection:
        raise ValueError('--reconcile-aws-capacity-rejection requires --cleanup-only.')
    args.workload = 'hotel_reservation'
    baseline = support._adopt(args)
    guarded = adopt_guarded_hotel_artifacts(
        args.guarded_image_lock, args.guarded_publication,
        preparation=args.guarded_preparation, upstream=args.upstream, baseline=baseline,
    )
    attempt = str(uuid.uuid4())

    def persist_inputs(job, target, lease):
        metadata = guarded.metadata()
        pin = {
            'schema_version': 1, 'candidate_schema': PIN_SCHEMA,
            'run_id': job['id'], 'initialization_attempt_id': attempt,
            'candidate_only': True, 'released': False,
            'support_pin_fingerprint': job['candidate_support_pin']['fingerprint'],
            'baseline_artifact_fingerprint': baseline.fingerprint,
            'guarded_artifact_fingerprint': guarded.fingerprint,
            'guarded_artifacts': metadata,
            'application_start_allowed': False, 'runtime_qualified': False,
            'measurement_qualified': False, 'public_release': False,
        }
        pin['fingerprint'] = fingerprint(pin)
        target.publish_artifact_input(job['id'], 'guarded-image-lock.json', args.guarded_image_lock,
                                      metadata['candidate_lock_sha256'], lease)
        target.publish_artifact_input(job['id'], 'guarded-publication.json', args.guarded_publication,
                                      metadata['publication_document_sha256'], lease)
        target.publish_hotel_initialization_pin(pin, lease)
        job['guarded_hotel_initialization_pin'] = pin

    def initialize(job, lease, *, execute, persist, fresh_audit):
        # This import stays outside cleanup-only, which has no runtime dependency.
        from app.deathstarbench_hotel_controller import run_guarded_hotel_initialization
        from app.deathstarbench_candidate_operator import _hotel_operation_pin
        original_support_pin = json.loads(support.canonical(job['candidate_support_pin']))
        accepted = _hotel_operation_pin(job, original_support_pin, lease, True)

        def checked(function, *positional, **keywords):
            def binding():
                support.require_acquired_run_lease(lease, job['id'])
                if (support.canonical(job['candidate_support_pin']) != support.canonical(original_support_pin)
                        or support.canonical(_hotel_operation_pin(job, original_support_pin, lease, True))
                        != support.canonical(accepted)):
                    raise ValueError('Guarded Hotel operation identity changed during initialization.')
            binding()
            result = function(*positional, **keywords)
            binding()
            return result

        def guarded_persist(value):
            return checked(persist, value)

        return run_guarded_hotel_initialization(
            job, guarded, baseline_artifacts=baseline, initialization_attempt_id=attempt,
            lease=lease, execute=lambda *values, **options: checked(execute, *values, **options),
            persist=guarded_persist,
            fresh_audit=lambda *values, **options: checked(fresh_audit, *values, **options),
            checkpoint=lambda name: support._checkpoint(args, job, name, guarded_persist),
        )

    extension = support._SupportStageInitializer(baseline, persist_inputs, initialize)
    return support._run(args, store=store, initializer=extension)


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        return _run(args)
    except (ValueError, OSError, RuntimeError) as exc:
        print(f'Hotel initialization operator refused: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
