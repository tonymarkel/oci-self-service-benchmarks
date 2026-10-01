#!/usr/bin/env python3
"""Safely qualify the unreleased AWS or OCI distributed DSB candidate.

This is an operator-only harness.  It deliberately bypasses the public
dispatcher, persists immutable provider/image inputs before the first cloud
write, publishes a cloud-validated runtime projection before the first SSH
command, and always uses the candidate provider's dedicated cleanup path.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sys
from typing import Any, Callable, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import main as application
from app.deathstarbench_contract import (
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    K3S_RUNTIME_ID,
    K3S_RUNTIME_JOURNAL_KEY,
)
from app.deathstarbench_distributed import (
    DISTRIBUTED_NETWORK_QUALIFICATION_KEY,
    prepare_distributed_k3s_candidate,
    prepare_distributed_social_network_candidate,
    qualify_distributed_network_paths,
)
from app.deathstarbench_distributed_measurement import (
    run_distributed_social_network_measurement,
)
from app.models import BenchmarkPlan
from app.providers import aws, oci
from scripts import qualify_deathstarbench_distributed_shared as shared


QualificationError = shared.QualificationError
INTERRUPTION_CHECKPOINTS = shared.INTERRUPTION_CHECKPOINTS
INTERRUPTION_MODES = shared.INTERRUPTION_MODES
SAFE_MEASUREMENT_RESUME_STATES = shared.SAFE_MEASUREMENT_RESUME_STATES
WORKLOAD_OPTION_DEFAULTS = dict(shared.WORKLOAD_OPTION_DEFAULTS)

PROVIDER_PIN_FILENAME = 'distributed-provider-pin.json'
PROVIDER_PIN_SCHEMA_VERSION = 2
MAX_PROVIDER_PIN_BYTES = 64 * 1024
AWS_IMAGE_PIN_SCHEMA_VERSION = 1
AWS_IMAGE_FIELDS = frozenset({
    'image_id',
    'owner_id',
    'name',
    'creation_date',
    'product_code',
    'architecture',
    'root_device_name',
})


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--provider', required=True, choices=('aws', 'oci'))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--resume', metavar='JOB_ID')
    mode.add_argument('--cleanup-only', metavar='JOB_ID')
    parser.add_argument('--image-lock', type=Path)
    parser.add_argument(
        '--ssh-env-file',
        type=Path,
        help=(
            'Optional .env file containing benchmark SSH key paths. Useful '
            'when the qualification script runs from an isolated worktree.'
        ),
    )

    parser.add_argument('--region')
    parser.add_argument('--availability-zone')
    parser.add_argument('--application-shape')
    parser.add_argument('--application-vcpus', type=int)
    parser.add_argument('--application-memory-gib', type=float)

    parser.add_argument('--aws-profile')
    parser.add_argument(
        '--aws-image-pin',
        type=Path,
        help=(
            'JSON containing exact support_image and application_image Rocky '
            'Linux 9 official-public or Marketplace AMI pins. Required for a '
            'new AWS run.'
        ),
    )

    parser.add_argument('--oci-config-file', type=Path)
    parser.add_argument('--oci-profile')
    parser.add_argument('--oci-compartment-id')
    parser.add_argument('--oci-application-architecture', choices=('x86_64', 'arm64'))
    parser.add_argument('--oci-application-image-id')
    parser.add_argument('--oci-support-image-id')
    parser.add_argument(
        '--oci-defined-tags-json',
        type=Path,
        help=(
            'Optional JSON object of exact OCI defined tags to apply to every '
            'taggable candidate resource. Use this when compartment tag '
            'defaults must be supplied explicitly.'
        ),
    )

    parser.add_argument('--measure', action='store_true')
    parser.add_argument('--interrupt-at', choices=INTERRUPTION_CHECKPOINTS)
    parser.add_argument('--interrupt-mode', choices=INTERRUPTION_MODES)
    parser.add_argument('--inject-initializer-response-loss', action='store_true')
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


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise QualificationError(f'Unable to read {label} {path}: {exc}') from exc
    if size <= 0 or size > MAX_PROVIDER_PIN_BYTES:
        raise QualificationError(
            f'{label.capitalize()} must be between 1 and '
            f'{MAX_PROVIDER_PIN_BYTES} bytes.'
        )
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise QualificationError(f'Unable to parse {label} {path}: {exc}') from exc
    if not isinstance(value, dict):
        raise QualificationError(f'{label.capitalize()} must be one JSON object.')
    return value


def _validate_oci_defined_tags(value: Any) -> dict[str, dict[str, str]]:
    try:
        oci._validate_defined_tags(value)
    except oci.LifecycleError as exc:
        raise QualificationError(str(exc)) from exc
    return copy.deepcopy(value)


def _read_oci_defined_tags(path: Path | None) -> dict[str, dict[str, str]]:
    if path is None:
        return {}
    return _validate_oci_defined_tags(
        _read_json(path, label='OCI defined-tags JSON')
    )


def _validate_aws_image(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != AWS_IMAGE_FIELDS:
        raise QualificationError(
            f'{label} must contain exactly: ' + ', '.join(sorted(AWS_IMAGE_FIELDS))
        )
    if (
        not re.fullmatch(r'ami-[0-9a-f]{8,17}', str(value['image_id']))
        or not re.fullmatch(r'[0-9]{12}', str(value['owner_id']))
        or not isinstance(value['name'], str)
        or not value['name']
        or not isinstance(value['creation_date'], str)
        or not value['creation_date']
        or not (
            (
                value['owner_id'] == aws.AWS_ROCKY_OFFICIAL_OWNER_ID
                and value['product_code'] is None
            )
            or (
                isinstance(value['product_code'], str)
                and bool(value['product_code'])
            )
        )
        or value['architecture'] not in {'x86_64', 'arm64'}
        or not isinstance(value['root_device_name'], str)
        or not value['root_device_name'].startswith('/dev/')
    ):
        raise QualificationError(f'{label} contains an invalid AMI identity field.')
    return dict(value)


def _read_aws_image_pin(path: Path | None) -> dict[str, Any]:
    if path is None:
        raise QualificationError('--aws-image-pin is required for a new AWS run.')
    value = _read_json(path, label='AWS image pin')
    if set(value) != {
        'schema_version', 'provider', 'support_image', 'application_image'
    } or value.get('schema_version') != AWS_IMAGE_PIN_SCHEMA_VERSION or value.get('provider') != 'aws':
        raise QualificationError('AWS image pin has an unsupported exact schema.')
    return {
        'schema_version': AWS_IMAGE_PIN_SCHEMA_VERSION,
        'provider': 'aws',
        'support_image': _validate_aws_image(value['support_image'], label='support_image'),
        'application_image': _validate_aws_image(
            value['application_image'], label='application_image'
        ),
    }


def _public_key_digest(value: str) -> str:
    parts = str(value or '').split()
    if len(parts) < 2 or parts[0] not in {
        'ssh-ed25519', 'ssh-rsa', 'ecdsa-sha2-nistp256'
    }:
        raise QualificationError('A canonical SSH public key is required.')
    return hashlib.sha256(f'{parts[0]} {parts[1]}'.encode()).hexdigest()


def _provider_pin_path(job: Mapping[str, Any]) -> Path:
    return application.RUNS / str(job['id']) / PROVIDER_PIN_FILENAME


def _validate_provider_pin(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get('schema_version') != PROVIDER_PIN_SCHEMA_VERSION:
        raise QualificationError('Saved provider pin has an unsupported schema.')
    provider = value.get('provider')
    if provider == 'aws':
        expected = {
            'schema_version', 'provider', 'profile', 'region',
            'availability_zone', 'application_shape', 'application_vcpus',
            'application_memory_gib', 'support_image', 'application_image',
            'ssh_public_key_sha256',
        }
        if set(value) != expected:
            raise QualificationError('Saved AWS provider pin is incomplete.')
        _validate_aws_image(value['support_image'], label='support_image')
        _validate_aws_image(value['application_image'], label='application_image')
    elif provider == 'oci':
        expected = {
            'schema_version', 'provider', 'profile', 'region',
            'compartment_id', 'availability_domain', 'application_shape',
            'application_architecture', 'application_vcpus',
            'application_memory_gib', 'application_image_id',
            'support_image_id', 'defined_tags', 'ssh_public_key_sha256',
        }
        if set(value) != expected:
            raise QualificationError('Saved OCI provider pin is incomplete.')
        if value.get('application_architecture') not in {'x86_64', 'arm64'}:
            raise QualificationError('Saved OCI architecture is invalid.')
        _validate_oci_defined_tags(value.get('defined_tags'))
        for key in ('compartment_id', 'application_image_id', 'support_image_id'):
            prefix = 'compartment' if key == 'compartment_id' else 'image'
            if not re.fullmatch(rf'ocid1\.{prefix}\.[A-Za-z0-9._-]+', str(value.get(key, ''))):
                raise QualificationError(f'Saved OCI {key} is invalid.')
    else:
        raise QualificationError('Saved provider pin does not select AWS or OCI.')
    for key in ('profile', 'region', 'availability_zone' if provider == 'aws' else 'availability_domain', 'application_shape'):
        if not isinstance(value.get(key), str) or not value[key].strip():
            raise QualificationError(f'Saved provider pin has an invalid {key}.')
    if type(value.get('application_vcpus')) is not int or value['application_vcpus'] <= 0:
        raise QualificationError('Saved provider pin has invalid application_vcpus.')
    if type(value.get('application_memory_gib')) not in (int, float) or value['application_memory_gib'] < 16:
        raise QualificationError('Saved provider pin has invalid application_memory_gib.')
    if not re.fullmatch(r'[0-9a-f]{64}', str(value.get('ssh_public_key_sha256', ''))):
        raise QualificationError('Saved provider pin has an invalid SSH key digest.')
    return dict(value)


def _write_provider_pin(job: Mapping[str, Any], pin: Mapping[str, Any]) -> None:
    validated = _validate_provider_pin(dict(pin))
    path = _provider_pin_path(job)
    if path.exists():
        raise QualificationError(f'Refusing to replace existing provider pin {path}.')
    try:
        with path.open('x', encoding='utf-8') as output:
            json.dump(validated, output, indent=2, sort_keys=True)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
    except OSError as exc:
        raise QualificationError(f'Unable to persist provider pin {path}: {exc}') from exc


def _read_provider_pin(job: Mapping[str, Any]) -> dict[str, Any]:
    path = _provider_pin_path(job)
    if not path.is_file():
        raise QualificationError(
            f'The saved provider pin is missing; restore {path} before recovery.'
        )
    return _validate_provider_pin(_read_json(path, label='saved provider pin'))


def _oci_config(
    args: argparse.Namespace,
    *,
    region: str | None = None,
    profile: str | None = None,
) -> dict[str, Any]:
    try:
        config = oci.sdk.config.from_file(
            file_location=str(args.oci_config_file or Path.home() / '.oci' / 'config'),
            profile_name=str(profile or args.oci_profile or 'DEFAULT'),
        )
    except Exception as exc:
        raise QualificationError(f'Unable to load the OCI SDK configuration: {exc}') from exc
    config = dict(config)
    if region:
        config['region'] = region
    try:
        oci.sdk.config.validate_config(config)
    except Exception as exc:
        raise QualificationError(f'OCI SDK configuration is invalid: {exc}') from exc
    return config


def _oci_clients(args: argparse.Namespace, pin: Mapping[str, Any]) -> dict[str, Any]:
    # Resume/cleanup authentication follows the immutable selector saved before
    # provisioning.  An omitted CLI option must never silently fall back to a
    # different local DEFAULT profile.
    config = _oci_config(
        args,
        region=str(pin['region']),
        profile=str(pin['profile']),
    )
    return {
        'compute': oci.sdk.core.ComputeClient(config),
        'network': oci.sdk.core.VirtualNetworkClient(config),
        'block': oci.sdk.core.BlockstorageClient(config),
    }


def _validate_load_options(args: argparse.Namespace) -> None:
    if args.threads > 0 and (
        args.connections < args.threads
        or args.request_rate < args.threads
        or args.connections % args.threads
        or args.request_rate % args.threads
    ):
        raise QualificationError(
            'Qualification connections and request rate must each be at least '
            'and evenly divisible by the wrk2 thread count.'
        )


def _new_provider_pin(
    args: argparse.Namespace,
    ssh: Mapping[str, str | None],
) -> dict[str, Any]:
    _validate_load_options(args)
    digest = _public_key_digest(str(ssh['public_key']))
    if args.provider == 'aws':
        region = str(args.region or 'us-east-1').strip()
        zone = str(args.availability_zone or ('us-east-1a' if region == 'us-east-1' else '')).strip()
        if not zone or not zone.startswith(region):
            raise QualificationError(
                'Pass an AWS --availability-zone belonging to the selected region.'
            )
        images = _read_aws_image_pin(args.aws_image_pin)
        return _validate_provider_pin({
            'schema_version': PROVIDER_PIN_SCHEMA_VERSION,
            'provider': 'aws',
            'profile': str(args.aws_profile or 'default'),
            'region': region,
            'availability_zone': zone,
            'application_shape': str(args.application_shape or 'm7i.2xlarge'),
            'application_vcpus': (
                8 if args.application_vcpus is None else args.application_vcpus
            ),
            'application_memory_gib': (
                32
                if args.application_memory_gib is None
                else args.application_memory_gib
            ),
            'support_image': images['support_image'],
            'application_image': images['application_image'],
            'ssh_public_key_sha256': digest,
        })

    profile = str(args.oci_profile or 'DEFAULT')
    config = _oci_config(args)
    region = str(args.region or config.get('region') or '').strip()
    values = {
        'schema_version': PROVIDER_PIN_SCHEMA_VERSION,
        'provider': 'oci',
        'profile': profile,
        'region': region,
        'compartment_id': str(args.oci_compartment_id or '').strip(),
        'availability_domain': str(args.availability_zone or '').strip(),
        'application_shape': str(args.application_shape or 'VM.Standard.E5.Flex'),
        'application_architecture': str(args.oci_application_architecture or 'x86_64'),
        'application_vcpus': (
            4 if args.application_vcpus is None else args.application_vcpus
        ),
        'application_memory_gib': (
            32
            if args.application_memory_gib is None
            else args.application_memory_gib
        ),
        'application_image_id': str(args.oci_application_image_id or '').strip(),
        'support_image_id': str(args.oci_support_image_id or '').strip(),
        'defined_tags': _read_oci_defined_tags(args.oci_defined_tags_json),
        'ssh_public_key_sha256': digest,
    }
    if not region or not values['availability_domain']:
        raise QualificationError('OCI region and --availability-zone are required.')
    return _validate_provider_pin(values)


def _plan(
    args: argparse.Namespace,
    ssh: Mapping[str, str | None],
    pin: Mapping[str, Any],
) -> BenchmarkPlan:
    common = {
        'provider': pin['provider'],
        'region': pin['region'],
        'availability_domain': (
            pin['availability_zone']
            if pin['provider'] == 'aws'
            else pin['availability_domain']
        ),
        'shape': pin['application_shape'],
        'ocpus': pin['application_vcpus'],
        'memory_gb': pin['application_memory_gib'],
        'ssh_private_key': str(ssh['private_key']),
        'ssh_public_key': str(ssh['public_key']),
        'ssh_key_passphrase': ssh['passphrase'],
        'security': {'mode': 'none'},
        'networking': 'paravirtualized',
        'storage': {'additional_volume': False},
        'deathstarbench': {
            'topology_id': DISTRIBUTED_TIERED_TOPOLOGY_ID,
            'runtime_id': K3S_RUNTIME_ID,
            'workload': 'social_network',
            'warmup_seconds': args.warmup_seconds,
            'duration_seconds': args.duration_seconds,
            'threads': args.threads,
            'connections': args.connections,
            'request_rate': args.request_rate,
        },
        'benchmarks': ['deathstarbench'],
        'destroy_after_completion': True,
    }
    if pin['provider'] == 'aws':
        common['aws_profile'] = pin['profile']
    else:
        common['compartment_id'] = pin['compartment_id']
    return BenchmarkPlan(**common)


def _validate_saved_plan(job: Mapping[str, Any], pin: Mapping[str, Any]) -> None:
    plan = job.get('plan')
    options = plan.get('deathstarbench') if isinstance(plan, dict) else None
    zone_key = 'availability_zone' if pin['provider'] == 'aws' else 'availability_domain'
    expected = {
        'provider': pin['provider'],
        'region': pin['region'],
        'availability_domain': pin[zone_key],
        'shape': pin['application_shape'],
        'ocpus': pin['application_vcpus'],
        'memory_gb': pin['application_memory_gib'],
    }
    if (
        not isinstance(plan, dict)
        or any(plan.get(key) != value for key, value in expected.items())
        or not isinstance(options, dict)
        or options.get('topology_id') != DISTRIBUTED_TIERED_TOPOLOGY_ID
        or options.get('runtime_id') != K3S_RUNTIME_ID
        or options.get('workload') != 'social_network'
        or plan.get('benchmarks') != ['deathstarbench']
    ):
        raise QualificationError('Saved plan differs from its immutable provider pin.')
    if pin['provider'] == 'aws' and plan.get('aws_profile') != pin['profile']:
        raise QualificationError('Saved AWS profile differs from its provider pin.')
    if pin['provider'] == 'oci' and plan.get('compartment_id') != pin['compartment_id']:
        raise QualificationError('Saved OCI compartment differs from its provider pin.')


def _assert_resume_overrides(args: argparse.Namespace, pin: Mapping[str, Any]) -> None:
    common = {
        'region': args.region,
        'availability_zone' if pin['provider'] == 'aws' else 'availability_domain': args.availability_zone,
        'application_shape': args.application_shape,
        'application_vcpus': args.application_vcpus,
        'application_memory_gib': args.application_memory_gib,
    }
    for key, requested in common.items():
        if requested is not None and requested != pin[key]:
            raise QualificationError(f'Resume override for {key} differs from the saved provider pin.')
    if pin['provider'] == 'aws':
        if args.aws_profile is not None and args.aws_profile != pin['profile']:
            raise QualificationError('Resume AWS profile differs from the saved provider pin.')
        if args.aws_image_pin is not None:
            requested = _read_aws_image_pin(args.aws_image_pin)
            for key in ('support_image', 'application_image'):
                if requested[key] != pin[key]:
                    raise QualificationError('Resume AWS image pin differs from the saved provider pin.')
    else:
        checks = {
            'profile': args.oci_profile,
            'compartment_id': args.oci_compartment_id,
            'application_architecture': args.oci_application_architecture,
            'application_image_id': args.oci_application_image_id,
            'support_image_id': args.oci_support_image_id,
        }
        for key, requested in checks.items():
            if requested is not None and requested != pin[key]:
                raise QualificationError(f'Resume OCI {key} differs from the saved provider pin.')
        if (args.oci_defined_tags_json is not None
                and _read_oci_defined_tags(args.oci_defined_tags_json) != pin['defined_tags']):
            raise QualificationError('Resume OCI defined_tags differs from the saved provider pin.')


def _validate_candidate_job(job: Mapping[str, Any], *, provider: str) -> None:
    pin = _read_provider_pin(job)
    if pin['provider'] != provider:
        raise QualificationError(
            f'Saved job {job.get("id")} belongs to {pin["provider"].upper()}, not {provider.upper()}.'
        )
    _validate_saved_plan(job, pin)
    resources = job.get('resources')
    if resources:
        if not isinstance(resources, dict):
            raise QualificationError('Saved candidate resources are malformed.')
        if provider == 'aws':
            if resources.get('provider') != 'aws' or resources.get('aws_distributed_candidate') is not True:
                raise QualificationError('Saved resources lack exact AWS candidate markers.')
        elif oci.CONTRACT_KEY not in resources:
            raise QualificationError('Saved resources lack the exact OCI candidate contract.')


def _load_candidate_job(job_id: str, *, provider: str) -> dict[str, Any]:
    if not application.RUN_ID_PATTERN.fullmatch(job_id):
        raise QualificationError('Qualification job IDs must be 12 lowercase hex digits.')
    job = application.load_persisted_job(job_id)
    if job is None:
        raise QualificationError(f'No persisted qualification job {job_id!r} exists.')
    _validate_candidate_job(job, provider=provider)
    return job


def _oci_inputs(pin: Mapping[str, Any], ssh: Mapping[str, str | None]) -> dict[str, Any]:
    if _public_key_digest(str(ssh['public_key'])) != pin['ssh_public_key_sha256']:
        raise QualificationError('SSH public key differs from the immutable candidate pin.')
    return {
        'compartment_id': pin['compartment_id'],
        'availability_domain': pin['availability_domain'],
        'region': pin['region'],
        'shape': pin['application_shape'],
        'architecture': pin['application_architecture'],
        'ocpus': pin['application_vcpus'],
        'memory_gb': pin['application_memory_gib'],
        'application_image_id': pin['application_image_id'],
        'support_image_id': pin['support_image_id'],
        'defined_tags': copy.deepcopy(pin['defined_tags']),
        'public_key': str(ssh['public_key']),
    }


def _qualify_and_persist_network_paths(
    job: dict[str, Any], image_lock: Mapping[str, Any]
) -> dict[str, Any]:
    attestation = qualify_distributed_network_paths(
        job,
        image_lock,
        execute=application.ssh,
        emit=shared._event,
    )
    if not isinstance(attestation, dict):
        raise QualificationError('Distributed network qualification returned malformed evidence.')
    resources = job.get('resources')
    if not isinstance(resources, dict):
        raise QualificationError('Distributed network qualification lost resource state.')
    previous = resources.get(DISTRIBUTED_NETWORK_QUALIFICATION_KEY)
    if previous is not None:
        if previous != attestation:
            raise QualificationError('Persisted network qualification evidence drifted.')
        return attestation
    resources[DISTRIBUTED_NETWORK_QUALIFICATION_KEY] = attestation
    shared._persist(job)
    return attestation


def _cleanup(
    job: dict[str, Any],
    *,
    provider: str,
    args: argparse.Namespace,
    pin: Mapping[str, Any] | None = None,
    clients: Mapping[str, Any] | None = None,
) -> bool:
    if not job.get('resources'):
        shared._set_status(job, 'destroyed')
        return True
    pin = dict(pin) if pin is not None else None
    shared._set_status(job, 'destroying')
    shared._event(job, 'Destroy', f'Removing live {provider.upper()} qualification resources.')
    try:
        if provider == 'aws':
            aws.destroy_distributed_deathstarbench_candidate(
                job,
                emit=shared._event,
                persist=shared._persist,
            )
            # A second read-only reconciliation is the independent absence gate.
            aws.recover_distributed_deathstarbench_candidate(
                job,
                persist=shared._persist,
                wait_for_attachments=False,
            )
            complete = aws.distributed_deathstarbench_candidate_deleted(job)
        else:
            pin = pin or _read_provider_pin(job)
            clients = dict(clients or _oci_clients(args, pin))
            oci.destroy_distributed_deathstarbench_candidate(
                job,
                clients=clients,
                persist=shared._persist,
                emit=shared._event,
            )
            # Tombstones are retained for recovery; explicitly re-read every
            # identity instead of relying on validation that skips deleted
            # entries.
            oci.attest_distributed_candidate_terminal_deletion(
                job,
                clients=clients,
            )
            complete = oci.distributed_candidate_is_deleted(job)
            if complete:
                shared._set_status(job, 'destroyed')
        if not complete:
            raise QualificationError(
                f'{provider.upper()} cleanup lacks strict terminal deletion evidence.'
            )
    except application.RunCancelled:
        raise
    except Exception as exc:
        job['cleanup_error'] = f'{type(exc).__name__}: {exc}'
        shared._set_status(job, 'cleanup_failed')
        print(f'Cleanup did not finish for job {job["id"]}: {job["cleanup_error"]}', file=sys.stderr, flush=True)
        return False
    job.pop('cleanup_error', None)
    shared._persist(job)
    print(f'{provider.upper()} cleanup proved complete for job {job["id"]}.', flush=True)
    return True


def _cleanup_only(args: argparse.Namespace) -> int:
    job_id = str(args.cleanup_only)
    if not application.RUN_ID_PATTERN.fullmatch(job_id):
        raise QualificationError(
            'Qualification job IDs must be 12 lowercase hex digits.'
        )
    with shared._exclusive_job_lock(job_id):
        job = _load_candidate_job(job_id, provider=args.provider)
        pin = _read_provider_pin(job)
        _assert_resume_overrides(args, pin)
        clients = _oci_clients(args, pin) if args.provider == 'oci' else None
        complete = False
        evidence_error: BaseException | None = None
        with shared._cooperative_signal_handlers(job) as signal_state:
            try:
                if shared._read_interruption_evidence(job['id']) is not None:
                    shared._update_interruption_evidence(job, recovery_outcome='cleanup_only_started')
                complete = _cleanup(
                    job,
                    provider=args.provider,
                    args=args,
                    pin=pin,
                    clients=clients,
                )
            except application.RunCancelled:
                job['cleanup_error'] = (
                    f'{args.provider.upper()} cleanup-only was interrupted; '
                    'recoverable ownership remains for another attempt.'
                )
                shared._set_status(job, 'cleanup_failed')
            finally:
                signal_state['raise_cancel'] = False
                signum = signal_state['number']
                if signum is not None:
                    shared._mark_interrupted_unless_benchmark_is_terminal(job)
                    try:
                        shared._record_signal_interruption(job, signum)
                        signal_state['evidence_recorded'] = True
                    except BaseException as exc:
                        evidence_error = exc
                    shared._persist(job)
                try:
                    shared._finalize_interruption_evidence_signal_aware(
                        job,
                        signal_state,
                        cleanup_outcome='completed' if complete else 'failed',
                        recovery_outcome='cleanup_completed' if complete else 'cleanup_failed',
                    )
                except BaseException as exc:
                    evidence_error = evidence_error or exc
        if not complete or evidence_error is not None:
            return 2
        signum = signal_state['number']
        return 128 + signum if signum is not None else 0


def _run_qualification(
    job: dict[str, Any],
    setup: Callable[[], tuple[BenchmarkPlan, Mapping[str, str | None], Mapping[str, Any], Mapping[str, Any], Mapping[str, Any] | None]],
    *,
    provider: str,
    args: argparse.Namespace,
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
    pin: Mapping[str, Any] | None = None
    clients: Mapping[str, Any] | None = None
    with shared._cooperative_signal_handlers(job) as signal_state:
        try:
            application.raise_if_cancelled(job)
            evidence = shared._read_interruption_evidence(str(job['id']))
            if recovery_attempt and evidence is not None:
                shared._update_interruption_evidence(job, recovery_outcome='resume_started')
            plan, ssh, image_lock, pin, clients = setup()
            setup_complete = True
            if (
                not recovery_attempt
                and _public_key_digest(str(ssh['public_key']))
                != pin['ssh_public_key_sha256']
            ):
                raise QualificationError('SSH public key differs from the immutable candidate pin.')
            if measure:
                job['_persist_results_artifact'] = True
            shared._set_status(job, 'provisioning')
            application.raise_if_cancelled(job)
            if provider == 'aws':
                aws.provision_distributed_deathstarbench_candidate(
                    job,
                    plan,
                    support_image=pin['support_image'],
                    application_image=pin['application_image'],
                    public_key=str(ssh['public_key']),
                    emit=shared._event,
                    persist=shared._persist,
                )
                aws.validate_distributed_deathstarbench_candidate(
                    job,
                    persist=shared._persist,
                )
            else:
                if clients is None:
                    raise QualificationError('OCI live clients are missing.')
                oci.provision_distributed_deathstarbench_candidate(
                    job,
                    inputs=_oci_inputs(pin, ssh),
                    clients=clients,
                    persist=shared._persist,
                    emit=shared._event,
                )
                oci.publish_distributed_runtime_projection(
                    job,
                    clients=clients,
                    persist=shared._persist,
                )
            # No SSH operation may occur before the validated aliases above exist.
            shared._set_status(job, 'testing')
            application.raise_if_cancelled(job)
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
            resources = job['resources']
            runtime = resources.get(K3S_RUNTIME_JOURNAL_KEY) or {}
            workload = resources.get(DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY) or {}
            if runtime.get('state') != 'cluster_ready' or workload.get('state') != 'workload_ready':
                raise QualificationError('Candidate did not reach both exact readiness boundaries.')
            application.raise_if_cancelled(job)
            _qualify_and_persist_network_paths(job, image_lock)
            if measure:
                kwargs: dict[str, Any] = {}
                if interrupt_at is not None:
                    kwargs['qualification_checkpoint'] = shared._qualification_checkpoint_callback(
                        interrupt_at=interrupt_at,
                        interrupt_mode=str(interrupt_mode),
                    )
                if inject_initializer_response_loss:
                    kwargs['qualification_inject_initializer_response_loss'] = True
                result = run_distributed_social_network_measurement(
                    job,
                    image_lock,
                    plan.deathstarbench,
                    execute=application.ssh,
                    emit=shared._event,
                    persist=shared._persist,
                    **kwargs,
                )
                shared._require_qualified_measurement_result(job, plan, result)
                shared._write_and_require_report(job)
            qualified = True
            shared._event(job, 'Qualified', f'{provider.upper()} distributed candidate passed qualification.')
        except BaseException as exc:
            failure = exc
            job['error'] = f'{type(exc).__name__}: {exc}'
            if isinstance(exc, (KeyboardInterrupt, application.RunCancelled)):
                shared._mark_interrupted_unless_benchmark_is_terminal(job)
            shared._set_status(job, 'cleanup_pending')
            print(f'Qualification failed: {job["error"]}', file=sys.stderr, flush=True)
        finally:
            signum = signal_state['number']
            if signum is not None:
                shared._mark_interrupted_unless_benchmark_is_terminal(job)
                if failure is None:
                    failure = application.RunCancelled(
                        f'Qualification received {signal.Signals(signum).name}.'
                    )
                try:
                    shared._record_signal_interruption(job, signum)
                    signal_state['evidence_recorded'] = True
                except BaseException as exc:
                    failure = failure or exc
            try:
                cleanup_complete = _cleanup(
                    job,
                    provider=provider,
                    args=args,
                    pin=pin,
                    clients=clients,
                )
            except application.RunCancelled as exc:
                failure = failure or exc
                job['cleanup_error'] = (
                    f'{provider.upper()} cleanup was interrupted; recoverable '
                    'ownership remains for --cleanup-only.'
                )
                shared._set_status(job, 'cleanup_failed')
            signal_state['raise_cancel'] = False
            try:
                outcome = None
                if recovery_attempt:
                    outcome = 'resume_completed' if qualified else (
                        'resume_failed' if setup_complete else 'resume_refused'
                    )
                shared._finalize_interruption_evidence_signal_aware(
                    job,
                    signal_state,
                    cleanup_outcome='completed' if cleanup_complete else 'failed',
                    recovery_outcome=outcome,
                )
            except BaseException as exc:
                failure = failure or exc

    if not cleanup_complete:
        return 2
    signum = signal_state['number']
    if signum is not None:
        return 128 + signum
    if failure is not None or not qualified:
        return 130 if isinstance(failure, (KeyboardInterrupt, application.RunCancelled)) else 1
    print(
        f'Qualification succeeded and all {provider.upper()} resources were destroyed '
        f'(job {job["id"]}).',
        flush=True,
    )
    return 0


def _require_resumable(job: Mapping[str, Any], *, provider: str, measure: bool) -> None:
    evidence = shared._read_interruption_evidence(str(job['id']))
    if evidence is not None:
        if evidence['subsequent_signal'] is None:
            evidence_state = evidence['execution_state']
            replay_decision = evidence['replay_decision']
        else:
            evidence_state = evidence['subsequent_signal_execution_state']
            replay_decision = evidence['subsequent_signal_replay_decision']
        current_state = shared._execution_state(job)
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
    shared._require_resumable(job, provider_name=provider.upper(), measure=measure)


def _qualify(args: argparse.Namespace) -> int:
    interrupt_mode = shared._effective_interrupt_mode(args)
    if args.resume:
        job_id = str(args.resume)
        if not application.RUN_ID_PATTERN.fullmatch(job_id):
            raise QualificationError(
                'Qualification job IDs must be 12 lowercase hex digits.'
            )
        print(f'{args.provider.upper()} qualification job: {job_id}', flush=True)
        with shared._exclusive_job_lock(job_id):
            try:
                job = _load_candidate_job(job_id, provider=args.provider)

                # Every resume check is a preflight.  Complete it before
                # entering the orchestration runner, whose finally block is
                # intentionally cleanup-owning after cloud work begins.  A
                # refused resume must leave the graph untouched for the
                # operator's explicit --cleanup-only decision.
                _require_resumable(
                    job,
                    provider=args.provider,
                    measure=args.measure,
                )
                if (
                    args.interrupt_at is not None
                    and shared._read_interruption_evidence(job_id) is not None
                ):
                    raise QualificationError(
                        'A recovery with retained interruption evidence cannot '
                        'inject another checkpoint.'
                    )
                shared._require_resume_workload_settings(job, args)
                pin = _read_provider_pin(job)
                _assert_resume_overrides(args, pin)
                ssh = shared._ssh_material(args.ssh_env_file)
                if (
                    _public_key_digest(str(ssh['public_key']))
                    != pin['ssh_public_key_sha256']
                ):
                    raise QualificationError(
                        'Resume SSH key differs from the saved provider pin.'
                    )
                _job, plan = shared._resume_job(job, ssh)
                _validate_saved_plan(job, pin)
                image_lock = shared._lock_for_run(args.image_lock, job)
                shared._preflight_anonymous_ghcr_images(image_lock)
                clients = (
                    _oci_clients(args, pin)
                    if args.provider == 'oci'
                    else None
                )
            except QualificationError:
                # The shared updater strictly reloads canonical evidence and is
                # a no-op when no artifact exists.  Preserve every origin and
                # cleanup field; only finalize this recovery attempt.
                shared._update_interruption_evidence(
                    {'id': job_id},
                    recovery_outcome='resume_refused',
                )
                raise

            def resume_setup():
                return plan, ssh, image_lock, pin, clients

            return _run_qualification(
                job,
                resume_setup,
                provider=args.provider,
                args=args,
                measure=args.measure,
                interrupt_at=args.interrupt_at,
                interrupt_mode=interrupt_mode,
                inject_initializer_response_loss=args.inject_initializer_response_loss,
                recovery_attempt=True,
            )

    ssh = shared._ssh_material(args.ssh_env_file)
    image_lock = shared._read_image_lock(args.image_lock)
    shared._preflight_anonymous_ghcr_images(image_lock)
    pin = _new_provider_pin(args, ssh)
    plan = _plan(args, ssh, pin)
    job, lease = shared._new_job(plan, ssh, image_lock)
    try:
        _write_provider_pin(job, pin)
    except BaseException:
        lease.release()
        raise
    print(f'{args.provider.upper()} qualification job: {job["id"]}', flush=True)

    def new_setup():
        clients = _oci_clients(args, pin) if args.provider == 'oci' else None
        return plan, ssh, image_lock, pin, clients

    with lease:
        return _run_qualification(
            job,
            new_setup,
            provider=args.provider,
            args=args,
            measure=args.measure,
            interrupt_at=args.interrupt_at,
            interrupt_mode=interrupt_mode,
            inject_initializer_response_loss=args.inject_initializer_response_loss,
        )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        shared._validate_interruption_options(args)
        if args.provider == 'oci' and any(value is not None for value in (
            args.aws_profile,
            args.aws_image_pin,
        )):
            raise QualificationError('AWS candidate options are only valid for AWS.')
        if args.provider == 'aws' and any(value is not None for value in (
            args.oci_config_file,
            args.oci_profile,
            args.oci_compartment_id,
            args.oci_application_architecture,
            args.oci_application_image_id,
            args.oci_support_image_id,
            args.oci_defined_tags_json,
        )):
            raise QualificationError('OCI candidate options are only valid for OCI.')
        if args.cleanup_only:
            return _cleanup_only(args)
        if not args.resume and args.image_lock is None:
            raise QualificationError('--image-lock is required for a new run.')
        return _qualify(args)
    except QualificationError as exc:
        print(f'Qualification refused: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
