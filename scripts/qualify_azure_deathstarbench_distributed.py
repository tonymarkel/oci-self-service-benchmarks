#!/usr/bin/env python3
"""Run the unreleased distributed DeathStarBench Azure qualification safely.

This is an operator-only harness.  It persists the complete ownership
contract before Azure writes, deploys the candidate topology/runtime, and by
default stops at the attested ``workload_ready`` boundary.  ``--measure``
additionally runs the one-shot dataset and load qualification, writes its
report, and only then attempts exact resource-group cleanup.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import sys
import threading
import uuid
from typing import Any, Callable, Iterator, Mapping, NoReturn
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import (
    HTTPRedirectHandler,
    Request,
    build_opener,
    parse_http_list,
    parse_keqv_list,
)

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
    prepare_azure_distributed_k3s_candidate,
    prepare_azure_distributed_social_network_candidate,
)
from app.deathstarbench_k3s_workload import validate_image_lock
from app.models import BenchmarkPlan
from app.providers import azure
from app.resource_inventory import ROLE_NODE_INVENTORY_KEY
from app.run_lease import (
    LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME,
    RunLease,
    RunLeaseError,
    RunLeaseHeldError,
    acquire_run_lease,
)


LOCK_FILENAME = 'deathstarbench-social-image-lock.json'
# Retain this name for callers/tests that used the harness-specific constant,
# while the process-wide run lease holds it as a migration guard for older
# qualifier processes.
QUALIFICATION_LOCK_FILENAME = LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME
INTERRUPTION_EVIDENCE_FILENAME = 'qualification-interruption.json'
INTERRUPTION_EVIDENCE_SCHEMA_VERSION = 1
MAX_INTERRUPTION_EVIDENCE_BYTES = 1024
HARD_INTERRUPTION_EXIT_CODE = 86
INTERRUPTION_CHECKPOINTS = (
    'load_generator_ready',
    'initialization_started',
    'warmup_started',
    'measurement_started',
)
INTERRUPTION_MODES = ('graceful', 'hard')
GHCR_HOST = 'ghcr.io'
GHCR_RESPONSE_LIMIT_BYTES = 2 * 1024 * 1024
GHCR_TIMEOUT_SECONDS = 30
GHCR_IMAGE_KEYS = frozenset({
    'media-frontend',
    'nginx-thrift',
    'social-network-microservices',
})
GHCR_MANIFEST_ACCEPT = ', '.join((
    'application/vnd.oci.image.manifest.v1+json',
    'application/vnd.docker.distribution.manifest.v2+json',
    'application/vnd.oci.image.index.v1+json',
    'application/vnd.docker.distribution.manifest.list.v2+json',
))
RESUME_TEARDOWN_STATUSES = frozenset({
    'cleanup_pending',
    'destroying',
    'cleanup_failed',
    'destroyed',
})
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
WORKLOAD_OPTION_DEFAULTS = {
    'warmup_seconds': 30,
    'duration_seconds': 60,
    'threads': 4,
    'connections': 64,
    'request_rate': 100,
}
SAFE_MEASUREMENT_RESUME_STATES = frozenset({
    'preparing_load_generator',
    'load_generator_ready',
})
MEASUREMENT_JOURNAL_STATES = frozenset({
    *SAFE_MEASUREMENT_RESUME_STATES,
    'initialization_started',
    'dataset_ready',
    'warmup_started',
    'warmup_complete',
    'measurement_started',
    'measurement_complete',
})


class QualificationError(RuntimeError):
    """Raised when live qualification cannot prove its required boundary."""


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


def _read_image_lock(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise QualificationError(f'Unable to read image lock {path}: {exc}') from exc
    if not isinstance(value, dict):
        raise QualificationError('The candidate image lock must be a JSON object.')
    try:
        validate_image_lock(value)
    except ValueError as exc:
        raise QualificationError(f'The candidate image lock is invalid: {exc}') from exc
    return value


class _RejectRegistryRedirects(HTTPRedirectHandler):
    """Keep the anonymous registry exchange on the explicitly audited host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: N802
        return None


def _anonymous_registry_open(request: Request, timeout: int):
    return build_opener(_RejectRegistryRedirects()).open(request, timeout=timeout)


def _require_ghcr_https_url(value: str, *, label: str) -> None:
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as exc:
        raise QualificationError(f'{label} has an invalid port.') from exc
    if (
        parsed.scheme != 'https'
        or parsed.hostname != GHCR_HOST
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.fragment
    ):
        raise QualificationError(
            f'{label} must remain on anonymous HTTPS {GHCR_HOST}.'
        )


def _read_bounded_response(response: Any, *, label: str) -> bytes:
    try:
        status = getattr(response, 'status', None)
        if status is None:
            status = response.getcode()
        if status != 200:
            raise QualificationError(f'{label} returned HTTP {status}.')
        final_url = response.geturl()
        _require_ghcr_https_url(final_url, label=f'{label} response URL')
        body = response.read(GHCR_RESPONSE_LIMIT_BYTES + 1)
    finally:
        response.close()
    if len(body) > GHCR_RESPONSE_LIMIT_BYTES:
        raise QualificationError(
            f'{label} exceeded the {GHCR_RESPONSE_LIMIT_BYTES}-byte safety limit.'
        )
    return body


def _request_bounded(
    request: Request,
    *,
    label: str,
    open_request: Callable[[Request, int], Any],
) -> tuple[bytes, Mapping[str, str]]:
    _require_ghcr_https_url(request.full_url, label=f'{label} request URL')
    try:
        response = open_request(request, GHCR_TIMEOUT_SECONDS)
    except (HTTPError, URLError, OSError) as exc:
        raise QualificationError(f'{label} failed: {exc}') from exc
    headers = response.headers
    return _read_bounded_response(response, label=label), headers


def _bearer_token_url(challenge: str | None, *, repository: str) -> str:
    scheme, separator, parameters = str(challenge or '').partition(' ')
    if not separator or scheme.casefold() != 'bearer':
        raise QualificationError(
            'GHCR did not offer the required anonymous Bearer challenge.'
        )
    try:
        values = {
            str(key).casefold(): value
            for key, value in parse_keqv_list(parse_http_list(parameters)).items()
        }
    except (TypeError, ValueError) as exc:
        raise QualificationError('GHCR returned an invalid Bearer challenge.') from exc
    realm = values.get('realm')
    if not isinstance(realm, str):
        raise QualificationError('GHCR Bearer challenge omitted its token realm.')
    _require_ghcr_https_url(realm, label='GHCR token realm')
    service = values.get('service')
    if service not in (None, GHCR_HOST):
        raise QualificationError('GHCR Bearer challenge named an unexpected service.')
    scope = values.get('scope') or f'repository:{repository}:pull'
    if scope != f'repository:{repository}:pull':
        raise QualificationError('GHCR Bearer challenge named an unexpected scope.')
    parsed = urlsplit(realm)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query['service'] = GHCR_HOST
    query['scope'] = scope
    return urlunsplit((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        urlencode(query),
        '',
    ))


def _anonymous_bearer_token(
    challenge: str | None,
    *,
    repository: str,
    open_request: Callable[[Request, int], Any],
) -> str:
    token_url = _bearer_token_url(challenge, repository=repository)
    body, _headers = _request_bounded(
        Request(token_url, headers={'Accept': 'application/json'}),
        label=f'GHCR token request for {repository}',
        open_request=open_request,
    )
    try:
        document = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QualificationError('GHCR token response was not valid JSON.') from exc
    if not isinstance(document, dict):
        raise QualificationError('GHCR token response was not a JSON object.')
    token = document.get('token') or document.get('access_token')
    if not isinstance(token, str) or not token or any(
        character.isspace() for character in token
    ):
        raise QualificationError('GHCR did not return a valid anonymous token.')
    return token


def _preflight_one_ghcr_manifest(
    reference: str,
    *,
    open_request: Callable[[Request, int], Any],
) -> None:
    repository_with_host, expected_digest = reference.rsplit('@', 1)
    repository = repository_with_host.removeprefix(f'{GHCR_HOST}/')
    manifest_url = (
        f'https://{GHCR_HOST}/v2/{repository}/manifests/{expected_digest}'
    )
    request = Request(manifest_url, headers={'Accept': GHCR_MANIFEST_ACCEPT})
    try:
        response = open_request(request, GHCR_TIMEOUT_SECONDS)
    except HTTPError as exc:
        if exc.code != 401:
            raise QualificationError(
                f'Anonymous GHCR manifest request for {reference} failed: {exc}'
            ) from exc
        challenge = exc.headers.get('WWW-Authenticate')
        exc.close()
    except (URLError, OSError) as exc:
        raise QualificationError(
            f'Anonymous GHCR manifest request for {reference} failed: {exc}'
        ) from exc
    else:
        # Public GHCR currently still uses a token challenge, but accepting a
        # direct anonymous 200 keeps the proof valid if that behavior changes.
        headers = response.headers
        body = _read_bounded_response(
            response,
            label=f'Anonymous GHCR manifest request for {reference}',
        )
        returned_digest = headers.get('Docker-Content-Digest')
        if returned_digest is not None and returned_digest != expected_digest:
            raise QualificationError(
                f'GHCR returned the wrong response digest for {reference}.'
            )
        if f'sha256:{hashlib.sha256(body).hexdigest()}' != expected_digest:
            raise QualificationError(
                f'GHCR returned bytes that do not match {reference}.'
            )
        return

    token = _anonymous_bearer_token(
        challenge,
        repository=repository,
        open_request=open_request,
    )
    body, headers = _request_bounded(
        Request(
            manifest_url,
            headers={
                'Accept': GHCR_MANIFEST_ACCEPT,
                'Authorization': f'Bearer {token}',
            },
        ),
        label=f'Authenticated anonymous GHCR manifest request for {reference}',
        open_request=open_request,
    )
    returned_digest = headers.get('Docker-Content-Digest')
    if returned_digest is not None and returned_digest != expected_digest:
        raise QualificationError(
            f'GHCR returned the wrong response digest for {reference}.'
        )
    if f'sha256:{hashlib.sha256(body).hexdigest()}' != expected_digest:
        raise QualificationError(f'GHCR returned bytes that do not match {reference}.')


def _preflight_anonymous_ghcr_images(
    image_lock: Mapping[str, Any],
    *,
    open_request: Callable[[Request, int], Any] = _anonymous_registry_open,
) -> None:
    """Prove every custom platform manifest is anonymously retrievable."""

    try:
        validated = validate_image_lock(image_lock)
    except ValueError as exc:
        raise QualificationError(f'The candidate image lock is invalid: {exc}') from exc
    references: set[str] = set()
    for platform, images in validated.platforms.items():
        for image_key in GHCR_IMAGE_KEYS:
            reference = images[image_key]
            if not reference.startswith(f'{GHCR_HOST}/'):
                raise QualificationError(
                    f'Candidate image {image_key} for {platform} is not hosted '
                    f'on {GHCR_HOST}.'
                )
            references.add(reference)
    for reference in sorted(references):
        _preflight_one_ghcr_manifest(reference, open_request=open_request)


def _ssh_material() -> dict[str, str | None]:
    defaults = application.load_ssh_defaults()
    if defaults.get('configured') is not True:
        raise QualificationError(
            str(defaults.get('error') or 'Benchmark SSH keys are not configured.')
        )
    private_key = application.normalize_private_key(defaults['private_key'])
    public_key = application.normalize_public_key(defaults['public_key'])
    passphrase = (
        os.getenv('BENCHMARK_SSH_KEY_PASSPHRASE')
        or os.getenv('OCI_BENCHMARK_SSH_KEY_PASSPHRASE')
        or None
    )
    derived = application.derive_public_key(private_key, passphrase)
    if derived != public_key:
        raise QualificationError(
            'The configured benchmark SSH public key does not match its private key.'
        )
    return {
        'private_key': private_key,
        'public_key': public_key,
        'passphrase': passphrase,
    }


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


def _event(job: dict[str, Any], stage: str, message: str):
    application.event(job, stage, message)
    print(f'{application.now()}  {stage}: {message}', flush=True)


def _persist(job: dict[str, Any]):
    application.persist_job_state(job)


def _set_status(job: dict[str, Any], status: str):
    job['status'] = status
    job['updated_at'] = application.now()
    _persist(job)


def _validate_interruption_options(args: argparse.Namespace) -> None:
    interrupt_at = getattr(args, 'interrupt_at', None)
    interrupt_mode = getattr(args, 'interrupt_mode', None)
    if interrupt_at is not None and interrupt_at not in INTERRUPTION_CHECKPOINTS:
        raise QualificationError('The interruption checkpoint is invalid.')
    if interrupt_mode is not None and interrupt_mode not in INTERRUPTION_MODES:
        raise QualificationError('The interruption mode is invalid.')
    if getattr(args, 'cleanup_only', None) and (
        interrupt_at is not None or interrupt_mode is not None
    ):
        raise QualificationError(
            'Interruption injection cannot be used with --cleanup-only.'
        )
    if interrupt_at is not None and not getattr(args, 'measure', False):
        raise QualificationError('--interrupt-at requires --measure.')
    if interrupt_mode is not None and (
        not getattr(args, 'measure', False) or interrupt_at is None
    ):
        raise QualificationError(
            '--interrupt-mode requires --measure and --interrupt-at.'
        )
    if (
        interrupt_at == 'warmup_started'
        and getattr(args, 'warmup_seconds', None) == 0
    ):
        raise QualificationError(
            '--interrupt-at warmup_started requires a non-zero warm-up.'
        )


def _effective_interrupt_mode(args: argparse.Namespace) -> str | None:
    if getattr(args, 'interrupt_at', None) is None:
        return None
    return str(getattr(args, 'interrupt_mode', None) or 'graceful')


def _execution_state(job: Mapping[str, Any]) -> str | None:
    resources = job.get('resources')
    journal = (
        resources.get(DEATHSTARBENCH_EXECUTION_JOURNAL_KEY)
        if isinstance(resources, Mapping)
        else None
    )
    state = journal.get('state') if isinstance(journal, Mapping) else None
    return state if isinstance(state, str) and state else None


def _replay_decision(execution_state: str | None) -> str:
    if execution_state is None or execution_state in SAFE_MEASUREMENT_RESUME_STATES:
        return 'resume_allowed'
    return 'cleanup_only_required'


def _evidence_path(job_id: str) -> Path:
    if not application.RUN_ID_PATTERN.fullmatch(job_id):
        raise QualificationError(
            'Qualification job IDs must be 12 lowercase hex digits.'
        )
    return application.RUNS / job_id / INTERRUPTION_EVIDENCE_FILENAME


def _validated_interruption_evidence(
    value: Mapping[str, Any] | object,
    *,
    job_id: str,
) -> dict[str, Any]:
    fields = {
        'schema_version',
        'job_id',
        'source',
        'mode',
        'checkpoint',
        'signal',
        'execution_state',
        'replay_decision',
        'requested_at',
        'subsequent_signal',
        'subsequent_signal_at',
        'subsequent_signal_execution_state',
        'subsequent_signal_replay_decision',
        'cleanup_outcome',
        'recovery_outcome',
    }
    if not isinstance(value, Mapping) or set(value) != fields:
        raise QualificationError(
            'The qualification interruption evidence schema is invalid.'
        )
    evidence = dict(value)
    schema_version = evidence['schema_version']
    source = evidence['source']
    mode = evidence['mode']
    checkpoint = evidence['checkpoint']
    signal_name = evidence['signal']
    execution_state = evidence['execution_state']
    replay_decision = evidence['replay_decision']
    requested_at = evidence['requested_at']
    subsequent_signal = evidence['subsequent_signal']
    subsequent_signal_at = evidence['subsequent_signal_at']
    subsequent_execution_state = evidence[
        'subsequent_signal_execution_state'
    ]
    subsequent_replay_decision = evidence[
        'subsequent_signal_replay_decision'
    ]
    cleanup_outcome = evidence['cleanup_outcome']
    recovery_outcome = evidence['recovery_outcome']
    try:
        parsed_requested_at = datetime.fromisoformat(requested_at)
    except (TypeError, ValueError):
        parsed_requested_at = None
    try:
        parsed_subsequent_signal_at = (
            datetime.fromisoformat(subsequent_signal_at)
            if subsequent_signal_at is not None
            else None
        )
    except (TypeError, ValueError):
        parsed_subsequent_signal_at = None
    if (
        type(schema_version) is not int
        or schema_version != INTERRUPTION_EVIDENCE_SCHEMA_VERSION
        or not isinstance(evidence['job_id'], str)
        or evidence['job_id'] != job_id
        or not isinstance(source, str)
        or source not in {'checkpoint', 'signal'}
        or not isinstance(mode, str)
        or mode not in INTERRUPTION_MODES
        or (
            checkpoint is not None
            and (
                not isinstance(checkpoint, str)
                or checkpoint not in INTERRUPTION_CHECKPOINTS
            )
        )
        or (
            signal_name is not None
            and (
                not isinstance(signal_name, str)
                or signal_name not in {'SIGINT', 'SIGTERM'}
            )
        )
        or (
            execution_state is not None
            and (
                not isinstance(execution_state, str)
                or execution_state not in MEASUREMENT_JOURNAL_STATES
            )
        )
        or not isinstance(replay_decision, str)
        or replay_decision not in {'resume_allowed', 'cleanup_only_required'}
        or not isinstance(requested_at, str)
        or not requested_at
        or len(requested_at) > 64
        or parsed_requested_at is None
        or parsed_requested_at.utcoffset() != timedelta(0)
        or parsed_requested_at.isoformat() != requested_at
        or (
            subsequent_signal is None
            and any(
                item is not None
                for item in (
                    subsequent_signal_at,
                    subsequent_execution_state,
                    subsequent_replay_decision,
                )
            )
        )
        or (
            subsequent_signal is not None
            and (
                not isinstance(subsequent_signal, str)
                or subsequent_signal not in {'SIGINT', 'SIGTERM'}
                or not isinstance(subsequent_signal_at, str)
                or not subsequent_signal_at
                or len(subsequent_signal_at) > 64
                or parsed_subsequent_signal_at is None
                or parsed_subsequent_signal_at.utcoffset() != timedelta(0)
                or parsed_subsequent_signal_at.isoformat()
                != subsequent_signal_at
                or (
                    subsequent_execution_state is not None
                    and (
                        not isinstance(subsequent_execution_state, str)
                        or subsequent_execution_state
                        not in MEASUREMENT_JOURNAL_STATES
                    )
                )
                or not isinstance(subsequent_replay_decision, str)
                or subsequent_replay_decision
                not in {'resume_allowed', 'cleanup_only_required'}
                or subsequent_replay_decision
                != _replay_decision(subsequent_execution_state)
            )
        )
        or not isinstance(cleanup_outcome, str)
        or cleanup_outcome
        not in {
            'pending',
            'not_attempted_process_exit',
            'completed',
            'failed',
        }
        or not isinstance(recovery_outcome, str)
        or recovery_outcome
        not in {
            'pending',
            'not_required',
            'resume_started',
            'resume_completed',
            'resume_failed',
            'resume_refused',
            'cleanup_only_started',
            'cleanup_completed',
            'cleanup_failed',
        }
    ):
        raise QualificationError(
            'The qualification interruption evidence values are invalid.'
        )
    if source == 'checkpoint':
        if (
            checkpoint is None
            or signal_name is not None
            or execution_state != checkpoint
        ):
            raise QualificationError(
                'Checkpoint interruption evidence is inconsistent.'
            )
    elif (
        checkpoint is not None
        or signal_name is None
        or mode != 'graceful'
    ):
        raise QualificationError('Signal interruption evidence is inconsistent.')
    if replay_decision != _replay_decision(execution_state):
        raise QualificationError(
            'The qualification interruption replay decision is inconsistent.'
        )
    return evidence


def _serialize_interruption_evidence(evidence: Mapping[str, Any]) -> bytes:
    value = (
        json.dumps(
            dict(evidence),
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        )
        + '\n'
    ).encode('ascii')
    if len(value) > MAX_INTERRUPTION_EVIDENCE_BYTES:
        raise QualificationError(
            'The qualification interruption evidence exceeds its safety limit.'
        )
    return value


def _write_interruption_evidence(
    job: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    job_id = str(job.get('id') or '')
    validated = _validated_interruption_evidence(evidence, job_id=job_id)
    value = _serialize_interruption_evidence(validated)
    target = _evidence_path(job_id)
    temporary = target.with_name(
        f'.{INTERRUPTION_EVIDENCE_FILENAME}.{uuid.uuid4().hex}.tmp'
    )
    try:
        with temporary.open('xb') as artifact:
            os.fchmod(artifact.fileno(), 0o600)
            artifact.write(value)
            artifact.flush()
            os.fsync(artifact.fileno())
        os.replace(temporary, target)
        directory_descriptor = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
        if target.read_bytes() != value:
            raise QualificationError(
                'The qualification interruption evidence changed after writing.'
            )
    except QualificationError:
        raise
    except OSError as exc:
        raise QualificationError(
            f'Unable to persist qualification interruption evidence: {exc}'
        ) from exc
    finally:
        temporary.unlink(missing_ok=True)
    return validated


def _read_interruption_evidence(job_id: str) -> dict[str, Any] | None:
    path = _evidence_path(job_id)
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise QualificationError(
            f'Unable to read qualification interruption evidence: {exc}'
        ) from exc
    if not raw or len(raw) > MAX_INTERRUPTION_EVIDENCE_BYTES:
        raise QualificationError(
            'The qualification interruption evidence has an invalid size.'
        )
    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f'duplicate field {key!r}')
            value[key] = item
        return value

    try:
        value = json.loads(raw, object_pairs_hook=unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise QualificationError(
            'The qualification interruption evidence is invalid JSON.'
        ) from exc
    validated = _validated_interruption_evidence(value, job_id=job_id)
    if raw != _serialize_interruption_evidence(validated):
        raise QualificationError(
            'The qualification interruption evidence is not canonical.'
        )
    return validated


def _update_interruption_evidence(
    job: Mapping[str, Any],
    *,
    cleanup_outcome: str | None = None,
    recovery_outcome: str | None = None,
) -> dict[str, Any] | None:
    evidence = _read_interruption_evidence(str(job['id']))
    if evidence is None:
        return None
    if cleanup_outcome is not None:
        evidence['cleanup_outcome'] = cleanup_outcome
    if recovery_outcome is not None:
        evidence['recovery_outcome'] = recovery_outcome
    return _write_interruption_evidence(job, evidence)


def _record_checkpoint_interruption(
    job: dict[str, Any],
    state: str,
    *,
    mode: str,
) -> None:
    observed_state = _execution_state(job)
    if state not in INTERRUPTION_CHECKPOINTS or observed_state != state:
        raise QualificationError(
            'The requested interruption checkpoint does not match the durable '
            'measurement journal.'
        )
    if _read_interruption_evidence(str(job['id'])) is not None:
        raise QualificationError(
            'Qualification interruption origin evidence already exists; '
            'refusing to replace it with another checkpoint.'
        )
    _write_interruption_evidence(
        job,
        {
            'schema_version': INTERRUPTION_EVIDENCE_SCHEMA_VERSION,
            'job_id': str(job['id']),
            'source': 'checkpoint',
            'mode': mode,
            'checkpoint': state,
            'signal': None,
            'execution_state': observed_state,
            'replay_decision': _replay_decision(observed_state),
            'requested_at': application.now(),
            'subsequent_signal': None,
            'subsequent_signal_at': None,
            'subsequent_signal_execution_state': None,
            'subsequent_signal_replay_decision': None,
            'cleanup_outcome': (
                'not_attempted_process_exit' if mode == 'hard' else 'pending'
            ),
            'recovery_outcome': 'pending' if mode == 'hard' else 'not_required',
        },
    )


def _record_signal_interruption(
    job: dict[str, Any],
    signum: int,
) -> None:
    signal_name = signal.Signals(signum).name
    if signal_name not in {'SIGINT', 'SIGTERM'}:
        raise QualificationError('Unsupported qualification cancellation signal.')
    observed_state = _execution_state(job)
    existing = _read_interruption_evidence(str(job['id']))
    if existing is not None:
        # Origin fields are immutable qualification evidence. A signal during
        # later resume/cleanup work is retained separately, and the first such
        # signal wins so repeated recovery attempts cannot rewrite history.
        if existing['subsequent_signal'] is None:
            existing['subsequent_signal'] = signal_name
            existing['subsequent_signal_at'] = application.now()
            existing['subsequent_signal_execution_state'] = observed_state
            existing['subsequent_signal_replay_decision'] = _replay_decision(
                observed_state
            )
            _write_interruption_evidence(job, existing)
        return
    _write_interruption_evidence(
        job,
        {
            'schema_version': INTERRUPTION_EVIDENCE_SCHEMA_VERSION,
            'job_id': str(job['id']),
            'source': 'signal',
            'mode': 'graceful',
            'checkpoint': None,
            'signal': signal_name,
            'execution_state': observed_state,
            'replay_decision': _replay_decision(observed_state),
            'requested_at': application.now(),
            'subsequent_signal': None,
            'subsequent_signal_at': None,
            'subsequent_signal_execution_state': None,
            'subsequent_signal_replay_decision': None,
            'cleanup_outcome': 'pending',
            'recovery_outcome': 'not_required',
        },
    )


def _mark_interrupted_unless_benchmark_is_terminal(
    job: dict[str, Any],
) -> str:
    """Preserve a completed/failed result when cancellation happens later."""

    outcome_probe = {
        **job,
        'error': None,
        'benchmark_interrupted': False,
    }
    outcome = application.benchmark_status(outcome_probe)
    if outcome not in {'complete', 'failed'}:
        job['benchmark_interrupted'] = True
        job['error'] = None
    job['cancel_requested_at'] = (
        job.get('cancel_requested_at') or application.now()
    )
    return outcome


def _hard_exit(exit_code: int) -> NoReturn:
    """Injectable hard-stop primitive; the OS releases the held lease fd."""

    os._exit(exit_code)


def _qualification_checkpoint_callback(
    *,
    interrupt_at: str,
    interrupt_mode: str,
) -> Callable[[dict[str, Any], str], None]:
    def checkpoint(job: dict[str, Any], state: str) -> None:
        application.raise_if_cancelled(job)
        if state != interrupt_at:
            return
        _record_checkpoint_interruption(job, state, mode=interrupt_mode)
        if interrupt_mode == 'graceful':
            cancel = application.cancellation_event(job)
            if cancel is None:
                cancel = threading.Event()
                job['_cancel_event'] = cancel
            cancel.set()
            raise application.RunCancelled(
                f'Qualification interruption injected at {state}.'
            )
        _hard_exit(HARD_INTERRUPTION_EXIT_CODE)
        raise QualificationError('The hard-exit primitive unexpectedly returned.')

    return checkpoint


@contextmanager
def _cooperative_signal_handlers(
    job: dict[str, Any],
) -> Iterator[dict[str, int | None]]:
    """Interrupt synchronous waits and retain the cooperative cancel signal.

    Azure SDK pollers are synchronous and can remain inside a socket wait or
    retry sleep for several minutes.  Merely setting the job's cancellation
    event leaves the main thread trapped in that wait.  Raising the existing
    ``RunCancelled`` control-flow exception from Python's main-thread signal
    handler breaks those waits promptly; durable evidence is still written by
    the normal lifecycle unwinder, never by the handler itself.
    """

    state: dict[str, int | None] = {'number': None}
    cancel = application.cancellation_event(job)
    if cancel is None:
        cancel = threading.Event()
        job['_cancel_event'] = cancel

    def handler(signum, _frame):
        # Python dispatches handlers on the main thread. Keep this deliberately
        # in-memory: durable writes happen after normal control returns.  The
        # BaseException-derived cancellation signal also bypasses Azure SDK
        # ``except Exception`` retry/error wrappers.
        if state['number'] is None:
            state['number'] = signum
        cancel.set()
        raise application.RunCancelled(
            'Qualification interrupted by an operating-system signal.'
        )

    if threading.current_thread() is not threading.main_thread():
        raise QualificationError(
            'Qualification signal handling requires the main Python thread.'
        )
    previous = {
        signum: signal.getsignal(signum)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        for signum in previous:
            signal.signal(signum, handler)
        yield state
    finally:
        for signum, old_handler in previous.items():
            signal.signal(signum, old_handler)


def _acquire_job_lease(job_id: str) -> RunLease:
    try:
        return acquire_run_lease(
            application.RUNS,
            job_id,
            'qualification-cli',
        )
    except RunLeaseHeldError as exc:
        raise QualificationError(
            f'Qualification job {job_id} is already owned by another process '
            f'({exc.owner.owner_label}, pid {exc.owner.pid}).'
        ) from exc
    except (RunLeaseError, ValueError) as exc:
        raise QualificationError(
            f'Unable to establish the exclusive lease for qualification job '
            f'{job_id}: {exc}'
        ) from exc


def _new_job(
    plan: BenchmarkPlan,
    ssh: Mapping[str, str | None],
    image_lock: Mapping[str, Any],
) -> tuple[dict[str, Any], RunLease]:
    job_id = uuid.uuid4().hex[:12]
    sanitized_plan = plan.model_dump(
        exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'}
    )
    run_directory = application.RUNS / job_id
    run_directory.mkdir(parents=True, exist_ok=False)
    # Acquire before publishing any active state, closing the prior gap where
    # the web app could observe and mutate a newly-created run before the
    # qualification harness obtained its own lock.
    lease = _acquire_job_lease(job_id)
    timestamp = application.now()
    job = {
        'id': job_id,
        'plan': sanitized_plan,
        '_key': ssh['private_key'],
        '_passphrase': ssh['passphrase'],
        '_public_key': ssh['public_key'],
        '_persist_state': True,
        '_cancel_event': threading.Event(),
        'status': 'queued',
        'events': [],
        'resources': {},
        'results': [],
        'created_at': timestamp,
        'updated_at': timestamp,
    }
    try:
        (run_directory / 'plan.json').write_text(
            json.dumps(sanitized_plan, indent=2),
            encoding='utf-8',
        )
        (run_directory / LOCK_FILENAME).write_text(
            json.dumps(dict(image_lock), indent=2) + '\n',
            encoding='utf-8',
        )
        _persist(job)
    except BaseException:
        lease.release()
        raise
    return job, lease


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


@contextmanager
def _exclusive_job_lock(job_id: str) -> Iterator[None]:
    """Compatibility context around the shared cross-process run lease."""

    lease = _acquire_job_lease(job_id)
    with lease:
        yield


def _resume_job(
    job: dict[str, Any],
    ssh: Mapping[str, str | None],
) -> tuple[dict[str, Any], BenchmarkPlan]:
    values = dict(job.get('plan') or {})
    values.update({
        'ssh_private_key': ssh['private_key'],
        'ssh_public_key': ssh['public_key'],
        'ssh_key_passphrase': ssh['passphrase'],
    })
    plan = BenchmarkPlan.model_validate(values)
    job.update({
        '_key': ssh['private_key'],
        '_passphrase': ssh['passphrase'],
        '_public_key': ssh['public_key'],
        '_persist_state': True,
        '_persist_results_artifact': False,
        '_cancel_event': threading.Event(),
    })
    return job, plan


def _require_resumable(
    job: Mapping[str, Any],
    *,
    measure: bool = False,
) -> None:
    job_id = str(job['id'])
    if job.get('status') in RESUME_TEARDOWN_STATUSES:
        raise QualificationError(
            f'Qualification job {job_id} is already in '
            f'{job.get("status")!r}; use --cleanup-only {job_id} instead.'
        )
    if not job.get('resources'):
        raise QualificationError(
            f'Qualification job {job_id} has no recoverable Azure resources.'
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


def _require_resume_workload_settings(
    job: Mapping[str, Any],
    args: argparse.Namespace,
) -> None:
    """Require the resume CLI to repeat the exact persisted load contract."""

    plan = job.get('plan')
    options = plan.get('deathstarbench') if isinstance(plan, Mapping) else None
    if not isinstance(options, Mapping):
        raise QualificationError('Saved qualification plan has no workload options.')
    mismatches = []
    for name, default in WORKLOAD_OPTION_DEFAULTS.items():
        saved = options.get(name, default)
        requested = getattr(args, name, default)
        if requested != saved:
            mismatches.append(
                f'{name.replace("_", "-")} saved={saved!r} requested={requested!r}'
            )
    if mismatches:
        raise QualificationError(
            'Resume workload settings differ from the persisted plan: '
            + '; '.join(mismatches)
        )


def _finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _require_qualified_metrics(
    metrics: Mapping[str, Any] | object,
    *,
    requested_duration: int,
    max_uncompleted_requests: int,
    label: str,
) -> None:
    if not isinstance(metrics, Mapping):
        raise QualificationError(f'The qualification {label} metrics are missing.')
    zero_metrics = ('errors', 'socket_errors')
    if any(metrics.get(name) != 0 for name in zero_metrics):
        raise QualificationError(
            f'Qualification {label} requires zero request and socket errors.'
        )
    uncompleted = metrics.get('uncompleted_requests')
    if (
        type(uncompleted) is not int
        or uncompleted < 0
        or uncompleted > max_uncompleted_requests
    ):
        raise QualificationError(
            f'Qualification {label} permits at most one in-flight request '
            'per connection at the fixed-duration cutoff.'
        )
    for name in ('successful_requests', 'total_requests'):
        value = metrics.get(name)
        if not _finite_number(value) or float(value) <= 0:
            raise QualificationError(
                f'Qualification {label} metric {name} must be greater than zero.'
            )
    sent_requests = metrics.get('sent_requests')
    if (
        type(sent_requests) is not int
        or sent_requests
        != int(metrics['total_requests']) + uncompleted
    ):
        raise QualificationError(
            f'Qualification {label} sent/completed request accounting is invalid.'
        )
    completion_rate = metrics.get('completion_rate_percent')
    if (
        not _finite_number(completion_rate)
        or not 99.0 <= float(completion_rate) <= 100.0
    ):
        raise QualificationError(
            f'Qualification {label} completion rate must be 99%-100%.'
        )

    measured_duration = metrics.get('duration_seconds')
    if (
        not _finite_number(measured_duration)
        or not 0.9 * requested_duration
        <= float(measured_duration)
        <= 1.1 * requested_duration
    ):
        raise QualificationError(
            f'Qualification {label} duration is outside 90%-110% of the '
            'requested interval.'
        )

    positive_capacity = (
        'load_generator_peak_rss_kb',
        'load_generator_logical_cpus',
    )
    for name in positive_capacity:
        value = metrics.get(name)
        if not _finite_number(value) or float(value) <= 0:
            raise QualificationError(
                f'Qualification {label} metric {name} is missing or invalid.'
            )
    nonnegative_capacity = (
        'load_generator_cpu_percent',
        'load_generator_capacity_used_percent',
    )
    for name in nonnegative_capacity:
        value = metrics.get(name)
        # GNU time reports CPU as an integer percentage, so a valid lightly
        # loaded run can round down to zero.  Zero is evidence, not absence.
        if not _finite_number(value) or float(value) < 0:
            raise QualificationError(
                f'Qualification {label} metric {name} is missing or invalid.'
            )
    if 'load_generator_saturation_warning' in metrics:
        raise QualificationError(
            f'The {label} load generator reported a saturation warning.'
        )
    if float(metrics['load_generator_capacity_used_percent']) >= 95:
        raise QualificationError(
            f'The {label} load generator used at least 95% of aggregate CPU '
            'capacity.'
        )


def _require_qualified_measurement_result(
    job: Mapping[str, Any],
    plan: BenchmarkPlan,
    runner_result: Mapping[str, Any] | object,
) -> None:
    """Apply qualification-only gates stricter than the general parser."""

    resources = job.get('resources')
    journal = (
        resources.get(DEATHSTARBENCH_EXECUTION_JOURNAL_KEY)
        if isinstance(resources, Mapping)
        else None
    )
    if not isinstance(journal, Mapping) or journal.get('state') != 'measurement_complete':
        raise QualificationError(
            'Distributed measurement did not persist measurement_complete.'
        )
    warmup_seconds = plan.deathstarbench.warmup_seconds
    if warmup_seconds:
        _require_qualified_metrics(
            journal.get('warmup_metrics'),
            requested_duration=warmup_seconds,
            max_uncompleted_requests=plan.deathstarbench.connections,
            label='warm-up',
        )
    results = job.get('results')
    if not isinstance(results, list) or len(results) != 1:
        raise QualificationError(
            'Qualification requires exactly one benchmark result.'
        )
    result = results[0]
    if (
        not isinstance(result, Mapping)
        or result.get('id') != 'deathstarbench'
        or result.get('status') != 'completed'
    ):
        raise QualificationError(
            'Qualification requires one completed DeathStarBench result.'
        )
    if runner_result is not result and runner_result != result:
        raise QualificationError(
            'The measurement runner returned a result different from the '
            'persisted result.'
        )

    output = result.get('output')
    if not isinstance(output, str):
        raise QualificationError('The qualification result output is missing.')
    lines = output.splitlines()
    metrics_line_count = sum(
        line.startswith('OCI_DSB_METRICS ') for line in lines
    )
    sent_line_count = sum(
        re.fullmatch(r'Sent [0-9]+ requests', line) is not None for line in lines
    )
    if metrics_line_count != 1 or sent_line_count != 1:
        raise QualificationError(
            'Qualification output must contain exactly one OCI_DSB_METRICS '
            'line and one Sent line.'
        )

    _require_qualified_metrics(
        result.get('metrics'),
        requested_duration=plan.deathstarbench.duration_seconds,
        max_uncompleted_requests=plan.deathstarbench.connections,
        label='measurement',
    )

    try:
        application.require_complete_benchmark_results(job)
    except RuntimeError as exc:
        raise QualificationError(str(exc)) from exc


def _write_and_require_report(job: dict[str, Any]) -> None:
    _set_status(job, 'reporting')
    _event(job, 'Report', 'Generating the qualification result and report artifacts.')
    application.make_report(job)
    run_directory = application.RUNS / str(job['id'])
    missing = [
        name
        for name in ('results.json', 'report.html')
        if not (run_directory / name).is_file()
    ]
    if missing:
        raise QualificationError(
            'Qualification report generation did not create: ' + ', '.join(missing)
        )


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
    # Prove the requested path is a candidate before creating/refreshing lease
    # metadata, then re-load under the lease so cleanup never mutates stale
    # pre-lock state.
    _load_candidate_job(job_id)
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
    run_path = application.RUNS / str(job['id']) / LOCK_FILENAME
    if not run_path.is_file():
        raise QualificationError(
            f'The saved candidate image lock is missing; restore {run_path} '
            'before resuming.'
        )
    saved_lock = _read_image_lock(run_path)
    if requested_path is None:
        return saved_lock
    if not requested_path.is_file():
        raise QualificationError(
            f'The requested candidate image lock does not exist: {requested_path}.'
        )
    requested_lock = _read_image_lock(requested_path)
    if requested_lock != saved_lock:
        raise QualificationError(
            'The requested candidate image lock does not exactly match the '
            'saved run lock; refusing to replace persisted provenance.'
        )
    return saved_lock


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
                existing_evidence = _read_interruption_evidence(str(job['id']))
                if existing_evidence is not None:
                    recovery_outcome = None
                    if recovery_attempt:
                        if qualified:
                            recovery_outcome = 'resume_completed'
                        elif not setup_complete:
                            recovery_outcome = 'resume_refused'
                        else:
                            recovery_outcome = 'resume_failed'
                    _update_interruption_evidence(
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
    if args.resume:
        # Ownership is proven before even creating the harness lock file. Once
        # this persisted candidate is accepted, acquire the shared lease and
        # re-load it so no stale pre-lock object can authorize mutation.
        preliminary = _load_candidate_job(args.resume)
        job_id = str(preliminary['id'])
        print(f'Azure qualification job: {job_id}', flush=True)

        with _exclusive_job_lock(job_id):
            job = _load_candidate_job(job_id)

            def resume_setup():
                _require_resumable(job, measure=args.measure)
                _require_resume_workload_settings(job, args)
                ssh = _ssh_material()
                _job, plan = _resume_job(job, ssh)
                image_lock = _lock_for_run(args.image_lock, job)
                _preflight_anonymous_ghcr_images(image_lock)
                return plan, ssh, image_lock

            return _run_qualification(
                job,
                resume_setup,
                measure=args.measure,
                interrupt_at=interrupt_at,
                interrupt_mode=interrupt_mode,
                recovery_attempt=True,
            )

    # A new run performs every local/registry validation before its ownership
    # journal exists. No Azure write is possible until the job is persisted and
    # entered into the cleanup-guaranteed lifecycle below.
    ssh = _ssh_material()
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
