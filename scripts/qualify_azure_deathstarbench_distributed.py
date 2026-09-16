#!/usr/bin/env python3
"""Run the unreleased distributed DeathStarBench Azure qualification safely.

This is an operator-only harness.  It persists the complete ownership
contract before Azure writes, deploys only the candidate topology/runtime,
stops at the attested ``workload_ready`` boundary, and always attempts exact
resource-group cleanup.  It does not initialize a dataset or produce a
benchmark result.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import uuid
from typing import Any, Callable, Iterator, Mapping
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


LOCK_FILENAME = 'deathstarbench-social-image-lock.json'
QUALIFICATION_LOCK_FILENAME = '.deathstarbench-azure-qualification.lock'
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


def _new_job(
    plan: BenchmarkPlan,
    ssh: Mapping[str, str | None],
    image_lock: Mapping[str, Any],
) -> dict[str, Any]:
    job_id = uuid.uuid4().hex[:12]
    sanitized_plan = plan.model_dump(
        exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'}
    )
    run_directory = application.RUNS / job_id
    run_directory.mkdir(parents=True, exist_ok=False)
    (run_directory / 'plan.json').write_text(
        json.dumps(sanitized_plan, indent=2),
        encoding='utf-8',
    )
    (run_directory / LOCK_FILENAME).write_text(
        json.dumps(dict(image_lock), indent=2) + '\n',
        encoding='utf-8',
    )
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
    _persist(job)
    return job


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
    """Exclude a second qualification-harness process for the same job."""

    lock_path = application.RUNS / job_id / QUALIFICATION_LOCK_FILENAME
    try:
        lock_file = lock_path.open('a+', encoding='utf-8')
    except OSError as exc:
        raise QualificationError(
            f'Unable to open qualification lock {lock_path}: {exc}'
        ) from exc
    try:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise QualificationError(
                f'Qualification job {job_id} is already owned by another '
                'qualification process.'
            ) from exc
        yield
    finally:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        finally:
            lock_file.close()


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


def _require_resumable(job: Mapping[str, Any]) -> None:
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
    job = _load_candidate_job(job_id)
    with _exclusive_job_lock(job_id):
        return 0 if _cleanup(job) else 2


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
) -> int:
    failure: BaseException | None = None
    qualified = False
    try:
        plan, ssh, image_lock = setup()
        _set_status(job, 'provisioning')
        azure.provision_distributed_deathstarbench_candidate(
            job,
            plan,
            public_key=str(ssh['public_key']),
            emit=_event,
            persist=_persist,
        )
        _set_status(job, 'testing')
        prepare_azure_distributed_k3s_candidate(
            job,
            execute=application.ssh,
            emit=_event,
            persist=_persist,
        )
        prepare_azure_distributed_social_network_candidate(
            job,
            image_lock,
            execute=application.ssh,
            emit=_event,
            persist=_persist,
        )
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
        qualified = True
        _event(
            job,
            'Qualified',
            'Azure K3s and Social Network workload reached exact workload_ready.',
        )
    except BaseException as exc:  # cleanup must also run for Ctrl-C
        failure = exc
        job['error'] = f'{type(exc).__name__}: {exc}'
        _set_status(job, 'cleanup_pending')
        print(f'Qualification failed: {job["error"]}', file=sys.stderr, flush=True)
    finally:
        cleanup_complete = _cleanup(job)

    if not cleanup_complete:
        return 2
    if failure is not None:
        return 130 if isinstance(failure, KeyboardInterrupt) else 1
    if not qualified:
        return 1
    print(
        f'Qualification succeeded and all Azure resources were destroyed '
        f'(job {job["id"]}).',
        flush=True,
    )
    return 0


def _qualify(args: argparse.Namespace) -> int:
    if args.resume:
        # Ownership is proven before even creating the harness lock file. Once
        # this persisted candidate is accepted, every subsequent preflight is
        # within the cleanup-guaranteed lifecycle.
        job = _load_candidate_job(args.resume)
        print(f'Azure qualification job: {job["id"]}', flush=True)

        def resume_setup():
            _require_resumable(job)
            ssh = _ssh_material()
            _job, plan = _resume_job(job, ssh)
            image_lock = _lock_for_run(args.image_lock, job)
            _preflight_anonymous_ghcr_images(image_lock)
            return plan, ssh, image_lock

        with _exclusive_job_lock(str(job['id'])):
            return _run_qualification(job, resume_setup)

    # A new run performs every local/registry validation before its ownership
    # journal exists. No Azure write is possible until the job is persisted and
    # entered into the cleanup-guaranteed lifecycle below.
    ssh = _ssh_material()
    image_lock = _read_image_lock(args.image_lock)
    _preflight_anonymous_ghcr_images(image_lock)
    plan = _plan(args, ssh)
    job = _new_job(plan, ssh, image_lock)
    print(f'Azure qualification job: {job["id"]}', flush=True)

    def new_setup():
        return plan, ssh, image_lock

    with _exclusive_job_lock(str(job['id'])):
        return _run_qualification(job, new_setup)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
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
