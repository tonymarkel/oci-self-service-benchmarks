"""Normal application dispatch for the distributed DeathStarBench profile.

This module is deliberately small.  Cloud adapters continue to own resource
creation and recovery, while the existing provider-neutral distributed
runtime owns K3s, workload deployment, network attestation, and measurement.
The web supervisor supplies persistence, SSH execution, and the run lease.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping
import json
import os
from pathlib import Path
import re
import uuid
from typing import Any

from .deathstarbench_contract import (
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    K3S_RUNTIME_ID,
)
from .deathstarbench_distributed import (
    DISTRIBUTED_NETWORK_QUALIFICATION_KEY,
    prepare_distributed_k3s_candidate,
    prepare_distributed_social_network_candidate,
    qualify_distributed_network_paths,
)
from .deathstarbench_distributed_measurement import (
    run_distributed_social_network_measurement,
)
from .deathstarbench_k3s_workload import (
    require_released_distributed_bundle,
)
from .providers import aws, azure, gcp, oci
from .providers.registry import provider_id


IMAGE_LOCK_FILENAME = 'deathstarbench-social-image-lock.json'
CHECKED_IN_IMAGE_LOCK = (
    Path(__file__).resolve().parents[1]
    / 'docs'
    / 'qualification'
    / 'deathstarbench-social-network-images-v1.json'
)


class PublicDistributedLifecycleError(RuntimeError):
    """The released application path cannot prove its immutable contract."""


def _value(document: Any, key: str, default: Any = None) -> Any:
    if isinstance(document, Mapping):
        return document.get(key, default)
    return getattr(document, key, default)


def _deathstarbench_options(plan: Any) -> Any:
    return _value(plan, 'deathstarbench', {}) or {}


def is_distributed_deathstarbench_plan(plan: Any) -> bool:
    """Return true only for the exact versioned distributed profile."""

    benchmarks = tuple(_value(plan, 'benchmarks', ()) or ())
    options = _deathstarbench_options(plan)
    return (
        'deathstarbench' in benchmarks
        and _value(options, 'topology_id') == DISTRIBUTED_TIERED_TOPOLOGY_ID
        and _value(options, 'runtime_id') == K3S_RUNTIME_ID
    )


def validate_distributed_deathstarbench_plan(plan: Any) -> str:
    """Validate the exact public lifecycle contract without opening its gate.

    The release flag remains enforced by the API, provisioner, and benchmark
    dispatcher.  Keeping this structural validator independent lets the
    operator-qualified implementation be tested while that flag is closed.
    """

    provider = provider_id(plan)
    benchmarks = tuple(_value(plan, 'benchmarks', ()) or ())
    llm_benchmarks = tuple(_value(plan, 'llm_benchmarks', ()) or ())
    options = _deathstarbench_options(plan)
    if benchmarks != ('deathstarbench',) or llm_benchmarks:
        raise ValueError(
            'Distributed DeathStarBench must be the only selected benchmark.'
        )
    if (
        _value(options, 'topology_id') != DISTRIBUTED_TIERED_TOPOLOGY_ID
        or _value(options, 'runtime_id') != K3S_RUNTIME_ID
        or _value(options, 'workload') != 'social_network'
    ):
        raise ValueError(
            'The public distributed lifecycle requires the exact Social '
            'Network distributed_tiered_v1/k3s_v1 contract.'
        )
    return provider


def _read_image_lock(path: Path) -> tuple[bytes, dict[str, Any]]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise PublicDistributedLifecycleError(
            f'Unable to read the distributed image lock {path}: {exc}'
        ) from exc
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublicDistributedLifecycleError(
            f'The distributed image lock {path} is not valid UTF-8 JSON.'
        ) from exc
    if not isinstance(value, dict):
        raise PublicDistributedLifecycleError(
            'The distributed image lock must be a JSON object.'
        )
    try:
        require_released_distributed_bundle(value)
    except ValueError as exc:
        raise PublicDistributedLifecycleError(
            f'The distributed image lock is invalid: {exc}'
        ) from exc
    return payload, value


def preflight_distributed_deathstarbench_release(
    *,
    source: Path = CHECKED_IN_IMAGE_LOCK,
) -> dict[str, Any]:
    """Validate the complete checked-in public bundle before run creation."""

    _payload, value = _read_image_lock(source)
    return value


def snapshot_distributed_image_lock(
    run_directory: Path,
    *,
    source: Path = CHECKED_IN_IMAGE_LOCK,
) -> Path:
    """Validate and exclusively snapshot the release lock before dispatch."""

    payload, _ = _read_image_lock(source)
    target = run_directory / IMAGE_LOCK_FILENAME
    temporary = run_directory / f'.{IMAGE_LOCK_FILENAME}.{uuid.uuid4().hex}.tmp'
    linked = False
    try:
        with temporary.open('xb') as artifact:
            os.fchmod(artifact.fileno(), 0o600)
            artifact.write(payload)
            artifact.flush()
            os.fsync(artifact.fileno())
        if target.exists():
            raise PublicDistributedLifecycleError(
                f'Refusing to replace the saved distributed image lock {target}.'
            )
        os.link(temporary, target)
        linked = True
        directory_descriptor = os.open(run_directory, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except PublicDistributedLifecycleError:
        if linked:
            target.unlink(missing_ok=True)
        raise
    except OSError as exc:
        if linked:
            try:
                target.unlink(missing_ok=True)
            except OSError as cleanup_exc:
                raise PublicDistributedLifecycleError(
                    'Unable to snapshot the distributed image lock and '
                    f'unable to remove its incomplete copy: {cleanup_exc}'
                ) from exc
        raise PublicDistributedLifecycleError(
            f'Unable to snapshot the distributed image lock: {exc}'
        ) from exc
    finally:
        temporary.unlink(missing_ok=True)
    try:
        saved_payload = target.read_bytes()
    except OSError as exc:
        try:
            target.unlink(missing_ok=True)
        except OSError as cleanup_exc:
            raise PublicDistributedLifecycleError(
                'Unable to verify the saved distributed image lock and '
                f'unable to remove its incomplete copy: {cleanup_exc}'
            ) from exc
        raise PublicDistributedLifecycleError(
            f'Unable to verify the saved distributed image lock: {exc}'
        ) from exc
    if saved_payload != payload:
        try:
            target.unlink(missing_ok=True)
        except OSError as exc:
            raise PublicDistributedLifecycleError(
                'The saved distributed image lock changed while it was '
                f'written and could not be removed: {exc}'
            ) from exc
        raise PublicDistributedLifecycleError(
            'The saved distributed image lock changed while it was written.'
        )
    return target


def load_run_distributed_image_lock(
    runs_root: Path,
    job_id: str,
) -> dict[str, Any]:
    """Load only the immutable copy owned by one persisted run."""

    job_id = str(job_id)
    if not re.fullmatch(r'[0-9a-f]{12}', job_id):
        raise PublicDistributedLifecycleError(
            'A canonical run ID is required to load the distributed image lock.'
        )
    _payload, value = _read_image_lock(
        runs_root / job_id / IMAGE_LOCK_FILENAME
    )
    return value


def provision_distributed_deathstarbench(
    job: MutableMapping[str, Any],
    plan: Any,
    image_lock: Mapping[str, Any],
    *,
    public_key: str,
    emit: Callable[..., Any],
    persist: Callable[..., Any],
    oci_context_factory: Callable[[Any, str], Mapping[str, Any]],
) -> MutableMapping[str, Any]:
    """Dispatch the exact five-node provider graph and publish SSH aliases."""

    provider = validate_distributed_deathstarbench_plan(plan)
    require_released_distributed_bundle(image_lock)
    if provider == 'azure':
        return azure.provision_distributed_deathstarbench_candidate(
            job,
            plan,
            public_key=public_key,
            emit=emit,
            persist=persist,
        )
    if provider == 'gcp':
        return gcp.provision_distributed_deathstarbench_candidate(
            job,
            plan,
            public_key=public_key,
            emit=emit,
            persist=persist,
        )
    if provider == 'aws':
        pins = aws.resolve_distributed_deathstarbench_image_pins(plan)
        result = aws.provision_distributed_deathstarbench_candidate(
            job,
            plan,
            support_image=pins['support_image'],
            application_image=pins['application_image'],
            public_key=public_key,
            emit=emit,
            persist=persist,
        )
        # AWS aliases are intentionally published only after a fresh whole-
        # graph read verifies every resource and attachment.
        aws.validate_distributed_deathstarbench_candidate(
            job,
            persist=persist,
        )
        return result

    context = oci_context_factory(plan, public_key)
    if not isinstance(context, Mapping) or set(context) != {'clients', 'inputs'}:
        raise PublicDistributedLifecycleError(
            'The OCI distributed provisioning context is incomplete.'
        )
    clients = context['clients']
    inputs = context['inputs']
    oci.provision_distributed_deathstarbench_candidate(
        job,
        inputs=inputs,
        clients=clients,
        persist=persist,
        emit=emit,
    )
    oci.publish_distributed_runtime_projection(
        job,
        clients=clients,
        persist=persist,
    )
    return job['resources']


def run_distributed_deathstarbench(
    job: MutableMapping[str, Any],
    plan: Any,
    image_lock: Mapping[str, Any],
    *,
    execute: Callable[..., str],
    emit: Callable[..., Any],
    persist: Callable[..., Any],
) -> dict[str, Any]:
    """Run the qualified provider-neutral runtime exactly once."""

    validate_distributed_deathstarbench_plan(plan)
    require_released_distributed_bundle(image_lock)
    prepare_distributed_k3s_candidate(
        job,
        execute=execute,
        emit=emit,
        persist=persist,
    )
    prepare_distributed_social_network_candidate(
        job,
        image_lock,
        execute=execute,
        emit=emit,
        persist=persist,
    )
    attestation = qualify_distributed_network_paths(
        job,
        image_lock,
        execute=execute,
        emit=emit,
    )
    if not isinstance(attestation, dict):
        raise PublicDistributedLifecycleError(
            'Distributed network qualification returned malformed evidence.'
        )
    resources = job.get('resources')
    if not isinstance(resources, MutableMapping):
        raise PublicDistributedLifecycleError(
            'Distributed network qualification lost resource state.'
        )
    previous = resources.get(DISTRIBUTED_NETWORK_QUALIFICATION_KEY)
    if previous is not None and previous != attestation:
        raise PublicDistributedLifecycleError(
            'Distributed network qualification evidence changed within the run.'
        )
    if previous is None:
        resources[DISTRIBUTED_NETWORK_QUALIFICATION_KEY] = attestation
        persist(job)
    return run_distributed_social_network_measurement(
        job,
        image_lock,
        _deathstarbench_options(plan),
        execute=execute,
        emit=emit,
        persist=persist,
    )
