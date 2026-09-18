"""Safe, versioned result documents and like-for-like chart contracts.

This module deliberately has no dependency on the FastAPI application.  It
turns an in-memory run into a small JSON artifact, describes the metrics that
are safe to chart, and groups runs only when their workload contracts match.
Raw commands and benchmark output never enter the normalized artifact.
"""

from __future__ import annotations

import ast
import hashlib
import html
import json
import math
import os
import re
import statistics
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .catalog import PHORONIX_PROFILES
from .deathstarbench_contract import (
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    SINGLE_HOST_RUNTIME_REVISION,
    runtime_profile,
)
from .llama_cpp import (
    AARCH64_NATIVE_CMAKE_OPTIONS,
    AARCH64_NATIVE_CPU_PROFILE,
    CPU_BUILD_PROFILE_NATIVE_PAIRS,
    X86_64_PORTABLE_CMAKE_OPTIONS,
    X86_64_PORTABLE_CPU_PROFILE,
)
from .models import (
    ApacheBenchOptions,
    DEATHSTARBENCH_DEFAULT_RUNTIME_ID,
    DEATHSTARBENCH_DEFAULT_TOPOLOGY_ID,
    DeathStarBenchOptions,
    Iperf3Options,
    PhoronixOptions,
    SecurityOptions,
    StorageOptions,
    SysbenchOptions,
    canonicalize_benchmark_plan,
    canonicalize_provider_plan,
)


RESULTS_ARTIFACT_NAME = 'results.json'
RESULTS_SCHEMA = 'cloud-self-service-benchmarks/results'
RESULTS_SCHEMA_VERSION = 1
COMPARISON_SCHEMA = 'cloud-self-service-benchmarks/comparison'
COMPARISON_SCHEMA_VERSION = 1
WORKLOAD_CONTRACT_VERSION = 3

LLAMA_ARCHITECTURE_PROFILE_METHOD = 'validated-cloud-cpu-profile-v1'
DEATHSTARBENCH_LEGACY_RUNTIME_REVISION = SINGLE_HOST_RUNTIME_REVISION
STORAGE_RESULT_IDS = frozenset({'fio', 'sysbench_fileio'})
STORAGE_TARGET_CONTRACT = 'v1'
STORAGE_TARGET_POLICY = (
    'prefer_verified_instance_local_nvme_else_additional_volume_v1'
)
STORAGE_TARGET_KINDS = frozenset({
    'instance_local_nvme',
    'provisioned_data_volume',
})
STORAGE_TARGET_LAYOUT = 'single_device_v1'
STORAGE_TARGET_MOUNT_POINTS = frozenset({'/benchmark-local', '/data'})
STORAGE_TARGET_TRANSPORTS = frozenset({
    'iscsi',
    'nvme',
    'paravirtualized',
    'scsi',
    'virtio',
    'xen',
})
STORAGE_TARGET_VERIFICATION = {
    'instance_local_nvme': 'provider_attested_guest_verified_v1',
    'provisioned_data_volume': 'manifest_bound_guest_verified_v1',
}

DIRECTION_HIGHER = 'higher_is_better'
DIRECTION_LOWER = 'lower_is_better'


@dataclass(frozen=True)
class MetricSpec:
    """A numeric result that can be compared without changing its meaning."""

    key: str
    label: str
    unit: str
    direction: str
    primary: bool = False
    subtest: str | None = None
    uncertainty_key: str | None = None
    uncertainty_kind: str | None = None


def _spec(
    key: str,
    label: str,
    unit: str,
    direction: str,
    *,
    primary: bool = False,
    subtest: str | None = None,
    uncertainty_key: str | None = None,
    uncertainty_kind: str | None = None,
) -> MetricSpec:
    return MetricSpec(
        key=key,
        label=label,
        unit=unit,
        direction=direction,
        primary=primary,
        subtest=subtest,
        uncertainty_key=uncertainty_key,
        uncertainty_kind=uncertainty_kind,
    )


_APACHEBENCH_METRICS = (
    _spec(
        'mean_requests_per_second',
        'Requests per second',
        'requests/s',
        DIRECTION_HIGHER,
        primary=True,
        uncertainty_key='standard_deviation_requests_per_second',
        uncertainty_kind='standard_deviation',
    ),
    _spec(
        'mean_time_per_request_ms',
        'Mean request latency',
        'ms',
        DIRECTION_LOWER,
    ),
    _spec('mean_p50_ms', 'p50 latency', 'ms', DIRECTION_LOWER),
    _spec('mean_p90_ms', 'p90 latency', 'ms', DIRECTION_LOWER),
    _spec(
        'mean_p95_ms',
        'p95 latency',
        'ms',
        DIRECTION_LOWER,
        primary=True,
    ),
    _spec(
        'mean_p99_ms',
        'p99 latency',
        'ms',
        DIRECTION_LOWER,
        primary=True,
    ),
    _spec(
        'mean_transfer_rate_kib_per_second',
        'Transfer rate',
        'KiB/s',
        DIRECTION_HIGHER,
    ),
)

_PHORONIX_RESULT_SPECS = {
    f'phoronix_{item["id"]}': (
        _spec(
            'score',
            item['name'],
            item['unit'],
            item['direction'],
            primary=True,
            subtest='dynamic_configuration',
            uncertainty_key='measured_trials',
            uncertainty_kind='standard_deviation',
        ),
    )
    for item in PHORONIX_PROFILES
}

METRIC_REGISTRY: dict[str, tuple[MetricSpec, ...]] = {
    'sysbench_cpu': (
        _spec(
            'events_per_second',
            'CPU events per second',
            'events/s',
            DIRECTION_HIGHER,
            primary=True,
            subtest='cpu',
        ),
    ),
    'sysbench_memory': (
        _spec(
            'throughput_mib_per_second',
            'Memory throughput',
            'MiB/s',
            DIRECTION_HIGHER,
            primary=True,
            subtest='memory_write',
        ),
        _spec(
            'operations_per_second',
            'Memory operations per second',
            'operations/s',
            DIRECTION_HIGHER,
            subtest='memory_write',
        ),
    ),
    'sysbench_fileio': (
        _spec(
            'reads_per_second',
            'Read operations',
            'operations/s',
            DIRECTION_HIGHER,
            primary=True,
            subtest='random_read_write',
        ),
        _spec(
            'writes_per_second',
            'Write operations',
            'operations/s',
            DIRECTION_HIGHER,
            primary=True,
            subtest='random_read_write',
        ),
        _spec(
            'read_mib_per_second',
            'Read bandwidth',
            'MiB/s',
            DIRECTION_HIGHER,
            primary=True,
            subtest='random_read_write',
        ),
        _spec(
            'written_mib_per_second',
            'Write bandwidth',
            'MiB/s',
            DIRECTION_HIGHER,
            primary=True,
            subtest='random_read_write',
        ),
    ),
    'stream': tuple(
        _spec(
            f'{name}_mb_per_second',
            f'STREAM {name.title()}',
            'MB/s',
            DIRECTION_HIGHER,
            primary=name == 'triad',
            subtest=name,
        )
        for name in ('copy', 'scale', 'add', 'triad')
    ),
    'fio': tuple(
        _spec(
            f'{workload}_{kind}',
            f'fio {workload} {label}',
            unit,
            DIRECTION_HIGHER,
            primary=(
                (workload in {'read', 'write'} and kind.startswith('bandwidth'))
                or (workload in {'randread', 'randwrite'} and kind == 'iops')
            ),
            subtest=workload,
        )
        for workload in ('read', 'write', 'randread', 'randwrite')
        for kind, label, unit in (
            ('bandwidth_bytes_per_second', 'bandwidth', 'bytes/s'),
            ('iops', 'IOPS', 'IOPS'),
        )
    ),
    'iperf_tcp': (
        _spec(
            'receiver_gigabits_per_second',
            'TCP receiver throughput',
            'Gbit/s',
            DIRECTION_HIGHER,
            primary=True,
            subtest='tcp',
        ),
        _spec(
            'sender_gigabits_per_second',
            'TCP sender throughput',
            'Gbit/s',
            DIRECTION_HIGHER,
            subtest='tcp',
        ),
        _spec(
            'retransmits',
            'TCP retransmits',
            'count',
            DIRECTION_LOWER,
            subtest='tcp',
        ),
    ),
    'iperf_udp': (
        _spec(
            'throughput_gigabits_per_second',
            'UDP receiver throughput',
            'Gbit/s',
            DIRECTION_HIGHER,
            primary=True,
            subtest='udp',
        ),
        _spec(
            'packet_loss_percent',
            'UDP packet loss',
            '%',
            DIRECTION_LOWER,
            primary=True,
            subtest='udp',
        ),
        _spec(
            'jitter_ms',
            'UDP jitter',
            'ms',
            DIRECTION_LOWER,
            primary=True,
            subtest='udp',
        ),
    ),
    'iperf_sctp': (
        _spec(
            'receiver_gigabits_per_second',
            'SCTP receiver throughput',
            'Gbit/s',
            DIRECTION_HIGHER,
            primary=True,
            subtest='sctp',
        ),
        _spec(
            'sender_gigabits_per_second',
            'SCTP sender throughput',
            'Gbit/s',
            DIRECTION_HIGHER,
            subtest='sctp',
        ),
    ),
    'apachebench_new_connections': _APACHEBENCH_METRICS,
    'apachebench_keep_alive': _APACHEBENCH_METRICS,
    'deathstarbench': (
        _spec(
            'p50_ms',
            'p50 latency',
            'ms',
            DIRECTION_LOWER,
            subtest='request_latency',
        ),
        _spec(
            'p95_ms',
            'p95 latency',
            'ms',
            DIRECTION_LOWER,
            primary=True,
            subtest='request_latency',
        ),
        _spec(
            'p99_ms',
            'p99 latency',
            'ms',
            DIRECTION_LOWER,
            primary=True,
            subtest='request_latency',
        ),
        _spec(
            'completion_rate_percent',
            'Request completion rate',
            '%',
            DIRECTION_HIGHER,
            primary=True,
            subtest='request_outcome',
        ),
        _spec(
            'error_rate_percent',
            'HTTP error rate',
            '%',
            DIRECTION_LOWER,
            primary=True,
            subtest='request_outcome',
        ),
        _spec(
            'socket_errors',
            'Socket errors',
            'count',
            DIRECTION_LOWER,
            subtest='request_outcome',
        ),
        _spec(
            'uncompleted_requests',
            'Uncompleted requests',
            'count',
            DIRECTION_LOWER,
            subtest='request_outcome',
        ),
        _spec(
            'throughput_requests_per_second',
            'Completed throughput',
            'requests/s',
            DIRECTION_HIGHER,
            subtest='requested_rate_capped_throughput',
        ),
    ),
    'llama_bench': tuple(
        _spec(
            f'{prefix}_tokens_per_second',
            label,
            'tokens/s',
            DIRECTION_HIGHER,
            primary=True,
            subtest=prefix,
            uncertainty_key=f'{prefix}_stddev_tokens_per_second',
            uncertainty_kind='standard_deviation',
        )
        for prefix, label in (
            ('prompt_512', 'Prompt processing — 512 tokens'),
            ('prompt_2048', 'Prompt processing — 2,048 tokens'),
            ('generation_128', 'Token generation — 128 tokens'),
            ('generation_512', 'Token generation — 512 tokens'),
        )
    ),
    **_PHORONIX_RESULT_SPECS,
}

CANONICAL_RESULT_IDS = frozenset(METRIC_REGISTRY)


_RESULT_FIELDS = (
    'id',
    'name',
    'status',
    'error',
    'started_at',
    'completed_at',
    'finished_at',
    'duration_seconds',
    'metadata',
    'metrics',
    'trial_metrics',
)
_RUN_FIELDS = (
    'id',
    'status',
    'benchmark_status',
    'error',
    'cleanup_error',
    'benchmark_interrupted',
    'created_at',
    'updated_at',
)
_PLAN_FIELDS = (
    'provider',
    'region',
    'gcp_zone',
    'azure_zone',
    'availability_domain',
    'fault_domain',
    'shape',
    'ocpus',
    'memory_gb',
    'security',
    'networking',
    'storage',
    'deathstarbench',
    'apachebench',
    'sysbench',
    'iperf3',
    'phoronix',
    'benchmarks',
    'llm_benchmarks',
    'destroy_after_completion',
)
_UNSAFE_KEY_PARTS = (
    'password',
    'passphrase',
    'private_key',
    'public_key',
    'secret',
    'credential',
    'authorization',
    'cookie',
    'command',
    'output',
    'script',
    'user_data',
)
_IP_KEY_RE = re.compile(r'(?:^|_)(?:ip|ip_address)(?:$|_)')
_CONTROL_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')
_DROP = object()
_SAFE_ATTESTATION_KEYS = frozenset({
    # A digest of the pinned, public workload driver is execution provenance,
    # not executable source.  Preserve this exact key while continuing to
    # reject arbitrary script-bearing metadata.
    'load_script_sha256',
})


def _mapping(value: object) -> dict[str, Any]:
    if hasattr(value, 'model_dump'):
        value = value.model_dump()
    return dict(value) if isinstance(value, Mapping) else {}


def _unsafe_key(key: object) -> bool:
    normalized = str(key).strip().lower().replace('-', '_')
    if normalized in _SAFE_ATTESTATION_KEYS:
        return False
    return (
        any(part in normalized for part in _UNSAFE_KEY_PARTS)
        or normalized == 'token'
        or normalized.endswith(('_token', '_access_token'))
        or bool(_IP_KEY_RE.search(normalized))
        or normalized.endswith(('_private_address', '_public_address'))
    )


def _clean_text(value: object, limit: int = 8192) -> str:
    return _CONTROL_RE.sub('', str(value))[:limit]


def _safe_json_value(
    value: object,
    *,
    filter_keys: bool = True,
    depth: int = 0,
) -> Any:
    if depth > 8:
        return _DROP
    if value is None or isinstance(value, (str, int, bool)):
        if isinstance(value, str):
            return _clean_text(value, 16384)
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else _DROP
    if isinstance(value, Mapping):
        sanitized = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key)
            if filter_keys and _unsafe_key(key):
                continue
            item = _safe_json_value(
                raw_value,
                filter_keys=filter_keys,
                depth=depth + 1,
            )
            if item is not _DROP:
                sanitized[key] = item
        return sanitized
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        sanitized_items = []
        for raw_item in value[:2048]:
            item = _safe_json_value(
                raw_item,
                filter_keys=filter_keys,
                depth=depth + 1,
            )
            if item is not _DROP:
                sanitized_items.append(item)
        return sanitized_items
    return _DROP


def sanitize_result(result: Mapping[str, Any] | object) -> dict[str, Any]:
    """Return only report-safe structured fields from one benchmark result."""

    source = _mapping(result)
    sanitized: dict[str, Any] = {}
    for key in _RESULT_FIELDS:
        if key not in source:
            continue
        value = source[key]
        if key in {'id', 'name', 'status'}:
            if value is not None:
                sanitized[key] = _clean_text(value, 512)
            continue
        if key == 'error':
            if value:
                sanitized[key] = _clean_text(value)
            continue
        item = _safe_json_value(value, filter_keys=True)
        if item is not _DROP:
            sanitized[key] = item
    return sanitized


def _validated_options(model, value: object) -> tuple[dict[str, Any], bool]:
    try:
        return model.model_validate(value or {}).model_dump(), True
    except Exception:
        safe = _safe_json_value(value or {}, filter_keys=True)
        return (safe if isinstance(safe, dict) else {}), False


def canonical_plan(plan: Mapping[str, Any] | object) -> dict[str, Any]:
    """Canonicalize legacy selections and remove credentials/account IDs."""

    source = _mapping(plan)
    canonical = canonicalize_benchmark_plan(canonicalize_provider_plan(source))
    canonical = _mapping(canonical)
    result: dict[str, Any] = {}
    for key in _PLAN_FIELDS:
        if key not in canonical:
            continue
        item = _safe_json_value(canonical[key], filter_keys=True)
        if item is not _DROP:
            result[key] = item

    option_models = {
        'security': SecurityOptions,
        'storage': StorageOptions,
        'deathstarbench': DeathStarBenchOptions,
        'apachebench': ApacheBenchOptions,
        'sysbench': SysbenchOptions,
        'iperf3': Iperf3Options,
        'phoronix': PhoronixOptions,
    }
    for key, model in option_models.items():
        options, valid = _validated_options(model, canonical.get(key))
        if valid:
            result[key] = options

    result.setdefault('provider', 'oci')
    result.setdefault('benchmarks', [])
    result.setdefault('llm_benchmarks', [])
    return result


def _profile_for_result(result_id: str) -> dict[str, Any] | None:
    profile_id = result_id.removeprefix('phoronix_')
    return next(
        (item for item in PHORONIX_PROFILES if item['id'] == profile_id),
        None,
    )


def _storage_target_context(
    metadata: Mapping[str, Any],
    issues: list[str],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Return the versioned storage method and observed target provenance.

    Results without a marker predate automatic local-NVMe selection.  Their
    exact historical contract remains the additional ``/data`` volume so
    existing fingerprints and comparison links stay stable.  Once a marker
    is present, incomplete or contradictory metadata fails closed rather
    than being mistaken for a legacy result.
    """

    marker = metadata.get('storage_target_contract')
    has_storage_target_metadata = any(
        str(key).startswith('storage_target_') for key in metadata
    )
    if marker is None and not has_storage_target_metadata:
        return {'data_path': 'additional_volume'}, None

    settings = {
        'data_path': 'selected_benchmark_storage',
        'storage_target_contract': STORAGE_TARGET_CONTRACT,
        'storage_target_policy': STORAGE_TARGET_POLICY,
        'filesystem': 'xfs',
        'layout_policy': STORAGE_TARGET_LAYOUT,
    }
    if marker is None:
        issues.append(
            'The storage target contract marker is missing from otherwise '
            'versioned storage metadata.'
        )
    elif marker != STORAGE_TARGET_CONTRACT:
        issues.append(
            'The storage target contract marker is unsupported.'
        )

    policy = metadata.get('storage_target_policy')
    if policy != STORAGE_TARGET_POLICY:
        issues.append(
            'The storage target selection policy is missing or unsupported.'
        )

    kind = metadata.get('storage_target_kind')
    if not isinstance(kind, str) or kind not in STORAGE_TARGET_KINDS:
        issues.append(
            'The observed storage target kind is missing or unsupported.'
        )

    verification = metadata.get('storage_target_verification')
    expected_verification = (
        STORAGE_TARGET_VERIFICATION.get(kind)
        if isinstance(kind, str)
        else None
    )
    if not expected_verification or verification != expected_verification:
        issues.append(
            'The observed storage target verification is missing or '
            'inconsistent with its kind.'
        )

    layout = metadata.get('storage_target_layout')
    if layout != STORAGE_TARGET_LAYOUT:
        issues.append(
            'The observed storage target layout is missing or unsupported.'
        )

    device_count = metadata.get('storage_target_device_count')
    if (
        isinstance(device_count, bool)
        or not isinstance(device_count, int)
        or device_count != 1
    ):
        issues.append(
            'The observed storage target must contain exactly one device.'
        )

    filesystem = metadata.get('storage_target_filesystem')
    if filesystem != 'xfs':
        issues.append(
            'The observed storage target filesystem must be XFS.'
        )

    mount_point = metadata.get('storage_target_mount_point')
    expected_mount_point = (
        '/benchmark-local'
        if kind == 'instance_local_nvme'
        else '/data'
    )
    if (
        not isinstance(mount_point, str)
        or mount_point not in STORAGE_TARGET_MOUNT_POINTS
        or mount_point != expected_mount_point
    ):
        issues.append(
            'The observed storage target mount point is missing or '
            'inconsistent with its kind.'
        )

    capacity_bytes = metadata.get('storage_target_capacity_bytes')
    if (
        isinstance(capacity_bytes, bool)
        or not isinstance(capacity_bytes, int)
        or capacity_bytes <= 0
    ):
        issues.append(
            'The observed storage target capacity must be a positive integer.'
        )

    transport = metadata.get('storage_target_transport')
    if (
        not isinstance(transport, str)
        or transport not in STORAGE_TARGET_TRANSPORTS
    ):
        issues.append(
            'The observed storage target transport is missing or unsupported.'
        )
    elif kind == 'instance_local_nvme' and transport != 'nvme':
        issues.append(
            'An instance-local storage target must use the NVMe transport.'
        )

    raw_model = metadata.get('storage_target_model')
    model = (
        _clean_text(raw_model.strip(), 256).strip()
        if isinstance(raw_model, str)
        else ''
    )
    if not model:
        issues.append(
            'The observed storage target model is missing or must be '
            'non-empty text.'
        )

    target = {
        'kind': kind,
        'verification': verification,
        'transport': transport,
        'capacity_bytes': capacity_bytes,
        'device_count': device_count,
        'layout': layout,
        'filesystem': filesystem,
        'mount_point': mount_point,
    }
    if model:
        target['model'] = model
    # Invalid descriptors are never exposed as verified provenance.  Keeping
    # their raw structural values out of later cohort processing also ensures
    # a damaged artifact is excluded rather than raising while warnings are
    # assembled.
    return settings, None if issues else {'storage_target': target}


def _deathstarbench_execution_context(
    options: Mapping[str, Any],
    metadata: Mapping[str, Any],
    issues: list[str],
) -> dict[str, Any]:
    """Return the attested DeathStarBench topology/runtime identity.

    Historical results predate explicit execution metadata.  The only safe
    compatibility inference for those artifacts is the original single-host
    Podman Compose implementation.  Any distributed result, or any result
    that starts emitting the versioned markers, must attest the complete
    identity, registered topology/placement revisions, and an exact runtime
    revision.  Runtime revisions are intentionally not required to equal the
    profile's current revision: keeping the recorded value in the fingerprint
    preserves comparison support for older, fully attested runtime builds.
    """

    topology_id = options.get(
        'topology_id',
        DEATHSTARBENCH_DEFAULT_TOPOLOGY_ID,
    )
    runtime_id = options.get(
        'runtime_id',
        DEATHSTARBENCH_DEFAULT_RUNTIME_ID,
    )
    marker_keys = (
        'topology_id',
        'topology_revision',
        'runtime_id',
        'runtime_revision',
        'service_placement_revision',
    )
    present_marker_keys = tuple(key for key in marker_keys if key in metadata)

    try:
        profile = runtime_profile(topology_id, runtime_id)
    except ValueError:
        profile = None
        issues.append(
            'The planned DeathStarBench topology/runtime profile is not '
            'registered.'
        )

    distributed_identity: dict[str, Any] = {}
    if topology_id == DISTRIBUTED_TIERED_TOPOLOGY_ID:
        identity_keys = (
            'workload_revision',
            'image_set_revision',
            'image_lock_fingerprint',
            'rendered_manifest_sha256',
            'dataset_revision',
            'load_driver_revision',
            'measurement_revision',
            'initializer_sha256',
            'dataset_nodes_sha256',
            'dataset_edges_sha256',
            'load_script_sha256',
        )
        for key in identity_keys:
            raw = metadata.get(key)
            value = (
                _clean_text(raw, 256).strip()
                if isinstance(raw, str)
                else ''
            )
            distributed_identity[key] = value or None
            if not value:
                issues.append(
                    'DeathStarBench distributed execution identity is '
                    f'missing or invalid: {key}.'
                )
                continue
            if key in {
                'image_lock_fingerprint',
                'rendered_manifest_sha256',
            } and not re.fullmatch(r'sha256:[0-9a-f]{64}', value):
                issues.append(
                    'DeathStarBench distributed execution identity has an '
                    f'invalid prefixed SHA-256: {key}.'
                )
            if key in {
                'initializer_sha256',
                'dataset_nodes_sha256',
                'dataset_edges_sha256',
                'load_script_sha256',
            } and not re.fullmatch(r'[0-9a-f]{64}', value):
                issues.append(
                    'DeathStarBench distributed execution identity has an '
                    f'invalid SHA-256: {key}.'
                )

    if not present_marker_keys:
        if (
            topology_id == DEATHSTARBENCH_DEFAULT_TOPOLOGY_ID
            and runtime_id == DEATHSTARBENCH_DEFAULT_RUNTIME_ID
            and profile is not None
        ):
            topology_revision: str | None = profile.topology_revision
            runtime_revision: str | None = profile.runtime_revision
            placement_revision: str | None = profile.placement_revision
        else:
            topology_revision = None
            runtime_revision = None
            placement_revision = None
            issues.append(
                'DeathStarBench distributed topology/runtime metadata is '
                'missing.'
            )
        return {
            'topology_id': topology_id,
            'topology_revision': topology_revision,
            'runtime_id': runtime_id,
            'runtime_revision': runtime_revision,
            'service_placement_revision': placement_revision,
            **distributed_identity,
        }

    if len(present_marker_keys) != len(marker_keys):
        missing = [key for key in marker_keys if key not in metadata]
        issues.append(
            'The complete DeathStarBench execution attestation is required; '
            'missing: ' + ', '.join(missing) + '.'
        )

    reported_topology_id = metadata.get('topology_id')
    if not isinstance(reported_topology_id, str) or not reported_topology_id:
        issues.append(
            'The DeathStarBench topology identity is missing or invalid.'
        )
    elif reported_topology_id != topology_id:
        issues.append(
            'The reported DeathStarBench topology does not match the plan.'
        )

    raw_topology_revision = metadata.get('topology_revision')
    topology_revision = (
        _clean_text(raw_topology_revision, 256).strip()
        if isinstance(raw_topology_revision, str)
        else ''
    )
    if not topology_revision:
        issues.append(
            'The DeathStarBench topology revision is missing or invalid.'
        )
    elif (
        profile is not None
        and topology_revision != profile.topology_revision
    ):
        issues.append(
            'The reported DeathStarBench topology revision does not match '
            'the registered topology/runtime profile.'
        )

    reported_runtime_id = metadata.get('runtime_id')
    if not isinstance(reported_runtime_id, str) or not reported_runtime_id:
        issues.append(
            'The DeathStarBench runtime identity is missing or invalid.'
        )
    elif reported_runtime_id != runtime_id:
        issues.append(
            'The reported DeathStarBench runtime does not match the plan.'
        )

    raw_runtime_revision = metadata.get('runtime_revision')
    runtime_revision = (
        _clean_text(raw_runtime_revision, 256).strip()
        if isinstance(raw_runtime_revision, str)
        else ''
    )
    if not runtime_revision:
        issues.append(
            'The exact DeathStarBench runtime revision is missing or invalid.'
        )

    raw_placement_revision = metadata.get('service_placement_revision')
    placement_revision = (
        _clean_text(raw_placement_revision, 256).strip()
        if isinstance(raw_placement_revision, str)
        else ''
    )
    if not placement_revision:
        issues.append(
            'The DeathStarBench service placement revision is missing or '
            'invalid.'
        )
    elif (
        profile is not None
        and placement_revision != profile.placement_revision
    ):
        issues.append(
            'The reported DeathStarBench service placement revision does '
            'not match the registered topology/runtime profile.'
        )

    return {
        'topology_id': topology_id,
        'topology_revision': topology_revision or None,
        'runtime_id': runtime_id,
        'runtime_revision': runtime_revision or None,
        'service_placement_revision': placement_revision or None,
        **distributed_identity,
    }


def workload_fingerprint(
    result_id: str,
    plan: Mapping[str, Any] | object,
    metadata: Mapping[str, Any] | object | None = None,
) -> dict[str, Any]:
    """Build a hashable workload contract without host addresses or commands.

    Hardware identity (provider, shape, CPU count, memory, architecture, and
    storage class) is intentionally not part of the fingerprint: those are the
    dimensions users compare.  Workload size, concurrency, duration, pinned
    artifacts, and protocol settings are part of it.
    """

    result_id = _clean_text(result_id, 256)
    safe_plan = canonical_plan(plan)
    safe_metadata = _safe_json_value(metadata or {}, filter_keys=True)
    safe_metadata = safe_metadata if isinstance(safe_metadata, dict) else {}
    provider = str(safe_plan.get('provider') or 'oci').lower()
    issues: list[str] = []
    settings: dict[str, Any] | None = None
    provenance: dict[str, Any] | None = None

    if result_id == 'sysbench_cpu':
        settings = {
            'tool': 'sysbench',
            'workload': 'cpu',
            'duration_seconds': 60,
            'maximum_prime': 10000,
            'threads_policy': 'all_configured_vcpus',
        }
    elif result_id == 'sysbench_memory':
        settings = {
            'tool': 'sysbench',
            'workload': 'memory_sequential_write',
            'duration_seconds': 60,
            'block_size_mib': 1,
            'threads_policy': 'all_configured_vcpus',
        }
    elif result_id == 'sysbench_fileio':
        settings = {
            'tool': 'sysbench',
            'workload': 'fileio_random_read_write',
            'duration_seconds': 60,
            'file_total_size_gib': 4,
        }
        storage_settings, provenance = _storage_target_context(
            safe_metadata,
            issues,
        )
        settings.update(storage_settings)
    elif result_id == 'stream':
        settings = {
            'tool': 'STREAM',
            'kernels': ['copy', 'scale', 'add', 'triad'],
            'threads_policy': 'all_configured_vcpus',
            'array_policy': 'at_least_4x_llc_with_50_percent_memory_cap',
            'compiler_flags': '-O3 -fopenmp',
            'source_contract': 'checksum_pinned_stream_v1',
        }
    elif result_id == 'fio':
        settings = {
            'tool': 'fio',
            'workloads': ['read', 'write', 'randread', 'randwrite'],
            'block_sizes': {
                'read': '1MiB',
                'write': '1MiB',
                'randread': '4KiB',
                'randwrite': '4KiB',
            },
            'size_gib': 4,
            'duration_seconds_each': 60,
            'direct_io': True,
            'time_based': True,
        }
        storage_settings, provenance = _storage_target_context(
            safe_metadata,
            issues,
        )
        settings.update(storage_settings)
    elif result_id in {'iperf_tcp', 'iperf_udp', 'iperf_sctp'}:
        protocol = result_id.removeprefix('iperf_')
        settings = {
            'tool': 'iperf3',
            'protocol': protocol,
            'duration_seconds': 60,
            'parallel_streams': 1 if protocol == 'udp' else 4,
            'address_family': 'ipv4',
            'traffic_path': 'private_same_location_peer',
        }
        if protocol == 'udp':
            settings['offered_load'] = 'unlimited'
    elif result_id.startswith('apachebench_'):
        mode = result_id.removeprefix('apachebench_')
        options, valid = _validated_options(
            ApacheBenchOptions,
            safe_plan.get('apachebench'),
        )
        if mode not in {'new_connections', 'keep_alive'} or not valid:
            issues.append('ApacheBench workload options are missing or invalid.')
        settings = {
            'tool': 'ApacheBench',
            'connection_mode': mode,
            'request_count': options.get('request_count'),
            'concurrency': options.get('concurrency'),
            'response_size_kib': options.get('response_size_kib'),
            'warmup_requests': options.get('warmup_requests'),
            'measured_trials': options.get('trials'),
            'server_mpm': safe_metadata.get('httpd_mpm', 'event'),
            'server_keep_alive': safe_metadata.get('httpd_keep_alive', 'On'),
        }
    elif result_id == 'deathstarbench':
        options, valid = _validated_options(
            DeathStarBenchOptions,
            safe_plan.get('deathstarbench'),
        )
        upstream_revision = safe_metadata.get('upstream_revision')
        if not valid or not upstream_revision:
            issues.append(
                'DeathStarBench options or its pinned upstream revision are '
                'missing.'
            )
        settings = {
            'tool': 'DeathStarBench/wrk2',
            'workload': options.get('workload'),
            'warmup_seconds': options.get('warmup_seconds'),
            'duration_seconds': options.get('duration_seconds'),
            'threads': options.get('threads'),
            'connections': options.get('connections'),
            'request_rate': options.get('request_rate'),
            'upstream_revision': upstream_revision,
            'latency_distribution': 'exponential',
        }
        settings.update(
            _deathstarbench_execution_context(
                options,
                safe_metadata,
                issues,
            )
        )
    elif result_id == 'llama_bench':
        required = (
            'llama_cpp_revision',
            'model_revision',
            'model_sha256',
            'model_quantization',
            'execution_backend',
            'llama_compiler_version',
            'llama_cxx_compiler_version',
            'llama_assembler_version',
        )
        missing = [key for key in required if not safe_metadata.get(key)]
        native_cpu_optimization = safe_metadata.get(
            'llama_native_optimization'
        )
        if not isinstance(native_cpu_optimization, bool):
            missing.append('llama_native_optimization')
        raw_cpu_build_profile = safe_metadata.get('llama_cpu_build_profile')
        cpu_build_profile = (
            raw_cpu_build_profile.strip()
            if isinstance(raw_cpu_build_profile, str)
            and raw_cpu_build_profile.strip()
            else None
        )
        if not cpu_build_profile and native_cpu_optimization is True:
            # Results produced before named CPU profiles were introduced used
            # the same native-build contract on every supported architecture.
            cpu_build_profile = 'native'
        if not cpu_build_profile:
            missing.append('llama_cpu_build_profile')
        if missing:
            issues.append(
                'llama.cpp artifact or toolchain metadata is missing: '
                + ', '.join(missing)
            )
        elif (
            cpu_build_profile,
            native_cpu_optimization,
        ) not in CPU_BUILD_PROFILE_NATIVE_PAIRS:
            issues.append(
                'llama.cpp CPU build profile and native-optimization '
                'metadata are inconsistent.'
            )
        raw_architecture = str(
            safe_metadata.get('architecture') or ''
        ).strip().lower()
        architecture = {
            'amd64': 'x86_64',
            'x64': 'x86_64',
            'arm64': 'aarch64',
        }.get(raw_architecture, raw_architecture)
        if (
            not architecture
            and cpu_build_profile == X86_64_PORTABLE_CPU_PROFILE
        ):
            # The versioned profile is unambiguously x86_64 and was recorded
            # before architecture became part of build provenance in a small
            # number of otherwise current artifacts.
            architecture = 'x86_64'
        if architecture and architecture not in {'x86_64', 'aarch64'}:
            issues.append(
                'llama.cpp architecture metadata is unsupported.'
            )
        raw_cpu_cmake_options = safe_metadata.get(
            'llama_cpu_cmake_options'
        )
        cpu_cmake_options = (
            raw_cpu_cmake_options.strip()
            if isinstance(raw_cpu_cmake_options, str)
            and raw_cpu_cmake_options.strip()
            else None
        )
        expected_cpu_cmake_options = None
        if (
            architecture == 'x86_64'
            and cpu_build_profile == X86_64_PORTABLE_CPU_PROFILE
            and native_cpu_optimization is False
        ):
            expected_cpu_cmake_options = ' '.join(
                X86_64_PORTABLE_CMAKE_OPTIONS
            )
        elif (
            architecture == 'aarch64'
            and cpu_build_profile == AARCH64_NATIVE_CPU_PROFILE
            and native_cpu_optimization is True
        ):
            expected_cpu_cmake_options = ' '.join(
                AARCH64_NATIVE_CMAKE_OPTIONS
            )
        elif architecture == 'aarch64' or (
            architecture == 'x86_64'
            and cpu_build_profile != AARCH64_NATIVE_CPU_PROFILE
        ):
            issues.append(
                'llama.cpp CPU build profile is not valid for the recorded '
                'architecture.'
            )
        if (
            expected_cpu_cmake_options is not None
            and cpu_cmake_options != expected_cpu_cmake_options
        ):
            issues.append(
                'llama.cpp architecture-specific CPU build options are '
                'missing or inconsistent.'
            )
        architecture_profile_method = (
            expected_cpu_cmake_options is not None
            and cpu_cmake_options == expected_cpu_cmake_options
        )
        settings = {
            'tool': 'llama-bench',
            'llama_cpp_revision': safe_metadata.get('llama_cpp_revision'),
            'model_revision': safe_metadata.get('model_revision'),
            'model_sha256': safe_metadata.get('model_sha256'),
            'model_quantization': safe_metadata.get('model_quantization'),
            'execution_backend': safe_metadata.get('execution_backend'),
            'compiler': 'GCC',
            'cxx_compiler': 'G++',
            'assembler': 'GNU as',
            'prompt_token_counts': [512, 2048],
            'generation_token_counts': [128, 512],
            'repetitions': 5,
            'threads_policy': 'all_observed_guest_logical_cpus',
            'gpu_layers': 0,
        }
        if architecture_profile_method:
            # The supported x86_64 and Arm64 builds deliberately use
            # different compiler flags, but they implement the same CPU-only
            # benchmark method for their respective architectures.  The
            # concrete build remains visible as non-hashed provenance.
            settings['cpu_build_method'] = LLAMA_ARCHITECTURE_PROFILE_METHOD
        else:
            # Preserve conservative compatibility for historical builds that
            # predate the validated architecture profiles.  Their exact
            # architecture, build profile, options, and toolchain remain part
            # of the workload contract and cannot join the current cohort.
            settings.update({
                'architecture': architecture or None,
                'cpu_build_profile': cpu_build_profile,
                'native_cpu_optimization': native_cpu_optimization,
                'cpu_cmake_options': cpu_cmake_options,
                'compiler_version': safe_metadata.get(
                    'llama_compiler_version'
                ),
                'cxx_compiler_version': safe_metadata.get(
                    'llama_cxx_compiler_version'
                ),
                'assembler_version': safe_metadata.get(
                    'llama_assembler_version'
                ),
            })
        # Compiler, binutils, and architecture-specific build details remain
        # required, auditable provenance rather than workload parameters for
        # the validated cloud CPU method.  Comparison payloads call out mixed
        # architectures and toolchains explicitly.
        provenance = {
            'cpu_build': {
                'architecture': architecture or None,
                'profile': cpu_build_profile,
                'native_cpu_optimization': native_cpu_optimization,
                'cmake_options': cpu_cmake_options,
            },
            'build_toolchain': {
                'compiler': 'GCC',
                'compiler_version': safe_metadata.get(
                    'llama_compiler_version'
                ),
                'cxx_compiler': 'G++',
                'cxx_compiler_version': safe_metadata.get(
                    'llama_cxx_compiler_version'
                ),
                'assembler': 'GNU as',
                'assembler_version': safe_metadata.get(
                    'llama_assembler_version'
                ),
            },
        }
    elif result_id.startswith('phoronix_'):
        profile = _profile_for_result(result_id)
        expected_profile = profile['profile'] if profile else None
        recorded_profile = safe_metadata.get('phoronix_profile')
        client_revision = safe_metadata.get('phoronix_client_revision')
        measured_trials = safe_metadata.get('measured_trials')
        architecture = str(safe_metadata.get('architecture') or '').strip().lower()
        architecture = {
            'amd64': 'x86_64',
            'x64': 'x86_64',
            'arm64': 'aarch64',
        }.get(architecture, architecture)
        if (
            not profile
            or recorded_profile != expected_profile
            or not client_revision
            or not isinstance(measured_trials, int)
            or measured_trials <= 0
        ):
            issues.append(
                'Phoronix profile, client revision, or trial-count metadata '
                'is missing or inconsistent.'
            )
        if (
            result_id == 'phoronix_build_linux_kernel'
            and architecture not in {'x86_64', 'aarch64'}
        ):
            issues.append(
                'The architecture is required for the architecture-specific '
                'Linux kernel defconfig workload.'
            )
        settings = {
            'tool': 'Phoronix Test Suite',
            'profile': expected_profile,
            'client_revision': client_revision,
            'fixed_options': safe_metadata.get('fixed_options'),
            'measured_trials': measured_trials,
        }
        if result_id == 'phoronix_build_linux_kernel':
            settings['architecture'] = architecture or None
    else:
        issues.append(f'No comparison contract is registered for {result_id}.')

    # OCI's original scalar benchmark path did not use the pinned parsers and
    # deterministic commands now shared by AWS, GCP, and Azure.  Runtime
    # metadata
    # marker lets the parity-normalized OCI path opt in without making old
    # artifacts appear comparable retroactively.
    if (
        provider == 'oci'
        and result_id in {
            'sysbench_cpu',
            'sysbench_memory',
            'sysbench_fileio',
            'stream',
            'fio',
        }
        and safe_metadata.get('portable_result_contract') != 'v1'
    ):
        issues.append(
            'The OCI result predates the portable parsed workload contract.'
        )

    contract = None
    fingerprint = None
    if settings is not None:
        contract = {
            'schema_version': WORKLOAD_CONTRACT_VERSION,
            'result_id': result_id,
            'settings': settings,
        }
    if contract is not None and not issues:
        encoded = json.dumps(
            contract,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        ).encode()
        fingerprint = hashlib.sha256(encoded).hexdigest()
    result = {
        'schema_version': WORKLOAD_CONTRACT_VERSION,
        'fingerprint': fingerprint,
        'contract_unknown': fingerprint is None,
        'contract': contract,
        'issues': issues,
    }
    if provenance is not None:
        result['provenance'] = provenance
    return result


def _run_record(job: Mapping[str, Any]) -> dict[str, Any]:
    record = {}
    for key in _RUN_FIELDS:
        if key not in job:
            continue
        value = job[key]
        if key in {'error', 'cleanup_error'}:
            if value:
                record[key] = _clean_text(value)
            continue
        item = _safe_json_value(value, filter_keys=True)
        if item is not _DROP:
            record[key] = item
    return record


def build_results_artifact(job: Mapping[str, Any] | object) -> dict[str, Any]:
    """Build the complete JSON-safe results artifact for a run."""

    source = _mapping(job)
    plan = canonical_plan(source.get('plan', {}))
    results = []
    for raw_result in source.get('results') or []:
        sanitized = sanitize_result(raw_result)
        result_id = sanitized.get('id')
        if result_id:
            sanitized['comparison'] = workload_fingerprint(
                result_id,
                plan,
                sanitized.get('metadata'),
            )
        results.append(sanitized)
    generated_at = source.get('updated_at') or datetime.now(timezone.utc).isoformat()
    return {
        '$schema': RESULTS_SCHEMA,
        'schema_version': RESULTS_SCHEMA_VERSION,
        'generated_at': _clean_text(generated_at, 128),
        'provenance': 'results_artifact',
        'contract_unknown': any(
            result.get('comparison', {}).get('contract_unknown', True)
            for result in results
        ),
        'run': _run_record(source),
        'plan': plan,
        'results': results,
    }


def _results_path(path: str | os.PathLike[str]) -> Path:
    value = Path(path)
    return value if value.name == RESULTS_ARTIFACT_NAME else value / RESULTS_ARTIFACT_NAME


def write_results_artifact(
    run_directory: str | os.PathLike[str],
    job: Mapping[str, Any] | object,
) -> Path:
    """Atomically write ``results.json`` and return its path."""

    path = _results_path(run_directory)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f'.results-{uuid.uuid4().hex}.tmp'
    document = build_results_artifact(job)
    try:
        temporary.write_text(
            json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False)
            + '\n'
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def read_results_artifact(
    path: str | os.PathLike[str],
) -> dict[str, Any]:
    """Read and validate a normalized results document."""

    artifact_path = _results_path(path)
    try:
        raw = json.loads(artifact_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f'Unable to read {artifact_path.name}: {exc}') from exc
    return _normalize_results_document(raw)


def _normalize_results_document(raw: object) -> dict[str, Any]:
    """Validate and re-sanitize an artifact read from any trusted container."""

    if not isinstance(raw, dict):
        raise ValueError('The results artifact must contain a JSON object.')
    if raw.get('$schema') != RESULTS_SCHEMA:
        raise ValueError('The results artifact has an unknown schema.')
    if raw.get('schema_version') != RESULTS_SCHEMA_VERSION:
        raise ValueError('The results artifact has an unsupported version.')
    if not isinstance(raw.get('run'), Mapping):
        raise ValueError('The results artifact is missing its run record.')
    if not isinstance(raw.get('results'), list):
        raise ValueError('The results artifact is missing its result list.')

    plan = canonical_plan(raw.get('plan', {}))
    results = []
    for raw_result in raw['results']:
        if not isinstance(raw_result, Mapping):
            continue
        result = sanitize_result(raw_result)
        result_id = result.get('id')
        if result_id:
            result['comparison'] = workload_fingerprint(
                result_id,
                plan,
                result.get('metadata'),
            )
        results.append(result)
    return {
        '$schema': RESULTS_SCHEMA,
        'schema_version': RESULTS_SCHEMA_VERSION,
        'generated_at': _clean_text(raw.get('generated_at', ''), 128),
        'provenance': 'results_artifact',
        'contract_unknown': any(
            result.get('comparison', {}).get('contract_unknown', True)
            for result in results
        ),
        'run': _run_record(raw['run']),
        'plan': plan,
        'results': results,
    }


def metric_specs(result_id: str) -> list[dict[str, Any]]:
    """Return JSON-ready metric definitions for a canonical result ID."""

    return [
        {key: value for key, value in asdict(spec).items() if value is not None}
        for spec in METRIC_REGISTRY.get(str(result_id), ())
    ]


def _finite_number(value: object) -> float | int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(float(value)):
        return None
    return value


def _direction(value: object) -> str | None:
    normalized = str(value or '').strip().lower().replace(' ', '_')
    return {
        'hib': DIRECTION_HIGHER,
        'higher_is_better': DIRECTION_HIGHER,
        'lib': DIRECTION_LOWER,
        'lower_is_better': DIRECTION_LOWER,
    }.get(normalized)


def _population_stddev(values: object) -> float | None:
    if not isinstance(values, list) or len(values) < 2:
        return None
    numbers = [_finite_number(value) for value in values]
    if any(value is None for value in numbers):
        return None
    return statistics.pstdev(float(value) for value in numbers if value is not None)


def _phoronix_chart_metrics(
    result_id: str,
    metrics: Mapping[str, Any],
) -> list[dict[str, Any]]:
    spec = METRIC_REGISTRY[result_id][0]
    raw_measurements = metrics.get('measurements')
    measurements = (
        raw_measurements
        if isinstance(raw_measurements, list)
        else [metrics]
        if 'score' in metrics
        else []
    )
    chart_metrics = []
    for measurement in measurements:
        if not isinstance(measurement, Mapping):
            continue
        value = _finite_number(measurement.get('score'))
        configuration = str(measurement.get('configuration') or '').strip()
        unit = str(measurement.get('unit') or '').strip()
        direction = _direction(measurement.get('direction'))
        if value is None or not configuration or not unit or direction is None:
            continue
        identity = json.dumps(
            {
                'profile': measurement.get('profile'),
                'configuration': configuration,
            },
            sort_keys=True,
            separators=(',', ':'),
        )
        key = f'score_{hashlib.sha256(identity.encode()).hexdigest()[:12]}'
        uncertainty = _population_stddev(measurement.get('measured_trials'))
        chart_metrics.append({
            'key': key,
            'label': f'{spec.label} — {configuration}',
            'unit': unit,
            'direction': direction,
            'primary': True,
            'subtest': configuration,
            'value': value,
            'uncertainty': uncertainty,
            'uncertainty_kind': (
                'standard_deviation' if uncertainty is not None else None
            ),
        })
    return chart_metrics


def extract_chart_metrics(
    result: Mapping[str, Any] | object,
) -> list[dict[str, Any]]:
    """Extract only registered, finite numeric metrics from one result."""

    value = sanitize_result(result)
    if value.get('status') != 'completed':
        return []
    result_id = str(value.get('id') or '')
    metrics = value.get('metrics')
    if result_id not in METRIC_REGISTRY or not isinstance(metrics, Mapping):
        return []
    if result_id.startswith('phoronix_'):
        return _phoronix_chart_metrics(result_id, metrics)

    chart_metrics = []
    for spec in METRIC_REGISTRY[result_id]:
        number = _finite_number(metrics.get(spec.key))
        if number is None:
            continue
        uncertainty = (
            _finite_number(metrics.get(spec.uncertainty_key))
            if spec.uncertainty_key
            else None
        )
        chart_metrics.append({
            'key': spec.key,
            'label': spec.label,
            'unit': spec.unit,
            'direction': spec.direction,
            'primary': spec.primary,
            'subtest': spec.subtest,
            'value': number,
            'uncertainty': uncertainty,
            'uncertainty_kind': (
                spec.uncertainty_kind if uncertainty is not None else None
            ),
        })
    return chart_metrics


def _flatten_contract(value: object, prefix: str = '') -> dict[str, object]:
    if isinstance(value, Mapping):
        flattened = {}
        for key in sorted(value):
            path = f'{prefix}.{key}' if prefix else str(key)
            flattened.update(_flatten_contract(value[key], path))
        return flattened
    if isinstance(value, list):
        return {prefix: tuple(value)}
    return {prefix: value}


def explain_workload_mismatch(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Return stable field-level differences between two fingerprint records."""

    if left.get('contract_unknown') or right.get('contract_unknown'):
        return [{
            'path': 'contract',
            'left': 'unknown' if left.get('contract_unknown') else 'known',
            'right': 'unknown' if right.get('contract_unknown') else 'known',
        }]
    left_values = _flatten_contract(left.get('contract'))
    right_values = _flatten_contract(right.get('contract'))
    differences = []
    for path in sorted(set(left_values) | set(right_values)):
        if left_values.get(path) != right_values.get(path):
            differences.append({
                'path': path,
                'left': left_values.get(path),
                'right': right_values.get(path),
            })
    return differences


def _run_id(document: Mapping[str, Any], index: int) -> str:
    run = document.get('run')
    if isinstance(run, Mapping) and run.get('id'):
        return str(run['id'])
    return f'unknown-run-{index + 1}'


def _run_descriptor(document: Mapping[str, Any], index: int) -> dict[str, Any]:
    run = _mapping(document.get('run'))
    plan = canonical_plan(document.get('plan', {}))
    metadata = {}
    for result in document.get('results') or []:
        if isinstance(result, Mapping) and isinstance(result.get('metadata'), Mapping):
            metadata = result['metadata']
            break
    provider = plan.get('provider') or metadata.get('provider')
    shape = (
        plan.get('shape')
        or metadata.get('shape')
        or metadata.get('instance_type')
        or metadata.get('machine_type')
        or metadata.get('vm_size')
    )
    cpu_count = plan.get('ocpus') or metadata.get('ocpus') or metadata.get('vcpus')
    return {
        'id': _run_id(document, index),
        'provider': provider,
        'shape': shape,
        'architecture': metadata.get('architecture'),
        'ocpus': cpu_count,
        'cpu_count': cpu_count,
        'memory_gb': plan.get('memory_gb') or metadata.get('memory_gb'),
        'region': plan.get('region') or metadata.get('region'),
        'gcp_zone': plan.get('gcp_zone'),
        'azure_zone': plan.get('azure_zone'),
        'status': run.get('status'),
        'created_at': run.get('created_at'),
        'benchmark_status': run.get('benchmark_status'),
        'provenance': document.get('provenance', 'unknown'),
    }


def _mismatch_message(differences: list[dict[str, Any]]) -> str:
    if not differences:
        return 'The workload contracts do not match.'
    if differences[0]['path'] == 'contract':
        return 'At least one run lacks a verified workload contract.'
    fields = ', '.join(
        difference['path'].removeprefix('settings.')
        for difference in differences[:4]
    )
    suffix = '' if len(differences) <= 4 else ', and more'
    return f'Workload settings differ: {fields}{suffix}.'


def _llama_toolchain_description(provenance: object) -> str:
    """Return a concise, display-safe compiler/binutils provenance label."""

    record = _mapping(provenance)
    toolchain = _mapping(record.get('build_toolchain'))
    compiler = str(toolchain.get('compiler') or 'GCC')
    compiler_version = str(toolchain.get('compiler_version') or 'unknown')
    cxx_compiler = str(toolchain.get('cxx_compiler') or 'G++')
    cxx_version = str(
        toolchain.get('cxx_compiler_version') or 'unknown'
    )
    assembler = str(toolchain.get('assembler') or 'GNU as')
    assembler_version = str(
        toolchain.get('assembler_version') or 'unknown'
    )
    if compiler_version == cxx_version:
        compiler_label = (
            f'{compiler}/{cxx_compiler} {compiler_version}'
        )
    else:
        compiler_label = (
            f'{compiler} {compiler_version}, '
            f'{cxx_compiler} {cxx_version}'
        )
    return f'{compiler_label}; {assembler} {assembler_version}'


def _llama_cpu_build_description(provenance: object) -> str:
    """Return a concise architecture/profile provenance label."""

    record = _mapping(provenance)
    cpu_build = _mapping(record.get('cpu_build'))
    architecture = str(cpu_build.get('architecture') or 'unknown')
    profile = str(cpu_build.get('profile') or 'unknown')
    return f'{architecture} · {profile}'


def _add_llama_provenance_warnings(
    result_id: str,
    cohort_entries: Sequence[dict[str, Any]],
) -> list[str]:
    """Expose architecture and toolchain differences without hiding results."""

    if result_id != 'llama_bench' or len(cohort_entries) < 2:
        return []
    toolchain_signatures = {
        json.dumps(
            _mapping(entry.get('provenance')).get('build_toolchain', {}),
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        )
        for entry in cohort_entries
    }
    cpu_build_signatures = {
        json.dumps(
            _mapping(entry.get('provenance')).get('cpu_build', {}),
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        )
        for entry in cohort_entries
    }
    mixed_toolchains = len(toolchain_signatures) > 1
    mixed_cpu_builds = len(cpu_build_signatures) > 1
    if not mixed_toolchains and not mixed_cpu_builds:
        return []
    for entry in cohort_entries:
        details = []
        if mixed_cpu_builds:
            details.append(
                'CPU build: '
                + _llama_cpu_build_description(entry.get('provenance'))
            )
        if mixed_toolchains:
            details.append(
                'Build toolchain: '
                + _llama_toolchain_description(entry.get('provenance'))
            )
        entry.setdefault('warnings', []).append('; '.join(details) + '.')
    warnings = []
    if mixed_cpu_builds:
        warnings.append(
            'These runs use the same pinned workload with validated '
            'architecture-appropriate CPU builds. The x86_64 portable AVX2 '
            'and Arm64 native binaries are architecture-specific, so treat '
            'this as a platform-level comparison rather than a bit-identical '
            'binary comparison.'
        )
    if mixed_toolchains:
        warnings.append(
            'Build toolchains differ across these runs; compiler code '
            'generation may affect performance.'
        )
    return warnings


def _storage_capacity_description(value: object) -> str | None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value <= 0
    ):
        return None
    gibibytes = value / (1024 ** 3)
    if gibibytes >= 1024:
        amount = gibibytes / 1024
        number = f'{amount:.2f}'.rstrip('0').rstrip('.')
        return f'{number} TiB'
    number = f'{gibibytes:.2f}'.rstrip('0').rstrip('.')
    return f'{number} GiB'


def _storage_target_description(provenance: object) -> str:
    """Return a concise, display-safe description of tested storage."""

    target = _mapping(_mapping(provenance).get('storage_target'))
    kind = {
        'instance_local_nvme': 'instance-local NVMe',
        'provisioned_data_volume': 'provisioned data volume',
    }.get(target.get('kind'), 'unknown storage')
    pieces = [kind]
    model = target.get('model')
    if isinstance(model, str) and model.strip():
        pieces.append(model.strip())
    transport = target.get('transport')
    if isinstance(transport, str) and transport.strip():
        pieces.append(
            'NVMe'
            if transport.strip().lower() == 'nvme'
            else transport.strip().replace('_', ' ')
        )
    device_count = target.get('device_count')
    if isinstance(device_count, int) and not isinstance(device_count, bool):
        suffix = '' if device_count == 1 else 's'
        pieces.append(f'{device_count} device{suffix}')
    filesystem = target.get('filesystem')
    if isinstance(filesystem, str) and filesystem.strip():
        pieces.append(filesystem.strip().upper())
    capacity = _storage_capacity_description(target.get('capacity_bytes'))
    if capacity:
        pieces.append(capacity)
    return ' · '.join(pieces)


def _add_storage_provenance_warnings(
    result_id: str,
    cohort_entries: Sequence[dict[str, Any]],
) -> list[str]:
    """Call out mixed tested-storage classes without splitting the cohort."""

    if result_id not in STORAGE_RESULT_IDS or len(cohort_entries) < 2:
        return []
    targets = [
        _mapping(_mapping(entry.get('provenance')).get('storage_target'))
        for entry in cohort_entries
    ]
    if not all(targets):
        # Legacy storage cohorts intentionally have no target provenance and
        # cannot share a fingerprint with the versioned selection method.
        return []
    signatures = {
        (
            target.get('kind'),
            target.get('transport'),
            target.get('layout'),
            target.get('model'),
            target.get('capacity_bytes'),
        )
        for target in targets
    }
    if len(signatures) <= 1:
        return []
    for entry in cohort_entries:
        warning = (
            'Storage target: '
            + _storage_target_description(entry.get('provenance'))
            + '.'
        )
        entry_warnings = entry.setdefault('warnings', [])
        if warning not in entry_warnings:
            entry_warnings.append(warning)
    return [
        'These runs use the same storage workload but different storage '
        'target classes, attachment transports, device models, or capacities; '
        'deltas compare storage hardware and are not compute-only differences.'
    ]


def build_comparison_payload(
    documents: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Group selected result documents into chart-safe workload cohorts."""

    normalized_documents = [document for document in documents if isinstance(document, Mapping)]
    runs = [
        _run_descriptor(document, index)
        for index, document in enumerate(normalized_documents)
    ]
    entries_by_result: dict[str, list[dict[str, Any]]] = {}
    issues: list[dict[str, Any]] = []

    for index, document in enumerate(normalized_documents):
        run_id = _run_id(document, index)
        plan = canonical_plan(document.get('plan', {}))
        for raw_result in document.get('results') or []:
            if not isinstance(raw_result, Mapping):
                continue
            result = sanitize_result(raw_result)
            result_id = str(result.get('id') or '')
            if not result_id:
                issues.append({
                    'run_id': run_id,
                    'reason': 'A result has no canonical result ID.',
                })
                continue
            if result.get('status') != 'completed':
                issues.append({
                    'run_id': run_id,
                    'result_id': result_id,
                    'result_name': result.get('name') or result_id,
                    'reason': f'Result status is {result.get("status") or "unknown"}.',
                })
                continue
            contract = workload_fingerprint(
                result_id,
                plan,
                result.get('metadata'),
            )
            metrics = extract_chart_metrics(result)
            if result_id not in METRIC_REGISTRY:
                issues.append({
                    'run_id': run_id,
                    'result_id': result_id,
                    'result_name': result.get('name') or result_id,
                    'reason': 'No registered chart metrics exist for this result.',
                })
            elif not metrics:
                issues.append({
                    'run_id': run_id,
                    'result_id': result_id,
                    'result_name': result.get('name') or result_id,
                    'reason': 'The result has no valid registered numeric metrics.',
                })
            metric_values = result.get('metrics')
            warnings = []
            if isinstance(metric_values, Mapping):
                for key, value in metric_values.items():
                    if not str(key).endswith('_warning'):
                        continue
                    if isinstance(value, str) and value.strip():
                        warnings.append(value.strip())
                    elif isinstance(value, list):
                        warnings.extend(
                            str(item).strip() for item in value if str(item).strip()
                        )
            entries_by_result.setdefault(result_id, []).append({
                'run_id': run_id,
                'name': result.get('name') or result_id,
                'contract': contract,
                'provenance': contract.get('provenance', {}),
                'metrics': metrics,
                'warnings': list(dict.fromkeys(warnings)),
            })

    groups = []
    mismatches = []
    for result_id, entries in sorted(entries_by_result.items()):
        for left_index, left in enumerate(entries):
            for right in entries[left_index + 1:]:
                if left['contract'].get('fingerprint') == right['contract'].get('fingerprint') and left['contract'].get('fingerprint'):
                    continue
                differences = explain_workload_mismatch(
                    left['contract'],
                    right['contract'],
                )
                mismatches.append({
                    'result_id': result_id,
                    'left_run_id': left['run_id'],
                    'right_run_id': right['run_id'],
                    'differences': differences,
                    'message': _mismatch_message(differences),
                })

        cohorts: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            fingerprint = entry['contract'].get('fingerprint')
            cohort_key = fingerprint or f'unknown:{entry["run_id"]}'
            cohorts.setdefault(cohort_key, []).append(entry)
        for cohort_entries in cohorts.values():
            contract = cohort_entries[0]['contract']
            fingerprint = contract.get('fingerprint')
            provenance_warnings = _add_llama_provenance_warnings(
                result_id,
                cohort_entries,
            )
            provenance_warnings.extend(
                _add_storage_provenance_warnings(
                    result_id,
                    cohort_entries,
                )
            )
            metric_series: dict[tuple[str, str, str, str], dict[str, Any]] = {}
            for entry in cohort_entries:
                for metric in entry['metrics']:
                    identity = (
                        metric['key'],
                        metric['unit'],
                        metric['direction'],
                        str(metric.get('subtest') or ''),
                    )
                    series = metric_series.setdefault(identity, {
                        key: metric.get(key)
                        for key in (
                            'key',
                            'label',
                            'unit',
                            'direction',
                            'primary',
                            'subtest',
                        )
                    })
                    series.setdefault('values', []).append({
                        'run_id': entry['run_id'],
                        'value': metric['value'],
                        'uncertainty': metric.get('uncertainty'),
                        'uncertainty_kind': metric.get('uncertainty_kind'),
                        'warnings': entry.get('warnings', []),
                        'provenance': entry.get('provenance', {}),
                    })
            cohort_run_ids = [entry['run_id'] for entry in cohort_entries]
            metrics = []
            for series in metric_series.values():
                present = {value['run_id'] for value in series['values']}
                series['missing_run_ids'] = [
                    run_id for run_id in cohort_run_ids if run_id not in present
                ]
                series['comparable'] = (
                    fingerprint is not None and len(series['values']) >= 2
                )
                metrics.append(series)
            comparable = fingerprint is not None and len(cohort_entries) >= 2
            if fingerprint is None:
                reason = 'The run lacks a verified workload contract.'
            elif len(cohort_entries) < 2:
                reason = 'No second selected run has the same workload contract.'
            else:
                reason = None
            groups.append({
                'result_id': result_id,
                'name': cohort_entries[0]['name'],
                'fingerprint': fingerprint,
                'contract': contract.get('contract'),
                'contract_unknown': contract.get('contract_unknown', True),
                'contract_issues': contract.get('issues', []),
                'provenance_warnings': provenance_warnings,
                'run_ids': cohort_run_ids,
                'comparable': comparable,
                'reason': reason,
                'metrics': metrics,
            })

    charts = []
    excluded = []
    groups_by_result: dict[str, list[dict[str, Any]]] = {}
    for group in groups:
        groups_by_result.setdefault(group['result_id'], []).append(group)
    for result_id, result_groups in groups_by_result.items():
        candidates = [group for group in result_groups if group['comparable']]
        selected_group = max(candidates, key=lambda group: len(group['run_ids'])) if candidates else None
        if selected_group:
            fingerprint_suffix = str(selected_group['fingerprint'])[:12]
            for metric in selected_group['metrics']:
                if not metric.get('comparable'):
                    continue
                values = []
                for item in metric['values']:
                    normalized_value = {
                        'run_id': item['run_id'],
                        'value': item['value'],
                        'warnings': item.get('warnings', []),
                        'provenance': item.get('provenance', {}),
                    }
                    uncertainty = _finite_number(item.get('uncertainty'))
                    if uncertainty is not None:
                        normalized_value['error_low'] = (
                            float(item['value']) - float(uncertainty)
                        )
                        normalized_value['error_high'] = (
                            float(item['value']) + float(uncertainty)
                        )
                    values.append(normalized_value)
                direction = {
                    DIRECTION_HIGHER: 'higher',
                    DIRECTION_LOWER: 'lower',
                }.get(metric['direction'], 'neutral')
                subtest_token = hashlib.sha256(
                    str(metric.get('subtest') or '').encode()
                ).hexdigest()[:8]
                charts.append({
                    'id': (
                        f'{result_id}:{metric["key"]}:'
                        f'{subtest_token}:{fingerprint_suffix}'
                    ),
                    'result_id': result_id,
                    'result_name': selected_group['name'],
                    'metric_id': metric['key'],
                    'label': metric['label'],
                    'unit': metric['unit'],
                    'direction': direction,
                    'primary': bool(metric.get('primary')),
                    'subtest': metric.get('subtest'),
                    'fingerprint': selected_group['fingerprint'],
                    'provenance_warnings': selected_group.get(
                        'provenance_warnings',
                        [],
                    ),
                    'values': values,
                })
        selected_ids = set(selected_group['run_ids']) if selected_group else set()
        for group in result_groups:
            if group is selected_group:
                continue
            for run_id in group['run_ids']:
                related = [
                    mismatch['message']
                    for mismatch in mismatches
                    if mismatch['result_id'] == result_id
                    and run_id in {
                        mismatch['left_run_id'],
                        mismatch['right_run_id'],
                    }
                    and (
                        not selected_ids
                        or selected_ids & {
                            mismatch['left_run_id'],
                            mismatch['right_run_id'],
                        }
                    )
                ]
                excluded.append({
                    'run_id': run_id,
                    'result_id': result_id,
                    'result_name': group['name'],
                    'reasons': list(dict.fromkeys(
                        related
                        or group.get('contract_issues')
                        or [group.get('reason') or 'No matching workload contract.']
                    )),
                })
    for issue in issues:
        existing = next((
            item for item in excluded
            if item.get('run_id') == issue.get('run_id')
            and item.get('result_id') == issue.get('result_id')
        ), None)
        if existing:
            if issue['reason'] not in existing['reasons']:
                existing['reasons'].append(issue['reason'])
        else:
            excluded.append({
                'run_id': issue.get('run_id'),
                'result_id': issue.get('result_id'),
                'result_name': issue.get('result_name'),
                'reasons': [issue['reason']],
            })

    return {
        '$schema': COMPARISON_SCHEMA,
        'schema_version': COMPARISON_SCHEMA_VERSION,
        'runs': runs,
        'charts': charts,
        'excluded': excluded,
        'groups': groups,
        'mismatches': mismatches,
        'issues': issues,
    }


_NAME_TO_RESULT_ID = {
    'stream': 'stream',
    'fio storage suite': 'fio',
    'sysbench cpu': 'sysbench_cpu',
    'sysbench memory': 'sysbench_memory',
    'sysbench file i o': 'sysbench_fileio',
    'iperf3 tcp': 'iperf_tcp',
    'iperf3 udp': 'iperf_udp',
    'iperf3 sctp': 'iperf_sctp',
    'llama cpp throughput cpu': 'llama_bench',
    'phoronix 7 zip cpu': 'phoronix_compress_7zip',
    'phoronix 7 zip compression': 'phoronix_compress_7zip',
    'phoronix openssl sha 256': 'phoronix_openssl',
    'phoronix linux kernel compilation': 'phoronix_build_linux_kernel',
    'phoronix tinymembench': 'phoronix_tinymembench',
    'apachebench new connections': 'apachebench_new_connections',
    'apachebench keep alive': 'apachebench_keep_alive',
}


def _name_key(value: object) -> str:
    return re.sub(r'[^a-z0-9]+', ' ', str(value).lower()).strip()


def _legacy_result_id(name: str, plan: Mapping[str, Any]) -> str | None:
    key = _name_key(name)
    if key.startswith('deathstarbench'):
        return 'deathstarbench'
    return _NAME_TO_RESULT_ID.get(key)


def _strip_markup(value: str) -> str:
    return html.unescape(re.sub(r'<[^>]*>', '', value)).strip()


def _legacy_value(value: str) -> Any:
    text = html.unescape(value).strip()
    if not text:
        return ''
    if re.fullmatch(r'[+-]?\d+', text):
        try:
            return int(text)
        except ValueError:
            return text
    if re.fullmatch(r'[+-]?(?:\d+\.\d*|\d*\.\d+)(?:[eE][+-]?\d+)?', text):
        try:
            number = float(text)
            return number if math.isfinite(number) else text
        except ValueError:
            return text
    if text.startswith(('[', '{', '(', "'", '"')) or text in {
        'True',
        'False',
        'None',
    }:
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError, MemoryError, RecursionError):
            return text
        safe = _safe_json_value(parsed, filter_keys=True)
        return text if safe is _DROP else safe
    return _clean_text(text, 16384)


def _legacy_table(section: str, title: str) -> dict[str, Any]:
    match = re.search(
        rf'<h3>{re.escape(title)}</h3><table><tbody>(.*?)</tbody></table>',
        section,
        re.DOTALL | re.IGNORECASE,
    )
    if not match:
        return {}
    result = {}
    for key, value in re.findall(
        r'<tr><th>(.*?)</th><td>(.*?)</td></tr>',
        match.group(1),
        re.DOTALL | re.IGNORECASE,
    ):
        normalized_key = _name_key(_strip_markup(key)).replace(' ', '_')
        if not normalized_key or _unsafe_key(normalized_key):
            continue
        result[normalized_key] = _legacy_value(_strip_markup(value))
    return result


def _legacy_report_document(run_directory: Path) -> dict[str, Any]:
    report_path = run_directory / 'report.html'
    try:
        report = report_path.read_text(errors='replace')
    except OSError as exc:
        raise ValueError(f'Unable to read legacy report: {exc}') from exc

    embedded_match = re.search(
        r'<script\b(?=[^>]*\bid=["\']benchmark-results["\'])'
        r'(?=[^>]*\btype=["\']application/json["\'])[^>]*>'
        r'(.*?)</script>',
        report,
        re.DOTALL | re.IGNORECASE,
    )
    if embedded_match:
        try:
            embedded = json.loads(embedded_match.group(1))
            normalized = _normalize_results_document(embedded)
            normalized['provenance'] = 'embedded_results_artifact'
            return normalized
        except (json.JSONDecodeError, ValueError):
            # A damaged embedded artifact must never be partially trusted.
            # The scalar-table recovery below remains safe and marks exactly
            # what it could verify.
            pass

    plan = {}
    plan_match = re.search(
        r'<h2>Configuration</h2><pre>(.*?)</pre>',
        report,
        re.DOTALL | re.IGNORECASE,
    )
    if plan_match:
        try:
            loaded_plan = json.loads(_strip_markup(plan_match.group(1)))
            if isinstance(loaded_plan, Mapping):
                plan = loaded_plan
        except json.JSONDecodeError:
            pass
    plan_path = run_directory / 'plan.json'
    if not plan and plan_path.exists():
        try:
            loaded_plan = json.loads(plan_path.read_text())
            if isinstance(loaded_plan, Mapping):
                plan = loaded_plan
        except (OSError, json.JSONDecodeError):
            pass
    plan = canonical_plan(plan)

    state = {}
    state_path = run_directory / 'state.json'
    if state_path.exists():
        try:
            loaded_state = json.loads(state_path.read_text())
            if isinstance(loaded_state, Mapping):
                state = loaded_state
        except (OSError, json.JSONDecodeError):
            pass

    report_identity = re.search(
        r'<body><h1>.*?</h1><p>Run\s+([^<·]+)\s*·\s*([^<]+)</p>',
        report,
        re.DOTALL | re.IGNORECASE,
    )
    run_id = (
        _strip_markup(report_identity.group(1))
        if report_identity
        else run_directory.name
    )
    created_at = (
        _strip_markup(report_identity.group(2))
        if report_identity
        else state.get('created_at')
    )

    results = []
    for section in re.findall(
        r'<section>(.*?)</section>',
        report,
        re.DOTALL | re.IGNORECASE,
    ):
        name_match = re.search(r'<h2>(.*?)</h2>', section, re.DOTALL | re.IGNORECASE)
        if not name_match:
            continue
        name = _strip_markup(name_match.group(1))
        result_id = _legacy_result_id(name, plan)
        timing = re.search(
            r'<p>Started\s+(.*?)\s+·\s+([^<]+)\s+seconds</p>',
            section,
            re.DOTALL | re.IGNORECASE,
        )
        status_match = re.search(
            r'<p>Status:\s*<strong>(.*?)</strong>(.*?)</p>',
            section,
            re.DOTALL | re.IGNORECASE,
        )
        result: dict[str, Any] = {
            'name': name,
            'status': (
                _strip_markup(status_match.group(1))
                if status_match
                else 'unknown'
            ),
            'metadata': _legacy_table(section, 'Environment'),
            'metrics': _legacy_table(section, 'Measured results'),
        }
        if result_id:
            result['id'] = result_id
        if timing:
            result['started_at'] = _strip_markup(timing.group(1))
            duration = _legacy_value(_strip_markup(timing.group(2)))
            if isinstance(duration, (int, float)) and not isinstance(duration, bool):
                result['duration_seconds'] = duration
        if status_match:
            error = _strip_markup(status_match.group(2)).lstrip(' ·')
            if error:
                result['error'] = error
        result = sanitize_result(result)
        if result_id:
            known = workload_fingerprint(result_id, plan, result.get('metadata'))
            if not extract_chart_metrics(result):
                known = {
                    **known,
                    'fingerprint': None,
                    'contract_unknown': True,
                    'issues': [
                        *known.get('issues', []),
                        'No registered scalar measurements could be recovered '
                        'from the legacy report.',
                    ],
                }
            result['comparison'] = {
                **known,
                'provenance_warnings': [
                    'Measurements were recovered from report.html. Only '
                    'registered scalar and safely parsed structured values '
                    'are eligible for comparison.'
                ],
            }
        results.append(result)
    if not results:
        raise ValueError('The legacy report contains no recognizable result sections.')

    run = _run_record({
        'id': state.get('id') or run_id,
        'status': state.get('status') or 'reported',
        'benchmark_status': state.get('benchmark_status') or 'unknown',
        'error': state.get('error'),
        'created_at': state.get('created_at') or created_at,
        'updated_at': state.get('updated_at'),
    })
    return {
        '$schema': RESULTS_SCHEMA,
        'schema_version': RESULTS_SCHEMA_VERSION,
        'generated_at': run.get('updated_at') or run.get('created_at') or '',
        'provenance': 'legacy_report',
        'contract_unknown': any(
            result.get('comparison', {}).get('contract_unknown', True)
            for result in results
        ),
        'run': run,
        'plan': plan,
        'results': results,
    }


def load_results_document(
    run_directory: str | os.PathLike[str],
) -> dict[str, Any]:
    """Prefer ``results.json`` and conservatively recover legacy report data."""

    path = Path(run_directory)
    directory = path.parent if path.is_file() else path
    artifact = directory / RESULTS_ARTIFACT_NAME
    if artifact.exists():
        return read_results_artifact(artifact)
    report = directory / 'report.html'
    if report.exists():
        return _legacy_report_document(directory)
    raise FileNotFoundError(
        f'Run {directory.name} has neither {RESULTS_ARTIFACT_NAME} nor report.html.'
    )


__all__ = [
    'CANONICAL_RESULT_IDS',
    'COMPARISON_SCHEMA',
    'COMPARISON_SCHEMA_VERSION',
    'DEATHSTARBENCH_LEGACY_RUNTIME_REVISION',
    'DIRECTION_HIGHER',
    'DIRECTION_LOWER',
    'LLAMA_ARCHITECTURE_PROFILE_METHOD',
    'METRIC_REGISTRY',
    'MetricSpec',
    'RESULTS_ARTIFACT_NAME',
    'RESULTS_SCHEMA',
    'RESULTS_SCHEMA_VERSION',
    'STORAGE_RESULT_IDS',
    'STORAGE_TARGET_CONTRACT',
    'STORAGE_TARGET_KINDS',
    'STORAGE_TARGET_LAYOUT',
    'STORAGE_TARGET_MOUNT_POINTS',
    'STORAGE_TARGET_POLICY',
    'STORAGE_TARGET_TRANSPORTS',
    'STORAGE_TARGET_VERIFICATION',
    'WORKLOAD_CONTRACT_VERSION',
    'build_comparison_payload',
    'build_results_artifact',
    'canonical_plan',
    'explain_workload_mismatch',
    'extract_chart_metrics',
    'load_results_document',
    'metric_specs',
    'read_results_artifact',
    'sanitize_result',
    'workload_fingerprint',
    'write_results_artifact',
]
