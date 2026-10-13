"""Run-bound Hotel/Media preflight, separate from the released Social lifecycle.

This bridge adopts artifact evidence into an offline render and an explicit
database-storage contract. It does not provision, apply, seed, measure, or
authorize public execution. Persisted snapshots are checked against fresh
artifact adoption, never used as a substitute for that verification.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any

from .deathstarbench_candidate_artifacts import CandidateArtifactBinding
from .deathstarbench_candidate_storage import (
    CandidateStorageContract,
    candidate_storage_contract,
    parse_candidate_database_storage_attestation,
    validate_candidate_database_storage_attestation,
)
from .deathstarbench_candidate_workload import (
    CandidateWorkloadBundle,
    render_candidate_workload_bundle,
)
from .deathstarbench_contract import DISTRIBUTED_RUNTIME_REVISION
from .deathstarbench_workload_contract import distributed_workload_profile


SNAPSHOT_SCHEMA = 'candidate-runtime-preflight-v1'
JOURNAL_SCHEMA = 'candidate-storage-journal-v1'
_PROVIDERS = frozenset({'aws', 'azure', 'gcp', 'oci'})
_RUN_ID = re.compile(r'[0-9a-f]{12}\Z')
_SHA256 = re.compile(r'sha256:[0-9a-f]{64}\Z')
_MAX_SNAPSHOT_BYTES = 1024 * 1024


class CandidateRuntimeError(ValueError):
    """The saved or requested candidate is crossed, stale, or unqualified."""


def _canonical(value: Any) -> str:
    # Recent Python JSON implementations can accept deeply nested input without
    # RecursionError. Keep operator snapshots bounded independently of that.
    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > 64:
            raise CandidateRuntimeError('Candidate JSON exceeds its nesting limit.')
        if isinstance(item, Mapping):
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, (list, tuple)):
            pending.extend((child, depth + 1) for child in item)
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n'
    except (TypeError, ValueError, RecursionError) as exc:
        raise CandidateRuntimeError('Candidate state must contain finite JSON values.') from exc


def _fingerprint(value: Any) -> str:
    return 'sha256:' + hashlib.sha256(_canonical(value).encode()).hexdigest()


def _closed_gates() -> dict[str, bool]:
    # This preflight cannot promote any public or runtime qualification gate.
    return {key: False for key in (
        'anonymous_layer_pull', 'cloud_runtime', 'dataset', 'measurement',
        'denied_edges', 'cleanup', 'public_release',
    )}


@dataclass(frozen=True, slots=True)
class CandidateRuntimePlan:
    run_id: str
    provider: str
    topology_fingerprint: str
    artifacts: CandidateArtifactBinding
    bundle: CandidateWorkloadBundle
    storage: CandidateStorageContract
    snapshot_json: str
    fingerprint: str

    def __post_init__(self):
        # frozen prevents mutation, not dataclasses.replace with crossed fields.
        # Reconstruct the cheap offline contracts before accepting any instance.
        _validate_run_scope(self.artifacts, self.run_id, self.provider, self.topology_fingerprint)
        rebuilt_bundle = render_candidate_workload_bundle(
            self.artifacts.workload_id, self.artifacts.runtime_manifest,
            self.bundle.application_architecture,
            self.bundle.load_generator_cidr.removesuffix('/32'),
        )
        rebuilt_storage = candidate_storage_contract(
            self.artifacts.workload_id, self.run_id, self.storage.filesystem_uuid,
        )
        expected = _snapshot_identity(
            self.artifacts, self.run_id, self.provider, self.topology_fingerprint,
            rebuilt_bundle, rebuilt_storage,
        )
        if (
            self.bundle != rebuilt_bundle or self.storage != rebuilt_storage
            or self.snapshot_json != _canonical(expected)
            or self.fingerprint != _fingerprint(expected)
        ):
            raise CandidateRuntimeError('Candidate plan fields conflict with its retained run snapshot.')

    def snapshot(self) -> dict[str, Any]:
        """Return a copy; callers cannot change the retained run identity."""

        return json.loads(self.snapshot_json)


def _validate_run_scope(artifacts, run_id, provider, topology_fingerprint):
    if not isinstance(artifacts, CandidateArtifactBinding):
        raise CandidateRuntimeError('Freshly adopted candidate artifacts are required.')
    if not isinstance(run_id, str) or not _RUN_ID.fullmatch(run_id) or run_id == '0' * 12:
        raise CandidateRuntimeError('An exact nonzero 12-character lowercase run ID is required.')
    if not isinstance(provider, str) or provider not in _PROVIDERS:
        raise CandidateRuntimeError('An exact supported candidate cloud provider is required.')
    if (
        not isinstance(topology_fingerprint, str)
        or not _SHA256.fullmatch(topology_fingerprint)
        or topology_fingerprint.endswith('0' * 64)
    ):
        raise CandidateRuntimeError('An exact nonzero topology SHA-256 is required.')


def _snapshot_identity(artifacts, run_id, provider, topology_fingerprint, bundle, storage):
    profile = distributed_workload_profile(artifacts.workload_id)
    return {
        'schema_version': 1, 'candidate_schema': SNAPSHOT_SCHEMA,
        'candidate_only': True, 'released': False,
        'run_id': run_id, 'provider': provider,
        'topology_fingerprint': topology_fingerprint,
        'runtime_revision': DISTRIBUTED_RUNTIME_REVISION,
        'profile': profile.metadata(),
        'artifacts': artifacts.metadata(),
        'storage': storage.metadata(),
        'application_architecture': bundle.application_architecture,
        'namespace': bundle.namespace,
        'load_generator_cidr': bundle.load_generator_cidr,
        'bundle': {
            'manifest_source_sha256': bundle.manifest_source_sha256,
            'rendered_manifest_sha256': bundle.rendered_manifest_sha256,
            'image_manifest_sha256': bundle.image_manifest_sha256,
        },
        'qualification': _closed_gates(),
    }


def create_candidate_runtime_plan(
    artifacts: CandidateArtifactBinding,
    *,
    run_id: str,
    provider: str,
    topology_fingerprint: str,
    application_architecture: str,
    load_generator_ip: str,
    filesystem_uuid: str,
) -> CandidateRuntimePlan:
    """Bind freshly verified artifacts to one explicit run, without execution."""

    _validate_run_scope(artifacts, run_id, provider, topology_fingerprint)
    bundle = render_candidate_workload_bundle(
        artifacts.workload_id, artifacts.runtime_manifest,
        application_architecture, load_generator_ip,
    )
    storage = candidate_storage_contract(artifacts.workload_id, run_id, filesystem_uuid)
    snapshot = _snapshot_identity(artifacts, run_id, provider, topology_fingerprint, bundle, storage)
    return CandidateRuntimePlan(
        run_id, provider, topology_fingerprint, artifacts, bundle, storage,
        _canonical(snapshot), _fingerprint(snapshot),
    )


def validate_candidate_runtime_snapshot(
    value: Any, plan: CandidateRuntimePlan,
) -> dict[str, Any]:
    """Fail before remote work if any saved run/artifact/storage field drifted."""

    if not isinstance(value, Mapping) or _canonical(dict(value)) != plan.snapshot_json:
        raise CandidateRuntimeError('Saved candidate snapshot conflicts with fresh run preflight.')
    return plan.snapshot()


def read_candidate_runtime_snapshot(path: Path) -> dict[str, Any]:
    """Bounded strict JSON reader; no duplicate keys, symlinks, or NaN."""

    def object_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise CandidateRuntimeError('Saved candidate snapshot contains duplicate keys.')
            result[key] = value
        return result

    def constant(_value):
        raise CandidateRuntimeError('Saved candidate snapshot contains non-finite values.')

    try:
        descriptor = path.lstat()
        if not stat.S_ISREG(descriptor.st_mode):
            raise CandidateRuntimeError('Saved candidate snapshot must be a regular non-symlink file.')
        flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
        with os.fdopen(os.open(path, flags), 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(opened.st_mode)
                or (opened.st_dev, opened.st_ino) != (descriptor.st_dev, descriptor.st_ino)
            ):
                raise CandidateRuntimeError('Saved candidate snapshot changed while opening.')
            payload = stream.read(_MAX_SNAPSHOT_BYTES + 1)
        if len(payload) > _MAX_SNAPSHOT_BYTES:
            raise CandidateRuntimeError('Saved candidate snapshot exceeds its size limit.')
        result = json.loads(payload.decode('utf-8'), object_pairs_hook=object_pairs, parse_constant=constant)
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise CandidateRuntimeError('Cannot read saved candidate snapshot as strict UTF-8 JSON.') from exc
    if not isinstance(result, dict):
        raise CandidateRuntimeError('Saved candidate snapshot must be a JSON object.')
    _canonical(result)  # Also rejects numeric overflow and excessive nesting.
    return result


def new_candidate_storage_journal(plan: CandidateRuntimePlan) -> dict[str, Any]:
    return {
        'schema_version': 1, 'candidate_schema': JOURNAL_SCHEMA,
        'candidate_only': True, 'released': False,
        'snapshot_fingerprint': plan.fingerprint,
        'run_id': plan.run_id, 'workload_id': plan.artifacts.workload_id,
        'namespace': plan.bundle.namespace,
        'state': 'preflight_only', 'storage_attestation': None,
        'qualification': _closed_gates(),
    }


def validate_candidate_storage_journal(
    value: Any, plan: CandidateRuntimePlan,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise CandidateRuntimeError('Candidate storage journal must be an object.')
    expected = new_candidate_storage_journal(plan)
    if value.get('state') == 'storage_prepared':
        attestation = value.get('storage_attestation')
        if not isinstance(attestation, Mapping):
            raise CandidateRuntimeError('Prepared candidate storage lacks its attestation.')
        # Reuse the same parser validation, rather than trusting saved numbers
        # or calling preparation again on a potentially populated dataset.
        expected['state'] = 'storage_prepared'
        expected['storage_attestation'] = _validate_saved_storage_attestation(attestation, plan)
    if _canonical(dict(value)) != _canonical(expected):
        raise CandidateRuntimeError('Candidate storage journal conflicts with its run or state.')
    return json.loads(_canonical(expected))


def _validate_saved_storage_attestation(value: Mapping[str, Any], plan: CandidateRuntimePlan) -> dict[str, Any]:
    return validate_candidate_database_storage_attestation(value, plan.storage)


def record_candidate_storage_attestation(
    plan: CandidateRuntimePlan, journal: Any, output: str,
) -> dict[str, Any]:
    """Record only storage evidence, without implying dataset/runtime readiness."""

    saved = validate_candidate_storage_journal(journal, plan)
    observed = parse_candidate_database_storage_attestation(output, plan.storage)
    if saved['state'] != 'preflight_only':
        raise CandidateRuntimeError('Storage is already recorded; use read-only reconciliation before retry.')
    saved['state'] = 'storage_prepared'
    saved['storage_attestation'] = observed
    return validate_candidate_storage_journal(saved, plan)
