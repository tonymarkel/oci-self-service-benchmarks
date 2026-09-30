#!/usr/bin/env python3
"""Validate the fail-closed distributed DeathStarBench cleanup receipt."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RECEIPT = (
    PROJECT_ROOT
    / 'docs'
    / 'qualification'
    / 'deathstarbench-distributed-cleanup-review-v1.json'
)
PROVIDERS = frozenset({'aws', 'azure', 'gcp', 'oci'})
REPRESENTATIVE_SCENARIOS = frozenset({
    'safe_resume',
    'initializer_response_loss',
    'unsafe_cleanup_only',
})
RUN_ID_PATTERN = re.compile(r'[0-9a-f]{12}')
DIGEST_PATTERN = re.compile(r'sha256:[0-9a-f]{64}')
NAME_PATTERN = re.compile(r'[a-z][a-z0-9_]*')
ROOT_KEYS = frozenset({
    'schema_version',
    'review_kind',
    'topology_id',
    'runtime_id',
    'reviewed_at',
    'overall_gate_complete',
    'providers',
})
PROVIDER_KEYS = frozenset({
    'representative_matrix',
    'additional_failure_scenario_runs',
    'runs',
    'local_terminal_evidence',
    'independent_cloud_inventory',
})


class CleanupReviewValidationError(ValueError):
    """The cleanup receipt is incomplete, malformed, or internally unsafe."""


def _fail(path: str, message: str) -> None:
    raise CleanupReviewValidationError(f'{path}: {message}')


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        _fail(path, 'must be an object')
    return value


def _exact_keys(
    value: Mapping[str, Any],
    expected: frozenset[str],
    path: str,
    *,
    optional: frozenset[str] = frozenset(),
) -> None:
    keys = set(value)
    missing = expected - keys
    unknown = keys - expected - optional
    if missing:
        _fail(path, 'missing keys: ' + ', '.join(sorted(missing)))
    if unknown:
        _fail(path, 'unknown keys: ' + ', '.join(sorted(unknown)))


def _timestamp(value: Any, path: str) -> datetime:
    if not isinstance(value, str) or not value.endswith('Z'):
        _fail(path, 'must be an RFC 3339 UTC timestamp ending in Z')
    try:
        parsed = datetime.fromisoformat(value.removesuffix('Z') + '+00:00')
    except ValueError as exc:
        raise CleanupReviewValidationError(
            f'{path}: must be an RFC 3339 UTC timestamp'
        ) from exc
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        _fail(path, 'must use UTC')
    return parsed


def _job_id(value: Any, path: str) -> str:
    if not isinstance(value, str) or RUN_ID_PATTERN.fullmatch(value) is None:
        _fail(path, 'must be a 12-character lowercase hexadecimal job ID')
    return value


def _digest(value: Any, path: str) -> None:
    if not isinstance(value, str) or DIGEST_PATTERN.fullmatch(value) is None:
        _fail(path, 'must be a sha256: digest with 64 lowercase hex digits')


def _scenario_runs(
    value: Any,
    path: str,
    *,
    allowed: frozenset[str] | None = None,
) -> dict[str, str]:
    raw = _mapping(value, path)
    result: dict[str, str] = {}
    for scenario, raw_job_id in raw.items():
        if not isinstance(scenario, str) or NAME_PATTERN.fullmatch(scenario) is None:
            _fail(path, f'invalid scenario name {scenario!r}')
        if allowed is not None and scenario not in allowed:
            _fail(path, f'unknown representative scenario {scenario!r}')
        result[scenario] = _job_id(raw_job_id, f'{path}.{scenario}')
    return result


def _validate_provider(provider: str, value: Any) -> bool:
    path = f'providers.{provider}'
    record = _mapping(value, path)
    _exact_keys(
        record,
        PROVIDER_KEYS - {'additional_failure_scenario_runs'},
        path,
        optional=frozenset({'additional_failure_scenario_runs'}),
    )

    runs = _mapping(record['runs'], f'{path}.runs')
    if not runs:
        _fail(f'{path}.runs', 'must retain at least one reviewed run')
    for raw_job_id, raw_artifacts in runs.items():
        job_id = _job_id(raw_job_id, f'{path}.runs key')
        artifacts = _mapping(raw_artifacts, f'{path}.runs.{job_id}')
        _exact_keys(
            artifacts,
            frozenset({'state_sha256'}),
            f'{path}.runs.{job_id}',
            optional=frozenset({'interruption_evidence_sha256'}),
        )
        _digest(
            artifacts['state_sha256'],
            f'{path}.runs.{job_id}.state_sha256',
        )
        if 'interruption_evidence_sha256' in artifacts:
            _digest(
                artifacts['interruption_evidence_sha256'],
                f'{path}.runs.{job_id}.interruption_evidence_sha256',
            )

    matrix_path = f'{path}.representative_matrix'
    matrix = _mapping(record['representative_matrix'], matrix_path)
    _exact_keys(
        matrix,
        frozenset({
            'required_scenarios',
            'evidenced_scenario_runs',
            'complete',
        }),
        matrix_path,
    )
    required = matrix['required_scenarios']
    if (
        not isinstance(required, list)
        or any(not isinstance(item, str) for item in required)
        or len(required) != len(set(required))
        or set(required) != REPRESENTATIVE_SCENARIOS
    ):
        _fail(
            f'{matrix_path}.required_scenarios',
            'must contain each representative scenario exactly once',
        )
    evidenced = _scenario_runs(
        matrix['evidenced_scenario_runs'],
        f'{matrix_path}.evidenced_scenario_runs',
        allowed=REPRESENTATIVE_SCENARIOS,
    )
    if any(job_id not in runs for job_id in evidenced.values()):
        _fail(
            f'{matrix_path}.evidenced_scenario_runs',
            'must reference only retained reviewed runs',
        )
    matrix_complete = matrix['complete']
    if type(matrix_complete) is not bool:
        _fail(f'{matrix_path}.complete', 'must be a boolean')
    evidence_is_complete = set(evidenced) == REPRESENTATIVE_SCENARIOS
    if matrix_complete is not evidence_is_complete:
        _fail(
            f'{matrix_path}.complete',
            'must equal completeness of the representative scenario evidence',
        )

    if 'additional_failure_scenario_runs' in record:
        additional = _scenario_runs(
            record['additional_failure_scenario_runs'],
            f'{path}.additional_failure_scenario_runs',
        )
        if set(additional) & REPRESENTATIVE_SCENARIOS:
            _fail(
                f'{path}.additional_failure_scenario_runs',
                'must not repeat representative scenarios',
            )
        if any(job_id not in runs for job_id in additional.values()):
            _fail(
                f'{path}.additional_failure_scenario_runs',
                'must reference only retained reviewed runs',
            )

    terminal_path = f'{path}.local_terminal_evidence'
    terminal = _mapping(record['local_terminal_evidence'], terminal_path)
    _exact_keys(
        terminal,
        frozenset({
            'sampled_run_ids',
            'predicate',
            'predicate_result',
            'recoverable_resources',
            'ownership_remaining',
            'cleanup_error_present',
            'retained_state',
        }),
        terminal_path,
    )
    sampled = terminal['sampled_run_ids']
    if not isinstance(sampled, list) or not sampled:
        _fail(f'{terminal_path}.sampled_run_ids', 'must be a non-empty array')
    normalized_sampled = [
        _job_id(item, f'{terminal_path}.sampled_run_ids')
        for item in sampled
    ]
    if len(normalized_sampled) != len(set(normalized_sampled)):
        _fail(f'{terminal_path}.sampled_run_ids', 'must not contain duplicates')
    if set(normalized_sampled) - set(runs):
        _fail(
            f'{terminal_path}.sampled_run_ids',
            'must reference only retained reviewed runs',
        )
    representative_run_ids = set(evidenced.values())
    missing_terminal_run_ids = (
        representative_run_ids - set(normalized_sampled)
    )
    if missing_terminal_run_ids:
        _fail(
            f'{terminal_path}.sampled_run_ids',
            'must cover every evidenced representative scenario run; missing '
            + ', '.join(sorted(missing_terminal_run_ids)),
        )
    for key in ('predicate', 'retained_state'):
        if not isinstance(terminal[key], str) or not terminal[key].strip():
            _fail(f'{terminal_path}.{key}', 'must be a non-empty string')
    if terminal['predicate_result'] is not True:
        _fail(f'{terminal_path}.predicate_result', 'must be true')
    for key in (
        'recoverable_resources',
        'ownership_remaining',
        'cleanup_error_present',
    ):
        if terminal[key] is not False:
            _fail(f'{terminal_path}.{key}', 'must be false')

    inventory_path = f'{path}.independent_cloud_inventory'
    inventory = _mapping(record['independent_cloud_inventory'], inventory_path)
    _exact_keys(
        inventory,
        frozenset({'checked_at', 'method', 'scopes'}),
        inventory_path,
    )
    _timestamp(inventory['checked_at'], f'{inventory_path}.checked_at')
    if not isinstance(inventory['method'], str) or not inventory['method'].strip():
        _fail(f'{inventory_path}.method', 'must be a non-empty string')
    scopes = inventory['scopes']
    if not isinstance(scopes, list) or not scopes:
        _fail(f'{inventory_path}.scopes', 'must be a non-empty array')
    inventory_run_ids: set[str] = set()
    for index, raw_scope in enumerate(scopes):
        scope_path = f'{inventory_path}.scopes[{index}]'
        scope = _mapping(raw_scope, scope_path)
        _exact_keys(
            scope,
            frozenset({'run_ids', 'scope', 'queries'}),
            scope_path,
        )
        run_ids = scope['run_ids']
        if not isinstance(run_ids, list) or not run_ids:
            _fail(f'{scope_path}.run_ids', 'must be a non-empty array')
        for raw_job_id in run_ids:
            job_id = _job_id(raw_job_id, f'{scope_path}.run_ids')
            if job_id not in runs:
                _fail(
                    f'{scope_path}.run_ids',
                    'must reference only retained reviewed runs',
                )
            inventory_run_ids.add(job_id)
        selector = _mapping(scope['scope'], f'{scope_path}.scope')
        if not selector or any(
            not isinstance(key, str)
            or not key
            or not isinstance(value, str)
            or not value.strip()
            for key, value in selector.items()
        ):
            _fail(
                f'{scope_path}.scope',
                'must contain non-empty string selectors',
            )
        queries = scope['queries']
        if not isinstance(queries, list) or not queries:
            _fail(f'{scope_path}.queries', 'must be a non-empty array')
        categories: set[str] = set()
        for query_index, raw_query in enumerate(queries):
            query_path = f'{scope_path}.queries[{query_index}]'
            query = _mapping(raw_query, query_path)
            _exact_keys(
                query,
                frozenset({'category', 'live_resource_count', 'outcome'}),
                query_path,
            )
            category = query['category']
            if (
                not isinstance(category, str)
                or NAME_PATTERN.fullmatch(category) is None
                or category in categories
            ):
                _fail(
                    f'{query_path}.category',
                    'must be a unique lowercase snake-case name',
                )
            categories.add(category)
            count = query['live_resource_count']
            if type(count) is not int or count != 0:
                _fail(
                    f'{query_path}.live_resource_count',
                    'must be the integer zero',
                )
            if query['outcome'] not in {'absent', 'no_live_resources'}:
                _fail(
                    f'{query_path}.outcome',
                    'must be absent or no_live_resources',
                )
    if not inventory_run_ids:
        _fail(inventory_path, 'must cover at least one reviewed run')
    missing_inventory_run_ids = representative_run_ids - inventory_run_ids
    if missing_inventory_run_ids:
        _fail(
            f'{inventory_path}.scopes',
            'must cover every evidenced representative scenario run; missing '
            + ', '.join(sorted(missing_inventory_run_ids)),
        )
    return matrix_complete


def validate_cleanup_review(value: Any) -> dict[str, Any]:
    """Return a plain validated copy, or raise on any unsafe ambiguity."""

    root = _mapping(value, 'receipt')
    _exact_keys(root, ROOT_KEYS, 'receipt')
    if root['schema_version'] != 1:
        _fail('schema_version', 'must equal 1')
    if root['review_kind'] != 'deathstarbench_distributed_cleanup':
        _fail('review_kind', 'is unsupported')
    if root['topology_id'] != 'distributed_tiered_v1':
        _fail('topology_id', 'is unsupported')
    if root['runtime_id'] != 'k3s_v1':
        _fail('runtime_id', 'is unsupported')
    reviewed_at = _timestamp(root['reviewed_at'], 'reviewed_at')

    providers = _mapping(root['providers'], 'providers')
    if set(providers) != PROVIDERS:
        missing = PROVIDERS - set(providers)
        unknown = set(providers) - PROVIDERS
        details = []
        if missing:
            details.append('missing ' + ', '.join(sorted(missing)))
        if unknown:
            details.append('unknown ' + ', '.join(sorted(unknown)))
        _fail('providers', '; '.join(details))
    matrix_complete = {
        provider: _validate_provider(provider, providers[provider])
        for provider in sorted(PROVIDERS)
    }
    latest_inventory_at = max(
        _timestamp(
            providers[provider]['independent_cloud_inventory']['checked_at'],
            f'providers.{provider}.independent_cloud_inventory.checked_at',
        )
        for provider in sorted(PROVIDERS)
    )
    if reviewed_at < latest_inventory_at:
        _fail(
            'reviewed_at',
            'must not precede any independent cloud inventory check',
        )

    overall = root['overall_gate_complete']
    if type(overall) is not bool:
        _fail('overall_gate_complete', 'must be a boolean')
    expected_overall = all(matrix_complete.values())
    if overall is not expected_overall:
        incomplete = sorted(
            provider for provider, complete in matrix_complete.items()
            if not complete
        )
        suffix = (
            ' (incomplete providers: ' + ', '.join(incomplete) + ')'
            if incomplete else ''
        )
        _fail(
            'overall_gate_complete',
            'must equal the conjunction of provider matrix completeness'
            + suffix,
        )
    return json.loads(json.dumps(root))


def load_cleanup_review(path: Path = DEFAULT_RECEIPT) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding='utf-8')
        value = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise CleanupReviewValidationError(
            f'{path}: unable to load cleanup review: {exc}'
        ) from exc
    return validate_cleanup_review(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('receipt', nargs='?', type=Path, default=DEFAULT_RECEIPT)
    args = parser.parse_args(argv)
    try:
        receipt = load_cleanup_review(args.receipt)
    except CleanupReviewValidationError as exc:
        parser.error(str(exc))
    state = 'complete' if receipt['overall_gate_complete'] else 'incomplete'
    print(f'Distributed DeathStarBench cleanup gate: {state}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
