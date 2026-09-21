"""One-shot measurement orchestration for distributed DeathStarBench.

This module is deliberately separate from the replayable K3s runtime and
workload deployment phases.  Social Network initialization and wrk2 traffic
are not safe to replay after an ambiguous SSH response, so the execution
journal records the point of no return before the first initializing request
and refuses every later invocation.
"""

from __future__ import annotations

import base64
from collections.abc import Callable, Mapping, MutableMapping
from datetime import datetime, timezone
import hashlib
from ipaddress import ip_address
import json
import re
import shlex
import time
from typing import Any

from . import deathstarbench
from .deathstarbench_contract import (
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    DISTRIBUTED_DATASET_REVISION,
    DISTRIBUTED_IMAGE_SET_REVISION,
    DISTRIBUTED_LOAD_DRIVER_REVISION,
    DISTRIBUTED_MEASUREMENT_REVISION,
    DISTRIBUTED_RUNTIME_REVISION,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    DISTRIBUTED_WORKLOAD_REVISION,
    K3S_RUNTIME_ID,
)
from .deathstarbench_distributed import (
    RUNTIME_JOURNAL_KEY,
    WORKLOAD_JOURNAL_KEY,
    AzureK3sCandidatePlan,
    DistributedRuntimeError,
    RuntimeHost,
    _validate_existing_journal,
    _validate_existing_workload_journal,
    azure_k3s_candidate_plan,
)
from .deathstarbench_k3s_workload import (
    EXPECTED_COMPONENTS,
    NAMESPACE as WORKLOAD_NAMESPACE,
    WorkloadBundleError,
    parse_workload_execution_attestation,
    render_social_network_bundle,
    workload_readiness_command,
)
from .guests import web as web_guest
from .k3s_runtime import K3S_BINARY, normalized_architecture
from .models import DeathStarBenchOptions


EXECUTION_JOURNAL_KEY = DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
EXECUTION_JOURNAL_SCHEMA_VERSION = 1
MAX_RESULT_OUTPUT_CHARS = 65_536
INITIALIZER_MAX_RUNTIME_SECONDS = 2_160
INITIALIZER_POLL_TIMEOUT_SECONDS = 2_400
INITIALIZER_POLL_INTERVAL_SECONDS = 5
INITIALIZER_DISPATCH_GRACE_SECONDS = 60
MAX_INITIALIZER_TRANSCRIPT_BYTES = 8 * 1024 * 1024
INITIALIZER_STATE_ROOT = '/var/lib/oci-self-service-benchmarks/deathstarbench'
INITIALIZER_UNIT_PREFIX = 'benchmark-deathstarbench-init-'

DATASET_GRAPH = 'socfb-Reed98'
DATASET_USER_COUNT = 962
DATASET_UNDIRECTED_EDGE_COUNT = 18_812
DATASET_FOLLOW_COUNT = 37_624
DATASET_POST_COUNT = 9_424
DATASET_TIMELINE_USER_COUNT = 908

# These hashes bind every initializer input that is not already bound by the
# immutable upstream Git revision.  The patched hash is the exact result of
# deathstarbench.initialize_workload_command()'s strict HTTP response patch.
INITIALIZER_SOURCE_SHA256 = (
    '1b7dce9e14b82b3fb8b4ecdfe90b6fda9b7797da849840ca1a4521ef706e0482'
)
PATCHED_INITIALIZER_SHA256 = (
    'ff504a03311c1d6da4e5ba031b49ec824541fa78b898cda029a60e364edcadaf'
)
REED98_NODES_SHA256 = (
    '084917af148384c1e8396addcec2fca2a9f2c3918cad9676e12cdaad7dc7dfb2'
)
REED98_EDGES_SHA256 = (
    'ad6861fc9c27cfa77a865614454e5836988277a84889232acdb1fd1e0f557300'
)
MIXED_WORKLOAD_LUA_SHA256 = (
    'ab2cd04b6cffb53beaf27efd8dfb5eae7dcd6c8abecbb70623fda93139b3dd32'
)

FRONTEND_PORT = 8080
FRONTEND_PROBE_ROUTE = (
    '/wrk2-api/user-timeline/read?user_id=0&start=0&stop=1'
)

_EXECUTION_STATES = (
    'preparing_load_generator',
    'load_generator_ready',
    'initialization_started',
    'dataset_ready',
    'warmup_started',
    'warmup_complete',
    'measurement_started',
    'measurement_complete',
)
_ONE_SHOT_STATES = frozenset(_EXECUTION_STATES[2:])
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_PREFIXED_SHA256_RE = re.compile(r'^sha256:[0-9a-f]{64}$')
_JOB_ID_RE = re.compile(r'^[0-9a-f]{12}$')
_REMOTE_USER_RE = re.compile(r'^[a-z_][a-z0-9_-]{0,31}$')

_LOADGEN_MARKER_RE = re.compile(
    r'^DISTRIBUTED_DSB_LOAD_GENERATOR_READY '
    r'architecture=(x86_64) revision=([0-9a-f]{40}) '
    r'wrk_sha256=([0-9a-f]{64}) compiler_version_sha256=([0-9a-f]{64})$',
    re.MULTILINE,
)
_FRONTEND_MARKER_RE = re.compile(
    r'^DISTRIBUTED_DSB_FRONTEND_READY '
    r'target=([^\s]+) port=([0-9]+)$',
    re.MULTILINE,
)
_DATASET_MARKER_RE = re.compile(
    r'^DISTRIBUTED_DSB_DATASET_READY '
    r'graph=([^\s]+) users=([0-9]+) follows=([0-9]+) '
    r'posts=([0-9]+) revision=([0-9a-f]{40})$',
    re.MULTILINE,
)
_DATABASE_MARKER_RE = re.compile(
    r'^DISTRIBUTED_DSB_DATABASE_DATA '
    r'users=([0-9]+) social_graph_users=([0-9]+) '
    r'followers=([0-9]+) followees=([0-9]+) posts=([0-9]+) '
    r'timeline_users=([0-9]+) timeline_posts=([0-9]+)$',
    re.MULTILINE,
)
_INITIALIZER_DISPATCH_MARKER_RE = re.compile(
    r'^DISTRIBUTED_DSB_INITIALIZER_DISPATCHED '
    r'unit=([a-z0-9.-]+) run_user=([a-z_][a-z0-9_-]{0,31}) '
    r'payload_sha256=([0-9a-f]{64})$',
    re.MULTILINE,
)
_INITIALIZER_STATUS_MARKER_RE = re.compile(
    r'^DISTRIBUTED_DSB_INITIALIZER_STATUS '
    r'unit=([a-z0-9.-]+) '
    r'run_user=([a-z_][a-z0-9_-]{0,31}) '
    r'payload_sha256=(none|[0-9a-f]{64}) '
    r'service_user=(none|[a-z_][a-z0-9_-]{0,31}) '
    r'exec_start_match=(yes|no) '
    r'load=([a-z0-9_-]+) active=([a-z0-9_-]+) '
    r'sub=([a-z0-9_-]+) result=([a-z0-9_-]+) '
    r'exec_code=([0-9]+) exec_status=([0-9]+) '
    r'invocation_id=(none|[0-9a-f]{32}) '
    r'start_mono_usec=([0-9]+) exit_mono_usec=([0-9]+) '
    r'output_bytes=([0-9]+) output_sha256=(none|[0-9a-f]{64})$',
    re.MULTILINE,
)
_WRK2_METRICS_LINE_RE = re.compile(r'^OCI_DSB_METRICS\s+.+$', re.MULTILINE)
_WRK2_SENT_LINE_RE = re.compile(r'^Sent\s+[0-9]+\s+requests\s*$', re.MULTILINE)


class DistributedMeasurementError(ValueError):
    """Raised when a distributed measurement cannot proceed safely."""


def _exact_marker(
    pattern: re.Pattern[str],
    value: str | bytes,
    label: str,
) -> re.Match[str]:
    if isinstance(value, bytes):
        try:
            value = value.decode('utf-8')
        except UnicodeDecodeError as exc:
            raise DistributedMeasurementError(
                f'{label} output is not UTF-8.'
            ) from exc
    if not isinstance(value, str):
        raise DistributedMeasurementError(f'{label} output must be text.')
    matches = list(pattern.finditer(value))
    if len(matches) != 1:
        raise DistributedMeasurementError(
            f'{label} output must contain exactly one attestation marker.'
        )
    return matches[0]


def _private_ipv4(value: Any, label: str) -> str:
    if not isinstance(value, str) or value != value.strip():
        raise DistributedMeasurementError(
            f'{label} must be a private IPv4 address.'
        )
    try:
        parsed = ip_address(value)
    except (TypeError, ValueError):
        raise DistributedMeasurementError(
            f'{label} must be a private IPv4 address.'
        ) from None
    if parsed.version != 4 or not parsed.is_private:
        raise DistributedMeasurementError(
            f'{label} must be a private IPv4 address.'
        )
    return str(parsed)


def load_generator_attestation_command() -> str:
    """Build wrk2 and attest every pinned load-generator input."""

    root = deathstarbench.REMOTE_ROOT
    social = f'{root}/socialNetwork'
    initializer = f'{social}/scripts/init_social_graph.py'
    nodes = (
        f'{social}/datasets/social-graph/{DATASET_GRAPH}/'
        f'{DATASET_GRAPH}.nodes'
    )
    edges = (
        f'{social}/datasets/social-graph/{DATASET_GRAPH}/'
        f'{DATASET_GRAPH}.edges'
    )
    lua = f'{social}/wrk2/scripts/social-network/mixed-workload.lua'
    prepare = deathstarbench.load_generator_prepare_command()
    return (
        f'{prepare}; '
        'test "$(uname -m)" = x86_64; '
        f'test "$(git -C {root} rev-parse HEAD)" = '
        f'"{deathstarbench.REVISION}"; '
        f'test "$(sha256sum {initializer} | awk \'{{print $1}}\')" = '
        f'"{INITIALIZER_SOURCE_SHA256}"; '
        f'test "$(sha256sum {nodes} | awk \'{{print $1}}\')" = '
        f'"{REED98_NODES_SHA256}"; '
        f'test "$(sha256sum {edges} | awk \'{{print $1}}\')" = '
        f'"{REED98_EDGES_SHA256}"; '
        f'test "$(sha256sum {lua} | awk \'{{print $1}}\')" = '
        f'"{MIXED_WORKLOAD_LUA_SHA256}"; '
        f'WRK_SHA256=$(sha256sum {root}/wrk2/wrk | awk \'{{print $1}}\'); '
        'COMPILER_VERSION=$(cc --version | head -n 1); '
        'test -n "$COMPILER_VERSION"; '
        'COMPILER_VERSION_SHA256=$(printf %s "$COMPILER_VERSION" '
        '| sha256sum | awk \'{print $1}\'); '
        'echo "DISTRIBUTED_DSB_LOAD_GENERATOR_READY '
        f'architecture=x86_64 revision={deathstarbench.REVISION} '
        'wrk_sha256=$WRK_SHA256 '
        'compiler_version_sha256=$COMPILER_VERSION_SHA256"'
    )


def parse_load_generator_attestation(value: str | bytes) -> dict[str, Any]:
    match = _exact_marker(
        _LOADGEN_MARKER_RE,
        value,
        'Load-generator attestation',
    )
    architecture, revision, wrk_sha256, compiler_version_sha256 = match.groups()
    if architecture != 'x86_64' or revision != deathstarbench.REVISION:
        raise DistributedMeasurementError(
            'The load-generator attestation conflicts with the pinned driver.'
        )
    return {
        'architecture': architecture,
        'upstream_revision': revision,
        'load_driver_revision': DISTRIBUTED_LOAD_DRIVER_REVISION,
        'wrk_binary_sha256': wrk_sha256,
        'compiler_version_sha256': compiler_version_sha256,
    }


def non_mutating_frontend_probe_command(target_private_ip: str) -> str:
    """Return a GET-only probe requiring the exact empty JSON object response."""

    target = _private_ipv4(target_private_ip, 'Frontend target')
    route = FRONTEND_PROBE_ROUTE
    return (
        'set -euo pipefail; '
        f'TARGET={target}; PORT={FRONTEND_PORT}; '
        f'ROUTE={route!r}; OUTPUT=/tmp/deathstarbench-frontend-probe.json; '
        'for attempt in $(seq 1 120); do '
        'STATUS=$(curl --silent --show-error --max-time 5 '
        '--output "$OUTPUT" --write-out "%{http_code}" '
        '"http://$TARGET:$PORT$ROUTE" 2>/dev/null || true); '
        'if test "$STATUS" = 200 '
        "&& python3 -c 'import json,sys; "
        'value=json.load(open(sys.argv[1])); '
        "assert value == {}' \"$OUTPUT\" 2>/dev/null; then "
        'echo "DISTRIBUTED_DSB_FRONTEND_READY '
        f'target={target} port={FRONTEND_PORT}"; exit 0; fi; '
        'echo "Waiting for the Social Network frontend ($attempt/120)."; '
        'sleep 5; done; '
        'echo "The Social Network GET-only frontend probe failed." >&2; '
        'exit 1'
    )


def parse_frontend_probe_attestation(
    value: str | bytes,
    expected_target: str | None = None,
) -> dict[str, Any]:
    match = _exact_marker(
        _FRONTEND_MARKER_RE,
        value,
        'Frontend probe',
    )
    target = _private_ipv4(match.group(1), 'Attested frontend target')
    port = int(match.group(2))
    if port != FRONTEND_PORT:
        raise DistributedMeasurementError(
            'The frontend probe attested an unexpected port.'
        )
    if expected_target is not None and target != _private_ipv4(
        expected_target,
        'Expected frontend target',
    ):
        raise DistributedMeasurementError(
            'The frontend probe attested an unexpected target.'
        )
    return {
        'target_private_ip': target,
        'port': port,
        'route': FRONTEND_PROBE_ROUTE,
    }


def social_network_initialization_command(target_private_ip: str) -> str:
    """Wrap the existing initializer with exact source and count checks."""

    target = _private_ipv4(target_private_ip, 'Initialization target')
    root = deathstarbench.REMOTE_ROOT
    social = f'{root}/socialNetwork'
    initializer = f'{social}/scripts/init_social_graph.py'
    nodes = (
        f'{social}/datasets/social-graph/{DATASET_GRAPH}/'
        f'{DATASET_GRAPH}.nodes'
    )
    edges = (
        f'{social}/datasets/social-graph/{DATASET_GRAPH}/'
        f'{DATASET_GRAPH}.edges'
    )
    initialize = deathstarbench.initialize_workload_command(
        'social_network',
        target,
    )
    bounded_initialize = (
        'timeout --signal=TERM --kill-after=30s 2100s '
        f'bash -lc {shlex.quote(initialize)}'
    )
    return (
        'set -euo pipefail; '
        f'test "$(git -C {root} rev-parse HEAD)" = '
        f'"{deathstarbench.REVISION}"; '
        f'test "$(sha256sum {initializer} | awk \'{{print $1}}\')" = '
        f'"{INITIALIZER_SOURCE_SHA256}"; '
        f'test "$(sha256sum {nodes} | awk \'{{print $1}}\')" = '
        f'"{REED98_NODES_SHA256}"; '
        f'test "$(sha256sum {edges} | awk \'{{print $1}}\')" = '
        f'"{REED98_EDGES_SHA256}"; '
        f'{bounded_initialize}; '
        f'test "$(sha256sum {initializer} | awk \'{{print $1}}\')" = '
        f'"{PATCHED_INITIALIZER_SHA256}"; '
        'mapfile -t DSB_COUNTS < <(awk \'\n'
        '/^Registering Users\\.\\.\\.$/ { phase=1; next }\n'
        '/^Adding follows\\.\\.\\.$/ { phase=2; next }\n'
        '/^Composing posts\\.\\.\\.$/ { phase=3; next }\n'
        '/^Succeeded: [0-9]+$/ { count[phase]+=$2; next }\n'
        'END { for (i=1; i<=3; i++) print count[i]+0 }\n'
        "' /tmp/deathstarbench-social-init.out); "
        'test "${#DSB_COUNTS[@]}" -eq 3; '
        f'test "${{DSB_COUNTS[0]}}" -eq {DATASET_USER_COUNT}; '
        f'test "${{DSB_COUNTS[1]}}" -eq {DATASET_FOLLOW_COUNT}; '
        f'test "${{DSB_COUNTS[2]}}" -eq {DATASET_POST_COUNT}; '
        'echo "DISTRIBUTED_DSB_DATASET_READY '
        f'graph={DATASET_GRAPH} users={DATASET_USER_COUNT} '
        f'follows={DATASET_FOLLOW_COUNT} posts={DATASET_POST_COUNT} '
        f'revision={deathstarbench.REVISION}"'
    )


def _initializer_job_id(job: Mapping[str, Any]) -> str:
    job_id = job.get('id')
    if not isinstance(job_id, str) or not _JOB_ID_RE.fullmatch(job_id):
        raise DistributedMeasurementError(
            'A 12-character lowercase hexadecimal job identifier is required '
            'for the durable initializer.'
        )
    return job_id


def _initializer_unit_name(job_id: str) -> str:
    if not _JOB_ID_RE.fullmatch(job_id):
        raise DistributedMeasurementError(
            'The durable initializer job identifier is invalid.'
        )
    return f'{INITIALIZER_UNIT_PREFIX}{job_id}.service'


def _initializer_run_user(job: Mapping[str, Any]) -> str:
    resources = job.get('resources')
    run_user = resources.get('ssh_user') if isinstance(resources, Mapping) else None
    if not isinstance(run_user, str) or not _REMOTE_USER_RE.fullmatch(run_user):
        raise DistributedMeasurementError(
            'The durable initializer requires a safe benchmark SSH user.'
        )
    return run_user


def _initializer_paths(job_id: str) -> tuple[str, str, str, str]:
    if not _JOB_ID_RE.fullmatch(job_id):
        raise DistributedMeasurementError(
            'The durable initializer job identifier is invalid.'
        )
    state_directory = f'{INITIALIZER_STATE_ROOT}/{job_id}'
    return (
        state_directory,
        f'{state_directory}/initializer.sh',
        f'{state_directory}/initializer.claimed',
        f'{state_directory}/initializer.out',
    )


def _initializer_script(
    job_id: str,
    target_private_ip: str,
    run_user: str,
) -> str:
    """Return the exact guarded payload run by the transient systemd unit."""

    target = _private_ipv4(target_private_ip, 'Initialization target')
    if not _REMOTE_USER_RE.fullmatch(run_user):
        raise DistributedMeasurementError(
            'The durable initializer run user is invalid.'
        )
    state_directory, _, claim_directory, output_path = _initializer_paths(job_id)
    temporary_output_path = f'{output_path}.in-progress'
    payload = social_network_initialization_command(target)
    return (
        '#!/usr/bin/env bash\n'
        'set -euo pipefail\n'
        'umask 077\n'
        f'STATE_DIRECTORY={shlex.quote(state_directory)}\n'
        f'CLAIM_DIRECTORY={shlex.quote(claim_directory)}\n'
        f'TEMPORARY_OUTPUT_PATH={shlex.quote(temporary_output_path)}\n'
        f'FINAL_OUTPUT_PATH={shlex.quote(output_path)}\n'
        f'RUN_USER={shlex.quote(run_user)}\n'
        'test -d "$STATE_DIRECTORY"\n'
        '# mkdir is the non-replayable claim.  A restarted unit exits before\n'
        '# opening (and therefore before truncating) the original output.\n'
        'if ! mkdir -- "$CLAIM_DIRECTORY"; then\n'
        '  echo "The one-shot initializer was already claimed." >&2\n'
        '  exit 125\n'
        'fi\n'
        'test ! -e "$TEMPORARY_OUTPUT_PATH"\n'
        'test ! -e "$FINAL_OUTPUT_PATH"\n'
        '# The root wrapper owns the evidence file, but the benchmark payload\n'
        '# itself runs with the same unprivileged account used over SSH.\n'
        'set +e\n'
        f'runuser --user "$RUN_USER" -- /bin/bash -lc {shlex.quote(payload)} '
        '>"$TEMPORARY_OUTPUT_PATH" 2>&1\n'
        'PAYLOAD_STATUS=$?\n'
        'set -e\n'
        'mv -- "$TEMPORARY_OUTPUT_PATH" "$FINAL_OUTPUT_PATH"\n'
        'exit "$PAYLOAD_STATUS"\n'
    )


def initializer_execution_identity(
    job: Mapping[str, Any],
    target_private_ip: str,
) -> dict[str, str]:
    """Bind the persisted journal to the deterministic unit and payload."""

    job_id = _initializer_job_id(job)
    run_user = _initializer_run_user(job)
    script = _initializer_script(job_id, target_private_ip, run_user)
    return {
        'unit_name': _initializer_unit_name(job_id),
        'run_user': run_user,
        'payload_sha256': hashlib.sha256(script.encode('utf-8')).hexdigest(),
    }


def durable_initializer_dispatch_command(
    job: Mapping[str, Any],
    target_private_ip: str,
) -> str:
    """Install and enqueue the initializer exactly once without waiting for it."""

    job_id = _initializer_job_id(job)
    run_user = _initializer_run_user(job)
    unit_name = _initializer_unit_name(job_id)
    state_directory, script_path, claim_directory, _ = _initializer_paths(job_id)
    script = _initializer_script(job_id, target_private_ip, run_user)
    payload_sha256 = hashlib.sha256(script.encode('utf-8')).hexdigest()
    encoded_script = base64.b64encode(script.encode('utf-8')).decode('ascii')
    return (
        'set -euo pipefail; '
        f'UNIT={shlex.quote(unit_name)}; '
        f'STATE_DIRECTORY={shlex.quote(state_directory)}; '
        f'SCRIPT_PATH={shlex.quote(script_path)}; '
        f'CLAIM_DIRECTORY={shlex.quote(claim_directory)}; '
        'LOAD_STATE=$(sudo systemctl show "$UNIT" '
        '--property=LoadState --value 2>/dev/null || true); '
        'test -z "$LOAD_STATE" -o "$LOAD_STATE" = not-found; '
        'sudo test ! -e "$CLAIM_DIRECTORY"; '
        'sudo install -d -m 0700 "$STATE_DIRECTORY"; '
        'TEMPORARY_SCRIPT=$(mktemp /tmp/deathstarbench-init.XXXXXX); '
        'trap \'rm -f "$TEMPORARY_SCRIPT"\' EXIT; '
        f'printf %s {shlex.quote(encoded_script)} '
        '| base64 --decode >"$TEMPORARY_SCRIPT"; '
        f'test "$(sha256sum "$TEMPORARY_SCRIPT" | awk \'{{print $1}}\')" = '
        f'{shlex.quote(payload_sha256)}; '
        'sudo install -m 0700 "$TEMPORARY_SCRIPT" "$SCRIPT_PATH"; '
        'INSTALLED_PAYLOAD_SHA256=$(sudo sha256sum "$SCRIPT_PATH" '
        '| awk \'{print $1}\'); '
        f'test "$INSTALLED_PAYLOAD_SHA256" = {shlex.quote(payload_sha256)}; '
        'sudo systemd-run --quiet --no-block --unit="$UNIT" '
        '--property=Type=oneshot --property=RemainAfterExit=yes '
        '--property=User=root '
        '--property=Restart=no --property=KillMode=control-group '
        '--property=TimeoutStopSec=30s '
        f'--property=TimeoutStartSec={INITIALIZER_MAX_RUNTIME_SECONDS}s '
        '/bin/bash "$SCRIPT_PATH"; '
        'echo "DISTRIBUTED_DSB_INITIALIZER_DISPATCHED '
        f'unit={unit_name} run_user={run_user} '
        'payload_sha256=$INSTALLED_PAYLOAD_SHA256"'
    )


def parse_initializer_dispatch_attestation(
    value: str | bytes,
    expected: Mapping[str, str],
) -> dict[str, str]:
    match = _exact_marker(
        _INITIALIZER_DISPATCH_MARKER_RE,
        value,
        'Initializer dispatch',
    )
    observed = {
        'unit_name': match.group(1),
        'run_user': match.group(2),
        'payload_sha256': match.group(3),
    }
    _exact_mapping(observed, expected, 'Initializer dispatch attestation')
    return observed


def durable_initializer_status_command(job: Mapping[str, Any]) -> str:
    """Return a read-only, payload-bound query for the run-owned unit."""

    job_id = _initializer_job_id(job)
    run_user = _initializer_run_user(job)
    unit_name = _initializer_unit_name(job_id)
    _, script_path, _, output_path = _initializer_paths(job_id)
    return (
        'set -euo pipefail; '
        f'UNIT={shlex.quote(unit_name)}; '
        f'RUN_USER={shlex.quote(run_user)}; '
        f'SCRIPT_PATH={shlex.quote(script_path)}; '
        f'OUTPUT_PATH={shlex.quote(output_path)}; '
        'LOAD=unknown; ACTIVE=unknown; SUB=unknown; RESULT=unknown; '
        'EXEC_CODE=0; EXEC_STATUS=0; INVOCATION_ID=none; '
        'START_MONO_USEC=0; EXIT_MONO_USEC=0; '
        'SERVICE_USER=none; EXEC_START=""; EXEC_START_MATCH=no; '
        'while IFS="=" read -r KEY VALUE; do '
        'case "$KEY" in '
        'LoadState) LOAD="$VALUE";; ActiveState) ACTIVE="$VALUE";; '
        'SubState) SUB="$VALUE";; Result) RESULT="$VALUE";; '
        'User) SERVICE_USER="$VALUE";; ExecStart) EXEC_START="$VALUE";; '
        'ExecMainCode) EXEC_CODE="$VALUE";; '
        'ExecMainStatus) EXEC_STATUS="$VALUE";; '
        'InvocationID) INVOCATION_ID="$VALUE";; '
        'ExecMainStartTimestampMonotonic) START_MONO_USEC="$VALUE";; '
        'ExecMainExitTimestampMonotonic) EXIT_MONO_USEC="$VALUE";; esac; '
        'done < <(sudo systemctl show "$UNIT" --no-pager '
        '--property=LoadState --property=ActiveState --property=SubState '
        '--property=Result --property=ExecMainCode '
        '--property=ExecMainStatus --property=InvocationID '
        '--property=User --property=ExecStart '
        '--property=ExecMainStartTimestampMonotonic '
        '--property=ExecMainExitTimestampMonotonic 2>/dev/null || true); '
        'test -n "$SERVICE_USER" || SERVICE_USER=none; '
        'if test "$LOAD" = loaded; then '
        'case "$EXEC_START" in '
        '*"path=/bin/bash ;"*"argv[]=/bin/bash $SCRIPT_PATH ;"*) '
        'EXEC_START_MATCH=yes;; esac; fi; '
        'case "$EXEC_CODE" in ""|*[!0-9]*) EXEC_CODE=0;; esac; '
        'case "$EXEC_STATUS" in ""|*[!0-9]*) EXEC_STATUS=0;; esac; '
        'case "$INVOCATION_ID" in '
        '[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]'
        '[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]'
        '[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]'
        '[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]'
        '[0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;; *) INVOCATION_ID=none;; esac; '
        'case "$START_MONO_USEC" in ""|*[!0-9]*) START_MONO_USEC=0;; esac; '
        'case "$EXIT_MONO_USEC" in ""|*[!0-9]*) EXIT_MONO_USEC=0;; esac; '
        'PAYLOAD_SHA256=none; '
        'if sudo test -f "$SCRIPT_PATH" '
        '&& sudo test ! -L "$SCRIPT_PATH" '
        '&& test "$(sudo stat --format=%u:%g:%a "$SCRIPT_PATH")" '
        '= 0:0:700; then '
        'PAYLOAD_SHA256=$(sudo sha256sum "$SCRIPT_PATH" | awk \'{print $1}\'); '
        'fi; '
        'OUTPUT_BYTES=0; OUTPUT_SHA256=none; '
        'TERMINAL=no; '
        'if test "$LOAD" = loaded -a "$ACTIVE" = active '
        '-a "$SUB" = exited -a "$RESULT" = success '
        '-a "$EXEC_CODE" = 1 -a "$EXEC_STATUS" = 0; then '
        'TERMINAL=yes; '
        'elif test "$ACTIVE" = failed; then TERMINAL=yes; '
        'elif test "$LOAD" = loaded -a "$ACTIVE" = inactive '
        '-a "$RESULT" != success -a "$RESULT" != unknown; then '
        'TERMINAL=yes; fi; '
        'if test "$TERMINAL" = yes && sudo test -f "$OUTPUT_PATH"; then '
        'OUTPUT_BYTES=$(sudo stat --format=%s "$OUTPUT_PATH"); '
        f'if test "$OUTPUT_BYTES" -le {MAX_INITIALIZER_TRANSCRIPT_BYTES}; then '
        'OUTPUT_SHA256=$(sudo sha256sum "$OUTPUT_PATH" | awk \'{print $1}\'); '
        'fi; '
        'fi; '
        'echo "DISTRIBUTED_DSB_INITIALIZER_STATUS '
        f'unit={unit_name} '
        f'run_user={run_user} payload_sha256=$PAYLOAD_SHA256 '
        'service_user=$SERVICE_USER exec_start_match=$EXEC_START_MATCH '
        'load=$LOAD active=$ACTIVE sub=$SUB result=$RESULT '
        'exec_code=$EXEC_CODE exec_status=$EXEC_STATUS '
        'invocation_id=$INVOCATION_ID start_mono_usec=$START_MONO_USEC '
        'exit_mono_usec=$EXIT_MONO_USEC output_bytes=$OUTPUT_BYTES '
        'output_sha256=$OUTPUT_SHA256"; '
        'if test "$OUTPUT_SHA256" != none; then sudo cat "$OUTPUT_PATH"; fi'
    )


def parse_durable_initializer_status(
    value: str | bytes,
    expected: Mapping[str, str],
) -> dict[str, Any]:
    """Parse one read-only unit observation and verify terminal output bytes."""

    if isinstance(value, bytes):
        try:
            text = value.decode('utf-8')
        except UnicodeDecodeError as exc:
            raise DistributedMeasurementError(
                'Initializer status output is not UTF-8.'
            ) from exc
    elif isinstance(value, str):
        text = value
    else:
        raise DistributedMeasurementError(
            'Initializer status output must be text.'
        )
    match = _exact_marker(
        _INITIALIZER_STATUS_MARKER_RE,
        text,
        'Initializer status',
    )
    (
        unit_name,
        run_user,
        payload_sha256,
        service_user,
        exec_start_match,
        load_state,
        active_state,
        sub_state,
        result,
        exec_code,
        exec_status_text,
        invocation_id,
        start_mono_usec_text,
        exit_mono_usec_text,
        output_bytes_text,
        output_sha256,
    ) = match.groups()
    expected_unit_name = expected.get('unit_name')
    expected_run_user = expected.get('run_user')
    expected_payload_sha256 = expected.get('payload_sha256')
    if (
        not isinstance(expected_unit_name, str)
        or not isinstance(expected_run_user, str)
        or not _REMOTE_USER_RE.fullmatch(expected_run_user)
        or not isinstance(expected_payload_sha256, str)
        or not _SHA256_RE.fullmatch(expected_payload_sha256)
    ):
        raise DistributedMeasurementError(
            'The expected initializer execution identity is invalid.'
        )
    if unit_name != expected_unit_name:
        raise DistributedMeasurementError(
            'The initializer status belongs to a different unit.'
        )
    if run_user != expected_run_user:
        raise DistributedMeasurementError(
            'The initializer status names a different unprivileged run user.'
        )
    if payload_sha256 not in {'none', expected_payload_sha256}:
        raise DistributedMeasurementError(
            'The installed initializer payload does not match its persisted '
            'SHA-256 identity.'
        )
    output = text[match.end():]
    if output.startswith('\r\n'):
        output = output[2:]
    elif output.startswith('\n'):
        output = output[1:]
    exec_status = int(exec_status_text)
    start_mono_usec = int(start_mono_usec_text)
    exit_mono_usec = int(exit_mono_usec_text)
    output_bytes = int(output_bytes_text)
    terminal_success = (
        load_state == 'loaded'
        and active_state == 'active'
        and sub_state == 'exited'
        and result == 'success'
        and exec_code == '1'
        and exec_status == 0
    )
    terminal_failure = active_state == 'failed' or (
        load_state == 'loaded'
        and active_state == 'inactive'
        and result not in {'success', 'unknown'}
    )
    not_found = load_state in {'not-found', 'unknown'}
    pending = (
        load_state == 'loaded'
        and not terminal_success
        and not terminal_failure
        and active_state in {'inactive', 'activating', 'deactivating', 'active'}
        and not (active_state == 'active' and sub_state == 'exited')
    )
    if not any((terminal_success, terminal_failure, not_found, pending)):
        raise DistributedMeasurementError(
            'The durable initializer returned an unknown systemd state.'
        )

    # The exact payload hash transitively binds the generated RUN_USER
    # assignment and runuser invocation.  The systemd properties separately
    # prove that root launched that wrapper through the sole approved argv.
    if load_state == 'loaded' and (
        payload_sha256 != expected_payload_sha256
        or service_user != 'root'
        or exec_start_match != 'yes'
    ):
        raise DistributedMeasurementError(
            'The loaded initializer unit does not match its persisted payload, '
            'service-user, or ExecStart contract.'
        )
    if not_found and (
        service_user != 'none'
        or exec_start_match != 'no'
    ):
        raise DistributedMeasurementError(
            'A missing initializer unit returned conflicting systemd identity '
            'evidence.'
        )

    if terminal_success or terminal_failure:
        if (
            invocation_id == 'none'
            or invocation_id == '0' * 32
            or start_mono_usec <= 0
            or exit_mono_usec < start_mono_usec
        ):
            raise DistributedMeasurementError(
                'The terminal initializer status lacks a valid systemd '
                'invocation identity or monotonic execution timestamps.'
            )
    if output_sha256 == 'none':
        if output_bytes or output:
            raise DistributedMeasurementError(
                'Initializer status returned unattested output.'
            )
    else:
        encoded_output = output.encode('utf-8')
        if output_bytes != len(encoded_output):
            raise DistributedMeasurementError(
                'Initializer output length does not match its durable '
                'attestation.'
            )
        if output_bytes > MAX_INITIALIZER_TRANSCRIPT_BYTES:
            raise DistributedMeasurementError(
                'Initializer output exceeds the safe transcript limit.'
            )
        if hashlib.sha256(encoded_output).hexdigest() != output_sha256:
            raise DistributedMeasurementError(
                'Initializer output does not match its durable SHA-256 '
                'attestation.'
            )
    if terminal_success and (output_sha256 == 'none' or not output_bytes):
        raise DistributedMeasurementError(
            'The successful initializer is missing its finalized transcript '
            'or the transcript exceeded the safe size limit.'
        )
    if (pending or not_found) and output_sha256 != 'none':
        raise DistributedMeasurementError(
            'A nonterminal initializer status exposed mutable output.'
        )

    observation = {
        'unit_name': unit_name,
        'run_user': run_user,
        'payload_sha256': payload_sha256,
        'service_user': service_user,
        'exec_start_match': exec_start_match == 'yes',
        'load_state': load_state,
        'active_state': active_state,
        'sub_state': sub_state,
        'result': result,
        'exec_code': exec_code,
        'exec_status': exec_status,
        'invocation_id': invocation_id,
        'start_monotonic_usec': start_mono_usec,
        'exit_monotonic_usec': exit_mono_usec,
        'output_bytes': output_bytes,
        'output_sha256': output_sha256,
        'output': output,
    }
    if not_found:
        observation['state'] = 'not_found'
    elif terminal_success:
        observation['state'] = 'succeeded'
    elif terminal_failure:
        observation['state'] = 'failed'
    elif pending:
        observation['state'] = 'pending'
    return observation


def parse_dataset_attestation(value: str | bytes) -> dict[str, Any]:
    match = _exact_marker(_DATASET_MARKER_RE, value, 'Dataset initialization')
    graph, users, follows, posts, revision = match.groups()
    observed = {
        'graph': graph,
        'users': int(users),
        'follows': int(follows),
        'posts': int(posts),
        'upstream_revision': revision,
        'dataset_revision': DISTRIBUTED_DATASET_REVISION,
    }
    expected = {
        'graph': DATASET_GRAPH,
        'users': DATASET_USER_COUNT,
        'follows': DATASET_FOLLOW_COUNT,
        'posts': DATASET_POST_COUNT,
        'upstream_revision': deathstarbench.REVISION,
        'dataset_revision': DISTRIBUTED_DATASET_REVISION,
    }
    if observed != expected:
        raise DistributedMeasurementError(
            'The Social Network initializer cardinalities drifted.'
        )
    return observed


def dataset_database_attestation_command() -> str:
    """Read the initialized cardinalities directly from the four databases."""

    kubectl = f'sudo {K3S_BINARY} kubectl -n {WORKLOAD_NAMESPACE}'
    user_js = (
        'var c=db.getSiblingDB("user").getCollection("user");'
        'print(c.countDocuments({}));'
    )
    graph_js = (
        'var c=db.getSiblingDB("social-graph").getCollection("social-graph");'
        'var r=c.aggregate([{$group:{_id:null,'
        'followers:{$sum:{$size:{$ifNull:["$followers",[]]}}},'
        'followees:{$sum:{$size:{$ifNull:["$followees",[]]}}}}}]).toArray();'
        'var x=r.length?r[0]:{followers:0,followees:0};'
        'print(c.countDocuments({})+" "+x.followers+" "+x.followees);'
    )
    post_js = (
        'var c=db.getSiblingDB("post").getCollection("post");'
        'print(c.countDocuments({}));'
    )
    timeline_js = (
        'var c=db.getSiblingDB("user-timeline").getCollection("user-timeline");'
        'var r=c.aggregate([{$group:{_id:null,'
        'posts:{$sum:{$size:{$ifNull:["$posts",[]]}}}}}]).toArray();'
        'var x=r.length?r[0]:{posts:0};'
        'print(c.countDocuments({})+" "+x.posts);'
    )
    return (
        'set -euo pipefail; '
        'pod_for() { local COMPONENT="$1"; local -a PODS=(); '
        f'mapfile -t PODS < <({kubectl} get pods '
        '-l app.kubernetes.io/component="$COMPONENT" -o name); '
        'test "${#PODS[@]}" -eq 1; '
        'printf %s "${PODS[0]#pod/}"; }; '
        'USER_POD=$(pod_for user-mongodb); '
        'GRAPH_POD=$(pod_for social-graph-mongodb); '
        'POST_POD=$(pod_for post-storage-mongodb); '
        'TIMELINE_POD=$(pod_for user-timeline-mongodb); '
        f'USER_COUNT=$({kubectl} exec pod/$USER_POD -- '
        f'mongo --quiet --eval \'{user_js}\' | tail -n 1 | tr -d "\\r"); '
        f'GRAPH_DATA=$({kubectl} exec pod/$GRAPH_POD -- '
        f'mongo --quiet --eval \'{graph_js}\' '
        '| tail -n 1 | tr -d "\\r"); '
        'read -r GRAPH_USERS FOLLOWERS FOLLOWEES <<<"$GRAPH_DATA"; '
        f'POSTS=$({kubectl} exec pod/$POST_POD -- '
        f'mongo --quiet --eval \'{post_js}\' | tail -n 1 | tr -d "\\r"); '
        f'TIMELINE_DATA=$({kubectl} exec pod/$TIMELINE_POD -- '
        f'mongo --quiet --eval \'{timeline_js}\' '
        '| tail -n 1 | tr -d "\\r"); '
        'read -r TIMELINE_USERS TIMELINE_POSTS <<<"$TIMELINE_DATA"; '
        'for VALUE in "$USER_COUNT" "$GRAPH_USERS" "$FOLLOWERS" '
        '"$FOLLOWEES" "$POSTS" "$TIMELINE_USERS" "$TIMELINE_POSTS"; do '
        'case "$VALUE" in ""|*[!0-9]*) '
        'echo "Invalid MongoDB dataset cardinality: $VALUE" >&2; exit 1;; '
        'esac; done; '
        'echo "DISTRIBUTED_DSB_DATABASE_DATA users=$USER_COUNT '
        'social_graph_users=$GRAPH_USERS followers=$FOLLOWERS '
        'followees=$FOLLOWEES posts=$POSTS timeline_users=$TIMELINE_USERS '
        'timeline_posts=$TIMELINE_POSTS"'
    )


def parse_dataset_database_attestation(
    value: str | bytes,
    *,
    initialized: bool,
) -> dict[str, int]:
    match = _exact_marker(_DATABASE_MARKER_RE, value, 'Database dataset')
    keys = (
        'users',
        'social_graph_users',
        'followers',
        'followees',
        'posts',
        'timeline_users',
        'timeline_posts',
    )
    observed = dict(zip(keys, (int(item) for item in match.groups()), strict=True))
    expected = (
        {
            'users': DATASET_USER_COUNT,
            'social_graph_users': DATASET_USER_COUNT,
            'followers': DATASET_FOLLOW_COUNT,
            'followees': DATASET_FOLLOW_COUNT,
            'posts': DATASET_POST_COUNT,
            'timeline_users': DATASET_TIMELINE_USER_COUNT,
            'timeline_posts': DATASET_POST_COUNT,
        }
        if initialized
        else {key: 0 for key in keys}
    )
    if observed != expected:
        condition = 'initialized' if initialized else 'empty'
        raise DistributedMeasurementError(
            f'The Social Network databases are not in the exact {condition} state.'
        )
    return observed


def parse_wrk2_execution_output(
    value: str | bytes,
    *,
    label: str,
) -> dict[str, Any]:
    """Parse one complete wrk2 invocation without accepting concatenation."""

    if isinstance(value, bytes):
        try:
            value = value.decode('utf-8')
        except UnicodeDecodeError as exc:
            raise DistributedMeasurementError(
                f'{label} output is not UTF-8.'
            ) from exc
    if not isinstance(value, str):
        raise DistributedMeasurementError(f'{label} output must be text.')
    if (
        len(_WRK2_METRICS_LINE_RE.findall(value)) != 1
        or len(_WRK2_SENT_LINE_RE.findall(value)) != 1
    ):
        raise DistributedMeasurementError(
            f'{label} output must contain exactly one metrics marker and '
            'one sent-request line.'
        )
    try:
        return deathstarbench.parse_wrk2_output(value)
    except ValueError as exc:
        raise DistributedMeasurementError(f'{label} output is invalid: {exc}') from exc


def _load_command(
    target_private_ip: str,
    options: DeathStarBenchOptions,
    duration_seconds: int,
) -> str:
    lua = (
        f'{deathstarbench.REMOTE_ROOT}/socialNetwork/wrk2/scripts/'
        'social-network/mixed-workload.lua'
    )
    command = deathstarbench.load_command(
        'social_network',
        target_private_ip,
        options,
        duration_seconds,
    )
    bounded_seconds = int(duration_seconds) + 240
    return (
        'set -euo pipefail; '
        f'test "$(sha256sum {lua} | awk \'{{print $1}}\')" = '
        f'"{MIXED_WORKLOAD_LUA_SHA256}"; '
        f'export max_user_index={DATASET_USER_COUNT}; '
        f'timeout --signal=TERM --kill-after=30s {bounded_seconds}s '
        f'bash -lc {shlex.quote(command)}'
    )


def _json_value(value: Any) -> Any:
    """Normalize immutable parser values to persistence-safe JSON values."""

    def normalize(item: Any) -> Any:
        if isinstance(item, Mapping):
            normalized = {}
            for key, nested in item.items():
                if not isinstance(key, str):
                    raise TypeError('JSON object keys must be strings.')
                normalized[key] = normalize(nested)
            return normalized
        if isinstance(item, (list, tuple)):
            return [normalize(nested) for nested in item]
        if item is None or isinstance(item, (str, int, float, bool)):
            return item
        raise TypeError(f'Unsupported JSON value {type(item).__name__}.')

    try:
        normalized = normalize(value)
        return json.loads(
            json.dumps(
                normalized,
                sort_keys=True,
                separators=(',', ':'),
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as exc:
        raise DistributedMeasurementError(
            'An execution attestation is not JSON serializable.'
        ) from exc


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(',', ':'),
    ).encode('utf-8')
    return 'sha256:' + hashlib.sha256(encoded).hexdigest()


def _sha256_text(value: str) -> str:
    return 'sha256:' + hashlib.sha256(value.encode('utf-8')).hexdigest()


def _settings(options: DeathStarBenchOptions) -> dict[str, Any]:
    return {
        'workload': options.workload,
        'warmup_seconds': int(options.warmup_seconds),
        'duration_seconds': int(options.duration_seconds),
        'threads': int(options.threads),
        'connections': int(options.connections),
        'request_rate': int(options.request_rate),
    }


def _validated_options(value: Any) -> DeathStarBenchOptions:
    if (
        not isinstance(value, (DeathStarBenchOptions, Mapping))
        and all(
            hasattr(value, field)
            for field in (
                'topology_id',
                'runtime_id',
                'workload',
                'warmup_seconds',
                'duration_seconds',
                'threads',
                'connections',
                'request_rate',
            )
        )
    ):
        value = {
            field: getattr(value, field)
            for field in (
                'topology_id',
                'runtime_id',
                'workload',
                'warmup_seconds',
                'duration_seconds',
                'threads',
                'connections',
                'request_rate',
            )
        }
    try:
        options = DeathStarBenchOptions.model_validate(value)
    except Exception as exc:
        raise DistributedMeasurementError(
            f'Distributed DeathStarBench options are invalid: {exc}'
        ) from exc
    if (
        options.topology_id != DISTRIBUTED_TIERED_TOPOLOGY_ID
        or options.runtime_id != K3S_RUNTIME_ID
        or options.workload != 'social_network'
    ):
        raise DistributedMeasurementError(
            'Measurement requires distributed_tiered_v1/k3s_v1 and the '
            'Social Network workload.'
        )
    if (
        options.connections < options.threads
        or options.request_rate < options.threads
        or options.connections % options.threads
        or options.request_rate % options.threads
    ):
        raise DistributedMeasurementError(
            'Connections and request rate must each be at least and evenly '
            'divisible by the wrk2 thread count.'
        )
    return options


def _journal_identity(
    plan: AzureK3sCandidatePlan,
    bundle: Any,
    options: DeathStarBenchOptions,
) -> dict[str, Any]:
    load_generator = plan.load_generator
    return {
        'schema_version': EXECUTION_JOURNAL_SCHEMA_VERSION,
        'runtime_revision': DISTRIBUTED_RUNTIME_REVISION,
        'topology_fingerprint': plan.topology_fingerprint,
        'workload_revision': DISTRIBUTED_WORKLOAD_REVISION,
        'image_set_revision': DISTRIBUTED_IMAGE_SET_REVISION,
        'manifest_source_sha256': bundle.manifest_source_sha256,
        'rendered_manifest_sha256': bundle.rendered_manifest_sha256,
        'image_lock_fingerprint': bundle.image_lock_fingerprint,
        'dataset_revision': DISTRIBUTED_DATASET_REVISION,
        'load_driver_revision': DISTRIBUTED_LOAD_DRIVER_REVISION,
        'measurement_revision': DISTRIBUTED_MEASUREMENT_REVISION,
        'application_private_ip': plan.host('application').private_ip,
        'load_generator_private_ip': load_generator.private_ip,
        'load_generator_node_name': load_generator.node_name,
        'load_generator_architecture': load_generator.architecture,
        'settings': _settings(options),
    }


def _exact_mapping(value: Any, expected: Mapping[str, Any], label: str) -> None:
    if not isinstance(value, Mapping) or dict(value) != dict(expected):
        raise DistributedMeasurementError(f'{label} is invalid or stale.')


def _validate_dataset_journal(value: Any, *, ready: bool) -> None:
    if not isinstance(value, Mapping):
        raise DistributedMeasurementError('The dataset journal is invalid.')
    base_fields = {
        'dataset_revision',
        'graph',
        'pre_initialization_database',
        'initializer_execution',
    }
    expected_fields = base_fields | ({'initializer', 'database'} if ready else set())
    if set(value) != expected_fields:
        raise DistributedMeasurementError('The dataset journal schema is invalid.')
    if (
        value['dataset_revision'] != DISTRIBUTED_DATASET_REVISION
        or value['graph'] != DATASET_GRAPH
    ):
        raise DistributedMeasurementError('The dataset journal identity drifted.')
    expected_empty = {
        key: 0
        for key in (
            'users',
            'social_graph_users',
            'followers',
            'followees',
            'posts',
            'timeline_users',
            'timeline_posts',
        )
    }
    _exact_mapping(
        value['pre_initialization_database'],
        expected_empty,
        'Pre-initialization database attestation',
    )
    execution = value['initializer_execution']
    expected_execution_fields = {
        'unit_name',
        'run_user',
        'payload_sha256',
    } | ({
        'invocation_id',
        'start_monotonic_usec',
        'exit_monotonic_usec',
        'initializer_log_bytes',
        'initializer_log_sha256',
    } if ready else set())
    if (
        not isinstance(execution, Mapping)
        or set(execution) != expected_execution_fields
        or not isinstance(execution.get('unit_name'), str)
        or not re.fullmatch(
            rf'{re.escape(INITIALIZER_UNIT_PREFIX)}[0-9a-f]{{12}}\.service',
            execution['unit_name'],
        )
        or not isinstance(execution.get('run_user'), str)
        or not _REMOTE_USER_RE.fullmatch(execution['run_user'])
        or not isinstance(execution.get('payload_sha256'), str)
        or not _SHA256_RE.fullmatch(execution['payload_sha256'])
    ):
        raise DistributedMeasurementError(
            'The durable initializer execution attestation is invalid.'
        )
    if ready and (
        not isinstance(execution.get('invocation_id'), str)
        or not re.fullmatch(r'[0-9a-f]{32}', execution['invocation_id'])
        or execution['invocation_id'] == '0' * 32
        or type(execution.get('start_monotonic_usec')) is not int
        or execution['start_monotonic_usec'] <= 0
        or type(execution.get('exit_monotonic_usec')) is not int
        or execution['exit_monotonic_usec'] < execution['start_monotonic_usec']
        or type(execution.get('initializer_log_bytes')) is not int
        or not 0 < execution['initializer_log_bytes'] <= MAX_INITIALIZER_TRANSCRIPT_BYTES
        or not isinstance(execution.get('initializer_log_sha256'), str)
        or not _SHA256_RE.fullmatch(execution['initializer_log_sha256'])
    ):
        raise DistributedMeasurementError(
            'The completed durable initializer evidence is invalid.'
        )
    if ready:
        _exact_mapping(
            value['initializer'],
            {
                'graph': DATASET_GRAPH,
                'users': DATASET_USER_COUNT,
                'follows': DATASET_FOLLOW_COUNT,
                'posts': DATASET_POST_COUNT,
                'upstream_revision': deathstarbench.REVISION,
                'dataset_revision': DISTRIBUTED_DATASET_REVISION,
            },
            'Initializer attestation',
        )
        _exact_mapping(
            value['database'],
            {
                'users': DATASET_USER_COUNT,
                'social_graph_users': DATASET_USER_COUNT,
                'followers': DATASET_FOLLOW_COUNT,
                'followees': DATASET_FOLLOW_COUNT,
                'posts': DATASET_POST_COUNT,
                'timeline_users': DATASET_TIMELINE_USER_COUNT,
                'timeline_posts': DATASET_POST_COUNT,
            },
            'Initialized database attestation',
        )


def _validate_execution_identity(
    value: Any,
    *,
    rendered_manifest_sha256: str,
) -> None:
    if not isinstance(value, Mapping) or set(value) != {
        'schema_version',
        'workload_revision',
        'bundle_fingerprint',
        'pods',
    }:
        raise DistributedMeasurementError(
            'The workload execution identity schema is invalid.'
        )
    if (
        value['schema_version'] != 1
        or value['workload_revision'] != DISTRIBUTED_WORKLOAD_REVISION
        or value['bundle_fingerprint'] != rendered_manifest_sha256
        or not isinstance(value['pods'], list)
    ):
        raise DistributedMeasurementError(
            'The workload execution identity conflicts with the candidate.'
        )
    components = []
    for pod in value['pods']:
        if not isinstance(pod, Mapping) or set(pod) != {
            'component',
            'name',
            'uid',
            'node_name',
            'restart_count',
            'image_id',
        }:
            raise DistributedMeasurementError(
                'A workload pod execution identity is invalid.'
            )
        components.append(pod['component'])
        if (
            pod['component'] not in EXPECTED_COMPONENTS
            or any(
                not isinstance(pod[field], str)
                or not pod[field]
                or pod[field] != pod[field].strip()
                for field in ('name', 'uid', 'node_name', 'image_id')
            )
            or pod['restart_count'] != 0
            or type(pod['restart_count']) is not int
        ):
            raise DistributedMeasurementError(
                'A workload pod execution identity drifted or restarted.'
            )
    if components != sorted(EXPECTED_COMPONENTS):
        raise DistributedMeasurementError(
            'The workload pod execution inventory is incomplete.'
        )


def _validate_measurement_attestation(
    value: Any,
    *,
    complete: bool,
    rendered_manifest_sha256: str,
) -> None:
    if not isinstance(value, Mapping):
        raise DistributedMeasurementError(
            'The workload execution attestation is invalid.'
        )
    expected_fields = {'pre_workload_execution'}
    if complete:
        expected_fields |= {
            'post_workload_execution',
            'result_id',
            'metrics_sha256',
            'output_sha256',
        }
    if set(value) != expected_fields:
        raise DistributedMeasurementError(
            'The workload execution attestation schema is invalid.'
        )
    pre = value['pre_workload_execution']
    _validate_execution_identity(
        pre,
        rendered_manifest_sha256=rendered_manifest_sha256,
    )
    if complete:
        _validate_execution_identity(
            value['post_workload_execution'],
            rendered_manifest_sha256=rendered_manifest_sha256,
        )
        if value['post_workload_execution'] != pre:
            raise DistributedMeasurementError(
                'The workload pod identity changed during measurement.'
            )
        if value['result_id'] != 'deathstarbench':
            raise DistributedMeasurementError('The result attestation is invalid.')
        for field in ('metrics_sha256', 'output_sha256'):
            if not isinstance(value[field], str) or not _PREFIXED_SHA256_RE.fullmatch(
                value[field]
            ):
                raise DistributedMeasurementError(
                    f'The result attestation has invalid {field}.'
                )


def _validate_execution_journal(
    journal: Any,
    *,
    identity: Mapping[str, Any],
) -> None:
    if not isinstance(journal, Mapping):
        raise DistributedMeasurementError('The execution journal is invalid.')
    expected_fields = {
        *identity,
        'state',
        'load_generator_attestation',
        'frontend_attestation',
        'dataset_attestation',
        'warmup_metrics',
        'measurement_attestation',
    }
    if set(journal) != expected_fields:
        raise DistributedMeasurementError(
            'The execution journal schema is invalid.'
        )
    if any(journal[key] != value for key, value in identity.items()):
        raise DistributedMeasurementError(
            'The execution journal conflicts with the current candidate.'
        )
    state = journal['state']
    if state not in _EXECUTION_STATES:
        raise DistributedMeasurementError('The execution journal state is invalid.')
    state_index = _EXECUTION_STATES.index(state)
    if state_index == 0:
        if any(
            journal[field] is not None
            for field in (
                'load_generator_attestation',
                'frontend_attestation',
                'dataset_attestation',
                'warmup_metrics',
                'measurement_attestation',
            )
        ):
            raise DistributedMeasurementError(
                'The execution journal records evidence before preparation.'
            )
        return
    load_generator_attestation = journal['load_generator_attestation']
    if not isinstance(load_generator_attestation, Mapping) or any(
        not isinstance(load_generator_attestation.get(field), str)
        or not _SHA256_RE.fullmatch(load_generator_attestation[field])
        for field in ('wrk_binary_sha256', 'compiler_version_sha256')
    ):
        raise DistributedMeasurementError(
            'The load-generator build attestation is invalid.'
        )
    _exact_mapping(
        load_generator_attestation,
        {
            'architecture': 'x86_64',
            'upstream_revision': deathstarbench.REVISION,
            'load_driver_revision': DISTRIBUTED_LOAD_DRIVER_REVISION,
            'wrk_binary_sha256': load_generator_attestation[
                'wrk_binary_sha256'
            ],
            'compiler_version_sha256': load_generator_attestation[
                'compiler_version_sha256'
            ],
        },
        'Load-generator journal attestation',
    )
    if state_index == 1:
        if any(
            journal[field] is not None
            for field in (
                'frontend_attestation',
                'dataset_attestation',
                'warmup_metrics',
                'measurement_attestation',
            )
        ):
            raise DistributedMeasurementError(
                'The load-generator-ready journal contains later evidence.'
            )
        return
    _exact_mapping(
        journal['frontend_attestation'],
        {
            'target_private_ip': identity['application_private_ip'],
            'port': FRONTEND_PORT,
            'route': FRONTEND_PROBE_ROUTE,
        },
        'Frontend journal attestation',
    )
    _validate_dataset_journal(
        journal['dataset_attestation'],
        ready=state != 'initialization_started',
    )
    warmup_seconds = identity['settings']['warmup_seconds']
    if state == 'initialization_started':
        if (
            journal['warmup_metrics'] is not None
            or journal['measurement_attestation'] is not None
        ):
            raise DistributedMeasurementError(
                'The initialization journal contains measurement evidence.'
            )
        return
    if state == 'dataset_ready':
        if (
            journal['warmup_metrics'] is not None
            or journal['measurement_attestation'] is not None
        ):
            raise DistributedMeasurementError(
                'The dataset-ready journal contains measurement evidence.'
            )
        return
    if state in {'warmup_started', 'warmup_complete'} and not warmup_seconds:
        raise DistributedMeasurementError(
            'The execution journal contains a disabled warm-up phase.'
        )
    if state == 'warmup_started' and journal['warmup_metrics'] is not None:
        raise DistributedMeasurementError(
            'The warm-up journal records metrics before completion.'
        )
    if state in {'warmup_complete', 'measurement_started', 'measurement_complete'}:
        if warmup_seconds:
            if not isinstance(journal['warmup_metrics'], Mapping):
                raise DistributedMeasurementError(
                    'The completed warm-up metrics are invalid.'
                )
        elif journal['warmup_metrics'] is not None:
            raise DistributedMeasurementError(
                'The journal records metrics for a disabled warm-up.'
            )
    _validate_measurement_attestation(
        journal['measurement_attestation'],
        complete=state == 'measurement_complete',
        rendered_manifest_sha256=identity['rendered_manifest_sha256'],
    )


def _write_journal(
    job: MutableMapping[str, Any],
    identity: Mapping[str, Any],
    *,
    state: str,
    load_generator_attestation: Mapping[str, Any] | None,
    frontend_attestation: Mapping[str, Any] | None,
    dataset_attestation: Mapping[str, Any] | None,
    warmup_metrics: Mapping[str, Any] | None,
    measurement_attestation: Mapping[str, Any] | None,
    persist: Callable[[MutableMapping[str, Any]], Any] | None,
) -> dict[str, Any]:
    journal = {
        **dict(identity),
        'state': state,
        'load_generator_attestation': (
            dict(load_generator_attestation)
            if load_generator_attestation is not None
            else None
        ),
        'frontend_attestation': (
            dict(frontend_attestation)
            if frontend_attestation is not None
            else None
        ),
        'dataset_attestation': (
            _json_value(dataset_attestation)
            if dataset_attestation is not None
            else None
        ),
        'warmup_metrics': (
            _json_value(warmup_metrics) if warmup_metrics is not None else None
        ),
        'measurement_attestation': (
            _json_value(measurement_attestation)
            if measurement_attestation is not None
            else None
        ),
    }
    _validate_execution_journal(journal, identity=identity)
    resources = job.get('resources')
    if not isinstance(resources, MutableMapping):
        raise DistributedMeasurementError(
            'The benchmark job has no mutable resource state.'
        )
    resources[EXECUTION_JOURNAL_KEY] = journal
    if persist is not None:
        persist(job)
    return journal


def _qualification_checkpoint(
    checkpoint: Callable[[MutableMapping[str, Any], str], Any] | None,
    job: MutableMapping[str, Any],
    state: str,
) -> None:
    """Notify an operator-only harness after a state is durably persisted."""

    if checkpoint is not None:
        checkpoint(job, state)


def _execute(
    execute: Callable[..., str],
    job: MutableMapping[str, Any],
    host: RuntimeHost,
    command: str,
    *,
    timeout: int,
    transport_attempts: int | None = None,
    include_stderr: bool = False,
) -> str:
    kwargs: dict[str, Any] = {
        'timeout': timeout,
        'host_key': host.host_key,
    }
    if include_stderr:
        kwargs['include_stderr'] = True
    if host.jump_host_key is not None:
        kwargs['jump_host_key'] = host.jump_host_key
    if transport_attempts is not None:
        kwargs['transport_attempts'] = transport_attempts
    output = execute(job, command, **kwargs)
    if isinstance(output, bytes):
        try:
            return output.decode('utf-8')
        except UnicodeDecodeError as exc:
            raise DistributedMeasurementError(
                'A remote command returned non-UTF-8 output.'
            ) from exc
    if not isinstance(output, str):
        raise DistributedMeasurementError(
            'A remote command returned a non-text response.'
        )
    return output


def _emit(
    emit: Callable[[MutableMapping[str, Any], str, str], Any] | None,
    job: MutableMapping[str, Any],
    message: str,
) -> None:
    if emit is not None:
        emit(job, 'Distributed DeathStarBench', message)


def _timestamp(
    timestamp: Callable[[], str] | None,
) -> str:
    value = (
        datetime.now(timezone.utc).isoformat()
        if timestamp is None
        else timestamp()
    )
    if not isinstance(value, str) or not value or value != value.strip():
        raise DistributedMeasurementError('The result timestamp is invalid.')
    return value


def _execution_snapshot(
    execute: Callable[..., str],
    job: MutableMapping[str, Any],
    control: RuntimeHost,
    bundle: Any,
) -> dict[str, Any]:
    output = _execute(
        execute,
        job,
        control,
        workload_readiness_command(bundle),
        timeout=2_100,
    )
    try:
        return _json_value(parse_workload_execution_attestation(output, bundle))
    except WorkloadBundleError as exc:
        raise DistributedMeasurementError(
            f'The live workload execution identity is invalid: {exc}'
        ) from exc


def _run_durable_initializer(
    execute: Callable[..., str],
    job: MutableMapping[str, Any],
    load_generator: RuntimeHost,
    target_private_ip: str,
    expected_execution: Mapping[str, str],
    *,
    emit: Callable[[MutableMapping[str, Any], str, str], Any] | None,
    clock: Callable[[], float],
    sleep: Callable[[float], Any],
) -> tuple[str, dict[str, str]]:
    """Dispatch once, then reconcile exclusively through read-only polling."""

    started = clock()
    if not isinstance(started, (int, float)):
        raise DistributedMeasurementError(
            'The initializer polling clock returned an invalid value.'
        )
    deadline = float(started) + INITIALIZER_POLL_TIMEOUT_SECONDS
    dispatch_confirmed = False
    try:
        dispatch_output = _execute(
            execute,
            job,
            load_generator,
            durable_initializer_dispatch_command(job, target_private_ip),
            timeout=180,
            transport_attempts=1,
        )
    except Exception:
        # A lost response cannot reveal whether systemd accepted the unit.  Do
        # not submit it again: the deterministic unit is reconciled below.
        _emit(
            emit,
            job,
            'Initializer dispatch was ambiguous; reconciling its durable '
            'systemd unit without replaying the payload.',
        )
    else:
        # A returned response is no longer ambiguous.  Its exact unit, user,
        # and installed-payload identity must match; malformed evidence must
        # fail closed rather than being reclassified as a transport loss.
        parse_initializer_dispatch_attestation(
            dispatch_output,
            expected_execution,
        )
        dispatch_confirmed = True

    saw_transport_error = False
    while True:
        try:
            status_output = _execute(
                execute,
                job,
                load_generator,
                durable_initializer_status_command(job),
                timeout=120,
                transport_attempts=1,
            )
            observation = parse_durable_initializer_status(
                status_output,
                expected_execution,
            )
            saw_transport_error = False
        except DistributedMeasurementError:
            raise
        except Exception as exc:
            observation = None
            if not saw_transport_error:
                _emit(
                    emit,
                    job,
                    'Initializer status transport was interrupted; '
                    'reconnecting to the same durable unit.',
                )
            saw_transport_error = True
            last_error = exc

        now = clock()
        if not isinstance(now, (int, float)) or float(now) < float(started):
            raise DistributedMeasurementError(
                'The initializer polling clock returned an invalid value.'
            )
        elapsed = float(now) - float(started)

        if observation is not None:
            state = observation['state']
            if state == 'succeeded':
                execution = {
                    **dict(expected_execution),
                    'invocation_id': observation['invocation_id'],
                    'start_monotonic_usec': observation[
                        'start_monotonic_usec'
                    ],
                    'exit_monotonic_usec': observation[
                        'exit_monotonic_usec'
                    ],
                    'initializer_log_bytes': observation['output_bytes'],
                    # Avoid an ``output``-bearing metadata key: comparison
                    # artifacts reject arbitrary raw-output fields, while the
                    # digest of the exact initializer transcript is safe.
                    'initializer_log_sha256': observation['output_sha256'],
                }
                return observation['output'], execution
            if state == 'failed':
                raise DistributedMeasurementError(
                    'The durable Social Network initializer failed '
                    f'(result={observation["result"]}, '
                    f'exit_status={observation["exec_status"]}).'
                )
            if state == 'not_found' and (
                dispatch_confirmed
                or elapsed >= INITIALIZER_DISPATCH_GRACE_SECONDS
            ):
                raise DistributedMeasurementError(
                    'The initializer dispatch did not produce its deterministic '
                    'systemd unit; the payload was not resubmitted.'
                )

        if float(now) >= deadline:
            message = (
                'Timed out while reconciling the durable Social Network '
                'initializer; the payload was not resubmitted.'
            )
            if observation is None:
                raise DistributedMeasurementError(message) from last_error
            raise DistributedMeasurementError(message)
        sleep(min(
            float(INITIALIZER_POLL_INTERVAL_SECONDS),
            deadline - float(now),
        ))


def run_azure_distributed_social_network_measurement(
    job: MutableMapping[str, Any],
    image_lock: Mapping[str, Any],
    options: DeathStarBenchOptions | Mapping[str, Any],
    *,
    execute: Callable[..., str],
    emit: Callable[[MutableMapping[str, Any], str, str], Any] | None = None,
    persist: Callable[[MutableMapping[str, Any]], Any] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    timestamp: Callable[[], str] | None = None,
    initializer_clock: Callable[[], float] = time.monotonic,
    initializer_sleep: Callable[[float], Any] = time.sleep,
    qualification_checkpoint: (
        Callable[[MutableMapping[str, Any], str], Any] | None
    ) = None,
) -> dict[str, Any]:
    """Initialize and measure the exact Azure distributed candidate once."""

    if not isinstance(job, MutableMapping):
        raise DistributedMeasurementError('A mutable benchmark job is required.')
    if (
        qualification_checkpoint is not None
        and not callable(qualification_checkpoint)
    ):
        raise DistributedMeasurementError(
            'The qualification checkpoint must be callable.'
        )
    if qualification_checkpoint is not None and not callable(persist):
        raise DistributedMeasurementError(
            'A qualification checkpoint requires a callable persistence '
            'hook so its journal boundary is durable.'
        )
    resources = job.get('resources')
    if not isinstance(resources, MutableMapping):
        raise DistributedMeasurementError(
            'The benchmark job has no mutable resource state.'
        )
    results = job.get('results')
    if not isinstance(results, list):
        raise DistributedMeasurementError('The benchmark result list is invalid.')
    if any(
        result.get('id') == 'deathstarbench'
        for result in results
        if isinstance(result, Mapping)
    ):
        raise DistributedMeasurementError(
            'This job already contains a DeathStarBench result.'
        )
    validated_options = _validated_options(options)
    try:
        plan = azure_k3s_candidate_plan(resources)
        runtime_journal = resources.get(RUNTIME_JOURNAL_KEY)
        _validate_existing_journal(runtime_journal, plan)
        if runtime_journal is None or runtime_journal['state'] != 'cluster_ready':
            raise DistributedMeasurementError(
                'The exact K3s runtime must be cluster_ready before measurement.'
            )
        bundle = render_social_network_bundle(
            image_lock,
            plan.host('application').architecture,
            plan.load_generator.private_ip,
        )
        filesystem_uuid = runtime_journal['database_volume']['filesystem_uuid']
        workload_journal = resources.get(WORKLOAD_JOURNAL_KEY)
        _validate_existing_workload_journal(
            workload_journal,
            plan=plan,
            bundle=bundle,
            filesystem_uuid=filesystem_uuid,
        )
        if workload_journal is None or workload_journal['state'] != 'workload_ready':
            raise DistributedMeasurementError(
                'The exact Social Network workload must be workload_ready '
                'before measurement.'
            )
    except DistributedMeasurementError:
        raise
    except (DistributedRuntimeError, WorkloadBundleError, KeyError, TypeError) as exc:
        raise DistributedMeasurementError(
            f'The distributed measurement prerequisite is invalid: {exc}'
        ) from exc

    load_generator = plan.load_generator
    if normalized_architecture(load_generator.architecture) != 'x86_64':
        raise DistributedMeasurementError(
            'The distributed load generator must attest x86_64 architecture.'
        )
    identity = _journal_identity(plan, bundle, validated_options)
    existing = resources.get(EXECUTION_JOURNAL_KEY)
    if existing is not None:
        _validate_execution_journal(existing, identity=identity)
        if existing['state'] in _ONE_SHOT_STATES:
            raise DistributedMeasurementError(
                'Distributed DeathStarBench initialization already began; '
                'the dataset and traffic phase is one-shot and cannot be retried.'
            )
    else:
        _write_journal(
            job,
            identity,
            state='preparing_load_generator',
            load_generator_attestation=None,
            frontend_attestation=None,
            dataset_attestation=None,
            warmup_metrics=None,
            measurement_attestation=None,
            persist=persist,
        )

    _emit(emit, job, 'Preparing the pinned x86_64 wrk2 load generator.')
    _execute(
        execute,
        job,
        load_generator,
        web_guest.readiness_command(
            'azure',
            'deathstarbench',
            'loadgen',
            expected_architecture='x86_64',
        ),
        timeout=900,
    )
    for step in web_guest.deathstarbench_install_steps('azure', 'loadgen'):
        _execute(
            execute,
            job,
            load_generator,
            step.command,
            timeout=step.timeout_seconds,
        )
    loadgen_output = _execute(
        execute,
        job,
        load_generator,
        load_generator_attestation_command(),
        timeout=1_800,
    )
    loadgen_attestation = parse_load_generator_attestation(loadgen_output)
    _write_journal(
        job,
        identity,
        state='load_generator_ready',
        load_generator_attestation=loadgen_attestation,
        frontend_attestation=None,
        dataset_attestation=None,
        warmup_metrics=None,
        measurement_attestation=None,
        persist=persist,
    )
    _qualification_checkpoint(
        qualification_checkpoint,
        job,
        'load_generator_ready',
    )

    application = plan.host('application')
    control = plan.host('control')
    target_ip = application.private_ip
    _emit(emit, job, 'Probing the Social Network frontend with a GET-only request.')
    probe_output = _execute(
        execute,
        job,
        load_generator,
        non_mutating_frontend_probe_command(target_ip),
        timeout=720,
    )
    frontend_attestation = parse_frontend_probe_attestation(
        probe_output,
        target_ip,
    )
    empty_database_output = _execute(
        execute,
        job,
        control,
        dataset_database_attestation_command(),
        timeout=300,
    )
    empty_database = parse_dataset_database_attestation(
        empty_database_output,
        initialized=False,
    )
    initializer_execution = initializer_execution_identity(job, target_ip)
    dataset_started = {
        'dataset_revision': DISTRIBUTED_DATASET_REVISION,
        'graph': DATASET_GRAPH,
        'pre_initialization_database': empty_database,
        'initializer_execution': initializer_execution,
    }
    # This persisted state is the point of no return.  The initializer may
    # have mutated the service even if the SSH transport reports no response.
    _write_journal(
        job,
        identity,
        state='initialization_started',
        load_generator_attestation=loadgen_attestation,
        frontend_attestation=frontend_attestation,
        dataset_attestation=dataset_started,
        warmup_metrics=None,
        measurement_attestation=None,
        persist=persist,
    )
    _qualification_checkpoint(
        qualification_checkpoint,
        job,
        'initialization_started',
    )

    _emit(emit, job, 'Initializing the pinned Reed98 Social Network dataset.')
    initialization_output, completed_initializer_execution = (
        _run_durable_initializer(
            execute,
            job,
            load_generator,
            target_ip,
            initializer_execution,
            emit=emit,
            clock=initializer_clock,
            sleep=initializer_sleep,
        )
    )
    initializer_attestation = parse_dataset_attestation(initialization_output)
    database_output = _execute(
        execute,
        job,
        control,
        dataset_database_attestation_command(),
        timeout=300,
    )
    database_attestation = parse_dataset_database_attestation(
        database_output,
        initialized=True,
    )
    dataset_ready = {
        **dataset_started,
        'initializer_execution': completed_initializer_execution,
        'initializer': initializer_attestation,
        'database': database_attestation,
    }
    _write_journal(
        job,
        identity,
        state='dataset_ready',
        load_generator_attestation=loadgen_attestation,
        frontend_attestation=frontend_attestation,
        dataset_attestation=dataset_ready,
        warmup_metrics=None,
        measurement_attestation=None,
        persist=persist,
    )

    _emit(emit, job, 'Capturing the pre-measurement pod execution identity.')
    pre_execution = _execution_snapshot(
        execute,
        job,
        control,
        bundle,
    )
    in_progress_attestation = {'pre_workload_execution': pre_execution}
    warmup_metrics = None
    if validated_options.warmup_seconds:
        _write_journal(
            job,
            identity,
            state='warmup_started',
            load_generator_attestation=loadgen_attestation,
            frontend_attestation=frontend_attestation,
            dataset_attestation=dataset_ready,
            warmup_metrics=None,
            measurement_attestation=in_progress_attestation,
            persist=persist,
        )
        _qualification_checkpoint(
            qualification_checkpoint,
            job,
            'warmup_started',
        )
        _emit(emit, job, 'Running the excluded distributed warm-up interval.')
        warmup_output = _execute(
            execute,
            job,
            load_generator,
            _load_command(
                target_ip,
                validated_options,
                validated_options.warmup_seconds,
            ),
            timeout=validated_options.warmup_seconds + 300,
            transport_attempts=1,
            include_stderr=True,
        )
        warmup_metrics = parse_wrk2_execution_output(
            warmup_output,
            label='Distributed warm-up',
        )
        _write_journal(
            job,
            identity,
            state='warmup_complete',
            load_generator_attestation=loadgen_attestation,
            frontend_attestation=frontend_attestation,
            dataset_attestation=dataset_ready,
            warmup_metrics=warmup_metrics,
            measurement_attestation=in_progress_attestation,
            persist=persist,
        )

    measured_command = _load_command(
        target_ip,
        validated_options,
        validated_options.duration_seconds,
    )
    _write_journal(
        job,
        identity,
        state='measurement_started',
        load_generator_attestation=loadgen_attestation,
        frontend_attestation=frontend_attestation,
        dataset_attestation=dataset_ready,
        warmup_metrics=warmup_metrics,
        measurement_attestation=in_progress_attestation,
        persist=persist,
    )
    _qualification_checkpoint(
        qualification_checkpoint,
        job,
        'measurement_started',
    )
    _emit(emit, job, 'Running the one-shot distributed measurement interval.')
    started_at = _timestamp(timestamp)
    started = monotonic()
    measured_output = _execute(
        execute,
        job,
        load_generator,
        measured_command,
        timeout=validated_options.duration_seconds + 300,
        transport_attempts=1,
        include_stderr=True,
    )
    elapsed = monotonic() - started
    if not isinstance(elapsed, (int, float)) or elapsed < 0:
        raise DistributedMeasurementError(
            'The measurement clock returned an invalid duration.'
        )
    metrics = parse_wrk2_execution_output(
        measured_output,
        label='Distributed measurement',
    )

    _emit(emit, job, 'Re-attesting pod identity after measurement.')
    post_execution = _execution_snapshot(
        execute,
        job,
        control,
        bundle,
    )
    if post_execution != pre_execution:
        raise DistributedMeasurementError(
            'A workload pod was replaced, restarted, moved, or changed image '
            'during warm-up or measurement.'
        )

    metrics_sha256 = _sha256_json(metrics)
    output_sha256 = _sha256_text(measured_output)
    safe_frontend_attestation = {
        'target_role': 'application',
        'target_address_sha256': _sha256_text(
            frontend_attestation['target_private_ip']
        ),
        'port': frontend_attestation['port'],
        'route': frontend_attestation['route'],
    }
    safe_measurement_evidence = _json_value({
        'schema_version': 1,
        'load_generator_attestation': loadgen_attestation,
        # Preserve the validated endpoint without publishing its private IP in
        # results.json.  The role and address digest bind this evidence to the
        # exact application target recorded in the cleanup-owned journal.
        'frontend_attestation': safe_frontend_attestation,
        'dataset_attestation': dataset_ready,
        'warmup_metrics': warmup_metrics,
        'pre_workload_execution': pre_execution,
        'post_workload_execution': post_execution,
        'pod_identity_unchanged': True,
        'result_id': 'deathstarbench',
        'metrics_sha256': metrics_sha256,
        # Keep the digest while avoiding an ``output``-bearing metadata key:
        # the comparison artifact deliberately rejects arbitrary output fields.
        'raw_benchmark_sha256': output_sha256,
    })

    result_metadata = deathstarbench.metadata(
        'social_network',
        validated_options,
        service_architecture=application.architecture,
        loadgen_architecture=load_generator.architecture,
    )
    result_metadata.update({
        'dataset_revision': DISTRIBUTED_DATASET_REVISION,
        'load_driver_revision': DISTRIBUTED_LOAD_DRIVER_REVISION,
        'measurement_revision': DISTRIBUTED_MEASUREMENT_REVISION,
        'workload_revision': DISTRIBUTED_WORKLOAD_REVISION,
        'image_set_revision': DISTRIBUTED_IMAGE_SET_REVISION,
        'manifest_source_sha256': bundle.manifest_source_sha256,
        'rendered_manifest_sha256': bundle.rendered_manifest_sha256,
        'image_lock_fingerprint': bundle.image_lock_fingerprint,
        'topology_fingerprint': plan.topology_fingerprint,
        'target_private_ip': target_ip,
        'load_generator_private_ip': load_generator.private_ip,
        'load_generator_node_name': load_generator.node_name,
        'dataset_graph': DATASET_GRAPH,
        'dataset_users': DATASET_USER_COUNT,
        'dataset_follows': DATASET_FOLLOW_COUNT,
        'dataset_posts': DATASET_POST_COUNT,
        'dataset_initializer_source_sha256': INITIALIZER_SOURCE_SHA256,
        'dataset_initializer_patched_sha256': PATCHED_INITIALIZER_SHA256,
        'initializer_sha256': PATCHED_INITIALIZER_SHA256,
        'dataset_nodes_sha256': REED98_NODES_SHA256,
        'dataset_edges_sha256': REED98_EDGES_SHA256,
        'load_driver_lua_sha256': MIXED_WORKLOAD_LUA_SHA256,
        'load_script_sha256': MIXED_WORKLOAD_LUA_SHA256,
        'load_generator_wrk_binary_sha256': loadgen_attestation[
            'wrk_binary_sha256'
        ],
        'load_generator_compiler_version_sha256': loadgen_attestation[
            'compiler_version_sha256'
        ],
        'workload_execution_attestation_sha256': _sha256_json(pre_execution),
        # The execution journal is an infrastructure-ownership record and is
        # removed after Azure cleanup.  Preserve its safe benchmark evidence
        # with the result so results.json and report.html retain the completed
        # dataset, warm-up, and pre/post execution attestations.
        'measurement_evidence': safe_measurement_evidence,
        'measurement_evidence_sha256': _sha256_json(
            safe_measurement_evidence
        ),
    })
    result = {
        'id': 'deathstarbench',
        'name': 'DeathStarBench — Social Network',
        'command': measured_command,
        'started_at': started_at,
        'duration_seconds': round(float(elapsed), 2),
        'status': 'completed',
        'output': measured_output[-MAX_RESULT_OUTPUT_CHARS:],
        'metadata': result_metadata,
        'metrics': metrics,
    }
    measurement_attestation = {
        'pre_workload_execution': pre_execution,
        'post_workload_execution': post_execution,
        'result_id': result['id'],
        'metrics_sha256': metrics_sha256,
        'output_sha256': output_sha256,
    }
    # Append before the final journal write so one persistence operation binds
    # the result and its measurement-complete attestation atomically.
    results.append(result)
    _write_journal(
        job,
        identity,
        state='measurement_complete',
        load_generator_attestation=loadgen_attestation,
        frontend_attestation=frontend_attestation,
        dataset_attestation=dataset_ready,
        warmup_metrics=warmup_metrics,
        measurement_attestation=measurement_attestation,
        persist=persist,
    )
    _emit(emit, job, 'Distributed Social Network measurement completed.')
    return result
