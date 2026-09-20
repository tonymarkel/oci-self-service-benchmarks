import asyncio
import json
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import uuid
import base64
import html
import hmac
import math
import re
import shlex
from ipaddress import ip_address
from pathlib import Path
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

import oci
from cryptography.hazmat.primitives import serialization
from dotenv import dotenv_values
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .apachebench import (
    WORKLOADS as APACHEBENCH_WORKLOADS,
    aggregate_trial_metrics as aggregate_apachebench_trials,
    benchmark_command as apachebench_command,
    loadgen_prepare_command as apachebench_loadgen_prepare_command,
    metadata as apachebench_metadata,
    parse_output as parse_apachebench_output,
    readiness_command as apachebench_readiness_command,
    record_network_capacity as record_apachebench_network_capacity,
    target_prepare_command as apachebench_target_prepare_command,
    validate_metrics as validate_apachebench_metrics,
    warmup_command as apachebench_warmup_command,
    workload as apachebench_workload,
)
from .catalog import (
    BENCHMARKS,
    DEATHSTARBENCH_WORKLOADS,
    IPERF3_PROTOCOLS,
    LLM_BENCHMARKS,
    PHORONIX_PROFILES,
    SYSBENCH_WORKLOADS,
)
from .comparison import (
    build_comparison_payload,
    build_results_artifact,
    extract_chart_metrics,
    load_results_document,
    write_results_artifact,
)
from .deathstarbench import (
    build_workload_command,
    deploy_workload_command,
    frontend_firewall_command,
    frontend_readiness_command,
    initialize_workload_command,
    load_command as deathstarbench_load_command,
    load_generator_prepare_command,
    metadata as deathstarbench_metadata,
    parse_wrk2_output,
    podman_runtime_verification_command,
    prefetch_workload_images_command,
    prepare_workload_command,
    workload as deathstarbench_workload,
)
from .deathstarbench_contract import (
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    K3S_RUNTIME_ID,
    PODMAN_COMPOSE_RUNTIME_ID,
    SINGLE_HOST_TOPOLOGY_ID,
    require_released_runtime,
)
from .deathstarbench_distributed import (
    prepare_azure_distributed_k3s_candidate,
    prepare_azure_distributed_social_network_candidate,
)
from .deathstarbench_distributed_measurement import (
    run_azure_distributed_social_network_measurement,
)
from .models import (
    BenchmarkPlan,
    ClearSavedRunsRequest,
    canonicalize_benchmark_plan,
)
from .guests import amazon_linux, rocky_linux, web as web_guest
from . import llama_cpp, storage_target
from .iperf3 import parse_output as parse_iperf3_output
from .providers import aws as aws_provider
from .providers import azure as azure_provider
from .providers import gcp as gcp_provider
from .providers.registry import (
    dispatch_provider_operation,
    provider_adapter,
    providers as registered_providers,
)
from .resource_inventory import (
    ROLE_NODE_INVENTORY_KEY,
    ResourceInventoryError,
    load_role_node_inventory,
)
from .run_lease import (
    RunLeaseError,
    RunLeaseHeldError,
    acquire_run_lease,
    inspect_run_lease,
)
from .phoronix import (
    PREPARE_TIMEOUT_SECONDS as PHORONIX_PREPARE_TIMEOUT_SECONDS,
    parse_result_output as parse_phoronix_result,
    prepare_command as phoronix_prepare_command,
    profile_runs as phoronix_profile_runs,
    required_packages as phoronix_required_packages,
)

ROOT = Path(__file__).parent
PROJECT_ROOT = ROOT.parent
RUNS = ROOT / 'runs'
RUNS.mkdir(exist_ok=True)
app = FastAPI(title='Cloud Self-Service Benchmarks')
app.mount('/static', StaticFiles(directory=ROOT / 'static'), name='static')
jobs: dict[str, dict[str, Any]] = {}
job_tasks: dict[str, asyncio.Task] = {}
job_cancel_events: dict[str, threading.Event] = {}
job_processes: dict[str, set[subprocess.Popen]] = {}
job_processes_lock = threading.Lock()
LLAMA_TOOLSET_PACKAGE = 'gcc-toolset-12'
LLAMA_TOOLSET_ENABLE = '/opt/rh/gcc-toolset-12/enable'
LOAD_GENERATOR_SHAPES = (
    'VM.Standard.E5.Flex',
    'VM.Standard.E4.Flex',
    'VM.Standard.E3.Flex',
    'VM.Standard3.Flex',
)
SYSBENCH_RESULT_IDS = {
    'cpu': 'sysbench_cpu',
    'memory': 'sysbench_memory',
    'fileio': 'sysbench_fileio',
}
STORAGE_BENCHMARK_RESULT_IDS = frozenset({'fio', 'sysbench_fileio'})
IPERF3_RESULT_IDS = {
    'tcp': 'iperf_tcp',
    'udp': 'iperf_udp',
    'sctp': 'iperf_sctp',
}
IPERF_PEER_READINESS_TIMEOUT_SECONDS = 720
RUN_ID_PATTERN = re.compile(r'^[0-9a-f]{12}$')
ACTIVE_RUN_STATUSES = frozenset({
    'queued',
    'provisioning',
    'testing',
    'reporting',
    'cancelling',
    'cleanup_pending',
    'destroying',
})
PRESERVED_RUN_STATUSES = frozenset({
    'complete',
    'cleanup_failed',
    'interrupted',
})
DELETABLE_RUN_STATUSES = frozenset({'destroyed', 'reported', 'failed'})
SAVED_RUN_ARTIFACTS = ('report.html', 'state.json', 'results.json')


class SSHCommandError(RuntimeError):
    def __init__(self, message, output='', returncode=None):
        super().__init__(message)
        self.output = output
        self.returncode = returncode


class RunCancelled(BaseException):
    """Cooperative live-run cancellation that bypasses workload wrappers."""


def cancellation_event(job):
    event_value = job.get('_cancel_event')
    if isinstance(event_value, threading.Event):
        return event_value
    return job_cancel_events.get(str(job.get('id', '')))


def cancellation_requested(job):
    event_value = cancellation_event(job)
    return bool(event_value and event_value.is_set())


def raise_if_cancelled(job):
    if cancellation_requested(job):
        raise RunCancelled('The benchmark run was stopped by the user.')


def register_job_process(job, process):
    job_id = str(job['id'])
    with job_processes_lock:
        job_processes.setdefault(job_id, set()).add(process)


def unregister_job_process(job, process):
    job_id = str(job['id'])
    with job_processes_lock:
        processes = job_processes.get(job_id)
        if not processes:
            return
        processes.discard(process)
        if not processes:
            job_processes.pop(job_id, None)


def signal_process_termination(process):
    if process.poll() is not None:
        return
    try:
        if os.name == 'posix':
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
    except (ProcessLookupError, OSError):
        # The process may have exited between poll() and the signal.
        return


def signal_job_processes(job_id):
    with job_processes_lock:
        processes = tuple(job_processes.get(str(job_id), ()))
    for process in processes:
        signal_process_termination(process)


def retain_job_task(job_id, task, *, cancel_event=None):
    job_id = str(job_id)
    job_tasks[job_id] = task
    if cancel_event is not None:
        job_cancel_events[job_id] = cancel_event

    def release(completed_task):
        if job_tasks.get(job_id) is completed_task:
            job_tasks.pop(job_id, None)
            job_cancel_events.pop(job_id, None)
        try:
            completed_task.exception()
        except (asyncio.CancelledError, RunCancelled):
            pass

    task.add_done_callback(release)
    return task


def selected_sysbench_workloads(plan):
    selected = set(plan.benchmarks)
    workloads = []
    if 'sysbench' in selected:
        workloads.extend(plan.sysbench.workloads)
    for workload_id, result_id in SYSBENCH_RESULT_IDS.items():
        if result_id in selected:
            workloads.append(workload_id)
    return tuple(dict.fromkeys(workloads))


def expanded_benchmark_ids(plan):
    selected = set(plan.benchmarks)
    selected.update(
        SYSBENCH_RESULT_IDS[workload]
        for workload in selected_sysbench_workloads(plan)
    )
    selected.update(expanded_iperf3_result_ids(plan))
    return selected


def plan_uses_sysbench(plan):
    return bool(selected_sysbench_workloads(plan))


def selected_iperf3_protocols(plan):
    selected = set(plan.benchmarks)
    protocols = []
    if 'iperf3' in selected:
        protocols.extend(plan.iperf3.protocols)
    for protocol, result_id in IPERF3_RESULT_IDS.items():
        if result_id in selected:
            protocols.append(protocol)
    return tuple(dict.fromkeys(protocols))


def expanded_iperf3_result_ids(plan):
    return tuple(
        IPERF3_RESULT_IDS[protocol]
        for protocol in selected_iperf3_protocols(plan)
    )


def plan_uses_iperf3(plan):
    return bool(selected_iperf3_protocols(plan))


def plan_uses_load_generator(plan):
    return bool({'deathstarbench', 'apachebench'} & set(plan.benchmarks))


def expected_benchmark_result_ids(plan):
    """Return the result rows required to complete a saved benchmark plan."""
    if hasattr(plan, 'model_dump'):
        plan = plan.model_dump()
    if not isinstance(plan, dict):
        return set()

    canonical = canonicalize_benchmark_plan(dict(plan))
    expected = set()
    for benchmark_id in canonical.get('benchmarks') or []:
        if benchmark_id == 'sysbench':
            options = canonical.get('sysbench') or {}
            workloads = options.get('workloads') or ['cpu']
            expected.update(
                SYSBENCH_RESULT_IDS.get(workload, f'sysbench_{workload}')
                for workload in workloads
            )
        elif benchmark_id == 'iperf3':
            options = canonical.get('iperf3') or {}
            protocols = options.get('protocols') or ['tcp']
            expected.update(
                IPERF3_RESULT_IDS.get(protocol, f'iperf_{protocol}')
                for protocol in protocols
            )
        elif benchmark_id == 'phoronix':
            options = canonical.get('phoronix') or {}
            profile_ids = options.get('profiles') or ['compress_7zip']
            try:
                expected.update(
                    run.benchmark_id
                    for run in phoronix_profile_runs(profile_ids, 'status')
                )
            except ValueError:
                # Preserve unknown selections as impossible-to-satisfy rows so
                # a stale or corrupt plan cannot be reported as complete.
                expected.update(
                    f'phoronix_unknown_{profile_id}'
                    for profile_id in profile_ids
                )
        elif benchmark_id == 'apachebench':
            options = canonical.get('apachebench') or {}
            workloads = options.get('workloads') or [
                'new_connections',
                'keep_alive',
            ]
            expected.update(
                f'apachebench_{workload}' for workload in workloads
            )
        else:
            expected.add(benchmark_id)

    expected.update(canonical.get('llm_benchmarks') or [])
    return expected


def benchmark_result_name(result_id, plan):
    """Return a stable friendly name for an expected result row."""
    if hasattr(plan, 'model_dump'):
        plan = plan.model_dump()
    canonical = canonicalize_benchmark_plan(
        dict(plan) if isinstance(plan, dict) else {}
    )
    fixed_names = {
        'sysbench_cpu': 'Sysbench — CPU',
        'sysbench_memory': 'Sysbench — Memory',
        'sysbench_fileio': 'Sysbench — File I/O',
        'stream': 'STREAM',
        'fio': 'fio storage suite',
        'iperf_tcp': 'iperf3 — TCP',
        'iperf_udp': 'iperf3 — UDP',
        'iperf_sctp': 'iperf3 — SCTP',
        'llama_bench': 'llama.cpp throughput (CPU)',
    }
    if result_id in fixed_names:
        return fixed_names[result_id]
    if result_id.startswith('apachebench_'):
        workload_id = result_id.removeprefix('apachebench_')
        try:
            return f'ApacheBench — {apachebench_workload(workload_id)["name"]}'
        except ValueError:
            pass
    if result_id.startswith('phoronix_'):
        profile_ids = (canonical.get('phoronix') or {}).get('profiles') or []
        try:
            for run in phoronix_profile_runs(profile_ids, 'history'):
                if run.benchmark_id == result_id:
                    return run.name
        except ValueError:
            pass
    if result_id == 'deathstarbench':
        workload_id = (canonical.get('deathstarbench') or {}).get('workload')
        selected = next(
            (
                workload for workload in DEATHSTARBENCH_WORKLOADS
                if workload['id'] == workload_id
            ),
            None,
        )
        if selected:
            return f'DeathStarBench — {selected["name"]}'
    catalog_names = {
        item['id']: item['name']
        for item in [*BENCHMARKS, *LLM_BENCHMARKS]
    }
    return catalog_names.get(result_id, result_id)


def git_clone_command(repository, destination):
    return (
        '{ for attempt in $(seq 1 6); do '
        f'rm -rf {destination}; '
        'if getent ahostsv4 github.com >/dev/null 2>&1 '
        f'&& git clone --depth 1 {repository} {destination}; then '
        'break; fi; '
        'echo "GitHub clone failed; retrying in 10 seconds '
        '($attempt/6)." >&2; '
        'sleep 10; '
        'done; '
        f'test -d {destination}/.git; }}'
    )


def llama_toolset_command(command):
    return f'source {LLAMA_TOOLSET_ENABLE} && {{ {command}; }}'


def llama_benchmark_command():
    clone = git_clone_command(
        'https://github.com/ggml-org/llama.cpp.git',
        '/tmp/llama.cpp',
    )
    return llama_toolset_command(
        'set -eo pipefail; '
        'ARCH="$(uname -m)"; '
        'if [ "$ARCH" = "x86_64" ]; then '
        'GGML_CPU_OPTIONS="-DGGML_NATIVE=OFF '
        '-DGGML_AMX_TILE=OFF -DGGML_AMX_INT8=OFF '
        '-DGGML_AMX_BF16=OFF"; '
        'else GGML_CPU_OPTIONS="-DGGML_NATIVE=ON"; fi; '
        'echo "Using $(gcc --version | head -n 1)"; '
        'echo "CPU architecture: $ARCH"; '
        'echo "llama.cpp CPU options: $GGML_CPU_OPTIONS"; '
        'lscpu | grep -E "^(Model name|Flags|Features):" || true; '
        f'{clone}; '
        'cmake -S /tmp/llama.cpp -B /tmp/llama.cpp/build '
        '-DCMAKE_BUILD_TYPE=Release '
        '-DCMAKE_C_COMPILER="$(command -v gcc)" '
        '-DCMAKE_CXX_COMPILER="$(command -v g++)" '
        '$GGML_CPU_OPTIONS; '
        'cmake --build /tmp/llama.cpp/build -j$(nproc) '
        '--target llama-bench; '
        'curl -L --fail --retry 3 --retry-all-errors '
        'https://huggingface.co/TheBloke/'
        'TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/main/'
        'tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf '
        '-o /tmp/tinyllama.gguf; '
        '/tmp/llama.cpp/build/bin/llama-bench '
        '-m /tmp/tinyllama.gguf -p 512,2048 -n 128,512 -r 5 -o json'
    )


def benchmark_security_rules(
    include_deathstarbench=False,
    iperf3_protocols=(),
    include_apachebench=False,
):
    iperf3_protocols = set(iperf3_protocols)
    rules = [
        oci.core.models.IngressSecurityRule(
            protocol='6',
            source='0.0.0.0/0',
            tcp_options=oci.core.models.TcpOptions(
                destination_port_range=oci.core.models.PortRange(
                    min=22,
                    max=22,
                )
            ),
        ),
    ]
    if iperf3_protocols:
        # iperf3 always negotiates a test through its TCP control connection,
        # including when UDP or SCTP carries the measured traffic.
        rules.append(
            oci.core.models.IngressSecurityRule(
                protocol='6',
                source='10.42.1.0/24',
                tcp_options=oci.core.models.TcpOptions(
                    destination_port_range=oci.core.models.PortRange(
                        min=5201,
                        max=5201,
                    )
                ),
            )
        )
    if 'udp' in iperf3_protocols:
        rules.append(
            oci.core.models.IngressSecurityRule(
                protocol='17',
                source='10.42.1.0/24',
                udp_options=oci.core.models.UdpOptions(
                    destination_port_range=oci.core.models.PortRange(
                        min=5201,
                        max=5201,
                    )
                ),
            )
        )
    if 'sctp' in iperf3_protocols:
        # OCI models SCTP as IP protocol 132. Non-TCP/UDP security rules do
        # not support port options, so limit this rule to the runner subnet.
        rules.append(
            oci.core.models.IngressSecurityRule(
                protocol='132',
                source='10.42.1.0/24',
            )
        )
    if include_deathstarbench:
        for port in (5000, 8080):
            rules.append(
                oci.core.models.IngressSecurityRule(
                    protocol='6',
                    source='10.42.1.0/24',
                    tcp_options=oci.core.models.TcpOptions(
                        destination_port_range=oci.core.models.PortRange(
                            min=port,
                            max=port,
                        )
                    ),
                )
            )
    if include_apachebench:
        rules.append(
            oci.core.models.IngressSecurityRule(
                protocol='6',
                source='10.42.1.0/24',
                tcp_options=oci.core.models.TcpOptions(
                    destination_port_range=oci.core.models.PortRange(
                        min=80,
                        max=80,
                    )
                ),
            )
        )
    return rules


def sctp_kernel_support_command(use_sudo=False):
    elevate = 'sudo ' if use_sudo else ''
    return (
        'set -euo pipefail; '
        'KERNEL_RELEASE="$(uname -r)"; '
        'case "$KERNEL_RELEASE" in '
        '*.el9uek.aarch64.64k) '
        'SCTP_MODULE_PACKAGE="kernel-uek64k-modules-extra-'
        '${KERNEL_RELEASE%.64k}" ;; '
        '*.el9uek.*) SCTP_MODULE_PACKAGE="kernel-uek-modules-extra-'
        '$KERNEL_RELEASE" ;; '
        '*.el9*) SCTP_MODULE_PACKAGE="kernel-modules-extra-'
        '$KERNEL_RELEASE" ;; '
        '*) echo "Unsupported Oracle Linux kernel release: '
        '$KERNEL_RELEASE" >&2; exit 1 ;; '
        'esac; '
        f'if ! {elevate}modprobe sctp >/dev/null 2>&1; then '
        'SCTP_MODULE_INSTALLED=false; '
        'for attempt in 1 2 3; do '
        f'if {elevate}dnf -y --disablerepo=ol9_ksplice '
        '--setopt=retries=10 --setopt=timeout=30 '
        'install "$SCTP_MODULE_PACKAGE"; then '
        'SCTP_MODULE_INSTALLED=true; break; fi; '
        'echo "Unable to install $SCTP_MODULE_PACKAGE; retrying in 10 '
        'seconds ($attempt/3)." >&2; sleep 10; done; '
        'if [ "$SCTP_MODULE_INSTALLED" != true ]; then '
        'echo "The SCTP module package for the running kernel '
        '$KERNEL_RELEASE could not be installed." >&2; exit 1; fi; '
        'fi; '
        'SCTP_MODULE_PATH="$(modinfo -n sctp)"; '
        f'if ! {elevate}modprobe sctp; then '
        'echo "$SCTP_MODULE_PACKAGE is present, but SCTP still cannot be '
        'loaded for $KERNEL_RELEASE." >&2; exit 1; fi; '
        'test -d /proc/net/sctp; '
        'echo "SCTP kernel support is ready for $KERNEL_RELEASE at '
        '$SCTP_MODULE_PATH."'
    )


def iperf_peer_cloud_init(protocols=('tcp',)):
    protocols = set(protocols)
    packages = ['iperf3']
    if 'sctp' in protocols:
        packages.append('lksctp-tools')
    firewall_rules = ['firewall-cmd --permanent --add-port=5201/tcp']
    if 'udp' in protocols:
        firewall_rules.append('firewall-cmd --permanent --add-port=5201/udp')
    if 'sctp' in protocols:
        firewall_rules.append('firewall-cmd --permanent --add-port=5201/sctp')
    package_list = ' '.join(packages)
    firewall_commands = '\n  '.join(firewall_rules)
    sctp_verification = ''
    if 'sctp' in protocols:
        sctp_verification = f'''
{sctp_kernel_support_command()}
IPERF_HELP="$(/usr/bin/iperf3 --help 2>&1)"
grep -q -- '--sctp' <<< "$IPERF_HELP"
'''
    return f'''#!/bin/bash
set -euo pipefail
PACKAGES_READY=false
for attempt in 1 2 3 4 5 6; do
  if dnf -y --disablerepo=ol9_ksplice \
      --setopt=retries=10 --setopt=timeout=30 install {package_list}; then
    PACKAGES_READY=true
    break
  fi
  sleep 10
done
if [ "$PACKAGES_READY" != true ]; then
  echo "Unable to install the iperf3 peer prerequisites: {package_list}." >&2
  exit 1
fi
test -x /usr/bin/iperf3
{sctp_verification.rstrip()}
if command -v firewall-cmd >/dev/null 2>&1 \
    && systemctl is-active --quiet firewalld; then
  {firewall_commands}
  firewall-cmd --reload
fi
cat > /etc/systemd/system/iperf3-server.service <<'EOF'
[Unit]
Description=OCI benchmark iperf3 server
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/bin/iperf3 -s
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now iperf3-server.service
'''

def now(): return datetime.now(timezone.utc).isoformat()
def config(region=None):
    cfg = oci.config.from_file(profile_name='DEFAULT')
    if region: cfg['region'] = region
    return cfg
def clients(region):
    cfg = config(region)
    return cfg, oci.core.ComputeClient(cfg), oci.core.VirtualNetworkClient(cfg), oci.core.BlockstorageClient(cfg), oci.identity.IdentityClient(cfg)
def event(job, stage, message):
    job['events'].append({'at': now(), 'stage': stage, 'message': message})
    job['updated_at'] = now()
    if job.get('_persist_state'):
        persist_job_state(job)
def fail(job, message):
    job['status'] = 'failed'; job['error'] = message; event(job, 'Failed', message)


def benchmark_status(job):
    results = job.get('results', [])
    statuses = [result.get('status') for result in results]
    if 'failed' in statuses:
        return 'failed'
    if not statuses and job.get('error'):
        return 'failed'
    if job.get('benchmark_interrupted'):
        return 'interrupted'
    completed_ids = {
        result.get('id')
        for result in results
        if result.get('status') == 'completed' and result.get('id')
    }
    expected_ids = expected_benchmark_result_ids(job.get('plan'))
    if (
        statuses
        and all(status == 'completed' for status in statuses)
        and (not expected_ids or expected_ids <= completed_ids)
    ):
        return 'complete'
    if job.get('error'):
        return 'failed'
    return 'pending'


def require_complete_benchmark_results(job):
    """Fail before reporting when a runner omitted an expected result row."""
    outcome = benchmark_status(job)
    if outcome == 'complete':
        return
    expected_ids = expected_benchmark_result_ids(job.get('plan'))
    completed_ids = {
        result.get('id')
        for result in job.get('results', [])
        if result.get('status') == 'completed' and result.get('id')
    }
    incomplete_ids = sorted(expected_ids - completed_ids)
    detail = (
        ' Incomplete result IDs: ' + ', '.join(incomplete_ids) + '.'
        if incomplete_ids
        else ''
    )
    raise RuntimeError(
        'Benchmark execution ended without completing every selected '
        f'result.{detail}'
    )


def has_recoverable_resources(job):
    """Return whether a saved run has enough state for provider cleanup."""
    resources = job.get('resources', {})
    if not isinstance(resources, dict):
        return True
    if ROLE_NODE_INVENTORY_KEY in resources:
        try:
            inventory = load_role_node_inventory(
                resources,
                plan=job.get('plan'),
            )
        except ResourceInventoryError:
            # Cleanup must inspect an unknown or damaged manifest rather than
            # advertising the run as disposable. Provider destroy paths will
            # refuse mutation until they understand the persisted schema.
            return True
        pending_statuses = {
            'creating',
            'create_ambiguous',
            'running',
            'stopping',
            'stopped',
            'deleting',
            'delete_ambiguous',
            'failed',
        }
        for node in inventory.nodes:
            node_identity = bool(
                node.provider_resource_id
                or node.provider_resource_name
                or node.public_addresses
                or node.private_addresses
            )
            storage_identity = any(
                item.provider_resource_id
                or item.provider_resource_name
                or item.lifecycle_status in pending_statuses
                for item in node.storage
            )
            if (
                node_identity
                or storage_identity
                or node.lifecycle_status in pending_statuses
            ):
                return True
    identity_only_ids = {
        'image_id',
        'aws_account_id',
        'aws_peer_image_id',
        'azure_subscription_id',
        'azure_tenant_id',
        'azure_peer_image_id',
        'gcp_project_id',
        'gcp_compute_project_id',
        'gcp_peer_image_id',
    }
    if any(
        value and key.endswith('_id') and key not in identity_only_ids
        for key, value in resources.items()
    ):
        return True
    plan = job.get('plan', {})
    provider = (
        plan.get('provider', 'oci')
        if isinstance(plan, dict)
        else getattr(plan, 'provider', 'oci')
    )
    provider = str(provider).lower()
    # Provider ownership metadata is persisted before the first create. If a
    # create succeeds but its response is lost, cleanup can recover the exact
    # run-owned resources even though no resource ID was saved locally.
    if provider == 'aws':
        return bool(resources.get('aws_account_id'))
    if provider == 'gcp':
        return bool(
            resources.get('gcp_project_id')
            and resources.get('gcp_resource_prefix')
        )
    if provider == 'azure':
        return bool(
            resources.get('azure_subscription_id')
            and resources.get('azure_resource_group_name')
            and resources.get('azure_resource_group_tags')
        )
    return False


def has_recoverable_resources_for_any_provider(job):
    """Fail closed when saved provider metadata is missing or inconsistent."""
    if has_recoverable_resources(job):
        return True
    resources = job.get('resources', {})
    if not isinstance(resources, dict):
        return True
    return bool(
        resources.get('aws_account_id')
        or (
            resources.get('gcp_project_id')
            and resources.get('gcp_resource_prefix')
        )
        or (
            resources.get('azure_subscription_id')
            and resources.get('azure_resource_group_name')
            and resources.get('azure_resource_group_tags')
        )
    )


def saved_report_benchmark_status(report_path):
    """Recover benchmark outcome for reports created before state.json existed."""
    report = report_path.read_text(errors='replace')
    statuses = {
        html.unescape(status).strip().lower()
        for status in re.findall(
            r'<p\b[^>]*>\s*Status:\s*<strong\b[^>]*>'
            r'\s*([^<]+?)\s*</strong>',
            report,
            flags=re.IGNORECASE,
        )
    }
    if 'failed' in statuses:
        return 'failed'
    if 'completed' in statuses:
        return 'complete'
    return 'unknown'


def normalize_saved_job_state(saved):
    """Upgrade states written before benchmark and lifecycle outcomes were split."""
    cleanup_warnings = [
        item.get('message')
        for item in saved.get('events', [])
        if item.get('stage') == 'Cleanup warning' and item.get('message')
    ]
    if (
        saved.get('status') == 'failed'
        and saved.get('benchmark_status') == 'complete'
        and cleanup_warnings
    ):
        saved['lifecycle_warning'] = saved.get('error')
        saved['error'] = None
        saved['status'] = 'cleanup_failed'
        saved['cleanup_error'] = (
            saved.get('cleanup_error') or cleanup_warnings[-1]
        )
    return saved


def results_artifact_job(job):
    """Add the computed benchmark outcome to a safe artifact source."""
    return {**job, 'benchmark_status': benchmark_status(job)}


def persist_job_state(job):
    directory = RUNS / job['id']
    directory.mkdir(parents=True, exist_ok=True)
    # Live producers retain the complete parsed result objects in memory, so
    # persist their safe chart artifact alongside the small lifecycle state.
    # A job recovered after an app restart contains only state.json summaries;
    # load_persisted_job marks those jobs read-only for this artifact so a
    # cleanup retry cannot replace rich metrics with summary rows.
    if (
        job.get('_persist_results_artifact', True)
        and job.get('results')
    ):
        write_results_artifact(directory, results_artifact_job(job))
    state = {
        'id': job['id'],
        'status': job['status'],
        'error': job.get('error'),
        'cleanup_error': job.get('cleanup_error'),
        'cancel_requested_at': job.get('cancel_requested_at'),
        'benchmark_interrupted': bool(job.get('benchmark_interrupted')),
        'benchmark_status': benchmark_status(job),
        'created_at': job.get('created_at'),
        'updated_at': job.get('updated_at'),
        'plan': job.get('plan', {}),
        'events': job.get('events', []),
        'resources': job.get('resources', {}),
        'results': [
            {
                key: result.get(key)
                for key in ('id', 'name', 'status', 'error')
                if result.get(key) is not None
            }
            for result in job.get('results', [])
        ],
    }
    state_path = directory / 'state.json'
    temporary_path = directory / f'.state-{uuid.uuid4().hex}.tmp'
    try:
        temporary_path.write_text(json.dumps(state, indent=2))
        os.replace(temporary_path, state_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def record_resource(job, key, value):
    """Record a managed resource before a later provisioning step can fail."""
    job.setdefault('resources', {})[key] = value
    if job.get('_persist_state'):
        persist_job_state(job)


def forget_resource(job, key):
    job.setdefault('resources', {}).pop(key, None)
    if job.get('_persist_state'):
        persist_job_state(job)


def resolve_env_path(value, env_path):
    expanded = Path(os.path.expandvars(value)).expanduser()
    if not expanded.is_absolute():
        expanded = env_path.parent / expanded
    return expanded


def load_ssh_defaults(env_path=None):
    env_path = env_path or PROJECT_ROOT / '.env'
    if not env_path.exists():
        return {'configured': False}
    settings = dotenv_values(env_path)
    private_value = (
        settings.get('BENCHMARK_SSH_PRIVATE_KEY_FILE')
        or settings.get('OCI_BENCHMARK_SSH_PRIVATE_KEY_FILE')
    )
    public_value = (
        settings.get('BENCHMARK_SSH_PUBLIC_KEY_FILE')
        or settings.get('OCI_BENCHMARK_SSH_PUBLIC_KEY_FILE')
    )
    if not private_value and not public_value:
        return {'configured': False}
    if not private_value or not public_value:
        return {
            'configured': False,
            'error': (
                'Set both BENCHMARK_SSH_PRIVATE_KEY_FILE and '
                'BENCHMARK_SSH_PUBLIC_KEY_FILE in .env (the existing '
                'OCI_BENCHMARK_SSH_* aliases are also supported).'
            ),
        }
    paths = {
        'private_key': resolve_env_path(private_value, env_path),
        'public_key': resolve_env_path(public_value, env_path),
    }
    try:
        values = {name: path.read_text() for name, path in paths.items()}
    except (OSError, UnicodeError) as exc:
        return {
            'configured': False,
            'error': f'Unable to read the SSH key files configured in .env: {exc}',
        }
    return {'configured': True, **values}


def is_loopback_request(request):
    if request.client and request.client.host == 'testclient':
        return True
    try:
        return bool(request.client and ip_address(request.client.host).is_loopback)
    except ValueError:
        return False


def parse_host_header(value):
    """Return a normalized ``(host, port)`` pair for a strict Host header."""
    value = str(value or '').strip()
    if not value or any(character.isspace() for character in value) or ',' in value:
        return None
    port = None
    if value.startswith('['):
        closing = value.find(']')
        if closing < 0:
            return None
        host = value[1:closing]
        suffix = value[closing + 1:]
        if suffix:
            if not suffix.startswith(':') or not suffix[1:].isdigit():
                return None
            port = int(suffix[1:])
    else:
        if value.count(':') > 1:
            # RFC-compliant IPv6 literals in Host are enclosed in brackets.
            return None
        if ':' in value:
            host, port_value = value.rsplit(':', 1)
            if not port_value.isdigit():
                return None
            port = int(port_value)
        else:
            host = value
    host = host.casefold().rstrip('.')
    if not host or (port is not None and not 1 <= port <= 65535):
        return None
    return host, port


def request_host_is_local(request):
    host_values = request.headers.getlist('host')
    if len(host_values) != 1:
        return False
    parsed = parse_host_header(host_values[0])
    if not parsed:
        return False
    host, _ = parsed
    if (
        request.client
        and request.client.host == 'testclient'
        and host == 'testserver'
    ):
        return True
    if host == 'localhost':
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


def request_origin_is_same(request):
    """Require browser mutations to originate from this exact local origin."""
    origins = request.headers.getlist('origin')
    if not origins:
        # Starlette's in-process test client is not a browser and does not add
        # Origin automatically. Real unsafe HTTP requests must provide it.
        return bool(request.client and request.client.host == 'testclient')
    if len(origins) != 1:
        return False
    try:
        origin = urlsplit(origins[0])
    except ValueError:
        return False
    if (
        origin.scheme not in {'http', 'https'}
        or origin.username is not None
        or origin.password is not None
        or origin.path not in {'', '/'}
        or origin.query
        or origin.fragment
    ):
        return False
    request_host_values = request.headers.getlist('host')
    if len(request_host_values) != 1:
        return False
    request_host = parse_host_header(request_host_values[0])
    if not request_host or not origin.hostname:
        return False
    try:
        origin_port = origin.port
    except ValueError:
        return False
    origin_host = origin.hostname.casefold().rstrip('.')
    request_hostname, request_port = request_host
    default_port = 443 if request.url.scheme == 'https' else 80
    return (
        origin.scheme == request.url.scheme
        and origin_host == request_hostname
        and (origin_port or default_port) == (request_port or default_port)
    )


@app.middleware('http')
async def require_loopback_for_api(request: Request, call_next):
    """Never expose local cloud credentials or mutations to remote clients."""
    is_api = request.url.path == '/api' or request.url.path.startswith('/api/')
    if is_api:
        if not is_loopback_request(request) or not request_host_is_local(request):
            return JSONResponse(
                {'detail': 'The benchmark API is available only over loopback.'},
                status_code=403,
                headers={'Cache-Control': 'no-store'},
            )
        if request.method.upper() not in {'GET', 'HEAD', 'OPTIONS'}:
            if not request_origin_is_same(request):
                return JSONResponse(
                    {'detail': 'The benchmark API rejected a cross-origin request.'},
                    status_code=403,
                    headers={'Cache-Control': 'no-store'},
                )
    return await call_next(request)


def read_json_file(path, default):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return default


def run_lease_ownership_state(runs_root, job_id):
    """Return ``held``, ``unheld``, or ``unknown`` for a persisted run."""
    if not RUN_ID_PATTERN.fullmatch(str(job_id)):
        # Legacy/unit-test artifacts with noncanonical names cannot be owned
        # by the shared run-lease protocol. Preserve their prior behavior.
        return 'unheld'
    try:
        return (
            'held'
            if inspect_run_lease(runs_root, str(job_id)).held
            else 'unheld'
        )
    except RunLeaseError:
        # A malformed, inaccessible, or otherwise ambiguous lease is never
        # proof that a persisted run is abandoned.
        return 'unknown'


def run_lease_is_definitively_unheld(runs_root, job_id):
    return run_lease_ownership_state(runs_root, job_id) == 'unheld'


def run_summary(directory):
    report_path = directory / 'report.html'
    state_path = directory / 'state.json'
    results_path = directory / 'results.json'
    report_ready = report_path.exists()
    state = read_json_file(state_path, {})
    plan = read_json_file(directory / 'plan.json', {})
    results_document = None
    ownership_state = None
    if results_path.exists():
        try:
            results_document = load_results_document(directory)
        except ValueError:
            # Keep history usable when one artifact is truncated or corrupt;
            # the state/report paths below remain authoritative fallbacks.
            results_document = None
    if not state and results_document:
        state = dict(results_document.get('run') or {})
        state['results'] = results_document.get('results') or []
        state['plan'] = results_document.get('plan') or {}
    if not plan and isinstance(state.get('plan'), dict):
        plan = state['plan']
    if state:
        state = normalize_saved_job_state(state)
        status_job = {**state, 'plan': plan}
        benchmark_outcome = (
            benchmark_status(status_job)
            if expected_benchmark_result_ids(plan)
            else state.get('benchmark_status', 'unknown')
        )
        if (
            benchmark_outcome in (None, 'pending', 'unknown')
            and state.get('error')
        ):
            # Older state files may have persisted ``pending`` before failures
            # without a benchmark result were classified separately from the
            # infrastructure lifecycle.
            benchmark_outcome = 'failed'
        status = state.get('status', 'reported')
        if status in ACTIVE_RUN_STATUSES:
            ownership_state = run_lease_ownership_state(
                directory.parent,
                directory.name,
            )
            if ownership_state == 'unheld':
                status = 'interrupted'
        created_at = state.get('created_at') or next(
            (
                item.get('at')
                for item in state.get('events', [])
                if item.get('at')
            ),
            None,
        )
        recorded_results = state.get('results', [])
        recorded_result_ids = {
            item.get('id') for item in recorded_results if item.get('id')
        }
        result_names = [
            item.get('name') or benchmark_result_name(item.get('id'), plan)
            for item in recorded_results
            if item.get('name') or item.get('id')
        ]
        resources = state.get('resources') or {}
        architecture = (
            resources.get('architecture')
            or resources.get('aws_architecture')
            or resources.get('azure_architecture')
            or resources.get('gcp_architecture')
        )
    else:
        if not report_ready:
            raise ValueError(f'Run {directory.name} has no saved state or report.')
        benchmark_outcome = saved_report_benchmark_status(report_path)
        status = 'failed' if benchmark_outcome == 'failed' else 'reported'
        created_at = None
        recorded_result_ids = set()
        result_names = []
        architecture = None
    if results_document is None:
        try:
            results_document = load_results_document(directory)
        except (FileNotFoundError, ValueError):
            results_document = None
    comparison_result_ids = sorted({
        result.get('id')
        for result in (
            results_document.get('results', []) if results_document else []
        )
        if (
            result.get('id')
            and result.get('status') == 'completed'
            and extract_chart_metrics(result)
        )
    })
    if architecture is None and results_document:
        architecture = next(
            (
                result.get('metadata', {}).get('architecture')
                for result in results_document.get('results', [])
                if isinstance(result.get('metadata'), dict)
                and result['metadata'].get('architecture')
            ),
            None,
        )
    catalog_names = {
        item['id']: item['name']
        for item in [*BENCHMARKS, *LLM_BENCHMARKS]
    }
    catalog_names.update({
        'sysbench_cpu': 'Sysbench — CPU',
        'sysbench_memory': 'Sysbench — Memory',
        'sysbench_fileio': 'Sysbench — File I/O',
        'iperf_tcp': 'iperf3 — TCP',
        'iperf_udp': 'iperf3 — UDP',
        'iperf_sctp': 'iperf3 — SCTP',
        'baseline': 'Cloud baseline suite (legacy)',
    })
    selected_ids = [
        *plan.get('benchmarks', []),
        *plan.get('llm_benchmarks', []),
    ]
    if result_names:
        benchmark_names = list(dict.fromkeys(result_names))
        missing_result_ids = (
            expected_benchmark_result_ids(plan) - recorded_result_ids
        )
        for result_id in sorted(missing_result_ids):
            name = benchmark_result_name(result_id, plan)
            if name not in benchmark_names:
                benchmark_names.append(name)
    else:
        benchmark_names = [
            catalog_names.get(item, item) for item in selected_ids
        ]
    artifacts = [
        path for path in (
            report_path,
            state_path,
            results_path,
        )
        if path.exists()
    ]
    modified_at = datetime.fromtimestamp(
        max(path.stat().st_mtime for path in artifacts),
        tz=timezone.utc,
    ).isoformat()
    # A provider assigns ``destroyed`` only after cleanup has proved that its
    # run-owned resources are absent. OCI intentionally retains the deleted
    # resource IDs in the audit trail, so those stale IDs must not make a
    # successfully destroyed run look recoverable in history.
    recoverable = (
        status != 'destroyed'
        and has_recoverable_resources_for_any_provider({
            **state,
            'plan': plan,
        })
    )
    provider = plan.get('provider', 'oci')
    return {
        'id': directory.name,
        'provider': provider,
        'created_at': created_at or modified_at,
        'modified_at': modified_at,
        'status': status,
        'benchmark_status': benchmark_outcome,
        'report_ready': report_ready,
        'recoverable': recoverable,
        'ownership_unknown': ownership_state == 'unknown',
        'region': plan.get('region'),
        'gcp_project_id': plan.get('gcp_project_id'),
        'gcp_zone': plan.get('gcp_zone'),
        'azure_subscription_id': plan.get('azure_subscription_id'),
        'azure_zone': plan.get('azure_zone'),
        'availability_domain': plan.get('availability_domain'),
        'shape': plan.get('shape'),
        'ocpus': plan.get('ocpus'),
        'cpu_kind': 'OCPU' if provider == 'oci' else 'vCPU',
        'memory_gb': plan.get('memory_gb'),
        'architecture': architecture,
        'benchmarks': benchmark_names,
        'comparison_result_ids': comparison_result_ids,
    }


def report_summary(report_path):
    return run_summary(report_path.parent)


def saved_report_summaries():
    directories = {
        path.parent
        for pattern in ('*/report.html', '*/state.json', '*/results.json')
        for path in RUNS.glob(pattern)
    }
    runs = [run_summary(directory) for directory in directories]
    return sorted(runs, key=lambda item: item['created_at'], reverse=True)


def saved_run_directories():
    """Return only direct, app-owned run directories eligible for review."""
    directories = []
    for directory in RUNS.iterdir():
        if (
            directory.is_symlink()
            or not directory.is_dir()
            or not RUN_ID_PATTERN.fullmatch(directory.name)
        ):
            continue
        if not any(
            (directory / artifact).is_file()
            for artifact in SAVED_RUN_ARTIFACTS
        ):
            continue
        directories.append(directory)
    return sorted(directories, key=lambda item: item.name)


def saved_run_can_be_deleted(directory, *, lease_held=False):
    """Observe whether a saved run is finalized and locally disposable.

    This observation does not authorize deletion unless ``lease_held`` is
    true. Callers that mutate history must acquire the run lease first and
    keep it through removal.
    """
    job_id = directory.name
    if (
        not lease_held
        and not run_lease_is_definitively_unheld(directory.parent, job_id)
    ):
        return False
    task = job_tasks.get(job_id)
    if task is not None:
        try:
            if not task.done():
                return False
        except Exception:
            return False
    with job_processes_lock:
        if job_processes.get(job_id):
            return False

    job = jobs.get(job_id)
    state_path = directory / 'state.json'
    if job is None and (state_path.exists() or state_path.is_symlink()):
        if state_path.is_symlink():
            return False
        try:
            job = load_persisted_job(job_id)
        except Exception:
            return False
        if job is None:
            # A present state file that cannot prove its own identity or
            # lifecycle may still be the only cloud-cleanup manifest.
            return False
    if job is None:
        # Reports and structured results created before state persistence do
        # not contain a resource manifest that this app could later recover.
        return True

    status = job.get('status')
    if not isinstance(status, str):
        return False
    status = status.strip().lower()
    if status in ACTIVE_RUN_STATUSES or status in PRESERVED_RUN_STATUSES:
        return False
    # Every provider assigns ``destroyed`` only after its cleanup routine has
    # proved that the run-owned resources are absent. Some providers retain
    # harmless ownership metadata (for example, the AWS account ID), so the
    # generic recovery detector is intentionally bypassed for this definitive
    # terminal state.
    if status == 'destroyed':
        return True
    try:
        if has_recoverable_resources_for_any_provider(job):
            return False
    except Exception:
        return False
    return status in DELETABLE_RUN_STATUSES


def forget_deleted_run(job_id):
    """Drop stale terminal registries only after local history is gone."""
    jobs.pop(job_id, None)
    job_tasks.pop(job_id, None)
    job_cancel_events.pop(job_id, None)
    with job_processes_lock:
        if not job_processes.get(job_id):
            job_processes.pop(job_id, None)


@app.get('/')
def home():
    return FileResponse(
        ROOT / 'static' / 'index.html',
        headers={'Cache-Control': 'no-store'},
    )


@app.get('/history')
def history():
    return FileResponse(
        ROOT / 'static' / 'history.html',
        headers={'Cache-Control': 'no-store'},
    )


@app.get('/comparison')
def comparison_page():
    return FileResponse(
        ROOT / 'static' / 'comparison.html',
        headers={'Cache-Control': 'no-store'},
    )


@app.get('/api/config/ssh-defaults')
def ssh_defaults(request: Request):
    if not is_loopback_request(request):
        raise HTTPException(
            403,
            'SSH key defaults are available only through a loopback connection.',
        )
    return JSONResponse(
        load_ssh_defaults(),
        headers={'Cache-Control': 'no-store'},
    )

@app.get('/api/catalog')
def catalog():
    return {
        'benchmarks': BENCHMARKS,
        'llm_benchmarks': LLM_BENCHMARKS,
        'sysbench_workloads': SYSBENCH_WORKLOADS,
        'iperf3_protocols': IPERF3_PROTOCOLS,
        'phoronix_profiles': PHORONIX_PROFILES,
        'apachebench_workloads': APACHEBENCH_WORKLOADS,
        'deathstarbench_workloads': DEATHSTARBENCH_WORKLOADS,
    }


@app.get('/api/providers')
def providers():
    return {'items': [provider.as_dict() for provider in registered_providers()]}


@app.get('/api/reports')
def reports():
    return JSONResponse(
        {'items': saved_report_summaries()},
        headers={'Cache-Control': 'no-store'},
    )


@app.delete('/api/reports')
def clear_saved_runs(confirmation: ClearSavedRunsRequest):
    """Permanently remove only finalized local history that is safe to lose."""
    # Keep the destructive precondition explicit here as well as in FastAPI's
    # strict request validation.
    if confirmation.confirmed is not True:
        raise HTTPException(422, 'Explicit confirmation is required.')

    deleted_run_ids = []
    preserved_run_ids = []
    for directory in saved_run_directories():
        job_id = directory.name
        try:
            deletion_lease = acquire_run_lease(
                RUNS,
                job_id,
                'history-delete',
            )
        except (RunLeaseError, ValueError):
            preserved_run_ids.append(job_id)
            continue
        quarantine = RUNS / f'.deleting-{job_id}-{uuid.uuid4().hex}'
        try:
            # Re-read all local lifecycle guards while ownership is held.
            if not saved_run_can_be_deleted(directory, lease_held=True):
                preserved_run_ids.append(job_id)
                continue
            try:
                # Rename first so unlinking the locked lease inode cannot let
                # another process create a second lease for the same visible
                # run directory while recursive removal is in progress.
                directory.rename(quarantine)
                shutil.rmtree(quarantine)
            except OSError:
                # Never restore a quarantine after recursive deletion began:
                # rmtree may already have unlinked the held lock inode, and a
                # restored canonical directory could then admit a second lock
                # owner. Hidden remnants are retained for operator recovery.
                if not directory.exists():
                    forget_deleted_run(job_id)
                preserved_run_ids.append(job_id)
                continue
            forget_deleted_run(job_id)
            deleted_run_ids.append(job_id)
        finally:
            deletion_lease.release()

    preserved_run_ids.sort()
    return JSONResponse(
        {
            'deleted_count': len(deleted_run_ids),
            'preserved_count': len(preserved_run_ids),
            'preserved_run_ids': preserved_run_ids,
        },
        headers={'Cache-Control': 'no-store'},
    )

@app.get('/api/reports/latest')
def latest_report():
    runs = saved_report_summaries()
    if not runs:
        raise HTTPException(404, 'No saved runs are available')
    return {'id': runs[0]['id']}

@app.get('/api/oci/bootstrap')
def bootstrap():
    try:
        cfg = config()
        identity = oci.identity.IdentityClient(cfg)
        regions = [
            r.name
            for r in oci.pagination.list_call_get_all_results(
                identity.list_regions
            ).data
        ]
        return {'default_region': cfg['region'], 'default_compartment': cfg['tenancy'], 'regions': sorted(regions)}
    except Exception as exc:
        raise HTTPException(400, f'Unable to read DEFAULT OCI profile: {exc}')

@app.get('/api/oci/placement')
def placement(region: str, compartment_id: str | None = None):
    try:
        cfg, _, _, _, identity = clients(region)
        compartment_id = compartment_id or cfg['tenancy']
        ads = identity.list_availability_domains(compartment_id).data
        data = []
        for ad in ads:
            fds = identity.list_fault_domains(compartment_id, ad.name).data
            data.append({'name': ad.name, 'fault_domains': [f.name for f in fds]})
        return {'availability_domains': data}
    except Exception as exc: raise HTTPException(400, str(exc))


def _oci_local_nvme_storage_summary(shape):
    """Return a fail-closed local-NVMe profile from one OCI Shape."""

    unavailable = {
        'local_nvme_supported': False,
        'local_nvme_disk_count': 0,
        'local_nvme_disk_size_gb': 0,
        'local_nvme_total_size_gb': 0,
    }
    raw_count = getattr(shape, 'local_disks', 0)
    raw_total = getattr(shape, 'local_disks_total_size_in_gbs', 0)
    description = str(
        getattr(shape, 'local_disk_description', '') or ''
    ).strip()
    try:
        total_size_gb = float(raw_total)
    except (TypeError, ValueError):
        return unavailable
    if (
        isinstance(raw_count, bool)
        or not isinstance(raw_count, int)
        or raw_count <= 0
        or isinstance(raw_total, bool)
        or not isinstance(raw_total, (int, float))
        or not math.isfinite(total_size_gb)
        or total_size_gb <= 0
        or 'nvme' not in description.casefold()
    ):
        return unavailable
    return {
        'local_nvme_supported': True,
        'local_nvme_disk_count': raw_count,
        'local_nvme_disk_size_gb': total_size_gb / raw_count,
        'local_nvme_total_size_gb': total_size_gb,
    }


def unique_shape_summaries(items):
    unique = {}
    for item in items:
        if item.shape in unique:
            continue
        unique[item.shape] = {
            'shape': item.shape,
            'ocpus': item.ocpus,
            'memory_gb': item.memory_in_gbs,
            'flexible': bool(item.ocpu_options or item.memory_options),
            'networking': item.networking_bandwidth_in_gbps,
            **_oci_local_nvme_storage_summary(item),
        }
    return [unique[name] for name in sorted(unique, key=str.casefold)]


@app.get('/api/oci/shapes')
def shapes(region: str, compartment_id: str | None = None, availability_domain: str | None = None):
    try:
        cfg, compute, _, _, _ = clients(region)
        list_kwargs = {}
        if availability_domain and availability_domain.strip():
            list_kwargs['availability_domain'] = availability_domain.strip()
        target_compartment = (compartment_id or '').strip() or cfg['tenancy']
        items = oci.pagination.list_call_get_all_results(
            compute.list_shapes,
            target_compartment,
            **list_kwargs,
        ).data
        return {'items': unique_shape_summaries(items)}
    except Exception as exc: raise HTTPException(400, str(exc))


def aws_api_error(action, exc):
    message = str(exc)
    if 'SSO' in message and ('expired' in message.lower() or 'token' in message.lower()):
        message += ' Run `aws sso login --profile default` and try again.'
    return HTTPException(
        400,
        f'Unable to {action} with the local AWS profile: {message}',
        headers={'Cache-Control': 'no-store'},
    )


def gcp_api_error(action, exc):
    message = str(exc)
    if 'credential' in message.casefold() or 'auth' in message.casefold():
        message += (
            ' Run `gcloud auth application-default login` and try again.'
        )
    return HTTPException(
        400,
        f'Unable to {action} with local Google Cloud ADC: {message}',
        headers={'Cache-Control': 'no-store'},
    )


def azure_api_error(action, exc):
    message = str(exc)
    lowered = message.casefold()
    if 'credential' in lowered or 'authentication' in lowered or 'login' in lowered:
        message += ' Run `az login`, select a subscription, and try again.'
    return HTTPException(
        400,
        f'Unable to {action} with the local Azure CLI session: {message}',
        headers={'Cache-Control': 'no-store'},
    )


@app.get('/api/providers/aws/bootstrap')
def aws_bootstrap(profile: str = 'default'):
    try:
        return JSONResponse(
            aws_provider.bootstrap(
                profile=(profile or 'default').strip(),
                default_region='us-east-2',
            ),
            headers={'Cache-Control': 'no-store'},
        )
    except Exception as exc:
        raise aws_api_error('read AWS identity and regions', exc) from exc


@app.get('/api/providers/aws/placement')
def aws_placement(
    region: str = 'us-east-2',
    profile: str = 'default',
):
    try:
        return JSONResponse(
            aws_provider.placement(
                profile=(profile or 'default').strip(),
                region=(region or 'us-east-2').strip(),
            ),
            headers={'Cache-Control': 'no-store'},
        )
    except Exception as exc:
        raise aws_api_error('list AWS Availability Zones', exc) from exc


@app.get('/api/providers/aws/instance-types')
def aws_instance_types(
    region: str = 'us-east-2',
    profile: str = 'default',
    availability_zone: str | None = None,
):
    try:
        return JSONResponse(
            aws_provider.instance_types(
                profile=(profile or 'default').strip(),
                region=(region or 'us-east-2').strip(),
                availability_zone=(availability_zone or '').strip() or None,
            ),
            headers={'Cache-Control': 'no-store'},
        )
    except Exception as exc:
        raise aws_api_error('list AWS EC2 instance types', exc) from exc


@app.get('/api/providers/gcp/bootstrap')
def gcp_bootstrap(project_id: str | None = None):
    try:
        return JSONResponse(
            gcp_provider.bootstrap(
                project_id=(
                    project_id or os.getenv('GCP_PROJECT_ID') or ''
                ).strip() or None,
                default_region=(
                    os.getenv('GCP_DEFAULT_REGION') or 'us-east1'
                ).strip(),
            ),
            headers={'Cache-Control': 'no-store'},
        )
    except Exception as exc:
        raise gcp_api_error(
            'read the Google Cloud ADC identity and regions',
            exc,
        ) from exc


@app.get('/api/providers/gcp/placement')
def gcp_placement(project_id: str, region: str):
    try:
        return JSONResponse(
            gcp_provider.placement(
                project_id=project_id.strip(),
                region=region.strip(),
            ),
            headers={'Cache-Control': 'no-store'},
        )
    except Exception as exc:
        raise gcp_api_error('list Google Cloud zones', exc) from exc


@app.get('/api/providers/gcp/machine-types')
def gcp_machine_types(project_id: str, zone: str):
    try:
        return JSONResponse(
            gcp_provider.machine_types(
                project_id=project_id.strip(),
                zone=zone.strip(),
            ),
            headers={'Cache-Control': 'no-store'},
        )
    except Exception as exc:
        raise gcp_api_error(
            'list Google Cloud Compute Engine machine types',
            exc,
        ) from exc


@app.get('/api/providers/azure/bootstrap')
def azure_bootstrap(subscription_id: str | None = None):
    try:
        return JSONResponse(
            azure_provider.bootstrap(
                subscription_id=(
                    subscription_id
                    or os.getenv('AZURE_SUBSCRIPTION_ID')
                    or ''
                ).strip() or None,
                default_region=(
                    os.getenv('AZURE_DEFAULT_REGION') or 'eastus2'
                ).strip(),
            ),
            headers={'Cache-Control': 'no-store'},
        )
    except Exception as exc:
        raise azure_api_error(
            'read the Azure CLI identity, subscriptions, and regions',
            exc,
        ) from exc


@app.get('/api/providers/azure/placement')
def azure_placement(subscription_id: str, region: str):
    try:
        return JSONResponse(
            azure_provider.placement(
                subscription_id=subscription_id.strip(),
                region=region.strip(),
            ),
            headers={'Cache-Control': 'no-store'},
        )
    except Exception as exc:
        raise azure_api_error(
            'list Azure availability zones and quota usage',
            exc,
        ) from exc


@app.get('/api/providers/azure/vm-sizes')
def azure_vm_sizes(
    subscription_id: str,
    region: str,
    zone: str | None = None,
):
    try:
        return JSONResponse(
            azure_provider.vm_sizes(
                subscription_id=subscription_id.strip(),
                region=region.strip(),
                zone=(zone or '').strip() or None,
            ),
            headers={'Cache-Control': 'no-store'},
        )
    except Exception as exc:
        raise azure_api_error(
            'list Azure virtual machine sizes',
            exc,
        ) from exc

@app.post('/api/jobs')
async def create_job(plan: BenchmarkPlan):
    adapter = provider_adapter(plan)
    if 'deathstarbench' in plan.benchmarks:
        try:
            require_released_runtime(
                plan.deathstarbench.topology_id,
                plan.deathstarbench.runtime_id,
            )
        except ValueError as exc:
            # Reject unreleased topology contracts before SSH validation,
            # filesystem writes, task creation, or any cloud API call.
            raise HTTPException(422, str(exc)) from exc
    try:
        derived_public_key = derive_public_key(
            plan.ssh_private_key,
            plan.ssh_key_passphrase,
        )
        uploaded_public_key = normalize_public_key(plan.ssh_public_key)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    if not hmac.compare_digest(derived_public_key, uploaded_public_key):
        raise HTTPException(
            422,
            'The uploaded public key does not match the supplied private key.',
        )
    if (
        adapter.id == 'aws'
        and not uploaded_public_key.startswith(('ssh-rsa ', 'ssh-ed25519 '))
    ):
        raise HTTPException(
            422,
            'AWS EC2 key-pair import supports RSA and Ed25519 public keys. '
            'Configure one of those key types in .env for an AWS run.',
        )
    job_id = uuid.uuid4().hex[:12]
    sanitized_plan = plan.model_dump(
        exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'}
    )
    job = {
        'id': job_id,
        'plan': sanitized_plan,
        '_key': normalize_private_key(plan.ssh_private_key),
        '_passphrase': plan.ssh_key_passphrase,
        '_public_key': uploaded_public_key,
        '_persist_state': True,
        '_cancel_event': threading.Event(),
        'status': 'queued',
        'events': [],
        'resources': {},
        'results': [],
        'created_at': now(),
        'updated_at': now(),
    }
    run_directory = RUNS / job_id
    run_directory.mkdir(parents=True, exist_ok=False)
    try:
        run_lease = acquire_run_lease(RUNS, job_id, 'web-run')
    except (RunLeaseError, ValueError) as exc:
        raise HTTPException(
            500,
            'Unable to establish exclusive ownership for the new run.',
        ) from exc
    try:
        (run_directory / 'plan.json').write_text(
            json.dumps(sanitized_plan, indent=2)
        )
        jobs[job_id] = job
        event(
            job,
            'Queued',
            f'Plan accepted. Waiting to provision {adapter.short_name} resources.',
        )
        task = asyncio.create_task(run_job(job, plan, run_lease=run_lease))
    except BaseException:
        jobs.pop(job_id, None)
        run_lease.release()
        raise
    # A task cancelled before its coroutine executes cannot enter run_job's
    # finally block. The callback is the never-started fallback; normal and
    # in-flight cancellation releases are idempotent and thread-safe.
    task.add_done_callback(lambda _completed: run_lease.release())
    retain_job_task(job_id, task, cancel_event=job['_cancel_event'])
    return {'id': job_id}


def load_persisted_job(job_id):
    state_path = RUNS / job_id / 'state.json'
    if not state_path.exists():
        return None
    saved = normalize_saved_job_state(read_json_file(state_path, {}))
    if not saved or saved.get('id') != job_id:
        return None
    saved.setdefault('events', [])
    saved.setdefault('resources', {})
    saved.setdefault('results', [])
    saved.setdefault('plan', read_json_file(state_path.parent / 'plan.json', {}))
    if expected_benchmark_result_ids(saved['plan']):
        saved['benchmark_status'] = benchmark_status(saved)
    saved['_persist_state'] = True
    saved['_persist_results_artifact'] = False
    return saved

@app.get('/api/jobs/{job_id}')
def job_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        report_path = RUNS / job_id / 'report.html'
        saved = load_persisted_job(job_id)
        if saved:
            safe = {k: v for k, v in saved.items() if not k.startswith('_')}
            safe['report_ready'] = report_path.exists()
            safe['live'] = False
            safe['recoverable'] = has_recoverable_resources(safe)
            safe['ownership_unknown'] = False
            if safe.get('status') in ACTIVE_RUN_STATUSES:
                ownership_state = run_lease_ownership_state(RUNS, job_id)
                if ownership_state == 'unheld':
                    safe['status'] = 'interrupted'
                    safe['error'] = (
                        'The local app stopped before this lifecycle completed. '
                        'Its recorded resources can still be destroyed.'
                    )
                elif ownership_state == 'unknown':
                    safe['ownership_unknown'] = True
                    safe['ownership_message'] = (
                        'Run ownership could not be verified. Cleanup and '
                        'history deletion remain disabled until the local '
                        'lease files are repaired.'
                    )
            return safe
        if report_path.exists():
            return {
                'id': job_id,
                'status': 'reported',
                'benchmark_status': saved_report_benchmark_status(report_path),
                'events': [],
                'resources': {},
                'results': [{'name': 'Saved benchmark report'}],
                'report_ready': True,
                'live': False,
            }
        raise HTTPException(404, 'Job not found')
    safe = {k:v for k,v in job.items() if not k.startswith('_')}
    safe['benchmark_status'] = benchmark_status(job)
    safe['report_ready'] = (RUNS / job_id / 'report.html').exists()
    safe['live'] = True
    safe['recoverable'] = has_recoverable_resources(safe)
    return safe

@app.get('/api/jobs/{job_id}/report')
def report(job_id: str, download: bool = False):
    path = RUNS / job_id / 'report.html'
    if not path.exists(): raise HTTPException(404, 'Report not ready')
    if download:
        plan = read_json_file(path.parent / 'plan.json', {})
        provider = str(plan.get('provider', 'oci')).lower()
        return FileResponse(
            path,
            media_type='text/html',
            filename=f'{provider}-benchmark-{job_id}.html',
        )
    return FileResponse(path, media_type='text/html')

@app.get('/api/jobs/{job_id}/plan')
def report_plan(job_id: str):
    job = jobs.get(job_id)
    if job:
        return canonicalize_benchmark_plan(job['plan'])
    plan_path = RUNS / job_id / 'plan.json'
    if plan_path.exists():
        return canonicalize_benchmark_plan(json.loads(plan_path.read_text()))
    report_path = RUNS / job_id / 'report.html'
    if report_path.exists():
        match = re.search(
            r'<h2>Configuration</h2><pre>(.*?)</pre>',
            report_path.read_text(),
            re.DOTALL,
        )
        if match:
            return canonicalize_benchmark_plan(
                json.loads(html.unescape(match.group(1)))
            )
    raise HTTPException(404, 'Saved plan not found')


def results_document_for_job(job_id):
    """Load a saved result document, with a safe live-run fallback."""
    try:
        document = load_results_document(RUNS / job_id)
    except FileNotFoundError:
        job = jobs.get(job_id)
        if job is not None:
            document = build_results_artifact(results_artifact_job(job))
        else:
            raise
    recorded_id = str((document.get('run') or {}).get('id') or '')
    if recorded_id and recorded_id != job_id:
        raise ValueError(
            f'The saved run identity {recorded_id!r} does not match '
            f'{job_id!r}.'
        )
    return document


def parse_comparison_run_ids(value):
    """Validate the bounded canonical run-ID list accepted by the API."""
    if not isinstance(value, str):
        raise ValueError('Provide exactly one runs query parameter.')
    run_ids = value.split(',')
    if not 2 <= len(run_ids) <= 8:
        raise ValueError('Select between 2 and 8 runs for comparison.')
    if any(not RUN_ID_PATTERN.fullmatch(run_id) for run_id in run_ids):
        raise ValueError(
            'Every comparison run ID must be exactly 12 lowercase '
            'hexadecimal characters.'
        )
    if len(set(run_ids)) != len(run_ids):
        raise ValueError('Comparison run IDs must be unique.')
    return run_ids


@app.get('/api/jobs/{job_id}/results')
def job_results(job_id: str):
    if not RUN_ID_PATTERN.fullmatch(job_id):
        raise HTTPException(
            404,
            'Results not found',
            headers={'Cache-Control': 'no-store'},
        )
    try:
        document = results_document_for_job(job_id)
    except FileNotFoundError:
        raise HTTPException(
            404,
            'Results not found',
            headers={'Cache-Control': 'no-store'},
        ) from None
    except ValueError as exc:
        raise HTTPException(
            422,
            str(exc),
            headers={'Cache-Control': 'no-store'},
        ) from exc
    return JSONResponse(
        document,
        headers={'Cache-Control': 'no-store'},
    )


@app.get('/api/comparisons')
def comparisons(runs: str | None = None):
    try:
        run_ids = parse_comparison_run_ids(runs)
    except ValueError as exc:
        raise HTTPException(
            422,
            str(exc),
            headers={'Cache-Control': 'no-store'},
        ) from exc

    documents = []
    for run_id in run_ids:
        try:
            documents.append(results_document_for_job(run_id))
        except FileNotFoundError:
            raise HTTPException(
                404,
                f'Run {run_id} has no saved benchmark results.',
                headers={'Cache-Control': 'no-store'},
            ) from None
        except ValueError as exc:
            raise HTTPException(
                422,
                f'Run {run_id} has invalid saved results: {exc}',
                headers={'Cache-Control': 'no-store'},
            ) from exc
    return JSONResponse(
        build_comparison_payload(documents),
        headers={'Cache-Control': 'no-store'},
    )

@app.post('/api/jobs/{job_id}/destroy')
async def destroy(job_id: str):
    if not RUN_ID_PATTERN.fullmatch(job_id):
        raise HTTPException(404, 'Job not found')
    job = jobs.get(job_id)
    live_job = job is not None
    if job is not None and job['status'] == 'destroyed':
        return {'status': job['status']}
    supervisor = job_tasks.get(job_id)
    supervisor_active = bool(supervisor and not supervisor.done())
    if live_job and supervisor_active:
        cancellable_statuses = {
            'queued',
            'provisioning',
            'testing',
            'reporting',
            'cancelling',
        }
        if job['status'] in {
            'cleanup_pending',
            'destroying',
        }:
            # The supervisor already owns this cleanup attempt. Never launch a
            # second cleanup worker against the same resource manifest.
            return {'status': job['status']}
        if job['status'] in cancellable_statuses:
            cancel = cancellation_event(job)
            if cancel is None:
                cancel = threading.Event()
                job['_cancel_event'] = cancel
                job_cancel_events[job_id] = cancel
            if not cancel.is_set():
                outcome_before_stop = benchmark_status(job)
                if outcome_before_stop not in {'complete', 'failed'}:
                    job['benchmark_interrupted'] = True
                job['cancel_requested_at'] = now()
                job['status'] = 'cancelling'
                event(
                    job,
                    'Stop requested',
                    'Stopping the active benchmark operation before '
                    'destroying its infrastructure.',
                )
                cancel.set()
            signal_job_processes(job_id)
            return {'status': job['status']}
        # A retained or failed run can become terminal immediately before its
        # supervisor's final persistence step. Wait for that producer to exit
        # before starting manual cleanup; do not relabel completed results as
        # interrupted merely because the task callback has not run yet.
        await asyncio.shield(supervisor)
        if job['status'] == 'destroyed':
            return {'status': job['status']}

    # Every manual cleanup without an active in-memory supervisor is a
    # persisted cleanup. This includes retries of jobs that this process
    # recovered earlier: treating those objects as ordinary live jobs would
    # let a retry bypass ownership after the first cleanup task completed.
    job = load_persisted_job(job_id)
    if job is None and live_job:
        # Normal API jobs persist before they can reach this path. Keep the
        # in-memory fallback recoverable if an older/test producer omitted its
        # initial state write, then continue through the same leased path.
        in_memory_job = jobs.get(job_id)
        if in_memory_job is not None:
            persist_job_state(in_memory_job)
            job = load_persisted_job(job_id)
    if not job:
        raise HTTPException(404, 'Job not found')
    if job['status'] == 'destroyed':
        return {'status': job['status']}
    try:
        cleanup_lease = acquire_run_lease(
            RUNS,
            job_id,
            'api-destroy',
        )
    except RunLeaseHeldError as exc:
        raise HTTPException(
            409,
            'This run is active in another benchmark process. Wait for '
            'that operation to finish before destroying infrastructure.',
        ) from exc
    except RunLeaseError as exc:
        raise HTTPException(
            409,
            'Run ownership could not be established safely, so no cleanup '
            'was started.',
        ) from exc
    try:
        # State may have changed between the initial existence check and lease
        # acquisition. Only the fresh lease-protected manifest can authorize
        # mutation or enter the in-memory registry.
        job = load_persisted_job(job_id)
        if not job:
            raise HTTPException(404, 'Job not found')
        if job['status'] == 'destroyed':
            cleanup_lease.release()
            return {'status': job['status']}
        jobs[job_id] = job
    except BaseException:
        if not cleanup_lease.released:
            cleanup_lease.release()
        raise

    persisted_status = job.get('status')
    if (
        persisted_status in {
            'queued',
            'provisioning',
            'testing',
            'reporting',
            'cancelling',
        }
        and job.get('benchmark_status') not in {'complete', 'failed'}
    ):
        # A restarted app cannot resume the benchmark process itself. Preserve
        # that partial outcome before cleanup changes the lifecycle status to
        # ``destroying`` and later ``destroyed``.
        job['benchmark_interrupted'] = True
    # A persisted ``destroying`` state means the process stopped before the
    # cleanup task finished.  Mark the retry synchronously so a second click
    # cannot enqueue another cleanup task, then resume from the exact recorded
    # provider resource manifest.
    job['status'] = 'destroying'
    try:
        persist_job_state(job)
    except BaseException:
        if not cleanup_lease.released:
            cleanup_lease.release()
        raise

    # Submit the thread before creating its awaiter. The worker, not the
    # asyncio Task, owns release of the lease. If the waiter has started,
    # ``shield`` delays its cancellation until the thread stops. If it is
    # cancelled before its first step, the submitted worker still retains the
    # lease, so another cleanup attempt fails closed until that worker exits.
    loop = asyncio.get_running_loop()
    try:
        cleanup_future = loop.run_in_executor(
            None,
            destroy_with_status_under_lease,
            job,
            cleanup_lease,
        )
    except BaseException:
        if not cleanup_lease.released:
            cleanup_lease.release()
        raise
    # Once submission succeeds, only the worker may release ownership. Even a
    # callback/task construction failure must not expose an unlocked run while
    # that worker can still mutate cloud or persisted state.
    cleanup_future.add_done_callback(consume_future_exception)
    cleanup_waiter = wait_for_uncancellable_cleanup(cleanup_future)
    try:
        cleanup_task = asyncio.create_task(cleanup_waiter)
    except BaseException:
        # The already-submitted worker retains ownership and will release the
        # lease in its own ``finally`` block.
        cleanup_waiter.close()
        raise
    retain_job_task(job_id, cleanup_task)
    return {'status': 'destroying'}


async def wait_for_uncancellable_cleanup(cleanup_future):
    """Delay waiter cancellation until its cleanup thread has really ended."""

    cancellation = None
    while True:
        try:
            result = await asyncio.shield(cleanup_future)
            break
        except asyncio.CancelledError as exc:
            cancellation = cancellation or exc
    if cancellation is not None:
        raise cancellation
    return result


async def run_in_thread_uncancellable(function, *args):
    """Run one lifecycle mutation without outliving its owning supervisor."""

    loop = asyncio.get_running_loop()
    worker = loop.run_in_executor(None, function, *args)
    return await wait_for_uncancellable_cleanup(worker)


def consume_future_exception(completed_future):
    """Retrieve an orphaned executor failure after Task creation failed."""

    try:
        completed_future.exception()
    except asyncio.CancelledError:
        pass

def normalize_private_key(private_key):
    return private_key.replace('\r\n', '\n').replace('\r', '\n').strip() + '\n'

def normalize_public_key(public_key):
    normalized = public_key.replace('\r\n', '\n').replace('\r', '\n').strip()
    if 'PRIVATE KEY' in normalized:
        raise ValueError(
            'The public-key field contains a private key. Upload the matching '
            '.pub file instead.'
        )
    try:
        key = serialization.load_ssh_public_key(normalized.encode())
    except (TypeError, ValueError):
        raise ValueError(
            'The uploaded public key is malformed or unsupported. Choose an '
            'OpenSSH .pub file.'
        ) from None
    return key.public_bytes(
        serialization.Encoding.OpenSSH,
        serialization.PublicFormat.OpenSSH,
    ).decode()

def derive_public_key(private_key, passphrase=None):
    normalized = normalize_private_key(private_key)
    if normalized.startswith(('ssh-rsa ', 'ssh-ed25519 ', 'ecdsa-sha2-')):
        raise ValueError(
            'The supplied value is a public key. Paste or choose the matching '
            'private key file instead.'
        )
    password = passphrase.encode() if passphrase else None
    key_bytes = normalized.encode()
    try:
        if 'BEGIN OPENSSH PRIVATE KEY' in normalized:
            key = serialization.load_ssh_private_key(key_bytes, password=password)
        else:
            key = serialization.load_pem_private_key(key_bytes, password=password)
    except TypeError as exc:
        message = str(exc).lower()
        if (
            'password was not given' in message
            or 'password was not provided' in message
            or 'password-protected' in message
            or 'private key is encrypted' in message
        ):
            raise ValueError(
                'This SSH private key is encrypted. Enter its passphrase and try again.'
            ) from None
        if 'not encrypted' in message:
            raise ValueError(
                'A passphrase was supplied for an unencrypted SSH key. Clear the '
                'passphrase and try again.'
            ) from None
        raise ValueError(f'Unable to read the SSH private key: {exc}') from None
    except ValueError:
        if passphrase:
            raise ValueError(
                'Unable to decrypt the SSH private key. Check the passphrase and '
                'key contents.'
            ) from None
        raise ValueError(
            'The SSH private key is malformed or uses an unsupported format. '
            'Choose the original OpenSSH, RSA, ECDSA, or Ed25519 private key file.'
        ) from None
    return key.public_key().public_bytes(
        serialization.Encoding.OpenSSH,
        serialization.PublicFormat.OpenSSH,
    ).decode()

def platform_config(plan):
    """Request shielded/confidential controls; OCI validates shape support at launch."""
    if plan.security.mode == 'none':
        return None
    if plan.shape.startswith('VM.Standard.E'):
        config_type = 'AMD_VM'
    elif plan.shape.startswith('VM.'):
        config_type = 'INTEL_VM'
    else:
        config_type = 'GENERIC_BM'
    return oci.core.models.LaunchInstancePlatformConfig(
        type=config_type,
        is_secure_boot_enabled=plan.security.secure_boot if plan.security.mode == 'shielded' else False,
        is_measured_boot_enabled=plan.security.measured_boot if plan.security.mode == 'shielded' else False,
        is_trusted_platform_module_enabled=plan.security.trusted_platform_module if plan.security.mode == 'shielded' else False,
        is_memory_encryption_enabled=plan.security.mode == 'confidential',
    )

async def finish_cancelled_job(job):
    """Let the one run supervisor serialize interruption and cloud cleanup."""
    if job.get('_cancellation_cleanup_started'):
        return
    job['_cancellation_cleanup_started'] = True
    outcome_before_cleanup = benchmark_status(job)
    if outcome_before_cleanup not in {'complete', 'failed'}:
        job['benchmark_interrupted'] = True
        job['error'] = None
    if job.get('status') == 'destroyed':
        persist_job_state(job)
        return
    job['status'] = 'cleanup_pending'
    event(
        job,
        'Stopped',
        'The active run operation stopped. Saving any available results before '
        'destroying its infrastructure.',
    )
    if job.get('results'):
        try:
            await run_in_thread_uncancellable(make_report, job)
        except Exception as report_error:
            event(job, 'Report warning', str(report_error))
    await run_in_thread_uncancellable(destroy_with_status, job)


async def run_job(job, plan, *, run_lease=None):
    try:
        raise_if_cancelled(job)
        adapter = provider_adapter(plan)
        job['status'] = 'provisioning'
        event(
            job,
            'Provision',
            adapter.provisioning_message,
        )
        await run_in_thread_uncancellable(provision, job, plan)
        raise_if_cancelled(job)
        job['status'] = 'testing'; event(job, 'Connect', 'Instance is ready; connecting with SSH.')
        await run_in_thread_uncancellable(run_benchmarks, job, plan)
        raise_if_cancelled(job)
        require_complete_benchmark_results(job)
        job['status'] = 'reporting'; event(job, 'Report', 'Generating standalone HTML report.')
        await run_in_thread_uncancellable(make_report, job)
        raise_if_cancelled(job)
        if plan.destroy_after_completion:
            job['status'] = 'cleanup_pending'
            event(
                job,
                'Cleanup',
                'The report is ready; starting automatic infrastructure '
                'cleanup.',
            )
            await run_in_thread_uncancellable(destroy_with_status, job)
        else:
            job['status'] = 'complete'; event(job, 'Complete', 'Benchmarks and report are ready. Infrastructure has been retained.')
    except RunCancelled:
        await finish_cancelled_job(job)
    except Exception as exc:
        if cancellation_requested(job):
            await finish_cancelled_job(job)
            return
        fail(job, str(exc))
        if plan.destroy_after_completion:
            job['status'] = 'cleanup_pending'
            outcome = benchmark_status(job)
            if outcome == 'complete':
                cleanup_message = (
                    'The benchmarks completed, but a later step failed; '
                    'preserving results before automatic infrastructure cleanup.'
                )
            elif any(
                result.get('status') == 'completed'
                for result in job.get('results', [])
            ):
                cleanup_message = (
                    'The run failed after some benchmarks completed; saving '
                    'partial results before automatic infrastructure cleanup.'
                )
            else:
                cleanup_message = (
                    'The run failed before producing benchmark results; saving '
                    'diagnostics before automatic infrastructure cleanup.'
                )
            event(
                job,
                'Cleanup',
                cleanup_message,
            )
        if job['results']:
            try:
                await run_in_thread_uncancellable(make_report, job)
            except Exception as report_error:
                event(job, 'Report warning', str(report_error))
        if plan.destroy_after_completion:
            try:
                await run_in_thread_uncancellable(destroy_with_status, job)
            except Exception as cleanup:
                event(job, 'Cleanup warning', str(cleanup))
        elif job['resources'].get('public_ip'):
            ssh_user = job['resources'].get('ssh_user', 'opc')
            event(
                job,
                'Retained',
                f'Infrastructure retained after failure. Connect with: ssh -i '
                f'/path/to/private-key {ssh_user}@'
                f'{job["resources"]["public_ip"]}',
            )
    finally:
        try:
            if cancellation_requested(job):
                await finish_cancelled_job(job)
            job.pop('_key', None)
            job.pop('_passphrase', None)
            job.pop('_public_key', None)
            job.pop('_cancel_event', None)
            persist_job_state(job)
        finally:
            if run_lease is not None:
                run_lease.release()


def latest_oracle_linux_image(compute, compartment_id, shape, require_ol9=False):
    images = compute.list_images(
        compartment_id,
        operating_system='Oracle Linux',
        shape=shape,
        sort_by='TIMECREATED',
        sort_order='DESC',
    ).data
    for image in images:
        if image.lifecycle_state != 'AVAILABLE':
            continue
        version = str(getattr(image, 'operating_system_version', '') or '')
        if require_ol9 and not version.startswith('9'):
            continue
        return image
    version_label = ' 9' if require_ol9 else ''
    raise RuntimeError(
        f'No available Oracle Linux{version_label} image supports {shape}.'
    )


def load_generator_shape(available_shapes):
    by_name = {item.shape: item for item in available_shapes}
    for shape_name in LOAD_GENERATOR_SHAPES:
        if shape_name in by_name:
            return by_name[shape_name]
    raise RuntimeError(
        'The selected web benchmark needs an x86 flexible shape for its separate load '
        'generator, but none of the supported shapes are available in the '
        'selected availability domain.'
    )


def configured_network_bandwidth_gbps(shape, ocpus):
    """Resolve a flex shape's configured VNIC bandwidth from OCI metadata."""
    options = getattr(shape, 'networking_bandwidth_options', None)
    per_ocpu = getattr(options, 'default_per_ocpu_in_gbps', None)
    if per_ocpu is not None:
        bandwidth = float(per_ocpu) * float(ocpus)
        minimum = getattr(options, 'min_in_gbps', None)
        maximum = getattr(options, 'max_in_gbps', None)
        if minimum is not None:
            bandwidth = max(bandwidth, float(minimum))
        if maximum is not None:
            bandwidth = min(bandwidth, float(maximum))
    else:
        bandwidth = getattr(shape, 'networking_bandwidth_in_gbps', None)
    if bandwidth is None or float(bandwidth) <= 0:
        raise RuntimeError(
            f'OCI did not report network bandwidth for load-generator shape '
            f'{getattr(shape, "shape", "unknown")}.'
        )
    return float(bandwidth)


def provision(job, plan):
    selected_benchmarks = (
        plan.get('benchmarks', ())
        if isinstance(plan, dict)
        else getattr(plan, 'benchmarks', ())
    ) or ()
    if 'deathstarbench' in selected_benchmarks:
        # Defense in depth for direct/background callers that bypass the API.
        options = (
            plan.get('deathstarbench', {})
            if isinstance(plan, dict)
            else getattr(plan, 'deathstarbench', None)
        )
        topology_id = (
            options.get('topology_id', SINGLE_HOST_TOPOLOGY_ID)
            if isinstance(options, dict)
            else getattr(options, 'topology_id', SINGLE_HOST_TOPOLOGY_ID)
        )
        runtime_id = (
            options.get('runtime_id', PODMAN_COMPOSE_RUNTIME_ID)
            if isinstance(options, dict)
            else getattr(options, 'runtime_id', PODMAN_COMPOSE_RUNTIME_ID)
        )
        require_released_runtime(
            topology_id,
            runtime_id,
        )
    return dispatch_provider_operation(
        plan,
        'provision',
        {
            'oci': lambda: provision_oci(job, plan),
            'aws': lambda: aws_provider.provision(
                job,
                plan,
                public_key=job.get('_public_key'),
                emit=event,
                persist=persist_job_state,
            ),
            'gcp': lambda: gcp_provider.provision(
                job,
                plan,
                public_key=job.get('_public_key'),
                emit=event,
                persist=persist_job_state,
            ),
            'azure': lambda: azure_provider.provision(
                job,
                plan,
                public_key=job.get('_public_key'),
                emit=event,
                persist=persist_job_state,
            ),
        },
    )


def _oci_available_volume_device(compute, instance_id):
    """Reserve one OCI-reported virtual block-device path for ``/data``."""

    available = sorted(
        {
            str(getattr(device, 'name', '') or '').strip()
            for device in compute.list_instance_devices(instance_id).data
            if getattr(device, 'is_available', False) is True
        },
        key=str.casefold,
    )
    valid = [
        device
        for device in available
        if (
            re.fullmatch(
                r'/dev/(?:[A-Za-z0-9._+-]+/)*[A-Za-z0-9._+-]+',
                device,
            )
            and '..' not in device.split('/')
        )
    ]
    if not valid:
        raise RuntimeError(
            'OCI did not report an available virtual block-device path for '
            'the additional /data volume.'
        )
    return valid[0]


def provision_oci(job, plan):
    cfg, compute, network, storage, identity = clients(plan.region)
    iperf3_protocols = selected_iperf3_protocols(plan)
    uses_load_generator = plan_uses_load_generator(plan)
    compartment = plan.compartment_id or cfg['tenancy']; suffix = job['id']; tags = {'oci-benchmark-job': suffix, 'managed-by': 'oci-self-service-benchmarks'}
    ad = plan.availability_domain or identity.list_availability_domains(compartment).data[0].name
    vcn = network.create_vcn(oci.core.models.CreateVcnDetails(compartment_id=compartment, cidr_block='10.42.0.0/16', dns_label=f'b{suffix}', display_name=f'benchmark-{suffix}', freeform_tags=tags)).data
    record_resource(job, 'vcn_id', vcn.id)
    oci.wait_until(network, network.get_vcn(vcn.id), 'lifecycle_state', 'AVAILABLE')
    vcn = network.get_vcn(vcn.id).data
    igw = network.create_internet_gateway(oci.core.models.CreateInternetGatewayDetails(compartment_id=compartment, vcn_id=vcn.id, is_enabled=True, display_name=f'benchmark-igw-{suffix}', freeform_tags=tags)).data; record_resource(job, 'igw_id', igw.id)
    nat = network.create_nat_gateway(oci.core.models.CreateNatGatewayDetails(compartment_id=compartment, vcn_id=vcn.id, display_name=f'benchmark-nat-{suffix}', freeform_tags=tags)).data; record_resource(job, 'nat_id', nat.id)
    rt = network.create_route_table(oci.core.models.CreateRouteTableDetails(compartment_id=compartment, vcn_id=vcn.id, display_name=f'benchmark-routes-{suffix}', route_rules=[oci.core.models.RouteRule(destination='0.0.0.0/0', destination_type='CIDR_BLOCK', network_entity_id=igw.id)], freeform_tags=tags)).data; record_resource(job, 'route_table_id', rt.id)
    sl = network.create_security_list(oci.core.models.CreateSecurityListDetails(compartment_id=compartment, vcn_id=vcn.id, display_name=f'benchmark-security-{suffix}', ingress_security_rules=benchmark_security_rules(include_deathstarbench='deathstarbench' in plan.benchmarks, iperf3_protocols=iperf3_protocols, include_apachebench='apachebench' in plan.benchmarks), egress_security_rules=[oci.core.models.EgressSecurityRule(protocol='all', destination='0.0.0.0/0')], freeform_tags=tags)).data; record_resource(job, 'security_list_id', sl.id)
    subnet = network.create_subnet(oci.core.models.CreateSubnetDetails(compartment_id=compartment, vcn_id=vcn.id, cidr_block='10.42.1.0/24', dns_label='public', display_name=f'benchmark-public-{suffix}', route_table_id=rt.id, dhcp_options_id=vcn.default_dhcp_options_id, security_list_ids=[sl.id], prohibit_public_ip_on_vnic=False, freeform_tags=tags)).data; record_resource(job, 'subnet_id', subnet.id)
    event(job, 'Provision', 'Public subnet uses the VCN default Internet and VCN Resolver DHCP options.')
    image = latest_oracle_linux_image(
        compute,
        compartment,
        plan.shape,
        require_ol9=(
            'deathstarbench' in plan.benchmarks
            or 'apachebench' in plan.benchmarks
            or plan_uses_sysbench(plan)
            or plan_uses_iperf3(plan)
        ),
    )
    image_id = str(getattr(image, 'id', '') or '').strip()
    image_name = str(getattr(image, 'display_name', '') or '').strip()
    if not image_id or not image_name:
        raise RuntimeError(
            'OCI did not return the immutable ID and display name for the '
            'selected runner image.'
        )
    record_resource(job, 'image_id', image_id)
    record_resource(job, 'image_name', image_name)
    available_shapes = compute.list_shapes(
        compartment,
        availability_domain=ad,
    ).data
    listed_shape = next(x for x in available_shapes if x.shape == plan.shape)
    record_resource(job, 'provider', 'oci')
    shape_config = oci.core.models.LaunchInstanceShapeConfigDetails(ocpus=plan.ocpus, memory_in_gbs=plan.memory_gb) if (listed_shape.ocpu_options or listed_shape.memory_options) else None
    source = oci.core.models.InstanceSourceViaImageDetails(source_type='image', image_id=image_id, boot_volume_size_in_gbs=plan.storage.boot_size_gb, boot_volume_vpus_per_gb=plan.storage.boot_performance)
    launch_options = oci.core.models.LaunchOptions(network_type='VFIO' if plan.networking == 'sriov' else 'PARAVIRTUALIZED')
    launch = oci.core.models.LaunchInstanceDetails(compartment_id=compartment, availability_domain=ad, fault_domain=plan.fault_domain, shape=plan.shape, shape_config=shape_config, launch_options=launch_options, platform_config=platform_config(plan), display_name=f'benchmark-{suffix}', metadata={'ssh_authorized_keys': job['_public_key']}, source_details=source, create_vnic_details=oci.core.models.CreateVnicDetails(subnet_id=subnet.id, assign_public_ip=True, assign_private_dns_record=True, hostname_label='runner', skip_source_dest_check=False), freeform_tags=tags)
    instance = compute.launch_instance(launch).data; record_resource(job, 'instance_id', instance.id)
    event(job, 'Provision', f'Launched {plan.shape}; waiting for it to become RUNNING.')
    oci.wait_until(compute, compute.get_instance(instance.id), 'lifecycle_state', 'RUNNING')
    running_instance = compute.get_instance(instance.id).data
    instance_shape_config = getattr(running_instance, 'shape_config', None)
    local_nvme = _oci_local_nvme_storage_summary(instance_shape_config)
    record_resource(
        job,
        'local_nvme_supported',
        local_nvme['local_nvme_supported'],
    )
    record_resource(
        job,
        'local_nvme_disk_count',
        local_nvme['local_nvme_disk_count'],
    )
    record_resource(
        job,
        'local_nvme_total_size_gb',
        local_nvme['local_nvme_total_size_gb'],
    )
    record_resource(
        job,
        'local_nvme_disk_size_gb',
        local_nvme['local_nvme_disk_size_gb'],
    )
    local_nvme_description = str(
        getattr(instance_shape_config, 'local_disk_description', '') or ''
    ).strip()
    if local_nvme_description:
        record_resource(
            job,
            'local_nvme_description',
            local_nvme_description,
        )
    vnic_attachment = compute.list_vnic_attachments(
        compartment,
        instance_id=instance.id,
    ).data[0]
    runner_vnic = network.get_vnic(vnic_attachment.vnic_id).data
    pub = runner_vnic.public_ip
    record_resource(job, 'public_ip', pub)
    record_resource(job, 'private_ip', runner_vnic.private_ip)
    if plan.storage.additional_volume:
        data_volume_device = _oci_available_volume_device(
            compute,
            instance.id,
        )
        record_resource(job, 'oci_data_volume_device', data_volume_device)
        vol = storage.create_volume(oci.core.models.CreateVolumeDetails(compartment_id=compartment, availability_domain=ad, size_in_gbs=plan.storage.additional_size_gb, vpus_per_gb=plan.storage.additional_performance, display_name=f'benchmark-data-{suffix}', freeform_tags=tags)).data; record_resource(job, 'volume_id', vol.id)
        oci.wait_until(storage, storage.get_volume(vol.id), 'lifecycle_state', 'AVAILABLE')
        if plan.storage.mount_style == 'iscsi':
            attach_details = oci.core.models.AttachIScsiVolumeDetails(
                type='iscsi',
                instance_id=instance.id,
                volume_id=vol.id,
                device=data_volume_device,
                is_shareable=False,
                use_chap=False,
                is_agent_auto_iscsi_login_enabled=True,
            )
        else:
            attach_details = oci.core.models.AttachParavirtualizedVolumeDetails(
                type='paravirtualized',
                instance_id=instance.id,
                volume_id=vol.id,
                device=data_volume_device,
                is_shareable=False,
            )
        attachment = compute.attach_volume(attach_details).data
        record_resource(job, 'volume_attachment_id', attachment.id)
        event(job, 'Provision', 'Waiting for the /data volume attachment.')
        oci.wait_until(
            compute,
            compute.get_volume_attachment(attachment.id),
            'lifecycle_state',
            'ATTACHED',
        )
        attached_volume = compute.get_volume_attachment(attachment.id).data
        attached_device = str(
            getattr(attached_volume, 'device', '')
            or getattr(attachment, 'device', '')
            or ''
        ).strip()
        if attached_device and attached_device != data_volume_device:
            raise RuntimeError(
                'OCI attached the /data Block Volume at a device path other '
                'than the recorded request; refusing guest disk discovery.'
            )
    if uses_load_generator:
        loadgen_shape = load_generator_shape(available_shapes)
        loadgen_image = latest_oracle_linux_image(
            compute,
            compartment,
            loadgen_shape.shape,
            require_ol9=True,
        )
        loadgen_shape_config = oci.core.models.LaunchInstanceShapeConfigDetails(
            ocpus=2,
            memory_in_gbs=8,
        )
        loadgen_launch = oci.core.models.LaunchInstanceDetails(
            compartment_id=compartment,
            availability_domain=ad,
            shape=loadgen_shape.shape,
            shape_config=loadgen_shape_config,
            display_name=f'benchmark-loadgen-{suffix}',
            metadata={'ssh_authorized_keys': job['_public_key']},
            source_details=oci.core.models.InstanceSourceViaImageDetails(
                source_type='image',
                image_id=loadgen_image.id,
                boot_volume_size_in_gbs=50,
            ),
            create_vnic_details=oci.core.models.CreateVnicDetails(
                subnet_id=subnet.id,
                assign_public_ip=True,
                assign_private_dns_record=True,
                hostname_label='loadgen',
            ),
            freeform_tags=tags,
        )
        loadgen = compute.launch_instance(loadgen_launch).data
        record_resource(job, 'loadgen_instance_id', loadgen.id)
        record_resource(job, 'loadgen_shape', loadgen_shape.shape)
        record_resource(job, 'loadgen_network_bandwidth_gbps',
            configured_network_bandwidth_gbps(loadgen_shape, 2)
        )
        event(
            job,
            'Provision',
            f'Launched separate {loadgen_shape.shape} load generator with '
            '2 OCPUs and 8 GB for the selected web benchmark(s); waiting for '
            'it to become RUNNING.',
        )
        oci.wait_until(
            compute,
            compute.get_instance(loadgen.id),
            'lifecycle_state',
            'RUNNING',
        )
        loadgen_attachment = compute.list_vnic_attachments(
            compartment,
            instance_id=loadgen.id,
        ).data[0]
        loadgen_vnic = network.get_vnic(loadgen_attachment.vnic_id).data
        record_resource(job, 'loadgen_public_ip', loadgen_vnic.public_ip)
        record_resource(job, 'loadgen_private_ip', loadgen_vnic.private_ip)

    if iperf3_protocols:
        event(
            job,
            'Provision',
            'Creating a NAT-backed private peer for the selected iperf3 '
            f'protocols: {", ".join(protocol.upper() for protocol in iperf3_protocols)}.',
        )
        peer_rt = network.create_route_table(oci.core.models.CreateRouteTableDetails(compartment_id=compartment, vcn_id=vcn.id, display_name=f'benchmark-peer-routes-{suffix}', route_rules=[oci.core.models.RouteRule(destination='0.0.0.0/0', destination_type='CIDR_BLOCK', network_entity_id=nat.id)], freeform_tags=tags)).data
        record_resource(job, 'peer_route_table_id', peer_rt.id)
        peer_subnet = network.create_subnet(oci.core.models.CreateSubnetDetails(compartment_id=compartment, vcn_id=vcn.id, cidr_block='10.42.2.0/24', dns_label='peer', display_name=f'benchmark-peer-{suffix}', route_table_id=peer_rt.id, dhcp_options_id=vcn.default_dhcp_options_id, security_list_ids=[sl.id], prohibit_public_ip_on_vnic=True, freeform_tags=tags)).data
        record_resource(job, 'peer_subnet_id', peer_subnet.id)
        peer_image = latest_oracle_linux_image(
            compute,
            compartment,
            'VM.Standard.E5.Flex',
            require_ol9=True,
        )
        peer_script = iperf_peer_cloud_init(iperf3_protocols)
        peer_launch = oci.core.models.LaunchInstanceDetails(compartment_id=compartment, availability_domain=ad, shape='VM.Standard.E5.Flex', shape_config=oci.core.models.LaunchInstanceShapeConfigDetails(ocpus=1, memory_in_gbs=8), display_name=f'benchmark-peer-{suffix}', metadata={'user_data': base64.b64encode(peer_script.encode()).decode()}, source_details=oci.core.models.InstanceSourceViaImageDetails(source_type='image', image_id=peer_image.id, boot_volume_size_in_gbs=50), create_vnic_details=oci.core.models.CreateVnicDetails(subnet_id=peer_subnet.id, assign_public_ip=False, assign_private_dns_record=True, hostname_label='peer'), freeform_tags=tags)
        peer = compute.launch_instance(peer_launch).data; record_resource(job, 'peer_instance_id', peer.id)
        oci.wait_until(compute, compute.get_instance(peer.id), 'lifecycle_state', 'RUNNING')
        peer_attachment = compute.list_vnic_attachments(compartment, instance_id=peer.id).data[0]
        record_resource(job, 'peer_private_ip', network.get_vnic(peer_attachment.vnic_id).data.private_ip)
    event(job, 'Provision', f'Infrastructure ready. Public IP: {pub}')

def terminate_process_and_collect(process, grace_seconds=2):
    signal_process_termination(process)
    try:
        return process.communicate(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        try:
            if os.name == 'posix':
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except (ProcessLookupError, OSError):
            pass
        return process.communicate()
    except (OSError, ValueError):
        # If a local pipe was unexpectedly closed, output recovery is no
        # longer possible, but the producer still must be reaped before cloud
        # cleanup can begin.
        try:
            process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            try:
                if os.name == 'posix':
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except (ProcessLookupError, OSError):
                pass
            process.wait()
        return '', ''


def command_output(stdout, stderr):
    return '\n'.join(
        part.strip()
        for part in (stdout, stderr)
        if part and part.strip()
    )


def ssh(
    job,
    command,
    timeout=1800,
    host_key='public_ip',
    include_stderr=False,
    transport_attempts=12,
    transport_retry_delay_seconds=10,
    secret_stdin=None,
    stdin_text=None,
    jump_host_key=None,
    sensitive_output=False,
):
    """Run one SSH command, optionally supplying bounded standard input.

    ``secret_stdin`` is written only to the SSH process pipe. It is never
    added to argv, the environment, an event, or an exception message.
    ``stdin_text`` provides the same argv-safe transport for larger,
    non-secret declarative payloads such as a Kubernetes manifest. The two
    inputs are mutually exclusive so a caller cannot accidentally weaken the
    smaller secret boundary.
    ``sensitive_output`` prevents stdout and stderr from being copied into an
    exception when the remote command itself returns a secret.
    """
    transport_attempts = int(transport_attempts)
    transport_retry_delay_seconds = float(transport_retry_delay_seconds)
    if transport_attempts < 1:
        raise ValueError('SSH transport attempts must be at least one.')
    if transport_retry_delay_seconds < 0:
        raise ValueError('SSH transport retry delay cannot be negative.')
    if not isinstance(sensitive_output, bool):
        raise ValueError('SSH sensitive_output must be a boolean.')
    if secret_stdin is not None and stdin_text is not None:
        raise ValueError(
            'SSH secret_stdin and stdin_text are mutually exclusive.'
        )
    if secret_stdin is not None:
        if (
            not isinstance(secret_stdin, str)
            or not secret_stdin
            or '\x00' in secret_stdin
            or len(secret_stdin.encode('utf-8')) > 4096
        ):
            raise ValueError(
                'SSH secret standard input must be non-empty UTF-8 text of '
                'at most 4096 bytes without NUL characters.'
            )
    if stdin_text is not None:
        if (
            not isinstance(stdin_text, str)
            or not stdin_text
            or '\x00' in stdin_text
            or len(stdin_text.encode('utf-8')) > 1024 * 1024
        ):
            raise ValueError(
                'SSH non-secret standard input must be non-empty UTF-8 text '
                'of at most 1 MiB without NUL characters.'
            )
    has_standard_input = secret_stdin is not None or stdin_text is not None
    resources = job.get('resources', {})
    target = resources.get(host_key)
    if not target:
        raise RuntimeError(f'SSH target {host_key!r} is not available for this run.')
    jump_target = None
    if jump_host_key is not None:
        if not isinstance(jump_host_key, str) or not jump_host_key:
            raise ValueError('SSH jump_host_key must be a non-empty resource key.')
        jump_target = resources.get(jump_host_key)
        if not jump_target:
            raise RuntimeError(
                f'SSH jump target {jump_host_key!r} is not available for this run.'
            )
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write(job['_key'])
        path = f.name
    os.chmod(path, 0o600)
    askpass_path = None
    ssh_env = os.environ.copy()
    if job.get('_passphrase'):
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as askpass:
            askpass.write(
                '#!/bin/sh\n'
                'printf "%s\\n" "$OCI_BENCHMARK_SSH_PASSPHRASE"\n'
            )
            askpass_path = askpass.name
        os.chmod(askpass_path, 0o700)
        ssh_env.update({
            'DISPLAY': 'oci-benchmark:0',
            'SSH_ASKPASS': askpass_path,
            'SSH_ASKPASS_REQUIRE': 'force',
            'OCI_BENCHMARK_SSH_PASSPHRASE': job['_passphrase'],
        })
    ssh_user = job.get('resources', {}).get('ssh_user', 'opc')
    known_hosts_path = RUNS / str(job['id']) / 'known_hosts'
    known_hosts_path.parent.mkdir(parents=True, exist_ok=True)
    known_hosts_path.touch(exist_ok=True)
    os.chmod(known_hosts_path, 0o600)
    transport_options = [
        '-F', '/dev/null',
        '-o', 'ForwardAgent=no',
        '-o', 'ClearAllForwardings=yes',
        '-o', 'IdentityAgent=none',
        '-o', 'StrictHostKeyChecking=accept-new',
        '-o', f'UserKnownHostsFile={known_hosts_path}',
        '-o', 'GlobalKnownHostsFile=/dev/null',
        '-o', 'ConnectTimeout=15',
        '-o', 'ServerAliveInterval=30',
        '-o', 'ServerAliveCountMax=6',
        '-o', 'NumberOfPasswordPrompts=1',
        '-o', 'PasswordAuthentication=no',
        '-o', 'KbdInteractiveAuthentication=no',
        '-o', 'PreferredAuthentications=publickey',
        '-o', 'IdentitiesOnly=yes',
        '-i', path,
    ]
    args = ['ssh', *transport_options]
    if jump_target is not None:
        # Keep the private key on the orchestrator.  The local proxy SSH uses
        # the same isolated identity and run-owned host-key database as the
        # destination connection, then forwards only the SSH byte stream to
        # the manifest-selected private address.
        proxy_args = [
            'ssh',
            *transport_options,
            '-W', '%h:%p',
            f'{ssh_user}@{jump_target}',
        ]
        args.extend(('-o', f'ProxyCommand={shlex.join(proxy_args)}'))
    args.extend((
        f'{ssh_user}@{target}',
        command,
    ))
    try:
        for attempt in range(transport_attempts):
            raise_if_cancelled(job)
            process = subprocess.Popen(
                args,
                stdin=(
                    subprocess.PIPE
                    if has_standard_input
                    else subprocess.DEVNULL
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                env=ssh_env,
                start_new_session=os.name == 'posix',
            )
            register_job_process(job, process)
            if secret_stdin is not None:
                try:
                    process.stdin.write(secret_stdin)
                    process.stdin.close()
                    # ``communicate`` must not try to flush an already closed
                    # pipe during the cancellable polling loop below.
                    process.stdin = None
                except Exception:
                    if process.poll() is None:
                        terminate_process_and_collect(process)
                    unregister_job_process(job, process)
                    raise
            deadline = time.monotonic() + timeout
            stdout = ''
            stderr = ''
            communicate_input = stdin_text
            try:
                while True:
                    if cancellation_requested(job):
                        stdout, stderr = terminate_process_and_collect(process)
                        raise RunCancelled(
                            'The benchmark run was stopped by the user.'
                        )
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        stdout, stderr = terminate_process_and_collect(process)
                        raise SSHCommandError(
                            f'SSH command timed out after {timeout} seconds.',
                            output=(
                                '' if sensitive_output
                                else command_output(stdout, stderr)
                            ),
                        )
                    try:
                        communicate_kwargs = {
                            'timeout': min(0.25, remaining),
                        }
                        if communicate_input is not None:
                            communicate_kwargs['input'] = communicate_input
                        stdout, stderr = process.communicate(
                            **communicate_kwargs
                        )
                        communicate_input = None
                        break
                    except subprocess.TimeoutExpired:
                        # ``Popen.communicate`` retains its internal write
                        # buffer after a timeout. Subsequent calls must omit
                        # ``input`` while it finishes streaming the same
                        # bounded payload.
                        communicate_input = None
                        continue
            finally:
                try:
                    if process.poll() is None:
                        # Never let an unexpected local pipe/communicate
                        # failure leave SSH (and its remote producer) running
                        # while the supervisor moves on to cloud cleanup.
                        terminate_process_and_collect(process)
                finally:
                    unregister_job_process(job, process)
            raise_if_cancelled(job)
            if process.returncode == 0:
                if include_stderr:
                    return command_output(stdout, stderr)
                return stdout
            if (
                process.returncode != 255
                or attempt == transport_attempts - 1
            ):
                output = command_output(stdout, stderr)
                if sensitive_output:
                    raise SSHCommandError(
                        'SSH command failed with exit status '
                        f'{process.returncode}; remote output was redacted.',
                        returncode=process.returncode,
                    )
                raise SSHCommandError(
                    f'SSH command failed with exit status '
                    f'{process.returncode}: {output[-2000:]}',
                    output=output,
                    returncode=process.returncode,
                )
            cancel = cancellation_event(job)
            if cancel is None:
                time.sleep(transport_retry_delay_seconds)
            elif cancel.wait(transport_retry_delay_seconds):
                raise RunCancelled('The benchmark run was stopped by the user.')
        raise RuntimeError('SSH connection failed.')
    finally:
        os.unlink(path)
        if askpass_path:
            os.unlink(askpass_path)


def wait_for_ssh_transport(
    job,
    *,
    attempts=24,
    retry_delay_seconds=5,
):
    """Wait for first-boot SSH without rerunning a remote setup command."""
    attempts = int(attempts)
    retry_delay_seconds = float(retry_delay_seconds)
    if attempts < 1:
        raise ValueError('SSH readiness attempts must be at least one.')
    if retry_delay_seconds < 0:
        raise ValueError('SSH readiness retry delay cannot be negative.')
    event(
        job,
        'Connect',
        'Waiting for the guest SSH service and metadata-backed user key.',
    )
    last_error = None
    for attempt in range(1, attempts + 1):
        raise_if_cancelled(job)
        try:
            ssh(
                job,
                'true',
                timeout=30,
                transport_attempts=1,
                transport_retry_delay_seconds=0,
            )
            event(job, 'Connect', 'Guest SSH transport is ready.')
            return
        except SSHCommandError as exc:
            # OpenSSH uses 255 for connection/authentication transport errors.
            # A timeout of this no-op command is likewise a connection-class
            # failure. Any real remote exit status fails immediately.
            if exc.returncode not in {None, 255}:
                raise
            last_error = exc
        if attempt < attempts:
            event(
                job,
                'Connect',
                f'Guest SSH is not ready; retrying in '
                f'{retry_delay_seconds:g} seconds ({attempt}/{attempts}).',
            )
            cancel = cancellation_event(job)
            if cancel is None:
                time.sleep(retry_delay_seconds)
            elif cancel.wait(retry_delay_seconds):
                raise RunCancelled(
                    'The benchmark run was stopped by the user.'
                )
    raise RuntimeError(
        f'Guest SSH did not become ready after {attempts} bounded attempts. '
        f'{last_error or "Connection failed."}'
    ) from last_error

def wait_for_guest_readiness(
    job,
    region,
    host_key='public_ip',
    label='benchmark instance',
    required_hosts=None,
):
    event(
        job,
        'Connect',
        f'Waiting for cloud-init and OCI DNS resolution on the {label}.',
    )
    regional_yum = f'yum.{region}.oci.oraclecloud.com'
    required_hosts = required_hosts or [
        regional_yum,
        'github.com',
        'openbenchmarking.org',
    ]
    if any(not re.fullmatch(r'[A-Za-z0-9.-]+', host) for host in required_hosts):
        raise ValueError('A guest-readiness DNS hostname is invalid.')
    required_host_list = ' '.join(required_hosts)
    command = (
        'if command -v cloud-init >/dev/null 2>&1; then '
        'CLOUD_INIT_OUTPUT=$(sudo timeout 120 cloud-init status --wait 2>&1); '
        'CLOUD_INIT_RC=$?; '
        'echo "$CLOUD_INIT_OUTPUT"; '
        'if [ "$CLOUD_INIT_RC" -eq 124 ]; then '
        'echo "Warning: cloud-init was still running after 120 seconds; '
        'continuing with network readiness checks."; '
        'elif [ "$CLOUD_INIT_RC" -ne 0 ]; then '
        'echo "Warning: cloud-init reported a non-success status; '
        'continuing with network readiness checks."; '
        'fi; '
        'else echo "Warning: cloud-init is not installed."; fi; '
        f'REQUIRED_HOSTS="{required_host_list}"; '
        'for attempt in $(seq 1 24); do '
        'MISSING_HOSTS=""; '
        'for host in $REQUIRED_HOSTS; do '
        'getent ahostsv4 "$host" >/dev/null 2>&1 '
        '|| MISSING_HOSTS="$MISSING_HOSTS $host"; '
        'done; '
        'if [ -z "$MISSING_HOSTS" ]; then '
        'echo "DNS resolved all required benchmark hosts."; exit 0; fi; '
        'echo "Waiting for DNS:$MISSING_HOSTS ($attempt/24)."; '
        'sleep 5; '
        'done; '
        'echo "DNS did not resolve required benchmark hosts after '
        '120 seconds:$MISSING_HOSTS" >&2; '
        'echo "--- /etc/resolv.conf ---" >&2; '
        'cat /etc/resolv.conf >&2; '
        'echo "--- NSS hosts configuration ---" >&2; '
        'grep "^hosts:" /etc/nsswitch.conf >&2 || true; '
        'echo "--- routes ---" >&2; '
        'ip route >&2; '
        'exit 1'
    )
    output = ssh(job, command, timeout=300, host_key=host_key)
    event(
        job,
        'Connect',
        f'{label.capitalize()} readiness passed: {output.strip()[-1000:]}',
    )


def iperf_peer_readiness_command(
    peer_private_ip,
    protocols=('tcp',),
    *,
    verify_tcp_udp=False,
):
    peer_private_ip = str(ip_address(peer_private_ip))
    protocols = tuple(dict.fromkeys(protocols))
    unsupported = set(protocols) - {'tcp', 'udp', 'sctp'}
    if unsupported:
        raise ValueError(
            f'Unsupported iperf3 readiness protocol: {sorted(unsupported)[0]}'
        )
    data_probes = ''
    if verify_tcp_udp:
        probe_commands = {
            'tcp': 'iperf3 -4 -c "$PEER_IP" -t 1 -J',
            'udp': 'iperf3 -4 -c "$PEER_IP" -u -b 10M -t 1 -J',
        }
        probes = []
        for protocol in protocols:
            command = probe_commands.get(protocol)
            if not command:
                continue
            label = protocol.upper()
            path = f'/tmp/iperf3-{protocol}-readiness.json'
            probes.append(
                f'{label}_READY=false; for attempt in $(seq 1 6); do '
                f'if timeout 20 {command} >{path} 2>&1; then '
                f'{label}_READY=true; break; fi; '
                f'if [ "$attempt" -lt 6 ]; then sleep 2; fi; done; '
                f'if [ "${label}_READY" != true ]; then '
                f'echo "The private peer did not complete an iperf3 {label} '
                f'readiness test." >&2; cat {path} >&2 || true; exit 1; fi; '
                f'rm -f {path}; echo "iperf3 {label} data path is ready."; '
            )
        data_probes = ''.join(probes)
    sctp_probe = ''
    if 'sctp' in protocols:
        sctp_probe = (
            'for attempt in $(seq 1 6); do '
            'if timeout 20 iperf3 -c "$PEER_IP" --sctp -t 1 -J '
            '>/tmp/iperf3-sctp-readiness.json 2>&1; then '
            'echo "iperf3 SCTP data path is ready."; '
            'rm -f /tmp/iperf3-sctp-readiness.json; exit 0; fi; '
            'if [ "$attempt" -lt 6 ]; then sleep 2; fi; done; '
            'echo "The private peer did not complete an SCTP readiness test." '
            '>&2; cat /tmp/iperf3-sctp-readiness.json >&2 || true; exit 1'
        )
    return (
        f'PEER_IP={peer_private_ip}; '
        'CONTROL_READY=false; '
        'for attempt in $(seq 1 60); do '
        'if timeout 2 bash -c '
        '"exec 3<>/dev/tcp/$PEER_IP/5201" 2>/dev/null; then '
        'CONTROL_READY=true; break; fi; '
        'echo "Waiting for iperf3 peer ($attempt/60)."; '
        'if [ "$attempt" -lt 60 ]; then sleep 5; fi; '
        'done; '
        'if [ "$CONTROL_READY" != true ]; then '
        'echo "The private iperf3 peer did not open TCP control port 5201 '
        'after 60 bounded attempts." >&2; exit 1; fi; '
        'echo "iperf3 TCP control connection at $PEER_IP:5201 is ready."; '
        f'{data_probes}{sctp_probe}'
    )


def wait_for_iperf_peer(
    job,
    protocols=('tcp',),
    *,
    verify_tcp_udp=False,
):
    peer_private_ip = job.get('resources', {}).get('peer_private_ip')
    if not peer_private_ip:
        raise RuntimeError(
            'The selected network benchmark has no private peer address.'
        )
    event(
        job,
        'Network',
        f'Waiting for the iperf3 peer at {peer_private_ip}:5201.',
    )
    output = ssh(
        job,
        iperf_peer_readiness_command(
            peer_private_ip,
            protocols,
            verify_tcp_udp=verify_tcp_udp,
        ),
        timeout=IPERF_PEER_READINESS_TIMEOUT_SECONDS,
    )
    event(job, 'Network', output.strip())

def dnf_install(job, packages, host_key='public_ip', timeout=1800):
    package_list = ' '.join(sorted(set(packages)))
    ssh(
        job,
        'for attempt in 1 2 3; do '
        'sudo dnf -y --disablerepo=ol9_ksplice '
        '--setopt=retries=10 --setopt=timeout=30 '
        f'install {package_list} && exit 0; '
        'sleep 10; '
        'done; exit 1',
        host_key=host_key,
        timeout=timeout,
    )


def enable_ol9_developer_epel(job, host_key='public_ip'):
    dnf_install(
        job,
        {'dnf-plugins-core', 'oracle-epel-release-el9'},
        host_key=host_key,
    )
    ssh(
        job,
        'sudo dnf config-manager --enable ol9_developer_EPEL',
        host_key=host_key,
    )


def wait_for_web_guest_readiness(
    job,
    plan,
    benchmark_id,
    *,
    role,
    host_key='public_ip',
    label='web benchmark instance',
):
    """Wait for one web guest without leaking provider logic into runners."""
    provider = web_guest.provider_id(plan)
    hosts = web_guest.readiness_hosts(
        provider,
        benchmark_id,
        role,
        region=getattr(plan, 'region', None),
    )
    if provider == 'oci':
        wait_for_guest_readiness(
            job,
            plan.region,
            host_key=host_key,
            label=label,
            required_hosts=hosts,
        )
        return

    resources = job.get('resources', {})
    architecture = (
        resources.get('architecture')
        if role == 'service'
        else resources.get('loadgen_architecture', 'x86_64')
    )
    event(
        job,
        'Connect',
        f'Waiting for {label} DNS and package repositories on '
        f'{provider.upper()}.',
    )
    output = ssh(
        job,
        web_guest.readiness_command(
            provider,
            benchmark_id,
            role,
            region=getattr(plan, 'region', None),
            expected_architecture=architecture,
        ),
        host_key=host_key,
        timeout=600,
        include_stderr=True,
    )
    event(
        job,
        'Connect',
        f'{label.capitalize()} readiness passed: {output.strip()[-1000:]}',
    )


def install_apachebench_guest(job, plan, *, role, host_key='public_ip'):
    """Install ApacheBench target/client packages for the selected provider."""
    provider = web_guest.provider_id(plan)
    if provider == 'oci':
        packages = (
            {'firewalld', 'httpd'}
            if role == 'service'
            else {'httpd-tools', 'time'}
        )
        dnf_install(job, packages, host_key=host_key)
        return
    for step in web_guest.apachebench_install_steps(provider, role):
        event(job, 'ApacheBench install', f'Installing {step.name}.')
        output = ssh(
            job,
            step.command,
            host_key=host_key,
            timeout=step.timeout_seconds,
            include_stderr=True,
        )
        event(
            job,
            'ApacheBench install',
            f'{step.name} is ready: {output.strip()[-1000:]}',
        )


def install_deathstarbench_guest(job, plan, *, role, host_key='public_ip'):
    """Install Podman service or wrk2 load-generator prerequisites."""
    provider = web_guest.provider_id(plan)
    if provider == 'oci':
        if role == 'service':
            dnf_install(
                job,
                {
                    'container-tools',
                    'curl',
                    'git',
                    'python3',
                    'python3-pyyaml',
                },
                host_key=host_key,
            )
            enable_ol9_developer_epel(job, host_key=host_key)
            dnf_install(job, {'podman-compose'}, host_key=host_key)
            return
        enable_ol9_developer_epel(job, host_key=host_key)
        dnf_install(
            job,
            {
                'curl',
                'gcc',
                'git',
                'luarocks',
                'make',
                'openssl-devel',
                'python3',
                'python3-aiohttp',
                'time',
            },
            host_key=host_key,
        )
        return

    for step in web_guest.deathstarbench_install_steps(provider, role):
        event(job, 'DeathStarBench install', f'Installing {step.name}.')
        output = ssh(
            job,
            step.command,
            host_key=host_key,
            timeout=step.timeout_seconds,
            include_stderr=True,
        )
        event(
            job,
            'DeathStarBench install',
            f'{step.name} is ready: {output.strip()[-1000:]}',
        )

def install_benchmark_tools(job, plan, selected):
    packages = set()
    iperf3_protocols = selected_iperf3_protocols(plan)
    phoronix_profiles = (
        tuple(plan.phoronix.profiles)
        if 'phoronix' in selected
        else ()
    )
    sysbench_selected = bool(
        {'sysbench_cpu', 'sysbench_memory', 'sysbench_fileio'} & selected
    )
    if {'stream'} & selected:
        packages.update(amazon_linux.STREAM_PACKAGES)
    if {'fio'} & selected:
        packages.add('fio')
    if sysbench_selected:
        packages.update(amazon_linux.SYSBENCH_BUILD_PACKAGES)
    if iperf3_protocols:
        packages.add('iperf3')
    if 'sctp' in iperf3_protocols:
        packages.add('lksctp-tools')
    if phoronix_profiles:
        packages.update(phoronix_required_packages(phoronix_profiles))
    if plan.llm_benchmarks:
        packages.update(llama_cpp.REQUIRED_PACKAGES)
        packages.add(LLAMA_TOOLSET_PACKAGE)
    # Oracle Linux 9 images provide the curl command through curl-minimal.
    # Installing the full curl RPM can conflict with that stock provider, but
    # the minimal build supports every HTTPS option in the pinned workloads.
    packages.discard('curl')
    if packages:
        event(
            job,
            'Install',
            f'Installing selected benchmark prerequisites: '
            f'{", ".join(sorted(packages))}.',
        )
        dnf_install(job, packages)
    if 'sctp' in iperf3_protocols:
        event(
            job,
            'Install',
            'Installing SCTP modules for the running kernel when needed, '
            'then verifying the kernel and iperf3 SCTP support.',
        )
        output = ssh(
            job,
            sctp_kernel_support_command(use_sudo=True) + '; '
            'IPERF_HELP="$(iperf3 --help 2>&1)" && '
            'grep -q -- "--sctp" <<< "$IPERF_HELP" && '
            'echo "iperf3 SCTP support is ready."',
        )
        event(job, 'Install', output.strip())
    if plan.llm_benchmarks:
        event(job, 'Install', 'Verifying the GCC Toolset 12 compiler and assembler.')
        toolchain = ssh(
            job,
            llama_toolset_command(
                'set -eo pipefail; '
                'command -v gcc >/dev/null; '
                'command -v g++ >/dev/null; '
                'command -v as >/dev/null; '
                'printf "gcc: "; gcc --version | head -n 1; '
                'printf "g++: "; g++ --version | head -n 1; '
                'printf "assembler: "; as --version | head -n 1'
            ),
        )
        event(job, 'Install', f'LLM toolchain ready: {toolchain.strip()}')
    if sysbench_selected:
        event(
            job,
            'Install',
            f'Building checksum-pinned sysbench '
            f'{amazon_linux.SYSBENCH_VERSION}.',
        )
        output = ssh(
            job,
            amazon_linux.sysbench_install_command(),
            timeout=1800,
            include_stderr=True,
        )
        event(
            job,
            'Install',
            f'Pinned sysbench is ready: {output.strip()[-1000:]}',
        )

def aws_data_volume_mount_command(volume_id, hypervisor):
    """Resolve one exact EBS volume and mount it without touching local NVMe."""
    volume_id = str(volume_id or '')
    if not re.fullmatch(r'vol-[0-9a-f]+', volume_id):
        raise ValueError('The recorded AWS data volume ID is invalid.')
    hypervisor = str(hypervisor or '').strip().lower()
    if hypervisor not in {'nitro', 'xen'}:
        raise ValueError('The recorded AWS hypervisor must be nitro or xen.')
    return (
        'set -euo pipefail; '
        f'EXPECTED_VOLUME_ID={volume_id}; '
        f'HYPERVISOR={hypervisor}; '
        'EXPECTED_SERIAL="${EXPECTED_VOLUME_ID//-/}"; '
        'sudo mkdir -p /data; '
        'ROOT_SOURCE="$(findmnt -rn -o SOURCE /)"; '
        'ROOT_PARENT="$(lsblk -ndo PKNAME "$ROOT_SOURCE" 2>/dev/null '
        '|| true)"; '
        'if test -n "$ROOT_PARENT"; then '
        'ROOT_DEVICE="$(readlink -f "/dev/$ROOT_PARENT")"; else '
        'ROOT_DEVICE="$(readlink -f "$ROOT_SOURCE")"; fi; '
        'if findmnt -rn /data >/dev/null; then '
        'MOUNTED_SOURCE="$(findmnt -rn -o SOURCE /data)"; '
        'MOUNTED_DEVICE="$(readlink -f "$MOUNTED_SOURCE")"; '
        'MOUNTED_SERIAL="$(lsblk -dnro SERIAL "$MOUNTED_DEVICE" '
        '2>/dev/null '
        "| tr -d '[:space:]-' | tr '[:upper:]' '[:lower:]')\"; "
        'if test "$MOUNTED_SERIAL" = "$EXPECTED_SERIAL"; then '
        'findmnt /data; exit 0; fi; '
        'if test "$HYPERVISOR" = xen; then '
        'for alias in /dev/xvdf /dev/sdf; do '
        'test -b "$alias" || continue; '
        'device="$(readlink -f "$alias")"; '
        'if test "$MOUNTED_DEVICE" = "$device" '
        '&& test "$device" != "$ROOT_DEVICE" '
        '&& test "$(lsblk -dnro TYPE "$device")" = disk; then '
        'findmnt /data; exit 0; fi; done; fi; '
        'echo "/data is mounted from a device other than the recorded EBS '
        'volume; refusing to continue." >&2; exit 1; fi; '
        'DATA_DEVICE=""; '
        'for attempt in $(seq 1 36); do '
        'while IFS= read -r device; do '
        'serial="$(lsblk -dnro SERIAL "$device" 2>/dev/null '
        "| tr -d '[:space:]-' | tr '[:upper:]' '[:lower:]')\"; "
        'if test "$serial" = "$EXPECTED_SERIAL"; then '
        'DATA_DEVICE="$device"; break; fi; '
        "done < <(lsblk -dnpo NAME,TYPE | awk '$2==\"disk\" {print $1}'); "
        'if test -z "$DATA_DEVICE"; then '
        'for alias in '
        '"/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_${EXPECTED_SERIAL}"; do '
        'test -b "$alias" || continue; '
        'device="$(readlink -f "$alias")"; '
        'serial="$(lsblk -dnro SERIAL "$device" 2>/dev/null '
        "| tr -d '[:space:]-' | tr '[:upper:]' '[:lower:]')\"; "
        'if test "$serial" = "$EXPECTED_SERIAL"; then '
        'DATA_DEVICE="$device"; break; fi; done; fi; '
        'if test -z "$DATA_DEVICE" && test "$HYPERVISOR" = xen; then '
        'for alias in /dev/xvdf /dev/sdf; do '
        'test -b "$alias" || continue; '
        'device="$(readlink -f "$alias")"; '
        'test "$device" != "$ROOT_DEVICE" || continue; '
        'test "$(lsblk -dnro TYPE "$device")" = disk || continue; '
        'if ! lsblk -nro MOUNTPOINT "$device" '
        "| grep -q '[^[:space:]]'; then "
        'DATA_DEVICE="$device"; break; fi; done; fi; '
        'test -n "$DATA_DEVICE" && break; sleep 5; done; '
        'test -b "$DATA_DEVICE" || '
        '{ echo "Recorded EBS data volume did not appear in the guest; '
        'refusing to select an arbitrary local disk." >&2; exit 1; }; '
        'if lsblk -nro MOUNTPOINT "$DATA_DEVICE" '
        "| grep -q '[^[:space:]]'; then "
        'echo "Recorded EBS data volume is already mounted somewhere other '
        'than /data; refusing to format it." >&2; exit 1; fi; '
        'echo "Using EBS data device $DATA_DEVICE for $EXPECTED_VOLUME_ID"; '
        'FSTYPE="$(lsblk -dnro FSTYPE "$DATA_DEVICE" | head -n 1)"; '
        'if test -z "$FSTYPE"; then sudo mkfs.xfs -f "$DATA_DEVICE"; fi; '
        'sudo mount "$DATA_DEVICE" /data; '
        'sudo chmod 0777 /data; '
        'FSTYPE="$(findmnt -rn -o FSTYPE /data)"; '
        'FILESYSTEM_UUID="$(sudo blkid -s UUID -o value "$DATA_DEVICE")"; '
        'test -n "$FILESYSTEM_UUID"; '
        'if ! grep -Fq "UUID=$FILESYSTEM_UUID /data " /etc/fstab; then '
        'printf "UUID=%s /data %s defaults,nofail 0 2\\n" '
        '"$FILESYSTEM_UUID" "$FSTYPE" | sudo tee -a /etc/fstab >/dev/null; '
        'fi; findmnt /data'
    )


def oci_data_volume_mount_command(device):
    """Mount the exact OCI Block Volume device reported by its attachment."""

    device = str(device or '').strip()
    if (
        not re.fullmatch(r'/dev/(?:[A-Za-z0-9._+-]+/)*[A-Za-z0-9._+-]+', device)
        or '..' in device.split('/')
    ):
        raise ValueError('The recorded OCI data-volume device path is invalid.')
    expected_device = shlex.quote(device)
    return (
        'set -euo pipefail; '
        f'EXPECTED_DEVICE={expected_device}; '
        'sudo mkdir -p /data; '
        'DATA_DEVICE=""; '
        'for attempt in $(seq 1 36); do '
        'if test -b "$EXPECTED_DEVICE"; then '
        'DATA_DEVICE="$(readlink -f "$EXPECTED_DEVICE")"; break; fi; '
        'sleep 5; done; '
        'test -b "$DATA_DEVICE" || '
        '{ echo "The recorded OCI Block Volume device did not appear in the '
        'guest." >&2; exit 1; }; '
        'test "$(lsblk -dnro TYPE "$DATA_DEVICE")" = disk || '
        '{ echo "The recorded OCI data-volume path is not a whole disk." '
        '>&2; exit 1; }; '
        'ROOT_SOURCE="$(findmnt -rn -o SOURCE /)"; '
        'if lsblk -srnpo NAME "$ROOT_SOURCE" 2>/dev/null '
        '| grep -Fxq "$DATA_DEVICE"; then '
        'echo "The recorded OCI data volume resolves to the root disk; '
        'refusing to continue." >&2; exit 1; fi; '
        'if findmnt -rn /data >/dev/null; then '
        'MOUNTED_SOURCE="$(findmnt -rn -o SOURCE /data)"; '
        'MOUNTED_DEVICE="$(readlink -f "$MOUNTED_SOURCE")"; '
        'if test "$MOUNTED_DEVICE" = "$DATA_DEVICE"; then '
        'findmnt /data; exit 0; fi; '
        'echo "/data is mounted from a device other than the recorded OCI '
        'Block Volume; refusing to continue." >&2; exit 1; fi; '
        'if lsblk -nro MOUNTPOINT "$DATA_DEVICE" '
        "| grep -q '[^[:space:]]'; then "
        'echo "The recorded OCI Block Volume is already mounted somewhere '
        'other than /data; refusing to format it." >&2; exit 1; fi; '
        'CHILD_COUNT="$(lsblk -nrpo NAME "$DATA_DEVICE" | tail -n +2 '
        '| grep -c . || true)"; '
        'test "$CHILD_COUNT" -eq 0 || '
        '{ echo "The recorded OCI Block Volume already has partitions; '
        'refusing to format it." >&2; exit 1; }; '
        'FSTYPE="$(lsblk -dnro FSTYPE "$DATA_DEVICE" | head -n 1)"; '
        'if test -z "$FSTYPE"; then sudo mkfs.xfs -f "$DATA_DEVICE"; '
        'elif test "$FSTYPE" != xfs; then '
        'echo "The recorded OCI Block Volume has an unexpected filesystem; '
        'refusing to mount it." >&2; exit 1; fi; '
        'sudo mount "$DATA_DEVICE" /data; '
        'sudo chmod 0777 /data; '
        'FILESYSTEM_UUID="$(sudo blkid -s UUID -o value "$DATA_DEVICE")"; '
        'test -n "$FILESYSTEM_UUID"; '
        'if ! grep -Fq "UUID=$FILESYSTEM_UUID /data " /etc/fstab; then '
        'printf "UUID=%s /data xfs defaults,nofail 0 2\\n" '
        '"$FILESYSTEM_UUID" | sudo tee -a /etc/fstab >/dev/null; fi; '
        'findmnt /data'
    )


def mount_data_volume(job):
    event(job, 'Storage', 'Discovering, formatting, and mounting the data volume.')
    resources = job.get('resources', {})
    aws_volume_id = resources.get('aws_data_volume_id')
    if resources.get('provider') == 'aws':
        if not aws_volume_id:
            raise RuntimeError(
                'The AWS /data volume ID is missing from the persisted run '
                'manifest; refusing to inspect or format guest disks.'
            )
        command = aws_data_volume_mount_command(
            aws_volume_id,
            resources.get('hypervisor'),
        )
    elif resources.get('provider') == 'gcp':
        device_name = resources.get('gcp_data_device_name')
        if not device_name:
            raise RuntimeError(
                'The GCP /data disk device name is missing from the persisted '
                'run manifest; refusing to inspect or format guest disks.'
            )
        command = rocky_linux.data_volume_mount_command(
            device_name,
            owner=resources.get('ssh_user') or rocky_linux.SSH_USER,
        )
    elif resources.get('provider') == 'azure':
        lun = resources.get('azure_data_disk_lun')
        if lun is None:
            raise RuntimeError(
                'The Azure /data disk LUN is missing from the persisted run '
                'manifest; refusing to inspect or format guest disks.'
            )
        command = rocky_linux.azure_data_volume_mount_command(
            lun=lun,
            owner=resources.get('ssh_user') or rocky_linux.SSH_USER,
        )
    else:
        device = resources.get('oci_data_volume_device')
        if not device:
            raise RuntimeError(
                'The OCI /data Block Volume device is missing from the '
                'persisted run manifest; refusing to inspect or format '
                'arbitrary guest disks.'
            )
        command = oci_data_volume_mount_command(device)
    output = ssh(job, command, timeout=300)
    event(job, 'Storage', f'Data volume mounted successfully: {output.strip()}')
    return output

def _verified_local_nvme_profile(resources):
    """Return a control-plane-attested local-NVMe profile or ``None``."""

    if resources.get('local_nvme_supported') is not True:
        return None
    count = resources.get('local_nvme_disk_count')
    total_size_gb = resources.get('local_nvme_total_size_gb')
    if (
        isinstance(count, bool)
        or not isinstance(count, int)
        or count <= 0
        or isinstance(total_size_gb, bool)
        or not isinstance(total_size_gb, (int, float))
        or not math.isfinite(float(total_size_gb))
        or float(total_size_gb) <= 0
    ):
        return None
    return count, float(total_size_gb)


def prepare_storage_benchmark_target(job, plan, selected):
    """Prefer one verified local NVMe disk, with exact ``/data`` fallback."""

    if not STORAGE_BENCHMARK_RESULT_IDS.intersection(selected):
        return '/data', None

    resources = job.get('resources', {})
    provider = str(getattr(plan, 'provider', 'oci') or 'oci').lower()
    local_profile = _verified_local_nvme_profile(resources)
    if local_profile is not None:
        expected_count, expected_total_size_gb = local_profile
        event(
            job,
            'Storage',
            f'Verifying {expected_count} provider-reported instance-local '
            f'NVMe device{"s" if expected_count != 1 else ""} before '
            'selecting benchmark storage.',
        )
        output = ssh(
            job,
            storage_target.local_nvme_prepare_command(
                provider,
                expected_count,
                expected_total_size_gb,
            ),
            timeout=300,
            transport_attempts=1,
        )
        (
            selected_target,
            fallback_reason,
        ) = storage_target.parse_local_nvme_prepare_result(
            output,
        )
        if selected_target is not None:
            record_resource(
                job,
                'benchmark_storage_target',
                selected_target,
            )
            capacity_gib = (
                selected_target['storage_target_capacity_bytes'] / 1024**3
            )
            event(
                job,
                'Storage',
                'Selected verified instance-local NVMe at '
                f'{selected_target["storage_target_mount_point"]} '
                f'({capacity_gib:.2f} GiB, '
                f'{selected_target["storage_target_model"]}). The '
                'provisioned /data volume remains attached as an unused '
                'fallback and will be cleaned up with the run.',
            )
            return (
                selected_target['storage_target_mount_point'],
                selected_target,
            )
        event(
            job,
            'Storage',
            'Provider-reported local NVMe did not pass the fail-closed '
            'guest identity and blank-device checks '
            f'(guest_reason={fallback_reason}); using the exact '
            'provisioned /data volume instead.',
        )

    if not plan.storage.additional_volume:
        raise RuntimeError(
            'No verified instance-local NVMe target was available and the '
            'plan has no additional /data fallback volume.'
        )
    mount_data_volume(job)
    observation = ssh(
        job,
        storage_target.mounted_storage_descriptor_command('/data'),
        timeout=60,
    )
    selected_target = (
        storage_target.parse_mounted_storage_descriptor_output(observation)
    )
    record_resource(job, 'benchmark_storage_target', selected_target)
    capacity_gib = selected_target['storage_target_capacity_bytes'] / 1024**3
    event(
        job,
        'Storage',
        'Selected the manifest-bound /data fallback volume '
        f'({capacity_gib:.2f} GiB, '
        f'{selected_target["storage_target_model"]}).',
    )
    return selected_target['storage_target_mount_point'], selected_target


def execute_benchmark(
    job,
    benchmark_id,
    name,
    command,
    timeout=1800,
    host_key='public_ip',
    parser=None,
    metadata=None,
    output_limit=20000,
    include_stderr=False,
    transport_attempts=None,
):
    started_at = now()
    started = time.monotonic()
    event(job, 'Run', f'Started {name}.')

    def record_failure(error, output=''):
        duration = round(time.monotonic() - started, 2)
        job['results'].append({
            'id': benchmark_id,
            'name': name,
            'command': command,
            'started_at': started_at,
            'duration_seconds': duration,
            'status': 'failed',
            'error': str(error),
            'output': (
                output
                if output_limit is None
                else output[-output_limit:]
            ),
            **({'metadata': metadata} if metadata else {}),
        })

    try:
        ssh_options = {}
        if transport_attempts is not None:
            ssh_options['transport_attempts'] = transport_attempts
        output = ssh(
            job,
            command,
            timeout=timeout,
            host_key=host_key,
            include_stderr=include_stderr,
            **ssh_options,
        )
    except Exception as exc:
        failure_output = (
            exc.output
            if isinstance(exc, SSHCommandError) and exc.output
            else ''
        )
        record_failure(exc, failure_output)
        raise RuntimeError(f'{name} failed. {exc}') from exc
    duration = round(time.monotonic() - started, 2)
    if not output.strip():
        error = RuntimeError(
            f'{name} exited without producing benchmark output.'
        )
        record_failure(error)
        raise error
    try:
        metrics = parser(output) if parser else None
    except Exception as exc:
        record_failure(exc, output)
        raise RuntimeError(f'{name} produced an invalid result. {exc}') from exc
    result = {
        'id': benchmark_id,
        'name': name,
        'command': command,
        'started_at': started_at,
        'duration_seconds': duration,
        'status': 'completed',
        'output': output if output_limit is None else output[-output_limit:],
    }
    if metadata:
        result['metadata'] = metadata
    if metrics:
        result['metrics'] = metrics
    job['results'].append(result)
    event(
        job,
        'Run complete',
        f'Completed {name} in {duration:.2f}s and captured '
        f'{len(output.encode())} bytes of output.',
    )


def run_llama_benchmark(job, *, metadata=None, toolset_enable=None):
    """Probe CPU and toolchain provenance, then run the llama.cpp contract."""

    benchmark_id = 'llama_bench'
    name = 'llama.cpp throughput (CPU)'
    probe_command = llama_cpp.architecture_command()
    started_at = now()
    started = time.monotonic()
    immutable_metadata = {
        **(metadata or {}),
        **llama_cpp.benchmark_metadata(),
    }
    event(
        job,
        'Run',
        'Validating the guest CPU architecture and probing its logical CPU '
        'count immediately before llama.cpp.',
    )
    probe_output = ''
    try:
        architecture = immutable_metadata.get('architecture')
        if architecture is None:
            probe_output = ssh(job, probe_command, timeout=60)
            architecture = llama_cpp.parse_architecture(probe_output)
        else:
            architecture = llama_cpp.parse_architecture(str(architecture))
        immutable_metadata['architecture'] = architecture
        immutable_metadata.update(llama_cpp.cpu_build_profile(architecture))

        probe_command = llama_cpp.logical_cpu_count_command()
        probe_output = ''
        probe_output = ssh(job, probe_command, timeout=60)
        logical_cpu_count = llama_cpp.parse_logical_cpu_count(probe_output)
    except Exception as exc:
        failure_output = (
            exc.output
            if isinstance(exc, SSHCommandError) and exc.output
            else probe_output
        )
        job['results'].append({
            'id': benchmark_id,
            'name': name,
            'command': probe_command,
            'started_at': started_at,
            'duration_seconds': round(time.monotonic() - started, 2),
            'status': 'failed',
            'error': f'CPU-topology probe failed. {exc}',
            'output': failure_output,
            'metadata': immutable_metadata,
        })
        raise RuntimeError(
            f'{name} CPU-topology probe failed. {exc}'
        ) from exc

    result_metadata = {
        **immutable_metadata,
        'logical_cpu_count': logical_cpu_count,
        'llama_toolset_enable': toolset_enable or 'system-default',
    }
    probe_command = llama_cpp.toolchain_probe_command(
        toolset_enable=toolset_enable
    )
    probe_output = ''
    event(
        job,
        'Run',
        'Recording the exact GCC and GNU assembler selected for the '
        f'{immutable_metadata["llama_cpu_build_profile"]} llama.cpp build.',
    )
    try:
        probe_output = ssh(
            job,
            probe_command,
            timeout=60,
            include_stderr=False,
        )
        toolchain_metadata = llama_cpp.parse_toolchain_probe(probe_output)
    except Exception as exc:
        failure_output = (
            exc.output
            if isinstance(exc, SSHCommandError) and exc.output
            else probe_output
        )
        job['results'].append({
            'id': benchmark_id,
            'name': name,
            'command': probe_command,
            'started_at': started_at,
            'duration_seconds': round(time.monotonic() - started, 2),
            'status': 'failed',
            'error': f'Build-toolchain probe failed. {exc}',
            'output': failure_output,
            'metadata': result_metadata,
        })
        raise RuntimeError(
            f'{name} build-toolchain probe failed. {exc}'
        ) from exc

    result_metadata.update(toolchain_metadata)
    event(
        job,
        'Run',
        f'Building and running llama.cpp with {logical_cpu_count} observed '
        f'guest logical CPUs using GCC '
        f'{toolchain_metadata["llama_compiler_version"]} and '
        f'{toolchain_metadata["llama_assembler_version"]}.',
    )
    execute_benchmark(
        job,
        benchmark_id,
        name,
        llama_cpp.benchmark_command(
            architecture=architecture,
            toolset_enable=toolset_enable,
            threads=logical_cpu_count,
        ),
        parser=lambda output: llama_cpp.parse_output(
            output,
            expected_threads=logical_cpu_count,
        ),
        metadata=result_metadata,
        output_limit=None,
        include_stderr=False,
        transport_attempts=1,
    )


def oci_runtime_environment(
    job,
    plan,
    architecture=None,
    logical_cpu_count=None,
):
    """Return the exact OCI runner profile used by benchmark reports."""

    resources = job.get('resources', {})
    environment = {
        'provider': 'OCI',
        'region': plan.region,
        'shape': plan.shape,
        'ocpus': plan.ocpus,
        'memory_gb': plan.memory_gb,
        'architecture': architecture or resources.get('architecture'),
        'logical_cpu_count': logical_cpu_count,
        'portable_result_contract': 'v1',
        'image_id': resources.get('image_id'),
        'image_name': resources.get('image_name'),
        'data_volume_type': (
            'OCI Block Volume'
            if plan.storage.additional_volume
            else None
        ),
        'data_volume_size_gb': (
            plan.storage.additional_size_gb
            if plan.storage.additional_volume
            else None
        ),
        'data_volume_vpus_per_gb': (
            plan.storage.additional_performance
            if plan.storage.additional_volume
            else None
        ),
        'data_volume_attachment': (
            (
                'iscsi'
                if plan.storage.mount_style == 'iscsi'
                else 'paravirtualized'
            )
            if plan.storage.additional_volume
            else None
        ),
        'local_nvme_disk_count': (
            resources.get('local_nvme_disk_count')
            if resources.get('local_nvme_supported') is True
            else None
        ),
        'local_nvme_total_size_gb': (
            resources.get('local_nvme_total_size_gb')
            if resources.get('local_nvme_supported') is True
            else None
        ),
    }
    return {
        key: value for key, value in environment.items() if value is not None
    }


def gcp_runtime_environment(job, plan, architecture=None):
    """Return the exact persisted GCE profile used by benchmark reports."""
    resources = job.get('resources', {})
    environment = {
        'provider': 'GCP',
        'project_id': plan.gcp_project_id,
        'region': plan.region,
        'zone': plan.gcp_zone,
        'machine_type': plan.shape,
        'vcpus': plan.ocpus,
        'memory_gb': plan.memory_gb,
        'architecture': architecture or resources.get('architecture'),
        'image_id': resources.get('image_id'),
        'image_name': resources.get('image_name'),
        'network_interface_type': resources.get(
            'gcp_network_interface_type'
        ),
        'disk_interface': resources.get('gcp_disk_interface'),
        'boot_volume_type': resources.get('gcp_boot_disk_type'),
        'boot_volume_provisioned_iops': resources.get(
            'gcp_boot_disk_provisioned_iops'
        ),
        'boot_volume_provisioned_throughput_mibps': resources.get(
            'gcp_boot_disk_provisioned_throughput_mibps'
        ),
        'data_volume_type': resources.get('gcp_data_disk_type'),
        'data_volume_size_gb': resources.get('gcp_data_disk_size_gb'),
        'data_volume_provisioned_iops': resources.get(
            'gcp_data_disk_provisioned_iops'
        ),
        'data_volume_provisioned_throughput_mibps': resources.get(
            'gcp_data_disk_provisioned_throughput_mibps'
        ),
        'iperf3_peer_machine_type': resources.get('gcp_peer_machine_type'),
        'iperf3_peer_image_id': resources.get('gcp_peer_image_id'),
    }
    return {
        key: value for key, value in environment.items() if value is not None
    }


def azure_runtime_environment(job, plan, architecture=None):
    """Return the exact persisted Azure VM profile used by reports."""
    resources = job.get('resources', {})
    environment = {
        'provider': 'Azure',
        'subscription_id': plan.azure_subscription_id,
        'region': plan.region,
        'zone': plan.azure_zone,
        'vm_size': plan.shape,
        'vcpus': plan.ocpus,
        'memory_gb': plan.memory_gb,
        'architecture': architecture or resources.get('architecture'),
        'image_id': resources.get('image_id'),
        'image_name': resources.get('image_name'),
        'resource_group': resources.get('azure_resource_group_name'),
        'network_accelerated': resources.get('azure_accelerated_networking'),
        'boot_volume_type': resources.get('azure_boot_disk_type'),
        'data_volume_type': resources.get('azure_data_disk_type'),
        'data_volume_size_gb': resources.get('azure_data_disk_size_gb'),
        'data_volume_provisioned_iops': resources.get(
            'azure_data_disk_iops'
        ),
        'data_volume_provisioned_throughput_mibps': resources.get(
            'azure_data_disk_throughput_mibps'
        ),
        'local_nvme_available_disk_count': (
            resources.get('local_nvme_disk_count')
            if resources.get('local_nvme_supported') is True
            else None
        ),
        'local_nvme_available_total_size_gb': (
            resources.get('local_nvme_total_size_gb')
            if resources.get('local_nvme_supported') is True
            else None
        ),
        'iperf3_peer_vm_size': resources.get('azure_peer_vm_size'),
        'iperf3_peer_image_id': resources.get('azure_peer_image_id'),
    }
    return {
        key: value for key, value in environment.items() if value is not None
    }


def run_phoronix_profiles(job, plan):
    """Prepare PTS once, then run every selected profile independently."""
    runs = phoronix_profile_runs(plan.phoronix.profiles, job['id'])
    if not runs:
        return ()

    event(
        job,
        'Phoronix prepare',
        'Checking out the pinned Phoronix Test Suite client and refreshing '
        'the official OpenBenchmarking profile index.',
    )
    try:
        preparation_output = ssh(
            job,
            phoronix_prepare_command(),
            timeout=PHORONIX_PREPARE_TIMEOUT_SECONDS,
            include_stderr=True,
        )
        architecture = ssh(job, 'uname -m').strip().splitlines()[-1]
        if architecture not in {'x86_64', 'aarch64'}:
            raise RuntimeError(
                f'Phoronix profiles support x86_64 and aarch64, not '
                f'{architecture or "an unknown architecture"}.'
            )
        event(
            job,
            'Phoronix prepare',
            f'Pinned client and profile index are ready on {architecture}: '
            f'{preparation_output.strip()[-500:]}.',
        )
    except Exception as exc:
        failure_output = (
            exc.output
            if isinstance(exc, SSHCommandError) and exc.output
            else str(exc)
        )
        for run in runs:
            failure_metadata = dict(run.metadata)
            if getattr(plan, 'provider', 'oci') == 'gcp':
                failure_metadata.update(gcp_runtime_environment(job, plan))
            job['results'].append({
                'id': run.benchmark_id,
                'name': run.name,
                'command': run.command,
                'started_at': now(),
                'duration_seconds': 0,
                'status': 'failed',
                'error': f'Phoronix preparation failed. {exc}',
                'output': failure_output,
                'metadata': failure_metadata,
            })
        return (f'Phoronix preparation failed. {exc}',)

    failures = []
    for run in runs:
        metadata = {
            **run.metadata,
            'provider': getattr(plan, 'provider', 'oci').upper(),
            'region': plan.region,
            'architecture': architecture,
            'shape': plan.shape,
            'ocpus': plan.ocpus,
            'memory_gb': plan.memory_gb,
        }
        if getattr(plan, 'provider', 'oci') == 'gcp':
            metadata.update(gcp_runtime_environment(job, plan, architecture))
        if job.get('resources', {}).get('image_id'):
            metadata['image_id'] = job['resources']['image_id']
        if job.get('resources', {}).get('image_name'):
            metadata['image_name'] = job['resources']['image_name']
        result_count = len(job['results'])
        try:
            def parse_selected_profile(output, expected=run.profile):
                return parse_phoronix_result(
                    output,
                    expected_profile=expected,
                )

            execute_benchmark(
                job,
                run.benchmark_id,
                run.name,
                run.command,
                timeout=run.timeout_seconds,
                parser=parse_selected_profile,
                metadata=metadata,
                output_limit=None,
                include_stderr=True,
            )
        except Exception as exc:
            failures.append(f'{run.name}: {exc}')
            if len(job['results']) == result_count:
                job['results'].append({
                    'id': run.benchmark_id,
                    'name': run.name,
                    'command': run.command,
                    'started_at': now(),
                    'duration_seconds': 0,
                    'status': 'failed',
                    'error': str(exc),
                    'output': (
                        exc.output
                        if isinstance(exc, SSHCommandError) and exc.output
                        else ''
                    ),
                    'metadata': metadata,
                })
            event(
                job,
                'Phoronix warning',
                f'{run.name} failed; continuing with the remaining selected '
                f'Phoronix profiles. {exc}',
            )
    return tuple(failures)


def apachebench_request_timeout(request_count):
    """Allow slow shapes to finish large user-selected request counts."""
    return min(86400, max(1800, int(request_count / 25) + 600))


def run_apachebench(job, plan):
    """Run each selected HTTP mode from the shared private load generator."""
    options = plan.apachebench
    resources = job.get('resources', {})
    selected_workloads = tuple(options.workloads)
    diagnostics = []
    failures = []
    common_metadata = web_guest.static_runtime_metadata(plan, resources)
    try:
        target_private_ip = str(ip_address(resources.get('private_ip', '')))
        loadgen_private_ip = str(
            ip_address(resources.get('loadgen_private_ip', ''))
        )
        loadgen_network_bandwidth_gbps = float(
            resources.get('loadgen_network_bandwidth_gbps', 0)
        )
        if loadgen_network_bandwidth_gbps <= 0:
            raise ValueError('invalid load-generator network capacity')
    except (TypeError, ValueError) as exc:
        error = RuntimeError(
            'ApacheBench provisioning did not produce valid private target '
            'and load-generator addresses plus a network-capacity value.'
        )
        for workload_id in selected_workloads:
            settings = apachebench_workload(workload_id)
            name = f'ApacheBench — {settings["name"]}'
            job['results'].append({
                'id': f'apachebench_{workload_id}',
                'name': name,
                'command': 'ApacheBench HTTP server lifecycle',
                'started_at': now(),
                'duration_seconds': 0,
                'status': 'failed',
                'error': str(error),
                'metadata': {
                    **apachebench_metadata(workload_id, options),
                    **common_metadata,
                },
                'output': '',
            })
            failures.append(f'{name}: {error}')
        event(job, 'ApacheBench warning', str(error))
        return tuple(failures)
    if not resources.get('loadgen_public_ip'):
        error = RuntimeError(
            'ApacheBench provisioning did not produce a reachable load '
            'generator.'
        )
        for workload_id in selected_workloads:
            settings = apachebench_workload(workload_id)
            name = f'ApacheBench — {settings["name"]}'
            job['results'].append({
                'id': f'apachebench_{workload_id}',
                'name': name,
                'command': 'ApacheBench HTTP server lifecycle',
                'started_at': now(),
                'duration_seconds': 0,
                'status': 'failed',
                'error': str(error),
                'metadata': {
                    **apachebench_metadata(workload_id, options),
                    **common_metadata,
                },
                'output': '',
            })
            failures.append(f'{name}: {error}')
        event(job, 'ApacheBench warning', str(error))
        return tuple(failures)
    httpd_installed = False
    httpd_ready = False

    def stage(
        label,
        message,
        command,
        host_key='public_ip',
        timeout=1800,
        include_stderr=True,
    ):
        event(job, label, message)
        try:
            output = ssh(
                job,
                command,
                timeout=timeout,
                host_key=host_key,
                include_stderr=include_stderr,
            )
        except SSHCommandError as exc:
            if exc.output:
                diagnostics.append(
                    f'--- {label} (failed) ---\n{exc.output[-20000:]}'
                )
            raise
        diagnostics.append(f'--- {label} ---\n{output[-12000:]}')
        event(job, label, f'{message} Complete.')
        return output

    try:
        provider = web_guest.provider_id(plan)
        wait_for_web_guest_readiness(
            job,
            plan,
            'apachebench',
            role='service',
            label='ApacheBench web-server instance',
        )
        wait_for_web_guest_readiness(
            job,
            plan,
            'apachebench',
            role='loadgen',
            host_key='loadgen_public_ip',
            label='ApacheBench load generator',
        )
        event(
            job,
            'ApacheBench install',
            'Installing Apache HTTP Server on the target and ApacheBench on '
            'the separate load-generator VM.',
        )
        install_apachebench_guest(job, plan, role='service')
        httpd_installed = True
        install_apachebench_guest(
            job,
            plan,
            role='loadgen',
            host_key='loadgen_public_ip',
        )
        stage(
            'ApacheBench target',
            'Configuring the deterministic static response and restricting '
            'guest TCP/80 ingress to the load generator.',
            apachebench_target_prepare_command(
                options.response_size_kib,
                loadgen_private_ip,
            ),
        )
        httpd_ready = True
        stage(
            'ApacheBench load generator',
            'Verifying ApacheBench and preparing client resource limits.',
            apachebench_loadgen_prepare_command(),
            host_key='loadgen_public_ip',
        )
        service_architecture = llama_cpp.parse_architecture(stage(
            'ApacheBench architecture',
            'Recording the web-server VM architecture.',
            'uname -m',
            include_stderr=False,
        ))
        loadgen_architecture = llama_cpp.parse_architecture(stage(
            'ApacheBench architecture',
            'Recording the load-generator VM architecture.',
            'uname -m',
            host_key='loadgen_public_ip',
            include_stderr=False,
        ))
        stage(
            'ApacheBench readiness',
            'Verifying HTTP 200 and the configured response size over the '
            f'{web_guest.traffic_path(provider)}.',
            apachebench_readiness_command(
                target_private_ip,
                options.response_size_kib,
            ),
            host_key='loadgen_public_ip',
            timeout=600,
        )
        common_metadata = {
            **web_guest.runtime_metadata(
                plan,
                resources,
                service_architecture=service_architecture,
                loadgen_architecture=loadgen_architecture,
            ),
            'target_private_ip': target_private_ip,
        }
        timeout = apachebench_request_timeout(options.request_count)
        for workload_id in selected_workloads:
            settings = apachebench_workload(workload_id)
            result_id = f'apachebench_{workload_id}'
            result_name = f'ApacheBench — {settings["name"]}'
            started_at = now()
            started = time.monotonic()
            trial_outputs = []
            trial_metrics = []
            commands = []
            try:
                if options.warmup_requests:
                    warmup_output = stage(
                        'ApacheBench warm-up',
                        f'Running {options.warmup_requests:,} untimed '
                        f'{settings["name"]} warm-up requests.',
                        apachebench_warmup_command(
                            workload_id,
                            target_private_ip,
                            options,
                        ),
                        host_key='loadgen_public_ip',
                        timeout=apachebench_request_timeout(
                            options.warmup_requests
                        ),
                    )
                    warmup_metrics = parse_apachebench_output(warmup_output)
                    record_apachebench_network_capacity(
                        warmup_metrics,
                        loadgen_network_bandwidth_gbps,
                    )
                    validate_apachebench_metrics(
                        warmup_metrics,
                        workload_id,
                        options.warmup_requests,
                        min(options.concurrency, options.warmup_requests),
                        options.response_size_kib,
                    )
                    event(
                        job,
                        'ApacheBench warm-up',
                        f'{settings["name"]} warm-up achieved '
                        f'{warmup_metrics["requests_per_second"]:.2f} '
                        'requests/second; these results are excluded from '
                        'the report.',
                    )
                for trial in range(1, options.trials + 1):
                    command = apachebench_command(
                        workload_id,
                        target_private_ip,
                        options,
                        trial,
                    )
                    commands.append(command)
                    event(
                        job,
                        'ApacheBench run',
                        f'Started {settings["name"]} trial {trial} of '
                        f'{options.trials}.',
                    )
                    output = ssh(
                        job,
                        command,
                        timeout=timeout,
                        host_key='loadgen_public_ip',
                        include_stderr=True,
                    )
                    trial_outputs.append(
                        f'--- Trial {trial} of {options.trials} ---\n{output}'
                    )
                    metrics = parse_apachebench_output(output)
                    record_apachebench_network_capacity(
                        metrics,
                        loadgen_network_bandwidth_gbps,
                    )
                    validate_apachebench_metrics(
                        metrics,
                        workload_id,
                        options.request_count,
                        options.concurrency,
                        options.response_size_kib,
                    )
                    trial_metrics.append(metrics)
                    event(
                        job,
                        'ApacheBench run',
                        f'Completed {settings["name"]} trial {trial} at '
                        f'{metrics["requests_per_second"]:.2f} requests/second.',
                    )
                metadata = {
                    **apachebench_metadata(workload_id, options),
                    **common_metadata,
                    'measured_trials': options.trials,
                }
                job['results'].append({
                    'id': result_id,
                    'name': result_name,
                    'command': '\n'.join(commands),
                    'started_at': started_at,
                    'duration_seconds': round(time.monotonic() - started, 2),
                    'status': 'completed',
                    'metadata': metadata,
                    'metrics': aggregate_apachebench_trials(trial_metrics),
                    'trial_metrics': trial_metrics,
                    'output': '\n\n'.join(trial_outputs),
                    'setup_output': '\n\n'.join(diagnostics),
                })
                event(
                    job,
                    'ApacheBench complete',
                    f'Completed {result_name} across {options.trials} measured '
                    'trials.',
                )
            except Exception as exc:
                if isinstance(exc, SSHCommandError) and exc.output:
                    trial_outputs.append(
                        f'--- Trial {len(trial_metrics) + 1} failed ---\n'
                        f'{exc.output}'
                    )
                failure_metadata = {
                    **common_metadata,
                    **apachebench_metadata(workload_id, options),
                    'measured_trials': options.trials,
                    'completed_trials': len(trial_metrics),
                }
                job['results'].append({
                    'id': result_id,
                    'name': result_name,
                    'command': '\n'.join(commands) or 'ApacheBench lifecycle',
                    'started_at': started_at,
                    'duration_seconds': round(time.monotonic() - started, 2),
                    'status': 'failed',
                    'error': str(exc),
                    'metadata': failure_metadata,
                    'trial_metrics': trial_metrics,
                    'output': '\n\n'.join(trial_outputs),
                    'setup_output': '\n\n'.join(diagnostics),
                })
                failures.append(f'{result_name}: {exc}')
                event(
                    job,
                    'ApacheBench warning',
                    f'{result_name} failed; continuing with the remaining '
                    f'selected connection modes. {exc}',
                )
    except Exception as exc:
        failure_output = '\n\n'.join(diagnostics)
        if isinstance(exc, SSHCommandError) and exc.output:
            failure_output += f'\n\n--- failure ---\n{exc.output[-20000:]}'
        existing_ids = {result.get('id') for result in job.get('results', [])}
        for workload_id in selected_workloads:
            result_id = f'apachebench_{workload_id}'
            if result_id in existing_ids:
                continue
            settings = apachebench_workload(workload_id)
            name = f'ApacheBench — {settings["name"]}'
            job['results'].append({
                'id': result_id,
                'name': name,
                'command': 'ApacheBench HTTP server lifecycle',
                'started_at': now(),
                'duration_seconds': 0,
                'status': 'failed',
                'error': str(exc),
                'metadata': {
                    **apachebench_metadata(workload_id, options),
                    **common_metadata,
                },
                'output': failure_output,
            })
            failures.append(f'{name}: {exc}')
        event(job, 'ApacheBench warning', f'ApacheBench setup failed. {exc}')
    finally:
        retain_httpd = (
            httpd_ready
            and not getattr(plan, 'destroy_after_completion', True)
            and 'deathstarbench' not in plan.benchmarks
        )
        if retain_httpd:
            event(
                job,
                'ApacheBench retained',
                'Apache HTTP Server remains active for follow-up testing on '
                'the retained benchmark VM.',
            )
        elif httpd_installed:
            try:
                ssh(
                    job,
                    'sudo systemctl stop httpd >/dev/null 2>&1 || true',
                    timeout=120,
                )
                event(
                    job,
                    'ApacheBench teardown',
                    'Stopped Apache HTTP Server after the benchmark.',
                )
            except Exception as exc:
                event(
                    job,
                    'ApacheBench teardown warning',
                    f'Unable to stop Apache HTTP Server: {exc}',
                )
    return tuple(failures)


def run_deathstarbench(job, plan):
    options = plan.deathstarbench
    topology_id = getattr(
        options,
        'topology_id',
        SINGLE_HOST_TOPOLOGY_ID,
    )
    runtime_id = getattr(
        options,
        'runtime_id',
        PODMAN_COMPOSE_RUNTIME_ID,
    )
    if (
        topology_id != SINGLE_HOST_TOPOLOGY_ID
        or runtime_id != PODMAN_COMPOSE_RUNTIME_ID
    ):
        raise RuntimeError(
            'The compact DeathStarBench runner only accepts the exact '
            'single_host_v1/podman_compose_v1 contract. Distributed K3s '
            'remains available only through its internal qualification path.'
        )
    settings = deathstarbench_workload(options.workload)
    resources = job.get('resources', {})
    diagnostics = []
    started_at = now()
    lifecycle_started = time.monotonic()
    service_architecture = None
    loadgen_architecture = None
    runtime_details = None
    service_private_ip = None
    loadgen_private_ip = None
    base_metadata = {
        **deathstarbench_metadata(options.workload, options, None, None),
        **web_guest.static_runtime_metadata(plan, resources),
    }

    def current_metadata():
        metadata = {
            **base_metadata,
            **deathstarbench_metadata(
                options.workload,
                options,
                service_architecture,
                loadgen_architecture,
            ),
            **web_guest.runtime_metadata(
                plan,
                resources,
                service_architecture=service_architecture,
                loadgen_architecture=loadgen_architecture,
            ),
        }
        if runtime_details:
            metadata['container_runtime_details'] = runtime_details.strip()
        if service_private_ip:
            metadata['target_private_ip'] = service_private_ip
        return metadata

    def stage(
        label,
        message,
        command,
        host_key='public_ip',
        timeout=1800,
        include_stderr=True,
    ):
        event(job, label, message)
        try:
            output = ssh(
                job,
                command,
                timeout=timeout,
                host_key=host_key,
                include_stderr=include_stderr,
            )
        except SSHCommandError as exc:
            if exc.output:
                diagnostics.append(
                    f'--- {label} (failed) ---\n{exc.output[-20000:]}'
                )
            raise
        diagnostics.append(f'--- {label} ---\n{output[-12000:]}')
        event(job, label, f'{message} Complete.')
        return output

    try:
        raw_service_private_ip = resources.get('private_ip')
        loadgen_public_ip = resources.get('loadgen_public_ip')
        raw_loadgen_private_ip = resources.get('loadgen_private_ip')
        if (
            not raw_service_private_ip
            or not loadgen_public_ip
            or not raw_loadgen_private_ip
        ):
            raise RuntimeError(
                'DeathStarBench provisioning did not produce a private service '
                'address plus reachable public and private load-generator '
                'addresses.'
            )
        service_private_ip = str(ip_address(raw_service_private_ip))
        loadgen_private_ip = str(ip_address(raw_loadgen_private_ip))
        provider = web_guest.provider_id(plan)
        compose_path = web_guest.podman_compose_path(provider)
        wait_for_web_guest_readiness(
            job,
            plan,
            'deathstarbench',
            role='service',
            label='DeathStarBench service instance',
        )
        wait_for_web_guest_readiness(
            job,
            plan,
            'deathstarbench',
            role='loadgen',
            host_key='loadgen_public_ip',
            label='DeathStarBench load generator',
        )

        event(
            job,
            'DeathStarBench install',
            'Installing the native, daemonless Podman toolchain on the service '
            'instance. Only Podman and podman-compose are invoked; a '
            'Podman-supplied Docker compatibility wrapper may be present but '
            'is never called.',
        )
        install_deathstarbench_guest(job, plan, role='service')
        runtime_details = stage(
            'DeathStarBench runtime',
            'Verifying rootful Podman, Netavark DNS, podman-compose, and the '
            'absence of Docker Engine. A verified Podman compatibility wrapper '
            'is permitted but never invoked.',
            podman_runtime_verification_command(compose_path),
        )
        stage(
            'DeathStarBench network',
            f'Allowing only the load generator at {loadgen_private_ip} to '
            f'reach the {settings["name"]} frontend on TCP '
            f'{settings["port"]}.',
            frontend_firewall_command(
                options.workload,
                loadgen_private_ip,
            ),
        )
        stage(
            'DeathStarBench source',
            'Checking out the pinned upstream revision and generating a '
            'Podman/SELinux-specific Compose definition.',
            prepare_workload_command(options.workload),
        )
        stage(
            'DeathStarBench images',
            'Prefetching every external base and runtime image sequentially '
            'with bounded registry retries.',
            prefetch_workload_images_command(options.workload),
            timeout=3600,
        )
        stage(
            'DeathStarBench build',
            'Building the selected workload images natively for the service '
            'VM architecture and validating every custom image architecture.',
            build_workload_command(options.workload),
            timeout=7200,
        )
        stage(
            'DeathStarBench deploy',
            'Creating the Podman bridge/DNS network and starting the '
            'microservice stack with podman-compose.',
            deploy_workload_command(options.workload, compose_path),
            timeout=3600,
        )

        event(
            job,
            'DeathStarBench load generator',
            'Installing wrk2 build and workload-initialization prerequisites '
            'on the separate x86 load-generator VM.',
        )
        install_deathstarbench_guest(
            job,
            plan,
            role='loadgen',
            host_key='loadgen_public_ip',
        )
        stage(
            'DeathStarBench load generator',
            'Building wrk2 from the pinned upstream source on the separate VM.',
            load_generator_prepare_command(),
            host_key='loadgen_public_ip',
            timeout=1800,
        )
        service_architecture = llama_cpp.parse_architecture(stage(
            'DeathStarBench architecture',
            'Recording the service VM architecture.',
            'uname -m',
            include_stderr=False,
        ))
        loadgen_architecture = llama_cpp.parse_architecture(stage(
            'DeathStarBench architecture',
            'Recording the load-generator VM architecture.',
            'uname -m',
            host_key='loadgen_public_ip',
            include_stderr=False,
        ))
        if loadgen_architecture != 'x86_64':
            raise RuntimeError(
                'The DeathStarBench load generator must be x86_64 because '
                'upstream wrk2 is not supported on ARM.'
            )
        stage(
            'DeathStarBench readiness',
            f'Waiting for a successful request through the {settings["name"]} '
            'frontend.',
            frontend_readiness_command(options.workload, service_private_ip),
            host_key='loadgen_public_ip',
            timeout=720,
        )
        stage(
            'DeathStarBench initialize',
            f'Loading the {settings["name"]} benchmark dataset.',
            initialize_workload_command(options.workload, service_private_ip),
            host_key='loadgen_public_ip',
            timeout=2400,
        )

        if options.warmup_seconds:
            warmup_output = stage(
                'DeathStarBench warm-up',
                f'Running an untimed {options.warmup_seconds}-second warm-up.',
                deathstarbench_load_command(
                    options.workload,
                    service_private_ip,
                    options,
                    options.warmup_seconds,
                ),
                host_key='loadgen_public_ip',
                timeout=options.warmup_seconds + 300,
            )
            warmup_metrics = parse_wrk2_output(warmup_output)
            event(
                job,
                'DeathStarBench warm-up',
                f'Warm-up achieved '
                f'{warmup_metrics["throughput_requests_per_second"]:.2f} '
                'requests/second; these results are excluded from the report.',
            )

        result_metadata = current_metadata()
        measured_command = deathstarbench_load_command(
            options.workload,
            service_private_ip,
            options,
            options.duration_seconds,
        )
        execute_benchmark(
            job,
            'deathstarbench',
            f'DeathStarBench — {settings["name"]}',
            measured_command,
            timeout=options.duration_seconds + 300,
            host_key='loadgen_public_ip',
            parser=parse_wrk2_output,
            metadata=result_metadata,
            output_limit=None,
            include_stderr=True,
        )
        for result in reversed(job['results']):
            if result.get('id') == 'deathstarbench':
                result['setup_output'] = '\n\n'.join(diagnostics)
                break
    except Exception as exc:
        existing = any(
            result.get('id') == 'deathstarbench'
            for result in job.get('results', [])
        )
        if not existing:
            failure_output = '\n\n'.join(diagnostics)
            if isinstance(exc, SSHCommandError) and exc.output:
                failure_output += f'\n\n--- failure ---\n{exc.output[-20000:]}'
            job['results'].append({
                'id': 'deathstarbench',
                'name': f'DeathStarBench — {settings["name"]}',
                'command': 'DeathStarBench Podman lifecycle',
                'started_at': started_at,
                'duration_seconds': round(
                    time.monotonic() - lifecycle_started,
                    2,
                ),
                'status': 'failed',
                'error': str(exc),
                'metadata': current_metadata(),
                'output': failure_output,
            })
        else:
            for result in reversed(job['results']):
                if result.get('id') == 'deathstarbench':
                    result.setdefault('setup_output', '\n\n'.join(diagnostics))
                    break
        raise RuntimeError(f'DeathStarBench failed. {exc}') from exc


def _validate_azure_distributed_deathstarbench_candidate_plan(plan):
    """Validate the exact internal plan shared by candidate-only hooks."""

    provider = (
        plan.get('provider')
        if isinstance(plan, dict)
        else getattr(plan, 'provider', None)
    )
    benchmarks = (
        plan.get('benchmarks', ())
        if isinstance(plan, dict)
        else getattr(plan, 'benchmarks', ())
    ) or ()
    options = (
        plan.get('deathstarbench')
        if isinstance(plan, dict)
        else getattr(plan, 'deathstarbench', None)
    )

    def option_value(key, default=None):
        if isinstance(options, dict):
            return options.get(key, default)
        return getattr(options, key, default)

    if str(provider or '').casefold() != 'azure':
        raise ValueError('The distributed runtime candidate requires Azure.')
    if tuple(benchmarks) != ('deathstarbench',):
        raise ValueError(
            'The distributed runtime candidate requires DeathStarBench alone.'
        )
    if (
        option_value('topology_id') != DISTRIBUTED_TIERED_TOPOLOGY_ID
        or option_value('runtime_id') != K3S_RUNTIME_ID
        or option_value('workload') != 'social_network'
    ):
        raise ValueError(
            'The distributed runtime candidate requires the exact Social '
            'Network distributed_tiered_v1/k3s_v1 contract.'
        )


def prepare_azure_distributed_deathstarbench_candidate_runtime(job, plan):
    """Internal live-qualification hook for the unreleased Azure candidate.

    This is intentionally not called by ``run_benchmarks``. It stops after
    K3s cluster attestation and does not create benchmark results or deploy a
    DeathStarBench workload.
    """

    _validate_azure_distributed_deathstarbench_candidate_plan(plan)
    return prepare_azure_distributed_k3s_candidate(
        job,
        execute=ssh,
        emit=event,
        persist=persist_job_state,
    )


def prepare_azure_distributed_deathstarbench_candidate_workload(
    job,
    plan,
    image_lock,
):
    """Internal hook that deploys but does not initialize the Social workload.

    The caller supplies the artifact emitted by the manual image-publication
    workflow. This hook remains outside ``run_benchmarks`` and stops at the
    attested ``workload_ready`` boundary without producing a result.
    """

    _validate_azure_distributed_deathstarbench_candidate_plan(plan)
    return prepare_azure_distributed_social_network_candidate(
        job,
        image_lock,
        execute=ssh,
        emit=event,
        persist=persist_job_state,
    )


def run_azure_distributed_deathstarbench_candidate_measurement(
    job,
    plan,
    image_lock,
    *,
    qualification_checkpoint=None,
):
    """Internal one-shot measurement hook for the unreleased candidate.

    The operator-only qualification harness is the sole caller.  Keeping this
    outside ``run_benchmarks`` prevents the distributed candidate from being
    selected through the public UI before its provider qualification and
    immutable load-driver gates are complete.
    """

    _validate_azure_distributed_deathstarbench_candidate_plan(plan)
    options = (
        plan.get('deathstarbench')
        if isinstance(plan, dict)
        else getattr(plan, 'deathstarbench', None)
    )
    return run_azure_distributed_social_network_measurement(
        job,
        image_lock,
        options,
        execute=ssh,
        emit=event,
        persist=persist_job_state,
        qualification_checkpoint=qualification_checkpoint,
    )


def run_aws_benchmarks(job, plan):
    """Run the selected supported benchmarks on Amazon Linux 2023."""
    selected = expanded_benchmark_ids(plan)
    apachebench_failures = ()
    iperf3_protocols = selected_iperf3_protocols(plan)
    amazon_linux.validate_benchmark_selection(
        plan.benchmarks,
        sysbench_workloads=selected_sysbench_workloads(plan),
        iperf3_protocols=iperf3_protocols,
        phoronix_profiles=(
            plan.phoronix.profiles if 'phoronix' in plan.benchmarks else ()
        ),
        llm_benchmarks=plan.llm_benchmarks,
    )
    event(
        job,
        'Connect',
        'Waiting for Amazon Linux cloud-init, DNS, and package repositories.',
    )
    readiness_output = ssh(
        job,
        amazon_linux.readiness_command(
            amazon_linux.benchmark_readiness_hosts(
                plan.benchmarks,
                llm_benchmarks=plan.llm_benchmarks,
            )
        ),
        timeout=600,
        include_stderr=True,
    )
    event(
        job,
        'Connect',
        f'Amazon Linux readiness passed: {readiness_output.strip()[-1000:]}',
    )

    for step in amazon_linux.installation_steps(
        plan.benchmarks,
        sysbench_workloads=selected_sysbench_workloads(plan),
        iperf3_protocols=iperf3_protocols,
        phoronix_profiles=(
            plan.phoronix.profiles if 'phoronix' in plan.benchmarks else ()
        ),
        llm_benchmarks=plan.llm_benchmarks,
        additional_volume=plan.storage.additional_volume,
    ):
        event(job, 'Install', f'Installing {step.name}.')
        output = ssh(
            job,
            step.command,
            timeout=1800,
            include_stderr=True,
        )
        event(job, 'Install', f'{step.name} is ready: {output.strip()[-1000:]}')

    if iperf3_protocols:
        wait_for_iperf_peer(
            job,
            iperf3_protocols,
            verify_tcp_udp=True,
        )

    storage_directory = '/data'
    storage_metadata = None
    if STORAGE_BENCHMARK_RESULT_IDS.intersection(selected):
        storage_directory, storage_metadata = (
            prepare_storage_benchmark_target(job, plan, selected)
        )
    elif plan.storage.additional_volume:
        mount_data_volume(job)

    commands = amazon_linux.benchmark_commands(
        plan.ocpus,
        storage_directory=storage_directory,
    )
    resources = job.get('resources', {})
    environment = {
        'provider': 'AWS',
        'region': plan.region,
        'instance_type': plan.shape,
        'vcpus': plan.ocpus,
        'memory_gb': plan.memory_gb,
        'bare_metal': resources.get('bare_metal'),
        'hypervisor': resources.get('hypervisor'),
        'architecture': resources.get('architecture'),
        'image_id': resources.get('image_id'),
        'image_name': resources.get('image_name'),
        'data_volume_type': resources.get('aws_data_volume_type'),
        'data_volume_size_gb': resources.get('aws_data_volume_size_gb'),
        'data_volume_iops': resources.get('aws_data_volume_iops'),
        'data_volume_throughput_mibps': resources.get(
            'aws_data_volume_throughput_mibps'
        ),
        'local_nvme_available_disk_count': (
            resources.get('local_nvme_disk_count')
            if resources.get('local_nvme_supported') is True
            else None
        ),
        'local_nvme_available_total_size_gb': (
            resources.get('local_nvme_total_size_gb')
            if resources.get('local_nvme_supported') is True
            else None
        ),
        'iperf3_peer_instance_type': resources.get(
            'aws_peer_instance_type'
        ),
        'iperf3_peer_image_id': resources.get('aws_peer_image_id'),
    }
    environment = {
        key: value for key, value in environment.items() if value is not None
    }
    for key in (
        'sysbench_cpu',
        'sysbench_memory',
        'stream',
        'fio',
        'sysbench_fileio',
    ):
        if key in selected:
            name, command = commands[key]
            parser = None
            if key == 'sysbench_cpu':
                parser = amazon_linux.parse_sysbench_cpu_output
            elif key == 'sysbench_memory':
                parser = amazon_linux.parse_sysbench_memory_output
            elif key == 'stream':
                parser = lambda output: amazon_linux.parse_stream_output(
                    output,
                    expected_threads=plan.ocpus,
                )
            elif key == 'fio':
                parser = amazon_linux.parse_fio_output
            elif key == 'sysbench_fileio':
                parser = amazon_linux.parse_sysbench_fileio_output
            execute_benchmark(
                job,
                key,
                name,
                command,
                parser=parser,
                metadata=(
                    {**environment, **(storage_metadata or {})}
                    if key in STORAGE_BENCHMARK_RESULT_IDS
                    else environment
                ),
                output_limit=None if key == 'fio' else 20000,
            )

    network_selected = expanded_iperf3_result_ids(plan)
    network_commands = network_benchmark_commands(job, network_selected)
    for key in network_selected:
        name, command = network_commands[key]
        protocol = key.removeprefix('iperf_')
        event(
            job,
            'Network',
            'Targeting the same-AZ AWS peer over its private VPC address '
            f'{resources["peer_private_ip"]}.',
        )
        execute_benchmark(
            job,
            key,
            name,
            command,
            parser=lambda output, expected=protocol: parse_iperf3_output(
                output,
                expected_protocol=expected,
            ),
            metadata={
                **environment,
                'protocol': protocol.upper(),
                'traffic_path': 'AWS private VPC address',
                'peer_private_ip': resources['peer_private_ip'],
            },
            output_limit=None,
        )

    phoronix_failures = (
        run_phoronix_profiles(job, plan)
        if 'phoronix' in selected
        else ()
    )
    if 'llama_bench' in plan.llm_benchmarks:
        run_llama_benchmark(
            job,
            metadata=environment,
            toolset_enable=None,
        )
    if 'apachebench' in plan.benchmarks:
        apachebench_failures = run_apachebench(job, plan)
    if 'deathstarbench' in plan.benchmarks:
        run_deathstarbench(job, plan)
    deferred_failures = (*phoronix_failures, *apachebench_failures)
    if deferred_failures:
        raise RuntimeError(
            'One or more independently selected benchmark workloads failed '
            'after the remaining workloads were attempted: '
            + ' | '.join(deferred_failures)
        )


def run_rocky_linux_benchmarks(
    job,
    plan,
    *,
    provider_name,
    environment,
):
    """Run the shared strict Rocky Linux 9 benchmark contract."""
    selected = expanded_benchmark_ids(plan)
    apachebench_failures = ()
    iperf3_protocols = selected_iperf3_protocols(plan)
    phoronix_profiles = (
        plan.phoronix.profiles if 'phoronix' in plan.benchmarks else ()
    )
    guest_provider_options = (
        {'provider': 'azure'} if provider_name == 'Azure' else {}
    )
    rocky_linux.validate_benchmark_selection(
        plan.benchmarks,
        sysbench_workloads=selected_sysbench_workloads(plan),
        iperf3_protocols=iperf3_protocols,
        phoronix_profiles=phoronix_profiles,
        llm_benchmarks=plan.llm_benchmarks,
        **guest_provider_options,
    )

    resources = job.get('resources', {})
    network_path = (
        'GCP private VPC address'
        if provider_name == 'GCP'
        else 'Azure private virtual-network address'
    )
    architecture = resources.get('architecture')
    if not architecture:
        raise RuntimeError(
            f'The {provider_name} machine architecture is missing from the persisted run '
            'manifest; refusing to prepare an unverified guest.'
        )
    readiness_hosts = rocky_linux.benchmark_readiness_hosts(
        plan.benchmarks,
        llm_benchmarks=plan.llm_benchmarks,
    )
    wait_for_ssh_transport(job)
    event(
        job,
        'Connect',
        'Waiting for Rocky Linux 9 DNS, package repositories, and the ' +
        (
            'selected machine architecture.'
            if provider_name == 'GCP'
            else 'selected Azure machine architecture.'
        ),
    )
    readiness_output = ssh(
        job,
        rocky_linux.readiness_command(
            architecture,
            readiness_hosts,
            **guest_provider_options,
        ),
        timeout=600,
        include_stderr=True,
        transport_attempts=1,
    )
    event(
        job,
        'Connect',
        f'Rocky Linux 9 readiness passed: '
        f'{readiness_output.strip()[-1000:]}',
    )

    for step in rocky_linux.installation_steps(
        plan.benchmarks,
        sysbench_workloads=selected_sysbench_workloads(plan),
        iperf3_protocols=iperf3_protocols,
        phoronix_profiles=phoronix_profiles,
        llm_benchmarks=plan.llm_benchmarks,
        additional_volume=plan.storage.additional_volume,
        **guest_provider_options,
    ):
        event(job, 'Install', f'Installing {step.name}.')
        output = ssh(
            job,
            step.command,
            timeout=1800,
            include_stderr=True,
            transport_attempts=1,
        )
        event(
            job,
            'Install',
            f'{step.name} is ready: {output.strip()[-1000:]}',
        )

    if iperf3_protocols:
        wait_for_iperf_peer(
            job,
            iperf3_protocols,
            verify_tcp_udp=True,
        )

    storage_directory = '/data'
    storage_metadata = None
    if STORAGE_BENCHMARK_RESULT_IDS.intersection(selected):
        storage_directory, storage_metadata = (
            prepare_storage_benchmark_target(job, plan, selected)
        )
    elif plan.storage.additional_volume:
        mount_data_volume(job)

    commands = rocky_linux.benchmark_commands(
        plan.ocpus,
        storage_directory=storage_directory,
    )
    parser_by_id = {
        'sysbench_cpu': rocky_linux.parse_sysbench_cpu_output,
        'sysbench_memory': rocky_linux.parse_sysbench_memory_output,
        'fio': rocky_linux.parse_fio_output,
        'sysbench_fileio': rocky_linux.parse_sysbench_fileio_output,
    }
    for key in (
        'sysbench_cpu',
        'sysbench_memory',
        'stream',
        'fio',
        'sysbench_fileio',
    ):
        if key not in selected:
            continue
        name, command = commands[key]
        parser = parser_by_id.get(key)
        if key == 'stream':
            parser = lambda output: rocky_linux.parse_stream_output(
                output,
                expected_threads=plan.ocpus,
            )
        execute_benchmark(
            job,
            key,
            name,
            command,
            parser=parser,
            metadata=(
                {**environment, **(storage_metadata or {})}
                if key in STORAGE_BENCHMARK_RESULT_IDS
                else environment
            ),
            output_limit=None if key == 'fio' else 20000,
        )

    network_selected = expanded_iperf3_result_ids(plan)
    network_commands = network_benchmark_commands(job, network_selected)
    for key in network_selected:
        name, command = network_commands[key]
        protocol = key.removeprefix('iperf_')
        event(
            job,
            'Network',
            f'Targeting the same-zone {provider_name} peer over its private '
            f'{"VPC" if provider_name == "GCP" else "virtual-network"} address '
            f'{resources["peer_private_ip"]}.',
        )
        execute_benchmark(
            job,
            key,
            name,
            command,
            parser=lambda output, expected=protocol: parse_iperf3_output(
                output,
                expected_protocol=expected,
            ),
            metadata={
                **environment,
                'protocol': protocol.upper(),
                'traffic_path': network_path,
                'peer_private_ip': resources['peer_private_ip'],
            },
            output_limit=None,
        )

    phoronix_failures = (
        run_phoronix_profiles(job, plan) if 'phoronix' in selected else ()
    )
    if 'llama_bench' in plan.llm_benchmarks:
        run_llama_benchmark(
            job,
            metadata=environment,
            toolset_enable=rocky_linux.LLAMA_TOOLSET_ENABLE,
        )
    if 'apachebench' in plan.benchmarks:
        apachebench_failures = run_apachebench(job, plan)
    if 'deathstarbench' in plan.benchmarks:
        run_deathstarbench(job, plan)
    deferred_failures = (*phoronix_failures, *apachebench_failures)
    if deferred_failures:
        raise RuntimeError(
            'One or more independently selected benchmark workloads failed '
            'after the remaining workloads were attempted: '
            + ' | '.join(deferred_failures)
        )


def run_gcp_benchmarks(job, plan):
    """Run the selected supported benchmarks on Rocky Linux 9 for GCE."""
    return run_rocky_linux_benchmarks(
        job,
        plan,
        provider_name='GCP',
        environment=gcp_runtime_environment(
            job,
            plan,
            job.get('resources', {}).get('architecture'),
        ),
    )


def run_azure_benchmarks(job, plan):
    """Run the selected supported benchmarks on Rocky Linux 9 for Azure."""
    return run_rocky_linux_benchmarks(
        job,
        plan,
        provider_name='Azure',
        environment=azure_runtime_environment(
            job,
            plan,
            job.get('resources', {}).get('architecture'),
        ),
    )


def run_benchmarks(job, plan):
    return dispatch_provider_operation(
        plan,
        'benchmark',
        {
            'oci': lambda: run_oci_benchmarks(job, plan),
            'aws': lambda: run_aws_benchmarks(job, plan),
            'gcp': lambda: run_gcp_benchmarks(job, plan),
            'azure': lambda: run_azure_benchmarks(job, plan),
        },
    )


def run_oci_benchmarks(job, plan):
    selected = expanded_benchmark_ids(plan)
    apachebench_failures = ()
    readiness_hosts = [f'yum.{plan.region}.oci.oraclecloud.com']
    core_result_ids = {
        'sysbench_cpu',
        'sysbench_memory',
        'sysbench_fileio',
        'stream',
        'fio',
    }
    if {
        'sysbench_cpu',
        'sysbench_memory',
        'sysbench_fileio',
    } & selected:
        readiness_hosts.extend(('github.com', 'codeload.github.com'))
    if 'stream' in selected:
        readiness_hosts.append('raw.githubusercontent.com')
    if {'phoronix', 'deathstarbench'} & selected or plan.llm_benchmarks:
        readiness_hosts.append('github.com')
    if 'phoronix' in selected:
        readiness_hosts.append('openbenchmarking.org')
    if plan.llm_benchmarks:
        readiness_hosts.append('huggingface.co')
    wait_for_guest_readiness(
        job,
        plan.region,
        required_hosts=readiness_hosts,
    )
    install_benchmark_tools(job, plan, selected)
    iperf3_protocols = selected_iperf3_protocols(plan)
    if iperf3_protocols:
        wait_for_iperf_peer(job, iperf3_protocols)
    storage_directory = '/data'
    storage_metadata = None
    if STORAGE_BENCHMARK_RESULT_IDS.intersection(selected):
        storage_directory, storage_metadata = (
            prepare_storage_benchmark_target(job, plan, selected)
        )
    elif plan.storage.additional_volume:
        mount_data_volume(job)
    environment = None
    logical_cpu_count = None
    if core_result_ids & selected:
        event(
            job,
            'Connect',
            'Recording the OCI guest architecture and observed logical CPU '
            'count for comparable benchmark results.',
        )
        architecture_output = ssh(
            job,
            llama_cpp.architecture_command(),
            timeout=60,
        )
        architecture = llama_cpp.parse_architecture(architecture_output)
        logical_cpu_output = ssh(
            job,
            llama_cpp.logical_cpu_count_command(),
            timeout=60,
        )
        logical_cpu_count = llama_cpp.parse_logical_cpu_count(
            logical_cpu_output
        )
        expected_architecture = job.get('resources', {}).get('architecture')
        if expected_architecture is not None:
            expected_architecture = amazon_linux.normalize_architecture(
                expected_architecture
            )
            if expected_architecture != architecture:
                raise RuntimeError(
                    'The observed OCI guest architecture does not match the '
                    'persisted run manifest.'
                )
        environment = oci_runtime_environment(
            job,
            plan,
            architecture,
            logical_cpu_count,
        )
        event(
            job,
            'Connect',
            f'OCI CPU topology recorded: {architecture}, '
            f'{logical_cpu_count} logical CPUs across {plan.ocpus:g} OCPUs.',
        )

    commands = amazon_linux.benchmark_commands(
        logical_cpu_count or 1,
        storage_directory=storage_directory,
    )
    parser_by_id = {
        'sysbench_cpu': amazon_linux.parse_sysbench_cpu_output,
        'sysbench_memory': amazon_linux.parse_sysbench_memory_output,
        'fio': amazon_linux.parse_fio_output,
        'sysbench_fileio': amazon_linux.parse_sysbench_fileio_output,
    }
    for key in (
        'sysbench_cpu',
        'sysbench_memory',
        'stream',
        'fio',
        'sysbench_fileio',
    ):
        if key not in selected:
            continue
        name, command = commands[key]
        parser = parser_by_id.get(key)
        if key == 'stream':
            parser = lambda output: amazon_linux.parse_stream_output(
                output,
                expected_threads=logical_cpu_count,
            )
        execute_benchmark(
            job,
            key,
            name,
            command,
            parser=parser,
            metadata=(
                {**environment, **(storage_metadata or {})}
                if key in STORAGE_BENCHMARK_RESULT_IDS
                else environment
            ),
            output_limit=None if key == 'fio' else 20000,
        )
    phoronix_failures = (
        run_phoronix_profiles(job, plan)
        if 'phoronix' in selected
        else ()
    )
    network_selected = expanded_iperf3_result_ids(plan)
    network_commands = network_benchmark_commands(job, network_selected)
    for key in network_selected:
        name, command = network_commands[key]
        protocol = key.removeprefix('iperf_')
        event(
            job,
            'Network',
            f'Targeting private peer {job["resources"]["peer_private_ip"]}.',
        )
        execute_benchmark(
            job,
            key,
            name,
            command,
            parser=lambda output, expected=protocol: parse_iperf3_output(
                output,
                expected_protocol=expected,
            ),
            output_limit=None,
        )
    if 'llama_bench' in plan.llm_benchmarks:
        run_llama_benchmark(
            job,
            metadata=oci_runtime_environment(job, plan),
            toolset_enable=LLAMA_TOOLSET_ENABLE,
        )
    if 'apachebench' in plan.benchmarks:
        apachebench_failures = run_apachebench(job, plan)
    if 'deathstarbench' in plan.benchmarks:
        run_deathstarbench(job, plan)
    deferred_failures = (*phoronix_failures, *apachebench_failures)
    if deferred_failures:
        raise RuntimeError(
            'One or more independently selected benchmark workloads failed '
            'after the remaining workloads were attempted: '
            + ' | '.join(deferred_failures)
        )


def network_benchmark_commands(job, selected):
    if not selected:
        return {}
    peer_private_ip = job.get('resources', {}).get('peer_private_ip')
    if not peer_private_ip:
        raise RuntimeError(
            'A private peer IP was not created for the selected network benchmark.'
        )
    return {
        'iperf_tcp': (
            'iperf3 — TCP',
            f'iperf3 -4 -c {peer_private_ip} -t 60 -P 4 -J',
        ),
        'iperf_udp': (
            'iperf3 — UDP',
            f'iperf3 -4 -c {peer_private_ip} -u -b 0 -t 60 -J',
        ),
        'iperf_sctp': (
            'iperf3 — SCTP',
            f'iperf3 -4 -c {peer_private_ip} --sctp -t 60 -P 4 -J',
        ),
    }

def report_value_table(title, values):
    if not values:
        return ''
    rows = ''.join(
        '<tr>'
        f'<th>{html.escape(str(key).replace("_", " ").title())}</th>'
        f'<td>{html.escape(str(value))}</td>'
        '</tr>'
        for key, value in values.items()
    )
    return f'<h3>{html.escape(title)}</h3><table><tbody>{rows}</tbody></table>'


def report_result_section(result):
    setup = ''
    if result.get('setup_output'):
        setup = (
            '<details><summary>Setup and deployment diagnostics</summary>'
            f'<pre>{html.escape(result["setup_output"])}</pre></details>'
        )
    return (
        f'<section><h2>{html.escape(result["name"])}</h2>'
        f'<p>Started {html.escape(result.get("started_at", "unknown"))} · '
        f'{result.get("duration_seconds", "unknown")} seconds</p>'
        f'<p>Status: <strong>{html.escape(result.get("status", "completed"))}'
        f'</strong>{(" · " + html.escape(result["error"])) if result.get("error") else ""}</p>'
        f'{report_value_table("Environment", result.get("metadata"))}'
        f'{report_value_table("Measured results", result.get("metrics"))}'
        f'<h3>Command</h3><p><code>{html.escape(result.get("command", ""))}</code></p>'
        f'<h3>Raw benchmark output</h3>'
        f'<pre>{html.escape(result.get("output", ""))}</pre>{setup}</section>'
    )


def embedded_results_json(document):
    """Serialize report data without allowing an HTML script boundary."""
    return (
        json.dumps(
            document,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        )
        .replace('&', r'\u0026')
        .replace('<', r'\u003c')
        .replace('>', r'\u003e')
    )


def make_report(job):
    directory = RUNS / job['id']; directory.mkdir(exist_ok=True)
    if job.get('_persist_results_artifact', True):
        artifact_job = results_artifact_job(job)
        artifact = build_results_artifact(artifact_job)
        write_results_artifact(directory, artifact_job)
    else:
        try:
            artifact = load_results_document(directory)
        except (FileNotFoundError, ValueError):
            # Recovered lifecycle state contains summary rows only. It can be
            # rendered for diagnostics, but must not replace a rich artifact.
            artifact = build_results_artifact(results_artifact_job(job))
    embedded_artifact = embedded_results_json(artifact)
    body = ''.join(report_result_section(result) for result in job['results'])
    plan = html.escape(json.dumps(job['plan'], indent=2))
    provider_name = str(job.get('plan', {}).get('provider', 'oci')).upper()
    report_html = f'''<!doctype html><html><head><meta charset="utf-8"><title>{provider_name} Benchmark {job['id']}</title><style>body{{font:16px system-ui;max-width:1100px;margin:40px auto;padding:0 20px;color:#172033}}pre{{white-space:pre-wrap;background:#101828;color:#d0d5dd;padding:16px;border-radius:8px;overflow:auto}}code{{font-size:12px;overflow-wrap:anywhere}}section{{border-top:1px solid #ddd;padding:18px 0}}table{{border-collapse:collapse;width:100%;margin:8px 0 20px}}th,td{{border:1px solid #d0d5dd;padding:8px 10px;text-align:left;vertical-align:top;white-space:pre-wrap}}th{{width:34%;background:#f2f4f7}}summary{{cursor:pointer;font-weight:650;margin:16px 0}}</style></head><body><h1>{provider_name} Compute Benchmark Report</h1><p>Run {job['id']} · {job['created_at']}</p><h2>Configuration</h2><pre>{plan}</pre>{body}<script id="benchmark-results" type="application/json">{embedded_artifact}</script></body></html>'''
    (directory / 'plan.json').write_text(json.dumps(job['plan'], indent=2))
    (directory / 'report.html').write_text(report_html)
    event(job, 'Report', 'Report is ready for download.')

def wait_for_deletion(client, get_operation, resource_id):
    try:
        response = get_operation(resource_id)
    except oci.exceptions.ServiceError as exc:
        if exc.status == 404:
            return
        raise
    oci.wait_until(
        client,
        response,
        'lifecycle_state',
        'TERMINATED',
        succeed_on_not_found=True,
        max_interval_seconds=15,
        max_wait_seconds=600,
    )

def delete_resource(
    job,
    client,
    delete_operation,
    get_operation,
    resource_id,
    label,
    max_attempts=8,
    retry_delay_seconds=5,
):
    for attempt in range(1, max_attempts + 1):
        try:
            delete_operation(resource_id)
            wait_for_deletion(client, get_operation, resource_id)
            event(job, 'Destroy', f'Deleted {label}.')
            return
        except oci.exceptions.ServiceError as exc:
            if exc.status == 404:
                event(job, 'Destroy', f'{label.capitalize()} was already deleted.')
                return
            if exc.status == 409 and attempt < max_attempts:
                delay = min(retry_delay_seconds * attempt, 30)
                event(
                    job,
                    'Destroy',
                    f'{label.capitalize()} still has an OCI dependency; '
                    f'retrying in {delay} seconds ({attempt}/{max_attempts}).',
                )
                time.sleep(delay)
                continue
            message = getattr(exc, 'message', str(exc))
            raise RuntimeError(f'Unable to delete {label}: {message}') from exc

    raise RuntimeError(f'Unable to delete {label} after {max_attempts} attempts.')


def clear_route_table_rules(
    job,
    network,
    route_table_id,
    label,
    max_attempts=8,
    retry_delay_seconds=5,
):
    for attempt in range(1, max_attempts + 1):
        try:
            network.update_route_table(
                route_table_id,
                oci.core.models.UpdateRouteTableDetails(route_rules=[]),
            )
            response = network.get_route_table(route_table_id)
            if getattr(response.data, 'lifecycle_state', None) != 'AVAILABLE':
                oci.wait_until(
                    network,
                    response,
                    'lifecycle_state',
                    'AVAILABLE',
                    max_interval_seconds=15,
                    max_wait_seconds=300,
                )
            event(job, 'Destroy', f'Removed route rules from {label}.')
            return
        except oci.exceptions.ServiceError as exc:
            if exc.status == 404:
                return
            if exc.status == 409 and attempt < max_attempts:
                delay = min(retry_delay_seconds * attempt, 30)
                event(
                    job,
                    'Destroy',
                    f'{label.capitalize()} is still updating; retrying route-rule '
                    f'removal in {delay} seconds ({attempt}/{max_attempts}).',
                )
                time.sleep(delay)
                continue
            message = getattr(exc, 'message', str(exc))
            raise RuntimeError(
                f'Unable to remove route rules from {label}: {message}'
            ) from exc

    raise RuntimeError(
        f'Unable to remove route rules from {label} after {max_attempts} attempts.'
    )


def destroy_with_status(job):
    try:
        destroy_resources(job)
    except Exception as exc:
        job['status'] = 'cleanup_failed'
        job['cleanup_error'] = str(exc)
        event(
            job,
            'Cleanup warning',
            f'Benchmarks completed, but infrastructure cleanup did not finish: {exc}',
        )
    finally:
        persist_job_state(job)


def destroy_with_status_under_lease(job, lease):
    """Keep exclusive persisted-run ownership through the entire cleanup."""
    try:
        destroy_with_status(job)
    finally:
        lease.release()


def destroy_resources(job, preserve_status=False):
    plan = job.get('plan', {})
    return dispatch_provider_operation(
        plan,
        'destroy',
        {
            'oci': lambda: destroy_oci_resources(
                job,
                preserve_status=preserve_status,
            ),
            'aws': lambda: aws_provider.destroy_resources(
                job,
                emit=event,
                persist=persist_job_state,
                preserve_status=preserve_status,
            ),
            'gcp': lambda: gcp_provider.destroy_resources(
                job,
                emit=event,
                persist=persist_job_state,
                preserve_status=preserve_status,
            ),
            'azure': lambda: azure_provider.destroy_resources(
                job,
                emit=event,
                persist=persist_job_state,
                preserve_status=preserve_status,
            ),
        },
    )


def destroy_oci_resources(job, preserve_status=False):
    if not preserve_status: job['status'] = 'destroying'
    event(job, 'Destroy', 'Removing benchmark infrastructure.')
    plan = job['plan']; cfg, compute, network, storage, _ = clients(plan['region']); r = job['resources']
    cleanup_errors = []
    instance_address_keys = {
        'instance_id': ('public_ip', 'private_ip'),
        'loadgen_instance_id': (
            'loadgen_public_ip',
            'loadgen_private_ip',
            'loadgen_shape',
            'loadgen_network_bandwidth_gbps',
        ),
        'peer_instance_id': ('peer_private_ip',),
    }
    for instance_key in (
        'instance_id',
        'loadgen_instance_id',
        'peer_instance_id',
    ):
      if r.get(instance_key):
        try:
            compute.terminate_instance(r[instance_key], preserve_boot_volume=False)
            oci.wait_until(
                compute,
                compute.get_instance(r[instance_key]),
                'lifecycle_state',
                'TERMINATED',
                succeed_on_not_found=True,
            )
            event(job, 'Destroy', f'Terminated {instance_key.replace("_id", "").replace("_", " ")}.')
            r.pop(instance_key, None)
            for address_key in instance_address_keys[instance_key]:
                r.pop(address_key, None)
        except oci.exceptions.ServiceError as exc:
            if exc.status == 404:
                r.pop(instance_key, None)
                for address_key in instance_address_keys[instance_key]:
                    r.pop(address_key, None)
            else:
                cleanup_errors.append(
                    f'Unable to terminate {instance_key}: '
                    f'{getattr(exc, "message", str(exc))}'
                )
        except Exception as exc:
            cleanup_errors.append(f'Unable to terminate {instance_key}: {exc}')
    if r.get('volume_id'):
        try:
            delete_resource(
                job,
                storage,
                storage.delete_volume,
                storage.get_volume,
                r['volume_id'],
                'data volume',
            )
        except Exception as exc:
            cleanup_errors.append(str(exc))
    # Subnets must be gone before their security lists and route tables.
    subnet_resources = [
        ('peer_subnet_id', network.delete_subnet, network.get_subnet, 'peer subnet'),
        ('subnet_id', network.delete_subnet, network.get_subnet, 'public subnet'),
    ]
    for name, delete_operation, get_operation, label in subnet_resources:
        if r.get(name):
            try:
                delete_resource(
                    job,
                    network,
                    delete_operation,
                    get_operation,
                    r[name],
                    label,
                )
            except Exception as exc:
                cleanup_errors.append(str(exc))

    # OCI will not delete a gateway while a route rule targets it. Remove the
    # rules explicitly before deleting either the custom tables or gateways.
    for name, label in (
        ('peer_route_table_id', 'peer route table'),
        ('route_table_id', 'public route table'),
    ):
        if r.get(name):
            try:
                clear_route_table_rules(job, network, r[name], label)
            except Exception as exc:
                cleanup_errors.append(str(exc))

    network_resources = [
        ('security_list_id', network.delete_security_list, network.get_security_list, 'security list'),
        ('peer_route_table_id', network.delete_route_table, network.get_route_table, 'peer route table'),
        ('route_table_id', network.delete_route_table, network.get_route_table, 'public route table'),
        ('nat_id', network.delete_nat_gateway, network.get_nat_gateway, 'NAT gateway'),
        ('igw_id', network.delete_internet_gateway, network.get_internet_gateway, 'internet gateway'),
        ('vcn_id', network.delete_vcn, network.get_vcn, 'VCN'),
    ]
    for name, delete_operation, get_operation, label in network_resources:
        if r.get(name):
            try:
                delete_resource(
                    job,
                    network,
                    delete_operation,
                    get_operation,
                    r[name],
                    label,
                )
            except Exception as exc:
                cleanup_errors.append(str(exc))
    if cleanup_errors:
        raise RuntimeError('; '.join(cleanup_errors))
    if not preserve_status:
        job['status'] = 'destroyed'
        job['cleanup_error'] = None
        event(job, 'Complete', 'Infrastructure was destroyed.')
