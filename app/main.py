import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import time
import uuid
import base64
import html
import hmac
import re
from ipaddress import ip_address
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

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
from .models import BenchmarkPlan, canonicalize_benchmark_plan
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
app = FastAPI(title='OCI Self-Service Benchmarks')
app.mount('/static', StaticFiles(directory=ROOT / 'static'), name='static')
jobs: dict[str, dict[str, Any]] = {}
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
IPERF3_RESULT_IDS = {
    'tcp': 'iperf_tcp',
    'udp': 'iperf_udp',
    'sctp': 'iperf_sctp',
}


class SSHCommandError(RuntimeError):
    def __init__(self, message, output='', returncode=None):
        super().__init__(message)
        self.output = output
        self.returncode = returncode


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
def fail(job, message):
    job['status'] = 'failed'; job['error'] = message; event(job, 'Failed', message)


def benchmark_status(job):
    statuses = [result.get('status') for result in job.get('results', [])]
    if 'failed' in statuses:
        return 'failed'
    if statuses and all(status == 'completed' for status in statuses):
        return 'complete'
    return 'pending'


def saved_report_benchmark_status(report_path):
    """Recover benchmark outcome for reports created before state.json existed."""
    report = report_path.read_text(errors='replace')
    if '<p>Status: <strong>failed</strong></p>' in report:
        return 'failed'
    if '<p>Status: <strong>completed</strong></p>' in report:
        return 'complete'
    return 'unknown'


def normalize_saved_job_state(saved):
    """Upgrade states written before benchmark and lifecycle outcomes were split."""
    if (
        saved.get('status') == 'failed'
        and saved.get('benchmark_status') == 'complete'
    ):
        saved['lifecycle_warning'] = saved.get('error')
        saved['error'] = None
        cleanup_warnings = [
            item.get('message')
            for item in saved.get('events', [])
            if item.get('stage') == 'Cleanup warning' and item.get('message')
        ]
        if cleanup_warnings:
            saved['status'] = 'cleanup_failed'
            saved['cleanup_error'] = (
                saved.get('cleanup_error') or cleanup_warnings[-1]
            )
        else:
            saved['status'] = 'reported'
    return saved


def persist_job_state(job):
    directory = RUNS / job['id']
    if not (directory / 'report.html').exists():
        return
    state = {
        'id': job['id'],
        'status': job['status'],
        'error': job.get('error'),
        'cleanup_error': job.get('cleanup_error'),
        'benchmark_status': benchmark_status(job),
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
    (directory / 'state.json').write_text(json.dumps(state, indent=2))


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
    private_value = settings.get('OCI_BENCHMARK_SSH_PRIVATE_KEY_FILE')
    public_value = settings.get('OCI_BENCHMARK_SSH_PUBLIC_KEY_FILE')
    if not private_value and not public_value:
        return {'configured': False}
    if not private_value or not public_value:
        return {
            'configured': False,
            'error': (
                'Set both OCI_BENCHMARK_SSH_PRIVATE_KEY_FILE and '
                'OCI_BENCHMARK_SSH_PUBLIC_KEY_FILE in .env.'
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
    try:
        return bool(request.client and ip_address(request.client.host).is_loopback)
    except ValueError:
        return False


def read_json_file(path, default):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return default


def report_summary(report_path):
    directory = report_path.parent
    plan = read_json_file(directory / 'plan.json', {})
    state = read_json_file(directory / 'state.json', {})
    if state:
        state = normalize_saved_job_state(state)
        benchmark_outcome = state.get('benchmark_status', 'unknown')
        status = state.get('status', 'reported')
        created_at = next(
            (
                item.get('at')
                for item in state.get('events', [])
                if item.get('at')
            ),
            None,
        )
        result_names = [
            item.get('name')
            for item in state.get('results', [])
            if item.get('name')
        ]
    else:
        benchmark_outcome = saved_report_benchmark_status(report_path)
        status = 'failed' if benchmark_outcome == 'failed' else 'reported'
        created_at = None
        result_names = []
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
    benchmark_names = result_names or [
        catalog_names.get(item, item) for item in selected_ids
    ]
    modified_at = datetime.fromtimestamp(
        report_path.stat().st_mtime,
        tz=timezone.utc,
    ).isoformat()
    return {
        'id': directory.name,
        'created_at': created_at or modified_at,
        'modified_at': modified_at,
        'status': status,
        'benchmark_status': benchmark_outcome,
        'region': plan.get('region'),
        'availability_domain': plan.get('availability_domain'),
        'shape': plan.get('shape'),
        'ocpus': plan.get('ocpus'),
        'memory_gb': plan.get('memory_gb'),
        'benchmarks': benchmark_names,
    }


def saved_report_summaries():
    reports = [report_summary(path) for path in RUNS.glob('*/report.html')]
    return sorted(reports, key=lambda item: item['created_at'], reverse=True)

@app.get('/')
def home(): return FileResponse(ROOT / 'static' / 'index.html')


@app.get('/history')
def history(): return FileResponse(ROOT / 'static' / 'history.html')


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


@app.get('/api/reports')
def reports():
    return JSONResponse(
        {'items': saved_report_summaries()},
        headers={'Cache-Control': 'no-store'},
    )

@app.get('/api/reports/latest')
def latest_report():
    reports = list(RUNS.glob('*/report.html'))
    if not reports:
        raise HTTPException(404, 'No reports are available')
    latest = max(reports, key=lambda path: path.stat().st_mtime)
    return {'id': latest.parent.name}

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

@app.post('/api/jobs')
async def create_job(plan: BenchmarkPlan):
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
    job_id = uuid.uuid4().hex[:12]
    job = {
        'id': job_id,
        'plan': plan.model_dump(
            exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'}
        ),
        '_key': normalize_private_key(plan.ssh_private_key),
        '_passphrase': plan.ssh_key_passphrase,
        '_public_key': uploaded_public_key,
        'status': 'queued',
        'events': [],
        'resources': {},
        'results': [],
        'created_at': now(),
        'updated_at': now(),
    }
    jobs[job_id] = job
    event(job, 'Queued', 'Plan accepted. Waiting to provision OCI resources.')
    asyncio.create_task(run_job(job, plan))
    return {'id': job_id}

@app.get('/api/jobs/{job_id}')
def job_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        report_path = RUNS / job_id / 'report.html'
        if report_path.exists():
            state_path = RUNS / job_id / 'state.json'
            if state_path.exists():
                saved = normalize_saved_job_state(
                    json.loads(state_path.read_text())
                )
                saved.setdefault(
                    'plan',
                    read_json_file(report_path.parent / 'plan.json', {}),
                )
                saved['report_ready'] = True
                saved['live'] = False
                return saved
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
    return safe

@app.get('/api/jobs/{job_id}/report')
def report(job_id: str, download: bool = False):
    path = RUNS / job_id / 'report.html'
    if not path.exists(): raise HTTPException(404, 'Report not ready')
    if download:
        return FileResponse(
            path,
            media_type='text/html',
            filename=f'oci-benchmark-{job_id}.html',
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

@app.post('/api/jobs/{job_id}/destroy')
async def destroy(job_id: str):
    job = jobs.get(job_id)
    if not job: raise HTTPException(404, 'Job not found')
    if job['status'] in ('destroying', 'destroyed'): return {'status': job['status']}
    asyncio.create_task(asyncio.to_thread(destroy_with_status, job))
    return {'status': 'destroying'}

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

async def run_job(job, plan):
    try:
        job['status'] = 'provisioning'; event(job, 'Provision', 'Creating VCN, gateway, subnet, and security rules.')
        await asyncio.to_thread(provision, job, plan)
        job['status'] = 'testing'; event(job, 'Connect', 'Instance is ready; connecting with SSH.')
        await asyncio.to_thread(run_benchmarks, job, plan)
        job['status'] = 'reporting'; event(job, 'Report', 'Generating standalone HTML report.')
        await asyncio.to_thread(make_report, job)
        if plan.destroy_after_completion:
            await asyncio.to_thread(destroy_with_status, job)
        else:
            job['status'] = 'complete'; event(job, 'Complete', 'Benchmarks and report are ready. Infrastructure has been retained.')
    except Exception as exc:
        fail(job, str(exc))
        if plan.destroy_after_completion:
            job['status'] = 'cleanup_pending'
            event(
                job,
                'Cleanup',
                'The run failed; saving any partial results before automatic '
                'infrastructure cleanup.',
            )
        if job['results']:
            try:
                await asyncio.to_thread(make_report, job)
            except Exception as report_error:
                event(job, 'Report warning', str(report_error))
        if plan.destroy_after_completion:
            try:
                await asyncio.to_thread(destroy_with_status, job)
            except Exception as cleanup:
                event(job, 'Cleanup warning', str(cleanup))
        elif job['resources'].get('public_ip'):
            event(
                job,
                'Retained',
                f'Infrastructure retained after failure. Connect with: ssh -i '
                f'/path/to/private-key opc@{job["resources"]["public_ip"]}',
            )
    finally:
        job.pop('_key', None)
        job.pop('_passphrase', None)
        job.pop('_public_key', None)
        persist_job_state(job)


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
    cfg, compute, network, storage, identity = clients(plan.region)
    iperf3_protocols = selected_iperf3_protocols(plan)
    uses_load_generator = plan_uses_load_generator(plan)
    compartment = plan.compartment_id or cfg['tenancy']; suffix = job['id']; tags = {'oci-benchmark-job': suffix, 'managed-by': 'oci-self-service-benchmarks'}
    ad = plan.availability_domain or identity.list_availability_domains(compartment).data[0].name
    vcn = network.create_vcn(oci.core.models.CreateVcnDetails(compartment_id=compartment, cidr_block='10.42.0.0/16', dns_label=f'b{suffix}', display_name=f'benchmark-{suffix}', freeform_tags=tags)).data
    job['resources']['vcn_id'] = vcn.id
    oci.wait_until(network, network.get_vcn(vcn.id), 'lifecycle_state', 'AVAILABLE')
    vcn = network.get_vcn(vcn.id).data
    igw = network.create_internet_gateway(oci.core.models.CreateInternetGatewayDetails(compartment_id=compartment, vcn_id=vcn.id, is_enabled=True, display_name=f'benchmark-igw-{suffix}', freeform_tags=tags)).data; job['resources']['igw_id'] = igw.id
    nat = network.create_nat_gateway(oci.core.models.CreateNatGatewayDetails(compartment_id=compartment, vcn_id=vcn.id, display_name=f'benchmark-nat-{suffix}', freeform_tags=tags)).data; job['resources']['nat_id'] = nat.id
    rt = network.create_route_table(oci.core.models.CreateRouteTableDetails(compartment_id=compartment, vcn_id=vcn.id, display_name=f'benchmark-routes-{suffix}', route_rules=[oci.core.models.RouteRule(destination='0.0.0.0/0', destination_type='CIDR_BLOCK', network_entity_id=igw.id)], freeform_tags=tags)).data; job['resources']['route_table_id'] = rt.id
    sl = network.create_security_list(oci.core.models.CreateSecurityListDetails(compartment_id=compartment, vcn_id=vcn.id, display_name=f'benchmark-security-{suffix}', ingress_security_rules=benchmark_security_rules(include_deathstarbench='deathstarbench' in plan.benchmarks, iperf3_protocols=iperf3_protocols, include_apachebench='apachebench' in plan.benchmarks), egress_security_rules=[oci.core.models.EgressSecurityRule(protocol='all', destination='0.0.0.0/0')], freeform_tags=tags)).data; job['resources']['security_list_id'] = sl.id
    subnet = network.create_subnet(oci.core.models.CreateSubnetDetails(compartment_id=compartment, vcn_id=vcn.id, cidr_block='10.42.1.0/24', dns_label='public', display_name=f'benchmark-public-{suffix}', route_table_id=rt.id, dhcp_options_id=vcn.default_dhcp_options_id, security_list_ids=[sl.id], prohibit_public_ip_on_vnic=False, freeform_tags=tags)).data; job['resources']['subnet_id'] = subnet.id
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
    available_shapes = compute.list_shapes(
        compartment,
        availability_domain=ad,
    ).data
    listed_shape = next(x for x in available_shapes if x.shape == plan.shape)
    shape_config = oci.core.models.LaunchInstanceShapeConfigDetails(ocpus=plan.ocpus, memory_in_gbs=plan.memory_gb) if (listed_shape.ocpu_options or listed_shape.memory_options) else None
    source = oci.core.models.InstanceSourceViaImageDetails(source_type='image', image_id=image.id, boot_volume_size_in_gbs=plan.storage.boot_size_gb, boot_volume_vpus_per_gb=plan.storage.boot_performance)
    launch_options = oci.core.models.LaunchOptions(network_type='VFIO' if plan.networking == 'sriov' else 'PARAVIRTUALIZED')
    launch = oci.core.models.LaunchInstanceDetails(compartment_id=compartment, availability_domain=ad, fault_domain=plan.fault_domain, shape=plan.shape, shape_config=shape_config, launch_options=launch_options, platform_config=platform_config(plan), display_name=f'benchmark-{suffix}', metadata={'ssh_authorized_keys': job['_public_key']}, source_details=source, create_vnic_details=oci.core.models.CreateVnicDetails(subnet_id=subnet.id, assign_public_ip=True, assign_private_dns_record=True, hostname_label='runner', skip_source_dest_check=False), freeform_tags=tags)
    instance = compute.launch_instance(launch).data; job['resources']['instance_id'] = instance.id
    event(job, 'Provision', f'Launched {plan.shape}; waiting for it to become RUNNING.')
    oci.wait_until(compute, compute.get_instance(instance.id), 'lifecycle_state', 'RUNNING')
    vnic_attachment = compute.list_vnic_attachments(
        compartment,
        instance_id=instance.id,
    ).data[0]
    runner_vnic = network.get_vnic(vnic_attachment.vnic_id).data
    pub = runner_vnic.public_ip
    job['resources']['public_ip'] = pub
    job['resources']['private_ip'] = runner_vnic.private_ip
    if plan.storage.additional_volume:
        vol = storage.create_volume(oci.core.models.CreateVolumeDetails(compartment_id=compartment, availability_domain=ad, size_in_gbs=plan.storage.additional_size_gb, vpus_per_gb=plan.storage.additional_performance, display_name=f'benchmark-data-{suffix}', freeform_tags=tags)).data; job['resources']['volume_id'] = vol.id
        oci.wait_until(storage, storage.get_volume(vol.id), 'lifecycle_state', 'AVAILABLE')
        if plan.storage.mount_style == 'iscsi':
            attach_details = oci.core.models.AttachIScsiVolumeDetails(
                type='iscsi',
                instance_id=instance.id,
                volume_id=vol.id,
                is_shareable=False,
                use_chap=False,
                is_agent_auto_iscsi_login_enabled=True,
            )
        else:
            attach_details = oci.core.models.AttachParavirtualizedVolumeDetails(
                type='paravirtualized',
                instance_id=instance.id,
                volume_id=vol.id,
                is_shareable=False,
            )
        attachment = compute.attach_volume(attach_details).data
        job['resources']['volume_attachment_id'] = attachment.id
        event(job, 'Provision', 'Waiting for the /data volume attachment.')
        oci.wait_until(
            compute,
            compute.get_volume_attachment(attachment.id),
            'lifecycle_state',
            'ATTACHED',
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
        job['resources']['loadgen_instance_id'] = loadgen.id
        job['resources']['loadgen_shape'] = loadgen_shape.shape
        job['resources']['loadgen_network_bandwidth_gbps'] = (
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
        job['resources']['loadgen_public_ip'] = loadgen_vnic.public_ip
        job['resources']['loadgen_private_ip'] = loadgen_vnic.private_ip

    if iperf3_protocols:
        event(
            job,
            'Provision',
            'Creating a NAT-backed private peer for the selected iperf3 '
            f'protocols: {", ".join(protocol.upper() for protocol in iperf3_protocols)}.',
        )
        peer_rt = network.create_route_table(oci.core.models.CreateRouteTableDetails(compartment_id=compartment, vcn_id=vcn.id, display_name=f'benchmark-peer-routes-{suffix}', route_rules=[oci.core.models.RouteRule(destination='0.0.0.0/0', destination_type='CIDR_BLOCK', network_entity_id=nat.id)], freeform_tags=tags)).data
        job['resources']['peer_route_table_id'] = peer_rt.id
        peer_subnet = network.create_subnet(oci.core.models.CreateSubnetDetails(compartment_id=compartment, vcn_id=vcn.id, cidr_block='10.42.2.0/24', dns_label='peer', display_name=f'benchmark-peer-{suffix}', route_table_id=peer_rt.id, dhcp_options_id=vcn.default_dhcp_options_id, security_list_ids=[sl.id], prohibit_public_ip_on_vnic=True, freeform_tags=tags)).data
        job['resources']['peer_subnet_id'] = peer_subnet.id
        peer_image = latest_oracle_linux_image(
            compute,
            compartment,
            'VM.Standard.E5.Flex',
            require_ol9=True,
        )
        peer_script = iperf_peer_cloud_init(iperf3_protocols)
        peer_launch = oci.core.models.LaunchInstanceDetails(compartment_id=compartment, availability_domain=ad, shape='VM.Standard.E5.Flex', shape_config=oci.core.models.LaunchInstanceShapeConfigDetails(ocpus=1, memory_in_gbs=8), display_name=f'benchmark-peer-{suffix}', metadata={'user_data': base64.b64encode(peer_script.encode()).decode()}, source_details=oci.core.models.InstanceSourceViaImageDetails(source_type='image', image_id=peer_image.id, boot_volume_size_in_gbs=50), create_vnic_details=oci.core.models.CreateVnicDetails(subnet_id=peer_subnet.id, assign_public_ip=False, assign_private_dns_record=True, hostname_label='peer'), freeform_tags=tags)
        peer = compute.launch_instance(peer_launch).data; job['resources']['peer_instance_id'] = peer.id
        oci.wait_until(compute, compute.get_instance(peer.id), 'lifecycle_state', 'RUNNING')
        peer_attachment = compute.list_vnic_attachments(compartment, instance_id=peer.id).data[0]
        job['resources']['peer_private_ip'] = network.get_vnic(peer_attachment.vnic_id).data.private_ip
    event(job, 'Provision', f'Infrastructure ready. Public IP: {pub}')

def ssh(
    job,
    command,
    timeout=1800,
    host_key='public_ip',
    include_stderr=False,
):
    target = job.get('resources', {}).get(host_key)
    if not target:
        raise RuntimeError(f'SSH target {host_key!r} is not available for this run.')
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
    args = [
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'ConnectTimeout=15',
        '-o', 'NumberOfPasswordPrompts=1',
        '-i', path,
        f'opc@{target}',
        command,
    ]
    try:
        for attempt in range(12):
            try:
                result = subprocess.run(
                    args,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    env=ssh_env,
                )
            except subprocess.TimeoutExpired as exc:
                stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else exc.stdout
                stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else exc.stderr
                output = '\n'.join(
                    part.strip()
                    for part in (stdout, stderr)
                    if part and part.strip()
                )
                raise SSHCommandError(
                    f'SSH command timed out after {timeout} seconds.',
                    output=output,
                ) from exc
            if result.returncode == 0:
                if include_stderr:
                    return '\n'.join(
                        part.strip()
                        for part in (result.stdout, result.stderr)
                        if part and part.strip()
                    )
                return result.stdout
            if result.returncode != 255 or attempt == 11:
                output = '\n'.join(
                    part.strip()
                    for part in (result.stdout, result.stderr)
                    if part and part.strip()
                )
                raise SSHCommandError(
                    f'SSH command failed: {output[-2000:]}',
                    output=output,
                    returncode=result.returncode,
                )
            time.sleep(10)
        raise RuntimeError('SSH connection failed.')
    finally:
        os.unlink(path)
        if askpass_path:
            os.unlink(askpass_path)

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


def iperf_peer_readiness_command(peer_private_ip, protocols=('tcp',)):
    peer_private_ip = str(ip_address(peer_private_ip))
    sctp_probe = ''
    if 'sctp' in protocols:
        sctp_probe = (
            'for attempt in $(seq 1 6); do '
            'if timeout 20 iperf3 -c "$PEER_IP" --sctp -t 1 -J '
            '>/tmp/iperf3-sctp-readiness.json 2>&1; then '
            'echo "iperf3 SCTP data path is ready."; '
            'rm -f /tmp/iperf3-sctp-readiness.json; exit 0; fi; '
            'sleep 2; done; '
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
        'sleep 5; '
        'done; '
        'if [ "$CONTROL_READY" != true ]; then '
        'echo "The private iperf3 peer did not open TCP control port 5201 '
        'after 300 seconds." >&2; exit 1; fi; '
        'echo "iperf3 TCP control connection at $PEER_IP:5201 is ready."; '
        f'{sctp_probe}'
    )


def wait_for_iperf_peer(job, protocols=('tcp',)):
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
        iperf_peer_readiness_command(peer_private_ip, protocols),
        timeout=360,
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
        packages.update({'git', 'gcc', 'make'})
    if {'fio'} & selected:
        packages.add('fio')
    if iperf3_protocols:
        packages.add('iperf3')
    if 'sctp' in iperf3_protocols:
        packages.add('lksctp-tools')
    if phoronix_profiles:
        packages.update(phoronix_required_packages(phoronix_profiles))
    if plan.llm_benchmarks:
        packages.update({
            'git',
            'make',
            'cmake',
            'curl',
            LLAMA_TOOLSET_PACKAGE,
        })
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
            'Enabling Oracle Linux 9 Developer EPEL and installing sysbench.',
        )
        dnf_install(job, {'dnf-plugins-core', 'oracle-epel-release-el9'})
        ssh(
            job,
            'sudo dnf config-manager --enable ol9_developer_EPEL '
            '&& sudo dnf -y --disablerepo=ol9_ksplice '
            '--setopt=retries=10 --setopt=timeout=30 install sysbench',
        )

def mount_data_volume(job):
    event(job, 'Storage', 'Discovering, formatting, and mounting the data volume.')
    command = (
        'sudo mkdir -p /data; '
        'DATA_DEVICE=""; '
        'for attempt in $(seq 1 36); do '
        'for device in $(lsblk -dnpo NAME,TYPE | '
        """awk '$2=="disk" {print $1}'); do """
        'if ! lsblk -npo MOUNTPOINT "$device" | grep -q "^/$"; then '
        'DATA_DEVICE="$device"; break; '
        'fi; '
        'done; '
        'test -n "$DATA_DEVICE" && break; '
        'sleep 5; '
        'done; '
        'test -b "$DATA_DEVICE" || '
        '{ echo "Attached data volume did not appear in the guest." >&2; exit 1; }; '
        'echo "Using data device $DATA_DEVICE"; '
        'sudo mkfs.xfs -f "$DATA_DEVICE"; '
        'sudo mount "$DATA_DEVICE" /data; '
        'sudo chmod 777 /data; '
        'findmnt /data'
    )
    output = ssh(job, command, timeout=300)
    event(job, 'Storage', f'Data volume mounted successfully: {output.strip()}')

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
):
    started_at = now()
    started = time.monotonic()
    event(job, 'Run', f'Started {name}.')
    try:
        output = ssh(
            job,
            command,
            timeout=timeout,
            host_key=host_key,
            include_stderr=include_stderr,
        )
    except SSHCommandError as exc:
        duration = round(time.monotonic() - started, 2)
        if exc.output.strip():
            job['results'].append({
                'id': benchmark_id,
                'name': name,
                'command': command,
                'started_at': started_at,
                'duration_seconds': duration,
                'status': 'failed',
                'error': str(exc),
                'output': (
                    exc.output
                    if output_limit is None
                    else exc.output[-output_limit:]
                ),
                **({'metadata': metadata} if metadata else {}),
            })
        raise RuntimeError(f'{name} failed. {exc}') from exc
    duration = round(time.monotonic() - started, 2)
    if not output.strip():
        raise RuntimeError(
            f'{name} exited without producing benchmark output.'
        )
    try:
        metrics = parser(output) if parser else None
    except ValueError as exc:
        job['results'].append({
            'id': benchmark_id,
            'name': name,
            'command': command,
            'started_at': started_at,
            'duration_seconds': duration,
            'status': 'failed',
            'error': str(exc),
            'output': output if output_limit is None else output[-output_limit:],
            **({'metadata': metadata} if metadata else {}),
        })
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
            job['results'].append({
                'id': run.benchmark_id,
                'name': run.name,
                'command': run.command,
                'started_at': now(),
                'duration_seconds': 0,
                'status': 'failed',
                'error': f'Phoronix preparation failed. {exc}',
                'output': failure_output,
                'metadata': run.metadata,
            })
        return (f'Phoronix preparation failed. {exc}',)

    failures = []
    for run in runs:
        metadata = {
            **run.metadata,
            'architecture': architecture,
            'shape': plan.shape,
            'ocpus': plan.ocpus,
            'memory_gb': plan.memory_gb,
        }
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
                'output': '',
            })
            failures.append(f'{name}: {error}')
        event(job, 'ApacheBench warning', str(error))
        return tuple(failures)
    httpd_installed = False
    httpd_ready = False

    def stage(label, message, command, host_key='public_ip', timeout=1800):
        event(job, label, message)
        try:
            output = ssh(
                job,
                command,
                timeout=timeout,
                host_key=host_key,
                include_stderr=True,
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
        regional_yum = f'yum.{plan.region}.oci.oraclecloud.com'
        wait_for_guest_readiness(
            job,
            plan.region,
            label='ApacheBench web-server instance',
            required_hosts=[regional_yum],
        )
        wait_for_guest_readiness(
            job,
            plan.region,
            host_key='loadgen_public_ip',
            label='ApacheBench load generator',
            required_hosts=[regional_yum],
        )
        event(
            job,
            'ApacheBench install',
            'Installing Apache HTTP Server on the target and ApacheBench on '
            'the separate load-generator VM.',
        )
        dnf_install(job, {'firewalld', 'httpd'})
        httpd_installed = True
        dnf_install(
            job,
            {'httpd-tools', 'time'},
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
        service_architecture = stage(
            'ApacheBench architecture',
            'Recording the web-server VM architecture.',
            'uname -m',
        ).strip().splitlines()[-1]
        loadgen_architecture = stage(
            'ApacheBench architecture',
            'Recording the load-generator VM architecture.',
            'uname -m',
            host_key='loadgen_public_ip',
        ).strip().splitlines()[-1]
        stage(
            'ApacheBench readiness',
            'Verifying HTTP 200 and the configured response size over the '
            'private VCN path.',
            apachebench_readiness_command(
                target_private_ip,
                options.response_size_kib,
            ),
            host_key='loadgen_public_ip',
            timeout=600,
        )
        common_metadata = {
            'service_shape': plan.shape,
            'service_ocpus': plan.ocpus,
            'service_memory_gb': plan.memory_gb,
            'service_architecture': service_architecture,
            'load_generator_shape': resources.get('loadgen_shape'),
            'load_generator_ocpus': 2,
            'load_generator_memory_gb': 8,
            'load_generator_architecture': loadgen_architecture,
            'load_generator_network_bandwidth_gbps': resources.get(
                'loadgen_network_bandwidth_gbps'
            ),
            'traffic_path': 'OCI private VCN address',
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
    settings = deathstarbench_workload(options.workload)
    resources = job.get('resources', {})
    service_private_ip = resources.get('private_ip')
    loadgen_public_ip = resources.get('loadgen_public_ip')
    if not service_private_ip or not loadgen_public_ip:
        raise RuntimeError(
            'DeathStarBench provisioning did not produce both service and '
            'load-generator addresses.'
        )

    diagnostics = []
    started_at = now()
    lifecycle_started = time.monotonic()

    def stage(label, message, command, host_key='public_ip', timeout=1800):
        event(job, label, message)
        try:
            output = ssh(
                job,
                command,
                timeout=timeout,
                host_key=host_key,
                include_stderr=True,
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
        regional_yum = f'yum.{plan.region}.oci.oraclecloud.com'
        wait_for_guest_readiness(
            job,
            plan.region,
            label='DeathStarBench service instance',
            required_hosts=[
                regional_yum,
                'github.com',
                'registry-1.docker.io',
                'auth.docker.io',
                'archive.ubuntu.com',
                'security.ubuntu.com',
                'ports.ubuntu.com',
                'www.openssl.org',
                'openssl-library.org',
                'openresty.org',
                'luarocks.org',
                'sourceforge.net',
            ],
        )
        wait_for_guest_readiness(
            job,
            plan.region,
            host_key='loadgen_public_ip',
            label='DeathStarBench load generator',
            required_hosts=[regional_yum, 'github.com', 'luarocks.org'],
        )

        event(
            job,
            'DeathStarBench install',
            'Installing the native, daemonless Podman toolchain on the service '
            'instance. Only Podman and podman-compose are invoked; a '
            'Podman-supplied Docker compatibility wrapper may be present but '
            'is never called.',
        )
        dnf_install(
            job,
            {
                'container-tools',
                'curl',
                'git',
                'python3',
                'python3-pyyaml',
            },
        )
        enable_ol9_developer_epel(job)
        dnf_install(job, {'podman-compose'})
        runtime_details = stage(
            'DeathStarBench runtime',
            'Verifying rootful Podman, Netavark DNS, podman-compose, and the '
            'absence of Docker Engine. A verified Podman compatibility wrapper '
            'is permitted but never invoked.',
            podman_runtime_verification_command(),
        )
        stage(
            'DeathStarBench network',
            f'Allowing private-subnet traffic to the {settings["name"]} '
            f'frontend on TCP {settings["port"]}.',
            frontend_firewall_command(options.workload),
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
            deploy_workload_command(options.workload),
            timeout=3600,
        )

        event(
            job,
            'DeathStarBench load generator',
            'Installing wrk2 build and workload-initialization prerequisites '
            'on the separate x86 load-generator VM.',
        )
        enable_ol9_developer_epel(job, host_key='loadgen_public_ip')
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
            host_key='loadgen_public_ip',
        )
        stage(
            'DeathStarBench load generator',
            'Building wrk2 from the pinned upstream source on the separate VM.',
            load_generator_prepare_command(),
            host_key='loadgen_public_ip',
            timeout=1800,
        )
        service_architecture = stage(
            'DeathStarBench architecture',
            'Recording the service VM architecture.',
            'uname -m',
        ).strip().splitlines()[-1]
        loadgen_architecture = stage(
            'DeathStarBench architecture',
            'Recording the load-generator VM architecture.',
            'uname -m',
            host_key='loadgen_public_ip',
        ).strip().splitlines()[-1]
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

        result_metadata = deathstarbench_metadata(
            options.workload,
            options,
            service_architecture,
            loadgen_architecture,
        )
        result_metadata.update({
            'service_shape': plan.shape,
            'service_ocpus': plan.ocpus,
            'service_memory_gb': plan.memory_gb,
            'load_generator_shape': resources.get('loadgen_shape'),
            'load_generator_ocpus': 2,
            'load_generator_memory_gb': 8,
            'container_runtime_details': runtime_details.strip(),
            'traffic_path': 'OCI private VCN address',
        })
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
                'output': failure_output,
            })
        else:
            for result in reversed(job['results']):
                if result.get('id') == 'deathstarbench':
                    result.setdefault('setup_output', '\n\n'.join(diagnostics))
                    break
        raise RuntimeError(f'DeathStarBench failed. {exc}') from exc


def run_benchmarks(job, plan):
    selected = expanded_benchmark_ids(plan)
    apachebench_failures = ()
    readiness_hosts = [f'yum.{plan.region}.oci.oraclecloud.com']
    if {'stream', 'phoronix', 'deathstarbench'} & selected or plan.llm_benchmarks:
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
    if plan.storage.additional_volume:
        mount_data_volume(job)
    commands = {
      'sysbench_cpu': ('Sysbench — CPU', f'sysbench cpu --threads={int(plan.ocpus)} --time=60 run'),
      'sysbench_memory': ('Sysbench — Memory', f'sysbench memory --threads={int(plan.ocpus)} --time=60 --memory-block-size=1M run'),
      'stream': ('STREAM', f'{git_clone_command("https://github.com/jeffhammond/STREAM.git", "/tmp/stream")} && cd /tmp/stream && gcc -O3 -fopenmp stream.c -o stream && OMP_NUM_THREADS=$(nproc) ./stream'),
      'fio': ('fio storage suite', 'for W in read write randread randwrite; do fio --name=$W --directory=/data --rw=$W --bs=$([ "$W" = "read" -o "$W" = "write" ] && echo 1M || echo 4k) --size=4G --direct=1 --time_based --runtime=60 --group_reporting --output-format=json; done'),
      'sysbench_fileio': (
          'Sysbench — File I/O',
          'cd /data '
          '&& sysbench fileio --file-total-size=4G prepare '
          "&& trap 'sysbench fileio --file-total-size=4G cleanup "
          ">/dev/null 2>&1 || true' EXIT "
          '&& sysbench fileio --file-total-size=4G --time=60 '
          '--file-test-mode=rndrw run',
      ),
    }
    for key, (name, command) in commands.items():
        if key in selected:
            execute_benchmark(job, key, name, command)
    phoronix_failures = (
        run_phoronix_profiles(job, plan)
        if 'phoronix' in selected
        else ()
    )
    network_selected = expanded_iperf3_result_ids(plan)
    network_commands = network_benchmark_commands(job, network_selected)
    for key in network_selected:
        name, command = network_commands[key]
        event(
            job,
            'Network',
            f'Targeting private peer {job["resources"]["peer_private_ip"]}.',
        )
        execute_benchmark(job, key, name, command, output_limit=None)
    if 'llama_bench' in plan.llm_benchmarks:
        event(job, 'Run', 'Building and running llama.cpp CPU benchmark.')
        execute_benchmark(
            job,
            'llama_bench',
            'llama.cpp throughput (CPU)',
            llama_benchmark_command(),
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


def make_report(job):
    directory = RUNS / job['id']; directory.mkdir(exist_ok=True)
    body = ''.join(report_result_section(result) for result in job['results'])
    plan = html.escape(json.dumps(job['plan'], indent=2))
    report_html = f'''<!doctype html><html><head><meta charset="utf-8"><title>OCI Benchmark {job['id']}</title><style>body{{font:16px system-ui;max-width:1100px;margin:40px auto;padding:0 20px;color:#172033}}pre{{white-space:pre-wrap;background:#101828;color:#d0d5dd;padding:16px;border-radius:8px;overflow:auto}}code{{font-size:12px;overflow-wrap:anywhere}}section{{border-top:1px solid #ddd;padding:18px 0}}table{{border-collapse:collapse;width:100%;margin:8px 0 20px}}th,td{{border:1px solid #d0d5dd;padding:8px 10px;text-align:left;vertical-align:top;white-space:pre-wrap}}th{{width:34%;background:#f2f4f7}}summary{{cursor:pointer;font-weight:650;margin:16px 0}}</style></head><body><h1>OCI Compute Benchmark Report</h1><p>Run {job['id']} · {job['created_at']}</p><h2>Configuration</h2><pre>{plan}</pre>{body}</body></html>'''
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


def destroy_resources(job, preserve_status=False):
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
