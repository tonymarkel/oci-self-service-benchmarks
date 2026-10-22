"""Pinned hey HTTP/1.1 workloads against the shared Apache static target."""

import re
import shlex
from statistics import fmean, pstdev

from . import apachebench


VERSION = '0.1.4'
WORKLOADS = [
    {
        **item,
        'description': item['description'].replace('HTTP/1.0', 'HTTP/1.1'),
    }
    for item in apachebench.WORKLOADS
]


def workload(mode):
    for item in WORKLOADS:
        if item['id'] == mode:
            return item
    raise ValueError(f'Unknown hey connection mode: {mode}')


def target_prepare_command(response_size_kib, loadgen_private_ip):
    return apachebench.target_prepare_command(response_size_kib, loadgen_private_ip)


def readiness_command(target_ip, response_size_kib):
    return apachebench.readiness_command(target_ip, response_size_kib)


def install_command():
    # Go verifies the versioned module and dependencies against the checksum DB.
    # Build unprivileged; only the resulting executable is installed as root.
    return (
        'set -euo pipefail; BUILD_DIR=$(mktemp -d); '
        'trap \'rm -rf "$BUILD_DIR"\' EXIT; '
        'GOBIN="$BUILD_DIR" GOTOOLCHAIN=local GOPROXY=https://proxy.golang.org '
        'GOSUMDB=sum.golang.org GOFLAGS= '
        f'go install github.com/rakyll/hey@v{VERSION}; '
        'sudo install -o root -g root -m 0755 "$BUILD_DIR/hey" /usr/local/bin/hey; '
        f'go version -m /usr/local/bin/hey | grep -F "github.com/rakyll/hey" '
        f'| grep -F "v{VERSION}"'
    )


def loadgen_prepare_command():
    return apachebench.loadgen_prepare_command().replace(
        'command -v ab;', 'command -v /usr/local/bin/hey;'
    ).replace('ab -V;', '').replace('ApacheBench', 'hey')


def _command(mode, target_ip, request_count, concurrency):
    settings = workload(mode)
    target = apachebench._private_ip(target_ip)
    count, workers = int(request_count), int(concurrency)
    if not 100 <= count <= 1000000 or not 1 <= workers <= 10000 or count % workers:
        raise ValueError('hey requires 100–1000000 requests evenly divisible by concurrency.')
    connection = '' if settings['keep_alive'] else ' -disable-keepalive'
    inner = (
        f'ulimit -n {workers + 256}; '
        '/usr/local/bin/hey -disable-compression -disable-redirects'
        f'{connection} -n {count} -c {workers} http://{target}/benchmark.bin'
    )
    return (
        'set -euo pipefail; '
        'printf "OCI_HEY_LOGICAL_CPUS=%s\\n" "$(nproc)"; '
        '/usr/bin/time -f "OCI_HEY_TIME cpu_percent=%P peak_rss_kb=%M" '
        f'bash -euo pipefail -c {shlex.quote(inner)} 2>&1'
    )


def warmup_command(mode, target_ip, options):
    if not options.warmup_requests:
        return 'echo "hey warm-up skipped."'
    return _command(mode, target_ip, options.warmup_requests,
                    min(options.concurrency, options.warmup_requests))


def benchmark_command(mode, target_ip, options, trial_number):
    if not 1 <= int(trial_number) <= options.trials:
        raise ValueError('hey trial number is out of range.')
    return _command(mode, target_ip, options.request_count, options.concurrency)


def parse_output(output):
    def number(pattern, label):
        match = re.search(pattern, output, re.MULTILINE)
        if not match:
            raise ValueError(f'hey output is missing {label}.')
        return float(match.group(1))

    statuses = re.findall(r'^\s*\[(\d{3})\]\s+(\d+) responses\s*$', output, re.MULTILINE)
    complete = sum(int(count) for _, count in statuses)
    non_2xx = sum(int(count) for code, count in statuses if not 200 <= int(code) < 300)
    if non_2xx or 'Error distribution:' in output:
        raise ValueError('hey reported unsuccessful HTTP requests or transport errors.')
    if not complete:
        raise ValueError('hey output is missing completed requests.')
    duration = number(r'^\s*Total:\s+([\d.]+) secs', 'total time')
    if duration <= 0:
        raise ValueError('hey elapsed time must be positive.')
    cpu = number(r'OCI_HEY_TIME cpu_percent=([\d.]+)%', 'CPU instrumentation')
    cpus = number(r'OCI_HEY_LOGICAL_CPUS=(\d+)', 'CPU count')
    if cpus <= 0:
        raise ValueError('hey CPU count must be positive.')
    data = int(number(r'^\s*Total data:\s+(\d+) bytes', 'total data'))
    metrics = {
        'complete_requests': complete,
        'failed_requests': 0,
        'non_2xx_responses': 0,
        'document_length_bytes': int(number(r'^\s*Size/request:\s+(\d+) bytes', 'response size')),
        'total_data_bytes': data,
        'time_taken_seconds': duration,
        'requests_per_second': number(r'^\s*Requests/sec:\s+([\d.]+)', 'throughput'),
        'mean_time_per_request_ms': 1000 * number(r'^\s*Average:\s+([\d.]+) secs', 'mean latency'),
        'transfer_rate_kib_per_second': data / duration / 1024,
        'load_generator_cpu_percent': cpu,
        'load_generator_logical_cpus': int(cpus),
        'load_generator_total_cpu_capacity_used_percent': cpu / cpus,
        'load_generator_peak_rss_kb': int(number(r'peak_rss_kb=(\d+)', 'peak RSS')),
    }
    for percentile in (50, 90, 95, 99):
        metrics[f'p{percentile}_ms'] = 1000 * number(
            rf'^\s*{percentile}%{{1,2}} in ([\d.]+) secs', f'p{percentile} latency'
        )
    if cpu / cpus >= 95:
        metrics['load_generator_saturation_warning'] = (
            'The hey client averaged at least 95% of total logical CPU capacity '
            'and may have limited the measured result.'
        )
    return metrics


def validate_metrics(metrics, mode, request_count, concurrency, response_size_kib):
    workload(mode)
    expected = {
        'complete_requests': int(request_count),
        'document_length_bytes': int(response_size_kib) * 1024,
        'total_data_bytes': int(request_count) * int(response_size_kib) * 1024,
    }
    for key, value in expected.items():
        if metrics.get(key) != value:
            raise ValueError(f'hey {key} was {metrics.get(key)}, but the plan required {value}.')


def record_network_capacity(metrics, bandwidth_gbps):
    return apachebench.record_network_capacity(metrics, bandwidth_gbps)


def metadata(mode, options):
    values = apachebench.metadata(mode, options)
    values.pop('minimum_keep_alive_reuse_percent')
    return {
        **values,
        'tool': 'hey',
        'tool_version': VERSION,
        'http_version': 'HTTP/1.1',
        'compression': False,
        'follow_redirects': False,
        'connection_reuse_validation': 'client_flag_only',
        'response_size_validation': 'reported_content_length',
        'transfer_rate_measurement': 'reported_content_length_per_elapsed_time',
        'response_body_validation_note': (
            'hey v0.1.4 counts advertised Content-Length and does not expose '
            'response-body read errors; truncated bodies cannot be detected '
            'from its report.'
        ),
    }


def aggregate_trial_metrics(trials):
    if not trials:
        raise ValueError('hey has no measured trials to aggregate.')
    rates = [t['requests_per_second'] for t in trials]
    metrics = {
        'measured_trials': len(trials),
        'mean_requests_per_second': fmean(rates),
        'minimum_requests_per_second': min(rates),
        'maximum_requests_per_second': max(rates),
        'standard_deviation_requests_per_second': pstdev(rates),
        'max_load_generator_total_cpu_capacity_used_percent': max(
            t['load_generator_total_cpu_capacity_used_percent'] for t in trials
        ),
    }
    for key in ('mean_time_per_request_ms', 'transfer_rate_kib_per_second',
                'p50_ms', 'p90_ms', 'p95_ms', 'p99_ms'):
        metrics[key if key.startswith('mean_') else f'mean_{key}'] = fmean(t[key] for t in trials)
    for key in ('complete_requests', 'failed_requests', 'non_2xx_responses'):
        metrics[f'total_{key}'] = sum(t[key] for t in trials)
    if all('load_generator_network_utilization_percent' in t for t in trials):
        metrics.update({
            'mean_measured_transfer_rate_gbps': fmean(t['measured_transfer_rate_gbps'] for t in trials),
            'load_generator_network_capacity_gbps': trials[0]['load_generator_network_capacity_gbps'],
            'max_load_generator_network_utilization_percent': max(t['load_generator_network_utilization_percent'] for t in trials),
        })
    warnings = dict.fromkeys(t['load_generator_saturation_warning'] for t in trials
                             if t.get('load_generator_saturation_warning'))
    if warnings:
        metrics['load_generator_saturation_warning'] = ' '.join(warnings)
    return metrics
