import re
import shlex
from ipaddress import ip_address, ip_network
from statistics import fmean, pstdev


WORKLOADS = [
    {
        'id': 'new_connections',
        'name': 'New Connections',
        'description': (
            'Measures HTTP/1.0 request throughput with a new TCP connection '
            'for each request.'
        ),
        'keep_alive': False,
    },
    {
        'id': 'keep_alive',
        'name': 'Keep-Alive',
        'description': (
            'Measures HTTP/1.0 request throughput while reusing persistent '
            'connections.'
        ),
        'keep_alive': True,
    },
]
_WORKLOADS_BY_ID = {item['id']: item for item in WORKLOADS}
_NUMBER = r'([0-9]+(?:\.[0-9]+)?)'
_BENCHMARK_VCN = ip_network('10.42.0.0/16')
MINIMUM_KEEP_ALIVE_REUSE_PERCENT = 90.0
LOAD_GENERATOR_NETWORK_WARNING_PERCENT = 90.0


def workload(workload_id):
    try:
        return _WORKLOADS_BY_ID[workload_id]
    except KeyError as exc:
        raise ValueError(f'Unknown ApacheBench connection mode: {workload_id}') from exc


def _private_ip(value):
    parsed = ip_address(value)
    if parsed.version != 4 or parsed not in _BENCHMARK_VCN:
        raise ValueError(
            'ApacheBench traffic must use a private IPv4 address in the benchmark '
            'VCN (10.42.0.0/16).'
        )
    return str(parsed)


def target_prepare_command(response_size_kib, loadgen_private_ip):
    size_bytes = int(response_size_kib) * 1024
    if not 1024 <= size_bytes <= 1024 * 1024:
        raise ValueError('ApacheBench response size must be between 1 and 1024 KiB.')
    source = _private_ip(loadgen_private_ip)
    return (
        'set -euo pipefail; '
        'sudo install -d -m 755 /var/www/html; '
        f'sudo dd if=/dev/zero of=/var/www/html/benchmark.bin bs=1024 '
        f'count={int(response_size_kib)} status=none; '
        'printf "%s\n" "KeepAlive On" "KeepAliveTimeout 5" '
        '"MaxKeepAliveRequests 10000" "<IfModule mpm_event_module>" '
        '"ServerLimit 256" "StartServers 8" "ThreadsPerChild 64" '
        '"MaxRequestWorkers 16384" "ListenBacklog 10000" "</IfModule>" '
        '| sudo tee /etc/httpd/conf.d/oci-benchmark.conf >/dev/null; '
        'sudo install -d -m 755 /etc/systemd/system/httpd.service.d; '
        'printf "%s\n" "[Service]" "LimitNOFILE=65536" '
        '| sudo tee /etc/systemd/system/httpd.service.d/limits.conf >/dev/null; '
        'sudo sysctl -w net.core.somaxconn=10000; '
        'sudo systemctl enable --now firewalld; '
        f'INTERFACE=$(ip -o route get {source} | '
        "awk '{for (i=1; i<=NF; i++) if ($i == \"dev\") "
        "{print $(i+1); exit}}'); "
        'test -n "$INTERFACE"; '
        'ZONE=$(sudo firewall-cmd --get-zone-of-interface="$INTERFACE" '
        '2>/dev/null || true); '
        'if [ -z "$ZONE" ] || [ "$ZONE" = "no zone" ]; then '
        'ZONE=$(sudo firewall-cmd --get-default-zone); fi; '
        'sudo firewall-cmd --permanent --zone="$ZONE" '
        '--remove-service=http >/dev/null 2>&1 || true; '
        'sudo firewall-cmd --permanent --zone="$ZONE" '
        f'--add-rich-rule={shlex.quote(f"rule family=ipv4 source address={source}/32 port port=80 protocol=tcp accept")}; '
        'sudo firewall-cmd --reload; '
        'sudo firewall-cmd --zone="$ZONE" '
        f'--query-rich-rule={shlex.quote(f"rule family=ipv4 source address={source}/32 port port=80 protocol=tcp accept")}; '
        'HTTPD_MODULES="$(sudo httpd -M 2>&1)"; '
        'printf "%s\n" "$HTTPD_MODULES"; '
        'grep -q "mpm_event_module" <<< "$HTTPD_MODULES"; '
        'sudo apachectl configtest; sudo systemctl daemon-reload; '
        'sudo systemctl enable --now httpd; '
        'sudo systemctl restart httpd; '
        f'test "$(stat -c %s /var/www/html/benchmark.bin)" -eq {size_bytes}; '
        'curl --fail --silent --output /dev/null http://127.0.0.1/benchmark.bin; '
        'httpd -v; '
        'echo "Apache HTTP Server target is ready."'
    )


def loadgen_prepare_command():
    return (
        'set -euo pipefail; command -v ab; command -v /usr/bin/time; '
        'ab -V; HARD_NOFILE=$(ulimit -Hn); '
        'test "$HARD_NOFILE" = unlimited || [ "$HARD_NOFILE" -ge 10256 ] '
        '|| { echo "The load generator hard nofile limit must be at least '
        '10256." >&2; exit 1; }; '
        'sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"; '
        'sudo sysctl -w net.ipv4.tcp_tw_reuse=1; '
        'echo "ApacheBench load generator is ready."'
    )


def readiness_command(target_ip, response_size_kib):
    target = _private_ip(target_ip)
    size_bytes = int(response_size_kib) * 1024
    return (
        'set -euo pipefail; '
        f'TARGET={shlex.quote(target)}; EXPECTED={size_bytes}; READY=false; '
        'for attempt in $(seq 1 60); do '
        'STATUS=$(curl --silent --output /tmp/oci-ab-ready.bin '
        '--write-out "%{http_code}" --connect-timeout 2 --max-time 10 '
        '"http://$TARGET/benchmark.bin" || true); '
        'BYTES=$(stat -c %s /tmp/oci-ab-ready.bin 2>/dev/null || echo 0); '
        'if [ "$STATUS" = 200 ] && [ "$BYTES" -eq "$EXPECTED" ]; then '
        'READY=true; break; fi; sleep 5; done; '
        'if [ "$READY" != true ]; then echo "Apache HTTP target did not return '
        'HTTP 200 with the configured response size." >&2; exit 1; fi; '
        'echo "Apache HTTP target is ready (HTTP 200, $BYTES bytes)."'
    )


def _ab_command(mode, target_ip, request_count, concurrency, percentile_path=None):
    settings = workload(mode)
    target = _private_ip(target_ip)
    keep_alive = ' -k' if settings['keep_alive'] else ''
    csv = f' -e {shlex.quote(percentile_path)}' if percentile_path else ''
    ab = (
        f'ab -r{keep_alive}{csv} -n {int(request_count)} '
        f'-c {int(concurrency)} http://{target}/benchmark.bin'
    )
    required_nofile = int(concurrency) + 256
    return (
        'set -euo pipefail; '
        f'ulimit -n {required_nofile}; '
        f'test "$(ulimit -n)" -ge {required_nofile}; {ab}'
    )


def warmup_command(mode, target_ip, options):
    requests = int(options.warmup_requests)
    if requests == 0:
        return 'echo "ApacheBench warm-up skipped."'
    concurrency = min(int(options.concurrency), requests)
    ab_command = _ab_command(mode, target_ip, requests, concurrency)
    return (
        'set -euo pipefail; '
        'printf "OCI_AB_LOGICAL_CPUS=%s\\n" "$(nproc)"; '
        '/usr/bin/time -f "OCI_AB_TIME cpu_percent=%P peak_rss_kb=%M" '
        f'bash -c {shlex.quote(ab_command)} 2>&1'
    )


def benchmark_command(mode, target_ip, options, trial_number):
    workload(mode)
    trial = int(trial_number)
    if not 1 <= trial <= int(options.trials):
        raise ValueError('ApacheBench trial number is out of range.')
    csv_path = f'/tmp/oci-apachebench-{mode}-{trial}.csv'
    ab_command = _ab_command(
        mode,
        target_ip,
        options.request_count,
        options.concurrency,
        csv_path,
    )
    return (
        'set -o pipefail; '
        f'rm -f {csv_path}; '
        'printf "OCI_AB_LOGICAL_CPUS=%s\\n" "$(nproc)"; '
        f'/usr/bin/time -f "OCI_AB_TIME cpu_percent=%P peak_rss_kb=%M" '
        f'bash -c {shlex.quote(ab_command)} 2>&1; RC=$?; '
        f'test -s {csv_path} || {{ echo "ApacheBench percentile CSV is missing." >&2; exit 1; }}; '
        f'echo "OCI_AB_PERCENTILES_BEGIN"; cat {csv_path}; '
        'echo "OCI_AB_PERCENTILES_END"; exit "$RC"'
    )


def _integer(output, label, optional=False):
    match = re.search(rf'^{re.escape(label)}:\s+([0-9]+)', output, re.MULTILINE)
    if not match:
        if optional:
            return 0
        raise ValueError(f'ApacheBench output is missing {label}.')
    return int(match.group(1))


def _float(output, pattern, label):
    match = re.search(pattern, output, re.MULTILINE)
    if not match:
        raise ValueError(f'ApacheBench output is missing {label}.')
    return float(match.group(1))


def _percentiles(output):
    marker = re.search(
        r'OCI_AB_PERCENTILES_BEGIN\s*(.*?)\s*OCI_AB_PERCENTILES_END',
        output,
        re.DOTALL,
    )
    if marker:
        values = {}
        for line in marker.group(1).splitlines():
            match = re.match(r'\s*"?([0-9]+)"?\s*,\s*"?([0-9.]+)"?', line)
            if match:
                values[int(match.group(1))] = float(match.group(2))
        if all(percentile in values for percentile in (50, 90, 95, 99)):
            return {f'p{p}_ms': values[p] for p in (50, 90, 95, 99)}
    values = {}
    section = re.search(
        r'Percentage of the requests served within a certain time.*?(?:\n\n|\Z)',
        output,
        re.DOTALL,
    )
    if section:
        for line in section.group(0).splitlines():
            match = re.match(r'\s*(50|90|95|99)%\s+([0-9.]+)', line)
            if match:
                values[int(match.group(1))] = float(match.group(2))
    if not all(percentile in values for percentile in (50, 90, 95, 99)):
        raise ValueError('ApacheBench output is missing latency percentiles.')
    return {f'p{p}_ms': values[p] for p in (50, 90, 95, 99)}


def parse_output(output):
    if not str(output).strip():
        raise ValueError('ApacheBench produced no output.')
    complete = _integer(output, 'Complete requests')
    if complete <= 0:
        raise ValueError('ApacheBench completed zero requests.')
    time_marker = re.search(
        r'OCI_AB_TIME cpu_percent=([0-9.]+)% peak_rss_kb=([0-9]+)',
        output,
    )
    if not time_marker:
        raise ValueError(
            'ApacheBench output is missing load-generator CPU instrumentation.'
        )
    cpu_percent = float(time_marker.group(1))
    peak_rss = int(time_marker.group(2))
    logical_cpus_match = re.search(r'OCI_AB_LOGICAL_CPUS=([0-9]+)', output)
    if not logical_cpus_match or int(logical_cpus_match.group(1)) <= 0:
        raise ValueError(
            'ApacheBench output is missing the load-generator CPU count.'
        )
    logical_cpus = int(logical_cpus_match.group(1))
    total_capacity = cpu_percent / logical_cpus
    single_core_utilization = min(cpu_percent, 100.0)
    metrics = {
        'document_length_bytes': _integer(output, 'Document Length'),
        'concurrency_level': _integer(output, 'Concurrency Level'),
        'time_taken_seconds': _float(output, rf'^Time taken for tests:\s+{_NUMBER}', 'time taken'),
        'complete_requests': complete,
        'failed_requests': _integer(output, 'Failed requests'),
        'write_errors': _integer(output, 'Write errors', optional=True),
        'non_2xx_responses': _integer(output, 'Non-2xx responses', optional=True),
        'keep_alive_requests': _integer(output, 'Keep-Alive requests', optional=True),
        'requests_per_second': _float(output, rf'^Requests per second:\s+{_NUMBER}', 'requests per second'),
        'mean_time_per_request_ms': _float(output, rf'^Time per request:\s+{_NUMBER}\s+\[ms\]\s+\(mean\)$', 'mean time per request'),
        'mean_time_across_concurrent_requests_ms': _float(output, rf'^Time per request:\s+{_NUMBER}\s+\[ms\]\s+\(mean, across all concurrent requests\)$', 'concurrent mean time'),
        'transfer_rate_kib_per_second': _float(output, rf'^Transfer rate:\s+{_NUMBER}', 'transfer rate'),
        **_percentiles(output),
        'load_generator_cpu_percent': cpu_percent,
        'load_generator_logical_cpus': logical_cpus,
        'load_generator_total_cpu_capacity_used_percent': total_capacity,
        'apachebench_single_core_utilization_percent': single_core_utilization,
        'load_generator_peak_rss_kb': peak_rss,
    }
    metrics['keep_alive_request_percent'] = (
        100.0 * metrics['keep_alive_requests'] / complete
    )
    if single_core_utilization >= 95:
        metrics['load_generator_saturation_warning'] = (
            'The ApacheBench client averaged at least 95% of one logical CPU '
            'and may have limited the measured result.'
        )
    errors = (
        metrics['failed_requests']
        + metrics['write_errors']
        + metrics['non_2xx_responses']
    )
    if errors:
        raise ValueError(
            'ApacheBench reported unsuccessful requests: '
            f'{metrics["failed_requests"]} failed, '
            f'{metrics["write_errors"]} write errors, and '
            f'{metrics["non_2xx_responses"]} non-2xx responses.'
        )
    return metrics


def validate_metrics(
    metrics,
    mode,
    request_count,
    concurrency,
    response_size_kib,
):
    """Require the parsed run to match the exact workload that was requested."""
    settings = workload(mode)
    expected = {
        'complete_requests': int(request_count),
        'concurrency_level': int(concurrency),
        'document_length_bytes': int(response_size_kib) * 1024,
    }
    labels = {
        'complete_requests': 'completed request count',
        'concurrency_level': 'concurrency level',
        'document_length_bytes': 'document length',
    }
    for key, expected_value in expected.items():
        actual = int(metrics.get(key, -1))
        if actual != expected_value:
            raise ValueError(
                f'ApacheBench {labels[key]} was {actual}, but the plan '
                f'required {expected_value}.'
            )
    reused = int(metrics.get('keep_alive_requests', 0))
    reuse_percent = 100.0 * reused / int(request_count)
    if (
        settings['keep_alive']
        and reuse_percent < MINIMUM_KEEP_ALIVE_REUSE_PERCENT
    ):
        raise ValueError(
            'ApacheBench Keep-Alive mode reused only '
            f'{reuse_percent:.2f}% of requests; at least '
            f'{MINIMUM_KEEP_ALIVE_REUSE_PERCENT:.0f}% is required.'
        )
    if not settings['keep_alive'] and reused != 0:
        raise ValueError(
            'ApacheBench new-connection mode unexpectedly reused '
            f'{reused} connections.'
        )


def record_network_capacity(metrics, bandwidth_gbps):
    """Record client VNIC headroom using OCI's configured shape bandwidth."""
    capacity = float(bandwidth_gbps)
    if not capacity > 0:
        raise ValueError(
            'The ApacheBench load-generator network capacity is invalid.'
        )
    measured = (
        float(metrics['transfer_rate_kib_per_second']) * 1024 * 8 / 1e9
    )
    utilization = 100.0 * measured / capacity
    metrics['measured_transfer_rate_gbps'] = measured
    metrics['load_generator_network_capacity_gbps'] = capacity
    metrics['load_generator_network_utilization_percent'] = utilization
    if utilization >= LOAD_GENERATOR_NETWORK_WARNING_PERCENT:
        warning = (
            'Measured HTTP transfer rate reached at least 90% of the load '
            'generator VNIC bandwidth and may have limited the result.'
        )
        existing = metrics.get('load_generator_saturation_warning')
        metrics['load_generator_saturation_warning'] = (
            f'{existing} {warning}' if existing else warning
        )
    return metrics


def metadata(mode, options):
    settings = workload(mode)
    return {
        'tool': 'Apache HTTP Server Benchmarking Tool (ab)',
        'server': 'Apache HTTP Server (httpd)',
        'connection_mode': settings['name'],
        'keep_alive': settings['keep_alive'],
        'request_count_per_trial': int(options.request_count),
        'concurrency': int(options.concurrency),
        'response_size_kib': int(options.response_size_kib),
        'warmup_requests_per_mode': int(options.warmup_requests),
        'trial_count': int(options.trials),
        'httpd_keep_alive': 'On',
        'httpd_max_keep_alive_requests': 10000,
        'httpd_max_request_workers': 16384,
        'httpd_listen_backlog': 10000,
        'target_net_core_somaxconn': 10000,
        'httpd_mpm': 'event',
        'minimum_keep_alive_reuse_percent': (
            MINIMUM_KEEP_ALIVE_REUSE_PERCENT
        ),
    }


def aggregate_trial_metrics(trials):
    if not trials:
        raise ValueError('ApacheBench has no measured trials to aggregate.')
    def mean(key):
        return fmean(float(trial[key]) for trial in trials)
    metrics = {
        'measured_trials': len(trials),
        'mean_requests_per_second': mean('requests_per_second'),
        'minimum_requests_per_second': min(
            float(t['requests_per_second']) for t in trials
        ),
        'maximum_requests_per_second': max(
            float(t['requests_per_second']) for t in trials
        ),
        'standard_deviation_requests_per_second': pstdev(
            float(t['requests_per_second']) for t in trials
        ),
        'mean_time_per_request_ms': mean('mean_time_per_request_ms'),
        'mean_transfer_rate_kib_per_second': mean(
            'transfer_rate_kib_per_second'
        ),
        'mean_p50_ms': mean('p50_ms'),
        'mean_p90_ms': mean('p90_ms'),
        'mean_p95_ms': mean('p95_ms'),
        'mean_p99_ms': mean('p99_ms'),
        'mean_keep_alive_request_percent': mean(
            'keep_alive_request_percent'
        ),
        'minimum_keep_alive_request_percent': min(
            float(t['keep_alive_request_percent']) for t in trials
        ),
        'total_complete_requests': sum(int(t['complete_requests']) for t in trials),
        'total_failed_requests': sum(int(t['failed_requests']) for t in trials),
        'total_write_errors': sum(int(t.get('write_errors', 0)) for t in trials),
        'total_non_2xx_responses': sum(int(t['non_2xx_responses']) for t in trials),
        'max_load_generator_total_cpu_capacity_used_percent': max(
            float(t['load_generator_total_cpu_capacity_used_percent'])
            for t in trials
        ),
        'max_apachebench_single_core_utilization_percent': max(
            float(t['apachebench_single_core_utilization_percent'])
            for t in trials
        ),
    }
    if all('load_generator_network_utilization_percent' in t for t in trials):
        metrics.update({
            'mean_measured_transfer_rate_gbps': mean(
                'measured_transfer_rate_gbps'
            ),
            'load_generator_network_capacity_gbps': float(
                trials[0]['load_generator_network_capacity_gbps']
            ),
            'max_load_generator_network_utilization_percent': max(
                float(t['load_generator_network_utilization_percent'])
                for t in trials
            ),
        })
    warnings = [
        t.get('load_generator_saturation_warning') for t in trials
        if t.get('load_generator_saturation_warning')
    ]
    if warnings:
        metrics['load_generator_saturation_warning'] = ' '.join(
            dict.fromkeys(warnings)
        )
    return metrics
