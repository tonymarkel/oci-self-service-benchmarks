"""Response-body accounting shared by synthetic Hotel and Media drivers."""

import math
import re


def parse_body_metrics(output, *, workload, label, error_type=RuntimeError):
    """Keep body failures separate, while supplying standard UI metric names."""
    lines = re.findall(r'^CANDIDATE_DSB_METRICS (.+)$', output, re.MULTILINE)
    sent = re.findall(r'^Sent\s+(\d+)\s+requests\s*$', output, re.MULTILINE)
    if len(lines) != 1 or len(sent) != 1:
        raise error_type(f'{label} wrk2 must emit one complete measurement.')
    pairs = [part.split('=', 1) for part in lines[0].split()]
    if any(len(pair) != 2 for pair in pairs) or len(dict(pairs)) != len(pairs):
        raise error_type(f'{label} wrk2 emitted malformed metrics.')
    values = dict(pairs)
    if values.get('schema') != 'candidate-body-v1' or values.get('workload') != workload:
        raise error_type(f'{label} wrk2 emitted a different workload or measurement method.')
    integer_fields = ('completed_responses', 'summary_requests', 'body_check_successes', 'http_errors',
                      'http_status_errors', 'semantic_failures', 'socket_errors', 'connect_errors',
                      'read_errors', 'write_errors', 'timeout_errors', 'counter_consistent', 'workers',
                      'seed_base')
    float_fields = ('duration_seconds', 'body_check_successes_per_second', 'completed_failure_percent',
                    'p50_ms', 'p95_ms', 'p99_ms')
    try:
        for key in integer_fields:
            if not re.fullmatch(r'[0-9]+', values[key]):
                raise ValueError(key)
        numbers = {key: int(values[key]) for key in integer_fields}
        numbers.update({key: float(values[key]) for key in float_fields})
        if any(not math.isfinite(number) or number < 0 for number in numbers.values()):
            raise ValueError('invalid number')
    except (KeyError, ValueError) as exc:
        raise error_type(f'{label} wrk2 metrics are incomplete or invalid.') from exc
    completed = numbers['completed_responses']
    successes = numbers['body_check_successes']
    errors = numbers['http_errors'] + numbers['semantic_failures']
    duration = numbers['duration_seconds']
    if (not completed or duration <= 0 or numbers['counter_consistent'] != 1
            or completed != numbers['summary_requests'] or successes + errors != completed
            or int(sent[0]) < completed or successes <= 0
            or numbers['http_status_errors'] > numbers['http_errors']
            or numbers['socket_errors'] != sum(numbers[key] for key in
                ('connect_errors', 'read_errors', 'write_errors', 'timeout_errors'))
            or not numbers['p50_ms'] <= numbers['p95_ms'] <= numbers['p99_ms']):
        raise error_type(f'{label} wrk2 counters are inconsistent or all responses failed.')
    if (not math.isclose(numbers['body_check_successes_per_second'], successes / duration,
                         rel_tol=0.0001, abs_tol=0.0001)
            or not math.isclose(numbers['completed_failure_percent'], errors / completed * 100,
                                rel_tol=0.0001, abs_tol=0.0001)):
        raise error_type(f'{label} wrk2 rates do not match its response counters.')
    metrics = {
        **numbers, 'total_requests': completed, 'successful_requests': successes, 'errors': errors,
        'throughput_requests_per_second': round(successes / duration, 6),
        'completed_requests_per_second': round(completed / duration, 6),
        'error_rate_percent': round(errors / completed * 100, 6), 'sent_requests': int(sent[0]),
        'uncompleted_requests': int(sent[0]) - completed,
        'completion_rate_percent': round(completed / int(sent[0]) * 100, 6),
    }
    cpu = re.search(r'Percent of CPU this job got:\s*([0-9.]+)%', output)
    cores = re.search(r'^CANDIDATE_DSB_LOADGEN_LOGICAL_CPUS=(\d+)$', output, re.MULTILINE)
    rss = re.search(r'Maximum resident set size \(kbytes\):\s*(\d+)', output)
    if cpu:
        metrics['load_generator_cpu_percent'] = float(cpu[1])
        if cores and int(cores[1]):
            metrics['load_generator_logical_cpus'] = int(cores[1])
            metrics['load_generator_capacity_used_percent'] = round(float(cpu[1]) / int(cores[1]), 3)
            if metrics['load_generator_capacity_used_percent'] >= 95:
                metrics['load_generator_saturation_warning'] = 'Load generator averaged at least 95% CPU capacity.'
    if rss:
        metrics['load_generator_peak_rss_kb'] = int(rss[1])
    return metrics
