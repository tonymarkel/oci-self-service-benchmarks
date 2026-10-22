import unittest
from types import SimpleNamespace

from app import hey


OUTPUT = '''OCI_HEY_LOGICAL_CPUS=2
Summary:
  Total: 2.0000 secs
  Slowest: 0.2000 secs
  Fastest: 0.0010 secs
  Average: 0.0100 secs
  Requests/sec: 500.0000
  Total data: 65536000 bytes
  Size/request: 65536 bytes
Latency distribution:
  50%% in 0.0080 secs
  90%% in 0.0200 secs
  95%% in 0.0300 secs
  99%% in 0.0400 secs
Status code distribution:
  [200] 1000 responses
OCI_HEY_TIME cpu_percent=150% peak_rss_kb=20000
'''


def options(**kw):
    return SimpleNamespace(**dict(request_count=1000, concurrency=10,
                                  warmup_requests=100, trials=2, **kw))


class HeyTests(unittest.TestCase):
    def test_parses_units_status_counts_and_multicore_utilization(self):
        result = hey.parse_output(OUTPUT)
        self.assertEqual(result['complete_requests'], 1000)
        self.assertEqual(result['mean_time_per_request_ms'], 10)
        self.assertEqual(result['p99_ms'], 40)
        self.assertEqual(result['transfer_rate_kib_per_second'], 32000)
        self.assertEqual(result['load_generator_total_cpu_capacity_used_percent'], 75)
        self.assertNotIn('apachebench_single_core_utilization_percent', result)
        self.assertNotIn('keep_alive_request_percent', result)
        hey.validate_metrics(result, 'keep_alive', 1000, 10, 64)

    def test_rejects_http_and_transport_errors_even_if_hey_exits_zero(self):
        for output in (OUTPUT.replace('[200]', '[500]'),
                       OUTPUT + 'Error distribution:\n  [1] connection reset\n'):
            with self.subTest(output=output), self.assertRaisesRegex(ValueError, 'unsuccessful'):
                hey.parse_output(output)

    def test_rejects_missing_counts_instrumentation_and_percentiles(self):
        for line in ('  [200] 1000 responses\n',
                     'OCI_HEY_LOGICAL_CPUS=2\n',
                     'OCI_HEY_TIME cpu_percent=150% peak_rss_kb=20000\n',
                     '  99%% in 0.0400 secs\n'):
            with self.subTest(line=line), self.assertRaises(ValueError):
                hey.parse_output(OUTPUT.replace(line, ''))

    def test_exact_request_count_and_payload_are_validated(self):
        metrics = hey.parse_output(OUTPUT)
        for count, size in ((1100, 64), (1000, 32)):
            with self.subTest(count=count, size=size), self.assertRaises(ValueError):
                hey.validate_metrics(metrics, 'keep_alive', count, 10, size)
        with self.assertRaises(ValueError):
            hey.validate_metrics({**metrics, 'total_data_bytes': 1}, 'keep_alive', 1000, 10, 64)

    def test_commands_use_private_http_and_explicit_connection_options(self):
        for mode in ('new_connections', 'keep_alive'):
            command = hey.benchmark_command(mode, '10.42.1.10', options(), 1)
            self.assertIn('-n 1000 -c 10', command)
            self.assertIn('http://10.42.1.10/benchmark.bin', command)
            self.assertIn('-disable-compression', command)
            self.assertIn('-disable-redirects', command)
            self.assertEqual('-disable-keepalive' in command, mode == 'new_connections')
            self.assertIn('OCI_HEY_TIME', command)
            with self.assertRaises(ValueError):
                hey.benchmark_command(mode, '198.51.100.1', options(), 1)
        with self.assertRaises(ValueError):
            hey.benchmark_command('keep_alive', '10.42.1.10', options(), 3)

    def test_aggregation_has_hey_metrics_without_claiming_measured_reuse(self):
        trial = hey.parse_output(OUTPUT)
        metrics = hey.aggregate_trial_metrics([trial, {**trial, 'requests_per_second': 700}])
        self.assertEqual(metrics['mean_requests_per_second'], 600)
        self.assertEqual(metrics['standard_deviation_requests_per_second'], 100)
        self.assertEqual(metrics['total_complete_requests'], 2000)
        self.assertNotIn('mean_keep_alive_request_percent', metrics)
        self.assertNotIn('max_apachebench_single_core_utilization_percent', metrics)

    def test_network_capacity_warning_uses_payload_throughput(self):
        metrics = hey.parse_output(OUTPUT)
        hey.record_network_capacity(metrics, 0.1)
        self.assertGreater(metrics['load_generator_network_utilization_percent'], 90)
        self.assertIn('VNIC', metrics['load_generator_saturation_warning'])
