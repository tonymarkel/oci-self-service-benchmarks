import unittest
from types import SimpleNamespace

from app import apachebench


def options(**overrides):
    values = {
        'workloads': ['new_connections', 'keep_alive'],
        'request_count': 500000,
        'concurrency': 100,
        'response_size_kib': 64,
        'warmup_requests': 10000,
        'trials': 3,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


SAMPLE_OUTPUT = '''
This is ApacheBench, Version 2.3
Document Length:        65536 bytes
Concurrency Level:      100
Time taken for tests:   5.000 seconds
Complete requests:      500000
Failed requests:        0
Write errors:           0
Non-2xx responses:      0
Keep-Alive requests:    499998
Requests per second:    100000.00 [#/sec] (mean)
Time per request:       1.000 [ms] (mean)
Time per request:       0.010 [ms] (mean, across all concurrent requests)
Transfer rate:          6400000.00 [Kbytes/sec] received
OCI_AB_LOGICAL_CPUS=4
OCI_AB_PERCENTILES_BEGIN
"Percentage served","Time in ms"
"0","0.100"
"50","0.800"
"90","1.200"
"95","1.500"
"99","2.500"
"100","8.000"
OCI_AB_PERCENTILES_END
OCI_AB_TIME cpu_percent=98% peak_rss_kb=24576
'''


class ApacheBenchCommandTests(unittest.TestCase):
    def test_target_is_exact_size_keepalive_and_private_source_restricted(self):
        command = apachebench.target_prepare_command(64, '10.42.1.20')

        self.assertIn('count=64', command)
        self.assertIn('KeepAlive On', command)
        self.assertIn('MaxKeepAliveRequests 10000', command)
        self.assertIn('MaxRequestWorkers 16384', command)
        self.assertIn('net.core.somaxconn=10000', command)
        self.assertIn('mpm_event_module', command)
        self.assertIn('source address=10.42.1.20/32', command)
        self.assertIn('--get-zone-of-interface', command)
        self.assertIn('--zone="$ZONE"', command)
        self.assertIn('--remove-service=http', command)
        self.assertIn('systemctl restart httpd', command)
        self.assertIn('httpd -v', command)
        self.assertNotIn('docker', command.lower())

    def test_readiness_requires_http_200_and_exact_size(self):
        command = apachebench.readiness_command('10.42.1.10', 64)

        self.assertIn('EXPECTED=65536', command)
        self.assertIn('"$STATUS" = 200', command)
        self.assertIn('"$BYTES" -eq "$EXPECTED"', command)
        self.assertIn('http://$TARGET/benchmark.bin', command)

    def test_connection_modes_control_only_the_keepalive_flag(self):
        value = options(request_count=1000, concurrency=10)
        fresh = apachebench.benchmark_command(
            'new_connections', '10.42.1.10', value, 1
        )
        keepalive = apachebench.benchmark_command(
            'keep_alive', '10.42.1.10', value, 1
        )

        self.assertNotIn('ab -r -k', fresh)
        self.assertIn('ab -r -k', keepalive)
        for command in (fresh, keepalive):
            self.assertIn('-n 1000', command)
            self.assertIn('-c 10', command)
            self.assertIn('OCI_AB_LOGICAL_CPUS', command)
            self.assertIn('OCI_AB_TIME', command)
            self.assertIn('OCI_AB_PERCENTILES_BEGIN', command)
            self.assertIn('ulimit -n 266', command)

    def test_load_generator_preflight_requires_the_maximum_nofile_capacity(self):
        command = apachebench.loadgen_prepare_command()

        self.assertIn('ulimit -Hn', command)
        self.assertIn('10256', command)
        self.assertIn('tcp_tw_reuse=1', command)

    def test_warmup_caps_concurrency_and_zero_is_a_noop(self):
        capped = apachebench.warmup_command(
            'new_connections',
            '10.42.1.10',
            options(warmup_requests=5, concurrency=100),
        )
        skipped = apachebench.warmup_command(
            'new_connections',
            '10.42.1.10',
            options(warmup_requests=0),
        )

        self.assertIn('-n 5 -c 5', capped)
        self.assertIn('OCI_AB_LOGICAL_CPUS', capped)
        self.assertIn('OCI_AB_TIME', capped)
        self.assertIn('skipped', skipped.lower())

    def test_rejects_unknown_modes_public_ips_and_bad_trials(self):
        with self.assertRaisesRegex(ValueError, 'Unknown'):
            apachebench.workload('unknown')
        with self.assertRaisesRegex(ValueError, 'benchmark VCN'):
            apachebench.readiness_command('8.8.8.8', 64)
        with self.assertRaisesRegex(ValueError, 'out of range'):
            apachebench.benchmark_command(
                'keep_alive', '10.42.1.10', options(), 4
            )


class ApacheBenchParserTests(unittest.TestCase):
    def test_parses_ab_percentiles_errors_and_generator_saturation(self):
        metrics = apachebench.parse_output(SAMPLE_OUTPUT)

        self.assertEqual(metrics['document_length_bytes'], 65536)
        self.assertEqual(metrics['concurrency_level'], 100)
        self.assertEqual(metrics['complete_requests'], 500000)
        self.assertEqual(metrics['failed_requests'], 0)
        self.assertEqual(metrics['write_errors'], 0)
        self.assertEqual(metrics['non_2xx_responses'], 0)
        self.assertEqual(metrics['keep_alive_requests'], 499998)
        self.assertEqual(metrics['requests_per_second'], 100000.0)
        self.assertEqual(metrics['p50_ms'], 0.8)
        self.assertEqual(metrics['p90_ms'], 1.2)
        self.assertEqual(metrics['p95_ms'], 1.5)
        self.assertEqual(metrics['p99_ms'], 2.5)
        self.assertEqual(metrics['load_generator_cpu_percent'], 98.0)
        self.assertEqual(metrics['load_generator_logical_cpus'], 4)
        self.assertEqual(
            metrics['load_generator_total_cpu_capacity_used_percent'],
            24.5,
        )
        self.assertEqual(
            metrics['apachebench_single_core_utilization_percent'],
            98.0,
        )
        self.assertIn('load_generator_saturation_warning', metrics)

    def test_single_thread_client_saturation_warns_on_a_multi_cpu_vm(self):
        metrics = apachebench.parse_output(
            SAMPLE_OUTPUT.replace(
                'OCI_AB_TIME cpu_percent=98%',
                'OCI_AB_TIME cpu_percent=95%',
            )
        )

        self.assertEqual(
            metrics['load_generator_total_cpu_capacity_used_percent'],
            23.75,
        )
        self.assertIn('load_generator_saturation_warning', metrics)

    def test_requires_load_generator_instrumentation(self):
        without_time = SAMPLE_OUTPUT.replace(
            'OCI_AB_TIME cpu_percent=98% peak_rss_kb=24576',
            '',
        )
        without_cpus = SAMPLE_OUTPUT.replace('OCI_AB_LOGICAL_CPUS=4', '')

        with self.assertRaisesRegex(ValueError, 'CPU instrumentation'):
            apachebench.parse_output(without_time)
        with self.assertRaisesRegex(ValueError, 'CPU count'):
            apachebench.parse_output(without_cpus)

    def test_validates_exact_plan_and_connection_mode(self):
        metrics = apachebench.parse_output(SAMPLE_OUTPUT)

        apachebench.validate_metrics(
            metrics,
            'keep_alive',
            500000,
            100,
            64,
        )
        for overrides, message in (
            ({'request_count': 499999}, 'request count'),
            ({'concurrency': 99}, 'concurrency'),
            ({'response_size_kib': 32}, 'document length'),
        ):
            values = {
                'request_count': 500000,
                'concurrency': 100,
                'response_size_kib': 64,
                **overrides,
            }
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ValueError, message):
                    apachebench.validate_metrics(
                        metrics,
                        'keep_alive',
                        **values,
                    )
        with self.assertRaisesRegex(ValueError, 'unexpectedly reused'):
            apachebench.validate_metrics(
                metrics,
                'new_connections',
                500000,
                100,
                64,
            )
        low_reuse = {**metrics, 'keep_alive_requests': 1}
        with self.assertRaisesRegex(ValueError, 'at least 90%'):
            apachebench.validate_metrics(
                low_reuse,
                'keep_alive',
                500000,
                100,
                64,
            )

    def test_requires_completed_requests_and_percentiles(self):
        with self.assertRaisesRegex(ValueError, 'zero requests'):
            apachebench.parse_output(
                SAMPLE_OUTPUT.replace(
                    'Complete requests:      500000',
                    'Complete requests:      0',
                )
            )
        start = SAMPLE_OUTPUT.index('OCI_AB_PERCENTILES_BEGIN')
        end = SAMPLE_OUTPUT.index('OCI_AB_TIME')
        with self.assertRaisesRegex(ValueError, 'percentiles'):
            apachebench.parse_output(SAMPLE_OUTPUT[:start] + SAMPLE_OUTPUT[end:])

    def test_rejects_failed_write_and_non_2xx_requests(self):
        for label, value in (
            ('Failed requests:        0', 'Failed requests:        1'),
            ('Write errors:           0', 'Write errors:           1'),
            ('Non-2xx responses:      0', 'Non-2xx responses:      1'),
        ):
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, 'unsuccessful'):
                    apachebench.parse_output(SAMPLE_OUTPUT.replace(label, value))

    def test_aggregates_trials_and_propagates_saturation_warning(self):
        first = apachebench.parse_output(SAMPLE_OUTPUT)
        apachebench.record_network_capacity(first, 50.0)
        second = {
            **first,
            'requests_per_second': 80000.0,
            'failed_requests': 0,
            'non_2xx_responses': 0,
            'load_generator_total_cpu_capacity_used_percent': 20.0,
            'apachebench_single_core_utilization_percent': 80.0,
            'measured_transfer_rate_gbps': 40.0,
            'load_generator_network_capacity_gbps': 50.0,
            'load_generator_network_utilization_percent': 80.0,
        }
        second.pop('load_generator_saturation_warning')

        aggregate = apachebench.aggregate_trial_metrics([first, second])

        self.assertEqual(aggregate['mean_requests_per_second'], 90000.0)
        self.assertEqual(aggregate['measured_trials'], 2)
        self.assertEqual(aggregate['minimum_requests_per_second'], 80000.0)
        self.assertEqual(aggregate['maximum_requests_per_second'], 100000.0)
        self.assertEqual(
            aggregate['standard_deviation_requests_per_second'],
            10000.0,
        )
        self.assertEqual(aggregate['total_complete_requests'], 1000000)
        self.assertEqual(aggregate['total_failed_requests'], 0)
        self.assertEqual(aggregate['total_non_2xx_responses'], 0)
        self.assertAlmostEqual(
            aggregate['minimum_keep_alive_request_percent'],
            99.9996,
        )
        self.assertEqual(
            aggregate['max_load_generator_total_cpu_capacity_used_percent'],
            24.5,
        )
        self.assertEqual(
            aggregate['max_apachebench_single_core_utilization_percent'],
            98.0,
        )
        self.assertAlmostEqual(
            aggregate['max_load_generator_network_utilization_percent'],
            104.8576,
        )
        self.assertIn('load_generator_saturation_warning', aggregate)

    def test_network_capacity_warns_near_the_load_generator_vnic_limit(self):
        metrics = apachebench.parse_output(SAMPLE_OUTPUT)

        apachebench.record_network_capacity(metrics, 50.0)

        self.assertAlmostEqual(metrics['measured_transfer_rate_gbps'], 52.4288)
        self.assertAlmostEqual(
            metrics['load_generator_network_utilization_percent'],
            104.8576,
        )
        self.assertIn('VNIC bandwidth', metrics['load_generator_saturation_warning'])
        with self.assertRaisesRegex(ValueError, 'network capacity'):
            apachebench.record_network_capacity(metrics, 0)

    def test_metadata_records_httpd_tuning_and_selected_mode(self):
        value = apachebench.metadata('keep_alive', options())

        self.assertTrue(value['keep_alive'])
        self.assertEqual(value['httpd_keep_alive'], 'On')
        self.assertEqual(value['httpd_max_request_workers'], 16384)
        self.assertEqual(value['httpd_listen_backlog'], 10000)


if __name__ == '__main__':
    unittest.main()
