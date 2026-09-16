import html
import json
import tempfile
import unittest
from pathlib import Path

from app import comparison


def aws_plan(**overrides):
    plan = {
        'provider': 'aws',
        'region': 'us-east-2',
        'shape': 'c7i.2xlarge',
        'ocpus': 8,
        'memory_gb': 16,
        'benchmarks': ['apachebench'],
        'llm_benchmarks': [],
        'apachebench': {
            'workloads': ['new_connections'],
            'request_count': 500000,
            'concurrency': 100,
            'response_size_kib': 64,
            'warmup_requests': 10000,
            'trials': 3,
        },
    }
    plan.update(overrides)
    return plan


def apache_result(value=1000.0, *, status='completed'):
    return {
        'id': 'apachebench_new_connections',
        'name': 'ApacheBench — New Connections',
        'status': status,
        'started_at': '2026-08-26T12:00:00+00:00',
        'duration_seconds': 61.5,
        'metadata': {
            'provider': 'AWS',
            'architecture': 'x86_64',
            'httpd_mpm': 'event',
            'httpd_keep_alive': 'On',
        },
        'metrics': {
            'mean_requests_per_second': value,
            'standard_deviation_requests_per_second': 25.0,
            'mean_time_per_request_ms': 10.0,
            'mean_p50_ms': 8.0,
            'mean_p90_ms': 15.0,
            'mean_p95_ms': 18.0,
            'mean_p99_ms': 25.0,
            'mean_transfer_rate_kib_per_second': 64000.0,
        },
        'command': 'ab -n 500000 http://10.0.0.1/',
        'output': 'raw benchmark output',
    }


def storage_target_metadata(kind='instance_local_nvme', **overrides):
    local = kind == 'instance_local_nvme'
    metadata = {
        'storage_target_contract': comparison.STORAGE_TARGET_CONTRACT,
        'storage_target_policy': comparison.STORAGE_TARGET_POLICY,
        'storage_target_kind': kind,
        'storage_target_verification': (
            comparison.STORAGE_TARGET_VERIFICATION[kind]
        ),
        'storage_target_transport': 'nvme',
        'storage_target_model': (
            'Local NVMe SSD' if local else 'gp3'
        ),
        'storage_target_capacity_bytes': (
            1_750_000_000_000 if local else 1_099_511_627_776
        ),
        'storage_target_device_count': 1,
        'storage_target_layout': comparison.STORAGE_TARGET_LAYOUT,
        'storage_target_filesystem': 'xfs',
        'storage_target_mount_point': (
            '/benchmark-local' if local else '/data'
        ),
    }
    metadata.update(overrides)
    return metadata


def artifact(run_id, value, *, plan=None):
    return comparison.build_results_artifact({
        'id': run_id,
        'status': 'destroyed',
        'benchmark_status': 'complete',
        'created_at': '2026-08-26T12:00:00+00:00',
        'updated_at': '2026-08-26T12:02:00+00:00',
        'plan': plan or aws_plan(),
        'results': [apache_result(value)],
    })


class ResultArtifactTests(unittest.TestCase):
    def test_artifact_is_versioned_and_excludes_raw_or_sensitive_data(self):
        result = apache_result()
        result['setup_output'] = 'private setup details'
        result['metadata'].update({
            'target_private_ip': '10.0.0.4',
            'auth_token': 'secret-token',
            'nested': {'command': 'curl example.test', 'safe': 'yes'},
        })
        result['metrics']['invalid'] = float('nan')
        job = {
            'id': 'safe-run',
            'status': 'complete',
            'benchmark_status': 'complete',
            'updated_at': '2026-08-26T12:02:00+00:00',
            'plan': {
                **aws_plan(),
                'ssh_private_key': 'private',
                'gcp_project_id': 'account-identifier',
            },
            'results': [result],
        }

        document = comparison.build_results_artifact(job)
        encoded = json.dumps(document)

        self.assertEqual(document['$schema'], comparison.RESULTS_SCHEMA)
        self.assertEqual(document['schema_version'], 1)
        self.assertEqual(document['provenance'], 'results_artifact')
        self.assertNotIn('raw benchmark output', encoded)
        self.assertNotIn('private setup details', encoded)
        self.assertNotIn('curl example.test', encoded)
        self.assertNotIn('secret-token', encoded)
        self.assertNotIn('10.0.0.4', encoded)
        self.assertNotIn('ssh_private_key', encoded)
        self.assertNotIn('gcp_project_id', document['plan'])
        self.assertEqual(
            document['results'][0]['metadata']['nested'],
            {'safe': 'yes'},
        )
        self.assertNotIn('invalid', document['results'][0]['metrics'])
        self.assertFalse(
            document['results'][0]['comparison']['contract_unknown']
        )

    def test_writer_is_atomic_and_reader_validates_the_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / 'run-1'
            path = comparison.write_results_artifact(
                run,
                {
                    'id': 'run-1',
                    'status': 'complete',
                    'benchmark_status': 'complete',
                    'plan': aws_plan(),
                    'results': [apache_result()],
                },
            )

            loaded = comparison.read_results_artifact(run)

            self.assertEqual(path, run / 'results.json')
            self.assertEqual(loaded['run']['id'], 'run-1')
            self.assertEqual(len(loaded['results']), 1)
            self.assertEqual(list(run.glob('.results-*.tmp')), [])

            path.write_text(json.dumps({'schema_version': 99}))
            with self.assertRaisesRegex(ValueError, 'unknown schema'):
                comparison.read_results_artifact(path)


class MetricRegistryTests(unittest.TestCase):
    def test_registry_covers_every_canonical_result_and_subtests(self):
        expected = {
            'sysbench_cpu', 'sysbench_memory', 'sysbench_fileio', 'stream',
            'fio', 'iperf_tcp', 'iperf_udp', 'iperf_sctp',
            'apachebench_new_connections', 'apachebench_keep_alive',
            'deathstarbench', 'llama_bench',
            'phoronix_compress_7zip', 'phoronix_openssl',
            'phoronix_build_linux_kernel', 'phoronix_tinymembench',
        }
        self.assertEqual(comparison.CANONICAL_RESULT_IDS, expected)
        self.assertEqual(
            {spec['subtest'] for spec in comparison.metric_specs('stream')},
            {'copy', 'scale', 'add', 'triad'},
        )
        self.assertEqual(len(comparison.metric_specs('fio')), 8)
        llama = comparison.metric_specs('llama_bench')
        self.assertEqual(len(llama), 4)
        self.assertTrue(all(spec['uncertainty_key'] for spec in llama))
        self.assertEqual(comparison.metric_specs('unknown'), [])

    def test_phoronix_configurations_become_separate_metrics_with_uncertainty(self):
        result = {
            'id': 'phoronix_compress_7zip',
            'status': 'completed',
            'metrics': {
                'measurement_count': 2,
                'measurements': [
                    {
                        'profile': 'pts/compress-7zip-1.13.1',
                        'configuration': 'Compression Rating',
                        'score': 31000,
                        'unit': 'MIPS',
                        'direction': 'Higher is better',
                        'measured_trials': [30000, 31000, 32000],
                    },
                    {
                        'profile': 'pts/compress-7zip-1.13.1',
                        'configuration': 'Decompression Rating',
                        'score': 20000,
                        'unit': 'MIPS',
                        'direction': 'Higher is better',
                        'measured_trials': [19500, 20000, 20500],
                    },
                ],
            },
        }

        metrics = comparison.extract_chart_metrics(result)

        self.assertEqual(len(metrics), 2)
        self.assertNotEqual(metrics[0]['key'], metrics[1]['key'])
        self.assertEqual(metrics[0]['direction'], 'higher_is_better')
        self.assertGreater(metrics[0]['uncertainty'], 0)

    def test_unknown_nonfinite_and_failed_metrics_are_not_charted(self):
        self.assertEqual(comparison.extract_chart_metrics({
            'id': 'new_benchmark',
            'status': 'completed',
            'metrics': {'score': 5},
        }), [])
        self.assertEqual(comparison.extract_chart_metrics({
            'id': 'sysbench_cpu',
            'status': 'completed',
            'metrics': {'events_per_second': float('inf')},
        }), [])
        self.assertEqual(comparison.extract_chart_metrics({
            'id': 'sysbench_cpu',
            'status': 'failed',
            'metrics': {'events_per_second': 10},
        }), [])


class WorkloadContractTests(unittest.TestCase):
    def test_legacy_storage_contract_fingerprints_remain_stable(self):
        sysbench = comparison.workload_fingerprint(
            'sysbench_fileio',
            aws_plan(benchmarks=['sysbench']),
            {},
        )
        fio = comparison.workload_fingerprint(
            'fio',
            aws_plan(benchmarks=['fio']),
            {},
        )

        self.assertEqual(comparison.WORKLOAD_CONTRACT_VERSION, 3)
        self.assertEqual(
            sysbench['contract']['settings'],
            {
                'tool': 'sysbench',
                'workload': 'fileio_random_read_write',
                'duration_seconds': 60,
                'file_total_size_gib': 4,
                'data_path': 'additional_volume',
            },
        )
        self.assertEqual(
            sysbench['fingerprint'],
            '59bfb65757004f486913990e94eec0b7540b0f97b478dadc07f9a21c0e6b126a',
        )
        self.assertEqual(
            fio['fingerprint'],
            '0a03b5c33351b0c9b6f5f53e185e8b072b5f9c86008f8c9da96298279d93bd89',
        )
        self.assertNotIn('provenance', sysbench)
        self.assertNotIn('provenance', fio)

    def test_new_storage_policy_separates_from_legacy(self):
        plan = aws_plan(benchmarks=['fio'])
        legacy = comparison.workload_fingerprint('fio', plan, {})
        selected = comparison.workload_fingerprint(
            'fio',
            plan,
            storage_target_metadata(),
        )

        differences = comparison.explain_workload_mismatch(legacy, selected)

        self.assertFalse(selected['contract_unknown'])
        self.assertNotEqual(legacy['fingerprint'], selected['fingerprint'])
        paths = {difference['path'] for difference in differences}
        self.assertIn('settings.data_path', paths)
        self.assertIn('settings.storage_target_policy', paths)

    def test_actual_storage_hardware_is_provenance_not_workload(self):
        plan = aws_plan(benchmarks=['fio'])
        local = comparison.workload_fingerprint(
            'fio',
            plan,
            storage_target_metadata(),
        )
        fallback = comparison.workload_fingerprint(
            'fio',
            plan,
            storage_target_metadata('provisioned_data_volume'),
        )

        self.assertEqual(local['fingerprint'], fallback['fingerprint'])
        self.assertEqual(local['contract'], fallback['contract'])
        self.assertEqual(
            local['contract']['settings']['data_path'],
            'selected_benchmark_storage',
        )
        self.assertEqual(
            local['provenance']['storage_target']['kind'],
            'instance_local_nvme',
        )
        self.assertEqual(
            fallback['provenance']['storage_target']['kind'],
            'provisioned_data_volume',
        )

    def test_new_storage_metadata_fails_closed(self):
        valid = storage_target_metadata()
        invalid_records = {
            'unknown marker': {
                **valid,
                'storage_target_contract': 'v2',
            },
            'missing policy': {
                key: value for key, value in valid.items()
                if key != 'storage_target_policy'
            },
            'unknown kind': {
                **valid,
                'storage_target_kind': 'mystery_disk',
            },
            'structured kind': {
                **valid,
                'storage_target_kind': ['instance_local_nvme'],
            },
            'wrong verification': {
                **valid,
                'storage_target_verification': (
                    'manifest_bound_guest_verified_v1'
                ),
            },
            'unsupported layout': {
                **valid,
                'storage_target_layout': 'raid0_v1',
            },
            'multiple devices': {
                **valid,
                'storage_target_device_count': 2,
            },
            'boolean device count': {
                **valid,
                'storage_target_device_count': True,
            },
            'wrong filesystem': {
                **valid,
                'storage_target_filesystem': 'ext4',
            },
            'unsafe mount point': {
                **valid,
                'storage_target_mount_point': '/tmp/benchmark',
            },
            'structured mount point': {
                **valid,
                'storage_target_mount_point': {'path': '/benchmark-local'},
            },
            'mount inconsistent with kind': {
                **valid,
                'storage_target_mount_point': '/data',
            },
            'invalid capacity': {
                **valid,
                'storage_target_capacity_bytes': 0,
            },
            'non-NVMe local transport': {
                **valid,
                'storage_target_transport': 'scsi',
            },
            'structured transport': {
                **valid,
                'storage_target_transport': ['nvme'],
            },
            'missing model': {
                key: value for key, value in valid.items()
                if key != 'storage_target_model'
            },
            'invalid model': {
                **valid,
                'storage_target_model': ['not', 'text'],
            },
            'empty sanitized model': {
                **valid,
                'storage_target_model': '\x00\x01',
            },
        }

        for label, metadata in invalid_records.items():
            with self.subTest(label=label):
                contract = comparison.workload_fingerprint(
                    'fio',
                    aws_plan(benchmarks=['fio']),
                    metadata,
                )
                self.assertTrue(contract['contract_unknown'])
                self.assertIsNone(contract['fingerprint'])
                self.assertTrue(contract['issues'])
                self.assertTrue(any(
                    'storage target' in issue.lower()
                    for issue in contract['issues']
                ))

    def test_orphaned_storage_target_metadata_fails_closed(self):
        valid = storage_target_metadata()
        plan = aws_plan(benchmarks=['fio'])

        for key, value in valid.items():
            if key == 'storage_target_contract':
                continue
            with self.subTest(key=key):
                contract = comparison.workload_fingerprint(
                    'fio',
                    plan,
                    {key: value},
                )
                self.assertTrue(contract['contract_unknown'])
                self.assertIsNone(contract['fingerprint'])
                self.assertTrue(any(
                    'contract marker is missing' in issue.lower()
                    for issue in contract['issues']
                ))

        for metadata in (
            {'storage_target_contract': None},
            {'storage_target_future_field': 'value'},
        ):
            with self.subTest(metadata=metadata):
                contract = comparison.workload_fingerprint(
                    'fio',
                    plan,
                    metadata,
                )
                self.assertTrue(contract['contract_unknown'])
                self.assertIsNone(contract['fingerprint'])

    def test_new_storage_metadata_survives_result_sanitization(self):
        metadata = storage_target_metadata()
        sanitized = comparison.sanitize_result({
            'id': 'fio',
            'name': 'fio storage suite',
            'status': 'completed',
            'metadata': metadata,
            'metrics': {'read_iops': 1000.0},
        })

        self.assertEqual(sanitized['metadata'], metadata)

    def test_hardware_dimensions_do_not_change_apachebench_fingerprint(self):
        left = comparison.workload_fingerprint(
            'apachebench_new_connections',
            aws_plan(),
            {'httpd_mpm': 'event', 'target_private_ip': '10.0.0.1'},
        )
        right = comparison.workload_fingerprint(
            'apachebench_new_connections',
            {
                **aws_plan(
                    provider='gcp',
                    region='us-central1',
                    shape='c4-standard-8',
                    ocpus=16,
                    memory_gb=32,
                ),
                'gcp_zone': 'us-central1-a',
            },
            {'httpd_mpm': 'event', 'target_private_ip': '10.4.0.9'},
        )

        self.assertFalse(left['contract_unknown'])
        self.assertEqual(left['fingerprint'], right['fingerprint'])
        self.assertNotIn('10.0.0.1', json.dumps(left))

    def test_option_change_is_explained(self):
        left = comparison.workload_fingerprint(
            'apachebench_new_connections', aws_plan(), {}
        )
        changed_plan = aws_plan()
        changed_plan['apachebench'] = {
            **changed_plan['apachebench'],
            'concurrency': 200,
        }
        right = comparison.workload_fingerprint(
            'apachebench_new_connections', changed_plan, {}
        )

        differences = comparison.explain_workload_mismatch(left, right)

        self.assertNotEqual(left['fingerprint'], right['fingerprint'])
        self.assertIn(
            'settings.concurrency',
            {difference['path'] for difference in differences},
        )

    def test_oci_portable_marker_and_kernel_architecture_are_conservative(self):
        oci = aws_plan(provider='oci')
        legacy = comparison.workload_fingerprint('stream', oci, {})
        portable = comparison.workload_fingerprint(
            'stream', oci, {'portable_result_contract': 'v1'}
        )
        kernel_metadata = {
            'phoronix_profile': 'pts/build-linux-kernel-1.18.0',
            'phoronix_client_revision': 'pinned',
            'measured_trials': 3,
            'fixed_options': 'build-linux-kernel.build=defconfig',
        }
        missing_arch = comparison.workload_fingerprint(
            'phoronix_build_linux_kernel', aws_plan(), kernel_metadata
        )
        x86 = comparison.workload_fingerprint(
            'phoronix_build_linux_kernel',
            aws_plan(),
            {**kernel_metadata, 'architecture': 'amd64'},
        )
        arm = comparison.workload_fingerprint(
            'phoronix_build_linux_kernel',
            aws_plan(),
            {**kernel_metadata, 'architecture': 'arm64'},
        )

        self.assertTrue(legacy['contract_unknown'])
        self.assertFalse(portable['contract_unknown'])
        self.assertTrue(missing_arch['contract_unknown'])
        self.assertNotEqual(x86['fingerprint'], arm['fingerprint'])
        self.assertEqual(
            x86['contract']['settings']['architecture'], 'x86_64'
        )

    def test_llama_architecture_profiles_are_required_provenance(self):
        metadata = {
            'architecture': 'x86_64',
            'llama_cpp_revision': 'de699957b92f490efebad149665b0dccf127eaff',
            'model_revision': 'c1d7cb837a660d93ba28f936efb148591bfba3e9',
            'model_sha256': (
                '9fecc3b3cd76bba89d504f29b616eedf7da85b96540e490ca5824d3f7d2776a0'
            ),
            'model_quantization': 'Q4_K_M',
            'execution_backend': 'CPU',
            'llama_compiler_version': '15.2.1',
            'llama_cxx_compiler_version': '15.2.1',
            'llama_assembler_version': '2.44',
            'llama_native_optimization': False,
            'llama_cpu_build_profile': 'x86_64-avx2-portable-v1',
            'llama_cpu_cmake_options': ' '.join(
                comparison.X86_64_PORTABLE_CMAKE_OPTIONS
            ),
        }
        plan = aws_plan(benchmarks=[], llm_benchmarks=['llama_cpp'])

        baseline = comparison.workload_fingerprint(
            'llama_bench', plan, metadata
        )
        changed = comparison.workload_fingerprint(
            'llama_bench',
            plan,
            {**metadata, 'llama_assembler_version': '2.35.2'},
        )
        missing = comparison.workload_fingerprint(
            'llama_bench',
            plan,
            {key: value for key, value in metadata.items()
             if key != 'llama_compiler_version'},
        )

        self.assertFalse(baseline['contract_unknown'])
        self.assertEqual(baseline['fingerprint'], changed['fingerprint'])
        self.assertNotEqual(
            baseline['provenance'],
            changed['provenance'],
        )
        self.assertTrue(missing['contract_unknown'])
        arm = comparison.workload_fingerprint(
            'llama_bench',
            plan,
            {
                **metadata,
                'architecture': 'arm64',
                'llama_cpu_build_profile': (
                    comparison.AARCH64_NATIVE_CPU_PROFILE
                ),
                'llama_native_optimization': True,
                'llama_cpu_cmake_options': ' '.join(
                    comparison.AARCH64_NATIVE_CMAKE_OPTIONS
                ),
            },
        )
        self.assertEqual(baseline['fingerprint'], arm['fingerprint'])
        self.assertEqual(
            arm['provenance']['cpu_build']['architecture'],
            'aarch64',
        )

        legacy_native_metadata = {
            **metadata,
            'llama_cpu_build_profile': 'native',
            'llama_native_optimization': True,
            'llama_cpu_cmake_options': '-DGGML_NATIVE=ON',
        }
        legacy_native = comparison.workload_fingerprint(
            'llama_bench',
            plan,
            legacy_native_metadata,
        )
        changed_legacy_toolchain = comparison.workload_fingerprint(
            'llama_bench',
            plan,
            {
                **legacy_native_metadata,
                'llama_assembler_version': '2.35.2',
            },
        )
        self.assertNotEqual(
            baseline['fingerprint'],
            legacy_native['fingerprint'],
        )
        self.assertNotEqual(
            legacy_native['fingerprint'],
            changed_legacy_toolchain['fingerprint'],
        )
        self.assertEqual(
            baseline['contract']['settings']['cpu_build_method'],
            comparison.LLAMA_ARCHITECTURE_PROFILE_METHOD,
        )
        historical_native_metadata = dict(legacy_native_metadata)
        historical_native_metadata.pop('llama_cpu_build_profile')
        historical_native = comparison.workload_fingerprint(
            'llama_bench',
            plan,
            historical_native_metadata,
        )
        self.assertFalse(historical_native['contract_unknown'])
        self.assertEqual(
            historical_native['contract']['settings']['cpu_build_profile'],
            'native',
        )
        for inconsistent in (
            {
                **metadata,
                'llama_cpu_build_profile': 'native',
                'llama_native_optimization': False,
            },
            {
                **metadata,
                'llama_cpu_build_profile': 'x86_64-avx2-portable-v1',
                'llama_native_optimization': True,
            },
            {
                **metadata,
                'llama_cpu_build_profile': 'future-unknown-profile',
            },
            {
                **metadata,
                'architecture': 'aarch64',
                'llama_cpu_build_profile': (
                    comparison.X86_64_PORTABLE_CPU_PROFILE
                ),
            },
        ):
            with self.subTest(inconsistent=inconsistent):
                rejected = comparison.workload_fingerprint(
                    'llama_bench',
                    plan,
                    inconsistent,
                )
                self.assertTrue(rejected['contract_unknown'])
                self.assertTrue(any(
                    'inconsistent' in issue or 'not valid' in issue
                    for issue in rejected['issues']
                ))
        malformed_profile = comparison.workload_fingerprint(
            'llama_bench',
            plan,
            {
                **metadata,
                'llama_cpu_build_profile': ['not', 'a', 'profile'],
            },
        )
        self.assertTrue(malformed_profile['contract_unknown'])
        self.assertIn(
            'llama_cpu_build_profile',
            malformed_profile['issues'][0],
        )
        malformed_options = comparison.workload_fingerprint(
            'llama_bench',
            plan,
            {
                **metadata,
                'llama_cpu_cmake_options': '-DGGML_NATIVE=OFF',
            },
        )
        self.assertTrue(malformed_options['contract_unknown'])
        self.assertTrue(any(
            'architecture-specific CPU build options' in issue
            for issue in malformed_options['issues']
        ))
        malformed_arm_options = comparison.workload_fingerprint(
            'llama_bench',
            plan,
            {
                **metadata,
                'architecture': 'aarch64',
                'llama_cpu_build_profile': 'native',
                'llama_native_optimization': True,
                'llama_cpu_cmake_options': '-DGGML_NATIVE=OFF',
            },
        )
        self.assertTrue(malformed_arm_options['contract_unknown'])
        unsupported_architecture = comparison.workload_fingerprint(
            'llama_bench',
            plan,
            {**metadata, 'architecture': 'riscv64'},
        )
        self.assertTrue(unsupported_architecture['contract_unknown'])
        self.assertTrue(any(
            'architecture metadata is unsupported' in issue
            for issue in unsupported_architecture['issues']
        ))
        for provenance_key in (
            'cpu_build_profile',
            'native_cpu_optimization',
            'cpu_cmake_options',
            'compiler_version',
        ):
            self.assertNotIn(
                provenance_key,
                baseline['contract']['settings'],
            )
        self.assertEqual(
            baseline['provenance']['cpu_build']['profile'],
            'x86_64-avx2-portable-v1',
        )
        self.assertEqual(
            baseline['provenance']['build_toolchain']['compiler_version'],
            '15.2.1',
        )
        self.assertEqual(
            baseline['provenance']['build_toolchain'][
                'cxx_compiler_version'
            ],
            '15.2.1',
        )


class ComparisonPayloadTests(unittest.TestCase):
    def test_mixed_storage_targets_warn_and_preserve_value_provenance(self):
        plan = aws_plan(benchmarks=['fio'])

        def document(run_id, value, metadata):
            return comparison.build_results_artifact({
                'id': run_id,
                'status': 'destroyed',
                'benchmark_status': 'complete',
                'plan': plan,
                'results': [{
                    'id': 'fio',
                    'name': 'fio storage suite',
                    'status': 'completed',
                    'metadata': metadata,
                    'metrics': {'read_iops': value},
                }],
            })

        payload = comparison.build_comparison_payload([
            document('local-run', 1000.0, storage_target_metadata()),
            document(
                'fallback-run',
                500.0,
                storage_target_metadata('provisioned_data_volume'),
            ),
        ])

        self.assertEqual(payload['excluded'], [])
        self.assertEqual(payload['mismatches'], [])
        self.assertEqual(len(payload['charts']), 1)
        chart = payload['charts'][0]
        self.assertIn(
            'different storage target classes',
            chart['provenance_warnings'][0],
        )
        values = {value['run_id']: value for value in chart['values']}
        self.assertEqual(
            values['local-run']['provenance']['storage_target']['kind'],
            'instance_local_nvme',
        )
        self.assertEqual(
            values['fallback-run']['provenance']['storage_target']['kind'],
            'provisioned_data_volume',
        )
        self.assertIn(
            'Storage target: instance-local NVMe',
            values['local-run']['warnings'][0],
        )
        self.assertIn(
            'Storage target: provisioned data volume',
            values['fallback-run']['warnings'][0],
        )
        group_values = payload['groups'][0]['metrics'][0]['values']
        self.assertTrue(all('provenance' in value for value in group_values))

    def test_storage_model_and_capacity_differences_are_disclosed(self):
        plan = aws_plan(benchmarks=['fio'])

        def document(run_id, metadata):
            return comparison.build_results_artifact({
                'id': run_id,
                'status': 'destroyed',
                'benchmark_status': 'complete',
                'plan': plan,
                'results': [{
                    'id': 'fio',
                    'name': 'fio storage suite',
                    'status': 'completed',
                    'metadata': metadata,
                    'metrics': {'read_iops': 1000.0},
                }],
            })

        variants = {
            'model': storage_target_metadata(
                storage_target_model='OCI local NVMe',
            ),
            'capacity': storage_target_metadata(
                storage_target_capacity_bytes=6_800_000_000_000,
            ),
        }
        for label, changed in variants.items():
            with self.subTest(label=label):
                payload = comparison.build_comparison_payload([
                    document('baseline-run', storage_target_metadata()),
                    document('changed-run', changed),
                ])

                chart = payload['charts'][0]
                self.assertIn(
                    'device models, or capacities',
                    chart['provenance_warnings'][0],
                )
                values = {item['run_id']: item for item in chart['values']}
                self.assertIn(
                    'Storage target: instance-local NVMe',
                    values['baseline-run']['warnings'][0],
                )
                self.assertIn(
                    'Storage target: instance-local NVMe',
                    values['changed-run']['warnings'][0],
                )

    def test_portable_llama_toolchains_are_comparable_with_provenance_warning(self):
        plan = aws_plan(benchmarks=[], llm_benchmarks=['llama_bench'])
        base_metadata = {
            'architecture': 'x86_64',
            'llama_cpp_revision': 'de699957b92f490efebad149665b0dccf127eaff',
            'model_revision': 'c1d7cb837a660d93ba28f936efb148591bfba3e9',
            'model_sha256': (
                '9fecc3b3cd76bba89d504f29b616eedf7da85b96540e490ca5824d3f7d2776a0'
            ),
            'model_quantization': 'Q4_K_M',
            'execution_backend': 'CPU',
            'llama_native_optimization': False,
            'llama_cpu_build_profile': 'x86_64-avx2-portable-v1',
            'llama_cpu_cmake_options': ' '.join(
                comparison.X86_64_PORTABLE_CMAKE_OPTIONS
            ),
        }

        def document(
            run_id,
            compiler_version,
            assembler_version,
            value,
            metadata_overrides=None,
        ):
            metadata = {
                **base_metadata,
                'llama_compiler_version': compiler_version,
                'llama_cxx_compiler_version': compiler_version,
                'llama_assembler_version': assembler_version,
                **(metadata_overrides or {}),
            }
            metrics = {}
            for prefix in (
                'prompt_512',
                'prompt_2048',
                'generation_128',
                'generation_512',
            ):
                metrics[f'{prefix}_tokens_per_second'] = value
                metrics[f'{prefix}_stddev_tokens_per_second'] = 1.0
            return comparison.build_results_artifact({
                'id': run_id,
                'status': 'destroyed',
                'benchmark_status': 'complete',
                'plan': plan,
                'results': [{
                    'id': 'llama_bench',
                    'name': 'llama.cpp throughput (CPU)',
                    'status': 'completed',
                    'metadata': metadata,
                    'metrics': metrics,
                }],
            })

        payload = comparison.build_comparison_payload([
            document('oci-run', '12.2.1', '2.38', 100.0),
            document('aws-run', '11.5.0', '2.41', 110.0),
        ])

        self.assertEqual(len(payload['charts']), 4)
        self.assertEqual(payload['excluded'], [])
        self.assertEqual(payload['mismatches'], [])
        for chart in payload['charts']:
            self.assertIn(
                'Build toolchains differ',
                chart['provenance_warnings'][0],
            )
            warnings = {
                value['run_id']: value['warnings'][0]
                for value in chart['values']
            }
            self.assertIn('GCC/G++ 12.2.1', warnings['oci-run'])
            self.assertIn('GNU as 2.38', warnings['oci-run'])
            self.assertIn('GCC/G++ 11.5.0', warnings['aws-run'])
            self.assertIn('GNU as 2.41', warnings['aws-run'])

        same_toolchain = comparison.build_comparison_payload([
            document('run-a', '12.2.1', '2.38', 100.0),
            document('run-b', '12.2.1', '2.38', 110.0),
        ])
        self.assertTrue(same_toolchain['charts'])
        self.assertTrue(all(
            chart['provenance_warnings'] == []
            and all(not value['warnings'] for value in chart['values'])
            for chart in same_toolchain['charts']
        ))

        cross_architecture = comparison.build_comparison_payload([
            document('oci-run', '12.2.1', '2.38', 100.0),
            document('aws-run', '11.5.0', '2.41', 110.0),
            document(
                'gcp-arm-run',
                '15.2.1',
                '2.44',
                70.0,
                {
                    'architecture': 'aarch64',
                    'llama_cpu_build_profile': 'native',
                    'llama_native_optimization': True,
                    'llama_cpu_cmake_options': '-DGGML_NATIVE=ON',
                },
            ),
        ])
        self.assertEqual(len(cross_architecture['charts']), 4)
        self.assertEqual(cross_architecture['excluded'], [])
        self.assertEqual(cross_architecture['mismatches'], [])
        for chart in cross_architecture['charts']:
            self.assertEqual(len(chart['values']), 3)
            notes = ' '.join(chart['provenance_warnings'])
            self.assertIn('architecture-appropriate CPU builds', notes)
            self.assertIn('Build toolchains differ', notes)
            warnings = {
                value['run_id']: value['warnings'][0]
                for value in chart['values']
            }
            self.assertIn(
                'CPU build: aarch64 · native',
                warnings['gcp-arm-run'],
            )
            self.assertIn(
                'CPU build: x86_64 · x86_64-avx2-portable-v1',
                warnings['aws-run'],
            )

    def test_payload_charts_largest_matching_cohort_and_explains_mismatch(self):
        changed = aws_plan()
        changed['apachebench'] = {
            **changed['apachebench'],
            'concurrency': 200,
        }
        payload = comparison.build_comparison_payload([
            artifact('run-a', 1000),
            artifact('run-b', 1100),
            artifact('run-c', 1200, plan=changed),
        ])

        request_chart = next(
            chart for chart in payload['charts']
            if chart['metric_id'] == 'mean_requests_per_second'
        )
        self.assertEqual(
            [value['run_id'] for value in request_chart['values']],
            ['run-a', 'run-b'],
        )
        self.assertEqual(request_chart['direction'], 'higher')
        self.assertEqual(request_chart['values'][0]['error_low'], 975.0)
        self.assertEqual(request_chart['values'][0]['error_high'], 1025.0)
        self.assertTrue(request_chart['primary'])
        self.assertEqual(len(payload['mismatches']), 2)
        self.assertTrue(any(
            item['run_id'] == 'run-c'
            and item['result_name'] == 'ApacheBench — New Connections'
            and any('differ' in reason for reason in item['reasons'])
            for item in payload['excluded']
        ))
        self.assertEqual(payload['runs'][0]['ocpus'], 8)

    def test_dynamic_subtests_have_distinct_stable_chart_ids(self):
        plan = aws_plan(
            benchmarks=['phoronix'],
            phoronix={'profiles': ['compress_7zip']},
        )

        def document(run_id, offset):
            return comparison.build_results_artifact({
                'id': run_id,
                'plan': plan,
                'results': [{
                    'id': 'phoronix_compress_7zip',
                    'name': 'Phoronix — 7-Zip Compression',
                    'status': 'completed',
                    'metadata': {
                        'phoronix_profile': 'pts/compress-7zip-1.13.1',
                        'phoronix_client_revision': 'pinned',
                        'measured_trials': 3,
                    },
                    'metrics': {
                        'measurements': [
                            {
                                'profile': 'pts/compress-7zip-1.13.1',
                                'configuration': 'Compression Rating',
                                'score': 31000 + offset,
                                'unit': 'MIPS',
                                'direction': 'Higher is better',
                                'measured_trials': [30000, 31000, 32000],
                            },
                            {
                                'profile': 'pts/compress-7zip-1.13.1',
                                'configuration': 'Decompression Rating',
                                'score': 21000 + offset,
                                'unit': 'MIPS',
                                'direction': 'Higher is better',
                                'measured_trials': [20000, 21000, 22000],
                            },
                        ],
                    },
                }],
            })

        first = comparison.build_comparison_payload([
            document('run-a', 0),
            document('run-b', 100),
        ])
        second = comparison.build_comparison_payload([
            document('run-a', 0),
            document('run-b', 100),
        ])

        self.assertEqual(len(first['charts']), 2)
        self.assertEqual(
            {chart['id'] for chart in first['charts']},
            {chart['id'] for chart in second['charts']},
        )
        self.assertEqual(len({chart['id'] for chart in first['charts']}), 2)

    def test_unknown_metric_is_excluded_with_a_reason(self):
        document = comparison.build_results_artifact({
            'id': 'unknown-run',
            'plan': aws_plan(benchmarks=['new_benchmark']),
            'results': [{
                'id': 'new_benchmark',
                'name': 'New benchmark',
                'status': 'completed',
                'metrics': {'score': 5},
            }],
        })

        payload = comparison.build_comparison_payload([document])

        self.assertEqual(payload['charts'], [])
        self.assertTrue(any(
            'No registered chart metrics' in reason
            for item in payload['excluded']
            for reason in item['reasons']
        ))


class LegacyReportTests(unittest.TestCase):
    def report(self, plan):
        nested = [{
            'profile': 'pts/compress-7zip-1.13.1',
            'configuration': 'Compression Rating',
            'score': 31000,
            'unit': 'MIPS',
            'direction': 'Higher is better',
            'measured_trials': [30000, 31000, 32000],
        }]
        plan_html = html.escape(json.dumps(plan, indent=2))
        nested_html = html.escape(str(nested))
        return (
            '<!doctype html><html><body><h1>AWS Compute Benchmark Report</h1>'
            '<p>Run legacy-1 · 2026-08-26T12:00:00+00:00</p>'
            f'<h2>Configuration</h2><pre>{plan_html}</pre>'
            '<section><h2>Sysbench — CPU</h2>'
            '<p>Started 2026-08-26T12:01:00+00:00 · 60.1 seconds</p>'
            '<p>Status: <strong>completed</strong></p>'
            '<h3>Environment</h3><table><tbody>'
            '<tr><th>Provider</th><td>AWS</td></tr>'
            '<tr><th>Architecture</th><td>x86_64</td></tr>'
            '</tbody></table>'
            '<h3>Measured results</h3><table><tbody>'
            '<tr><th>Events Per Second</th><td>1234.5</td></tr>'
            '</tbody></table><h3>Command</h3><p><code>secret command</code></p>'
            '<h3>Raw benchmark output</h3><pre>secret output</pre></section>'
            '<section><h2>Phoronix — 7-Zip Compression</h2>'
            '<p>Started 2026-08-26T12:02:00+00:00 · 90 seconds</p>'
            '<p>Status: <strong>completed</strong></p>'
            '<h3>Environment</h3><table><tbody>'
            '<tr><th>Phoronix Profile</th><td>pts/compress-7zip-1.13.1</td></tr>'
            '<tr><th>Phoronix Client Revision</th><td>pinned</td></tr>'
            '<tr><th>Measured Trials</th><td>3</td></tr>'
            '</tbody></table>'
            '<h3>Measured results</h3><table><tbody>'
            f'<tr><th>Measurements</th><td>{nested_html}</td></tr>'
            '</tbody></table></section></body></html>'
        )

    def test_legacy_report_recovers_safe_metrics_and_known_contracts(self):
        plan = aws_plan(
            benchmarks=['sysbench', 'phoronix'],
            sysbench={'workloads': ['cpu']},
            phoronix={'profiles': ['compress_7zip']},
        )
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / 'legacy-1'
            run.mkdir()
            (run / 'report.html').write_text(self.report(plan))

            document = comparison.load_results_document(run)

        encoded = json.dumps(document)
        self.assertEqual(document['provenance'], 'legacy_report')
        self.assertFalse(document['contract_unknown'])
        self.assertEqual(document['results'][0]['id'], 'sysbench_cpu')
        self.assertEqual(
            document['results'][0]['metrics']['events_per_second'], 1234.5
        )
        self.assertIsInstance(
            document['results'][1]['metrics']['measurements'], list
        )
        self.assertFalse(
            document['results'][1]['comparison']['contract_unknown']
        )
        self.assertNotIn('secret command', encoded)
        self.assertNotIn('secret output', encoded)

    def test_loader_prefers_results_json_over_legacy_html(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / 'run-1'
            run.mkdir()
            (run / 'report.html').write_text(self.report(aws_plan()))
            comparison.write_results_artifact(run, {
                'id': 'normalized-run',
                'plan': aws_plan(),
                'results': [apache_result()],
            })

            document = comparison.load_results_document(run)

        self.assertEqual(document['provenance'], 'results_artifact')
        self.assertEqual(document['run']['id'], 'normalized-run')

    def test_loader_uses_a_valid_embedded_artifact_before_scalar_tables(self):
        embedded = artifact('embedded-run', 1250)
        encoded = json.dumps(embedded).replace('<', r'\u003c')
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / 'embedded-run'
            run.mkdir()
            (run / 'report.html').write_text(
                '<html><body><script id="benchmark-results" '
                f'type="application/json">{encoded}</script></body></html>'
            )

            document = comparison.load_results_document(run)

        self.assertEqual(document['provenance'], 'embedded_results_artifact')
        self.assertEqual(document['run']['id'], 'embedded-run')


if __name__ == '__main__':
    unittest.main()
