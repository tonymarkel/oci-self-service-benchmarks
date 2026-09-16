import json
import subprocess
import tempfile
import unittest

from app.guests import amazon_linux


class AmazonLinuxCommandTests(unittest.TestCase):
    def assert_valid_bash(self, command):
        with tempfile.NamedTemporaryFile(mode='w') as script:
            script.write(command)
            script.flush()
            subprocess.run(
                ['bash', '-n', script.name],
                check=True,
                capture_output=True,
                text=True,
            )

    def test_readiness_waits_for_cloud_init_dns_and_active_dnf_repo(self):
        command = amazon_linux.readiness_command()

        self.assertIn('cloud-init status --wait', command)
        self.assertIn('cdn.amazonlinux.com', command)
        self.assertIn('github.com', command)
        self.assertIn('openbenchmarking.org', command)
        self.assertIn('dnf -q', command)
        self.assertIn('makecache --refresh', command)
        self.assertIn('for attempt in $(seq 1 24)', command)
        self.assertIn('command -v curl', command)
        self.assertIn('curl-minimal package provided by the standard AMI', command)
        self.assertNotIn('oci.oraclecloud.com', command)
        self.assertNotIn('ol9_', command)
        self.assert_valid_bash(command)

    def test_readiness_and_dnf_reject_shell_injection(self):
        with self.assertRaisesRegex(ValueError, 'hostname'):
            amazon_linux.readiness_command(['github.com; id'])
        with self.assertRaisesRegex(ValueError, 'package'):
            amazon_linux.dnf_install_command(['gcc; id'])

    def test_readiness_hosts_are_limited_to_selected_benchmarks(self):
        sysbench = amazon_linux.benchmark_readiness_hosts(['sysbench'])
        stream = amazon_linux.benchmark_readiness_hosts(['stream'])
        phoronix = amazon_linux.benchmark_readiness_hosts(['phoronix'])
        llama = amazon_linux.benchmark_readiness_hosts(
            [],
            llm_benchmarks=['llama_bench'],
        )

        self.assertEqual(
            sysbench,
            ('cdn.amazonlinux.com', 'github.com', 'codeload.github.com'),
        )
        self.assertEqual(
            stream,
            ('cdn.amazonlinux.com', 'raw.githubusercontent.com'),
        )
        self.assertEqual(
            phoronix,
            ('cdn.amazonlinux.com', 'github.com', 'openbenchmarking.org'),
        )
        self.assertEqual(
            llama,
            ('cdn.amazonlinux.com', 'github.com', 'huggingface.co'),
        )

    def test_dnf_install_is_bounded_and_retried(self):
        command = amazon_linux.dnf_install_command({'gcc', 'make'})

        self.assertIn('sudo timeout 480 dnf -y', command)
        self.assertIn('--setopt=retries=10', command)
        self.assertIn('dnf clean expire-cache', command)
        self.assertEqual(command.count('gcc'), 1)
        self.assert_valid_bash(command)

    def test_sysbench_build_is_pinned_checksum_verified_and_idempotent(self):
        command = amazon_linux.sysbench_install_command()

        self.assertIn(amazon_linux.SYSBENCH_VERSION, command)
        self.assertIn(amazon_linux.SYSBENCH_SOURCE_URL, command)
        self.assertIn(amazon_linux.SYSBENCH_SOURCE_SHA256, command)
        self.assertIn('sha256sum -c -', command)
        self.assertIn('./configure --without-mysql', command)
        self.assertIn('make -j"$(nproc)"', command)
        self.assertIn('command -v sysbench', command)
        self.assertNotIn('packagecloud', command)
        self.assertNotIn('docker', command.lower())
        self.assert_valid_bash(command)

    def test_stream_source_and_commands_are_reproducibly_pinned(self):
        command = amazon_linux.stream_benchmark_command(8)

        self.assertIn(amazon_linux.STREAM_REVISION, command)
        self.assertIn(amazon_linux.STREAM_SOURCE_SHA256, command)
        self.assertIn('sha256sum -c -', command)
        self.assertIn('gcc -O3 -fopenmp', command)
        self.assertIn('/sys/devices/system/cpu/', command)
        self.assertIn('MemAvailable', command)
        self.assertIn('MIN_ARRAY_BYTES=$((4 * LLC_BYTES))', command)
        self.assertIn('MAX_STREAM_BYTES=$((MEM_AVAILABLE_BYTES / 2))', command)
        self.assertIn('-DSTREAM_ARRAY_SIZE="$ARRAY_ELEMENTS"', command)
        self.assertIn('OCI_STREAM_SIZING', command)
        self.assertIn('x86_64) MODEL_FLAGS=(-mcmodel=medium)', command)
        self.assertIn(
            'aarch64|arm64) MODEL_FLAGS=(-mcmodel=large -fno-pie -no-pie)',
            command,
        )
        self.assertIn('OMP_NUM_THREADS=8', command)
        self.assert_valid_bash(command)

    def test_commands_include_cpu_memory_and_storage_contracts(self):
        commands = amazon_linux.benchmark_commands(8.0)

        self.assertEqual(
            set(commands),
            {
                'sysbench_cpu',
                'sysbench_memory',
                'sysbench_fileio',
                'fio',
                'stream',
            },
        )
        self.assertIn('--threads=8', commands['sysbench_cpu'][1])
        self.assertIn('--cpu-max-prime=10000', commands['sysbench_cpu'][1])
        self.assertIn('--threads=8', commands['sysbench_memory'][1])
        self.assertIn('--memory-block-size=1M', commands['sysbench_memory'][1])
        self.assertIn('--memory-total-size=0', commands['sysbench_memory'][1])
        self.assertIn('cd /data', commands['sysbench_fileio'][1])
        self.assertIn('OCI_SYSBENCH_FILEIO_ERRORS=0', commands['sysbench_fileio'][1])
        self.assertIn('read write randread randwrite', commands['fio'][1])
        self.assertIn('set -euo pipefail', commands['fio'][1])
        self.assert_valid_bash(commands['sysbench_fileio'][1])
        self.assert_valid_bash(commands['fio'][1])
        with self.assertRaisesRegex(ValueError, 'positive integer'):
            amazon_linux.benchmark_commands(1.5)

    def test_storage_commands_accept_only_a_safe_absolute_directory(self):
        commands = amazon_linux.benchmark_commands(
            8,
            storage_directory='/benchmark-local',
        )

        self.assertIn('cd /benchmark-local', commands['sysbench_fileio'][1])
        self.assertIn(
            '--directory=/benchmark-local',
            commands['fio'][1],
        )
        self.assert_valid_bash(commands['sysbench_fileio'][1])
        self.assert_valid_bash(commands['fio'][1])
        for directory in (
            'data', '/', '//data', '/data//nested', '/data/../tmp',
            '/data;id', ' /data',
        ):
            with self.subTest(directory=directory), self.assertRaisesRegex(
                ValueError,
                'normalized absolute path',
            ):
                amazon_linux.benchmark_commands(
                    8,
                    storage_directory=directory,
                )


class AmazonLinuxResultParserTests(unittest.TestCase):
    SYSBENCH_CPU_OUTPUT = """\
CPU speed:
    events per second:  1086.80

General statistics:
    total time:                          60.0011s
    total number of events:              65210
"""

    STREAM_OUTPUT = """\
OCI_STREAM_SIZING aggregate_llc_bytes=33554432 array_elements=16777216 array_bytes=134217728 total_bytes=402653184 mem_available_bytes=1073741824
STREAM version $Revision: 5.10 $
Array size = 16777216 (elements), Offset = 0 (elements)
Number of Threads requested = 8
Number of Threads counted = 8
Function    Best Rate MB/s  Avg time     Min time     Max time
Copy:            100000.0     0.002000     0.001000     0.003000
Scale:            90000.0     0.002100     0.001100     0.003100
Add:              80000.0     0.003000     0.002000     0.004000
Triad:            85000.0     0.003100     0.002100     0.004100
Solution Validates: avg error less than 1.000000e-13 on all three arrays
"""

    SYSBENCH_MEMORY_OUTPUT = """\
Running memory speed test with the following options:
Total operations: 125829120 (2097114.92 per second)
122880.00 MiB transferred (2047.96 MiB/sec)

General statistics:
    total time:                          60.0012s
"""

    SYSBENCH_FILEIO_OUTPUT = """\
File operations:
    reads/s:                      2037.52
    writes/s:                     1358.35
    fsyncs/s:                     0.00

Throughput:
    read, MiB/s:                  31.84
    written, MiB/s:               21.22

General statistics:
    total time:                          60.0014s
    total number of events:              203771
OCI_SYSBENCH_FILEIO_ERRORS=0
"""

    def test_stream_parser_requires_validation_sizing_threads_and_metrics(self):
        metrics = amazon_linux.parse_stream_output(
            self.STREAM_OUTPUT,
            expected_threads=8,
        )

        self.assertEqual(metrics['triad_mb_per_second'], 85000.0)
        self.assertEqual(metrics['threads'], 8)
        self.assertEqual(metrics['aggregate_llc_bytes'], 33554432)
        self.assertEqual(metrics['array_elements'], 16777216)

    def test_stream_parser_rejects_false_success_and_bad_metrics(self):
        failed = self.STREAM_OUTPUT.replace(
            'Solution Validates: avg error less than 1.000000e-13 on all '
            'three arrays',
            'Failed Validation on array a[], AvgRelAbsErr > epsilon',
        )
        with self.assertRaisesRegex(ValueError, 'validation'):
            amazon_linux.parse_stream_output(failed, expected_threads=8)
        with self.assertRaisesRegex(ValueError, 'positive and finite'):
            amazon_linux.parse_stream_output(
                self.STREAM_OUTPUT.replace('Triad:            85000.0',
                                           'Triad:                0.0'),
                expected_threads=8,
            )
        with self.assertRaisesRegex(ValueError, 'instead of 4'):
            amazon_linux.parse_stream_output(
                self.STREAM_OUTPUT,
                expected_threads=4,
            )

    def test_stream_parser_rejects_cache_or_memory_sizing_mismatch(self):
        too_small = self.STREAM_OUTPUT.replace(
            'aggregate_llc_bytes=33554432',
            'aggregate_llc_bytes=67108864',
        )
        with self.assertRaisesRegex(ValueError, 'sizing'):
            amazon_linux.parse_stream_output(too_small, expected_threads=8)

    def test_sysbench_memory_parser_requires_the_timed_interval(self):
        metrics = amazon_linux.parse_sysbench_memory_output(
            self.SYSBENCH_MEMORY_OUTPUT
        )

        self.assertEqual(metrics['throughput_mib_per_second'], 2047.96)
        self.assertEqual(metrics['elapsed_seconds'], 60.0012)
        with self.assertRaisesRegex(ValueError, 'before the requested'):
            amazon_linux.parse_sysbench_memory_output(
                self.SYSBENCH_MEMORY_OUTPUT.replace('60.0012s', '5.0012s')
            )

    def test_sysbench_cpu_parser_requires_positive_timed_results(self):
        metrics = amazon_linux.parse_sysbench_cpu_output(
            self.SYSBENCH_CPU_OUTPUT
        )

        self.assertEqual(metrics['events_per_second'], 1086.8)
        self.assertEqual(metrics['total_events'], 65210)
        self.assertEqual(metrics['elapsed_seconds'], 60.0011)
        with self.assertRaisesRegex(ValueError, 'positive and finite'):
            amazon_linux.parse_sysbench_cpu_output(
                self.SYSBENCH_CPU_OUTPUT.replace('1086.80', '0.00')
            )
        with self.assertRaisesRegex(ValueError, 'before the requested'):
            amazon_linux.parse_sysbench_cpu_output(
                self.SYSBENCH_CPU_OUTPUT.replace('60.0011s', '5.0011s')
            )

    def test_sysbench_fileio_parser_requires_timed_metrics_and_zero_errors(self):
        metrics = amazon_linux.parse_sysbench_fileio_output(
            self.SYSBENCH_FILEIO_OUTPUT
        )

        self.assertEqual(metrics['reads_per_second'], 2037.52)
        self.assertEqual(metrics['written_mib_per_second'], 21.22)
        self.assertEqual(metrics['errors'], 0)
        with self.assertRaisesRegex(ValueError, 'reported one or more errors'):
            amazon_linux.parse_sysbench_fileio_output(
                self.SYSBENCH_FILEIO_OUTPUT.replace(
                    'OCI_SYSBENCH_FILEIO_ERRORS=0',
                    'OCI_SYSBENCH_FILEIO_ERRORS=1',
                )
            )
        with self.assertRaisesRegex(ValueError, 'before the requested'):
            amazon_linux.parse_sysbench_fileio_output(
                self.SYSBENCH_FILEIO_OUTPUT.replace('60.0014s', '5.0014s')
            )

    def test_fio_parser_requires_four_successful_timed_json_results(self):
        documents = []
        for name in ('read', 'write', 'randread', 'randwrite'):
            direction = 'read' if name in {'read', 'randread'} else 'write'
            documents.append(json.dumps({
                'jobs': [{
                    'jobname': name,
                    'error': 0,
                    direction: {
                        'bw_bytes': 1048576,
                        'iops': 256.5,
                        'runtime': 60001,
                    },
                }],
            }))
        output = '\n'.join(documents)

        metrics = amazon_linux.parse_fio_output(output)

        self.assertEqual(metrics['workload_count'], 4)
        self.assertEqual(metrics['errors'], 0)
        self.assertEqual(metrics['randwrite_iops'], 256.5)
        with self.assertRaisesRegex(ValueError, 'exactly 4'):
            amazon_linux.parse_fio_output('\n'.join(documents[:3]))
        with self.assertRaisesRegex(ValueError, 'reported error 7'):
            failed = output.replace('"error": 0', '"error": 7', 1)
            amazon_linux.parse_fio_output(failed)
        with self.assertRaisesRegex(ValueError, 'must be positive'):
            invalid = output.replace('"iops": 256.5', '"iops": 0', 1)
            amazon_linux.parse_fio_output(invalid)


class AmazonLinuxPackageTests(unittest.TestCase):
    def test_benchmark_packages_preserve_the_stock_curl_minimal(self):
        selections = (
            ('sysbench', {'sysbench'}, {'cpu', 'memory'}, ()),
            ('stream', {'stream'}, (), ()),
            ('fio', {'fio'}, (), ()),
            (
                'phoronix',
                {'phoronix'},
                (),
                amazon_linux.SUPPORTED_PHORONIX_PROFILES,
            ),
        )

        for name, benchmarks, workloads, profiles in selections:
            with self.subTest(benchmark=name):
                packages = amazon_linux.benchmark_packages(
                    benchmarks,
                    sysbench_workloads=workloads,
                    phoronix_profiles=profiles,
                )

                self.assertTrue(packages)
                self.assertNotIn('curl', packages)

        llama_packages = amazon_linux.benchmark_packages(
            set(),
            llm_benchmarks={'llama_bench'},
        )
        self.assertTrue({'git', 'cmake', 'gcc-c++', 'make'}.issubset(
            llama_packages
        ))
        self.assertNotIn('curl', llama_packages)

    def test_phoronix_packages_translate_rhel_only_names_to_al2023(self):
        packages = amazon_linux.amazon_linux_phoronix_packages([
            'compress_7zip',
            'openssl',
            'build_linux_kernel',
            'tinymembench',
        ])

        self.assertTrue({
            'php8.2-cli',
            'php8.2-common',
            'php8.2-process',
            'php8.2-xml',
            'perl',
            'bc',
            'bison',
            'cpio',
            'elfutils-libelf-devel',
            'flex',
            'gawk',
            'openssl-devel',
        }.issubset(packages))
        self.assertFalse({
            'php-cli',
            'php-common',
            'php-process',
            'php-xml',
            'perl-core',
            'perl-interpreter',
            'perl-IPC-Cmd',
        } & packages)

    def test_perl_core_maps_to_the_complete_al2023_core_module_set(self):
        # OpenSSL 3.6 Configure imports FindBin before it can generate its
        # Makefile.  AL2023's ``perl`` meta-package is the supported
        # replacement for RHEL's ``perl-core`` and pulls in FindBin plus the
        # other split core-module RPMs.  perl-interpreter alone does not.
        self.assertEqual(
            amazon_linux.PHORONIX_PACKAGE_REPLACEMENTS['perl-core'],
            frozenset({'perl'}),
        )

        for profile in ('openssl', 'build_linux_kernel'):
            with self.subTest(profile=profile):
                packages = amazon_linux.amazon_linux_phoronix_packages([
                    profile,
                ])
                self.assertIn('perl', packages)
                self.assertNotIn('perl-core', packages)
                self.assertNotIn('perl-interpreter', packages)
                self.assertNotIn('perl-IPC-Cmd', packages)

    def test_installation_orders_packages_before_pinned_sysbench_build(self):
        steps = amazon_linux.installation_steps(
            {'sysbench', 'stream', 'phoronix'},
            sysbench_workloads={'cpu', 'memory'},
            phoronix_profiles={'openssl'},
        )

        self.assertEqual(len(steps), 2)
        self.assertIn('prerequisites', steps[0].name)
        self.assertIn('dnf -y', steps[0].command)
        self.assertEqual(steps[1].name, 'sysbench 1.0.20')
        self.assertIn('./configure --without-mysql', steps[1].command)
        for step in steps:
            self.assertNotIn('ol9_', step.command)
            self.assertNotIn('docker', step.command.lower())


class AmazonLinuxValidationTests(unittest.TestCase):
    def test_architecture_and_official_ami_alias_normalization(self):
        self.assertEqual(
            amazon_linux.normalize_architecture('amd64'), 'x86_64'
        )
        self.assertEqual(
            amazon_linux.normalize_architecture('arm64'), 'aarch64'
        )
        self.assertEqual(
            amazon_linux.ami_parameter_name('aarch64'),
            '/aws/service/ami-amazon-linux-latest/'
            'al2023-ami-kernel-default-arm64',
        )
        with self.assertRaisesRegex(ValueError, 'unsupported'):
            amazon_linux.normalize_architecture('i386')

    def test_accepts_the_supported_aws_benchmark_selection(self):
        amazon_linux.validate_benchmark_selection(
            {
                'apachebench',
                'deathstarbench',
                'sysbench',
                'stream',
                'fio',
                'iperf3',
                'phoronix',
            },
            sysbench_workloads={'cpu', 'memory', 'fileio'},
            iperf3_protocols={'tcp', 'udp', 'sctp'},
            phoronix_profiles={
                'compress_7zip',
                'openssl',
                'build_linux_kernel',
                'tinymembench',
            },
            llm_benchmarks={'llama_bench'},
        )

        with self.assertRaisesRegex(ValueError, 'not supported on AWS'):
            amazon_linux.validate_benchmark_selection({'unknown'})
        with self.assertRaisesRegex(ValueError, 'protocol is not supported'):
            amazon_linux.validate_benchmark_selection(
                {'iperf3'}, iperf3_protocols={'dccp'}
            )
        with self.assertRaisesRegex(ValueError, 'LLM benchmark'):
            amazon_linux.validate_benchmark_selection(
                {'stream'}, llm_benchmarks={'unknown_llm'}
            )


if __name__ == '__main__':
    unittest.main()
