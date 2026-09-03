import copy
import json
import subprocess
import unittest

from app import llama_cpp


def llama_results():
    rows = []
    for index, (prompt, generation) in enumerate(
        ((512, 0), (2048, 0), (0, 128), (0, 512)),
        start=1,
    ):
        average = 100.0 + index
        rows.append({
            'build_commit': llama_cpp.LLAMA_CPP_REVISION[:8],
            'backends': 'CPU',
            'model_filename': f'/tmp/{llama_cpp.MODEL_FILENAME}',
            # llama-bench reports the GGUF tensor payload, not the downloaded
            # file's full byte count.
            'model_size': 667_078_656,
            'model_n_params': 1_100_048_384,
            'n_threads': 8,
            'n_gpu_layers': 0,
            'n_prompt': prompt,
            'n_gen': generation,
            'avg_ns': 1_000_000 * index,
            'stddev_ns': 1000,
            'avg_ts': average,
            'stddev_ts': 0.25,
            'samples_ns': [1_000_000 * index + value for value in range(5)],
            'samples_ts': [average + value / 10 for value in range(5)],
        })
    return rows


class LlamaCppCommandTests(unittest.TestCase):
    def test_architecture_probe_normalizes_only_supported_cpu_architectures(self):
        self.assertEqual(llama_cpp.architecture_command(), 'uname -m')
        self.assertEqual(llama_cpp.parse_architecture('x86_64\n'), 'x86_64')
        self.assertEqual(llama_cpp.parse_architecture('arm64\n'), 'aarch64')
        with self.assertRaisesRegex(ValueError, 'x86_64 or aarch64'):
            llama_cpp.parse_architecture('ppc64le\n')

    def test_logical_cpu_probe_is_read_only_and_strict(self):
        self.assertEqual(llama_cpp.logical_cpu_count_command(), 'nproc')
        self.assertEqual(llama_cpp.parse_logical_cpu_count('8\n'), 8)
        for output in ('', '0', '-1', '4 CPUs', '4\n5\n'):
            with self.subTest(output=output):
                with self.assertRaisesRegex(ValueError, 'positive integer'):
                    llama_cpp.parse_logical_cpu_count(output)

    def test_command_pins_and_verifies_source_and_model_before_cpu_run(self):
        command = llama_cpp.benchmark_command(architecture='x86_64')

        self.assertIn(f'--branch {llama_cpp.LLAMA_CPP_RELEASE}', command)
        self.assertIn(llama_cpp.LLAMA_CPP_REVISION, command)
        self.assertIn(llama_cpp.MODEL_REVISION, command)
        self.assertIn(llama_cpp.MODEL_SHA256, command)
        self.assertNotIn('/resolve/main/', command)
        self.assertIn(
            f'--model /tmp/{llama_cpp.MODEL_FILENAME}',
            command,
        )
        self.assertNotIn(
            f'--model /tmp/oci-benchmark-{llama_cpp.MODEL_FILENAME}',
            command,
        )
        self.assertIn('sha256sum -c -', command)
        self.assertIn('CC_PATH=$(command -v gcc)', command)
        self.assertIn('CXX_PATH=$(command -v g++)', command)
        self.assertIn('-DCMAKE_C_COMPILER="$CC_PATH"', command)
        self.assertIn('-DCMAKE_CXX_COMPILER="$CXX_PATH"', command)
        self.assertIn('-DGGML_NATIVE=OFF', command)
        self.assertIn('-DGGML_AVX2=ON', command)
        self.assertIn('-DGGML_AVX512=OFF', command)
        self.assertIn('-DGGML_AMX_TILE=OFF', command)
        self.assertIn('-DGGML_BACKEND_DL=OFF', command)
        self.assertIn('-DGGML_CPU_ALL_VARIANTS=OFF', command)
        self.assertNotIn('-march=native', command)
        self.assertIn(
            f'CPU_BUILD_PROFILE={llama_cpp.X86_64_PORTABLE_CPU_PROFILE}',
            command,
        )
        self.assertIn('-DGGML_CUDA=OFF', command)
        self.assertIn('--n-prompt 512,2048', command)
        self.assertIn('--n-gen 128,512', command)
        self.assertIn('--repetitions 5', command)
        self.assertIn('--threads "$THREADS"', command)
        self.assertIn('--n-gpu-layers 0', command)
        self.assertIn('--device none', command)
        self.assertIn('--output json', command)
        self.assertNotIn('} >&2; exec ', command)
        self.assertIn('LLAMA_BENCH_STATUS=$?', command)
        self.assertIn(
            'llama-bench exited with status $LLAMA_BENCH_STATUS.',
            command,
        )
        self.assertIn('exit "$LLAMA_BENCH_STATUS"', command)
        subprocess.run(
            ['bash', '-n'],
            input=command,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_observed_thread_count_is_reused_by_the_benchmark_command(self):
        command = llama_cpp.benchmark_command(
            architecture='x86_64',
            threads=4,
        )

        self.assertIn('THREADS=4;', command)
        self.assertNotIn('THREADS=$(nproc)', command)
        self.assertIn('--threads "$THREADS"', command)
        with self.assertRaisesRegex(ValueError, 'positive integer'):
            llama_cpp.benchmark_command(architecture='x86_64', threads=0)
        with self.assertRaisesRegex(ValueError, 'positive integer'):
            llama_cpp.benchmark_command(architecture='x86_64', threads=True)

    def test_arm_command_retains_native_optimization(self):
        command = llama_cpp.benchmark_command(architecture='aarch64')

        self.assertIn('-DGGML_NATIVE=ON', command)
        self.assertNotIn('-DGGML_NATIVE=OFF', command)
        self.assertNotIn('-DGGML_AVX2=ON', command)
        self.assertIn(
            f'CPU_BUILD_PROFILE={llama_cpp.AARCH64_NATIVE_CPU_PROFILE}',
            command,
        )
        with self.assertRaisesRegex(ValueError, 'x86_64 or aarch64'):
            llama_cpp.benchmark_command(architecture='ppc64le')

    def test_optional_toolset_is_safely_sourced(self):
        command = llama_cpp.benchmark_command(
            architecture='x86_64',
            toolset_enable='/opt/rh/gcc toolset/enable'
        )

        self.assertIn("source '/opt/rh/gcc toolset/enable'", command)
        with self.assertRaisesRegex(ValueError, 'absolute'):
            llama_cpp.benchmark_command(
                architecture='x86_64',
                toolset_enable='relative/enable',
            )
        with self.assertRaisesRegex(ValueError, 'absolute'):
            llama_cpp.benchmark_command(
                architecture='x86_64',
                toolset_enable='/tmp/enable\nid',
            )

    def test_toolchain_probe_uses_the_selected_toolset_and_is_strict(self):
        command = llama_cpp.toolchain_probe_command(
            toolset_enable='/opt/rh/gcc-toolset-15/enable'
        )

        self.assertIn(
            'source /opt/rh/gcc-toolset-15/enable',
            command,
        )
        self.assertIn('gcc -dumpfullversion -dumpversion', command)
        self.assertIn('g++ -dumpfullversion -dumpversion', command)
        self.assertIn('g++ -print-prog-name=as', command)
        self.assertIn('"$ASSEMBLER_PATH" --version', command)
        subprocess.run(
            ['bash', '-n'],
            input=command,
            check=True,
            capture_output=True,
            text=True,
        )
        with self.assertRaisesRegex(ValueError, 'absolute'):
            llama_cpp.toolchain_probe_command(toolset_enable='relative/enable')

    def test_parses_exact_gcc_and_binutils_provenance(self):
        output = (
            'LLAMA_COMPILER_PATH=/opt/rh/gcc-toolset-15/root/usr/bin/gcc\n'
            'LLAMA_COMPILER_VERSION=15.2.1\n'
            'LLAMA_CXX_COMPILER_PATH=/opt/rh/gcc-toolset-15/root/usr/bin/g++\n'
            'LLAMA_CXX_COMPILER_VERSION=15.2.1\n'
            'LLAMA_ASSEMBLER_PATH=/opt/rh/gcc-toolset-15/root/usr/bin/as\n'
            'LLAMA_ASSEMBLER_VERSION=GNU assembler version 2.44-5.el9_8\n'
        )

        metadata = llama_cpp.parse_toolchain_probe(output)

        self.assertEqual(metadata['llama_compiler'], 'GCC')
        self.assertEqual(metadata['llama_compiler_version'], '15.2.1')
        self.assertEqual(metadata['llama_cxx_compiler'], 'G++')
        self.assertEqual(metadata['llama_cxx_compiler_version'], '15.2.1')
        self.assertEqual(
            metadata['llama_assembler_version'],
            '2.44',
        )
        self.assertEqual(
            metadata['llama_assembler_banner'],
            'GNU assembler version 2.44-5.el9_8',
        )
        self.assertNotIn('llama_native_optimization', metadata)

        malformed = (
            output.replace('/usr/bin/gcc', '/usr/bin/clang'),
            output.replace(
                'LLAMA_CXX_COMPILER_VERSION=15.2.1',
                'LLAMA_CXX_COMPILER_VERSION=14.3.1',
            ),
            output.replace(
                'GNU assembler version 2.44-5.el9_8',
                'LLVM assembler 19',
            ),
            output + 'EXTRA=value\n',
        )
        for value in malformed:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    llama_cpp.parse_toolchain_probe(value)

    def test_metadata_records_every_immutable_artifact_identity(self):
        metadata = llama_cpp.benchmark_metadata()

        self.assertEqual(metadata['llama_cpp_revision'], llama_cpp.LLAMA_CPP_REVISION)
        self.assertEqual(metadata['model_revision'], llama_cpp.MODEL_REVISION)
        self.assertEqual(metadata['model_sha256'], llama_cpp.MODEL_SHA256)
        self.assertEqual(
            metadata['model_file_size_bytes'],
            668_788_096,
        )
        self.assertEqual(
            metadata['model_payload_size_bytes'],
            667_078_656,
        )
        self.assertEqual(metadata['execution_backend'], 'CPU')

    def test_cpu_build_profile_is_architecture_specific_and_reportable(self):
        x86 = llama_cpp.cpu_build_profile('x86_64')
        arm = llama_cpp.cpu_build_profile('arm64')

        self.assertFalse(x86['llama_native_optimization'])
        self.assertEqual(
            x86['llama_cpu_build_profile'],
            llama_cpp.X86_64_PORTABLE_CPU_PROFILE,
        )
        self.assertIn('-DGGML_NATIVE=OFF', x86['llama_cpu_cmake_options'])
        self.assertIn('-DGGML_AVX512=OFF', x86['llama_cpu_cmake_options'])
        self.assertIn(
            '-DGGML_CPU_ALL_VARIANTS=OFF',
            x86['llama_cpu_cmake_options'],
        )
        self.assertTrue(arm['llama_native_optimization'])
        self.assertEqual(
            arm['llama_cpu_build_profile'],
            llama_cpp.AARCH64_NATIVE_CPU_PROFILE,
        )
        self.assertEqual(arm['llama_cpu_cmake_options'], '-DGGML_NATIVE=ON')


class LlamaCppResultTests(unittest.TestCase):
    def test_parses_exact_four_test_cpu_matrix(self):
        metrics = llama_cpp.parse_output(
            json.dumps(llama_results()),
            expected_threads=8,
        )

        self.assertEqual(metrics['backend'], 'CPU')
        self.assertEqual(metrics['model_file_size_bytes'], 668_788_096)
        self.assertEqual(metrics['model_payload_size_bytes'], 667_078_656)
        self.assertEqual(metrics['threads'], 8)
        self.assertEqual(metrics['repetitions'], 5)
        self.assertEqual(metrics['prompt_512_tokens_per_second'], 101.0)
        self.assertEqual(metrics['prompt_2048_tokens_per_second'], 102.0)
        self.assertEqual(metrics['generation_128_tokens_per_second'], 103.0)
        self.assertEqual(metrics['generation_512_tokens_per_second'], 104.0)
        self.assertEqual(metrics['unit'], 'tokens/second')

    def test_distinguishes_real_payload_size_from_downloaded_file_size(self):
        metrics = llama_cpp.parse_output(json.dumps(llama_results()))

        self.assertEqual(llama_cpp.MODEL_FILE_SIZE_BYTES, 668_788_096)
        self.assertEqual(llama_cpp.MODEL_PAYLOAD_SIZE_BYTES, 667_078_656)
        self.assertEqual(metrics['model_file_size_bytes'], 668_788_096)
        self.assertEqual(metrics['model_payload_size_bytes'], 667_078_656)

        rows = llama_results()
        rows[0]['model_size'] = 668_788_096
        with self.assertRaisesRegex(ValueError, 'unexpected model size'):
            llama_cpp.parse_output(json.dumps(rows))

    def test_rejects_wrong_count_duplicate_or_unexpected_test(self):
        rows = llama_results()
        with self.assertRaisesRegex(ValueError, 'exactly four'):
            llama_cpp.parse_output(json.dumps(rows[:3]))

        duplicate = copy.deepcopy(rows)
        duplicate[1]['n_prompt'] = 512
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            llama_cpp.parse_output(json.dumps(duplicate))

        unexpected = copy.deepcopy(rows)
        unexpected[0]['n_prompt'] = 256
        with self.assertRaisesRegex(ValueError, 'unexpected'):
            llama_cpp.parse_output(json.dumps(unexpected))

    def test_rejects_unpinned_gpu_or_inconsistent_execution(self):
        cases = (
            ('build_commit', 'deadbee', 'pinned build commit'),
            ('backends', 'CPU,CUDA', 'CPU-only backend'),
            ('n_gpu_layers', 1, 'offloaded'),
            ('model_filename', '/tmp/not-the-pinned-model.gguf', 'model artifact'),
            ('model_size', 1, 'unexpected model size'),
            ('n_threads', 4, 'changed between tests'),
        )
        for key, value, message in cases:
            with self.subTest(key=key):
                rows = llama_results()
                rows[1][key] = value
                with self.assertRaisesRegex(ValueError, message):
                    llama_cpp.parse_output(json.dumps(rows))

        with self.assertRaisesRegex(ValueError, 'expected 16'):
            llama_cpp.parse_output(
                json.dumps(llama_results()),
                expected_threads=16,
            )

    def test_rejects_missing_nonfinite_or_incomplete_samples(self):
        rows = llama_results()
        rows[0]['samples_ts'] = rows[0]['samples_ts'][:4]
        with self.assertRaisesRegex(ValueError, 'exactly 5 samples'):
            llama_cpp.parse_output(json.dumps(rows))

        rows = llama_results()
        rows[0]['avg_ts'] = float('nan')
        with self.assertRaisesRegex(ValueError, 'finite'):
            llama_cpp.parse_output(json.dumps(rows))

        with self.assertRaisesRegex(ValueError, 'valid JSON'):
            llama_cpp.parse_output('not-json')


if __name__ == '__main__':
    unittest.main()
