"""Pinned CPU-only llama.cpp benchmark command and strict result parser.

The command returned here is guest-independent.  Distribution adapters only
need to install :data:`REQUIRED_PACKAGES`; the benchmark itself verifies both
the llama.cpp Git revision and the GGUF model checksum before it runs.
"""

from __future__ import annotations

import json
import math
import os
import re
import shlex
from collections.abc import Mapping, Sequence


LLAMA_CPP_RELEASE = 'b10218'
LLAMA_CPP_REVISION = 'de699957b92f490efebad149665b0dccf127eaff'
LLAMA_CPP_REPOSITORY = 'https://github.com/ggml-org/llama.cpp.git'

MODEL_REPOSITORY = 'TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF'
MODEL_REVISION = 'c1d7cb837a660d93ba28f936efb148591bfba3e9'
MODEL_FILENAME = 'tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf'
MODEL_SHA256 = (
    '9fecc3b3cd76bba89d504f29b616eedf7da85b96540e490ca5824d3f7d2776a0'
)
# The GGUF file includes container metadata that llama-bench excludes from its
# reported ``model_size`` payload.  Keep the two values distinct so the pinned
# download identity is not incorrectly used to validate llama-bench output.
MODEL_FILE_SIZE_BYTES = 668_788_096
MODEL_PAYLOAD_SIZE_BYTES = 667_078_656
MODEL_URL = (
    f'https://huggingface.co/{MODEL_REPOSITORY}/resolve/'
    f'{MODEL_REVISION}/{MODEL_FILENAME}'
)

REPETITIONS = 5
PROMPT_TOKEN_COUNTS = (512, 2048)
GENERATION_TOKEN_COUNTS = (128, 512)
READINESS_HOSTS = ('github.com', 'huggingface.co')
REQUIRED_PACKAGES = frozenset({
    'ca-certificates',
    'cmake',
    'curl',
    'gcc',
    'gcc-c++',
    'git',
    'make',
})

_SOURCE_DIR = f'/tmp/oci-benchmark-llama.cpp-{LLAMA_CPP_RELEASE}'
_BUILD_DIR = f'{_SOURCE_DIR}/build'
_MODEL_PATH = f'/tmp/{MODEL_FILENAME}'
_COMMIT_RE = re.compile(r'^[0-9a-f]{7,40}$')
_GPU_BACKEND_RE = re.compile(
    r'\b(?:CUDA|HIP|ROCM|VULKAN|SYCL|METAL|OPENCL|MUSA|CANN)\b',
    re.IGNORECASE,
)


def _toolset_source_command(path: str | None) -> str:
    if path is None:
        return ''
    normalized = str(path).strip()
    if (
        not normalized.startswith('/')
        or any(character in normalized for character in ('\x00', '\n', '\r'))
    ):
        raise ValueError('The llama.cpp compiler toolset path must be absolute.')
    return f'source {shlex.quote(normalized)}; '


def logical_cpu_count_command() -> str:
    """Return the read-only guest probe used to size a llama.cpp run."""

    return 'nproc'


def parse_logical_cpu_count(output: str) -> int:
    """Parse one positive logical-CPU count without accepting extra output."""

    normalized = output.strip() if isinstance(output, str) else ''
    if not re.fullmatch(r'[1-9][0-9]*', normalized):
        raise ValueError(
            'The llama.cpp logical-CPU probe must return exactly one positive '
            'integer.'
        )
    return int(normalized)


def architecture_command() -> str:
    """Return the read-only guest architecture probe used for result metadata."""

    return 'uname -m'


def parse_architecture(output: str) -> str:
    """Return one supported, normalized llama.cpp CPU architecture."""

    normalized = output.strip() if isinstance(output, str) else ''
    if normalized == 'arm64':
        normalized = 'aarch64'
    if normalized not in {'x86_64', 'aarch64'}:
        raise ValueError(
            'The llama.cpp architecture probe must return x86_64 or aarch64.'
        )
    return normalized


def toolchain_probe_command(*, toolset_enable: str | None = None) -> str:
    """Return a stdout-only probe for the exact native-build toolchain."""

    toolset = _toolset_source_command(toolset_enable)
    return (
        'set -euo pipefail; '
        f'{toolset}'
        'export LC_ALL=C; '
        'COMPILER_PATH=$(command -v gcc); '
        'COMPILER_VERSION=$(gcc -dumpfullversion -dumpversion); '
        'CXX_COMPILER_PATH=$(command -v g++); '
        'CXX_COMPILER_VERSION=$(g++ -dumpfullversion -dumpversion); '
        'ASSEMBLER_PROGRAM=$(g++ -print-prog-name=as); '
        'case "$ASSEMBLER_PROGRAM" in '
        '/*) ASSEMBLER_PATH=$(readlink -f "$ASSEMBLER_PROGRAM") ;; '
        '*) ASSEMBLER_PATH=$(command -v "$ASSEMBLER_PROGRAM") ;; '
        'esac; '
        'test -x "$ASSEMBLER_PATH"; '
        'ASSEMBLER_VERSION=$("$ASSEMBLER_PATH" --version | sed -n \'1p\'); '
        'printf "LLAMA_COMPILER_PATH=%s\\n" "$COMPILER_PATH"; '
        'printf "LLAMA_COMPILER_VERSION=%s\\n" "$COMPILER_VERSION"; '
        'printf "LLAMA_CXX_COMPILER_PATH=%s\\n" "$CXX_COMPILER_PATH"; '
        'printf "LLAMA_CXX_COMPILER_VERSION=%s\\n" '
        '"$CXX_COMPILER_VERSION"; '
        'printf "LLAMA_ASSEMBLER_PATH=%s\\n" "$ASSEMBLER_PATH"; '
        'printf "LLAMA_ASSEMBLER_VERSION=%s\\n" "$ASSEMBLER_VERSION"'
    )


def parse_toolchain_probe(output: str) -> dict[str, str | bool]:
    """Validate and normalize the exact GCC/binutils provenance probe."""

    expected_keys = (
        'LLAMA_COMPILER_PATH',
        'LLAMA_COMPILER_VERSION',
        'LLAMA_CXX_COMPILER_PATH',
        'LLAMA_CXX_COMPILER_VERSION',
        'LLAMA_ASSEMBLER_PATH',
        'LLAMA_ASSEMBLER_VERSION',
    )
    lines = output.splitlines() if isinstance(output, str) else []
    if len(lines) != len(expected_keys):
        raise ValueError(
            'The llama.cpp toolchain probe must return exactly six fields.'
        )
    values: dict[str, str] = {}
    for expected_key, line in zip(expected_keys, lines, strict=True):
        key, separator, value = line.partition('=')
        if key != expected_key or separator != '=' or not value.strip():
            raise ValueError(
                'The llama.cpp toolchain probe returned malformed fields.'
            )
        values[key] = value.strip()

    compiler_path = values['LLAMA_COMPILER_PATH']
    cxx_compiler_path = values['LLAMA_CXX_COMPILER_PATH']
    assembler_path = values['LLAMA_ASSEMBLER_PATH']
    compiler_version = values['LLAMA_COMPILER_VERSION']
    cxx_compiler_version = values['LLAMA_CXX_COMPILER_VERSION']
    assembler_banner = values['LLAMA_ASSEMBLER_VERSION']
    if (
        not os.path.isabs(compiler_path)
        or os.path.basename(compiler_path) != 'gcc'
    ):
        raise ValueError(
            'The llama.cpp toolchain probe did not resolve an absolute GCC path.'
        )
    if (
        not os.path.isabs(cxx_compiler_path)
        or os.path.basename(cxx_compiler_path) != 'g++'
    ):
        raise ValueError(
            'The llama.cpp toolchain probe did not resolve an absolute G++ path.'
        )
    if (
        not os.path.isabs(assembler_path)
        or os.path.basename(assembler_path) != 'as'
    ):
        raise ValueError(
            'The llama.cpp toolchain probe did not resolve an absolute '
            'assembler path.'
        )
    if not re.fullmatch(r'[0-9][0-9A-Za-z.+:~_-]*', compiler_version):
        raise ValueError(
            'The llama.cpp toolchain probe returned an invalid GCC version.'
        )
    if (
        not re.fullmatch(
            r'[0-9][0-9A-Za-z.+:~_-]*',
            cxx_compiler_version,
        )
        or cxx_compiler_version != compiler_version
    ):
        raise ValueError(
            'The llama.cpp toolchain probe did not resolve a coherent G++ '
            'version.'
        )
    assembler_version_match = re.search(
        r'(?<![0-9A-Za-z])'
        r'([0-9]+(?:\.[0-9]+)+)',
        assembler_banner,
    )
    if (
        not assembler_banner.startswith('GNU assembler')
        or assembler_version_match is None
    ):
        raise ValueError(
            'The llama.cpp toolchain probe did not report GNU assembler.'
        )
    return {
        'llama_compiler': 'GCC',
        'llama_compiler_path': compiler_path,
        'llama_compiler_version': compiler_version,
        'llama_cxx_compiler': 'G++',
        'llama_cxx_compiler_path': cxx_compiler_path,
        'llama_cxx_compiler_version': cxx_compiler_version,
        'llama_assembler': 'GNU as',
        'llama_assembler_path': assembler_path,
        'llama_assembler_version': assembler_version_match.group(1),
        'llama_assembler_banner': assembler_banner,
        'llama_native_optimization': True,
    }


def benchmark_command(
    *,
    toolset_enable: str | None = None,
    threads: int | None = None,
) -> str:
    """Return a reproducible CPU-only ``llama-bench`` shell command.

    Setup diagnostics are redirected to stderr so stdout contains only the
    final JSON array consumed by :func:`parse_output`.
    """

    toolset = _toolset_source_command(toolset_enable)
    if threads is None:
        thread_assignment = 'THREADS=$(nproc); '
    else:
        if (
            isinstance(threads, bool)
            or not isinstance(threads, int)
            or threads <= 0
        ):
            raise ValueError('llama.cpp threads must be a positive integer.')
        thread_assignment = f'THREADS={threads}; '
    return (
        'set -euo pipefail; '
        '{ '
        f'{toolset}'
        'CC_PATH=$(command -v gcc); '
        'CXX_PATH=$(command -v g++); '
        'test -x "$CC_PATH"; test -x "$CXX_PATH"; '
        'export CC="$CC_PATH" CXX="$CXX_PATH"; '
        'ARCH=$(uname -m); '
        'case "$ARCH" in x86_64|aarch64|arm64) ;; '
        '*) echo "Unsupported llama.cpp CPU architecture: $ARCH" >&2; '
        'exit 1 ;; esac; '
        f'{thread_assignment}'
        'case "$THREADS" in ""|*[!0-9]*|0) '
        'echo "Unable to determine a positive llama.cpp thread count." >&2; '
        'exit 1 ;; esac; '
        f'SOURCE_DIR={shlex.quote(_SOURCE_DIR)}; '
        f'BUILD_DIR={shlex.quote(_BUILD_DIR)}; '
        f'EXPECTED_REVISION={shlex.quote(LLAMA_CPP_REVISION)}; '
        'CLONE_OK=false; '
        'for attempt in $(seq 1 3); do '
        'rm -rf "$SOURCE_DIR"; '
        f'if timeout 600 git clone --depth 1 --single-branch '
        f'--branch {shlex.quote(LLAMA_CPP_RELEASE)} '
        f'{shlex.quote(LLAMA_CPP_REPOSITORY)} "$SOURCE_DIR" '
        '&& [ "$(git -C "$SOURCE_DIR" rev-parse HEAD)" '
        '= "$EXPECTED_REVISION" ]; then '
        'CLONE_OK=true; break; fi; '
        'echo "Pinned llama.cpp clone or revision verification failed '
        '($attempt/3)." >&2; '
        'if [ "$attempt" -lt 3 ]; then sleep 10; fi; '
        'done; '
        'if [ "$CLONE_OK" != true ]; then '
        'echo "Unable to obtain the pinned llama.cpp revision." >&2; '
        'exit 1; fi; '
        'ACTUAL_REVISION=$(git -C "$SOURCE_DIR" rev-parse HEAD); '
        '[ "$ACTUAL_REVISION" = "$EXPECTED_REVISION" ]; '
        'cmake -S "$SOURCE_DIR" -B "$BUILD_DIR" '
        '-DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF '
        '-DCMAKE_C_COMPILER="$CC_PATH" '
        '-DCMAKE_CXX_COMPILER="$CXX_PATH" '
        '-DGGML_NATIVE=ON -DGGML_BLAS=OFF '
        '-DGGML_CUDA=OFF -DGGML_HIP=OFF -DGGML_VULKAN=OFF '
        '-DGGML_SYCL=OFF -DGGML_METAL=OFF -DGGML_OPENCL=OFF; '
        'cmake --build "$BUILD_DIR" --config Release '
        '--parallel "$THREADS" --target llama-bench; '
        'test -x "$BUILD_DIR/bin/llama-bench"; '
        f'MODEL={shlex.quote(_MODEL_PATH)}; '
        f'EXPECTED_MODEL_SHA256={shlex.quote(MODEL_SHA256)}; '
        'MODEL_OK=false; '
        'if [ -f "$MODEL" ] '
        '&& printf "%s  %s\\n" "$EXPECTED_MODEL_SHA256" "$MODEL" '
        '| sha256sum -c -; then MODEL_OK=true; fi; '
        'if [ "$MODEL_OK" != true ]; then '
        'for attempt in $(seq 1 3); do '
        'rm -f "$MODEL.part"; '
        f'if curl -L --fail --retry 3 --retry-all-errors '
        f'--connect-timeout 15 --max-time 1200 '
        f'{shlex.quote(MODEL_URL)} -o "$MODEL.part" '
        '&& printf "%s  %s\\n" "$EXPECTED_MODEL_SHA256" "$MODEL.part" '
        '| sha256sum -c -; then '
        'mv "$MODEL.part" "$MODEL"; MODEL_OK=true; break; fi; '
        'echo "Pinned llama.cpp model download or checksum verification '
        'failed ($attempt/3)." >&2; '
        'if [ "$attempt" -lt 3 ]; then sleep 10; fi; '
        'done; fi; '
        'if [ "$MODEL_OK" != true ]; then '
        'echo "Unable to obtain the checksum-pinned llama.cpp model." >&2; '
        'exit 1; fi; '
        'printf "%s  %s\\n" "$EXPECTED_MODEL_SHA256" "$MODEL" '
        '| sha256sum -c -; '
        'echo "llama.cpp revision=$ACTUAL_REVISION architecture=$ARCH '
        'threads=$THREADS backend=CPU" >&2; '
        '} >&2; '
        f'exec {shlex.quote(_BUILD_DIR + "/bin/llama-bench")} '
        f'--model {shlex.quote(_MODEL_PATH)} '
        '--n-prompt 512,2048 --n-gen 128,512 --repetitions 5 '
        '--threads "$THREADS" --n-gpu-layers 0 --device none --output json'
    )


def benchmark_metadata() -> dict[str, str | int]:
    """Return immutable artifact and execution metadata for result reports."""

    return {
        'llama_cpp_release': LLAMA_CPP_RELEASE,
        'llama_cpp_revision': LLAMA_CPP_REVISION,
        'model_filename': MODEL_FILENAME,
        'model_revision': MODEL_REVISION,
        'model_sha256': MODEL_SHA256,
        'model_file_size_bytes': MODEL_FILE_SIZE_BYTES,
        'model_payload_size_bytes': MODEL_PAYLOAD_SIZE_BYTES,
        'model_quantization': 'Q4_K_M',
        'execution_backend': 'CPU',
    }


def _mapping(value, label: str) -> Mapping:
    if not isinstance(value, Mapping):
        raise ValueError(f'llama-bench result {label} must be a JSON object.')
    return value


def _number(
    row: Mapping,
    key: str,
    label: str,
    *,
    allow_zero: bool = False,
) -> float:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f'llama-bench {label} is missing or is not numeric.')
    result = float(value)
    if (
        not math.isfinite(result)
        or result < 0
        or (result == 0 and not allow_zero)
    ):
        qualifier = 'non-negative' if allow_zero else 'positive'
        raise ValueError(f'llama-bench {label} must be finite and {qualifier}.')
    return result


def _integer(
    row: Mapping,
    key: str,
    label: str,
    *,
    allow_zero: bool = False,
) -> int:
    value = _number(row, key, label, allow_zero=allow_zero)
    if not value.is_integer():
        raise ValueError(f'llama-bench {label} must be an integer.')
    return int(value)


def _samples(row: Mapping, key: str, label: str) -> tuple[float, ...]:
    values = row.get(key)
    if (
        not isinstance(values, Sequence)
        or isinstance(values, (str, bytes, bytearray))
        or len(values) != REPETITIONS
    ):
        raise ValueError(
            f'llama-bench {label} must contain exactly {REPETITIONS} samples.'
        )
    result = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f'llama-bench {label} contains a non-numeric value.')
        number = float(value)
        if not math.isfinite(number) or number <= 0:
            raise ValueError(
                f'llama-bench {label} values must be positive and finite.'
            )
        result.append(number)
    return tuple(result)


def parse_output(
    output: str,
    *,
    expected_threads: int | None = None,
) -> dict[str, float | int | str]:
    """Validate the four pinned CPU tests and return report-ready metrics."""

    try:
        document = json.loads(output)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError('llama-bench did not return valid JSON.') from exc
    if not isinstance(document, list) or len(document) != 4:
        raise ValueError('llama-bench must return exactly four JSON results.')

    if expected_threads is not None:
        if (
            isinstance(expected_threads, bool)
            or not isinstance(expected_threads, int)
            or expected_threads <= 0
        ):
            raise ValueError('Expected llama-bench threads must be positive.')

    expected_tests = {
        (512, 0): 'prompt_512',
        (2048, 0): 'prompt_2048',
        (0, 128): 'generation_128',
        (0, 512): 'generation_512',
    }
    observed: dict[tuple[int, int], Mapping] = {}
    consistent: dict[str, object] = {}
    for index, raw_row in enumerate(document, start=1):
        row = _mapping(raw_row, f'row {index}')
        prompt = _integer(
            row, 'n_prompt', f'row {index} prompt-token count', allow_zero=True
        )
        generation = _integer(
            row, 'n_gen', f'row {index} generation-token count', allow_zero=True
        )
        test = (prompt, generation)
        if test not in expected_tests:
            raise ValueError(
                'llama-bench returned an unexpected prompt/generation test: '
                f'{prompt}/{generation}.'
            )
        if test in observed:
            raise ValueError('llama-bench returned a duplicate benchmark test.')

        commit = str(row.get('build_commit') or '').strip().lower()
        if not _COMMIT_RE.fullmatch(commit) or not LLAMA_CPP_REVISION.startswith(
            commit
        ):
            raise ValueError('llama-bench did not report the pinned build commit.')
        backend = str(row.get('backends') or '').strip()
        if not re.search(r'\bCPU\b', backend, re.IGNORECASE) or _GPU_BACKEND_RE.search(
            backend
        ):
            raise ValueError('llama-bench did not report a CPU-only backend.')
        if _integer(
            row,
            'n_gpu_layers',
            f'row {index} GPU-layer count',
            allow_zero=True,
        ) != 0:
            raise ValueError('llama-bench offloaded one or more layers to a GPU.')

        model_filename = os.path.basename(str(row.get('model_filename') or ''))
        if model_filename != MODEL_FILENAME:
            raise ValueError('llama-bench reported an unexpected model artifact.')
        model_size = _integer(row, 'model_size', f'row {index} model size')
        if model_size != MODEL_PAYLOAD_SIZE_BYTES:
            raise ValueError('llama-bench reported an unexpected model size.')
        model_parameters = _integer(
            row, 'model_n_params', f'row {index} model parameter count'
        )
        threads = _integer(row, 'n_threads', f'row {index} thread count')
        if expected_threads is not None and threads != expected_threads:
            raise ValueError(
                f'llama-bench used {threads} threads; expected {expected_threads}.'
            )
        _number(row, 'avg_ns', f'row {index} average duration')
        _number(
            row,
            'stddev_ns',
            f'row {index} duration standard deviation',
            allow_zero=True,
        )
        _number(row, 'avg_ts', f'row {index} average throughput')
        _number(
            row,
            'stddev_ts',
            f'row {index} throughput standard deviation',
            allow_zero=True,
        )
        _samples(row, 'samples_ns', f'row {index} duration samples')
        _samples(row, 'samples_ts', f'row {index} throughput samples')

        row_consistent = {
            'commit': commit,
            'backend': backend,
            'model_filename': model_filename,
            'model_size': model_size,
            'model_parameters': model_parameters,
            'threads': threads,
        }
        if not consistent:
            consistent = row_consistent
        elif consistent != row_consistent:
            raise ValueError(
                'llama-bench artifact, backend, build, or thread metadata '
                'changed between tests.'
            )
        observed[test] = row

    if set(observed) != set(expected_tests):
        raise ValueError('llama-bench did not return every required CPU test.')

    metrics: dict[str, float | int | str] = {
        'model': MODEL_FILENAME,
        'model_file_size_bytes': MODEL_FILE_SIZE_BYTES,
        'model_payload_size_bytes': MODEL_PAYLOAD_SIZE_BYTES,
        'model_parameters': int(consistent['model_parameters']),
        'llama_cpp_build_commit': str(consistent['commit']),
        'threads': int(consistent['threads']),
        'backend': str(consistent['backend']),
        'repetitions': REPETITIONS,
        'unit': 'tokens/second',
    }
    for test, prefix in expected_tests.items():
        row = observed[test]
        metrics[f'{prefix}_tokens_per_second'] = round(
            float(row['avg_ts']), 6
        )
        metrics[f'{prefix}_stddev_tokens_per_second'] = round(
            float(row['stddev_ts']), 6
        )
    return metrics


__all__ = [
    'GENERATION_TOKEN_COUNTS',
    'LLAMA_CPP_RELEASE',
    'LLAMA_CPP_REPOSITORY',
    'LLAMA_CPP_REVISION',
    'MODEL_FILENAME',
    'MODEL_FILE_SIZE_BYTES',
    'MODEL_PAYLOAD_SIZE_BYTES',
    'MODEL_REPOSITORY',
    'MODEL_REVISION',
    'MODEL_SHA256',
    'MODEL_URL',
    'PROMPT_TOKEN_COUNTS',
    'READINESS_HOSTS',
    'REPETITIONS',
    'REQUIRED_PACKAGES',
    'benchmark_command',
    'benchmark_metadata',
    'architecture_command',
    'logical_cpu_count_command',
    'parse_architecture',
    'parse_logical_cpu_count',
    'parse_output',
    'parse_toolchain_probe',
    'toolchain_probe_command',
]
