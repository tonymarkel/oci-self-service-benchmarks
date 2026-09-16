"""Amazon Linux 2023 commands for the supported AWS benchmark workloads.

The helpers in this module are deliberately pure: provider code can assemble
and validate a run without opening an SSH connection, and tests can syntax
check every generated shell command locally.
"""

from dataclasses import dataclass
import json
import math
from pathlib import PurePosixPath
import re
import shlex
from typing import Iterable

from app import llama_cpp, phoronix


SSH_USER = 'ec2-user'

SUPPORTED_ARCHITECTURES = frozenset({'x86_64', 'aarch64'})
SUPPORTED_BENCHMARKS = frozenset({
    'apachebench',
    'deathstarbench',
    'sysbench',
    'stream',
    'fio',
    'iperf3',
    'phoronix',
})
SUPPORTED_SYSBENCH_WORKLOADS = frozenset({'cpu', 'memory', 'fileio'})
SUPPORTED_IPERF3_PROTOCOLS = frozenset({'tcp', 'udp', 'sctp'})
SUPPORTED_PHORONIX_PROFILES = frozenset(phoronix.PROFILES)
SUPPORTED_LLM_BENCHMARKS = frozenset({'llama_bench'})

DEFAULT_READINESS_HOSTS = (
    'cdn.amazonlinux.com',
    'github.com',
    'codeload.github.com',
    'raw.githubusercontent.com',
    'openbenchmarking.org',
    'huggingface.co',
)

_BASE_READINESS_HOSTS = frozenset({'cdn.amazonlinux.com'})
_SYSBENCH_READINESS_HOSTS = frozenset({
    'github.com',
    'codeload.github.com',
})
_STREAM_READINESS_HOSTS = frozenset({'raw.githubusercontent.com'})
_PHORONIX_READINESS_HOSTS = frozenset({
    'github.com',
    'openbenchmarking.org',
})
_LLAMA_READINESS_HOSTS = frozenset(llama_cpp.READINESS_HOSTS)

AL2023_AMI_PARAMETER = (
    '/aws/service/ami-amazon-linux-latest/'
    'al2023-ami-kernel-default-{architecture}'
)

SYSBENCH_VERSION = '1.0.20'
SYSBENCH_SOURCE_URL = (
    'https://github.com/akopytov/sysbench/archive/refs/tags/'
    f'{SYSBENCH_VERSION}.tar.gz'
)
SYSBENCH_SOURCE_SHA256 = (
    'e8ee79b1f399b2d167e6a90de52ccc90e52408f7ade1b9b7135727efe181347f'
)
SYSBENCH_BUILD_PACKAGES = frozenset({
    'autoconf',
    'automake',
    'ca-certificates',
    'curl',
    'gcc',
    'gzip',
    'libaio-devel',
    'libtool',
    'make',
    'pkgconf-pkg-config',
    'tar',
})

STREAM_REVISION = '6703f7504a38a8da96b353cadafa64d3c2d7a2d3'
STREAM_SOURCE_URL = (
    'https://raw.githubusercontent.com/jeffhammond/STREAM/'
    f'{STREAM_REVISION}/stream.c'
)
STREAM_SOURCE_SHA256 = (
    'c388924eb140fda95f534cdb808ae7f1f8ebb18da41d8aec1b512a3c8d303c9b'
)
STREAM_PACKAGES = frozenset({'ca-certificates', 'curl', 'gcc', 'libgomp'})
FIO_PACKAGES = frozenset({'fio'})

# AL2023 version-prefixes its PHP RPMs and uses the ``perl`` meta-package for
# the complete core-module set provided by RHEL's ``perl-core`` package.  Do
# not reduce this to ``perl-interpreter``: OpenSSL's Configure script imports
# FindBin before it can generate a Makefile, and AL2023 splits that module into
# a separate dependency pulled in by the meta-package.
PHORONIX_PACKAGE_REPLACEMENTS = {
    'php-cli': frozenset({'php8.2-cli'}),
    'php-common': frozenset({'php8.2-common'}),
    'php-process': frozenset({'php8.2-process'}),
    'php-xml': frozenset({'php8.2-xml'}),
    'perl-core': frozenset({'perl'}),
}

_PACKAGE_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9+_.:-]*$')
_HOST_RE = re.compile(
    r'^(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}'
    r'[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}'
    r'[A-Za-z0-9])?$'
)
_NUMBER_PATTERN = r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?'
_STREAM_RESULT_RE = re.compile(
    rf'^(Copy|Scale|Add|Triad):\s+({_NUMBER_PATTERN})'
    rf'(?:\s+{_NUMBER_PATTERN}){{3}}\s*$',
    re.MULTILINE,
)
_STREAM_SIZING_RE = re.compile(
    r'^OCI_STREAM_SIZING '
    r'aggregate_llc_bytes=(\d+) '
    r'array_elements=(\d+) '
    r'array_bytes=(\d+) '
    r'total_bytes=(\d+) '
    r'mem_available_bytes=(\d+)\s*$',
    re.MULTILINE,
)


@dataclass(frozen=True)
class InstallStep:
    """One observable, independently retryable guest installation step."""

    name: str
    command: str


def normalize_architecture(value: str) -> str:
    """Normalize EC2 and ``uname -m`` spellings to the app's vocabulary."""
    normalized = str(value).strip().lower().replace('-', '_')
    aliases = {
        'amd64': 'x86_64',
        'x64': 'x86_64',
        'x86_64': 'x86_64',
        'arm64': 'aarch64',
        'aarch64': 'aarch64',
    }
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise ValueError(
            f'Amazon Linux 2023 benchmark architecture is unsupported: {value}'
        ) from exc


def amazon_linux_ami_architecture(value: str) -> str:
    """Return the architecture suffix used by AWS's public AL2023 AMI alias."""
    architecture = normalize_architecture(value)
    return 'arm64' if architecture == 'aarch64' else architecture


def ami_parameter_name(value: str) -> str:
    return AL2023_AMI_PARAMETER.format(
        architecture=amazon_linux_ami_architecture(value)
    )


def validate_benchmark_selection(
    benchmarks: Iterable[str],
    *,
    sysbench_workloads: Iterable[str] = (),
    iperf3_protocols: Iterable[str] = (),
    phoronix_profiles: Iterable[str] = (),
    llm_benchmarks: Iterable[str] = (),
) -> None:
    """Reject workloads outside the explicitly supported AWS surface."""
    selected = tuple(dict.fromkeys(str(item) for item in benchmarks))
    unsupported = sorted(set(selected) - SUPPORTED_BENCHMARKS)
    if unsupported:
        raise ValueError(
            'The selected benchmark is not supported on AWS; '
            f'unsupported selection: {", ".join(unsupported)}.'
        )
    llm_selected = tuple(
        dict.fromkeys(str(item) for item in llm_benchmarks)
    )
    unsupported_llm = sorted(set(llm_selected) - SUPPORTED_LLM_BENCHMARKS)
    if unsupported_llm:
        raise ValueError(
            'The selected LLM benchmark is not supported on AWS; '
            f'unsupported selection: {", ".join(unsupported_llm)}.'
        )

    workloads = tuple(dict.fromkeys(str(item) for item in sysbench_workloads))
    if 'sysbench' in selected:
        if not workloads:
            raise ValueError('Select at least one Sysbench workload for AWS.')
        unsupported_workloads = sorted(
            set(workloads) - SUPPORTED_SYSBENCH_WORKLOADS
        )
        if unsupported_workloads:
            raise ValueError(
                'The selected Sysbench workload is not supported on AWS; '
                f'unsupported selection: {", ".join(unsupported_workloads)}.'
            )

    profiles = tuple(dict.fromkeys(str(item) for item in phoronix_profiles))
    if 'phoronix' in selected:
        if not profiles:
            raise ValueError('Select at least one Phoronix profile for AWS.')
        unsupported_profiles = sorted(
            set(profiles) - SUPPORTED_PHORONIX_PROFILES
        )
        if unsupported_profiles:
            raise ValueError(
                'The Phoronix profile is not supported on Amazon Linux 2023: '
                f'{", ".join(unsupported_profiles)}.'
            )

    protocols = tuple(dict.fromkeys(str(item) for item in iperf3_protocols))
    if 'iperf3' in selected:
        if not protocols:
            raise ValueError('Select at least one iperf3 protocol for AWS.')
        unsupported_protocols = sorted(
            set(protocols) - SUPPORTED_IPERF3_PROTOCOLS
        )
        if unsupported_protocols:
            raise ValueError(
                'The selected iperf3 protocol is not supported on AWS; '
                f'unsupported selection: {", ".join(unsupported_protocols)}.'
            )


def benchmark_readiness_hosts(
    benchmarks: Iterable[str],
    *,
    llm_benchmarks: Iterable[str] = (),
) -> tuple[str, ...]:
    """Return only the external hosts needed by the selected AWS tools."""
    selected = set(str(item) for item in benchmarks)
    hosts = set(_BASE_READINESS_HOSTS)
    if selected & {'sysbench', 'sysbench_cpu', 'sysbench_memory'}:
        hosts.update(_SYSBENCH_READINESS_HOSTS)
    if 'stream' in selected:
        hosts.update(_STREAM_READINESS_HOSTS)
    if 'phoronix' in selected:
        hosts.update(_PHORONIX_READINESS_HOSTS)
    if set(str(item) for item in llm_benchmarks) & SUPPORTED_LLM_BENCHMARKS:
        hosts.update(_LLAMA_READINESS_HOSTS)
    return tuple(host for host in DEFAULT_READINESS_HOSTS if host in hosts)


def readiness_command(
    required_hosts: Iterable[str] = DEFAULT_READINESS_HOSTS,
) -> str:
    """Wait for cloud-init, public DNS, and the active AL2023 DNF repo."""
    hosts = tuple(dict.fromkeys(str(host).strip() for host in required_hosts))
    if not hosts:
        raise ValueError('At least one readiness hostname is required.')
    invalid = [host for host in hosts if not _HOST_RE.fullmatch(host)]
    if invalid:
        raise ValueError(f'Invalid readiness hostname: {invalid[0]}')
    host_list = ' '.join(shlex.quote(host) for host in hosts)
    return (
        'set -u; '
        'if command -v cloud-init >/dev/null 2>&1; then '
        'CLOUD_INIT_OUTPUT=$(sudo timeout 180 cloud-init status --wait 2>&1); '
        'CLOUD_INIT_RC=$?; printf "%s\\n" "$CLOUD_INIT_OUTPUT"; '
        'if [ "$CLOUD_INIT_RC" -eq 124 ]; then '
        'echo "Warning: cloud-init was still running after 180 seconds; '
        'continuing with network readiness checks."; '
        'elif [ "$CLOUD_INIT_RC" -ne 0 ]; then '
        'echo "Warning: cloud-init reported a non-success status; '
        'continuing with network readiness checks."; fi; '
        'else echo "Warning: cloud-init is not installed."; fi; '
        f'REQUIRED_HOSTS={shlex.quote(host_list)}; DNS_READY=false; '
        'for attempt in $(seq 1 24); do MISSING_HOSTS=""; '
        'for host in $REQUIRED_HOSTS; do '
        'getent ahostsv4 "$host" >/dev/null 2>&1 '
        '|| MISSING_HOSTS="$MISSING_HOSTS $host"; done; '
        'if [ -z "$MISSING_HOSTS" ]; then DNS_READY=true; break; fi; '
        'echo "Waiting for DNS:$MISSING_HOSTS ($attempt/24)."; sleep 5; '
        'done; if [ "$DNS_READY" != true ]; then '
        'echo "DNS did not resolve required benchmark hosts after '
        '120 seconds:$MISSING_HOSTS" >&2; cat /etc/resolv.conf >&2; '
        'grep "^hosts:" /etc/nsswitch.conf >&2 || true; '
        'ip route >&2 || true; exit 1; fi; '
        'DNF_READY=false; for attempt in $(seq 1 3); do '
        'if sudo timeout 60 dnf -q --setopt=retries=5 '
        '--setopt=timeout=20 makecache --refresh; then '
        'DNF_READY=true; break; fi; '
        'echo "Waiting for the Amazon Linux package repository '
        '($attempt/3)."; sleep 10; done; '
        'if [ "$DNF_READY" != true ]; then '
        'echo "The Amazon Linux package repository was not ready after '
        'three attempts." >&2; exit 1; fi; '
        'if ! command -v curl >/dev/null 2>&1; then '
        'echo "The stock Amazon Linux curl command is unavailable; expected '
        'the curl-minimal package provided by the standard AMI." >&2; '
        'exit 1; fi; curl --version | head -n 1; '
        'echo "Amazon Linux cloud-init, DNS, and package repositories are ready."'
    )


def _validated_packages(packages: Iterable[str]) -> tuple[str, ...]:
    unique = tuple(sorted(set(str(package).strip() for package in packages)))
    if not unique:
        raise ValueError('At least one package is required.')
    invalid = [package for package in unique if not _PACKAGE_RE.fullmatch(package)]
    if invalid:
        raise ValueError(f'Invalid DNF package name: {invalid[0]}')
    return unique


def dnf_install_command(packages: Iterable[str]) -> str:
    """Install AL2023 packages with bounded DNF and metadata retries."""
    package_list = ' '.join(_validated_packages(packages))
    return (
        'set -euo pipefail; INSTALLED=false; '
        'for attempt in $(seq 1 3); do '
        'if sudo timeout 480 dnf -y --setopt=retries=10 '
        f'--setopt=timeout=30 install {package_list}; then '
        'INSTALLED=true; break; fi; '
        'echo "DNF installation failed; refreshing metadata before retry '
        '($attempt/3)." >&2; '
        'sudo dnf clean expire-cache >/dev/null 2>&1 || true; sleep 10; '
        'done; test "$INSTALLED" = true'
    )


def sctp_kernel_support_command(*, use_sudo: bool = True) -> str:
    """Load AL2023 SCTP support, installing the exact running-kernel RPM.

    Amazon Linux keeps less-common modules in a kernel-series-specific
    ``modules-extra`` package.  Deriving the RPM from ``uname -r`` prevents a
    normal repository update from installing modules for a different kernel.
    """

    if not isinstance(use_sudo, bool):
        raise ValueError('use_sudo must be a boolean.')
    privilege = 'sudo ' if use_sudo else ''
    return (
        'set -euo pipefail; '
        'KERNEL_RELEASE=$(uname -r); '
        'KERNEL_SERIES=$(printf "%s\\n" "$KERNEL_RELEASE" '
        "| grep -oE '^[0-9]+\\.[0-9]+'); "
        'if [ -z "$KERNEL_SERIES" ]; then '
        'echo "Unable to derive the Amazon Linux kernel series." >&2; '
        'exit 1; fi; '
        f'if ! {privilege}modprobe sctp >/dev/null 2>&1; then '
        'MODULE_PACKAGE="kernel${KERNEL_SERIES}-modules-extra-'
        '${KERNEL_RELEASE}"; '
        'INSTALLED=false; for attempt in $(seq 1 3); do '
        f'if {privilege}timeout 480 dnf -y --setopt=retries=10 '
        '--setopt=timeout=30 install "$MODULE_PACKAGE"; then '
        'INSTALLED=true; break; fi; '
        'echo "SCTP kernel-module installation failed; refreshing metadata '
        'before retry ($attempt/3)." >&2; '
        f'{privilege}dnf clean expire-cache >/dev/null 2>&1 || true; '
        'if [ "$attempt" -lt 3 ]; then sleep 10; fi; '
        'done; test "$INSTALLED" = true; fi; '
        f'{privilege}modprobe sctp; '
        'MODULE_PATH=$(modinfo -n sctp 2>/dev/null || true); '
        'if [ -z "$MODULE_PATH" ]; then '
        'echo "The running Amazon Linux kernel has no SCTP module metadata." '
        '>&2; exit 1; fi; '
        'if [ ! -d /proc/net/sctp ]; then '
        'echo "The Amazon Linux SCTP kernel interface is unavailable." >&2; '
        'exit 1; fi; '
        'if ! iperf3 --help 2>&1 | grep -q -- "--sctp"; then '
        'echo "The installed iperf3 build lacks SCTP support." >&2; '
        'exit 1; fi; '
        'printf "SCTP_READY kernel=%s module=%s\\n" '
        '"$KERNEL_RELEASE" "$MODULE_PATH"'
    )


def amazon_linux_phoronix_packages(
    profile_ids: Iterable[str],
) -> set[str]:
    """Translate curated PTS package requirements to AL2023 RPM names."""
    packages = phoronix.required_packages(profile_ids)
    translated: set[str] = set()
    for package in packages:
        translated.update(
            PHORONIX_PACKAGE_REPLACEMENTS.get(package, frozenset({package}))
        )
    return translated


def benchmark_packages(
    benchmarks: Iterable[str],
    *,
    sysbench_workloads: Iterable[str] = (),
    iperf3_protocols: Iterable[str] = (),
    phoronix_profiles: Iterable[str] = (),
    llm_benchmarks: Iterable[str] = (),
    additional_volume: bool = False,
) -> set[str]:
    """Return the AL2023 prerequisites for a validated AWS selection."""
    selected = set(benchmarks)
    packages: set[str] = set()
    if 'stream' in selected:
        packages.update(STREAM_PACKAGES)
    if 'fio' in selected:
        packages.update(FIO_PACKAGES)
    if 'sysbench' in selected or selected & {'sysbench_cpu', 'sysbench_memory'}:
        if set(sysbench_workloads or SUPPORTED_SYSBENCH_WORKLOADS):
            packages.update(SYSBENCH_BUILD_PACKAGES)
    if 'phoronix' in selected:
        packages.update(amazon_linux_phoronix_packages(phoronix_profiles))
    if 'iperf3' in selected:
        packages.add('iperf3')
        if 'sctp' in set(str(item) for item in iperf3_protocols):
            packages.add('lksctp-tools')
    if set(str(item) for item in llm_benchmarks) & SUPPORTED_LLM_BENCHMARKS:
        packages.update(llama_cpp.REQUIRED_PACKAGES)
    if additional_volume:
        packages.add('xfsprogs')
    # The standard Amazon Linux 2023 AMI already provides /usr/bin/curl via
    # curl-minimal. The full curl RPM conflicts with that package, while the
    # minimal build supports every HTTPS option used by these benchmarks.
    packages.discard('curl')
    return packages


def sysbench_install_command() -> str:
    """Build the last stable sysbench release from checksum-pinned source."""
    archive = f'/tmp/oci-benchmark-sysbench-{SYSBENCH_VERSION}.tar.gz'
    source = f'/tmp/sysbench-{SYSBENCH_VERSION}'
    return (
        'set -euo pipefail; '
        f'EXPECTED_VERSION={shlex.quote(SYSBENCH_VERSION)}; '
        'if command -v sysbench >/dev/null 2>&1 '
        '&& sysbench --version | grep -Fq "sysbench $EXPECTED_VERSION"; then '
        'sysbench --version; exit 0; fi; '
        f'ARCHIVE={shlex.quote(archive)}; SOURCE_DIR={shlex.quote(source)}; '
        f'EXPECTED_SHA256={shlex.quote(SYSBENCH_SOURCE_SHA256)}; '
        'DOWNLOAD_OK=false; for attempt in $(seq 1 3); do '
        'rm -f "$ARCHIVE.part"; '
        f'if curl -L --fail --retry 3 --retry-all-errors '
        f'--connect-timeout 15 --max-time 300 '
        f'{shlex.quote(SYSBENCH_SOURCE_URL)} -o "$ARCHIVE.part" '
        '&& printf "%s  %s\\n" "$EXPECTED_SHA256" "$ARCHIVE.part" '
        '| sha256sum -c -; then '
        'mv "$ARCHIVE.part" "$ARCHIVE"; DOWNLOAD_OK=true; break; fi; '
        'echo "sysbench source download failed; retrying in 10 seconds '
        '($attempt/3)." >&2; sleep 10; done; '
        'test "$DOWNLOAD_OK" = true; '
        'printf "%s  %s\\n" "$EXPECTED_SHA256" "$ARCHIVE" '
        '| sha256sum -c -; '
        'rm -rf "$SOURCE_DIR"; tar -xzf "$ARCHIVE" -C /tmp; '
        'cd "$SOURCE_DIR"; ./autogen.sh; ./configure --without-mysql; '
        'make -j"$(nproc)"; sudo make install; sudo ldconfig; '
        'command -v sysbench; '
        'sysbench --version | grep -F "sysbench $EXPECTED_VERSION"'
    )


def _thread_count(ocpus: int | float) -> int:
    try:
        value = float(ocpus)
    except (TypeError, ValueError) as exc:
        raise ValueError('Benchmark CPU count must be a positive integer.') from exc
    if not math.isfinite(value) or value < 1 or not value.is_integer():
        raise ValueError('Benchmark CPU count must be a positive integer.')
    return int(value)


def sysbench_cpu_command(ocpus: int | float) -> str:
    threads = _thread_count(ocpus)
    return (
        f'sysbench cpu --threads={threads} --cpu-max-prime=10000 '
        '--time=60 run'
    )


def sysbench_memory_command(ocpus: int | float) -> str:
    threads = _thread_count(ocpus)
    return (
        f'sysbench memory --threads={threads} --time=60 '
        '--memory-block-size=1M --memory-total-size=0 run'
    )


def _storage_directory(value: str) -> str:
    """Validate one absolute benchmark directory before shell quoting it."""

    raw = str(value or '')
    directory = raw.strip()
    path = PurePosixPath(directory)
    if (
        raw != directory
        or not directory.startswith('/')
        or directory.startswith('//')
        or '//' in directory
        or directory == '/'
        or path.as_posix() != directory
        or '..' in path.parts
        or not re.fullmatch(r'/[A-Za-z0-9._/-]+', directory)
    ):
        raise ValueError(
            'Benchmark storage directory must be a normalized absolute path.'
        )
    return directory


def sysbench_fileio_command(storage_directory: str = '/data') -> str:
    """Run the existing 4 GiB random read/write workload on one directory."""

    directory = shlex.quote(_storage_directory(storage_directory))
    return (
        f'cd {directory} '
        '&& sysbench fileio --file-total-size=4G prepare '
        "&& trap 'sysbench fileio --file-total-size=4G cleanup "
        ">/dev/null 2>&1 || true' EXIT "
        '&& sysbench fileio --file-total-size=4G --time=60 '
        '--file-test-mode=rndrw run '
        "&& printf '\\nOCI_SYSBENCH_FILEIO_ERRORS=0\\n'"
    )


def fio_benchmark_command(storage_directory: str = '/data') -> str:
    """Run the established sequential/random fio suite on one directory."""

    directory = shlex.quote(_storage_directory(storage_directory))
    return (
        'set -euo pipefail; '
        'for W in read write randread randwrite; do '
        f'fio --name=$W --directory={directory} --rw=$W '
        '--bs=$([ "$W" = "read" -o "$W" = "write" ] '
        '&& echo 1M || echo 4k) --size=4G --direct=1 --time_based '
        '--runtime=60 --group_reporting --output-format=json; done'
    )


def parse_sysbench_fileio_output(
    output: str,
    *,
    expected_seconds: float = 60,
) -> dict[str, float | int | str]:
    """Validate the timed mixed file-I/O result and expose its core metrics."""
    patterns = {
        'reads_per_second': r'^\s*reads/s:\s*(' + _NUMBER_PATTERN + r')\s*$',
        'writes_per_second': r'^\s*writes/s:\s*(' + _NUMBER_PATTERN + r')\s*$',
        'read_mib_per_second': (
            r'^\s*read,\s*MiB/s:\s*(' + _NUMBER_PATTERN + r')\s*$'
        ),
        'written_mib_per_second': (
            r'^\s*written,\s*MiB/s:\s*(' + _NUMBER_PATTERN + r')\s*$'
        ),
        'total_events': r'^\s*total number of events:\s*(\d+)\s*$',
    }
    matches = {
        name: re.search(pattern, output, re.MULTILINE | re.IGNORECASE)
        for name, pattern in patterns.items()
    }
    elapsed = re.findall(
        rf'^\s*total time:\s*({_NUMBER_PATTERN})s\s*$',
        output,
        re.MULTILINE | re.IGNORECASE,
    )
    error_marker = re.search(
        r'^OCI_SYSBENCH_FILEIO_ERRORS=(\d+)\s*$',
        output,
        re.MULTILINE,
    )
    if not all(matches.values()) or not elapsed or not error_marker:
        raise ValueError(
            'Sysbench file I/O output is missing throughput, operation rate, '
            'elapsed-time, event-count, or error-status fields.'
        )
    metrics: dict[str, float | int | str] = {
        name: (
            int(match.group(1))
            if name == 'total_events'
            else float(match.group(1))
        )
        for name, match in matches.items()
    }
    metrics['elapsed_seconds'] = float(elapsed[-1])
    metrics['errors'] = int(error_marker.group(1))
    metrics['unit'] = 'operations/s and MiB/s'
    if metrics['errors'] != 0:
        raise ValueError('Sysbench file I/O reported one or more errors.')
    for name in (
        'reads_per_second',
        'writes_per_second',
        'read_mib_per_second',
        'written_mib_per_second',
        'total_events',
        'elapsed_seconds',
    ):
        value = float(metrics[name])
        if not math.isfinite(value) or value <= 0:
            raise ValueError(
                f'Sysbench file I/O metric {name} must be positive and finite.'
            )
    minimum_seconds = float(expected_seconds) * 0.9
    if metrics['elapsed_seconds'] < minimum_seconds:
        raise ValueError(
            'Sysbench file I/O stopped before the requested timed interval: '
            f'{metrics["elapsed_seconds"]:g}s observed; expected at least '
            f'{minimum_seconds:g}s.'
        )
    return metrics


def _json_documents(output: str) -> list[dict]:
    decoder = json.JSONDecoder()
    documents = []
    position = 0
    while position < len(output):
        while position < len(output) and output[position].isspace():
            position += 1
        if position >= len(output):
            break
        try:
            value, position = decoder.raw_decode(output, position)
        except json.JSONDecodeError as exc:
            raise ValueError(
                'fio output contains text that is not one of its JSON results.'
            ) from exc
        if not isinstance(value, dict):
            raise ValueError('Each fio result must be a JSON object.')
        documents.append(value)
    return documents


def parse_fio_output(output: str) -> dict[str, float | int | str]:
    """Validate all four concatenated fio JSON results from the storage suite."""
    expected = ('read', 'write', 'randread', 'randwrite')
    documents = _json_documents(output)
    if len(documents) != len(expected):
        raise ValueError(
            f'fio returned {len(documents)} JSON results; expected exactly '
            f'{len(expected)}.'
        )
    metrics: dict[str, float | int | str] = {
        'workload_count': len(documents),
        'errors': 0,
        'bandwidth_unit': 'bytes/s',
        'iops_unit': 'IOPS',
    }
    for expected_name, document in zip(expected, documents):
        jobs = document.get('jobs')
        if not isinstance(jobs, list) or len(jobs) != 1:
            raise ValueError(
                f'fio {expected_name} result must contain exactly one job.'
            )
        job = jobs[0]
        if not isinstance(job, dict) or job.get('jobname') != expected_name:
            raise ValueError(
                f'fio result order/name mismatch; expected {expected_name}.'
            )
        try:
            error_count = int(job.get('error'))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f'fio {expected_name} result is missing its error count.'
            ) from exc
        if error_count != 0:
            raise ValueError(f'fio {expected_name} reported error {error_count}.')
        direction = 'read' if expected_name in {'read', 'randread'} else 'write'
        values = job.get(direction)
        if not isinstance(values, dict):
            raise ValueError(
                f'fio {expected_name} result is missing {direction} metrics.'
            )
        bandwidth = values.get('bw_bytes')
        if bandwidth is None and values.get('bw') is not None:
            bandwidth = float(values['bw']) * 1024
        iops = values.get('iops')
        runtime_ms = values.get('runtime')
        for label, value in (
            ('bandwidth', bandwidth),
            ('IOPS', iops),
            ('runtime', runtime_ms),
        ):
            try:
                numeric = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f'fio {expected_name} {label} is missing or invalid.'
                ) from exc
            if not math.isfinite(numeric) or numeric <= 0:
                raise ValueError(
                    f'fio {expected_name} {label} must be positive and finite.'
                )
        if float(runtime_ms) < 54000:
            raise ValueError(
                f'fio {expected_name} stopped before its 60-second interval.'
            )
        metrics[f'{expected_name}_bandwidth_bytes_per_second'] = float(
            bandwidth
        )
        metrics[f'{expected_name}_iops'] = float(iops)
        metrics[f'{expected_name}_runtime_ms'] = int(float(runtime_ms))
    return metrics


def parse_sysbench_cpu_output(
    output: str,
    *,
    expected_seconds: float = 60,
) -> dict[str, float | int | str]:
    """Validate a timed sysbench CPU run and return its core metrics."""
    events_per_second = re.search(
        rf'^\s*events per second:\s*({_NUMBER_PATTERN})\s*$',
        output,
        re.MULTILINE,
    )
    total_events = re.search(
        r'^\s*total number of events:\s*(\d+)\s*$',
        output,
        re.MULTILINE,
    )
    elapsed_matches = re.findall(
        rf'^\s*total time:\s*({_NUMBER_PATTERN})s\s*$',
        output,
        re.MULTILINE,
    )
    if not events_per_second or not total_events or not elapsed_matches:
        raise ValueError(
            'Sysbench CPU output is missing its event rate, event count, or '
            'elapsed time.'
        )
    metrics: dict[str, float | int | str] = {
        'events_per_second': float(events_per_second.group(1)),
        'total_events': int(total_events.group(1)),
        'elapsed_seconds': float(elapsed_matches[-1]),
        'unit': 'events/s',
    }
    for name in ('events_per_second', 'total_events', 'elapsed_seconds'):
        value = float(metrics[name])
        if not math.isfinite(value) or value <= 0:
            raise ValueError(
                f'Sysbench CPU metric {name} must be positive and finite.'
            )
    minimum_seconds = float(expected_seconds) * 0.9
    if metrics['elapsed_seconds'] < minimum_seconds:
        raise ValueError(
            'Sysbench CPU stopped before the requested timed interval: '
            f'{metrics["elapsed_seconds"]:g}s observed; expected at least '
            f'{minimum_seconds:g}s.'
        )
    return metrics


def parse_sysbench_memory_output(
    output: str,
    *,
    expected_seconds: float = 60,
) -> dict[str, float | int | str]:
    """Validate a timed sysbench memory run and return its core metrics."""
    operations = re.search(
        rf'^Total operations:\s+(\d+)\s+\(({_NUMBER_PATTERN}) per second\)\s*$',
        output,
        re.MULTILINE,
    )
    transfer = re.search(
        rf'^({_NUMBER_PATTERN}) MiB transferred '
        rf'\(({_NUMBER_PATTERN}) MiB/sec\)\s*$',
        output,
        re.MULTILINE,
    )
    elapsed_matches = re.findall(
        rf'^\s*total time:\s*({_NUMBER_PATTERN})s\s*$',
        output,
        re.MULTILINE,
    )
    if not operations or not transfer or not elapsed_matches:
        raise ValueError(
            'Sysbench memory output is missing operations, transfer rate, '
            'or elapsed time.'
        )
    metrics: dict[str, float | int | str] = {
        'operations': int(operations.group(1)),
        'operations_per_second': float(operations.group(2)),
        'transferred_mib': float(transfer.group(1)),
        'throughput_mib_per_second': float(transfer.group(2)),
        'elapsed_seconds': float(elapsed_matches[-1]),
        'unit': 'MiB/s',
    }
    for name in (
        'operations',
        'operations_per_second',
        'transferred_mib',
        'throughput_mib_per_second',
        'elapsed_seconds',
    ):
        value = float(metrics[name])
        if not math.isfinite(value) or value <= 0:
            raise ValueError(
                f'Sysbench memory metric {name} must be positive and finite.'
            )
    minimum_seconds = float(expected_seconds) * 0.9
    if metrics['elapsed_seconds'] < minimum_seconds:
        raise ValueError(
            'Sysbench memory stopped before the requested timed interval: '
            f'{metrics["elapsed_seconds"]:g}s observed; expected at least '
            f'{minimum_seconds:g}s.'
        )
    return metrics


def stream_benchmark_command(ocpus: int | float) -> str:
    """Download, verify, compile, and execute an exact STREAM revision."""
    threads = _thread_count(ocpus)
    source = '/tmp/oci-benchmark-stream.c'
    executable = '/tmp/oci-benchmark-stream'
    return (
        'set -euo pipefail; '
        f'SOURCE={shlex.quote(source)}; BINARY={shlex.quote(executable)}; '
        f'EXPECTED_SHA256={shlex.quote(STREAM_SOURCE_SHA256)}; '
        'if ! printf "%s  %s\\n" "$EXPECTED_SHA256" "$SOURCE" '
        '| sha256sum -c - >/dev/null 2>&1; then '
        'DOWNLOAD_OK=false; for attempt in $(seq 1 3); do '
        'rm -f "$SOURCE.part"; '
        f'if curl -L --fail --retry 3 --retry-all-errors '
        f'--connect-timeout 15 --max-time 300 '
        f'{shlex.quote(STREAM_SOURCE_URL)} -o "$SOURCE.part" '
        '&& printf "%s  %s\\n" "$EXPECTED_SHA256" "$SOURCE.part" '
        '| sha256sum -c -; then '
        'mv "$SOURCE.part" "$SOURCE"; DOWNLOAD_OK=true; break; fi; '
        'echo "STREAM source download failed; retrying in 10 seconds '
        '($attempt/3)." >&2; sleep 10; done; '
        'test "$DOWNLOAD_OK" = true; fi; '
        'printf "%s  %s\\n" "$EXPECTED_SHA256" "$SOURCE" '
        '| sha256sum -c -; '
        'CACHE_ROWS=$(for CACHE_DIR in '
        '/sys/devices/system/cpu/cpu[0-9]*/cache/index*; do '
        '[ -r "$CACHE_DIR/level" ] || continue; '
        'CACHE_TYPE=$(cat "$CACHE_DIR/type"); '
        'if [ "$CACHE_TYPE" != Data ] && [ "$CACHE_TYPE" != Unified ]; '
        'then continue; fi; '
        'printf "%s|%s|%s\\n" "$(cat "$CACHE_DIR/level")" '
        '"$(cat "$CACHE_DIR/shared_cpu_list")" '
        '"$(cat "$CACHE_DIR/size")"; done | sort -u); '
        'MAX_CACHE_LEVEL=$(printf "%s\\n" "$CACHE_ROWS" '
        "| awk -F'|' 'NF == 3 && $1 > max { max=$1 } END { print max+0 }'); "
        'LLC_KIB=$(printf "%s\\n" "$CACHE_ROWS" '
        f"| awk -F'|' -v level=\"$MAX_CACHE_LEVEL\" "
        "'function kib(v, u, n) { u=substr(v,length(v),1); "
        "n=substr(v,1,length(v)-1)+0; if (u==\"K\") return n; "
        "if (u==\"M\") return n*1024; if (u==\"G\") return n*1048576; "
        "return v+0 } $1 == level { total += kib($3) } "
        "END { printf \"%.0f\\n\", total }'); "
        "MEM_AVAILABLE_KIB=$(awk '/^MemAvailable:/ { print $2; exit }' "
        '/proc/meminfo); '
        'if [ "${LLC_KIB:-0}" -le 0 ]; then '
        'echo "Unable to derive aggregate last-level cache size from sysfs." '
        '>&2; exit 1; fi; '
        'if [ "${MEM_AVAILABLE_KIB:-0}" -le 0 ]; then '
        'echo "Unable to derive MemAvailable from /proc/meminfo." >&2; '
        'exit 1; fi; '
        'LLC_BYTES=$((LLC_KIB * 1024)); '
        'MIN_ARRAY_BYTES=$((4 * LLC_BYTES)); '
        'DEFAULT_ARRAY_BYTES=$((10000000 * 8)); '
        'if [ "$MIN_ARRAY_BYTES" -lt "$DEFAULT_ARRAY_BYTES" ]; then '
        'ARRAY_BYTES=$DEFAULT_ARRAY_BYTES; else '
        'ARRAY_BYTES=$MIN_ARRAY_BYTES; fi; '
        'ARRAY_ELEMENTS=$(((ARRAY_BYTES + 7) / 8)); '
        'ARRAY_BYTES=$((ARRAY_ELEMENTS * 8)); '
        'TOTAL_BYTES=$((3 * ARRAY_BYTES)); '
        'MEM_AVAILABLE_BYTES=$((MEM_AVAILABLE_KIB * 1024)); '
        'MAX_STREAM_BYTES=$((MEM_AVAILABLE_BYTES / 2)); '
        'if [ "$TOTAL_BYTES" -gt "$MAX_STREAM_BYTES" ]; then '
        'echo "STREAM requires $TOTAL_BYTES bytes so each array is at least '
        'four times the aggregate LLC, but the 50% MemAvailable safety limit '
        'is $MAX_STREAM_BYTES bytes." >&2; exit 1; fi; '
        'printf "OCI_STREAM_SIZING aggregate_llc_bytes=%s '
        'array_elements=%s array_bytes=%s total_bytes=%s '
        'mem_available_bytes=%s\\n" "$LLC_BYTES" "$ARRAY_ELEMENTS" '
        '"$ARRAY_BYTES" "$TOTAL_BYTES" "$MEM_AVAILABLE_BYTES"; '
        'MODEL_FLAGS=(); '
        'if [ "$TOTAL_BYTES" -gt 1610612736 ]; then '
        'case "$(uname -m)" in '
        'x86_64) MODEL_FLAGS=(-mcmodel=medium) ;; '
        'aarch64|arm64) MODEL_FLAGS=(-mcmodel=large -fno-pie -no-pie) ;; '
        '*) echo "Unsupported STREAM architecture: $(uname -m)" >&2; '
        'exit 1 ;; esac; fi; '
        'gcc -O3 -fopenmp "${MODEL_FLAGS[@]}" '
        '-DSTREAM_ARRAY_SIZE="$ARRAY_ELEMENTS" "$SOURCE" -o "$BINARY"; '
        f'OMP_DYNAMIC=false OMP_NUM_THREADS={threads} "$BINARY"'
    )


def parse_stream_output(
    output: str,
    *,
    expected_threads: int | float | None = None,
) -> dict[str, float | int | str]:
    """Reject invalid or non-conforming STREAM output and return bandwidths."""
    if 'Failed Validation' in output or not re.search(
        r'^Solution Validates:', output, re.MULTILINE
    ):
        raise ValueError('STREAM did not report a successful array validation.')

    matches = _STREAM_RESULT_RE.findall(output)
    if len(matches) != 4 or {name for name, _value in matches} != {
        'Copy',
        'Scale',
        'Add',
        'Triad',
    }:
        raise ValueError(
            'STREAM output must contain exactly one Copy, Scale, Add, and '
            'Triad result.'
        )
    bandwidths = {name.lower(): float(value) for name, value in matches}
    for name, value in bandwidths.items():
        if not math.isfinite(value) or value <= 0:
            raise ValueError(
                f'STREAM {name} bandwidth must be positive and finite.'
            )

    sizing = _STREAM_SIZING_RE.search(output)
    actual_elements = re.search(r'^Array size = (\d+) ', output, re.MULTILINE)
    if not sizing or not actual_elements:
        raise ValueError('STREAM output is missing its verified sizing data.')
    (
        aggregate_llc_bytes,
        array_elements,
        array_bytes,
        total_bytes,
        mem_available_bytes,
    ) = (int(value) for value in sizing.groups())
    if (
        aggregate_llc_bytes <= 0
        or array_elements <= 0
        or array_bytes != array_elements * 8
        or int(actual_elements.group(1)) != array_elements
        or total_bytes != array_bytes * 3
        or array_bytes < aggregate_llc_bytes * 4
        or total_bytes > mem_available_bytes // 2
    ):
        raise ValueError('STREAM reported inconsistent or unsafe sizing data.')

    counted_threads = re.search(
        r'^Number of Threads counted =\s*(\d+)\s*$',
        output,
        re.MULTILINE,
    )
    if not counted_threads:
        raise ValueError('STREAM did not report its OpenMP thread count.')
    threads = int(counted_threads.group(1))
    if expected_threads is not None and threads != _thread_count(expected_threads):
        raise ValueError(
            f'STREAM used {threads} threads instead of '
            f'{_thread_count(expected_threads)}.'
        )

    return {
        'copy_mb_per_second': bandwidths['copy'],
        'scale_mb_per_second': bandwidths['scale'],
        'add_mb_per_second': bandwidths['add'],
        'triad_mb_per_second': bandwidths['triad'],
        'unit': 'MB/s',
        'threads': threads,
        'aggregate_llc_bytes': aggregate_llc_bytes,
        'array_elements': array_elements,
        'array_bytes': array_bytes,
        'total_memory_bytes': total_bytes,
    }


def benchmark_commands(
    ocpus: int | float,
    storage_directory: str = '/data',
) -> dict[str, tuple[str, str]]:
    """Return commands compatible with the existing benchmark executor."""
    storage_directory = _storage_directory(storage_directory)
    return {
        'sysbench_cpu': ('Sysbench — CPU', sysbench_cpu_command(ocpus)),
        'sysbench_memory': (
            'Sysbench — Memory',
            sysbench_memory_command(ocpus),
        ),
        'sysbench_fileio': (
            'Sysbench — File I/O',
            sysbench_fileio_command(storage_directory),
        ),
        'fio': (
            'fio storage suite',
            fio_benchmark_command(storage_directory),
        ),
        'stream': ('STREAM', stream_benchmark_command(ocpus)),
    }


def installation_steps(
    benchmarks: Iterable[str],
    *,
    sysbench_workloads: Iterable[str] = (),
    iperf3_protocols: Iterable[str] = (),
    phoronix_profiles: Iterable[str] = (),
    llm_benchmarks: Iterable[str] = (),
    additional_volume: bool = False,
) -> tuple[InstallStep, ...]:
    """Return package and source-build steps in their required order."""
    selected = set(benchmarks)
    packages = benchmark_packages(
        selected,
        sysbench_workloads=sysbench_workloads,
        iperf3_protocols=iperf3_protocols,
        phoronix_profiles=phoronix_profiles,
        llm_benchmarks=llm_benchmarks,
        additional_volume=additional_volume,
    )
    steps: list[InstallStep] = []
    if packages:
        steps.append(
            InstallStep(
                name='Amazon Linux benchmark prerequisites',
                command=dnf_install_command(packages),
            )
        )
    if 'sysbench' in selected or selected & {'sysbench_cpu', 'sysbench_memory'}:
        steps.append(
            InstallStep(
                name=f'sysbench {SYSBENCH_VERSION}',
                command=sysbench_install_command(),
            )
        )
    if 'iperf3' in selected and 'sctp' in set(
        str(item) for item in iperf3_protocols
    ):
        steps.append(
            InstallStep(
                name='Amazon Linux SCTP kernel support',
                command=sctp_kernel_support_command(),
            )
        )
    return tuple(steps)


def iperf3_peer_cloud_init(protocols: Iterable[str]) -> str:
    """Install and supervise an AL2023 iperf3 server during cloud-init."""

    selected = tuple(dict.fromkeys(str(item) for item in protocols))
    if not selected:
        raise ValueError('Select at least one iperf3 peer protocol.')
    unsupported = sorted(set(selected) - SUPPORTED_IPERF3_PROTOCOLS)
    if unsupported:
        raise ValueError(
            'The selected Amazon Linux iperf3 peer protocol is unsupported; '
            f'unsupported selection: {", ".join(unsupported)}.'
        )
    firewall_commands = [
        'firewall-cmd --permanent --add-port=5201/tcp',
    ]
    if 'udp' in selected:
        firewall_commands.append(
            'firewall-cmd --permanent --add-port=5201/udp'
        )
    if 'sctp' in selected:
        firewall_commands.append(
            'firewall-cmd --permanent --add-port=5201/sctp'
        )
    firewall = '\n  '.join(firewall_commands)
    packages = {'iperf3'}
    if 'sctp' in selected:
        packages.add('lksctp-tools')
    install = dnf_install_command(packages).replace('sudo ', '')
    sctp_setup = (
        sctp_kernel_support_command(use_sudo=False)
        if 'sctp' in selected
        else ''
    )
    return f'''#!/bin/bash
set -euo pipefail
{install}
{sctp_setup}
test -x /usr/bin/iperf3
/usr/bin/iperf3 --version
if command -v firewall-cmd >/dev/null 2>&1 \
    && systemctl is-active --quiet firewalld; then
  {firewall}
  firewall-cmd --reload
fi
cat > /etc/systemd/system/iperf3-server.service <<'EOF'
[Unit]
Description=AWS benchmark iperf3 server
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
systemctl is-active --quiet iperf3-server.service
'''
