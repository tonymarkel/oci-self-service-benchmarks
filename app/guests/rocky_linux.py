"""Rocky Linux 9 guest contract for GCP and Azure benchmarks.

The module contains no cloud API calls.  It validates the supported guest
surface and returns deterministic metadata and shell commands for providers
to use when they provision and prepare benchmark instances.  GCP-specific
image and SSH metadata helpers remain here because they describe the same
Rocky Linux guest.
"""

from dataclasses import dataclass
from datetime import datetime
import base64
import json
import re
import shlex
from typing import Iterable

from app import llama_cpp, phoronix
from app.guests import amazon_linux


SSH_USER = 'benchmark'
SUPPORTED_PROVIDERS = frozenset({'gcp', 'azure'})
IMAGE_PROJECT = 'rocky-linux-cloud'
IMAGE_FAMILIES = {
    'x86_64': 'rocky-linux-9',
    'aarch64': 'rocky-linux-9-arm64',
}

SUPPORTED_ARCHITECTURES = frozenset(IMAGE_FAMILIES)
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
# Keep the public constant as the established GCP capability set.  Azure is
# intentionally limited to TCP and UDP until SCTP has been proven through its
# virtual-network data path, not merely loaded successfully in the guest.
SUPPORTED_IPERF3_PROTOCOLS = frozenset({'tcp', 'udp', 'sctp'})
SUPPORTED_IPERF3_PROTOCOLS_BY_PROVIDER = {
    'gcp': SUPPORTED_IPERF3_PROTOCOLS,
    'azure': frozenset({'tcp', 'udp'}),
}
SUPPORTED_PHORONIX_PROFILES = frozenset(phoronix.PROFILES)
SUPPORTED_LLM_BENCHMARKS = frozenset({'llama_bench'})

# Rocky's base GCC and binutils can disagree about newer native x86
# instructions (for example AVX-VNNI).  Keep the compiler, C++ frontend,
# assembler/linker, and Software Collections runtime on one supported toolset
# whenever llama.cpp is selected.
LLAMA_TOOLSET_VERSION = 15
LLAMA_TOOLSET_ENABLE = '/opt/rh/gcc-toolset-15/enable'
LLAMA_TOOLSET_ROOT_DIRECTORY = '/opt/rh/gcc-toolset-15/root/usr'
LLAMA_TOOLSET_BIN_DIRECTORY = '/opt/rh/gcc-toolset-15/root/usr/bin'
LLAMA_TOOLSET_PACKAGES = frozenset({
    'gcc-toolset-15-binutils',
    'gcc-toolset-15-gcc',
    'gcc-toolset-15-gcc-c++',
    'gcc-toolset-15-runtime',
})

DEFAULT_READINESS_HOSTS = (
    'mirrors.rockylinux.org',
    'github.com',
    'codeload.github.com',
    'raw.githubusercontent.com',
    'openbenchmarking.org',
    'huggingface.co',
)

_BASE_READINESS_HOSTS = frozenset({'mirrors.rockylinux.org'})
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

SYSBENCH_BUILD_PACKAGES = frozenset({
    'autoconf',
    'automake',
    'ca-certificates',
    'gcc',
    'gzip',
    'libaio-devel',
    'libtool',
    'make',
    'pkgconf-pkg-config',
    'tar',
})
STREAM_PACKAGES = frozenset({
    'ca-certificates',
    'gcc',
    'libgomp',
})
FIO_PACKAGES = frozenset({'fio'})

_PACKAGE_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9+_.:-]*$')
_HOST_RE = re.compile(
    r'^(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}'
    r'[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}'
    r'[A-Za-z0-9])?$'
)
_GCE_DEVICE_NAME_RE = re.compile(
    r'^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$'
)
_LINUX_USER_RE = re.compile(r'^[a-z_][a-z0-9_-]{0,31}$')
_SSH_ALGORITHM_RE = re.compile(
    r'^(?:ssh-(?:ed25519|rsa)|ecdsa-sha2-nistp(?:256|384|521)|'
    r'sk-ssh-ed25519@openssh\.com|'
    r'sk-ecdsa-sha2-nistp256@openssh\.com)$'
)
_SSH_BLOB_RE = re.compile(r'^[A-Za-z0-9+/]+={0,3}$')


@dataclass(frozen=True)
class InstallStep:
    """One observable, independently retryable guest installation step."""

    name: str
    command: str


def _validated_provider(value: str) -> str:
    provider = str(value).strip().lower()
    if provider not in SUPPORTED_PROVIDERS:
        raise ValueError(
            f'Unsupported Rocky Linux benchmark provider: {provider!r}.'
        )
    return provider


def _provider_label(value: str) -> str:
    return {'gcp': 'GCP', 'azure': 'Azure'}[_validated_provider(value)]


def supported_iperf3_protocols(provider: str = 'gcp') -> frozenset[str]:
    """Return the protocols proven for one provider's network data path."""
    return SUPPORTED_IPERF3_PROTOCOLS_BY_PROVIDER[
        _validated_provider(provider)
    ]


def normalize_architecture(value: str) -> str:
    """Normalize cloud API and guest architecture spellings."""
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
            f'Rocky Linux 9 benchmark architecture is unsupported: {value}'
        ) from exc


def image_family(value: str) -> str:
    """Return the public Rocky Linux 9 family for a machine architecture."""
    return IMAGE_FAMILIES[normalize_architecture(value)]


def image_family_uri(value: str) -> str:
    """Return a Compute Engine source-image family URI."""
    return (
        f'projects/{IMAGE_PROJECT}/global/images/family/'
        f'{image_family(value)}'
    )


def _validated_ssh_key(public_key: str) -> str:
    raw = str(public_key).strip()
    if '\n' in raw or '\r' in raw:
        raise ValueError('The GCP SSH public key must contain exactly one key.')
    parts = raw.split()
    if (
        len(parts) < 2
        or not _SSH_ALGORITHM_RE.fullmatch(parts[0])
        or not _SSH_BLOB_RE.fullmatch(parts[1])
    ):
        raise ValueError('The GCP SSH public key is not a supported OpenSSH key.')
    try:
        base64.b64decode(parts[1], validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            'The GCP SSH public key payload is not valid base64.'
        ) from exc
    return f'{parts[0]} {parts[1]}'


def ssh_metadata_items(
    public_key: str,
    *,
    username: str = SSH_USER,
    expires_at: datetime | None = None,
) -> tuple[dict[str, str], ...]:
    """Build isolated instance metadata for the configured local SSH key.

    The explicit username avoids distribution-specific default accounts.
    Project-wide keys are blocked, and metadata authentication is selected
    explicitly because OS Login ignores the ``ssh-keys`` metadata value.
    """
    username = str(username).strip()
    if username == 'root' or not _LINUX_USER_RE.fullmatch(username):
        raise ValueError('The GCP SSH username is invalid or unsafe.')
    key_value = _validated_ssh_key(public_key)
    value = f'{username}:{key_value}'
    if expires_at is not None:
        if expires_at.tzinfo is None or expires_at.utcoffset() is None:
            raise ValueError('The GCP SSH key expiration must be timezone-aware.')
        expiration = expires_at.strftime('%Y-%m-%dT%H:%M:%S%z')
        value += ' google-ssh ' + json.dumps(
            {'userName': username, 'expireOn': expiration},
            separators=(',', ':'),
        )
    return (
        {'key': 'ssh-keys', 'value': value},
        {'key': 'block-project-ssh-keys', 'value': 'TRUE'},
        {'key': 'enable-oslogin', 'value': 'FALSE'},
    )


def validate_benchmark_selection(
    benchmarks: Iterable[str],
    *,
    sysbench_workloads: Iterable[str] = (),
    iperf3_protocols: Iterable[str] = (),
    phoronix_profiles: Iterable[str] = (),
    llm_benchmarks: Iterable[str] = (),
    provider: str = 'gcp',
) -> None:
    """Reject workloads outside a provider's proven Rocky Linux surface."""
    provider = _validated_provider(provider)
    provider_label = _provider_label(provider)
    selected = tuple(dict.fromkeys(str(item) for item in benchmarks))
    unsupported = sorted(set(selected) - SUPPORTED_BENCHMARKS)
    if unsupported:
        raise ValueError(
            f'The selected benchmark is not supported on {provider_label}; '
            'unsupported '
            f'selection: {", ".join(unsupported)}.'
        )
    llm_selected = tuple(
        dict.fromkeys(str(item) for item in llm_benchmarks)
    )
    unsupported_llm = sorted(set(llm_selected) - SUPPORTED_LLM_BENCHMARKS)
    if unsupported_llm:
        raise ValueError(
            'The selected LLM benchmark is not supported on '
            f'{provider_label}; '
            f'unsupported selection: {", ".join(unsupported_llm)}.'
        )

    workloads = tuple(dict.fromkeys(str(item) for item in sysbench_workloads))
    if 'sysbench' in selected:
        if not workloads:
            raise ValueError(
                'Select at least one Sysbench workload for '
                f'{provider_label}.'
            )
        unsupported_workloads = sorted(
            set(workloads) - SUPPORTED_SYSBENCH_WORKLOADS
        )
        if unsupported_workloads:
            raise ValueError(
                'The selected Sysbench workload is not supported on '
                f'{provider_label}; '
                f'unsupported selection: {", ".join(unsupported_workloads)}.'
            )

    profiles = tuple(dict.fromkeys(str(item) for item in phoronix_profiles))
    if 'phoronix' in selected:
        if not profiles:
            raise ValueError(
                'Select at least one Phoronix profile for '
                f'{provider_label}.'
            )
        unsupported_profiles = sorted(
            set(profiles) - SUPPORTED_PHORONIX_PROFILES
        )
        if unsupported_profiles:
            raise ValueError(
                'The Phoronix profile is not supported on Rocky Linux 9: '
                f'{", ".join(unsupported_profiles)}.'
            )

    protocols = tuple(dict.fromkeys(str(item) for item in iperf3_protocols))
    if 'iperf3' in selected:
        if not protocols:
            raise ValueError(
                'Select at least one iperf3 protocol for '
                f'{provider_label}.'
            )
        unsupported_protocols = sorted(
            set(protocols) - supported_iperf3_protocols(provider)
        )
        if unsupported_protocols:
            raise ValueError(
                'The selected iperf3 protocol is not supported on '
                f'{provider_label}; '
                'unsupported '
                f'selection: {", ".join(unsupported_protocols)}.'
            )


def benchmark_readiness_hosts(
    benchmarks: Iterable[str],
    *,
    llm_benchmarks: Iterable[str] = (),
) -> tuple[str, ...]:
    """Return only external hosts required by the selected guest tools."""
    selected = set(str(item) for item in benchmarks)
    hosts = set(_BASE_READINESS_HOSTS)
    if selected & {
        'sysbench',
        'sysbench_cpu',
        'sysbench_memory',
        'sysbench_fileio',
    }:
        hosts.update(_SYSBENCH_READINESS_HOSTS)
    if 'stream' in selected:
        hosts.update(_STREAM_READINESS_HOSTS)
    if 'phoronix' in selected:
        hosts.update(_PHORONIX_READINESS_HOSTS)
    if set(str(item) for item in llm_benchmarks) & SUPPORTED_LLM_BENCHMARKS:
        hosts.update(_LLAMA_READINESS_HOSTS)
    return tuple(host for host in DEFAULT_READINESS_HOSTS if host in hosts)


def readiness_command(
    expected_architecture: str,
    required_hosts: Iterable[str] = DEFAULT_READINESS_HOSTS,
    *,
    provider: str = 'gcp',
) -> str:
    """Verify Rocky 9, architecture, DNS, curl, and bounded DNF readiness."""
    provider_label = _provider_label(provider)
    architecture = normalize_architecture(expected_architecture)
    guest_architecture = 'aarch64' if architecture == 'aarch64' else 'x86_64'
    hosts = tuple(dict.fromkeys(str(host).strip() for host in required_hosts))
    if not hosts:
        raise ValueError('At least one readiness hostname is required.')
    invalid = [host for host in hosts if not _HOST_RE.fullmatch(host)]
    if invalid:
        raise ValueError(f'Invalid readiness hostname: {invalid[0]}')
    host_list = ' '.join(shlex.quote(host) for host in hosts)
    return (
        'set -u; '
        '. /etc/os-release; '
        'if [ "$ID" != rocky ] || [ "${VERSION_ID%%.*}" != 9 ]; then '
        f'echo "The {provider_label} benchmark guest must be Rocky Linux 9." '
        '>&2; '
        'exit 1; fi; '
        f'EXPECTED_ARCH={shlex.quote(guest_architecture)}; '
        'ACTUAL_ARCH=$(uname -m); '
        'if [ "$ACTUAL_ARCH" != "$EXPECTED_ARCH" ]; then '
        'echo "Rocky Linux architecture mismatch: expected $EXPECTED_ARCH, '
        'got $ACTUAL_ARCH." >&2; exit 1; fi; '
        f'REQUIRED_HOSTS={shlex.quote(host_list)}; DNS_READY=false; '
        'for attempt in $(seq 1 24); do MISSING_HOSTS=""; '
        'for host in $REQUIRED_HOSTS; do '
        'getent ahostsv4 "$host" >/dev/null 2>&1 '
        '|| MISSING_HOSTS="$MISSING_HOSTS $host"; done; '
        'if [ -z "$MISSING_HOSTS" ]; then DNS_READY=true; break; fi; '
        'echo "Waiting for DNS:$MISSING_HOSTS ($attempt/24)."; '
        'if [ "$attempt" -lt 24 ]; then sleep 5; fi; done; '
        'if [ "$DNS_READY" != true ]; then '
        'echo "DNS did not resolve required Rocky benchmark hosts:'
        '$MISSING_HOSTS" >&2; cat /etc/resolv.conf >&2; '
        'grep "^hosts:" /etc/nsswitch.conf >&2 || true; '
        'ip route >&2 || true; exit 1; fi; '
        'DNF_READY=false; for attempt in $(seq 1 3); do '
        'if sudo timeout 60 dnf -q --setopt=retries=5 '
        '--setopt=timeout=20 makecache --refresh; then '
        'DNF_READY=true; break; fi; '
        'echo "Waiting for the Rocky Linux package repositories '
        '($attempt/3)."; '
        'if [ "$attempt" -lt 3 ]; then sleep 10; fi; done; '
        'if [ "$DNF_READY" != true ]; then '
        'echo "The Rocky Linux package repositories were not ready after '
        'three attempts." >&2; exit 1; fi; '
        'if ! command -v curl >/dev/null 2>&1; then '
        'echo "The stock Rocky Linux curl command is unavailable." >&2; '
        'exit 1; fi; curl --version | head -n 1; '
        'echo "Rocky Linux 9 DNS and package repositories are ready."'
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
    """Install Rocky Linux packages with bounded DNF retries."""
    package_list = ' '.join(_validated_packages(packages))
    return (
        'set -euo pipefail; INSTALLED=false; '
        'for attempt in $(seq 1 3); do '
        'if sudo timeout 480 dnf -y --setopt=retries=10 '
        f'--setopt=timeout=30 install {package_list}; then '
        'INSTALLED=true; break; fi; '
        'echo "DNF installation failed; refreshing metadata before retry '
        '($attempt/3)." >&2; '
        'sudo dnf clean expire-cache >/dev/null 2>&1 || true; '
        'if [ "$attempt" -lt 3 ]; then sleep 10; fi; '
        'done; test "$INSTALLED" = true'
    )


def llama_toolset_verification_command() -> str:
    """Verify llama.cpp will resolve every build tool from GCC Toolset 15."""

    enable_path = shlex.quote(LLAMA_TOOLSET_ENABLE)
    bin_prefix = shlex.quote(f'{LLAMA_TOOLSET_BIN_DIRECTORY}/')
    root_prefix = shlex.quote(f'{LLAMA_TOOLSET_ROOT_DIRECTORY}/')
    return (
        'set -euo pipefail; '
        f'if [ ! -r {enable_path} ]; then '
        'echo "The GCC Toolset 15 enable script is unavailable." >&2; '
        'exit 1; fi; '
        f'source {enable_path}; '
        'for TOOL in gcc g++ as ld; do '
        'TOOL_PATH=$(command -v "$TOOL" || true); '
        f'case "$TOOL_PATH" in {bin_prefix}*) ;; '
        '*) echo "GCC Toolset 15 did not provide $TOOL: $TOOL_PATH" >&2; '
        'exit 1 ;; esac; '
        'done; '
        'COMPILER_AS=$(g++ -print-prog-name=as); '
        'case "$COMPILER_AS" in '
        '/*) COMPILER_AS_PATH=$(readlink -f "$COMPILER_AS") ;; '
        '*) COMPILER_AS_PATH=$(command -v "$COMPILER_AS" || true) ;; '
        'esac; '
        f'case "$COMPILER_AS_PATH" in {root_prefix}*) ;; '
        '*) echo "GCC Toolset 15 selected an external assembler: '
        '$COMPILER_AS_PATH" >&2; exit 1 ;; esac; '
        'printf "gcc: %s\\n" "$(gcc --version | sed -n \'1p\')"; '
        'printf "g++: %s\\n" "$(g++ --version | sed -n \'1p\')"; '
        'printf "assembler: %s (%s)\\n" "$COMPILER_AS_PATH" '
        '"$("$COMPILER_AS_PATH" --version | sed -n \'1p\')"; '
        'printf "linker: %s\\n" "$(ld --version | sed -n \'1p\')"'
    )


def sctp_kernel_support_command(*, use_sudo: bool = True) -> str:
    """Load SCTP, installing modules-extra for the exact running kernel."""

    if not isinstance(use_sudo, bool):
        raise ValueError('use_sudo must be a boolean.')
    privilege = 'sudo ' if use_sudo else ''
    return (
        'set -euo pipefail; '
        'KERNEL_RELEASE=$(uname -r); '
        f'if ! {privilege}modprobe sctp >/dev/null 2>&1; then '
        'MODULE_PACKAGE="kernel-modules-extra-${KERNEL_RELEASE}"; '
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
        'echo "The running Rocky Linux kernel has no SCTP module metadata." '
        '>&2; exit 1; fi; '
        'if [ ! -d /proc/net/sctp ]; then '
        'echo "The Rocky Linux SCTP kernel interface is unavailable." >&2; '
        'exit 1; fi; '
        'if ! iperf3 --help 2>&1 | grep -q -- "--sctp"; then '
        'echo "The installed iperf3 build lacks SCTP support." >&2; '
        'exit 1; fi; '
        'printf "SCTP_READY kernel=%s module=%s\\n" '
        '"$KERNEL_RELEASE" "$MODULE_PATH"'
    )


def rocky_phoronix_packages(profile_ids: Iterable[str]) -> set[str]:
    """Return the curated Phoronix RPM names used directly by Rocky 9."""
    return phoronix.required_packages(profile_ids)


def benchmark_packages(
    benchmarks: Iterable[str],
    *,
    sysbench_workloads: Iterable[str] = (),
    iperf3_protocols: Iterable[str] = (),
    phoronix_profiles: Iterable[str] = (),
    llm_benchmarks: Iterable[str] = (),
    additional_volume: bool = False,
    provider: str = 'gcp',
) -> set[str]:
    """Return Rocky Linux 9 prerequisites for a validated selection."""
    provider = _validated_provider(provider)
    selected = set(benchmarks)
    protocols = set(str(item) for item in iperf3_protocols)
    unsupported_protocols = sorted(
        protocols - supported_iperf3_protocols(provider)
    )
    if unsupported_protocols:
        raise ValueError(
            'The selected Rocky Linux iperf3 protocol is not supported on '
            f'{_provider_label(provider)}; unsupported selection: '
            f'{", ".join(unsupported_protocols)}.'
        )
    packages: set[str] = set()
    if 'stream' in selected:
        packages.update(STREAM_PACKAGES)
    if 'fio' in selected:
        packages.update(FIO_PACKAGES)
    if 'sysbench' in selected or selected & {
        'sysbench_cpu',
        'sysbench_memory',
        'sysbench_fileio',
    }:
        if set(sysbench_workloads or SUPPORTED_SYSBENCH_WORKLOADS):
            packages.update(SYSBENCH_BUILD_PACKAGES)
    if 'phoronix' in selected:
        packages.update(rocky_phoronix_packages(phoronix_profiles))
    if 'iperf3' in selected:
        packages.add('iperf3')
        if 'sctp' in protocols:
            packages.add('lksctp-tools')
    llama_selected = bool(
        set(str(item) for item in llm_benchmarks)
        & SUPPORTED_LLM_BENCHMARKS
    )
    if llama_selected:
        packages.update(llama_cpp.REQUIRED_PACKAGES)
        packages.update(LLAMA_TOOLSET_PACKAGES)
    if additional_volume:
        packages.add('xfsprogs')
    # Supported Rocky images include a curl command.  Avoid requesting the
    # full curl RPM because a minimal provider can already own that path.
    packages.discard('curl')
    return packages


def installation_steps(
    benchmarks: Iterable[str],
    *,
    sysbench_workloads: Iterable[str] = (),
    iperf3_protocols: Iterable[str] = (),
    phoronix_profiles: Iterable[str] = (),
    llm_benchmarks: Iterable[str] = (),
    additional_volume: bool = False,
    provider: str = 'gcp',
) -> tuple[InstallStep, ...]:
    """Return package and pinned-source steps in dependency order."""
    selected = set(benchmarks)
    packages = benchmark_packages(
        selected,
        sysbench_workloads=sysbench_workloads,
        iperf3_protocols=iperf3_protocols,
        phoronix_profiles=phoronix_profiles,
        llm_benchmarks=llm_benchmarks,
        additional_volume=additional_volume,
        provider=provider,
    )
    steps: list[InstallStep] = []
    if packages:
        steps.append(InstallStep(
            name='Rocky Linux benchmark prerequisites',
            command=dnf_install_command(packages),
        ))
    if set(str(item) for item in llm_benchmarks) & SUPPORTED_LLM_BENCHMARKS:
        steps.append(InstallStep(
            name=f'GCC Toolset {LLAMA_TOOLSET_VERSION} for llama.cpp',
            command=llama_toolset_verification_command(),
        ))
    if 'sysbench' in selected or selected & {
        'sysbench_cpu',
        'sysbench_memory',
        'sysbench_fileio',
    }:
        steps.append(InstallStep(
            name=f'sysbench {amazon_linux.SYSBENCH_VERSION}',
            command=amazon_linux.sysbench_install_command(),
        ))
    if 'iperf3' in selected and 'sctp' in set(
        str(item) for item in iperf3_protocols
    ):
        steps.append(InstallStep(
            name='Rocky Linux SCTP kernel support',
            command=sctp_kernel_support_command(),
        ))
    return tuple(steps)


def data_volume_mount_command(
    device_name: str = 'benchmark-data',
    *,
    owner: str = SSH_USER,
) -> str:
    """Format and mount only the named GCE data disk at ``/data``.

    The provider-supplied device name becomes a stable
    ``/dev/disk/by-id/google-*`` link for either SCSI or NVMe.  The command
    formats only a blank device, requires XFS on replay, and persists the
    filesystem UUID rather than a kernel-assigned device path.
    """
    device_name = str(device_name).strip()
    owner = str(owner).strip()
    if not _GCE_DEVICE_NAME_RE.fullmatch(device_name):
        raise ValueError('The GCP data-disk device name is invalid.')
    if owner == 'root' or not _LINUX_USER_RE.fullmatch(owner):
        raise ValueError('The GCP data-disk owner is invalid or unsafe.')
    device_link = f'/dev/disk/by-id/google-{device_name}'
    return (
        'set -euo pipefail; '
        f'DEVICE_LINK={shlex.quote(device_link)}; '
        'MOUNT_POINT=/data; '
        f'OWNER={shlex.quote(owner)}; '
        'DEVICE_READY=false; for attempt in $(seq 1 60); do '
        'if [ -b "$DEVICE_LINK" ]; then DEVICE_READY=true; break; fi; '
        'echo "Waiting for exact GCP data disk $DEVICE_LINK ($attempt/60)."; '
        'if [ "$attempt" -lt 60 ]; then sleep 2; fi; done; '
        'if [ "$DEVICE_READY" != true ]; then '
        'echo "The exact GCP data disk did not appear: $DEVICE_LINK" >&2; '
        'exit 1; fi; '
        'DEVICE=$(readlink -f "$DEVICE_LINK"); '
        'if [ ! -b "$DEVICE" ]; then '
        'echo "The GCP data-disk link does not resolve to a block device." '
        '>&2; exit 1; fi; '
        'ROOT_SOURCE=$(sudo findmnt -rn -o SOURCE --mountpoint /); '
        'ROOT_DEVICE=$(readlink -f "$ROOT_SOURCE"); '
        'ROOT_PARENT=$(lsblk -no PKNAME "$ROOT_DEVICE" 2>/dev/null '
        '| head -n 1 || true); '
        'if [ "$DEVICE" = "$ROOT_DEVICE" ] '
        '|| { [ -n "$ROOT_PARENT" ] '
        '&& [ "$DEVICE" = "/dev/$ROOT_PARENT" ]; }; then '
        'echo "Refusing to format or mount the GCP boot disk as /data." >&2; '
        'exit 1; fi; '
        'FSTYPE=$(sudo blkid -s TYPE -o value "$DEVICE" 2>/dev/null || true); '
        'if [ -z "$FSTYPE" ]; then sudo mkfs.xfs "$DEVICE"; '
        'elif [ "$FSTYPE" != xfs ]; then '
        'echo "Refusing to replace unexpected $FSTYPE filesystem on '
        '$DEVICE_LINK." >&2; exit 1; fi; '
        'UUID=$(sudo blkid -s UUID -o value "$DEVICE"); '
        'if [ -z "$UUID" ]; then '
        'echo "The GCP data disk has no filesystem UUID." >&2; exit 1; fi; '
        'sudo mkdir -p "$MOUNT_POINT"; '
        'MOUNTED_SOURCE=$(sudo findmnt -rn -o SOURCE '
        '--mountpoint "$MOUNT_POINT" 2>/dev/null || true); '
        'MOUNTED_UUID=$(sudo findmnt -rn -o UUID '
        '--mountpoint "$MOUNT_POINT" 2>/dev/null || true); '
        'if [ -n "$MOUNTED_SOURCE" ] '
        '&& { [ -z "$MOUNTED_UUID" ] '
        '|| [ "$MOUNTED_UUID" != "$UUID" ]; }; then '
        'echo "A different filesystem is already mounted at /data." >&2; '
        'exit 1; fi; '
        'if sudo awk -v mount="$MOUNT_POINT" '
        "'$0 !~ /^[[:space:]]*#/ && NF >= 2 && $2 == mount "
        "&& $0 !~ /# cloud-benchmark-data$/ { found=1 } "
        "END { exit found ? 0 : 1 }' /etc/fstab; then "
        'echo "Refusing to replace a non-benchmark /data fstab entry." >&2; '
        'exit 1; fi; '
        'FSTAB_TMP=$(mktemp); '
        "sudo awk '$0 !~ /# cloud-benchmark-data$/' /etc/fstab "
        '>"$FSTAB_TMP"; '
        'printf "UUID=%s /data xfs discard,nofail 0 2 '
        '# cloud-benchmark-data\\n" "$UUID" >>"$FSTAB_TMP"; '
        'sudo install -m 0644 "$FSTAB_TMP" /etc/fstab; '
        'rm -f "$FSTAB_TMP"; '
        'if [ -z "$MOUNTED_UUID" ]; then sudo mount "$MOUNT_POINT"; fi; '
        'VERIFY_UUID=$(sudo findmnt -rn -o UUID --mountpoint "$MOUNT_POINT"); '
        'if [ "$VERIFY_UUID" != "$UUID" ]; then '
        'echo "The GCP data disk did not mount by its expected UUID." >&2; '
        'exit 1; fi; '
        'sudo chown "$OWNER:$OWNER" "$MOUNT_POINT"; '
        'sudo chmod 0775 "$MOUNT_POINT"; '
        'if command -v restorecon >/dev/null 2>&1; then '
        'sudo restorecon -RF "$MOUNT_POINT"; fi; '
        'printf "GCP_DATA_VOLUME device_link=%s uuid=%s '
        'mount_point=/data filesystem=xfs\\n" "$DEVICE_LINK" "$UUID"'
    )


def azure_data_volume_mount_command(
    lun: int = 0,
    *,
    owner: str = SSH_USER,
) -> str:
    """Format and mount only the Azure data disk at an exact LUN.

    Azure exposes stable udev links for both NVMe and SCSI data disks.  The
    command accepts either exact link for the requested LUN, never enumerates
    arbitrary block devices, formats only a blank disk, and persists the XFS
    filesystem UUID rather than a kernel-assigned device path.
    """
    if isinstance(lun, bool) or not isinstance(lun, int) or not 0 <= lun <= 63:
        raise ValueError('The Azure data-disk LUN must be an integer from 0 to 63.')
    owner = str(owner).strip()
    if owner == 'root' or not _LINUX_USER_RE.fullmatch(owner):
        raise ValueError('The Azure data-disk owner is invalid or unsafe.')
    return (
        'set -euo pipefail; '
        f'LUN={lun}; '
        'MOUNT_POINT=/data; '
        f'OWNER={shlex.quote(owner)}; '
        'NVME_LINK="/dev/disk/azure/data/by-lun/$LUN"; '
        'SCSI_LINK="/dev/disk/azure/scsi1/lun$LUN"; '
        'DEVICE_LINK=""; DEVICE_READY=false; '
        'for attempt in $(seq 1 60); do '
        'for candidate in "$NVME_LINK" "$SCSI_LINK"; do '
        'if [ -b "$candidate" ]; then DEVICE_LINK="$candidate"; '
        'DEVICE_READY=true; break; fi; done; '
        'if [ "$DEVICE_READY" = true ]; then break; fi; '
        'echo "Waiting for exact Azure data-disk LUN $LUN ($attempt/60)."; '
        'if [ "$attempt" -lt 60 ]; then sleep 2; fi; done; '
        'if [ "$DEVICE_READY" != true ]; then '
        'echo "Azure data-disk LUN $LUN did not appear at either expected '
        'stable link: $NVME_LINK or $SCSI_LINK" >&2; exit 1; fi; '
        'DEVICE=$(readlink -f "$DEVICE_LINK"); '
        'if [ ! -b "$DEVICE" ]; then '
        'echo "The Azure data-disk link does not resolve to a block device." '
        '>&2; exit 1; fi; '
        'ROOT_SOURCE=$(sudo findmnt -rn -o SOURCE --mountpoint /); '
        'ROOT_DEVICE=$(readlink -f "$ROOT_SOURCE"); '
        'ROOT_PARENT=$(lsblk -no PKNAME "$ROOT_DEVICE" 2>/dev/null '
        '| head -n 1 || true); '
        'if [ "$DEVICE" = "$ROOT_DEVICE" ] '
        '|| { [ -n "$ROOT_PARENT" ] '
        '&& [ "$DEVICE" = "/dev/$ROOT_PARENT" ]; }; then '
        'echo "Refusing to format or mount the Azure boot disk as /data." '
        '>&2; exit 1; fi; '
        'FSTYPE=$(sudo blkid -s TYPE -o value "$DEVICE" 2>/dev/null || true); '
        'if [ -z "$FSTYPE" ]; then sudo mkfs.xfs "$DEVICE"; '
        'elif [ "$FSTYPE" != xfs ]; then '
        'echo "Refusing to replace unexpected $FSTYPE filesystem on Azure '
        'data-disk LUN $LUN." >&2; exit 1; fi; '
        'UUID=$(sudo blkid -s UUID -o value "$DEVICE"); '
        'if [ -z "$UUID" ]; then '
        'echo "Azure data-disk LUN $LUN has no filesystem UUID." >&2; '
        'exit 1; fi; '
        'sudo mkdir -p "$MOUNT_POINT"; '
        'MOUNTED_SOURCE=$(sudo findmnt -rn -o SOURCE '
        '--mountpoint "$MOUNT_POINT" 2>/dev/null || true); '
        'MOUNTED_UUID=$(sudo findmnt -rn -o UUID '
        '--mountpoint "$MOUNT_POINT" 2>/dev/null || true); '
        'if [ -n "$MOUNTED_SOURCE" ] '
        '&& { [ -z "$MOUNTED_UUID" ] '
        '|| [ "$MOUNTED_UUID" != "$UUID" ]; }; then '
        'echo "A different filesystem is already mounted at /data." >&2; '
        'exit 1; fi; '
        'if sudo awk -v mount="$MOUNT_POINT" '
        "'$0 !~ /^[[:space:]]*#/ && NF >= 2 && $2 == mount "
        "&& $0 !~ /# cloud-benchmark-data$/ { found=1 } "
        "END { exit found ? 0 : 1 }' /etc/fstab; then "
        'echo "Refusing to replace a non-benchmark /data fstab entry." >&2; '
        'exit 1; fi; '
        'FSTAB_TMP=$(mktemp); '
        "sudo awk '$0 !~ /# cloud-benchmark-data$/' /etc/fstab "
        '>"$FSTAB_TMP"; '
        'printf "UUID=%s /data xfs discard,nofail 0 2 '
        '# cloud-benchmark-data\\n" "$UUID" >>"$FSTAB_TMP"; '
        'sudo install -m 0644 "$FSTAB_TMP" /etc/fstab; '
        'rm -f "$FSTAB_TMP"; '
        'if [ -z "$MOUNTED_UUID" ]; then sudo mount "$MOUNT_POINT"; fi; '
        'VERIFY_UUID=$(sudo findmnt -rn -o UUID --mountpoint "$MOUNT_POINT"); '
        'if [ "$VERIFY_UUID" != "$UUID" ]; then '
        'echo "Azure data-disk LUN $LUN did not mount by its expected UUID." '
        '>&2; exit 1; fi; '
        'sudo chown "$OWNER:$OWNER" "$MOUNT_POINT"; '
        'sudo chmod 0775 "$MOUNT_POINT"; '
        'if command -v restorecon >/dev/null 2>&1; then '
        'sudo restorecon -RF "$MOUNT_POINT"; fi; '
        'printf "AZURE_DATA_VOLUME lun=%s device_link=%s uuid=%s '
        'mount_point=/data filesystem=xfs\\n" '
        '"$LUN" "$DEVICE_LINK" "$UUID"'
    )


def iperf3_peer_startup_script(
    protocols: Iterable[str],
    *,
    provider: str = 'gcp',
) -> str:
    """Install and supervise a Rocky Linux iperf3 peer at boot."""
    provider = _validated_provider(provider)
    provider_label = _provider_label(provider)
    selected = tuple(dict.fromkeys(str(item) for item in protocols))
    if not selected:
        raise ValueError('Select at least one iperf3 peer protocol.')
    unsupported = sorted(set(selected) - supported_iperf3_protocols(provider))
    if unsupported:
        raise ValueError(
            f'The selected Rocky Linux {provider_label} iperf3 peer protocol '
            'is unsupported; '
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
Description={provider_label} benchmark iperf3 server
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


# The benchmark commands and strict parsers are guest-independent and already
# exercised for both x86_64 and aarch64.  Reuse them to keep result semantics
# identical across AWS, GCP, and Azure.
benchmark_commands = amazon_linux.benchmark_commands
parse_fio_output = amazon_linux.parse_fio_output
parse_stream_output = amazon_linux.parse_stream_output
parse_sysbench_cpu_output = amazon_linux.parse_sysbench_cpu_output
parse_sysbench_fileio_output = amazon_linux.parse_sysbench_fileio_output
parse_sysbench_memory_output = amazon_linux.parse_sysbench_memory_output
