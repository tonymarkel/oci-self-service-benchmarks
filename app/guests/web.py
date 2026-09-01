"""Shared guest contracts for two-instance web benchmarks.

Cloud lifecycle code is responsible for creating a benchmark target and one
separate load generator and for recording their canonical addresses in the
job resource manifest.  This module deliberately contains no cloud API calls;
it only describes the provider-specific guest preparation that the shared
ApacheBench and DeathStarBench runners need.
"""

from dataclasses import dataclass
import shlex
from typing import Any, Mapping

from app.guests import amazon_linux, rocky_linux


SUPPORTED_PROVIDERS = frozenset({'oci', 'aws', 'gcp', 'azure'})
SUPPORTED_BENCHMARKS = frozenset({'apachebench', 'deathstarbench'})
SUPPORTED_ROLES = frozenset({'service', 'loadgen'})

PODMAN_COMPOSE_VERSION = '1.5.0'
PODMAN_COMPOSE_URL = (
    'https://raw.githubusercontent.com/containers/podman-compose/'
    f'v{PODMAN_COMPOSE_VERSION}/podman_compose.py'
)
PODMAN_COMPOSE_SHA256 = (
    '7a39883eda17dbf8473e8ebdfd7d27a893bd518ea9583eeaaafee3cca7e50338'
)
PODMAN_COMPOSE_PATHS = {
    'oci': '/usr/bin/podman-compose',
    'aws': '/usr/local/bin/podman-compose',
    'gcp': '/usr/bin/podman-compose',
    'azure': '/usr/bin/podman-compose',
}
MINIMUM_SPAL_SYSTEM_RELEASE = '2023.9.20251117'

_APACHEBENCH_PACKAGES = {
    'service': frozenset({'firewalld', 'httpd'}),
    'loadgen': frozenset({'httpd-tools', 'time'}),
}

_DEATHSTARBENCH_SERVICE_BASE_PACKAGES = {
    'aws': frozenset({'git', 'python3', 'python3-pyyaml'}),
    'gcp': frozenset({'git', 'podman', 'python3', 'python3-pyyaml'}),
    'azure': frozenset({'git', 'podman', 'python3', 'python3-pyyaml'}),
}
_DEATHSTARBENCH_LOADGEN_BASE_PACKAGES = frozenset({
    'gcc',
    'git',
    'make',
    'openssl-devel',
    'python3',
    'time',
    'zlib-devel',
})
_DEATHSTARBENCH_SPAL_PACKAGES = {
    'service': frozenset({'podman', 'python3-dotenv'}),
    'loadgen': frozenset({'luarocks', 'python3-aiohttp'}),
}
_DEATHSTARBENCH_EPEL_PACKAGES = {
    'service': frozenset({'podman-compose'}),
    'loadgen': frozenset({'luarocks', 'python3-aiohttp'}),
}

_BASE_READINESS_HOST = {
    'aws': 'cdn.amazonlinux.com',
    'gcp': 'mirrors.rockylinux.org',
    'azure': 'mirrors.rockylinux.org',
}

_DEATHSTARBENCH_SERVICE_HOSTS = (
    'github.com',
    'raw.githubusercontent.com',
    'registry-1.docker.io',
    'auth.docker.io',
    'archive.ubuntu.com',
    'security.ubuntu.com',
    'ports.ubuntu.com',
    'www.openssl.org',
    'openssl-library.org',
    'openresty.org',
    'luarocks.org',
    'sourceforge.net',
)
_DEATHSTARBENCH_LOADGEN_HOSTS = (
    'github.com',
    'luarocks.org',
)


@dataclass(frozen=True)
class InstallStep:
    """One observable, independently retryable web guest installation step."""

    name: str
    command: str
    timeout_seconds: int = 1800


def provider_id(value: Any) -> str:
    """Resolve the provider while keeping saved pre-provider plans as OCI."""
    if isinstance(value, str):
        raw = value
    elif isinstance(value, Mapping):
        raw = value.get('provider', 'oci')
    else:
        raw = getattr(value, 'provider', 'oci')
    provider = str(raw).strip().lower() if raw is not None else ''
    if provider not in SUPPORTED_PROVIDERS:
        raise ValueError(f'Unsupported web benchmark provider: {provider!r}.')
    return provider


def _validated_benchmark(value: str) -> str:
    benchmark = str(value).strip().lower()
    if benchmark not in SUPPORTED_BENCHMARKS:
        raise ValueError(f'Unsupported shared web benchmark: {benchmark!r}.')
    return benchmark


def _validated_role(value: str) -> str:
    role = str(value).strip().lower()
    if role not in SUPPORTED_ROLES:
        raise ValueError(f'Unsupported web guest role: {role!r}.')
    return role


def readiness_hosts(
    provider_value: Any,
    benchmark_value: str,
    role_value: str,
    *,
    region: str | None = None,
) -> tuple[str, ...]:
    """Return the exact external hosts needed by one web guest role."""
    provider = provider_id(provider_value)
    benchmark = _validated_benchmark(benchmark_value)
    role = _validated_role(role_value)
    if provider == 'oci':
        if not region:
            raise ValueError('OCI web guest readiness requires a region.')
        base = (f'yum.{region}.oci.oraclecloud.com',)
    else:
        base = (_BASE_READINESS_HOST[provider],)
    if benchmark == 'apachebench':
        return base
    additional = (
        _DEATHSTARBENCH_SERVICE_HOSTS
        if role == 'service'
        else _DEATHSTARBENCH_LOADGEN_HOSTS
    )
    return tuple(dict.fromkeys((*base, *additional)))


def readiness_command(
    provider_value: Any,
    benchmark_value: str,
    role_value: str,
    *,
    region: str | None = None,
    expected_architecture: str = 'x86_64',
) -> str:
    """Build cloud guest readiness; OCI retains its orchestrator helper."""
    provider = provider_id(provider_value)
    hosts = readiness_hosts(
        provider,
        benchmark_value,
        role_value,
        region=region,
    )
    if provider == 'aws':
        return amazon_linux.readiness_command(hosts)
    if provider in {'gcp', 'azure'}:
        return rocky_linux.readiness_command(
            expected_architecture,
            hosts,
            provider=provider,
        )
    raise ValueError(
        'OCI web readiness is handled by wait_for_guest_readiness to preserve '
        'the existing Oracle Linux lifecycle.'
    )


def apachebench_install_steps(
    provider_value: Any,
    role_value: str,
) -> tuple[InstallStep, ...]:
    """Return Apache HTTP Server or load-generator packages by provider."""
    provider = provider_id(provider_value)
    role = _validated_role(role_value)
    if provider == 'oci':
        raise ValueError(
            'OCI ApacheBench packages use the existing dnf_install helper.'
        )
    packages = _APACHEBENCH_PACKAGES[role]
    command = (
        amazon_linux.dnf_install_command(packages)
        if provider == 'aws'
        else rocky_linux.dnf_install_command(packages)
    )
    label = (
        'Apache HTTP Server target packages'
        if role == 'service'
        else 'ApacheBench load-generator packages'
    )
    return (InstallStep(label, command),)


def rocky_crb_enable_command() -> str:
    """Enable Rocky's CRB repository before resolving EPEL dependencies."""
    return (
        'set -euo pipefail; '
        'sudo dnf config-manager --set-enabled crb; '
        'sudo dnf repolist --enabled | grep -Eq "(^|[[:space:]])crb([[:space:]]|$)"'
    )


def amazon_linux_spal_prerequisite_command() -> str:
    """Fail closed when the selected AL2023 image predates SPAL support."""
    minimum = shlex.quote(MINIMUM_SPAL_SYSTEM_RELEASE)
    return (
        'set -euo pipefail; '
        f'MINIMUM_RELEASE={minimum}; '
        'CURRENT_RELEASE=$(rpm -q --qf "%{VERSION}" system-release); '
        'test -n "$CURRENT_RELEASE"; '
        'OLDEST=$(printf "%s\\n%s\\n" "$MINIMUM_RELEASE" '
        '"$CURRENT_RELEASE" | sort -V | head -n 1); '
        'if [ "$OLDEST" != "$MINIMUM_RELEASE" ]; then '
        'echo "Amazon Linux system-release $CURRENT_RELEASE predates the '
        'minimum SPAL release $MINIMUM_RELEASE." >&2; exit 1; fi; '
        'echo "Amazon Linux system-release $CURRENT_RELEASE supports SPAL."'
    )


def deathstarbench_install_steps(
    provider_value: Any,
    role_value: str,
) -> tuple[InstallStep, ...]:
    """Return the Podman service or x86 wrk2 guest installation steps."""
    provider = provider_id(provider_value)
    role = _validated_role(role_value)
    if provider == 'oci':
        raise ValueError(
            'OCI DeathStarBench packages use the existing Oracle Linux '
            'installation helpers.'
        )

    if role == 'service':
        base_packages = _DEATHSTARBENCH_SERVICE_BASE_PACKAGES[provider]
    else:
        base_packages = _DEATHSTARBENCH_LOADGEN_BASE_PACKAGES
    dnf_command = (
        amazon_linux.dnf_install_command
        if provider == 'aws'
        else rocky_linux.dnf_install_command
    )
    steps = [InstallStep(
        name=f'DeathStarBench {role} base packages',
        command=dnf_command(base_packages),
    )]

    if provider == 'aws':
        steps.extend((
            InstallStep(
                name='Amazon Linux SPAL release prerequisite',
                command=amazon_linux_spal_prerequisite_command(),
            ),
            InstallStep(
                name='Amazon Linux SPAL repository release package',
                command=amazon_linux.dnf_install_command({'spal-release'}),
            ),
            InstallStep(
                name=f'DeathStarBench {role} SPAL packages',
                command=amazon_linux.dnf_install_command(
                    _DEATHSTARBENCH_SPAL_PACKAGES[role]
                ),
            ),
        ))
        if role == 'service':
            steps.append(InstallStep(
                name=(
                    'checksum-pinned podman-compose '
                    f'{PODMAN_COMPOSE_VERSION}'
                ),
                command=pinned_podman_compose_install_command(),
            ))
        return tuple(steps)

    steps.extend((
        InstallStep(
            name='Rocky Linux DNF repository tooling',
            command=rocky_linux.dnf_install_command({'dnf-plugins-core'}),
        ),
        InstallStep(
            name='Rocky Linux CRB repository',
            command=rocky_crb_enable_command(),
        ),
        InstallStep(
            name='Fedora EPEL 9 repository release package',
            command=rocky_linux.dnf_install_command({'epel-release'}),
        ),
        InstallStep(
            name=f'DeathStarBench {role} EPEL packages',
            command=rocky_linux.dnf_install_command(
                _DEATHSTARBENCH_EPEL_PACKAGES[role]
            ),
        ),
    ))
    return tuple(steps)


def podman_compose_path(provider_value: Any) -> str:
    return PODMAN_COMPOSE_PATHS[provider_id(provider_value)]


def pinned_podman_compose_install_command() -> str:
    """Install one checksum-pinned upstream script on Amazon Linux 2023."""
    url = shlex.quote(PODMAN_COMPOSE_URL)
    checksum = shlex.quote(PODMAN_COMPOSE_SHA256)
    version = shlex.quote(PODMAN_COMPOSE_VERSION)
    destination = shlex.quote(PODMAN_COMPOSE_PATHS['aws'])
    return (
        'set -euo pipefail; '
        f'URL={url}; EXPECTED_SHA256={checksum}; EXPECTED_VERSION={version}; '
        f'DESTINATION={destination}; DOWNLOAD=/tmp/podman-compose.download; '
        'if sudo test -x "$DESTINATION" '
        '&& sudo sha256sum "$DESTINATION" '
        '| grep -Fq "$EXPECTED_SHA256  $DESTINATION"; then '
        '"$DESTINATION" --version | grep -F "$EXPECTED_VERSION"; exit 0; fi; '
        'DOWNLOAD_OK=false; for attempt in $(seq 1 3); do '
        'rm -f "$DOWNLOAD"; '
        'if curl -L --fail --retry 3 --retry-all-errors '
        '--connect-timeout 15 --max-time 300 "$URL" -o "$DOWNLOAD" '
        '&& printf "%s  %s\\n" "$EXPECTED_SHA256" "$DOWNLOAD" '
        '| sha256sum -c -; then DOWNLOAD_OK=true; break; fi; '
        'echo "podman-compose download failed checksum validation; retrying '
        'in 10 seconds ($attempt/3)." >&2; '
        'if [ "$attempt" -lt 3 ]; then sleep 10; fi; done; '
        'test "$DOWNLOAD_OK" = true; '
        'sudo install -o root -g root -m 0755 "$DOWNLOAD" "$DESTINATION"; '
        'rm -f "$DOWNLOAD"; '
        'sudo sha256sum "$DESTINATION" '
        '| grep -F "$EXPECTED_SHA256  $DESTINATION"; '
        '"$DESTINATION" --version | grep -F "$EXPECTED_VERSION"'
    )


def traffic_path(provider_value: Any) -> str:
    return {
        'oci': 'OCI private VCN address',
        'aws': 'AWS private VPC address',
        'gcp': 'GCP private VPC address',
        'azure': 'Azure private VNet address',
    }[provider_id(provider_value)]


def static_runtime_metadata(
    plan: Any,
    resources: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the web topology metadata known before guest setup starts."""
    provider = provider_id(plan)
    values: dict[str, Any] = {
        'provider': {
            'oci': 'OCI',
            'aws': 'AWS',
            'gcp': 'GCP',
            'azure': 'Azure',
        }[provider],
        'region': getattr(plan, 'region', None),
        'service_shape': getattr(plan, 'shape', None),
        'service_memory_gb': getattr(plan, 'memory_gb', None),
        'service_architecture': resources.get('architecture'),
        'target_private_ip': resources.get('private_ip'),
        'load_generator_shape': resources.get('loadgen_shape'),
        'load_generator_memory_gb': resources.get(
            'loadgen_memory_gb',
            8,
        ),
        'load_generator_architecture': resources.get('loadgen_architecture'),
        'load_generator_private_ip': resources.get('loadgen_private_ip'),
        'load_generator_network_bandwidth_gbps': resources.get(
            'loadgen_network_bandwidth_gbps'
        ),
        'traffic_path': traffic_path(provider),
        'load_generator_sizing_note': (
            'The workload uses a separate fixed x86 load generator. GNU time '
            'CPU utilization and peak RSS are included in measured results so '
            'generator saturation is visible.'
        ),
    }
    if provider == 'oci':
        values.update({
            'service_ocpus': getattr(plan, 'ocpus', None),
            'load_generator_ocpus': resources.get('loadgen_ocpus', 2),
        })
    else:
        values.update({
            'service_vcpus': resources.get(
                'vcpu',
                getattr(plan, 'ocpus', None),
            ),
            'load_generator_vcpus': resources.get('loadgen_vcpus'),
        })
    optional_resource_fields = {
        'load_generator_network_peak_bandwidth_gbps': (
            'loadgen_network_peak_bandwidth_gbps'
        ),
        'load_generator_network_capacity_kind': (
            'loadgen_network_capacity_kind'
        ),
        'load_generator_image_id': 'loadgen_image_id',
        'load_generator_image_name': 'loadgen_image_name',
    }
    for metadata_key, resource_key in optional_resource_fields.items():
        if resources.get(resource_key) is not None:
            values[metadata_key] = resources[resource_key]
    return {key: value for key, value in values.items() if value is not None}


def runtime_metadata(
    plan: Any,
    resources: Mapping[str, Any],
    *,
    service_architecture: str | None = None,
    loadgen_architecture: str | None = None,
) -> dict[str, Any]:
    """Add observed guest architecture to the static web topology metadata."""
    values = static_runtime_metadata(plan, resources)
    if service_architecture:
        values['service_architecture'] = service_architecture
    if loadgen_architecture:
        values['load_generator_architecture'] = loadgen_architecture
    return values
