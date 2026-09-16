"""Immutable K3s guest commands for distributed DeathStarBench nodes.

This module deliberately performs no cloud or network work itself.  It builds
auditable commands for the provider lifecycle to execute on short-lived Linux
guests.  K3s is downloaded as an exact release asset and checked against a
digest recorded in source; the moving K3s install channels and ``curl | sh``
installer are never used.

Secrets are also kept out of generated command lines.  ``token_install_command``
reads one token from standard input and installs it as a root-only file.  Both
the server and agent systemd units refer to that file through ``--token-file``.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
import ipaddress
import json
import re
import shlex
from types import MappingProxyType
from typing import Iterable
from urllib.parse import quote as url_quote

from .deathstarbench_contract import (
    DISTRIBUTED_RUNTIME_REVISION,
    K3S_VERSION,
)


K3S_BINARY = '/usr/local/bin/k3s'
K3S_OWNERSHIP_DIRECTORY = '/etc/deathstarbench'
K3S_TOKEN_DIRECTORY = f'{K3S_OWNERSHIP_DIRECTORY}/k3s'
K3S_SERVER_TOKEN_FILE = f'{K3S_TOKEN_DIRECTORY}/server-token'
K3S_AGENT_TOKEN_FILE = f'{K3S_TOKEN_DIRECTORY}/agent-token'
K3S_OWNERSHIP_MARKER = f'{K3S_TOKEN_DIRECTORY}/owned-runtime-v1'
K3S_RESTART_MARKER = '/run/deathstarbench/k3s-restart-required'
K3S_AIRGAP_IMAGES_DIRECTORY = '/var/lib/rancher/k3s/agent/images'
K3S_SERVER_UNIT = '/etc/systemd/system/k3s.service'
K3S_AGENT_UNIT = '/etc/systemd/system/k3s-agent.service'
K3S_SERVER_STATE_DIRECTORY = '/var/lib/rancher/k3s/server'
K3S_AGENT_KUBECONFIG = '/var/lib/rancher/k3s/agent/kubelet.kubeconfig'
K3S_CONFIG_DIRECTORY = '/etc/rancher/k3s'
K3S_CLUSTER_CIDR = '10.42.0.0/16'
K3S_SERVICE_CIDR = '10.43.0.0/16'
K3S_CLUSTER_DNS_IP = '10.43.0.10'
K3S_SERVICE_NODE_PORT_RANGE = '8080-8080'

# K3s's SELinux policy is released independently from the K3s binary.  Keep
# the exact EL9 package and checksum beside the runtime pins so a guest never
# resolves Rancher's moving ``latest`` RPM path during a benchmark run.
K3S_SELINUX_VERSION_RELEASE = '1.6-1.el9'
K3S_SELINUX_ASSET = (
    f'k3s-selinux-{K3S_SELINUX_VERSION_RELEASE}.noarch.rpm'
)
K3S_SELINUX_SHA256 = (
    '23d0095a766b89317d2bdbe8773825987db7221e48743417930a994ca1096793'
)
K3S_SELINUX_URL = (
    'https://github.com/k3s-io/k3s-selinux/releases/download/'
    f'v1.6.stable.1/{K3S_SELINUX_ASSET}'
)
K3S_MODULES_FILE = '/etc/modules-load.d/deathstarbench-k3s.conf'
K3S_SYSCTL_FILE = '/etc/sysctl.d/90-deathstarbench-k3s.conf'

DOWNLOAD_CONNECT_TIMEOUT_SECONDS = 20
DOWNLOAD_MAX_TIME_SECONDS = 600
SERVICE_START_TIMEOUT_SECONDS = 600
READINESS_TIMEOUT_SECONDS = 300

_VERSION_RE = re.compile(r'^v\d+\.\d+\.\d+\+k3s\d+$')
_NODE_NAME_RE = re.compile(
    r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$'
)
_TOKEN_RE = r'^[A-Za-z0-9:._-]{20,512}$'
_TOKEN_PATTERN = re.compile(_TOKEN_RE)
_AGENT_ROLES = frozenset({'application', 'cache', 'database'})
_CLUSTER_ROLES = frozenset({'control', *_AGENT_ROLES})


@dataclass(frozen=True)
class K3sArtifact:
    """One release asset qualified for a canonical Linux architecture."""

    architecture: str
    asset_name: str
    sha256: str
    url: str
    images_asset_name: str
    images_sha256: str
    images_url: str


@dataclass(frozen=True)
class ExpectedK3sNode:
    """Exact node identity that cluster readiness must attest."""

    name: str
    role: str
    private_ip: str
    architecture: str

    def __post_init__(self):
        object.__setattr__(self, 'name', _validated_node_name(self.name))
        role = str(self.role).strip().lower()
        if role not in _CLUSTER_ROLES:
            raise ValueError(f'Unsupported K3s node role: {self.role!r}.')
        object.__setattr__(self, 'role', role)
        object.__setattr__(
            self,
            'private_ip',
            _validated_private_ipv4(self.private_ip),
        )
        object.__setattr__(
            self,
            'architecture',
            normalized_architecture(self.architecture),
        )

    @property
    def kubernetes_architecture(self) -> str:
        return {
            'x86_64': 'amd64',
            'aarch64': 'arm64',
        }[self.architecture]


def _release_url(asset_name: str) -> str:
    if not _VERSION_RE.fullmatch(K3S_VERSION):
        raise RuntimeError(
            'The distributed runtime contract does not contain an exact '
            'K3s release version.'
        )
    encoded_version = url_quote(K3S_VERSION, safe='')
    return (
        'https://github.com/k3s-io/k3s/releases/download/'
        f'{encoded_version}/{asset_name}'
    )


# These are the GitHub release-asset SHA-256 digests for the exact candidate
# named by app.deathstarbench_contract.  Updating K3S_VERSION requires an
# explicit digest update and CI qualification for both architectures.
K3S_ARTIFACTS = MappingProxyType({
    'x86_64': K3sArtifact(
        architecture='x86_64',
        asset_name='k3s',
        sha256=(
            '835873f37245fc615f547a2fe2af9402a347875f13fa64a1f136de644955ea3f'
        ),
        url=_release_url('k3s'),
        images_asset_name='k3s-airgap-images-amd64.tar.zst',
        images_sha256=(
            '9024613e2d468c51ba0e5ba21898604c41339354449fb3338cc6a7931189eb6a'
        ),
        images_url=_release_url('k3s-airgap-images-amd64.tar.zst'),
    ),
    'aarch64': K3sArtifact(
        architecture='aarch64',
        asset_name='k3s-arm64',
        sha256=(
            'c920706346d5ad4e5cd3c7bf1bb09ce71ebe07fec829e513e40f1caf98aed8bb'
        ),
        url=_release_url('k3s-arm64'),
        images_asset_name='k3s-airgap-images-arm64.tar.zst',
        images_sha256=(
            '9d3c4c2197bcf857ca17633aa393bad683cc982ddd408620f93036a3cca953b5'
        ),
        images_url=_release_url('k3s-airgap-images-arm64.tar.zst'),
    ),
})


def normalized_architecture(value: str) -> str:
    """Return the canonical K3s guest architecture or fail closed."""

    architecture = str(value).strip().lower()
    aliases = {
        'amd64': 'x86_64',
        'x86_64': 'x86_64',
        'arm64': 'aarch64',
        'aarch64': 'aarch64',
    }
    try:
        return aliases[architecture]
    except KeyError:
        raise ValueError(
            f'Unsupported or unsafe K3s guest architecture: {value!r}.'
        ) from None


def artifact_for_architecture(value: str) -> K3sArtifact:
    """Resolve immutable release metadata for a supported guest."""

    return K3S_ARTIFACTS[normalized_architecture(value)]


def _validated_node_name(value: str) -> str:
    node_name = str(value).strip()
    if not _NODE_NAME_RE.fullmatch(node_name):
        raise ValueError(
            'K3s node names must be lower-case DNS labels of at most 63 '
            f'characters; received {value!r}.'
        )
    return node_name


def _validated_private_ipv4(value: str) -> str:
    raw = str(value).strip()
    try:
        address = ipaddress.ip_address(raw)
    except ValueError:
        raise ValueError(f'Invalid K3s private IPv4 address: {value!r}.') from None
    if (
        address.version != 4
        or not address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_unspecified
    ):
        raise ValueError(
            f'K3s nodes require a non-loopback private IPv4 address; received '
            f'{value!r}.'
        )
    return str(address)


def _validated_agent_role(value: str) -> str:
    role = str(value).strip().lower()
    if role not in _AGENT_ROLES:
        raise ValueError(f'Unsupported or unsafe K3s agent role: {value!r}.')
    return role


def _validated_selinux_mode(value: bool) -> bool:
    if not isinstance(value, bool):
        raise ValueError('K3s SELinux mode must be an explicit boolean.')
    return value


def validated_token(value: str) -> str:
    """Validate a transient K3s credential without persisting or logging it."""

    token = str(value).strip()
    if not _TOKEN_PATTERN.fullmatch(token):
        raise ValueError('K3s returned an invalid scoped token.')
    return token


def rocky_host_prepare_command(role: str) -> str:
    """Prepare one Rocky Linux 9 guest for the pinned K3s runtime.

    Azure NSGs enforce the exact underlay matrix for this candidate.  K3s's
    documented RHEL-family recommendation is followed by disabling firewalld
    on these ephemeral cluster nodes; pod isolation remains the responsibility
    of the separately checked-in Kubernetes NetworkPolicies.
    """

    role = str(role).strip().lower()
    if role not in _CLUSTER_ROLES:
        raise ValueError(f'Unsupported K3s host role: {role!r}.')
    modules_payload = base64.b64encode(b'overlay\nbr_netfilter\n').decode('ascii')
    sysctl_payload = base64.b64encode(
        b'net.ipv4.ip_forward=1\n'
        b'net.bridge.bridge-nf-call-iptables=1\n'
        b'net.bridge.bridge-nf-call-ip6tables=1\n'
    ).decode('ascii')
    package_names = [
        'ca-certificates',
        'container-selinux',
        'coreutils',
        'findutils',
        'iproute',
        'iptables',
        'kmod',
        'policycoreutils',
        'selinux-policy-base',
        'tar',
        'util-linux',
        'zstd',
    ]
    if role == 'database':
        package_names.extend(('policycoreutils-python-utils', 'xfsprogs'))
    packages = ' '.join(package_names)
    expected_rpm_identity = f'{K3S_SELINUX_VERSION_RELEASE}.noarch'
    return (
        'set -euo pipefail; '
        'test -r /etc/os-release; source /etc/os-release; '
        'test "$ID" = rocky; case "$VERSION_ID" in 9|9.*) ;; *) '
        'echo "The distributed K3s candidate requires Rocky Linux 9." >&2; '
        'exit 1;; esac; '
        f'EXPECTED_ROLE={shlex.quote(role)}; '
        'case "$EXPECTED_ROLE" in control|application|cache|database) ;; '
        '*) exit 1;; esac; '
        'DNF_READY=false; for attempt in $(seq 1 3); do '
        'if sudo timeout 480 dnf -y --setopt=retries=10 '
        f'--setopt=timeout=30 install {packages}; then '
        'DNF_READY=true; break; fi; '
        'echo "K3s prerequisite installation failed; refreshing metadata '
        'before retry ($attempt/3)." >&2; '
        'sudo dnf clean expire-cache >/dev/null 2>&1 || true; '
        'if [ "$attempt" -lt 3 ]; then sleep 10; fi; done; '
        'test "$DNF_READY" = true; '
        f'EXPECTED_SELINUX_RPM={shlex.quote(expected_rpm_identity)}; '
        'CURRENT_SELINUX_RPM=$(rpm -q --qf '
        "'%{VERSION}-%{RELEASE}.%{ARCH}' k3s-selinux 2>/dev/null || true); "
        'if [ "$CURRENT_SELINUX_RPM" != "$EXPECTED_SELINUX_RPM" ]; then '
        'TEMP_DIRECTORY=$(mktemp -d /tmp/deathstarbench-k3s-selinux.XXXXXX); '
        'trap \'rm -rf -- "$TEMP_DIRECTORY"\' EXIT; '
        f'RPM_DOWNLOAD="$TEMP_DIRECTORY/{K3S_SELINUX_ASSET}"; '
        f'curl --fail --location --silent --show-error --proto "=https" '
        f'--tlsv1.2 --retry 3 --retry-connrefused '
        f'--connect-timeout {DOWNLOAD_CONNECT_TIMEOUT_SECONDS} '
        f'--max-time {DOWNLOAD_MAX_TIME_SECONDS} --output "$RPM_DOWNLOAD" '
        f'{shlex.quote(K3S_SELINUX_URL)}; '
        f'printf "%s  %s\\n" {shlex.quote(K3S_SELINUX_SHA256)} '
        '"$RPM_DOWNLOAD" | sha256sum -c -; '
        'sudo timeout 480 dnf -y install "$RPM_DOWNLOAD"; fi; '
        "test \"$(rpm -q --qf '%{VERSION}-%{RELEASE}.%{ARCH}' "
        'k3s-selinux)" '
        '= "$EXPECTED_SELINUX_RPM"; '
        'rpm -V k3s-selinux >/dev/null; '
        f'printf "%s" {shlex.quote(modules_payload)} | base64 --decode '
        '> /tmp/deathstarbench-k3s-modules; '
        f'sudo install -o root -g root -m 0644 '
        f'/tmp/deathstarbench-k3s-modules {shlex.quote(K3S_MODULES_FILE)}; '
        'rm -f /tmp/deathstarbench-k3s-modules; '
        'sudo modprobe overlay; sudo modprobe br_netfilter; '
        f'printf "%s" {shlex.quote(sysctl_payload)} | base64 --decode '
        '> /tmp/deathstarbench-k3s-sysctl; '
        f'sudo install -o root -g root -m 0644 '
        f'/tmp/deathstarbench-k3s-sysctl {shlex.quote(K3S_SYSCTL_FILE)}; '
        'rm -f /tmp/deathstarbench-k3s-sysctl; '
        'sudo sysctl --system >/dev/null; '
        'test "$(sysctl -n net.ipv4.ip_forward)" = 1; '
        'test "$(sysctl -n net.bridge.bridge-nf-call-iptables)" = 1; '
        'if sudo systemctl list-unit-files firewalld.service --no-legend '
        '2>/dev/null | grep -q "^firewalld.service"; then '
        'timeout --signal=TERM 60s sudo systemctl disable --now '
        'firewalld.service >/dev/null; fi; '
        'if sudo systemctl is-active --quiet firewalld.service 2>/dev/null; '
        'then echo "firewalld remained active after disable." >&2; exit 1; fi; '
        'rpm -q container-selinux k3s-selinux >/dev/null; '
        'for COMMAND in curl sha256sum base64 systemctl timeout findmnt '
        'lsmod modprobe; do command -v "$COMMAND" >/dev/null; done'
    )


def host_preflight_command(*, selinux_enabled: bool) -> str:
    """Check the provider-prepared guest before a K3s service is installed.

    OS adapters remain responsible for installing their qualified, pinned
    package set. This command makes a missing SELinux policy, kernel module,
    cgroup mount, or required utility a hard failure instead of silently
    starting a different runtime configuration.
    """

    selinux_enabled = _validated_selinux_mode(selinux_enabled)
    selinux_check = (
        'for COMMAND in getenforce rpm matchpathcon; do '
        'command -v "$COMMAND" >/dev/null; done; '
        'test "$(getenforce)" = Enforcing; '
        'rpm -q container-selinux k3s-selinux >/dev/null; '
        f'matchpathcon -V {shlex.quote(K3S_BINARY)}; '
        if selinux_enabled
        else 'test "$(getenforce 2>/dev/null || echo Disabled)" != Enforcing; '
    )
    return (
        'set -euo pipefail; '
        'for COMMAND in curl sha256sum base64 systemctl timeout findmnt '
        'lsmod modprobe swapon; do command -v "$COMMAND" >/dev/null; done; '
        'test -d /sys/fs/cgroup; '
        'test "$(findmnt -rn -o FSTYPE --mountpoint /sys/fs/cgroup)" '
        '= cgroup2; '
        'test -z "$(swapon --noheadings --show=NAME)"; '
        'sudo modprobe overlay; sudo modprobe br_netfilter; '
        'lsmod | awk \'{print $1}\' | grep -Fx overlay >/dev/null; '
        'lsmod | awk \'{print $1}\' | grep -Fx br_netfilter >/dev/null; '
        + selinux_check
        + f'sudo {shlex.quote(K3S_BINARY)} check-config'
    )


def _binary_verification_fragment(architecture: str) -> str:
    artifact = artifact_for_architecture(architecture)
    images_destination = (
        f'{K3S_AIRGAP_IMAGES_DIRECTORY}/{artifact.images_asset_name}'
    )
    return (
        f'EXPECTED_VERSION={shlex.quote(K3S_VERSION)}; '
        f'EXPECTED_SHA256={shlex.quote(artifact.sha256)}; '
        f'EXPECTED_IMAGES_SHA256={shlex.quote(artifact.images_sha256)}; '
        f'K3S_BINARY={shlex.quote(K3S_BINARY)}; '
        f'K3S_IMAGES={shlex.quote(images_destination)}; '
        'test -x "$K3S_BINARY"; '
        'ACTUAL_SHA256=$(sha256sum "$K3S_BINARY" | awk "{print \\$1}"); '
        'test "$ACTUAL_SHA256" = "$EXPECTED_SHA256"; '
        'test -f "$K3S_IMAGES"; '
        'ACTUAL_IMAGES_SHA256=$(sha256sum "$K3S_IMAGES" '
        '| awk "{print \\$1}"); '
        'test "$ACTUAL_IMAGES_SHA256" = "$EXPECTED_IMAGES_SHA256"; '
        'ACTUAL_VERSION=$("$K3S_BINARY" --version | '
        'awk "NR == 1 {print \\$3}"); '
        'test "$ACTUAL_VERSION" = "$EXPECTED_VERSION"; '
    )


def artifact_install_command(architecture: str) -> str:
    """Install the exact K3s binary and matching system-image bundle."""

    artifact = artifact_for_architecture(architecture)
    images_destination = (
        f'{K3S_AIRGAP_IMAGES_DIRECTORY}/{artifact.images_asset_name}'
    )
    return (
        'set -euo pipefail; '
        f'EXPECTED_ARCH={shlex.quote(artifact.architecture)}; '
        'ACTUAL_ARCH=$(uname -m); '
        'test "$ACTUAL_ARCH" = "$EXPECTED_ARCH"; '
        f'ASSET={shlex.quote(artifact.asset_name)}; '
        f'URL={shlex.quote(artifact.url)}; '
        f'EXPECTED_SHA256={shlex.quote(artifact.sha256)}; '
        f'IMAGES_ASSET={shlex.quote(artifact.images_asset_name)}; '
        f'IMAGES_URL={shlex.quote(artifact.images_url)}; '
        f'EXPECTED_IMAGES_SHA256={shlex.quote(artifact.images_sha256)}; '
        f'EXPECTED_VERSION={shlex.quote(K3S_VERSION)}; '
        f'DESTINATION={shlex.quote(K3S_BINARY)}; '
        f'IMAGES_DESTINATION={shlex.quote(images_destination)}; '
        f'RESTART_MARKER={shlex.quote(K3S_RESTART_MARKER)}; '
        'if [ -x "$DESTINATION" ]; then '
        'CURRENT_SHA256=$(sha256sum "$DESTINATION" | awk "{print \\$1}"); '
        'if [ "$CURRENT_SHA256" = "$EXPECTED_SHA256" ]; then '
        'CURRENT_VERSION=$("$DESTINATION" --version 2>/dev/null '
        '| awk "NR == 1 {print \\$3}") || true; '
        'CURRENT_IMAGES_SHA256=""; '
        'if [ -f "$IMAGES_DESTINATION" ]; then '
        'CURRENT_IMAGES_SHA256=$(sha256sum "$IMAGES_DESTINATION" '
        '| awk "{print \\$1}"); fi; '
        'if [ "$CURRENT_VERSION" = "$EXPECTED_VERSION" ] '
        '&& [ "$CURRENT_IMAGES_SHA256" = "$EXPECTED_IMAGES_SHA256" ]; then '
        'sudo chown root:root "$DESTINATION" "$IMAGES_DESTINATION"; '
        'sudo chmod 0755 "$DESTINATION"; '
        'sudo chmod 0644 "$IMAGES_DESTINATION"; '
        'if command -v restorecon >/dev/null; then '
        'sudo restorecon -F "$DESTINATION"; '
        'sudo restorecon -RF '
        f'{shlex.quote(K3S_AIRGAP_IMAGES_DIRECTORY)}; fi; exit 0; fi; '
        'fi; fi; '
        'TEMP_DIRECTORY=$(mktemp -d /tmp/deathstarbench-k3s.XXXXXX); '
        'trap \'rm -rf -- "$TEMP_DIRECTORY"\' EXIT; '
        'DOWNLOAD="$TEMP_DIRECTORY/$ASSET"; '
        'IMAGES_DOWNLOAD="$TEMP_DIRECTORY/$IMAGES_ASSET"; '
        'curl --fail --location --silent --show-error '
        '--proto "=https" --tlsv1.2 --retry 3 --retry-connrefused '
        f'--connect-timeout {DOWNLOAD_CONNECT_TIMEOUT_SECONDS} '
        f'--max-time {DOWNLOAD_MAX_TIME_SECONDS} '
        '--output "$DOWNLOAD" "$URL"; '
        'curl --fail --location --silent --show-error '
        '--proto "=https" --tlsv1.2 --retry 3 --retry-connrefused '
        f'--connect-timeout {DOWNLOAD_CONNECT_TIMEOUT_SECONDS} '
        f'--max-time {DOWNLOAD_MAX_TIME_SECONDS} '
        '--output "$IMAGES_DOWNLOAD" "$IMAGES_URL"; '
        'printf "%s  %s\\n" "$EXPECTED_SHA256" "$DOWNLOAD" '
        '| sha256sum -c -; '
        'printf "%s  %s\\n" "$EXPECTED_IMAGES_SHA256" "$IMAGES_DOWNLOAD" '
        '| sha256sum -c -; '
        'sudo install -o root -g root -m 0755 "$DOWNLOAD" "$DESTINATION"; '
        f'sudo install -d -o root -g root -m 0755 '
        f'{shlex.quote(K3S_AIRGAP_IMAGES_DIRECTORY)}; '
        'sudo install -o root -g root -m 0644 '
        '"$IMAGES_DOWNLOAD" "$IMAGES_DESTINATION"; '
        'if command -v restorecon >/dev/null; then '
        'sudo restorecon -F "$DESTINATION"; '
        'sudo restorecon -RF '
        f'{shlex.quote(K3S_AIRGAP_IMAGES_DIRECTORY)}; fi; '
        'sudo install -d -o root -g root -m 0755 '
        '"$(dirname "$RESTART_MARKER")"; '
        'sudo touch "$RESTART_MARKER"; '
        'ACTUAL_SHA256=$(sha256sum "$DESTINATION" | awk "{print \\$1}"); '
        'test "$ACTUAL_SHA256" = "$EXPECTED_SHA256"; '
        'ACTUAL_IMAGES_SHA256=$(sha256sum "$IMAGES_DESTINATION" '
        '| awk "{print \\$1}"); '
        'test "$ACTUAL_IMAGES_SHA256" = "$EXPECTED_IMAGES_SHA256"; '
        'ACTUAL_VERSION=$("$DESTINATION" --version | '
        'awk "NR == 1 {print \\$3}"); '
        'test "$ACTUAL_VERSION" = "$EXPECTED_VERSION"'
    )


def _marker_validation_fragment() -> str:
    return (
        'if sudo test -L "$OWNERSHIP_MARKER" '
        '|| ! sudo test -f "$OWNERSHIP_MARKER"; then '
        'echo "K3s runtime ownership marker is not a regular file." >&2; '
        'exit 1; fi; '
        'test "$(sudo stat -c "%a:%u:%g" "$OWNERSHIP_MARKER")" '
        '= "600:0:0"; '
        'test "$(sudo cat "$OWNERSHIP_MARKER")" = "$RUNTIME_REVISION"; '
    )


def _token_directory_validation_fragment() -> str:
    return (
        'if sudo test -L "$TOKEN_PARENT" '
        '|| ! sudo test -d "$TOKEN_PARENT"; then '
        'echo "K3s ownership parent is missing or not a directory." >&2; '
        'exit 1; fi; '
        'test "$(sudo stat -c "%a:%u:%g" "$TOKEN_PARENT")" = "755:0:0"; '
        'if sudo test -L "$TOKEN_DIRECTORY" '
        '|| ! sudo test -d "$TOKEN_DIRECTORY"; then '
        'echo "K3s token directory is missing or not a directory." >&2; '
        'exit 1; fi; '
        'test "$(sudo stat -c "%a:%u:%g" "$TOKEN_DIRECTORY")" '
        '= "700:0:0"; '
    )


def _token_parent_prepare_fragment() -> str:
    return (
        'if sudo test -e "$TOKEN_PARENT"; then '
        'if sudo test -L "$TOKEN_PARENT" '
        '|| ! sudo test -d "$TOKEN_PARENT"; then '
        'echo "K3s ownership parent is not a trusted directory." >&2; '
        'exit 1; fi; '
        'test "$(sudo stat -c "%a:%u:%g" "$TOKEN_PARENT")" = "755:0:0"; '
        'else sudo install -d -o root -g root -m 0755 "$TOKEN_PARENT"; fi; '
    )


def _token_validation_fragment() -> str:
    return (
        'if sudo test -L "$TOKEN_FILE" '
        '|| ! sudo test -f "$TOKEN_FILE"; then '
        'echo "Owned K3s token file is missing or not regular." >&2; '
        'exit 1; fi; '
        'test "$(sudo stat -c "%a:%u:%g" "$TOKEN_FILE")" = "600:0:0"; '
        'EXISTING_TOKEN=$(sudo cat "$TOKEN_FILE"); '
        'if [ "${#EXISTING_TOKEN}" -lt 20 ] '
        '|| [ "${#EXISTING_TOKEN}" -gt 512 ]; then '
        'unset EXISTING_TOKEN; '
        'echo "Owned K3s token file is malformed." >&2; exit 1; fi; '
        'case "$EXISTING_TOKEN" in *[!A-Za-z0-9:._-]*) '
        'unset EXISTING_TOKEN; '
        'echo "Owned K3s token file is malformed." >&2; exit 1;; esac; '
        'unset EXISTING_TOKEN; '
    )


def _pristine_runtime_check_fragment() -> str:
    state_paths = ' '.join(shlex.quote(path) for path in (
        K3S_SERVER_TOKEN_FILE,
        K3S_AGENT_TOKEN_FILE,
        K3S_OWNERSHIP_MARKER,
        K3S_SERVER_UNIT,
        K3S_AGENT_UNIT,
        K3S_SERVER_STATE_DIRECTORY,
        K3S_AGENT_KUBECONFIG,
        K3S_CONFIG_DIRECTORY,
    ))
    return (
        'if sudo test -e "$TOKEN_DIRECTORY"; then '
        'echo "Unowned K3s token directory already exists." >&2; exit 1; fi; '
        f'for STATE_PATH in {state_paths}; do '
        'if sudo test -e "$STATE_PATH"; then '
        'echo "Unowned K3s runtime state already exists." >&2; exit 1; fi; '
        'done; '
        'if sudo systemctl is-active --quiet k3s.service 2>/dev/null '
        '|| sudo systemctl is-active --quiet k3s-agent.service 2>/dev/null; '
        'then echo "Unowned K3s service is already active." >&2; exit 1; fi; '
    )


def token_install_command(token_kind: str) -> str:
    """Read one scoped K3s token from stdin and install it root-only.

    The caller should pass the token as SSH standard input.  It must not append
    the token to this command, an environment assignment, or a process argument.
    """

    token_files = {
        'server': K3S_SERVER_TOKEN_FILE,
        'agent': K3S_AGENT_TOKEN_FILE,
    }
    try:
        token_file = token_files[token_kind]
    except KeyError:
        raise ValueError('K3s token kind must be server or agent.') from None

    return (
        'set -euo pipefail; '
        f'TOKEN_PARENT={shlex.quote(K3S_OWNERSHIP_DIRECTORY)}; '
        f'TOKEN_DIRECTORY={shlex.quote(K3S_TOKEN_DIRECTORY)}; '
        f'TOKEN_FILE={shlex.quote(token_file)}; '
        f'OWNERSHIP_MARKER={shlex.quote(K3S_OWNERSHIP_MARKER)}; '
        f'RUNTIME_REVISION={shlex.quote(DISTRIBUTED_RUNTIME_REVISION)}; '
        'TEMP_DIRECTORY=$(mktemp -d /tmp/deathstarbench-k3s-token.XXXXXX); '
        'trap \'rm -rf -- "$TEMP_DIRECTORY"\' EXIT; '
        'TEMP_FILE="$TEMP_DIRECTORY/token"; '
        'MARKER_FILE="$TEMP_DIRECTORY/owner"; '
        'umask 077; '
        'TOKEN=$(cat); '
        'if [ "${#TOKEN}" -lt 20 ] || [ "${#TOKEN}" -gt 512 ]; then '
        'echo "Invalid K3s token received on standard input." >&2; exit 1; fi; '
        'case "$TOKEN" in *[!A-Za-z0-9:._-]*) '
        'echo "Invalid K3s token received on standard input." >&2; exit 1;; '
        'esac; '
        'printf "%s\\n" "$TOKEN" > "$TEMP_FILE"; '
        'unset TOKEN; '
        'if sudo test -e "$OWNERSHIP_MARKER"; then '
        + _token_directory_validation_fragment()
        + _marker_validation_fragment()
        + _token_validation_fragment()
        + 'if ! sudo cmp -s "$TEMP_FILE" "$TOKEN_FILE"; then '
        'echo "Refusing to replace an owned K3s token." >&2; exit 1; fi; '
        'else '
        + _pristine_runtime_check_fragment()
        + _token_parent_prepare_fragment()
        + 'sudo install -d -o root -g root -m 0700 "$TOKEN_DIRECTORY"; '
        'sudo install -o root -g root -m 0600 "$TEMP_FILE" "$TOKEN_FILE"; '
        'printf "%s\\n" "$RUNTIME_REVISION" > "$MARKER_FILE"; '
        'sudo install -o root -g root -m 0600 '
        '"$MARKER_FILE" "$OWNERSHIP_MARKER"; fi; '
        + _token_directory_validation_fragment()
        + _marker_validation_fragment()
        + _token_validation_fragment()
    )


def _token_initialize_command(token_kind: str, *, reveal: bool) -> str:
    """Build the atomic control-token initializer and optional scoped read."""

    token_files = {
        'server': K3S_SERVER_TOKEN_FILE,
        'agent': K3S_AGENT_TOKEN_FILE,
    }
    try:
        token_file = token_files[token_kind]
    except KeyError:
        raise ValueError('K3s token kind must be server or agent.') from None
    command = control_tokens_initialize_command()
    if not reveal:
        return command
    return (
        command
        + f'; TOKEN_FILE={shlex.quote(token_file)}; '
        + 'sudo cat "$TOKEN_FILE"'
    )


def control_tokens_initialize_command() -> str:
    """Initialize or verify both control-plane credentials as one contract."""

    return (
        'set -euo pipefail; '
        f'TOKEN_PARENT={shlex.quote(K3S_OWNERSHIP_DIRECTORY)}; '
        f'TOKEN_DIRECTORY={shlex.quote(K3S_TOKEN_DIRECTORY)}; '
        f'SERVER_TOKEN_FILE={shlex.quote(K3S_SERVER_TOKEN_FILE)}; '
        f'AGENT_TOKEN_FILE={shlex.quote(K3S_AGENT_TOKEN_FILE)}; '
        f'OWNERSHIP_MARKER={shlex.quote(K3S_OWNERSHIP_MARKER)}; '
        f'RUNTIME_REVISION={shlex.quote(DISTRIBUTED_RUNTIME_REVISION)}; '
        'if sudo test -e "$OWNERSHIP_MARKER"; then '
        + _token_directory_validation_fragment()
        + _marker_validation_fragment()
        + 'for TOKEN_FILE in "$SERVER_TOKEN_FILE" "$AGENT_TOKEN_FILE"; do '
        + _token_validation_fragment()
        + 'done; '
        'else '
        + _pristine_runtime_check_fragment()
        + _token_parent_prepare_fragment()
        + 'TEMP_DIRECTORY=$(mktemp -d /tmp/deathstarbench-k3s-token.XXXXXX); '
        'trap \'rm -rf -- "$TEMP_DIRECTORY"\' EXIT; umask 077; '
        'SERVER_TEMP="$TEMP_DIRECTORY/server-token"; '
        'AGENT_TEMP="$TEMP_DIRECTORY/agent-token"; '
        'MARKER_FILE="$TEMP_DIRECTORY/owner"; '
        'for TEMP_FILE in "$SERVER_TEMP" "$AGENT_TEMP"; do '
        'od -An -N32 -tx1 /dev/urandom | tr -d " \\n" > "$TEMP_FILE"; '
        'grep -Eq "^[0-9a-f]{64}$" "$TEMP_FILE"; done; '
        'sudo install -d -o root -g root -m 0700 "$TOKEN_DIRECTORY"; '
        'sudo install -o root -g root -m 0600 '
        '"$SERVER_TEMP" "$SERVER_TOKEN_FILE"; '
        'sudo install -o root -g root -m 0600 '
        '"$AGENT_TEMP" "$AGENT_TOKEN_FILE"; '
        'printf "%s\\n" "$RUNTIME_REVISION" > "$MARKER_FILE"; '
        'sudo install -o root -g root -m 0600 '
        '"$MARKER_FILE" "$OWNERSHIP_MARKER"; fi; '
        + _token_directory_validation_fragment()
        + _marker_validation_fragment()
        + 'for TOKEN_FILE in "$SERVER_TOKEN_FILE" "$AGENT_TOKEN_FILE"; do '
        + _token_validation_fragment()
        + 'done'
    )


def token_initialize_command(token_kind: str) -> str:
    """Create or verify one owned control-node token without revealing it."""

    return _token_initialize_command(token_kind, reveal=False)


def token_initialize_and_read_command(token_kind: str) -> str:
    """Create a missing control-node token, then return it through stdout.

    This variant is reserved for tightly scoped recovery tools. Runtime
    bootstrap normally initializes short credentials without revealing them,
    then reads K3s's CA-bound secure agent token after the server is ready.
    Callers must never add the returned secret to events, diagnostics,
    persisted job state, argv, or environment variables.
    """

    return _token_initialize_command(token_kind, reveal=True)


def secure_agent_token_read_command() -> str:
    """Return K3s's CA-bound agent token after attesting its CA prefix."""

    return (
        'set -euo pipefail; '
        'TOKEN=$(sudo cat /var/lib/rancher/k3s/server/agent-token); '
        'CA_SHA256=$(sudo sha256sum '
        '/var/lib/rancher/k3s/server/tls/server-ca.crt | awk "{print \\$1}"); '
        'test ${#CA_SHA256} -eq 64; '
        'case "$CA_SHA256" in *[!0-9a-f]*) exit 1;; esac; '
        'case "$TOKEN" in K10"$CA_SHA256"::* ) ;; '
        '*) echo "K3s did not expose the expected CA-bound agent token." >&2; '
        'exit 1;; esac; '
        'printf "%s\\n" "$TOKEN"'
    )


def server_ca_sha256_read_command() -> str:
    """Return only the non-secret SHA-256 identity of K3s's server CA."""

    return (
        'set -euo pipefail; '
        'CA_SHA256=$(sudo sha256sum '
        '/var/lib/rancher/k3s/server/tls/server-ca.crt | awk "{print \\$1}"); '
        'test ${#CA_SHA256} -eq 64; '
        'case "$CA_SHA256" in *[!0-9a-f]*) exit 1;; esac; '
        'printf "%s\\n" "$CA_SHA256"'
    )


def secure_agent_token_ca_sha256(value: str) -> str:
    """Extract the non-secret server-CA fingerprint from a secure token."""

    token = validated_token(value)
    match = re.fullmatch(r'K10([0-9a-f]{64})::.+', token)
    if match is None:
        raise ValueError('K3s did not return a CA-bound secure agent token.')
    return match.group(1)


def server_token_install_command() -> str:
    return token_install_command('server')


def agent_token_install_command() -> str:
    return token_install_command('agent')


def _service_unit(
    *,
    service_mode: str,
    node_name: str,
    node_private_ip: str,
    role: str,
    selinux_enabled: bool,
    server_private_ip: str | None = None,
) -> str:
    selinux_enabled = _validated_selinux_mode(selinux_enabled)
    token_file = (
        K3S_SERVER_TOKEN_FILE
        if service_mode == 'server'
        else K3S_AGENT_TOKEN_FILE
    )
    executable_arguments = [
        K3S_BINARY,
        service_mode,
        f'--token-file={token_file}',
        f'--node-name={node_name}',
        f'--node-ip={node_private_ip}',
        f'--node-label=deathstarbench.io/role={role}',
    ]
    if selinux_enabled:
        executable_arguments.append('--selinux')
    if service_mode == 'server':
        executable_arguments.extend((
            f'--agent-token-file={K3S_AGENT_TOKEN_FILE}',
            f'--advertise-address={node_private_ip}',
            f'--tls-san={node_private_ip}',
            '--write-kubeconfig-mode=0600',
            f'--cluster-cidr={K3S_CLUSTER_CIDR}',
            f'--service-cidr={K3S_SERVICE_CIDR}',
            f'--service-node-port-range={K3S_SERVICE_NODE_PORT_RANGE}',
            '--flannel-backend=vxlan',
            '--disable=traefik',
            '--disable=servicelb',
            '--disable=metrics-server',
            '--disable=local-storage',
            '--node-taint=node-role.kubernetes.io/control-plane=true:NoSchedule',
        ))
        description = 'DeathStarBench pinned K3s server'
    else:
        if server_private_ip is None:
            raise ValueError('A K3s agent requires the server private address.')
        executable_arguments.append(
            f'--server=https://{server_private_ip}:6443'
        )
        description = f'DeathStarBench pinned K3s {role} agent'

    return '\n'.join((
        '[Unit]',
        f'Description={description}',
        'Documentation=https://docs.k3s.io/',
        'Wants=network-online.target',
        'After=network-online.target',
        f'ConditionPathIsExecutable={K3S_BINARY}',
        f'ConditionPathExists={token_file}',
        *(
            (f'ConditionPathExists={K3S_AGENT_TOKEN_FILE}',)
            if service_mode == 'server'
            else ()
        ),
        '',
        '[Service]',
        'Type=notify',
        'KillMode=control-group',
        'Delegate=yes',
        'LimitNOFILE=1048576',
        'LimitNPROC=infinity',
        'LimitCORE=infinity',
        'TasksMax=infinity',
        'TimeoutStartSec=10min',
        'TimeoutStopSec=2min',
        'SendSIGKILL=yes',
        'Restart=always',
        'RestartSec=5s',
        f'ExecStart={" ".join(executable_arguments)}',
        '',
        '[Install]',
        'WantedBy=multi-user.target',
        '',
    ))


def server_unit(
    node_name: str,
    node_private_ip: str,
    *,
    selinux_enabled: bool,
) -> str:
    """Return the deterministic server systemd unit contents."""

    return _service_unit(
        service_mode='server',
        node_name=_validated_node_name(node_name),
        node_private_ip=_validated_private_ipv4(node_private_ip),
        role='control',
        selinux_enabled=selinux_enabled,
    )


def agent_unit(
    node_name: str,
    node_private_ip: str,
    server_private_ip: str,
    role: str,
    *,
    selinux_enabled: bool,
) -> str:
    """Return one deterministic, role-labelled agent systemd unit."""

    return _service_unit(
        service_mode='agent',
        node_name=_validated_node_name(node_name),
        node_private_ip=_validated_private_ipv4(node_private_ip),
        server_private_ip=_validated_private_ipv4(server_private_ip),
        role=_validated_agent_role(role),
        selinux_enabled=selinux_enabled,
    )


def _unit_install_command(
    unit: str,
    unit_path: str,
    service: str,
    token_files: tuple[str, ...],
    *,
    selinux_enabled: bool,
) -> str:
    payload = base64.b64encode(unit.encode('utf-8')).decode('ascii')
    token_checks = ''.join(
        f'TOKEN_FILE={shlex.quote(token_file)}; '
        + _token_validation_fragment()
        for token_file in token_files
    )
    selinux_checks = (
        'for COMMAND in restorecon matchpathcon; do '
        'command -v "$COMMAND" >/dev/null; done; '
        'sudo restorecon -F "$UNIT_FILE"; '
        'matchpathcon -V "$UNIT_FILE"; '
        if selinux_enabled
        else ''
    )
    return (
        'set -euo pipefail; '
        + f'TOKEN_PARENT={shlex.quote(K3S_OWNERSHIP_DIRECTORY)}; '
        f'TOKEN_DIRECTORY={shlex.quote(K3S_TOKEN_DIRECTORY)}; '
        f'OWNERSHIP_MARKER={shlex.quote(K3S_OWNERSHIP_MARKER)}; '
        f'RESTART_MARKER={shlex.quote(K3S_RESTART_MARKER)}; '
        f'RUNTIME_REVISION={shlex.quote(DISTRIBUTED_RUNTIME_REVISION)}; '
        + _token_directory_validation_fragment()
        + _marker_validation_fragment()
        + token_checks
        + f'UNIT_FILE={shlex.quote(unit_path)}; '
        f'SERVICE={shlex.quote(service)}; '
        'TEMP_FILE=$(mktemp /tmp/deathstarbench-k3s-unit.XXXXXX); '
        'trap \'rm -f -- "$TEMP_FILE"\' EXIT; '
        f'printf "%s" {shlex.quote(payload)} | base64 --decode > "$TEMP_FILE"; '
        'CHANGED=0; '
        'if ! sudo cmp -s "$TEMP_FILE" "$UNIT_FILE"; then '
        'sudo install -o root -g root -m 0644 "$TEMP_FILE" "$UNIT_FILE"; '
        'CHANGED=1; fi; '
        'sudo chown root:root "$UNIT_FILE"; sudo chmod 0644 "$UNIT_FILE"; '
        + selinux_checks
        + 'if sudo test -e "$RESTART_MARKER"; then CHANGED=1; fi; '
        'timeout --signal=TERM 60s sudo systemctl daemon-reload; '
        'timeout --signal=TERM 60s sudo systemctl enable "$SERVICE" >/dev/null; '
        'if sudo systemctl is-active --quiet "$SERVICE"; then '
        'if [ "$CHANGED" -eq 1 ]; then '
        f'timeout --signal=TERM {SERVICE_START_TIMEOUT_SECONDS}s '
        'sudo systemctl restart "$SERVICE"; fi; '
        'else '
        f'timeout --signal=TERM {SERVICE_START_TIMEOUT_SECONDS}s '
        'sudo systemctl start "$SERVICE"; fi; '
        'sudo systemctl is-active --quiet "$SERVICE"; '
        'sudo rm -f -- "$RESTART_MARKER"'
    )


def server_install_command(
    node_name: str,
    node_private_ip: str,
    *,
    selinux_enabled: bool,
) -> str:
    """Install/start the pinned K3s server without an upstream shell installer."""

    return _unit_install_command(
        server_unit(
            node_name,
            node_private_ip,
            selinux_enabled=selinux_enabled,
        ),
        K3S_SERVER_UNIT,
        'k3s.service',
        (K3S_SERVER_TOKEN_FILE, K3S_AGENT_TOKEN_FILE),
        selinux_enabled=selinux_enabled,
    )


def agent_join_command(
    node_name: str,
    node_private_ip: str,
    server_private_ip: str,
    role: str,
    *,
    selinux_enabled: bool,
) -> str:
    """Install/start one agent that reads its join secret from a root-only file."""

    return _unit_install_command(
        agent_unit(
            node_name,
            node_private_ip,
            server_private_ip,
            role,
            selinux_enabled=selinux_enabled,
        ),
        K3S_AGENT_UNIT,
        'k3s-agent.service',
        (K3S_AGENT_TOKEN_FILE,),
        selinux_enabled=selinux_enabled,
    )


def server_readiness_command(architecture: str) -> str:
    """Verify the exact server artifact and wait for the Kubernetes readyz API."""

    inner = (
        f'until sudo {shlex.quote(K3S_BINARY)} kubectl get --raw=/readyz '
        '>/dev/null 2>&1; do sleep 2; done'
    )
    return (
        'set -euo pipefail; '
        + _binary_verification_fragment(architecture)
        + 'sudo systemctl is-active --quiet k3s.service; '
        + f'if ! timeout --signal=TERM {READINESS_TIMEOUT_SECONDS}s '
        + f'bash -c {shlex.quote(inner)}; then '
        + 'sudo systemctl status --no-pager --full k3s.service >&2 || true; '
        + 'exit 1; fi; '
        + f'sudo {shlex.quote(K3S_BINARY)} kubectl get --raw=/readyz '
        + '| grep -Fx ok >/dev/null'
    )


def agent_readiness_command(architecture: str) -> str:
    """Verify an exact agent artifact and wait for its kubelet configuration."""

    inner = (
        'until sudo systemctl is-active --quiet k3s-agent.service '
        '&& sudo test -s /var/lib/rancher/k3s/agent/kubelet.kubeconfig; '
        'do sleep 2; done'
    )
    return (
        'set -euo pipefail; '
        + _binary_verification_fragment(architecture)
        + f'if ! timeout --signal=TERM {READINESS_TIMEOUT_SECONDS}s '
        + f'bash -c {shlex.quote(inner)}; then '
        + 'sudo systemctl status --no-pager --full '
        + 'k3s-agent.service >&2 || true; exit 1; fi; '
        + 'sudo systemctl is-active --quiet k3s-agent.service; '
        + 'sudo test -s /var/lib/rancher/k3s/agent/kubelet.kubeconfig'
    )


def cluster_nodes_readiness_command(
    expected_nodes: Iterable[ExpectedK3sNode],
) -> str:
    """Attest exact membership, identity, role, version, and readiness."""

    nodes = tuple(expected_nodes)
    if (
        not nodes
        or any(not isinstance(node, ExpectedK3sNode) for node in nodes)
    ):
        raise ValueError('Expected K3s nodes must be typed node descriptors.')
    names = tuple(node.name for node in nodes)
    roles = tuple(node.role for node in nodes)
    if len(names) != len(set(names)):
        raise ValueError('Expected K3s node names must be unique.')
    if len(roles) != len(set(roles)) or set(roles) != _CLUSTER_ROLES:
        raise ValueError(
            'Expected K3s nodes must contain exactly one control, '
            'application, cache, and database role.'
        )
    ordered_nodes = tuple(sorted(nodes, key=lambda node: node.name))
    arguments = ' '.join(
        shlex.quote(f'node/{node.name}') for node in ordered_nodes
    )
    inner = (
        f'until sudo {shlex.quote(K3S_BINARY)} kubectl get {arguments} '
        '>/dev/null 2>&1; do sleep 2; done'
    )
    expected_names = '\n'.join(node.name for node in ordered_nodes) + '\n'
    expected_names_payload = base64.b64encode(
        expected_names.encode('ascii')
    ).decode('ascii')
    checks = []
    for node in ordered_nodes:
        prefix = f'NODE={shlex.quote(node.name)}; '
        checks.append(
            prefix
            + 'ACTUAL_ROLE=$(sudo '
            + shlex.quote(K3S_BINARY)
            + ' kubectl get node "$NODE" -o '
            + shlex.quote(
                'jsonpath={.metadata.labels.deathstarbench\\.io/role}'
            )
            + '); '
            + f'test "$ACTUAL_ROLE" = {shlex.quote(node.role)}; '
            + 'ACTUAL_IP=$(sudo '
            + shlex.quote(K3S_BINARY)
            + ' kubectl get node "$NODE" -o '
            + shlex.quote(
                'jsonpath={.status.addresses[?(@.type=="InternalIP")].address}'
            )
            + '); '
            + f'test "$ACTUAL_IP" = {shlex.quote(node.private_ip)}; '
            + 'ACTUAL_ARCH=$(sudo '
            + shlex.quote(K3S_BINARY)
            + ' kubectl get node "$NODE" -o '
            + shlex.quote('jsonpath={.status.nodeInfo.architecture}')
            + '); '
            + 'ACTUAL_VERSION=$(sudo '
            + shlex.quote(K3S_BINARY)
            + ' kubectl get node "$NODE" -o '
            + shlex.quote('jsonpath={.status.nodeInfo.kubeletVersion}')
            + '); '
            + f'test "$ACTUAL_ARCH" = '
            + shlex.quote(node.kubernetes_architecture)
            + '; '
            + f'test "$ACTUAL_VERSION" = {shlex.quote(K3S_VERSION)}; '
        )
        if node.role == 'control':
            checks.append(
                prefix
                + 'CONTROL_LABEL=$(sudo '
                + shlex.quote(K3S_BINARY)
                + ' kubectl get node "$NODE" -o '
                + shlex.quote(
                    'jsonpath={.metadata.labels.node-role\\.kubernetes\\.io/'
                    'control-plane}'
                )
                + '); test "$CONTROL_LABEL" = true; '
                + 'CONTROL_TAINT=$(sudo '
                + shlex.quote(K3S_BINARY)
                + ' kubectl get node "$NODE" -o '
                + shlex.quote(
                    'jsonpath={.spec.taints[?(@.key=='
                    '"node-role.kubernetes.io/control-plane")].effect}'
                )
                + '); test "$CONTROL_TAINT" = NoSchedule; '
            )
    return (
        'set -euo pipefail; '
        f'if ! timeout --signal=TERM {READINESS_TIMEOUT_SECONDS}s '
        f'bash -c {shlex.quote(inner)}; then '
        'echo "Timed out waiting for all K3s nodes to register." >&2; '
        'exit 1; fi; '
        f'sudo {shlex.quote(K3S_BINARY)} kubectl wait '
        '--for=condition=Ready '
        f'--timeout={READINESS_TIMEOUT_SECONDS}s {arguments}; '
        'ACTUAL_NAMES=$(sudo '
        f'{shlex.quote(K3S_BINARY)} kubectl get nodes -o name '
        '| sed "s#^node/##" | LC_ALL=C sort); '
        'EXPECTED_NAMES=$(printf "%s" '
        f'{shlex.quote(expected_names_payload)} | base64 --decode); '
        'test "$ACTUAL_NAMES" = "$EXPECTED_NAMES"; '
        + ''.join(checks)
    )


def _coredns_control_selector_check(control_node_name: str) -> str:
    return (
        'COREDNS_SELECTOR=$(sudo '
        f'{shlex.quote(K3S_BINARY)} kubectl -n kube-system get deployment '
        'coredns -o '
        + shlex.quote(
            'jsonpath={.spec.template.spec.nodeSelector.kubernetes\\.io/'
            'hostname}'
        )
        + '); '
        f'test "$COREDNS_SELECTOR" = {shlex.quote(control_node_name)}; '
    )


def system_services_pin_command(control_node_name: str) -> str:
    """Permanently constrain CoreDNS replicas to the control node."""

    control_node_name = _validated_node_name(control_node_name)
    patch = json.dumps(
        {
            'spec': {
                'template': {
                    'spec': {
                        'nodeSelector': {
                            'kubernetes.io/os': 'linux',
                            'kubernetes.io/hostname': control_node_name,
                        },
                    },
                },
            },
        },
        sort_keys=True,
        separators=(',', ':'),
    )
    patch_payload = base64.b64encode(patch.encode('ascii')).decode('ascii')
    wait_for_coredns = (
        f'until sudo {shlex.quote(K3S_BINARY)} kubectl -n kube-system get '
        'deployment/coredns >/dev/null 2>&1; do sleep 2; done'
    )
    return (
        'set -euo pipefail; '
        f'if ! timeout --signal=TERM {READINESS_TIMEOUT_SECONDS}s '
        f'bash -c {shlex.quote(wait_for_coredns)}; then '
        'echo "Timed out waiting for the CoreDNS deployment to exist." >&2; '
        'exit 1; fi; '
        f'PATCH=$(printf "%s" {shlex.quote(patch_payload)} '
        '| base64 --decode); '
        f'sudo {shlex.quote(K3S_BINARY)} kubectl -n kube-system patch '
        'deployment coredns --type=merge --patch "$PATCH" >/dev/null; '
        f'sudo {shlex.quote(K3S_BINARY)} kubectl -n kube-system rollout '
        'status deployment/coredns '
        f'--timeout={READINESS_TIMEOUT_SECONDS}s >/dev/null; '
        + _coredns_control_selector_check(control_node_name)
    )


def system_services_readiness_command(control_node_name: str) -> str:
    """Attest the minimal K3s system workload stays on the control node."""

    control_node_name = _validated_node_name(control_node_name)
    inner = (
        f'until sudo {shlex.quote(K3S_BINARY)} kubectl -n kube-system '
        'rollout status deployment/coredns --timeout=5s >/dev/null 2>&1; '
        'do sleep 2; done'
    )
    return (
        'set -euo pipefail; '
        f'if ! timeout --signal=TERM {READINESS_TIMEOUT_SECONDS}s '
        f'bash -c {shlex.quote(inner)}; then '
        'echo "Timed out waiting for the pinned CoreDNS deployment." >&2; '
        'exit 1; fi; '
        + _coredns_control_selector_check(control_node_name)
        + 'COREDNS_SERVICE_IP=$(sudo '
        f'{shlex.quote(K3S_BINARY)} kubectl -n kube-system get service '
        'kube-dns -o '
        + shlex.quote('jsonpath={.spec.clusterIP}')
        + '); '
        f'if [ "$COREDNS_SERVICE_IP" != {shlex.quote(K3S_CLUSTER_DNS_IP)} ]; '
        'then echo "The CoreDNS service IP drifted from the workload resolver '
        'contract." >&2; exit 1; fi; '
        + 'COREDNS_NODES=$(sudo '
        f'{shlex.quote(K3S_BINARY)} kubectl -n kube-system get pods '
        '-l k8s-app=kube-dns -o '
        + shlex.quote(
            'jsonpath={range .items[*]}{.spec.nodeName}{"\\n"}{end}'
        )
        + ' | sed "/^$/d" | LC_ALL=C sort -u); '
        f'test "$COREDNS_NODES" = {shlex.quote(control_node_name)}; '
        'if sudo '
        f'{shlex.quote(K3S_BINARY)} kubectl -n kube-system get deployment '
        'metrics-server >/dev/null 2>&1; then '
        'echo "The disabled metrics-server deployment is present." >&2; '
        'exit 1; fi; '
        'if sudo '
        f'{shlex.quote(K3S_BINARY)} kubectl -n kube-system get deployment '
        'local-path-provisioner >/dev/null 2>&1; then '
        'echo "The disabled local-path provisioner is present." >&2; '
        'exit 1; fi'
    )


def uninstall_command() -> str:
    """Build bounded cleanup for only this ephemeral K3s guest runtime."""

    return (
        'set -euo pipefail; '
        f'OWNERSHIP_MARKER={shlex.quote(K3S_OWNERSHIP_MARKER)}; '
        f'RUNTIME_REVISION={shlex.quote(DISTRIBUTED_RUNTIME_REVISION)}; '
        'test "$(sudo cat "$OWNERSHIP_MARKER")" = "$RUNTIME_REVISION" || '
        '{ echo "Refusing to remove an unowned K3s runtime." >&2; exit 1; }; '
        'for SERVICE in k3s-agent.service k3s.service; do '
        'if sudo systemctl list-unit-files "$SERVICE" --no-legend '
        '| grep -q "^$SERVICE"; then '
        f'if ! timeout --signal=TERM 60s sudo systemctl disable --now '
        '"$SERVICE" >/dev/null 2>&1; then '
        'sudo systemctl kill --kill-who=all --signal=KILL "$SERVICE" '
        '>/dev/null 2>&1 || true; '
        'timeout --signal=TERM 20s bash -c '
        '\'until ! sudo systemctl is-active --quiet "$1"; do sleep 1; done\' '
        'bash "$SERVICE"; fi; fi; done; '
        'MOUNTS=$(findmnt -rn -o TARGET | awk '
        '\'/^\\/run\\/k3s(\\/|$)|^\\/var\\/lib\\/kubelet(\\/|$)|'
        '^\\/var\\/lib\\/rancher\\/k3s(\\/|$)/ {print}\' | sort -r); '
        'if [ -n "$MOUNTS" ]; then while IFS= read -r TARGET; do '
        'if ! timeout --signal=TERM 20s sudo umount "$TARGET" '
        '>/dev/null 2>&1; then '
        'timeout --signal=TERM 20s sudo umount -l "$TARGET" '
        '>/dev/null 2>&1; fi; '
        'done <<< "$MOUNTS"; fi; '
        'REMAINING_MOUNTS=$(findmnt -rn -o TARGET | awk '
        '\'/^\\/run\\/k3s(\\/|$)|^\\/var\\/lib\\/kubelet(\\/|$)|'
        '^\\/var\\/lib\\/rancher\\/k3s(\\/|$)/ {print}\'); '
        'test -z "$REMAINING_MOUNTS"; '
        'sudo ip link delete cni0 >/dev/null 2>&1 || true; '
        'sudo ip link delete flannel.1 >/dev/null 2>&1 || true; '
        f'sudo rm -f -- {shlex.quote(K3S_SERVER_UNIT)} '
        f'{shlex.quote(K3S_AGENT_UNIT)} {shlex.quote(K3S_BINARY)}; '
        'sudo rm -rf -- /etc/rancher/k3s /var/lib/rancher/k3s '
        '/var/lib/kubelet /var/lib/cni /etc/cni/net.d /run/k3s '
        f'{shlex.quote(K3S_TOKEN_DIRECTORY)}; '
        f'sudo rm -f -- {shlex.quote(K3S_RESTART_MARKER)}; '
        'timeout --signal=TERM 60s sudo systemctl daemon-reload; '
        'sudo systemctl reset-failed k3s.service k3s-agent.service '
        '>/dev/null 2>&1 || true'
    )
