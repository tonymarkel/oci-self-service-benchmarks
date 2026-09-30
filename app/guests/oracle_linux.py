"""Oracle Linux 9 guest contract for distributed DeathStarBench on OCI.

The module performs no OCI API calls.  Cloud-side validation binds one exact
block-volume OCID to the fixed ``/dev/oracleoci/oraclevdb`` attachment before
these commands execute.  Guest commands never enumerate disks or fall back to
kernel-assigned ``/dev`` names.
"""

from dataclasses import dataclass
import re
import shlex
from typing import Iterable


SSH_USER = 'opc'
OCI_VCN_RESOLVER = '169.254.169.254'
DATABASE_DEVICE_LINK = '/dev/oracleoci/oraclevdb'
DATABASE_MOUNT_POINT = '/var/lib/deathstarbench/database'

DEFAULT_READINESS_HOSTS = (
    'yum.oracle.com',
    'github.com',
    'luarocks.org',
)

DEATHSTARBENCH_LOADGEN_BASE_PACKAGES = frozenset({
    'gcc',
    'git',
    'make',
    'openssl-devel',
    'python3',
    'time',
    'zlib-devel',
})
DEATHSTARBENCH_LOADGEN_EPEL_PACKAGES = frozenset({
    'luarocks',
    'python3-aiohttp',
})

_PACKAGE_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9+_.:-]*$')
_HOST_RE = re.compile(
    r'^(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}'
    r'[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}'
    r'[A-Za-z0-9])?$'
)
_OCI_VOLUME_OCID_RE = re.compile(r'^ocid1\.volume\.[A-Za-z0-9._-]+$')
_FILESYSTEM_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
)


@dataclass(frozen=True)
class InstallStep:
    """One observable, independently retryable OL9 installation step."""

    name: str
    command: str
    timeout_seconds: int = 1800


def normalize_architecture(value: str) -> str:
    """Normalize OCI and Linux architecture spellings."""

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
    except KeyError:
        raise ValueError(
            f'Oracle Linux 9 benchmark architecture is unsupported: {value}'
        ) from None


def oci_dns_prepare_command(
    required_hosts: Iterable[str] = DEFAULT_READINESS_HOSTS,
) -> str:
    """Reapply and prove OCI's DHCP-provided VCN resolver configuration."""

    hosts = tuple(dict.fromkeys(str(host).strip() for host in required_hosts))
    if not hosts:
        raise ValueError('At least one readiness hostname is required.')
    invalid = [host for host in hosts if not _HOST_RE.fullmatch(host)]
    if invalid:
        raise ValueError(f'Invalid readiness hostname: {invalid[0]}')
    host_list = ' '.join(shlex.quote(host) for host in hosts)
    return (
        'set -euo pipefail; '
        'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin; '
        'export PATH; '
        'for COMMAND in getent grep nmcli timeout; do '
        'if ! command -v "$COMMAND" >/dev/null 2>&1; then '
        'echo "Required OCI DNS command is unavailable: $COMMAND" >&2; '
        'exit 1; fi; done; '
        f'EXPECTED_OCI_RESOLVER={shlex.quote(OCI_VCN_RESOLVER)}; '
        'if ! nmcli -g IP4.DNS device show '
        '| grep -Fxq "$EXPECTED_OCI_RESOLVER"; then '
        'echo "OCI DHCP did not advertise the expected VCN resolver '
        '$EXPECTED_OCI_RESOLVER." >&2; exit 1; fi; '
        f'REQUIRED_HOSTS={shlex.quote(host_list)}; DNS_READY=false; '
        'MISSING_HOSTS=""; '
        'for attempt in $(seq 1 24); do '
        'sudo nmcli general reload dns-full; '
        "if ! grep -Eq '^nameserver[[:space:]]+169\\.254\\.169\\.254"
        "([[:space:]]|$)' /etc/resolv.conf; then "
        'echo "Waiting for NetworkManager to publish the OCI VCN resolver '
        '($attempt/24)." >&2; '
        'else MISSING_HOSTS=""; for host in $REQUIRED_HOSTS; do '
        'timeout 5 getent ahostsv4 "$host" >/dev/null 2>&1 '
        '|| MISSING_HOSTS="$MISSING_HOSTS $host"; done; '
        'if [ -z "$MISSING_HOSTS" ]; then DNS_READY=true; break; fi; '
        'echo "Waiting for DNS:$MISSING_HOSTS ($attempt/24)." >&2; fi; '
        'if [ "$attempt" -lt 24 ]; then sleep 5; fi; done; '
        'if [ "$DNS_READY" != true ]; then '
        'echo "DNS did not resolve required Oracle Linux benchmark hosts: '
        '$MISSING_HOSTS" >&2; cat /etc/resolv.conf >&2; '
        'grep "^hosts:" /etc/nsswitch.conf >&2 || true; '
        'ip route >&2 || true; exit 1; fi; '
        'echo "OCI VCN DNS resolver is ready."'
    )


def readiness_command(
    expected_architecture: str,
    required_hosts: Iterable[str] = DEFAULT_READINESS_HOSTS,
) -> str:
    """Verify OL9, architecture, DNS, stock curl, and bounded DNF readiness."""

    architecture = normalize_architecture(expected_architecture)
    dns_prepare = oci_dns_prepare_command(required_hosts)
    return (
        f'{dns_prepare}; '
        'test -r /etc/os-release; . /etc/os-release; '
        'if [ "$ID" != ol ] || [ "${VERSION_ID%%.*}" != 9 ]; then '
        'echo "The OCI benchmark guest must be Oracle Linux 9." >&2; '
        'exit 1; fi; '
        f'EXPECTED_ARCH={shlex.quote(architecture)}; ACTUAL_ARCH=$(uname -m); '
        'if [ "$ACTUAL_ARCH" != "$EXPECTED_ARCH" ]; then '
        'echo "Oracle Linux architecture mismatch: expected $EXPECTED_ARCH, '
        'got $ACTUAL_ARCH." >&2; exit 1; fi; '
        'DNF_READY=false; for attempt in $(seq 1 3); do '
        'if sudo timeout 60 dnf -q --disablerepo=ol9_ksplice '
        '--setopt=retries=5 --setopt=timeout=20 makecache --refresh; then '
        'DNF_READY=true; break; fi; '
        'echo "Waiting for the Oracle Linux package repositories '
        '($attempt/3)."; '
        'if [ "$attempt" -lt 3 ]; then sleep 10; fi; done; '
        'if [ "$DNF_READY" != true ]; then '
        'echo "The Oracle Linux package repositories were not ready after '
        'three attempts." >&2; exit 1; fi; '
        'if ! command -v curl >/dev/null 2>&1; then '
        'echo "The stock Oracle Linux curl command is unavailable." >&2; '
        'exit 1; fi; curl --version | head -n 1; '
        'echo "Oracle Linux 9 DNS and package repositories are ready."'
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
    """Install OL9 packages with bounded retries and no Ksplice dependency."""

    package_list = ' '.join(_validated_packages(packages))
    return (
        'set -euo pipefail; INSTALLED=false; '
        'for attempt in $(seq 1 3); do '
        'if sudo timeout 480 dnf -y --disablerepo=ol9_ksplice '
        f'--setopt=retries=10 --setopt=timeout=30 install {package_list}; then '
        'INSTALLED=true; break; fi; '
        'echo "Oracle Linux DNF installation failed; refreshing metadata '
        'before retry ($attempt/3)." >&2; '
        'sudo dnf clean expire-cache >/dev/null 2>&1 || true; '
        'if [ "$attempt" -lt 3 ]; then sleep 10; fi; done; '
        'test "$INSTALLED" = true'
    )


def developer_epel_enable_command() -> str:
    """Enable and prove the exact Oracle Linux 9 developer EPEL repository."""

    return (
        'set -euo pipefail; '
        'sudo dnf config-manager --enable ol9_developer_EPEL; '
        'sudo dnf repolist --enabled '
        '| awk \'$1 == "ol9_developer_EPEL" { found=1 } '
        'END { exit found ? 0 : 1 }\''
    )


def deathstarbench_loadgen_install_steps() -> tuple[InstallStep, ...]:
    """Return the fail-closed OL9 package sequence for the x86 load driver."""

    return (
        InstallStep(
            'DeathStarBench load-generator base packages',
            dnf_install_command(DEATHSTARBENCH_LOADGEN_BASE_PACKAGES),
        ),
        InstallStep(
            'Oracle Linux developer EPEL repository packages',
            dnf_install_command({'dnf-plugins-core', 'oracle-epel-release-el9'}),
        ),
        InstallStep(
            'Oracle Linux developer EPEL repository',
            developer_epel_enable_command(),
        ),
        InstallStep(
            'DeathStarBench load-generator EPEL packages',
            dnf_install_command(DEATHSTARBENCH_LOADGEN_EPEL_PACKAGES),
        ),
    )


def _validated_volume_id(volume_id: str) -> str:
    normalized = str(volume_id).strip()
    if not _OCI_VOLUME_OCID_RE.fullmatch(normalized):
        raise ValueError(
            'The OCI DeathStarBench database block-volume OCID is invalid.'
        )
    return normalized


def _validated_filesystem_uuid(filesystem_uuid: str) -> str:
    normalized = str(filesystem_uuid).strip().lower()
    if not _FILESYSTEM_UUID_RE.fullmatch(normalized):
        raise ValueError(
            'DeathStarBench database attestation requires an exact filesystem UUID.'
        )
    return normalized


def oci_deathstarbench_database_volume_mount_command(
    volume_id: str,
) -> str:
    """Mount only the cloud-bound OCI database volume at its fixed link."""

    normalized_id = _validated_volume_id(volume_id)
    return (
        'set -euo pipefail; '
        f'VOLUME_ID={shlex.quote(normalized_id)}; '
        f'DEVICE_LINK={shlex.quote(DATABASE_DEVICE_LINK)}; '
        f'MOUNT_POINT={shlex.quote(DATABASE_MOUNT_POINT)}; '
        'DEVICE_READY=false; for attempt in $(seq 1 60); do '
        'if [ -L "$DEVICE_LINK" ] && [ -b "$DEVICE_LINK" ]; then '
        'DEVICE_READY=true; break; fi; '
        'echo "Waiting for exact OCI DeathStarBench database device '
        '$DEVICE_LINK ($attempt/60)."; '
        'if [ "$attempt" -lt 60 ]; then sleep 2; fi; done; '
        'if [ "$DEVICE_READY" != true ]; then '
        'echo "The exact OCI DeathStarBench database device did not appear: '
        '$DEVICE_LINK" >&2; exit 1; fi; '
        'DEVICE=$(readlink -f "$DEVICE_LINK"); '
        'if [ ! -b "$DEVICE" ]; then '
        'echo "The OCI database device link does not resolve to a block '
        'device." >&2; exit 1; fi; '
        'if [ "$(lsblk -dnro TYPE "$DEVICE")" != disk ]; then '
        'echo "The OCI DeathStarBench database attachment is not a whole '
        'disk." >&2; exit 1; fi; '
        'ROOT_SOURCE=$(sudo findmnt -rn -o SOURCE --mountpoint /); '
        'ROOT_DEVICE=$(readlink -f "$ROOT_SOURCE"); '
        'if [ ! -b "$ROOT_DEVICE" ]; then '
        'echo "The OCI guest root filesystem did not resolve to a block '
        'device." >&2; exit 1; fi; '
        'ROOT_ANCESTRY=$(lsblk -srnpo NAME "$ROOT_DEVICE"); '
        'test -n "$ROOT_ANCESTRY"; '
        'if printf "%s\\n" "$ROOT_ANCESTRY" | grep -Fxq "$DEVICE"; then '
        'echo "Refusing to format or mount the OCI boot disk as the '
        'DeathStarBench database volume." >&2; exit 1; fi; '
        'DEVICE_TREE=$(lsblk -nrpo NAME "$DEVICE"); test -n "$DEVICE_TREE"; '
        'CHILD_COUNT=$(printf "%s\\n" "$DEVICE_TREE" | tail -n +2 '
        '| awk \'NF { count++ } END { print count + 0 }\'); '
        'if [ "$CHILD_COUNT" -ne 0 ]; then '
        'echo "The OCI DeathStarBench database volume already has partitions '
        'or child devices; refusing to format it." >&2; exit 1; fi; '
        'if [ -L "$MOUNT_POINT" ] '
        '|| { [ -e "$MOUNT_POINT" ] && [ ! -d "$MOUNT_POINT" ]; }; then '
        'echo "The DeathStarBench database mount point is not a trusted '
        'directory." >&2; exit 1; fi; '
        'sudo install -d -o root -g root -m 0755 "$MOUNT_POINT"; '
        'if [ "$(readlink -f "$MOUNT_POINT")" != "$MOUNT_POINT" ]; then '
        'echo "The DeathStarBench database mount point resolves outside its '
        'fixed path." >&2; exit 1; fi; '
        'DEVICE_MOUNTS=$(lsblk -dnro MOUNTPOINTS "$DEVICE" '
        '| sed \'/^[[:space:]]*$/d\'); '
        'if [ -n "$DEVICE_MOUNTS" ] '
        '&& [ "$DEVICE_MOUNTS" != "$MOUNT_POINT" ]; then '
        'echo "The OCI DeathStarBench database volume is already mounted at '
        'an unexpected path." >&2; exit 1; fi; '
        'FSTYPE=$(sudo blkid -s TYPE -o value "$DEVICE" 2>/dev/null || true); '
        'if [ -z "$FSTYPE" ]; then '
        'SIGNATURES=$(sudo wipefs -n --noheadings --output TYPE "$DEVICE" '
        '| awk \'NF { print }\'); '
        'if [ -n "$SIGNATURES" ]; then '
        'echo "Refusing to format the OCI volume because it contains '
        'unrecognized storage signatures." >&2; exit 1; fi; '
        'sudo mkfs.xfs "$DEVICE"; '
        'elif [ "$FSTYPE" != xfs ]; then '
        'echo "Refusing to replace unexpected $FSTYPE filesystem on the OCI '
        'DeathStarBench database volume." >&2; exit 1; fi; '
        'UUID=$(sudo blkid -s UUID -o value "$DEVICE" '
        '| tr "[:upper:]" "[:lower:]"); '
        'if [ -z "$UUID" ]; then '
        'echo "The OCI DeathStarBench database volume has no filesystem UUID." '
        '>&2; exit 1; fi; '
        'MOUNTED_SOURCE=$(sudo findmnt -rn -o SOURCE '
        '--mountpoint "$MOUNT_POINT" 2>/dev/null || true); '
        'MOUNTED_UUID=$(sudo findmnt -rn -o UUID '
        '--mountpoint "$MOUNT_POINT" 2>/dev/null '
        '| tr "[:upper:]" "[:lower:]" || true); '
        'if [ -n "$MOUNTED_SOURCE" ] '
        '&& { [ -z "$MOUNTED_UUID" ] || [ "$MOUNTED_UUID" != "$UUID" ]; }; '
        'then echo "A different filesystem is already mounted at '
        '/var/lib/deathstarbench/database." >&2; exit 1; fi; '
        'if sudo awk -v mount="$MOUNT_POINT" '
        "'$0 !~ /^[[:space:]]*#/ && NF >= 2 && $2 == mount "
        "&& $0 !~ /# cloud-benchmark-dsb-database$/ { found=1 } "
        "END { exit found ? 0 : 1 }' /etc/fstab; then "
        'echo "Refusing to replace a non-benchmark DeathStarBench database '
        'fstab entry." >&2; exit 1; fi; '
        'FSTAB_TMP=$(mktemp); '
        "sudo awk '$0 !~ /# cloud-benchmark-dsb-database$/' /etc/fstab "
        '>"$FSTAB_TMP"; '
        'printf "UUID=%s /var/lib/deathstarbench/database xfs '
        'discard,nofail 0 2 # cloud-benchmark-dsb-database\\n" '
        '"$UUID" >>"$FSTAB_TMP"; '
        'sudo install -m 0644 "$FSTAB_TMP" /etc/fstab; rm -f "$FSTAB_TMP"; '
        'if [ -z "$MOUNTED_UUID" ]; then sudo mount "$MOUNT_POINT"; fi; '
        'MOUNTED_SOURCE=$(sudo findmnt -rn -o SOURCE '
        '--mountpoint "$MOUNT_POINT"); '
        'MOUNTED_DEVICE=$(readlink -f "$MOUNTED_SOURCE"); '
        'VERIFY_UUID=$(sudo findmnt -rn -o UUID --mountpoint "$MOUNT_POINT" '
        '| tr "[:upper:]" "[:lower:]"); '
        'VERIFY_TYPE=$(sudo findmnt -rn -o FSTYPE --mountpoint "$MOUNT_POINT"); '
        'test "$MOUNTED_DEVICE" = "$DEVICE"; test "$VERIFY_UUID" = "$UUID"; '
        'test "$VERIFY_TYPE" = xfs; '
        'DEVICE_MOUNTS=$(lsblk -dnro MOUNTPOINTS "$DEVICE" '
        '| sed \'/^[[:space:]]*$/d\'); '
        'test "$DEVICE_MOUNTS" = "$MOUNT_POINT"; '
        'sudo chown root:root "$MOUNT_POINT"; sudo chmod 0755 "$MOUNT_POINT"; '
        'if command -v restorecon >/dev/null 2>&1; then '
        'sudo restorecon -F "$MOUNT_POINT"; fi; '
        'printf "OCI_DSB_DATABASE_VOLUME volume_id=%s device_link=%s uuid=%s '
        'mount_point=/var/lib/deathstarbench/database filesystem=xfs\\n" '
        '"$VOLUME_ID" "$DEVICE_LINK" "$UUID"'
    )


def oci_deathstarbench_database_volume_attestation_command(
    volume_id: str,
    filesystem_uuid: str,
) -> str:
    """Re-attest the exact OCI database filesystem without mutating it."""

    normalized_id = _validated_volume_id(volume_id)
    normalized_uuid = _validated_filesystem_uuid(filesystem_uuid)
    return (
        'set -euo pipefail; '
        f'VOLUME_ID={shlex.quote(normalized_id)}; '
        f'DEVICE_LINK={shlex.quote(DATABASE_DEVICE_LINK)}; '
        f'MOUNT_POINT={shlex.quote(DATABASE_MOUNT_POINT)}; '
        f'EXPECTED_UUID={shlex.quote(normalized_uuid)}; '
        'for attempt in $(seq 1 60); do '
        'if [ -L "$DEVICE_LINK" ] && [ -b "$DEVICE_LINK" ]; then break; fi; '
        'if [ "$attempt" -lt 60 ]; then sleep 2; fi; done; '
        'if [ ! -L "$DEVICE_LINK" ] || [ ! -b "$DEVICE_LINK" ]; then '
        'echo "The OCI DeathStarBench database device is unavailable for '
        'read-only attestation: $DEVICE_LINK" >&2; exit 1; fi; '
        'DEVICE=$(readlink -f "$DEVICE_LINK"); test -b "$DEVICE"; '
        'test "$(lsblk -dnro TYPE "$DEVICE")" = disk; '
        'DEVICE_TREE=$(lsblk -nrpo NAME "$DEVICE"); test -n "$DEVICE_TREE"; '
        'test "$(printf "%s\\n" "$DEVICE_TREE" | tail -n +2 '
        '| awk \'NF { count++ } END { print count + 0 }\')" -eq 0; '
        'ROOT_SOURCE=$(sudo findmnt -rn -o SOURCE --mountpoint /); '
        'ROOT_DEVICE=$(readlink -f "$ROOT_SOURCE"); test -b "$ROOT_DEVICE"; '
        'ROOT_ANCESTRY=$(lsblk -srnpo NAME "$ROOT_DEVICE"); '
        'test -n "$ROOT_ANCESTRY"; '
        'if printf "%s\\n" "$ROOT_ANCESTRY" | grep -Fxq "$DEVICE"; then '
        'echo "The OCI DeathStarBench database volume resolves into the '
        'root-device ancestry." >&2; exit 1; fi; '
        'test -d "$MOUNT_POINT"; test ! -L "$MOUNT_POINT"; '
        'test "$(readlink -f "$MOUNT_POINT")" = "$MOUNT_POINT"; '
        'DEVICE_TYPE=$(sudo blkid -s TYPE -o value "$DEVICE" 2>/dev/null); '
        'test "$DEVICE_TYPE" = xfs; '
        'DEVICE_UUID=$(sudo blkid -s UUID -o value "$DEVICE" '
        '| tr "[:upper:]" "[:lower:]"); '
        'if [ "$DEVICE_UUID" != "$EXPECTED_UUID" ]; then '
        'echo "The OCI DeathStarBench database volume UUID changed." >&2; '
        'exit 1; fi; '
        'MOUNTED_SOURCE=$(sudo findmnt -rn -o SOURCE '
        '--mountpoint "$MOUNT_POINT"); '
        'MOUNTED_DEVICE=$(readlink -f "$MOUNTED_SOURCE"); '
        'test -b "$MOUNTED_DEVICE"; test "$MOUNTED_DEVICE" = "$DEVICE"; '
        'MOUNTED_UUID=$(sudo findmnt -rn -o UUID '
        '--mountpoint "$MOUNT_POINT" | tr "[:upper:]" "[:lower:]"); '
        'MOUNTED_TYPE=$(sudo findmnt -rn -o FSTYPE '
        '--mountpoint "$MOUNT_POINT"); '
        'test "$MOUNTED_UUID" = "$EXPECTED_UUID"; test "$MOUNTED_TYPE" = xfs; '
        'DEVICE_MOUNTS=$(lsblk -dnro MOUNTPOINTS "$DEVICE" '
        '| sed \'/^[[:space:]]*$/d\'); '
        'test "$DEVICE_MOUNTS" = "$MOUNT_POINT"; '
        'EXPECTED_FSTAB="UUID=$EXPECTED_UUID '
        '/var/lib/deathstarbench/database xfs discard,nofail 0 2 '
        '# cloud-benchmark-dsb-database"; '
        'grep -Fxq "$EXPECTED_FSTAB" /etc/fstab; '
        'printf "OCI_DSB_DATABASE_VOLUME volume_id=%s device_link=%s uuid=%s '
        'mount_point=/var/lib/deathstarbench/database filesystem=xfs\\n" '
        '"$VOLUME_ID" "$DEVICE_LINK" "$EXPECTED_UUID"'
    )


def oci_deathstarbench_database_workload_storage_command(
    filesystem_uuid: str,
) -> str:
    """Prepare provider-neutral MongoDB roots on an attested OCI volume."""

    from .rocky_linux import (
        azure_deathstarbench_database_workload_storage_command,
    )

    return azure_deathstarbench_database_workload_storage_command(
        filesystem_uuid
    ).replace('AZURE_DSB_WORKLOAD_STORAGE', 'OCI_DSB_WORKLOAD_STORAGE')
