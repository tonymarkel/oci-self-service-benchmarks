"""Pure guest-command helpers for verified benchmark storage targets.

Local NVMe is ephemeral and provider-specific.  These helpers deliberately
separate its preparation output from benchmark output, and require both a
control-plane assertion and a provider-specific guest identity before a disk
can be formatted.  A safe fallback marker means the caller should use its
already provisioned, manifest-bound additional volume instead.
"""

from __future__ import annotations

import base64
import binascii
import math
from pathlib import PurePosixPath
import re
import shlex


LOCAL_NVME_MOUNT_POINT = '/benchmark-local'
STORAGE_TARGET_CONTRACT = 'v1'
STORAGE_TARGET_POLICY = (
    'prefer_verified_instance_local_nvme_else_additional_volume_v1'
)
LOCAL_NVME_VERIFICATION = 'provider_attested_guest_verified_v1'
ADDITIONAL_VOLUME_VERIFICATION = 'manifest_bound_guest_verified_v1'
MINIMUM_LOCAL_NVME_BYTES = 8 * 1024**3

_PROVIDERS = frozenset({'aws', 'oci', 'azure', 'gcp'})
_SUCCESS_MARKER = 'OCI_BENCH_STORAGE_TARGET_V1'
_FALLBACK_MARKER = 'OCI_BENCH_STORAGE_TARGET_FALLBACK_V1'
_MOUNTED_MARKER = 'OCI_BENCH_MOUNTED_STORAGE_TARGET_V1'
_REASON_RE = re.compile(r'^[a-z0-9_]+$')
_MODEL_RE = re.compile(r'[^A-Za-z0-9 ._()+:/-]+')


def _provider(value: str) -> str:
    provider = str(value or '').strip().lower()
    if provider not in _PROVIDERS:
        raise ValueError(
            'Local-NVMe storage provider must be aws, oci, azure, or gcp.'
        )
    return provider


def _expected_count(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(
            'Expected local-NVMe device count must be a non-negative integer.'
        )
    return value


def _expected_total_size_gb(value: int | float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError('Expected local-NVMe capacity must be positive.')
    try:
        size = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            'Expected local-NVMe capacity must be positive.'
        ) from exc
    if not math.isfinite(size) or size <= 0:
        raise ValueError('Expected local-NVMe capacity must be positive.')
    return size


def _mount_point(value: str) -> str:
    raw = str(value or '')
    mount_point = raw.strip()
    path = PurePosixPath(mount_point)
    if (
        raw != mount_point
        or not mount_point.startswith('/')
        or mount_point.startswith('//')
        or '//' in mount_point
        or mount_point == '/'
        or path.as_posix() != mount_point
        or '..' in path.parts
        or not re.fullmatch(r'/[A-Za-z0-9._/-]+', mount_point)
    ):
        raise ValueError(
            'Storage target mount point must be a normalized absolute path.'
        )
    return mount_point


def _provider_discovery(provider: str) -> str:
    if provider == 'aws':
        return (
            'for LINK in /dev/nvme*n1; do '
            '[ -b "$LINK" ] || continue; '
            'MODEL=$(lsblk -dno MODEL "$LINK" 2>/dev/null '
            "| sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'); "
            '[ "$MODEL" = "Amazon EC2 NVMe Instance Storage" ] '
            '|| continue; add_candidate "$LINK"; done; '
        )
    if provider == 'oci':
        # OCI documents local DenseIO devices as nvme namespaces.  Name-only
        # discovery is permitted here solely because EXPECTED_COUNT is a
        # positive shape assertion supplied by the control plane.
        return (
            'for LINK in /dev/nvme*n1; do '
            '[ -b "$LINK" ] || continue; add_candidate "$LINK"; done; '
        )
    if provider == 'azure':
        return (
            'for LINK in /dev/disk/azure/local/by-serial/* '
            '/dev/disk/azure/local/by-index/*; do '
            '[ -L "$LINK" ] || continue; add_candidate "$LINK"; done; '
        )
    return (
        'for LINK in /dev/disk/by-id/google-local-nvme-ssd-*; do '
        '[ -L "$LINK" ] || continue; BASE=${LINK##*/}; '
        '[[ "$BASE" =~ ^google-local-nvme-ssd-[0-9]+$ ]] || continue; '
        'add_candidate "$LINK"; done; '
    )


def local_nvme_prepare_command(
    provider: str,
    expected_count: int,
    expected_total_size_gb: int | float | None = None,
    mount_point: str = LOCAL_NVME_MOUNT_POINT,
) -> str:
    """Build a fail-closed command that prepares one verified local NVMe.

    ``expected_count`` and, when available, ``expected_total_size_gb`` must
    come from provider control-plane metadata.  Zero devices produces a safe
    fallback marker without inspecting or modifying guest disks.
    """

    provider = _provider(provider)
    expected_count = _expected_count(expected_count)
    expected_size_gb = _expected_total_size_gb(expected_total_size_gb)
    mount_point = _mount_point(mount_point)
    mount_literal = shlex.quote(mount_point)
    discovery = _provider_discovery(provider)

    capacity_check = ''
    if expected_size_gb is not None:
        decimal_bytes = round(expected_size_gb * 1_000_000_000)
        binary_bytes = round(expected_size_gb * 1024**3)
        tolerance = max(
            1024**3,
            round(min(decimal_bytes, binary_bytes) * 0.05),
        )
        decimal_low = max(1, decimal_bytes - tolerance)
        decimal_high = decimal_bytes + tolerance
        binary_low = max(1, binary_bytes - tolerance)
        binary_high = binary_bytes + tolerance
        capacity_check = (
            f'if ! {{ [ "$TOTAL_BYTES" -ge {decimal_low} ] '
            f'&& [ "$TOTAL_BYTES" -le {decimal_high} ]; }} '
            f'&& ! {{ [ "$TOTAL_BYTES" -ge {binary_low} ] '
            f'&& [ "$TOTAL_BYTES" -le {binary_high} ]; }}; then '
            'fallback capacity_mismatch; fi; '
        )

    return (
        'set -euo pipefail; '
        f'PROVIDER={provider}; EXPECTED_COUNT={expected_count}; '
        f'MOUNT_POINT={mount_literal}; '
        f'MINIMUM_BYTES={MINIMUM_LOCAL_NVME_BYTES}; '
        'fallback() { printf "OCI_BENCH_STORAGE_TARGET_FALLBACK_V1 '
        'provider=%s reason=%s\\n" "$PROVIDER" "$1"; exit 0; }; '
        '[ "$EXPECTED_COUNT" -gt 0 ] || fallback no_control_plane_local_nvme; '
        'for TOOL in awk base64 find findmnt grep head lsblk mkfs.xfs mount '
        'readlink sed sudo swapon tr wipefs; do '
        'command -v "$TOOL" >/dev/null 2>&1 '
        '|| fallback required_tool_unavailable; done; '
        'ROOT_SOURCE=$(findmnt -rn -o SOURCE --mountpoint / 2>/dev/null) '
        '|| fallback root_device_unresolved; '
        '[ -n "$ROOT_SOURCE" ] || fallback root_device_unresolved; '
        'ROOT_DEVICE=$(readlink -f "$ROOT_SOURCE") '
        '|| fallback root_device_unresolved; '
        '[ -b "$ROOT_DEVICE" ] || fallback root_device_unresolved; '
        'ROOT_ROWS=$(lsblk -srnpo NAME "$ROOT_DEVICE" 2>/dev/null) '
        '|| fallback root_device_unresolved; '
        'declare -a ROOT_DEVICES=(); '
        'while IFS= read -r ROOT_MEMBER; do '
        '[ -n "$ROOT_MEMBER" ] || continue; '
        'ROOT_MEMBER=$(readlink -f "$ROOT_MEMBER") '
        '|| fallback root_device_unresolved; '
        '[ -b "$ROOT_MEMBER" ] || fallback root_device_unresolved; '
        'ROOT_DEVICES+=("$ROOT_MEMBER"); done <<<"$ROOT_ROWS"; '
        '[ "${#ROOT_DEVICES[@]}" -gt 0 ] '
        '|| fallback root_device_unresolved; '
        'declare -a DEVICES=(); declare -a LINKS=(); '
        'add_candidate() { local LINK_VALUE=$1 DEVICE_VALUE EXISTING ROOT_VALUE; '
        'DEVICE_VALUE=$(readlink -f "$LINK_VALUE"); '
        '[ -b "$DEVICE_VALUE" ] || return 0; '
        '[ "$(lsblk -dnro TYPE "$DEVICE_VALUE" 2>/dev/null '
        '| tr -d "[:space:]")" = disk ] || return 0; '
        'for ROOT_VALUE in "${ROOT_DEVICES[@]}"; do '
        '[ "$DEVICE_VALUE" != "$ROOT_VALUE" ] || return 0; done; '
        'for EXISTING in "${DEVICES[@]}"; do '
        '[ "$EXISTING" = "$DEVICE_VALUE" ] && return 0; done; '
        'DEVICES+=("$DEVICE_VALUE"); LINKS+=("$LINK_VALUE"); }; '
        + discovery
        + '[ "${#DEVICES[@]}" -eq "$EXPECTED_COUNT" ] '
        '|| fallback candidate_count_mismatch; '
        'TOTAL_BYTES=0; declare -a CAPACITIES=(); '
        'for DEVICE_VALUE in "${DEVICES[@]}"; do '
        'SIZE_VALUE=$(lsblk -bdnro SIZE "$DEVICE_VALUE" 2>/dev/null '
        '| head -n 1 | tr -d "[:space:]"); '
        'case "$SIZE_VALUE" in ""|*[!0-9]*) fallback invalid_capacity;; esac; '
        'CAPACITIES+=("$SIZE_VALUE"); TOTAL_BYTES=$((TOTAL_BYTES + SIZE_VALUE)); '
        'done; '
        + capacity_check
        + 'DEVICE=${DEVICES[0]}; DEVICE_LINK=${LINKS[0]}; '
        'CAPACITY_BYTES=${CAPACITIES[0]}; '
        '[ "$CAPACITY_BYTES" -ge "$MINIMUM_BYTES" ] '
        '|| fallback device_too_small; '
        'READ_ONLY=$(lsblk -dnro RO "$DEVICE" 2>/dev/null '
        '| tr -d "[:space:]") || fallback readonly_probe_failed; '
        '[ "$READ_ONLY" = 0 ] || fallback readonly_device; '
        'BLOCK_ROWS=$(lsblk -nrpo NAME,TYPE "$DEVICE" 2>/dev/null) '
        '|| fallback partition_probe_failed; '
        'while read -r _BLOCK_NAME BLOCK_TYPE; do '
        '[ "$BLOCK_TYPE" != part ] || fallback device_has_partitions; '
        'done <<<"$BLOCK_ROWS"; '
        'SYSFS_NAME=${DEVICE##*/}; '
        'HOLDERS_DIR="/sys/class/block/$SYSFS_NAME/holders"; '
        '[ -d "$HOLDERS_DIR" ] || fallback holders_probe_failed; '
        'HOLDER=$(find "$HOLDERS_DIR" -mindepth 1 -maxdepth 1 '
        '-print -quit 2>/dev/null) || fallback holders_probe_failed; '
        '[ -z "$HOLDER" ] || fallback device_has_holders; '
        'MOUNTPOINTS=$(lsblk -nrpo MOUNTPOINTS "$DEVICE" 2>/dev/null) '
        '|| fallback mount_probe_failed; '
        '[[ ! "$MOUNTPOINTS" =~ [^[:space:]] ]] '
        '|| fallback device_is_mounted; '
        'SWAPS=$(swapon --show=NAME --noheadings --raw 2>/dev/null) '
        '|| fallback swap_probe_failed; '
        'while IFS= read -r SWAP_DEVICE; do '
        '[ -n "$SWAP_DEVICE" ] || continue; '
        'SWAP_DEVICE=$(readlink -f "$SWAP_DEVICE") '
        '|| fallback swap_probe_failed; '
        '[ -e "$SWAP_DEVICE" ] || fallback swap_probe_failed; '
        '[ "$SWAP_DEVICE" != "$DEVICE" ] '
        '|| fallback device_is_swap; done <<<"$SWAPS"; '
        'SIGNATURES=$(sudo wipefs -n --noheadings --output TYPE '
        '"$DEVICE" 2>/dev/null) || fallback signature_probe_failed; '
        '[[ ! "$SIGNATURES" =~ [^[:space:]] ]] '
        '|| fallback device_has_signatures; '
        'MOUNT_TARGETS=$(findmnt -rn -o TARGET 2>/dev/null) '
        '|| fallback mount_point_probe_failed; '
        'while IFS= read -r MOUNT_TARGET; do '
        '[ "$MOUNT_TARGET" != "$MOUNT_POINT" ] '
        '|| fallback mount_point_busy; done <<<"$MOUNT_TARGETS"; '
        'if [ -L "$MOUNT_POINT" ] '
        '|| { [ -e "$MOUNT_POINT" ] && [ ! -d "$MOUNT_POINT" ]; }; then '
        'fallback invalid_mount_point; fi; '
        'sudo mkdir -p "$MOUNT_POINT"; '
        'MOUNT_CONTENT=$(sudo find "$MOUNT_POINT" -mindepth 1 -maxdepth 1 '
        '-print -quit 2>/dev/null) || fallback mount_point_probe_failed; '
        '[ -z "$MOUNT_CONTENT" ] || fallback mount_point_not_empty; '
        'sudo mkfs.xfs "$DEVICE" >/dev/null; '
        'sudo mount "$DEVICE" "$MOUNT_POINT"; sudo chmod 0777 "$MOUNT_POINT"; '
        'MOUNTED_SOURCE=$(findmnt -rn -o SOURCE --mountpoint "$MOUNT_POINT"); '
        'MOUNTED_TYPE=$(findmnt -rn -o FSTYPE --mountpoint "$MOUNT_POINT"); '
        '[ "$(readlink -f "$MOUNTED_SOURCE")" = "$DEVICE" ] '
        '&& [ "$MOUNTED_TYPE" = xfs ]; '
        'MODEL=$(lsblk -dno MODEL "$DEVICE" 2>/dev/null '
        "| sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'); "
        'if [ -z "$MODEL" ]; then '
        'case "$PROVIDER" in aws) MODEL="Amazon EC2 NVMe Instance Storage";; '
        'oci) MODEL="OCI local NVMe";; azure) MODEL="Azure local NVMe";; '
        'gcp) MODEL="Google Local SSD NVMe";; esac; fi; '
        'MODEL_B64=$(printf %s "$MODEL" | base64 | tr -d "\\n"); '
        'printf "OCI_BENCH_STORAGE_TARGET_V1 provider=%s model_b64=%s '
        'capacity_bytes=%s mount_point=%s filesystem=xfs\\n" '
        '"$PROVIDER" "$MODEL_B64" "$CAPACITY_BYTES" "$MOUNT_POINT"'
    )


def mounted_storage_descriptor_command(mount_point: str = '/data') -> str:
    """Build a read-only descriptor probe for an exact, already mounted disk."""

    mount_point = _mount_point(mount_point)
    mount_literal = shlex.quote(mount_point)
    return (
        'set -euo pipefail; '
        f'MOUNT_POINT={mount_literal}; '
        'for TOOL in findmnt lsblk base64; do command -v "$TOOL" '
        '>/dev/null 2>&1; done; '
        'SOURCE=$(findmnt -rn -o SOURCE --mountpoint "$MOUNT_POINT"); '
        'FILESYSTEM=$(findmnt -rn -o FSTYPE --mountpoint "$MOUNT_POINT"); '
        '[ "$FILESYSTEM" = xfs ]; '
        'OPTIONS=$(findmnt -rn -o OPTIONS --mountpoint "$MOUNT_POINT"); '
        'case ",$OPTIONS," in *,ro,*) exit 1;; esac; '
        'DEVICE=$(readlink -f "$SOURCE"); [ -b "$DEVICE" ]; '
        'while [ "$(lsblk -dnro TYPE "$DEVICE" 2>/dev/null '
        '| tr -d "[:space:]")" != disk ]; do '
        'PARENTS=$(lsblk -dnro PKNAME "$DEVICE" 2>/dev/null '
        '| sed "/^[[:space:]]*$/d"); '
        '[ "$(printf "%s\\n" "$PARENTS" | wc -l)" -eq 1 ]; '
        'DEVICE="/dev/$(printf %s "$PARENTS" | tr -d "[:space:]")"; '
        '[ -b "$DEVICE" ]; done; '
        'CAPACITY_BYTES=$(lsblk -bdnro SIZE "$DEVICE" 2>/dev/null '
        '| head -n 1 | tr -d "[:space:]"); '
        'case "$CAPACITY_BYTES" in ""|*[!0-9]*) exit 1;; esac; '
        '[ "$CAPACITY_BYTES" -gt 0 ]; '
        'MODEL=$(lsblk -dno MODEL "$DEVICE" 2>/dev/null '
        "| sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//'); "
        '[ -n "$MODEL" ] || MODEL="Cloud attached block volume"; '
        'MODEL_B64=$(printf %s "$MODEL" | base64 | tr -d "\\n"); '
        'TRANSPORT=$(lsblk -dnro TRAN "$DEVICE" 2>/dev/null '
        '| head -n 1 | tr "[:upper:]" "[:lower:]" '
        '| tr -d "[:space:]"); '
        'case "$TRANSPORT" in nvme) :;; iscsi) :;; scsi|sas|sata) '
        'TRANSPORT=scsi;; *) TRANSPORT=paravirtualized;; esac; '
        'printf "OCI_BENCH_MOUNTED_STORAGE_TARGET_V1 model_b64=%s '
        'capacity_bytes=%s mount_point=%s filesystem=xfs transport=%s\\n" '
        '"$MODEL_B64" "$CAPACITY_BYTES" "$MOUNT_POINT" "$TRANSPORT"'
    )


def _parse_fields(line: str, marker: str, expected: set[str]) -> dict[str, str]:
    parts = line.split()
    if not parts or parts[0] != marker:
        raise ValueError('Storage-target output marker is malformed.')
    fields: dict[str, str] = {}
    for part in parts[1:]:
        key, separator, value = part.partition('=')
        if not separator or not key or not value or key in fields:
            raise ValueError('Storage-target output marker is malformed.')
        fields[key] = value
    if set(fields) != expected:
        raise ValueError('Storage-target output marker has unexpected fields.')
    return fields


def _marker(output: str, *markers: str) -> tuple[str, str]:
    matches = []
    for raw_line in str(output or '').splitlines():
        line = raw_line.strip()
        if any(line == marker or line.startswith(marker + ' ') for marker in markers):
            matches.append(line)
    if len(matches) != 1:
        raise ValueError(
            'Storage-target output must contain exactly one result marker.'
        )
    marker = matches[0].split(maxsplit=1)[0]
    return marker, matches[0]


def _model(value: str) -> str:
    if not value or not re.fullmatch(r'[A-Za-z0-9+/]+={0,2}', value):
        raise ValueError('Storage-target model encoding is invalid.')
    try:
        decoded = base64.b64decode(value, validate=True).decode('utf-8')
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError('Storage-target model encoding is invalid.') from exc
    normalized = ' '.join(_MODEL_RE.sub(' ', decoded).split())
    if not normalized:
        raise ValueError('Storage-target model is empty after normalization.')
    return normalized[:160]


def _positive_capacity(value: str, *, minimum: int = 1) -> int:
    if not value.isdigit():
        raise ValueError('Storage-target capacity is invalid.')
    capacity = int(value)
    if capacity < minimum:
        raise ValueError('Storage-target capacity is below the required minimum.')
    return capacity


def parse_local_nvme_prepare_result(
    output: str,
) -> tuple[dict | None, str | None]:
    """Parse a local-NVMe target or its sanitized fallback reason."""

    marker, line = _marker(output, _SUCCESS_MARKER, _FALLBACK_MARKER)
    if marker == _FALLBACK_MARKER:
        fields = _parse_fields(
            line,
            marker,
            {'provider', 'reason'},
        )
    else:
        fields = _parse_fields(
            line,
            marker,
            {
                'provider',
                'model_b64',
                'capacity_bytes',
                'mount_point',
                'filesystem',
            },
        )
    _provider(fields['provider'])
    if marker == _FALLBACK_MARKER:
        if not _REASON_RE.fullmatch(fields['reason']):
            raise ValueError('Storage-target fallback reason is invalid.')
        return None, fields['reason']
    if fields['filesystem'] != 'xfs':
        raise ValueError('Local-NVMe storage target must use XFS.')
    mount_point = _mount_point(fields['mount_point'])
    capacity = _positive_capacity(
        fields['capacity_bytes'],
        minimum=MINIMUM_LOCAL_NVME_BYTES,
    )
    return {
        'storage_target_contract': STORAGE_TARGET_CONTRACT,
        'storage_target_policy': STORAGE_TARGET_POLICY,
        'storage_target_kind': 'instance_local_nvme',
        'storage_target_verification': LOCAL_NVME_VERIFICATION,
        'storage_target_transport': 'nvme',
        'storage_target_model': _model(fields['model_b64']),
        'storage_target_capacity_bytes': capacity,
        'storage_target_device_count': 1,
        'storage_target_layout': 'single_device_v1',
        'storage_target_filesystem': 'xfs',
        'storage_target_mount_point': mount_point,
    }, None


def parse_local_nvme_prepare_output(output: str) -> dict | None:
    """Parse local-NVMe output into report-ready target metadata."""

    target, _fallback_reason = parse_local_nvme_prepare_result(output)
    return target


def parse_mounted_storage_descriptor_output(output: str) -> dict:
    """Parse metadata for the manifest-bound additional-volume fallback."""

    marker, line = _marker(output, _MOUNTED_MARKER)
    fields = _parse_fields(
        line,
        marker,
        {
            'model_b64',
            'capacity_bytes',
            'mount_point',
            'filesystem',
            'transport',
        },
    )
    if fields['filesystem'] != 'xfs':
        raise ValueError('Mounted storage target must use XFS.')
    if fields['transport'] not in {
        'nvme',
        'iscsi',
        'scsi',
        'paravirtualized',
    }:
        raise ValueError('Mounted storage target transport is invalid.')
    return {
        'storage_target_contract': STORAGE_TARGET_CONTRACT,
        'storage_target_policy': STORAGE_TARGET_POLICY,
        'storage_target_kind': 'provisioned_data_volume',
        'storage_target_verification': ADDITIONAL_VOLUME_VERIFICATION,
        'storage_target_transport': fields['transport'],
        'storage_target_model': _model(fields['model_b64']),
        'storage_target_capacity_bytes': _positive_capacity(
            fields['capacity_bytes']
        ),
        'storage_target_device_count': 1,
        'storage_target_layout': 'single_device_v1',
        'storage_target_filesystem': 'xfs',
        'storage_target_mount_point': _mount_point(fields['mount_point']),
    }


__all__ = [
    'ADDITIONAL_VOLUME_VERIFICATION',
    'LOCAL_NVME_MOUNT_POINT',
    'LOCAL_NVME_VERIFICATION',
    'MINIMUM_LOCAL_NVME_BYTES',
    'STORAGE_TARGET_CONTRACT',
    'STORAGE_TARGET_POLICY',
    'local_nvme_prepare_command',
    'mounted_storage_descriptor_command',
    'parse_local_nvme_prepare_output',
    'parse_local_nvme_prepare_result',
    'parse_mounted_storage_descriptor_output',
]
