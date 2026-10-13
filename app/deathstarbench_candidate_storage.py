"""Owned database storage for offline Hotel/Media runtime candidates.

This module is separate from Social Network's released guest commands. The
generated command prepares directories only, before deploying a workload. It
does not format, mount, erase data, initialize MongoDB, or qualify a benchmark.
Preparation requires the full claim budget to remain available: it is not a
live-data recovery command and cannot reassign storage to another run/profile.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import re
import shlex
from typing import Any

from .deathstarbench_candidate_workload import validate_candidate_assets
from .deathstarbench_workload_contract import distributed_workload_profile


MOUNT_POINT = '/var/lib/deathstarbench/database'
DATABASE_ROOT = f'{MOUNT_POINT}/mongodb'
OWNERSHIP_MARKER = f'{MOUNT_POINT}/.deathstarbench-candidate-storage-v1'
LEGACY_SOCIAL_MARKER = f'{MOUNT_POINT}/.deathstarbench-workload-storage-v1'
CLAIM_BYTES_PER_DATABASE = 10 * 1024**3
ATTESTATION_MARKER = 'CANDIDATE_DSB_DATABASE_STORAGE'
_RUN_ID = re.compile(r'[0-9a-f]{12}')
_UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')


class CandidateStorageError(ValueError):
    """The storage identity or guest evidence cannot prove its exact scope."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


@dataclass(frozen=True, slots=True)
class CandidateStorageContract:
    """One run's immutable eight-directory predeployment storage identity."""

    run_id: str
    workload_id: str
    workload_revision: str
    namespace: str
    filesystem_uuid: str
    component_asset_sha256: str
    database_names: tuple[str, ...]

    def _identity(self) -> dict[str, Any]:
        return {
            'schema_version': 1,
            'candidate_only': True,
            'released': False,
            'runtime_qualified': False,
            'run_id': self.run_id,
            'workload_id': self.workload_id,
            'workload_revision': self.workload_revision,
            'namespace': self.namespace,
            'filesystem_uuid': self.filesystem_uuid,
            'mount_point': MOUNT_POINT,
            'root': DATABASE_ROOT,
            'component_asset_sha256': self.component_asset_sha256,
            'database_names': list(self.database_names),
            'database_count': len(self.database_names),
            'claim_bytes_per_database': CLAIM_BYTES_PER_DATABASE,
            'total_claim_bytes': len(self.database_names) * CLAIM_BYTES_PER_DATABASE,
            'inventory_sha256': self.inventory_sha256,
        }

    @property
    def inventory_sha256(self) -> str:
        return hashlib.sha256(_canonical(list(self.database_names)).encode()).hexdigest()

    @property
    def sha256(self) -> str:
        return hashlib.sha256(_canonical(self._identity()).encode()).hexdigest()

    def metadata(self) -> dict[str, Any]:
        return {**self._identity(), 'storage_contract_sha256': self.sha256}


def candidate_storage_contract(
    workload_id: str, run_id: str, filesystem_uuid: str,
) -> CandidateStorageContract:
    """Resolve the audited candidate inventory, never caller-supplied paths."""

    if not isinstance(run_id, str) or not _RUN_ID.fullmatch(run_id) or run_id == '0' * 12:
        raise CandidateStorageError('Candidate storage requires a canonical 12-hex run ID.')
    if (
        not isinstance(filesystem_uuid, str) or not _UUID.fullmatch(filesystem_uuid)
        or filesystem_uuid == '00000000-0000-0000-0000-000000000000'
    ):
        raise CandidateStorageError('Candidate storage requires a canonical lowercase filesystem UUID.')
    try:
        profile = distributed_workload_profile(workload_id)
        components, _edges = validate_candidate_assets(workload_id)
    except ValueError as exc:
        raise CandidateStorageError(f'Candidate storage profile cannot be validated: {exc}') from exc
    names = tuple(sorted(
        item['storage_subdir'] for item in components if item['role'] == 'database'
    ))
    if profile.released or workload_id not in {'hotel_reservation', 'media_microservices'}:
        raise CandidateStorageError('Only unreleased Hotel and Media storage candidates are supported.')
    if len(names) != 8 or len(set(names)) != 8:
        raise CandidateStorageError('Candidate storage requires eight distinct audited MongoDB directories.')
    return CandidateStorageContract(
        run_id=run_id,
        workload_id=profile.workload_id,
        workload_revision=profile.workload_revision,
        namespace=profile.namespace,
        filesystem_uuid=filesystem_uuid,
        component_asset_sha256=profile.component_asset_sha256,
        database_names=names,
    )


def _validated_contract(value: CandidateStorageContract) -> CandidateStorageContract:
    if not isinstance(value, CandidateStorageContract):
        raise CandidateStorageError('An explicit CandidateStorageContract is required.')
    expected = candidate_storage_contract(value.workload_id, value.run_id, value.filesystem_uuid)
    if value != expected:
        raise CandidateStorageError('Candidate storage contract differs from the audited run/profile identity.')
    return expected


def candidate_database_storage_prepare_command(contract: CandidateStorageContract) -> str:
    """Generate one privileged, fail-closed predeployment directory preparation.

    An exclusive hard-link publishes the deterministic ownership marker without
    overwriting an existing marker. Interrupted preparation may be repeated only
    with that exact marker. Existing regular data is never deleted or rewritten;
    special files, symlinks, hardlinks, nested mounts and unknown entries fail.
    The sole temporary file removed is this invocation's fresh marker staging
    file, never a database directory or pre-existing file.
    """

    contract = _validated_contract(contract)
    encoded_marker = base64.b64encode(_canonical(contract.metadata()).encode()).decode()
    expected_marker_bytes = len(_canonical(contract.metadata()).encode())
    names = ' '.join(shlex.quote(name) for name in contract.database_names)
    expected_inventory = '\n'.join(contract.database_names)
    shell = f'''set -euo pipefail
export LC_ALL=C
umask 077
MOUNT_POINT={shlex.quote(MOUNT_POINT)}
DATA_ROOT={shlex.quote(DATABASE_ROOT)}
MARKER={shlex.quote(OWNERSHIP_MARKER)}
LEGACY_MARKER={shlex.quote(LEGACY_SOCIAL_MARKER)}
EXPECTED_UUID={shlex.quote(contract.filesystem_uuid)}
EXPECTED_INVENTORY={shlex.quote(expected_inventory)}
REQUIRED_BYTES={len(contract.database_names) * CLAIM_BYTES_PER_DATABASE}
EXPECTED_MARKER=$(printf '%s' {shlex.quote(encoded_marker)} | base64 --decode)
fail() {{ printf '%s\\n' "candidate-storage: $*" >&2; exit 1; }}
test "$(id -u)" = 0 || fail 'root ownership is required'
for TOOL in id findmnt readlink stat find sort awk df semanage restorecon cat mktemp chmod ln unlink install base64; do
  command -v "$TOOL" >/dev/null || fail "required tool missing: $TOOL"
done
test -d "$MOUNT_POINT" && test ! -L "$MOUNT_POINT" || fail 'mount point is not an ordinary directory'
test "$(readlink -f "$MOUNT_POINT")" = "$MOUNT_POINT" || fail 'mount point is not canonical'
case "$(stat -c %u:%g:%a "$MOUNT_POINT")" in 0:0:700|0:0:750|0:0:755) ;; *) fail 'mount point has unsafe ownership or permissions';; esac
verify_mount() {{
  test "$(findmnt -rn -o FSTYPE --mountpoint "$MOUNT_POINT")" = xfs || fail 'filesystem is not xfs'
  ACTUAL_UUID=$(findmnt -rn -o UUID --mountpoint "$MOUNT_POINT")
  test "$ACTUAL_UUID" = "$EXPECTED_UUID" || fail 'filesystem UUID changed'
  MOUNTS=$(findmnt -rn -o TARGET)
  if printf '%s\\n' "$MOUNTS" | awk -v root="$MOUNT_POINT/" 'index($0, root) == 1 {{ found=1 }} END {{ exit found ? 0 : 1 }}'; then
    fail 'nested mount exists beneath candidate storage'
  fi
}}
verify_space() {{
  SPACE=$(df -B1 --output=size,avail "$MOUNT_POINT")
  SPACE=$(printf '%s\\n' "$SPACE" | awk 'NR==2 {{ print $1, $2 }} END {{ if (NR != 2) exit 1 }}')
  read -r TOTAL_BYTES AVAILABLE_BYTES EXTRA <<<"$SPACE"
  [[ "$TOTAL_BYTES" =~ ^[1-9][0-9]*$ && "$AVAILABLE_BYTES" =~ ^[1-9][0-9]*$ && -z "${{EXTRA:-}}" ]] || fail 'filesystem space output is invalid'
  test "${{#TOTAL_BYTES}}" -le 18 && test "${{#AVAILABLE_BYTES}}" -le 18 || fail 'filesystem space output is too large'
  (( TOTAL_BYTES >= REQUIRED_BYTES && AVAILABLE_BYTES >= REQUIRED_BYTES && AVAILABLE_BYTES <= TOTAL_BYTES )) || fail 'full candidate claim budget is not available before deployment'
}}
verify_mount
verify_space
if test -e "$LEGACY_MARKER" || test -L "$LEGACY_MARKER"; then fail 'released Social storage marker already exists'; fi
ROOT_ENTRIES=$(find "$MOUNT_POINT" -mindepth 1 -maxdepth 1 -printf '%f\\n' | sort)
if test -e "$MARKER" || test -L "$MARKER"; then
  test -f "$MARKER" && test ! -L "$MARKER" || fail 'ownership marker is not a regular file'
  test "$(stat -c %u:%g:%a:%h:%s "$MARKER")" = 0:0:600:1:{expected_marker_bytes} || fail 'ownership marker mode, ownership, links or size changed'
  test "$(cat "$MARKER")" = "$EXPECTED_MARKER" || fail 'storage belongs to another run, profile or disk'
  while IFS= read -r NAME; do
    case "$NAME" in .deathstarbench-candidate-storage-v1|mongodb) ;; *) fail 'unknown entry exists on candidate filesystem';; esac
  done <<<"$ROOT_ENTRIES"
else
  test -z "$ROOT_ENTRIES" || fail 'unowned filesystem entries already exist'
  TEMP_MARKER=$(mktemp "$MOUNT_POINT/.candidate-storage-marker.XXXXXXXX")
  trap 'test -z "${{TEMP_MARKER:-}}" || unlink "$TEMP_MARKER"' EXIT
  printf '%s' "$EXPECTED_MARKER" > "$TEMP_MARKER"
  chmod 0600 "$TEMP_MARKER"
  ln "$TEMP_MARKER" "$MARKER" || fail 'another storage owner claimed this filesystem'
  unlink "$TEMP_MARKER"
  TEMP_MARKER=''
fi
if test -e "$DATA_ROOT" || test -L "$DATA_ROOT"; then
  test -d "$DATA_ROOT" && test ! -L "$DATA_ROOT" || fail 'database root is not an ordinary directory'
  test "$(readlink -f "$DATA_ROOT")" = "$DATA_ROOT" || fail 'database root is not canonical'
else
  install -d -o root -g root -m 0755 "$DATA_ROOT"
fi
case "$(stat -c %u:%g:%a "$DATA_ROOT")" in 0:0:700|0:0:750|0:0:755) ;; *) fail 'database root has unsafe ownership or permissions';; esac
ACTUAL_ENTRIES=$(find "$DATA_ROOT" -mindepth 1 -maxdepth 1 -printf '%f\\n' | sort)
if test -n "$ACTUAL_ENTRIES"; then
  while IFS= read -r NAME; do
    case "$NAME" in {'|'.join(contract.database_names)}) ;; *) fail 'unknown database entry exists';; esac
    DIRECTORY="$DATA_ROOT/$NAME"
    test -d "$DIRECTORY" && test ! -L "$DIRECTORY" || fail 'database entry is not an ordinary directory'
    test "$(readlink -f "$DIRECTORY")" = "$DIRECTORY" || fail 'database directory is not canonical'
  done <<<"$ACTUAL_ENTRIES"
fi
UNSAFE=$(find "$DATA_ROOT" -xdev \\( -type l -o -type f -links +1 -o ! \\( -type f -o -type d \\) \\) -print -quit)
test -z "$UNSAFE" || fail 'database tree contains a symlink, hardlink or special file'
for NAME in {names}; do
  DIRECTORY="$DATA_ROOT/$NAME"
  if test ! -e "$DIRECTORY"; then install -d -o root -g root -m 0755 "$DIRECTORY"; fi
  test -d "$DIRECTORY" && test ! -L "$DIRECTORY" || fail 'database directory changed'
  test "$(readlink -f "$DIRECTORY")" = "$DIRECTORY" || fail 'database directory changed canonical path'
  test "$(findmnt -rn -o UUID -T "$DIRECTORY")" = "$EXPECTED_UUID" || fail 'database directory moved to another filesystem'
done
ACTUAL_INVENTORY=$(find "$DATA_ROOT" -mindepth 1 -maxdepth 1 -printf '%f\\n' | sort)
test "$ACTUAL_INVENTORY" = "$EXPECTED_INVENTORY" || fail 'final database inventory differs from audited profile'
SELINUX_PATTERN='/var/lib/deathstarbench/database/mongodb(/.*)?'
semanage fcontext -a -t container_file_t "$SELINUX_PATTERN" 2>/dev/null || semanage fcontext -m -t container_file_t "$SELINUX_PATTERN"
restorecon -R -x "$DATA_ROOT"
for DIRECTORY in "$DATA_ROOT" { ' '.join(shlex.quote(f'{DATABASE_ROOT}/{name}') for name in contract.database_names) }; do
  CONTEXT=$(stat -c %C "$DIRECTORY")
  case "$CONTEXT" in *:container_file_t:*) ;; *) fail 'database directory SELinux label is unsafe';; esac
done
verify_mount
verify_space
printf '{ATTESTATION_MARKER} schema=candidate-storage-v1 candidate_only=1 released=0 runtime_qualified=0 run={contract.run_id} workload={contract.workload_id} namespace={contract.namespace} uuid=%s root=%s contract_sha256={contract.sha256} inventory_sha256={contract.inventory_sha256} databases=8 claim_bytes={len(contract.database_names) * CLAIM_BYTES_PER_DATABASE} total_bytes=%s available_bytes=%s filesystem=xfs selinux=container_file_t\\n' "$ACTUAL_UUID" "$DATA_ROOT" "$TOTAL_BYTES" "$AVAILABLE_BYTES"
'''
    return 'sudo bash -c ' + shlex.quote(shell)


def parse_candidate_database_storage_attestation(
    output: str | bytes, contract: CandidateStorageContract,
) -> dict[str, Any]:
    """Require one exact candidate marker bound to the complete storage scope."""

    contract = _validated_contract(contract)
    if isinstance(output, bytes):
        try:
            output = output.decode('utf-8')
        except UnicodeDecodeError as exc:
            raise CandidateStorageError('Candidate storage evidence is not UTF-8.') from exc
    if not isinstance(output, str) or len(output) > 262144 or '\r' in output or '\0' in output:
        raise CandidateStorageError('Candidate storage evidence is invalid or too large.')
    marker_lines = [line for line in output.split('\n') if ATTESTATION_MARKER in line]
    if len(marker_lines) != 1:
        raise CandidateStorageError('Candidate storage requires one exact attestation marker.')
    pattern = (
        rf'{ATTESTATION_MARKER} schema=candidate-storage-v1 candidate_only=1 released=0 runtime_qualified=0 '
        r'run=(\S+) workload=(\S+) namespace=(\S+) uuid=(\S+) root=(\S+) '
        r'contract_sha256=([0-9a-f]{64}) inventory_sha256=([0-9a-f]{64}) databases=8 '
        r'claim_bytes=85899345920 total_bytes=([1-9][0-9]{0,17}) available_bytes=([1-9][0-9]{0,17}) '
        r'filesystem=xfs selinux=container_file_t'
    )
    match = re.fullmatch(pattern, marker_lines[0])
    if match is None:
        raise CandidateStorageError('Candidate storage marker schema differs from its contract.')
    run_id, workload, namespace, uuid, root, digest, inventory_digest, total, available = match.groups()
    expected = (
        contract.run_id, contract.workload_id, contract.namespace,
        contract.filesystem_uuid, DATABASE_ROOT, contract.sha256, contract.inventory_sha256,
    )
    if (run_id, workload, namespace, uuid, root, digest, inventory_digest) != expected:
        raise CandidateStorageError('Candidate storage attestation belongs to another run/profile/disk.')
    total_bytes, available_bytes = int(total), int(available)
    if not len(contract.database_names) * CLAIM_BYTES_PER_DATABASE <= available_bytes <= total_bytes:
        raise CandidateStorageError('Candidate storage attestation lacks the full available claim budget.')
    return validate_candidate_database_storage_attestation({
        **contract.metadata(),
        'total_bytes': total_bytes,
        'available_bytes': available_bytes,
        'filesystem': 'xfs',
        'selinux_type': 'container_file_t',
    }, contract)


def validate_candidate_database_storage_attestation(
    value: Mapping[str, Any], contract: CandidateStorageContract,
) -> dict[str, Any]:
    """Validate persisted evidence, without claiming a fresh guest observation.

    This only restores one historical predeployment receipt. A caller must not
    treat it as current space/mount/readiness evidence or replay live database
    preparation on its authority.
    """

    contract = _validated_contract(contract)
    identity = contract.metadata()
    observed_fields = {'total_bytes', 'available_bytes', 'filesystem', 'selinux_type'}
    if not isinstance(value, Mapping) or set(value) != set(identity) | observed_fields:
        raise CandidateStorageError('Saved candidate storage attestation schema is invalid.')
    for key, expected in identity.items():
        actual = value[key]
        if type(actual) is not type(expected) or actual != expected:
            raise CandidateStorageError(f'Saved candidate storage identity differs: {key}.')
    for key in ('total_bytes', 'available_bytes'):
        if type(value[key]) is not int or not 1 <= value[key] < 10**18:
            raise CandidateStorageError('Saved candidate storage capacity observation is invalid.')
    if not identity['total_claim_bytes'] <= value['available_bytes'] <= value['total_bytes']:
        raise CandidateStorageError('Saved candidate storage lacks its full predeployment claim budget.')
    if value['filesystem'] != 'xfs' or value['selinux_type'] != 'container_file_t':
        raise CandidateStorageError('Saved candidate storage filesystem/SELinux evidence differs.')
    return {
        **identity,
        'total_bytes': value['total_bytes'],
        'available_bytes': value['available_bytes'],
        'filesystem': 'xfs',
        'selinux_type': 'container_file_t',
    }
