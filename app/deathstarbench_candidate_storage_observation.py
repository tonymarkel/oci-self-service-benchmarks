"""Read-only storage observations for staged Hotel/Media support workloads.

The initial 80-GiB free-space budget belongs to the separate predeployment
preparation receipt. MongoDB may consume that space after staging. This boundary
only observes the same owned filesystem and audited directory inventory; it
cannot prepare, relabel, repair, erase, or qualify any data or benchmark.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
import json
import re
import shlex
from typing import Any

from . import deathstarbench_candidate_storage as storage


OBSERVATION_SCHEMA = 'candidate-storage-observation-v1'
OBSERVATION_MARKER = 'CANDIDATE_DSB_DATABASE_STORAGE_OBSERVATION'
_MAX_OUTPUT_CHARACTERS = 262144


class CandidateStorageObservationError(ValueError):
    """A read-only receipt cannot prove its exact owned storage scope."""


def _validated_contract(value: storage.CandidateStorageContract) -> storage.CandidateStorageContract:
    try:
        return storage._validated_contract(value)
    except (TypeError, ValueError) as exc:
        raise CandidateStorageObservationError('An exact audited candidate storage contract is required.') from exc


def _identity(contract: storage.CandidateStorageContract) -> dict[str, Any]:
    return {
        **contract.metadata(),
        'candidate_schema': OBSERVATION_SCHEMA,
        'read_only': True,
        'dataset_qualified': False,
        'measurement_qualified': False,
    }


def candidate_database_storage_observation_command(contract: storage.CandidateStorageContract) -> str:
    """Observe owned mounted storage without replaying preparation on live data.

    Every filesystem operation is a read. Root is needed to inspect MongoDB's
    directory contents and root-only ownership marker. Database directories may
    belong to MongoDB after startup; the mount, database root, and marker must
    retain their original root ownership. No per-directory quotas are implied.
    """

    contract = _validated_contract(contract)
    marker_json = json.dumps(contract.metadata(), sort_keys=True, separators=(',', ':'), ensure_ascii=True)
    encoded_marker = base64.b64encode(marker_json.encode('utf-8')).decode('ascii')
    expected_inventory = '\n'.join(contract.database_names)
    expected_root_inventory = '\n'.join(sorted((storage.OWNERSHIP_MARKER.rsplit('/', 1)[1], 'mongodb')))
    directories = ' '.join(shlex.quote(f'{storage.DATABASE_ROOT}/{name}') for name in contract.database_names)
    claim_bytes = len(contract.database_names) * storage.CLAIM_BYTES_PER_DATABASE
    shell = f'''set -euo pipefail
export LC_ALL=C
MOUNT_POINT={shlex.quote(storage.MOUNT_POINT)}
DATA_ROOT={shlex.quote(storage.DATABASE_ROOT)}
MARKER={shlex.quote(storage.OWNERSHIP_MARKER)}
LEGACY_MARKER={shlex.quote(storage.LEGACY_SOCIAL_MARKER)}
EXPECTED_UUID={shlex.quote(contract.filesystem_uuid)}
EXPECTED_INVENTORY={shlex.quote(expected_inventory)}
EXPECTED_ROOT_INVENTORY={shlex.quote(expected_root_inventory)}
EXPECTED_MARKER=$(printf '%s' {shlex.quote(encoded_marker)} | base64 --decode)
REQUIRED_BYTES={claim_bytes}
fail() {{ printf '%s\n' "candidate-storage-observation: $*" >&2; exit 1; }}
test "$(id -u)" = 0 || fail 'root inspection is required'
for TOOL in id findmnt readlink stat find sort awk df cat base64; do
  command -v "$TOOL" >/dev/null || fail "required tool missing: $TOOL"
done
verify_mount() {{
  test -d "$MOUNT_POINT" && test ! -L "$MOUNT_POINT" || fail 'mount point is not an ordinary directory'
  test "$(readlink -f "$MOUNT_POINT")" = "$MOUNT_POINT" || fail 'mount point is not canonical'
  case "$(stat -c %u:%g:%a "$MOUNT_POINT")" in 0:0:700|0:0:750|0:0:755) ;; *) fail 'mount point has unsafe ownership or permissions';; esac
  test "$(findmnt -rn -o FSTYPE --mountpoint "$MOUNT_POINT")" = xfs || fail 'filesystem is not xfs'
  ACTUAL_UUID=$(findmnt -rn -o UUID --mountpoint "$MOUNT_POINT")
  test "$ACTUAL_UUID" = "$EXPECTED_UUID" || fail 'filesystem UUID changed'
  MOUNTS=$(findmnt -rn -o TARGET)
  if printf '%s\n' "$MOUNTS" | awk -v root="$MOUNT_POINT/" 'index($0, root) == 1 {{ found=1 }} END {{ exit found ? 0 : 1 }}'; then
    fail 'nested mount exists beneath candidate storage'
  fi
}}
verify_ownership() {{
  if test -e "$LEGACY_MARKER" || test -L "$LEGACY_MARKER"; then fail 'released Social storage marker exists'; fi
  test -f "$MARKER" && test ! -L "$MARKER" || fail 'exact ownership marker is missing'
  test "$(stat -c %u:%g:%a:%h:%s "$MARKER")" = 0:0:600:1:{len(marker_json.encode('utf-8'))} || fail 'ownership marker mode, owner, links or size changed'
  test "$(cat "$MARKER")" = "$EXPECTED_MARKER" || fail 'storage belongs to another run, workload or disk'
  ROOT_INVENTORY=$(find "$MOUNT_POINT" -mindepth 1 -maxdepth 1 -printf '%f\n' | sort)
  test "$ROOT_INVENTORY" = "$EXPECTED_ROOT_INVENTORY" || fail 'candidate filesystem inventory changed'
  test -d "$DATA_ROOT" && test ! -L "$DATA_ROOT" || fail 'database root is not an ordinary directory'
  test "$(readlink -f "$DATA_ROOT")" = "$DATA_ROOT" || fail 'database root is not canonical'
  case "$(stat -c %u:%g:%a "$DATA_ROOT")" in 0:0:700|0:0:750|0:0:755) ;; *) fail 'database root has unsafe ownership or permissions';; esac
}}
verify_inventory() {{
  ACTUAL_INVENTORY=$(find "$DATA_ROOT" -mindepth 1 -maxdepth 1 -printf '%f\n' | sort)
  test "$ACTUAL_INVENTORY" = "$EXPECTED_INVENTORY" || fail 'database inventory differs from audited profile'
  UNSAFE=$(find "$DATA_ROOT" -xdev \\( -type l -o -type f -links +1 -o ! \\( -type f -o -type d \\) \\) -print -quit)
  test -z "$UNSAFE" || fail 'database tree contains a symlink, hardlink or special file'
  for DIRECTORY in "$DATA_ROOT" {directories}; do
    test -d "$DIRECTORY" && test ! -L "$DIRECTORY" || fail 'database directory is not ordinary'
    test "$(readlink -f "$DIRECTORY")" = "$DIRECTORY" || fail 'database directory is not canonical'
    test "$(findmnt -rn -o UUID -T "$DIRECTORY")" = "$EXPECTED_UUID" || fail 'database directory moved to another filesystem'
    CONTEXT=$(stat -c %C "$DIRECTORY")
    case "$CONTEXT" in *:container_file_t:*) ;; *) fail 'database directory SELinux label changed';; esac
  done
}}
verify_space() {{
  SPACE=$(df -B1 --output=size,avail "$MOUNT_POINT")
  SPACE=$(printf '%s\n' "$SPACE" | awk 'NR==2 {{ print $1, $2 }} END {{ if (NR != 2) exit 1 }}')
  read -r TOTAL_BYTES AVAILABLE_BYTES EXTRA <<<"$SPACE"
  [[ "$TOTAL_BYTES" =~ ^[1-9][0-9]*$ && "$AVAILABLE_BYTES" =~ ^(0|[1-9][0-9]*)$ && -z "${{EXTRA:-}}" ]] || fail 'filesystem space output is invalid'
  test "${{#TOTAL_BYTES}}" -le 18 && test "${{#AVAILABLE_BYTES}}" -le 18 || fail 'filesystem space output is too large'
  (( TOTAL_BYTES >= REQUIRED_BYTES && AVAILABLE_BYTES >= 0 && AVAILABLE_BYTES <= TOTAL_BYTES )) || fail 'filesystem capacity observation is invalid'
}}
verify_mount
verify_ownership
verify_inventory
verify_space
verify_mount
verify_ownership
verify_inventory
verify_space
printf '{OBSERVATION_MARKER} schema={OBSERVATION_SCHEMA} candidate_only=1 released=0 runtime_qualified=0 dataset_qualified=0 measurement_qualified=0 read_only=1 run={contract.run_id} workload={contract.workload_id} namespace={contract.namespace} uuid=%s root=%s contract_sha256={contract.sha256} inventory_sha256={contract.inventory_sha256} databases=8 claim_bytes={claim_bytes} total_bytes=%s available_bytes=%s filesystem=xfs selinux=container_file_t\n' "$ACTUAL_UUID" "$DATA_ROOT" "$TOTAL_BYTES" "$AVAILABLE_BYTES"
'''
    return 'sudo bash -c ' + shlex.quote(shell)


def parse_candidate_database_storage_observation(
    output: str | bytes, contract: storage.CandidateStorageContract,
) -> dict[str, Any]:
    """Parse one exact read-only observation for the current owned contract."""

    contract = _validated_contract(contract)
    if isinstance(output, bytes):
        try:
            output = output.decode('utf-8')
        except UnicodeDecodeError as exc:
            raise CandidateStorageObservationError('Storage observation must be UTF-8.') from exc
    if not isinstance(output, str) or len(output) > _MAX_OUTPUT_CHARACTERS or '\r' in output or '\0' in output:
        raise CandidateStorageObservationError('Storage observation is invalid or oversized.')
    markers = [line for line in output.split('\n') if OBSERVATION_MARKER in line]
    if len(markers) != 1:
        raise CandidateStorageObservationError('Exactly one storage observation marker is required.')
    pattern = (
        rf'{OBSERVATION_MARKER} schema={OBSERVATION_SCHEMA} candidate_only=1 released=0 runtime_qualified=0 '
        r'dataset_qualified=0 measurement_qualified=0 read_only=1 '
        r'run=(\S+) workload=(\S+) namespace=(\S+) uuid=(\S+) root=(\S+) '
        r'contract_sha256=([0-9a-f]{64}) inventory_sha256=([0-9a-f]{64}) databases=8 '
        r'claim_bytes=85899345920 total_bytes=([1-9][0-9]{0,17}) available_bytes=(0|[1-9][0-9]{0,17}) '
        r'filesystem=xfs selinux=container_file_t'
    )
    match = re.fullmatch(pattern, markers[0])
    if match is None:
        raise CandidateStorageObservationError('Storage observation marker schema differs.')
    run_id, workload, namespace, uuid, root, digest, inventory_digest, total, available = match.groups()
    expected = (
        contract.run_id, contract.workload_id, contract.namespace,
        contract.filesystem_uuid, storage.DATABASE_ROOT, contract.sha256, contract.inventory_sha256,
    )
    if (run_id, workload, namespace, uuid, root, digest, inventory_digest) != expected:
        raise CandidateStorageObservationError('Storage observation belongs to another run, workload or disk.')
    return validate_saved_candidate_database_storage_observation({
        **_identity(contract),
        'total_bytes': int(total), 'available_bytes': int(available),
        'filesystem': 'xfs', 'selinux_type': 'container_file_t',
    }, contract)


def validate_saved_candidate_database_storage_observation(
    value: Mapping[str, Any], contract: storage.CandidateStorageContract,
) -> dict[str, Any]:
    """Restore exact historical evidence; this does not make it a fresh read."""

    contract = _validated_contract(contract)
    identity = _identity(contract)
    observed_fields = {'total_bytes', 'available_bytes', 'filesystem', 'selinux_type'}
    if not isinstance(value, Mapping) or set(value) != set(identity) | observed_fields:
        raise CandidateStorageObservationError('Saved storage observation schema is invalid.')
    for key, expected in identity.items():
        if type(value[key]) is not type(expected) or value[key] != expected:
            raise CandidateStorageObservationError(f'Saved storage observation identity differs: {key}.')
    for key in ('total_bytes', 'available_bytes'):
        if type(value[key]) is not int or not 0 <= value[key] < 10**18:
            raise CandidateStorageObservationError('Saved storage observation capacity is invalid.')
    if not identity['total_claim_bytes'] <= value['total_bytes'] or value['available_bytes'] > value['total_bytes']:
        raise CandidateStorageObservationError('Saved storage observation capacity is invalid.')
    if value['filesystem'] != 'xfs' or value['selinux_type'] != 'container_file_t':
        raise CandidateStorageObservationError('Saved storage observation filesystem or SELinux identity differs.')
    return {
        **identity,
        'total_bytes': value['total_bytes'], 'available_bytes': value['available_bytes'],
        'filesystem': 'xfs', 'selinux_type': 'container_file_t',
    }
