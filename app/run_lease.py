"""Fail-closed, process-wide leases for persisted benchmark runs.

Each run owns two current lease files:

* a stable, empty lease file whose inode is locked with POSIX ``flock``; and
* a bounded owner-metadata sidecar replaced atomically while that lock is held.

The lease file is never unlinked. Removing a locked file would let another
process create and lock a different inode for the same run. Owner metadata is
written to a private same-directory temporary file, flushed, validated, and
atomically renamed so a crash leaves either the prior or new canonical record.

During migration, every acquired lease also locks the historical Azure
qualification lock. Older qualifier processes know only that filename, so
holding both locks is required to prevent an old and a new process from
simultaneously owning the same persisted run.

Callers that mutate a run must hold ``acquire_run_lease`` for the complete
operation. ``inspect_run_lease`` is observational only: its answer can become
stale immediately and must never authorize a mutation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import errno
import json
import os
from pathlib import Path
import re
import stat
import threading
from typing import Any, Mapping
import uuid
import weakref

try:  # pragma: no cover - exercised only on unsupported platforms.
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None


RUN_LEASE_FILENAME = '.benchmark-run.lease'
RUN_LEASE_METADATA_FILENAME = '.benchmark-run.lease.owner'
LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME = (
    '.deathstarbench-azure-qualification.lock'
)
RUN_LEASE_SCHEMA_VERSION = 1
MAX_RUN_LEASE_METADATA_BYTES = 512
MAX_RUN_LEASE_OWNER_LABEL_CHARS = 64

_RUN_ID_RE = re.compile(r'^[0-9a-f]{12}$')
_OWNER_LABEL_RE = re.compile(
    rf'^[a-z][a-z0-9._-]{{0,{MAX_RUN_LEASE_OWNER_LABEL_CHARS - 1}}}$'
)
_METADATA_FIELDS = frozenset({
    'schema_version',
    'run_id',
    'owner_label',
    'pid',
    'acquired_at',
})


class RunLeaseError(RuntimeError):
    """Raised when lease state cannot be established safely."""


class RunLeaseHeldError(RunLeaseError):
    """Raised when another process already owns the requested run lease."""

    def __init__(self, owner: 'RunLeaseOwner') -> None:
        self.owner = owner
        super().__init__(
            f'Run {owner.run_id} is already leased by '
            f'{owner.owner_label!r} (pid {owner.pid}).'
        )


@dataclass(frozen=True)
class RunLeaseOwner:
    """Validated, deliberately non-secret metadata for one lease owner."""

    run_id: str
    owner_label: str
    pid: int
    acquired_at: str
    schema_version: int = RUN_LEASE_SCHEMA_VERSION

    def as_dict(self) -> dict[str, Any]:
        return {
            'schema_version': self.schema_version,
            'run_id': self.run_id,
            'owner_label': self.owner_label,
            'pid': self.pid,
            'acquired_at': self.acquired_at,
        }


@dataclass(frozen=True)
class RunLeaseStatus:
    """A point-in-time observation of a run lease."""

    held: bool
    owner: RunLeaseOwner | None


class RunLease:
    """An acquired run lease. Prefer using it as a context manager."""

    def __init__(
        self,
        *,
        file_descriptor: int,
        legacy_file_descriptor: int,
        path: Path,
        owner: RunLeaseOwner,
    ) -> None:
        self._file_descriptor = file_descriptor
        self._legacy_file_descriptor = legacy_file_descriptor
        self.path = path
        self.owner = owner
        self._released = False
        self._release_lock = threading.Lock()

    @property
    def released(self) -> bool:
        with self._release_lock:
            return self._released

    def release(self) -> None:
        """Release this lease exactly once; repeated calls are harmless."""

        with self._release_lock:
            if self._released:
                return
            self._released = True
            unlock_error: BaseException | None = None
            # Release the shared lock first and the legacy migration guard
            # last. A new process therefore cannot acquire either complete
            # lock pair while an old qualifier can begin mutating the run.
            for descriptor in (
                self._file_descriptor,
                self._legacy_file_descriptor,
            ):
                try:
                    _require_posix_support()
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                except BaseException as exc:
                    if unlock_error is None:
                        unlock_error = exc
                finally:
                    try:
                        os.close(descriptor)
                    except OSError as exc:
                        if unlock_error is None:
                            unlock_error = exc
        if unlock_error is not None:
            if isinstance(unlock_error, RunLeaseError):
                raise unlock_error
            raise RunLeaseError(
                f'Unable to release the run lease safely: {unlock_error}'
            ) from unlock_error

    def __enter__(self) -> 'RunLease':
        with self._release_lock:
            if self._released:
                raise RunLeaseError('A released run lease cannot be re-entered.')
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.release()


@dataclass(frozen=True)
class _AcquiredLeaseProof:
    """Factory-only process-local capability; never reconstructed from metadata."""

    path: Path
    canonical_path: Path
    directory_identity: tuple[int, int]
    file_descriptor: int
    legacy_file_descriptor: int
    file_identity: tuple[int, int]
    legacy_file_identity: tuple[int, int]
    metadata_identity: tuple[int, int]
    owner_metadata: bytes
    pid: int
    release_lock: Any


# Keying authority by the actual returned object rejects hand-made instances
# and shallow copies even when they borrow genuine fields/descriptors. Weak
# keys do not keep released objects alive or add any lock-file lifecycle.
_ACQUIRED_LEASE_PROOFS: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
_ACQUIRED_LEASE_PROOFS_LOCK = threading.Lock()


def _authority_file(
    directory_descriptor: int,
    filename: str,
    *,
    run_id: str,
    label: str,
) -> tuple[int, os.stat_result]:
    """Open an existing regular authority file without following or blocking."""

    descriptor: int | None = None
    try:
        descriptor = os.open(
            filename,
            os.O_RDWR | os.O_NOFOLLOW | getattr(os, 'O_NONBLOCK', 0),
            dir_fd=directory_descriptor,
        )
        details = _validate_private_regular_file(descriptor, run_id=run_id, label=label)
        named = os.stat(filename, dir_fd=directory_descriptor, follow_symlinks=False)
        if (
            not stat.S_ISREG(named.st_mode)
            or (named.st_dev, named.st_ino) != (details.st_dev, details.st_ino)
        ):
            raise RunLeaseError(f'The {label} for {run_id} changed while opening.')
        return descriptor, details
    except BaseException as exc:
        if descriptor is not None:
            os.close(descriptor)
        if isinstance(exc, OSError):
            raise RunLeaseError(f'Unable to open the exact {label} for {run_id}: {exc}') from exc
        raise


def _record_acquired_lease(lease: RunLease, directory_descriptor: int) -> None:
    """Capture original relationships after acquisition, without extra locks."""

    run_id = lease.owner.run_id
    shared = _validate_private_regular_file(lease._file_descriptor, run_id=run_id, label='run lease')
    legacy = _validate_private_regular_file(lease._legacy_file_descriptor, run_id=run_id, label='legacy qualification lock')
    directory = os.fstat(directory_descriptor)
    if shared.st_size != 0 or legacy.st_size != 0:
        raise RunLeaseError(f'The acquired lock files for {run_id} must remain empty.')
    try:
        canonical_path = lease.path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise RunLeaseError(f'The acquired run lease path for {run_id} is not canonical.') from exc
    for filename, retained in (
        (RUN_LEASE_FILENAME, shared),
        (LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME, legacy),
    ):
        descriptor, opened = _authority_file(directory_descriptor, filename, run_id=run_id, label='acquired lock')
        try:
            if (opened.st_dev, opened.st_ino) != (retained.st_dev, retained.st_ino):
                raise RunLeaseError(f'The acquired lock path for {run_id} changed before publication.')
        finally:
            os.close(descriptor)
    metadata_descriptor, metadata = _authority_file(
        directory_descriptor, RUN_LEASE_METADATA_FILENAME,
        run_id=run_id, label='run lease owner metadata',
    )
    try:
        owner = _read_owner_metadata_from_descriptor(metadata_descriptor, run_id=run_id)
        if owner != lease.owner:
            raise RunLeaseError(f'The acquired run lease owner for {run_id} changed.')
    finally:
        os.close(metadata_descriptor)
    proof = _AcquiredLeaseProof(
        path=lease.path, canonical_path=canonical_path,
        directory_identity=(directory.st_dev, directory.st_ino),
        file_descriptor=lease._file_descriptor,
        legacy_file_descriptor=lease._legacy_file_descriptor,
        file_identity=(shared.st_dev, shared.st_ino),
        legacy_file_identity=(legacy.st_dev, legacy.st_ino),
        metadata_identity=(metadata.st_dev, metadata.st_ino),
        owner_metadata=_canonical_metadata(owner), pid=os.getpid(),
        release_lock=lease._release_lock,
    )
    with _ACQUIRED_LEASE_PROOFS_LOCK:
        _ACQUIRED_LEASE_PROOFS[lease] = proof


def _require_held_authority_file(
    directory_descriptor: int,
    filename: str,
    identity: tuple[int, int],
    *,
    run_id: str,
) -> None:
    """Probe a separate open description; never relock a retained descriptor."""

    descriptor, details = _authority_file(directory_descriptor, filename, run_id=run_id, label='acquired lock')
    try:
        if (details.st_dev, details.st_ino) != identity or details.st_size != 0:
            raise RunLeaseError(f'The acquired lock inode for {run_id} changed.')
        if _try_lock(descriptor, run_id=run_id):
            # Closing only this fresh descriptor releases its transient probe
            # lock. Acquiring a probe never repairs or authorizes the lease.
            raise RunLeaseError(f'The factory-acquired lock for {run_id} is no longer held.')
    finally:
        os.close(descriptor)


def _require_authority_metadata(
    directory_descriptor: int,
    proof: _AcquiredLeaseProof,
    *,
    run_id: str,
) -> None:
    descriptor, details = _authority_file(
        directory_descriptor, RUN_LEASE_METADATA_FILENAME,
        run_id=run_id, label='run lease owner metadata',
    )
    try:
        if (details.st_dev, details.st_ino) != proof.metadata_identity:
            raise RunLeaseError(f'The acquired owner-metadata inode for {run_id} changed.')
        observed = _read_owner_metadata_from_descriptor(descriptor, run_id=run_id)
        if _canonical_metadata(observed) != proof.owner_metadata:
            raise RunLeaseError(f'The acquired owner metadata for {run_id} changed.')
    finally:
        os.close(descriptor)


def require_acquired_run_lease(lease: RunLease, run_id: str) -> RunLease:
    """Require this process's original active acquired capability for a run.

    This is an additive authorization boundary, not ``inspect_run_lease`` and
    not a substitute for retaining the lease throughout the caller's operation.
    Validation holds the original release mutex, checks original descriptor,
    path/inode/owner relationships, and expects separate lock probes to block.
    It never relocks/unlocks retained descriptors, changes owner metadata, or
    creates/deletes/repairs lease files. Existing constructors remain source
    compatible, but manually constructed/copied objects have no authority.
    """

    _require_posix_support()
    try:
        run_id = _validate_run_id(run_id)
    except ValueError as exc:
        raise RunLeaseError('An exact run ID is required for acquired lease authority.') from exc
    if type(lease) is not RunLease:
        raise RunLeaseError('An original factory-acquired RunLease is required.')
    with _ACQUIRED_LEASE_PROOFS_LOCK:
        proof = _ACQUIRED_LEASE_PROOFS.get(lease)
    if proof is None:
        raise RunLeaseError('A manually constructed or copied RunLease has no acquired authority.')
    with proof.release_lock:
        if lease._release_lock is not proof.release_lock or lease._released is not False:
            raise RunLeaseError('The acquired run lease is released or its synchronization changed.')
        if proof.pid != os.getpid() or type(lease.owner) is not RunLeaseOwner:
            raise RunLeaseError('Acquired lease authority belongs to another process or owner.')
        try:
            owner = _parse_owner_metadata(_canonical_metadata(lease.owner), expected_run_id=run_id)
        except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
            raise RunLeaseError('The retained acquired lease owner is invalid.') from exc
        if owner.pid != os.getpid() or _canonical_metadata(owner) != proof.owner_metadata:
            raise RunLeaseError('The acquired lease owner or run identity changed.')
        if (
            not isinstance(lease.path, Path) or lease.path != proof.path
            or lease.path.name != RUN_LEASE_FILENAME or lease.path.parent.name != run_id
        ):
            raise RunLeaseError('The acquired lease path conflicts with its exact run identity.')
        for descriptor, original, identity, label in (
            (lease._file_descriptor, proof.file_descriptor, proof.file_identity, 'run lease'),
            (lease._legacy_file_descriptor, proof.legacy_file_descriptor, proof.legacy_file_identity, 'legacy qualification lock'),
        ):
            if type(descriptor) is not int or descriptor < 0 or descriptor != original:
                raise RunLeaseError(f'The retained {label} descriptor changed.')
            details = _validate_private_regular_file(descriptor, run_id=run_id, label=label)
            if (details.st_dev, details.st_ino) != identity or details.st_size != 0:
                raise RunLeaseError(f'The retained {label} inode changed.')
        try:
            if lease.path.resolve(strict=True) != proof.canonical_path:
                raise RunLeaseError('The acquired lease canonical path changed.')
            directory_descriptor, _path = _open_run_directory(proof.canonical_path.parent.parent, run_id)
        except (OSError, RuntimeError) as exc:
            if isinstance(exc, RunLeaseError):
                raise
            raise RunLeaseError('The acquired lease canonical path cannot be verified.') from exc
        try:
            directory = os.fstat(directory_descriptor)
            if (directory.st_dev, directory.st_ino) != proof.directory_identity:
                raise RunLeaseError('The acquired run-directory inode changed.')
            _require_authority_metadata(directory_descriptor, proof, run_id=run_id)
            for filename, identity in (
                (RUN_LEASE_FILENAME, proof.file_identity),
                (LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME, proof.legacy_file_identity),
            ):
                _require_held_authority_file(directory_descriptor, filename, identity, run_id=run_id)
            _require_authority_metadata(directory_descriptor, proof, run_id=run_id)
            # A renamed/replaced path must not leave authority pointing at the
            # detached inode observed through an already-open directory FD.
            named_directory = os.stat(proof.canonical_path.parent, follow_symlinks=False)
            if (
                not stat.S_ISDIR(named_directory.st_mode)
                or (named_directory.st_dev, named_directory.st_ino) != proof.directory_identity
            ):
                raise RunLeaseError('The acquired run-directory path changed during validation.')
            for filename, identity in (
                (RUN_LEASE_FILENAME, proof.file_identity),
                (LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME, proof.legacy_file_identity),
                (RUN_LEASE_METADATA_FILENAME, proof.metadata_identity),
            ):
                named = os.stat(filename, dir_fd=directory_descriptor, follow_symlinks=False)
                if not stat.S_ISREG(named.st_mode) or (named.st_dev, named.st_ino) != identity:
                    raise RunLeaseError('An acquired authority path changed during validation.')
        except OSError as exc:
            raise RunLeaseError('The acquired lease filesystem identity cannot be verified.') from exc
        finally:
            os.close(directory_descriptor)
    return lease


def _require_posix_support() -> None:
    if os.name != 'posix' or fcntl is None:
        raise RunLeaseError('Run leases require POSIX flock support.')
    for name in ('O_DIRECTORY', 'O_NOFOLLOW'):
        if not hasattr(os, name):
            raise RunLeaseError(f'Run leases require POSIX {name} support.')
    for name in ('pread', 'pwrite', 'replace'):
        if not hasattr(os, name):
            raise RunLeaseError(f'Run leases require POSIX os.{name} support.')


def _validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or not _RUN_ID_RE.fullmatch(run_id):
        raise ValueError(
            'Run lease IDs must be exactly 12 lowercase hexadecimal '
            'characters.'
        )
    return run_id


def _validate_owner_label(owner_label: str) -> str:
    if (
        not isinstance(owner_label, str)
        or not _OWNER_LABEL_RE.fullmatch(owner_label)
    ):
        raise ValueError(
            'Run lease owner labels must start with a lowercase letter and '
            f'contain at most {MAX_RUN_LEASE_OWNER_LABEL_CHARS} lowercase '
            'letters, digits, dots, underscores, or hyphens.'
        )
    return owner_label


def _open_run_directory(runs_root: Path | str, run_id: str) -> tuple[int, Path]:
    _require_posix_support()
    path = Path(runs_root) / _validate_run_id(run_id)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RunLeaseError(
            f'Unable to open the exact run directory for {run_id}: {exc}'
        ) from exc
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISDIR(details.st_mode):
            raise RunLeaseError(
                f'The run lease path for {run_id} is not a directory.'
            )
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor, path


def _validate_private_regular_file(
    descriptor: int,
    *,
    run_id: str,
    label: str,
) -> os.stat_result:
    try:
        details = os.fstat(descriptor)
    except OSError as exc:
        raise RunLeaseError(
            f'Unable to inspect the {label} for {run_id}: {exc}'
        ) from exc
    if not stat.S_ISREG(details.st_mode):
        raise RunLeaseError(f'The {label} for {run_id} is not a regular file.')
    if details.st_nlink != 1:
        raise RunLeaseError(f'The {label} for {run_id} has an unsafe link count.')
    if hasattr(os, 'geteuid') and details.st_uid != os.geteuid():
        raise RunLeaseError(
            f'The {label} for {run_id} is not owned by the current user.'
        )
    if stat.S_IMODE(details.st_mode) != 0o600:
        raise RunLeaseError(f'The {label} for {run_id} must have mode 0600.')
    return details


def _open_existing_file(
    directory_descriptor: int,
    filename: str,
    *,
    run_id: str,
    label: str,
) -> int | None:
    try:
        descriptor = os.open(
            filename,
            os.O_RDWR | os.O_NOFOLLOW,
            dir_fd=directory_descriptor,
        )
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise RunLeaseError(
            f'Unable to open the {label} for {run_id}: {exc}'
        ) from exc
    try:
        _validate_private_regular_file(
            descriptor,
            run_id=run_id,
            label=label,
        )
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _open_or_create_lock(
    directory_descriptor: int,
    *,
    run_id: str,
) -> int:
    descriptor = _open_existing_file(
        directory_descriptor,
        RUN_LEASE_FILENAME,
        run_id=run_id,
        label='run lease',
    )
    if descriptor is None:
        # Owner metadata without its stable lock inode is ambiguous. Check
        # before creating anything so acquisition cannot silently heal it.
        metadata = _open_existing_file(
            directory_descriptor,
            RUN_LEASE_METADATA_FILENAME,
            run_id=run_id,
            label='run lease owner metadata',
        )
        if metadata is not None:
            os.close(metadata)
            raise RunLeaseError(
                f'Run lease owner metadata exists without its lock for '
                f'{run_id}.'
            )
        try:
            descriptor = os.open(
                RUN_LEASE_FILENAME,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory_descriptor,
            )
        except FileExistsError:
            descriptor = _open_existing_file(
                directory_descriptor,
                RUN_LEASE_FILENAME,
                run_id=run_id,
                label='run lease',
            )
            if descriptor is None:
                raise RunLeaseError(
                    f'The run lease for {run_id} disappeared while opening it.'
                )
        except OSError as exc:
            raise RunLeaseError(
                f'Unable to create the run lease for {run_id}: {exc}'
            ) from exc
        else:
            try:
                os.fchmod(descriptor, 0o600)
            except OSError as exc:
                os.close(descriptor)
                raise RunLeaseError(
                    f'Unable to secure the run lease for {run_id}: {exc}'
                ) from exc
    try:
        details = _validate_private_regular_file(
            descriptor,
            run_id=run_id,
            label='run lease',
        )
        if details.st_size != 0:
            raise RunLeaseError(
                f'The stable run lease for {run_id} must be empty.'
            )
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _validate_legacy_lock_file(
    descriptor: int,
    *,
    run_id: str,
) -> os.stat_result:
    """Validate the identity invariants supported by the historical lock.

    The old qualifier created this file through ``Path.open('a+')``, so its
    mode depended on the operator's umask and was commonly 0644. We accept
    that historical mode, then tighten it to 0600 only after obtaining the
    lock. Symlinks, hard links, foreign ownership, and file contents remain
    ambiguous and fail closed.
    """

    try:
        details = os.fstat(descriptor)
    except OSError as exc:
        raise RunLeaseError(
            f'Unable to inspect the legacy qualification lock for '
            f'{run_id}: {exc}'
        ) from exc
    if not stat.S_ISREG(details.st_mode):
        raise RunLeaseError(
            f'The legacy qualification lock for {run_id} is not a regular '
            'file.'
        )
    if details.st_nlink != 1:
        raise RunLeaseError(
            f'The legacy qualification lock for {run_id} has an unsafe link '
            'count.'
        )
    if hasattr(os, 'geteuid') and details.st_uid != os.geteuid():
        raise RunLeaseError(
            f'The legacy qualification lock for {run_id} is not owned by '
            'the current user.'
        )
    if details.st_size != 0:
        raise RunLeaseError(
            f'The legacy qualification lock for {run_id} must be empty.'
        )
    return details


def _open_existing_legacy_lock(
    directory_descriptor: int,
    *,
    run_id: str,
) -> int | None:
    try:
        descriptor = os.open(
            LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME,
            os.O_RDWR | os.O_NOFOLLOW,
            dir_fd=directory_descriptor,
        )
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise RunLeaseError(
            f'Unable to open the legacy qualification lock for '
            f'{run_id}: {exc}'
        ) from exc
    try:
        _validate_legacy_lock_file(descriptor, run_id=run_id)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _open_or_create_legacy_lock(
    directory_descriptor: int,
    *,
    run_id: str,
) -> int:
    descriptor = _open_existing_legacy_lock(
        directory_descriptor,
        run_id=run_id,
    )
    if descriptor is None:
        try:
            descriptor = os.open(
                LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory_descriptor,
            )
        except FileExistsError:
            descriptor = _open_existing_legacy_lock(
                directory_descriptor,
                run_id=run_id,
            )
            if descriptor is None:
                raise RunLeaseError(
                    f'The legacy qualification lock for {run_id} '
                    'disappeared while opening it.'
                )
        except OSError as exc:
            raise RunLeaseError(
                f'Unable to create the legacy qualification lock for '
                f'{run_id}: {exc}'
            ) from exc
    try:
        _validate_legacy_lock_file(descriptor, run_id=run_id)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _canonical_metadata(owner: RunLeaseOwner) -> bytes:
    value = (
        json.dumps(
            owner.as_dict(),
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
        )
        + '\n'
    ).encode('ascii')
    if len(value) > MAX_RUN_LEASE_METADATA_BYTES:
        raise RunLeaseError('The generated run lease metadata is too large.')
    return value


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f'duplicate field {key!r}')
        value[key] = item
    return value


def _parse_owner_metadata(value: bytes, *, expected_run_id: str) -> RunLeaseOwner:
    if not value or len(value) > MAX_RUN_LEASE_METADATA_BYTES:
        raise RunLeaseError(
            f'The run lease metadata for {expected_run_id} has an invalid size.'
        )
    try:
        decoded = value.decode('utf-8')
        metadata = json.loads(decoded, object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise RunLeaseError(
            f'The run lease metadata for {expected_run_id} is invalid.'
        ) from exc
    if not isinstance(metadata, Mapping) or set(metadata) != _METADATA_FIELDS:
        raise RunLeaseError(
            f'The run lease metadata schema for {expected_run_id} is invalid.'
        )
    try:
        run_id = _validate_run_id(metadata['run_id'])
        owner_label = _validate_owner_label(metadata['owner_label'])
    except ValueError as exc:
        raise RunLeaseError(
            f'The run lease identity for {expected_run_id} is invalid.'
        ) from exc
    if run_id != expected_run_id:
        raise RunLeaseError(
            f'The run lease metadata does not belong to {expected_run_id}.'
        )
    schema_version = metadata['schema_version']
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version != RUN_LEASE_SCHEMA_VERSION
    ):
        raise RunLeaseError(
            f'The run lease schema for {expected_run_id} is unsupported.'
        )
    pid = metadata['pid']
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise RunLeaseError(
            f'The run lease PID for {expected_run_id} is invalid.'
        )
    acquired_at = metadata['acquired_at']
    if not isinstance(acquired_at, str) or len(acquired_at) > 40:
        raise RunLeaseError(
            f'The run lease timestamp for {expected_run_id} is invalid.'
        )
    try:
        parsed_timestamp = datetime.fromisoformat(acquired_at)
    except ValueError as exc:
        raise RunLeaseError(
            f'The run lease timestamp for {expected_run_id} is invalid.'
        ) from exc
    if (
        parsed_timestamp.tzinfo is None
        or parsed_timestamp.utcoffset() != timedelta(0)
        or parsed_timestamp.isoformat() != acquired_at
    ):
        raise RunLeaseError(
            f'The run lease timestamp for {expected_run_id} is invalid.'
        )
    owner = RunLeaseOwner(
        run_id=run_id,
        owner_label=owner_label,
        pid=pid,
        acquired_at=acquired_at,
        schema_version=schema_version,
    )
    if value != _canonical_metadata(owner):
        raise RunLeaseError(
            f'The run lease metadata for {expected_run_id} is not canonical.'
        )
    return owner


def _read_owner_metadata_from_descriptor(
    descriptor: int,
    *,
    run_id: str,
) -> RunLeaseOwner:
    details = _validate_private_regular_file(
        descriptor,
        run_id=run_id,
        label='run lease owner metadata',
    )
    if not 0 < details.st_size <= MAX_RUN_LEASE_METADATA_BYTES:
        raise RunLeaseError(
            f'The run lease metadata for {run_id} has an invalid size.'
        )
    try:
        value = os.pread(descriptor, details.st_size, 0)
    except OSError as exc:
        raise RunLeaseError(
            f'Unable to read the run lease metadata for {run_id}: {exc}'
        ) from exc
    if len(value) != details.st_size:
        raise RunLeaseError(
            f'The run lease metadata for {run_id} changed while being read.'
        )
    return _parse_owner_metadata(value, expected_run_id=run_id)


def _read_owner_metadata(
    directory_descriptor: int,
    *,
    run_id: str,
    required: bool,
) -> RunLeaseOwner | None:
    descriptor = _open_existing_file(
        directory_descriptor,
        RUN_LEASE_METADATA_FILENAME,
        run_id=run_id,
        label='run lease owner metadata',
    )
    if descriptor is None:
        if required:
            raise RunLeaseError(
                f'The held run lease for {run_id} has no owner metadata.'
            )
        return None
    try:
        return _read_owner_metadata_from_descriptor(descriptor, run_id=run_id)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, value: bytes) -> None:
    offset = 0
    while offset < len(value):
        written = os.pwrite(descriptor, value[offset:], offset)
        if written <= 0:
            raise OSError('run lease metadata write made no progress')
        offset += written


def _unlink_temporary_metadata(
    directory_descriptor: int,
    temporary_name: str,
) -> None:
    try:
        os.unlink(temporary_name, dir_fd=directory_descriptor)
    except FileNotFoundError:
        pass
    except OSError:
        # The canonical lock and metadata remain unchanged. Never weaken the
        # primary error or unlink either canonical path as a workaround.
        pass


def _replace_owner_metadata(
    directory_descriptor: int,
    owner: RunLeaseOwner,
) -> None:
    value = _canonical_metadata(owner)
    temporary_name = (
        f'.{RUN_LEASE_METADATA_FILENAME}.{uuid.uuid4().hex}.tmp'
    )
    descriptor: int | None = None
    replaced = False
    try:
        try:
            descriptor = os.open(
                temporary_name,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory_descriptor,
            )
            os.fchmod(descriptor, 0o600)
            _write_all(descriptor, value)
            os.fsync(descriptor)
            observed = _read_owner_metadata_from_descriptor(
                descriptor,
                run_id=owner.run_id,
            )
            if observed != owner:
                raise RunLeaseError(
                    f'The prepared run lease metadata for {owner.run_id} '
                    'changed before publication.'
                )
        except RunLeaseError:
            raise
        except OSError as exc:
            raise RunLeaseError(
                f'Unable to prepare run lease metadata for '
                f'{owner.run_id}: {exc}'
            ) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
                descriptor = None

        try:
            os.replace(
                temporary_name,
                RUN_LEASE_METADATA_FILENAME,
                src_dir_fd=directory_descriptor,
                dst_dir_fd=directory_descriptor,
            )
            replaced = True
            os.fsync(directory_descriptor)
        except OSError as exc:
            raise RunLeaseError(
                f'Unable to publish run lease metadata for '
                f'{owner.run_id}: {exc}'
            ) from exc

        observed = _read_owner_metadata(
            directory_descriptor,
            run_id=owner.run_id,
            required=True,
        )
        if observed != owner:
            raise RunLeaseError(
                f'The published run lease metadata for {owner.run_id} changed.'
            )
    finally:
        # Before replacement this removes only our private staging file. After
        # replacement that name no longer exists, leaving the owner sidecar.
        if not replaced:
            _unlink_temporary_metadata(directory_descriptor, temporary_name)


def _try_lock(descriptor: int, *, run_id: str) -> bool:
    _require_posix_support()
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError as exc:
        if exc.errno in {errno.EACCES, errno.EAGAIN}:
            return False
        raise RunLeaseError(
            f'Unable to determine whether run {run_id} is leased: {exc}'
        ) from exc


def _inspect_shared_lease(
    directory_descriptor: int,
    *,
    run_id: str,
) -> RunLeaseStatus:
    """Inspect only the current shared lock while its run directory is open."""

    descriptor: int | None = None
    locked = False
    try:
        descriptor = _open_existing_file(
            directory_descriptor,
            RUN_LEASE_FILENAME,
            run_id=run_id,
            label='run lease',
        )
        if descriptor is None:
            metadata = _open_existing_file(
                directory_descriptor,
                RUN_LEASE_METADATA_FILENAME,
                run_id=run_id,
                label='run lease owner metadata',
            )
            if metadata is not None:
                os.close(metadata)
                raise RunLeaseError(
                    f'Run lease owner metadata exists without its lock for '
                    f'{run_id}.'
                )
            return RunLeaseStatus(held=False, owner=None)

        details = _validate_private_regular_file(
            descriptor,
            run_id=run_id,
            label='run lease',
        )
        if details.st_size != 0:
            raise RunLeaseError(
                f'The stable run lease for {run_id} must be empty.'
            )
        locked = _try_lock(descriptor, run_id=run_id)
        owner = _read_owner_metadata(
            directory_descriptor,
            run_id=run_id,
            required=not locked,
        )
        if locked:
            return RunLeaseStatus(held=False, owner=None)
        return RunLeaseStatus(held=True, owner=owner)
    finally:
        if descriptor is not None:
            if locked:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                except OSError as exc:
                    os.close(descriptor)
                    raise RunLeaseError(
                        f'Unable to release the inspected lease for '
                        f'{run_id}: {exc}'
                    ) from exc
            os.close(descriptor)


def acquire_run_lease(
    runs_root: Path | str,
    run_id: str,
    owner_label: str,
) -> RunLease:
    """Acquire one run's exclusive lease without waiting."""

    run_id = _validate_run_id(run_id)
    owner_label = _validate_owner_label(owner_label)
    directory_descriptor, run_directory = _open_run_directory(
        runs_root,
        run_id,
    )
    descriptor: int | None = None
    legacy_descriptor: int | None = None
    locked = False
    legacy_locked = False
    try:
        # Lock the migration guard first. Historical qualifier binaries know
        # only this filename, so merely checking it and then taking the new
        # lock would leave a split-brain race.
        legacy_descriptor = _open_or_create_legacy_lock(
            directory_descriptor,
            run_id=run_id,
        )
        legacy_locked = _try_lock(legacy_descriptor, run_id=run_id)
        if not legacy_locked:
            # When another current-process owner holds both locks, preserve
            # the rich owner error expected by callers. A legacy-only holder
            # has no trustworthy metadata, but still fails closed.
            shared_status = _inspect_shared_lease(
                directory_descriptor,
                run_id=run_id,
            )
            if shared_status.held and shared_status.owner is not None:
                raise RunLeaseHeldError(shared_status.owner)
            raise RunLeaseError(
                f'Run {run_id} is already owned through the legacy Azure '
                'qualification lock.'
            )
        try:
            os.fchmod(legacy_descriptor, 0o600)
        except OSError as exc:
            raise RunLeaseError(
                f'Unable to secure the legacy qualification lock for '
                f'{run_id}: {exc}'
            ) from exc

        descriptor = _open_or_create_lock(
            directory_descriptor,
            run_id=run_id,
        )
        locked = _try_lock(descriptor, run_id=run_id)
        if not locked:
            owner = _read_owner_metadata(
                directory_descriptor,
                run_id=run_id,
                required=True,
            )
            raise RunLeaseHeldError(owner)

        # Missing metadata on an unlocked valid lock is a recoverable first
        # acquisition or pre-publication crash. Malformed metadata is not.
        _read_owner_metadata(
            directory_descriptor,
            run_id=run_id,
            required=False,
        )
        owner = RunLeaseOwner(
            run_id=run_id,
            owner_label=owner_label,
            pid=os.getpid(),
            acquired_at=datetime.now(timezone.utc).isoformat(),
        )
        _replace_owner_metadata(directory_descriptor, owner)
        lease = RunLease(
            file_descriptor=descriptor,
            legacy_file_descriptor=legacy_descriptor,
            path=run_directory / RUN_LEASE_FILENAME,
            owner=owner,
        )
        _record_acquired_lease(lease, directory_descriptor)
        descriptor = None
        legacy_descriptor = None
        locked = False
        legacy_locked = False
        return lease
    except BaseException:
        if descriptor is not None:
            if locked:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                except OSError:
                    pass
            os.close(descriptor)
        if legacy_descriptor is not None:
            if legacy_locked:
                try:
                    fcntl.flock(legacy_descriptor, fcntl.LOCK_UN)
                except OSError:
                    pass
            os.close(legacy_descriptor)
        raise
    finally:
        os.close(directory_descriptor)


def inspect_run_lease(
    runs_root: Path | str,
    run_id: str,
) -> RunLeaseStatus:
    """Observe whether a run lease is held at this instant."""

    run_id = _validate_run_id(run_id)
    directory_descriptor, _run_directory = _open_run_directory(
        runs_root,
        run_id,
    )
    legacy_descriptor: int | None = None
    legacy_locked = False
    try:
        legacy_descriptor = _open_existing_legacy_lock(
            directory_descriptor,
            run_id=run_id,
        )
        if legacy_descriptor is not None:
            legacy_locked = _try_lock(
                legacy_descriptor,
                run_id=run_id,
            )
            if not legacy_locked:
                # A current owner will also have its validated shared owner
                # metadata. An old qualifier has none, so report an anonymous
                # held lease rather than inventing an identity or PID.
                shared_status = _inspect_shared_lease(
                    directory_descriptor,
                    run_id=run_id,
                )
                if shared_status.held:
                    return shared_status
                return RunLeaseStatus(held=True, owner=None)
        return _inspect_shared_lease(
            directory_descriptor,
            run_id=run_id,
        )
    finally:
        if legacy_descriptor is not None:
            if legacy_locked:
                try:
                    fcntl.flock(legacy_descriptor, fcntl.LOCK_UN)
                except OSError as exc:
                    os.close(legacy_descriptor)
                    os.close(directory_descriptor)
                    raise RunLeaseError(
                        f'Unable to release the inspected legacy '
                        f'qualification lock for {run_id}: {exc}'
                    ) from exc
            os.close(legacy_descriptor)
        os.close(directory_descriptor)


__all__ = [
    'LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME',
    'MAX_RUN_LEASE_METADATA_BYTES',
    'MAX_RUN_LEASE_OWNER_LABEL_CHARS',
    'RUN_LEASE_FILENAME',
    'RUN_LEASE_METADATA_FILENAME',
    'RUN_LEASE_SCHEMA_VERSION',
    'RunLease',
    'RunLeaseError',
    'RunLeaseHeldError',
    'RunLeaseOwner',
    'RunLeaseStatus',
    'acquire_run_lease',
    'inspect_run_lease',
    'require_acquired_run_lease',
]
