"""Private, lease-protected state for support-only candidate operators.

These records live one level below the public run directory. They are not
benchmark results and are never registered with the web application's jobs.
Only JSON identities/evidence are saved; private SSH credentials stay local.
Public authorized keys may legitimately occur in a provider ownership graph.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid

from app.run_lease import RunLease, acquire_run_lease, require_acquired_run_lease


MAX_STATE_BYTES = 64 * 1024 * 1024
_RUN_ID = re.compile(r'[0-9a-f]{12}\Z')


class CandidateStateError(ValueError):
    """Private candidate state cannot be safely loaded or published."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n'


def fingerprint(value):
    return 'sha256:' + hashlib.sha256(canonical(value).encode()).hexdigest()


def _object_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CandidateStateError('Duplicate private state field.')
        result[key] = value
    return result


def _private_directory(path, *, create=False):
    if create:
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
    details = path.lstat()
    if (not stat.S_ISDIR(details.st_mode) or details.st_uid != os.geteuid()
            or stat.S_IMODE(details.st_mode) != 0o700):
        raise CandidateStateError('Candidate state directory must be private and owned, not a symlink.')
    return path


def _read_raw(path, *, private=True):
    details = path.lstat()
    if (not stat.S_ISREG(details.st_mode)
            or (private and (details.st_uid != os.geteuid() or stat.S_IMODE(details.st_mode) != 0o600))
            or details.st_nlink != 1
            or not 0 < details.st_size <= MAX_STATE_BYTES):
        raise CandidateStateError('Private candidate JSON must be a bounded owned 0600 regular file.')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        if (opened.st_dev, opened.st_ino) != (details.st_dev, details.st_ino):
            raise CandidateStateError('Private candidate JSON changed while opening.')
        raw = stream.read(MAX_STATE_BYTES + 1)
    if len(raw) != details.st_size or len(raw) > MAX_STATE_BYTES or raw.startswith(b'\xef\xbb\xbf'):
        raise CandidateStateError('Private candidate JSON changed or is oversized.')
    return raw


def _read(path):
    raw = _read_raw(path)
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=_object_pairs,
                           parse_constant=lambda _value: (_ for _ in ()).throw(CandidateStateError('Non-finite JSON.')))
        canonical(value).encode('utf-8')
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise CandidateStateError('Private candidate JSON is invalid.') from exc
    return value


def _atomic_write_raw(path, raw):
    if not 0 < len(raw) <= MAX_STATE_BYTES:
        raise CandidateStateError('Private candidate state is oversized.')
    if path.exists() or path.is_symlink():
        _read(path)  # Never replace a foreign/link/special file as a repair.
    temporary = path.parent / ('.' + path.name + '-' + uuid.uuid4().hex + '.tmp')
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_write(path, value):
    _atomic_write_raw(path, canonical(value).encode('utf-8'))


class CandidateStateStore:
    def __init__(self, root: Path):
        self.root = _private_directory(Path(root), create=True)

    def directory(self, run_id):
        if not isinstance(run_id, str) or not _RUN_ID.fullmatch(run_id):
            raise CandidateStateError('Candidate run IDs must be twelve lowercase hexadecimal digits.')
        return _private_directory(self.root / run_id)

    def require_lease(self, run_id, lease: RunLease):
        require_acquired_run_lease(lease, run_id)
        directory = self.directory(run_id)
        if lease.path.parent.resolve(strict=True) != directory.resolve(strict=True):
            raise CandidateStateError('The lease belongs to another candidate state store.')
        return directory

    def allocate(self):
        for _attempt in range(10):
            run_id = uuid.uuid4().hex[:12]
            # Avoid sharing deterministic cloud resource names with a public run.
            if (self.root.parent / run_id).exists():
                continue
            directory = self.root / run_id
            try:
                directory.mkdir(mode=0o700)
            except FileExistsError:
                continue
            lease = acquire_run_lease(self.root, run_id, owner_label='candidate-support-operator')
            return run_id, lease
        raise CandidateStateError('Unable to allocate an unused private candidate run ID.')

    def acquire(self, run_id):
        self.directory(run_id)
        return acquire_run_lease(self.root, run_id, owner_label='candidate-support-operator')

    def publish_pin(self, pin, lease):
        directory = self.require_lease(pin['run_id'], lease)
        path = directory / 'pin.json'
        if path.exists() or path.is_symlink():
            raise CandidateStateError('An immutable candidate pin cannot be replaced.')
        _atomic_write(path, pin)

    def pin(self, run_id, lease):
        directory = self.require_lease(run_id, lease)
        value = _read(directory / 'pin.json')
        if not isinstance(value, dict) or value.get('run_id') != run_id:
            raise CandidateStateError('Private candidate pin crossed its run identity.')
        return value

    def save(self, job, lease):
        directory = self.require_lease(job['id'], lease)
        pin = self.pin(job['id'], lease)
        if (canonical(job.get('candidate_support_pin')) != canonical(pin)
                or canonical(job.get('plan')) != canonical(pin.get('infrastructure_plan'))):
            raise CandidateStateError('Candidate pin or infrastructure plan changed before persistence.')
        if any(key in job['plan'] for key in ('ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase')):
            raise CandidateStateError('SSH material must not enter the private plan snapshot.')
        value = {key: item for key, item in job.items() if not key.startswith('_')}
        _atomic_write(directory / 'state.json', value)
        self.require_lease(job['id'], lease)

    def load(self, run_id, lease):
        directory = self.require_lease(run_id, lease)
        value = _read(directory / 'state.json')
        pin = self.pin(run_id, lease)
        if (not isinstance(value, dict) or value.get('id') != run_id
                or canonical(value.get('candidate_support_pin')) != canonical(pin)
                or canonical(value.get('plan')) != canonical(pin.get('infrastructure_plan'))):
            raise CandidateStateError('Private candidate state/pin ownership differs.')
        if any(key.startswith('_') for key in value):
            raise CandidateStateError('Process-local material was persisted in candidate state.')
        return value

    def publish_artifact_input(self, run_id, filename, source, expected_sha256, lease):
        if filename not in ('candidate-image-lock.json', 'publication.json'):
            raise CandidateStateError('Unknown candidate evidence filename.')
        directory = self.require_lease(run_id, lease)
        path = directory / filename
        raw = _read_raw(Path(source), private=False)
        if hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise CandidateStateError('Artifact input bytes changed after full adoption.')
        if path.exists() or path.is_symlink():
            if _read_raw(path) != raw:
                raise CandidateStateError('Immutable artifact input differs from the saved candidate copy.')
            return path
        _atomic_write_raw(path, raw)
        _read(path)
        return path
