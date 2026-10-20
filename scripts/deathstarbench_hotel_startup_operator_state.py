"""Private startup pins and independent, cleanup-only ownership persistence.

Execution history is a diagnostic projection, never an authority source. The
provider bridge validates native facts before they enter this store. Immutable
installation uses link(2)'s no-clobber final-name operation, not replace(2).
"""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import stat
import uuid

from app import deathstarbench_candidate_operator as support
from app import deathstarbench_hotel_initialization_operator as initialization
from app import deathstarbench_hotel_startup_operator as startup
from app import deathstarbench_hotel_startup_ownership as ownership
from app.deathstarbench_hotel_startup_ownership import project_hotel_startup_job
from scripts.deathstarbench_hotel_initialization_operator_state import HotelInitializationStateStore
from scripts.deathstarbench_candidate_operator_state import (
    CandidateStateError, _atomic_write, _object_pairs, canonical,
)

MAX_STARTUP_FILE_BYTES = 1024 * 1024
CHECKPOINT_FILE = 'hotel-startup-owned-graph.json'
OWNERSHIP_FILE = 'hotel-startup-cleanup-ownership.json'
OWNERSHIP_KEY = 'hotel_startup_cleanup_ownership'


def _bounded_read(path):
    details = path.lstat()
    if (not stat.S_ISREG(details.st_mode) or details.st_uid != os.geteuid()
            or stat.S_IMODE(details.st_mode) != 0o600 or details.st_nlink != 1
            or not 0 < details.st_size <= MAX_STARTUP_FILE_BYTES):
        raise CandidateStateError('Startup state must be a bounded owned 0600 regular file.')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        if (opened.st_dev, opened.st_ino) != (details.st_dev, details.st_ino):
            raise CandidateStateError('Startup state changed while opening.')
        raw = stream.read(MAX_STARTUP_FILE_BYTES + 1)
    if len(raw) != details.st_size or len(raw) > MAX_STARTUP_FILE_BYTES or raw.startswith(b'\xef\xbb\xbf'):
        raise CandidateStateError('Startup state changed or exceeds its bound.')
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=_object_pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        _bounded_bytes(value)
        return value
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise CandidateStateError('Invalid private startup JSON.') from exc


def _bounded_bytes(value):
    raw = canonical(value).encode('utf-8')
    if not 0 < len(raw) <= MAX_STARTUP_FILE_BYTES:
        raise CandidateStateError('Startup state exceeds its private file bound.')
    return raw


def _sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _install_immutable(path, value):
    """Fully write first; atomically link the final name without replacement."""
    raw = _bounded_bytes(value)
    temporary = path.parent / ('.' + path.name + '-' + uuid.uuid4().hex + '.tmp')
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path, follow_symlinks=False)
    finally:
        temporary.unlink(missing_ok=True)
    _sync_directory(path.parent)
    if canonical(_bounded_read(path)) != canonical(value):
        raise CandidateStateError('Immutable startup publication did not read back exactly.')


class HotelStartupStateStore(HotelInitializationStateStore):
    def __init__(self, root):
        super().__init__(root)
        self._initial_checkpoint_runs = set()

    def publish_pin(self, pin, lease):
        directory = self.require_lease(pin['run_id'], lease)
        _install_immutable(directory / 'pin.json', pin)

    def pin(self, run_id, lease):
        directory = self.require_lease(run_id, lease)
        value = _bounded_read(directory / 'pin.json')
        if type(value) is not dict or value.get('run_id') != run_id:
            raise CandidateStateError('Startup support pin crossed its run identity.')
        return value

    def publish_hotel_pin(self, pin, lease):
        directory = self.require_lease(pin['run_id'], lease)
        initialization.validate_hotel_initialization_pin(pin, self.pin(pin['run_id'], lease), cleanup_only=True)
        _install_immutable(directory / 'hotel-initialization-pin.json', pin)

    def hotel_pin(self, run_id, lease):
        directory = self.require_lease(run_id, lease)
        return initialization.validate_hotel_initialization_pin(
            _bounded_read(directory / 'hotel-initialization-pin.json'), self.pin(run_id, lease), cleanup_only=True)

    def publish_startup_pin(self, pin, lease):
        directory = self.require_lease(pin['run_id'], lease)
        startup.validate_hotel_startup_pin(pin, self.pin(pin['run_id'], lease), self.hotel_pin(pin['run_id'], lease))
        _install_immutable(directory / 'hotel-startup-pin.json', pin)
        # Process-local authority only from this successful, exclusive creation.
        # Reopening an old pin must never recreate a missing ownership file.
        self._initial_checkpoint_runs.add(pin['run_id'])

    def startup_pin(self, run_id, lease):
        directory = self.require_lease(run_id, lease)
        return startup.validate_hotel_startup_pin(_bounded_read(directory / 'hotel-startup-pin.json'),
            self.pin(run_id, lease), self.hotel_pin(run_id, lease))

    def _roots(self, run_id, lease):
        pin, initialization_pin, startup_pin = self.pin(run_id, lease), self.hotel_pin(run_id, lease), self.startup_pin(run_id, lease)
        job = {'id': run_id, '_persist_state': False, 'plan': copy.deepcopy(pin['infrastructure_plan']), 'resources': {}, 'results': [],
               support.PIN_KEY: pin, initialization.PIN_KEY: initialization_pin, startup.PIN_KEY: startup_pin}
        support._validate_pin(job, pin, None, cleanup_only=True)
        return job

    def _check_job(self, job, lease):
        roots = self._roots(job['id'], lease)
        if any(canonical(job.get(key)) != canonical(roots[key])
               for key in ('plan', support.PIN_KEY, initialization.PIN_KEY, startup.PIN_KEY)):
            raise CandidateStateError('Startup state differs from its three immutable pin files.')
        return roots

    @staticmethod
    def _bind(job):
        return ownership.bind_hotel_startup_ownership_checkpoint(job,
            support_pin=job[support.PIN_KEY], initialization_pin=job[initialization.PIN_KEY],
            startup_pin=job[startup.PIN_KEY])

    def checkpoint(self, run_id, lease):
        job = self._roots(run_id, lease)
        job[ownership.CHECKPOINT_KEY] = _bounded_read(self.require_lease(run_id, lease) / CHECKPOINT_FILE)
        value = self._bind(job)
        if value['contract_recorded']:
            from scripts.deathstarbench_candidate_provider import validate_startup_provider_checkpoint
            validate_startup_provider_checkpoint(value, job)
            if value['seal_fingerprint'] is not None:
                seal = _bounded_read(self.require_lease(run_id, lease) / OWNERSHIP_FILE)
                ownership.seal_hotel_startup_ownership_checkpoint(value, seal)
                # Flat providers clear their native graph at terminal state.
                # The original sealed graph must remain independently native-
                # valid, not just pass the generic JSON/fingerprint schema.
                original = {**value, 'resources': seal['resources']}
                original['fingerprint'] = ownership._fingerprint({key: item for key, item in original.items() if key != 'fingerprint'})
                validate_startup_provider_checkpoint(original, job)
        return value

    def save(self, job, lease):
        self._save_checkpoint(job, lease, initial=True)
        # Checkpoint publication is deliberately BEFORE this fallible serializer.
        value = project_hotel_startup_job(job)
        _bounded_bytes(value)
        _atomic_write(self.require_lease(job['id'], lease) / 'state.json', value)
        self._check_job(job, lease)

    def _save_checkpoint(self, job, lease, *, initial=False, transition=None, finalizing=False, verified_implicit=None):
        self._check_job(job, lease)
        directory = self.require_lease(job['id'], lease)
        path = directory / CHECKPOINT_FILE
        supplied = self._bind(job)
        if not path.exists() and not path.is_symlink():
            if (not initial or job['id'] not in self._initial_checkpoint_runs or supplied['contract_recorded']
                    or job.get('resources') or (directory / 'state.json').exists()):
                raise CandidateStateError('Missing independent startup checkpoint cannot be reconstructed.')
            self._initial_checkpoint_runs.remove(job['id'])
            _install_immutable(path, supplied)
            return
        previous = self.checkpoint(job['id'], lease)
        seal_path = directory / OWNERSHIP_FILE
        if previous['seal_fingerprint'] is None and (seal_path.exists() or seal_path.is_symlink()):
            self.publish_cleanup_ownership(job['id'], self.cleanup_ownership(job['id'], lease), lease)
            previous = self.checkpoint(job['id'], lease)
        if previous['contract_recorded'] or job.get('resources'):
            from scripts.deathstarbench_candidate_provider import project_startup_provider_ownership
            projection = project_startup_provider_ownership(job, previous, transition=transition, finalizing=finalizing,
                                                            verified_implicit=verified_implicit)
            value = ownership.update_hotel_startup_ownership_checkpoint(previous, projection)
        else:
            if canonical(previous) != canonical(supplied):
                raise CandidateStateError('Unallocated ownership checkpoint changed outside its provider gate.')
            value = previous
        if value['seal_fingerprint'] is not None:
            ownership.seal_hotel_startup_ownership_checkpoint(value, self.cleanup_ownership(job['id'], lease))
        _bounded_bytes(value)
        _atomic_write(path, value)
        if canonical(_bounded_read(path)) != canonical(value):
            raise CandidateStateError('Startup checkpoint did not read back exactly.')
        job[ownership.CHECKPOINT_KEY] = value
        self._check_job(job, lease)

    def save_cleanup_progress(self, job, lease, *, transition=None, finalizing=False, verified_implicit=None):
        self._save_checkpoint(job, lease, transition=transition, finalizing=finalizing, verified_implicit=verified_implicit)

    def load(self, run_id, lease):
        value = _bounded_read(self.require_lease(run_id, lease) / 'state.json')
        self._check_job(value, lease)
        # Do not restore any executable or byte-access capability from JSON.
        value[ownership.CHECKPOINT_KEY] = self.checkpoint(run_id, lease)
        safe = project_hotel_startup_job(value)
        safe['_persist_state'] = False
        safe[ownership.CHECKPOINT_KEY] = value[ownership.CHECKPOINT_KEY]
        return safe

    def cleanup_ownership(self, run_id, lease):
        value = _bounded_read(self.require_lease(run_id, lease) / OWNERSHIP_FILE)
        roots = self._roots(run_id, lease)
        checkpoint = self.checkpoint(run_id, lease)
        seal = ownership.validate_hotel_startup_cleanup_ownership(value, run_id=run_id,
            provider=roots['plan']['provider'])
        ownership.seal_hotel_startup_ownership_checkpoint(checkpoint, seal)
        return seal

    def publish_cleanup_ownership(self, run_id, ownership_value, lease):
        directory = self.require_lease(run_id, lease)
        checkpoint = self.checkpoint(run_id, lease)
        sealed = ownership.seal_hotel_startup_ownership_checkpoint(checkpoint, ownership_value)
        path = directory / OWNERSHIP_FILE
        try:
            _install_immutable(path, ownership_value)
        except FileExistsError:
            if canonical(_bounded_read(path)) != canonical(ownership_value):
                raise CandidateStateError('Immutable startup ownership cannot be replaced.')
            _sync_directory(directory)
        _bounded_bytes(sealed)
        _atomic_write(directory / CHECKPOINT_FILE, sealed)
        self.cleanup_ownership(run_id, lease)

    def load_for_destroy(self, run_id, lease):
        job = self._roots(run_id, lease)
        checkpoint = self.checkpoint(run_id, lease)
        job[ownership.CHECKPOINT_KEY] = checkpoint
        job['resources'] = copy.deepcopy(checkpoint['resources'])
        seal_path = self.require_lease(run_id, lease) / OWNERSHIP_FILE
        if seal_path.exists() or seal_path.is_symlink():
            job[OWNERSHIP_KEY] = self.cleanup_ownership(run_id, lease)
            if checkpoint['seal_fingerprint'] is None:
                # A crash may have installed the immutable seal before writing
                # its checkpoint marker. Reuse that exact seal, never replace it.
                self.publish_cleanup_ownership(run_id, job[OWNERSHIP_KEY], lease)
                job[ownership.CHECKPOINT_KEY] = self.checkpoint(run_id, lease)
        elif checkpoint['seal_fingerprint'] is not None:
            raise CandidateStateError('Missing immutable startup ownership seal.')
        return job
