"""Additional immutable Hotel attempt identity in private candidate storage."""

from __future__ import annotations

from pathlib import Path

from app.deathstarbench_hotel_initialization_operator import (
    PIN_KEY, validate_hotel_initialization_pin,
)
from scripts.deathstarbench_candidate_operator_state import (
    CandidateStateStore, CandidateStateError, _atomic_write, _atomic_write_raw,
    _read, _read_raw, canonical,
)
import hashlib


class HotelInitializationStateStore(CandidateStateStore):
    """Support pin and guarded attempt pin remain separate immutable files.

    The original support operator's on-disk schema is unchanged. Every Hotel
    save and load independently rechecks both exact pin files under its lease.
    No serialized image-access or execution capability can be restored here.
    """

    def publish_hotel_pin(self, pin, lease):
        directory = self.require_lease(pin['run_id'], lease)
        validate_hotel_initialization_pin(pin, self.pin(pin['run_id'], lease), cleanup_only=True)
        path = directory / 'hotel-initialization-pin.json'
        if path.exists() or path.is_symlink():
            raise CandidateStateError('An immutable Hotel attempt pin cannot be replaced.')
        _atomic_write(path, pin)

    def hotel_pin(self, run_id, lease):
        directory = self.require_lease(run_id, lease)
        value = _read(directory / 'hotel-initialization-pin.json')
        return validate_hotel_initialization_pin(value, self.pin(run_id, lease), cleanup_only=True)

    def _require_hotel_pin(self, job, lease):
        pin = self.hotel_pin(job['id'], lease)
        if canonical(job.get(PIN_KEY)) != canonical(pin):
            raise CandidateStateError('Hotel initialization attempt differs from its immutable pin file.')

    def save(self, job, lease):
        self._require_hotel_pin(job, lease)
        return super().save(job, lease)

    def load(self, run_id, lease):
        job = super().load(run_id, lease)
        self._require_hotel_pin(job, lease)
        return job

    def publish_guarded_artifact_input(self, run_id, filename, source, expected_sha256, lease):
        if filename not in ('guarded-image-lock.json', 'guarded-publication.json'):
            raise CandidateStateError('Unknown guarded Hotel evidence filename.')
        directory = self.require_lease(run_id, lease)
        path = directory / filename
        raw = _read_raw(Path(source), private=False)
        if hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise CandidateStateError('Guarded artifact input changed after fresh adoption.')
        if path.exists() or path.is_symlink():
            if _read_raw(path) != raw:
                raise CandidateStateError('Immutable guarded artifact input differs from saved bytes.')
            return path
        _atomic_write_raw(path, raw)
        _read(path)
        return path
