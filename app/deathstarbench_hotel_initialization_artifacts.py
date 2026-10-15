"""Immutable, separate adoption of guarded Hotel artifact evidence only.

There is deliberately no runtime_manifest, plan factory, serialized authority
constructor, or conversion to the legacy CandidateArtifactBinding here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, Mapping

from app.deathstarbench_candidate_artifacts import (
    CandidateArtifactBinding, CandidateArtifactError, _canonical, _fingerprint,
    _freeze, _read_input, _thaw,
)
from scripts import create_deathstarbench_hotel_initialization_image_lock as validation


ADOPTION_SCHEMA = 'hotel-guarded-initialization-artifact-adoption-v1'
_ADOPTED = object()


class GuardedHotelArtifactError(ValueError):
    """Guarded Hotel evidence was not completely revalidated."""


class _Proof:
    __slots__ = ('metadata_json', 'token')

    def __init__(self, metadata_json, token):
        if token is not _ADOPTED:
            raise GuardedHotelArtifactError('Guarded Hotel adoption proof is factory-only.')
        object.__setattr__(self, 'metadata_json', metadata_json)
        object.__setattr__(self, 'token', token)

    def __setattr__(self, name, value):
        raise GuardedHotelArtifactError('Guarded Hotel adoption proof is immutable.')


@dataclass(frozen=True, slots=True)
class GuardedHotelArtifactBinding:
    """Factory-only evidence identity; never an initializer/runtime capability."""

    app: Mapping[str, Any]
    reused_images: Mapping[str, Any]
    dataset_attestation: Mapping[str, Any]
    fingerprint: str
    _metadata_json: str = field(repr=False)
    _proof: object = field(repr=False, compare=False)
    workload_id: str = field(init=False, default='hotel_reservation')
    candidate_only: bool = field(init=False, default=True)
    released: bool = field(init=False, default=False)
    initializer_authorized: bool = field(init=False, default=False)
    runtime_authorized: bool = field(init=False, default=False)
    initialization_executed: bool = field(init=False, default=False)
    database_executed: bool = field(init=False, default=False)
    application_start_allowed: bool = field(init=False, default=False)
    dataset_qualified: bool = field(init=False, default=False)
    runtime_qualified: bool = field(init=False, default=False)
    measurement_qualified: bool = field(init=False, default=False)
    cloud_qualified: bool = field(init=False, default=False)
    public_release: bool = field(init=False, default=False)
    anonymous_pull_qualified: bool = field(init=False, default=False)

    def __post_init__(self):
        proof = self._proof
        if type(proof) is not _Proof or proof.token is not _ADOPTED or self._metadata_json != proof.metadata_json:
            raise GuardedHotelArtifactError('Guarded Hotel bindings require complete original factory adoption.')
        try:
            metadata = json.loads(proof.metadata_json)
            for name, expected in {
                'workload_id': 'hotel_reservation', 'candidate_only': True,
                **{name: False for name in (
                    'released', 'initializer_authorized', 'runtime_authorized', 'initialization_executed', 'database_executed',
                    'application_start_allowed', 'dataset_qualified',
                    'runtime_qualified', 'measurement_qualified', 'cloud_qualified', 'public_release',
                    'anonymous_pull_qualified',
                )},
            }.items():
                validation._equal(getattr(self, name), expected, 'closed guarded binding ' + name)
                validation._equal(metadata[name], expected, 'closed guarded proof ' + name)
            for name in ('app', 'reused_images', 'dataset_attestation'):
                validation._equal(_thaw(getattr(self, name)), metadata[name], 'guarded adopted ' + name)
            validation._equal(self.fingerprint, _fingerprint(metadata), 'guarded adopted fingerprint')
            for name in ('app', 'reused_images', 'dataset_attestation'):
                object.__setattr__(self, name, _freeze(_thaw(getattr(self, name))))
        except (KeyError, TypeError, ValueError) as exc:
            raise GuardedHotelArtifactError('Guarded Hotel binding conflicts with its original proof.') from exc

    def metadata(self) -> dict:
        return {**json.loads(self._metadata_json), 'fingerprint': self.fingerprint}


def adopt_guarded_hotel_artifacts(lock_path: Path, publication_path: Path, *,
                                  preparation: Path, upstream: Path, baseline: CandidateArtifactBinding,
                                  inspector=None, blob_inspector=None) -> GuardedHotelArtifactBinding:
    """Regenerate exact evidence using complete source and fresh registry bytes."""
    try:
        supplied, lock_sha = _read_input(Path(lock_path), 'guarded Hotel lock')
        publication, publication_sha = _read_input(Path(publication_path), 'guarded Hotel publication')
        if inspector is None or blob_inspector is None:
            from scripts.deathstarbench_candidate_registry import inspect_manifest, inspect_blob
            inspector = inspect_manifest if inspector is None else inspector
            blob_inspector = inspect_blob if blob_inspector is None else blob_inspector
        regenerated = validation.create_guarded_hotel_lock(
            Path(preparation), Path(upstream), publication, baseline,
            inspector=inspector, blob_inspector=blob_inspector,
        )
        validation._equal(supplied, regenerated, 'complete regenerated guarded Hotel lock')
        metadata = {
            'schema_version': 1, 'candidate_schema': ADOPTION_SCHEMA,
            'workload_id': 'hotel_reservation', 'candidate_only': True, 'released': False,
            'candidate_lock_sha256': lock_sha, 'publication_document_sha256': publication_sha,
            'publication_evidence_sha256': regenerated['publication_evidence_sha256'],
            'preparation': regenerated['preparation'], 'baseline': regenerated['baseline'],
            'recipe_revision': regenerated['recipe_revision'], 'dataset_revision': regenerated['dataset_revision'],
            'generator_revision': regenerated['generator_revision'], 'source_fingerprint': regenerated['source_fingerprint'],
            'app': regenerated['app'], 'reused_images': regenerated['baseline']['images'],
            'dataset_attestation': regenerated['dataset_attestation'],
            **{name: False for name in (
                'initializer_authorized', 'runtime_authorized', 'initialization_executed', 'database_executed',
                'application_start_allowed', 'dataset_qualified', 'runtime_qualified',
                'measurement_qualified', 'cloud_qualified', 'public_release', 'anonymous_pull_qualified',
            )},
            'scope': regenerated['scope'],
            'remaining_gates': [
                'controller-wide-initialization-and-readonly-start-transition',
                'service-discovery-preload-and-restart-recovery', 'warmup-and-reservation-state-epochs',
                'semantic-and-measurement-qualification', 'denied-network-edges-and-cloud-cleanup',
                'public-release-and-ui-review',
            ],
        }
        canonical = _canonical(metadata)
        return GuardedHotelArtifactBinding(
            app=metadata['app'], reused_images=metadata['reused_images'],
            dataset_attestation=metadata['dataset_attestation'], fingerprint=_fingerprint(metadata),
            _metadata_json=canonical, _proof=_Proof(canonical, _ADOPTED),
        )
    except (CandidateArtifactError, validation.GuardedHotelImageLockError, OSError,
            UnicodeError, TypeError, KeyError, ValueError, RecursionError) as exc:
        if isinstance(exc, GuardedHotelArtifactError):
            raise
        raise GuardedHotelArtifactError('Complete guarded Hotel adoption failed: ' + str(exc)) from exc
