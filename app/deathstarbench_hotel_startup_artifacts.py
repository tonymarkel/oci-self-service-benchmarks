"""Immutable artifact-only startup Hotel adoption, deliberately not runtime.

No runtime manifest, initializer/startup plan, deserialization constructor or
conversion to either existing artifact capability is provided.
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
from scripts import create_deathstarbench_hotel_startup_image_lock as validation


ADOPTION_SCHEMA = 'hotel-guarded-startup-artifact-adoption-v1'
_ADOPTED = object()


class StartupHotelArtifactError(ValueError):
    """Complete startup Hotel evidence has not been freshly validated."""


class _Proof:
    __slots__ = ('metadata_json', 'token')

    def __init__(self, metadata_json, token):
        if token is not _ADOPTED:
            raise StartupHotelArtifactError('Startup Hotel adoption proof is factory-only.')
        object.__setattr__(self, 'metadata_json', metadata_json)
        object.__setattr__(self, 'token', token)

    def __setattr__(self, name, value):
        raise StartupHotelArtifactError('Startup Hotel adoption proof is immutable.')


@dataclass(frozen=True, slots=True)
class StartupHotelArtifactBinding:
    """Fresh source/image evidence identity; never execution authority."""

    app: Mapping[str, Any]
    reused_images: Mapping[str, Any]
    dataset_attestation: Mapping[str, Any]
    startup_attestations: Mapping[str, Any]
    fingerprint: str
    _metadata_json: str = field(repr=False)
    _proof: object = field(repr=False, compare=False)
    workload_id: str = field(init=False, default='hotel_reservation')
    candidate_only: bool = field(init=False, default=True)
    released: bool = field(init=False, default=False)
    qualification_authority: bool = field(init=False, default=False)
    initializer_authorized: bool = field(init=False, default=False)
    runtime_authorized: bool = field(init=False, default=False)
    initialization_executed: bool = field(init=False, default=False)
    database_executed: bool = field(init=False, default=False)
    application_start_allowed: bool = field(init=False, default=False)
    application_started: bool = field(init=False, default=False)
    application_ready: bool = field(init=False, default=False)
    global_readonly_verification_completed: bool = field(init=False, default=False)
    database_execution_verified: bool = field(init=False, default=False)
    dataset_qualified: bool = field(init=False, default=False)
    runtime_qualified: bool = field(init=False, default=False)
    measurement_qualified: bool = field(init=False, default=False)
    cloud_qualified: bool = field(init=False, default=False)
    public_release: bool = field(init=False, default=False)
    anonymous_pull_qualified: bool = field(init=False, default=False)
    current_candidate_factory_accepts_recipe: bool = field(init=False, default=False)
    current_guarded_factory_accepts_recipe: bool = field(init=False, default=False)

    def __post_init__(self):
        proof = self._proof
        if type(proof) is not _Proof or proof.token is not _ADOPTED or self._metadata_json != proof.metadata_json:
            raise StartupHotelArtifactError('Startup Hotel bindings require complete original factory adoption.')
        try:
            metadata = json.loads(proof.metadata_json)
            for name, expected in {
                'workload_id': 'hotel_reservation', 'candidate_only': True,
                **{name: False for name in validation.FALSE_FLAGS},
            }.items():
                validation._equal(getattr(self, name), expected, 'closed startup binding ' + name)
                validation._equal(metadata[name], expected, 'closed startup proof ' + name)
            for name in ('app', 'reused_images', 'dataset_attestation', 'startup_attestations'):
                validation._equal(_thaw(getattr(self, name)), metadata[name], 'startup adopted ' + name)
            validation._equal(self.fingerprint, _fingerprint(metadata), 'startup adopted fingerprint')
            for name in ('app', 'reused_images', 'dataset_attestation', 'startup_attestations'):
                object.__setattr__(self, name, _freeze(_thaw(getattr(self, name))))
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            raise StartupHotelArtifactError('Startup Hotel binding conflicts with its original proof.') from exc

    def metadata(self) -> dict:
        return {**json.loads(self._metadata_json), 'fingerprint': self.fingerprint}


def adopt_startup_hotel_artifacts(lock_path: Path, publication_path: Path, *,
                                  preparation: Path, upstream: Path, baseline: CandidateArtifactBinding,
                                  inspector=None, blob_inspector=None) -> StartupHotelArtifactBinding:
    """Regenerate complete source and fresh registry evidence; authorize no run."""
    try:
        supplied, lock_sha = _read_input(Path(lock_path), 'startup Hotel lock')
        publication, publication_sha = _read_input(Path(publication_path), 'startup Hotel publication')
        if inspector is None or blob_inspector is None:
            from scripts.deathstarbench_candidate_registry import inspect_manifest, inspect_blob
            inspector = inspect_manifest if inspector is None else inspector
            blob_inspector = inspect_blob if blob_inspector is None else blob_inspector
        regenerated = validation.create_startup_hotel_lock(
            Path(preparation), Path(upstream), publication, baseline,
            inspector=inspector, blob_inspector=blob_inspector,
        )
        validation._equal(supplied, regenerated, 'complete regenerated startup Hotel lock')
        metadata = {
            'schema_version': 1, 'candidate_schema': ADOPTION_SCHEMA,
            'workload_id': 'hotel_reservation', 'candidate_only': True,
            'candidate_lock_sha256': lock_sha, 'publication_document_sha256': publication_sha,
            'publication_evidence_sha256': regenerated['publication_evidence_sha256'],
            'preparation': regenerated['preparation'], 'baseline': regenerated['baseline'],
            **{name: regenerated[name] for name in (
                'recipe_revision', 'dataset_revision', 'policy_revision', 'generator_revision',
                'source_fingerprint', 'config_sha256', 'app', 'dataset_attestation', 'startup_attestations',
            )},
            'reused_images': regenerated['baseline']['images'],
            **{name: False for name in validation.FALSE_FLAGS},
            'scope': regenerated['scope'],
            'remaining_gates': [
                'fresh-anonymous-full-byte-image-access', 'separate-compatible-startup-stage-and-cleanup-controller',
                'service-identity-discovery-preload-and-restart-recovery',
                'authentication-reservation-state-and-semantic-measurement-qualification',
                'denied-network-edges-and-cloud-cleanup', 'public-release-and-ui-review',
            ],
        }
        canonical = _canonical(metadata)
        return StartupHotelArtifactBinding(
            app=metadata['app'], reused_images=metadata['reused_images'],
            dataset_attestation=metadata['dataset_attestation'], startup_attestations=metadata['startup_attestations'],
            fingerprint=_fingerprint(metadata), _metadata_json=canonical, _proof=_Proof(canonical, _ADOPTED),
        )
    except (CandidateArtifactError, validation.StartupHotelImageLockError, OSError,
            UnicodeError, TypeError, KeyError, ValueError, RecursionError) as exc:
        if isinstance(exc, StartupHotelArtifactError):
            raise
        raise StartupHotelArtifactError('Complete startup Hotel adoption failed: ' + str(exc)) from exc
