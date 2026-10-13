"""Adopt fully revalidated Hotel/Media publications for candidate-only runtime.

Neither a checked-in summary nor a lock's hashes are an adoption authority.
The exact lock must be regenerated from its publication, complete prepared
source trees, and freshly inspected registry bytes. This module performs no
registry writes, image pulls, infrastructure actions, or qualification work.
All public-release, runtime, measurement, and anonymous-pull gates stay closed.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import stat
from types import MappingProxyType
from typing import Any

from .deathstarbench_candidate_workload import (
    CANDIDATE_RUNTIME_IMAGE_SCHEMA_VERSION, validate_candidate_assets,
    validate_candidate_images,
)
from .deathstarbench_workload_contract import distributed_workload_profile
from scripts import create_deathstarbench_candidate_image_lock as validation
from scripts.deathstarbench_artifact_common import PreparationError


ADOPTION_SCHEMA = 'candidate-runtime-artifact-adoption-v1'
RUNTIME_IMAGE_SCHEMA_VERSION = CANDIDATE_RUNTIME_IMAGE_SCHEMA_VERSION
MAX_DOCUMENT_BYTES = 16 * 1024 * 1024
REMAINING_GATES = (
    'isolated-anonymous-layer-pull-and-smoke',
    'guest-storage-and-k3s-runtime-integration',
    'workload-dataset-and-semantic-readiness',
    'denied-network-edge-qualification',
    'workload-measurement-qualification',
    'cleanup-and-failure-path-qualification',
    'public-release-and-ui-review',
)
_ADOPTED = object()


class CandidateArtifactError(ValueError):
    """Candidate evidence cannot be fully revalidated or safely adopted."""


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n'


def _fingerprint(value: Any) -> str:
    return 'sha256:' + hashlib.sha256(_canonical(value).encode('utf-8')).hexdigest()


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_thaw(item) for item in value]
    return value


def _strict_json_values(value: Any) -> None:
    # json.loads accepts floating-point overflow and escaped lone surrogates.
    # Neither is admissible evidence for canonical UTF-8 JSON fingerprints.
    if isinstance(value, dict):
        for key, item in value.items():
            key.encode('utf-8')
            _strict_json_values(item)
    elif isinstance(value, list):
        for item in value:
            _strict_json_values(item)
    elif isinstance(value, str):
        value.encode('utf-8')
    elif isinstance(value, float) and not math.isfinite(value):
        raise CandidateArtifactError('Candidate input contains a non-finite JSON number.')


def _read_input(path: Path, label: str) -> tuple[dict, str]:
    try:
        descriptor = path.lstat()
        if not stat.S_ISREG(descriptor.st_mode) or not 0 < descriptor.st_size <= MAX_DOCUMENT_BYTES:
            raise CandidateArtifactError(f'{label} must be a bounded regular JSON file, not a symlink.')
        # Do not follow a replaced leaf link or block on a substituted FIFO.
        flags = (os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
                 | getattr(os, 'O_NONBLOCK', 0) | getattr(os, 'O_BINARY', 0))
        with os.fdopen(os.open(path, flags), 'rb') as stream:
            opened = os.fstat(stream.fileno())
            if (not stat.S_ISREG(opened.st_mode)
                    or (opened.st_dev, opened.st_ino) != (descriptor.st_dev, descriptor.st_ino)):
                raise CandidateArtifactError(f'{label} changed while opening its evidence file.')
            raw = stream.read(MAX_DOCUMENT_BYTES + 1)
        if not 0 < len(raw) <= MAX_DOCUMENT_BYTES or raw.startswith(b'\xef\xbb\xbf'):
            raise CandidateArtifactError(f'{label} must be bounded UTF-8 JSON without a BOM.')
        raw.decode('utf-8')
        document = validation._json_bytes(raw, label)
        _strict_json_values(document)
    except (OSError, UnicodeError, RecursionError, validation.CandidateImageLockError) as exc:
        raise CandidateArtifactError(f'Cannot read strict {label} JSON.') from exc
    return document, hashlib.sha256(raw).hexdigest()


def _preparation_root(path: Path, label: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise CandidateArtifactError(f'Complete {label} preparation is required; summaries are insufficient.')
    _read_input(path / 'context-manifest.json', label + ' preparation manifest')
    return path


class _AdoptionProof:
    # Not a dataclass: dataclasses.replace must not clone the original proof
    # into a new authority while carrying forward its validation token.
    __slots__ = ('metadata_json', 'validation_token')

    def __init__(self, metadata_json: str, validation_token: object):
        if validation_token is not _ADOPTED:
            raise CandidateArtifactError('Candidate evidence proof is factory-only.')
        object.__setattr__(self, 'metadata_json', metadata_json)
        object.__setattr__(self, 'validation_token', validation_token)

    def __setattr__(self, name, value):
        raise CandidateArtifactError('Candidate evidence proof is immutable.')


@dataclass(frozen=True, slots=True)
class CandidateArtifactBinding:
    """Immutable, architecture-neutral adoption result, not a release receipt.

    Obtain instances through adopt_candidate_artifacts. There is deliberately
    no from_metadata shortcut: serialized metadata is evidence identity only.
    """

    workload_id: str
    runtime_manifest: Mapping[str, Any]
    load_driver: Mapping[str, Any]
    image_bindings: Mapping[str, Any]
    fingerprint: str
    _metadata_json: str = field(repr=False)
    _adoption_token: object = field(repr=False, compare=False)
    supported_application_architectures: tuple[str, ...] = field(
        init=False, default=('x86_64', 'aarch64'),
    )
    candidate_only: bool = field(init=False, default=True)
    released: bool = field(init=False, default=False)
    runtime_qualified: bool = field(init=False, default=False)
    measurement_qualified: bool = field(init=False, default=False)
    anonymous_pull_qualified: bool = field(init=False, default=False)

    def __post_init__(self):
        proof = self._adoption_token
        if not isinstance(proof, _AdoptionProof) or proof.validation_token is not _ADOPTED:
            raise CandidateArtifactError('Candidate bindings require complete evidence adoption.')
        if self._metadata_json != proof.metadata_json:
            raise CandidateArtifactError('Candidate binding metadata changed after evidence adoption.')
        metadata = json.loads(proof.metadata_json)
        try:
            validation._equal(self.workload_id, metadata['workload_id'], 'adopted binding workload')
            for name in ('runtime_manifest', 'load_driver', 'image_bindings'):
                validation._equal(_thaw(getattr(self, name)), metadata[name], 'adopted binding ' + name)
            validation._equal(self.fingerprint, _fingerprint(metadata), 'adopted binding fingerprint')
        except (validation.CandidateImageLockError, KeyError, TypeError, ValueError) as exc:
            raise CandidateArtifactError('Candidate binding conflicts with its original adoption proof.') from exc
        for name in ('runtime_manifest', 'load_driver', 'image_bindings'):
            object.__setattr__(self, name, _freeze(dict(getattr(self, name))))

    @property
    def load_driver_image(self) -> str:
        return self.load_driver['image']

    def metadata(self) -> dict[str, Any]:
        """Return a fresh portable snapshot without exposing mutable internals."""
        return {**json.loads(self._metadata_json), 'fingerprint': self.fingerprint}


def _runtime_manifest(workload_id: str, images: dict) -> dict:
    profile = distributed_workload_profile(workload_id)
    components, _ = validate_candidate_assets(workload_id)
    application_keys = {component['image_key'] for component in components if component['role'] == 'application'}
    keys = {
        'linux/amd64': set(profile.required_image_keys),
        'linux/arm64': application_keys,
    }
    # Support roles and the driver are fixed amd64 even for an arm application.
    # Never fill missing support arm manifests using an amd64 image/index/tag.
    result = {
        'schema_version': RUNTIME_IMAGE_SCHEMA_VERSION, 'workload_id': workload_id,
        'workload_revision': profile.workload_revision,
        'image_set_revision': profile.image_set_revision,
        'upstream_revision': validation.UPSTREAM_REVISION, 'released': False,
        'platforms': {platform: {'images': {key: images[key]['platforms'][platform]['image']
                                          for key in sorted(selected)}}
                      for platform, selected in keys.items()},
    }
    validate_candidate_images(workload_id, result)
    return result


def _image_bindings(images: dict) -> dict:
    return {key: {
        'index_image': value['index_image'],
        'platforms': {platform: {name: item[name] for name in (
            'image', 'manifest_digest', 'config_digest', 'os', 'architecture',
        )} for platform, item in sorted(value['platforms'].items())},
        **({'prepared_identity': value['prepared_identity']} if 'prepared_identity' in value else {}),
    } for key, value in sorted(images.items())}


def adopt_candidate_artifacts(
    workload_id: str,
    lock_path: Path,
    publication_path: Path,
    *,
    workload_preparation: Path,
    driver_preparation: Path,
    inspector: Callable[[str], bytes] | None = None,
    blob_inspector: Callable[[str, str], bytes] | None = None,
) -> CandidateArtifactBinding:
    """Read-only adoption after full source, receipt, and registry validation.

    Both native application platforms and the fixed amd64 support/driver roles
    must be proven even if one upcoming run uses only one application platform.
    Missing preparations fail before registry inspection. Inputs are read once
    for adoption, and no caller-owned document mapping is retained.
    """
    try:
        profile = distributed_workload_profile(workload_id)
    except (TypeError, ValueError) as exc:
        raise CandidateArtifactError('Only unreleased Hotel/Media artifacts can be adopted.') from exc
    if profile.released or workload_id not in validation.CUSTOM_KEYS:
        raise CandidateArtifactError('Social Network keeps its existing released artifact path.')
    try:
        workload_root = _preparation_root(Path(workload_preparation), 'workload')
        driver_root = _preparation_root(Path(driver_preparation), 'driver')
        supplied, lock_sha256 = _read_input(Path(lock_path), 'candidate lock')
        publication, publication_sha256 = _read_input(Path(publication_path), 'candidate publication')
        for name, expected in {
            'schema_version': 1, 'candidate_schema': validation.LOCK_SCHEMA,
            'candidate_only': True, 'released': False, 'runtime_qualified': False,
            'measurement_qualified': False, 'anonymous_pull_qualified': False,
            'workload_id': workload_id, 'upstream_revision': validation.UPSTREAM_REVISION,
            'workload_revision': profile.workload_revision,
            'image_set_revision': profile.image_set_revision,
            'load_driver_revision': profile.load_driver_revision,
        }.items():
            validation._equal(supplied.get(name), expected, 'candidate adoption ' + name)
        if inspector is None or blob_inspector is None:
            from scripts.deathstarbench_candidate_registry import inspect_manifest, inspect_blob
            inspector = inspect_manifest if inspector is None else inspector
            blob_inspector = inspect_blob if blob_inspector is None else blob_inspector
        regenerated = validation.create_candidate_lock(
            workload_id, workload_root, driver_root, publication,
            inspector=inspector, blob_inspector=blob_inspector,
        )
        validation._equal(supplied, regenerated, 'complete regenerated candidate artifact lock')
        runtime_manifest = _runtime_manifest(workload_id, regenerated['images'])
        image_bindings = _image_bindings(regenerated['images'])
        driver = regenerated['images']['load-driver']['platforms']['linux/amd64']
        attestation = driver['smoke_receipt']['load_driver_attestation']
        load_driver = {
            'workload_id': workload_id, 'revision': profile.load_driver_revision,
            'image': driver['image'], 'config_digest': driver['config_digest'],
            'platform': 'linux/amd64', 'architecture': 'x86_64',
            'attestation': attestation,
            'attestation_sha256': hashlib.sha256(attestation.encode('utf-8')).hexdigest(),
            'artifact': dict(driver['driver_attestation']),
            'prepared_identity': regenerated['images']['load-driver']['prepared_identity'],
            'measurement_qualified': False, 'released': False,
        }
        metadata = {
            'schema_version': 1, 'candidate_schema': ADOPTION_SCHEMA,
            'candidate_only': True, 'released': False, 'runtime_qualified': False,
            'measurement_qualified': False, 'anonymous_pull_qualified': False,
            'workload_id': workload_id, 'workload_identity': profile.metadata(),
            'candidate_lock_sha256': lock_sha256,
            'publication_document_sha256': publication_sha256,
            'publication_evidence_sha256': regenerated['publication_evidence_sha256'],
            'workload_preparation_manifest_sha256': regenerated['preparation']['workload_manifest_sha256'],
            'driver_preparation_manifest_sha256': regenerated['preparation']['driver_manifest_sha256'],
            'preparation_identity_sha256': _fingerprint(regenerated['preparation']),
            'supported_application_architectures': ['x86_64', 'aarch64'],
            'runtime_manifest': runtime_manifest, 'image_bindings': image_bindings,
            'load_driver': load_driver, 'remaining_gates': list(REMAINING_GATES),
        }
        return CandidateArtifactBinding(
            workload_id=workload_id, runtime_manifest=runtime_manifest,
            load_driver=load_driver, image_bindings=image_bindings,
            fingerprint=_fingerprint(metadata), _metadata_json=_canonical(metadata),
            _adoption_token=_AdoptionProof(_canonical(metadata), _ADOPTED),
        )
    except (validation.CandidateImageLockError, PreparationError, OSError, UnicodeError,
            TypeError, KeyError, ValueError, RecursionError) as exc:
        if isinstance(exc, CandidateArtifactError):
            raise
        raise CandidateArtifactError('Complete candidate artifact adoption failed: ' + str(exc)) from exc
