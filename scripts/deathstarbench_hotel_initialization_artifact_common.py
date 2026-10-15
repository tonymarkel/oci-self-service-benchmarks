"""Source boundary for the separate guarded Hotel image recipe.

Unlike the offline metadata consistency checker, artifact adoption requires a
complete independently regenerated context from the audited Git checkout. No
caller-supplied manifest/context digest can substitute for that reconstruction.
This performs no image, registry, database, cloud or release operations.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import tempfile

from scripts import prepare_deathstarbench_hotel_initialization as preparation
from scripts import prepare_deathstarbench_hotel_images as legacy
from scripts import validate_deathstarbench_hotel_initialization as offline
from scripts.deathstarbench_artifact_common import PreparationError, validate_tracked_source


PROGRAMS = tuple(sorted((*preparation.APPLICATIONS, 'hotel-initialize')))
PLATFORMS = ('linux/amd64', 'linux/arm64')
ARCHITECTURES = {'linux/amd64': 'x86_64', 'linux/arm64': 'aarch64'}

# Historical review identities select baseline INPUTS, not fresh authority.
# The baseline adopter still reconstructs its lock from complete preparations
# and freshly read registry bytes on every guarded publication/adoption call.
BASELINE_RUN = 38414696606
BASELINE_ARTIFACT = 'candidate-lock-hotel_reservation-38414696606-1'
BASELINE_LOCK_SHA256 = 'a53ceb377feef1a064d990dbc403deccd8d761b8bd70db2e912af69df3d3e86f'
BASELINE_PUBLICATION_SHA256 = 'bef00c72962b3f26393eadbc8e4149888bf6c019e7d7299d32985d5da2eb6ea8'


def _directory(value: Path, label: str) -> Path:
    supplied = Path(value).absolute()
    if '..' in supplied.parts or supplied.is_symlink() or not supplied.is_dir():
        raise PreparationError(f'{label} must be a complete real directory.')
    # Accept ordinary ancestor aliases such as macOS /var -> /private/var,
    # never a linked evidence root. Subsequent I/O pins the real parent chain.
    return supplied.parent.resolve(strict=True) / supplied.name


def _pinned_upstream(upstream: Path):
    return validate_tracked_source(
        upstream, relative_roots=('hotelReservation', 'LICENSE'),
        anchors=legacy.UPSTREAM_ANCHORS,
    )


def validate_guarded_preparation(preparation_root: Path, upstream: Path) -> dict:
    """Reconstruct the whole exact recipe before any registry observation.

    Original source files are held by no-follow descriptors/signatures while
    the legacy/new preparers run. Both the supplied receipt and the complete
    prepared tree (including modes, tests, licenses and retained v1 input
    evidence) must match the independently generated result. Hashes remain
    source evidence, not cryptographically signed binary build provenance.
    """
    root = _directory(preparation_root, 'Guarded Hotel preparation')
    upstream = _directory(upstream, 'Audited upstream')
    if root == upstream or root in upstream.parents or upstream in root.parents:
        raise PreparationError('Guarded Hotel preparation overlaps the audited source.')
    _pinned_upstream(upstream)
    with preparation._GuardedFiles(root) as supplied, \
            preparation._GuardedFiles(upstream / 'hotelReservation') as original, \
            preparation._GuardedFiles(upstream, capture=False) as source_root:
        source_root.read('LICENSE')
        manifest = offline._validate_preparation(supplied)
        before = supplied.tree_digest()
        with tempfile.TemporaryDirectory(prefix='dsb-guarded-hotel-source-') as directory:
            regenerated = Path(directory).resolve() / 'regenerated'
            expected = preparation.prepare_contexts(upstream, regenerated)
            with preparation._GuardedFiles(regenerated) as trusted:
                if manifest != expected or before != trusted.tree_digest():
                    raise PreparationError('Guarded Hotel artifacts differ from independently regenerated audited source.')
                trusted.verify()
        original.verify()
        source_root.read('LICENSE')
        _pinned_upstream(upstream)
        supplied.verify()
        if supplied.tree_digest() != before:
            raise PreparationError('Guarded Hotel source evidence changed during reconstruction.')
        result = {
            'manifest': manifest,
            'manifest_sha256': supplied.digest('context-manifest.json'),
            'prepared_tree_sha256': before,
            'context_sha256': supplied.tree_digest('app'),
            'recipe_sha256': supplied.digest('app/Dockerfile.candidate'),
            'config_sha256': supplied.digest('app/config.json'),
            'source_fingerprint': manifest['seed_source']['source_fingerprint'],
            'recipe_revision': preparation.RECIPE_REVISION,
        }
        supplied.verify()
        return result


def verify_baseline_files(root: Path) -> tuple[Path, Path]:
    """Select the historical reviewed inputs; this is not artifact adoption."""
    root = _directory(root, 'Reviewed Hotel baseline evidence')
    with preparation._GuardedFiles(root, max_file_bytes=16 * 1024 * 1024) as files:
        lock, publication = 'candidate-image-lock.json', 'publication.json'
        for name, expected in ((lock, BASELINE_LOCK_SHA256), (publication, BASELINE_PUBLICATION_SHA256)):
            if hashlib.sha256(files.read(name)).hexdigest() != expected:
                raise PreparationError('Reviewed Hotel baseline file identity differs.')
        files.verify()
    return root / lock, root / publication
