#!/usr/bin/env python3
"""Manual anonymous Hotel OCI-byte qualification, never runtime execution.

Historical file hashes select reviewed inputs, not authority. Every invocation
regenerates complete source preparations, independently reconstructs the guarded
source, freshly adopts both image locks, and downloads every exact image layer.
No Docker daemon, cloud, Kubernetes, database, service, or initializer is used.
Only detached historical access evidence is written; process-local capability
objects are never serialized or turned into run/cluster/Pod identities.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_hotel_initialization_artifacts import adopt_guarded_hotel_artifacts
from scripts.deathstarbench_artifact_common import PreparationError
from scripts.deathstarbench_hotel_initialization_artifact_common import (
    BASELINE_ARTIFACT, BASELINE_RUN, _directory, validate_guarded_preparation,
)
from scripts.prepare_deathstarbench_hotel_initialization import _GuardedFiles
from scripts.prepare_deathstarbench_load_driver import LUASOCKET_SOURCE_SHA256
from scripts import publish_deathstarbench_hotel_initialization_images as publication


GUARDED_RUN = 38767705438
GUARDED_ARTIFACT = 'hotel-guarded-lock-38767705438-1'
GUARDED_LOCK_SHA256 = 'ddb175985a0555adca5d2457198a289f588aa847301c95dd47e1f5ae4796fee6'
GUARDED_PUBLICATION_SHA256 = '2bfcf18580a4a2092896816ffa4eb2a451d97a69d31b22a263ec4c4548d6701b'
MAX_OUTPUT_BYTES = 4 * 1024 * 1024
_EMPTY_ENVIRONMENT = (
    'REGISTRY_TOKEN', 'REGISTRY_USERNAME', 'DOCKER_AUTH_CONFIG', 'GH_TOKEN', 'GITHUB_TOKEN',
    'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
    'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy',
    'SSL_CERT_FILE', 'SSL_CERT_DIR', 'REQUESTS_CA_BUNDLE', 'CURL_CA_BUNDLE', 'NETRC',
)


def _anonymous_environment():
    if any(os.environ.get(name) for name in _EMPTY_ENVIRONMENT):
        raise PreparationError('Anonymous qualification requires absent credentials, proxies, and trust overrides.')


def _fresh_path(value: Path, label: str, protected=()) -> Path:
    supplied = Path(value).absolute()
    if '..' in supplied.parts or os.path.lexists(supplied):
        raise PreparationError(label + ' must be a fresh real path.')
    parent = _directory(supplied.parent, label + ' parent')
    result = parent / supplied.name
    for item in protected:
        item = Path(item).resolve(strict=True)
        if result == item or result in item.parents or item in result.parents:
            raise PreparationError(label + ' overlaps a protected input.')
    return result


def _reviewed_guarded_files(root: Path, files: _GuardedFiles):
    for name, expected in (('candidate-image-lock.json', GUARDED_LOCK_SHA256),
                           ('publication.json', GUARDED_PUBLICATION_SHA256)):
        if hashlib.sha256(files.read(name)).hexdigest() != expected:
            raise PreparationError('Reviewed guarded Hotel file identity differs.')
    files.verify()
    return root / 'candidate-image-lock.json', root / 'publication.json'


def qualify(upstream: Path, rock: Path, root: Path, baseline_evidence: Path,
            guarded_evidence: Path) -> dict:
    """Run complete read-only qualification; return historical evidence only."""
    _anonymous_environment()
    upstream = _directory(upstream, 'Audited upstream')
    baseline_evidence = _directory(baseline_evidence, 'Reviewed baseline evidence')
    guarded_evidence = _directory(guarded_evidence, 'Reviewed guarded evidence')
    rock = Path(rock).absolute()
    if '..' in rock.parts:
        raise PreparationError('Pinned LuaSocket source requires a canonical path.')
    rock = _directory(rock.parent, 'Pinned LuaSocket source parent') / rock.name
    root = _fresh_path(root, 'Fresh qualification preparation',
                       (upstream, rock, baseline_evidence, guarded_evidence))
    with _GuardedFiles(rock.parent, capture=False, max_file_bytes=16 * 1024 * 1024) as pinned, \
            _GuardedFiles(baseline_evidence, max_file_bytes=16 * 1024 * 1024) as old_files, \
            _GuardedFiles(guarded_evidence, max_file_bytes=16 * 1024 * 1024) as new_files:
        if hashlib.sha256(pinned.read(rock.name)).hexdigest() != LUASOCKET_SOURCE_SHA256:
            raise PreparationError('Pinned LuaSocket source bytes differ.')
        lock_path, publication_path = _reviewed_guarded_files(guarded_evidence, new_files)
        # Source-only creation and independent reconstruction precede ALL
        # registry access, even the fresh adoption of the historical baseline.
        publication.prepare(upstream, rock, root)
        validate_guarded_preparation(root / 'guarded', upstream)
        pinned.read(rock.name)
        old_files.verify()
        new_files.verify()
        baseline = publication.baseline(root, baseline_evidence)
        artifacts = adopt_guarded_hotel_artifacts(
            lock_path, publication_path, preparation=root / 'guarded',
            upstream=upstream, baseline=baseline,
            inspector=publication.inspect_manifest, blob_inspector=publication.inspect_blob,
        )
        old_files.verify()
        _reviewed_guarded_files(guarded_evidence, new_files)
        from app.deathstarbench_hotel_image_access import (
            qualify_guarded_hotel_image_access, require_guarded_hotel_image_access,
        )
        access = qualify_guarded_hotel_image_access(artifacts)
        require_guarded_hotel_image_access(access, artifacts)
        evidence = access.metadata()
        pinned.read(rock.name)
        old_files.verify()
        _reviewed_guarded_files(guarded_evidence, new_files)
        # No serialized capability, artificial run IDs, cloud shape, cluster
        # identity, Pod UID, or application readiness is manufactured here.
        _encoded(evidence)
        return evidence


def _encoded(value) -> bytes:
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode('utf-8')
    if len(raw) > MAX_OUTPUT_BYTES:
        raise PreparationError('Historical access evidence exceeds its size bound.')
    return raw


def failure_record(exc: BaseException) -> dict:
    """Never retain exception messages, signed URLs, HTTP headers, or tokens."""
    name = type(exc).__name__
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', name):
        name = 'QualificationError'
    return {
        'schema_version': 1, 'candidate_schema': 'hotel-image-access-qualification-failure-v1',
        'candidate_only': True, 'qualification_completed': False, 'error_type': name,
        'message': 'Read-only Hotel image-access prerequisites or byte verification failed.',
        **{key: False for key in ('initializer_authorized', 'initialization_executed', 'database_executed',
                                 'application_start_allowed', 'runtime_qualified', 'measurement_qualified',
                                 'cloud_qualified', 'public_release', 'released')},
        'baseline_input': {'run': BASELINE_RUN, 'artifact': BASELINE_ARTIFACT},
        'guarded_input': {'run': GUARDED_RUN, 'artifact': GUARDED_ARTIFACT},
    }


def _write_owned(directory: Path, name: str, value):
    with _GuardedFiles(directory, capture=False, writable=True, max_file_bytes=MAX_OUTPUT_BYTES) as owned:
        owned.write(name, _encoded(value), new=True)
        owned.verify()


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    for name in ('upstream', 'luasocket-rock', 'root', 'baseline', 'publication', 'output'):
        result.add_argument('--' + name, required=True, type=Path)
    return result


def main(argv=None) -> int:
    arguments = parser().parse_args(argv)
    # Select/create only an explicit fresh output outside every read input and
    # preparation. If it cannot be safely owned, never overwrite anything.
    try:
        protected = (arguments.upstream, arguments.luasocket_rock,
                     arguments.baseline, arguments.publication)
        output = _fresh_path(arguments.output, 'Qualification evidence output', protected)
        root = Path(arguments.root).absolute().resolve()
        if output == root or output in root.parents or root in output.parents:
            raise PreparationError('Qualification evidence output overlaps its preparation.')
        output.mkdir(mode=0o755)
    except BaseException:
        print('Cannot safely create fresh Hotel image-access evidence output.', file=sys.stderr)
        return 1
    try:
        evidence = qualify(arguments.upstream, arguments.luasocket_rock, arguments.root,
                           arguments.baseline, arguments.publication)
        _write_owned(output, 'image-access.json', evidence)
    except BaseException as exc:
        try:
            _write_owned(output, 'qualification-failure.json', failure_record(exc))
        except BaseException:
            print('Hotel image-access failure evidence could not be safely retained.', file=sys.stderr)
        print('Read-only Hotel image-access qualification failed; no runtime operation was requested.', file=sys.stderr)
        return 1
    print('Anonymous Hotel image bytes verified; runtime and public gates remain closed.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
