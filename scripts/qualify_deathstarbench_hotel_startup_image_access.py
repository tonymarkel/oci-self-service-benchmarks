#!/usr/bin/env python3
"""Anonymous full-byte qualification of the separate startup Hotel images.

Reviewed historical hashes select inputs, never authority. Each invocation
prepares and independently reconstructs complete sources, freshly adopts the
original baseline and startup artifacts, and downloads every bound image body.
No Docker, cloud, Kubernetes, database, initializer, application or benchmark is
executed. Only detached historical evidence is written; process-local access
capabilities cannot be restored from that evidence.
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

from app.deathstarbench_hotel_startup_artifacts import adopt_startup_hotel_artifacts
from scripts.deathstarbench_artifact_common import PreparationError
from scripts.deathstarbench_hotel_initialization_artifact_common import (
    BASELINE_ARTIFACT, BASELINE_RUN, _directory,
)
from scripts.deathstarbench_hotel_startup_artifact_common import validate_startup_preparation
from scripts.prepare_deathstarbench_hotel_initialization import _GuardedFiles
from scripts.prepare_deathstarbench_load_driver import LUASOCKET_SOURCE_SHA256
from scripts import publish_deathstarbench_hotel_startup_images as publication


STARTUP_RUN = 38909780616
STARTUP_ARTIFACT = 'hotel-startup-lock-38909780616-1'
STARTUP_LOCK_SHA256 = 'e22515f1c0f5c99ea08aed42e47e9c56476edcc67615555f075e19fc1066f057'
STARTUP_PUBLICATION_SHA256 = 'd3d1b192c6590123b13f2e4457b38bac8cb2c956baf2cf374efbf12fdc4d2f40'
MAX_OUTPUT_BYTES = 4 * 1024 * 1024
_EMPTY_ENVIRONMENT = (
    'REGISTRY_TOKEN', 'REGISTRY_USERNAME', 'DOCKER_AUTH_CONFIG', 'GH_TOKEN', 'GITHUB_TOKEN',
    'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
    'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy',
    'SSL_CERT_FILE', 'SSL_CERT_DIR', 'REQUESTS_CA_BUNDLE', 'CURL_CA_BUNDLE', 'NETRC',
    'PYTHONHTTPSVERIFY', 'SSLKEYLOGFILE', 'OPENSSL_CONF',
)
_FAILURE_FALSE_FLAGS = (
    'qualification_completed', 'qualification_authority', 'initializer_authorized', 'runtime_authorized',
    'initialization_executed', 'database_executed', 'database_execution_verified',
    'global_readonly_verification_completed', 'application_start_allowed', 'application_started',
    'application_ready', 'dataset_qualified', 'runtime_qualified', 'measurement_qualified',
    'cloud_qualified', 'public_release', 'released', 'anonymous_pull_qualified',
    'anonymous_layer_pull_qualified', 'anonymous_all_bound_image_bytes_verified',
    'all_layers_independently_downloaded', 'image_execution_performed',
    'current_candidate_factory_accepts_recipe', 'current_guarded_factory_accepts_recipe',
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


def _reviewed_startup_files(root: Path, files: _GuardedFiles):
    for name, expected in (('candidate-image-lock.json', STARTUP_LOCK_SHA256),
                           ('publication.json', STARTUP_PUBLICATION_SHA256)):
        if hashlib.sha256(files.read(name)).hexdigest() != expected:
            raise PreparationError('Reviewed startup Hotel file identity differs.')
    files.verify()
    return root / 'candidate-image-lock.json', root / 'publication.json'


def qualify(upstream: Path, rock: Path, root: Path, baseline_evidence: Path,
            startup_evidence: Path) -> dict:
    """Freshly reconstruct, adopt and read every byte; return history only."""
    _anonymous_environment()
    upstream = _directory(upstream, 'Audited upstream')
    baseline_evidence = _directory(baseline_evidence, 'Reviewed baseline evidence')
    startup_evidence = _directory(startup_evidence, 'Reviewed startup evidence')
    rock = Path(rock).absolute()
    if '..' in rock.parts:
        raise PreparationError('Pinned LuaSocket source requires a canonical path.')
    rock = _directory(rock.parent, 'Pinned LuaSocket source parent') / rock.name
    root = _fresh_path(root, 'Fresh qualification preparation',
                       (upstream, rock, baseline_evidence, startup_evidence))
    with _GuardedFiles(rock.parent, capture=False, max_file_bytes=16 * 1024 * 1024) as pinned, \
            _GuardedFiles(baseline_evidence, max_file_bytes=16 * 1024 * 1024) as old_files, \
            _GuardedFiles(startup_evidence, max_file_bytes=16 * 1024 * 1024) as new_files:
        if hashlib.sha256(pinned.read(rock.name)).hexdigest() != LUASOCKET_SOURCE_SHA256:
            raise PreparationError('Pinned LuaSocket source bytes differ.')
        lock_path, publication_path = _reviewed_startup_files(startup_evidence, new_files)
        # Complete source preparation and independent reconstruction precede
        # ALL registry I/O, including adoption of the original baseline.
        publication.prepare(upstream, rock, root)
        reconstructed = _encoded(validate_startup_preparation(root / 'startup', upstream))
        pinned.read(rock.name)
        old_files.verify()
        new_files.verify()
        _anonymous_environment()
        baseline = publication.baseline(root, baseline_evidence)
        _anonymous_environment()
        artifacts = adopt_startup_hotel_artifacts(
            lock_path, publication_path, preparation=root / 'startup',
            upstream=upstream, baseline=baseline,
            inspector=publication.inspect_manifest, blob_inspector=publication.inspect_blob,
        )
        old_files.verify()
        _reviewed_startup_files(startup_evidence, new_files)
        _anonymous_environment()
        from app.deathstarbench_hotel_startup_image_access import (
            qualify_startup_hotel_image_access, require_startup_hotel_image_access,
        )
        access = qualify_startup_hotel_image_access(artifacts)
        require_startup_hotel_image_access(access, artifacts)
        _anonymous_environment()
        # Full layer reads can be slow. Reconstruct independently again, not
        # just from a self-consistently rehashed receipt after those reads.
        if _encoded(validate_startup_preparation(root / 'startup', upstream)) != reconstructed:
            raise PreparationError('Startup Hotel source identity changed during anonymous byte qualification.')
        pinned.read(rock.name)
        old_files.verify()
        _reviewed_startup_files(startup_evidence, new_files)
        _anonymous_environment()
        evidence = access.metadata()
        # Never manufacture run, cluster or Pod identities or serialize the
        # process-local capability itself. The detached result is history only.
        _encoded(evidence)
        return evidence


def _encoded(value) -> bytes:
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode('utf-8')
    if len(raw) > MAX_OUTPUT_BYTES:
        raise PreparationError('Historical access evidence exceeds its size bound.')
    return raw


def failure_record(exc: BaseException) -> dict:
    """Never retain exception messages, signed URLs, HTTP headers or tokens."""
    name = type(exc).__name__
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,63}', name):
        name = 'QualificationError'
    return {
        'schema_version': 1, 'candidate_schema': 'hotel-startup-image-access-qualification-failure-v1',
        'candidate_only': True, 'error_type': name,
        'message': 'Read-only startup Hotel image-access prerequisites or byte verification failed.',
        **{key: False for key in _FAILURE_FALSE_FLAGS},
        'baseline_input': {'run': BASELINE_RUN, 'artifact': BASELINE_ARTIFACT},
        'startup_input': {'run': STARTUP_RUN, 'artifact': STARTUP_ARTIFACT},
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
    try:
        protected = (arguments.upstream, arguments.luasocket_rock,
                     arguments.baseline, arguments.publication)
        output = _fresh_path(arguments.output, 'Qualification evidence output', protected)
        root = Path(arguments.root).absolute().resolve()
        if output == root or output in root.parents or root in output.parents:
            raise PreparationError('Qualification evidence output overlaps its preparation.')
        output.mkdir(mode=0o755)
    except BaseException:
        print('Cannot safely create fresh startup Hotel image-access evidence output.', file=sys.stderr)
        return 1
    try:
        evidence = qualify(arguments.upstream, arguments.luasocket_rock, arguments.root,
                           arguments.baseline, arguments.publication)
        _write_owned(output, 'image-access.json', evidence)
    except BaseException as exc:
        try:
            _write_owned(output, 'qualification-failure.json', failure_record(exc))
        except BaseException:
            print('Startup Hotel image-access failure evidence could not be safely retained.', file=sys.stderr)
        print('Read-only startup Hotel image-access qualification failed; no runtime operation was requested.', file=sys.stderr)
        return 1
    print('Anonymous startup Hotel image bytes verified; runtime and public gates remain closed.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
