#!/usr/bin/env python3
"""Native, network-disabled smoke of the separate startup-safe Hotel image.

Only pure startup/initializer metadata commands and unbound startup refusals run.
This is not database initialization, dataset qualification, live semantics,
anonymous registry-access evidence, or authority to dispatch a public run.
"""

from __future__ import annotations

import argparse
import copy
from datetime import datetime
import hashlib
from pathlib import Path
import re
import sys
import tempfile
import uuid

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_hotel_initialization import (
    CONTRACT_REVISION, DATASET_REVISION, MAX_ATTESTATION_BYTES,
)
from scripts import prepare_deathstarbench_hotel_startup as preparation_module
from scripts import smoke_deathstarbench_hotel_initialization_images as shared
from scripts import validate_deathstarbench_hotel_startup as startup_validation
from scripts import smoke_deathstarbench_candidate_images as original
from scripts.deathstarbench_artifact_common import PreparationError
from scripts.deathstarbench_candidate_registry import DIGEST, parse_reference, resolve_platform
from scripts.inspect_deathstarbench_support_images import write_receipt
from scripts.validate_deathstarbench_hotel_startup import _validate_preparation

RECIPE_REVISION = preparation_module.RECIPE_REVISION
POLICY_REVISION = preparation_module.POLICY_REVISION


SCHEMA = 'hotel-guarded-startup-image-smoke-v1'
SCOPE = 'offline-native-startup-policy-attestation-and-unbound-startup-refusal-only-no-mongodb'
PROGRAMS = tuple(sorted((*original.HOTEL_PROGRAMS, 'hotel-initialize')))
EXECUTABLES = tuple('/go/bin/' + name for name in PROGRAMS)
# BuildKit materializes Docker's standard Linux PATH for scratch images even
# though the Dockerfile declares only the four workload variables. No identity
# or caller-provided PATH is allowed: binaries always use absolute exec paths.
ENVIRONMENT = ['PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
               'TLS=false', 'GOGC=100', 'MEMC_TIMEOUT=2', 'LOG_LEVEL=INFO']
METHODS = [
    'verified-registry-manifest-and-config-bytes',
    'stopped-container-export-no-host-extraction',
    'exact-eleven-static-native-ELF64-programs',
    'baked-config-license-hashes-and-private-key-clone-screen',
    'ten-early-pure-startup-policy-and-one-baseline-initializer-attestations-no-identity-no-config',
    'eleven-unbound-startup-refusals-no-identity-no-config',
]
CONTROLS = {
    'network': 'none', 'read_only': True, 'user': '65532:65532',
    'drop_capabilities': ['ALL'], 'no_new_privileges': True,
    'pids_limit': 32, 'memory_bytes': 512 * 1024 * 1024,
    'tmpfs': '/tmp:rw,noexec,nosuid,nodev,size=16m,mode=1777',
    'working_directory': '/tmp', 'identity_environment_supplied': False,
    'container_cleanup': 'unique-owner-label-inspect-image-and-id-then-force-remove-finally',
}
FALSE_FLAGS = (
    'released', 'database_executed', 'initialization_executed', 'dataset_qualified',
    'runtime_qualified', 'measurement_qualified', 'application_start_allowed',
    'public_release', 'anonymous_pull_qualified', 'application_started', 'application_ready',
    'initializer_authorized', 'runtime_authorized', 'qualification_authority',
    'global_readonly_verification_completed', 'database_execution_verified',
)
MAX_STDERR_BYTES = 16384
MAX_EXPORT_BYTES = 1024 * 1024 * 1024
MAX_ARCHIVE_ENTRIES = 20000
HEX = re.compile(r'[0-9a-f]{64}\Z')
CONTAINER_NAME = re.compile(r'dsb-hotel-offline-[0-9a-f]{32}\Z')
OWNER_LABEL = 'io.deathstarbench.candidate.hotel-offline-owner'
LOG_TIMESTAMP = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})\Z')


# Reuse immutable, audited Docker/OCI/archive primitives without changing their
# module globals. Recipe selection, attestation commands and receipts stay local.
_require = shared._require
_canonical = shared._canonical
_equal = shared._equal
_fields = shared._fields
_digest = shared._digest
_dataset = shared._dataset
_json = shared._json
_execute = shared._execute
_runtime_config = shared._runtime_config
_native_runner = shared._native_runner
_artifact_identities = shared._artifact_identities
_validate_archive = shared._validate_archive
_run_arguments = shared._run_arguments
_export_create_arguments = shared._export_create_arguments
_validate_identities = shared._validate_identities


def _command(arguments, **kwargs):
    status, stdout, _ = _execute(arguments, **kwargs)
    _require(status == 0, 'Startup Hotel inspection command failed (exit ' + str(status) + ').')
    return stdout


def _preparation(files):
    value = _validate_preparation(files)
    return value, {
        'recipe_revision': RECIPE_REVISION, 'initialization_revision': CONTRACT_REVISION,
        'dataset_revision': DATASET_REVISION, 'policy_revision': POLICY_REVISION,
        'config_sha256': value['config_sha256'],
        'context_sha256': value['contexts']['app']['context_sha256'],
        'preparation_manifest_sha256': files.digest('context-manifest.json'),
        'source_fingerprint': value['seed_source']['source_fingerprint'],
    }


def _owned_container(name, image):
    """Resolve a fresh owned name to an exact ID, or prove its absence."""
    status, stdout, stderr = _execute(['docker', 'container', 'inspect', name], timeout=30)
    if status != 0:
        # Docker emits the exact requested bounded name, not an arbitrary
        # resource selector. Other daemon/permission errors leave state
        # uncertain and must not silently qualify a successful smoke.
        missing = re.fullmatch(
            rb'(?:Error(?: response from daemon)?: )?No such (?:object|container): '
            + re.escape(name.encode('ascii')) + rb'\r?\n?', stderr,
        )
        _require(status == 1 and missing and stdout.strip() in {b'', b'[]'},
                 'Cannot prove the inspection container is absent after execution.')
        return None
    inspected = _json(stdout)
    _require(isinstance(inspected, list) and len(inspected) == 1 and isinstance(inspected[0], dict),
             'Expected one owned native inspection container before cleanup.')
    container = inspected[0]
    identity, config = container.get('Id'), container.get('Config')
    _require(isinstance(identity, str) and HEX.fullmatch(identity)
             and identity != '0' * 64 and container.get('Name') == '/' + name
             and isinstance(config, dict) and config.get('Image') == image
             and isinstance(config.get('Labels'), dict)
             and config['Labels'].get(OWNER_LABEL) == name.removeprefix('dsb-hotel-offline-'),
             'Refusing to remove a foreign or uncertain inspection container.')
    return identity


def _cleanup_execution(name, image):
    """Never delete by a guessed name, including a failed run/name collision."""
    identity = _owned_container(name, image)
    if identity is None:
        return
    _command(['docker', 'rm', '-f', identity], timeout=30)


def _refusal_stderr(path):
    return ('Candidate Hotel initialization/verification refused\n' if path.endswith('/hotel-initialize')
            else 'Candidate Hotel startup preflight refused\n')


def _validate_stderr(stderr, *, path, attest):
    if attest:
        _equal(stderr, '', 'pure metadata has no TLS, config or service logs')
        return
    if path.endswith('/hotel-initialize'):
        _equal(stderr, '' if attest else _refusal_stderr(path), 'initializer stderr')
        return
    # Normal unbound execution reaches only the audited TLS-disabled event
    # and explicit preflight refusal. Metadata skips TLS setup altogether.
    lines = stderr.splitlines(keepends=True)
    _require(len(lines) == (1 if attest else 2) and all(line.endswith('\n') for line in lines),
             'Expected only the audited TLS-disabled event and explicit identity refusal.')
    log = _json(lines[0].encode('utf-8'))
    _fields(log, {'level', 'time', 'message'}, 'audited upstream TLS event')
    _equal(log['level'], 'info', 'TLS event level')
    _equal(log['message'], 'TLS disabled.', 'TLS event message')
    stamp = log['time']
    _require(isinstance(stamp, str) and LOG_TIMESTAMP.fullmatch(stamp), 'TLS log timestamp must be bounded RFC3339.')
    try:
        datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    except ValueError:
        raise PreparationError('TLS log timestamp is invalid.') from None
    if not attest:
        _equal(lines[1], _refusal_stderr(path), 'explicit identity/startup refusal')


def _execution_command(path, attest):
    if not attest:
        return [path]
    flag = ('--candidate-initialization-attestation' if path.endswith('/hotel-initialize')
            else '--candidate-startup-attestation')
    return [path, flag]


def _startup_metadata(value, *, component, bindings, dataset_fingerprint, encoded=False):
    if encoded:
        raw = value.encode('utf-8')
        _require(0 < len(raw) <= startup_validation.MAX_METADATA_BYTES,
                 'Startup metadata exceeds its bounded size.')
        observed = _json(raw)
    else:
        observed = value
    expected = {
        'schema_version': 1, 'candidate_schema': 'hotel-guarded-startup-source-attestation-v1',
        'recipe_revision': RECIPE_REVISION, 'policy_revision': POLICY_REVISION,
        'component': component, 'config_sha256': 'sha256:' + bindings['config_sha256'],
        'dataset_fingerprint': dataset_fingerprint, 'source_fingerprint': bindings['source_fingerprint'],
        'candidate_only': True,
        'scope': 'offline-source-contract-only-no-live-readiness-or-execution-authority',
        **{flag: False for flag in startup_validation._ATTESTATION_FALSE},
    }
    _equal(observed, expected, 'exact startup metadata for ' + component)
    return copy.deepcopy(observed)


def _execution(image, path, *, attest, elf_sha256):
    command = _execution_command(path, attest)
    name = 'dsb-hotel-offline-' + uuid.uuid4().hex
    arguments = _run_arguments(image, command, name)
    try:
        status, stdout, stderr = _execute(arguments, timeout=45)
    finally:
        # Killing a timed-out docker client does not guarantee its container
        # stopped. Named, explicitly removed executions prevent leaked work.
        _cleanup_execution(name, image)
    expected = 0 if attest else 2
    _require(status == expected, 'Hotel pure attestation/startup refusal returned an unexpected exit status.')
    _require(attest or not stdout, 'Hotel unbound startup refusal produced unexpected stdout.')
    try:
        out, err = stdout.decode('utf-8'), stderr.decode('utf-8')
    except UnicodeError:
        raise PreparationError('Guarded Hotel output must be bounded UTF-8.') from None
    _validate_stderr(err, path=path, attest=attest)
    return {'program': path, 'elf_sha256': elf_sha256, 'command': command, 'container_name': name, 'docker_arguments': arguments,
            'exit_code': status, 'stdout': out, 'stdout_sha256': hashlib.sha256(stdout).hexdigest(),
            'stderr': err, 'stderr_sha256': hashlib.sha256(stderr).hexdigest()}


def _validate_execution(value, *, image, path, attest, elf_sha256):
    _fields(value, {'program', 'elf_sha256', 'command', 'container_name', 'docker_arguments', 'exit_code', 'stdout', 'stdout_sha256',
                    'stderr', 'stderr_sha256'}, 'execution')
    command = _execution_command(path, attest)
    _equal(value['program'], path, 'execution program')
    _equal(value['elf_sha256'], elf_sha256, 'execution binary hash')
    _equal(value['command'], command, 'execution command')
    name = value['container_name']
    _require(isinstance(name, str) and CONTAINER_NAME.fullmatch(name), 'Invalid bounded inspection-container name.')
    _equal(value['docker_arguments'], _run_arguments(image, command, name), 'hardened Docker arguments')
    _equal(value['exit_code'], 0 if attest else 2, 'execution exit code')
    for field, limit in (('stdout', MAX_ATTESTATION_BYTES), ('stderr', MAX_STDERR_BYTES)):
        text = value[field]
        _require(isinstance(text, str), 'Expected UTF-8 execution output.')
        try:
            raw = text.encode('utf-8')
        except UnicodeError:
            raise PreparationError('Execution output must be UTF-8.') from None
        _require(len(raw) <= limit, 'Execution output exceeds its bound.')
        _equal(value[field + '_sha256'], hashlib.sha256(raw).hexdigest(), 'execution output hash')
    _require(attest or value['stdout'] == '', 'Unexpected unbound startup-refusal stdout.')
    _validate_stderr(value['stderr'], path=path, attest=attest)


def validate_startup_hotel_smoke_receipt(receipt, *, image, platform, config_digest, preparation):
    """Validate historical offline smoke evidence; grant no runtime authority."""
    _require(platform in {'linux/amd64', 'linux/arm64'}, 'Unsupported native Hotel platform.')
    _, _, selector = parse_reference(image)
    _digest(selector, 'immutable platform image', prefix=True)
    _digest(config_digest, 'config digest', prefix=True)
    architecture = 'x86_64' if platform == 'linux/amd64' else 'aarch64'
    keys = {
        'schema_version', 'candidate_schema', 'scope', 'workload_id', 'image_key', 'image', 'platform',
        'architecture', 'config_digest', 'recipe_revision', 'initialization_revision', 'dataset_revision',
        'policy_revision', 'config_sha256', 'startup_attestations',
        'context_sha256', 'preparation_manifest_sha256', 'source_fingerprint', 'candidate_only',
        'native_runner', 'image_config', 'methods', 'execution_controls', 'artifact_identities',
        'attestation_executions', 'unbound_startup_executions', 'dataset_attestation', 'dataset_fingerprint',
        'filesystem_inspection', 'passed', *FALSE_FLAGS,
    }
    _fields(receipt, keys, 'receipt')
    with preparation_module.guarded._GuardedFiles(Path(preparation)) as files:
        _, bindings = _preparation(files)
        for key, expected in {
            'schema_version': 1, 'candidate_schema': SCHEMA, 'scope': SCOPE,
            'workload_id': 'hotel_reservation', 'image_key': 'hotel-reservation',
            'image': image, 'platform': platform, 'architecture': architecture,
            'config_digest': config_digest, 'candidate_only': True, 'passed': True,
            **bindings, **{flag: False for flag in FALSE_FLAGS},
        }.items():
            _equal(receipt[key], expected, key)
        _equal(receipt['native_runner'], {'platform': platform, 'architecture': architecture, 'qemu_used': False}, 'native runner')
        _fields(receipt['image_config'], {'os', 'architecture', 'config_digest', 'runtime'}, 'image config')
        for key, expected in {'os': 'linux', 'architecture': platform.split('/')[1], 'config_digest': config_digest}.items():
            _equal(receipt['image_config'][key], expected, 'image config ' + key)
        _runtime_config(receipt['image_config']['runtime'])
        _equal(receipt['methods'], METHODS, 'inspection methods')
        _equal(receipt['execution_controls'], CONTROLS, 'execution controls')
        inspection = receipt['filesystem_inspection']
        _fields(inspection, {'container_name', 'container_id', 'create_arguments', 'export_arguments',
                             'started', 'ownership_verified_before_export'}, 'stopped filesystem inspection')
        name, identity = inspection['container_name'], inspection['container_id']
        _require(isinstance(name, str) and CONTAINER_NAME.fullmatch(name), 'Invalid stopped-export inspection name.')
        _digest(identity, 'stopped-export container identity')
        _equal(inspection['create_arguments'], _export_create_arguments(image, name), 'stopped-export hardened create arguments')
        _equal(inspection['export_arguments'], ['docker', 'export', identity], 'stopped-export exact ID')
        _equal(inspection['started'], False, 'stopped-export never-started scope')
        _equal(inspection['ownership_verified_before_export'], True, 'stopped-export ownership check')
        _validate_identities(receipt['artifact_identities'], architecture=architecture, context=Path(preparation) / 'app')
        dataset = _dataset(receipt['dataset_attestation'], source_fingerprint=bindings['source_fingerprint'])
        _equal(receipt['dataset_fingerprint'], dataset['dataset_fingerprint'], 'shared dataset fingerprint')
        startup_attestations = receipt['startup_attestations']
        _fields(startup_attestations, preparation_module.APPLICATIONS, 'exact startup component attestations')
        for component in preparation_module.APPLICATIONS:
            _startup_metadata(startup_attestations[component], component=component, bindings=bindings,
                              dataset_fingerprint=dataset['dataset_fingerprint'])
        names = {name}
        for field, attest in (('attestation_executions', True), ('unbound_startup_executions', False)):
            executions = receipt[field]
            _require(isinstance(executions, list) and len(executions) == len(EXECUTABLES), 'Expected exactly eleven ordered native executions.')
            for execution, path in zip(executions, EXECUTABLES):
                _validate_execution(execution, image=image, path=path, attest=attest,
                                    elf_sha256=receipt['artifact_identities']['elf_artifacts'][path]['sha256'])
                _require(execution['container_name'] not in names, 'Each native execution requires its own inspection container.')
                names.add(execution['container_name'])
                if attest:
                    component = path.rsplit('/', 1)[1]
                    if component == 'hotel-initialize':
                        observed = _dataset(execution['stdout'], source_fingerprint=bindings['source_fingerprint'], encoded=True)
                        _equal(observed, dataset, 'unchanged initializer dataset attestation')
                    else:
                        observed = _startup_metadata(execution['stdout'], component=component, bindings=bindings,
                                                     dataset_fingerprint=dataset['dataset_fingerprint'], encoded=True)
                        _equal(observed, startup_attestations[component], 'per-program startup attestation')
        files.verify()
    return copy.deepcopy(receipt)


def smoke_startup_hotel_image(image, *, architecture, preparation):
    _require(architecture in original.ARCHITECTURES, 'Unsupported native Hotel architecture.')
    _, _, selector = parse_reference(image)
    _digest(selector, 'immutable platform image', prefix=True)
    platform = original.ARCHITECTURES[architecture][0]
    with preparation_module.guarded._GuardedFiles(Path(preparation)) as files:
        # Pin and locally validate all inputs before any registry/daemon call.
        _, bindings = _preparation(files)
        native = _native_runner(architecture)
        verified = resolve_platform(image, platform)
        _equal(verified['image'], image, 'exact platform manifest (not index)')
        config = _runtime_config(verified['config'].get('config'))
        local = _json(_command(['docker', 'image', 'inspect', image]))
        _require(isinstance(local, list) and len(local) == 1 and isinstance(local[0], dict), 'Expected exactly one locally loaded native image.')
        for key, expected in {'Id': verified['config_digest'], 'Architecture': platform.split('/')[1], 'Os': 'linux'}.items():
            _equal(local[0].get(key), expected, 'local image ' + key)
        # Docker may materialize harmless absent/default config fields during
        # inspect. Validate those independently; immutable Id binds raw registry
        # config bytes, while required execution semantics must remain exact.
        _runtime_config(local[0].get('Config'))
        with tempfile.TemporaryDirectory(prefix='dsb-hotel-startup-smoke-') as directory:
            archive = Path(directory) / 'filesystem.tar'
            name = 'dsb-hotel-offline-' + uuid.uuid4().hex
            create_arguments = _export_create_arguments(image, name)
            try:
                # Enclose create itself: Docker can create successfully even
                # when its client times out or loses the returned ID bytes.
                raw = _command(create_arguments)
                try:
                    container = raw.decode('ascii').strip()
                except UnicodeError:
                    raise PreparationError('Docker returned a non-ASCII stopped-container identity.') from None
                _require(HEX.fullmatch(container) and container != '0' * 64,
                         'Docker returned an invalid stopped-container identity.')
                _equal(_owned_container(name, image), container, 'stopped-export inspected ownership identity')
                with archive.open('xb') as stream:
                    _command(['docker', 'export', container], timeout=300, stdout_file=stream, stdout_limit=MAX_EXPORT_BYTES)
                _validate_archive(archive)
                fs = original.ImageFilesystem(archive)
                try:
                    identities = _artifact_identities(fs, architecture, Path(preparation) / 'app', config)
                finally:
                    fs.close()
            finally:
                _cleanup_execution(name, image)
            filesystem_inspection = {'container_name': name, 'container_id': container,
                'create_arguments': create_arguments, 'export_arguments': ['docker', 'export', container],
                'started': False, 'ownership_verified_before_export': True}
        attestations = [_execution(image, path, attest=True, elf_sha256=identities['elf_artifacts'][path]['sha256']) for path in EXECUTABLES]
        refusals = [_execution(image, path, attest=False, elf_sha256=identities['elf_artifacts'][path]['sha256']) for path in EXECUTABLES]
        initializer = next(value for value in attestations if value['program'].endswith('/hotel-initialize'))
        dataset = _dataset(initializer['stdout'], source_fingerprint=bindings['source_fingerprint'], encoded=True)
        startup_attestations = {}
        for execution in attestations:
            component = execution['program'].rsplit('/', 1)[1]
            if component != 'hotel-initialize':
                startup_attestations[component] = _startup_metadata(
                    execution['stdout'], component=component, bindings=bindings,
                    dataset_fingerprint=dataset['dataset_fingerprint'], encoded=True)
        _fields(startup_attestations, preparation_module.APPLICATIONS, 'exact native startup metadata inventory')
        files.verify()
        receipt = {
            'schema_version': 1, 'candidate_schema': SCHEMA, 'scope': SCOPE,
            'workload_id': 'hotel_reservation', 'image_key': 'hotel-reservation',
            'image': image, 'platform': platform, 'architecture': architecture,
            'config_digest': verified['config_digest'], **bindings,
            'candidate_only': True, **{flag: False for flag in FALSE_FLAGS},
            'native_runner': native, 'image_config': {'os': 'linux', 'architecture': platform.split('/')[1],
                 'config_digest': verified['config_digest'], 'runtime': config},
            'methods': list(METHODS), 'execution_controls': copy.deepcopy(CONTROLS),
            'artifact_identities': identities, 'attestation_executions': attestations,
            'filesystem_inspection': filesystem_inspection,
            'unbound_startup_executions': refusals, 'dataset_attestation': dataset,
            'startup_attestations': startup_attestations,
            'dataset_fingerprint': dataset['dataset_fingerprint'], 'passed': True,
        }
        return validate_startup_hotel_smoke_receipt(receipt, image=image, platform=platform,
                                                    config_digest=verified['config_digest'], preparation=preparation)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', required=True)
    parser.add_argument('--architecture', choices=tuple(original.ARCHITECTURES), required=True)
    parser.add_argument('--preparation', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    receipt = smoke_startup_hotel_image(args.image, architecture=args.architecture, preparation=args.preparation)
    write_receipt(args.output, receipt)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
