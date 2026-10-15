#!/usr/bin/env python3
"""Native, network-disabled smoke of the separate-initialization Hotel image.

Only the pure dataset-attestation command and an unbound startup refusal run.
This is not database initialization, dataset qualification, live semantics,
anonymous registry-access evidence, or authority to dispatch a public run.
"""

from __future__ import annotations

import argparse
import copy
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_hotel_initialization import (
    CONTRACT_REVISION, DATASET_REVISION, HotelInitializationError, MAX_ATTESTATION_BYTES, RECIPE_REVISION,
    parse_hotel_dataset_attestation, validate_hotel_dataset_attestation,
)
from scripts import prepare_deathstarbench_hotel_initialization as preparation_module
from scripts import smoke_deathstarbench_candidate_images as original
from scripts.deathstarbench_artifact_common import PreparationError
from scripts.deathstarbench_candidate_registry import DIGEST, parse_reference, resolve_platform
from scripts.inspect_deathstarbench_support_images import write_receipt
from scripts.validate_deathstarbench_hotel_initialization import _validate_preparation


SCHEMA = 'hotel-guarded-initialization-image-smoke-v1'
SCOPE = 'offline-native-image-attestation-and-unbound-startup-refusal-only-no-mongodb'
PROGRAMS = tuple(sorted((*original.HOTEL_PROGRAMS, 'hotel-initialize')))
EXECUTABLES = tuple('/go/bin/' + name for name in PROGRAMS)
ENVIRONMENT = ['TLS=false', 'GOGC=100', 'MEMC_TIMEOUT=2', 'LOG_LEVEL=INFO']
METHODS = [
    'verified-registry-manifest-and-config-bytes',
    'stopped-container-export-no-host-extraction',
    'exact-eleven-static-native-ELF64-programs',
    'baked-config-license-hashes-and-private-key-clone-screen',
    'eleven-early-pure-dataset-attestations-no-identity-no-config',
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
    'public_release', 'anonymous_pull_qualified',
)
MAX_STDERR_BYTES = 16384
MAX_EXPORT_BYTES = 1024 * 1024 * 1024
MAX_ARCHIVE_ENTRIES = 20000
HEX = re.compile(r'[0-9a-f]{64}\Z')
CONTAINER_NAME = re.compile(r'dsb-hotel-offline-[0-9a-f]{32}\Z')
OWNER_LABEL = 'io.deathstarbench.candidate.hotel-offline-owner'
SOURCE_LABEL = re.compile(r'https://github\.com/[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}\Z')
REVISION_LABEL = re.compile(r'[0-9a-f]{40}\Z')
LOG_TIMESTAMP = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})\Z')


def _require(condition, message):
    if not condition:
        raise PreparationError(message)


def _canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False, ensure_ascii=False)
    except (ValueError, TypeError, RecursionError, UnicodeError) as exc:
        raise PreparationError('Guarded Hotel smoke requires finite JSON.') from exc


def _equal(value, expected, label):
    _require(_canonical(value) == _canonical(expected), 'Guarded Hotel smoke ' + label + ' differs.')


def _fields(value, keys, label):
    _require(isinstance(value, dict) and set(value) == set(keys), 'Guarded Hotel smoke ' + label + ' schema differs.')


def _digest(value, label, *, prefix=False):
    expression = DIGEST if prefix else HEX
    _require(isinstance(value, str) and expression.fullmatch(value)
             and value.removeprefix('sha256:') != '0' * 64,
             'Guarded Hotel smoke ' + label + ' requires a nonzero SHA-256.')
    return value


def _dataset(value, *, source_fingerprint, encoded=False):
    try:
        validator = parse_hotel_dataset_attestation if encoded else validate_hotel_dataset_attestation
        return validator(value, expected_source_fingerprint=source_fingerprint)
    except HotelInitializationError as exc:
        raise PreparationError('Guarded Hotel dataset attestation differs: ' + str(exc)) from exc


def _json(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            _require(key not in value, 'Duplicate Docker inspection field.')
            value[key] = item
        return value
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(PreparationError('Nonfinite Docker JSON.')))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise PreparationError('Invalid bounded Docker inspection JSON.') from exc


def _execute(arguments, *, timeout=120, stdout_file=None, stdout_limit=MAX_ATTESTATION_BYTES):
    """Drain bounded pipes while running; never capture unbounded Docker output."""
    process = None
    selector = selectors.DefaultSelector()
    stdout, stderr = bytearray(), bytearray()
    count = 0
    try:
        process = subprocess.Popen(arguments, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        selector.register(process.stdout, selectors.EVENT_READ, 'stdout')
        selector.register(process.stderr, selectors.EVENT_READ, 'stderr')
        deadline = time.monotonic() + timeout
        while selector.get_map():
            remaining = deadline - time.monotonic()
            _require(remaining > 0, 'Guarded Hotel inspection command exceeded its deadline.')
            for key, _ in selector.select(min(remaining, 0.25)):
                chunk = os.read(key.fileobj.fileno(), 65536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                if key.data == 'stdout':
                    count += len(chunk)
                    _require(count <= stdout_limit, 'Guarded Hotel command stdout exceeded its bounded size.')
                    if stdout_file is None:
                        stdout.extend(chunk)
                    else:
                        stdout_file.write(chunk)
                else:
                    _require(len(stderr) + len(chunk) <= MAX_STDERR_BYTES,
                             'Guarded Hotel command stderr exceeded its bounded size.')
                    stderr.extend(chunk)
        remaining = deadline - time.monotonic()
        _require(remaining > 0, 'Guarded Hotel inspection command exceeded its deadline.')
        return process.wait(timeout=remaining), bytes(stdout), bytes(stderr)
    except (OSError, subprocess.TimeoutExpired):
        raise PreparationError('Guarded Hotel inspection command failed or timed out.') from None
    finally:
        selector.close()
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdout.close()
            process.stderr.close()


def _command(arguments, **kwargs):
    status, stdout, _ = _execute(arguments, **kwargs)
    _require(status == 0, 'Guarded Hotel inspection command failed (exit ' + str(status) + ').')
    return stdout


def _runtime_config(value):
    _require(isinstance(value, dict), 'Expected explicit scratch image configuration.')
    allowed = {
        'Hostname', 'Domainname', 'User', 'AttachStdin', 'AttachStdout', 'AttachStderr',
        'Tty', 'OpenStdin', 'StdinOnce', 'Env', 'Cmd', 'Image', 'Volumes', 'WorkingDir',
        'Entrypoint', 'OnBuild', 'Labels', 'StopSignal', 'Healthcheck', 'Shell',
        'ArgsEscaped', 'ExposedPorts', 'NetworkDisabled',
    }
    _require(set(value) <= allowed, 'Unexpected scratch runtime configuration field.')
    for field, expected in {'User': '65532:65532', 'WorkingDir': '/workspace',
                            'Cmd': ['/go/bin/frontend'], 'Env': ENVIRONMENT}.items():
        _equal(value.get(field), expected, 'scratch config ' + field)
    for field in ('Entrypoint', 'OnBuild', 'Shell'):
        _require(value.get(field) is None or value[field] == [], 'Scratch image has an unexpected ' + field + '.')
    for field in ('Volumes', 'ExposedPorts'):
        _require(value.get(field) is None or value[field] == {}, 'Scratch image has an unexpected ' + field + '.')
    labels = value.get('Labels')
    if labels is not None and labels != {}:
        _fields(labels, {'org.opencontainers.image.source', 'org.opencontainers.image.revision'}, 'OCI provenance labels')
        source, revision = labels['org.opencontainers.image.source'], labels['org.opencontainers.image.revision']
        _require(isinstance(source, str) and SOURCE_LABEL.fullmatch(source)
                 and isinstance(revision, str) and REVISION_LABEL.fullmatch(revision) and revision != '0' * 40,
                 'Scratch image provenance labels require an exact GitHub repository URL and Git SHA.')
    _require(value.get('Healthcheck') is None, 'Scratch image must not contain healthcheck commands.')
    for field in ('Hostname', 'Domainname', 'Image', 'StopSignal'):
        _require(value.get(field) is None or value[field] == '', 'Unexpected scratch image ' + field + '.')
    for field in ('AttachStdin', 'AttachStdout', 'AttachStderr', 'Tty', 'OpenStdin', 'StdinOnce', 'ArgsEscaped', 'NetworkDisabled'):
        _require(field not in value or value[field] is False, 'Unexpected scratch image ' + field + '.')
    return copy.deepcopy(value)


def _preparation(files):
    value = _validate_preparation(files)
    return value, {
        'recipe_revision': RECIPE_REVISION, 'initialization_revision': CONTRACT_REVISION,
        'dataset_revision': DATASET_REVISION,
        'context_sha256': value['contexts']['app']['context_sha256'],
        'preparation_manifest_sha256': files.digest('context-manifest.json'),
        'source_fingerprint': value['seed_source']['source_fingerprint'],
    }


def _baked_hashes(context):
    return {path: hashlib.sha256(source.read_bytes()).hexdigest()
            for path, source in original._baked_files('hotel_reservation', 'hotel-reservation', context).items()}


def _native_runner(architecture):
    observed = {'amd64': 'x86_64', 'arm64': 'aarch64'}.get(original.host_platform.machine(), original.host_platform.machine())
    _require(original.host_platform.system() == 'Linux' and observed == architecture,
             'Guarded Hotel smoke requires a matching native Linux host, not QEMU.')
    info = _json(_command(['docker', 'info', '--format', '{{json .}}']))
    _require(isinstance(info, dict), 'Expected Docker daemon platform inspection.')
    daemon = {'amd64': 'x86_64', 'arm64': 'aarch64'}.get(info.get('Architecture'), info.get('Architecture'))
    _require(info.get('OSType') == 'linux' and daemon == architecture,
             'Docker daemon must match the native Linux host architecture.')
    return {'platform': original.ARCHITECTURES[architecture][0], 'architecture': architecture, 'qemu_used': False}


def _artifact_identities(fs, architecture, context, config):
    _require(len(fs.members) <= MAX_ARCHIVE_ENTRIES, 'Scratch image archive has too many members.')
    identities = original._inspect_filesystem(fs, 'hotel_reservation', 'hotel-reservation', architecture, context, config)
    _require(set(identities['elf_artifacts']) == set(EXECUTABLES),
             'Scratch image must contain exactly eleven Hotel native ELF programs.')
    for path in EXECUTABLES:
        member = fs._resolve(path, follow_leaf=False)
        identity = identities['elf_artifacts'][path]
        _require(member.isfile() and member.mode & 0o111 and identity['static'] is True,
                 'Every guarded Hotel program must be a regular executable static ELF.')
    # Container exports may include daemon-created /etc/hosts, hostname,
    # resolv.conf and .dockerenv files absent from the image itself. Do not
    # mistake those for an image layer; nevertheless screen every non-ELF
    # regular file so a hidden script cannot escape the clone check.
    for name, member in fs.members.items():
        if member.isfile() and '/' + name not in identities['elf_artifacts']:
            raw = fs.read(name)
            _require(not original.CLONE.search(raw.decode('utf-8', errors='ignore')),
                     'Scratch runtime file clones mutable source.')
    identities['required_executables'] = list(EXECUTABLES)
    return identities


def _validate_archive(archive):
    """Bound tar descriptors before ImageFilesystem materializes its index."""
    try:
        with tarfile.open(archive, mode='r:') as stream:
            for count, member in enumerate(stream, 1):
                _require(count <= MAX_ARCHIVE_ENTRIES, 'Scratch image archive has too many members.')
                _require(len(member.name) <= 4096 and member.size >= 0
                         and member.size <= original.MAX_FILE_BYTES
                         and (member.isfile() or member.isdir() or member.issym() or member.islnk()),
                         'Scratch image archive member has an unsafe type or bounded size.')
    except (OSError, tarfile.TarError):
        raise PreparationError('Scratch image export is not a valid bounded tar archive.') from None


def _run_arguments(image, command, container_name):
    return [
        'docker', 'run', '--name=' + container_name, '--network=none', '--read-only', '--user=65532:65532',
        '--label=' + OWNER_LABEL + '=' + container_name.removeprefix('dsb-hotel-offline-'),
        '--cap-drop=ALL', '--security-opt=no-new-privileges', '--pids-limit=32',
        '--memory=512m', '--tmpfs=' + CONTROLS['tmpfs'], '--workdir=/tmp',
        '--entrypoint=' + command[0], image, *command[1:],
    ]


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


def _export_create_arguments(image, name):
    return ['docker', 'create', '--name=' + name,
            '--label=' + OWNER_LABEL + '=' + name.removeprefix('dsb-hotel-offline-'),
            '--network=none', '--read-only', '--user=65532:65532', '--cap-drop=ALL',
            '--security-opt=no-new-privileges', '--entrypoint=/nonexistent-smoke-not-started', image]


def _refusal_stderr(path):
    return ('Candidate Hotel initialization/verification refused\n' if path.endswith('/hotel-initialize')
            else 'Explicit candidate Hotel initialization identity is required\n')


def _validate_stderr(stderr, *, path, attest):
    if path.endswith('/hotel-initialize'):
        _equal(stderr, '' if attest else _refusal_stderr(path), 'initializer stderr')
        return
    # Upstream tls package init runs before main and writes one informational
    # zerolog event even for pure attestation. Accept only that audited event,
    # never arbitrary warnings/errors or logs suggesting service activity.
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


def _execution(image, path, *, attest, elf_sha256):
    command = [path, '--candidate-initialization-attestation'] if attest else [path]
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


def _validate_identities(value, *, architecture, context):
    _fields(value, {'baked_file_sha256', 'dependency_license_sha256', 'elf_artifacts',
                    'required_executables', 'runtime_clone_detected', 'private_key_detected'}, 'artifact identities')
    _equal(value['required_executables'], list(EXECUTABLES), 'required executable inventory')
    _equal(value['dependency_license_sha256'], {}, 'scratch dependency license layout')
    _equal(value['runtime_clone_detected'], False, 'runtime clone detection')
    _equal(value['private_key_detected'], False, 'private key detection')
    baked = value['baked_file_sha256']
    expected = _baked_hashes(context)
    _fields(baked, {*expected, '/usr/share/licenses/go/LICENSE'}, 'baked file inventory')
    for path, digest in baked.items():
        _digest(digest, 'baked file hash')
        if path in expected:
            _equal(digest, expected[path], 'prepared baked file ' + path)
    elves = value['elf_artifacts']
    _fields(elves, EXECUTABLES, 'exact ELF inventory')
    for path, elf in elves.items():
        _fields(elf, {'sha256', 'elf_class', 'elf_type', 'machine', 'architecture', 'interpreter',
                      'dynamic_dependencies', 'runnable', 'static'}, 'ELF identity')
        _digest(elf['sha256'], 'ELF hash')
        for key, expected in {'elf_class': 64, 'machine': original.ARCHITECTURES[architecture][1],
                              'architecture': architecture, 'interpreter': None, 'dynamic_dependencies': False,
                              'runnable': True, 'static': True}.items():
            _equal(elf[key], expected, 'ELF ' + key)
        _require(type(elf['elf_type']) is int and elf['elf_type'] in {2, 3}, 'ELF must be a static executable.')


def _validate_execution(value, *, image, path, attest, elf_sha256):
    _fields(value, {'program', 'elf_sha256', 'command', 'container_name', 'docker_arguments', 'exit_code', 'stdout', 'stdout_sha256',
                    'stderr', 'stderr_sha256'}, 'execution')
    command = [path, '--candidate-initialization-attestation'] if attest else [path]
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


def validate_guarded_hotel_smoke_receipt(receipt, *, image, platform, config_digest, preparation):
    """Validate historical offline smoke evidence; grant no runtime authority."""
    _require(platform in {'linux/amd64', 'linux/arm64'}, 'Unsupported native Hotel platform.')
    _, _, selector = parse_reference(image)
    _digest(selector, 'immutable platform image', prefix=True)
    _digest(config_digest, 'config digest', prefix=True)
    architecture = 'x86_64' if platform == 'linux/amd64' else 'aarch64'
    keys = {
        'schema_version', 'candidate_schema', 'scope', 'workload_id', 'image_key', 'image', 'platform',
        'architecture', 'config_digest', 'recipe_revision', 'initialization_revision', 'dataset_revision',
        'context_sha256', 'preparation_manifest_sha256', 'source_fingerprint', 'candidate_only',
        'native_runner', 'image_config', 'methods', 'execution_controls', 'artifact_identities',
        'attestation_executions', 'unbound_startup_executions', 'dataset_attestation', 'dataset_fingerprint',
        'filesystem_inspection', 'passed', *FALSE_FLAGS,
    }
    _fields(receipt, keys, 'receipt')
    with preparation_module._GuardedFiles(Path(preparation)) as files:
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
                    observed = _dataset(execution['stdout'], source_fingerprint=bindings['source_fingerprint'], encoded=True)
                    _equal(observed, dataset, 'per-program dataset attestation')
        files.verify()
    return copy.deepcopy(receipt)


def smoke_guarded_hotel_image(image, *, architecture, preparation):
    _require(architecture in original.ARCHITECTURES, 'Unsupported native Hotel architecture.')
    _, _, selector = parse_reference(image)
    _digest(selector, 'immutable platform image', prefix=True)
    platform = original.ARCHITECTURES[architecture][0]
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
    with preparation_module._GuardedFiles(Path(preparation)) as files:
        _, bindings = _preparation(files)
        with tempfile.TemporaryDirectory(prefix='dsb-hotel-guarded-smoke-') as directory:
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
        dataset = None
        for execution in attestations:
            observed = _dataset(execution['stdout'], source_fingerprint=bindings['source_fingerprint'], encoded=True)
            _require(dataset is None or dataset == observed, 'Native Hotel programs attest different seed data.')
            dataset = observed
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
        'dataset_fingerprint': dataset['dataset_fingerprint'], 'passed': True,
    }
    return validate_guarded_hotel_smoke_receipt(receipt, image=image, platform=platform,
                                                config_digest=verified['config_digest'], preparation=preparation)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', required=True)
    parser.add_argument('--architecture', choices=tuple(original.ARCHITECTURES), required=True)
    parser.add_argument('--preparation', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    receipt = smoke_guarded_hotel_image(args.image, architecture=args.architecture, preparation=args.preparation)
    write_receipt(args.output, receipt)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
