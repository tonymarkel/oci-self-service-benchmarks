"""Synthetic native smoke/receipt safety tests; no image or database execution."""

import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from scripts import smoke_deathstarbench_hotel_initialization_images as smoke
from scripts.deathstarbench_artifact_common import PreparationError
from tests.test_deathstarbench_candidate_image_smoke import archive, elf, hotel_fixture, write
from tests.test_deathstarbench_hotel_initialization import metadata_fixture, SOURCE


IMAGE = 'ghcr.io/example/guarded-hotel@sha256:' + 'd' * 64
CONFIG = 'sha256:' + 'e' * 64
TLS_LOG = '{"level":"info","time":"2026-10-15T17:21:31Z","message":"TLS disabled."}\n'


class GuardedHotelSmokeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        context, self.members = hotel_fixture(self.root)
        self.preparation = self.root / 'preparation'
        self.preparation.mkdir()
        context.rename(self.preparation / 'app')
        self.context = self.preparation / 'app'
        self.members['go/bin/hotel-initialize'] = (elf(), 0o755)
        write(self.preparation / 'context-manifest.json', b'{"synthetic":true}\n')
        with smoke.preparation_module._GuardedFiles(self.preparation) as files:
            context_sha = files.tree_digest('app')
        self.manifest = {'contexts': {'app': {'context_sha256': context_sha}},
                         'seed_source': {'source_fingerprint': SOURCE}}
        self.patch = mock.patch.object(smoke, '_validate_preparation', return_value=self.manifest)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.dataset = metadata_fixture()
        self.config = {'User': '65532:65532', 'WorkingDir': '/workspace',
                       'Cmd': ['/go/bin/frontend'], 'Env': smoke.ENVIRONMENT.copy()}
        self.archive = self.root / 'filesystem.tar'
        archive(self.archive, self.members)
        self.commands = []
        self.owned_containers = {}
        self.native_architecture = 'x86_64'

    def execute(self, arguments, **kwargs):
        self.commands.append(arguments)
        if arguments[:3] == ['docker', 'image', 'inspect']:
            architecture = 'amd64' if self.native_architecture == 'x86_64' else 'arm64'
            value = [{'Id': CONFIG, 'Architecture': architecture, 'Os': 'linux', 'Config': self.config}]
            return 0, json.dumps(value).encode(), b''
        if arguments[:2] == ['docker', 'create']:
            name = next(arg.split('=', 1)[1] for arg in arguments if arg.startswith('--name='))
            self.owned_containers[name] = {'Id': '1' * 64, 'Name': '/' + name,
                'Config': {'Image': IMAGE, 'Labels': {smoke.OWNER_LABEL: name.removeprefix('dsb-hotel-offline-')}}}
            return 0, ('1' * 64 + '\n').encode(), b''
        if arguments[:2] == ['docker', 'export']:
            kwargs['stdout_file'].write(self.archive.read_bytes())
            return 0, b'', b''
        if arguments[:2] == ['docker', 'rm']:
            return 0, b'', b''
        if arguments[:3] == ['docker', 'container', 'inspect']:
            name = arguments[3]
            return 0, json.dumps([self.owned_containers[name]]).encode(), b''
        if arguments[:2] == ['docker', 'run']:
            name = next(arg.split('=', 1)[1] for arg in arguments if arg.startswith('--name='))
            identity = hashlib.sha256(name.encode()).hexdigest()
            self.owned_containers[name] = {'Id': identity, 'Name': '/' + name,
                'Config': {'Image': IMAGE, 'Labels': {smoke.OWNER_LABEL: name.removeprefix('dsb-hotel-offline-')}}}
            program = next(arg.split('=', 1)[1] for arg in arguments if arg.startswith('--entrypoint='))
            tls = b'' if program.endswith('/hotel-initialize') else TLS_LOG.encode()
            if arguments[-1] == '--candidate-initialization-attestation':
                return 0, (json.dumps(self.dataset) + '\n').encode(), tls
            return 2, b'', tls + smoke._refusal_stderr(program).encode()
        raise AssertionError(arguments)

    def run_smoke(self, execute=None, *, architecture='x86_64'):
        self.native_architecture = architecture
        verified = {'image': IMAGE, 'config_digest': CONFIG, 'config': {'config': self.config}}
        platform = smoke.original.ARCHITECTURES[architecture][0]
        with mock.patch.object(smoke, '_native_runner', return_value={'platform': platform, 'architecture': architecture, 'qemu_used': False}), \
                mock.patch.object(smoke, 'resolve_platform', return_value=verified), \
                mock.patch.object(smoke, '_execute', side_effect=execute or self.execute):
            return smoke.smoke_guarded_hotel_image(IMAGE, architecture=architecture, preparation=self.preparation)

    def validate(self, receipt):
        return smoke.validate_guarded_hotel_smoke_receipt(receipt, image=IMAGE, platform='linux/amd64',
                                                          config_digest=CONFIG, preparation=self.preparation)

    def test_all_eleven_early_attestations_and_unbound_refusals_are_bound_and_detached(self):
        receipt = self.run_smoke()
        self.assertEqual(len(receipt['attestation_executions']), 11)
        self.assertEqual(len(receipt['unbound_startup_executions']), 11)
        self.assertEqual(receipt['dataset_attestation'], self.dataset)
        self.assertEqual(receipt['artifact_identities']['required_executables'], list(smoke.EXECUTABLES))
        self.assertTrue(all(not receipt[flag] for flag in smoke.FALSE_FLAGS))
        self.assertEqual(self.commands[-1][:3], ['docker', 'rm', '-f'])
        user = receipt['unbound_startup_executions'][-1]
        self.assertIn(smoke._run_arguments(IMAGE, ['/go/bin/user'], user['container_name']), self.commands)
        self.assertIn(['docker', 'rm', '-f', '1' * 64], self.commands)
        clone = self.validate(receipt)
        clone['dataset_attestation']['document_count'] = 0
        self.assertEqual(receipt['dataset_attestation']['document_count'], 873)
        for execution in receipt['attestation_executions'] + receipt['unbound_startup_executions']:
            self.assertNotIn('initialize-empty', execution['command'])
            self.assertNotIn('verify', execution['command'])
            args = execution['docker_arguments']
            self.assertIn('--network=none', args)
            self.assertIn('--workdir=/tmp', args)
            self.assertIn('--pids-limit=32', args)
            self.assertIn('--memory=512m', args)
            self.assertFalse(any(arg.startswith('--env') or arg.startswith('-e=') for arg in args))

    def test_missing_initializer_and_extra_dynamic_or_non_program_artifacts_fail(self):
        mutations = [
            lambda value: value.pop('go/bin/hotel-initialize'),
            lambda value: value.update({'go/bin/hotel-initialize': (elf(dynamic=True), 0o755)}),
            lambda value: value.update({'go/bin/extra': (elf(), 0o755)}),
            lambda value: value.update({'workspace/startup.sh': (b'#!/bin/sh\ngit clone evil\n', 0o755)}),
            lambda value: value.update({'go/bin/hotel-initialize': (elf(), 0o644)}),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                files = copy.deepcopy(self.members)
                mutate(files)
                archive(self.archive, files)
                self.commands.clear()
                with self.assertRaises(PreparationError):
                    self.run_smoke()
                self.assertIn(['docker', 'rm', '-f', '1' * 64], self.commands)
                self.assertFalse(any(args[:2] == ['docker', 'run'] for args in self.commands))

    def test_arm64_native_inventory_keeps_identical_logical_dataset_metadata(self):
        x86 = self.run_smoke()
        files = copy.deepcopy(self.members)
        for path in list(files):
            if path.startswith('go/bin/'):
                files[path] = (elf(183), 0o755)
        archive(self.archive, files)
        arm = self.run_smoke(architecture='aarch64')
        self.assertEqual(arm['dataset_attestation'], x86['dataset_attestation'])
        self.assertEqual(arm['platform'], 'linux/arm64')
        self.assertEqual(arm['architecture'], 'aarch64')
        self.assertNotEqual(arm['artifact_identities']['elf_artifacts']['/go/bin/frontend']['sha256'],
                            x86['artifact_identities']['elf_artifacts']['/go/bin/frontend']['sha256'])
        self.assertEqual({item['machine'] for item in arm['artifact_identities']['elf_artifacts'].values()}, {183})

    def test_export_failure_always_removes_stopped_inspection_container(self):
        def fail(arguments, **kwargs):
            if arguments[:2] == ['docker', 'export']:
                self.commands.append(arguments)
                raise PreparationError('synthetic export failure')
            return self.execute(arguments, **kwargs)
        with self.assertRaisesRegex(PreparationError, 'synthetic export'):
            self.run_smoke(fail)
        self.assertIn(['docker', 'rm', '-f', '1' * 64], self.commands)

    def test_export_create_lost_response_or_malformed_id_still_removes_only_owned_exact_id(self):
        for failure in ('lost', 'malformed'):
            def execute(arguments, **kwargs):
                value = self.execute(arguments, **kwargs)
                if arguments[:2] == ['docker', 'create']:
                    if failure == 'lost':
                        raise PreparationError('synthetic create response lost')
                    return 0, b'malformed ID\n', b''
                return value
            self.commands.clear()
            with self.subTest(failure=failure), self.assertRaises(PreparationError):
                self.run_smoke(execute)
            self.assertIn(['docker', 'rm', '-f', '1' * 64], self.commands)
            self.assertFalse(any(args[:2] == ['docker', 'export'] for args in self.commands))

    def test_export_create_foreign_collision_is_preserved_without_export_or_remove(self):
        def execute(arguments, **kwargs):
            value = self.execute(arguments, **kwargs)
            if arguments[:2] == ['docker', 'create']:
                name = next(arg.split('=', 1)[1] for arg in arguments if arg.startswith('--name='))
                self.owned_containers[name]['Config']['Labels'][smoke.OWNER_LABEL] = 'foreign-owner'
                return 125, b'', b'foreign name collision'
            return value
        with self.assertRaisesRegex(PreparationError, 'foreign or uncertain'):
            self.run_smoke(execute)
        self.assertFalse(any(args[:2] in (['docker', 'rm'], ['docker', 'export']) for args in self.commands))

    def test_every_exec_exit_and_utf8_is_checked_and_startup_cannot_succeed(self):
        for bad in ('attest-status', 'attest-stderr', 'attest-utf8', 'startup-status', 'startup-stdout'):
            def execute(arguments, **kwargs):
                value = self.execute(arguments, **kwargs)
                if arguments[:2] != ['docker', 'run']:
                    return value
                attest = arguments[-1] == '--candidate-initialization-attestation'
                if attest and bad == 'attest-status':
                    return 1, value[1], value[2]
                if attest and bad == 'attest-stderr':
                    return 0, value[1], b'warning'
                if attest and bad == 'attest-utf8':
                    return 0, b'\xff', b''
                if not attest and bad == 'startup-status':
                    return 0, b'', b''
                if not attest and bad == 'startup-stdout':
                    return 2, b'connected', b''
                return value
            with self.subTest(bad=bad), self.assertRaises(PreparationError):
                self.run_smoke(execute)

    def test_timed_out_native_execution_is_force_removed_before_returning_error(self):
        def execute(arguments, **kwargs):
            if arguments[:2] == ['docker', 'run']:
                self.execute(arguments, **kwargs)
                raise PreparationError('synthetic timeout')
            return self.execute(arguments, **kwargs)
        with self.assertRaisesRegex(PreparationError, 'synthetic timeout'):
            self.run_smoke(execute)
        run = next(arguments for arguments in self.commands if arguments[:2] == ['docker', 'run'])
        name = next(argument.split('=', 1)[1] for argument in run if argument.startswith('--name='))
        self.assertIn(['docker', 'rm', '-f', self.owned_containers[name]['Id']], self.commands)

    def test_failed_run_name_collision_never_removes_a_foreign_or_uncertain_container(self):
        for mismatch in ('owner', 'image', 'name', 'id'):
            def execute(arguments, **kwargs):
                if arguments[:2] == ['docker', 'run']:
                    self.execute(arguments, **kwargs)
                    name = next(arg.split('=', 1)[1] for arg in arguments if arg.startswith('--name='))
                    container = self.owned_containers[name]
                    if mismatch == 'owner':
                        container['Config']['Labels'][smoke.OWNER_LABEL] = 'foreign'
                    elif mismatch == 'image':
                        container['Config']['Image'] = 'foreign:latest'
                    elif mismatch == 'name':
                        container['Name'] = '/foreign'
                    else:
                        container['Id'] = 'foreign-id'
                    return 125, b'', b'container name collision'
                return self.execute(arguments, **kwargs)
            self.commands.clear()
            with self.subTest(mismatch=mismatch), self.assertRaisesRegex(PreparationError, 'foreign or uncertain'):
                self.run_smoke(execute)
            # The stopped export container is owned by its returned exact ID;
            # the failed native command's pre-existing/uncertain target is not.
            removals = [args for args in self.commands if args[:2] == ['docker', 'rm']]
            self.assertEqual(removals, [['docker', 'rm', '-f', '1' * 64]])

    def test_cleanup_absence_must_be_explicit_not_a_permission_or_daemon_error(self):
        name = 'dsb-hotel-offline-' + 'a' * 32
        with mock.patch.object(smoke, '_execute', return_value=(1, b'[]\n', ('Error: No such object: ' + name + '\n').encode())) as execute:
            smoke._cleanup_execution(name, IMAGE)
            self.assertEqual(execute.call_count, 1)
        with mock.patch.object(smoke, '_execute', return_value=(1, b'', b'permission denied')) as execute, \
                self.assertRaisesRegex(PreparationError, 'Cannot prove'):
            smoke._cleanup_execution(name, IMAGE)
        self.assertEqual(execute.call_count, 1)

    def test_receipt_rejects_false_controls_cross_identity_and_qualification_overclaims(self):
        original = self.run_smoke()
        mutations = [
            lambda r: r.update(extra='not allowed'),
            lambda r: r.update(image=IMAGE.replace('d' * 64, 'c' * 64)),
            lambda r: r.update(platform='linux/arm64'),
            lambda r: r.update(config_digest='sha256:' + 'c' * 64),
            lambda r: r.update(preparation_manifest_sha256='0' * 64),
            lambda r: r.update(context_sha256='f' * 64),
            lambda r: r.update(source_fingerprint='sha256:' + 'f' * 64),
            lambda r: r.update(recipe_revision='legacy'),
            lambda r: r['native_runner'].update(qemu_used=True),
            lambda r: r['execution_controls'].update(network='bridge'),
            lambda r: r['execution_controls'].update(memory_bytes=True),
            lambda r: r['execution_controls'].update(identity_environment_supplied=True),
            lambda r: r.update(methods=[]),
            lambda r: r['filesystem_inspection'].update(started=True),
            lambda r: r['filesystem_inspection'].update(ownership_verified_before_export=False),
            lambda r: r['filesystem_inspection']['create_arguments'].remove('--read-only'),
            lambda r: r['filesystem_inspection'].update(export_arguments=['docker', 'export', 'foreign']),
            lambda r: r['artifact_identities']['elf_artifacts'].pop('/go/bin/hotel-initialize'),
            lambda r: r['artifact_identities']['elf_artifacts']['/go/bin/hotel-initialize'].update(static=False),
            lambda r: r['artifact_identities']['elf_artifacts']['/go/bin/frontend'].update(machine=183),
            lambda r: r['artifact_identities']['elf_artifacts']['/go/bin/frontend'].update(sha256='0' * 64),
            lambda r: r['artifact_identities']['baked_file_sha256'].update({'/workspace/config.json': 'f' * 64}),
            lambda r: r['attestation_executions'].pop(),
            lambda r: r['attestation_executions'].reverse(),
            lambda r: r['attestation_executions'][0]['docker_arguments'].remove('--network=none'),
            lambda r: r['attestation_executions'][0]['docker_arguments'].append('--privileged'),
            lambda r: r['attestation_executions'][0].update(container_name='external-user-container'),
            lambda r: r['attestation_executions'][0].update(stdout_sha256='0' * 64),
            lambda r: r['attestation_executions'][0].update(elf_sha256='f' * 64),
            lambda r: r['attestation_executions'][0].update(exit_code=True),
            lambda r: r['unbound_startup_executions'][0].update(exit_code=0),
            lambda r: r['dataset_attestation'].update(initialization_executed=True),
            lambda r: r.update(dataset_fingerprint='sha256:' + 'f' * 64),
        ]
        mutations += [(lambda flag: lambda r: r.update({flag: True}))(flag) for flag in smoke.FALSE_FLAGS]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                receipt = copy.deepcopy(original)
                mutate(receipt)
                with self.assertRaises(PreparationError):
                    self.validate(receipt)

    def test_duplicate_or_promoted_dataset_stdout_is_rejected_even_with_matching_output_hash(self):
        original = self.run_smoke()
        for output in (
            '{"schema_version":1,"schema_version":1}',
            json.dumps({**self.dataset, 'initialization_executed': True}),
            json.dumps(self.dataset) + '\n' + json.dumps(self.dataset),
            '\ufeff' + json.dumps(self.dataset),
        ):
            value = copy.deepcopy(original)
            execution = value['attestation_executions'][0]
            execution['stdout'] = output
            execution['stdout_sha256'] = hashlib.sha256(output.encode()).hexdigest()
            with self.subTest(output=output[:40]), self.assertRaises(PreparationError):
                self.validate(value)

    def test_baked_scratch_config_cannot_carry_runtime_authority_or_side_effects(self):
        for mutation in (
            {'Env': smoke.ENVIRONMENT + ['HOTEL_CANDIDATE_RUN_ID=123456abcdef']},
            {'Env': ['TLS=false']}, {'Cmd': ['/go/bin/hotel-initialize', 'initialize-empty']},
            {'Entrypoint': ['/bin/sh']}, {'Volumes': {'/data': {}}},
            {'Healthcheck': {'Test': ['CMD', '/go/bin/frontend']}},
            {'User': 'root'}, {'WorkingDir': '/tmp'}, {'OnBuild': ['RUN echo mutation']},
            {'Shell': ['/bin/sh']}, {'Labels': {'promoted': 'true'}},
            {'NetworkDisabled': True}, {'Unrecognized': True},
        ):
            with self.subTest(mutation=mutation):
                value = {**self.config, **mutation}
                with self.assertRaises(PreparationError):
                    smoke._runtime_config(value)

    def test_exact_provenance_labels_are_allowed_but_arbitrary_labels_are_not(self):
        labels = {'org.opencontainers.image.source': 'https://github.com/example/repo',
                  'org.opencontainers.image.revision': 'a' * 40}
        self.assertEqual(smoke._runtime_config({**self.config, 'Labels': labels})['Labels'], labels)
        for change in (
            {'org.opencontainers.image.source': 'https://attacker.invalid/repo'},
            {'org.opencontainers.image.source': 'https://github.com/example/repo?token=secret'},
            {'org.opencontainers.image.revision': 'latest'},
            {'org.opencontainers.image.revision': '0' * 40},
            {'other': 'hidden'},
        ):
            with self.subTest(change=change), self.assertRaises(PreparationError):
                smoke._runtime_config({**self.config, 'Labels': {**labels, **change}})

    def test_actual_linux_buildkit_path_and_legacy_args_escaped_are_narrowly_accepted(self):
        actual = {**self.config, 'ArgsEscaped': True,
                  'Labels': {'org.opencontainers.image.source': 'https://github.com/example/repo',
                             'org.opencontainers.image.revision': 'a' * 40}}
        self.assertEqual(smoke._runtime_config(actual), actual)
        self.assertEqual(smoke._runtime_config({**actual, 'ArgsEscaped': False})['ArgsEscaped'], False)
        for env in (
            ['PATH=/tmp:/usr/bin', *smoke.ENVIRONMENT[1:]],
            smoke.ENVIRONMENT[1:],
            [*smoke.ENVIRONMENT, 'PATH=/tmp'],
            [*smoke.ENVIRONMENT, 'HOTEL_CANDIDATE_RUN_ID=123456abcdef'],
        ):
            with self.subTest(env=env), self.assertRaises(PreparationError):
                smoke._runtime_config({**actual, 'Env': env})
        for escaped in (0, 1, 'true', None, [], {}):
            with self.subTest(args_escaped=escaped), self.assertRaises(PreparationError):
                smoke._runtime_config({**actual, 'ArgsEscaped': escaped})

    def test_only_audited_bounded_tls_event_is_permitted_before_service_main(self):
        smoke._validate_stderr(TLS_LOG, path='/go/bin/frontend', attest=True)
        smoke._validate_stderr(TLS_LOG + smoke._refusal_stderr('/go/bin/frontend'), path='/go/bin/frontend', attest=False)
        for changed in (
            '', TLS_LOG + TLS_LOG, TLS_LOG.replace('info', 'error'), TLS_LOG.replace('TLS disabled.', 'connected MongoDB'),
            TLS_LOG.replace('2026-10-15', '2026-99-99'), TLS_LOG.replace('17:21:31Z', '17:21:31'),
            TLS_LOG.replace('"level":"info"', '"level":"info","level":"info"'),
            TLS_LOG.replace('"message":', '"extra":"unsafe","message":'),
        ):
            with self.subTest(changed=changed), self.assertRaises(PreparationError):
                smoke._validate_stderr(changed, path='/go/bin/frontend', attest=True)

    def test_no_mutating_command_is_needed_and_receipt_cannot_substitute_different_dataset(self):
        receipt = self.run_smoke()
        execution = receipt['attestation_executions'][0]
        changed = copy.deepcopy(self.dataset)
        changed['collections'][0]['content_sha256'] = 'sha256:' + 'f' * 64
        identity = {key: value for key, value in changed.items() if key != 'dataset_fingerprint'}
        changed['dataset_fingerprint'] = 'sha256:' + hashlib.sha256(smoke._canonical(identity).encode()).hexdigest()
        execution['stdout'] = json.dumps(changed)
        execution['stdout_sha256'] = hashlib.sha256(execution['stdout'].encode()).hexdigest()
        with self.assertRaisesRegex(PreparationError, 'per-program dataset'):
            self.validate(receipt)

    def test_native_host_and_daemon_must_both_match_and_index_is_not_executable_reference(self):
        for system, machine, daemon in (
            ('Darwin', 'arm64', {'OSType': 'linux', 'Architecture': 'arm64'}),
            ('Linux', 'aarch64', {'OSType': 'linux', 'Architecture': 'amd64'}),
            ('Linux', 'x86_64', {'OSType': 'linux', 'Architecture': 'arm64'}),
        ):
            with self.subTest(system=system, machine=machine), \
                    mock.patch.object(smoke.original.host_platform, 'system', return_value=system), \
                    mock.patch.object(smoke.original.host_platform, 'machine', return_value=machine), \
                    mock.patch.object(smoke, '_command', return_value=json.dumps(daemon).encode()), \
                    self.assertRaises(PreparationError):
                smoke._native_runner('x86_64')
        with self.assertRaises(PreparationError):
            smoke.smoke_guarded_hotel_image('ghcr.io/example/guarded-hotel:latest', architecture='x86_64', preparation=self.preparation)


class BoundedSubprocessTests(unittest.TestCase):
    def test_stdout_stderr_are_bounded_and_timeout_kills_child(self):
        for command, kwargs in (
            ([sys.executable, '-c', 'import sys; sys.stdout.write("x" * 10000)'], {'stdout_limit': 100}),
            ([sys.executable, '-c', 'import sys; sys.stderr.write("x" * 20000)'], {}),
            ([sys.executable, '-c', 'import time; time.sleep(3)'], {'timeout': 0.01}),
        ):
            with self.subTest(command=command), self.assertRaises(PreparationError):
                smoke._execute(command, **kwargs)

    def test_success_exit_and_bounded_file_export(self):
        status, out, err = smoke._execute([sys.executable, '-c', 'import sys; print("hello"); sys.exit(2)'])
        self.assertEqual((status, out, err), (2, b'hello\n', b''))
        with tempfile.TemporaryFile() as stream:
            status, out, err = smoke._execute([sys.executable, '-c', 'print("export")'], stdout_file=stream, stdout_limit=100)
            stream.seek(0)
            self.assertEqual(stream.read(), b'export\n')
            self.assertEqual((status, out, err), (0, b'', b''))


if __name__ == '__main__':
    unittest.main()
