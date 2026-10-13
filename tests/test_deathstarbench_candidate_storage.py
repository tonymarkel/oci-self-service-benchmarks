"""Candidate storage contracts and a portable, unprivileged guest simulation.

The shell harness exercises control flow against real temporary files while
stubbing Linux-only privileged inventory/label tools. It is not live-cloud,
SELinux, disk-capacity or runtime qualification evidence.
"""

from dataclasses import replace
import copy
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from app import deathstarbench_candidate_storage as storage
from app import deathstarbench_candidate_workload as candidate_workload
from app.deathstarbench_workload_contract import distributed_workload_profile


UUID = '12345678-1234-5678-9abc-123456789abc'
RUN = 'a1b2c3d4e5f6'
CLAIMS = 80 * 1024**3
HOTEL_NAMES = (
    'mongodb-attractions', 'mongodb-geo', 'mongodb-profile', 'mongodb-rate',
    'mongodb-recommendation', 'mongodb-reservation', 'mongodb-review', 'mongodb-user',
)
MEDIA_NAMES = (
    'cast-info-mongodb', 'movie-id-mongodb', 'movie-info-mongodb', 'plot-mongodb',
    'review-storage-mongodb', 'user-mongodb', 'user-review-mongodb', 'movie-review-mongodb',
)


def contract(workload='hotel_reservation', run=RUN, uuid=UUID):
    return storage.candidate_storage_contract(workload, run, uuid)


def marker(scope, **changes):
    fields = {
        'run': scope.run_id, 'workload': scope.workload_id, 'namespace': scope.namespace,
        'uuid': scope.filesystem_uuid, 'root': storage.DATABASE_ROOT,
        'contract_sha256': scope.sha256, 'inventory_sha256': scope.inventory_sha256,
        'total_bytes': 100 * 1024**3, 'available_bytes': 90 * 1024**3,
    }
    fields.update(changes)
    return (
        f'{storage.ATTESTATION_MARKER} schema=candidate-storage-v1 candidate_only=1 '
        'released=0 runtime_qualified=0 '
        f"run={fields['run']} workload={fields['workload']} namespace={fields['namespace']} "
        f"uuid={fields['uuid']} root={fields['root']} contract_sha256={fields['contract_sha256']} "
        f"inventory_sha256={fields['inventory_sha256']} databases=8 claim_bytes={CLAIMS} "
        f"total_bytes={fields['total_bytes']} available_bytes={fields['available_bytes']} "
        'filesystem=xfs selinux=container_file_t'
    )


class CandidateStorageContractTests(unittest.TestCase):
    def test_exact_audited_eight_directory_and_claim_contracts(self):
        for workload, names, namespace in (
            ('hotel_reservation', HOTEL_NAMES, 'deathstarbench-hotel'),
            ('media_microservices', MEDIA_NAMES, 'deathstarbench-media'),
        ):
            with self.subTest(workload=workload):
                scope = contract(workload)
                self.assertEqual(scope.database_names, tuple(sorted(names)))
                self.assertEqual(scope.namespace, namespace)
                metadata = scope.metadata()
                self.assertEqual(metadata['total_claim_bytes'], CLAIMS)
                self.assertEqual(metadata['claim_bytes_per_database'], 10 * 1024**3)
                self.assertEqual(metadata['database_count'], 8)
                self.assertTrue(metadata['candidate_only'])
                self.assertFalse(metadata['released'])
                self.assertFalse(metadata['runtime_qualified'])
                self.assertEqual(metadata['root'], storage.DATABASE_ROOT)
                self.assertEqual(metadata['mount_point'], storage.MOUNT_POINT)
                without_digest = dict(metadata)
                without_digest.pop('storage_contract_sha256')
                encoded = json.dumps(without_digest, sort_keys=True, separators=(',', ':')).encode()
                self.assertEqual(scope.sha256, hashlib.sha256(encoded).hexdigest())

    def test_claims_and_directory_paths_match_actual_candidate_renderer(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            with self.subTest(workload=workload):
                scope = contract(workload)
                profile = distributed_workload_profile(workload)
                image_preview = {
                    'schema_version': candidate_workload.CANDIDATE_IMAGE_SCHEMA_VERSION,
                    'workload_id': workload, 'workload_revision': profile.workload_revision,
                    'image_set_revision': profile.image_set_revision,
                    'upstream_revision': candidate_workload.UPSTREAM_REVISION, 'released': False,
                    'platforms': {
                        platform: {'images': {
                            key: f'registry.example/{workload}/{key}@sha256:' + hashlib.sha256(
                                f'test-storage-only:{platform}:{key}'.encode()
                            ).hexdigest()
                            for key in profile.required_image_keys
                        }}
                        for platform in ('linux/amd64', 'linux/arm64')
                    },
                }
                rendered = candidate_workload.render_candidate_workload_bundle(
                    workload, image_preview, 'aarch64', '10.240.2.10',
                )
                documents = json.loads(rendered.canonical_json)['items']
                volumes = [item for item in documents if item['kind'] == 'PersistentVolume']
                claims = [item for item in documents if item['kind'] == 'PersistentVolumeClaim']
                self.assertEqual(len(volumes), 8)
                self.assertEqual(len(claims), 8)
                self.assertEqual(
                    sorted(item['spec']['hostPath']['path'] for item in volumes),
                    [f'{storage.DATABASE_ROOT}/{name}' for name in scope.database_names],
                )
                self.assertEqual({item['spec']['capacity']['storage'] for item in volumes}, {'10Gi'})
                self.assertEqual({item['spec']['resources']['requests']['storage'] for item in claims}, {'10Gi'})

    def test_identity_is_stable_frozen_and_returned_metadata_is_detached(self):
        scope = contract()
        self.assertEqual(scope, contract())
        self.assertEqual(scope.sha256, contract().sha256)
        metadata = scope.metadata()
        metadata['database_names'].append('foreign')
        self.assertEqual(len(scope.database_names), 8)
        self.assertEqual(scope, contract())
        with self.assertRaises(AttributeError):
            scope.run_id = '000000000000'

    def test_run_workload_and_disk_change_contract_digest(self):
        scopes = [contract(), contract(run='000000000001'), contract('media_microservices'),
                  contract(uuid='12345678-1234-5678-9abc-123456789abd')]
        self.assertEqual(len({scope.sha256 for scope in scopes}), 4)

    def test_bad_identity_and_social_are_rejected(self):
        for workload, run, uuid in (
            ('social_network', RUN, UUID), ('unknown', RUN, UUID),
            ('hotel_reservation', RUN.upper(), UUID), ('hotel_reservation', RUN + '0', UUID),
            ('hotel_reservation', '../run', UUID), ('hotel_reservation', True, UUID),
            ('hotel_reservation', '000000000000', UUID),
            ('hotel_reservation', RUN, UUID.upper()), ('hotel_reservation', RUN, UUID + '\n'),
            ('hotel_reservation', RUN, None),
            ('hotel_reservation', RUN, '00000000-0000-0000-0000-000000000000'),
        ):
            with self.subTest(workload=workload, run=run, uuid=uuid):
                with self.assertRaises(storage.CandidateStorageError):
                    storage.candidate_storage_contract(workload, run, uuid)

    def test_asset_drift_and_wrong_directory_count_fail_closed(self):
        with patch.object(storage, 'validate_candidate_assets', side_effect=ValueError('asset drift')):
            with self.assertRaisesRegex(storage.CandidateStorageError, 'asset drift'):
                contract()
        with patch.object(storage, 'validate_candidate_assets', return_value=([], ())):
            with self.assertRaisesRegex(storage.CandidateStorageError, 'eight distinct'):
                contract()

    def test_manual_contract_field_changes_are_revalidated(self):
        scope = contract()
        for changed in (
            replace(scope, namespace='foreign'), replace(scope, workload_revision='foreign'),
            replace(scope, component_asset_sha256='0' * 64),
            replace(scope, database_names=scope.database_names[:-1] + ('foreign',)),
        ):
            with self.subTest(changed=changed):
                with self.assertRaises(storage.CandidateStorageError):
                    storage.candidate_database_storage_prepare_command(changed)
        with self.assertRaises(storage.CandidateStorageError):
            storage.candidate_database_storage_prepare_command(scope.metadata())


class CandidateStorageAttestationTests(unittest.TestCase):
    def test_exact_marker_and_saved_evidence_remain_candidate_only(self):
        scope = contract()
        receipt = storage.parse_candidate_database_storage_attestation(
            ('harmless UTF-8 tool output: pr\u00e9paration \u2713\n' + marker(scope) + '\n').encode(), scope,
        )
        self.assertEqual(receipt['database_names'], list(HOTEL_NAMES))
        self.assertEqual(receipt['available_bytes'], 90 * 1024**3)
        self.assertFalse(receipt['runtime_qualified'])
        self.assertEqual(
            storage.validate_candidate_database_storage_attestation(receipt, scope), receipt,
        )

    def test_duplicate_partial_prefixed_or_trailing_markers_are_rejected(self):
        scope = contract()
        exact = marker(scope)
        for output in (
            '', exact + '\n' + exact, exact + ' junk', ' ' + exact,
            exact + '\n' + storage.ATTESTATION_MARKER + ' malformed',
            exact.replace('released=0', 'released=1'), exact.replace('databases=8', 'databases=6'),
            exact.replace('filesystem=xfs', 'filesystem=ext4'), exact + '\r\n',
            exact + '\0', b'\xff', exact.replace('schema=candidate-storage-v1', 'schema=public-v1'),
        ):
            with self.subTest(output=repr(output)[:100]):
                with self.assertRaises(storage.CandidateStorageError):
                    storage.parse_candidate_database_storage_attestation(output, scope)

    def test_crossed_identity_and_bad_space_are_rejected(self):
        scope = contract()
        for changes in (
            {'run': '000000000001'}, {'workload': 'media_microservices'},
            {'namespace': 'deathstarbench-social'}, {'uuid': UUID.upper()},
            {'root': '/data'}, {'contract_sha256': '0' * 64}, {'inventory_sha256': '0' * 64},
            {'available_bytes': CLAIMS - 1}, {'total_bytes': CLAIMS - 1},
            {'available_bytes': 101 * 1024**3}, {'total_bytes': True},
            {'available_bytes': '00085899345920'}, {'total_bytes': 10**18},
        ):
            with self.subTest(changes=changes):
                with self.assertRaises(storage.CandidateStorageError):
                    storage.parse_candidate_database_storage_attestation(marker(scope, **changes), scope)

    def test_saved_schema_numbers_and_identity_are_strict(self):
        scope = contract()
        receipt = storage.parse_candidate_database_storage_attestation(marker(scope), scope)
        for key, value in (
            ('schema_version', True), ('candidate_only', 1), ('released', 0),
            ('runtime_qualified', True), ('run_id', '000000000001'),
            ('database_names', tuple(HOTEL_NAMES)), ('database_count', 8.0),
            ('total_claim_bytes', float(CLAIMS)), ('available_bytes', True),
            ('total_bytes', 10**18), ('filesystem', 'ext4'), ('selinux_type', 'default_t'),
            ('extra', True),
        ):
            changed = copy.deepcopy(receipt)
            changed[key] = value
            with self.subTest(key=key, value=value):
                with self.assertRaises(storage.CandidateStorageError):
                    storage.validate_candidate_database_storage_attestation(changed, scope)
        changed = dict(receipt)
        changed.pop('available_bytes')
        with self.assertRaises(storage.CandidateStorageError):
            storage.validate_candidate_database_storage_attestation(changed, scope)


_GUEST_TOOL = r'''import os, pathlib, stat, sys
tool = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
mount = pathlib.Path(os.environ['GUEST_MOUNT'])
uuid = os.environ.get('GUEST_UUID', '12345678-1234-5678-9abc-123456789abc')
if tool == 'id':
    print(os.environ.get('GUEST_OWNER', '0'))
elif tool == 'findmnt':
    if 'FSTYPE' in args: print(os.environ.get('GUEST_FS', 'xfs'))
    elif 'UUID' in args: print(uuid)
    elif 'TARGET' in args:
        print(mount)
        if os.environ.get('GUEST_NESTED'): print(mount / 'mongodb' / 'nested')
    else: sys.exit(1)
elif tool == 'readlink':
    print(os.path.realpath(args[-1]))
elif tool == 'stat':
    item = pathlib.Path(args[-1])
    details = item.stat()
    mode = args[1]
    if mode == '%u:%g:%a': print(os.environ.get('GUEST_STAT_OWNER', '0:0') + ':%o' % stat.S_IMODE(details.st_mode))
    elif mode == '%u:%g:%a:%h:%s':
        print('0:0:%o:%d:%d' % (stat.S_IMODE(details.st_mode), details.st_nlink, details.st_size))
    elif mode == '%C': print('system_u:object_r:%s:s0' % os.environ.get('GUEST_LABEL', 'container_file_t'))
    else: sys.exit(1)
elif tool == 'df':
    print('1B-blocks Avail')
    print(os.environ.get('GUEST_TOTAL', str(100*1024**3)), os.environ.get('GUEST_AVAILABLE', str(90*1024**3)))
elif tool == 'find':
    root = pathlib.Path(args[0])
    if '-maxdepth' in args:
        for item in root.iterdir(): print(item.name)
    else:
        for base, dirs, files in os.walk(root, followlinks=False):
            for name in dirs + files:
                item = pathlib.Path(base) / name
                details = item.lstat()
                if stat.S_ISLNK(details.st_mode) or not (stat.S_ISDIR(details.st_mode) or stat.S_ISREG(details.st_mode)) or (stat.S_ISREG(details.st_mode) and details.st_nlink > 1):
                    print(item)
                    sys.exit(0)
elif tool == 'install':
    item = pathlib.Path(args[-1])
    if os.environ.get('GUEST_FAIL_INSTALL') == item.name: sys.exit(1)
    item.mkdir(mode=0o755, parents=True, exist_ok=True)
elif tool in ('semanage', 'restorecon'):
    with open(os.environ['GUEST_ACTIONS'], 'a') as log: log.write(tool + ' ' + ' '.join(args) + '\n')
else: sys.exit(1)
'''


@unittest.skipUnless(shutil.which('bash'), 'Bash is required for shell contract tests')
class CandidateStorageGuestCommandTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.mount = self.root / 'disk'
        self.mount.mkdir()
        self.tools = self.root / 'tools'
        self.tools.mkdir()
        for name in ('id', 'findmnt', 'readlink', 'stat', 'df', 'find', 'install', 'semanage', 'restorecon'):
            path = self.tools / name
            path.write_text(f'#!{sys.executable}\n' + _GUEST_TOOL)
            path.chmod(0o755)
        self.actions = self.root / 'actions'

    def run_guest(self, scope=None, **env):
        command = storage.candidate_database_storage_prepare_command(scope or contract())
        argv = shlex.split(command)
        self.assertEqual(argv[:3], ['sudo', 'bash', '-c'])
        shell = argv[3].replace(storage.MOUNT_POINT, str(self.mount))
        result = subprocess.run(
            [shutil.which('bash'), '-c', shell], text=True, capture_output=True, timeout=20,
            env={**os.environ, 'PATH': f'{self.tools}{os.pathsep}{os.environ["PATH"]}',
                 'GUEST_MOUNT': str(self.mount), 'GUEST_ACTIONS': str(self.actions), **env},
        )
        return result

    def assert_safe_failure(self, result):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertNotIn(storage.ATTESTATION_MARKER, result.stdout)

    def test_real_generated_shell_has_valid_bash_syntax_and_no_destructive_storage_commands(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            command = storage.candidate_database_storage_prepare_command(contract(workload))
            result = subprocess.run([shutil.which('bash'), '-n', '-c', command], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            for forbidden in ('mkfs', 'wipefs', 'umount', 'rm -rf', 'mount -', 'truncate', 'dd if='):
                self.assertNotIn(forbidden, command)
            self.assertIn('ln "$TEMP_MARKER" "$MARKER"', command)
            self.assertIn('restorecon -R -x "$DATA_ROOT"', command)

    def test_new_and_same_identity_repeat_preserve_data(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            with self.subTest(workload=workload):
                if list(self.mount.iterdir()):
                    self.mount = self.root / 'media-disk'
                    self.mount.mkdir()
                scope = contract(workload)
                first = self.run_guest(scope)
                self.assertEqual(first.returncode, 0, first.stderr)
                database_root = self.mount / 'mongodb'
                self.assertEqual(sorted(item.name for item in database_root.iterdir()), list(scope.database_names))
                data = database_root / scope.database_names[0] / 'WiredTiger'
                data.write_bytes(b'owned-existing-data')
                old_marker = (self.mount / Path(storage.OWNERSHIP_MARKER).name).read_bytes()
                second = self.run_guest(scope)
                self.assertEqual(second.returncode, 0, second.stderr)
                self.assertEqual(data.read_bytes(), b'owned-existing-data')
                self.assertEqual((self.mount / Path(storage.OWNERSHIP_MARKER).name).read_bytes(), old_marker)
                receipt = storage.parse_candidate_database_storage_attestation(
                    second.stdout.replace(str(self.mount), storage.MOUNT_POINT), scope,
                )
                self.assertEqual(receipt['database_count'], 8)

    def test_interrupted_owned_directory_creation_can_retry_same_scope(self):
        failure = self.run_guest(GUEST_FAIL_INSTALL='mongodb-profile')
        self.assert_safe_failure(failure)
        self.assertTrue((self.mount / Path(storage.OWNERSHIP_MARKER).name).is_file())
        success = self.run_guest()
        self.assertEqual(success.returncode, 0, success.stderr)

    def test_cross_run_workload_disk_markers_and_marker_permissions_reject_reuse(self):
        self.assertEqual(self.run_guest().returncode, 0)
        for scope, env in (
            (contract(run='000000000001'), {}), (contract('media_microservices'), {}),
            (contract(uuid='12345678-1234-5678-9abc-123456789abd'),
             {'GUEST_UUID': '12345678-1234-5678-9abc-123456789abd'}),
        ):
            with self.subTest(scope=scope):
                self.assert_safe_failure(self.run_guest(scope, **env))
        marker_path = self.mount / Path(storage.OWNERSHIP_MARKER).name
        marker_path.chmod(0o644)
        self.assert_safe_failure(self.run_guest())

    def test_unknown_unowned_files_and_legacy_social_marker_are_not_deleted(self):
        unknown = self.mount / 'unowned'
        unknown.write_bytes(b'keep')
        self.assert_safe_failure(self.run_guest())
        self.assertEqual(unknown.read_bytes(), b'keep')
        unknown.unlink()
        legacy = self.mount / Path(storage.LEGACY_SOCIAL_MARKER).name
        legacy.write_bytes(b'social-marker')
        self.assert_safe_failure(self.run_guest())
        self.assertEqual(legacy.read_bytes(), b'social-marker')

    def test_unsafe_mount_owner_space_uuid_and_labels_fail(self):
        for env in (
            {'GUEST_OWNER': '501'}, {'GUEST_STAT_OWNER': '501:20'},
            {'GUEST_FS': 'ext4'}, {'GUEST_UUID': '12345678-1234-5678-9abc-123456789abd'},
            {'GUEST_NESTED': '1'}, {'GUEST_TOTAL': str(CLAIMS - 1)},
            {'GUEST_AVAILABLE': str(CLAIMS - 1)}, {'GUEST_AVAILABLE': 'garbage'},
        ):
            with self.subTest(env=env):
                self.assert_safe_failure(self.run_guest(**env))
                self.assertFalse((self.mount / Path(storage.OWNERSHIP_MARKER).name).exists())
        self.assert_safe_failure(self.run_guest(GUEST_LABEL='default_t'))

    def test_mount_and_database_root_writable_permissions_are_rejected(self):
        self.mount.chmod(0o777)
        self.assert_safe_failure(self.run_guest())
        self.assertFalse((self.mount / Path(storage.OWNERSHIP_MARKER).name).exists())
        self.mount.chmod(0o755)
        self.assertEqual(self.run_guest().returncode, 0)
        database_root = self.mount / 'mongodb'
        database_root.chmod(0o777)
        before = self.actions.read_text()
        self.assert_safe_failure(self.run_guest())
        self.assertEqual(self.actions.read_text(), before)
        self.assertEqual(database_root.stat().st_mode & 0o777, 0o777)

    def test_unknown_database_entry_symlink_and_hardlink_are_not_mutated(self):
        self.assertEqual(self.run_guest().returncode, 0)
        database_root = self.mount / 'mongodb'
        unknown = database_root / 'foreign'
        unknown.write_bytes(b'keep')
        self.assert_safe_failure(self.run_guest())
        self.assertEqual(unknown.read_bytes(), b'keep')
        unknown.unlink()
        link = database_root / HOTEL_NAMES[0] / 'outside'
        link.symlink_to(self.tools, target_is_directory=True)
        self.assert_safe_failure(self.run_guest())
        self.assertTrue(link.is_symlink())
        link.unlink()
        regular = database_root / HOTEL_NAMES[0] / 'data'
        regular.write_bytes(b'keep')
        hardlink = database_root / HOTEL_NAMES[0] / 'second-link'
        os.link(regular, hardlink)
        self.assert_safe_failure(self.run_guest())
        self.assertEqual(regular.read_bytes(), b'keep')

    def test_marker_and_root_dangling_symlinks_reject_before_creation(self):
        marker_path = self.mount / Path(storage.OWNERSHIP_MARKER).name
        marker_path.symlink_to(self.root / 'absent')
        self.assert_safe_failure(self.run_guest())
        self.assertFalse((self.mount / 'mongodb').exists())
        marker_path.unlink()
        self.mount.rmdir()
        self.mount.symlink_to(self.tools, target_is_directory=True)
        self.assert_safe_failure(self.run_guest())


if __name__ == '__main__':
    unittest.main()
