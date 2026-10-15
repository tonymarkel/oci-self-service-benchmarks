"""Read-only storage receipts and a portable, stubbed guest simulation.

The harness uses real temporary files with the earlier Linux privileged-tool
stubs. It is not live-cloud, filesystem-capacity, SELinux or runtime evidence.
"""

import copy
from dataclasses import replace
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import unittest

from app import deathstarbench_candidate_storage as storage
from app import deathstarbench_candidate_storage_observation as observation
from tests import test_deathstarbench_candidate_storage as fixtures


def marker(contract, **changes):
    values = {
        'run': contract.run_id, 'workload': contract.workload_id,
        'namespace': contract.namespace, 'uuid': contract.filesystem_uuid,
        'root': storage.DATABASE_ROOT, 'contract_sha256': contract.sha256,
        'inventory_sha256': contract.inventory_sha256,
        'total_bytes': 100 * 1024**3, 'available_bytes': 0,
    }
    values.update(changes)
    return (
        f'{observation.OBSERVATION_MARKER} schema={observation.OBSERVATION_SCHEMA} '
        'candidate_only=1 released=0 runtime_qualified=0 dataset_qualified=0 '
        'measurement_qualified=0 read_only=1 '
        f"run={values['run']} workload={values['workload']} namespace={values['namespace']} "
        f"uuid={values['uuid']} root={values['root']} contract_sha256={values['contract_sha256']} "
        f"inventory_sha256={values['inventory_sha256']} databases=8 claim_bytes={fixtures.CLAIMS} "
        f"total_bytes={values['total_bytes']} available_bytes={values['available_bytes']} "
        'filesystem=xfs selinux=container_file_t'
    )


class CandidateStorageObservationTests(unittest.TestCase):
    def test_zero_or_consumed_free_space_is_observed_without_requalifying_initial_budget(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            contract = fixtures.contract(workload)
            for available in (0, 1, fixtures.CLAIMS - 1, 90 * 1024**3):
                with self.subTest(workload=workload, available=available):
                    receipt = observation.parse_candidate_database_storage_observation(
                        ('harmless output\n' + marker(contract, available_bytes=available) + '\n').encode(), contract,
                    )
                    self.assertEqual(receipt['available_bytes'], available)
                    self.assertEqual(receipt['total_claim_bytes'], fixtures.CLAIMS)
                    self.assertEqual(receipt['candidate_schema'], observation.OBSERVATION_SCHEMA)
                    self.assertIs(receipt['read_only'], True)
                    self.assertTrue(receipt['candidate_only'])
                    for flag in ('released', 'runtime_qualified', 'dataset_qualified', 'measurement_qualified'):
                        self.assertIs(receipt[flag], False)
                    self.assertEqual(
                        observation.validate_saved_candidate_database_storage_observation(receipt, contract), receipt,
                    )
                    receipt['database_names'].clear()
                    self.assertEqual(len(contract.database_names), 8)

    def test_crossed_identity_and_invalid_capacity_are_rejected(self):
        contract = fixtures.contract()
        for change in (
            {'run': '000000000001'}, {'workload': 'media_microservices'},
            {'namespace': 'deathstarbench-social'}, {'uuid': fixtures.UUID.upper()},
            {'root': '/data'}, {'contract_sha256': '0' * 64}, {'inventory_sha256': '0' * 64},
            {'total_bytes': fixtures.CLAIMS - 1}, {'total_bytes': 0}, {'total_bytes': 10**18},
            {'total_bytes': True}, {'available_bytes': -1}, {'available_bytes': 101 * 1024**3},
            {'available_bytes': '00'}, {'available_bytes': True},
        ):
            with self.subTest(change=change), self.assertRaises(observation.CandidateStorageObservationError):
                observation.parse_candidate_database_storage_observation(marker(contract, **change), contract)

    def test_missing_duplicate_prefixed_malformed_or_promoted_markers_are_rejected(self):
        contract = fixtures.contract()
        exact = marker(contract)
        for output in (
            '', exact + '\n' + exact, ' ' + exact, exact + ' junk', exact + '\r\n',
            exact + '\0', b'\xff', exact.replace('read_only=1', 'read_only=0'),
            exact.replace('dataset_qualified=0', 'dataset_qualified=1'),
            exact.replace('released=0', 'released=1'), exact.replace('databases=8', 'databases=6'),
            exact.replace('filesystem=xfs', 'filesystem=ext4'),
            exact.replace('selinux=container_file_t', 'selinux=default_t'),
            fixtures.marker(contract), exact + '\n' + observation.OBSERVATION_MARKER,
            'x' * 262145,
        ):
            with self.subTest(output=repr(output)[:100]), self.assertRaises(observation.CandidateStorageObservationError):
                observation.parse_candidate_database_storage_observation(output, contract)
        with self.assertRaises(storage.CandidateStorageError):
            storage.parse_candidate_database_storage_attestation(exact, contract)

    def test_saved_observation_schema_identity_number_types_and_flags_are_strict(self):
        contract = fixtures.contract()
        receipt = observation.parse_candidate_database_storage_observation(marker(contract), contract)
        for key, value in (
            ('schema_version', True), ('candidate_schema', 'candidate-storage-v1'),
            ('candidate_only', 1), ('released', 0), ('runtime_qualified', True),
            ('dataset_qualified', True), ('measurement_qualified', True), ('read_only', 1),
            ('run_id', '000000000001'), ('database_names', tuple(contract.database_names)),
            ('database_count', 8.0), ('total_bytes', True), ('available_bytes', False),
            ('total_bytes', fixtures.CLAIMS - 1), ('available_bytes', -1),
            ('available_bytes', 101 * 1024**3), ('filesystem', 'ext4'),
            ('selinux_type', 'default_t'), ('unknown', True),
        ):
            changed = copy.deepcopy(receipt)
            changed[key] = value
            with self.subTest(key=key), self.assertRaises(observation.CandidateStorageObservationError):
                observation.validate_saved_candidate_database_storage_observation(changed, contract)
        for value in (None, [], {}, {key: item for key, item in receipt.items() if key != 'total_bytes'}):
            with self.assertRaises(observation.CandidateStorageObservationError):
                observation.validate_saved_candidate_database_storage_observation(value, contract)

    def test_manually_crossed_contracts_and_metadata_cannot_generate_or_validate_commands(self):
        contract = fixtures.contract()
        for changed in (
            contract.metadata(), replace(contract, namespace='foreign'),
            replace(contract, database_names=contract.database_names[:-1]),
        ):
            for operation in (
                lambda: observation.candidate_database_storage_observation_command(changed),
                lambda: observation.parse_candidate_database_storage_observation(marker(contract), changed),
                lambda: observation.validate_saved_candidate_database_storage_observation({}, changed),
            ):
                with self.assertRaises(observation.CandidateStorageObservationError):
                    operation()


@unittest.skipUnless(shutil.which('bash'), 'Bash is required for shell contract tests')
class CandidateStorageObservationGuestTests(unittest.TestCase):
    def setUp(self):
        # Reuse only setup/tool stubs, not the preparer's test class or commands.
        fixtures.CandidateStorageGuestCommandTests.setUp(self)
        self.scope = fixtures.contract()
        self.create_owned_filesystem(self.scope)

    def create_owned_filesystem(self, scope):
        root = self.mount / 'mongodb'
        root.mkdir()
        for name in scope.database_names:
            (root / name).mkdir()
        ownership = self.mount / Path(storage.OWNERSHIP_MARKER).name
        ownership.write_text(json.dumps(scope.metadata(), sort_keys=True, separators=(',', ':'), ensure_ascii=True))
        ownership.chmod(0o600)
        (root / scope.database_names[0] / 'WiredTiger').write_bytes(b'existing-mongo-data')

    def filesystem_snapshot(self):
        return {
            item.relative_to(self.mount).as_posix(): (
                item.lstat().st_mode, item.lstat().st_nlink,
                item.read_bytes() if item.is_file() and not item.is_symlink() else None,
            )
            for item in self.mount.rglob('*')
        }

    def run_guest(self, scope=None, **changes):
        command = observation.candidate_database_storage_observation_command(scope or self.scope)
        argv = shlex.split(command)
        self.assertEqual(argv[:3], ['sudo', 'bash', '-c'])
        return subprocess.run(
            [shutil.which('bash'), '-c', argv[3].replace(storage.MOUNT_POINT, str(self.mount))],
            text=True, capture_output=True, timeout=20,
            env={**os.environ, 'PATH': f'{self.tools}{os.pathsep}{os.environ["PATH"]}',
                 'GUEST_MOUNT': str(self.mount), 'GUEST_ACTIONS': str(self.actions), **changes},
        )

    def assert_rejected_without_changes(self, result, before):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertNotIn(observation.OBSERVATION_MARKER, result.stdout)
        self.assertEqual(self.filesystem_snapshot(), before)
        self.assertFalse(self.actions.exists(), 'No privileged mutation tool may run')

    def test_generated_command_is_valid_bash_and_has_no_storage_mutation_operations(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            command = observation.candidate_database_storage_observation_command(fixtures.contract(workload))
            result = subprocess.run([shutil.which('bash'), '-n', '-c', command], text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            for forbidden in ('mkfs', 'wipefs', 'mount -', 'umount', 'mkdir', 'install ', 'chmod', 'chown',
                              'restorecon', 'semanage', 'unlink', 'rm -', 'mktemp', 'truncate', 'dd if='):
                self.assertNotIn(forbidden, command)

    def test_success_with_zero_available_space_preserves_existing_data_and_marker(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            with self.subTest(workload=workload):
                if workload != self.scope.workload_id:
                    self.mount = self.root / 'media-disk'
                    self.mount.mkdir()
                    self.scope = fixtures.contract(workload)
                    self.create_owned_filesystem(self.scope)
                before = self.filesystem_snapshot()
                result = self.run_guest(GUEST_AVAILABLE='0')
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.filesystem_snapshot(), before)
                self.assertFalse(self.actions.exists())
                receipt = observation.parse_candidate_database_storage_observation(
                    result.stdout.replace(str(self.mount), storage.MOUNT_POINT), self.scope,
                )
                self.assertEqual(receipt['available_bytes'], 0)

    def test_mount_owner_filesystem_uuid_capacity_and_selinux_drift_reject_without_repair(self):
        for changes in (
            {'GUEST_OWNER': '501'}, {'GUEST_STAT_OWNER': '501:20'}, {'GUEST_FS': 'ext4'},
            {'GUEST_UUID': '12345678-1234-5678-9abc-123456789abd'}, {'GUEST_NESTED': '1'},
            {'GUEST_TOTAL': str(fixtures.CLAIMS - 1)}, {'GUEST_AVAILABLE': '-1'},
            {'GUEST_AVAILABLE': 'garbage'}, {'GUEST_AVAILABLE': '00'},
            {'GUEST_AVAILABLE': str(101 * 1024**3)}, {'GUEST_LABEL': 'default_t'},
        ):
            with self.subTest(changes=changes):
                before = self.filesystem_snapshot()
                self.assert_rejected_without_changes(self.run_guest(**changes), before)

    def test_crossed_run_workload_disk_or_marker_mode_reject_without_modification(self):
        for scope, changes in (
            (fixtures.contract(run='000000000001'), {}),
            (fixtures.contract('media_microservices'), {}),
            (fixtures.contract(uuid='12345678-1234-5678-9abc-123456789abd'),
             {'GUEST_UUID': '12345678-1234-5678-9abc-123456789abd'}),
        ):
            before = self.filesystem_snapshot()
            self.assert_rejected_without_changes(self.run_guest(scope, **changes), before)
        (self.mount / Path(storage.OWNERSHIP_MARKER).name).chmod(0o644)
        before = self.filesystem_snapshot()
        self.assert_rejected_without_changes(self.run_guest(), before)

    def test_writable_parent_directory_modes_are_rejected_without_repair(self):
        for directory in (self.mount, self.mount / 'mongodb'):
            with self.subTest(directory=directory.name):
                original = directory.stat().st_mode & 0o777
                directory.chmod(0o777)
                before = self.filesystem_snapshot()
                self.assert_rejected_without_changes(self.run_guest(), before)
                self.assertEqual(directory.stat().st_mode & 0o777, 0o777)
                directory.chmod(original)

    def test_missing_marker_inventory_or_legacy_social_marker_reject_without_creation(self):
        ownership = self.mount / Path(storage.OWNERSHIP_MARKER).name
        retained = ownership.read_bytes()
        ownership.unlink()
        before = self.filesystem_snapshot()
        self.assert_rejected_without_changes(self.run_guest(), before)
        ownership.write_bytes(retained)
        ownership.chmod(0o600)
        (self.mount / Path(storage.LEGACY_SOCIAL_MARKER).name).write_bytes(b'owned-social-storage')
        before = self.filesystem_snapshot()
        self.assert_rejected_without_changes(self.run_guest(), before)

    def test_missing_database_directory_and_dangling_marker_link_are_not_repaired(self):
        missing = self.mount / 'mongodb' / self.scope.database_names[-1]
        missing.rmdir()
        before = self.filesystem_snapshot()
        self.assert_rejected_without_changes(self.run_guest(), before)
        self.assertFalse(missing.exists())
        missing.mkdir()
        ownership = self.mount / Path(storage.OWNERSHIP_MARKER).name
        ownership.unlink()
        ownership.symlink_to(self.root / 'absent-marker')
        before = self.filesystem_snapshot()
        self.assert_rejected_without_changes(self.run_guest(), before)
        self.assertTrue(ownership.is_symlink())

    def test_marker_hardlink_is_rejected_without_unlinking_either_name(self):
        ownership = self.mount / Path(storage.OWNERSHIP_MARKER).name
        alias = self.mount / 'second-marker-name'
        os.link(ownership, alias)
        before = self.filesystem_snapshot()
        self.assert_rejected_without_changes(self.run_guest(), before)
        self.assertEqual(ownership.stat().st_nlink, 2)
        self.assertEqual(alias.read_bytes(), ownership.read_bytes())

    def test_unknown_entries_links_and_special_files_reject_without_touching_owned_data(self):
        root = self.mount / 'mongodb'
        for item in (self.mount / 'foreign', root / 'foreign'):
            item.write_bytes(b'keep')
            before = self.filesystem_snapshot()
            self.assert_rejected_without_changes(self.run_guest(), before)
            item.unlink()
        target = root / self.scope.database_names[0] / 'WiredTiger'
        unsafe = target.parent / 'unsafe'
        unsafe.symlink_to(self.tools, target_is_directory=True)
        before = self.filesystem_snapshot()
        self.assert_rejected_without_changes(self.run_guest(), before)
        unsafe.unlink()
        os.link(target, unsafe)
        before = self.filesystem_snapshot()
        self.assert_rejected_without_changes(self.run_guest(), before)
        unsafe.unlink()
        os.mkfifo(unsafe)
        before = self.filesystem_snapshot()
        self.assert_rejected_without_changes(self.run_guest(), before)


if __name__ == '__main__':
    unittest.main()
