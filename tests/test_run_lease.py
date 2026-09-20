import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from app import run_lease
from app.run_lease import (
    LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME,
    MAX_RUN_LEASE_METADATA_BYTES,
    RUN_LEASE_FILENAME,
    RUN_LEASE_METADATA_FILENAME,
    RunLeaseError,
    RunLeaseHeldError,
    acquire_run_lease,
    inspect_run_lease,
)


RUN_ID = 'abc123def456'


class RunLeaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.runs_root = Path(self.temporary.name)
        self.run_directory = self.runs_root / RUN_ID
        self.run_directory.mkdir()

    def tearDown(self):
        self.temporary.cleanup()

    @property
    def lock_path(self):
        return self.run_directory / RUN_LEASE_FILENAME

    @property
    def metadata_path(self):
        return self.run_directory / RUN_LEASE_METADATA_FILENAME

    @property
    def legacy_lock_path(self):
        return (
            self.run_directory
            / LEGACY_AZURE_QUALIFICATION_LOCK_FILENAME
        )

    def _start_legacy_qualifier_holder(self):
        program = (
            'import fcntl, pathlib, sys; '
            'lock = pathlib.Path(sys.argv[1]).open("a+"); '
            'fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB); '
            'print("ready", flush=True); '
            'sys.stdin.readline()'
        )
        process = subprocess.Popen(
            [sys.executable, '-c', program, str(self.legacy_lock_path)],
            cwd=Path(__file__).resolve().parents[1],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(process.stdout.readline().strip(), 'ready')
        return process

    def test_acquire_uses_stable_empty_lock_and_atomic_owner_sidecar(self):
        initial = inspect_run_lease(self.runs_root, RUN_ID)
        self.assertFalse(initial.held)
        self.assertIsNone(initial.owner)

        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'qualification-cli',
        ) as lease:
            lock_identity = self.lock_path.stat().st_ino
            self.assertEqual(self.lock_path.read_bytes(), b'')
            self.assertEqual(lease.path, self.lock_path)
            self.assertEqual(self.lock_path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(self.metadata_path.stat().st_mode & 0o777, 0o600)
            self.assertLessEqual(
                len(self.metadata_path.read_bytes()),
                MAX_RUN_LEASE_METADATA_BYTES,
            )
            self.assertEqual(
                json.loads(self.metadata_path.read_bytes()),
                lease.owner.as_dict(),
            )
            status = inspect_run_lease(self.runs_root, RUN_ID)
            self.assertTrue(status.held)
            self.assertEqual(status.owner, lease.owner)

        released = inspect_run_lease(self.runs_root, RUN_ID)
        self.assertFalse(released.held)
        self.assertIsNone(released.owner)

        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'api-destroy',
        ) as replacement:
            self.assertEqual(self.lock_path.stat().st_ino, lock_identity)
            self.assertEqual(replacement.owner.owner_label, 'api-destroy')
            self.assertNotIn('key', replacement.owner.as_dict())
            self.assertNotIn('token', replacement.owner.as_dict())

    def test_second_nonblocking_acquire_reports_validated_owner(self):
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'qualification-cli',
        ) as first:
            with self.assertRaises(RunLeaseHeldError) as raised:
                acquire_run_lease(
                    self.runs_root,
                    RUN_ID,
                    'api-destroy',
                )
            still_held = inspect_run_lease(self.runs_root, RUN_ID)
            self.assertTrue(still_held.held)
            self.assertEqual(still_held.owner, first.owner)

        self.assertEqual(raised.exception.owner, first.owner)

    def test_real_second_process_observes_the_held_lease(self):
        program = (
            'import json, sys; '
            'from app.run_lease import inspect_run_lease; '
            'status = inspect_run_lease(sys.argv[1], sys.argv[2]); '
            'print(json.dumps({'
            '"held": status.held, '
            '"owner": status.owner.owner_label if status.owner else None'
            '}))'
        )
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'qualification-cli',
        ):
            completed = subprocess.run(
                [
                    sys.executable,
                    '-c',
                    program,
                    str(self.runs_root),
                    RUN_ID,
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )

        self.assertEqual(
            json.loads(completed.stdout),
            {'held': True, 'owner': 'qualification-cli'},
        )

    def test_legacy_qualifier_process_blocks_new_shared_lease(self):
        process = self._start_legacy_qualifier_holder()
        try:
            status = inspect_run_lease(self.runs_root, RUN_ID)
            self.assertTrue(status.held)
            self.assertIsNone(status.owner)
            with self.assertRaisesRegex(
                RunLeaseError,
                'legacy Azure qualification lock',
            ):
                acquire_run_lease(
                    self.runs_root,
                    RUN_ID,
                    'api-destroy',
                )
            self.assertFalse(self.lock_path.exists())
        finally:
            _stdout, stderr = process.communicate(input='release\n', timeout=10)

        self.assertEqual(process.returncode, 0, stderr)
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'api-destroy',
        ):
            self.assertEqual(
                self.legacy_lock_path.stat().st_mode & 0o777,
                0o600,
            )

    def test_new_shared_lease_blocks_legacy_qualifier_process(self):
        program = (
            'import fcntl, json, pathlib, sys; '
            'lock = pathlib.Path(sys.argv[1]).open("a+"); '
            'acquired = True; '
            '\ntry:\n '
            ' fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)'
            '\nexcept BlockingIOError:\n '
            ' acquired = False'
            '\nprint(json.dumps({"acquired": acquired}))'
        )
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'qualification-cli',
        ):
            completed = subprocess.run(
                [
                    sys.executable,
                    '-c',
                    program,
                    str(self.legacy_lock_path),
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )

        self.assertEqual(
            json.loads(completed.stdout),
            {'acquired': False},
        )

    def test_concurrent_release_is_idempotent_and_never_double_closes(self):
        lease = acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'qualification-cli',
        )
        start = threading.Barrier(3)
        failures = []

        def release():
            start.wait()
            try:
                lease.release()
            except BaseException as exc:  # pragma: no cover - assertion path
                failures.append(exc)

        workers = [threading.Thread(target=release) for _ in range(2)]
        for worker in workers:
            worker.start()
        start.wait()
        for worker in workers:
            worker.join(timeout=3)

        self.assertFalse(any(worker.is_alive() for worker in workers))
        self.assertEqual(failures, [])
        self.assertTrue(lease.released)
        self.assertFalse(inspect_run_lease(self.runs_root, RUN_ID).held)

    def test_missing_metadata_on_unlocked_lock_is_safely_claimed(self):
        self.lock_path.touch(mode=0o600)
        self.lock_path.chmod(0o600)

        status = inspect_run_lease(self.runs_root, RUN_ID)
        self.assertFalse(status.held)
        self.assertIsNone(status.owner)
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'api-destroy',
        ) as lease:
            self.assertEqual(lease.owner.owner_label, 'api-destroy')
            self.assertTrue(self.metadata_path.is_file())

    def test_held_lock_with_missing_metadata_fails_closed(self):
        self.lock_path.touch(mode=0o600)
        self.lock_path.chmod(0o600)
        descriptor = os.open(self.lock_path, os.O_RDWR)
        try:
            run_lease.fcntl.flock(
                descriptor,
                run_lease.fcntl.LOCK_EX | run_lease.fcntl.LOCK_NB,
            )
            with self.assertRaisesRegex(RunLeaseError, 'no owner metadata'):
                inspect_run_lease(self.runs_root, RUN_ID)
            with self.assertRaisesRegex(RunLeaseError, 'no owner metadata'):
                acquire_run_lease(
                    self.runs_root,
                    RUN_ID,
                    'api-destroy',
                )
        finally:
            run_lease.fcntl.flock(descriptor, run_lease.fcntl.LOCK_UN)
            os.close(descriptor)

    def test_lock_missing_with_owner_metadata_fails_closed(self):
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'qualification-cli',
        ):
            pass
        self.lock_path.unlink()  # Test-only corruption of the invariant.

        with self.assertRaisesRegex(RunLeaseError, 'without its lock'):
            inspect_run_lease(self.runs_root, RUN_ID)
        with self.assertRaisesRegex(RunLeaseError, 'without its lock'):
            acquire_run_lease(
                self.runs_root,
                RUN_ID,
                'api-destroy',
            )
        self.assertFalse(self.lock_path.exists())

    def test_replace_failure_retains_prior_canonical_metadata(self):
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'first-owner',
        ):
            pass
        prior = self.metadata_path.read_bytes()
        lock_identity = self.lock_path.stat().st_ino

        with (
            mock.patch.object(
                run_lease.os,
                'replace',
                side_effect=OSError('injected replace failure'),
            ),
            self.assertRaisesRegex(RunLeaseError, 'publish'),
        ):
            acquire_run_lease(
                self.runs_root,
                RUN_ID,
                'second-owner',
            )

        self.assertEqual(self.metadata_path.read_bytes(), prior)
        self.assertEqual(self.lock_path.stat().st_ino, lock_identity)
        self.assertFalse(inspect_run_lease(self.runs_root, RUN_ID).held)
        self.assertEqual(list(self.run_directory.glob('.*.tmp')), [])

    def test_write_failure_retains_prior_canonical_metadata(self):
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'first-owner',
        ):
            pass
        prior = self.metadata_path.read_bytes()

        with (
            mock.patch.object(
                run_lease.os,
                'pwrite',
                side_effect=OSError('injected write failure'),
            ),
            self.assertRaisesRegex(RunLeaseError, 'prepare'),
        ):
            acquire_run_lease(
                self.runs_root,
                RUN_ID,
                'second-owner',
            )

        self.assertEqual(self.metadata_path.read_bytes(), prior)
        self.assertFalse(inspect_run_lease(self.runs_root, RUN_ID).held)

    def test_orphaned_torn_temporary_write_cannot_poison_owner_metadata(self):
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'first-owner',
        ) as lease:
            expected_owner = lease.owner
        orphan = self.run_directory / (
            f'.{RUN_LEASE_METADATA_FILENAME}.orphan.tmp'
        )
        orphan.write_bytes(b'{"torn":')
        orphan.chmod(0o600)

        self.assertFalse(inspect_run_lease(self.runs_root, RUN_ID).held)
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'second-owner',
        ) as second:
            self.assertNotEqual(second.owner, expected_owner)

    def test_release_context_and_validation_contracts(self):
        lease = acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'qualification-cli',
        )
        lease.release()
        lease.release()
        with self.assertRaisesRegex(RunLeaseError, 'cannot be re-entered'):
            lease.__enter__()

        with self.assertRaisesRegex(RuntimeError, 'injected'):
            with acquire_run_lease(
                self.runs_root,
                RUN_ID,
                'qualification-cli',
            ):
                raise RuntimeError('injected')
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'api-destroy',
        ):
            pass

        for invalid_id in (
            '', 'abc123def45', 'abc123def4567', 'ABC123DEF456',
            '../123456789', 'gggggggggggg', None,
        ):
            with self.subTest(invalid_id=invalid_id):
                with self.assertRaises(ValueError):
                    acquire_run_lease(
                        self.runs_root / 'missing',
                        invalid_id,
                        'qualification-cli',
                    )
        for invalid_owner in (
            '', 'Qualification', '9-owner', 'owner label', 'owner/label',
            'owner\nlabel', 'a' * 65, None,
        ):
            with self.subTest(invalid_owner=invalid_owner):
                with self.assertRaises(ValueError):
                    acquire_run_lease(
                        self.runs_root / 'missing',
                        RUN_ID,
                        invalid_owner,
                    )

    def test_filesystem_and_metadata_ambiguity_fail_closed(self):
        target = self.runs_root / 'target'
        target.mkdir()
        symlink_id = '111111111111'
        (self.runs_root / symlink_id).symlink_to(target, target_is_directory=True)
        with self.assertRaises(RunLeaseError):
            inspect_run_lease(self.runs_root, symlink_id)

        outside = self.runs_root / 'outside'
        outside.write_bytes(b'')
        self.lock_path.symlink_to(outside)
        with self.assertRaises(RunLeaseError):
            acquire_run_lease(
                self.runs_root,
                RUN_ID,
                'qualification-cli',
            )
        self.lock_path.unlink()

        self.lock_path.touch(mode=0o600)
        self.lock_path.chmod(0o600)
        self.metadata_path.write_bytes(b'not-json\n')
        self.metadata_path.chmod(0o600)
        with self.assertRaises(RunLeaseError):
            inspect_run_lease(self.runs_root, RUN_ID)
        with self.assertRaises(RunLeaseError):
            acquire_run_lease(
                self.runs_root,
                RUN_ID,
                'qualification-cli',
            )

        self.metadata_path.write_bytes(
            b'x' * (MAX_RUN_LEASE_METADATA_BYTES + 1)
        )
        with self.assertRaises(RunLeaseError):
            inspect_run_lease(self.runs_root, RUN_ID)

    def test_sidecar_symlink_permissions_and_hardlinks_fail_closed(self):
        self.lock_path.touch(mode=0o600)
        self.lock_path.chmod(0o600)
        outside = self.runs_root / 'outside-owner'
        outside.write_bytes(b'{}\n')
        self.metadata_path.symlink_to(outside)
        with self.assertRaises(RunLeaseError):
            inspect_run_lease(self.runs_root, RUN_ID)
        self.metadata_path.unlink()

        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'qualification-cli',
        ):
            pass
        self.metadata_path.chmod(0o644)
        with self.assertRaisesRegex(RunLeaseError, 'mode 0600'):
            inspect_run_lease(self.runs_root, RUN_ID)
        self.metadata_path.chmod(0o600)

        metadata_link = self.run_directory / 'owner-hardlink'
        os.link(self.metadata_path, metadata_link)
        with self.assertRaisesRegex(RunLeaseError, 'unsafe link count'):
            inspect_run_lease(self.runs_root, RUN_ID)
        metadata_link.unlink()

        lock_link = self.run_directory / 'lock-hardlink'
        os.link(self.lock_path, lock_link)
        with self.assertRaisesRegex(RunLeaseError, 'unsafe link count'):
            acquire_run_lease(
                self.runs_root,
                RUN_ID,
                'api-destroy',
            )

    def test_noncanonical_cross_run_and_duplicate_metadata_fail_closed(self):
        with acquire_run_lease(
            self.runs_root,
            RUN_ID,
            'qualification-cli',
        ) as lease:
            metadata = lease.owner.as_dict()

        values = [
            (json.dumps(metadata, indent=2) + '\n').encode(),
            (
                json.dumps(
                    dict(metadata, run_id='000000000000'),
                    sort_keys=True,
                    separators=(',', ':'),
                )
                + '\n'
            ).encode(),
            (
                '{"acquired_at":' + json.dumps(metadata['acquired_at']) + ','
                '"owner_label":"qualification-cli",'
                '"owner_label":"api-destroy",'
                f'"pid":{metadata["pid"]},"run_id":"{RUN_ID}",'
                '"schema_version":1}\n'
            ).encode(),
            (
                json.dumps(
                    dict(metadata, schema_version=1.0),
                    sort_keys=True,
                    separators=(',', ':'),
                )
                + '\n'
            ).encode(),
        ]
        for value in values:
            with self.subTest(value=value):
                self.metadata_path.write_bytes(value)
                self.metadata_path.chmod(0o600)
                with self.assertRaises(RunLeaseError):
                    inspect_run_lease(self.runs_root, RUN_ID)
                with self.assertRaises(RunLeaseError):
                    acquire_run_lease(
                        self.runs_root,
                        RUN_ID,
                        'qualification-cli',
                    )


if __name__ == '__main__':
    unittest.main()
