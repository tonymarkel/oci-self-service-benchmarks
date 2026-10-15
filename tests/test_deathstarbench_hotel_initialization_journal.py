"""Real leased private files, synthetic offline identities; no live execution."""

import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from app import deathstarbench_hotel_initialization_journal as journal
from app.deathstarbench_hotel_initialization_stage import build_guarded_hotel_initialization_stage
from app.run_lease import acquire_run_lease, RunLeaseError
from tests import test_deathstarbench_hotel_runtime as fixtures


class GuardedHotelJournalTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.GuardedHotelRuntimeTests()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.plan = self.fixture.plan()
        self.stage = build_guarded_hotel_initialization_stage(self.plan)
        self.observation = self.fixture.observation(self.plan)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.directory = self.root / self.plan.run_id
        self.directory.mkdir(mode=0o700)
        self.lease = acquire_run_lease(self.root, self.plan.run_id, 'hotel-journal-test')
        self.addCleanup(self.lease.release)
        # Full anonymous byte qualification has independent hostile transport
        # tests. Only this boundary is stubbed; real plan/proof/stage and lease
        # validation plus all no-follow/exclusive/fsync file logic execute.
        self.access = mock.Mock(fingerprint='sha256:' + 'e' * 64)
        self.require_access = mock.patch.object(journal, 'require_guarded_hotel_image_access', return_value=self.access)
        self.require_access.start()
        self.addCleanup(self.require_access.stop)

    def open(self, **overrides):
        options = dict(plan=self.plan, stage=self.stage, lease=self.lease)
        options.update(overrides)
        return journal.GuardedHotelInitializationJournal(**options)

    def pin(self, store):
        return store.record_blocked_pods(self.observation, verifier_pod_uid=fixtures.uid(99))

    def intent(self, store, mode='initialize-empty'):
        return store.record_dispatch_intent(mode, image_access=self.access)

    def output(self, mode='initialize-empty', **overrides):
        value = dict(candidate_only=True, released=False, application_start_allowed=False,
                     runtime_qualified=False, measurement_qualified=False,
                     run_id=self.plan.run_id, contract_sha256=self.plan.snapshot()['dataset_fingerprint'],
                     mode=mode, database_verification_completed=True)
        value.update(overrides)
        return json.dumps(value)

    def exit(self, store, mode='initialize-empty', **overrides):
        options = dict(pod_uid=self.observation.initializer_pod_uid if mode == 'initialize-empty' else fixtures.uid(99),
                       exit_code=0, output=self.output(mode))
        options.update(overrides)
        return store.record_exit_observation(mode, **options)

    def test_forward_only_history_never_grants_permission_even_after_both_local_successes(self):
        store = self.open()
        self.assertEqual(store.metadata()['state'], 'created')
        self.pin(store)
        self.intent(store)
        self.exit(store)
        self.intent(store, 'verify')
        final = self.exit(store, 'verify')
        self.assertEqual(final['event_count'], 6)
        self.assertEqual(final['state'], 'verify-exit-recorded')
        self.assertEqual(final['dispatch_latches'], ['initialize-empty', 'verify'])
        self.assertEqual(final['resume_policy'], 'cleanup-only-no-replay')
        self.assertFalse(final['journal_is_execution_permission'])
        self.assertTrue(all(final[key] is False for key in journal.CLOSED))
        self.assertFalse(hasattr(store, 'execute'))
        self.assertFalse(hasattr(store, 'release_gate'))
        self.assertEqual(self.open().metadata(), final)
        for path in self.directory.glob('.hotel-initialization-*'):
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        for mode in journal.LATCHES:
            with self.assertRaises(journal.GuardedHotelJournalError):
                self.intent(store, mode)

    def test_reopen_after_each_latched_state_is_cleanup_only_including_completed_initializer(self):
        store = self.open()
        self.pin(store)
        self.intent(store)
        reopened = self.open()
        self.assertEqual(reopened.metadata()['resume_policy'], 'cleanup-only-no-replay')
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.exit(reopened)
        self.exit(store)
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.intent(self.open(), 'verify')
        self.assertEqual(len(store.metadata()['events']), 4)

    def test_reopen_before_dispatch_is_only_blocked_preflight_not_live_qualification(self):
        store = self.open()
        self.pin(store)
        reopened = self.open()
        self.assertEqual(reopened.metadata()['resume_policy'], 'blocked-preflight-only')
        self.assertFalse(reopened.metadata()['live_identity_verified'])
        self.intent(reopened)
        journal.require_guarded_hotel_image_access.assert_called_once_with(self.access, self.plan.artifacts)

    def test_latch_survives_response_loss_between_latch_and_event_and_cannot_be_replayed(self):
        store = self.open()
        self.pin(store)
        real = store._exclusive

        def lose_event(directory, name, value):
            if name == store._event_name(2):
                raise OSError('simulated write response loss')
            return real(directory, name, value)

        with mock.patch.object(store, '_exclusive', side_effect=lose_event), self.assertRaises(OSError):
            self.intent(store)
        latch = self.directory / journal.LATCHES['initialize-empty']
        original = latch.read_bytes()
        self.assertTrue(original)
        reopened = self.open()
        self.assertEqual(reopened.metadata()['event_count'], 2)
        self.assertEqual(reopened.metadata()['resume_policy'], 'cleanup-only-no-replay')
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.intent(reopened)
        self.assertEqual(latch.read_bytes(), original)
        with self.assertRaises(journal.GuardedHotelJournalError):
            store.metadata()

    def test_fsync_failure_preserves_latch_and_stops_original_process(self):
        store = self.open()
        self.pin(store)
        with mock.patch.object(journal.os, 'fsync', side_effect=OSError('simulated fsync failure')):
            with self.assertRaises(OSError):
                self.intent(store)
        self.assertTrue((self.directory / journal.LATCHES['initialize-empty']).exists())
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.intent(store)
        self.assertEqual(self.open().metadata()['resume_policy'], 'cleanup-only-no-replay')

    def test_input_failures_leave_history_unchanged_and_never_consume_latch(self):
        store = self.open()
        before = store.metadata()
        for uid in (self.observation.initializer_pod_uid, self.observation.namespace_uid,
                    self.observation.database_node_uid, self.observation.kube_system_namespace_uid,
                    self.observation.databases[0].pod_uid, self.observation.databases[0].pvc_uid,
                    self.observation.databases[0].pv_uid,
                    'not-a-uid', '00000000-0000-0000-0000-000000000000'):
            with self.subTest(uid=uid), self.assertRaises(ValueError):
                store.record_blocked_pods(self.observation, verifier_pod_uid=uid)
            self.assertEqual(store.metadata(), before)
        self.pin(store)
        for mode in ('verify', '', 'initialize-empty ', True):
            with self.subTest(mode=mode), self.assertRaises(journal.GuardedHotelJournalError):
                self.intent(store, mode)
        self.assertFalse((self.directory / journal.LATCHES['initialize-empty']).exists())

    def test_command_output_binds_exact_mode_run_dataset_uid_and_false_gates(self):
        store = self.open()
        self.pin(store)
        self.intent(store)
        cases = [dict(exit_code=True), dict(exit_code=132), dict(pod_uid=fixtures.uid(98)),
                 dict(output=self.output('verify')), dict(output=self.output(run_id='abcdef123456')),
                 dict(output=self.output(contract_sha256='sha256:' + 'a' * 64)),
                 dict(output=self.output(application_start_allowed=True)),
                 dict(output=self.output(database_verification_completed=1)),
                 dict(output=self.output(extra=True)), dict(output='x' * 4097),
                 dict(output=self.output()[:-1] + ',"mode":"initialize-empty"}'),
                 dict(output=b'\xef\xbb\xbf' + self.output().encode())]
        for changes in cases:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.exit(store, **changes)
            self.assertEqual(store.metadata()['event_count'], 3)
        self.exit(store)

    def test_new_attempt_cannot_reassign_existing_run_journal_or_latch(self):
        store = self.open()
        self.pin(store)
        self.intent(store)
        changed = self.fixture.plan(initialization_attempt_id='ffffffff-ffff-ffff-ffff-ffffffffffff')
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.open(plan=changed, stage=build_guarded_hotel_initialization_stage(changed))
        self.assertEqual(store.metadata()['event_count'], 3)

    def test_released_copied_and_foreign_run_leases_are_rejected(self):
        with self.assertRaises(RunLeaseError):
            self.open(lease=copy.copy(self.lease))
        self.lease.release()
        with self.assertRaises(RunLeaseError):
            self.open()
        other_id = 'abcdef123456'
        (self.root / other_id).mkdir(mode=0o700)
        with acquire_run_lease(self.root, other_id, 'hotel-other') as other:
            with self.assertRaises(RunLeaseError):
                self.open(lease=other)

    def test_corrupt_or_partial_records_are_never_repaired(self):
        self.open()
        path = self.directory / (journal.PREFIX + '00.json')
        path.write_bytes(b'{')
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.open()
        self.assertEqual(path.read_bytes(), b'{')

    def test_link_and_mode_attacks_rejected_without_touching_targets(self):
        store = self.open()
        path = self.directory / store._event_name(0)
        original = path.read_bytes()
        foreign = self.root / 'foreign.json'
        foreign.write_bytes(original)
        foreign.chmod(0o600)
        path.unlink()
        path.symlink_to(foreign)
        with self.assertRaises(OSError):
            self.open()
        self.assertEqual(foreign.read_bytes(), original)
        path.unlink()
        os.link(foreign, path)
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.open()
        self.assertEqual(foreign.read_bytes(), original)
        path.unlink()
        path.write_bytes(original)
        path.chmod(0o644)
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.open()

    def test_history_drift_gaps_unknown_names_and_missing_latch_fail_closed(self):
        store = self.open()
        self.pin(store)
        self.intent(store)
        latch = self.directory / journal.LATCHES['initialize-empty']
        latch.unlink()
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.open()
        with self.assertRaises(journal.GuardedHotelJournalError):
            store.metadata()

    def test_oversized_fifo_and_unknown_event_files_are_not_followed_or_blocked(self):
        self.open()
        path = self.directory / (journal.PREFIX + '01.json')
        os.mkfifo(path, 0o600)
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.open()
        path.unlink()
        path.write_bytes(b' ' * (journal.MAX_BYTES + 1))
        path.chmod(0o600)
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.open()
        path.unlink()
        (self.directory / (journal.PREFIX + '99.json')).write_bytes(b'{}')
        with self.assertRaises(journal.GuardedHotelJournalError):
            self.open()


if __name__ == '__main__':
    unittest.main()
