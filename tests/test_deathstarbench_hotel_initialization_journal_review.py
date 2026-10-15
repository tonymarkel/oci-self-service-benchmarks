"""Independent crossed-identity and unexpected-latch journal regressions."""

import unittest
from unittest import mock

from app import deathstarbench_hotel_initialization_journal as journal
from tests import test_deathstarbench_hotel_initialization_journal as fixtures
from tests import test_deathstarbench_hotel_runtime as runtime_fixtures


class GuardedHotelJournalSafetyReviewTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.GuardedHotelJournalTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_verifier_uid_cannot_alias_any_pinned_kubernetes_object(self):
        fixture = self.fixture
        store = fixture.open()
        before = store.metadata()
        observation = fixture.observation
        crossed = [observation.kube_system_namespace_uid, observation.namespace_uid,
                   observation.database_node_uid, observation.initializer_pod_uid]
        crossed.extend(value for row in observation.databases
                       for value in (row.pvc_uid, row.pv_uid, row.pod_uid))
        for value in crossed:
            with self.subTest(value=value), self.assertRaises(journal.GuardedHotelJournalError):
                store.record_blocked_pods(observation, verifier_pod_uid=value)
            self.assertEqual(store.metadata(), before)
            self.assertFalse(any((fixture.directory / name).exists() for name in journal.LATCHES.values()))

    def test_saved_rehashed_verifier_uid_alias_cannot_restore_a_pinned_state(self):
        fixture = self.fixture
        store = fixture.open()
        fixture.pin(store)
        event = store.metadata()['events'][1]
        event['payload']['verifier_pod_uid'] = fixture.observation.databases[0].pvc_uid
        path = fixture.directory / store._event_name(1)
        path.write_bytes(journal._canonical(event))
        with self.assertRaises(journal.GuardedHotelJournalError):
            fixture.open()
        self.assertFalse(any((fixture.directory / name).exists() for name in journal.LATCHES.values()))

    def test_unexpected_valid_future_latch_stops_current_unlatched_session(self):
        fixture = self.fixture
        store = fixture.open()
        fixture.pin(store)
        pinned = store.metadata()
        # Simulate another writer's durable dispatch evidence while this
        # session retains the earlier, unlatched history. The event write was
        # lost, but the one-use latch exists and must not be ignored.
        directory = store._directory()
        try:
            store._exclusive(directory, journal.LATCHES['verify'], {
                'identity': store._identity(),
                'intent': {'mode': 'verify', 'pod_uid': runtime_fixtures.uid(99),
                           'image': fixture.plan.application_image,
                           'image_access_fingerprint': fixture.access.fingerprint},
            })
        finally:
            journal.os.close(directory)
        self.assertEqual(store.metadata()['resume_policy'], 'cleanup-only-no-replay')
        with self.assertRaises(journal.GuardedHotelJournalError):
            fixture.intent(store)
        self.assertFalse((fixture.directory / journal.LATCHES['initialize-empty']).exists())
        reopened = fixture.open().metadata()
        self.assertEqual(reopened['event_count'], pinned['event_count'])
        self.assertEqual(reopened['resume_policy'], 'cleanup-only-no-replay')

    def test_competing_latch_during_intent_persistence_is_detected_before_receipt(self):
        fixture = self.fixture
        store = fixture.open()
        fixture.pin(store)
        original = store._exclusive

        def competing_latch(directory, name, value):
            result = original(directory, name, value)
            if name == journal.LATCHES['initialize-empty']:
                original(directory, journal.LATCHES['verify'], {
                    'identity': store._identity(),
                    'intent': {'mode': 'verify', 'pod_uid': runtime_fixtures.uid(99),
                               'image': fixture.plan.application_image,
                               'image_access_fingerprint': fixture.access.fingerprint},
                })
            return result

        with mock.patch.object(store, '_exclusive', side_effect=competing_latch), \
                self.assertRaises(journal.GuardedHotelJournalError):
            fixture.intent(store)
        self.assertTrue(all((fixture.directory / name).exists() for name in journal.LATCHES.values()))
        reopened = fixture.open()
        self.assertEqual(reopened.metadata()['resume_policy'], 'cleanup-only-no-replay')
        with self.assertRaises(journal.GuardedHotelJournalError):
            fixture.intent(reopened)


if __name__ == '__main__':
    unittest.main()
