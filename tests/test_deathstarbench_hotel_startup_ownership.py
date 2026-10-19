"""Pure ownership/integrity tests, not cloud authorization or qualification."""
import copy
import json
import unittest
from unittest import mock

from app import deathstarbench_candidate_operator as support
from app import deathstarbench_hotel_initialization_operator as initialization
from app import deathstarbench_hotel_startup_ownership as ownership
from app import deathstarbench_hotel_startup_operator as operator
from tests import test_deathstarbench_hotel_initialization_artifacts as guarded_fixture
from tests import test_deathstarbench_hotel_startup_artifacts as startup_fixture

RUN = '123456abcdef'
INITIALIZATION = '87654321-4321-4321-8321-cba987654321'
STARTUP = '87654321-4321-4321-8321-cba987654322'


class OriginalArtifactsMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Capture the original baseline BEFORE the startup fixture changes the
        # publication. Both actual adoption factories bind the same capability.
        cls.fixture = startup_fixture.StartupHotelArtifactTests()
        cls.addClassCleanup(cls.fixture.doCleanups)
        original = guarded_fixture.GuardedHotelArtifactsTests.fixture
        captured = {}
        def capture(helper, **kwargs):
            data = original(helper, **kwargs)
            captured.update({**data, 'publication': copy.deepcopy(data['publication']),
                             'prepared': copy.deepcopy(data['prepared'])})
            return data
        with mock.patch.object(guarded_fixture.GuardedHotelArtifactsTests, 'fixture', capture):
            cls.data = cls.fixture.fixture()
        cls.startup_artifacts = cls.fixture.adopt(cls.data)
        helper = guarded_fixture.GuardedHotelArtifactsTests()
        cls.addClassCleanup(helper.doCleanups)
        cls.guarded_artifacts = helper.adopt(captured)
        cls.baseline = cls.data['baseline']
        cls.plan = {'provider': 'azure', 'benchmarks': ['deathstarbench'],
                    'deathstarbench': {'workload': 'social_network'}}
        pin = {'schema_version': 1, 'candidate_schema': support.PIN_SCHEMA,
               'candidate_only': True, 'released': False, 'run_id': RUN,
               'workload_id': 'hotel_reservation', 'artifact_fingerprint': cls.baseline.fingerprint,
               'artifacts': cls.baseline.metadata(), 'infrastructure_plan': cls.plan,
               'provider_pin': {'provider': 'azure', 'account': 'offline-selector'},
               'ssh_public_key_sha256': '1' * 64}
        cls.pin = {**pin, 'fingerprint': support._fingerprint(pin)}
        cls.initialization_pin = initialization.make_hotel_initialization_pin(
            RUN, cls.pin, cls.guarded_artifacts, attempt_id=INITIALIZATION)
        cls.startup_pin = operator.make_hotel_startup_pin(
            RUN, cls.pin, cls.initialization_pin, cls.startup_artifacts, startup_attempt_id=STARTUP)

    def new_job(self):
        return {'id': RUN, 'plan': copy.deepcopy(self.plan), 'resources': {},
                '_persist_state': False, 'results': [], support.PIN_KEY: copy.deepcopy(self.pin),
                initialization.PIN_KEY: copy.deepcopy(self.initialization_pin),
                operator.PIN_KEY: copy.deepcopy(self.startup_pin)}

    def provider_projection(self):
        # A normalized provider-validated boundary value; its hashes do not
        # establish that any cloud resource really exists or is ours.
        target = {'candidate_schema': 'hotel-startup-owned-target-v1', 'kind': 'resource-group',
                  'id': '/subscriptions/offline/groups/original', 'name': 'offline-original-group',
                  'scope': {'subscription_id': 'offline-subscription'}, 'parent_id': None,
                  'self_link': None, 'incarnation_id': None}
        return {'provider_scope': {'subscription_id': 'offline-subscription'},
                'create_contract': {'group': 'offline-original-group', 'run_id': RUN},
                'resources': {'azure_resource_group_id': '/subscriptions/offline/groups/original'},
                'known_identities': {'group': copy.deepcopy(target)},
                'absence_targets': {'group': copy.deepcopy(target)},
                'create_attempts': {}, 'unresolved_creates': [], 'audits': []}

    def owned_job(self):
        job = self.new_job()
        job[ownership.CHECKPOINT_KEY] = ownership.make_hotel_startup_ownership_checkpoint(
            job, support_pin=self.pin, initialization_pin=self.initialization_pin,
            startup_pin=self.startup_pin)
        job[ownership.CHECKPOINT_KEY] = ownership.update_hotel_startup_ownership_checkpoint(
            job[ownership.CHECKPOINT_KEY], self.provider_projection())
        return job


class StartupOwnershipTests(OriginalArtifactsMixin, unittest.TestCase):
    def test_unallocated_checkpoint_cannot_become_empty_cleanup_authority(self):
        # Catches sealing intent before any independent create contract exists.
        job = self.new_job()
        job[ownership.CHECKPOINT_KEY] = ownership.make_hotel_startup_ownership_checkpoint(
            job, support_pin=self.pin, initialization_pin=self.initialization_pin,
            startup_pin=self.startup_pin)
        self.assertIs(job[ownership.CHECKPOINT_KEY]['contract_recorded'], False)
        with self.assertRaises(ownership.HotelStartupOwnershipError):
            ownership.make_hotel_startup_cleanup_ownership(job, support_pin=self.pin,
                initialization_pin=self.initialization_pin, startup_pin=self.startup_pin)

    def test_partial_known_identity_without_ready_audit_seals_detached_original_targets(self):
        # Catches demanding fake readiness and using the cleared live graph.
        job = self.owned_job()
        seal = ownership.make_hotel_startup_cleanup_ownership(job, support_pin=self.pin,
            initialization_pin=self.initialization_pin, startup_pin=self.startup_pin)
        self.assertEqual(seal['candidate_schema'], 'hotel-startup-cleanup-ownership-v1')
        self.assertEqual(seal['audits'], [])
        self.assertEqual(seal['provider_scope'], {'subscription_id': 'offline-subscription'})
        job['resources'].clear()
        self.assertEqual(seal['absence_targets']['group']['id'], '/subscriptions/offline/groups/original')
        self.assertEqual(ownership.validate_hotel_startup_cleanup_ownership(
            seal, run_id=RUN, provider='azure'), seal)

    def test_known_identities_and_original_targets_survive_graph_clear(self):
        # Catches erasing original identity evidence with mutable graph progress.
        job = self.owned_job()
        projection = self.provider_projection()
        projection.update(resources={}, known_identities={}, absence_targets={})
        updated = ownership.update_hotel_startup_ownership_checkpoint(job[ownership.CHECKPOINT_KEY], projection)
        self.assertEqual(updated['resources'], {})
        self.assertEqual(updated['known_identities']['group']['id'], '/subscriptions/offline/groups/original')
        self.assertIn('group', updated['absence_targets'])

    def test_replaced_contract_scope_identity_and_target_refuse_even_with_new_hash(self):
        # Catches accepting a self-consistent replacement as original provenance.
        old = self.owned_job()[ownership.CHECKPOINT_KEY]
        for key in ('provider_scope', 'create_contract', 'known_identities', 'absence_targets'):
            with self.subTest(key=key):
                projection = self.provider_projection()
                if key in ('known_identities', 'absence_targets'):
                    projection[key]['group']['id'] = 'replacement'
                else:
                    projection[key] = {'replaced': True}
                with self.assertRaises(ownership.HotelStartupOwnershipError):
                    ownership.update_hotel_startup_ownership_checkpoint(old, projection)

    def test_unresolved_create_refuses_seal_without_discovery_or_ready_claim(self):
        job = self.owned_job()
        projection = self.provider_projection()
        projection['unresolved_creates'] = ['lost-response-vm']
        job[ownership.CHECKPOINT_KEY] = ownership.update_hotel_startup_ownership_checkpoint(
            job[ownership.CHECKPOINT_KEY], projection)
        with self.assertRaises(ownership.HotelStartupOwnershipError):
            ownership.make_hotel_startup_cleanup_ownership(job, support_pin=self.pin,
                initialization_pin=self.initialization_pin, startup_pin=self.startup_pin)

    def test_sealed_progress_cannot_expand_identity_scope_or_replace_seal(self):
        job = self.owned_job()
        seal = ownership.make_hotel_startup_cleanup_ownership(job, support_pin=self.pin,
            initialization_pin=self.initialization_pin, startup_pin=self.startup_pin)
        checkpoint = ownership.seal_hotel_startup_ownership_checkpoint(job[ownership.CHECKPOINT_KEY], seal)
        projection = self.provider_projection()
        projection['known_identities']['foreign'] = {'id': 'unexpected'}
        with self.assertRaises(ownership.HotelStartupOwnershipError):
            ownership.update_hotel_startup_ownership_checkpoint(checkpoint, projection)
        other = copy.deepcopy(seal)
        other['absence_targets']['group']['id'] = 'replacement'
        other['fingerprint'] = support._fingerprint({k: v for k, v in other.items() if k != 'fingerprint'})
        with self.assertRaises(ownership.HotelStartupOwnershipError):
            ownership.seal_hotel_startup_ownership_checkpoint(checkpoint, other)

    def test_crossed_roots_and_unknown_schema_fields_refuse(self):
        job = self.owned_job()
        job[operator.PIN_KEY] = {**self.startup_pin, 'run_id': 'abcdef123456'}
        with self.assertRaises(ValueError):
            ownership.make_hotel_startup_cleanup_ownership(job, support_pin=self.pin,
                initialization_pin=self.initialization_pin, startup_pin=self.startup_pin)
        value = self.owned_job()[ownership.CHECKPOINT_KEY]
        value['logs'] = 'not ownership'
        value['fingerprint'] = support._fingerprint({k: v for k, v in value.items() if k != 'fingerprint'})
        with self.assertRaises(ownership.HotelStartupOwnershipError):
            ownership.validate_hotel_startup_ownership_checkpoint(value, run_id=RUN, provider='azure')

    def test_safe_projection_drops_raw_observations_errors_and_nested_copies_without_mutation(self):
        # Catches arbitrary Pod/status/HTTP/error leakage and broken live custody.
        secret = 'https://registry.invalid/private?token=SIGNED_SECRET'
        job = self.owned_job()
        live = {'candidate_schema': 'hotel-guarded-live-initialization-outcome-v1',
                'run_id': RUN, 'initialization_attempt_id': INITIALIZATION,
                'initialization_executed': True, 'last_observation': {'pod': {'status': secret}},
                'logs': secret, 'error': {'message': secret}}
        job['guarded_hotel_initialization'] = live
        job['nested_carrier'] = {'again': copy.deepcopy(live)}
        original = copy.deepcopy(job)
        projected = ownership.project_hotel_startup_job(job)
        encoded = json.dumps(projected)
        self.assertNotIn('SIGNED_SECRET', encoded)
        self.assertNotIn('last_observation', encoded)
        self.assertIs(job['guarded_hotel_initialization'], live)
        self.assertEqual(job, original)
        self.assertIs(projected['guarded_hotel_initialization']['initialization_executed'], True)

    def test_startup_pin_rejects_crossed_zero_reused_attempts_and_metadata_replay(self):
        for attempt in (INITIALIZATION, '00000000-0000-0000-0000-000000000000', STARTUP.upper()):
            with self.subTest(attempt=attempt), self.assertRaises(ValueError):
                operator.make_hotel_startup_pin(RUN, self.pin, self.initialization_pin,
                    self.startup_artifacts, startup_attempt_id=attempt)
        with self.assertRaises(ValueError):
            operator.make_hotel_startup_pin(RUN, self.pin, self.initialization_pin,
                self.startup_artifacts.metadata(), startup_attempt_id=STARTUP)

    def test_no_authorized_create_receipt_requires_intact_original_precontract_checkpoint(self):
        # Catches mistaking empty current resources or a recomputed hash for
        # proof that no create ever passed the mandatory durable contract gate.
        job = self.new_job()
        checkpoint = ownership.make_hotel_startup_ownership_checkpoint(job, support_pin=self.pin,
            initialization_pin=self.initialization_pin, startup_pin=self.startup_pin)
        job[ownership.CHECKPOINT_KEY] = checkpoint
        receipt = ownership.make_hotel_startup_cleanup_not_required(job, support_pin=self.pin,
            initialization_pin=self.initialization_pin, startup_pin=self.startup_pin)
        self.assertEqual(receipt['reason'], 'provider-create-contract-never-authorized')
        self.assertNotIn('cleanup_complete', receipt)
        self.assertNotIn('independent_absence_proved', receipt)
        self.assertEqual(ownership.validate_hotel_startup_cleanup_not_required(receipt, checkpoint), receipt)
        owned = self.owned_job()[ownership.CHECKPOINT_KEY]
        owned['resources'] = {}
        owned['fingerprint'] = support._fingerprint({k: v for k, v in owned.items() if k != 'fingerprint'})
        with self.assertRaises(ownership.HotelStartupOwnershipError):
            ownership.validate_hotel_startup_cleanup_not_required(receipt, owned)

    def test_typed_identity_rejects_nested_messages_and_retains_dual_provider_ids(self):
        job = self.owned_job()
        projection = self.provider_projection()
        projection['known_identities']['vm'] = {**projection['known_identities']['group'],
            'kind': 'vm', 'name': 'original-vm', 'id': '123456',
            'self_link': 'https://compute.googleapis.com/compute/v1/projects/p/zones/z/instances/vm',
            'incarnation_id': '12345678-1234-4234-8234-123456789abc'}
        updated = ownership.update_hotel_startup_ownership_checkpoint(job[ownership.CHECKPOINT_KEY], projection)
        self.assertEqual(updated['known_identities']['vm']['id'], '123456')
        self.assertEqual(updated['known_identities']['vm']['incarnation_id'], '12345678-1234-4234-8234-123456789abc')
        projection['known_identities']['vm']['status'] = {'message': 'token=SECRET'}
        with self.assertRaises(ownership.HotelStartupOwnershipError):
            ownership.update_hotel_startup_ownership_checkpoint(job[ownership.CHECKPOINT_KEY], projection)

    def test_actual_attempt_facts_survive_confirmed_rejection_and_current_graph_removal(self):
        # Catches losing the durable pre-create attempt when native rejection
        # removes a request field; pre-generated planned UUIDs are not attempts.
        old = self.owned_job()[ownership.CHECKPOINT_KEY]
        projection = self.provider_projection()
        projection['create_attempts'] = {'vm': {
            'candidate_schema': 'hotel-startup-create-attempt-v1', 'kind': 'vm', 'name': 'original-vm',
            'scope': {'subscription_id': 'offline-subscription'}, 'request_id': INITIALIZATION}}
        attempted = ownership.update_hotel_startup_ownership_checkpoint(old, projection)
        projection['create_attempts'] = {}
        projection['resources'] = {}
        retained = ownership.update_hotel_startup_ownership_checkpoint(attempted, projection)
        self.assertEqual(retained['create_attempts']['vm']['request_id'], INITIALIZATION)
        self.assertIs(retained['contract_recorded'], True)
        projection['create_attempts'] = {'vm': {**attempted['create_attempts']['vm'], 'request_id': STARTUP}}
        with self.assertRaises(ownership.HotelStartupOwnershipError):
            ownership.update_hotel_startup_ownership_checkpoint(attempted, projection)

    def test_new_typed_target_cannot_cross_independent_provider_scope(self):
        # Catches admitting a newly observed foreign account as merely another
        # self-consistently hashed target.
        old = self.owned_job()[ownership.CHECKPOINT_KEY]
        projection = self.provider_projection()
        projection['known_identities']['foreign'] = {
            **projection['known_identities']['group'], 'id': 'foreign-id',
            'scope': {'subscription_id': 'different-subscription'}}
        with self.assertRaises(ownership.HotelStartupOwnershipError):
            ownership.update_hotel_startup_ownership_checkpoint(old, projection)
