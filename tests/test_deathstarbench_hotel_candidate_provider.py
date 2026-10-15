"""Synthetic provider wiring only; never a cloud or deletion qualification."""

import copy
from contextlib import ExitStack
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from app import deathstarbench_candidate_operator as support
from app import deathstarbench_hotel_initialization_operator as hotel
from app.models import BenchmarkPlan
from app.run_lease import acquire_run_lease
from scripts import deathstarbench_candidate_cloud_audit as audit
from scripts import deathstarbench_candidate_provider as provider
from tests import test_deathstarbench_hotel_initialization_artifacts as artifacts
from tests.test_deathstarbench_candidate_operator_cli import plan_values


RUN = '123456abcdef'


class HotelCandidateProviderTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory())) / '.candidate-support'
        root.mkdir(mode=0o700)
        (root / RUN).mkdir(mode=0o700)
        self.lease = acquire_run_lease(root, RUN, 'hotel-provider-test')
        self.addCleanup(self.lease.release)
        helper = artifacts.GuardedHotelArtifactsTests()
        self.addCleanup(helper.doCleanups)
        data = helper.fixture()
        self.guarded = helper.adopt(data)
        self.baseline = data['baseline']

    def fixture(self, cloud_provider='azure', *, with_identity=True):
        plan = BenchmarkPlan.model_validate({**plan_values(cloud_provider),
            'ssh_private_key': 'synthetic-only', 'ssh_public_key': 'synthetic-only'})
        saved_plan = plan.model_dump(exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'})
        pin = {
            'schema_version': 1, 'candidate_schema': support.PIN_SCHEMA,
            'candidate_only': True, 'released': False, 'run_id': RUN,
            'workload_id': 'hotel_reservation', 'artifact_fingerprint': self.baseline.fingerprint,
            'artifacts': self.baseline.metadata(), 'infrastructure_plan': saved_plan,
            'provider_pin': {'provider': cloud_provider}, 'ssh_public_key_sha256': '1' * 64,
        }
        pin['fingerprint'] = support._fingerprint(pin)
        hotel_pin = hotel.make_hotel_initialization_pin(RUN, pin, self.guarded)
        record = hotel._new_record(hotel_pin)
        identity = {
            'revision': audit.REVISION, 'provider': cloud_provider, 'run_id': RUN,
            'plan_sha256': audit._sha(saved_plan), 'account': {'fixture_only': 'no-live-identity'},
            'resources': [],
        }
        resources = {'provider': cloud_provider, cloud_provider + '_distributed_candidate': True}
        if cloud_provider == 'gcp':
            for index, prefix in enumerate(provider.gcp.DISTRIBUTED_RESOURCE_PREFIXES, 1):
                link = 'https://www.googleapis.com/compute/v1/projects/candidate-project/global/networks/' + prefix
                identity['resources'].append({'prefix': prefix, 'id': str(index), 'self_link': link})
                resources.update({prefix + '_id': str(index), prefix + '_self_link': link})
        if with_identity:
            record.update(state='initializing', cloud_identity=identity, audits=[{
                'schema_version': 1, 'candidate_only': True, 'read_only': True,
                'run_id': RUN, 'provider': cloud_provider, 'cloud_identity': copy.deepcopy(identity),
            }])
        job = {
            'id': RUN, '_persist_state': False, 'results': [], 'plan': saved_plan,
            'resources': resources, support.PIN_KEY: pin, hotel.PIN_KEY: hotel_pin,
            hotel.OPERATOR_JOURNAL_KEY: record,
        }
        saved = []
        cloud = provider.CandidateCloud(plan, pin['provider_pin'],
            persist=lambda value: saved.append(copy.deepcopy(value)), emit=lambda *_args: None,
            operator_journal_key=hotel.OPERATOR_JOURNAL_KEY)
        return job, cloud, saved

    def cleanup(self, job, cloud, *, guard_error=None):
        adapter = provider.azure if cloud.plan.provider == 'azure' else provider.gcp
        absent_name = 'audit_' + cloud.plan.provider + '_absent'
        with mock.patch.object(audit, 'require_azure_cleanup_identity', side_effect=guard_error) as guard, \
             mock.patch.object(adapter, 'destroy_resources') as destroy, \
             mock.patch.object(audit, absent_name, return_value={'fixture_only': True}), \
             mock.patch.object(provider.application, '_cleared_distributed_provider_contract', return_value=True):
            if guard_error is not None:
                with self.assertRaisesRegex(ValueError, 'replacement'):
                    cloud.destroy_and_prove(job, self.lease)
                destroy.assert_not_called()
            else:
                result = cloud.destroy_and_prove(job, self.lease)
                self.assertTrue(result['cleanup_complete'])
                destroy.assert_called_once()
            return guard

    def test_only_known_operator_selector_keys_are_accepted(self):
        for value in ('arbitrary-field', None, True):
            with self.assertRaises(ValueError):
                provider.CandidateCloud(None, {}, persist=None, emit=None, operator_journal_key=value)

    def test_hotel_selects_its_own_audit_not_legacy_decoy_before_azure_delete(self):
        job, cloud, saved = self.fixture()
        job[support.OPERATOR_JOURNAL_KEY] = {'cloud_identity': {'legacy-decoy': True}}
        expected = copy.deepcopy(job[hotel.OPERATOR_JOURNAL_KEY]['cloud_identity'])
        guard = self.cleanup(job, cloud)
        self.assertEqual(guard.call_args.args[1], expected)
        self.assertEqual(saved[0][provider.CLEANUP_SOURCE_KEY]['cloud_identity'], expected)

    def test_azure_opaque_identity_drift_refuses_before_delete(self):
        job, cloud, _saved = self.fixture()
        guard = self.cleanup(job, cloud, guard_error=ValueError('same-name replacement'))
        guard.assert_called_once()

    def test_crossed_run_pin_or_audit_refuses_before_archive_or_azure_delete(self):
        for change in ('run', 'audit', 'pin'):
            with self.subTest(change=change):
                job, cloud, saved = self.fixture()
                if change == 'run':
                    job[hotel.OPERATOR_JOURNAL_KEY]['cloud_identity']['run_id'] = 'fedcba654321'
                    job[hotel.OPERATOR_JOURNAL_KEY]['audits'][0]['cloud_identity']['run_id'] = 'fedcba654321'
                elif change == 'audit':
                    job[hotel.OPERATOR_JOURNAL_KEY]['audits'][0]['read_only'] = False
                else:
                    job[hotel.PIN_KEY]['support_pin_fingerprint'] = 'sha256:' + '0' * 64
                with mock.patch.object(provider.azure, 'destroy_resources') as destroy, self.assertRaises(ValueError):
                    cloud.destroy_and_prove(job, self.lease)
                destroy.assert_not_called()
                self.assertEqual(saved, [])
                self.assertNotIn(provider.CLEANUP_SOURCE_KEY, job)

    def test_gcp_audited_numeric_graph_is_preserved_and_existing_delete_guard_used(self):
        job, cloud, saved = self.fixture('gcp')
        guard = self.cleanup(job, cloud)
        guard.assert_not_called()
        self.assertEqual(saved[0][provider.CLEANUP_SOURCE_KEY]['cloud_identity'],
                         job[hotel.OPERATOR_JOURNAL_KEY]['cloud_identity'])

    def test_gcp_numeric_or_link_drift_refuses_before_archive_or_delete(self):
        for field in ('id', 'self_link'):
            with self.subTest(field=field):
                job, cloud, saved = self.fixture('gcp')
                prefix = provider.gcp.DISTRIBUTED_RESOURCE_PREFIXES[0]
                job['resources'][prefix + '_' + field] = '999' if field == 'id' else 'projects/other/global/networks/replacement'
                with mock.patch.object(provider.gcp, 'destroy_resources') as destroy, self.assertRaises(ValueError):
                    cloud.destroy_and_prove(job, self.lease)
                destroy.assert_not_called()
                self.assertEqual(saved, [])
                self.assertNotIn(provider.CLEANUP_SOURCE_KEY, job)

    def test_retained_cleanup_source_cannot_cross_the_original_audit(self):
        job, cloud, _saved = self.fixture()
        self.cleanup(job, cloud)
        source = job[provider.CLEANUP_SOURCE_KEY]
        source['cloud_identity']['account'] = {'crossed': 'replacement'}
        source['fingerprint'] = provider.fingerprint({key: value for key, value in source.items() if key != 'fingerprint'})
        with mock.patch.object(provider.azure, 'destroy_resources') as destroy, self.assertRaisesRegex(ValueError, 'crossed'):
            cloud.destroy_and_prove(job, self.lease)
        destroy.assert_not_called()

    def test_audit_mutation_during_archive_persistence_refuses_before_delete(self):
        job, cloud, _saved = self.fixture()
        def mutate(value):
            identity = value[hotel.OPERATOR_JOURNAL_KEY]['cloud_identity']
            identity['account'] = {'replacement': True}
            value[hotel.OPERATOR_JOURNAL_KEY]['audits'][0]['cloud_identity'] = copy.deepcopy(identity)
        cloud.persist = mutate
        with mock.patch.object(provider.azure, 'destroy_resources') as destroy, self.assertRaisesRegex(ValueError, 'changed during'):
            cloud.destroy_and_prove(job, self.lease)
        destroy.assert_not_called()

    def test_partial_provision_without_ready_audit_retains_exact_unaudited_cleanup(self):
        job, cloud, saved = self.fixture(with_identity=False)
        guard = self.cleanup(job, cloud)
        guard.assert_not_called()
        self.assertIsNone(saved[0][provider.CLEANUP_SOURCE_KEY]['cloud_identity'])


if __name__ == '__main__':
    unittest.main()
