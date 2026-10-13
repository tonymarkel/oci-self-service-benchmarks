"""Adoption reuses full evidence validation; no cloud/registry writes occur."""

from contextlib import contextmanager
import copy
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from app import deathstarbench_candidate_artifacts as artifacts
from app.deathstarbench_candidate_workload import (
    render_candidate_workload_bundle, validate_candidate_assets,
)
from tests import test_deathstarbench_candidate_image_lock as fixtures


class CandidateArtifactAdoptionTests(unittest.TestCase):
    def fixture(self, workload='hotel_reservation'):
        case = fixtures.CandidateLockTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        publication, *inputs = case.fixture(workload)
        identities, driver_document, driver_identity = inputs
        prepared = {'prepared': True}
        with self.prepared_inputs(prepared, inputs):
            lock = artifacts.validation.create_candidate_lock(
                workload, case.workload_path, case.driver_path, publication,
                inspector=case.registry.inspect, blob_inspector=case.registry.blob,
            )
        lock = copy.deepcopy(lock)
        directory = case.workload_path.parent
        lock_path, publication_path = directory / 'lock.json', directory / 'publication.json'
        fixtures.write(lock_path, fixtures.encoded(lock))
        fixtures.write(publication_path, fixtures.encoded(publication))
        return {'case': case, 'inputs': inputs, 'prepared': prepared, 'lock': lock,
                'publication': publication, 'lock_path': lock_path, 'publication_path': publication_path}

    @contextmanager
    def prepared_inputs(self, document, inputs):
        # Unit fixtures replace only the independently tested source-tree
        # validators; actual platform/config/native-smoke/driver validators run.
        identities, driver_document, driver_identity = inputs
        with mock.patch.object(artifacts.validation, '_workload_preparation', return_value=(document, identities)) as workload, \
             mock.patch.object(artifacts.validation, '_driver_preparation', return_value=(driver_document, driver_identity)) as driver:
            yield workload, driver

    def adopt(self, data, **changes):
        case = data['case']
        parameters = dict(workload_preparation=case.workload_path, driver_preparation=case.driver_path,
                          inspector=case.registry.inspect, blob_inspector=case.registry.blob)
        parameters.update(changes)
        with self.prepared_inputs(data['prepared'], data['inputs']):
            return artifacts.adopt_candidate_artifacts(
                data['lock']['workload_id'], data['lock_path'], data['publication_path'], **parameters,
            )

    def save(self, data):
        fixtures.write(data['lock_path'], fixtures.encoded(data['lock']))
        fixtures.write(data['publication_path'], fixtures.encoded(data['publication']))

    def test_both_workloads_revalidate_then_adopt_only_actual_role_platforms(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            with self.subTest(workload=workload):
                data = self.fixture(workload)
                inspect = mock.Mock(wraps=data['case'].registry.inspect)
                blob = mock.Mock(wraps=data['case'].registry.blob)
                binding = self.adopt(data, inspector=inspect, blob_inspector=blob)
                profile = artifacts.distributed_workload_profile(workload)
                components, _ = validate_candidate_assets(workload)
                application_keys = {item['image_key'] for item in components if item['role'] == 'application'}
                self.assertEqual(binding.workload_id, workload)
                self.assertEqual(binding.runtime_manifest['schema_version'], 2)
                self.assertEqual(set(binding.runtime_manifest['platforms']['linux/amd64']['images']), set(profile.required_image_keys))
                self.assertEqual(set(binding.runtime_manifest['platforms']['linux/arm64']['images']), application_keys)
                self.assertEqual(binding.load_driver_image, data['lock']['images']['load-driver']['platforms']['linux/amd64']['image'])
                self.assertEqual(binding.load_driver['platform'], 'linux/amd64')
                self.assertEqual(binding.load_driver['artifact']['request_script_sha256'], data['inputs'][1]['prepared_request_script_sha256'])
                inspected = {call.args[0] for call in inspect.call_args_list}
                inspected_configs = {call.args for call in blob.call_args_list}
                for image in data['lock']['images'].values():
                    self.assertIn(image['index_image'], inspected)
                    for item in image['platforms'].values():
                        self.assertIn(item['image'], inspected)
                        self.assertIn((item['image'].split('@')[0], item['config_digest']), inspected_configs)
                for architecture in ('x86_64', 'aarch64'):
                    bundle = render_candidate_workload_bundle(workload, binding.runtime_manifest, architecture, '10.2.3.4')
                    workloads = {item['metadata']['name']: item for item in json.loads(bundle.canonical_json)['items'] if item['kind'] == 'Deployment'}
                    for component in components:
                        selected = 'linux/arm64' if architecture == 'aarch64' and component['role'] == 'application' else 'linux/amd64'
                        self.assertEqual(workloads[component['name']]['spec']['template']['spec']['containers'][0]['image'],
                                         data['lock']['images'][component['image_key']]['platforms'][selected]['image'])

    def test_all_gates_stay_false_and_metadata_is_complete_copy_safe_json(self):
        data = self.fixture()
        binding = self.adopt(data)
        metadata = binding.metadata()
        for name in ('released', 'runtime_qualified', 'measurement_qualified', 'anonymous_pull_qualified'):
            self.assertIs(getattr(binding, name), False)
            self.assertIs(metadata[name], False)
        self.assertIs(binding.candidate_only, True)
        self.assertEqual(metadata['candidate_lock_sha256'], hashlib.sha256(data['lock_path'].read_bytes()).hexdigest())
        self.assertEqual(metadata['publication_document_sha256'], hashlib.sha256(data['publication_path'].read_bytes()).hexdigest())
        self.assertEqual(metadata['publication_evidence_sha256'], data['lock']['publication_evidence_sha256'])
        self.assertEqual(metadata['workload_preparation_manifest_sha256'], data['publication']['workload_preparation_manifest_sha256'])
        self.assertEqual(metadata['driver_preparation_manifest_sha256'], data['publication']['driver_preparation_manifest_sha256'])
        self.assertEqual(metadata['preparation_identity_sha256'], artifacts._fingerprint(data['lock']['preparation']))
        self.assertEqual(metadata['fingerprint'], binding.fingerprint)
        self.assertEqual(json.loads(json.dumps(metadata)), metadata)
        self.assertTrue(metadata['remaining_gates'])
        with self.assertRaises(TypeError):
            binding.runtime_manifest['platforms']['linux/amd64']['images']['mongodb'] = 'changed'
        with self.assertRaises(TypeError):
            binding.load_driver['artifact']['wrk_binary_sha256'] = 'changed'
        with self.assertRaises(FrozenInstanceError):
            binding.released = True
        metadata['runtime_manifest']['platforms']['linux/amd64']['images']['mongodb'] = 'changed'
        metadata['load_driver']['artifact']['wrk_binary_sha256'] = 'changed'
        self.assertNotEqual(binding.metadata(), metadata)
        self.assertEqual(self.adopt(data).fingerprint, binding.fingerprint)

    def test_dataclass_replace_cannot_reuse_authority_for_changed_fields(self):
        binding = self.adopt(self.fixture())
        other = self.adopt(self.fixture('media_microservices'))
        changes = (
            {'workload_id': other.workload_id}, {'runtime_manifest': other.runtime_manifest},
            {'load_driver': other.load_driver}, {'image_bindings': other.image_bindings},
            {'fingerprint': other.fingerprint}, {'_metadata_json': other._metadata_json},
            {'runtime_manifest': other.runtime_manifest, 'workload_id': other.workload_id,
             'load_driver': other.load_driver, 'image_bindings': other.image_bindings,
             'fingerprint': other.fingerprint, '_metadata_json': other._metadata_json},
        )
        for change in changes:
            with self.subTest(change=tuple(change)), self.assertRaises(artifacts.CandidateArtifactError):
                replace(binding, **change)
        with self.assertRaises(TypeError):
            replace(binding._adoption_token, metadata_json=other._metadata_json)
        with self.assertRaises(artifacts.CandidateArtifactError):
            binding._adoption_token.metadata_json = other._metadata_json
        self.assertEqual(replace(binding).metadata(), binding.metadata())

    def test_arbitrary_factory_token_or_serialized_metadata_cannot_construct_binding(self):
        binding = self.adopt(self.fixture())
        with self.assertRaises(artifacts.CandidateArtifactError):
            replace(binding, _adoption_token=object())
        self.assertFalse(hasattr(artifacts.CandidateArtifactBinding, 'from_metadata'))

    def test_source_preparations_are_required_before_any_registry_call(self):
        data = self.fixture()
        for changes in ({'workload_preparation': Path('/nonexistent-prepared-workload')},
                        {'driver_preparation': Path('/nonexistent-prepared-driver')}):
            inspect, blob = mock.Mock(), mock.Mock()
            with self.subTest(changes=changes), self.assertRaises(artifacts.CandidateArtifactError):
                self.adopt(data, inspector=inspect, blob_inspector=blob, **changes)
            inspect.assert_not_called()
            blob.assert_not_called()

    def test_full_source_validation_failure_is_not_replaced_by_embedded_hashes(self):
        data = self.fixture()
        inspect, blob = mock.Mock(), mock.Mock()
        with mock.patch.object(artifacts.validation, '_workload_preparation', side_effect=artifacts.validation.CandidateImageLockError('prepared source drifted')):
            with self.assertRaisesRegex(artifacts.CandidateArtifactError, 'source drifted'):
                artifacts.adopt_candidate_artifacts(data['lock']['workload_id'], data['lock_path'], data['publication_path'],
                                                    workload_preparation=data['case'].workload_path, driver_preparation=data['case'].driver_path,
                                                    inspector=inspect, blob_inspector=blob)
        inspect.assert_not_called()
        blob.assert_not_called()

    def test_lock_gate_revision_preparation_config_and_attestation_tampering_fail(self):
        for change in ('released', 'runtime_qualified', 'measurement_qualified', 'anonymous_pull_qualified',
                       'workload_id', 'workload_revision', 'image_set_revision', 'load_driver_revision',
                       'upstream_revision', 'schema-bool', 'preparation', 'config', 'attestation', 'unknown-field'):
            data = self.fixture()
            lock = data['lock']
            if change in ('released', 'runtime_qualified', 'measurement_qualified', 'anonymous_pull_qualified'):
                lock[change] = True
            elif change == 'schema-bool':
                lock['schema_version'] = True
            elif change == 'preparation':
                lock['preparation']['workload']['prepared'] = False
            elif change == 'config':
                lock['images']['hotel-reservation']['platforms']['linux/amd64']['config_digest'] = 'sha256:' + 'a' * 64
            elif change == 'attestation':
                lock['images']['load-driver']['platforms']['linux/amd64']['driver_attestation']['wrk_binary_sha256'] = 'a' * 64
            elif change == 'unknown-field':
                lock['invented_qualification'] = True
            else:
                lock[change] = 'crossed-or-stale'
            self.save(data)
            with self.subTest(change=change), self.assertRaises(artifacts.CandidateArtifactError):
                self.adopt(data)

    def test_publication_binary_or_request_drift_cannot_be_hidden_by_rehashing_lock(self):
        data = self.fixture()
        driver = data['publication']['load_driver']['platforms']['linux/amd64']['smoke_receipt']
        marker = driver['load_driver_attestation'].replace('wrk_binary_sha256=' + 'b' * 64, 'wrk_binary_sha256=' + 'a' * 64)
        driver['load_driver_attestation'] = marker
        driver['inspection_executions'][-1].update(stdout=marker, stdout_sha256=hashlib.sha256(marker.encode()).hexdigest())
        data['lock']['publication_evidence_sha256'] = hashlib.sha256(fixtures.encoded(data['publication'])).hexdigest()
        self.save(data)
        with self.assertRaises(artifacts.CandidateArtifactError):
            self.adopt(data)

    def test_registry_byte_drift_and_crossed_platform_proofs_fail(self):
        for change in ('manifest', 'config', 'platform'):
            data = self.fixture()
            item = data['lock']['images']['hotel-reservation']['platforms']['linux/amd64']
            registry = data['case'].registry
            if change == 'manifest':
                registry.manifests[item['image']] += b' '
            elif change == 'config':
                registry.blobs[(item['image'].split('@')[0], item['config_digest'])] += b' '
            else:
                proof = data['publication']['custom_images']['app']['platforms']
                proof['linux/amd64']['image'] = proof['linux/arm64']['image']
                self.save(data)
            with self.subTest(change=change), self.assertRaises(artifacts.CandidateArtifactError):
                self.adopt(data)

    def test_documents_reject_duplicate_keys_non_json_values_and_unsafe_files(self):
        cases = (b'{"schema_version":1,"schema_version":1}', b'{"number":NaN}', b'{"number":1e9999}',
                 b'{"text":"\\ud800"}', b'\xef\xbb\xbf{}', '{}'.encode('utf-16'), b'[]')
        for raw in cases:
            data = self.fixture()
            fixtures.write(data['lock_path'], raw)
            with self.subTest(raw=raw), self.assertRaises(artifacts.CandidateArtifactError):
                self.adopt(data)
        data = self.fixture()
        link = data['lock_path'].parent / 'linked.json'
        link.symlink_to(data['lock_path'])
        with self.assertRaises(artifacts.CandidateArtifactError):
            artifacts._read_input(link, 'symlink')
        with mock.patch.object(artifacts, 'MAX_DOCUMENT_BYTES', 1):
            with self.assertRaises(artifacts.CandidateArtifactError):
                artifacts._read_input(data['lock_path'], 'oversized')

    def test_reader_bounds_growth_after_initial_stat(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'growing.json'
            path.write_bytes(b'{}' + b' ' * 100)
            actual = path.lstat()
            descriptor = mock.Mock(st_mode=actual.st_mode, st_size=2, st_dev=actual.st_dev, st_ino=actual.st_ino)
            with mock.patch.object(artifacts, 'MAX_DOCUMENT_BYTES', 16), mock.patch.object(Path, 'lstat', return_value=descriptor):
                with self.assertRaisesRegex(artifacts.CandidateArtifactError, 'bounded UTF-8'):
                    artifacts._read_input(path, 'grown input')

    def test_social_and_unknown_workloads_never_enter_candidate_adoption(self):
        for workload in ('social_network', 'unknown', None):
            with self.subTest(workload=workload), self.assertRaises(artifacts.CandidateArtifactError):
                artifacts.adopt_candidate_artifacts(workload, Path('/unused'), Path('/unused'),
                                                    workload_preparation=Path('/unused'), driver_preparation=Path('/unused'))


if __name__ == '__main__':
    unittest.main()
