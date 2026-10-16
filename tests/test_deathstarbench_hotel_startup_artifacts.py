"""New startup artifact capabilities are immutable and never runtime inputs."""

import copy
from dataclasses import FrozenInstanceError, replace
import json
import unittest
from unittest import mock

from app import deathstarbench_hotel_startup_artifacts as adoption
from app import deathstarbench_hotel_image_access as old_access
from app import deathstarbench_hotel_initialization_operator as old_operator
from app import deathstarbench_hotel_runtime as old_runtime
from app.deathstarbench_candidate_artifacts import CandidateArtifactBinding
from app.deathstarbench_hotel_initialization_artifacts import GuardedHotelArtifactBinding
from scripts import create_deathstarbench_hotel_startup_image_lock as factory
from tests.test_deathstarbench_candidate_image_lock import encoded, write
from tests.test_deathstarbench_hotel_startup_image_lock import StartupHotelFixtureMixin


class StartupHotelArtifactTests(StartupHotelFixtureMixin, unittest.TestCase):
    def adopt(self, data, *, save=True, **changes):
        if save:
            self.save(data)
        parameters = {'preparation': data['preparation'], 'upstream': data['upstream'],
                      'baseline': data['baseline'], 'inspector': data['registry'].inspect,
                      'blob_inspector': data['registry'].blob}
        parameters.update(changes)
        with self.evidence(data):
            return adoption.adopt_startup_hotel_artifacts(data['lock_path'], data['publication_path'], **parameters)

    def test_distinct_immutable_factory_only_binding_keeps_every_authority_closed(self):
        data = self.fixture()
        value = self.adopt(data)
        self.assertIs(type(value), adoption.StartupHotelArtifactBinding)
        self.assertNotIsInstance(value, (CandidateArtifactBinding, GuardedHotelArtifactBinding))
        self.assertEqual(value.metadata()['candidate_schema'], adoption.ADOPTION_SCHEMA)
        for flag in factory.FALSE_FLAGS:
            self.assertIs(getattr(value, flag), False, flag)
            self.assertIs(value.metadata()[flag], False, flag)
        for name in ('runtime_manifest', 'runtime_plan', 'initializer_plan', 'from_metadata', 'to_candidate'):
            self.assertFalse(hasattr(value, name), name)
        with self.assertRaises(FrozenInstanceError):
            value.application_start_allowed = True
        with self.assertRaises(TypeError):
            value.startup_attestations['frontend']['component'] = 'changed'
        with self.assertRaises(TypeError):
            value.app['platforms']['linux/amd64']['image'] = 'changed'
        self.assertEqual(replace(value).metadata(), value.metadata())
        detached = value.metadata()
        detached['startup_attestations']['frontend']['component'] = 'changed'
        self.assertEqual(value.startup_attestations['frontend']['component'], 'frontend')

    def test_replaced_fields_and_mutated_proof_are_rejected(self):
        value = self.adopt(self.fixture())
        for changes in ({'app': {}}, {'reused_images': {}}, {'dataset_attestation': {}},
                        {'startup_attestations': {}}, {'fingerprint': 'sha256:' + 'a' * 64},
                        {'_metadata_json': '{}'}, {'_proof': object()}):
            with self.subTest(changes=changes), self.assertRaises(adoption.StartupHotelArtifactError):
                replace(value, **changes)
        with self.assertRaises(adoption.StartupHotelArtifactError):
            value._proof.metadata_json = '{}'
        with self.assertRaises(adoption.StartupHotelArtifactError):
            adoption._Proof('{}', object())

    def test_old_runtime_access_and_operator_factories_refuse_exact_new_type(self):
        value = self.adopt(self.fixture())
        with self.assertRaises(old_runtime.GuardedHotelRuntimeError):
            old_runtime._checked_inputs(value, None, None)
        with self.assertRaises(old_access.GuardedHotelImageAccessError):
            old_access._context(value)
        with self.assertRaises(old_operator.HotelInitializationOperatorError):
            old_operator.make_hotel_initialization_pin('0123456789ab', {}, value)

    def test_each_adoption_reconstructs_source_and_freshly_observes_registry(self):
        data = self.fixture()
        self.save(data)
        inspect = mock.Mock(wraps=data['registry'].inspect)
        blobs = mock.Mock(wraps=data['registry'].blob)
        for _ in range(2):
            self.adopt(data, save=False, inspector=inspect, blob_inspector=blobs)
        self.assertEqual(inspect.call_count, len(set(call.args for call in inspect.call_args_list)) * 2)
        self.assertTrue(blobs.call_count)
        data['registry'].manifests[data['publication']['app']['index_image']] += b' '
        with self.assertRaises(adoption.StartupHotelArtifactError):
            self.adopt(data, save=False)

    def test_rehashed_lock_or_publication_cannot_change_closed_identity(self):
        for target in ('lock', 'publication'):
            data = self.fixture()
            self.save(data)
            path = data['lock_path' if target == 'lock' else 'publication_path']
            value = json.loads(path.read_bytes())
            value['policy_revision'] = 'other'
            write(path, encoded(value))
            with self.subTest(target=target), self.assertRaises(adoption.StartupHotelArtifactError):
                self.adopt(data, save=False)

    def test_lock_false_flags_cannot_be_promoted_even_with_recomputed_json(self):
        data = self.fixture()
        self.save(data)
        lock = json.loads(data['lock_path'].read_bytes())
        for flag in factory.FALSE_FLAGS:
            changed = copy.deepcopy(lock)
            changed[flag] = True
            write(data['lock_path'], encoded(changed))
            with self.subTest(flag=flag), self.assertRaises(adoption.StartupHotelArtifactError):
                self.adopt(data, save=False)

    def test_duplicate_nonfinite_or_symlink_input_documents_rejected(self):
        for raw in (b'{"schema_version":1,"schema_version":1}', b'{"x":NaN}', b'{}'):
            data = self.fixture()
            self.save(data)
            write(data['lock_path'], raw)
            with self.subTest(raw=raw), self.assertRaises(adoption.StartupHotelArtifactError):
                self.adopt(data, save=False)
        data = self.fixture()
        self.save(data)
        alias = data['lock_path'].with_name('linked-lock.json')
        alias.symlink_to(data['lock_path'])
        with self.evidence(data), self.assertRaises(adoption.StartupHotelArtifactError):
            adoption.adopt_startup_hotel_artifacts(
                alias, data['publication_path'], preparation=data['preparation'], upstream=data['upstream'],
                baseline=data['baseline'], inspector=data['registry'].inspect, blob_inspector=data['registry'].blob)
