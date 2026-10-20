"""Real private filesystem custody; no cloud qualification."""
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from app import deathstarbench_hotel_startup_ownership as ownership
from app import deathstarbench_hotel_startup_operator as startup
from app.run_lease import acquire_run_lease
from scripts.deathstarbench_candidate_operator_state import canonical
from tests.test_deathstarbench_hotel_startup_ownership import OriginalArtifactsMixin, RUN


class StartupStateTests(OriginalArtifactsMixin, unittest.TestCase):
    def setUp(self):
        from scripts.deathstarbench_hotel_startup_operator_state import HotelStartupStateStore
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / 'private'
        self.store = HotelStartupStateStore(self.root)
        self.directory = self.root / RUN
        self.directory.mkdir(mode=0o700)
        self.lease = acquire_run_lease(self.root, RUN, 'startup-state-test')
        self.addCleanup(self.lease.release)
        self.store.publish_pin(self.pin, self.lease)
        self.store.publish_hotel_pin(self.initialization_pin, self.lease)
        self.store.publish_startup_pin(self.startup_pin, self.lease)
        self.job = self.new_job()
        self.job[ownership.CHECKPOINT_KEY] = ownership.make_hotel_startup_ownership_checkpoint(
            self.job, support_pin=self.pin, initialization_pin=self.initialization_pin,
            startup_pin=self.startup_pin)

    def test_normal_load_checks_three_pins_without_restoring_capabilities(self):
        # Catches copying arbitrary live capabilities/history and skipping any pin.
        self.job.update(_source_capability=object(), execute=lambda: None,
                        image_access={'token': 'SECRET'}, source={'private': 'SECRET'})
        self.store.save(self.job, self.lease)
        loaded = self.store.load(RUN, self.lease)
        self.assertEqual(loaded['resources'], {})
        for key in ('_source_capability', 'execute', 'image_access', 'source'):
            self.assertNotIn(key, loaded)
        for filename in ('pin.json', 'hotel-initialization-pin.json', 'hotel-startup-pin.json'):
            path = self.directory / filename
            original = path.read_bytes()
            value = json.loads(original)
            value['run_id'] = 'fedcba654321'
            path.write_text(canonical(value))
            with self.assertRaises(ValueError):
                self.store.load(RUN, self.lease)
            path.write_bytes(original)

    def test_startup_inputs_retain_original_bytes_and_refuse_changed_adoption(self):
        # Catches canonicalizing evidence, accepting changed hashes or clobbering.
        source = self.root.parent / 'input.json'
        raw = b'{ "source" : "original" }\n\n'
        source.write_bytes(raw)
        digest = hashlib.sha256(raw).hexdigest()
        for name in ('startup-image-lock.json', 'startup-publication.json'):
            path = self.store.publish_startup_artifact_input(RUN, name, source, digest, self.lease)
            self.assertEqual(path.read_bytes(), raw)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(self.store.publish_startup_artifact_input(RUN, name, source, digest, self.lease), path)
            source.write_bytes(b'{"source":"different"}')
            with self.assertRaises(ValueError):
                self.store.publish_startup_artifact_input(RUN, name, source, digest, self.lease)
            with self.assertRaises(ValueError):
                self.store.publish_startup_artifact_input(RUN, name, source, hashlib.sha256(source.read_bytes()).hexdigest(), self.lease)
            self.assertEqual(path.read_bytes(), raw)
            source.write_bytes(raw)

    def test_startup_input_refuses_unsafe_source_json_and_destination(self):
        # Catches unbounded/ambiguous JSON, symlink/hardlink custody and broad names.
        source = self.root.parent / 'input.json'
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'\xef\xbb\xbf{}', b'{', b' ' * (1024 * 1024 + 1)):
            source.write_bytes(raw)
            with self.subTest(raw=raw[:20]), self.assertRaises(ValueError):
                self.store.publish_startup_artifact_input(RUN, 'startup-image-lock.json', source, hashlib.sha256(raw).hexdigest(), self.lease)
        source.write_bytes(b'{}')
        digest = hashlib.sha256(b'{}').hexdigest()
        for name in ('publication.json', '../startup-image-lock.json'):
            with self.assertRaises(ValueError):
                self.store.publish_startup_artifact_input(RUN, name, source, digest, self.lease)
        alias = source.with_name('alias.json')
        alias.symlink_to(source)
        with self.assertRaises(ValueError):
            self.store.publish_startup_artifact_input(RUN, 'startup-image-lock.json', alias, digest, self.lease)
        alias.unlink()
        os.link(source, alias)
        with self.assertRaises(ValueError):
            self.store.publish_startup_artifact_input(RUN, 'startup-image-lock.json', source, digest, self.lease)
        alias.unlink()
        destination = self.directory / 'startup-image-lock.json'
        destination.symlink_to(source)
        with self.assertRaises(ValueError):
            self.store.publish_startup_artifact_input(RUN, destination.name, source, digest, self.lease)

    def test_startup_input_publication_never_clobbers_racing_final_name(self):
        # Catches an exists/replace implementation instead of exclusive install.
        from scripts import deathstarbench_hotel_startup_operator_state as state
        source = self.root.parent / 'input.json'
        source.write_bytes(b'{ "original":true }\n')
        real_link = os.link
        def race(temporary, destination, **kwargs):
            Path(destination).write_bytes(b'{"foreign":true}\n')
            os.chmod(destination, 0o600)
            return real_link(temporary, destination, **kwargs)
        with mock.patch.object(state.os, 'link', side_effect=race), self.assertRaises(ValueError):
            self.store.publish_startup_artifact_input(RUN, 'startup-image-lock.json', source, hashlib.sha256(source.read_bytes()).hexdigest(), self.lease)
        self.assertEqual((self.directory / 'startup-image-lock.json').read_bytes(), b'{"foreign":true}\n')

    def test_destroy_load_ignores_corrupt_startup_history_not_corrupt_ownership(self):
        # Catches requiring execution history to obtain cleanup authority.
        self.store.save(self.job, self.lease)
        history = self.directory / 'state.json'
        history.write_bytes(b'not-json SECRET')
        loaded = self.store.load_for_destroy(RUN, self.lease)
        self.assertFalse(loaded[ownership.CHECKPOINT_KEY]['contract_recorded'])
        self.assertNotIn(startup.OPERATOR_JOURNAL_KEY, loaded)
        history.unlink()
        history.symlink_to(self.directory / 'never-open-history')
        self.assertEqual(self.store.load_for_destroy(RUN, self.lease)['id'], RUN)
        (self.directory / 'hotel-startup-owned-graph.json').write_bytes(b'{}')
        with self.assertRaises(ValueError):
            self.store.load_for_destroy(RUN, self.lease)

    def test_checkpoint_is_durable_before_history_failure_and_cleanup_skips_history(self):
        # Catches ordering history first or rerunning failed serialization in cleanup.
        from scripts import deathstarbench_hotel_startup_operator_state as state
        with mock.patch.object(state, 'project_hotel_startup_job', side_effect=ValueError('SECRET')):
            with self.assertRaises(ValueError):
                self.store.save(self.job, self.lease)
            loaded = self.store.load_for_destroy(RUN, self.lease)
            self.store.save_cleanup_progress(loaded, self.lease)
        self.assertFalse((self.directory / 'state.json').exists())
        self.assertEqual(loaded[ownership.CHECKPOINT_KEY], self.job[ownership.CHECKPOINT_KEY])

    def test_immutable_install_is_exclusive_and_never_replaces_racing_target(self):
        # Catches exists-then-replace races at the actual final-name boundary.
        from scripts import deathstarbench_hotel_startup_operator_state as state
        path = self.directory / 'racing.json'
        real_link = os.link
        def race(source, destination, **kwargs):
            Path(destination).write_text('{"foreign":true}\n')
            os.chmod(destination, 0o600)
            return real_link(source, destination, **kwargs)
        with mock.patch.object(state.os, 'link', side_effect=race):
            with self.assertRaises(FileExistsError):
                state._install_immutable(path, {'ours': True})
        self.assertEqual(json.loads(path.read_text()), {'foreign': True})

    def test_checkpoint_link_and_oversized_pin_refuse_without_opening_history(self):
        self.store.save(self.job, self.lease)
        checkpoint = self.directory / 'hotel-startup-owned-graph.json'
        original = checkpoint.read_bytes()
        checkpoint.unlink()
        checkpoint.symlink_to(self.directory / 'state.json')
        with self.assertRaises(ValueError):
            self.store.load_for_destroy(RUN, self.lease)
        checkpoint.unlink()
        checkpoint.write_bytes(original)
        checkpoint.chmod(0o600)
        pin = self.directory / 'hotel-startup-pin.json'
        with pin.open('r+b') as stream:
            stream.truncate(1024 * 1024 + 1)
        with self.assertRaises(ValueError):
            self.store.load_for_destroy(RUN, self.lease)

    def test_missing_checkpoint_cannot_be_reinitialized_after_initial_publication(self):
        self.store.save(self.job, self.lease)
        (self.directory / 'hotel-startup-owned-graph.json').unlink()
        (self.directory / 'state.json').unlink()
        with self.assertRaises(ValueError):
            self.store.save(self.job, self.lease)

    def test_normal_load_of_not_required_receipt_binds_independent_checkpoint_first(self):
        self.store.save(self.job, self.lease)
        receipt = ownership.make_hotel_startup_cleanup_not_required(self.job, support_pin=self.pin,
            initialization_pin=self.initialization_pin, startup_pin=self.startup_pin)
        self.job['hotel_startup_cleanup_not_required'] = receipt
        self.store.save(self.job, self.lease)
        self.assertEqual(self.store.load(RUN, self.lease).get('hotel_startup_cleanup_not_required'), receipt)


if __name__ == '__main__':
    unittest.main()
