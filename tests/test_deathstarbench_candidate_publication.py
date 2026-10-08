import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import publish_deathstarbench_candidate_images as publish
from scripts.deathstarbench_artifact_common import PreparationError, sha256

ROOT = Path(__file__).resolve().parents[1]


class CandidatePublicationTests(unittest.TestCase):
    def test_matrix_is_native_and_support_roles_are_not_arm(self):
        plan = publish.plan("all")
        self.assertEqual(plan["workloads"], ["hotel_reservation", "media_microservices"])
        self.assertEqual(len(plan["native"]["include"]), 4)
        self.assertEqual({r["runner"] for r in plan["native"]["include"]}, {"ubuntu-24.04", "ubuntu-24.04-arm"})
        self.assertEqual(len(publish.plan("hotel_reservation")["native"]["include"]), 2)
        with self.assertRaises(PreparationError):
            publish.plan("social_network")

    def test_candidate_names_are_workload_and_run_specific(self):
        hotel = publish.candidate_name("Example", "hotel_reservation", "app", "123", "1")
        self.assertEqual(hotel, "ghcr.io/example/deathstarbench-hotel-app:dsb-6ecb097-123-1")
        self.assertNotEqual(hotel, publish.candidate_name("example", "hotel_reservation", "app", "123", "2"))
        self.assertNotEqual(hotel, publish.candidate_name("example", "media_microservices", "app", "123", "1"))
        for args in (("bad/owner", "hotel_reservation", "app", "123", "1"),
                     ("example", "social_network", "app", "123", "1"),
                     ("example", "hotel_reservation", "frontend", "123", "1"),
                     ("example", "hotel_reservation", "app", "$(id)", "1"),
                     ("example", "hotel_reservation", "app", "123", "0")):
            with self.subTest(args=args), self.assertRaises(PreparationError):
                publish.candidate_name(*args)

    def _roots_and_records(self, root):
        for name in ("workload", "driver"):
            (root / name).mkdir()
            (root / name / "context-manifest.json").write_text('{}\n')
        records = []
        for platform in ("linux/amd64", "linux/arm64"):
            images = {"app": {"build_receipt": {"published_index_image": "original-" + platform}}}
            if platform == "linux/amd64":
                images["load_driver"] = {}
            records.append({"schema_version": 1, "candidate_schema": "candidate-native-publication-v1",
                            "workload_id": "hotel_reservation", "platform": platform, "images": images,
                            "workload_preparation_manifest_sha256": sha256(root / "workload/context-manifest.json"),
                            "driver_preparation_manifest_sha256": sha256(root / "driver/context-manifest.json")})
        return records

    def test_records_preserve_original_native_indexes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._roots_and_records(root)
            before = copy.deepcopy(records)
            validated = publish.validate_records("hotel_reservation", root, records)
            self.assertEqual(records, before)
            self.assertEqual(validated["linux/arm64"]["images"]["app"]["build_receipt"]["published_index_image"], "original-linux/arm64")

    def test_missing_duplicate_or_crossed_records_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid = self._roots_and_records(root)
            bad_workload = copy.deepcopy(valid)
            bad_workload[0]["workload_id"] = "media_microservices"
            arm_driver = copy.deepcopy(valid)
            arm_driver[1]["images"]["load_driver"] = {}
            boolean_schema = copy.deepcopy(valid)
            boolean_schema[0]["schema_version"] = True
            for records in ([valid[0]], [valid[0], valid[0]], bad_workload, arm_driver, boolean_schema):
                with self.subTest(records=records), self.assertRaises(PreparationError):
                    publish.validate_records("hotel_reservation", root, records)

    def test_regenerated_manifest_drift_fails_before_registry_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._roots_and_records(root)
            (root / "workload/context-manifest.json").write_text('{"drift":true}\n')
            with mock.patch.object(publish.subprocess, "run") as run:
                with self.assertRaises(PreparationError):
                    publish.assemble("hotel_reservation", root, records, {}, "example", "1", "1", root / "output")
                run.assert_not_called()
            self.assertFalse((root / "output").exists())

    def test_native_runner_rejects_mismatched_host_before_pull(self):
        with mock.patch.object(publish.platform, "system", return_value="Linux"), \
             mock.patch.object(publish.platform, "machine", return_value="x86_64"), \
             mock.patch.object(publish.subprocess, "run") as run:
            with self.assertRaises(PreparationError):
                publish.native_record("hotel_reservation", "arm64", Path("not-used"), {})
            run.assert_not_called()

    def test_preparation_never_overwrites_existing_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sentinel = root / "sentinel"
            sentinel.write_text("keep")
            with self.assertRaises(PreparationError):
                publish.prepare("hotel_reservation", Path("unused"), Path("unused"), root)
            self.assertEqual(sentinel.read_text(), "keep")

    def test_publication_input_rejects_duplicate_json_keys_and_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / 'receipt.json'
            path.write_text('{"released": true, "released": false}')
            with self.assertRaises(publish.CandidateImageLockError):
                publish._read_document(path, 'publication input')
            path.write_text('{}')
            link = root / 'alias.json'
            link.symlink_to(path)
            with self.assertRaises(publish.CandidateImageLockError):
                publish._read_document(link, 'publication input')

    def test_workflow_is_manual_native_and_remote_actions_pinned(self):
        called = (ROOT / '.github/workflows/deathstarbench-candidate-images.yml').read_text()
        caller = (ROOT / '.github/workflows/deathstarbench-social-images.yml').read_text()
        trigger = called.split('permissions:', 1)[0]
        self.assertIn('workflow_call:', trigger)
        self.assertNotIn('workflow_dispatch:', trigger)
        self.assertNotIn('pull_request:', trigger)
        self.assertNotIn('push:', trigger)
        self.assertIn('uses: ./.github/workflows/deathstarbench-candidate-images.yml', caller)
        self.assertIn('hotel-media-images', caller)
        self.assertIn('contents: read\n  packages: write', called)
        self.assertNotIn('id-token:', called)
        self.assertNotIn('setup-qemu', called)
        self.assertNotIn('secrets: inherit', caller)
        for line in called.splitlines():
            if 'uses:' in line:
                self.assertRegex(line.split('@', 1)[1].split()[0], r'^[0-9a-f]{40}$')
        self.assertIn("if: matrix.architecture == 'amd64'", called)
        self.assertIn('provenance: mode=max', called)
        self.assertIn('sbom: true', called)
        self.assertIn('include-hidden-files: true', called)
        self.assertLess(called.index('Verify backing-image'), called.index('Publish native app'))
        self.assertIn('Hotel and Media remain unreleased', called)


if __name__ == '__main__':
    unittest.main()
