import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

from scripts import validate_deathstarbench_candidate_artifacts as validator


class CandidateArtifactValidationTests(unittest.TestCase):
    def test_two_round_receipts_and_no_image_qualification_claims(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            rock = root / "socket.rock"
            rock.write_bytes(b"protected")

            def prepare(workload, output):
                output.mkdir()
                (output / "context").mkdir()
                (output / "context/input").write_text("deterministic source")
                digest = validator.tree_sha256(output / "context")
                manifest = {"candidate_only": True, "released": False,
                            "workload_id": workload, "upstream_revision": validator.UPSTREAM_REVISION,
                            "contexts": {"app": {"path": "context", "context_sha256": digest}},
                            "context_path": "context", "context_sha256": digest}
                (output / "context-manifest.json").write_text(json.dumps(manifest, sort_keys=True))
                return manifest

            with mock.patch.object(validator.hotel, "prepare_contexts", side_effect=lambda source, out: prepare("hotel_reservation", out)), \
                    mock.patch.object(validator.media, "prepare_contexts", side_effect=lambda source, out: prepare("media_microservices", out)), \
                    mock.patch.object(validator.driver, "prepare_context", side_effect=lambda workload, source, rock, out: prepare(workload, out)):
                result = validator.validate(source, rock, root / "out")
            self.assertEqual(result["independent_preparation_rounds"], 2)
            self.assertEqual(len(result["artifact_receipts"]), 4)
            for field in ("released", "images_built", "images_published", "runtime_qualified", "byte_reproducible_image_rebuild"):
                self.assertIs(result[field], False)

    def test_wrong_candidate_identity_cleans_partial_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            rock = root / "socket.rock"
            rock.write_bytes(b"protected")
            with mock.patch.object(validator.hotel, "prepare_contexts", return_value={"released": True}):
                with self.assertRaisesRegex(validator.PreparationError, "receipt"):
                    validator.validate(source, rock, root / "out")
            self.assertFalse((root / "out").exists())
            self.assertEqual(rock.read_bytes(), b"protected")

    def test_stale_declared_context_digest_cannot_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            rock = root / "socket.rock"
            rock.write_bytes(b"protected")

            def prepare(source, output):
                output.mkdir()
                (output / "context").mkdir()
                (output / "context/input").write_text("changed context")
                manifest = {"candidate_only": True, "released": False,
                            "workload_id": "hotel_reservation", "upstream_revision": validator.UPSTREAM_REVISION,
                            "contexts": {"app": {"path": "context", "context_sha256": "0" * 64}}}
                (output / "context-manifest.json").write_text(json.dumps(manifest))
                return manifest

            with mock.patch.object(validator.hotel, "prepare_contexts", side_effect=prepare):
                with self.assertRaisesRegex(validator.PreparationError, "digest drifted"):
                    validator.validate(source, rock, root / "out")
            self.assertFalse((root / "out").exists())

    def test_manifest_omitted_file_drift_cannot_pass_two_rounds(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            rock = root / "socket.rock"
            rock.write_bytes(b"protected")

            def prepare(workload, output):
                output.mkdir()
                (output / "context").mkdir()
                (output / "context/input").write_text("stable")
                (output / "not-in-manifest").write_text(output.parent.name)
                digest = validator.tree_sha256(output / "context")
                manifest = {"candidate_only": True, "released": False,
                            "workload_id": workload, "upstream_revision": validator.UPSTREAM_REVISION,
                            "contexts": {"app": {"path": "context", "context_sha256": digest}},
                            "context_path": "context", "context_sha256": digest}
                (output / "context-manifest.json").write_text(json.dumps(manifest, sort_keys=True))
                return manifest

            with mock.patch.object(validator.hotel, "prepare_contexts", side_effect=lambda source, out: prepare("hotel_reservation", out)), \
                    mock.patch.object(validator.media, "prepare_contexts", side_effect=lambda source, out: prepare("media_microservices", out)), \
                    mock.patch.object(validator.driver, "prepare_context", side_effect=lambda workload, source, rock, out: prepare(workload, out)):
                with self.assertRaisesRegex(validator.PreparationError, "not deterministic"):
                    validator.validate(source, rock, root / "out")
            self.assertFalse((root / "out").exists())

    def test_required_ci_enforces_real_source_and_interpreter_tests_without_publish(self):
        root = Path(__file__).resolve().parents[1]
        text = (root / ".github/workflows/ci.yml").read_text()
        workflow = yaml.safe_load(text)
        job = workflow["jobs"]["candidate-artifacts"]
        self.assertIn("candidate-artifacts", workflow["jobs"]["required"]["needs"])
        self.assertEqual(workflow["permissions"], {"contents": "read"})
        self.assertIn("ARTIFACT_RESULT: ${{ needs.candidate-artifacts.result }}", text)
        self.assertIn("DSB_REQUIRE_LUA: \"1\"", text)
        self.assertIn(validator.UPSTREAM_REVISION, text)
        self.assertIn("validate_deathstarbench_candidate_artifacts.py", text)
        self.assertIn("--network=none", text)
        self.assertIn("--read-only", text)
        self.assertIn("GOPROXY=off", text)
        self.assertNotIn("packages: write", text)
        self.assertNotIn("secrets.", text)
        self.assertGreaterEqual(job["timeout-minutes"], 10)
        upload = next(step for step in job["steps"] if step.get("uses", "").startswith("actions/upload-artifact@"))
        self.assertIs(upload["with"]["include-hidden-files"], True)
        self.assertIn("context-manifest.json", upload["with"]["path"])


if __name__ == "__main__":
    unittest.main()
