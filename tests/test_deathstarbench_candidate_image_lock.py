"""Candidate locks verify bytes/receipts and never open the public release gate."""

import copy
import dataclasses
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import create_deathstarbench_candidate_image_lock as lock
from scripts import inspect_deathstarbench_support_images as support


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data.encode() if isinstance(data, str) else data)
    path.chmod(0o644)
    return path


def marker(document):
    values = {
        "schema": "candidate-driver-v1", "measurement_qualified": "0", "workload": document["workload_id"],
        "architecture": "x86_64", "platform": "linux/amd64", "revision": document["load_driver_revision"],
        "upstream_revision": lock.UPSTREAM_REVISION, "context_sha256": document["context_sha256"],
        "wrk_binary_sha256": "b" * 64, "wrk2_source_sha256": document["wrk2_source_sha256"],
        "luajit_revision": lock.social_driver.LUAJIT_REVISION,
        "luasocket_source_sha256": lock.social_driver.LUASOCKET_SOURCE_SHA256,
        "original_request_script_sha256": document["original_request_script_sha256"],
        "request_script_sha256": document["prepared_request_script_sha256"],
        "metrics_script_sha256": document["metrics_script_sha256"], "entrypoint_sha256": document["entrypoint_sha256"],
        "seed_base": str(lock.driver.SEED_BASE),
    }
    return "CANDIDATE_DSB_LOAD_DRIVER_ARTIFACT " + " ".join(name + "=" + values[name] for name in lock.ATTESTATION_FIELDS)


class RegistryFixture:
    def __init__(self):
        self.manifests = {}
        self.blobs = {}

    def image(self, repository, platforms):
        descriptors, images, configs, native_indices = [], {}, {}, {}
        for platform in platforms:
            raw_config = encoded({"os": "linux", "architecture": platform.split("/")[1], "config": {"Labels": {"fixture": repository}}})
            config_digest = digest(raw_config)
            self.blobs[(repository, config_digest)] = raw_config
            manifest = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": len(raw_config)},
                        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": "sha256:" + "c" * 64, "size": 123}]}
            raw_manifest = encoded(manifest)
            image_digest = digest(raw_manifest)
            image = repository + "@" + image_digest
            self.manifests[image] = raw_manifest
            images[platform], configs[platform] = image, config_digest
            descriptor = {"mediaType": manifest["mediaType"], "digest": image_digest, "size": len(raw_manifest),
                          "platform": {"os": "linux", "architecture": platform.split("/")[1]}}
            descriptors.append(descriptor)
            native_index = encoded({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": [descriptor]})
            native_reference = repository + "@" + digest(native_index)
            self.manifests[native_reference] = native_index
            native_indices[platform] = native_reference
        raw_index = encoded({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": descriptors})
        index = repository + "@" + digest(raw_index)
        self.manifests[index] = raw_index
        return index, images, configs, native_indices

    def inspect(self, reference):
        return self.manifests[reference]

    def blob(self, repository, digest):
        return self.blobs[(repository, digest)]


class ImageBytesTests(unittest.TestCase):
    def setUp(self):
        self.registry = RegistryFixture()
        self.index, self.images, self.configs, _ = self.registry.image("ghcr.io/example/hotel", lock.PLATFORMS)

    def verify(self, index=None, images=None, **kwargs):
        return lock.verify_image_index(index or self.index, images or self.images,
                                       inspector=self.registry.inspect, blob_inspector=self.registry.blob, **kwargs)

    def test_original_bytes_and_platform_configs_verified(self):
        result = self.verify()
        for platform in lock.PLATFORMS:
            self.assertEqual(result["platforms"][platform]["config_digest"], self.configs[platform])
            self.assertEqual(result["platforms"][platform]["architecture"], platform.split("/")[1])

    def test_tampered_registry_index_is_rejected(self):
        self.registry.manifests[self.index] += b"\n"
        with self.assertRaisesRegex(lock.CandidateImageLockError, "raw byte digest"):
            self.verify()

    def test_tampered_platform_bytes_are_rejected(self):
        self.registry.manifests[self.images["linux/amd64"]] += b" "
        with self.assertRaisesRegex(lock.CandidateImageLockError, "raw byte digest"):
            self.verify()

    def test_tampered_config_bytes_are_rejected(self):
        repository = self.index.split("@")[0]
        self.registry.blobs[(repository, self.configs["linux/arm64"])] += b" "
        with self.assertRaisesRegex(lock.CandidateImageLockError, "raw byte digest"):
            self.verify()

    def test_crossed_platform_image_is_rejected(self):
        images = dict(self.images)
        images["linux/amd64"] = images["linux/arm64"]
        with self.assertRaisesRegex(lock.CandidateImageLockError, "selected manifest"):
            self.verify(images=images)

    def test_unexpected_platform_fails_custom_but_support_can_select_amd64(self):
        with self.assertRaisesRegex(lock.CandidateImageLockError, "runtime platform"):
            self.verify(images={"linux/amd64": self.images["linux/amd64"]})
        result = self.verify(images={"linux/amd64": self.images["linux/amd64"]}, allow_other_platforms=True)
        self.assertEqual(set(result["platforms"]), {"linux/amd64"})

    def _replace_index(self, document):
        raw = encoded(document)
        reference = self.index.split("@")[0] + "@" + digest(raw)
        self.registry.manifests[reference] = raw
        return reference

    def test_duplicate_and_missing_platforms_rejected(self):
        for mutation in (lambda values: values.append(values[0]), lambda values: values.pop()):
            with self.subTest(mutation=mutation):
                document = json.loads(self.registry.manifests[self.index])
                mutation(document["manifests"])
                with self.assertRaises(lock.CandidateImageLockError):
                    self.verify(index=self._replace_index(document))

    def test_descriptor_size_must_match_original_manifest(self):
        document = json.loads(self.registry.manifests[self.index])
        document["manifests"][0]["size"] += 1
        with self.assertRaisesRegex(lock.CandidateImageLockError, "descriptor size"):
            self.verify(index=self._replace_index(document))

    def test_wrong_config_architecture_fails_even_when_all_digests_match(self):
        repository = self.index.split("@")[0]
        config = encoded({"architecture": "arm64", "os": "linux"})
        config_digest = digest(config)
        self.registry.blobs[(repository, config_digest)] = config
        image = json.loads(self.registry.manifests[self.images["linux/amd64"]])
        image["config"].update(digest=config_digest, size=len(config))
        raw = encoded(image)
        image_ref = repository + "@" + digest(raw)
        self.registry.manifests[image_ref] = raw
        index = json.loads(self.registry.manifests[self.index])
        index["manifests"][0].update(digest=digest(raw), size=len(raw))
        with self.assertRaisesRegex(lock.CandidateImageLockError, "config architecture"):
            self.verify(index=self._replace_index(index), images={**self.images, "linux/amd64": image_ref})

    def test_unknown_platform_requires_explicit_buildkit_attestation_type(self):
        document = json.loads(self.registry.manifests[self.index])
        descriptor = copy.deepcopy(document["manifests"][0])
        descriptor["platform"] = {"os": "unknown", "architecture": "unknown"}
        descriptor["annotations"] = {"vnd.docker.reference.type": "attestation-manifest"}
        document["manifests"].append(descriptor)
        self.verify(index=self._replace_index(document))
        del descriptor["annotations"]
        with self.assertRaises(lock.CandidateImageLockError):
            self.verify(index=self._replace_index(document))

    def test_digest_placeholders_and_tagged_immutable_refs_fail(self):
        for reference in ("ghcr.io/example/hotel@sha256:" + "0" * 64,
                          "registry.invalid/hotel@sha256:" + "a" * 64,
                          "ghcr.io/example/hotel:latest@sha256:" + "a" * 64,
                          "ghcr.io/example/hotel:latest"):
            with self.subTest(reference=reference), self.assertRaises(lock.CandidateImageLockError):
                self.verify(index=reference)

    def test_duplicate_json_keys_non_json_numbers_and_nested_bool_schema_rejected(self):
        for raw in (b'{"schemaVersion":1,"schemaVersion":2}', b'{"size":NaN}', b'{"size":Infinity}'):
            with self.subTest(raw=raw), self.assertRaisesRegex(lock.CandidateImageLockError, "JSON"):
                lock._json_bytes(raw, "fixture")
        with self.assertRaisesRegex(lock.CandidateImageLockError, "nested"):
            lock._equal({"schema_version": True}, {"schema_version": 1}, "nested schema")


class CandidateLockTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.workload_path, self.driver_path = root / "workload", root / "driver"
        write(self.workload_path / "context-manifest.json", "{\"workload\":\"fixture\"}\n")
        write(self.driver_path / "context-manifest.json", "{\"driver\":\"fixture\"}\n")
        self.registry = RegistryFixture()

    def fixture(self, workload_id="hotel_reservation"):
        from scripts import smoke_deathstarbench_candidate_images as smoke_tool
        profile = lock.get_workload_profile(workload_id)
        identities = {name: {"context_sha256": "d" * 64, "recipe_sha256": "e" * 64,
                             "deathstarbench_license_sha256": "f" * 64} for name in lock.CUSTOM_KEYS[workload_id]}
        driver_identity = {"context_sha256": "1" * 64, "recipe_sha256": "2" * 64, "licenses_sha256": {"fixture": "3" * 64}}
        driver_document = {"workload_id": workload_id, "context_sha256": driver_identity["context_sha256"],
                           "load_driver_revision": profile.load_driver_revision, "wrk2_source_sha256": "4" * 64,
                           "original_request_script_sha256": profile.request_script_sha256,
                           "prepared_request_script_sha256": "5" * 64, "metrics_script_sha256": "6" * 64,
                           "entrypoint_sha256": "7" * 64}
        driver_context = self.driver_path / "context"
        for name, field in (("request.lua", "prepared_request_script_sha256"), ("metrics.lua", "metrics_script_sha256"), ("entrypoint", "entrypoint_sha256")):
            write(driver_context / name, "fixture " + name + "\n")
            driver_document[field] = lock.sha256(driver_context / name)
        for name in ("DeathStarBench-LICENSE", "LuaSocket-LICENSE", "LuaJIT-COPYRIGHT", "wrk2-LICENSE"):
            write(driver_context / "licenses" / name, "fixture license " + name + "\n")
        if workload_id == "hotel_reservation":
            for path in ("app/config.json", "app/LICENSE.deathstarbench", "app/licenses/vendor/fixture/LICENSE"):
                write(self.workload_path / path, "fixture " + path + "\n")
        else:
            for path in ("app/config/service-config.json", "app/config/jaeger-config.yml", "app/LICENSE.deathstarbench", "app/third_party/PicoSHA2/LICENSE",
                         "frontend/LICENSE.deathstarbench", "frontend/COPYRIGHT", "frontend/lua-bridge-tracer/LICENSE", "frontend/runtime/nginx.conf",
                         "frontend/runtime/jaeger-config.json", "frontend/runtime/gen-lua/fixture.lua", "frontend/runtime/lua-scripts/fixture.lua"):
                write(self.workload_path / path, "fixture " + path + "\n")
        evidence = {"schema_version": 1, "candidate_schema": lock.SCHEMA, "workload_id": workload_id,
                    "upstream_revision": lock.UPSTREAM_REVISION, "workload_revision": profile.workload_revision,
                    "image_set_revision": profile.image_set_revision,
                    "workload_preparation_manifest_sha256": lock.sha256(self.workload_path / "context-manifest.json"),
                    "driver_preparation_manifest_sha256": lock.sha256(self.driver_path / "context-manifest.json"),
                    "custom_images": {}, "load_driver": None, "support_images": {}}
        for context_name in (*identities, "load_driver"):
            identity = driver_identity if context_name == "load_driver" else identities[context_name]
            runtime_key = "load-driver" if context_name == "load_driver" else lock.CUSTOM_KEYS[workload_id][context_name]
            platforms = ("linux/amd64",) if context_name == "load_driver" else lock.PLATFORMS
            index, images, configs, native = self.registry.image(f"ghcr.io/example/{workload_id}-{context_name}", platforms)
            image_evidence = {"index_image": index, "platforms": {}}
            for platform in platforms:
                build = {"schema_version": 1, "workload_id": workload_id, "context_name": context_name,
                         "platform": platform, "context_sha256": identity["context_sha256"], "recipe_sha256": identity["recipe_sha256"],
                         "runner_architecture": lock.ARCHITECTURES[platform], "qemu_used": False, "published_index_image": native[platform]}
                context = driver_context if context_name == "load_driver" else self.workload_path / context_name
                baked = {path: lock.sha256(source) for path, source in smoke_tool._baked_files(workload_id, runtime_key, context).items()}
                if runtime_key == "hotel-reservation":
                    baked["/usr/share/licenses/go/LICENSE"] = "a" * 64
                required = (["/go/bin/" + name for name in smoke_tool.HOTEL_PROGRAMS] if runtime_key == "hotel-reservation" else
                            ["/usr/local/bin/" + name for name in smoke_tool.MEDIA_PROGRAMS] if runtime_key == "media-microservices" else
                            ["/usr/local/openresty/nginx/sbin/nginx"] if runtime_key == "nginx-web-server" else
                            ["/opt/deathstarbench-candidate-driver/bin/wrk"])
                elves = {path: {"sha256": "b" * 64, "elf_class": 64, "elf_type": 2, "machine": 62 if platform == "linux/amd64" else 183,
                                "architecture": lock.ARCHITECTURES[platform], "interpreter": None, "dynamic_dependencies": False, "runnable": True, "static": True}
                         for path in required}
                methods = ["stopped-container-export-no-host-extraction", "all-ELF64-native-architecture", "baked-config-script-license-hashes", "runtime-clone-and-private-key-screen"]
                if runtime_key == "hotel-reservation":
                    methods.append("scratch-static-linkage-no-service-startup")
                elif runtime_key == "nginx-web-server":
                    methods.extend(["baked-nginx-config-syntax-with-explicit-mock-DNS", "baked-Lua-Thrift-module-loading-no-service-methods"])
                elif runtime_key == "load-driver":
                    methods.append("candidate-driver-offline-hardened-attestation")
                attestation = marker(driver_document) if context_name == "load_driver" else None
                executions = [] if attestation is None else [{"command": ["/opt/deathstarbench-candidate-driver/bin/entrypoint", "attest"],
                                                              "stdout": attestation, "stdout_sha256": hashlib.sha256(attestation.encode()).hexdigest(), "mock_dns": {}}]
                smoke = {"schema_version": 1, "candidate_schema": "candidate-image-smoke-v1", "workload_id": workload_id,
                         "image_key": runtime_key, "image": images[platform], "platform": platform, "architecture": lock.ARCHITECTURES[platform],
                         "config_digest": configs[platform], "context_sha256": identity["context_sha256"], "methods": methods,
                         "candidate_only": True, "released": False, "runtime_semantics_qualified": False,
                         "smoke_execution": {"passed": True, "network": "none", "read_only": True, "user": "65532:65532",
                                             "drop_capabilities": ["ALL"], "no_new_privileges": True},
                         "native_builder": {"platform": platform, "architecture": lock.ARCHITECTURES[platform], "qemu_used": False},
                         "image_config": {"os": "linux", "architecture": platform.split("/")[1], "config_digest": configs[platform]},
                         "load_driver_attestation": attestation, "inspection_executions": executions,
                         "artifact_identities": {"baked_file_sha256": baked, "elf_artifacts": elves, "required_executables": required,
                                                 "runtime_clone_detected": False, "private_key_detected": False}}
                image_evidence["platforms"][platform] = {"image": images[platform], "build_receipt": build, "smoke_receipt": smoke}
            if context_name == "load_driver":
                evidence["load_driver"] = image_evidence
            else:
                evidence["custom_images"][context_name] = image_evidence
        for key, requested in support.SUPPORT_IMAGES[workload_id].items():
            repository = requested.rsplit(":", 1)[0]
            index, images, _, _ = self.registry.image(repository, lock.PLATFORMS)
            self.registry.manifests[requested] = self.registry.manifests[index]
            evidence["support_images"][key] = {"requested_image": requested, "index_image": index,
                                               "platforms": {"linux/amd64": {"image": images["linux/amd64"]}}}
        return evidence, identities, driver_document, driver_identity

    def create(self, evidence, identities, driver_document, driver_identity):
        with mock.patch.object(lock, "_workload_preparation", return_value=({"prepared": True}, identities)), \
             mock.patch.object(lock, "_driver_preparation", return_value=(driver_document, driver_identity)):
            return lock.create_candidate_lock(evidence["workload_id"], self.workload_path, self.driver_path, evidence,
                                              inspector=self.registry.inspect, blob_inspector=self.registry.blob)

    def native_records(self, evidence):
        records = []
        for platform in lock.PLATFORMS:
            images = {key: copy.deepcopy(value["platforms"][platform]) for key, value in evidence["custom_images"].items()}
            if platform == "linux/amd64":
                images["load_driver"] = copy.deepcopy(evidence["load_driver"]["platforms"][platform])
            records.append({"schema_version": 1, "candidate_schema": "candidate-native-publication-v1", "workload_id": evidence["workload_id"],
                            "platform": platform, "images": images,
                            "workload_preparation_manifest_sha256": evidence["workload_preparation_manifest_sha256"],
                            "driver_preparation_manifest_sha256": evidence["driver_preparation_manifest_sha256"]})
        return records

    def preflight(self, evidence, inputs, records=None):
        identities, driver_document, driver_identity = inputs
        with mock.patch.object(lock, "_workload_preparation", return_value=({"prepared": True}, identities)), \
             mock.patch.object(lock, "_driver_preparation", return_value=(driver_document, driver_identity)):
            return lock.preflight_native_candidate(evidence["workload_id"], self.workload_path, self.driver_path,
                                                    records or self.native_records(evidence),
                                                    inspector=self.registry.inspect, blob_inspector=self.registry.blob)

    def test_both_workloads_lock_native_images_and_driver_without_promotion(self):
        for workload in lock.CUSTOM_KEYS:
            with self.subTest(workload=workload):
                evidence, *inputs = self.fixture(workload)
                result = self.create(evidence, *inputs)
                self.assertEqual(result["candidate_schema"], "candidate-artifact-lock-v1")
                for key in ("released", "runtime_qualified", "measurement_qualified", "anonymous_pull_qualified"):
                    self.assertIs(result[key], False)
                self.assertEqual(set(result["images"]), set(lock.get_workload_profile(workload).required_image_keys) | {"load-driver"})
                self.assertEqual(result["images"]["load-driver"]["platforms"]["linux/amd64"]["driver_attestation"]["wrk_binary_sha256"], "b" * 64)
                self.assertFalse(result["qualification"]["anonymous_pull"]["qualified"])

    def test_crossed_workload_revision_manifest_or_unknown_fields_rejected(self):
        mutations = {"workload_revision": "media-microservices-6ecb097-workload-v1", "upstream_revision": "0" * 40,
                     "image_set_revision": "hotel-reservation-images-v2", "workload_preparation_manifest_sha256": "a" * 64,
                     "driver_preparation_manifest_sha256": "b" * 64, "schema_version": True, "released": True}
        for field, value in mutations.items():
            evidence, *inputs = self.fixture()
            evidence[field] = value
            with self.subTest(field=field), self.assertRaises(lock.CandidateImageLockError):
                self.create(evidence, *inputs)

    def test_crossed_receipts_or_qemu_rejected(self):
        mutations = {"workload_id": "media_microservices", "context_sha256": "0" * 64, "recipe_sha256": "0" * 64,
                     "runner_architecture": "aarch64", "qemu_used": True, "platform": "linux/arm64"}
        for field, value in mutations.items():
            evidence, *inputs = self.fixture()
            evidence["custom_images"]["app"]["platforms"]["linux/amd64"]["build_receipt"][field] = value
            with self.subTest(field=field), self.assertRaises(lock.CandidateImageLockError):
                self.create(evidence, *inputs)

    def test_smoke_must_be_bound_hardened_and_semantically_unqualified(self):
        mutations = {"runtime_semantics_qualified": True, "image_key": "media-microservices", "config_digest": "sha256:" + "0" * 64,
                     "image": "ghcr.io/example/unrelated@sha256:" + "a" * 64, "candidate_only": False, "context_sha256": "b" * 64}
        for field, value in mutations.items():
            evidence, *inputs = self.fixture()
            evidence["custom_images"]["app"]["platforms"]["linux/amd64"]["smoke_receipt"][field] = value
            with self.subTest(field=field), self.assertRaises(lock.CandidateImageLockError):
                self.create(evidence, *inputs)
        evidence, *inputs = self.fixture()
        evidence["load_driver"]["platforms"]["linux/amd64"]["smoke_receipt"]["smoke_execution"]["network"] = "host"
        with self.assertRaisesRegex(lock.CandidateImageLockError, "smoke execution network"):
            self.create(evidence, *inputs)

    def test_load_driver_attestation_rejects_other_workload_and_any_tampering(self):
        evidence, *inputs = self.fixture()
        receipt = evidence["load_driver"]["platforms"]["linux/amd64"]["smoke_receipt"]
        good = receipt["load_driver_attestation"]
        for changed in (good.replace("workload=hotel_reservation", "workload=media_microservices"),
                        good.replace("measurement_qualified=0", "measurement_qualified=1"),
                        good.replace("CANDIDATE_DSB", "DISTRIBUTED_DSB"), good + " extra=value", good + "\n" + good,
                        good.replace("wrk_binary_sha256=" + "b" * 64, "wrk_binary_sha256=" + "0" * 64)):
            with self.subTest(attestation=changed), self.assertRaises(lock.CandidateImageLockError):
                receipt["load_driver_attestation"] = changed
                self.create(evidence, *inputs)

    def test_support_versions_and_fixed_x86_roles_are_exact(self):
        evidence, *inputs = self.fixture()
        evidence["support_images"]["mongodb"]["requested_image"] = "docker.io/library/mongo:4.4.6"
        with self.assertRaisesRegex(lock.CandidateImageLockError, "candidate source"):
            self.create(evidence, *inputs)
        evidence, *inputs = self.fixture()
        evidence["support_images"]["mongodb"]["platforms"]["linux/arm64"] = {"image": "not-a-reference"}
        with self.assertRaisesRegex(lock.CandidateImageLockError, "platforms"):
            self.create(evidence, *inputs)

    def test_mutable_support_source_cannot_retarget_index(self):
        evidence, *inputs = self.fixture()
        source = evidence["support_images"]["mongodb"]["requested_image"]
        self.registry.manifests[source] += b" "
        with self.assertRaisesRegex(lock.CandidateImageLockError, "raw byte digest"):
            self.create(evidence, *inputs)

    def test_native_index_must_retain_exact_built_platform_manifest(self):
        evidence, *inputs = self.fixture()
        arm_build = evidence["custom_images"]["app"]["platforms"]["linux/arm64"]["build_receipt"]
        arm_build["published_index_image"] = evidence["custom_images"]["app"]["platforms"]["linux/amd64"]["build_receipt"]["published_index_image"]
        with self.assertRaisesRegex(lock.CandidateImageLockError, "runtime platform"):
            self.create(evidence, *inputs)

    def test_full_readonly_preflight_checks_both_original_native_records(self):
        for workload in lock.CUSTOM_KEYS:
            with self.subTest(workload=workload):
                evidence, *inputs = self.fixture(workload)
                result = self.preflight(evidence, inputs)
                self.assertEqual(set(result), set(lock.PLATFORMS))
                self.assertIn("load_driver", result["linux/amd64"])
                self.assertNotIn("load_driver", result["linux/arm64"])

    def test_preflight_rejects_hash_mismatch_duplicate_platform_and_arm_driver(self):
        for case in ("source_hash", "duplicate", "arm_driver"):
            evidence, *inputs = self.fixture()
            records = self.native_records(evidence)
            if case == "source_hash":
                records[0]["workload_preparation_manifest_sha256"] = "a" * 64
            elif case == "duplicate":
                records[1]["platform"] = "linux/amd64"
            else:
                records[1]["images"]["load_driver"] = records[0]["images"]["load_driver"]
            with self.subTest(case=case), self.assertRaises(lock.CandidateImageLockError):
                self.preflight(evidence, inputs, records)

    def test_preflight_requires_native_elf_baked_files_and_all_executables(self):
        for case in ("elf_arch", "baked_sha", "missing_binary", "private_key", "clone", "unknown_file"):
            evidence, *inputs = self.fixture()
            records = self.native_records(evidence)
            artifacts = records[0]["images"]["app"]["smoke_receipt"]["artifact_identities"]
            if case == "elf_arch":
                artifacts["elf_artifacts"]["/go/bin/frontend"]["machine"] = 183
            elif case == "baked_sha":
                artifacts["baked_file_sha256"]["/workspace/config.json"] = "a" * 64
            elif case == "missing_binary":
                del artifacts["elf_artifacts"]["/go/bin/frontend"]
            elif case == "private_key":
                artifacts["private_key_detected"] = True
            elif case == "clone":
                artifacts["runtime_clone_detected"] = True
            else:
                artifacts["baked_file_sha256"]["/unexpected/runtime-file"] = "a" * 64
            with self.subTest(case=case), self.assertRaises(lock.CandidateImageLockError):
                self.preflight(evidence, inputs, records)

    def test_driver_marker_binary_identity_and_execution_receipt_must_reconcile(self):
        evidence, *inputs = self.fixture()
        records = self.native_records(evidence)
        receipt = records[0]["images"]["load_driver"]["smoke_receipt"]
        changed = receipt["load_driver_attestation"].replace("wrk_binary_sha256=" + "b" * 64, "wrk_binary_sha256=" + "c" * 64)
        receipt["load_driver_attestation"] = changed
        receipt["inspection_executions"][0].update(stdout=changed, stdout_sha256=hashlib.sha256(changed.encode()).hexdigest())
        with self.assertRaisesRegex(lock.CandidateImageLockError, "inspected binary hash"):
            self.preflight(evidence, inputs, records)
        receipt["inspection_executions"][0]["stdout_sha256"] = "a" * 64
        with self.assertRaisesRegex(lock.CandidateImageLockError, "stdout bytes"):
            self.preflight(evidence, inputs, records)


class LocalPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_tree_identity_includes_recipe_content_mode_and_ignored_extras(self):
        root = self.root / "context"
        path = write(root / "Dockerfile", "FROM scratch\n")
        identity = lock.tree_sha256(root)
        lock._tree_hash(self.root, "context", identity, "context")
        path.chmod(0o755)
        with self.assertRaisesRegex(lock.CandidateImageLockError, "tree identity"):
            lock._tree_hash(self.root, "context", identity, "context")
        path.chmod(0o644)
        write(root / "ignored-build-output", "unexpected\n")
        with self.assertRaises(lock.CandidateImageLockError):
            lock._tree_hash(self.root, "context", identity, "context")

    def test_missing_traversal_and_symlink_paths_rejected(self):
        write(self.root / "good", "value")
        (self.root / "alias").symlink_to(self.root / "good")
        for path in ("../good", "/tmp/good", "alias", "missing", "./good", ".git/config", ""):
            with self.subTest(path=path), self.assertRaises(lock.CandidateImageLockError):
                lock._file(self.root, path, "test")

    def test_foreign_preparation_or_promotion_flags_fail(self):
        common = {"schema_version": 1, "workload_id": "hotel_reservation", "candidate_only": True,
                  "released": False, "upstream_revision": lock.UPSTREAM_REVISION,
                  "upstream_repository": lock.UPSTREAM_REPOSITORY}
        lock._common_preparation(common, "hotel_reservation", "fixture")
        for field, value in (("workload_id", "media_microservices"), ("candidate_only", False),
                             ("released", True), ("schema_version", True), ("upstream_revision", "other")):
            with self.subTest(field=field), self.assertRaises(lock.CandidateImageLockError):
                lock._common_preparation({**common, field: value}, "hotel_reservation", "fixture")

    def test_real_hotel_preparer_manifest_revalidated_and_corrected_receipts_cannot_hide_drift(self):
        # Small source fixtures stand in for the pinned checkout, but preparation
        # and every generated-context/manifest validation below are real code.
        from tests import test_deathstarbench_hotel_images as fixture
        upstream = self.root / "upstream"
        fixture.source_fixture(upstream)
        inventory = [{"path": path.relative_to(upstream).as_posix(), "sha256": lock.sha256(path)}
                     for path in sorted(upstream.rglob("*")) if path.is_file()]
        anchors = {name: lock.sha256(upstream / name) for name in lock.hotel.UPSTREAM_ANCHORS
                   if (upstream / name).is_file()}
        availability_hash = hashlib.sha256(fixture.ORIGINAL_METHOD.encode()).hexdigest()
        with mock.patch.object(lock.hotel, "validate_tracked_source", return_value=upstream), \
             mock.patch.object(lock.hotel, "_inventory", return_value=inventory), \
             mock.patch.object(lock.hotel, "UPSTREAM_ANCHORS", anchors), \
             mock.patch.object(lock.hotel, "ORIGINAL_AVAILABILITY_SHA256", availability_hash):
            root = self.root / "hotel"
            document = lock.hotel.prepare_contexts(upstream, root)
            lock._workload_preparation(root, "hotel_reservation")
            # Updating a digest receipt is not enough: the original inventory,
            # approved generated recipe, and semantic fixture hashes still bind.
            original_recipe = (root / "app/Dockerfile.candidate").read_bytes()
            write(root / "app/Dockerfile.candidate", original_recipe + b"RUN false\n")
            document["contexts"]["app"]["context_sha256"] = lock.tree_sha256(root / "app")
            write(root / "context-manifest.json", encoded(document))
            with self.assertRaisesRegex(lock.CandidateImageLockError, "generated recipe"):
                lock._workload_preparation(root, "hotel_reservation")
            write(root / "app/Dockerfile.candidate", original_recipe)
            write(root / "app/vendor/modules.txt", "changed vendor identity\n")
            document["software"]["vendor_inventory_sha256"] = lock.tree_sha256(root / "app/vendor")
            document["contexts"]["app"]["context_sha256"] = lock.tree_sha256(root / "app")
            write(root / "context-manifest.json", encoded(document))
            with self.assertRaisesRegex(lock.CandidateImageLockError, "unchanged source"):
                lock._workload_preparation(root, "hotel_reservation")

    def test_real_driver_preparer_revalidated_and_metrics_recipe_original_hashes_bound(self):
        from tests import test_deathstarbench_candidate_load_driver as fixture
        for workload_id in lock.CUSTOM_KEYS:
            with self.subTest(workload=workload_id):
                case = self.root / workload_id
                case.mkdir()
                upstream, wrk, request, rock = fixture.fixture_inputs(case, workload_id)
                profile = dataclasses.replace(lock.get_workload_profile(workload_id), request_script_sha256=lock.sha256(request))
                socket_license = b"fixture source-rock license\n"
                source_hash = lock.social_driver._tree_sha256(wrk)
                inputs = (profile, wrk, request, lock.driver.prepare_request_script(workload_id, request.read_text()), socket_license, source_hash)
                with mock.patch.object(lock.driver, "_validate_inputs", return_value=inputs), \
                     mock.patch.object(lock, "get_workload_profile", return_value=profile), \
                     mock.patch.object(lock.social_driver, "LUASOCKET_SOURCE_SHA256", lock.sha256(rock)), \
                     mock.patch.object(lock.social_driver, "LUASOCKET_LICENSE_SHA256", hashlib.sha256(socket_license).hexdigest()), \
                     mock.patch.dict(lock.hotel.UPSTREAM_ANCHORS, {"LICENSE": lock.sha256(upstream / "LICENSE")}):
                    root = case / "driver"
                    document = lock.driver.prepare_context(workload_id, upstream, rock, root)
                    lock._driver_preparation(root, workload_id)
                    write(root / "context/metrics.lua", "-- changed measurement semantics\n")
                    document["metrics_script_sha256"] = lock.sha256(root / "context/metrics.lua")
                    document["context_sha256"] = lock.tree_sha256(root / "context")
                    write(root / "context-manifest.json", encoded(document))
                    with self.assertRaisesRegex(lock.CandidateImageLockError, "generated metrics"):
                        lock._driver_preparation(root, workload_id)

    def test_real_media_preparer_initializer_and_checksum_source_lock_are_bound(self):
        from tests import test_deathstarbench_media_images as fixture
        upstream = self.root / "upstream"
        upstream.mkdir()
        _, anchors = fixture.fixture(upstream)
        with mock.patch.object(lock.media, "validate_tracked_source", return_value=upstream), \
             mock.patch.object(lock.media, "UPSTREAM_ANCHORS", anchors), \
             mock.patch.dict(lock.hotel.UPSTREAM_ANCHORS, {"LICENSE": anchors["LICENSE"]}):
            root = self.root / "media"
            document = lock.media.prepare_contexts(upstream, root)
            lock._workload_preparation(root, "media_microservices")
            original_env = (root / "app/build/source.env").read_bytes()
            write(root / "app/build/source.env", original_env + b"MONGO_URL='https://example.invalid/unreviewed'\n")
            document["contexts"]["app"]["context_sha256"] = lock.tree_sha256(root / "app")
            document["contexts"]["app"]["build_inputs_sha256"] = lock.tree_sha256(root / "app/build")
            write(root / "context-manifest.json", encoded(document))
            with self.assertRaisesRegex(lock.CandidateImageLockError, "generated build environment"):
                lock._workload_preparation(root, "media_microservices")
            write(root / "app/build/source.env", original_env)
            document["contexts"]["app"]["context_sha256"] = lock.tree_sha256(root / "app")
            document["contexts"]["app"]["build_inputs_sha256"] = lock.tree_sha256(root / "app/build")
            write(root / "initializer/datasets/tmdb/movies.json", "[]\n")
            document["initializer"]["datasets"]["movies.json"] = lock.sha256(root / "initializer/datasets/tmdb/movies.json")
            document["initializer"]["context_sha256"] = lock.tree_sha256(root / "initializer")
            write(root / "context-manifest.json", encoded(document))
            with self.assertRaisesRegex(lock.CandidateImageLockError, "dataset movies.json source"):
                lock._workload_preparation(root, "media_microservices")


if __name__ == "__main__":
    unittest.main()
