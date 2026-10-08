import base64
import io
import json
import os
import struct
import tarfile
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

from scripts import deathstarbench_candidate_registry as registry
from scripts import inspect_deathstarbench_support_images as support
from scripts import smoke_deathstarbench_candidate_images as smoke


def encode(document):
    return json.dumps(document, separators=(",", ":")).encode()


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data if isinstance(data, bytes) else data.encode())


def elf(machine=62, *, dynamic=False, relocatable=False):
    header = bytearray(64)
    header[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<HH", header, 16, 1 if relocatable else 2, machine)
    if relocatable:
        return bytes(header)
    struct.pack_into("<Q", header, 32, 64)
    struct.pack_into("<HH", header, 54, 56, 1)
    if dynamic:
        return bytes(header) + struct.pack("<IIQQQQQQ", 2, 0, 120, 0, 0, 16, 0, 0) + struct.pack("<qQ", 1, 0)
    return bytes(header) + struct.pack("<IIQQQQQQ", 1, 0, 0, 0, 0, 120, 0, 0)


def archive(path, members):
    with tarfile.open(path, "w") as output:
        for name, value in members.items():
            info = tarfile.TarInfo(name)
            if isinstance(value, tuple):
                payload, info.mode = value
            else:
                payload, info.mode = value, 0o644
            if isinstance(payload, tarfile.TarInfo):
                payload.name = name
                output.addfile(payload)
            else:
                info.size = len(payload)
                output.addfile(info, io.BytesIO(payload))


def hotel_fixture(root):
    context = root / "context"
    write(context / "config.json", b'{"consulAddress":"consul:8500"}\n')
    write(context / "LICENSE.deathstarbench", b"Apache license fixture\n")
    write(context / "licenses/vendor/example/LICENSE", b"vendor license fixture\n")
    files = {path.lstrip("/"): original.read_bytes()
             for path, original in smoke._baked_files("hotel_reservation", "hotel-reservation", context).items()}
    files["usr/share/licenses/go/LICENSE"] = b"Copyright Go authors\n" + b"redistribution terms\n" * 12
    files.update({"go/bin/" + name: (elf(), 0o755) for name in smoke.HOTEL_PROGRAMS})
    return context, files


def media_fixture(root, key, *, machine=62):
    context = root / "context"
    for path in ("LICENSE.deathstarbench", "third_party/PicoSHA2/LICENSE", "config/service-config.json", "config/jaeger-config.yml",
                 "COPYRIGHT", "lua-bridge-tracer/LICENSE", "runtime/nginx.conf", "runtime/jaeger-config.json", "lua-json/json.lua",
                 "lua-thrift/Thrift.lua", "runtime/gen-lua/media_service_ttypes.lua", "runtime/lua-scripts/example.lua"):
        write(context / path, "candidate context fixture " + path + "\n")
    files = {path.lstrip("/"): original.read_bytes()
             for path, original in smoke._baked_files("media_microservices", key, context).items()}
    programs = ["usr/local/bin/" + name for name in smoke.MEDIA_PROGRAMS] if key == "media-microservices" else ["usr/local/openresty/nginx/sbin/nginx"]
    files.update({name: (elf(machine), 0o755) for name in programs})
    files.update({"usr/share/licenses/deathstarbench/dependencies/" + name + "/LICENSE": b"Copyright upstream; redistribution under this license.\n"
                  for name in smoke.MEDIA_DEPENDENCY_LICENSES[key]})
    return context, files


class RegistryBytesTests(unittest.TestCase):
    def test_exact_platform_resolution_checks_manifest_and_config_bytes(self):
        cfg = encode({"os": "linux", "architecture": "amd64", "config": {"Env": []}})
        cfg_digest = registry.digest_bytes(cfg)
        manifest = encode({"schemaVersion": 2, "config": {"digest": cfg_digest, "size": len(cfg)}, "layers": []})
        manifest_digest = registry.digest_bytes(manifest)
        index = encode({"schemaVersion": 2, "manifests": [{"digest": manifest_digest, "size": len(manifest), "platform": {"os": "linux", "architecture": "amd64"}}]})
        reference = "ghcr.io/example/candidate@" + registry.digest_bytes(index)
        def request(_registry, _repository, kind, selector):
            raw = cfg if kind == "blobs" else index if selector == registry.digest_bytes(index) else manifest
            return raw, {"docker-content-digest": registry.digest_bytes(raw)}
        with mock.patch.object(registry, "_request", side_effect=request):
            resolved = registry.resolve_platform(reference, "linux/amd64")
            self.assertEqual(resolved["image"], "ghcr.io/example/candidate@" + manifest_digest)
            self.assertEqual(resolved["index_image"], reference)
            self.assertEqual(resolved["config_digest"], cfg_digest)
            self.assertEqual(registry.get_platform_image(reference, "linux/amd64"), resolved["image"])

    def test_manifest_and_blob_byte_mismatch_are_rejected(self):
        raw = b'{}'
        with mock.patch.object(registry, "_request", return_value=(raw, {"docker-content-digest": "sha256:" + "a" * 64})):
            with self.assertRaises(smoke.PreparationError):
                registry.inspect_manifest("ghcr.io/example/candidate@sha256:" + "a" * 64)
            with self.assertRaises(smoke.PreparationError):
                registry.inspect_blob("ghcr.io/example/candidate", "sha256:" + "a" * 64)

    def test_wrong_actual_config_architecture_is_rejected_even_if_descriptor_matches(self):
        cfg = encode({"os": "linux", "architecture": "arm64"})
        manifest = encode({"schemaVersion": 2, "config": {"digest": registry.digest_bytes(cfg), "size": len(cfg)}, "layers": []})
        with mock.patch.object(registry, "inspect_manifest", return_value=manifest), mock.patch.object(registry, "inspect_blob", return_value=cfg):
            with self.assertRaisesRegex(smoke.PreparationError, "architecture"):
                registry.resolve_platform("ghcr.io/example/candidate:fixed", "linux/amd64")

    def test_duplicate_platform_descriptors_and_descriptor_size_drift_fail(self):
        descriptor = {"digest": "sha256:" + "a" * 64, "size": 123, "platform": {"os": "linux", "architecture": "amd64"}}
        for descriptors in ([descriptor, descriptor], [descriptor]):
            with self.subTest(descriptors=descriptors), mock.patch.object(registry, "inspect_manifest", side_effect=[encode({"schemaVersion": 2, "manifests": descriptors}), b'{}']):
                with self.assertRaises(smoke.PreparationError):
                    registry.resolve_platform("ghcr.io/example/candidate:fixed", "linux/amd64")

    def test_invalid_or_implicit_image_references_are_rejected(self):
        for reference in ("mongo", "docker.io/library/mongo", "https://ghcr.io/a/b:tag", "ghcr.io/../bad:tag", "unknown.invalid/a:tag", "ghcr.io/a/b@sha256:ABC", "ghcr.io/a/b:tag\n"):
            with self.subTest(reference=reference), self.assertRaises(smoke.PreparationError):
                registry.parse_reference(reference)

    def test_github_token_basic_auth_is_only_at_validated_scoped_ghcr_endpoint(self):
        calls = []
        challenge = {"WWW-Authenticate": 'Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:example/candidate:pull"'}
        def opened(url, headers):
            calls.append((url, dict(headers)))
            if len(calls) == 1:
                raise urllib.error.HTTPError(url, 401, "auth", challenge, None)
            if len(calls) == 2:
                return b'{"token":"short-lived-registry-jwt"}', {}
            return b"manifest", {}
        with mock.patch.dict(os.environ, {"REGISTRY_USERNAME": "operator", "REGISTRY_TOKEN": "github-token-fixture"}), mock.patch.object(registry, "_open", side_effect=opened):
            registry._request("ghcr.io", "example/candidate", "manifests", "fixed")
        self.assertNotIn("Authorization", calls[0][1])
        self.assertTrue(calls[1][0].startswith("https://ghcr.io/token?"))
        self.assertIn("scope=repository%3Aexample%2Fcandidate%3Apull", calls[1][0])
        self.assertEqual(calls[1][1]["Authorization"], "Basic " + base64.b64encode(b"operator:github-token-fixture").decode())
        self.assertEqual(calls[2][1]["Authorization"], "Bearer short-lived-registry-jwt")
        self.assertNotIn("github-token-fixture", calls[0][0] + calls[1][0] + calls[2][0])

    def test_anonymous_access_never_authenticates_token_endpoint_and_rejects_bad_realm(self):
        for realm in ("https://ghcr.io/token", "https://attacker.invalid/token"):
            calls = []
            def opened(url, headers):
                calls.append((url, dict(headers)))
                if len(calls) == 1:
                    raise urllib.error.HTTPError(url, 401, "auth", {"WWW-Authenticate": f'Bearer realm="{realm}",service="ghcr.io"'}, None)
                if len(calls) == 2:
                    return b'{"token":"public-jwt"}', {}
                return b"manifest", {}
            with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(registry, "_open", side_effect=opened):
                if "attacker" in realm:
                    with self.assertRaises(smoke.PreparationError):
                        registry._request("ghcr.io", "example/candidate", "manifests", "fixed")
                    self.assertEqual(len(calls), 1)
                else:
                    registry._request("ghcr.io", "example/candidate", "manifests", "fixed")
                    self.assertNotIn("Authorization", calls[1][1])

    def test_redirects_strip_all_credentials_and_refuse_http(self):
        request = urllib.request.Request("https://ghcr.io/token", headers={"Authorization": "Basic fixture"})
        redirected = registry._SafeRedirect().redirect_request(request, None, 302, "found", {}, "https://ghcr.io/token2")
        self.assertIsNone(redirected.get_header("Authorization"))
        with self.assertRaises(smoke.PreparationError):
            registry._SafeRedirect().redirect_request(request, None, 302, "found", {}, "http://ghcr.io/token")


class ImageFilesystemTests(unittest.TestCase):
    def test_virtual_image_symlinks_are_read_without_host_extraction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            link = tarfile.TarInfo("link")
            link.type, link.linkname = tarfile.SYMTYPE, "/config"
            archive(root / "image.tar", {"config": b"baked bytes", "link": link})
            fs = smoke.ImageFilesystem(root / "image.tar")
            self.addCleanup(fs.close)
            self.assertEqual(fs.read("/link"), b"baked bytes")
            self.assertFalse((root / "config").exists())

    def test_usrmerge_directory_links_and_hardlinks_resolve_in_virtual_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            usrmerge = tarfile.TarInfo("lib64")
            usrmerge.type, usrmerge.linkname = tarfile.SYMTYPE, "usr/lib64"
            loader = tarfile.TarInfo("usr/lib64/ld.so")
            loader.type, loader.linkname = tarfile.SYMTYPE, "../lib/loader.so"
            hardlink = tarfile.TarInfo("hard")
            hardlink.type, hardlink.linkname = tarfile.LNKTYPE, "usr/lib/loader.so"
            archive(root / "image.tar", {"lib64": usrmerge, "usr/lib64/ld.so": loader,
                                         "usr/lib/loader.so": b"native loader", "hard": hardlink})
            fs = smoke.ImageFilesystem(root / "image.tar")
            self.addCleanup(fs.close)
            self.assertEqual(fs.read("/lib64/ld.so"), b"native loader")
            self.assertEqual(fs.read("/hard"), b"native loader")

    def test_cyclic_directory_link_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            link = tarfile.TarInfo("loop")
            link.type, link.linkname = tarfile.SYMTYPE, "loop"
            archive(root / "image.tar", {"loop": link})
            fs = smoke.ImageFilesystem(root / "image.tar")
            self.addCleanup(fs.close)
            with self.assertRaisesRegex(smoke.PreparationError, "Cyclic"):
                fs.read("/loop/file")

    def test_archive_traversal_and_escaping_symlinks_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive(root / "traversal.tar", {"../outside": b"unsafe"})
            with self.assertRaises(smoke.PreparationError):
                smoke.ImageFilesystem(root / "traversal.tar")
            link = tarfile.TarInfo("link")
            link.type, link.linkname = tarfile.SYMTYPE, "../outside"
            archive(root / "link.tar", {"link": link})
            fs = smoke.ImageFilesystem(root / "link.tar")
            self.addCleanup(fs.close)
            with self.assertRaises(smoke.PreparationError):
                fs.read("link")

    def test_all_required_hotel_programs_config_and_licenses_are_inspected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context, files = hotel_fixture(root)
            archive(root / "image.tar", files)
            fs = smoke.ImageFilesystem(root / "image.tar")
            self.addCleanup(fs.close)
            identities = smoke._inspect_filesystem(fs, "hotel_reservation", "hotel-reservation", "x86_64", context, {"Cmd": ["/go/bin/frontend"]})
            self.assertEqual(len(identities["required_executables"]), 10)
            self.assertTrue(all(value["static"] for value in identities["elf_artifacts"].values()))
            self.assertIn("/usr/share/licenses/go/LICENSE", identities["baked_file_sha256"])

    def test_media_arm_app_checks_all_thirteen_binaries_including_page_service(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context, files = media_fixture(root, "media-microservices", machine=183)
            archive(root / "image.tar", files)
            fs = smoke.ImageFilesystem(root / "image.tar")
            self.addCleanup(fs.close)
            identities = smoke._inspect_filesystem(fs, "media_microservices", "media-microservices", "aarch64", context, {})
            self.assertEqual(len(identities["required_executables"]), 13)
            self.assertIn("/usr/local/bin/PageService", identities["elf_artifacts"])
            self.assertEqual(len(identities["dependency_license_sha256"]), 9)

    def test_media_baked_lua_modules_and_all_dependency_licenses_are_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context, files = media_fixture(root, "nginx-web-server")
            archive(root / "image.tar", files)
            fs = smoke.ImageFilesystem(root / "image.tar")
            self.addCleanup(fs.close)
            identities = smoke._inspect_filesystem(fs, "media_microservices", "nginx-web-server", "x86_64", context, {})
            self.assertIn("/usr/local/openresty/lualib/json/json.lua", identities["baked_file_sha256"])
            self.assertIn("/usr/local/openresty/lualib/thrift/Thrift.lua", identities["baked_file_sha256"])
            self.assertEqual(len(identities["dependency_license_sha256"]), 11)
            del files["usr/share/licenses/deathstarbench/dependencies/thrift/LICENSE"]
            archive(root / "missing-license.tar", files)
            missing = smoke.ImageFilesystem(root / "missing-license.tar")
            self.addCleanup(missing.close)
            with self.assertRaisesRegex(smoke.PreparationError, "license/notice"):
                smoke._inspect_filesystem(missing, "media_microservices", "nginx-web-server", "x86_64", context, {})

    def test_wrong_arch_missing_binary_dynamic_hotel_config_drift_and_private_key_fail(self):
        for mutation in ("wrong-arch", "missing", "dynamic", "config", "key", "runtime-clone"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                context, files = hotel_fixture(root)
                config = {"Cmd": ["/go/bin/frontend"]}
                if mutation == "wrong-arch": files["go/bin/frontend"] = (elf(183), 0o755)
                elif mutation == "missing": del files["go/bin/user"]
                elif mutation == "dynamic": files["go/bin/frontend"] = (elf(dynamic=True), 0o755)
                elif mutation == "config": files["workspace/config.json"] = b"drift"
                elif mutation == "key": files["x509/demo.pem"] = b"-----BEGIN PRIVATE KEY-----\n" + b"A" * 64 + b"\n-----END PRIVATE KEY-----\n"
                elif mutation == "runtime-clone": config = {"Entrypoint": ["/bin/sh", "-c", "git clone https://example.invalid/mutable"]}
                archive(root / "image.tar", files)
                fs = smoke.ImageFilesystem(root / "image.tar")
                self.addCleanup(fs.close)
                with self.assertRaises(smoke.PreparationError):
                    smoke._inspect_filesystem(fs, "hotel_reservation", "hotel-reservation", "x86_64", context, config)

    def test_elf_architecture_and_relocatable_objects_are_distinguished(self):
        self.assertTrue(smoke.elf_identity(elf(183), "aarch64")["static"])
        identity = smoke.elf_identity(elf(relocatable=True), "x86_64")
        self.assertFalse(identity["runnable"])
        self.assertFalse(identity["static"])
        for data in (b"not elf", elf()[:70], elf(183)):
            with self.assertRaises(smoke.PreparationError):
                smoke.elf_identity(data, "x86_64")


class SmokeExecutionTests(unittest.TestCase):
    def test_frontend_plan_checks_exact_baked_config_and_all_generated_modules(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context, _ = media_fixture(root, "nginx-web-server")
            identities = {"elf_artifacts": {"/usr/local/lib/example.so": {"dynamic_dependencies": True}}}
            plan = smoke.inspection_plan("media_microservices", "nginx-web-server", context, identities)
            self.assertEqual(plan[0], (smoke._ldd_command(["/usr/local/lib/example.so"]), ()))
            self.assertEqual(plan[1][0], ["/usr/local/openresty/bin/openresty", "-t"])
            self.assertEqual(plan[2][0][:2], ["/usr/local/openresty/bin/resty", "-e"])
            self.assertIn('require("media_service_ttypes")', plan[2][0][2])
            self.assertIn('require("opentracing_bridge_tracer")', plan[2][0][2])
            self.assertTrue(plan[1][1])
            self.assertEqual(plan[1][1], plan[2][1])

    def test_scratch_smoke_uses_only_stopped_owned_container_and_candidate_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context, files = hotel_fixture(root)
            tar_path = root / "image.tar"
            archive(tar_path, files)
            image = "ghcr.io/example/hotel@sha256:" + "a" * 64
            config_digest = "sha256:" + "b" * 64
            commands = []
            def command(args, *, timeout=120, stdout_file=None):
                commands.append(args)
                if args[1] == "image": return encode([{"Id": config_digest, "Architecture": "amd64", "Os": "linux"}])
                if args[1] == "create": return ("c" * 64 + "\n").encode()
                if args[1] == "export": stdout_file.write(tar_path.read_bytes()); return b""
                if args[1] == "rm": return b""
                raise AssertionError(args)
            verified = {"image": image, "index_image": None, "config_digest": config_digest, "config": {"config": {"Cmd": ["/go/bin/frontend"]}}}
            with mock.patch.object(smoke, "_native_runner", return_value={"platform": "linux/amd64", "architecture": "x86_64", "qemu_used": False}), mock.patch.object(smoke, "resolve_platform", return_value=verified), mock.patch.object(smoke, "_command", side_effect=command):
                receipt = smoke.smoke_image(image, workload_id="hotel_reservation", image_key="hotel-reservation", architecture="x86_64", context=context)
            self.assertTrue(receipt["candidate_only"])
            self.assertFalse(receipt["released"])
            self.assertFalse(receipt["runtime_semantics_qualified"])
            self.assertEqual(receipt["context_sha256"], smoke.tree_sha256(context))
            self.assertEqual(receipt["smoke_execution"]["network"], "none")
            self.assertEqual(receipt["load_driver_attestation"], None)
            self.assertEqual([c[1] for c in commands], ["image", "create", "export", "rm"])
            self.assertIn("--entrypoint=/nonexistent-smoke-not-started", commands[1])
            self.assertEqual(commands[-1], ["docker", "rm", "c" * 64])

    def test_native_runner_rejects_qemu_wrong_host_and_daemon_architecture(self):
        for system, host, daemon in (("Darwin", "x86_64", "x86_64"), ("Linux", "aarch64", "x86_64"), ("Linux", "x86_64", "aarch64")):
            with self.subTest(system=system, host=host, daemon=daemon), mock.patch.object(smoke.host_platform, "system", return_value=system), mock.patch.object(smoke.host_platform, "machine", return_value=host), mock.patch.object(smoke, "_command", return_value=encode({"OSType": "linux", "Architecture": daemon})):
                with self.assertRaises(smoke.PreparationError):
                    smoke._native_runner("x86_64")

    def test_hardened_command_records_mock_dns_and_never_exposes_host_network(self):
        with mock.patch.object(smoke, "_command", return_value=b"syntax passed") as command:
            result = smoke._run_inspection("ghcr.io/example/image@sha256:" + "a" * 64, ["/usr/bin/test", "argument"], hosts=("jaeger",))
        args = command.call_args.args[0]
        for required in ("--network=none", "--read-only", "--user=65532:65532", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--add-host=jaeger:127.0.0.1"):
            self.assertIn(required, args)
        self.assertEqual(result["mock_dns"], {"jaeger": "127.0.0.1"})
        self.assertNotIn("--network=host", args)

    def test_smoke_rejects_mutable_crossed_and_arm_driver_before_docker_commands(self):
        for image, workload, key, architecture in (("ghcr.io/example/a:mutable", "hotel_reservation", "hotel-reservation", "x86_64"), ("ghcr.io/example/a@sha256:" + "a" * 64, "hotel_reservation", "nginx-web-server", "x86_64"), ("ghcr.io/example/a@sha256:" + "a" * 64, "hotel_reservation", "load-driver", "aarch64")):
            with self.subTest(key=key, architecture=architecture), mock.patch.object(smoke, "_command") as command:
                with self.assertRaises(smoke.PreparationError):
                    smoke.smoke_image(image, workload_id=workload, image_key=key, architecture=architecture, context=Path("/nonexistent"))
                command.assert_not_called()


class SupportInventoryTests(unittest.TestCase):
    def test_support_versions_platform_configs_and_candidate_only_state(self):
        def resolve(reference, platform):
            key = "MONGO_VERSION" if "/mongo:" in reference else "REDIS_VERSION" if "/redis:" in reference else "MEMCACHED_VERSION" if "/memcached:" in reference else "CONSUL_VERSION"
            return {"image": reference.split(":")[0] + "@sha256:" + "a" * 64, "index_image": None, "manifest_digest": "sha256:" + "a" * 64, "config_digest": "sha256:" + "b" * 64, "manifest": {"config": {}}, "config": {"os": "linux", "architecture": "amd64", "config": {"Env": [key + "=" + reference.rsplit(":", 1)[1]]}}}
        with mock.patch.object(support, "resolve_platform", side_effect=resolve) as resolver:
            inventory = support.inventory_support_images()
        self.assertTrue(inventory["candidate_only"])
        self.assertFalse(inventory["released"])
        self.assertFalse(inventory["runtime_semantics_qualified"])
        hotel = inventory["workloads"]["hotel_reservation"]["images"]
        media = inventory["workloads"]["media_microservices"]["images"]
        self.assertEqual(hotel["mongodb"]["candidate_version"], "5.0.31")
        self.assertEqual(hotel["consul"]["candidate_version"], "1.20.5")
        self.assertEqual(media["mongodb"]["candidate_version"], "4.4.6")
        self.assertTrue(all(call.args[1] == "linux/amd64" for call in resolver.call_args_list))
        self.assertEqual(resolver.call_count, 6)
        self.assertFalse(hotel["mongodb"]["binary_version_executed"])

    def test_mismatched_version_metadata_and_existing_receipt_are_rejected(self):
        with mock.patch.object(support, "resolve_platform", return_value={"config": {"config": {"Env": ["MONGO_VERSION=wrong"]}}}):
            with self.assertRaises(smoke.PreparationError):
                support.inventory_support_images()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            support.write_receipt(path, {"candidate_only": True})
            with self.assertRaises(smoke.PreparationError):
                support.write_receipt(path, {"replacement": True})
            self.assertEqual(json.loads(path.read_text()), {"candidate_only": True})


if __name__ == "__main__":
    unittest.main()
