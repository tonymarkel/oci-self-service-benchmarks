import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from app.deathstarbench_k3s_workload import validate_image_lock


ROOT = Path(__file__).resolve().parents[1]
LUASOCKET_LICENSE = b'''Copyright (C) 2004-2022 Diego Nehab

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
'''


def load_script(name, relative_path):
    specification = importlib.util.spec_from_file_location(
        name, ROOT / relative_path
    )
    module = importlib.util.module_from_spec(specification)
    assert specification.loader is not None
    specification.loader.exec_module(module)
    return module


prepare = load_script(
    "prepare_deathstarbench_load_driver",
    "scripts/prepare_deathstarbench_load_driver.py",
)
lock_script = load_script(
    "create_deathstarbench_image_lock_load_driver",
    "scripts/create_deathstarbench_image_lock.py",
)


def write(path, content="fixture\n"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def fake_source(root):
    wrk2 = root / "wrk2"
    request = root / prepare.REQUEST_SCRIPT_RELATIVE
    write(wrk2 / "Makefile", "all:\n\t@true\n")
    write(wrk2 / "LICENSE", "Apache fixture\n")
    write(wrk2 / "deps/luajit/COPYRIGHT", "MIT fixture\n")
    write(request, "http://localhost:8080\n" * 3)
    write(root / "LICENSE", "Apache fixture\n")
    return wrk2, request


def load_driver_context(path):
    document = {
        "schema_version": 1,
        "architecture": "x86_64",
        "platform": "linux/amd64",
        "load_driver_revision": lock_script.LOAD_DRIVER_REVISION,
        "upstream_repository": prepare.UPSTREAM_REPOSITORY,
        "upstream_revision": lock_script.UPSTREAM_REVISION,
        "base_image": prepare.ROCKY_LINUX_9_MINIMAL_AMD64,
        "context_path": "context",
        "context_sha256": "a" * 64,
        "wrk2_tree_git_sha": lock_script.WRK2_TREE_GIT_SHA,
        "wrk2_source_sha256": "b" * 64,
        "luajit_revision": lock_script.LUAJIT_REVISION,
        "luasocket_source_url": prepare.LUASOCKET_URL,
        "luasocket_source_sha256": lock_script.LUASOCKET_SOURCE_SHA256,
        "request_script_sha256": lock_script.REQUEST_SCRIPT_SHA256,
        "runtime_network_required": False,
        "runtime_build_tools": False,
        "runtime_user": "65532:65532",
    }
    path.write_text(json.dumps(document), encoding="utf-8")
    return document


def attestation(context, *, wrk_hash="c" * 64):
    values = {
        "architecture": "x86_64",
        "platform": "linux/amd64",
        "revision": lock_script.LOAD_DRIVER_REVISION,
        "upstream_revision": lock_script.UPSTREAM_REVISION,
        "context_sha256": context["context_sha256"],
        "wrk_binary_sha256": wrk_hash,
        "wrk2_tree_git_sha": lock_script.WRK2_TREE_GIT_SHA,
        "wrk2_source_sha256": context["wrk2_source_sha256"],
        "luajit_revision": lock_script.LUAJIT_REVISION,
        "luasocket_source_sha256": lock_script.LUASOCKET_SOURCE_SHA256,
        "request_script_sha256": lock_script.REQUEST_SCRIPT_SHA256,
    }
    return "DISTRIBUTED_DSB_LOAD_DRIVER_ARTIFACT " + " ".join(
        f"{name}={values[name]}"
        for name in lock_script._LOAD_DRIVER_ATTESTATION_FIELDS
    ) + "\n"


def driver_index():
    return json.dumps({
        "schemaVersion": 2,
        "manifests": [
            {
                "digest": "sha256:" + "d" * 64,
                "platform": {"os": "linux", "architecture": "amd64"},
            },
            {
                "digest": "sha256:" + "e" * 64,
                "platform": {"os": "unknown", "architecture": "unknown"},
            },
        ],
    }).encode()


def direct_manifest():
    return json.dumps({
        "schemaVersion": 2,
        "config": {"digest": "sha256:" + "f" * 64},
        "layers": [{"digest": "sha256:" + "1" * 64}],
    }).encode()


def runtime_attestation_fixture(root):
    runtime = root / "runtime"
    fake_bin = root / "fake-bin"
    write(
        runtime / "bin/wrk",
        "#!/bin/bash\n"
        "if [[ ${FAKE_WRK_MODE:-ready} == success ]]; then exit 0; fi\n"
        "echo 'Usage: wrk <options> <url>' >&2\n"
        "if [[ ${FAKE_WRK_MODE:-ready} != bad-usage ]]; then "
        "echo '  -R, --rate Rate' >&2; fi\n"
        "exit 1\n",
    )
    (runtime / "bin/wrk").chmod(0o755)
    write(
        runtime / "luajit/bin/luajit",
        "#!/bin/bash\n"
        "[[ ${FAKE_LUA_MODE:-ready} == ready ]]\n",
    )
    (runtime / "luajit/bin/luajit").chmod(0o755)
    request = runtime / "share/mixed-workload.lua"
    write(request, "http://localhost:8080\n" * 3)
    write(runtime / "share/metrics.lua", "done = function() end\n")
    write(
        fake_bin / "uname",
        "#!/bin/bash\nprintf 'x86_64\\n'\n",
    )
    (fake_bin / "uname").chmod(0o755)
    write(
        fake_bin / "ldd",
        "#!/bin/bash\n"
        "case ${FAKE_LDD_MODE:-ready} in\n"
        "  missing) echo 'libssl.so.3 => not found' ;;\n"
        "  error) exit 1 ;;\n"
        "  *) echo 'libc.so.6 => /lib64/libc.so.6' ;;\n"
        "esac\n",
    )
    (fake_bin / "ldd").chmod(0o755)

    wrk_hash = hashlib.sha256((runtime / "bin/wrk").read_bytes()).hexdigest()
    request_hash = hashlib.sha256(request.read_bytes()).hexdigest()
    manifest_values = {
        "LOAD_DRIVER_REVISION": prepare.LOAD_DRIVER_REVISION,
        "UPSTREAM_REVISION": prepare.UPSTREAM_REVISION,
        "CONTEXT_SHA256": "1" * 64,
        "WRK_BINARY_SHA256": wrk_hash,
        "WRK2_TREE_GIT_SHA": prepare.WRK2_TREE_GIT_SHA,
        "WRK2_SOURCE_SHA256": "2" * 64,
        "LUAJIT_REVISION": prepare.LUAJIT_REVISION,
        "LUASOCKET_SOURCE_SHA256": prepare.LUASOCKET_SOURCE_SHA256,
        "REQUEST_SCRIPT_SHA256": request_hash,
    }
    write(
        runtime / "artifact-manifest.env",
        "".join(f"{key}={value}\n" for key, value in manifest_values.items()),
    )
    entrypoint = root / "entrypoint"
    entrypoint.write_text(
        prepare.ENTRYPOINT.replace(
            "/opt/deathstarbench-load-driver",
            str(runtime),
        ),
        encoding="utf-8",
    )
    entrypoint.chmod(0o755)
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '/usr/bin:/bin')}",
    }
    return entrypoint, environment


class LoadDriverContextTests(unittest.TestCase):
    def test_context_is_pinned_multistage_nonroot_and_offline_at_runtime(self):
        with tempfile.TemporaryDirectory() as temporary:
            temporary_path = Path(temporary)
            upstream = temporary_path / "upstream"
            wrk2, request = fake_source(upstream)
            rock = temporary_path / prepare.LUASOCKET_FILENAME
            rock.write_bytes(b"pinned rock fixture")
            output = temporary_path / "output"

            with mock.patch.object(
                prepare,
                "_validate_upstream",
                return_value=(wrk2, request),
            ), mock.patch.object(
                prepare,
                "_sha256",
                return_value=prepare.LUASOCKET_SOURCE_SHA256,
            ), mock.patch.object(
                prepare,
                "_extract_luasocket_license",
                return_value=LUASOCKET_LICENSE,
            ):
                manifest = prepare.prepare_context(upstream, rock, output)

            dockerfile = (output / "context/Dockerfile").read_text()
            entrypoint = (output / "context/entrypoint").read_text()
            self.assertEqual(dockerfile.count(prepare.ROCKY_LINUX_9_MINIMAL_AMD64), 2)
            self.assertIn("FROM " + prepare.ROCKY_LINUX_9_MINIMAL_AMD64, dockerfile)
            self.assertIn(" AS builder", dockerfile)
            self.assertIn(" AS runtime", dockerfile)
            self.assertIn("USER 65532:65532", dockerfile)
            self.assertNotIn("microdnf install -y coreutils", dockerfile)
            self.assertIn(
                '      linux; \\\n    make -C "$source_dir"',
                dockerfile,
            )
            self.assertIn("rm -f /usr/bin/microdnf", dockerfile)
            runtime = dockerfile.split(" AS runtime", 1)[1]
            for forbidden in (" gcc ", " make ", " git ", " curl ", "luarocks"):
                self.assertNotIn(forbidden, runtime)
            self.assertEqual(manifest["platform"], "linux/amd64")
            self.assertEqual(manifest["load_driver_revision"], prepare.LOAD_DRIVER_REVISION)
            self.assertFalse(manifest["runtime_network_required"])
            self.assertFalse(manifest["runtime_build_tools"])
            self.assertRegex(manifest["context_sha256"], r"^[0-9a-f]{64}$")
            self.assertRegex(manifest["wrk2_source_sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(
                (output / "context/licenses/LuaSocket-LICENSE").read_bytes(),
                LUASOCKET_LICENSE,
            )
            self.assertIn("COPY licenses /src/licenses", dockerfile)
            self.assertIn(
                "cp -a /src/licenses /opt/deathstarbench-load-driver/licenses",
                dockerfile,
            )
            self.assertNotIn("image_manifest_sha256", entrypoint)
            self.assertIn("DISTRIBUTED_DSB_LOAD_DRIVER_ARTIFACT", entrypoint)
            self.assertIn("OCI_DSB_LOADGEN_LOGICAL_CPUS=%s", entrypoint)
            self.assertIn("/usr/bin/time -v", entrypoint)
            self.assertIn("[[ $# == 14 ]]", entrypoint)
            self.assertIn("--max-user-index", entrypoint)
            self.assertIn('dependencies=$(ldd "$wrk_binary"', entrypoint)
            self.assertIn('"Usage: wrk <options> <url>"*"-R, --rate"', entrypoint)
            self.assertIn('assert(require("socket.core"))', entrypoint)
            subprocess.run(
                ["bash", "-n", str(output / "context/entrypoint")],
                check=True,
            )

    def test_source_validation_fails_on_wrk2_tree_drift(self):
        with tempfile.TemporaryDirectory() as temporary:
            upstream = Path(temporary)
            with mock.patch.object(
                prepare,
                "_git",
                side_effect=[prepare.UPSTREAM_REVISION, "", "0" * 40],
            ):
                with self.assertRaisesRegex(
                    prepare.PreparationError,
                    "wrk2 Git tree drifted",
                ):
                    prepare._validate_upstream(upstream)

    def test_dirty_luajit_submodule_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            upstream = Path(temporary)
            (upstream / "wrk2/deps/luajit").mkdir(parents=True)
            with mock.patch.object(
                prepare,
                "_git",
                side_effect=[
                    prepare.UPSTREAM_REVISION,
                    "",
                    prepare.WRK2_TREE_GIT_SHA,
                    prepare.LUAJIT_REVISION,
                    "?? generated-object.o",
                ],
            ):
                with self.assertRaisesRegex(
                    prepare.PreparationError,
                    "LuaJIT checkout is dirty",
                ):
                    prepare._validate_upstream(upstream)

    def test_ignored_or_untracked_wrk2_artifact_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            upstream = Path(temporary)
            wrk2 = upstream / "wrk2"
            write(wrk2 / "Makefile")
            write(wrk2 / "obj/ignored-build-output.o")
            with mock.patch.object(
                prepare,
                "_git",
                return_value="wrk2/Makefile\0",
            ):
                with self.assertRaisesRegex(
                    prepare.PreparationError,
                    "untracked obj",
                ):
                    prepare._validate_tracked_wrk2_tree(upstream, wrk2)

    def test_exact_luasocket_license_is_extracted_from_source_rock(self):
        self.assertEqual(
            hashlib.sha256(LUASOCKET_LICENSE).hexdigest(),
            prepare.LUASOCKET_LICENSE_SHA256,
        )
        with tempfile.TemporaryDirectory() as temporary:
            rock = Path(temporary) / prepare.LUASOCKET_FILENAME
            with zipfile.ZipFile(rock, "w") as archive:
                archive.writestr("luasocket/LICENSE", LUASOCKET_LICENSE)
            self.assertEqual(
                prepare._extract_luasocket_license(rock),
                LUASOCKET_LICENSE,
            )

    def test_luasocket_checksum_drift_fails_before_context_creation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            upstream = root / "upstream"
            wrk2, request = fake_source(upstream)
            rock = root / prepare.LUASOCKET_FILENAME
            rock.write_bytes(b"wrong")
            output = root / "output"
            with mock.patch.object(
                prepare,
                "_validate_upstream",
                return_value=(wrk2, request),
            ):
                with self.assertRaisesRegex(
                    prepare.PreparationError,
                    "LuaSocket source rock drifted",
                ):
                    prepare.prepare_context(upstream, rock, output)
            self.assertFalse(output.exists())


class LoadDriverRuntimeAttestationTests(unittest.TestCase):
    def test_attest_smoke_checks_binary_links_usage_and_embedded_lua(self):
        with tempfile.TemporaryDirectory() as temporary:
            entrypoint, environment = runtime_attestation_fixture(Path(temporary))
            result = subprocess.run(
                [str(entrypoint), "attest"],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )
            self.assertEqual(result.stderr, "")
            self.assertEqual(len(result.stdout.splitlines()), 1)
            self.assertTrue(
                result.stdout.startswith("DISTRIBUTED_DSB_LOAD_DRIVER_ARTIFACT ")
            )

    def test_attest_rejects_missing_library_bad_usage_and_lua_failure(self):
        cases = (
            ("FAKE_LDD_MODE", "missing", "missing dynamic library"),
            ("FAKE_WRK_MODE", "bad-usage", "rate-limited usage interface"),
            ("FAKE_LUA_MODE", "broken", "Lua/LuaSocket runtime cannot load"),
        )
        with tempfile.TemporaryDirectory() as temporary:
            entrypoint, environment = runtime_attestation_fixture(Path(temporary))
            for name, value, error in cases:
                with self.subTest(name=name):
                    case_environment = {**environment, name: value}
                    result = subprocess.run(
                        [str(entrypoint), "attest"],
                        env=case_environment,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                    self.assertEqual(result.returncode, 64)
                    self.assertEqual(result.stdout, "")
                    self.assertIn(error, result.stderr)

    def test_run_parses_all_seven_ordered_named_arguments(self):
        with tempfile.TemporaryDirectory() as temporary:
            entrypoint, environment = runtime_attestation_fixture(Path(temporary))
            result = subprocess.run(
                [
                    str(entrypoint),
                    "run",
                    "--target",
                    "10.240.1.13",
                    "--port",
                    "8080",
                    "--threads",
                    "2",
                    "--connections",
                    "3",
                    "--rate",
                    "100",
                    "--duration",
                    "5",
                    "--max-user-index",
                    "962",
                ],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(result.returncode, 64)
            self.assertEqual(result.stdout, "")
            self.assertEqual(
                result.stderr,
                "load-driver: connections must be at least and divisible by threads\n",
            )

    @unittest.skipUnless(
        sys.platform.startswith("linux"),
        "the runtime image uses GNU sed and GNU time",
    )
    def test_successful_run_clears_exit_trap_before_local_scope_ends(self):
        with tempfile.TemporaryDirectory() as temporary:
            entrypoint, environment = runtime_attestation_fixture(Path(temporary))
            result = subprocess.run(
                [
                    str(entrypoint),
                    "run",
                    "--target",
                    "10.240.1.13",
                    "--port",
                    "8080",
                    "--threads",
                    "4",
                    "--connections",
                    "4",
                    "--rate",
                    "100",
                    "--duration",
                    "5",
                    "--max-user-index",
                    "962",
                ],
                env={**environment, "FAKE_WRK_MODE": "success"},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn("unbound variable", result.stderr)
            self.assertIn("OCI_DSB_LOADGEN_LOGICAL_CPUS=", result.stdout)


class LoadDriverLockTests(unittest.TestCase):
    def test_merge_retains_workload_refs_and_locks_exact_direct_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            base = ROOT / "docs/qualification/deathstarbench-social-network-images-v1.json"
            context_path = root / "context.json"
            context = load_driver_context(context_path)
            attestation_path = root / "attestation.txt"
            attestation_path.write_text(attestation(context), encoding="utf-8")
            calls = []

            def inspect(reference):
                calls.append(reference)
                return direct_manifest() if len(calls) == 2 else driver_index()

            merged = lock_script.merge_load_driver_lock(
                base,
                context_path,
                "ghcr.io/example/load-driver:run@sha256:" + "9" * 64,
                attestation_path,
                inspector=inspect,
            )
            original = json.loads(base.read_text())
            self.assertEqual(merged["platforms"], original["platforms"])
            self.assertEqual(
                merged["load_driver"]["image"],
                "ghcr.io/example/load-driver@sha256:" + "d" * 64,
            )
            self.assertTrue(merged["load_driver"]["published"])
            self.assertEqual(merged["load_driver"]["wrk_binary_sha256"], "c" * 64)
            validated = validate_image_lock(merged)
            self.assertEqual(validated.load_driver.context_sha256, "a" * 64)
            self.assertEqual(len(calls), 2)

    def test_resolution_rejects_an_index_as_the_selected_platform(self):
        calls = 0

        def inspect(_reference):
            nonlocal calls
            calls += 1
            return driver_index()

        with self.assertRaisesRegex(
            lock_script.ImageLockError,
            "direct non-empty platform manifest",
        ):
            lock_script.resolve_load_driver_image(
                "ghcr.io/example/load-driver:run@sha256:" + "9" * 64,
                inspector=inspect,
            )
        self.assertEqual(calls, 2)

    def test_attestation_rejects_reordering_and_context_drift(self):
        with tempfile.TemporaryDirectory() as temporary:
            context_path = Path(temporary) / "context.json"
            context = load_driver_context(context_path)
            marker = attestation(context)
            first = "architecture=x86_64"
            second = "platform=linux/amd64"
            with self.assertRaisesRegex(lock_script.ImageLockError, "schema"):
                lock_script.parse_load_driver_attestation(
                    marker.replace(f"{first} {second}", f"{second} {first}"),
                    context,
                )
            with self.assertRaisesRegex(lock_script.ImageLockError, "context_sha256"):
                lock_script.parse_load_driver_attestation(
                    marker.replace("context_sha256=" + "a" * 64, "context_sha256=" + "f" * 64),
                    context,
                )


if __name__ == "__main__":
    unittest.main()
