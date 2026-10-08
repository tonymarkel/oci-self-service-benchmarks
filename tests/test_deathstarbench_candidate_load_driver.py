"""Offline candidate preparation, executable Bash, and optional Lua smoke.

The dedicated artifact CI gate sets DSB_REQUIRE_LUA=1 and supplies both real
pinned prepared contexts. Missing interpreter is then a failure, never a skip.
The ordinary offline suite may use fixtures without a host Lua installation.
"""

import dataclasses
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

from scripts import prepare_deathstarbench_candidate_load_driver as prepare


HOTEL_SCRIPT = '''local socket = require("socket")
math.randomseed(socket.gettime()*1000)
math.random(); math.random(); math.random()

local url = "http://localhost:5000"
local function get_user()
  local id = math.random(0, 500)
  local user_name = "Cornell_" .. tostring(id)
  local pass_word = ""
  for i = 0, 9, 1 do pass_word = pass_word .. tostring(id) end
  return user_name, pass_word
end
local function search_hotel()
  local lat = 38.0235 + (math.random(0, 481) - 240.5)/1000.0
  local lon = -122.095 + (math.random(0, 325) - 157.0)/1000.0
  local path = url .. "/hotels?inDate=" .. "2015-04-10" ..
    "&outDate=2015-04-12&lat=" .. tostring(lat) .. "&lon=" .. tostring(lon)
  return wrk.format("GET", path, {}, nil)
end
local function recommend()
  local lat = 38.0235 + (math.random(0, 481) - 240.5)/1000.0
  local lon = -122.095 + (math.random(0, 325) - 157.0)/1000.0
  local path = url .. "/recommendations?require=" .. "price" ..
    "&lat=" .. tostring(lat) .. "&lon=" .. tostring(lon)
  return wrk.format("GET", path, {}, nil)
end
local function reserve()
  local hotel_id = tostring(math.random(1, 80))
  local user_id, password = get_user()
  local cust_name = user_id
  local num_room = "1"
  local path = url .. "/reservation?inDate=" .. "2015-04-10" ..
    "&outDate=2015-04-12&lat=" .. tostring(lat) .. "&lon=" .. tostring(lon) ..
    "&hotelId=" .. hotel_id .. "&customerName=" .. cust_name .. "&username=" .. user_id ..
    "&password=" .. password .. "&number=" .. num_room
  return wrk.format("POST", path, {}, nil)
end
local function user_login()
  local user_name, password = get_user()
  local path = url .. "/user?username=" .. user_name .. "&password=" .. password
  return wrk.format("POST", path, {}, nil)
end
request = function()
  local coin = math.random()
  if coin < 0.6 then return search_hotel()
  elseif coin < 0.99 then return recommend()
  elseif coin < 0.995 then return user_login()
  else return reserve() end
end
'''


MEDIA_SCRIPT = '''math.randomseed(os.time())
math.random(); math.random(); math.random()
local movie_titles = {}
for i = 1, 1000 do movie_titles[i] = "Fixture Movie " .. tostring(i) end
request = function()
  local movie_index = math.random(1000)
  local user_index = math.random(1000)
  local username = "username_" .. tostring(user_index)
  local password = "password_" .. tostring(user_index)
  local title = movie_titles[movie_index]:gsub(" ", "+")
  local rating = math.random(0, 10)
  local text = tostring(math.random(1000000))
  local path = url .. "/wrk2-api/review/compose"
  local headers = {["Content-Type"] = "application/x-www-form-urlencoded"}
  local body = "username=" .. username .. "&password=" .. password .. "&title=" ..
    title .. "&rating=" .. rating .. "&text=" .. text
  return wrk.format("POST", path, headers, body)
end
'''


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def metrics_for(workload):
    return (f'local candidate_workload = "{workload}"\n'
            f'local candidate_seed_base = {prepare.SEED_BASE}\n'
            + (prepare.ASSET_ROOT / "candidate-metrics.lua").read_text())


def source_fixture(root, workload):
    profile = prepare._profile(workload)
    wrk = root / "wrk2"
    write(wrk / "Makefile", "all:\n\t@true\n")
    write(wrk / "LICENSE", "wrk license fixture\n")
    write(wrk / "deps/luajit/COPYRIGHT", "LuaJIT license fixture\n")
    write(wrk / "deps/luajit/source.c", "/* candidate source fixture */\n")
    write(wrk / "deps/luajit/.git", "gitdir: ignored-input-metadata\n")
    write(root / "LICENSE", "upstream license fixture\n")
    request = write(root / profile.request_script_path,
                    HOTEL_SCRIPT if workload == "hotel_reservation" else MEDIA_SCRIPT)
    return wrk, request


def fixture_inputs(root, workload):
    upstream = root / "upstream"
    wrk, request = source_fixture(upstream, workload)
    rock = root / prepare.social.LUASOCKET_FILENAME
    rock.write_bytes(b"candidate LuaSocket source rock fixture")
    return upstream, wrk, request, rock


class CandidateDriverContextTests(unittest.TestCase):
    def setUp(self):
        # These small context fixtures are deliberately not the pinned wrk Git
        # repositories. Exact hidden-drift validation is exercised below with
        # real Git indexes and by the separate recursive-source CI gate.
        patcher = mock.patch.object(prepare, "_validate_wrk_git_bytes")
        self.wrk_git_validation = patcher.start()
        self.addCleanup(patcher.stop)

    def test_identity_is_distinct_and_social_globals_unchanged(self):
        old_identity = (prepare.social.LOAD_DRIVER_REVISION,
                        prepare.social.REQUEST_SCRIPT_RELATIVE,
                        prepare.social.MIXED_WORKLOAD_LUA_SHA256)
        profiles = [prepare._profile(workload) for workload in prepare.WORKLOAD_IDS]
        self.assertEqual(len({profile.load_driver_revision for profile in profiles}), 2)
        for profile in profiles:
            self.assertFalse(profile.released)
            self.assertNotEqual(profile.load_driver_revision, old_identity[0])
            self.assertEqual(prepare._profile(profile.workload_id).request_script_sha256,
                             profile.request_script_sha256)
        self.assertEqual(old_identity, (prepare.social.LOAD_DRIVER_REVISION,
                                       prepare.social.REQUEST_SCRIPT_RELATIVE,
                                       prepare.social.MIXED_WORKLOAD_LUA_SHA256))

    def test_unknown_and_social_workloads_rejected(self):
        for workload in ("social_network", "", "hotel", "../media_microservices"):
            with self.subTest(workload=workload), self.assertRaises(prepare.PreparationError):
                prepare._profile(workload)

    def test_request_patches_are_strict_and_endpoint_is_not_duplicated(self):
        hotel = prepare.prepare_request_script("hotel_reservation", HOTEL_SCRIPT)
        self.assertIn('local url = "http://localhost:8080"', hotel)
        reserve = hotel.split("local function reserve()", 1)[1].split("local function user_login()", 1)[0]
        self.assertIn("local lat = 38.0235", reserve)
        self.assertIn("local lon = -122.095", reserve)
        self.assertNotIn("math.randomseed(", hotel)
        media = prepare.prepare_request_script("media_microservices", MEDIA_SCRIPT)
        self.assertEqual(media.count("http://localhost:8080"), 1)
        self.assertEqual(media.count("/wrk2-api/review/compose"), 1)
        self.assertNotIn("/wrk2-api/review/compose/wrk2-api", media)
        mutations = (
            ("hotel_reservation", HOTEL_SCRIPT.replace("localhost:5000", "localhost:5001")),
            ("hotel_reservation", HOTEL_SCRIPT + 'local url = "http://localhost:5000"\n'),
            ("hotel_reservation", HOTEL_SCRIPT.replace('  local num_room = "1"\n', "")),
            ("hotel_reservation", HOTEL_SCRIPT.replace('"/hotels?inDate="', '"/changed?inDate="')),
            ("media_microservices", MEDIA_SCRIPT.replace("os.time()", "42")),
            ("media_microservices", MEDIA_SCRIPT.replace("/review/compose", "/review/create")),
        )
        for workload, script in mutations:
            with self.subTest(workload=workload, script=script), self.assertRaises(prepare.PreparationError):
                prepare.prepare_request_script(workload, script)

    def test_prepared_contexts_hash_scripts_metrics_licenses_and_share_exact_wrk_recipe(self):
        for workload in prepare.WORKLOAD_IDS:
            with self.subTest(workload=workload), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                upstream, wrk, request, rock = fixture_inputs(root, workload)
                output = root / "output"
                with mock.patch.object(prepare.social, "_validate_upstream", return_value=(wrk, request)), \
                     mock.patch.object(prepare, "validate_tracked_source", return_value=upstream) as validate, \
                     mock.patch.object(prepare, "sha256", wraps=prepare.sha256) as digest, \
                     mock.patch.object(prepare.social, "_extract_luasocket_license", return_value=b"exact license fixture\n"):
                    digest.side_effect = lambda path: (prepare.social.LUASOCKET_SOURCE_SHA256
                        if path == rock else hashlib.sha256(path.read_bytes()).hexdigest())
                    manifest = prepare.prepare_context(workload, upstream, rock, output)
                context = output / "context"
                profile = prepare._profile(workload)
                validate.assert_called_once_with(
                    upstream, relative_roots=("LICENSE", profile.request_script_path),
                    anchors={"LICENSE": prepare.UPSTREAM_LICENSE_SHA256,
                             profile.request_script_path: profile.request_script_sha256})
                self.assertTrue(manifest["candidate_only"])
                self.assertFalse(manifest["released"])
                self.assertFalse(manifest["measurement_qualified"])
                self.assertEqual(manifest["platform"], "linux/amd64")
                self.assertEqual(manifest["context_sha256"], prepare.tree_sha256(context))
                self.assertEqual(manifest["original_request_script_sha256"], profile.request_script_sha256)
                self.assertEqual((context / "original-request.lua").read_text(), request.read_text())
                self.assertEqual(manifest["prepared_request_script_sha256"],
                                 hashlib.sha256((context / "request.lua").read_bytes()).hexdigest())
                self.assertEqual(manifest["metrics_script_sha256"],
                                 hashlib.sha256((context / "metrics.lua").read_bytes()).hexdigest())
                self.assertEqual(manifest["entrypoint_sha256"],
                                 hashlib.sha256((context / "entrypoint").read_bytes()).hexdigest())
                self.assertFalse((context / "wrk2/deps/luajit/.git").exists())
                self.assertEqual(manifest["seed_method"], prepare.SEED_METHOD)
                self.assertFalse(manifest["attestation_network_required"])
                self.assertTrue(manifest["runtime_read_only"])
                self.assertTrue(manifest["runtime_no_new_privileges"])
                self.assertEqual(manifest["runtime_drop_capabilities"], ["ALL"])
                self.assertIn("noexec,nosuid,nodev", manifest["runtime_tmpfs"])
                for filename, expected in manifest["licenses_sha256"].items():
                    self.assertEqual(hashlib.sha256((context / "licenses" / filename).read_bytes()).hexdigest(), expected)
                dockerfile = (context / "Dockerfile").read_text()
                self.assertEqual(dockerfile.count(prepare.social.ROCKY_LINUX_9_MINIMAL_AMD64), 2)
                self.assertIn("USER 65532:65532", dockerfile)
                self.assertIn("RUN --network=none /opt/deathstarbench-candidate-driver/bin/entrypoint attest", dockerfile)
                self.assertIn(manifest["prepared_request_script_sha256"], dockerfile)
                self.assertIn(manifest["metrics_script_sha256"], dockerfile)
                self.assertIn(manifest["entrypoint_sha256"], dockerfile)
                self.assertNotIn(prepare.social.LOAD_DRIVER_REVISION, dockerfile)
                self.assertIn('CMD ["attest"]', dockerfile)
                self.assertIn("not a published platform digest lock", " ".join(manifest["limitations"]))
                self.assertEqual(json.loads((output / "context-manifest.json").read_text()), manifest)
                self.wrk_git_validation.assert_called_with(upstream, wrk)

    def test_shared_build_recipe_anchor_drift_rejected(self):
        original = prepare.social._dockerfile()
        with mock.patch.object(prepare.social, "_dockerfile", return_value=original.replace("mixed-workload.lua", "new.lua", 1)):
            with self.assertRaises(prepare.PreparationError):
                prepare._dockerfile(prepare._profile("hotel_reservation"), "1" * 64, "2" * 64, "3" * 64)

    def test_generated_contexts_and_manifest_are_umask_independent(self):
        for workload in prepare.WORKLOAD_IDS:
            with self.subTest(workload=workload), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                upstream, wrk, request, rock = fixture_inputs(root, workload)
                manifests = []
                with mock.patch.object(prepare.social, "_validate_upstream", return_value=(wrk, request)), \
                     mock.patch.object(prepare, "validate_tracked_source", return_value=upstream), \
                     mock.patch.object(prepare, "sha256") as digest, \
                     mock.patch.object(prepare.social, "_extract_luasocket_license", return_value=b"exact license fixture\n"):
                    digest.side_effect = lambda path: (prepare.social.LUASOCKET_SOURCE_SHA256
                        if path == rock else hashlib.sha256(path.read_bytes()).hexdigest())
                    for mask in (0o022, 0o077):
                        old = os.umask(mask)
                        try:
                            output = root / f"output-{mask}"
                            manifests.append(prepare.prepare_context(workload, upstream, rock, output))
                        finally:
                            os.umask(old)
                        self.assertEqual((output / "context").stat().st_mode & 0o777, 0o755)
                        self.assertEqual((output / "context/request.lua").stat().st_mode & 0o777, 0o644)
                        self.assertEqual((output / "context/entrypoint").stat().st_mode & 0o777, 0o755)
                        self.assertEqual((output / "context/licenses/LuaSocket-LICENSE").stat().st_mode & 0o777, 0o644)
                        self.assertEqual((output / "context-manifest.json").stat().st_mode & 0o777, 0o644)
                self.assertEqual(manifests[0], manifests[1])

    def test_input_source_and_rock_drift_fail_before_output_survives(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            upstream, wrk, request, rock = fixture_inputs(root, "hotel_reservation")
            for failure in ("social", "tracked", "rock", "license"):
                with self.subTest(failure=failure):
                    with mock.patch.object(prepare.social, "_validate_upstream", return_value=(wrk, request)) as shared, \
                         mock.patch.object(prepare, "validate_tracked_source", return_value=upstream) as tracked, \
                         mock.patch.object(prepare, "sha256", return_value=prepare.social.LUASOCKET_SOURCE_SHA256) as digest, \
                         mock.patch.object(prepare.social, "_extract_luasocket_license", return_value=b"license") as license_read:
                        if failure == "social": shared.side_effect = prepare.social.PreparationError("source drift")
                        elif failure == "tracked": tracked.side_effect = prepare.PreparationError("script drift")
                        elif failure == "rock": digest.return_value = "0" * 64
                        else: license_read.side_effect = prepare.social.PreparationError("license drift")
                        with self.assertRaises(prepare.PreparationError):
                            prepare.prepare_context("hotel_reservation", upstream, rock, root / "output")
                    self.assertFalse((root / "output").exists())

    def test_existing_symlink_and_overlapping_outputs_are_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            upstream, wrk, request, rock = fixture_inputs(root, "hotel_reservation")
            existing = root / "existing"
            existing.mkdir()
            sentinel = write(existing / "sentinel", "keep\n")
            linked = root / "linked"
            linked.symlink_to(existing, target_is_directory=True)
            dangling = root / "dangling"
            dangling.symlink_to(root / "missing")
            for output in (existing, linked, dangling, upstream / "output", upstream, root):
                with self.subTest(output=output), self.assertRaises(prepare.PreparationError):
                    prepare.prepare_context("hotel_reservation", upstream, rock, output)
            self.assertEqual(sentinel.read_text(), "keep\n")
            self.assertTrue(linked.is_symlink())
            self.assertTrue(dangling.is_symlink())

    def test_source_symlink_and_luasocket_symlink_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            upstream, wrk, request, rock = fixture_inputs(root, "hotel_reservation")
            linked_upstream = root / "linked-upstream"
            linked_upstream.symlink_to(upstream, target_is_directory=True)
            with self.assertRaises(prepare.PreparationError):
                prepare.prepare_context("hotel_reservation", linked_upstream, rock, root / "output")
            linked_rock = root / "linked-rock"
            linked_rock.symlink_to(rock)
            with mock.patch.object(prepare.social, "_validate_upstream", return_value=(wrk, request)), \
                 mock.patch.object(prepare, "validate_tracked_source", return_value=upstream):
                with self.assertRaises(prepare.PreparationError):
                    prepare.prepare_context("hotel_reservation", upstream, linked_rock, root / "output")
            self.assertFalse((root / "output").exists())

    def test_parallel_requests_and_recipes_do_not_exchange_workload(self):
        social_values = (prepare.social.LOAD_DRIVER_REVISION, prepare.social.REQUEST_SCRIPT_RELATIVE,
                         prepare.social.MIXED_WORKLOAD_LUA_SHA256)
        def render(workload):
            script = prepare.prepare_request_script(workload, HOTEL_SCRIPT if workload == "hotel_reservation" else MEDIA_SCRIPT)
            return script, prepare._dockerfile(prepare._profile(workload), hashlib.sha256(script.encode()).hexdigest(), "2" * 64, "3" * 64)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(render, prepare.WORKLOAD_IDS * 8))
        for index, (script, recipe) in enumerate(results):
            expected = prepare.WORKLOAD_IDS[index % 2]
            self.assertIn(prepare._profile(expected).load_driver_revision, recipe)
            self.assertIn("/reservation?" if expected == "hotel_reservation" else "/wrk2-api/review/compose", script)
        self.assertEqual(social_values, (prepare.social.LOAD_DRIVER_REVISION,
                                       prepare.social.REQUEST_SCRIPT_RELATIVE,
                                       prepare.social.MIXED_WORKLOAD_LUA_SHA256))

    def test_cli_limits_workload_choices(self):
        arguments = prepare._parser().parse_args(["--workload", "hotel_reservation", "--upstream", "/tmp/source",
                                                 "--luasocket-rock", "/tmp/source.rock", "--output", "/tmp/context"])
        self.assertEqual(arguments.workload, "hotel_reservation")
        self.assertEqual(arguments.upstream, Path("/tmp/source"))


class CandidateWrkGitIntegrityTests(unittest.TestCase):
    def git(self, root, *arguments):
        return subprocess.run(["git", "-C", str(root), *arguments], check=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True).stdout.strip()

    def fixture(self, root):
        self.git(root, "init", "-q")
        source = write(root / "source.c", "/* pinned source fixture */\n")
        source.chmod(0o644)
        self.git(root, "add", "source.c")
        self.git(root, "-c", "user.name=Candidate Fixture", "-c", "user.email=fixture@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "-qm", "fixture")
        return source

    def test_assume_unchanged_cannot_hide_modified_wrk_or_luajit_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.fixture(root)
            prepare._validate_git_blobs(root, "HEAD", root, allowed_gitlinks={})
            self.git(root, "update-index", "--assume-unchanged", "source.c")
            source.write_text("/* attacker replaced the build input */\n")
            self.assertEqual(self.git(root, "status", "--porcelain"), "")
            with self.assertRaisesRegex(prepare.PreparationError, "source bytes drifted"):
                prepare._validate_git_blobs(root, "HEAD", root, allowed_gitlinks={})

    def test_skip_worktree_cannot_hide_modified_wrk_or_luajit_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.fixture(root)
            self.git(root, "update-index", "--skip-worktree", "source.c")
            source.write_text("/* hidden changed source */\n")
            self.assertEqual(self.git(root, "status", "--porcelain"), "")
            with self.assertRaisesRegex(prepare.PreparationError, "source bytes drifted"):
                prepare._validate_git_blobs(root, "HEAD", root, allowed_gitlinks={})

    def test_core_filemode_false_cannot_hide_modified_executable_mode(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = self.fixture(root)
            self.git(root, "config", "core.filemode", "false")
            source.chmod(0o755)
            self.assertEqual(self.git(root, "status", "--porcelain"), "")
            with self.assertRaisesRegex(prepare.PreparationError, "executable mode"):
                prepare._validate_git_blobs(root, "HEAD", root, allowed_gitlinks={})

    def test_unexpected_gitlink_inventory_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.fixture(root)
            with self.assertRaisesRegex(prepare.PreparationError, "Gitlink inventory drifted"):
                prepare._validate_git_blobs(root, "HEAD", root, allowed_gitlinks={"deps/luajit": "0" * 40})


def runtime_fixture(root):
    runtime = root / "runtime"
    fake_bin = root / "fake-bin"
    binaries = {
        "uname": "#!/bin/bash\nprintf 'x86_64\\n'\n",
        "ldd": "#!/bin/bash\nif [[ ${FAKE_LDD_MODE:-ready} == missing ]]; then echo 'libssl => not found'; else echo 'libc => /lib64/libc'; fi\n",
        "nproc": "#!/bin/bash\necho 2\n",
        "sed": '#!/bin/bash\n[[ $1 == -i ]] || exit 10\nshift\n/usr/bin/sed "$1" "$2" > "$2.tmp"\nmv "$2.tmp" "$2"\n',
    }
    for name, content in binaries.items():
        write(fake_bin / name, content).chmod(0o755)
    write(runtime / "bin/wrk", '''#!/bin/bash
if [[ $# == 0 ]]; then
  echo 'Usage: wrk <options> <url>' >&2
  [[ ${FAKE_WRK_MODE:-ready} == bad-usage ]] || echo '  -R, --rate Rate' >&2
  exit 1
fi
printf '%s\\n' "$@" > "$FAKE_WRK_ARGS"
while [[ $# -gt 0 ]]; do
  if [[ $1 == -s ]]; then cp "$2" "$FAKE_WRK_SCRIPT"; break; fi
  shift
done
exit "${FAKE_WRK_EXIT:-0}"
''').chmod(0o755)
    write(runtime / "luajit/bin/luajit", "#!/bin/bash\n[[ ${FAKE_LUA_MODE:-ready} == ready ]]\n").chmod(0o755)
    write(runtime / "share/request.lua", 'local url = "http://localhost:8080"\nrequest = function() end\n')
    write(runtime / "share/metrics.lua", metrics_for("hotel_reservation"))
    fake_time = write(fake_bin / "candidate-time", '#!/bin/bash\n[[ $1 == -v ]] || exit 12\nshift\nexec "$@"\n')
    fake_time.chmod(0o755)
    entrypoint = write(runtime / "bin/entrypoint", (prepare.ASSET_ROOT / "candidate-entrypoint").read_text()
                       .replace("/opt/deathstarbench-candidate-driver", str(runtime))
                       .replace("/usr/bin/time", str(fake_time)))
    entrypoint.chmod(0o755)
    manifest = {
        "WORKLOAD_ID": "hotel_reservation", "LOAD_DRIVER_REVISION": prepare._profile("hotel_reservation").load_driver_revision,
        "UPSTREAM_REVISION": prepare.social.UPSTREAM_REVISION, "CONTEXT_SHA256": "1" * 64,
        "WRK2_SOURCE_SHA256": "2" * 64, "LUAJIT_REVISION": prepare.social.LUAJIT_REVISION,
        "LUASOCKET_SOURCE_SHA256": prepare.social.LUASOCKET_SOURCE_SHA256,
        "ORIGINAL_REQUEST_SCRIPT_SHA256": prepare._profile("hotel_reservation").request_script_sha256,
        "SEED_BASE": str(prepare.SEED_BASE),
    }
    for name, relative in (("WRK_BINARY_SHA256", "bin/wrk"), ("REQUEST_SCRIPT_SHA256", "share/request.lua"),
                           ("METRICS_SCRIPT_SHA256", "share/metrics.lua"), ("ENTRYPOINT_SHA256", "bin/entrypoint")):
        manifest[name] = hashlib.sha256((runtime / relative).read_bytes()).hexdigest()
    write(runtime / "artifact-manifest.env", "".join(f"{name}={value}\n" for name, value in manifest.items()))
    environment = {**os.environ, "PATH": f"{fake_bin}:{os.environ.get('PATH', '/usr/bin:/bin')}",
                   "FAKE_WRK_ARGS": str(root / "wrk-args"), "FAKE_WRK_SCRIPT": str(root / "wrk-script")}
    return runtime, entrypoint, environment


class CandidateDriverEntrypointTests(unittest.TestCase):
    def run_entrypoint(self, path, environment, *arguments):
        return subprocess.run(["bash", str(path), *arguments], env=environment, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def test_attest_is_candidate_only_and_smoke_checks_are_executed(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime, entrypoint, environment = runtime_fixture(Path(temporary))
            result = self.run_entrypoint(entrypoint, environment, "attest")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("CANDIDATE_DSB_LOAD_DRIVER_ARTIFACT", result.stdout)
            self.assertIn("measurement_qualified=0", result.stdout)
            self.assertNotIn("DISTRIBUTED_DSB_LOAD_DRIVER_ARTIFACT", result.stdout)
            for key, value in (("FAKE_LDD_MODE", "missing"), ("FAKE_WRK_MODE", "bad-usage"), ("FAKE_LUA_MODE", "bad")):
                with self.subTest(key=key):
                    failed = self.run_entrypoint(entrypoint, {**environment, key: value}, "attest")
                    self.assertNotEqual(failed.returncode, 0)

    def test_attestation_rejects_each_changed_runtime_asset(self):
        for relative in ("bin/wrk", "share/request.lua", "share/metrics.lua", "bin/entrypoint"):
            with self.subTest(relative=relative), tempfile.TemporaryDirectory() as temporary:
                runtime, entrypoint, environment = runtime_fixture(Path(temporary))
                path = runtime / relative
                path.write_text(path.read_text() + "\n# changed\n")
                result = self.run_entrypoint(entrypoint, environment, "attest")
                self.assertEqual(result.returncode, 64)
                self.assertIn("artifact digest mismatch", result.stderr)

    def test_run_binds_private_nodeport_once_and_propagates_wrk_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime, entrypoint, environment = runtime_fixture(root)
            result = self.run_entrypoint(entrypoint, environment, "run", "--target", "10.2.3.4", "--port", "8080",
                                         "--threads", "2", "--connections", "4", "--rate", "100", "--duration", "7")
            self.assertEqual(result.returncode, 0, result.stderr)
            arguments = (root / "wrk-args").read_text().splitlines()
            self.assertEqual(arguments[:12], ["-D", "exp", "-r", "-t", "2", "-c", "4", "-d", "7s", "-L", "-s", arguments[11]])
            self.assertEqual(arguments[-3:], ["-R", "100", "http://10.2.3.4:8080/"])
            script = (root / "wrk-script").read_text()
            self.assertEqual(script.count("http://10.2.3.4:8080"), 1)
            self.assertNotIn("localhost:8080", script)
            self.assertIn("candidate-body-v1", script)
            self.assertFalse(Path(arguments[11]).exists())
            result = self.run_entrypoint(entrypoint, {**environment, "FAKE_WRK_EXIT": "23"}, "run", "--target", "10.2.3.4", "--port", "8080",
                                         "--threads", "2", "--connections", "4", "--rate", "100", "--duration", "7")
            self.assertEqual(result.returncode, 23, result.stderr)

    def test_invalid_targets_ports_values_and_named_argument_order_fail_closed(self):
        valid = ["run", "--target", "10.2.3.4", "--port", "8080", "--threads", "2", "--connections", "4", "--rate", "100", "--duration", "7"]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime, entrypoint, environment = runtime_fixture(root)
            for index, values in ((2, ("127.0.0.1", "8.8.8.8", "10.0.0.999", "10.0.0.1;touch /tmp/unsafe", "localhost",
                                      "010.0.0.1", "10.010.0.1", "10.0.010.1", "10.0.0.010", "10.0.0.00")),
                                  (4, ("80", "5000", "08080")),
                                  (6, ("0", "-1", "1025", "999999999999999999999", "2;echo")),
                                  (8, ("1", "3", "04")), (10, ("1", "99")), (12, ("0", "86401")),
                                  (1, ("--duration",))):
                for value in values:
                    with self.subTest(index=index, value=value):
                        arguments = valid.copy()
                        arguments[index] = value
                        result = self.run_entrypoint(entrypoint, environment, *arguments)
                        self.assertEqual(result.returncode, 64, result.stderr)
                        self.assertFalse((root / "wrk-args").exists())


class CandidateDriverLuaTests(unittest.TestCase):
    def test_actual_lua_request_body_failure_and_worker_accounting_harness(self):
        binary = os.environ.get("DSB_LUA_BIN") or shutil.which("luajit") or shutil.which("lua5.1")
        required = os.environ.get("DSB_REQUIRE_LUA") == "1"
        if not binary:
            if required:
                self.fail("DSB_REQUIRE_LUA=1 but no Lua 5.1/LuaJIT interpreter is available")
            self.skipTest("Lua interpreter smoke is enforced by the separate artifact-preparation CI gate")
        for workload in prepare.WORKLOAD_IDS:
            with self.subTest(workload=workload), tempfile.TemporaryDirectory() as temporary:
                supplied = os.environ.get("DSB_HOTEL_DRIVER_CONTEXT" if workload == "hotel_reservation" else "DSB_MEDIA_DRIVER_CONTEXT")
                if supplied:
                    context = Path(supplied)
                    request, metrics = context / "request.lua", context / "metrics.lua"
                    self.assertTrue(request.is_file() and metrics.is_file(), "Supplied pinned context assets are missing")
                else:
                    context = Path(temporary)
                    request = write(context / "request.lua", prepare.prepare_request_script(
                        workload, HOTEL_SCRIPT if workload == "hotel_reservation" else MEDIA_SCRIPT))
                    metrics = write(context / "metrics.lua", metrics_for(workload))
                result = subprocess.run([binary, str(prepare.ASSET_ROOT / "candidate-harness.lua"), workload,
                                         str(request), str(metrics)], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("candidate Lua harness passed: " + workload, result.stdout)


if __name__ == "__main__":
    unittest.main()
