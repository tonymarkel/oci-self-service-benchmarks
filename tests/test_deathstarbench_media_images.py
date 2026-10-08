"""Offline preparation and semantic patch tests, never a cloud qualification."""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest import mock

from scripts import deathstarbench_artifact_common as common
from scripts import prepare_deathstarbench_media_images as prepare
from scripts import prepare_deathstarbench_social_images as social


INITIALIZER = '''import aiohttp
import asyncio
async def upload_cast_info(session, addr, cast):
  async with session.post(addr + "/wrk2-api/cast-info/write", json=cast) as resp:
    return await resp.text()
async def upload_plot(session, addr, plot):
  async with session.post(addr + "/wrk2-api/plot/write", json=plot) as resp:
    return await resp.text()
async def upload_movie_info(session, addr, movie):
  async with session.post(addr + "/wrk2-api/movie-info/write", json=movie) as resp:
    return await resp.text()
async def register_movie(session, addr, movie):
  params = {"title": movie["title"], "movie_id": movie["movie_id"]}
  async with session.post(addr + "/wrk2-api/movie/register", data=params) as resp:
    return await resp.text()
async def write_movie_info(addr, raw_movies):
  idx = 0
  tasks = []
  conn = aiohttp.TCPConnector(limit=200)
  async with aiohttp.ClientSession(connector=conn) as session:
    for raw_movie in raw_movies:
      movie = dict()
      movie["movie_id"] = str(raw_movie["id"])
      movie["title"] = raw_movie["title"]
      movie["thumbnail_ids"] = [raw_movie["poster_path"]]
      task = asyncio.ensure_future(upload_movie_info(session, addr, movie))
      tasks.append(task)
      plot = {"plot_id": raw_movie["id"], "plot": raw_movie["overview"]}
      task = asyncio.ensure_future(upload_plot(session, addr, plot))
      tasks.append(task)
      task = asyncio.ensure_future(register_movie(session, addr, movie))
      tasks.append(task)
      idx += 1
    await asyncio.gather(*tasks)
'''


def write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def git(root, *arguments):
    return subprocess.run(
        ["git", "-C", str(root), *arguments], check=True,
        capture_output=True, text=True,
    ).stdout.strip()


def fixture(root):
    media = root / prepare.SOURCE_ROOT
    write(root / "LICENSE", "Apache License 2.0 fixture\n")
    write(media / ".dockerignore", "*\n!/src/\n")
    write(media / "CMakeLists.txt", "add_subdirectory(src)\n")
    write(media / "Dockerfile", "FROM yg397/thrift-microservice-deps:xenial\nCOPY ./ /media-microservices\nRUN echo make\nWORKDIR /media-microservices\n")
    write(media / "docker/thrift-microservice-deps/cpp/Dockerfile", "FROM ubuntu:16.04\nARG LIB_MONGOC_VERSION=1.14.0\nRUN echo dependency-build\n")
    write(media / "src/service.cpp", "// Media application source\n")
    write(media / "cmake/Findthrift.cmake", "# fixture\n")
    write(media / "gen-cpp/media_service_types.h", "// generated Media types\n")
    write(media / "third_party/PicoSHA2/LICENSE", "MIT fixture\n")
    write(media / "config/service-config.json", '{"movie-info-service":{"addr":"movie-info-service","port":9090}}\n')
    write(media / "config/jaeger-config.yml", 'disabled: false\nreporter:\n  localAgentHostPort: "jaeger:6831"\n')
    frontend = media / "docker/openresty-thrift"
    write(frontend / ".dockerignore", ".git\n")
    write(frontend / "COPYRIGHT", "OpenResty copyright fixture\n")
    write(frontend / "lua-thrift/src/longnumberutils.c", "// Lua long fixture\n")
    write(frontend / "lua-json/json.lua", "return {}\n")
    write(frontend / "lua-bridge-tracer/src/module.cpp", "// tracer fixture\n")
    write(frontend / "lua-bridge-tracer/LICENSE", "MIT tracer fixture\n")
    write(frontend / "lua-bridge-tracer/example/private-example.txt", "not a runtime input\n")
    write(frontend / ".travis.yml", "not a runtime input\n")
    write(frontend / "nginx.conf", "# base builder default\n")
    write(frontend / "nginx.vh.default.conf", "# base builder default\n")
    write(frontend / "xenial/Dockerfile", 'ARG RESTY_IMAGE_BASE="ubuntu"\nARG RESTY_IMAGE_TAG="xenial"\nFROM ${RESTY_IMAGE_BASE}:${RESTY_IMAGE_TAG}\n'
          + "RUN curl http://ftp.cs.stanford.edu/pub/exim/pcre/pcre-${RESTY_PCRE_VERSION}.tar.gz\n"
          + social.OPENRESTY_ROCKS_ORIGINAL + "\n")
    write(media / "gen-lua/media_service_ttypes.lua", 'return {Cast={character="fixture"}}\n')
    write(media / "nginx-web-server/lua-scripts/wrk2-api/movie-info/write.lua", 'new_cast["charactor"]=cast["charactor"]\n')
    write(media / "nginx-web-server/lua-scripts/wrk2-api/review/compose.lua", 'local k8s_suffix = os.getenv("fqdn_suffix")\n')
    write(media / "nginx-web-server/jaeger-config.json", '{"service_name":"nginx","diabled": false,"reporter":{"logSpans":true,"localAgentHostPort":"jaeger:6831"},"sampler":{"type":"const","param":"1"}}\n')
    write(media / "helm-chart/mediamicroservices/templates/configs/nginx/nginx.tpl", '{{- define "mediamicroservices.templates.nginx.nginx.conf"  }}\n'
          + "worker_processes  auto;\nerror_log  logs/error.log;\nenv fqdn_suffix;\nevents {\n  worker_connections  1024;\n}\nhttp {\n"
          + "  # Docker default hostname resolver\n  # resolver 127.0.0.11 ipv6=off;\n"
          + "  resolver {{ .Values.global.nginx.resolverName }} ipv6=off;\n  server {\n    listen 8080;\n    lua_need_request_body on;\n  }\n}\n{{- end}}\n")
    write(media / "scripts/write_movie_info.py", INITIALIZER)
    for name, endpoint in (("register_users.sh", "user"), ("register_movies.sh", "movie")):
        write(media / "scripts" / name, '#!/usr/bin/env bash\nfor i in {1..1000}; do\n  curl -d "fixture=$i" http://127.0.0.1:8080/wrk2-api/' + endpoint + '/register\ndone\n')
    write(media / "datasets/tmdb/casts.json", '[{"id":1}]\n')
    write(media / "datasets/tmdb/movies.json", '[{"id":1,"title":"fixture"}]\n')
    write(media / "docker-compose.yml", 'dns-media:\n  volumes: ["/var/run/docker.sock:/var/run/docker.sock"]\n')
    write(media / "keys/server.key", "PRIVATE KEY demo must be excluded\n")
    write(media / "wrk2/scripts/media-microservices/compose-review.lua", "math.randomseed(os.time())\n")
    git(root, "init", "--quiet")
    git(root, "add", ".")
    git(root, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "fixture")
    anchors = {relative: common.sha256(root / relative) for relative in prepare.UPSTREAM_ANCHORS}
    return git(root, "rev-parse", "HEAD"), anchors


class FakeResponse:
    def __init__(self, failure=False):
        self.failure = failure
        self.checked = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *arguments):
        return False

    def raise_for_status(self):
        self.checked = True
        if self.failure:
            raise RuntimeError("fixture HTTP 500")

    async def text(self):
        if not self.checked:
            raise AssertionError("HTTP body read without status validation")
        return "ok"


class FakeSession:
    def __init__(self, calls, failure=False):
        self.calls = calls
        self.failure = failure

    async def __aenter__(self):
        return self

    async def __aexit__(self, *arguments):
        return False

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.failure)


class DeathStarBenchMediaPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.upstream = self.root / "upstream"
        self.upstream.mkdir()
        revision, self.anchors = fixture(self.upstream)
        self.revision_patch = mock.patch.object(common, "UPSTREAM_REVISION", revision)
        self.revision_patch.start()
        self.addCleanup(self.revision_patch.stop)
        self.anchor_patch = mock.patch.object(prepare, "UPSTREAM_ANCHORS", self.anchors)
        self.anchor_patch.start()
        self.addCleanup(self.anchor_patch.stop)

    def prepare(self, name="contexts"):
        output = self.root / name
        return prepare.prepare_contexts(self.upstream, output), output

    def test_only_media_app_and_frontend_image_contexts_are_prepared(self):
        manifest, output = self.prepare()
        self.assertEqual(set(manifest["contexts"]), {"app", "frontend"})
        self.assertEqual(manifest["workload_id"], "media_microservices")
        self.assertTrue(manifest["candidate_only"])
        self.assertFalse(manifest["released"])
        self.assertFalse(manifest["byte_reproducible_rebuild"])
        self.assertTrue(manifest["immutable_base_images_verified"])
        self.assertFalse(manifest["initializer"]["executed"])
        self.assertFalse(manifest["initializer"]["dataset_ready"])
        self.assertTrue(manifest["initializer"]["requires_journal_and_semantic_readiness"])
        self.assertGreaterEqual(len(manifest["build_risks"]), 5)
        self.assertNotIn("images", manifest)
        self.assertEqual(json.loads((output / "context-manifest.json").read_text()), manifest)
        app = (output / "app/Dockerfile.candidate").read_text()
        self.assertIn("AS media-dependencies", app)
        self.assertIn("FROM media-dependencies", app)
        self.assertIn("ARG LIB_MONGOC_VERSION=1.14.0", app)
        self.assertNotIn("1.15.0", app)
        self.assertNotIn("yg397/thrift-microservice-deps", app)
        self.assertNotIn("social-network-microservices", app)
        self.assertIn("/media-microservices/config/service-config.json", app)
        self.assertIn("/usr/share/licenses/deathstarbench/LICENSE", app)

    def test_native_portable_recipes_use_verified_bases_and_checksum_locked_sources(self):
        manifest, output = self.prepare()
        self.assertTrue(manifest["native_build_required"])
        self.assertTrue(manifest["portable_cpu_build"])
        self.assertTrue(manifest["build_network_required"])
        self.assertTrue(manifest["build_source_archives_checksum_pinned"])
        self.assertFalse(manifest["byte_reproducible_rebuild"])
        self.assertEqual(manifest["base_images"]["ubuntu_xenial"]["reference"], prepare.UBUNTU_BASE)
        self.assertEqual(set(manifest["base_images"]["ubuntu_xenial"]["platform_digests"]), {"linux/amd64", "linux/arm64"})
        for name in ("app", "frontend"):
            recipe = (output / name / "Dockerfile.candidate").read_text()
            self.assertIn(prepare.UBUNTU_BASE, recipe)
            self.assertIn(prepare.NATIVE_BUILD_GUARD, recipe)
            self.assertNotIn("ubuntu:16.04", recipe)
            self.assertNotIn("-march=native", recipe)
            self.assertNotIn("-mcpu=native", recipe)
            self.assertNotIn("git clone", recipe)
            build = output / name / "build"
            lock = json.loads((build / "source-lock.json").read_text())
            environment = (build / "source.env").read_text()
            self.assertTrue(lock["candidate_only"])
            for source, (url, digest) in prepare.BUILD_SOURCES.items():
                self.assertEqual(lock["sources"][source], {"url": url, "sha256": digest})
                self.assertIn(url, environment)
                self.assertIn(digest, environment)
            self.assertEqual(manifest["contexts"][name]["build_source_lock_sha256"], common.sha256(build / "source-lock.json"))
            self.assertEqual(manifest["contexts"][name]["build_inputs_sha256"], common.tree_sha256(build))
            dependencies = (build / "build-dependencies.sh").read_text()
            self.assertIn("-DHUNTER_ENABLED=OFF", dependencies)
            self.assertIn("-DBUILD_STATIC_LIBS=ON", dependencies)
            self.assertIn("-DBUILD_SHARED_LIBS=ON", dependencies)
            self.assertIn("tacopie-243089d84a5a8032b85e81cae237b823df99abee", dependencies)
            self.assertIn("printf '%s  %s\\n'", dependencies)
            self.assertLess(dependencies.index("sha256sum -c -"), dependencies.index('tar xzf "$name.tar.gz"'))
            self.assertNotIn("git clone", dependencies)
            self.assertNotIn("pip3 install", dependencies)
            openresty = (build / "build-openresty.sh").read_text()
            self.assertLess(openresty.index("sha256sum -c -"), openresty.index('tar xzf "$name.tar.gz"'))
            self.assertIn('cp README.markdown "$licenses/hmac/README.markdown"', openresty)
            self.assertNotIn("raw.githubusercontent.com", openresty)
            self.assertIn("install lib/resty/*.lua", openresty)
            for script in (build / "build-dependencies.sh", build / "build-openresty.sh"):
                subprocess.run(["sh", "-n", str(script)], check=True, capture_output=True)

    def test_runtime_bakes_media_sources_configuration_namespace_and_licenses(self):
        manifest, output = self.prepare()
        self.assertFalse(manifest["runtime_git_clone"])
        self.assertTrue(manifest["runtime_source_baked_into_images"])
        self.assertEqual(manifest["namespace"], "deathstarbench-media")
        frontend = (output / "frontend/Dockerfile.candidate").read_text()
        self.assertIn('ENV fqdn_suffix=".deathstarbench-media.svc.cluster.local"', frontend)
        self.assertIn("COPY runtime/gen-lua /gen-lua", frontend)
        self.assertIn("COPY runtime/lua-scripts", frontend)
        self.assertIn(social.OPENRESTY_JWT_SHA256, frontend)
        self.assertIn("--deps-mode=none", frontend)
        self.assertNotIn("luarocks install long", frontend)
        self.assertNotIn("ftp.cs.stanford.edu", frontend)
        nginx = (output / "frontend/runtime/nginx.conf").read_text()
        self.assertIn("resolver 10.43.0.10 valid=10s ipv6=off;", nginx)
        self.assertIn("client_body_buffer_size 64k;", nginx)
        self.assertIn("worker_processes  auto;", nginx)
        self.assertIn("worker_connections  16384;", nginx)
        self.assertIn("worker_rlimit_nofile 65536;", nginx)
        self.assertNotIn("127.0.0.11", nginx)
        self.assertNotIn("{{", nginx)
        self.assertIn("env fqdn_suffix;", nginx)
        self.assertIn("error_log stderr warn;", nginx)
        self.assertIn("pid /tmp/nginx.pid;", nginx)
        for directory in ("client-body", "proxy", "fastcgi", "scgi", "uwsgi"):
            self.assertIn(f"/tmp/{directory};", nginx)
        self.assertEqual(manifest["frontend_runtime_writable_paths"], ["/tmp"])
        handler = (output / "frontend/runtime/lua-scripts/wrk2-api/movie-info/write.lua").read_text()
        self.assertIn('new_cast["character"]=cast["character"]', handler)
        self.assertNotIn("charactor", handler)
        tracing = json.loads((output / "frontend/runtime/jaeger-config.json").read_text())
        self.assertFalse(tracing["disabled"])
        self.assertFalse(tracing["reporter"]["logSpans"])
        self.assertEqual(tracing["reporter"]["localAgentHostPort"], "jaeger:6831")
        self.assertEqual(tracing["sampler"], {"type": "const", "param": 1})
        for name in ("app", "frontend", "initializer"):
            self.assertEqual((output / name / prepare.LICENSE_NAME).read_bytes(), (self.upstream / "LICENSE").read_bytes())
        self.assertTrue((output / "app/third_party/PicoSHA2/LICENSE").is_file())
        self.assertTrue((output / "frontend/COPYRIGHT").is_file())
        self.assertTrue((output / "frontend/lua-bridge-tracer/LICENSE").is_file())

    def test_contexts_exclude_compose_helm_dns_private_keys_and_unneeded_sources(self):
        _, output = self.prepare()
        for name in ("app", "frontend", "initializer"):
            paths = [path.relative_to(output / name).as_posix() for path in (output / name).rglob("*") if path.is_file()]
            for path in paths:
                self.assertNotIn(".git/", path)
                self.assertNotIn("helm-chart/", path)
                self.assertNotIn("docker-compose", path)
                self.assertNotIn("keys/", path)
                self.assertNotIn("example/", path)
                self.assertNotIn(".travis", path)
                self.assertFalse(path.endswith(".key"))
        self.assertFalse((output / "app/datasets").exists())
        self.assertFalse((output / "app/scripts").exists())
        self.assertFalse((output / "app/wrk2").exists())
        self.assertEqual(sorted(path.name for path in (output / "initializer/scripts").iterdir()), ["register_movies.sh", "register_users.sh", "write_movie_info.py"])

    def test_identical_inputs_produce_deterministic_context_recipe_and_source_receipts(self):
        first, first_output = self.prepare("first")
        second, second_output = self.prepare("second")
        self.assertEqual(first, second)
        self.assertEqual((first_output / "context-manifest.json").read_bytes(), (second_output / "context-manifest.json").read_bytes())
        self.assertEqual(first["original_source_tree_sha256"], common.tree_sha256(self.upstream / prepare.SOURCE_ROOT))
        for name, receipt in first["contexts"].items():
            self.assertEqual(receipt["context_sha256"], common.tree_sha256(first_output / name))
            self.assertEqual(receipt["recipe_sha256"], common.sha256(first_output / name / "Dockerfile.candidate"))
            self.assertEqual(receipt["license_sha256"], common.sha256(self.upstream / "LICENSE"))
        for receipt in first["prepared_source_receipts"].values():
            self.assertEqual(receipt["original_sha256"], common.sha256(self.upstream / receipt["original_path"]))
            self.assertEqual(receipt["prepared_sha256"], common.sha256(first_output / receipt["prepared_path"]))
            self.assertNotEqual(receipt["original_sha256"], receipt["prepared_sha256"])

    def test_context_hashes_are_independent_of_preparer_umask(self):
        first, _ = self.prepare("normal-mask")
        original_mask = os.umask(0o077)
        try:
            second, _ = self.prepare("restrictive-mask")
        finally:
            os.umask(original_mask)
        self.assertEqual(first, second)

    def test_dirty_wrong_revision_and_ignored_private_input_fail_before_creation(self):
        write(self.upstream / prepare.SOURCE_ROOT / "src/service.cpp", "dirty\n")
        with self.assertRaises(prepare.PreparationError):
            self.prepare()
        self.assertFalse((self.root / "contexts").exists())
        git(self.upstream, "restore", "--", f"{prepare.SOURCE_ROOT}/src/service.cpp")
        with mock.patch.object(common, "UPSTREAM_REVISION", "0" * 40):
            with self.assertRaises(prepare.PreparationError):
                self.prepare()
        write(self.upstream / ".gitignore", "secret.key\n")
        git(self.upstream, "add", ".gitignore")
        git(self.upstream, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "ignore fixture")
        write(self.upstream / prepare.SOURCE_ROOT / "secret.key", "ignored private input\n")
        self.assertEqual(git(self.upstream, "status", "--porcelain"), "")
        with mock.patch.object(common, "UPSTREAM_REVISION", git(self.upstream, "rev-parse", "HEAD")):
            with self.assertRaises(prepare.PreparationError):
                self.prepare()
        self.assertFalse((self.root / "contexts").exists())

    def test_source_hash_drift_is_rejected_independently_of_clean_git_revision(self):
        relative = f"{prepare.SOURCE_ROOT}/config/service-config.json"
        write(self.upstream / relative, '{"modified":true}\n')
        git(self.upstream, "add", relative)
        git(self.upstream, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "drift")
        with mock.patch.object(common, "UPSTREAM_REVISION", git(self.upstream, "rev-parse", "HEAD")):
            with self.assertRaises(prepare.PreparationError):
                self.prepare()

    def test_patch_drift_removes_only_new_partial_output(self):
        for old, replacement in ((
            'new_cast["charactor"]=cast["charactor"]', "already-fixed",
        ), (
            'new_cast["charactor"]=cast["charactor"]',
            'new_cast["charactor"]=cast["charactor"]\nnew_cast["charactor"]=cast["charactor"]',
        )):
            with self.subTest(replacement=replacement):
                with mock.patch.object(prepare, "validate_tracked_source", return_value=self.upstream):
                    handler = self.upstream / prepare.SOURCE_ROOT / "nginx-web-server/lua-scripts/wrk2-api/movie-info/write.lua"
                    write(handler, replacement + "\n")
                    with self.assertRaises(prepare.PreparationError):
                        self.prepare()
                    self.assertFalse((self.root / "contexts").exists())
                write(handler, old + "\n")
        existing = self.root / "existing"
        write(existing / "valuable.txt", "keep me\n")
        with self.assertRaises(prepare.PreparationError):
            prepare.prepare_contexts(self.upstream, existing)
        self.assertEqual((existing / "valuable.txt").read_text(), "keep me\n")
        with self.assertRaises(prepare.PreparationError):
            prepare.prepare_contexts(self.upstream, self.upstream / "output")

    def test_initializer_shell_scripts_fail_closed_and_require_explicit_target(self):
        manifest, output = self.prepare()
        for name in ("register_users.sh", "register_movies.sh"):
            path = output / "initializer/scripts" / name
            text = path.read_text()
            self.assertIn("set -euo pipefail", text)
            self.assertIn("curl --fail --silent --show-error --output /dev/null", text)
            self.assertIn("${DEATHSTARBENCH_TARGET_URL:?", text)
            self.assertIn('"${DEATHSTARBENCH_TARGET_URL%/}"', text)
            self.assertNotIn("127.0.0.1", text)
            subprocess.run(["bash", "-n", str(path)], check=True, capture_output=True)
            result = subprocess.run(["bash", str(path)], env={"PATH": "/usr/bin:/bin"}, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("DEATHSTARBENCH_TARGET_URL", result.stderr)
        self.assertFalse(manifest["initializer"]["executed"])

    def _initializer_module(self, path, calls, failure=False):
        fake_aiohttp = types.SimpleNamespace(
            TCPConnector=lambda **kwargs: object(),
            ClientSession=lambda **kwargs: FakeSession(calls, failure),
        )
        specification = importlib.util.spec_from_file_location("prepared_media_initializer_fixture", path)
        module = importlib.util.module_from_spec(specification)
        with mock.patch.dict("sys.modules", {"aiohttp": fake_aiohttp}):
            specification.loader.exec_module(module)
        return module

    def test_prepared_initializer_normalizes_null_posters_and_retains_first_title_mapping(self):
        _, output = self.prepare()
        calls = []
        module = self._initializer_module(output / "initializer/scripts/write_movie_info.py", calls)
        movies = [
            {"id": 10, "title": "Same", "poster_path": None, "overview": "first"},
            {"id": 11, "title": "Same", "poster_path": "poster", "overview": "second"},
            {"id": 12, "title": "Other", "poster_path": "other", "overview": "third"},
        ]
        asyncio.run(module.write_movie_info("http://10.0.0.5:8080", movies))
        registrations = [kwargs["data"] for url, kwargs in calls if url.endswith("movie/register")]
        self.assertEqual(registrations, [{"title": "Same", "movie_id": "10"}, {"title": "Other", "movie_id": "12"}])
        uploads = [kwargs["json"] for url, kwargs in calls if url.endswith("movie-info/write")]
        self.assertEqual(len(uploads), 3)
        self.assertEqual([movie["thumbnail_ids"] for movie in uploads], [[], ["poster"], ["other"]])
        self.assertEqual(len([url for url, _ in calls if url.endswith("plot/write")]), 3)

    def test_all_four_prepared_http_handlers_propagate_failure(self):
        _, output = self.prepare()
        module = self._initializer_module(output / "initializer/scripts/write_movie_info.py", [], failure=True)
        for name, item in (("upload_cast_info", {}), ("upload_plot", {}), ("upload_movie_info", {}), ("register_movie", {"title": "title", "movie_id": "1"})):
            with self.subTest(handler=name):
                with self.assertRaisesRegex(RuntimeError, "HTTP 500"):
                    asyncio.run(getattr(module, name)(FakeSession([], failure=True), "http://10.0.0.5:8080", item))

    def test_initializer_patch_requires_exactly_four_http_anchors(self):
        with self.assertRaises(prepare.PreparationError):
            prepare._patch_initializer(INITIALIZER.replace(" as resp:\n    return await resp.text()", " as resp:\n    return 'changed'", 1))
        with self.assertRaises(prepare.PreparationError):
            prepare._patch_initializer(INITIALIZER + "\n# unexpected as resp:\n    return await resp.text()\n")


if __name__ == "__main__":
    unittest.main()
