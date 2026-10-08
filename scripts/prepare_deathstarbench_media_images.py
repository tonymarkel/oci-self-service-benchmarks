#!/usr/bin/env python3
"""Prepare unreleased Media Microservices image contexts and initializer sources.

This is an offline source-preparation step, not an image builder, publisher,
dataset initializer or release attestation. Runtime source is baked into the
contexts. Base images and build source archives are immutable, but mutable
Xenial package repositories still prevent byte-reproducible image rebuilds.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Support both direct CLI execution and normal package imports without masking
# an import-time error as a second, misleading fallback import attempt.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.deathstarbench_artifact_common import (
    PreparationError, UPSTREAM_REPOSITORY, UPSTREAM_REVISION,
    candidate_output, copy_tracked_tree, replace_exact, sha256,
    tree_sha256, validate_tracked_source,
)
from scripts.prepare_deathstarbench_social_images import (
    K3S_CLUSTER_DNS_IP, OPENRESTY_ROCKS_ORIGINAL, OPENRESTY_ROCKS_PINNED,
    THIRD_PARTY_IMAGES,
)


SOURCE_ROOT = "mediaMicroservices"
NAMESPACE = "deathstarbench-media"
PREPARATION_REVISION = "media-microservices-6ecb097-contexts-candidate-v2"
INITIALIZER_REVISION = "media-microservices-6ecb097-initializer-candidate-v1"
LICENSE_NAME = "LICENSE.deathstarbench"
LICENSE_DESTINATION = "/usr/share/licenses/deathstarbench/LICENSE"
BUILD_ASSETS = Path(__file__).resolve().parent / "assets/deathstarbench/media"

# Verified against the official Docker registry's raw manifest bytes. The
# immutable index selects the corresponding native platform image; tags are
# not consulted by the builder. These are identities, not security approvals
# for Xenial, which remains an old, explicitly gated benchmark dependency.
UBUNTU_BASE = "docker.io/library/ubuntu@sha256:1f1a2d56de1d604801a9671f301190704c25d604a416f59e03c04f5c6ffee0d6"
UBUNTU_PLATFORM_DIGESTS = {
    "linux/amd64": "sha256:a3785f78ab8547ae2710c89e627783cfa7ee7824d3468cae6835c9f4eae23ff7",
    "linux/arm64": "sha256:f4c51ba054967fd4b06715f1b67078efbe9ca152e8be98d8f3c1f4d08c6042f8",
}

# Each archive was downloaded from the upstream project's release endpoint,
# hashed independently, and inspected before recording this lock. cpp_redis's
# tacopie entry is its exact 4.3.1 gitlink, not the submodule's current HEAD.
# Fetches remain build-time operations, with a checksum check before tar.
BUILD_SOURCES = {
    "mongo": ("https://github.com/mongodb/mongo-c-driver/releases/download/1.14.0/mongo-c-driver-1.14.0.tar.gz", "ebe9694f7fa6477e594f19507877bbaa0b72747682541cf0cf9a6c29187e97e8"),
    "thrift": ("https://codeload.github.com/apache/thrift/tar.gz/refs/tags/v0.12.0", "b7452d1873c6c43a580d2b4ae38cfaf8fa098ee6dc2925bae98dce0c010b1366"),
    "json": ("https://codeload.github.com/nlohmann/json/tar.gz/refs/tags/v3.6.1", "80c45b090e40bf3d7a7f2a6e9f36206d3ff710acfa8d8cc1f8c763bb3075e22e"),
    "yaml": ("https://codeload.github.com/jbeder/yaml-cpp/tar.gz/refs/tags/yaml-cpp-0.6.2", "e4d8560e163c3d875fd5d9e5542b5fd5bec810febdcba61481fe5fc4e6b1fd05"),
    "opentracing": ("https://codeload.github.com/opentracing/opentracing-cpp/tar.gz/refs/tags/v1.5.1", "015c4187f7a6426a2b5196f0ccd982aa87f010cf61f507ae3ce5c90523f92301"),
    "jaeger": ("https://codeload.github.com/jaegertracing/jaeger-client-cpp/tar.gz/refs/tags/v0.4.2", "21257af93a64fee42c04ca6262d292b2e4e0b7b0660c511db357b32fd42ef5d3"),
    "jwt": ("https://codeload.github.com/arun11299/cpp-jwt/tar.gz/refs/tags/v1.1.1", "6dbf93969ec48d97ecb6c157014985846df8c01995a0011c21f4e2c146594922"),
    "redis": ("https://codeload.github.com/cpp-redis/cpp_redis/tar.gz/bbe38a7f83de943ffcc90271092d689ae02b3489", "c138a5517cbb579d059611538e1ffa0e0204d78609b937ab9a80933678c80d89"),
    "tacopie": ("https://codeload.github.com/cpp-redis/tacopie/tar.gz/243089d84a5a8032b85e81cae237b823df99abee", "19a6af00b00a57172907fbf361f8acb7779f3812312a7060fdb52c4badaf4b95"),
    "hmac": ("https://codeload.github.com/jkeys089/lua-resty-hmac/tar.gz/23da759b69f208576526c8ac21b7c5ad66740321", "e15c6441d29f4289ed1ec205363c19f7cce989a95a70a650b6e4d0b2a2a57126"),
    "openssl": ("https://www.openssl.org/source/old/1.1.0/openssl-1.1.0j.tar.gz", "31bec6c203ce1a8e93d5994f4ed304c63ccf07676118b6634edded12ad1b3246"),
    "pcre": ("https://ftp.exim.org/pub/pcre/pcre-8.42.tar.gz", "69acbc2fbdefb955d42a4c606dfde800c2885711d2979e356c0636efde9ec3b5"),
    "nginx": ("https://codeload.github.com/opentracing-contrib/nginx-opentracing/tar.gz/refs/tags/v0.8.0", "b2159297814d5df153cf45f355bcd8ffdb71f2468e8149ad549d4f9c0cdc81ad"),
    "openresty": ("https://openresty.org/download/openresty-1.15.8.1rc1.tar.gz", "dfbb0038fd5829014efaf1ab7f269e60ebcf7d53220262a96b4c2b9e42ea7226"),
    "luarocks": ("https://luarocks.github.io/luarocks/releases/luarocks-3.5.0.tar.gz", "701d0cc0c7e97cc2cf2c2f4068fce45e52a8854f5dc6c9e49e2014202eec9a4f"),
}
NATIVE_BUILD_GUARD = (
    "ARG BUILDPLATFORM\nARG TARGETPLATFORM\n"
    'RUN test -n "${BUILDPLATFORM}" && test "${BUILDPLATFORM}" = "${TARGETPLATFORM}" \\\n'
    '    && case "${TARGETPLATFORM}" in linux/amd64|linux/arm64) ;; *) exit 1 ;; esac\n'
)

# Hashes of the original pinned files, not guessed hashes of patched outputs.
UPSTREAM_ANCHORS = {
    "LICENSE": "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4",
    f"{SOURCE_ROOT}/.dockerignore": "15601746875d1296b3e8b48dbed887700f5f425702a143f76e8893f3135ce75c",
    f"{SOURCE_ROOT}/CMakeLists.txt": "a70ff4016724e1d64230560e528b3fa6cea2c02ea6aad84b8596c26df5e78da5",
    f"{SOURCE_ROOT}/Dockerfile": "0322b8e036091c887cab74d3b49cb84a469f1d26f732c22fc3c0bedb979c9563",
    f"{SOURCE_ROOT}/docker/thrift-microservice-deps/cpp/Dockerfile": "25114f38329d54e11a6b3782b8332153b37d9b6df9725a636c9d433502244016",
    f"{SOURCE_ROOT}/docker/openresty-thrift/xenial/Dockerfile": "5319eb130887b29a020503c686f10e35f5e9bcc7cd6ead95237e57567dc4f18a",
    f"{SOURCE_ROOT}/config/service-config.json": "558a19ce892498f716988209627da08d2f5e7a5c8a8f1afa24f268040c5c95dc",
    f"{SOURCE_ROOT}/config/jaeger-config.yml": "cff14dc9d8ced8c0bfafad247b05f0a4fb8c744e4c404fcdf54acece452e6ee1",
    f"{SOURCE_ROOT}/helm-chart/mediamicroservices/templates/configs/nginx/nginx.tpl": "31c1d9359964c80daadc0649e1dcbc7573b370739d038452749cbf0799e79889",
    f"{SOURCE_ROOT}/nginx-web-server/jaeger-config.json": "a92a36fc877c8b3e1967c5c543cc7030f72c0659320ab7108a70a0f2e0b19703",
    f"{SOURCE_ROOT}/nginx-web-server/lua-scripts/wrk2-api/movie-info/write.lua": "8872cc5830aedb4c3641eb31fadb3661e9e0816d6f8c0bef692a586e9cee50ce",
    f"{SOURCE_ROOT}/scripts/write_movie_info.py": "de6f06f7e809361b00cfa33f40365a0d5ae7f84a69bd5d2120d63eadcd91b938",
    f"{SOURCE_ROOT}/scripts/register_users.sh": "0de03dd5bfb1e786dd0fb766d137dd20b4b505694c17be3b793206b360519c56",
    f"{SOURCE_ROOT}/scripts/register_movies.sh": "911554a01a5af5fe5a8358cdfa5473aa8662058e66627dac29d028328162c0b5",
    f"{SOURCE_ROOT}/datasets/tmdb/casts.json": "d6de92717fcdaf9e16e7c499a47ae9925a0c3b32d1459ce6c992d7ac8d0e968d",
    f"{SOURCE_ROOT}/datasets/tmdb/movies.json": "b9085236dc7d3b04e07b72a9d4b77cf447dde93ce263a00c1e2dfd82ff2b8fb5",
}

PATCHED_SOURCE_PATHS = {
    "movie_info_handler": (
        "nginx-web-server/lua-scripts/wrk2-api/movie-info/write.lua",
        "frontend/runtime/lua-scripts/wrk2-api/movie-info/write.lua",
    ),
    "frontend_nginx": (
        "helm-chart/mediamicroservices/templates/configs/nginx/nginx.tpl",
        "frontend/runtime/nginx.conf",
    ),
    "frontend_jaeger": (
        "nginx-web-server/jaeger-config.json", "frontend/runtime/jaeger-config.json",
    ),
    "movie_initializer": (
        "scripts/write_movie_info.py", "initializer/scripts/write_movie_info.py",
    ),
    "register_users": ("scripts/register_users.sh", "initializer/scripts/register_users.sh"),
    "register_movies": ("scripts/register_movies.sh", "initializer/scripts/register_movies.sh"),
}

BUILD_RISKS = (
    "Xenial apt repositories are mutable and package versions are not fully pinned.",
    "The pinned Xenial and legacy library versions are not a modern production-security baseline.",
    "Native amd64 and arm64 image builds and architecture smoke tests are required before qualification.",
    "Supporting image tags still require verified platform digests before runtime qualification.",
    "No images were built, published, anonymously pulled, or smoke-tested by this preparation step.",
    "Initializer sources are prepared only; no dataset or semantic readiness is attested.",
)


def _write_build_inputs(destination: Path) -> None:
    build = destination / "build"
    build.mkdir()
    for name in ("build-dependencies.sh", "build-openresty.sh"):
        (build / name).write_bytes((BUILD_ASSETS / name).read_bytes())
    (build / "source.env").write_text("".join(
        f"{name.upper()}_URL='{url}'\n{name.upper()}_SHA256='{digest}'\n"
        for name, (url, digest) in sorted(BUILD_SOURCES.items())
    ), encoding="utf-8")
    (build / "source-lock.json").write_text(json.dumps({
        "schema_version": 1, "candidate_only": True,
        "sources": {name: {"url": url, "sha256": digest} for name, (url, digest) in BUILD_SOURCES.items()},
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read(upstream: Path, relative: str) -> str:
    return (upstream / SOURCE_ROOT / relative).read_text(encoding="utf-8")


def _copy_file(upstream: Path, relative: str, destination: Path) -> None:
    copy_tracked_tree(upstream, relative, destination)


def _license(upstream: Path, destination: Path) -> None:
    _copy_file(upstream, "LICENSE", destination / LICENSE_NAME)


def _labels() -> str:
    return (
        "\nARG OCI_SOURCE_REPOSITORY\nARG OCI_SOURCE_REVISION\n"
        'LABEL org.opencontainers.image.source="${OCI_SOURCE_REPOSITORY}" \\\n'
        '      org.opencontainers.image.revision="${OCI_SOURCE_REVISION}" \\\n'
        '      org.opencontainers.image.title="DeathStarBench Media Microservices candidate" \\\n'
        '      org.opencontainers.image.licenses="Apache-2.0" \\\n'
        '      io.github.oci-self-service-benchmarks.upstream.revision='
        f'"{UPSTREAM_REVISION}"\n'
    )


def _write_app(upstream: Path, destination: Path) -> None:
    destination.mkdir()
    for name in ("src", "cmake", "gen-cpp", "third_party", "config"):
        copy_tracked_tree(upstream, f"{SOURCE_ROOT}/{name}", destination / name)
    _copy_file(upstream, f"{SOURCE_ROOT}/CMakeLists.txt", destination / "CMakeLists.txt")
    _license(upstream, destination)
    # A deliberately minimal context does not carry datasets, registration
    # scripts, Helm init containers, host-socket DNS helpers or private keys.
    (destination / ".dockerignore").write_text(".git\n", encoding="utf-8")
    original_dependencies = _read(upstream, "docker/thrift-microservice-deps/cpp/Dockerfile")
    if original_dependencies.count("ARG LIB_MONGOC_VERSION=1.14.0") != 1:
        raise PreparationError("Media mongo-c-driver 1.14.0 dependency anchor drifted.")
    _write_build_inputs(destination)
    dependencies = (
        f"FROM {UBUNTU_BASE} AS media-dependencies\n" + NATIVE_BUILD_GUARD
        + "ARG LIB_MONGOC_VERSION=1.14.0\n"
        + 'RUN test "${LIB_MONGOC_VERSION}" = 1.14.0\n'
        + "RUN DEBIAN_FRONTEND=noninteractive apt-get update \\\n"
        + "    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \\\n"
        + "        ca-certificates g++ cmake curl libmemcached-dev automake bison flex \\\n"
        + "        libboost-all-dev libevent-dev libssl-dev libtool make pkg-config\n"
        + "COPY build /tmp/media-build\n"
        + "RUN sh /tmp/media-build/build-dependencies.sh app\n"
        + 'ENV LD_LIBRARY_PATH="/usr/local/lib"\n'
    )
    application = replace_exact(
        _read(upstream, "Dockerfile"),
        "FROM yg397/thrift-microservice-deps:xenial", "FROM media-dependencies",
        label="Media application dependency stage",
    )
    (destination / "Dockerfile.candidate").write_text(
        f"# Candidate source preparation {PREPARATION_REVISION}; NOT released.\n"
        + dependencies.rstrip() + "\n\n" + application.rstrip() + "\n"
        + "COPY ./config/service-config.json /media-microservices/config/service-config.json\n"
        + "COPY ./config/jaeger-config.yml /media-microservices/config/jaeger-config.yml\n"
        + f"COPY {LICENSE_NAME} {LICENSE_DESTINATION}\n"
        + "COPY third_party/PicoSHA2/LICENSE /usr/share/licenses/deathstarbench/PicoSHA2.LICENSE\n"
        + _labels(), encoding="utf-8",
    )


def _render_nginx(upstream: Path) -> str:
    text = _read(upstream, "helm-chart/mediamicroservices/templates/configs/nginx/nginx.tpl")
    header = '{{- define "mediamicroservices.templates.nginx.nginx.conf"  }}\n'
    footer = "{{- end}}\n"
    if not text.startswith(header) or not text.endswith(footer):
        raise PreparationError("Media Nginx Helm wrapper drifted.")
    text = text[len(header):-len(footer)]
    text = replace_exact(
        text, "error_log  logs/error.log;", "error_log stderr warn;",
        label="Media nonroot stderr logging",
    )
    text = replace_exact(
        text, "worker_processes  auto;", "worker_processes  auto;\npid /tmp/nginx.pid;",
        label="Media read-only runtime PID path",
    )
    text = replace_exact(
        text, "http {\n", "http {\n"
        "  client_body_temp_path /tmp/client-body;\n"
        "  proxy_temp_path /tmp/proxy;\n"
        "  fastcgi_temp_path /tmp/fastcgi;\n"
        "  scgi_temp_path /tmp/scgi;\n"
        "  uwsgi_temp_path /tmp/uwsgi;\n",
        label="Media read-only runtime request temporary paths",
    )
    text = replace_exact(
        text, "  resolver {{ .Values.global.nginx.resolverName }} ipv6=off;",
        f"  resolver {K3S_CLUSTER_DNS_IP} valid=10s ipv6=off;",
        label="Media K3s resolver",
    )
    text = replace_exact(
        text, "env fqdn_suffix;", "env fqdn_suffix;\nworker_rlimit_nofile 65536;",
        label="Media frontend environment and file limit",
    )
    text = replace_exact(
        text, "  worker_connections  1024;", "  worker_connections  16384;",
        label="Media frontend worker connections",
    )
    text = replace_exact(
        text, "    lua_need_request_body on;",
        "    lua_need_request_body on;\n    client_body_buffer_size 64k;",
        label="Media in-memory request body buffer",
    )
    text = "\n".join(
        line for line in text.splitlines()
        if "127.0.0.11" not in line and "Docker default hostname resolver" not in line
    ) + "\n"
    if "{{" in text or "}}" in text:
        raise PreparationError("Unrendered Media Helm tokens remain.")
    return text


def _write_frontend(upstream: Path, destination: Path) -> None:
    copy_tracked_tree(
        upstream, f"{SOURCE_ROOT}/docker/openresty-thrift", destination,
        exclude=(".travis.yml", "appveyor.yml", "gen_travis.lua", "lua-bridge-tracer/.circleci", "lua-bridge-tracer/ci", "lua-bridge-tracer/example"),
    )
    _license(upstream, destination)
    runtime = destination / "runtime"
    copy_tracked_tree(upstream, f"{SOURCE_ROOT}/gen-lua", runtime / "gen-lua")
    copy_tracked_tree(upstream, f"{SOURCE_ROOT}/nginx-web-server/lua-scripts", runtime / "lua-scripts")
    handler = runtime / "lua-scripts/wrk2-api/movie-info/write.lua"
    handler.write_text(replace_exact(
        handler.read_text(encoding="utf-8"),
        'new_cast["charactor"]=cast["charactor"]',
        'new_cast["character"]=cast["character"]',
        label="Media movie-info Thrift character field",
    ), encoding="utf-8")
    (runtime / "nginx.conf").write_text(_render_nginx(upstream), encoding="utf-8")
    tracing_text = replace_exact(
        _read(upstream, "nginx-web-server/jaeger-config.json"),
        '"diabled": false', '"disabled": false', label="Media frontend Jaeger disabled flag",
    )
    tracing = json.loads(tracing_text)
    if tracing["reporter"]["localAgentHostPort"] != "jaeger:6831":
        raise PreparationError("Media Jaeger service name drifted.")
    tracing["reporter"]["logSpans"] = False
    tracing["sampler"] = {"type": "const", "param": 1}
    (runtime / "jaeger-config.json").write_text(
        json.dumps(tracing, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    _write_build_inputs(destination)
    original = _read(upstream, "docker/openresty-thrift/xenial/Dockerfile")
    if OPENRESTY_ROCKS_ORIGINAL not in original:
        raise PreparationError("Media OpenResty source-rock stage drifted.")
    source = (BUILD_ASSETS / "Dockerfile.frontend").read_text(encoding="utf-8")
    source = replace_exact(
        source, "@UBUNTU_BASE@", UBUNTU_BASE,
        label="Media immutable OpenResty base",
    )
    source = replace_exact(
        source, f"FROM {UBUNTU_BASE}\n", f"FROM {UBUNTU_BASE}\n" + NATIVE_BUILD_GUARD,
        label="Media native frontend build guard",
    )
    source = replace_exact(
        source, "@JWT_INSTALL@", OPENRESTY_ROCKS_PINNED,
        label="Media checksum-pinned LuaRocks installation",
    )
    source += (
        "\n# Bake the Media runtime; no pod-start Git clone or Helm init container.\n"
        f'ENV fqdn_suffix=".{NAMESPACE}.svc.cluster.local"\n'
        "COPY runtime/gen-lua /gen-lua\n"
        "COPY runtime/lua-scripts /usr/local/openresty/nginx/lua-scripts\n"
        "COPY runtime/nginx.conf /usr/local/openresty/nginx/conf/nginx.conf\n"
        "COPY runtime/jaeger-config.json /usr/local/openresty/nginx/jaeger-config.json\n"
        f"COPY {LICENSE_NAME} {LICENSE_DESTINATION}\n"
        "COPY COPYRIGHT /usr/share/licenses/deathstarbench/OpenResty.COPYRIGHT\n"
        "COPY lua-bridge-tracer/LICENSE /usr/share/licenses/deathstarbench/lua-bridge-tracer.LICENSE\n"
        + _labels()
    )
    (destination / "Dockerfile.candidate").write_text(source, encoding="utf-8")


def _patch_initializer(text: str) -> str:
    text = replace_exact(
        text, 'movie["thumbnail_ids"] = [raw_movie["poster_path"]]',
        'movie["thumbnail_ids"] = ([raw_movie["poster_path"]] if raw_movie["poster_path"] else [])',
        label="Media null poster normalization",
    )
    text = replace_exact(
        text, "async def write_movie_info(addr, raw_movies):\n  idx = 0\n  tasks = []\n",
        "async def write_movie_info(addr, raw_movies):\n  idx = 0\n  tasks = []\n  registered_titles = set()\n",
        label="Media title registration set",
    )
    text = replace_exact(
        text, "      task = asyncio.ensure_future(register_movie(session, addr, movie))\n      tasks.append(task)",
        '      if movie["title"] not in registered_titles:\n'
        '        registered_titles.add(movie["title"])\n'
        "        task = asyncio.ensure_future(register_movie(session, addr, movie))\n"
        "        tasks.append(task)",
        label="Media first-title registration",
    )
    return replace_exact(
        text, " as resp:\n    return await resp.text()",
        " as resp:\n    resp.raise_for_status()\n    return await resp.text()",
        count=4, label="Media strict upload HTTP responses",
    )


def _write_initializer(upstream: Path, destination: Path) -> None:
    destination.mkdir()
    _license(upstream, destination)
    for dataset in ("casts.json", "movies.json"):
        _copy_file(upstream, f"{SOURCE_ROOT}/datasets/tmdb/{dataset}", destination / "datasets/tmdb" / dataset)
    _copy_file(upstream, f"{SOURCE_ROOT}/scripts/write_movie_info.py", destination / "scripts/write_movie_info.py")
    initializer = destination / "scripts/write_movie_info.py"
    initializer.write_text(_patch_initializer(initializer.read_text(encoding="utf-8")), encoding="utf-8")
    for name in ("register_users.sh", "register_movies.sh"):
        text = replace_exact(
            _read(upstream, f"scripts/{name}"), "#!/usr/bin/env bash\n",
            '#!/usr/bin/env bash\nset -euo pipefail\n'
            ': "${DEATHSTARBENCH_TARGET_URL:?Set the validated private frontend URL}"\n',
            label=f"Media strict shell initializer {name}",
        )
        text = replace_exact(
            text, "curl -d", "curl --fail --silent --show-error --output /dev/null -d",
            label=f"Media HTTP fail-closed registration {name}",
        )
        text = replace_exact(
            text, "http://127.0.0.1:8080", '"${DEATHSTARBENCH_TARGET_URL%/}"',
            label=f"Media explicit registration target {name}",
        )
        (destination / "scripts" / name).write_text(text.rstrip() + "\n", encoding="utf-8")


def _normalize_generated_modes(destination: Path) -> None:
    """Do not let the preparer's umask change an otherwise identical context."""
    for path in (destination, *destination.rglob("*")):
        if path.is_symlink():
            continue
        if path.is_dir():
            path.chmod(0o755)
        elif path.is_file():
            path.chmod(0o755 if path.stat().st_mode & 0o111 else 0o644)


def prepare_contexts(upstream: Path, output: Path) -> dict[str, object]:
    upstream = validate_tracked_source(
        upstream, relative_roots=("LICENSE", SOURCE_ROOT), anchors=UPSTREAM_ANCHORS,
    )
    with candidate_output(upstream, output) as destination:
        _write_app(upstream, destination / "app")
        _write_frontend(upstream, destination / "frontend")
        _write_initializer(upstream, destination / "initializer")
        for name in ("app", "frontend", "initializer"):
            _normalize_generated_modes(destination / name)
        source_receipts = {
            name: {
                "original_path": f"{SOURCE_ROOT}/{original}",
                "original_sha256": sha256(upstream / SOURCE_ROOT / original),
                "prepared_path": prepared,
                "prepared_sha256": sha256(destination / prepared),
            }
            for name, (original, prepared) in PATCHED_SOURCE_PATHS.items()
        }
        contexts = {
            name: {
                "path": name, "dockerfile": "Dockerfile.candidate",
                "context_sha256": tree_sha256(destination / name),
                "recipe_sha256": sha256(destination / name / "Dockerfile.candidate"),
                "license_sha256": sha256(destination / name / LICENSE_NAME),
                "build_source_lock_sha256": sha256(destination / name / "build/source-lock.json"),
                "build_inputs_sha256": tree_sha256(destination / name / "build"),
            }
            for name in ("app", "frontend")
        }
        manifest: dict[str, object] = {
            "schema_version": 1, "candidate_only": True, "released": False,
            "workload_id": "media_microservices", "namespace": NAMESPACE,
            "preparation_revision": PREPARATION_REVISION,
            "upstream_repository": UPSTREAM_REPOSITORY,
            "upstream_revision": UPSTREAM_REVISION,
            "original_source_tree_sha256": tree_sha256(upstream / SOURCE_ROOT),
            "original_anchor_sha256": dict(UPSTREAM_ANCHORS),
            "prepared_source_receipts": source_receipts,
            "runtime_git_clone": False, "runtime_source_baked_into_images": True,
            "byte_reproducible_rebuild": False,
            "immutable_base_images_verified": True,
            "base_images": {"ubuntu_xenial": {"reference": UBUNTU_BASE, "platform_digests": dict(UBUNTU_PLATFORM_DIGESTS)}},
            "build_source_archives_checksum_pinned": True,
            "build_network_required": True,
            "native_build_required": True,
            "portable_cpu_build": True,
            "frontend_runtime_writable_paths": ["/tmp"],
            "build_risks": list(BUILD_RISKS), "contexts": contexts,
            "third_party_images": {
                "mongodb": THIRD_PARTY_IMAGES["mongo"],
                **{name: THIRD_PARTY_IMAGES[name] for name in ("redis", "memcached", "jaeger")},
            },
            "initializer": {
                "path": "initializer", "revision": INITIALIZER_REVISION,
                "context_sha256": tree_sha256(destination / "initializer"),
                "license_sha256": sha256(destination / "initializer" / LICENSE_NAME),
                "datasets": {name: sha256(destination / "initializer/datasets/tmdb" / name) for name in ("casts.json", "movies.json")},
                "executed": False, "dataset_ready": False,
                "requires_journal_and_semantic_readiness": True,
            },
        }
        (destination / "context-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8",
        )
        return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    print(json.dumps(prepare_contexts(arguments.upstream, arguments.output), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
