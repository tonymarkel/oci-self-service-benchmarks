#!/usr/bin/env python3
"""Prepare immutable-candidate build contexts for DeathStarBench Social Network.

The input must be a clean checkout of the exact upstream revision.  The
generated frontend contexts bake the files that the upstream Helm chart fetches
from an unpinned Git repository at pod start, so the resulting runtime never
needs Git or network access to source code.

This script deliberately calls the outputs *candidates*.  The pinned upstream
revision still uses mutable package repositories and several downloads without
checksums while building.  Publication plus a platform digest lock makes a
particular runtime immutable; it does not make a later rebuild byte-for-byte
reproducible.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path


UPSTREAM_REPOSITORY = "https://github.com/delimitrou/DeathStarBench.git"
UPSTREAM_REVISION = "6ecb09706140f8730b5385c08f1386c654c3c526"

# These anchors make local modifications, incomplete checkouts, and an
# accidentally changed upstream ref fail before a registry push can begin.
UPSTREAM_ANCHORS = {
    "LICENSE": (
        "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4"
    ),
    "socialNetwork/.dockerignore": (
        "c50eaf011e492ea9d91a75abf8111fe332f17ed4b40fafe647a753ebecea06ca"
    ),
    "socialNetwork/Dockerfile": (
        "a36563b6ee47f2daefba3a9e90f61085d831a799af435275927b1a428c88e5ca"
    ),
    "socialNetwork/docker/thrift-microservice-deps/cpp/Dockerfile": (
        "facf17bb059d11ac6b7f9e74a9c9958c9ae9aafcc29f6e86b7e431fc9b22cb2e"
    ),
    "socialNetwork/docker/openresty-thrift/xenial/Dockerfile": (
        "5319eb130887b29a020503c686f10e35f5e9bcc7cd6ead95237e57567dc4f18a"
    ),
    "socialNetwork/docker/media-frontend/xenial/Dockerfile": (
        "48239524752c1e7c1c2997fd063dafa2575d84e46a3fdeb6bd7ede78d87b3a8b"
    ),
    "socialNetwork/config/service-config.json": (
        "783c9b76cc673f8f583b6fdc02a8f2272a9b183cad24c3edc94267458f689057"
    ),
    "socialNetwork/config/jaeger-config.yml": (
        "ce5f8238f6a63f5f819cf3731be3a89152ce942c9aecf705ec8c06d1b559e272"
    ),
    "socialNetwork/keys/CA.pem": (
        "e625a716b035fd8198e4350e3594fcbeb126599adbc9a124f11acafde35d9021"
    ),
    "socialNetwork/helm-chart/socialnetwork/templates/configs/nginx/nginx.tpl": (
        "76cca192826c20c466ca2b5bb640c4a3296c56558140b755deb07e824a87f8ec"
    ),
    "socialNetwork/helm-chart/socialnetwork/templates/configs/media-service/"
    "nginx.tpl": (
        "28311930be4b918ce681cae18b07d048e9dae992d5aa6b482e0127ad7e4c2a55"
    ),
    "socialNetwork/helm-chart/socialnetwork/charts/nginx-thrift/values.yaml": (
        "825177312373385f539ea933c0388e47e14c8cd231e06e916e45fe6669b5dcbf"
    ),
    "socialNetwork/helm-chart/socialnetwork/charts/media-frontend/values.yaml": (
        "7ff2e1f8d94f537862f983455ad0617635189eb7d0a8013f9b1f0971f0c0b41d"
    ),
}

THIRD_PARTY_IMAGES = {
    # Match the existing compact runner so compact and distributed results use
    # the same supporting software versions.
    "mongo": "docker.io/library/mongo:4.4.6",
    "redis": "docker.io/library/redis:7.2.4",
    "memcached": "docker.io/library/memcached:1.6.26",
    "jaeger": "docker.io/jaegertracing/all-in-one:1.57.0",
}

# Mirrored from app.k3s_runtime.K3S_CLUSTER_DNS_IP.  A cross-contract test
# prevents the image pipeline and the pinned K3s service CIDR from diverging.
K3S_CLUSTER_DNS_IP = "10.43.0.10"

OPENRESTY_JWT_VERSION = "0.2.2-0"
OPENRESTY_JWT_FILENAME = f"lua-resty-jwt-{OPENRESTY_JWT_VERSION}.src.rock"
OPENRESTY_JWT_URL = (
    "https://luarocks.org/manifests/cdbattags/" + OPENRESTY_JWT_FILENAME
)
OPENRESTY_JWT_SHA256 = (
    "8a6bc8a12679953e345da55fe1653f2c581301140a90245e2af971c978e49ca8"
)

# Ubuntu Xenial's Python is 3.5.  An unversioned PyPI install eventually began
# resolving to a PyYAML release whose setup.py uses Python 3.6 syntax.  Use the
# architecture-native package at the exact version published for both Xenial
# amd64 and arm64, then verify the imported module before continuing.
XENIAL_PYYAML_PACKAGE_VERSION = "3.11-3build1"
XENIAL_PYYAML_MODULE_VERSION = "3.11"
PYYAML_INSTALL_ORIGINAL = "  && pip3 install PyYAML \\\n"
PYYAML_INSTALL_PINNED = (
    "  && apt-get install -y --no-install-recommends "
    f"python3-yaml={XENIAL_PYYAML_PACKAGE_VERSION} \\\n"
    f"  && python3 -c 'import yaml; assert yaml.__version__ == "
    f'"{XENIAL_PYYAML_MODULE_VERSION}"\' \\\n'
)

OPENRESTY_ROCKS_ORIGINAL = (
    "RUN luarocks install long \\\n"
    "    && luarocks install lua-resty-jwt \\\n"
    "    && ldconfig"
)
OPENRESTY_ROCKS_PINNED = (
    "RUN curl -fSL --retry 5 --retry-delay 5 \\\n"
    f"        {OPENRESTY_JWT_URL} \\\n"
    f"        -o /tmp/{OPENRESTY_JWT_FILENAME} \\\n"
    f"    && echo \"{OPENRESTY_JWT_SHA256}  /tmp/{OPENRESTY_JWT_FILENAME}\" "
    "| sha256sum -c - \\\n"
    f"    && luarocks install --deps-mode=none /tmp/{OPENRESTY_JWT_FILENAME} "
    "\\\n"
    f"    && rm -f /tmp/{OPENRESTY_JWT_FILENAME} \\\n"
    "    && ldconfig \\\n"
    "    && luajit -e 'assert(require \"liblualongnumber\")' \\\n"
    "    && /usr/local/openresty/bin/resty -e "
    "'assert(require \"resty.jwt\")'"
)

# LuaRocks' root manifest eventually grew beyond Lua 5.1/LuaJIT's limit of
# 65,536 constants. The pinned media frontend previously resolved four rocks
# by name, so an otherwise unchanged image stopped building when that mutable
# manifest crossed the parser limit. Fetch exact source artifacts directly,
# verify every byte, and disable dependency resolution so no manifest is read.
MEDIA_RESTY_MONGOL_REVISION = "697adfe5e63a3a1493b45102a987dc374e3e7560"
MEDIA_RESTY_MONGOL_ARCHIVE = (
    f"resty-mongol-{MEDIA_RESTY_MONGOL_REVISION}.tar.gz"
)
MEDIA_RESTY_MONGOL_URL = (
    "https://codeload.github.com/Olivine-Labs/resty-mongol/tar.gz/"
    f"{MEDIA_RESTY_MONGOL_REVISION}"
)
MEDIA_RESTY_MONGOL_SHA256 = (
    "85ac911d5d0fe2c8073d49cba63158989af7d768c6c789331e59b5d4594172ea"
)
MEDIA_RESTY_MONGOL_ROCKSPEC = "resty-mongol-0.8-4.rockspec"
MEDIA_RESTY_MONGOL_ROCKSPEC_TEXT = '''package = "resty-mongol"
version = "0.8-4"
source = {
  url = "https://github.com/Olivine-Labs/resty-mongol/archive/v0.8.tar.gz",
  dir = "resty-mongol-0.8"
}
description = {
  summary = "Mongo driver for openresty.",
  detailed = [[
  ]],
  homepage = "",
  license = "MIT <http://opensource.org/licenses/MIT>"
}
dependencies = {
  "lua >= 5.1",
  "luacrypto >= 0.3.2"
}
build = {
  type = "builtin",
  modules = {
    ["resty-mongol.init"]        = "src/init.lua",
    ["resty-mongol.colmt"]       = "src/colmt.lua",
    ["resty-mongol.cursor"]      = "src/cursor.lua",
    ["resty-mongol.dbmt"]        = "src/dbmt.lua",
    ["resty-mongol.get"]         = "src/get.lua",
    ["resty-mongol.gridfs"]      = "src/gridfs.lua",
    ["resty-mongol.gridfs_file"] = "src/gridfs_file.lua",
    ["resty-mongol.ll"]          = "src/ll.lua",
    ["resty-mongol.misc"]        = "src/misc.lua",
    ["resty-mongol.object_id"]   = "src/object_id.lua",
    ["resty-mongol.bson"]        = "src/bson.lua",
  }
}
'''

MEDIA_LUACRYPTO_FILENAME = "luacrypto-0.3.2-1.src.rock"
MEDIA_LUACRYPTO_URL = (
    "https://luarocks.org/manifests/luarocks/"
    f"{MEDIA_LUACRYPTO_FILENAME}"
)
MEDIA_LUACRYPTO_SHA256 = (
    "dc935c923b8851208d5d504b343448a9d5bd3e537bb8657875f12d72155600b8"
)

MEDIA_ROCKS_ORIGINAL = (
    "RUN luarocks install resty-mongol --server=http://rocks.moonscript.org \\\n"
    "    && luarocks install luasocket \\\n"
    "    && luarocks install chronos \\\n"
    "    && luarocks install magick"
)


def _media_rocks_pinned() -> str:
    return (
        f"COPY {MEDIA_RESTY_MONGOL_ROCKSPEC} "
        f"/tmp/{MEDIA_RESTY_MONGOL_ROCKSPEC}\n"
        "RUN set -eux; \\\n"
        f"    curl -fSL --retry 5 --retry-delay 5 {MEDIA_RESTY_MONGOL_URL} \\\n"
        f"        -o /tmp/{MEDIA_RESTY_MONGOL_ARCHIVE}; \\\n"
        f"    curl -fSL --retry 5 --retry-delay 5 {MEDIA_LUACRYPTO_URL} \\\n"
        f"        -o /tmp/{MEDIA_LUACRYPTO_FILENAME}; \\\n"
        f'    echo "{MEDIA_RESTY_MONGOL_SHA256}  '
        f'/tmp/{MEDIA_RESTY_MONGOL_ARCHIVE}" | sha256sum -c -; \\\n'
        f'    echo "{MEDIA_LUACRYPTO_SHA256}  '
        f'/tmp/{MEDIA_LUACRYPTO_FILENAME}" | sha256sum -c -; \\\n'
        f"    luarocks install --deps-mode=none /tmp/{MEDIA_LUACRYPTO_FILENAME}; \\\n"
        f"    tar xzf /tmp/{MEDIA_RESTY_MONGOL_ARCHIVE} -C /tmp; \\\n"
        f"    cd /tmp/resty-mongol-{MEDIA_RESTY_MONGOL_REVISION}; \\\n"
        "    luarocks make --deps-mode=none "
        f"/tmp/{MEDIA_RESTY_MONGOL_ROCKSPEC}; \\\n"
        "    ldconfig; \\\n"
        "    /usr/local/openresty/bin/resty -e "
        "'assert(require \"crypto\"); assert(require \"resty-mongol\")'; \\\n"
        f"    rm -rf /tmp/resty-mongol-{MEDIA_RESTY_MONGOL_REVISION} "
        f"/tmp/{MEDIA_RESTY_MONGOL_ARCHIVE} "
        f"/tmp/{MEDIA_RESTY_MONGOL_ROCKSPEC} /tmp/{MEDIA_LUACRYPTO_FILENAME}"
    )

OCI_LABELS = (
    '\nARG OCI_SOURCE_REPOSITORY\n'
    'ARG OCI_SOURCE_REVISION\n'
    'LABEL org.opencontainers.image.source="${OCI_SOURCE_REPOSITORY}" \\\n'
    '      org.opencontainers.image.revision="${OCI_SOURCE_REVISION}" \\\n'
    '      org.opencontainers.image.title="DeathStarBench Social Network" \\\n'
    '      org.opencontainers.image.licenses="Apache-2.0" \\\n'
    '      io.github.oci-self-service-benchmarks.upstream.source='
    '"https://github.com/delimitrou/DeathStarBench" \\\n'
    '      io.github.oci-self-service-benchmarks.upstream.revision='
    f'"{UPSTREAM_REVISION}"\n'
)

UPSTREAM_LICENSE_CONTEXT_NAME = "LICENSE.deathstarbench"
UPSTREAM_LICENSE_IMAGE_PATH = "/usr/share/licenses/deathstarbench/LICENSE"


class PreparationError(RuntimeError):
    """Raised when source validation or context generation fails closed."""


def _run_git(upstream: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(upstream), *arguments],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout.strip()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _replace_once(text: str, old: str, new: str, *, label: str) -> str:
    occurrences = text.count(old)
    if occurrences != 1:
        raise PreparationError(
            f"Expected exactly one {label} anchor; found {occurrences}."
        )
    return text.replace(old, new)


def _validate_upstream(upstream: Path) -> None:
    if not upstream.is_dir():
        raise PreparationError(f"Upstream checkout does not exist: {upstream}")

    try:
        revision = _run_git(upstream, "rev-parse", "HEAD")
        status = _run_git(upstream, "status", "--porcelain=v1", "--untracked-files=all")
    except (OSError, subprocess.CalledProcessError) as exc:
        raise PreparationError("The upstream input must be a Git checkout.") from exc

    if revision != UPSTREAM_REVISION:
        raise PreparationError(
            f"Expected upstream revision {UPSTREAM_REVISION}; found {revision}."
        )
    if status:
        raise PreparationError(
            "The pinned upstream checkout contains local or untracked changes."
        )

    for relative_name, expected_digest in UPSTREAM_ANCHORS.items():
        path = upstream / relative_name
        if not path.is_file():
            raise PreparationError(f"Required upstream anchor is missing: {relative_name}")
        actual_digest = _sha256(path)
        if actual_digest != expected_digest:
            raise PreparationError(
                f"Upstream anchor drift for {relative_name}: expected "
                f"{expected_digest}, found {actual_digest}."
            )


def _patch_legacy_downloads(text: str) -> str:
    return text.replace(
        "http://ftp.cs.stanford.edu/pub/exim/pcre/"
        "pcre-${RESTY_PCRE_VERSION}.tar.gz",
        "https://sourceforge.net/projects/pcre/files/pcre/"
        "${RESTY_PCRE_VERSION}/pcre-${RESTY_PCRE_VERSION}.tar.gz/download",
    )


def _copy_tree(source: Path, destination: Path) -> None:
    if not source.is_dir():
        raise PreparationError(f"Required upstream directory is missing: {source}")
    shutil.copytree(source, destination, symlinks=False)


def _copy_upstream_license(upstream: Path, destination: Path) -> None:
    """Include the exact Apache-2.0 text in the context and final image."""
    source = upstream / "LICENSE"
    if not source.is_file():
        raise PreparationError("The pinned upstream license is missing.")
    shutil.copy2(source, destination / UPSTREAM_LICENSE_CONTEXT_NAME)

    # Make the license explicit even when an inherited Docker ignore file has
    # a broad exclusion.  A file containing only this negation ignores
    # nothing, so creating it for a context without one is harmless.
    dockerignore = destination / ".dockerignore"
    existing = (
        dockerignore.read_text(encoding="utf-8").rstrip()
        if dockerignore.is_file()
        else ""
    )
    include = f"!/{UPSTREAM_LICENSE_CONTEXT_NAME}"
    if include not in existing.splitlines():
        dockerignore.write_text(
            (existing + "\n" if existing else "") + include + "\n",
            encoding="utf-8",
        )


def _license_copy_instruction() -> str:
    return (
        f"COPY {UPSTREAM_LICENSE_CONTEXT_NAME} "
        f"{UPSTREAM_LICENSE_IMAGE_PATH}\n"
    )


def _remove_app_redis_patch(text: str) -> str:
    """Remove the app-stage patch already applied by the combined deps stage."""
    start_anchor = "ARG LIB_REDIS_PLUS_PLUS_VERSION=1.2.3"
    end_anchor = "COPY ./ /social-network-microservices"
    if text.count(start_anchor) != 1 or text.count(end_anchor) != 1:
        raise PreparationError("The Social Network Redis++ patch anchors drifted.")
    start = text.index(start_anchor)
    end = text.index(end_anchor, start)
    removed = text[start:end]
    if removed.count("get_shards_pool") != 1:
        raise PreparationError("The Social Network Redis++ patch body drifted.")
    return text[:start] + text[end:]


def _render_helm_nginx_config(
    template: Path,
    *,
    definition: str,
) -> str:
    """Render the pinned, single-token Helm Nginx template for fixed K3s DNS."""
    text = template.read_text(encoding="utf-8")
    header = f'{{{{- define "{definition}"  }}}}\n'
    footer = "{{- end }}\n"
    if not text.startswith(header) or not text.endswith(footer):
        raise PreparationError(f"The Helm wrapper drifted in {template}.")
    text = text[len(header):-len(footer)]
    resolver = "{{ .Values.global.nginx.resolverName }}"
    text = _replace_once(
        text,
        f"  resolver {resolver} valid=10s ipv6=off;",
        "  # Fixed K3s CoreDNS service IP from the cluster runtime contract.\n"
        f"  resolver {K3S_CLUSTER_DNS_IP} valid=10s ipv6=off;",
        label=f"Kubernetes resolver in {template}",
    )
    # Remove the upstream Docker-only resolver commentary.  The live resolver
    # above is an exact K3s service address, not Docker's embedded DNS server.
    text = "\n".join(
        line
        for line in text.splitlines()
        if "127.0.0.11" not in line
        and "Docker default hostname resolver" not in line
        and "ttl for resolver caching" not in line
    ) + "\n"
    if "{{" in text or "}}" in text:
        raise PreparationError(f"Unresolved Helm template content remains in {template}.")
    if text.count("env fqdn_suffix;") != 1:
        raise PreparationError(f"The fqdn_suffix environment contract drifted in {template}.")
    if text.count(f"resolver {K3S_CLUSTER_DNS_IP} valid=10s ipv6=off;") != 1:
        raise PreparationError(f"The K3s DNS resolver contract drifted in {template}.")
    return text


def _write_app_context(upstream: Path, destination: Path) -> None:
    social = upstream / "socialNetwork"
    _copy_tree(social, destination)
    _copy_upstream_license(upstream, destination)
    # The source repository contains a demo server private key.  It is not a
    # benchmark input and must never enter any generated build context.
    shutil.rmtree(destination / "keys")
    dockerignore = destination / ".dockerignore"
    dockerignore.write_text(
        dockerignore.read_text(encoding="utf-8").rstrip() + "\n!/config/\n",
        encoding="utf-8",
    )

    dependency_source = (
        social / "docker/thrift-microservice-deps/cpp/Dockerfile"
    ).read_text(encoding="utf-8")
    dependency_source = _replace_once(
        dependency_source,
        "FROM ubuntu:16.04",
        "FROM docker.io/library/ubuntu:16.04 AS social-dependencies",
        label="Social Network dependency base image",
    )
    dependency_source = _replace_once(
        dependency_source,
        PYYAML_INSTALL_ORIGINAL,
        PYYAML_INSTALL_PINNED,
        label="unversioned PyYAML installation",
    )

    application_source = (social / "Dockerfile").read_text(encoding="utf-8")
    application_source = _remove_app_redis_patch(application_source)
    application_source = _replace_once(
        application_source,
        "FROM yg397/thrift-microservice-deps:xenial AS builder",
        "FROM social-dependencies AS builder",
        label="Social Network dependency stage",
    )
    application_source = _replace_once(
        application_source,
        "FROM ubuntu:16.04",
        "FROM docker.io/library/ubuntu:16.04",
        label="Social Network runtime base image",
    )

    dockerfile = (
        "# Generated only from DeathStarBench "
        f"{UPSTREAM_REVISION}.\n"
        + dependency_source.rstrip()
        + "\n\n"
        + application_source.rstrip()
        + "\n\n# Bake the exact workload configuration into the final image.\n"
        + "COPY ./config/service-config.json "
        + "/social-network-microservices/config/service-config.json\n"
        + "COPY ./config/jaeger-config.yml "
        + "/social-network-microservices/config/jaeger-config.yml\n"
        + _license_copy_instruction()
        + OCI_LABELS
    )
    if dockerfile.count("get_shards_pool") != 1:
        raise PreparationError(
            "The combined application build must apply the Redis++ patch exactly once."
        )
    (destination / "Dockerfile.candidate").write_text(dockerfile, encoding="utf-8")


def _write_frontend_context(upstream: Path, destination: Path) -> None:
    social = upstream / "socialNetwork"
    _copy_tree(social / "docker/openresty-thrift", destination)
    _copy_upstream_license(upstream, destination)

    runtime = destination / "runtime"
    _copy_tree(social / "gen-lua", runtime / "gen-lua")
    _copy_tree(
        social / "nginx-web-server/lua-scripts", runtime / "lua-scripts"
    )
    _copy_tree(social / "nginx-web-server/pages", runtime / "pages")
    (runtime / "keys").mkdir(parents=True)
    shutil.copy2(social / "keys/CA.pem", runtime / "keys/CA.pem")
    (runtime / "nginx.conf").write_text(
        _render_helm_nginx_config(
            social
            / "helm-chart/socialnetwork/templates/configs/nginx/nginx.tpl",
            definition="socialnetwork.templates.nginx.nginx.conf",
        ),
        encoding="utf-8",
    )
    jaeger_config = (
        social / "nginx-web-server/jaeger-config.json"
    ).read_text(encoding="utf-8")
    jaeger_config = _replace_once(
        jaeger_config,
        '"diabled": false',
        '"disabled": false',
        label="Nginx Jaeger disabled flag",
    )
    (runtime / "jaeger-config.json").write_text(
        jaeger_config,
        encoding="utf-8",
    )

    source = (destination / "xenial/Dockerfile").read_text(encoding="utf-8")
    source = _replace_once(
        source,
        'ARG RESTY_IMAGE_BASE="ubuntu"',
        'ARG RESTY_IMAGE_BASE="docker.io/library/ubuntu"',
        label="OpenResty base image",
    )
    source = _replace_once(
        source,
        OPENRESTY_ROCKS_ORIGINAL,
        OPENRESTY_ROCKS_PINNED,
        label="OpenResty LuaRocks installation",
    )
    source = _patch_legacy_downloads(source)
    source += (
        "\n# Bake the exact runtime assets formerly fetched by an init container.\n"
        "COPY runtime/gen-lua /gen-lua\n"
        "COPY runtime/lua-scripts /usr/local/openresty/nginx/lua-scripts\n"
        "COPY runtime/pages /usr/local/openresty/nginx/pages\n"
        "COPY runtime/keys/CA.pem /keys/CA.pem\n"
        "COPY runtime/nginx.conf /usr/local/openresty/nginx/conf/nginx.conf\n"
        "COPY runtime/jaeger-config.json "
        "/usr/local/openresty/nginx/jaeger-config.json\n"
        + _license_copy_instruction()
        + OCI_LABELS
    )
    (destination / "Dockerfile.candidate").write_text(source, encoding="utf-8")


def _write_media_frontend_context(upstream: Path, destination: Path) -> None:
    social = upstream / "socialNetwork"
    _copy_tree(social / "docker/media-frontend", destination)
    _copy_upstream_license(upstream, destination)

    runtime = destination / "runtime"
    _copy_tree(
        social / "media-frontend/lua-scripts", runtime / "lua-scripts"
    )
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / "nginx.conf").write_text(
        _render_helm_nginx_config(
            social
            / "helm-chart/socialnetwork/templates/configs/media-service/nginx.tpl",
            definition="socialnetwork.templates.media-service.nginx.conf",
        ),
        encoding="utf-8",
    )

    source = (destination / "xenial/Dockerfile").read_text(encoding="utf-8")
    source = _replace_once(
        source,
        "FROM ubuntu:xenial",
        "FROM docker.io/library/ubuntu:xenial",
        label="media frontend base image",
    )
    source = _replace_once(
        source,
        MEDIA_ROCKS_ORIGINAL,
        _media_rocks_pinned(),
        label="media frontend LuaRocks installation",
    )
    source = _patch_legacy_downloads(source)
    (destination / MEDIA_RESTY_MONGOL_ROCKSPEC).write_text(
        MEDIA_RESTY_MONGOL_ROCKSPEC_TEXT,
        encoding="utf-8",
    )
    source += (
        "\n# Bake the exact runtime assets formerly fetched by an init container.\n"
        "COPY runtime/lua-scripts /usr/local/openresty/nginx/lua-scripts\n"
        "COPY runtime/nginx.conf /usr/local/openresty/nginx/conf/nginx.conf\n"
        + _license_copy_instruction()
        + OCI_LABELS
    )
    (destination / "Dockerfile.candidate").write_text(source, encoding="utf-8")


def _tree_digest(directory: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in directory.rglob("*") if item.is_file()):
        relative = path.relative_to(directory).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        data_digest = bytes.fromhex(_sha256(path))
        digest.update(data_digest)
    return digest.hexdigest()


def prepare_contexts(upstream: Path, output: Path) -> dict[str, object]:
    upstream = upstream.resolve()
    output = output.resolve()
    try:
        output.relative_to(upstream)
    except ValueError:
        pass
    else:
        raise PreparationError("The output directory must be outside the checkout.")

    _validate_upstream(upstream)
    if output.exists():
        raise PreparationError(f"Output path already exists: {output}")
    output.mkdir(parents=True)

    writers = {
        "app": _write_app_context,
        "frontend": _write_frontend_context,
        "media_frontend": _write_media_frontend_context,
    }
    contexts: dict[str, dict[str, str]] = {}
    try:
        for name, writer in writers.items():
            destination = output / name
            writer(upstream, destination)
            contexts[name] = {
                "path": name,
                "dockerfile": "Dockerfile.candidate",
                "context_sha256": _tree_digest(destination),
            }

        manifest: dict[str, object] = {
            "schema_version": 1,
            "candidate_only": True,
            "upstream_repository": UPSTREAM_REPOSITORY,
            "upstream_revision": UPSTREAM_REVISION,
            "runtime_git_clone": False,
            "runtime_source_baked_into_images": True,
            "byte_reproducible_rebuild": False,
            "contexts": contexts,
            "third_party_images": THIRD_PARTY_IMAGES,
        }
        (output / "context-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return manifest
    except Exception:
        shutil.rmtree(output, ignore_errors=True)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    manifest = prepare_contexts(arguments.upstream, arguments.output)
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
