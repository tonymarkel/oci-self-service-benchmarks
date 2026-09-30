#!/usr/bin/env python3
"""Prepare the immutable amd64 DeathStarBench wrk2 load-driver context.

The caller supplies a clean, recursively checked-out DeathStarBench tree and
the exact LuaSocket source rock.  This script verifies every source identity
before copying it into a self-contained OCI build context.  Nothing in the
resulting runtime image downloads source or compiles code.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import zipfile
from pathlib import Path


UPSTREAM_REPOSITORY = "https://github.com/delimitrou/DeathStarBench.git"
UPSTREAM_REVISION = "6ecb09706140f8730b5385c08f1386c654c3c526"
LOAD_DRIVER_REVISION = "wrk2-6ecb097-oci-amd64-v1"
WRK2_TREE_GIT_SHA = "ebb227ba3684e6b69166abbeefe8210ada396018"
LUAJIT_REVISION = "2090842410e0ba6f81fad310a77bf5432488249a"
MIXED_WORKLOAD_LUA_SHA256 = (
    "ab2cd04b6cffb53beaf27efd8dfb5eae7dcd6c8abecbb70623fda93139b3dd32"
)
LUASOCKET_VERSION = "3.1.0-1"
LUASOCKET_FILENAME = f"luasocket-{LUASOCKET_VERSION}.src.rock"
LUASOCKET_URL = (
    "https://luarocks.org/manifests/lunarmodules/" + LUASOCKET_FILENAME
)
LUASOCKET_SOURCE_SHA256 = (
    "f4a207f50a3f99ad65def8e29c54ac9aac668b216476f7fae3fae92413398ed2"
)
LUASOCKET_LICENSE_SHA256 = (
    "224afe42d0738eaaeb57ab289466a1c4e77091591e69dbcef2dbb385589f2f41"
)
ROCKY_LINUX_9_MINIMAL_AMD64 = (
    "docker.io/rockylinux/rockylinux@sha256:"
    "8c5ffaa82079057f9d5206379996763a491f0cea9ef0e27c1395557ecd0f5bef"
)
REQUEST_SCRIPT_RELATIVE = (
    "socialNetwork/wrk2/scripts/social-network/mixed-workload.lua"
)
EXPECTED_TARGET_ANCHORS = 3


class PreparationError(RuntimeError):
    """Raised when a pinned source or generated context drifts."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git(directory: Path, *arguments: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(directory), *arguments],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        raise PreparationError(f"Git source validation failed: {detail.strip()}") from exc
    return result.stdout.strip()


def _validate_upstream(upstream: Path) -> tuple[Path, Path]:
    if _git(upstream, "rev-parse", "HEAD") != UPSTREAM_REVISION:
        raise PreparationError(
            f"Expected upstream revision {UPSTREAM_REVISION}."
        )
    if _git(upstream, "status", "--porcelain", "--untracked-files=all"):
        raise PreparationError("The upstream checkout or a submodule is dirty.")
    if _git(upstream, "rev-parse", "HEAD:wrk2") != WRK2_TREE_GIT_SHA:
        raise PreparationError("The pinned upstream wrk2 Git tree drifted.")

    wrk2 = upstream / "wrk2"
    luajit = wrk2 / "deps" / "luajit"
    if not wrk2.is_dir() or not luajit.is_dir():
        raise PreparationError(
            "The upstream checkout must include the recursive wrk2/LuaJIT submodules."
        )
    if _git(luajit, "rev-parse", "HEAD") != LUAJIT_REVISION:
        raise PreparationError("The pinned wrk2 LuaJIT submodule drifted.")
    if _git(luajit, "status", "--porcelain", "--untracked-files=all"):
        raise PreparationError("The pinned LuaJIT checkout is dirty.")

    _validate_tracked_wrk2_tree(upstream, wrk2)

    request_script = upstream / REQUEST_SCRIPT_RELATIVE
    if not request_script.is_file():
        raise PreparationError("The pinned mixed-workload Lua script is missing.")
    if _sha256(request_script) != MIXED_WORKLOAD_LUA_SHA256:
        raise PreparationError("The pinned mixed-workload Lua script drifted.")
    try:
        script_text = request_script.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise PreparationError("The mixed-workload Lua script is not UTF-8.") from exc
    if script_text.count("http://localhost:8080") != EXPECTED_TARGET_ANCHORS:
        raise PreparationError("The mixed-workload target anchors drifted.")
    return wrk2, request_script


def _validate_tracked_wrk2_tree(upstream: Path, wrk2: Path) -> None:
    """Reject every copied entry that is not in the pinned Git trees.

    ``git status`` intentionally hides ignored files.  A compiler output under
    ``wrk2/obj`` would therefore otherwise be copied into the build context.
    Compare the filesystem to Git's recursive tracked-file inventory as well.
    """

    raw = _git(
        upstream,
        "ls-files",
        "--recurse-submodules",
        "-z",
        "--",
        "wrk2",
    )
    prefix = "wrk2/"
    tracked_files: set[str] = set()
    for value in raw.split("\0"):
        if not value:
            continue
        if not value.startswith(prefix):
            raise PreparationError("The tracked wrk2 inventory is invalid.")
        relative = value[len(prefix):]
        if not relative or relative.startswith("/") or ".." in Path(relative).parts:
            raise PreparationError("The tracked wrk2 inventory is invalid.")
        tracked_files.add(relative)

    expected_entries = set(tracked_files)
    for relative in tracked_files:
        parent = Path(relative).parent
        while parent != Path("."):
            expected_entries.add(parent.as_posix())
            parent = parent.parent
    actual_entries = {
        path.relative_to(wrk2).as_posix()
        for path in wrk2.rglob("*")
        if ".git" not in path.relative_to(wrk2).parts
    }
    if actual_entries != expected_entries:
        extra = sorted(actual_entries - expected_entries)
        missing = sorted(expected_entries - actual_entries)
        detail = []
        if extra:
            detail.append(f"untracked {extra[0]}")
        if missing:
            detail.append(f"missing {missing[0]}")
        raise PreparationError(
            "The copied wrk2 filesystem differs from its pinned Git inventory"
            + (f" ({'; '.join(detail)})." if detail else ".")
        )


def _extract_luasocket_license(luasocket_rock: Path) -> bytes:
    member = "luasocket/LICENSE"
    try:
        with zipfile.ZipFile(luasocket_rock) as archive:
            if archive.namelist().count(member) != 1:
                raise PreparationError(
                    "The pinned LuaSocket source rock has an invalid license inventory."
                )
            license_bytes = archive.read(member)
    except (OSError, zipfile.BadZipFile, KeyError) as exc:
        raise PreparationError(
            "The pinned LuaSocket source rock license is unreadable."
        ) from exc
    if (
        len(license_bytes) > 64 * 1024
        or hashlib.sha256(license_bytes).hexdigest() != LUASOCKET_LICENSE_SHA256
    ):
        raise PreparationError("The pinned LuaSocket license drifted.")
    return license_bytes


def _safe_symlink(path: Path, root: Path) -> str:
    target = os.readlink(path)
    if os.path.isabs(target):
        raise PreparationError(f"Absolute source symlink is forbidden: {path}")
    resolved = (path.parent / target).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise PreparationError(f"Escaping source symlink is forbidden: {path}") from exc
    return target


def _tree_sha256(root: Path) -> str:
    """Hash names, modes, symlink targets, and bytes in deterministic order."""

    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if ".git" in path.relative_to(root).parts:
            continue
        relative = path.relative_to(root).as_posix().encode("utf-8")
        mode = stat.S_IMODE(path.lstat().st_mode)
        if path.is_symlink():
            kind = b"L"
            payload = _safe_symlink(path, root).encode("utf-8")
        elif path.is_dir():
            kind = b"D"
            payload = b""
        elif path.is_file():
            kind = b"F"
            payload = path.read_bytes()
        else:
            raise PreparationError(f"Unsupported source entry: {path}")
        digest.update(kind + b"\0" + relative + b"\0")
        digest.update(f"{mode:o}".encode("ascii") + b"\0")
        digest.update(str(len(payload)).encode("ascii") + b"\0" + payload)
    return digest.hexdigest()


METRICS_LUA = r'''done = function(summary, latency, requests)
  local http_errors = summary.errors.status
  local socket_errors = summary.errors.connect + summary.errors.read +
    summary.errors.write + summary.errors.timeout
  local seconds = summary.duration / 1000000.0
  local throughput = 0.0
  local error_rate = 0.0
  if seconds > 0 then throughput = summary.requests / seconds end
  if summary.requests > 0 then
    error_rate = http_errors * 100.0 / summary.requests
  end
  io.write(string.format(
    "OCI_DSB_METRICS duration_seconds=%.6f total_requests=%d " ..
    "throughput_requests_per_second=%.6f errors=%d error_rate_percent=%.6f " ..
    "socket_errors=%d connect_errors=%d read_errors=%d write_errors=%d " ..
    "timeout_errors=%d " ..
    "p50_ms=%.6f p95_ms=%.6f p99_ms=%.6f\n",
    seconds, summary.requests, throughput, http_errors, error_rate,
    socket_errors, summary.errors.connect, summary.errors.read,
    summary.errors.write, summary.errors.timeout,
    latency:percentile(50.0) / 1000.0,
    latency:percentile(95.0) / 1000.0,
    latency:percentile(99.0) / 1000.0))
end
'''


ENTRYPOINT = r'''#!/bin/bash
set -euo pipefail

readonly artifact_manifest=/opt/deathstarbench-load-driver/artifact-manifest.env
readonly wrk_binary=/opt/deathstarbench-load-driver/bin/wrk
readonly luajit_binary=/opt/deathstarbench-load-driver/luajit/bin/luajit
readonly request_script=/opt/deathstarbench-load-driver/share/mixed-workload.lua
readonly metrics_script=/opt/deathstarbench-load-driver/share/metrics.lua

fail() {
  printf '%s\n' "load-driver: $*" >&2
  exit 64
}

test -r "$artifact_manifest" || fail "artifact manifest is missing"
# The manifest is created from fixed hexadecimal and identifier build inputs.
# shellcheck disable=SC1090
. "$artifact_manifest"

require_uint() {
  local label=$1 value=$2 minimum=$3 maximum=$4
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || fail "$label must be a positive integer"
  (( value >= minimum && value <= maximum )) || fail "$label is out of range"
}

verify_artifact() {
  [[ "$(uname -m)" == x86_64 ]] || fail "runtime architecture is not x86_64"
  [[ -x "$wrk_binary" ]] || fail "wrk2 binary is missing"
  [[ -r "$request_script" && -r "$metrics_script" ]] || fail "workload assets are missing"
  [[ "$(sha256sum "$wrk_binary" | awk '{print $1}')" == "$WRK_BINARY_SHA256" ]] ||
    fail "wrk2 binary digest mismatch"
  [[ "$(sha256sum "$request_script" | awk '{print $1}')" == "$REQUEST_SCRIPT_SHA256" ]] ||
    fail "request script digest mismatch"
  [[ "$(grep -Foc 'http://localhost:8080' "$request_script")" == 3 ]] ||
    fail "request script target anchors drifted"
}

configure_lua_environment() {
  export LUA_PATH='/opt/deathstarbench-load-driver/luasocket/share/lua/5.1/?.lua;/opt/deathstarbench-load-driver/luasocket/share/lua/5.1/?/init.lua;;'
  export LUA_CPATH='/opt/deathstarbench-load-driver/luasocket/lib/lua/5.1/?.so;;'
}

runtime_smoke_check() {
  local dependencies usage
  command -v ldd >/dev/null || fail "ldd is missing from the runtime image"
  if ! dependencies=$(ldd "$wrk_binary" 2>&1); then
    fail "wrk2 dynamic dependency inspection failed"
  fi
  [[ "$dependencies" != *"not found"* ]] || fail "wrk2 has a missing dynamic library"
  if usage=$("$wrk_binary" 2>&1); then
    fail "wrk2 unexpectedly accepted an argument-free invocation"
  fi
  case "$usage" in
    *"Usage: wrk <options> <url>"*"-R, --rate"*) ;;
    *) fail "wrk2 does not expose the expected rate-limited usage interface" ;;
  esac
  [[ -x "$luajit_binary" ]] || fail "the pinned LuaJIT runtime is missing"
  configure_lua_environment
  "$luajit_binary" -e \
    'local socket=assert(require("socket")); assert(require("socket.core")); local mime=assert(require("mime")); assert(type(socket.gettime)=="function"); assert(type(mime.b64)=="function")' \
    || fail "the embedded Lua/LuaSocket runtime cannot load"
}

attest() {
  verify_artifact
  runtime_smoke_check
  printf '%s\n' \
    "DISTRIBUTED_DSB_LOAD_DRIVER_ARTIFACT architecture=x86_64 platform=linux/amd64 revision=$LOAD_DRIVER_REVISION upstream_revision=$UPSTREAM_REVISION context_sha256=$CONTEXT_SHA256 wrk_binary_sha256=$WRK_BINARY_SHA256 wrk2_tree_git_sha=$WRK2_TREE_GIT_SHA wrk2_source_sha256=$WRK2_SOURCE_SHA256 luajit_revision=$LUAJIT_REVISION luasocket_source_sha256=$LUASOCKET_SOURCE_SHA256 request_script_sha256=$REQUEST_SCRIPT_SHA256"
}

private_ipv4() {
  local value=$1 first second third fourth
  [[ "$value" =~ ^([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})$ ]] || return 1
  first=$((10#${BASH_REMATCH[1]}))
  second=$((10#${BASH_REMATCH[2]}))
  third=$((10#${BASH_REMATCH[3]}))
  fourth=$((10#${BASH_REMATCH[4]}))
  (( first <= 255 && second <= 255 && third <= 255 && fourth <= 255 )) || return 1
  (( first == 10 || (first == 172 && second >= 16 && second <= 31) ||
     (first == 192 && second == 168) ))
}

run_driver() {
  [[ $# == 14 ]] || fail "run requires exactly seven named arguments"
  [[ ${1} == --target && ${3} == --port && ${5} == --threads &&
     ${7} == --connections && ${9} == --rate && ${11} == --duration &&
     ${13} == --max-user-index ]] || fail "run arguments are missing or out of order"
  local target=$2 port=$4 threads=$6 connections=$8 rate=${10} duration=${12} max_user_index=${14}
  private_ipv4 "$target" || fail "target must be an RFC1918 IPv4 address"
  require_uint port "$port" 1 65535
  require_uint threads "$threads" 1 1024
  require_uint connections "$connections" 1 1000000
  require_uint rate "$rate" 1 1000000000
  require_uint duration "$duration" 1 86400
  [[ "$max_user_index" == 962 ]] || fail "max-user-index must be exactly 962"
  (( connections >= threads && connections % threads == 0 )) ||
    fail "connections must be at least and divisible by threads"
  (( rate >= threads && rate % threads == 0 )) ||
    fail "rate must be at least and divisible by threads"

  verify_artifact
  local hard_nofile required_nofile run_dir
  required_nofile=$((connections + 128))
  hard_nofile=$(ulimit -Hn)
  if [[ "$hard_nofile" != unlimited ]]; then
    [[ "$hard_nofile" =~ ^[0-9]+$ ]] || fail "unable to read open-file hard limit"
    (( hard_nofile >= required_nofile )) || fail "open-file hard limit is too low"
  fi
  ulimit -Sn "$required_nofile"

  run_dir=$(mktemp -d /tmp/deathstarbench-load-driver.XXXXXXXX)
  trap 'rm -rf "$run_dir"' EXIT
  cp "$request_script" "$run_dir/workload.lua"
  sed -i "s|http://localhost:8080|http://$target:$port|g" "$run_dir/workload.lua"
  [[ "$(grep -Foc "http://$target:$port" "$run_dir/workload.lua")" == 3 ]] ||
    fail "failed to bind the request script target"
  printf '\n' >> "$run_dir/workload.lua"
  cat "$metrics_script" >> "$run_dir/workload.lua"
  export max_user_index
  configure_lua_environment
  printf 'wrk2 open-file soft limit: %s\n' "$(ulimit -Sn)"
  printf 'OCI_DSB_LOADGEN_LOGICAL_CPUS=%s\n' "$(nproc)"
  /usr/bin/time -v "$wrk_binary" -D exp -r -t "$threads" -c "$connections" \
    -d "${duration}s" -L -s "$run_dir/workload.lua" -R "$rate" \
    "http://$target:$port/"
}

case ${1-} in
  attest)
    [[ $# == 1 ]] || fail "attest accepts no arguments"
    attest
    ;;
  run)
    shift
    run_driver "$@"
    ;;
  *) fail "expected: attest | run --target ... --port ... --threads ... --connections ... --rate ... --duration ... --max-user-index ..." ;;
esac
'''


def _dockerfile() -> str:
    return f'''FROM {ROCKY_LINUX_9_MINIMAL_AMD64} AS builder
ARG LOAD_DRIVER_CONTEXT_SHA256
ARG WRK2_SOURCE_SHA256
RUN set -eux; \\
    test "$(uname -m)" = x86_64; \\
    microdnf install -y findutils gcc grep make openssl-devel zlib-devel unzip; \\
    microdnf clean all
COPY wrk2 /src/wrk2
COPY {LUASOCKET_FILENAME} /src/{LUASOCKET_FILENAME}
COPY mixed-workload.lua /src/mixed-workload.lua
COPY metrics.lua /src/metrics.lua
COPY entrypoint /src/entrypoint
COPY licenses /src/licenses
RUN set -eux; \\
    test "$(sha256sum /src/{LUASOCKET_FILENAME} | awk '{{print $1}}')" = "{LUASOCKET_SOURCE_SHA256}"; \\
    test "$(sha256sum /src/mixed-workload.lua | awk '{{print $1}}')" = "{MIXED_WORKLOAD_LUA_SHA256}"; \\
    printf '%s' "$LOAD_DRIVER_CONTEXT_SHA256" | grep -Eq '^[0-9a-f]{{64}}$'; \\
    printf '%s' "$WRK2_SOURCE_SHA256" | grep -Eq '^[0-9a-f]{{64}}$'; \\
    make -C /src/wrk2 -j"$(nproc)"; \\
    make -C /src/wrk2/deps/luajit PREFIX=/opt/deathstarbench-load-driver/luajit install; \\
    luajit_real=$(find /opt/deathstarbench-load-driver/luajit/bin -maxdepth 1 -type f -name 'luajit-*' -print -quit); \\
    test -n "$luajit_real"; \\
    ln -s "$(basename "$luajit_real")" /opt/deathstarbench-load-driver/luajit/bin/luajit; \\
    mkdir -p /tmp/luasocket /opt/deathstarbench-load-driver/luasocket; \\
    unzip -q /src/{LUASOCKET_FILENAME} -d /tmp/luasocket; \\
    source_make=$(find /tmp/luasocket -mindepth 2 -maxdepth 3 -type f -path '*/src/makefile' -print -quit); \\
    test -n "$source_make"; \\
    source_dir=${{source_make%/src/makefile}}; \\
    make -C "$source_dir" -j"$(nproc)" PLAT=linux LUAV=5.1 \\
      LUAINC_linux=/opt/deathstarbench-load-driver/luajit/include/luajit-2.1 \\
      linux; \\
    make -C "$source_dir" PLAT=linux LUAV=5.1 \\
      LUAINC_linux=/opt/deathstarbench-load-driver/luajit/include/luajit-2.1 \\
      prefix=/ DESTDIR=/opt/deathstarbench-load-driver/luasocket install-unix; \\
    install -D -m 0755 /src/wrk2/wrk /opt/deathstarbench-load-driver/bin/wrk; \\
    install -D -m 0644 /src/mixed-workload.lua /opt/deathstarbench-load-driver/share/mixed-workload.lua; \\
    install -D -m 0644 /src/metrics.lua /opt/deathstarbench-load-driver/share/metrics.lua; \\
    install -D -m 0755 /src/entrypoint /opt/deathstarbench-load-driver/bin/entrypoint; \\
    cp -a /src/licenses /opt/deathstarbench-load-driver/licenses; \\
    wrk_sha=$(sha256sum /opt/deathstarbench-load-driver/bin/wrk | awk '{{print $1}}'); \\
    printf '%s\\n' \\
      'LOAD_DRIVER_REVISION={LOAD_DRIVER_REVISION}' \\
      'UPSTREAM_REVISION={UPSTREAM_REVISION}' \\
      "CONTEXT_SHA256=$LOAD_DRIVER_CONTEXT_SHA256" \\
      "WRK_BINARY_SHA256=$wrk_sha" \\
      'WRK2_TREE_GIT_SHA={WRK2_TREE_GIT_SHA}' \\
      "WRK2_SOURCE_SHA256=$WRK2_SOURCE_SHA256" \\
      'LUAJIT_REVISION={LUAJIT_REVISION}' \\
      'LUASOCKET_SOURCE_SHA256={LUASOCKET_SOURCE_SHA256}' \\
      'REQUEST_SCRIPT_SHA256={MIXED_WORKLOAD_LUA_SHA256}' \\
      > /opt/deathstarbench-load-driver/artifact-manifest.env

FROM {ROCKY_LINUX_9_MINIMAL_AMD64} AS runtime
ARG OCI_SOURCE_REPOSITORY
ARG OCI_SOURCE_REVISION
RUN set -eux; \\
    test "$(uname -m)" = x86_64; \\
    microdnf install -y bash gawk glibc-common grep openssl-libs sed time zlib; \\
    microdnf clean all; \\
    rm -rf /var/cache/dnf /var/cache/yum /var/log/dnf*; \\
    rm -f /usr/bin/microdnf /usr/bin/curl /usr/bin/wget /usr/bin/ftp \\
      /usr/bin/nc /usr/bin/ncat /usr/bin/ssh /usr/bin/scp /usr/bin/git \\
      /usr/bin/gcc /usr/bin/cc /usr/bin/make
COPY --from=builder --chown=65532:65532 /opt/deathstarbench-load-driver /opt/deathstarbench-load-driver
LABEL org.opencontainers.image.source="${{OCI_SOURCE_REPOSITORY}}" \\
      org.opencontainers.image.revision="${{OCI_SOURCE_REVISION}}" \\
      org.opencontainers.image.title="DeathStarBench wrk2 load driver" \\
      org.opencontainers.image.licenses="Apache-2.0 AND MIT" \\
      io.github.oci-self-service-benchmarks.load-driver.revision="{LOAD_DRIVER_REVISION}" \\
      io.github.oci-self-service-benchmarks.upstream.revision="{UPSTREAM_REVISION}"
USER 65532:65532
WORKDIR /tmp
ENTRYPOINT ["/opt/deathstarbench-load-driver/bin/entrypoint"]
CMD ["attest"]
'''


def _copy_source_tree(source: Path, destination: Path) -> None:
    def ignore(_directory: str, names: list[str]) -> set[str]:
        return {name for name in names if name == ".git"}

    shutil.copytree(source, destination, symlinks=True, ignore=ignore)


def prepare_context(upstream: Path, luasocket_rock: Path, output: Path) -> dict:
    wrk2, request_script = _validate_upstream(upstream)
    if not luasocket_rock.is_file():
        raise PreparationError("The pinned LuaSocket source rock is missing.")
    if _sha256(luasocket_rock) != LUASOCKET_SOURCE_SHA256:
        raise PreparationError("The pinned LuaSocket source rock drifted.")
    luasocket_license = _extract_luasocket_license(luasocket_rock)

    context = output / "context"
    if output.exists():
        raise PreparationError("The output directory already exists.")
    context.mkdir(parents=True)
    _copy_source_tree(wrk2, context / "wrk2")
    shutil.copy2(luasocket_rock, context / LUASOCKET_FILENAME)
    shutil.copy2(request_script, context / "mixed-workload.lua")
    (context / "metrics.lua").write_text(METRICS_LUA, encoding="utf-8")
    (context / "entrypoint").write_text(ENTRYPOINT, encoding="utf-8")
    (context / "entrypoint").chmod(0o755)
    (context / "Dockerfile").write_text(_dockerfile(), encoding="utf-8")
    (context / ".dockerignore").write_text(
        "*\n!/.dockerignore\n!/Dockerfile\n!/entrypoint\n!/metrics.lua\n"
        "!/mixed-workload.lua\n!/luasocket-3.1.0-1.src.rock\n"
        "!/wrk2/\n!/wrk2/**\n!/licenses/\n!/licenses/**\n",
        encoding="utf-8",
    )
    licenses = context / "licenses"
    licenses.mkdir()
    for source, name in (
        (upstream / "LICENSE", "DeathStarBench-LICENSE"),
        (wrk2 / "LICENSE", "wrk2-LICENSE"),
        (wrk2 / "deps" / "luajit" / "COPYRIGHT", "LuaJIT-COPYRIGHT"),
    ):
        if not source.is_file():
            raise PreparationError(f"Required license file is missing: {source}")
        shutil.copy2(source, licenses / name)
    (licenses / "LuaSocket-LICENSE").write_bytes(luasocket_license)

    wrk2_source_sha256 = _tree_sha256(context / "wrk2")
    context_sha256 = _tree_sha256(context)
    manifest = {
        "schema_version": 1,
        "architecture": "x86_64",
        "platform": "linux/amd64",
        "load_driver_revision": LOAD_DRIVER_REVISION,
        "upstream_repository": UPSTREAM_REPOSITORY,
        "upstream_revision": UPSTREAM_REVISION,
        "base_image": ROCKY_LINUX_9_MINIMAL_AMD64,
        "context_path": "context",
        "context_sha256": context_sha256,
        "wrk2_tree_git_sha": WRK2_TREE_GIT_SHA,
        "wrk2_source_sha256": wrk2_source_sha256,
        "luajit_revision": LUAJIT_REVISION,
        "luasocket_source_url": LUASOCKET_URL,
        "luasocket_source_sha256": LUASOCKET_SOURCE_SHA256,
        "request_script_sha256": MIXED_WORKLOAD_LUA_SHA256,
        "runtime_network_required": False,
        "runtime_build_tools": False,
        "runtime_user": "65532:65532",
    }
    (output / "context-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--luasocket-rock", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    prepare_context(
        arguments.upstream.resolve(),
        arguments.luasocket_rock.resolve(),
        arguments.output.resolve(),
    )
    print(arguments.output / "context-manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
