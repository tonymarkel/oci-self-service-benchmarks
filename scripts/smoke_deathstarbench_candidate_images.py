#!/usr/bin/env python3
"""Inspect native candidate images offline, without qualifying live semantics.

The image must already be available locally by an immutable platform digest.
Scratch Hotel images are inspected via a stopped container export; service
startup is deliberately not invoked because upstream startup mutates MongoDB.
Other checks run only short, hardened, network-disabled inspection commands.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform as host_platform
import re
import shlex
import struct
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_workload_contract import DISTRIBUTED_WORKLOAD_PROFILES
from scripts.deathstarbench_artifact_common import PreparationError, sha256, tree_sha256
from scripts.deathstarbench_candidate_registry import (
    DIGEST, digest_bytes, inspect_manifest, inspect_blob, parse_reference, resolve_platform,
)
from scripts.inspect_deathstarbench_support_images import write_receipt
from scripts.deathstarbench_public_crypto_fixtures import PUBLIC_PEM_SHA256, system_gnutls_path


HOTEL_PROGRAMS = ("attractions", "frontend", "geo", "profile", "rate", "recommendation", "reservation", "review", "search", "user")
MEDIA_PROGRAMS = ("CastInfoService", "ComposeReviewService", "MovieIdService", "MovieInfoService", "MovieReviewService", "PageService", "PlotService", "RatingService", "ReviewStorageService", "TextService", "UniqueIdService", "UserReviewService", "UserService")
MEDIA_DEPENDENCY_LICENSES = {
    "media-microservices": ("thrift", "json", "yaml", "opentracing", "jaeger", "mongo", "jwt", "redis", "tacopie"),
    "nginx-web-server": ("thrift", "json", "yaml", "opentracing", "jaeger", "openssl", "pcre", "nginx", "openresty", "hmac", "luarocks"),
}
ARCHITECTURES = {"x86_64": ("linux/amd64", 62), "aarch64": ("linux/arm64", 183)}
IMAGE_KEYS = {"hotel_reservation": {"hotel-reservation", "load-driver"},
              "media_microservices": {"media-microservices", "nginx-web-server", "load-driver"}}
# Reject actual PEM material, not OpenSSL's binary parser-format strings.
PRIVATE_KEY = re.compile(
    rb"-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----\r?\n"
    rb"(?:Proc-Type: 4,ENCRYPTED\r?\nDEK-Info: [A-Z0-9-]+,[0-9A-F]+\r?\n\r?\n)?"
    rb"[A-Za-z0-9+/=]{32,}"
)
COMPLETE_PRIVATE_KEY = re.compile(
    rb"-----BEGIN ((?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY)-----\r?\n"
    rb"[A-Za-z0-9+/=\r\n]{32,65536}-----END \1-----"
)
CLONE = re.compile(r"\bgit\s+(?:-[^\s]+\s+)*(?:clone|pull|fetch)\b")
MAX_FILE_BYTES = 256 * 1024 * 1024


def _command(arguments: list[str], *, timeout: int = 120, stdout_file=None) -> bytes:
    try:
        result = subprocess.run(arguments, check=True, stdout=stdout_file or subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=timeout)
        return result.stdout or b""
    except subprocess.CalledProcessError as error:
        # These bounded offline commands do not carry credentials. Preserve
        # useful linkage/config diagnostics, never environment or file bodies.
        diagnostic = ((error.stdout or b"") + (error.stderr or b"")).decode("utf-8", errors="replace")[-2000:]
        raise PreparationError(f"Candidate image inspection command {arguments[0]} failed ({error.returncode}): {diagnostic}") from None
    except (OSError, subprocess.TimeoutExpired):
        raise PreparationError("Candidate image inspection command failed.") from None


def _normalized(name: str) -> str:
    value = PurePosixPath(name)
    if not name or name.startswith("/") or ".." in value.parts or "\\" in name or "\0" in name:
        raise PreparationError("Unsafe container archive member path.")
    return value.as_posix().removeprefix("./")


class ImageFilesystem:
    """Read a Docker export without extracting paths or following host links."""

    def __init__(self, archive: Path):
        self.archive = tarfile.open(archive, mode="r:")
        self.members = {}
        self.public_crypto_selftest_pem_sha256 = {}
        try:
            for member in self.archive.getmembers():
                name = _normalized(member.name)
                if name == ".":
                    continue
                if name in self.members:
                    raise PreparationError("Duplicate container archive member.")
                self.members[name] = member
        except Exception:
            self.archive.close()
            raise

    def close(self):
        self.archive.close()

    def _resolve(self, name: str) -> tarfile.TarInfo:
        name = _normalized(name.lstrip("/"))
        for _ in range(40):
            parts = name.split("/")
            # Ubuntu's usrmerge exports /lib and /lib64 as directory symlinks.
            # Resolve each virtual ancestor without touching the host filesystem.
            link = next((("/".join(parts[:i]), self.members["/".join(parts[:i])], parts[i:])
                         for i in range(1, len(parts) + 1)
                         if "/".join(parts[:i]) in self.members
                         and (self.members["/".join(parts[:i])].issym()
                              or self.members["/".join(parts[:i])].islnk())), None)
            if link is None:
                member = self.members.get(name)
                if member is None:
                    raise PreparationError("Required baked file is missing from the image.")
                return member
            link_name, member, suffix = link
            target = member.linkname
            combined = target.lstrip("/") if target.startswith("/") or member.islnk() else str(PurePosixPath(link_name).parent / target)
            if suffix:
                combined += "/" + "/".join(suffix)
            parts = []
            for part in combined.split("/"):
                if part in {"", "."}:
                    continue
                if part == "..":
                    if not parts:
                        raise PreparationError("Container symlink escapes its virtual root.")
                    parts.pop()
                else:
                    parts.append(part)
            name = _normalized("/".join(parts))
        raise PreparationError("Cyclic container symlink.")

    def read(self, name: str) -> bytes:
        member = self._resolve(name)
        if not member.isfile() or member.size < 0 or member.size > MAX_FILE_BYTES:
            raise PreparationError("Required image file has an invalid type or bounded size.")
        stream = self.archive.extractfile(member)
        if stream is None:
            raise PreparationError("Container file is unreadable.")
        with stream:
            data = stream.read(MAX_FILE_BYTES + 1)
        if len(data) != member.size:
            raise PreparationError("Container file bytes differ from its archive descriptor.")
        return data

    def prefix(self, name: str, length: int = 4) -> bytes:
        member = self._resolve(name)
        if not member.isfile():
            return b""
        stream = self.archive.extractfile(member)
        if stream is None:
            return b""
        with stream:
            return stream.read(length)

    def has_private_key(self, name: str) -> bool:
        member = self._resolve(name)
        if not member.isfile():
            return False
        stream = self.archive.extractfile(member)
        if stream is None:
            raise PreparationError("Runtime key screening could not read a regular file.")
        with stream:
            overlap = b""
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                data = overlap + chunk
                if PRIVATE_KEY.search(data):
                    # GnuTLS deliberately embeds public FIPS known-answer PEM
                    # vectors. Never exempt a whole library or a bare marker.
                    if not system_gnutls_path(name) or self.prefix(name) != b"\x7fELF":
                        return True
                    raw = self.read(name)
                    prefixes = list(PRIVATE_KEY.finditer(raw))
                    keys = list(COMPLETE_PRIVATE_KEY.finditer(raw))
                    if {match.start() for match in prefixes} != {match.start() for match in keys}:
                        return True
                    identities = sorted({hashlib.sha256(match.group().replace(b"\r\n", b"\n").strip()).hexdigest() for match in keys})
                    if not identities or not set(identities) <= PUBLIC_PEM_SHA256:
                        return True
                    self.public_crypto_selftest_pem_sha256["/" + name] = identities
                    return False
                overlap = data[-256:]
        return False


def elf_identity(data: bytes, architecture: str) -> dict[str, object]:
    """Inspect ELF64 headers/program tables without executing the binary."""
    if architecture not in ARCHITECTURES or len(data) < 64 or data[:6] != b"\x7fELF\x02\x01":
        raise PreparationError("A native little-endian ELF64 artifact is required.")
    file_type, machine = struct.unpack_from("<HH", data, 16)
    if machine != ARCHITECTURES[architecture][1]:
        raise PreparationError("ELF machine architecture differs from the native image platform.")
    offset = struct.unpack_from("<Q", data, 32)[0]
    size, count = struct.unpack_from("<HH", data, 54)
    if file_type not in {1, 2, 3} or (file_type != 1 and count == 0) or (count and size < 56) or offset + count * size > len(data):
        raise PreparationError("ELF program table is invalid.")
    interpreter, needed = None, False
    for index in range(count):
        kind, _, position, _, _, filesz, _, _ = struct.unpack_from("<IIQQQQQQ", data, offset + index * size)
        if position + filesz > len(data):
            raise PreparationError("ELF segment extends outside its file.")
        if kind == 3:
            raw = data[position:position + filesz]
            if not raw.endswith(b"\0"):
                raise PreparationError("ELF interpreter path is invalid.")
            try:
                interpreter = raw[:-1].decode("ascii")
            except UnicodeError:
                raise PreparationError("ELF interpreter path is invalid.") from None
        if kind == 2:
            if filesz % 16:
                raise PreparationError("ELF dynamic table is invalid.")
            for dynamic in range(position, position + filesz, 16):
                tag, _ = struct.unpack_from("<qQ", data, dynamic)
                if tag == 1:
                    needed = True
    return {"sha256": hashlib.sha256(data).hexdigest(), "elf_class": 64, "elf_type": file_type,
            "machine": machine, "architecture": architecture,
            "interpreter": interpreter, "dynamic_dependencies": needed,
            "runnable": file_type in {2, 3}, "static": file_type in {2, 3} and interpreter is None and not needed}


def _native_runner(architecture: str) -> dict[str, object]:
    expected = ARCHITECTURES[architecture][0]
    observed = {"amd64": "x86_64", "arm64": "aarch64"}.get(host_platform.machine(), host_platform.machine())
    if host_platform.system() != "Linux" or observed != architecture:
        raise PreparationError("Image smoke requires a matching native Linux runner, not QEMU.")
    try:
        info = json.loads(_command(["docker", "info", "--format", "{{json .}}"] ))
    except (ValueError, TypeError):
        raise PreparationError("Docker daemon platform inspection failed.") from None
    daemon = {"amd64": "x86_64", "arm64": "aarch64"}.get(info.get("Architecture"), info.get("Architecture"))
    if info.get("OSType") != "linux" or daemon != architecture:
        raise PreparationError("Docker daemon is not on the matching native Linux platform.")
    return {"platform": expected, "architecture": architecture, "qemu_used": False}


def _controls(image: str, *, hosts: tuple[str, ...] = ()) -> list[str]:
    args = ["docker", "run", "--rm", "--network=none", "--read-only", "--user=65532:65532",
            "--cap-drop=ALL", "--security-opt=no-new-privileges",
            "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=256m,mode=1777"]
    for name in hosts:
        args.append("--add-host=" + name + ":127.0.0.1")
    return args


def _run_inspection(image: str, command: list[str], *, hosts: tuple[str, ...] = ()) -> dict[str, object]:
    args = _controls(image, hosts=hosts) + ["--entrypoint=" + command[0], image, *command[1:]]
    raw = _command(args)
    return {"command": command, "stdout_sha256": hashlib.sha256(raw).hexdigest(),
            "stdout": raw.decode("utf-8", errors="replace"), "mock_dns": {name: "127.0.0.1" for name in hosts}}


def _baked_files(workload: str, key: str, context: Path) -> dict[str, Path]:
    if key == "load-driver":
        if not (context / "licenses").is_dir():
            raise PreparationError("The prepared driver license directory is missing.")
        prefix = "/opt/deathstarbench-candidate-driver"
        files = {prefix + "/share/request.lua": context / "request.lua",
                 prefix + "/share/metrics.lua": context / "metrics.lua",
                 prefix + "/bin/entrypoint": context / "entrypoint"}
        files.update({prefix + "/licenses/" + p.name: p for p in (context / "licenses").iterdir() if p.is_file()})
    elif workload == "hotel_reservation":
        if not (context / "licenses/vendor").is_dir():
            raise PreparationError("The prepared Hotel dependency license directory is missing.")
        files = {"/workspace/config.json": context / "config.json",
                 "/usr/share/licenses/deathstarbench/LICENSE": context / "LICENSE.deathstarbench"}
        files.update({"/usr/share/licenses/deathstarbench/vendor/" + p.relative_to(context / "licenses/vendor").as_posix(): p
                      for p in (context / "licenses/vendor").rglob("*") if p.is_file()})
    elif key == "media-microservices":
        files = {"/media-microservices/config/" + name: context / "config" / name
                 for name in ("service-config.json", "jaeger-config.yml")}
        files["/usr/share/licenses/deathstarbench/LICENSE"] = context / "LICENSE.deathstarbench"
        files["/usr/share/licenses/deathstarbench/PicoSHA2.LICENSE"] = context / "third_party/PicoSHA2/LICENSE"
    else:
        files = {"/usr/share/licenses/deathstarbench/LICENSE": context / "LICENSE.deathstarbench",
                 "/usr/share/licenses/deathstarbench/OpenResty.COPYRIGHT": context / "COPYRIGHT",
                 "/usr/share/licenses/deathstarbench/lua-bridge-tracer.LICENSE": context / "lua-bridge-tracer/LICENSE",
                 "/usr/local/openresty/lualib/json/json.lua": context / "lua-json/json.lua",
                 "/usr/local/openresty/nginx/conf/nginx.conf": context / "runtime/nginx.conf",
                 "/usr/local/openresty/nginx/jaeger-config.json": context / "runtime/jaeger-config.json"}
        files.update({"/usr/local/openresty/lualib/thrift/" + p.relative_to(context / "lua-thrift").as_posix(): p
                      for p in (context / "lua-thrift").rglob("*.lua") if p.is_file()})
        for source, target in (("runtime/gen-lua", "/gen-lua"), ("runtime/lua-scripts", "/usr/local/openresty/nginx/lua-scripts")):
            files.update({target + "/" + p.relative_to(context / source).as_posix(): p
                          for p in (context / source).rglob("*") if p.is_file()})
    if not files or any(p.is_symlink() or not p.is_file() for p in files.values()):
        raise PreparationError("Expected baked context files/licenses are missing or unsafe.")
    return files


def _inspect_filesystem(fs: ImageFilesystem, workload: str, key: str, architecture: str,
                        context: Path, image_config: dict) -> dict[str, object]:
    files = {}
    expected = _baked_files(workload, key, context)
    for path, original in expected.items():
        raw = fs.read(path)
        if hashlib.sha256(raw).hexdigest() != sha256(original):
            raise PreparationError("Baked runtime config/script/license differs from the prepared context.")
        files[path] = hashlib.sha256(raw).hexdigest()
        if path.endswith(".lua") and CLONE.search(raw.decode("utf-8", errors="ignore")):
            raise PreparationError("A baked runtime Lua script clones mutable source.")
    if key == "hotel-reservation":
        go_license = fs.read("/usr/share/licenses/go/LICENSE")
        if len(go_license) < 100 or b"Copyright" not in go_license:
            raise PreparationError("Scratch Hotel is missing its Go toolchain license.")
        files["/usr/share/licenses/go/LICENSE"] = hashlib.sha256(go_license).hexdigest()
    license_files = {}
    for dependency in MEDIA_DEPENDENCY_LICENSES.get(key, ()):
        prefix = "usr/share/licenses/deathstarbench/dependencies/" + dependency + "/"
        candidates = sorted(name for name, member in fs.members.items() if name.startswith(prefix) and member.isfile())
        if not candidates:
            raise PreparationError("A Media dependency license/notice directory is missing or empty.")
        for name in candidates:
            raw = fs.read(name)
            if len(raw.strip()) < 20:
                raise PreparationError("A baked Media dependency license/notice is empty or truncated.")
            license_files["/" + name] = hashlib.sha256(raw).hexdigest()
    command = [*(image_config.get("Entrypoint") or []), *(image_config.get("Cmd") or [])]
    if CLONE.search(" ".join(command)):
        raise PreparationError("Runtime command clones mutable source.")
    for token in command:
        if token.startswith("/") and token.lstrip("/") in fs.members:
            raw = fs.read(token)
            if raw.startswith(b"#!") and CLONE.search(raw.decode("utf-8", errors="ignore")):
                raise PreparationError("Runtime entrypoint clones mutable source.")
    elves = {}
    for name, member in fs.members.items():
        if not member.isfile():
            continue
        if fs.has_private_key(name):
            raise PreparationError(f"Runtime image contains a private/demo key in {name}.")
        if fs.prefix(name) == b"\x7fELF":
            identity = elf_identity(fs.read(name), architecture)
            if identity["interpreter"]:
                interpreter = fs.read(identity["interpreter"])
                elf_identity(interpreter, architecture)
            elves["/" + name] = identity
    required = (["/go/bin/" + p for p in HOTEL_PROGRAMS] if key == "hotel-reservation" else
                ["/usr/local/bin/" + p for p in MEDIA_PROGRAMS] if key == "media-microservices" else
                ["/usr/local/openresty/nginx/sbin/nginx"] if key == "nginx-web-server" else
                ["/opt/deathstarbench-candidate-driver/bin/wrk"])
    for path in required:
        if path not in elves or not elves[path]["runnable"] or not fs._resolve(path).mode & 0o111:
            raise PreparationError("A required native executable is missing or not executable.")
        if key == "hotel-reservation" and not elves[path]["static"]:
            raise PreparationError("Scratch Hotel executables must be statically linked.")
    return {"baked_file_sha256": files, "dependency_license_sha256": license_files, "elf_artifacts": elves,
            "required_executables": required, "runtime_clone_detected": False,
            "private_key_detected": False}


def _ldd_command(paths: list[str]) -> list[str]:
    quoted = " ".join(shlex.quote(path) for path in paths)
    return ["/bin/sh", "-c", 'set -eu; for binary in ' + quoted + '; do status=0; output=$(ldd "$binary" 2>&1) || status=$?; '
            'printf "%s\\n%s\\n" "$binary" "$output"; test "$status" -eq 0; '
            'case "$output" in *"not found"*) exit 1;; esac; done']


def inspection_plan(workload: str, key: str, context: Path, identities: dict) -> list[tuple[list[str], tuple[str, ...]]]:
    """Return exact, bounded offline commands shared with receipt validation."""
    plan = []
    dynamic = sorted(path for path, value in identities["elf_artifacts"].items() if value["dynamic_dependencies"])
    if key != "hotel-reservation" and dynamic:
        plan.append((_ldd_command(dynamic), ()))
    if key == "nginx-web-server":
        names = tuple(sorted(DISTRIBUTED_WORKLOAD_PROFILES[workload].expected_component_placement))
        hosts = tuple(sorted({*names, *(name + ".deathstarbench-media.svc.cluster.local" for name in names)}))
        plan.append((["/usr/local/openresty/bin/openresty", "-t"], hosts))
        modules = [p.stem for p in sorted((context / "runtime/gen-lua").glob("*.lua"))]
        lua = 'package.path="/gen-lua/?.lua;/usr/local/openresty/nginx/lua-scripts/?.lua;"..package.path; '
        lua += '; '.join('assert(require(' + json.dumps(module) + '))' for module in ["Thrift", "liblualongnumber", "json", "resty.jwt", "opentracing_bridge_tracer", *modules])
        plan.append((["/usr/local/openresty/bin/resty", "-e", lua], hosts))
    elif key == "load-driver":
        plan.append((["/opt/deathstarbench-candidate-driver/bin/entrypoint", "attest"], ()))
    return plan


def _driver_attestation(text: str, workload: str, context_hash: str, identities: dict) -> None:
    lines = [line for line in text.splitlines() if line.startswith("CANDIDATE_DSB_LOAD_DRIVER_ARTIFACT ")]
    if len(lines) != 1:
        raise PreparationError("Exactly one candidate driver attestation is required.")
    fields = {}
    for token in lines[0].split()[1:]:
        if token.count("=") != 1:
            raise PreparationError("Malformed candidate driver attestation.")
        key, value = token.split("=", 1)
        if key in fields:
            raise PreparationError("Duplicate candidate driver attestation field.")
        fields[key] = value
    profile = DISTRIBUTED_WORKLOAD_PROFILES[workload]
    expected = {"schema": "candidate-driver-v1", "measurement_qualified": "0", "workload": workload,
                "architecture": "x86_64", "platform": "linux/amd64", "context_sha256": context_hash,
                "revision": profile.load_driver_revision, "upstream_revision": profile.metadata()["upstream_revision"],
                "original_request_script_sha256": profile.request_script_sha256,
                "wrk_binary_sha256": identities["elf_artifacts"]["/opt/deathstarbench-candidate-driver/bin/wrk"]["sha256"],
                "request_script_sha256": identities["baked_file_sha256"]["/opt/deathstarbench-candidate-driver/share/request.lua"],
                "metrics_script_sha256": identities["baked_file_sha256"]["/opt/deathstarbench-candidate-driver/share/metrics.lua"],
                "entrypoint_sha256": identities["baked_file_sha256"]["/opt/deathstarbench-candidate-driver/bin/entrypoint"]}
    if any(fields.get(k) != v for k, v in expected.items()):
        raise PreparationError("Candidate driver attestation differs from inspected runtime artifacts.")


def smoke_image(image: str, *, workload_id: str, image_key: str, architecture: str,
                context: Path) -> dict[str, object]:
    if workload_id not in IMAGE_KEYS or image_key not in IMAGE_KEYS[workload_id] or architecture not in ARCHITECTURES:
        raise PreparationError("Unsupported candidate workload/image/platform combination.")
    if image_key == "load-driver" and architecture != "x86_64":
        raise PreparationError("Candidate load drivers require the fixed native x86 support role.")
    _, _, selector = parse_reference(image)
    if not DIGEST.fullmatch(selector):
        raise PreparationError("Candidate smoke requires an immutable image reference.")
    if context.is_symlink() or not context.is_dir():
        raise PreparationError("Prepared context must be a regular directory.")
    platform = ARCHITECTURES[architecture][0]
    native = _native_runner(architecture)
    verified = resolve_platform(image, platform)
    if verified["image"] != image:
        raise PreparationError("Smoke must name an exact platform manifest, not a multi-platform index.")
    try:
        local = json.loads(_command(["docker", "image", "inspect", image]))
        if len(local) != 1 or local[0].get("Id") != verified["config_digest"] or local[0].get("Architecture") != platform.split("/")[1] or local[0].get("Os") != "linux":
            raise PreparationError("Locally loaded image does not match the verified native config.")
    except (ValueError, TypeError):
        raise PreparationError("Local immutable image inspection failed.") from None
    methods = ["stopped-container-export-no-host-extraction", "all-ELF64-native-architecture", "baked-config-script-license-hashes", "runtime-clone-and-private-key-screen"]
    executions = []
    with tempfile.TemporaryDirectory(prefix="dsb-candidate-image-smoke-") as directory:
        archive = Path(directory) / "filesystem.tar"
        container = _command(["docker", "create", "--network=none", "--read-only", "--user=65532:65532", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--entrypoint=/nonexistent-smoke-not-started", image]).decode().strip()
        if not re.fullmatch(r"[0-9a-f]{64}", container):
            raise PreparationError("Docker did not return an exact inspection-container identity.")
        try:
            with archive.open("wb") as stream:
                _command(["docker", "export", container], timeout=300, stdout_file=stream)
            fs = ImageFilesystem(archive)
            try:
                identities = _inspect_filesystem(fs, workload_id, image_key, architecture, context, verified["config"].get("config", {}))
                public_crypto_selftests = fs.public_crypto_selftest_pem_sha256
            finally:
                fs.close()
        finally:
            _command(["docker", "rm", container])
    dynamic = [path for path, value in identities["elf_artifacts"].items() if value["dynamic_dependencies"]]
    if image_key == "hotel-reservation":
        methods.append("scratch-static-linkage-no-service-startup")
    elif dynamic:
        methods.append("native-hardened-ldd-all-dynamic-ELFs")
    executions = [_run_inspection(image, command, hosts=hosts)
                  for command, hosts in inspection_plan(workload_id, image_key, context, identities)]
    attestation = None
    if image_key == "nginx-web-server":
        methods.extend(["baked-nginx-config-syntax-with-explicit-mock-DNS", "baked-Lua-Thrift-module-loading-no-service-methods"])
    elif image_key == "load-driver":
        attestation = executions[-1]["stdout"]
        _driver_attestation(attestation, workload_id, tree_sha256(context), identities)
        methods.append("candidate-driver-offline-hardened-attestation")
    return {"schema_version": 1, "candidate_schema": "candidate-image-smoke-v1", "workload_id": workload_id,
            "image_key": image_key, "image": image, "index_image": verified["index_image"],
            "platform": platform, "architecture": architecture, "config_digest": verified["config_digest"],
            "image_config": {"os": "linux", "architecture": platform.split("/")[1], "config_digest": verified["config_digest"]},
            "context_sha256": tree_sha256(context), "native_builder": native, "methods": methods,
            "candidate_only": True, "released": False, "runtime_semantics_qualified": False,
            "smoke_execution": {"passed": True, "network": "none", "read_only": True, "user": "65532:65532",
                                "drop_capabilities": ["ALL"], "no_new_privileges": True},
            "load_driver_attestation": attestation, "artifact_identities": identities, "inspection_executions": executions,
            "public_crypto_selftest_pem_sha256": public_crypto_selftests,
            "limitations": ["No database/cache/network service was contacted and no dataset or persistent transaction was qualified.",
                            "Scratch Hotel checks inspect binaries without starting seed-mutating application services.",
                            "Only SHA-bound public GnuTLS self-test PEMs inside system-library ELFs are exempted from the unexpected-key screen; these are never application credentials.",
                            "Frontend mock DNS permits syntax/module checks only, not live endpoint connectivity."]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--workload", required=True, choices=tuple(IMAGE_KEYS))
    parser.add_argument("--image-key", required=True)
    parser.add_argument("--architecture", required=True, choices=tuple(ARCHITECTURES))
    parser.add_argument("--context", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    write_receipt(args.output, smoke_image(args.image, workload_id=args.workload, image_key=args.image_key,
                                         architecture=args.architecture, context=args.context))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
