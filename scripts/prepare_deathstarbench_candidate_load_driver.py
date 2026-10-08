#!/usr/bin/env python3
"""Prepare Hotel/Media amd64 candidate wrk2 contexts; never publish or release.

Inputs and original request identities are verified before copying. The pinned
Social recipe is reused without mutation solely for wrk2/LuaJIT/LuaSocket and
its amd64 base/runtime dependency recipe. Each candidate has its own prepared
request, seed, body-check method, context and attestation identity. This does
not qualify endpoint semantics or a rebuild against mutable package archives.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.deathstarbench_workload_contract import DISTRIBUTED_WORKLOAD_PROFILES
from scripts import prepare_deathstarbench_load_driver as social
from scripts.deathstarbench_artifact_common import (
    PreparationError,
    candidate_output,
    copy_tracked_tree,
    replace_exact,
    sha256,
    tree_sha256,
    validate_tracked_source,
)


WORKLOAD_IDS = ("hotel_reservation", "media_microservices")
ASSET_ROOT = ROOT / "scripts/assets/deathstarbench/driver"
SEED_BASE = 20261008
SEED_METHOD = "luajit-per-worker-20261008-plus-index-v1"
MEASUREMENT_METHOD = "candidate-body-v1"
UPSTREAM_LICENSE_SHA256 = "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4"


def _profile(workload_id: str):
    if workload_id not in WORKLOAD_IDS:
        raise PreparationError("Only Hotel Reservation and Media Microservices are candidates.")
    return DISTRIBUTED_WORKLOAD_PROFILES[workload_id]


def prepare_request_script(workload_id: str, original: str) -> str:
    """Apply narrowly anchored changes after the caller verifies source bytes."""

    _profile(workload_id)
    if workload_id == "hotel_reservation":
        script = replace_exact(
            original,
            'math.randomseed(socket.gettime()*1000)\nmath.random(); math.random(); math.random()\n',
            '-- Candidate deterministic per-worker seeding is performed by init().\n',
            label="Hotel random seed",
        )
        script = replace_exact(
            script, 'local url = "http://localhost:5000"',
            'local url = "http://localhost:8080"', label="Hotel frontend target",
        )
        # Upstream reserve() references lat/lon that are local to other request
        # functions. Give it the same geographic sampling method explicitly.
        script = replace_exact(
            script, '  local num_room = "1"\n',
            '  local num_room = "1"\n'
            '  local lat = 38.0235 + (math.random(0, 481) - 240.5)/1000.0\n'
            '  local lon = -122.095 + (math.random(0, 325) - 157.0)/1000.0\n',
            label="Hotel reservation coordinates",
        )
        for endpoint in ('"/hotels?inDate="', '"/recommendations?require="',
                         '"/reservation?inDate="', '"/user?username="'):
            if script.count(endpoint) != 1:
                raise PreparationError("The Hotel request endpoint anchors drifted.")
    else:
        script = replace_exact(
            original, 'math.randomseed(os.time())\nmath.random(); math.random(); math.random()\n',
            '-- Candidate deterministic per-worker seeding is performed by init().\n'
            'local url = "http://localhost:8080"\n', label="Media random seed and base URL",
        )
        if script.count('local path = url .. "/wrk2-api/review/compose"') != 1:
            raise PreparationError("The Media compose-review target anchor drifted.")
    if script.count("http://localhost:8080") != 1 or "math.randomseed(" in script:
        raise PreparationError("The prepared candidate target or random seed drifted.")
    return script


def _validate_git_blobs(repository: Path, tree: str, source_root: Path, *, allowed_gitlinks: dict[str, str]):
    """Compare bytes and executable modes even when Git status hides changes.

    assume-unchanged/skip-worktree and core.filemode=false must not turn status
    into evidence of intact sources. Existing Social inventory validation still
    rejects ignored/untracked copied entries and verifies the recursive HEADs.
    """
    try:
        raw = subprocess.run(["git", "-C", str(repository), "ls-tree", "-r", "-z", tree],
                             check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise PreparationError("Candidate wrk2/LuaJIT Git blob validation failed.") from exc
    observed_gitlinks = {}
    entries = 0
    for item in raw.split(b"\0"):
        if not item:
            continue
        try:
            header, relative_bytes = item.split(b"\t", 1)
            mode, kind, blob = header.decode("ascii").split()
            relative = relative_bytes.decode("utf-8")
        except (ValueError, UnicodeError) as exc:
            raise PreparationError("Invalid candidate wrk2/LuaJIT Git inventory.") from exc
        path = Path(relative)
        if (not relative or path.is_absolute() or ".." in path.parts or ".git" in path.parts
                or "\\" in relative or path.as_posix() != relative):
            raise PreparationError("Unsafe candidate wrk2/LuaJIT Git source path.")
        if kind == "commit" and mode == "160000":
            observed_gitlinks[relative] = blob
            continue
        entry = source_root / path
        try:
            physical = entry.lstat().st_mode
            if kind != "blob" or mode not in ("100644", "100755", "120000"):
                raise PreparationError("Unsupported candidate wrk2/LuaJIT Git entry.")
            if mode == "120000":
                if not stat.S_ISLNK(physical):
                    raise PreparationError("Candidate tracked symlink type drifted.")
                payload = os.readlink(entry).encode("utf-8")
            else:
                if not stat.S_ISREG(physical) or bool(physical & 0o111) != (mode == "100755"):
                    raise PreparationError("Candidate tracked executable mode or file type drifted.")
                payload = entry.read_bytes()
        except OSError as exc:
            raise PreparationError("Candidate tracked wrk2/LuaJIT file is missing or unreadable.") from exc
        physical_blob = hashlib.sha1(b"blob " + str(len(payload)).encode("ascii") + b"\0" + payload).hexdigest()
        if physical_blob != blob:
            raise PreparationError("Candidate tracked wrk2/LuaJIT source bytes drifted.")
        entries += 1
    if not entries or observed_gitlinks != allowed_gitlinks:
        raise PreparationError("Candidate wrk2/LuaJIT Gitlink inventory drifted.")


def _validate_wrk_git_bytes(upstream: Path, wrk2: Path):
    _validate_git_blobs(upstream, "HEAD:wrk2", wrk2,
                        allowed_gitlinks={"deps/luajit": social.LUAJIT_REVISION})
    _validate_git_blobs(wrk2 / "deps/luajit", "HEAD", wrk2 / "deps/luajit", allowed_gitlinks={})


def _validate_inputs(workload_id: str, upstream: Path, luasocket_rock: Path):
    profile = _profile(workload_id)
    if upstream.is_symlink() or not upstream.is_dir():
        raise PreparationError("The upstream input must be a regular checkout directory.")
    # Deliberately validate the existing Social script too rather than change
    # that module's globals to choose another workload (unsafe in parallel).
    try:
        wrk2, _ = social._validate_upstream(upstream)
    except social.PreparationError as exc:
        raise PreparationError(str(exc)) from exc
    _validate_wrk_git_bytes(upstream, wrk2)
    request = upstream / profile.request_script_path
    validate_tracked_source(
        upstream, relative_roots=("LICENSE", profile.request_script_path),
        anchors={"LICENSE": UPSTREAM_LICENSE_SHA256,
                 profile.request_script_path: profile.request_script_sha256},
    )
    if luasocket_rock.is_symlink() or not luasocket_rock.is_file():
        raise PreparationError("LuaSocket input must be a regular, non-symlink source rock.")
    if sha256(luasocket_rock) != social.LUASOCKET_SOURCE_SHA256:
        raise PreparationError("The pinned LuaSocket source rock drifted.")
    try:
        socket_license = social._extract_luasocket_license(luasocket_rock)
        # This also validates the copied symlink targets (the pinned wrk2
        # tracked inventory was established above).
        wrk_source_sha = social._tree_sha256(wrk2)
    except social.PreparationError as exc:
        raise PreparationError(str(exc)) from exc
    try:
        original = request.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise PreparationError("The pinned candidate request script is not readable UTF-8.") from exc
    return profile, wrk2, request, prepare_request_script(workload_id, original), socket_license, wrk_source_sha


def _dockerfile(profile, prepared_sha: str, metrics_sha: str, entrypoint_sha: str) -> str:
    """Preserve the audited shared compiler recipe; bind candidate-only assets."""

    text = social._dockerfile()
    replacements = (
        ("mixed-workload.lua", "request.lua", 5),
        ("/opt/deathstarbench-load-driver", "/opt/deathstarbench-candidate-driver", 17),
        (social.MIXED_WORKLOAD_LUA_SHA256, prepared_sha, 2),
        (social.LOAD_DRIVER_REVISION, profile.load_driver_revision, 2),
        ("DeathStarBench wrk2 load driver", f"DeathStarBench {profile.name} candidate load driver", 1),
    )
    for old, new, count in replacements:
        text = replace_exact(text, old, new, count=count, label="candidate build recipe")
    text = replace_exact(
        text, '    make -C /src/wrk2 -j"$(nproc)"; \\\n',
        f'    test "$(sha256sum /src/metrics.lua | awk \'{{print $1}}\')" = "{metrics_sha}"; \\\n'
        f'    test "$(sha256sum /src/entrypoint | awk \'{{print $1}}\')" = "{entrypoint_sha}"; \\\n'
        '    make -C /src/wrk2 -j"$(nproc)"; \\\n', label="candidate build asset verification",
    )
    text = replace_exact(
        text, f"      'REQUEST_SCRIPT_SHA256={prepared_sha}' \\\n",
        f"      'WORKLOAD_ID={profile.workload_id}' \\\n"
        f"      'ORIGINAL_REQUEST_SCRIPT_SHA256={profile.request_script_sha256}' \\\n"
        f"      'REQUEST_SCRIPT_SHA256={prepared_sha}' \\\n"
        f"      'METRICS_SCRIPT_SHA256={metrics_sha}' \\\n"
        f"      'ENTRYPOINT_SHA256={entrypoint_sha}' \\\n"
        f"      'SEED_BASE={SEED_BASE}' \\\n", label="candidate artifact manifest",
    )
    text = replace_exact(
        text, "USER 65532:65532\nWORKDIR /tmp\n",
        "USER 65532:65532\nWORKDIR /tmp\n"
        "RUN --network=none /opt/deathstarbench-candidate-driver/bin/entrypoint attest\n",
        label="unprivileged offline candidate attestation",
    )
    return text


def _prepare_context(workload_id: str, upstream: Path, luasocket_rock: Path, output: Path) -> dict:
    profile, wrk2, request, prepared, socket_license, wrk_source_sha = _validate_inputs(
        workload_id, upstream, luasocket_rock,
    )
    metrics = (
        f'local candidate_workload = "{workload_id}"\n'
        f'local candidate_seed_base = {SEED_BASE}\n'
        + (ASSET_ROOT / "candidate-metrics.lua").read_text(encoding="utf-8")
    )
    entrypoint = (ASSET_ROOT / "candidate-entrypoint").read_text(encoding="utf-8")
    prepared_sha = hashlib.sha256(prepared.encode("utf-8")).hexdigest()
    metrics_sha = hashlib.sha256(metrics.encode("utf-8")).hexdigest()
    entrypoint_sha = hashlib.sha256(entrypoint.encode("utf-8")).hexdigest()
    dockerfile = _dockerfile(profile, prepared_sha, metrics_sha, entrypoint_sha)
    context = output / "context"
    context.mkdir()
    context.chmod(0o755)
    # The exact Git inventories have already been checked by the Social helper.
    social._copy_source_tree(wrk2, context / "wrk2")
    shutil.copyfile(luasocket_rock, context / social.LUASOCKET_FILENAME)
    (context / social.LUASOCKET_FILENAME).chmod(0o644)
    copy_tracked_tree(upstream, profile.request_script_path, context / "original-request.lua")
    (context / "request.lua").write_text(prepared, encoding="utf-8")
    (context / "metrics.lua").write_text(metrics, encoding="utf-8")
    (context / "entrypoint").write_text(entrypoint, encoding="utf-8")
    (context / "entrypoint").chmod(0o755)
    (context / "Dockerfile").write_text(dockerfile, encoding="utf-8")
    (context / ".dockerignore").write_text(
        "*\n!/.dockerignore\n!/Dockerfile\n!/entrypoint\n!/metrics.lua\n"
        "!/request.lua\n!/original-request.lua\n!/luasocket-3.1.0-1.src.rock\n"
        "!/wrk2/\n!/wrk2/**\n!/licenses/\n!/licenses/**\n", encoding="utf-8",
    )
    licenses = context / "licenses"
    licenses.mkdir()
    licenses.chmod(0o755)
    for relative, name in (
        ("LICENSE", "DeathStarBench-LICENSE"), ("wrk2/LICENSE", "wrk2-LICENSE"),
        ("wrk2/deps/luajit/COPYRIGHT", "LuaJIT-COPYRIGHT"),
    ):
        copy_tracked_tree(upstream, relative, licenses / name)
    (licenses / "LuaSocket-LICENSE").write_bytes(socket_license)
    for generated in (context / "request.lua", context / "metrics.lua", context / "Dockerfile",
                      context / ".dockerignore", licenses / "LuaSocket-LICENSE"):
        generated.chmod(0o644)
    copied_wrk_sha = social._tree_sha256(context / "wrk2")
    if copied_wrk_sha != wrk_source_sha:
        raise PreparationError("The copied wrk2 source identity drifted.")
    manifest = {
        "schema_version": 1, "candidate_schema": "candidate-driver-v1",
        "workload_id": workload_id, "candidate_only": True,
        "released": False, "measurement_qualified": False,
        "load_driver_revision": profile.load_driver_revision,
        "measurement_revision": profile.measurement_revision,
        "measurement_method": MEASUREMENT_METHOD,
        "upstream_repository": social.UPSTREAM_REPOSITORY,
        "upstream_revision": social.UPSTREAM_REVISION,
        "architecture": "x86_64", "platform": "linux/amd64",
        "base_image": social.ROCKY_LINUX_9_MINIMAL_AMD64,
        "context_path": "context", "context_sha256": tree_sha256(context),
        "wrk2_tree_git_sha": social.WRK2_TREE_GIT_SHA,
        "wrk2_source_sha256": copied_wrk_sha,
        "wrk2_source_hash_method": "legacy-social-tree-v1",
        "luajit_revision": social.LUAJIT_REVISION,
        "luajit_source_sha256": tree_sha256(context / "wrk2/deps/luajit"),
        "luasocket_source_url": social.LUASOCKET_URL,
        "luasocket_source_sha256": social.LUASOCKET_SOURCE_SHA256,
        "original_request_script_path": profile.request_script_path,
        "original_request_script_sha256": profile.request_script_sha256,
        "prepared_request_script_sha256": prepared_sha,
        "metrics_script_sha256": metrics_sha, "entrypoint_sha256": entrypoint_sha,
        "licenses_sha256": {path.name: sha256(path) for path in sorted(licenses.iterdir())},
        "seed_base": SEED_BASE, "seed_method": SEED_METHOD,
        "frontend_node_port": 8080, "prepared_target_anchor_count": 1,
        "runtime_source_network_required": False, "attestation_network_required": False,
        "benchmark_private_network_required": True, "runtime_build_tools": False,
        "runtime_user": "65532:65532", "runtime_read_only": True,
        "runtime_drop_capabilities": ["ALL"], "runtime_no_new_privileges": True,
        "runtime_tmpfs": "/tmp:rw,noexec,nosuid,nodev,size=64m",
        "limitations": [
            "Candidate context is not a published platform digest lock or release gate.",
            "Build package repositories remain mutable; rebuilding is not byte-reproducible.",
            "wrk2 response callbacks lack request/connection identity; endpoint semantics are not qualified.",
            "Hotel GeoJSON checks are structural, not full JSON/dataset validation; reservation exhaustion counts as failure.",
            "Per-worker seeds fix random streams, not concurrent request wall-clock ordering.",
            "Callback counters must reconcile with wrk summary; no production measurement accepts this candidate schema.",
        ],
    }
    (output / "context-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    (output / "context-manifest.json").chmod(0o644)
    return manifest


def prepare_context(workload_id: str, upstream: Path, luasocket_rock: Path, output: Path) -> dict:
    _profile(workload_id)
    # The context manager rejects existing/overlapping targets and cleans only
    # the fresh output it owns if preparation fails.
    with candidate_output(upstream, output, protected_paths=(luasocket_rock,)) as destination:
        return _prepare_context(workload_id, upstream, luasocket_rock, destination)


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", required=True, choices=WORKLOAD_IDS)
    parser.add_argument("--upstream", required=True, type=Path)
    parser.add_argument("--luasocket-rock", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def main():
    arguments = _parser().parse_args()
    # Preserve supplied symlink spellings so safety checks can reject them.
    prepare_context(arguments.workload, arguments.upstream, arguments.luasocket_rock, arguments.output)
    print(arguments.output / "context-manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
