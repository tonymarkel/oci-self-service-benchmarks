#!/usr/bin/env python3
"""Prepare unreleased Hotel Reservation app contexts, never build or publish.

The exact upstream source and vendored dependency inventory are validated
before occurrence-checked candidate corrections. An immutable Go builder and
scratch runtime avoid mutable build downloads. This is not a published image
lock, byte-reproducible rebuild claim, seed executor, or release attestation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

try:
    from scripts.deathstarbench_artifact_common import (
        PreparationError, UPSTREAM_REPOSITORY, UPSTREAM_REVISION,
        candidate_output, copy_tracked_tree, replace_exact, sha256,
        tree_sha256, validate_tracked_source,
    )
except ModuleNotFoundError as exc:
    if exc.name not in {"scripts", "scripts.deathstarbench_artifact_common"}:
        raise
    from deathstarbench_artifact_common import (
        PreparationError, UPSTREAM_REPOSITORY, UPSTREAM_REVISION,
        candidate_output, copy_tracked_tree, replace_exact, sha256,
        tree_sha256, validate_tracked_source,
    )


ASSET_ROOT = Path(__file__).resolve().parent / "assets/deathstarbench/hotel"
PATCH_REVISION = "hotel-reservation-source-fixes-candidate-v1"
RECIPE_REVISION = "hotel-reservation-native-go-scratch-candidate-v1"
DATASET_REVISION = "hotel-reservation-decimal-user-seed-candidate-v1"
GO_VERSION = "1.21.13"
# Verified read-only against Docker's official registry on 2026-10-08: index,
# platform manifest and config byte hashes; linux architecture; Go version.
GO_BUILDER = (
    "docker.io/library/golang:1.21.13-bookworm@sha256:"
    "c6a5b9308b3f3095e8fde83c8bf4d68bd101fce606c1a0a1394522542509dda9"
)
GO_PLATFORMS = {
    "linux/amd64": {
        "manifest_digest": "sha256:e6ba96dde4af9f4e06caba475fa65e94e4c54fe4aa3e9f4f504c02178eb8934e",
        "config_digest": "sha256:246ea1ed9cdb1164bb5cb7e1f45d7914b98c6d9418c8e1cc443105e820bbd9d1",
    },
    "linux/arm64": {
        "manifest_digest": "sha256:dfceff6afa1332bfb629241677a1272cf914eb51da2e370d47ab9ce8b4d94b0c",
        "config_digest": "sha256:b2069a7515d9403fee6f9f4d9271c76446c420b06c475edd4be5020642238340",
    },
}
UPSTREAM_ANCHORS = {
    "LICENSE": "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4",
    "hotelReservation/Dockerfile": "99651371ef0722a273bbef9c496608593257f4a2a13ec1f3246bdd1b7c048d4a",
    "hotelReservation/config.json": "758febb12cf66301e91755fb4123c4dfca985a6f4a9a461c18b6a9a38bcfddaa",
    "hotelReservation/go.mod": "a5a886b6b67cea384f09f4497cc273b1d710dbe719d9904bd9258446fa38ce90",
    "hotelReservation/go.sum": "347d12f01fb8f6c442a5b5cfb26d3377e702531f6b4a193c6d3c81829a3dd435",
    "hotelReservation/vendor/modules.txt": "525e0e709615e94946514fd43a45d6b090b1b754dfa4828ecfbc917748896bb8",
    "hotelReservation/services/frontend/server.go": "453d3efeeb27c28896434cd1e7fa18f1810678b82835be909094bc629fe7764c",
    "hotelReservation/services/reservation/server.go": "cc24f3bd38b845b0cb9fe591f811dc33abe36d02c53bc1c28f426739d26c1c0e",
    "hotelReservation/cmd/user/db.go": "aced9202d319f1da37060e39513520ff073993572dc12b0869fc24d9dd668d1b",
    "hotelReservation/cmd/attractions/db.go": "be72ded0c1cadc8c515457fda790aa936e7cde307aca2fbf4dcf4d28d0015de2",
    "hotelReservation/cmd/geo/db.go": "1080bc0fe779b9d2523d2289bc52ef7a90fbe9be98da2a730875617003c49219",
    "hotelReservation/cmd/profile/db.go": "cbfa8112e162e62696a28a2bdf23245368ac8f7165dd5ce5bdcd939b23da7191",
    "hotelReservation/cmd/rate/db.go": "ca75806fddf2d6119321de18854672abfd60a988e3de9b7949db6db5c8d66be2",
    "hotelReservation/cmd/recommendation/db.go": "c921b415b63e0ed69d1ccecfdfde8d8671cb268419508df95b827fd9c65899b7",
    "hotelReservation/cmd/reservation/db.go": "c4c8858fd1a6101c65ac898835da9a4a731861c8b9369100f2177e28414a013e",
    "hotelReservation/cmd/review/db.go": "49bc45dd35e5caf27cdb8dcc3b7422e78f2d1db0aa0a7cef98bea17dd80cd740",
    "hotelReservation/vendor/github.com/bradfitz/gomemcache/memcache/memcache.go": "ec73b17dffd561d75edc9d509b85acf340957e237b79f6ed2d4f6c8c75e26810",
}
ORIGINAL_AVAILABILITY_SHA256 = "1cae29e57e2d5becd70037075e25cde54b236f73a42c09dd52212584056a3e6d"
APP_DIRECTORIES = ("cmd", "dialer", "registry", "services", "tls", "tracing", "tune", "vendor")
APP_FILES = ("go.mod", "go.sum", "config.json")
GO_TEST_PACKAGES = ("./services/reservation", "./services/frontend", "./cmd/user")
RELEASE_BLOCKERS = (
    "Eight startup seeders unconditionally InsertMany; duplicate/restart behavior needs a guarded seed contract and attestation.",
    "MakeReservation read/check/write is not an atomic capacity transaction; concurrent oversubscription and partial writes remain unqualified.",
    "Warmup and measured reservations consume persistent inventory; no adopted reset/post-warmup dataset identity exists.",
    "Native image builds, anonymous digest pulls, backing-image locks (MongoDB 5.0, Consul, Jaeger, memcached), semantic measurement, denied-edge and cleanup qualification remain required.",
)


def _asset(name: str) -> Path:
    path = ASSET_ROOT / name
    if path.is_symlink() or not path.is_file():
        raise PreparationError(f"Missing or unsafe Hotel correction asset: {name}")
    return path


def _patched_user(text: str) -> str:
    text = replace_exact(text, 'fmt.Sprintf("Cornell_%x", suffix)',
                         'seededUsername(suffix)', label="decimal seeded username")
    return replace_exact(
        text, "func initializeDatabase(url string) (*mongo.Client, func()) {",
        'func seededUsername(suffix string) string {\n\treturn fmt.Sprintf("Cornell_%s", suffix)\n}\n\n'
        "func initializeDatabase(url string) (*mongo.Client, func()) {",
        label="testable decimal seed formatter",
    )


def _patched_frontend(text: str) -> str:
    old = ('conn, err := dialer.Dial(\n\t\tname,\n\t\tdialer.WithTracer(s.Tracer),\n'
           '\t\tdialer.WithBalancer(s.Registry.Client),\n\t)')
    text = replace_exact(text, old, "conn, err := s.getGprcConn(name)",
                         label="review/attractions Consul resolution", count=2)
    text = replace_exact(
        text, 'fmt.Sprintf("consul://%s/%s.%s", s.ConsulAddr, name, s.KnativeDns)',
        "s.consulDialTarget(name)", label="Knative Consul target")
    text = replace_exact(text, 'fmt.Sprintf("consul://%s/%s", s.ConsulAddr, name)',
                         "s.consulDialTarget(name)", label="standard Consul target")
    return replace_exact(
        text, "func (s *Server) getGprcConn(name string) (*grpc.ClientConn, error) {",
        'func (s *Server) consulDialTarget(name string) string {\n'
        '\tif s.KnativeDns != "" {\n'
        '\t\treturn fmt.Sprintf("consul://%s/%s.%s", s.ConsulAddr, name, s.KnativeDns)\n'
        '\t}\n\treturn fmt.Sprintf("consul://%s/%s", s.ConsulAddr, name)\n}\n\n'
        "func (s *Server) getGprcConn(name string) (*grpc.ClientConn, error) {",
        label="testable Consul target helper",
    )


def _patched_reservation(text: str) -> str:
    start = "// CheckAvailability checks if given information is available\n"
    end = "\ntype reservation struct {"
    if text.count(start) != 1 or text.count(end) != 1:
        raise PreparationError("Reservation availability method boundary drifted.")
    first, last = text.index(start), text.index(end)
    original = text[first:last]
    if hashlib.sha256(original.encode("utf-8")).hexdigest() != ORIGINAL_AVAILABILITY_SHA256:
        raise PreparationError("Reservation availability method SHA-256 drifted.")
    corrected = _asset("availability.go").read_text(encoding="utf-8").rstrip() + "\n"
    text = replace_exact(text, original, corrected, label="cache membership and capacity reads")
    return replace_exact(
        text, 'memc_key := hotelId + "_" + inDate.String()[0:10] + "_" + outdate',
        'memc_key := reservationCacheKey(hotelId, indate, outdate)',
        label="reservation write nightly key",
    )


def _dockerfile() -> str:
    return f'''# {RECIPE_REVISION}; candidate only, BuildKit with native workers required.
FROM --platform=$TARGETPLATFORM {GO_BUILDER} AS builder
ARG BUILDPLATFORM
ARG TARGETPLATFORM
ARG TARGETARCH
WORKDIR /workspace
ENV CGO_ENABLED=0 GO111MODULE=on GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOAMD64=v1
COPY go.mod go.sum ./
COPY vendor/ vendor/
COPY cmd/ cmd/
COPY dialer/ dialer/
COPY registry/ registry/
COPY services/ services/
COPY tls/ tls/
COPY tracing/ tracing/
COPY tune/ tune/
COPY config.json config.json
RUN --network=none test "$BUILDPLATFORM" = "$TARGETPLATFORM" \\
    && case "$TARGETPLATFORM" in linux/amd64|linux/arm64) ;; *) exit 1 ;; esac \\
    && test "$(go env GOARCH)" = "$TARGETARCH" \\
    && test "$(go version | awk '{{print $3}}')" = "go{GO_VERSION}" \\
    && go test -mod=vendor -vet=off {' '.join(GO_TEST_PACKAGES)} \\
    && go install -trimpath -ldflags="-s -w -buildid=" -mod=vendor ./cmd/...
FROM scratch
WORKDIR /workspace
ENV TLS=false GOGC=100 MEMC_TIMEOUT=2 LOG_LEVEL=INFO
COPY --from=builder /go/bin/ /go/bin/
COPY config.json /workspace/config.json
COPY LICENSE.deathstarbench /usr/share/licenses/deathstarbench/LICENSE
COPY licenses/vendor/ /usr/share/licenses/deathstarbench/vendor/
COPY --from=builder /usr/local/go/LICENSE /usr/share/licenses/go/LICENSE
USER 65532:65532
CMD ["/go/bin/frontend"]
'''


def _inventory(upstream: Path) -> list[dict[str, str]]:
    names = subprocess.run(
        ["git", "-C", str(upstream), "ls-files", "-z", "--", "hotelReservation", "LICENSE"],
        check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    ).stdout.decode("utf-8").split("\0")
    return [{"path": name, "sha256": sha256(upstream / name)} for name in sorted(filter(None, names))]


def _json_digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def prepare_contexts(upstream: Path, output: Path) -> dict[str, object]:
    upstream = validate_tracked_source(
        upstream, relative_roots=("hotelReservation", "LICENSE"), anchors=UPSTREAM_ANCHORS,
    )
    inventory = _inventory(upstream)
    with candidate_output(upstream, output, protected_paths=(ASSET_ROOT, Path(__file__))) as output:
        app = output / "app"
        app.mkdir(mode=0o755)
        for relative in (*APP_DIRECTORIES, *APP_FILES):
            copy_tracked_tree(upstream, "hotelReservation/" + relative, app / relative)
        copy_tracked_tree(upstream, "LICENSE", app / "LICENSE.deathstarbench")
        patches = {}
        for relative, patcher in (
            ("cmd/user/db.go", _patched_user),
            ("services/frontend/server.go", _patched_frontend),
            ("services/reservation/server.go", _patched_reservation),
        ):
            path = app / relative
            original = sha256(path)
            path.write_text(patcher(path.read_text(encoding="utf-8")), encoding="utf-8")
            path.chmod(0o644)
            patches["hotelReservation/" + relative] = {"original_sha256": original, "patched_sha256": sha256(path)}
        fixtures = {}
        for name, relative in (
            ("availability_test.go", "services/reservation/candidate_availability_test.go"),
            ("frontend_resolver_test.go", "services/frontend/candidate_resolver_test.go"),
            ("user_seed_test.go", "cmd/user/candidate_seed_test.go"),
        ):
            source = _asset(name)
            shutil.copyfile(source, app / relative)
            (app / relative).chmod(0o644)
            fixtures[relative] = sha256(source)
        licenses = []
        for path in sorted((app / "vendor").rglob("*")):
            if path.is_file() and path.name.upper().startswith(("LICENSE", "LICENCE", "COPYING", "NOTICE", "COPYRIGHT")):
                relative = path.relative_to(app / "vendor")
                destination = app / "licenses/vendor" / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, destination)
                destination.chmod(0o644)
                licenses.append({"path": relative.as_posix(), "sha256": sha256(path)})
        if not licenses:
            raise PreparationError("No vendored Go dependency licenses found.")
        (app / "Dockerfile.candidate").write_text(_dockerfile(), encoding="utf-8")
        (app / "Dockerfile.candidate").chmod(0o644)
        # mkdir modes are filtered by umask; fingerprint canonical modes, not
        # the caller's local directory-creation preferences.
        for path in (output, app, *app.rglob("*")):
            if path.is_symlink():
                continue
            path.chmod(0o755 if path.is_dir() or path.stat().st_mode & 0o111 else 0o644)
        manifest = {
            "schema_version": 1, "workload_id": "hotel_reservation",
            "candidate_only": True, "released": False,
            "upstream_repository": UPSTREAM_REPOSITORY, "upstream_revision": UPSTREAM_REVISION,
            "patch_revision": PATCH_REVISION, "recipe_revision": RECIPE_REVISION,
            "dataset_revision": DATASET_REVISION, "source_inventory": inventory,
            "source_inventory_sha256": _json_digest(inventory),
            "source_patches": patches,
            "correction_asset_sha256": sha256(_asset("availability.go")),
            "semantic_test_fixtures": fixtures,
            "contexts": {"app": {"path": "app", "dockerfile": "Dockerfile.candidate", "context_sha256": tree_sha256(app)}},
            "software": {"go_version": GO_VERSION, "builder": GO_BUILDER,
                         "builder_platforms": GO_PLATFORMS, "runtime_base": "scratch",
                         "go_mod_sha256": sha256(app / "go.mod"), "go_sum_sha256": sha256(app / "go.sum"),
                         "vendor_inventory_sha256": tree_sha256(app / "vendor")},
            "license_identities": {"deathstarbench_license_sha256": sha256(app / "LICENSE.deathstarbench"),
                                   "vendored_licenses": licenses, "go_license_from_immutable_builder": True},
            "native_platforms": ["linux/amd64", "linux/arm64"],
            "config_sha256": sha256(app / "config.json"),
            "runtime_git_clone": False, "runtime_build_tools": False, "runtime_demo_private_keys": False,
            "build_network_required": False, "byte_reproducible_rebuild": False,
            "release_blockers": list(RELEASE_BLOCKERS),
        }
        path = output / "context-manifest.json"
        path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        path.chmod(0o644)
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
