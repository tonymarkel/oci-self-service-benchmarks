import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.deathstarbench_contract import (
    DISTRIBUTED_IMAGE_SET_REVISION,
    DISTRIBUTED_WORKLOAD_REVISION,
)
from app.deathstarbench_k3s_workload import (
    REQUIRED_IMAGE_KEYS,
    validate_image_lock,
)
from app.k3s_runtime import K3S_CLUSTER_DNS_IP


ROOT = Path(__file__).resolve().parents[1]


def load_script(name, relative_path):
    specification = importlib.util.spec_from_file_location(
        name, ROOT / relative_path
    )
    module = importlib.util.module_from_spec(specification)
    assert specification.loader is not None
    specification.loader.exec_module(module)
    return module


prepare = load_script(
    "prepare_deathstarbench_social_images",
    "scripts/prepare_deathstarbench_social_images.py",
)
image_lock = load_script(
    "create_deathstarbench_image_lock",
    "scripts/create_deathstarbench_image_lock.py",
)


def write(path, content="fixture\n"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def minimal_upstream(root):
    write(root / "LICENSE", "Apache License 2.0 fixture\n")
    social = root / "socialNetwork"
    write(social / ".dockerignore", "*\n!/src/\n")
    write(
        social / "Dockerfile",
        "FROM yg397/thrift-microservice-deps:xenial AS builder\n"
        "ARG LIB_REDIS_PLUS_PLUS_VERSION=1.2.3\n"
        "RUN echo get_shards_pool\n"
        "COPY ./ /social-network-microservices\n"
        "FROM ubuntu:16.04\n"
        "WORKDIR /social-network-microservices\n",
    )
    write(
        social / "docker/thrift-microservice-deps/cpp/Dockerfile",
        "FROM ubuntu:16.04\nRUN echo get_shards_pool\n",
    )
    write(
        social / "docker/openresty-thrift/xenial/Dockerfile",
        'ARG RESTY_IMAGE_BASE="ubuntu"\n'
        "ARG RESTY_PCRE_VERSION=8.42\n"
        "FROM ${RESTY_IMAGE_BASE}:xenial\n"
        "RUN curl http://ftp.cs.stanford.edu/pub/exim/pcre/"
        "pcre-${RESTY_PCRE_VERSION}.tar.gz\n"
        + prepare.OPENRESTY_ROCKS_ORIGINAL
        + "\n",
    )
    write(social / "docker/openresty-thrift/lua-thrift/module.lua")
    write(
        social / "docker/media-frontend/xenial/Dockerfile",
        "FROM ubuntu:xenial\n"
        "ARG RESTY_PCRE_VERSION=8.41\n"
        "RUN curl http://ftp.cs.stanford.edu/pub/exim/pcre/"
        "pcre-${RESTY_PCRE_VERSION}.tar.gz\n",
    )
    write(social / "docker/media-frontend/lualongnumber/source.c")
    write(social / "gen-lua/service.lua")
    write(social / "nginx-web-server/lua-scripts/api.lua")
    write(social / "nginx-web-server/pages/index.html")
    write(
        social / "nginx-web-server/conf/nginx.conf",
        "resolver 127.0.0.11;\n",
    )
    write(
        social / "nginx-web-server/jaeger-config.json",
        '{"service_name":"nginx-web-server","diabled": false}\n',
    )
    write(social / "keys/CA.pem")
    write(social / "keys/server.key", "private fixture\n")
    write(social / "keys/server.pem", "private fixture\n")
    write(social / "media-frontend/lua-scripts/media.lua")
    write(
        social / "media-frontend/conf/nginx.conf",
        "resolver 127.0.0.11;\n",
    )
    write(social / "config/service-config.json", "{}\n")
    write(social / "config/jaeger-config.yml", "disabled: false\n")
    write(
        social
        / "helm-chart/socialnetwork/templates/configs/nginx/nginx.tpl",
        '{{- define "socialnetwork.templates.nginx.nginx.conf"  }}\n'
        "env fqdn_suffix;\n"
        "http {\n"
        "  # Docker default hostname resolver. Set valid timeout to prevent unlimited\n"
        "  # ttl for resolver caching.\n"
        "  # resolver 127.0.0.11 valid=10s ipv6=off;\n"
        "  resolver {{ .Values.global.nginx.resolverName }} valid=10s ipv6=off;\n"
        "}\n"
        "{{- end }}\n",
    )
    write(
        social
        / "helm-chart/socialnetwork/templates/configs/media-service/nginx.tpl",
        '{{- define "socialnetwork.templates.media-service.nginx.conf"  }}\n'
        "env fqdn_suffix;\n"
        "http {\n"
        "  # Docker default hostname resolver\n"
        "  resolver {{ .Values.global.nginx.resolverName }} valid=10s ipv6=off;\n"
        "}\n"
        "{{- end }}\n",
    )
    return social


class DeathStarBenchContextPreparationTests(unittest.TestCase):
    def test_contexts_combine_build_stages_and_bake_runtime_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            temporary_path = Path(temporary)
            upstream = temporary_path / "upstream"
            output = temporary_path / "contexts"
            minimal_upstream(upstream)

            with mock.patch.object(prepare, "_validate_upstream"):
                manifest = prepare.prepare_contexts(upstream, output)

            application = (output / "app/Dockerfile.candidate").read_text()
            frontend = (output / "frontend/Dockerfile.candidate").read_text()
            media = (
                output / "media_frontend/Dockerfile.candidate"
            ).read_text()
            frontend_nginx = (
                output / "frontend/runtime/nginx.conf"
            ).read_text()
            frontend_jaeger = (
                output / "frontend/runtime/jaeger-config.json"
            ).read_text()
            media_nginx = (
                output / "media_frontend/runtime/nginx.conf"
            ).read_text()

            self.assertIn("AS social-dependencies", application)
            self.assertIn("FROM social-dependencies AS builder", application)
            self.assertNotIn("yg397/thrift-microservice-deps", application)
            self.assertEqual(application.count("get_shards_pool"), 1)
            self.assertIn(
                "COPY ./config/service-config.json "
                "/social-network-microservices/config/service-config.json",
                application,
            )
            self.assertIn(
                "COPY ./config/jaeger-config.yml "
                "/social-network-microservices/config/jaeger-config.yml",
                application,
            )
            self.assertIn("!/config/", (output / "app/.dockerignore").read_text())
            self.assertIn("COPY runtime/gen-lua /gen-lua", frontend)
            self.assertIn(
                "COPY runtime/lua-scripts /usr/local/openresty/nginx/lua-scripts",
                frontend,
            )
            self.assertIn(
                "COPY runtime/lua-scripts /usr/local/openresty/nginx/lua-scripts",
                media,
            )
            self.assertNotIn("dimoibiehg", frontend + media)
            self.assertEqual(prepare.K3S_CLUSTER_DNS_IP, K3S_CLUSTER_DNS_IP)
            for nginx_config in (frontend_nginx, media_nginx):
                self.assertIn(
                    f"resolver {K3S_CLUSTER_DNS_IP} valid=10s ipv6=off;",
                    nginx_config,
                )
                self.assertIn("env fqdn_suffix;", nginx_config)
                self.assertNotIn("127.0.0.11", nginx_config)
                self.assertNotIn("{{", nginx_config)
                self.assertNotIn("}}", nginx_config)
            self.assertIn('"disabled": false', frontend_jaeger)
            self.assertNotIn('"diabled"', frontend_jaeger)
            self.assertIn(
                'org.opencontainers.image.source="${OCI_SOURCE_REPOSITORY}"',
                application + frontend + media,
            )
            self.assertIn(
                "io.github.oci-self-service-benchmarks.upstream.revision",
                application + frontend + media,
            )
            for context_name, dockerfile in (
                ("app", application),
                ("frontend", frontend),
                ("media_frontend", media),
            ):
                copied_license = (
                    output
                    / context_name
                    / prepare.UPSTREAM_LICENSE_CONTEXT_NAME
                )
                self.assertEqual(
                    copied_license.read_bytes(),
                    (upstream / "LICENSE").read_bytes(),
                )
                self.assertIn(
                    f"COPY {prepare.UPSTREAM_LICENSE_CONTEXT_NAME} "
                    f"{prepare.UPSTREAM_LICENSE_IMAGE_PATH}",
                    dockerfile,
                )
                self.assertIn(
                    'org.opencontainers.image.licenses="Apache-2.0"',
                    dockerfile,
                )
                self.assertIn(
                    f"!/{prepare.UPSTREAM_LICENSE_CONTEXT_NAME}",
                    (output / context_name / ".dockerignore").read_text(),
                )
            self.assertNotIn(
                'org.opencontainers.image.source="https://github.com/'
                'delimitrou/DeathStarBench"',
                application + frontend + media,
            )
            self.assertTrue((output / "frontend/runtime/gen-lua/service.lua").is_file())
            self.assertTrue(
                (output / "media_frontend/runtime/lua-scripts/media.lua").is_file()
            )
            self.assertTrue((output / "frontend/runtime/keys/CA.pem").is_file())
            self.assertFalse(list(output.rglob("server.key")))
            self.assertFalse(list(output.rglob("server.pem")))
            self.assertNotIn("server.key", frontend + media)
            self.assertNotIn("server.pem", frontend + media)
            self.assertFalse(manifest["runtime_git_clone"])
            self.assertFalse(manifest["byte_reproducible_rebuild"])
            self.assertEqual(
                manifest["third_party_images"],
                {
                    "mongo": "docker.io/library/mongo:4.4.6",
                    "redis": "docker.io/library/redis:7.2.4",
                    "memcached": "docker.io/library/memcached:1.6.26",
                    "jaeger": "docker.io/jaegertracing/all-in-one:1.57.0",
                },
            )

    def test_pinned_upstream_license_is_a_drift_checked_input(self):
        self.assertEqual(
            prepare.UPSTREAM_ANCHORS["LICENSE"],
            "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4",
        )

    def test_anchor_drift_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            upstream = Path(temporary)
            anchor = upstream / "socialNetwork/Dockerfile"
            write(anchor, "changed\n")
            expected = hashlib.sha256(b"expected\n").hexdigest()
            with mock.patch.object(
                prepare, "UPSTREAM_ANCHORS", {"socialNetwork/Dockerfile": expected}
            ), mock.patch.object(
                prepare,
                "_run_git",
                side_effect=[prepare.UPSTREAM_REVISION, ""],
            ):
                with self.assertRaisesRegex(prepare.PreparationError, "anchor drift"):
                    prepare._validate_upstream(upstream)

    def test_wrong_revision_fails_before_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            upstream = Path(temporary)
            with mock.patch.object(
                prepare, "_run_git", side_effect=["0" * 40, ""]
            ):
                with self.assertRaisesRegex(prepare.PreparationError, "Expected upstream"):
                    prepare._validate_upstream(upstream)


def context_manifest(path):
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "candidate_only": True,
                "upstream_repository": prepare.UPSTREAM_REPOSITORY,
                "upstream_revision": prepare.UPSTREAM_REVISION,
                "runtime_git_clone": False,
                "runtime_source_baked_into_images": True,
                "byte_reproducible_rebuild": False,
                "contexts": {
                    name: {
                        "path": name,
                        "dockerfile": "Dockerfile.candidate",
                        "context_sha256": character * 64,
                    }
                    for name, character in (
                        ("app", "a"),
                        ("frontend", "b"),
                        ("media_frontend", "c"),
                    )
                },
                "third_party_images": prepare.THIRD_PARTY_IMAGES,
            }
        ),
        encoding="utf-8",
    )


def raw_index(include_arm64=True):
    manifests = [
        {
            "digest": "sha256:" + "1" * 64,
            "platform": {"os": "linux", "architecture": "amd64"},
        },
        {
            "digest": "sha256:" + "f" * 64,
            "platform": {"os": "unknown", "architecture": "unknown"},
        },
    ]
    if include_arm64:
        manifests.append(
            {
                "digest": "sha256:" + "2" * 64,
                "platform": {"os": "linux", "architecture": "arm64"},
            }
        )
    return json.dumps({"schemaVersion": 2, "manifests": manifests}).encode()


class DeathStarBenchImageLockTests(unittest.TestCase):
    def test_lock_records_exact_platform_digests_for_all_images(self):
        with tempfile.TemporaryDirectory() as temporary:
            manifest_path = Path(temporary) / "context.json"
            context_manifest(manifest_path)
            custom = {
                "app": "ghcr.io/example/app:run@sha256:" + "a" * 64,
                "frontend": "ghcr.io/example/frontend:run@sha256:" + "b" * 64,
                "media_frontend": (
                    "ghcr.io/example/media-frontend:run@sha256:" + "c" * 64
                ),
            }
            inspected = []

            def inspector(reference):
                inspected.append(reference)
                return raw_index()

            lock = image_lock.build_lock(
                manifest_path, custom, inspector=inspector
            )

            self.assertEqual(
                set(lock),
                {
                    "schema_version",
                    "workload_revision",
                    "image_set_revision",
                    "upstream_revision",
                    "released",
                    "platforms",
                },
            )
            self.assertEqual(
                lock["workload_revision"], DISTRIBUTED_WORKLOAD_REVISION
            )
            self.assertEqual(
                lock["image_set_revision"], DISTRIBUTED_IMAGE_SET_REVISION
            )
            self.assertEqual(
                set(lock["platforms"]["linux/amd64"]["images"]),
                set(REQUIRED_IMAGE_KEYS),
            )
            self.assertEqual(
                lock["platforms"]["linux/amd64"]["images"]
                ["social-network-microservices"],
                "ghcr.io/example/app@sha256:" + "1" * 64,
            )
            self.assertEqual(
                lock["platforms"]["linux/arm64"]["images"]["mongodb"],
                "docker.io/library/mongo@sha256:" + "2" * 64,
            )
            for platform in image_lock.REQUIRED_PLATFORMS:
                for reference in lock["platforms"][platform]["images"].values():
                    repository, digest = reference.split("@", 1)
                    self.assertNotIn(":", repository.rsplit("/", 1)[-1])
                    self.assertRegex(digest, r"^sha256:[0-9a-f]{64}$")
            self.assertEqual(len(inspected), 7)
            # This is the authoritative cross-module compatibility check.
            validated = validate_image_lock(lock)
            self.assertEqual(
                validated.image("x86_64", "social-network-microservices"),
                "ghcr.io/example/app@sha256:" + "1" * 64,
            )

    def test_preflight_resolves_support_images_before_custom_builds(self):
        with tempfile.TemporaryDirectory() as temporary:
            temporary_path = Path(temporary)
            manifest_path = temporary_path / "context.json"
            preflight_path = temporary_path / "support.json"
            context_manifest(manifest_path)
            preflight = image_lock.preflight_third_party(
                manifest_path, inspector=lambda unused: raw_index()
            )
            preflight_path.write_text(json.dumps(preflight), encoding="utf-8")

            inspected = []
            custom = {
                "app": "ghcr.io/example/app:run@sha256:" + "a" * 64,
                "frontend": "ghcr.io/example/frontend:run@sha256:" + "b" * 64,
                "media_frontend": (
                    "ghcr.io/example/media-frontend:run@sha256:" + "c" * 64
                ),
            }
            lock = image_lock.build_lock(
                manifest_path,
                custom,
                inspector=lambda reference: inspected.append(reference) or raw_index(),
                third_party_preflight=preflight_path,
            )

            self.assertEqual(len(inspected), 3)
            self.assertTrue(preflight["runtime_schema_preflight"])
            validate_image_lock(lock)

    def test_lock_rejects_a_missing_required_platform(self):
        with self.assertRaisesRegex(image_lock.ImageLockError, "linux/arm64"):
            image_lock.platform_digests(
                raw_index(include_arm64=False), reference="example.invalid/image:tag"
            )

    def test_custom_image_requires_its_pushed_index_digest(self):
        with tempfile.TemporaryDirectory() as temporary:
            manifest_path = Path(temporary) / "context.json"
            context_manifest(manifest_path)
            custom = {
                "app": "ghcr.io/example/app:mutable",
                "frontend": "ghcr.io/example/frontend:run@sha256:" + "b" * 64,
                "media_frontend": (
                    "ghcr.io/example/media-frontend:run@sha256:" + "c" * 64
                ),
            }
            with self.assertRaisesRegex(image_lock.ImageLockError, "index digest"):
                image_lock.build_lock(
                    manifest_path, custom, inspector=lambda unused: raw_index()
                )


class DeathStarBenchImageWorkflowTests(unittest.TestCase):
    def test_workflow_is_manual_least_privilege_and_commit_pinned(self):
        workflow = (
            ROOT / ".github/workflows/deathstarbench-social-images.yml"
        ).read_text(encoding="utf-8")
        trigger = workflow.split("permissions:", 1)[0]
        self.assertIn("workflow_dispatch:", trigger)
        self.assertNotIn("pull_request:", trigger)
        self.assertNotIn("  push:", trigger)
        self.assertIn("  contents: read\n  packages: write\n", workflow)
        self.assertNotIn("id-token:", workflow)
        self.assertNotRegex(workflow, r"uses:\s+[^\s]+@v[0-9]")
        action_references = [
            line.split("@", 1)[1].split()[0]
            for line in workflow.splitlines()
            if "uses:" in line
        ]
        self.assertGreaterEqual(len(action_references), 8)
        for reference in action_references:
            self.assertRegex(reference, r"^[0-9a-f]{40}$")
        self.assertIn("platforms: linux/amd64,linux/arm64", workflow)
        self.assertLess(
            workflow.index("Preflight support images and deployment lock schema"),
            workflow.index("Build and publish application candidate"),
        )
        self.assertIn("--third-party-preflight", workflow)
        self.assertEqual(workflow.count("OCI_SOURCE_REPOSITORY="), 3)
        self.assertEqual(workflow.count("OCI_SOURCE_REVISION="), 3)
        self.assertIn(prepare.UPSTREAM_REVISION, workflow)
        self.assertIn("mongo:4.4.6", workflow + str(prepare.THIRD_PARTY_IMAGES))
        self.assertIn("redis:7.2.4", workflow + str(prepare.THIRD_PARTY_IMAGES))
        self.assertIn("memcached:1.6.26", workflow + str(prepare.THIRD_PARTY_IMAGES))
        self.assertIn(
            "jaegertracing/all-in-one:1.57.0",
            workflow + str(prepare.THIRD_PARTY_IMAGES),
        )
        self.assertIn("GHCR creates new packages as private by default", workflow)


if __name__ == "__main__":
    unittest.main()
