import asyncio
import inspect
import io
import re
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from pydantic import ValidationError

from app import deathstarbench
from app.models import BenchmarkPlan, DeathStarBenchOptions


EXPECTED_REVISION = "6ecb09706140f8730b5385c08f1386c654c3c526"
TARGET_PRIVATE_IP = "10.0.0.42"


def valid_plan(**overrides):
    values = {
        "region": "us-ashburn-1",
        "shape": "VM.Standard.E5.Flex",
        "memory_gb": 32,
        "ssh_private_key": "private-key",
        "ssh_public_key": "ssh-rsa public-key",
        "benchmarks": ["deathstarbench"],
    }
    values.update(overrides)
    return BenchmarkPlan(**values)


class DeathStarBenchOptionsTests(unittest.TestCase):
    def test_defaults(self):
        options = DeathStarBenchOptions()

        self.assertEqual(options.workload, "media_microservices")
        self.assertEqual(options.warmup_seconds, 30)
        self.assertEqual(options.duration_seconds, 60)
        self.assertEqual(options.threads, 4)
        self.assertEqual(options.connections, 64)
        self.assertEqual(options.request_rate, 100)

        plan = valid_plan()
        self.assertEqual(plan.deathstarbench, options)

    def test_numeric_bounds_reject_values_outside_supported_range(self):
        invalid_values = {
            "warmup_seconds": (-1, 601),
            "duration_seconds": (9, 1801),
            "threads": (0, 65),
            "connections": (0, 4097),
            "request_rate": (0, 100001),
        }

        for field, values in invalid_values.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValidationError):
                        DeathStarBenchOptions(**{field: value})

    def test_numeric_bounds_are_inclusive(self):
        minimum = DeathStarBenchOptions(
            warmup_seconds=0,
            duration_seconds=10,
            threads=1,
            connections=1,
            request_rate=1,
        )
        maximum = DeathStarBenchOptions(
            warmup_seconds=600,
            duration_seconds=1800,
            threads=64,
            connections=4096,
            request_rate=100000,
        )

        self.assertEqual(minimum.warmup_seconds, 0)
        self.assertEqual(minimum.duration_seconds, 10)
        self.assertEqual(maximum.threads, 64)
        self.assertEqual(maximum.request_rate, 100000)

    def test_workload_must_be_supported(self):
        with self.assertRaises(ValidationError):
            DeathStarBenchOptions(workload="unknown")

    def test_connections_must_cover_worker_threads(self):
        options = DeathStarBenchOptions(threads=8, connections=7, request_rate=8)

        with self.assertRaisesRegex(
            ValidationError,
            "connections must be greater than or equal",
        ):
            valid_plan(deathstarbench=options)

    def test_request_rate_must_cover_worker_threads(self):
        options = DeathStarBenchOptions(threads=8, connections=8, request_rate=7)

        with self.assertRaisesRegex(
            ValidationError,
            "request rate must be greater than or equal",
        ):
            valid_plan(deathstarbench=options)

    def test_per_thread_settings_must_not_be_truncated_by_wrk2(self):
        with self.assertRaisesRegex(ValidationError, "connections must be evenly"):
            valid_plan(
                deathstarbench=DeathStarBenchOptions(
                    threads=4,
                    connections=65,
                    request_rate=100,
                )
            )
        with self.assertRaisesRegex(ValidationError, "request rate must be evenly"):
            valid_plan(
                deathstarbench=DeathStarBenchOptions(
                    threads=4,
                    connections=64,
                    request_rate=101,
                )
            )

    def test_deathstarbench_requires_sixteen_gibibytes(self):
        with self.assertRaisesRegex(
            ValidationError,
            "requires at least 16 GB",
        ):
            valid_plan(memory_gb=15.99)

        self.assertEqual(valid_plan(memory_gb=16).memory_gb, 16)

    def test_cross_field_limits_apply_only_when_deathstarbench_selected(self):
        options = DeathStarBenchOptions(
            threads=8,
            connections=1,
            request_rate=1,
        )

        plan = valid_plan(
            benchmarks=["sysbench_cpu"],
            memory_gb=8,
            deathstarbench=options,
        )

        self.assertEqual(plan.memory_gb, 8)
        self.assertEqual(plan.deathstarbench.connections, 1)


class Wrk2ParserTests(unittest.TestCase):
    REALISTIC_OUTPUT = """\
Running 30s test @ http://10.0.0.42:8080/wrk2-api/review/compose
  4 threads and 64 connections
Sent 30010 requests

  Thread calibration: mean lat.: 1.109ms, rate sampling interval: 10ms
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.52ms  612.31us  12.42ms   88.30%
    Req/Sec   249.98      7.02   260.00     90.00%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%  850.00us
 75.000%    2.20ms
 90.000%    3.80ms
 99.000%    1.50s
 99.900%    1.80s
 99.990%    1.90s
 99.999%    2.00s
100.000%    2.10s

  Detailed Percentile spectrum:
       Value   Percentile   TotalCount 1/(1-Percentile)
       0.350     0.000000            1         1.00
       0.850     0.500000        14999         2.00
       4.750     0.950000        28498        20.00
    1500.000     0.990000        29698       100.00
    2100.000     1.000000        29997          inf
  29997 requests in 30.00s, 8.74MB read
  Socket errors: connect 1, read 2, write 3, timeout 4
  Non-2xx or 3xx responses: 5
Requests/sec:    999.90
Transfer/sec:    298.31KB
"""

    def test_parses_throughput_errors_and_latency_percentiles(self):
        result = deathstarbench.parse_wrk2_output(
            self.REALISTIC_OUTPUT,
            require_marker=False,
        )

        self.assertEqual(result["throughput_requests_per_second"], 999.9)
        self.assertEqual(result["total_requests"], 29997)
        self.assertEqual(result["errors"], 5)
        self.assertEqual(result["socket_errors"], 10)
        self.assertEqual(result["connect_errors"], 1)
        self.assertEqual(result["read_errors"], 2)
        self.assertEqual(result["write_errors"], 3)
        self.assertEqual(result["timeout_errors"], 4)
        self.assertEqual(result["error_rate_percent"], 0.0167)
        self.assertEqual(result["sent_requests"], 30010)
        self.assertEqual(result["uncompleted_requests"], 13)
        self.assertEqual(result["p50_ms"], 0.85)
        self.assertEqual(result["p95_ms"], 4.75)
        self.assertEqual(result["p99_ms"], 1500.0)

    def test_absent_error_lines_mean_zero_errors(self):
        output = self.REALISTIC_OUTPUT.replace(
            "  Socket errors: connect 1, read 2, write 3, timeout 4\n",
            "",
        ).replace("  Non-2xx or 3xx responses: 5\n", "")

        result = deathstarbench.parse_wrk2_output(output, require_marker=False)

        self.assertEqual(result["errors"], 0)
        self.assertEqual(result["error_rate_percent"], 0)

    def test_rejects_incomplete_wrk2_output(self):
        with self.assertRaisesRegex(ValueError, "throughput and request totals"):
            deathstarbench.parse_wrk2_output(
                "Requests/sec: 100.00",
                require_marker=False,
            )

        without_p95 = re.sub(
            r"^\s*4\.750\s+0\.950000.*$",
            "",
            self.REALISTIC_OUTPUT,
            flags=re.MULTILINE,
        )
        with self.assertRaisesRegex(ValueError, "p50, p95, and p99"):
            deathstarbench.parse_wrk2_output(
                without_p95,
                require_marker=False,
            )

    def test_rejects_a_normal_wrk_run_when_the_workload_lua_did_not_load(self):
        output = (
            "script error: module 'socket' not found\n"
            + self.REALISTIC_OUTPUT
        )

        with self.assertRaisesRegex(ValueError, "metrics marker"):
            deathstarbench.parse_wrk2_output(output)

    def test_prefers_machine_readable_metrics_emitted_by_wrk2_lua(self):
        output = (
            "wrk2 human-readable output\n"
            "Sent 60000 requests\n"
            "OCI_DSB_METRICS duration_seconds=60.000000 total_requests=59994 "
            "throughput_requests_per_second=999.900000 errors=6 "
            "error_rate_percent=0.010001 socket_errors=4 connect_errors=1 "
            "read_errors=1 write_errors=0 timeout_errors=2 p50_ms=0.850000 "
            "p95_ms=4.750000 p99_ms=1500.000000\n"
            "OCI_DSB_LOADGEN_LOGICAL_CPUS=4\n"
            "\tPercent of CPU this job got: 390%\n"
            "\tMaximum resident set size (kbytes): 24576\n"
        )

        result = deathstarbench.parse_wrk2_output(output)

        self.assertEqual(result["duration_seconds"], 60.0)
        self.assertEqual(result["total_requests"], 59994)
        self.assertEqual(result["throughput_requests_per_second"], 999.9)
        self.assertEqual(result["p95_ms"], 4.75)
        self.assertEqual(result["sent_requests"], 60000)
        self.assertEqual(result["uncompleted_requests"], 6)
        self.assertEqual(result["load_generator_cpu_percent"], 390.0)
        self.assertEqual(result["load_generator_logical_cpus"], 4)
        self.assertEqual(result["load_generator_capacity_used_percent"], 97.5)
        self.assertEqual(result["load_generator_peak_rss_kb"], 24576)
        self.assertIn("generator-limited", result["load_generator_saturation_warning"])

    def test_rejects_measurements_with_no_successful_requests(self):
        zero = (
            "OCI_DSB_METRICS duration_seconds=10 total_requests=0 "
            "throughput_requests_per_second=0 errors=0 error_rate_percent=0 "
            "socket_errors=0 connect_errors=0 read_errors=0 write_errors=0 "
            "timeout_errors=0 "
            "p50_ms=0 p95_ms=0 p99_ms=0"
        )
        all_failed = zero.replace(
            "total_requests=0 throughput_requests_per_second=0 errors=0",
            "total_requests=10 throughput_requests_per_second=1 errors=10",
        ) + "\nSent 10 requests\n"

        with self.assertRaisesRegex(ValueError, "without any requests"):
            deathstarbench.parse_wrk2_output(zero)
        with self.assertRaisesRegex(ValueError, "Every completed"):
            deathstarbench.parse_wrk2_output(all_failed)


class GeneratedCommandTests(unittest.TestCase):
    def command_cases(self):
        cases = [
            ("runtime verification", deathstarbench.podman_runtime_verification_command()),
            ("pinned clone", deathstarbench.pinned_clone_command()),
            ("pinned recursive clone", deathstarbench.pinned_clone_command(recursive=True)),
            ("load generator preparation", deathstarbench.load_generator_prepare_command()),
        ]
        for workload_id in deathstarbench.WORKLOADS:
            options = DeathStarBenchOptions(workload=workload_id)
            cases.extend(
                [
                    (
                        f"{workload_id} image prefetch",
                        deathstarbench.prefetch_workload_images_command(
                            workload_id
                        ),
                    ),
                    (
                        f"{workload_id} prepare",
                        deathstarbench.prepare_workload_command(workload_id),
                    ),
                    (
                        f"{workload_id} build",
                        deathstarbench.build_workload_command(workload_id),
                    ),
                    (
                        f"{workload_id} deploy",
                        deathstarbench.deploy_workload_command(workload_id),
                    ),
                    (
                        f"{workload_id} readiness",
                        deathstarbench.frontend_readiness_command(
                            workload_id,
                            TARGET_PRIVATE_IP,
                        ),
                    ),
                    (
                        f"{workload_id} firewall",
                        deathstarbench.frontend_firewall_command(workload_id),
                    ),
                    (
                        f"{workload_id} initialization",
                        deathstarbench.initialize_workload_command(
                            workload_id,
                            TARGET_PRIVATE_IP,
                        ),
                    ),
                    (
                        f"{workload_id} load",
                        deathstarbench.load_command(
                            workload_id,
                            TARGET_PRIVATE_IP,
                            options,
                            options.duration_seconds,
                        ),
                    ),
                ]
            )
        return cases

    def test_all_generated_commands_are_valid_bash(self):
        for name, command in self.command_cases():
            with self.subTest(command=name):
                result = subprocess.run(
                    ["bash", "-n"],
                    input=command,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_generated_commands_have_no_unresolved_python_placeholders(self):
        unresolved = re.compile(
            r"(?<![$%{])\{[A-Za-z_][A-Za-z0-9_]*"
            r"(?:\[[^\]\r\n]+\])?\}"
        )

        for name, command in self.command_cases():
            with self.subTest(command=name):
                self.assertIsNone(unresolved.search(command), command)

    def test_media_nginx_tuning_uses_the_rendered_config_path(self):
        command = deathstarbench.deploy_workload_command('media_microservices')
        config = (
            f'{deathstarbench.REMOTE_ROOT}/mediaMicroservices/'
            'nginx-web-server/conf/nginx.conf'
        )

        self.assertNotIn('{dns_files[0]}', command)
        self.assertIn(f"worker_processes auto;/' {config}", command)
        self.assertIn(f"grep -F 'worker_processes auto;' {config}", command)

    def test_registry_images_are_prefetched_with_bounded_retries(self):
        expected = {
            'media_microservices': {
                'docker.io/library/ubuntu:16.04',
                'docker.io/library/ubuntu:xenial',
                'docker.io/jaegertracing/all-in-one:1.57.0',
                'docker.io/library/memcached:1.6.26',
                'docker.io/library/mongo:4.4.6',
                'docker.io/library/redis:7.2.4',
            },
            'social_network': {
                'docker.io/library/ubuntu:16.04',
                'docker.io/library/ubuntu:xenial',
                'docker.io/jaegertracing/all-in-one:1.57.0',
                'docker.io/library/memcached:1.6.26',
                'docker.io/library/mongo:4.4.6',
                'docker.io/library/redis:7.2.4',
            },
            'hotel_reservation': {
                'docker.io/library/golang:1.21',
                'docker.io/hashicorp/consul:1.19.0',
                'docker.io/jaegertracing/all-in-one:1.57.0',
                'docker.io/library/memcached:1.6.26',
                'docker.io/library/mongo:5.0',
            },
        }

        self.assertEqual(
            {
                workload_id: set(images)
                for workload_id, images
                in deathstarbench.WORKLOAD_REGISTRY_IMAGES.items()
            },
            expected,
        )
        for workload_id, images in expected.items():
            with self.subTest(workload=workload_id):
                command = deathstarbench.prefetch_workload_images_command(
                    workload_id
                )
                self.assertIn(
                    'podman pull --retry=10 --retry-delay=10s',
                    command,
                )
                self.assertIn('sudo podman image exists "$IMAGE"', command)
                self.assertIn('ip route get 169.254.169.254', command)
                for image in images:
                    self.assertIn(image, command)

    def test_custom_builds_use_local_bases_and_retry_cached_steps(self):
        expected_builds = {
            'media_microservices': 3,
            'social_network': 4,
            'hotel_reservation': 1,
        }
        for workload_id, count in expected_builds.items():
            with self.subTest(workload=workload_id):
                command = deathstarbench.build_workload_command(workload_id)
                self.assertEqual(command.count('sudo podman build'), count)
                self.assertEqual(command.count('--pull=never'), count)
                self.assertEqual(
                    command.count('BUILD_COMPLETE=false'),
                    count,
                )
                self.assertEqual(
                    command.count('failed after three attempts'),
                    count,
                )
                self.assertNotIn('--pull=always', command)

    def test_cached_build_retry_succeeds_then_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            counter = Path(directory) / 'attempts'
            transient = (
                f'COUNT=$(cat {counter} 2>/dev/null || echo 0); '
                'COUNT=$((COUNT + 1)); '
                f'echo "$COUNT" > {counter}; '
                '[ "$COUNT" -ge 2 ]'
            )
            succeeds = deathstarbench._retry_podman_build_command(
                transient,
                'test-image',
                retry_delay_seconds=0,
            )
            result = subprocess.run(
                ['bash', '-c', f'set -euo pipefail; {succeeds}'],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(counter.read_text().strip(), '2')

            exhausted = deathstarbench._retry_podman_build_command(
                'false',
                'test-image',
                retry_delay_seconds=0,
            )
            result = subprocess.run(
                ['bash', '-c', f'set -euo pipefail; {exhausted}'],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('failed after three attempts', result.stderr)

    def test_commands_never_invoke_docker_or_docker_compose(self):
        direct_docker_invocation = re.compile(
            r"(?:^|[;&|()]|\bthen\b|\bdo\b)\s*"
            r"(?:sudo\s+)?(?:env\s+)?(?:/[^\s;&|()]+/)?"
            r"docker(?:-compose)?(?=\s|[;&|()]|$)"
        )

        for name, command in self.command_cases():
            with self.subTest(command=name):
                self.assertIsNone(direct_docker_invocation.search(command))

    def test_source_never_installs_prohibited_container_packages(self):
        source = inspect.getsource(deathstarbench)
        prohibited_install = re.compile(
            r"(?:dnf|yum|apt(?:-get)?)\s+[^;\n]*install[^;\n]*"
            r"(?:docker(?:-ce|-compose)?|moby-engine|podman-docker)",
            re.IGNORECASE,
        )

        self.assertIsNone(prohibited_install.search(source))

    def test_runtime_allows_only_verified_podman_docker_wrappers(self):
        runtime = deathstarbench.podman_runtime_verification_command()

        self.assertNotIn(
            "A Docker-compatible executable is present; refusing to run.",
            runtime,
        )
        self.assertIn('DOCKER_OWNER=$(rpm -qf', runtime)
        self.assertIn('DOCKER_OWNER" = "podman-docker', runtime)
        self.assertIn("rpm -V podman-docker", runtime)
        self.assertIn('DOCKER_TARGET" = "$PODMAN_TARGET', runtime)
        self.assertIn("compatibility wrapper; it will not be invoked", runtime)
        self.assertIn("command -v dockerd", runtime)
        self.assertIn("for unit in docker.service docker.socket", runtime)
        self.assertIn("docker-engine docker-ee moby-engine; do", runtime)
        self.assertNotIn("moby-engine podman-docker; do", runtime)

    def test_runtime_allows_only_a_podman_compose_compatibility_symlink(self):
        runtime = deathstarbench.podman_runtime_verification_command()

        self.assertIn(
            'DOCKER_COMPOSE_TARGET" = "$PODMAN_COMPOSE_TARGET',
            runtime,
        )
        self.assertIn(
            "docker-compose-to-podman-compose compatibility symlink; it will "
            "not be invoked",
            runtime,
        )
        self.assertIn("docker-compose-plugin", runtime)

    def test_social_initialization_loads_the_complete_reference_graph(self):
        command = deathstarbench.initialize_workload_command(
            "social_network",
            TARGET_PRIVATE_IP,
        )

        self.assertIn("--graph=socfb-Reed98", command)
        self.assertIn("--compose", command)
        self.assertNotIn("--limit=", command)

    def test_media_load_does_not_duplicate_the_lua_request_path(self):
        options = DeathStarBenchOptions(workload="media_microservices")
        command = deathstarbench.load_command(
            "media_microservices",
            TARGET_PRIVATE_IP,
            options,
            options.duration_seconds,
        )

        self.assertIn(f"-R 100 http://{TARGET_PRIVATE_IP}:8080", command)
        self.assertNotIn("/wrk2-api/review/compose -R", command)

    def test_load_generator_uses_installed_luajit_and_private_luarocks_tree(self):
        prepare = deathstarbench.load_generator_prepare_command()
        load = deathstarbench.load_command(
            "social_network",
            TARGET_PRIVATE_IP,
            DeathStarBenchOptions(workload="social_network"),
            60,
        )

        self.assertIn("deps/luajit install", prepare)
        self.assertIn("-name 'luajit-*'", prepare)
        self.assertIn("ln -sfn", prepare)
        self.assertIn("luasocket 3.1.0-1", prepare)
        self.assertIn("require(\"socket\")", prepare)
        for command in (prepare, load):
            self.assertIn("path --lr-path", command)
            self.assertIn("path --lr-cpath", command)
            self.assertIn(f"--lua-dir={deathstarbench.LUAJIT_PREFIX}", command)
            self.assertIn(f"--tree={deathstarbench.LUAROCKS_TREE}", command)
            self.assertNotIn(
                f"{deathstarbench.LUAROCKS_TREE}/lib/lua/5.1/?.so",
                command,
            )
        self.assertIn(" -r -t ", load)
        self.assertIn("ulimit -Sn", load)

    def test_wrk_usage_probe_accepts_the_pinned_nonzero_usage_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory) / "wrk"
            executable.write_text(
                "#!/bin/bash\n"
                "echo 'Usage: wrk <options> <url>' >&2\n"
                "echo '  -R, --rate <T> work rate' >&2\n"
                "exit 1\n"
            )
            executable.chmod(0o755)

            command = deathstarbench._wrk_binary_verification_command(executable)
            result = subprocess.run(
                ["bash", "-c", f"set -euo pipefail; {command}"],
                text=True,
                capture_output=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("wrk2 binary is ready", result.stdout)

            executable.write_text(
                "#!/bin/bash\n"
                "echo 'unexpected output' >&2\n"
                "exit 1\n"
            )
            result = subprocess.run(
                ["bash", "-c", f"set -euo pipefail; {command}"],
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("expected wrk2 usage banner", result.stderr)

            executable.write_text(
                "#!/bin/bash\n"
                "echo 'Usage: wrk <options> <url>' >&2\n"
                "echo '  -R, --rate <T> work rate' >&2\n"
                "exit 0\n"
            )
            result = subprocess.run(
                ["bash", "-c", f"set -euo pipefail; {command}"],
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unexpectedly accepted", result.stderr)

    def test_initializers_fail_on_http_and_reported_dataset_errors(self):
        media = deathstarbench.initialize_workload_command(
            "media_microservices",
            TARGET_PRIVATE_IP,
        )
        social = deathstarbench.initialize_workload_command(
            "social_network",
            TARGET_PRIVATE_IP,
        )

        self.assertIn("base64 -d | python3 -", media)
        self.assertIn("--fail-with-body", media)
        self.assertIn("base64 -d | python3 -", social)
        self.assertIn("grep -q '^Failed:'", social)

    def test_aiohttp_response_patch_is_strict_and_checks_every_handler(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "initializer.py"
            path.write_text(
                "async def one(resp):\n"
                "    return await resp.text()\n"
                "async def two(resp):\n"
                "    return await resp.text()\n"
            )
            command = deathstarbench._strict_aiohttp_patch_command(path, 2)
            result = subprocess.run(
                ["bash", "-c", command],
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(path.read_text().count("body = await resp.text()"), 2)
            self.assertEqual(path.read_text().count("if resp.status >= 400"), 2)
            self.assertEqual(path.read_text().count("body[:500]"), 2)
            compile(path.read_text(), str(path), "exec")

            repeat = deathstarbench._strict_aiohttp_patch_command(path, 2)
            result = subprocess.run(
                ["bash", "-c", repeat],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(path.read_text().count("body = await resp.text()"), 2)

            incomplete = Path(directory) / "incomplete.py"
            incomplete.write_text(
                "async def one(resp):\n"
                "    return await resp.text()\n"
            )
            mismatch = deathstarbench._strict_aiohttp_patch_command(incomplete, 2)
            result = subprocess.run(
                ["bash", "-c", mismatch],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)

    def test_media_null_thumbnail_patch_is_strict_and_idempotent(self):
        old = 'movie["thumbnail_ids"] = [raw_movie["poster_path"]]'
        new = (
            'movie["thumbnail_ids"] = ([raw_movie["poster_path"]] '
            'if raw_movie["poster_path"] else [])'
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "initializer.py"
            path.write_text(old + "\n")
            command = deathstarbench._strict_text_patch_command(path, old, new)

            for _ in range(2):
                result = subprocess.run(
                    ["bash", "-c", command],
                    text=True,
                    capture_output=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(path.read_text().count(new), 1)

            path.write_text("unrelated\n")
            result = subprocess.run(
                ["bash", "-c", command],
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(result.returncode, 0)

    def test_media_title_dedup_patch_is_strict_idempotent_and_keeps_first_id(self):
        initializer = '''async def upload_movie_info(session, addr, movie):
  events.append(("movie-info", movie["movie_id"], movie["title"]))

async def upload_plot(session, addr, plot):
  events.append(("plot", str(plot["plot_id"])))

async def register_movie(session, addr, movie):
  events.append(("register", movie["movie_id"], movie["title"]))

async def write_movie_info(addr, raw_movies):
  idx = 0
  tasks = []
  conn = aiohttp.TCPConnector(limit=200)
  async with aiohttp.ClientSession(connector=conn) as session:
    for raw_movie in raw_movies:
      movie = dict()
      movie["movie_id"] = str(raw_movie["id"])
      movie["title"] = raw_movie["title"]
      task = asyncio.ensure_future(upload_movie_info(session, addr, movie))
      tasks.append(task)
      plot = dict()
      plot["plot_id"] = raw_movie["id"]
      task = asyncio.ensure_future(upload_plot(session, addr, plot))
      tasks.append(task)
      task = asyncio.ensure_future(register_movie(session, addr, movie))
      tasks.append(task)
      idx += 1
      if idx % 200 == 0:
        resps = await asyncio.gather(*tasks)
        print(idx, "movies finished")
    resps = await asyncio.gather(*tasks)
    print(idx, "movies finished")
'''

        class FakeClientSession:
            def __init__(self, connector):
                self.connector = connector

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return False

        class FakeAiohttp:
            TCPConnector = lambda limit: limit
            ClientSession = FakeClientSession

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "initializer.py"
            path.write_text(initializer)
            commands = list(
                deathstarbench._media_title_dedup_patch_commands(path)
            )
            self.assertTrue(commands)

            for _ in range(2):
                for command in commands:
                    result = subprocess.run(
                        ["bash", "-c", command],
                        text=True,
                        capture_output=True,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)

            patched = path.read_text()
            compile(patched, str(path), "exec")
            self.assertEqual(patched.count("registered_titles = set()"), 1)

            events = []
            namespace = {
                "aiohttp": FakeAiohttp,
                "asyncio": asyncio,
                "events": events,
            }
            exec(compile(patched, str(path), "exec"), namespace)
            output = io.StringIO()
            with redirect_stdout(output):
                asyncio.run(
                    namespace["write_movie_info"](
                        "http://10.0.0.42:8080",
                        [
                            {"id": 11549, "title": "Duplicate title"},
                            {"id": 7, "title": "Unique title"},
                            {"id": 11850, "title": "Duplicate title"},
                        ],
                    )
                )
            self.assertIn(
                "Skipping duplicate movie title registration: "
                "Duplicate title (first mapping retained)",
                output.getvalue(),
            )

            self.assertEqual(
                [event for event in events if event[0] == "movie-info"],
                [
                    ("movie-info", "11549", "Duplicate title"),
                    ("movie-info", "7", "Unique title"),
                    ("movie-info", "11850", "Duplicate title"),
                ],
            )
            self.assertEqual(
                [event for event in events if event[0] == "plot"],
                [("plot", "11549"), ("plot", "7"), ("plot", "11850")],
            )
            self.assertEqual(
                [event for event in events if event[0] == "register"],
                [
                    ("register", "11549", "Duplicate title"),
                    ("register", "7", "Unique title"),
                ],
            )

            drifted = Path(directory) / "drifted.py"
            drifted.write_text("async def unrelated():\n  pass\n")
            for command in deathstarbench._media_title_dedup_patch_commands(
                drifted
            ):
                result = subprocess.run(
                    ["bash", "-c", command],
                    text=True,
                    capture_output=True,
                )
                self.assertNotEqual(result.returncode, 0)

    def test_container_operations_use_rootful_podman(self):
        runtime = deathstarbench.podman_runtime_verification_command()
        self.assertIn("sudo podman info", runtime)
        self.assertIn("rootless={{.Host.Security.Rootless}}", runtime)

        for workload_id in deathstarbench.WORKLOADS:
            with self.subTest(workload=workload_id):
                build = deathstarbench.build_workload_command(workload_id)
                deploy = deathstarbench.deploy_workload_command(workload_id)

                self.assertIn("sudo podman build", build)
                self.assertNotRegex(build, r"(?:^|;\s*)podman\s+build")
                self.assertIn("sudo /usr/bin/podman-compose", deploy)
                self.assertIn("sudo podman ps", deploy)
                self.assertIn('COMPOSE_STARTED', deploy)
                self.assertIn('failed after three attempts', deploy)
                self.assertNotRegex(deploy, r"(?:^|;\s*)podman-compose\s+")
                self.assertNotIn("podman compose", deploy)


class RevisionPinningTests(unittest.TestCase):
    def test_revision_is_a_specific_full_commit(self):
        self.assertEqual(deathstarbench.REVISION, EXPECTED_REVISION)
        self.assertRegex(deathstarbench.REVISION, r"^[0-9a-f]{40}$")
        self.assertEqual(
            deathstarbench.REVISION_TAG,
            EXPECTED_REVISION[:12],
        )

    def test_checkout_fetches_and_verifies_the_pinned_revision(self):
        for recursive in (False, True):
            with self.subTest(recursive=recursive):
                command = deathstarbench.pinned_clone_command(recursive=recursive)
                self.assertIn(
                    f"fetch --depth 1 origin {EXPECTED_REVISION}",
                    command,
                )
                self.assertIn(
                    f'test "$(git -C {deathstarbench.REMOTE_ROOT} '
                    f'rev-parse HEAD 2>/dev/null)" = "{EXPECTED_REVISION}"',
                    command,
                )
                self.assertNotRegex(command, r"\b(?:main|master|latest)\b")

    def test_report_metadata_records_revision_and_runtime(self):
        options = DeathStarBenchOptions()
        result = deathstarbench.metadata(
            options.workload,
            options,
            service_architecture="aarch64",
            loadgen_architecture="x86_64",
        )

        self.assertEqual(result["upstream_revision"], EXPECTED_REVISION)
        self.assertEqual(result["container_runtime"], "Podman with podman-compose")


if __name__ == "__main__":
    unittest.main()
