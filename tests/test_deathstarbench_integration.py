import tempfile
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from app import main
from app import deathstarbench
from app.models import BenchmarkPlan


def plan(**overrides):
    values = {
        'region': 'us-ashburn-1',
        'shape': 'VM.Standard.A1.Flex',
        'ocpus': 8,
        'memory_gb': 32,
        'ssh_private_key': 'private',
        'ssh_public_key': 'public',
        'benchmarks': ['deathstarbench'],
        'storage': {'additional_volume': False},
    }
    values.update(overrides)
    return BenchmarkPlan(**values)


class CatalogAndNetworkTests(unittest.TestCase):
    def test_catalog_exposes_all_released_workloads(self):
        payload = main.catalog()

        self.assertEqual(
            {item['id'] for item in payload['deathstarbench_workloads']},
            {'media_microservices', 'hotel_reservation', 'social_network'},
        )

    def test_frontend_ports_are_private_subnet_only_and_optional(self):
        normal = main.benchmark_security_rules(False)
        deathstar = main.benchmark_security_rules(True)

        normal_tcp_ports = {
            rule.tcp_options.destination_port_range.min
            for rule in normal
            if rule.tcp_options
        }
        deathstar_rules = [
            rule
            for rule in deathstar
            if rule.tcp_options
            and rule.tcp_options.destination_port_range.min in {5000, 8080}
        ]
        self.assertNotIn(5000, normal_tcp_ports)
        self.assertNotIn(8080, normal_tcp_ports)
        self.assertEqual(len(deathstar_rules), 2)
        self.assertTrue(
            all(rule.source == '10.42.1.0/24' for rule in deathstar_rules)
        )

    def test_load_generator_shape_is_fixed_x86_and_priority_ordered(self):
        shapes = [
            SimpleNamespace(shape='VM.Standard.E4.Flex'),
            SimpleNamespace(shape='VM.Standard.A1.Flex'),
            SimpleNamespace(shape='VM.Standard.E5.Flex'),
        ]

        self.assertEqual(
            main.load_generator_shape(shapes).shape,
            'VM.Standard.E5.Flex',
        )
        with self.assertRaisesRegex(RuntimeError, 'x86 flexible shape'):
            main.load_generator_shape(
                [SimpleNamespace(shape='VM.Standard.A1.Flex')]
            )

    def test_media_compose_conversion_removes_socket_and_patches_podman_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workload = root / 'mediaMicroservices'
            files = {
                'docker/thrift-microservice-deps/cpp/Dockerfile':
                    'FROM ubuntu:16.04\n',
                'Dockerfile': 'FROM yg397/thrift-microservice-deps:xenial\n',
                'docker/openresty-thrift/xenial/Dockerfile': (
                    'ARG RESTY_IMAGE_BASE="ubuntu"\n'
                    'ARG RESTY_IMAGE_TAG="xenial"\n'
                    'FROM ${RESTY_IMAGE_BASE}:${RESTY_IMAGE_TAG}\n'
                    'RUN curl http://ftp.cs.stanford.edu/pub/exim/pcre/'
                    'pcre-${RESTY_PCRE_VERSION}.tar.gz\n'
                    'RUN luarocks install long \\\n'
                    '    && luarocks install lua-resty-jwt \\\n'
                    '    && ldconfig\n'
                ),
                'nginx-web-server/conf/nginx.conf': (
                    'http {\n'
                    "  lua_package_path "
                    "'/usr/local/openresty/nginx/lua-scripts/?.lua;;';\n"
                    '  server { lua_need_request_body on; }\n'
                    '}\n'
                ),
                'nginx-web-server/lua-scripts/wrk2-api/movie-info/write.lua': (
                    'new_cast["charactor"]=cast["charactor"]\n'
                ),
                'docker-compose.yml': yaml.safe_dump({
                    'version': '3',
                    'services': {
                        'dns-media': {
                            'image': 'defreitas/dns-proxy-server',
                            'volumes': ['/var/run/docker.sock:/var/run/docker.sock'],
                        },
                        'app': {
                            'image': 'yg397/media-microservices',
                            'ports': ['9090:9090'],
                        },
                        'nginx-web-server': {
                            'image': 'yg397/openresty-thrift:xenial',
                            'ports': ['8080:8080'],
                            'volumes': ['./conf:/conf'],
                        },
                        'cache': {'image': 'redis'},
                    },
                }),
            }
            for relative, contents in files.items():
                path = workload / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(contents)

            script = deathstarbench._compose_preparation_script().replace(
                "root = Path('/tmp/deathstarbench')",
                f'root = Path({str(root)!r})',
            )
            with patch.object(
                sys,
                'argv',
                ['prepare', 'media_microservices', deathstarbench.REVISION_TAG],
            ):
                exec(compile(script, '<compose-preparation>', 'exec'), {})

            generated = yaml.safe_load(
                (workload / 'compose-oci.yml').read_text()
            )
            self.assertNotIn('dns-media', generated['services'])
            self.assertNotIn('ports', generated['services']['app'])
            self.assertEqual(
                generated['services']['nginx-web-server']['ports'],
                ['8080:8080'],
            )
            self.assertEqual(
                generated['services']['cache']['image'],
                'docker.io/library/redis:7.2.4',
            )
            self.assertTrue(
                generated['services']['nginx-web-server']['volumes'][0]
                .endswith(':z')
            )
            self.assertNotIn(
                ':z:z',
                generated['services']['nginx-web-server']['volumes'][0],
            )
            self.assertEqual(
                generated['networks']['default']['name'],
                'oci_dsb_media_network',
            )
            self.assertTrue(
                all(
                    service['pull_policy'] == 'never'
                    for service in generated['services'].values()
                )
            )
            openresty = (
                workload
                / 'docker/openresty-thrift/xenial/Containerfile.oci'
            ).read_text()
            self.assertNotIn('old-releases.ubuntu.com', openresty)
            self.assertIn(
                'ARG RESTY_IMAGE_BASE="docker.io/library/ubuntu"',
                openresty,
            )
            self.assertIn('sourceforge.net/projects/pcre', openresty)
            self.assertNotIn('ftp.cs.stanford.edu', openresty)
            self.assertNotIn('luarocks install long', openresty)
            self.assertNotIn('luarocks install lua-resty-jwt', openresty)
            self.assertIn(deathstarbench.OPENRESTY_JWT_URL, openresty)
            self.assertIn(deathstarbench.OPENRESTY_JWT_SHA256, openresty)
            self.assertIn('luarocks install --deps-mode=none', openresty)
            self.assertIn('luajit -e', openresty)
            self.assertIn('require "liblualongnumber"', openresty)
            self.assertIn('require "resty.jwt"', openresty)
            nginx_config = (
                workload / 'nginx-web-server/conf/nginx.conf'
            ).read_text()
            self.assertEqual(
                nginx_config.count('client_body_buffer_size 64k;'),
                1,
            )
            movie_info_handler = (
                workload / 'nginx-web-server/lua-scripts/wrk2-api/'
                'movie-info/write.lua'
            ).read_text()
            self.assertNotIn('charactor', movie_info_handler)
            self.assertIn(
                'new_cast["character"]=cast["character"]',
                movie_info_handler,
            )

    def test_both_openresty_workloads_apply_the_pinned_lua_rock(self):
        script = deathstarbench._compose_preparation_script()

        self.assertEqual(
            script.count(
                '{OPENRESTY_ROCKS_ORIGINAL: OPENRESTY_ROCKS_PINNED}'
            ),
            2,
        )
        self.assertNotIn('__OPENRESTY_JWT_', script)

    def test_explicit_containerfile_replacements_fail_on_upstream_drift(self):
        script = deathstarbench._compose_preparation_script()
        definitions = script.split("if workload == 'media_microservices':", 1)[0]
        namespace = {}
        exec(compile(definitions, '<compose-definitions>', 'exec'), namespace)

        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'Dockerfile'
            destination = Path(directory) / 'Containerfile.oci'
            replacements = {
                namespace['OPENRESTY_ROCKS_ORIGINAL']:
                    namespace['OPENRESTY_ROCKS_PINNED'],
            }

            source.write_text('FROM ubuntu:xenial\n')
            with self.assertRaisesRegex(RuntimeError, 'found 0'):
                namespace['write_containerfile'](
                    source,
                    destination,
                    replacements,
                )

            original = namespace['OPENRESTY_ROCKS_ORIGINAL']
            source.write_text(f'{original}\n{original}\n')
            with self.assertRaisesRegex(RuntimeError, 'found 2'):
                namespace['write_containerfile'](
                    source,
                    destination,
                    replacements,
                )

            patch_target = Path(directory) / 'nginx.conf'
            patch_target.write_text('needle\n')
            namespace['replace_exact_in_file'](
                patch_target,
                'needle',
                'replacement',
            )
            self.assertEqual(patch_target.read_text(), 'replacement\n')
            with self.assertRaisesRegex(RuntimeError, 'found 0'):
                namespace['replace_exact_in_file'](
                    patch_target,
                    'needle',
                    'replacement',
                )


class LifecycleAndReportTests(unittest.TestCase):
    METRICS = (
        'wrk2 raw output\n'
        'Sent 6005 requests\n'
        'OCI_DSB_METRICS duration_seconds=60.000000 total_requests=6000 '
        'throughput_requests_per_second=100.000000 errors=2 '
        'error_rate_percent=0.033333 socket_errors=1 connect_errors=0 '
        'read_errors=0 write_errors=0 timeout_errors=1 p50_ms=1.000000 '
        'p95_ms=2.000000 p99_ms=3.000000\n'
    )

    def test_orchestrator_uses_separate_x86_loadgen_and_structured_result(self):
        benchmark_plan = plan()
        job = {
            'id': 'unit-deathstar',
            'events': [],
            'results': [],
            'resources': {
                'private_ip': '10.42.1.10',
                'public_ip': '203.0.113.10',
                'loadgen_public_ip': '203.0.113.20',
                'loadgen_private_ip': '10.42.1.20',
                'loadgen_shape': 'VM.Standard.E5.Flex',
            },
        }
        calls = []

        def fake_ssh(
            _job,
            command,
            timeout=1800,
            host_key='public_ip',
            include_stderr=False,
        ):
            calls.append((host_key, command, timeout, include_stderr))
            if command == 'uname -m':
                return 'x86_64' if host_key == 'loadgen_public_ip' else 'aarch64'
            if '/wrk2/wrk -D exp' in command:
                return self.METRICS
            if 'podman info' in command:
                return (
                    'podman version 5\npodman-compose version 1.5\n'
                    'rootless=false networkBackend=netavark graphDriver=overlay'
                )
            return 'ok'

        with (
            patch.object(main, 'wait_for_guest_readiness'),
            patch.object(main, 'dnf_install'),
            patch.object(main, 'enable_ol9_developer_epel'),
            patch.object(main, 'ssh', side_effect=fake_ssh),
        ):
            main.run_deathstarbench(job, benchmark_plan)

        result = job['results'][0]
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['metrics']['p95_ms'], 2.0)
        self.assertEqual(result['output'], self.METRICS)
        self.assertEqual(result['metadata']['service_architecture'], 'aarch64')
        self.assertEqual(
            result['metadata']['load_generator_architecture'],
            'x86_64',
        )
        self.assertEqual(
            result['metadata']['traffic_path'],
            'OCI private VCN address',
        )
        measured_calls = [
            call
            for call in calls
            if '/wrk2/wrk -D exp' in call[1]
        ]
        self.assertTrue(measured_calls)
        self.assertTrue(
            all(call[0] == 'loadgen_public_ip' for call in measured_calls)
        )
        source_index = next(
            index for index, call in enumerate(calls)
            if '/tmp/prepare-deathstarbench.py' in call[1]
        )
        prefetch_index = next(
            index for index, call in enumerate(calls)
            if 'podman pull --retry=10' in call[1]
        )
        build_index = next(
            index for index, call in enumerate(calls)
            if 'podman build --pull=never' in call[1]
        )
        deploy_index = next(
            index for index, call in enumerate(calls)
            if 'podman network create' in call[1]
        )
        self.assertLess(source_index, prefetch_index)
        self.assertLess(prefetch_index, build_index)
        self.assertLess(build_index, deploy_index)
        self.assertIn('Setup and', main.report_result_section(result))

    def test_report_renders_metrics_and_escapes_raw_output(self):
        job = {
            'id': 'report-unit',
            'plan': {'shape': 'VM.Standard.A1.Flex'},
            'created_at': '2026-08-10T00:00:00+00:00',
            'updated_at': '2026-08-10T00:00:00+00:00',
            'events': [],
            'results': [{
                'id': 'deathstarbench',
                'name': 'DeathStarBench — Media Microservices',
                'command': 'wrk2 <target>',
                'started_at': '2026-08-10T00:00:00+00:00',
                'duration_seconds': 60,
                'status': 'completed',
                'metadata': {'container_runtime': 'Podman with podman-compose'},
                'metrics': {'p95_ms': 2.5, 'errors': 0},
                'output': '<script>alert(1)</script> raw',
                'setup_output': 'podman ok',
            }],
        }
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(main, 'RUNS', Path(directory)):
                main.make_report(job)
                report = (Path(directory) / job['id'] / 'report.html').read_text()

        self.assertIn('Measured results', report)
        self.assertIn('P95 Ms', report)
        self.assertIn('Podman with podman-compose', report)
        self.assertIn('&lt;script&gt;alert(1)&lt;/script&gt; raw', report)
        self.assertNotIn('<script>alert(1)</script>', report)

    def test_cleanup_includes_load_generator_before_network_resources(self):
        source = Path(main.__file__).read_text()
        instance_loop = source.index("'loadgen_instance_id'")
        subnet_cleanup = source.index("subnet_resources = [")

        self.assertLess(instance_loop, subnet_cleanup)

    def test_report_ui_only_shows_ssh_for_explicitly_retained_runs(self):
        source = (main.ROOT / 'static' / 'app.js').read_text()

        self.assertIn('const explicitlyRetained', source)
        self.assertIn("job.plan?.destroy_after_completion === false", source)
        self.assertIn(
            'const connections = explicitlyRetained ? retainedConnections(job) : [];',
            source,
        )

    def test_saved_state_keeps_sanitized_plan_for_retained_run_detection(self):
        job = {
            'id': 'saved-retained',
            'status': 'failed',
            'error': 'benchmark failed',
            'plan': {
                'shape': 'VM.Standard.A1.Flex',
                'destroy_after_completion': False,
            },
            'events': [],
            'resources': {
                'public_ip': '203.0.113.10',
                'loadgen_public_ip': '203.0.113.20',
            },
            'results': [{'status': 'failed'}],
        }
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(main, 'RUNS', Path(directory)):
                run_directory = Path(directory) / job['id']
                run_directory.mkdir()
                (run_directory / 'report.html').write_text('<html></html>')
                main.persist_job_state(job)
                saved = main.read_json_file(run_directory / 'state.json', {})

        self.assertFalse(saved['plan']['destroy_after_completion'])
        self.assertNotIn('ssh_private_key', saved['plan'])
        self.assertEqual(saved['resources']['loadgen_public_ip'], '203.0.113.20')

    def test_private_key_download_uses_only_the_live_submitted_job_key(self):
        source = (main.ROOT / 'static' / 'app.js').read_text()

        self.assertIn("let currentJobPrivateKey = '';", source)
        self.assertIn("currentJobPrivateKey = $('#key').value;", source)
        self.assertIn('new Blob([currentJobPrivateKey]', source)
        self.assertNotIn("new Blob([$('#key').value]", source)


class FailedRunCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_benchmark_uses_normal_destroying_destroyed_lifecycle(self):
        benchmark_plan = plan()
        job = {
            'id': 'failed-cleanup',
            'plan': benchmark_plan.model_dump(
                exclude={
                    'ssh_private_key',
                    'ssh_public_key',
                    'ssh_key_passphrase',
                }
            ),
            '_key': 'private',
            '_passphrase': None,
            '_public_key': 'public',
            'status': 'queued',
            'events': [],
            'resources': {'instance_id': 'instance'},
            'results': [],
        }

        def fail_benchmark(target_job, _plan):
            target_job['results'].append({
                'id': 'deathstarbench',
                'name': 'DeathStarBench',
                'status': 'failed',
                'error': 'test failure',
            })
            raise RuntimeError('test failure')

        status_before_destroy = []

        def destroy(target_job):
            status_before_destroy.append(target_job['status'])
            target_job['status'] = 'destroying'
            target_job['status'] = 'destroyed'

        with (
            patch.object(main, 'provision'),
            patch.object(main, 'run_benchmarks', side_effect=fail_benchmark),
            patch.object(main, 'make_report'),
            patch.object(main, 'destroy_with_status', side_effect=destroy) as cleanup,
            patch.object(main, 'persist_job_state'),
        ):
            await main.run_job(job, benchmark_plan)

        cleanup.assert_called_once_with(job)
        self.assertEqual(status_before_destroy, ['cleanup_pending'])
        self.assertEqual(job['status'], 'destroyed')
        self.assertEqual(main.benchmark_status(job), 'failed')
        self.assertEqual(job['error'], 'test failure')


if __name__ == '__main__':
    unittest.main()
