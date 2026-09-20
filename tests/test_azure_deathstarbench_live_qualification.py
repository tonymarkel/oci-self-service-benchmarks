from contextlib import nullcontext
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock
from urllib.error import HTTPError

from app.deathstarbench_contract import (
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DISTRIBUTED_IMAGE_SET_REVISION,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    DISTRIBUTED_WORKLOAD_REVISION,
    K3S_RUNTIME_ID,
    K3S_RUNTIME_JOURNAL_KEY,
)
from app.deathstarbench_k3s_workload import REQUIRED_IMAGE_KEYS, UPSTREAM_REVISION
from app.run_lease import inspect_run_lease
from scripts import qualify_azure_deathstarbench_distributed as qualification


def candidate_plan():
    return {
        'provider': 'azure',
        'deathstarbench': {
            'topology_id': DISTRIBUTED_TIERED_TOPOLOGY_ID,
            'runtime_id': K3S_RUNTIME_ID,
            'workload': 'social_network',
            'warmup_seconds': 30,
            'duration_seconds': 60,
            'threads': 4,
            'connections': 64,
            'request_rate': 100,
        },
    }


def qualified_result(*, duration_seconds=60, **metric_overrides):
    metrics = {
        'duration_seconds': float(duration_seconds),
        'total_requests': 6000,
        'sent_requests': 6000,
        'successful_requests': 6000,
        'errors': 0,
        'socket_errors': 0,
        'uncompleted_requests': 0,
        'completion_rate_percent': 100.0,
        'load_generator_cpu_percent': 100.0,
        'load_generator_logical_cpus': 4,
        'load_generator_capacity_used_percent': 25.0,
        'load_generator_peak_rss_kb': 24576,
    }
    metrics.update(metric_overrides)
    return {
        'id': 'deathstarbench',
        'name': 'DeathStarBench — Social Network',
        'status': 'completed',
        'output': (
            'Sent 6000 requests\n'
            'OCI_DSB_METRICS duration_seconds=60.000000 '
            'total_requests=6000 errors=0 socket_errors=0\n'
            'OCI_DSB_LOADGEN_LOGICAL_CPUS=4\n'
            '\tPercent of CPU this job got: 100%\n'
            '\tMaximum resident set size (kbytes): 24576\n'
        ),
        'metrics': metrics,
    }


def candidate_image_lock():
    platforms = {}
    manifest_bodies = {}
    custom = qualification.GHCR_IMAGE_KEYS
    for platform_index, platform in enumerate(('linux/amd64', 'linux/arm64')):
        images = {}
        for image_index, image_key in enumerate(REQUIRED_IMAGE_KEYS):
            body = f'{platform}:{image_key}:manifest'.encode()
            digest = f'sha256:{hashlib.sha256(body).hexdigest()}'
            repository = (
                f'ghcr.io/example/{image_key}'
                if image_key in custom
                else f'docker.io/example/{image_key}'
            )
            images[image_key] = f'{repository}@{digest}'
            if image_key in custom:
                manifest_bodies[digest] = body
        platforms[platform] = {'images': images}
    return ({
        'schema_version': 1,
        'workload_revision': DISTRIBUTED_WORKLOAD_REVISION,
        'image_set_revision': DISTRIBUTED_IMAGE_SET_REVISION,
        'upstream_revision': UPSTREAM_REVISION,
        'released': False,
        'platforms': platforms,
    }, manifest_bodies)


class FakeResponse:
    def __init__(self, url, body, *, headers=None):
        self.status = 200
        self.headers = headers or {}
        self._url = url
        self._body = body
        self.closed = False

    def getcode(self):
        return self.status

    def geturl(self):
        return self._url

    def read(self, _limit=-1):
        return self._body

    def close(self):
        self.closed = True


class AzureDeathStarBenchLiveQualificationHarnessTests(unittest.TestCase):
    @staticmethod
    def args():
        return SimpleNamespace(
            resume=None,
            image_lock=Path('/candidate/image-lock.json'),
            subscription_id='subscription-id',
            region='eastus2',
            zone='1',
            application_size='Standard_D8ps_v6',
            application_vcpus=8,
            application_memory_gib=32,
            measure=False,
            interrupt_at=None,
            interrupt_mode=None,
            warmup_seconds=30,
            duration_seconds=60,
            threads=4,
            connections=64,
            request_rate=100,
        )

    @staticmethod
    def job():
        return {
            'id': 'abc123def456',
            'plan': candidate_plan(),
            'status': 'queued',
            'events': [],
            'resources': {},
            'results': [],
        }

    def test_failure_always_attempts_cleanup_and_returns_nonzero(self):
        job = self.job()
        with (
            mock.patch.object(
                qualification,
                '_ssh_material',
                return_value={
                    'private_key': 'private',
                    'public_key': 'public',
                    'passphrase': None,
                },
            ),
            mock.patch.object(qualification, '_read_image_lock', return_value={}),
            mock.patch.object(qualification, '_preflight_anonymous_ghcr_images'),
            mock.patch.object(qualification, '_plan', return_value=object()),
            mock.patch.object(
                qualification,
                '_new_job',
                return_value=(job, nullcontext()),
            ),
            mock.patch.object(
                qualification,
                '_exclusive_job_lock',
                side_effect=lambda _job_id: nullcontext(),
            ),
            mock.patch.object(qualification, '_persist'),
            mock.patch.object(
                qualification.azure,
                'provision_distributed_deathstarbench_candidate',
                side_effect=RuntimeError('injected provisioning failure'),
            ),
            mock.patch.object(qualification, '_cleanup', return_value=True) as cleanup,
        ):
            result = qualification._qualify(self.args())

        self.assertEqual(result, 1)
        cleanup.assert_called_once_with(job)
        self.assertEqual(job['status'], 'cleanup_pending')
        self.assertIn('injected provisioning failure', job['error'])

    def test_success_requires_both_journals_then_cleans_up(self):
        job = self.job()

        def ready_runtime(current, **_kwargs):
            current['resources'][K3S_RUNTIME_JOURNAL_KEY] = {
                'state': 'cluster_ready'
            }

        def ready_workload(current, _lock, **_kwargs):
            current['resources'][DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY] = {
                'state': 'workload_ready'
            }

        with (
            mock.patch.object(
                qualification,
                '_ssh_material',
                return_value={
                    'private_key': 'private',
                    'public_key': 'public',
                    'passphrase': None,
                },
            ),
            mock.patch.object(qualification, '_read_image_lock', return_value={}),
            mock.patch.object(qualification, '_preflight_anonymous_ghcr_images'),
            mock.patch.object(qualification, '_plan', return_value=object()),
            mock.patch.object(
                qualification,
                '_new_job',
                return_value=(job, nullcontext()),
            ),
            mock.patch.object(
                qualification,
                '_exclusive_job_lock',
                side_effect=lambda _job_id: nullcontext(),
            ),
            mock.patch.object(qualification, '_persist'),
            mock.patch.object(
                qualification.azure,
                'provision_distributed_deathstarbench_candidate',
            ),
            mock.patch.object(
                qualification,
                'prepare_azure_distributed_k3s_candidate',
                side_effect=ready_runtime,
            ),
            mock.patch.object(
                qualification,
                'prepare_azure_distributed_social_network_candidate',
                side_effect=ready_workload,
            ),
            mock.patch.object(qualification, '_event'),
            mock.patch.object(
                qualification.application,
                'run_azure_distributed_deathstarbench_candidate_measurement',
            ) as measurement,
            mock.patch.object(
                qualification.application,
                'make_report',
            ) as report,
            mock.patch.object(qualification, '_cleanup', return_value=True) as cleanup,
        ):
            result = qualification._qualify(self.args())

        self.assertEqual(result, 0)
        cleanup.assert_called_once_with(job)
        measurement.assert_not_called()
        report.assert_not_called()
        self.assertNotIn('error', job)

    def test_workload_options_are_embedded_in_the_candidate_plan(self):
        args = self.args()
        args.warmup_seconds = 12
        args.duration_seconds = 120
        args.threads = 8
        args.connections = 80
        args.request_rate = 800

        plan = qualification._plan(
            args,
            {
                'private_key': 'private',
                'public_key': 'ssh-ed25519 public',
                'passphrase': None,
            },
        )

        self.assertEqual(plan.deathstarbench.warmup_seconds, 12)
        self.assertEqual(plan.deathstarbench.duration_seconds, 120)
        self.assertEqual(plan.deathstarbench.threads, 8)
        self.assertEqual(plan.deathstarbench.connections, 80)
        self.assertEqual(plan.deathstarbench.request_rate, 800)

    def test_incompatible_load_settings_fail_before_job_or_cloud_creation(self):
        ssh = {
            'private_key': 'private',
            'public_key': 'ssh-ed25519 public',
            'passphrase': None,
        }
        incompatible = (
            {'threads': 4, 'connections': 2, 'request_rate': 100},
            {'threads': 4, 'connections': 62, 'request_rate': 100},
            {'threads': 4, 'connections': 64, 'request_rate': 2},
            {'threads': 4, 'connections': 64, 'request_rate': 101},
        )
        for values in incompatible:
            with self.subTest(values=values):
                args = self.args()
                for name, value in values.items():
                    setattr(args, name, value)
                with self.assertRaisesRegex(
                    qualification.QualificationError,
                    'evenly divisible',
                ):
                    qualification._plan(args, ssh)

        args = self.args()
        args.connections = 62
        new_job = mock.Mock()
        provision = mock.Mock()
        with (
            mock.patch.object(qualification, '_ssh_material', return_value=ssh),
            mock.patch.object(qualification, '_read_image_lock', return_value={}),
            mock.patch.object(qualification, '_preflight_anonymous_ghcr_images'),
            mock.patch.object(qualification, '_new_job', new_job),
            mock.patch.object(
                qualification.azure,
                'provision_distributed_deathstarbench_candidate',
                provision,
            ),
        ):
            with self.assertRaises(qualification.QualificationError):
                qualification._qualify(args)
        new_job.assert_not_called()
        provision.assert_not_called()

    def test_measurement_runs_once_and_requires_complete_report_artifacts(self):
        args = self.args()
        args.measure = True
        job = self.job()
        image_lock = {'lock': 'candidate'}
        plan = SimpleNamespace(
            deathstarbench=SimpleNamespace(
                warmup_seconds=30,
                duration_seconds=60,
                connections=64,
            ),
        )

        def ready_runtime(current, **_kwargs):
            current['resources'][K3S_RUNTIME_JOURNAL_KEY] = {
                'state': 'cluster_ready'
            }

        def ready_workload(current, _lock, **_kwargs):
            current['resources'][DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY] = {
                'state': 'workload_ready'
            }

        result = qualified_result()

        def measure(current, actual_plan, actual_lock):
            self.assertIs(actual_plan, plan)
            self.assertIs(actual_lock, image_lock)
            current['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
                'state': 'measurement_complete',
                'warmup_metrics': qualified_result(
                    duration_seconds=30
                )['metrics'],
            }
            current['results'].append(result)
            return result

        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            run_directory = runs / job['id']
            run_directory.mkdir()

            def make_report(_current):
                (run_directory / 'results.json').write_text('{}')
                (run_directory / 'report.html').write_text('<html></html>')

            with (
                mock.patch.object(
                    qualification,
                    '_ssh_material',
                    return_value={
                        'private_key': 'private',
                        'public_key': 'public',
                        'passphrase': None,
                    },
                ),
                mock.patch.object(
                    qualification,
                    '_read_image_lock',
                    return_value=image_lock,
                ),
                mock.patch.object(qualification, '_preflight_anonymous_ghcr_images'),
                mock.patch.object(qualification, '_plan', return_value=plan),
                mock.patch.object(
                    qualification,
                    '_new_job',
                    return_value=(job, nullcontext()),
                ),
                mock.patch.object(
                    qualification,
                    '_exclusive_job_lock',
                    side_effect=lambda _job_id: nullcontext(),
                ),
                mock.patch.object(qualification, '_persist'),
                mock.patch.object(
                    qualification.azure,
                    'provision_distributed_deathstarbench_candidate',
                ),
                mock.patch.object(
                    qualification,
                    'prepare_azure_distributed_k3s_candidate',
                    side_effect=ready_runtime,
                ),
                mock.patch.object(
                    qualification,
                    'prepare_azure_distributed_social_network_candidate',
                    side_effect=ready_workload,
                ),
                mock.patch.object(
                    qualification.application,
                    'run_azure_distributed_deathstarbench_candidate_measurement',
                    side_effect=measure,
                ) as runner,
                mock.patch.object(
                    qualification.application,
                    'make_report',
                    side_effect=make_report,
                ) as report,
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(qualification, '_event'),
                mock.patch.object(
                    qualification,
                    '_cleanup',
                    return_value=True,
                ) as cleanup,
            ):
                outcome = qualification._qualify(args)

        self.assertEqual(outcome, 0)
        runner.assert_called_once_with(job, plan, image_lock)
        report.assert_called_once_with(job)
        cleanup.assert_called_once_with(job)
        self.assertTrue(job['_persist_results_artifact'])
        self.assertNotIn('error', job)

    def test_qualification_result_gate_rejects_every_unsafe_measurement(self):
        plan = SimpleNamespace(
            deathstarbench=SimpleNamespace(
                warmup_seconds=30,
                duration_seconds=60,
                connections=64,
            ),
        )

        def base():
            result = qualified_result()
            job = self.job()
            job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
                'state': 'measurement_complete',
                'warmup_metrics': qualified_result(
                    duration_seconds=30
                )['metrics'],
            }
            job['results'] = [result]
            return job, result

        mutations = {
            'duplicate metrics marker': lambda result: result.update({
                'output': result['output'] + 'OCI_DSB_METRICS duplicate=1\n'
            }),
            'duplicate sent marker': lambda result: result.update({
                'output': result['output'] + 'Sent 1 requests\n'
            }),
            'request errors': lambda result: result['metrics'].update(errors=1),
            'socket errors': lambda result: result['metrics'].update(socket_errors=1),
            'too many incomplete requests': lambda result: result['metrics'].update(
                uncompleted_requests=65,
                sent_requests=6065,
                completion_rate_percent=round(6000 / 6065 * 100, 6),
            ),
            'inconsistent sent requests': lambda result: result['metrics'].update(
                uncompleted_requests=1,
            ),
            'low completion rate': lambda result: result['metrics'].update(
                uncompleted_requests=64,
                sent_requests=6064,
                completion_rate_percent=98.99,
            ),
            'no successful requests': lambda result: result['metrics'].update(
                successful_requests=0
            ),
            'no total requests': lambda result: result['metrics'].update(
                total_requests=0
            ),
            'short duration': lambda result: result['metrics'].update(
                duration_seconds=53.9
            ),
            'long duration': lambda result: result['metrics'].update(
                duration_seconds=66.1
            ),
            'missing CPU': lambda result: result['metrics'].pop(
                'load_generator_cpu_percent'
            ),
            'negative CPU': lambda result: result['metrics'].update(
                load_generator_cpu_percent=-0.1
            ),
            'missing RSS': lambda result: result['metrics'].pop(
                'load_generator_peak_rss_kb'
            ),
            'zero RSS': lambda result: result['metrics'].update(
                load_generator_peak_rss_kb=0
            ),
            'missing logical CPUs': lambda result: result['metrics'].pop(
                'load_generator_logical_cpus'
            ),
            'zero logical CPUs': lambda result: result['metrics'].update(
                load_generator_logical_cpus=0
            ),
            'missing capacity': lambda result: result['metrics'].pop(
                'load_generator_capacity_used_percent'
            ),
            'negative capacity': lambda result: result['metrics'].update(
                load_generator_capacity_used_percent=-0.1
            ),
            'saturation warning': lambda result: result['metrics'].update(
                load_generator_saturation_warning='generator-limited'
            ),
            '95 percent capacity': lambda result: result['metrics'].update(
                load_generator_capacity_used_percent=95.0
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                job, result = base()
                mutate(result)
                with self.assertRaises(qualification.QualificationError):
                    qualification._require_qualified_measurement_result(
                        job,
                        plan,
                        result,
                    )

        job, result = base()
        result['metrics'].update(
            load_generator_cpu_percent=0,
            load_generator_capacity_used_percent=0,
            uncompleted_requests=3,
            sent_requests=6003,
            completion_rate_percent=round(6000 / 6003 * 100, 6),
        )
        qualification._require_qualified_measurement_result(job, plan, result)

    def test_qualification_result_gate_applies_to_persisted_warmup_metrics(self):
        plan = SimpleNamespace(
            deathstarbench=SimpleNamespace(
                warmup_seconds=30,
                duration_seconds=60,
                connections=64,
            ),
        )

        def base():
            result = qualified_result()
            job = self.job()
            job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
                'state': 'measurement_complete',
                'warmup_metrics': qualified_result(
                    duration_seconds=30
                )['metrics'],
            }
            job['results'] = [result]
            return job, result

        mutations = {
            'warm-up errors': lambda metrics: metrics.update(errors=1),
            'warm-up socket errors': lambda metrics: metrics.update(
                socket_errors=1
            ),
            'warm-up too many incomplete requests': lambda metrics: metrics.update(
                uncompleted_requests=65,
                sent_requests=6065,
                completion_rate_percent=round(6000 / 6065 * 100, 6),
            ),
            'warm-up duration drift': lambda metrics: metrics.update(
                duration_seconds=27.0 - 0.01
            ),
            'warm-up missing CPU': lambda metrics: metrics.pop(
                'load_generator_cpu_percent'
            ),
            'warm-up negative CPU': lambda metrics: metrics.update(
                load_generator_cpu_percent=-0.1
            ),
            'warm-up missing RSS': lambda metrics: metrics.pop(
                'load_generator_peak_rss_kb'
            ),
            'warm-up zero RSS': lambda metrics: metrics.update(
                load_generator_peak_rss_kb=0
            ),
            'warm-up missing logical CPUs': lambda metrics: metrics.pop(
                'load_generator_logical_cpus'
            ),
            'warm-up zero logical CPUs': lambda metrics: metrics.update(
                load_generator_logical_cpus=0
            ),
            'warm-up missing capacity': lambda metrics: metrics.pop(
                'load_generator_capacity_used_percent'
            ),
            'warm-up negative capacity': lambda metrics: metrics.update(
                load_generator_capacity_used_percent=-0.1
            ),
            'warm-up saturation warning': lambda metrics: metrics.update(
                load_generator_saturation_warning='generator-limited'
            ),
            'warm-up saturated capacity': lambda metrics: metrics.update(
                load_generator_capacity_used_percent=95.0
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                job, result = base()
                mutate(
                    job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY][
                        'warmup_metrics'
                    ]
                )
                with self.assertRaises(qualification.QualificationError):
                    qualification._require_qualified_measurement_result(
                        job,
                        plan,
                        result,
                    )

        job, result = base()
        qualification._require_qualified_measurement_result(job, plan, result)

    def test_report_gate_rejects_missing_artifacts(self):
        job = self.job()
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with (
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(qualification.application, 'make_report'),
                mock.patch.object(qualification, '_set_status'),
                mock.patch.object(qualification, '_event'),
            ):
                with self.assertRaisesRegex(
                    qualification.QualificationError,
                    'results.json, report.html',
                ):
                    qualification._write_and_require_report(job)

    def test_cleanup_failure_is_a_distinct_terminal_error(self):
        job = self.job()
        with (
            mock.patch.object(
                qualification,
                '_ssh_material',
                side_effect=qualification.QualificationError('no keys'),
            ),
        ):
            self.assertEqual(
                qualification.main(['--image-lock', '/candidate/lock.json']),
                2,
            )

        with (
            mock.patch.object(
                qualification,
                '_ssh_material',
                return_value={
                    'private_key': 'private',
                    'public_key': 'public',
                    'passphrase': None,
                },
            ),
            mock.patch.object(qualification, '_read_image_lock', return_value={}),
            mock.patch.object(qualification, '_preflight_anonymous_ghcr_images'),
            mock.patch.object(qualification, '_plan', return_value=object()),
            mock.patch.object(
                qualification,
                '_new_job',
                return_value=(job, nullcontext()),
            ),
            mock.patch.object(
                qualification,
                '_exclusive_job_lock',
                side_effect=lambda _job_id: nullcontext(),
            ),
            mock.patch.object(qualification, '_persist'),
            mock.patch.object(
                qualification.azure,
                'provision_distributed_deathstarbench_candidate',
                side_effect=RuntimeError('failure'),
            ),
            mock.patch.object(qualification, '_cleanup', return_value=False),
        ):
            self.assertEqual(qualification._qualify(self.args()), 2)

    def test_new_run_preflights_ghcr_before_creating_job_or_azure_resources(self):
        args = self.args()
        new_job = mock.Mock()
        provision = mock.Mock()
        with (
            mock.patch.object(
                qualification,
                '_ssh_material',
                return_value={
                    'private_key': 'private',
                    'public_key': 'public',
                    'passphrase': None,
                },
            ),
            mock.patch.object(qualification, '_read_image_lock', return_value={}),
            mock.patch.object(
                qualification,
                '_preflight_anonymous_ghcr_images',
                side_effect=qualification.QualificationError('not public'),
            ),
            mock.patch.object(qualification, '_new_job', new_job),
            mock.patch.object(
                qualification.azure,
                'provision_distributed_deathstarbench_candidate',
                provision,
            ),
        ):
            with self.assertRaisesRegex(
                qualification.QualificationError,
                'not public',
            ):
                qualification._qualify(args)

        new_job.assert_not_called()
        provision.assert_not_called()

    def test_resume_preflight_failure_still_cleans_loaded_candidate(self):
        args = self.args()
        args.resume = 'abc123def456'
        args.image_lock = None
        job = self.job()
        job['resources'] = {
            'provider': 'azure',
            'azure_distributed_candidate': True,
        }
        ssh = mock.Mock(
            side_effect=qualification.QualificationError('missing key')
        )
        with (
            mock.patch.object(qualification, '_load_candidate_job', return_value=job),
            mock.patch.object(
                qualification,
                '_exclusive_job_lock',
                side_effect=lambda _job_id: nullcontext(),
            ),
            mock.patch.object(qualification, '_ssh_material', ssh),
            mock.patch.object(qualification, '_persist'),
            mock.patch.object(qualification, '_cleanup', return_value=True) as cleanup,
        ):
            result = qualification._qualify(args)

        self.assertEqual(result, 1)
        cleanup.assert_called_once_with(job)
        self.assertIn('missing key', job['error'])

    def test_resume_never_replaces_saved_image_lock(self):
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            run_directory = runs / 'abc123def456'
            run_directory.mkdir()
            saved_path = run_directory / qualification.LOCK_FILENAME
            requested_path = runs / 'requested.json'
            saved_path.write_text('saved provenance\n', encoding='utf-8')
            requested_path.write_text('different candidate\n', encoding='utf-8')
            saved_lock = {'saved': True}
            requested_lock = {'saved': False}

            def read_lock(path):
                return saved_lock if path == saved_path else requested_lock

            with (
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(
                    qualification,
                    '_read_image_lock',
                    side_effect=read_lock,
                ),
            ):
                with self.assertRaisesRegex(
                    qualification.QualificationError,
                    'does not exactly match',
                ):
                    qualification._lock_for_run(
                        requested_path,
                        {'id': 'abc123def456'},
                    )

            self.assertEqual(
                saved_path.read_text(encoding='utf-8'),
                'saved provenance\n',
            )

            saved_path.unlink()
            with (
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(qualification, '_read_image_lock') as reader,
            ):
                with self.assertRaisesRegex(
                    qualification.QualificationError,
                    'saved candidate image lock is missing',
                ):
                    qualification._lock_for_run(
                        requested_path,
                        {'id': 'abc123def456'},
                    )
            reader.assert_not_called()

    def test_anonymous_ghcr_preflight_verifies_all_platform_manifest_bytes(self):
        image_lock, manifests = candidate_image_lock()
        fetched = set()

        def open_request(request, _timeout):
            url = request.full_url
            if '/token?' in url:
                return FakeResponse(url, json.dumps({'token': 'public-token'}).encode())
            repository, digest = url.split('/v2/', 1)[1].rsplit('/manifests/', 1)
            authorization = request.get_header('Authorization')
            if authorization is None:
                challenge = (
                    'Bearer realm="https://ghcr.io/token",'
                    'service="ghcr.io",'
                    f'scope="repository:{repository}:pull"'
                )
                raise HTTPError(
                    url,
                    401,
                    'Unauthorized',
                    {'WWW-Authenticate': challenge},
                    BytesIO(),
                )
            self.assertEqual(authorization, 'Bearer public-token')
            fetched.add(digest)
            return FakeResponse(
                url,
                manifests[digest],
                headers={'Docker-Content-Digest': digest},
            )

        qualification._preflight_anonymous_ghcr_images(
            image_lock,
            open_request=open_request,
        )

        self.assertEqual(fetched, set(manifests))
        self.assertEqual(len(fetched), 6)

    def test_anonymous_ghcr_preflight_rejects_wrong_manifest_bytes(self):
        image_lock, _manifests = candidate_image_lock()

        def open_request(request, _timeout):
            return FakeResponse(request.full_url, b'wrong manifest bytes')

        with self.assertRaisesRegex(
            qualification.QualificationError,
            'do not match',
        ):
            qualification._preflight_anonymous_ghcr_images(
                image_lock,
                open_request=open_request,
            )

    def test_saved_job_ownership_is_proven_before_cleanup(self):
        unrelated = self.job()
        unrelated['plan'] = {'provider': 'aws'}
        cleanup = mock.Mock()
        with (
            mock.patch.object(
                qualification.application,
                'load_persisted_job',
                return_value=unrelated,
            ),
            mock.patch.object(qualification, '_cleanup', cleanup),
        ):
            with self.assertRaisesRegex(
                qualification.QualificationError,
                'not an Azure distributed',
            ):
                qualification._cleanup_only('abc123def456')
        cleanup.assert_not_called()

        missing_marker = self.job()
        missing_marker['resources'] = {
            'provider': 'azure',
            'instance_id': '/subscriptions/example/resourceGroups/unrelated',
        }
        with self.assertRaisesRegex(
            qualification.QualificationError,
            'ownership markers',
        ):
            qualification._validate_candidate_job(
                missing_marker,
                job_id='abc123def456',
            )

    def test_resume_refuses_teardown_state_and_attempts_cleanup(self):
        args = self.args()
        args.resume = 'abc123def456'
        job = self.job()
        job['status'] = 'cleanup_failed'
        job['resources'] = {
            'provider': 'azure',
            'azure_distributed_candidate': True,
        }
        with (
            mock.patch.object(qualification, '_load_candidate_job', return_value=job),
            mock.patch.object(
                qualification,
                '_exclusive_job_lock',
                side_effect=lambda _job_id: nullcontext(),
            ),
            mock.patch.object(qualification, '_persist'),
            mock.patch.object(qualification, '_ssh_material') as ssh,
            mock.patch.object(qualification, '_cleanup', return_value=True) as cleanup,
        ):
            result = qualification._qualify(args)

        self.assertEqual(result, 1)
        ssh.assert_not_called()
        cleanup.assert_called_once_with(job)
        self.assertIn('--cleanup-only abc123def456', job['error'])

    def test_measurement_resume_refuses_one_shot_state_before_ssh(self):
        args = self.args()
        args.resume = 'abc123def456'
        args.image_lock = None
        args.measure = True
        job = self.job()
        job['resources'] = {
            'provider': 'azure',
            'azure_distributed_candidate': True,
            DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                'state': 'initialization_started'
            },
        }
        with (
            mock.patch.object(qualification, '_load_candidate_job', return_value=job),
            mock.patch.object(
                qualification,
                '_exclusive_job_lock',
                side_effect=lambda _job_id: nullcontext(),
            ),
            mock.patch.object(qualification, '_persist'),
            mock.patch.object(qualification, '_ssh_material') as ssh,
            mock.patch.object(
                qualification.azure,
                'provision_distributed_deathstarbench_candidate',
            ) as provision,
            mock.patch.object(qualification, '_cleanup', return_value=True) as cleanup,
        ):
            outcome = qualification._qualify(args)

        self.assertEqual(outcome, 1)
        ssh.assert_not_called()
        provision.assert_not_called()
        cleanup.assert_called_once_with(job)
        self.assertIn('not safe to resume', job['error'])

    def test_workload_only_resume_refuses_any_measurement_journal(self):
        args = self.args()
        args.resume = 'abc123def456'
        args.image_lock = None
        job = self.job()
        job['resources'] = {
            'provider': 'azure',
            'azure_distributed_candidate': True,
            DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                'state': 'preparing_load_generator'
            },
        }
        with (
            mock.patch.object(qualification, '_load_candidate_job', return_value=job),
            mock.patch.object(
                qualification,
                '_exclusive_job_lock',
                side_effect=lambda _job_id: nullcontext(),
            ),
            mock.patch.object(qualification, '_persist'),
            mock.patch.object(qualification, '_ssh_material') as ssh,
            mock.patch.object(
                qualification.azure,
                'provision_distributed_deathstarbench_candidate',
            ) as provision,
            mock.patch.object(qualification, '_cleanup', return_value=True),
        ):
            outcome = qualification._qualify(args)

        self.assertEqual(outcome, 1)
        ssh.assert_not_called()
        provision.assert_not_called()
        self.assertIn('workload-only', job['error'])

    def test_measurement_resume_allows_only_safe_pre_init_states(self):
        for state in ('preparing_load_generator', 'load_generator_ready'):
            with self.subTest(state=state):
                job = self.job()
                job['resources'] = {
                    'provider': 'azure',
                    'azure_distributed_candidate': True,
                    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {'state': state},
                }
                qualification._require_resumable(job, measure=True)

    def test_resume_refuses_workload_setting_drift(self):
        args = self.args()
        args.duration_seconds = 61
        with self.assertRaisesRegex(
            qualification.QualificationError,
            'duration-seconds',
        ):
            qualification._require_resume_workload_settings(self.job(), args)

    def test_execution_journal_is_cleanup_owned(self):
        self.assertIn(
            DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
            qualification.CLEANUP_OWNERSHIP_KEYS,
        )

    def test_harness_lock_excludes_a_second_process_owner(self):
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / 'abc123def456').mkdir()
            with mock.patch.object(qualification.application, 'RUNS', runs):
                with qualification._exclusive_job_lock('abc123def456'):
                    with self.assertRaisesRegex(
                        qualification.QualificationError,
                        'already owned',
                    ):
                        with qualification._exclusive_job_lock('abc123def456'):
                            self.fail('a second lock owner must never enter')

    def test_interrupt_cli_requires_measurement_and_checkpoint(self):
        invalid = (
            (
                '--image-lock', 'lock.json',
                '--interrupt-at', 'load_generator_ready',
            ),
            (
                '--image-lock', 'lock.json',
                '--interrupt-mode', 'graceful',
            ),
            (
                '--image-lock', 'lock.json', '--measure',
                '--interrupt-mode', 'hard',
            ),
            (
                '--cleanup-only', 'abc123def456', '--measure',
                '--interrupt-at', 'measurement_started',
            ),
            (
                '--cleanup-only', 'abc123def456', '--measure',
                '--interrupt-at', 'measurement_started',
                '--interrupt-mode', 'hard',
            ),
            (
                '--image-lock', 'lock.json', '--measure',
                '--interrupt-at', 'warmup_started',
                '--warmup-seconds', '0',
            ),
        )
        for argv in invalid:
            with self.subTest(argv=argv):
                args = qualification._parser().parse_args(argv)
                with self.assertRaises(qualification.QualificationError):
                    qualification._validate_interruption_options(args)

        for checkpoint in qualification.INTERRUPTION_CHECKPOINTS:
            with self.subTest(checkpoint=checkpoint, mode='default'):
                args = qualification._parser().parse_args((
                    '--image-lock', 'lock.json', '--measure',
                    '--interrupt-at', checkpoint,
                ))
                qualification._validate_interruption_options(args)
                self.assertEqual(
                    qualification._effective_interrupt_mode(args),
                    'graceful',
                )
            for mode in qualification.INTERRUPTION_MODES:
                with self.subTest(checkpoint=checkpoint, mode=mode):
                    args = qualification._parser().parse_args((
                        '--image-lock', 'lock.json', '--measure',
                        '--interrupt-at', checkpoint,
                        '--interrupt-mode', mode,
                    ))
                    qualification._validate_interruption_options(args)
                    self.assertEqual(
                        qualification._effective_interrupt_mode(args),
                        mode,
                    )

        with self.assertRaises(SystemExit):
            qualification._parser().parse_args((
                '--image-lock', 'lock.json', '--measure',
                '--interrupt-at', 'not-a-checkpoint',
            ))

    def test_interruption_artifact_is_bounded_canonical_and_strictly_reloaded(self):
        job = self.job()
        job['_key'] = 'TOP-SECRET-PRIVATE-KEY'
        job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'load_generator_ready'
        }
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            run_directory = runs / job['id']
            run_directory.mkdir()
            with mock.patch.object(qualification.application, 'RUNS', runs):
                qualification._record_checkpoint_interruption(
                    job,
                    'load_generator_ready',
                    mode='graceful',
                )
                evidence = qualification._read_interruption_evidence(job['id'])
                artifact = (
                    run_directory / qualification.INTERRUPTION_EVIDENCE_FILENAME
                )
                raw = artifact.read_bytes()

                self.assertEqual(evidence['job_id'], job['id'])
                self.assertEqual(evidence['replay_decision'], 'resume_allowed')
                self.assertEqual(evidence['cleanup_outcome'], 'pending')
                self.assertLessEqual(
                    len(raw),
                    qualification.MAX_INTERRUPTION_EVIDENCE_BYTES,
                )
                self.assertEqual(artifact.stat().st_mode & 0o777, 0o600)
                self.assertNotIn(b'TOP-SECRET-PRIVATE-KEY', raw)
                self.assertNotIn(b'_key', raw)

                malformed_documents = {
                    'oversized': b'x' * (
                        qualification.MAX_INTERRUPTION_EVIDENCE_BYTES + 1
                    ),
                    'duplicate field': (
                        b'{"schema_version":1,"schema_version":1}\n'
                    ),
                    'bool schema': qualification._serialize_interruption_evidence({
                        **evidence,
                        'schema_version': True,
                    }),
                    'signal hard mode': qualification._serialize_interruption_evidence({
                        **evidence,
                        'source': 'signal',
                        'mode': 'hard',
                        'checkpoint': None,
                        'signal': 'SIGTERM',
                    }),
                    'wrong replay decision': qualification._serialize_interruption_evidence({
                        **evidence,
                        'replay_decision': 'cleanup_only_required',
                    }),
                    'checkpoint journal mismatch': qualification._serialize_interruption_evidence({
                        **evidence,
                        'checkpoint': 'measurement_started',
                    }),
                    'unknown journal state': qualification._serialize_interruption_evidence({
                        **evidence,
                        'execution_state': 'invented_state',
                    }),
                    'noncanonical time': qualification._serialize_interruption_evidence({
                        **evidence,
                        'requested_at': '2026-01-01T00:00:00Z',
                    }),
                }
                for label, value in malformed_documents.items():
                    with self.subTest(label=label):
                        artifact.write_bytes(value)
                        with self.assertRaises(qualification.QualificationError):
                            qualification._read_interruption_evidence(job['id'])

    def test_later_signal_preserves_checkpoint_origin_and_first_signal(self):
        job = self.job()
        job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'load_generator_ready'
        }
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with mock.patch.object(qualification.application, 'RUNS', runs):
                qualification._record_checkpoint_interruption(
                    job,
                    'load_generator_ready',
                    mode='hard',
                )
                origin = qualification._read_interruption_evidence(job['id'])

                job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
                    'state': 'initialization_started'
                }
                with self.assertRaisesRegex(
                    qualification.QualificationError,
                    'origin evidence already exists',
                ):
                    qualification._record_checkpoint_interruption(
                        job,
                        'initialization_started',
                        mode='graceful',
                    )
                self.assertEqual(
                    qualification._read_interruption_evidence(job['id']),
                    origin,
                )

                job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
                    'state': 'measurement_started'
                }
                qualification._record_signal_interruption(job, signal.SIGTERM)
                after_signal = qualification._read_interruption_evidence(
                    job['id']
                )

                job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
                    'state': 'measurement_complete'
                }
                qualification._record_signal_interruption(job, signal.SIGINT)
                after_second_signal = qualification._read_interruption_evidence(
                    job['id']
                )

        for field in (
            'source',
            'mode',
            'checkpoint',
            'signal',
            'execution_state',
            'replay_decision',
            'requested_at',
        ):
            self.assertEqual(after_signal[field], origin[field])
        self.assertEqual(after_signal['source'], 'checkpoint')
        self.assertEqual(after_signal['mode'], 'hard')
        self.assertEqual(after_signal['checkpoint'], 'load_generator_ready')
        self.assertIsNone(after_signal['signal'])
        self.assertEqual(after_signal['subsequent_signal'], 'SIGTERM')
        self.assertEqual(
            after_signal['subsequent_signal_execution_state'],
            'measurement_started',
        )
        self.assertEqual(
            after_signal['subsequent_signal_replay_decision'],
            'cleanup_only_required',
        )
        self.assertEqual(after_second_signal, after_signal)

    def test_real_hard_exit_persists_evidence_and_releases_lease(self):
        job_id = 'ab12cd34ef56'
        child_source = """
import sys
from pathlib import Path
from app.deathstarbench_contract import DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
from scripts import qualify_azure_deathstarbench_distributed as qualification

runs = Path(sys.argv[1])
job_id = sys.argv[2]
qualification.application.RUNS = runs
job = {
    'id': job_id,
    'resources': {
        DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
            'state': 'load_generator_ready',
        },
    },
}
with qualification._exclusive_job_lock(job_id):
    callback = qualification._qualification_checkpoint_callback(
        interrupt_at='load_generator_ready',
        interrupt_mode='hard',
    )
    callback(job, 'load_generator_ready')
raise AssertionError('hard exit unexpectedly returned')
"""
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job_id).mkdir()
            completed = subprocess.run(
                [sys.executable, '-c', child_source, str(runs), job_id],
                cwd=qualification.PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            with mock.patch.object(qualification.application, 'RUNS', runs):
                evidence = qualification._read_interruption_evidence(job_id)
                status_after_exit = inspect_run_lease(runs, job_id)
                with qualification._exclusive_job_lock(job_id):
                    status_after_reacquire = inspect_run_lease(runs, job_id)

        self.assertEqual(
            completed.returncode,
            qualification.HARD_INTERRUPTION_EXIT_CODE,
            completed.stderr,
        )
        self.assertEqual(evidence['source'], 'checkpoint')
        self.assertEqual(evidence['mode'], 'hard')
        self.assertEqual(evidence['checkpoint'], 'load_generator_ready')
        self.assertEqual(
            evidence['cleanup_outcome'],
            'not_attempted_process_exit',
        )
        self.assertFalse(status_after_exit.held)
        self.assertTrue(status_after_reacquire.held)

    def test_recovery_refuses_second_injection_before_setup(self):
        job = self.job()
        job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'load_generator_ready'
        }
        setup = mock.Mock()
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with (
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(qualification, '_persist'),
                mock.patch.object(
                    qualification.azure,
                    'provision_distributed_deathstarbench_candidate',
                ) as provision,
                mock.patch.object(qualification, '_cleanup', return_value=True),
            ):
                qualification._record_checkpoint_interruption(
                    job,
                    'load_generator_ready',
                    mode='hard',
                )
                origin = qualification._read_interruption_evidence(job['id'])
                outcome = qualification._run_qualification(
                    job,
                    setup,
                    measure=True,
                    interrupt_at='load_generator_ready',
                    interrupt_mode='hard',
                    recovery_attempt=True,
                )
                evidence = qualification._read_interruption_evidence(job['id'])

        self.assertEqual(outcome, 1)
        setup.assert_not_called()
        provision.assert_not_called()
        for field in (
            'source',
            'mode',
            'checkpoint',
            'signal',
            'execution_state',
            'replay_decision',
            'requested_at',
        ):
            self.assertEqual(evidence[field], origin[field])
        self.assertEqual(evidence['cleanup_outcome'], 'completed')
        self.assertEqual(evidence['recovery_outcome'], 'resume_refused')

    def test_all_checkpoint_modes_persist_before_the_injected_action(self):
        class HardExitProbe(BaseException):
            pass

        job = self.job()
        job['_key'] = 'TOP-SECRET-PRIVATE-KEY'
        job['_cancel_event'] = threading.Event()
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with mock.patch.object(qualification.application, 'RUNS', runs):
                for checkpoint in qualification.INTERRUPTION_CHECKPOINTS:
                    (
                        runs
                        / job['id']
                        / qualification.INTERRUPTION_EVIDENCE_FILENAME
                    ).unlink(missing_ok=True)
                    job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
                        'state': checkpoint
                    }
                    with self.subTest(checkpoint=checkpoint, mode='hard'):
                        callback = qualification._qualification_checkpoint_callback(
                            interrupt_at=checkpoint,
                            interrupt_mode='hard',
                        )
                        with mock.patch.object(
                            qualification,
                            '_hard_exit',
                            side_effect=HardExitProbe(),
                        ) as hard_exit:
                            with self.assertRaises(HardExitProbe):
                                callback(job, checkpoint)
                        hard_exit.assert_called_once_with(
                            qualification.HARD_INTERRUPTION_EXIT_CODE
                        )
                        evidence = qualification._read_interruption_evidence(
                            job['id']
                        )
                        self.assertEqual(evidence['checkpoint'], checkpoint)
                        self.assertEqual(evidence['mode'], 'hard')
                        self.assertEqual(
                            evidence['cleanup_outcome'],
                            'not_attempted_process_exit',
                        )
                        self.assertEqual(
                            evidence['replay_decision'],
                            (
                                'resume_allowed'
                                if checkpoint == 'load_generator_ready'
                                else 'cleanup_only_required'
                            ),
                        )
                        raw = (
                            runs
                            / job['id']
                            / qualification.INTERRUPTION_EVIDENCE_FILENAME
                        ).read_bytes()
                        self.assertNotIn(b'TOP-SECRET-PRIVATE-KEY', raw)

                job['_cancel_event'].clear()
                (
                    runs
                    / job['id']
                    / qualification.INTERRUPTION_EVIDENCE_FILENAME
                ).unlink(missing_ok=True)
                job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
                    'state': 'measurement_started'
                }
                graceful = qualification._qualification_checkpoint_callback(
                    interrupt_at='measurement_started',
                    interrupt_mode='graceful',
                )
                with self.assertRaises(qualification.application.RunCancelled):
                    graceful(job, 'measurement_started')
                self.assertTrue(job['_cancel_event'].is_set())
                evidence = qualification._read_interruption_evidence(job['id'])
                self.assertEqual(evidence['mode'], 'graceful')
                self.assertEqual(evidence['cleanup_outcome'], 'pending')

    def test_graceful_checkpoint_cleans_up_and_retains_final_evidence(self):
        job = self.job()
        plan = object()

        def ready_runtime(current, **_kwargs):
            current['resources'][K3S_RUNTIME_JOURNAL_KEY] = {
                'state': 'cluster_ready'
            }

        def ready_workload(current, _lock, **_kwargs):
            current['resources'][DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY] = {
                'state': 'workload_ready'
            }

        def measure(current, _plan, _lock, *, qualification_checkpoint):
            current['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
                'state': 'load_generator_ready'
            }
            qualification_checkpoint(current, 'load_generator_ready')
            self.fail('graceful interruption must stop before measurement')

        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with (
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(qualification, '_persist'),
                mock.patch.object(
                    qualification.azure,
                    'provision_distributed_deathstarbench_candidate',
                ),
                mock.patch.object(
                    qualification,
                    'prepare_azure_distributed_k3s_candidate',
                    side_effect=ready_runtime,
                ),
                mock.patch.object(
                    qualification,
                    'prepare_azure_distributed_social_network_candidate',
                    side_effect=ready_workload,
                ),
                mock.patch.object(
                    qualification.application,
                    'run_azure_distributed_deathstarbench_candidate_measurement',
                    side_effect=measure,
                ),
                mock.patch.object(qualification, '_event'),
                mock.patch.object(
                    qualification,
                    '_cleanup',
                    return_value=True,
                ) as cleanup,
            ):
                outcome = qualification._run_qualification(
                    job,
                    lambda: (
                        plan,
                        {'public_key': 'public'},
                        {'lock': 'candidate'},
                    ),
                    measure=True,
                    interrupt_at='load_generator_ready',
                    interrupt_mode='graceful',
                )

                evidence = qualification._read_interruption_evidence(job['id'])

        self.assertEqual(outcome, 130)
        cleanup.assert_called_once_with(job)
        self.assertTrue(job['benchmark_interrupted'])
        self.assertIsNone(job['error'])
        self.assertEqual(
            qualification.application.benchmark_status(job),
            'interrupted',
        )
        self.assertEqual(evidence['cleanup_outcome'], 'completed')
        self.assertEqual(evidence['recovery_outcome'], 'not_required')

    def test_hard_exit_evidence_is_finalized_by_cleanup_only(self):
        job = self.job()
        job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'measurement_started'
        }
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with (
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(
                    qualification,
                    '_load_candidate_job',
                    return_value=job,
                ) as loader,
                mock.patch.object(
                    qualification,
                    '_exclusive_job_lock',
                    side_effect=lambda _job_id: nullcontext(),
                ),
                mock.patch.object(
                    qualification,
                    '_cleanup',
                    return_value=True,
                ),
            ):
                qualification._record_checkpoint_interruption(
                    job,
                    'measurement_started',
                    mode='hard',
                )
                outcome = qualification._cleanup_only(job['id'])
                evidence = qualification._read_interruption_evidence(job['id'])

        self.assertEqual(outcome, 0)
        self.assertEqual(loader.call_count, 2)
        self.assertEqual(evidence['cleanup_outcome'], 'completed')
        self.assertEqual(evidence['recovery_outcome'], 'cleanup_completed')

    def test_safe_resume_finalizes_prior_hard_exit_evidence(self):
        job = self.job()
        plan = object()
        job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'load_generator_ready'
        }

        def ready_runtime(current, **_kwargs):
            current['resources'][K3S_RUNTIME_JOURNAL_KEY] = {
                'state': 'cluster_ready'
            }

        def ready_workload(current, _lock, **_kwargs):
            current['resources'][DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY] = {
                'state': 'workload_ready'
            }

        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with (
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(qualification, '_persist'),
                mock.patch.object(
                    qualification.azure,
                    'provision_distributed_deathstarbench_candidate',
                ),
                mock.patch.object(
                    qualification,
                    'prepare_azure_distributed_k3s_candidate',
                    side_effect=ready_runtime,
                ),
                mock.patch.object(
                    qualification,
                    'prepare_azure_distributed_social_network_candidate',
                    side_effect=ready_workload,
                ),
                mock.patch.object(
                    qualification.application,
                    'run_azure_distributed_deathstarbench_candidate_measurement',
                    return_value={},
                ),
                mock.patch.object(
                    qualification,
                    '_require_qualified_measurement_result',
                ),
                mock.patch.object(qualification, '_write_and_require_report'),
                mock.patch.object(qualification, '_event'),
                mock.patch.object(qualification, '_cleanup', return_value=True),
            ):
                qualification._record_checkpoint_interruption(
                    job,
                    'load_generator_ready',
                    mode='hard',
                )
                qualification._update_interruption_evidence(
                    job,
                    recovery_outcome='resume_started',
                )
                outcome = qualification._run_qualification(
                    job,
                    lambda: (
                        plan,
                        {'public_key': 'public'},
                        {'lock': 'candidate'},
                    ),
                    measure=True,
                    recovery_attempt=True,
                )
                evidence = qualification._read_interruption_evidence(job['id'])

        self.assertEqual(outcome, 0)
        self.assertEqual(evidence['cleanup_outcome'], 'completed')
        self.assertEqual(evidence['recovery_outcome'], 'resume_completed')

    def test_refused_resume_is_distinct_in_retained_evidence(self):
        job = self.job()
        job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'initialization_started'
        }
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with (
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(qualification, '_persist'),
                mock.patch.object(qualification, '_cleanup', return_value=True),
            ):
                qualification._record_checkpoint_interruption(
                    job,
                    'initialization_started',
                    mode='hard',
                )
                qualification._update_interruption_evidence(
                    job,
                    recovery_outcome='resume_started',
                )
                outcome = qualification._run_qualification(
                    job,
                    lambda: (_ for _ in ()).throw(
                        qualification.QualificationError('resume refused')
                    ),
                    measure=True,
                    recovery_attempt=True,
                )
                evidence = qualification._read_interruption_evidence(job['id'])

        self.assertEqual(outcome, 1)
        self.assertEqual(evidence['cleanup_outcome'], 'completed')
        self.assertEqual(evidence['recovery_outcome'], 'resume_refused')

    def test_sigint_and_sigterm_interrupt_blocked_control_plane_waits(self):
        for signum in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=signal.Signals(signum).name):
                job = self.job()
                job['_cancel_event'] = threading.Event()
                evidence_path = None
                wait_started = threading.Event()
                release_wait = threading.Event()
                operation_finished = threading.Event()

                def provision(*_args, **_kwargs):
                    self.assertIsNotNone(evidence_path)
                    self.assertFalse(evidence_path.exists())
                    wait_started.set()
                    release_wait.wait(30)
                    self.fail(
                        'the signal must unwind a blocked control-plane wait'
                    )

                def interrupt_blocked_operation():
                    if not wait_started.wait(5):
                        return
                    os.kill(os.getpid(), signum)
                    # Bound a regression: if the handler merely sets the
                    # cancellation event again, release the fake SDK wait so
                    # the test fails quickly instead of hanging the suite.
                    if not operation_finished.wait(3):
                        release_wait.set()

                interrupter = threading.Thread(
                    target=interrupt_blocked_operation,
                    daemon=True,
                )

                with tempfile.TemporaryDirectory() as temporary:
                    runs = Path(temporary)
                    (runs / job['id']).mkdir()
                    evidence_path = (
                        runs
                        / job['id']
                        / qualification.INTERRUPTION_EVIDENCE_FILENAME
                    )
                    with (
                        mock.patch.object(qualification.application, 'RUNS', runs),
                        mock.patch.object(qualification, '_persist'),
                        mock.patch.object(
                            qualification.azure,
                            'provision_distributed_deathstarbench_candidate',
                            side_effect=provision,
                        ),
                        mock.patch.object(
                            qualification,
                            '_cleanup',
                            return_value=True,
                        ) as cleanup,
                    ):
                        started_at = time.monotonic()
                        interrupter.start()
                        try:
                            outcome = qualification._run_qualification(
                                job,
                                lambda: (
                                    object(),
                                    {'public_key': 'public'},
                                    {'lock': 'candidate'},
                                ),
                            )
                        finally:
                            operation_finished.set()
                            release_wait.set()
                            interrupter.join(timeout=5)
                        elapsed = time.monotonic() - started_at
                        evidence = qualification._read_interruption_evidence(
                            job['id']
                        )

                self.assertEqual(outcome, 128 + signum)
                self.assertLess(elapsed, 2)
                self.assertFalse(interrupter.is_alive())
                cleanup.assert_called_once_with(job)
                self.assertTrue(job['benchmark_interrupted'])
                self.assertIsNone(job['error'])
                self.assertEqual(
                    qualification.application.benchmark_status(job),
                    'interrupted',
                )
                self.assertEqual(evidence['source'], 'signal')
                self.assertEqual(evidence['signal'], signal.Signals(signum).name)
                self.assertEqual(evidence['cleanup_outcome'], 'completed')

    def test_signal_interrupts_blocked_cleanup_and_preserves_benchmark_outcome(self):
        job = self.job()
        job['results'] = [qualified_result()]
        job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'measurement_complete'
        }

        def ready_runtime(current, **_kwargs):
            current['resources'][K3S_RUNTIME_JOURNAL_KEY] = {
                'state': 'cluster_ready'
            }

        def ready_workload(current, _lock, **_kwargs):
            current['resources'][DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY] = {
                'state': 'workload_ready'
            }

        cleanup_started = threading.Event()
        release_cleanup = threading.Event()
        operation_finished = threading.Event()

        def interrupted_cleanup(_job):
            cleanup_started.set()
            release_cleanup.wait(30)
            self.fail('the signal must unwind a blocked Azure cleanup wait')

        def interrupt_blocked_cleanup():
            if not cleanup_started.wait(5):
                return
            os.kill(os.getpid(), signal.SIGTERM)
            if not operation_finished.wait(3):
                release_cleanup.set()

        interrupter = threading.Thread(
            target=interrupt_blocked_cleanup,
            daemon=True,
        )

        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with (
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(qualification, '_persist'),
                mock.patch.object(
                    qualification.azure,
                    'provision_distributed_deathstarbench_candidate',
                ),
                mock.patch.object(
                    qualification,
                    'prepare_azure_distributed_k3s_candidate',
                    side_effect=ready_runtime,
                ),
                mock.patch.object(
                    qualification,
                    'prepare_azure_distributed_social_network_candidate',
                    side_effect=ready_workload,
                ),
                mock.patch.object(
                    qualification,
                    '_cleanup',
                    side_effect=interrupted_cleanup,
                ),
                mock.patch.object(qualification, '_event'),
            ):
                started_at = time.monotonic()
                interrupter.start()
                try:
                    outcome = qualification._run_qualification(
                        job,
                        lambda: (
                            object(),
                            {'public_key': 'public'},
                            {'lock': 'candidate'},
                        ),
                    )
                finally:
                    operation_finished.set()
                    release_cleanup.set()
                    interrupter.join(timeout=5)
                elapsed = time.monotonic() - started_at
                evidence = qualification._read_interruption_evidence(job['id'])

        self.assertEqual(outcome, 2)
        self.assertLess(elapsed, 2)
        self.assertFalse(interrupter.is_alive())
        self.assertFalse(job.get('benchmark_interrupted', False))
        self.assertEqual(
            qualification.application.benchmark_status(job),
            'complete',
        )
        self.assertEqual(job['status'], 'cleanup_failed')
        self.assertIn('--cleanup-only', job['cleanup_error'])
        self.assertEqual(evidence['signal'], 'SIGTERM')
        self.assertEqual(evidence['cleanup_outcome'], 'failed')

    def test_new_job_holds_shared_lease_before_publishing_state(self):
        plan = mock.Mock()
        plan.model_dump.return_value = candidate_plan()
        observed = []
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)

            def persist(current):
                observed.append(inspect_run_lease(runs, current['id']))

            with (
                mock.patch.object(qualification.application, 'RUNS', runs),
                mock.patch.object(qualification, '_persist', side_effect=persist),
            ):
                job, lease = qualification._new_job(
                    plan,
                    {
                        'private_key': 'private',
                        'public_key': 'public',
                        'passphrase': None,
                    },
                    {'lock': 'candidate'},
                )
                try:
                    self.assertEqual(len(observed), 1)
                    self.assertTrue(observed[0].held)
                    self.assertEqual(
                        observed[0].owner.owner_label,
                        'qualification-cli',
                    )
                    self.assertTrue(inspect_run_lease(runs, job['id']).held)
                finally:
                    lease.release()

                self.assertFalse(inspect_run_lease(runs, job['id']).held)

    def test_resume_reloads_candidate_after_acquiring_lease(self):
        args = self.args()
        args.resume = 'abc123def456'
        preliminary = self.job()
        fresh = self.job()
        fresh['resources'] = {
            'provider': 'azure',
            'azure_distributed_candidate': True,
        }
        with (
            mock.patch.object(
                qualification,
                '_load_candidate_job',
                side_effect=(preliminary, fresh),
            ) as loader,
            mock.patch.object(
                qualification,
                '_exclusive_job_lock',
                side_effect=lambda _job_id: nullcontext(),
            ),
            mock.patch.object(
                qualification,
                '_read_interruption_evidence',
                return_value=None,
            ),
            mock.patch.object(
                qualification,
                '_run_qualification',
                return_value=0,
            ) as run,
        ):
            outcome = qualification._qualify(args)

        self.assertEqual(outcome, 0)
        self.assertEqual(loader.call_count, 2)
        self.assertIs(run.call_args.args[0], fresh)

    def test_cleanup_reloads_candidate_after_acquiring_lease(self):
        preliminary = self.job()
        fresh = self.job()
        fresh['resources'] = {
            'provider': 'azure',
            'azure_distributed_candidate': True,
        }
        with (
            mock.patch.object(
                qualification,
                '_load_candidate_job',
                side_effect=(preliminary, fresh),
            ) as loader,
            mock.patch.object(
                qualification,
                '_exclusive_job_lock',
                side_effect=lambda _job_id: nullcontext(),
            ),
            mock.patch.object(
                qualification,
                '_read_interruption_evidence',
                return_value=None,
            ),
            mock.patch.object(
                qualification,
                '_cleanup',
                return_value=True,
            ) as cleanup,
        ):
            outcome = qualification._cleanup_only('abc123def456')

        self.assertEqual(outcome, 0)
        self.assertEqual(loader.call_count, 2)
        cleanup.assert_called_once_with(fresh)

    def test_cleanup_accepts_destroyed_state_with_harmless_provenance(self):
        job = self.job()
        job['resources'] = {
            'provider': 'azure',
            'region': 'eastus2',
            'architecture': 'arm64',
        }

        def set_status(current, status):
            current['status'] = status

        def destroy(current):
            current['status'] = 'destroyed'

        with (
            mock.patch.object(qualification, '_set_status', side_effect=set_status),
            mock.patch.object(qualification, '_event'),
            mock.patch.object(
                qualification.application,
                'destroy_with_status',
                side_effect=destroy,
            ),
        ):
            self.assertTrue(qualification._cleanup(job))

        self.assertEqual(job['resources']['provider'], 'azure')


if __name__ == '__main__':
    unittest.main()
