from contextlib import nullcontext
import hashlib
from io import BytesIO
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
from urllib.error import HTTPError

from app.deathstarbench_contract import (
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DISTRIBUTED_IMAGE_SET_REVISION,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    DISTRIBUTED_WORKLOAD_REVISION,
    K3S_RUNTIME_ID,
    K3S_RUNTIME_JOURNAL_KEY,
)
from app.deathstarbench_k3s_workload import REQUIRED_IMAGE_KEYS, UPSTREAM_REVISION
from scripts import qualify_azure_deathstarbench_distributed as qualification


def candidate_plan():
    return {
        'provider': 'azure',
        'deathstarbench': {
            'topology_id': DISTRIBUTED_TIERED_TOPOLOGY_ID,
            'runtime_id': K3S_RUNTIME_ID,
            'workload': 'social_network',
        },
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
            mock.patch.object(qualification, '_new_job', return_value=job),
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
            mock.patch.object(qualification, '_new_job', return_value=job),
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
            mock.patch.object(qualification, '_cleanup', return_value=True) as cleanup,
        ):
            result = qualification._qualify(self.args())

        self.assertEqual(result, 0)
        cleanup.assert_called_once_with(job)
        self.assertNotIn('error', job)

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
            mock.patch.object(qualification, '_new_job', return_value=job),
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
