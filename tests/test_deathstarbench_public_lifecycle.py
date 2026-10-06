"""Focused tests for normal distributed DeathStarBench dispatch."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch

from app import deathstarbench_public as public
from app import main
from app.deathstarbench_distributed import (
    DISTRIBUTED_NETWORK_QUALIFICATION_KEY,
)
from app.models import BenchmarkPlan
from app.providers import aws


PUBLIC_KEY = (
    'ssh-ed25519 '
    'AAAAC3NzaC1lZDI1NTE5AAAAIAABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4f '
    'benchmark'
)


def distributed_plan(provider='aws', **updates):
    plan = {
        'provider': provider,
        'region': 'test-region-1',
        'shape': 'm7i.2xlarge',
        'ocpus': 8,
        'memory_gb': 32,
        'benchmarks': ['deathstarbench'],
        'llm_benchmarks': [],
        'deathstarbench': {
            'topology_id': 'distributed_tiered_v1',
            'runtime_id': 'k3s_v1',
            'workload': 'social_network',
        },
    }
    plan.update(updates)
    return plan


def released_image_lock():
    return json.loads(public.CHECKED_IN_IMAGE_LOCK.read_text())


class PublicContractTests(unittest.TestCase):
    def test_exact_predicate_and_validator(self):
        for provider in ('aws', 'azure', 'gcp', 'oci'):
            with self.subTest(provider=provider):
                plan = distributed_plan(provider)
                self.assertTrue(public.is_distributed_deathstarbench_plan(plan))
                self.assertEqual(
                    public.validate_distributed_deathstarbench_plan(plan),
                    provider,
                )

        mixed = distributed_plan(benchmarks=['deathstarbench', 'fio'])
        self.assertTrue(public.is_distributed_deathstarbench_plan(mixed))
        with self.assertRaisesRegex(ValueError, 'only selected benchmark'):
            public.validate_distributed_deathstarbench_plan(mixed)

        wrong_workload = distributed_plan()
        wrong_workload['deathstarbench']['workload'] = 'hotel_reservation'
        self.assertTrue(public.is_distributed_deathstarbench_plan(wrong_workload))
        with self.assertRaisesRegex(ValueError, 'exact Social Network'):
            public.validate_distributed_deathstarbench_plan(wrong_workload)

        compact = distributed_plan()
        compact['deathstarbench'].update(
            topology_id='single_host_v1',
            runtime_id='podman_compose_v1',
        )
        self.assertFalse(public.is_distributed_deathstarbench_plan(compact))

    def test_image_lock_is_validated_snapshotted_once_and_loaded_from_run(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_directory = root / 'abc123abc123'
            run_directory.mkdir()
            source = root / 'source.json'
            source.write_bytes(public.CHECKED_IN_IMAGE_LOCK.read_bytes())

            with patch.object(public, 'require_released_distributed_bundle'):
                target = public.snapshot_distributed_image_lock(
                    run_directory,
                    source=source,
                )
            expected = json.loads(target.read_text())
            source.write_text('{"changed": true}')

            with patch.object(public, 'require_released_distributed_bundle'):
                self.assertEqual(
                    public.load_run_distributed_image_lock(
                        root,
                        run_directory.name,
                    ),
                    expected,
                )
                with self.assertRaisesRegex(
                    public.PublicDistributedLifecycleError,
                    'Refusing to replace',
                ):
                    public.snapshot_distributed_image_lock(
                        run_directory,
                        source=public.CHECKED_IN_IMAGE_LOCK,
                    )

    def test_invalid_image_lock_is_not_snapshotted(self):
        with tempfile.TemporaryDirectory() as temporary:
            run_directory = Path(temporary) / 'run'
            run_directory.mkdir()
            source = Path(temporary) / 'invalid.json'
            source.write_text('{}')
            with self.assertRaises(public.PublicDistributedLifecycleError):
                public.snapshot_distributed_image_lock(
                    run_directory,
                    source=source,
                )
            self.assertFalse(
                (run_directory / public.IMAGE_LOCK_FILENAME).exists()
            )

    def test_unpublished_placeholder_load_driver_is_not_snapshotted(self):
        with tempfile.TemporaryDirectory() as temporary:
            run_directory = Path(temporary) / 'run'
            run_directory.mkdir()
            source = Path(temporary) / 'unpublished-driver.json'
            lock = json.loads(public.CHECKED_IN_IMAGE_LOCK.read_text())
            lock['load_driver'].update({
                'image': (
                    'ghcr.io/example/deathstarbench-load-driver@sha256:'
                    + '0' * 64
                ),
                'published': False,
                'context_sha256': '0' * 64,
                'wrk_binary_sha256': '0' * 64,
                'wrk2_source_sha256': '0' * 64,
            })
            source.write_text(json.dumps(lock))

            with self.assertRaisesRegex(
                public.PublicDistributedLifecycleError,
                'load_driver=false',
            ):
                public.snapshot_distributed_image_lock(
                    run_directory,
                    source=source,
                )
            self.assertFalse(
                (run_directory / public.IMAGE_LOCK_FILENAME).exists()
            )

    def test_snapshot_failure_removes_its_published_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            run_directory = Path(temporary) / 'run'
            run_directory.mkdir()
            with (
                patch.object(public, 'require_released_distributed_bundle'),
                patch.object(
                    public.os,
                    'fsync',
                    side_effect=[None, OSError('directory sync failed')],
                ),
            ):
                with self.assertRaisesRegex(
                    public.PublicDistributedLifecycleError,
                    'directory sync failed',
                ):
                    public.snapshot_distributed_image_lock(run_directory)
            self.assertEqual(tuple(run_directory.iterdir()), ())


class PublicProvisionDispatchTests(unittest.TestCase):
    def test_provider_dispatch_rechecks_the_run_owned_release_lock(self):
        image_lock = released_image_lock()
        image_lock['load_driver'].update({
            'image': (
                'ghcr.io/example/deathstarbench-load-driver@sha256:'
                + '0' * 64
            ),
            'published': False,
            'context_sha256': '0' * 64,
            'wrk_binary_sha256': '0' * 64,
            'wrk2_source_sha256': '0' * 64,
        })
        provision = Mock()
        with (
            patch.object(
                public.azure,
                'provision_distributed_deathstarbench_candidate',
                provision,
            ),
            self.assertRaisesRegex(ValueError, 'load_driver=false'),
        ):
            public.provision_distributed_deathstarbench(
                {'id': '123456789abc', 'resources': {}},
                distributed_plan('azure'),
                image_lock,
                public_key=PUBLIC_KEY,
                emit=Mock(),
                persist=Mock(),
                oci_context_factory=Mock(),
            )
        provision.assert_not_called()

    def test_azure_and_gcp_use_existing_candidate_controllers(self):
        for provider, adapter in (
            ('azure', public.azure),
            ('gcp', public.gcp),
        ):
            with self.subTest(provider=provider):
                job = {'id': '123456789abc', 'resources': {}}
                result = {'provider': provider}
                provision = Mock(return_value=result)
                with patch.object(
                    adapter,
                    'provision_distributed_deathstarbench_candidate',
                    provision,
                ):
                    observed = public.provision_distributed_deathstarbench(
                        job,
                        distributed_plan(provider),
                        released_image_lock(),
                        public_key=PUBLIC_KEY,
                        emit=Mock(),
                        persist=Mock(),
                        oci_context_factory=Mock(),
                    )
                self.assertIs(observed, result)
                provision.assert_called_once()

    def test_aws_resolves_then_provisions_and_live_validates(self):
        job = {'id': '123456789abc', 'resources': {}}
        pins = {'support_image': {'pin': 'support'},
                'application_image': {'pin': 'application'}}
        order = []

        def provision(candidate_job, _plan, **kwargs):
            order.append('provision')
            self.assertEqual(kwargs['support_image'], pins['support_image'])
            self.assertEqual(
                kwargs['application_image'], pins['application_image']
            )
            candidate_job['resources']['provider'] = 'aws'
            return candidate_job['resources']

        with (
            patch.object(
                public.aws,
                'resolve_distributed_deathstarbench_image_pins',
                side_effect=lambda _plan: order.append('resolve') or pins,
            ),
            patch.object(
                public.aws,
                'provision_distributed_deathstarbench_candidate',
                side_effect=provision,
            ),
            patch.object(
                public.aws,
                'validate_distributed_deathstarbench_candidate',
                side_effect=lambda *_args, **_kwargs: order.append('validate'),
            ),
        ):
            result = public.provision_distributed_deathstarbench(
                job,
                distributed_plan('aws'),
                released_image_lock(),
                public_key=PUBLIC_KEY,
                emit=Mock(),
                persist=Mock(),
                oci_context_factory=Mock(),
            )
        self.assertIs(result, job['resources'])
        self.assertEqual(order, ['resolve', 'provision', 'validate'])

    def test_oci_uses_exact_context_then_publishes_live_projection(self):
        job = {'id': '123456789abc', 'resources': {}}
        context = {
            'clients': {'compute': object(), 'network': object(), 'block': object()},
            'inputs': {'exact': 'inputs'},
        }
        order = []

        def provision(candidate_job, **kwargs):
            order.append('provision')
            self.assertIs(kwargs['inputs'], context['inputs'])
            self.assertIs(kwargs['clients'], context['clients'])
            candidate_job['resources']['provider'] = 'oci'

        with (
            patch.object(
                public.oci,
                'provision_distributed_deathstarbench_candidate',
                side_effect=provision,
            ),
            patch.object(
                public.oci,
                'publish_distributed_runtime_projection',
                side_effect=lambda *_args, **_kwargs: order.append('publish'),
            ),
        ):
            result = public.provision_distributed_deathstarbench(
                job,
                distributed_plan('oci'),
                released_image_lock(),
                public_key=PUBLIC_KEY,
                emit=Mock(),
                persist=Mock(),
                oci_context_factory=Mock(return_value=context),
            )
        self.assertIs(result, job['resources'])
        self.assertEqual(order, ['provision', 'publish'])


class PublicExecutionTests(unittest.TestCase):
    def test_generic_runtime_qualifies_and_persists_before_measurement(self):
        job = {'id': '123456789abc', 'resources': {}, 'results': []}
        plan = distributed_plan()
        image_lock = {'lock': 'run-owned'}
        order = []

        def prepare_runtime(*_args, **_kwargs):
            order.append('k3s')

        def prepare_workload(*_args, **_kwargs):
            order.append('workload')

        def qualify(*_args, **_kwargs):
            order.append('network')
            return {'qualified': True}

        def persist(candidate_job):
            order.append('persist-network')
            self.assertEqual(
                candidate_job['resources'][
                    DISTRIBUTED_NETWORK_QUALIFICATION_KEY
                ],
                {'qualified': True},
            )

        def measure(*_args, **_kwargs):
            order.append('measurement')
            return {'status': 'completed'}

        with (
            patch.object(public, 'require_released_distributed_bundle'),
            patch.object(
                public,
                'prepare_distributed_k3s_candidate',
                side_effect=prepare_runtime,
            ),
            patch.object(
                public,
                'prepare_distributed_social_network_candidate',
                side_effect=prepare_workload,
            ),
            patch.object(
                public,
                'qualify_distributed_network_paths',
                side_effect=qualify,
            ),
            patch.object(
                public,
                'run_distributed_social_network_measurement',
                side_effect=measure,
            ),
        ):
            result = public.run_distributed_deathstarbench(
                job,
                plan,
                image_lock,
                execute=Mock(),
                emit=Mock(),
                persist=persist,
            )
        self.assertEqual(result, {'status': 'completed'})
        self.assertEqual(
            order,
            ['k3s', 'workload', 'network', 'persist-network', 'measurement'],
        )

    def test_changed_network_evidence_fails_before_measurement(self):
        job = {
            'id': '123456789abc',
            'resources': {
                DISTRIBUTED_NETWORK_QUALIFICATION_KEY: {'qualified': 'old'}
            },
        }
        with (
            patch.object(public, 'require_released_distributed_bundle'),
            patch.object(public, 'prepare_distributed_k3s_candidate'),
            patch.object(public, 'prepare_distributed_social_network_candidate'),
            patch.object(
                public,
                'qualify_distributed_network_paths',
                return_value={'qualified': 'new'},
            ),
            patch.object(
                public,
                'run_distributed_social_network_measurement',
            ) as measurement,
        ):
            with self.assertRaisesRegex(
                public.PublicDistributedLifecycleError,
                'evidence changed',
            ):
                public.run_distributed_deathstarbench(
                    job,
                    distributed_plan(),
                    {'lock': 'run-owned'},
                    execute=Mock(),
                    emit=Mock(),
                    persist=Mock(),
                )
        measurement.assert_not_called()


class AwsPublicImageResolverTests(unittest.TestCase):
    @staticmethod
    def image(image_id, created, *, architecture='x86_64', **updates):
        value = {
            'ImageId': image_id,
            'OwnerId': aws.AWS_ROCKY_OFFICIAL_OWNER_ID,
            'Name': f'Rocky-9-EC2-Base-{architecture}',
            'CreationDate': created,
            'Architecture': architecture,
            'RootDeviceName': '/dev/sda1',
            'State': 'available',
            'RootDeviceType': 'ebs',
            'VirtualizationType': 'hvm',
            'ImageType': 'machine',
            'EnaSupport': True,
            'Public': True,
            'ProductCodes': [],
        }
        value.update(updates)
        return value

    def test_resolver_selects_latest_official_non_marketplace_image(self):
        older = self.image('ami-11111111', '2026-01-01T00:00:00.000Z')
        newer = self.image('ami-22222222', '2026-02-01T00:00:00.000Z')
        marketplace = self.image(
            'ami-33333333',
            '2026-03-01T00:00:00.000Z',
            ProductCodes=[{
                'ProductCodeId': 'marketplace',
                'ProductCodeType': 'marketplace',
            }],
        )
        ec2 = SimpleNamespace(
            describe_images=Mock(return_value={
                'Images': [newer, marketplace, older]
            })
        )
        session = SimpleNamespace(client=Mock(return_value=ec2))
        with patch.object(
            aws,
            '_instance_type_details',
            return_value={'architecture': 'x86_64'},
        ):
            pins = aws.resolve_distributed_deathstarbench_image_pins(
                distributed_plan('aws'),
                aws_session=session,
            )
        self.assertEqual(pins['support_image']['image_id'], 'ami-22222222')
        self.assertIsNone(pins['support_image']['product_code'])
        self.assertEqual(
            pins['application_image'],
            pins['support_image'],
        )
        self.assertIsNot(
            pins['application_image'],
            pins['support_image'],
        )
        ec2.describe_images.assert_called_once()

    def test_resolver_requires_an_approved_image_for_application_architecture(self):
        x86 = self.image('ami-11111111', '2026-01-01T00:00:00.000Z')

        def images(**kwargs):
            architecture = next(
                item['Values'][0]
                for item in kwargs['Filters']
                if item['Name'] == 'architecture'
            )
            return {'Images': [x86] if architecture == 'x86_64' else []}

        ec2 = SimpleNamespace(describe_images=Mock(side_effect=images))
        session = SimpleNamespace(client=Mock(return_value=ec2))
        with patch.object(
            aws,
            '_instance_type_details',
            return_value={'architecture': 'arm64'},
        ):
            with self.assertRaisesRegex(RuntimeError, 'arm64'):
                aws.resolve_distributed_deathstarbench_image_pins(
                    distributed_plan('aws'),
                    aws_session=session,
                )


class MainIntegrationTests(unittest.TestCase):
    def test_catalog_advertises_distributed_only_after_coordinated_preflight(self):
        def distributed_entry(document):
            self.assertEqual(
                [
                    (item['topology_id'], item['runtime_id'])
                    for item in document['deathstarbench_topologies']
                ],
                [('distributed_tiered_v1', 'k3s_v1')],
            )
            self.assertEqual(
                [item['id'] for item in document['deathstarbench_workloads']],
                ['social_network'],
            )
            return document['deathstarbench_topologies'][0]

        with patch.object(
            main,
            'preflight_distributed_deathstarbench_release',
        ):
            self.assertTrue(distributed_entry(main.catalog())['released'])
        with patch.object(
            main,
            'preflight_distributed_deathstarbench_release',
            side_effect=public.PublicDistributedLifecycleError('closed'),
        ):
            self.assertFalse(distributed_entry(main.catalog())['released'])

    def test_history_summary_preserves_topology_options(self):
        options = distributed_plan()['deathstarbench']
        plan = {
            **distributed_plan(),
            'deathstarbench': options,
        }
        with tempfile.TemporaryDirectory() as temporary:
            run_directory = Path(temporary) / '123456789abc'
            run_directory.mkdir()
            (run_directory / 'plan.json').write_text(json.dumps(plan))
            (run_directory / 'state.json').write_text(json.dumps({
                'id': run_directory.name,
                'status': 'failed',
                'benchmark_status': 'failed',
                'error': 'synthetic failure',
                'events': [],
                'resources': {},
                'results': [],
                'plan': plan,
            }))
            summary = main.run_summary(run_directory)
        self.assertEqual(summary['deathstarbench'], options)

    def test_provision_branches_before_compact_dispatch(self):
        job = {'id': '123456789abc', 'resources': {}, '_public_key': PUBLIC_KEY}
        with (
            patch.object(main, 'require_released_runtime'),
            patch.object(main, 'load_run_distributed_image_lock') as load_lock,
            patch.object(
                main,
                'provision_public_distributed_deathstarbench',
                return_value={'distributed': True},
            ) as distributed,
            patch.object(main, 'dispatch_provider_operation') as compact,
        ):
            result = main.provision(job, distributed_plan('azure'))
        self.assertEqual(result, {'distributed': True})
        load_lock.assert_called_once_with(main.RUNS, job['id'])
        distributed.assert_called_once()
        compact.assert_not_called()

    def test_benchmark_dispatch_loads_only_run_lock_before_generic_runtime(self):
        job = {'id': '123456789abc', 'resources': {}}
        run_lock = {'run': 'owned'}
        with (
            patch.object(main, 'require_released_runtime'),
            patch.object(
                main,
                'load_run_distributed_image_lock',
                return_value=run_lock,
            ) as load_lock,
            patch.object(
                main,
                'run_public_distributed_deathstarbench',
                return_value={'status': 'completed'},
            ) as distributed,
            patch.object(main, 'dispatch_provider_operation') as compact,
        ):
            result = main.run_benchmarks(job, distributed_plan('gcp'))
        self.assertEqual(result, {'status': 'completed'})
        load_lock.assert_called_once_with(main.RUNS, job['id'])
        self.assertIs(distributed.call_args.args[2], run_lock)
        compact.assert_not_called()

    def test_oci_context_uses_platform_image_architecture_for_ax_shapes(self):
        compute, network, block = object(), object(), object()
        identity = SimpleNamespace(
            list_availability_domains=Mock(
                return_value=SimpleNamespace(
                    data=[SimpleNamespace(name='test-ad-1')]
                )
            )
        )
        tags = {'Operations': {'Owner': 'benchmark-team'}}
        cases = (
            ('VM.Standard4.Ax.Flex', 'Oracle-Linux-9.6-2026.09.01-0', 'x86_64'),
            ('VM.Standard.E6.Ax.Flex', 'Oracle-Linux-9.6-2026.09.01-0', 'x86_64'),
            (
                'VM.Standard.A4.Ax.Flex',
                'Oracle-Linux-9.6-aarch64-2026.09.01-0',
                'arm64',
            ),
        )
        for shape, display_name, expected_architecture in cases:
            with self.subTest(shape=shape):
                plan = distributed_plan(
                    'oci',
                    shape=shape,
                    ocpus=4,
                    memory_gb=32,
                    oci_defined_tags=tags,
                    compartment_id='ocid1.compartment.oc1..example',
                )

                def image(_compute, _compartment, image_shape, require_ol9=False):
                    self.assertTrue(require_ol9)
                    application = image_shape == plan['shape']
                    return SimpleNamespace(
                        id=(
                            'ocid1.image.oc1..application'
                            if application
                            else 'ocid1.image.oc1..support'
                        ),
                        display_name=(
                            display_name
                            if application
                            else 'Oracle-Linux-9.6-2026.09.01-0'
                        ),
                    )

                with (
                    patch.object(
                        main,
                        'clients',
                        return_value=(
                            {'tenancy': 'ocid1.tenancy.oc1..example'},
                            compute,
                            network,
                            block,
                            identity,
                        ),
                    ),
                    patch.object(
                        main,
                        'latest_oracle_linux_image',
                        side_effect=image,
                    ),
                ):
                    context = main.distributed_oci_provisioning_context(
                        plan,
                        PUBLIC_KEY,
                    )
                self.assertEqual(
                    context['inputs']['application_image_id'],
                    'ocid1.image.oc1..application',
                )
                self.assertEqual(
                    context['inputs']['support_image_id'],
                    'ocid1.image.oc1..support',
                )
                self.assertEqual(
                    context['inputs']['architecture'],
                    expected_architecture,
                )
                self.assertIs(context['inputs']['defined_tags'], tags)
                self.assertEqual(
                    set(context['clients']),
                    {'compute', 'network', 'block'},
                )


class CreateJobSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_precedes_task_creation_under_the_run_lease(self):
        class Lease:
            def __init__(self):
                self.release = Mock()

        class Task:
            def add_done_callback(self, callback):
                self.callback = callback

            def exception(self):
                return None

        plan = BenchmarkPlan(
            provider='aws',
            region='us-east-2',
            shape='m7i.2xlarge',
            ocpus=8,
            memory_gb=32,
            ssh_private_key='private',
            ssh_public_key=PUBLIC_KEY,
            storage={'additional_volume': False},
            benchmarks=['deathstarbench'],
            deathstarbench={
                'topology_id': 'distributed_tiered_v1',
                'runtime_id': 'k3s_v1',
                'workload': 'social_network',
            },
        )
        order = []
        task = Task()
        lease = Lease()

        def snapshot(run_directory):
            order.append('snapshot')
            return public.snapshot_distributed_image_lock(run_directory)

        def create_task(coroutine):
            order.append('task')
            coroutine.close()
            return task

        def preflight():
            order.append('preflight')

        with tempfile.TemporaryDirectory() as temporary:
            with (
                patch.object(main, 'RUNS', Path(temporary)),
                patch.object(main, 'require_released_runtime'),
                patch.object(
                    main,
                    'preflight_distributed_deathstarbench_release',
                    side_effect=preflight,
                ),
                patch.object(
                    public,
                    'require_released_distributed_bundle',
                ),
                patch.object(main, 'derive_public_key', return_value=PUBLIC_KEY),
                patch.object(main, 'normalize_public_key', return_value=PUBLIC_KEY),
                patch.object(main, 'normalize_private_key', return_value='private'),
                patch.object(main, 'acquire_run_lease', return_value=lease),
                patch.object(
                    main,
                    'snapshot_distributed_image_lock',
                    side_effect=snapshot,
                ),
                patch.object(main, 'persist_job_state'),
                patch.object(main, 'retain_job_task'),
                patch.object(
                    main.uuid,
                    'uuid4',
                    return_value=SimpleNamespace(hex='a' * 32),
                ),
                patch.object(
                    main.asyncio,
                    'create_task',
                    side_effect=create_task,
                ),
            ):
                try:
                    result = await main.create_job(plan)
                    self.assertEqual(result, {'id': 'a' * 12})
                    self.assertEqual(order, ['preflight', 'snapshot', 'task'])
                    self.assertTrue(
                        (
                            Path(temporary)
                            / ('a' * 12)
                            / public.IMAGE_LOCK_FILENAME
                        ).is_file()
                    )
                finally:
                    main.jobs.pop('a' * 12, None)


if __name__ == '__main__':
    unittest.main()
