from __future__ import annotations

import argparse
import copy
from contextlib import ExitStack, nullcontext
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from app.deathstarbench_contract import (
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    K3S_RUNTIME_JOURNAL_KEY,
)
from app.run_lease import inspect_run_lease
from scripts import qualify_aws_oci_deathstarbench_distributed as qualification


class AwsOciDistributedQualificationTests(unittest.TestCase):
    def args(self, provider='aws', **overrides):
        values = {
            'provider': provider,
            'resume': None,
            'cleanup_only': None,
            'image_lock': Path('/candidate/image-lock.json'),
            'ssh_env_file': None,
            'region': None,
            'availability_zone': None,
            'application_shape': None,
            'application_vcpus': None,
            'application_memory_gib': None,
            'aws_profile': None,
            'aws_image_pin': None,
            'oci_config_file': None,
            'oci_profile': None,
            'oci_compartment_id': None,
            'oci_application_architecture': None,
            'oci_application_image_id': None,
            'oci_support_image_id': None,
            'oci_defined_tags_json': None,
            'measure': False,
            'interrupt_at': None,
            'interrupt_mode': None,
            'inject_initializer_response_loss': False,
            'warmup_seconds': 30,
            'duration_seconds': 60,
            'threads': 4,
            'connections': 4,
            'request_rate': 100,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def ssh(self):
        return {
            'private_key': 'PRIVATE',
            'public_key': 'ssh-ed25519 QUFBQQ== benchmark',
            'passphrase': None,
        }

    def test_main_rejects_every_opposite_provider_selector(self):
        cases = (
            ('oci', '--aws-profile', ('default', '')),
            ('oci', '--aws-image-pin', ('/tmp/aws-pin.json', '')),
            ('aws', '--oci-config-file', ('/tmp/oci-config', '')),
            ('aws', '--oci-profile', ('DEFAULT', '')),
            ('aws', '--oci-compartment-id', ('ocid1.compartment.test', '')),
            ('aws', '--oci-application-architecture', ('x86_64',)),
            ('aws', '--oci-application-image-id', ('ocid1.image.app', '')),
            ('aws', '--oci-support-image-id', ('ocid1.image.support', '')),
            ('aws', '--oci-defined-tags-json', ('/tmp/defined-tags.json', '')),
        )
        for provider, option, values in cases:
            for value in values:
                for mode in ('--cleanup-only', '--resume'):
                    with self.subTest(
                        provider=provider,
                        selector=option,
                        value=value,
                        mode=mode,
                    ), mock.patch.object(
                        qualification,
                        '_cleanup_only',
                        side_effect=AssertionError(
                            'opposite-provider selector reached cleanup'
                        ),
                    ) as cleanup, mock.patch.object(
                        qualification,
                        '_qualify',
                        side_effect=AssertionError(
                            'opposite-provider selector reached qualification'
                        ),
                    ) as qualify:
                        result = qualification.main([
                            '--provider',
                            provider,
                            mode,
                            'abc123def456',
                            option,
                            value,
                        ])
                        self.assertEqual(result, 2)
                        cleanup.assert_not_called()
                        qualify.assert_not_called()

    def test_main_rejects_checkpoint_and_response_loss_combination(self):
        for provider in ('aws', 'oci'):
            with self.subTest(provider=provider), mock.patch.object(
                qualification,
                '_qualify',
                side_effect=AssertionError(
                    'illegal interruption combination reached qualification'
                ),
            ) as qualify:
                result = qualification.main([
                    '--provider',
                    provider,
                    '--resume',
                    'abc123def456',
                    '--measure',
                    '--interrupt-at',
                    'load_generator_ready',
                    '--inject-initializer-response-loss',
                ])

            self.assertEqual(result, 2)
            qualify.assert_not_called()

    def image(self, image_id='ami-0123456789abcdef0'):
        return {
            'image_id': image_id,
            'owner_id': '679593333241',
            'name': 'Rocky-9-EC2-Base-9.8-x86_64',
            'creation_date': '2026-06-25T04:51:13.000Z',
            'product_code': '3qk9e6x2ni81uiqnorll45r3f',
            'architecture': 'x86_64',
            'root_device_name': '/dev/sda1',
        }

    def aws_pin(self):
        image = self.image()
        return {
            'schema_version': qualification.PROVIDER_PIN_SCHEMA_VERSION,
            'provider': 'aws',
            'profile': 'default',
            'region': 'us-east-1',
            'availability_zone': 'us-east-1a',
            'application_shape': 'm7i.2xlarge',
            'application_vcpus': 8,
            'application_memory_gib': 32,
            'support_image': dict(image),
            'application_image': dict(image),
            'ssh_public_key_sha256': qualification._public_key_digest(
                self.ssh()['public_key']
            ),
        }

    def test_aws_image_pin_accepts_only_official_public_null_product_code(self):
        image = {
            **self.image(),
            'owner_id': qualification.aws.AWS_ROCKY_OFFICIAL_OWNER_ID,
            'product_code': None,
        }

        self.assertEqual(
            qualification._validate_aws_image(image, label='support_image'),
            image,
        )
        with self.assertRaisesRegex(
            qualification.QualificationError,
            'invalid AMI identity',
        ):
            qualification._validate_aws_image(
                {**image, 'owner_id': '123456789012'},
                label='support_image',
            )

    def oci_pin(self):
        return {
            'schema_version': qualification.PROVIDER_PIN_SCHEMA_VERSION,
            'provider': 'oci',
            'profile': 'DEFAULT',
            'region': 'us-ashburn-1',
            'compartment_id': 'ocid1.compartment.oc1..candidate',
            'availability_domain': 'DfpY:US-ASHBURN-AD-1',
            'application_shape': 'VM.Standard.E5.Flex',
            'application_architecture': 'x86_64',
            'application_vcpus': 4,
            'application_memory_gib': 32,
            'application_image_id': 'ocid1.image.oc1.iad.application',
            'support_image_id': 'ocid1.image.oc1.iad.support',
            'defined_tags': {
                'CostCenter': {'Department': 'Sales'},
            },
            'ssh_public_key_sha256': qualification._public_key_digest(
                self.ssh()['public_key']
            ),
        }

    def job(self, provider='aws'):
        args = self.args(provider)
        pin = self.aws_pin() if provider == 'aws' else self.oci_pin()
        plan = qualification._plan(args, self.ssh(), pin)
        return {
            'id': 'abc123def456',
            'plan': plan.model_dump(
                exclude={
                    'ssh_private_key',
                    'ssh_public_key',
                    'ssh_key_passphrase',
                }
            ),
            '_key': self.ssh()['private_key'],
            '_public_key': self.ssh()['public_key'],
            '_passphrase': None,
            '_cancel_event': threading.Event(),
            'status': 'queued',
            'events': [],
            'resources': {},
            'results': [],
        }

    def test_plans_use_exact_provider_contracts(self):
        aws_plan = qualification._plan(
            self.args('aws'), self.ssh(), self.aws_pin()
        )
        self.assertEqual(aws_plan.provider, 'aws')
        self.assertEqual(aws_plan.aws_profile, 'default')
        self.assertEqual(aws_plan.availability_domain, 'us-east-1a')
        self.assertEqual(aws_plan.deathstarbench.topology_id, 'distributed_tiered_v1')

        oci_plan = qualification._plan(
            self.args('oci'), self.ssh(), self.oci_pin()
        )
        self.assertEqual(oci_plan.provider, 'oci')
        self.assertEqual(
            oci_plan.compartment_id, 'ocid1.compartment.oc1..candidate'
        )
        self.assertEqual(oci_plan.availability_domain, 'DfpY:US-ASHBURN-AD-1')
        self.assertEqual(oci_plan.deathstarbench.runtime_id, 'k3s_v1')

    def test_provider_pin_round_trip_is_immutable(self):
        job = self.job('aws')
        with tempfile.TemporaryDirectory() as temporary:
            runs = Path(temporary)
            (runs / job['id']).mkdir()
            with mock.patch.object(qualification.application, 'RUNS', runs):
                qualification._write_provider_pin(job, self.aws_pin())
                self.assertEqual(
                    qualification._read_provider_pin(job), self.aws_pin()
                )
                with self.assertRaisesRegex(
                    qualification.QualificationError, 'Refusing to replace'
                ):
                    qualification._write_provider_pin(job, self.aws_pin())

    def test_oci_defined_tags_are_pinned_propagated_and_resume_checked(self):
        pin = self.oci_pin()
        inputs = qualification._oci_inputs(pin, self.ssh())
        self.assertEqual(inputs['defined_tags'], pin['defined_tags'])
        self.assertIsNot(inputs['defined_tags'], pin['defined_tags'])
        with tempfile.TemporaryDirectory() as temporary:
            tag_file = Path(temporary) / 'tags.json'
            tag_file.write_text('{"CostCenter":{"Department":"Marketing"}}')
            with self.assertRaisesRegex(
                    qualification.QualificationError, 'defined_tags differs'):
                qualification._assert_resume_overrides(
                    self.args('oci', oci_defined_tags_json=tag_file), pin
                )

    def test_oci_clients_use_saved_profile_and_region_when_cli_omits_them(self):
        args = self.args('oci', oci_profile=None, region=None)
        pin = {**self.oci_pin(), 'profile': 'SAVED_PROFILE'}
        sentinel_clients = [object(), object(), object()]
        with (
            mock.patch.object(
                qualification.oci.sdk.config,
                'from_file',
                return_value={'region': 'wrong-region'},
            ) as from_file,
            mock.patch.object(
                qualification.oci.sdk.config, 'validate_config'
            ) as validate,
            mock.patch.object(
                qualification.oci.sdk.core,
                'ComputeClient',
                return_value=sentinel_clients[0],
            ) as compute,
            mock.patch.object(
                qualification.oci.sdk.core,
                'VirtualNetworkClient',
                return_value=sentinel_clients[1],
            ),
            mock.patch.object(
                qualification.oci.sdk.core,
                'BlockstorageClient',
                return_value=sentinel_clients[2],
            ),
        ):
            clients = qualification._oci_clients(args, pin)

        from_file.assert_called_once_with(
            file_location=str(Path.home() / '.oci' / 'config'),
            profile_name='SAVED_PROFILE',
        )
        validate.assert_called_once_with({'region': 'us-ashburn-1'})
        compute.assert_called_once_with({'region': 'us-ashburn-1'})
        self.assertEqual(
            clients,
            {
                'compute': sentinel_clients[0],
                'network': sentinel_clients[1],
                'block': sentinel_clients[2],
            },
        )

    def _orchestration_patches(self, provider, job, order):
        def runtime(candidate, **_kwargs):
            prefix = 'aws_dsb' if provider == 'aws' else 'oci_dsb'
            self.assertIn(f'{prefix}_control_public_ip', candidate['resources'])
            order.append('ssh-k3s')
            candidate['resources'][K3S_RUNTIME_JOURNAL_KEY] = {
                'state': 'cluster_ready'
            }

        def workload(candidate, _image_lock, **_kwargs):
            order.append('ssh-workload')
            candidate['resources'][DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY] = {
                'state': 'workload_ready'
            }

        return (
            mock.patch.object(
                qualification,
                'prepare_distributed_k3s_candidate',
                side_effect=runtime,
            ),
            mock.patch.object(
                qualification,
                'prepare_distributed_social_network_candidate',
                side_effect=workload,
            ),
            mock.patch.object(
                qualification,
                'qualify_distributed_network_paths',
                return_value={'schema_version': 1, 'provider': provider},
            ),
            mock.patch.object(qualification, '_cleanup', return_value=True),
            mock.patch.object(
                qualification.shared, '_read_interruption_evidence', return_value=None
            ),
            mock.patch.object(qualification.shared, '_set_status'),
            mock.patch.object(qualification.shared, '_event'),
            mock.patch.object(qualification.shared, '_persist'),
        )

    def test_aws_publishes_validated_projection_before_any_ssh(self):
        args = self.args('aws')
        pin = self.aws_pin()
        plan = qualification._plan(args, self.ssh(), pin)
        job = self.job('aws')
        order = []

        def provision(candidate, *_args, **_kwargs):
            order.append('provision')
            candidate['resources'].update({
                'provider': 'aws',
                'aws_distributed_candidate': True,
            })

        def validate(candidate, **_kwargs):
            order.append('publish')
            candidate['resources']['aws_dsb_control_public_ip'] = '198.51.100.10'

        patches = self._orchestration_patches('aws', job, order)
        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(
                qualification.aws,
                'provision_distributed_deathstarbench_candidate',
                side_effect=provision,
            ))
            stack.enter_context(mock.patch.object(
                qualification.aws,
                'validate_distributed_deathstarbench_candidate',
                side_effect=validate,
            ))
            for context in patches:
                stack.enter_context(context)
            result = qualification._run_qualification(
                job,
                lambda: (plan, self.ssh(), {'schema_version': 1}, pin, None),
                provider='aws',
                args=args,
            )

        self.assertEqual(result, 0)
        self.assertEqual(
            order[:4], ['provision', 'publish', 'ssh-k3s', 'ssh-workload']
        )

    def test_oci_publishes_validated_projection_before_any_ssh(self):
        args = self.args('oci')
        pin = self.oci_pin()
        plan = qualification._plan(args, self.ssh(), pin)
        job = self.job('oci')
        clients = {'compute': object(), 'network': object(), 'block': object()}
        order = []

        def provision(candidate, **_kwargs):
            order.append('provision')
            candidate['resources'][qualification.oci.CONTRACT_KEY] = {'owned': True}

        def publish(candidate, **_kwargs):
            order.append('publish')
            candidate['resources']['oci_dsb_control_public_ip'] = '198.51.100.20'

        patches = self._orchestration_patches('oci', job, order)
        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(
                qualification.oci,
                'provision_distributed_deathstarbench_candidate',
                side_effect=provision,
            ))
            stack.enter_context(mock.patch.object(
                qualification.oci,
                'publish_distributed_runtime_projection',
                side_effect=publish,
            ))
            for context in patches:
                stack.enter_context(context)
            result = qualification._run_qualification(
                job,
                lambda: (plan, self.ssh(), {'schema_version': 1}, pin, clients),
                provider='oci',
                args=args,
            )

        self.assertEqual(result, 0)
        self.assertEqual(
            order[:4], ['provision', 'publish', 'ssh-k3s', 'ssh-workload']
        )

    def test_aws_cleanup_uses_dedicated_destroy_recovery_and_terminal_gate(self):
        job = self.job('aws')
        job['resources'] = {'provider': 'aws', 'aws_distributed_candidate': True}

        def destroy(candidate, **_kwargs):
            candidate['status'] = 'destroyed'

        with (
            mock.patch.object(qualification.shared, '_set_status'),
            mock.patch.object(qualification.shared, '_event'),
            mock.patch.object(
                qualification.aws,
                'destroy_distributed_deathstarbench_candidate',
                side_effect=destroy,
            ) as destroy_call,
            mock.patch.object(
                qualification.aws,
                'recover_distributed_deathstarbench_candidate',
            ) as recover,
            mock.patch.object(
                qualification.aws,
                'distributed_deathstarbench_candidate_deleted',
                return_value=True,
            ) as terminal,
            mock.patch.object(
                qualification.application, 'destroy_with_status'
            ) as generic_destroy,
        ):
            result = qualification._cleanup(
                job,
                provider='aws',
                args=self.args('aws'),
                pin=self.aws_pin(),
            )

        self.assertTrue(result)
        destroy_call.assert_called_once()
        recover.assert_called_once()
        terminal.assert_called_once_with(job)
        generic_destroy.assert_not_called()

    def test_oci_cleanup_uses_dedicated_destroy_validation_and_terminal_gate(self):
        job = self.job('oci')
        job['resources'] = {qualification.oci.CONTRACT_KEY: {'owned': True}}
        clients = {'compute': object(), 'network': object(), 'block': object()}
        with (
            mock.patch.object(qualification.shared, '_set_status'),
            mock.patch.object(qualification.shared, '_event'),
            mock.patch.object(
                qualification.oci,
                'destroy_distributed_deathstarbench_candidate',
            ) as destroy_call,
            mock.patch.object(
                qualification.oci,
                'attest_distributed_candidate_terminal_deletion',
            ) as attest,
            mock.patch.object(
                qualification.oci,
                'distributed_candidate_is_deleted',
                return_value=True,
            ) as terminal,
            mock.patch.object(
                qualification.application, 'destroy_with_status'
            ) as generic_destroy,
        ):
            result = qualification._cleanup(
                job,
                provider='oci',
                args=self.args('oci'),
                pin=self.oci_pin(),
                clients=clients,
            )

        self.assertTrue(result)
        destroy_call.assert_called_once()
        attest.assert_called_once_with(job, clients=clients)
        terminal.assert_called_once_with(job)
        generic_destroy.assert_not_called()

    def test_successful_cleanup_clears_a_stale_cleanup_error(self):
        job = self.job('aws')
        job['resources'] = {
            'provider': 'aws',
            'aws_distributed_candidate': True,
        }
        job['cleanup_error'] = 'RuntimeError: earlier cleanup failed'

        with (
            mock.patch.object(qualification.shared, '_set_status'),
            mock.patch.object(qualification.shared, '_event'),
            mock.patch.object(
                qualification.aws,
                'destroy_distributed_deathstarbench_candidate',
            ),
            mock.patch.object(
                qualification.aws,
                'recover_distributed_deathstarbench_candidate',
            ),
            mock.patch.object(
                qualification.aws,
                'distributed_deathstarbench_candidate_deleted',
                return_value=True,
            ),
            mock.patch.object(qualification.shared, '_persist') as persist,
        ):
            result = qualification._cleanup(
                job,
                provider='aws',
                args=self.args('aws'),
                pin=self.aws_pin(),
            )

        self.assertTrue(result)
        self.assertNotIn('cleanup_error', job)
        persist.assert_called_once_with(job)

    def test_cleanup_fails_closed_when_terminal_predicate_is_false(self):
        job = self.job('aws')
        job['resources'] = {'provider': 'aws', 'aws_distributed_candidate': True}
        with (
            mock.patch.object(qualification.shared, '_set_status'),
            mock.patch.object(qualification.shared, '_event'),
            mock.patch.object(
                qualification.aws,
                'destroy_distributed_deathstarbench_candidate',
            ),
            mock.patch.object(
                qualification.aws,
                'recover_distributed_deathstarbench_candidate',
            ),
            mock.patch.object(
                qualification.aws,
                'distributed_deathstarbench_candidate_deleted',
                return_value=False,
            ),
        ):
            self.assertFalse(
                qualification._cleanup(
                    job,
                    provider='aws',
                    args=self.args('aws'),
                    pin=self.aws_pin(),
                )
            )
        self.assertIn('strict terminal', job['cleanup_error'])

    def test_cleanup_only_does_not_load_ssh_or_image_lock(self):
        args = self.args(
            'aws',
            cleanup_only='abc123def456',
            image_lock=None,
        )
        job = self.job('aws')
        job['resources'] = {'provider': 'aws', 'aws_distributed_candidate': True}
        with (
            mock.patch.object(
                qualification,
                '_load_candidate_job',
                return_value=job,
            ) as load,
            mock.patch.object(
                qualification.shared,
                '_exclusive_job_lock',
                return_value=nullcontext(),
            ),
            mock.patch.object(
                qualification, '_read_provider_pin', return_value=self.aws_pin()
            ),
            mock.patch.object(qualification, '_cleanup', return_value=True),
            mock.patch.object(
                qualification.shared, '_read_interruption_evidence', return_value=None
            ),
            mock.patch.object(
                qualification.shared,
                '_finalize_interruption_evidence_signal_aware',
            ),
            mock.patch.object(
                qualification.shared,
                '_ssh_material',
                side_effect=AssertionError('cleanup loaded SSH credentials'),
            ) as ssh,
            mock.patch.object(
                qualification.shared,
                '_lock_for_run',
                side_effect=AssertionError('cleanup loaded the image lock'),
            ) as image_lock,
        ):
            result = qualification._cleanup_only(args)

        self.assertEqual(result, 0)
        load.assert_called_once_with(job['id'], provider='aws')
        ssh.assert_not_called()
        image_lock.assert_not_called()

    def test_resume_provider_overrides_and_image_drift_are_refused(self):
        with self.assertRaisesRegex(
            qualification.QualificationError, 'application_shape'
        ):
            qualification._assert_resume_overrides(
                self.args('aws', application_shape='m7i.4xlarge'),
                self.aws_pin(),
            )

        changed = {
            'schema_version': 1,
            'provider': 'aws',
            'support_image': self.image('ami-11111111111111111'),
            'application_image': self.image('ami-11111111111111111'),
        }
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'pin.json'
            path.write_text(__import__('json').dumps(changed), encoding='utf-8')
            with self.assertRaisesRegex(
                qualification.QualificationError, 'image pin differs'
            ):
                qualification._assert_resume_overrides(
                    self.args('aws', aws_image_pin=path), self.aws_pin()
                )

        with self.assertRaisesRegex(
            qualification.QualificationError, 'application_image_id'
        ):
            qualification._assert_resume_overrides(
                self.args(
                    'oci',
                    oci_application_image_id='ocid1.image.oc1.iad.different',
                ),
                self.oci_pin(),
            )

    def test_failure_before_runtime_still_attempts_provider_cleanup(self):
        args = self.args('aws')
        pin = self.aws_pin()
        plan = qualification._plan(args, self.ssh(), pin)
        job = self.job('aws')
        with (
            mock.patch.object(
                qualification.aws,
                'provision_distributed_deathstarbench_candidate',
                side_effect=RuntimeError('quota denied'),
            ),
            mock.patch.object(
                qualification, '_cleanup', return_value=True
            ) as cleanup,
            mock.patch.object(
                qualification.shared, '_read_interruption_evidence', return_value=None
            ),
            mock.patch.object(qualification.shared, '_set_status'),
        ):
            result = qualification._run_qualification(
                job,
                lambda: (plan, self.ssh(), {'schema_version': 1}, pin, None),
                provider='aws',
                args=args,
            )

        self.assertEqual(result, 1)
        cleanup.assert_called_once()
        self.assertIn('quota denied', job['error'])

    def test_measured_run_gates_result_and_report_before_cleanup(self):
        args = self.args(
            'aws',
            measure=True,
            interrupt_at='load_generator_ready',
            interrupt_mode='graceful',
        )
        pin = self.aws_pin()
        plan = qualification._plan(args, self.ssh(), pin)
        job = self.job('aws')
        image_lock = {'schema_version': 1, 'images': {}}
        network_attestation = {'schema_version': 1, 'provider': 'aws'}
        measured_result = {'benchmark': 'deathstarbench', 'status': 'success'}
        checkpoint = object()
        order = []

        def provision(candidate, *_args, **_kwargs):
            order.append('provision')
            candidate['resources'].update({
                'provider': 'aws',
                'aws_distributed_candidate': True,
            })

        def publish(candidate, **_kwargs):
            order.append('publish')
            candidate['resources']['aws_dsb_control_public_ip'] = '198.51.100.10'

        def runtime(candidate, **_kwargs):
            order.append('runtime')
            candidate['resources'][K3S_RUNTIME_JOURNAL_KEY] = {
                'state': 'cluster_ready'
            }

        def workload(candidate, actual_lock, **_kwargs):
            self.assertIs(actual_lock, image_lock)
            order.append('workload')
            candidate['resources'][DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY] = {
                'state': 'workload_ready'
            }

        def network(candidate, actual_lock, **_kwargs):
            self.assertIs(candidate, job)
            self.assertIs(actual_lock, image_lock)
            order.append('network')
            return network_attestation

        def measure(candidate, actual_lock, options, **kwargs):
            self.assertIs(candidate, job)
            self.assertIs(actual_lock, image_lock)
            self.assertIs(options, plan.deathstarbench)
            self.assertEqual(
                candidate['resources'][
                    qualification.DISTRIBUTED_NETWORK_QUALIFICATION_KEY
                ],
                network_attestation,
            )
            self.assertIs(kwargs['qualification_checkpoint'], checkpoint)
            self.assertNotIn(
                'qualification_inject_initializer_response_loss',
                kwargs,
            )
            order.append('measurement')
            return measured_result

        def result_gate(candidate, actual_plan, result):
            self.assertIs(candidate, job)
            self.assertIs(actual_plan, plan)
            self.assertIs(result, measured_result)
            order.append('result-gate')

        def report_gate(candidate):
            self.assertIs(candidate, job)
            order.append('report-gate')

        def cleanup(candidate, **_kwargs):
            self.assertIs(candidate, job)
            order.append('cleanup')
            return True

        def event(_candidate, phase, _message):
            if phase == 'Qualified':
                order.append('qualified')

        with (
            mock.patch.object(
                qualification.aws,
                'provision_distributed_deathstarbench_candidate',
                side_effect=provision,
            ),
            mock.patch.object(
                qualification.aws,
                'validate_distributed_deathstarbench_candidate',
                side_effect=publish,
            ),
            mock.patch.object(
                qualification,
                'prepare_distributed_k3s_candidate',
                side_effect=runtime,
            ),
            mock.patch.object(
                qualification,
                'prepare_distributed_social_network_candidate',
                side_effect=workload,
            ),
            mock.patch.object(
                qualification,
                'qualify_distributed_network_paths',
                side_effect=network,
            ),
            mock.patch.object(
                qualification,
                'run_distributed_social_network_measurement',
                side_effect=measure,
            ),
            mock.patch.object(
                qualification.shared,
                '_qualification_checkpoint_callback',
                return_value=checkpoint,
            ) as checkpoint_factory,
            mock.patch.object(
                qualification.shared,
                '_require_qualified_measurement_result',
                side_effect=result_gate,
            ),
            mock.patch.object(
                qualification.shared,
                '_write_and_require_report',
                side_effect=report_gate,
            ),
            mock.patch.object(
                qualification,
                '_cleanup',
                side_effect=cleanup,
            ),
            mock.patch.object(
                qualification.shared,
                '_read_interruption_evidence',
                return_value=None,
            ),
            mock.patch.object(
                qualification.shared,
                '_event',
                side_effect=event,
            ),
            mock.patch.object(qualification.shared, '_set_status'),
            mock.patch.object(qualification.shared, '_persist'),
        ):
            result = qualification._run_qualification(
                job,
                lambda: (plan, self.ssh(), image_lock, pin, None),
                provider='aws',
                args=args,
                measure=True,
                interrupt_at='load_generator_ready',
                interrupt_mode='graceful',
            )

        self.assertEqual(result, 0)
        self.assertTrue(job['_persist_results_artifact'])
        self.assertEqual(
            order,
            [
                'provision',
                'publish',
                'runtime',
                'workload',
                'network',
                'measurement',
                'result-gate',
                'report-gate',
                'qualified',
                'cleanup',
            ],
        )
        checkpoint_factory.assert_called_once_with(
            interrupt_at='load_generator_ready',
            interrupt_mode='graceful',
        )

    def test_resume_forwards_response_loss_without_checkpoint_injection(self):
        args = self.args(
            'oci',
            resume='abc123def456',
            measure=True,
            inject_initializer_response_loss=True,
        )
        job = self.job('oci')
        job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'load_generator_ready',
        }
        plan = qualification._plan(args, self.ssh(), self.oci_pin())
        image_lock = {'schema_version': 1}
        clients = {'compute': object()}

        with (
            mock.patch.object(
                qualification,
                '_load_candidate_job',
                return_value=job,
            ) as load,
            mock.patch.object(
                qualification.shared,
                '_exclusive_job_lock',
                return_value=nullcontext(),
            ),
            mock.patch.object(
                qualification.shared,
                '_read_interruption_evidence',
                return_value=None,
            ),
            mock.patch.object(
                qualification.shared,
                '_require_resume_workload_settings',
            ) as workload_settings,
            mock.patch.object(
                qualification,
                '_read_provider_pin',
                return_value=self.oci_pin(),
            ) as read_pin,
            mock.patch.object(
                qualification,
                '_assert_resume_overrides',
            ) as resume_overrides,
            mock.patch.object(
                qualification.shared,
                '_ssh_material',
                return_value=self.ssh(),
            ) as ssh_material,
            mock.patch.object(
                qualification.shared,
                '_resume_job',
                return_value=(job, plan),
            ) as resume_job,
            mock.patch.object(
                qualification,
                '_validate_saved_plan',
            ) as validate_plan,
            mock.patch.object(
                qualification.shared,
                '_lock_for_run',
                return_value=image_lock,
            ) as lock_for_run,
            mock.patch.object(
                qualification.shared,
                '_preflight_anonymous_ghcr_images',
            ) as image_preflight,
            mock.patch.object(
                qualification,
                '_oci_clients',
                return_value=clients,
            ) as oci_clients,
            mock.patch.object(
                qualification,
                '_run_qualification',
                return_value=0,
            ) as run,
        ):
            result = qualification._qualify(args)

        self.assertEqual(result, 0)
        load.assert_called_once_with(job['id'], provider='oci')
        run.assert_called_once()
        self.assertIs(run.call_args.args[0], job)
        self.assertEqual(run.call_args.kwargs['provider'], 'oci')
        self.assertIs(run.call_args.kwargs['measure'], True)
        self.assertIsNone(run.call_args.kwargs['interrupt_at'])
        self.assertIsNone(run.call_args.kwargs['interrupt_mode'])
        self.assertIs(
            run.call_args.kwargs['inject_initializer_response_loss'],
            True,
        )
        self.assertIs(run.call_args.kwargs['recovery_attempt'], True)
        self.assertEqual(
            run.call_args.args[1](),
            (plan, self.ssh(), image_lock, self.oci_pin(), clients),
        )
        workload_settings.assert_called_once_with(job, args)
        read_pin.assert_called_once_with(job)
        resume_overrides.assert_called_once_with(args, self.oci_pin())
        ssh_material.assert_called_once_with(args.ssh_env_file)
        resume_job.assert_called_once_with(job, self.ssh())
        validate_plan.assert_called_once_with(job, self.oci_pin())
        lock_for_run.assert_called_once_with(args.image_lock, job)
        image_preflight.assert_called_once_with(image_lock)
        oci_clients.assert_called_once_with(args, self.oci_pin())

    def test_workload_setting_mismatch_is_refused_before_runner_or_cleanup(self):
        immutable_fields = (
            'source',
            'mode',
            'checkpoint',
            'signal',
            'execution_state',
            'replay_decision',
            'requested_at',
            'cleanup_outcome',
        )
        for provider in ('aws', 'oci'):
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as temporary:
                runs = Path(temporary)
                job = self.job(provider)
                (runs / job['id']).mkdir()
                job['resources'] = {
                    'provider': provider,
                    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                        'state': 'load_generator_ready',
                    },
                }
                with mock.patch.object(qualification.application, 'RUNS', runs):
                    qualification.shared._record_checkpoint_interruption(
                        job,
                        'load_generator_ready',
                        mode='hard',
                    )
                    origin = qualification.shared._read_interruption_evidence(
                        job['id']
                    )
                    resources_before = copy.deepcopy(job['resources'])
                    with (
                        mock.patch.object(
                            qualification,
                            '_load_candidate_job',
                            return_value=job,
                        ) as load,
                        mock.patch.object(
                            qualification.shared,
                            '_exclusive_job_lock',
                            return_value=nullcontext(),
                        ),
                        mock.patch.object(
                            qualification,
                            '_run_qualification',
                            side_effect=AssertionError(
                                'mismatched resume entered cleanup-owning runner'
                            ),
                        ) as run,
                        mock.patch.object(
                            qualification,
                            '_cleanup',
                            side_effect=AssertionError(
                                'mismatched resume attempted cleanup'
                            ),
                        ) as cleanup,
                        mock.patch.object(
                            qualification,
                            '_read_provider_pin',
                            side_effect=AssertionError(
                                'workload mismatch advanced to provider preflight'
                            ),
                        ) as read_pin,
                    ):
                        outcome = qualification.main([
                            '--provider',
                            provider,
                            '--resume',
                            job['id'],
                            '--measure',
                            '--duration-seconds',
                            '61',
                        ])
                    evidence = qualification.shared._read_interruption_evidence(
                        job['id']
                    )

                load.assert_called_once_with(job['id'], provider=provider)
                self.assertEqual(outcome, 2)
                run.assert_not_called()
                cleanup.assert_not_called()
                read_pin.assert_not_called()
                self.assertEqual(job['resources'], resources_before)
                self.assertNotIn('error', job)
                self.assertNotIn('cleanup_error', job)
                for field in immutable_fields:
                    self.assertEqual(evidence[field], origin[field])
                self.assertEqual(evidence['recovery_outcome'], 'resume_refused')

    def test_candidate_validation_refusal_is_leased_and_updates_evidence(self):
        for provider in ('aws', 'oci'):
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as temporary:
                runs = Path(temporary)
                job = self.job(provider)
                (runs / job['id']).mkdir()
                job['resources'] = {
                    'provider': provider,
                    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                        'state': 'load_generator_ready',
                    },
                }
                order = []

                class TrackingLease:
                    def __enter__(self):
                        order.append('lease-entered')

                    def __exit__(self, *_args):
                        order.append('lease-released')

                def refuse_candidate(*_args, **_kwargs):
                    order.append('candidate-load')
                    raise qualification.QualificationError(
                        'saved candidate validation failed'
                    )

                with mock.patch.object(qualification.application, 'RUNS', runs):
                    qualification.shared._record_checkpoint_interruption(
                        job,
                        'load_generator_ready',
                        mode='hard',
                    )
                    origin = qualification.shared._read_interruption_evidence(
                        job['id']
                    )
                    with (
                        mock.patch.object(
                            qualification.shared,
                            '_exclusive_job_lock',
                            return_value=TrackingLease(),
                        ) as lock,
                        mock.patch.object(
                            qualification,
                            '_load_candidate_job',
                            side_effect=refuse_candidate,
                        ) as load,
                        mock.patch.object(
                            qualification,
                            '_run_qualification',
                            side_effect=AssertionError(
                                'invalid candidate entered the runner'
                            ),
                        ) as run,
                        mock.patch.object(
                            qualification,
                            '_cleanup',
                            side_effect=AssertionError(
                                'invalid candidate attempted cleanup'
                            ),
                        ) as cleanup,
                    ):
                        outcome = qualification.main([
                            '--provider',
                            provider,
                            '--resume',
                            job['id'],
                            '--measure',
                        ])
                    evidence = qualification.shared._read_interruption_evidence(
                        job['id']
                    )

                self.assertEqual(outcome, 2)
                lock.assert_called_once_with(job['id'])
                load.assert_called_once_with(job['id'], provider=provider)
                self.assertEqual(
                    order,
                    ['lease-entered', 'candidate-load', 'lease-released'],
                )
                run.assert_not_called()
                cleanup.assert_not_called()
                for field in origin:
                    if field != 'recovery_outcome':
                        self.assertEqual(evidence[field], origin[field])
                self.assertEqual(evidence['recovery_outcome'], 'resume_refused')

    def test_retained_evidence_must_authorize_and_exactly_match_resume(self):
        cases = (
            (
                'initialization_started',
                'load_generator_ready',
                'requires cleanup-only recovery',
            ),
            (
                'load_generator_ready',
                'preparing_load_generator',
                'does not exactly match retained interruption evidence',
            ),
        )
        for provider in ('aws', 'oci'):
            for evidence_state, current_state, expected_error in cases:
                with (
                    self.subTest(
                        provider=provider,
                        evidence_state=evidence_state,
                        current_state=current_state,
                    ),
                    tempfile.TemporaryDirectory() as temporary,
                ):
                    runs = Path(temporary)
                    job = self.job(provider)
                    (runs / job['id']).mkdir()
                    job['resources'] = {
                        'provider': provider,
                        DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                            'state': evidence_state,
                        },
                    }
                    with mock.patch.object(qualification.application, 'RUNS', runs):
                        qualification.shared._record_checkpoint_interruption(
                            job,
                            evidence_state,
                            mode='hard',
                        )
                        job['resources'][
                            DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
                        ]['state'] = current_state
                        with self.assertRaisesRegex(
                            qualification.QualificationError,
                            expected_error,
                        ):
                            qualification._require_resumable(
                                job,
                                provider=provider,
                                measure=True,
                            )

    def test_exact_safe_retained_evidence_allows_resume_preflight(self):
        for provider in ('aws', 'oci'):
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as temporary:
                runs = Path(temporary)
                job = self.job(provider)
                (runs / job['id']).mkdir()
                job['resources'] = {
                    'provider': provider,
                    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                        'state': 'load_generator_ready',
                    },
                }
                with mock.patch.object(qualification.application, 'RUNS', runs):
                    qualification.shared._record_checkpoint_interruption(
                        job,
                        'load_generator_ready',
                        mode='hard',
                    )
                    qualification._require_resumable(
                        job,
                        provider=provider,
                        measure=True,
                    )

    def test_retained_evidence_checkpoint_injection_is_preflight_refused(self):
        for provider in ('aws', 'oci'):
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as temporary:
                runs = Path(temporary)
                job = self.job(provider)
                (runs / job['id']).mkdir()
                job['resources'] = {
                    'provider': provider,
                    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                        'state': 'load_generator_ready',
                    },
                }
                with mock.patch.object(qualification.application, 'RUNS', runs):
                    qualification.shared._record_checkpoint_interruption(
                        job,
                        'load_generator_ready',
                        mode='hard',
                    )
                    with (
                        mock.patch.object(
                            qualification,
                            '_load_candidate_job',
                            return_value=job,
                        ),
                        mock.patch.object(
                            qualification.shared,
                            '_exclusive_job_lock',
                            return_value=nullcontext(),
                        ),
                        mock.patch.object(
                            qualification,
                            '_run_qualification',
                            side_effect=AssertionError(
                                'checkpoint replay entered cleanup-owning runner'
                            ),
                        ) as run,
                        mock.patch.object(
                            qualification,
                            '_cleanup',
                            side_effect=AssertionError(
                                'checkpoint replay attempted cleanup'
                            ),
                        ) as cleanup,
                    ):
                        outcome = qualification.main([
                            '--provider',
                            provider,
                            '--resume',
                            job['id'],
                            '--measure',
                            '--interrupt-at',
                            'load_generator_ready',
                        ])
                    evidence = qualification.shared._read_interruption_evidence(
                        job['id']
                    )

                self.assertEqual(outcome, 2)
                run.assert_not_called()
                cleanup.assert_not_called()
                self.assertEqual(evidence['recovery_outcome'], 'resume_refused')

    def test_latest_retained_signal_evidence_is_authoritative(self):
        for provider in ('aws', 'oci'):
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as temporary:
                runs = Path(temporary)
                job = self.job(provider)
                (runs / job['id']).mkdir()
                job['resources'] = {
                    'provider': provider,
                    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                        'state': 'load_generator_ready',
                    },
                }
                with mock.patch.object(qualification.application, 'RUNS', runs):
                    qualification.shared._record_checkpoint_interruption(
                        job,
                        'load_generator_ready',
                        mode='hard',
                    )
                    job['resources'][
                        DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
                    ]['state'] = 'warmup_started'
                    qualification.shared._record_signal_interruption(
                        job,
                        signal.SIGTERM,
                    )
                    # Regressing the current journal to the safe origin cannot
                    # erase the later proof that the one-shot phase started.
                    job['resources'][
                        DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
                    ]['state'] = 'load_generator_ready'
                    with self.assertRaisesRegex(
                        qualification.QualificationError,
                        'requires cleanup-only recovery',
                    ):
                        qualification._require_resumable(
                            job,
                            provider=provider,
                            measure=True,
                        )

    def test_unsafe_resume_is_refused_before_runner_or_cleanup(self):
        immutable_fields = (
            'source',
            'mode',
            'checkpoint',
            'signal',
            'execution_state',
            'replay_decision',
            'requested_at',
            'cleanup_outcome',
        )
        for provider in ('aws', 'oci'):
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as temporary:
                runs = Path(temporary)
                job = self.job(provider)
                (runs / job['id']).mkdir()
                job['resources'] = {
                    'provider': provider,
                    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                        'state': 'warmup_started',
                    },
                }
                with mock.patch.object(qualification.application, 'RUNS', runs):
                    qualification.shared._record_checkpoint_interruption(
                        job,
                        'warmup_started',
                        mode='hard',
                    )
                    origin = qualification.shared._read_interruption_evidence(
                        job['id']
                    )
                    resources_before = copy.deepcopy(job['resources'])
                    with (
                        mock.patch.object(
                            qualification,
                            '_load_candidate_job',
                            return_value=job,
                        ) as load,
                        mock.patch.object(
                            qualification.shared,
                            '_exclusive_job_lock',
                            return_value=nullcontext(),
                        ),
                        mock.patch.object(
                            qualification,
                            '_run_qualification',
                            side_effect=AssertionError(
                                'unsafe resume entered cleanup-owning runner'
                            ),
                        ) as run,
                        mock.patch.object(
                            qualification,
                            '_cleanup',
                            side_effect=AssertionError(
                                'unsafe resume attempted cleanup'
                            ),
                        ) as cleanup,
                        mock.patch.object(
                            qualification,
                            '_read_provider_pin',
                            side_effect=AssertionError(
                                'unsafe resume advanced beyond replay preflight'
                            ),
                        ) as read_pin,
                    ):
                        outcome = qualification.main([
                            '--provider',
                            provider,
                            '--resume',
                            job['id'],
                            '--measure',
                        ])
                    evidence = qualification.shared._read_interruption_evidence(
                        job['id']
                    )

                load.assert_called_once_with(job['id'], provider=provider)
                self.assertEqual(outcome, 2)
                run.assert_not_called()
                cleanup.assert_not_called()
                read_pin.assert_not_called()
                self.assertEqual(job['status'], 'queued')
                self.assertEqual(job['resources'], resources_before)
                self.assertNotIn('error', job)
                self.assertNotIn('cleanup_error', job)
                for field in immutable_fields:
                    self.assertEqual(evidence[field], origin[field])
                self.assertEqual(evidence['recovery_outcome'], 'resume_refused')

    def test_real_hard_exit_persists_evidence_and_releases_lease(self):
        job_id = 'ab12cd34ef56'
        child_source = """
import sys
from pathlib import Path
from app.deathstarbench_contract import DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
from scripts import qualify_aws_oci_deathstarbench_distributed as qualification

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
with qualification.shared._exclusive_job_lock(job_id):
    callback = qualification.shared._qualification_checkpoint_callback(
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
                evidence = qualification.shared._read_interruption_evidence(
                    job_id
                )
                status_after_exit = inspect_run_lease(runs, job_id)
                with qualification.shared._exclusive_job_lock(job_id):
                    status_after_reacquire = inspect_run_lease(runs, job_id)

        self.assertEqual(
            completed.returncode,
            qualification.shared.HARD_INTERRUPTION_EXIT_CODE,
            completed.stderr,
        )
        self.assertEqual(evidence['source'], 'checkpoint')
        self.assertEqual(evidence['mode'], 'hard')
        self.assertEqual(evidence['checkpoint'], 'load_generator_ready')
        self.assertEqual(evidence['replay_decision'], 'resume_allowed')
        self.assertEqual(
            evidence['cleanup_outcome'],
            'not_attempted_process_exit',
        )
        self.assertEqual(evidence['recovery_outcome'], 'pending')
        self.assertFalse(status_after_exit.held)
        self.assertTrue(status_after_reacquire.held)

    def test_safe_resume_with_response_loss_finalizes_evidence_for_both_providers(self):
        immutable_fields = (
            'source',
            'mode',
            'checkpoint',
            'signal',
            'execution_state',
            'replay_decision',
            'requested_at',
        )
        for provider in ('aws', 'oci'):
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as temporary:
                runs = Path(temporary)
                (runs / 'abc123def456').mkdir()
                args = self.args(
                    provider,
                    measure=True,
                    inject_initializer_response_loss=True,
                )
                pin = self.aws_pin() if provider == 'aws' else self.oci_pin()
                plan = qualification._plan(args, self.ssh(), pin)
                job = self.job(provider)
                job['resources'] = {
                    'provider': provider,
                    K3S_RUNTIME_JOURNAL_KEY: {'state': 'cluster_ready'},
                    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY: {
                        'state': 'workload_ready'
                    },
                    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                        'state': 'load_generator_ready'
                    },
                }
                clients = (
                    {'compute': object(), 'network': object(), 'block': object()}
                    if provider == 'oci'
                    else None
                )
                measurement = mock.Mock(return_value={'status': 'completed'})
                with mock.patch.object(qualification.application, 'RUNS', runs):
                    qualification.shared._record_checkpoint_interruption(
                        job,
                        'load_generator_ready',
                        mode='hard',
                    )
                    origin = qualification.shared._read_interruption_evidence(
                        job['id']
                    )
                    with (
                        mock.patch.object(
                            qualification.aws,
                            'provision_distributed_deathstarbench_candidate',
                        ) as aws_provision,
                        mock.patch.object(
                            qualification.aws,
                            'validate_distributed_deathstarbench_candidate',
                        ),
                        mock.patch.object(
                            qualification.oci,
                            'provision_distributed_deathstarbench_candidate',
                        ) as oci_provision,
                        mock.patch.object(
                            qualification.oci,
                            'publish_distributed_runtime_projection',
                        ),
                        mock.patch.object(
                            qualification,
                            'prepare_distributed_k3s_candidate',
                        ),
                        mock.patch.object(
                            qualification,
                            'prepare_distributed_social_network_candidate',
                        ),
                        mock.patch.object(
                            qualification,
                            '_qualify_and_persist_network_paths',
                        ),
                        mock.patch.object(
                            qualification,
                            'run_distributed_social_network_measurement',
                            measurement,
                        ),
                        mock.patch.object(
                            qualification.shared,
                            '_require_qualified_measurement_result',
                        ),
                        mock.patch.object(
                            qualification.shared,
                            '_write_and_require_report',
                        ),
                        mock.patch.object(qualification.shared, '_event'),
                        mock.patch.object(
                            qualification,
                            '_cleanup',
                            return_value=True,
                        ),
                    ):
                        outcome = qualification._run_qualification(
                            job,
                            lambda: (
                                plan,
                                self.ssh(),
                                {'schema_version': 1},
                                pin,
                                clients,
                            ),
                            provider=provider,
                            args=args,
                            measure=True,
                            inject_initializer_response_loss=True,
                            recovery_attempt=True,
                        )
                    evidence = qualification.shared._read_interruption_evidence(
                        job['id']
                    )

                self.assertEqual(outcome, 0)
                if provider == 'aws':
                    aws_provision.assert_called_once()
                    oci_provision.assert_not_called()
                else:
                    oci_provision.assert_called_once()
                    aws_provision.assert_not_called()
                measurement.assert_called_once()
                measurement_kwargs = measurement.call_args.kwargs
                self.assertNotIn('qualification_checkpoint', measurement_kwargs)
                self.assertIs(
                    measurement_kwargs[
                        'qualification_inject_initializer_response_loss'
                    ],
                    True,
                )
                for field in immutable_fields:
                    self.assertEqual(evidence[field], origin[field])
                self.assertEqual(evidence['cleanup_outcome'], 'completed')
                self.assertEqual(evidence['recovery_outcome'], 'resume_completed')

    def test_cleanup_only_finalizes_hard_exit_evidence_for_both_providers(self):
        for provider in ('aws', 'oci'):
            with self.subTest(provider=provider), tempfile.TemporaryDirectory() as temporary:
                runs = Path(temporary)
                job = self.job(provider)
                (runs / job['id']).mkdir()
                job['resources'] = {
                    'provider': provider,
                    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                        'state': 'warmup_started',
                    },
                }
                args = self.args(
                    provider,
                    cleanup_only=job['id'],
                    image_lock=None,
                )
                pin = self.aws_pin() if provider == 'aws' else self.oci_pin()
                clients = {
                    'compute': object(),
                    'network': object(),
                    'block': object(),
                }
                with (
                    mock.patch.object(qualification.application, 'RUNS', runs),
                    mock.patch.object(
                        qualification,
                        '_load_candidate_job',
                        return_value=job,
                    ) as load,
                    mock.patch.object(
                        qualification.shared,
                        '_exclusive_job_lock',
                        return_value=nullcontext(),
                    ),
                    mock.patch.object(
                        qualification,
                        '_read_provider_pin',
                        return_value=pin,
                    ),
                    mock.patch.object(
                        qualification,
                        '_oci_clients',
                        return_value=clients,
                    ) as oci_clients,
                    mock.patch.object(
                        qualification,
                        '_cleanup',
                        return_value=True,
                    ) as cleanup,
                ):
                    qualification.shared._record_checkpoint_interruption(
                        job,
                        'warmup_started',
                        mode='hard',
                    )
                    outcome = qualification._cleanup_only(args)
                    evidence = qualification.shared._read_interruption_evidence(
                        job['id']
                    )

                self.assertEqual(outcome, 0)
                load.assert_called_once_with(job['id'], provider=provider)
                if provider == 'oci':
                    oci_clients.assert_called_once_with(args, pin)
                    expected_clients = clients
                else:
                    oci_clients.assert_not_called()
                    expected_clients = None
                cleanup.assert_called_once_with(
                    job,
                    provider=provider,
                    args=args,
                    pin=pin,
                    clients=expected_clients,
                )
                self.assertEqual(evidence['source'], 'checkpoint')
                self.assertEqual(evidence['checkpoint'], 'warmup_started')
                self.assertEqual(
                    evidence['replay_decision'],
                    'cleanup_only_required',
                )
                self.assertEqual(evidence['cleanup_outcome'], 'completed')
                self.assertEqual(
                    evidence['recovery_outcome'],
                    'cleanup_completed',
                )


if __name__ == '__main__':
    unittest.main()
