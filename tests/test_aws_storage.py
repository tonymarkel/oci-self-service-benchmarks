import copy
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError
from pydantic import ValidationError

from app import main
from app.models import BenchmarkPlan
from app.providers import aws


class FakeSession:
    def __init__(self, **clients):
        self.clients = clients

    def client(self, service_name, region_name=None):
        return self.clients[service_name]


def storage_plan(**overrides):
    values = {
        'provider': 'aws',
        'aws_profile': 'default',
        'region': 'us-east-2',
        'availability_domain': 'us-east-2a',
        'shape': 'm7i.large',
        'ocpus': 2,
        'memory_gb': 8,
        'ssh_private_key': 'private',
        'ssh_public_key': 'ssh-ed25519 AAAATEST',
        'benchmarks': ['sysbench', 'fio'],
        'sysbench': {'workloads': ['fileio']},
        'storage': {
            'additional_volume': True,
            'additional_size_gb': 250,
        },
    }
    values.update(overrides)
    return BenchmarkPlan(**values)


def aws_tags(job_id='job123'):
    return [
        {'Key': 'Name', 'Value': f'benchmark-{job_id}'},
        {'Key': 'benchmark-job', 'Value': job_id},
        {'Key': 'managed-by', 'Value': aws.MANAGED_BY},
        {'Key': 'benchmark-role', 'Value': 'data'},
    ]


class AwsStoragePlanTests(unittest.TestCase):
    def test_fio_and_sysbench_fileio_require_a_data_volume_on_aws(self):
        for benchmarks, workloads in (
            (['fio'], ['cpu']),
            (['sysbench'], ['fileio']),
        ):
            with self.subTest(benchmarks=benchmarks, workloads=workloads):
                with self.assertRaisesRegex(
                    ValidationError,
                    'additional /data volume',
                ):
                    storage_plan(
                        benchmarks=benchmarks,
                        sysbench={'workloads': workloads},
                        storage={'additional_volume': False},
                    )

        plan = storage_plan()
        self.assertEqual(plan.storage.additional_size_gb, 250)
        self.assertTrue(plan.storage.additional_volume)


class AwsStorageProvisionTests(unittest.TestCase):
    def setUp(self):
        self.ec2 = MagicMock()
        self.sts = MagicMock()
        self.session = FakeSession(ec2=self.ec2, sts=self.sts)
        self.sts.get_caller_identity.return_value = {
            'Account': '123456789012',
            'Arn': 'arn:aws:iam::123456789012:user/tester',
        }
        self.ec2.create_vpc.return_value = {'Vpc': {'VpcId': 'vpc-1'}}
        self.ec2.create_internet_gateway.return_value = {
            'InternetGateway': {'InternetGatewayId': 'igw-1'}
        }
        self.ec2.create_subnet.return_value = {'Subnet': {
            'SubnetId': 'subnet-1',
            'AvailabilityZone': 'us-east-2a',
        }}
        self.ec2.create_route_table.return_value = {
            'RouteTable': {'RouteTableId': 'rtb-1'}
        }
        self.ec2.associate_route_table.return_value = {
            'AssociationId': 'rtbassoc-1'
        }
        self.ec2.create_security_group.return_value = {'GroupId': 'sg-1'}
        self.ec2.import_key_pair.return_value = {'KeyPairId': 'key-1'}
        self.ec2.create_volume.return_value = {
            'VolumeId': 'vol-0123456789abcdef0'
        }
        self.ec2.run_instances.return_value = {
            'Instances': [{'InstanceId': 'i-1'}]
        }
        self.ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-1',
                'PublicIpAddress': '198.51.100.10',
                'PrivateIpAddress': '10.42.1.10',
            }],
        }]}
        self.type_details = {
            'architecture': 'x86_64',
            'vcpu': 2,
            'memory_gb': 8,
            'bare_metal': False,
            'hypervisor': 'nitro',
        }
        self.image = {
            'image_id': 'ami-123',
            'name': 'al2023-test',
            'root_device_name': '/dev/xvda',
            'parameter_name': '/aws/service/example',
            'parameter_version': 1,
        }

    def provision(self, job, persist=None):
        with (
            patch.object(
                aws,
                '_instance_type_details',
                return_value=self.type_details,
            ),
            patch.object(
                aws,
                '_resolve_availability_zone',
                return_value='us-east-2a',
            ),
            patch.object(
                aws,
                'latest_amazon_linux_2023_ami',
                return_value=self.image,
            ),
        ):
            return aws.provision(
                job,
                storage_plan(),
                public_key='ssh-ed25519 AAAATEST',
                aws_session=self.session,
                persist=persist,
            )

    def test_gp3_volume_uses_baseline_performance_and_is_attached(self):
        job = {'id': 'job123', 'resources': {}}
        snapshots = []

        resources = self.provision(
            job,
            persist=lambda value: snapshots.append(
                copy.deepcopy(value['resources'])
            ),
        )

        request = self.ec2.create_volume.call_args.kwargs
        self.assertEqual(request['AvailabilityZone'], 'us-east-2a')
        self.assertEqual(request['Size'], 250)
        self.assertEqual(request['VolumeType'], 'gp3')
        self.assertEqual(request['Iops'], 3000)
        self.assertEqual(request['Throughput'], 125)
        self.assertTrue(request['Encrypted'])
        self.assertIn(
            {'Key': 'benchmark-role', 'Value': 'data'},
            request['TagSpecifications'][0]['Tags'],
        )
        root_mapping = self.ec2.run_instances.call_args.kwargs[
            'BlockDeviceMappings'
        ][0]['Ebs']
        self.assertTrue(root_mapping['Encrypted'])
        self.ec2.attach_volume.assert_called_once_with(
            Device='/dev/sdf',
            InstanceId='i-1',
            VolumeId='vol-0123456789abcdef0',
        )
        self.assertEqual(
            resources['aws_data_volume_id'],
            'vol-0123456789abcdef0',
        )
        self.assertEqual(resources['aws_data_volume_iops'], 3000)
        self.assertEqual(resources['aws_data_volume_throughput_mibps'], 125)
        self.assertTrue(
            any(
                item.get('aws_data_volume_id') == 'vol-0123456789abcdef0'
                for item in snapshots
            )
        )

    def test_lost_create_volume_response_keeps_reconciliation_contract(self):
        self.ec2.create_volume.side_effect = TimeoutError('response lost')
        job = {'id': 'job123', 'resources': {}}

        with self.assertRaisesRegex(TimeoutError, 'response lost'):
            self.provision(job)

        resources = job['resources']
        self.assertEqual(len(resources['aws_data_volume_client_token']), 32)
        self.assertEqual(resources['aws_data_volume_size_gb'], 250)
        self.assertEqual(resources['aws_data_volume_type'], 'gp3')
        self.assertTrue(resources['aws_data_volume_create_ambiguous'])
        self.assertNotIn('aws_data_volume_id', resources)

    def test_confirmed_create_volume_rejection_clears_replay_contract(self):
        self.ec2.create_volume.side_effect = ClientError(
            {
                'Error': {
                    'Code': 'UnauthorizedOperation',
                    'Message': 'denied',
                },
                'ResponseMetadata': {'HTTPStatusCode': 403},
            },
            'CreateVolume',
        )
        job = {'id': 'job123', 'resources': {}}

        with self.assertRaises(ClientError):
            self.provision(job)

        self.assertNotIn(
            'aws_data_volume_client_token',
            job['resources'],
        )
        self.assertNotIn(
            'aws_data_volume_create_ambiguous',
            job['resources'],
        )


class AwsStorageCleanupTests(unittest.TestCase):
    def test_cleanup_deletes_the_exact_owned_volume_and_forgets_metadata(self):
        ec2 = MagicMock()
        ec2.describe_volumes.return_value = {'Volumes': [{
            'VolumeId': 'vol-0123456789abcdef0',
            'Attachments': [],
            'Tags': aws_tags(),
        }]}
        job = {
            'id': 'job123',
            'status': 'complete',
            'plan': {'provider': 'aws', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'region': 'us-east-2',
                'aws_data_volume_id': 'vol-0123456789abcdef0',
                'aws_data_volume_client_token': 'token',
                'aws_data_volume_type': 'gp3',
                'aws_data_volume_size_gb': 250,
            },
        }

        aws.destroy_resources(
            job,
            aws_session=FakeSession(ec2=ec2),
        )

        ec2.delete_volume.assert_called_once_with(
            VolumeId='vol-0123456789abcdef0'
        )
        self.assertNotIn('aws_data_volume_id', job['resources'])
        self.assertNotIn('aws_data_volume_client_token', job['resources'])
        self.assertEqual(job['status'], 'destroyed')

    def test_cleanup_reconciles_a_response_lost_data_volume_by_exact_tags(self):
        ec2 = MagicMock()
        volume = {
            'VolumeId': 'vol-0123456789abcdef0',
            'Attachments': [],
            'Tags': aws_tags(),
        }
        ec2.describe_volumes.side_effect = [
            {'Volumes': [volume]},
            {'Volumes': [volume]},
        ]
        job = {
            'id': 'job123',
            'status': 'interrupted',
            'plan': {'provider': 'aws', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'region': 'us-east-2',
                'aws_data_volume_client_token': 'token',
                'aws_data_volume_create_ambiguous': True,
                'aws_data_volume_type': 'gp3',
                'aws_data_volume_size_gb': 250,
                'aws_data_volume_availability_zone': 'us-east-2a',
                'aws_data_volume_iops': 3000,
                'aws_data_volume_throughput_mibps': 125,
            },
        }

        aws.destroy_resources(
            job,
            aws_session=FakeSession(ec2=ec2),
        )

        filters = ec2.describe_volumes.call_args_list[0].kwargs['Filters']
        self.assertIn(
            {'Name': 'tag:benchmark-role', 'Values': ['data']},
            filters,
        )
        self.assertIn(
            {'Name': 'tag:benchmark-job', 'Values': ['job123']},
            filters,
        )
        self.assertIn(
            {'Name': 'tag:managed-by', 'Values': [aws.MANAGED_BY]},
            filters,
        )
        ec2.delete_volume.assert_called_once_with(
            VolumeId='vol-0123456789abcdef0'
        )

    def test_zero_match_replays_the_exact_idempotent_encrypted_request(self):
        ec2 = MagicMock()
        snapshots = []
        volume = {
            'VolumeId': 'vol-0123456789abcdef0',
            'Attachments': [],
            'Tags': aws_tags(),
        }
        ec2.describe_volumes.side_effect = [
            {'Volumes': []},
            {'Volumes': [volume]},
        ]
        ec2.create_volume.return_value = volume
        job = {
            'id': 'job123',
            'status': 'interrupted',
            'plan': {'provider': 'aws', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'region': 'us-east-2',
                'availability_zone': 'us-east-2a',
                'aws_data_volume_client_token': 'stable-token',
                'aws_data_volume_create_ambiguous': True,
                'aws_data_volume_availability_zone': 'us-east-2a',
                'aws_data_volume_size_gb': 250,
                'aws_data_volume_type': 'gp3',
                'aws_data_volume_iops': 3000,
                'aws_data_volume_throughput_mibps': 125,
            },
        }

        aws.destroy_resources(
            job,
            aws_session=FakeSession(ec2=ec2),
            persist=lambda value: snapshots.append(
                copy.deepcopy(value['resources'])
            ),
        )

        request = ec2.create_volume.call_args.kwargs
        self.assertEqual(request['ClientToken'], 'stable-token')
        self.assertEqual(request['AvailabilityZone'], 'us-east-2a')
        self.assertEqual(request['Size'], 250)
        self.assertTrue(request['Encrypted'])
        ec2.delete_volume.assert_called_once_with(
            VolumeId='vol-0123456789abcdef0'
        )
        self.assertFalse(any(
            snapshot.get('aws_data_volume_create_ambiguous') is False
            and not snapshot.get('aws_data_volume_id')
            for snapshot in snapshots
        ))

    def test_replay_failure_keeps_contract_but_cleans_independent_key(self):
        ec2 = MagicMock()
        ec2.describe_volumes.return_value = {'Volumes': []}
        ec2.create_volume.side_effect = TimeoutError('still ambiguous')
        ec2.describe_key_pairs.return_value = {'KeyPairs': [{
            'KeyPairId': 'key-1',
            'KeyName': 'benchmark-job123',
            'Tags': aws_tags()[:-1],
        }]}
        job = {
            'id': 'job123',
            'status': 'interrupted',
            'plan': {'provider': 'aws', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'region': 'us-east-2',
                'aws_key_pair_id': 'key-1',
                'aws_key_pair_name': 'benchmark-job123',
                'availability_zone': 'us-east-2a',
                'aws_data_volume_client_token': 'stable-token',
                'aws_data_volume_create_ambiguous': True,
                'aws_data_volume_availability_zone': 'us-east-2a',
                'aws_data_volume_size_gb': 250,
                'aws_data_volume_type': 'gp3',
                'aws_data_volume_iops': 3000,
                'aws_data_volume_throughput_mibps': 125,
            },
        }

        with self.assertRaisesRegex(RuntimeError, 'still ambiguous'):
            aws.destroy_resources(
                job,
                aws_session=FakeSession(ec2=ec2),
            )

        ec2.delete_key_pair.assert_called_once_with(KeyPairId='key-1')
        self.assertIn('aws_data_volume_client_token', job['resources'])
        self.assertIn(
            'aws_data_volume_reconciliation_error',
            job['resources'],
        )

    def test_restart_accepts_a_still_detaching_recorded_instance_attachment(self):
        ec2 = MagicMock()
        ec2.describe_volumes.return_value = {'Volumes': [{
            'VolumeId': 'vol-0123456789abcdef0',
            'Attachments': [{
                'InstanceId': 'i-terminated',
                'State': 'detaching',
            }],
            'Tags': aws_tags(),
        }]}
        job = {
            'id': 'job123',
            'status': 'cleanup_failed',
            'plan': {'provider': 'aws', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'region': 'us-east-2',
                # The prior cleanup already terminated and forgot the runner,
                # but deliberately retained this attachment identity until the
                # volume itself is deleted.
                'aws_data_volume_attached_instance_id': 'i-terminated',
                'aws_data_volume_id': 'vol-0123456789abcdef0',
                'aws_data_volume_client_token': 'stable-token',
                'aws_data_volume_create_ambiguous': False,
            },
        }

        aws.destroy_resources(
            job,
            aws_session=FakeSession(ec2=ec2),
        )

        ec2.delete_volume.assert_called_once_with(
            VolumeId='vol-0123456789abcdef0'
        )
        self.assertEqual(job['status'], 'destroyed')

    def test_cleanup_refuses_a_volume_attached_to_an_unrecorded_instance(self):
        ec2 = MagicMock()
        ec2.describe_volumes.return_value = {'Volumes': [{
            'VolumeId': 'vol-0123456789abcdef0',
            'Attachments': [{
                'InstanceId': 'i-foreign',
                'State': 'attached',
            }],
            'Tags': aws_tags(),
        }]}
        job = {
            'id': 'job123',
            'status': 'interrupted',
            'plan': {'provider': 'aws', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'region': 'us-east-2',
                'aws_data_volume_id': 'vol-0123456789abcdef0',
            },
        }

        with self.assertRaisesRegex(RuntimeError, 'other than the recorded'):
            aws.destroy_resources(
                job,
                aws_session=FakeSession(ec2=ec2),
            )

        ec2.delete_volume.assert_not_called()


class AwsStorageGuestTests(unittest.TestCase):
    def test_storage_installation_includes_xfs_tools_before_mount(self):
        steps = main.amazon_linux.installation_steps(
            {'fio'},
            additional_volume=True,
        )

        self.assertTrue(steps)
        self.assertIn('fio', steps[0].command)
        self.assertIn('xfsprogs', steps[0].command)

    def test_mount_command_matches_only_the_recorded_ebs_serial(self):
        command = main.aws_data_volume_mount_command(
            'vol-0123456789abcdef0',
            'nitro',
        )

        self.assertIn('EXPECTED_SERIAL="${EXPECTED_VOLUME_ID//-/}"', command)
        self.assertIn('lsblk -dnro SERIAL', command)
        self.assertIn('refusing to select an arbitrary local disk', command)
        self.assertNotIn('if ! lsblk -npo MOUNTPOINT', command)
        self.assertIn('UUID=$FILESYSTEM_UUID /data', command)
        self.assertIn('defaults,nofail', command)
        with self.assertRaisesRegex(ValueError, 'volume ID is invalid'):
            main.aws_data_volume_mount_command(
                '$(touch /tmp/unsafe)',
                'nitro',
            )

    def test_xen_mount_uses_only_the_explicit_non_root_whole_disk_mapping(self):
        command = main.aws_data_volume_mount_command(
            'vol-0123456789abcdef0',
            'xen',
        )

        self.assertIn('HYPERVISOR=xen', command)
        self.assertIn('for alias in /dev/xvdf /dev/sdf', command)
        self.assertIn('test "$device" != "$ROOT_DEVICE"', command)
        self.assertIn('lsblk -dnro TYPE "$device"', command)
        self.assertIn('test -z "$DATA_DEVICE" && test "$HYPERVISOR" = xen', command)
        self.assertNotIn('CANDIDATES', command)

    def test_aws_storage_workloads_mount_and_use_strict_parsers(self):
        plan = storage_plan()
        job = {
            'id': 'job123',
            'events': [],
            'results': [],
            'resources': {
                'provider': 'aws',
                'public_ip': '198.51.100.10',
                'aws_data_volume_id': 'vol-0123456789abcdef0',
                'aws_data_volume_type': 'gp3',
                'aws_data_volume_size_gb': 250,
                'aws_data_volume_iops': 3000,
                'aws_data_volume_throughput_mibps': 125,
                'hypervisor': 'nitro',
            },
        }
        with (
            patch.object(main, 'ssh', return_value='ready'),
            patch.object(main, 'mount_data_volume') as mount,
            patch.object(main, 'execute_benchmark') as execute,
            patch.object(main, 'run_phoronix_profiles', return_value=()),
        ):
            main.run_aws_benchmarks(job, plan)

        mount.assert_called_once_with(job)
        calls = {
            item.args[1]: item
            for item in execute.call_args_list
        }
        self.assertEqual(set(calls), {'fio', 'sysbench_fileio'})
        self.assertIs(
            calls['fio'].kwargs['parser'],
            main.amazon_linux.parse_fio_output,
        )
        self.assertIs(
            calls['sysbench_fileio'].kwargs['parser'],
            main.amazon_linux.parse_sysbench_fileio_output,
        )
        self.assertEqual(
            calls['fio'].kwargs['metadata']['data_volume_iops'],
            3000,
        )
        self.assertIsNone(calls['fio'].kwargs['output_limit'])


if __name__ == '__main__':
    unittest.main()
