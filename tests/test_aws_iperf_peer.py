import copy
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch

from app import main
from app.providers import aws
from tests.test_aws_integration import aws_plan
from tests.test_aws_provider import FakeSession, ownership_tags


def peer_plan(protocols=('tcp', 'udp')):
    return SimpleNamespace(
        provider='aws',
        aws_profile='default',
        region='us-east-2',
        availability_domain='us-east-2a',
        shape='m7i.large',
        ocpus=2,
        memory_gb=8,
        benchmarks=['iperf3'],
        iperf3=SimpleNamespace(protocols=list(protocols)),
        storage=SimpleNamespace(
            boot_size_gb=100,
            additional_volume=False,
        ),
    )


class AwsIperfPeerProvisionTests(unittest.TestCase):
    def setUp(self):
        self.ec2 = MagicMock()
        self.sts = MagicMock()
        self.ssm = MagicMock()
        self.session = FakeSession(
            ec2=self.ec2,
            sts=self.sts,
            ssm=self.ssm,
        )
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
        self.ec2.create_security_group.side_effect = [
            {'GroupId': 'sg-runner'},
            {'GroupId': 'sg-peer'},
        ]
        self.ec2.import_key_pair.return_value = {
            'KeyPairId': 'key-1',
            'KeyName': 'benchmark-peerjob',
        }
        self.ec2.describe_instance_type_offerings.return_value = {
            'InstanceTypeOfferings': [{
                'InstanceType': 'm7i.large',
                'Location': 'us-east-2a',
            }]
        }
        self.ec2.describe_availability_zones.return_value = {
            'AvailabilityZones': [{
                'ZoneName': 'us-east-2a',
                'State': 'available',
            }]
        }
        self.ec2.describe_instance_types.return_value = {'InstanceTypes': [{
            'InstanceType': 'm7i.large',
            'ProcessorInfo': {'SupportedArchitectures': ['x86_64']},
            'VCpuInfo': {'DefaultVCpus': 2},
            'MemoryInfo': {'SizeInMiB': 8192},
        }]}
        self.ssm.get_parameter.return_value = {
            'Parameter': {'Value': 'ami-123', 'Version': 1}
        }
        self.ec2.describe_images.return_value = {'Images': [{
            'ImageId': 'ami-123',
            'Name': 'al2023-ami-test',
            'Architecture': 'x86_64',
            'State': 'available',
            'RootDeviceName': '/dev/xvda',
        }]}
        self.ec2.run_instances.side_effect = [
            {'Instances': [{'InstanceId': 'i-runner'}]},
            {'Instances': [{'InstanceId': 'i-peer'}]},
        ]
        self.ec2.describe_instances.side_effect = [
            {'Reservations': [{'Instances': [{
                'InstanceId': 'i-runner',
                'PublicIpAddress': '198.51.100.10',
                'PrivateIpAddress': '10.42.1.10',
            }]}]},
            {'Reservations': [{'Instances': [{
                'InstanceId': 'i-peer',
                'PublicIpAddress': '198.51.100.11',
                'PrivateIpAddress': '10.42.1.11',
            }]}]},
        ]

    def test_peer_is_same_type_ami_az_subnet_and_private_sg_path(self):
        job = {'id': 'peerjob', 'resources': {}}
        snapshots = []

        resources = aws.provision(
            job,
            peer_plan(),
            public_key='ssh-ed25519 AAAATEST tester',
            aws_session=self.session,
            persist=lambda value: snapshots.append(
                copy.deepcopy(value['resources'])
            ),
        )

        self.assertEqual(resources['aws_peer_instance_id'], 'i-peer')
        self.assertEqual(resources['peer_private_ip'], '10.42.1.11')
        self.assertEqual(resources['aws_peer_instance_type'], 'm7i.large')
        self.assertEqual(resources['aws_peer_image_id'], 'ami-123')
        self.assertEqual(resources['aws_peer_security_group_id'], 'sg-peer')
        launches = self.ec2.run_instances.call_args_list
        runner = launches[0].kwargs
        peer = launches[1].kwargs
        for key in ('ImageId', 'InstanceType'):
            self.assertEqual(peer[key], runner[key])
        self.assertEqual(
            peer['NetworkInterfaces'][0]['SubnetId'],
            runner['NetworkInterfaces'][0]['SubnetId'],
        )
        self.assertEqual(peer['NetworkInterfaces'][0]['Groups'], ['sg-peer'])
        self.assertTrue(peer['BlockDeviceMappings'][0]['Ebs']['Encrypted'])
        self.assertNotIn('KmsKeyId', peer['BlockDeviceMappings'][0]['Ebs'])
        self.assertNotIn('KeyName', peer)
        self.assertIn('Description=AWS benchmark iperf3 server', peer['UserData'])
        self.assertIn('dnf -y', peer['UserData'])
        self.assertNotIn('ol9_', peer['UserData'])
        self.assertIn('5201/udp', peer['UserData'])

        peer_ingress = self.ec2.authorize_security_group_ingress.call_args_list[
            1
        ].kwargs
        self.assertEqual(peer_ingress['GroupId'], 'sg-peer')
        self.assertEqual(
            [item['IpProtocol'] for item in peer_ingress['IpPermissions']],
            ['tcp', 'udp'],
        )
        for permission in peer_ingress['IpPermissions']:
            self.assertNotIn('IpRanges', permission)
            self.assertEqual(
                permission['UserIdGroupPairs'][0]['GroupId'],
                'sg-runner',
            )
        self.assertTrue(any(
            item.get('aws_peer_instance_client_token')
            and not item.get('aws_peer_instance_id')
            for item in snapshots
        ))

    def test_tcp_only_peer_has_no_udp_ingress_or_firewall_rule(self):
        aws.provision(
            {'id': 'peerjob', 'resources': {}},
            peer_plan(('tcp',)),
            public_key='ssh-ed25519 AAAATEST tester',
            aws_session=self.session,
        )

        peer_ingress = self.ec2.authorize_security_group_ingress.call_args_list[
            1
        ].kwargs['IpPermissions']
        self.assertEqual([item['IpProtocol'] for item in peer_ingress], ['tcp'])
        peer_launch = self.ec2.run_instances.call_args_list[1].kwargs
        self.assertNotIn('5201/udp', peer_launch['UserData'])

    def test_sctp_peer_uses_protocol_132_without_fake_port_bounds(self):
        aws.provision(
            {'id': 'peerjob', 'resources': {}},
            peer_plan(('sctp',)),
            public_key='ssh-ed25519 AAAATEST tester',
            aws_session=self.session,
        )

        permissions = self.ec2.authorize_security_group_ingress.call_args_list[
            1
        ].kwargs['IpPermissions']
        self.assertEqual(
            [permission['IpProtocol'] for permission in permissions],
            ['tcp', '132'],
        )
        sctp = permissions[1]
        self.assertNotIn('FromPort', sctp)
        self.assertNotIn('ToPort', sctp)
        self.assertNotIn('IpRanges', sctp)
        self.assertEqual(
            sctp['UserIdGroupPairs'][0]['GroupId'],
            'sg-runner',
        )
        peer_launch = self.ec2.run_instances.call_args_list[1].kwargs
        self.assertIn('5201/sctp', peer_launch['UserData'])


class AwsIperfPeerCleanupTests(unittest.TestCase):
    def test_cleanup_fails_closed_when_peer_role_tag_is_missing(self):
        ec2 = MagicMock()
        ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-peer',
                'Tags': ownership_tags('peerjob'),
            }],
        }]}

        with self.assertRaisesRegex(RuntimeError, 'ownership and role tags'):
            aws._verify_cleanup_ownership(
                ec2,
                'peerjob',
                {'aws_peer_instance_id': 'i-peer'},
            )

    def test_response_lost_peer_security_group_is_reconciled_by_role_tags(self):
        ec2 = MagicMock()
        ec2.describe_security_groups.return_value = {'SecurityGroups': [{
            'GroupId': 'sg-peer-recovered',
            'VpcId': 'vpc-1',
        }]}
        job = {
            'id': 'peerjob',
            'plan': {'provider': 'aws', 'benchmarks': ['iperf3']},
            'resources': {
                'aws_vpc_id': 'vpc-1',
                'aws_internet_gateway_id': 'igw-1',
                'aws_subnet_id': 'subnet-1',
                'aws_route_table_id': 'rtb-1',
                'aws_security_group_id': 'sg-runner',
                'aws_key_pair_id': 'key-1',
            },
        }

        recovered = aws._reconcile_cleanup_manifest(ec2, job, None)

        self.assertEqual(
            recovered['aws_peer_security_group_id'],
            'sg-peer-recovered',
        )
        self.assertEqual(
            job['resources']['aws_peer_security_group_id'],
            'sg-peer-recovered',
        )
        filters = ec2.describe_security_groups.call_args.kwargs['Filters']
        self.assertIn(
            {'Name': 'tag:benchmark-role', 'Values': ['iperf-peer']},
            filters,
        )
        self.assertIn(
            {'Name': 'tag:benchmark-job', 'Values': ['peerjob']},
            filters,
        )
        self.assertIn(
            {'Name': 'tag:managed-by', 'Values': [aws.MANAGED_BY]},
            filters,
        )

    def test_response_lost_peer_launch_is_reconciled_by_exact_client_token(self):
        ec2 = MagicMock()
        ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-peer-recovered',
                'VpcId': 'vpc-1',
                'SubnetId': 'subnet-1',
            }],
        }]}
        session = FakeSession(ec2=ec2)
        job = {
            'id': 'peerjob',
            'status': 'provisioning',
            'plan': {
                'provider': 'aws',
                'benchmarks': ['iperf3'],
                'region': 'us-east-2',
            },
            'resources': {
                'provider': 'aws',
                'region': 'us-east-2',
                'aws_peer_instance_client_token': 'peer-token',
            },
        }
        snapshots = []

        with patch.object(aws, '_reconcile_cleanup_manifest'):
            aws.destroy_resources(
                job,
                aws_session=session,
                persist=lambda value: snapshots.append(
                    copy.deepcopy(value['resources'])
                ),
            )

        lookup = ec2.describe_instances.call_args_list[0].kwargs
        self.assertEqual(lookup['Filters'], [
            {'Name': 'client-token', 'Values': ['peer-token']},
            {'Name': 'tag:benchmark-job', 'Values': ['peerjob']},
            {'Name': 'tag:managed-by', 'Values': [aws.MANAGED_BY]},
            {'Name': 'tag:benchmark-role', 'Values': ['iperf-peer']},
        ])
        self.assertTrue(any(
            item.get('aws_peer_instance_id') == 'i-peer-recovered'
            for item in snapshots
        ))
        ec2.terminate_instances.assert_called_once_with(
            InstanceIds=['i-peer-recovered']
        )
        self.assertNotIn('aws_peer_instance_id', job['resources'])
        self.assertNotIn('aws_peer_instance_client_token', job['resources'])

    def test_cleanup_verifies_and_removes_peer_before_network(self):
        ec2 = MagicMock()
        tags = ownership_tags('peerjob')

        def describe_instances(**kwargs):
            instance_id = kwargs['InstanceIds'][0]
            instance_tags = list(tags)
            if instance_id == 'i-peer':
                instance_tags.append({
                    'Key': 'benchmark-role',
                    'Value': 'iperf-peer',
                })
            return {'Reservations': [{'Instances': [{
                'InstanceId': instance_id,
                'VpcId': 'vpc-1',
                'SubnetId': 'subnet-1',
                'Tags': instance_tags,
            }]}]}

        def describe_groups(**kwargs):
            group_id = kwargs['GroupIds'][0]
            group_tags = list(tags)
            if group_id == 'sg-peer':
                group_tags.append({
                    'Key': 'benchmark-role',
                    'Value': 'iperf-peer',
                })
            return {'SecurityGroups': [{
                'GroupId': group_id,
                'VpcId': 'vpc-1',
                'Tags': group_tags,
            }]}

        ec2.describe_instances.side_effect = describe_instances
        ec2.describe_security_groups.side_effect = describe_groups
        ec2.describe_key_pairs.return_value = {'KeyPairs': [{
            'KeyPairId': 'key-1',
            'Tags': tags,
        }]}
        ec2.describe_subnets.return_value = {'Subnets': [{
            'SubnetId': 'subnet-1',
            'VpcId': 'vpc-1',
            'Tags': tags,
        }]}
        ec2.describe_route_tables.return_value = {'RouteTables': [{
            'RouteTableId': 'rtb-1',
            'VpcId': 'vpc-1',
            'Tags': tags,
            'Associations': [{
                'RouteTableAssociationId': 'rtbassoc-1',
                'SubnetId': 'subnet-1',
            }],
        }]}
        ec2.describe_internet_gateways.return_value = {
            'InternetGateways': [{
                'InternetGatewayId': 'igw-1',
                'Attachments': [{'VpcId': 'vpc-1'}],
                'Tags': tags,
            }],
        }
        ec2.describe_vpcs.return_value = {'Vpcs': [{
            'VpcId': 'vpc-1',
            'Tags': tags,
        }]}
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        session = FakeSession(ec2=ec2, sts=sts)
        job = {
            'id': 'peerjob',
            'status': 'complete',
            'plan': {
                'provider': 'aws',
                'aws_profile': 'default',
                'region': 'us-east-2',
                'benchmarks': ['iperf3'],
            },
            'resources': {
                'provider': 'aws',
                'aws_account_id': '123456789012',
                'region': 'us-east-2',
                'aws_instance_id': 'i-runner',
                'aws_instance_client_token': 'runner-token',
                'instance_id': 'i-runner',
                'public_ip': '198.51.100.10',
                'private_ip': '10.42.1.10',
                'aws_peer_instance_id': 'i-peer',
                'aws_peer_instance_client_token': 'peer-token',
                'peer_private_ip': '10.42.1.11',
                'aws_peer_security_group_id': 'sg-peer',
                'aws_security_group_id': 'sg-runner',
                'aws_key_pair_id': 'key-1',
                'aws_key_pair_name': 'benchmark-peerjob',
                'aws_route_table_association_id': 'rtbassoc-1',
                'aws_subnet_id': 'subnet-1',
                'aws_route_table_id': 'rtb-1',
                'aws_internet_gateway_id': 'igw-1',
                'aws_vpc_id': 'vpc-1',
            },
        }

        aws.destroy_resources(job, aws_session=session)

        self.assertEqual(job['status'], 'destroyed')
        self.assertNotIn('aws_peer_instance_id', job['resources'])
        self.assertNotIn('aws_peer_instance_client_token', job['resources'])
        self.assertNotIn('peer_private_ip', job['resources'])
        terminations = [
            item.kwargs['InstanceIds']
            for item in ec2.terminate_instances.call_args_list
        ]
        self.assertEqual(terminations, [['i-peer'], ['i-runner']])
        group_deletes = [
            item.kwargs['GroupId']
            for item in ec2.delete_security_group.call_args_list
        ]
        self.assertEqual(group_deletes, ['sg-peer', 'sg-runner'])
        names = [item[0] for item in ec2.method_calls]
        self.assertLess(
            names.index('terminate_instances'),
            names.index('delete_subnet'),
        )


class AwsIperfPeerExecutionTests(unittest.TestCase):
    def test_aws_readiness_verifies_control_tcp_and_udp_data_paths(self):
        command = main.iperf_peer_readiness_command(
            '10.42.1.11',
            ('tcp', 'udp'),
            verify_tcp_udp=True,
        )

        self.assertIn('/dev/tcp/$PEER_IP/5201', command)
        self.assertIn('iperf3 -4 -c "$PEER_IP" -t 1 -J', command)
        self.assertIn(
            'iperf3 -4 -c "$PEER_IP" -u -b 10M -t 1 -J',
            command,
        )
        self.assertIn('iperf3 TCP data path is ready', command)
        self.assertIn('iperf3 UDP data path is ready', command)
        self.assertIn(
            'if [ "$attempt" -lt 60 ]; then sleep 5; fi',
            command,
        )
        self.assertEqual(
            command.count(
                'if [ "$attempt" -lt 6 ]; then sleep 2; fi'
            ),
            2,
        )
        with tempfile.NamedTemporaryFile(mode='w') as file:
            file.write(f'#!/bin/bash\n{command}\n')
            file.flush()
            subprocess.run(
                ['bash', '-n', file.name],
                check=True,
                capture_output=True,
                text=True,
            )

    def test_aws_readiness_ssh_timeout_covers_all_bounded_probes(self):
        job = {
            'events': [],
            'resources': {'peer_private_ip': '10.42.1.11'},
        }

        with patch.object(main, 'ssh', return_value='peer ready') as ssh:
            main.wait_for_iperf_peer(
                job,
                ('tcp', 'udp'),
                verify_tcp_udp=True,
            )

        self.assertEqual(
            ssh.call_args.kwargs['timeout'],
            main.IPERF_PEER_READINESS_TIMEOUT_SECONDS,
        )
        self.assertEqual(main.IPERF_PEER_READINESS_TIMEOUT_SECONDS, 720)

    def test_aws_runner_waits_then_runs_parsed_tcp_and_udp_over_private_ip(self):
        plan = aws_plan(
            benchmarks=['iperf3'],
            iperf3={'protocols': ['tcp', 'udp']},
        )
        job = {
            'id': 'peer-run',
            'events': [],
            'results': [],
            'resources': {
                'provider': 'aws',
                'public_ip': '198.51.100.10',
                'peer_private_ip': '10.42.1.11',
                'ssh_user': 'ec2-user',
                'architecture': 'x86_64',
                'image_id': 'ami-123',
                'aws_peer_instance_type': 't3.micro',
                'aws_peer_image_id': 'ami-123',
            },
        }

        with (
            patch.object(main, 'ssh', return_value='ready') as ssh,
            patch.object(main, 'wait_for_iperf_peer') as wait,
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_aws_benchmarks(job, plan)

        wait.assert_called_once_with(
            job,
            ('tcp', 'udp'),
            verify_tcp_udp=True,
        )
        install_commands = [item.args[1] for item in ssh.call_args_list]
        self.assertTrue(any('install iperf3' in item for item in install_commands))
        self.assertEqual(
            [item.args[1] for item in execute.call_args_list],
            ['iperf_tcp', 'iperf_udp'],
        )
        self.assertEqual(
            [item.args[3] for item in execute.call_args_list],
            [
                'iperf3 -4 -c 10.42.1.11 -t 60 -P 4 -J',
                'iperf3 -4 -c 10.42.1.11 -u -b 0 -t 60 -J',
            ],
        )
        for call in execute.call_args_list:
            self.assertTrue(callable(call.kwargs['parser']))
            self.assertIsNone(call.kwargs['output_limit'])
            self.assertEqual(
                call.kwargs['metadata']['traffic_path'],
                'AWS private VPC address',
            )
            self.assertEqual(
                call.kwargs['metadata']['iperf3_peer_instance_type'],
                't3.micro',
            )


if __name__ == '__main__':
    unittest.main()
