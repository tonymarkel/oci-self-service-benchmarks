import copy
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.providers import aws
from tests.test_aws_provider import FakeSession, ownership_tags


def web_plan(benchmarks=('apachebench',)):
    return SimpleNamespace(
        provider='aws',
        aws_profile='default',
        region='us-east-2',
        availability_domain='us-east-2a',
        shape='m7g.xlarge',
        ocpus=4,
        memory_gb=16,
        benchmarks=list(benchmarks),
        storage=SimpleNamespace(
            boot_size_gb=100,
            additional_volume=False,
        ),
    )


def instance_type(
    name,
    *,
    architecture='x86_64',
    vcpus=2,
    memory_mib=8192,
    baseline_gbps=1.25,
    peak_gbps=12.5,
):
    item = {
        'InstanceType': name,
        'ProcessorInfo': {'SupportedArchitectures': [architecture]},
        'VCpuInfo': {'DefaultVCpus': vcpus},
        'MemoryInfo': {'SizeInMiB': memory_mib},
    }
    if baseline_gbps is not None or peak_gbps is not None:
        item['NetworkInfo'] = {'NetworkCards': [{
            'NetworkCardIndex': 0,
            'BaselineBandwidthInGbps': baseline_gbps,
            'PeakBandwidthInGbps': peak_gbps,
        }]}
    return item


class AwsWebLoadGeneratorSelectionTests(unittest.TestCase):
    def test_priority_selection_requires_same_az_and_exact_capacity(self):
        ec2 = MagicMock()
        ec2.describe_instance_type_offerings.return_value = {
            'InstanceTypeOfferings': [
                {'InstanceType': 'm5.large', 'Location': 'us-east-2a'},
                {'InstanceType': 'm7i.large', 'Location': 'us-east-2a'},
                # An otherwise preferred type in another AZ is not eligible.
                {'InstanceType': 'm7i.large', 'Location': 'us-east-2b'},
            ]
        }
        ec2.describe_instance_types.return_value = {'InstanceTypes': [
            instance_type('m5.large', baseline_gbps=0.75, peak_gbps=10),
            instance_type('m7i.large', baseline_gbps=2.5, peak_gbps=12.5),
        ]}

        details = aws._load_generator_type_details(ec2, 'us-east-2a')

        self.assertEqual(details, {
            'instance_type': 'm7i.large',
            'architecture': 'x86_64',
            'vcpu': 2,
            'memory_gb': 8,
            'network_baseline_gbps': 2.5,
            'network_peak_gbps': 12.5,
        })
        ec2.describe_instance_type_offerings.assert_called_once_with(
            LocationType='availability-zone',
            Filters=[
                {
                    'Name': 'instance-type',
                    'Values': list(aws.LOAD_GENERATOR_INSTANCE_TYPES),
                },
                {'Name': 'location', 'Values': ['us-east-2a']},
            ],
        )

    def test_selection_skips_an_offered_candidate_with_wrong_capacity(self):
        ec2 = MagicMock()
        ec2.describe_instance_type_offerings.return_value = {
            'InstanceTypeOfferings': [
                {'InstanceType': 'm7i.large', 'Location': 'us-east-2a'},
                {'InstanceType': 'm6i.large', 'Location': 'us-east-2a'},
            ]
        }
        ec2.describe_instance_types.return_value = {'InstanceTypes': [
            instance_type('m7i.large', memory_mib=4096),
            instance_type('m6i.large', baseline_gbps=1, peak_gbps=10),
        ]}

        details = aws._load_generator_type_details(ec2, 'us-east-2a')

        self.assertEqual(details['instance_type'], 'm6i.large')

    def test_selection_fails_before_provisioning_without_network_capacity(self):
        ec2 = MagicMock()
        ec2.describe_instance_type_offerings.return_value = {
            'InstanceTypeOfferings': [{
                'InstanceType': 'm7i.large',
                'Location': 'us-east-2a',
            }]
        }
        ec2.describe_instance_types.return_value = {'InstanceTypes': [
            instance_type(
                'm7i.large',
                baseline_gbps=None,
                peak_gbps=None,
            ),
        ]}

        with self.assertRaisesRegex(RuntimeError, 'network-capacity metadata'):
            aws._load_generator_type_details(ec2, 'us-east-2a')


class AwsWebLoadGeneratorProvisionTests(unittest.TestCase):
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
            {'GroupId': 'sg-loadgen'},
        ]
        self.ec2.import_key_pair.return_value = {
            'KeyPairId': 'key-1',
            'KeyName': 'benchmark-webjob',
        }
        self.ec2.describe_availability_zones.return_value = {
            'AvailabilityZones': [{
                'ZoneName': 'us-east-2a',
                'State': 'available',
            }]
        }

        def offerings(**kwargs):
            requested = kwargs['Filters'][0]['Values']
            if requested == ['m7g.xlarge']:
                return {'InstanceTypeOfferings': [{
                    'InstanceType': 'm7g.xlarge',
                    'Location': 'us-east-2a',
                }]}
            return {'InstanceTypeOfferings': [
                {'InstanceType': 'm6i.large', 'Location': 'us-east-2a'},
                {'InstanceType': 'm7i.large', 'Location': 'us-east-2a'},
            ]}

        def types(**kwargs):
            requested = kwargs['InstanceTypes']
            if requested == ['m7g.xlarge']:
                return {'InstanceTypes': [instance_type(
                    'm7g.xlarge',
                    architecture='arm64',
                    vcpus=4,
                    memory_mib=16384,
                )]}
            return {'InstanceTypes': [
                instance_type(
                    'm7i.large',
                    baseline_gbps=2.5,
                    peak_gbps=12.5,
                ),
                instance_type(
                    'm6i.large',
                    baseline_gbps=1.25,
                    peak_gbps=12.5,
                ),
            ]}

        self.ec2.describe_instance_type_offerings.side_effect = offerings
        self.ec2.describe_instance_types.side_effect = types

        def parameter(**kwargs):
            architecture = (
                'arm64' if kwargs['Name'].endswith('-arm64') else 'x86_64'
            )
            return {'Parameter': {
                'Value': f'ami-{architecture}',
                'Version': 1,
            }}

        def images(**kwargs):
            image_id = kwargs['ImageIds'][0]
            architecture = 'arm64' if image_id.endswith('arm64') else 'x86_64'
            return {'Images': [{
                'ImageId': image_id,
                'Name': f'al2023-{architecture}',
                'Architecture': architecture,
                'State': 'available',
                'RootDeviceName': '/dev/xvda',
            }]}

        self.ssm.get_parameter.side_effect = parameter
        self.ec2.describe_images.side_effect = images
        self.ec2.run_instances.side_effect = [
            {'Instances': [{'InstanceId': 'i-runner'}]},
            {'Instances': [{'InstanceId': 'i-loadgen'}]},
        ]
        self.ec2.describe_instances.side_effect = [
            {'Reservations': [{'Instances': [{
                'InstanceId': 'i-runner',
                'PublicIpAddress': '198.51.100.10',
                'PrivateIpAddress': '10.42.1.10',
            }]}]},
            {'Reservations': [{'Instances': [{
                'InstanceId': 'i-loadgen',
                'PublicIpAddress': '198.51.100.20',
                'PrivateIpAddress': '10.42.1.20',
            }]}]},
        ]

    def provision(self, benchmarks=('apachebench',), persist=None):
        job = {'id': 'webjob', 'resources': {}}
        resources = aws.provision(
            job,
            web_plan(benchmarks),
            public_key='ssh-ed25519 AAAATEST tester',
            aws_session=self.session,
            persist=persist,
        )
        return job, resources

    def test_apachebench_uses_dedicated_x86_loadgen_and_private_port_80(self):
        _, resources = self.provision()

        self.assertEqual(resources['aws_loadgen_instance_id'], 'i-loadgen')
        self.assertEqual(resources['loadgen_instance_id'], 'i-loadgen')
        self.assertEqual(resources['loadgen_shape'], 'm7i.large')
        self.assertEqual(resources['loadgen_image_id'], 'ami-x86_64')
        self.assertEqual(resources['loadgen_architecture'], 'x86_64')
        self.assertEqual(resources['loadgen_vcpus'], 2)
        self.assertEqual(resources['loadgen_memory_gb'], 8)
        self.assertEqual(resources['loadgen_network_bandwidth_gbps'], 2.5)
        self.assertEqual(
            resources['loadgen_network_peak_bandwidth_gbps'],
            12.5,
        )
        self.assertEqual(resources['loadgen_public_ip'], '198.51.100.20')
        self.assertEqual(resources['loadgen_private_ip'], '10.42.1.20')

        runner, loadgen = [
            call.kwargs for call in self.ec2.run_instances.call_args_list
        ]
        self.assertEqual(runner['InstanceType'], 'm7g.xlarge')
        self.assertEqual(runner['ImageId'], 'ami-arm64')
        self.assertEqual(loadgen['InstanceType'], 'm7i.large')
        self.assertEqual(loadgen['ImageId'], 'ami-x86_64')
        self.assertEqual(loadgen['KeyName'], 'benchmark-webjob')
        self.assertEqual(loadgen['NetworkInterfaces'][0]['SubnetId'], 'subnet-1')
        self.assertEqual(loadgen['NetworkInterfaces'][0]['Groups'], ['sg-loadgen'])
        self.assertEqual(
            loadgen['BlockDeviceMappings'][0]['Ebs']['VolumeSize'],
            aws.LOAD_GENERATOR_BOOT_SIZE_GB,
        )
        loadgen_tags = loadgen['TagSpecifications'][0]['Tags']
        self.assertIn(
            {'Key': 'benchmark-role', 'Value': 'load-generator'},
            loadgen_tags,
        )

        loadgen_ssh = self.ec2.authorize_security_group_ingress.call_args_list[
            1
        ].kwargs
        self.assertEqual(loadgen_ssh['GroupId'], 'sg-loadgen')
        self.assertEqual(
            loadgen_ssh['IpPermissions'][0]['IpRanges'][0]['CidrIp'],
            aws.SSH_SOURCE_CIDR,
        )
        web_ingress = self.ec2.authorize_security_group_ingress.call_args_list[
            2
        ].kwargs
        self.assertEqual(web_ingress['GroupId'], 'sg-runner')
        self.assertEqual(
            [permission['FromPort'] for permission in web_ingress['IpPermissions']],
            [80],
        )
        permission = web_ingress['IpPermissions'][0]
        self.assertNotIn('IpRanges', permission)
        self.assertEqual(
            permission['UserIdGroupPairs'][0]['GroupId'],
            'sg-loadgen',
        )

    def test_deathstarbench_opens_only_private_ports_5000_and_8080(self):
        self.provision(('deathstarbench',))

        web_ingress = self.ec2.authorize_security_group_ingress.call_args_list[
            2
        ].kwargs['IpPermissions']
        self.assertEqual(
            [permission['FromPort'] for permission in web_ingress],
            [5000, 8080],
        )
        for permission in web_ingress:
            self.assertNotIn('IpRanges', permission)
            self.assertEqual(
                permission['UserIdGroupPairs'][0]['GroupId'],
                'sg-loadgen',
            )

    def test_combined_web_workloads_share_one_loadgen_and_all_three_rules(self):
        self.provision(('apachebench', 'deathstarbench'))

        self.assertEqual(self.ec2.run_instances.call_count, 2)
        self.assertEqual(self.ec2.create_security_group.call_count, 2)
        web_ingress = self.ec2.authorize_security_group_ingress.call_args_list[
            2
        ].kwargs['IpPermissions']
        self.assertEqual(
            [permission['FromPort'] for permission in web_ingress],
            [80, 5000, 8080],
        )

    def test_non_web_plan_does_not_select_or_launch_a_load_generator(self):
        self.ec2.create_security_group.side_effect = None
        self.ec2.create_security_group.return_value = {'GroupId': 'sg-runner'}
        self.ec2.run_instances.side_effect = None
        self.ec2.run_instances.return_value = {
            'Instances': [{'InstanceId': 'i-runner'}]
        }
        self.ec2.describe_instances.side_effect = None
        self.ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-runner',
                'PublicIpAddress': '198.51.100.10',
                'PrivateIpAddress': '10.42.1.10',
            }],
        }]}

        with patch.object(aws, '_load_generator_type_details') as select:
            _, resources = self.provision(('stream',))

        select.assert_not_called()
        self.assertEqual(self.ec2.run_instances.call_count, 1)
        self.assertNotIn('aws_loadgen_instance_id', resources)
        self.assertNotIn('loadgen_public_ip', resources)

    def test_lost_loadgen_launch_response_keeps_exact_recovery_token(self):
        self.ec2.run_instances.side_effect = [
            {'Instances': [{'InstanceId': 'i-runner'}]},
            TimeoutError('loadgen response lost'),
        ]
        job = {'id': 'webjob', 'resources': {}}
        snapshots = []

        with self.assertRaisesRegex(TimeoutError, 'loadgen response lost'):
            aws.provision(
                job,
                web_plan(),
                public_key='ssh-ed25519 AAAATEST tester',
                aws_session=self.session,
                persist=lambda value: snapshots.append(
                    copy.deepcopy(value['resources'])
                ),
            )

        token = self.ec2.run_instances.call_args_list[1].kwargs['ClientToken']
        self.assertEqual(
            job['resources']['aws_loadgen_instance_client_token'],
            token,
        )
        self.assertNotIn('aws_loadgen_instance_id', job['resources'])
        self.assertTrue(any(
            snapshot.get('aws_loadgen_instance_client_token') == token
            and not snapshot.get('aws_loadgen_instance_id')
            for snapshot in snapshots
        ))


class AwsWebLoadGeneratorCleanupTests(unittest.TestCase):
    def test_response_lost_security_group_is_reconciled_by_role_tags(self):
        ec2 = MagicMock()
        ec2.describe_security_groups.return_value = {'SecurityGroups': [{
            'GroupId': 'sg-loadgen-recovered',
            'GroupName': 'benchmark-loadgen-webjob',
            'VpcId': 'vpc-1',
            'Tags': [
                *ownership_tags('webjob'),
                {'Key': 'benchmark-role', 'Value': 'load-generator'},
            ],
        }]}
        job = {
            'id': 'webjob',
            'plan': {'provider': 'aws', 'benchmarks': ['apachebench']},
            'resources': {
                'aws_vpc_id': 'vpc-1',
                'aws_internet_gateway_id': 'igw-1',
                'aws_subnet_id': 'subnet-1',
                'aws_route_table_id': 'rtb-1',
                'aws_security_group_id': 'sg-runner',
                'aws_key_pair_id': 'key-1',
                'aws_loadgen_security_group_create_ambiguous': True,
            },
        }

        recovered = aws._reconcile_cleanup_manifest(ec2, job, None)

        self.assertEqual(
            recovered['aws_loadgen_security_group_id'],
            'sg-loadgen-recovered',
        )
        self.assertFalse(
            job['resources']['aws_loadgen_security_group_create_ambiguous']
        )
        self.assertIsNone(
            job['resources']['aws_loadgen_security_group_reconciliation_error']
        )
        filters = ec2.describe_security_groups.call_args.kwargs['Filters']
        self.assertIn(
            {
                'Name': 'group-name',
                'Values': ['benchmark-loadgen-webjob'],
            },
            filters,
        )
        self.assertIn(
            {'Name': 'tag:benchmark-role', 'Values': ['load-generator']},
            filters,
        )
        self.assertIn(
            {'Name': 'tag:benchmark-job', 'Values': ['webjob']},
            filters,
        )
        self.assertIn(
            {'Name': 'tag:managed-by', 'Values': [aws.MANAGED_BY]},
            filters,
        )

    def test_response_lost_security_group_rejects_wrong_exact_name(self):
        ec2 = MagicMock()
        ec2.describe_security_groups.return_value = {'SecurityGroups': [{
            'GroupId': 'sg-loadgen-wrong',
            'GroupName': 'benchmark-loadgen-another-job',
            'VpcId': 'vpc-1',
            'Tags': [
                *ownership_tags('webjob'),
                {'Key': 'benchmark-role', 'Value': 'load-generator'},
            ],
        }]}
        job = {
            'id': 'webjob',
            'plan': {'provider': 'aws', 'benchmarks': ['apachebench']},
            'resources': {
                'aws_vpc_id': 'vpc-1',
                'aws_loadgen_security_group_create_ambiguous': True,
            },
        }

        with self.assertRaisesRegex(RuntimeError, 'deterministic name'):
            aws._reconcile_cleanup_manifest(ec2, job, None)

        self.assertNotIn(
            'aws_loadgen_security_group_id',
            job['resources'],
        )

    def test_lost_loadgen_launch_is_reconciled_by_token_and_role(self):
        ec2 = MagicMock()
        ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-loadgen-recovered',
                'ClientToken': 'loadgen-token',
                'InstanceType': 'm7i.large',
                'ImageId': 'ami-x86',
                'Placement': {'AvailabilityZone': 'us-east-2a'},
                'VpcId': 'vpc-1',
                'SubnetId': 'subnet-1',
                'SecurityGroups': [{'GroupId': 'sg-loadgen'}],
                'Tags': [
                    *ownership_tags('webjob'),
                    {'Key': 'benchmark-role', 'Value': 'load-generator'},
                ],
            }],
        }]}
        session = FakeSession(ec2=ec2)
        job = {
            'id': 'webjob',
            'status': 'provisioning',
            'plan': {
                'provider': 'aws',
                'benchmarks': ['apachebench'],
                'region': 'us-east-2',
            },
            'resources': {
                'provider': 'aws',
                'region': 'us-east-2',
                'aws_loadgen_instance_client_token': 'loadgen-token',
                'aws_loadgen_instance_type': 'm7i.large',
                'aws_loadgen_image_id': 'ami-x86',
                'aws_loadgen_availability_zone': 'us-east-2a',
                'aws_vpc_id': 'vpc-1',
                'aws_subnet_id': 'subnet-1',
                'aws_loadgen_security_group_id': 'sg-loadgen',
                'loadgen_shape': 'm7i.large',
            },
        }

        with patch.object(aws, '_reconcile_cleanup_manifest'):
            aws.destroy_resources(job, aws_session=session)

        lookup = ec2.describe_instances.call_args_list[0].kwargs
        self.assertEqual(lookup['Filters'], [
            {'Name': 'client-token', 'Values': ['loadgen-token']},
            {'Name': 'tag:benchmark-job', 'Values': ['webjob']},
            {'Name': 'tag:managed-by', 'Values': [aws.MANAGED_BY]},
            {'Name': 'tag:benchmark-role', 'Values': ['load-generator']},
        ])
        ec2.terminate_instances.assert_called_once_with(
            InstanceIds=['i-loadgen-recovered']
        )
        self.assertNotIn('aws_loadgen_instance_client_token', job['resources'])
        self.assertNotIn('loadgen_shape', job['resources'])

    def test_lost_loadgen_launch_rejects_any_contract_mismatch(self):
        base_instance = {
            'InstanceId': 'i-loadgen-recovered',
            'ClientToken': 'loadgen-token',
            'InstanceType': 'm7i.large',
            'ImageId': 'ami-x86',
            'Placement': {'AvailabilityZone': 'us-east-2a'},
            'VpcId': 'vpc-1',
            'SubnetId': 'subnet-1',
            'SecurityGroups': [{'GroupId': 'sg-loadgen'}],
            'Tags': [
                *ownership_tags('webjob'),
                {'Key': 'benchmark-role', 'Value': 'load-generator'},
            ],
        }
        mutations = {
            'client token': lambda item: item.update(ClientToken='other-token'),
            'instance type': lambda item: item.update(InstanceType='m6i.large'),
            'AMI': lambda item: item.update(ImageId='ami-other'),
            'Availability Zone': lambda item: item.update(
                Placement={'AvailabilityZone': 'us-east-2b'}
            ),
            'VPC': lambda item: item.update(VpcId='vpc-other'),
            'subnet': lambda item: item.update(SubnetId='subnet-other'),
            'security group': lambda item: item.update(
                SecurityGroups=[{'GroupId': 'sg-other'}]
            ),
            'role tags': lambda item: item.update(
                Tags=ownership_tags('webjob')
            ),
        }
        resources = {
            'provider': 'aws',
            'region': 'us-east-2',
            'aws_loadgen_instance_client_token': 'loadgen-token',
            'aws_loadgen_instance_type': 'm7i.large',
            'aws_loadgen_image_id': 'ami-x86',
            'aws_loadgen_availability_zone': 'us-east-2a',
            'aws_vpc_id': 'vpc-1',
            'aws_subnet_id': 'subnet-1',
            'aws_loadgen_security_group_id': 'sg-loadgen',
        }

        for label, mutate in mutations.items():
            with self.subTest(label=label):
                instance = copy.deepcopy(base_instance)
                mutate(instance)
                ec2 = MagicMock()
                ec2.describe_instances.return_value = {
                    'Reservations': [{'Instances': [instance]}]
                }
                session = FakeSession(ec2=ec2)
                job = {
                    'id': 'webjob',
                    'status': 'provisioning',
                    'plan': {
                        'provider': 'aws',
                        'benchmarks': ['apachebench'],
                        'region': 'us-east-2',
                    },
                    'resources': copy.deepcopy(resources),
                }

                with patch.object(aws, '_reconcile_cleanup_manifest'):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        'ownership and role tags|exact launch contract',
                    ):
                        aws.destroy_resources(job, aws_session=session)

                ec2.terminate_instances.assert_not_called()

    def test_cleanup_clears_prelaunch_loadgen_metadata_without_token(self):
        ec2 = MagicMock()
        session = FakeSession(ec2=ec2)
        job = {
            'id': 'webjob',
            'status': 'provisioning',
            'plan': {
                'provider': 'aws',
                'benchmarks': ['apachebench'],
                'region': 'us-east-2',
            },
            'resources': {
                'provider': 'aws',
                'region': 'us-east-2',
                'aws_loadgen_instance_type': 'm7i.large',
                'aws_loadgen_image_id': 'ami-x86',
                'aws_loadgen_availability_zone': 'us-east-2a',
                'loadgen_shape': 'm7i.large',
                'loadgen_architecture': 'x86_64',
                'loadgen_vcpus': 2,
                'loadgen_memory_gb': 8,
            },
        }

        with patch.object(aws, '_reconcile_cleanup_manifest'):
            aws.destroy_resources(job, aws_session=session)

        self.assertEqual(job['status'], 'destroyed')
        for key in aws.LOAD_GENERATOR_INSTANCE_CONTRACT_KEYS:
            self.assertNotIn(key, job['resources'])
        ec2.terminate_instances.assert_not_called()

    def test_unresolved_retry_keeps_prelaunch_loadgen_metadata(self):
        ec2 = MagicMock()
        session = FakeSession(ec2=ec2)
        job = {
            'id': 'webjob',
            'status': 'provisioning',
            'plan': {
                'provider': 'aws',
                'benchmarks': ['apachebench'],
                'region': 'us-east-2',
            },
            'resources': {
                'provider': 'aws',
                'region': 'us-east-2',
                'aws_loadgen_instance_type': 'm7i.large',
                'loadgen_shape': 'm7i.large',
                'aws_loadgen_security_group_create_ambiguous': True,
                'aws_loadgen_security_group_reconciliation_error': (
                    'retry load-generator SG reconciliation'
                ),
            },
        }

        with patch.object(aws, '_reconcile_cleanup_manifest'):
            with self.assertRaisesRegex(
                RuntimeError,
                'retry load-generator SG reconciliation',
            ):
                aws.destroy_resources(job, aws_session=session)

        self.assertEqual(
            job['resources']['aws_loadgen_instance_type'],
            'm7i.large',
        )
        self.assertEqual(job['resources']['loadgen_shape'], 'm7i.large')

    def test_cleanup_fails_closed_when_loadgen_role_tag_is_missing(self):
        ec2 = MagicMock()
        ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-loadgen',
                'Tags': ownership_tags('webjob'),
            }],
        }]}

        with self.assertRaisesRegex(RuntimeError, 'ownership and role tags'):
            aws._verify_cleanup_ownership(
                ec2,
                'webjob',
                {'aws_loadgen_instance_id': 'i-loadgen'},
            )

    def test_cleanup_fails_closed_when_loadgen_is_in_another_vpc(self):
        ec2 = MagicMock()
        ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-loadgen',
                'VpcId': 'vpc-foreign',
                'SubnetId': 'subnet-1',
                'Tags': [
                    *ownership_tags('webjob'),
                    {'Key': 'benchmark-role', 'Value': 'load-generator'},
                ],
            }],
        }]}

        with self.assertRaisesRegex(RuntimeError, 'recorded VPC and subnet'):
            aws._verify_cleanup_ownership(
                ec2,
                'webjob',
                {
                    'aws_loadgen_instance_id': 'i-loadgen',
                    'aws_vpc_id': 'vpc-1',
                    'aws_subnet_id': 'subnet-1',
                },
            )

    def test_cleanup_terminates_loadgen_before_network_and_deletes_sgs_safely(self):
        ec2 = MagicMock()
        tags = ownership_tags('webjob')

        def describe_instances(**kwargs):
            instance_id = kwargs['InstanceIds'][0]
            instance_tags = list(tags)
            if instance_id == 'i-loadgen':
                instance_tags.append({
                    'Key': 'benchmark-role',
                    'Value': 'load-generator',
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
            if group_id == 'sg-loadgen':
                group_tags.append({
                    'Key': 'benchmark-role',
                    'Value': 'load-generator',
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
            'id': 'webjob',
            'status': 'complete',
            'plan': {
                'provider': 'aws',
                'aws_profile': 'default',
                'region': 'us-east-2',
                'benchmarks': ['apachebench'],
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
                'aws_loadgen_instance_id': 'i-loadgen',
                'aws_loadgen_instance_client_token': 'loadgen-token',
                'loadgen_instance_id': 'i-loadgen',
                'loadgen_public_ip': '198.51.100.20',
                'loadgen_private_ip': '10.42.1.20',
                'loadgen_shape': 'm7i.large',
                'loadgen_network_bandwidth_gbps': 2.5,
                'aws_loadgen_security_group_id': 'sg-loadgen',
                'aws_loadgen_security_group_create_ambiguous': False,
                'aws_security_group_id': 'sg-runner',
                'aws_key_pair_id': 'key-1',
                'aws_key_pair_name': 'benchmark-webjob',
                'aws_route_table_association_id': 'rtbassoc-1',
                'aws_subnet_id': 'subnet-1',
                'aws_route_table_id': 'rtb-1',
                'aws_internet_gateway_id': 'igw-1',
                'aws_vpc_id': 'vpc-1',
            },
        }

        aws.destroy_resources(job, aws_session=session)

        self.assertEqual(job['status'], 'destroyed')
        terminations = [
            call.kwargs['InstanceIds']
            for call in ec2.terminate_instances.call_args_list
        ]
        self.assertEqual(terminations, [['i-loadgen'], ['i-runner']])
        group_deletes = [
            call.kwargs['GroupId']
            for call in ec2.delete_security_group.call_args_list
        ]
        # The runner SG contains the source reference to the load-generator
        # SG, so removing it first releases that dependency.
        self.assertEqual(group_deletes, ['sg-runner', 'sg-loadgen'])
        names = [call[0] for call in ec2.method_calls]
        self.assertLess(
            names.index('terminate_instances'),
            names.index('delete_subnet'),
        )
        for key in aws.LOAD_GENERATOR_INSTANCE_CONTRACT_KEYS:
            self.assertNotIn(key, job['resources'])
        self.assertNotIn('aws_loadgen_security_group_id', job['resources'])

        before = len(ec2.method_calls)
        aws.destroy_resources(job, aws_session=session)
        self.assertEqual(len(ec2.method_calls), before)


if __name__ == '__main__':
    unittest.main()
