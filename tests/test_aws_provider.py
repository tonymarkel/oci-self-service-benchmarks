import copy
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError

from app.providers import aws


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        return iter(self.pages)


class FakeSession:
    def __init__(self, **clients):
        self.clients = clients
        self.calls = []

    def client(self, service_name, region_name=None):
        self.calls.append((service_name, region_name))
        return self.clients[service_name]


def plan(**overrides):
    values = {
        'provider': 'aws',
        'aws_profile': 'default',
        'region': 'us-east-2',
        'availability_domain': 'us-east-2a',
        'shape': 't3.micro',
        'ocpus': 2,
        'memory_gb': 1,
        'storage': SimpleNamespace(boot_size_gb=100),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def ownership_tags(job_id='job123'):
    return [
        {'Key': 'benchmark-job', 'Value': job_id},
        {'Key': 'managed-by', 'Value': aws.MANAGED_BY},
    ]


class AwsDiscoveryTests(unittest.TestCase):
    def test_create_session_uses_only_the_explicit_local_profile_reference(self):
        with patch.object(aws.boto3, 'Session') as session:
            aws.create_session('default', 'us-east-2')

        session.assert_called_once_with(
            profile_name='default',
            region_name='us-east-2',
        )

    def test_bootstrap_uses_named_profile_session_and_enabled_regions(self):
        sts = MagicMock()
        sts.get_caller_identity.return_value = {
            'Account': '123456789012',
            'Arn': 'arn:aws:iam::123456789012:user/tester',
            'UserId': 'AIDAEXAMPLE',
        }
        ec2 = MagicMock()
        ec2.describe_regions.return_value = {
            'Regions': [
                {'RegionName': 'us-west-2', 'OptInStatus': 'opt-in-not-required'},
                {'RegionName': 'ap-east-1', 'OptInStatus': 'not-opted-in'},
                {'RegionName': 'us-east-2', 'OptInStatus': 'opted-in'},
            ]
        }

        result = aws.bootstrap(
            profile='default',
            default_region='us-east-2',
            aws_session=FakeSession(sts=sts, ec2=ec2),
        )

        self.assertEqual(result['default_region'], 'us-east-2')
        self.assertEqual(result['regions'], ['us-east-2', 'us-west-2'])
        self.assertEqual(result['account_id'], '123456789012')
        self.assertNotIn('credentials', result)
        ec2.describe_regions.assert_called_once_with(AllRegions=False)

    def test_placement_returns_only_available_zones(self):
        ec2 = MagicMock()
        ec2.describe_availability_zones.return_value = {
            'AvailabilityZones': [
                {'ZoneName': 'us-east-2b', 'ZoneId': 'use2-az2', 'State': 'available'},
                {'ZoneName': 'us-east-2a', 'ZoneId': 'use2-az1', 'State': 'available'},
            ]
        }

        result = aws.placement(
            aws_session=FakeSession(ec2=ec2),
        )

        self.assertEqual(
            [item['name'] for item in result['availability_zones']],
            ['us-east-2a', 'us-east-2b'],
        )
        ec2.describe_availability_zones.assert_called_once_with(
            Filters=[{'Name': 'state', 'Values': ['available']}],
        )

    def test_instance_types_are_deduplicated_normalized_and_az_scoped(self):
        ec2 = MagicMock()
        paginator = FakePaginator([{
            'InstanceTypeOfferings': [
                {'InstanceType': 't4g.small'},
                {'InstanceType': 't3.micro'},
                {'InstanceType': 't3.micro'},
                {'InstanceType': 't3.nano'},
            ]
        }])
        ec2.get_paginator.return_value = paginator
        ec2.describe_instance_types.return_value = {
            'InstanceTypes': [
                {
                    'InstanceType': 't4g.small',
                    'ProcessorInfo': {'SupportedArchitectures': ['arm64']},
                    'VCpuInfo': {'DefaultVCpus': 2},
                    'MemoryInfo': {'SizeInMiB': 2048},
                    'NetworkInfo': {'NetworkPerformance': 'Up to 5 Gigabit'},
                    'CurrentGeneration': True,
                },
                {
                    'InstanceType': 't3.micro',
                    'ProcessorInfo': {'SupportedArchitectures': ['x86_64']},
                    'VCpuInfo': {'DefaultVCpus': 2},
                    'MemoryInfo': {'SizeInMiB': 1024},
                    'BurstablePerformanceSupported': True,
                },
                {
                    'InstanceType': 't3.nano',
                    'ProcessorInfo': {'SupportedArchitectures': ['x86_64']},
                    'VCpuInfo': {'DefaultVCpus': 2},
                    'MemoryInfo': {'SizeInMiB': 512},
                },
            ]
        }

        result = aws.instance_types(
            availability_zone='us-east-2a',
            aws_session=FakeSession(ec2=ec2),
        )

        self.assertEqual(
            [item['instance_type'] for item in result['items']],
            ['t3.micro', 't4g.small'],
        )
        self.assertEqual(result['items'][0]['memory_gb'], 1)
        self.assertEqual(result['items'][1]['architecture'], 'arm64')
        self.assertEqual(
            paginator.calls,
            [{
                'LocationType': 'availability-zone',
                'Filters': [{'Name': 'location', 'Values': ['us-east-2a']}],
            }],
        )

    def test_instance_types_exclude_a1_and_mac_from_the_al2023_catalog(self):
        ec2 = MagicMock()
        paginator = FakePaginator([{
            'InstanceTypeOfferings': [
                {'InstanceType': 'a1.large'},
                {'InstanceType': 'mac1.metal'},
                {'InstanceType': 'mac2.metal'},
                {'InstanceType': 'm7g.large'},
            ]
        }])
        ec2.get_paginator.return_value = paginator
        ec2.describe_instance_types.return_value = {'InstanceTypes': [
            {
                'InstanceType': 'a1.large',
                'ProcessorInfo': {'SupportedArchitectures': ['arm64']},
                'VCpuInfo': {'DefaultVCpus': 2},
                'MemoryInfo': {'SizeInMiB': 4096},
            },
            {
                'InstanceType': 'mac1.metal',
                'ProcessorInfo': {'SupportedArchitectures': ['x86_64']},
                'VCpuInfo': {'DefaultVCpus': 12},
                'MemoryInfo': {'SizeInMiB': 32768},
            },
            {
                'InstanceType': 'mac2.metal',
                'ProcessorInfo': {'SupportedArchitectures': ['arm64']},
                'VCpuInfo': {'DefaultVCpus': 8},
                'MemoryInfo': {'SizeInMiB': 16384},
            },
            {
                'InstanceType': 'm7g.large',
                'ProcessorInfo': {'SupportedArchitectures': ['arm64']},
                'VCpuInfo': {'DefaultVCpus': 2},
                'MemoryInfo': {'SizeInMiB': 8192},
            },
        ]}

        result = aws.instance_types(aws_session=FakeSession(ec2=ec2))

        self.assertEqual(
            [item['instance_type'] for item in result['items']],
            ['m7g.large'],
        )

    def test_latest_ami_uses_the_dynamic_default_public_parameter(self):
        ssm = MagicMock()
        ssm.get_parameter.return_value = {
            'Parameter': {'Value': 'ami-123', 'Version': 42}
        }
        ec2 = MagicMock()
        ec2.describe_images.return_value = {'Images': [{
            'ImageId': 'ami-123',
            'Name': 'al2023-ami-test',
            'Architecture': 'arm64',
            'State': 'available',
            'RootDeviceName': '/dev/xvda',
        }]}

        result = aws.latest_amazon_linux_2023_ami(
            architecture='aarch64',
            aws_session=FakeSession(ssm=ssm, ec2=ec2),
        )

        expected = (
            '/aws/service/ami-amazon-linux-latest/'
            'al2023-ami-kernel-default-arm64'
        )
        self.assertEqual(result['parameter_name'], expected)
        self.assertEqual(result['image_id'], 'ami-123')
        ssm.get_parameter.assert_called_once_with(Name=expected)
        ec2.describe_images.assert_called_once_with(
            ImageIds=['ami-123'],
            Owners=['amazon'],
        )


class AwsProvisionTests(unittest.TestCase):
    def setUp(self):
        self.ec2 = MagicMock()
        self.sts = MagicMock()
        self.ssm = MagicMock()
        self.session = FakeSession(ec2=self.ec2, sts=self.sts, ssm=self.ssm)
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
        self.ec2.import_key_pair.return_value = {
            'KeyPairId': 'key-1',
            'KeyName': 'benchmark-job123',
        }
        self.ec2.describe_instance_type_offerings.return_value = {
            'InstanceTypeOfferings': [{
                'InstanceType': 't3.micro',
                'Location': 'us-east-2a',
            }]
        }
        self.ec2.describe_availability_zones.return_value = {
            'AvailabilityZones': [
                {'ZoneName': 'us-east-2a', 'State': 'available'},
                {'ZoneName': 'us-east-2b', 'State': 'available'},
            ]
        }
        self.ec2.describe_instance_types.return_value = {'InstanceTypes': [{
            'InstanceType': 't3.micro',
            'ProcessorInfo': {'SupportedArchitectures': ['x86_64']},
            'VCpuInfo': {'DefaultVCpus': 2},
            'MemoryInfo': {'SizeInMiB': 1024},
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
        self.ec2.run_instances.return_value = {
            'Instances': [{'InstanceId': 'i-1'}]
        }
        self.ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-1',
                'PublicIpAddress': '198.51.100.10',
                'PrivateIpAddress': '10.42.1.10',
            }]
        }]}

    def test_provision_records_each_exact_resource_and_launches_al2023(self):
        job = {'id': 'job123', 'resources': {}}
        snapshots = []

        resources = aws.provision(
            job,
            plan(),
            public_key='ssh-ed25519 AAAATEST tester',
            aws_session=self.session,
            persist=lambda value: snapshots.append(
                copy.deepcopy(value['resources'])
            ),
        )

        self.assertEqual(resources['provider'], 'aws')
        self.assertEqual(resources['aws_vpc_id'], 'vpc-1')
        self.assertEqual(resources['aws_instance_id'], 'i-1')
        self.assertEqual(resources['instance_id'], 'i-1')
        self.assertEqual(resources['ssh_user'], 'ec2-user')
        self.assertEqual(resources['public_ip'], '198.51.100.10')
        self.assertEqual(resources['vcpu'], 2)
        self.assertEqual(resources['memory_gb'], 1)
        self.assertEqual(resources['availability_zone'], 'us-east-2a')
        self.assertEqual(resources['aws_account_id'], '123456789012')
        self.assertFalse(resources['bare_metal'])
        self.assertEqual(
            resources['aws_instance_client_token'],
            self.ec2.run_instances.call_args.kwargs['ClientToken'],
        )
        self.assertTrue(any(item.get('aws_vpc_id') == 'vpc-1' for item in snapshots))
        self.assertTrue(any(item.get('aws_instance_client_token') for item in snapshots))
        self.assertTrue(any(item.get('aws_instance_id') == 'i-1' for item in snapshots))
        self.assertNotIn('public_key', resources)
        self.assertNotIn('credentials', resources)

        launch = self.ec2.run_instances.call_args.kwargs
        self.assertEqual(len(launch['ClientToken']), 32)
        self.assertEqual(launch['ImageId'], 'ami-123')
        self.assertEqual(launch['InstanceType'], 't3.micro')
        self.assertEqual(launch['MetadataOptions']['HttpTokens'], 'required')
        self.assertEqual(
            launch['BlockDeviceMappings'][0]['Ebs']['VolumeType'],
            'gp3',
        )
        self.assertEqual(
            launch['NetworkInterfaces'][0]['Groups'],
            ['sg-1'],
        )
        self.assertEqual(
            self.ec2.create_subnet.call_args.kwargs['AvailabilityZone'],
            'us-east-2a',
        )
        self.ec2.describe_instance_type_offerings.assert_called_once_with(
            LocationType='availability-zone',
            Filters=[
                {'Name': 'instance-type', 'Values': ['t3.micro']},
                {'Name': 'location', 'Values': ['us-east-2a']},
            ],
        )
        ingress = self.ec2.authorize_security_group_ingress.call_args.kwargs
        self.assertEqual(
            ingress['IpPermissions'][0]['IpRanges'][0]['CidrIp'],
            '0.0.0.0/0',
        )

    def test_bare_metal_launch_uses_explicit_forty_minute_waiters(self):
        self.ec2.describe_instance_types.return_value['InstanceTypes'][0][
            'BareMetal'
        ] = True
        waiters = {}

        def get_waiter(name):
            waiters.setdefault(name, MagicMock())
            return waiters[name]

        self.ec2.get_waiter.side_effect = get_waiter

        resources = aws.provision(
            {'id': 'job123', 'resources': {}},
            plan(),
            public_key='ssh-ed25519 AAAATEST tester',
            aws_session=self.session,
        )

        expected = {
            'InstanceIds': ['i-1'],
            'WaiterConfig': {
                'Delay': aws.BARE_METAL_WAITER_DELAY_SECONDS,
                'MaxAttempts': aws.BARE_METAL_WAITER_MAX_ATTEMPTS,
            },
        }
        waiters['instance_running'].wait.assert_called_once_with(**expected)
        waiters['instance_status_ok'].wait.assert_called_once_with(**expected)
        self.assertEqual(
            aws.BARE_METAL_WAITER_DELAY_SECONDS
            * aws.BARE_METAL_WAITER_MAX_ATTEMPTS,
            40 * 60,
        )
        self.assertTrue(resources['bare_metal'])

    def test_partial_failure_leaves_a_persisted_cleanup_manifest(self):
        self.ec2.create_route_table.side_effect = RuntimeError('route failure')
        job = {'id': 'job123', 'resources': {}}
        snapshots = []

        with self.assertRaisesRegex(RuntimeError, 'route failure'):
            aws.provision(
                job,
                plan(),
                public_key='ssh-ed25519 AAAATEST tester',
                aws_session=self.session,
                persist=lambda value: snapshots.append(
                    copy.deepcopy(value['resources'])
                ),
            )

        self.assertEqual(job['resources']['aws_vpc_id'], 'vpc-1')
        self.assertEqual(job['resources']['aws_subnet_id'], 'subnet-1')
        self.assertEqual(snapshots[-1]['aws_subnet_id'], 'subnet-1')

    def test_lost_run_instances_response_leaves_a_reconcilable_token(self):
        self.ec2.run_instances.side_effect = TimeoutError('response lost')
        job = {'id': 'job123', 'resources': {}}
        snapshots = []

        with self.assertRaisesRegex(TimeoutError, 'response lost'):
            aws.provision(
                job,
                plan(),
                public_key='ssh-ed25519 AAAATEST tester',
                aws_session=self.session,
                persist=lambda value: snapshots.append(
                    copy.deepcopy(value['resources'])
                ),
            )

        token = self.ec2.run_instances.call_args.kwargs['ClientToken']
        self.assertEqual(job['resources']['aws_instance_client_token'], token)
        self.assertNotIn('aws_instance_id', job['resources'])
        self.assertEqual(
            snapshots[-1]['aws_instance_client_token'],
            token,
        )

    def test_lost_base_create_responses_leave_durable_ambiguity_contracts(self):
        cases = (
            (
                'VPC',
                'create_vpc',
                'aws_vpc_create_ambiguous',
                'aws_vpc_id',
            ),
            (
                'internet gateway',
                'create_internet_gateway',
                'aws_internet_gateway_create_ambiguous',
                'aws_internet_gateway_id',
            ),
            (
                'route table',
                'create_route_table',
                'aws_route_table_create_ambiguous',
                'aws_route_table_id',
            ),
            (
                'key pair',
                'import_key_pair',
                'aws_key_pair_import_ambiguous',
                'aws_key_pair_id',
            ),
        )
        for label, operation, marker, resource_id in cases:
            with self.subTest(resource=label):
                self.setUp()
                getattr(self.ec2, operation).side_effect = TimeoutError(
                    f'{label} response lost'
                )
                job = {'id': 'job123', 'resources': {}}
                snapshots = []

                with self.assertRaisesRegex(TimeoutError, 'response lost'):
                    aws.provision(
                        job,
                        plan(),
                        public_key='ssh-ed25519 AAAATEST tester',
                        aws_session=self.session,
                        persist=lambda value: snapshots.append(
                            copy.deepcopy(value['resources'])
                        ),
                    )

                self.assertTrue(job['resources'][marker])
                self.assertNotIn(resource_id, job['resources'])
                self.assertTrue(any(item.get(marker) is True for item in snapshots))
                if operation == 'create_route_table':
                    request = self.ec2.create_route_table.call_args.kwargs
                    self.assertEqual(
                        request['ClientToken'],
                        job['resources']['aws_route_table_client_token'],
                    )
                    self.assertEqual(
                        job['resources']['aws_route_table_vpc_id'],
                        'vpc-1',
                    )
                if operation == 'import_key_pair':
                    self.assertEqual(
                        job['resources']['aws_key_pair_name'],
                        'benchmark-job123',
                    )

    def test_confirmed_base_create_rejections_clear_their_contracts(self):
        cases = (
            (
                'create_vpc',
                ('aws_vpc_create_ambiguous',),
            ),
            (
                'create_internet_gateway',
                ('aws_internet_gateway_create_ambiguous',),
            ),
            (
                'create_route_table',
                (
                    'aws_route_table_client_token',
                    'aws_route_table_create_ambiguous',
                    'aws_route_table_vpc_id',
                ),
            ),
            (
                'import_key_pair',
                ('aws_key_pair_name', 'aws_key_pair_import_ambiguous'),
            ),
        )
        for operation, contract_keys in cases:
            with self.subTest(operation=operation):
                self.setUp()
                getattr(self.ec2, operation).side_effect = ClientError(
                    {
                        'Error': {
                            'Code': 'InvalidParameterValue',
                            'Message': 'rejected',
                        },
                        'ResponseMetadata': {'HTTPStatusCode': 400},
                    },
                    operation,
                )
                job = {'id': 'job123', 'resources': {}}

                with self.assertRaises(ClientError):
                    aws.provision(
                        job,
                        plan(),
                        public_key='ssh-ed25519 AAAATEST tester',
                        aws_session=self.session,
                    )

                for key in contract_keys:
                    self.assertNotIn(key, job['resources'])

    def test_rejects_forged_fixed_instance_capacity(self):
        job = {'id': 'job123', 'resources': {}}

        with self.assertRaisesRegex(ValueError, 'has 2 vCPUs'):
            aws.provision(
                job,
                plan(ocpus=8),
                public_key='ssh-ed25519 AAAATEST tester',
                aws_session=self.session,
            )

    def test_kernel_phoronix_requires_four_gib_before_resource_creation(self):
        job = {'id': 'job123', 'resources': {}}

        with self.assertRaisesRegex(
            ValueError,
            'Linux Kernel Compilation.*at least 4 GiB',
        ):
            aws.provision(
                job,
                plan(
                    benchmarks=['phoronix'],
                    phoronix=SimpleNamespace(
                        profiles=['build_linux_kernel']
                    ),
                ),
                public_key='ssh-ed25519 AAAATEST tester',
                aws_session=self.session,
            )

        self.ec2.describe_availability_zones.assert_not_called()
        self.ec2.create_vpc.assert_not_called()

    def test_rejects_a_stale_or_unavailable_regional_instance_type(self):
        self.ec2.describe_instance_type_offerings.return_value = {
            'InstanceTypeOfferings': []
        }
        job = {'id': 'job123', 'resources': {}}

        with self.assertRaisesRegex(ValueError, 'not currently offered'):
            aws.provision(
                job,
                plan(),
                public_key='ssh-ed25519 AAAATEST tester',
                aws_session=self.session,
            )

        self.ec2.create_vpc.assert_not_called()

    def test_rejects_forged_a1_and_mac_plans_before_creating_resources(self):
        incompatible_types = (
            ('a1.large', 'arm64', 'Graviton'),
            ('mac1.metal', 'x86_64', 'Dedicated Host'),
            ('mac2.metal', 'arm64', 'Dedicated Host'),
        )
        for instance_type, architecture, message in incompatible_types:
            with self.subTest(instance_type=instance_type):
                self.ec2.reset_mock()
                self.ec2.describe_instance_types.return_value = {
                    'InstanceTypes': [{
                        'InstanceType': instance_type,
                        'ProcessorInfo': {
                            'SupportedArchitectures': [architecture]
                        },
                        'VCpuInfo': {'DefaultVCpus': 2},
                        'MemoryInfo': {'SizeInMiB': 4096},
                    }]
                }
                with self.assertRaisesRegex(ValueError, message):
                    aws.provision(
                        {'id': 'job123', 'resources': {}},
                        plan(
                            shape=instance_type,
                            ocpus=2,
                            memory_gb=4,
                        ),
                        public_key='ssh-ed25519 AAAATEST tester',
                        aws_session=self.session,
                    )
                self.ec2.create_vpc.assert_not_called()

    def test_selects_an_available_zone_that_explicitly_offers_the_type(self):
        self.ec2.create_subnet.return_value = {'Subnet': {
            'SubnetId': 'subnet-1',
            'AvailabilityZone': 'us-east-2b',
        }}
        self.ec2.describe_instance_type_offerings.return_value = {
            'InstanceTypeOfferings': [{
                'InstanceType': 't3.micro',
                'Location': 'us-east-2b',
            }]
        }
        job = {'id': 'job123', 'resources': {}}

        resources = aws.provision(
            job,
            plan(availability_domain=None),
            public_key='ssh-ed25519 AAAATEST tester',
            aws_session=self.session,
        )

        self.assertEqual(resources['availability_zone'], 'us-east-2b')
        self.assertEqual(
            self.ec2.create_subnet.call_args.kwargs['AvailabilityZone'],
            'us-east-2b',
        )
        self.ec2.describe_instance_type_offerings.assert_called_once_with(
            LocationType='availability-zone',
            Filters=[{'Name': 'instance-type', 'Values': ['t3.micro']}],
        )

    def test_persisted_zone_is_reused_and_validated(self):
        self.ec2.create_subnet.return_value = {'Subnet': {
            'SubnetId': 'subnet-1',
            'AvailabilityZone': 'us-east-2b',
        }}
        self.ec2.describe_instance_type_offerings.return_value = {
            'InstanceTypeOfferings': [{
                'InstanceType': 't3.micro',
                'Location': 'us-east-2b',
            }]
        }
        job = {
            'id': 'job123',
            'resources': {'availability_zone': 'us-east-2b'},
        }

        resources = aws.provision(
            job,
            plan(availability_domain='us-east-2a'),
            public_key='ssh-ed25519 AAAATEST tester',
            aws_session=self.session,
        )

        self.assertEqual(resources['availability_zone'], 'us-east-2b')
        self.assertEqual(
            self.ec2.create_subnet.call_args.kwargs['AvailabilityZone'],
            'us-east-2b',
        )


class AwsCleanupTests(unittest.TestCase):
    def test_cleanup_uses_only_recorded_ids_in_dependency_order(self):
        ec2 = MagicMock()
        tags = ownership_tags()
        ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-1',
                'VpcId': 'vpc-1',
                'SubnetId': 'subnet-1',
                'Tags': tags,
            }],
        }]}
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
        ec2.describe_security_groups.return_value = {'SecurityGroups': [{
            'GroupId': 'sg-1',
            'VpcId': 'vpc-1',
            'Tags': tags,
        }]}
        ec2.describe_internet_gateways.return_value = {
            'InternetGateways': [{
                'InternetGatewayId': 'igw-1',
                'Attachments': [{'VpcId': 'vpc-1', 'State': 'available'}],
                'Tags': tags,
            }]
        }
        ec2.describe_vpcs.return_value = {'Vpcs': [{
            'VpcId': 'vpc-1',
            'Tags': tags,
        }]}
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        session = FakeSession(ec2=ec2, sts=sts)
        job = {
            'id': 'job123',
            'status': 'complete',
            'plan': {'aws_profile': 'default', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'aws_profile': 'default',
                'aws_account_id': '123456789012',
                'region': 'us-east-2',
                'aws_instance_id': 'i-1',
                'aws_instance_client_token': 'client-token-1',
                'instance_id': 'i-1',
                'public_ip': '198.51.100.10',
                'private_ip': '10.42.1.10',
                'aws_key_pair_id': 'key-1',
                'aws_key_pair_name': 'benchmark-job123',
                'aws_route_table_association_id': 'rtbassoc-1',
                'aws_subnet_id': 'subnet-1',
                'aws_route_table_id': 'rtb-1',
                'aws_security_group_id': 'sg-1',
                'aws_internet_gateway_id': 'igw-1',
                'aws_internet_gateway_attached': True,
                'aws_vpc_id': 'vpc-1',
            },
        }
        snapshots = []

        aws.destroy_resources(
            job,
            aws_session=session,
            persist=lambda value: snapshots.append(
                copy.deepcopy(value['resources'])
            ),
        )

        self.assertEqual(job['status'], 'destroyed')
        self.assertNotIn('aws_vpc_id', job['resources'])
        self.assertNotIn('aws_instance_client_token', job['resources'])
        self.assertEqual(job['resources']['provider'], 'aws')
        self.assertGreaterEqual(len(snapshots), 8)
        names = [call[0] for call in ec2.method_calls]
        self.assertLess(names.index('terminate_instances'), names.index('delete_subnet'))
        self.assertLess(names.index('disassociate_route_table'), names.index('delete_route_table'))
        self.assertLess(names.index('delete_subnet'), names.index('delete_vpc'))
        self.assertLess(names.index('detach_internet_gateway'), names.index('delete_vpc'))
        self.assertLess(names.index('describe_vpcs'), names.index('terminate_instances'))
        ec2.describe_vpcs.assert_called_once_with(VpcIds=['vpc-1'])
        ec2.describe_key_pairs.assert_called_once_with(KeyPairIds=['key-1'])
        ec2.describe_instances.assert_called_once_with(InstanceIds=['i-1'])
        ec2.delete_key_pair.assert_called_once_with(KeyPairId='key-1')
        ec2.delete_vpc.assert_called_once_with(VpcId='vpc-1')

        # A second pass is safe and does not discover or delete by broad tags.
        before = len(ec2.method_calls)
        aws.destroy_resources(job, aws_session=session)
        after_calls = [call[0] for call in ec2.method_calls[before:]]
        self.assertNotIn('delete_vpc', after_calls)
        self.assertNotIn('describe_vpcs', after_calls)

    def test_cleanup_recovers_every_response_lost_tagged_resource_before_delete(self):
        ec2 = MagicMock()
        tags = ownership_tags()
        vpc = {'VpcId': 'vpc-recovered', 'Tags': tags}
        gateway = {
            'InternetGatewayId': 'igw-recovered',
            'Attachments': [{
                'VpcId': 'vpc-recovered',
                'State': 'available',
            }],
            'Tags': tags,
        }
        subnet = {
            'SubnetId': 'subnet-recovered',
            'VpcId': 'vpc-recovered',
            'Tags': tags,
        }
        route_table = {
            'RouteTableId': 'rtb-recovered',
            'VpcId': 'vpc-recovered',
            'Tags': tags,
            'Associations': [{
                'RouteTableAssociationId': 'rtbassoc-recovered',
                'SubnetId': 'subnet-recovered',
                'Main': False,
            }],
        }
        group = {
            'GroupId': 'sg-recovered',
            'VpcId': 'vpc-recovered',
            'Tags': tags,
        }
        key_pair = {
            'KeyPairId': 'key-recovered',
            'KeyName': 'benchmark-job123',
            'Tags': tags,
        }
        ec2.describe_vpcs.side_effect = lambda **_kwargs: {'Vpcs': [vpc]}
        ec2.describe_internet_gateways.side_effect = (
            lambda **_kwargs: {'InternetGateways': [gateway]}
        )
        ec2.describe_subnets.side_effect = (
            lambda **_kwargs: {'Subnets': [subnet]}
        )
        ec2.describe_route_tables.side_effect = (
            lambda **_kwargs: {'RouteTables': [route_table]}
        )
        ec2.describe_security_groups.side_effect = (
            lambda **_kwargs: {'SecurityGroups': [group]}
        )
        ec2.describe_key_pairs.side_effect = (
            lambda **_kwargs: {'KeyPairs': [key_pair]}
        )
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        job = {
            'id': 'job123',
            'status': 'provisioning',
            'plan': {'aws_profile': 'default', 'region': 'us-east-2'},
            # Provisioning metadata is durable, but every create response was
            # lost before its exact ID could be recorded.
            'resources': {
                'provider': 'aws',
                'aws_profile': 'default',
                'aws_account_id': '123456789012',
                'region': 'us-east-2',
            },
        }
        snapshots = []

        aws.destroy_resources(
            job,
            aws_session=FakeSession(ec2=ec2, sts=sts),
            persist=lambda value: snapshots.append(
                copy.deepcopy(value['resources'])
            ),
        )

        sts.get_caller_identity.assert_called_once_with()
        tagged_filters = [
            {'Name': 'tag:benchmark-job', 'Values': ['job123']},
            {'Name': 'tag:managed-by', 'Values': [aws.MANAGED_BY]},
        ]
        self.assertIn(
            call(Filters=tagged_filters),
            ec2.describe_vpcs.call_args_list,
        )
        self.assertIn(
            call(
                Filters=[
                    {
                        'Name': 'key-name',
                        'Values': ['benchmark-job123'],
                    },
                    *tagged_filters,
                ],
            ),
            ec2.describe_key_pairs.call_args_list,
        )
        self.assertTrue(any(
            snapshot.get('aws_vpc_id') == 'vpc-recovered'
            and snapshot.get('aws_internet_gateway_id') == 'igw-recovered'
            and snapshot.get('aws_subnet_id') == 'subnet-recovered'
            and snapshot.get('aws_route_table_id') == 'rtb-recovered'
            and snapshot.get('aws_security_group_id') == 'sg-recovered'
            and snapshot.get('aws_key_pair_id') == 'key-recovered'
            for snapshot in snapshots
        ))
        self.assertTrue(any(
            snapshot.get('aws_route_table_association_id')
            == 'rtbassoc-recovered'
            for snapshot in snapshots
        ))
        ec2.delete_key_pair.assert_called_once_with(KeyPairId='key-recovered')
        ec2.disassociate_route_table.assert_called_once_with(
            AssociationId='rtbassoc-recovered'
        )
        ec2.delete_subnet.assert_called_once_with(SubnetId='subnet-recovered')
        ec2.delete_route_table.assert_called_once_with(
            RouteTableId='rtb-recovered'
        )
        ec2.delete_security_group.assert_called_once_with(GroupId='sg-recovered')
        ec2.detach_internet_gateway.assert_called_once_with(
            InternetGatewayId='igw-recovered',
            VpcId='vpc-recovered',
        )
        ec2.delete_internet_gateway.assert_called_once_with(
            InternetGatewayId='igw-recovered'
        )
        ec2.delete_vpc.assert_called_once_with(VpcId='vpc-recovered')
        self.assertEqual(job['status'], 'destroyed')

    def test_cleanup_retries_an_ambiguous_vpc_until_its_tags_are_visible(self):
        ec2 = MagicMock()
        tags = ownership_tags()
        vpc = {'VpcId': 'vpc-eventual', 'Tags': tags}
        visibility = iter(([], [], [vpc]))

        def describe_vpcs(**kwargs):
            if 'Filters' in kwargs:
                return {'Vpcs': next(visibility)}
            return {'Vpcs': [vpc]}

        ec2.describe_vpcs.side_effect = describe_vpcs
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        job = {
            'id': 'job123',
            'status': 'provisioning',
            'plan': {'provider': 'aws', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'aws_account_id': '123456789012',
                'region': 'us-east-2',
                'aws_vpc_create_ambiguous': True,
            },
        }
        snapshots = []

        with patch.object(aws.time, 'sleep') as sleep:
            aws.destroy_resources(
                job,
                aws_session=FakeSession(ec2=ec2, sts=sts),
                persist=lambda value: snapshots.append(
                    copy.deepcopy(value['resources'])
                ),
            )

        self.assertEqual(sleep.call_args_list, [call(2), call(4)])
        tagged_calls = [
            item for item in ec2.describe_vpcs.call_args_list
            if 'Filters' in item.kwargs
        ]
        self.assertEqual(len(tagged_calls), 3)
        self.assertTrue(any(
            item.get('aws_vpc_id') == 'vpc-eventual'
            and item.get('aws_vpc_create_ambiguous') is False
            for item in snapshots
        ))
        ec2.delete_vpc.assert_called_once_with(VpcId='vpc-eventual')
        self.assertNotIn('aws_vpc_create_ambiguous', job['resources'])
        self.assertEqual(job['status'], 'destroyed')

    def test_unresolved_gateway_ambiguity_cleans_independent_vpc_and_retries(self):
        ec2 = MagicMock()
        tags = ownership_tags()
        ec2.describe_vpcs.return_value = {'Vpcs': [{
            'VpcId': 'vpc-1',
            'Tags': tags,
        }]}
        ec2.describe_internet_gateways.return_value = {
            'InternetGateways': []
        }
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        job = {
            'id': 'job123',
            'status': 'provisioning',
            'plan': {'provider': 'aws', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'aws_account_id': '123456789012',
                'region': 'us-east-2',
                'aws_vpc_id': 'vpc-1',
                'aws_vpc_create_ambiguous': False,
                'aws_internet_gateway_create_ambiguous': True,
            },
        }

        with (
            patch.object(aws.time, 'sleep'),
            self.assertRaisesRegex(RuntimeError, 'response-lost AWS internet'),
        ):
            aws.destroy_resources(
                job,
                aws_session=FakeSession(ec2=ec2, sts=sts),
            )

        tagged_calls = [
            item for item in ec2.describe_internet_gateways.call_args_list
            if 'Filters' in item.kwargs
        ]
        self.assertEqual(
            len(tagged_calls),
            aws.AMBIGUOUS_TAG_LOOKUP_ATTEMPTS,
        )
        ec2.delete_vpc.assert_called_once_with(VpcId='vpc-1')
        self.assertNotIn('aws_vpc_id', job['resources'])
        self.assertTrue(
            job['resources']['aws_internet_gateway_create_ambiguous']
        )
        self.assertIn(
            'aws_internet_gateway_reconciliation_error',
            job['resources'],
        )
        self.assertNotEqual(job['status'], 'destroyed')

    def test_ambiguous_route_table_replays_its_exact_client_token(self):
        ec2 = MagicMock()
        tags = ownership_tags()
        route_table = {
            'RouteTableId': 'rtb-replayed',
            'VpcId': 'vpc-1',
            'Tags': tags,
            'Associations': [],
        }
        ec2.describe_vpcs.return_value = {'Vpcs': [{
            'VpcId': 'vpc-1',
            'Tags': tags,
        }]}

        def describe_route_tables(**kwargs):
            if 'Filters' in kwargs:
                return {'RouteTables': []}
            return {'RouteTables': [route_table]}

        ec2.describe_route_tables.side_effect = describe_route_tables
        ec2.create_route_table.return_value = {'RouteTable': route_table}
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        job = {
            'id': 'job123',
            'status': 'provisioning',
            'plan': {'provider': 'aws', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'aws_account_id': '123456789012',
                'region': 'us-east-2',
                'aws_vpc_id': 'vpc-1',
                'aws_route_table_client_token': 'stable-route-token',
                'aws_route_table_create_ambiguous': True,
                'aws_route_table_vpc_id': 'vpc-1',
            },
        }
        snapshots = []

        with patch.object(aws.time, 'sleep'):
            aws.destroy_resources(
                job,
                aws_session=FakeSession(ec2=ec2, sts=sts),
                persist=lambda value: snapshots.append(
                    copy.deepcopy(value['resources'])
                ),
            )

        request = ec2.create_route_table.call_args.kwargs
        self.assertEqual(request['ClientToken'], 'stable-route-token')
        self.assertEqual(request['VpcId'], 'vpc-1')
        self.assertEqual(
            [tag for tag in request['TagSpecifications'][0]['Tags'] if tag['Key'] != 'Name'],
            tags,
        )
        self.assertTrue(any(
            item.get('aws_route_table_id') == 'rtb-replayed'
            and item.get('aws_route_table_create_ambiguous') is False
            for item in snapshots
        ))
        ec2.delete_route_table.assert_called_once_with(
            RouteTableId='rtb-replayed'
        )
        self.assertNotIn('aws_route_table_client_token', job['resources'])
        self.assertEqual(job['status'], 'destroyed')

    def test_deleted_vpc_conclusively_clears_unresolved_route_contract(self):
        ec2 = MagicMock()
        tags = ownership_tags()
        ec2.describe_vpcs.return_value = {'Vpcs': [{
            'VpcId': 'vpc-1',
            'Tags': tags,
        }]}
        ec2.describe_route_tables.return_value = {'RouteTables': []}
        ec2.create_route_table.side_effect = TimeoutError('response lost')
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        job = {
            'id': 'job123',
            'status': 'provisioning',
            'plan': {'provider': 'aws', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'aws_account_id': '123456789012',
                'region': 'us-east-2',
                'aws_vpc_id': 'vpc-1',
                'aws_route_table_client_token': 'stable-route-token',
                'aws_route_table_create_ambiguous': True,
                'aws_route_table_vpc_id': 'vpc-1',
            },
        }

        with patch.object(aws.time, 'sleep'):
            aws.destroy_resources(
                job,
                aws_session=FakeSession(ec2=ec2, sts=sts),
            )

        tagged_calls = [
            item for item in ec2.describe_route_tables.call_args_list
            if 'Filters' in item.kwargs
        ]
        self.assertEqual(
            len(tagged_calls),
            aws.AMBIGUOUS_TAG_LOOKUP_ATTEMPTS,
        )
        ec2.create_route_table.assert_called_once()
        ec2.delete_vpc.assert_called_once_with(VpcId='vpc-1')
        for key in aws.ROUTE_TABLE_CONTRACT_KEYS:
            self.assertNotIn(key, job['resources'])
        self.assertEqual(job['status'], 'destroyed')

    def test_unresolved_key_import_never_deletes_an_unverified_name(self):
        ec2 = MagicMock()
        ec2.describe_key_pairs.return_value = {'KeyPairs': []}
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        job = {
            'id': 'job123',
            'status': 'provisioning',
            'plan': {'provider': 'aws', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'aws_account_id': '123456789012',
                'region': 'us-east-2',
                'aws_key_pair_name': 'benchmark-job123',
                'aws_key_pair_import_ambiguous': True,
            },
        }

        with (
            patch.object(aws.time, 'sleep'),
            self.assertRaisesRegex(RuntimeError, 'response-lost AWS key pair'),
        ):
            aws.destroy_resources(
                job,
                aws_session=FakeSession(ec2=ec2, sts=sts),
            )

        ec2.delete_key_pair.assert_not_called()
        self.assertTrue(job['resources']['aws_key_pair_import_ambiguous'])
        self.assertEqual(
            job['resources']['aws_key_pair_name'],
            'benchmark-job123',
        )
        self.assertNotEqual(job['status'], 'destroyed')

    def test_cleanup_fails_before_mutation_on_ambiguous_tag_reconciliation(self):
        ec2 = MagicMock()
        tags = ownership_tags()
        ec2.describe_vpcs.return_value = {'Vpcs': [
            {'VpcId': 'vpc-one', 'Tags': tags},
            {'VpcId': 'vpc-two', 'Tags': tags},
        ]}
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        resources = {
            'provider': 'aws',
            'aws_profile': 'default',
            'aws_account_id': '123456789012',
            'region': 'us-east-2',
        }
        job = {
            'id': 'job123',
            'status': 'provisioning',
            'plan': {'aws_profile': 'default', 'region': 'us-east-2'},
            'resources': copy.deepcopy(resources),
        }

        with self.assertRaisesRegex(RuntimeError, 'matched 2 VPC resources'):
            aws.destroy_resources(
                job,
                aws_session=FakeSession(ec2=ec2, sts=sts),
            )

        self.assertEqual(job['status'], 'provisioning')
        self.assertEqual(job['resources'], resources)
        ec2.terminate_instances.assert_not_called()
        ec2.delete_vpc.assert_not_called()
        ec2.delete_subnet.assert_not_called()

    def test_cleanup_rejects_reconciled_child_from_another_vpc(self):
        ec2 = MagicMock()
        tags = ownership_tags()
        ec2.describe_vpcs.return_value = {'Vpcs': [{
            'VpcId': 'vpc-owned',
            'Tags': tags,
        }]}
        ec2.describe_internet_gateways.return_value = {
            'InternetGateways': []
        }
        ec2.describe_subnets.return_value = {'Subnets': [{
            'SubnetId': 'subnet-foreign-parent',
            'VpcId': 'vpc-other',
            'Tags': tags,
        }]}
        ec2.describe_route_tables.return_value = {'RouteTables': []}
        ec2.describe_security_groups.return_value = {'SecurityGroups': []}
        ec2.describe_key_pairs.return_value = {'KeyPairs': []}
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        job = {
            'id': 'job123',
            'status': 'provisioning',
            'plan': {'aws_profile': 'default', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'aws_profile': 'default',
                'aws_account_id': '123456789012',
                'region': 'us-east-2',
            },
        }

        with self.assertRaisesRegex(RuntimeError, 'does not belong'):
            aws.destroy_resources(
                job,
                aws_session=FakeSession(ec2=ec2, sts=sts),
            )

        ec2.delete_subnet.assert_not_called()
        ec2.delete_vpc.assert_not_called()

    def test_cleanup_refuses_to_forget_resources_from_another_account(self):
        ec2 = MagicMock()
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '999999999999'}
        session = FakeSession(ec2=ec2, sts=sts)
        resources = {
            'provider': 'aws',
            'aws_profile': 'default',
            'aws_account_id': '123456789012',
            'region': 'us-east-2',
            'aws_instance_id': 'i-1',
            'instance_id': 'i-1',
            'aws_vpc_id': 'vpc-1',
        }
        job = {
            'id': 'job123',
            'status': 'complete',
            'plan': {'aws_profile': 'default', 'region': 'us-east-2'},
            'resources': copy.deepcopy(resources),
        }

        with self.assertRaisesRegex(RuntimeError, 'account 999999999999'):
            aws.destroy_resources(job, aws_session=session)

        self.assertEqual(job['status'], 'complete')
        self.assertEqual(job['resources'], resources)
        ec2.terminate_instances.assert_not_called()
        ec2.delete_vpc.assert_not_called()

    def test_cleanup_fails_closed_before_mutation_on_tag_mismatch(self):
        ec2 = MagicMock()
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [{
                'InstanceId': 'i-1',
                'Tags': ownership_tags(),
            }],
        }]}
        ec2.describe_vpcs.return_value = {'Vpcs': [{
            'VpcId': 'vpc-unrelated',
            'Tags': [
                {'Key': 'benchmark-job', 'Value': 'another-job'},
                {'Key': 'managed-by', 'Value': aws.MANAGED_BY},
            ],
        }]}
        session = FakeSession(ec2=ec2, sts=sts)
        resources = {
            'provider': 'aws',
            'aws_profile': 'default',
            'aws_account_id': '123456789012',
            'region': 'us-east-2',
            'aws_instance_id': 'i-1',
            'instance_id': 'i-1',
            'aws_vpc_id': 'vpc-unrelated',
        }
        job = {
            'id': 'job123',
            'status': 'complete',
            'plan': {'aws_profile': 'default', 'region': 'us-east-2'},
            'resources': copy.deepcopy(resources),
        }

        with self.assertRaisesRegex(RuntimeError, 'does not carry'):
            aws.destroy_resources(job, aws_session=session)

        self.assertEqual(job['status'], 'complete')
        self.assertEqual(job['resources'], resources)
        ec2.terminate_instances.assert_not_called()
        ec2.delete_vpc.assert_not_called()

    def test_cleanup_reconciles_an_instance_after_a_lost_launch_response(self):
        ec2 = MagicMock()
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [{'InstanceId': 'i-recovered'}],
        }]}
        session = FakeSession(ec2=ec2, sts=sts)
        job = {
            'id': 'job123',
            'status': 'provisioning',
            'plan': {'aws_profile': 'default', 'region': 'us-east-2'},
            'resources': {
                'provider': 'aws',
                'aws_profile': 'default',
                'aws_account_id': '123456789012',
                'region': 'us-east-2',
                'aws_instance_client_token': 'client-token-1',
                'aws_subnet_id': 'subnet-1',
                'aws_vpc_id': 'vpc-1',
            },
        }
        snapshots = []

        aws.destroy_resources(
            job,
            aws_session=session,
            persist=lambda value: snapshots.append(
                copy.deepcopy(value['resources'])
            ),
        )

        ec2.describe_instances.assert_called_once_with(Filters=[
            {'Name': 'client-token', 'Values': ['client-token-1']},
            {'Name': 'tag:benchmark-job', 'Values': ['job123']},
            {'Name': 'tag:managed-by', 'Values': [aws.MANAGED_BY]},
        ])
        ec2.terminate_instances.assert_called_once_with(
            InstanceIds=['i-recovered']
        )
        self.assertTrue(any(
            item.get('aws_instance_id') == 'i-recovered'
            for item in snapshots
        ))
        self.assertNotIn('aws_instance_client_token', job['resources'])
        self.assertEqual(job['status'], 'destroyed')

    def test_cleanup_fails_closed_when_client_token_match_is_ambiguous(self):
        ec2 = MagicMock()
        sts = MagicMock()
        sts.get_caller_identity.return_value = {'Account': '123456789012'}
        ec2.describe_instances.return_value = {'Reservations': [{
            'Instances': [
                {'InstanceId': 'i-one'},
                {'InstanceId': 'i-two'},
            ],
        }]}
        session = FakeSession(ec2=ec2, sts=sts)
        resources = {
            'provider': 'aws',
            'aws_profile': 'default',
            'aws_account_id': '123456789012',
            'region': 'us-east-2',
            'aws_instance_client_token': 'client-token-1',
            'aws_subnet_id': 'subnet-1',
            'aws_vpc_id': 'vpc-1',
        }
        job = {
            'id': 'job123',
            'status': 'provisioning',
            'plan': {'aws_profile': 'default', 'region': 'us-east-2'},
            'resources': copy.deepcopy(resources),
        }

        with self.assertRaisesRegex(RuntimeError, 'matched 2 instances'):
            aws.destroy_resources(job, aws_session=session)

        self.assertEqual(job['status'], 'provisioning')
        self.assertEqual(job['resources'], resources)
        ec2.terminate_instances.assert_not_called()
        ec2.delete_subnet.assert_not_called()
        ec2.delete_vpc.assert_not_called()


if __name__ == '__main__':
    unittest.main()
