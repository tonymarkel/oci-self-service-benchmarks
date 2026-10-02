"""Stateful EC2 lifecycle tests; all mutation calls validate against botocore."""

import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from botocore.session import Session
from botocore.exceptions import ClientError
from botocore.validate import validate_parameters

from app import main
from app.deathstarbench_contract import (
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    K3S_RUNTIME_JOURNAL_KEY,
)
from app.deathstarbench_distributed import (
    DISTRIBUTED_NETWORK_QUALIFICATION_KEY,
)
from app.providers import aws
from app.resource_inventory import ResourceInventoryError, load_role_node_inventory
from tests.test_aws_provider import FakeSession


PIN = {'image_id': 'ami-0123456789abcdef0', 'owner_id': '123456789012',
       'name': 'Rocky-9-EC2-Base-9.6', 'creation_date': '2026-09-01T00:00:00.000Z',
       'product_code': 'rocky9marketplace', 'architecture': 'x86_64',
       'root_device_name': '/dev/sda1'}
PUBLIC_KEY = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4f benchmark'


def candidate_plan():
    return {'provider': 'aws', 'region': 'us-east-2', 'aws_profile': 'default',
            'shape': 'm7i.2xlarge', 'availability_zone': 'us-east-2a',
            'ocpus': 2, 'memory_gb': 8,
            'benchmarks': ['deathstarbench'], 'deathstarbench': {
                'topology_id': 'distributed_tiered_v1', 'runtime_id': 'k3s_v1',
                'workload': 'social_network'}}


class StatefulEC2:
    def __init__(self):
        self.data = {kind: {} for kind in aws._DSB_KINDS}
        self.calls = []
        self.on_create = None
        self.lose_response = None
        self.lose_delete_response = None
        self.extra_nics = []
        self.default_sg = None
        self.default_acl = None
        self.nic_read_override = None
        self.pending_instances = False
        self.attach_after_wait = True
        self.delay_public_address = False
        self.delay_boot_mapping = False
        self.delayed_boot_mappings = {}
        self.hidden_igw_tag_filter_reads = 0
        self.create_response_override = None
        self.on_wait = None
        self.wait_calls = []
        self.service = Session().get_service_model('ec2')

    def get_waiter(self, name):
        def wait(**kwargs):
            self.wait_calls.append((name, copy.deepcopy(kwargs)))
            if self.on_wait:
                self.on_wait(name, kwargs)
            if name == 'instance_running':
                for rid in kwargs['InstanceIds']:
                    instance = self.data['instance'][rid]
                    instance['State']['Name'] = 'running'
                    if self.attach_after_wait:
                        instance['NetworkInterfaces'][0]['Attachment']['Status'] = 'attached'
                    if rid in self.delayed_boot_mappings:
                        instance['BlockDeviceMappings'] = self.delayed_boot_mappings.pop(rid)
                    role = next(t['Value'] for t in instance['Tags'] if t['Key'] == 'benchmark-role')
                    if self.delay_public_address and role not in ('instance-database', 'instance-cache'):
                        instance['PublicIpAddress'] = '192.0.2.' + rid.rsplit('-', 1)[-1]
                        instance['NetworkInterfaces'][0]['Association'] = {'PublicIp': instance['PublicIpAddress']}
            elif name == 'instance_stopped':
                for rid in kwargs['InstanceIds']:
                    self.data['instance'][rid]['State']['Name'] = 'stopped'
        return SimpleNamespace(wait=wait)

    def _matches(self, item, filters):
        fields = {'vpc-id': 'VpcId', 'subnet-id': 'SubnetId', 'group-id': 'GroupId',
                  'internet-gateway-id': 'InternetGatewayId', 'allocation-id': 'AllocationId',
                  'nat-gateway-id': 'NatGatewayId', 'key-pair-id': 'KeyPairId',
                  'route-table-id': 'RouteTableId', 'volume-id': 'VolumeId',
                  'instance-id': 'InstanceId'}
        tags = {t['Key']: t['Value'] for t in item.get('Tags', [])}
        for f in filters:
            name, values = f['Name'], f['Values']
            if name.startswith('tag:'):
                if tags.get(name[4:]) not in values:
                    return False
            elif name == 'attachment.vpc-id':
                if not any(a['VpcId'] in values for a in item.get('Attachments', [])):
                    return False
            elif item.get(fields[name]) not in values:
                return False
        return True

    def __getattr__(self, name):
        def call(**request):
            operation = ''.join(part.capitalize() for part in name.split('_'))
            validate_parameters(request, self.service.operation_model(operation).input_shape)
            self.calls.append((name, copy.deepcopy(request)))
            if name == 'describe_images':
                return {'Images': [{
                    'ImageId': PIN['image_id'], 'OwnerId': PIN['owner_id'], 'Name': PIN['name'],
                    'CreationDate': PIN['creation_date'], 'Architecture': PIN['architecture'],
                    'RootDeviceName': PIN['root_device_name'], 'State': 'available',
                    'RootDeviceType': 'ebs', 'VirtualizationType': 'hvm', 'EnaSupport': True,
                    'ProductCodes': [{'ProductCodeId': PIN['product_code'], 'ProductCodeType': 'marketplace'}]}]}
            if name == 'describe_instance_types':
                return {'InstanceTypes': [{'InstanceType': shape,
                    'ProcessorInfo': {'SupportedArchitectures': ['x86_64']},
                    'VCpuInfo': {'DefaultVCpus': 4 if shape == 'm7i.xlarge' else 2},
                    'MemoryInfo': {'SizeInMiB': 16384 if shape == 'm7i.xlarge' else 8192},
                    'Hypervisor': 'nitro', 'SupportedVirtualizationTypes': ['hvm']}
                    for shape in request['InstanceTypes']]}
            if name == 'describe_availability_zones':
                return {'AvailabilityZones': [{'ZoneName': 'us-east-2a', 'State': 'available'}]}
            if name == 'describe_instance_type_offerings':
                return {'InstanceTypeOfferings': [{'InstanceType': shape, 'Location': 'us-east-2a'}
                    for shape in ('m7i.large', 'm7i.xlarge', 'm7i.2xlarge')]}
            if name == 'describe_network_interfaces':
                nics = list(self.extra_nics)
                for i in self.data['instance'].values():
                    for primary in i['NetworkInterfaces']:
                        nic = copy.deepcopy(primary)
                        nic.update(VpcId=i['VpcId'], SubnetId=i['SubnetId'],
                                   PrivateIpAddress=i['PrivateIpAddress'], Groups=i['SecurityGroups'])
                        nic['Attachment'].update(InstanceId=i['InstanceId'], InstanceOwnerId='123456789012')
                        if self.nic_read_override:
                            self.nic_read_override(nic)
                        nics.append(nic)
                for n in self.data['nat'].values():
                    nics.extend({'NetworkInterfaceId': a['NetworkInterfaceId']} for a in n['NatGatewayAddresses'])
                return {'NetworkInterfaces': copy.deepcopy(nics)}
            if name == 'describe_network_acls':
                return {'NetworkAcls': [copy.deepcopy(self.default_acl)] if self.default_acl else []}
            if name == 'describe_vpc_endpoints':
                return {'VpcEndpoints': []}
            if name in ('describe_vpc_peering_connections', 'describe_transit_gateway_vpc_attachments', 'describe_vpn_gateways'):
                return {}
            for kind, (describe, result, id_field, create, create_result) in aws._DSB_KINDS.items():
                if name == describe:
                    items = [copy.deepcopy(i) for i in self.data[kind].values()
                             if self._matches(i, request.get('Filters', request.get('Filter', [])))]
                    if (kind == 'igw' and self.hidden_igw_tag_filter_reads > 0
                            and any(f.get('Name', '').startswith('tag:')
                                    for f in request.get('Filters', []))):
                        self.hidden_igw_tag_filter_reads -= 1
                        items = []
                    if kind == 'sg' and self.default_sg and self._matches(self.default_sg, request.get('Filters', [])):
                        items.append(copy.deepcopy(self.default_sg))
                    return {result: [{'Instances': items}] if kind == 'instance' else items}
                if name != create:
                    continue
                if self.on_create:
                    self.on_create(kind, request)
                rid = kind + '-' + str(len(self.data[kind]) + 1)
                item = {k: copy.deepcopy(v) for k, v in request.items()
                        if k not in ('TagSpecifications', 'MinCount', 'MaxCount')}
                item[id_field] = rid
                item['Tags'] = copy.deepcopy(request['TagSpecifications'][0]['Tags'])
                role = next(t['Value'] for t in item['Tags'] if t['Key'] == 'benchmark-role')
                if kind == 'vpc':
                    self.default_sg = {'GroupId': 'sg-default', 'GroupName': 'default', 'VpcId': rid,
                        'IpPermissions': [{'IpProtocol': '-1', 'UserIdGroupPairs': [
                            {'GroupId': 'sg-default', 'UserId': '123456789012'}]}],
                        'IpPermissionsEgress': [{'IpProtocol': '-1', 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}]}
                    self.default_acl = {'NetworkAclId': 'acl-default', 'VpcId': rid, 'IsDefault': True,
                        'Associations': [], 'Entries': [
                            {'RuleNumber': number, 'Protocol': '-1', 'RuleAction': action,
                             'Egress': egress, 'CidrBlock': '0.0.0.0/0'}
                            for egress in (False, True) for number, action in ((100, 'allow'), (32767, 'deny'))]}
                if kind == 'subnet':
                    self.default_acl['Associations'].append({'NetworkAclId': 'acl-default',
                        'NetworkAclAssociationId': 'aclassoc-' + rid, 'SubnetId': rid})
                if kind == 'key':
                    item['PublicKey'] = request['PublicKeyMaterial']
                if kind == 'sg':
                    item.update(IpPermissions=[], IpPermissionsEgress=[{
                        'IpProtocol': '-1', 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}])
                if kind == 'route':
                    item.update(Routes=[{'DestinationCidrBlock': '10.240.0.0/16', 'GatewayId': 'local'}], Associations=[])
                if kind == 'igw':
                    item.update(Attachments=[], OwnerId='123456789012')
                if kind == 'nat':
                    subnet = self.data['subnet'][request['SubnetId']]
                    item.update(VpcId=subnet['VpcId'], State='available', NatGatewayAddresses=[{
                        'AllocationId': request['AllocationId'], 'NetworkInterfaceId': 'eni-nat'}])
                    self.data['eip'][request['AllocationId']]['NetworkInterfaceId'] = 'eni-nat'
                if kind == 'volume':
                    item['Attachments'] = []
                if kind == 'instance':
                    nic = request['NetworkInterfaces'][0]
                    item.update(VpcId=self.data['subnet'][nic['SubnetId']]['VpcId'],
                                SubnetId=nic['SubnetId'], PrivateIpAddress=nic['PrivateIpAddress'],
                                SecurityGroups=[{'GroupId': g} for g in nic['Groups']],
                                State={'Name': 'running'}, NetworkInterfaces=[{'NetworkInterfaceId': 'eni-' + rid,
                                    'Attachment': {'AttachmentId': 'eni-attach-' + rid,
                                        'DeleteOnTermination': True, 'DeviceIndex': 0, 'Status': 'attached'}}])
                    if nic['AssociatePublicIpAddress'] and not self.delay_public_address:
                        item['PublicIpAddress'] = '192.0.2.' + str(len(self.data['instance']) + 1)
                        item['NetworkInterfaces'][0]['Association'] = {'PublicIp': item['PublicIpAddress']}
                    if self.pending_instances:
                        item['State']['Name'] = 'pending'
                        item['NetworkInterfaces'][0]['Attachment']['Status'] = 'attaching'
                    boot = 'boot-' + rid
                    self.data['volume'][boot] = {'VolumeId': boot, 'Size': 50, 'VolumeType': 'gp3',
                        'Iops': request['BlockDeviceMappings'][0]['Ebs']['Iops'],
                        'Throughput': request['BlockDeviceMappings'][0]['Ebs']['Throughput'],
                        'Encrypted': True, 'Tags': request['TagSpecifications'][1]['Tags'],
                        'Attachments': [{'InstanceId': rid}]}
                    item['BlockDeviceMappings'] = [{'DeviceName': PIN['root_device_name'],
                        'Ebs': {'VolumeId': boot, 'DeleteOnTermination': True}}]
                    item['RootDeviceName'] = PIN['root_device_name']
                    if self.delay_boot_mapping:
                        item['State']['Name'] = 'pending'
                        item['NetworkInterfaces'][0]['Attachment']['Status'] = 'attaching'
                        self.delayed_boot_mappings[rid] = item['BlockDeviceMappings']
                        item['BlockDeviceMappings'] = []
                self.data[kind][rid] = item
                if self.lose_response == role:
                    self.lose_response = None
                    raise ConnectionError('accepted response was lost')
                response = ({create_result: [copy.deepcopy(item)] if kind == 'instance'
                             else copy.deepcopy(item)} if create_result else copy.deepcopy(item))
                if self.create_response_override:
                    response = self.create_response_override(kind, copy.deepcopy(response))
                return response
            if name == 'attach_internet_gateway':
                self.data['igw'][request['InternetGatewayId']]['Attachments'] = [{'VpcId': request['VpcId']}]
            elif name == 'detach_internet_gateway':
                self.data['igw'][request['InternetGatewayId']]['Attachments'] = []
            elif name == 'create_route':
                self.data['route'][request['RouteTableId']]['Routes'].append({k: v for k, v in request.items() if k != 'RouteTableId'})
            elif name == 'associate_route_table':
                table = self.data['route'][request['RouteTableId']]
                table['Associations'].append({'SubnetId': request['SubnetId'],
                    'RouteTableAssociationId': 'assoc-' + request['SubnetId']})
            elif name == 'disassociate_route_table':
                for table in self.data['route'].values():
                    table['Associations'] = [a for a in table['Associations'] if a['RouteTableAssociationId'] != request['AssociationId']]
            elif name == 'authorize_security_group_ingress':
                self.data['sg'][request['GroupId']]['IpPermissions'].extend(request['IpPermissions'])
            elif name == 'revoke_security_group_ingress':
                self.data['sg'][request['GroupId']]['IpPermissions'] = []
            elif name == 'attach_volume':
                self.data['volume'][request['VolumeId']]['Attachments'] = [{
                    'InstanceId': request['InstanceId'], 'Device': request['Device'], 'State': 'attached'}]
                self.data['instance'][request['InstanceId']]['BlockDeviceMappings'].append({
                    'DeviceName': request['Device'], 'Ebs': {'VolumeId': request['VolumeId'], 'DeleteOnTermination': False}})
            elif name == 'stop_instances':
                for rid in request['InstanceIds']:
                    self.data['instance'][rid]['State']['Name'] = 'stopping'
            elif name == 'terminate_instances':
                for rid in request['InstanceIds']:
                    instance = self.data['instance'].pop(rid)
                    for disk in instance['BlockDeviceMappings']:
                        vid = disk['Ebs']['VolumeId']
                        if disk['Ebs']['DeleteOnTermination']:
                            self.data['volume'].pop(vid)
                        else:
                            self.data['volume'][vid]['Attachments'] = []
            elif name.startswith('delete_') or name == 'release_address':
                matches = [(k, rid) for k, values in self.data.items() for rid in values
                           if rid in request.values()]
                if len(matches) != 1:
                    raise AssertionError((name, request, matches))
                kind, rid = matches[0]
                self.data[kind].pop(rid)
                if kind == 'subnet':
                    self.default_acl['Associations'] = [a for a in self.default_acl['Associations'] if a['SubnetId'] != rid]
                if kind == 'vpc':
                    self.default_acl = None
                    self.default_sg = None
                if kind == 'nat':
                    for eip in self.data['eip'].values():
                        eip.pop('NetworkInterfaceId', None)
                if self.lose_delete_response == kind:
                    self.lose_delete_response = None
                    raise ConnectionError('accepted delete response lost')
            else:
                raise AssertionError((name, request))
            return {}
        return call


class AwsDistributedLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.ec2 = StatefulEC2()
        self.session = FakeSession(ec2=self.ec2, sts=SimpleNamespace(
            get_caller_identity=lambda: {'Account': '123456789012'}))
        self.job = {'id': 'awsdsb123456', 'resources': {}, 'plan': candidate_plan()}

    def provision(self, **kwargs):
        return aws.provision_distributed_deathstarbench_candidate(self.job, candidate_plan(),
            public_key=PUBLIC_KEY, support_image=copy.deepcopy(PIN), application_image=copy.deepcopy(PIN),
            aws_session=self.session, **kwargs)

    def test_full_topology_is_persisted_before_first_create(self):
        snapshots = []
        self.ec2.on_create = lambda *_: snapshots.append(copy.deepcopy(self.job['resources']))
        self.provision()
        planned = load_role_node_inventory(snapshots[0])
        self.assertEqual(len(planned.nodes), 5)
        self.assertTrue(all(n.lifecycle_status == 'planned' and n.shape and n.private_addresses for n in planned.nodes))
        self.assertEqual(planned.node('database').storage[0].size_gb, 100)
        self.assertEqual(len(self.ec2.data['subnet']), 3)
        self.assertEqual(len(self.ec2.data['instance']), 5)
        self.assertEqual(len(self.ec2.data['nat']), 1)
        inventory = load_role_node_inventory(self.job['resources'])
        self.assertEqual(inventory.node('database').public_addresses, ())
        self.assertEqual(inventory.node('cache').public_addresses, ())
        self.assertIsNone(inventory.node('database').storage[0].device)

    def test_candidate_ec2_clients_disable_sdk_retries(self):
        self.provision()
        aws.validate_distributed_deathstarbench_candidate(
            self.job,
            aws_session=self.session,
        )
        aws.destroy_distributed_deathstarbench_candidate(
            self.job,
            aws_session=self.session,
        )
        configs = [
            config
            for service, _region, config in self.session.client_configs
            if service == 'ec2'
        ]
        self.assertTrue(configs)
        self.assertTrue(all(config.retries['total_max_attempts'] == 1
                            for config in configs))

    def test_application_capacity_must_match_selected_shape_before_writes(self):
        for field, value in (('ocpus', 99), ('memory_gb', 999)):
            with self.subTest(field=field):
                self.setUp()
                plan = candidate_plan()
                plan[field] = value
                with self.assertRaisesRegex(ValueError, 'must exactly match'):
                    aws.provision_distributed_deathstarbench_candidate(
                        self.job,
                        plan,
                        public_key=PUBLIC_KEY,
                        support_image=copy.deepcopy(PIN),
                        application_image=copy.deepcopy(PIN),
                        aws_session=self.session,
                    )
                self.assertFalse(any(
                    name.startswith(('create_', 'run_', 'import_', 'allocate_'))
                    for name, _request in self.ec2.calls
                ))

    def test_image_pin_drift_fails_before_writes(self):
        pin = {**PIN, 'creation_date': 'unexpected'}
        with self.assertRaisesRegex(ValueError, 'approved pin'):
            aws.provision_distributed_deathstarbench_candidate(self.job, candidate_plan(),
                support_image=pin, application_image=PIN, public_key=PUBLIC_KEY, aws_session=self.session)
        self.assertFalse(any(name.startswith(('create_', 'run_', 'import_', 'allocate_')) for name, _ in self.ec2.calls))

    def test_official_rocky_public_image_pin_requires_exact_owner_and_publicity(self):
        pin = {
            **PIN,
            'owner_id': aws.AWS_ROCKY_OFFICIAL_OWNER_ID,
            'product_code': None,
        }

        class ImageClient:
            public = True

            def describe_images(self, **_request):
                return {'Images': [{
                    'ImageId': pin['image_id'],
                    'OwnerId': pin['owner_id'],
                    'Name': pin['name'],
                    'CreationDate': pin['creation_date'],
                    'Architecture': pin['architecture'],
                    'RootDeviceName': pin['root_device_name'],
                    'State': 'available',
                    'RootDeviceType': 'ebs',
                    'VirtualizationType': 'hvm',
                    'EnaSupport': True,
                    'Public': self.public,
                    'ProductCodes': [],
                }]}

        client = ImageClient()
        self.assertEqual(aws._dsb_pin_image(client, pin, 'x86_64'), pin)
        client.public = False
        with self.assertRaisesRegex(ValueError, 'approved pin'):
            aws._dsb_pin_image(client, pin, 'x86_64')
        with self.assertRaisesRegex(ValueError, 'invalid'):
            aws._dsb_pin_image(
                client,
                {**pin, 'owner_id': '123456789012'},
                'x86_64',
            )

    def test_compact_provider_entrypoint_refuses_distributed_route(self):
        with self.assertRaisesRegex(ValueError, 'public distributed lifecycle'):
            aws.provision(self.job, candidate_plan(), public_key=PUBLIC_KEY, aws_session=self.session)
        self.assertEqual(self.ec2.calls, [])

    def test_accepted_response_loss_reconciles_without_duplicate_creates(self):
        self.ec2.lose_response = 'instance-database'
        self.provision()
        self.assertEqual(sum(n == 'run_instances' for n, _ in self.ec2.calls), 5)
        self.provision()
        self.assertEqual(sum(n == 'run_instances' for n, _ in self.ec2.calls), 5)

    def test_igw_create_response_persists_id_before_tag_index_visibility(self):
        self.ec2.hidden_igw_tag_filter_reads = 100
        self.provision()
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        self.assertEqual(graph['igw']['id'], 'igw-1')
        self.assertEqual(graph['igw']['status'], 'running')
        self.assertEqual(sum(name == 'create_internet_gateway'
                             for name, _ in self.ec2.calls), 1)
        # Once the response is accepted, every subsequent lookup uses the
        # persisted exact ID rather than the eventually-consistent tag index.
        igw_reads = [request for name, request in self.ec2.calls
                     if name == 'describe_internet_gateways']
        accepted = next(i for i, request in enumerate(igw_reads)
                        if request.get('Filters') == aws._dsb_filters(self.job['id'], 'igw'))
        self.assertTrue(any(request.get('Filters') == [{
            'Name': 'internet-gateway-id', 'Values': ['igw-1'],
        }] for request in igw_reads[accepted + 1:]))

    def test_malformed_igw_create_response_is_not_persisted_as_identity(self):
        self.ec2.hidden_igw_tag_filter_reads = 100
        def corrupt(kind, response):
            if kind == 'igw':
                response['InternetGateway']['Tags'].append({
                    'Key': 'foreign', 'Value': 'tag',
                })
            return response
        self.ec2.create_response_override = corrupt
        with patch.object(aws.time, 'sleep'), self.assertRaisesRegex(
                ResourceInventoryError, 'igw create cannot yet be reconciled'):
            self.provision()
        entry = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']['igw']
        self.assertEqual(entry, {'status': 'create_ambiguous'})

    def test_missing_igw_create_response_stays_ambiguous_without_id(self):
        self.ec2.hidden_igw_tag_filter_reads = 100
        self.ec2.create_response_override = lambda kind, response: (
            {} if kind == 'igw' else response
        )
        with patch.object(aws.time, 'sleep'), self.assertRaisesRegex(
                ResourceInventoryError, 'igw create cannot yet be reconciled'):
            self.provision()
        entry = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']['igw']
        self.assertEqual(entry, {'status': 'create_ambiguous'})

    def test_pending_eni_waits_then_attaches_without_replaying_accepted_launch(self):
        self.ec2.pending_instances = True
        self.ec2.lose_response = 'instance-database'
        observed_waits = []
        def before_wait(name, request):
            if name == 'instance_running':
                observed_waits.append(copy.deepcopy(self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']))
        self.ec2.on_wait = before_wait
        self.provision()
        self.assertEqual(sum(n == 'run_instances' for n, _ in self.ec2.calls), 5)
        self.assertTrue(any(g['instance-database'].get('id') and g['instance-database']['status'] == 'creating'
                            for g in observed_waits))
        self.assertTrue(all(n.lifecycle_status == 'running' for n in load_role_node_inventory(self.job['resources']).nodes))

    def test_delayed_ec2_boot_mapping_waits_then_verifies_without_replaying_launch(self):
        self.ec2.delay_boot_mapping = True
        self.ec2.lose_response = 'instance-control'
        self.provision()
        self.assertEqual(sum(n == 'run_instances' for n, _ in self.ec2.calls), 5)
        # One readiness wait observes each delayed root mapping, followed by
        # the provider's existing final running-state waiter for each node.
        self.assertEqual(sum(name == 'instance_running' for name, _ in self.ec2.wait_calls), 10)
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        for role in aws.AWS_DSB_ADDRESSES:
            instance = self.ec2.data['instance'][graph['instance-' + role]['id']]
            self.assertEqual(instance['BlockDeviceMappings'][0]['DeviceName'], PIN['root_device_name'])
            self.assertTrue(instance['BlockDeviceMappings'][0]['Ebs']['DeleteOnTermination'])

    def test_empty_boot_mapping_is_only_allowed_on_waiting_reconciliation_path(self):
        self.provision()
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        instance = self.ec2.data['instance'][graph['instance-control']['id']]
        instance['BlockDeviceMappings'] = []
        self.assert_cleanup_read_only_failure('boot disk deletion contract')

    def test_contradictory_boot_mapping_is_rejected_before_readiness_wait(self):
        self.provision()
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        instance = self.ec2.data['instance'][graph['instance-control']['id']]
        original = copy.deepcopy(instance['BlockDeviceMappings'])
        original_waits = len(self.ec2.wait_calls)
        for mode in ('wrong-device', 'delete-false', 'missing-volume-id', 'unexpected-disk'):
            with self.subTest(mode=mode):
                instance['BlockDeviceMappings'] = copy.deepcopy(original)
                root = instance['BlockDeviceMappings'][0]
                if mode == 'wrong-device':
                    root['DeviceName'] = '/dev/foreign'
                elif mode == 'delete-false':
                    root['Ebs']['DeleteOnTermination'] = False
                elif mode == 'missing-volume-id':
                    root['Ebs'].pop('VolumeId')
                else:
                    instance['BlockDeviceMappings'].append({
                        'DeviceName': '/dev/foreign',
                        'Ebs': {'VolumeId': 'vol-foreign', 'DeleteOnTermination': False},
                    })
                with self.assertRaisesRegex(ResourceInventoryError,
                                            'boot disk deletion contract|unexpected attached disk'):
                    aws.recover_distributed_deathstarbench_candidate(
                        self.job, aws_session=self.session, wait_for_attachments=True)
                self.assertEqual(len(self.ec2.wait_calls), original_waits)
        instance['BlockDeviceMappings'] = original

    def test_read_only_recovery_waits_for_ambiguous_pending_instance(self):
        self.provision()
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        instance = self.ec2.data['instance'][graph['instance-database']['id']]
        instance['State']['Name'] = 'pending'
        instance['NetworkInterfaces'][0]['Attachment']['Status'] = 'attaching'
        graph['instance-database'].pop('id')
        graph['instance-database']['status'] = 'create_ambiguous'
        aws._dsb_save(self.job, None)
        aws.recover_distributed_deathstarbench_candidate(self.job, aws_session=self.session)
        self.assertEqual(graph['instance-database']['id'], instance['InstanceId'])
        self.assertEqual(graph['instance-database']['status'], 'running')
        self.assertEqual(sum(n == 'run_instances' for n, _ in self.ec2.calls), 5)

    def test_attaching_timeout_retains_identity_and_resume_does_not_recreate(self):
        self.ec2.pending_instances = True
        self.ec2.attach_after_wait = False
        with patch.object(aws.time, 'sleep'), self.assertRaisesRegex(ResourceInventoryError, 'not yet attached'):
            self.provision()
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        self.assertTrue(graph['instance-control']['id'])
        self.assertEqual(graph['instance-control']['status'], 'creating')
        self.ec2.attach_after_wait = True
        self.provision()
        self.assertEqual(sum(n == 'run_instances' for n, _ in self.ec2.calls), 5)

    def test_cleanup_still_rejects_transient_eni_attachment_without_waiting(self):
        self.provision()
        instance = next(iter(self.ec2.data['instance'].values()))
        instance['NetworkInterfaces'][0]['Attachment']['Status'] = 'attaching'
        waits = len(self.ec2.wait_calls)
        self.assert_cleanup_read_only_failure('primary ENI attachment/deletion contract')
        self.assertEqual(len(self.ec2.wait_calls), waits)

    def test_public_address_readiness_waits_without_replaying_launch(self):
        self.ec2.delay_public_address = True
        self.ec2.lose_response = 'instance-application'
        self.provision()
        self.assertEqual(sum(n == 'run_instances' for n, _ in self.ec2.calls), 5)
        inventory = load_role_node_inventory(self.job['resources'])
        for role in ('control', 'application', 'load-generator'):
            self.assertTrue(inventory.node(role).public_addresses)
        for role in ('database', 'cache'):
            self.assertEqual(inventory.node(role).public_addresses, ())

    def test_missing_required_public_addresses_block_cleanup(self):
        self.provision()
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        for role in ('control', 'application', 'load-generator'):
            instance = self.ec2.data['instance'][graph['instance-' + role]['id']]
            original = copy.deepcopy(instance)
            for missing in ('instance-address', 'eni-association', 'both'):
                with self.subTest(role=role, missing=missing):
                    instance.clear()
                    instance.update(copy.deepcopy(original))
                    if missing != 'eni-association':
                        instance.pop('PublicIpAddress')
                    if missing != 'instance-address':
                        instance['NetworkInterfaces'][0].pop('Association')
                    self.assert_cleanup_read_only_failure('required public address/primary ENI association')
            instance.clear()
            instance.update(original)

    def test_unexpected_private_role_public_addresses_block_cleanup(self):
        self.provision()
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        for role in ('database', 'cache'):
            instance = self.ec2.data['instance'][graph['instance-' + role]['id']]
            original = copy.deepcopy(instance)
            for unexpected in ('instance-address', 'eni-association'):
                with self.subTest(role=role, unexpected=unexpected):
                    instance.clear()
                    instance.update(copy.deepcopy(original))
                    if unexpected == 'instance-address':
                        instance['PublicIpAddress'] = '192.0.2.99'
                    else:
                        instance['NetworkInterfaces'][0]['Association'] = {'PublicIp': '192.0.2.99'}
                    self.assert_cleanup_read_only_failure('unexpected public address/primary ENI association')
            instance.clear()
            instance.update(original)

    def test_independent_primary_eni_public_association_must_match(self):
        self.provision()
        for mode in ('missing-public', 'unexpected-private', 'mismatched-public'):
            with self.subTest(mode=mode):
                def drift(nic):
                    if mode == 'missing-public' and nic.get('Association'):
                        nic.pop('Association')
                    elif mode == 'unexpected-private' and not nic.get('Association'):
                        nic['Association'] = {'PublicIp': '192.0.2.99'}
                    elif mode == 'mismatched-public' and nic.get('Association'):
                        nic['Association']['PublicIp'] = '192.0.2.99'
                self.ec2.nic_read_override = drift
                self.assert_cleanup_read_only_failure('primary ENI public association differs')

    def test_boot_gp3_performance_drift_blocks_cleanup(self):
        self.provision()
        boot = next(v for v in self.ec2.data['volume'].values() if v['Size'] == 50)
        for field, altered, original in (('Iops', 6000, 3000), ('Throughput', 250, 125)):
            with self.subTest(field=field):
                boot[field] = altered
                self.assert_cleanup_read_only_failure('boot volume configuration/attachment differs')
                boot[field] = original

    def test_runtime_projection_publishes_exact_ssh_routes_after_cloud_audit(self):
        self.provision()
        with self.assertRaisesRegex(ResourceInventoryError, 'have not been cloud-validated'):
            aws.distributed_deathstarbench_candidate_projection(self.job['resources'])
        before = len(self.ec2.calls)
        projection = aws.validate_distributed_deathstarbench_candidate(self.job, aws_session=self.session)
        self.assertFalse(any(n.startswith(('create_', 'run_', 'import_', 'authorize_', 'attach_'))
                             for n, _ in self.ec2.calls[before:]))
        self.assertEqual(projection['ssh_user'], 'rocky')
        self.assertEqual(projection['provider'], 'aws')
        self.assertEqual(projection['nodes']['database']['private_ip'], '10.240.3.11')
        self.assertEqual(projection['nodes']['cache']['private_ip'], '10.240.3.12')
        self.assertEqual(projection['nodes']['database']['host_key'], 'aws_dsb_database_private_ip')
        self.assertEqual(projection['nodes']['application']['jump_host_key'], 'aws_dsb_control_public_ip')
        self.assertIsNone(projection['nodes']['control']['jump_host_key'])
        self.assertEqual(projection['nodes']['load-generator']['host_key'], 'aws_dsb_load_generator_public_ip')
        self.assertEqual(projection['database_volume_id'],
            self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']['database-data']['id'])
        self.assertIsNone(self.job['resources']['aws_dsb_database_public_ip'])
        self.assertEqual(projection, aws.distributed_deathstarbench_candidate_projection(self.job['resources']))
        before = len(self.ec2.calls)
        projection['nodes']['database']['private_ip'] = '192.0.2.99'
        fresh = aws.distributed_deathstarbench_candidate_projection(self.job['resources'])
        self.assertEqual(fresh['nodes']['database']['private_ip'], '10.240.3.11')
        self.assertEqual(len(self.ec2.calls), before)

    def test_runtime_projection_does_not_publish_before_failed_cloud_audit(self):
        self.provision()
        self.ec2.extra_nics.append({'NetworkInterfaceId': 'eni-foreign'})
        with self.assertRaisesRegex(ResourceInventoryError, 'foreign network interface'):
            aws.validate_distributed_deathstarbench_candidate(self.job, aws_session=self.session)
        self.assertNotIn('ssh_user', self.job['resources'])
        self.assertNotIn('aws_dsb_database_volume_id', self.job['resources'])

    def test_published_runtime_alias_drift_is_not_repaired(self):
        self.provision()
        aws.validate_distributed_deathstarbench_candidate(self.job, aws_session=self.session)
        original = copy.deepcopy(self.job['resources'])
        for field in ('ssh_user', 'aws_dsb_control_public_ip', 'aws_dsb_database_private_ip',
                      'aws_dsb_database_volume_id'):
            with self.subTest(field=field):
                self.job['resources'] = copy.deepcopy(original)
                self.job['resources'][field] = 'foreign-value'
                before = len(self.ec2.calls)
                with self.assertRaisesRegex(ResourceInventoryError, 'published runtime SSH/storage aliases differ'):
                    aws.validate_distributed_deathstarbench_candidate(self.job, aws_session=self.session)
                self.assertEqual(len(self.ec2.calls), before)
                self.assertEqual(self.job['resources'][field], 'foreign-value')

    def test_runtime_validation_requires_complete_database_attachment(self):
        self.provision()
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        self.ec2.data['volume'][graph['database-data']['id']]['Attachments'] = []
        instance = self.ec2.data['instance'][graph['instance-database']['id']]
        instance['BlockDeviceMappings'] = [d for d in instance['BlockDeviceMappings']
                                          if d['DeviceName'] != aws.DATA_VOLUME_DEVICE]
        with self.assertRaisesRegex(ResourceInventoryError, 'database EBS attachment is missing'):
            aws.validate_distributed_deathstarbench_candidate(self.job, aws_session=self.session)
        self.assertNotIn('ssh_user', self.job['resources'])

    def test_runtime_validation_requires_complete_ingress_not_only_safe_subset(self):
        self.provision()
        group = next(iter(self.ec2.data['sg'].values()))
        group['IpPermissions'] = []
        with self.assertRaisesRegex(ResourceInventoryError, 'runtime ingress paths are incomplete'):
            aws.validate_distributed_deathstarbench_candidate(self.job, aws_session=self.session)
        self.assertNotIn('ssh_user', self.job['resources'])

    def test_deleted_predicate_accepts_verified_retained_tombstones_without_cloud_calls(self):
        self.provision()
        aws.validate_distributed_deathstarbench_candidate(self.job, aws_session=self.session)
        self.assertFalse(aws.distributed_deathstarbench_candidate_deleted(self.job))
        self.job['cleanup_error'] = 'a previous attempt failed'
        aws.destroy_distributed_deathstarbench_candidate(self.job, aws_session=self.session)
        aws.recover_distributed_deathstarbench_candidate(self.job, aws_session=self.session,
                                                        wait_for_attachments=False)
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        self.assertTrue(all(entry.get('id') for entry in graph.values()))
        before = len(self.ec2.calls)
        self.assertTrue(aws.distributed_deathstarbench_candidate_deleted(self.job))
        self.assertFalse(main.has_recoverable_resources(self.job))
        self.assertFalse(main.has_recoverable_resources_for_any_provider(self.job))
        self.assertEqual(len(self.ec2.calls), before)
        self.assertNotIn('cleanup_error', self.job)
        with self.assertRaisesRegex(ResourceInventoryError, 'running state'):
            aws.distributed_deathstarbench_candidate_projection(self.job['resources'])

    def test_deleted_predicate_rejects_partial_malformed_or_failed_terminal_state(self):
        self.provision()
        aws.destroy_distributed_deathstarbench_candidate(self.job, aws_session=self.session)
        original = copy.deepcopy(self.job)
        for mode in ('mixed-status', 'missing-entry', 'unknown-entry-field', 'cleanup-error', 'wrong-status'):
            with self.subTest(mode=mode):
                job = copy.deepcopy(original)
                graph = job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
                if mode == 'mixed-status':
                    graph['vpc']['status'] = 'delete_ambiguous'
                elif mode == 'missing-entry':
                    graph.pop('vpc')
                elif mode == 'unknown-entry-field':
                    graph['vpc']['foreign_resource_id'] = 'vpc-foreign'
                elif mode == 'cleanup-error':
                    job['cleanup_error'] = 'not confirmed'
                else:
                    job['status'] = 'destroying'
                self.assertFalse(aws.distributed_deathstarbench_candidate_deleted(job))
        self.assertFalse(aws.distributed_deathstarbench_candidate_deleted({'resources': None}))

    def test_deleted_predicate_rejects_foreign_top_level_ownership(self):
        self.provision()
        aws.validate_distributed_deathstarbench_candidate(
            self.job,
            aws_session=self.session,
        )
        aws.destroy_distributed_deathstarbench_candidate(
            self.job,
            aws_session=self.session,
        )
        self.job['resources'].update({
            K3S_RUNTIME_JOURNAL_KEY: {'state': 'cluster_ready'},
            DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY: {
                'state': 'workload_ready',
            },
            DEATHSTARBENCH_EXECUTION_JOURNAL_KEY: {
                'state': 'measurement_complete',
            },
            DISTRIBUTED_NETWORK_QUALIFICATION_KEY: {
                'schema_version': 3,
            },
        })
        self.assertTrue(
            aws.distributed_deathstarbench_candidate_deleted(self.job)
        )

        cases = {
            'compact-aws': {'instance_id': 'i-foreign'},
            'gcp': {
                'gcp_project_id': 'foreign-project',
                'gcp_resource_prefix': 'foreign-prefix',
            },
            'azure': {
                'azure_subscription_id': 'foreign-subscription',
                'azure_resource_group_name': 'foreign-group',
                'azure_resource_group_tags': {'managed-by': 'foreign'},
            },
            'arbitrary': {'foreign_resource_id': 'foreign-id'},
        }
        for label, extra in cases.items():
            with self.subTest(label=label):
                job = copy.deepcopy(self.job)
                job['resources'].update(extra)
                self.assertFalse(
                    aws.distributed_deathstarbench_candidate_deleted(job)
                )
                self.assertTrue(main.has_recoverable_resources(job))
                self.assertTrue(
                    main.has_recoverable_resources_for_any_provider(job)
                )

    def test_unknown_ambiguous_create_retains_all_dependencies(self):
        def fail(kind, request):
            if kind == 'nat':
                raise ConnectionError('not yet visible')
        self.ec2.on_create = fail
        with self.assertRaises(ConnectionError):
            self.provision()
        before = len(self.ec2.calls)
        with self.assertRaisesRegex(ResourceInventoryError, 'ambiguous'):
            aws.destroy_resources(self.job, aws_session=self.session)
        self.assertFalse(any(n.startswith('delete_') for n, _ in self.ec2.calls[before:]))

    def test_definitive_capacity_rejection_allows_partial_graph_cleanup(self):
        def fail(kind, request):
            if kind == 'instance':
                raise ClientError({'Error': {'Code': 'InsufficientInstanceCapacity'}}, 'RunInstances')
        self.ec2.on_create = fail
        with self.assertRaises(ClientError):
            self.provision()
        self.ec2.on_create = None
        aws.destroy_resources(self.job, aws_session=self.session)
        self.assertTrue(all(not values for values in self.ec2.data.values()))

    def test_foreign_security_group_ingress_blocks_every_delete(self):
        self.provision()
        group = next(iter(self.ec2.data['sg'].values()))
        group['IpPermissions'].append({'IpProtocol': '-1', 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]})
        before = len(self.ec2.calls)
        with self.assertRaisesRegex(ResourceInventoryError, 'ingress differs'):
            aws.destroy_resources(self.job, aws_session=self.session)
        self.assertFalse(any(n.startswith(('delete_', 'terminate_', 'revoke_')) for n, _ in self.ec2.calls[before:]))

    def test_inventory_tamper_blocks_cleanup(self):
        self.provision()
        self.job['resources']['deathstarbench_topology_fingerprint'] = 'sha256:' + '0' * 64
        with self.assertRaisesRegex(ResourceInventoryError, 'topology or inventory'):
            aws.destroy_resources(self.job, aws_session=self.session)

    def assert_cleanup_read_only_failure(self, pattern):
        before = len(self.ec2.calls)
        with self.assertRaisesRegex(ResourceInventoryError, pattern):
            aws.destroy_resources(self.job, aws_session=self.session)
        self.assertFalse(any(n.startswith(('delete_', 'terminate_', 'revoke_', 'detach_', 'release_'))
                             for n, _ in self.ec2.calls[before:]))

    def test_default_security_group_drift_blocks_every_delete(self):
        self.provision()
        original = copy.deepcopy(self.ec2.default_sg)
        for field in ('IpPermissions', 'IpPermissionsEgress'):
            with self.subTest(field=field):
                self.ec2.default_sg = copy.deepcopy(original)
                self.ec2.default_sg[field] = []
                self.assert_cleanup_read_only_failure('default VPC security group')

    def test_default_network_acl_entry_drift_blocks_every_delete(self):
        self.provision()
        original = copy.deepcopy(self.ec2.default_acl['Entries'])
        for mode in ('foreign-rule', 'duplicate-rule'):
            with self.subTest(mode=mode):
                self.ec2.default_acl['Entries'] = copy.deepcopy(original)
                if mode == 'foreign-rule':
                    self.ec2.default_acl['Entries'][0]['RuleNumber'] = 99
                else:
                    self.ec2.default_acl['Entries'][0] = copy.deepcopy(original[1])
                self.assert_cleanup_read_only_failure('default VPC network ACL')

    def test_default_network_acl_foreign_association_blocks_every_delete(self):
        self.provision()
        self.ec2.default_acl['Associations'][0]['SubnetId'] = 'subnet-foreign'
        self.assert_cleanup_read_only_failure('default VPC network ACL')

    def test_default_drift_after_initial_audit_blocks_delete_vpc(self):
        self.provision()
        delete_igw = self.ec2.delete_internet_gateway
        def introduce_drift(**request):
            result = delete_igw(**request)
            self.ec2.default_sg['IpPermissions'] = []
            return result
        with patch.object(self.ec2, 'delete_internet_gateway', introduce_drift):
            with self.assertRaisesRegex(ResourceInventoryError, 'default VPC security group'):
                aws.destroy_resources(self.job, aws_session=self.session)
        self.assertFalse(any(n == 'delete_vpc' for n, _ in self.ec2.calls))

    def test_primary_eni_delete_on_termination_drift_blocks_every_delete(self):
        self.provision()
        instance = next(iter(self.ec2.data['instance'].values()))
        instance['NetworkInterfaces'][0]['Attachment']['DeleteOnTermination'] = False
        self.assert_cleanup_read_only_failure('primary ENI attachment/deletion contract')

    def test_independent_primary_eni_attachment_identity_drift_blocks_every_delete(self):
        self.provision()
        self.ec2.nic_read_override = lambda nic: nic['Attachment'].update(InstanceId='i-foreign')
        self.assert_cleanup_read_only_failure('primary ENI attachment identity')

    def test_imported_key_mismatch_blocks_known_and_ambiguous_reconciliation(self):
        self.provision()
        key = next(iter(self.ec2.data['key'].values()))
        key['PublicKey'] = PUBLIC_KEY.replace('HR4f', 'HR4e')
        self.assert_cleanup_read_only_failure('imported SSH public key differs')
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        graph['key'].pop('id')
        graph['key']['status'] = 'create_ambiguous'
        self.assert_cleanup_read_only_failure('imported SSH public key differs')
        self.assertNotIn('id', graph['key'])
        self.assertTrue(all(request['IncludePublicKey'] is True
                            for name, request in self.ec2.calls if name == 'describe_key_pairs'))

    def test_imported_key_comments_are_not_part_of_key_identity(self):
        self.provision()
        key = next(iter(self.ec2.data['key'].values()))
        key['PublicKey'] = ' '.join(PUBLIC_KEY.split()[:2]) + ' AWS-returned-comment\n'
        aws.recover_distributed_deathstarbench_candidate(self.job, aws_session=self.session)

    def test_cleanup_deletes_exact_graph_and_keeps_tombstones(self):
        self.provision()
        aws.destroy_resources(self.job, aws_session=self.session)
        self.assertEqual(self.job['status'], 'destroyed')
        self.assertTrue(all(not values for values in self.ec2.data.values()))
        self.assertTrue(all(n.lifecycle_status == 'deleted' for n in load_role_node_inventory(self.job['resources']).nodes))
        aws.destroy_resources(self.job, aws_session=self.session)

    def test_foreign_nic_blocks_every_delete(self):
        self.provision()
        self.ec2.extra_nics.append({'NetworkInterfaceId': 'eni-foreign'})
        before = len(self.ec2.calls)
        with self.assertRaisesRegex(ResourceInventoryError, 'foreign network interface'):
            aws.destroy_resources(self.job, aws_session=self.session)
        self.assertFalse(any(n.startswith(('delete_', 'terminate_', 'revoke_')) for n, _ in self.ec2.calls[before:]))

    def test_load_generator_is_stopped_before_foreign_graph_refusal(self):
        self.provision()
        self.job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'measurement_started',
        }
        self.ec2.extra_nics.append({'NetworkInterfaceId': 'eni-foreign'})
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        loadgen_id = graph['instance-load-generator']['id']
        before = len(self.ec2.calls)

        with self.assertRaisesRegex(ResourceInventoryError, 'foreign network interface'):
            aws.destroy_resources(self.job, aws_session=self.session)

        mutations = [
            name for name, _ in self.ec2.calls[before:]
            if name.startswith(('stop_', 'terminate_', 'delete_', 'revoke_'))
        ]
        self.assertEqual(mutations, ['stop_instances'])
        self.assertEqual(
            self.ec2.data['instance'][loadgen_id]['State']['Name'],
            'stopped',
        )
        self.assertIn(
            DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
            self.job['resources'],
        )

    def test_load_generator_stop_is_replay_safe(self):
        self.provision()
        self.job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'measurement_started',
        }

        self.assertTrue(
            aws.quiesce_distributed_deathstarbench_load_generator(
                self.job, aws_session=self.session,
            )
        )
        self.assertFalse(
            aws.quiesce_distributed_deathstarbench_load_generator(
                self.job, aws_session=self.session,
            )
        )
        self.assertEqual(
            sum(name == 'stop_instances' for name, _ in self.ec2.calls),
            1,
        )

    def test_cleanup_accepts_ephemeral_public_address_release_after_quiesce(self):
        self.provision()
        self.job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'measurement_started',
        }
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        loadgen_id = graph['instance-load-generator']['id']

        def release_public_address(waiter_name, request):
            if waiter_name != 'instance_stopped':
                return
            self.assertEqual(request['InstanceIds'], [loadgen_id])
            loadgen = self.ec2.data['instance'][loadgen_id]
            loadgen.pop('PublicIpAddress', None)
            loadgen['NetworkInterfaces'][0].pop('Association', None)

        self.ec2.on_wait = release_public_address
        aws.destroy_resources(self.job, aws_session=self.session)

        self.assertEqual(self.job['status'], 'destroyed')
        self.assertTrue(all(not values for values in self.ec2.data.values()))
        self.assertTrue(
            aws.distributed_deathstarbench_candidate_deleted(self.job)
        )
        self.assertNotIn('ssh_user', self.job['resources'])
        self.assertNotIn(
            'aws_dsb_load_generator_public_ip',
            self.job['resources'],
        )

    def test_cleanup_recovers_quiesced_ephemeral_alias_transition(self):
        self.provision()
        aws.validate_distributed_deathstarbench_candidate(
            self.job,
            aws_session=self.session,
        )
        self.job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'measurement_started',
        }
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        entry = graph['instance-load-generator']
        loadgen = self.ec2.data['instance'].pop(entry['id'])
        for device in loadgen['BlockDeviceMappings']:
            if device['Ebs']['DeleteOnTermination']:
                self.ec2.data['volume'].pop(device['Ebs']['VolumeId'])
        entry['status'] = 'deleted'
        entry['public_addresses'] = []
        aws._dsb_save(self.job, None)

        # The launch-time alias is the one tolerated legacy difference. A
        # successful terminal cleanup withdraws all runtime aliases.
        self.assertIsInstance(
            self.job['resources']['aws_dsb_load_generator_public_ip'],
            str,
        )
        aws.destroy_resources(self.job, aws_session=self.session)

        self.assertEqual(self.job['status'], 'destroyed')
        self.assertTrue(all(not values for values in self.ec2.data.values()))
        self.assertNotIn(
            'aws_dsb_load_generator_public_ip',
            self.job['resources'],
        )
        self.assertTrue(
            aws.distributed_deathstarbench_candidate_deleted(self.job)
        )

    def test_load_generator_stop_rejects_changed_ownership(self):
        self.provision()
        self.job['resources'][DEATHSTARBENCH_EXECUTION_JOURNAL_KEY] = {
            'state': 'measurement_started',
        }
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        loadgen = self.ec2.data['instance'][
            graph['instance-load-generator']['id']
        ]
        next(
            tag for tag in loadgen['Tags']
            if tag['Key'] == 'benchmark-job'
        )['Value'] = 'foreign-job'

        with self.assertRaisesRegex(ResourceInventoryError, 'ownership tags'):
            aws.quiesce_distributed_deathstarbench_load_generator(
                self.job, aws_session=self.session,
            )

        self.assertFalse(any(
            name == 'stop_instances' for name, _ in self.ec2.calls
        ))

    def test_removed_ownership_tag_cannot_be_mistaken_for_absence(self):
        self.provision()
        instance = next(iter(self.ec2.data['instance'].values()))
        instance['Tags'] = []
        with self.assertRaisesRegex(ResourceInventoryError, 'ownership tags'):
            aws.destroy_resources(self.job, aws_session=self.session)

    def test_foreign_volume_attachment_blocks_cleanup(self):
        self.provision()
        volume = next(v for v in self.ec2.data['volume'].values() if v['Size'] == 100)
        volume['Attachments'][0]['InstanceId'] = 'foreign-instance'
        with self.assertRaisesRegex(ResourceInventoryError, 'foreign attachment'):
            aws.destroy_resources(self.job, aws_session=self.session)

    def test_delete_response_loss_can_resume_without_recreation(self):
        self.provision()
        self.ec2.lose_delete_response = 'nat'
        with self.assertRaises(ConnectionError):
            aws.destroy_resources(self.job, aws_session=self.session)
        aws.destroy_resources(self.job, aws_session=self.session)
        self.assertEqual(self.job['status'], 'destroyed')
        self.assertEqual(sum(n == 'create_nat_gateway' for n, _ in self.ec2.calls), 1)

    def test_loadgen_has_no_k3s_database_or_cache_ingress_path(self):
        self.provision()
        g = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        loadgen = g['sg-load-generator']['id']
        for role in ('control', 'database', 'cache'):
            rules = self.ec2.data['sg'][g['sg-' + role]['id']]['IpPermissions']
            self.assertFalse(any(p.get('GroupId') == loadgen for r in rules for p in r.get('UserIdGroupPairs', [])))


if __name__ == '__main__':
    unittest.main()
