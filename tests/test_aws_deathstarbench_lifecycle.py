"""Stateful EC2 lifecycle tests; all mutation calls validate against botocore."""

import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from botocore.session import Session
from botocore.exceptions import ClientError
from botocore.validate import validate_parameters

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
                    role = next(t['Value'] for t in instance['Tags'] if t['Key'] == 'benchmark-role')
                    if self.delay_public_address and role not in ('instance-database', 'instance-cache'):
                        instance['PublicIpAddress'] = '192.0.2.' + rid.rsplit('-', 1)[-1]
                        instance['NetworkInterfaces'][0]['Association'] = {'PublicIp': instance['PublicIpAddress']}
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
                    item['Attachments'] = []
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
                self.data[kind][rid] = item
                if self.lose_response == role:
                    self.lose_response = None
                    raise ConnectionError('accepted response was lost')
                return {create_result: [copy.deepcopy(item)] if kind == 'instance' else copy.deepcopy(item)} if create_result else copy.deepcopy(item)
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
                    'InstanceId': request['InstanceId'], 'Device': request['Device']}]
                self.data['instance'][request['InstanceId']]['BlockDeviceMappings'].append({
                    'DeviceName': request['Device'], 'Ebs': {'VolumeId': request['VolumeId'], 'DeleteOnTermination': False}})
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

    def test_image_pin_drift_fails_before_writes(self):
        pin = {**PIN, 'creation_date': 'unexpected'}
        with self.assertRaisesRegex(ValueError, 'approved pin'):
            aws.provision_distributed_deathstarbench_candidate(self.job, candidate_plan(),
                support_image=pin, application_image=PIN, public_key=PUBLIC_KEY, aws_session=self.session)
        self.assertFalse(any(name.startswith(('create_', 'run_', 'import_', 'allocate_')) for name, _ in self.ec2.calls))

    def test_normal_provider_entrypoint_keeps_release_gate_closed(self):
        with self.assertRaisesRegex(ValueError, 'not released'):
            aws.provision(self.job, candidate_plan(), public_key=PUBLIC_KEY, aws_session=self.session)
        self.assertEqual(self.ec2.calls, [])

    def test_accepted_response_loss_reconciles_without_duplicate_creates(self):
        self.ec2.lose_response = 'instance-database'
        self.provision()
        self.assertEqual(sum(n == 'run_instances' for n, _ in self.ec2.calls), 5)
        self.provision()
        self.assertEqual(sum(n == 'run_instances' for n, _ in self.ec2.calls), 5)

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
