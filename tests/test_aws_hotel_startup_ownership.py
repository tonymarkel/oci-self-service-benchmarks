"""Strict startup path against the stateful EC2 SDK boundary."""
import copy
import hashlib
from types import SimpleNamespace
import unittest

from botocore.exceptions import ClientError
from app.providers import aws
from tests.test_aws_provider import FakeSession
from tests.test_aws_deathstarbench_lifecycle import StatefulEC2, candidate_plan, PIN, PUBLIC_KEY


class StrictEC2:
    """Expose canonical service IDs around the historical lifecycle fixture."""
    def __init__(self):
        self.cloud = StatefulEC2()
        self.ids = {}
        self.calls = []
        self.before = None
        self.application_capacity = None

    def _convert(self, value, reverse=False):
        if isinstance(value, dict):
            return {key: self._convert(item, reverse) for key, item in value.items()}
        if isinstance(value, list):
            return [self._convert(item, reverse) for item in value]
        if not isinstance(value, str):
            return value
        if reverse:
            return {v: k for k, v in self.ids.items()}.get(value, value)
        prefixes = {'instance': 'i', 'volume': 'vol', 'boot': 'vol', 'route': 'rtb',
                    'eip': 'eipalloc', 'key': 'key', 'eni-attach': 'eni-attach',
                    'eni': 'eni', 'acl': 'acl', 'sg': 'sg', 'vpc': 'vpc',
                    'subnet': 'subnet', 'nat': 'nat', 'igw': 'igw'}
        for prefix in sorted(prefixes, key=len, reverse=True):
            suffix = value.removeprefix(prefix + '-')
            if suffix != value and (suffix.isdecimal() or suffix in ('default', 'nat')
                                    or suffix.startswith('instance-')):
                self.ids.setdefault(value, prefixes[prefix] + '-' + hashlib.sha256(value.encode()).hexdigest()[:17])
                return self.ids[value]
        return value

    def get_waiter(self, name):
        waiter = self.cloud.get_waiter(name)
        return SimpleNamespace(wait=lambda **kwargs: waiter.wait(**self._convert(kwargs, True)))

    def __getattr__(self, name):
        def call(**kwargs):
            self.calls.append((name, copy.deepcopy(kwargs)))
            if self.before:
                self.before(name, kwargs)
            result = getattr(self.cloud, name)(**self._convert(kwargs, True))
            if name == 'describe_instance_types' and self.application_capacity:
                for row in result['InstanceTypes']:
                    if row['InstanceType'] == 'm7i.2xlarge':
                        row['VCpuInfo']['DefaultVCpus'], row['MemoryInfo']['SizeInMiB'] = self.application_capacity
            if name == 'create_vpc':
                self.cloud.data['route']['route-default'] = {
                    'RouteTableId': 'route-default', 'VpcId': next(iter(self.cloud.data['vpc'])),
                    'Associations': [{'Main': True, 'RouteTableAssociationId': 'assoc-default'}],
                    'Routes': [{'DestinationCidrBlock': '10.240.0.0/16', 'GatewayId': 'local'}]}
            elif name == 'delete_vpc':
                self.cloud.data['route'].pop('route-default', None)
            return self._convert(result)
        return call


class AwsStartupOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.ec2 = StrictEC2()
        self.session = FakeSession(ec2=self.ec2, sts=SimpleNamespace(
            get_caller_identity=lambda: {'Account': '123456789012'}))
        self.job = {'id': '123456abcdef', 'resources': {}, 'plan': candidate_plan()}
        self.saved = []

    def provision(self, **kwargs):
        return aws.provision_distributed_deathstarbench_candidate(self.job, candidate_plan(),
            public_key=PUBLIC_KEY, support_image=copy.deepcopy(PIN), application_image=copy.deepcopy(PIN),
            aws_session=self.session, strict_create_identity=True,
            persist=lambda job: self.saved.append(copy.deepcopy(job)), **kwargs)

    def test_every_create_id_is_persisted_before_first_exact_read_and_no_role_discovery(self):
        # Catches any successful create path adopting its ID through tag lookup.
        def before(name, kwargs):
            if name.startswith('describe_'):
                for item in kwargs.get('Filters', kwargs.get('Filter', [])):
                    self.assertNotEqual(item['Name'], 'tag:benchmark-role')
                    if item['Name'].endswith('-id') and item['Name'] not in ('vpc-id', 'attachment.vpc-id'):
                        explicit = {entry.get('id') for entry in self.saved[-1]['resources'][aws.AWS_DSB_GRAPH_KEY]['graph'].values()}
                        if name not in ('describe_network_interfaces', 'describe_network_acls', 'describe_vpc_endpoints'):
                            for rid in item['Values']:
                                if rid not in explicit:
                                    self.assertTrue(name == 'describe_volumes' and rid.startswith('vol-'))
        self.ec2.before = before
        self.provision()
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        self.assertEqual(len(graph), 21)
        self.assertTrue(all(row.get('id') for row in graph.values()))

    def test_response_loss_never_recovers_a_tag_perfect_live_resource(self):
        self.ec2.cloud.lose_response = 'vpc'
        with self.assertRaises(ConnectionError):
            self.provision()
        graph = self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        self.assertEqual(graph['vpc'], {'status': 'create_ambiguous'})
        self.assertEqual(len(self.ec2.cloud.data['vpc']), 1)
        before = len(self.ec2.calls)
        with self.assertRaises(ValueError):
            aws.destroy_distributed_deathstarbench_candidate(self.job, aws_session=self.session,
                strict_cleanup=True, expected_cleanup_identity={})
        self.assertFalse(any(name.startswith(('delete', 'stop', 'revoke', 'detach'))
                             for name, _ in self.ec2.calls[before:]))
        self.assertFalse(any(f['Name'] == 'tag:benchmark-role' for _, args in self.ec2.calls
                             for f in args.get('Filters', [])))

    def test_confirmed_rejection_emits_original_producer_identity(self):
        def reject(kind, request):
            if kind == 'instance':
                raise ClientError({'Error': {'Code': 'InsufficientInstanceCapacity'}}, 'RunInstances')
        self.ec2.cloud.on_create = reject
        events = []
        def resolution(job, **event):
            events.append(copy.deepcopy(event))
            self.assertEqual(self.saved[-1]['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']['instance-control']['status'], 'creating')
            self.assertEqual(job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']['instance-control']['status'], 'planned')
        with self.assertRaises(ClientError):
            self.provision(persist_create_resolution=resolution)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['resource_key'], 'instance-control')
        self.assertEqual(events[0]['result'], 'rejected-before-allocation')
        self.assertEqual(events[0]['classification'], 'InsufficientInstanceCapacity')

    def test_first_contract_persistence_failure_authorizes_no_create(self):
        def fail(_job):
            raise OSError('disk unavailable')
        with self.assertRaises(OSError):
            aws.provision_distributed_deathstarbench_candidate(self.job, candidate_plan(),
                public_key=PUBLIC_KEY, support_image=PIN, application_image=PIN,
                aws_session=self.session, strict_create_identity=True, persist=fail)
        self.assertFalse(any(self.ec2.cloud.data.values()))

    def test_preflight_freezes_implicit_children_and_strict_destroy_keeps_exact_absence(self):
        # Catches child adoption after seal and parent-only absence claims.
        self.provision()
        boundary = aws.prepare_distributed_candidate_cleanup(self.job, aws_session=self.session)
        self.assertEqual(len(boundary['explicit']), 21)
        self.assertEqual(len(boundary['implicit']), 19)
        original = copy.deepcopy(boundary)
        aws.destroy_distributed_deathstarbench_candidate(self.job, aws_session=self.session,
            strict_cleanup=True, expected_cleanup_identity=boundary)
        aws.attest_startup_candidate_absence(self.job, original, aws_session=self.session)
        self.assertTrue(aws.distributed_deathstarbench_candidate_deleted(self.job))
        self.assertFalse(any(f['Name'] == 'tag:benchmark-role' for _, args in self.ec2.calls
                             for f in args.get('Filters', [])))

    def test_replaced_implicit_child_refuses_before_quiesce_or_revoke(self):
        self.provision()
        boundary = aws.prepare_distributed_candidate_cleanup(self.job, aws_session=self.session)
        self.ec2.cloud.default_sg['GroupId'] = 'sg-999'
        self.ec2.cloud.default_sg['IpPermissions'][0]['UserIdGroupPairs'][0]['GroupId'] = 'sg-999'
        before = len(self.ec2.calls)
        with self.assertRaises(ValueError):
            aws.destroy_distributed_deathstarbench_candidate(self.job, aws_session=self.session,
                strict_cleanup=True, expected_cleanup_identity=boundary)
        self.assertFalse(any(name.startswith(('delete', 'stop', 'revoke', 'detach', 'terminate'))
                             for name, _ in self.ec2.calls[before:]))

    def test_malformed_create_response_never_uses_tag_lookup(self):
        self.ec2.cloud.create_response_override = lambda kind, response: {} if kind == 'vpc' else response
        with self.assertRaises(ValueError):
            self.provision()
        self.assertEqual(self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']['vpc'], {'status': 'create_ambiguous'})
        self.assertFalse(any(f['Name'] == 'tag:benchmark-role' for _, args in self.ec2.calls
                             for f in args.get('Filters', [])))

    def test_successful_instance_response_with_contradictory_parent_is_not_accepted(self):
        def response(kind, value):
            if kind == 'instance':
                value['Instances'][0]['VpcId'] = 'vpc-fffffffffffffffff'
            return value
        self.ec2.cloud.create_response_override = response
        with self.assertRaises(ValueError):
            self.provision()
        self.assertNotIn('id', self.job['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']['instance-control'])


if __name__ == '__main__':
    unittest.main()
