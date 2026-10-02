"""Stateful OCI candidate lifecycle tests; no credentials or cloud requests."""

import copy
from datetime import datetime, timezone
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import oci as sdk

from app import main
from app.providers import oci


COMPARTMENT = 'ocid1.compartment.oc1..test'
IMAGE = 'ocid1.image.oc1.iad.oraclelinux9'
INPUTS = {
    'compartment_id': COMPARTMENT, 'availability_domain': 'test:US-ASHBURN-AD-1',
    'region': 'us-ashburn-1', 'shape': 'VM.Standard.E5.Flex', 'architecture': 'x86_64',
    'ocpus': 4, 'memory_gb': 32, 'application_image_id': IMAGE, 'support_image_id': IMAGE,
    'defined_tags': {
        'CostCenter': {'Department': 'Sales'},
        'app-instance': {'object-read': 'YOUMUSTUSETHIS'},
    },
    'public_key': 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEtest benchmark',
}


def response(data, etag='1'):
    return SimpleNamespace(data=copy.deepcopy(data), headers={'etag': etag})


def not_found():
    return sdk.exceptions.ServiceError(404, 'NotAuthorizedOrNotFound', {}, 'missing')


class Cloud:
    def __init__(self):
        self.items = {}
        self.calls = []
        self.rules = {}
        self.fail_after = None
        self.fail_before = None
        self.delete_fail_after = None
        self.on_mutation = None
        self.sequence = 0
        self.clients = {kind: Client(self, kind) for kind in ('compute', 'network', 'block')}
        self.put('image', {'id': IMAGE, 'operating_system': 'Oracle Linux', 'operating_system_version': '9.6',
                           'display_name': 'Oracle-Linux-9.6-2026.09.01-0', 'compartment_id': None,
                           'lifecycle_state': 'AVAILABLE'})

    def put(self, kind, body):
        body = copy.deepcopy(body)
        self.sequence += 1
        body.setdefault('id', f'ocid1.{kind.replace("_", "")}.oc1.iad.{self.sequence}')
        body.setdefault('time_created', datetime.now(timezone.utc).isoformat())
        body.setdefault('lifecycle_state', 'RUNNING' if kind == 'instance' else 'ATTACHED' if kind.endswith('_attachment') else 'AVAILABLE')
        self.items.setdefault(kind, {})[body['id']] = body
        return body

    def create(self, method, model, **kwargs):
        self.calls.append((method, kwargs))
        if self.on_mutation:
            self.on_mutation(method)
        if self.fail_before == method:
            self.fail_before = None
            raise ConnectionError('request not delivered')
        kind = {'launch_instance': 'instance', 'attach_volume': 'volume_attachment'}.get(method, method.removeprefix('create_'))
        body = sdk.util.to_dict(model)
        if kind == 'instance':
            vnic_details = body.pop('create_vnic_details')
            source = body.pop('source_details')
            body['image_id'] = source['image_id']
        data = self.put(kind, body)
        if kind == 'vcn':
            for child, field in (('route_table', 'default_route_table_id'), ('security_list', 'default_security_list_id'), ('dhcp_options', 'default_dhcp_options_id')):
                data[field] = self.put(child, {'vcn_id': data['id'], 'compartment_id': COMPARTMENT,
                    'display_name': 'Default ' + child})['id']
        elif kind == 'instance':
            vnic = self.put('vnic', {**vnic_details, 'is_primary': True, 'compartment_id': COMPARTMENT,
                'availability_domain': INPUTS['availability_domain'],
                'public_ip': f'198.51.100.{self.sequence}' if vnic_details.pop('assign_public_ip') else None})
            self.put('vnic_attachment', {'compartment_id': COMPARTMENT, 'instance_id': data['id'], 'vnic_id': vnic['id'],
                'availability_domain': INPUTS['availability_domain'], 'subnet_id': vnic_details['subnet_id']})
            boot = self.put('boot_volume', {'compartment_id': COMPARTMENT, 'image_id': source['image_id'],
                'availability_domain': INPUTS['availability_domain'], 'size_in_gbs': 50})
            self.put('boot_volume_attachment', {'id': data['id'],
                'compartment_id': COMPARTMENT, 'availability_domain': INPUTS['availability_domain'],
                'instance_id': data['id'], 'boot_volume_id': boot['id']})
        if kind == 'volume_attachment':
            data['attachment_type'] = data.pop('type')
            data['compartment_id'] = COMPARTMENT
            data['availability_domain'] = INPUTS['availability_domain']
        if self.fail_after == method:
            self.fail_after = None
            raise ConnectionError('response lost after accepted request')
        return response(data)

    def delete(self, method, identity, **kwargs):
        self.calls.append((method, kwargs))
        if self.on_mutation:
            self.on_mutation(method)
        if kwargs.get('if_match') != '1':
            raise AssertionError('All deletes must carry an ETag')
        kind = {'terminate_instance': 'instance', 'detach_volume': 'volume_attachment'}.get(method, method.removeprefix('delete_'))
        if kind == 'instance':
            assert kwargs['preserve_boot_volume']
            for attachment in self.items['vnic_attachment'].values():
                if attachment['instance_id'] == identity:
                    self.items['vnic'].pop(attachment['vnic_id'])
                    attachment['lifecycle_state'] = 'DETACHED'
            for attachment in self.items['boot_volume_attachment'].values():
                if attachment['instance_id'] == identity:
                    attachment['lifecycle_state'] = 'DETACHED'
        elif kind == 'vcn':
            for children in self.items.values():
                for key, item in list(children.items()):
                    if item.get('vcn_id') == identity:
                        del children[key]
        self.items[kind].pop(identity, None)
        if self.delete_fail_after == method:
            self.delete_fail_after = None
            raise ConnectionError('delete accepted, response lost')
        return response(None)


class Client:
    def __init__(self, cloud, kind):
        self.cloud = cloud
        self.kind = kind
        self._config = {'region': INPUTS['region']}

    def __getattr__(self, name):
        if name.startswith('create_') or name in {'launch_instance', 'attach_volume'}:
            return lambda model, **kwargs: self.cloud.create(name, model, **kwargs)
        if name.startswith('delete_') or name in {'terminate_instance', 'detach_volume'}:
            return lambda identity, **kwargs: self.cloud.delete(name, identity, **kwargs)
        if name.startswith('get_'):
            def get(identity):
                item = self.cloud.items.get(name[4:], {}).get(identity)
                if item is None:
                    raise not_found()
                return response(item)
            return get
        if name.startswith('list_'):
            def listed(**kwargs):
                kind = 'dhcp_options' if name == 'list_dhcp_options' else name[5:-1]
                result = self.cloud.items.get(kind, {}).values()
                return response([item for item in result if all(item.get(key) == value for key, value in kwargs.items())])
            return listed
        raise AttributeError(name)

    def list_shapes(self, **kwargs):
        return response([{'shape': shape, 'is_flexible': True,
                          'ocpu_options': {'min': 1, 'max': 94},
                          'memory_options': {'min_in_g_bs': 1, 'max_in_g_bs': 1024, 'min_per_ocpu_in_gbs': 1, 'max_per_ocpu_in_gbs': 64}}
                         for shape in (oci.SUPPORT_SHAPE, 'VM.Standard.A2.Flex')])

    def list_image_shape_compatibility_entries(self, **kwargs):
        return response([{'shape': oci.SUPPORT_SHAPE}, {'shape': 'VM.Standard.A2.Flex'}])

    def add_network_security_group_security_rules(self, identity, details, **kwargs):
        self.cloud.calls.append(('add_network_security_group_security_rules', kwargs))
        if self.cloud.on_mutation:
            self.cloud.on_mutation('add_network_security_group_security_rules')
        self.cloud.rules[identity] = sdk.util.to_dict(details)['security_rules']
        if self.cloud.fail_after == 'add_network_security_group_security_rules':
            self.cloud.fail_after = None
            raise ConnectionError('rules response lost')
        return response(None)

    def list_network_security_group_security_rules(self, network_security_group_id, **kwargs):
        return response(self.cloud.rules.get(network_security_group_id, []))

    def list_network_security_group_vnics(self, network_security_group_id, **kwargs):
        return response([{'vnic_id': item['id']} for item in self.cloud.items.get('vnic', {}).values()
                         if network_security_group_id in item.get('nsg_ids', [])])

    def list_private_ips(self, subnet_id, **kwargs):
        return response([{'vnic_id': item['id'], 'is_primary': True} for item in self.cloud.items.get('vnic', {}).values()
                         if item.get('subnet_id') == subnet_id])

    def instance_action(self, identity, action, **kwargs):
        self.cloud.calls.append((
            'instance_action',
            {'identity': identity, 'action': action, **kwargs},
        ))
        if action != 'STOP' or kwargs.get('if_match') != '1':
            raise AssertionError((identity, action, kwargs))
        self.cloud.items['instance'][identity]['lifecycle_state'] = 'STOPPED'
        return response(self.cloud.items['instance'][identity])


class OCIDistributedLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.cloud = Cloud()
        self.job = {'id': '012345abcdef', 'resources': {}}
        self.snapshots = []
        self.inputs = copy.deepcopy(INPUTS)

    def test_root_tenancy_is_a_valid_compartment_scope(self):
        oci._validate_inputs({
            **self.inputs,
            'compartment_id': 'ocid1.tenancy.oc1..test',
        })

    def persist(self, job):
        # Ensures SDK timestamps and models have not leaked into persisted JSON.
        self.snapshots.append(json.loads(json.dumps(job)))

    def provision(self):
        return oci.provision_distributed_deathstarbench_candidate(self.job, inputs=self.inputs, clients=self.cloud.clients, persist=self.persist)

    def destroy(self):
        return oci.destroy_distributed_deathstarbench_candidate(self.job, clients=self.cloud.clients, persist=self.persist)

    @property
    def contract(self):
        return self.job['resources'][oci.CONTRACT_KEY]

    def test_full_candidate_graph_inventory_and_cleanup(self):
        inventory = self.provision()
        self.assertEqual(len(inventory.nodes), 5)
        self.assertEqual(inventory.node('database').storage[0].size_gb, 256)
        self.assertEqual(inventory.node('database').storage[0].device, oci.DATABASE_DEVICE)
        self.assertFalse(inventory.node('database').public_addresses)
        self.assertEqual(inventory.node('database').private_addresses, ('10.240.3.11',))
        self.assertEqual(len(self.cloud.items['network_security_group']), 5)
        self.destroy()
        self.assertEqual(self.contract['status'], 'deleted')
        for kind in ('instance', 'vcn', 'volume', 'boot_volume', 'vnic', 'subnet', 'network_security_group', 'route_table'):
            self.assertFalse(self.cloud.items[kind], kind)
        deletes = [method for method, _ in self.cloud.calls if method.startswith('delete_') or method in ('detach_volume', 'terminate_instance')]
        self.assertEqual(deletes[0], 'detach_volume')
        self.assertEqual(deletes[-1], 'delete_vcn')
        count = len(self.cloud.calls)
        self.destroy()
        self.assertEqual(len(self.cloud.calls), count)

    def test_read_only_tenant_throttles_retry_with_bounded_backoff(self):
        client = self.cloud.clients['network']
        original = client.list_network_security_group_vnics
        attempts = 0

        def throttled(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise sdk.exceptions.ServiceError(
                    429,
                    'TooManyRequests',
                    {},
                    'tenant read throttle',
                )
            return original(*args, **kwargs)

        with (
            patch.object(
                client,
                'list_network_security_group_vnics',
                side_effect=throttled,
            ),
            patch.object(oci.time, 'sleep') as sleep,
        ):
            result = oci._list(
                client,
                'list_network_security_group_vnics',
                network_security_group_id='ocid1.nsg.oc1.iad.test',
            )

        self.assertEqual(result, [])
        self.assertEqual(attempts, 3)
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list],
            list(oci.READ_THROTTLE_RETRY_DELAYS[:2]),
        )

    def test_read_only_tenant_throttle_retry_is_bounded(self):
        client = self.cloud.clients['network']
        error = sdk.exceptions.ServiceError(
            429,
            'TooManyRequests',
            {},
            'persistent tenant read throttle',
        )
        with (
            patch.object(
                client,
                'list_network_security_group_vnics',
                side_effect=error,
            ) as read,
            patch.object(oci.time, 'sleep') as sleep,
            self.assertRaises(sdk.exceptions.ServiceError),
        ):
            oci._list(
                client,
                'list_network_security_group_vnics',
                network_security_group_id='ocid1.nsg.oc1.iad.test',
            )

        self.assertEqual(
            read.call_count,
            len(oci.READ_THROTTLE_RETRY_DELAYS) + 1,
        )
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list],
            list(oci.READ_THROTTLE_RETRY_DELAYS),
        )

    def test_persistent_audit_throttle_prevents_all_cleanup_mutations(self):
        self.provision()
        client = self.cloud.clients['network']
        error = sdk.exceptions.TransientServiceError(
            429,
            'TooManyRequests',
            {},
            'persistent tenant audit throttle',
        )
        before = len(self.cloud.calls)
        with (
            patch.object(
                client,
                'list_network_security_group_vnics',
                side_effect=error,
            ) as read,
            patch.object(oci.time, 'sleep'),
            self.assertRaises(sdk.exceptions.TransientServiceError),
        ):
            self.destroy()

        self.assertEqual(
            read.call_count,
            len(oci.READ_THROTTLE_RETRY_DELAYS) + 1,
        )
        self.assertEqual(len(self.cloud.calls), before)
        self.assertEqual(self.contract['status'], 'ready')

    def test_mutation_throttle_is_not_retried(self):
        self.provision()
        client = self.cloud.clients['compute']
        error = sdk.exceptions.TransientServiceError(
            429,
            'TooManyRequests',
            {},
            'mutation throttle with unknown acceptance',
        )
        with (
            patch.object(client, 'detach_volume', side_effect=error) as detach,
            patch.object(oci.time, 'sleep') as sleep,
            self.assertRaises(sdk.exceptions.TransientServiceError),
        ):
            self.destroy()

        detach.assert_called_once()
        self.assertFalse(sleep.called)
        self.assertIsInstance(
            detach.call_args.kwargs['retry_strategy'],
            sdk.retry.NoneRetryStrategy,
        )
        self.assertEqual(
            self.contract['entries']['database-attachment']['status'],
            'deleting',
        )

    def test_all_intents_and_inventory_persist_before_first_mutation(self):
        def verify(method):
            saved = self.snapshots[-1]['resources']
            contract = saved[oci.CONTRACT_KEY]
            self.assertEqual(len(contract['entries']), 21)
            self.assertEqual(len(saved['role_node_inventory']['nodes']), 5)
            for entry in contract['entries'].values():
                self.assertTrue(entry['retry_token'])
            if method == 'create_vcn':
                self.assertEqual(contract['entries']['vcn']['status'], 'creating')
                self.assertTrue(contract['entries']['vcn']['attempted_at'])
        self.cloud.on_mutation = verify
        self.provision()

    def test_every_taggable_create_carries_exact_immutable_defined_tags(self):
        self.provision()
        expected = INPUTS['defined_tags']
        for key, entry in self.contract['entries'].items():
            with self.subTest(key=key):
                body = entry['spec']['body']
                if entry['spec']['kind'] == 'volume_attachment':
                    self.assertNotIn('defined_tags', body)
                else:
                    self.assertEqual(body['defined_tags'], expected)
                    self.assertIsNot(body['defined_tags'], expected)

    def test_defined_tags_are_exact_validated_inputs_and_resume_contract(self):
        for invalid in (
            None,
            {'bad.name': {'key': 'value'}},
            {'namespace': {}},
            {'namespace': {'bad key': 'value'}},
            {'namespace': {'key': 1}},
        ):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                    oci.LifecycleError, 'defined_tags'):
                oci._validate_inputs({**self.inputs, 'defined_tags': invalid})
        self.provision()
        changed = copy.deepcopy(self.inputs)
        changed['defined_tags']['CostCenter']['Department'] = 'Marketing'
        with self.assertRaisesRegex(oci.LifecycleError, 'change or recreate'):
            oci.provision_distributed_deathstarbench_candidate(
                self.job, inputs=changed, clients=self.cloud.clients,
                persist=self.persist,
            )

    def test_resume_ready_does_not_duplicate_resources_or_rules(self):
        self.provision()
        calls = len(self.cloud.calls)
        self.provision()
        self.assertEqual(calls, len(self.cloud.calls))

    def test_create_response_loss_reconciles_without_replay(self):
        for method in ('create_vcn', 'create_subnet', 'launch_instance', 'attach_volume'):
            with self.subTest(method=method):
                self.setUp()
                self.cloud.fail_after = method
                with self.assertRaises(ConnectionError):
                    self.provision()
                self.provision()
                expected = 3 if method == 'create_subnet' else 5 if method == 'launch_instance' else 1
                self.assertEqual(sum(name == method for name, _ in self.cloud.calls), expected)
                self.destroy()

    def test_missing_ambiguous_create_never_replays_or_claims_clean(self):
        self.cloud.fail_before = 'create_vcn'
        with self.assertRaises(ConnectionError):
            self.provision()
        with self.assertRaisesRegex(oci.LifecycleError, 'ambiguous'):
            self.provision()
        with self.assertRaisesRegex(oci.LifecycleError, 'ambiguous'):
            self.destroy()
        self.assertNotEqual(self.contract['status'], 'deleted')
        self.assertEqual(len(self.cloud.calls), 1)

    def test_rules_response_loss_reconciles_exact_set(self):
        self.cloud.fail_after = 'add_network_security_group_security_rules'
        with self.assertRaises(ConnectionError):
            self.provision()
        self.provision()
        self.assertEqual(sum(name == 'add_network_security_group_security_rules' for name, _ in self.cloud.calls), 5)

    def test_empty_unconfirmed_nsg_rules_do_not_strand_owned_cleanup(self):
        failures = (
            ConnectionError('rule request outcome unknown'),
            sdk.exceptions.ServiceError(
                403,
                'NotAuthorized',
                {},
                'rule request explicitly rejected',
            ),
        )
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                self.setUp()

                def reject_rules(method):
                    if method == 'add_network_security_group_security_rules':
                        raise failure

                self.cloud.on_mutation = reject_rules
                with self.assertRaises(type(failure)):
                    self.provision()
                attempted = [
                    entry
                    for entry in self.contract['entries'].values()
                    if entry.get('rules_attempted_at')
                ]
                self.assertEqual(len(attempted), 1)
                self.assertNotIn('rules_ready', attempted[0])
                self.assertEqual(
                    self.cloud.rules.get(attempted[0]['id'], []),
                    [],
                )
                self.cloud.on_mutation = None
                self.destroy()
                self.assertEqual(self.contract['status'], 'deleted')

    def test_partial_or_extra_rules_refuse_resume_and_cleanup(self):
        self.provision()
        nsg = self.contract['entries']['nsg-database']['id']
        self.cloud.rules[nsg].append({'direction': 'INGRESS', 'protocol': 'all', 'source': '0.0.0.0/0'})
        before = len(self.cloud.calls)
        with self.assertRaisesRegex(oci.LifecycleError, 'NSG rules'):
            self.provision()
        with self.assertRaisesRegex(oci.LifecycleError, 'NSG rules'):
            self.destroy()
        self.assertEqual(before, len(self.cloud.calls))

    def test_exact_nsg_rules_cover_manifest_data_and_management_channels(self):
        self.provision()
        addresses = oci.PRIVATE_ADDRESSES
        cluster = ('control', 'application', 'database', 'cache')
        expected = {
            (destination, addresses[source] + '/32', '17', 8472)
            for source in cluster for destination in cluster if source != destination
        }
        expected.update(('control', addresses[source] + '/32', '6', 6443)
                        for source in ('application', 'database', 'cache'))
        expected.update({
            ('application', addresses['load-generator'] + '/32', '6', 8080),
            ('cache', addresses['application'] + '/32', '6', 6379),
            ('cache', addresses['application'] + '/32', '6', 11211),
            ('database', addresses['application'] + '/32', '6', 27017),
        })
        expected.update((role, '0.0.0.0/0', '6', 22) for role in ('control', 'application', 'load-generator'))
        expected.update((role, addresses[source] + '/32', '6', 22)
                        for role in ('database', 'cache') for source in ('control', 'application'))
        actual = []
        for role in addresses:
            rules = [oci._canonical_rule(rule) for rule in self.cloud.rules[self.contract['entries'][f'nsg-{role}']['id']]]
            self.assertEqual([rule for rule in rules if rule['direction'] == 'EGRESS'], [
                {'direction': 'EGRESS', 'protocol': 'all', 'destination': '0.0.0.0/0',
                 'destination_type': 'CIDR_BLOCK', 'is_stateless': False}])
            for rule in rules:
                if rule['direction'] != 'INGRESS':
                    continue
                self.assertEqual(rule['source_type'], 'CIDR_BLOCK')
                self.assertFalse(rule['is_stateless'])
                options = rule['tcp_options'] if rule['protocol'] == '6' else rule['udp_options']
                port_range = options['destination_port_range']
                self.assertEqual(port_range['min'], port_range['max'])
                actual.append((role, rule['source'], rule['protocol'], port_range['min']))
        self.assertEqual(set(actual), expected)
        self.assertEqual(len(actual), len(expected), 'Duplicate allow rules are not part of the exact contract.')

    def test_nsg_rules_forbid_undeclared_data_loadgen_and_public_paths(self):
        manifest = oci._manifest(self.inputs).as_dict()
        forbidden = [
            ('database', 'load-generator', '6', 22), ('cache', 'load-generator', '6', 22),
            ('control', 'load-generator', '6', 6443), ('control', 'load-generator', '17', 8472),
            ('database', 'load-generator', '6', 27017), ('cache', 'load-generator', '6', 6379),
            ('cache', 'database', '6', 6379), ('cache', 'control', '6', 11211),
            ('database', 'cache', '6', 27017), ('application', 'load-generator', '6', 5000),
        ]
        for destination, source, protocol, port in forbidden:
            with self.subTest(destination=destination, source=source, port=port):
                rules = oci._rules(destination, manifest)
                self.assertFalse(any(rule['direction'] == 'INGRESS' and rule['source'] == oci.PRIVATE_ADDRESSES[source] + '/32'
                    and rule['protocol'] == protocol
                    and (rule.get('tcp_options') or rule.get('udp_options'))['destination_port_range']['min'] == port
                    for rule in rules))
        for role in oci.PRIVATE_ADDRESSES:
            public = [rule for rule in oci._rules(role, manifest) if rule['direction'] == 'INGRESS' and rule['source'] == '0.0.0.0/0']
            self.assertEqual(len(public), 1 if role in oci.PUBLIC_ROLES else 0)
            for rule in public:
                self.assertEqual(rule['protocol'], '6')
                self.assertEqual(rule['tcp_options']['destination_port_range'], {'min': 22, 'max': 22})

    def test_confirmed_create_rejection_leaves_partial_graph_cleanable(self):
        for status, code in ((400, 'LimitExceeded'), (400, 'InvalidParameter'), (403, 'NotAuthorized'), (429, 'TooManyRequests')):
            with self.subTest(status=status, code=code):
                self.setUp()
                error = sdk.exceptions.ServiceError(status, code, {}, 'confirmed non-acceptance')
                with patch.object(self.cloud.clients['network'], 'create_nat_gateway', side_effect=error):
                    with self.assertRaises(sdk.exceptions.ServiceError):
                        self.provision()
                entry = self.contract['entries']['nat']
                self.assertEqual(entry['status'], 'planned')
                self.assertNotIn('id', entry)
                self.assertEqual(entry['last_create_rejection']['code'], code)
                self.assertEqual(self.snapshots[-1]['resources'][oci.CONTRACT_KEY]['entries']['nat']['status'], 'planned')
                self.assertEqual(len(self.cloud.items['vcn']), 1)
                self.assertEqual(len(self.cloud.items['internet_gateway']), 1)
                self.destroy()
                self.assertEqual(self.contract['status'], 'deleted')
                self.assertFalse(self.cloud.items['vcn'])
                self.assertFalse(self.cloud.items['internet_gateway'])
                self.assertFalse(any(method == 'delete_nat_gateway' for method, _ in self.cloud.calls))

    def test_ambiguous_service_errors_never_reset_or_replay_create_intent(self):
        for status, code in ((404, 'NotAuthorizedOrNotFound'), (403, 'NotAuthorizedOrNotFound'),
                             (400, 'UnknownRejection'), (409, 'Conflict'), (408, 'RequestTimeout'),
                             (500, 'InternalError'), (503, 'ServiceUnavailable')):
            with self.subTest(status=status, code=code):
                self.setUp()
                with patch.object(self.cloud.clients['network'], 'create_nat_gateway',
                                  side_effect=sdk.exceptions.ServiceError(status, code, {}, 'unknown acceptance')):
                    with self.assertRaises(sdk.exceptions.ServiceError):
                        self.provision()
                self.assertEqual(self.contract['entries']['nat']['status'], 'creating')
                self.assertNotIn('last_create_rejection', self.contract['entries']['nat'])
                before = len(self.cloud.calls)
                with self.assertRaisesRegex(oci.LifecycleError, 'ambiguous'):
                    self.provision()
                with self.assertRaisesRegex(oci.LifecycleError, 'ambiguous'):
                    self.destroy()
                self.assertEqual(before, len(self.cloud.calls))

    def test_mutating_creates_and_rule_additions_disable_automatic_sdk_replays(self):
        self.provision()
        for method, kwargs in self.cloud.calls:
            if method.startswith('create_') or method in {'launch_instance', 'attach_volume', 'add_network_security_group_security_rules'}:
                with self.subTest(method=method):
                    self.assertIsInstance(kwargs.get('retry_strategy'), sdk.retry.NoneRetryStrategy)

    def test_foreign_child_blocks_all_cleanup_mutations(self):
        for kind in ('subnet', 'route_table', 'security_list', 'local_peering_gateway', 'service_gateway'):
            with self.subTest(kind=kind):
                self.setUp()
                self.provision()
                self.cloud.put(kind, {'vcn_id': self.contract['entries']['vcn']['id'], 'compartment_id': COMPARTMENT})
                before = len(self.cloud.calls)
                with self.assertRaisesRegex(oci.LifecycleError, 'Foreign'):
                    self.destroy()
                self.assertEqual(before, len(self.cloud.calls))

    def test_load_generator_is_stopped_before_foreign_graph_refusal(self):
        self.provision()
        self.job['resources'][
            oci.DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
        ] = {'state': 'measurement_started'}
        self.cloud.put('local_peering_gateway', {
            'vcn_id': self.contract['entries']['vcn']['id'],
            'compartment_id': COMPARTMENT,
        })
        loadgen = self.contract['entries']['load-generator']
        before = len(self.cloud.calls)

        with self.assertRaisesRegex(oci.LifecycleError, 'Foreign'):
            self.destroy()

        mutations = [
            method for method, _ in self.cloud.calls[before:]
            if method == 'instance_action'
            or method.startswith('delete_')
            or method in {'detach_volume', 'terminate_instance'}
        ]
        self.assertEqual(mutations, ['instance_action'])
        self.assertEqual(
            self.cloud.items['instance'][loadgen['id']]['lifecycle_state'],
            'STOPPED',
        )
        self.assertIn(
            oci.DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
            self.job['resources'],
        )

    def test_load_generator_stop_is_replay_safe(self):
        self.provision()
        self.job['resources'][
            oci.DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
        ] = {'state': 'measurement_started'}

        self.assertTrue(
            oci.quiesce_distributed_deathstarbench_load_generator(
                self.job, clients=self.cloud.clients,
            )
        )
        self.assertFalse(
            oci.quiesce_distributed_deathstarbench_load_generator(
                self.job, clients=self.cloud.clients,
            )
        )
        self.assertEqual(
            sum(method == 'instance_action' for method, _ in self.cloud.calls),
            1,
        )

    def test_load_generator_stop_rejects_changed_ownership(self):
        self.provision()
        self.job['resources'][
            oci.DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
        ] = {'state': 'measurement_started'}
        entry = self.contract['entries']['load-generator']
        self.cloud.items['instance'][entry['id']]['display_name'] = 'foreign'

        with self.assertRaisesRegex(oci.LifecycleError, 'display_name'):
            oci.quiesce_distributed_deathstarbench_load_generator(
                self.job, clients=self.cloud.clients,
            )

        self.assertFalse(any(
            method == 'instance_action' for method, _ in self.cloud.calls
        ))

    def test_changed_default_child_blocks_all_cleanup(self):
        self.provision()
        identity = self.contract['entries']['vcn']['implicit']['security_list']['id']
        self.cloud.items['security_list'][identity]['display_name'] = 'changed'
        before = len(self.cloud.calls)
        with self.assertRaisesRegex(oci.LifecycleError, 'Implicit resource changed'):
            self.destroy()
        self.assertEqual(before, len(self.cloud.calls))

    def test_foreign_instance_volume_attachment_blocks_cleanup(self):
        self.provision()
        self.cloud.put('volume_attachment', {'compartment_id': COMPARTMENT,
            'instance_id': self.contract['entries']['application']['id'], 'volume_id': 'foreign'})
        before = len(self.cloud.calls)
        with self.assertRaisesRegex(oci.LifecycleError, 'foreign data-volume'):
            self.destroy()
        self.assertEqual(before, len(self.cloud.calls))

    def test_delete_response_loss_resumes_without_losing_inventory(self):
        for method in ('detach_volume', 'terminate_instance', 'delete_boot_volume', 'delete_subnet', 'delete_vcn'):
            with self.subTest(method=method):
                self.setUp()
                self.provision()
                self.cloud.delete_fail_after = method
                with self.assertRaises(ConnectionError):
                    self.destroy()
                self.assertEqual(self.contract['status'], 'deleting')
                self.destroy()
                self.assertEqual(self.contract['status'], 'deleted')

    def test_parent_etag_change_is_revalidated_before_delete_retry(self):
        self.provision()
        network = self.cloud.clients['network']
        original_get = network.get_internet_gateway
        original_delete = network.delete_internet_gateway
        rejected = False
        delete_matches = []

        def get_gateway(identity):
            result = original_get(identity)
            result.headers['etag'] = 'fresh' if rejected else 'stale'
            return result

        def delete_gateway(identity, **kwargs):
            nonlocal rejected
            delete_matches.append(kwargs['if_match'])
            if not rejected:
                rejected = True
                raise sdk.exceptions.ServiceError(412, 'NoEtagMatch', {}, 'changed')
            forwarded = dict(kwargs)
            forwarded['if_match'] = '1'
            return original_delete(identity, **forwarded)

        with (patch.object(network, 'get_internet_gateway', side_effect=get_gateway),
              patch.object(network, 'delete_internet_gateway', side_effect=delete_gateway)):
            self.destroy()
        self.assertEqual(delete_matches, ['stale', 'fresh'])
        self.assertEqual(self.contract['status'], 'deleted')

    def test_stable_exact_resource_can_use_persisted_etagless_fallback(self):
        self.provision()
        network = self.cloud.clients['network']
        original_delete = network.delete_internet_gateway
        delete_matches = []

        def delete_gateway(identity, **kwargs):
            delete_matches.append(kwargs.get('if_match'))
            if len(delete_matches) <= oci.ETAG_DELETE_ATTEMPTS:
                raise sdk.exceptions.ServiceError(412, 'NoEtagMatch', {}, 'changed')
            forwarded = dict(kwargs)
            forwarded['if_match'] = '1'
            return original_delete(identity, **forwarded)

        with (patch.object(oci.time, 'sleep'),
              patch.object(network, 'delete_internet_gateway', side_effect=delete_gateway)):
            self.destroy()
        self.assertEqual(delete_matches, ['1', '1', '1', None])
        igw = self.contract['entries']['igw']
        self.assertTrue(igw['etag_fallback_attempted_at'])
        self.assertEqual(igw['etag_fallback_observed'], '1')
        self.assertEqual(igw['etag_fallback_confirmed'], '1')
        self.assertEqual(self.contract['status'], 'deleted')

    def test_identical_resource_with_unstable_get_etags_can_use_fallback(self):
        self.provision()
        network = self.cloud.clients['network']
        original_get = network.get_internet_gateway
        original_delete = network.delete_internet_gateway
        get_count = 0
        delete_matches = []

        def get_gateway(identity):
            nonlocal get_count
            get_count += 1
            result = original_get(identity)
            result.headers['etag'] = f'unstable-{get_count}'
            return result

        def delete_gateway(identity, **kwargs):
            delete_matches.append(kwargs.get('if_match'))
            if len(delete_matches) <= oci.ETAG_DELETE_ATTEMPTS:
                raise sdk.exceptions.ServiceError(
                    412,
                    'NoEtagMatch',
                    {},
                    'service computed a different ETag',
                )
            forwarded = dict(kwargs)
            forwarded['if_match'] = '1'
            return original_delete(identity, **forwarded)

        with (
            patch.object(oci.time, 'sleep'),
            patch.object(
                network,
                'get_internet_gateway',
                side_effect=get_gateway,
            ),
            patch.object(
                network,
                'delete_internet_gateway',
                side_effect=delete_gateway,
            ),
        ):
            self.destroy()

        self.assertIsNone(delete_matches[-1])
        igw = self.contract['entries']['igw']
        self.assertNotEqual(
            igw['etag_fallback_observed'],
            igw['etag_fallback_confirmed'],
        )
        self.assertEqual(self.contract['status'], 'deleted')

    def test_etagless_fallback_is_refused_for_non_gateway_mutations(self):
        self.provision()
        compute = self.cloud.clients['compute']
        matches = []

        def reject_detach(_identity, **kwargs):
            matches.append(kwargs.get('if_match'))
            raise sdk.exceptions.ServiceError(
                412,
                'NoEtagMatch',
                {},
                'attachment changed',
            )

        with (
            patch.object(
                compute,
                'detach_volume',
                side_effect=reject_detach,
            ),
            self.assertRaisesRegex(
                oci.LifecycleError,
                'only for the qualified internet gateway',
            ),
        ):
            self.destroy()

        self.assertEqual(matches, ['1', '1', '1'])
        self.assertNotIn(
            'etag_fallback_attempted_at',
            self.contract['entries']['database-attachment'],
        )

    def test_tampered_request_contract_refuses_mutation(self):
        self.provision()
        self.contract['entries']['database-data']['spec']['body']['size_in_gbs'] = 1
        before = len(self.cloud.calls)
        with self.assertRaisesRegex(oci.LifecycleError, 'contract changed'):
            self.destroy()
        self.assertEqual(before, len(self.cloud.calls))

    def test_ownership_tag_mismatch_refuses_all_cleanup(self):
        self.provision()
        self.cloud.items['volume'][self.contract['entries']['database-data']['id']]['freeform_tags']['benchmark-job'] = 'other'
        before = len(self.cloud.calls)
        with self.assertRaisesRegex(oci.LifecycleError, 'benchmark-job'):
            self.destroy()
        self.assertEqual(before, len(self.cloud.calls))

    def test_required_defined_tag_drift_refuses_all_cleanup_mutations(self):
        self.provision()
        identity = self.contract['entries']['database-data']['id']
        self.cloud.items['volume'][identity]['defined_tags']['CostCenter']['Department'] = 'Marketing'
        before = len(self.cloud.calls)
        with self.assertRaisesRegex(
                oci.LifecycleError,
                r'defined_tags\.CostCenter\.Department no longer matches'):
            self.destroy()
        self.assertEqual(before, len(self.cloud.calls))
        self.assertIn(identity, self.cloud.items['volume'])

    def test_image_architecture_or_custom_image_fails_before_mutation(self):
        for field, value in (('display_name', 'Oracle-Linux-9-aarch64'), ('compartment_id', COMPARTMENT), ('operating_system_version', '8.10')):
            with self.subTest(field=field):
                self.setUp()
                self.cloud.items['image'][IMAGE][field] = value
                with self.assertRaisesRegex(oci.LifecycleError, 'platform image'):
                    self.provision()
                self.assertFalse(self.cloud.calls)

    def test_wrong_region_fails_before_mutation(self):
        self.cloud.clients['block']._config['region'] = 'wrong'
        with self.assertRaisesRegex(oci.LifecycleError, 'region'):
            self.provision()
        self.assertFalse(self.cloud.calls)

    def test_changed_inputs_cannot_replace_existing_contract(self):
        self.provision()
        self.inputs['memory_gb'] = 64
        with self.assertRaisesRegex(oci.LifecycleError, 'Cannot change'):
            self.provision()

    def test_cleanup_after_partial_infrastructure_creation(self):
        self.cloud.fail_after = 'create_nat_gateway'
        with self.assertRaises(ConnectionError):
            self.provision()
        self.destroy()
        self.assertEqual(self.contract['status'], 'deleted')
        self.assertFalse(self.cloud.items['vcn'])

    def test_coordinated_runtime_profile_is_released(self):
        from app.deathstarbench_contract import require_released_runtime
        profile = require_released_runtime('distributed_tiered_v1', 'k3s_v1')
        self.assertTrue(profile.released)

    def test_arm_application_pins_separate_compatible_platform_image(self):
        arm_image = 'ocid1.image.oc1.iad.oraclelinux9aarch64'
        self.cloud.put('image', {**self.cloud.items['image'][IMAGE], 'id': arm_image,
                                'display_name': 'Oracle-Linux-9.6-aarch64-2026.09.01-0'})
        self.inputs.update(shape='VM.Standard.A2.Flex', architecture='arm64', application_image_id=arm_image)
        inventory = self.provision()
        self.assertEqual(inventory.node('application').architecture, 'arm64')
        self.assertEqual(inventory.node('database').architecture, 'x86_64')
        self.destroy()

    def test_unknown_inventory_version_refuses_cleanup(self):
        self.provision()
        self.job['resources']['role_node_inventory']['schema_version'] = 99
        before = len(self.cloud.calls)
        with self.assertRaisesRegex(oci.LifecycleError, 'inventory conflicts'):
            self.destroy()
        self.assertEqual(before, len(self.cloud.calls))

    def test_sdk_404_is_not_absence_when_list_still_contains_resource(self):
        self.provision()
        with patch.object(self.cloud.clients['block'], 'get_volume', side_effect=not_found()):
            with self.assertRaisesRegex(oci.LifecycleError, 'scoped listing still contains'):
                self.destroy()

    def test_missing_etag_prevents_cleanup(self):
        self.provision()
        identity = self.contract['entries']['database-data']['id']
        data = self.cloud.items['volume'][identity]
        before = len(self.cloud.calls)
        with patch.object(self.cloud.clients['block'], 'get_volume', return_value=response(data, etag=None)):
            with self.assertRaisesRegex(oci.LifecycleError, 'ETag missing'):
                self.destroy()
        self.assertEqual(before, len(self.cloud.calls))

    def test_recheck_detects_drift_between_cleanup_steps(self):
        self.provision()
        def mutate(method):
            if method == 'detach_volume':
                self.cloud.put('service_gateway', {'vcn_id': self.contract['entries']['vcn']['id'], 'compartment_id': COMPARTMENT})
        self.cloud.on_mutation = mutate
        before = len(self.cloud.calls)
        with self.assertRaisesRegex(oci.LifecycleError, 'Foreign'):
            self.destroy()
        self.assertEqual(len(self.cloud.calls), before + 1)
        self.assertEqual(self.contract['status'], 'deleting')

    def test_old_name_collision_is_not_adopted_after_response_loss(self):
        self.cloud.fail_after = 'create_vcn'
        with self.assertRaises(ConnectionError):
            self.provision()
        next(iter(self.cloud.items['vcn'].values()))['time_created'] = '2020-01-01T00:00:00+00:00'
        with self.assertRaisesRegex(oci.LifecycleError, 'creation time'):
            self.destroy()

    def test_unknown_nsg_member_blocks_cleanup(self):
        self.provision()
        self.cloud.put('vnic', {'nsg_ids': [self.contract['entries']['nsg-control']['id']], 'subnet_id': 'foreign'})
        before = len(self.cloud.calls)
        with self.assertRaisesRegex(oci.LifecycleError, 'foreign VNIC'):
            self.destroy()
        self.assertEqual(before, len(self.cloud.calls))

    def test_complete_cloud_creation_is_json_serializable(self):
        self.provision()
        encoded = json.dumps(self.job)
        self.assertIn('opc', oci.SSH_USER)
        self.assertNotIn('private_key', encoded)
        self.assertEqual(self.contract['entries']['database-attachment']['spec']['body']['type'], 'paravirtualized')

    def test_persistence_failure_prevents_first_cloud_mutation(self):
        with self.assertRaisesRegex(OSError, 'disk full'):
            oci.provision_distributed_deathstarbench_candidate(self.job, inputs=self.inputs,
                clients=self.cloud.clients, persist=lambda job: (_ for _ in ()).throw(OSError('disk full')))
        self.assertFalse(self.cloud.calls)

    def test_provider_managed_implicit_transitions_are_not_ownership_drift(self):
        self.provision()
        for boot in self.cloud.items['boot_volume'].values():
            boot['is_hydrated'] = True
        control = self.contract['entries']['control']
        boot_attachment = control['implicit']['attachments']['boot']
        boot_attachment['time_updated'] = '2026-09-29T22:43:31+00:00'
        self.cloud.items['boot_volume_attachment'][boot_attachment['id']][
            'time_updated'
        ] = '2026-09-29T23:06:18+00:00'
        boot_volume = control['implicit']['boot_volume']
        boot_volume['snapshot']['time_updated'] = '2026-09-29T22:43:31+00:00'
        self.cloud.items['boot_volume'][boot_volume['id']][
            'time_updated'
        ] = '2026-09-29T23:06:18+00:00'
        oci.publish_distributed_runtime_projection(
            self.job,
            clients=self.cloud.clients,
            persist=self.persist,
        )
        self.destroy()
        self.assertEqual(self.contract['status'], 'deleted')

    def test_sdk_model_builder_uses_wire_field_mappings(self):
        body = oci._model('CreateVolumeDetails', {'compartment_id': COMPARTMENT, 'size_in_gbs': 256,
            'vpus_per_gb': 20, 'is_auto_tune_enabled': False})
        self.assertEqual(body.attribute_map['vpus_per_gb'], 'vpusPerGB')
        attachment = oci._model('AttachParavirtualizedVolumeDetails', {'type': 'paravirtualized',
            'instance_id': 'instance', 'volume_id': 'volume', 'device': oci.DATABASE_DEVICE})
        self.assertEqual(attachment.type, 'paravirtualized')
        shape = sdk.core.models.ShapeMemoryOptions(min_per_ocpu_in_gbs=1, max_per_ocpu_in_gbs=64)
        self.assertEqual(sdk.util.to_dict(shape)['min_per_ocpu_in_gbs'], 1)

    def test_cleanup_rechecks_secondary_vnic_after_preterminate_failure(self):
        self.provision()
        compute = self.cloud.clients['compute']
        with patch.object(compute, 'terminate_instance', side_effect=ConnectionError('not accepted')) as terminate:
            with self.assertRaisesRegex(ConnectionError, 'not accepted'):
                self.destroy()
            terminate.assert_called_once()
        entry = self.contract['entries']['load-generator']
        self.assertEqual(entry['status'], 'deleting')
        self.assertEqual(self.cloud.items['instance'][entry['id']]['lifecycle_state'], 'RUNNING')
        # The foreign interface is outside this run's subnet/NSG scans. Only
        # re-reading the live instance's attachment graph can find it.
        foreign = self.cloud.put('vnic', {'compartment_id': COMPARTMENT, 'availability_domain': INPUTS['availability_domain'],
            'is_primary': False, 'subnet_id': 'foreign-subnet', 'nsg_ids': []})
        self.cloud.put('vnic_attachment', {'compartment_id': COMPARTMENT, 'availability_domain': INPUTS['availability_domain'],
            'instance_id': entry['id'], 'vnic_id': foreign['id'], 'subnet_id': 'foreign-subnet'})
        before = len(self.cloud.calls)
        with self.assertRaisesRegex(oci.LifecycleError, 'exactly one VNIC'):
            self.destroy()
        self.assertEqual(len(self.cloud.calls), before, 'Recovery must make zero destructive calls.')
        self.assertIn(foreign['id'], self.cloud.items['vnic'])

    def test_unowned_cloud_termination_blocks_all_cleanup_mutations(self):
        for cloud_state in ('TERMINATING', 'TERMINATED'):
            with self.subTest(cloud_state=cloud_state):
                self.setUp()
                self.provision()
                entry = self.contract['entries']['load-generator']
                self.cloud.items['instance'][entry['id']]['lifecycle_state'] = cloud_state
                before = len(self.cloud.calls)
                with self.assertRaisesRegex(oci.LifecycleError, 'without an owned delete intent'):
                    self.destroy()
                self.assertEqual(before, len(self.cloud.calls), 'Preflight must refuse before even the database detach.')
                attachment_id = self.contract['entries']['database-attachment']['id']
                self.assertEqual(self.cloud.items['volume_attachment'][attachment_id]['lifecycle_state'], 'ATTACHED')
                self.assertEqual(entry['status'], 'ready')

    def test_cloud_termination_requires_timestamped_delete_intent_not_status_alone(self):
        for cloud_state in ('TERMINATING', 'TERMINATED'):
            for timestamp in (None, 'invalid', '2026-09-28T12:00:00'):
                with self.subTest(cloud_state=cloud_state, timestamp=timestamp):
                    self.setUp()
                    self.provision()
                    entry = self.contract['entries']['load-generator']
                    entry['status'] = 'deleting'
                    entry['delete_attempted_at'] = timestamp
                    oci._save(self.job, self.persist)
                    self.cloud.items['instance'][entry['id']]['lifecycle_state'] = cloud_state
                    before = len(self.cloud.calls)
                    with self.assertRaisesRegex(oci.LifecycleError, 'without an owned delete intent'):
                        self.destroy()
                    self.assertEqual(before, len(self.cloud.calls))

    def test_first_default_child_capture_rejects_foreign_compartment_identity_or_parent(self):
        for kind in ('route_table', 'security_list', 'dhcp_options'):
            for field in ('compartment_id', 'id', 'vcn_id'):
                with self.subTest(kind=kind, field=field):
                    self.setUp()
                    self.cloud.fail_after = 'create_vcn'
                    with self.assertRaises(ConnectionError):
                        self.provision()
                    child = next(iter(self.cloud.items[kind].values()))
                    child[field] = 'foreign'
                    before = len(self.cloud.calls)
                    with self.assertRaisesRegex(oci.LifecycleError, 'VCN implicit child'):
                        self.provision()
                    self.assertEqual(before, len(self.cloud.calls))
                    self.assertNotIn('implicit', self.contract['entries']['vcn'])

    def test_first_instance_child_capture_rejects_foreign_compartment_ad_or_identity(self):
        for kind in ('vnic', 'boot_volume'):
            for field in ('compartment_id', 'availability_domain', 'id'):
                with self.subTest(kind=kind, field=field):
                    self.setUp()
                    self.cloud.fail_after = 'launch_instance'
                    with self.assertRaises(ConnectionError):
                        self.provision()
                    child = next(iter(self.cloud.items[kind].values()))
                    child[field] = 'foreign'
                    before = len(self.cloud.calls)
                    with self.assertRaisesRegex(oci.LifecycleError, 'primary-vnic|boot-volume'):
                        self.provision()
                    self.assertEqual(before, len(self.cloud.calls))
                    self.assertNotIn('implicit', self.contract['entries']['control'])

    def test_first_attachment_capture_validates_scoped_list_boundaries(self):
        for kind in ('vnic_attachment', 'boot_volume_attachment'):
            for field in ('compartment_id', 'availability_domain', 'instance_id'):
                with self.subTest(kind=kind, field=field):
                    self.setUp()
                    self.cloud.fail_after = 'launch_instance'
                    with self.assertRaises(ConnectionError):
                        self.provision()
                    item = copy.deepcopy(next(iter(self.cloud.items[kind].values())))
                    item[field] = 'foreign'
                    # Do not trust SDK list filters as an ownership assertion.
                    with patch.object(self.cloud.clients['compute'], 'list_' + kind + 's', return_value=response([item])):
                        before = len(self.cloud.calls)
                        with self.assertRaisesRegex(oci.LifecycleError, 'Implicit attachment'):
                            self.provision()
                        self.assertEqual(before, len(self.cloud.calls))
                    self.assertNotIn('implicit', self.contract['entries']['control'])

    def test_first_attachment_capture_validates_get_identity_and_parent(self):
        for kind, target_field in (('vnic_attachment', 'vnic_id'), ('boot_volume_attachment', 'boot_volume_id')):
            for field in ('id', 'instance_id', 'compartment_id', 'availability_domain', target_field):
                with self.subTest(kind=kind, field=field):
                    self.setUp()
                    self.cloud.fail_after = 'launch_instance'
                    with self.assertRaises(ConnectionError):
                        self.provision()
                    item = copy.deepcopy(next(iter(self.cloud.items[kind].values())))
                    item[field] = 'foreign'
                    with patch.object(self.cloud.clients['compute'], 'get_' + kind, return_value=response(item)):
                        before = len(self.cloud.calls)
                        with self.assertRaisesRegex(oci.LifecycleError, 'Implicit attachment readback'):
                            self.provision()
                        self.assertEqual(before, len(self.cloud.calls))
                    self.assertNotIn('implicit', self.contract['entries']['control'])

    def test_candidate_validation_is_read_only_and_returns_detached_contract(self):
        self.provision()
        before = copy.deepcopy(self.job)
        calls = len(self.cloud.calls)
        validated = oci.validate_distributed_deathstarbench_candidate(
            self.job, clients=self.cloud.clients, require_ready=True)
        self.assertEqual(self.job, before)
        self.assertEqual(len(self.cloud.calls), calls)
        validated['inputs']['region'] = 'changed-copy'
        self.assertEqual(self.contract['inputs']['region'], INPUTS['region'])

    def test_runtime_publication_and_exact_local_role_connections(self):
        self.provision()
        with self.assertRaisesRegex(oci.LifecycleError, 'cloud-validated and published'):
            oci.distributed_deathstarbench_candidate_projection(self.job['resources'])
        calls = len(self.cloud.calls)
        aliases = oci.publish_distributed_runtime_projection(
            self.job, clients=self.cloud.clients, persist=self.persist)
        self.assertEqual(set(aliases), oci.RUNTIME_ALIAS_KEYS)
        self.assertEqual(self.snapshots[-1]['resources']['ssh_user'], 'opc')
        self.assertEqual(len(self.cloud.calls), calls)
        before = copy.deepcopy(self.job)
        projection = oci.distributed_deathstarbench_candidate_projection(self.job['resources'])
        self.assertEqual(self.job, before)
        self.assertEqual(projection['provider'], 'oci')
        self.assertEqual(projection['zone'], INPUTS['availability_domain'])
        self.assertEqual(projection['database_volume_id'], self.contract['entries']['database-data']['id'])
        self.assertEqual(projection['database_attachment_id'], self.contract['entries']['database-attachment']['id'])
        self.assertEqual(projection['database_device'], oci.DATABASE_DEVICE)
        self.assertEqual(projection['database_mount_point'], '/var/lib/deathstarbench/database')
        for role, node in projection['nodes'].items():
            direct = role in {'control', 'load-generator'}
            prefix = 'oci_dsb_' + role.replace('-', '_')
            self.assertEqual(node['private_ip'], oci.PRIVATE_ADDRESSES[role])
            self.assertEqual(node['instance_id'], self.contract['entries'][role]['id'])
            self.assertEqual(node['host_key'], prefix + ('_public_ip' if direct else '_private_ip'))
            self.assertEqual(node['jump_host_key'], None if direct else 'oci_dsb_control_public_ip')
            self.assertEqual(bool(node['public_ip']), role in oci.PUBLIC_ROLES)
        projection['nodes']['control']['private_ip'] = 'wrong-copy'
        self.assertEqual(self.job['resources']['oci_dsb_control_private_ip'], '10.240.1.10')
        self.assertEqual(self.job['resources']['role_node_inventory']['nodes'][3]['storage'][0]['mount_point'],
                         '/var/lib/deathstarbench/database')

    def test_runtime_alias_drift_is_never_overwritten(self):
        self.provision()
        oci.publish_distributed_runtime_projection(self.job, clients=self.cloud.clients, persist=self.persist)
        for key, value in (('ssh_user', 'root'), ('provider', 'aws'),
                           ('oci_dsb_control_public_ip', '198.51.100.254'),
                           ('oci_dsb_database_private_ip', '10.240.3.99'),
                           ('oci_dsb_database_volume_id', 'ocid1.volume.oc1.iad.foreign'),
                           ('oci_dsb_database_device', '/dev/sda'),
                           ('oci_dsb_database_mount_point', '/data')):
            with self.subTest(key=key):
                job = copy.deepcopy(self.job)
                job['resources'][key] = value
                with self.assertRaisesRegex(oci.LifecycleError, 'runtime alias'):
                    oci.distributed_deathstarbench_candidate_projection(job['resources'])
                with self.assertRaisesRegex(oci.LifecycleError, 'runtime alias'):
                    oci.publish_distributed_runtime_projection(job, clients=self.cloud.clients, persist=self.persist)
                self.assertEqual(job['resources'][key], value)

    def test_runtime_rejects_partial_aliases_and_unknown_routes(self):
        self.provision()
        oci.publish_distributed_runtime_projection(self.job, clients=self.cloud.clients, persist=self.persist)
        missing = copy.deepcopy(self.job)
        missing['resources'].pop('oci_dsb_database_device')
        with self.assertRaisesRegex(oci.LifecycleError, 'completely cloud-validated'):
            oci.distributed_deathstarbench_candidate_projection(missing['resources'])
        self.job['resources']['oci_dsb_override_public_ip'] = '198.51.100.254'
        with self.assertRaisesRegex(oci.LifecycleError, 'Unknown OCI runtime aliases'):
            oci.distributed_deathstarbench_candidate_projection(self.job['resources'])

    def test_runtime_rejects_incomplete_or_nonrunning_cloud_before_publication(self):
        self.cloud.fail_after = 'create_nat_gateway'
        with self.assertRaises(ConnectionError):
            self.provision()
        with self.assertRaisesRegex(oci.LifecycleError, 'complete ready'):
            oci.distributed_runtime_projection(self.job)
        self.provision()
        self.cloud.items['instance'][self.contract['entries']['application']['id']]['lifecycle_state'] = 'STOPPED'
        with self.assertRaisesRegex(oci.LifecycleError, 'live ready resources'):
            oci.publish_distributed_runtime_projection(self.job, clients=self.cloud.clients, persist=self.persist)
        self.assertNotIn('ssh_user', self.job['resources'])

    def test_runtime_rejects_live_graph_drift_without_mutating_saved_snapshots(self):
        self.provision()
        control = self.contract['entries']['control']
        self.cloud.items['vnic'][control['implicit']['vnic']['id']]['public_ip'] = '198.51.100.254'
        before = copy.deepcopy(self.job)
        calls = len(self.cloud.calls)
        with self.assertRaisesRegex(oci.LifecycleError, 'Implicit resource changed'):
            oci.publish_distributed_runtime_projection(self.job, clients=self.cloud.clients, persist=self.persist)
        self.assertEqual(self.job, before)
        self.assertEqual(len(self.cloud.calls), calls)

    def test_local_runtime_checks_captured_identity_and_pinned_image_evidence(self):
        self.provision()
        oci.publish_distributed_runtime_projection(self.job, clients=self.cloud.clients, persist=self.persist)
        mutations = (
            lambda contract: contract['entries']['control']['implicit']['boot_volume']['snapshot'].update(compartment_id='foreign'),
            lambda contract: contract['entries']['control']['implicit']['attachments']['vnic'].update(instance_id='foreign'),
            lambda contract: contract['images']['support'].update(id='foreign'),
            lambda contract: contract['entries']['control']['implicit']['vnic']['snapshot'].update(private_ip='10.240.1.99'),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                job = copy.deepcopy(self.job)
                mutate(job['resources'][oci.CONTRACT_KEY])
                with self.assertRaises(oci.LifecycleError):
                    oci.distributed_deathstarbench_candidate_projection(job['resources'])

    def test_deleted_predicate_accepts_full_confirmed_graph_with_retained_tombstones(self):
        self.provision()
        oci.publish_distributed_runtime_projection(self.job, clients=self.cloud.clients, persist=self.persist)
        self.assertFalse(oci.distributed_candidate_is_deleted(self.job))
        self.destroy()
        self.assertTrue(oci.distributed_candidate_is_deleted(self.job))
        self.assertTrue(self.contract['entries']['control']['id'])
        self.assertTrue(self.job['resources']['oci_dsb_control_public_ip'])
        self.job['plan'] = {'provider': 'oci'}
        self.assertTrue(main.has_recoverable_resources(self.job))
        self.assertTrue(main.has_recoverable_resources_for_any_provider(self.job))
        self.job['status'] = 'destroyed'
        self.job['cleanup_error'] = None
        before = len(self.cloud.calls)
        self.assertFalse(main.has_recoverable_resources(self.job))
        self.assertFalse(main.has_recoverable_resources_for_any_provider(self.job))
        self.assertEqual(len(self.cloud.calls), before)
        self.job['cleanup_error'] = 'terminal audit failed'
        self.assertTrue(main.has_recoverable_resources(self.job))
        self.assertTrue(main.has_recoverable_resources_for_any_provider(self.job))
        self.job['cleanup_error'] = None
        self.job['resources']['deathstarbench_distributed_network_qualification'] = {'retained': 'evidence'}
        self.assertTrue(oci.distributed_candidate_is_deleted(self.job))
        self.destroy()
        with self.assertRaisesRegex(oci.LifecycleError, 'complete ready'):
            oci.distributed_deathstarbench_candidate_projection(self.job['resources'])

    def test_terminal_deletion_attestation_is_fresh_read_only_cloud_evidence(self):
        self.provision()
        self.destroy()
        before = copy.deepcopy(self.job)
        mutation_calls = len(self.cloud.calls)
        persisted = len(self.snapshots)
        network = self.cloud.clients['network']
        compute = self.cloud.clients['compute']
        block = self.cloud.clients['block']
        self.cloud.on_mutation = lambda method: self.fail(
            f'terminal deletion attestation attempted mutation {method}'
        )

        with (
            patch.object(network, 'get_vcn', wraps=network.get_vcn) as get_vcn,
            patch.object(network, 'list_vcns', wraps=network.list_vcns) as list_vcns,
            patch.object(
                network,
                'list_route_tables',
                wraps=network.list_route_tables,
            ) as list_route_tables,
            patch.object(
                compute,
                'get_vnic_attachment',
                wraps=compute.get_vnic_attachment,
            ) as get_vnic_attachment,
            patch.object(
                compute,
                'list_vnic_attachments',
                wraps=compute.list_vnic_attachments,
            ) as list_vnic_attachments,
            patch.object(
                block,
                'get_boot_volume',
                wraps=block.get_boot_volume,
            ) as get_boot_volume,
            patch.object(
                block,
                'list_boot_volumes',
                wraps=block.list_boot_volumes,
            ) as list_boot_volumes,
        ):
            self.assertTrue(
                oci.attest_distributed_candidate_terminal_deletion(
                    self.job,
                    clients=self.cloud.clients,
                )
            )

        self.assertTrue(get_vcn.called)
        self.assertTrue(list_vcns.called)
        self.assertTrue(list_route_tables.called)
        self.assertTrue(
            all('vcn_id' not in call.kwargs for call in list_route_tables.call_args_list)
        )
        self.assertTrue(get_vnic_attachment.called)
        self.assertTrue(list_vnic_attachments.called)
        self.assertTrue(get_boot_volume.called)
        self.assertTrue(list_boot_volumes.called)
        self.assertEqual(self.job, before)
        self.assertEqual(len(self.cloud.calls), mutation_calls)
        self.assertEqual(len(self.snapshots), persisted)

    def test_terminal_deletion_attestation_rejects_live_explicit_tombstone(self):
        self.provision()
        volume = self.contract['entries']['database-data']
        identity = volume['id']
        live_snapshot = copy.deepcopy(self.cloud.items['volume'][identity])
        self.destroy()
        self.cloud.items['volume'][identity] = live_snapshot
        before = copy.deepcopy(self.job)

        with self.assertRaisesRegex(oci.LifecycleError, 'explicit database-data still live'):
            oci.attest_distributed_candidate_terminal_deletion(
                self.job,
                clients=self.cloud.clients,
            )

        self.assertEqual(self.job, before)

    def test_terminal_deletion_attestation_rejects_live_list_after_get_404(self):
        self.provision()
        volume = self.contract['entries']['database-data']
        identity = volume['id']
        live_snapshot = copy.deepcopy(self.cloud.items['volume'][identity])
        self.destroy()
        block = self.cloud.clients['block']

        with (
            patch.object(block, 'get_volume', side_effect=not_found()),
            patch.object(
                block,
                'list_volumes',
                return_value=response([live_snapshot]),
            ),
            self.assertRaisesRegex(
                oci.LifecycleError,
                'explicit database-data still live',
            ),
        ):
            oci.attest_distributed_candidate_terminal_deletion(
                self.job,
                clients=self.cloud.clients,
            )

    def test_terminal_deletion_attestation_accepts_retained_terminal_record(self):
        self.provision()
        volume = self.contract['entries']['database-data']
        identity = volume['id']
        terminal_snapshot = copy.deepcopy(self.cloud.items['volume'][identity])
        self.destroy()
        terminal_snapshot['lifecycle_state'] = 'TERMINATED'
        self.cloud.items['volume'][identity] = terminal_snapshot

        self.assertTrue(
            oci.attest_distributed_candidate_terminal_deletion(
                self.job,
                clients=self.cloud.clients,
            )
        )

    def test_terminal_deletion_attestation_ignores_transition_metadata_not_parents(self):
        self.provision()
        attachment = self.contract['entries']['control']['implicit']['attachments']['boot']
        identity = attachment['id']
        cloud_attachment = self.cloud.items['boot_volume_attachment'][identity]
        attachment['time_updated'] = '2026-09-29T22:43:31.929000+00:00'
        cloud_attachment['time_updated'] = attachment['time_updated']
        self.destroy()
        cloud_attachment = self.cloud.items['boot_volume_attachment'][identity]
        cloud_attachment['time_updated'] = '2026-09-29T23:06:18.410000+00:00'
        cloud_attachment['lifecycle_details'] = 'Detached during instance termination.'

        self.assertTrue(
            oci.attest_distributed_candidate_terminal_deletion(
                self.job,
                clients=self.cloud.clients,
            )
        )

        cloud_attachment['instance_id'] = 'ocid1.instance.oc1.iad.foreign'
        with self.assertRaisesRegex(
            oci.LifecycleError,
            r'control boot attachment\.instance_id no longer matches',
        ):
            oci.attest_distributed_candidate_terminal_deletion(
                self.job,
                clients=self.cloud.clients,
            )

    def test_terminal_deletion_attestation_accepts_confirmed_partial_cleanup(self):
        error = sdk.exceptions.ServiceError(400, 'LimitExceeded', {}, 'no acceptance')
        with patch.object(
            self.cloud.clients['network'],
            'create_nat_gateway',
            side_effect=error,
        ):
            with self.assertRaises(sdk.exceptions.ServiceError):
                self.provision()
        self.destroy()

        self.assertTrue(
            oci.attest_distributed_candidate_terminal_deletion(
                self.job,
                clients=self.cloud.clients,
            )
        )

    def test_deleted_predicate_accepts_confirmed_rejection_and_partial_graph_cleanup(self):
        error = sdk.exceptions.ServiceError(400, 'LimitExceeded', {}, 'no acceptance')
        with patch.object(self.cloud.clients['network'], 'create_nat_gateway', side_effect=error):
            with self.assertRaises(sdk.exceptions.ServiceError):
                self.provision()
        self.destroy()
        self.assertTrue(oci.distributed_candidate_is_deleted(self.job))
        self.assertEqual(self.contract['entries']['nat']['status'], 'planned')

    def test_deleted_predicate_rejects_forged_missing_or_conflicting_evidence(self):
        self.provision()
        forged = copy.deepcopy(self.job)
        forged['resources'][oci.CONTRACT_KEY]['status'] = 'deleted'
        self.assertFalse(oci.distributed_candidate_is_deleted(forged))
        self.destroy()
        mutations = (
            lambda job: job['resources'][oci.CONTRACT_KEY].pop('deletion_confirmed_at'),
            lambda job: job['resources'][oci.CONTRACT_KEY]['entries']['control'].pop('deletion_confirmed_at'),
            lambda job: job['resources'][oci.CONTRACT_KEY]['entries']['control']['implicit']['boot_volume'].pop('deletion_confirmed_at'),
            lambda job: job['resources'][oci.CONTRACT_KEY]['entries']['vcn'].pop('implicit'),
            lambda job: job['resources'][oci.CONTRACT_KEY]['entries']['control'].update(deletion_confirmed_at='2000-01-01T00:00:00+00:00'),
            lambda job: job['resources'].update(instance_id='untracked-other-instance'),
            lambda job: job['resources']['role_node_inventory'].update(schema_version=99),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                job = copy.deepcopy(self.job)
                mutate(job)
                self.assertFalse(oci.distributed_candidate_is_deleted(job))
                with self.assertRaises(oci.LifecycleError):
                    oci.destroy_distributed_deathstarbench_candidate(job, clients=self.cloud.clients, persist=self.persist)


if __name__ == '__main__':
    unittest.main()
