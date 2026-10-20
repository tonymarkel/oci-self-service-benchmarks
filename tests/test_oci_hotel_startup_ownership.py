"""Strict supported OCI snapshots over the actual stateful SDK fixture."""
import copy
import json
import traceback
import unittest

from app.providers import oci
from tests.test_oci_deathstarbench_lifecycle import Cloud, INPUTS


class StrictCloud(Cloud):
    def put(self, kind, body):
        body = copy.deepcopy(body)
        if kind == 'route_table':
            body.setdefault('route_rules', [])
        elif kind == 'security_list':
            body.setdefault('ingress_security_rules', [])
            body.setdefault('egress_security_rules', [])
        elif kind == 'dhcp_options':
            body.setdefault('options', [{'type': 'DomainNameServer', 'server_type': 'VcnLocal'}])
        elif kind == 'vnic':
            body.pop('assign_public_ip', None)  # Not a field returned by OCI Vnic.
        return super().put(kind, body)


class OciStartupOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.cloud = StrictCloud()
        self.inputs = {**copy.deepcopy(INPUTS), 'defined_tags': {}}
        self.job = {'id': '123456abcdef', 'resources': {}}
        self.saved = []

    def provision(self):
        return oci.provision_distributed_deathstarbench_candidate(self.job, inputs=self.inputs,
            clients=self.cloud.clients, persist=lambda job: self.saved.append(copy.deepcopy(job)),
            ownership_snapshot='startup-v1')

    def test_supported_finite_snapshots_complete_actual_strict_engine(self):
        self.provision()
        contract = oci.prepare_distributed_candidate_cleanup(self.job, clients=self.cloud.clients)
        self.assertEqual(set(contract['images']['support']), {'id', 'display_name', 'compartment_id',
            'operating_system', 'operating_system_version', 'lifecycle_state'})
        oci.destroy_distributed_deathstarbench_candidate(self.job, clients=self.cloud.clients,
            persist=lambda job: self.saved.append(copy.deepcopy(job)), ownership_snapshot='startup-v1', strict_cleanup=True)
        oci.attest_distributed_candidate_terminal_deletion(self.job, clients=self.cloud.clients)
        self.assertTrue(oci.distributed_candidate_is_deleted(self.job))

    def test_unapproved_configured_inputs_refuse_before_contract_or_create(self):
        for field, value in [('defined_tags', {'Secret': {'Value': 'SECRET'}}),
                             ('public_key', self.inputs['public_key'] + '-SECRET')]:
            with self.subTest(field=field):
                self.inputs[field] = value
                with self.assertRaises((ValueError, oci.LifecycleError)):
                    self.provision()
                self.assertEqual(self.saved, [])
                self.assertEqual(self.cloud.calls, [])
                self.inputs = {**copy.deepcopy(INPUTS), 'defined_tags': {}}

    def test_response_lost_allocation_never_enters_display_name_recovery(self):
        self.cloud.fail_after = 'create_vcn'
        with self.assertRaises(ConnectionError):
            self.provision()
        before = list(self.cloud.calls)
        with self.assertRaises((ValueError, oci.LifecycleError)):
            oci.destroy_distributed_deathstarbench_candidate(self.job, clients=self.cloud.clients,
                persist=lambda job: self.saved.append(copy.deepcopy(job)), ownership_snapshot='startup-v1', strict_cleanup=True)
        self.assertEqual(self.cloud.calls, before)
        self.assertNotIn('id', self.job['resources'][oci.CONTRACT_KEY]['entries']['vcn'])

    def test_unknown_nonempty_implicit_config_refuses_before_destructive_calls(self):
        self.provision()
        for kind, field, value in [('vnic', 'metadata', {'token': 'SECRET'}),
                ('vnic', 'ipv6_addresses', ['2001:db8::1']), ('boot_volume', 'source_details', {'type': 'bootVolume'}),
                ('boot_volume', 'system_tags', {'SECRET': 'SECRET'}), ('dhcp_options', 'options', [{'type': 'Unknown'}]),
                ('route_table', 'route_rules', [{'network_entity_id': 'ocid1.internetgateway.oc1.x', 'destination': 'SECRET'}])]:
            row = next(iter(self.cloud.items[kind].values()))
            old = copy.deepcopy(row)
            row[field] = value
            before = list(self.cloud.calls)
            with self.subTest(kind=kind, field=field), self.assertRaises((ValueError, oci.LifecycleError)):
                oci.prepare_distributed_candidate_cleanup(self.job, clients=self.cloud.clients)
            self.assertEqual(self.cloud.calls, before)
            row.clear()
            row.update(old)
        self.assertNotIn('SECRET', json.dumps(self.saved))

    def test_descriptions_are_omitted_but_retained_configuration_drift_refuses(self):
        self.provision()
        row = next(iter(self.cloud.items['security_list'].values()))
        row['lifecycle_details'] = 'SECRET'
        row['unknown_null_sdk_addition'] = None
        contract = oci.prepare_distributed_candidate_cleanup(self.job, clients=self.cloud.clients)
        self.assertNotIn('SECRET', json.dumps(contract))
        row['egress_security_rules'] = [{'protocol': 'all', 'destination': '0.0.0.0/0', 'destination_type': 'CIDR_BLOCK'}]
        with self.assertRaises((ValueError, oci.LifecycleError)):
            oci.prepare_distributed_candidate_cleanup(self.job, clients=self.cloud.clients)

    def test_malformed_live_custom_dns_never_exposes_payload_in_error_or_evidence(self):
        # Exercise the SDK-returned DHCP body, not the parser in isolation.
        self.provision()
        row = next(iter(self.cloud.items['dhcp_options'].values()))
        sentinel = 'SECRET_DNS_PAYLOAD'
        before = list(self.cloud.calls)
        for server in (sentinel, {'payload': sentinel}, None):
            with self.subTest(server_type=type(server).__name__):
                row['options'] = [{'type': 'DomainNameServer', 'server_type': 'CustomDnsServer',
                                   'custom_dns_servers': [server]}]
                try:
                    oci.prepare_distributed_candidate_cleanup(self.job, clients=self.cloud.clients)
                except (ValueError, oci.LifecycleError) as error:
                    self.assertNotIn(sentinel, str(error))
                    self.assertNotIn(sentinel, ''.join(traceback.format_exception(error)))
                    self.assertIsInstance(error, oci.LifecycleError)
                else:
                    self.fail('Malformed SDK custom DNS configuration was accepted.')
                self.assertEqual(self.cloud.calls, before)
                self.assertNotIn(sentinel, json.dumps(self.saved))
                self.assertNotIn(sentinel, json.dumps(self.job))

    def test_every_implicit_kind_keeps_creation_and_configuration_corroboration(self):
        self.provision()
        changes = {
            'route_table': {'display_name': 'changed default', 'route_rules': [{'destination': '0.0.0.0/0',
                'destination_type': 'CIDR_BLOCK', 'network_entity_id': 'ocid1.internetgateway.oc1.changed'}]},
            'security_list': {'egress_security_rules': [{'protocol': '6', 'destination': '0.0.0.0/0',
                'tcp_options': {'destination_port_range': {'min': 80, 'max': 80}}}]},
            'dhcp_options': {'options': [{'type': 'SearchDomain', 'search_domain_names': ['example.test']}]},
            'vnic': {'hostname_label': 'changed', 'skip_source_dest_check': True, 'mac_address': '00:11:22:33:44:55'},
            'boot_volume': {'vpus_per_gb': 20, 'is_auto_tune_enabled': True, 'kms_key_id': 'ocid1.key.oc1.changed'},
            'vnic_attachment': {'nic_index': 2},
            'boot_volume_attachment': {'is_pv_encryption_in_transit_enabled': True},
        }
        for kind, fields in changes.items():
            row = next(iter(self.cloud.items[kind].values()))
            fields = {**fields, 'time_created': '2026-01-02T00:00:00+00:00'}
            for field, value in fields.items():
                original = copy.deepcopy(row)
                before = list(self.cloud.calls)
                with self.subTest(kind=kind, field=field):
                    row[field] = value
                    with self.assertRaises((ValueError, oci.LifecycleError)):
                        oci.prepare_distributed_candidate_cleanup(self.job, clients=self.cloud.clients)
                    self.assertEqual(self.cloud.calls, before)
                row.clear()
                row.update(original)

    def test_direct_boot_delete_revalidates_unknown_config_after_instance_termination(self):
        self.provision()
        def mutate(method):
            if method == 'terminate_instance':
                for boot in self.cloud.items['boot_volume'].values():
                    boot['source_details'] = {'type': 'unknown-secret-source'}
        self.cloud.on_mutation = mutate
        with self.assertRaises((ValueError, oci.LifecycleError)):
            oci.destroy_distributed_deathstarbench_candidate(self.job, clients=self.cloud.clients,
                persist=lambda job: self.saved.append(copy.deepcopy(job)), ownership_snapshot='startup-v1', strict_cleanup=True)
        self.assertFalse(any(method == 'delete_boot_volume' for method, _ in self.cloud.calls))


if __name__ == '__main__':
    unittest.main()
