"""Local handler tests only: no cloud, registry, SSH server, or TLS bypass."""

import copy
import json
import os
from pathlib import Path
import socket
import ssl
import tempfile
import threading
import unittest
import urllib.request
from unittest import mock

from scripts import deathstarbench_registry_egress as egress


class RegistryEgressTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.path = self.root / 'egress.json'
        self.value = {'schema_version': 1, 'transport': egress.TRANSPORT,
                      'forwards': {host: {'host': '127.0.0.1', 'port': 24000 + index}
                                   for index, host in enumerate(sorted(egress.HOSTS))}}
        self.write(self.value)
        self.environment = mock.patch.dict(os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def write(self, value):
        self.path.write_text(json.dumps(value), encoding='utf-8')
        self.path.chmod(0o600)

    def load(self):
        return egress.load_mapping(self.path)

    def tls(self):
        context = mock.Mock(spec=ssl.SSLContext)
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        return context

    def connection(self, handler, host='ghcr.io', timeout=8):
        # Real handler and inherited HTTPSConnection.connect; only the HTTP
        # request is omitted so tests do not contact an external registry.
        def build(factory, request, **kwargs):
            return factory(request.host, timeout=timeout, **kwargs)
        with mock.patch.object(handler, 'do_open', side_effect=build):
            return handler.https_open(urllib.request.Request('https://' + host + '/'))

    def test_default_handler_is_unchanged_and_active_default_tls_is_verified(self):
        self.assertIs(type(egress.https_handler()), urllib.request.HTTPSHandler)
        context = ssl.create_default_context()
        self.assertIs(egress.https_handler(context)._context, context)
        with egress.registry_egress(self.load()):
            handler = egress.https_handler()
            self.assertEqual(handler._context.verify_mode, ssl.CERT_REQUIRED)
            self.assertTrue(handler._context.check_hostname)

    def test_exact_routes_and_original_tls_hostname_without_global_socket_changes(self):
        mapping, context, raw_socket = self.load(), self.tls(), mock.Mock()
        with mock.patch.object(socket, 'create_connection', return_value=raw_socket) as original:
            with egress.registry_egress(mapping) as session:
                handler = egress.https_handler(context)
                self.assertIs(socket.create_connection, original)
                for host in sorted(egress.HOSTS):
                    connection = self.connection(handler, host)
                    connection.connect()
                    original.assert_called_with(mapping[host], timeout=8, source_address=None)
                    context.wrap_socket.assert_called_with(raw_socket, server_hostname=host)
                    self.assertEqual(connection.host, host)
                receipt = session.metadata()
                self.assertTrue(receipt['active'])
                self.assertTrue(receipt['ssh_tcp_routing_used'])
                self.assertEqual(receipt['network_scope'], 'explicit-ssh-tcp-egress-not-direct-network')
                self.assertEqual(receipt['connection_attempts'], dict.fromkeys(egress.HOSTS, 1))
                self.assertEqual(receipt['connection_successes'], dict.fromkeys(egress.HOSTS, 1))
                for flag in ('global_socket_routing_changed', 'global_urllib_opener_installed',
                             'ssh_identity_verified_by_this_module', 'tls_terminated_by_this_module'):
                    self.assertFalse(receipt[flag])
            self.assertIs(socket.create_connection, original)
        self.assertFalse(session.metadata()['active'])
        receipt['connection_attempts'].clear()
        self.assertEqual(len(session.metadata()['connection_attempts']), len(egress.HOSTS))

    def test_cloud_api_and_other_global_sockets_are_unchanged(self):
        addresses = [('management.azure.com', 443), ('ec2.us-east-1.amazonaws.com', 443),
                     ('compute.googleapis.com', 443), ('iaas.us-ashburn-1.oraclecloud.com', 443),
                     ('registry-1.docker.io', 443), ('127.0.0.1', 8000)]
        with mock.patch.object(socket, 'create_connection') as original:
            with egress.registry_egress(self.load()) as session:
                for address in addresses:
                    socket.create_connection(address, timeout=7)
                    original.assert_called_with(address, timeout=7)
                self.assertFalse(session.metadata()['ssh_tcp_routing_used'])

    def test_active_handler_rejects_other_hosts_ports_and_proxy_tunnels(self):
        with mock.patch.object(socket, 'create_connection') as original:
            with egress.registry_egress(self.load()):
                handler = egress.https_handler(self.tls())
                for host in ('management.azure.com', '169.254.169.254', 'ghcr.io:8443', 'GHCR.IO', 'ghcr.io.'):
                    with self.subTest(host=host), self.assertRaises(egress.RegistryEgressError):
                        self.connection(handler, host)
                connection = self.connection(handler)
                connection.set_tunnel('ghcr.io', 443)
                with self.assertRaises(egress.RegistryEgressError):
                    connection.connect()
            original.assert_not_called()

    def test_active_opener_disables_ambient_proxies_without_reading_settings(self):
        with egress.registry_egress(self.load()):
            handler = egress.https_handler(self.tls())
            self.assertIsInstance(handler, urllib.request.ProxyHandler)
            self.assertEqual(handler.proxies, {})
            with mock.patch.object(urllib.request, 'getproxies', side_effect=AssertionError('Ambient proxies read')):
                opener = urllib.request.build_opener(handler)
            self.assertEqual([item for item in opener.handlers if isinstance(item, urllib.request.ProxyHandler)], [handler])

    def test_failed_forward_never_falls_back_to_direct_access(self):
        mapping = self.load()
        with mock.patch.object(socket, 'create_connection', side_effect=ConnectionRefusedError('no forward')) as original:
            with self.assertRaises(ConnectionRefusedError):
                with egress.registry_egress(mapping) as session:
                    self.connection(egress.https_handler(self.tls()), timeout=3).connect()
            original.assert_called_once_with(mapping['ghcr.io'], timeout=3, source_address=None)
            self.assertIs(socket.create_connection, original)
        self.assertEqual(session.metadata()['connection_attempts']['ghcr.io'], 1)
        self.assertEqual(session.metadata()['connection_successes']['ghcr.io'], 0)
        self.assertEqual(session.metadata()['failure_type'], 'ConnectionRefusedError')

    def test_base_exception_resets_context_and_handler_cannot_escape(self):
        with mock.patch.object(socket, 'create_connection') as original:
            with self.assertRaises(KeyboardInterrupt):
                with egress.registry_egress(self.load()) as session:
                    handler = egress.https_handler(self.tls())
                    connection = self.connection(handler)
                    raise KeyboardInterrupt()
            self.assertIs(socket.create_connection, original)
            self.assertIs(type(egress.https_handler()), urllib.request.HTTPSHandler)
            with self.assertRaises(egress.RegistryEgressError):
                self.connection(handler)
            with self.assertRaises(egress.RegistryEgressError):
                connection.connect()
            original.assert_not_called()
        self.assertFalse(session.metadata()['active'])
        self.assertEqual(session.metadata()['failure_type'], 'KeyboardInterrupt')

    def test_invalid_missing_unknown_duplicate_and_non_loopback_routes_rejected(self):
        bad = []
        value = copy.deepcopy(self.value)
        value['forwards'].pop('ghcr.io')
        bad.append(value)
        value = copy.deepcopy(self.value)
        value['forwards']['metadata.azure.internal'] = {'host': '127.0.0.1', 'port': 20000}
        bad.append(value)
        for host in ('localhost', '::1', '127.0.0.2', '0.0.0.0', '169.254.169.254', 'example.com'):
            value = copy.deepcopy(self.value)
            value['forwards']['ghcr.io']['host'] = host
            bad.append(value)
        for port in (True, False, 0, -1, 65536, 443.0, '443', None):
            value = copy.deepcopy(self.value)
            value['forwards']['ghcr.io']['port'] = port
            bad.append(value)
        value = copy.deepcopy(self.value)
        value['forwards']['ghcr.io']['port'] = value['forwards']['auth.docker.io']['port']
        bad.append(value)
        for field, item in (('schema_version', True), ('transport', 'http-proxy'), ('extra', True)):
            value = copy.deepcopy(self.value)
            value[field] = item
            bad.append(value)
        for index, value in enumerate(bad):
            with self.subTest(case=index):
                self.write(value)
                with self.assertRaises(egress.RegistryEgressError):
                    self.load()
        self.path.write_text(json.dumps(self.value)[:-1] + ',"schema_version":1}', encoding='utf-8')
        with self.assertRaises(egress.RegistryEgressError):
            self.load()

    def test_owned_private_single_link_file_and_real_parent_required(self):
        self.path.chmod(0o644)
        with self.assertRaises(egress.RegistryEgressError):
            self.load()
        self.path.chmod(0o600)
        link = self.root / 'link.json'
        link.symlink_to(self.path)
        with self.assertRaises(OSError):
            egress.load_mapping(link)
        hard = self.root / 'hard.json'
        os.link(self.path, hard)
        with self.assertRaises(egress.RegistryEgressError):
            self.load()
        hard.unlink()
        parent_link = self.root / 'linked-parent'
        parent_link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(egress.RegistryEgressError):
            egress.load_mapping(parent_link / self.path.name)

    def test_mapping_cannot_be_constructed_mutated_or_reused(self):
        mapping = self.load()
        with self.assertRaises(egress.RegistryEgressError):
            egress.RegistryEgressMapping()
        with self.assertRaises(egress.RegistryEgressError):
            mapping._routes = ()
        with egress.registry_egress(mapping):
            pass
        with self.assertRaises(egress.RegistryEgressError):
            with egress.registry_egress(mapping):
                self.fail('Consumed mapping reused.')
        changed = self.load()
        object.__setattr__(changed, '_routes', ())
        with self.assertRaises(egress.RegistryEgressError):
            with egress.registry_egress(changed):
                self.fail('Mutated mapping installed.')

    def test_nested_concurrent_and_foreign_context_use_are_rejected(self):
        first, second, errors = self.load(), self.load(), []
        with mock.patch.object(socket, 'create_connection') as original:
            with egress.registry_egress(first):
                handler = egress.https_handler(self.tls())
                with self.assertRaises(egress.RegistryEgressError):
                    with egress.registry_egress(second):
                        self.fail('Nested context installed.')

                def concurrent():
                    try:
                        with egress.registry_egress(second):
                            errors.append('unexpected success')
                    except egress.RegistryEgressError:
                        errors.append('refused')
                    try:
                        self.connection(handler).connect()
                    except egress.RegistryEgressError:
                        errors.append('foreign handler refused')
                thread = threading.Thread(target=concurrent)
                thread.start()
                thread.join(timeout=3)
                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, ['refused', 'foreign handler refused'])
            original.assert_not_called()

    def test_forked_process_cannot_load_install_or_use_context(self):
        mapping = self.load()
        with mock.patch.object(egress.os, 'getpid', return_value=egress._MODULE_PID + 1):
            with self.assertRaises(egress.RegistryEgressError):
                self.load()
            with self.assertRaises(egress.RegistryEgressError):
                with egress.registry_egress(mapping):
                    self.fail('Forked context installed.')
        with mock.patch.object(socket, 'create_connection') as original:
            with egress.registry_egress(mapping):
                connection = self.connection(egress.https_handler(self.tls()))
                with mock.patch.object(egress.os, 'getpid', return_value=egress._MODULE_PID + 1):
                    with self.assertRaises(egress.RegistryEgressError):
                        connection.connect()
            original.assert_not_called()

    def test_environment_proxy_and_trust_overrides_refused(self):
        for name in egress._FORBIDDEN_ENVIRONMENT:
            with self.subTest(name=name), mock.patch.dict(os.environ, {name: 'not-printed'}):
                with self.assertRaises(egress.RegistryEgressError):
                    with egress.registry_egress(self.load()):
                        self.fail('Override accepted.')

    def test_insecure_tls_context_refused_without_modification(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        with egress.registry_egress(self.load()):
            with self.assertRaises(egress.RegistryEgressError):
                egress.https_handler(context)
        self.assertFalse(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_NONE)

    def test_configuration_and_environment_changes_prevent_access(self):
        with mock.patch.object(socket, 'create_connection') as original:
            with self.assertRaises(egress.RegistryEgressError):
                with egress.registry_egress(self.load()):
                    connection = self.connection(egress.https_handler(self.tls()))
                    self.path.write_bytes(self.path.read_bytes() + b'\n')
                    connection.connect()
            original.assert_not_called()
        self.write(self.value)
        with mock.patch.object(socket, 'create_connection') as original:
            with self.assertRaises(egress.RegistryEgressError):
                with egress.registry_egress(self.load()):
                    connection = self.connection(egress.https_handler(self.tls()))
                    os.environ['HTTPS_PROXY'] = 'not-printed'
                    connection.connect()
            original.assert_not_called()

    def test_socket_closed_if_configuration_changes_during_connect(self):
        raw_socket = mock.Mock()

        def connect(*_args, **_kwargs):
            self.path.write_bytes(self.path.read_bytes() + b'\n')
            return raw_socket

        with mock.patch.object(socket, 'create_connection', side_effect=connect):
            with self.assertRaises(egress.RegistryEgressError):
                with egress.registry_egress(self.load()):
                    self.connection(egress.https_handler(self.tls())).connect()
        raw_socket.close.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
