"""Private CLI transport selection only, never a registry or cloud operation."""

from contextlib import contextmanager
from unittest import mock
import unittest

from scripts import qualify_deathstarbench_hotel_initialization as cli


class HotelRegistryEgressCliTests(unittest.TestCase):
    def arguments(self, *extra):
        values = ['--plan', 'synthetic-plan.json']
        for field in cli._ARTIFACT_OPTIONS:
            values.extend(['--' + field.replace('_', '-'), 'synthetic-input'])
        return cli._parser().parse_args([*values, *extra])

    def test_default_transport_leaves_existing_workflow_unchanged(self):
        args = self.arguments()
        with mock.patch.object(cli, '_run_private', return_value=7) as run:
            self.assertEqual(cli._run(args), 7)
        run.assert_called_once_with(args, store=None)

    def test_cleanup_only_refuses_transport_before_loading_state_or_network(self):
        args = cli._parser().parse_args([
            '--cleanup-only', '123456abcdef', '--registry-egress', 'synthetic-mapping.json'])
        with mock.patch.object(cli, '_run_private') as run, self.assertRaises(ValueError):
            cli._run(args)
        run.assert_not_called()

    def test_explicit_transport_scopes_whole_lifecycle_and_restores_on_exception(self):
        from scripts import deathstarbench_registry_egress as transport
        args = self.arguments('--registry-egress', 'synthetic-mapping.json')
        sentinel = object()
        events = []

        @contextmanager
        def scoped(mapping):
            self.assertIs(mapping, sentinel)
            events.append('enter')
            try:
                yield sentinel
            finally:
                events.append('exit')

        def invoke(actual, *, store, registry_transport):
            self.assertEqual(events, ['enter'])
            self.assertIs(actual, args)
            self.assertIs(registry_transport, sentinel)
            raise KeyboardInterrupt('simulated private qualification interruption')

        with mock.patch.object(transport, 'load_mapping', return_value=sentinel) as load, \
                mock.patch.object(transport, 'registry_egress', side_effect=scoped), \
                mock.patch.object(cli, '_run_private', side_effect=invoke), \
                self.assertRaises(KeyboardInterrupt):
            cli._run(args)
        self.assertEqual(events, ['enter', 'exit'])
        load.assert_called_once_with(args.registry_egress)

    def test_lost_transport_observation_cannot_prevent_cleanup_state_persistence(self):
        # A changed/deleted route file still rejects new registry requests,
        # but its optional observation cannot block the same persist callback
        # that saves the exact cleanup source before the provider delete.
        original = {'historical_observation_only': True, 'connection_successes': {'ghcr.io': 2}}
        value = {'registry_egress_observation': original, 'cleanup_source': {'owned': True}}
        saved = []
        transport = mock.Mock()
        transport.metadata.side_effect = FileNotFoundError('simulated deleted mapping')
        cli._refresh_registry_observation(value, transport)
        saved.append(dict(value))
        self.assertIs(value['registry_egress_observation'], original)
        self.assertEqual(saved[0]['cleanup_source'], {'owned': True})
        self.assertEqual(value['registry_egress_observation_refresh_failure'], {
            'historical_observation_only': True, 'qualification_authority': False,
            'error_type': 'FileNotFoundError',
        })

    def test_observation_refresh_does_not_swallow_operator_cancellation(self):
        transport = mock.Mock()
        transport.metadata.side_effect = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            cli._refresh_registry_observation({}, transport)
