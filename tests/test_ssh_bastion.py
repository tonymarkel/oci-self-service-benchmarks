import shlex
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from app import main


class _CompletedProcess:
    pid = 32101
    returncode = 0

    def poll(self):
        return self.returncode

    def communicate(self, timeout=None):
        return 'ok\n', ''


def _job(*, cancel_event=None):
    job = {
        'id': 'ssh-bastion',
        '_key': 'private-key-material',
        '_passphrase': None,
        'resources': {
            'ssh_user': 'benchmark',
            'azure_dsb_control_public_ip': '198.51.100.10',
            'azure_dsb_database_private_ip': '10.240.1.11',
        },
    }
    if cancel_event is not None:
        job['_cancel_event'] = cancel_event
    return job


def _proxy_arguments(arguments):
    option = next(
        item for item in arguments if item.startswith('ProxyCommand=')
    )
    return shlex.split(option.removeprefix('ProxyCommand='))


def _option_value(arguments, prefix):
    return next(item for item in arguments if item.startswith(prefix))


class SSHBastionTests(unittest.TestCase):
    def tearDown(self):
        main.job_cancel_events.clear()
        with main.job_processes_lock:
            main.job_processes.clear()

    def test_private_target_uses_local_proxy_with_same_identity_and_known_hosts(self):
        job = _job()
        captured = {}

        def popen(arguments, **kwargs):
            captured['arguments'] = arguments
            captured['kwargs'] = kwargs
            return _CompletedProcess()

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', side_effect=popen),
            ):
                output = main.ssh(
                    job,
                    'true',
                    host_key='azure_dsb_database_private_ip',
                    jump_host_key='azure_dsb_control_public_ip',
                )
                known_hosts_path = (
                    Path(directory) / job['id'] / 'known_hosts'
                )
                self.assertEqual(
                    known_hosts_path.stat().st_mode & 0o777,
                    0o600,
                )

        self.assertEqual(output, 'ok\n')
        arguments = captured['arguments']
        proxy_arguments = _proxy_arguments(arguments)
        self.assertEqual(
            arguments[-2:],
            ['benchmark@10.240.1.11', 'true'],
        )
        self.assertEqual(proxy_arguments[0], 'ssh')
        self.assertEqual(
            proxy_arguments[-3:],
            ['-W', '%h:%p', 'benchmark@198.51.100.10'],
        )
        outer_identity = arguments[arguments.index('-i') + 1]
        proxy_identity = proxy_arguments[proxy_arguments.index('-i') + 1]
        self.assertEqual(proxy_identity, outer_identity)
        outer_known_hosts = _option_value(
            arguments,
            'UserKnownHostsFile=',
        )
        proxy_known_hosts = _option_value(
            proxy_arguments,
            'UserKnownHostsFile=',
        )
        self.assertEqual(proxy_known_hosts, outer_known_hosts)
        self.assertTrue(
            outer_known_hosts.endswith('/ssh-bastion/known_hosts')
        )
        for option in (
            'ForwardAgent=no',
            'ClearAllForwardings=yes',
            'IdentityAgent=none',
            'StrictHostKeyChecking=accept-new',
            'GlobalKnownHostsFile=/dev/null',
            'PasswordAuthentication=no',
            'KbdInteractiveAuthentication=no',
            'PreferredAuthentications=publickey',
            'IdentitiesOnly=yes',
        ):
            self.assertIn(option, arguments)
            self.assertIn(option, proxy_arguments)
        self.assertNotIn(job['_key'], ' '.join(arguments))
        self.assertTrue(captured['kwargs']['start_new_session'])

    def test_jump_host_must_be_a_present_resource_key(self):
        job = _job()
        with (
            patch.object(main.subprocess, 'Popen') as popen,
            patch.object(main.tempfile, 'NamedTemporaryFile') as temporary,
        ):
            for jump_host_key in (
                'missing_control_public_ip',
                '198.51.100.10',
            ):
                with self.subTest(jump_host_key=jump_host_key):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        'SSH jump target',
                    ):
                        main.ssh(
                            job,
                            'true',
                            host_key='azure_dsb_database_private_ip',
                            jump_host_key=jump_host_key,
                        )
            with self.assertRaisesRegex(ValueError, 'non-empty resource key'):
                main.ssh(
                    job,
                    'true',
                    host_key='azure_dsb_database_private_ip',
                    jump_host_key='',
                )
        temporary.assert_not_called()
        popen.assert_not_called()

    def test_secret_stdin_remains_pipe_only_through_bastion(self):
        job = _job()
        secret = 'K10example::agent:secret-through-stdin'
        captured = {}

        class SecretPipe:
            def write(self, value):
                captured['secret'] = value

            def close(self):
                captured['closed'] = True

        process = _CompletedProcess()
        process.stdin = SecretPipe()

        def popen(arguments, **kwargs):
            captured['arguments'] = arguments
            captured['kwargs'] = kwargs
            return process

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', side_effect=popen),
            ):
                output = main.ssh(
                    job,
                    'read-secret',
                    host_key='azure_dsb_database_private_ip',
                    jump_host_key='azure_dsb_control_public_ip',
                    secret_stdin=secret,
                )

        self.assertEqual(output, 'ok\n')
        self.assertIs(captured['kwargs']['stdin'], subprocess.PIPE)
        self.assertEqual(captured['secret'], secret)
        self.assertTrue(captured['closed'])
        self.assertNotIn(secret, captured['arguments'])
        self.assertNotIn(secret, captured['kwargs']['env'].values())
        self.assertTrue(any(
            item.startswith('ProxyCommand=')
            for item in captured['arguments']
        ))

    def test_nonsecret_stdin_streams_a_bounded_manifest_without_argv(self):
        job = _job()
        manifest = '{"apiVersion":"v1","kind":"Namespace"}\n' * 200
        captured = {}

        class ManifestProcess(_CompletedProcess):
            def communicate(self, input=None, timeout=None):
                captured['manifest'] = input
                return 'ok\n', ''

        process = ManifestProcess()

        def popen(arguments, **kwargs):
            captured['arguments'] = arguments
            captured['kwargs'] = kwargs
            return process

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', side_effect=popen),
            ):
                output = main.ssh(
                    job,
                    'sudo k3s kubectl apply -f -',
                    host_key='azure_dsb_control_public_ip',
                    stdin_text=manifest,
                )

        self.assertEqual(output, 'ok\n')
        self.assertIs(captured['kwargs']['stdin'], subprocess.PIPE)
        self.assertEqual(captured['manifest'], manifest)
        self.assertNotIn(manifest, captured['arguments'])

    def test_standard_input_contract_rejects_ambiguous_or_oversized_data(self):
        job = _job()
        with patch.object(main.subprocess, 'Popen') as popen:
            with self.assertRaisesRegex(ValueError, 'mutually exclusive'):
                main.ssh(
                    job,
                    'true',
                    host_key='azure_dsb_control_public_ip',
                    secret_stdin='secret',
                    stdin_text='manifest',
                )
            with self.assertRaisesRegex(ValueError, 'at most 1 MiB'):
                main.ssh(
                    job,
                    'true',
                    host_key='azure_dsb_control_public_ip',
                    stdin_text='x' * (1024 * 1024 + 1),
                )
            with self.assertRaisesRegex(ValueError, 'non-empty UTF-8'):
                main.ssh(
                    job,
                    'true',
                    host_key='azure_dsb_control_public_ip',
                    stdin_text='',
                )
        popen.assert_not_called()

    def test_nonsecret_stdin_is_submitted_only_once_across_poll_timeouts(self):
        job = _job()
        manifest = '{"kind":"List","items":[]}\n'

        class Process(_CompletedProcess):
            def __init__(self):
                self.inputs = []

            def communicate(self, input=None, timeout=None):
                self.inputs.append(input)
                if len(self.inputs) == 1:
                    raise subprocess.TimeoutExpired('ssh', timeout)
                return 'applied\n', ''

        process = Process()
        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', return_value=process),
            ):
                output = main.ssh(
                    job,
                    'sudo k3s kubectl apply -f -',
                    host_key='azure_dsb_control_public_ip',
                    stdin_text=manifest,
                )

        self.assertEqual(output, 'applied\n')
        self.assertEqual(process.inputs, [manifest, None])

    def test_transport_retry_reuses_the_same_bastion_contract(self):
        job = _job()
        calls = []

        class Process(_CompletedProcess):
            def __init__(self, returncode, output):
                self.returncode = returncode
                self.output = output

            def communicate(self, timeout=None):
                return self.output, ''

        processes = iter((Process(255, 'not ready'), Process(0, 'ready\n')))

        def popen(arguments, **_kwargs):
            calls.append(arguments)
            return next(processes)

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', side_effect=popen),
                patch.object(main.time, 'sleep') as sleep,
            ):
                output = main.ssh(
                    job,
                    'true',
                    host_key='azure_dsb_database_private_ip',
                    jump_host_key='azure_dsb_control_public_ip',
                    transport_attempts=2,
                    transport_retry_delay_seconds=0,
                )

        self.assertEqual(output, 'ready\n')
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], calls[1])
        self.assertTrue(any(
            item.startswith('ProxyCommand=') for item in calls[0]
        ))
        sleep.assert_called_once_with(0)

    def test_sensitive_remote_output_is_redacted_on_nonzero_exit(self):
        job = _job()
        secret = 'K10' + ('a' * 64) + '::agent-password'

        class Process(_CompletedProcess):
            returncode = 23

            def communicate(self, timeout=None):
                return secret + '\n', 'also-sensitive\n'

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', return_value=Process()),
            ):
                with self.assertRaises(main.SSHCommandError) as raised:
                    main.ssh(
                        job,
                        'read-sensitive-value',
                        host_key='azure_dsb_control_public_ip',
                        sensitive_output=True,
                        transport_attempts=1,
                    )

        self.assertEqual(raised.exception.output, '')
        self.assertEqual(raised.exception.returncode, 23)
        self.assertIn('remote output was redacted', str(raised.exception))
        self.assertNotIn(secret, str(raised.exception))
        self.assertNotIn('also-sensitive', str(raised.exception))

    def test_sensitive_partial_output_is_redacted_on_timeout(self):
        job = _job()
        secret = 'K10' + ('b' * 64) + '::partial-agent-password'

        class Process:
            pid = 32103
            returncode = None

            def poll(self):
                return self.returncode

            def communicate(self, timeout=None):
                raise AssertionError('timeout path should terminate first')

        process = Process()

        def terminate(target):
            target.returncode = -15
            return secret + '\n', 'sensitive-stderr\n'

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', return_value=process),
                patch.object(main.time, 'monotonic', side_effect=(0, 1)),
                patch.object(
                    main,
                    'terminate_process_and_collect',
                    side_effect=terminate,
                ),
            ):
                with self.assertRaises(main.SSHCommandError) as raised:
                    main.ssh(
                        job,
                        'read-sensitive-value',
                        host_key='azure_dsb_control_public_ip',
                        timeout=0,
                        sensitive_output=True,
                        transport_attempts=1,
                    )

        self.assertEqual(raised.exception.output, '')
        self.assertNotIn(secret, str(raised.exception))
        self.assertNotIn('sensitive-stderr', str(raised.exception))

    def test_cancellation_terminates_the_bastioned_process_group(self):
        cancel = threading.Event()
        job = _job(cancel_event=cancel)
        captured = {}

        class Process:
            pid = 32102
            returncode = None

            def poll(self):
                return self.returncode

            def communicate(self, timeout=None):
                if self.returncode is not None:
                    return 'partial output\n', ''
                cancel.set()
                raise subprocess.TimeoutExpired('ssh', timeout)

        process = Process()

        def popen(arguments, **kwargs):
            captured['arguments'] = arguments
            captured['kwargs'] = kwargs
            return process

        def terminate(target):
            self.assertIs(target, process)
            target.returncode = -15

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(main, 'RUNS', Path(directory)),
                patch.object(main.subprocess, 'Popen', side_effect=popen),
                patch.object(
                    main,
                    'signal_process_termination',
                    side_effect=terminate,
                ) as terminate_process,
            ):
                with self.assertRaises(main.RunCancelled):
                    main.ssh(
                        job,
                        'long-running-command',
                        host_key='azure_dsb_database_private_ip',
                        jump_host_key='azure_dsb_control_public_ip',
                        timeout=60,
                    )

        terminate_process.assert_called_once_with(process)
        self.assertTrue(captured['kwargs']['start_new_session'])
        self.assertTrue(any(
            item.startswith('ProxyCommand=')
            for item in captured['arguments']
        ))
        self.assertNotIn(job['id'], main.job_processes)


if __name__ == '__main__':
    unittest.main()
