import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError

from app import main
from app.models import BenchmarkPlan, Iperf3Options


def plan(**overrides):
    values = {
        'region': 'us-ashburn-1',
        'shape': 'VM.Standard.E5.Flex',
        'ocpus': 8,
        'memory_gb': 32,
        'ssh_private_key': 'private',
        'ssh_public_key': 'public',
        'storage': {'additional_volume': False},
        'benchmarks': ['iperf3'],
    }
    values.update(overrides)
    return BenchmarkPlan(**values)


def destination_port(rule, option_name):
    options = getattr(rule, option_name, None)
    port_range = getattr(options, 'destination_port_range', None)
    return getattr(port_range, 'min', None)


class Iperf3PlanTests(unittest.TestCase):
    def test_defaults_to_tcp(self):
        self.assertEqual(Iperf3Options().protocols, ['tcp'])
        self.assertEqual(plan().iperf3.protocols, ['tcp'])

    def test_protocols_are_typed_unique_and_required_when_selected(self):
        with self.assertRaises(ValidationError):
            plan(iperf3={'protocols': ['icmp']})

        with self.assertRaisesRegex(ValidationError, 'must be unique'):
            plan(iperf3={'protocols': ['tcp', 'tcp']})

        with self.assertRaisesRegex(ValidationError, 'at least one iperf3'):
            plan(iperf3={'protocols': []})

        unselected = plan(
            benchmarks=['stream'],
            iperf3={'protocols': []},
        )
        self.assertEqual(unselected.iperf3.protocols, [])

    def test_legacy_top_level_ids_are_canonicalized(self):
        migrated = plan(
            benchmarks=[
                'stream',
                'iperf_udp',
                'iperf_tcp',
                'phoronix',
            ],
        )

        self.assertEqual(
            migrated.benchmarks,
            ['stream', 'iperf3', 'phoronix'],
        )
        self.assertEqual(migrated.iperf3.protocols, ['tcp', 'udp'])

    def test_mixed_legacy_and_canonical_selections_merge_once(self):
        migrated = plan(
            benchmarks=['iperf3', 'iperf_sctp', 'fio'],
            iperf3={'protocols': ['udp']},
        )

        self.assertEqual(migrated.benchmarks, ['iperf3', 'fio'])
        self.assertEqual(migrated.iperf3.protocols, ['udp', 'sctp'])


class Iperf3CatalogAndExpansionTests(unittest.TestCase):
    def test_catalog_exposes_one_parent_and_ordered_nested_protocols(self):
        payload = main.catalog()
        top_level_ids = [item['id'] for item in payload['benchmarks']]

        self.assertEqual(top_level_ids.count('iperf3'), 1)
        self.assertFalse(
            {'iperf_tcp', 'iperf_udp', 'iperf_sctp'} & set(top_level_ids)
        )
        self.assertEqual(
            [item['id'] for item in payload['iperf3_protocols']],
            ['tcp', 'udp', 'sctp'],
        )

    def test_expansion_preserves_selected_protocol_order(self):
        selected_plan = plan(
            iperf3={'protocols': ['tcp', 'udp', 'sctp']},
        )

        self.assertEqual(
            main.selected_iperf3_protocols(selected_plan),
            ('tcp', 'udp', 'sctp'),
        )
        self.assertEqual(
            main.expanded_iperf3_result_ids(selected_plan),
            ('iperf_tcp', 'iperf_udp', 'iperf_sctp'),
        )
        self.assertEqual(
            main.expanded_benchmark_ids(selected_plan),
            {'iperf3', 'iperf_tcp', 'iperf_udp', 'iperf_sctp'},
        )


class Iperf3SecurityAndBootstrapTests(unittest.TestCase):
    def test_security_rules_are_conditional_and_protocol_specific(self):
        no_iperf = main.benchmark_security_rules(False)
        tcp = main.benchmark_security_rules(False, ('tcp',))
        udp = main.benchmark_security_rules(False, ('udp',))
        sctp = main.benchmark_security_rules(False, ('sctp',))

        self.assertEqual(
            [(rule.protocol, rule.source) for rule in no_iperf],
            [('6', '0.0.0.0/0')],
        )

        tcp_control = [
            rule for rule in tcp
            if rule.protocol == '6' and rule.source == '10.42.1.0/24'
        ]
        self.assertEqual(len(tcp_control), 1)
        self.assertEqual(destination_port(tcp_control[0], 'tcp_options'), 5201)
        self.assertFalse(any(rule.protocol in {'17', '132'} for rule in tcp))

        udp_control = [
            rule for rule in udp
            if rule.protocol == '6' and rule.source == '10.42.1.0/24'
        ]
        udp_data = [rule for rule in udp if rule.protocol == '17']
        self.assertEqual(len(udp_control), 1)
        self.assertEqual(len(udp_data), 1)
        self.assertEqual(udp_data[0].source, '10.42.1.0/24')
        self.assertEqual(destination_port(udp_data[0], 'udp_options'), 5201)

        sctp_control = [
            rule for rule in sctp
            if rule.protocol == '6' and rule.source == '10.42.1.0/24'
        ]
        sctp_data = [rule for rule in sctp if rule.protocol == '132']
        self.assertEqual(len(sctp_control), 1)
        self.assertEqual(len(sctp_data), 1)
        self.assertEqual(sctp_data[0].source, '10.42.1.0/24')
        self.assertIsNone(getattr(sctp_data[0], 'tcp_options', None))
        self.assertIsNone(getattr(sctp_data[0], 'udp_options', None))

    def test_peer_cloud_init_is_protocol_specific_and_has_one_server(self):
        tcp = main.iperf_peer_cloud_init(('tcp',))
        udp = main.iperf_peer_cloud_init(('udp',))
        sctp = main.iperf_peer_cloud_init(('sctp',))
        all_protocols = main.iperf_peer_cloud_init(('tcp', 'udp', 'sctp'))

        for script in (tcp, udp, sctp, all_protocols):
            self.assertEqual(
                script.count('ExecStart=/usr/bin/iperf3 -s'),
                1,
            )
            self.assertIn('5201/tcp', script)
            self.assertIn('PACKAGES_READY=false', script)
            self.assertIn('PACKAGES_READY=true', script)
            self.assertIn('if [ "$PACKAGES_READY" != true ]', script)

        self.assertNotIn('5201/udp', tcp)
        self.assertNotIn('5201/sctp', tcp)
        self.assertNotIn('lksctp-tools', tcp)

        self.assertIn('5201/udp', udp)
        self.assertNotIn('5201/sctp', udp)
        self.assertNotIn('lksctp-tools', udp)

        self.assertIn('lksctp-tools', sctp)
        self.assertIn('modprobe sctp', sctp)
        self.assertIn(
            'kernel-uek-modules-extra-$KERNEL_RELEASE',
            sctp,
        )
        self.assertIn('kernel-modules-extra-$KERNEL_RELEASE', sctp)
        self.assertIn("grep -q -- '--sctp'", sctp)
        self.assertIn('5201/sctp', sctp)
        self.assertNotIn('5201/udp', sctp)

        self.assertIn('5201/udp', all_protocols)
        self.assertIn('5201/sctp', all_protocols)

        with tempfile.NamedTemporaryFile(mode='w') as script:
            script.write(sctp)
            script.flush()
            subprocess.run(
                ['bash', '-n', script.name],
                check=True,
                capture_output=True,
                text=True,
            )

    def test_sctp_module_package_matches_the_running_kernel_family(self):
        root = main.sctp_kernel_support_command()
        runner = main.sctp_kernel_support_command(use_sudo=True)

        for command in (root, runner):
            self.assertTrue(command.startswith('set -euo pipefail; '))
            self.assertIn('KERNEL_RELEASE="$(uname -r)"', command)
            self.assertIn('*.el9uek.aarch64.64k)', command)
            self.assertIn(
                'SCTP_MODULE_PACKAGE="kernel-uek64k-modules-extra-'
                '${KERNEL_RELEASE%.64k}"',
                command,
            )
            self.assertIn('*.el9uek.*)', command)
            self.assertIn(
                'SCTP_MODULE_PACKAGE="kernel-uek-modules-extra-'
                '$KERNEL_RELEASE"',
                command,
            )
            self.assertIn(
                'SCTP_MODULE_PACKAGE="kernel-modules-extra-'
                '$KERNEL_RELEASE"',
                command,
            )
            self.assertIn('for attempt in 1 2 3', command)
            self.assertIn('install "$SCTP_MODULE_PACKAGE"', command)
            self.assertIn('modinfo -n sctp', command)
            self.assertIn('test -d /proc/net/sctp', command)
            self.assertIn('could not be installed', command)
            self.assertIn('Unsupported Oracle Linux kernel release', command)

        self.assertNotIn('sudo modprobe', root)
        self.assertIn('sudo modprobe', runner)
        self.assertIn('sudo dnf', runner)

    def test_sctp_module_load_failure_is_not_masked_by_later_output(self):
        with tempfile.TemporaryDirectory() as directory:
            fake_bin = Path(directory)
            scripts = {
                'uname': '#!/bin/sh\necho 6.12.0-test.el9uek.x86_64\n',
                'modprobe': '#!/bin/sh\nexit 1\n',
                'dnf': '#!/bin/sh\nexit 0\n',
                'modinfo': '#!/bin/sh\necho /fake/sctp.ko.xz\n',
            }
            for name, contents in scripts.items():
                path = fake_bin / name
                path.write_text(contents)
                path.chmod(0o755)
            environment = os.environ.copy()
            environment['PATH'] = f'{fake_bin}:{environment["PATH"]}'

            result = subprocess.run(
                ['bash', '-c', main.sctp_kernel_support_command()],
                capture_output=True,
                text=True,
                env=environment,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn('SCTP still cannot be loaded', result.stderr)

    def test_runner_installs_and_verifies_sctp_support(self):
        selected_plan = plan(iperf3={'protocols': ['sctp']})
        job = {'events': []}

        with (
            patch.object(main, 'dnf_install') as install,
            patch.object(main, 'ssh', return_value='ready') as ssh,
        ):
            main.install_benchmark_tools(
                job,
                selected_plan,
                main.expanded_benchmark_ids(selected_plan),
            )

        self.assertEqual(
            install.call_args.args[1],
            {'iperf3', 'lksctp-tools'},
        )
        verification = ssh.call_args.args[1]
        self.assertIn('sudo modprobe sctp', verification)
        self.assertIn(
            'kernel-uek-modules-extra-$KERNEL_RELEASE',
            verification,
        )
        self.assertIn('iperf3 --help', verification)
        self.assertIn('--sctp', verification)

    def test_sctp_readiness_checks_control_and_data_paths(self):
        tcp = main.iperf_peer_readiness_command(
            '10.42.2.8',
            ('tcp',),
        )
        sctp = main.iperf_peer_readiness_command(
            '10.42.2.8',
            ('sctp',),
        )

        self.assertIn('/dev/tcp/$PEER_IP/5201', tcp)
        self.assertNotIn('--sctp', tcp)
        self.assertIn('/dev/tcp/$PEER_IP/5201', sctp)
        self.assertIn(
            'iperf3 -c "$PEER_IP" --sctp -t 1 -J',
            sctp,
        )
        self.assertIn('SCTP data path is ready', sctp)


class Iperf3ExecutionAndCompatibilityTests(unittest.TestCase):
    def test_run_uses_protocol_order_commands_and_unlimited_report_output(self):
        selected_plan = plan(
            iperf3={'protocols': ['tcp', 'udp', 'sctp']},
        )
        job = {
            'events': [],
            'results': [],
            'resources': {'peer_private_ip': '10.42.2.9'},
        }

        with (
            patch.object(main, 'wait_for_guest_readiness'),
            patch.object(main, 'install_benchmark_tools'),
            patch.object(main, 'wait_for_iperf_peer') as ready,
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_benchmarks(job, selected_plan)

        ready.assert_called_once_with(job, ('tcp', 'udp', 'sctp'))
        self.assertEqual(
            [item.args[1] for item in execute.call_args_list],
            ['iperf_tcp', 'iperf_udp', 'iperf_sctp'],
        )
        self.assertEqual(
            [item.args[2] for item in execute.call_args_list],
            ['iperf3 — TCP', 'iperf3 — UDP', 'iperf3 — SCTP'],
        )
        commands = [item.args[3] for item in execute.call_args_list]
        self.assertEqual(
            commands,
            [
                'iperf3 -4 -c 10.42.2.9 -t 60 -P 4 -J',
                'iperf3 -4 -c 10.42.2.9 -u -b 0 -t 60 -J',
                'iperf3 -4 -c 10.42.2.9 --sctp -t 60 -P 4 -J',
            ],
        )
        self.assertTrue(
            all(
                item.kwargs.get('output_limit') is None
                for item in execute.call_args_list
            )
        )

    def test_report_plan_migrates_legacy_selection_without_rewriting_it(self):
        saved = {
            'region': 'us-ashburn-1',
            'shape': 'VM.Standard.E5.Flex',
            'benchmarks': ['stream', 'iperf_udp', 'iperf_tcp'],
        }
        with tempfile.TemporaryDirectory() as directory:
            runs = Path(directory)
            job_id = 'legacy-iperf3'
            run = runs / job_id
            run.mkdir()
            path = run / 'plan.json'
            original = json.dumps(saved, indent=2)
            path.write_text(original)

            with (
                patch.object(main, 'RUNS', runs),
                patch.dict(main.jobs, {}, clear=True),
            ):
                restored = main.report_plan(job_id)

            self.assertEqual(restored['benchmarks'], ['stream', 'iperf3'])
            self.assertEqual(
                restored['iperf3']['protocols'],
                ['tcp', 'udp'],
            )
            self.assertEqual(path.read_text(), original)


if __name__ == '__main__':
    unittest.main()
