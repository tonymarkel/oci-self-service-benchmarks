import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from app import k3s_runtime as runtime
from app.deathstarbench_contract import K3S_VERSION
from app.k3s_runtime import (
    ExpectedK3sNode,
    K3S_AGENT_UNIT,
    K3S_AGENT_TOKEN_FILE,
    K3S_ARTIFACTS,
    K3S_BINARY,
    K3S_CLUSTER_DNS_IP,
    K3S_OWNERSHIP_MARKER,
    K3S_SELINUX_SHA256,
    K3S_SELINUX_URL,
    K3S_SERVER_UNIT,
    K3S_SERVER_TOKEN_FILE,
    agent_join_command,
    agent_readiness_command,
    agent_unit,
    artifact_for_architecture,
    artifact_install_command,
    cluster_nodes_readiness_command,
    control_tokens_initialize_command,
    host_preflight_command,
    normalized_architecture,
    rocky_host_prepare_command,
    secure_agent_token_ca_sha256,
    secure_agent_token_read_command,
    server_ca_sha256_read_command,
    server_install_command,
    server_readiness_command,
    server_unit,
    system_services_pin_command,
    system_services_readiness_command,
    token_initialize_command,
    token_initialize_and_read_command,
    token_install_command,
    uninstall_command,
)


class K3sRuntimeCommandTests(unittest.TestCase):
    @staticmethod
    def expected_nodes():
        return (
            ExpectedK3sNode(
                'dsb-control', 'control', '10.20.0.10', 'x86_64'
            ),
            ExpectedK3sNode(
                'dsb-app', 'application', '10.20.0.11', 'aarch64'
            ),
            ExpectedK3sNode(
                'dsb-cache', 'cache', '10.20.0.12', 'x86_64'
            ),
            ExpectedK3sNode(
                'dsb-database', 'database', '10.20.0.13', 'x86_64'
            ),
        )

    def test_release_assets_are_exact_and_checksum_pinned(self):
        amd64 = artifact_for_architecture('x86_64')
        arm64 = artifact_for_architecture('aarch64')

        self.assertEqual(normalized_architecture('amd64'), 'x86_64')
        self.assertEqual(normalized_architecture('arm64'), 'aarch64')
        self.assertEqual(amd64.asset_name, 'k3s')
        self.assertEqual(arm64.asset_name, 'k3s-arm64')
        self.assertEqual(
            amd64.sha256,
            '835873f37245fc615f547a2fe2af9402a347875f13fa64a1f136de644955ea3f',
        )
        self.assertEqual(
            arm64.sha256,
            'c920706346d5ad4e5cd3c7bf1bb09ce71ebe07fec829e513e40f1caf98aed8bb',
        )
        self.assertEqual(
            amd64.images_sha256,
            '9024613e2d468c51ba0e5ba21898604c41339354449fb3338cc6a7931189eb6a',
        )
        self.assertEqual(
            arm64.images_sha256,
            '9d3c4c2197bcf857ca17633aa393bad683cc982ddd408620f93036a3cca953b5',
        )
        self.assertIn(K3S_VERSION.replace('+', '%2B'), amd64.url)
        self.assertIn(K3S_VERSION.replace('+', '%2B'), arm64.url)
        with self.assertRaises(TypeError):
            K3S_ARTIFACTS['x86_64'] = arm64

    def test_artifact_commands_use_no_moving_channel_or_pipe_installer(self):
        for architecture in ('x86_64', 'aarch64'):
            with self.subTest(architecture=architecture):
                artifact = artifact_for_architecture(architecture)
                command = artifact_install_command(architecture)

                self.assertIn(artifact.url, command)
                self.assertIn(artifact.sha256, command)
                self.assertIn(artifact.images_url, command)
                self.assertIn(artifact.images_sha256, command)
                self.assertIn('sha256sum -c -', command)
                self.assertIn('--connect-timeout 20', command)
                self.assertIn('--max-time 600', command)
                self.assertIn('mktemp -d', command)
                self.assertIn('sudo install -o root -g root -m 0755', command)
                self.assertIn('exit 0; fi', command)
                hash_gate = (
                    'if [ "$CURRENT_SHA256" = "$EXPECTED_SHA256" ]; then '
                    'CURRENT_VERSION=$("$DESTINATION" --version'
                )
                self.assertIn(hash_gate, command)
                self.assertNotIn('get.k3s.io', command)
                self.assertNotIn('stable', command.lower())
                self.assertNotRegex(command, r'curl[^;]*\|\s*(?:ba)?sh')

    def test_scoped_tokens_are_read_from_stdin_and_installed_root_only(self):
        server = token_install_command('server')
        agent = token_install_command('agent')

        for command, path in (
            (server, K3S_SERVER_TOKEN_FILE),
            (agent, K3S_AGENT_TOKEN_FILE),
        ):
            self.assertIn('TOKEN=$(cat)', command)
            self.assertIn('-o root -g root -m 0600', command)
            self.assertIn(path, command)
            self.assertIn(K3S_OWNERSHIP_MARKER, command)
            self.assertIn('600:0:0', command)
            self.assertIn('cmp -s', command)
            self.assertNotIn('echo "$TOKEN"', command)
        self.assertIn(f'TOKEN_FILE={K3S_SERVER_TOKEN_FILE}', server)
        self.assertIn(f'TOKEN_FILE={K3S_AGENT_TOKEN_FILE}', agent)
        with self.assertRaises(ValueError):
            token_install_command('shared')

    def test_rocky_host_preparation_pins_selinux_policy_and_network_prereqs(self):
        control = rocky_host_prepare_command('control')
        database = rocky_host_prepare_command('database')

        for command in (control, database):
            self.assertIn(K3S_SELINUX_URL, command)
            self.assertIn(K3S_SELINUX_SHA256, command)
            self.assertIn('sha256sum -c -', command)
            self.assertIn('container-selinux', command)
            self.assertIn('modprobe overlay', command)
            self.assertIn('modprobe br_netfilter', command)
            self.assertIn('sysctl -n net.ipv4.ip_forward', command)
            self.assertIn('/etc/sysctl.d/90-deathstarbench-k3s.conf', command)
            self.assertIn('systemctl disable --now firewalld.service', command)
            self.assertNotRegex(
                command,
                r'dnf[^;]*\binstall\b[^;]*\bcurl\b',
            )
            self.assertIn('for COMMAND in curl', command)
            self.assertIn(
                'lsmod modprobe; do command -v "$COMMAND"',
                command,
            )
            self.assertNotIn('rpm.rancher.io/k3s/latest', command)
            self.assertNotRegex(command, r'curl[^;]*\|\s*(?:ba)?sh')
        self.assertNotIn('xfsprogs', control)
        self.assertIn('xfsprogs', database)
        self.assertNotIn('policycoreutils-python-utils', control)
        self.assertIn('policycoreutils-python-utils', database)

    def test_control_tokens_are_idempotent_and_agents_use_ca_bound_token(self):
        initializer = token_initialize_command('agent')
        control_initializer = control_tokens_initialize_command()
        recovery = token_initialize_and_read_command('agent')
        secure_reader = secure_agent_token_read_command()
        ca_reader = server_ca_sha256_read_command()
        ca_sha = 'a' * 64
        secure_token = f'K10{ca_sha}::agent-password'

        self.assertIn('/dev/urandom', initializer)
        self.assertIn('if sudo test -e "$OWNERSHIP_MARKER"', initializer)
        self.assertIn(K3S_SERVER_TOKEN_FILE, control_initializer)
        self.assertIn(K3S_AGENT_TOKEN_FILE, control_initializer)
        self.assertIn('Unowned K3s runtime state already exists', initializer)
        self.assertNotIn('sudo cat "$TOKEN_FILE"', initializer.split('; ')[-1])
        self.assertTrue(recovery.endswith('sudo cat "$TOKEN_FILE"'))
        self.assertIn('/var/lib/rancher/k3s/server/agent-token', secure_reader)
        self.assertIn('/var/lib/rancher/k3s/server/tls/server-ca.crt', secure_reader)
        self.assertIn('/var/lib/rancher/k3s/server/tls/server-ca.crt', ca_reader)
        self.assertNotIn('/var/lib/rancher/k3s/server/agent-token', ca_reader)
        self.assertIn('test ${#CA_SHA256} -eq 64', ca_reader)
        subprocess.run(
            ['bash', '-n'],
            input=ca_reader,
            text=True,
            check=True,
            capture_output=True,
        )
        self.assertEqual(secure_agent_token_ca_sha256(secure_token), ca_sha)
        with self.assertRaisesRegex(ValueError, 'CA-bound'):
            secure_agent_token_ca_sha256('b' * 64)

    def test_token_shell_refuses_rotation_foreign_and_partial_state(self):
        def executable(path, body):
            path.write_text(body)
            path.chmod(0o755)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tools = root / 'bin'
            tools.mkdir()
            executable(
                tools / 'sudo',
                '#!/bin/sh\nexec "$@"\n',
            )
            executable(
                tools / 'systemctl',
                '#!/bin/sh\nexit 3\n',
            )
            executable(
                tools / 'stat',
                '#!/bin/sh\n'
                'target=""\nfor argument in "$@"; do target="$argument"; done\n'
                'if mode=$(/usr/bin/stat -c %a "$target" 2>/dev/null); then\n'
                '  :\nelse\n'
                '  mode=$(/usr/bin/stat -f %Lp "$target")\nfi\n'
                'printf "%s:0:0\\n" "$mode"\n',
            )
            executable(
                tools / 'install',
                '#!/bin/sh\n'
                'mode=\ndirectory=false\n'
                'while [ "$#" -gt 0 ]; do\n'
                '  case "$1" in\n'
                '    -d) directory=true; shift;;\n'
                '    -o|-g) shift 2;;\n'
                '    -m) mode="$2"; shift 2;;\n'
                '    *) break;;\n'
                '  esac\n'
                'done\n'
                'if [ "$directory" = true ]; then\n'
                '  for target in "$@"; do\n'
                '    /bin/mkdir -p "$target"\n'
                '    /bin/chmod "$mode" "$target"\n'
                '  done\n'
                'else\n'
                '  source_file="$1"\n  destination="$2"\n'
                '  /bin/mkdir -p "$(dirname "$destination")"\n'
                '  /bin/cp "$source_file" "$destination"\n'
                '  /bin/chmod "$mode" "$destination"\n'
                'fi\n',
            )
            environment = os.environ.copy()
            environment['PATH'] = f'{tools}{os.pathsep}{environment["PATH"]}'

            def state(root_path):
                token_directory = root_path / 'tokens'
                return {
                    'K3S_OWNERSHIP_DIRECTORY': str(root_path),
                    'K3S_TOKEN_DIRECTORY': str(token_directory),
                    'K3S_SERVER_TOKEN_FILE': str(token_directory / 'server'),
                    'K3S_AGENT_TOKEN_FILE': str(token_directory / 'agent'),
                    'K3S_OWNERSHIP_MARKER': str(token_directory / 'owner'),
                    'K3S_SERVER_UNIT': str(root_path / 'units/server.service'),
                    'K3S_AGENT_UNIT': str(root_path / 'units/agent.service'),
                    'K3S_SERVER_STATE_DIRECTORY': str(root_path / 'server-state'),
                    'K3S_AGENT_KUBECONFIG': str(root_path / 'agent-kubeconfig'),
                    'K3S_CONFIG_DIRECTORY': str(root_path / 'config'),
                }

            def run(command, standard_input=''):
                return subprocess.run(
                    ['bash', '-c', command],
                    input=standard_input,
                    text=True,
                    capture_output=True,
                    env=environment,
                )

            worker_state = state(root / 'worker')
            with patch.multiple(runtime, **worker_state):
                worker_command = runtime.token_install_command('agent')
            agent_file = Path(worker_state['K3S_AGENT_TOKEN_FILE'])
            marker = Path(worker_state['K3S_OWNERSHIP_MARKER'])
            agent_token = 'K10' + ('a' * 64) + '::agent-password'
            first = run(worker_command, agent_token + '\n')
            self.assertEqual(first.returncode, 0, first.stderr)
            same = run(worker_command, agent_token + '\n')
            self.assertEqual(same.returncode, 0, same.stderr)
            different = run(
                worker_command,
                'K10' + ('b' * 64) + '::other-password\n',
            )
            self.assertNotEqual(different.returncode, 0)
            self.assertEqual(agent_file.read_text().strip(), agent_token)

            token_directory = Path(worker_state['K3S_TOKEN_DIRECTORY'])
            token_directory.chmod(0o777)
            insecure_directory = run(worker_command, agent_token + '\n')
            self.assertNotEqual(insecure_directory.returncode, 0)
            token_directory.chmod(0o700)
            real_directory = token_directory.with_name('tokens-real')
            token_directory.rename(real_directory)
            token_directory.symlink_to(real_directory, target_is_directory=True)
            symlinked_directory = run(worker_command, agent_token + '\n')
            self.assertNotEqual(symlinked_directory.returncode, 0)
            token_directory.unlink()
            real_directory.rename(token_directory)

            marker.write_text('foreign-runtime\n')
            foreign = run(worker_command, agent_token + '\n')
            self.assertNotEqual(foreign.returncode, 0)
            marker.write_text(runtime.DISTRIBUTED_RUNTIME_REVISION + '\n')
            agent_file.write_text('bad\n')
            malformed = run(worker_command, agent_token + '\n')
            self.assertNotEqual(malformed.returncode, 0)
            agent_file.unlink()
            unit = Path(worker_state['K3S_AGENT_UNIT'])
            unit.parent.mkdir(parents=True)
            unit.touch()
            partial = run(worker_command, agent_token + '\n')
            self.assertNotEqual(partial.returncode, 0)
            self.assertFalse(agent_file.exists())

            control_state = state(root / 'control')
            with patch.multiple(runtime, **control_state):
                control_command = runtime.control_tokens_initialize_command()
            initialized = run(control_command)
            self.assertEqual(initialized.returncode, 0, initialized.stderr)
            repeated = run(control_command)
            self.assertEqual(repeated.returncode, 0, repeated.stderr)
            control_agent = Path(control_state['K3S_AGENT_TOKEN_FILE'])
            control_server = Path(control_state['K3S_SERVER_TOKEN_FILE'])
            original_server = control_server.read_text()
            control_agent.unlink()
            missing = run(control_command)
            self.assertNotEqual(missing.returncode, 0)
            self.assertEqual(control_server.read_text(), original_server)

    def test_server_unit_is_fixed_private_and_uses_token_file(self):
        unit = server_unit(
            'dsb-control',
            '10.20.0.10',
            selinux_enabled=True,
        )

        self.assertIn(f'ExecStart={K3S_BINARY} server ', unit)
        self.assertIn(f'--token-file={K3S_SERVER_TOKEN_FILE}', unit)
        self.assertIn(f'--agent-token-file={K3S_AGENT_TOKEN_FILE}', unit)
        self.assertIn('--node-name=dsb-control', unit)
        self.assertIn('--node-ip=10.20.0.10', unit)
        self.assertIn('--advertise-address=10.20.0.10', unit)
        self.assertIn('--disable=traefik', unit)
        self.assertIn('--disable=servicelb', unit)
        self.assertIn('--disable=metrics-server', unit)
        self.assertIn('--disable=local-storage', unit)
        self.assertIn('--cluster-cidr=10.42.0.0/16', unit)
        self.assertIn('--service-cidr=10.43.0.0/16', unit)
        self.assertIn('--service-node-port-range=8080-8080', unit)
        self.assertIn('--flannel-backend=vxlan', unit)
        self.assertIn(
            'node-role.kubernetes.io/control-plane=true:NoSchedule',
            unit,
        )
        self.assertIn('--selinux', unit)
        self.assertIn('KillMode=control-group', unit)

    def test_agent_unit_joins_private_server_and_has_one_role(self):
        unit = agent_unit(
            'dsb-cache',
            '10.20.0.12',
            '10.20.0.10',
            'cache',
            selinux_enabled=False,
        )

        self.assertIn(f'ExecStart={K3S_BINARY} agent ', unit)
        self.assertIn('--server=https://10.20.0.10:6443', unit)
        self.assertIn(f'--token-file={K3S_AGENT_TOKEN_FILE}', unit)
        self.assertNotIn(K3S_SERVER_TOKEN_FILE, unit)
        self.assertIn('--node-ip=10.20.0.12', unit)
        self.assertIn('--node-label=deathstarbench.io/role=cache', unit)
        self.assertNotIn('server:', unit)

    def test_service_commands_are_idempotent_and_bounded(self):
        server = server_install_command(
            'dsb-control',
            '10.20.0.10',
            selinux_enabled=True,
        )
        agent = agent_join_command(
            'dsb-database',
            '10.20.0.13',
            '10.20.0.10',
            'database',
            selinux_enabled=True,
        )

        self.assertIn(f'UNIT_FILE={K3S_SERVER_UNIT}', server)
        self.assertIn(f'UNIT_FILE={K3S_AGENT_UNIT}', agent)
        for command in (server, agent):
            self.assertIn('cmp -s', command)
            self.assertIn('restorecon -F "$UNIT_FILE"', command)
            self.assertIn('matchpathcon -V "$UNIT_FILE"', command)
            self.assertIn('systemctl is-active --quiet', command)
            self.assertIn('timeout --signal=TERM 600s', command)
            self.assertIn('600:0:0', command)

    def test_readiness_checks_exact_binary_and_have_deadlines(self):
        server = server_readiness_command('x86_64')
        agent = agent_readiness_command('aarch64')
        cluster = cluster_nodes_readiness_command(self.expected_nodes())

        for command, artifact in (
            (server, K3S_ARTIFACTS['x86_64']),
            (agent, K3S_ARTIFACTS['aarch64']),
        ):
            self.assertIn(f'EXPECTED_VERSION={K3S_VERSION}', command)
            self.assertIn(artifact.sha256, command)
            self.assertIn('timeout --signal=TERM 300s', command)
        self.assertIn('get --raw=/readyz', server)
        self.assertIn('grep -Fx ok', server)
        self.assertIn('kubelet.kubeconfig', agent)
        self.assertIn('--for=condition=Ready', cluster)
        self.assertIn('node/dsb-app', cluster)
        self.assertIn('node/dsb-cache', cluster)
        self.assertIn('kubeletVersion', cluster)
        self.assertIn('InternalIP', cluster)
        self.assertIn('deathstarbench\\.io/role', cluster)
        self.assertIn('NoSchedule', cluster)

        pin = system_services_pin_command('dsb-control')
        self.assertIn('patch deployment coredns --type=merge', pin)
        self.assertIn('get deployment/coredns', pin)
        self.assertIn('timeout --signal=TERM 300s', pin)
        self.assertIn('rollout status deployment/coredns', pin)
        self.assertIn('COREDNS_SELECTOR', pin)
        self.assertIn('dsb-control', pin)

        system = system_services_readiness_command('dsb-control')
        self.assertIn('deployment/coredns', system)
        self.assertIn('COREDNS_SELECTOR', system)
        self.assertIn('k8s-app=kube-dns', system)
        self.assertIn('get service kube-dns', system)
        self.assertIn(K3S_CLUSTER_DNS_IP, system)
        self.assertIn('CoreDNS service IP drifted', system)
        self.assertIn('dsb-control', system)
        self.assertIn('metrics-server', system)
        self.assertIn('local-path-provisioner', system)
        self.assertIn('timeout --signal=TERM 300s', system)

    def test_uninstall_is_bounded_and_scoped_to_known_k3s_paths(self):
        command = uninstall_command()

        self.assertIn('timeout --signal=TERM 60s', command)
        self.assertIn(K3S_SERVER_UNIT, command)
        self.assertIn(K3S_AGENT_UNIT, command)
        self.assertIn(K3S_BINARY, command)
        self.assertIn('/var/lib/rancher/k3s', command)
        self.assertIn('/etc/deathstarbench/k3s', command)
        self.assertIn(K3S_OWNERSHIP_MARKER, command)
        self.assertIn('systemctl kill --kill-who=all', command)
        self.assertNotIn('/home/', command)
        self.assertNotIn('rm -rf -- / ', command)

    def test_untrusted_inputs_are_rejected_before_shell_generation(self):
        bad_values = (
            'x86_64; id',
            '$(id)',
            'dsb-app; reboot',
            '10.20.0.10; reboot',
            'application\nreboot',
        )
        with self.assertRaises(ValueError):
            artifact_install_command(bad_values[0])
        with self.assertRaises(ValueError):
            server_install_command(
                bad_values[2],
                '10.20.0.10',
                selinux_enabled=False,
            )
        with self.assertRaises(ValueError):
            server_install_command(
                'dsb-control',
                bad_values[3],
                selinux_enabled=False,
            )
        with self.assertRaises(ValueError):
            agent_join_command(
                'dsb-app',
                '10.20.0.11',
                '10.20.0.10',
                bad_values[4],
                selinux_enabled=False,
            )
        with self.assertRaises(ValueError):
            server_install_command(
                'dsb-control',
                '8.8.8.8',
                selinux_enabled=False,
            )
        with self.assertRaises(ValueError):
            host_preflight_command(selinux_enabled='auto')
        with self.assertRaises(ValueError):
            cluster_nodes_readiness_command(())
        with self.assertRaises(ValueError):
            cluster_nodes_readiness_command((
                self.expected_nodes()[0],
                self.expected_nodes()[0],
            ))

    def test_every_generated_command_is_valid_bash(self):
        commands = (
            artifact_install_command('x86_64'),
            artifact_install_command('aarch64'),
            token_install_command('server'),
            token_install_command('agent'),
            token_initialize_command('server'),
            control_tokens_initialize_command(),
            token_initialize_and_read_command('agent'),
            secure_agent_token_read_command(),
            rocky_host_prepare_command('control'),
            rocky_host_prepare_command('database'),
            host_preflight_command(selinux_enabled=False),
            host_preflight_command(selinux_enabled=True),
            server_install_command(
                'dsb-control',
                '10.20.0.10',
                selinux_enabled=True,
            ),
            agent_join_command(
                'dsb-app',
                '10.20.0.11',
                '10.20.0.10',
                'application',
                selinux_enabled=False,
            ),
            server_readiness_command('x86_64'),
            agent_readiness_command('aarch64'),
            cluster_nodes_readiness_command(self.expected_nodes()),
            system_services_pin_command('dsb-control'),
            system_services_readiness_command('dsb-control'),
            uninstall_command(),
        )
        for command in commands:
            with self.subTest(command=command[:60]):
                completed = subprocess.run(
                    ['bash', '-n'],
                    input=command,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == '__main__':
    unittest.main()
