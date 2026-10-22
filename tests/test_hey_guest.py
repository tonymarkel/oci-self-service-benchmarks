import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import hey, main
from app.guests import web
from app.models import ApacheBenchOptions, HeyOptions
from test_apachebench import SAMPLE_OUTPUT
from test_hey import OUTPUT, options
from test_hey_integration import job, plan


class HeyGuestTests(unittest.TestCase):
    def test_generated_command_executes_instruments_and_preserves_exit_status(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Path(directory) / 'hey'
            summary = '\n'.join(line for line in OUTPUT.splitlines() if not line.startswith('OCI_'))
            client.write_text('#!/bin/bash\n'
                              'test "$*" = "-disable-compression -disable-redirects -disable-keepalive -n 1000 -c 10 http://10.42.1.10/benchmark.bin" || exit 90\n'
                              "cat <<'RESULT'\n" + summary + '\nRESULT\n')
            client.chmod(0o755)
            command = hey.benchmark_command('new_connections', '10.42.1.10', options(), 1)
            command = command.replace('/usr/local/bin/hey', str(client))
            result = subprocess.run(['bash', '-c', command], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            metrics = hey.parse_output(result.stdout)
            self.assertEqual(metrics['complete_requests'], 1000)
            self.assertGreater(metrics['load_generator_logical_cpus'], 0)
            client.write_text('#!/bin/bash\necho "client failed" >&2\nexit 7\n')
            failed = subprocess.run(['bash', '-c', command], capture_output=True, text=True)
            self.assertEqual(failed.returncode, 7)
            self.assertIn('client failed', failed.stdout)

    def test_install_and_preparation_commands_have_valid_shell_syntax(self):
        for command in (hey.install_command(), hey.loadgen_prepare_command(),
                        hey.target_prepare_command(64, '10.42.1.20'),
                        hey.readiness_command('10.42.1.10', 64)):
            result = subprocess.run(['bash', '-n'], input=command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_all_provider_loadgens_wait_for_go_module_services(self):
        for provider in ('oci', 'aws', 'gcp', 'azure'):
            with self.subTest(provider=provider):
                service = web.readiness_hosts(provider, 'hey', 'service', region='us-ashburn-1')
                loadgen = web.readiness_hosts(provider, 'hey', 'loadgen', region='us-ashburn-1')
                self.assertNotIn('proxy.golang.org', service)
                self.assertIn('proxy.golang.org', loadgen)
                self.assertIn('sum.golang.org', loadgen)

    def test_combined_benchmarks_reconfigure_target_for_each_payload(self):
        target_job = job()
        benchmark_plan = plan(benchmarks=['apachebench', 'hey'],
                              apachebench=ApacheBenchOptions(workloads=['keep_alive'], warmup_requests=0, trials=1),
                              hey=HeyOptions(workloads=['keep_alive'], warmup_requests=0, trials=1,
                                             request_count=1000, concurrency=10, response_size_kib=32))
        target_sizes = []

        def ssh(_job, command, **kwargs):
            if command == 'uname -m':
                return 'x86_64'
            if 'dd if=/dev/zero' in command:
                target_sizes.append(32 if 'count=32 ' in command else 64)
            if 'OCI_AB_TIME' in command:
                return SAMPLE_OUTPUT
            if 'OCI_HEY_TIME' in command:
                return OUTPUT.replace('65536000', '32768000').replace('65536 bytes', '32768 bytes')
            return 'ready'

        with (patch.object(main, 'wait_for_web_guest_readiness'),
              patch.object(main, 'install_apachebench_guest'),
              patch.object(main, 'install_hey_guest'),
              patch.object(main, 'ssh', side_effect=ssh)):
            self.assertEqual(main.run_apachebench(target_job, benchmark_plan), ())
            self.assertEqual(main.run_hey(target_job, benchmark_plan), ())
        self.assertEqual(target_sizes, [64, 32])
        self.assertEqual([result['id'] for result in target_job['results']],
                         ['apachebench_keep_alive', 'hey_keep_alive'])
        self.assertTrue(all(result['status'] == 'completed' for result in target_job['results']))
        self.assertEqual(target_job['results'][1]['metadata']['response_size_validation'], 'reported_content_length')
