import json
import unittest
from unittest.mock import patch

from pydantic import ValidationError

from app import main
from app import phoronix as phoronix_runner
from app.catalog import PHORONIX_PROFILES
from app.models import BenchmarkPlan, PhoronixOptions


def plan(**overrides):
    values = {
        'region': 'us-ashburn-1',
        'shape': 'VM.Standard.E5.Flex',
        'ocpus': 8,
        'memory_gb': 32,
        'ssh_private_key': 'private',
        'ssh_public_key': 'public',
        'benchmarks': ['phoronix'],
    }
    values.update(overrides)
    return BenchmarkPlan(**values)


def phoronix_output(profile, *, prefix='', score=100.0):
    payload = {
        'title': 'OCI Phoronix result',
        'results': {
            'result-hash': {
                'identifier': profile,
                'title': 'Curated profile',
                'description': 'Fixed configuration',
                'scale': 'units/s',
                'proportion': 'HIB',
                'results': {
                    'OCI benchmark runner': {
                        'value': score,
                        'raw_values': [score - 1, score, score + 1],
                        'test_run_times': [1.0, 1.1, 0.9],
                    },
                },
            },
        },
    }
    return (
        prefix
        + f'\n{phoronix_runner.EXPORT_START}\n'
        + json.dumps(payload)
        + f'\n{phoronix_runner.EXPORT_END}\n'
    )


class PhoronixPlanTests(unittest.TestCase):
    def test_defaults_legacy_plans_to_7zip(self):
        self.assertEqual(PhoronixOptions().profiles, ['compress_7zip'])
        self.assertEqual(plan().phoronix.profiles, ['compress_7zip'])

    def test_profiles_are_typed_unique_and_required_when_selected(self):
        with self.assertRaises(ValidationError):
            plan(phoronix={'profiles': ['unsupported']})

        with self.assertRaisesRegex(ValidationError, 'must be unique'):
            plan(phoronix={'profiles': ['openssl', 'openssl']})

        with self.assertRaisesRegex(ValidationError, 'at least one Phoronix'):
            plan(phoronix={'profiles': []})

        unselected = plan(
            benchmarks=['stream'],
            phoronix={'profiles': []},
        )
        self.assertEqual(unselected.phoronix.profiles, [])

    def test_accepts_all_curated_profiles_in_requested_order(self):
        selected = [
            'compress_7zip',
            'openssl',
            'build_linux_kernel',
            'tinymembench',
        ]

        self.assertEqual(
            plan(phoronix={'profiles': selected}).phoronix.profiles,
            selected,
        )


class PhoronixCatalogTests(unittest.TestCase):
    def test_catalog_exposes_one_parent_and_the_pinned_profile_registry(self):
        payload = main.catalog()
        top_level_ids = [item['id'] for item in payload['benchmarks']]

        self.assertEqual(top_level_ids.count('phoronix'), 1)
        self.assertIs(payload['phoronix_profiles'], PHORONIX_PROFILES)
        self.assertEqual(
            [item['id'] for item in payload['phoronix_profiles']],
            [
                'compress_7zip',
                'openssl',
                'build_linux_kernel',
                'tinymembench',
            ],
        )
        self.assertEqual(
            [item['profile'] for item in payload['phoronix_profiles']],
            [
                'pts/compress-7zip-1.13.1',
                'pts/openssl-3.6.0',
                'pts/build-linux-kernel-1.18.0',
                'pts/tinymembench-1.0.2',
            ],
        )

    def test_each_profile_has_complete_cross_architecture_metadata(self):
        self.assertEqual(
            [item['category'] for item in PHORONIX_PROFILES],
            ['CPU', 'CPU', 'CPU', 'Memory'],
        )
        for profile in PHORONIX_PROFILES:
            with self.subTest(profile=profile['id']):
                self.assertEqual(
                    set(profile['architectures']),
                    {'x86_64', 'aarch64'},
                )
                self.assertGreater(profile['estimated_runtime_minutes'], 0)
                self.assertTrue(profile['unit'])
                self.assertIn(
                    profile['direction'],
                    {'higher_is_better', 'lower_is_better', 'mixed'},
                )
                self.assertTrue(profile['description'])


class PhoronixMainIntegrationTests(unittest.TestCase):
    def test_install_uses_only_the_selected_profiles_package_set(self):
        selected_plan = plan(
            phoronix={'profiles': ['openssl', 'tinymembench']},
        )
        job = {'events': [], 'results': [], 'resources': {}}

        with (
            patch.object(
                main,
                'phoronix_required_packages',
                return_value={'phoronix-package'},
            ) as required_packages,
            patch.object(main, 'dnf_install') as install,
        ):
            main.install_benchmark_tools(
                job,
                selected_plan,
                {'phoronix'},
            )

        required_packages.assert_called_once_with(
            ('openssl', 'tinymembench')
        )
        install.assert_called_once_with(job, {'phoronix-package'})
        self.assertIn(
            'phoronix-package',
            job['events'][0]['message'],
        )

    def test_profiles_prepare_once_and_fail_independently_with_full_results(self):
        selected_plan = plan(
            shape='VM.Standard.A2.Flex',
            ocpus=16,
            memory_gb=64,
            phoronix={'profiles': ['openssl', 'tinymembench']},
        )
        job = {
            'id': 'phoronix-main-unit',
            'events': [],
            'results': [],
            'resources': {'public_ip': '203.0.113.10'},
        }
        runs = phoronix_runner.profile_runs(
            selected_plan.phoronix.profiles,
            job['id'],
        )
        preparation_command = phoronix_runner.prepare_command()
        wrong_profile_output = phoronix_output(
            'pts/compress-7zip-1.13.1',
            prefix='OpenSSL raw output\n',
        )
        full_tinymembench_output = phoronix_output(
            'pts/tinymembench-1.0.2',
            prefix='x' * 25000,
            score=25000.0,
        )
        calls = []

        def fake_ssh(
            _job,
            command,
            timeout=1800,
            host_key='public_ip',
            include_stderr=False,
        ):
            calls.append((command, timeout, host_key, include_stderr))
            if command == preparation_command:
                return 'Pinned client ready.'
            if command == 'uname -m':
                return 'aarch64\n'
            if command == runs[0].command:
                return wrong_profile_output
            if command == runs[1].command:
                return full_tinymembench_output
            self.fail(f'Unexpected SSH command: {command}')

        with patch.object(main, 'ssh', side_effect=fake_ssh):
            failures = main.run_phoronix_profiles(job, selected_plan)

        self.assertEqual(
            [call[0] for call in calls].count(preparation_command),
            1,
        )
        preparation_call = next(
            call for call in calls if call[0] == preparation_command
        )
        self.assertEqual(
            preparation_call[1],
            phoronix_runner.PREPARE_TIMEOUT_SECONDS,
        )
        self.assertTrue(preparation_call[3])
        self.assertEqual(
            [call[0] for call in calls if call[0] in {
                runs[0].command,
                runs[1].command,
            }],
            [runs[0].command, runs[1].command],
        )
        self.assertEqual(len(failures), 1)
        self.assertIn('Phoronix — OpenSSL SHA-256', failures[0])

        failed, completed = job['results']
        self.assertEqual(failed['id'], 'phoronix_openssl')
        self.assertEqual(failed['status'], 'failed')
        self.assertIn('unexpected profile', failed['error'])
        self.assertEqual(failed['output'], wrong_profile_output)

        self.assertEqual(completed['id'], 'phoronix_tinymembench')
        self.assertEqual(completed['status'], 'completed')
        self.assertEqual(completed['output'], full_tinymembench_output)
        self.assertGreater(len(completed['output']), 20000)
        self.assertEqual(completed['metrics']['profile'], runs[1].profile)
        self.assertEqual(completed['metrics']['score'], 25000.0)

        for result, run in zip(job['results'], runs):
            with self.subTest(profile=run.profile):
                self.assertEqual(
                    result['metadata']['phoronix_profile'],
                    run.profile,
                )
                self.assertEqual(
                    result['metadata']['phoronix_client_revision'],
                    phoronix_runner.PTS_REVISION,
                )
                self.assertEqual(
                    result['metadata']['measured_trials'],
                    phoronix_runner.MEASURED_TRIALS,
                )
                self.assertEqual(result['metadata']['architecture'], 'aarch64')
                self.assertEqual(
                    result['metadata']['shape'],
                    'VM.Standard.A2.Flex',
                )
                self.assertEqual(result['metadata']['ocpus'], 16)
                self.assertEqual(result['metadata']['memory_gb'], 64)

        benchmark_calls = [
            call for call in calls
            if call[0] in {runs[0].command, runs[1].command}
        ]
        self.assertTrue(all(call[3] for call in benchmark_calls))
        self.assertEqual(
            [call[1] for call in benchmark_calls],
            [phoronix_runner.SSH_TIMEOUT_SECONDS] * 2,
        )

    def test_unselected_phoronix_does_not_prepare_or_install_profiles(self):
        selected_plan = plan(benchmarks=['stream'])
        job = {'events': [], 'results': [], 'resources': {}}

        with (
            patch.object(main, 'phoronix_required_packages') as packages,
            patch.object(main, 'dnf_install'),
        ):
            main.install_benchmark_tools(job, selected_plan, {'stream'})

        packages.assert_not_called()


if __name__ == '__main__':
    unittest.main()
