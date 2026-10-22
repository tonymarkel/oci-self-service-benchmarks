import os
import re
import subprocess
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = (ROOT / '.github/workflows/ci.yml').read_text()
WORKFLOW_DATA = yaml.safe_load(WORKFLOW)
CHECK_SCRIPT = (ROOT / 'scripts/check.sh').read_text()
CONTRIBUTING = (ROOT / 'CONTRIBUTING.md').read_text()


class CiConfigurationTests(unittest.TestCase):
    def test_pull_request_workflow_is_credential_free_and_read_only(self):
        self.assertIn('  pull_request:\n', WORKFLOW)
        self.assertIn('  workflow_dispatch:\n', WORKFLOW)
        self.assertRegex(
            WORKFLOW,
            re.compile(r'^permissions:\n  contents: read$', re.MULTILINE),
        )
        self.assertIn('persist-credentials: false', WORKFLOW)
        self.assertNotIn('pull_request_target', WORKFLOW)
        self.assertNotIn('secrets.', WORKFLOW)
        self.assertNotIn('id-token: write', WORKFLOW)

        inherited_permissions = WORKFLOW_DATA['permissions']
        self.assertEqual(inherited_permissions, {'contents': 'read'})
        for job_name, job in WORKFLOW_DATA['jobs'].items():
            permissions = job.get('permissions', inherited_permissions)
            with self.subTest(job=job_name):
                self.assertIsInstance(permissions, dict)
                self.assertTrue(
                    all(
                        access in {'read', 'none'}
                        for access in permissions.values()
                    )
                )

    def test_workflow_uses_pinned_actions_and_supported_runtime_endpoints(self):
        expected_actions = (
            'actions/checkout',
            'actions/setup-python',
            'actions/setup-node',
        )
        for action in expected_actions:
            with self.subTest(action=action):
                self.assertRegex(
                    WORKFLOW,
                    rf'uses: {re.escape(action)}@[0-9a-f]{{40}}',
                )
        action_references = re.findall(
            r'^\s*uses:\s*([^\s#]+)',
            WORKFLOW,
            flags=re.MULTILINE,
        )
        self.assertTrue(action_references)
        for action_reference in action_references:
            with self.subTest(action_reference=action_reference):
                self.assertRegex(
                    action_reference,
                    r'^[^@\s]+@[0-9a-f]{40}$',
                )
        self.assertIn('- "3.10"', WORKFLOW)
        self.assertIn('- "3.14"', WORKFLOW)
        self.assertIn('node-version: "22"', WORKFLOW)

    def test_workflow_has_bounded_concurrency_and_one_stable_required_check(self):
        concurrency = WORKFLOW_DATA['concurrency']
        self.assertIs(concurrency['cancel-in-progress'], True)
        self.assertIn('github.workflow', concurrency['group'])
        self.assertIn('github.event.pull_request.number', concurrency['group'])
        self.assertIn('github.ref', concurrency['group'])

        jobs = WORKFLOW_DATA['jobs']
        for job_name, job in jobs.items():
            with self.subTest(job=job_name):
                timeout = job['timeout-minutes']
                self.assertIsInstance(timeout, int)
                self.assertGreater(timeout, 0)
                self.assertLessEqual(timeout, 20)

        required_jobs = [
            job for job in jobs.values() if job.get('name') == 'Required CI'
        ]
        self.assertEqual(len(required_jobs), 1)
        required = required_jobs[0]
        self.assertEqual(required['if'], 'always()')
        self.assertEqual(set(required['needs']), {'test', 'candidate-artifacts'})
        self.assertNotIn('matrix', required.get('strategy', {}))

    def test_required_check_succeeds_only_when_all_dependencies_succeed(self):
        step, = WORKFLOW_DATA['jobs']['required']['steps']
        self.assertEqual(step['env'], {
            'TEST_RESULT': '${{ needs.test.result }}',
            'ARTIFACT_RESULT': '${{ needs.candidate-artifacts.result }}',
        })
        results = ('success', 'failure', 'cancelled', 'skipped')
        for test_result in results:
            for artifact_result in results:
                with self.subTest(test=test_result, artifacts=artifact_result):
                    completed = subprocess.run(
                        ['bash', '-e', '-c', step['run']],
                        env={
                            **os.environ,
                            'TEST_RESULT': test_result,
                            'ARTIFACT_RESULT': artifact_result,
                        },
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    self.assertEqual(
                        completed.returncode == 0,
                        test_result == 'success' and artifact_result == 'success',
                    )

    def test_shared_check_script_runs_every_credential_free_check(self):
        expected_commands = (
            '-m pip check',
            '-m compileall -q app tests',
            'node --check',
            'bash -n',
            '-m unittest discover -s tests -v',
        )
        for command in expected_commands:
            with self.subTest(command=command):
                self.assertIn(command, CHECK_SCRIPT)

    def test_contributor_guidance_keeps_live_cloud_runs_out_of_pr_ci(self):
        self.assertIn('scripts/check.sh', CONTRIBUTING)
        self.assertIn('does not authenticate to a provider', CONTRIBUTING)
        self.assertIn('Do not add cloud credentials', CONTRIBUTING)
        self.assertIn('Required CI', CONTRIBUTING)


if __name__ == '__main__':
    unittest.main()
