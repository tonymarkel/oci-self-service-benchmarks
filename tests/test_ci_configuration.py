import re
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
        self.assertIn('cancel-in-progress: true', WORKFLOW)
        self.assertIn('timeout-minutes: 15', WORKFLOW)
        self.assertIn('name: Required CI', WORKFLOW)
        self.assertIn('TEST_RESULT: ${{ needs.test.result }}', WORKFLOW)
        self.assertIn('run: test "$TEST_RESULT" = "success"', WORKFLOW)

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
