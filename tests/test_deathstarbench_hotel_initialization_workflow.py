"""Narrow, offline regression checks for the additive manual image workflow."""

import ast
from pathlib import Path
import re
import unittest

from scripts.deathstarbench_artifact_common import UPSTREAM_REVISION
from scripts.deathstarbench_hotel_initialization_artifact_common import BASELINE_ARTIFACT, BASELINE_RUN


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / '.github/workflows'
PIPELINE = ROOT / 'scripts/publish_deathstarbench_hotel_initialization_images.py'


def job(source, name):
    match = re.search(r'(?ms)^  ' + re.escape(name) + r':\n(.*?)(?=^  [a-zA-Z0-9_-]+:\n|\Z)', source)
    if match is None:
        raise AssertionError('Missing workflow job: ' + name)
    return match.group(1)


def step(source, name):
    match = re.search(r'(?ms)^      - name: ' + re.escape(name) + r'\n(.*?)(?=^      - (?:name:|uses:)|\Z)', source)
    if match is None:
        raise AssertionError('Missing workflow step: ' + name)
    return match.group(1)


class GuardedHotelWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.wrapper = (WORKFLOWS / 'deathstarbench-social-images.yml').read_text()
        cls.guarded = (WORKFLOWS / 'deathstarbench-hotel-initialization-images.yml').read_text()
        cls.ci = (WORKFLOWS / 'ci.yml').read_text()

    def test_manual_choice_is_explicit_additive_and_original_default_choices_stay_intact(self):
        trigger = self.wrapper.split('\npermissions:', 1)[0]
        self.assertIn('  workflow_dispatch:', trigger)
        self.assertNotRegex(trigger, r'^  (?:push|pull_request|schedule):')
        self.assertIn('        default: load-driver\n', trigger)
        options = re.findall(r'^          - ([a-z-]+)$', trigger, re.MULTILINE)
        self.assertEqual(options, [
            'load-driver', 'workload-images', 'hotel-images', 'media-images',
            'hotel-media-images', 'hotel-initialization-images', 'candidate-runtime-preflight',
        ])
        guarded = job(self.wrapper, 'publish-guarded-hotel')
        self.assertIn("if: github.event.inputs.publication == 'hotel-initialization-images'", guarded)
        self.assertIn('uses: ./.github/workflows/deathstarbench-hotel-initialization-images.yml', guarded)
        self.assertNotIn('secrets: inherit', guarded)
        self.assertIn("if: github.event.inputs.publication == 'load-driver'", job(self.wrapper, 'publish-load-driver'))
        self.assertIn("if: github.event.inputs.publication == 'workload-images'", job(self.wrapper, 'publish-candidate'))
        legacy = job(self.wrapper, 'publish-hotel-media')
        self.assertIn("contains(fromJSON('[\"hotel-images\",\"media-images\",\"hotel-media-images\"]'), github.event.inputs.publication)", legacy)
        self.assertNotIn('hotel-initialization-images', legacy)
        self.assertIn('uses: ./.github/workflows/deathstarbench-candidate-images.yml', legacy)
        preflight = job(self.wrapper, 'preflight-hotel-media')
        self.assertIn("if: github.event.inputs.publication == 'candidate-runtime-preflight'", preflight)
        self.assertIn('uses: ./.github/workflows/deathstarbench-candidate-runtime.yml', preflight)

    def test_required_ci_only_reconstructs_source_without_registry_or_cloud_authority(self):
        candidate = job(self.ci, 'candidate-artifacts')
        source = step(candidate, 'Reconstruct guarded Hotel artifact inputs independently (no registry or release)')
        self.assertIn('from scripts.deathstarbench_hotel_initialization_artifact_common import validate_guarded_preparation', source)
        self.assertIn("validate_guarded_preparation(Path('.build/hotel-initialization/first'), Path('.build/upstream'))", source)
        self.assertNotRegex(source, r'(?i)(?:REGISTRY_TOKEN|GITHUB_TOKEN|docker\s+(?:pull|push|login)|(?:adopt|publish)_guarded|kubectl|terraform|\baws\s|\bgcloud\s|\baz\s|\boci\s)')
        preparation = step(candidate, 'Verify additive guarded Hotel preparation twice (no publication)')
        self.assertIn('validate_deathstarbench_hotel_initialization.py prepare --upstream .build/upstream', preparation)
        self.assertNotIn('publish_deathstarbench_hotel_initialization_images.py', candidate)
        self.assertNotIn('packages: write', candidate)

    def test_reusable_native_workflow_has_only_explicit_call_and_commit_pinned_actions(self):
        trigger = self.guarded.split('\npermissions:', 1)[0]
        self.assertIn('  workflow_call:', trigger)
        self.assertNotRegex(trigger, r'(?m)^  (?:push|pull_request|schedule|workflow_dispatch):')
        actions = re.findall(r'(?m)^\s+(?:- )?uses: (\S+)', self.guarded)
        self.assertTrue(actions)
        for action in actions:
            with self.subTest(action=action):
                self.assertRegex(action, r'^[a-zA-Z0-9_.-]+/[a-zA-Z0-9_./-]+@[0-9a-f]{40}$')
        self.assertNotIn('id-token:', self.guarded)
        self.assertNotIn('secrets: inherit', self.guarded)
        for name in ('native-build', 'assemble', 'anonymous-native'):
            body = job(self.guarded, name)
            self.assertIn('persist-credentials: false', body)
            self.assertIn('repository: delimitrou/DeathStarBench', body)
            self.assertIn('ref: ' + UPSTREAM_REVISION, body)
            self.assertIn('submodules: recursive', body)
            self.assertIn('run-id: ' + str(BASELINE_RUN), body)
            self.assertIn('name: ' + BASELINE_ARTIFACT, body)

    def test_builds_and_anonymous_smokes_use_matching_native_runner_pairs_not_qemu(self):
        for name in ('native-build', 'anonymous-native'):
            body = job(self.guarded, name)
            for fragment in ('architecture: amd64\n            runner: ubuntu-24.04',
                             'architecture: arm64\n            runner: ubuntu-24.04-arm',
                             'runs-on: ${{ matrix.runner }}'):
                self.assertIn(fragment, body)
        native = job(self.guarded, 'native-build')
        check = step(native, 'Verify matching native host and daemon without QEMU')
        self.assertIn('uname -m', check)
        self.assertIn("docker info --format '{{.OSType}}'", check)
        self.assertIn("docker info --format '{{.Architecture}}'", check)
        self.assertIn('platforms: linux/${{ matrix.architecture }}', native)
        self.assertNotRegex(self.guarded, r'(?i)(?:setup-qemu|binfmt|qemu-user|tonistiigi)')
        self.assertIn('needs: native-build', job(self.guarded, 'assemble'))
        self.assertIn('needs: assemble', job(self.guarded, 'anonymous-native'))

    def test_every_source_sensitive_pipeline_command_receives_audited_upstream(self):
        # CLI arity drift can make an expensive successful build impossible to
        # record. Every mode that regenerates source requires its checkout.
        for command in ('prepare', 'baseline', 'record-native', 'assemble', 'anonymous-native'):
            invocations = re.findall(
                r'publish_deathstarbench_hotel_initialization_images\.py ' + command
                + r'(.*?)(?=\n      - |\Z)', self.guarded, re.DOTALL)
            self.assertTrue(invocations, command)
            for invocation in invocations:
                with self.subTest(command=command):
                    self.assertIn('--upstream .build/upstream', invocation)

    def test_anonymous_job_has_no_login_or_auth_env_and_uses_explicit_empty_client_config(self):
        anonymous = job(self.guarded, 'anonymous-native')
        self.assertNotIn('docker/login-action', anonymous)
        self.assertNotIn('packages: write', anonymous)
        execution = step(anonymous, 'Anonymous exact native pull and no-database smoke with fresh empty Docker config')
        self.assertIn('unset REGISTRY_TOKEN REGISTRY_USERNAME DOCKER_AUTH_CONFIG', execution)
        self.assertIn('export DOCKER_CONFIG', execution)
        self.assertIn('DOCKER_CONFIG=$(mktemp -d)', execution)
        self.assertIn('anonymous-native', execution)
        self.assertNotIn('${{ github.token }}', execution)
        self.assertNotIn('secrets.', execution)

    def test_workflow_does_not_deploy_cluster_or_execute_initialization_or_public_gate(self):
        self.assertNotRegex(self.guarded, r'(?im)^\s*(?:sudo\s+)?(?:kubectl|helm|terraform|k3s|aws|gcloud|az|oci)\s')
        self.assertNotRegex(self.guarded, r'(?i)docker\s+run[^\n]*(?:mongo(?:db)?|redis|consul|k3s)')
        self.assertNotRegex(self.guarded, r'(?im)^\s*(?:/go/bin/)?hotel-initialize\s+(?:initialize-empty|verify)')
        self.assertIn("'database_executed': False", self.guarded)
        self.assertIn("'runtime_qualified': False", self.guarded)
        self.assertIn("'measurement_qualified': False", self.guarded)
        self.assertIn("'public_release': False", self.guarded)
        self.assertIn('Hotel and Media remain unreleased', self.guarded)

    def test_anonymous_receipt_preserves_narrow_false_qualification_scope(self):
        module = ast.parse(PIPELINE.read_text())
        function = next(value for value in module.body if isinstance(value, ast.FunctionDef) and value.name == 'anonymous_record')
        returns = [value for value in ast.walk(function) if isinstance(value, ast.Return) and isinstance(value.value, ast.Dict)]
        self.assertEqual(len(returns), 1)
        literal = {key.value: value.value for key, value in zip(returns[0].value.keys, returns[0].value.values)
                   if isinstance(key, ast.Constant) and isinstance(value, ast.Constant)}
        for flag in ('all_layers_independently_downloaded', 'proxy_or_host_credential_independence_qualified',
                     'anonymous_pull_qualified', 'support_or_driver_layers_pulled', 'database_executed',
                     'application_start_allowed', 'runtime_qualified', 'measurement_qualified', 'public_release'):
            self.assertIs(literal[flag], False, flag)
        self.assertIs(literal['empty_docker_config'], True)
        self.assertIs(literal['registry_credentials_supplied'], False)
        self.assertEqual(literal['scope'], 'fresh-empty-docker-client-config-exact-native-app-pull-and-no-database-smoke-only')


if __name__ == '__main__':
    unittest.main()
