"""Additive manual startup publication boundaries; no cloud or runtime gate."""

import ast
from pathlib import Path
import re
import unittest

import yaml

from scripts.deathstarbench_artifact_common import UPSTREAM_REVISION
from scripts.deathstarbench_hotel_initialization_artifact_common import BASELINE_ARTIFACT, BASELINE_RUN
from tests.test_deathstarbench_hotel_initialization_workflow import job, step


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / '.github/workflows'


class HotelStartupImagesWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.wrapper = (WORKFLOWS / 'deathstarbench-social-images.yml').read_text()
        cls.source = (WORKFLOWS / 'deathstarbench-hotel-startup-images.yml').read_text()
        cls.workflow = yaml.safe_load(cls.source)
        cls.jobs = cls.workflow['jobs']
        cls.ci = (WORKFLOWS / 'ci.yml').read_text()

    def test_new_operation_is_explicit_manual_addition_without_repurposing_old_choices(self):
        trigger = self.wrapper.split('\npermissions:', 1)[0]
        self.assertIn('  workflow_dispatch:', trigger)
        self.assertNotRegex(trigger, r'(?m)^  (?:push|pull_request|schedule):')
        self.assertIn('        default: load-driver\n', trigger)
        self.assertEqual(re.findall(r'^          - ([a-z-]+)$', trigger, re.MULTILINE), [
            'load-driver', 'workload-images', 'hotel-images', 'media-images', 'hotel-media-images',
            'hotel-initialization-images', 'hotel-startup-images', 'hotel-runtime-preflight', 'candidate-runtime-preflight',
        ])
        startup = job(self.wrapper, 'publish-startup-hotel')
        self.assertIn("if: github.event.inputs.publication == 'hotel-startup-images'", startup)
        self.assertIn('uses: ./.github/workflows/deathstarbench-hotel-startup-images.yml', startup)
        self.assertIn('packages: write', startup)
        self.assertIn('actions: read', startup)
        self.assertNotIn('secrets: inherit', startup)
        old = job(self.wrapper, 'publish-guarded-hotel')
        self.assertIn("if: github.event.inputs.publication == 'hotel-initialization-images'", old)
        self.assertIn('uses: ./.github/workflows/deathstarbench-hotel-initialization-images.yml', old)
        self.assertNotIn('hotel-startup-images', old)
        self.assertNotIn('hotel-startup-images', job(self.wrapper, 'publish-hotel-media'))

    def test_reusable_workflow_has_no_automatic_or_independent_dispatch_trigger(self):
        trigger = self.source.split('\npermissions:', 1)[0]
        self.assertIn('  workflow_call:', trigger)
        self.assertNotRegex(trigger, r'(?m)^  (?:push|pull_request|schedule|workflow_dispatch):')
        self.assertEqual(self.workflow['permissions'], {'contents': 'read', 'actions': 'read', 'packages': 'read'})
        self.assertEqual(set(self.jobs), {'native-build', 'assemble', 'anonymous-native'})
        for action in re.findall(r'(?m)^\s+(?:- )?uses: (\S+)', self.source):
            self.assertRegex(action, r'^[a-zA-Z0-9_.-]+/[a-zA-Z0-9_./-]+@[0-9a-f]{40}$')
        self.assertNotIn('id-token:', self.source)
        self.assertNotIn('secrets: inherit', self.source)

    def test_every_job_checks_out_exact_upstream_and_selects_only_reviewed_baseline_inputs(self):
        for name in self.jobs:
            body = job(self.source, name)
            with self.subTest(job=name):
                self.assertIn('persist-credentials: false', body)
                self.assertIn('repository: delimitrou/DeathStarBench', body)
                self.assertIn('ref: ' + UPSTREAM_REVISION, body)
                self.assertIn('submodules: recursive', body)
                self.assertIn('run-id: ' + str(BASELINE_RUN), body)
                self.assertIn('name: ' + BASELINE_ARTIFACT, body)
                self.assertIn('sha256sum --check --strict -', body)
                self.assertIn('f4a207f50a3f99ad65def8e29c54ac9aac668b216476f7fae3fae92413398ed2', body)
        for mode in ('prepare', 'baseline', 'record-native', 'assemble', 'anonymous-native'):
            invocations = re.findall(r'publish_deathstarbench_hotel_startup_images\.py ' + mode
                                     + r'(.*?)(?=\n      - |\Z)', self.source, re.DOTALL)
            self.assertTrue(invocations, mode)
            for invocation in invocations:
                self.assertIn('--upstream .build/upstream', invocation, mode)

    def test_both_native_architectures_and_daemon_are_checked_without_emulation(self):
        for name in ('native-build', 'anonymous-native'):
            matrix = self.jobs[name]['strategy']['matrix']['include']
            self.assertEqual(matrix, [{'architecture': 'amd64', 'runner': 'ubuntu-24.04'},
                                      {'architecture': 'arm64', 'runner': 'ubuntu-24.04-arm'}])
            self.assertEqual(self.jobs[name]['runs-on'], '${{ matrix.runner }}')
        native = job(self.source, 'native-build')
        check = step(native, 'Verify matching native host and daemon without QEMU')
        self.assertIn('uname -m', check)
        self.assertIn("docker info --format '{{.OSType}}'", check)
        self.assertIn("docker info --format '{{.Architecture}}'", check)
        self.assertIn('platforms: linux/${{ matrix.architecture }}', native)
        self.assertNotRegex(self.source, r'(?i)(?:setup-qemu|binfmt|qemu-user|tonistiigi)')
        self.assertEqual(self.jobs['assemble']['needs'], 'native-build')
        self.assertEqual(self.jobs['anonymous-native']['needs'], 'assemble')

    def test_only_new_app_context_is_built_with_separate_cache_and_provenance(self):
        native = job(self.source, 'native-build')
        build = next(value for value in self.jobs['native-build']['steps']
                     if value.get('uses', '').startswith('docker/build-push-action@'))
        self.assertEqual(build['with']['context'], '.build/prepared/startup/app')
        self.assertEqual(build['with']['file'], '.build/prepared/startup/app/Dockerfile.candidate')
        self.assertEqual(build['with']['platforms'], 'linux/${{ matrix.architecture }}')
        self.assertIs(build['with']['push'], True)
        self.assertEqual(build['with']['provenance'], 'mode=max')
        self.assertIs(build['with']['sbom'], True)
        self.assertIn('scope=dsb-hotel-guard-startup-app-${{ matrix.architecture }}', build['with']['cache-to'])
        self.assertNotIn('guard-init-app', native)
        self.assertEqual(self.source.count('uses: docker/build-push-action@'), 1)
        self.assertLess(native.index('Freshly adopt the complete reviewed Hotel baseline'),
                        native.index('Publish only the native startup app'))
        self.assertIn('--app-index "${APP_TAG%:*}@${APP_DIGEST}"', native)

    def test_both_original_native_receipts_are_reconstructed_before_assembly_and_preserved_on_failure(self):
        assembly = job(self.source, 'assemble')
        self.assertIn('Independently regenerate exact old and startup preparations', assembly)
        self.assertIn('Preflight all sources, baseline and native bytes before index publication', assembly)
        for architecture in ('amd64', 'arm64'):
            self.assertIn('hotel-startup-native-' + architecture + '-${RUN_ID}-${ATTEMPT}/native.json', assembly)
        self.assertIn('pattern: hotel-startup-native-*-${{ github.run_id }}-${{ github.run_attempt }}', assembly)
        for name in ('native-build', 'assemble', 'anonymous-native'):
            uploads = [value for value in self.jobs[name]['steps']
                       if value.get('uses', '').startswith('actions/upload-artifact@')]
            self.assertEqual(len(uploads), 1)
            self.assertEqual(uploads[0]['if'], 'always()')
            self.assertEqual(uploads[0]['with']['retention-days'], 90)
            self.assertTrue(uploads[0]['with']['path'].endswith('/*.json'))
            self.assertNotIn('**', uploads[0]['with']['path'])
        native = job(self.source, 'native-build')
        self.assertIn('for part in startup baseline-workload baseline-driver;', native)
        self.assertIn('build-metadata.json', native)

    def test_anonymous_job_has_read_only_permissions_and_an_explicit_empty_docker_client(self):
        anonymous = job(self.source, 'anonymous-native')
        self.assertEqual(self.jobs['anonymous-native']['permissions'],
                         {'contents': 'read', 'actions': 'read', 'packages': 'read'})
        self.assertNotIn('docker/login-action', anonymous)
        self.assertNotIn('packages: write', anonymous)
        execution = step(anonymous, 'Anonymous exact native pull and no-database smoke with fresh empty Docker config')
        self.assertIn('unset REGISTRY_TOKEN REGISTRY_USERNAME DOCKER_AUTH_CONFIG', execution)
        self.assertIn('export DOCKER_CONFIG', execution)
        self.assertIn('DOCKER_CONFIG=$(mktemp -d)', execution)
        self.assertIn('anonymous-native', execution)
        self.assertNotIn('${{ github.token }}', execution)
        self.assertNotIn('secrets.', execution)

    def test_publication_is_absent_from_required_ci_and_has_no_runtime_or_cloud_operations(self):
        candidate = job(self.ci, 'candidate-artifacts')
        self.assertNotIn('publish_deathstarbench_hotel_startup_images.py', candidate)
        self.assertNotIn('packages: write', candidate)
        self.assertNotRegex(self.source, r'(?im)^\s*(?:sudo\s+)?(?:kubectl|helm|terraform|k3s|aws|gcloud|az|oci)\s')
        self.assertNotRegex(self.source, r'(?i)docker\s+run[^\n]*(?:mongo(?:db)?|redis|consul|k3s)')
        self.assertNotRegex(self.source, r'(?im)^\s*(?:/go/bin/)?hotel-initialize\s+(?:initialize-empty|verify)')
        for flag in ('database_executed', 'runtime_qualified', 'measurement_qualified', 'public_release'):
            self.assertIn("'" + flag + "': False", self.source)
        self.assertIn('Hotel and Media remain unreleased', self.source)
        self.assertIn('application startup', self.source)

    def test_required_ci_reconstructs_new_artifact_inputs_without_registry_or_execution_authority(self):
        candidate = job(self.ci, 'candidate-artifacts')
        source = step(candidate, 'Reconstruct startup Hotel artifact inputs independently (no registry or release)')
        self.assertIn('from scripts.deathstarbench_hotel_startup_artifact_common import validate_startup_preparation', source)
        self.assertIn("validate_startup_preparation(Path('.build/hotel-startup/first'), Path('.build/upstream'))", source)
        self.assertLess(candidate.index('Verify separate Hotel startup-safe preparation twice'),
                        candidate.index('Reconstruct startup Hotel artifact inputs independently'))
        self.assertLess(candidate.index('Reconstruct startup Hotel artifact inputs independently'),
                        candidate.index('Test startup-safe Hotel sources without external network or databases'))
        self.assertNotRegex(source, r'(?i)(?:REGISTRY_TOKEN|GITHUB_TOKEN|docker\s+(?:pull|push|login)|(?:adopt|publish)_startup|kubectl|terraform|\baws\s|\bgcloud\s|\baz\s|\boci\s)')

    def test_anonymous_scope_stays_client_isolation_and_no_database_smoke_not_release(self):
        pipeline = ROOT / 'scripts/publish_deathstarbench_hotel_startup_images.py'
        module = ast.parse(pipeline.read_text())
        function = next(value for value in module.body
                        if isinstance(value, ast.FunctionDef) and value.name == 'anonymous_record')
        returns = [value for value in ast.walk(function)
                   if isinstance(value, ast.Return) and isinstance(value.value, ast.Dict)]
        self.assertEqual(len(returns), 1)
        values = {key.value: value.value for key, value in zip(returns[0].value.keys, returns[0].value.values)
                  if isinstance(key, ast.Constant) and isinstance(value, ast.Constant)}
        for flag in ('all_layers_independently_downloaded', 'proxy_or_host_credential_independence_qualified',
                     'anonymous_pull_qualified', 'support_or_driver_layers_pulled', 'database_executed',
                     'application_start_allowed', 'runtime_qualified', 'measurement_qualified', 'public_release'):
            self.assertIs(values[flag], False, flag)
        self.assertIs(values['empty_docker_config'], True)
        self.assertIs(values['registry_credentials_supplied'], False)
        self.assertEqual(values['scope'], 'fresh-empty-docker-client-config-exact-native-app-pull-and-no-database-smoke-only')


if __name__ == '__main__':
    unittest.main()
