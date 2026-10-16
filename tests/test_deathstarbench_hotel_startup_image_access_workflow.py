"""Additive manual, anonymous, read-only startup image-access workflow."""

from pathlib import Path
import re
import unittest

import yaml

from scripts import qualify_deathstarbench_hotel_startup_image_access as pipeline
from scripts.deathstarbench_artifact_common import UPSTREAM_REVISION
from tests.test_deathstarbench_hotel_initialization_workflow import job, step


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / '.github/workflows'


class StartupHotelImageAccessWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.wrapper = (WORKFLOWS / 'deathstarbench-social-images.yml').read_text()
        cls.source = (WORKFLOWS / 'deathstarbench-hotel-startup-image-access.yml').read_text()
        cls.workflow = yaml.safe_load(cls.source)

    def test_new_route_is_manual_explicit_and_old_choices_defaults_and_routes_remain(self):
        trigger = self.wrapper.split('\npermissions:', 1)[0]
        self.assertIn('  workflow_dispatch:', trigger)
        self.assertNotRegex(trigger, r'(?m)^  (?:push|pull_request|schedule):')
        self.assertIn('        default: load-driver\n', trigger)
        self.assertEqual(re.findall(r'^          - ([a-z-]+)$', trigger, re.MULTILINE), [
            'load-driver', 'workload-images', 'hotel-images', 'media-images', 'hotel-media-images',
            'hotel-initialization-images', 'hotel-startup-images', 'hotel-startup-image-access',
            'hotel-runtime-preflight', 'candidate-runtime-preflight'])
        called = job(self.wrapper, 'preflight-startup-hotel')
        self.assertIn("if: github.event.inputs.publication == 'hotel-startup-image-access'", called)
        self.assertIn('uses: ./.github/workflows/deathstarbench-hotel-startup-image-access.yml', called)
        self.assertIn('contents: read', called)
        self.assertIn('actions: read', called)
        self.assertIn('packages: read', called)
        self.assertNotIn('packages: write', called)
        self.assertNotIn('secrets: inherit', called)
        for name, option, workflow in (
                ('publish-startup-hotel', 'hotel-startup-images', 'deathstarbench-hotel-startup-images.yml'),
                ('preflight-guarded-hotel', 'hotel-runtime-preflight', 'deathstarbench-hotel-runtime-preflight.yml'),
                ('publish-guarded-hotel', 'hotel-initialization-images', 'deathstarbench-hotel-initialization-images.yml'),
                ('preflight-hotel-media', 'candidate-runtime-preflight', 'deathstarbench-candidate-runtime.yml')):
            body = job(self.wrapper, name)
            self.assertIn("if: github.event.inputs.publication == '" + option + "'", body)
            self.assertIn('uses: ./.github/workflows/' + workflow, body)
            self.assertNotIn('hotel-startup-image-access', body)
        self.assertNotIn('hotel-startup-image-access', job(self.wrapper, 'publish-hotel-media'))

    def test_reusable_workflow_has_one_native_read_only_job_no_execution_or_publication(self):
        trigger = self.source.split('\npermissions:', 1)[0]
        self.assertIn('  workflow_call:', trigger)
        self.assertNotRegex(trigger, r'(?m)^  (?:push|pull_request|workflow_dispatch|schedule):')
        self.assertEqual(self.workflow['permissions'], {'contents': 'read', 'actions': 'read', 'packages': 'read'})
        self.assertEqual(set(self.workflow['jobs']), {'qualify-startup-image-access'})
        qualifier = self.workflow['jobs']['qualify-startup-image-access']
        self.assertEqual(qualifier['runs-on'], 'ubuntu-24.04')
        self.assertEqual(qualifier['timeout-minutes'], 60)
        self.assertIn('python-version: "3.14"', self.source)
        self.assertNotIn('id-token:', self.source)
        self.assertNotIn('secrets: inherit', self.source)
        self.assertNotIn('packages: write', self.source)
        self.assertNotRegex(self.source, r'(?i)(?:docker/|setup-qemu|buildx|docker\s+(?:run|pull|push|login)|kubectl|terraform|\bk3s\s|\bhelm\s|\baws\s|\bgcloud\s|\baz\s|\boci\s)')
        for action in re.findall(r'(?m)^\s+(?:- )?uses: (\S+)', self.source):
            self.assertRegex(action, r'^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}$')
        self.assertEqual(self.source.count('persist-credentials: false'), 2)

    def test_source_inputs_and_reviewed_publication_downloads_are_exact_and_not_authority(self):
        self.assertIn('ref: ' + UPSTREAM_REVISION, self.source)
        self.assertIn('submodules: recursive', self.source)
        self.assertIn('f4a207f50a3f99ad65def8e29c54ac9aac668b216476f7fae3fae92413398ed2', self.source)
        self.assertIn('sha256sum --check --strict -', self.source)
        for run, artifact in ((pipeline.BASELINE_RUN, pipeline.BASELINE_ARTIFACT),
                              (pipeline.STARTUP_RUN, pipeline.STARTUP_ARTIFACT)):
            self.assertIn('run-id: ' + str(run), self.source)
            self.assertIn('name: ' + artifact, self.source)
        self.assertEqual(self.source.count('github-token: ${{ github.token }}'), 2)
        self.assertNotIn('38767705438', self.source)

    def test_execution_strips_ambient_secrets_and_only_calls_new_source_byte_qualifier(self):
        execution = step(self.source, 'Fresh source reconstruction and anonymous exact indexes manifests configs and every layer')
        self.assertNotIn('github.token', execution)
        self.assertNotIn('secrets.', execution)
        for name in pipeline._EMPTY_ENVIRONMENT:
            self.assertRegex(execution, r'\b' + re.escape(name) + r'\b')
        self.assertIn('scripts/qualify_deathstarbench_hotel_startup_image_access.py', execution)
        for option, value in (('upstream', '.build/upstream'), ('root', '.build/prepared'),
                              ('baseline', '.build/baseline'), ('publication', '.build/publication'), ('output', '.build/access')):
            self.assertIn('--' + option + ' ' + value, execution)
        self.assertNotIn('qualify_deathstarbench_hotel_initialization.py', execution)
        self.assertNotIn('--ssh', execution)

    def test_only_detached_historical_success_or_sanitized_failure_is_preserved(self):
        failure = step(self.source, 'Retain a bounded sanitized failure record if an earlier prerequisite failed')
        self.assertIn('if: always()', failure)
        self.assertIn('qualify_deathstarbench_hotel_startup_image_access import _write_owned, failure_record', failure)
        upload = step(self.source, 'Preserve historical byte-access observations (never a runtime capability)')
        self.assertIn('if: always()', upload)
        self.assertIn('name: hotel-startup-image-access-${{ github.run_id }}-${{ github.run_attempt }}', upload)
        self.assertIn('path: .build/access/*.json', upload)
        self.assertIn('if-no-files-found: error', upload)
        self.assertIn('retention-days: 90', upload)
        self.assertNotIn('.build/prepared', upload)
        self.assertNotIn('.build/publication', upload)
        self.assertIn('Hotel and Media remain unreleased', self.source)
        self.assertNotIn('deathstarbench-hotel-startup-image-access.yml', (WORKFLOWS / 'ci.yml').read_text())


if __name__ == '__main__':
    unittest.main()
