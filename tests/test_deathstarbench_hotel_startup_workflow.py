"""Static CI boundaries for source-only Hotel startup preparation and metadata."""

from pathlib import Path
import re
import unittest

import yaml

from scripts import prepare_deathstarbench_hotel_startup as preparation


ROOT = Path(__file__).resolve().parents[1]
BUILDER = ('docker.io/library/golang:1.21.13-bookworm@sha256:'
           'c6a5b9308b3f3095e8fde83c8bf4d68bd101fce606c1a0a1394522542509dda9')


class HotelStartupWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())
        cls.job = cls.workflow['jobs']['candidate-artifacts']
        cls.steps = cls.job['steps']
        cls.prepare = next(step for step in cls.steps
                           if step['name'] == 'Verify separate Hotel startup-safe preparation twice (no publication)')
        cls.native = next(step for step in cls.steps
                          if step['name'] == 'Test startup-safe Hotel sources without external network or databases')

    def test_source_preparation_selects_separate_recipe_and_runs_before_native_checks(self):
        self.assertEqual(self.prepare['run'].strip(),
                         'python scripts/validate_deathstarbench_hotel_startup.py prepare '
                         '--upstream .build/upstream --output .build/hotel-startup')
        self.assertLess(self.steps.index(self.prepare), self.steps.index(self.native))
        source_checkout = next(step for step in self.steps
                               if step.get('with', {}).get('repository') == 'delimitrou/DeathStarBench')
        self.assertEqual(source_checkout['with']['ref'], '6ecb09706140f8730b5385c08f1386c654c3c526')
        self.assertIs(source_checkout['with']['persist-credentials'], False)

    def test_native_builder_is_pinned_rootless_readonly_and_has_no_external_network(self):
        run = self.native['run']
        for argument in ('docker run --rm --network=none --read-only --cap-drop=ALL',
                         '--user "$(id -u):$(id -g)"', '--security-opt=no-new-privileges',
                         '--tmpfs /tmp:rw,exec,nosuid,nodev,size=2g',
                         '-v "$PWD/.build/hotel-startup/first/app:/workspace:ro"',
                         '-v "$PWD/.build/hotel-startup-attestations:/evidence:rw"',
                         '-e CGO_ENABLED=0', '-e GO111MODULE=on', '-e GOTOOLCHAIN=local',
                         '-e GOPROXY=off', '-e GOSUMDB=off', BUILDER):
            with self.subTest(argument=argument):
                self.assertIn(argument, run)
        self.assertEqual(run.count('docker run '), 1)
        self.assertIn('test "$(go env GOARCH)" = amd64', run)
        self.assertNotIn('--privileged', run)
        self.assertNotIn('--network=host', run)

    def test_native_job_executes_all_reviewed_startup_helpers_offline(self):
        run = self.native['run']
        command = re.search(r'^\s*go test -mod=vendor -vet=off (.+)$', run, re.MULTILINE)
        self.assertIsNotNone(command)
        self.assertEqual(command.group(1).split(), list(preparation.GO_TEST_PACKAGES))
        self.assertIn('go build -mod=vendor -trimpath -o /tmp/hotel-bin/ ./cmd/...', run)

    def test_exactly_ten_startup_metadata_files_and_one_unchanged_initializer_are_collected(self):
        run = self.native['run']
        loops = re.findall(r'for command in ([^;\n]+); do', run)
        self.assertEqual(len(loops), 1)
        commands = loops[0].split()
        self.assertEqual(commands, list(preparation.APPLICATIONS))
        self.assertEqual(len(commands), 10)
        self.assertNotIn('hotel-initialize', commands)
        self.assertIn('TLS=true /tmp/hotel-bin/$command --candidate-startup-attestation '
                      '> /evidence/$command.json', run)
        initializer = ('/tmp/hotel-bin/hotel-initialize --candidate-initialization-attestation '
                       '> /evidence/hotel-initialize.json')
        self.assertEqual(run.count(initializer), 1)
        self.assertNotIn('initialize-empty', run)

    def test_ordinary_unbound_startup_is_refused_outside_baked_configuration_directory(self):
        run = self.native['run']
        self.assertLess(run.index('cd /tmp\n'), run.index('for command in '))
        self.assertIn('if /tmp/hotel-bin/$command > /tmp/refused.stdout 2> /tmp/refused.stderr; then', run)
        self.assertIn('Unexpected unbound startup application success', run)
        self.assertIn('code=$?', run)
        self.assertIn('test "$code" -eq 2', run)
        self.assertNotIn('HOTEL_CANDIDATE_RUN_ID', run)
        self.assertNotIn('HOTEL_CANDIDATE_POD_UID', run)

    def test_metadata_verification_reconstructs_from_explicit_pinned_upstream(self):
        run = self.native['run']
        verifier = ('python scripts/validate_deathstarbench_hotel_startup.py verify '
                    '--preparation .build/hotel-startup/first --upstream .build/upstream '
                    '--native-evidence .build/hotel-startup-attestations --output .build/hotel-startup-native')
        self.assertIn(verifier, run)
        self.assertGreater(run.index(verifier), run.index('/evidence/hotel-initialize.json'))

    def test_startup_steps_have_no_publication_cloud_controller_or_public_release_authority(self):
        self.assertEqual(self.workflow['permissions'], {'contents': 'read'})
        self.assertNotIn('permissions', self.job)
        text = '\n'.join(step['run'] for step in (self.prepare, self.native))
        for forbidden in ('secrets.', 'docker push', 'docker login', 'buildx', 'gh workflow',
                          'kubectl', 'k3s ', 'qualify_deathstarbench_', 'publish_deathstarbench_',
                          'application_start_allowed', 'released: true', 'public_release: true',
                          'aws ', 'az ', 'gcloud ', 'oci '):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, text)
        self.assertNotIn('env', self.prepare)
        self.assertNotIn('env', self.native)

    def test_retained_artifacts_are_only_bounded_offline_startup_receipts(self):
        upload = next(step for step in self.steps
                      if step['name'] == 'Retain preparation receipts (not image or live qualification evidence)')
        paths = upload['with']['path'].splitlines()
        startup_paths = [path for path in paths if 'hotel-startup' in path]
        self.assertEqual(startup_paths, [
            '.build/hotel-startup/preparation-validation.json',
            '.build/hotel-startup/first/context-manifest.json',
            '.build/hotel-startup-native/native-metadata-validation.json',
            '.build/hotel-startup-attestations/*.json',
        ])
        self.assertEqual(upload['with']['retention-days'], 14)
        self.assertEqual(upload['with']['if-no-files-found'], 'error')
        self.assertNotIn('.build/hotel-startup/**', paths)


if __name__ == '__main__':
    unittest.main()
