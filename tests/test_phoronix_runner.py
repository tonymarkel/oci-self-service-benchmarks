import json
import subprocess
import tempfile
import unittest

from app import phoronix
from app.catalog import PHORONIX_PROFILES


def result_output(
    *,
    raw_values=None,
    value=1234.5,
    scale='MIPS',
    proportion='HIB',
):
    if raw_values is None:
        raw_values = [1220.0, 1234.5, 1240.0]
    payload = {
        'title': 'OCI Phoronix result',
        'results': {
            'result-hash': {
                'identifier': 'pts/compress-7zip-1.13.1',
                'title': '7-Zip Compression',
                'arguments': '',
                'description': 'Default',
                'scale': scale,
                'proportion': proportion,
                'results': {
                    'OCI benchmark runner': {
                        'value': value,
                        'raw_values': raw_values,
                        'test_run_times': [60.1, 59.9, 60.0],
                    },
                },
            },
        },
    }
    return (
        'normal console output\n'
        f'{phoronix.EXPORT_START}\n'
        f'{json.dumps(payload)}\n'
        f'{phoronix.EXPORT_END}\n'
    )


class PhoronixProfileTests(unittest.TestCase):
    def test_profiles_are_exactly_pinned_and_have_separate_result_ids(self):
        expected = {
            'compress_7zip': (
                'phoronix_compress_7zip',
                'pts/compress-7zip-1.13.1',
                None,
            ),
            'openssl': (
                'phoronix_openssl',
                'pts/openssl-3.6.0',
                'openssl.algo=SHA256',
            ),
            'build_linux_kernel': (
                'phoronix_build_linux_kernel',
                'pts/build-linux-kernel-1.18.0',
                'build-linux-kernel.build=defconfig',
            ),
            'tinymembench': (
                'phoronix_tinymembench',
                'pts/tinymembench-1.0.2',
                None,
            ),
        }

        self.assertEqual(set(phoronix.PROFILES), set(expected))
        self.assertEqual(
            len({item.result_id for item in phoronix.PROFILES.values()}),
            len(expected),
        )
        for profile_id, expected_values in expected.items():
            item = phoronix.profile(profile_id)
            self.assertEqual(
                (item.result_id, item.profile, item.preset_options),
                expected_values,
            )

        with self.assertRaisesRegex(ValueError, 'Unsupported Phoronix profile'):
            phoronix.profile('arbitrary-openbenchmarking-profile')

    def test_runner_profiles_match_the_public_catalog(self):
        catalog_profiles = {
            item['id']: item['profile'] for item in PHORONIX_PROFILES
        }
        runner_profiles = {
            item.id: item.profile for item in phoronix.PROFILES.values()
        }

        self.assertEqual(runner_profiles, catalog_profiles)

    def test_packages_cover_curated_source_builds(self):
        packages = phoronix.required_packages([
            'compress_7zip',
            'openssl',
            'build_linux_kernel',
            'tinymembench',
        ])

        self.assertTrue({
            'git',
            'gcc',
            'gcc-c++',
            'make',
            'php-cli',
            'php-xml',
            'perl-core',
            'bc',
            'bison',
            'flex',
            'gawk',
            'cpio',
            'elfutils-libelf-devel',
            'openssl-devel',
        }.issubset(packages))
        self.assertEqual(phoronix.required_packages([]), set())


class PhoronixCommandTests(unittest.TestCase):
    def assert_valid_bash(self, command):
        with tempfile.NamedTemporaryFile(mode='w') as script:
            script.write(command)
            script.flush()
            subprocess.run(
                ['bash', '-n', script.name],
                check=True,
                capture_output=True,
                text=True,
            )

    def test_prepare_is_idempotent_and_checks_out_one_exact_client_revision(self):
        command = phoronix.prepare_command()

        self.assertIn(phoronix.PTS_REPOSITORY, command)
        self.assertIn(phoronix.PTS_REVISION, command)
        self.assertIn(
            'timeout 120 git -C "$PTS_DIR" fetch --depth 1 origin "$PTS_REV"',
            command,
        )
        self.assertIn('checkout --detach "$PTS_REV"', command)
        self.assertIn('rev-parse HEAD', command)
        self.assertIn('RunAllTestCombinations=FALSE', command)
        self.assertIn('PromptForTestIdentifier=FALSE', command)
        self.assertIn('UploadResults=FALSE', command)
        self.assertIn('AnonymousUsageReporting=FALSE', command)
        self.assertIn('openbenchmarking-refresh', command)
        self.assertIn('for attempt in $(seq 1 3)', command)
        self.assertIn('REFRESHED=FALSE', command)
        self.assertIn('test "$REFRESHED" = TRUE', command)
        self.assertNotIn('docker', command.lower())
        for item in phoronix.PROFILES.values():
            self.assertNotIn(item.profile, command)
        self.assert_valid_bash(command)

    def test_each_profile_command_is_noninteractive_bounded_and_exported(self):
        for profile_id, item in phoronix.PROFILES.items():
            with self.subTest(profile=profile_id):
                command = phoronix.benchmark_command(profile_id, 'job-123')
                self.assertIn(
                    f'export FORCE_TIMES_TO_RUN={phoronix.MEASURED_TRIALS}',
                    command,
                )
                self.assertIn(
                    f'timeout --signal=TERM --kill-after=30s '
                    f'{phoronix.PROFILE_TIMEOUT_SECONDS}',
                    command,
                )
                self.assertIn(f'batch-benchmark {item.profile}', command)
                self.assertIn(f'info {item.profile}', command)
                self.assertIn(
                    'test -s "$HOME/.phoronix-test-suite/test-results/'
                    '$RESULT_NAME/composite.xml"',
                    command,
                )
                self.assertIn('result-file-to-json "$RESULT_NAME"', command)
                self.assertIn(phoronix.EXPORT_START, command)
                self.assertIn(phoronix.EXPORT_END, command)
                self.assertIn('unset PRESET_OPTIONS PRESET_OPTIONS_VALUES', command)
                self.assertNotIn('docker', command.lower())
                if item.preset_options:
                    self.assertIn(
                        f'export PRESET_OPTIONS={item.preset_options}',
                        command,
                    )
                else:
                    self.assertNotIn('export PRESET_OPTIONS=', command)
                self.assert_valid_bash(command)

    def test_result_names_match_pts_clean_save_name_for_every_profile(self):
        expected_names = {
            'compress_7zip': 'oci-phoronix-job-123-compress7zip',
            'openssl': 'oci-phoronix-job-123-openssl',
            'build_linux_kernel': 'oci-phoronix-job-123-buildlinuxkernel',
            'tinymembench': 'oci-phoronix-job-123-tinymembench',
        }

        for profile_id, expected_name in expected_names.items():
            with self.subTest(profile=profile_id):
                command = phoronix.benchmark_command(profile_id, 'JOB-123')
                self.assertIn(f'RESULT_NAME={expected_name};', command)
                self.assertIn('export TEST_RESULTS_NAME="$RESULT_NAME"', command)
                self.assertIn(
                    'result-file-to-json "$RESULT_NAME"',
                    command,
                )
                self.assertNotIn(f'RESULT_NAME={profile_id};', command)

    def test_pts_save_name_cleaning_matches_the_pinned_client(self):
        self.assertEqual(
            phoronix._pts_clean_save_name(' Build_linux_kernel  RUN '),
            'buildlinuxkernel-run',
        )
        self.assertEqual(
            phoronix._pts_clean_save_name('A_ result!? -- with symbols'),
            'a-result-with-symbols',
        )
        self.assertEqual(
            phoronix._pts_clean_save_name('A' * 140),
            'a' * 126,
        )

    def test_7zip_keeps_the_verified_download_cache_workaround(self):
        command = phoronix.benchmark_command('compress_7zip', 'job-123')

        self.assertIn(phoronix.SEVEN_ZIP_ASSET.filename, command)
        self.assertIn(phoronix.SEVEN_ZIP_ASSET.url, command)
        self.assertIn(phoronix.SEVEN_ZIP_ASSET.sha256, command)
        self.assertGreaterEqual(command.count('sha256sum -c -'), 2)

    def test_run_specs_are_deduplicated_and_independent(self):
        runs = phoronix.profile_runs(
            ['openssl', 'compress_7zip', 'openssl'],
            'job-123',
        )

        self.assertEqual(
            [run.benchmark_id for run in runs],
            ['phoronix_openssl', 'phoronix_compress_7zip'],
        )
        self.assertEqual(
            [run.timeout_seconds for run in runs],
            [phoronix.SSH_TIMEOUT_SECONDS] * 2,
        )
        self.assertNotEqual(runs[0].command, runs[1].command)
        self.assertEqual(
            runs[0].metadata['fixed_options'],
            'openssl.algo=SHA256',
        )
        self.assertNotIn('fixed_options', runs[1].metadata)

    def test_untrusted_run_token_cannot_add_shell_syntax(self):
        command = phoronix.benchmark_command(
            'openssl',
            'job; touch /tmp/not-allowed $(id)',
        )

        self.assertNotIn('touch /tmp/not-allowed', command)
        self.assertNotIn('$(id)', command)
        self.assertIn('job-touch-tmp-not-allowed-id', command)
        self.assert_valid_bash(command)


class PhoronixResultParserTests(unittest.TestCase):
    def test_extracts_and_summarizes_a_complete_three_trial_export(self):
        output = result_output()

        export = phoronix.extract_result_export(output)
        metrics = phoronix.parse_result_output(
            output,
            expected_profile='pts/compress-7zip-1.13.1',
        )

        self.assertEqual(export['title'], 'OCI Phoronix result')
        self.assertEqual(metrics['measurement_count'], 1)
        self.assertEqual(metrics['profile'], 'pts/compress-7zip-1.13.1')
        self.assertEqual(metrics['benchmark'], '7-Zip Compression')
        self.assertEqual(metrics['score'], 1234.5)
        self.assertEqual(metrics['unit'], 'MIPS')
        self.assertEqual(metrics['direction'], 'Higher is better')
        self.assertEqual(
            metrics['measured_trials'],
            [1220.0, 1234.5, 1240.0],
        )
        self.assertEqual(
            metrics['test_run_times_seconds'],
            [60.1, 59.9, 60.0],
        )

    def test_preserves_multiple_generic_measurements(self):
        output = result_output(proportion='LIB', scale='Seconds')
        payload = phoronix.extract_result_export(output)
        result = payload['results']['result-hash']
        result['results']['second system'] = {
            'value': 10.0,
            'raw_values': [10.1, 10.0, 9.9],
        }
        output = (
            f'{phoronix.EXPORT_START}\n{json.dumps(payload)}\n'
            f'{phoronix.EXPORT_END}'
        )

        metrics = phoronix.parse_result_output(output)

        self.assertEqual(metrics['measurement_count'], 2)
        self.assertEqual(len(metrics['measurements']), 2)
        self.assertTrue(all(
            item['direction'] == 'Lower is better'
            for item in metrics['measurements']
        ))

    def test_rejects_missing_incomplete_and_invalid_exports(self):
        invalid_outputs = (
            ('console only', 'did not emit'),
            (f'{phoronix.EXPORT_START}\n{{}}', 'incomplete'),
            (
                f'{phoronix.EXPORT_START}\nnot-json\n{phoronix.EXPORT_END}',
                'invalid structured JSON',
            ),
            (
                f'{phoronix.EXPORT_START}\n{{}}\n{phoronix.EXPORT_END}',
                'contains no benchmark results',
            ),
        )

        for output, message in invalid_outputs:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    phoronix.parse_result_output(output)

    def test_rejects_partial_or_non_numeric_trials(self):
        with self.assertRaisesRegex(ValueError, 'completed 2 of 3'):
            phoronix.parse_result_output(
                result_output(raw_values=[1220.0, 1234.5])
            )

        with self.assertRaisesRegex(ValueError, 'trial values must be numeric'):
            phoronix.parse_result_output(
                result_output(raw_values=[1220.0, 'failed', 1240.0])
            )

        with self.assertRaisesRegex(ValueError, 'does not contain a score'):
            phoronix.parse_result_output(result_output(value='1234.5'))

        with self.assertRaisesRegex(ValueError, 'does not contain a score'):
            phoronix.parse_result_output(result_output(value=float('inf')))

        with self.assertRaisesRegex(ValueError, 'trial values must be numeric'):
            phoronix.parse_result_output(
                result_output(raw_values=[1220.0, float('nan'), 1240.0])
            )

    def test_rejects_a_result_export_for_the_wrong_profile(self):
        with self.assertRaisesRegex(ValueError, 'unexpected profile'):
            phoronix.parse_result_output(
                result_output(),
                expected_profile='pts/openssl-3.6.0',
            )


if __name__ == '__main__':
    unittest.main()
