import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app import comparison, main


def benchmark_job(run_id, *, provider='aws', value=100.0):
    plan = {
        'provider': provider,
        'region': 'us-east-2' if provider == 'aws' else 'us-central1',
        'gcp_zone': 'us-central1-a' if provider == 'gcp' else None,
        'shape': 'c7i.2xlarge' if provider == 'aws' else 'c4-standard-8',
        'ocpus': 8,
        'memory_gb': 16,
        'benchmarks': ['sysbench'],
        'llm_benchmarks': [],
        'sysbench': {'workloads': ['cpu']},
    }
    return {
        'id': run_id,
        'status': 'destroyed',
        'benchmark_status': 'complete',
        'error': None,
        'events': [],
        'resources': {},
        'plan': plan,
        'results': [{
            'id': 'sysbench_cpu',
            'name': 'Sysbench — CPU',
            'status': 'completed',
            'started_at': '2026-08-26T12:00:00+00:00',
            'duration_seconds': 60.0,
            'command': 'sysbench --password=never-persist-this',
            'output': 'raw benchmark output must stay out of results.json',
            'metadata': {
                'provider': provider.upper(),
                'architecture': 'x86_64',
                'private_ip': '10.0.0.7',
            },
            'metrics': {'events_per_second': value},
        }],
        'created_at': '2026-08-26T12:00:00+00:00',
        'updated_at': '2026-08-26T12:02:00+00:00',
    }


def response_json(response):
    return json.loads(response.body.decode())


class ResultsArtifactPersistenceTests(unittest.TestCase):
    def test_state_persistence_writes_sanitized_versioned_results(self):
        job = benchmark_job('aaaaaaaaaaaa')
        job['_persist_state'] = True
        job['plan']['ssh_private_key'] = 'PRIVATE-SECRET'
        job['results'][0]['metrics']['api_secret'] = 'METRIC-SECRET'
        job['results'][0]['metadata']['password'] = 'PASSWORD-SECRET'

        with tempfile.TemporaryDirectory() as directory, patch.object(
            main,
            'RUNS',
            Path(directory),
        ):
            main.persist_job_state(job)
            artifact_text = (
                Path(directory) / job['id'] / 'results.json'
            ).read_text()
            artifact = json.loads(artifact_text)
            state = json.loads(
                (Path(directory) / job['id'] / 'state.json').read_text()
            )

        self.assertEqual(artifact['$schema'], comparison.RESULTS_SCHEMA)
        self.assertEqual(
            artifact['schema_version'],
            comparison.RESULTS_SCHEMA_VERSION,
        )
        self.assertEqual(
            artifact['results'][0]['metrics']['events_per_second'],
            100.0,
        )
        for forbidden in (
            'PRIVATE-SECRET',
            'METRIC-SECRET',
            'PASSWORD-SECRET',
            '10.0.0.7',
            'raw benchmark output',
            '--password=',
        ):
            self.assertNotIn(forbidden, artifact_text)
        self.assertEqual(
            state['results'],
            [{
                'id': 'sysbench_cpu',
                'name': 'Sysbench — CPU',
                'status': 'completed',
            }],
        )

    def test_restart_cleanup_state_cannot_overwrite_rich_results(self):
        job = benchmark_job('bbbbbbbbbbbb')
        job['_persist_state'] = True

        with tempfile.TemporaryDirectory() as directory, patch.object(
            main,
            'RUNS',
            Path(directory),
        ):
            main.persist_job_state(job)
            artifact_path = Path(directory) / job['id'] / 'results.json'
            original = artifact_path.read_bytes()

            recovered = main.load_persisted_job(job['id'])
            self.assertIs(recovered['_persist_results_artifact'], False)
            self.assertNotIn('metrics', recovered['results'][0])
            recovered['status'] = 'destroying'
            recovered['updated_at'] = '2026-08-26T12:03:00+00:00'
            main.persist_job_state(recovered)

            self.assertEqual(artifact_path.read_bytes(), original)
            self.assertEqual(
                comparison.read_results_artifact(artifact_path)['results'][0]
                ['metrics']['events_per_second'],
                100.0,
            )

    def test_report_embeds_safe_machine_readable_artifact(self):
        job = benchmark_job('cccccccccccc')
        job['results'][0]['name'] = (
            'Sysbench — CPU </script><script>alert("bad")</script>'
        )
        job['results'][0]['metadata']['note'] = '</script><script>bad()</script>'

        with tempfile.TemporaryDirectory() as directory, patch.object(
            main,
            'RUNS',
            Path(directory),
        ):
            main.make_report(job)
            report = (
                Path(directory) / job['id'] / 'report.html'
            ).read_text()

        match = re.search(
            r'<script id="benchmark-results" type="application/json">'
            r'(.*?)</script>',
            report,
            re.DOTALL,
        )
        self.assertIsNotNone(match)
        embedded = json.loads(match.group(1))
        self.assertEqual(embedded['$schema'], comparison.RESULTS_SCHEMA)
        self.assertEqual(
            embedded['results'][0]['metrics']['events_per_second'],
            100.0,
        )
        self.assertNotIn('</script><script>', match.group(1))
        self.assertEqual(report.count('<script'), 1)


class ComparisonApiTests(unittest.TestCase):
    def test_run_query_requires_two_to_eight_unique_canonical_ids(self):
        valid = ','.join(f'{value:012x}' for value in range(2, 10))
        self.assertEqual(len(main.parse_comparison_run_ids(valid)), 8)

        invalid_values = (
            None,
            'aaaaaaaaaaaa',
            ','.join(f'{value:012x}' for value in range(9)),
            'aaaaaaaaaaaa,aaaaaaaaaaaa',
            'AAAAAAAAAAAA,bbbbbbbbbbbb',
            'aaaaaaaaaaa,bbbbbbbbbbbb',
            'aaaaaaaaaaaa, bbbbbbbbbbbb',
            'aaaaaaaaaaaa,../../passwd',
        )
        for value in invalid_values:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    main.parse_comparison_run_ids(value)

        with self.assertRaises(HTTPException) as raised:
            main.comparisons('aaaaaaaaaaaa')
        self.assertEqual(raised.exception.status_code, 422)
        self.assertEqual(
            raised.exception.headers['Cache-Control'],
            'no-store',
        )

    def test_results_endpoint_recovers_a_legacy_report(self):
        job = benchmark_job('dddddddddddd')

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(main, 'RUNS', Path(directory)),
            patch.dict(main.jobs, {}, clear=True),
        ):
            main.make_report(job)
            run = Path(directory) / job['id']
            (run / 'results.json').unlink()
            report_path = run / 'report.html'
            report_path.write_text(re.sub(
                r'<script id="benchmark-results" type="application/json">'
                r'.*?</script>',
                '',
                report_path.read_text(),
                flags=re.DOTALL,
            ))

            response = main.job_results(job['id'])
            payload = response_json(response)

        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertEqual(payload['provenance'], 'legacy_report')
        self.assertEqual(payload['results'][0]['id'], 'sysbench_cpu')
        self.assertEqual(
            payload['results'][0]['metrics']['events_per_second'],
            100.0,
        )
        self.assertNotIn('output', payload['results'][0])

    def test_comparison_endpoint_returns_chart_ready_payload(self):
        run_ids = ('eeeeeeeeeeee', 'ffffffffffff')
        jobs = (
            benchmark_job(run_ids[0], provider='aws', value=100.0),
            benchmark_job(run_ids[1], provider='gcp', value=125.0),
        )

        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(main, 'RUNS', Path(directory)),
            patch.dict(main.jobs, {}, clear=True),
        ):
            for job in jobs:
                comparison.write_results_artifact(
                    Path(directory) / job['id'],
                    job,
                )
            response = main.comparisons(','.join(run_ids))
            payload = response_json(response)

        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertEqual(payload['schema_version'], 1)
        self.assertEqual([item['id'] for item in payload['runs']], list(run_ids))
        self.assertTrue(all(item['ocpus'] == 8 for item in payload['runs']))
        chart = next(
            item for item in payload['charts']
            if item['metric_id'] == 'events_per_second'
        )
        self.assertEqual(chart['result_id'], 'sysbench_cpu')
        self.assertEqual(chart['direction'], 'higher')
        self.assertEqual(
            [(item['run_id'], item['value']) for item in chart['values']],
            [(run_ids[0], 100.0), (run_ids[1], 125.0)],
        )
        self.assertEqual(payload['excluded'], [])

    def test_results_endpoint_rejects_a_mismatched_artifact_identity(self):
        requested_id = '123456789abc'
        job = benchmark_job('abcdef123456')
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(main, 'RUNS', Path(directory)),
            patch.dict(main.jobs, {}, clear=True),
        ):
            comparison.write_results_artifact(
                Path(directory) / requested_id,
                job,
            )
            with self.assertRaises(HTTPException) as raised:
                main.job_results(requested_id)

        self.assertEqual(raised.exception.status_code, 422)
        self.assertEqual(
            raised.exception.headers['Cache-Control'],
            'no-store',
        )

    def test_results_only_crash_window_remains_visible_in_history(self):
        job = benchmark_job('0123456789ab')
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / job['id']
            comparison.write_results_artifact(run, job)

            summary = main.run_summary(run)

        self.assertFalse(summary['report_ready'])
        self.assertEqual(summary['benchmark_status'], 'complete')
        self.assertEqual(summary['comparison_result_ids'], ['sysbench_cpu'])


if __name__ == '__main__':
    unittest.main()
