import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import main
from app.models import BenchmarkPlan


class ResultCompletionGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_job_cannot_report_an_omitted_selected_result(self):
        plan = BenchmarkPlan(
            region='us-ashburn-1',
            shape='VM.Standard.E5.Flex',
            ssh_private_key='private',
            ssh_public_key='ssh-ed25519 AAAATEST',
            storage={'additional_volume': False},
            benchmarks=['stream'],
            destroy_after_completion=False,
        )
        job = {
            'id': 'missing-stream-result',
            'plan': plan.model_dump(exclude={
                'ssh_private_key',
                'ssh_public_key',
                'ssh_key_passphrase',
            }),
            'status': 'queued',
            'events': [],
            'resources': {},
            'results': [],
            'created_at': main.now(),
            'updated_at': main.now(),
        }

        with (
            patch.object(main, 'provision'),
            patch.object(main, 'run_benchmarks'),
            patch.object(main, 'make_report') as make_report,
        ):
            await main.run_job(job, plan)

        self.assertEqual(job['status'], 'failed')
        self.assertEqual(main.benchmark_status(job), 'failed')
        self.assertIn('Incomplete result IDs: stream.', job['error'])
        make_report.assert_not_called()


class SavedResultReportingTests(unittest.TestCase):
    def test_report_only_failure_with_error_text_takes_precedence(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / 'report.html'
            report.write_text(
                '<p>Status: <strong>completed</strong></p>'
                '<p>Status: <strong>failed</strong> · setup failed</p>'
            )

            outcome = main.saved_report_benchmark_status(report)

        self.assertEqual(outcome, 'failed')

    def test_history_unions_recorded_and_selected_but_missing_results(self):
        plan = {
            'provider': 'aws',
            'region': 'us-east-2',
            'shape': 'm7i.2xlarge',
            'benchmarks': ['apachebench', 'deathstarbench'],
            'llm_benchmarks': [],
            'apachebench': {
                'workloads': ['new_connections', 'keep_alive'],
            },
            'deathstarbench': {'workload': 'media_microservices'},
        }
        state = {
            'id': 'partial-web-run',
            'status': 'failed',
            'benchmark_status': 'failed',
            'error': 'DeathStarBench setup failed.',
            'plan': plan,
            'events': [],
            'resources': {},
            'results': [{
                'id': 'apachebench_new_connections',
                'name': 'ApacheBench — New Connections',
                'status': 'completed',
            }, {
                'id': 'apachebench_keep_alive',
                'name': 'ApacheBench — Keep-Alive',
                'status': 'failed',
            }],
        }
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / state['id']
            run.mkdir()
            (run / 'plan.json').write_text(json.dumps(plan))
            (run / 'state.json').write_text(json.dumps(state))

            summary = main.run_summary(run)

        self.assertEqual(summary['benchmark_status'], 'failed')
        self.assertEqual(summary['benchmarks'], [
            'ApacheBench — New Connections',
            'ApacheBench — Keep-Alive',
            'DeathStarBench — Media Microservices',
        ])
        # state.json intentionally retains only lifecycle summaries. Without
        # a results artifact or report metrics, the completed row is not safe
        # to advertise as chartable.
        self.assertEqual(summary['comparison_result_ids'], [])
        self.assertEqual(summary['cpu_kind'], 'vCPU')

    def test_saved_complete_state_is_recomputed_when_results_are_missing(self):
        plan = {
            'provider': 'oci',
            'region': 'us-ashburn-1',
            'shape': 'VM.Standard.E5.Flex',
            'benchmarks': ['stream'],
            'llm_benchmarks': [],
        }
        state = {
            'id': 'stale-complete-run',
            'status': 'destroyed',
            'benchmark_status': 'complete',
            'error': None,
            'plan': plan,
            'events': [],
            'resources': {},
            'results': [],
        }
        with tempfile.TemporaryDirectory() as directory:
            runs = Path(directory)
            run = runs / state['id']
            run.mkdir()
            (run / 'plan.json').write_text(json.dumps(plan))
            (run / 'state.json').write_text(json.dumps(state))
            with (
                patch.object(main, 'RUNS', runs),
                patch.dict(main.jobs, {}, clear=True),
            ):
                status = main.job_status(state['id'])
                summary = main.run_summary(run)

        self.assertEqual(status['benchmark_status'], 'pending')
        self.assertEqual(summary['benchmark_status'], 'pending')

    def test_frontend_does_not_present_noncomplete_outcomes_as_success(self):
        root = Path(__file__).resolve().parents[1]
        app_javascript = (root / 'app/static/app.js').read_text()
        history_javascript = (root / 'app/static/history.js').read_text()

        self.assertIn(
            "job.benchmark_status !== 'complete'",
            app_javascript,
        )
        self.assertIn(
            "destroyed: 'Incomplete · infrastructure destroyed'",
            history_javascript,
        )
        self.assertIn(
            "destroyed: 'Outcome unknown · infrastructure destroyed'",
            history_javascript,
        )


if __name__ == '__main__':
    unittest.main()
