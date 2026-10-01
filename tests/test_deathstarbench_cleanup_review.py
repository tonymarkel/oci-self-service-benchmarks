from __future__ import annotations

import copy
import json
from pathlib import Path
import unittest

from scripts import validate_deathstarbench_cleanup_review as review


RECEIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / 'docs'
    / 'qualification'
    / 'deathstarbench-distributed-cleanup-review-v1.json'
)


class DeathStarBenchCleanupReviewTests(unittest.TestCase):
    def setUp(self):
        self.receipt = json.loads(RECEIPT_PATH.read_text(encoding='utf-8'))

    def test_checked_in_receipt_is_valid_and_complete(self):
        validated = review.validate_cleanup_review(self.receipt)

        provider_completeness = []
        for record in validated['providers'].values():
            evidenced = record['representative_matrix'][
                'evidenced_scenario_runs'
            ]
            expected_complete = (
                set(evidenced) == review.REPRESENTATIVE_SCENARIOS
            )
            self.assertIs(
                record['representative_matrix']['complete'],
                expected_complete,
            )
            self.assertTrue(record['representative_matrix']['complete'])
            provider_completeness.append(expected_complete)
        self.assertIs(
            validated['overall_gate_complete'],
            all(provider_completeness),
        )
        self.assertTrue(validated['overall_gate_complete'])

    def test_validator_fails_closed_on_missing_provider_bad_id_or_bad_date(self):
        cases = []

        missing_provider = copy.deepcopy(self.receipt)
        missing_provider['providers'].pop('oci')
        cases.append(('missing provider', missing_provider, 'providers'))

        bad_id = copy.deepcopy(self.receipt)
        bad_id['providers']['aws']['representative_matrix'][
            'evidenced_scenario_runs'
        ]['safe_resume'] = 'NOT-A-RUN-ID'
        cases.append(('bad job ID', bad_id, 'job ID'))

        bad_date = copy.deepcopy(self.receipt)
        bad_date['reviewed_at'] = 'September 30, 2026'
        cases.append(('bad review date', bad_date, 'timestamp'))

        for label, value, message in cases:
            with self.subTest(label=label):
                with self.assertRaisesRegex(
                    review.CleanupReviewValidationError,
                    message,
                ):
                    review.validate_cleanup_review(value)

    def test_validator_fails_closed_on_live_inventory_or_false_terminal_state(self):
        live_inventory = copy.deepcopy(self.receipt)
        live_inventory['providers']['gcp']['independent_cloud_inventory'][
            'scopes'
        ][0]['queries'][0]['live_resource_count'] = 1

        false_predicate = copy.deepcopy(self.receipt)
        false_predicate['providers']['aws']['local_terminal_evidence'][
            'predicate_result'
        ] = False

        recoverable = copy.deepcopy(self.receipt)
        recoverable['providers']['oci']['local_terminal_evidence'][
            'recoverable_resources'
        ] = True

        for label, value, message in (
            ('live inventory', live_inventory, 'integer zero'),
            ('false predicate', false_predicate, 'must be true'),
            ('recoverable ownership', recoverable, 'must be false'),
        ):
            with self.subTest(label=label):
                with self.assertRaisesRegex(
                    review.CleanupReviewValidationError,
                    message,
                ):
                    review.validate_cleanup_review(value)

    def test_review_must_not_predate_independent_inventory(self):
        receipt = copy.deepcopy(self.receipt)
        receipt['reviewed_at'] = '2026-09-30T00:00:00Z'

        with self.assertRaisesRegex(
            review.CleanupReviewValidationError,
            'reviewed_at.*must not precede',
        ):
            review.validate_cleanup_review(receipt)

    def test_representative_runs_require_terminal_and_inventory_coverage(self):
        missing_terminal = copy.deepcopy(self.receipt)
        aws = missing_terminal['providers']['aws']
        safe_run_id = aws['representative_matrix']['evidenced_scenario_runs'][
            'safe_resume'
        ]
        aws['local_terminal_evidence']['sampled_run_ids'].remove(safe_run_id)

        missing_inventory = copy.deepcopy(self.receipt)
        aws = missing_inventory['providers']['aws']
        response_loss_run_id = aws['representative_matrix'][
            'evidenced_scenario_runs'
        ]['initializer_response_loss']
        for scope in aws['independent_cloud_inventory']['scopes']:
            scope['run_ids'] = [
                job_id
                for job_id in scope['run_ids']
                if job_id != response_loss_run_id
            ]

        for label, value, path, run_id in (
            (
                'missing terminal coverage',
                missing_terminal,
                'local_terminal_evidence.sampled_run_ids',
                safe_run_id,
            ),
            (
                'missing inventory coverage',
                missing_inventory,
                'independent_cloud_inventory.scopes',
                response_loss_run_id,
            ),
        ):
            with self.subTest(label=label):
                with self.assertRaisesRegex(
                    review.CleanupReviewValidationError,
                    rf'{path}.*missing {run_id}',
                ):
                    review.validate_cleanup_review(value)

    def test_one_run_may_cover_multiple_representative_scenarios(self):
        receipt = copy.deepcopy(self.receipt)
        aws = receipt['providers']['aws']
        matrix = aws['representative_matrix']['evidenced_scenario_runs']
        original_response_loss_run_id = matrix['initializer_response_loss']
        shared_run_id = matrix['safe_resume']
        matrix['initializer_response_loss'] = shared_run_id

        aws['local_terminal_evidence']['sampled_run_ids'] = [
            job_id
            for job_id in aws['local_terminal_evidence']['sampled_run_ids']
            if job_id != original_response_loss_run_id
        ]
        for scope in aws['independent_cloud_inventory']['scopes']:
            scope['run_ids'] = [
                job_id
                for job_id in scope['run_ids']
                if job_id != original_response_loss_run_id
            ]

        validated = review.validate_cleanup_review(receipt)

        self.assertEqual(
            validated['providers']['aws']['representative_matrix'][
                'evidenced_scenario_runs'
            ]['initializer_response_loss'],
            shared_run_id,
        )

    def test_overall_gate_cannot_open_while_any_provider_matrix_is_incomplete(self):
        unsafe = copy.deepcopy(self.receipt)
        unsafe['providers']['aws']['representative_matrix'][
            'evidenced_scenario_runs'
        ].pop('initializer_response_loss')
        unsafe['providers']['aws']['representative_matrix']['complete'] = False
        unsafe['overall_gate_complete'] = True

        with self.assertRaisesRegex(
            review.CleanupReviewValidationError,
            'incomplete providers: aws',
        ):
            review.validate_cleanup_review(unsafe)


if __name__ == '__main__':
    unittest.main()
