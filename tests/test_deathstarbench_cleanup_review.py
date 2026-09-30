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

    def test_checked_in_receipt_is_valid_and_azure_keeps_gate_closed(self):
        validated = review.validate_cleanup_review(self.receipt)

        self.assertFalse(validated['overall_gate_complete'])
        self.assertFalse(
            validated['providers']['azure']['representative_matrix']['complete']
        )
        self.assertTrue(
            validated['providers']['aws']['representative_matrix']['complete']
        )
        self.assertTrue(
            validated['providers']['gcp']['representative_matrix']['complete']
        )
        self.assertTrue(
            validated['providers']['oci']['representative_matrix']['complete']
        )

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

    def test_overall_gate_cannot_open_while_any_provider_matrix_is_incomplete(self):
        unsafe = copy.deepcopy(self.receipt)
        unsafe['overall_gate_complete'] = True

        with self.assertRaisesRegex(
            review.CleanupReviewValidationError,
            'incomplete providers: azure',
        ):
            review.validate_cleanup_review(unsafe)


if __name__ == '__main__':
    unittest.main()
