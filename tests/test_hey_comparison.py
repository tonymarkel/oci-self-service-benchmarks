import unittest

from app import comparison
from app.models import HeyOptions


class HeyComparisonTests(unittest.TestCase):
    def test_options_survive_export_and_workload_changes_split_comparisons(self):
        plan = {'benchmarks': ['hey'], 'hey': HeyOptions().model_dump()}
        safe = comparison.canonical_plan(plan)
        self.assertEqual(safe['hey'], plan['hey'])
        left = comparison.workload_fingerprint('hey_keep_alive', plan, {'tool_version': '0.1.4'})
        self.assertFalse(left['contract_unknown'])
        changed = {**plan, 'hey': {**plan['hey'], 'concurrency': 200}}
        right = comparison.workload_fingerprint('hey_keep_alive', changed, {'tool_version': '0.1.4'})
        self.assertNotEqual(left['fingerprint'], right['fingerprint'])
        other_version = comparison.workload_fingerprint('hey_keep_alive', plan, {'tool_version': '0.1.5'})
        self.assertNotEqual(left['fingerprint'], other_version['fingerprint'])
        unknown = comparison.workload_fingerprint('hey_keep_alive', plan, {})
        self.assertTrue(unknown['contract_unknown'])

    def test_metric_contract_is_separate_from_apachebench(self):
        self.assertIn('hey_keep_alive', comparison.METRIC_REGISTRY)
        self.assertIn('hey_new_connections', comparison.METRIC_REGISTRY)
        plan = {'hey': HeyOptions().model_dump()}
        hey = comparison.workload_fingerprint('hey_keep_alive', plan, {'tool_version': '0.1.4'})
        apache = comparison.workload_fingerprint('apachebench_keep_alive', {}, {})
        self.assertNotEqual(hey['fingerprint'], apache['fingerprint'])
