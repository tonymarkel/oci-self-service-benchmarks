import unittest

from tests.test_deathstarbench_hotel_benchmark import METRICS
from app.deathstarbench_synthetic_metrics import parse_body_metrics


class SyntheticMetricsTests(unittest.TestCase):
    def test_media_body_accounting_matches_driver_and_rejects_crossed_workload(self):
        output = METRICS.replace('hotel_reservation', 'media_microservices')
        result = parse_body_metrics(output, workload='media_microservices', label='Media')
        self.assertEqual(result['successful_requests'], 90)
        self.assertEqual(result['semantic_failures'], 8)
        self.assertEqual(result['error_rate_percent'], 10)
        with self.assertRaisesRegex(RuntimeError, 'different workload'):
            parse_body_metrics(METRICS, workload='media_microservices', label='Media')

    def test_workload_error_type_is_preserved(self):
        with self.assertRaises(ValueError):
            parse_body_metrics('', workload='media_microservices', label='Media', error_type=ValueError)
