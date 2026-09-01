import json
import unittest

from app.iperf3 import parse_output


def result(protocol='TCP', **end):
    return json.dumps({
        'start': {
            'test_start': {
                'protocol': protocol,
                'num_streams': 1 if protocol == 'UDP' else 4,
            },
        },
        'end': end,
    })


class Iperf3ResultTests(unittest.TestCase):
    def test_parses_tcp_sender_receiver_and_retransmit_metrics(self):
        output = result(
            sum_sent={
                'seconds': 60.001,
                'bytes': 9_000_000_000,
                'bits_per_second': 1_199_980_000,
                'retransmits': 7,
            },
            sum_received={
                'seconds': 59.998,
                'bytes': 8_990_000_000,
                'bits_per_second': 1_198_000_000,
            },
        )

        metrics = parse_output(output, expected_protocol='tcp')

        self.assertEqual(metrics['protocol'], 'TCP')
        self.assertEqual(metrics['parallel_streams'], 4)
        self.assertEqual(metrics['sender_gigabits_per_second'], 1.19998)
        self.assertEqual(metrics['receiver_gigabits_per_second'], 1.198)
        self.assertEqual(metrics['retransmits'], 7)

    def test_parses_udp_throughput_jitter_and_loss_metrics(self):
        output = result(
            'UDP',
            sum={
                'seconds': 60.0,
                'bytes': 7_500_000_000,
                'bits_per_second': 1_000_000_000,
                'jitter_ms': 0.071,
                'lost_packets': 2,
                'packets': 100_000,
                'lost_percent': 0.002,
            },
        )

        metrics = parse_output(output, expected_protocol='udp')

        self.assertEqual(metrics['protocol'], 'UDP')
        self.assertEqual(metrics['throughput_gigabits_per_second'], 1.0)
        self.assertEqual(metrics['jitter_ms'], 0.071)
        self.assertEqual(metrics['lost_packets'], 2)
        self.assertEqual(metrics['packet_loss_percent'], 0.002)

    def test_parses_sctp_without_assuming_a_tcp_retransmit_field(self):
        output = result(
            'SCTP',
            sum_sent={
                'seconds': 60.002,
                'bytes': 6_000_000_000,
                'bits_per_second': 799_973_334,
            },
            sum_received={
                'seconds': 60.0,
                'bytes': 5_990_000_000,
                'bits_per_second': 798_666_667,
            },
        )

        metrics = parse_output(output, expected_protocol='sctp')

        self.assertEqual(metrics['protocol'], 'SCTP')
        self.assertEqual(metrics['parallel_streams'], 4)
        self.assertEqual(metrics['sender_gigabits_per_second'], 0.799973)
        self.assertEqual(metrics['receiver_gigabits_per_second'], 0.798667)
        self.assertEqual(metrics['bytes_sent'], 6_000_000_000)
        self.assertEqual(metrics['bytes_received'], 5_990_000_000)
        self.assertNotIn('retransmits', metrics)

    def test_prefers_modern_udp_receiver_summary_over_sender_sum(self):
        output = result(
            'UDP',
            sum={
                'seconds': 60.0,
                'bytes': 10_000_000_000,
                'bits_per_second': 1_333_333_333,
                'jitter_ms': 0,
                'lost_packets': 0,
                'packets': 200_000,
                'lost_percent': 0,
            },
            sum_received={
                'seconds': 60.0,
                'bytes': 9_000_000_000,
                'bits_per_second': 1_200_000_000,
                'jitter_ms': 0.125,
                'lost_packets': 100,
                'packets': 199_000,
                'lost_percent': 0.050251,
            },
        )

        metrics = parse_output(output, expected_protocol='udp')

        self.assertEqual(metrics['throughput_gigabits_per_second'], 1.2)
        self.assertEqual(metrics['bytes_transferred'], 9_000_000_000)
        self.assertEqual(metrics['jitter_ms'], 0.125)
        self.assertEqual(metrics['lost_packets'], 100)

    def test_rejects_error_wrong_protocol_and_short_or_invalid_results(self):
        with self.assertRaisesRegex(ValueError, 'reported an error'):
            parse_output(
                json.dumps({'error': 'unable to connect'}),
                expected_protocol='tcp',
            )
        with self.assertRaisesRegex(ValueError, 'expected TCP'):
            parse_output(
                result('UDP', sum={}),
                expected_protocol='tcp',
            )
        with self.assertRaisesRegex(ValueError, 'before the requested'):
            parse_output(
                result(
                    sum_sent={
                        'seconds': 2,
                        'bytes': 1,
                        'bits_per_second': 1,
                        'retransmits': 0,
                    },
                    sum_received={
                        'seconds': 2,
                        'bytes': 1,
                        'bits_per_second': 1,
                    },
                ),
                expected_protocol='tcp',
            )
        with self.assertRaisesRegex(ValueError, 'valid JSON'):
            parse_output('not-json', expected_protocol='udp')

    def test_rejects_an_unexpected_parallel_stream_count(self):
        document = json.loads(result(
            'SCTP',
            sum_sent={
                'seconds': 60,
                'bytes': 1,
                'bits_per_second': 1,
            },
            sum_received={
                'seconds': 60,
                'bytes': 1,
                'bits_per_second': 1,
            },
        ))
        document['start']['test_start']['num_streams'] = 1

        with self.assertRaisesRegex(ValueError, 'expected exactly 4'):
            parse_output(json.dumps(document), expected_protocol='sctp')


if __name__ == '__main__':
    unittest.main()
