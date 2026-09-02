"""Strict parsing helpers for iperf3's JSON benchmark output."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping


SUPPORTED_PROTOCOLS = frozenset({'tcp', 'udp', 'sctp'})


def _mapping(value, label):
    if not isinstance(value, Mapping):
        raise ValueError(f'iperf3 output is missing {label}.')
    return value


def _number(mapping, key, label, *, allow_zero=False):
    value = mapping.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f'iperf3 {label} is missing or is not numeric.')
    number = float(value)
    if not math.isfinite(number) or number < 0 or (not allow_zero and number == 0):
        qualifier = 'non-negative' if allow_zero else 'positive'
        raise ValueError(
            f'iperf3 {label} must be finite and {qualifier}.'
        )
    return number


def _integer(mapping, key, label, *, allow_zero=False):
    number = _number(mapping, key, label, allow_zero=allow_zero)
    if not number.is_integer():
        raise ValueError(f'iperf3 {label} must be an integer.')
    return int(number)


def _validate_duration(seconds, expected_seconds):
    minimum = float(expected_seconds) * 0.9
    if seconds < minimum:
        raise ValueError(
            'iperf3 stopped before the requested timed interval: '
            f'{seconds:g}s observed; expected at least {minimum:g}s.'
        )


def parse_output(
    output: str,
    *,
    expected_protocol: str,
    expected_seconds: float = 60,
) -> dict[str, float | int | str]:
    """Validate one TCP, UDP, or SCTP run and return report-friendly metrics."""

    protocol = str(expected_protocol).strip().lower()
    if protocol not in SUPPORTED_PROTOCOLS:
        raise ValueError(f'Unsupported iperf3 result protocol: {protocol!r}.')
    try:
        document = json.loads(output)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError('iperf3 did not return valid JSON.') from exc
    document = _mapping(document, 'JSON document')
    if document.get('error'):
        raise ValueError(f'iperf3 reported an error: {document["error"]}')

    start = _mapping(document.get('start'), 'start metadata')
    test_start = _mapping(start.get('test_start'), 'test-start metadata')
    actual_protocol = str(test_start.get('protocol') or '').strip().lower()
    if actual_protocol != protocol:
        raise ValueError(
            f'iperf3 reported {actual_protocol or "no"} protocol for an '
            f'expected {protocol.upper()} run.'
        )
    streams = _integer(
        test_start,
        'num_streams',
        'parallel-stream count',
    )
    expected_streams = 1 if protocol == 'udp' else 4
    if streams != expected_streams:
        raise ValueError(
            f'iperf3 reported {streams} parallel streams for {protocol.upper()}; '
            f'expected exactly {expected_streams}.'
        )
    end = _mapping(document.get('end'), 'end results')

    if protocol in {'tcp', 'sctp'}:
        label = protocol.upper()
        sent = _mapping(end.get('sum_sent'), f'{label} sender summary')
        received = _mapping(
            end.get('sum_received'),
            f'{label} receiver summary',
        )
        sent_seconds = _number(sent, 'seconds', f'{label} sender duration')
        received_seconds = _number(
            received,
            'seconds',
            f'{label} receiver duration',
        )
        _validate_duration(min(sent_seconds, received_seconds), expected_seconds)
        sender_bps = _number(
            sent,
            'bits_per_second',
            f'{label} sender throughput',
        )
        receiver_bps = _number(
            received,
            'bits_per_second',
            f'{label} receiver throughput',
        )
        metrics: dict[str, float | int | str] = {
            'protocol': label,
            'parallel_streams': streams,
            'duration_seconds': round(min(sent_seconds, received_seconds), 6),
            'sender_gigabits_per_second': round(sender_bps / 1_000_000_000, 6),
            'receiver_gigabits_per_second': round(
                receiver_bps / 1_000_000_000,
                6,
            ),
            'bytes_sent': _integer(sent, 'bytes', f'{label} bytes sent'),
            'bytes_received': _integer(
                received,
                'bytes',
                f'{label} bytes received',
            ),
            'unit': 'Gbit/s',
        }
        # iperf3's TCP JSON includes retransmits, while its SCTP JSON does not
        # provide a portable retransmit counter across supported versions.
        if protocol == 'tcp':
            metrics['retransmits'] = _integer(
                sent,
                'retransmits',
                'TCP retransmit count',
                allow_zero=True,
            )
        return metrics

    # iperf3 3.11+ reports separate UDP sender and receiver summaries. The
    # client is the sender in our forward tests, so prefer the server-reported
    # receiver values (which include actual loss and jitter). ``sum`` remains
    # a compatibility fallback for older iperf3 output.
    summary = _mapping(
        end.get('sum_received') or end.get('sum'),
        'UDP receiver summary',
    )
    seconds = _number(summary, 'seconds', 'UDP duration')
    _validate_duration(seconds, expected_seconds)
    throughput = _number(
        summary,
        'bits_per_second',
        'UDP throughput',
    )
    packets = _integer(summary, 'packets', 'UDP packet count')
    lost = _integer(
        summary,
        'lost_packets',
        'UDP lost-packet count',
        allow_zero=True,
    )
    loss_percent = _number(
        summary,
        'lost_percent',
        'UDP packet-loss percentage',
        allow_zero=True,
    )
    if lost > packets or loss_percent > 100:
        raise ValueError('iperf3 reported inconsistent UDP packet loss.')
    return {
        'protocol': 'UDP',
        'parallel_streams': streams,
        'duration_seconds': round(seconds, 6),
        'throughput_gigabits_per_second': round(
            throughput / 1_000_000_000,
            6,
        ),
        'bytes_transferred': _integer(
            summary,
            'bytes',
            'UDP bytes transferred',
        ),
        'jitter_ms': round(
            _number(
                summary,
                'jitter_ms',
                'UDP jitter',
                allow_zero=True,
            ),
            6,
        ),
        'lost_packets': lost,
        'packets': packets,
        'packet_loss_percent': round(loss_percent, 6),
        'unit': 'Gbit/s',
    }


__all__ = ['SUPPORTED_PROTOCOLS', 'parse_output']
