import json
import shutil
import subprocess
import unittest
from html.parser import HTMLParser
from pathlib import Path

from app import main


ROOT = Path(__file__).resolve().parents[1]
COMPARISON_HTML_PATH = ROOT / 'app/static/comparison.html'
COMPARISON_HTML = COMPARISON_HTML_PATH.read_text()


class ElementIndex(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements = {}
        self.scripts = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        element_id = attributes.get('id')
        if element_id:
            self.elements[element_id] = (tag, attributes)
        if tag == 'script' and attributes.get('src'):
            self.scripts.append(attributes['src'])


class ComparisonPageUiTests(unittest.TestCase):
    def test_page_has_accessible_controls_charts_notes_and_exclusions(self):
        parser = ElementIndex()
        parser.feed(COMPARISON_HTML)

        expected = {
            'comparisonPageHeading': 'h1',
            'comparisonPageContext': 'p',
            'comparisonPageResult': 'select',
            'comparisonBaseline': 'select',
            'comparisonPageStatus': 'p',
            'comparisonPageWarnings': 'section',
            'comparisonPageWarningsHeading': 'h2',
            'comparisonPageWarningsList': 'ul',
            'comparisonRunLegend': 'section',
            'comparisonMetricIndex': 'nav',
            'comparisonCharts': 'section',
            'comparisonExcluded': 'section',
            'comparisonExcludedHeading': 'h2',
            'comparisonExcludedList': 'ul',
        }
        for element_id, tag in expected.items():
            with self.subTest(element_id=element_id):
                self.assertEqual(parser.elements[element_id][0], tag)

        self.assertEqual(
            parser.elements['comparisonPageHeading'][1].get('tabindex'),
            '-1',
        )
        status = parser.elements['comparisonPageStatus'][1]
        self.assertEqual(status.get('role'), 'status')
        self.assertEqual(status.get('aria-live'), 'polite')
        self.assertEqual(
            parser.elements['comparisonPageWarnings'][1].get(
                'aria-labelledby'
            ),
            'comparisonPageWarningsHeading',
        )
        self.assertEqual(
            parser.elements['comparisonExcluded'][1].get('aria-labelledby'),
            'comparisonExcludedHeading',
        )
        self.assertEqual(
            parser.elements['comparisonRunLegend'][1].get('aria-label'),
            'Compared runs',
        )
        self.assertEqual(
            parser.elements['comparisonMetricIndex'][1].get('aria-label'),
            'Metric index',
        )
        self.assertEqual(
            parser.elements['comparisonCharts'][1].get('aria-label'),
            'Benchmark metric charts',
        )
        for element_id in (
            'comparisonPageWarnings',
            'comparisonRunLegend',
            'comparisonMetricIndex',
            'comparisonExcluded',
        ):
            self.assertIn('hidden', parser.elements[element_id][1])
        for element_id in ('comparisonPageResult', 'comparisonBaseline'):
            self.assertIn('disabled', parser.elements[element_id][1])

    def test_shared_view_loads_before_page_controller_with_cache_versions(self):
        parser = ElementIndex()
        parser.feed(COMPARISON_HTML)
        self.assertEqual(
            parser.scripts,
            [
                '/static/comparison-view.js?v=1',
                '/static/comparison-page.js?v=1',
            ],
        )
        self.assertIn('/static/styles.css?v=19', COMPARISON_HTML)
        self.assertIn('All benchmark metrics.', COMPARISON_HTML)
        self.assertIn('Exact values and run details', (
            ROOT / 'app/static/comparison-page.js'
        ).read_text())
        self.assertIn(
            "window.addEventListener('hashchange', updateComparisonPageLinks)",
            (ROOT / 'app/static/comparison-page.js').read_text(),
        )

    def test_route_serves_comparison_shell_without_caching(self):
        route = next(
            route
            for route in main.app.routes
            if route.path == '/comparison' and 'GET' in route.methods
        )
        self.assertIs(route.endpoint, main.comparison_page)

        response = main.comparison_page()
        self.assertEqual(Path(response.path), COMPARISON_HTML_PATH)
        self.assertEqual(response.headers['cache-control'], 'no-store')

    def test_pure_frontend_helpers_preserve_comparison_semantics(self):
        if not shutil.which('node'):
            self.skipTest('Node.js is unavailable')
        script = r"""
const view = require('./app/static/comparison-view.js');
const page = require('./app/static/comparison-page.js');
const runs = ['aaaaaaaaaaaa', 'bbbbbbbbbbbb', 'cccccccccccc'];
const data = {
  charts: [
    {
      id: 'cpu:latency', result_id: 'cpu', result_name: 'CPU',
      metric_id: 'latency', label: 'Latency', unit: 'ms',
      direction: 'lower', fingerprint: 'cpu-contract',
      provenance_warnings: ['same warning', 'cpu only'],
      values: [
        {run_id: runs[2], value: 80, error_low: 75, error_high: 85},
        {run_id: runs[0], value: 100},
        {run_id: runs[1], value: null},
      ],
    },
    {
      id: 'cpu:throughput', result_id: 'cpu', result_name: 'CPU',
      metric_id: 'throughput', label: 'Throughput', unit: 'ops/s',
      direction: 'higher', fingerprint: 'cpu-contract',
      provenance_warnings: ['same warning'],
      values: [
        {run_id: runs[1], value: 125},
        {run_id: runs[0], value: 100},
      ],
    },
    {
      id: 'storage:iops', result_id: 'storage', result_name: 'Storage',
      metric_id: 'iops', direction: 'higher',
      provenance_warnings: ['storage only'],
      values: [{run_id: runs[0], value: 1000}],
    },
  ],
  excluded: [
    {run_id: runs[1], result_id: 'cpu', reason: 'CPU mismatch'},
    {run_id: runs[2], result_id: 'storage', reason: 'Storage mismatch'},
    {run_id: runs[0], reason: 'General mismatch'},
  ],
  groups: [
    {
      result_id: 'cpu', name: 'CPU', fingerprint: 'cpu-contract',
      comparable: true, run_ids: runs,
      metrics: [
        {
          key: 'latency', label: 'Latency', unit: 'ms',
          direction: 'lower_is_better', values: [
            {run_id: runs[2], value: 80},
            {run_id: runs[0], value: 100},
          ],
        },
        {
          key: 'throughput', label: 'Throughput', unit: 'ops/s',
          direction: 'higher_is_better', values: [
            {run_id: runs[1], value: 125},
            {run_id: runs[0], value: 100},
          ],
        },
        {
          key: 'temperature', label: 'Temperature', unit: 'C',
          direction: 'lower_is_better', values: [
            {run_id: runs[0], value: 72},
          ], missing_run_ids: [runs[1], runs[2]], comparable: false,
        },
      ],
    },
  ],
};
const parsed = page.parseComparisonState(
  `?compare=${runs.join(',')}&result=cpu&baseline=${runs[1]}&metric=cpu%3Alatency`
);
const legacyParsed = page.parseComparisonState(
  `?runs=${runs.slice(0, 2).join(',')}`
);
const cpuCharts = view.chartsForResult(data, 'cpu');
const allCpuCharts = view.allChartsForResult(data, 'cpu');
const ordered = view.orderedChartValues(cpuCharts[0], [runs[0], runs[1], runs[2]]);
process.stdout.write(JSON.stringify({
  parsed,
  legacyParsed,
  valid: page.validComparisonRunIds(runs),
  duplicateInvalid: page.validComparisonRunIds([runs[0], runs[0]]),
  uppercaseInvalid: page.validComparisonRunIds(['AAAAAAAAAAAA', runs[1]]),
  tooFewInvalid: page.validComparisonRunIds([runs[0]]),
  tooManyInvalid: page.validComparisonRunIds([...runs, ...[
    'dddddddddddd', 'eeeeeeeeeeee', 'ffffffffffff',
    '111111111111', '222222222222', '333333333333',
  ]]),
  cpuChartIds: cpuCharts.map(view.chartKey),
  allCpuChartCount: allCpuCharts.length,
  unavailableMetric: allCpuCharts[2].unavailable,
  orderedRunIds: ordered.map(value => value.run_id),
  representedRunIds: [...page.metricRunIds(cpuCharts)],
  coverage: page.metricCoverage(cpuCharts, runs),
  higherDelta: view.percentFromBaseline(125, 100, 'higher'),
  lowerDelta: view.percentFromBaseline(80, 100, 'lower'),
  lowerDeltaText: view.deltaText(80, 100, 'lower'),
  neutralDeltaText: view.deltaText(125, 100, 'neutral'),
  warnings: view.provenanceWarnings(cpuCharts),
  cpuExclusions: page.exclusionsForResult(data, 'cpu'),
  storageExclusions: page.exclusionsForResult(data, 'storage'),
  allExclusions: page.exclusionsForResult(data, ''),
  safeReturn: page.safeHistoryReturnPath('/history?provider=aws'),
  unsafeReturn: page.safeHistoryReturnPath('//example.com/history'),
}));
"""
        output = subprocess.check_output(
            ['node', '-e', script],
            cwd=ROOT,
            text=True,
        )
        values = json.loads(output)

        self.assertEqual(values['parsed'], {
            'runIds': [
                'aaaaaaaaaaaa',
                'bbbbbbbbbbbb',
                'cccccccccccc',
            ],
            'resultId': 'cpu',
            'baselineId': 'bbbbbbbbbbbb',
            'metricId': 'cpu:latency',
            'returnTo': '',
        })
        self.assertEqual(
            values['legacyParsed']['runIds'],
            ['aaaaaaaaaaaa', 'bbbbbbbbbbbb'],
        )
        self.assertTrue(values['valid'])
        self.assertFalse(values['duplicateInvalid'])
        self.assertFalse(values['uppercaseInvalid'])
        self.assertFalse(values['tooFewInvalid'])
        self.assertFalse(values['tooManyInvalid'])
        self.assertEqual(
            values['cpuChartIds'],
            ['cpu:latency', 'cpu:throughput'],
        )
        self.assertEqual(values['allCpuChartCount'], 3)
        self.assertTrue(values['unavailableMetric'])
        self.assertEqual(
            values['orderedRunIds'],
            ['aaaaaaaaaaaa', 'cccccccccccc'],
        )
        self.assertEqual(
            values['representedRunIds'],
            ['cccccccccccc', 'aaaaaaaaaaaa', 'bbbbbbbbbbbb'],
        )
        self.assertEqual(values['coverage'], {
            'aaaaaaaaaaaa': 2,
            'bbbbbbbbbbbb': 1,
            'cccccccccccc': 1,
        })
        self.assertEqual(values['higherDelta'], 25)
        self.assertEqual(values['lowerDelta'], 25)
        self.assertEqual(values['lowerDeltaText'], '+25.0%')
        self.assertEqual(values['neutralDeltaText'], '+25.0%')
        self.assertEqual(values['warnings'], ['same warning', 'cpu only'])
        self.assertEqual(
            [item['reason'] for item in values['cpuExclusions']],
            ['CPU mismatch'],
        )
        self.assertEqual(
            [item['reason'] for item in values['storageExclusions']],
            ['Storage mismatch'],
        )
        self.assertEqual(len(values['allExclusions']), 3)
        self.assertEqual(values['safeReturn'], '/history?provider=aws')
        self.assertEqual(values['unsafeReturn'], '')


if __name__ == '__main__':
    unittest.main()
