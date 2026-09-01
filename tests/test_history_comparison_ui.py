import json
import shutil
import subprocess
import unittest
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HISTORY_HTML = (ROOT / 'app/static/history.html').read_text()
HISTORY_JS = (ROOT / 'app/static/history.js').read_text()
INDEX_HTML = (ROOT / 'app/static/index.html').read_text()
STYLES = (ROOT / 'app/static/styles.css').read_text()


class ElementIndex(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements = {}

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        element_id = attributes.get('id')
        if element_id:
            self.elements[element_id] = (tag, attributes)


class HistoryComparisonUiTests(unittest.TestCase):
    def test_history_has_filters_selection_tray_and_comparison_workspace(self):
        parser = ElementIndex()
        parser.feed(HISTORY_HTML)

        expected = {
            'historyFilters': 'form',
            'historySearch': 'input',
            'benchmarkFilter': 'select',
            'providerFilter': 'select',
            'completedFilter': 'input',
            'comparisonTray': 'aside',
            'selectionStatus': 'span',
            'compareSelected': 'button',
            'clearComparison': 'button',
            'comparisonWorkspace': 'section',
            'comparisonResult': 'select',
            'comparisonMetric': 'select',
            'comparisonStatus': 'p',
            'comparisonChart': 'div',
            'comparisonTable': 'div',
            'comparisonExcluded': 'section',
        }
        for element_id, tag in expected.items():
            with self.subTest(element_id=element_id):
                self.assertEqual(parser.elements[element_id][0], tag)

        self.assertEqual(
            parser.elements['comparisonStatus'][1].get('aria-live'),
            'polite',
        )
        self.assertIn('hidden', parser.elements['comparisonWorkspace'][1])
        self.assertIn('hidden', parser.elements['comparisonTray'][1])

    def test_selection_is_limited_to_structured_completed_runs_and_eight(self):
        self.assertIn('comparison_result_ids', HISTORY_JS)
        self.assertIn("run.benchmark_status === 'complete'", HISTORY_JS)
        self.assertIn('selectedRunIds.size >= 8', HISTORY_JS)
        self.assertIn('ids.length < 2 || ids.length > 8', HISTORY_JS)
        self.assertIn('Select 2–8 completed runs', HISTORY_JS)

    def test_filters_and_comparison_are_deep_linked(self):
        for parameter in ('q', 'benchmark', 'provider', 'completed', 'compare', 'result', 'metric'):
            self.assertIn(parameter, HISTORY_JS)
        self.assertIn("params.has('completed')", HISTORY_JS)
        self.assertIn("params.get('completed') === '1' : true", HISTORY_JS)
        self.assertIn("new URLSearchParams({runs: ids.join(',')})", HISTORY_JS)
        self.assertIn('/api/comparisons?', HISTORY_JS)
        self.assertIn("{cache: 'no-store'}", HISTORY_JS)
        self.assertIn('window.history.replaceState', HISTORY_JS)

    def test_chart_has_zero_baseline_uncertainty_and_exact_table(self):
        self.assertIn('const points = [0];', HISTORY_JS)
        self.assertIn('error_low', HISTORY_JS)
        self.assertIn('error_high', HISTORY_JS)
        self.assertIn("table.className = 'comparison-table'", HISTORY_JS)
        self.assertIn("caption.textContent = `${chart.result_name}: ${chart.label}.", HISTORY_JS)
        self.assertIn('Performance vs baseline', HISTORY_JS)
        self.assertIn('positive percentages indicate better performance', HISTORY_JS)
        self.assertIn("action('View', `/?report=", HISTORY_JS)
        self.assertIn('.comparison-bar-track::after', STYLES)
        self.assertIn('.comparison-error-bar', STYLES)

    def test_lower_is_better_delta_is_direction_aware(self):
        if not shutil.which('node'):
            self.skipTest('Node.js is unavailable')
        script = """
const ui = require('./app/static/history.js');
process.stdout.write(JSON.stringify({
  higher: ui.percentFromBaseline(120, 100, 'higher'),
  lower: ui.percentFromBaseline(80, 100, 'lower'),
  domain: ui.comparisonDomain([{value: 10, error_low: 8, error_high: 13}]),
}));
"""
        output = subprocess.check_output(
            ['node', '-e', script],
            cwd=ROOT,
            text=True,
        )
        values = json.loads(output)
        self.assertEqual(values['higher'], 20)
        self.assertEqual(values['lower'], 25)
        self.assertEqual(values['domain'], {'min': 0, 'max': 13, 'span': 13})

    def test_mismatch_explanations_and_mobile_layout_are_present(self):
        self.assertIn('function exclusionText(item)', HISTORY_JS)
        self.assertIn('item?.result_name || item?.result_id', HISTORY_JS)
        self.assertIn('No matching methodology and workload contract.', HISTORY_JS)
        self.assertIn('.comparison-excluded', STYLES)
        self.assertIn('.comparison-bar-row', STYLES)
        self.assertIn('@media(max-width:720px)', STYLES)

    def test_changed_assets_are_cache_busted_everywhere(self):
        self.assertIn('/static/history.js?v=10', HISTORY_HTML)
        self.assertIn('/static/styles.css?v=15', HISTORY_HTML)
        self.assertIn('/static/styles.css?v=15', INDEX_HTML)


if __name__ == '__main__':
    unittest.main()
