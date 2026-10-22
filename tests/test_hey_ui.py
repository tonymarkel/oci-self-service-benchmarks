import re
import unittest
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / 'app/static/index.html').read_text()
JAVASCRIPT = (ROOT / 'app/static/app.js').read_text()


class ElementIndex(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements = {}

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        element_id = attributes.get('id')
        if element_id:
            self.elements[element_id] = (tag, attributes)


def javascript_function(name):
    marker = f'function {name}('
    start = JAVASCRIPT.find(marker)
    if start < 0:
        raise AssertionError(f'JavaScript function {name!r} was not found')
    opening = JAVASCRIPT.find('{', start + len(marker))
    if opening < 0:
        raise AssertionError(f'JavaScript function {name!r} has no body')
    depth = 0
    for index in range(opening, len(JAVASCRIPT)):
        character = JAVASCRIPT[index]
        if character == '{':
            depth += 1
        elif character == '}':
            depth -= 1
            if depth == 0:
                return JAVASCRIPT[start:index + 1]
    raise AssertionError(f'JavaScript function {name!r} has no closing brace')


class HeyUiTests(unittest.TestCase):
    def test_conditional_settings_and_numeric_controls_exist(self):
        parser = ElementIndex()
        parser.feed(INDEX)

        tag, attributes = parser.elements['heySettings']
        self.assertEqual(tag, 'fieldset')
        self.assertIn('hidden', attributes)
        self.assertIn('benchmark-settings', attributes.get('class', '').split())
        self.assertIn('hey-settings', attributes.get('class', '').split())
        self.assertEqual(parser.elements['heyWorkloads'][0], 'div')
        expected = {
            'heyRequestCount': ('500000', '100', '1000000'),
            'heyConcurrency': ('100', '1', '10000'),
            'heyResponseSize': ('64', '1', '1024'),
            'heyWarmupRequests': ('10000', '0', '1000000'),
            'heyTrials': ('3', '1', '10'),
        }
        for element_id, (value, minimum, maximum) in expected.items():
            field_tag, field = parser.elements[element_id]
            self.assertEqual(field_tag, 'input')
            self.assertEqual(field['type'], 'number')
            self.assertEqual(field['value'], value)
            self.assertEqual(field['min'], minimum)
            self.assertEqual(field['max'], maximum)

    def test_catalog_renders_nested_workloads_with_both_defaults(self):
        render = javascript_function('renderCatalog')

        self.assertIn("workloads: ['new_connections', 'keep_alive']", JAVASCRIPT)
        self.assertIn('catalog.hey_workloads || []', render)
        self.assertIn('data-kind="hey-workload"', render)
        self.assertIn('setHeyValues();', render)
        self.assertIn('[value="hey"]', render)
        self.assertIn(
            "addEventListener('change', toggleHeySettings)",
            render,
        )

    def test_toggle_hides_and_disables_all_settings(self):
        toggle = javascript_function('toggleHeySettings')

        self.assertIn('[value="hey"]', toggle)
        self.assertIn("$('#heySettings').hidden = !selected", toggle)
        self.assertIn("$$('#heySettings input')", toggle)
        self.assertIn('field.disabled = !selected', toggle)

    def test_payload_collects_all_options_and_validates_selection(self):
        options = javascript_function('heyOptions')
        submit_start = JAVASCRIPT.index(
            "$('#planForm').addEventListener('submit'"
        )
        submit = JAVASCRIPT[submit_start:JAVASCRIPT.index(
            'function activatePhase', submit_start
        )]

        self.assertIn('input[data-kind="hey-workload"]:checked', options)
        for field in (
            'request_count',
            'concurrency',
            'response_size_kib',
            'warmup_requests',
            'trials',
        ):
            self.assertIn(f'{field}:', options)
        self.assertIn(
            "heyOptions(selectedBenchmarks.includes('hey'))",
            submit,
        )
        self.assertIn('!hey.workloads.length', submit)
        self.assertIn('Select at least one hey connection mode.', submit)
        self.assertIn(
            'hey.concurrency > hey.request_count',
            submit,
        )
        self.assertIn(
            'hey concurrency cannot exceed the measured request count.',
            submit,
        )
        self.assertRegex(submit, r'\n\s*hey,\n')

    def test_reset_and_rerun_restore_options_and_visibility(self):
        reset = javascript_function('resetToPlan')
        restore = javascript_function('restorePlan')
        from_plan = javascript_function('heyOptionsFromPlan')

        self.assertIn('setHeyValues();', reset)
        self.assertIn('toggleHeySettings();', reset)
        self.assertIn("includes('hey')", from_plan)
        self.assertIn('plan.hey || {}', from_plan)
        self.assertIn(
            'setHeyValues(heyOptionsFromPlan(plan));',
            restore,
        )
        self.assertIn('toggleHeySettings();', restore)

    def test_index_references_updated_cache_busted_assets(self):
        styles = re.findall(r'/static/styles\.css\?v=(\d+)', INDEX)
        scripts = re.findall(r'/static/app\.js\?v=(\d+)', INDEX)

        self.assertEqual(styles, ['20'])
        self.assertEqual(scripts, ['38'])


if __name__ == '__main__':
    unittest.main()
