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


class Iperf3UiTests(unittest.TestCase):
    def test_conditional_settings_fieldset_and_protocol_container_exist(self):
        parser = ElementIndex()
        parser.feed(INDEX)

        tag, attributes = parser.elements['iperf3Settings']
        self.assertEqual(tag, 'fieldset')
        self.assertIn('hidden', attributes)
        self.assertIn(
            'benchmark-settings',
            attributes.get('class', '').split(),
        )
        self.assertIn(
            'iperf3-settings',
            attributes.get('class', '').split(),
        )
        self.assertEqual(parser.elements['iperf3Protocols'][0], 'div')

    def test_javascript_renders_nested_protocols_with_tcp_default(self):
        render = javascript_function('renderCatalog')

        self.assertIn(
            "const iperf3Defaults = {protocols: ['tcp']};",
            JAVASCRIPT,
        )
        self.assertIn('catalog.iperf3_protocols || []', render)
        self.assertIn('data-kind="iperf3-protocol"', render)
        self.assertIn('setIperf3Values();', render)
        self.assertIn(
            'input[data-kind="bench"][value="iperf3"]',
            render,
        )
        self.assertIn(
            "addEventListener('change', toggleIperf3Settings)",
            render,
        )

    def test_toggle_hides_and_disables_the_conditional_fields(self):
        toggle = javascript_function('toggleIperf3Settings')

        self.assertIn('[value="iperf3"]', toggle)
        self.assertIn("$('#iperf3Settings').hidden = !selected", toggle)
        self.assertIn("$$('#iperf3Settings input')", toggle)
        self.assertIn('field.disabled = !selected', toggle)

    def test_payload_collects_checked_protocols_and_includes_iperf3(self):
        options = javascript_function('iperf3Options')
        submit_start = JAVASCRIPT.index(
            "$('#planForm').addEventListener('submit'"
        )
        submit = JAVASCRIPT[submit_start:JAVASCRIPT.index(
            'function activatePhase', submit_start
        )]

        self.assertIn(
            'input[data-kind="iperf3-protocol"]:checked',
            options,
        )
        self.assertIn('protocols: [...iperf3Defaults.protocols]', options)
        self.assertIn(
            "iperf3Options(selectedBenchmarks.includes('iperf3'))",
            submit,
        )
        self.assertRegex(submit, r'\n\s*iperf3,\n')

    def test_submit_requires_at_least_one_protocol(self):
        submit_start = JAVASCRIPT.index(
            "$('#planForm').addEventListener('submit'"
        )
        submit = JAVASCRIPT[submit_start:JAVASCRIPT.index(
            'function activatePhase', submit_start
        )]

        self.assertIn("selectedBenchmarks.includes('iperf3')", submit)
        self.assertIn('!iperf3.protocols.length', submit)
        self.assertIn('Select at least one iperf3 protocol.', submit)

    def test_reset_restores_tcp_default_and_visibility(self):
        reset = javascript_function('resetToPlan')

        self.assertIn("$('#planForm').reset()", reset)
        self.assertIn('setIperf3Values();', reset)
        self.assertIn('toggleIperf3Settings();', reset)

    def test_rerun_restores_saved_and_legacy_iperf3_selections(self):
        restore = javascript_function('restorePlan')
        from_plan = javascript_function('iperf3OptionsFromPlan')

        for legacy_id, protocol in (
            ('iperf_tcp', 'tcp'),
            ('iperf_udp', 'udp'),
            ('iperf_sctp', 'sctp'),
        ):
            self.assertIn(f'{legacy_id}: \'{protocol}\'', JAVASCRIPT)
        self.assertIn('legacyIperf3Protocols[benchmark]', from_plan)
        self.assertIn('plan.iperf3?.protocols', from_plan)
        self.assertIn('...savedProtocols', from_plan)
        self.assertIn('...legacyProtocols', from_plan)
        self.assertIn('hasLegacyIperf3', restore)
        self.assertIn(
            "input.value === 'iperf3' && hasLegacyIperf3",
            restore,
        )
        self.assertIn(
            'setIperf3Values(iperf3OptionsFromPlan(plan));',
            restore,
        )
        self.assertIn('toggleIperf3Settings();', restore)

    def test_index_references_updated_javascript_asset(self):
        scripts = re.findall(r'/static/app\.js\?v=(\d+)', INDEX)

        self.assertEqual(len(scripts), 1)
        self.assertGreaterEqual(int(scripts[0]), 14)


if __name__ == '__main__':
    unittest.main()
