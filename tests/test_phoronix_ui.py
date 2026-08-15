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


class PhoronixUiTests(unittest.TestCase):
    def test_conditional_settings_fieldset_and_profile_container_exist(self):
        parser = ElementIndex()
        parser.feed(INDEX)

        tag, attributes = parser.elements['phoronixSettings']
        self.assertEqual(tag, 'fieldset')
        self.assertIn('hidden', attributes)
        self.assertIn('benchmark-settings', attributes.get('class', '').split())
        self.assertIn('phoronix-settings', attributes.get('class', '').split())
        self.assertEqual(parser.elements['phoronixProfiles'][0], 'div')

    def test_catalog_profiles_render_as_checkboxes_with_metadata(self):
        render = javascript_function('renderCatalog')
        metadata = javascript_function('phoronixProfileMetadata')

        self.assertIn(
            "const phoronixDefaults = {profiles: ['compress_7zip']};",
            JAVASCRIPT,
        )
        self.assertIn('catalog.phoronix_profiles || []', render)
        self.assertIn('data-kind="phoronix-profile"', render)
        self.assertIn('profile.description', render)
        self.assertIn('phoronixProfileMetadata(profile)', render)
        self.assertIn('profile.category', metadata)
        self.assertIn('profile.architectures', metadata)
        self.assertIn('profile.estimated_runtime_minutes', metadata)
        self.assertIn('profile.unit', metadata)
        self.assertIn('profile.direction', metadata)
        self.assertIn('profile.profile', metadata)
        self.assertIn('setPhoronixValues();', render)

    def test_toggle_hides_and_disables_the_conditional_fields(self):
        toggle = javascript_function('togglePhoronixSettings')
        render = javascript_function('renderCatalog')

        self.assertIn('[value="phoronix"]', toggle)
        self.assertIn("$('#phoronixSettings').hidden = !selected", toggle)
        self.assertIn("$$('#phoronixSettings input')", toggle)
        self.assertIn('field.disabled = !selected', toggle)
        self.assertIn(
            "addEventListener('change', togglePhoronixSettings)",
            render,
        )

    def test_payload_collects_profiles_and_requires_one_when_selected(self):
        options = javascript_function('phoronixOptions')
        submit_start = JAVASCRIPT.index(
            "$('#planForm').addEventListener('submit'"
        )
        submit = JAVASCRIPT[submit_start:JAVASCRIPT.index(
            'function activatePhase', submit_start
        )]

        self.assertIn(
            'input[data-kind="phoronix-profile"]:checked',
            options,
        )
        self.assertIn(
            "phoronixOptions(selectedBenchmarks.includes('phoronix'))",
            submit,
        )
        self.assertIn("!phoronix.profiles.length", submit)
        self.assertIn('Select at least one Phoronix test profile.', submit)
        self.assertRegex(submit, r'\n\s*phoronix,\n')

    def test_reset_restores_seven_zip_default_and_visibility(self):
        reset = javascript_function('resetToPlan')

        self.assertIn("$('#planForm').reset()", reset)
        self.assertIn('setPhoronixValues();', reset)
        self.assertIn('togglePhoronixSettings();', reset)

    def test_rerun_restores_saved_profiles_and_legacy_default(self):
        restore = javascript_function('restorePlan')
        from_plan = javascript_function('phoronixOptionsFromPlan')

        self.assertIn("includes('phoronix')", from_plan)
        self.assertIn('plan.phoronix?.profiles', from_plan)
        self.assertIn(
            'profiles.length ? profiles : [...phoronixDefaults.profiles]',
            from_plan,
        )
        self.assertIn(
            'setPhoronixValues(phoronixOptionsFromPlan(plan));',
            restore,
        )
        self.assertIn('togglePhoronixSettings();', restore)

    def test_index_references_updated_javascript_asset(self):
        scripts = re.findall(r'/static/app\.js\?v=(\d+)', INDEX)

        self.assertEqual(len(scripts), 1)
        self.assertGreaterEqual(int(scripts[0]), 16)


if __name__ == '__main__':
    unittest.main()
