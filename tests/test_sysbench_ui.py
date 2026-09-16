import re
import unittest
from html.parser import HTMLParser
from pathlib import Path

from app.catalog import BENCHMARKS, SYSBENCH_WORKLOADS


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


class SysbenchUiTests(unittest.TestCase):
    def test_conditional_settings_fieldset_and_workload_container_exist(self):
        parser = ElementIndex()
        parser.feed(INDEX)

        tag, attributes = parser.elements['sysbenchSettings']
        self.assertEqual(tag, 'fieldset')
        self.assertIn('hidden', attributes)
        self.assertIn(
            'benchmark-settings',
            attributes.get('class', '').split(),
        )
        self.assertIn(
            'sysbench-settings',
            attributes.get('class', '').split(),
        )
        self.assertEqual(parser.elements['sysbenchWorkloads'][0], 'div')

    def test_catalog_has_one_parent_and_no_legacy_benchmark_entries(self):
        benchmark_ids = [item['id'] for item in BENCHMARKS]

        self.assertEqual(benchmark_ids.count('sysbench'), 1)
        self.assertTrue(
            {'sysbench_cpu', 'sysbench_memory', 'sysbench_fileio'}
            .isdisjoint(benchmark_ids)
        )
        self.assertEqual(
            [item['id'] for item in SYSBENCH_WORKLOADS],
            ['cpu', 'memory', 'fileio'],
        )

    def test_storage_copy_explains_local_nvme_preference_and_fallback(self):
        fio = next(item for item in BENCHMARKS if item['id'] == 'fio')
        fileio = next(
            item for item in SYSBENCH_WORKLOADS
            if item['id'] == 'fileio'
        )

        for description in (fio['description'], fileio['description']):
            self.assertIn('provider-verified instance-local NVMe', description)
            self.assertIn('/data', description)
            self.assertIn('ephemeral', description)
        self.assertIn('/benchmark-local', INDEX)
        self.assertIn(
            'always require and provision the additional /data',
            INDEX,
        )
        self.assertIn(
            'current GCP catalog excludes Local SSD and -lssd machine types',
            INDEX,
        )

    def test_javascript_renders_nested_workloads_with_cpu_default(self):
        render = javascript_function('renderCatalog')

        self.assertIn(
            "const sysbenchDefaults = { workloads: ['cpu'] };",
            JAVASCRIPT,
        )
        self.assertIn('catalog.sysbench_workloads || []', render)
        self.assertIn('data-kind="sysbench-workload"', render)
        self.assertIn('setSysbenchValues();', render)
        self.assertIn(
            "input[data-kind=\"bench\"][value=\"sysbench\"]",
            render,
        )
        self.assertIn(
            "addEventListener('change', toggleSysbenchSettings)",
            render,
        )

    def test_toggle_hides_and_disables_the_conditional_fields(self):
        toggle = javascript_function('toggleSysbenchSettings')

        self.assertIn("[value=\"sysbench\"]", toggle)
        self.assertIn("$('#sysbenchSettings').hidden = !selected", toggle)
        self.assertIn("$$('#sysbenchSettings input')", toggle)
        self.assertIn('field.disabled = !selected', toggle)

    def test_payload_collects_checked_workloads_and_includes_sysbench(self):
        options = javascript_function('sysbenchOptions')
        submit_start = JAVASCRIPT.index(
            "$('#planForm').addEventListener('submit'"
        )
        submit = JAVASCRIPT[submit_start:JAVASCRIPT.index(
            'function activatePhase', submit_start
        )]

        self.assertIn(
            'input[data-kind="sysbench-workload"]:checked',
            options,
        )
        self.assertIn('workloads: [...sysbenchDefaults.workloads]', options)
        self.assertIn(
            "sysbenchOptions(selectedBenchmarks.includes('sysbench'))",
            submit,
        )
        self.assertRegex(submit, r'\n\s*sysbench,\n')

    def test_submit_validates_workload_selection_and_fileio_volume(self):
        submit_start = JAVASCRIPT.index(
            "$('#planForm').addEventListener('submit'"
        )
        submit = JAVASCRIPT[submit_start:JAVASCRIPT.index(
            'function activatePhase', submit_start
        )]

        self.assertIn("!sysbench.workloads.length", submit)
        self.assertIn('Select at least one Sysbench workload.', submit)
        self.assertIn("sysbench.workloads.includes('fileio')", submit)
        self.assertIn("!$('#additional').checked", submit)
        self.assertIn(
            'Sysbench file I/O requires the additional /data volume.',
            submit,
        )

    def test_reset_restores_defaults_and_visibility(self):
        reset = javascript_function('resetToPlan')

        self.assertIn("$('#planForm').reset()", reset)
        self.assertIn('setSysbenchValues();', reset)
        self.assertIn('toggleSysbenchSettings();', reset)

    def test_rerun_restores_saved_and_legacy_sysbench_selections(self):
        restore = javascript_function('restorePlan')
        from_plan = javascript_function('sysbenchOptionsFromPlan')

        for legacy_id, workload in (
            ('sysbench_cpu', 'cpu'),
            ('sysbench_memory', 'memory'),
            ('sysbench_fileio', 'fileio'),
        ):
            self.assertIn(f'{legacy_id}: \'{workload}\'', JAVASCRIPT)
        self.assertIn('legacySysbenchWorkloads[benchmark]', from_plan)
        self.assertIn('...(plan.sysbench?.workloads || [])', from_plan)
        self.assertIn('...legacyWorkloads', from_plan)
        self.assertIn('hasLegacySysbench', restore)
        self.assertIn(
            "input.value === 'sysbench' && hasLegacySysbench",
            restore,
        )
        self.assertIn(
            'setSysbenchValues(sysbenchOptionsFromPlan(plan));',
            restore,
        )
        self.assertIn('toggleSysbenchSettings();', restore)

    def test_index_references_updated_cache_busted_assets(self):
        styles = re.findall(r'/static/styles\.css\?v=(\d+)', INDEX)
        scripts = re.findall(r'/static/app\.js\?v=(\d+)', INDEX)

        self.assertEqual(len(styles), 1)
        self.assertEqual(len(scripts), 1)
        self.assertGreaterEqual(int(styles[0]), 10)
        self.assertGreaterEqual(int(scripts[0]), 13)


if __name__ == '__main__':
    unittest.main()
