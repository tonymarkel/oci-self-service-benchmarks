import json
import shutil
import subprocess
import unittest
from html.parser import HTMLParser
from pathlib import Path

from app.catalog import DEATHSTARBENCH_TOPOLOGIES
from app.deathstarbench_contract import (
    DISTRIBUTED_TIERED_PROFILE,
    SINGLE_HOST_PROFILE,
)


ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / 'app/static/index.html').read_text()
JAVASCRIPT = (ROOT / 'app/static/app.js').read_text()
HISTORY = (ROOT / 'app/static/history.js').read_text()
HISTORY_HTML = (ROOT / 'app/static/history.html').read_text()


class ElementIndex(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements = {}

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        element_id = attributes.get('id')
        if element_id:
            self.elements[element_id] = (tag, attributes)


class DeathStarBenchPublicUiTests(unittest.TestCase):
    def test_catalog_topologies_follow_runtime_release_contract(self):
        entries = {
            (item['topology_id'], item['runtime_id']): item
            for item in DEATHSTARBENCH_TOPOLOGIES
        }
        compact = entries[(
            SINGLE_HOST_PROFILE.topology_id,
            SINGLE_HOST_PROFILE.runtime_id,
        )]
        distributed = entries[(
            DISTRIBUTED_TIERED_PROFILE.topology_id,
            DISTRIBUTED_TIERED_PROFILE.runtime_id,
        )]

        self.assertEqual(compact['released'], SINGLE_HOST_PROFILE.released)
        self.assertEqual(
            distributed['released'],
            DISTRIBUTED_TIERED_PROFILE.released,
        )
        self.assertEqual(compact['node_count'], 2)
        self.assertEqual(distributed['node_count'], 5)
        self.assertEqual(distributed['workloads'], ['social_network'])
        self.assertEqual(
            distributed['roles'],
            ['control', 'database', 'cache', 'application', 'load_generator'],
        )

    def test_plan_contains_topology_warning_and_oci_defined_tag_controls(self):
        parser = ElementIndex()
        parser.feed(INDEX)

        self.assertEqual(parser.elements['deathstarTopology'][0], 'select')
        self.assertIn('required', parser.elements['deathstarTopology'][1])
        self.assertEqual(
            parser.elements['deathstarTopologyDescription'][0],
            'small',
        )
        self.assertIn(
            'hidden',
            parser.elements['deathstarDistributedWarning'][1],
        )
        self.assertIn('Five-VM deployment', INDEX)
        self.assertIn('dedicated', INDEX)
        self.assertIn('database storage', INDEX)
        self.assertIn('increasing cloud cost', INDEX)
        self.assertEqual(parser.elements['ociDefinedTags'][0], 'textarea')
        self.assertIn('hidden', parser.elements['ociDefinedTagsField'][1])

    def test_only_released_catalog_topologies_are_rendered(self):
        self.assertIn(
            'catalog.deathstarbench_topologies',
            JAVASCRIPT,
        )
        self.assertIn(
            'topologyCatalog.filter(topology => topology.released === true)',
            JAVASCRIPT,
        )
        self.assertIn('COMPACT_DEATHSTAR_TOPOLOGY_ID', JAVASCRIPT)
        self.assertIn('DISTRIBUTED_DEATHSTAR_TOPOLOGY_ID', JAVASCRIPT)

    def test_distributed_selection_enforces_exclusive_social_network_plan(self):
        self.assertIn("input.value === 'deathstarbench'", JAVASCRIPT)
        self.assertIn('supportedLlmBenchmarks.has(input.value) && !distributed', JAVASCRIPT)
        self.assertIn("$('#deathstarWorkload').value = 'social_network'", JAVASCRIPT)
        self.assertIn("if (newlySelected) $('#deathstarConnections').value = '4'", JAVASCRIPT)
        self.assertIn("$('#additional').checked = false", JAVASCRIPT)
        self.assertIn("$('#additional').disabled = distributed", JAVASCRIPT)
        self.assertIn("field.id === 'deathstarWorkload'", JAVASCRIPT)
        self.assertIn('Application ${baseShapeLabel}', JAVASCRIPT)
        self.assertIn("distributed && currentProvider() === 'oci'", JAVASCRIPT)

    def test_payload_and_rerun_preserve_topology_and_defined_tags(self):
        self.assertIn('topology_id: topology.topology_id', JAVASCRIPT)
        self.assertIn('runtime_id: topology.runtime_id', JAVASCRIPT)
        self.assertIn('oci_defined_tags: ociDefinedTags', JAVASCRIPT)
        self.assertIn('tags = JSON.parse(source)', JAVASCRIPT)
        self.assertIn('OCI defined tags must be valid JSON.', JAVASCRIPT)
        self.assertIn(
            'options.topology_id || COMPACT_DEATHSTAR_TOPOLOGY_ID',
            JAVASCRIPT,
        )
        self.assertIn(
            "JSON.stringify(plan.oci_defined_tags || {}, null, 2)",
            JAVASCRIPT,
        )

    def test_provider_copy_tracks_distributed_guest_and_network_contract(self):
        self.assertIn('Distributed AWS runs use approved Rocky Linux 9 images.', JAVASCRIPT)
        self.assertIn('connect as rocky', JAVASCRIPT)
        self.assertIn('application, database, cache, and control VMs with K3s', JAVASCRIPT)
        self.assertIn('A fifth load-generator VM', JAVASCRIPT)

    def test_history_labels_and_searches_topology(self):
        self.assertIn('function deathstarTopologyIdFor(run)', HISTORY)
        self.assertIn('function deathstarTopologyLabelFor(run)', HISTORY)
        self.assertIn("addMeta(meta, 'Topology', topologyLabel)", HISTORY)
        self.assertIn('deathstarTopologyIdFor(run)', HISTORY)
        self.assertIn('deathstarTopologyLabelFor(run)', HISTORY)

        if not shutil.which('node'):
            return
        script = """
const ui = require('./app/static/history.js');
const distributed = {
  id: 'distributed-run',
  benchmarks: ['DeathStarBench — Social Network'],
  plan: {deathstarbench: {topology_id: 'distributed_tiered_v1'}},
};
const legacy = {
  id: 'legacy-run',
  benchmarks: ['DeathStarBench — Media Microservices'],
};
const legacyPlan = {
  id: 'legacy-plan-run',
  plan: {
    benchmarks: ['deathstarbench'],
    deathstarbench: {topology_id: 'single_host_v1'},
  },
};
const legacyResult = {
  id: 'legacy-result-run',
  comparison_result_ids: ['deathstarbench'],
  deathstarbench: {topology_id: 'single_host_v1'},
};
const unrelated = {
  id: 'fio-run',
  benchmarks: ['fio storage suite'],
  comparison_result_ids: ['fio'],
  deathstarbench: {topology_id: 'single_host_v1'},
  plan: {
    benchmarks: ['fio'],
    deathstarbench: {topology_id: 'single_host_v1'},
  },
};
const filters = {search: 'k3s', provider: '', benchmark: '', completed: false};
process.stdout.write(JSON.stringify({
  distributedId: ui.deathstarTopologyIdFor(distributed),
  distributedLabel: ui.deathstarTopologyLabelFor(distributed),
  legacyLabel: ui.deathstarTopologyLabelFor(legacy),
  legacyPlanLabel: ui.deathstarTopologyLabelFor(legacyPlan),
  legacyResultLabel: ui.deathstarTopologyLabelFor(legacyResult),
  unrelatedId: ui.deathstarTopologyIdFor(unrelated),
  unrelatedLabel: ui.deathstarTopologyLabelFor(unrelated),
  searchable: ui.matchesFilters(distributed, filters),
}));
"""
        values = json.loads(subprocess.check_output(
            ['node', '-e', script],
            cwd=ROOT,
            text=True,
        ))
        self.assertEqual(values['distributedId'], 'distributed_tiered_v1')
        self.assertEqual(
            values['distributedLabel'],
            'Distributed tiered / K3s (5 VMs)',
        )
        self.assertEqual(values['legacyLabel'], 'Compact / single-host')
        self.assertEqual(values['legacyPlanLabel'], 'Compact / single-host')
        self.assertEqual(values['legacyResultLabel'], 'Compact / single-host')
        self.assertIsNone(values['unrelatedId'])
        self.assertIsNone(values['unrelatedLabel'])
        self.assertTrue(values['searchable'])

    def test_changed_scripts_are_cache_busted(self):
        self.assertIn('/static/app.js?v=35', INDEX)
        self.assertIn('/static/history.js?v=18', HISTORY_HTML)


if __name__ == '__main__':
    unittest.main()
