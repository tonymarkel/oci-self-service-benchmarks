"""Execute the actual plan UI helpers without a browser or cloud resources."""

import json
import re
import shutil
import subprocess
import unittest
from pathlib import Path

from app.catalog import DEATHSTARBENCH_TOPOLOGIES
from app.models import BenchmarkPlan
from app.providers.registry import providers


ROOT = Path(__file__).resolve().parents[1]
JAVASCRIPT = (ROOT / 'app/static/app.js').read_text()
NODE = shutil.which('node')


def ui_function(name):
    """Read a complete top-level function, preserving its production body."""

    match = re.search(rf'^(?:async )?function {name}\(', JAVASCRIPT, re.M)
    if match is None:
        raise AssertionError(f'Missing production UI function: {name}')
    end = JAVASCRIPT.index('\n}\n', match.start()) + len('\n}')
    return JAVASCRIPT[match.start():end]


UI_FUNCTIONS = (
    'currentProvider',
    'providerCapabilitySet',
    'setProviderOnlyElement',
    'applyProviderBenchmarkSupport',
    'setSysbenchValues',
    'toggleSysbenchSettings',
    'setIperf3Values',
    'toggleIperf3Settings',
    'setPhoronixValues',
    'togglePhoronixSettings',
    'updateDeathstarWorkloadDescription',
    'selectableDeathstarTopologies',
    'selectedDeathstarTopology',
    'isDistributedDeathstarbenchSelected',
    'enforceDeathstarTopologyValues',
    'updateDeathstarTopologyPresentation',
    'handleDeathstarSelectionChange',
    'setDeathstarValues',
    'restoreDeathstarSelection',
    'toggleDeathstarSettings',
    'setApachebenchValues',
    'toggleApachebenchSettings',
    'deathstarOptions',
    'sysbenchOptionsFromPlan',
    'iperf3OptionsFromPlan',
    'phoronixOptionsFromPlan',
    'apachebenchOptionsFromPlan',
    'restorePlan',
)


FAKE_DOM = r"""
const elements = [];
const ids = new Map();
class FakeElement {
    constructor(id, {tag = 'input', kind = '', name = '', value = '', containers = []} = {}) {
        this.id = id;
        this.tag = tag;
        this.kind = kind;
        this.name = name;
        this.value = value;
        this.containers = containers;
        this.checked = false;
        this.disabled = false;
        this.hidden = true;
        this.dataset = {};
        this.textContent = '';
        this.classes = new Set();
        this.label = {classList: {toggle: (name, enabled) => {
            if (enabled) this.classes.add(name);
            else this.classes.delete(name);
        }}};
    }
    set value(value) { this._value = String(value); }
    get value() { return this._value; }
    closest() { return this.label; }
    querySelectorAll(selector) {
        return elements.filter(element => element.containers.includes(this.id) && matches(element, selector));
    }
    dispatchEvent() {}
}
function add(id, options = {}) {
    const element = new FakeElement(id, options);
    elements.push(element);
    ids.set(id, element);
    return element;
}
function matches(element, selector) {
    const tag = selector.match(/^(input|select|textarea)/)?.[1];
    if (tag && element.tag !== tag) return false;
    for (const match of selector.matchAll(/\[([\w-]+)=(?:"([^"]*)"|'([^']*)'|([^\]]+))\]/g)) {
        const value = match[2] ?? match[3] ?? match[4];
        const actual = match[1] === 'data-kind' ? element.kind : element[match[1]];
        if (actual !== value) return false;
    }
    if (selector.includes(':checked') && !element.checked) return false;
    if (selector.includes(':not(:disabled)') && element.disabled) return false;
    return true;
}
const document = {
    querySelectorAll(selector) {
        return [...new Set(selector.split(',').flatMap(part => {
            const piece = part.trim();
            if (!piece.startsWith('#')) return elements.filter(element => matches(element, piece));
            const [, id, descendants] = piece.match(/^#([\w-]+)(?:\s+(.*))?$/);
            if (!descendants) return ids.has(id) ? [ids.get(id)] : [];
            return elements.filter(element => element.containers.includes(id) && matches(element, descendants));
        }))];
    },
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; },
};
for (const id of [
    'provider', 'additional', 'deathstarSettings', 'deathstarTopologyDescription',
    'deathstarWorkloadDescription', 'deathstarAvailabilityWarning',
    'deathstarDistributedWarning', 'deathstarLegacyPlanWarning',
    'ociDefinedTagsField', 'sysbenchSettings', 'iperf3Settings',
    'phoronixSettings', 'apachebenchSettings', 'awsProfile', 'gcpProject',
    'azureSubscription', 'region', 'compartment', 'ad', 'fd', 'gcpZone',
    'azureZone', 'shape', 'ocpus', 'memory', 'secureBoot', 'measuredBoot',
    'tpm', 'shielded', 'bootSize', 'bootPerf', 'dataSize', 'dataPerf', 'mount',
    'destroy', 'key', 'publicKey', 'keyPassphrase', 'keyFile', 'publicKeyFile',
]) add(id);
add('ociDefinedTags', {tag: 'textarea', containers: ['ociDefinedTagsField']});
for (const id of ['deathstarTopology', 'deathstarWorkload']) {
    add(id, {tag: 'select', containers: ['deathstarSettings']});
}
for (const id of [
    'deathstarWarmup', 'deathstarDuration', 'deathstarThreads',
    'deathstarConnections', 'deathstarRequestRate',
]) add(id, {containers: ['deathstarSettings']});
for (const value of ['deathstarbench', 'fio', 'sysbench', 'iperf3', 'phoronix', 'apachebench', 'stream']) {
    add(`bench-${value}`, {kind: 'bench', value});
}
add('llm-llama_bench', {kind: 'llm', value: 'llama_bench'});
for (const value of ['cpu', 'memory', 'fileio']) {
    add(`sysbench-${value}`, {kind: 'sysbench-workload', value,
        containers: ['sysbenchWorkloads', 'sysbenchSettings']});
}
for (const value of ['tcp', 'udp', 'sctp']) {
    add(`iperf3-${value}`, {kind: 'iperf3-protocol', value,
        containers: ['iperf3Protocols', 'iperf3Settings']});
}
add('phoronix-compress_7zip', {kind: 'phoronix-profile', value: 'compress_7zip',
    containers: ['phoronixProfiles', 'phoronixSettings']});
for (const value of ['new_connections', 'keep_alive']) {
    add(`apachebench-${value}`, {kind: 'apachebench-workload', value,
        containers: ['apachebenchWorkloads', 'apachebenchSettings']});
}
for (const id of [
    'apachebenchRequestCount', 'apachebenchConcurrency', 'apachebenchResponseSize',
    'apachebenchWarmupRequests', 'apachebenchTrials',
]) add(id, {containers: ['apachebenchSettings']});
for (const value of ['none', 'shielded', 'confidential']) add(`security-${value}`, {name: 'security', value});
for (const value of ['paravirtualized', 'sriov']) add(`networking-${value}`, {name: 'networking', value});
// Discovery and unrelated explanatory copy are outside this local form test.
async function loadProvider() {}
async function placement() {}
async function loadShapes() {}
function faultDomains() {}
function applySshDefaults() {}
function updateProviderTopologyCopy() {}
"""


def ordinary_plan(provider, **overrides):
    payload = {
        'provider': provider,
        'region': {
            'oci': 'us-ashburn-1', 'aws': 'us-east-1',
            'gcp': 'us-east1', 'azure': 'eastus',
        }[provider],
        'shape': {
            'oci': 'VM.Standard.E5.Flex', 'aws': 'm7i.2xlarge',
            'gcp': 'n2-standard-8', 'azure': 'Standard_D8as_v7',
        }[provider],
        'memory_gb': 32,
        'ssh_private_key': 'test-private-key',
        'ssh_public_key': 'ssh-rsa test-public-key',
        'gcp_project_id': 'test-project',
        'gcp_zone': 'us-east1-b',
        'azure_subscription_id': 'test-subscription',
        'azure_zone': '1',
        'benchmarks': ['fio'],
        'storage': {'additional_volume': True},
    }
    payload.update(overrides)
    return payload


@unittest.skipUnless(NODE, 'Node.js is required for executable UI regressions')
class DeathStarBenchDistributedOnlyUiTests(unittest.TestCase):
    def run_ui(self, scenario, **fixtures):
        topology = next(
            dict(item, released=True)
            for item in DEATHSTARBENCH_TOPOLOGIES
            if item['topology_id'] == 'distributed_tiered_v1'
        )
        script = '\n'.join([
            FAKE_DOM,
            JAVASCRIPT[:JAVASCRIPT.index('\nasync function api(')],
            *(ui_function(name) for name in UI_FUNCTIONS),
            f'const topologyFixture = {json.dumps(topology)};',
            f'const fixtures = {json.dumps(fixtures)};',
            'deathstarTopologies = [topologyFixture];',
            "deathstarWorkloads = [{id: 'social_network', description: 'Social Network'}];",
            'providerDefinitions = new Map(' + json.dumps([
                [provider.id, provider.as_dict()] for provider in providers()
            ]) + ');',
            "$('#provider').value = 'oci';",
            'setDeathstarValues();',
            'async function scenario() {\n' + scenario + '\n}',
            'scenario().then(result => process.stdout.write(JSON.stringify(result)))',
            '.catch(error => { console.error(error.stack); process.exitCode = 1; });',
        ])
        result = subprocess.run(
            [NODE, '-'], input=script, cwd=ROOT, text=True,
            capture_output=True, timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_catalog_accepts_only_released_exact_five_vm_social_network_profile(self):
        result = self.run_ui(r"""
const invalid = [
    null, 5, 'distributed_tiered_v1',
    {...topologyFixture, released: false},
    {...topologyFixture, released: 'true'},
    {...topologyFixture, topology_id: 'single_host_v1', runtime_id: 'podman_compose_v1'},
    {...topologyFixture, topology_id: 'distributed_tiered_v2'},
    {...topologyFixture, runtime_id: 'podman_compose_v1'},
    {...topologyFixture, runtime_id: 'k3s_v2'},
    {...topologyFixture, node_count: 2},
    {...topologyFixture, node_count: '5'},
    {...topologyFixture, workloads: ['media_microservices']},
    {...topologyFixture, workloads: 'social_network'},
    {...topologyFixture, workloads: null},
];
return {
    selected: selectableDeathstarTopologies({deathstarbench_topologies: [...invalid, topologyFixture]}),
    unavailable: [{}, {deathstarbench_topologies: null}, {deathstarbench_topologies: {}},
        {deathstarbench_topologies: invalid}].map(selectableDeathstarTopologies),
};
""")
        self.assertEqual(len(result['selected']), 1)
        selected = result['selected'][0]
        self.assertEqual(selected['topology_id'], 'distributed_tiered_v1')
        self.assertEqual(selected['runtime_id'], 'k3s_v1')
        self.assertEqual(selected['node_count'], 5)
        self.assertEqual(selected['workloads'], ['social_network'])
        self.assertEqual(result['unavailable'], [[], [], [], []])

    def test_new_selection_is_exclusive_warns_and_restores_previous_data_volume(self):
        results = self.run_ui(r"""
const results = [];
for (const provider of providerDefinitions.keys()) {
    for (const previousAdditional of [true, false]) {
        $('#provider').value = provider;
        $('#additional').checked = previousAdditional;
        $('#deathstarConnections').value = '64';
        $('#deathstarLegacyPlanWarning').hidden = false;
        ids.get('bench-fio').checked = true;
        ids.get('llm-llama_bench').checked = true;
        ids.get('bench-deathstarbench').checked = true;
        handleDeathstarSelectionChange();
        const selected = {
            provider, previousAdditional, options: deathstarOptions(true),
            nodeCount: selectedDeathstarTopology().node_count,
            benchmarks: $$('input[data-kind="bench"]:checked').map(input => input.value),
            otherDisabled: ids.get('bench-fio').disabled && ids.get('llm-llama_bench').disabled,
            llmChecked: ids.get('llm-llama_bench').checked,
            additional: $('#additional').checked, additionalDisabled: $('#additional').disabled,
            settingsHidden: $('#deathstarSettings').hidden,
            durationDisabled: $('#deathstarDuration').disabled,
            workloadDisabled: $('#deathstarWorkload').disabled,
            costWarningHidden: $('#deathstarDistributedWarning').hidden,
            legacyWarningHidden: $('#deathstarLegacyPlanWarning').hidden,
            tagsHidden: $('#ociDefinedTagsField').hidden,
        };
        ids.get('bench-deathstarbench').checked = false;
        handleDeathstarSelectionChange();
        selected.deselected = {
            additional: $('#additional').checked, additionalDisabled: $('#additional').disabled,
            otherDisabled: ids.get('bench-fio').disabled || ids.get('llm-llama_bench').disabled,
            settingsHidden: $('#deathstarSettings').hidden,
            disabledFields: $$('#deathstarSettings input, #deathstarSettings select').every(field => field.disabled),
            costWarningHidden: $('#deathstarDistributedWarning').hidden,
            savedVolumeOverride: $('#additional').dataset.deathstarPreviousAdditional,
        };
        results.push(selected);
    }
}
return results;
""")
        self.assertEqual(len(results), 8)
        for result in results:
            with self.subTest(provider=result['provider'], volume=result['previousAdditional']):
                self.assertEqual(result['nodeCount'], 5)
                self.assertEqual(result['benchmarks'], ['deathstarbench'])
                self.assertEqual(result['options']['workload'], 'social_network')
                self.assertEqual(result['options']['connections'], 4)
                self.assertTrue(result['otherDisabled'])
                self.assertFalse(result['llmChecked'])
                self.assertFalse(result['additional'])
                self.assertTrue(result['additionalDisabled'])
                self.assertFalse(result['settingsHidden'])
                self.assertFalse(result['durationDisabled'])
                self.assertTrue(result['workloadDisabled'])
                self.assertFalse(result['costWarningHidden'])
                self.assertTrue(result['legacyWarningHidden'])
                self.assertEqual(result['tagsHidden'], result['provider'] != 'oci')
                deselected = result['deselected']
                self.assertEqual(deselected['additional'], result['previousAdditional'])
                self.assertFalse(deselected['additionalDisabled'])
                self.assertFalse(deselected['otherDisabled'])
                self.assertTrue(deselected['settingsHidden'])
                self.assertTrue(deselected['disabledFields'])
                self.assertTrue(deselected['costWarningHidden'])
                self.assertNotIn('savedVolumeOverride', deselected)

    def test_unavailable_release_disables_only_deathstarbench(self):
        results = self.run_ui(r"""
deathstarTopologies = [];
$('#deathstarTopology').value = '';
return [...providerDefinitions.keys()].map(provider => {
    $('#provider').value = provider;
    ids.get('bench-deathstarbench').checked = true;
    ids.get('bench-fio').checked = true;
    ids.get('llm-llama_bench').checked = true;
    $('#additional').checked = true;
    applyProviderBenchmarkSupport();
    let rejected = false;
    try { deathstarOptions(true); } catch (error) { rejected = error.message.includes('unavailable'); }
    return {
        provider, rejected,
        dsbChecked: ids.get('bench-deathstarbench').checked,
        dsbDisabled: ids.get('bench-deathstarbench').disabled,
        fioChecked: ids.get('bench-fio').checked, fioDisabled: ids.get('bench-fio').disabled,
        llmChecked: ids.get('llm-llama_bench').checked, llmDisabled: ids.get('llm-llama_bench').disabled,
        additional: $('#additional').checked, additionalDisabled: $('#additional').disabled,
        unavailableHidden: $('#deathstarAvailabilityWarning').hidden,
        payload: JSON.parse(JSON.stringify({deathstarbench: deathstarOptions(false)})),
    };
});
""")
        for result in results:
            with self.subTest(provider=result['provider']):
                self.assertTrue(result['rejected'])
                self.assertFalse(result['dsbChecked'])
                self.assertTrue(result['dsbDisabled'])
                self.assertTrue(result['fioChecked'])
                self.assertFalse(result['fioDisabled'])
                self.assertTrue(result['llmChecked'])
                self.assertFalse(result['llmDisabled'])
                self.assertTrue(result['additional'])
                self.assertFalse(result['additionalDisabled'])
                self.assertFalse(result['unavailableHidden'])
                self.assertEqual(result['payload'], {})

    def test_inactive_options_are_omitted_and_ordinary_plans_validate_all_providers(self):
        plans = [
            ordinary_plan(provider, **options)
            for provider in ('oci', 'aws', 'gcp', 'azure')
            for options in (
                {'benchmarks': ['fio']},
                {'benchmarks': ['sysbench'], 'sysbench': {'workloads': ['fileio']}},
                {'benchmarks': ['apachebench']},
                {'benchmarks': [], 'llm_benchmarks': ['llama_bench']},
            )
        ]
        results = self.run_ui(r"""
return fixtures.plans.map(plan => {
    $('#provider').value = plan.provider;
    restoreDeathstarSelection(plan);
    applyProviderBenchmarkSupport();
    return JSON.parse(JSON.stringify({...plan, deathstarbench: deathstarOptions(false)}));
});
""", plans=plans)
        self.assertEqual(len(results), 16)
        for payload in results:
            with self.subTest(provider=payload['provider'], benchmarks=payload['benchmarks']):
                self.assertNotIn('deathstarbench', payload)
                parsed = BenchmarkPlan.model_validate(payload)
                self.assertEqual(parsed.deathstarbench.topology_id, 'single_host_v1')
                self.assertEqual(parsed.deathstarbench.runtime_id, 'podman_compose_v1')
                self.assertEqual(parsed.benchmarks, payload['benchmarks'])

    def test_legacy_missing_unknown_and_crossed_saved_pairs_require_reselection(self):
        options = [
            None, {}, {'workload': 'social_network'},
            {'topology_id': 'single_host_v1', 'runtime_id': 'podman_compose_v1'},
            {'topology_id': 'distributed_tiered_v1'},
            {'runtime_id': 'k3s_v1'},
            {'topology_id': 'distributed_tiered_v1', 'runtime_id': 'podman_compose_v1'},
            {'topology_id': 'single_host_v1', 'runtime_id': 'k3s_v1'},
            {'topology_id': 'distributed_tiered_v2', 'runtime_id': 'k3s_v1'},
            {'topology_id': 'distributed_tiered_v1', 'runtime_id': 'k3s_v2'},
            {'topology_id': 'distributed_tiered_v1', 'runtime_id': 'k3s_v1'},
            {
                'topology_id': 'distributed_tiered_v1', 'runtime_id': 'k3s_v1',
                'workload': 'hotel_reservation',
            },
        ]
        results = self.run_ui(r"""
return fixtures.options.map(options => {
    $('#additional').checked = true;
    ids.get('bench-fio').checked = true;
    ids.get('bench-deathstarbench').checked = true;
    restoreDeathstarSelection({benchmarks: ['deathstarbench', 'fio'], deathstarbench: options});
    applyProviderBenchmarkSupport();
    return {
        selected: ids.get('bench-deathstarbench').checked,
        warningHidden: $('#deathstarLegacyPlanWarning').hidden,
        fioSelected: ids.get('bench-fio').checked,
        fioDisabled: ids.get('bench-fio').disabled,
        additional: $('#additional').checked,
        costWarningHidden: $('#deathstarDistributedWarning').hidden,
        payload: JSON.parse(JSON.stringify({deathstarbench: deathstarOptions(false)})),
    };
});
""", options=options)
        self.assertEqual(len(results), len(options))
        for options, result in zip(options, results):
            with self.subTest(saved_options=options):
                self.assertFalse(result['selected'])
                self.assertFalse(result['warningHidden'])
                self.assertTrue(result['fioSelected'])
                self.assertFalse(result['fioDisabled'])
                self.assertTrue(result['additional'])
                self.assertTrue(result['costWarningHidden'])
                self.assertEqual(result['payload'], {})

    def test_exact_distributed_saved_pair_preserves_numeric_settings(self):
        options = {
            'topology_id': 'distributed_tiered_v1', 'runtime_id': 'k3s_v1',
            'workload': 'social_network', 'warmup_seconds': 7,
            'duration_seconds': 90, 'threads': 8, 'connections': 16,
            'request_rate': 800,
        }
        result = self.run_ui(r"""
restoreDeathstarSelection({benchmarks: ['deathstarbench'], deathstarbench: fixtures.options});
applyProviderBenchmarkSupport();
return {
    selected: ids.get('bench-deathstarbench').checked,
    warningHidden: $('#deathstarLegacyPlanWarning').hidden,
    options: deathstarOptions(true),
};
""", options=options)
        self.assertTrue(result['selected'])
        self.assertTrue(result['warningHidden'])
        self.assertEqual(result['options'], options)

    def test_non_deathstarbench_saved_defaults_do_not_show_retirement_warning(self):
        result = self.run_ui(r"""
ids.get('bench-fio').checked = true;
$('#additional').checked = true;
restoreDeathstarSelection({benchmarks: ['fio'], deathstarbench: {
    topology_id: 'single_host_v1', runtime_id: 'podman_compose_v1',
}});
applyProviderBenchmarkSupport();
return {
    selected: ids.get('bench-deathstarbench').checked,
    warningHidden: $('#deathstarLegacyPlanWarning').hidden,
    fioSelected: ids.get('bench-fio').checked,
    additional: $('#additional').checked,
};
""")
        self.assertFalse(result['selected'])
        self.assertTrue(result['warningHidden'])
        self.assertTrue(result['fioSelected'])
        self.assertTrue(result['additional'])

    def test_full_legacy_mixed_plan_restore_preserves_other_tests_and_storage(self):
        plans = [ordinary_plan(
            provider,
            benchmarks=['deathstarbench', 'fio', 'sysbench_memory', 'sysbench_fileio', 'iperf_tcp'],
            llm_benchmarks=['llama_bench'],
            deathstarbench={'topology_id': 'single_host_v1', 'runtime_id': 'podman_compose_v1'},
            storage={
                'additional_volume': True, 'boot_size_gb': 150, 'boot_performance': 20,
                'additional_size_gb': 2048, 'additional_performance': 30,
                'mount_style': 'iscsi',
            },
        ) for provider in ('oci', 'aws', 'gcp', 'azure')]
        results = self.run_ui(r"""
const results = [];
for (const plan of fixtures.plans) {
    // A previous form's override must not replace this saved plan's storage.
    $('#additional').dataset.deathstarPreviousAdditional = 'false';
    await restorePlan(plan);
    results.push({
        provider: currentProvider(),
        benchmarks: $$('input[data-kind="bench"]:checked').map(input => input.value),
        llm: $$('input[data-kind="llm"]:checked').map(input => input.value),
        sysbench: $$('input[data-kind="sysbench-workload"]:checked').map(input => input.value),
        iperf3: $$('input[data-kind="iperf3-protocol"]:checked').map(input => input.value),
        additional: $('#additional').checked, additionalDisabled: $('#additional').disabled,
        bootSize: $('#bootSize').value, bootPerf: $('#bootPerf').value,
        dataSize: $('#dataSize').value, dataPerf: $('#dataPerf').value, mount: $('#mount').value,
        warningHidden: $('#deathstarLegacyPlanWarning').hidden,
        savedVolumeOverride: $('#additional').dataset.deathstarPreviousAdditional,
    });
}
return results;
""", plans=plans)
        for result in results:
            with self.subTest(provider=result['provider']):
                self.assertEqual(result['benchmarks'], ['fio', 'sysbench', 'iperf3'])
                self.assertEqual(result['llm'], ['llama_bench'])
                self.assertEqual(result['sysbench'], ['memory', 'fileio'])
                self.assertEqual(result['iperf3'], ['tcp'])
                self.assertTrue(result['additional'])
                self.assertFalse(result['additionalDisabled'])
                self.assertEqual(result['bootSize'], '150')
                self.assertEqual(result['bootPerf'], '20')
                self.assertEqual(result['dataSize'], '2048')
                self.assertEqual(result['dataPerf'], '30')
                self.assertEqual(result['mount'], 'iscsi')
                self.assertFalse(result['warningHidden'])
                self.assertNotIn('savedVolumeOverride', result)

    def test_full_distributed_restore_preserves_settings_and_oci_defined_tags(self):
        options = {
            'topology_id': 'distributed_tiered_v1', 'runtime_id': 'k3s_v1',
            'workload': 'social_network', 'warmup_seconds': 0,
            'duration_seconds': 120, 'threads': 8, 'connections': 32,
            'request_rate': 800,
        }
        tags = {'Benchmarks': {'Owner': 'test-owner'}}
        plans = [ordinary_plan(
            provider, benchmarks=['deathstarbench'],
            deathstarbench=options, storage={'additional_volume': False},
            oci_defined_tags=tags if provider == 'oci' else {},
        ) for provider in ('oci', 'aws', 'gcp', 'azure')]
        results = self.run_ui(r"""
const results = [];
for (const plan of fixtures.plans) {
    $('#additional').dataset.deathstarPreviousAdditional = 'true';
    await restorePlan(plan);
    results.push({
        provider: currentProvider(), options: deathstarOptions(true),
        benchmarks: $$('input[data-kind="bench"]:checked').map(input => input.value),
        additional: $('#additional').checked, additionalDisabled: $('#additional').disabled,
        warningHidden: $('#deathstarLegacyPlanWarning').hidden,
        costWarningHidden: $('#deathstarDistributedWarning').hidden,
        tags: JSON.parse($('#ociDefinedTags').value),
    });
}
return results;
""", plans=plans)
        for result in results:
            with self.subTest(provider=result['provider']):
                self.assertEqual(result['options'], options)
                self.assertEqual(result['benchmarks'], ['deathstarbench'])
                self.assertFalse(result['additional'])
                self.assertTrue(result['additionalDisabled'])
                self.assertTrue(result['warningHidden'])
                self.assertFalse(result['costWarningHidden'])
                self.assertEqual(result['tags'], tags if result['provider'] == 'oci' else {})

    def test_ordinary_restore_clears_active_distributed_selection_before_discovery(self):
        plans = [ordinary_plan(
            provider,
            benchmarks=['fio'] if additional else ['sysbench'],
            sysbench={'workloads': ['cpu']},
            llm_benchmarks=['llama_bench'],
            storage={'additional_volume': additional},
            deathstarbench={
                'topology_id': 'single_host_v1', 'runtime_id': 'podman_compose_v1',
            },
        ) for provider in ('oci', 'aws', 'gcp', 'azure') for additional in (True, False)]
        results = self.run_ui(r"""
const discoverySnapshots = [];
// Real provider discovery reapplies benchmark support while restorePlan is
// awaiting it. A no-op discovery stub would hide the stale override bug.
loadProvider = async () => {
    discoverySnapshots.push({
        dsbSelected: ids.get('bench-deathstarbench').checked,
        volumeOverride: $('#additional').dataset.deathstarPreviousAdditional,
    });
    applyProviderBenchmarkSupport();
};
const results = [];
for (const plan of fixtures.plans) {
    $('#additional').checked = true;
    ids.get('bench-deathstarbench').checked = true;
    handleDeathstarSelectionChange();
    const initiallyDistributed = isDistributedDeathstarbenchSelected();
    const initiallyOverridden = $('#additional').checked === false
        && $('#additional').dataset.deathstarPreviousAdditional === 'true';
    await restorePlan(plan);
    results.push({
        provider: currentProvider(), expectedAdditional: plan.storage.additional_volume,
        initiallyDistributed, initiallyOverridden,
        discovery: discoverySnapshots.at(-1),
        additional: $('#additional').checked, additionalDisabled: $('#additional').disabled,
        savedVolumeOverride: $('#additional').dataset.deathstarPreviousAdditional,
        dsbSelected: ids.get('bench-deathstarbench').checked,
        benchmarks: $$('input[data-kind="bench"]:checked').map(input => input.value),
        fioDisabled: ids.get('bench-fio').disabled,
        sysbenchDisabled: ids.get('bench-sysbench').disabled,
        llm: $$('input[data-kind="llm"]:checked').map(input => input.value),
        llmDisabled: ids.get('llm-llama_bench').disabled,
        warningHidden: $('#deathstarLegacyPlanWarning').hidden,
    });
}
return results;
""", plans=plans)
        self.assertEqual(len(results), 8)
        for result in results:
            with self.subTest(provider=result['provider'], volume=result['expectedAdditional']):
                self.assertTrue(result['initiallyDistributed'])
                self.assertTrue(result['initiallyOverridden'])
                self.assertFalse(result['discovery']['dsbSelected'])
                self.assertNotIn('volumeOverride', result['discovery'])
                self.assertEqual(result['additional'], result['expectedAdditional'])
                self.assertFalse(result['additionalDisabled'])
                self.assertFalse(result['dsbSelected'])
                self.assertNotIn('savedVolumeOverride', result)
                self.assertEqual(
                    result['benchmarks'], ['fio'] if result['expectedAdditional'] else ['sysbench'],
                )
                self.assertFalse(result['fioDisabled'])
                self.assertFalse(result['sysbenchDisabled'])
                self.assertEqual(result['llm'], ['llama_bench'])
                self.assertFalse(result['llmDisabled'])
                self.assertTrue(result['warningHidden'])


if __name__ == '__main__':
    unittest.main()
