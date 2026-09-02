import re
import unittest
from html.parser import HTMLParser
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / 'app/static/index.html').read_text()
JAVASCRIPT = (ROOT / 'app/static/app.js').read_text()
HISTORY = (ROOT / 'app/static/history.js').read_text()


class ElementIndex(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements = {}

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        element_id = attributes.get('id')
        if element_id:
            self.elements[element_id] = (tag, attributes)


class AwsUiTests(unittest.TestCase):
    def test_plan_has_provider_and_default_aws_profile_controls(self):
        parser = ElementIndex()
        parser.feed(INDEX)

        self.assertEqual(parser.elements['provider'][0], 'select')
        self.assertEqual(parser.elements['awsProfile'][0], 'input')
        self.assertEqual(parser.elements['awsProfile'][1].get('value'), 'default')
        self.assertIn('hidden', parser.elements['awsProfileField'][1])

    def test_frontend_uses_provider_scoped_aws_discovery_routes(self):
        self.assertIn('/api/providers/aws/bootstrap?profile=', JAVASCRIPT)
        self.assertIn('/api/providers/aws/instance-types', JAVASCRIPT)
        self.assertIn("provider === 'aws' ? 'us-east-2'", JAVASCRIPT)

    def test_aws_mode_hides_oci_controls_and_locks_capacity(self):
        for element_id in (
            'compartmentField',
            'adField',
            'fdField',
            'ociComputeOptions',
        ):
            self.assertIn(
                f"setProviderOnlyElement('#{element_id}', oci)",
                JAVASCRIPT,
            )
        for element_id in ('bootPerfField', 'dataPerfField', 'mountField'):
            self.assertIn(
                f"setProviderOnlyElement('#{element_id}', oci)",
                JAVASCRIPT,
            )
        self.assertIn("$('#ocpus').readOnly = fixedCapacity", JAVASCRIPT)
        self.assertIn("$('#memory').readOnly = fixedCapacity", JAVASCRIPT)
        self.assertIn(
            "$('#additional').dataset.ociChecked === undefined",
            JAVASCRIPT,
        )
        self.assertIn("'EC2 Instance Type'", JAVASCRIPT)
        self.assertIn('Amazon Linux 2023', JAVASCRIPT)
        self.assertIn(
            'Select a valid EC2 instance type from the searchable list.',
            JAVASCRIPT,
        )
        self.assertIn(
            "$('#shape').addEventListener('input', updateSelectedShape)",
            JAVASCRIPT,
        )
        self.assertIn('burstable performance', JAVASCRIPT)
        self.assertIn('bare metal', JAVASCRIPT)

    def test_aws_mode_warns_about_public_ssh_and_uses_guest_wording(self):
        self.assertIn('SSH TCP/22 is open to 0.0.0.0/0', JAVASCRIPT)
        self.assertIn('connect as ec2-user', JAVASCRIPT)
        self.assertIn('Arm64 Amazon Linux 2023 instances', JAVASCRIPT)
        self.assertIn('connect as opc', JAVASCRIPT)
        self.assertIn('AArch64 Oracle Linux instances', JAVASCRIPT)

    def test_aws_mode_exposes_gp3_storage_benchmarks_and_controls(self):
        self.assertIn("providerCapabilitySet('benchmarks')", JAVASCRIPT)
        self.assertIn("providerCapabilitySet('sysbench_workloads')", JAVASCRIPT)
        self.assertNotIn('const awsSupportedBenchmarks', JAVASCRIPT)
        self.assertIn("llm_benchmarks: selected('llm')", JAVASCRIPT)
        self.assertIn("additional_volume: $('#additional').checked", JAVASCRIPT)
        self.assertIn("$('#awsStorageHint').hidden = !aws", JAVASCRIPT)
        self.assertIn('3,000 IOPS and 125 MiB/s', INDEX)

        parser = ElementIndex()
        parser.feed(INDEX)
        self.assertEqual(parser.elements['storageOptions'][0], 'fieldset')

    def test_payload_and_rerun_are_provider_aware(self):
        self.assertRegex(JAVASCRIPT, r'const data = \{\s*provider,')
        self.assertIn("aws_profile: ($('#awsProfile').value || 'default').trim()", JAVASCRIPT)
        self.assertIn("const provider = plan.provider || 'oci'", JAVASCRIPT)
        self.assertIn("$('#awsProfile').value = plan.aws_profile || 'default'", JAVASCRIPT)

    def test_saved_job_resume_does_not_depend_on_default_provider_discovery(self):
        init_start = JAVASCRIPT.index('async function init()')
        init_end = JAVASCRIPT.index('function currentProvider()', init_start)
        init = JAVASCRIPT[init_start:init_end]

        self.assertIn('const resumed = await resumeLastJob();', init)
        self.assertIn('if (!resumed) await loadProvider();', init)
        self.assertLess(init.index('resumeLastJob()'), init.index('loadProvider()'))

        resume_start = JAVASCRIPT.index('async function resumeLastJob()')
        resume_end = JAVASCRIPT.index('function leaveReport()', resume_start)
        resume = JAVASCRIPT[resume_start:resume_end]
        self.assertIn('return true;', resume)
        self.assertIn('return false;', resume)

    def test_discovery_clears_stale_shapes_and_ignores_old_responses(self):
        self.assertIn('let discoveryGeneration = 0;', JAVASCRIPT)
        self.assertIn('shapes = [];', JAVASCRIPT)
        self.assertIn("$('#shapeList').innerHTML = '';", JAVASCRIPT)
        self.assertIn('const generation = ++discoveryGeneration;', JAVASCRIPT)
        self.assertGreaterEqual(
            JAVASCRIPT.count('if (!discoveryIsCurrent(generation)) return false;'),
            2,
        )
        self.assertIn("$('#shape').disabled = loading", JAVASCRIPT)
        self.assertIn('if (submit) submit.disabled = loading;', JAVASCRIPT)
        self.assertIn('if (discoveryLoading) {', JAVASCRIPT)
        self.assertIn(
            'Wait for the current cloud discovery request to finish.',
            JAVASCRIPT,
        )

    def test_history_displays_provider_with_legacy_oci_fallback(self):
        self.assertIn("runField(run, 'provider') || 'oci'", HISTORY)
        self.assertIn("addMeta(meta, 'Provider'", HISTORY)
        self.assertIn("aws: 'AWS'", HISTORY)
        self.assertIn("gcp: 'GCP'", HISTORY)

    def test_history_distinguishes_failed_benchmarks_after_destroy(self):
        self.assertIn('benchmarkLifecycleLabels[run.benchmark_status]', HISTORY)
        self.assertIn('Failed · infrastructure destroyed', HISTORY)
        self.assertIn('Failed · cleanup warning', HISTORY)
        self.assertIn('Interrupted · infrastructure destroyed', HISTORY)
        self.assertIn('status-${benchmarkLifecycleLabel ? run.benchmark_status', HISTORY)
        self.assertIn('Completed · infrastructure destroyed', HISTORY)

    def test_history_offers_cleanup_without_download_for_state_only_runs(self):
        self.assertIn('if (run.report_ready !== false)', HISTORY)
        self.assertIn("action('Review / clean up'", HISTORY)
        report_branch = HISTORY.index('if (run.report_ready !== false)')
        state_branch = HISTORY.index('} else {', report_branch)
        branch_end = HISTORY.index('card.append(', state_branch)
        self.assertIn('Download HTML', HISTORY[report_branch:state_branch])
        self.assertNotIn('Download HTML', HISTORY[state_branch:branch_end])

    def test_interrupted_recoverable_run_renders_cleanup_controls(self):
        parser = ElementIndex()
        parser.feed(INDEX)

        self.assertEqual(parser.elements['resources'][0], 'div')
        self.assertIn('hidden', parser.elements['resources'][1])
        self.assertEqual(parser.elements['destroyInterrupted'][0], 'button')
        self.assertIn('hidden', parser.elements['destroyInterrupted'][1])
        self.assertIn('function renderJobProgress(job)', JAVASCRIPT)
        self.assertIn('recordedResourceEntries(job)', JAVASCRIPT)
        self.assertIn("['interrupted', 'cleanup_failed'].includes(job.status)", JAVASCRIPT)
        resume_start = JAVASCRIPT.index('async function resumeLastJob()')
        resume_end = JAVASCRIPT.index('function leaveReport()', resume_start)
        self.assertIn('renderJobProgress(job);', JAVASCRIPT[resume_start:resume_end])
        self.assertIn("$('#destroyInterrupted').addEventListener", JAVASCRIPT)
        self.assertIn("destroyCurrentJob($('#destroyInterrupted'))", JAVASCRIPT)

    def test_assets_are_cache_busted(self):
        styles = re.findall(r'/static/styles\.css\?v=(\d+)', INDEX)
        scripts = re.findall(r'/static/app\.js\?v=(\d+)', INDEX)

        self.assertEqual(styles, ['15'])
        self.assertEqual(scripts, ['30'])

    def test_hidden_provider_controls_cannot_be_overridden_by_label_layout(self):
        styles = (ROOT / 'app/static/styles.css').read_text()

        self.assertRegex(
            styles,
            r'\[hidden\]\s*\{\s*display:\s*none\s*!important',
        )


if __name__ == '__main__':
    unittest.main()
