import re
import unittest
from html.parser import HTMLParser
from pathlib import Path

from app import main
from app.providers import aws, oci


ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / 'app/static/index.html').read_text()
HISTORY_INDEX = (ROOT / 'app/static/history.html').read_text()
JAVASCRIPT = (ROOT / 'app/static/app.js').read_text()
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


class LiveStopUiTests(unittest.TestCase):
    @staticmethod
    def partial_distributed_job(provider, status):
        ownership_key = (
            aws.AWS_DSB_GRAPH_KEY
            if provider == 'aws'
            else oci.CONTRACT_KEY
        )
        return {
            'id': '123456789abc',
            'status': status,
            'plan': {
                'provider': provider,
                'benchmarks': ['deathstarbench'],
                'llm_benchmarks': [],
                'deathstarbench': {
                    'topology_id': 'distributed_tiered_v1',
                    'runtime_id': 'k3s_v1',
                    'workload': 'social_network',
                },
            },
            'resources': {ownership_key: {}},
        }

    def test_live_stop_action_is_shared_prominent_and_accessible(self):
        parser = ElementIndex()
        parser.feed(INDEX)

        panel_tag, panel_attrs = parser.elements['liveRunStop']
        self.assertEqual(panel_tag, 'aside')
        self.assertIn('hidden', panel_attrs)
        self.assertEqual(panel_attrs.get('aria-labelledby'), 'liveRunStopTitle')
        button_tag, button_attrs = parser.elements['stopAndDestroy']
        self.assertEqual(button_tag, 'button')
        self.assertEqual(button_attrs.get('type'), 'button')
        self.assertIn('danger-action', button_attrs.get('class', ''))
        self.assertEqual(
            parser.elements['liveRunStopStatus'][1].get('aria-live'),
            'polite',
        )
        self.assertIn('.live-run-stop {', STYLES)
        self.assertIn('.danger-action {', STYLES)
        self.assertIn('.danger-action:disabled {', STYLES)

    def test_only_live_active_phases_offer_stop(self):
        self.assertIn(
            "const liveStoppableStatuses = ['queued', 'provisioning', "
            "'testing', 'reporting'];",
            JAVASCRIPT,
        )
        self.assertIn(
            'job.live !== false && liveStoppableStatuses.includes(lifecycle)',
            JAVASCRIPT,
        )
        self.assertIn("lifecycle === 'cancelling'", JAVASCRIPT)
        self.assertIn("lifecycle === 'destroying'", JAVASCRIPT)
        self.assertIn('button.disabled = cancelling || destroyingAfterStop;', JAVASCRIPT)
        self.assertNotIn('cancelling', re.search(
            r'const terminalStatuses = \[(.*?)\];',
            JAVASCRIPT,
        ).group(1))

    def test_stop_is_confirmed_idempotent_and_uses_destroy_contract(self):
        self.assertIn('async function stopLiveRunAndDestroy()', JAVASCRIPT)
        self.assertIn(
            'if (!jobId || liveStopPending || button.disabled) return;',
            JAVASCRIPT,
        )
        self.assertIn('if (!window.confirm(', JAVASCRIPT)
        self.assertIn(
            'Stop this benchmark run and destroy all recorded cloud infrastructure?',
            JAVASCRIPT,
        )
        self.assertIn('This cannot be undone.', JAVASCRIPT)
        self.assertIn('liveStopPending = true;', JAVASCRIPT)
        self.assertIn(
            "api(`/api/jobs/${jobId}/destroy`, { method: 'POST' })",
            JAVASCRIPT,
        )
        self.assertIn(
            "$('#stopAndDestroy').addEventListener('click', stopLiveRunAndDestroy)",
            JAVASCRIPT,
        )

    def test_rejected_destroy_keeps_retry_action_and_surfaces_api_detail(self):
        start = JAVASCRIPT.index('async function destroyCurrentJob(button)')
        end = JAVASCRIPT.index(
            "$('#stopAndDestroy').addEventListener",
            start,
        )
        destroy = JAVASCRIPT[start:end]
        self.assertIn('button.disabled = true;', destroy)
        self.assertIn('} catch (error) {', destroy)
        self.assertIn('button.hidden = false;', destroy)
        self.assertIn('button.disabled = false;', destroy)
        self.assertIn(
            '`Unable to destroy infrastructure: ${error.message}`',
            destroy,
        )

    def test_cancel_state_keeps_polling_through_cleanup(self):
        self.assertIn("status: response.status || 'cancelling'", JAVASCRIPT)
        self.assertIn("status: 'cancelling'", JAVASCRIPT)
        self.assertIn("button.textContent = 'Stopping run…'", JAVASCRIPT)
        self.assertIn("button.textContent = 'Destroying infrastructure…'", JAVASCRIPT)
        self.assertIn('poller = setInterval(poll, 2500);', JAVASCRIPT)
        self.assertIn(
            'if (terminalStatuses.includes(job.status)) clearInterval(poller);',
            JAVASCRIPT,
        )
        self.assertIn("'destroyed'", JAVASCRIPT)
        self.assertIn("'cleanup_failed'", JAVASCRIPT)

    def test_recoverable_terminal_run_destroy_action_is_preserved(self):
        parser = ElementIndex()
        parser.feed(INDEX)

        self.assertEqual(parser.elements['destroyInterrupted'][0], 'button')
        self.assertIn(
            "const recoverableDestroyStatuses = ['complete', 'failed', "
            "'cleanup_failed', 'interrupted'];",
            JAVASCRIPT,
        )
        self.assertIn(
            'function canDestroyRecoverableJob(job)',
            JAVASCRIPT,
        )
        self.assertIn(
            'return job.recoverable === true',
            JAVASCRIPT,
        )
        self.assertIn(
            "recoverableDestroyStatuses.includes(job.status)",
            JAVASCRIPT,
        )
        self.assertIn(
            "destroyCurrentJob($('#destroyInterrupted'))",
            JAVASCRIPT,
        )

    def test_failed_partial_distributed_contract_uses_backend_recovery_gate(self):
        progress_start = JAVASCRIPT.index('function renderJobProgress(job)')
        progress_end = JAVASCRIPT.index('function retainedConnections(job)', progress_start)
        report_start = JAVASCRIPT.index('function showReport(job)')
        report_end = JAVASCRIPT.index('async function poll()', report_start)
        progress = JAVASCRIPT[progress_start:progress_end]
        report = JAVASCRIPT[report_start:report_end]

        # Partial AWS and OCI distributed identities live inside their durable
        # provider contracts until runtime aliases are published. The backend's
        # recoverable projection understands both graphs; the UI must not
        # require a flat top-level resource ID before offering cleanup.
        self.assertIn(
            "$('#destroyInterrupted').hidden = !canDestroyRecoverableJob(job);",
            progress,
        )
        self.assertIn(
            'const canDestroy = canDestroyRecoverableJob(job);',
            report,
        )
        self.assertNotIn('managedResourcesRemain', report)
        self.assertNotIn("resources.some(([key]) => key.endsWith('_id'))", progress)
        for provider in ('aws', 'oci'):
            with self.subTest(provider=provider):
                self.assertTrue(main.has_recoverable_resources(
                    self.partial_distributed_job(provider, 'failed')
                ))

    def test_completed_retained_run_uses_the_same_backend_recovery_gate(self):
        self.assertIn("'complete'", re.search(
            r'const recoverableDestroyStatuses = \[(.*?)\];',
            JAVASCRIPT,
        ).group(1))
        report_start = JAVASCRIPT.index('function showReport(job)')
        report_end = JAVASCRIPT.index('async function poll()', report_start)
        report = JAVASCRIPT[report_start:report_end]
        self.assertIn('explicitlyRetained', report)
        self.assertIn(
            'const canDestroy = canDestroyRecoverableJob(job);',
            report,
        )
        for provider in ('aws', 'oci'):
            with self.subTest(provider=provider):
                self.assertTrue(main.has_recoverable_resources(
                    self.partial_distributed_job(provider, 'complete')
                ))

    def test_reset_copy_explicitly_does_not_stop_a_run(self):
        normalized_index = ' '.join(INDEX.split())
        self.assertEqual(
            normalized_index.count('Reset view (does not stop run)'),
            1,
        )
        self.assertIn(
            'Create a new run while the current run is active.',
            normalized_index,
        )
        self.assertIn(
            'Reset view does not stop the run ' +
            "' +\n        'or destroy its infrastructure.",
            JAVASCRIPT,
        )

    def test_changed_assets_are_cache_busted(self):
        self.assertIn('/static/styles.css?v=19', INDEX)
        self.assertIn('/static/styles.css?v=19', HISTORY_INDEX)
        self.assertIn('/static/app.js?v=36', INDEX)


if __name__ == '__main__':
    unittest.main()
