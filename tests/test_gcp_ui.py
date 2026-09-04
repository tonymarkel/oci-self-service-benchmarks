import json
import os
import unittest
from html.parser import HTMLParser
from pathlib import Path
from unittest.mock import patch

from app import main


ROOT = Path(__file__).resolve().parents[1]
INDEX = (ROOT / 'app/static/index.html').read_text()
JAVASCRIPT = (ROOT / 'app/static/app.js').read_text()
HISTORY = (ROOT / 'app/static/history.js').read_text()
MAIN = (ROOT / 'app/main.py').read_text()


class ElementIndex(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements = {}

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        element_id = attributes.get('id')
        if element_id:
            self.elements[element_id] = (tag, attributes)


class GcpUiTests(unittest.TestCase):
    def test_plan_has_provider_specific_project_and_zone_controls(self):
        parser = ElementIndex()
        parser.feed(INDEX)

        self.assertEqual(parser.elements['gcpProject'][0], 'input')
        self.assertEqual(parser.elements['gcpZone'][0], 'select')
        self.assertIn('hidden', parser.elements['gcpProjectField'][1])
        self.assertIn('hidden', parser.elements['gcpZoneField'][1])

    def test_frontend_uses_provider_scoped_gcp_discovery_routes(self):
        self.assertIn('/api/providers/gcp/bootstrap', JAVASCRIPT)
        self.assertIn('/api/providers/gcp/placement?project_id=', JAVASCRIPT)
        self.assertIn('/api/providers/gcp/machine-types', JAVASCRIPT)
        self.assertIn("provider === 'gcp' ? 'us-east1'", JAVASCRIPT)
        self.assertIn(
            'data.availability_zones || data.zones || []',
            JAVASCRIPT,
        )

    def test_gcp_mode_uses_adc_rocky_linux_and_fixed_capacity_wording(self):
        self.assertIn('Accesses Google Cloud through ADC', JAVASCRIPT)
        self.assertIn('standard Rocky Linux 9 image', JAVASCRIPT)
        self.assertIn('Compute Engine Machine Type', JAVASCRIPT)
        self.assertIn('connect as benchmark', JAVASCRIPT)
        self.assertIn('SSH TCP/22 is open to 0.0.0.0/0', JAVASCRIPT)
        self.assertIn("$('#gcpStorageHint').hidden = !gcp", JAVASCRIPT)
        self.assertIn('GCP /data volumes use pd-balanced', INDEX)
        self.assertIn('const fixedCapacity = aws || gcp;', JAVASCRIPT)
        self.assertIn("setProviderOnlyElement('#gcpProjectField', gcp)", JAVASCRIPT)
        self.assertIn("setProviderOnlyElement('#gcpZoneField', gcp)", JAVASCRIPT)

    def test_payload_and_rerun_preserve_gcp_project_and_zone(self):
        self.assertIn("gcp_project_id: gcp ? $('#gcpProject').value.trim() : null", JAVASCRIPT)
        self.assertIn("gcp_zone: gcp ? $('#gcpZone').value : null", JAVASCRIPT)
        self.assertIn("$('#gcpProject').value = plan.gcp_project_id || ''", JAVASCRIPT)
        self.assertIn("$('#gcpZone').value = plan.gcp_zone || ''", JAVASCRIPT)

    def test_history_labels_gcp_capacity_without_project_or_zone(self):
        self.assertIn("gcp: 'GCP'", HISTORY)
        self.assertNotIn(
            "addMeta(meta, 'Project', run.gcp_project_id)",
            HISTORY,
        )
        self.assertNotIn("addMeta(meta, 'Zone', run.gcp_zone)", HISTORY)
        self.assertIn("provider === 'gcp' ? 'Machine type' : 'Shape'", HISTORY)

    def test_project_and_image_metadata_are_not_treated_as_resources(self):
        for metadata_key in (
            'gcp_compute_project_id',
            'gcp_peer_image_id',
            'gcp_project_id',
        ):
            self.assertIn(f"'{metadata_key}'", JAVASCRIPT)
        self.assertIn(
            'const managedResourcesRemain = recordedResourceEntries(job)',
            JAVASCRIPT,
        )

    def test_discovery_routes_delegate_to_the_provider_adapter(self):
        self.assertIn('gcp_provider.bootstrap(', MAIN)
        self.assertIn('gcp_provider.placement(', MAIN)
        self.assertIn('gcp_provider.machine_types(', MAIN)
        self.assertNotIn('def gcp_sdk(', MAIN)
        self.assertNotIn('def gcp_adc_context(', MAIN)

    def test_gcp_discovery_responses_disable_http_caching(self):
        cases = (
            (
                'bootstrap',
                lambda: main.gcp_bootstrap('example-project'),
                {
                    'provider': 'gcp',
                    'project_id': 'example-project',
                    'regions': ['us-east1'],
                },
            ),
            (
                'placement',
                lambda: main.gcp_placement('example-project', 'us-east1'),
                {'availability_zones': [{'name': 'us-east1-b'}]},
            ),
            (
                'machine_types',
                lambda: main.gcp_machine_types(
                    'example-project',
                    'us-east1-b',
                ),
                {'items': [{'machine_type': 'n2-standard-2'}]},
            ),
        )
        for method_name, route, payload in cases:
            with self.subTest(method=method_name):
                with (
                    patch.object(
                        main.gcp_provider,
                        method_name,
                        return_value=payload,
                    ),
                    patch.dict(os.environ, {}, clear=False),
                ):
                    response = route()

                self.assertEqual(response.headers['cache-control'], 'no-store')
                self.assertEqual(json.loads(response.body), payload)

    def test_gcp_discovery_errors_also_disable_http_caching(self):
        with (
            patch.object(
                main.gcp_provider,
                'bootstrap',
                side_effect=RuntimeError('ADC unavailable'),
            ),
            self.assertRaises(main.HTTPException) as raised,
        ):
            main.gcp_bootstrap()

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(
            raised.exception.headers['Cache-Control'],
            'no-store',
        )

    def test_gcp_bootstrap_uses_environment_discovery_defaults(self):
        payload = {
            'project_id': 'environment-project',
            'default_region': 'us-east4',
            'regions': ['us-east4'],
        }
        with (
            patch.dict(
                os.environ,
                {
                    'GCP_PROJECT_ID': 'environment-project',
                    'GCP_DEFAULT_REGION': 'us-east4',
                },
            ),
            patch.object(
                main.gcp_provider,
                'bootstrap',
                return_value=payload,
            ) as bootstrap,
        ):
            response = main.gcp_bootstrap()

        bootstrap.assert_called_once_with(
            project_id='environment-project',
            default_region='us-east4',
        )
        self.assertEqual(json.loads(response.body), payload)


if __name__ == '__main__':
    unittest.main()
