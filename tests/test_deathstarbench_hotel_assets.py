import copy
import hashlib
import json
from pathlib import Path
import re
import unittest

from app.k3s_runtime import K3S_SERVICE_NODE_PORT_RANGE


ASSET_DIRECTORY = (
    Path(__file__).resolve().parents[1]
    / 'app'
    / 'manifests'
    / 'deathstarbench'
    / 'hotel-reservation-v1'
)
UPSTREAM_REVISION = '6ecb09706140f8730b5385c08f1386c654c3c526'
WORKLOAD_REVISION = 'hotel-reservation-6ecb097-workload-v1'
NAMESPACE = 'deathstarbench-hotel'
REQUEST_SCRIPT_SHA256 = (
    'af6230aaaf220fceaef6208d416c5a9d75f15f66d70e3cde00b6e441957bf352'
)
COMPONENT_ASSET_SHA256 = (
    'bc71e76636b394012689e5a54d3d265ae0c69b4ef13daf0c98ff2e0ef74ed531'
)
POLICY_ASSET_SHA256 = (
    'c5b00bde4a2e86236ca1b97ecc9a32a98cbea12e8eee667e83c7345715422c38'
)
APPLICATION_PORTS = {
    'attractions': 8089,
    'frontend': 5000,
    'geo': 8083,
    'profile': 8081,
    'rate': 8084,
    'recommendation': 8085,
    'reservation': 8087,
    'review': 8088,
    'search': 8082,
    'user': 8086,
}
DATABASE_SERVICES = (
    'attractions',
    'geo',
    'profile',
    'rate',
    'recommendation',
    'reservation',
    'review',
    'user',
)
CACHE_SERVICES = (
    ('profile', 'memcached-profile'),
    ('rate', 'memcached-rate'),
    ('reservation', 'memcached-reserve'),
    ('review', 'memcached-review'),
)
EXPECTED_ROLES = {
    **{name: 'application' for name in APPLICATION_PORTS},
    **{f'mongodb-{name}': 'database' for name in DATABASE_SERVICES},
    **{name: 'cache' for _, name in CACHE_SERVICES},
    'consul': 'control',
    'jaeger': 'control',
}
EXPECTED_IMAGE_KEYS = {
    'application': 'hotel-reservation',
    'database': 'mongodb',
    'cache': 'memcached',
}
APPLICATION_ENVIRONMENT = {
    'GC': '100',
    'JAEGER_SAMPLE_RATIO': '0.01',
    'LOG_LEVEL': 'INFO',
    'MEMC_TIMEOUT': '2',
    'TLS': '0',
}


def _unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'Duplicate JSON key: {key}')
        result[key] = value
    return result


def read_asset(name):
    return json.loads(
        (ASSET_DIRECTORY / name).read_text(encoding='utf-8'),
        object_pairs_hook=_unique_json_object,
    )


def expected_edges():
    edges = set()
    for destination in (
        'attractions', 'profile', 'recommendation', 'reservation',
        'review', 'search', 'user',
    ):
        edges.add((
            'frontend', destination, 'TCP', APPLICATION_PORTS[destination],
        ))
    for destination in ('geo', 'rate'):
        edges.add((
            'search', destination, 'TCP', APPLICATION_PORTS[destination],
        ))
    for name in DATABASE_SERVICES:
        edges.add((name, f'mongodb-{name}', 'TCP', 27017))
    for source, destination in CACHE_SERVICES:
        edges.add((source, destination, 'TCP', 11211))
    for name in APPLICATION_PORTS:
        edges.add((name, 'consul', 'TCP', 8500))
        edges.add((name, 'jaeger', 'UDP', 6831))
    return edges


class HotelReservationCandidateAssetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.components = read_asset('components.json')
        cls.policies = read_asset('network-policies.json')
        cls.by_name = {
            item['name']: item for item in cls.components['components']
        }
        cls.audit = (ASSET_DIRECTORY / 'upstream-audit.md').read_text(
            encoding='utf-8',
        )

    def assert_candidate_identity(self, value, payload_field):
        self.assertEqual(set(value), {
            'schema_version', 'namespace', 'upstream_revision',
            'workload_revision', 'released', payload_field,
        })
        self.assertIs(type(value['schema_version']), int)
        self.assertEqual(value['schema_version'], 2)
        self.assertEqual(value['namespace'], NAMESPACE)
        self.assertEqual(value['upstream_revision'], UPSTREAM_REVISION)
        self.assertEqual(value['workload_revision'], WORKLOAD_REVISION)
        self.assertIs(value['released'], False)

    def test_both_asset_identities_are_explicit_unreleased_candidates(self):
        self.assert_candidate_identity(self.components, 'components')
        self.assert_candidate_identity(self.policies, 'edges')
        self.assertNotIn('social-network', self.components['namespace'])

    def test_exact_asset_bytes_are_frozen_independently_of_rendering(self):
        for name, expected in (
            ('components.json', COMPONENT_ASSET_SHA256),
            ('network-policies.json', POLICY_ASSET_SHA256),
        ):
            with self.subTest(asset=name):
                actual = hashlib.sha256(
                    (ASSET_DIRECTORY / name).read_bytes()
                ).hexdigest()
                self.assertEqual(actual, expected)

    def test_candidate_registry_and_validator_agree_with_the_audited_fixture(self):
        from app.deathstarbench_candidate_workload import validate_candidate_assets
        from app.deathstarbench_workload_contract import distributed_workload_profile

        profile = distributed_workload_profile('hotel_reservation')
        self.assertIs(profile.released, False)
        self.assertEqual(profile.request_script_sha256, REQUEST_SCRIPT_SHA256)
        self.assertEqual(profile.component_asset_sha256, COMPONENT_ASSET_SHA256)
        self.assertEqual(profile.network_policy_asset_sha256, POLICY_ASSET_SHA256)
        self.assertEqual(dict(profile.expected_component_placement), EXPECTED_ROLES)
        components, edges = validate_candidate_assets('hotel_reservation')
        self.assertEqual(list(components), self.components['components'])
        self.assertEqual(list(edges), sorted(expected_edges()))

    def test_all_24_components_have_exact_roles_and_sorted_unique_names(self):
        components = self.components['components']
        names = [item['name'] for item in components]
        self.assertEqual(names, sorted(EXPECTED_ROLES))
        self.assertEqual(len(components), 24)
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual(
            {item['name']: item['role'] for item in components},
            EXPECTED_ROLES,
        )
        self.assertEqual(
            {role: sum(item['role'] == role for item in components)
             for role in ('application', 'database', 'cache', 'control')},
            {'application': 10, 'database': 8, 'cache': 4, 'control': 2},
        )
        self.assertNotIn('load_generator', {item['role'] for item in components})

    def test_component_schema_and_images_do_not_embed_mutable_runtime_inputs(self):
        fields = {
            'name', 'image_key', 'role', 'command', 'environment', 'ports',
            'service_type', 'external_traffic_policy', 'storage_subdir',
        }
        keys = set()
        for component in self.components['components']:
            with self.subTest(component=component['name']):
                self.assertEqual(set(component), fields)
                role = component['role']
                expected_key = EXPECTED_IMAGE_KEYS.get(role, component['name'])
                self.assertEqual(component['image_key'], expected_key)
                keys.add(component['image_key'])
                self.assertNotIn('image', component)
                self.assertNotIn('initContainers', component)
                self.assertNotIn('host_network', component)
                for port in component['ports']:
                    self.assertEqual(set(port), {
                        'name', 'container_port', 'service_port',
                        'protocol', 'node_port',
                    })
                    self.assertRegex(port['name'], r'^[a-z][a-z0-9-]*$')
                    self.assertIn(port['protocol'], ('TCP', 'UDP'))
                    for field in ('container_port', 'service_port'):
                        self.assertIs(type(port[field]), int)
                        self.assertGreater(port[field], 0)
                        self.assertLessEqual(port[field], 65535)
        self.assertEqual(keys, {
            'consul', 'hotel-reservation', 'jaeger', 'memcached', 'mongodb',
        })

    def test_application_executables_environment_and_original_private_ports(self):
        for name, number in APPLICATION_PORTS.items():
            with self.subTest(component=name):
                component = self.by_name[name]
                self.assertEqual(component['command'], [f'/go/bin/{name}'])
                self.assertEqual(component['environment'], APPLICATION_ENVIRONMENT)
                self.assertEqual(component['ports'], [{
                    'name': 'http' if name == 'frontend' else 'grpc',
                    'container_port': number,
                    'service_port': 8080 if name == 'frontend' else number,
                    'protocol': 'TCP',
                    'node_port': 8080 if name == 'frontend' else None,
                }])

    def test_frontend_reuses_8080_nodeport_but_preserves_5000_pod_port(self):
        frontend = self.by_name['frontend']
        self.assertEqual(frontend['service_type'], 'NodePort')
        self.assertEqual(frontend['external_traffic_policy'], 'Local')
        self.assertEqual(frontend['ports'][0]['node_port'], 8080)
        self.assertEqual(frontend['ports'][0]['service_port'], 8080)
        self.assertEqual(frontend['ports'][0]['container_port'], 5000)
        low, high = (int(item) for item in K3S_SERVICE_NODE_PORT_RANGE.split('-'))
        self.assertLessEqual(low, 8080)
        self.assertGreaterEqual(high, 8080)
        for name, component in self.by_name.items():
            if name == 'frontend':
                continue
            with self.subTest(component=name):
                self.assertEqual(component['service_type'], 'ClusterIP')
                self.assertIsNone(component['external_traffic_policy'])
                self.assertTrue(all(
                    port['node_port'] is None for port in component['ports']
                ))

    def test_eight_databases_have_separate_named_storage_subdirectories(self):
        directories = []
        for name, component in self.by_name.items():
            with self.subTest(component=name):
                if component['role'] == 'database':
                    self.assertEqual(component['storage_subdir'], name)
                    self.assertEqual(component['ports'], [{
                        'name': 'mongodb', 'container_port': 27017,
                        'service_port': 27017, 'protocol': 'TCP',
                        'node_port': None,
                    }])
                    self.assertIsNone(component['command'])
                    directories.append(component['storage_subdir'])
                else:
                    self.assertIsNone(component['storage_subdir'])
        self.assertEqual(len(set(directories)), 8)
        self.assertEqual(sorted(directories), [
            f'mongodb-{name}' for name in DATABASE_SERVICES
        ])

    def test_control_and_cache_keep_private_locked_image_defaults(self):
        self.assertEqual(self.by_name['consul']['command'], [
            'consul', 'agent', '-dev', '-client=0.0.0.0',
        ])
        self.assertEqual(self.by_name['consul']['ports'], [{
            'name': 'consul-http', 'container_port': 8500,
            'service_port': 8500, 'protocol': 'TCP', 'node_port': None,
        }])
        self.assertIsNone(self.by_name['jaeger']['command'])
        self.assertEqual(self.by_name['jaeger']['ports'], [
            {'name': 'jaeger-compact', 'container_port': 6831,
             'service_port': 6831, 'protocol': 'UDP', 'node_port': None},
            {'name': 'jaeger-admin', 'container_port': 16686,
             'service_port': 16686, 'protocol': 'TCP', 'node_port': None},
        ])
        for _, name in CACHE_SERVICES:
            with self.subTest(component=name):
                self.assertIsNone(self.by_name[name]['command'])
                self.assertEqual(self.by_name[name]['ports'], [{
                    'name': 'memcached', 'container_port': 11211,
                    'service_port': 11211, 'protocol': 'TCP', 'node_port': None,
                }])
        for component in self.by_name.values():
            if component['role'] != 'application':
                self.assertEqual(component['environment'], {})

    def test_41_sorted_edges_are_the_exact_audited_dependency_graph(self):
        observed = []
        for edge in self.policies['edges']:
            self.assertEqual(set(edge), {
                'source', 'destination', 'protocol', 'port',
            })
            self.assertIn(edge['source'], EXPECTED_ROLES)
            self.assertIn(edge['destination'], EXPECTED_ROLES)
            self.assertNotEqual(edge['source'], edge['destination'])
            destination = self.by_name[edge['destination']]
            self.assertIn(
                (edge['protocol'], edge['port']),
                {(port['protocol'], port['container_port'])
                 for port in destination['ports']},
            )
            observed.append((
                edge['source'], edge['destination'], edge['protocol'], edge['port'],
            ))
        self.assertEqual(observed, sorted(expected_edges()))
        self.assertEqual(len(observed), 41)
        self.assertEqual(len(observed), len(set(observed)))

    def test_audited_edges_include_discovery_tracing_and_both_extra_clients(self):
        edges = expected_edges()
        for name in APPLICATION_PORTS:
            self.assertIn((name, 'consul', 'TCP', 8500), edges)
            self.assertIn((name, 'jaeger', 'UDP', 6831), edges)
        self.assertIn(('frontend', 'review', 'TCP', 8088), edges)
        self.assertIn(('frontend', 'attractions', 'TCP', 8089), edges)
        self.assertFalse(any(
            source in ('consul', 'jaeger') or destination == 'frontend'
            for source, destination, _, _ in edges
        ))
        self.assertFalse(any(port == 16686 for _, _, _, port in edges))
        self.assertNotIn('dns', self.policies)
        self.assertNotIn('load_generator_ingress', self.policies)

    def test_json_duplicate_keys_are_rejected_by_fixture_reader(self):
        with self.assertRaisesRegex(ValueError, 'Duplicate JSON key'):
            json.loads(
                '{"released":false,"released":true}',
                object_pairs_hook=_unique_json_object,
            )

    def test_candidate_promotions_are_not_silently_accepted_by_the_asset_contract(self):
        for value, field in ((self.components, 'components'), (self.policies, 'edges')):
            altered = copy.deepcopy(value)
            altered['released'] = True
            with self.assertRaises(AssertionError):
                self.assert_candidate_identity(altered, field)

    def test_audit_retains_exact_original_source_hashes_and_release_blockers(self):
        anchors = re.findall(
            r'^\| `(hotelReservation/[^`]+)` \| `([0-9a-f]{64})` \|$',
            self.audit,
            re.MULTILINE,
        )
        self.assertEqual(len(anchors), 21)
        self.assertEqual(len(dict(anchors)), 21)
        self.assertEqual(dict(anchors)[
            'hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua'
        ], REQUEST_SCRIPT_SHA256)
        for fragment in (
            '**foundation candidate**', '`released: false`',
            'No correction listed here has been implemented',
            'ASCII-hex usernames', 'missing keys with `ErrCacheMiss`',
            'HTTP 200', 'eight database paths', 'anonymous digest-pull proof',
            'stable restart counts', 'room inventory',
        ):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, self.audit)

    def test_audit_documents_every_original_dataset_cardinality(self):
        expected_rows = (
            ('mongodb-geo', 'geo-db / geo', 80),
            ('mongodb-profile', 'profile-db / hotels', 80),
            ('mongodb-rate', 'rate-db / inventory', 27),
            ('mongodb-recommendation', 'recommendation-db / recommendation', 80),
            ('mongodb-user', 'user-db / user', 501),
            ('mongodb-reservation', 'reservation-db / number', 80),
            ('mongodb-reservation', 'reservation-db / reservation', 1),
            ('mongodb-review', 'review-db / reviews', 6),
            ('mongodb-attractions', 'attractions-db / hotels', 6),
            ('mongodb-attractions', 'attractions-db / restaurants', 6),
            ('mongodb-attractions', 'attractions-db / museums', 6),
        )
        for component, collection, count in expected_rows:
            with self.subTest(component=component, collection=collection):
                self.assertIn(
                    f'| `{component}` | `{collection}` | {count} |',
                    self.audit,
                )


if __name__ == '__main__':
    unittest.main()
