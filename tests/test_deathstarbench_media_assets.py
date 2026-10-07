"""Freeze the reviewed, deliberately unreleased Media workload foundation."""

from collections import Counter
import hashlib
import json
from pathlib import Path
import unittest


ASSET_ROOT = (
    Path(__file__).resolve().parents[1]
    / 'app/manifests/deathstarbench/media-microservices-v1'
)
UPSTREAM_REVISION = '6ecb09706140f8730b5385c08f1386c654c3c526'
WORKLOAD_REVISION = 'media-microservices-6ecb097-workload-v1'
NAMESPACE = 'deathstarbench-media'
COMPONENT_SHA256 = (
    '692a0e86908dabe4600cbc9b2656ab2f6ffdbaa01f38b634811ecbebb5ce0016'
)
NETWORK_SHA256 = (
    '50ea77adfaae027ffd050352d6a3f0afec767c8cc3bbd9d6652eeb8e7343ee6e'
)

CPP_COMMANDS = {
    'cast-info-service': 'CastInfoService',
    'compose-review-service': 'ComposeReviewService',
    'movie-id-service': 'MovieIdService',
    'movie-info-service': 'MovieInfoService',
    'movie-review-service': 'MovieReviewService',
    'plot-service': 'PlotService',
    'rating-service': 'RatingService',
    'review-storage-service': 'ReviewStorageService',
    'text-service': 'TextService',
    'unique-id-service': 'UniqueIdService',
    'user-review-service': 'UserReviewService',
    'user-service': 'UserService',
}
MONGODB = frozenset({
    'cast-info-mongodb',
    'movie-id-mongodb',
    'movie-info-mongodb',
    'movie-review-mongodb',
    'plot-mongodb',
    'review-storage-mongodb',
    'user-mongodb',
    'user-review-mongodb',
})
MEMCACHED = frozenset({
    'cast-info-memcached',
    'compose-review-memcached',
    'movie-id-memcached',
    'movie-info-memcached',
    'plot-memcached',
    'review-storage-memcached',
    'user-memcached',
})
REDIS = frozenset({
    'movie-review-redis',
    'rating-redis',
    'user-review-redis',
})
APPLICATIONS = frozenset({*CPP_COMMANDS, 'nginx-web-server'})
ALL_COMPONENTS = frozenset({
    *APPLICATIONS, *MONGODB, *MEMCACHED, *REDIS, 'jaeger',
})

# Independent reviewed adjacency inventory, not inferred from asset contents.
# This includes initializer RPCs as well as compose-review measurement RPCs.
DEPENDENCIES = {
    'cast-info-service': ('cast-info-memcached', 'cast-info-mongodb'),
    'compose-review-service': (
        'compose-review-memcached',
        'movie-review-service',
        'review-storage-service',
        'user-review-service',
    ),
    'movie-id-service': (
        'compose-review-service',
        'movie-id-memcached',
        'movie-id-mongodb',
        'rating-service',
    ),
    'movie-info-service': ('movie-info-memcached', 'movie-info-mongodb'),
    'movie-review-service': (
        'movie-review-mongodb', 'movie-review-redis', 'review-storage-service',
    ),
    'nginx-web-server': (
        'cast-info-service',
        'movie-id-service',
        'movie-info-service',
        'plot-service',
        'text-service',
        'unique-id-service',
        'user-service',
    ),
    'plot-service': ('plot-memcached', 'plot-mongodb'),
    'rating-service': ('compose-review-service', 'rating-redis'),
    'review-storage-service': (
        'review-storage-memcached', 'review-storage-mongodb',
    ),
    'text-service': ('compose-review-service',),
    'unique-id-service': ('compose-review-service',),
    'user-review-service': (
        'review-storage-service', 'user-review-mongodb', 'user-review-redis',
    ),
    'user-service': (
        'compose-review-service', 'user-memcached', 'user-mongodb',
    ),
}


def port_spec(name, port, protocol='TCP', node_port=None):
    return {
        'container_port': port,
        'name': name,
        'node_port': node_port,
        'protocol': protocol,
        'service_port': port,
    }


class DeathStarBenchMediaAssetsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.components = json.loads((ASSET_ROOT / 'components.json').read_text())
        cls.network = json.loads((ASSET_ROOT / 'network-policies.json').read_text())
        cls.by_name = {
            value['name']: value for value in cls.components['components']
        }

    def test_candidate_headers_are_exact_and_fail_closed(self):
        common = {
            'namespace': NAMESPACE,
            'released': False,
            'schema_version': 2,
            'upstream_revision': UPSTREAM_REVISION,
            'workload_revision': WORKLOAD_REVISION,
        }
        for value, inventory_field in (
            (self.components, 'components'), (self.network, 'edges'),
        ):
            with self.subTest(asset=inventory_field):
                self.assertEqual(set(value), {*common, inventory_field})
                self.assertEqual(
                    {key: value[key] for key in common}, common,
                )
                self.assertIs(value['released'], False)

    def test_checked_in_bytes_match_the_reviewed_revision(self):
        for name, expected in (
            ('components.json', COMPONENT_SHA256),
            ('network-policies.json', NETWORK_SHA256),
        ):
            with self.subTest(asset=name):
                self.assertEqual(
                    hashlib.sha256((ASSET_ROOT / name).read_bytes()).hexdigest(),
                    expected,
                )

    def test_exact_inventory_roles_and_safe_commands(self):
        rows = self.components['components']
        names = [value['name'] for value in rows]
        self.assertEqual(names, sorted(ALL_COMPONENTS))
        self.assertEqual(len(names), 32)
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual(Counter(row['role'] for row in rows), {
            'application': 13, 'database': 8, 'cache': 10, 'control': 1,
        })
        self.assertNotIn('dns-media', self.by_name)
        self.assertNotIn('page-service', self.by_name)
        self.assertNotIn('load-generator', self.by_name)
        self.assertEqual({row['image_key'] for row in rows}, {
            'jaeger', 'media-microservices', 'memcached', 'mongodb',
            'nginx-web-server', 'redis',
        })
        expected_fields = {
            'command', 'environment', 'external_traffic_policy', 'image_key',
            'name', 'ports', 'role', 'service_type', 'storage_subdir',
        }
        for name, row in self.by_name.items():
            with self.subTest(component=name):
                self.assertEqual(set(row), expected_fields)
                self.assertEqual(
                    row['command'],
                    [CPP_COMMANDS[name]] if name in CPP_COMMANDS else None,
                )
                self.assertEqual(row['environment'], (
                    {'fqdn_suffix': '.deathstarbench-media.svc.cluster.local'}
                    if name == 'nginx-web-server' else
                    {'COLLECTOR_ZIPKIN_HTTP_PORT': '9411'}
                    if name == 'jaeger' else {}
                ))
                if name in APPLICATIONS:
                    self.assertEqual(row['role'], 'application')
                elif name in MONGODB:
                    self.assertEqual(row['role'], 'database')
                elif name in MEMCACHED | REDIS:
                    self.assertEqual(row['role'], 'cache')
                else:
                    self.assertEqual(row['role'], 'control')

    def test_exact_storage_and_port_contracts(self):
        for name, row in self.by_name.items():
            with self.subTest(component=name):
                self.assertEqual(
                    row['storage_subdir'], name if name in MONGODB else None,
                )
                self.assertEqual(
                    row['service_type'],
                    'NodePort' if name == 'nginx-web-server' else 'ClusterIP',
                )
                self.assertEqual(
                    row['external_traffic_policy'],
                    'Local' if name == 'nginx-web-server' else None,
                )
                if name in CPP_COMMANDS:
                    expected_image = 'media-microservices'
                    expected_ports = [port_spec('thrift', 9090)]
                elif name in MONGODB:
                    expected_image = 'mongodb'
                    expected_ports = [port_spec('mongodb', 27017)]
                elif name in MEMCACHED:
                    expected_image = 'memcached'
                    expected_ports = [port_spec('memcached', 11211)]
                elif name in REDIS:
                    expected_image = 'redis'
                    expected_ports = [port_spec('redis', 6379)]
                elif name == 'nginx-web-server':
                    expected_image = 'nginx-web-server'
                    expected_ports = [port_spec('http', 8080, node_port=8080)]
                else:
                    expected_image = 'jaeger'
                    expected_ports = [
                        port_spec('jaeger-compact', 6831, protocol='UDP'),
                        port_spec('jaeger-admin', 16686),
                    ]
                self.assertEqual(row['image_key'], expected_image)
                self.assertEqual(row['ports'], expected_ports)

    def test_complete_dependency_and_tracing_graph(self):
        expected = set()
        for source, destinations in DEPENDENCIES.items():
            for destination in destinations:
                port = (
                    27017 if destination in MONGODB else
                    11211 if destination in MEMCACHED else
                    6379 if destination in REDIS else 9090
                )
                expected.add((source, destination, 'TCP', port))
        self.assertEqual(len(expected), 36)
        expected.update(
            (source, 'jaeger', 'UDP', 6831) for source in APPLICATIONS
        )
        rows = self.network['edges']
        actual = []
        for row in rows:
            self.assertEqual(set(row), {
                'destination', 'port', 'protocol', 'source',
            })
            actual.append((
                row['source'], row['destination'], row['protocol'], row['port'],
            ))
        self.assertEqual(actual, sorted(expected))
        self.assertEqual(len(actual), 49)
        self.assertEqual(len(actual), len(set(actual)))

    def test_every_edge_targets_an_existing_exact_service_port(self):
        for edge in self.network['edges']:
            with self.subTest(edge=edge):
                self.assertIn(edge['source'], APPLICATIONS)
                self.assertIn(edge['destination'], ALL_COMPONENTS)
                destination = self.by_name[edge['destination']]
                self.assertIn(
                    (edge['protocol'], edge['port']),
                    {(port['protocol'], port['service_port'])
                     for port in destination['ports']},
                )
                self.assertNotEqual(edge['source'], edge['destination'])
        # No broad namespace or admin/Zipkin edge is hidden in the contract.
        self.assertNotIn(16686, {row['port'] for row in self.network['edges']})
        self.assertNotIn(9411, {row['port'] for row in self.network['edges']})

    def test_compose_callbacks_and_initializer_paths_are_retained(self):
        edges = {
            (row['source'], row['destination'], row['protocol'], row['port'])
            for row in self.network['edges']
        }
        for callback in (
            'movie-id-service', 'rating-service', 'text-service',
            'unique-id-service', 'user-service',
        ):
            self.assertIn(
                (callback, 'compose-review-service', 'TCP', 9090), edges,
            )
        for initializer_target in (
            'cast-info-service', 'movie-id-service', 'movie-info-service',
            'plot-service', 'user-service',
        ):
            self.assertIn(
                ('nginx-web-server', initializer_target, 'TCP', 9090), edges,
            )


if __name__ == '__main__':
    unittest.main()
