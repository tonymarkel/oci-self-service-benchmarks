import ast
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from app import deathstarbench_workload_contract as workloads
from app.deathstarbench_contract import (
    DISTRIBUTED_DATASET_REVISION,
    DISTRIBUTED_IMAGE_SET_REVISION,
    DISTRIBUTED_LOAD_DRIVER_REVISION,
    DISTRIBUTED_MEASUREMENT_REVISION,
    DISTRIBUTED_WORKLOAD_REVISION,
)
from app.deathstarbench_k3s_workload import (
    ASSET_DIRECTORY,
    EXPECTED_COMPONENTS,
    FRONTEND_COMPONENT,
    FRONTEND_NODE_PORT,
    LOAD_DRIVER_REQUEST_SCRIPT_SHA256,
    NAMESPACE,
    REQUIRED_IMAGE_KEYS,
    UPSTREAM_REVISION,
    WORKLOAD_LABEL,
)


class DistributedDeathStarBenchWorkloadRegistryTests(unittest.TestCase):
    def test_registry_contains_three_exact_distinct_workload_profiles(self):
        profiles = workloads.supported_distributed_workloads()
        self.assertEqual(
            tuple(profile.workload_id for profile in profiles),
            ('social_network', 'hotel_reservation', 'media_microservices'),
        )
        for field in (
            'workload_id', 'namespace', 'workload_label', 'workload_revision',
            'image_set_revision', 'dataset_revision', 'load_driver_revision',
            'measurement_revision', 'request_script_path', 'asset_directory',
        ):
            with self.subTest(field=field):
                self.assertEqual(len({getattr(profile, field) for profile in profiles}), 3)
        for profile in profiles:
            self.assertIs(workloads.distributed_workload_profile(profile.workload_id), profile)

    def test_only_social_network_is_selectable(self):
        self.assertEqual(
            workloads.selectable_distributed_workloads(),
            (workloads.SOCIAL_NETWORK_PROFILE,),
        )
        self.assertIs(
            workloads.require_released_distributed_workload('social_network'),
            workloads.SOCIAL_NETWORK_PROFILE,
        )
        for profile in (
            workloads.HOTEL_RESERVATION_PROFILE,
            workloads.MEDIA_MICROSERVICES_PROFILE,
        ):
            with self.subTest(workload=profile.workload_id):
                self.assertIs(profile.released, False)
                with self.assertRaisesRegex(ValueError, rf'{profile.name} is not released'):
                    workloads.require_released_distributed_workload(profile.workload_id)

    def test_candidates_cannot_be_released_by_changing_asset_flags(self):
        # Registry resolution does not read or trust a component's release flag.
        with patch.object(Path, 'read_bytes', return_value=b'{"released":true}') as read:
            for workload_id in ('hotel_reservation', 'media_microservices'):
                with self.subTest(workload=workload_id):
                    with self.assertRaisesRegex(ValueError, 'qualification are not complete'):
                        workloads.require_released_distributed_workload(workload_id)
            self.assertEqual(
                workloads.selectable_distributed_workloads(),
                (workloads.SOCIAL_NETWORK_PROFILE,),
            )
        read.assert_not_called()

    def test_unknown_malformed_or_aliased_ids_fail_closed(self):
        for workload_id in (
            'unknown', '', 'SOCIAL_NETWORK', 'social-network', 'social_network ',
            ' hotel_reservation', None, 1, [], {},
        ):
            with self.subTest(workload=workload_id):
                with self.assertRaisesRegex(ValueError, 'Unsupported distributed'):
                    workloads.distributed_workload_profile(workload_id)
                with self.assertRaisesRegex(ValueError, 'Unsupported distributed'):
                    workloads.require_released_distributed_workload(workload_id)

    def test_profile_registry_and_nested_placement_are_immutable(self):
        profile = workloads.HOTEL_RESERVATION_PROFILE
        with self.assertRaises(FrozenInstanceError):
            profile.released = True
        with self.assertRaises(FrozenInstanceError):
            profile.required_image_keys = ('unexpected',)
        with self.assertRaises(TypeError):
            profile.required_image_keys[0] = 'unexpected'
        with self.assertRaises(TypeError):
            profile.expected_component_placement['frontend'] = 'database'
        with self.assertRaises(TypeError):
            workloads.DISTRIBUTED_WORKLOAD_PROFILES['hotel_reservation'] = (
                workloads.SOCIAL_NETWORK_PROFILE
            )

    def test_constructed_profile_snapshots_caller_owned_mutable_inputs(self):
        source_placement = dict(workloads.HOTEL_RESERVATION_PROFILE.expected_component_placement)
        source_image_keys = list(workloads.HOTEL_RESERVATION_PROFILE.required_image_keys)
        profile = replace(
            workloads.HOTEL_RESERVATION_PROFILE,
            expected_component_placement=source_placement,
            required_image_keys=source_image_keys,
        )
        source_placement['frontend'] = 'database'
        source_image_keys.append('unreviewed')
        self.assertEqual(profile.expected_component_placement['frontend'], 'application')
        self.assertNotIn('unreviewed', profile.required_image_keys)
        with self.assertRaises(TypeError):
            profile.expected_component_placement['frontend'] = 'database'

    def test_social_network_identity_matches_existing_qualified_aliases(self):
        social = workloads.SOCIAL_NETWORK_PROFILE
        self.assertEqual(social.namespace, NAMESPACE)
        self.assertEqual(social.workload_label, WORKLOAD_LABEL)
        self.assertEqual(social.asset_directory, ASSET_DIRECTORY)
        self.assertEqual(social.workload_revision, DISTRIBUTED_WORKLOAD_REVISION)
        self.assertEqual(social.image_set_revision, DISTRIBUTED_IMAGE_SET_REVISION)
        self.assertEqual(social.dataset_revision, DISTRIBUTED_DATASET_REVISION)
        self.assertEqual(social.load_driver_revision, DISTRIBUTED_LOAD_DRIVER_REVISION)
        self.assertEqual(social.measurement_revision, DISTRIBUTED_MEASUREMENT_REVISION)
        self.assertEqual(social.frontend_component, FRONTEND_COMPONENT)
        self.assertEqual(social.frontend_node_port, FRONTEND_NODE_PORT)
        self.assertEqual(social.required_image_keys, REQUIRED_IMAGE_KEYS)
        self.assertEqual(social.request_script_sha256, LOAD_DRIVER_REQUEST_SCRIPT_SHA256)
        self.assertEqual(set(social.expected_component_placement), EXPECTED_COMPONENTS)
        self.assertEqual(workloads.UPSTREAM_REVISION, UPSTREAM_REVISION)

    def test_frontend_port_mapping_retains_existing_cloud_ingress_contract(self):
        for profile in workloads.supported_distributed_workloads():
            with self.subTest(workload=profile.workload_id):
                self.assertEqual(profile.frontend_node_port, 8080)
                self.assertEqual(profile.frontend_service_port, 8080)
                self.assertEqual(
                    profile.expected_component_placement[profile.frontend_component],
                    'application',
                )
        self.assertEqual(workloads.HOTEL_RESERVATION_PROFILE.frontend_container_port, 5000)
        self.assertEqual(workloads.SOCIAL_NETWORK_PROFILE.frontend_container_port, 8080)
        self.assertEqual(workloads.MEDIA_MICROSERVICES_PROFILE.frontend_container_port, 8080)

    def test_all_asset_paths_are_repo_anchored_and_match_exact_byte_hashes(self):
        root = Path(workloads.__file__).resolve().parent / 'manifests' / 'deathstarbench'
        expected_directories = {
            'social_network': 'social-network-v1',
            'hotel_reservation': 'hotel-reservation-v1',
            'media_microservices': 'media-microservices-v1',
        }
        for profile in workloads.supported_distributed_workloads():
            with self.subTest(workload=profile.workload_id):
                self.assertEqual(profile.asset_directory, root / expected_directories[profile.workload_id])
                self.assertTrue(profile.asset_directory.is_absolute())
                for filename, expected_digest in (
                    ('components.json', profile.component_asset_sha256),
                    ('network-policies.json', profile.network_policy_asset_sha256),
                ):
                    self.assertRegex(expected_digest, r'^[0-9a-f]{64}$')
                    self.assertEqual(
                        hashlib.sha256((profile.asset_directory / filename).read_bytes()).hexdigest(),
                        expected_digest,
                    )

    def test_profiles_bind_exact_checked_in_component_roles_images_and_frontend(self):
        for profile in workloads.supported_distributed_workloads():
            with self.subTest(workload=profile.workload_id):
                components = json.loads((profile.asset_directory / 'components.json').read_bytes())
                self.assertEqual(components['namespace'], profile.namespace)
                self.assertEqual(components['workload_revision'], profile.workload_revision)
                self.assertEqual(
                    {component['name']: component['role'] for component in components['components']},
                    dict(profile.expected_component_placement),
                )
                self.assertEqual(
                    {component['image_key'] for component in components['components']},
                    set(profile.required_image_keys),
                )
                frontend = next(
                    component for component in components['components']
                    if component['name'] == profile.frontend_component
                )
                self.assertEqual(frontend['service_type'], 'NodePort')
                self.assertEqual(frontend['external_traffic_policy'], 'Local')
                self.assertEqual(len(frontend['ports']), 1)
                port = frontend['ports'][0]
                self.assertEqual(port['container_port'], profile.frontend_container_port)
                self.assertEqual(port['service_port'], profile.frontend_service_port)
                self.assertEqual(port['node_port'], profile.frontend_node_port)

    def test_component_counts_and_roles_match_reviewed_workload_graphs(self):
        expected_counts = {
            'social_network': {'application': 13, 'cache': 7, 'database': 6, 'control': 1},
            'hotel_reservation': {'application': 10, 'cache': 4, 'database': 8, 'control': 2},
            'media_microservices': {'application': 13, 'cache': 10, 'database': 8, 'control': 1},
        }
        for profile in workloads.supported_distributed_workloads():
            with self.subTest(workload=profile.workload_id):
                roles = list(profile.expected_component_placement.values())
                self.assertEqual(set(roles), set(expected_counts[profile.workload_id]))
                for role, count in expected_counts[profile.workload_id].items():
                    self.assertEqual(roles.count(role), count)
                self.assertEqual(profile.required_image_keys, tuple(sorted(profile.required_image_keys)))
                self.assertEqual(len(set(profile.required_image_keys)), len(profile.required_image_keys))

    def test_candidate_source_script_identities_are_original_pinned_inputs(self):
        expected_scripts = {
            'hotel_reservation': (
                'hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua',
                'af6230aaaf220fceaef6208d416c5a9d75f15f66d70e3cde00b6e441957bf352',
            ),
            'media_microservices': (
                'mediaMicroservices/wrk2/scripts/media-microservices/compose-review.lua',
                '147fdc983955c46a06c80ab41c188e0d946b37eb45e019a1f0a9e3073017bbaa',
            ),
        }
        for workload_id, (script_path, digest) in expected_scripts.items():
            with self.subTest(workload=workload_id):
                profile = workloads.distributed_workload_profile(workload_id)
                self.assertEqual(profile.request_script_path, script_path)
                self.assertEqual(profile.request_script_sha256, digest)
                self.assertIn('-candidate-', profile.load_driver_revision)
                self.assertIn('-candidate-', profile.measurement_revision)

    def test_metadata_is_portable_distinct_and_has_no_fabricated_social_dataset_fields(self):
        for profile in workloads.supported_distributed_workloads():
            with self.subTest(workload=profile.workload_id):
                metadata = profile.metadata()
                self.assertEqual(metadata['workload_id'], profile.workload_id)
                self.assertEqual(metadata['workload_namespace'], profile.namespace)
                self.assertEqual(metadata['upstream_revision'], UPSTREAM_REVISION)
                self.assertNotIn('asset_directory', metadata)
                self.assertNotIn('released', metadata)
                self.assertNotIn(str(profile.asset_directory), json.dumps(metadata))
                self.assertEqual(json.loads(json.dumps(metadata)), metadata)
                metadata['workload_id'] = 'unexpected'
                self.assertEqual(profile.metadata()['workload_id'], profile.workload_id)
        for profile in (workloads.HOTEL_RESERVATION_PROFILE, workloads.MEDIA_MICROSERVICES_PROFILE):
            self.assertNotIn('socfb', profile.dataset_revision)
            for field in ('dataset_graph', 'dataset_nodes_sha256', 'dataset_edges_sha256'):
                self.assertNotIn(field, profile.metadata())

    def test_interleaved_and_concurrent_resolution_cannot_exchange_profile_state(self):
        ids = ('hotel_reservation', 'social_network', 'media_microservices') * 10
        expected = {profile.workload_id: profile.metadata() for profile in workloads.supported_distributed_workloads()}
        before = workloads.SOCIAL_NETWORK_PROFILE.metadata()

        def resolve(workload_id):
            profile = workloads.distributed_workload_profile(workload_id)
            metadata = profile.metadata()
            self.assertEqual(metadata, expected[workload_id])
            return profile

        with ThreadPoolExecutor(max_workers=3) as executor:
            results = tuple(executor.map(resolve, ids))
        for workload_id, profile in zip(ids, results, strict=True):
            self.assertIs(profile, workloads.distributed_workload_profile(workload_id))
        self.assertEqual(workloads.SOCIAL_NETWORK_PROFILE.metadata(), before)
        self.assertEqual(workloads.selectable_distributed_workloads(), (workloads.SOCIAL_NETWORK_PROFILE,))

    def test_registry_has_no_import_of_models_execution_network_or_release_gate(self):
        source = Path(workloads.__file__).read_text(encoding='utf-8')
        tree = ast.parse(source)
        application_imports = [
            node.module for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.level
        ]
        self.assertEqual(application_imports, ['deathstarbench_contract'])


if __name__ == '__main__':
    unittest.main()
