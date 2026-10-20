"""Original-operation provenance at the actual fake Compute API boundary."""
import copy
import sys
import unittest

from tests import test_gcp_provider as base
sys.modules.setdefault('test_gcp_provider', base)
from tests.test_gcp_deathstarbench_lifecycle import candidate_clients, candidate_plan, PUBLIC_KEY
from app.providers import gcp


def strict_clients():
    clients, timeline = candidate_clients()
    for service in clients.values():
        if not isinstance(service, base.ResourceService):
            continue
        original = service.insert
        def insert(request, *, service=service, original=original):
            operation = original(request)
            item = service.items[request[service.BODY_FIELDS[service.kind]]['name']]
            operation.target_id = int(item['id'])
            operation.target_link = item['self_link']
            return operation
        service.insert = insert
    return clients, timeline


class GcpStartupOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.clients, self.timeline = strict_clients()
        self.job = {'id': '123456abcdef', 'resources': {}, 'plan': candidate_plan()}
        self.saved = []

    def provision(self, **kwargs):
        return gcp.provision_distributed_deathstarbench_candidate(self.job, candidate_plan(),
            public_key=PUBLIC_KEY, clients=self.clients, strict_create_identity=True,
            persist=lambda job: self.saved.append(copy.deepcopy(job)), **kwargs)

    def test_operation_original_target_is_saved_before_get_and_cleanup_preserves_id(self):
        original_get = self.clients['networks'].get
        def get(request):
            row = self.saved[-1]['resources']
            self.assertEqual(row['gcp_dsb_network_id'], '1000')
            self.assertTrue(row['gcp_dsb_network_create_ambiguous'])
            return original_get(request)
        self.clients['networks'].get = get
        self.provision()
        self.clients['networks'].get = original_get
        original_ids = {prefix: self.job['resources'][prefix + '_id'] for prefix in gcp.DISTRIBUTED_RESOURCE_PREFIXES}
        self.assertEqual(len(original_ids), 20)
        gcp.prepare_distributed_candidate_cleanup(self.job, clients=self.clients)
        gcp.destroy_resources(self.job, clients=self.clients, strict_cleanup=True)
        self.assertTrue(all(not client.items for client in self.clients.values() if isinstance(client, base.ResourceService)))

    def test_missing_target_metadata_refuses_without_name_adoption(self):
        original = self.clients['networks'].insert
        def insert(request):
            operation = original(request)
            del operation.target_id
            return operation
        self.clients['networks'].insert = insert
        with self.assertRaises(ValueError):
            self.provision()
        self.assertNotIn('gcp_dsb_network_id', self.job['resources'])
        self.assertEqual(len(self.clients['networks'].items), 1)

    def test_409_never_adopts_matching_name(self):
        self.clients['networks'].insert_error = base.ApiError(409, 'existing-name')
        with self.assertRaises(base.ApiError):
            self.provision()
        self.assertNotIn('gcp_dsb_network_id', self.job['resources'])
        before = len(self.timeline)
        with self.assertRaises((ValueError, RuntimeError)):
            gcp.destroy_resources(self.job, clients=self.clients, strict_cleanup=True)
        self.assertEqual(self.timeline[before:], [])

    def test_numeric_incarnation_mismatch_refuses(self):
        original = self.clients['networks'].insert
        def insert(request):
            operation = original(request)
            operation.target_id = 9999
            return operation
        self.clients['networks'].insert = insert
        with self.assertRaises((ValueError, RuntimeError)):
            self.provision()
        self.assertEqual(self.job['resources']['gcp_dsb_network_id'], '9999')
        self.assertTrue(self.job['resources']['gcp_dsb_network_create_ambiguous'])

    def test_confirmed_parent_rejection_resolves_boot_in_the_same_callback(self):
        self.clients['instances'].insert_error = base.ApiError(400, 'unsupported request')
        resolutions = []
        def resolution(job, **event):
            resolutions.append(copy.deepcopy(event))
            self.assertNotIn('gcp_dsb_control_instance_name', job['resources'])
            self.assertNotIn('gcp_dsb_control_boot_disk_name', job['resources'])
        with self.assertRaises(base.ApiError):
            self.provision(persist_create_resolution=resolution)
        self.assertEqual(len(resolutions), 1)
        self.assertEqual(resolutions[0]['resource_key'], 'gcp_dsb_control_instance')
        self.assertEqual(resolutions[0]['result'], 'insert-rejected-before-allocation')

    def test_failed_operation_matching_visible_object_never_becomes_original_identity(self):
        original = self.clients['networks'].insert
        def insert(request):
            original(request)  # A name-perfect object exists, but no target proof.
            return base.Operation(error=base.ApiError(400, 'terminal failure'))
        self.clients['networks'].insert = insert
        with self.assertRaises(base.ApiError):
            self.provision()
        self.assertNotIn('gcp_dsb_network_id', self.job['resources'])
        self.assertTrue(self.job['resources']['gcp_dsb_network_create_ambiguous'])
        self.assertEqual(len(self.clients['networks'].items), 1)

    def test_failed_operation_exact_absence_resolves_only_through_producer_hook(self):
        self.clients['networks'].insert = lambda request: base.Operation(error=base.ApiError(400, 'terminal failure'))
        resolutions = []
        with self.assertRaises(base.ApiError):
            self.provision(persist_create_resolution=lambda job, **event: resolutions.append(event))
        self.assertEqual(resolutions[0]['result'], 'terminal-operation-failed-and-exact-target-absent')
        self.assertNotIn('gcp_dsb_network_request_id', self.job['resources'])

    def test_wrong_scope_original_target_refuses_before_exact_get(self):
        original = self.clients['networks'].insert
        def insert(request):
            operation = original(request)
            operation.target_link = operation.target_link.replace('/global/', '/regions/foreign-region/')
            return operation
        self.clients['networks'].insert = insert
        self.clients['networks'].get = lambda request: self.fail('Malformed operation must refuse before GET.')
        with self.assertRaises(ValueError):
            self.provision()
        self.assertNotIn('gcp_dsb_network_id', self.job['resources'])

    def test_name_perfect_boot_disk_requires_live_original_parent_relationship(self):
        original_get = self.clients['disks'].get
        def get(request):
            disk = original_get(request)
            if request['disk'].endswith('-boot'):
                disk['users'] = []
            return disk
        self.clients['disks'].get = get
        with self.assertRaises((ValueError, RuntimeError)):
            self.provision()
        self.assertNotIn('gcp_dsb_control_boot_disk_id', self.job['resources'])


if __name__ == '__main__':
    unittest.main()
