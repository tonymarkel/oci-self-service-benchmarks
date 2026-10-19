"""Real private leased files and genuine offline factory/access capabilities."""
import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest import mock

from app import deathstarbench_hotel_startup_journal as journal
from app import deathstarbench_hotel_startup_identity as identity
from app.run_lease import acquire_run_lease, RunLeaseError
from tests import test_deathstarbench_hotel_startup_identity as fixtures
from tests.test_deathstarbench_hotel_startup_runtime import uid


ORDER = ('attractions', 'geo', 'profile', 'rate', 'recommendation',
         'reservation', 'review', 'user', 'search', 'frontend')
ROLES = fixtures.ROLES


class StartupJournalTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.StartupIdentityTests()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.plan, self.stage = self.fixture.runtime, self.fixture.stage
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.directory = self.root / self.plan.run_id
        self.directory.mkdir(mode=0o700)
        self.lease = acquire_run_lease(self.root, self.plan.run_id, 'startup-journal-test')
        self.addCleanup(self.lease.release)
        self.pods = {c: identity.validate_startup_hotel_admitted_pod(
            self.fixture.admitted(c), self.stage.pod(c), ROLES) for c in ORDER}
        self.running = self.fixture.executions()
        boundary = fixtures.support_fixtures.k3s
        self._support_inventory = fixtures.support_fixtures.live_inventory(self.plan.support_stage)
        self._support_identity = boundary._checked_inventory(self._support_inventory,
            boundary._validated_context(self.plan.support_plan, self.plan.support_stage, ROLES), complete=True)
        self._observations = {}

    def open(self, **changes):
        args = dict(plan=self.plan, stage=self.stage, lease=self.lease)
        args.update(changes)
        return journal.StartupHotelStartupJournal(**args)

    def preflight(self):
        return {'schema_version': 1, 'candidate_schema': 'hotel-startup-create-preflight-v1',
                'run_id': self.plan.run_id, 'startup_attempt_id': self.plan.startup_attempt_id,
                'initialization_attempt_id': self.plan.initialization_attempt_id,
                'plan_fingerprint': self.plan.fingerprint, 'stage_fingerprint': self.stage.fingerprint,
                **{k: 'sha256:' + 'a' * 64 for k in ('initialization_plan_fingerprint',
                    'initialization_stage_fingerprint', 'initialization_identity_fingerprint',
                    'support_identity_fingerprint')}}

    def observation(self, released=()):
        # This is the Task2 normalized consumer boundary, not live evidence.
        key = tuple(sorted(released))
        if key in self._observations:
            return copy.deepcopy(self._observations[key])
        pods = {c: copy.deepcopy(self.running[c] if c in released else self.pods[c]) for c in ORDER}
        slices, services, support, consul = self.fixture.network({c: pods[c] for c in released})
        endpoints = identity.validate_startup_hotel_endpoints(slices, services, support, pods, allow_pending=True)
        registrations = identity.validate_startup_hotel_consul_identity(
            self.fixture.consul(pods, consul, tuple(released)), self.plan, pods,
            tuple(released), consul, allow_pending=True)
        objects = {(item['kind'], item['metadata'].get('namespace'), item['metadata']['name']): item
                   for item in self._support_inventory['items'] + slices + [p['pod'] for p in pods.values()]}
        value = {'plan_fingerprint': self.plan.fingerprint, 'startup_pods': pods,
            'identity': {'nodes': copy.deepcopy(self._support_identity['nodes']),
                'objects': sorted([{'kind': item['kind'], 'name': item['metadata']['name'],
                    'namespace': item['metadata'].get('namespace'), 'uid': item['metadata']['uid']}
                    for item in objects.values()], key=lambda item: (item['kind'], item['namespace'] or '', item['name'])),
                'support_containers': copy.deepcopy(self._support_identity['support_pods']),
                'support_endpoints': {name: copy.deepcopy(row) for name, row in endpoints.items() if name not in ORDER}},
            'startup_execution_observed': bool(released), 'observed_running_components': sorted(released),
            'observed_running_count': len(released), 'execution_pending_components': [],
            'released_components': sorted(released), 'running_components': sorted(released),
            'publication_pending': {'endpoints': [], 'consul': []},
            'publication_confirmed': {'endpoints': sorted(released),
                                      'consul': sorted(set(released) - {'frontend'})},
            'endpoints': endpoints, 'consul': registrations}
        self._observations[key] = value
        return copy.deepcopy(value)

    def admit(self):
        store = self.open()
        store.record_create_intent(self.preflight())
        store.record_admitted_pods(self.observation())
        return store

    def dispatch(self, store, component, released=()):
        return store.record_dispatch_intent(component, self.observation(released), image_access=self.plan.access)

    def files(self):
        return {p.name: p.read_bytes() for p in self.directory.glob('.hotel-startup-*') if p.is_file()}

    def fresh_directory(self, label):
        self.lease.release()
        root = self.root / label
        root.mkdir(mode=0o700)
        self.directory = root / self.plan.run_id
        self.directory.mkdir(mode=0o700)
        self.lease = acquire_run_lease(root, self.plan.run_id, 'startup-journal-test')
        self.addCleanup(self.lease.release)

    def test_create_and_each_component_have_exclusive_durable_intent(self):
        # Catches missing exclusive latches, ordering, identity binding or chaining.
        store = self.admit()
        with self.assertRaises(ValueError): self.dispatch(store, 'geo')
        released = []
        for component in ORDER:
            self.dispatch(store, component, released)
            with self.assertRaises(ValueError): self.dispatch(store, component, released)
            released.append(component)
            store.record_component_observation(component, self.observation(released))
        all_ten = self.observation(released)
        for field, value in (('observed_running_components', sorted(ORDER[:-1])),
                             ('publication_confirmed', {'endpoints': sorted(ORDER), 'consul': []}),
                             ('running_components', sorted(ORDER[:-1]))):
            bad = copy.deepcopy(all_ten); bad[field] = value
            with self.subTest(final_field=field), self.assertRaises(ValueError):
                store.record_final_observation(bad)
        for label, bad in self.incomplete_publications(all_ten):
            before = self.files()
            with self.subTest(final_publication=label), self.assertRaises(ValueError):
                store.record_final_observation(bad)
            self.assertEqual(self.files(), before)
        final = store.record_final_observation(all_ten)
        self.assertEqual(final['event_count'], 24)
        self.assertEqual(final['state'], 'final-observation-recorded')
        self.assertFalse(final['journal_is_execution_permission'])
        for field in ('startup_authorized', 'application_start_allowed', 'application_started',
                      'application_ready', 'runtime_qualified', 'measurement_qualified'):
            self.assertIs(final[field], False)
        self.assertTrue(all(final[k] is False for k in journal.CLOSED))
        component_latches = list(self.directory.glob('.hotel-startup-component-*.latch'))
        create_latches = list(self.directory.glob('.hotel-startup-create.latch'))
        self.assertEqual(len(component_latches), 10)
        self.assertEqual(len(create_latches), 1)
        events = final['events']
        self.assertEqual([e['payload']['component'] for e in events if e['event'] == 'dispatch-intent-recorded'], list(ORDER))
        for index, event in enumerate(events):
            previous = None if not index else 'sha256:' + hashlib.sha256(
                (json.dumps(events[index - 1], sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()).hexdigest()
            self.assertEqual(event['sequence'], index)
            self.assertEqual(event['previous_event_sha256'], previous)
        intent = events[3]['payload']
        self.assertEqual(intent['pod_uid'], self.pods['attractions']['pod_uid'])
        self.assertEqual(intent['image_access_fingerprint'], self.plan.access.fingerprint)
        for field in ('admitted_spec', 'admitted_metadata'):
            expected = 'sha256:' + hashlib.sha256((json.dumps(self.pods['attractions'][field],
                sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()).hexdigest()
            self.assertEqual(intent[field + '_fingerprint'], expected)
        self.assertEqual(events[1]['payload']['create_manifest_sha256'], self.stage.rendered_manifest_sha256)
        for path in self.directory.glob('.hotel-startup-*'):
            self.assertLessEqual(path.stat().st_size, 1024 * 1024)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_every_reopen_refuses_even_created_only_and_keeps_evidence(self):
        # Catches accidental pre-dispatch resume from historical created state.
        store = self.open()
        for operation in (None, lambda: store.record_create_intent(self.preflight()),
                          lambda: store.record_admitted_pods(self.observation()),
                          lambda: self.dispatch(store, 'attractions')):
            if operation: operation()
            before = self.files()
            with self.assertRaises(ValueError): self.open()
            self.assertEqual(self.files(), before)

    def test_partial_fsync_or_unsafe_file_never_grants_dispatch(self):
        # Catches retry after a short write or failed durability boundary.
        real_write, real_fsync = os.write, os.fsync
        for failure in ('partial', 'file-fsync', 'directory-fsync'):
            with self.subTest(failure=failure):
                child = self.root / failure
                child.mkdir(mode=0o700)
                run = child / self.plan.run_id
                run.mkdir(mode=0o700)
                # Release the other lease before acquiring the same run elsewhere.
                self.lease.release()
                lease = acquire_run_lease(child, self.plan.run_id, 'startup-journal-test')
                self.addCleanup(lease.release)
                store = self.open(lease=lease)
                count = [0]
                def write(fd, data):
                    count[0] += 1
                    if count[0] == 1: return real_write(fd, data[:17])
                    raise OSError('simulated partial write')
                def fsync(fd):
                    directory = stat.S_ISDIR(os.fstat(fd).st_mode)
                    if directory == (failure == 'directory-fsync'): raise OSError('simulated fsync failure')
                    return real_fsync(fd)
                patch = mock.patch.object(os, 'write', side_effect=write) if failure == 'partial' else mock.patch.object(os, 'fsync', side_effect=fsync)
                with patch, self.assertRaises(OSError): store.record_create_intent(self.preflight())
                evidence = {p.name: p.read_bytes() for p in run.glob('.hotel-startup-*')}
                with self.assertRaises(ValueError): store.record_create_intent(self.preflight())
                with self.assertRaises(ValueError): self.open(lease=lease)
                self.assertEqual(evidence, {p.name: p.read_bytes() for p in run.glob('.hotel-startup-*')})
                lease.release()

    def test_orphan_or_unsafe_file_is_retained_and_refused(self):
        # Catches trusting alien latches, special files, aliases or weak modes.
        for attack in ('symlink', 'hardlink', 'fifo', 'mode', 'orphan'):
            with self.subTest(attack=attack):
                self.fresh_directory(attack)
                store = self.admit()
                event = self.directory / '.hotel-startup-event-00.json'
                if attack == 'orphan':
                    path = self.directory / '.hotel-startup-component-geo.latch'
                    path.touch(mode=0o600)
                elif attack == 'mode':
                    path = event
                    path.chmod(0o644)
                else:
                    retained = self.directory / 'retained-test-evidence'
                    event.rename(retained)
                    path = event
                    if attack == 'symlink': path.symlink_to(retained)
                    elif attack == 'hardlink': os.link(retained, path)
                    else: os.mkfifo(path, 0o600)
                before = path.lstat()
                with self.assertRaises((ValueError, OSError)): self.dispatch(store, 'attractions')
                self.assertEqual(path.lstat().st_ino, before.st_ino)
                with self.assertRaises((ValueError, OSError)): self.open()

    def test_latch_precedes_lost_event_write_and_never_allows_retry(self):
        # Catches publishing the intent event before its durable exclusive latch.
        store = self.open()
        real_write = os.write
        writes = [0]
        def lose_event(fd, data):
            writes[0] += 1
            if writes[0] == 2: raise OSError('simulated event write loss')
            return real_write(fd, data)
        with mock.patch.object(os, 'write', side_effect=lose_event), self.assertRaises(OSError):
            store.record_create_intent(self.preflight())
        latch = self.directory / '.hotel-startup-create.latch'
        self.assertEqual(json.loads(latch.read_bytes())['intent']['run_id'], self.plan.run_id)
        event = self.directory / '.hotel-startup-event-01.json'
        self.assertEqual(event.read_bytes(), b'')
        before = self.files()
        with self.assertRaises(ValueError): store.record_create_intent(self.preflight())
        with self.assertRaises(ValueError): self.open()
        self.assertEqual(self.files(), before)

    def test_copied_forked_and_released_lease_cannot_write(self):
        # Catches capability reconstruction, process inheritance or release reuse.
        store = self.admit()
        before = self.files()
        with self.assertRaises(RunLeaseError): self.open(lease=copy.copy(self.lease))
        read, write = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read)
            try: self.dispatch(store, 'attractions')
            except (ValueError, RunLeaseError): os.write(write, b'refused')
            finally: os._exit(0)
        os.close(write)
        self.assertEqual(os.read(read, 32), b'refused')
        os.close(read)
        os.waitpid(pid, 0)
        self.assertEqual(self.files(), before)
        self.lease.release()
        with self.assertRaises(RunLeaseError): self.dispatch(store, 'attractions')
        self.assertEqual(self.files(), before)

    def test_original_context_cannot_be_rebound_after_creation(self):
        # Catches mixing original history with a new otherwise-genuine typed plan.
        from app.deathstarbench_hotel_startup_stage import build_startup_hotel_startup_stage
        store = self.open()
        before = self.files()
        store.plan = self.fixture.plan(startup_attempt_id=uid(888))
        store.stage = build_startup_hotel_startup_stage(store.plan)
        with self.assertRaises(ValueError): store.metadata()
        self.assertEqual(self.files(), before)
        store.plan, store.stage = self.plan, self.stage
        self.fresh_directory('lease-rebind')
        for name, raw in before.items():
            path = self.directory / name
            path.write_bytes(raw)
            path.chmod(0o600)
        store.lease = self.lease
        with self.assertRaises((ValueError, RunLeaseError)): store.metadata()
        self.assertEqual(self.files(), before)

    def test_raw_status_and_http_payloads_are_not_persisted(self):
        # Catches leaking arbitrary status strings/signed URLs into public history.
        store = self.open()
        store.record_create_intent(self.preflight())
        observed = self.observation()
        secret = 'https://registry.example/blob?X-Amz-Signature=DO-NOT-RETAIN'
        observed['startup_pods']['geo']['pod']['status']['message'] = secret
        observed['untrusted_http_payload'] = secret
        store.record_admitted_pods(observed)
        self.assertNotIn(b'DO-NOT-RETAIN', b''.join(self.files().values()))
        self.assertNotIn('DO-NOT-RETAIN', json.dumps(store.metadata()))

    def test_admissions_and_publications_need_exact_component_sets(self):
        # Catches absent admissions and promoting a partial runtime bool to readiness.
        store = self.open()
        store.record_create_intent(self.preflight())
        bad = self.observation(); bad['startup_pods'].pop('geo')
        with self.assertRaises(ValueError): store.record_admitted_pods(bad)
        store.record_admitted_pods(self.observation())
        self.dispatch(store, 'attractions')
        for field, value in (('observed_running_components', []),
                             ('publication_confirmed', {'endpoints': [], 'consul': []}),
                             ('execution_pending_components', ['attractions'])):
            bad = self.observation(['attractions']); bad[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                store.record_component_observation('attractions', bad)
        store.record_component_observation('attractions', self.observation(['attractions']))
        with self.assertRaises(ValueError): store.record_final_observation(self.observation(['attractions']))

    @staticmethod
    def incomplete_publications(observation):
        # Mutations of real normalized rows, not an invented wire schema.
        mutations = {
            'endpoint-missing-service': lambda o: o['endpoints']['attractions'].pop('service_uid'),
            'endpoint-missing-slice': lambda o: o['endpoints']['attractions'].pop('endpoint_slice_uid'),
            'endpoint-forged-service': lambda o: o['endpoints']['attractions'].update(service_uid=uid(701)),
            'endpoint-crossed-service': lambda o: o['endpoints']['attractions'].update(service_uid=o['endpoints']['geo']['service_uid']),
            'endpoint-forged-slice': lambda o: o['endpoints']['attractions'].update(endpoint_slice_uid=uid(702)),
            'endpoint-wrong-kind': lambda o: o['endpoints']['attractions'].update(endpoint_slice_uid=o['startup_pods']['attractions']['pod_uid']),
            'endpoint-slice-alias': lambda o: o['endpoints']['geo'].update(endpoint_slice_uid=o['endpoints']['attractions']['endpoint_slice_uid']),
            'endpoint-slice-wrong-namespace': lambda o: next(item for item in o['identity']['objects']
                if item['uid'] == o['endpoints']['attractions']['endpoint_slice_uid']).update(namespace='wrong-namespace'),
            'endpoint-extra-field': lambda o: o['endpoints']['attractions'].update(ready=True),
            'source-objects-missing': lambda o: o['identity'].pop('objects'),
            'source-extra-slice': lambda o: o['identity']['objects'].append({'kind': 'EndpointSlice',
                'namespace': 'deathstarbench-hotel', 'name': 'unexpected-slice', 'uid': uid(703)}),
            'source-support-execution-missing': lambda o: o['identity'].pop('support_containers'),
            'source-support-endpoints-missing': lambda o: o['identity'].pop('support_endpoints'),
            'consul-missing-id': lambda o: o['consul']['backend_registrations']['attractions'].pop('service_id'),
            'consul-forged-id': lambda o: o['consul']['backend_registrations']['attractions'].update(service_id='forged-id'),
            'consul-missing-port': lambda o: o['consul']['backend_registrations']['attractions'].pop('port'),
            'consul-wrong-port': lambda o: o['consul']['backend_registrations']['attractions'].update(port=8083),
            'consul-float-port': lambda o: o['consul']['backend_registrations']['attractions'].update(port=8089.0),
            'consul-pending-row': lambda o: o['consul']['backend_registrations']['attractions'].update(publication_pending=True),
            'consul-pending-contradiction': lambda o: o['consul'].update(publication_pending=['attractions']),
            'consul-pending-missing': lambda o: o['consul'].pop('publication_pending'),
            'consul-extra-registration': lambda o: o['consul']['backend_registrations'].update(
                frontend=copy.deepcopy(o['consul']['backend_registrations']['attractions'])),
            'intrinsic-missing-container': lambda o: o['consul']['intrinsic_consul'].pop('container_id'),
            'intrinsic-forged-container': lambda o: o['consul']['intrinsic_consul'].update(container_id='containerd://' + 'e' * 64),
            'intrinsic-forged-address': lambda o: o['consul']['intrinsic_consul'].update(pod_ip='10.42.2.250'),
        }
        for label, mutate in mutations.items():
            changed = copy.deepcopy(observation)
            mutate(changed)
            yield label, changed

    def test_component_rejects_incomplete_or_contradictory_normalized_publication(self):
        # Catches confirmed lists promoting truncated/forged/pending rows to history.
        store = self.admit()
        self.dispatch(store, 'attractions')
        good = self.observation(['attractions'])
        for label, bad in self.incomplete_publications(good):
            before = self.files()
            with self.subTest(component_publication=label), self.assertRaises(ValueError):
                store.record_component_observation('attractions', bad)
            self.assertEqual(self.files(), before)
        store.record_component_observation('attractions', good)

    def test_component_rejects_forged_consul_service_identity(self):
        # Catches accepting UID/IP-only registration rows with the wrong run ID.
        store = self.admit()
        self.dispatch(store, 'attractions')
        bad = self.observation(['attractions'])
        bad['consul']['backend_registrations']['attractions']['service_id'] = 'forged-id'
        before = self.files()
        with self.assertRaises(ValueError): store.record_component_observation('attractions', bad)
        self.assertEqual(self.files(), before)

    def test_component_rejects_contradictory_consul_pending(self):
        # Catches confirmed summaries hiding the producer's explicit pending set.
        store = self.admit()
        self.dispatch(store, 'attractions')
        bad = self.observation(['attractions'])
        bad['consul']['publication_pending'] = ['attractions']
        before = self.files()
        with self.assertRaises(ValueError): store.record_component_observation('attractions', bad)
        self.assertEqual(self.files(), before)

    def test_crossed_identity_and_access_never_consume_intent(self):
        # Catches caller hash substitution, UID/spec drift and summary access adoption.
        store = self.open()
        for field, value in (('initialization_attempt_id', uid(99)), ('plan_fingerprint', 'sha256:' + 'b' * 64),
                             ('support_identity_fingerprint', 'not-a-hash'), ('create_manifest_sha256', 'sha256:' + 'b' * 64)):
            bad = self.preflight(); bad[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): store.record_create_intent(bad)
        store.record_create_intent(self.preflight())
        store.record_admitted_pods(self.observation())
        before = self.files()
        with self.assertRaises(ValueError): store.record_dispatch_intent('attractions', self.observation(), image_access=self.plan.access.metadata())
        for change in ('uid', 'spec', 'released'):
            bad = self.observation()
            if change == 'uid': bad['startup_pods']['attractions']['pod']['metadata']['uid'] = uid(777)
            elif change == 'spec': bad['startup_pods']['attractions']['pod']['spec']['containers'][0]['env'][0]['value'] = 'crossed'
            else: bad['released_components'] = ['attractions']
            with self.subTest(change=change), self.assertRaises(ValueError): self.dispatch_bad(store, bad)
        self.assertEqual(self.files(), before)

    def dispatch_bad(self, store, observation):
        return store.record_dispatch_intent('attractions', observation, image_access=self.plan.access)

    def test_tampered_partial_or_oversized_record_stays_unchanged(self):
        # Catches accepting damaged records or silently truncating/repairing evidence.
        for index in range(4):
            self.fresh_directory('damage-' + str(index))
            store = self.admit()
            path = self.directory / '.hotel-startup-event-02.json'
            original = path.read_bytes()
            changed = json.loads(original)
            changed['previous_event_sha256'] = 'sha256:' + 'b' * 64
            forged = (json.dumps(changed, sort_keys=True, separators=(',', ':')) + '\n').encode()
            damaged = (original[:-1], b'{', b'x' * (1024 * 1024 + 1), forged)[index]
            path.write_bytes(damaged)
            with self.assertRaises(ValueError): self.dispatch(store, 'attractions')
            with self.assertRaises(ValueError): self.open()
            self.assertEqual(path.read_bytes(), damaged)
