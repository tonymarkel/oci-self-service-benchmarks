"""Offline controller integration: real factories, validators, leases and journals.

Only SSH/cloud/registry/source-native fixture boundaries are simulated. This is
not evidence of live cloud, Kubernetes, application or benchmark qualification.
"""
import ast
import base64
import copy
from contextlib import nullcontext, redirect_stdout
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import shlex
import selectors
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import zlib
from unittest import mock

from app import deathstarbench_candidate_deployment as support
from app import deathstarbench_candidate_k3s as k3s
from app import deathstarbench_distributed as distributed
from app import deathstarbench_hotel_live_controller as initializer
from app import deathstarbench_hotel_startup_controller as controller
from app import deathstarbench_hotel_startup_k3s as live
from app.deathstarbench_candidate_storage import parse_candidate_database_storage_attestation
from app.deathstarbench_candidate_storage_observation import parse_candidate_database_storage_observation
from app.deathstarbench_hotel_startup_runtime import create_startup_hotel_runtime_plan, validate_startup_hotel_runtime_plan
from app.deathstarbench_hotel_runtime import create_guarded_hotel_runtime_plan
from app.deathstarbench_hotel_initialization_stage import build_guarded_hotel_initialization_stage
from app.deathstarbench_hotel_startup_stage import build_startup_hotel_startup_stage, validate_startup_hotel_startup_stage
from app.run_lease import acquire_run_lease
from tests import test_deathstarbench_candidate_storage as storage
from tests import test_deathstarbench_candidate_storage_observation as storage_read
from tests import test_deathstarbench_distributed_runtime as cluster_fixture
from tests import test_deathstarbench_hotel_image_access as access_fixture
from tests import test_deathstarbench_hotel_live_k3s as init_fixture
from tests import test_deathstarbench_hotel_startup_k3s as observation_fixture
from tests import test_deathstarbench_hotel_initialization_artifacts as guarded_fixture

ORDER = ('attractions', 'geo', 'profile', 'rate', 'recommendation',
         'reservation', 'review', 'user', 'search', 'frontend')
ROLES = observation_fixture.ROLES


_MATRIX_KEYS = tuple('boundary:' + component + ':' + boundary for component in ORDER
                     for boundary in ('intent-persist', 'release-request', 'lost-response')) + tuple(
    'drift:' + boundary for boundary in ('source', 'capability', 'lease', 'route', 'cloud', 'storage', 'support', 'namespace'))


def _matrix_result(raw, key):
    """Untrusted operational child result, never application proof."""
    def pairs(rows):
        value = {}
        for name, item in rows:
            if name in value: raise ValueError('Duplicate matrix result field.')
            value[name] = item
        return value
    def nonfinite(_value): raise ValueError('Nonfinite matrix result.')
    try:
        result = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=nonfinite)
        if (not isinstance(result, dict) or set(result) != {'schema_version', 'case_key', 'outcome', 'elapsed_seconds'}
                or type(result['schema_version']) is not int or result['schema_version'] != 1
                or result['case_key'] != key or result['outcome'] != 'passed'
                or type(result['elapsed_seconds']) not in (int, float)
                or not math.isfinite(result['elapsed_seconds']) or not 0 <= result['elapsed_seconds'] <= 900):
            raise ValueError('Incomplete or unrelated matrix result.')
        return result['elapsed_seconds']
    except (UnicodeError, RecursionError, TypeError, ValueError):
        raise ValueError('Invalid required matrix child result: ' + key) from None


def _run_startup_matrix(keys=_MATRIX_KEYS, *, command=None, child_timeout=900, matrix_timeout=4500):
    """At most three fresh interpreters; every selected cell must pass once."""
    keys = tuple(keys)
    if not keys or len(set(keys)) != len(keys) or not set(keys) <= set(_MATRIX_KEYS):
        raise ValueError('Required matrix selection is empty, duplicated or unknown.')
    if not 0 < child_timeout <= 900 or not 0 < matrix_timeout <= 4500:
        raise ValueError('Required matrix watchdog is invalid.')
    if command is None:
        def command(key):
            return [sys.executable, '-c', 'import sys;from tests.test_deathstarbench_hotel_startup_controller '
                    'import _matrix_child_main;_matrix_child_main(sys.argv[1])', key]
    deadline = time.monotonic() + matrix_timeout
    # Reserve bounded operational reaping inside the whole-matrix watchdog.
    work_deadline = deadline - min(6, matrix_timeout / 2)
    running, completed, pending = {}, {}, iter(keys)
    exhausted = False
    selector = selectors.DefaultSelector()

    def close_owned(process):
        try:
            # New session ID is exactly this original owned Popen PID. Also
            # terminate its descendants if the interpreter already exited.
            try: os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError: pass
            process.wait(timeout=min(2, max(0.001, deadline - time.monotonic())))
        finally:
            for pipe in (process.stdout, process.stderr):
                try: selector.unregister(pipe)
                except (KeyError, ValueError): pass
                pipe.close()

    try:
        while not exhausted or running:
            if time.monotonic() >= work_deadline: raise TimeoutError('Required matrix whole-run watchdog expired.')
            while len(running) < 3 and not exhausted:
                if time.monotonic() >= min([work_deadline] + [state['until'] for state in running.values()]):
                    raise TimeoutError('Required matrix watchdog expired before child dispatch.')
                try: key = next(pending)
                except StopIteration:
                    exhausted = True
                    break
                started = time.monotonic()
                process = subprocess.Popen(command(key), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                           start_new_session=True)
                running[process] = {'key': key, 'until': started + child_timeout, 'stdout': bytearray(),
                                    'stderr': bytearray(), 'open': 2, 'bytes': 0}
                for pipe, name in ((process.stdout, 'stdout'), (process.stderr, 'stderr')):
                    selector.register(pipe, selectors.EVENT_READ, (process, name))
            until = min([work_deadline] + [state['until'] for state in running.values()])
            if time.monotonic() >= until: raise TimeoutError('Required matrix child watchdog expired.')
            events = selector.select(min(0.1, until - time.monotonic()))
            if time.monotonic() >= min([work_deadline] + [state['until'] for state in running.values()]):
                raise TimeoutError('Required matrix watchdog expired after selector waiting.')
            for event, _mask in events:
                process, name = event.data
                state = running[process]
                chunk = os.read(event.fileobj.fileno(), min(4096, 65537 - state['bytes']))
                if chunk:
                    state['bytes'] += len(chunk)
                    if state['bytes'] > 65536: raise ValueError('Required matrix child output exceeded its bound.')
                    state[name].extend(chunk)
                else:
                    selector.unregister(event.fileobj)
                    state['open'] -= 1
            for process, state in list(running.items()):
                code = process.poll()
                if code is not None and code != 0:
                    raise ValueError('Required matrix child failed: ' + state['key'])
                if code is None or state['open']: continue
                if state['stderr']: raise ValueError('Required matrix child emitted unexpected diagnostics.')
                key = state['key']
                if key in completed: raise ValueError('Required matrix child result was duplicated.')
                elapsed = _matrix_result(bytes(state['stdout']), key)
                if time.monotonic() >= min([work_deadline] + [row['until'] for row in running.values()]):
                    raise TimeoutError('Required matrix watchdog expired before accepting completion.')
                completed[key] = elapsed
                close_owned(process)
                del running[process]
                print('Task4 required cell complete: %s wall=%.3fs' % (key, completed[key]), flush=True)
        if set(completed) != set(keys) or len(completed) != len(keys):
            raise ValueError('Required matrix result inventory is incomplete.')
        if time.monotonic() >= deadline: raise TimeoutError('Required matrix whole-run watchdog expired during cleanup.')
        return completed
    finally:
        try:
            failure = None
            for process in running:
                try: close_owned(process)
                except BaseException as exc: failure = failure or exc
            if failure is not None: raise failure
        finally: selector.close()


def _matrix_child_main(key):
    # Only this immutable key crosses the exec boundary. Each case runs its own
    # genuine setUp/actual initializer and unittest cleanup in this interpreter.
    if key not in _MATRIX_KEYS: raise SystemExit(2)
    case = StartupControllerTests('_required_matrix_cell')
    case.matrix_case_key = key
    result = unittest.TestResult()
    started = time.monotonic()
    case.run(result)
    passed = result.wasSuccessful() and result.testsRun == 1 and not result.skipped
    print(json.dumps({'schema_version': 1, 'case_key': key, 'outcome': 'passed' if passed else 'failed',
                      'elapsed_seconds': time.monotonic() - started}), flush=True)
    raise SystemExit(0 if passed else 1)


class StartupMatrixIntegrationTests(unittest.TestCase):
    def test_all_38_required_cells_in_fresh_interpreters(self):
        self.assertEqual(set(_run_startup_matrix()), set(_MATRIX_KEYS))


class StartupMatrixOrchestrationTests(unittest.TestCase):
    """Fast supervisor behavior only: synthetic external children, not cells."""

    def runner(self):
        runner = globals().get('_run_startup_matrix')
        self.assertIsNotNone(runner, 'Required fresh-interpreter matrix supervisor is missing.')
        return runner

    @staticmethod
    def command(raw, code=0, delay=0, stderr=b''):
        return [sys.executable, '-c',
                'import sys,time,zlib;time.sleep(float(sys.argv[3]));'
                'sys.stdout.buffer.write(zlib.decompress(bytes.fromhex(sys.argv[1])));'
                'sys.stderr.buffer.write(zlib.decompress(bytes.fromhex(sys.argv[4])));sys.exit(int(sys.argv[2]))',
                zlib.compress(raw).hex(), str(code), str(delay), zlib.compress(stderr).hex()]

    @staticmethod
    def row(key):
        return json.dumps({'schema_version': 1, 'case_key': key, 'outcome': 'passed', 'elapsed_seconds': 0.01}).encode()

    def test_all_required_cells_must_complete_once_with_at_most_three_live_children(self):
        runner = self.runner()
        # Hand-checked independent expected inventory, not a supervisor helper.
        expected = {'boundary:' + component + ':' + boundary
                    for component in ('attractions', 'geo', 'profile', 'rate', 'recommendation',
                                      'reservation', 'review', 'user', 'search', 'frontend')
                    for boundary in ('intent-persist', 'release-request', 'lost-response')}
        expected |= {'drift:' + value for value in
                     ('source', 'capability', 'lease', 'route', 'cloud', 'storage', 'support', 'namespace')}
        spawned, live_counts = [], []
        real_popen = subprocess.Popen
        def launch(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            spawned.append(process)
            live_counts.append(sum(child.poll() is None for child in spawned))
            return process
        with mock.patch('subprocess.Popen', side_effect=launch), redirect_stdout(io.StringIO()):
            completed = runner(command=lambda key: self.command(self.row(key), delay=0.04))
        self.assertEqual(set(completed), expected)
        self.assertEqual(len(completed), 38)
        self.assertEqual(len(spawned), 38)
        self.assertEqual(max(live_counts), 3)
        self.assertTrue(all(child.poll() is not None and child.stdout.closed and child.stderr.closed for child in spawned))

    def test_malformed_missing_duplicate_wrong_nonfinite_or_oversized_result_fails(self):
        runner = self.runner()
        key = 'boundary:attractions:intent-persist'
        good = self.row(key)
        rows = [b'', good[:-2], good + good, b'\xff', b'{}',
                good.replace(b'"passed"', b'"failed"'), good.replace(b'"schema_version": 1', b'"schema_version": true'),
                good.replace(key.encode(), b'drift:source'), good.replace(b'0.01', b'NaN'),
                good.replace(b'0.01', b'-1'), good.replace(b'0.01', b'901'),
                good[:-1] + b',"outcome":"passed"}', good[:-1] + b',"extra":0}', b' ' * 65537]
        for raw in rows:
            with self.subTest(output_bytes=len(raw)):
                with self.assertRaises((ValueError, AssertionError)):
                    runner((key,), command=lambda _key: self.command(raw))
        for diagnostic in (b'not-a-result', b' ' * 65537):
            with self.subTest(stderr_bytes=len(diagnostic)):
                with self.assertRaises(ValueError):
                    runner((key,), command=lambda _key: self.command(good, stderr=diagnostic))

    def test_nonzero_exit_and_watchdogs_reap_every_exact_owned_child(self):
        runner = self.runner()
        keys = ('boundary:attractions:intent-persist', 'boundary:attractions:release-request')
        for failure in ('nonzero', 'child-timeout', 'matrix-timeout'):
            with self.subTest(failure=failure):
                spawned = []
                real_popen = subprocess.Popen
                def launch(*args, **kwargs):
                    child = real_popen(*args, **kwargs)
                    spawned.append(child)
                    return child
                def command(key):
                    failing = failure == 'nonzero' and key == keys[1]
                    return self.command(self.row(key), code=7 if failing else 0,
                                        delay=0 if failing else 30)
                started = time.monotonic()
                with mock.patch('subprocess.Popen', side_effect=launch):
                    with self.assertRaises((ValueError, TimeoutError, AssertionError)):
                        runner(keys, command=command, child_timeout=0.15 if failure == 'child-timeout' else 2,
                               matrix_timeout=0.15 if failure == 'matrix-timeout' else 3)
                self.assertLess(time.monotonic() - started, 5)
                self.assertEqual(len(spawned), 2)
                self.assertTrue(all(child.poll() is not None and child.stdout.closed and child.stderr.closed for child in spawned))

    def test_invalid_required_selection_is_not_silently_skipped(self):
        runner = self.runner()
        key = 'drift:source'
        for keys in ((), (key, key), ('drift:unknown',), ('boundary:frontend:unknown',)):
            with self.subTest(selection=keys):
                with self.assertRaises(ValueError): runner(keys)

    def test_slow_launch_crossing_any_deadline_never_dispatches_next_child(self):
        runner = self.runner()
        keys = ('boundary:attractions:intent-persist', 'boundary:attractions:release-request')
        for child_limit, whole_limit in ((1, 0.2), (0.1, 2)):
            with self.subTest(child_watchdog=child_limit, whole_watchdog=whole_limit):
                spawned = []
                real_popen = subprocess.Popen
                def launch(*args, **kwargs):
                    child = real_popen(*args, **kwargs)
                    spawned.append(child)
                    time.sleep(0.15)  # Real external process-launch latency only.
                    return child
                with mock.patch('subprocess.Popen', side_effect=launch):
                    with self.assertRaises(TimeoutError):
                        runner(keys, command=lambda key: self.command(self.row(key), delay=30),
                               child_timeout=child_limit, matrix_timeout=whole_limit)
                self.assertEqual(len(spawned), 1, 'A later child was dispatched after an expired watchdog.')
                self.assertTrue(all(child.poll() is not None and child.stdout.closed and child.stderr.closed for child in spawned))


    def test_valid_child_observed_after_original_deadline_is_terminal(self):
        runner = self.runner()
        key = 'boundary:attractions:intent-persist'
        for boundary in ('selector-wait', 'completion-poll'):
            with self.subTest(boundary=boundary):
                spawned, delayed = [], []
                real_popen, real_selector = subprocess.Popen, selectors.DefaultSelector
                def launch(*args, **kwargs):
                    child = real_popen(*args, **kwargs)
                    spawned.append(child)
                    try: child.wait(timeout=1)  # Real completed process, data + EOF ready.
                    except BaseException:
                        # Independent exact fixture ownership if launch itself
                        # fails before the supervisor receives this Popen.
                        try:
                            child.kill()
                            child.wait(timeout=2)
                        finally:
                            child.stdout.close()
                            child.stderr.close()
                        raise
                    original_poll = child.poll
                    polls = [0]
                    def poll():
                        code = original_poll()
                        polls[0] += 1
                        if boundary == 'completion-poll' and polls[0] == 2:
                            delayed.append(boundary)
                            time.sleep(0.6)
                        return code
                    child.poll = poll  # External status boundary, never validator state.
                    return child
                def selector():
                    value = real_selector()
                    original_select = value.select
                    waits = [0]
                    def select(timeout=None):
                        events = original_select(timeout)
                        waits[0] += 1
                        if boundary == 'selector-wait' and waits[0] == 2:
                            delayed.append(boundary)
                            time.sleep(0.6)
                        return events
                    value.select = select  # External wait latency; real ready events.
                    return value
                rejected = False
                with mock.patch('subprocess.Popen', side_effect=launch), \
                     mock.patch('selectors.DefaultSelector', side_effect=selector), redirect_stdout(io.StringIO()):
                    try:
                        runner((key,), command=lambda _key: self.command(self.row(key)),
                               child_timeout=0.5, matrix_timeout=4)
                    except TimeoutError: rejected = True
                self.assertEqual(delayed, [boundary], 'Actual late observation boundary was not reached.')
                self.assertEqual(len(spawned), 1)
                self.assertTrue(all(child.poll() is not None and child.stdout.closed and child.stderr.closed for child in spawned))
                self.assertTrue(rejected, 'Valid child result was accepted after its ORIGINAL parent-held deadline.')


class StartupControllerTests(unittest.TestCase):
    def setUp(self):
        self.fixture = observation_fixture.StartupObservationTests()
        self.addCleanup(self.fixture.doCleanups)
        original = guarded_fixture.GuardedHotelArtifactsTests.fixture
        guarded_data = {}
        def capture(helper, **kwargs):
            data = original(helper, **kwargs)
            guarded_data.update({**data, 'publication': copy.deepcopy(data['publication']),
                                 'prepared': copy.deepcopy(data['prepared'])})
            return data
        with mock.patch.object(guarded_fixture.GuardedHotelArtifactsTests, 'fixture', capture):
            self.fixture.setUp()
        self.old_access = access_fixture.GuardedHotelImageAccessTests()
        self.addCleanup(self.old_access.doCleanups)
        with mock.patch.object(guarded_fixture.GuardedHotelArtifactsTests, 'fixture', return_value=guarded_data):
            old_artifacts, old_registry = self.old_access.case()
        old_access = self.old_access.qualify(old_artifacts, old_registry)
        self.job = cluster_fixture.candidate_job()
        self.job['id'] = self.fixture.runtime.run_id
        from app.providers import azure
        self.job['resources']['azure_resource_group_tags'] = azure._tags(self.job['id'], 'resource-group')
        self.base = cluster_fixture.FakeRemoteExecutor(
            ca_sha256=hashlib.sha256(b'OFFLINE-CA-NOT-A-REAL-CERTIFICATE').hexdigest())
        distributed.prepare_azure_distributed_k3s_candidate(self.job, execute=self.base)
        self.job.update(_persist_state=False, results=[])
        self.cluster, plan, stage, context = support._context(
            self.job, self.fixture.data['baseline'], distributed.distributed_k3s_candidate_plan)
        self.ca = context['server_ca_sha256']
        self.fixture.support_plan, self.fixture.support_stage = plan, stage
        old = self.fixture.initialization_plan
        self.fixture.runtime = create_startup_hotel_runtime_plan(self.fixture.artifacts,
            self.fixture.image_access, plan, stage, initialization_attempt_id=old.initialization_attempt_id,
            startup_attempt_id=self.fixture.runtime.startup_attempt_id)
        self.fixture.stage = build_startup_hotel_startup_stage(self.fixture.runtime)
        self.plan, self.stage = self.fixture.runtime, self.fixture.stage
        self.init_plan = create_guarded_hotel_runtime_plan(old_artifacts, plan, stage,
            initialization_attempt_id=old.initialization_attempt_id)
        self.init_stage = build_guarded_hotel_initialization_stage(self.init_plan)
        self.fixture.initialization_plan, self.fixture.initialization_stage = self.init_plan, self.init_stage
        self.assertEqual(context['role_nodes'], ROLES)
        # Derive complete support receipts with the real strict saved validators.
        inventory = init_fixture.support_fixtures.live_inventory(stage)
        ready = support._new_journal(plan, stage, context)
        ready.update(state=support.READY_STATE, applied_phases=[p for p, _ in stage.phases],
            storage_preparation=parse_candidate_database_storage_attestation(storage.marker(plan.storage), plan.storage),
            storage_observation=parse_candidate_database_storage_observation(storage_read.marker(plan.storage), plan.storage),
            support_attestation=k3s._support_receipt(inventory, k3s._validated_context(plan, stage, ROLES)))
        ready['phase_receipts'] = {p: k3s._reconciliation_receipt(inventory, k3s._phase_context(plan, stage, p, ROLES))
                                   for p, _ in stage.phases}
        self.job['resources'][support.SUPPORT_STAGE_JOURNAL_KEY] = ready
        support.validate_candidate_support_journal(ready, plan, stage, context)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name) / '.candidate-support'
        root.mkdir(mode=0o700)
        self.directory = root / self.job['id']
        self.directory.mkdir(mode=0o700)
        self.lease = acquire_run_lease(root, self.job['id'], 'startup-controller-test')
        self.addCleanup(self.lease.release)
        self.cloud = {'create_uid': 'offline-original-create'}
        self.job['candidate_support_pin'] = {'run_id': self.job['id'], 'provider_pin': {'provider': 'azure'}}
        self.job['hotel_initialization_pin'] = {'guarded_artifact_fingerprint': old_artifacts.fingerprint}
        self.init_data = init_fixture.inventory(self.init_plan, self.init_stage)
        self.normalize_support()
        self.init_logs, self.releases, self.calls, self.saved = {}, [], [], []
        self.dispatch_attempts = []
        self.local_drift = None
        self.local_mutations, self.local_api_calls = [], []
        self.created = False
        self.clock = 0.0
        self.hook = None
        self.response_hook = None
        self.lost = None
        self.cost = 0.01
        self.startup = False
        # Run the actual lower initializer. Nothing reconstructs its outcome.
        initialize_started = time.monotonic()
        self.initialized = initializer.prepare_guarded_hotel_initialization(self.job, old_artifacts,
            baseline_artifacts=self.fixture.data['baseline'], lease=self.lease, execute=self.execute,
            persist=self.persist, image_access=old_access, fresh_audit=self.audit,
            expected_cloud_identity=self.cloud, initialization_attempt_id=old.initialization_attempt_id)
        self.initializer_wall_seconds = time.monotonic() - initialize_started
        self.startup = True
        self.calls.clear(); self.saved.clear()

    def audit(self, job, lease):
        self.assertIs(job, self.job); self.assertIs(lease, self.lease)
        return {'schema_version': 1, 'candidate_only': True, 'read_only': True,
                'provider': 'azure', 'run_id': self.job['id'],
                'cloud_identity': copy.deepcopy(self.cloud)}

    def persist(self, job):
        if self.hook: self.hook('persist', job)
        self.saved.append(copy.deepcopy(job))
        self.clock += self.cost
        if getattr(self, 'diagnostics', False) and controller.JOB_KEY in job:
            state = job[controller.JOB_KEY]['state']
            if getattr(self, '_last_diagnostic_state', None) != state:
                self._last_diagnostic_state = state
                counts = {kind: sum(k == kind for k, _ in self.calls) for kind in
                          ('completion', 'create', 'release', 'kubernetes', 'cri')}
                print('Task4 progress: state=%s calls=%s wall=%.3fs' % (
                    state, counts, time.monotonic() - self.diagnostic_started), flush=True)

    def init_output(self, mode):
        return json.dumps(dict(candidate_only=True, released=False, application_start_allowed=False,
            runtime_qualified=False, measurement_qualified=False, run_id=self.init_plan.run_id,
            contract_sha256=self.init_plan.snapshot()['dataset_fingerprint'], mode=mode,
            database_verification_completed=True))

    def completion_pins(self):
        pins = {}
        for mode, row in self.job['guarded_hotel_initialization']['last_observation']['pods'].items():
            terminal = row['pod']['status']['containerStatuses'][0]['state']['terminated']
            pins[mode] = {key: copy.deepcopy(row[key]) for key in
                         ('name', 'uid', 'node_name', 'container_id', 'image_id', 'exit_code', 'spec')}
            pins[mode].update(started_at=terminal['startedAt'], finished_at=terminal['finishedAt'])
        return pins

    def normalize_support(self):
        slices, _, pods, _ = self.fixture.network({})
        self.init_data['items'] = [p for p in self.init_data['items'] if p['kind'] != 'EndpointSlice'
            and not (p['kind'] == 'Pod' and p['spec']['containers'][0]['name'] != 'hotel-initialize')] + pods + slices

    def run_mutation_script(self, command, arguments, response, *, raw_create=None, create_code=0,
                            create_descendant=None, signal_probe=None):
        """Real generated script/pipes; only API responses, CA file and timers fake."""
        workload = copy.deepcopy(next(p for p in self.init_data['items'] if p['kind'] == 'Namespace'))
        system = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
            'name': 'kube-system', 'uid': init_fixture.support_fixtures.identity('Namespace', 'kube-system')},
            'status': {'phase': 'Active'}}
        if self.local_drift == 'namespace': workload['metadata']['uid'] = init_fixture.support_fixtures.identity('Namespace', 'replacement')
        if self.local_drift == 'system': system['metadata']['uid'] = init_fixture.support_fixtures.identity('Namespace', 'replacement-system')
        admission = init_fixture.admission_evidence()
        if self.local_drift == 'admission': next(iter(admission['inventories'].values()))['items'] = [{'kind': 'MutatingWebhookConfiguration'}]
        if self.local_drift == 'partial': next(iter(admission['inventories'].values()))['metadata']['continue'] = 'next'
        responses = {'/api/v1/namespaces/' + self.plan.namespace: workload,
                     '/api/v1/namespaces/kube-system': system,
                     '/apis/' + init_fixture.live.ADMISSION_GROUP: admission['api_group'],
                     **{'/apis/' + k: v for k, v in admission['discovery'].items()},
                     **{'/apis/' + k: v for k, v in admission['inventories'].items()},
                     arguments[-1]: response}
        real_popen = subprocess.Popen
        calls, timers = [], []
        def popen(argv, **kwargs):
            if signal_probe is None: self.assertLessEqual(timers[-1][1], 15)
            else: self.assertTrue(0 < signal.getitimer(signal.ITIMER_REAL)[0] <= 15)
            calls.append(argv)
            self.local_api_calls.append(argv)
            code = 0
            creating = argv == arguments and argv[0] == '/bin/bash'
            if argv == arguments:
                self.local_mutations.append('create' if creating else 'release')
                if creating: self.created = True
                else:
                    component = response['metadata']['labels']['app.kubernetes.io/component']
                    current = self.fixture.admitted(component)
                    if self.response_hook: self.response_hook('before-patch', current)
                    patch = json.loads(argv[argv.index('-p') + 1])
                    for operation in patch:
                        parts = operation['path'].strip('/').split('/')
                        value = current
                        for part in parts[:-1]: value = value[part]
                        if operation['op'] == 'test':
                            if value[parts[-1]] != operation['value']:
                                code = 1
                                break
                        elif operation['op'] == 'remove': del value[parts[-1]]
                        else: self.fail('Unexpected API mutation operation.')
                    if not code:
                        self.assertNotIn('schedulingGates', current['spec'])
                        self.releases.append(component)
            if creating:
                # Authentic kubectl JSONPrinter framing, NOT a returned List.
                raw = raw_create if raw_create is not None else b''.join(
                    json.dumps(pod, indent=4).encode() + b'\n' for pod in responses[argv[-1]])
            else:
                raw = json.dumps(responses[argv[-1]]).encode()
            if creating: code = create_code
            if creating and create_descendant is not None:
                # Real owned subprocess tree: an early-exiting shell analogue
                # or a timed-out parent leaves a child which would work later.
                worker = r'''
import os, subprocess, sys, time, zlib
child = subprocess.Popen([sys.executable, '-c',
    'import os,sys,time;time.sleep(3.4);os.write(int(sys.argv[1]),b"late");time.sleep(30)', sys.argv[3]],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, pass_fds=(int(sys.argv[3]),))
os.write(int(sys.argv[2]), (str(child.pid) + '\n').encode())
os.close(int(sys.argv[2]))
os.close(int(sys.argv[3]))
sys.stdout.buffer.write(zlib.decompress(bytes.fromhex(sys.argv[1])))
sys.stdout.flush()
if sys.argv[4] == 'timeout': time.sleep(30)
'''
                process = real_popen([sys.executable, '-c', worker, zlib.compress(raw).hex(),
                    str(create_descendant['control']), str(create_descendant['work']), create_descendant['mode']],
                    pass_fds=(create_descendant['control'], create_descendant['work']), **kwargs)
                create_descendant['main_process'] = process
                if 'owner' in create_descendant:
                    os.write(create_descendant['owner'], (json.dumps({'pgid': process.pid}) + '\n').encode())
                return process
            return real_popen([sys.executable, '-c',
                'import sys,zlib;sys.stdout.buffer.write(zlib.decompress(bytes.fromhex(sys.argv[1])));sys.exit(int(sys.argv[2]))',
                zlib.compress(raw).hex(), str(code)], **kwargs)
        output = io.StringIO()
        previous = signal.getsignal(signal.SIGALRM)
        previous_trace = sys.gettrace()
        script = shlex.split(command)[-1]
        filename = '<hotel-startup-generated>'
        if signal_probe is not None:
            target = signal_probe['line']
            fired = [False]
            def inject(frame, event, _argument):
                if (not fired[0] and event == 'line' and frame.f_code.co_filename == filename
                        and frame.f_lineno == target):
                    fired[0] = True
                    os.write(signal_probe['owner'], b'{"probe":true}\n')
                    os.kill(os.getpid(), signal.SIGALRM)
                return inject
        certificate = b'CHANGED-CA' if self.local_drift == 'ca' else b'OFFLINE-CA-NOT-A-REAL-CERTIFICATE'
        try:
            timer_context = (mock.patch('signal.setitimer', side_effect=lambda *args: timers.append(args))
                             if signal_probe is None else nullcontext())
            with mock.patch('subprocess.Popen', popen), mock.patch('builtins.open', return_value=io.BytesIO(certificate)), \
                 timer_context, redirect_stdout(output):
                if signal_probe is not None: sys.settrace(inject)
                exec(compile(script, filename, 'exec'), {'__name__': '__main__'})
        finally:
            sys.settrace(previous_trace)
            if signal_probe is not None: signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
        self.assertEqual(calls[-1], arguments)
        return json.loads(output.getvalue())

    @staticmethod
    def _isolated_cleanup_signal_probe(payload):
        # Only immutable remote-command/API fixture bytes cross this boundary;
        # no factory/capability/lease/controller history is reconstructed here.
        from types import SimpleNamespace
        case = StartupControllerTests('test_create_cleanup_alarm_cannot_skip_owned_group_termination')
        case.plan = SimpleNamespace(namespace=payload['namespace'])
        case.init_data = {'items': [payload['namespace_object']]}
        case.local_drift = None
        case.local_mutations, case.local_api_calls, case.releases = [], [], []
        case.response_hook = None
        owned = {'control': payload['control'], 'work': payload['work'], 'owner': payload['owner'],
                 'mode': 'timeout' if payload['oversized'] else 'early-exit'}
        # Nonempty original mask proves restoration is exact, not blanket
        # unblocking. This is isolated OS transport state, never proof state.
        original_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGUSR1})
        expected_mask = original_mask | {signal.SIGUSR1}
        try:
            try:
                case.run_mutation_script(payload['command'], payload['arguments'], payload['pods'],
                    raw_create=b' ' * (4 * 1024 * 1024 + 1) if payload['oversized'] else None,
                    create_descendant=owned, signal_probe={'owner': payload['owner'], 'line': payload['line']})
            except BaseException as exc:
                reason = ('raw-byte-bound' if isinstance(exc, ValueError) and 'raw evidence exceeds' in str(exc)
                          else 'alarm' if isinstance(exc, ValueError) and ('window' in str(exc) or 'alarm' in str(exc))
                          else 'other-failure')
            else: reason = 'success'
            restored = signal.pthread_sigmask(signal.SIG_BLOCK, set()) == expected_mask
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, original_mask)
        print(json.dumps({'reason': reason, 'writes': len(case.local_mutations), 'mask_restored': restored}), flush=True)

    def test_create_cleanup_alarm_cannot_skip_owned_group_termination(self):
        # Actual SIGALRM at the kill boundary must not skip termination/reap.
        # Test-only trace is an instruction locator, never caller-local access.
        wire, _ = self.wire()
        wire['server_ca_sha256'] = 'sha256:' + self.ca
        observed = live.parse_startup_hotel_kubernetes_observation(json.dumps(wire), self.plan, self.stage,
            self.init_plan, self.init_stage, ROLES, self.ca)
        command = controller._mutation_command(['create', '--validate=strict', '-f', '-', '-o', 'json'], 10,
            namespace=self.plan.namespace, expected_identity=observed['identity'],
            manifest=self.stage.canonical_json, marker='HOTEL_STARTUP_CREATE')
        script = shlex.split(command)[-1]
        tree = ast.parse(script)
        arguments = ast.literal_eval(tree.body[-1].value.args[0].args[0])
        kill_line = next(node.lineno for node in ast.walk(tree) if isinstance(node, ast.Call)
                         and isinstance(node.func, ast.Attribute) and node.func.attr == 'killpg')
        mask_lines = [node.lineno for node in ast.walk(tree) if isinstance(node, ast.Call)
                      and isinstance(node.func, ast.Attribute) and node.func.attr == 'pthread_sigmask'
                      and len(node.args) > 1 and isinstance(node.args[1], ast.Set)
                      and any(isinstance(item, ast.Attribute) and item.attr == 'SIGALRM'
                              for item in node.args[1].elts)]
        probes = [('kill-oversize', kill_line, True), ('kill-valid', kill_line, False)]
        # No mask site exists on RED. Its absence is NOT a signal-boundary RED;
        # after introduction, physically test that first-mask race separately.
        if mask_lines: probes.append(('first-mask', min(mask_lines), True))
        namespace = copy.deepcopy(next(p for p in self.init_data['items'] if p['kind'] == 'Namespace'))
        for name, line, oversized in probes:
            with self.subTest(boundary=name):
                owner_read, owner_write = os.pipe()
                control_read, control_write = os.pipe()
                work_read, work_write = os.pipe()
                helper = None
                group = None
                payload = {'namespace': self.plan.namespace, 'namespace_object': namespace,
                    'command': command, 'arguments': arguments, 'pods': [self.fixture.admitted(c) for c in ORDER],
                    'owner': owner_write, 'control': control_write, 'work': work_write, 'line': line, 'oversized': oversized}
                encoded = zlib.compress(json.dumps(payload).encode()).hex()
                program = ('import json,sys,zlib;from tests.test_deathstarbench_hotel_startup_controller '
                           'import StartupControllerTests;StartupControllerTests._isolated_cleanup_signal_probe('
                           'json.loads(zlib.decompress(bytes.fromhex(sys.argv[1]))))')
                try:
                    helper = subprocess.Popen([sys.executable, '-c', program, encoded], stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL, pass_fds=(owner_write, control_write, work_write))
                    for descriptor in (owner_write, control_write, work_write): os.close(descriptor)
                    owner_write = control_write = work_write = None
                    data = b''
                    until = time.monotonic() + 6
                    with selectors.DefaultSelector() as selector:
                        selector.register(owner_read, selectors.EVENT_READ)
                        while b'"probe"' not in data and time.monotonic() < until:
                            if not selector.select(max(0, until - time.monotonic())): break
                            chunk = os.read(owner_read, 256)
                            if not chunk: break
                            data += chunk
                    events = [json.loads(row) for row in data.splitlines()]
                    group = next((row['pgid'] for row in events if 'pgid' in row), None)
                    self.assertIsInstance(group, int, 'Actual owned create process was not reported.')
                    self.assertTrue(any(row.get('probe') is True for row in events), 'OS signal probe was not reached.')
                    returned = True
                    try: raw = helper.communicate(timeout=6)[0]
                    except subprocess.TimeoutExpired:
                        returned = False
                        raw = b''
                        os.killpg(group, signal.SIGKILL)
                        helper.kill()
                        helper.communicate(timeout=2)
                    with selectors.DefaultSelector() as selector:
                        selector.register(work_read, selectors.EVENT_READ)
                        self.assertTrue(selector.select(5), 'Owned descendant pipe exceeded the test watchdog.')
                    delayed = os.read(work_read, 64)
                    result = json.loads(raw) if returned else {'reason': 'watchdog', 'writes': 1, 'mask_restored': False}
                    want = 'raw-byte-bound' if oversized else 'alarm'
                    self.assertEqual((returned, result['reason'], result['writes'], delayed),
                                     (True, want, 1, b''), 'Actual alarm skipped owned termination or replaced original failure.')
                    self.assertEqual(helper.returncode, 0)
                    self.assertTrue(result['mask_restored'], 'Original nonempty OS signal mask was not restored.')
                finally:
                    if group is not None:
                        try: os.killpg(group, signal.SIGKILL)
                        except ProcessLookupError: pass
                    if helper is not None:
                        if helper.poll() is None: helper.kill()
                        helper.communicate(timeout=2)
                    for descriptor in (owner_read, owner_write, control_read, control_write, work_read, work_write):
                        if descriptor is not None: os.close(descriptor)

    def test_generated_create_reaps_only_its_owned_descendant_group(self):
        # Killing only the outer shell permits delayed kubectl work after a
        # timeout, and misses background descendants after early shell exit.
        wire, _ = self.wire()
        wire['server_ca_sha256'] = 'sha256:' + self.ca
        observed = live.parse_startup_hotel_kubernetes_observation(json.dumps(wire), self.plan, self.stage,
            self.init_plan, self.init_stage, ROLES, self.ca)
        command = controller._mutation_command(['create', '--validate=strict', '-f', '-', '-o', 'json'], 3,
            namespace=self.plan.namespace, expected_identity=observed['identity'],
            manifest=self.stage.canonical_json, marker='HOTEL_STARTUP_CREATE')
        arguments = ast.literal_eval(ast.parse(shlex.split(command)[-1]).body[-1].value.args[0].args[0])
        pods = [self.fixture.admitted(component) for component in ORDER]
        for mode in ('timeout', 'early-exit'):
            with self.subTest(mode=mode):
                control_read, control_write = os.pipe()
                work_read, work_write = os.pipe()
                owned = {'control': control_write, 'work': work_write, 'mode': mode}
                descendant = None
                before = len(self.local_mutations)
                try:
                    failure, output = None, None
                    try:
                        output = self.run_mutation_script(command, arguments, pods, create_descendant=owned)
                    except ValueError as exc: failure = exc
                    os.close(control_write); control_write = None
                    os.close(work_write); work_write = None
                    with selectors.DefaultSelector() as selector:
                        selector.register(control_read, selectors.EVENT_READ)
                        self.assertTrue(selector.select(6), 'Owned descendant did not identify itself within the test bound.')
                    descendant = int(os.read(control_read, 64).decode().strip())
                    self.assertEqual(len(self.local_mutations) - before, 1)
                    if mode == 'timeout':
                        self.assertIsInstance(failure, ValueError)
                        self.assertRegex(str(failure), 'deadline|timed out|window')
                    else:
                        self.assertIsNone(failure)
                        self.assertEqual(len(output['pods']), 10)
                    with selectors.DefaultSelector() as selector:
                        selector.register(work_read, selectors.EVENT_READ)
                        self.assertTrue(selector.select(6), 'Owned descendant pipe did not close within the test bound.')
                    self.assertEqual(os.read(work_read, 64), b'', 'Owned descendant performed delayed work after the command bound.')
                finally:
                    # Independently clean this exact known bad-case child even
                    # on RED. Never enumerate/kill a process name or group we
                    # did not create; the baseline inherits the test's group.
                    if descendant is not None:
                        try: os.kill(descendant, signal.SIGKILL)
                        except ProcessLookupError: pass
                    main = owned.get('main_process')
                    if main is not None:
                        if main.poll() is None: main.kill()
                        main.wait()
                    for descriptor in (control_read, control_write, work_read, work_write):
                        if descriptor is not None: os.close(descriptor)

    def test_generated_create_consumes_exact_bounded_kubectl_json_stream(self):
        # Actual kubectl JSONPrinter emits one pretty Pod JSON document per
        # resource. A single-JSON run parser rejects real successful output.
        wire, _ = self.wire()
        wire['server_ca_sha256'] = 'sha256:' + self.ca
        observation = live.parse_startup_hotel_kubernetes_observation(json.dumps(wire), self.plan, self.stage,
            self.init_plan, self.init_stage, ROLES, self.ca)
        command = controller._mutation_command(['create', '--validate=strict',
            '--field-manager=deathstarbench-hotel-startup', '-f', '-', '-o', 'json'], 10,
            namespace=self.plan.namespace, expected_identity=observation['identity'],
            manifest=self.stage.canonical_json, marker='HOTEL_STARTUP_CREATE')
        arguments = ast.literal_eval(ast.parse(shlex.split(command)[-1]).body[-1].value.args[0].args[0])
        pods = [self.fixture.admitted(component) for component in ORDER]
        stream = b''.join(json.dumps(pod, indent=4).encode() + b'\n' for pod in pods)
        failure, output = None, None
        before = len(self.local_mutations)
        try:
            output = self.run_mutation_script(command, arguments, pods, raw_create=stream)
        except ValueError as exc: failure = exc
        self.assertEqual(len(self.local_mutations) - before, 1, 'Did not reach the actual write boundary.')
        self.assertIsNone(failure, 'Authentic ten-document kubectl output was rejected at framing: ' + type(failure).__name__)
        self.assertEqual(set(output), {'schema_version', 'candidate_schema', 'pods'})
        self.assertEqual(output['schema_version'], 1)
        self.assertEqual(output['candidate_schema'], 'hotel-startup-create-acknowledgment-v1')
        self.assertEqual(output['pods'], pods)
        approved = {pod['metadata']['name']: pod for pod in json.loads(self.stage.canonical_json)['items']}
        self.assertEqual(set(controller._creation_pins(output, approved, ROLES)), set(ORDER))
        def printed(documents):
            return b''.join(json.dumps(pod, indent=4).encode() + b'\n' for pod in documents)
        duplicate = copy.deepcopy(pods)
        duplicate[-1] = copy.deepcopy(duplicate[0])
        repeated_uid = copy.deepcopy(pods)
        repeated_uid[-1]['metadata']['uid'] = repeated_uid[0]['metadata']['uid']
        nonfinite = copy.deepcopy(pods)
        nonfinite[0]['status']['untrusted'] = float('nan')
        deep = copy.deepcopy(pods)
        nested = []
        for _ in range(66): nested = [nested]
        deep[0]['status']['untrusted'] = nested
        duplicate_keys = stream.replace(b'"apiVersion": "v1"', b'"apiVersion": "v1", "apiVersion": "v1"', 1)
        self.assertNotEqual(duplicate_keys, stream)
        cases = (
            ('incomplete', printed(pods[:-1]), 0, 'incomplete'),
            ('trailing', stream + b'not-json', 0, 'excess|trailing'),
            ('nonzero-after-ten', stream, 7, 'create command failed'),
            ('duplicate-document', printed(duplicate), 0, 'repeated or unrelated'),
            ('duplicate-uid', printed(repeated_uid), 0, 'repeated or unrelated'),
            ('duplicate-key', duplicate_keys, 0, 'duplicate keys'),
            ('nonfinite', printed(nonfinite), 0, 'non-finite'),
            ('depth', printed(deep), 0, 'nesting bound'),
            ('utf8', stream + b'\xff', 0, 'stream is invalid'),
            ('malformed-json', b'{', 0, 'stream is invalid'),
            ('status', printed([{'apiVersion': 'v1', 'kind': 'Status'}] + pods[1:]), 0, 'Pod documents'),
            ('old-list', json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': pods}).encode(), 0, 'Pod documents'),
            ('excess', stream + printed(pods[:1]), 0, 'excess'),
            ('oversized', b' ' * (4 * 1024 * 1024 + 1), 0, 'raw evidence exceeds its bound'),
        )
        for name, raw, exit_code, reason in cases:
            with self.subTest(stream=name):
                before = len(self.local_mutations)
                with self.assertRaisesRegex(ValueError, reason):
                    self.run_mutation_script(command, arguments, pods, raw_create=raw, create_code=exit_code)
                self.assertEqual(len(self.local_mutations) - before, 1)

    def wire(self):
        wire, _ = self.fixture.wire()
        pods = {c: (self.fixture.execution(c, 20 + ORDER.index(c))[1] if c in self.releases
                    else live.identity.validate_startup_hotel_admitted_pod(self.fixture.admitted(c), self.stage.pod(c), ROLES))
                for c in ORDER} if self.created else {}
        slices, _, support_pods, execution = self.fixture.network({c: pods[c] for c in self.releases})
        # Original completed initializer objects remain in the complete wire.
        init_objects = [p for p in self.init_data['items'] if p['metadata']['name'] in
                        {i['metadata']['name'] for i in json.loads(self.init_stage.canonical_json)['items']}]
        for collection in wire['collections'].values():
            kind = collection['kind'][:-4]
            if kind == 'Pod': collection['items'] = support_pods + [p['pod'] for p in pods.values()] + [p for p in init_objects if p['kind'] == kind]
            elif kind == 'EndpointSlice': collection['items'] = slices
            elif kind == 'NetworkPolicy':
                names = {p['metadata']['name'] for p in init_objects}
                collection['items'] = [p for p in collection['items'] if p['metadata']['name'] not in names] + [p for p in init_objects if p['kind'] == kind]
        wire['consul'] = self.fixture.consul(pods, execution, tuple(self.releases))
        return wire, pods

    def execute(self, job, command, **kwargs):
        self.clock += self.cost
        if 'CANDIDATE_DSB_DATABASE_STORAGE_OBSERVATION' in command:
            return storage_read.marker(self.init_plan.storage)
        if 'AZURE_DSB_DATABASE_VOLUME' in command:
            return self.base(job, command, **kwargs)
        script = shlex.split(command)[-1]
        if not self.startup:
            operation = ast.literal_eval(re.search(r'^OPERATION=(.+)$', script, re.M)[1])
            mode = ast.literal_eval(re.search(r'^MODE=(.+)$', script, re.M)[1])
            if operation == 'create':
                self.init_data = init_fixture.inventory(self.init_plan, self.init_stage, created=True)
                self.normalize_support()
            elif operation == 'release': init_fixture.run_pod(self.init_data, mode, state='running')
            elif operation == 'wait':
                terminal_data = init_fixture.inventory(self.init_plan, self.init_stage, created=True)
                terminal = init_fixture.run_pod(terminal_data, mode)
                current = init_fixture.initialization_pod(self.init_data, mode)
                current.clear(); current.update(terminal)
                self.init_logs[mode] = self.init_output(mode)
            doc = init_fixture.document(self.init_plan, self.init_stage, self.init_data, logs=self.init_logs)
            doc['server_ca_sha256'] = 'sha256:' + self.ca
            return json.dumps(doc)
        self.assertTrue(kwargs.get('sensitive_output'))
        self.assertLessEqual(kwargs['timeout'], 60)
        kind = ('completion' if 'HOTEL_STARTUP_COMPLETION' in script else 'create' if 'HOTEL_STARTUP_CREATE' in script
                else 'release' if 'HOTEL_STARTUP_RELEASE' in script else 'cri' if "'hotel-startup-cri-observation-v1'" in script else 'kubernetes')
        self.calls.append((kind, kwargs['host_key']))
        if kind == 'release':
            self.dispatch_attempts.append(job[controller.JOB_KEY]['state'].removesuffix('-dispatch-intent'))
        if len(self.calls) > 120:
            raise TimeoutError('Synthetic transport observation budget exhausted.')
        if self.hook: self.hook(kind, job)
        if kind == 'completion':
            result = {'pods': {}, 'logs': copy.deepcopy(self.init_logs)}
            for mode in ('initialize-empty', 'verify'):
                pod = init_fixture.initialization_pod(self.init_data, mode)
                result['pods'][mode] = {'before': copy.deepcopy(pod), 'after': copy.deepcopy(pod)}
            return json.dumps(result)
        if kind == 'create':
            arguments = ast.literal_eval(ast.parse(script).body[-1].value.args[0].args[0])
            self.assertEqual(arguments[:4], ['/bin/bash', '-o', 'pipefail', '-c'])
            shell = shlex.split(arguments[-1])
            self.assertEqual(shell[:2], ['printf', '%s'])
            self.assertEqual(shell[3:7], ['|', 'base64', '-d', '|'])
            self.assertEqual(shell[7:10], [live.K3S_BINARY, 'kubectl', 'create'])
            self.assertIn('--validate=strict', shell)
            payload = json.loads(base64.b64decode(shell[2]))
            self.assertEqual(payload['kind'], 'List')
            self.assertEqual(len(payload['items']), 10)
            self.assertEqual(payload, json.loads(self.stage.canonical_json))
            self.assertFalse(self.created)
            self.assertTrue((self.directory / '.hotel-startup-create.latch').exists())
            response = [self.fixture.admitted(c) for c in ORDER]
            if self.response_hook: self.response_hook('create', response)
            actual = self.run_mutation_script(command, arguments, response)
            return json.dumps(actual)
        if kind == 'release':
            arguments = ast.literal_eval(ast.parse(script).body[-1].value.args[0].args[0])
            component = next(c for c in ORDER if self.plan.application_pod_name(c) in arguments)
            patch = json.loads(arguments[arguments.index('-p') + 1])
            current = self.fixture.admitted(component)
            self.assertEqual({op['path'] for op in patch if op['op'] == 'test'},
                {'/metadata/' + key for key in current['metadata']} | {'/spec'})
            self.assertEqual([(op['op'], op['path']) for op in patch if op['op'] != 'test'],
                             [('remove', '/spec/schedulingGates')])
            self.assertEqual(len(self.job[controller.JOB_KEY]['journal']['events'][2]['payload']['pods']), 10)
            self.assertTrue((self.directory / ('.hotel-startup-component-' + component + '.latch')).exists())
            self.assertEqual(component, ORDER[len(self.releases)])
            response = self.fixture.execution(component, 20 + ORDER.index(component))[0]
            if self.response_hook: self.response_hook('release', response)
            actual = self.run_mutation_script(command, arguments, response)
            if self.lost == component: raise TimeoutError('secret https://example/?signed=do-not-store')
            return json.dumps(actual)
        wire, pods = self.wire()
        wire['server_ca_sha256'] = 'sha256:' + self.ca
        if kind == 'cri':
            self.assertEqual(kwargs['host_key'], self.cluster.host('application').host_key)
            evidence = self.fixture.cri_wire(pods)
            if self.response_hook: self.response_hook('cri', evidence)
            return json.dumps(evidence)
        self.assertEqual(kwargs['host_key'], self.cluster.host('control').host_key)
        if self.response_hook: self.response_hook('kubernetes', wire)
        return json.dumps(wire)

    def run_startup(self):
        return controller.prepare_startup_hotel_applications(self.job, self.fixture.artifacts,
            baseline_artifacts=self.fixture.data['baseline'], initialization_plan=self.init_plan,
            initialization_stage=self.init_stage, lease=self.lease, execute=self.execute, persist=self.persist,
            image_access=self.fixture.image_access, fresh_audit=self.audit, expected_cloud_identity=self.cloud,
            startup_attempt_id=self.plan.startup_attempt_id, monotonic=getattr(self, 'monotonic', lambda: self.clock))

    def test_fresh_initializer_and_all_ten_blocked_pods_precede_first_release(self):
        # Missing real completion checks, all-ten pin barrier or CRI/publication join must fail this.
        original_plan, original_stage, original_access = self.plan, self.stage, self.fixture.image_access
        initializer_record = copy.deepcopy(self.job['guarded_hotel_initialization'])
        self.assertIs(validate_startup_hotel_runtime_plan(original_plan), original_plan)
        self.assertIs(validate_startup_hotel_startup_stage(original_plan, original_stage), original_stage)
        clock_samples = {'first': None, 'last': None, 'count': 0}
        def real_clock():
            now = time.monotonic()
            if clock_samples['first'] is None: clock_samples['first'] = now
            clock_samples.update(last=now, count=clock_samples['count'] + 1)
            return now
        self.monotonic = real_clock
        transport_samples = []
        original_execute = self.execute
        def measured_transport(job, command, **kwargs):
            before = len(self.calls)
            began = time.monotonic()
            try:
                return original_execute(job, command, **kwargs)
            finally:
                kind = self.calls[-1][0] if len(self.calls) > before else 'prerequisite'
                transport_samples.append((kind, began, time.monotonic()))
        self.execute = measured_transport
        self.diagnostics = True
        self.diagnostic_started = time.monotonic()
        wall_started = time.monotonic()
        try:
            result = self.run_startup()
        finally:
            record = self.job.get(controller.JOB_KEY, {})
            elapsed = record.get('startup_elapsed_seconds')
            preflight = record.get('preflight_elapsed_seconds')
            if (record.get('state') == 'failed-cleanup-only' and type(elapsed) in (int, float)
                    and clock_samples['first'] is not None and clock_samples['last'] is not None):
                # Failure records elapsed at the final original-clock sample,
                # before terminal persistence. No caller-frame inspection.
                preflight = clock_samples['last'] - elapsed - clock_samples['first']
            triplets = [transport_samples[i:i+3] for i in range(len(transport_samples) - 2)
                        if [row[0] for row in transport_samples[i:i+3]] == ['kubernetes', 'cri', 'kubernetes']]
            span = triplets[-1][-1][2] - triplets[-1][0][1] if triplets else None
            counts = {kind: sum(k == kind for k, _ in self.calls) for kind in
                      ('completion', 'create', 'release', 'kubernetes', 'cri')}
            print('Task4 genuine timing: ' + json.dumps({
                'initializer_wall_seconds': self.initializer_wall_seconds,
                'controller_wall_seconds': time.monotonic() - wall_started,
                'post_create_recorded_seconds': elapsed, 'preflight_seconds': preflight,
                'last_triplet_transport_span_lower_bound_seconds': span,
                'transport_span_over_60_lower_bound_seconds': max(0.0, span - 60.0) if span is not None else None,
                'original_clock_samples': clock_samples['count'], 'calls': counts,
            }, sort_keys=True, allow_nan=False), flush=True)
        self.assertEqual(self.releases, list(ORDER))
        self.assertEqual(sum(k == 'create' for k, _ in self.calls), 1)
        self.assertLess(next(i for i, (k, _) in enumerate(self.calls) if k == 'completion'),
                        next(i for i, (k, _) in enumerate(self.calls) if k == 'create'))
        self.assertTrue(result['startup_execution_observed'])
        self.assertTrue(result['service_identity_observed'])
        self.assertEqual(result['journal']['event_count'], 24)
        self.assertEqual(result['plan_fingerprint'], original_plan.fingerprint)
        self.assertEqual(result['stage_fingerprint'], original_stage.fingerprint)
        self.assertIs(original_plan.access, original_access)
        self.assertEqual(self.job['guarded_hotel_initialization'], initializer_record)
        self.assertLessEqual(result['startup_elapsed_seconds'], 900)
        for i, (kind, _) in enumerate(self.calls):
            if kind == 'cri': self.assertEqual([k for k, _ in self.calls[i-1:i+2]], ['kubernetes', 'cri', 'kubernetes'])
        for flag in ('runtime_qualified', 'measurement_qualified', 'public_release', 'application_ready'):
            self.assertIs(result[flag], False)

    def test_shared_deadline_and_lost_response_are_terminal(self):
        # Post-create journal/callback overhead must share the original global
        # deadline, never a fresh per-component budget.
        def deadline(kind, job):
            if kind == 'persist' and controller.JOB_KEY in job:
                if job[controller.JOB_KEY]['state'] == 'attractions-observed': self.clock += 450
                elif job[controller.JOB_KEY]['state'] == 'geo-dispatch-intent': self.clock += 451
        self.hook = deadline
        with self.assertRaises(ValueError): self.run_startup()
        self.assertEqual(self.job[controller.JOB_KEY]['state'], 'failed-cleanup-only')
        self.assertGreaterEqual(self.job[controller.JOB_KEY]['startup_elapsed_seconds'], 900)
        self.assertEqual(self.releases, ['attractions'])
        self.assertEqual(self.dispatch_attempts, ['attractions'])

    def test_invalid_create_acknowledgment_cannot_be_repaired_by_later_pods(self):
        # Discarding the create acknowledgment must fail at the first forbidden
        # release, not slowly run all ten healthy independently observed Pods.
        for index, mutation in enumerate(('malformed', 'partial', 'wrong-uid')):
            with self.subTest(mutation=mutation):
                case = self if index == 0 else StartupControllerTests(
                    'test_invalid_create_acknowledgment_cannot_be_repaired_by_later_pods')
                if case is not self:
                    try: case.setUp()
                    except BaseException:
                        case.doCleanups()
                        raise
                try:
                    changed = [False]
                    def acknowledgment(kind, response):
                        if kind != 'create': return
                        changed[0] = True
                        if mutation == 'malformed': response.clear()
                        elif mutation == 'partial': response.pop()
                        else:
                            response[0]['metadata']['uid'] = init_fixture.support_fixtures.identity(
                                'Pod', 'different-create-acknowledgment')
                    def forbidden(kind, _job):
                        if kind == 'release': raise OSError('forbidden release after invalid create acknowledgment')
                    case.response_hook, case.hook = acknowledgment, forbidden
                    failure = None
                    try: case.run_startup()
                    except (ValueError, OSError) as exc: failure = exc
                    self.assertTrue(changed[0])
                    self.assertEqual(case.dispatch_attempts, [], 'Invalid acknowledgment reached a release request.')
                    self.assertEqual(case.releases, [])
                    self.assertEqual(sum(kind == 'create' for kind, _ in case.calls), 1)
                    self.assertIsInstance(failure, ValueError)
                    self.assertRegex(str(failure), 'create acknowledgment|Pinned.*(metadata|UID)')
                    self.assertEqual(case.job[controller.JOB_KEY]['state'], 'failed-cleanup-only')
                finally:
                    if case is not self: case.doCleanups()



    def test_cri_only_drift_after_durable_intent_prevents_next_release(self):
        # A Kubernetes-only post-intent read misses real native-execution drift
        # caused by persistence while all Kubernetes/publication data is stable.
        changed, inspected = [False], [False]
        def persisted(kind, job):
            state = job.get(controller.JOB_KEY, {}).get('state')
            if kind == 'persist' and state == 'geo-dispatch-intent': changed[0] = True
            if kind == 'release' and state == 'geo-dispatch-intent':
                raise OSError('forbidden geo dispatch after native execution drift')
        def native(kind, evidence):
            if kind == 'cri' and changed[0]:
                inspected[0] = True
                entry = next(row for row in evidence['evidence']['entries'] if row['component'] == 'attractions')
                entry['inspect']['status']['metadata']['attempt'] = 1
        self.hook, self.response_hook = persisted, native
        failure = None
        try: self.run_startup()
        except (ValueError, OSError) as exc: failure = exc
        self.assertTrue(changed[0])
        self.assertEqual(self.dispatch_attempts, ['attractions'], 'CRI-only drift reached geo dispatch.')
        self.assertEqual(self.releases, ['attractions'])
        self.assertTrue(inspected[0])
        self.assertIsInstance(failure, ValueError)
        self.assertRegex(str(failure), 'attempt|restart')
        self.assertEqual(self.job[controller.JOB_KEY]['state'], 'failed-cleanup-only')

    def test_fresh_initializer_log_and_execution_drift_stop_create(self):
        # Latches/success booleans cannot replace independent fresh dataset logs.
        self.init_logs['verify'] = self.init_output('initialize-empty')
        with self.assertRaises(ValueError): self.run_startup()
        self.assertNotIn('create', [k for k, _ in self.calls])
        self.assertEqual(self.releases, [])

    def test_lost_response_never_uses_later_running_pod_as_permission(self):
        self.lost = 'attractions'
        with self.assertRaises(TimeoutError): self.run_startup()
        self.assertEqual(self.releases, ['attractions'])
        self.assertEqual(self.job[controller.JOB_KEY]['state'], 'failed-cleanup-only')
        self.assertNotIn('signed=do-not-store', json.dumps(self.job[controller.JOB_KEY]))
        self.assertLessEqual(sum(k == 'create' for k, _ in self.calls), 1)
        before = list(self.calls)
        with self.assertRaises(ValueError): self.run_startup()
        self.assertEqual(self.calls, before)

    def test_callback_mutation_cannot_rebind_original_host_or_initializer(self):
        # The post-callback guard must fire before even the create request.
        def mutate(kind, job):
            if kind == 'completion':
                job['guarded_hotel_initialization']['initialization_attempt_id'] = 'changed'
        self.hook = mutate
        with self.assertRaises(ValueError): self.run_startup()
        self.assertEqual(self.releases, [])
        self.assertNotIn('create', [k for k, _ in self.calls])

    def test_initializer_execution_continuity_is_not_just_pod_uid(self):
        # Task2 UID stability alone must not accept a different completed execution.
        def change(kind, _job):
            if kind == 'create':
                pod = init_fixture.initialization_pod(self.init_data, 'verify')
                pod['status']['containerStatuses'][0]['state']['terminated']['finishedAt'] = '2026-10-15T15:00:16Z'
        self.hook = change
        with self.assertRaises(ValueError): self.run_startup()
        self.assertTrue(self.created)
        self.assertEqual(self.releases, [])

    def test_control_cri_control_has_one_shrinking_observation_deadline(self):
        # A slow first read cannot give CRI/final read a fresh 60 seconds.
        original = self.execute
        def slow(job, command, **kwargs):
            result = original(job, command, **kwargs)
            if self.calls and self.calls[-1][0] == 'cri':
                self.clock += 60
            return result
        self.execute = slow
        with self.assertRaises(ValueError): self.run_startup()
        index = next(i for i, (kind, _) in enumerate(self.calls) if kind == 'cri')
        self.assertEqual(self.calls[index-1][0], 'kubernetes')
        self.assertNotIn('kubernetes', [k for k, _ in self.calls[index+1:]])
        self.assertEqual(self.releases, ['attractions'])

    def test_generated_completion_collector_reads_real_pipes_with_bounds(self):
        # Exercise the actual generated collector, not just a returned evidence fixture.
        command = controller._completion_command(self.init_plan, self.init_stage, ROLES, self.ca, 10,
                                                  completed_pins=self.completion_pins())
        responses = {'json': init_fixture.initialization_pod(self.init_data, 'initialize-empty')}
        count = [0]
        def replace(_key, raw):
            mode = ('initialize-empty', 'verify')[count[0] // 3]
            offset = count[0] % 3
            count[0] += 1
            return (self.init_output(mode).encode() if offset == 1 else
                    json.dumps(init_fixture.initialization_pod(self.init_data, mode)).encode()), 0
        responses['--limit-bytes=4097'] = b'{}'
        output, calls, _, timers = self.fixture.execute_command(command, responses, replace=replace)
        self.assertEqual(len(calls), 6)
        self.assertEqual(set(output['pods']), {'initialize-empty', 'verify'})
        self.assertTrue(all(0 < args[1] <= 10 for args in timers))
        count[0] = 0
        def oversized(key, raw):
            if key == '--limit-bytes=4097': return b' ' * 4097 + b'{}', 0
            return replace(key, raw)
        with self.assertRaisesRegex(ValueError, 'Initializer log exceeds its bound'):
            self.fixture.execute_command(command, responses, replace=oversized)

    def test_completion_collector_rejects_replaced_uid_before_any_log_request(self):
        retained = self.job['guarded_hotel_initialization']['last_observation']['pods']['initialize-empty']['pod']
        retained['status']['message'] = 'https://private/?signed=never-in-command'
        command = controller._completion_command(self.init_plan, self.init_stage, ROLES, self.ca, 10,
                                                  completed_pins=self.completion_pins())
        encoded = re.search(r'^CONTEXT=.*base64.b64decode\(([^)]+)\)', shlex.split(command)[-1], re.M)[1]
        context = json.loads(zlib.decompress(base64.b64decode(ast.literal_eval(encoded))))
        self.assertNotIn('signed=never-in-command', json.dumps(context))
        self.assertEqual(set(context['completed_pins']['initialize-empty']), {
            'name', 'uid', 'node_name', 'container_id', 'image_id', 'exit_code', 'spec', 'started_at', 'finished_at'})
        for offset, drift in ((0, 'uid'), (0, 'finishedAt'), (2, 'uid'), (2, 'finishedAt')):
            with self.subTest(offset=offset, drift=drift):
                requests = []
                def changed(key, _raw):
                    index = len(requests)
                    mode = ('initialize-empty', 'verify')[index // 3]
                    requests.append(key)
                    if key == '--limit-bytes=4097': return self.init_output(mode).encode(), 0
                    pod = copy.deepcopy(init_fixture.initialization_pod(self.init_data, mode))
                    if index == offset:
                        if drift == 'uid': pod['metadata']['uid'] = init_fixture.support_fixtures.identity('Pod', 'replacement-' + mode)
                        else: pod['status']['containerStatuses'][0]['state']['terminated']['finishedAt'] = '2026-10-15T15:00:16Z'
                    return json.dumps(pod).encode(), 0
                with self.assertRaisesRegex(ValueError, 'Completed initializer around log retrieval drifted'):
                    self.fixture.execute_command(command, {'json': {}, '--limit-bytes=4097': b'{}'}, replace=changed)
                self.assertEqual(requests, ['json'] if offset == 0 else ['json', '--limit-bytes=4097', 'json'])

    def test_original_support_receipts_cannot_be_replaced_by_retained_summary(self):
        # Even a self-consistent changed old/new summary must preserve original
        # support-ready Node identities and resource receipts independently.
        replacement = init_fixture.support_fixtures.identity('Node', 'replacement-control')
        retained = self.job['guarded_hotel_initialization']['last_observation']
        for row in retained['nodes']:
            if row['role'] == 'control': row['uid'] = replacement
        for row in retained['identity']['objects']:
            if row['kind'] == 'Node' and row['name'] == ROLES['control']: row['uid'] = replacement
        def replace(kind, wire):
            if kind == 'kubernetes':
                nodes = next(c['items'] for c in wire['collections'].values() if c['kind'] == 'NodeList')
                next(n for n in nodes if n['metadata']['name'] == ROLES['control'])['metadata']['uid'] = replacement
        self.response_hook = replace
        self.hook = lambda kind, _job: (_ for _ in ()).throw(RuntimeError('unexpected create')) if kind == 'create' else None
        with self.assertRaises(Exception): self.run_startup()
        self.assertNotIn('create', [k for k, _ in self.calls])
        self.assertEqual(self.releases, [])

    def test_physical_mutation_after_durable_intent_stops_create(self):
        changed = [False]
        def mutate(kind, job):
            if kind == 'persist' and job.get(controller.JOB_KEY, {}).get('state') == 'create-intent': changed[0] = True
            if kind == 'create': raise OSError('unsafe create reached')
        def current(kind, wire):
            if kind == 'kubernetes' and changed[0]:
                namespaces = next(c['items'] for c in wire['collections'].values() if c['kind'] == 'NamespaceList')
                next(n for n in namespaces if n['metadata']['name'] == self.plan.namespace)['metadata']['uid'] = init_fixture.support_fixtures.identity('Namespace', 'new-after-intent')
        self.hook, self.response_hook = mutate, current
        with self.assertRaisesRegex(ValueError, 'namespace_uid|Namespace'): self.run_startup()
        self.assertTrue(changed[0])
        self.assertNotIn('create', [kind for kind, _ in self.calls])

    def test_generated_mutation_rechecks_namespace_before_create(self):
        self.local_drift = 'namespace'
        self.hook = lambda kind, _job: (_ for _ in ()).throw(OSError('stop after unsafe create')) if kind == 'release' else None
        with self.assertRaisesRegex(ValueError, 'Namespace identity changed'): self.run_startup()
        self.assertEqual(self.local_mutations, [])
        self.assertFalse(self.created)

    def test_physical_support_mutation_after_durable_intent_stops_release(self):
        changed = [False]
        def mutate(kind, job):
            if kind == 'persist' and job.get(controller.JOB_KEY, {}).get('state') == 'attractions-dispatch-intent': changed[0] = True
        def current(kind, wire):
            if kind == 'kubernetes' and changed[0]:
                pods = next(c['items'] for c in wire['collections'].values() if c['kind'] == 'PodList')
                next(p for p in pods if p['spec']['containers'][0]['name'] == 'consul')['status']['containerStatuses'][0]['restartCount'] = 1
        self.hook, self.response_hook = mutate, current
        with self.assertRaisesRegex(ValueError, 'restart'): self.run_startup()
        self.assertTrue(changed[0])
        self.assertEqual(self.dispatch_attempts, [])
        self.assertEqual(self.releases, [])

    def test_both_generated_mutations_enforce_local_custody_and_single_write(self):
        wire, _ = self.wire()
        wire['server_ca_sha256'] = 'sha256:' + self.ca
        observed = live.parse_startup_hotel_kubernetes_observation(json.dumps(wire), self.plan, self.stage,
            self.init_plan, self.init_stage, ROLES, self.ca)
        pod = self.fixture.admitted('attractions')
        patch = [{'op': 'test', 'path': '/metadata/' + key, 'value': value} for key, value in pod['metadata'].items()]
        patch += [{'op': 'test', 'path': '/spec', 'value': pod['spec']}, {'op': 'remove', 'path': '/spec/schedulingGates'}]
        cases = [('create', ['create', '--validate=strict', '-f', '-', '-o', 'json'], self.stage.canonical_json,
                  [self.fixture.admitted(c) for c in ORDER]),
                 ('release', ['-n', self.plan.namespace, 'patch', 'pod', pod['metadata']['name'], '--type=json',
                              '-p', json.dumps(patch), '-o', 'json'], None, self.fixture.execution('attractions')[0])]
        for name, args, manifest, response in cases:
            command = controller._mutation_command(args, 10, namespace=self.plan.namespace,
                expected_identity=observed['identity'], manifest=manifest, marker='HOTEL_STARTUP_' + name.upper())
            actual_args = ast.literal_eval(ast.parse(shlex.split(command)[-1]).body[-1].value.args[0].args[0])
            for drift in ('ca', 'namespace', 'system', 'admission', 'partial'):
                with self.subTest(operation=name, drift=drift):
                    self.local_drift = drift
                    before = list(self.local_mutations)
                    reason = {'ca': 'server CA', 'namespace': 'Namespace', 'system': 'Namespace',
                              'admission': 'admission mutators', 'partial': 'incomplete or paginated'}[drift]
                    with self.assertRaisesRegex(ValueError, reason): self.run_mutation_script(command, actual_args, response)
                    self.assertEqual(self.local_mutations, before)
            self.local_drift = None
            before = len(self.local_mutations)
            expected = ({'schema_version': 1, 'candidate_schema': 'hotel-startup-create-acknowledgment-v1',
                         'pods': response} if name == 'create' else response)
            self.assertEqual(self.run_mutation_script(command, actual_args, response), expected)
            self.assertEqual(len(self.local_mutations) - before, 1)

    def _required_matrix_cell(self):
        fields = self.matrix_case_key.split(':')
        if fields[0] == 'boundary': self._boundary_failure_cell(fields[1], fields[2])
        else: self._identity_drift_cell(fields[1])

    def _boundary_failure_cell(self, component, boundary):
        case = self
        consumed = [False]
        def fail(kind, job):
            state = job.get(controller.JOB_KEY, {}).get('state')
            if consumed[0] or state != component + '-dispatch-intent': return
            if ((boundary == 'intent-persist' and kind == 'persist') or
                (boundary == 'release-request' and kind == 'release')):
                consumed[0] = True
                raise OSError('synthetic https://private/?signed=must-not-persist')
        case.hook = fail
        if boundary == 'lost-response': case.lost = component
        with self.assertRaises((OSError, TimeoutError)): case.run_startup()
        completed = ORDER.index(component)
        expected_attempts = list(ORDER[:completed + (boundary != 'intent-persist')])
        self.assertEqual(case.dispatch_attempts, expected_attempts)
        self.assertEqual(case.releases, list(ORDER[:completed + (boundary == 'lost-response')]))
        self.assertLessEqual(sum(k == 'create' for k, _ in case.calls), 1)
        for target in ORDER: self.assertLessEqual(case.dispatch_attempts.count(target), 1)
        record = case.job[controller.JOB_KEY]
        self.assertEqual(record['state'], 'failed-cleanup-only')
        self.assertLessEqual(record['startup_elapsed_seconds'], 900)
        self.assertNotIn('signed=must-not-persist', json.dumps(record))
        before = list(case.calls)
        with self.assertRaises(ValueError): case.run_startup()
        self.assertEqual(case.calls, before)

    def _identity_drift_cell(self, boundary):
        case = self
        def drift(kind, job):
            if kind != 'completion': return
            if boundary == 'source': object.__setattr__(case.fixture.artifacts, 'fingerprint', 'sha256:' + '0' * 64)
            elif boundary == 'capability': object.__setattr__(case.fixture.image_access, '_fingerprint', 'sha256:' + '0' * 64)
            elif boundary == 'lease': case.lease.release()
            elif boundary == 'route': job['resources'][case.cluster.host('application').host_key] = '203.0.113.99'
        case.hook = drift
        if boundary == 'cloud':
            original = case.audit
            def changed_cloud(job, lease):
                value = original(job, lease)
                value['cloud_identity']['create_uid'] = 'replacement'
                return value
            case.audit = changed_cloud
        elif boundary == 'storage':
            original = case.execute
            def changed_volume(job, command, **kwargs):
                output = original(job, command, **kwargs)
                if 'AZURE_DSB_DATABASE_VOLUME' in command:
                    output = output.replace(case.init_plan.storage.filesystem_uuid, '11111111-1111-4111-8111-111111111111')
                return output
            case.execute = changed_volume
        elif boundary in ('support', 'namespace'):
            def changed_wire(kind, wire):
                if kind != 'kubernetes': return
                if boundary == 'namespace':
                    values = next(c['items'] for c in wire['collections'].values() if c['kind'] == 'NamespaceList')
                    next(p for p in values if p['metadata']['name'] == case.plan.namespace)['metadata']['uid'] = init_fixture.support_fixtures.identity('Namespace', 'replacement')
                else:
                    values = next(c['items'] for c in wire['collections'].values() if c['kind'] == 'PodList')
                    pod = next(p for p in values if p['spec']['containers'][0]['name'] == 'consul')
                    pod['status']['containerStatuses'][0]['restartCount'] = 1
            case.response_hook = changed_wire
        with self.assertRaises(Exception): case.run_startup()
        self.assertEqual(case.releases, [])
        self.assertNotIn('create', [kind for kind, _ in case.calls])
        record = case.job.get(controller.JOB_KEY, {})
        self.assertIsNot(record.get('startup_execution_observed'), True)
        self.assertIsNot(record.get('service_identity_observed'), True)
