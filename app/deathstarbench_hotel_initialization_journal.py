"""Private, leased, append-only Hotel dispatch-intent records; not an executor.

No method creates Kubernetes resources, releases scheduling gates or authorizes
database/application execution. Cluster observations and command output remain
unverified historical inputs. A future live controller needs independent exact
resource/spec/ownership checks in addition to this journal. In particular, a
successful initializer response is not permission to replay it or start apps.

Each intent first creates a private, fsynced, exclusive, run-wide latch. There
is no delete/reset/repair API. A lost write response, malformed partial record,
reopened latched attempt or changed observation requires cleanup, not replay.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat

from .deathstarbench_hotel_image_access import require_guarded_hotel_image_access
from .deathstarbench_hotel_initialization_stage import validate_guarded_hotel_initialization_stage
from .deathstarbench_hotel_runtime import (
    validate_guarded_hotel_cluster_observation, validate_guarded_hotel_runtime_plan,
    validate_saved_guarded_hotel_cluster_observation,
)
from .deathstarbench_hotel_initialization import _fingerprint as _observation_fingerprint
from .run_lease import require_acquired_run_lease


SCHEMA = 'hotel-guarded-initialization-intent-journal-v1'
PREFIX = '.hotel-initialization-event-'
LATCHES = {mode: '.hotel-initialization-' + mode + '.latch'
           for mode in ('initialize-empty', 'verify')}
EVENTS = ('created', 'blocked-pods-recorded', 'initialize-intent-recorded',
          'initialize-exit-recorded', 'verify-intent-recorded', 'verify-exit-recorded')
MAX_BYTES = 65536
UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')
CLOSED = {'initializer_authorized': False, 'runtime_authorized': False,
          'database_execution_verified': False, 'live_identity_verified': False,
          'application_start_allowed': False, 'dataset_qualified': False,
          'measurement_qualified': False, 'public_release': False}


class GuardedHotelJournalError(ValueError):
    """Intent evidence is uncertain; it must not be repaired into permission."""


def _require(condition, message):
    if not condition:
        raise GuardedHotelJournalError(message)


def _canonical(value):
    try:
        raw = (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()
        _require(0 < len(raw) <= MAX_BYTES, 'Hotel journal record exceeds its bound.')
        return raw
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise GuardedHotelJournalError('Hotel journal requires bounded finite JSON.') from exc


def _json(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            _require(key not in value, 'Hotel journal JSON repeats a field.')
            value[key] = item
        return value
    try:
        _require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES and not raw.startswith(b'\xef\xbb\xbf'),
                 'Hotel journal requires bounded UTF-8 without a BOM.')
        return json.loads(raw.decode(), object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(GuardedHotelJournalError('Nonfinite journal JSON.')))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise GuardedHotelJournalError('Hotel journal JSON is malformed; cleanup is required.') from exc


def _fingerprint(value):
    return 'sha256:' + hashlib.sha256(_canonical(value)).hexdigest()


def _uid(value):
    _require(type(value) is str and UUID.fullmatch(value) and value != '00000000-0000-0000-0000-000000000000',
             'An explicit observed nonzero Pod UID is required.')
    return value


def _verifier_uid(value, observation):
    value = _uid(value)
    retained = {item for key, item in observation.items() if key.endswith('_uid')}
    for row in observation['databases']:
        retained.update(item for key, item in row.items() if key.endswith('_uid'))
    _require(value not in retained, 'Verifier Pod UID must differ from every pinned Kubernetes object UID.')
    return value


def _success(output, mode, plan):
    raw = output.encode() if type(output) is str else output
    _require(type(raw) is bytes and len(raw) <= 4096, 'Hotel command evidence exceeds its bound.')
    value = _json(raw)
    expected = {'candidate_only': True, 'released': False, 'application_start_allowed': False,
                'runtime_qualified': False, 'measurement_qualified': False,
                'run_id': plan.run_id, 'contract_sha256': plan.snapshot()['dataset_fingerprint'],
                'mode': mode, 'database_verification_completed': True}
    _require(_canonical(value) == _canonical(expected), 'Hotel command evidence differs from its exact run/dataset/mode.')
    return value


class GuardedHotelInitializationJournal:
    """Local intent storage under an original acquired run lease, never authority."""

    def __init__(self, plan, stage, *, lease):
        self.plan = validate_guarded_hotel_runtime_plan(plan)
        self.stage = validate_guarded_hotel_initialization_stage(self.plan, stage)
        self.lease = lease
        self._events = []
        self._uncertain = False
        self._pid = os.getpid()
        self._check_context()
        descriptor = self._directory()
        try:
            events, latches = self._read_all(descriptor)
            if not events:
                _require(not latches, 'An orphan dispatch latch requires cleanup, never a new initialization attempt.')
                self._exclusive(descriptor, self._event_name(0), self._event(0, {}))
                events, latches = self._read_all(descriptor)
            self._events = events
            self._opened_latched = bool(latches)
        finally:
            os.close(descriptor)

    def _check_context(self):
        _require(self._pid == os.getpid() and not self._uncertain,
                 'Another process or uncertain journal operation requires cleanup-only recovery.')
        require_acquired_run_lease(self.lease, self.plan.run_id)
        validate_guarded_hotel_runtime_plan(self.plan)
        validate_guarded_hotel_initialization_stage(self.plan, self.stage)

    def _directory(self):
        self._check_context()
        path = self.lease.path.parent
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            opened = os.fstat(descriptor)
            named = os.stat(path, follow_symlinks=False)
            _require(stat.S_ISDIR(opened.st_mode) and opened.st_uid == os.getuid()
                     and stat.S_IMODE(opened.st_mode) & 0o077 == 0
                     and (opened.st_dev, opened.st_ino) == (named.st_dev, named.st_ino),
                     'Hotel intent journal needs the exact private leased run directory.')
            require_acquired_run_lease(self.lease, self.plan.run_id)
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _identity(self):
        return {'schema_version': 1, 'candidate_schema': SCHEMA, 'candidate_only': True,
                'scope': 'historical-unverified-observations-and-dispatch-intents-only-no-executor',
                'run_id': self.plan.run_id, 'plan_fingerprint': self.plan.fingerprint,
                'stage_fingerprint': self.stage.fingerprint,
                'initialization_attempt_id': self.plan.initialization_attempt_id, **CLOSED}

    @staticmethod
    def _event_name(index):
        return PREFIX + str(index).zfill(2) + '.json'

    def _event(self, index, payload):
        return {**self._identity(), 'sequence': index, 'event': EVENTS[index],
                'previous_event_sha256': _fingerprint(self._events[-1]) if index else None,
                'payload': payload}

    @staticmethod
    def _signature(details):
        return (details.st_dev, details.st_ino, details.st_uid, details.st_mode, details.st_nlink,
                details.st_size, details.st_mtime_ns, details.st_ctime_ns)

    def _read(self, directory, name):
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            before = os.fstat(descriptor)
            _require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_uid == os.getuid()
                     and stat.S_IMODE(before.st_mode) == 0o600 and 0 < before.st_size <= MAX_BYTES,
                     'Hotel journal records must be bounded private single-link regular files.')
            raw = bytearray()
            while len(raw) <= MAX_BYTES:
                part = os.read(descriptor, min(65536, MAX_BYTES + 1 - len(raw)))
                if not part:
                    break
                raw.extend(part)
            after = os.fstat(descriptor)
            named = os.stat(name, dir_fd=directory, follow_symlinks=False)
            _require(self._signature(before) == self._signature(after)
                     == self._signature(named) and len(raw) == before.st_size,
                     'Hotel journal record changed while reading.')
            value = _json(bytes(raw))
            _require(bytes(raw) == _canonical(value), 'Hotel journal records must remain canonical and complete.')
            return value
        finally:
            os.close(descriptor)

    def _exclusive(self, directory, name, value):
        # Never unlink a failed/partial record. Its presence deliberately
        # prevents a fresh attempt from repairing an ambiguous dispatch.
        self._check_context()
        raw = _canonical(value)
        descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=directory)
        try:
            _require(stat.S_IMODE(os.fstat(descriptor).st_mode) == 0o600, 'Private journal mode was not established.')
            view = memoryview(raw)
            while view:
                count = os.write(descriptor, view)
                _require(count > 0, 'Hotel journal write made no progress.')
                view = view[count:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.fsync(directory)
        self._check_context()

    def _read_all(self, directory):
        names = set(os.listdir(directory))
        expected_names = {self._event_name(index) for index in range(len(EVENTS))}
        _require(not {name for name in names if name.startswith(PREFIX)} - expected_names,
                 'Hotel journal contains unexpected or exhausted event names.')
        _require(not {name for name in names if name.startswith('.hotel-initialization-')
                      and name.endswith('.latch')} - set(LATCHES.values()),
                 'Hotel journal contains an unknown dispatch latch.')
        events = []
        for index in range(len(EVENTS)):
            name = self._event_name(index)
            if name not in names:
                _require(not names.intersection({self._event_name(i) for i in range(index + 1, len(EVENTS))}),
                         'Hotel journal event sequence contains a gap.')
                break
            value = self._read(directory, name)
            expected = {**self._identity(), 'sequence': index, 'event': EVENTS[index],
                        'previous_event_sha256': _fingerprint(events[-1]) if index else None,
                        'payload': value.get('payload')}
            _require(_canonical(value) == _canonical(expected) and type(value['payload']) is dict,
                     'Hotel journal has crossed identity, sequence, gates or fields.')
            payload = value['payload']
            if index == 0:
                _require(payload == {}, 'Initial journal event cannot contain execution observations.')
            elif index == 1:
                _require(set(payload) == {'cluster_observation', 'cluster_observation_fingerprint', 'verifier_pod_uid'},
                         'Blocked Pod identity fields differ.')
                validate_saved_guarded_hotel_cluster_observation(self.plan, payload['cluster_observation'])
                _verifier_uid(payload['verifier_pod_uid'], payload['cluster_observation'])
                _require(payload['cluster_observation_fingerprint'] == _observation_fingerprint(payload['cluster_observation']),
                         'Historical cluster observation fingerprint differs.')
            elif index in (2, 4):
                mode = 'initialize-empty' if index == 2 else 'verify'
                _require(set(payload) == {'mode', 'pod_uid', 'image', 'image_access_fingerprint'}
                         and payload['mode'] == mode and payload['image'] == self.plan.application_image,
                         'Intent must bind the exact guarded image and mode.')
                pinned = events[1]['payload']
                uid = (pinned['cluster_observation']['initializer_pod_uid'] if index == 2
                       else pinned['verifier_pod_uid'])
                _require(payload['pod_uid'] == uid, 'Intent Pod UID differs from the pinned blocked Pod.')
                _require(type(payload['image_access_fingerprint']) is str
                         and re.fullmatch(r'sha256:[0-9a-f]{64}', payload['image_access_fingerprint']),
                         'Intent requires the original fresh byte-access observation fingerprint.')
            else:
                mode = 'initialize-empty' if index == 3 else 'verify'
                _require(set(payload) == {'pod_uid', 'exit_code', 'output'}, 'Exit observation fields differ.')
                _require(type(payload['exit_code']) is int and payload['exit_code'] == 0
                         and payload['pod_uid'] == events[index - 1]['payload']['pod_uid'],
                         'Exit observation must bind the exact latched Pod and status.')
                _success(_canonical(payload['output']), mode, self.plan)
            events.append(value)
        latches = {}
        for mode, name in LATCHES.items():
            if name in names:
                value = self._read(directory, name)
                _require(set(value) == {'identity', 'intent'} and value['identity'] == self._identity(),
                         'Hotel dispatch latch belongs to another plan or attempt.')
                latches[mode] = value
        for index, mode in ((2, 'initialize-empty'), (4, 'verify')):
            if len(events) > index:
                _require(mode in latches and latches[mode]['intent'] == events[index]['payload'],
                         'A saved dispatch event without its original latch requires cleanup.')
        return events, latches

    def _append(self, payload, *, latch_mode=None):
        self._check_context()
        _require(not self._opened_latched, 'A reopened latched attempt is cleanup-only, even after a successful exit.')
        directory = self._directory()
        try:
            current, latches = self._read_all(directory)
            _require(current == self._events, 'Hotel intent history changed during this process.')
            recorded_modes = {mode for index, mode in ((2, 'initialize-empty'), (4, 'verify'))
                              if len(current) > index}
            _require(set(latches) == recorded_modes,
                     'An unexpected or orphan dispatch latch requires cleanup, even in the original process.')
            if latch_mode is not None:
                _require(latch_mode not in latches, 'A dispatch latch can never be consumed twice.')
                self._exclusive(directory, LATCHES[latch_mode], {'identity': self._identity(), 'intent': payload})
            event = self._event(len(current), payload)
            self._exclusive(directory, self._event_name(len(current)), event)
            after, after_latches = self._read_all(directory)
            expected_latches = recorded_modes | ({latch_mode} if latch_mode is not None else set())
            _require(after == [*current, event] and set(after_latches) == expected_latches,
                     'Hotel intent history or dispatch latches changed during persistence.')
            self._events = after
        except BaseException:
            self._uncertain = True
            raise
        finally:
            os.close(directory)

    def record_blocked_pods(self, observation, *, verifier_pod_uid):
        _require(len(self._events) == 1, 'Blocked Pods may be recorded exactly once before dispatch.')
        checked = validate_guarded_hotel_cluster_observation(self.plan, observation)
        _verifier_uid(verifier_pod_uid, checked.metadata())
        self._append({'cluster_observation': checked.metadata(),
                      'cluster_observation_fingerprint': checked.fingerprint,
                      'verifier_pod_uid': _uid(verifier_pod_uid)})
        return self.metadata()

    def record_dispatch_intent(self, mode, *, image_access):
        _require(type(mode) is str and mode in LATCHES, 'Only exact initialize-empty and verify intents exist.')
        index = 2 if mode == 'initialize-empty' else 4
        _require(len(self._events) == index, 'Dispatch ordering or one-use policy differs.')
        access = require_guarded_hotel_image_access(image_access, self.plan.artifacts)
        pinned = self._events[1]['payload']
        pod_uid = pinned['cluster_observation']['initializer_pod_uid'] if index == 2 else pinned['verifier_pod_uid']
        self._append({'mode': mode, 'pod_uid': pod_uid, 'image': self.plan.application_image,
                      'image_access_fingerprint': access.fingerprint}, latch_mode=mode)
        return self.metadata()

    def record_exit_observation(self, mode, *, pod_uid, exit_code, output):
        _require(type(mode) is str and mode in LATCHES, 'Only exact initialize-empty and verify evidence exists.')
        index = 3 if mode == 'initialize-empty' else 5
        _require(len(self._events) == index and type(exit_code) is int and exit_code == 0,
                 'Only the exact latched successful local command may be recorded in order.')
        _require(_uid(pod_uid) == self._events[index - 1]['payload']['pod_uid'], 'Exit Pod UID differs.')
        self._append({'pod_uid': pod_uid, 'exit_code': exit_code, 'output': _success(output, mode, self.plan)})
        return self.metadata()

    def metadata(self):
        self._check_context()
        directory = self._directory()
        try:
            current, latches = self._read_all(directory)
            _require(current == self._events, 'Hotel journal changed after its last retained event.')
            return {**self._identity(), 'events': json.loads(_canonical(current)),
                    'state': EVENTS[len(current) - 1], 'event_count': len(current),
                    'resume_policy': 'cleanup-only-no-replay' if latches else 'blocked-preflight-only',
                    'dispatch_latches': sorted(latches), 'journal_is_execution_permission': False}
        finally:
            os.close(directory)
