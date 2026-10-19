"""One-shot private startup intent/history, never execution authority.

The original controller retains live initializer/support proof, admission pins,
clock, hosts, access capability and acquired lease. These records cannot restore
any of them. Even a created-only invocation cannot be reopened. Failed writes
leave their evidence in place: there is no resume, repair, unlink or executor.

Normalized publication/source rows only reconcile historical assertions. Task4
must independently verify actual raw producers, Service-to-EndpointSlice owners
and ports, and live execution/custody. Slice UID membership is not owner proof.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat

from .deathstarbench_hotel_startup_identity import validate_startup_hotel_admitted_pod
from .deathstarbench_candidate_k3s import _uid as _checked_uid
from .deathstarbench_hotel_startup_image_access import require_startup_hotel_image_access
from .deathstarbench_hotel_startup_runtime import validate_startup_hotel_runtime_plan
from .deathstarbench_hotel_startup_stage import validate_startup_hotel_startup_stage
from .run_lease import require_acquired_run_lease


SCHEMA = 'hotel-startup-intent-journal-v1'
PREFLIGHT_SCHEMA = 'hotel-startup-create-preflight-v1'
PREFIX = '.hotel-startup-'
MAX_BYTES = 1024 * 1024
MAX_OBSERVATION_BYTES = 4 * MAX_BYTES
RELEASE_ORDER = ('attractions', 'geo', 'profile', 'rate', 'recommendation',
                 'reservation', 'review', 'user', 'search', 'frontend')
LATCHES = {'create': PREFIX + 'create.latch', **{
    c: PREFIX + 'component-' + c + '.latch' for c in RELEASE_ORDER}}
EVENTS = ('created', 'create-intent-recorded', 'admitted-pods-recorded',
          *(name for _ in RELEASE_ORDER for name in
            ('dispatch-intent-recorded', 'component-observation-recorded')),
          'final-observation-recorded')
CLOSED = {name: False for name in (
    'initializer_authorized', 'runtime_authorized', 'startup_authorized',
    'released', 'initialization_executed', 'application_start_allowed',
    'application_started', 'application_ready', 'startup_executed',
    'dataset_qualified', 'runtime_qualified', 'measurement_qualified',
    'cloud_qualified', 'public_release')}
_DIGEST = re.compile(r'sha256:[0-9a-f]{64}\Z')
_POD_FIELDS = ('component', 'pod_name', 'pod_uid', 'resource_version',
               'observed_resource_version', 'released', 'node_name', 'pod_ip',
               'container_id', 'image', 'image_id', 'started_at', 'state')
_PREFLIGHT_DIGESTS = ('initialization_plan_fingerprint', 'initialization_stage_fingerprint',
                      'initialization_identity_fingerprint', 'support_identity_fingerprint')


class StartupHotelJournalError(ValueError):
    """Uncertain startup history requires cleanup, never another dispatch."""


def _require(condition, message):
    if not condition:
        raise StartupHotelJournalError(message)


def _canonical(value, limit=MAX_BYTES):
    try:
        raw = (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode('utf-8')
        _require(0 < len(raw) <= limit, 'Startup journal JSON exceeds its bounded size.')
        return raw
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise StartupHotelJournalError('Startup journal requires bounded finite JSON.') from None


def _fingerprint(value, limit=MAX_BYTES):
    return 'sha256:' + hashlib.sha256(_canonical(value, limit)).hexdigest()


def _json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, 'Startup journal has duplicate JSON keys.')
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        _require(raw == _canonical(value), 'Startup journal must remain canonical and complete.')
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise StartupHotelJournalError('Startup journal evidence is malformed; cleanup is required.') from None


def _components(value, allowed):
    _require(type(value) is list and all(type(c) is str for c in value)
             and value == sorted(set(value)) and set(value) <= set(allowed),
             'Startup component inventory must be an exact sorted unique list.')
    return set(value)


class StartupHotelStartupJournal:
    """Append-only history using only the original current-process run lease."""

    def __init__(self, plan, stage, *, lease):
        self.plan = plan
        self.stage = stage
        self.lease = lease
        self._original_lease = lease
        self._bound_identity = None
        self._pid = os.getpid()
        self._uncertain = False
        self._events = []
        self._latches = {}
        self._pins = None
        self._check_context()
        self._bound_identity = _canonical(self._identity())
        directory = self._directory()
        try:
            _require(not any(name.startswith(PREFIX) for name in os.listdir(directory)),
                     'Any existing startup invocation is cleanup-only, including created-only history.')
            event = self._event(0, {})
            self._exclusive(directory, self._event_name(0), event)
            self._events = [event]
            self._read_all(directory)
        except BaseException:
            self._uncertain = True
            raise
        finally:
            os.close(directory)

    def _check_context(self):
        _require(os.getpid() == self._pid and not self._uncertain,
                 'Uncertain or other-process startup journal is cleanup-only.')
        _require(self.lease is self._original_lease,
                 'Startup history cannot replace its original acquired execution lease.')
        require_acquired_run_lease(self.lease, self.plan.run_id)
        validate_startup_hotel_runtime_plan(self.plan)
        validate_startup_hotel_startup_stage(self.plan, self.stage)
        _require(self._bound_identity is None or self._bound_identity == _canonical(self._identity()),
                 'Startup journal identity differs from its original invocation.')

    def _directory(self):
        self._check_context()
        path = self.lease.path.parent
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            opened, named = os.fstat(fd), os.stat(path, follow_symlinks=False)
            _require(stat.S_ISDIR(opened.st_mode) and opened.st_uid == os.getuid()
                     and stat.S_IMODE(opened.st_mode) & 0o077 == 0
                     and (opened.st_dev, opened.st_ino) == (named.st_dev, named.st_ino),
                     'Startup journal requires the exact private leased directory.')
            require_acquired_run_lease(self.lease, self.plan.run_id)
            return fd
        except BaseException:
            os.close(fd)
            raise

    def _identity(self):
        return {'schema_version': 1, 'candidate_schema': SCHEMA, 'candidate_only': True,
                'scope': 'historical-unverified-observations-and-dispatch-intents-only-no-executor',
                'run_id': self.plan.run_id, 'initialization_attempt_id': self.plan.initialization_attempt_id,
                'startup_attempt_id': self.plan.startup_attempt_id,
                'plan_fingerprint': self.plan.fingerprint, 'stage_fingerprint': self.stage.fingerprint,
                'create_manifest_sha256': self.stage.rendered_manifest_sha256, **CLOSED}

    @staticmethod
    def _event_name(index):
        return PREFIX + 'event-' + str(index).zfill(2) + '.json'

    def _event(self, index, payload):
        _require(index < len(EVENTS), 'Startup journal is exhausted.')
        return {**self._identity(), 'sequence': index, 'event': EVENTS[index],
                'previous_event_sha256': _fingerprint(self._events[-1]) if index else None,
                'payload': payload}

    @staticmethod
    def _signature(details):
        return (details.st_dev, details.st_ino, details.st_uid, details.st_mode,
                details.st_nlink, details.st_size, details.st_mtime_ns, details.st_ctime_ns)

    @staticmethod
    def _private(details):
        _require(stat.S_ISREG(details.st_mode) and details.st_nlink == 1
                 and details.st_uid == os.getuid() and stat.S_IMODE(details.st_mode) == 0o600,
                 'Startup evidence requires a private single-link regular file.')

    def _read(self, directory, name):
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            before = os.fstat(fd)
            self._private(before)
            _require(0 < before.st_size <= MAX_BYTES, 'Startup record exceeds its size bound or is empty.')
            raw = bytearray()
            while len(raw) <= MAX_BYTES:
                part = os.read(fd, min(65536, MAX_BYTES + 1 - len(raw)))
                if not part: break
                raw.extend(part)
            _require(self._signature(before) == self._signature(os.fstat(fd))
                     == self._signature(os.stat(name, dir_fd=directory, follow_symlinks=False))
                     and len(raw) == before.st_size, 'Startup record changed during its read.')
            return _json(bytes(raw))
        finally:
            os.close(fd)

    def _exclusive(self, directory, name, value):
        self._check_context()
        raw = _canonical(value)
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        try:
            before = os.fstat(fd)
            self._private(before)
            view = memoryview(raw)
            while view:
                count = os.write(fd, view)
                _require(type(count) is int and 0 < count <= len(view), 'Startup write made no progress.')
                view = view[count:]
            os.fsync(fd)
            after, named = os.fstat(fd), os.stat(name, dir_fd=directory, follow_symlinks=False)
            self._private(after)
            _require(self._signature(after) == self._signature(named)
                     and (before.st_dev, before.st_ino) == (after.st_dev, after.st_ino)
                     and after.st_size == len(raw), 'Startup publication identity changed.')
        finally:
            os.close(fd)
        os.fsync(directory)
        self._check_context()

    def _read_all(self, directory):
        expected = {self._event_name(i): event for i, event in enumerate(self._events)}
        expected.update({LATCHES[key]: value for key, value in self._latches.items()})
        names = {name for name in os.listdir(directory) if name.startswith(PREFIX)}
        _require(names == set(expected), 'Startup journal has missing, unexpected or orphan evidence.')
        for name, value in expected.items():
            _require(_canonical(self._read(directory, name)) == _canonical(value),
                     'Startup history changed from original process custody.')

    def _append(self, payload, *, latch=None):
        self._check_context()
        directory = self._directory()
        try:
            self._read_all(directory)
            event = self._event(len(self._events), payload)
            _canonical(event)
            if latch is not None:
                _require(latch not in self._latches, 'Startup latch is never consumable twice.')
                latched = {'identity': self._identity(), 'intent': payload}
                self._exclusive(directory, LATCHES[latch], latched)
            self._exclusive(directory, self._event_name(len(self._events)), event)
            self._events.append(event)
            if latch is not None: self._latches[latch] = latched
            self._read_all(directory)
        except BaseException:
            self._uncertain = True
            raise
        finally:
            os.close(directory)

    def _observation(self, observation, released):
        """Recheck actual admission pins; persist hashes, never raw Pod/HTTP data."""
        _require(type(observation) is dict and observation.get('plan_fingerprint') == self.plan.fingerprint,
                 'Startup observation must bind the original plan.')
        evidence_hash = _fingerprint(observation, MAX_OBSERVATION_BYTES)
        try:
            nodes = observation['identity']['nodes']
            _require(type(nodes) is list and all(type(n) is dict for n in nodes), 'Startup nodes are malformed.')
            roles = {n['role']: n['name'] for n in nodes}
            _require(len(roles) == len(nodes) and all(type(n) is str and re.fullmatch(
                r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', n) for n in roles.values()),
                'Startup node identity is malformed.')
            pods = observation['startup_pods']
            _require(type(pods) is dict and set(pods) == set(RELEASE_ORDER), 'All ten exact admitted Pods are required.')
            checked, safe = {}, {}
            for c in RELEASE_ORDER:
                p = pods[c]
                checked[c] = validate_startup_hotel_admitted_pod(
                    p['pod'], self.stage.pod(c), roles, pinned=self._pins[c] if self._pins else None,
                    released=c in released)
                _require(_canonical(p, MAX_OBSERVATION_BYTES) == _canonical(checked[c], MAX_OBSERVATION_BYTES),
                         'Normalized startup Pod differs from its actual observation/pin.')
                safe[c] = {k: checked[c][k] for k in _POD_FIELDS}
                safe[c].update(admitted_metadata_fingerprint=_fingerprint(checked[c]['admitted_metadata']),
                               admitted_spec_fingerprint=_fingerprint(checked[c]['admitted_spec']))
            _require(len({p['pod_uid'] for p in safe.values()}) == 10, 'Startup Pod UIDs alias each other.')
            actual_released = _components(observation['released_components'], RELEASE_ORDER)
            running = _components(observation['running_components'], RELEASE_ORDER)
            _require(actual_released == set(released) and running == {c for c, p in checked.items() if p['state'] == 'running'},
                     'Released/running startup inventories disagree with one-shot history.')
            pending, confirmed = {}, {}
            for channel, allowed in (('endpoints', RELEASE_ORDER), ('consul', RELEASE_ORDER[:-1])):
                pending[channel] = sorted(_components(observation['publication_pending'][channel], allowed))
                confirmed[channel] = sorted(_components(observation['publication_confirmed'][channel], allowed))
                _require(set(pending[channel]) <= set(released) and set(confirmed[channel]) <= running
                         and not set(pending[channel]) & set(confirmed[channel]), 'Publication sets disagree with execution.')
            proven = _components(observation.get('observed_running_components', []), RELEASE_ORDER)
            unproven = _components(observation.get('execution_pending_components', sorted(running)), RELEASE_ORDER)
            _require(proven <= running and unproven == running - proven
                     and type(observation.get('observed_running_count', 0)) is int
                     and observation.get('observed_running_count', 0) == len(proven), 'CRI-proven execution sets disagree.')
            _require(type(observation['startup_execution_observed']) is bool
                     and observation['startup_execution_observed'] == (bool(proven) and not unproven),
                     'Partial execution observation must not become all-ten readiness.')
            summary = {'observation_fingerprint': evidence_hash, 'pods': safe,
                       'released_components': sorted(released), 'running_components': sorted(running),
                       'observed_running_components': sorted(proven), 'execution_pending_components': sorted(unproven),
                       'publication_pending': pending, 'publication_confirmed': confirmed}
            return checked, summary
        except (KeyError, TypeError, AttributeError, IndexError, RecursionError):
            raise StartupHotelJournalError('Startup normalized observation is malformed or incomplete.') from None

    def _ready(self, observation, summary, components):
        # These are normalized-producer consistency checks, not a substitute
        # for Task4's actual raw Service/Slice owner/ports and live custody proof.
        _require(set(components) <= set(summary['observed_running_components'])
                 and not set(components) & set(summary['execution_pending_components']),
                 'Each observed component requires its actual CRI-proven Running execution.')
        try:
            current = set(components)
            backends = current - {'frontend'}
            _require(current == set(summary['released_components'])
                     and summary['publication_confirmed'] == {'endpoints': sorted(current), 'consul': sorted(backends)}
                     and summary['publication_pending'] == {'endpoints': [], 'consul': []},
                     'Ready history requires exact confirmed publications without pending components.')
            source = observation['identity']
            _require(type(source['objects']) is list, 'Publication source objects are required.')
            objects, uids = {}, set()
            for item in source['objects']:
                _require(type(item) is dict and set(item) == {'kind', 'name', 'namespace', 'uid'}
                         and type(item['kind']) is str and type(item['name']) is str
                         and (item['namespace'] is None or type(item['namespace']) is str),
                         'Publication source object identity is incomplete.')
                _checked_uid(item['uid'], 'Publication source object')
                key = (item['kind'], item['namespace'], item['name'])
                _require(key not in objects and item['uid'] not in uids, 'Publication source identities alias each other.')
                objects[key] = item['uid']
                uids.add(item['uid'])
            slices = {value for (kind, namespace, _), value in objects.items()
                      if kind == 'EndpointSlice' and namespace == self.plan.namespace}
            services = {item['metadata']['name']: item for item in
                        json.loads(self.plan.support_stage.canonical_json)['items'] if item['kind'] == 'Service'}
            endpoints = observation['endpoints']
            support_endpoints = source['support_endpoints']
            _require(type(endpoints) is dict and set(endpoints) == set(services)
                     and type(support_endpoints) is dict and set(support_endpoints) == set(services) - set(RELEASE_ORDER),
                     'Complete normalized endpoint and support source inventories are required.')
            seen_slices = set()
            pod_fields = ('pod_name', 'pod_uid', 'pod_ip', 'node_name')
            for name, endpoint in endpoints.items():
                _require(type(endpoint) is dict and set(endpoint) == {
                    'service_uid', 'endpoint_slice_uid', 'publication_pending', *pod_fields},
                    'Normalized endpoint row is incomplete or contains unexpected fields.')
                service = services[name]['metadata']
                _require(endpoint['service_uid'] == objects[('Service', service['namespace'], service['name'])]
                         and endpoint['endpoint_slice_uid'] in slices
                         and endpoint['endpoint_slice_uid'] not in seen_slices
                         and endpoint['publication_pending'] is False,
                         'Endpoint Service/Slice source identity or pending state differs.')
                seen_slices.add(endpoint['endpoint_slice_uid'])
                if name in RELEASE_ORDER:
                    _require(all(endpoint[k] == (summary['pods'][name][k] if name in current else None)
                                 for k in pod_fields), 'Application endpoint differs from its exact current Pod.')
                else:
                    _require(_canonical(endpoint) == _canonical(support_endpoints[name]),
                             'Support endpoint differs from its retained normalized source.')
            _require(seen_slices == slices,
                     'Complete normalized endpoint rows differ from the EndpointSlice source inventory.')
            consul = observation['consul']
            _require(type(consul) is dict and set(consul) == {
                'backend_registrations', 'frontend_registrations', 'intrinsic_consul', 'publication_pending'},
                'Complete normalized Consul result is required.')
            _require(_components(consul['publication_pending'], RELEASE_ORDER[:-1])
                     == set(summary['publication_pending']['consul']) == set(),
                     'Consul producer pending state contradicts confirmed history.')
            registrations = consul['backend_registrations']
            _require(type(registrations) is dict and set(registrations) == backends,
                     'Consul complete registrations must equal the exact confirmed backend set.')
            for c, registration in registrations.items():
                p = summary['pods'][c]
                port = self.stage.pod(c)['spec']['containers'][0]['ports'][0]['containerPort']
                expected = {'service_id': 'hotel-' + self.plan.run_id + '-' + self.plan.startup_attempt_id
                            + '-' + c + '-' + p['pod_uid'], 'pod_uid': p['pod_uid'], 'pod_ip': p['pod_ip'], 'port': port}
                _require(type(registration) is dict and type(registration.get('port')) is int
                         and _canonical(registration) == _canonical(expected),
                         'Complete Consul registration differs from original run/Pod/port identity.')
            containers = source['support_containers']
            _require(type(containers) is list and all(type(p) is dict for p in containers),
                     'Normalized support execution sources are required.')
            executions = [p for p in containers if p.get('component') == 'consul']
            _require(len(executions) == 1 and executions[0]['role'] == 'control',
                     'One exact normalized Consul execution source is required.')
            execution, endpoint = executions[0], support_endpoints['consul']
            _require(execution['pod_uid'] == objects[('Pod', self.plan.namespace, execution['pod_name'])]
                     and all(execution[k] == endpoint[k] for k in ('pod_name', 'pod_uid', 'node_name')),
                     'Consul execution and support endpoint sources disagree.')
            intrinsic = {**{k: execution[k] for k in ('pod_name', 'pod_uid', 'node_name', 'container_id')},
                         'pod_ip': endpoint['pod_ip'], 'service_id': 'consul', 'port': 8300,
                         'datacenter': 'dc1', 'catalog_node_address': '127.0.0.1'}
            _require(type(consul['intrinsic_consul']) is dict
                     and _canonical(consul['intrinsic_consul']) == _canonical(intrinsic),
                     'Complete intrinsic Consul row differs from its exact support sources.')
            _require(type(consul['frontend_registrations']) is int and consul['frontend_registrations'] == 0,
                     'Frontend must not register with Consul.')
        except (KeyError, TypeError, AttributeError):
            raise StartupHotelJournalError('Exact component publication is incomplete.') from None

    def record_create_intent(self, observation: dict) -> dict:
        self._check_context()
        _require(len(self._events) == 1, 'Create intent occurs exactly once before admissions.')
        expected = {'schema_version': 1, 'candidate_schema': PREFLIGHT_SCHEMA,
                    **{k: self._identity()[k] for k in ('run_id', 'initialization_attempt_id',
                       'startup_attempt_id', 'plan_fingerprint', 'stage_fingerprint')}}
        _require(type(observation) is dict and set(observation) == set(expected) | set(_PREFLIGHT_DIGESTS),
                 'Create preflight has an exact bounded historical assertion schema.')
        _require(all(type(observation[k]) is type(v) and observation[k] == v for k, v in expected.items())
                 and all(type(observation[k]) is str and _DIGEST.fullmatch(observation[k]) for k in _PREFLIGHT_DIGESTS),
                 'Create preflight identity or bounded fingerprints differ.')
        self._append({**observation, 'create_manifest_sha256': self.stage.rendered_manifest_sha256}, latch='create')
        return self.metadata()

    def record_admitted_pods(self, observation: dict) -> dict:
        self._check_context()
        _require(len(self._events) == 2, 'All gated admissions are recorded once after create intent.')
        checked, summary = self._observation(observation, ())
        self._append(summary)
        self._pins = checked
        return self.metadata()

    def record_dispatch_intent(self, component: str, observation: dict, *, image_access) -> dict:
        self._check_context()
        completed = (len(self._events) - 3) // 2
        _require(type(component) is str and 0 <= completed < 10
                 and len(self._events) == 3 + 2 * completed and component == RELEASE_ORDER[completed],
                 'Startup dispatch must be one-shot and in exact release order.')
        access = require_startup_hotel_image_access(image_access, self.plan.artifacts)
        _require(access is self.plan.access, 'Dispatch requires the original plan-bound image access capability.')
        checked, summary = self._observation(observation, RELEASE_ORDER[:completed])
        self._ready(observation, summary, RELEASE_ORDER[:completed])
        self._append({'component': component, **{k: summary['pods'][component][k] for k in
                      ('pod_uid', 'pod_name', 'admitted_metadata_fingerprint', 'admitted_spec_fingerprint')},
                      'image': self.plan.application_image, 'image_access_fingerprint': access.fingerprint,
                      'observation': summary}, latch=component)
        self._pins = checked
        return self.metadata()

    def record_component_observation(self, component: str, observation: dict) -> dict:
        self._check_context()
        current = (len(self._events) - 4) // 2
        _require(type(component) is str and 0 <= current < 10
                 and len(self._events) == 4 + 2 * current and component == RELEASE_ORDER[current],
                 'Component evidence must follow its exact one-shot dispatch intent.')
        checked, summary = self._observation(observation, RELEASE_ORDER[:current + 1])
        self._ready(observation, summary, RELEASE_ORDER[:current + 1])
        self._append({'component': component, 'observation': summary})
        self._pins = checked
        return self.metadata()

    def record_final_observation(self, observation: dict) -> dict:
        self._check_context()
        _require(len(self._events) == 23, 'Final evidence follows all ten ordered component observations.')
        checked, summary = self._observation(observation, RELEASE_ORDER)
        self._ready(observation, summary, RELEASE_ORDER)
        _require(summary['running_components'] == sorted(RELEASE_ORDER)
                 and summary['observed_running_components'] == sorted(RELEASE_ORDER)
                 and summary['publication_confirmed'] == {'endpoints': sorted(RELEASE_ORDER),
                      'consul': sorted(RELEASE_ORDER[:-1])}
                 and summary['publication_pending'] == {'endpoints': [], 'consul': []}
                 and set(observation['consul']['backend_registrations']) == set(RELEASE_ORDER[:-1]),
                 'Final history requires all ten exact executions/publications and nine backend joins.')
        self._append(summary)
        self._pins = checked
        return self.metadata()

    def metadata(self) -> dict:
        self._check_context()
        directory = self._directory()
        try:
            self._read_all(directory)
            return {**self._identity(), 'events': json.loads(_canonical(self._events, len(EVENTS) * MAX_BYTES)),
                    'state': self._events[-1]['event'], 'event_count': len(self._events),
                    'dispatch_latches': sorted(self._latches), 'resume_policy': 'cleanup-only-no-replay',
                    'journal_is_execution_permission': False}
        except BaseException:
            self._uncertain = True
            raise
        finally:
            os.close(directory)
