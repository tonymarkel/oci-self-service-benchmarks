"""Private fail-fast, one-shot startup under original live execution custody.

Only runtime and service identities are observed. No semantic, measurement or
public release authority is supplied. Uncertain writes are terminal: history
cannot reopen this controller, recover its admission pins, or restore a lease.
Raw Kubernetes/CRI/log data remains private in memory and sensitive transport.
"""
from __future__ import annotations

import base64
import hashlib
import inspect
import json
import math
import shlex
import time

from . import deathstarbench_candidate_deployment as support
from . import deathstarbench_distributed as distributed
from . import deathstarbench_hotel_live_k3s as initialization
from . import deathstarbench_hotel_startup_k3s as live
from .deathstarbench_candidate_operator import _audit
from .deathstarbench_candidate_runtime import _canonical
from .deathstarbench_candidate_storage_observation import (
    candidate_database_storage_observation_command, parse_candidate_database_storage_observation,
)
from .deathstarbench_hotel_initialization_journal import _success
from .deathstarbench_hotel_initialization_stage import validate_guarded_hotel_initialization_stage
from .deathstarbench_hotel_runtime import validate_guarded_hotel_runtime_plan
from .deathstarbench_hotel_startup_image_access import require_startup_hotel_image_access
from .deathstarbench_hotel_startup_journal import StartupHotelStartupJournal, RELEASE_ORDER
from .deathstarbench_hotel_startup_runtime import create_startup_hotel_runtime_plan
from .deathstarbench_hotel_startup_stage import build_startup_hotel_startup_stage
from .run_lease import require_acquired_run_lease

JOB_KEY = 'hotel_application_startup'
SCHEMA = 'hotel-live-startup-outcome-v1'
SUCCESS = 'startup-and-service-identities-observed'
MAX_BYTES = 4 * 1024 * 1024
_CLOSED = {name: False for name in (
    'released', 'application_start_allowed', 'application_started', 'application_ready', 'startup_executed',
    'initializer_authorized', 'runtime_authorized', 'startup_authorized', 'runtime_qualified',
    'measurement_qualified', 'dataset_qualified', 'cloud_qualified', 'public_release',
    'public_dispatch_enabled', 'semantic_correctness_observed',
)}


class StartupHotelControllerError(ValueError):
    """Terminal uncertainty; only exact-owned teardown may follow."""


def _require(condition, reason):
    if not condition:
        raise StartupHotelControllerError(reason)


def _copy(value):
    return json.loads(_canonical(value))


def _hash(value):
    return 'sha256:' + hashlib.sha256(_canonical(value).encode()).hexdigest()


def _completed(row):
    """Allowlisted execution continuity, including terminal timestamps/spec."""
    _require(row['state'] == 'terminated' and type(row['exit_code']) is int and row['exit_code'] == 0,
             'Both original initializer executions must have completed successfully.')
    terminal = row['pod']['status']['containerStatuses'][0]['state']['terminated']
    return {**{key: row[key] for key in ('name', 'uid', 'node_name', 'container_id', 'image_id', 'exit_code')},
            'spec': _copy(row['spec']),
            'started_at': terminal['startedAt'], 'finished_at': terminal['finishedAt']}


def _bridge(retained, observed, ready):
    old, new = retained['identity'], observed['identity']
    current = {(p['kind'], p['namespace'], p['name']): p['uid'] for p in new['objects']}
    attested = ready['support_attestation']
    for item in attested['inventory']['items']:
        meta = item['metadata']
        _require(current.get((item['kind'], meta.get('namespace'), meta['name'])) == meta['uid'],
                 'Original support-ready object identity changed before startup.')
    for receipt in ready['phase_receipts'].values():
        _require(all(current.get((p['kind'], p['namespace'], p['name'])) == p['uid'] for p in receipt['resources']),
                 'Original completed support receipt changed before startup.')
    _require(_canonical(new['support_containers']) == _canonical(attested['support_pods'])
             and _canonical(new['nodes']) == _canonical(attested['nodes'])
             and new['namespace_uid'] == attested['namespace_uid'],
             'Original support-ready execution, namespace or node identity changed.')
    _require(all(current.get((p['kind'], p['namespace'], p['name'])) == p['uid'] for p in old['objects']),
             'Original Kubernetes objects changed before startup.')
    for field in ('support_containers', 'namespace_uid', 'kube_system_namespace_uid',
                  'server_ca_sha256', 'admission_mutators'):
        _require(_canonical(old[field]) == _canonical(new[field]), 'Original support identity changed before startup.')
    _require(_canonical(retained['nodes']) == _canonical(new['nodes']), 'Original node identity changed before startup.')
    for name, prior in old['endpoints'].items():
        now = observed['endpoints'].get(name, {})
        _require(all(now.get(key) == value for key, value in prior.items()),
                 'Original EndpointSlice identity changed before startup.')


_REMOTE_COMPLETION_HELPERS = '\n'.join(inspect.getsource(function) for function in (
    initialization._exact, initialization._initialization_pod,
))


def _completion_command(plan, stage, roles, ca, remaining, *, completed_pins):
    context = initialization._context(plan, stage, roles, ca)
    context['completion_pods'] = {p['spec']['containers'][0]['args'][0]: p
        for p in json.loads(stage.canonical_json)['items'] if p['kind'] == 'Pod'}
    # Never transport retained raw Pod/status/messages in the command context.
    fields = ('name', 'uid', 'node_name', 'container_id', 'image_id', 'exit_code',
              'spec', 'started_at', 'finished_at')
    _require(set(completed_pins) == set(context['completion_pods'])
             and all(set(pin) == set(fields) for pin in completed_pins.values()),
             'Original completion pins are incomplete.')
    context['completed_pins'] = {mode: {key: _copy(pin[key]) for key in fields}
                               for mode, pin in completed_pins.items()}
    # run() charges actual UTF-8 bytes and bounds every process/read by <=15s.
    # Logs are themselves strict JSON; preserve their meaning, never messages.
    operations = r'''
# HOTEL_STARTUP_COMPLETION
def completed(raw, approved, pin):
    row = _initialization_pod(raw, approved, CONTEXT)
    if row['state'] != 'terminated' or type(row['exit_code']) is not int or row['exit_code'] != 0:
        raise CandidateK3sError('Completed initializer changed around log retrieval.')
    terminal = row['pod']['status']['containerStatuses'][0]['state']['terminated']
    actual = {key: row[key] for key in ('name', 'uid', 'node_name', 'container_id', 'image_id', 'exit_code', 'spec')}
    actual.update(started_at=terminal['startedAt'], finished_at=terminal['finishedAt'])
    _exact(actual, pin, 'Completed initializer around log retrieval')
document = {'pods': {}, 'logs': {}}
for mode, approved in CONTEXT['completion_pods'].items():
    name = approved['metadata']['name']
    pin = CONTEXT['completed_pins'][mode]
    args = [K3S_BINARY, 'kubectl', '-n', CONTEXT['namespace']]
    before = run(args + ['get', 'pod', name, '-o', 'json'])
    completed(before, approved, pin)
    used = USED
    log = run(args + ['logs', name, '-c', 'hotel-initialize', '--limit-bytes=4097'])
    if USED - used > 4096: raise CandidateK3sError('Initializer log exceeds its bound.')
    after = run(args + ['get', 'pod', name, '-o', 'json'])
    completed(after, approved, pin)
    document['pods'][mode] = {'before': before, 'after': after}
    document['logs'][mode] = _canonical(log)
emit(document)
'''
    operations = (f'SCHEDULING_GATE={initialization.SCHEDULING_GATE!r}\n'
                  + _REMOTE_COMPLETION_HELPERS + '\n' + operations)
    return 'sudo python3 -c ' + shlex.quote(live._script(context, live._deadline(remaining), operations))


_REMOTE_MUTATION_HELPERS = '\n'.join(inspect.getsource(function) for function in (
    initialization._admission_versions, initialization._admission_mutator_absence,
))

_REMOTE_MUTATION_CHECKS = r'''
with open('/var/lib/rancher/k3s/server/tls/server-ca.crt', 'rb') as certificate:
    ca_bytes = certificate.read(65537)
if len(ca_bytes) > 65536 or 'sha256:' + hashlib.sha256(ca_bytes).hexdigest() != CONTEXT['server_ca_sha256']:
    raise CandidateK3sError('Startup mutation server CA changed.')
for name, expected_uid in ((CONTEXT['namespace'], CONTEXT['namespace_uid']), ('kube-system', CONTEXT['kube_system_namespace_uid'])):
    namespace = kubectl('/api/v1/namespaces/' + name)
    metadata = namespace.get('metadata', {})
    if (namespace.get('apiVersion') != 'v1' or namespace.get('kind') != 'Namespace'
        or metadata.get('name') != name or metadata.get('uid') != expected_uid
        or metadata.get('deletionTimestamp') is not None or namespace.get('status', {}).get('phase') != 'Active'):
        raise CandidateK3sError('Startup mutation Namespace identity changed.')
group = kubectl('/apis/' + ADMISSION_GROUP)
admission = {'api_group': group, 'discovery': {}, 'inventories': {}}
for version in _admission_versions(group):
    group_version = ADMISSION_GROUP + '/' + version
    discovery = kubectl('/apis/' + group_version)
    admission['discovery'][group_version] = discovery
    for resource in discovery['resources']:
        if resource['name'] in MUTATOR_KINDS:
            path = group_version + '/' + resource['name']
            admission['inventories'][path] = kubectl('/apis/' + path)
if _canonical(_admission_mutator_absence(admission)) != _canonical(CONTEXT['admission_mutators']):
    raise CandidateK3sError('Startup mutation admission identity changed.')
'''


_REMOTE_CREATE = r'''
def create_documents(raw):
    try:
        text = raw.decode('utf-8')
        decoder = json.JSONDecoder()
        documents, names, uids = [], set(), set()
        cursor = 0
        while cursor < len(text):
            while cursor < len(text) and text[cursor] in ' \t\r\n': cursor += 1
            if cursor == len(text): break
            if len(documents) >= 10:
                raise CandidateK3sError('Startup create acknowledgment has excess documents.')
            begin = cursor
            _, cursor = decoder.raw_decode(text, cursor)
            if cursor < len(text) and text[cursor] not in ' \t\r\n':
                raise CandidateK3sError('Startup create acknowledgment has trailing garbage.')
            # Reuse the strict duplicate-key/nonfinite/UTF-8/depth validator;
            # raw_decode above locates document boundaries, not acceptance.
            pod = _strict_json(text[begin:cursor])
            if not isinstance(pod, dict) or pod.get('apiVersion') != 'v1' or pod.get('kind') != 'Pod':
                raise CandidateK3sError('Startup create acknowledgment must contain Pod documents.')
            metadata = _mapping(pod.get('metadata'), 'Startup create acknowledgment metadata')
            name, uid = metadata.get('name'), metadata.get('uid')
            _uid(uid, 'Startup create acknowledgment Pod')
            labels = _mapping(metadata.get('labels'), 'Startup create acknowledgment labels')
            if (not isinstance(name, str) or name not in CONTEXT['create_names']
                or metadata.get('namespace') != CONTEXT['namespace']
                or labels.get('app.kubernetes.io/component') != CONTEXT['create_names'][name]
                or name in names or uid in uids):
                raise CandidateK3sError('Startup create acknowledgment Pod identities are repeated or unrelated.')
            names.add(name)
            uids.add(uid)
            documents.append(pod)
        if len(documents) != 10 or names != set(CONTEXT['create_names']):
            raise CandidateK3sError('Startup create acknowledgment is incomplete.')
        return {'schema_version': 1, 'candidate_schema': 'hotel-startup-create-acknowledgment-v1', 'pods': documents}
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        if isinstance(exc, CandidateK3sError): raise
        raise CandidateK3sError('Startup create acknowledgment stream is invalid.') from None

def create(arguments):
    # Capture before owning a child: pthread_sigmask may apply its mask and
    # then raise a previously pending Python signal before returning it.
    original_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    original_handler = signal.getsignal(signal.SIGALRM)
    limit = remaining()
    signal.setitimer(signal.ITIMER_REAL, limit)
    until = time.monotonic() + limit
    selector = selectors.DefaultSelector()
    process = None
    raw = bytearray()
    failure = cleanup_failure = None
    try:
        try:
            process = subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True)
            selector.register(process.stdout, selectors.EVENT_READ)
            while selector.get_map():
                left = min(until, END) - time.monotonic()
                if left <= 0: raise CandidateK3sError('Startup create request deadline exceeded.')
                events = selector.select(left)
                if not events: raise CandidateK3sError('Startup create request timed out.')
                for key, event in events:
                    chunk = os.read(key.fileobj.fileno(), min(65536, MAX_JSON_BYTES - USED + 1))
                    if not chunk: selector.unregister(key.fileobj)
                    else: raw.extend(charge(chunk))
            left = min(until, END) - time.monotonic()
            if left <= 0: raise CandidateK3sError('Startup create request deadline exceeded.')
            if process.wait(timeout=left) != 0:
                raise CandidateK3sError('Startup create command failed; acknowledgment is uncertain.')
            remaining()
            document = create_documents(bytes(raw))
            remaining()
        except BaseException as exc:
            failure = exc
        finally:
            # The outer finally is already installed if the one-shot alarm
            # interrupts before/during this FIRST mask call. Never depend on
            # that call returning its old mask, or on an unbounded wait.
            signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
    except BaseException as exc:
        cleanup_failure = exc
    finally:
        # Either the first mask blocked the alarm, or its raising handler has
        # consumed it. Repeat with the independently captured original mask.
        try: signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
        except BaseException as exc: cleanup_failure = cleanup_failure or exc
        try: signal.setitimer(signal.ITIMER_REAL, 0)
        except BaseException as exc: cleanup_failure = cleanup_failure or exc
        try:
            # Exact original owned session/group, including exited leaders.
            if process is not None: os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except BaseException as exc:
            cleanup_failure = cleanup_failure or exc
        try:
            if process is not None: process.wait(timeout=1.0)
        except BaseException as exc:
            cleanup_failure = cleanup_failure or exc
        for close in (selector.close, process.stdout.close if process is not None else lambda: None):
            try: close()
            except BaseException as exc: cleanup_failure = cleanup_failure or exc
        # A blocked request alarm remains terminal. POSIX SIG_IGN discards
        # that pending signal without a blocking wait; restore both original
        # disposition and mask before leaving this private helper process.
        try:
            if signal.SIGALRM in signal.sigpending():
                cleanup_failure = cleanup_failure or CandidateK3sError('Startup create request alarm expired.')
            signal.signal(signal.SIGALRM, signal.SIG_IGN)
        except BaseException as exc:
            cleanup_failure = cleanup_failure or exc
        finally:
            try: signal.signal(signal.SIGALRM, original_handler)
            finally: signal.pthread_sigmask(signal.SIG_SETMASK, original_mask)
    if failure is not None: raise failure
    if cleanup_failure is not None: raise cleanup_failure
    if time.monotonic() >= min(until, END):
        raise CandidateK3sError('Startup create request deadline exceeded during cleanup.')
    # Only the original observation deadline is restored, never the request.
    signal.setitimer(signal.ITIMER_REAL, END - time.monotonic())
    return document
'''


def _mutation_command(arguments, remaining, *, namespace, expected_identity, manifest=None, marker):
    if manifest is not None:
        payload = base64.b64encode(manifest.encode()).decode()
        shell = ('printf %s ' + shlex.quote(payload) + ' | base64 -d | '
                 + shlex.join([live.K3S_BINARY, 'kubectl', *arguments]))
        arguments = ['/bin/bash', '-o', 'pipefail', '-c', shell]
    else:
        arguments = [live.K3S_BINARY, 'kubectl', *arguments]
    context = {key: _copy(expected_identity[key]) for key in (
        'server_ca_sha256', 'namespace_uid', 'kube_system_namespace_uid', 'admission_mutators')}
    context['namespace'] = namespace
    if manifest is not None:
        context['create_names'] = {pod['metadata']['name']: pod['metadata']['labels']['app.kubernetes.io/component']
                                   for pod in json.loads(manifest)['items']}
    operations = (f'ADMISSION_GROUP={initialization.ADMISSION_GROUP!r}\n'
                  f'MUTATOR_KINDS={initialization.MUTATOR_KINDS!r}\n'
                  + _REMOTE_MUTATION_HELPERS + '\n' + _REMOTE_MUTATION_CHECKS
                  + (_REMOTE_CREATE if manifest is not None else '')
                  + '# ' + marker + '\nemit(' + ('create' if manifest is not None else 'run')
                  + '(' + repr(arguments) + '))\n')
    return 'sudo python3 -c ' + shlex.quote(live._script(context, live._deadline(remaining), operations))


def _creation_pins(document, approved, roles):
    """Validate the write acknowledgment, never invent observer inventory."""
    _require(isinstance(document, dict)
             and set(document) == {'schema_version', 'candidate_schema', 'pods'}
             and type(document['schema_version']) is int and document['schema_version'] == 1
             and document['candidate_schema'] == 'hotel-startup-create-acknowledgment-v1'
             and isinstance(document['pods'], list) and len(document['pods']) == len(RELEASE_ORDER),
             'Startup create acknowledgment must contain the exact complete ten-Pod stream envelope.')
    pins = {}
    for raw in document['pods']:
        _require(isinstance(raw, dict) and isinstance(raw.get('metadata'), dict),
                 'Startup create acknowledgment contains an invalid Pod.')
        name = raw['metadata'].get('name')
        _require(isinstance(name, str) and name in approved, 'Startup create acknowledgment has an unrelated Pod.')
        pod = _copy(raw)
        # Same detached default projection as the actual Task2 Pod parser.
        pod['metadata'].pop('managedFields', None)
        pin = live.identity.validate_startup_hotel_admitted_pod(pod, approved[name], roles)
        component = pin['component']
        _require(component not in pins, 'Startup create acknowledgment repeats a Pod.')
        pins[component] = pin
    _require(set(pins) == set(RELEASE_ORDER) and len({pin['pod_uid'] for pin in pins.values()}) == len(RELEASE_ORDER),
             'Startup create acknowledgment has incomplete or aliased Pod identities.')
    return pins


def prepare_startup_hotel_applications(
    job, startup_artifacts, *, baseline_artifacts, initialization_plan, initialization_stage,
    lease, execute, persist, image_access, fresh_audit, expected_cloud_identity: dict,
    startup_attempt_id: str, emit=None, monotonic=time.monotonic,
) -> dict:
    """Start once before teardown, retaining the original lease/job/capabilities.

    SSH callback is the established execute(job, command, host_key=..., timeout=...)
    shape. No checkpoint, resume, mutate retry or injected failure API exists.
    """
    require_acquired_run_lease(lease, job.get('id'))
    _require(lease.path.parent.parent.name == '.candidate-support' and job.get('_persist_state') is False
             and job.get('results') in (None, []) and JOB_KEY not in job,
             'Startup requires a new private leased invocation with no prior startup outcome.')
    _require(all(callable(f) for f in (execute, persist, fresh_audit, monotonic))
             and (emit is None or callable(emit)), 'Startup requires durable callbacks and a monotonic clock.')
    validate_guarded_hotel_runtime_plan(initialization_plan)
    validate_guarded_hotel_initialization_stage(initialization_plan, initialization_stage)
    cluster, original_plan, original_stage, context = support._context(
        job, baseline_artifacts, distributed.distributed_k3s_candidate_plan)
    ready = support.validate_candidate_support_journal(job['resources'].get(support.SUPPORT_STAGE_JOURNAL_KEY),
                                                      original_plan, original_stage, context)
    _require(ready['state'] == support.READY_STATE, 'The original support stage is not complete.')
    plan = create_startup_hotel_runtime_plan(startup_artifacts, image_access, original_plan, original_stage,
        initialization_attempt_id=initialization_plan.initialization_attempt_id, startup_attempt_id=startup_attempt_id)
    stage = build_startup_hotel_startup_stage(plan)
    live._context(plan, stage, initialization_plan, initialization_stage, context['role_nodes'], context['server_ca_sha256'])
    retained = _copy(job.get('guarded_hotel_initialization'))
    _require(isinstance(retained, dict) and all(retained.get(k) == v for k, v in {
        'candidate_schema': 'hotel-guarded-live-initialization-outcome-v1', 'run_id': plan.run_id,
        'initialization_attempt_id': plan.initialization_attempt_id,
        'plan_fingerprint': initialization_plan.fingerprint, 'stage_fingerprint': initialization_stage.fingerprint,
        'guarded_artifact_fingerprint': initialization_plan.artifacts.fingerprint,
        'state': 'initialized-and-globally-verified-applications-paused', 'initialization_executed': True,
        'database_execution_verified': True, 'global_readonly_verification_completed': True,
    }.items()), 'Startup requires the retained lower initializer outcome under original custody.')
    _require(isinstance(expected_cloud_identity, dict) and bool(expected_cloud_identity),
             'Startup requires the original independently audited cloud identity.')
    cloud_identity = _copy(expected_cloud_identity)
    pinned = _canonical({k: job.get(k) for k in ('candidate_support_pin', 'hotel_initialization_pin', 'plan')})
    original_context, original_support = _canonical(context), _canonical(ready)
    original_initialization = _canonical(retained)
    current = None
    invocation_started = monotonic()
    _require(type(invocation_started) in (int, float) and math.isfinite(invocation_started), 'Startup clock is invalid.')
    started, deadline, last_time = None, None, invocation_started

    def remaining(until=None):
        nonlocal last_time
        now = monotonic()
        _require(type(now) in (int, float) and math.isfinite(now) and now >= last_time,
                 'Startup clock changed or moved backward.')
        last_time = now
        bound = min(deadline if deadline is not None else math.inf,
                    until if until is not None else math.inf)
        value = bound - now if math.isfinite(bound) else 60.0
        _require(value > 0, 'Startup deadline exceeded; cleanup only.')
        return value

    def window():
        remaining()
        return min(deadline if deadline is not None else math.inf, monotonic() + 60.0)

    def binding():
        remaining()
        require_acquired_run_lease(lease, plan.run_id)
        _require(_canonical({k: job.get(k) for k in ('candidate_support_pin', 'hotel_initialization_pin', 'plan')}) == pinned
                 and _canonical(job.get('guarded_hotel_initialization')) == original_initialization,
                 'Original initializer or operator inputs changed during a callback.')
        fresh_cluster, fresh_plan, fresh_stage, fresh_context = support._context(
            job, baseline_artifacts, distributed.distributed_k3s_candidate_plan)
        _require(fresh_plan.fingerprint == original_plan.fingerprint and fresh_stage.fingerprint == original_stage.fingerprint
                 and _canonical(fresh_context) == original_context
                 and _canonical(job['resources'].get(support.SUPPORT_STAGE_JOURNAL_KEY)) == original_support,
                 'Original provider, storage, runtime, support or host routing changed.')
        _require(_canonical(job.get(JOB_KEY)) == _canonical(current), 'Startup evidence changed outside its lifecycle.')
        require_startup_hotel_image_access(image_access, startup_artifacts)
        validate_guarded_hotel_runtime_plan(initialization_plan)
        validate_guarded_hotel_initialization_stage(initialization_plan, initialization_stage)
        remaining()
        return fresh_cluster

    def callback(function, *args):
        binding()
        try:
            return function(*args)
        finally:
            binding()

    def write(**changes):
        nonlocal current
        binding()
        current = _copy({**(current or {}), **changes})
        job[JOB_KEY] = _copy(current)
        callback(persist, job)

    def remote(role, command, until=None):
        fresh_cluster = binding()
        try:
            output = distributed._execute(execute, job, fresh_cluster.host(role), command,
                timeout=min(60.0, remaining(until)), sensitive_output=True)
        finally:
            binding()
            remaining(until)
        raw = output if isinstance(output, bytes) else output.encode('utf-8')
        _require(0 < len(raw) <= MAX_BYTES, 'Startup transport output exceeded its bound.')
        return output

    def prerequisites():
        audited = _audit(callback(fresh_audit, job, lease), job['candidate_support_pin'])
        _require(_canonical(audited['cloud_identity']) == _canonical(cloud_identity), 'Original cloud identity changed.')
        output = remote('database', distributed._database_volume_attestation_command(cluster, plan.storage.filesystem_uuid))
        volume = distributed._parse_plan_database_volume_attestation(output, cluster)
        _require(distributed._same_database_volume_identity(context['database_volume'], volume), 'Original database volume changed.')
        parse_candidate_database_storage_observation(remote('database', candidate_database_storage_observation_command(plan.storage)), plan.storage)

    completed = {mode: _completed(retained['last_observation']['pods'][mode]) for mode in ('initialize-empty', 'verify')}
    identity = None

    def kubernetes(until):
        nonlocal identity
        command = live.startup_hotel_observation_command(plan, stage, initialization_plan, initialization_stage,
            context['role_nodes'], context['server_ca_sha256'], deadline_remaining_seconds=remaining(until))
        output = remote('control', command, until)
        result = live.parse_startup_hotel_kubernetes_observation(output, plan, stage, initialization_plan,
            initialization_stage, context['role_nodes'], context['server_ca_sha256'], expected_identity=identity)
        _require(set(result['initialization_pods']) == set(completed), 'Completed initializer objects disappeared.')
        _require(all(_canonical(_completed(result['initialization_pods'][mode])) == _canonical(pin)
                     for mode, pin in completed.items()), 'Original completed initializer execution changed.')
        if identity is None:
            _bridge(retained['last_observation'], result, ready)
        identity = result['identity']
        remaining(until)
        return result

    released = []

    def observe():
        nonlocal identity
        until = window()
        first = kubernetes(until)
        if not first['running_components']:
            return first
        cri = remote('application', live.startup_hotel_cri_observation_command(plan, first['startup_pods'],
            deadline_remaining_seconds=remaining(until)), until)
        # identity is still the actual first read, including its measured bytes.
        final = kubernetes(until)
        result = live.validate_startup_hotel_live_observation(final, cri, plan, started_components=tuple(released))
        identity = result['identity']
        remaining(until)
        return result

    def ready_for(result, components):
        expected = set(components)
        return (set(result.get('observed_running_components', [])) == expected
                and set(result['running_components']) == expected
                and not result.get('execution_pending_components', [])
                and not any(result['publication_pending'].values())
                and set(result['publication_confirmed']['endpoints']) == expected
                and set(result['publication_confirmed']['consul']) == expected - {'frontend'})

    journal = StartupHotelStartupJournal(plan, stage, lease=lease)
    try:
        write(schema_version=1, candidate_schema=SCHEMA, candidate_only=True, run_id=plan.run_id,
            initialization_attempt_id=plan.initialization_attempt_id, startup_attempt_id=startup_attempt_id,
            plan_fingerprint=plan.fingerprint, stage_fingerprint=stage.fingerprint,
            state='preflight', startup_execution_observed=False, service_identity_observed=False,
            resume_policy='cleanup-only-no-replay', journal=journal.metadata(), **_CLOSED)
        prerequisites()
        before = kubernetes(window())
        _require(not before['startup_pods'], 'Startup Pods must be absent before the single create.')
        until = window()
        evidence = live._strict_json(remote('control', _completion_command(initialization_plan, initialization_stage,
            context['role_nodes'], context['server_ca_sha256'], remaining(until), completed_pins=completed), until))
        _require(set(evidence) == {'pods', 'logs'} and set(evidence['pods']) == set(completed)
                 and set(evidence['logs']) == set(completed), 'Fresh initializer evidence is incomplete.')
        events = retained['journal']['events']
        init_context = initialization._context(initialization_plan, initialization_stage, context['role_nodes'], context['server_ca_sha256'])
        for mode, pin in completed.items():
            approved = next(p for p in json.loads(initialization_stage.canonical_json)['items']
                            if p['kind'] == 'Pod' and p['spec']['containers'][0]['args'] == [mode])
            pair = evidence['pods'][mode]
            _require(set(pair) == {'before', 'after'}, 'Fresh completion retrieval is incomplete.')
            for raw in pair.values():
                row = initialization._initialization_pod(raw, approved, init_context)
                _require(_canonical(_completed(row)) == _canonical(pin), 'Completed initializer changed around log retrieval.')
            index = 2 if mode == 'initialize-empty' else 4
            intent, exited = events[index]['payload'], events[index + 1]['payload']
            output = _success(evidence['logs'][mode], mode, initialization_plan)
            _require(intent['mode'] == mode and intent['pod_uid'] == pin['uid']
                     and intent['image'] == initialization_plan.application_image
                     and exited['pod_uid'] == pin['uid'] and type(exited['exit_code']) is int and exited['exit_code'] == 0
                     and _canonical(output) == _canonical(exited['output'])
                     and _canonical(output) == _canonical(_success(retained['last_observation']['logs'][mode], mode, initialization_plan)),
                     'Fresh initializer output differs from original exact dataset execution.')
        prerequisites()
        before = kubernetes(window())
        _require(not before['startup_pods'], 'Startup Pods appeared before the create intent.')
        journal.record_create_intent(dict(schema_version=1, candidate_schema='hotel-startup-create-preflight-v1',
            run_id=plan.run_id, initialization_attempt_id=plan.initialization_attempt_id, startup_attempt_id=startup_attempt_id,
            plan_fingerprint=plan.fingerprint, stage_fingerprint=stage.fingerprint,
            initialization_plan_fingerprint=initialization_plan.fingerprint, initialization_stage_fingerprint=initialization_stage.fingerprint,
            initialization_identity_fingerprint=_hash(completed), support_identity_fingerprint=_hash(before['identity'])))
        write(state='create-intent', journal=journal.metadata())
        # The persistence callback may affect physical state without editing
        # this carrier. Re-attest after it, with no later non-dispatch callback.
        prerequisites()
        before = kubernetes(window())
        _require(not before['startup_pods'], 'Startup Pods appeared after durable create intent.')
        # Arm exactly once, after durable intent and its persistence, before the
        # first create dispatch. All subsequent guards/journals consume it.
        remaining()
        started = last_time
        deadline = started + 900.0
        acknowledgment = live._strict_json(remote('control', _mutation_command(['create', '--validate=strict',
            '--field-manager=deathstarbench-hotel-startup', '-f', '-', '-o', 'json'], remaining(),
            namespace=plan.namespace, expected_identity=identity,
            manifest=stage.canonical_json, marker='HOTEL_STARTUP_CREATE')))
        approved = {pod['metadata']['name']: pod for pod in json.loads(stage.canonical_json)['items']}
        pins = _creation_pins(acknowledgment, approved, context['role_nodes'])
        admitted = observe()
        _require(set(admitted['startup_pods']) == set(pins), 'Startup create acknowledgment is not independently observed.')
        for component, pin in pins.items():
            live.identity.validate_startup_hotel_admitted_pod(admitted['startup_pods'][component]['pod'],
                approved[pin['pod_name']], context['role_nodes'], pinned=pin)
        journal.record_admitted_pods(admitted)
        write(state='all-ten-admitted', journal=journal.metadata())
        for component in RELEASE_ORDER:
            prerequisites()
            before = observe()
            _require(set(before['startup_pods']) == set(RELEASE_ORDER) and ready_for(before, released),
                     'Previously released component proof is incomplete before dispatch.')
            selected = before['startup_pods'][component]
            _require(selected['state'] == 'gated' and selected['pod_uid'] == pins[component]['pod_uid'],
                     'Startup target is no longer the original blocked admission.')
            journal.record_dispatch_intent(component, before, image_access=image_access)
            write(state=component + '-dispatch-intent', journal=journal.metadata())
            prerequisites()
            fresh = observe()
            _require(set(fresh['startup_pods']) == set(RELEASE_ORDER) and ready_for(fresh, released),
                     'Released identities changed after durable dispatch intent.')
            selected = fresh['startup_pods'][component]
            _require(selected['state'] == 'gated' and selected['pod_uid'] == pins[component]['pod_uid'],
                     'Original target changed after durable dispatch intent.')
            pod = selected['pod']
            patch = [{'op': 'test', 'path': '/metadata/' + key, 'value': value}
                     for key, value in pod['metadata'].items()]
            patch.extend([{'op': 'test', 'path': '/spec', 'value': pod['spec']},
                          {'op': 'remove', 'path': '/spec/schedulingGates'}])
            response = live._strict_json(remote('control', _mutation_command(['-n', plan.namespace,
                'patch', 'pod', selected['pod_name'], '--type=json', '-p', _canonical(patch), '-o', 'json'],
                remaining(), namespace=plan.namespace, expected_identity=identity, marker='HOTEL_STARTUP_RELEASE')))
            response['metadata'].pop('managedFields', None)
            live.identity.validate_startup_hotel_admitted_pod(response, stage.pod(component), context['role_nodes'],
                pinned=pins[component], released=True)
            # Only an acknowledged exact response advances private release authority.
            identity['startup_pods'][component]['released'] = True
            released.append(component)
            while True:
                observed = observe()
                if ready_for(observed, released): break
                remaining()
            journal.record_component_observation(component, observed)
            write(state=component + '-observed', journal=journal.metadata())
        final = observe()
        _require(ready_for(final, RELEASE_ORDER), 'Final all-ten startup proof is incomplete.')
        journal.record_final_observation(final)
        write(state=SUCCESS, journal=journal.metadata(), startup_execution_observed=True, service_identity_observed=True,
              startup_elapsed_seconds=monotonic() - started,
              preflight_elapsed_seconds=started - invocation_started)
        if emit is not None:
            callback(emit, job, 'Hotel application startup', 'All ten startup executions and exact service identities observed; qualification gates remain closed.')
        result = _copy(current)
        remaining()
        return result
    except BaseException as exc:
        # No arbitrary exception/status/HTTP text enters new persisted evidence.
        # Recording terminal failure is not dispatch and remains possible after
        # the clock/custody check that stopped execution. Never adopt a record
        # replaced by a callback; retain the original lease through teardown.
        try:
            ended = monotonic()
            if type(ended) in (int, float) and math.isfinite(ended) and ended >= last_time:
                last_time = ended
            require_acquired_run_lease(lease, plan.run_id)
            _require(job.get('id') == plan.run_id and job.get('_persist_state') is False
                     and _canonical(job.get(JOB_KEY)) == _canonical(current), 'Failure recording lost original custody.')
            failure_type = type(exc).__name__
            if failure_type not in {'StartupHotelControllerError', 'TimeoutError', 'ValueError',
                                    'OSError', 'RuntimeError', 'KeyboardInterrupt', 'SystemExit'}:
                failure_type = 'StartupBoundaryError'
            elapsed = max(0.0, last_time - started) if started is not None else 0.0
            current = _copy({**current, 'state': 'failed-cleanup-only', 'failure_reason': 'startup-boundary-failed',
                'failure_type': failure_type, 'startup_execution_observed': False, 'service_identity_observed': False,
                'startup_elapsed_seconds': elapsed})
            job[JOB_KEY] = _copy(current)
            persist(job)
            require_acquired_run_lease(lease, plan.run_id)
            _require(_canonical(job.get(JOB_KEY)) == _canonical(current), 'Terminal failure evidence changed during persistence.')
        except BaseException:
            pass
        raise
