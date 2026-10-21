"""Synthetic Hotel workload on the existing five-node benchmark topology.

The public supervisor owns cloud cleanup. This runner uses the published native
Hotel images, seeds a fresh disposable database once, then measures the pinned
mixed workload. It intentionally does not reproduce the private qualification
controller's approval, identity, and scheduling-gate machinery.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import shlex
import time
from datetime import datetime, timezone
from typing import Any, Mapping

from . import deathstarbench
from .deathstarbench_synthetic_metrics import parse_body_metrics
from .deathstarbench_candidate_storage import DATABASE_ROOT, MOUNT_POINT
from .deathstarbench_candidate_workload import render_candidate_workload_bundle
from .deathstarbench_contract import DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
from .deathstarbench_distributed import (
    distributed_k3s_candidate_plan,
)
from .deathstarbench_hotel_release import validate_hotel_runtime_lock
from .models import DeathStarBenchOptions


NAMESPACE = 'deathstarbench-hotel'
KUBECTL = '/usr/local/bin/k3s kubectl'
STARTUP_GROUPS = (
    ('attractions', 'geo', 'profile', 'rate', 'recommendation', 'reservation', 'review', 'user'),
    ('search',), ('frontend',),
)
APPLICATIONS = frozenset(name for group in STARTUP_GROUPS for name in group)
JOURNAL_KEY = DEATHSTARBENCH_EXECUTION_JOURNAL_KEY


class HotelBenchmarkError(RuntimeError):
    """The disposable Hotel deployment or measurement did not succeed."""


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _digest(value):
    return 'sha256:' + hashlib.sha256(_canonical(value).encode()).hexdigest()


def _options(plan):
    value = plan.get('deathstarbench', {}) if isinstance(plan, Mapping) else plan.deathstarbench
    options = value if isinstance(value, DeathStarBenchOptions) else DeathStarBenchOptions.model_validate(value)
    if (options.workload != 'hotel_reservation' or options.topology_id != 'distributed_tiered_v1'
            or options.runtime_id != 'k3s_v1'):
        raise HotelBenchmarkError('Hotel requires distributed_tiered_v1/k3s_v1.')
    if (options.connections < options.threads or options.connections % options.threads
            or options.request_rate < options.threads or options.request_rate % options.threads):
        raise HotelBenchmarkError('Connections and request rate must be positive multiples of threads.')
    return options


def render_hotel_manifests(image_lock, architecture, load_generator_ip, run_id):
    """Derive support, guarded application, and single-use initializer objects."""
    if not re.fullmatch(r'[0-9a-f]{12}', str(run_id)):
        raise HotelBenchmarkError('Hotel requires a canonical benchmark run ID.')
    bundle = render_candidate_workload_bundle(
        'hotel_reservation', image_lock['candidate_images'], architecture, load_generator_ip,
    )
    platform = {'x86_64': 'linux/amd64', 'aarch64': 'linux/arm64'}[bundle.application_architecture]
    application_image = image_lock['application_images'][platform]
    environment = [
        {'name': 'HOTEL_CANDIDATE_RUN_ID', 'value': run_id},
        {'name': 'HOTEL_CANDIDATE_CONTRACT_SHA256', 'value': image_lock['dataset_fingerprint']},
    ]
    support, applications, database_names = [], {}, []
    for item in json.loads(bundle.canonical_json)['items']:
        # The cloud security groups isolate this short-lived synthetic cluster.
        # Per-service deny policies add no value to the benchmark UI workflow.
        if item['kind'] == 'NetworkPolicy':
            continue
        if item['kind'] == 'Deployment' and item['metadata']['name'] in APPLICATIONS:
            spec = item['spec']['template']['spec']
            container = spec['containers'][0]
            container['image'] = application_image
            container['workingDir'] = '/workspace'
            container['env'].extend(environment)
            item['spec']['template']['metadata']['annotations']['deathstarbench.io/image-reference'] = application_image
            applications[item['metadata']['name']] = item
        else:
            support.append(item)
            if item['kind'] == 'Deployment' and item['metadata']['name'].startswith('mongodb-'):
                database_names.append(item['metadata']['name'])
    if set(applications) != APPLICATIONS or len(database_names) != 8:
        raise HotelBenchmarkError('The published Hotel component inventory differs.')
    initializer = {
        'apiVersion': 'v1', 'kind': 'Pod',
        'metadata': {'name': 'hotel-initialize', 'namespace': NAMESPACE},
        'spec': {
            'restartPolicy': 'Never', 'activeDeadlineSeconds': 600,
            'automountServiceAccountToken': False, 'enableServiceLinks': False,
            'nodeSelector': {'deathstarbench.io/role': 'application',
                             'kubernetes.io/arch': platform.split('/')[1]},
            'containers': [{
                'name': 'initialize', 'image': application_image, 'workingDir': '/workspace',
                'command': ['/go/bin/hotel-initialize', 'initialize-empty'], 'env': environment,
            }],
        },
    }
    actual_items = [*support, initializer, *applications.values()]
    comparison_items = json.loads(_canonical(actual_items))
    for item in comparison_items:
        spec = item.get('spec', {})
        pod_spec = spec.get('template', {}).get('spec', spec)
        for container in pod_spec.get('containers', []):
            for variable in container.get('env', []):
                if variable['name'] == 'HOTEL_CANDIDATE_RUN_ID':
                    variable['value'] = '<run-id>'
    return {
        'support': support, 'applications': applications, 'initializer': initializer,
        'database_names': sorted(database_names), 'manifest_source_sha256': bundle.manifest_source_sha256,
        'rendered_manifest_sha256': _digest(comparison_items),
        'run_manifest_sha256': _digest(actual_items),
    }


def _python_command(script):
    return "python3 - <<'HOTEL_PY'\n" + script + '\nHOTEL_PY'


def semantic_smoke_command(target_ip):
    """Check response semantics, including HTTP-200 failed login responses."""
    target = str(ipaddress.IPv4Address(target_ip))
    return _python_command('''import json, time, urllib.parse, urllib.request
base = ''' + repr('http://' + target + ':8080') + '''
def request(route, params):
    url = base + route + '?' + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=15) as response:
        if response.status != 200:
            raise RuntimeError('Hotel HTTP status: ' + str(response.status))
        return json.load(response)
user = {'username': 'Cornell_1', 'password': '1111111111'}
# Consul registration is asynchronous; retry a read-only login before the
# single reservation probe. The probe dates do not overlap April load traffic.
for attempt in range(30):
    try:
        login = request('/user', user)
        if login.get('message') != 'Login successfully!':
            raise RuntimeError('Valid synthetic user could not log in: ' + repr(login))
        break
    except Exception:
        if attempt == 29:
            raise
        time.sleep(2)
invalid = request('/user', {'username': 'Cornell_1', 'password': 'invalid'})
if invalid.get('message', '').strip() != 'Failed. Please check your username and password.':
    raise RuntimeError('Invalid login was not rejected: ' + repr(invalid))
reservation = request('/reservation', dict(user, inDate='2015-05-20', outDate='2015-05-21',
    hotelId='1', customerName='benchmark-smoke', number='1'))
if reservation.get('message') != 'Reserve successfully!':
    raise RuntimeError('Synthetic reservation failed: ' + repr(reservation))
search = request('/hotels', {'inDate': '2015-04-10', 'outDate': '2015-04-11',
    'lat': '37.7749', 'lon': '-122.4194'})
recommendations = request('/recommendations', {'require': 'dis', 'lat': '37.7749', 'lon': '-122.4194'})
for name, value in [('search', search), ('recommendations', recommendations)]:
    if value.get('type') != 'FeatureCollection' or not isinstance(value.get('features'), list):
        raise RuntimeError('Invalid ' + name + ' response: ' + repr(value))
print('HOTEL_SEMANTIC_SMOKE ' + json.dumps({'valid_login': True, 'invalid_login_rejected': True,
    'reservation_success': True, 'search': True, 'recommendations': True}, sort_keys=True))
''')


def parse_semantic_smoke(output):
    lines = re.findall(r'^HOTEL_SEMANTIC_SMOKE (.+)$', output, re.MULTILINE)
    expected = dict.fromkeys(('valid_login', 'invalid_login_rejected', 'reservation_success',
                              'search', 'recommendations'), True)
    if len(lines) != 1 or json.loads(lines[0]) != expected:
        raise HotelBenchmarkError('Hotel semantic smoke checks were not completed.')
    return expected


def parse_hotel_metrics(output):
    return parse_body_metrics(output, workload='hotel_reservation', label='Hotel',
                              error_type=HotelBenchmarkError)


def hotel_load_command(image, target_ip, options, duration_seconds, run_id, phase):
    """Run the already published driver, with an explicit bounded container."""
    target = str(ipaddress.IPv4Address(target_ip))
    if phase not in {'warmup', 'measurement'} or not re.fullmatch('[0-9a-f]{12}', run_id):
        raise HotelBenchmarkError('Invalid Hotel load invocation identity.')
    name = 'benchmark-dsb-' + run_id + '-' + phase
    args = ['sudo', 'podman', 'run', '--rm', '--name', name, '--pull=never', '--network=host',
            '--ulimit', f'nofile={options.connections + 128}:{options.connections + 128}', image, 'run',
            '--target', target, '--port', '8080', '--threads', str(options.threads),
            '--connections', str(options.connections), '--rate', str(options.request_rate),
            '--duration', str(int(duration_seconds))]
    return ('set -euo pipefail; '
            f"trap 'sudo podman rm --force {name} >/dev/null 2>&1 || true' EXIT; "
            f'timeout --signal=TERM --kill-after=30s {int(duration_seconds) + 180}s ' + shlex.join(args))


def _wait_initializer_command():
    return _python_command('''import json, subprocess, time
k = ['sudo', '/usr/local/bin/k3s', 'kubectl', '-n', 'deathstarbench-hotel']
for attempt in range(125):
    pod = json.loads(subprocess.check_output(k + ['get', 'pod', 'hotel-initialize', '-o', 'json']))
    phase = pod.get('status', {}).get('phase')
    if phase in ('Succeeded', 'Failed'):
        subprocess.run(k + ['logs', 'hotel-initialize'], check=False)
        if phase != 'Succeeded':
            raise RuntimeError('Hotel initialization failed')
        break
    time.sleep(5)
else:
    subprocess.run(k + ['describe', 'pod', 'hotel-initialize'], check=False)
    raise RuntimeError('Hotel initialization timed out')
''')


def _pod_snapshot(output):
    try:
        pods = json.loads(output)['items']
        result = {}
        for pod in pods:
            component = pod['metadata'].get('labels', {}).get('app.kubernetes.io/component')
            if not component:
                continue  # completed initializer has no component label
            statuses = pod['status'].get('containerStatuses', [])
            if (len(statuses) != 1 or statuses[0]['restartCount'] != 0
                    or not statuses[0].get('ready') or 'running' not in statuses[0].get('state', {})):
                raise ValueError('unready or restarted component ' + component)
            if component in result:
                raise ValueError('duplicate component ' + component)
            result[component] = {'uid': pod['metadata']['uid'], 'container_id': statuses[0]['containerID'],
                                 'image_id': statuses[0]['imageID'], 'node': pod['spec']['nodeName']}
        if not APPLICATIONS.issubset(result) or len(result) != 24:
            raise ValueError('incomplete Hotel components')
        return result
    except (KeyError, TypeError, ValueError) as exc:
        raise HotelBenchmarkError('Hotel workload is incomplete, restarted, or unhealthy: ' + str(exc)) from exc


def run_hotel_benchmark(job, plan, image_lock, *, execute, emit, persist):
    """Deploy, smoke-test, warm up, and measure a fresh synthetic Hotel run."""
    options = _options(plan)
    image_lock = validate_hotel_runtime_lock(image_lock)
    resources = job['resources']
    if JOURNAL_KEY in resources or any(result.get('id') == 'deathstarbench' for result in job.get('results', [])):
        raise HotelBenchmarkError('Hotel already started in this run; start a fresh benchmark after cleanup.')
    runtime = distributed_k3s_candidate_plan(resources)
    manifests = render_hotel_manifests(image_lock, runtime.host('application').architecture,
                                       runtime.load_generator_private_ip, job['id'])

    def stage(state, message, **details):
        resources[JOURNAL_KEY] = {'schema_version': 1, 'workload': 'hotel_reservation',
                                  'state': state, 'run_id': job['id'], **details}
        persist(job)
        emit(job, 'Hotel benchmark', message)

    def run(host, command, timeout=600, stdin_text=None):
        kwargs = dict(timeout=timeout, host_key=host.host_key, transport_attempts=1, include_stderr=True)
        if host.jump_host_key:
            kwargs['jump_host_key'] = host.jump_host_key
        if stdin_text is not None:
            kwargs['stdin_text'] = stdin_text
        return execute(job, command, **kwargs)

    control = runtime.host('control')
    database = runtime.host('database')
    loadgen = runtime.load_generator
    target = runtime.host('application').private_ip

    def apply(items):
        run(control, f'sudo {KUBECTL} apply -f -', stdin_text=_canonical(
            {'apiVersion': 'v1', 'kind': 'List', 'items': items}))

    def wait(names):
        for name in names:
            run(control, f'sudo {KUBECTL} -n {NAMESPACE} rollout status deployment/{name} --timeout=300s', 330)

    stage('deploying_support', 'Starting Hotel MongoDB, cache, service discovery, and tracing.')
    directories = ' '.join(shlex.quote(DATABASE_ROOT + '/' + name) for name in manifests['database_names'])
    run(database, f'set -euo pipefail; mountpoint -q {MOUNT_POINT}; sudo mkdir -p {directories}; '
                  f'sudo chmod 0777 {directories}; '
                  f'{{ sudo semanage fcontext -a -t container_file_t {shlex.quote(DATABASE_ROOT + "(/.*)?")} 2>/dev/null '
                  f'|| sudo semanage fcontext -m -t container_file_t {shlex.quote(DATABASE_ROOT + "(/.*)?")}; }}; '
                  f'sudo restorecon -R -x {shlex.quote(DATABASE_ROOT)}')
    apply(manifests['support'])
    wait(item['metadata']['name'] for item in manifests['support'] if item['kind'] == 'Deployment')
    stage('initializing_dataset', 'Seeding and verifying the deterministic synthetic Hotel dataset.')
    apply([manifests['initializer']])
    initialization_output = run(control, _wait_initializer_command(), 660)
    initialization = []
    for line in initialization_output.splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if isinstance(item, dict) and item.get('database_verification_completed') is True:
            initialization.append(item)
    if (len(initialization) != 1 or initialization[0].get('run_id') != job['id']
            or initialization[0].get('contract_sha256') != image_lock['dataset_fingerprint']
            or initialization[0].get('mode') != 'initialize-empty'):
        raise HotelBenchmarkError('Hotel initializer did not verify this run and dataset.')
    stage('starting_applications', 'Starting all ten Hotel application services.')
    for group in STARTUP_GROUPS:
        apply([manifests['applications'][name] for name in group])
        wait(group)
    stage('semantic_checks', 'Checking valid and invalid login, reservation, search, and recommendations.')
    image = image_lock['load_generator_image']
    # The five-node provisioning path creates minimal EL9 guests. Only the
    # load generator needs Podman; the services use K3s's containerd runtime.
    run(loadgen, 'set -euo pipefail; if ! command -v podman >/dev/null; then '
        'sudo dnf -y --setopt=retries=5 install podman python3; fi; '
        f'sudo podman pull --quiet {shlex.quote(image)}', 900)
    driver_output = run(loadgen, f'sudo podman run --rm --pull=never --network=none {shlex.quote(image)} attest')
    driver_markers = re.findall(r'^CANDIDATE_DSB_LOAD_DRIVER_ARTIFACT (.+)$', driver_output, re.MULTILINE)
    if len(driver_markers) != 1:
        raise HotelBenchmarkError('The Hotel load generator did not attest its image contents.')
    driver = dict(part.split('=', 1) for part in driver_markers[0].split())
    if (driver.get('workload') != 'hotel_reservation' or driver.get('architecture') != 'x86_64'
            or driver.get('wrk_binary_sha256') != image_lock['wrk_binary_sha256']
            or driver.get('context_sha256') != image_lock['load_driver_context_sha256']
            or driver.get('request_script_sha256') != image_lock['load_script_sha256']
            or driver.get('revision') != image_lock['load_driver_revision']):
        raise HotelBenchmarkError('The Hotel load generator attested a different workload.')
    semantic = parse_semantic_smoke(run(loadgen, semantic_smoke_command(target), 180))
    snapshot_command = f'sudo {KUBECTL} -n {NAMESPACE} get pods -o json'
    before = _pod_snapshot(run(control, snapshot_command))
    warmup_metrics = None
    if options.warmup_seconds:
        stage('warming_up', f'Warming up the mixed Hotel workload for {options.warmup_seconds} seconds.')
        warmup_metrics = parse_hotel_metrics(run(loadgen, hotel_load_command(
            image, target, options, options.warmup_seconds, job['id'], 'warmup'), options.warmup_seconds + 240))
    stage('measuring', f'Measuring the Hotel workload at {options.request_rate} requests/second.')
    command = hotel_load_command(image, target, options, options.duration_seconds, job['id'], 'measurement')
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    output = run(loadgen, command, options.duration_seconds + 240)
    elapsed = time.monotonic() - started
    metrics = parse_hotel_metrics(output)
    after = _pod_snapshot(run(control, snapshot_command))
    if before != after:
        raise HotelBenchmarkError('A Hotel service restarted or was replaced during measurement.')
    metadata = deathstarbench.metadata('hotel_reservation', options,
        service_architecture=runtime.host('application').architecture, loadgen_architecture=loadgen.architecture)
    metadata.update({
        key: image_lock[key] for key in ('runtime_revision', 'dataset_revision', 'workload_revision', 'measurement_revision',
                                         'load_driver_revision', 'initializer_sha256', 'load_script_sha256',
                                         'load_driver_context_sha256')
    })
    metadata.update({
        'image_set_revision': image_lock['candidate_images']['image_set_revision'],
        'image_lock_fingerprint': _digest(image_lock),
        'dataset_manifest_sha256': image_lock['dataset_fingerprint'].removeprefix('sha256:'),
        'manifest_source_sha256': manifests['manifest_source_sha256'],
        'rendered_manifest_sha256': manifests['rendered_manifest_sha256'],
        'run_manifest_sha256': manifests['run_manifest_sha256'],
        'topology_fingerprint': runtime.topology_fingerprint,
        'load_driver_image': image, 'load_generator_wrk_binary_sha256': driver['wrk_binary_sha256'],
        'dataset_fingerprint': image_lock['dataset_fingerprint'], 'semantic_smoke': semantic,
        'warmup_metrics': warmup_metrics, 'pod_identity_unchanged': True,
        'measurement_method': 'candidate-body-v1',
        'dataset_state': 'Fresh seed, semantic probe outside measured dates, then warmup followed by measurement; no reset.',
        'error_rate_definition': 'HTTP failures plus HTTP-200 body-check failures divided by completed responses.',
        'throughput_definition': 'Responses passing the published Hotel body checks per second.',
        'workload_limitations': 'Synthetic upstream workload; reservation writes are not atomic and are not an authentication or booking-correctness test. Warmup and measurement mutate reservations; capacity exhaustion is counted as a body-check failure.',
    })
    result = {'id': 'deathstarbench', 'name': 'DeathStarBench — Hotel Reservation', 'command': command,
              'started_at': started_at, 'duration_seconds': round(elapsed, 2), 'status': 'completed',
              'output': output[-200000:], 'metadata': metadata, 'metrics': metrics}
    job.setdefault('results', []).append(result)
    stage('measurement_complete', 'Hotel benchmark completed; cloud resources will now be cleaned up.',
          metrics_sha256=_digest(metrics))
    return result
