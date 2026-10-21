"""Disposable synthetic Media compose-review workload on five-node K3s.

The public supervisor owns cleanup. HTTP success and persisted review counts
are reported separately because upstream compose is not transactional.
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
from pathlib import Path
import re
import shlex
import time
from datetime import datetime, timezone
from collections.abc import Mapping

from . import deathstarbench
from .deathstarbench_candidate_storage import DATABASE_ROOT, MOUNT_POINT
from .deathstarbench_candidate_workload import render_candidate_workload_bundle
from .deathstarbench_contract import DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
from .deathstarbench_distributed import distributed_k3s_candidate_plan
from .deathstarbench_synthetic_metrics import parse_body_metrics
from .models import DeathStarBenchOptions

NAMESPACE = 'deathstarbench-media'
KUBECTL = '/usr/local/bin/k3s kubectl'
JOURNAL_KEY = DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
SEED_SCRIPT = Path(__file__).parent / 'guests/media_seed.py'
APPLICATIONS = frozenset(('cast-info-service', 'compose-review-service', 'movie-id-service',
    'movie-info-service', 'movie-review-service', 'nginx-web-server',
    'plot-service', 'rating-service', 'review-storage-service', 'text-service',
    'unique-id-service', 'user-review-service', 'user-service'))
STARTUP_GROUPS = (tuple(sorted(APPLICATIONS - {'nginx-web-server'})), ('nginx-web-server',))


class MediaBenchmarkError(RuntimeError):
    """The Media deployment, initialization, or measurement failed."""


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _digest(value):
    return 'sha256:' + hashlib.sha256(_canonical(value).encode()).hexdigest()


def render_media_manifests(image_lock, architecture, load_generator_ip, run_id):
    if not re.fullmatch('[0-9a-f]{12}', str(run_id)):
        raise MediaBenchmarkError('Media requires a canonical benchmark run ID.')
    bundle = render_candidate_workload_bundle('media_microservices', image_lock['candidate_images'],
                                               architecture, load_generator_ip)
    support, applications, database_names = [], {}, []
    for item in json.loads(bundle.canonical_json)['items']:
        if item['kind'] == 'NetworkPolicy':
            continue
        if item['kind'] == 'Deployment' and item['metadata']['name'] in APPLICATIONS:
            applications[item['metadata']['name']] = item
        else:
            support.append(item)
            if item['kind'] == 'Deployment' and item['metadata']['name'].endswith('-mongodb'):
                database_names.append(item['metadata']['name'])
    if set(applications) != APPLICATIONS or len(database_names) != 8:
        raise MediaBenchmarkError('The published Media component inventory differs.')
    items = [*support, *applications.values()]
    return {'support': support, 'applications': applications, 'database_names': sorted(database_names),
            'manifest_source_sha256': bundle.manifest_source_sha256,
            'rendered_manifest_sha256': _digest(items), 'run_manifest_sha256': _digest(items)}


def parse_media_metrics(output):
    return parse_body_metrics(output, workload='media_microservices', label='Media', error_type=MediaBenchmarkError)


def _marker(output, marker):
    lines = re.findall(r'^' + re.escape(marker) + r' (.+)$', output, re.MULTILINE)
    try:
        if len(lines) != 1:
            raise ValueError('expected one marker')
        value = json.loads(lines[0])
        if not isinstance(value, dict):
            raise ValueError('expected object')
        return value
    except (ValueError, TypeError) as exc:
        raise MediaBenchmarkError('Media ' + marker + ' evidence is incomplete or malformed.') from exc


def parse_database_snapshot(output, *, baseline=False):
    value = _marker(output, 'MEDIA_DATABASE')
    expected = {'users':1000, 'movies':995, 'reviews':0, 'user_review_references':0,
                'movie_review_references':0, 'cast_info':0, 'movie_info':0, 'plots':0}
    if any(type(value.get(key)) is not int or value[key] < 0 for key in expected):
        raise MediaBenchmarkError('Media database counts are incomplete or invalid.')
    if not isinstance(value.get('user_id'), str) or not re.fullmatch(r'-?\d{1,19}', value['user_id']):
        raise MediaBenchmarkError('Media baseline user identity is invalid.')
    checked = expected if baseline else {key: expected[key] for key in ('users', 'movies', 'cast_info', 'movie_info', 'plots')}
    if any(value[key] != count for key, count in checked.items()):
        raise MediaBenchmarkError('Media database baseline differs from the synthetic dataset.')
    return value


def parse_canary(output, run_id, user_id):
    value = _marker(output, 'MEDIA_CANARY')
    if (not isinstance(value.get('review_id'), str) or not re.fullmatch(r'-?\d{1,19}', value['review_id'])
            or value.get('user_id') != user_id or value.get('movie_id') != 'movie_id_1'
            or value.get('text') != 'benchmark-media-canary-' + run_id or value.get('rating') != 5
            or value.get('user_references') != 1 or value.get('movie_references') != 1):
        raise MediaBenchmarkError('Media canary review or its user/movie references were not verified.')
    return value


def persistence_metrics(before, after, metrics):
    stored = after['reviews'] - before['reviews']
    users = after['user_review_references'] - before['user_review_references']
    movies = after['movie_review_references'] - before['movie_review_references']
    if stored <= 0 or users < 0 or movies < 0:
        raise MediaBenchmarkError('Media load produced no persisted reviews or persistence counts regressed.')
    return {'stored_reviews': stored, 'user_review_references': users, 'movie_review_references': movies,
        'stored_reviews_per_second': round(stored / metrics['duration_seconds'], 6),
        'stored_reviews_minus_http_successes': stored - metrics['successful_requests'],
        'user_references_minus_stored_reviews': users - stored,
        'movie_references_minus_stored_reviews': movies - stored}


def _options(plan):
    value = plan.get('deathstarbench', {}) if isinstance(plan, Mapping) else plan.deathstarbench
    options = value if isinstance(value, DeathStarBenchOptions) else DeathStarBenchOptions.model_validate(value)
    if (options.workload != 'media_microservices' or options.topology_id != 'distributed_tiered_v1'
            or options.runtime_id != 'k3s_v1'):
        raise MediaBenchmarkError('Media requires distributed_tiered_v1/k3s_v1.')
    if (options.connections < options.threads or options.connections % options.threads
            or options.request_rate < options.threads or options.request_rate % options.threads):
        raise MediaBenchmarkError('Connections and request rate must be positive multiples of threads.')
    return options


def _python_command(script):
    return "python3 - <<'MEDIA_PY'\n" + script + '\nMEDIA_PY'


def _mongo_args(component, collection, script):
    return ['sudo', '/usr/local/bin/k3s', 'kubectl', '-n', NAMESPACE, 'exec',
            'deployment/' + component, '--', 'mongo', '--quiet', collection, '--eval',
            'var c=db.getCollection(' + json.dumps(collection) + '); ' + script]


_LONG_STRING_JS = r'''function decimal(v) {
  if (!(v instanceof NumberLong)) throw new Error('Expected BSON int64');
  return v.toString().replace(/^NumberLong\("?(-?\d+)"?\)$/, '$1');
}'''


def database_snapshot_command(titles, *, baseline=False):
    queries = []
    for key, component, collection in (
        ('users','user-mongodb','user'), ('movies','movie-id-mongodb','movie-id'),
        ('reviews','review-storage-mongodb','review'),
        ('user_review_references','user-review-mongodb','user-review'),
        ('movie_review_references','movie-review-mongodb','movie-review'),
        ('cast_info','cast-info-mongodb','cast-info'),
        ('movie_info','movie-info-mongodb','movie-info'), ('plots','plot-mongodb','plot')):
        script = 'var count=c.count(); '
        if key.endswith('_references'):
            script = 'var count=0; c.find().forEach(function(d) { count += (d.reviews || []).length; }); '
            if baseline:
                script += "if(c.count()!==0) throw new Error('Media review indexes are not empty'); "
        if baseline and key == 'users':
            script += '''for(var i=1;i<=1000;i++) {
              if(c.count({username:'username_'+i,first_name:'first_name_'+i,last_name:'last_name_'+i})!==1)
                throw new Error('Synthetic user keyspace differs');
            } '''
        if baseline and key == 'movies':
            movies = {}
            for index, title in enumerate(titles, 1):
                movies.setdefault(title, 'movie_id_' + str(index))
            script += 'var movies=' + _canonical(list(movies.items())) + '; '
            script += '''movies.forEach(function(item) {
              if(c.count({title:item[0],movie_id:item[1]})!==1)
                throw new Error('Synthetic movie keyspace differs');
            }); '''
        if key == 'users':
            script += _LONG_STRING_JS + "var user=c.findOne({username:'username_1'}); if(!user) throw new Error('Missing canary user'); "
            script += "print('MEDIA_QUERY '+JSON.stringify({count:count,user_id:decimal(user.user_id)}));"
        else:
            script += "print('MEDIA_QUERY '+JSON.stringify({count:count}));"
        queries.append((key, _mongo_args(component, collection, script)))
    return _python_command('''import json, subprocess
queries = ''' + repr(queries) + '''
result = {}
for key, command in queries:
    output = subprocess.check_output(command, text=True, timeout=120)
    lines = [line[len('MEDIA_QUERY '):] for line in output.splitlines() if line.startswith('MEDIA_QUERY ')]
    if len(lines) != 1:
        raise RuntimeError('Missing Media database evidence: ' + key)
    item = json.loads(lines[0])
    result[key] = item['count']
    if key == 'users':
        result['user_id'] = item['user_id']
print('MEDIA_DATABASE ' + json.dumps(result, sort_keys=True))
''')


def canary_verification_command(run_id, user_id):
    if not re.fullmatch('[0-9a-f]{12}', run_id) or not re.fullmatch(r'-?\d{1,19}', user_id):
        raise MediaBenchmarkError('Invalid Media canary identity.')
    text = 'benchmark-media-canary-' + run_id
    query = {'text': text, 'movie_id': 'movie_id_1', 'rating': 5}
    review_js = (_LONG_STRING_JS + 'var q=' + _canonical(query) + '; '
        'q.user_id=NumberLong(' + json.dumps(user_id) + '); '
        "if(c.count()!==1 || c.count(q)!==1) throw new Error('Canary review differs'); "
        'var d=c.findOne(q); '
        "print('MEDIA_QUERY '+JSON.stringify({review_id:decimal(d.review_id),user_id:decimal(d.user_id),movie_id:d.movie_id,text:d.text,rating:d.rating}));")
    args = _mongo_args('review-storage-mongodb', 'review', review_js)
    base = ['sudo','/usr/local/bin/k3s','kubectl','-n',NAMESPACE,'exec']
    return _python_command('''import json, re, subprocess
base = ''' + repr(base) + '''
def query(command):
    output = subprocess.check_output(command, text=True, timeout=120)
    lines = [line[len('MEDIA_QUERY '):] for line in output.splitlines() if line.startswith('MEDIA_QUERY ')]
    if len(lines) != 1:
        raise RuntimeError('Missing Media canary database evidence')
    return json.loads(lines[0])
review = query(''' + repr(args) + ''')
review_id = review['review_id']
if not isinstance(review_id, str) or not re.fullmatch(r'-?\\d{1,19}', review_id):
    raise RuntimeError('Invalid exact review identity')
for field, component, collection, owner in [
    ('user_references','user-review-mongodb','user-review', 'user_id:NumberLong(' + json.dumps(review['user_id']) + ')'),
    ('movie_references','movie-review-mongodb','movie-review', 'movie_id:"movie_id_1"')]:
    script = 'var c=db.getCollection(' + json.dumps(collection) + '); var q={' + owner + '}; '
    script += 'var count=0; c.find(q).forEach(function(d) { (d.reviews || []).forEach(function(r) { '
    script += 'if(r.review_id.toString() === NumberLong(' + json.dumps(review_id) + ').toString()) count++; }); }); '
    script += "print('MEDIA_QUERY '+JSON.stringify({count:count}));"
    review[field] = query(base + ['deployment/' + component, '--','mongo','--quiet',collection,'--eval',script])['count']
print('MEDIA_CANARY ' + json.dumps(review, sort_keys=True))
''')


def media_load_command(image, target_ip, options, duration_seconds, run_id, phase):
    target = str(ipaddress.IPv4Address(target_ip))
    if phase not in {'warmup','measurement'} or not re.fullmatch('[0-9a-f]{12}',run_id):
        raise MediaBenchmarkError('Invalid Media load invocation identity.')
    name = 'benchmark-dsb-' + run_id + '-' + phase
    args = ['sudo','podman','run','--rm','--name',name,'--pull=never','--network=host',
            '--ulimit', f'nofile={options.connections + 128}:{options.connections + 128}',image,'run',
            '--target',target,'--port','8080','--threads',str(options.threads),'--connections',str(options.connections),
            '--rate',str(options.request_rate),'--duration',str(int(duration_seconds))]
    return ('set -euo pipefail; ' + f"trap 'sudo podman rm --force {name} >/dev/null 2>&1 || true' EXIT; "
            + f'timeout --signal=TERM --kill-after=30s {int(duration_seconds)+180}s ' + shlex.join(args))


def _pod_snapshot(output):
    try:
        result = {}
        for pod in json.loads(output)['items']:
            component = pod['metadata'].get('labels',{}).get('app.kubernetes.io/component')
            if not component:
                continue
            statuses = pod['status'].get('containerStatuses',[])
            if (len(statuses)!=1 or statuses[0]['restartCount']!=0 or not statuses[0].get('ready')
                    or 'running' not in statuses[0].get('state',{}) or component in result):
                raise ValueError('unready, duplicate, or restarted component ' + component)
            result[component] = {'uid':pod['metadata']['uid'],'container_id':statuses[0]['containerID'],
                'image_id':statuses[0]['imageID'],'node':pod['spec']['nodeName']}
        if not APPLICATIONS.issubset(result) or len(result)!=32:
            raise ValueError('incomplete Media components')
        return result
    except (KeyError,TypeError,ValueError) as exc:
        raise MediaBenchmarkError('Media workload is incomplete, restarted, or unhealthy: ' + str(exc)) from exc


def run_media_benchmark(job, plan, image_lock, *, execute, emit, persist):
    """Deploy Media, verify synthetic initialization, and measure compose-review."""
    from .deathstarbench_media_release import validate_media_runtime_lock
    options = _options(plan)
    image_lock = validate_media_runtime_lock(image_lock)
    resources = job['resources']
    if JOURNAL_KEY in resources or any(item.get('id')=='deathstarbench' for item in job.get('results',[])):
        raise MediaBenchmarkError('Media already started in this run; start a fresh benchmark after cleanup.')
    runtime = distributed_k3s_candidate_plan(resources)
    manifests = render_media_manifests(image_lock,runtime.host('application').architecture,
                                       runtime.load_generator_private_ip,job['id'])
    control, database, loadgen = runtime.host('control'), runtime.host('database'), runtime.load_generator
    target = str(ipaddress.IPv4Address(runtime.host('application').private_ip))

    def stage(state,message,**details):
        resources[JOURNAL_KEY] = {'schema_version':1,'workload':'media_microservices','state':state,'run_id':job['id'],**details}
        persist(job)
        emit(job,'Media benchmark',message)

    def run(host,command,timeout=600,stdin_text=None):
        kwargs = dict(timeout=timeout,host_key=host.host_key,transport_attempts=1,include_stderr=True)
        if host.jump_host_key:
            kwargs['jump_host_key'] = host.jump_host_key
        if stdin_text is not None:
            kwargs['stdin_text'] = stdin_text
        return execute(job,command,**kwargs)

    def apply(items):
        run(control,f'sudo {KUBECTL} apply -f -',stdin_text=_canonical({'apiVersion':'v1','kind':'List','items':items}))

    def wait(names):
        for name in names:
            run(control,f'sudo {KUBECTL} -n {NAMESPACE} rollout status deployment/{name} --timeout=300s',330)

    def snapshot(*,baseline=False):
        return parse_database_snapshot(run(control,database_snapshot_command(image_lock['dataset_manifest']['movies']['titles'],baseline=baseline),1200),baseline=baseline)

    def api(mode):
        command = shlex.join(['python3','-',mode,'--target',target,'--run-id',job['id'],'--titles-json',_canonical(image_lock['dataset_manifest']['movies']['titles'])])
        value = _marker(run(loadgen,command,900,SEED_SCRIPT.read_text()),'MEDIA_API_RESULT')
        expected = {'mode':mode,'run_id':job['id']}
        if mode == 'seed':
            expected.update(users_registered=1000,movies_registered=995)
        else:
            expected.update(canary_text='benchmark-media-canary-'+job['id'],http_status=200)
        if value != expected:
            raise MediaBenchmarkError('Media API initialization evidence does not match this run.')

    stage('deploying_support','Starting Media MongoDB, caches, Redis, and tracing.')
    directories=' '.join(shlex.quote(DATABASE_ROOT+'/'+name) for name in manifests['database_names'])
    run(database,f'set -euo pipefail; mountpoint -q {MOUNT_POINT}; sudo mkdir -p {directories}; '
        f'sudo chmod 0777 {directories}; '
        f'{{ sudo semanage fcontext -a -t container_file_t {shlex.quote(DATABASE_ROOT+"(/.*)?")} 2>/dev/null '
        f'|| sudo semanage fcontext -m -t container_file_t {shlex.quote(DATABASE_ROOT+"(/.*)?")}; }}; '
        f'sudo restorecon -R -x {shlex.quote(DATABASE_ROOT)}')
    apply(manifests['support'])
    wait(item['metadata']['name'] for item in manifests['support'] if item['kind']=='Deployment')
    stage('starting_applications','Starting the twelve Media services, then nginx.')
    for group in STARTUP_GROUPS:
        apply([manifests['applications'][name] for name in group])
        wait(group)
    image=image_lock['load_generator_image']
    run(loadgen,'set -euo pipefail; if ! command -v podman >/dev/null; then '
        'sudo dnf -y --setopt=retries=5 install podman python3; fi; '
        f'sudo podman pull --quiet {shlex.quote(image)}',900)
    output=run(loadgen,f'sudo podman run --rm --pull=never --network=none {shlex.quote(image)} attest')
    markers=re.findall(r'^CANDIDATE_DSB_LOAD_DRIVER_ARTIFACT (.+)$',output,re.MULTILINE)
    try:
        if len(markers)!=1:
            raise ValueError('expected one attestation')
        pairs=[part.split('=',1) for part in markers[0].split()]
        driver=dict(pairs)
        if len(driver)!=len(pairs):
            raise ValueError('duplicate keys')
    except ValueError as exc:
        raise MediaBenchmarkError('Media driver attestation is malformed.') from exc
    expected={'workload':'media_microservices','architecture':'x86_64',
        'wrk_binary_sha256':image_lock['wrk_binary_sha256'],'context_sha256':image_lock['load_driver_context_sha256'],
        'request_script_sha256':image_lock['load_script_sha256'],'revision':image_lock['load_driver_revision']}
    if any(driver.get(key)!=value for key,value in expected.items()):
        raise MediaBenchmarkError('Media load generator attested a different workload.')
    stage('initializing_dataset','Registering 1,000 synthetic users and 995 movie IDs matching the published driver titles.')
    api('seed')
    baseline=snapshot(baseline=True)
    stage('semantic_checks','Verifying one unique composed review and its exact user/movie references.')
    api('canary')
    semantic=parse_canary(run(control,canary_verification_command(job['id'],baseline['user_id']),420),job['id'],baseline['user_id'])
    before_counts=snapshot()
    if any(before_counts[key]!=1 for key in ('reviews','user_review_references','movie_review_references')):
        raise MediaBenchmarkError('Media canary must be the only initial review and reference.')
    pod_command=f'sudo {KUBECTL} -n {NAMESPACE} get pods -o json'
    before=_pod_snapshot(run(control,pod_command))
    warmup_metrics=warmup_persistence=None
    if options.warmup_seconds:
        stage('warming_up',f'Warming up Media compose-review for {options.warmup_seconds} seconds.')
        warmup_metrics=parse_media_metrics(run(loadgen,media_load_command(image,target,options,options.warmup_seconds,job['id'],'warmup'),options.warmup_seconds+240))
        warmup_counts=snapshot()
        warmup_persistence=persistence_metrics(before_counts,warmup_counts,warmup_metrics)
        before_counts=warmup_counts
    stage('measuring',f'Measuring Media compose-review at {options.request_rate} requests/second.')
    command=media_load_command(image,target,options,options.duration_seconds,job['id'],'measurement')
    started_at=datetime.now(timezone.utc).isoformat()
    started=time.monotonic()
    output=run(loadgen,command,options.duration_seconds+240)
    elapsed=time.monotonic()-started
    metrics=parse_media_metrics(output)
    after_counts=snapshot()
    persistence=persistence_metrics(before_counts,after_counts,metrics)
    metrics.update(persistence)
    after=_pod_snapshot(run(control,pod_command))
    if before!=after:
        raise MediaBenchmarkError('A Media service restarted or was replaced during measurement.')
    metadata=deathstarbench.metadata('media_microservices',options,
        service_architecture=runtime.host('application').architecture,loadgen_architecture=loadgen.architecture)
    metadata.update({key:image_lock[key] for key in ('runtime_revision','dataset_revision','workload_revision',
        'measurement_revision','load_driver_revision','initializer_sha256','load_script_sha256',
        'load_driver_context_sha256','dataset_manifest_sha256')})
    metadata.update({'image_set_revision':image_lock['candidate_images']['image_set_revision'],
        'image_lock_fingerprint':_digest(image_lock),'manifest_source_sha256':manifests['manifest_source_sha256'],
        'rendered_manifest_sha256':manifests['rendered_manifest_sha256'],'run_manifest_sha256':manifests['run_manifest_sha256'],
        'topology_fingerprint':runtime.topology_fingerprint,'load_driver_image':image,
        'load_generator_wrk_binary_sha256':driver['wrk_binary_sha256'],
        'dataset_fingerprint':'sha256:'+image_lock['dataset_manifest_sha256'].removeprefix('sha256:'),
        'semantic_smoke':semantic,'baseline_counts':baseline,'measurement_before_counts':before_counts,
        'measurement_after_counts':after_counts,'persistence':persistence,'warmup_metrics':warmup_metrics,
        'warmup_persistence':warmup_persistence,'pod_identity_unchanged':True,'measurement_method':'candidate-body-v1',
        'dataset_state':'Fresh synthetic users and movie IDs, one verified canary, then warmup and measurement; no reset.',
        'error_rate_definition':'HTTP failures plus published body-check failures divided by completed responses.',
        'throughput_definition':'Responses passing the published Media HTTP/body checks per second; persisted reviews reported separately.',
        'workload_limitations':'Synthetic compose-review only; no TMDB cast, plot, or movie-info dataset and no read workload. HTTP 200 can precede incomplete persistence. Upstream writes are not transactional; per-phase stored reviews and user/movie reference discrepancies are reported without claiming one-to-one request attribution.'})
    result={'id':'deathstarbench','name':'DeathStarBench — Media Microservices','command':command,
        'started_at':started_at,'duration_seconds':round(elapsed,2),'status':'completed','output':output[-200000:],
        'metadata':metadata,'metrics':metrics}
    job.setdefault('results',[]).append(result)
    stage('measurement_complete','Media benchmark completed; cloud resources will now be cleaned up.',metrics_sha256=_digest(metrics))
    return result
