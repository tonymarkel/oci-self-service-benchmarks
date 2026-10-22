import importlib.util
import io
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.parse

SEED_PATH = Path(__file__).parents[1] / 'app/guests/media_seed.py'


class MediaSeedTests(unittest.TestCase):
    def load_seed(self):
        self.assertTrue(SEED_PATH.exists(), 'Media requires an immutable API initializer')
        spec = importlib.util.spec_from_file_location('media_seed', SEED_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_seed_registers_exact_synthetic_keyspace_and_accepts_empty_success(self):
        seed = self.load_seed()
        requests = []
        def open_request(request, timeout):
            requests.append((request.full_url, urllib.parse.parse_qs(request.data.decode())))
            response = io.BytesIO(b'')
            response.status = 200
            return response
        with patch('urllib.request.urlopen', side_effect=open_request):
            result = seed.seed('10.0.0.8', ['Avengers: Endgame', 'First', 'Avengers: Endgame', 'Last'], workers=8)
        self.assertEqual(result, {'users_registered': 1000, 'movies_registered': 3})
        self.assertEqual(len(requests), 1003)
        users = [data for url, data in requests if url.endswith('/user/register')]
        movies = [data for url, data in requests if url.endswith('/movie/register')]
        self.assertEqual({data['username'][0] for data in users}, {'username_' + str(i) for i in range(1, 1001)})
        self.assertTrue(all(data['password'][0] == data['username'][0].replace('username_', 'password_') for data in users))
        self.assertEqual({(data['title'][0], data['movie_id'][0]) for data in movies}, {('Avengers: Endgame','movie_id_1'),('First','movie_id_2'),('Last','movie_id_4')})

    def test_seed_never_retries_failed_mutations(self):
        seed = self.load_seed()
        requests = []
        def fail(request, timeout):
            requests.append(request.data)
            raise OSError('connection lost after mutation')
        with patch('urllib.request.urlopen', side_effect=fail), self.assertRaises(OSError):
            seed.seed('10.0.0.8', ['Avengers: Endgame'], workers=1)
        self.assertEqual(len(requests), len(set(requests)))

    def test_canary_posts_one_unique_review_without_json_body_requirement(self):
        seed = self.load_seed()
        requests = []
        def success(request, timeout):
            requests.append(urllib.parse.parse_qs(request.data.decode()))
            response = io.BytesIO(b'')
            response.status = 200
            return response
        with patch('urllib.request.urlopen', side_effect=success):
            result = seed.canary('10.0.0.8', '012345abcdef', 'Avengers: Endgame')
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]['title'], ['Avengers: Endgame'])
        self.assertEqual(requests[0]['text'], ['benchmark-media-canary-012345abcdef'])
        self.assertEqual(result, {'canary_text': 'benchmark-media-canary-012345abcdef', 'http_status': 200})

import json
import subprocess
from types import SimpleNamespace
from tests.test_deathstarbench_candidate_workload import candidate_images
from tests.test_deathstarbench_hotel_benchmark import METRICS as HOTEL_METRICS

MEDIA_METRICS = HOTEL_METRICS.replace('hotel_reservation', 'media_microservices')


class MediaRuntimeTests(unittest.TestCase):
    def media(self):
        path = Path(__file__).parents[1] / 'app/deathstarbench_media_benchmark.py'
        self.assertTrue(path.exists(), 'Media requires a distributed synthetic runtime')
        from app import deathstarbench_media_benchmark
        return deathstarbench_media_benchmark

    def test_manifests_keep_32_components_native_architecture_and_no_policies(self):
        media = self.media()
        for architecture, arch in [('x86_64', 'amd64'), ('aarch64', 'arm64')]:
            result = media.render_media_manifests({'candidate_images': candidate_images('media_microservices')}, architecture, '10.240.2.10', '012345abcdef')
            support = [i for i in result['support'] if i['kind'] == 'Deployment']
            self.assertEqual(len(support), 19)
            self.assertEqual(len(result['applications']), 13)
            self.assertEqual(len(result['database_names']), 8)
            self.assertFalse(any(i['kind'] == 'NetworkPolicy' for i in result['support']))
            for deployment in result['applications'].values():
                spec = deployment['spec']['template']['spec']
                self.assertEqual(spec['nodeSelector']['kubernetes.io/arch'], arch)
                self.assertNotIn('schedulingGates', spec)
                self.assertNotIn('activeDeadlineSeconds', spec)

    def test_metrics_reject_other_workload_and_inconsistent_counts(self):
        media = self.media()
        result = media.parse_media_metrics(MEDIA_METRICS)
        self.assertEqual(result['errors'], 10)
        self.assertEqual(result['successful_requests'], 90)
        for data in [HOTEL_METRICS, MEDIA_METRICS * 2, MEDIA_METRICS.replace('body_check_successes=90', 'body_check_successes=80')]:
            with self.assertRaises(media.MediaBenchmarkError):
                media.parse_media_metrics(data)

    def test_database_baseline_rejects_preexisting_reviews_and_missing_users(self):
        media = self.media()
        baseline = {'users':1000,'movies':995,'reviews':0,'user_review_references':0,'movie_review_references':0,'cast_info':0,'movie_info':0,'plots':0,'user_id':'9223372036854775806'}
        self.assertEqual(media.parse_database_snapshot('MEDIA_DATABASE ' + json.dumps(baseline), baseline=True), baseline)
        for key, bad in [('reviews',1), ('users',999), ('movie_info',1)]:
            broken = dict(baseline, **{key:bad})
            with self.assertRaises(media.MediaBenchmarkError):
                media.parse_database_snapshot('MEDIA_DATABASE ' + json.dumps(broken), baseline=True)

    def test_persistence_reports_lost_indexes_but_rejects_http_only_noop(self):
        media = self.media()
        before = {'reviews':1,'user_review_references':1,'movie_review_references':1}
        after = {'reviews':81,'user_review_references':80,'movie_review_references':78}
        result = media.persistence_metrics(before, after, media.parse_media_metrics(MEDIA_METRICS))
        self.assertEqual(result['stored_reviews'],80)
        self.assertEqual(result['stored_reviews_minus_http_successes'],-10)
        self.assertEqual(result['user_references_minus_stored_reviews'],-1)
        self.assertEqual(result['movie_references_minus_stored_reviews'],-3)
        with self.assertRaises(media.MediaBenchmarkError):
            media.persistence_metrics(before,before,media.parse_media_metrics(MEDIA_METRICS))

    def test_canary_requires_exact_64bit_id_and_both_correct_indexes(self):
        media = self.media()
        value = {'review_id':'9223372036854775805','user_id':'9223372036854775806','movie_id':'movie_id_1','text':'benchmark-media-canary-012345abcdef','rating':5,'user_references':1,'movie_references':1}
        self.assertEqual(media.parse_canary('MEDIA_CANARY ' + json.dumps(value),'012345abcdef','9223372036854775806'),value)
        for key,bad in [('review_id',9223372036854775805),('user_id','9223372036854775807'),('movie_references',0),('text','wrong')]:
            with self.assertRaises(media.MediaBenchmarkError):
                media.parse_canary('MEDIA_CANARY ' + json.dumps(dict(value,**{key:bad})),'012345abcdef','9223372036854775806')

    def setup_runtime(self):
        from app.deathstarbench_distributed import RuntimeHost
        from app.deathstarbench_media_release import load_media_runtime_lock
        self.lock = load_media_runtime_lock()
        self.plan = {'deathstarbench': {'workload':'media_microservices', 'topology_id':'distributed_tiered_v1', 'runtime_id':'k3s_v1', 'threads':2,'connections':4,'request_rate':100,'warmup_seconds':10,'duration_seconds':10}}
        hosts = {name: RuntimeHost(name,name,name,'10.240.1.' + str(i),'x86_64',name + '_public_ip',None)
                 for i,name in enumerate(('control','database','cache','application','load-generator'),10)}
        self.runtime = SimpleNamespace(host=lambda name: hosts[name],load_generator=hosts['load-generator'],load_generator_private_ip='10.240.2.10',topology_fingerprint='sha256:'+'a'*64)
        manifests = self.media().render_media_manifests(self.lock,'x86_64','10.240.2.10','012345abcdef')
        names = [i['metadata']['name'] for i in manifests['support'] if i['kind']=='Deployment'] + list(manifests['applications'])
        self.pods = {'items':[{'metadata':{'uid':name,'labels':{'app.kubernetes.io/component':name}},'spec':{'nodeName':'node'},'status':{'containerStatuses':[{'restartCount':0,'ready':True,'containerID':name,'imageID':'sha256:'+'b'*64,'state':{'running':{}}}]}} for name in names]}
        self.baseline = {'users':1000,'movies':995,'reviews':0,'user_review_references':0,'movie_review_references':0,'cast_info':0,'movie_info':0,'plots':0,'user_id':'9223372036854775806'}
        self.calls,self.states = [],[]
        self.snapshots = 0
        self.job = {'id':'012345abcdef','resources':{},'results':[]}

    def execute(self,job,command,**kwargs):
        self.calls.append((command,kwargs))
        if command.endswith(' attest'):
            return ('CANDIDATE_DSB_LOAD_DRIVER_ARTIFACT workload=media_microservices architecture=x86_64 wrk_binary_sha256='+self.lock['wrk_binary_sha256']+' context_sha256='+self.lock['load_driver_context_sha256']+' request_script_sha256='+self.lock['load_script_sha256']+' revision='+self.lock['load_driver_revision'])
        if command.startswith('python3 - seed'):
            return 'MEDIA_API_RESULT '+json.dumps({'mode':'seed','run_id':job['id'],'users_registered':1000,'movies_registered':995})
        if command.startswith('python3 - canary'):
            return 'MEDIA_API_RESULT '+json.dumps({'mode':'canary','run_id':job['id'],'canary_text':'benchmark-media-canary-'+job['id'],'http_status':200})
        if 'MEDIA_DATABASE' in command:
            count = [0,1,81,161][min(self.snapshots,3)]
            self.snapshots += 1
            return 'MEDIA_DATABASE '+json.dumps(dict(self.baseline,reviews=count,user_review_references=count,movie_review_references=count))
        if 'MEDIA_CANARY' in command:
            return 'MEDIA_CANARY '+json.dumps({'review_id':'9223372036854775805','user_id':self.baseline['user_id'],'movie_id':'movie_id_1','text':'benchmark-media-canary-'+job['id'],'rating':5,'user_references':1,'movie_references':1})
        if 'get pods -o json' in command:
            return json.dumps(self.pods)
        if 'timeout --signal=TERM' in command:
            return MEDIA_METRICS
        return ''

    def run_runtime(self,execute=None):
        media=self.media()
        with patch.object(media,'distributed_k3s_candidate_plan',return_value=self.runtime):
            return media.run_media_benchmark(self.job,self.plan,self.lock,execute=execute or self.execute,emit=lambda *_:None,persist=lambda job:self.states.append(job['resources'][media.JOURNAL_KEY]['state']))

    def test_runtime_starts_support_then_cpp_then_nginx_and_checks_persistence_per_phase(self):
        self.setup_runtime()
        result=self.run_runtime()
        self.assertEqual(self.job['results'],[result])
        self.assertEqual(self.states[-1],'measurement_complete')
        self.assertLess(self.states.index('starting_applications'),self.states.index('initializing_dataset'))
        payloads=[json.loads(kw['stdin_text'])['items'] for command,kw in self.calls if 'apply -f -' in command]
        self.assertEqual(len([i for i in payloads[0] if i['kind']=='Deployment']),19)
        self.assertEqual(len(payloads[1]),12)
        self.assertEqual(payloads[2][0]['metadata']['name'],'nginx-web-server')
        self.assertEqual(result['metadata']['persistence']['stored_reviews'],80)
        self.assertEqual(result['metrics']['stored_reviews'],80)
        self.assertEqual(result['metadata']['warmup_persistence']['stored_reviews'],80)
        self.assertEqual(self.snapshots,4)
        self.assertTrue(all(kw['transport_attempts']==1 for _,kw in self.calls))
        self.assertEqual(result['metadata']['dataset_manifest_sha256'],self.lock['dataset_manifest_sha256'])
        self.assertIn('restorecon -R -x',self.calls[0][0])
        for command,_ in self.calls:
            subprocess.run(['bash','-n'],input=command,text=True,check=True)

    def test_runtime_failed_baseline_cannot_start_load_or_publish_result(self):
        self.setup_runtime()
        def bad(job,command,**kw):
            output=self.execute(job,command,**kw)
            return output.replace('"users": 1000','"users": 999') if 'MEDIA_DATABASE' in command else output
        with self.assertRaises(self.media().MediaBenchmarkError):
            self.run_runtime(bad)
        self.assertEqual(self.job['results'],[])
        self.assertFalse(any('timeout --signal=TERM' in command for command,_ in self.calls))
        self.assertIn(self.media().JOURNAL_KEY,self.job['resources'])

    def test_runtime_rejects_pod_restart_and_existing_journal(self):
        self.setup_runtime()
        self.pods['items'][0]['status']['containerStatuses'][0]['restartCount']=1
        with self.assertRaises(self.media().MediaBenchmarkError):
            self.run_runtime()
        with self.assertRaisesRegex(self.media().MediaBenchmarkError,'fresh benchmark'):
            self.run_runtime()
        self.assertEqual(self.job['results'],[])

    def test_canary_mongo_queries_preserve_int64_and_match_index_owners(self):
        media=self.media()
        command=media.canary_verification_command('012345abcdef','9223372036854775806')
        python_script=command.split('\n',1)[1].rsplit('\n',1)[0]
        real_check_output=subprocess.check_output
        calls=[]
        def mongo(command,**kwargs):
            component=next(part for part in command if part.startswith('deployment/'))
            calls.append(component)
            prefix='''function NumberLong(v) { if(!(this instanceof NumberLong)) return new NumberLong(v); this.value=BigInt(v); }
NumberLong.prototype.toString=function(){return 'NumberLong("'+this.value.toString()+'")';};
var review={review_id:NumberLong('9223372036854775805'),user_id:NumberLong('9223372036854775806'),movie_id:'movie_id_1',text:'benchmark-media-canary-012345abcdef',rating:5};
var wrong={review_id:NumberLong('9223372036854775804')};
var docs=COMPONENT==='deployment/review-storage-mongodb'?[review]:COMPONENT==='deployment/user-review-mongodb'?[{user_id:NumberLong('9223372036854775806'),reviews:[review]},{user_id:NumberLong('9223372036854775807'),reviews:[wrong]}]:[{movie_id:'movie_id_1',reviews:[review]},{movie_id:'movie_id_2',reviews:[wrong]}];
function matches(d,q) {return !q || Object.keys(q).every(k=>String(d[k])===String(q[k]));}
var db={getCollection:function(){return {count:q=>docs.filter(d=>matches(d,q)).length,findOne:q=>docs.find(d=>matches(d,q)),find:q=>docs.filter(d=>matches(d,q))};}};
var print=console.log;
'''.replace('COMPONENT',json.dumps(component))
            return real_check_output(['node','-e',prefix+command[-1]],text=True)
        output=io.StringIO()
        with patch('subprocess.check_output',side_effect=mongo),patch('sys.stdout',output):
            exec(compile(python_script,'<canary>','exec'),{})
        result=media.parse_canary(output.getvalue(),'012345abcdef','9223372036854775806')
        self.assertEqual(result['review_id'],'9223372036854775805')
        self.assertEqual(calls,['deployment/review-storage-mongodb','deployment/user-review-mongodb','deployment/movie-review-mongodb'])

    def test_baseline_queries_verify_titles_keyspace_and_empty_index_collections(self):
        media=self.media()
        command=media.database_snapshot_command(['One', 'Two', 'One', 'Four'],baseline=True)
        python_script=command.split('\n',1)[1].rsplit('\n',1)[0]
        real_check_output=subprocess.check_output
        def execute_database(dirty=False):
            def mongo(command,**kwargs):
                component=next(part for part in command if part.startswith('deployment/'))
                prefix='''function NumberLong(v) {if(!(this instanceof NumberLong))return new NumberLong(v);this.value=BigInt(v);}
NumberLong.prototype.toString=function(){return 'NumberLong("'+this.value.toString()+'")';};
var users=Array.from({length:1000},(_,i)=>({username:'username_'+(i+1),first_name:'first_name_'+(i+1),last_name:'last_name_'+(i+1),user_id:NumberLong('9223372036854775806')}));
var movies=[{title:'One',movie_id:'movie_id_1'},{title:'Two',movie_id:'movie_id_2'},{title:'Four',movie_id:'movie_id_4'}];
var docs=COMPONENT==='deployment/user-mongodb'?users:COMPONENT==='deployment/movie-id-mongodb'?movies:DIRTY&&COMPONENT==='deployment/user-review-mongodb'?[{reviews:[]}]:[];
function matches(d,q){return !q||Object.keys(q).every(k=>String(d[k])===String(q[k]));}
var db={getCollection:function(){return {count:q=>docs.filter(d=>matches(d,q)).length,findOne:q=>docs.find(d=>matches(d,q)),find:q=>docs.filter(d=>matches(d,q))};}};
var print=console.log;
'''.replace('COMPONENT',json.dumps(component)).replace('DIRTY',str(dirty).lower())
                return real_check_output(['node','-e',prefix+command[-1]],text=True,stderr=subprocess.PIPE)
            output=io.StringIO()
            with patch('subprocess.check_output',side_effect=mongo),patch('sys.stdout',output):
                exec(compile(python_script,'<baseline>','exec'),{})
            return json.loads(output.getvalue().removeprefix('MEDIA_DATABASE '))
        result=execute_database()
        self.assertEqual(result['users'],1000)
        self.assertEqual(result['movies'],3)
        self.assertEqual(result['user_id'],'9223372036854775806')
        with self.assertRaises(subprocess.CalledProcessError):
            execute_database(dirty=True)
