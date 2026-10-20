import io
import json
import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app import deathstarbench_hotel_benchmark as hotel
from app.deathstarbench_distributed import RuntimeHost
from app.deathstarbench_hotel_release import load_hotel_runtime_lock


METRICS = '''CANDIDATE_DSB_LOADGEN_LOGICAL_CPUS=2
Sent 102 requests
CANDIDATE_DSB_METRICS schema=candidate-body-v1 measurement_qualified=0 workload=hotel_reservation seed_base=20261008 workers=2 completed_responses=100 summary_requests=100 body_check_successes=90 http_errors=2 http_status_errors=2 semantic_failures=8 socket_errors=4 connect_errors=1 read_errors=1 write_errors=1 timeout_errors=1 counter_consistent=1 duration_seconds=10.000000 body_check_successes_per_second=9.000000 completed_failure_percent=10.000000 p50_ms=1 p95_ms=2 p99_ms=3
Percent of CPU this job got: 195%
Maximum resident set size (kbytes): 1234
'''


class HotelBenchmarkTests(unittest.TestCase):
    def setUp(self):
        self.lock = load_hotel_runtime_lock()
        self.plan = {'deathstarbench': {'workload': 'hotel_reservation',
            'topology_id': 'distributed_tiered_v1', 'runtime_id': 'k3s_v1',
            'threads': 2, 'connections': 4, 'request_rate': 100,
            'warmup_seconds': 0, 'duration_seconds': 10}}

    def test_manifest_uses_guarded_images_and_seed_before_applications(self):
        for architecture, platform in [('x86_64', 'linux/amd64'), ('aarch64', 'linux/arm64')]:
            with self.subTest(architecture=architecture):
                value = hotel.render_hotel_manifests(self.lock, architecture, '10.240.2.10', '012345abcdef')
                support_names = {item['metadata']['name'] for item in value['support'] if item['kind'] == 'Deployment'}
                self.assertFalse(support_names & hotel.APPLICATIONS)
                self.assertEqual(len(support_names), 14)
                self.assertEqual(len(value['database_names']), 8)
                self.assertNotIn('NetworkPolicy', {item['kind'] for item in value['support']})
                for deployment in value['applications'].values():
                    spec = deployment['spec']['template']['spec']
                    self.assertNotIn('activeDeadlineSeconds', spec)
                    container = spec['containers'][0]
                    self.assertEqual(container['image'], self.lock['application_images'][platform])
                    environment = {item['name']: item['value'] for item in container['env']}
                    self.assertEqual(environment['HOTEL_CANDIDATE_RUN_ID'], '012345abcdef')
                    self.assertEqual(environment['HOTEL_CANDIDATE_CONTRACT_SHA256'], self.lock['dataset_fingerprint'])
                initializer = value['initializer']['spec']
                self.assertEqual(initializer['restartPolicy'], 'Never')
                self.assertEqual(initializer['containers'][0]['command'], ['/go/bin/hotel-initialize', 'initialize-empty'])

    def test_metrics_treat_http_200_body_failures_as_failed_requests(self):
        value = hotel.parse_hotel_metrics(METRICS)
        self.assertEqual(value['throughput_requests_per_second'], 9)
        self.assertEqual(value['completed_requests_per_second'], 10)
        self.assertEqual(value['error_rate_percent'], 10)
        self.assertEqual(value['errors'], 10)
        self.assertEqual(value['semantic_failures'], 8)
        self.assertEqual(value['successful_requests'], 90)
        self.assertEqual(value['uncompleted_requests'], 2)
        self.assertEqual(value['load_generator_capacity_used_percent'], 97.5)
        self.assertIn('load_generator_saturation_warning', value)

    def test_comparison_manifest_identity_ignores_only_run_identity(self):
        first = hotel.render_hotel_manifests(self.lock, 'x86_64', '10.240.2.10', '012345abcdef')
        second = hotel.render_hotel_manifests(self.lock, 'x86_64', '10.240.2.10', 'fedcba543210')
        arm = hotel.render_hotel_manifests(self.lock, 'aarch64', '10.240.2.10', '012345abcdef')
        self.assertEqual(first['rendered_manifest_sha256'], second['rendered_manifest_sha256'])
        self.assertNotEqual(first['run_manifest_sha256'], second['run_manifest_sha256'])
        self.assertNotEqual(first['rendered_manifest_sha256'], arm['rendered_manifest_sha256'])

    def test_completed_initializer_without_labels_is_not_a_service(self):
        runtime, pods = self._runtime_and_pods()
        document = json.loads(pods) if isinstance(pods, str) else pods
        document['items'].append({'metadata': {'name': 'hotel-initialize'}, 'status': {'phase': 'Succeeded'}})
        self.assertEqual(len(hotel._pod_snapshot(json.dumps(document))), 24)

    def test_invalid_or_ambiguous_measurements_are_rejected(self):
        cases = [METRICS + METRICS, METRICS.replace('Sent 102', 'Sent 99'),
                 METRICS.replace('counter_consistent=1', 'counter_consistent=0'),
                 METRICS.replace('summary_requests=100', 'summary_requests=101'),
                 METRICS.replace('workload=hotel_reservation', 'workload=social_network'),
                 METRICS.replace('body_check_successes=90', 'body_check_successes=80'),
                 METRICS.replace('p99_ms=3', 'p99_ms=nan'),
                 METRICS.replace('p99_ms=3', 'p99_ms=0'),
                 METRICS.replace('body_check_successes_per_second=9.000000', 'body_check_successes_per_second=99'),
                 METRICS.replace('socket_errors=4', 'socket_errors=10')]
        for value in cases:
            with self.subTest(value=value):
                with self.assertRaises(hotel.HotelBenchmarkError):
                    hotel.parse_hotel_metrics(value)

    def test_load_command_has_fixed_driver_protocol_and_bound(self):
        options = hotel._options(self.plan)
        command = hotel.hotel_load_command(self.lock['load_generator_image'], '10.240.1.13', options,
                                          10, '012345abcdef', 'measurement')
        self.assertIn('--port 8080 --threads 2 --connections 4 --rate 100 --duration 10', command)
        self.assertIn('--pull=never --network=host', command)
        self.assertIn('timeout --signal=TERM --kill-after=30s 190s', command)
        self.assertNotIn('--max-user-index', command)
        subprocess.run(['bash', '-n'], input=command, text=True, check=True)

    def test_load_settings_reject_unachievable_thread_partition(self):
        self.plan['deathstarbench']['request_rate'] = 101
        with self.assertRaisesRegex(hotel.HotelBenchmarkError, 'multiples'):
            hotel._options(self.plan)

    def test_semantic_probe_checks_bodies_and_uses_unmeasured_dates(self):
        command = hotel.semantic_smoke_command('10.240.1.13')
        script = command.split('\n', 1)[1].rsplit('\n', 1)[0]
        calls = []

        def urlopen(url, timeout):
            calls.append(url)
            if '/user?' in url:
                message = ('Failed. Please check your username and password. '
                           if 'password=invalid' in url else 'Login successfully!')
                data = {'message': message}
            elif '/reservation?' in url:
                data = {'message': 'Reserve successfully!'}
            else:
                data = {'type': 'FeatureCollection', 'features': []}
            response = io.StringIO(json.dumps(data))
            response.status = 200
            return response

        output = io.StringIO()
        with patch('urllib.request.urlopen', side_effect=urlopen), patch('sys.stdout', output):
            exec(compile(script, '<semantic-smoke>', 'exec'), {})
        self.assertTrue(hotel.parse_semantic_smoke(output.getvalue())['invalid_login_rejected'])
        reservation = next(url for url in calls if '/reservation?' in url)
        self.assertIn('inDate=2015-05-20', reservation)
        self.assertNotIn('2015-04', reservation)
        with patch('urllib.request.urlopen', return_value=SimpleNamespace()), self.assertRaises(Exception):
            exec(compile(script.replace('range(30)', 'range(1)').replace('attempt == 29', 'attempt == 0'),
                         '<semantic-smoke>', 'exec'), {})

    def _runtime_and_pods(self):
        hosts = {name: RuntimeHost(name, name, name, '10.240.1.' + str(index), 'x86_64',
                                   name + '_public_ip', None)
                 for index, name in enumerate(('control', 'database', 'cache', 'application', 'load-generator'), 10)}
        runtime = SimpleNamespace(host=lambda name: hosts[name], load_generator=hosts['load-generator'],
                                  load_generator_private_ip='10.240.2.10', topology_fingerprint='sha256:' + 'a'*64)
        manifests = hotel.render_hotel_manifests(self.lock, 'x86_64', '10.240.2.10', '012345abcdef')
        deployments = [item for item in manifests['support'] if item['kind'] == 'Deployment']
        deployments += list(manifests['applications'].values())
        pods = {'items': [{'metadata': {'uid': item['metadata']['name'] + '-uid',
                                      'labels': item['spec']['template']['metadata']['labels']},
                           'spec': {'nodeName': 'some-node'},
                           'status': {'containerStatuses': [{'restartCount': 0, 'ready': True,
                              'containerID': 'container-' + item['metadata']['name'],
                              'imageID': 'sha256:' + 'b'*64, 'state': {'running': {}}}]}}
                          for item in deployments]}
        return runtime, pods

    def _execute(self, pods, calls, bad_initialization=False):
        def execute(job, command, **kwargs):
            calls.append((command, kwargs))
            if 'for attempt in range(125)' in command:
                return json.dumps({'database_verification_completed': not bad_initialization,
                                   'mode': 'initialize-empty', 'run_id': job['id'],
                                   'contract_sha256': self.lock['dataset_fingerprint']})
            if command.endswith(' attest'):
                return ('CANDIDATE_DSB_LOAD_DRIVER_ARTIFACT workload=hotel_reservation architecture=x86_64 '
                        'wrk_binary_sha256=' + self.lock['wrk_binary_sha256'] +
                        ' context_sha256=' + self.lock['load_driver_context_sha256'] +
                        ' request_script_sha256=' + self.lock['load_script_sha256'] +
                        ' revision=' + self.lock['load_driver_revision'])
            if 'HOTEL_SEMANTIC_SMOKE ' in command:
                return 'HOTEL_SEMANTIC_SMOKE ' + json.dumps(dict.fromkeys(
                    ('valid_login', 'invalid_login_rejected', 'reservation_success', 'search', 'recommendations'), True))
            if 'get pods -o json' in command:
                return json.dumps(pods)
            if 'timeout --signal=TERM' in command:
                return METRICS
            return ''
        return execute

    def test_full_runtime_orders_initialization_and_persists_ui_result(self):
        runtime, pods = self._runtime_and_pods()
        calls, states = [], []
        job = {'id': '012345abcdef', 'resources': {}, 'results': []}
        with patch.object(hotel, 'distributed_k3s_candidate_plan', return_value=runtime):
            result = hotel.run_hotel_benchmark(job, self.plan, self.lock, execute=self._execute(pods, calls),
                emit=lambda *_: None, persist=lambda job: states.append(job['resources'][hotel.JOURNAL_KEY]['state']))
        self.assertEqual(states[-1], 'measurement_complete')
        self.assertLess(states.index('initializing_dataset'), states.index('starting_applications'))
        self.assertEqual(job['results'], [result])
        self.assertEqual(result['metrics']['errors'], 10)
        self.assertNotIn('dataset_nodes_sha256', result['metadata'])
        self.assertEqual(result['metadata']['dataset_manifest_sha256'], self.lock['dataset_manifest_sha256'])
        self.assertEqual(result['metadata']['runtime_revision'], self.lock['runtime_revision'])
        self.assertEqual(set(job['resources']), {hotel.JOURNAL_KEY})
        self.assertTrue(all(kwargs['transport_attempts'] == 1 for _, kwargs in calls))
        storage = calls[0][0]
        self.assertIn('semanage fcontext', storage)
        self.assertIn('container_file_t', storage)
        self.assertIn('restorecon -R -x', storage)
        subprocess.run(['bash', '-n'], input=storage, text=True, check=True)
        payloads = [json.loads(kwargs['stdin_text'])['items'] for _, kwargs in calls if 'stdin_text' in kwargs]
        self.assertFalse(any(item['kind'] == 'Deployment' and item['metadata']['name'] in hotel.APPLICATIONS
                             for item in payloads[0]))
        self.assertEqual(payloads[1][0]['metadata']['name'], 'hotel-initialize')

    def test_failed_initializer_does_not_start_apps_or_publish_result(self):
        runtime, pods = self._runtime_and_pods()
        calls = []
        job = {'id': '012345abcdef', 'resources': {}, 'results': []}
        with patch.object(hotel, 'distributed_k3s_candidate_plan', return_value=runtime), \
                self.assertRaisesRegex(hotel.HotelBenchmarkError, 'initializer'):
            hotel.run_hotel_benchmark(job, self.plan, self.lock,
                execute=self._execute(pods, calls, bad_initialization=True), emit=lambda *_: None, persist=lambda *_: None)
        self.assertEqual(job['results'], [])
        self.assertEqual(len([kwargs for _, kwargs in calls if 'stdin_text' in kwargs]), 2)
        self.assertIn(hotel.JOURNAL_KEY, job['resources'])

    def test_restarted_workload_cannot_create_a_valid_result(self):
        _, pods = self._runtime_and_pods()
        self.assertEqual(len(hotel._pod_snapshot(json.dumps(pods))), 24)
        pods['items'][0]['status']['containerStatuses'][0]['restartCount'] = 1
        with self.assertRaisesRegex(hotel.HotelBenchmarkError, 'restarted'):
            hotel._pod_snapshot(json.dumps(pods))

    def test_existing_execution_is_cleanup_only(self):
        job = {'id': '012345abcdef', 'resources': {hotel.JOURNAL_KEY: {'state': 'measuring'}}}
        with self.assertRaisesRegex(hotel.HotelBenchmarkError, 'fresh benchmark'):
            hotel.run_hotel_benchmark(job, self.plan, self.lock, execute=None, emit=None, persist=None)


if __name__ == '__main__':
    unittest.main()
