import base64
import copy
import hashlib
import unittest
from unittest import mock

from app import comparison
from app.deathstarbench_contract import (
    DISTRIBUTED_DATASET_REVISION,
    DISTRIBUTED_LOAD_DRIVER_REVISION,
    DISTRIBUTED_MEASUREMENT_REVISION,
    DISTRIBUTED_RUNTIME_REVISION,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    DISTRIBUTED_WORKLOAD_REVISION,
    K3S_RUNTIME_ID,
)
from app.deathstarbench_distributed import (
    prepare_azure_distributed_k3s_candidate,
    prepare_azure_distributed_social_network_candidate,
)
from app.deathstarbench_distributed_measurement import (
    DATASET_FOLLOW_COUNT,
    DATASET_POST_COUNT,
    DATASET_TIMELINE_USER_COUNT,
    DATASET_USER_COUNT,
    EXECUTION_JOURNAL_KEY,
    MAX_RESULT_OUTPUT_CHARS,
    DistributedMeasurementError,
    dataset_database_attestation_command,
    durable_initializer_dispatch_command,
    durable_initializer_status_command,
    initializer_execution_identity,
    non_mutating_frontend_probe_command,
    parse_dataset_attestation,
    parse_dataset_database_attestation,
    parse_durable_initializer_status,
    parse_wrk2_execution_output,
    run_azure_distributed_social_network_measurement,
    social_network_initialization_command,
)
from app.deathstarbench_k3s_workload import EXPECTED_COMPONENTS, UPSTREAM_REVISION
from app.models import DeathStarBenchOptions
from tests.test_deathstarbench_distributed_runtime import (
    FakeRemoteExecutor,
    candidate_image_lock,
    candidate_job,
    candidate_workload_attestation,
)


APPLICATION_PRIVATE_IP = '10.240.1.13'
LOAD_GENERATOR_HOST_KEY = 'azure_dsb_load_generator_public_ip'
WRK_BINARY_SHA256 = 'a' * 64
COMPILER_VERSION_SHA256 = 'b' * 64


def measurement_options(**overrides):
    values = {
        'topology_id': DISTRIBUTED_TIERED_TOPOLOGY_ID,
        'runtime_id': K3S_RUNTIME_ID,
        'workload': 'social_network',
        'warmup_seconds': 5,
        'duration_seconds': 10,
        'threads': 2,
        'connections': 4,
        'request_rate': 20,
    }
    values.update(overrides)
    return DeathStarBenchOptions(**values)


def ready_workload_job():
    job = candidate_job()
    image_lock = candidate_image_lock()
    prepare_azure_distributed_k3s_candidate(
        job,
        execute=FakeRemoteExecutor(),
    )
    attestation = candidate_workload_attestation(job, image_lock)
    with mock.patch(
        'app.deathstarbench_distributed.parse_workload_attestation',
        return_value=attestation,
    ):
        prepare_azure_distributed_social_network_candidate(
            job,
            image_lock,
            execute=FakeRemoteExecutor(),
        )
    return job, image_lock


def wrk2_output(duration_seconds, *, prefix=''):
    total = int(duration_seconds) * 20
    if prefix and not prefix.endswith('\n'):
        prefix += '\n'
    return (
        prefix
        + f'Sent {total} requests\n'
        + 'OCI_DSB_METRICS '
        + f'duration_seconds={float(duration_seconds):.6f} '
        + f'total_requests={total} '
        + 'throughput_requests_per_second=20.000000 '
        + 'errors=0 error_rate_percent=0.000000 '
        + 'socket_errors=0 connect_errors=0 read_errors=0 '
        + 'write_errors=0 timeout_errors=0 '
        + 'p50_ms=1.000000 p95_ms=2.000000 p99_ms=3.000000\n'
        + 'OCI_DSB_LOADGEN_LOGICAL_CPUS=2\n'
        + '\tPercent of CPU this job got: 50%\n'
        + '\tMaximum resident set size (kbytes): 4096\n'
    )


def initializer_status_output(job, *, state='succeeded', output=None):
    identity = initializer_execution_identity(job, APPLICATION_PRIVATE_IP)
    if state == 'not_found':
        fields = (
            'load=not-found active=inactive sub=dead result=success '
            'exec_code=0 exec_status=0'
        )
        payload_sha256 = 'none'
        service_user = 'none'
        exec_start_match = 'no'
        invocation = 'none'
        start_mono_usec = 0
        exit_mono_usec = 0
        output = ''
    elif state == 'pending':
        fields = (
            'load=loaded active=activating sub=start result=success '
            'exec_code=0 exec_status=0'
        )
        payload_sha256 = identity['payload_sha256']
        service_user = 'root'
        exec_start_match = 'yes'
        invocation = 'c' * 32
        start_mono_usec = 100
        exit_mono_usec = 0
        output = ''
    elif state == 'failed':
        fields = (
            'load=loaded active=failed sub=failed result=exit-code '
            'exec_code=1 exec_status=1'
        )
        payload_sha256 = identity['payload_sha256']
        service_user = 'root'
        exec_start_match = 'yes'
        invocation = 'c' * 32
        start_mono_usec = 100
        exit_mono_usec = 200
        output = output or 'initializer failed\n'
    else:
        fields = (
            'load=loaded active=active sub=exited result=success '
            'exec_code=1 exec_status=0'
        )
        payload_sha256 = identity['payload_sha256']
        service_user = 'root'
        exec_start_match = 'yes'
        invocation = 'c' * 32
        start_mono_usec = 100
        exit_mono_usec = 200
        output = output or (
            'DISTRIBUTED_DSB_DATASET_READY '
            f'graph=socfb-Reed98 users={DATASET_USER_COUNT} '
            f'follows={DATASET_FOLLOW_COUNT} posts={DATASET_POST_COUNT} '
            f'revision={UPSTREAM_REVISION}\n'
        )
    output_sha256 = (
        hashlib.sha256(output.encode()).hexdigest() if output else 'none'
    )
    return (
        'DISTRIBUTED_DSB_INITIALIZER_STATUS '
        f'unit={identity["unit_name"]} run_user={identity["run_user"]} '
        f'payload_sha256={payload_sha256} service_user={service_user} '
        f'exec_start_match={exec_start_match} {fields} '
        f'invocation_id={invocation} start_mono_usec={start_mono_usec} '
        f'exit_mono_usec={exit_mono_usec} '
        f'output_bytes={len(output.encode())} '
        f'output_sha256={output_sha256}\n{output}'
    )


class MeasurementExecutor:
    def __init__(
        self,
        *,
        fail_on=None,
        measurement_prefix='',
        initializer_statuses=None,
    ):
        self.calls = []
        self.fail_on = fail_on
        self.measurement_prefix = measurement_prefix
        self.database_attestations = 0
        self.initializer_statuses = list(initializer_statuses or ())

    @staticmethod
    def stage(command):
        if 'DISTRIBUTED_DSB_LOAD_GENERATOR_READY' in command:
            return 'load_generator'
        if 'DISTRIBUTED_DSB_FRONTEND_READY' in command:
            return 'frontend_probe'
        if 'DISTRIBUTED_DSB_INITIALIZER_DISPATCHED' in command:
            return 'initialization'
        if 'DISTRIBUTED_DSB_INITIALIZER_STATUS' in command:
            return 'initialization_status'
        if 'DISTRIBUTED_DSB_DATABASE_DATA' in command:
            return 'database_attestation'
        if 'deployments.apps,services,persistentvolumes' in command:
            return 'workload_snapshot'
        if '/wrk2/wrk -D exp' in command:
            if ' -d 5s ' in command:
                return 'warmup'
            if ' -d 10s ' in command:
                return 'measurement'
        return None

    def __call__(self, _job, command, **kwargs):
        stage = self.stage(command)
        self.calls.append((stage, command, kwargs))
        if self.fail_on is not None and stage == self.fail_on:
            raise RuntimeError(f'injected {stage} failure')
        if stage == 'load_generator':
            return (
                'DISTRIBUTED_DSB_LOAD_GENERATOR_READY '
                f'architecture=x86_64 revision={UPSTREAM_REVISION} '
                f'wrk_sha256={WRK_BINARY_SHA256} '
                f'compiler_version_sha256={COMPILER_VERSION_SHA256}\n'
            )
        if stage == 'frontend_probe':
            return (
                'DISTRIBUTED_DSB_FRONTEND_READY '
                f'target={APPLICATION_PRIVATE_IP} port=8080\n'
            )
        if stage == 'initialization':
            identity = initializer_execution_identity(
                _job,
                APPLICATION_PRIVATE_IP,
            )
            return (
                'DISTRIBUTED_DSB_INITIALIZER_DISPATCHED '
                f'unit={identity["unit_name"]} '
                f'run_user={identity["run_user"]} '
                f'payload_sha256={identity["payload_sha256"]}\n'
            )
        if stage == 'initialization_status':
            if self.initializer_statuses:
                response = self.initializer_statuses.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response
            return initializer_status_output(_job)
        if stage == 'database_attestation':
            self.database_attestations += 1
            if self.database_attestations == 1:
                values = (0, 0, 0, 0, 0, 0, 0)
            else:
                values = (
                    DATASET_USER_COUNT,
                    DATASET_USER_COUNT,
                    DATASET_FOLLOW_COUNT,
                    DATASET_FOLLOW_COUNT,
                    DATASET_POST_COUNT,
                    DATASET_TIMELINE_USER_COUNT,
                    DATASET_POST_COUNT,
                )
            return (
                'DISTRIBUTED_DSB_DATABASE_DATA '
                f'users={values[0]} social_graph_users={values[1]} '
                f'followers={values[2]} followees={values[3]} '
                f'posts={values[4]} timeline_users={values[5]} '
                f'timeline_posts={values[6]}\n'
            )
        if stage == 'workload_snapshot':
            return '{"kind":"List","apiVersion":"v1","items":[]}'
        if stage == 'warmup':
            return wrk2_output(5)
        if stage == 'measurement':
            return wrk2_output(10, prefix=self.measurement_prefix)
        return ''

    @property
    def stages(self):
        return [stage for stage, _, _ in self.calls if stage is not None]


class DistributedDatasetContractTests(unittest.TestCase):
    def test_dataset_parser_requires_the_exact_reference_counts(self):
        output = (
            'initializer diagnostics\n'
            'DISTRIBUTED_DSB_DATASET_READY '
            'graph=socfb-Reed98 users=962 follows=37624 posts=9424 '
            f'revision={UPSTREAM_REVISION}\n'
        )

        parsed = parse_dataset_attestation(output)

        self.assertEqual(parsed['graph'], 'socfb-Reed98')
        self.assertEqual(parsed['users'], 962)
        self.assertEqual(parsed['follows'], 37624)
        self.assertEqual(parsed['posts'], 9424)
        self.assertEqual(parsed['upstream_revision'], UPSTREAM_REVISION)
        self.assertEqual(DATASET_USER_COUNT, 962)
        self.assertEqual(DATASET_FOLLOW_COUNT, 37624)
        self.assertEqual(DATASET_POST_COUNT, 9424)

        replacements = (
            ('users=962', 'users=961'),
            ('follows=37624', 'follows=18812'),
            ('posts=9424', 'posts=9425'),
            ('graph=socfb-Reed98', 'graph=ego-twitter'),
            (f'revision={UPSTREAM_REVISION}', 'revision=main'),
        )
        for old, new in replacements:
            with self.subTest(replacement=new):
                with self.assertRaises(DistributedMeasurementError):
                    parse_dataset_attestation(output.replace(old, new))
        with self.assertRaises(DistributedMeasurementError):
            parse_dataset_attestation(output + output.splitlines()[-1] + '\n')

        command = social_network_initialization_command(
            APPLICATION_PRIVATE_IP
        )
        self.assertIn('timeout --signal=TERM --kill-after=30s', command)
        self.assertIn('2100s bash -lc', command)
        self.assertIn('/^Registering Users', command)
        self.assertIn('/^Adding follows', command)
        self.assertIn('/^Composing posts', command)

        job = {'id': 'abc123def456', 'resources': {'ssh_user': 'benchmark'}}
        identity = initializer_execution_identity(job, APPLICATION_PRIVATE_IP)
        dispatch = durable_initializer_dispatch_command(
            job,
            APPLICATION_PRIVATE_IP,
        )
        status = durable_initializer_status_command(job)
        self.assertEqual(
            identity['unit_name'],
            'benchmark-deathstarbench-init-abc123def456.service',
        )
        self.assertEqual(identity['run_user'], 'benchmark')
        self.assertRegex(identity['payload_sha256'], r'^[0-9a-f]{64}$')
        self.assertIn('systemd-run --quiet --no-block', dispatch)
        self.assertIn('--property=Type=oneshot', dispatch)
        self.assertIn('--property=RemainAfterExit=yes', dispatch)
        self.assertIn('--property=User=root', dispatch)
        self.assertIn('--property=Restart=no', dispatch)
        self.assertIn('--property=KillMode=control-group', dispatch)
        self.assertIn('--property=TimeoutStopSec=30s', dispatch)
        self.assertIn('--property=TimeoutStartSec=2160s', dispatch)
        self.assertIn('INSTALLED_PAYLOAD_SHA256=$(sudo sha256sum', dispatch)
        self.assertNotIn('--collect', dispatch)
        self.assertNotIn('reset-failed', dispatch)
        self.assertNotIn('rm -rf', dispatch)
        self.assertIn('initializer.claimed', dispatch)
        encoded_script = dispatch.split('printf %s ', 1)[1].split(
            ' | base64',
            1,
        )[0]
        initializer_script = base64.b64decode(encoded_script).decode()
        self.assertIn('runuser --user "$RUN_USER"', initializer_script)
        self.assertIn('initializer.out.in-progress', initializer_script)
        self.assertNotIn('rmdir', initializer_script)
        self.assertIn(
            'mv -- "$TEMPORARY_OUTPUT_PATH" "$FINAL_OUTPUT_PATH"',
            initializer_script,
        )
        self.assertNotIn('systemd-run', status)
        self.assertNotIn('systemctl start', status)
        self.assertIn('systemctl show', status)
        self.assertIn('sudo test -f "$OUTPUT_PATH"', status)
        self.assertIn('test "$TERMINAL" = yes', status)
        self.assertIn('--property=InvocationID', status)
        self.assertIn('--property=ExecMainStartTimestampMonotonic', status)
        self.assertIn('--property=ExecMainExitTimestampMonotonic', status)
        self.assertIn('--property=User --property=ExecStart', status)
        self.assertIn('PAYLOAD_SHA256=$(sudo sha256sum', status)
        self.assertIn('0:0:700', status)
        self.assertIn('argv[]=/bin/bash $SCRIPT_PATH ;', status)
        self.assertIn('sha256sum "$OUTPUT_PATH"', status)
        with self.assertRaises(DistributedMeasurementError):
            initializer_execution_identity(
                {'id': 'abc123;reboot'},
                APPLICATION_PRIVATE_IP,
            )
        other_user_identity = initializer_execution_identity(
            {
                'id': 'abc123def456',
                'resources': {'ssh_user': 'other-benchmark'},
            },
            APPLICATION_PRIVATE_IP,
        )
        self.assertNotEqual(
            identity['payload_sha256'],
            other_user_identity['payload_sha256'],
        )
        with self.assertRaises(DistributedMeasurementError):
            initializer_execution_identity(
                {
                    'id': 'abc123def456',
                    'resources': {'ssh_user': 'benchmark; reboot'},
                },
                APPLICATION_PRIVATE_IP,
            )

    def test_database_parser_requires_empty_then_exact_initialized_counts(self):
        empty = (
            'DISTRIBUTED_DSB_DATABASE_DATA users=0 social_graph_users=0 '
            'followers=0 followees=0 posts=0 timeline_users=0 '
            'timeline_posts=0\n'
        )
        initialized = (
            'DISTRIBUTED_DSB_DATABASE_DATA users=962 social_graph_users=962 '
            'followers=37624 followees=37624 posts=9424 '
            'timeline_users=908 timeline_posts=9424\n'
        )

        self.assertEqual(
            parse_dataset_database_attestation(empty, initialized=False),
            {
                'users': 0,
                'social_graph_users': 0,
                'followers': 0,
                'followees': 0,
                'posts': 0,
                'timeline_users': 0,
                'timeline_posts': 0,
            },
        )
        self.assertEqual(
            parse_dataset_database_attestation(initialized, initialized=True),
            {
                'users': 962,
                'social_graph_users': 962,
                'followers': 37624,
                'followees': 37624,
                'posts': 9424,
                'timeline_users': 908,
                'timeline_posts': 9424,
            },
        )
        for output, initialized_state in (
            (empty.replace('users=0', 'users=1', 1), False),
            (initialized.replace('timeline_posts=9424', 'timeline_posts=9423'), True),
        ):
            with self.subTest(initialized=initialized_state):
                with self.assertRaises(DistributedMeasurementError):
                    parse_dataset_database_attestation(
                        output,
                        initialized=initialized_state,
                    )

        command = dataset_database_attestation_command()
        for component in (
            'user-mongodb',
            'social-graph-mongodb',
            'post-storage-mongodb',
            'user-timeline-mongodb',
        ):
            self.assertIn(f'pod_for {component}', command)
        self.assertIn('test "${#PODS[@]}" -eq 1', command)
        self.assertIn('exec pod/$USER_POD', command)

    def test_frontend_probe_is_an_exact_empty_timeline_get(self):
        command = non_mutating_frontend_probe_command(APPLICATION_PRIVATE_IP)

        self.assertIn(APPLICATION_PRIVATE_IP, command)
        self.assertIn('8080', command)
        self.assertIn(
            '/wrk2-api/user-timeline/read?user_id=0&start=0&stop=1',
            command,
        )
        self.assertIn('DISTRIBUTED_DSB_FRONTEND_READY', command)
        self.assertIn('test "$STATUS" = 200', command)
        self.assertIn('json.load', command)
        self.assertIn('assert value == {}', command)
        self.assertNotIn('assert value == []', command)
        lowered = command.casefold()
        for forbidden in ('post', '--data', 'register', 'follow', 'compose'):
            self.assertNotIn(forbidden, lowered)
        for invalid in ('10.240.1.0/24', 'not-an-ip'):
            with self.subTest(invalid=invalid):
                with self.assertRaises(DistributedMeasurementError):
                    non_mutating_frontend_probe_command(invalid)

    def test_wrk2_parser_rejects_concatenated_invocations(self):
        output = wrk2_output(10)
        self.assertEqual(
            parse_wrk2_execution_output(output, label='Measurement')[
                'total_requests'
            ],
            200,
        )
        with self.assertRaisesRegex(
            DistributedMeasurementError,
            'exactly one metrics marker',
        ):
            parse_wrk2_execution_output(
                output + output,
                label='Measurement',
            )

    def test_durable_initializer_status_binds_terminal_output_hash(self):
        job = {'id': 'abc123def456', 'resources': {'ssh_user': 'benchmark'}}
        expected = initializer_execution_identity(
            job,
            APPLICATION_PRIVATE_IP,
        )
        parsed = parse_durable_initializer_status(
            initializer_status_output(job),
            expected,
        )
        self.assertEqual(parsed['state'], 'succeeded')
        self.assertIn('DISTRIBUTED_DSB_DATASET_READY', parsed['output'])
        self.assertEqual(parsed['invocation_id'], 'c' * 32)
        self.assertEqual(parsed['start_monotonic_usec'], 100)
        self.assertEqual(parsed['exit_monotonic_usec'], 200)
        self.assertRegex(parsed['output_sha256'], r'^[0-9a-f]{64}$')

        with self.assertRaisesRegex(
            DistributedMeasurementError,
            'does not match',
        ):
            parse_durable_initializer_status(
                initializer_status_output(job) + 'tampered',
                expected,
            )

        pending = parse_durable_initializer_status(
            initializer_status_output(job, state='pending'),
            expected,
        )
        self.assertEqual(pending['state'], 'pending')
        self.assertEqual(pending['output'], '')
        self.assertEqual(pending['payload_sha256'], expected['payload_sha256'])
        self.assertEqual(pending['service_user'], 'root')
        self.assertTrue(pending['exec_start_match'])

        valid_status = initializer_status_output(job)
        mismatches = (
            (
                f'payload_sha256={expected["payload_sha256"]}',
                'payload_sha256=' + ('0' * 64),
            ),
            (
                f'payload_sha256={expected["payload_sha256"]}',
                'payload_sha256=none',
            ),
            ('service_user=root', 'service_user=benchmark'),
            ('exec_start_match=yes', 'exec_start_match=no'),
            ('run_user=benchmark', 'run_user=other-benchmark'),
        )
        for old, new in mismatches:
            with self.subTest(initializer_identity_mismatch=new):
                with self.assertRaises(DistributedMeasurementError):
                    parse_durable_initializer_status(
                        valid_status.replace(old, new, 1),
                        expected,
                    )

        missing = parse_durable_initializer_status(
            initializer_status_output(job, state='not_found'),
            expected,
        )
        self.assertEqual(missing['state'], 'not_found')
        self.assertEqual(missing['payload_sha256'], 'none')
        installed_but_not_dispatched = parse_durable_initializer_status(
            initializer_status_output(job, state='not_found').replace(
                'payload_sha256=none',
                f'payload_sha256={expected["payload_sha256"]}',
                1,
            ),
            expected,
        )
        self.assertEqual(installed_but_not_dispatched['state'], 'not_found')
        self.assertEqual(
            installed_but_not_dispatched['payload_sha256'],
            expected['payload_sha256'],
        )
        with self.assertRaisesRegex(
            DistributedMeasurementError,
            'payload does not match',
        ):
            parse_durable_initializer_status(
                initializer_status_output(job, state='not_found').replace(
                    'payload_sha256=none',
                    'payload_sha256=' + ('0' * 64),
                    1,
                ),
                expected,
            )

        success_marker = initializer_status_output(job).splitlines()[0]
        missing_transcript = success_marker[:success_marker.index('output_bytes=')]
        missing_transcript += 'output_bytes=0 output_sha256=none\n'
        with self.assertRaisesRegex(
            DistributedMeasurementError,
            'missing its finalized transcript',
        ):
            parse_durable_initializer_status(
                missing_transcript,
                expected,
            )

        missing_invocation = initializer_status_output(job).replace(
            f'invocation_id={"c" * 32}',
            'invocation_id=none',
            1,
        )
        with self.assertRaisesRegex(
            DistributedMeasurementError,
            'invocation identity',
        ):
            parse_durable_initializer_status(
                missing_invocation,
                expected,
            )


class DistributedMeasurementOrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.job, self.image_lock = ready_workload_job()
        self.options = measurement_options()
        self.job['plan'] = {
            'provider': 'azure',
            'region': 'eastus2',
            'shape': 'Standard_D8ps_v6',
            'ocpus': 8,
            'memory_gb': 32,
            'benchmarks': ['deathstarbench'],
            'llm_benchmarks': [],
            'deathstarbench': self.options.model_dump(),
        }

    def run_measurement(self, executor, **kwargs):
        execution_identity = {
            'schema_version': 1,
            'workload_revision': DISTRIBUTED_WORKLOAD_REVISION,
            'bundle_fingerprint': self.job['resources'][
                'deathstarbench_workload_state_v1'
            ]['rendered_manifest_sha256'],
            'pods': [
                {
                    'component': component,
                    'name': f'{component}-test',
                    'uid': f'pod-{index:02d}',
                    'node_name': 'dsb-application',
                    'restart_count': 0,
                    'image_id': (
                        f'registry.example/{component}@sha256:' + '1' * 64
                    ),
                }
                for index, component in enumerate(sorted(EXPECTED_COMPONENTS))
            ],
        }
        with mock.patch(
            'app.deathstarbench_distributed_measurement.'
            'parse_workload_execution_attestation',
            return_value=execution_identity,
        ):
            return run_azure_distributed_social_network_measurement(
                self.job,
                self.image_lock,
                self.options,
                execute=executor,
                **kwargs,
            )

    def test_success_uses_only_load_generator_in_order_and_persists_one_result(self):
        executor = MeasurementExecutor()
        persisted = []
        ticks = iter((100.0, 110.0))

        result = self.run_measurement(
            executor,
            persist=lambda current: persisted.append(copy.deepcopy(
                current['resources'][EXECUTION_JOURNAL_KEY]
            )),
            monotonic=lambda: next(ticks),
            timestamp=lambda: '2026-09-18T12:00:00+00:00',
        )

        self.assertEqual(
            executor.stages,
            [
                'load_generator',
                'frontend_probe',
                'database_attestation',
                'initialization',
                'initialization_status',
                'database_attestation',
                'workload_snapshot',
                'warmup',
                'measurement',
                'workload_snapshot',
            ],
        )
        for stage, _, kwargs in executor.calls:
            expected_host = (
                'azure_dsb_control_public_ip'
                if stage in {'database_attestation', 'workload_snapshot'}
                else LOAD_GENERATOR_HOST_KEY
            )
            self.assertEqual(kwargs['host_key'], expected_host)
            self.assertNotIn('jump_host_key', kwargs)
        mutating_calls = {
            stage: kwargs
            for stage, _, kwargs in executor.calls
            if stage in {'initialization', 'warmup', 'measurement'}
        }
        self.assertEqual(
            set(mutating_calls),
            {'initialization', 'warmup', 'measurement'},
        )
        self.assertTrue(all(
            kwargs['transport_attempts'] == 1
            for kwargs in mutating_calls.values()
        ))
        self.assertNotIn('include_stderr', mutating_calls['initialization'])
        self.assertTrue(mutating_calls['warmup']['include_stderr'])
        self.assertTrue(mutating_calls['measurement']['include_stderr'])
        self.assertEqual(
            [entry['state'] for entry in persisted],
            [
                'preparing_load_generator',
                'load_generator_ready',
                'initialization_started',
                'dataset_ready',
                'warmup_started',
                'warmup_complete',
                'measurement_started',
                'measurement_complete',
            ],
        )
        self.assertEqual(self.job['results'], [result])
        self.assertEqual(result['id'], 'deathstarbench')
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['metrics']['p95_ms'], 2.0)
        self.assertEqual(result['metadata']['runtime_revision'], DISTRIBUTED_RUNTIME_REVISION)
        self.assertEqual(result['metadata']['workload_revision'], DISTRIBUTED_WORKLOAD_REVISION)
        self.assertEqual(result['metadata']['dataset_revision'], DISTRIBUTED_DATASET_REVISION)
        self.assertEqual(result['metadata']['load_driver_revision'], DISTRIBUTED_LOAD_DRIVER_REVISION)
        self.assertEqual(result['metadata']['measurement_revision'], DISTRIBUTED_MEASUREMENT_REVISION)
        self.assertEqual(
            result['metadata']['initializer_sha256'],
            'ff504a03311c1d6da4e5ba031b49ec824541fa78b898cda029a60e364edcadaf',
        )
        self.assertRegex(
            result['metadata']['dataset_nodes_sha256'], r'^[0-9a-f]{64}$'
        )
        self.assertRegex(
            result['metadata']['dataset_edges_sha256'], r'^[0-9a-f]{64}$'
        )
        self.assertRegex(
            result['metadata']['load_script_sha256'], r'^[0-9a-f]{64}$'
        )
        evidence = result['metadata']['measurement_evidence']
        expected_loadgen = {
            'architecture': 'x86_64',
            'upstream_revision': UPSTREAM_REVISION,
            'load_driver_revision': DISTRIBUTED_LOAD_DRIVER_REVISION,
            'wrk_binary_sha256': WRK_BINARY_SHA256,
            'compiler_version_sha256': COMPILER_VERSION_SHA256,
        }
        self.assertEqual(evidence['schema_version'], 1)
        self.assertEqual(
            evidence['load_generator_attestation'],
            expected_loadgen,
        )
        self.assertEqual(
            evidence['frontend_attestation'],
            {
                'target_role': 'application',
                'target_address_sha256': (
                    'sha256:'
                    '04f6feb4a5fae54adf1a005a5b18740357a28bbe69774b0e64e6019f1325aa0d'
                ),
                'port': 8080,
                'route': (
                    '/wrk2-api/user-timeline/read?user_id=0&start=0&stop=1'
                ),
            },
        )
        self.assertNotIn(
            'target_private_ip', evidence['frontend_attestation']
        )
        self.assertTrue(evidence['pod_identity_unchanged'])
        self.assertEqual(
            evidence['pre_workload_execution'],
            evidence['post_workload_execution'],
        )
        self.assertRegex(
            evidence['metrics_sha256'], r'^sha256:[0-9a-f]{64}$'
        )
        self.assertRegex(
            evidence['raw_benchmark_sha256'], r'^sha256:[0-9a-f]{64}$'
        )
        self.assertRegex(
            result['metadata']['measurement_evidence_sha256'],
            r'^sha256:[0-9a-f]{64}$',
        )
        self.assertEqual(
            result['metadata']['load_generator_wrk_binary_sha256'],
            WRK_BINARY_SHA256,
        )
        self.assertEqual(
            result['metadata']['load_generator_compiler_version_sha256'],
            COMPILER_VERSION_SHA256,
        )
        self.assertIn('export max_user_index=962', result['command'])
        self.assertIn('timeout --signal=TERM --kill-after=30s', result['command'])
        artifact = comparison.build_results_artifact({
            **self.job,
            'status': 'reporting',
            'benchmark_status': 'complete',
        })
        self.assertFalse(artifact['contract_unknown'])
        self.assertFalse(
            artifact['results'][0]['comparison']['contract_unknown']
        )
        artifact_evidence = artifact['results'][0]['metadata'][
            'measurement_evidence'
        ]
        self.assertEqual(artifact_evidence, evidence)
        self.assertEqual(
            artifact_evidence['frontend_attestation'],
            evidence['frontend_attestation'],
        )
        self.assertEqual(
            artifact_evidence['raw_benchmark_sha256'],
            evidence['raw_benchmark_sha256'],
        )
        journal = self.job['resources'][EXECUTION_JOURNAL_KEY]
        self.assertEqual(journal['state'], 'measurement_complete')
        self.assertEqual(journal['load_generator_attestation'], expected_loadgen)
        self.assertEqual(journal['settings'], {
            'workload': 'social_network',
            'warmup_seconds': 5,
            'duration_seconds': 10,
            'threads': 2,
            'connections': 4,
            'request_rate': 20,
        })
        dataset = journal['dataset_attestation']
        self.assertEqual(
            dataset['initializer_execution']['unit_name'],
            'benchmark-deathstarbench-init-abc123def456.service',
        )
        self.assertRegex(
            dataset['initializer_execution']['payload_sha256'],
            r'^[0-9a-f]{64}$',
        )
        self.assertRegex(
            dataset['initializer_execution']['initializer_log_sha256'],
            r'^[0-9a-f]{64}$',
        )
        self.assertEqual(
            dataset['initializer_execution']['invocation_id'],
            'c' * 32,
        )
        self.assertGreater(
            dataset['initializer_execution']['initializer_log_bytes'],
            0,
        )
        self.assertEqual(
            dataset['pre_initialization_database'],
            {
                'users': 0,
                'social_graph_users': 0,
                'followers': 0,
                'followees': 0,
                'posts': 0,
                'timeline_users': 0,
                'timeline_posts': 0,
            },
        )
        self.assertEqual(dataset['initializer']['users'], 962)
        self.assertEqual(dataset['initializer']['follows'], 37624)
        self.assertEqual(dataset['initializer']['posts'], 9424)
        self.assertEqual(
            dataset['database'],
            {
                'users': 962,
                'social_graph_users': 962,
                'followers': 37624,
                'followees': 37624,
                'posts': 9424,
                'timeline_users': 908,
                'timeline_posts': 9424,
            },
        )
        measurement_attestation = journal['measurement_attestation']
        self.assertEqual(
            measurement_attestation['pre_workload_execution'],
            measurement_attestation['post_workload_execution'],
        )
        self.assertEqual(measurement_attestation['result_id'], 'deathstarbench')
        self.assertRegex(
            measurement_attestation['metrics_sha256'],
            r'^sha256:[0-9a-f]{64}$',
        )
        self.assertRegex(
            measurement_attestation['output_sha256'],
            r'^sha256:[0-9a-f]{64}$',
        )
        self.assertEqual(result['duration_seconds'], 10.0)

        # Azure cleanup removes the execution journal as ownership state.  The
        # completed result artifact must retain the safe evidence independently.
        self.job['resources'].pop(EXECUTION_JOURNAL_KEY)
        post_cleanup_artifact = comparison.build_results_artifact({
            **self.job,
            'status': 'destroyed',
            'benchmark_status': 'complete',
        })
        self.assertEqual(
            post_cleanup_artifact['results'][0]['metadata'][
                'measurement_evidence'
            ],
            evidence,
        )

    def test_qualification_checkpoint_follows_durable_persistence_exactly(self):
        executor = MeasurementExecutor()
        persisted = []
        checkpoints = []

        def persist(current):
            persisted.append(copy.deepcopy(
                current['resources'][EXECUTION_JOURNAL_KEY]
            ))

        def checkpoint(current, state):
            checkpoints.append((
                state,
                persisted[-1]['state'],
                current['resources'][EXECUTION_JOURNAL_KEY]['state'],
            ))

        self.run_measurement(
            executor,
            persist=persist,
            qualification_checkpoint=checkpoint,
        )

        expected = [
            'load_generator_ready',
            'initialization_started',
            'warmup_started',
            'measurement_started',
        ]
        self.assertEqual([entry[0] for entry in checkpoints], expected)
        self.assertEqual(
            checkpoints,
            [(state, state, state) for state in expected],
        )

        self.job, self.image_lock = ready_workload_job()
        self.options = measurement_options(warmup_seconds=0)
        no_warmup_checkpoints = []
        self.run_measurement(
            MeasurementExecutor(),
            persist=lambda _job: None,
            qualification_checkpoint=(
                lambda _job, state: no_warmup_checkpoints.append(state)
            ),
        )
        self.assertEqual(
            no_warmup_checkpoints,
            [
                'load_generator_ready',
                'initialization_started',
                'measurement_started',
            ],
        )

    def test_qualification_checkpoint_must_be_callable_before_mutation(self):
        executor = MeasurementExecutor()
        original_resources = copy.deepcopy(self.job['resources'])

        with self.assertRaisesRegex(
            DistributedMeasurementError,
            'qualification checkpoint must be callable',
        ):
            self.run_measurement(
                executor,
                qualification_checkpoint='not-callable',
            )

        self.assertEqual(executor.calls, [])
        self.assertEqual(self.job['resources'], original_resources)

    def test_qualification_checkpoint_requires_persistence_before_mutation(self):
        executor = MeasurementExecutor()
        original_resources = copy.deepcopy(self.job['resources'])

        with self.assertRaisesRegex(
            DistributedMeasurementError,
            'requires a callable persistence hook',
        ):
            self.run_measurement(
                executor,
                qualification_checkpoint=lambda _job, _state: None,
            )

        self.assertEqual(executor.calls, [])
        self.assertEqual(self.job['resources'], original_resources)

    def test_checkpoint_exceptions_propagate_before_next_phase_without_replay(self):
        boundaries = (
            ('load_generator_ready', 'frontend_probe'),
            ('initialization_started', 'initialization'),
            ('warmup_started', 'warmup'),
            ('measurement_started', 'measurement'),
        )
        for failed_state, blocked_stage in boundaries:
            with self.subTest(state=failed_state):
                self.job, self.image_lock = ready_workload_job()
                self.options = measurement_options()
                executor = MeasurementExecutor()
                persisted = []
                checkpoints = []

                def persist(current):
                    persisted.append(copy.deepcopy(
                        current['resources'][EXECUTION_JOURNAL_KEY]
                    ))

                def checkpoint(_job, state):
                    checkpoints.append(state)
                    if state == failed_state:
                        raise RuntimeError(f'injected {state} checkpoint failure')

                with self.assertRaisesRegex(
                    RuntimeError,
                    f'injected {failed_state} checkpoint failure',
                ):
                    self.run_measurement(
                        executor,
                        persist=persist,
                        qualification_checkpoint=checkpoint,
                    )

                self.assertEqual(checkpoints.count(failed_state), 1)
                self.assertEqual(persisted[-1]['state'], failed_state)
                self.assertEqual(
                    self.job['resources'][EXECUTION_JOURNAL_KEY]['state'],
                    failed_state,
                )
                self.assertNotIn(blocked_stage, executor.stages)

                if failed_state != 'load_generator_ready':
                    retry = MeasurementExecutor()
                    with self.assertRaises(DistributedMeasurementError):
                        self.run_measurement(retry)
                    self.assertEqual(retry.calls, [])

    def test_internal_main_wrapper_forwards_qualification_checkpoint(self):
        from app import main

        checkpoint = mock.Mock()
        expected = object()
        with mock.patch.object(
            main,
            'run_azure_distributed_social_network_measurement',
            return_value=expected,
        ) as measurement:
            result = (
                main.run_azure_distributed_deathstarbench_candidate_measurement(
                    self.job,
                    self.job['plan'],
                    self.image_lock,
                    qualification_checkpoint=checkpoint,
                )
            )

        self.assertIs(result, expected)
        measurement.assert_called_once_with(
            self.job,
            self.image_lock,
            self.job['plan']['deathstarbench'],
            execute=main.ssh,
            emit=main.event,
            persist=main.persist_job_state,
            qualification_checkpoint=checkpoint,
        )

    def test_ambiguous_dispatch_without_a_unit_is_poisoned_and_never_replayed(self):
        missing = initializer_status_output(self.job, state='not_found')
        first = MeasurementExecutor(
            fail_on='initialization',
            initializer_statuses=[missing, missing],
        )
        persisted = []
        clock = iter((0.0, 1.0, 61.0))

        with self.assertRaisesRegex(
            DistributedMeasurementError,
            'did not produce its deterministic systemd unit',
        ):
            self.run_measurement(
                first,
                persist=lambda current: persisted.append(copy.deepcopy(
                    current['resources'][EXECUTION_JOURNAL_KEY]
                )),
                initializer_clock=lambda: next(clock),
                initializer_sleep=lambda _seconds: None,
            )

        journal = copy.deepcopy(self.job['resources'][EXECUTION_JOURNAL_KEY])
        self.assertEqual(journal['state'], 'initialization_started')
        self.assertEqual(persisted[-1]['state'], 'initialization_started')
        self.assertEqual(
            journal['dataset_attestation']['pre_initialization_database'],
            {
                'users': 0,
                'social_graph_users': 0,
                'followers': 0,
                'followees': 0,
                'posts': 0,
                'timeline_users': 0,
                'timeline_posts': 0,
            },
        )
        self.assertNotIn('initializer', journal['dataset_attestation'])
        self.assertNotIn('database', journal['dataset_attestation'])
        self.assertRegex(
            journal['dataset_attestation']['initializer_execution'][
                'payload_sha256'
            ],
            r'^[0-9a-f]{64}$',
        )
        self.assertEqual(self.job['results'], [])
        self.assertEqual(first.stages.count('initialization'), 1)
        self.assertEqual(first.stages.count('initialization_status'), 2)

        retry = MeasurementExecutor()
        with self.assertRaises(DistributedMeasurementError):
            self.run_measurement(retry)
        self.assertEqual(retry.calls, [])
        self.assertEqual(self.job['resources'][EXECUTION_JOURNAL_KEY], journal)

    def test_ambiguous_dispatch_rejects_stale_same_name_unit_identity(self):
        identity = initializer_execution_identity(
            self.job,
            APPLICATION_PRIVATE_IP,
        )
        stale_success = initializer_status_output(self.job).replace(
            f'payload_sha256={identity["payload_sha256"]}',
            'payload_sha256=' + ('0' * 64),
            1,
        )
        executor = MeasurementExecutor(
            fail_on='initialization',
            initializer_statuses=[stale_success],
        )

        with self.assertRaisesRegex(
            DistributedMeasurementError,
            'payload does not match',
        ):
            self.run_measurement(executor)

        self.assertEqual(executor.stages.count('initialization'), 1)
        self.assertEqual(executor.stages.count('initialization_status'), 1)
        self.assertEqual(
            self.job['resources'][EXECUTION_JOURNAL_KEY]['state'],
            'initialization_started',
        )
        self.assertEqual(self.job['results'], [])
        self.assertEqual(executor.stages.count('database_attestation'), 1)
        for forbidden_stage in (
            'workload_snapshot',
            'warmup',
            'measurement',
        ):
            self.assertNotIn(forbidden_stage, executor.stages)

    def test_malformed_dispatch_attestation_is_not_treated_as_transport_loss(self):
        class MalformedDispatchExecutor(MeasurementExecutor):
            def __call__(self, job, command, **kwargs):
                if self.stage(command) == 'initialization':
                    self.calls.append(('initialization', command, kwargs))
                    return (
                        'DISTRIBUTED_DSB_INITIALIZER_DISPATCHED '
                        'unit=benchmark-deathstarbench-init-deadbeef0000.service '
                        'run_user=benchmark payload_sha256=' + ('0' * 64) + '\n'
                    )
                return super().__call__(job, command, **kwargs)

        executor = MalformedDispatchExecutor()

        with self.assertRaisesRegex(
            DistributedMeasurementError,
            'dispatch attestation',
        ):
            self.run_measurement(executor)

        self.assertEqual(executor.stages.count('initialization'), 1)
        self.assertEqual(executor.stages.count('initialization_status'), 0)
        self.assertEqual(
            self.job['resources'][EXECUTION_JOURNAL_KEY]['state'],
            'initialization_started',
        )
        self.assertEqual(self.job['results'], [])

    def test_ambiguous_dispatch_and_status_disconnect_reconcile_without_replay(self):
        pending = initializer_status_output(self.job, state='pending')
        executor = MeasurementExecutor(
            fail_on='initialization',
            initializer_statuses=[
                RuntimeError('transient status transport failure'),
                pending,
            ],
        )
        clock = iter((0.0, 1.0, 2.0, 3.0))
        sleeps = []

        result = self.run_measurement(
            executor,
            initializer_clock=lambda: next(clock),
            initializer_sleep=sleeps.append,
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(executor.stages.count('initialization'), 1)
        self.assertEqual(executor.stages.count('initialization_status'), 3)
        self.assertEqual(sleeps, [5.0, 5.0])
        dispatch_command = next(
            command
            for stage, command, _ in executor.calls
            if stage == 'initialization'
        )
        status_commands = [
            command
            for stage, command, _ in executor.calls
            if stage == 'initialization_status'
        ]
        self.assertIn('systemd-run --quiet --no-block', dispatch_command)
        self.assertTrue(all('systemd-run' not in command for command in status_commands))
        self.assertTrue(all('systemctl show' in command for command in status_commands))

    def test_warmup_and_measurement_failures_poison_retry(self):
        for failed_stage, expected_state in (
            ('warmup', 'warmup_started'),
            ('measurement', 'measurement_started'),
        ):
            with self.subTest(failed_stage=failed_stage):
                self.job, self.image_lock = ready_workload_job()
                first = MeasurementExecutor(fail_on=failed_stage)

                with self.assertRaisesRegex(RuntimeError, f'{failed_stage} failure'):
                    self.run_measurement(first)

                journal = copy.deepcopy(
                    self.job['resources'][EXECUTION_JOURNAL_KEY]
                )
                self.assertEqual(journal['state'], expected_state)
                self.assertIsNotNone(journal['dataset_attestation'])
                self.assertEqual(self.job['results'], [])

                retry = MeasurementExecutor()
                with self.assertRaises(DistributedMeasurementError):
                    self.run_measurement(retry)
                self.assertEqual(retry.calls, [])
                self.assertEqual(
                    self.job['resources'][EXECUTION_JOURNAL_KEY],
                    journal,
                )

    def test_safe_retry_rejects_identity_and_settings_drift_before_ssh(self):
        first = MeasurementExecutor(fail_on='frontend_probe')
        with self.assertRaisesRegex(RuntimeError, 'frontend_probe failure'):
            self.run_measurement(first)
        self.assertEqual(
            self.job['resources'][EXECUTION_JOURNAL_KEY]['state'],
            'load_generator_ready',
        )

        original_options = self.options
        self.options = measurement_options(request_rate=22)
        changed_settings = MeasurementExecutor()
        with self.assertRaises(DistributedMeasurementError):
            self.run_measurement(changed_settings)
        self.assertEqual(changed_settings.calls, [])

        self.options = original_options
        self.job['resources'][EXECUTION_JOURNAL_KEY][
            'load_generator_private_ip'
        ] = '10.240.2.99'
        changed_identity = MeasurementExecutor()
        with self.assertRaises(DistributedMeasurementError):
            self.run_measurement(changed_identity)
        self.assertEqual(changed_identity.calls, [])

    def test_completed_result_is_idempotent_and_raw_output_is_bounded(self):
        executor = MeasurementExecutor(
            measurement_prefix='x' * (MAX_RESULT_OUTPUT_CHARS * 2),
        )
        result = self.run_measurement(executor)
        completed_journal = copy.deepcopy(
            self.job['resources'][EXECUTION_JOURNAL_KEY]
        )

        self.assertLessEqual(len(result['output']), MAX_RESULT_OUTPUT_CHARS)
        self.assertIn('OCI_DSB_METRICS', result['output'])
        self.assertEqual(len(self.job['results']), 1)

        retry = MeasurementExecutor()
        with self.assertRaises(DistributedMeasurementError):
            self.run_measurement(retry)
        self.assertEqual(retry.calls, [])
        self.assertEqual(self.job['results'], [result])
        self.assertEqual(
            self.job['resources'][EXECUTION_JOURNAL_KEY],
            completed_journal,
        )


if __name__ == '__main__':
    unittest.main()
