"""Synthetic run-state tests; these fixtures are never publication evidence."""

from concurrent.futures import ThreadPoolExecutor
import copy
from dataclasses import replace
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import yaml

from app import deathstarbench_candidate_runtime as runtime
from app.deathstarbench_candidate_storage import ATTESTATION_MARKER, DATABASE_ROOT
from tests.test_deathstarbench_candidate_workload import published_candidate_images


UUID = '12345678-1234-1234-1234-123456789abc'
TOPOLOGY = 'sha256:' + '1' * 64


class SyntheticBinding:
    def __init__(self, workload_id):
        self.workload_id = workload_id
        self.runtime_manifest = published_candidate_images(workload_id)
        self.load_driver_image = f'registry.example/{workload_id}/driver@sha256:' + '2' * 64

    def metadata(self):
        return {
            'workload_id': self.workload_id,
            'runtime_manifest': copy.deepcopy(self.runtime_manifest),
            'load_driver_image': self.load_driver_image,
            'candidate_only': True, 'released': False,
            'fixture_only_not_publication_evidence': True,
        }


def make_plan(workload='hotel_reservation', **overrides):
    options = {
        'run_id': '123456abcdef', 'provider': 'azure',
        'topology_fingerprint': TOPOLOGY,
        'application_architecture': 'aarch64',
        'load_generator_ip': '10.240.2.10', 'filesystem_uuid': UUID,
    }
    options.update(overrides)
    if runtime.CandidateArtifactBinding is SyntheticBinding:
        return runtime.create_candidate_runtime_plan(SyntheticBinding(workload), **options)
    with patch.object(runtime, 'CandidateArtifactBinding', SyntheticBinding):
        return runtime.create_candidate_runtime_plan(SyntheticBinding(workload), **options)


def storage_output(plan):
    contract = plan.storage
    return (
        f'{ATTESTATION_MARKER} schema=candidate-storage-v1 candidate_only=1 released=0 runtime_qualified=0 '
        f'run={contract.run_id} workload={contract.workload_id} namespace={contract.namespace} uuid={UUID} root={DATABASE_ROOT} '
        f'contract_sha256={contract.sha256} inventory_sha256={contract.inventory_sha256} databases=8 '
        'claim_bytes=85899345920 total_bytes=107374182400 available_bytes=100000000000 '
        'filesystem=xfs selinux=container_file_t\n'
    )


class CandidateRuntimeTests(unittest.TestCase):
    def test_real_adoption_api_connects_to_render_storage_and_saved_snapshot(self):
        from tests.test_deathstarbench_candidate_artifacts import CandidateArtifactAdoptionTests

        helper = CandidateArtifactAdoptionTests()
        self.addCleanup(helper.doCleanups)
        for workload in ('hotel_reservation', 'media_microservices'):
            # Registry bytes are synthetic; the complete adoption validators
            # execute. Only independent source-preparation validators are stubbed.
            binding = helper.adopt(helper.fixture(workload))
            for architecture in ('x86_64', 'aarch64'):
                plan = runtime.create_candidate_runtime_plan(
                    binding, run_id='123456abcdef', provider='azure', topology_fingerprint=TOPOLOGY,
                    application_architecture=architecture, load_generator_ip='10.240.2.10', filesystem_uuid=UUID,
                )
                self.assertEqual(plan.snapshot()['artifacts']['fingerprint'], binding.fingerprint)
                self.assertEqual(plan.snapshot()['artifacts']['load_driver']['image'], binding.load_driver_image)
                self.assertEqual(plan.storage.workload_id, workload)
                self.assertEqual(runtime.validate_candidate_runtime_snapshot(plan.snapshot(), plan), plan.snapshot())
                restored = runtime.record_candidate_storage_attestation(
                    plan, runtime.new_candidate_storage_journal(plan), storage_output(plan),
                )
                self.assertEqual(restored['state'], 'storage_prepared')

    def test_explicit_identity_storage_and_all_gates_closed(self):
        for workload in ('hotel_reservation', 'media_microservices'):
            plan = make_plan(workload)
            saved = plan.snapshot()
            self.assertEqual(saved['profile']['workload_id'], workload)
            self.assertEqual(saved['storage']['namespace'], plan.bundle.namespace)
            self.assertEqual(saved['storage']['run_id'], plan.run_id)
            self.assertEqual(saved['storage']['total_claim_bytes'], 80 * 1024**3)
            self.assertEqual(saved['storage']['database_count'], 8)
            self.assertTrue(saved['candidate_only'])
            self.assertFalse(saved['released'])
            self.assertTrue(all(value is False for value in saved['qualification'].values()))
            self.assertNotIn('reed98', json.dumps(saved).lower())
            self.assertEqual(runtime.validate_candidate_runtime_snapshot(saved, plan), saved)

    def test_every_run_boundary_changes_identity_and_crossed_snapshots_fail(self):
        original = make_plan()
        cases = (
            {'run_id': 'abcdef123456'}, {'provider': 'aws'},
            {'topology_fingerprint': 'sha256:' + '3' * 64},
            {'application_architecture': 'x86_64'},
            {'load_generator_ip': '10.240.2.11'},
            {'filesystem_uuid': 'abcdef12-1234-1234-1234-123456789abc'},
        )
        for options in cases:
            with self.subTest(options=options):
                changed = make_plan(**options)
                self.assertNotEqual(original.fingerprint, changed.fingerprint)
                with self.assertRaises(runtime.CandidateRuntimeError):
                    runtime.validate_candidate_runtime_snapshot(original.snapshot(), changed)
        with self.assertRaises(runtime.CandidateRuntimeError):
            runtime.validate_candidate_runtime_snapshot(original.snapshot(), make_plan('media_microservices'))

    def test_invalid_run_provider_topology_or_bypassed_artifacts_fail(self):
        for options in (
            {'run_id': '000000000000'}, {'run_id': '123456ABCDEF'},
            {'run_id': '123456abcdef\n'}, {'provider': 'AWS'},
            {'provider': None}, {'topology_fingerprint': 'sha256:' + '0' * 64},
            {'topology_fingerprint': TOPOLOGY + '\n'}, {'topology_fingerprint': None},
        ):
            with self.subTest(options=options):
                with self.assertRaises(runtime.CandidateRuntimeError):
                    make_plan(**options)
        with self.assertRaises(runtime.CandidateRuntimeError):
            runtime.create_candidate_runtime_plan(
                {}, run_id='123456abcdef', provider='azure', topology_fingerprint=TOPOLOGY,
                application_architecture='aarch64', load_generator_ip='10.240.2.10', filesystem_uuid=UUID,
            )

    def test_snapshot_is_independent_of_returned_mutable_copies(self):
        plan = make_plan()
        saved = plan.snapshot()
        saved['storage']['database_names'].clear()
        saved['qualification']['public_release'] = True
        self.assertEqual(len(plan.snapshot()['storage']['database_names']), 8)
        self.assertIs(plan.snapshot()['qualification']['public_release'], False)

    def test_dataclass_replacement_cannot_cross_plan_snapshot_relationships(self):
        plan = make_plan()
        other_run = make_plan(run_id='abcdef123456')
        other_workload = make_plan('media_microservices')
        for fields in (
            {'storage': other_run.storage}, {'bundle': other_workload.bundle},
            {'artifacts': other_workload.artifacts}, {'run_id': other_run.run_id},
            {'provider': 'aws'}, {'fingerprint': 'sha256:' + 'f' * 64},
            {'snapshot_json': other_run.snapshot_json},
            {'bundle': None}, {'storage': None},
        ):
            with self.subTest(fields=fields), patch.object(runtime, 'CandidateArtifactBinding', SyntheticBinding):
                with self.assertRaises(ValueError):
                    replace(plan, **fields)
        placement = dict(plan.bundle.expected_component_placement)
        mutable_bundle = replace(plan.bundle, expected_component_placement=placement)
        with patch.object(runtime, 'CandidateArtifactBinding', SyntheticBinding):
            detached = replace(plan, bundle=mutable_bundle)
        placement.clear()
        self.assertEqual(detached.bundle.expected_component_placement, plan.bundle.expected_component_placement)

    def test_nested_snapshot_drift_extra_fields_and_bool_integer_aliases_fail(self):
        plan = make_plan()
        for path, replacement in (
            (('schema_version',), True), (('candidate_only',), 1),
            (('qualification', 'measurement'), True),
            (('artifacts', 'load_driver_image'), 'registry.example/driver:latest'),
            (('profile', 'dataset_revision'), 'reed98'),
            (('bundle', 'rendered_manifest_sha256'), 'sha256:' + '4' * 64),
            (('storage', 'database_count'), 6),
        ):
            saved = plan.snapshot()
            target = saved
            for field in path[:-1]:
                target = target[field]
            target[path[-1]] = replacement
            with self.subTest(path=path):
                with self.assertRaises(runtime.CandidateRuntimeError):
                    runtime.validate_candidate_runtime_snapshot(saved, plan)
        extra = plan.snapshot()
        extra['ready'] = True
        for saved in (extra, [], None):
            with self.assertRaises(runtime.CandidateRuntimeError):
                runtime.validate_candidate_runtime_snapshot(saved, plan)

    def test_parallel_workloads_remain_isolated(self):
        workloads = ['hotel_reservation', 'media_microservices'] * 12
        with patch.object(runtime, 'CandidateArtifactBinding', SyntheticBinding), ThreadPoolExecutor(max_workers=4) as pool:
            plans = list(pool.map(make_plan, workloads))
        for workload, plan in zip(workloads, plans):
            self.assertEqual(plan.snapshot()['profile']['workload_id'], workload)
            self.assertEqual(plan.snapshot()['storage']['workload_id'], workload)
            self.assertEqual(plan.artifacts.workload_id, workload)
        self.assertNotEqual(plans[0].fingerprint, plans[1].fingerprint)
        self.assertEqual(plans[0].fingerprint, plans[2].fingerprint)

    def test_strict_snapshot_reader(self):
        plan = make_plan()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'snapshot.json'
            path.write_text(plan.snapshot_json)
            self.assertEqual(runtime.read_candidate_runtime_snapshot(path), plan.snapshot())
            alias = Path(directory) / 'alias'
            alias.symlink_to(path)
            with self.assertRaises(runtime.CandidateRuntimeError):
                runtime.read_candidate_runtime_snapshot(alias)
            fifo = Path(directory) / 'fifo'
            os.mkfifo(fifo)
            with self.assertRaises(runtime.CandidateRuntimeError):
                runtime.read_candidate_runtime_snapshot(fifo)
            for payload in (
                b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}', b'{"a":1e999}', b'[]', b'\xff',
                '{}'.encode('utf-16'), b' ' * (1024 * 1024 + 1),
                b'{"nested":' + b'[' * 2000 + b'0' + b']' * 2000 + b'}',
            ):
                path.write_bytes(payload)
                with self.subTest(payload=payload[:25]):
                    with self.assertRaises(runtime.CandidateRuntimeError):
                        runtime.read_candidate_runtime_snapshot(path)


class CandidateStorageJournalTests(unittest.TestCase):
    def test_record_validated_storage_without_runtime_or_dataset_claim(self):
        plan = make_plan()
        journal = runtime.new_candidate_storage_journal(plan)
        recorded = runtime.record_candidate_storage_attestation(plan, journal, storage_output(plan))
        self.assertEqual(journal['state'], 'preflight_only')
        self.assertEqual(recorded['state'], 'storage_prepared')
        self.assertTrue(all(value is False for value in recorded['qualification'].values()))
        self.assertEqual(runtime.validate_candidate_storage_journal(recorded, plan), recorded)
        with self.assertRaises(runtime.CandidateRuntimeError):
            runtime.record_candidate_storage_attestation(plan, recorded, storage_output(plan))

    def test_crossed_journals_and_guest_evidence_fail(self):
        plan = make_plan()
        other = make_plan('media_microservices')
        journal = runtime.new_candidate_storage_journal(plan)
        with self.assertRaises(runtime.CandidateRuntimeError):
            runtime.validate_candidate_storage_journal(journal, other)
        with self.assertRaises(ValueError):
            runtime.record_candidate_storage_attestation(plan, journal, storage_output(other))

    def test_impossible_state_missing_evidence_and_promoted_gates_fail(self):
        plan = make_plan()
        initial = runtime.new_candidate_storage_journal(plan)
        recorded = runtime.record_candidate_storage_attestation(plan, initial, storage_output(plan))
        cases = []
        changed = copy.deepcopy(recorded)
        changed['storage_attestation']['available_bytes'] = 1
        cases.append(changed)
        changed = copy.deepcopy(recorded)
        changed['storage_attestation']['database_count'] = True
        cases.append(changed)
        changed = copy.deepcopy(recorded)
        changed['storage_attestation'] = None
        cases.append(changed)
        changed = copy.deepcopy(recorded)
        changed['state'] = 'preflight_only'
        cases.append(changed)
        for field, value in (('state', 'ready'), ('schema_version', True), ('released', True)):
            changed = copy.deepcopy(initial)
            changed[field] = value
            cases.append(changed)
        changed = copy.deepcopy(initial)
        changed['qualification']['dataset'] = True
        cases.append(changed)
        for changed in cases:
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    runtime.validate_candidate_storage_journal(changed, plan)


class CandidateRuntimeCliTests(unittest.TestCase):
    def test_manual_preflight_has_read_only_permissions_and_exact_evidence_anchors(self):
        root = Path(__file__).resolve().parents[1]
        workflow = yaml.safe_load((root / '.github/workflows/deathstarbench-candidate-runtime.yml').read_text())
        self.assertEqual(workflow['permissions'], {'contents': 'read', 'actions': 'read', 'packages': 'read'})
        # PyYAML's YAML 1.1 interpretation maps the literal `on` key to True.
        trigger = workflow.get('on', workflow.get(True))
        self.assertEqual(set(trigger), {'workflow_call'})
        retained = json.loads((root / 'docs/qualification/deathstarbench-hotel-media-candidate-images-v1.json').read_text())
        for item in workflow['jobs']['adopt']['strategy']['matrix']['include']:
            reviewed = retained['workloads'][item['workload']]
            self.assertEqual(item['publication_run'], reviewed['workflow_run_id'])
            self.assertEqual(item['artifact'], reviewed['artifact']['name'])
            self.assertEqual(item['lock_sha256'], reviewed['artifact']['candidate_lock_file_sha256'])
            self.assertEqual(item['publication_sha256'], reviewed['artifact']['publication_file_sha256'])
        text = (root / '.github/workflows/deathstarbench-candidate-runtime.yml').read_text()
        for disallowed in ('packages: write', 'id-token:', 'docker login', 'docker push', 'push: true', 'aws ', 'gcloud ', 'az ', 'oci ', 'set -x', 'ssl._create_unverified_context'):
            self.assertNotIn(disallowed, text)
        self.assertIn('synthetic-offline-fixtures-not-cloud-attestation', text)
        self.assertIn('adopt_candidate_artifacts(', text)
        caller = yaml.safe_load((root / '.github/workflows/deathstarbench-social-images.yml').read_text())
        self.assertEqual(caller['jobs']['preflight-hotel-media']['permissions'], workflow['permissions'])

    def test_cli_only_reads_and_returns_an_explicit_snapshot(self):
        from scripts import preflight_deathstarbench_candidate_runtime as cli

        args = cli.argument_parser().parse_args([
            '--workload', 'hotel_reservation', '--image-lock', '/tmp/lock',
            '--publication', '/tmp/publication', '--workload-preparation', '/tmp/prepared',
            '--driver-preparation', '/tmp/driver', '--run-id', '123456abcdef',
            '--provider', 'azure', '--topology-fingerprint', TOPOLOGY,
            '--application-architecture', 'aarch64', '--load-generator-ip', '10.240.2.10',
            '--database-filesystem-uuid', UUID,
        ])
        with patch.object(cli, 'adopt_candidate_artifacts', return_value=SyntheticBinding('hotel_reservation')) as adoption, \
                patch.object(runtime, 'CandidateArtifactBinding', SyntheticBinding):
            snapshot = cli.preflight(args, inspector=object(), blob_inspector=object())
            self.assertEqual(snapshot, make_plan().snapshot())
            self.assertEqual(adoption.call_args.kwargs['workload_preparation'], Path('/tmp/prepared'))
            with tempfile.TemporaryDirectory() as directory:
                args.verify_snapshot = Path(directory) / 'snapshot.json'
                args.verify_snapshot.write_text(json.dumps(snapshot))
                self.assertEqual(cli.preflight(args), snapshot)
                args.run_id = 'abcdef123456'
                with self.assertRaises(runtime.CandidateRuntimeError):
                    cli.preflight(args)
