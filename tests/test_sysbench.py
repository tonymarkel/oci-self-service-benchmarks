import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError

from app import main
from app.models import BenchmarkPlan, SysbenchOptions


def plan(**overrides):
    values = {
        'region': 'us-ashburn-1',
        'shape': 'VM.Standard.E5.Flex',
        'ocpus': 8,
        'memory_gb': 32,
        'ssh_private_key': 'private',
        'ssh_public_key': 'public',
        'benchmarks': ['sysbench'],
    }
    values.update(overrides)
    return BenchmarkPlan(**values)


class SysbenchPlanTests(unittest.TestCase):
    def test_defaults_to_the_cpu_workload(self):
        self.assertEqual(SysbenchOptions().workloads, ['cpu'])
        self.assertEqual(plan().sysbench.workloads, ['cpu'])

    def test_workloads_are_typed_unique_and_required_when_selected(self):
        with self.assertRaises(ValidationError):
            plan(sysbench={'workloads': ['unsupported']})

        with self.assertRaisesRegex(ValidationError, 'must be unique'):
            plan(sysbench={'workloads': ['cpu', 'cpu']})

        with self.assertRaisesRegex(ValidationError, 'at least one Sysbench'):
            plan(sysbench={'workloads': []})

        unselected = plan(
            benchmarks=['stream'],
            sysbench={'workloads': []},
        )
        self.assertEqual(unselected.sysbench.workloads, [])

    def test_fileio_requires_the_additional_data_volume(self):
        with self.assertRaisesRegex(ValidationError, 'additional /data volume'):
            plan(
                sysbench={'workloads': ['fileio']},
                storage={'additional_volume': False},
            )

    def test_legacy_top_level_ids_are_canonicalized(self):
        migrated = plan(
            benchmarks=[
                'stream',
                'sysbench_fileio',
                'sysbench_cpu',
                'phoronix',
            ],
        )

        self.assertEqual(
            migrated.benchmarks,
            ['stream', 'sysbench', 'phoronix'],
        )
        self.assertEqual(migrated.sysbench.workloads, ['cpu', 'fileio'])

    def test_mixed_legacy_and_canonical_selections_merge_once(self):
        migrated = plan(
            benchmarks=['sysbench', 'sysbench_memory', 'fio'],
            sysbench={'workloads': ['fileio']},
        )

        self.assertEqual(migrated.benchmarks, ['sysbench', 'fio'])
        self.assertEqual(migrated.sysbench.workloads, ['memory', 'fileio'])


class SysbenchCatalogAndExpansionTests(unittest.TestCase):
    def test_catalog_exposes_one_parent_and_three_nested_workloads(self):
        payload = main.catalog()
        top_level_ids = [item['id'] for item in payload['benchmarks']]
        top_level_names = [item['name'] for item in payload['benchmarks']]

        self.assertNotIn('baseline', top_level_ids)
        self.assertNotIn('Cloud baseline suite', top_level_names)
        self.assertEqual(top_level_ids.count('sysbench'), 1)
        self.assertFalse(
            {'sysbench_cpu', 'sysbench_memory', 'sysbench_fileio'}
            & set(top_level_ids)
        )
        self.assertEqual(
            [item['id'] for item in payload['sysbench_workloads']],
            ['cpu', 'memory', 'fileio'],
        )
        fileio = next(
            item
            for item in payload['sysbench_workloads']
            if item['id'] == 'fileio'
        )
        self.assertTrue(fileio['requires_data'])

    def test_expansion_selects_only_requested_sysbench_children(self):
        selected_plan = plan(sysbench={'workloads': ['memory', 'fileio']})

        self.assertEqual(
            main.selected_sysbench_workloads(selected_plan),
            ('memory', 'fileio'),
        )
        self.assertEqual(
            main.expanded_benchmark_ids(selected_plan),
            {'sysbench', 'sysbench_memory', 'sysbench_fileio'},
        )

    def test_legacy_baseline_migrates_to_explicit_components(self):
        baseline = plan(benchmarks=['baseline'])
        extended = plan(
            benchmarks=['baseline', 'sysbench'],
            sysbench={'workloads': ['fileio']},
        )

        self.assertEqual(
            baseline.benchmarks,
            ['sysbench', 'stream', 'fio'],
        )
        self.assertEqual(
            main.selected_sysbench_workloads(baseline),
            ('cpu', 'memory'),
        )
        self.assertEqual(
            main.expanded_benchmark_ids(baseline),
            {
                'sysbench',
                'stream',
                'fio',
                'sysbench_cpu',
                'sysbench_memory',
            },
        )
        self.assertEqual(
            extended.benchmarks,
            ['sysbench', 'stream', 'fio'],
        )
        self.assertEqual(
            main.selected_sysbench_workloads(extended),
            ('cpu', 'memory', 'fileio'),
        )

    def test_legacy_baseline_merges_with_legacy_child_selections(self):
        migrated = plan(
            benchmarks=[
                'baseline',
                'sysbench_fileio',
                'iperf_tcp',
                'iperf_udp',
                'phoronix',
            ],
        )

        self.assertEqual(
            migrated.benchmarks,
            ['sysbench', 'stream', 'fio', 'iperf3', 'phoronix'],
        )
        self.assertEqual(
            migrated.sysbench.workloads,
            ['cpu', 'memory', 'fileio'],
        )
        self.assertEqual(migrated.iperf3.protocols, ['tcp', 'udp'])


class SysbenchCompatibilityAndCommandTests(unittest.TestCase):
    def test_legacy_baseline_has_a_friendly_history_label(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / 'legacy-baseline'
            run.mkdir()
            (run / 'plan.json').write_text(json.dumps({
                'region': 'us-ashburn-1',
                'shape': 'VM.Standard.E5.Flex',
                'benchmarks': ['baseline'],
            }))
            report = run / 'report.html'
            report.write_text('<p>Status: <strong>completed</strong></p>')

            summary = main.report_summary(report)

        self.assertEqual(
            summary['benchmarks'],
            ['Cloud baseline suite (legacy)'],
        )

    def test_report_plan_migrates_legacy_baseline_without_rewriting_it(self):
        saved = {
            'region': 'us-ashburn-1',
            'shape': 'VM.Standard.E5.Flex',
            'benchmarks': ['baseline', 'phoronix'],
            'storage': {'additional_volume': True},
        }
        with tempfile.TemporaryDirectory() as directory:
            runs = Path(directory)
            job_id = 'legacy-baseline'
            run = runs / job_id
            run.mkdir()
            path = run / 'plan.json'
            original = json.dumps(saved, indent=2)
            path.write_text(original)

            with patch.object(main, 'RUNS', runs):
                restored = main.report_plan(job_id)

            self.assertEqual(
                restored['benchmarks'],
                ['sysbench', 'stream', 'fio', 'phoronix'],
            )
            self.assertEqual(
                restored['sysbench']['workloads'],
                ['cpu', 'memory'],
            )
            self.assertEqual(path.read_text(), original)

    def test_report_plan_migrates_legacy_selection_without_rewriting_it(self):
        saved = {
            'region': 'us-ashburn-1',
            'shape': 'VM.Standard.E5.Flex',
            'benchmarks': ['stream', 'sysbench_memory', 'sysbench_fileio'],
            'storage': {'additional_volume': True},
        }
        with tempfile.TemporaryDirectory() as directory:
            runs = Path(directory)
            job_id = 'legacy-sysbench'
            run = runs / job_id
            run.mkdir()
            path = run / 'plan.json'
            original = json.dumps(saved, indent=2)
            path.write_text(original)

            with patch.object(main, 'RUNS', runs):
                restored = main.report_plan(job_id)

            self.assertEqual(restored['benchmarks'], ['stream', 'sysbench'])
            self.assertEqual(
                restored['sysbench']['workloads'],
                ['memory', 'fileio'],
            )
            self.assertEqual(path.read_text(), original)

    def test_fileio_command_cleans_up_from_an_exit_trap(self):
        selected_plan = plan(sysbench={'workloads': ['fileio']})
        job = {'events': [], 'results': [], 'resources': {}}
        target_metadata = {
            'storage_target_contract': 'v1',
            'storage_target_kind': 'provisioned_data_volume',
            'storage_target_mount_point': '/data',
        }

        with (
            patch.object(main, 'wait_for_guest_readiness'),
            patch.object(main, 'install_benchmark_tools'),
            patch.object(main, 'mount_data_volume'),
            patch.object(
                main,
                'prepare_storage_benchmark_target',
                return_value=('/data', target_metadata),
            ) as prepare_target,
            patch.object(
                main,
                'ssh',
                side_effect=['x86_64\n', '16\n'],
            ),
            patch.object(main, 'execute_benchmark') as execute,
        ):
            main.run_benchmarks(job, selected_plan)

        prepare_target.assert_called_once()
        execute.assert_called_once()
        benchmark_id, name, command = execute.call_args.args[1:4]
        self.assertEqual(benchmark_id, 'sysbench_fileio')
        self.assertEqual(name, 'Sysbench — File I/O')
        self.assertIn(
            "trap 'sysbench fileio --file-total-size=4G cleanup ",
            command,
        )
        self.assertIn("|| true' EXIT", command)
        self.assertIn('--file-test-mode=rndrw run', command)
        self.assertEqual(command, main.amazon_linux.sysbench_fileio_command())


if __name__ == '__main__':
    unittest.main()
