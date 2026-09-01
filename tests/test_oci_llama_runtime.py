import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app import main
from app.models import BenchmarkPlan


def response(data):
    return SimpleNamespace(data=data)


class OciLlamaRuntimeTests(unittest.TestCase):
    def test_provision_persists_the_exact_runner_image_before_launch(self):
        plan = BenchmarkPlan(
            region='us-ashburn-1',
            availability_domain='AD-1',
            shape='VM.Standard.E5.Flex',
            ocpus=2,
            memory_gb=16,
            ssh_private_key='private',
            ssh_public_key='ssh-ed25519 AAAATEST',
            storage={'additional_volume': False},
            benchmarks=[],
            llm_benchmarks=['llama_bench'],
        )
        job = {
            'id': 'abc123def456',
            '_public_key': 'ssh-ed25519 AAAATEST',
            'events': [],
            'resources': {},
            'results': [],
        }
        compute = MagicMock()
        network = MagicMock()
        storage = MagicMock()
        identity = MagicMock()
        vcn = SimpleNamespace(id='vcn-1', default_dhcp_options_id='dhcp-1')
        network.create_vcn.return_value = response(vcn)
        network.get_vcn.return_value = response(vcn)
        network.create_internet_gateway.return_value = response(
            SimpleNamespace(id='igw-1')
        )
        network.create_nat_gateway.return_value = response(
            SimpleNamespace(id='nat-1')
        )
        network.create_route_table.return_value = response(
            SimpleNamespace(id='rt-1')
        )
        network.create_security_list.return_value = response(
            SimpleNamespace(id='sl-1')
        )
        network.create_subnet.return_value = response(
            SimpleNamespace(id='subnet-1')
        )
        image = SimpleNamespace(
            id='ocid1.image.oc1..runner',
            display_name='Oracle-Linux-9.6-2026.08.01-0',
        )
        compute.list_shapes.side_effect = RuntimeError('stop after image')

        with (
            patch.object(
                main,
                'clients',
                return_value=(
                    {'tenancy': 'ocid1.tenancy.oc1..test'},
                    compute,
                    network,
                    storage,
                    identity,
                ),
            ),
            patch.object(main, 'latest_oracle_linux_image', return_value=image),
            patch.object(main.oci, 'wait_until'),
            self.assertRaisesRegex(RuntimeError, 'stop after image'),
        ):
            main.provision_oci(job, plan)

        self.assertEqual(job['resources']['image_id'], image.id)
        self.assertEqual(job['resources']['image_name'], image.display_name)

    def test_runner_image_metadata_alone_is_not_a_recoverable_resource(self):
        job = {
            'plan': {'provider': 'oci'},
            'resources': {
                'image_id': 'ocid1.image.oc1..runner',
                'image_name': 'Oracle-Linux-9.6-2026.08.01-0',
            },
        }

        self.assertFalse(main.has_recoverable_resources(job))


if __name__ == '__main__':
    unittest.main()
