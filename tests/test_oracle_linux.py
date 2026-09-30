import subprocess
import unittest

from app.guests import oracle_linux


VOLUME_ID = (
    'ocid1.volume.oc1.iad.'
    'abcdefghijklmnopqrstuvwxyz234567abcdefghijklmnopqrstuvwxyz234567'
)
FILESYSTEM_UUID = '12345678-1234-1234-1234-123456789abc'


class OracleLinuxGuestContractTests(unittest.TestCase):
    @staticmethod
    def assert_valid_bash(command):
        subprocess.run(
            ['bash', '-n'],
            input=command,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_readiness_is_explicit_bounded_and_architecture_aware(self):
        command = oracle_linux.readiness_command(
            'amd64',
            (
                'yum.us-ashburn-1.oci.oraclecloud.com',
                'github.com',
            ),
        )

        self.assertIn('if [ "$ID" != ol ]', command)
        self.assertIn('Oracle Linux 9', command)
        self.assertIn('EXPECTED_ARCH=x86_64', command)
        self.assertIn('for attempt in $(seq 1 24)', command)
        self.assertIn('if [ "$attempt" -lt 24 ]; then sleep 5; fi', command)
        self.assertIn('nmcli general reload dns-full', command)
        self.assertIn(oracle_linux.OCI_VCN_RESOLVER, command)
        self.assertIn('nmcli -g IP4.DNS device show', command)
        self.assertIn('timeout 5 getent ahostsv4 "$host"', command)
        self.assertIn('sudo timeout 60 dnf -q', command)
        self.assertIn('--disablerepo=ol9_ksplice', command)
        self.assertIn('command -v curl', command)
        self.assertNotIn('mirrors.rockylinux.org', command)
        self.assert_valid_bash(command)

        self.assertEqual(
            oracle_linux.normalize_architecture('ARM64'),
            'aarch64',
        )
        with self.assertRaisesRegex(ValueError, 'architecture is unsupported'):
            oracle_linux.normalize_architecture('riscv64')
        with self.assertRaisesRegex(ValueError, 'hostname'):
            oracle_linux.readiness_command('x86_64', ['github.com; id'])

    def test_oci_dns_prepare_is_bounded_and_rejects_untrusted_hosts(self):
        command = oracle_linux.oci_dns_prepare_command(
            ('yum.oracle.com', 'github.com')
        )

        self.assertIn('nmcli general reload dns-full', command)
        self.assertIn('for attempt in $(seq 1 24)', command)
        self.assertIn('OCI DHCP did not advertise', command)
        self.assert_valid_bash(command)

        with self.assertRaisesRegex(ValueError, 'hostname'):
            oracle_linux.oci_dns_prepare_command(('github.com; id',))

    def test_dnf_install_is_bounded_and_rejects_shell_input(self):
        command = oracle_linux.dnf_install_command({'gcc', 'zlib-devel'})

        self.assertIn('for attempt in $(seq 1 3)', command)
        self.assertIn('sudo timeout 480 dnf -y', command)
        self.assertIn('--disablerepo=ol9_ksplice', command)
        self.assertIn('install gcc zlib-devel', command)
        self.assert_valid_bash(command)

        with self.assertRaisesRegex(ValueError, 'package'):
            oracle_linux.dnf_install_command({'gcc; id'})
        with self.assertRaisesRegex(ValueError, 'At least one package'):
            oracle_linux.dnf_install_command(())

    def test_deathstarbench_loadgen_steps_include_zlib_and_verified_epel(self):
        steps = oracle_linux.deathstarbench_loadgen_install_steps()

        self.assertEqual(
            [step.name for step in steps],
            [
                'DeathStarBench load-generator base packages',
                'Oracle Linux developer EPEL repository packages',
                'Oracle Linux developer EPEL repository',
                'DeathStarBench load-generator EPEL packages',
            ],
        )
        base, repository_packages, repository, epel = (
            step.command for step in steps
        )
        for package in (
            'gcc',
            'git',
            'make',
            'openssl-devel',
            'python3',
            'time',
            'zlib-devel',
        ):
            self.assertIn(package, base)
        self.assertIn('dnf-plugins-core', repository_packages)
        self.assertIn('oracle-epel-release-el9', repository_packages)
        self.assertIn(
            'config-manager --enable ol9_developer_EPEL',
            repository,
        )
        self.assertIn('$1 == "ol9_developer_EPEL"', repository)
        self.assertIn('luarocks', epel)
        self.assertIn('python3-aiohttp', epel)
        for step in steps:
            self.assert_valid_bash(step.command)

    def test_database_mount_uses_only_fixed_oracle_device(self):
        command = oracle_linux.oci_deathstarbench_database_volume_mount_command(
            VOLUME_ID
        )

        self.assertIn(f'VOLUME_ID={VOLUME_ID}', command)
        self.assertIn(
            f'DEVICE_LINK={oracle_linux.DATABASE_DEVICE_LINK}',
            command,
        )
        self.assertNotIn('/dev/sdb', command)
        self.assertNotIn('/dev/oracleoci/oraclevd*', command)
        self.assertIn('[ -L "$DEVICE_LINK" ]', command)
        self.assertIn('lsblk -dnro TYPE "$DEVICE"', command)
        self.assertIn('lsblk -srnpo NAME "$ROOT_DEVICE"', command)
        self.assertIn('lsblk -nrpo NAME "$DEVICE"', command)
        self.assertIn('wipefs -n --noheadings --output TYPE', command)
        self.assertIn('sudo mkfs.xfs "$DEVICE"', command)
        self.assertNotIn('mkfs.xfs -f', command)
        self.assertIn(
            'UUID=%s /var/lib/deathstarbench/database xfs '
            'discard,nofail 0 2 # cloud-benchmark-dsb-database',
            command,
        )
        self.assertIn(
            'OCI_DSB_DATABASE_VOLUME volume_id=%s device_link=%s uuid=%s '
            'mount_point=/var/lib/deathstarbench/database filesystem=xfs',
            command,
        )
        self.assert_valid_bash(command)

    def test_database_attestation_is_exact_and_never_mutates_storage(self):
        command = (
            oracle_linux.oci_deathstarbench_database_volume_attestation_command(
                VOLUME_ID,
                FILESYSTEM_UUID,
            )
        )

        self.assertIn(
            f'DEVICE_LINK={oracle_linux.DATABASE_DEVICE_LINK}',
            command,
        )
        self.assertIn(f'EXPECTED_UUID={FILESYSTEM_UUID}', command)
        self.assertIn('grep -Fxq "$EXPECTED_FSTAB" /etc/fstab', command)
        self.assertIn('lsblk -srnpo NAME "$ROOT_DEVICE"', command)
        self.assertNotIn('/dev/sdb', command)
        self.assertNotIn('mkfs', command)
        self.assertNotIn('wipefs', command)
        self.assertNotIn('sudo mount', command)
        self.assertNotIn('sudo install', command)
        self.assertIn(
            'OCI_DSB_DATABASE_VOLUME volume_id=%s device_link=%s uuid=%s',
            command,
        )
        self.assert_valid_bash(command)

    def test_workload_storage_uses_oci_attestation_marker(self):
        command = (
            oracle_linux.oci_deathstarbench_database_workload_storage_command(
                FILESYSTEM_UUID
            )
        )

        self.assertIn('OCI_DSB_WORKLOAD_STORAGE', command)
        self.assertNotIn('AZURE_DSB_WORKLOAD_STORAGE', command)
        self.assertIn('container_file_t', command)
        self.assert_valid_bash(command)

    def test_database_commands_reject_untrusted_identity(self):
        for volume_id in (
            'ocid1.volume.oc1.iad.bad;id',
            'ocid1.instance.oc1.iad.abcdefghijklmnop',
            '../ocid1.volume.oc1.iad.abcdefghijklmnop',
            '',
        ):
            with self.subTest(volume_id=volume_id):
                with self.assertRaisesRegex(ValueError, 'block-volume OCID'):
                    oracle_linux.oci_deathstarbench_database_volume_mount_command(
                        volume_id
                    )
        with self.assertRaisesRegex(ValueError, 'filesystem UUID'):
            oracle_linux.oci_deathstarbench_database_volume_attestation_command(
                VOLUME_ID,
                'not-a-uuid',
            )


if __name__ == '__main__':
    unittest.main()
