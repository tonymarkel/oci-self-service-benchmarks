"""Internal orchestration for the unreleased distributed K3s candidate.

The cluster and Social Network workload phases are deliberately separate and
neither is dispatched by the normal API/UI path.  The workload phase accepts
only a complete digest lock, applies the checked-in manifests over SSH stdin,
and persists an independent retry journal.  It still stops before dataset
initialization or measurement and therefore cannot create benchmark results.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
import re
from typing import Any

from .deathstarbench_contract import (
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DISTRIBUTED_IMAGE_SET_REVISION,
    DISTRIBUTED_RUNTIME_REVISION,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    DISTRIBUTED_WORKLOAD_REVISION,
    K3S_RUNTIME_ID,
    K3S_RUNTIME_JOURNAL_KEY,
)
from .deathstarbench_k3s_workload import (
    NAMESPACE as WORKLOAD_NAMESPACE,
    RenderedSocialNetworkBundle,
    WorkloadBundleError,
    parse_workload_attestation,
    render_social_network_bundle,
    workload_apply_command,
    workload_readiness_command,
)
from .deathstarbench_topology import DeathStarBenchTopologyManifest
from .guests.rocky_linux import (
    azure_deathstarbench_database_volume_attestation_command,
    azure_deathstarbench_database_volume_mount_command,
    azure_deathstarbench_database_workload_storage_command,
)
from .k3s_runtime import (
    ExpectedK3sNode,
    agent_join_command,
    agent_readiness_command,
    agent_token_install_command,
    artifact_install_command,
    cluster_nodes_readiness_command,
    control_tokens_initialize_command,
    host_preflight_command,
    normalized_architecture,
    rocky_host_prepare_command,
    secure_agent_token_ca_sha256,
    secure_agent_token_read_command,
    server_ca_sha256_read_command,
    server_install_command,
    server_readiness_command,
    system_services_pin_command,
    system_services_readiness_command,
    validated_token,
)
from .resource_inventory import (
    ResourceInventoryError,
    load_role_node_inventory,
)


TOPOLOGY_MANIFEST_KEY = 'deathstarbench_topology_manifest'
TOPOLOGY_FINGERPRINT_KEY = 'deathstarbench_topology_fingerprint'
RUNTIME_JOURNAL_KEY = K3S_RUNTIME_JOURNAL_KEY
RUNTIME_JOURNAL_SCHEMA_VERSION = 1
WORKLOAD_JOURNAL_KEY = DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY
WORKLOAD_JOURNAL_SCHEMA_VERSION = 1
WORKLOAD_ID = 'social_network'

_CLUSTER_KEYS = ('control', 'database', 'cache', 'application')
_AGENT_KEYS = ('database', 'cache', 'application')
_EXPECTED_ROLES = {
    'control': 'control',
    'database': 'database',
    'cache': 'cache',
    'application': 'application',
    'load-generator': 'load_generator',
}
_AZURE_PRIVATE_ADDRESSES = {
    'control': '10.240.1.10',
    'database': '10.240.1.11',
    'cache': '10.240.1.12',
    'application': '10.240.1.13',
    'load-generator': '10.240.2.10',
}
_AZURE_SUPPORT_SHAPES = {
    'control': 'Standard_D2as_v7',
    'database': 'Standard_D4as_v7',
    'cache': 'Standard_D2as_v7',
    'load-generator': 'Standard_D2as_v7',
}
_SSH_ROUTES = {
    'control': ('azure_dsb_control_public_ip', None),
    'database': (
        'azure_dsb_database_private_ip',
        'azure_dsb_control_public_ip',
    ),
    'cache': (
        'azure_dsb_cache_private_ip',
        'azure_dsb_control_public_ip',
    ),
    'application': (
        'azure_dsb_application_private_ip',
        'azure_dsb_control_public_ip',
    ),
    'load-generator': ('azure_dsb_load_generator_public_ip', None),
}
_STATES = frozenset({
    'preparing_hosts',
    'hosts_prepared',
    'database_storage_ready',
    'server_ready',
    'joining_agents',
    'agents_ready',
    'cluster_ready',
})
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_PREFIXED_SHA256_RE = re.compile(r'^sha256:[0-9a-f]{64}$')
_VOLUME_UUID_RE = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
    r'[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)
_DATABASE_DEVICE_LINKS = frozenset({
    '/dev/disk/azure/data/by-lun/0',
    '/dev/disk/azure/scsi1/lun0',
})
_DATABASE_WORKLOAD_ROOT = '/var/lib/deathstarbench/database/mongodb'
_WORKLOAD_READY_STATE = 'workload_ready'


class DistributedRuntimeError(ValueError):
    """Raised before remote work when candidate state is unsafe or stale."""


@dataclass(frozen=True, slots=True)
class RuntimeHost:
    """One exact SSH route and K3s identity from persisted inventory."""

    key: str
    role: str
    node_name: str
    private_ip: str
    architecture: str
    host_key: str
    jump_host_key: str | None

    @property
    def expected_node(self) -> ExpectedK3sNode:
        return ExpectedK3sNode(
            self.node_name,
            self.role,
            self.private_ip,
            self.architecture,
        )


@dataclass(frozen=True, slots=True)
class AzureK3sCandidatePlan:
    """Validated internal plan for one Azure candidate cluster."""

    topology_fingerprint: str
    hosts: tuple[RuntimeHost, ...]
    load_generator: RuntimeHost
    load_generator_private_ip: str

    def host(self, key: str) -> RuntimeHost:
        try:
            return next(host for host in self.hosts if host.key == key)
        except StopIteration:
            raise DistributedRuntimeError(
                f'Distributed runtime plan is missing host {key}.'
            ) from None

    @property
    def expected_nodes(self) -> tuple[ExpectedK3sNode, ...]:
        return tuple(host.expected_node for host in self.hosts)


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise DistributedRuntimeError(f'{label} is missing or invalid.')
    return value


def _same_azure_resource_id(left: Any, right: Any) -> bool:
    """Compare Azure IDs using the provider's case/slash normalization."""

    return (
        bool(str(left or '').strip().strip('/'))
        and '/' + str(left or '').strip().strip('/').casefold()
        == '/' + str(right or '').strip().strip('/').casefold()
    )


def _azure_id_has_named_type(
    resource_id: str,
    resource_type: str,
    resource_name: str,
) -> bool:
    parts = str(resource_id).strip().strip('/').split('/')
    return (
        len(parts) >= 2
        and parts[-2].casefold() == resource_type.casefold()
        and parts[-1].casefold() == resource_name.casefold()
    )


def _node_address_key(node_key: str, kind: str) -> str:
    normalized = node_key.replace('-', '_')
    return f'azure_dsb_{normalized}_{kind}_ip'


def azure_k3s_candidate_plan(
    resources: Mapping[str, Any],
) -> AzureK3sCandidatePlan:
    """Reload the strict persisted Azure contract before any SSH operation."""

    if not isinstance(resources, Mapping):
        raise DistributedRuntimeError('Job resources must be an object.')
    if resources.get('provider') != 'azure':
        raise DistributedRuntimeError(
            'The distributed K3s candidate requires Azure resource state.'
        )
    if resources.get('azure_distributed_candidate') is not True:
        raise DistributedRuntimeError(
            'The Azure distributed-candidate marker is missing.'
        )
    try:
        manifest = DeathStarBenchTopologyManifest.from_dict(
            resources[TOPOLOGY_MANIFEST_KEY]
        )
        inventory = load_role_node_inventory(resources)
    except (KeyError, ResourceInventoryError, ValueError) as exc:
        raise DistributedRuntimeError(
            f'The distributed topology/inventory contract is invalid: {exc}'
        ) from exc
    if (
        manifest.topology_id != DISTRIBUTED_TIERED_TOPOLOGY_ID
        or manifest.runtime_id != K3S_RUNTIME_ID
        or manifest.runtime_revision != DISTRIBUTED_RUNTIME_REVISION
    ):
        raise DistributedRuntimeError(
            'The persisted topology is not the exact distributed K3s candidate.'
        )
    fingerprint = resources.get(TOPOLOGY_FINGERPRINT_KEY)
    if (
        fingerprint != manifest.fingerprint
        or inventory.topology_fingerprint != manifest.fingerprint
    ):
        raise DistributedRuntimeError(
            'The topology and role inventory fingerprints do not match.'
        )
    if inventory.provider != 'azure':
        raise DistributedRuntimeError(
            'The role inventory does not belong to Azure.'
        )
    if {node.key for node in inventory.nodes} != set(_EXPECTED_ROLES):
        raise DistributedRuntimeError(
            'The role inventory must contain the exact five candidate nodes.'
        )
    if resources.get('ssh_user') != 'benchmark':
        raise DistributedRuntimeError(
            'The Azure candidate requires the exact benchmark SSH user.'
        )

    hosts: list[RuntimeHost] = []
    load_generator_private_ip = None
    load_generator_host = None
    manifest_nodes = {node.key: node for node in manifest.nodes}
    for node_key in manifest.creation_order:
        node = inventory.node(node_key)
        if node is None or node.role != _EXPECTED_ROLES[node_key]:
            raise DistributedRuntimeError(
                f'The {node_key} role inventory is missing or conflicting.'
            )
        if node.lifecycle_status != 'running':
            raise DistributedRuntimeError(
                f'The {node_key} role is not in running lifecycle state.'
            )
        _required_text(node.provider_resource_id, f'{node_key} resource ID')
        node_name = _required_text(
            node.provider_resource_name,
            f'{node_key} resource name',
        )
        prefix = f'azure_dsb_{node_key.replace("-", "_")}_instance'
        if resources.get(f'{prefix}_name') != node_name:
            raise DistributedRuntimeError(
                f'The {node_key} resource name conflicts with inventory.'
            )
        node_id = _required_text(
            node.provider_resource_id,
            f'{node_key} resource ID',
        )
        if (
            not _azure_id_has_named_type(
                node_id,
                'virtualMachines',
                node_name,
            )
            or not _same_azure_resource_id(
                resources.get(f'{prefix}_id'), node_id
            )
            or not _same_azure_resource_id(
                resources.get(f'{prefix}_expected_id'), node_id
            )
        ):
            raise DistributedRuntimeError(
                f'The {node_key} resource ID conflicts with inventory.'
            )
        if len(node.private_addresses) != 1:
            raise DistributedRuntimeError(
                f'The {node_key} role must have one private address.'
            )
        private_ip = node.private_addresses[0]
        if private_ip != _AZURE_PRIVATE_ADDRESSES[node_key]:
            raise DistributedRuntimeError(
                f'The {node_key} private address violates the Azure contract.'
            )
        flat_private = resources.get(_node_address_key(node_key, 'private'))
        if flat_private != private_ip:
            raise DistributedRuntimeError(
                f'The {node_key} private address conflicts with inventory.'
            )
        if node_key == 'load-generator':
            load_generator_private_ip = private_ip
        architecture = normalized_architecture(
            _required_text(node.architecture, f'{node_key} architecture')
        )
        manifest_node = manifest_nodes[node_key]
        expected_architecture = normalized_architecture(
            _required_text(
                manifest_node.architecture,
                f'{node_key} manifest architecture',
            )
        )
        if architecture != expected_architecture:
            raise DistributedRuntimeError(
                f'The {node_key} architecture conflicts with the manifest.'
            )
        if node_key in _CLUSTER_KEYS:
            try:
                ExpectedK3sNode(
                    node_name,
                    node.role,
                    private_ip,
                    architecture,
                )
            except ValueError as exc:
                raise DistributedRuntimeError(
                    f'The {node_key} K3s node identity is invalid: {exc}'
                ) from exc
        shape = _required_text(node.shape, f'{node_key} shape')
        expected_shape = _AZURE_SUPPORT_SHAPES.get(
            node_key,
            manifest_node.selected_shape,
        )
        if not expected_shape or shape.casefold() != expected_shape.casefold():
            raise DistributedRuntimeError(
                f'The {node_key} shape conflicts with the Azure contract.'
            )

        if node_key in {'control', 'application', 'load-generator'}:
            public_key = _node_address_key(node_key, 'public')
            public_ip = _required_text(
                resources.get(public_key),
                f'{node_key} public address',
            )
            if node.public_addresses != (public_ip,):
                raise DistributedRuntimeError(
                    f'The {node_key} public address conflicts with inventory.'
                )
        elif node.public_addresses:
            raise DistributedRuntimeError(
                f'The private {node_key} role unexpectedly has a public address.'
            )

        if node_key == 'database':
            storage = node.storage_resource('database-data')
            manifest_storage = manifest_nodes[node_key].storage
            if (
                storage is None
                or len(manifest_storage) != 1
                or len(node.storage) != 1
                or storage.lifecycle_status != 'running'
                or storage.kind != 'persistent_block_volume'
                or not storage.provider_resource_id
            ):
                raise DistributedRuntimeError(
                    'The database persistent-storage contract is incomplete.'
                )
            expected_storage = manifest_storage[0]
            if (
                storage.key != expected_storage.key
                or storage.device != '/dev/disk/azure/scsi1/lun0'
                or storage.filesystem != expected_storage.filesystem
                or storage.mount_point != expected_storage.mount_point
                or storage.size_gb != expected_storage.size_gib
                or storage.provisioned_iops != expected_storage.minimum_iops
                or storage.provisioned_throughput_mibps
                != expected_storage.minimum_throughput_mibps
                or storage.ephemeral is not False
                or resources.get('azure_database_disk_type')
                != 'PremiumV2_LRS'
                or resources.get('azure_database_disk_size_gb')
                != expected_storage.size_gib
                or resources.get('azure_database_disk_iops')
                != expected_storage.minimum_iops
                or resources.get('azure_database_disk_throughput_mibps')
                != expected_storage.minimum_throughput_mibps
                or resources.get('azure_database_disk_device')
                != storage.device
            ):
                raise DistributedRuntimeError(
                    'The database destructive-storage contract is invalid.'
                )
            if (
                not _azure_id_has_named_type(
                    storage.provider_resource_id,
                    'disks',
                    storage.provider_resource_name or '',
                )
                or not _same_azure_resource_id(
                    storage.provider_resource_id,
                    resources.get('azure_dsb_database_data_disk_id'),
                )
                or not _same_azure_resource_id(
                    resources.get(
                        'azure_dsb_database_data_disk_expected_id'
                    ),
                    storage.provider_resource_id,
                )
                or storage.provider_resource_name
                != resources.get('azure_dsb_database_data_disk_name')
            ):
                raise DistributedRuntimeError(
                    'The database disk identity conflicts with inventory.'
                )
        elif node.storage:
            raise DistributedRuntimeError(
                f'The {node_key} role has unexpected persistent storage.'
            )

        if node_key in _CLUSTER_KEYS:
            host_key, jump_host_key = _SSH_ROUTES[node_key]
            # Resolve both keys here as well as in main.ssh so a malformed
            # candidate fails before the first subprocess is created.
            _required_text(resources.get(host_key), f'{node_key} SSH target')
            if jump_host_key is not None:
                _required_text(
                    resources.get(jump_host_key),
                    f'{node_key} SSH jump target',
                )
            hosts.append(RuntimeHost(
                key=node_key,
                role=node.role,
                node_name=node_name,
                private_ip=private_ip,
                architecture=architecture,
                host_key=host_key,
                jump_host_key=jump_host_key,
            ))
        elif node_key == 'load-generator':
            host_key, jump_host_key = _SSH_ROUTES[node_key]
            _required_text(
                resources.get(host_key),
                f'{node_key} SSH target',
            )
            load_generator_host = RuntimeHost(
                key=node_key,
                role=node.role,
                node_name=node_name,
                private_ip=private_ip,
                architecture=architecture,
                host_key=host_key,
                jump_host_key=jump_host_key,
            )

    if tuple(host.key for host in hosts) != _CLUSTER_KEYS:
        raise DistributedRuntimeError(
            'The candidate cluster host order conflicts with its manifest.'
        )
    if load_generator_private_ip is None or load_generator_host is None:
        raise DistributedRuntimeError(
            'The candidate load-generator address is missing.'
        )
    return AzureK3sCandidatePlan(
        topology_fingerprint=manifest.fingerprint,
        hosts=tuple(hosts),
        load_generator=load_generator_host,
        load_generator_private_ip=load_generator_private_ip,
    )


def _validate_existing_journal(
    journal: Any,
    plan: AzureK3sCandidatePlan,
):
    if journal is None:
        return
    if not isinstance(journal, Mapping):
        raise DistributedRuntimeError('The K3s runtime journal is invalid.')
    expected = {
        'schema_version',
        'runtime_revision',
        'topology_fingerprint',
        'state',
        'prepared_hosts',
        'joined_agents',
        'server_ca_sha256',
        'database_volume',
    }
    if set(journal) != expected:
        raise DistributedRuntimeError(
            'The K3s runtime journal schema is invalid.'
        )
    if (
        journal['schema_version'] != RUNTIME_JOURNAL_SCHEMA_VERSION
        or journal['runtime_revision'] != DISTRIBUTED_RUNTIME_REVISION
        or journal['topology_fingerprint'] != plan.topology_fingerprint
        or journal['state'] not in _STATES
    ):
        raise DistributedRuntimeError(
            'The K3s runtime journal conflicts with the candidate contract.'
        )
    for key, allowed in (
        ('prepared_hosts', _CLUSTER_KEYS),
        ('joined_agents', _AGENT_KEYS),
    ):
        values = journal[key]
        if (
            not isinstance(values, list)
            or len(values) != len(set(values))
            or any(value not in allowed for value in values)
        ):
            raise DistributedRuntimeError(
                f'The K3s runtime journal has invalid {key}.'
            )
    if tuple(journal['prepared_hosts']) != _CLUSTER_KEYS[
        :len(journal['prepared_hosts'])
    ]:
        raise DistributedRuntimeError(
            'The K3s runtime journal prepared hosts are out of order.'
        )
    if tuple(journal['joined_agents']) != _AGENT_KEYS[
        :len(journal['joined_agents'])
    ]:
        raise DistributedRuntimeError(
            'The K3s runtime journal joined agents are out of order.'
        )
    ca_sha256 = journal['server_ca_sha256']
    if ca_sha256 is not None and (
        not isinstance(ca_sha256, str)
        or not _SHA256_RE.fullmatch(ca_sha256)
    ):
        raise DistributedRuntimeError(
            'The K3s runtime journal has an invalid server CA fingerprint.'
        )
    database_volume = journal['database_volume']
    if database_volume is not None:
        if (
            not isinstance(database_volume, Mapping)
            or set(database_volume) != {
                'lun',
                'device_link',
                'filesystem_uuid',
                'mount_point',
                'filesystem',
            }
            or database_volume['lun'] != 0
            or database_volume['device_link'] not in _DATABASE_DEVICE_LINKS
            or not isinstance(database_volume['filesystem_uuid'], str)
            or not _VOLUME_UUID_RE.fullmatch(
                database_volume['filesystem_uuid']
            )
            or database_volume['mount_point']
            != '/var/lib/deathstarbench/database'
            or database_volume['filesystem'] != 'xfs'
        ):
            raise DistributedRuntimeError(
                'The K3s runtime journal has an invalid database volume.'
            )
    state = journal['state']
    all_hosts = list(_CLUSTER_KEYS)
    all_agents = list(_AGENT_KEYS)
    if state == 'preparing_hosts':
        consistent = (
            database_volume is None
            and ca_sha256 is None
            and journal['joined_agents'] == []
        )
    elif state == 'hosts_prepared':
        consistent = (
            journal['prepared_hosts'] == all_hosts
            and database_volume is None
            and ca_sha256 is None
            and journal['joined_agents'] == []
        )
    elif state == 'database_storage_ready':
        consistent = (
            journal['prepared_hosts'] == all_hosts
            and database_volume is not None
            and ca_sha256 is None
            and journal['joined_agents'] == []
        )
    elif state == 'server_ready':
        consistent = (
            journal['prepared_hosts'] == all_hosts
            and database_volume is not None
            and ca_sha256 is not None
            and journal['joined_agents'] == []
        )
    elif state == 'joining_agents':
        consistent = (
            journal['prepared_hosts'] == all_hosts
            and database_volume is not None
            and ca_sha256 is not None
        )
    else:
        consistent = (
            journal['prepared_hosts'] == all_hosts
            and journal['joined_agents'] == all_agents
            and database_volume is not None
            and ca_sha256 is not None
        )
    if not consistent:
        raise DistributedRuntimeError(
            'The K3s runtime journal phase fields are inconsistent.'
        )


def parse_database_volume_attestation(output: str) -> dict[str, Any]:
    """Parse the exact, non-secret database mount attestation marker."""

    matches = re.findall(
        r'^AZURE_DSB_DATABASE_VOLUME lun=0 device_link=(\S+) '
        r'uuid=(\S+) mount_point=/var/lib/deathstarbench/database '
        r'filesystem=xfs$',
        str(output),
        flags=re.MULTILINE,
    )
    if len(matches) != 1:
        raise DistributedRuntimeError(
            'The database mount did not return one exact attestation marker.'
        )
    device_link, filesystem_uuid = matches[0]
    if (
        device_link not in _DATABASE_DEVICE_LINKS
        or not _VOLUME_UUID_RE.fullmatch(filesystem_uuid)
    ):
        raise DistributedRuntimeError(
            'The database mount returned an invalid device or filesystem UUID.'
        )
    return {
        'lun': 0,
        'device_link': device_link,
        'filesystem_uuid': filesystem_uuid.lower(),
        'mount_point': '/var/lib/deathstarbench/database',
        'filesystem': 'xfs',
    }


def parse_database_workload_storage_attestation(
    output: str,
    *,
    expected_filesystem_uuid: str,
) -> dict[str, Any]:
    """Parse the owned MongoDB-directory attestation without guest secrets."""

    expected_uuid = str(expected_filesystem_uuid).strip().lower()
    if not _VOLUME_UUID_RE.fullmatch(expected_uuid):
        raise DistributedRuntimeError(
            'The expected database workload filesystem UUID is invalid.'
        )
    matches = re.findall(
        r'^AZURE_DSB_WORKLOAD_STORAGE uuid=(\S+) '
        r'root=(\S+) databases=(\d+)$',
        str(output),
        flags=re.MULTILINE,
    )
    if len(matches) != 1:
        raise DistributedRuntimeError(
            'Database workload storage did not return one exact attestation.'
        )
    filesystem_uuid, root, database_count = matches[0]
    if (
        filesystem_uuid.casefold() != expected_uuid
        or root != _DATABASE_WORKLOAD_ROOT
        or database_count != '6'
    ):
        raise DistributedRuntimeError(
            'Database workload storage attestation conflicts with its contract.'
        )
    return {
        'filesystem_uuid': expected_uuid,
        'root': _DATABASE_WORKLOAD_ROOT,
        'database_count': 6,
    }


def _same_database_volume_identity(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
) -> bool:
    """Compare stable identity fields when an Azure disk is re-attested."""

    return (
        left.get('lun') == right.get('lun')
        and str(left.get('filesystem_uuid') or '').casefold()
        == str(right.get('filesystem_uuid') or '').casefold()
        and left.get('mount_point') == right.get('mount_point')
        and left.get('filesystem') == right.get('filesystem')
    )


def _write_journal(
    job: MutableMapping[str, Any],
    plan: AzureK3sCandidatePlan,
    *,
    state: str,
    prepared_hosts: list[str],
    joined_agents: list[str],
    server_ca_sha256: str | None,
    database_volume: Mapping[str, Any] | None,
    persist: Callable[[MutableMapping[str, Any]], Any] | None,
) -> dict[str, Any]:
    if state not in _STATES:
        raise DistributedRuntimeError(f'Invalid K3s runtime state {state}.')
    journal = {
        'schema_version': RUNTIME_JOURNAL_SCHEMA_VERSION,
        'runtime_revision': DISTRIBUTED_RUNTIME_REVISION,
        'topology_fingerprint': plan.topology_fingerprint,
        'state': state,
        'prepared_hosts': list(prepared_hosts),
        'joined_agents': list(joined_agents),
        'server_ca_sha256': server_ca_sha256,
        'database_volume': (
            dict(database_volume) if database_volume is not None else None
        ),
    }
    _validate_existing_journal(journal, plan)
    job.setdefault('resources', {})[RUNTIME_JOURNAL_KEY] = journal
    if persist is not None:
        persist(job)
    return journal


def _emit(
    emit: Callable[[MutableMapping[str, Any], str, str], Any] | None,
    job: MutableMapping[str, Any],
    message: str,
):
    if emit is not None:
        emit(job, 'Distributed K3s', message)


def _execute(
    execute: Callable[..., str],
    job: MutableMapping[str, Any],
    host: RuntimeHost,
    command: str,
    *,
    timeout: int,
    secret_stdin: str | None = None,
    stdin_text: str | None = None,
    sensitive_output: bool = False,
) -> str:
    kwargs: dict[str, Any] = {
        'timeout': timeout,
        'host_key': host.host_key,
    }
    if host.jump_host_key is not None:
        kwargs['jump_host_key'] = host.jump_host_key
    if secret_stdin is not None:
        kwargs['secret_stdin'] = secret_stdin
    if stdin_text is not None:
        kwargs['stdin_text'] = stdin_text
    if sensitive_output:
        kwargs['sensitive_output'] = True
    return execute(job, command, **kwargs)


def prepare_azure_distributed_k3s_candidate(
    job: MutableMapping[str, Any],
    *,
    execute: Callable[..., str],
    emit: Callable[[MutableMapping[str, Any], str, str], Any] | None = None,
    persist: Callable[[MutableMapping[str, Any]], Any] | None = None,
) -> dict[str, Any]:
    """Prepare and attest the internal Azure K3s cluster candidate.

    The function is intentionally not wired into normal benchmark dispatch.
    It may be invoked by a live qualification harness after the matching
    five-node infrastructure candidate has been created.
    """

    if not isinstance(job, MutableMapping):
        raise DistributedRuntimeError('A mutable benchmark job is required.')
    resources = job.get('resources')
    if not isinstance(resources, MutableMapping):
        raise DistributedRuntimeError('The job has no mutable resource state.')
    plan = azure_k3s_candidate_plan(resources)
    existing_journal = resources.get(RUNTIME_JOURNAL_KEY)
    _validate_existing_journal(existing_journal, plan)
    prior_server_ca_sha256 = (
        existing_journal['server_ca_sha256']
        if existing_journal is not None
        else None
    )
    prior_database_volume = (
        dict(existing_journal['database_volume'])
        if existing_journal is not None
        and existing_journal['database_volume'] is not None
        else None
    )
    prepared_hosts: list[str] = []
    joined_agents: list[str] = []
    server_ca_sha256 = None
    database_volume = None
    if existing_journal is None:
        _write_journal(
            job,
            plan,
            state='preparing_hosts',
            prepared_hosts=prepared_hosts,
            joined_agents=joined_agents,
            server_ca_sha256=server_ca_sha256,
            database_volume=database_volume,
            persist=persist,
        )

    for host in plan.hosts:
        _emit(emit, job, f'Preparing the {host.role} host ({host.node_name}).')
        _execute(
            execute,
            job,
            host,
            rocky_host_prepare_command(host.role),
            timeout=1800,
        )
        _execute(
            execute,
            job,
            host,
            artifact_install_command(host.architecture),
            timeout=1800,
        )
        _execute(
            execute,
            job,
            host,
            host_preflight_command(selinux_enabled=True),
            timeout=600,
        )
        prepared_hosts.append(host.key)
        if existing_journal is None:
            _write_journal(
                job,
                plan,
                state='preparing_hosts',
                prepared_hosts=prepared_hosts,
                joined_agents=joined_agents,
                server_ca_sha256=server_ca_sha256,
                database_volume=database_volume,
                persist=persist,
            )
    if prior_database_volume is None:
        _write_journal(
            job,
            plan,
            state='hosts_prepared',
            prepared_hosts=prepared_hosts,
            joined_agents=joined_agents,
            server_ca_sha256=server_ca_sha256,
            database_volume=database_volume,
            persist=persist,
        )

    database = plan.host('database')
    _emit(emit, job, 'Mounting and verifying the database data disk.')
    database_mount_output = _execute(
        execute,
        job,
        database,
        azure_deathstarbench_database_volume_mount_command(),
        timeout=300,
    )
    database_volume = parse_database_volume_attestation(
        database_mount_output
    )
    if (
        prior_database_volume is not None
        and not _same_database_volume_identity(
            prior_database_volume,
            database_volume,
        )
    ):
        raise DistributedRuntimeError(
            'The re-attested database volume identity changed during retry.'
        )
    if prior_server_ca_sha256 is None:
        _write_journal(
            job,
            plan,
            state='database_storage_ready',
            prepared_hosts=prepared_hosts,
            joined_agents=joined_agents,
            server_ca_sha256=server_ca_sha256,
            database_volume=database_volume,
            persist=persist,
        )

    control = plan.host('control')
    _emit(emit, job, 'Initializing root-only control and agent credentials.')
    _execute(
        execute,
        job,
        control,
        control_tokens_initialize_command(),
        timeout=120,
    )
    _emit(emit, job, 'Starting the pinned K3s control plane.')
    _execute(
        execute,
        job,
        control,
        server_install_command(
            control.node_name,
            control.private_ip,
            selinux_enabled=True,
        ),
        timeout=900,
    )
    _execute(
        execute,
        job,
        control,
        server_readiness_command(control.architecture),
        timeout=420,
    )
    _execute(
        execute,
        job,
        control,
        system_services_pin_command(control.node_name),
        timeout=660,
    )
    secure_agent_token = validated_token(_execute(
        execute,
        job,
        control,
        secure_agent_token_read_command(),
        timeout=120,
        sensitive_output=True,
    ))
    server_ca_sha256 = secure_agent_token_ca_sha256(secure_agent_token)
    if (
        prior_server_ca_sha256 is not None
        and server_ca_sha256 != prior_server_ca_sha256
    ):
        del secure_agent_token
        raise DistributedRuntimeError(
            'The re-attested K3s server CA fingerprint changed during retry.'
        )
    _write_journal(
        job,
        plan,
        state='server_ready',
        prepared_hosts=prepared_hosts,
        joined_agents=joined_agents,
        server_ca_sha256=server_ca_sha256,
        database_volume=database_volume,
        persist=persist,
    )

    _write_journal(
        job,
        plan,
        state='joining_agents',
        prepared_hosts=prepared_hosts,
        joined_agents=joined_agents,
        server_ca_sha256=server_ca_sha256,
        database_volume=database_volume,
        persist=persist,
    )
    try:
        for key in _AGENT_KEYS:
            host = plan.host(key)
            _emit(emit, job, f'Joining the {host.role} node ({host.node_name}).')
            _execute(
                execute,
                job,
                host,
                agent_token_install_command(),
                timeout=120,
                secret_stdin=secure_agent_token,
            )
            _execute(
                execute,
                job,
                host,
                agent_join_command(
                    host.node_name,
                    host.private_ip,
                    control.private_ip,
                    host.role,
                    selinux_enabled=True,
                ),
                timeout=900,
            )
            _execute(
                execute,
                job,
                host,
                agent_readiness_command(host.architecture),
                timeout=420,
            )
            joined_agents.append(host.key)
            _write_journal(
                job,
                plan,
                state='joining_agents',
                prepared_hosts=prepared_hosts,
                joined_agents=joined_agents,
                server_ca_sha256=server_ca_sha256,
                database_volume=database_volume,
                persist=persist,
            )
    finally:
        # Python strings cannot be reliably zeroed, but dropping the only
        # deliberate reference promptly avoids retaining it with job state.
        del secure_agent_token

    _write_journal(
        job,
        plan,
        state='agents_ready',
        prepared_hosts=prepared_hosts,
        joined_agents=joined_agents,
        server_ca_sha256=server_ca_sha256,
        database_volume=database_volume,
        persist=persist,
    )
    _emit(emit, job, 'Attesting exact K3s membership and node placement.')
    _execute(
        execute,
        job,
        control,
        cluster_nodes_readiness_command(plan.expected_nodes),
        timeout=420,
    )
    _execute(
        execute,
        job,
        control,
        system_services_readiness_command(control.node_name),
        timeout=420,
    )
    journal = _write_journal(
        job,
        plan,
        state='cluster_ready',
        prepared_hosts=prepared_hosts,
        joined_agents=joined_agents,
        server_ca_sha256=server_ca_sha256,
        database_volume=database_volume,
        persist=persist,
    )
    _emit(
        emit,
        job,
        'The exact four-node K3s candidate is ready; no workload was deployed.',
    )
    return journal


def _workload_phases(
    bundle: RenderedSocialNetworkBundle,
) -> tuple[tuple[str, str], ...]:
    """Return the exact, dependency-ordered workload apply phases."""

    names = getattr(bundle, 'phase_names', None)
    payloads = getattr(bundle, 'phase_payloads', None)
    expected_names = (
        'namespace',
        'storage',
        'policies',
        'services',
        'workloads',
    )
    if (
        not isinstance(names, tuple)
        or names != expected_names
        or not isinstance(payloads, tuple)
        or len(payloads) != len(names)
        or any(not isinstance(value, str) or not value for value in payloads)
    ):
        raise DistributedRuntimeError(
            'The rendered workload phase contract is invalid.'
        )
    return tuple(zip(names, payloads, strict=True))


def _workload_states(phase_names: tuple[str, ...]) -> frozenset[str]:
    return frozenset({
        'preparing_storage',
        'storage_ready',
        _WORKLOAD_READY_STATE,
        *(f'applying_{phase}' for phase in phase_names),
        *(f'{phase}_applied' for phase in phase_names),
    })


def _expected_workload_nodes(
    plan: AzureK3sCandidatePlan,
    bundle: RenderedSocialNetworkBundle,
) -> tuple[str, ...]:
    roles = set(bundle.expected_component_placement.values())
    if not roles or not roles.issubset(set(_CLUSTER_KEYS)):
        raise DistributedRuntimeError(
            'The rendered workload contains an invalid placement role.'
        )
    return tuple(sorted(
        plan.host(role).node_name
        for role in roles
    ))


def _normalized_workload_attestation(
    value: Mapping[str, Any],
    *,
    plan: AzureK3sCandidatePlan,
    bundle: RenderedSocialNetworkBundle,
) -> dict[str, Any]:
    expected_fields = {
        'schema_version',
        'namespace',
        'workload_revision',
        'image_set_revision',
        'architecture',
        'platform',
        'load_generator_cidr',
        'image_lock_fingerprint',
        'bundle_fingerprint',
        'component_count',
        'pod_nodes',
    }
    if not isinstance(value, Mapping) or set(value) != expected_fields:
        raise DistributedRuntimeError(
            'The workload readiness attestation schema is invalid.'
        )
    expected_nodes = _expected_workload_nodes(plan, bundle)
    pod_nodes = value['pod_nodes']
    if (
        isinstance(pod_nodes, str)
        or not isinstance(pod_nodes, (list, tuple))
        or tuple(pod_nodes) != expected_nodes
    ):
        raise DistributedRuntimeError(
            'The workload readiness attestation has invalid node placement.'
        )
    expected = {
        'schema_version': 1,
        'namespace': bundle.namespace,
        'workload_revision': DISTRIBUTED_WORKLOAD_REVISION,
        'image_set_revision': bundle.image_set_revision,
        'architecture': bundle.application_architecture,
        'platform': bundle.platform,
        'load_generator_cidr': bundle.load_generator_cidr,
        'image_lock_fingerprint': bundle.image_lock_fingerprint,
        'bundle_fingerprint': bundle.rendered_manifest_sha256,
        'component_count': len(bundle.expected_component_placement),
        'pod_nodes': list(expected_nodes),
    }
    comparable = dict(value)
    comparable['pod_nodes'] = list(pod_nodes)
    if comparable != expected:
        raise DistributedRuntimeError(
            'The workload readiness attestation conflicts with its bundle.'
        )
    return expected


def _validate_database_workload_storage(
    value: Any,
    *,
    filesystem_uuid: str,
) -> dict[str, Any]:
    expected = {
        'filesystem_uuid': filesystem_uuid.casefold(),
        'root': _DATABASE_WORKLOAD_ROOT,
        'database_count': 6,
    }
    if not isinstance(value, Mapping) or dict(value) != expected:
        raise DistributedRuntimeError(
            'The workload journal has invalid database storage state.'
        )
    return expected


def _validate_existing_workload_journal(
    journal: Any,
    *,
    plan: AzureK3sCandidatePlan,
    bundle: RenderedSocialNetworkBundle,
    filesystem_uuid: str,
):
    if journal is None:
        return
    if not isinstance(journal, Mapping):
        raise DistributedRuntimeError('The workload journal is invalid.')
    expected_fields = {
        'schema_version',
        'runtime_revision',
        'topology_fingerprint',
        'workload_id',
        'workload_revision',
        'manifest_source_sha256',
        'rendered_manifest_sha256',
        'image_set_revision',
        'image_lock_fingerprint',
        'application_architecture',
        'namespace',
        'load_generator_cidr',
        'state',
        'applied_phases',
        'database_storage',
        'workload_attestation',
    }
    if set(journal) != expected_fields:
        raise DistributedRuntimeError('The workload journal schema is invalid.')
    phases = _workload_phases(bundle)
    phase_names = tuple(name for name, _ in phases)
    metadata = {
        'schema_version': WORKLOAD_JOURNAL_SCHEMA_VERSION,
        'runtime_revision': DISTRIBUTED_RUNTIME_REVISION,
        'topology_fingerprint': plan.topology_fingerprint,
        'workload_id': WORKLOAD_ID,
        'workload_revision': DISTRIBUTED_WORKLOAD_REVISION,
        'manifest_source_sha256': bundle.manifest_source_sha256,
        'rendered_manifest_sha256': bundle.rendered_manifest_sha256,
        'image_set_revision': DISTRIBUTED_IMAGE_SET_REVISION,
        'image_lock_fingerprint': bundle.image_lock_fingerprint,
        'application_architecture': bundle.application_architecture,
        'namespace': WORKLOAD_NAMESPACE,
        'load_generator_cidr': bundle.load_generator_cidr,
    }
    if any(journal[key] != value for key, value in metadata.items()):
        raise DistributedRuntimeError(
            'The workload journal conflicts with the rendered candidate.'
        )
    for field in (
        'manifest_source_sha256',
        'rendered_manifest_sha256',
        'image_lock_fingerprint',
    ):
        if not isinstance(journal[field], str) or not _PREFIXED_SHA256_RE.fullmatch(
            journal[field]
        ):
            raise DistributedRuntimeError(
                f'The workload journal has invalid {field}.'
            )
    state = journal['state']
    if state not in _workload_states(phase_names):
        raise DistributedRuntimeError('The workload journal state is invalid.')
    applied = journal['applied_phases']
    if (
        not isinstance(applied, list)
        or tuple(applied) != phase_names[:len(applied)]
    ):
        raise DistributedRuntimeError(
            'The workload journal apply phases are invalid.'
        )
    if state == 'preparing_storage':
        expected_applied = ()
        storage_required = False
        attestation_required = False
    elif state == 'storage_ready':
        expected_applied = ()
        storage_required = True
        attestation_required = False
    elif state == _WORKLOAD_READY_STATE:
        expected_applied = phase_names
        storage_required = True
        attestation_required = True
    else:
        applying = state.startswith('applying_')
        phase = state[len('applying_'):] if applying else state[:-len('_applied')]
        phase_index = phase_names.index(phase)
        expected_applied = phase_names[:phase_index + (0 if applying else 1)]
        storage_required = True
        attestation_required = False
    if tuple(applied) != expected_applied:
        raise DistributedRuntimeError(
            'The workload journal state and apply phases conflict.'
        )
    database_storage = journal['database_storage']
    if storage_required:
        _validate_database_workload_storage(
            database_storage,
            filesystem_uuid=filesystem_uuid,
        )
    elif database_storage is not None:
        raise DistributedRuntimeError(
            'The workload journal records storage before preparation.'
        )
    attestation = journal['workload_attestation']
    if attestation_required:
        _normalized_workload_attestation(
            attestation,
            plan=plan,
            bundle=bundle,
        )
    elif attestation is not None:
        raise DistributedRuntimeError(
            'The workload journal records readiness before attestation.'
        )


def _write_workload_journal(
    job: MutableMapping[str, Any],
    plan: AzureK3sCandidatePlan,
    bundle: RenderedSocialNetworkBundle,
    *,
    state: str,
    applied_phases: list[str],
    filesystem_uuid: str,
    database_storage: Mapping[str, Any] | None,
    workload_attestation: Mapping[str, Any] | None,
    persist: Callable[[MutableMapping[str, Any]], Any] | None,
) -> dict[str, Any]:
    journal = {
        'schema_version': WORKLOAD_JOURNAL_SCHEMA_VERSION,
        'runtime_revision': DISTRIBUTED_RUNTIME_REVISION,
        'topology_fingerprint': plan.topology_fingerprint,
        'workload_id': WORKLOAD_ID,
        'workload_revision': DISTRIBUTED_WORKLOAD_REVISION,
        'manifest_source_sha256': bundle.manifest_source_sha256,
        'rendered_manifest_sha256': bundle.rendered_manifest_sha256,
        'image_set_revision': bundle.image_set_revision,
        'image_lock_fingerprint': bundle.image_lock_fingerprint,
        'application_architecture': bundle.application_architecture,
        'namespace': bundle.namespace,
        'load_generator_cidr': bundle.load_generator_cidr,
        'state': state,
        'applied_phases': list(applied_phases),
        'database_storage': (
            dict(database_storage) if database_storage is not None else None
        ),
        'workload_attestation': (
            dict(workload_attestation)
            if workload_attestation is not None
            else None
        ),
    }
    _validate_existing_workload_journal(
        journal,
        plan=plan,
        bundle=bundle,
        filesystem_uuid=filesystem_uuid,
    )
    job.setdefault('resources', {})[WORKLOAD_JOURNAL_KEY] = journal
    if persist is not None:
        persist(job)
    return journal


def prepare_azure_distributed_social_network_candidate(
    job: MutableMapping[str, Any],
    image_lock: Mapping[str, Any],
    *,
    execute: Callable[..., str],
    emit: Callable[[MutableMapping[str, Any], str, str], Any] | None = None,
    persist: Callable[[MutableMapping[str, Any]], Any] | None = None,
) -> dict[str, Any]:
    """Deploy and attest the unreleased Social Network workload candidate.

    A caller must first complete ``prepare_azure_distributed_k3s_candidate``.
    This operation is declarative and retryable, but intentionally stops at
    ``workload_ready`` before any non-resumable dataset initialization or
    benchmark measurement.
    """

    if not isinstance(job, MutableMapping):
        raise DistributedRuntimeError('A mutable benchmark job is required.')
    resources = job.get('resources')
    if not isinstance(resources, MutableMapping):
        raise DistributedRuntimeError('The job has no mutable resource state.')
    plan = azure_k3s_candidate_plan(resources)
    runtime_journal = resources.get(RUNTIME_JOURNAL_KEY)
    _validate_existing_journal(runtime_journal, plan)
    if runtime_journal is None or runtime_journal['state'] != 'cluster_ready':
        raise DistributedRuntimeError(
            'The exact K3s candidate must be cluster_ready before workload deployment.'
        )
    try:
        bundle = render_social_network_bundle(
            image_lock,
            plan.host('application').architecture,
            plan.load_generator_private_ip,
        )
    except WorkloadBundleError as exc:
        raise DistributedRuntimeError(
            f'The Social Network workload bundle is invalid: {exc}'
        ) from exc
    phases = _workload_phases(bundle)
    phase_names = tuple(name for name, _ in phases)
    database_volume = runtime_journal['database_volume']
    filesystem_uuid = database_volume['filesystem_uuid']
    existing = resources.get(WORKLOAD_JOURNAL_KEY)
    _validate_existing_workload_journal(
        existing,
        plan=plan,
        bundle=bundle,
        filesystem_uuid=filesystem_uuid,
    )
    if existing is None:
        current = _write_workload_journal(
            job,
            plan,
            bundle,
            state='preparing_storage',
            applied_phases=[],
            filesystem_uuid=filesystem_uuid,
            database_storage=None,
            workload_attestation=None,
            persist=persist,
        )
    else:
        current = dict(existing)
        if current['state'] == _WORKLOAD_READY_STATE:
            # Readiness describes live state, not merely completed apply
            # phases.  Revoke it before the first remote revalidation so a
            # transport failure, drift, or cancellation cannot leave a stale
            # ready journal for a later measurement phase to consume.
            current = _write_workload_journal(
                job,
                plan,
                bundle,
                state=f'{phase_names[-1]}_applied',
                applied_phases=list(phase_names),
                filesystem_uuid=filesystem_uuid,
                database_storage=current['database_storage'],
                workload_attestation=None,
                persist=persist,
            )

    control = plan.host('control')
    database = plan.host('database')
    _emit(emit, job, 'Re-attesting K3s before workload deployment.')
    _execute(
        execute,
        job,
        control,
        cluster_nodes_readiness_command(plan.expected_nodes),
        timeout=420,
    )
    _execute(
        execute,
        job,
        control,
        system_services_readiness_command(control.node_name),
        timeout=420,
    )
    observed_server_ca_sha256 = str(_execute(
        execute,
        job,
        control,
        server_ca_sha256_read_command(),
        timeout=120,
    )).strip()
    if (
        not _SHA256_RE.fullmatch(observed_server_ca_sha256)
        or observed_server_ca_sha256 != runtime_journal['server_ca_sha256']
    ):
        raise DistributedRuntimeError(
            'The K3s server CA identity changed before workload deployment.'
        )
    database_mount_output = _execute(
        execute,
        job,
        database,
        azure_deathstarbench_database_volume_attestation_command(
            filesystem_uuid
        ),
        timeout=300,
    )
    observed_volume = parse_database_volume_attestation(database_mount_output)
    if not _same_database_volume_identity(database_volume, observed_volume):
        raise DistributedRuntimeError(
            'The database volume identity changed before workload deployment.'
        )

    _emit(emit, job, 'Preparing the six owned MongoDB storage directories.')
    storage_output = _execute(
        execute,
        job,
        database,
        azure_deathstarbench_database_workload_storage_command(
            filesystem_uuid
        ),
        timeout=300,
    )
    database_storage = parse_database_workload_storage_attestation(
        storage_output,
        expected_filesystem_uuid=filesystem_uuid,
    )
    prior_storage = current['database_storage']
    if prior_storage is not None and dict(prior_storage) != database_storage:
        raise DistributedRuntimeError(
            'The database workload storage identity changed during retry.'
        )
    if current['state'] == 'preparing_storage':
        current = _write_workload_journal(
            job,
            plan,
            bundle,
            state='storage_ready',
            applied_phases=[],
            filesystem_uuid=filesystem_uuid,
            database_storage=database_storage,
            workload_attestation=None,
            persist=persist,
        )

    applied = list(current['applied_phases'])
    for phase, payload in phases:
        if phase in applied:
            continue
        current = _write_workload_journal(
            job,
            plan,
            bundle,
            state=f'applying_{phase}',
            applied_phases=applied,
            filesystem_uuid=filesystem_uuid,
            database_storage=database_storage,
            workload_attestation=None,
            persist=persist,
        )
        _emit(
            emit,
            job,
            f'Applying Social Network workload phase {phase}.',
        )
        _execute(
            execute,
            job,
            control,
            workload_apply_command(bundle.phase_sha256(phase)),
            timeout=600,
            stdin_text=payload,
        )
        applied.append(phase)
        current = _write_workload_journal(
            job,
            plan,
            bundle,
            state=f'{phase}_applied',
            applied_phases=applied,
            filesystem_uuid=filesystem_uuid,
            database_storage=database_storage,
            workload_attestation=None,
            persist=persist,
        )
    if tuple(applied) != phase_names:
        raise DistributedRuntimeError(
            'The workload apply journal did not reach its exact phase boundary.'
        )

    _emit(emit, job, 'Attesting exact Social Network workload state.')
    readiness_output = _execute(
        execute,
        job,
        control,
        workload_readiness_command(bundle),
        # The remote command has two sequential bounded 900-second waits
        # (Deployments, then Pods), so the SSH deadline must cover both plus
        # the final inventory read.
        timeout=2100,
    )
    try:
        parsed_attestation = parse_workload_attestation(
            readiness_output,
            bundle,
        )
    except WorkloadBundleError as exc:
        raise DistributedRuntimeError(
            f'The Social Network workload failed attestation: {exc}'
        ) from exc
    workload_attestation = _normalized_workload_attestation(
        parsed_attestation,
        plan=plan,
        bundle=bundle,
    )
    journal = _write_workload_journal(
        job,
        plan,
        bundle,
        state=_WORKLOAD_READY_STATE,
        applied_phases=applied,
        filesystem_uuid=filesystem_uuid,
        database_storage=database_storage,
        workload_attestation=workload_attestation,
        persist=persist,
    )
    _emit(
        emit,
        job,
        'The Social Network workload candidate is ready; no dataset was initialized.',
    )
    return journal
