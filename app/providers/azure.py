"""Microsoft Azure discovery, provisioning, and fail-closed cleanup.

Authentication is deliberately delegated to :class:`AzureCliCredential`.
The selected subscription ID is the only identity value persisted with a
job; access tokens and Azure CLI account material never enter a plan or run
state.

Each run owns one deterministic, tagged resource group.  The resource-group
identity and ownership contract are persisted before the first Azure write.
Cleanup re-reads that exact group, verifies its subscription, location, and
tags, and only then deletes it.  This makes an interrupted create recoverable
without ever broadening deletion to a tag search or subscription sweep.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import subprocess
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import replace
from types import MappingProxyType
from typing import Any

from ..deathstarbench_contract import (
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    K3S_RUNTIME_ID,
    K3S_RUNTIME_JOURNAL_KEY,
    PODMAN_COMPOSE_RUNTIME_ID,
    SINGLE_HOST_TOPOLOGY_ID,
    require_released_runtime,
)
from ..deathstarbench_topology import (
    BENCHMARK_INGRESS_CHANNEL,
    DeathStarBenchTopologyManifest,
    K3S_CONTROL_PLANE_CHANNEL,
    K3S_OVERLAY_CHANNEL,
    TopologyManifestError,
    build_topology_manifest,
)
from ..guests import rocky_linux
from ..resource_inventory import (
    ROLE_NODE_INVENTORY_KEY,
    ResourceInventoryError,
    RoleNode,
    RoleNodeInventory,
    StorageResource,
    load_role_node_inventory,
    persist_role_node_inventory,
)


DEFAULT_REGION = 'eastus2'
MANAGED_BY = 'oci-self-service-benchmarks'
SSH_USER = 'benchmark'
SSH_SOURCE_CIDR = '0.0.0.0/0'
VNET_CIDR = '10.42.0.0/16'
SUBNET_CIDR = '10.42.1.0/24'
DEFAULT_OS_DISK_SIZE_GB = 100
PEER_OS_DISK_SIZE_GB = 32
LOADGEN_OS_DISK_SIZE_GB = 50
PREMIUM_V2_DISK_TYPE = 'PremiumV2_LRS'
PREMIUM_V2_IOPS = 3000
PREMIUM_V2_THROUGHPUT_MIBPS = 125
DATA_DISK_LUN = 0
DATA_DISK_DEVICE = '/dev/disk/azure/scsi1/lun0'
SUPPORTED_ARCHITECTURES = frozenset({'x86_64', 'arm64'})
SUPPORTED_IPERF3_PROTOCOLS = frozenset({'tcp', 'udp'})
WEB_BENCHMARKS = frozenset({'apachebench', 'deathstarbench'})
LOADGEN_VM_SIZES = (
    'Standard_D2as_v7',
)
DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY = 'deathstarbench_topology_manifest'
DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY = (
    'deathstarbench_topology_fingerprint'
)
DISTRIBUTED_VNET_CIDR = '10.240.0.0/16'
DISTRIBUTED_CLUSTER_SUBNET_CIDR = '10.240.1.0/24'
DISTRIBUTED_LOADGEN_SUBNET_CIDR = '10.240.2.0/24'
DISTRIBUTED_PRIVATE_ADDRESSES = MappingProxyType({
    'control': '10.240.1.10',
    'database': '10.240.1.11',
    'cache': '10.240.1.12',
    'application': '10.240.1.13',
    'load-generator': '10.240.2.10',
})
# Fixed support-role sizes are part of the Azure implementation of the
# versioned provider-neutral capacity contract. Changing one requires a new
# distributed runtime revision and a fresh qualification pass.
DISTRIBUTED_ROLE_VM_SIZES = MappingProxyType({
    'control': 'Standard_D2as_v7',
    'database': 'Standard_D4as_v7',
    'cache': 'Standard_D2as_v7',
    'load-generator': 'Standard_D2as_v7',
})
DISTRIBUTED_PUBLIC_IP_NODES = frozenset({
    'control',
    'application',
    'load-generator',
})
DISTRIBUTED_OS_DISK_SIZE_GB = 50


def _managed_expected_id_keys() -> frozenset[str]:
    direct_prefixes = {
        'azure_nsg',
        'azure_vnet',
        'azure_subnet',
        'azure_data_disk',
        'azure_dsb_cluster_nsg',
        'azure_dsb_loadgen_nsg',
        'azure_dsb_vnet',
        'azure_dsb_cluster_nat_public_ip',
        'azure_dsb_cluster_nat_gateway',
        'azure_dsb_cluster_subnet',
        'azure_dsb_loadgen_subnet',
        'azure_dsb_database_data_disk',
    }
    vm_prefixes = {
        'azure_instance',
        'azure_loadgen_instance',
        'azure_peer_instance',
        'azure_dsb_control_instance',
        'azure_dsb_database_instance',
        'azure_dsb_cache_instance',
        'azure_dsb_application_instance',
        'azure_dsb_load_generator_instance',
    }
    keys = {f'{prefix}_expected_id' for prefix in direct_prefixes}
    for prefix in vm_prefixes:
        keys.update({
            f'{prefix}_expected_id',
            f'{prefix}_public_ip_expected_id',
            f'{prefix}_nic_expected_id',
            f'{prefix}_os_disk_expected_id',
        })
    return frozenset(keys)


AZURE_MANAGED_EXPECTED_ID_KEYS = _managed_expected_id_keys()
# Azure's Resource SKU response does not consistently include the optional
# MaxNetworkBandwidth capability for Dasv7 sizes.  ApacheBench needs this
# value to report load-generator network headroom, so retain the documented
# maximum for the one fixed load-generator size.  A positive live capability
# remains authoritative when Azure supplies one.
# https://learn.microsoft.com/azure/virtual-machines/sizes/general-purpose/dasv7-series
DOCUMENTED_MAX_NETWORK_BANDWIDTH_GBPS = {
    'standard_d2as_v7': 16.0,
}

# Rocky Enterprise Software Foundation Marketplace image coordinates.  The
# version is resolved and pinned before the resource group is created.
ROCKY_IMAGES = {
    'x86_64': {
        'publisher': 'resf',
        'offer': 'rockylinux-x86_64',
        'sku': '9-base',
    },
    'arm64': {
        'publisher': 'resf',
        'offer': 'rockylinux-aarch64',
        'sku': 'rockylinux-aarch64-9',
    },
}

EventCallback = Callable[[dict[str, Any], str, str], None]
PersistCallback = Callable[[dict[str, Any]], None]


def _value(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        if name in value:
            return value[name]
        parts = name.split('_')
        camel_name = parts[0] + ''.join(part.title() for part in parts[1:])
        if camel_name in value:
            return value[camel_name]
        return default
    return getattr(value, name, default)


def _iter(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        for key in ('value', 'items'):
            if key in value:
                return list(value[key] or ())
        return []
    nested = getattr(value, 'value', None)
    if nested is not None:
        return list(nested or ())
    return list(value)


def _emit(job: dict[str, Any], emit: EventCallback | None, stage: str, message: str):
    if emit:
        emit(job, stage, message)


def _persist(job: dict[str, Any], persist: PersistCallback | None):
    if persist:
        persist(job)


def _record(job: dict[str, Any], persist: PersistCallback | None, **values: Any):
    job.setdefault('resources', {}).update(values)
    _persist(job, persist)


def _forget(job: dict[str, Any], persist: PersistCallback | None, *keys: str):
    resources = job.setdefault('resources', {})
    for key in keys:
        resources.pop(key, None)
    _persist(job, persist)


def _persist_inventory(
    job: dict[str, Any],
    persist: PersistCallback | None,
    inventory: RoleNodeInventory,
) -> RoleNodeInventory:
    persist_role_node_inventory(job.setdefault('resources', {}), inventory)
    _persist(job, persist)
    return inventory


def _inventory_static_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.casefold()
    return value


def _validate_expected_role_inventory(
    current: RoleNodeInventory,
    expected: RoleNodeInventory,
):
    if current.provider != 'azure' or current.provider != expected.provider:
        raise ResourceInventoryError(
            'Azure role-node inventory has a conflicting provider.'
        )
    if current.topology_fingerprint != expected.topology_fingerprint:
        raise ResourceInventoryError(
            'Azure role-node inventory has a conflicting topology fingerprint.'
        )
    current_keys = tuple(node.key for node in current.nodes)
    expected_keys = tuple(node.key for node in expected.nodes)
    if current_keys != expected_keys:
        raise ResourceInventoryError(
            'Azure role-node inventory has conflicting logical nodes.'
        )
    stable_node_fields = (
        'role',
        'provider_resource_id',
        'provider_resource_name',
        'private_addresses',
        'zone',
        'shape',
        'architecture',
        'capacity_class',
    )
    stable_storage_fields = (
        'key',
        'kind',
        'provider_resource_id',
        'provider_resource_name',
        'mount_point',
        'filesystem',
        'size_gb',
        'provisioned_iops',
        'provisioned_throughput_mibps',
        'ephemeral',
    )
    for expected_node in expected.nodes:
        current_node = current.node(expected_node.key)
        if current_node is None:
            raise ResourceInventoryError(
                f'Azure role-node inventory is missing {expected_node.key}.'
            )
        for field_name in stable_node_fields:
            actual_value = _inventory_static_value(
                getattr(current_node, field_name)
            )
            expected_value = _inventory_static_value(
                getattr(expected_node, field_name)
            )
            if field_name == 'private_addresses' and not expected_value:
                continue
            if actual_value != expected_value:
                raise ResourceInventoryError(
                    f'Azure role node {expected_node.key} has conflicting '
                    f'{field_name}.'
                )
        current_storage = {item.key: item for item in current_node.storage}
        expected_storage = {item.key: item for item in expected_node.storage}
        if set(current_storage) != set(expected_storage):
            raise ResourceInventoryError(
                f'Azure role node {expected_node.key} has conflicting storage.'
            )
        for storage_key, expected_item in expected_storage.items():
            current_item = current_storage[storage_key]
            for field_name in stable_storage_fields:
                actual_value = _inventory_static_value(
                    getattr(current_item, field_name)
                )
                expected_value = _inventory_static_value(
                    getattr(expected_item, field_name)
                )
                if actual_value != expected_value:
                    raise ResourceInventoryError(
                        f'Azure storage {storage_key} has conflicting '
                        f'{field_name}.'
                    )


def _initialize_topology_inventory(
    job: dict[str, Any],
    persist: PersistCallback | None,
    manifest: DeathStarBenchTopologyManifest,
    expected_inventory: RoleNodeInventory,
) -> RoleNodeInventory:
    """Persist one exact manifest and every planned node before Azure writes."""

    resources = job.setdefault('resources', {})
    contract_keys = (
        DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
        DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY,
        ROLE_NODE_INVENTORY_KEY,
    )
    present = tuple(key in resources for key in contract_keys)
    if any(present) and not all(present):
        raise ResourceInventoryError(
            'Azure DeathStarBench topology metadata is incomplete.'
        )
    if all(present):
        saved_manifest = DeathStarBenchTopologyManifest.from_dict(
            resources[DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY]
        )
        if saved_manifest != manifest:
            raise ResourceInventoryError(
                'Azure DeathStarBench topology conflicts with the saved run.'
            )
        saved_fingerprint = resources[
            DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY
        ]
        if saved_fingerprint != manifest.fingerprint:
            raise ResourceInventoryError(
                'Azure DeathStarBench topology fingerprint is inconsistent.'
            )
        current = load_role_node_inventory(resources)
        _validate_expected_role_inventory(current, expected_inventory)
        return current

    resources[DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY] = manifest.as_dict()
    resources[DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY] = manifest.fingerprint
    persist_role_node_inventory(resources, expected_inventory)
    _persist(job, persist)
    return expected_inventory


def _initialize_distributed_candidate_contract(
    job: dict[str, Any],
    persist: PersistCallback | None,
    manifest: DeathStarBenchTopologyManifest,
    expected_inventory: RoleNodeInventory,
    values: Mapping[str, Any],
) -> RoleNodeInventory:
    """Atomically establish, or strictly validate, the Azure run contract."""

    resources = job.setdefault('resources', {})
    topology_keys = (
        DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
        DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY,
        ROLE_NODE_INVENTORY_KEY,
    )
    has_existing_contract = any(
        key in resources for key in (
            *topology_keys,
            'azure_distributed_candidate',
            'azure_resource_group_name',
            'azure_resource_group_expected_id',
        )
    )
    if has_existing_contract:
        if not all(key in resources for key in topology_keys):
            raise ResourceInventoryError(
                'Azure distributed candidate contract is incomplete.'
            )
        for key, expected in values.items():
            if (
                key == 'azure_subscription_name'
                or key.endswith('_create_ambiguous')
                or key.endswith('_delete_ambiguous')
            ):
                # Display names and lifecycle flags are not identities.
                continue
            if key not in resources:
                raise ResourceInventoryError(
                    f'Azure distributed candidate contract is missing {key}.'
                )
            current = resources[key]
            if key.endswith('_id') and isinstance(expected, str):
                matches = _normalize_id(current) == _normalize_id(expected)
            elif isinstance(expected, str):
                matches = isinstance(current, str) and (
                    current.casefold() == expected.casefold()
                )
            else:
                matches = current == expected
            if not matches:
                raise ResourceInventoryError(
                    f'Azure distributed candidate contract conflicts at {key}.'
                )
        return _initialize_topology_inventory(
            job,
            persist,
            manifest,
            expected_inventory,
        )

    resources.update(dict(values))
    resources[DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY] = manifest.as_dict()
    resources[DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY] = manifest.fingerprint
    persist_role_node_inventory(resources, expected_inventory)
    _persist(job, persist)
    return expected_inventory


def _set_inventory_node_status(
    job: dict[str, Any],
    persist: PersistCallback | None,
    node_key: str,
    status: str,
    *,
    provider_resource_id: str | None = None,
    public_address: str | None = None,
    private_address: str | None = None,
) -> RoleNodeInventory:
    resources = job.setdefault('resources', {})
    inventory = load_role_node_inventory(resources)
    node = inventory.node(node_key)
    if node is None:
        raise ResourceInventoryError(
            f'Azure role-node inventory is missing {node_key}.'
        )
    update = RoleNode(
        key=node.key,
        role=node.role,
        provider_resource_id=provider_resource_id,
        public_addresses=(public_address,) if public_address else (),
        private_addresses=(private_address,) if private_address else (),
        lifecycle_status=status,
    )
    return _persist_inventory(job, persist, inventory.upsert(update))


def _set_inventory_storage_status(
    job: dict[str, Any],
    persist: PersistCallback | None,
    node_key: str,
    storage_key: str,
    status: str,
    *,
    provider_resource_id: str | None = None,
) -> RoleNodeInventory:
    resources = job.setdefault('resources', {})
    inventory = load_role_node_inventory(resources)
    node = inventory.node(node_key)
    if node is None:
        raise ResourceInventoryError(
            f'Azure role-node inventory is missing {node_key}.'
        )
    storage = node.storage_resource(storage_key)
    if storage is None:
        raise ResourceInventoryError(
            f'Azure role node {node_key} is missing storage {storage_key}.'
        )
    updated_node = node.upsert_storage(replace(
        storage,
        provider_resource_id=(provider_resource_id or storage.provider_resource_id),
        lifecycle_status=status,
    ))
    return _persist_inventory(job, persist, inventory.upsert(updated_node))


def _load_topology_inventory_contract(
    resources: Mapping[str, Any],
) -> tuple[DeathStarBenchTopologyManifest, RoleNodeInventory] | None:
    contract_keys = (
        DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
        DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY,
        ROLE_NODE_INVENTORY_KEY,
    )
    present = tuple(key in resources for key in contract_keys)
    if not any(present):
        return None
    if not all(present):
        raise ResourceInventoryError(
            'Azure DeathStarBench cleanup metadata is incomplete.'
        )
    manifest = DeathStarBenchTopologyManifest.from_dict(
        resources[DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY]
    )
    fingerprint = resources[DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY]
    inventory = load_role_node_inventory(resources)
    if fingerprint != manifest.fingerprint:
        raise ResourceInventoryError(
            'Azure DeathStarBench cleanup fingerprint is inconsistent.'
        )
    if inventory.provider != 'azure':
        raise ResourceInventoryError(
            'Azure DeathStarBench cleanup inventory has a conflicting provider.'
        )
    if inventory.topology_fingerprint != manifest.fingerprint:
        raise ResourceInventoryError(
            'Azure DeathStarBench inventory fingerprint is inconsistent.'
        )
    if tuple(node.key for node in inventory.nodes) != tuple(
        sorted(node.key for node in manifest.nodes)
    ):
        raise ResourceInventoryError(
            'Azure DeathStarBench cleanup inventory has conflicting nodes.'
        )
    planned_inventory = manifest.planned_inventory('azure')
    for planned_node in planned_inventory.nodes:
        current_node = inventory.node(planned_node.key)
        if current_node is None or (
            current_node.role != planned_node.role
            or current_node.capacity_class != planned_node.capacity_class
        ):
            raise ResourceInventoryError(
                'Azure DeathStarBench cleanup inventory has conflicting role '
                'or capacity metadata.'
            )
        if planned_node.shape and (
            not current_node.shape
            or current_node.shape.casefold() != planned_node.shape.casefold()
        ):
            raise ResourceInventoryError(
                f'Azure DeathStarBench node {planned_node.key} has a '
                'conflicting selected shape.'
            )
        if (
            planned_node.architecture
            and current_node.architecture != planned_node.architecture
        ):
            raise ResourceInventoryError(
                f'Azure DeathStarBench node {planned_node.key} has a '
                'conflicting architecture.'
            )
        planned_storage = {item.key: item for item in planned_node.storage}
        current_storage = {item.key: item for item in current_node.storage}
        for storage_key, planned_item in planned_storage.items():
            current_item = current_storage.get(storage_key)
            if current_item is None or any((
                current_item.kind != planned_item.kind,
                current_item.mount_point != planned_item.mount_point,
                current_item.filesystem != planned_item.filesystem,
                current_item.size_gb != planned_item.size_gb,
                current_item.provisioned_iops != planned_item.provisioned_iops,
                current_item.provisioned_throughput_mibps
                != planned_item.provisioned_throughput_mibps,
                current_item.ephemeral != planned_item.ephemeral,
            )):
                raise ResourceInventoryError(
                    f'Azure DeathStarBench storage {storage_key} conflicts '
                    'with its topology contract.'
                )
    if manifest.topology_id == DISTRIBUTED_TIERED_TOPOLOGY_ID:
        for node in inventory.nodes:
            expected_shape = (
                next(
                    item.selected_shape
                    for item in manifest.nodes
                    if item.key == node.key
                )
                if node.key == 'application'
                else DISTRIBUTED_ROLE_VM_SIZES[node.key]
            )
            if (
                not node.shape
                or not expected_shape
                or node.shape.casefold() != expected_shape.casefold()
                or node.private_addresses
                != (DISTRIBUTED_PRIVATE_ADDRESSES[node.key],)
            ):
                raise ResourceInventoryError(
                    f'Azure distributed role {node.key} conflicts with its '
                    'provider placement contract.'
                )
    subscription_id = str(resources.get('azure_subscription_id') or '')
    resource_group = str(resources.get('azure_resource_group_name') or '')
    if not subscription_id or not resource_group:
        raise ResourceInventoryError(
            'Azure DeathStarBench cleanup is missing its provider ownership '
            'anchor.'
        )
    identity_specs = _validate_managed_expected_ids(
        resources,
        subscription_id=subscription_id,
        resource_group=resource_group,
        manifest=manifest,
        error_type=ResourceInventoryError,
    )
    allowed_ids = {
        _normalize_id(expected_id)
        for _, expected_id in identity_specs.values()
    }
    for node in inventory.nodes:
        if manifest.topology_id == DISTRIBUTED_TIERED_TOPOLOGY_ID:
            node_prefix = f'azure_dsb_{node.key.replace("-", "_")}_instance'
        elif node.key == 'runner':
            node_prefix = 'azure_instance'
        elif node.key == 'load-generator':
            node_prefix = 'azure_loadgen_instance'
        else:
            raise ResourceInventoryError(
                f'Azure DeathStarBench cleanup has unknown node {node.key}.'
            )
        expected_node_id = resources.get(f'{node_prefix}_expected_id')
        expected_node_name = resources.get(f'{node_prefix}_name')
        if (
            not node.provider_resource_id
            or not expected_node_id
            or _normalize_id(node.provider_resource_id)
            != _normalize_id(expected_node_id)
            or not node.provider_resource_name
            or node.provider_resource_name != expected_node_name
        ):
            raise ResourceInventoryError(
                f'Azure DeathStarBench node {node.key} conflicts with its '
                'persisted provider identity.'
            )
        for storage in node.storage:
            expected_storage_key = None
            if (
                manifest.topology_id == DISTRIBUTED_TIERED_TOPOLOGY_ID
                and node.key == 'database'
                and storage.key == 'database-data'
            ):
                expected_storage_key = 'azure_dsb_database_data_disk'
            elif (
                manifest.topology_id == SINGLE_HOST_TOPOLOGY_ID
                and node.key == 'runner'
                and storage.key == 'benchmark'
            ):
                expected_storage_key = 'azure_data_disk'
            if expected_storage_key is None:
                if storage.provider_resource_id:
                    raise ResourceInventoryError(
                        'Azure DeathStarBench cleanup has unknown persisted '
                        f'storage {storage.key}.'
                    )
                continue
            expected_storage_id = resources.get(
                f'{expected_storage_key}_expected_id'
            )
            expected_storage_name = resources.get(f'{expected_storage_key}_name')
            if (
                not storage.provider_resource_id
                or not expected_storage_id
                or _normalize_id(storage.provider_resource_id)
                != _normalize_id(expected_storage_id)
                or not storage.provider_resource_name
                or storage.provider_resource_name != expected_storage_name
            ):
                raise ResourceInventoryError(
                    f'Azure DeathStarBench storage {storage.key} conflicts '
                    'with its persisted provider identity.'
                )
        for identity in (node.provider_resource_id,):
            if _normalize_id(identity) not in allowed_ids:
                raise ResourceInventoryError(
                    'Azure DeathStarBench cleanup inventory contains an '
                    'identity outside its persisted resource allowlist.'
                )
    return manifest, inventory


def _status_code(exc: BaseException) -> int | None:
    for candidate in (
        getattr(exc, 'status_code', None),
        getattr(getattr(exc, 'response', None), 'status_code', None),
    ):
        if isinstance(candidate, int):
            return candidate
    return None


def _not_found(exc: BaseException) -> bool:
    return (
        _status_code(exc) == 404
        or exc.__class__.__name__ in {'ResourceNotFoundError', 'NotFound'}
    )


def _wait(operation: Any):
    if operation is None:
        return None
    return operation.result() if hasattr(operation, 'result') else operation


def _normalize_id(value: Any) -> str:
    return '/' + str(value or '').strip().strip('/').lower()


def _expected_id(subscription_id: str, resource_group: str, suffix: str = '') -> str:
    root = f'/subscriptions/{subscription_id}/resourceGroups/{resource_group}'
    return root + (f'/providers/{suffix}' if suffix else '')


def _active_managed_identity_specs(
    resources: Mapping[str, Any],
    *,
    subscription_id: str,
    resource_group: str,
    manifest: DeathStarBenchTopologyManifest | None,
) -> dict[str, tuple[str, str]]:
    """Return exact name/ID pairs that this run is allowed to own.

    The global expected-ID schema only identifies fields understood by this
    provider.  Cleanup needs a narrower contract: the fields applicable to
    this run's topology, each bound to its deterministic Azure name and
    resource type.
    """

    specs: dict[str, tuple[str, str]] = {}

    def add(prefix: str, name: str, suffix: str):
        specs[f'{prefix}_expected_id'] = (
            name,
            _expected_id(subscription_id, resource_group, suffix),
        )

    def add_vm(prefix: str, name: str, *, public_ip: bool):
        add(
            prefix,
            name,
            f'Microsoft.Compute/virtualMachines/{name}',
        )
        nic_name = f'{name}-nic'
        add(
            f'{prefix}_nic',
            nic_name,
            f'Microsoft.Network/networkInterfaces/{nic_name}',
        )
        os_disk_name = f'{name}-os'
        add(
            f'{prefix}_os_disk',
            os_disk_name,
            f'Microsoft.Compute/disks/{os_disk_name}',
        )
        if public_ip:
            public_ip_name = f'{name}-pip'
            add(
                f'{prefix}_public_ip',
                public_ip_name,
                f'Microsoft.Network/publicIPAddresses/{public_ip_name}',
            )

    if manifest is not None and (
        manifest.topology_id == DISTRIBUTED_TIERED_TOPOLOGY_ID
    ):
        vnet_name = f'{resource_group}-dsb-vnet'
        add(
            'azure_dsb_cluster_nsg',
            f'{resource_group}-cluster-nsg',
            'Microsoft.Network/networkSecurityGroups/'
            f'{resource_group}-cluster-nsg',
        )
        add(
            'azure_dsb_loadgen_nsg',
            f'{resource_group}-loadgen-nsg',
            'Microsoft.Network/networkSecurityGroups/'
            f'{resource_group}-loadgen-nsg',
        )
        add(
            'azure_dsb_vnet',
            vnet_name,
            f'Microsoft.Network/virtualNetworks/{vnet_name}',
        )
        nat_public_ip_name = f'{resource_group}-cluster-nat-pip'
        add(
            'azure_dsb_cluster_nat_public_ip',
            nat_public_ip_name,
            f'Microsoft.Network/publicIPAddresses/{nat_public_ip_name}',
        )
        nat_gateway_name = f'{resource_group}-cluster-nat'
        add(
            'azure_dsb_cluster_nat_gateway',
            nat_gateway_name,
            f'Microsoft.Network/natGateways/{nat_gateway_name}',
        )
        for key, subnet_name in (
            ('cluster', 'cluster-subnet'),
            ('loadgen', 'loadgen-subnet'),
        ):
            add(
                f'azure_dsb_{key}_subnet',
                subnet_name,
                f'Microsoft.Network/virtualNetworks/{vnet_name}/subnets/'
                f'{subnet_name}',
            )
        disk_name = f'{resource_group}-database-data'
        add(
            'azure_dsb_database_data_disk',
            disk_name,
            f'Microsoft.Compute/disks/{disk_name}',
        )
        node_names = {
            'control': f'{resource_group}-control',
            'database': f'{resource_group}-database',
            'cache': f'{resource_group}-cache',
            'application': f'{resource_group}-application',
            'load-generator': f'{resource_group}-loadgen',
        }
        for node_key, name in node_names.items():
            add_vm(
                f'azure_dsb_{node_key.replace("-", "_")}_instance',
                name,
                public_ip=node_key in DISTRIBUTED_PUBLIC_IP_NODES,
            )
        return specs

    nsg_name = f'{resource_group}-nsg'
    vnet_name = f'{resource_group}-vnet'
    subnet_name = 'benchmark-subnet'
    add(
        'azure_nsg',
        nsg_name,
        f'Microsoft.Network/networkSecurityGroups/{nsg_name}',
    )
    add(
        'azure_vnet',
        vnet_name,
        f'Microsoft.Network/virtualNetworks/{vnet_name}',
    )
    add(
        'azure_subnet',
        subnet_name,
        f'Microsoft.Network/virtualNetworks/{vnet_name}/subnets/{subnet_name}',
    )
    add_vm(
        'azure_instance',
        f'{resource_group}-runner',
        public_ip=True,
    )
    web_benchmarks = resources.get('azure_web_benchmarks')
    compact_load_generator = (
        manifest is not None
        and any(node.key == 'load-generator' for node in manifest.nodes)
    )
    if compact_load_generator or (
        isinstance(web_benchmarks, (list, tuple)) and bool(web_benchmarks)
    ):
        add_vm(
            'azure_loadgen_instance',
            f'{resource_group}-loadgen',
            public_ip=True,
        )
    protocols = resources.get('azure_iperf3_protocols')
    if isinstance(protocols, (list, tuple)) and bool(protocols):
        add_vm(
            'azure_peer_instance',
            f'{resource_group}-iperf-peer',
            public_ip=True,
        )
    if resources.get('azure_data_disk_type'):
        disk_name = f'{resource_group}-data'
        add(
            'azure_data_disk',
            disk_name,
            f'Microsoft.Compute/disks/{disk_name}',
        )
    return specs


def _validate_managed_expected_ids(
    resources: Mapping[str, Any],
    *,
    subscription_id: str,
    resource_group: str,
    manifest: DeathStarBenchTopologyManifest | None,
    error_type: type[Exception] = RuntimeError,
) -> dict[str, tuple[str, str]]:
    specs = _active_managed_identity_specs(
        resources,
        subscription_id=subscription_id,
        resource_group=resource_group,
        manifest=manifest,
    )
    persisted = {
        key for key in resources
        if key.startswith('azure_')
        and key.endswith('_expected_id')
        and key != 'azure_resource_group_expected_id'
    }
    unknown = persisted - AZURE_MANAGED_EXPECTED_ID_KEYS
    if unknown:
        raise error_type(
            'Azure cleanup contains unknown expected-resource identity fields.'
        )
    inapplicable = persisted - set(specs)
    if inapplicable:
        raise error_type(
            'Azure cleanup contains resource identities that are not part of '
            'the active topology.'
        )
    for key in persisted:
        expected_name, expected_id = specs[key]
        prefix = key.removesuffix('_expected_id')
        saved_name = resources.get(f'{prefix}_name')
        saved_id = resources.get(key)
        if (
            saved_name != expected_name
            or _normalize_id(saved_id) != _normalize_id(expected_id)
        ):
            raise error_type(
                f'Azure cleanup identity {key} conflicts with its exact '
                'deterministic name or resource type.'
            )
    return specs


def normalize_architecture(value: Any) -> str:
    aliases = {
        'amd64': 'x86_64',
        'x64': 'x86_64',
        'x86-64': 'x86_64',
        'x86_64': 'x86_64',
        'arm64': 'arm64',
        'aarch64': 'arm64',
    }
    normalized = aliases.get(str(value or '').strip().lower())
    if normalized not in SUPPORTED_ARCHITECTURES:
        raise ValueError(f'Unsupported Azure VM architecture {value!r}.')
    return normalized


class _AzureCliSubscriptions:
    """Read accounts from azure-cli and locations from the subscription SDK."""

    def __init__(self, credential=None):
        self.credential = credential

    @staticmethod
    def _run(*arguments: str) -> Any:
        try:
            result = subprocess.run(
                ('az', *arguments, '--only-show-errors', '--output', 'json'),
                check=True,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as exc:  # pragma: no cover - environment guard
            raise RuntimeError(
                'Azure support requires azure-cli. Install it and run az login.'
            ) from exc
        except subprocess.CalledProcessError as exc:  # pragma: no cover - live CLI
            detail = (exc.stderr or exc.stdout or '').strip()
            raise RuntimeError(
                'Azure CLI account discovery failed. Run az login and verify '
                f'the selected subscription. {detail}'
            ) from exc
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:  # pragma: no cover - live CLI
            raise RuntimeError('Azure CLI returned invalid account JSON.') from exc

    def list(self):
        return self._run('account', 'list', '--all')

    def list_locations(self, subscription_id: str):
        try:
            from azure.identity import AzureCliCredential
            from azure.mgmt.subscription import SubscriptionClient
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise RuntimeError(
                'Azure location discovery requires azure-identity and '
                'azure-mgmt-subscription.'
            ) from exc
        credential = self.credential or AzureCliCredential(
            subscription=subscription_id
        )
        client = SubscriptionClient(credential)
        try:
            return list(client.subscriptions.list_locations(subscription_id))
        finally:
            client.close()


class _AzureCliMarketplaceTerms:
    """Read marketplace agreement state without accepting legal terms."""

    def __init__(self, subscription_id: str):
        self.subscription_id = str(subscription_id)

    def show(self, urn: str):
        return _AzureCliSubscriptions._run(
            'vm', 'image', 'terms', 'show', '--urn', urn,
            '--subscription', self.subscription_id,
        )


def _subscription_operations(client: Any) -> Any:
    return getattr(client, 'subscriptions', client)


def _accounts(client: Any) -> list[Any]:
    if client is None:
        return []
    operations = _subscription_operations(client)
    return _iter(operations.list())


def _account_id(account: Any) -> str:
    return str(
        _value(account, 'subscription_id')
        or _value(account, 'id')
        or ''
    ).strip()


def _resolve_subscription(subscription_id: str | None, subscriptions: Any):
    requested = str(subscription_id or '').strip()
    accounts = _accounts(subscriptions)
    if requested:
        matching = [item for item in accounts if _account_id(item) == requested]
        if accounts and not matching:
            raise ValueError(
                f'Azure subscription {requested!r} is not available to the '
                'current Azure CLI account.'
            )
        account = matching[0] if matching else {'id': requested, 'name': requested}
    else:
        defaults = [
            item for item in accounts
            if bool(_value(item, 'is_default', _value(item, 'isDefault', False)))
        ]
        if len(defaults) == 1:
            account = defaults[0]
        elif len(accounts) == 1:
            account = accounts[0]
        else:
            raise ValueError(
                'Select an Azure subscription or set the Azure CLI default '
                'with az account set --subscription.'
            )
    resolved = _account_id(account)
    if not resolved:
        raise RuntimeError('Azure account discovery returned no subscription ID.')
    state = str(_value(account, 'state', 'Enabled') or 'Enabled').lower()
    if state != 'enabled':
        raise RuntimeError(
            f'Azure subscription {resolved} is not enabled (state: {state}).'
        )
    return resolved, account


def create_clients(subscription_id: str, credential=None) -> dict[str, Any]:
    """Create fresh Azure SDK clients backed by Azure CLI authentication."""
    subscription_id = str(subscription_id or '').strip()
    if not subscription_id:
        raise ValueError('An Azure subscription ID is required.')
    try:
        from azure.identity import AzureCliCredential
        from azure.mgmt.compute import ComputeManagementClient
        from azure.mgmt.network import NetworkManagementClient
        try:
            from azure.mgmt.resource import ResourceManagementClient
        except ImportError:
            # azure-mgmt-resource 26 is a namespace package.
            from azure.mgmt.resource.resources import ResourceManagementClient
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            'Azure support requires azure-identity, azure-mgmt-resource, '
            'azure-mgmt-compute, and azure-mgmt-network.'
        ) from exc
    credential = credential or AzureCliCredential(subscription=subscription_id)
    return {
        'credential': credential,
        'subscriptions': _AzureCliSubscriptions(credential),
        'marketplace_terms': _AzureCliMarketplaceTerms(subscription_id),
        'resource': ResourceManagementClient(credential, subscription_id),
        'compute': ComputeManagementClient(credential, subscription_id),
        'network': NetworkManagementClient(credential, subscription_id),
    }


def _runtime(
    subscription_id: str | None,
    credential=None,
    clients: Mapping[str, Any] | None = None,
):
    if clients is not None:
        resolved, account = _resolve_subscription(
            subscription_id, clients.get('subscriptions')
        )
        return resolved, account, clients
    subscriptions = _AzureCliSubscriptions(credential)
    resolved, account = _resolve_subscription(subscription_id, subscriptions)
    created = create_clients(resolved, credential)
    created['subscriptions'] = subscriptions
    return resolved, account, created


def _providers(clients: Mapping[str, Any]) -> Any:
    service = clients.get('providers')
    if service is not None:
        return service
    resource = clients.get('resource')
    return getattr(resource, 'providers', None)


def _resource_groups(clients: Mapping[str, Any]) -> Any:
    service = clients.get('resource_groups')
    if service is not None:
        return service
    resource = clients.get('resource')
    return getattr(resource, 'resource_groups', None)


def _resource_inventory(clients: Mapping[str, Any]) -> Any:
    service = clients.get('resources')
    if service is not None:
        return service
    resource = clients.get('resource')
    return getattr(resource, 'resources', None)


def _compute_service(clients: Mapping[str, Any], name: str) -> Any:
    service = clients.get(name)
    if service is not None:
        return service
    return getattr(clients.get('compute'), name, None)


def _network_service(clients: Mapping[str, Any], name: str) -> Any:
    service = clients.get(name)
    if service is not None:
        return service
    return getattr(clients.get('network'), name, None)


def _require_service(service: Any, label: str) -> Any:
    if service is None:
        raise RuntimeError(f'Azure SDK client does not expose {label}.')
    return service


def _require_registered_resource_providers(clients: Mapping[str, Any]):
    providers = _require_service(_providers(clients), 'resource providers')
    missing = []
    for namespace in ('Microsoft.Compute', 'Microsoft.Network'):
        provider = providers.get(namespace)
        state = str(_value(provider, 'registration_state', '') or '').lower()
        if state != 'registered':
            missing.append(namespace)
    if missing:
        raise RuntimeError(
            'Register the required Azure resource providers before '
            f'benchmarking: {", ".join(missing)}.'
        )


def _require_distributed_candidate_services(clients: Mapping[str, Any]):
    services = (
        (
            _resource_groups(clients),
            'resource groups',
            ('get', 'create_or_update', 'begin_delete'),
        ),
        (
            _resource_inventory(clients),
            'resource-group resource inventory',
            ('list_by_resource_group',),
        ),
        (
            _network_service(clients, 'network_security_groups'),
            'network security groups',
            ('get', 'begin_create_or_update'),
        ),
        (
            _network_service(clients, 'virtual_networks'),
            'virtual networks',
            ('get', 'begin_create_or_update'),
        ),
        (
            _network_service(clients, 'subnets'),
            'virtual network subnets',
            ('get', 'begin_create_or_update'),
        ),
        (
            _network_service(clients, 'public_ip_addresses'),
            'public IP addresses',
            ('get', 'begin_create_or_update'),
        ),
        (
            _network_service(clients, 'nat_gateways'),
            'NAT gateways',
            ('get', 'begin_create_or_update'),
        ),
        (
            _network_service(clients, 'network_interfaces'),
            'network interfaces',
            ('get', 'begin_create_or_update'),
        ),
        (
            _compute_service(clients, 'disks'),
            'managed disks',
            ('get', 'begin_create_or_update'),
        ),
        (
            _compute_service(clients, 'virtual_machines'),
            'virtual machines',
            ('get', 'begin_create_or_update'),
        ),
    )
    for candidate, label, operations in services:
        service = _require_service(candidate, label)
        for operation in operations:
            if not callable(getattr(service, operation, None)):
                raise RuntimeError(
                    f'Azure SDK {label} client does not expose {operation}.'
                )


def bootstrap(
    subscription_id: str | None = None,
    *,
    default_region: str = DEFAULT_REGION,
    credential=None,
    clients=None,
):
    """Validate the selected subscription and list its physical locations."""
    subscription_id, account, clients = _runtime(
        subscription_id, credential, clients
    )
    _require_registered_resource_providers(clients)

    subscription_ops = _subscription_operations(clients.get('subscriptions'))
    if not hasattr(subscription_ops, 'list_locations'):
        raise RuntimeError('Azure subscription client cannot list locations.')
    locations = _iter(subscription_ops.list_locations(subscription_id))
    regions = []
    for location in locations:
        region_type = str(
            _value(
                location,
                'region_type',
                _value(location, 'metadata', {}).get('regionType', '')
                if isinstance(_value(location, 'metadata', {}), Mapping)
                else '',
            )
            or ''
        ).lower()
        # Logical and extended locations cannot host normal benchmark VMs.
        if region_type and region_type != 'physical':
            continue
        name = str(_value(location, 'name', '') or '').strip()
        if name:
            regions.append(name)
    regions = sorted(set(regions), key=str.casefold)
    selected_default = (
        default_region if default_region in regions
        else (regions[0] if regions else default_region)
    )
    return {
        'provider': 'azure',
        'subscription_id': subscription_id,
        'subscription_name': str(_value(account, 'name', '') or ''),
        'tenant_id': str(
            _value(account, 'tenant_id', _value(account, 'tenantId', '')) or ''
        ),
        'default_region': selected_default,
        'regions': regions,
        'subscriptions': [
            {
                'id': _account_id(item),
                'name': str(_value(item, 'name', '') or _account_id(item)),
                'is_default': bool(
                    _value(item, 'is_default', _value(item, 'isDefault', False))
                ),
            }
            for item in _accounts(clients.get('subscriptions'))
            if _account_id(item)
        ],
    }


def _sku_capabilities(sku: Any) -> dict[str, str]:
    result = {}
    for capability in _value(sku, 'capabilities', ()) or ():
        name = str(_value(capability, 'name', '') or '').strip().lower()
        if name:
            result[name] = str(_value(capability, 'value', '') or '').strip()
    return result


def _truth(value: Any) -> bool | None:
    normalized = str(value or '').strip().lower()
    if normalized in {'true', '1', 'yes'}:
        return True
    if normalized in {'false', '0', 'no'}:
        return False
    return None


def _positive_capability_integer(value: Any) -> int | None:
    """Parse one positive integer SKU capability without numeric coercion."""
    normalized = str(value or '').strip()
    if not re.fullmatch(r'[1-9]\d*', normalized):
        return None
    return int(normalized)


def _local_nvme_storage_summary(
    capabilities: Mapping[str, str],
) -> dict[str, int | float | bool]:
    """Return only a self-consistent Azure local-NVMe SKU profile.

    Azure managed OS and data disks can also use an NVMe controller, so the
    controller capability alone does not prove local storage.  The dedicated
    total and per-device local-NVMe capacities are required here.  Azure can
    round each published device size by one MiB, hence the narrow one-MiB per
    device reconciliation tolerance.
    """
    unavailable: dict[str, int | float | bool] = {
        'local_nvme_supported': False,
        'local_nvme_disk_count': 0,
        'local_nvme_disk_size_gb': 0,
        'local_nvme_total_size_gb': 0,
    }
    total_mib = _positive_capability_integer(
        capabilities.get('nvmedisksizeinmib')
    )
    per_disk_mib = _positive_capability_integer(
        capabilities.get('nvmesizeperdiskinmib')
    )
    if total_mib is None or per_disk_mib is None:
        return unavailable
    disk_count = round(total_mib / per_disk_mib)
    if (
        disk_count <= 0
        or abs(total_mib - disk_count * per_disk_mib) > disk_count
    ):
        return unavailable
    explicit_count_value = capabilities.get('nvmediskcount')
    if explicit_count_value is not None:
        explicit_count = _positive_capability_integer(explicit_count_value)
        if explicit_count is None or explicit_count != disk_count:
            return unavailable
    return {
        'local_nvme_supported': True,
        'local_nvme_disk_count': disk_count,
        'local_nvme_disk_size_gb': round(per_disk_mib / 1024, 6),
        'local_nvme_total_size_gb': round(total_mib / 1024, 6),
    }


def _sku_zones(sku: Any, region: str) -> set[str]:
    zones: set[str] = set()
    for info in _value(sku, 'location_info', ()) or ():
        location = str(_value(info, 'location', '') or '')
        if location.casefold() != region.casefold():
            continue
        zones.update(str(item) for item in (_value(info, 'zones', ()) or ()))
    return zones


def _restriction_values(restriction: Any) -> set[str]:
    values = {str(item) for item in (_value(restriction, 'values', ()) or ())}
    info = _value(restriction, 'restriction_info', {}) or {}
    values.update(str(item) for item in (_value(info, 'zones', ()) or ()))
    return values


def _sku_restricted(sku: Any, region: str, zone: str | None = None) -> bool:
    for restriction in _value(sku, 'restrictions', ()) or ():
        kind = str(_value(restriction, 'type', '') or '').lower()
        values = _restriction_values(restriction)
        if kind == 'location' and (
            not values or region.casefold() in {item.casefold() for item in values}
        ):
            return True
        if kind == 'zone' and zone is not None and (
            not values or str(zone) in values
        ):
            return True
    return False


def _resource_skus(clients: Mapping[str, Any], region: str) -> list[Any]:
    service = _require_service(
        _compute_service(clients, 'resource_skus'), 'Compute resource SKUs'
    )
    try:
        response = service.list(filter=f"location eq '{region}'")
    except TypeError:  # simple mocks and older SDK releases
        response = service.list()
    return _iter(response)


def _premium_v2_available(
    clients: Mapping[str, Any], region: str, zone: str | None
) -> bool:
    if not zone:
        return False
    for sku in _resource_skus(clients, region):
        if str(_value(sku, 'resource_type', '') or '').lower() != 'disks':
            continue
        if str(_value(sku, 'name', '') or '').casefold() != (
            PREMIUM_V2_DISK_TYPE.casefold()
        ):
            continue
        if not _sku_in_region(sku, region) or _sku_restricted(sku, region, zone):
            continue
        if str(zone) in _sku_zones(sku, region):
            return True
    return False


def _sku_in_region(sku: Any, region: str) -> bool:
    return region.casefold() in {
        str(item).casefold() for item in (_value(sku, 'locations', ()) or ())
    }


def _sku_summary(sku: Any, region: str) -> dict[str, Any] | None:
    if str(_value(sku, 'resource_type', '') or '').lower() != 'virtualmachines':
        return None
    if not _sku_in_region(sku, region):
        return None
    name = str(_value(sku, 'name', '') or '')
    if not name.startswith('Standard_'):
        return None
    capabilities = _sku_capabilities(sku)
    try:
        architecture = normalize_architecture(
            capabilities.get('cpuarchitecturetype')
        )
        vcpus = int(float(capabilities.get('vcpus', '0')))
        memory_gb = float(capabilities.get('memorygb', '0'))
    except (TypeError, ValueError):
        return None
    if vcpus < 1 or memory_gb < 1:
        return None
    gpu_count = int(float(capabilities.get('gpus', '0') or 0))
    if gpu_count:
        return None
    summary = {
        'name': name,
        'vm_size': name,
        'machine_type': name,
        'instance_type': name,
        'shape': name,
        'vcpu': vcpus,
        'vcpus': vcpus,
        'ocpus': vcpus,
        'memory_gb': memory_gb,
        'architecture': architecture,
        'family': str(_value(sku, 'family', '') or ''),
        'zones': sorted(_sku_zones(sku, region)),
        'premium_io': _truth(capabilities.get('premiumio')),
        'premium_io_v2': _truth(
            capabilities.get('premiumiov2supported')
            or capabilities.get('premiumiov2')
        ),
        'accelerated_networking': _truth(
            capabilities.get('acceleratednetworkingenabled')
        ),
        'maximum_data_disks': int(float(
            capabilities.get('maxdatadiskcount', '0') or 0
        )),
        **_local_nvme_storage_summary(capabilities),
    }
    capacity_gbps = None
    bandwidth_mbps = capabilities.get('maxnetworkbandwidth')
    if bandwidth_mbps is not None:
        try:
            candidate = round(float(bandwidth_mbps) / 1000, 3)
            if math.isfinite(candidate) and candidate > 0:
                capacity_gbps = candidate
        except (TypeError, ValueError):
            pass
    if capacity_gbps is None:
        capacity_gbps = DOCUMENTED_MAX_NETWORK_BANDWIDTH_GBPS.get(
            name.casefold()
        )
    if capacity_gbps is not None:
        summary['network_bandwidth_gbps'] = capacity_gbps
        summary['network_capacity_kind'] = 'maximum'
    return summary


def placement(
    subscription_id: str,
    region: str,
    *,
    credential=None,
    clients=None,
):
    """List zones usable by at least one unrestricted VM SKU."""
    subscription_id, _, clients = _runtime(subscription_id, credential, clients)
    zones: set[str] = set()
    for sku in _resource_skus(clients, region):
        if _sku_summary(sku, region) is None or _sku_restricted(sku, region):
            continue
        restricted_zones = set()
        for restriction in _value(sku, 'restrictions', ()) or ():
            if str(_value(restriction, 'type', '') or '').lower() == 'zone':
                restricted_zones.update(_restriction_values(restriction))
        zones.update(_sku_zones(sku, region) - restricted_zones)
    return {
        'availability_zones': [
            {'name': zone, 'zone_id': zone, 'state': 'available'}
            for zone in sorted(zones)
        ]
    }


def vm_sizes(
    subscription_id: str,
    region: str,
    zone: str | None = None,
    *,
    credential=None,
    clients=None,
):
    """List fixed-capacity x86/Arm sizes launchable by this subscription."""
    subscription_id, _, clients = _runtime(subscription_id, credential, clients)
    del subscription_id
    selected_zone = str(zone or '').strip() or None
    items = []
    for sku in _resource_skus(clients, region):
        summary = _sku_summary(sku, region)
        if summary is None or _sku_restricted(sku, region, selected_zone):
            continue
        zones = set(summary['zones'])
        if selected_zone and selected_zone not in zones:
            continue
        items.append(summary)
    items.sort(key=lambda item: item['vm_size'].casefold())
    return {'items': items}


# Cross-provider callers historically use both of these names.
machine_types = vm_sizes
instance_types = vm_sizes


def _version_key(value: str) -> tuple[Any, ...]:
    return tuple(
        (0, int(part)) if part.isdigit() else (1, part.casefold())
        for part in re.split(r'([0-9]+)', value)
    )


def _resolve_rocky_linux_9_image_version(
    subscription_id: str,
    region: str,
    architecture: str,
    version: str,
    *,
    credential=None,
    clients=None,
):
    """Resolve one previously pinned Rocky image version exactly."""

    architecture = normalize_architecture(architecture)
    version = str(version or '').strip()
    if (
        not version
        or version.lower() == 'latest'
        or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', version)
    ):
        raise ValueError('Azure Rocky Linux image version must be immutable.')
    subscription_id, _, clients = _runtime(subscription_id, credential, clients)
    del subscription_id
    coordinates = ROCKY_IMAGES[architecture]
    service = _require_service(
        _compute_service(clients, 'virtual_machine_images'),
        'virtual machine images',
    )
    image = service.get(
        region,
        coordinates['publisher'],
        coordinates['offer'],
        coordinates['sku'],
        version,
    )
    image_id = str(_value(image, 'id', '') or '')
    expected_tail = (
        f'/publishers/{coordinates["publisher"]}/artifacttypes/vmimage/'
        f'offers/{coordinates["offer"]}/skus/{coordinates["sku"]}/'
        f'versions/{version}'
    )
    if not image_id or not _normalize_id(image_id).endswith(
        _normalize_id(expected_tail)
    ):
        raise RuntimeError(
            'Azure Rocky Linux image discovery returned incomplete or '
            'conflicting immutable identity metadata.'
        )
    urn = ':'.join((
        coordinates['publisher'],
        coordinates['offer'],
        coordinates['sku'],
        version,
    ))
    return {
        **coordinates,
        'version': version,
        'image_id': image_id,
        'name': urn,
        'urn': urn,
        'architecture': architecture,
        'image_reference': {**coordinates, 'version': version},
    }


def resolve_rocky_linux_9_image(
    subscription_id: str,
    region: str,
    architecture: str,
    *,
    credential=None,
    clients=None,
):
    """Resolve the latest marketplace Rocky 9 image to an immutable version."""
    architecture = normalize_architecture(architecture)
    subscription_id, _, clients = _runtime(subscription_id, credential, clients)
    coordinates = ROCKY_IMAGES[architecture]
    service = _require_service(
        _compute_service(clients, 'virtual_machine_images'),
        'virtual machine images',
    )
    versions = _iter(service.list(
        region,
        coordinates['publisher'],
        coordinates['offer'],
        coordinates['sku'],
    ))
    names = sorted(
        {
            str(_value(item, 'name', '') or '')
            for item in versions
            if str(_value(item, 'name', '') or '').lower() != 'latest'
        },
        key=_version_key,
    )
    if not names:
        raise RuntimeError(
            f'Azure returned no Rocky Linux 9 {architecture} image versions '
            f'in {region}.'
        )
    version = names[-1]
    return _resolve_rocky_linux_9_image_version(
        subscription_id=subscription_id,
        region=region,
        architecture=architecture,
        version=version,
        clients=clients,
    )


latest_rocky_linux_9_image = resolve_rocky_linux_9_image


def _distributed_candidate_image(
    resources: Mapping[str, Any],
    *,
    key_prefix: str,
    subscription_id: str,
    region: str,
    architecture: str,
    clients: Mapping[str, Any],
) -> dict[str, Any]:
    fields = {
        'version': f'{key_prefix}_version',
        'urn': f'{key_prefix}_urn',
        'id': f'{key_prefix}_id',
        'architecture': f'{key_prefix}_architecture',
    }
    present = {name: resources.get(key) for name, key in fields.items()}
    if any(value is not None for value in present.values()):
        if not all(isinstance(value, str) and value for value in present.values()):
            raise RuntimeError(
                'Azure distributed candidate image metadata is incomplete.'
            )
        if normalize_architecture(present['architecture']) != architecture:
            raise RuntimeError(
                'Azure distributed candidate image architecture conflicts '
                'with the saved run.'
            )
        image = _resolve_rocky_linux_9_image_version(
            subscription_id,
            region,
            architecture,
            present['version'],
            clients=clients,
        )
        if (
            image['urn'] != present['urn']
            or _normalize_id(image['image_id']) != _normalize_id(present['id'])
        ):
            raise RuntimeError(
                'Azure distributed candidate image identity conflicts with '
                'the saved run.'
            )
        return image
    return resolve_rocky_linux_9_image(
        subscription_id,
        region,
        architecture,
        clients=clients,
    )


def _require_marketplace_terms(
    clients: Mapping[str, Any], *images: Mapping[str, Any] | None
):
    service = clients.get('marketplace_terms')
    if service is None:
        raise RuntimeError(
            'Azure marketplace agreement discovery is unavailable; verify '
            'that azure-cli is installed.'
        )
    checked: set[str] = set()
    for image in images:
        if not image:
            continue
        urn = str(image['urn'])
        if urn in checked:
            continue
        checked.add(urn)
        agreement = service.show(urn)
        if _truth(_value(agreement, 'accepted')) is not True:
            raise RuntimeError(
                'Accept the Rocky Linux marketplace terms before '
                f'provisioning: az vm image terms accept --urn {urn}'
            )


def _plan_subscription(plan: Any) -> str:
    return str(
        _value(plan, 'azure_subscription_id')
        or _value(plan, 'subscription_id')
        or ''
    ).strip()


def _plan_region(plan: Any) -> str:
    return str(_value(plan, 'region', DEFAULT_REGION) or DEFAULT_REGION).strip()


def _plan_vm_size(plan: Any) -> str:
    return str(
        _value(plan, 'azure_vm_size')
        or _value(plan, 'vm_size')
        or _value(plan, 'machine_type')
        or _value(plan, 'instance_type')
        or _value(plan, 'shape')
        or ''
    ).strip()


def _storage_value(plan: Any, name: str, default: Any = None) -> Any:
    return _value(_value(plan, 'storage', {}) or {}, name, default)


def _selected_iperf3_protocols(plan: Any) -> tuple[str, ...]:
    if 'iperf3' not in set(_value(plan, 'benchmarks', ()) or ()):
        return ()
    options = _value(plan, 'iperf3', {}) or {}
    protocols = tuple(dict.fromkeys(
        str(item).strip().lower()
        for item in (_value(options, 'protocols', ()) or ())
        if str(item).strip()
    ))
    if not protocols:
        raise ValueError('Select at least one iperf3 protocol for Azure.')
    unsupported = sorted(set(protocols) - SUPPORTED_IPERF3_PROTOCOLS)
    if unsupported:
        raise ValueError(
            'Azure networking currently supports benchmark iperf3 over TCP '
            'and UDP only; unsupported selection: '
            + ', '.join(unsupported)
            + '.'
        )
    return protocols


def _selected_web_benchmarks(plan: Any) -> tuple[str, ...]:
    selected = set(_value(plan, 'benchmarks', ()) or ())
    return tuple(item for item in ('apachebench', 'deathstarbench') if item in selected)


def _deathstarbench_contract_ids(plan: Any) -> tuple[str, str]:
    options = _value(plan, 'deathstarbench', {}) or {}
    return (
        str(
            _value(options, 'topology_id', SINGLE_HOST_TOPOLOGY_ID)
            or SINGLE_HOST_TOPOLOGY_ID
        ),
        str(
            _value(options, 'runtime_id', PODMAN_COMPOSE_RUNTIME_ID)
            or PODMAN_COMPOSE_RUNTIME_ID
        ),
    )


def _validate_public_key(public_key: str) -> str:
    key = str(public_key or '').strip()
    if '\n' in key or '\r' in key:
        raise ValueError('The Azure SSH public key must contain exactly one key.')
    parts = key.split()
    algorithms = re.compile(
        r'^(?:ssh-(?:ed25519|rsa)|ecdsa-sha2-nistp(?:256|384|521)|'
        r'sk-ssh-ed25519@openssh\.com|'
        r'sk-ecdsa-sha2-nistp256@openssh\.com)$'
    )
    if len(parts) < 2 or not algorithms.fullmatch(parts[0]):
        raise ValueError('The Azure SSH public key is not a supported OpenSSH key.')
    try:
        base64.b64decode(parts[1], validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError('The Azure SSH public key payload is not valid base64.') from exc
    return f'{parts[0]} {parts[1]}'


def _job_name(job_id: str, role: str = '') -> str:
    raw = str(job_id or '').strip().lower()
    if not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,39}[a-z0-9])?', raw):
        raise ValueError(f'Job identifier {job_id!r} cannot form Azure names.')
    suffix = f'-{role}' if role else ''
    name = f'benchmark-{raw}{suffix}'
    if len(name) > 63:
        raise ValueError(f'Job identifier {job_id!r} is too long for Azure names.')
    return name


def _tags(job_id: str, role: str) -> dict[str, str]:
    return {
        'managed-by': MANAGED_BY,
        'benchmark-job': str(job_id).lower(),
        'benchmark-role': role,
    }


def _find_size(
    subscription_id: str,
    region: str,
    zone: str | None,
    name: str,
    clients: Mapping[str, Any],
) -> dict[str, Any]:
    for item in vm_sizes(subscription_id, region, zone, clients=clients)['items']:
        if item['vm_size'].casefold() == name.casefold():
            return item
    zone_text = f' in zone {zone}' if zone else ''
    raise ValueError(
        f'Azure VM size {name!r} is unavailable to this subscription in '
        f'{region}{zone_text}. Refresh size discovery or review Azure quota.'
    )


def _validate_capacity(plan: Any, details: Mapping[str, Any]):
    requested_vcpus = _value(plan, 'ocpus')
    requested_memory = _value(plan, 'memory_gb')
    if requested_vcpus is not None and float(requested_vcpus) != float(details['vcpu']):
        raise ValueError(
            f'{details["vm_size"]} has {details["vcpu"]} vCPUs, but the plan '
            f'requested {requested_vcpus}. Refresh Azure VM sizes.'
        )
    if requested_memory is not None and float(requested_memory) != float(
        details['memory_gb']
    ):
        raise ValueError(
            f'{details["vm_size"]} has {details["memory_gb"]:g} GiB, but the '
            f'plan requested {requested_memory}. Refresh Azure VM sizes.'
        )


def _select_loadgen(
    subscription_id: str,
    region: str,
    zone: str | None,
    clients: Mapping[str, Any],
) -> dict[str, Any]:
    available = {
        item['vm_size']: item
        for item in vm_sizes(subscription_id, region, zone, clients=clients)['items']
        if item['architecture'] == 'x86_64'
        and item['vcpu'] == 2
        and float(item['memory_gb']) == 8
    }
    for name in LOADGEN_VM_SIZES:
        if name in available:
            return available[name]
    raise ValueError(
        'Azure web tests require Standard_D2as_v7 as an available fixed '
        '2-vCPU/8-GiB x86 load generator in the selected region and zone.'
    )


def _validate_manifest_capacity(node: Any, details: Mapping[str, Any]):
    if node.selected_shape and (
        str(details['vm_size']).casefold() != node.selected_shape.casefold()
    ):
        raise ValueError(
            f'Azure role {node.key} resolved a VM size outside its topology '
            'contract.'
        )
    if (
        node.capacity.minimum_vcpus is not None
        and int(details['vcpu']) < node.capacity.minimum_vcpus
    ):
        raise ValueError(
            f'Azure VM size {details["vm_size"]} does not satisfy the '
            f'{node.key} vCPU capacity contract.'
        )
    if (
        node.capacity.minimum_memory_gib is not None
        and float(details['memory_gb']) < node.capacity.minimum_memory_gib
    ):
        raise ValueError(
            f'Azure VM size {details["vm_size"]} does not satisfy the '
            f'{node.key} memory capacity contract.'
        )
    required_architecture = node.capacity.required_architecture
    if (
        required_architecture is not None
        and details['architecture'] != required_architecture
    ):
        raise ValueError(
            f'Azure VM size {details["vm_size"]} does not satisfy the '
            f'{node.key} architecture contract.'
        )
    if node.architecture and details['architecture'] != node.architecture:
        raise ValueError(
            f'Azure VM size {details["vm_size"]} architecture conflicts with '
            f'the {node.key} topology contract.'
        )


def _distributed_role_details(
    manifest: DeathStarBenchTopologyManifest,
    *,
    subscription_id: str,
    region: str,
    zone: str | None,
    application_details: Mapping[str, Any],
    clients: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    details_by_key: dict[str, dict[str, Any]] = {}
    for node in manifest.nodes:
        details = (
            dict(application_details)
            if node.key == 'application'
            else _find_size(
                subscription_id,
                region,
                zone,
                DISTRIBUTED_ROLE_VM_SIZES[node.key],
                clients,
            )
        )
        _validate_manifest_capacity(node, details)
        details_by_key[node.key] = details
    return details_by_key


def _planned_azure_inventory(
    manifest: DeathStarBenchTopologyManifest,
    *,
    subscription_id: str,
    resource_group: str,
    zone: str | None,
    assignments: Mapping[str, Mapping[str, Any]],
    storage_by_node: Mapping[str, tuple[StorageResource, ...]] | None = None,
) -> RoleNodeInventory:
    inventory = manifest.planned_inventory('azure')
    storage_by_node = storage_by_node or {}
    for node in inventory.nodes:
        assignment = assignments[node.key]
        name = str(assignment['name'])
        expected_id = _expected_id(
            subscription_id,
            resource_group,
            f'Microsoft.Compute/virtualMachines/{name}',
        )
        storage_items = {item.key: item for item in node.storage}
        storage_items.update({
            item.key: item for item in storage_by_node.get(node.key, ())
        })
        storage = tuple(storage_items[key] for key in sorted(storage_items))
        inventory = inventory.upsert(RoleNode(
            key=node.key,
            role=node.role,
            provider_resource_id=expected_id,
            provider_resource_name=name,
            private_addresses=(
                (str(assignment['private_address']),)
                if assignment.get('private_address') else ()
            ),
            zone=zone,
            shape=str(assignment['details']['vm_size']),
            architecture=str(assignment['details']['architecture']),
            capacity_class=node.capacity_class,
            storage=storage,
            lifecycle_status='planned',
        ))
    return inventory


def _security_rules(
    protocols: Iterable[str], web_benchmarks: Iterable[str]
) -> list[dict[str, Any]]:
    def rule(name: str, **properties: Any) -> dict[str, Any]:
        return {'name': name, 'properties': properties}

    rules = [rule(
        'allow-ssh',
        priority=100,
        direction='Inbound',
        access='Allow',
        protocol='Tcp',
        sourcePortRange='*',
        destinationPortRange='22',
        sourceAddressPrefix=SSH_SOURCE_CIDR,
        destinationAddressPrefix='*',
    )]
    selected_protocols = set(protocols)
    # iperf3 uses its TCP listener/control path for both TCP and UDP tests.
    if selected_protocols:
        rules.append(rule(
            'allow-iperf-tcp', priority=110,
            direction='Inbound', access='Allow', protocol='Tcp',
            sourcePortRange='*', destinationPortRange='5201',
            sourceAddressPrefix=SUBNET_CIDR,
            destinationAddressPrefix=SUBNET_CIDR,
        ))
    if 'udp' in selected_protocols:
        rules.append(rule(
            'allow-iperf-udp', priority=111,
            direction='Inbound', access='Allow', protocol='Udp',
            sourcePortRange='*', destinationPortRange='5201',
            sourceAddressPrefix=SUBNET_CIDR,
            destinationAddressPrefix=SUBNET_CIDR,
        ))
    selected_web = set(web_benchmarks)
    ports = []
    if 'apachebench' in selected_web:
        ports.append('80')
    if 'deathstarbench' in selected_web:
        ports.extend(('5000', '8080'))
    if ports:
        rules.append(rule(
            'allow-web-loadgen', priority=120,
            direction='Inbound', access='Allow', protocol='Tcp',
            sourcePortRange='*', destinationPortRanges=ports,
            sourceAddressPrefix=SUBNET_CIDR,
            destinationAddressPrefix=SUBNET_CIDR,
        ))
    return rules


def _distributed_security_rules(
    manifest: DeathStarBenchTopologyManifest,
) -> dict[str, list[dict[str, Any]]]:
    """Build subnet-scoped rules without relying on Azure's broad defaults."""

    channels = {channel.key: channel for channel in manifest.network_policy.channels}

    def ports(key: str, protocol: str) -> tuple[str, ...]:
        channel = channels.get(key)
        if channel is None or channel.protocol != protocol:
            raise TopologyManifestError(
                f'Distributed Azure networking requires {key}/{protocol}.'
            )
        return tuple(str(port) for port in channel.ports)

    def rule(
        name: str,
        *,
        priority: int,
        access: str,
        protocol: str,
        sources: tuple[str, ...],
        destinations: tuple[str, ...],
        destination_ports: tuple[str, ...],
    ) -> dict[str, Any]:
        properties: dict[str, Any] = {
            'priority': priority,
            'direction': 'Inbound',
            'access': access,
            'protocol': protocol,
            'sourcePortRange': '*',
        }
        if len(sources) == 1:
            properties['sourceAddressPrefix'] = sources[0]
        else:
            properties['sourceAddressPrefixes'] = list(sources)
        if len(destinations) == 1:
            properties['destinationAddressPrefix'] = destinations[0]
        else:
            properties['destinationAddressPrefixes'] = list(destinations)
        if len(destination_ports) == 1:
            properties['destinationPortRange'] = destination_ports[0]
        else:
            properties['destinationPortRanges'] = list(destination_ports)
        return {'name': name, 'properties': properties}

    address = DISTRIBUTED_PRIVATE_ADDRESSES
    cluster_addresses = tuple(
        address[key] for key in ('control', 'database', 'cache', 'application')
    )
    worker_addresses = tuple(
        address[key] for key in ('database', 'cache', 'application')
    )
    public_cluster_addresses = tuple(
        address[key] for key in ('control', 'application')
    )
    cluster_rules = [
        rule(
            'allow-public-ssh',
            priority=100,
            access='Allow',
            protocol='Tcp',
            sources=(SSH_SOURCE_CIDR,),
            destinations=public_cluster_addresses,
            destination_ports=('22',),
        ),
        rule(
            'allow-private-management',
            priority=110,
            access='Allow',
            protocol='Tcp',
            sources=public_cluster_addresses,
            destinations=cluster_addresses,
            destination_ports=('22',),
        ),
        rule(
            'allow-k3s-control-plane',
            priority=120,
            access='Allow',
            protocol='Tcp',
            sources=worker_addresses,
            destinations=(address['control'],),
            destination_ports=ports(K3S_CONTROL_PLANE_CHANNEL, 'tcp'),
        ),
        rule(
            'allow-k3s-overlay',
            priority=130,
            access='Allow',
            protocol='Udp',
            sources=cluster_addresses,
            destinations=cluster_addresses,
            destination_ports=ports(K3S_OVERLAY_CHANNEL, 'udp'),
        ),
        rule(
            'allow-loadgen-frontend',
            priority=140,
            access='Allow',
            protocol='Tcp',
            sources=(address['load-generator'],),
            destinations=(address['application'],),
            destination_ports=ports(BENCHMARK_INGRESS_CHANNEL, 'tcp'),
        ),
        rule(
            'deny-unlisted-vnet-inbound',
            priority=4000,
            access='Deny',
            protocol='*',
            sources=('VirtualNetwork',),
            destinations=('*',),
            destination_ports=('*',),
        ),
    ]
    loadgen_rules = [
        rule(
            'allow-public-ssh',
            priority=100,
            access='Allow',
            protocol='Tcp',
            sources=(SSH_SOURCE_CIDR,),
            destinations=(address['load-generator'],),
            destination_ports=('22',),
        ),
        rule(
            'deny-unlisted-vnet-inbound',
            priority=4000,
            access='Deny',
            protocol='*',
            sources=('VirtualNetwork',),
            destinations=('*',),
            destination_ports=('*',),
        ),
    ]
    return {'cluster': cluster_rules, 'load-generator': loadgen_rules}


def _get_or_none(service: Any, *args):
    try:
        return service.get(*args)
    except Exception as exc:
        if _not_found(exc):
            return None
        raise


def _confirm_absent(
    service: Any,
    name: str,
    *,
    attempts: int,
    delay_seconds: float,
) -> bool:
    """Require repeated exact-name absence after an ambiguous ARM response."""

    attempts = int(attempts)
    delay_seconds = float(delay_seconds)
    if attempts < 2 or delay_seconds < 0:
        raise ValueError('Azure absence confirmation bounds are invalid.')
    for attempt in range(attempts):
        if _get_or_none(service, name) is not None:
            return False
        if attempt + 1 < attempts and delay_seconds:
            time.sleep(delay_seconds)
    return True


def _verify_group(
    group: Any,
    *,
    subscription_id: str,
    name: str,
    region: str,
    required_tags: Mapping[str, str],
    saved_id: str,
):
    _require_succeeded_provisioning(group, 'resource group')
    expected_id = _expected_id(subscription_id, name)
    actual_id = str(_value(group, 'id', '') or '')
    if (
        not actual_id
        or _normalize_id(actual_id) != _normalize_id(expected_id)
        or _normalize_id(saved_id) != _normalize_id(expected_id)
    ):
        raise RuntimeError(
            'Refusing Azure resource-group use because its immutable '
            'subscription/name identity differs from the saved run contract.'
        )
    if str(_value(group, 'location', '') or '').casefold() != region.casefold():
        raise RuntimeError(
            'Refusing Azure resource-group use because its location differs '
            'from the saved run contract.'
        )
    actual_tags = dict(_value(group, 'tags', {}) or {})
    if any(actual_tags.get(key) != value for key, value in required_tags.items()):
        raise RuntimeError(
            'Refusing Azure resource-group use because required ownership '
            'tags are missing or changed.'
        )


def _ensure_group(
    job: dict[str, Any],
    persist: PersistCallback | None,
    clients: Mapping[str, Any],
    subscription_id: str,
    region: str,
    name: str,
):
    resources = job.setdefault('resources', {})
    expected_id = _expected_id(subscription_id, name)
    required_tags = _tags(str(job['id']), 'resource-group')
    for key, expected in (
        ('azure_subscription_id', subscription_id),
        ('azure_resource_group_name', name),
        ('azure_resource_group_expected_id', expected_id),
    ):
        current = resources.get(key)
        if current and _normalize_id(current) != _normalize_id(expected):
            raise RuntimeError(
                f'Refusing Azure provisioning because saved {key} conflicts '
                'with this deterministic run contract.'
            )
    _record(
        job,
        persist,
        provider='azure',
        azure_subscription_id=subscription_id,
        azure_resource_group_name=name,
        azure_resource_group_expected_id=expected_id,
        azure_resource_group_tags=required_tags,
        azure_resource_group_create_ambiguous=True,
        region=region,
    )
    groups = _require_service(_resource_groups(clients), 'resource groups')
    existing = _get_or_none(groups, name)
    if existing is not None:
        _verify_group(
            existing,
            subscription_id=subscription_id,
            name=name,
            region=region,
            required_tags=required_tags,
            saved_id=expected_id,
        )
        group = existing
    else:
        try:
            group = groups.create_or_update(name, {
                'location': region,
                'tags': required_tags,
            })
        except Exception:
            reconciled = _get_or_none(groups, name)
            if reconciled is None:
                raise
            _verify_group(
                reconciled,
                subscription_id=subscription_id,
                name=name,
                region=region,
                required_tags=required_tags,
                saved_id=expected_id,
            )
            group = reconciled
    _verify_group(
        group,
        subscription_id=subscription_id,
        name=name,
        region=region,
        required_tags=required_tags,
        saved_id=expected_id,
    )
    _record(
        job,
        persist,
        azure_resource_group_id=str(_value(group, 'id')),
        azure_resource_group_create_ambiguous=False,
    )
    return group


_MISSING = object()


def _snake_case(name: str) -> str:
    # Preserve acronym groups used by ARM fields (for example ``diskSizeGB``,
    # ``diskIOPSReadWrite``, and ``privateIPAddress``) when projecting a wire
    # name onto an Azure SDK model attribute.
    words = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', words).lower()


def _observed_field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        if name in value:
            return value[name]
        snake_name = _snake_case(name)
        if snake_name in value:
            return value[snake_name]
        return _MISSING
    for candidate in (name, _snake_case(name)):
        if hasattr(value, candidate):
            return getattr(value, candidate)
    return _MISSING


def _provisioning_state(value: Any) -> str | None:
    properties = _observed_field(value, 'properties')
    candidates = (
        _observed_field(value, 'provisioningState'),
        (
            _observed_field(properties, 'provisioningState')
            if properties is not _MISSING else _MISSING
        ),
    )
    for candidate in candidates:
        if candidate is not _MISSING and candidate is not None:
            normalized = str(candidate).strip().casefold()
            if normalized:
                return normalized
    return None


def _require_succeeded_provisioning(value: Any, label: str):
    state = _provisioning_state(value)
    if state is not None and state != 'succeeded':
        raise RuntimeError(
            f'Azure {label} provisioning state is {state!r}, not succeeded.'
        )


def _matches_observed_spec(actual: Any, expected: Any) -> bool:
    """Compare a desired ARM body with its live SDK/mapping projection."""

    if isinstance(expected, Mapping):
        for key, expected_value in expected.items():
            actual_value = _observed_field(actual, key)
            # Some Azure SDK models flatten the wire-level ``properties``
            # envelope onto the model itself.
            if actual_value is _MISSING and key == 'properties':
                actual_value = actual
            if actual_value is _MISSING or not _matches_observed_spec(
                actual_value,
                expected_value,
            ):
                return False
        return True
    if isinstance(expected, (list, tuple)):
        if (
            isinstance(actual, (str, bytes, Mapping))
            or not isinstance(actual, Iterable)
        ):
            return False
        actual_items = list(actual)
        expected_items = list(expected)
        if len(actual_items) != len(expected_items):
            return False
        if expected_items and all(
            isinstance(item, Mapping) and 'name' in item
            for item in expected_items
        ):
            actual_by_name = {
                str(_observed_field(item, 'name')): item
                for item in actual_items
            }
            if set(actual_by_name) != {
                str(item['name']) for item in expected_items
            }:
                return False
            return all(
                _matches_observed_spec(actual_by_name[str(item['name'])], item)
                for item in expected_items
            )
        return all(
            _matches_observed_spec(actual_item, expected_item)
            for actual_item, expected_item in zip(actual_items, expected_items)
        )
    if isinstance(expected, str):
        return isinstance(actual, str) and actual.casefold() == expected.casefold()
    if isinstance(expected, (int, float)) and not isinstance(expected, bool):
        return (
            isinstance(actual, (int, float))
            and not isinstance(actual, bool)
            and float(actual) == float(expected)
        )
    return actual == expected


def _nic_public_ip_presence(resource: Any) -> bool | None:
    properties = _observed_field(resource, 'properties')
    if properties is _MISSING:
        properties = resource
    configurations = _observed_field(properties, 'ipConfigurations')
    if configurations is _MISSING:
        return None
    configurations = list(configurations or ())
    if len(configurations) != 1:
        return None
    configuration_properties = _observed_field(
        configurations[0],
        'properties',
    )
    if configuration_properties is _MISSING:
        configuration_properties = configurations[0]
    public_ip = _observed_field(configuration_properties, 'publicIPAddress')
    return public_ip is not _MISSING and public_ip is not None


def _ensure_child(
    job: dict[str, Any],
    persist: PersistCallback | None,
    *,
    prefix: str,
    name: str,
    expected_id: str,
    service: Any,
    resource_group: str,
    body: Mapping[str, Any],
):
    resources = job.setdefault('resources', {})
    spec_fingerprint = 'sha256:' + hashlib.sha256(
        json.dumps(
            body,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
            allow_nan=False,
        ).encode('utf-8')
    ).hexdigest()
    for key, expected in (
        (f'{prefix}_name', name),
        (f'{prefix}_expected_id', expected_id),
        (f'{prefix}_spec_fingerprint', spec_fingerprint),
    ):
        current = resources.get(key)
        if current and (
            (
                key.endswith('_expected_id')
                and _normalize_id(current) != _normalize_id(expected)
            )
            or (
                not key.endswith('_expected_id')
                and str(current) != str(expected)
            )
        ):
            raise RuntimeError(
                f'Refusing Azure provisioning because saved {prefix} identity '
                'conflicts with this run.'
            )
    _record(
        job,
        persist,
        **{
            f'{prefix}_name': name,
            f'{prefix}_expected_id': expected_id,
            f'{prefix}_spec_fingerprint': spec_fingerprint,
            f'{prefix}_create_ambiguous': True,
        },
    )

    def verify(resource: Any):
        _require_succeeded_provisioning(resource, prefix)
        actual_id = str(_value(resource, 'id', '') or '')
        if not actual_id or _normalize_id(actual_id) != _normalize_id(expected_id):
            raise RuntimeError(
                f'Azure returned conflicting immutable identity for {prefix}. '
                'The resource-group cleanup contract was retained.'
            )
        expected_tags = dict(body.get('tags') or {})
        actual_tags = dict(_value(resource, 'tags', {}) or {})
        if expected_tags and any(
            actual_tags.get(key) != value for key, value in expected_tags.items()
        ):
            raise RuntimeError(
                f'Azure returned {prefix} without the required ownership tags.'
            )
        if not _matches_observed_spec(resource, body):
            raise RuntimeError(
                f'Azure {prefix} configuration differs from its persisted '
                'request contract.'
            )
        if prefix.endswith('_nic'):
            desired_configuration = body['properties']['ipConfigurations'][0][
                'properties'
            ]
            desired_public_ip = 'publicIPAddress' in desired_configuration
            if _nic_public_ip_presence(resource) is not desired_public_ip:
                raise RuntimeError(
                    f'Azure {prefix} public-IP attachment differs from its '
                    'persisted request contract.'
                )
        return actual_id

    resource = _get_or_none(service, resource_group, name)
    if resource is None:
        try:
            operation = service.begin_create_or_update(
                resource_group,
                name,
                dict(body),
            )
            resource = _wait(operation)
        except Exception:
            reconciled = _get_or_none(service, resource_group, name)
            if reconciled is None:
                raise
            resource = reconciled
    actual_id = verify(resource)
    _record(
        job,
        persist,
        **{
            f'{prefix}_id': actual_id,
            f'{prefix}_create_ambiguous': False,
        },
    )
    return resource


def _ensure_subnet(
    job: dict[str, Any],
    persist: PersistCallback | None,
    *,
    service: Any,
    subscription_id: str,
    resource_group: str,
    vnet_name: str,
    prefix: str,
    name: str,
    cidr: str,
    nsg_id: str,
    nat_gateway_id: str | None = None,
    disable_default_outbound: bool = False,
    private_endpoint_network_policies: str = 'Disabled',
):
    if private_endpoint_network_policies not in {'Disabled', 'Enabled'}:
        raise ValueError(
            'Azure private-endpoint network policies must be Enabled or '
            'Disabled.'
        )
    expected_id = _expected_id(
        subscription_id,
        resource_group,
        f'Microsoft.Network/virtualNetworks/{vnet_name}/subnets/{name}',
    )
    subnet_properties: dict[str, Any] = {
        'addressPrefix': cidr,
        'networkSecurityGroup': {'id': nsg_id},
        'privateEndpointNetworkPolicies': private_endpoint_network_policies,
    }
    if nat_gateway_id:
        subnet_properties['natGateway'] = {'id': nat_gateway_id}
    if disable_default_outbound:
        subnet_properties['defaultOutboundAccess'] = False
    body = {'properties': subnet_properties}
    spec_fingerprint = 'sha256:' + hashlib.sha256(
        json.dumps(
            body,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
            allow_nan=False,
        ).encode('utf-8')
    ).hexdigest()
    resources = job.setdefault('resources', {})
    expected_values = {
        f'{prefix}_name': name,
        f'{prefix}_expected_id': expected_id,
        f'{prefix}_spec_fingerprint': spec_fingerprint,
    }
    for key, expected in expected_values.items():
        current = resources.get(key)
        if not current:
            continue
        matches = (
            _normalize_id(current) == _normalize_id(expected)
            if key.endswith('_expected_id')
            else str(current) == str(expected)
        )
        if not matches:
            raise RuntimeError(
                f'Refusing Azure provisioning because saved {prefix} '
                'identity conflicts with this run.'
            )
    _record(
        job,
        persist,
        **expected_values,
        **{f'{prefix}_create_ambiguous': True},
    )

    def verify(subnet: Any) -> str:
        _require_succeeded_provisioning(subnet, prefix)
        actual_id = str(_value(subnet, 'id', '') or '')
        properties = _value(subnet, 'properties', {}) or {}
        actual_cidr = str(
            _value(subnet, 'address_prefix')
            or _value(properties, 'address_prefix')
            or _value(properties, 'addressPrefix')
            or ''
        )
        actual_nsg = (
            _value(subnet, 'network_security_group')
            or _value(properties, 'network_security_group')
            or _value(properties, 'networkSecurityGroup')
            or {}
        )
        actual_nsg_id = str(_value(actual_nsg, 'id', '') or '')
        actual_policy = str(
            _value(subnet, 'private_endpoint_network_policies')
            or _value(properties, 'private_endpoint_network_policies')
            or _value(properties, 'privateEndpointNetworkPolicies')
            or ''
        )
        actual_nat = _observed_field(properties, 'natGateway')
        if actual_nat is _MISSING:
            actual_nat_id = ''
        else:
            actual_nat_id = str(_value(actual_nat, 'id', '') or '')
        actual_default_outbound = _observed_field(
            properties,
            'defaultOutboundAccess',
        )
        nat_matches = (
            _normalize_id(actual_nat_id) == _normalize_id(nat_gateway_id)
            if nat_gateway_id else not actual_nat_id
        )
        outbound_matches = (
            actual_default_outbound is False
            if disable_default_outbound else True
        )
        if (
            not actual_id
            or _normalize_id(actual_id) != _normalize_id(expected_id)
            or actual_cidr != cidr
            or _normalize_id(actual_nsg_id) != _normalize_id(nsg_id)
            or actual_policy.casefold()
            != private_endpoint_network_policies.casefold()
            or not nat_matches
            or not outbound_matches
        ):
            raise RuntimeError(
                f'Azure returned a conflicting {prefix} configuration; '
                'cleanup contract retained.'
            )
        return actual_id

    subnet = _get_or_none(service, resource_group, vnet_name, name)
    if subnet is None:
        try:
            subnet = _wait(service.begin_create_or_update(
                resource_group,
                vnet_name,
                name,
                body,
            ))
        except Exception:
            reconciled = _get_or_none(service, resource_group, vnet_name, name)
            if reconciled is None:
                raise
            subnet = reconciled
    actual_id = verify(subnet)
    _record(
        job,
        persist,
        **{
            f'{prefix}_id': actual_id,
            f'{prefix}_create_ambiguous': False,
        },
    )
    return subnet


def _custom_data(script: str) -> str:
    return base64.b64encode(script.encode()).decode()


def _image_plan(image: Mapping[str, Any]) -> dict[str, str]:
    return {
        'publisher': str(image['publisher']),
        'product': str(image['offer']),
        'name': str(image['sku']),
    }


def _vm_body(
    *,
    region: str,
    zone: str | None,
    name: str,
    role: str,
    job_id: str,
    vm_size: str,
    image: Mapping[str, Any],
    nic_id: str,
    public_key: str | None,
    os_disk_size_gb: int,
    data_disk_id: str | None = None,
    custom_data: str | None = None,
) -> dict[str, Any]:
    os_profile: dict[str, Any] = {
        'computerName': name[:64],
        'adminUsername': SSH_USER,
        'linuxConfiguration': {
            'disablePasswordAuthentication': True,
            'provisionVMAgent': True,
            'patchSettings': {'patchMode': 'ImageDefault'},
        },
    }
    if public_key:
        os_profile['linuxConfiguration']['ssh'] = {
            'publicKeys': [{
                'path': f'/home/{SSH_USER}/.ssh/authorized_keys',
                'keyData': public_key,
            }]
        }
    if custom_data:
        os_profile['customData'] = custom_data
    storage_profile: dict[str, Any] = {
        'imageReference': dict(image['image_reference']),
        'osDisk': {
            'name': f'{name}-os',
            'createOption': 'FromImage',
            'deleteOption': 'Delete',
            'diskSizeGB': int(os_disk_size_gb),
            'managedDisk': {'storageAccountType': 'Premium_LRS'},
            'caching': 'ReadWrite',
        },
        'dataDisks': [],
    }
    if data_disk_id:
        storage_profile['dataDisks'] = [{
            'lun': DATA_DISK_LUN,
            'name': data_disk_id.rstrip('/').rsplit('/', 1)[-1],
            'createOption': 'Attach',
            'managedDisk': {'id': data_disk_id},
            'deleteOption': 'Delete',
            'caching': 'None',
            'writeAcceleratorEnabled': False,
        }]
    body: dict[str, Any] = {
        'location': region,
        'tags': _tags(job_id, role),
        'properties': {
            'hardwareProfile': {'vmSize': vm_size},
            'storageProfile': storage_profile,
            'osProfile': os_profile,
            'networkProfile': {
                'networkInterfaces': [{
                    'id': nic_id,
                    'properties': {'deleteOption': 'Delete'},
                }]
            },
            'diagnosticsProfile': {
                'bootDiagnostics': {'enabled': True}
            },
        },
        'plan': _image_plan(image),
    }
    if zone:
        body['zones'] = [zone]
    return body


def _ip_values(nic: Any, public_ip: Any) -> tuple[str | None, str | None]:
    nic_properties = _value(nic, 'properties', {}) or {}
    configurations = list(
        _value(nic, 'ip_configurations')
        or _value(nic_properties, 'ip_configurations')
        or _value(nic_properties, 'ipConfigurations')
        or ()
    )
    configuration_properties = (
        _value(configurations[0], 'properties', {}) or {}
        if configurations else {}
    )
    private_ip = (
        str(
            _value(configurations[0], 'private_ip_address')
            or _value(configuration_properties, 'private_ip_address')
            or _value(configuration_properties, 'privateIPAddress')
            or ''
        )
        if configurations else ''
    )
    public_properties = _value(public_ip, 'properties', {}) or {}
    public = str(
        _value(public_ip, 'ip_address')
        or _value(public_properties, 'ip_address')
        or _value(public_properties, 'ipAddress')
        or ''
    )
    return public or None, private_ip or None


def _create_vm_role_resources(
    job: dict[str, Any],
    persist: PersistCallback | None,
    clients: Mapping[str, Any],
    *,
    subscription_id: str,
    resource_group: str,
    region: str,
    zone: str | None,
    prefix: str,
    name: str,
    role: str,
    vm_size: str,
    image: Mapping[str, Any],
    subnet_id: str,
    public_key: str | None,
    accelerated_networking: bool,
    os_disk_size_gb: int,
    data_disk_id: str | None = None,
    custom_data: str | None = None,
    assign_public_ip: bool = True,
    private_ip_address: str | None = None,
):
    job_id = str(job['id']).lower()
    pip_name = f'{name}-pip'
    pip_id = None
    public_ip = None
    if assign_public_ip:
        pip_id = _expected_id(
            subscription_id, resource_group,
            f'Microsoft.Network/publicIPAddresses/{pip_name}',
        )
        pip_body: dict[str, Any] = {
            'location': region,
            'tags': _tags(job_id, f'{role}-public-ip'),
            'sku': {'name': 'Standard', 'tier': 'Regional'},
            'properties': {
                'publicIPAllocationMethod': 'Static',
                'publicIPAddressVersion': 'IPv4',
            },
        }
        if zone:
            pip_body['zones'] = [zone]
        public_ip = _ensure_child(
            job, persist,
            prefix=f'{prefix}_public_ip', name=pip_name, expected_id=pip_id,
            service=_require_service(
                _network_service(clients, 'public_ip_addresses'),
                'public IP addresses',
            ),
            resource_group=resource_group, body=pip_body,
        )

    nic_name = f'{name}-nic'
    nic_id = _expected_id(
        subscription_id, resource_group,
        f'Microsoft.Network/networkInterfaces/{nic_name}',
    )
    nic = _ensure_child(
        job, persist,
        prefix=f'{prefix}_nic', name=nic_name, expected_id=nic_id,
        service=_require_service(
            _network_service(clients, 'network_interfaces'),
            'network interfaces',
        ),
        resource_group=resource_group,
        body={
            'location': region,
            'tags': _tags(job_id, f'{role}-nic'),
            'properties': {
                'enableAcceleratedNetworking': bool(accelerated_networking),
                'enableIPForwarding': False,
                'ipConfigurations': [{
                    'name': 'primary',
                    'properties': {
                        'privateIPAllocationMethod': (
                            'Static' if private_ip_address else 'Dynamic'
                        ),
                        'privateIPAddressVersion': 'IPv4',
                        'primary': True,
                        'subnet': {'id': subnet_id},
                        **(
                            {'privateIPAddress': private_ip_address}
                            if private_ip_address else {}
                        ),
                        **(
                            {'publicIPAddress': {'id': pip_id}}
                            if pip_id else {}
                        ),
                    },
                }],
            },
        },
    )
    vm_id = _expected_id(
        subscription_id, resource_group,
        f'Microsoft.Compute/virtualMachines/{name}',
    )
    os_disk_name = f'{name}-os'
    os_disk_id = _expected_id(
        subscription_id, resource_group,
        f'Microsoft.Compute/disks/{os_disk_name}',
    )
    # The managed OS disk is created atomically with the VM and can survive a
    # lost VM-create response. Persist its deterministic identity beforehand
    # so resource-group cleanup can include it in its exact allowlist.
    _record(
        job,
        persist,
        **{
            f'{prefix}_os_disk_name': os_disk_name,
            f'{prefix}_os_disk_expected_id': os_disk_id,
            f'{prefix}_os_disk_create_ambiguous': True,
        },
    )
    vm = _ensure_child(
        job, persist,
        prefix=prefix, name=name, expected_id=vm_id,
        service=_require_service(
            _compute_service(clients, 'virtual_machines'), 'virtual machines'
        ),
        resource_group=resource_group,
        body=_vm_body(
            region=region, zone=zone, name=name, role=role, job_id=job_id,
            vm_size=vm_size, image=image, nic_id=nic_id,
            public_key=public_key, os_disk_size_gb=os_disk_size_gb,
            data_disk_id=data_disk_id, custom_data=custom_data,
        ),
    )
    _record(
        job,
        persist,
        **{f'{prefix}_os_disk_create_ambiguous': False},
    )
    # Static Public IP allocation and dynamic private allocation are complete
    # by VM provisioning. Re-read both to avoid relying on stale create bodies.
    pip_service = _network_service(clients, 'public_ip_addresses')
    nic_service = _network_service(clients, 'network_interfaces')
    refreshed_pip = (
        _get_or_none(pip_service, resource_group, pip_name) or public_ip
        if assign_public_ip else None
    )
    refreshed_nic = _get_or_none(nic_service, resource_group, nic_name) or nic
    public, private = _ip_values(refreshed_nic, refreshed_pip)
    if not private or (assign_public_ip and not public):
        raise RuntimeError(
            f'Azure {role} became available without its required IPv4 '
            'addresses; the resource-group cleanup contract was retained.'
        )
    if private_ip_address and private != private_ip_address:
        raise RuntimeError(
            f'Azure {role} returned private address {private!r} instead of '
            f'the manifest-bound address {private_ip_address!r}; the '
            'resource-group cleanup contract was retained.'
        )
    return vm, public, private


def _create_vm_role(
    job: dict[str, Any],
    persist: PersistCallback | None,
    clients: Mapping[str, Any],
    *,
    inventory_node_key: str | None = None,
    **role_arguments: Any,
):
    if inventory_node_key:
        _set_inventory_node_status(
            job,
            persist,
            inventory_node_key,
            'creating',
        )
    try:
        vm, public, private = _create_vm_role_resources(
            job,
            persist,
            clients,
            **role_arguments,
        )
    except Exception:
        if inventory_node_key:
            _set_inventory_node_status(
                job,
                persist,
                inventory_node_key,
                'create_ambiguous',
            )
        raise
    if inventory_node_key:
        _set_inventory_node_status(
            job,
            persist,
            inventory_node_key,
            'running',
            public_address=public,
            private_address=private,
        )
    return vm, public, private


def provision(
    job: dict[str, Any],
    plan: Any,
    *,
    public_key: str | None = None,
    emit: EventCallback | None = None,
    persist: PersistCallback | None = None,
    credential=None,
    clients=None,
):
    """Create a deterministic, isolated Azure benchmark resource group."""
    benchmarks = tuple(_value(plan, 'benchmarks', ()) or ())
    if 'deathstarbench' in benchmarks:
        topology_id, runtime_id = _deathstarbench_contract_ids(plan)
        # The ordinary provider entry point mirrors the API release gate.
        # The Azure-only synthetic lifecycle below has a separate explicit
        # entry point and cannot accidentally execute benchmark work.
        require_released_runtime(topology_id, runtime_id)
    subscription_id = _plan_subscription(plan)
    if not subscription_id:
        raise ValueError('Select an Azure subscription.')
    subscription_id, account, clients = _runtime(
        subscription_id, credential, clients
    )
    region = _plan_region(plan)
    vm_size = _plan_vm_size(plan)
    if not vm_size:
        raise ValueError('Select an Azure VM size.')
    public_key = _validate_public_key(
        public_key or job.get('_public_key') or ''
    )
    protocols = _selected_iperf3_protocols(plan)
    web_benchmarks = _selected_web_benchmarks(plan)
    sysbench = _value(plan, 'sysbench', {}) or {}
    phoronix = _value(plan, 'phoronix', {}) or {}
    rocky_linux.validate_benchmark_selection(
        benchmarks,
        sysbench_workloads=_value(sysbench, 'workloads', ()) or (),
        iperf3_protocols=protocols,
        phoronix_profiles=_value(phoronix, 'profiles', ()) or (),
        llm_benchmarks=_value(plan, 'llm_benchmarks', ()) or (),
        provider='azure',
    )

    resources = job.setdefault('resources', {})
    requested_zone = str(
        resources.get('azure_zone')
        or _value(plan, 'azure_zone')
        or _value(plan, 'availability_zone')
        or _value(plan, 'availability_domain')
        or ''
    ).strip() or None
    details = _find_size(
        subscription_id, region, requested_zone, vm_size, clients
    )
    _validate_capacity(plan, details)
    image = resolve_rocky_linux_9_image(
        subscription_id, region, details['architecture'], clients=clients
    )
    loadgen_details = (
        _select_loadgen(subscription_id, region, requested_zone, clients)
        if web_benchmarks else None
    )
    loadgen_image = None
    if loadgen_details:
        loadgen_image = (
            image if image['architecture'] == 'x86_64'
            else resolve_rocky_linux_9_image(
                subscription_id, region, 'x86_64', clients=clients
            )
        )
    _require_marketplace_terms(clients, image, loadgen_image)

    additional_volume = bool(_storage_value(plan, 'additional_volume', False))
    boot_size_gb = int(
        _storage_value(plan, 'boot_size_gb', DEFAULT_OS_DISK_SIZE_GB)
    )
    data_size_gb = int(_storage_value(plan, 'additional_size_gb', 1024))
    if not 30 <= boot_size_gb <= 32767:
        raise ValueError('Azure OS disk size must be between 30 and 32767 GiB.')
    if additional_volume and not 1 <= data_size_gb <= 65536:
        raise ValueError(
            'Azure Premium SSD v2 data disk size must be between 1 and 65536 GiB.'
        )
    fileio_selected = 'fio' in benchmarks or (
        'sysbench' in benchmarks
        and 'fileio' in set(
            _value(_value(plan, 'sysbench', {}) or {}, 'workloads', ()) or ()
        )
    )
    if fileio_selected and not additional_volume:
        raise ValueError(
            'Azure fio and Sysbench File I/O require an additional Premium '
            'SSD v2 data disk.'
        )
    if additional_volume and details.get('premium_io_v2') is False:
        raise ValueError(
            f'{vm_size} does not support Azure Premium SSD v2 in the selected '
            'placement.'
        )
    if additional_volume and not _premium_v2_available(
        clients, region, requested_zone
    ):
        raise ValueError(
            'Azure Premium SSD v2 is unavailable in the selected region and '
            'zone. Refresh placement discovery or choose another zone.'
        )

    job_id = str(job.get('id') or '').lower()
    group_name = _job_name(job_id)
    nsg_name = f'{group_name}-nsg'
    vnet_name = f'{group_name}-vnet'
    subnet_name = 'benchmark-subnet'
    runner_name = f'{group_name}-runner'
    peer_name = f'{group_name}-iperf-peer'
    loadgen_name = f'{group_name}-loadgen'
    data_disk_name = f'{group_name}-data'

    _record(
        job,
        persist,
        provider='azure',
        azure_subscription_id=subscription_id,
        azure_subscription_name=str(_value(account, 'name', '') or ''),
        azure_tenant_id=str(
            _value(account, 'tenant_id', _value(account, 'tenantId', '')) or ''
        ),
        region=region,
        azure_zone=requested_zone,
        availability_zone=requested_zone,
        azure_vm_size=vm_size,
        instance_type=vm_size,
        architecture=details['architecture'],
        azure_architecture=details['architecture'],
        azure_accelerated_networking=bool(
            details.get('accelerated_networking')
        ),
        vcpu=details['vcpu'],
        ocpus=details['vcpu'],
        memory_gb=details['memory_gb'],
        local_nvme_supported=bool(
            details.get('local_nvme_supported', False)
        ),
        local_nvme_disk_count=int(
            details.get('local_nvme_disk_count', 0) or 0
        ),
        local_nvme_disk_size_gb=float(
            details.get('local_nvme_disk_size_gb', 0) or 0
        ),
        local_nvme_total_size_gb=float(
            details.get('local_nvme_total_size_gb', 0) or 0
        ),
        image_id=image['image_id'],
        azure_image_id=image['image_id'],
        image_name=image['urn'],
        azure_image_urn=image['urn'],
        azure_image_version=image['version'],
        azure_image_publisher=image['publisher'],
        azure_image_offer=image['offer'],
        azure_image_sku=image['sku'],
        ssh_user=SSH_USER,
        azure_iperf3_protocols=list(protocols),
        azure_web_benchmarks=list(web_benchmarks),
        azure_os_disk_size_gb=boot_size_gb,
        azure_boot_disk_type='Premium_LRS',
        azure_data_disk_type=PREMIUM_V2_DISK_TYPE if additional_volume else None,
        azure_data_disk_iops=PREMIUM_V2_IOPS if additional_volume else None,
        azure_data_disk_throughput_mibps=(
            PREMIUM_V2_THROUGHPUT_MIBPS if additional_volume else None
        ),
        data_volume_device=DATA_DISK_DEVICE if additional_volume else None,
    )

    compact_manifest = None
    if 'deathstarbench' in benchmarks:
        topology_id, runtime_id = _deathstarbench_contract_ids(plan)
        compact_manifest = build_topology_manifest(
            topology_id,
            runtime_id,
            selected_shape=vm_size,
            selected_architecture=details['architecture'],
        )
        # Persist the ownership anchor and complete logical plan before the
        # resource-group PUT. A crash at any later point is therefore
        # recoverable by exact name without broad subscription discovery.
        _record(
            job,
            persist,
            azure_resource_group_name=group_name,
            azure_resource_group_expected_id=_expected_id(
                subscription_id,
                group_name,
            ),
            azure_resource_group_tags=_tags(job_id, 'resource-group'),
            azure_resource_group_create_ambiguous=True,
            azure_instance_name=runner_name,
            azure_instance_expected_id=_expected_id(
                subscription_id,
                group_name,
                f'Microsoft.Compute/virtualMachines/{runner_name}',
            ),
            azure_loadgen_instance_name=loadgen_name,
            azure_loadgen_instance_expected_id=_expected_id(
                subscription_id,
                group_name,
                f'Microsoft.Compute/virtualMachines/{loadgen_name}',
            ),
            **(
                {
                    'azure_data_disk_name': data_disk_name,
                    'azure_data_disk_expected_id': _expected_id(
                        subscription_id,
                        group_name,
                        f'Microsoft.Compute/disks/{data_disk_name}',
                    ),
                }
                if additional_volume else {}
            ),
        )
        assignments = {
            'runner': {
                'name': runner_name,
                'private_address': None,
                'details': details,
            },
            'load-generator': {
                'name': loadgen_name,
                'private_address': None,
                'details': loadgen_details,
            },
        }
        storage_by_node: dict[str, tuple[StorageResource, ...]] = {}
        if additional_volume:
            expected_data_disk_id = _expected_id(
                subscription_id,
                group_name,
                f'Microsoft.Compute/disks/{data_disk_name}',
            )
            storage_by_node['runner'] = (StorageResource(
                key='benchmark',
                kind='persistent_block_volume',
                provider_resource_id=expected_data_disk_id,
                provider_resource_name=data_disk_name,
                device=DATA_DISK_DEVICE,
                mount_point='/data',
                filesystem='xfs',
                size_gb=data_size_gb,
                provisioned_iops=PREMIUM_V2_IOPS,
                provisioned_throughput_mibps=PREMIUM_V2_THROUGHPUT_MIBPS,
                ephemeral=False,
                lifecycle_status='planned',
            ),)
        expected_inventory = _planned_azure_inventory(
            compact_manifest,
            subscription_id=subscription_id,
            resource_group=group_name,
            zone=requested_zone,
            assignments=assignments,
            storage_by_node=storage_by_node,
        )
        _initialize_topology_inventory(
            job,
            persist,
            compact_manifest,
            expected_inventory,
        )

    _ensure_group(
        job, persist, clients, subscription_id, region, group_name
    )
    _emit(job, emit, 'Provision', f'Created Azure resource group {group_name}.')

    nsg_id = _expected_id(
        subscription_id, group_name,
        f'Microsoft.Network/networkSecurityGroups/{nsg_name}',
    )
    _ensure_child(
        job, persist,
        prefix='azure_nsg', name=nsg_name, expected_id=nsg_id,
        service=_require_service(
            _network_service(clients, 'network_security_groups'),
            'network security groups',
        ),
        resource_group=group_name,
        body={
            'location': region,
            'tags': _tags(job_id, 'network-security-group'),
            'properties': {
                'securityRules': _security_rules(protocols, web_benchmarks),
            },
        },
    )

    vnet_id = _expected_id(
        subscription_id, group_name,
        f'Microsoft.Network/virtualNetworks/{vnet_name}',
    )
    _ensure_child(
        job, persist,
        prefix='azure_vnet', name=vnet_name, expected_id=vnet_id,
        service=_require_service(
            _network_service(clients, 'virtual_networks'), 'virtual networks'
        ),
        resource_group=group_name,
        body={
            'location': region,
            'tags': _tags(job_id, 'virtual-network'),
            'properties': {
                'addressSpace': {'addressPrefixes': [VNET_CIDR]},
            },
        },
    )
    subnet_id = _expected_id(
        subscription_id, group_name,
        f'Microsoft.Network/virtualNetworks/{vnet_name}/subnets/{subnet_name}',
    )
    subnet_service = _require_service(
        _network_service(clients, 'subnets'), 'virtual network subnets'
    )
    _ensure_subnet(
        job,
        persist,
        service=subnet_service,
        subscription_id=subscription_id,
        resource_group=group_name,
        vnet_name=vnet_name,
        prefix='azure_subnet',
        name=subnet_name,
        cidr=SUBNET_CIDR,
        nsg_id=nsg_id,
    )

    data_disk_id = None
    if additional_volume:
        data_disk_id = _expected_id(
            subscription_id, group_name,
            f'Microsoft.Compute/disks/{data_disk_name}',
        )
        disk_body: dict[str, Any] = {
            'location': region,
            'tags': _tags(job_id, 'data-disk'),
            'sku': {'name': PREMIUM_V2_DISK_TYPE},
            'properties': {
                'creationData': {'createOption': 'Empty'},
                'diskSizeGB': data_size_gb,
                'diskIOPSReadWrite': PREMIUM_V2_IOPS,
                'diskMBpsReadWrite': PREMIUM_V2_THROUGHPUT_MIBPS,
                'networkAccessPolicy': 'DenyAll',
                'publicNetworkAccess': 'Disabled',
            },
        }
        if requested_zone:
            disk_body['zones'] = [requested_zone]
        if compact_manifest:
            _set_inventory_storage_status(
                job,
                persist,
                'runner',
                'benchmark',
                'creating',
            )
        try:
            _ensure_child(
                job, persist,
                prefix='azure_data_disk', name=data_disk_name,
                expected_id=data_disk_id,
                service=_require_service(
                    _compute_service(clients, 'disks'), 'managed disks'
                ),
                resource_group=group_name, body=disk_body,
            )
        except Exception:
            if compact_manifest:
                _set_inventory_storage_status(
                    job,
                    persist,
                    'runner',
                    'benchmark',
                    'create_ambiguous',
                )
            raise
        if compact_manifest:
            _set_inventory_storage_status(
                job,
                persist,
                'runner',
                'benchmark',
                'running',
            )
        _record(
            job,
            persist,
            azure_data_disk_size_gb=data_size_gb,
            azure_data_disk_lun=DATA_DISK_LUN,
            azure_data_disk_device=DATA_DISK_DEVICE,
        )
        _emit(
            job, emit, 'Provision',
            f'Created Azure Premium SSD v2 data disk {data_disk_name} '
            f'({data_size_gb} GiB, {PREMIUM_V2_IOPS} IOPS, '
            f'{PREMIUM_V2_THROUGHPUT_MIBPS} MiB/s).',
        )

    runner, public_ip, private_ip = _create_vm_role(
        job, persist, clients,
        inventory_node_key='runner' if compact_manifest else None,
        subscription_id=subscription_id, resource_group=group_name,
        region=region, zone=requested_zone,
        prefix='azure_instance', name=runner_name, role='runner',
        vm_size=vm_size, image=image, subnet_id=subnet_id,
        public_key=public_key,
        accelerated_networking=bool(details.get('accelerated_networking')),
        os_disk_size_gb=boot_size_gb, data_disk_id=data_disk_id,
    )
    _record(
        job,
        persist,
        instance_id=str(_value(runner, 'id')),
        public_ip=public_ip,
        private_ip=private_ip,
    )
    _emit(
        job, emit, 'Provision',
        f'Launched Azure {vm_size} runner in {region}'
        + (f' zone {requested_zone}' if requested_zone else '')
        + f'; public IP {public_ip}.',
    )

    if loadgen_details and loadgen_image:
        loadgen, loadgen_public, loadgen_private = _create_vm_role(
            job, persist, clients,
            inventory_node_key=(
                'load-generator' if compact_manifest else None
            ),
            subscription_id=subscription_id, resource_group=group_name,
            region=region, zone=requested_zone,
            prefix='azure_loadgen_instance', name=loadgen_name,
            role='load-generator', vm_size=loadgen_details['vm_size'],
            image=loadgen_image, subnet_id=subnet_id, public_key=public_key,
            accelerated_networking=bool(
                loadgen_details.get('accelerated_networking')
            ),
            os_disk_size_gb=LOADGEN_OS_DISK_SIZE_GB,
        )
        _record(
            job,
            persist,
            loadgen_instance_id=str(_value(loadgen, 'id')),
            loadgen_public_ip=loadgen_public,
            loadgen_private_ip=loadgen_private,
            loadgen_shape=loadgen_details['vm_size'],
            loadgen_vcpus=loadgen_details['vcpu'],
            loadgen_memory_gb=loadgen_details['memory_gb'],
            loadgen_architecture='x86_64',
            loadgen_image_id=loadgen_image['image_id'],
            loadgen_image_name=loadgen_image['urn'],
            loadgen_network_bandwidth_gbps=(
                loadgen_details.get('network_bandwidth_gbps')
            ),
            loadgen_network_capacity_kind=(
                loadgen_details.get('network_capacity_kind')
            ),
        )
        _emit(
            job, emit, 'Provision',
            f'Launched fixed x86 Azure web load generator '
            f'{loadgen_details["vm_size"]}; public IP {loadgen_public}.',
        )

    if protocols:
        peer, peer_public, peer_private = _create_vm_role(
            job, persist, clients,
            subscription_id=subscription_id, resource_group=group_name,
            region=region, zone=requested_zone,
            prefix='azure_peer_instance', name=peer_name, role='iperf-peer',
            vm_size=vm_size, image=image, subnet_id=subnet_id,
            public_key=public_key,
            accelerated_networking=bool(details.get('accelerated_networking')),
            os_disk_size_gb=PEER_OS_DISK_SIZE_GB,
            custom_data=_custom_data(
                rocky_linux.iperf3_peer_startup_script(
                    protocols, provider='azure'
                )
            ),
        )
        _record(
            job,
            persist,
            azure_peer_vm_size=vm_size,
            azure_peer_image_id=image['image_id'],
            azure_peer_public_ip=peer_public,
            peer_private_ip=peer_private,
            azure_peer_instance_id=str(_value(peer, 'id')),
        )
        _emit(
            job, emit, 'Provision',
            f'Launched same-size Azure iperf3 peer; benchmark traffic targets '
            f'{peer_private}.',
        )

    return job['resources']


def provision_distributed_deathstarbench_candidate(
    job: dict[str, Any],
    plan: Any,
    *,
    public_key: str | None = None,
    emit: EventCallback | None = None,
    persist: PersistCallback | None = None,
    credential=None,
    clients=None,
):
    """Exercise Azure's unreleased five-node infrastructure lifecycle.

    This entry point intentionally stops after infrastructure creation. It is
    not dispatched by ``app.main`` and must not install K3s or execute a
    benchmark until the workload/runtime slice is qualified.
    """

    plan_provider = str(_value(plan, 'provider', '') or '').strip().casefold()
    if plan_provider and plan_provider != 'azure':
        raise ValueError(
            'The Azure distributed candidate requires an Azure plan.'
        )
    benchmarks = tuple(_value(plan, 'benchmarks', ()) or ())
    if benchmarks != ('deathstarbench',):
        raise ValueError(
            'The Azure distributed candidate requires DeathStarBench as its '
            'only selected benchmark.'
        )
    deathstarbench = _value(plan, 'deathstarbench', {}) or {}
    if _value(deathstarbench, 'workload', None) != 'social_network':
        raise ValueError(
            'The Azure distributed candidate currently requires the exact '
            'DeathStarBench Social Network workload.'
        )
    existing_resources = job.setdefault('resources', {})
    saved_provider = str(
        existing_resources.get('provider') or ''
    ).strip().casefold()
    if (
        (saved_provider and saved_provider != 'azure')
        or (existing_resources and not saved_provider)
    ):
        raise ResourceInventoryError(
            'The Azure distributed candidate refuses pre-existing resource '
            'state without an exact Azure provider identity.'
        )
    topology_id, runtime_id = _deathstarbench_contract_ids(plan)
    if (
        topology_id != DISTRIBUTED_TIERED_TOPOLOGY_ID
        or runtime_id != K3S_RUNTIME_ID
    ):
        raise ValueError(
            'The Azure distributed candidate requires the exact '
            'distributed_tiered_v1/k3s_v1 contract.'
        )
    subscription_id = _plan_subscription(plan)
    if not subscription_id:
        raise ValueError('Select an Azure subscription.')
    subscription_id, account, clients = _runtime(
        subscription_id,
        credential,
        clients,
    )
    _require_registered_resource_providers(clients)
    _require_distributed_candidate_services(clients)
    region = _plan_region(plan)
    application_size = _plan_vm_size(plan)
    if not application_size:
        raise ValueError('Select an Azure VM size for the application role.')
    public_key = _validate_public_key(
        public_key or job.get('_public_key') or ''
    )
    resources = existing_resources
    requested_zone = str(
        resources.get('azure_zone')
        or _value(plan, 'azure_zone')
        or _value(plan, 'availability_zone')
        or _value(plan, 'availability_domain')
        or ''
    ).strip() or None

    # Resolve every requested placement and per-resource capability before the
    # first cloud write. Azure quota/capacity allocation can still fail during
    # creation; the persisted group contract makes that partial state safely
    # recoverable.
    application_details = _find_size(
        subscription_id,
        region,
        requested_zone,
        application_size,
        clients,
    )
    _validate_capacity(plan, application_details)
    manifest = build_topology_manifest(
        topology_id,
        runtime_id,
        selected_shape=application_size,
        selected_architecture=application_details['architecture'],
    )
    role_details = _distributed_role_details(
        manifest,
        subscription_id=subscription_id,
        region=region,
        zone=requested_zone,
        application_details=application_details,
        clients=clients,
    )
    premium_os_disk_incompatible = [
        f'{node_key} ({details["vm_size"]})'
        for node_key, details in role_details.items()
        if details.get('premium_io') is not True
    ]
    if premium_os_disk_incompatible:
        raise ValueError(
            'Azure distributed roles require Premium_LRS OS-disk support; '
            'capability is absent or unknown for '
            + ', '.join(premium_os_disk_incompatible)
            + '.'
        )
    database_details = role_details['database']
    # Azure's live Compute SKU feed does not consistently publish the
    # optional PremiumIOV2Supported VM capability.  An explicit false remains
    # a hard incompatibility; when omitted, the authoritative zonal
    # PremiumV2_LRS disk-SKU check below decides availability.  PremiumIO=True
    # is already required for every role above.
    if database_details.get('premium_io_v2') is False:
        raise ValueError(
            f'{database_details["vm_size"]} reports that Azure Premium SSD '
            'v2 is unsupported in the selected placement.'
        )
    if int(database_details.get('maximum_data_disks') or 0) < 1:
        raise ValueError(
            f'{database_details["vm_size"]} cannot attach the required '
            'database disk.'
        )
    if not _premium_v2_available(clients, region, requested_zone):
        raise ValueError(
            'Azure Premium SSD v2 is unavailable for the distributed '
            'database in the selected region and zone.'
        )
    application_image = _distributed_candidate_image(
        resources,
        key_prefix='azure_dsb_application_image',
        subscription_id=subscription_id,
        region=region,
        architecture=application_details['architecture'],
        clients=clients,
    )
    x86_image = _distributed_candidate_image(
        resources,
        key_prefix='azure_dsb_support_image',
        subscription_id=subscription_id,
        region=region,
        architecture='x86_64',
        clients=clients,
    )
    _require_marketplace_terms(clients, application_image, x86_image)

    job_id = str(job.get('id') or '').lower()
    group_name = _job_name(job_id)
    vnet_name = f'{group_name}-dsb-vnet'
    cluster_nsg_name = f'{group_name}-cluster-nsg'
    loadgen_nsg_name = f'{group_name}-loadgen-nsg'
    cluster_nat_public_ip_name = f'{group_name}-cluster-nat-pip'
    cluster_nat_gateway_name = f'{group_name}-cluster-nat'
    cluster_subnet_name = 'cluster-subnet'
    loadgen_subnet_name = 'loadgen-subnet'
    node_names = {
        'control': f'{group_name}-control',
        'database': f'{group_name}-database',
        'cache': f'{group_name}-cache',
        'application': f'{group_name}-application',
        'load-generator': f'{group_name}-loadgen',
    }
    node_prefixes = {
        key: f'azure_dsb_{key.replace("-", "_")}_instance'
        for key in node_names
    }
    database_disk_name = f'{group_name}-database-data'
    database_disk_id = _expected_id(
        subscription_id,
        group_name,
        f'Microsoft.Compute/disks/{database_disk_name}',
    )
    database_storage_contract = next(
        node for node in manifest.nodes if node.key == 'database'
    ).storage[0]
    database_storage = StorageResource(
        key=database_storage_contract.key,
        kind='persistent_block_volume',
        provider_resource_id=database_disk_id,
        provider_resource_name=database_disk_name,
        device=DATA_DISK_DEVICE,
        mount_point=database_storage_contract.mount_point,
        filesystem=database_storage_contract.filesystem,
        size_gb=database_storage_contract.size_gib,
        provisioned_iops=database_storage_contract.minimum_iops,
        provisioned_throughput_mibps=(
            database_storage_contract.minimum_throughput_mibps
        ),
        ephemeral=False,
        lifecycle_status='planned',
    )
    assignments = {
        node.key: {
            'name': node_names[node.key],
            'private_address': DISTRIBUTED_PRIVATE_ADDRESSES[node.key],
            'details': role_details[node.key],
        }
        for node in manifest.nodes
    }
    expected_inventory = _planned_azure_inventory(
        manifest,
        subscription_id=subscription_id,
        resource_group=group_name,
        zone=requested_zone,
        assignments=assignments,
        storage_by_node={'database': (database_storage,)},
    )
    expected_identity_fields: dict[str, Any] = {
        'azure_dsb_cluster_nsg_name': cluster_nsg_name,
        'azure_dsb_cluster_nsg_expected_id': _expected_id(
            subscription_id,
            group_name,
            f'Microsoft.Network/networkSecurityGroups/{cluster_nsg_name}',
        ),
        'azure_dsb_loadgen_nsg_name': loadgen_nsg_name,
        'azure_dsb_loadgen_nsg_expected_id': _expected_id(
            subscription_id,
            group_name,
            f'Microsoft.Network/networkSecurityGroups/{loadgen_nsg_name}',
        ),
        'azure_dsb_vnet_name': vnet_name,
        'azure_dsb_vnet_expected_id': _expected_id(
            subscription_id,
            group_name,
            f'Microsoft.Network/virtualNetworks/{vnet_name}',
        ),
        'azure_dsb_cluster_subnet_name': cluster_subnet_name,
        'azure_dsb_cluster_subnet_expected_id': _expected_id(
            subscription_id,
            group_name,
            f'Microsoft.Network/virtualNetworks/{vnet_name}/subnets/'
            f'{cluster_subnet_name}',
        ),
        'azure_dsb_loadgen_subnet_name': loadgen_subnet_name,
        'azure_dsb_loadgen_subnet_expected_id': _expected_id(
            subscription_id,
            group_name,
            f'Microsoft.Network/virtualNetworks/{vnet_name}/subnets/'
            f'{loadgen_subnet_name}',
        ),
        'azure_dsb_cluster_nat_public_ip_name': cluster_nat_public_ip_name,
        'azure_dsb_cluster_nat_public_ip_expected_id': _expected_id(
            subscription_id,
            group_name,
            'Microsoft.Network/publicIPAddresses/'
            f'{cluster_nat_public_ip_name}',
        ),
        'azure_dsb_cluster_nat_gateway_name': cluster_nat_gateway_name,
        'azure_dsb_cluster_nat_gateway_expected_id': _expected_id(
            subscription_id,
            group_name,
            f'Microsoft.Network/natGateways/{cluster_nat_gateway_name}',
        ),
        'azure_dsb_database_data_disk_name': database_disk_name,
        'azure_dsb_database_data_disk_expected_id': database_disk_id,
    }
    for node_key, prefix in node_prefixes.items():
        name = node_names[node_key]
        expected_identity_fields[f'{prefix}_name'] = name
        expected_identity_fields[f'{prefix}_expected_id'] = _expected_id(
            subscription_id,
            group_name,
            f'Microsoft.Compute/virtualMachines/{name}',
        )
        nic_name = f'{name}-nic'
        expected_identity_fields[f'{prefix}_nic_name'] = nic_name
        expected_identity_fields[f'{prefix}_nic_expected_id'] = _expected_id(
            subscription_id,
            group_name,
            f'Microsoft.Network/networkInterfaces/{nic_name}',
        )
        os_disk_name = f'{name}-os'
        expected_identity_fields[f'{prefix}_os_disk_name'] = os_disk_name
        expected_identity_fields[
            f'{prefix}_os_disk_expected_id'
        ] = _expected_id(
            subscription_id,
            group_name,
            f'Microsoft.Compute/disks/{os_disk_name}',
        )
        if node_key in DISTRIBUTED_PUBLIC_IP_NODES:
            public_ip_name = f'{name}-pip'
            expected_identity_fields[
                f'{prefix}_public_ip_name'
            ] = public_ip_name
            expected_identity_fields[
                f'{prefix}_public_ip_expected_id'
            ] = _expected_id(
                subscription_id,
                group_name,
                f'Microsoft.Network/publicIPAddresses/{public_ip_name}',
            )

    # Persist the group anchor, image pins, manifest, and every deterministic
    # role identity as one contract before the group create request.
    contract_values: dict[str, Any] = {
        'provider': 'azure',
        'azure_subscription_id': subscription_id,
        'azure_subscription_name': str(_value(account, 'name', '') or ''),
        'azure_tenant_id': str(
            _value(account, 'tenant_id', _value(account, 'tenantId', '')) or ''
        ),
        'region': region,
        'azure_zone': requested_zone,
        'availability_zone': requested_zone,
        'azure_vm_size': application_size,
        'instance_type': application_size,
        'architecture': application_details['architecture'],
        'azure_architecture': application_details['architecture'],
        'vcpu': application_details['vcpu'],
        'ocpus': application_details['vcpu'],
        'memory_gb': application_details['memory_gb'],
        'image_id': application_image['image_id'],
        'azure_image_id': application_image['image_id'],
        'image_name': application_image['urn'],
        'azure_image_urn': application_image['urn'],
        'azure_image_version': application_image['version'],
        'azure_dsb_application_image_id': application_image['image_id'],
        'azure_dsb_application_image_urn': application_image['urn'],
        'azure_dsb_application_image_version': application_image['version'],
        'azure_dsb_application_image_architecture': (
            application_image['architecture']
        ),
        'azure_dsb_support_image_id': x86_image['image_id'],
        'azure_dsb_support_image_urn': x86_image['urn'],
        'azure_dsb_support_image_version': x86_image['version'],
        'azure_dsb_support_image_architecture': x86_image['architecture'],
        'ssh_user': SSH_USER,
        'azure_web_benchmarks': ['deathstarbench'],
        'azure_resource_group_name': group_name,
        'azure_resource_group_expected_id': _expected_id(
            subscription_id,
            group_name,
        ),
        'azure_resource_group_tags': _tags(job_id, 'resource-group'),
        'azure_resource_group_create_ambiguous': True,
        'azure_distributed_candidate': True,
        'azure_database_disk_type': PREMIUM_V2_DISK_TYPE,
        'azure_database_disk_size_gb': database_storage_contract.size_gib,
        'azure_database_disk_iops': database_storage_contract.minimum_iops,
        'azure_database_disk_throughput_mibps': (
            database_storage_contract.minimum_throughput_mibps
        ),
        'azure_database_disk_device': DATA_DISK_DEVICE,
        **expected_identity_fields,
    }
    # A resume must reject added or topology-inapplicable allowlist fields
    # before it can reconcile any missing Azure child. Existing saved values
    # deliberately win in this validation projection so identity drift is not
    # hidden by the freshly derived contract.
    _validate_managed_expected_ids(
        {**contract_values, **resources},
        subscription_id=subscription_id,
        resource_group=group_name,
        manifest=manifest,
        error_type=ResourceInventoryError,
    )
    _initialize_distributed_candidate_contract(
        job,
        persist,
        manifest,
        expected_inventory,
        contract_values,
    )
    _ensure_group(
        job,
        persist,
        clients,
        subscription_id,
        region,
        group_name,
    )
    _emit(
        job,
        emit,
        'Provision',
        f'Created Azure distributed candidate resource group {group_name}.',
    )

    security_rules = _distributed_security_rules(manifest)
    nsg_specs = (
        (
            'azure_dsb_cluster_nsg',
            cluster_nsg_name,
            'cluster-network-security-group',
            security_rules['cluster'],
        ),
        (
            'azure_dsb_loadgen_nsg',
            loadgen_nsg_name,
            'loadgen-network-security-group',
            security_rules['load-generator'],
        ),
    )
    nsg_ids: dict[str, str] = {}
    for prefix, name, role, rules in nsg_specs:
        expected_id = _expected_id(
            subscription_id,
            group_name,
            f'Microsoft.Network/networkSecurityGroups/{name}',
        )
        _ensure_child(
            job,
            persist,
            prefix=prefix,
            name=name,
            expected_id=expected_id,
            service=_require_service(
                _network_service(clients, 'network_security_groups'),
                'network security groups',
            ),
            resource_group=group_name,
            body={
                'location': region,
                'tags': _tags(job_id, role),
                'properties': {'securityRules': rules},
            },
        )
        nsg_ids[prefix] = expected_id

    vnet_id = _expected_id(
        subscription_id,
        group_name,
        f'Microsoft.Network/virtualNetworks/{vnet_name}',
    )
    _ensure_child(
        job,
        persist,
        prefix='azure_dsb_vnet',
        name=vnet_name,
        expected_id=vnet_id,
        service=_require_service(
            _network_service(clients, 'virtual_networks'),
            'virtual networks',
        ),
        resource_group=group_name,
        body={
            'location': region,
            'tags': _tags(job_id, 'distributed-virtual-network'),
            'properties': {
                'addressSpace': {
                    # Keep the Azure underlay disjoint from K3s defaults:
                    # pods use 10.42.0.0/16 and services use 10.43.0.0/16.
                    'addressPrefixes': [DISTRIBUTED_VNET_CIDR],
                },
            },
        },
    )
    cluster_nat_public_ip_id = _expected_id(
        subscription_id,
        group_name,
        'Microsoft.Network/publicIPAddresses/'
        f'{cluster_nat_public_ip_name}',
    )
    nat_public_ip_body: dict[str, Any] = {
        'location': region,
        'tags': _tags(job_id, 'cluster-nat-public-ip'),
        'sku': {'name': 'Standard', 'tier': 'Regional'},
        'properties': {
            'publicIPAllocationMethod': 'Static',
            'publicIPAddressVersion': 'IPv4',
        },
    }
    if requested_zone:
        nat_public_ip_body['zones'] = [requested_zone]
    _ensure_child(
        job,
        persist,
        prefix='azure_dsb_cluster_nat_public_ip',
        name=cluster_nat_public_ip_name,
        expected_id=cluster_nat_public_ip_id,
        service=_require_service(
            _network_service(clients, 'public_ip_addresses'),
            'public IP addresses',
        ),
        resource_group=group_name,
        body=nat_public_ip_body,
    )
    cluster_nat_gateway_id = _expected_id(
        subscription_id,
        group_name,
        f'Microsoft.Network/natGateways/{cluster_nat_gateway_name}',
    )
    nat_gateway_body: dict[str, Any] = {
        'location': region,
        'tags': _tags(job_id, 'cluster-nat-gateway'),
        'sku': {'name': 'Standard'},
        'properties': {
            'idleTimeoutInMinutes': 4,
            'publicIpAddresses': [{'id': cluster_nat_public_ip_id}],
        },
    }
    if requested_zone:
        nat_gateway_body['zones'] = [requested_zone]
    _ensure_child(
        job,
        persist,
        prefix='azure_dsb_cluster_nat_gateway',
        name=cluster_nat_gateway_name,
        expected_id=cluster_nat_gateway_id,
        service=_require_service(
            _network_service(clients, 'nat_gateways'),
            'NAT gateways',
        ),
        resource_group=group_name,
        body=nat_gateway_body,
    )
    subnet_service = _require_service(
        _network_service(clients, 'subnets'),
        'virtual network subnets',
    )
    subnet_specs = (
        (
            'cluster',
            cluster_subnet_name,
            DISTRIBUTED_CLUSTER_SUBNET_CIDR,
            nsg_ids['azure_dsb_cluster_nsg'],
            cluster_nat_gateway_id,
        ),
        (
            'loadgen',
            loadgen_subnet_name,
            DISTRIBUTED_LOADGEN_SUBNET_CIDR,
            nsg_ids['azure_dsb_loadgen_nsg'],
            None,
        ),
    )
    subnet_ids: dict[str, str] = {}
    for key, name, cidr, nsg_id, nat_gateway_id in subnet_specs:
        prefix = f'azure_dsb_{key}_subnet'
        expected_id = _expected_id(
            subscription_id,
            group_name,
            f'Microsoft.Network/virtualNetworks/{vnet_name}/subnets/{name}',
        )
        _ensure_subnet(
            job,
            persist,
            service=subnet_service,
            subscription_id=subscription_id,
            resource_group=group_name,
            vnet_name=vnet_name,
            prefix=prefix,
            name=name,
            cidr=cidr,
            nsg_id=nsg_id,
            nat_gateway_id=nat_gateway_id,
            disable_default_outbound=True,
            private_endpoint_network_policies='Enabled',
        )
        subnet_ids[key] = expected_id

    disk_body: dict[str, Any] = {
        'location': region,
        'tags': _tags(job_id, 'database-data-disk'),
        'sku': {'name': PREMIUM_V2_DISK_TYPE},
        'properties': {
            'creationData': {'createOption': 'Empty'},
            'diskSizeGB': int(database_storage_contract.size_gib),
            'diskIOPSReadWrite': database_storage_contract.minimum_iops,
            'diskMBpsReadWrite': int(
                database_storage_contract.minimum_throughput_mibps
            ),
            'networkAccessPolicy': 'DenyAll',
            'publicNetworkAccess': 'Disabled',
        },
    }
    if requested_zone:
        disk_body['zones'] = [requested_zone]
    _set_inventory_storage_status(
        job,
        persist,
        'database',
        database_storage_contract.key,
        'creating',
    )
    try:
        _ensure_child(
            job,
            persist,
            prefix='azure_dsb_database_data_disk',
            name=database_disk_name,
            expected_id=database_disk_id,
            service=_require_service(
                _compute_service(clients, 'disks'),
                'managed disks',
            ),
            resource_group=group_name,
            body=disk_body,
        )
    except Exception:
        _set_inventory_storage_status(
            job,
            persist,
            'database',
            database_storage_contract.key,
            'create_ambiguous',
        )
        raise
    _set_inventory_storage_status(
        job,
        persist,
        'database',
        database_storage_contract.key,
        'running',
    )

    for node_key in manifest.creation_order:
        details = role_details[node_key]
        image = application_image if node_key == 'application' else x86_image
        subnet_key = 'loadgen' if node_key == 'load-generator' else 'cluster'
        vm, public, private = _create_vm_role(
            job,
            persist,
            clients,
            inventory_node_key=node_key,
            subscription_id=subscription_id,
            resource_group=group_name,
            region=region,
            zone=requested_zone,
            prefix=node_prefixes[node_key],
            name=node_names[node_key],
            role=node_key,
            vm_size=details['vm_size'],
            image=image,
            subnet_id=subnet_ids[subnet_key],
            public_key=public_key,
            accelerated_networking=bool(details.get('accelerated_networking')),
            os_disk_size_gb=DISTRIBUTED_OS_DISK_SIZE_GB,
            data_disk_id=(database_disk_id if node_key == 'database' else None),
            assign_public_ip=node_key in DISTRIBUTED_PUBLIC_IP_NODES,
            private_ip_address=DISTRIBUTED_PRIVATE_ADDRESSES[node_key],
        )
        normalized_key = node_key.replace('-', '_')
        values: dict[str, Any] = {
            f'azure_dsb_{normalized_key}_instance_id': str(_value(vm, 'id')),
            f'azure_dsb_{normalized_key}_private_ip': private,
            f'azure_dsb_{normalized_key}_public_ip': public,
            f'azure_dsb_{normalized_key}_vm_size': details['vm_size'],
            f'azure_dsb_{normalized_key}_architecture': details['architecture'],
        }
        if node_key == 'application':
            values.update({
                'instance_id': str(_value(vm, 'id')),
                'public_ip': public,
                'private_ip': private,
            })
        elif node_key == 'load-generator':
            values.update({
                'loadgen_instance_id': str(_value(vm, 'id')),
                'loadgen_public_ip': public,
                'loadgen_private_ip': private,
                'loadgen_shape': details['vm_size'],
                'loadgen_vcpus': details['vcpu'],
                'loadgen_memory_gb': details['memory_gb'],
                'loadgen_architecture': details['architecture'],
            })
        _record(job, persist, **values)
        _emit(
            job,
            emit,
            'Provision',
            f'Launched Azure distributed {node_key} role on '
            f'{details["vm_size"]} at {private}.',
        )

    _emit(
        job,
        emit,
        'Provision',
        'Azure distributed candidate infrastructure reached the synthetic '
        'five-node lifecycle boundary; no benchmark workload was installed.',
    )
    return job['resources']


def _azure_contract_keys(resources: Mapping[str, Any]) -> list[str]:
    cloud_keys = [key for key in resources if key.startswith('azure_')]
    canonical = [
        'instance_id', 'public_ip', 'private_ip', 'peer_private_ip',
        'loadgen_instance_id', 'loadgen_public_ip', 'loadgen_private_ip',
        'data_volume_device', ROLE_NODE_INVENTORY_KEY,
        DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
        DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY,
        K3S_RUNTIME_JOURNAL_KEY,
        DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    ]
    return cloud_keys + [key for key in canonical if key in resources]


def _verify_resource_group_inventory(
    clients: Mapping[str, Any],
    resources: Mapping[str, Any],
    *,
    subscription_id: str,
    resource_group: str,
    manifest: DeathStarBenchTopologyManifest | None,
):
    """Reject group deletion when any top-level resource is not expected.

    Resource-group tags establish ownership of the container, but Azure also
    allows principals to add unrelated resources to an existing group.  A
    group-wide delete is therefore safe only when its current top-level
    inventory is a subset of the exact IDs persisted before our deterministic
    PUTs (including VM-managed OS disks).
    """
    inventory = _require_service(
        _resource_inventory(clients), 'resource-group resource inventory'
    )
    group_root = _normalize_id(
        _expected_id(subscription_id, resource_group)
    )
    identity_specs = _validate_managed_expected_ids(
        resources,
        subscription_id=subscription_id,
        resource_group=resource_group,
        manifest=manifest,
    )
    allowed = set()
    managed_os_disk_ids = set()
    for key, (_, expected_id) in identity_specs.items():
        if key not in resources:
            continue
        normalized = _normalize_id(expected_id)
        if not normalized.startswith(group_root + '/providers/'):
            raise RuntimeError(
                'Refusing Azure cleanup because a persisted managed-resource '
                'identity falls outside the exact benchmark resource group.'
            )
        allowed.add(normalized)
        if key.endswith('_os_disk_expected_id'):
            managed_os_disk_ids.add(normalized)
    group_tags = resources.get('azure_resource_group_tags')
    expected_job_id = (
        str(group_tags.get('benchmark-job') or '')
        if isinstance(group_tags, Mapping) else ''
    )
    observed = _iter(inventory.list_by_resource_group(resource_group))
    unexpected = []
    for item in observed:
        resource_id = str(_value(item, 'id', '') or '')
        normalized = _normalize_id(resource_id)
        if (
            not resource_id
            or not normalized.startswith(group_root + '/providers/')
            or normalized not in allowed
        ):
            unexpected.append(resource_id or '<missing resource ID>')
            continue
        if normalized not in managed_os_disk_ids:
            tags = dict(_value(item, 'tags', {}) or {})
            if (
                tags.get('managed-by') != MANAGED_BY
                or tags.get('benchmark-job') != expected_job_id
            ):
                unexpected.append(
                    f'{resource_id or "<missing resource ID>"} '
                    '(ownership tags changed)'
                )
    if unexpected:
        raise RuntimeError(
            'Refusing Azure resource-group deletion because it contains '
            'top-level resources outside this run\'s exact allowlist: '
            + ', '.join(unexpected)
            + '. No resources were changed.'
        )


def destroy_resources(
    job: dict[str, Any],
    *,
    emit: EventCallback | None = None,
    persist: PersistCallback | None = None,
    preserve_status: bool = False,
    credential=None,
    clients=None,
    absence_retry_attempts: int = 6,
    absence_retry_delay_seconds: float = 1,
):
    """Delete one exact owned resource group after fail-closed verification."""
    resources = job.setdefault('resources', {})
    group_name = str(resources.get('azure_resource_group_name') or '')
    has_azure_contract = (
        any(key.startswith('azure_') for key in resources)
        or any(key in resources for key in (
            ROLE_NODE_INVENTORY_KEY,
            DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
            DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY,
        ))
    )
    if not group_name:
        if has_azure_contract:
            raise RuntimeError(
                'Refusing Azure cleanup because managed resource metadata '
                'exists without the resource-group ownership anchor.'
            )
        if K3S_RUNTIME_JOURNAL_KEY in resources:
            _forget(job, persist, K3S_RUNTIME_JOURNAL_KEY)
        if DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY in resources:
            _forget(job, persist, DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY)
        if not preserve_status:
            job['status'] = 'destroyed'
            job['cleanup_error'] = None
            _persist(job, persist)
        return resources

    subscription_id = str(resources.get('azure_subscription_id') or '')
    region = str(resources.get('region') or '')
    expected_id = str(resources.get('azure_resource_group_expected_id') or '')
    required_tags = resources.get('azure_resource_group_tags')
    expected_name = _job_name(str(job.get('id') or '').lower())
    if (
        not subscription_id
        or not region
        or not expected_id
        or not isinstance(required_tags, Mapping)
        or group_name != expected_name
        or dict(required_tags) != _tags(str(job['id']), 'resource-group')
    ):
        raise RuntimeError(
            'Refusing Azure cleanup because the persisted resource-group '
            'ownership contract is incomplete or inconsistent.'
        )
    try:
        topology_inventory = _load_topology_inventory_contract(resources)
    except (ResourceInventoryError, TopologyManifestError, ValueError) as exc:
        raise RuntimeError(
            'Refusing Azure cleanup because the persisted DeathStarBench '
            f'topology inventory is invalid: {exc}'
        ) from exc
    subscription_id, _, clients = _runtime(
        subscription_id, credential, clients
    )
    groups = _require_service(_resource_groups(clients), 'resource groups')
    manifest = topology_inventory[0] if topology_inventory is not None else None

    def reconcile_missing_group():
        if _confirm_absent(
            groups,
            group_name,
            attempts=absence_retry_attempts,
            delay_seconds=absence_retry_delay_seconds,
        ):
            return None
        visible = _get_or_none(groups, group_name)
        if visible is None:
            raise RuntimeError(
                'Azure resource-group visibility changed during absence '
                'reconciliation; cleanup contract retained.'
            )
        return visible

    group = _get_or_none(groups, group_name)
    if group is None:
        # Azure reads are eventually consistent and can transiently return 404
        # even after an earlier successful create. Never discard the ownership
        # contract on one missing read, regardless of the saved ambiguity flag.
        group = reconcile_missing_group()
    if group is not None:
        _verify_group(
            group,
            subscription_id=subscription_id,
            name=group_name,
            region=region,
            required_tags=required_tags,
            saved_id=expected_id,
        )
        # Re-read immediately before the name-only delete to catch a group
        # replacement between preflight and mutation.
        current = _get_or_none(groups, group_name)
        if current is None:
            group = reconcile_missing_group()
            current = group
        if current is not None:
            group = current
            _verify_group(
                current,
                subscription_id=subscription_id,
                name=group_name,
                region=region,
                required_tags=required_tags,
                saved_id=expected_id,
            )
            # This is intentionally the final read before begin_delete. It
            # catches manually added or cross-workload resources instead of
            # trusting the group tag as blanket deletion authority.
            _verify_resource_group_inventory(
                clients,
                resources,
                subscription_id=subscription_id,
                resource_group=group_name,
                manifest=manifest,
            )
            _record(job, persist, azure_resource_group_delete_ambiguous=True)
            try:
                _wait(groups.begin_delete(group_name))
            except Exception:
                # A lost polling response is successful only when exact-name
                # repeated absence can be proven. Otherwise retain the entire
                # contract for a later retry.
                if not _confirm_absent(
                    groups,
                    group_name,
                    attempts=absence_retry_attempts,
                    delay_seconds=absence_retry_delay_seconds,
                ):
                    raise
            remaining = _get_or_none(groups, group_name)
            if remaining is not None:
                raise RuntimeError(
                    'Azure resource-group deletion completed without proving '
                    'absence; the cleanup contract was retained for retry.'
                )
            group = None
        else:
            group = None
    if group is None:
        keys = _azure_contract_keys(resources)
        _forget(job, persist, *keys)
    if not preserve_status:
        job['status'] = 'destroyed'
        job['cleanup_error'] = None
        _persist(job, persist)
        _emit(job, emit, 'Complete', 'Azure infrastructure was destroyed.')
    return resources


cleanup = destroy_resources


__all__ = [
    'DATA_DISK_DEVICE',
    'DEFAULT_REGION',
    'LOADGEN_VM_SIZES',
    'MANAGED_BY',
    'PREMIUM_V2_DISK_TYPE',
    'PREMIUM_V2_IOPS',
    'PREMIUM_V2_THROUGHPUT_MIBPS',
    'ROCKY_IMAGES',
    'SSH_SOURCE_CIDR',
    'SSH_USER',
    'bootstrap',
    'cleanup',
    'create_clients',
    'destroy_resources',
    'instance_types',
    'latest_rocky_linux_9_image',
    'machine_types',
    'normalize_architecture',
    'placement',
    'provision',
    'resolve_rocky_linux_9_image',
    'vm_sizes',
]
