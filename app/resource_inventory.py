"""Provider-neutral persisted inventory for role-assigned compute nodes.

Cloud providers currently persist a flat collection of runner and load-generator
fields in ``job['resources']``.  A distributed benchmark needs more than one
node per role, and every accepted cloud create must remain discoverable after a
process interruption.  This module defines the small, versioned contract used
for that inventory without depending on any cloud SDK.

The serialized representation is deliberately strict.  An older application
must fail closed when it encounters an unsupported schema instead of silently
dropping a node that may still need cleanup.  Legacy flat fields are imported
only when no versioned inventory has been persisted.
"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, replace
from ipaddress import ip_address
import json
import math
from pathlib import PurePosixPath
import re
from typing import Any


ROLE_NODE_INVENTORY_KEY = 'role_node_inventory'
ROLE_NODE_INVENTORY_VERSION = 1

_TOKEN_RE = re.compile(r'^[a-z][a-z0-9_]*$')
_NODE_KEY_RE = re.compile(r'^[a-z][a-z0-9_-]*$')
_FINGERPRINT_RE = re.compile(r'^sha256:[0-9a-f]{64}$')
_KNOWN_LIFECYCLE_STATUSES = frozenset({
    'unknown',
    'planned',
    'creating',
    'create_ambiguous',
    'running',
    'stopping',
    'stopped',
    'deleting',
    'delete_ambiguous',
    'deleted',
    'failed',
})


class ResourceInventoryError(ValueError):
    """Raised when persisted inventory cannot be interpreted safely."""


def _required_token(value: Any, label: str, pattern=_TOKEN_RE) -> str:
    if not isinstance(value, str):
        raise ResourceInventoryError(f'{label} must be a string.')
    normalized = value.strip().lower()
    if normalized != value or not pattern.fullmatch(normalized):
        raise ResourceInventoryError(
            f'{label} must be a normalized lower-case identifier.'
        )
    return normalized


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ResourceInventoryError(f'{label} must be a non-empty string.')
    if value != value.strip():
        raise ResourceInventoryError(
            f'{label} must not contain surrounding whitespace.'
        )
    return value


def _optional_token(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _required_token(value, label)


def _topology_fingerprint(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _FINGERPRINT_RE.fullmatch(value):
        raise ResourceInventoryError(
            'Inventory topology_fingerprint must be a SHA-256 identifier.'
        )
    return value


def _merge_stable_text(
    current: str | None,
    update: str | None,
    label: str,
) -> str | None:
    """Add a stable value without replacing or erasing one already accepted."""
    if current is not None and update is not None and current != update:
        raise ResourceInventoryError(f'{label} cannot be replaced.')
    return update if update is not None else current


def _merge_stable_value(current: Any, update: Any, label: str) -> Any:
    if current is not None and update is not None and current != update:
        raise ResourceInventoryError(f'{label} cannot be replaced.')
    return update if update is not None else current


def _optional_number(
    value: Any,
    label: str,
    *,
    integer: bool = False,
) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ResourceInventoryError(f'{label} must be a positive number.')
    if not math.isfinite(float(value)) or float(value) <= 0:
        raise ResourceInventoryError(f'{label} must be a positive number.')
    if integer:
        if not isinstance(value, int):
            raise ResourceInventoryError(f'{label} must be a positive integer.')
        return value
    return float(value)


def _lifecycle_status(value: Any, label: str) -> str:
    status = _required_token(value, label)
    if status not in _KNOWN_LIFECYCLE_STATUSES:
        supported = ', '.join(sorted(_KNOWN_LIFECYCLE_STATUSES))
        raise ResourceInventoryError(
            f'{label} must be one of: {supported}.'
        )
    return status


def _mount_point(value: Any) -> str | None:
    value = _optional_text(value, 'Storage mount_point')
    if value is None:
        return None
    path = PurePosixPath(value)
    if (
        not value.startswith('/')
        or value.startswith('//')
        or '//' in value
        or value == '/'
        or path.as_posix() != value
        or '..' in path.parts
    ):
        raise ResourceInventoryError(
            'Storage mount_point must be a normalized absolute path.'
        )
    return value


def _addresses(values: Sequence[str], label: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise ResourceInventoryError(f'{label} must be a sequence of IP addresses.')
    parsed = []
    for value in values:
        if not isinstance(value, str):
            raise ResourceInventoryError(
                f'{label} must contain only IP-address strings.'
            )
        try:
            parsed.append(ip_address(value.strip()))
        except ValueError as exc:
            raise ResourceInventoryError(
                f'{label} contains an invalid IP address.'
            ) from exc
    unique = {(item.version, int(item)): item for item in parsed}
    return tuple(
        str(unique[key])
        for key in sorted(unique)
    )


def _exact_fields(value: Mapping[str, Any], expected: set[str], label: str):
    missing = expected - set(value)
    extra = set(value) - expected
    if missing or extra:
        details = []
        if missing:
            details.append(f'missing {", ".join(sorted(missing))}')
        if extra:
            details.append(f'unknown {", ".join(sorted(extra))}')
        raise ResourceInventoryError(
            f'{label} has an invalid schema ({"; ".join(details)}).'
        )


@dataclass(frozen=True, slots=True)
class StorageResource:
    """One storage resource attached to or selected by a role node."""

    key: str
    kind: str
    provider_resource_id: str | None = None
    provider_resource_name: str | None = None
    model: str | None = None
    device: str | None = None
    mount_point: str | None = None
    filesystem: str | None = None
    size_gb: float | None = None
    provisioned_iops: int | None = None
    provisioned_throughput_mibps: float | None = None
    ephemeral: bool | None = None
    lifecycle_status: str = 'unknown'

    def __post_init__(self):
        object.__setattr__(
            self,
            'key',
            _required_token(self.key, 'Storage key', _NODE_KEY_RE),
        )
        object.__setattr__(self, 'kind', _required_token(self.kind, 'Storage kind'))
        for field_name in (
            'provider_resource_id',
            'provider_resource_name',
            'model',
            'device',
            'filesystem',
        ):
            object.__setattr__(
                self,
                field_name,
                _optional_text(
                    getattr(self, field_name),
                    f'Storage {field_name}',
                ),
            )
        object.__setattr__(self, 'mount_point', _mount_point(self.mount_point))
        object.__setattr__(
            self,
            'size_gb',
            _optional_number(self.size_gb, 'Storage size_gb'),
        )
        object.__setattr__(
            self,
            'provisioned_iops',
            _optional_number(
                self.provisioned_iops,
                'Storage provisioned_iops',
                integer=True,
            ),
        )
        object.__setattr__(
            self,
            'provisioned_throughput_mibps',
            _optional_number(
                self.provisioned_throughput_mibps,
                'Storage provisioned_throughput_mibps',
            ),
        )
        if self.ephemeral is not None and not isinstance(self.ephemeral, bool):
            raise ResourceInventoryError('Storage ephemeral must be a boolean.')
        status = _lifecycle_status(
            self.lifecycle_status,
            'Storage lifecycle_status',
        )
        object.__setattr__(self, 'lifecycle_status', status)

    def as_dict(self) -> dict[str, Any]:
        return {
            'key': self.key,
            'kind': self.kind,
            'provider_resource_id': self.provider_resource_id,
            'provider_resource_name': self.provider_resource_name,
            'model': self.model,
            'device': self.device,
            'mount_point': self.mount_point,
            'filesystem': self.filesystem,
            'size_gb': self.size_gb,
            'provisioned_iops': self.provisioned_iops,
            'provisioned_throughput_mibps': (
                self.provisioned_throughput_mibps
            ),
            'ephemeral': self.ephemeral,
            'lifecycle_status': self.lifecycle_status,
        }

    def merge(self, update: StorageResource) -> StorageResource:
        """Merge lifecycle discovery without losing cleanup-critical identity."""
        if not isinstance(update, StorageResource):
            raise ResourceInventoryError(
                'Storage merge requires a StorageResource.'
            )
        if self.key != update.key:
            raise ResourceInventoryError('Storage key cannot be replaced.')
        if self.kind != update.kind:
            raise ResourceInventoryError(
                f'Storage {self.key} kind cannot be replaced.'
            )

        return StorageResource(
            key=self.key,
            kind=self.kind,
            provider_resource_id=_merge_stable_text(
                self.provider_resource_id,
                update.provider_resource_id,
                f'Storage {self.key} provider_resource_id',
            ),
            provider_resource_name=_merge_stable_text(
                self.provider_resource_name,
                update.provider_resource_name,
                f'Storage {self.key} provider_resource_name',
            ),
            model=update.model if update.model is not None else self.model,
            device=_merge_stable_text(
                self.device,
                update.device,
                f'Storage {self.key} device',
            ),
            mount_point=(
                update.mount_point
                if update.mount_point is not None
                else self.mount_point
            ),
            filesystem=(
                update.filesystem
                if update.filesystem is not None
                else self.filesystem
            ),
            size_gb=(
                update.size_gb if update.size_gb is not None else self.size_gb
            ),
            provisioned_iops=(
                update.provisioned_iops
                if update.provisioned_iops is not None
                else self.provisioned_iops
            ),
            provisioned_throughput_mibps=(
                update.provisioned_throughput_mibps
                if update.provisioned_throughput_mibps is not None
                else self.provisioned_throughput_mibps
            ),
            ephemeral=_merge_stable_value(
                self.ephemeral,
                update.ephemeral,
                f'Storage {self.key} ephemeral classification',
            ),
            lifecycle_status=update.lifecycle_status,
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> StorageResource:
        if not isinstance(value, Mapping):
            raise ResourceInventoryError('Storage resource must be an object.')
        expected = {
            'key',
            'kind',
            'provider_resource_id',
            'provider_resource_name',
            'model',
            'device',
            'mount_point',
            'filesystem',
            'size_gb',
            'provisioned_iops',
            'provisioned_throughput_mibps',
            'ephemeral',
            'lifecycle_status',
        }
        _exact_fields(value, expected, 'Storage resource')
        return cls(**{key: value[key] for key in expected})


@dataclass(frozen=True, slots=True)
class RoleNode:
    """A stable logical node and its current provider resource identity."""

    key: str
    role: str
    provider_resource_id: str | None = None
    provider_resource_name: str | None = None
    public_addresses: tuple[str, ...] = ()
    private_addresses: tuple[str, ...] = ()
    zone: str | None = None
    shape: str | None = None
    architecture: str | None = None
    capacity_class: str | None = None
    storage: tuple[StorageResource, ...] = ()
    lifecycle_status: str = 'unknown'

    def __post_init__(self):
        object.__setattr__(
            self,
            'key',
            _required_token(self.key, 'Role-node key', _NODE_KEY_RE),
        )
        object.__setattr__(self, 'role', _required_token(self.role, 'Role'))
        for field_name in (
            'provider_resource_id',
            'provider_resource_name',
            'zone',
            'shape',
            'architecture',
        ):
            object.__setattr__(
                self,
                field_name,
                _optional_text(getattr(self, field_name), f'Node {field_name}'),
            )
        object.__setattr__(
            self,
            'capacity_class',
            _optional_token(self.capacity_class, 'Node capacity_class'),
        )
        object.__setattr__(
            self,
            'public_addresses',
            _addresses(self.public_addresses, 'Node public_addresses'),
        )
        object.__setattr__(
            self,
            'private_addresses',
            _addresses(self.private_addresses, 'Node private_addresses'),
        )
        if (
            isinstance(self.storage, (str, bytes))
            or not isinstance(self.storage, Sequence)
            or any(not isinstance(item, StorageResource) for item in self.storage)
        ):
            raise ResourceInventoryError(
                'Node storage must contain only StorageResource values.'
            )
        storage_by_key = {item.key: item for item in self.storage}
        if len(storage_by_key) != len(self.storage):
            raise ResourceInventoryError('Node storage keys must be unique.')
        object.__setattr__(
            self,
            'storage',
            tuple(storage_by_key[key] for key in sorted(storage_by_key)),
        )
        status = _lifecycle_status(
            self.lifecycle_status,
            'Node lifecycle_status',
        )
        object.__setattr__(self, 'lifecycle_status', status)

    def as_dict(self) -> dict[str, Any]:
        return {
            'key': self.key,
            'role': self.role,
            'provider_resource_id': self.provider_resource_id,
            'provider_resource_name': self.provider_resource_name,
            'public_addresses': list(self.public_addresses),
            'private_addresses': list(self.private_addresses),
            'zone': self.zone,
            'shape': self.shape,
            'architecture': self.architecture,
            'capacity_class': self.capacity_class,
            'storage': [item.as_dict() for item in self.storage],
            'lifecycle_status': self.lifecycle_status,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> RoleNode:
        if not isinstance(value, Mapping):
            raise ResourceInventoryError('Role node must be an object.')
        expected = {
            'key',
            'role',
            'provider_resource_id',
            'provider_resource_name',
            'public_addresses',
            'private_addresses',
            'zone',
            'shape',
            'architecture',
            'capacity_class',
            'storage',
            'lifecycle_status',
        }
        _exact_fields(value, expected, 'Role node')
        storage = value['storage']
        if isinstance(storage, (str, bytes)) or not isinstance(storage, Sequence):
            raise ResourceInventoryError('Role-node storage must be a list.')
        return cls(
            **{
                key: value[key]
                for key in expected
                if key != 'storage'
            },
            storage=tuple(StorageResource.from_dict(item) for item in storage),
        )

    def with_lifecycle_status(self, status: str) -> RoleNode:
        return replace(self, lifecycle_status=status)

    def storage_resource(self, key: str) -> StorageResource | None:
        key = _required_token(key, 'Storage key', _NODE_KEY_RE)
        return next((item for item in self.storage if item.key == key), None)

    def upsert_storage(self, update: StorageResource) -> RoleNode:
        """Add storage or safely merge discovery into an existing resource."""
        if not isinstance(update, StorageResource):
            raise ResourceInventoryError(
                'Node storage upsert requires a StorageResource.'
            )
        storage = {item.key: item for item in self.storage}
        current = storage.get(update.key)
        storage[update.key] = current.merge(update) if current else update
        return replace(self, storage=tuple(storage.values()))

    def without_storage(self, key: str) -> RoleNode:
        """Forget storage only when it was never created or is confirmed deleted."""
        key = _required_token(key, 'Storage key', _NODE_KEY_RE)
        storage = self.storage_resource(key)
        if storage is None:
            return self
        if not _storage_can_be_forgotten(storage):
            raise ResourceInventoryError(
                f'Storage {key} cannot be removed until it is confirmed deleted.'
            )
        return replace(
            self,
            storage=tuple(item for item in self.storage if item.key != key),
        )


@dataclass(frozen=True, slots=True)
class RoleNodeInventory:
    """Immutable collection of nodes persisted as one versioned resource."""

    provider: str | None = None
    topology_fingerprint: str | None = None
    nodes: tuple[RoleNode, ...] = ()

    def __post_init__(self):
        if self.provider is not None:
            object.__setattr__(
                self,
                'provider',
                _required_token(self.provider, 'Inventory provider'),
            )
        object.__setattr__(
            self,
            'topology_fingerprint',
            _topology_fingerprint(self.topology_fingerprint),
        )
        if (
            isinstance(self.nodes, (str, bytes))
            or not isinstance(self.nodes, Sequence)
            or any(not isinstance(item, RoleNode) for item in self.nodes)
        ):
            raise ResourceInventoryError(
                'Inventory nodes must contain only RoleNode values.'
            )
        nodes_by_key = {item.key: item for item in self.nodes}
        if len(nodes_by_key) != len(self.nodes):
            raise ResourceInventoryError('Role-node keys must be unique.')
        object.__setattr__(
            self,
            'nodes',
            tuple(nodes_by_key[key] for key in sorted(nodes_by_key)),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            'schema_version': ROLE_NODE_INVENTORY_VERSION,
            'provider': self.provider,
            'topology_fingerprint': self.topology_fingerprint,
            'nodes': [node.as_dict() for node in self.nodes],
        }

    def canonical_json(self) -> str:
        """Return stable bytes for persistence, comparison, or hashing."""
        return json.dumps(
            self.as_dict(),
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
            allow_nan=False,
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> RoleNodeInventory:
        if not isinstance(value, Mapping):
            raise ResourceInventoryError('Role-node inventory must be an object.')
        expected = {
            'schema_version',
            'provider',
            'topology_fingerprint',
            'nodes',
        }
        _exact_fields(value, expected, 'Role-node inventory')
        version = value['schema_version']
        if type(version) is not int or version != ROLE_NODE_INVENTORY_VERSION:
            raise ResourceInventoryError(
                f'Unsupported role-node inventory schema version {version!r}.'
            )
        nodes = value['nodes']
        if isinstance(nodes, (str, bytes)) or not isinstance(nodes, Sequence):
            raise ResourceInventoryError('Role-node inventory nodes must be a list.')
        return cls(
            provider=value['provider'],
            topology_fingerprint=value['topology_fingerprint'],
            nodes=tuple(RoleNode.from_dict(node) for node in nodes),
        )

    def node(self, key: str) -> RoleNode | None:
        key = _required_token(key, 'Role-node key', _NODE_KEY_RE)
        return next((node for node in self.nodes if node.key == key), None)

    def nodes_for_role(self, role: str) -> tuple[RoleNode, ...]:
        role = _required_token(role, 'Role')
        return tuple(node for node in self.nodes if node.role == role)

    def upsert(self, node: RoleNode) -> RoleNodeInventory:
        if not isinstance(node, RoleNode):
            raise ResourceInventoryError('Inventory upsert requires a RoleNode.')
        nodes = {item.key: item for item in self.nodes}
        current = nodes.get(node.key)
        nodes[node.key] = _merge_role_node(current, node) if current else node
        return RoleNodeInventory(
            provider=self.provider,
            topology_fingerprint=self.topology_fingerprint,
            nodes=tuple(nodes.values()),
        )

    def without(self, key: str) -> RoleNodeInventory:
        key = _required_token(key, 'Role-node key', _NODE_KEY_RE)
        node = self.node(key)
        if node is None:
            return self
        if not _node_can_be_forgotten(node):
            raise ResourceInventoryError(
                f'Role node {key} cannot be removed until it and its storage '
                'are confirmed deleted.'
            )
        return RoleNodeInventory(
            provider=self.provider,
            topology_fingerprint=self.topology_fingerprint,
            nodes=tuple(node for node in self.nodes if node.key != key),
        )


def _storage_has_identity(storage: StorageResource) -> bool:
    return any((
        storage.provider_resource_id,
        storage.provider_resource_name,
        storage.device,
    ))


def _storage_can_be_forgotten(storage: StorageResource) -> bool:
    return (
        storage.lifecycle_status == 'deleted'
        or (
            storage.lifecycle_status == 'planned'
            and not _storage_has_identity(storage)
        )
    )


def _node_has_identity(node: RoleNode) -> bool:
    return any((
        node.provider_resource_id,
        node.provider_resource_name,
        node.public_addresses,
        node.private_addresses,
    ))


def _node_can_be_forgotten(node: RoleNode) -> bool:
    storage_is_safe = all(_storage_can_be_forgotten(item) for item in node.storage)
    return storage_is_safe and (
        node.lifecycle_status == 'deleted'
        or (
            node.lifecycle_status == 'planned'
            and not _node_has_identity(node)
        )
    )


def _merge_role_node(current: RoleNode, update: RoleNode) -> RoleNode:
    if current.key != update.key:
        raise ResourceInventoryError('Role-node key cannot be replaced.')
    if current.role != update.role:
        raise ResourceInventoryError(
            f'Role node {current.key} role cannot be replaced.'
        )

    storage = {item.key: item for item in current.storage}
    for item in update.storage:
        existing = storage.get(item.key)
        storage[item.key] = existing.merge(item) if existing else item

    return RoleNode(
        key=current.key,
        role=current.role,
        provider_resource_id=_merge_stable_text(
            current.provider_resource_id,
            update.provider_resource_id,
            f'Role node {current.key} provider_resource_id',
        ),
        provider_resource_name=_merge_stable_text(
            current.provider_resource_name,
            update.provider_resource_name,
            f'Role node {current.key} provider_resource_name',
        ),
        public_addresses=tuple(sorted(set(
            current.public_addresses + update.public_addresses
        ))),
        private_addresses=tuple(sorted(set(
            current.private_addresses + update.private_addresses
        ))),
        zone=_merge_stable_text(
            current.zone,
            update.zone,
            f'Role node {current.key} zone',
        ),
        shape=_merge_stable_text(
            current.shape,
            update.shape,
            f'Role node {current.key} shape',
        ),
        architecture=_merge_stable_text(
            current.architecture,
            update.architecture,
            f'Role node {current.key} architecture',
        ),
        capacity_class=_merge_stable_text(
            current.capacity_class,
            update.capacity_class,
            f'Role node {current.key} capacity_class',
        ),
        storage=tuple(storage.values()),
        lifecycle_status=update.lifecycle_status,
    )


def _value(source: Any, key: str) -> Any:
    if isinstance(source, Mapping):
        return source.get(key)
    return getattr(source, key, None)


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None


def _first_number(*values: Any, integer=False) -> int | float | None:
    for value in values:
        if value is None or isinstance(value, bool):
            continue
        try:
            parsed = int(value) if integer else float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(float(parsed)) and parsed > 0:
            if integer and float(value) != parsed:
                continue
            return parsed
    return None


def _legacy_storage(resources: Mapping[str, Any]) -> tuple[StorageResource, ...]:
    provider = _first_text(resources.get('provider'))
    prefix = f'{provider}_' if provider else ''
    descriptor = resources.get('benchmark_storage_target')
    descriptor = descriptor if isinstance(descriptor, Mapping) else {}
    kind = _first_text(descriptor.get('storage_target_kind'))

    resource_id = _first_text(
        resources.get(f'{prefix}data_volume_id'),
        resources.get(f'{prefix}data_disk_id'),
        resources.get('volume_id'),
    )
    resource_name = _first_text(
        resources.get(f'{prefix}data_volume_name'),
        resources.get(f'{prefix}data_disk_name'),
    )
    size_gb = _first_number(
        (
            float(descriptor['storage_target_capacity_bytes']) / 1024**3
            if isinstance(descriptor.get('storage_target_capacity_bytes'), (int, float))
            and not isinstance(descriptor.get('storage_target_capacity_bytes'), bool)
            else None
        ),
        resources.get(f'{prefix}data_volume_size_gb'),
        resources.get(f'{prefix}data_disk_size_gb'),
    )
    provisioned_iops = _first_number(
        resources.get(f'{prefix}data_disk_provisioned_iops'),
        resources.get(f'{prefix}data_volume_iops'),
        resources.get(f'{prefix}data_disk_iops'),
        integer=True,
    )
    provisioned_throughput_mibps = _first_number(
        resources.get(f'{prefix}data_disk_provisioned_throughput_mibps'),
        resources.get(f'{prefix}data_volume_throughput_mibps'),
        resources.get(f'{prefix}data_disk_throughput_mibps'),
    )
    if not any((
        descriptor,
        resource_id,
        resource_name,
        size_gb,
        provisioned_iops,
        provisioned_throughput_mibps,
    )):
        return ()

    local = kind == 'instance_local_nvme'
    return (StorageResource(
        key='benchmark',
        kind=kind or 'data_volume',
        provider_resource_id=None if local else resource_id,
        provider_resource_name=None if local else resource_name,
        model=_first_text(descriptor.get('storage_target_model')),
        device=_first_text(
            resources.get(f'{prefix}data_volume_device'),
            resources.get(f'{prefix}data_disk_device'),
            resources.get('oci_data_volume_device'),
        ),
        mount_point=_first_text(
            descriptor.get('storage_target_mount_point'),
            '/benchmark-local' if local else '/data',
        ),
        filesystem=_first_text(descriptor.get('storage_target_filesystem')),
        size_gb=size_gb,
        provisioned_iops=provisioned_iops,
        provisioned_throughput_mibps=provisioned_throughput_mibps,
        ephemeral=True if local else False,
    ),)


def _legacy_node(
    resources: Mapping[str, Any],
    plan: Any,
    *,
    key: str,
    role: str,
    prefix: str,
) -> RoleNode | None:
    provider = _first_text(resources.get('provider'), _value(plan, 'provider'))
    provider_prefix = f'{provider}_' if provider else ''
    generic_prefix = f'{prefix}_' if prefix else ''

    resource_id = _first_text(
        resources.get(f'{provider_prefix}{generic_prefix}instance_id'),
        resources.get(f'{generic_prefix}instance_id'),
    )
    resource_name = _first_text(
        resources.get(f'{provider_prefix}{generic_prefix}instance_name'),
        resources.get(f'{provider_prefix}{generic_prefix}vm_name'),
    )
    public_address = _first_text(
        resources.get(f'{generic_prefix}public_ip'),
        resources.get(f'{provider_prefix}{generic_prefix}public_ip'),
    )
    private_address = _first_text(
        resources.get(f'{generic_prefix}private_ip'),
        resources.get(f'{provider_prefix}{generic_prefix}private_ip'),
    )
    if prefix:
        shape = _first_text(
            resources.get(f'{generic_prefix}shape'),
            resources.get(f'{provider_prefix}{generic_prefix}instance_type'),
            resources.get(f'{provider_prefix}{generic_prefix}machine_type'),
            resources.get(f'{provider_prefix}{generic_prefix}vm_size'),
        )
        architecture = _first_text(
            resources.get(f'{generic_prefix}architecture'),
            resources.get(f'{provider_prefix}{generic_prefix}architecture'),
        )
        zone = _first_text(
            resources.get(f'{provider_prefix}{generic_prefix}availability_zone'),
            resources.get('availability_zone'),
        )
    else:
        shape = _first_text(
            resources.get('shape'),
            resources.get('instance_type'),
            resources.get(f'{provider_prefix}machine_type'),
            resources.get(f'{provider_prefix}vm_size'),
            _value(plan, 'shape'),
            _value(plan, 'instance_type'),
            _value(plan, 'machine_type'),
            _value(plan, 'vm_size'),
        )
        architecture = _first_text(
            resources.get('architecture'),
            resources.get(f'{provider_prefix}architecture'),
        )
        zone = _first_text(
            resources.get('availability_zone'),
            resources.get(f'{provider_prefix}zone'),
            _value(plan, 'availability_zone'),
            _value(plan, 'availability_domain'),
        )

    # Pre-create shape metadata is enough to represent a planned role.  This
    # lets a later provider integration persist the logical inventory before
    # the first cloud API call and then upsert its accepted resource identity.
    if not any((resource_id, resource_name, public_address, private_address, shape)):
        return None

    status = _first_text(
        resources.get(f'{provider_prefix}{generic_prefix}lifecycle_status'),
        resources.get(f'{generic_prefix}lifecycle_status'),
    ) or 'unknown'
    return RoleNode(
        key=key,
        role=role,
        provider_resource_id=resource_id,
        provider_resource_name=resource_name,
        public_addresses=(public_address,) if public_address else (),
        private_addresses=(private_address,) if private_address else (),
        zone=zone,
        shape=shape,
        architecture=architecture,
        storage=_legacy_storage(resources) if role == 'runner' else (),
        lifecycle_status=status.lower(),
    )


def inventory_from_legacy_resources(
    resources: Mapping[str, Any],
    *,
    plan: Any = None,
) -> RoleNodeInventory:
    """Read the former runner/load-generator fields without mutating them."""
    if not isinstance(resources, Mapping):
        raise ResourceInventoryError('Job resources must be an object.')
    provider = _first_text(resources.get('provider'), _value(plan, 'provider'))
    if provider:
        provider = provider.lower()
    nodes = tuple(
        node
        for node in (
            _legacy_node(
                resources,
                plan,
                key='runner',
                role='runner',
                prefix='',
            ),
            _legacy_node(
                resources,
                plan,
                key='load-generator',
                role='load_generator',
                prefix='loadgen',
            ),
        )
        if node is not None
    )
    return RoleNodeInventory(provider=provider, nodes=nodes)


def load_role_node_inventory(
    resources: Mapping[str, Any],
    *,
    plan: Any = None,
) -> RoleNodeInventory:
    """Load the versioned inventory, or import legacy fields when absent.

    A malformed or newer persisted inventory is never replaced with a legacy
    guess.  That fail-closed behavior protects cleanup from losing resources.
    """
    if not isinstance(resources, Mapping):
        raise ResourceInventoryError('Job resources must be an object.')
    if ROLE_NODE_INVENTORY_KEY not in resources:
        return inventory_from_legacy_resources(resources, plan=plan)

    inventory = RoleNodeInventory.from_dict(resources[ROLE_NODE_INVENTORY_KEY])
    legacy_provider = _first_text(
        resources.get('provider'),
        _value(plan, 'provider'),
    )
    if (
        legacy_provider
        and inventory.provider
        and legacy_provider.lower() != inventory.provider
    ):
        raise ResourceInventoryError(
            'Role-node inventory provider conflicts with job resources.'
        )
    return inventory


def persist_role_node_inventory(
    resources: MutableMapping[str, Any],
    inventory: RoleNodeInventory,
) -> dict[str, Any]:
    """Atomically replace the nested contract after complete validation."""
    if not isinstance(resources, MutableMapping):
        raise ResourceInventoryError('Job resources must be mutable.')
    if not isinstance(inventory, RoleNodeInventory):
        raise ResourceInventoryError(
            'Persisted role-node inventory must be a RoleNodeInventory.'
        )
    serialized = inventory.as_dict()
    # Exercise the strict reader before replacing a cleanup-critical manifest.
    RoleNodeInventory.from_dict(serialized)
    resources[ROLE_NODE_INVENTORY_KEY] = serialized
    return serialized


def upsert_role_node(
    resources: MutableMapping[str, Any],
    node: RoleNode,
    *,
    plan: Any = None,
) -> RoleNodeInventory:
    """Copy-on-write convenience operation for provider lifecycle code."""
    inventory = load_role_node_inventory(resources, plan=plan).upsert(node)
    persist_role_node_inventory(resources, inventory)
    return inventory


__all__ = [
    'ROLE_NODE_INVENTORY_KEY',
    'ROLE_NODE_INVENTORY_VERSION',
    'ResourceInventoryError',
    'RoleNode',
    'RoleNodeInventory',
    'StorageResource',
    'inventory_from_legacy_resources',
    'load_role_node_inventory',
    'persist_role_node_inventory',
    'upsert_role_node',
]
