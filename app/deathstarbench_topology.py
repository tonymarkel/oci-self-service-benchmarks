"""Deterministic, provider-neutral DeathStarBench topology manifests.

This module models *what* a benchmark run needs without deciding which cloud
shape satisfies a supporting role.  Provider adapters can resolve the
versioned capacity classes later, then persist their accepted resources via
``app.resource_inventory``.

The manifest is deliberately strict and self-fingerprinting.  A topology or
capacity change therefore requires a new versioned identifier instead of
silently changing the meaning of historical benchmark results.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import hmac
import json
import math
import re
from types import MappingProxyType
from typing import Any

from .deathstarbench_contract import (
    DISTRIBUTED_TIERED_PROFILE,
    SINGLE_HOST_PROFILE,
    runtime_profile,
)
from .resource_inventory import (
    RoleNode,
    RoleNodeInventory,
    StorageResource,
)


TOPOLOGY_MANIFEST_SCHEMA_VERSION = 1

SINGLE_HOST_NODE_KEYS = ('runner', 'load-generator')
DISTRIBUTED_TIERED_NODE_KEYS = (
    'control',
    'database',
    'cache',
    'application',
    'load-generator',
)

SELECTED_SHAPE_CAPACITY_CLASS = 'selected_benchmark_shape_v1'
LOAD_GENERATOR_CAPACITY_CLASS = 'load_generator_2vcpu_8gib_x86_v1'
CONTROL_CAPACITY_CLASS = 'control_2vcpu_4gib_x86_v1'
CACHE_CAPACITY_CLASS = 'cache_2vcpu_8gib_x86_v1'
DATABASE_CAPACITY_CLASS = 'database_4vcpu_16gib_x86_v1'
DATABASE_STORAGE_CLASS = 'database_persistent_storage_v1'

BENCHMARK_INGRESS_CHANNEL = 'benchmark_ingress_v1'
K3S_CONTROL_PLANE_CHANNEL = 'k3s_control_plane_v1'
K3S_OVERLAY_CHANNEL = 'k3s_overlay_v1'
CACHE_DATA_CHANNEL = 'cache_data_v1'
DATABASE_DATA_CHANNEL = 'database_data_v1'

_TOKEN_RE = re.compile(r'^[a-z][a-z0-9_]*$')
_NODE_KEY_RE = re.compile(r'^[a-z][a-z0-9_-]*$')
_REVISION_RE = re.compile(r'^[a-z0-9][a-z0-9._+\-]*$')
_FINGERPRINT_RE = re.compile(r'^sha256:[0-9a-f]{64}$')
_ARCHITECTURE_ALIASES = {
    'amd64': 'x86_64',
    'x86-64': 'x86_64',
    'x86_64': 'x86_64',
    'aarch64': 'arm64',
    'arm64': 'arm64',
}


class TopologyManifestError(ValueError):
    """Raised when a topology document does not satisfy its exact contract."""


def _exact_fields(value: Mapping[str, Any], expected: set[str], label: str):
    missing = expected - set(value)
    extra = set(value) - expected
    if missing or extra:
        details = []
        if missing:
            details.append(f'missing {", ".join(sorted(missing))}')
        if extra:
            details.append(f'unknown {", ".join(sorted(extra))}')
        raise TopologyManifestError(
            f'{label} has an invalid schema ({"; ".join(details)}).'
        )


def _identifier(value: Any, label: str, pattern=_TOKEN_RE) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise TopologyManifestError(
            f'{label} must be a normalized lower-case identifier.'
        )
    return value


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or value != value.strip():
        raise TopologyManifestError(
            f'{label} must be a non-empty string without surrounding whitespace.'
        )
    return value


def _positive_number(
    value: Any,
    label: str,
    *,
    optional: bool = False,
) -> int | float | None:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TopologyManifestError(f'{label} must be a positive number.')
    if not math.isfinite(float(value)) or value <= 0:
        raise TopologyManifestError(f'{label} must be a positive number.')
    return value


def _normalize_architecture(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TopologyManifestError('Node architecture must be a string.')
    try:
        return _ARCHITECTURE_ALIASES[value.strip().lower()]
    except KeyError:
        raise TopologyManifestError(
            f'Unsupported node architecture {value!r}.'
        ) from None


@dataclass(frozen=True, slots=True)
class CapacityContract:
    """An abstract provider-neutral compute-capacity requirement."""

    key: str
    minimum_vcpus: int | None
    minimum_memory_gib: float | None
    required_architecture: str | None

    def __post_init__(self):
        object.__setattr__(self, 'key', _identifier(self.key, 'Capacity key'))
        vcpus = _positive_number(
            self.minimum_vcpus,
            'Capacity minimum_vcpus',
            optional=True,
        )
        if vcpus is not None and type(vcpus) is not int:
            raise TopologyManifestError(
                'Capacity minimum_vcpus must be a positive integer.'
            )
        memory = _positive_number(
            self.minimum_memory_gib,
            'Capacity minimum_memory_gib',
            optional=True,
        )
        object.__setattr__(
            self,
            'minimum_memory_gib',
            float(memory) if memory is not None else None,
        )
        object.__setattr__(
            self,
            'required_architecture',
            _normalize_architecture(self.required_architecture),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            'key': self.key,
            'minimum_vcpus': self.minimum_vcpus,
            'minimum_memory_gib': self.minimum_memory_gib,
            'required_architecture': self.required_architecture,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> CapacityContract:
        if not isinstance(value, Mapping):
            raise TopologyManifestError('Capacity contract must be an object.')
        expected = {
            'key',
            'minimum_vcpus',
            'minimum_memory_gib',
            'required_architecture',
        }
        _exact_fields(value, expected, 'Capacity contract')
        return cls(**{key: value[key] for key in expected})


SELECTED_SHAPE_CAPACITY = CapacityContract(
    key=SELECTED_SHAPE_CAPACITY_CLASS,
    minimum_vcpus=None,
    minimum_memory_gib=None,
    required_architecture=None,
)
LOAD_GENERATOR_CAPACITY = CapacityContract(
    key=LOAD_GENERATOR_CAPACITY_CLASS,
    minimum_vcpus=2,
    minimum_memory_gib=8,
    required_architecture='x86_64',
)
CONTROL_CAPACITY = CapacityContract(
    key=CONTROL_CAPACITY_CLASS,
    minimum_vcpus=2,
    minimum_memory_gib=4,
    required_architecture='x86_64',
)
CACHE_CAPACITY = CapacityContract(
    key=CACHE_CAPACITY_CLASS,
    minimum_vcpus=2,
    minimum_memory_gib=8,
    required_architecture='x86_64',
)
DATABASE_CAPACITY = CapacityContract(
    key=DATABASE_CAPACITY_CLASS,
    minimum_vcpus=4,
    minimum_memory_gib=16,
    required_architecture='x86_64',
)

CAPACITY_CONTRACTS = MappingProxyType({
    item.key: item
    for item in (
        SELECTED_SHAPE_CAPACITY,
        LOAD_GENERATOR_CAPACITY,
        CONTROL_CAPACITY,
        CACHE_CAPACITY,
        DATABASE_CAPACITY,
    )
})


@dataclass(frozen=True, slots=True)
class PersistentStorageContract:
    """A normalized persistent disk requirement for a logical node."""

    key: str
    capacity_class: str
    size_gib: float
    minimum_iops: int
    minimum_throughput_mibps: float
    filesystem: str
    mount_point: str

    def __post_init__(self):
        object.__setattr__(
            self,
            'key',
            _identifier(self.key, 'Storage key', _NODE_KEY_RE),
        )
        object.__setattr__(
            self,
            'capacity_class',
            _identifier(self.capacity_class, 'Storage capacity class'),
        )
        object.__setattr__(
            self,
            'size_gib',
            float(_positive_number(self.size_gib, 'Storage size_gib')),
        )
        iops = _positive_number(self.minimum_iops, 'Storage minimum_iops')
        if type(iops) is not int:
            raise TopologyManifestError(
                'Storage minimum_iops must be a positive integer.'
            )
        object.__setattr__(
            self,
            'minimum_throughput_mibps',
            float(
                _positive_number(
                    self.minimum_throughput_mibps,
                    'Storage minimum_throughput_mibps',
                )
            ),
        )
        object.__setattr__(
            self,
            'filesystem',
            _identifier(self.filesystem, 'Storage filesystem'),
        )
        mount_point = _optional_text(self.mount_point, 'Storage mount_point')
        if (
            mount_point is None
            or not mount_point.startswith('/')
            or '..' in mount_point.split('/')
        ):
            raise TopologyManifestError(
                'Storage mount_point must be a normalized absolute path.'
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            'key': self.key,
            'capacity_class': self.capacity_class,
            'size_gib': self.size_gib,
            'minimum_iops': self.minimum_iops,
            'minimum_throughput_mibps': self.minimum_throughput_mibps,
            'filesystem': self.filesystem,
            'mount_point': self.mount_point,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> PersistentStorageContract:
        if not isinstance(value, Mapping):
            raise TopologyManifestError('Storage contract must be an object.')
        expected = {
            'key',
            'capacity_class',
            'size_gib',
            'minimum_iops',
            'minimum_throughput_mibps',
            'filesystem',
            'mount_point',
        }
        _exact_fields(value, expected, 'Storage contract')
        return cls(**{key: value[key] for key in expected})

    def as_resource(self) -> StorageResource:
        """Create the cleanup-safe planned inventory representation."""

        return StorageResource(
            key=self.key,
            kind='persistent_block_volume',
            mount_point=self.mount_point,
            filesystem=self.filesystem,
            size_gb=self.size_gib,
            provisioned_iops=self.minimum_iops,
            provisioned_throughput_mibps=self.minimum_throughput_mibps,
            ephemeral=False,
            lifecycle_status='planned',
        )


DATABASE_STORAGE = PersistentStorageContract(
    key='database-data',
    capacity_class=DATABASE_STORAGE_CLASS,
    size_gib=100,
    minimum_iops=3000,
    minimum_throughput_mibps=125,
    filesystem='xfs',
    mount_point='/var/lib/deathstarbench/database',
)


@dataclass(frozen=True, slots=True)
class LogicalNode:
    """One deterministic logical node before a provider creates a VM."""

    key: str
    role: str
    capacity: CapacityContract
    selected_shape: str | None = None
    architecture: str | None = None
    storage: tuple[PersistentStorageContract, ...] = ()

    def __post_init__(self):
        object.__setattr__(
            self,
            'key',
            _identifier(self.key, 'Logical-node key', _NODE_KEY_RE),
        )
        object.__setattr__(self, 'role', _identifier(self.role, 'Node role'))
        if not isinstance(self.capacity, CapacityContract):
            raise TopologyManifestError(
                'Logical-node capacity must be a CapacityContract.'
            )
        object.__setattr__(
            self,
            'selected_shape',
            _optional_text(self.selected_shape, 'Node selected_shape'),
        )
        architecture = _normalize_architecture(self.architecture)
        required_architecture = self.capacity.required_architecture
        if (
            required_architecture is not None
            and architecture != required_architecture
        ):
            raise TopologyManifestError(
                f'Node {self.key} must use {required_architecture} architecture.'
            )
        object.__setattr__(self, 'architecture', architecture)
        if (
            isinstance(self.storage, (str, bytes))
            or not isinstance(self.storage, Sequence)
            or any(
                not isinstance(item, PersistentStorageContract)
                for item in self.storage
            )
        ):
            raise TopologyManifestError(
                'Node storage must contain only PersistentStorageContract values.'
            )
        keys = [item.key for item in self.storage]
        if len(keys) != len(set(keys)):
            raise TopologyManifestError('Node storage keys must be unique.')
        object.__setattr__(self, 'storage', tuple(self.storage))

    def as_dict(self) -> dict[str, Any]:
        return {
            'key': self.key,
            'role': self.role,
            'capacity': self.capacity.as_dict(),
            'selected_shape': self.selected_shape,
            'architecture': self.architecture,
            'storage': [item.as_dict() for item in self.storage],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> LogicalNode:
        if not isinstance(value, Mapping):
            raise TopologyManifestError('Logical node must be an object.')
        expected = {
            'key',
            'role',
            'capacity',
            'selected_shape',
            'architecture',
            'storage',
        }
        _exact_fields(value, expected, 'Logical node')
        storage = value['storage']
        if isinstance(storage, (str, bytes)) or not isinstance(storage, Sequence):
            raise TopologyManifestError('Logical-node storage must be a list.')
        return cls(
            key=value['key'],
            role=value['role'],
            capacity=CapacityContract.from_dict(value['capacity']),
            selected_shape=value['selected_shape'],
            architecture=value['architecture'],
            storage=tuple(
                PersistentStorageContract.from_dict(item) for item in storage
            ),
        )

    def as_role_node(self) -> RoleNode:
        return RoleNode(
            key=self.key,
            role=self.role,
            capacity_class=self.capacity.key,
            shape=self.selected_shape,
            architecture=self.architecture,
            storage=tuple(item.as_resource() for item in self.storage),
            lifecycle_status='planned',
        )


@dataclass(frozen=True, slots=True)
class NetworkChannel:
    """A versioned transport/port contract used by reachability rules."""

    key: str
    protocol: str
    ports: tuple[int, ...]

    def __post_init__(self):
        object.__setattr__(self, 'key', _identifier(self.key, 'Channel key'))
        if self.protocol not in {'tcp', 'udp'}:
            raise TopologyManifestError('Channel protocol must be tcp or udp.')
        if (
            isinstance(self.ports, (str, bytes))
            or not isinstance(self.ports, Sequence)
            or not self.ports
            or any(
                type(port) is not int or port < 1 or port > 65535
                for port in self.ports
            )
        ):
            raise TopologyManifestError(
                'Channel ports must be a non-empty sequence of valid ports.'
            )
        if tuple(sorted(set(self.ports))) != tuple(self.ports):
            raise TopologyManifestError(
                'Channel ports must be unique and sorted.'
            )
        object.__setattr__(self, 'ports', tuple(self.ports))

    def as_dict(self) -> dict[str, Any]:
        return {
            'key': self.key,
            'protocol': self.protocol,
            'ports': list(self.ports),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> NetworkChannel:
        if not isinstance(value, Mapping):
            raise TopologyManifestError('Network channel must be an object.')
        expected = {'key', 'protocol', 'ports'}
        _exact_fields(value, expected, 'Network channel')
        ports = value['ports']
        if isinstance(ports, (str, bytes)) or not isinstance(ports, Sequence):
            raise TopologyManifestError('Network-channel ports must be a list.')
        return cls(
            key=value['key'],
            protocol=value['protocol'],
            ports=tuple(ports),
        )


@dataclass(frozen=True, slots=True)
class ReachabilityRule:
    """Allowed initiated traffic; every omitted matrix cell is denied."""

    source: str
    destination: str
    channels: tuple[str, ...]

    def __post_init__(self):
        object.__setattr__(
            self,
            'source',
            _identifier(self.source, 'Rule source', _NODE_KEY_RE),
        )
        object.__setattr__(
            self,
            'destination',
            _identifier(self.destination, 'Rule destination', _NODE_KEY_RE),
        )
        if self.source == self.destination:
            raise TopologyManifestError(
                'Reachability rules must connect different nodes.'
            )
        if (
            isinstance(self.channels, (str, bytes))
            or not isinstance(self.channels, Sequence)
            or not self.channels
        ):
            raise TopologyManifestError(
                'Reachability-rule channels must be a non-empty sequence.'
            )
        normalized = tuple(
            _identifier(item, 'Rule channel') for item in self.channels
        )
        if len(normalized) != len(set(normalized)):
            raise TopologyManifestError('Reachability-rule channels must be unique.')
        object.__setattr__(self, 'channels', normalized)

    def as_dict(self) -> dict[str, Any]:
        return {
            'source': self.source,
            'destination': self.destination,
            'channels': list(self.channels),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ReachabilityRule:
        if not isinstance(value, Mapping):
            raise TopologyManifestError('Reachability rule must be an object.')
        expected = {'source', 'destination', 'channels'}
        _exact_fields(value, expected, 'Reachability rule')
        channels = value['channels']
        if isinstance(channels, (str, bytes)) or not isinstance(channels, Sequence):
            raise TopologyManifestError('Reachability-rule channels must be a list.')
        return cls(
            source=value['source'],
            destination=value['destination'],
            channels=tuple(channels),
        )


@dataclass(frozen=True, slots=True)
class NetworkReachabilityPolicy:
    """A closed, auditable node-to-node reachability matrix."""

    default_action: str
    channels: tuple[NetworkChannel, ...]
    rules: tuple[ReachabilityRule, ...]

    def __post_init__(self):
        if self.default_action != 'deny':
            raise TopologyManifestError(
                'Network reachability must default to deny.'
            )
        if (
            isinstance(self.channels, (str, bytes))
            or not isinstance(self.channels, Sequence)
            or any(not isinstance(item, NetworkChannel) for item in self.channels)
        ):
            raise TopologyManifestError(
                'Policy channels must contain only NetworkChannel values.'
            )
        channel_keys = [item.key for item in self.channels]
        if len(channel_keys) != len(set(channel_keys)):
            raise TopologyManifestError('Policy channel keys must be unique.')
        if (
            isinstance(self.rules, (str, bytes))
            or not isinstance(self.rules, Sequence)
            or any(not isinstance(item, ReachabilityRule) for item in self.rules)
        ):
            raise TopologyManifestError(
                'Policy rules must contain only ReachabilityRule values.'
            )
        pairs = [(item.source, item.destination) for item in self.rules]
        if len(pairs) != len(set(pairs)):
            raise TopologyManifestError(
                'Policy may contain only one rule per source/destination pair.'
            )
        known_channels = set(channel_keys)
        for rule in self.rules:
            unknown = set(rule.channels) - known_channels
            if unknown:
                raise TopologyManifestError(
                    'Reachability rule references unknown channels: '
                    + ', '.join(sorted(unknown))
                    + '.'
                )
        object.__setattr__(self, 'channels', tuple(self.channels))
        object.__setattr__(self, 'rules', tuple(self.rules))

    def as_dict(self) -> dict[str, Any]:
        return {
            'default_action': self.default_action,
            'channels': [item.as_dict() for item in self.channels],
            'rules': [item.as_dict() for item in self.rules],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> NetworkReachabilityPolicy:
        if not isinstance(value, Mapping):
            raise TopologyManifestError('Network policy must be an object.')
        expected = {'default_action', 'channels', 'rules'}
        _exact_fields(value, expected, 'Network policy')
        channels = value['channels']
        rules = value['rules']
        if isinstance(channels, (str, bytes)) or not isinstance(channels, Sequence):
            raise TopologyManifestError('Network-policy channels must be a list.')
        if isinstance(rules, (str, bytes)) or not isinstance(rules, Sequence):
            raise TopologyManifestError('Network-policy rules must be a list.')
        return cls(
            default_action=value['default_action'],
            channels=tuple(NetworkChannel.from_dict(item) for item in channels),
            rules=tuple(ReachabilityRule.from_dict(item) for item in rules),
        )

    def allows(
        self,
        source: str,
        destination: str,
        channel: str | None = None,
    ) -> bool:
        rule = next(
            (
                item
                for item in self.rules
                if item.source == source and item.destination == destination
            ),
            None,
        )
        if rule is None:
            return False
        return channel is None or channel in rule.channels

    def matrix(
        self,
        node_order: Sequence[str],
    ) -> dict[str, dict[str, tuple[str, ...]]]:
        """Return the complete matrix, including denied cells as empty tuples."""

        keys = tuple(node_order)
        by_pair = {
            (item.source, item.destination): item.channels for item in self.rules
        }
        return {
            source: {
                destination: by_pair.get((source, destination), ())
                for destination in keys
            }
            for source in keys
        }


_BENCHMARK_INGRESS = NetworkChannel(
    key=BENCHMARK_INGRESS_CHANNEL,
    protocol='tcp',
    ports=(5000, 8080),
)
_DISTRIBUTED_SOCIAL_NETWORK_INGRESS = NetworkChannel(
    key=BENCHMARK_INGRESS_CHANNEL,
    protocol='tcp',
    ports=(8080,),
)
_K3S_CONTROL_PLANE = NetworkChannel(
    key=K3S_CONTROL_PLANE_CHANNEL,
    protocol='tcp',
    ports=(6443,),
)
_K3S_OVERLAY = NetworkChannel(
    key=K3S_OVERLAY_CHANNEL,
    protocol='udp',
    ports=(8472,),
)
_CACHE_DATA = NetworkChannel(
    key=CACHE_DATA_CHANNEL,
    protocol='tcp',
    ports=(6379, 11211),
)
_DATABASE_DATA = NetworkChannel(
    key=DATABASE_DATA_CHANNEL,
    protocol='tcp',
    ports=(27017,),
)


def _single_host_policy() -> NetworkReachabilityPolicy:
    return NetworkReachabilityPolicy(
        default_action='deny',
        channels=(_BENCHMARK_INGRESS,),
        rules=(
            ReachabilityRule(
                source='load-generator',
                destination='runner',
                channels=(BENCHMARK_INGRESS_CHANNEL,),
            ),
        ),
    )


def _distributed_policy() -> NetworkReachabilityPolicy:
    channels = (
        _DISTRIBUTED_SOCIAL_NETWORK_INGRESS,
        _K3S_CONTROL_PLANE,
        _K3S_OVERLAY,
        _CACHE_DATA,
        _DATABASE_DATA,
    )
    cluster_nodes = ('control', 'application', 'cache', 'database')
    rules = [
        ReachabilityRule(
            source='load-generator',
            destination='application',
            channels=(BENCHMARK_INGRESS_CHANNEL,),
        )
    ]
    for source in cluster_nodes:
        for destination in cluster_nodes:
            if source == destination:
                continue
            # UDP/8472 carries the pod overlay and must never be exposed
            # publicly; workload-tier isolation is additionally enforced by
            # Kubernetes NetworkPolicy inside that overlay. Metrics-server is
            # disabled, so this profile does not open kubelet TCP/10250.
            allowed = [K3S_OVERLAY_CHANNEL]
            if destination == 'control' and source != 'control':
                allowed.insert(0, K3S_CONTROL_PLANE_CHANNEL)
            if source == 'application' and destination == 'cache':
                allowed.append(CACHE_DATA_CHANNEL)
            if source == 'application' and destination == 'database':
                allowed.append(DATABASE_DATA_CHANNEL)
            rules.append(
                ReachabilityRule(
                    source=source,
                    destination=destination,
                    channels=tuple(allowed),
                )
            )
    return NetworkReachabilityPolicy(
        default_action='deny',
        channels=channels,
        rules=tuple(rules),
    )


@dataclass(frozen=True, slots=True)
class DeathStarBenchTopologyManifest:
    """One exact, reproducible topology plan and its network contract."""

    schema_version: int
    topology_id: str
    topology_revision: str
    runtime_id: str
    runtime_revision: str
    placement_revision: str
    nodes: tuple[LogicalNode, ...]
    creation_order: tuple[str, ...]
    deletion_order: tuple[str, ...]
    network_policy: NetworkReachabilityPolicy

    def __post_init__(self):
        if (
            type(self.schema_version) is not int
            or self.schema_version != TOPOLOGY_MANIFEST_SCHEMA_VERSION
        ):
            raise TopologyManifestError(
                f'Unsupported topology-manifest schema version '
                f'{self.schema_version!r}.'
            )
        for field_name in ('topology_id', 'runtime_id'):
            _identifier(getattr(self, field_name), field_name)
        for field_name in (
            'topology_revision',
            'runtime_revision',
            'placement_revision',
        ):
            _identifier(getattr(self, field_name), field_name, _REVISION_RE)
        try:
            profile = runtime_profile(self.topology_id, self.runtime_id)
        except ValueError as exc:
            raise TopologyManifestError(str(exc)) from exc
        if self.topology_revision != profile.topology_revision:
            raise TopologyManifestError(
                'Topology revision does not match the versioned runtime profile.'
            )
        if self.runtime_revision != profile.runtime_revision:
            raise TopologyManifestError(
                'Runtime revision does not match the versioned runtime profile.'
            )
        if self.placement_revision != profile.placement_revision:
            raise TopologyManifestError(
                'Placement revision does not match the versioned runtime profile.'
            )
        if (
            isinstance(self.nodes, (str, bytes))
            or not isinstance(self.nodes, Sequence)
            or any(not isinstance(item, LogicalNode) for item in self.nodes)
        ):
            raise TopologyManifestError(
                'Manifest nodes must contain only LogicalNode values.'
            )
        object.__setattr__(self, 'nodes', tuple(self.nodes))
        for field_name in ('creation_order', 'deletion_order'):
            order = getattr(self, field_name)
            if (
                isinstance(order, (str, bytes))
                or not isinstance(order, Sequence)
            ):
                raise TopologyManifestError(
                    f'Manifest {field_name} must be a node-key sequence.'
                )
            normalized = tuple(
                _identifier(item, f'Manifest {field_name} node', _NODE_KEY_RE)
                for item in order
            )
            object.__setattr__(self, field_name, normalized)
        if not isinstance(self.network_policy, NetworkReachabilityPolicy):
            raise TopologyManifestError(
                'Manifest network_policy must be a NetworkReachabilityPolicy.'
            )
        if len(self.nodes) != profile.node_count:
            raise TopologyManifestError(
                'Manifest node count does not match the versioned profile.'
            )
        self._validate_exact_topology()

    def _validate_exact_topology(self):
        profile = runtime_profile(self.topology_id, self.runtime_id)
        keys = tuple(node.key for node in self.nodes)
        if self.topology_id == SINGLE_HOST_PROFILE.topology_id:
            expected_keys = SINGLE_HOST_NODE_KEYS
            expected_roles = ('runner', 'load_generator')
            expected_capacities = (
                SELECTED_SHAPE_CAPACITY,
                LOAD_GENERATOR_CAPACITY,
            )
            expected_policy = _single_host_policy()
        elif self.topology_id == DISTRIBUTED_TIERED_PROFILE.topology_id:
            expected_keys = DISTRIBUTED_TIERED_NODE_KEYS
            expected_roles = (
                'control',
                'database',
                'cache',
                'application',
                'load_generator',
            )
            expected_capacities = (
                CONTROL_CAPACITY,
                DATABASE_CAPACITY,
                CACHE_CAPACITY,
                SELECTED_SHAPE_CAPACITY,
                LOAD_GENERATOR_CAPACITY,
            )
            expected_policy = _distributed_policy()
        else:  # runtime_profile currently makes this unreachable.
            raise TopologyManifestError(
                f'Unsupported topology manifest {self.topology_id!r}.'
            )

        if keys != expected_keys:
            raise TopologyManifestError(
                'Manifest logical-node keys or order do not match the '
                f'{self.topology_id} contract.'
            )
        if tuple(node.role for node in self.nodes) != expected_roles:
            raise TopologyManifestError(
                'Manifest node roles do not match the versioned contract.'
            )
        if tuple(node.capacity for node in self.nodes) != expected_capacities:
            raise TopologyManifestError(
                'Manifest capacity contracts do not match the versioned contract.'
            )
        if self.creation_order != profile.creation_order:
            raise TopologyManifestError(
                'Manifest creation order does not match the versioned contract.'
            )
        if self.deletion_order != profile.deletion_order:
            raise TopologyManifestError(
                'Manifest deletion order does not match the versioned contract.'
            )
        if (
            len(self.creation_order) != len(keys)
            or len(self.deletion_order) != len(keys)
            or set(self.creation_order) != set(keys)
            or set(self.deletion_order) != set(keys)
        ):
            raise TopologyManifestError(
                'Manifest lifecycle orders must contain every logical node '
                'exactly once.'
            )
        selected_nodes = (
            {'runner'}
            if self.topology_id == SINGLE_HOST_PROFILE.topology_id
            else {'application'}
        )
        for node in self.nodes:
            if node.key not in selected_nodes and node.selected_shape is not None:
                raise TopologyManifestError(
                    f'Support node {node.key} must use an abstract capacity '
                    'class, not a provider shape.'
                )
            expected_storage = (DATABASE_STORAGE,) if node.key == 'database' else ()
            if node.storage != expected_storage:
                raise TopologyManifestError(
                    f'Node {node.key} storage does not match the versioned contract.'
                )
        if self.network_policy != expected_policy:
            raise TopologyManifestError(
                'Network policy does not match the versioned topology contract.'
            )
        known_keys = set(keys)
        for rule in self.network_policy.rules:
            if rule.source not in known_keys or rule.destination not in known_keys:
                raise TopologyManifestError(
                    'Network policy references a node outside the manifest.'
                )

    def _payload(self) -> dict[str, Any]:
        return {
            'schema_version': self.schema_version,
            'topology_id': self.topology_id,
            'topology_revision': self.topology_revision,
            'runtime_id': self.runtime_id,
            'runtime_revision': self.runtime_revision,
            'placement_revision': self.placement_revision,
            'nodes': [item.as_dict() for item in self.nodes],
            'creation_order': list(self.creation_order),
            'deletion_order': list(self.deletion_order),
            'network_policy': self.network_policy.as_dict(),
        }

    @property
    def fingerprint(self) -> str:
        digest = hashlib.sha256(
            json.dumps(
                self._payload(),
                sort_keys=True,
                separators=(',', ':'),
                ensure_ascii=True,
                allow_nan=False,
            ).encode('ascii')
        ).hexdigest()
        return f'sha256:{digest}'

    @property
    def reachability_matrix(self) -> dict[str, dict[str, tuple[str, ...]]]:
        return self.network_policy.matrix(tuple(node.key for node in self.nodes))

    def as_dict(self) -> dict[str, Any]:
        document = self._payload()
        document['fingerprint'] = self.fingerprint
        return document

    def canonical_json(self) -> str:
        return json.dumps(
            self.as_dict(),
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True,
            allow_nan=False,
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> DeathStarBenchTopologyManifest:
        if not isinstance(value, Mapping):
            raise TopologyManifestError('Topology manifest must be an object.')
        expected = {
            'schema_version',
            'topology_id',
            'topology_revision',
            'runtime_id',
            'runtime_revision',
            'placement_revision',
            'nodes',
            'creation_order',
            'deletion_order',
            'network_policy',
            'fingerprint',
        }
        _exact_fields(value, expected, 'Topology manifest')
        nodes = value['nodes']
        if isinstance(nodes, (str, bytes)) or not isinstance(nodes, Sequence):
            raise TopologyManifestError('Topology-manifest nodes must be a list.')
        for order_key in ('creation_order', 'deletion_order'):
            order = value[order_key]
            if isinstance(order, (str, bytes)) or not isinstance(order, Sequence):
                raise TopologyManifestError(
                    f'Topology-manifest {order_key} must be a list.'
                )
        supplied_fingerprint = value['fingerprint']
        if (
            not isinstance(supplied_fingerprint, str)
            or not _FINGERPRINT_RE.fullmatch(supplied_fingerprint)
        ):
            raise TopologyManifestError(
                'Topology-manifest fingerprint must be a SHA-256 identifier.'
            )
        manifest = cls(
            schema_version=value['schema_version'],
            topology_id=value['topology_id'],
            topology_revision=value['topology_revision'],
            runtime_id=value['runtime_id'],
            runtime_revision=value['runtime_revision'],
            placement_revision=value['placement_revision'],
            nodes=tuple(LogicalNode.from_dict(item) for item in nodes),
            creation_order=tuple(value['creation_order']),
            deletion_order=tuple(value['deletion_order']),
            network_policy=NetworkReachabilityPolicy.from_dict(
                value['network_policy']
            ),
        )
        if not hmac.compare_digest(supplied_fingerprint, manifest.fingerprint):
            raise TopologyManifestError(
                'Topology-manifest fingerprint does not match its contents.'
            )
        return manifest

    def planned_inventory(self, provider: str | None = None) -> RoleNodeInventory:
        """Return the provider-neutral logical plan in persisted inventory form."""

        return RoleNodeInventory(
            provider=provider,
            topology_fingerprint=self.fingerprint,
            nodes=tuple(node.as_role_node() for node in self.nodes),
        )


def _build_manifest(
    profile,
    *,
    selected_shape: str | None,
    selected_architecture: str | None,
) -> DeathStarBenchTopologyManifest:
    selected_shape = _optional_text(selected_shape, 'Selected shape')
    selected_architecture = _normalize_architecture(selected_architecture)

    if profile is SINGLE_HOST_PROFILE:
        nodes = (
            LogicalNode(
                key='runner',
                role='runner',
                capacity=SELECTED_SHAPE_CAPACITY,
                selected_shape=selected_shape,
                architecture=selected_architecture,
            ),
            LogicalNode(
                key='load-generator',
                role='load_generator',
                capacity=LOAD_GENERATOR_CAPACITY,
                architecture='x86_64',
            ),
        )
        policy = _single_host_policy()
    elif profile is DISTRIBUTED_TIERED_PROFILE:
        nodes = (
            LogicalNode(
                key='control',
                role='control',
                capacity=CONTROL_CAPACITY,
                architecture='x86_64',
            ),
            LogicalNode(
                key='database',
                role='database',
                capacity=DATABASE_CAPACITY,
                architecture='x86_64',
                storage=(DATABASE_STORAGE,),
            ),
            LogicalNode(
                key='cache',
                role='cache',
                capacity=CACHE_CAPACITY,
                architecture='x86_64',
            ),
            LogicalNode(
                key='application',
                role='application',
                capacity=SELECTED_SHAPE_CAPACITY,
                selected_shape=selected_shape,
                architecture=selected_architecture,
            ),
            LogicalNode(
                key='load-generator',
                role='load_generator',
                capacity=LOAD_GENERATOR_CAPACITY,
                architecture='x86_64',
            ),
        )
        policy = _distributed_policy()
    else:
        raise TopologyManifestError(
            f'Unsupported topology profile {profile.topology_id!r}.'
        )

    return DeathStarBenchTopologyManifest(
        schema_version=TOPOLOGY_MANIFEST_SCHEMA_VERSION,
        topology_id=profile.topology_id,
        topology_revision=profile.topology_revision,
        runtime_id=profile.runtime_id,
        runtime_revision=profile.runtime_revision,
        placement_revision=profile.placement_revision,
        nodes=nodes,
        creation_order=profile.creation_order,
        deletion_order=profile.deletion_order,
        network_policy=policy,
    )


def build_topology_manifest(
    topology_id: str,
    runtime_id: str,
    *,
    selected_shape: str | None = None,
    selected_architecture: str | None = None,
) -> DeathStarBenchTopologyManifest:
    """Build an exact topology without consulting or writing any cloud API."""

    try:
        profile = runtime_profile(topology_id, runtime_id)
    except ValueError as exc:
        raise TopologyManifestError(str(exc)) from exc
    return _build_manifest(
        profile,
        selected_shape=selected_shape,
        selected_architecture=selected_architecture,
    )


def _value(source: Any, key: str) -> Any:
    if isinstance(source, Mapping):
        return source.get(key)
    return getattr(source, key, None)


def build_deathstarbench_topology(
    plan: Any,
    *,
    selected_architecture: str | None = None,
) -> DeathStarBenchTopologyManifest:
    """Build a manifest from a validated plan or its persisted dictionary."""

    if plan is None:
        raise TopologyManifestError('A benchmark plan is required.')
    options = _value(plan, 'deathstarbench')
    if options is None:
        options = plan
    topology_id = _value(options, 'topology_id')
    runtime_id = _value(options, 'runtime_id')
    if topology_id is None:
        topology_id = SINGLE_HOST_PROFILE.topology_id
    if runtime_id is None:
        runtime_id = SINGLE_HOST_PROFILE.runtime_id
    architecture = selected_architecture
    if architecture is None:
        architecture = _value(plan, 'architecture')
    return build_topology_manifest(
        topology_id,
        runtime_id,
        selected_shape=_value(plan, 'shape'),
        selected_architecture=architecture,
    )


def manifest_fingerprint(manifest: DeathStarBenchTopologyManifest) -> str:
    if not isinstance(manifest, DeathStarBenchTopologyManifest):
        raise TopologyManifestError(
            'A DeathStarBenchTopologyManifest is required for fingerprinting.'
        )
    return manifest.fingerprint


# Descriptive aliases for callers that prefer explicit plan terminology.
topology_manifest_for_plan = build_deathstarbench_topology
plan_deathstarbench_topology = build_deathstarbench_topology


__all__ = [
    'BENCHMARK_INGRESS_CHANNEL',
    'CACHE_CAPACITY_CLASS',
    'CACHE_DATA_CHANNEL',
    'CAPACITY_CONTRACTS',
    'CONTROL_CAPACITY_CLASS',
    'DATABASE_CAPACITY_CLASS',
    'DATABASE_DATA_CHANNEL',
    'DATABASE_STORAGE',
    'DATABASE_STORAGE_CLASS',
    'DISTRIBUTED_TIERED_NODE_KEYS',
    'DeathStarBenchTopologyManifest',
    'K3S_CONTROL_PLANE_CHANNEL',
    'K3S_OVERLAY_CHANNEL',
    'LOAD_GENERATOR_CAPACITY_CLASS',
    'LogicalNode',
    'NetworkChannel',
    'NetworkReachabilityPolicy',
    'PersistentStorageContract',
    'ReachabilityRule',
    'SELECTED_SHAPE_CAPACITY_CLASS',
    'SINGLE_HOST_NODE_KEYS',
    'TOPOLOGY_MANIFEST_SCHEMA_VERSION',
    'TopologyManifestError',
    'build_deathstarbench_topology',
    'build_topology_manifest',
    'manifest_fingerprint',
    'plan_deathstarbench_topology',
    'topology_manifest_for_plan',
]
