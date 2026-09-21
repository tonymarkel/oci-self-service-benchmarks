"""Google Compute Engine discovery, provisioning, and cleanup.

The adapter uses Application Default Credentials (ADC) on the local machine.
Credential material is never accepted as a plan value and is never written to
run state.  Every managed resource has a deterministic per-job name, and every
insert request uses a persisted UUID request ID so a lost API response can be
reconciled without creating a duplicate resource.

Compute Engine labels are available on instances and disks, but not on VPC
networks, subnetworks, or classic VPC firewall rules.  Those resources are
therefore protected by an ownership description, their exact deterministic
name, their persisted numeric ID, and their parent relationships.
"""

from __future__ import annotations

import re
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import replace
from types import MappingProxyType
from typing import Any

from ..deathstarbench_contract import (
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
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


DEFAULT_REGION = 'us-east1'
DEFAULT_DISK_TYPE = 'pd-balanced'
HYPERDISK_BALANCED_TYPE = 'hyperdisk-balanced'
HYPERDISK_BALANCED_IOPS = 3000
HYPERDISK_BALANCED_THROUGHPUT_MIBPS = 140
IMAGE_PROJECT = 'rocky-linux-cloud'
IMAGE_FAMILIES = {
    'x86_64': 'rocky-linux-9',
    'arm64': 'rocky-linux-9-arm64',
}
MANAGED_BY = 'oci-self-service-benchmarks'
NETWORK_CIDR = '10.42.0.0/16'
SUBNET_CIDR = '10.42.1.0/24'
SSH_SOURCE_CIDR = '0.0.0.0/0'
SSH_USER = 'benchmark'
PEER_BOOT_SIZE_GB = 20
LOADGEN_MACHINE_TYPE = 'n2-standard-2'
LOADGEN_ARCHITECTURE = 'x86_64'
LOADGEN_VCPUS = 2
LOADGEN_MEMORY_GB = 8
LOADGEN_BOOT_SIZE_GB = 20
LOADGEN_DISK_TYPE = 'pd-balanced'
# Google documents n2-standard-2 default egress as "up to 10 Gbps". Keep the
# capacity kind alongside the value so reports do not imply a guaranteed rate.
LOADGEN_NETWORK_BANDWIDTH_GBPS = 10
OPERATION_TIMEOUT_SECONDS = 1800
RECONCILIATION_ATTEMPTS = 12
RECONCILIATION_DELAY_SECONDS = 2

DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY = 'deathstarbench_topology_manifest'
DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY = (
    'deathstarbench_topology_fingerprint'
)
DISTRIBUTED_NETWORK_CIDR = '10.240.0.0/16'
DISTRIBUTED_CLUSTER_SUBNET_CIDR = '10.240.1.0/24'
DISTRIBUTED_LOADGEN_SUBNET_CIDR = '10.240.2.0/24'
DISTRIBUTED_PRIVATE_ADDRESSES = MappingProxyType({
    'control': '10.240.1.10',
    'database': '10.240.1.11',
    'cache': '10.240.1.12',
    'application': '10.240.1.13',
    'load-generator': '10.240.2.10',
})
DISTRIBUTED_ROLE_MACHINE_TYPES = MappingProxyType({
    'control': 'n2-standard-2',
    'database': 'n2-standard-4',
    'cache': 'n2-standard-2',
    'load-generator': 'n2-standard-2',
})
DISTRIBUTED_PUBLIC_IP_NODES = frozenset({
    'control',
    'application',
    'load-generator',
})
DISTRIBUTED_OS_DISK_SIZE_GB = 50
DISTRIBUTED_DATABASE_DISK_TYPE = 'pd-ssd'
DISTRIBUTED_DATABASE_DISK_INTERFACE = 'SCSI'
DISTRIBUTED_ROUTER_ASN = 64514
DISTRIBUTED_NODE_PREFIXES = MappingProxyType({
    role: f'gcp_dsb_{role.replace("-", "_")}_instance'
    for role in DISTRIBUTED_PRIVATE_ADDRESSES
})
DISTRIBUTED_BOOT_PREFIXES = MappingProxyType({
    role: f'gcp_dsb_{role.replace("-", "_")}_boot_disk'
    for role in DISTRIBUTED_PRIVATE_ADDRESSES
})
DISTRIBUTED_FIREWALL_PREFIXES = (
    'gcp_dsb_public_ssh_firewall',
    'gcp_dsb_private_ssh_firewall',
    'gcp_dsb_k3s_api_firewall',
    'gcp_dsb_k3s_overlay_firewall',
    'gcp_dsb_benchmark_firewall',
)

# Fourth-generation Compute Engine series use Hyperdisk rather than
# Persistent Disk. C4A has a deliberately supported Hyperdisk Balanced
# profile below; the other Hyperdisk-only families remain excluded until each
# architecture/guest/storage contract is implemented and tested explicitly.
HYPERDISK_ONLY_FAMILIES = frozenset({
    'a4',
    'a4x',
    'a4x-max',
    'c4',
    'c4d',
    'c4n',
    'g4',
    'h4d',
    'm4',
    'm4n',
    'n4',
    'n4a',
    'n4d',
    'x4',
})
SUPPORTED_ARCHITECTURES = frozenset({'x86_64', 'arm64'})
EXPLICIT_GVNIC_FAMILIES = frozenset({'c4a', 'h3'})

EventCallback = Callable[[dict[str, Any], str, str], None]
PersistCallback = Callable[[dict[str, Any]], None]


def _value(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _basename(value: Any) -> str:
    return str(value or '').rstrip('/').rsplit('/', 1)[-1]


def _list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        return list(value.get('items') or [])
    items = getattr(value, 'items', None)
    if items is not None and not callable(items):
        return list(items or [])
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


def _validate_expected_role_inventory(
    current: RoleNodeInventory,
    expected: RoleNodeInventory,
):
    """Reject topology drift without discarding accepted GCE identities."""

    if current.provider != 'gcp' or expected.provider != 'gcp':
        raise ResourceInventoryError(
            'GCP role-node inventory has a conflicting provider.'
        )
    if current.topology_fingerprint != expected.topology_fingerprint:
        raise ResourceInventoryError(
            'GCP role-node inventory has a conflicting topology fingerprint.'
        )
    if tuple(node.key for node in current.nodes) != tuple(
        node.key for node in expected.nodes
    ):
        raise ResourceInventoryError(
            'GCP role-node inventory has conflicting logical nodes.'
        )
    stable_node_fields = (
        'role',
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
        'provider_resource_name',
        'device',
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
                f'GCP role-node inventory is missing {expected_node.key}.'
            )
        for field_name in stable_node_fields:
            if getattr(current_node, field_name) != getattr(
                expected_node, field_name
            ):
                raise ResourceInventoryError(
                    f'GCP role node {expected_node.key} has conflicting '
                    f'{field_name}.'
                )
        current_storage = {item.key: item for item in current_node.storage}
        expected_storage = {item.key: item for item in expected_node.storage}
        if set(current_storage) != set(expected_storage):
            raise ResourceInventoryError(
                f'GCP role node {expected_node.key} has conflicting storage.'
            )
        for storage_key, expected_item in expected_storage.items():
            current_item = current_storage[storage_key]
            for field_name in stable_storage_fields:
                if getattr(current_item, field_name) != getattr(
                    expected_item, field_name
                ):
                    raise ResourceInventoryError(
                        f'GCP storage {storage_key} has conflicting '
                        f'{field_name}.'
                    )


def _initialize_distributed_candidate_contract(
    job: dict[str, Any],
    persist: PersistCallback | None,
    manifest: DeathStarBenchTopologyManifest,
    expected_inventory: RoleNodeInventory,
    values: Mapping[str, Any],
) -> RoleNodeInventory:
    """Persist every deterministic role identity before the first GCE write."""

    resources = job.setdefault('resources', {})
    topology_keys = (
        DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
        DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY,
        ROLE_NODE_INVENTORY_KEY,
    )
    has_contract = any(
        key in resources
        for key in (*topology_keys, 'gcp_distributed_candidate')
    )
    if has_contract:
        if not all(key in resources for key in topology_keys):
            raise ResourceInventoryError(
                'GCP distributed candidate contract is incomplete.'
            )
        if resources.get('gcp_distributed_candidate') is not True:
            raise ResourceInventoryError(
                'GCP distributed candidate marker is invalid.'
            )
        saved_manifest = DeathStarBenchTopologyManifest.from_dict(
            resources[DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY]
        )
        if saved_manifest != manifest or resources.get(
            DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY
        ) != manifest.fingerprint:
            raise ResourceInventoryError(
                'GCP distributed candidate topology conflicts with the saved run.'
            )
        for key, expected in values.items():
            if key not in resources:
                raise ResourceInventoryError(
                    f'GCP distributed candidate contract is missing {key}.'
                )
            current = resources[key]
            if isinstance(expected, str):
                matches = isinstance(current, str) and current == expected
            else:
                matches = current == expected
            if not matches:
                raise ResourceInventoryError(
                    f'GCP distributed candidate contract conflicts at {key}.'
                )
        current = load_role_node_inventory(resources)
        _validate_expected_role_inventory(current, expected_inventory)
        return current

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
    inventory = load_role_node_inventory(job.setdefault('resources', {}))
    node = inventory.node(node_key)
    if node is None:
        raise ResourceInventoryError(
            f'GCP role-node inventory is missing {node_key}.'
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
    inventory = load_role_node_inventory(job.setdefault('resources', {}))
    node = inventory.node(node_key)
    if node is None:
        raise ResourceInventoryError(
            f'GCP role-node inventory is missing {node_key}.'
        )
    storage = node.storage_resource(storage_key)
    if storage is None:
        raise ResourceInventoryError(
            f'GCP role node {node_key} is missing storage {storage_key}.'
        )
    updated = node.upsert_storage(replace(
        storage,
        provider_resource_id=(
            provider_resource_id or storage.provider_resource_id
        ),
        lifecycle_status=status,
    ))
    return _persist_inventory(job, persist, inventory.upsert(updated))


def _load_adc(project_id: str | None = None, credentials=None):
    if credentials is None:
        try:
            import google.auth
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise RuntimeError(
                'Google Cloud support requires google-cloud-compute and '
                'google-auth. Install the project requirements first.'
            ) from exc
        credentials, adc_project = google.auth.default(
            scopes=('https://www.googleapis.com/auth/cloud-platform',)
        )
    else:
        adc_project = getattr(credentials, 'project_id', None)
    resolved = str(project_id or adc_project or '').strip()
    if not resolved:
        raise ValueError(
            'No Google Cloud project was selected and ADC did not provide a '
            'default project. Select a project or set GOOGLE_CLOUD_PROJECT.'
        )
    return credentials, resolved


def create_clients(credentials=None) -> dict[str, Any]:
    """Create fresh Compute Engine clients using ADC-compatible credentials."""
    try:
        from google.cloud import compute_v1
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            'Google Cloud support requires the google-cloud-compute package.'
        ) from exc
    kwargs = {'credentials': credentials} if credentials is not None else {}
    return {
        'types': compute_v1,
        'projects': compute_v1.ProjectsClient(**kwargs),
        'regions': compute_v1.RegionsClient(**kwargs),
        'zones': compute_v1.ZonesClient(**kwargs),
        'machine_types': compute_v1.MachineTypesClient(**kwargs),
        'images': compute_v1.ImagesClient(**kwargs),
        'networks': compute_v1.NetworksClient(**kwargs),
        'subnetworks': compute_v1.SubnetworksClient(**kwargs),
        'routers': compute_v1.RoutersClient(**kwargs),
        'firewalls': compute_v1.FirewallsClient(**kwargs),
        'instances': compute_v1.InstancesClient(**kwargs),
        'disks': compute_v1.DisksClient(**kwargs),
    }


def _runtime(project_id: str | None, credentials=None, clients=None):
    if clients is not None:
        resolved = str(project_id or '').strip()
        if not resolved:
            raise ValueError('A Google Cloud project is required.')
        return resolved, credentials, clients
    credentials, resolved = _load_adc(project_id, credentials)
    return resolved, credentials, create_clients(credentials)


def _message(clients: Mapping[str, Any], name: str, values: Mapping[str, Any]):
    types = clients.get('types')
    message_type = getattr(types, name, None) if types else None
    return message_type(values) if message_type else dict(values)


def _call(client: Any, method: str, clients: Mapping[str, Any], request_type: str, **values):
    request = _message(clients, request_type, values)
    return getattr(client, method)(request=request)


def _wait(operation: Any, timeout: int = OPERATION_TIMEOUT_SECONDS):
    if operation is None:
        return None
    result = operation.result(timeout=timeout) if hasattr(operation, 'result') else operation
    error_code = getattr(operation, 'error_code', None)
    if error_code:
        raise RuntimeError(
            getattr(operation, 'error_message', None)
            or f'Google Compute Engine operation failed with code {error_code}.'
        )
    return result


def _http_status(exc: BaseException) -> int | None:
    code = getattr(exc, 'code', None)
    if callable(code):
        try:
            code = code()
        except Exception:
            code = None
    if hasattr(code, 'value'):
        value = code.value
        code = value[0] if isinstance(value, tuple) else value
    if isinstance(code, int):
        return code
    for candidate in (
        getattr(exc, 'status_code', None),
        getattr(getattr(exc, 'response', None), 'status_code', None),
    ):
        if isinstance(candidate, int):
            return candidate
    return None


def _not_found(exc: BaseException) -> bool:
    return _http_status(exc) == 404 or exc.__class__.__name__ == 'NotFound'


def _confirmed_rejection(exc: BaseException) -> bool:
    status = _http_status(exc)
    # Timeouts, cancellation-like 499s, throttling, and conflicts do not prove
    # that the server rejected an insert before accepting its requestId.
    return status in {400, 401, 403, 404, 405, 410, 411, 413, 414, 415, 422}


def normalize_architecture(value: Any) -> str:
    aliases = {
        'amd64': 'x86_64',
        'x86-64': 'x86_64',
        'x86_64': 'x86_64',
        'arm64': 'arm64',
        'aarch64': 'arm64',
    }
    normalized = aliases.get(str(value or '').strip().lower())
    if not normalized:
        raise ValueError(f'Unsupported Google Compute Engine architecture {value!r}.')
    return normalized


def _deprecated_state(resource: Any) -> str:
    deprecated = _value(resource, 'deprecated')
    return str(_value(deprecated, 'state', '') or '').upper()


def _machine_family(name: str) -> str:
    lowered = str(name or '').lower()
    for compound in ('a4x-max',):
        if lowered.startswith(compound + '-'):
            return compound
    return lowered.partition('-')[0]


def _bundled_local_ssd(resource: Any) -> bool:
    bundled = _value(resource, 'bundled_local_ssds')
    return bool(int(_value(bundled, 'partition_count', 0) or 0))


def _machine_architecture(
    resource: Any,
    *,
    missing_fallback: str | None = None,
) -> str:
    """Resolve architecture without masking contradictory API metadata."""

    raw_architecture = _value(resource, 'architecture')
    if str(raw_architecture or '').strip():
        return normalize_architecture(raw_architecture)
    if missing_fallback is not None:
        return normalize_architecture(missing_fallback)
    return normalize_architecture(raw_architecture)


def _machine_storage_profile(
    resource: Any,
    *,
    missing_architecture_fallback: str | None = None,
) -> dict[str, Any] | None:
    """Return the exact supported disk/NIC profile for one machine type."""
    name = str(_value(resource, 'name', '') or '')
    lowered = name.lower()
    family = _machine_family(lowered)
    local_suffix = bool(
        re.search(r'(?:^|-)[a-z0-9]*lssd(?:-|$)', lowered)
        or lowered.endswith('-localssd')
    )
    if (
        not name
        or family in HYPERDISK_ONLY_FAMILIES
        or local_suffix
        or 'metal' in lowered
        or _bundled_local_ssd(resource)
        or (_value(resource, 'accelerators', ()) or ())
    ):
        return None
    try:
        architecture = _machine_architecture(
            resource,
            missing_fallback=missing_architecture_fallback,
        )
    except ValueError:
        return None
    if family == 'c4a':
        # C4A is Google Axion (Arm64), supports only NVMe disks, requires
        # Hyperdisk for boot/storage, and requires gVNIC. Reject inconsistent
        # API metadata instead of silently launching another architecture.
        if architecture != 'arm64':
            return None
        return {
            'disk_type': HYPERDISK_BALANCED_TYPE,
            'disk_interface': 'NVME',
            'network_interface_type': 'GVNIC',
            'disk_provisioned_iops': HYPERDISK_BALANCED_IOPS,
            'disk_provisioned_throughput_mibps': (
                HYPERDISK_BALANCED_THROUGHPUT_MIBPS
            ),
        }
    return {
        'disk_type': DEFAULT_DISK_TYPE,
        'disk_interface': None,
        'network_interface_type': (
            'GVNIC' if family in EXPLICIT_GVNIC_FAMILIES else None
        ),
        'disk_provisioned_iops': None,
        'disk_provisioned_throughput_mibps': None,
    }


def _machine_summary(
    item: Any,
    *,
    missing_architecture_fallback: str | None = None,
) -> dict[str, Any]:
    architecture = _machine_architecture(
        item,
        missing_fallback=missing_architecture_fallback,
    )
    cpus = int(_value(item, 'guest_cpus', 0) or 0)
    memory_gb = round(float(_value(item, 'memory_mb', 0) or 0) / 1024, 3)
    name = str(_value(item, 'name'))
    summary = {
        'machine_type': name,
        'instance_type': name,
        'shape': name,
        'vcpu': cpus,
        'ocpus': cpus,
        'memory_gb': memory_gb,
        'architecture': architecture,
        'shared_cpu': bool(_value(item, 'is_shared_cpu', False)),
        'maximum_persistent_disks': _value(item, 'maximum_persistent_disks'),
    }
    profile = _machine_storage_profile(
        item,
        missing_architecture_fallback=missing_architecture_fallback,
    )
    if profile:
        summary.update(profile)
    return summary


def bootstrap(
    project_id: str | None = None,
    *,
    default_region: str = DEFAULT_REGION,
    credentials=None,
    clients=None,
):
    """Validate ADC/project access and list usable Compute Engine regions."""
    project_id, _, clients = _runtime(project_id, credentials, clients)
    project = _call(
        clients['projects'], 'get', clients, 'GetProjectRequest', project=project_id
    )
    regions = []
    for region in _list(_call(
        clients['regions'], 'list', clients, 'ListRegionsRequest', project=project_id
    )):
        if str(_value(region, 'status', 'UP') or 'UP').upper() != 'UP':
            continue
        if _deprecated_state(region) in {'OBSOLETE', 'DELETED'}:
            continue
        name = str(_value(region, 'name', '') or '')
        if name:
            regions.append(name)
    regions = sorted(set(regions))
    chosen_default = default_region if default_region in regions else (
        regions[0] if regions else default_region
    )
    return {
        'provider': 'gcp',
        'project_id': project_id,
        # Compute's Project.id is not the Cloud Resource Manager project
        # number.  Name it precisely so it is not confused with that value.
        'compute_project_id': str(_value(project, 'id', '') or ''),
        'default_region': chosen_default,
        'regions': regions,
    }


def placement(
    project_id: str,
    region: str,
    *,
    credentials=None,
    clients=None,
):
    """List UP, non-obsolete zones in one selected region."""
    project_id, _, clients = _runtime(project_id, credentials, clients)
    zones = []
    response = _call(
        clients['zones'], 'list', clients, 'ListZonesRequest', project=project_id
    )
    for zone in _list(response):
        if _basename(_value(zone, 'region')) != region:
            continue
        if str(_value(zone, 'status', 'UP') or 'UP').upper() != 'UP':
            continue
        if _deprecated_state(zone) in {'OBSOLETE', 'DELETED'}:
            continue
        name = str(_value(zone, 'name', '') or '')
        if name:
            zones.append({
                'name': name,
                'zone_id': str(_value(zone, 'id', '') or ''),
                'state': 'available',
            })
    zones.sort(key=lambda item: item['name'])
    return {'availability_zones': zones}


def machine_types(
    project_id: str,
    zone: str,
    *,
    credentials=None,
    clients=None,
):
    """List fixed-capacity machine types with an implemented storage profile."""
    project_id, _, clients = _runtime(project_id, credentials, clients)
    response = _call(
        clients['machine_types'],
        'list',
        clients,
        'ListMachineTypesRequest',
        project=project_id,
        zone=zone,
    )
    items = []
    for item in _list(response):
        if _deprecated_state(item) in {'OBSOLETE', 'DELETED'}:
            continue
        try:
            architecture = normalize_architecture(_value(item, 'architecture'))
        except ValueError:
            continue
        if architecture not in SUPPORTED_ARCHITECTURES:
            continue
        profile = _machine_storage_profile(item)
        if not profile:
            continue
        if int(_value(item, 'guest_cpus', 0) or 0) < 1:
            continue
        if int(_value(item, 'memory_mb', 0) or 0) < 1024:
            continue
        items.append(_machine_summary(item))
    items.sort(key=lambda item: item['machine_type'].casefold())
    return {'items': items}


def latest_rocky_linux_9_image(
    architecture: str,
    *,
    credentials=None,
    clients=None,
):
    """Resolve and pin Google's latest non-deprecated Rocky Linux 9 image."""
    architecture = normalize_architecture(architecture)
    if clients is None:
        credentials, _ = _load_adc('image-discovery', credentials)
        clients = create_clients(credentials)
    family = IMAGE_FAMILIES[architecture]
    image = _call(
        clients['images'],
        'get_from_family',
        clients,
        'GetFromFamilyImageRequest',
        project=IMAGE_PROJECT,
        family=family,
    )
    if str(_value(image, 'status', '') or '').upper() != 'READY':
        raise RuntimeError(
            f'Google Rocky Linux image family {family} did not resolve to a READY image.'
        )
    image_arch = _value(image, 'architecture')
    if image_arch and normalize_architecture(image_arch) != architecture:
        raise RuntimeError(
            f'Rocky Linux image {_value(image, "name")} has architecture '
            f'{image_arch!r}, expected {architecture!r}.'
        )
    resolved = {
        'image_id': str(_value(image, 'id', '') or ''),
        'name': str(_value(image, 'name', '') or ''),
        'self_link': str(_value(image, 'self_link', '') or ''),
        'project': IMAGE_PROJECT,
        'family': family,
        'architecture': architecture,
    }
    missing = [
        field for field in ('image_id', 'name', 'self_link')
        if not resolved[field]
    ]
    if missing:
        raise RuntimeError(
            f'Google Rocky Linux image family {family} returned incomplete '
            f'immutable identity metadata ({", ".join(missing)} missing).'
        )
    expected_link = (
        f'projects/{IMAGE_PROJECT}/global/images/{resolved["name"]}'
    )
    if not _scoped_link_equal(resolved['self_link'], expected_link):
        raise RuntimeError(
            f'Google Rocky Linux image family {family} returned a selfLink '
            'outside the expected image project or for a different image.'
        )
    return resolved


def _persisted_image_contract(
    resources: Mapping[str, Any],
    architecture: str,
    *,
    load_generator: bool = False,
) -> dict[str, str] | None:
    """Return a complete immutable image contract without re-resolving a family."""
    architecture = normalize_architecture(architecture)
    if load_generator:
        id_key = 'gcp_loadgen_image_id'
        name_key = 'gcp_loadgen_image_name'
        link_key = 'gcp_loadgen_image_self_link'
        family_key = 'gcp_loadgen_image_family'
        architecture_key = 'loadgen_architecture'
        label = 'load-generator'
    else:
        id_key = 'image_id'
        name_key = 'image_name'
        link_key = 'gcp_image_self_link'
        family_key = 'gcp_image_family'
        architecture_key = 'architecture'
        label = 'runner'
    values = {
        'image_id': str(resources.get(id_key) or ''),
        'name': str(resources.get(name_key) or ''),
        'self_link': str(resources.get(link_key) or ''),
    }
    family = str(resources.get(family_key) or '')
    present = any(values.values()) or bool(family)
    if not present:
        return None
    missing = [key for key, value in values.items() if not value]
    if missing:
        raise RuntimeError(
            f'Refusing GCP provisioning because the persisted {label} image '
            'identity is incomplete. Cleanup or repair the saved job before '
            'retrying.'
        )
    expected_family = IMAGE_FAMILIES[architecture]
    if family and family != expected_family:
        raise RuntimeError(
            f'Refusing GCP provisioning because the persisted {label} image '
            f'family {family!r} conflicts with architecture {architecture!r}.'
        )
    saved_architecture = resources.get(architecture_key)
    if saved_architecture and normalize_architecture(saved_architecture) != architecture:
        raise RuntimeError(
            f'Refusing GCP provisioning because the persisted {label} image '
            'architecture conflicts with the selected machine type.'
        )
    expected_link = (
        f'projects/{IMAGE_PROJECT}/global/images/{values["name"]}'
    )
    if not _scoped_link_equal(values['self_link'], expected_link):
        raise RuntimeError(
            f'Refusing GCP provisioning because the persisted {label} image '
            'selfLink is outside the Rocky Linux image project or names a '
            'different image.'
        )
    if load_generator:
        canonical_id = str(resources.get('loadgen_image_id') or '')
        canonical_name = str(resources.get('loadgen_image_name') or '')
        if canonical_id and canonical_id != values['image_id']:
            raise RuntimeError(
                'Refusing GCP provisioning because the canonical and '
                'provider-specific load-generator image IDs conflict.'
            )
        if canonical_name and canonical_name != values['name']:
            raise RuntimeError(
                'Refusing GCP provisioning because the canonical and '
                'provider-specific load-generator image names conflict.'
            )
    return {
        **values,
        'project': IMAGE_PROJECT,
        'family': family or expected_family,
        'architecture': architecture,
    }


def _project_id(plan: Any) -> str:
    return str(
        _value(plan, 'gcp_project_id')
        or _value(plan, 'project_id')
        or ''
    ).strip()


def _region(plan: Any) -> str:
    return str(_value(plan, 'region', DEFAULT_REGION) or DEFAULT_REGION).strip()


def _machine_type_name(plan: Any) -> str:
    return str(
        _value(plan, 'gcp_machine_type')
        or _value(plan, 'machine_type')
        or _value(plan, 'shape')
        or ''
    ).strip()


def _storage_value(plan: Any, name: str, default: Any = None) -> Any:
    return _value(_value(plan, 'storage', {}) or {}, name, default)


def _selected_iperf3_protocols(plan: Any) -> tuple[str, ...]:
    benchmarks = set(_value(plan, 'benchmarks', ()) or ())
    if 'iperf3' not in benchmarks:
        return ()
    options = _value(plan, 'iperf3', {}) or {}
    protocols = tuple(dict.fromkeys(
        str(item).strip().lower()
        for item in (_value(options, 'protocols', ()) or ())
        if str(item).strip()
    ))
    if not protocols:
        raise ValueError('Select at least one iperf3 protocol for GCP.')
    unsupported = sorted(set(protocols) - {'tcp', 'udp', 'sctp'})
    if unsupported:
        raise ValueError(
            'The selected iperf3 protocol is not supported on GCP; '
            f'unsupported selection: {", ".join(unsupported)}.'
        )
    return protocols


def _selected_web_benchmarks(plan: Any) -> tuple[str, ...]:
    selected = set(_value(plan, 'benchmarks', ()) or ())
    return tuple(
        benchmark
        for benchmark in ('apachebench', 'deathstarbench')
        if benchmark in selected
    )


def _labels(job_id: str, role: str) -> dict[str, str]:
    return {
        'managed-by': MANAGED_BY,
        'benchmark-job': job_id,
        'benchmark-role': role,
    }


def _description(job_id: str, role: str) -> str:
    return (
        f'Managed by {MANAGED_BY}; benchmark-job={job_id}; '
        f'benchmark-role={role}.'
    )


def _resource_name(job_id: str, role: str = '') -> str:
    suffix = f'-{role}' if role else ''
    name = f'benchmark-{job_id}{suffix}'
    if not re.fullmatch(r'[a-z]([-a-z0-9]{0,61}[a-z0-9])?', name):
        raise ValueError(f'Job identifier {job_id!r} cannot form a GCP resource name.')
    return name


def _get_or_none(
    client: Any,
    clients: Mapping[str, Any],
    request_type: str,
    **values,
):
    try:
        return _call(client, 'get', clients, request_type, **values)
    except Exception as exc:
        if _not_found(exc):
            return None
        raise


def _resource_identity(resource: Any) -> tuple[str, str]:
    return (
        str(_value(resource, 'id', '') or ''),
        str(_value(resource, 'self_link', '') or ''),
    )


def _required_resource_identity(prefix: str, resource: Any) -> tuple[str, str]:
    resource_id, self_link = _resource_identity(resource)
    if not resource_id or not self_link:
        raise RuntimeError(
            f'GCP {prefix.removeprefix("gcp_").replace("_", " ")} returned '
            'incomplete immutable identity metadata; retaining its response-loss '
            'contract for safe reconciliation.'
        )
    return resource_id, self_link


def _create_named_resource(
    job: dict[str, Any],
    persist: PersistCallback | None,
    *,
    prefix: str,
    name: str,
    client: Any,
    clients: Mapping[str, Any],
    insert_request_type: str,
    insert_values: Mapping[str, Any],
    get_request_type: str,
    get_values: Mapping[str, Any],
    clear_contract_on_confirmed_rejection: bool | None = None,
):
    """Insert one deterministic resource with a durable ambiguity contract."""
    resources = job.setdefault('resources', {})
    request_key = f'{prefix}_request_id'
    ambiguous_key = f'{prefix}_create_ambiguous'
    error_key = f'{prefix}_reconciliation_error'
    prior_ambiguity = resources.get(ambiguous_key) is True
    prior_contract = any(
        key in resources
        for key in (
            f'{prefix}_name',
            f'{prefix}_id',
            f'{prefix}_self_link',
            request_key,
            ambiguous_key,
        )
    )
    clear_confirmed = (
        not prior_contract
        if clear_contract_on_confirmed_rejection is None
        else clear_contract_on_confirmed_rejection
    )
    request_id = str(resources.get(request_key) or uuid.uuid4())
    _forget(job, persist, error_key)
    _record(
        job,
        persist,
        **{
            f'{prefix}_name': name,
            request_key: request_id,
            ambiguous_key: True,
        },
    )
    values = dict(insert_values)
    values['request_id'] = request_id
    try:
        operation = _call(
            client,
            'insert',
            clients,
            insert_request_type,
            **values,
        )
    except Exception as exc:
        if _http_status(exc) == 409:
            # requestId retries and deterministic names may surface an
            # AlreadyExists response.  Accept it only after exact-name
            # discovery proves this is the resource body owned by this job.
            resource = _get_or_none(
                client,
                clients,
                get_request_type,
                **dict(get_values),
            )
            if resource is not None:
                _assert_insert_resource_matches(
                    prefix,
                    resource,
                    next(
                        value
                        for key, value in insert_values.items()
                        if key.endswith('_resource')
                    ),
                    project_id=str(insert_values.get('project') or ''),
                    zone=str(insert_values.get('zone') or ''),
                )
                _assert_saved_identity(prefix, resource, resources)
                resource_id, self_link = _required_resource_identity(
                    prefix, resource
                )
                _record(
                    job,
                    persist,
                    **{
                        f'{prefix}_id': resource_id,
                        f'{prefix}_self_link': self_link,
                        ambiguous_key: False,
                    },
                )
                return resource
            raise
        # Only a deterministic rejection of the insert call itself proves
        # that no resource was accepted. Once an operation is returned, a
        # wait/GET failure (including a visibility-lag 404) stays ambiguous.
        if _confirmed_rejection(exc) and clear_confirmed:
            _forget(
                job,
                persist,
                f'{prefix}_name',
                request_key,
                ambiguous_key,
                error_key,
            )
        raise
    expected_resource = next(
        value
        for key, value in insert_values.items()
        if key.endswith('_resource')
    )
    try:
        _wait(operation)
    except Exception:
        # A polling/transport failure remains response-ambiguous and must keep
        # the original requestId. A conclusively completed failed operation,
        # however, has consumed that UUID; reconcile the exact name before
        # either adopting the verified resource or clearing the failed create
        # contract so a later provision can use a fresh UUID.
        if not _operation_completed_with_error(operation):
            raise
        resource = None
        for attempt in range(RECONCILIATION_ATTEMPTS):
            resource = _get_or_none(
                client,
                clients,
                get_request_type,
                **dict(get_values),
            )
            if resource is not None:
                break
            if attempt + 1 < RECONCILIATION_ATTEMPTS:
                time.sleep(RECONCILIATION_DELAY_SECONDS)
        if resource is None:
            _forget(job, persist, *_contract_keys(prefix))
            raise
        _assert_insert_resource_matches(
            prefix,
            resource,
            expected_resource,
            project_id=str(insert_values.get('project') or ''),
            zone=str(insert_values.get('zone') or ''),
        )
        _assert_saved_identity(prefix, resource, resources)
        resource_id, self_link = _required_resource_identity(prefix, resource)
        _record(
            job,
            persist,
            **{
                f'{prefix}_id': resource_id,
                f'{prefix}_self_link': self_link,
                ambiguous_key: False,
            },
        )
        return resource
    try:
        resource = _call(
            client,
            'get',
            clients,
            get_request_type,
            **dict(get_values),
        )
        _assert_insert_resource_matches(
            prefix,
            resource,
            expected_resource,
            project_id=str(insert_values.get('project') or ''),
            zone=str(insert_values.get('zone') or ''),
        )
        _assert_saved_identity(prefix, resource, resources)
    except Exception:
        # The insert operation succeeded, so visibility/matcher failures must
        # retain the original ambiguity contract for deterministic replay.
        raise
    resource_id, self_link = _required_resource_identity(prefix, resource)
    # Persist the exact identity and resolve ambiguity in one state write.
    _record(
        job,
        persist,
        **{
            f'{prefix}_id': resource_id,
            f'{prefix}_self_link': self_link,
            ambiguous_key: False,
        },
    )
    return resource


def _link_equal(left: Any, right: Any) -> bool:
    """Compare absolute or project-relative Compute resource references."""
    if not left or not right:
        return left == right
    left = str(left).split('?', 1)[0].rstrip('/')
    right = str(right).split('?', 1)[0].rstrip('/')

    def canonical(value: str) -> str | None:
        marker = '/projects/'
        if marker in value:
            return 'projects/' + value.split(marker, 1)[1]
        if value.startswith('projects/'):
            return value
        return None

    left_canonical = canonical(left)
    right_canonical = canonical(right)
    if left_canonical and right_canonical:
        return left_canonical == right_canonical
    # A bare resource name is used in a few request fields. Only that narrow
    # form may compare by basename; two scoped links must never discard their
    # project/region/zone relationship.
    if '/' not in left or '/' not in right:
        return _basename(left) == _basename(right)
    return left == right


def _scoped_link_equal(left: Any, right: Any) -> bool:
    """Compare two links only when both retain their project scope."""
    def canonical(value: Any) -> str | None:
        value = str(value or '').split('?', 1)[0].rstrip('/')
        marker = '/projects/'
        if marker in value:
            return 'projects/' + value.split(marker, 1)[1]
        if value.startswith('projects/'):
            return value
        return None

    left_canonical = canonical(left)
    right_canonical = canonical(right)
    return bool(
        left_canonical
        and right_canonical
        and left_canonical == right_canonical
    )


def _zonal_resource_link(
    project_id: str,
    zone: str,
    collection: str,
    name: str,
) -> str:
    """Return a project-scoped Compute Engine zonal resource reference."""
    if not project_id or not zone or not name:
        raise RuntimeError(
            'A complete GCP project, zone, and resource name are required for '
            'relationship verification.'
        )
    return f'projects/{project_id}/zones/{zone}/{collection}/{name}'


def _string_set(value: Any) -> set[str]:
    return {str(item) for item in (value or ())}


def _allowed_set(value: Any) -> set[tuple[str, tuple[str, ...]]]:
    return {
        (
            str(_value(item, 'I_p_protocol', '') or '').lower(),
            tuple(sorted(str(port) for port in (_value(item, 'ports', ()) or ()))),
        )
        for item in (value or ())
    }


def _assert_insert_resource_matches(
    prefix: str,
    actual: Any,
    expected: Any,
    *,
    project_id: str = '',
    zone: str = '',
):
    """Fail closed when an exact-name insert collides with another resource."""
    label = prefix.removeprefix('gcp_').replace('_', ' ')
    for field in ('name', 'description'):
        wanted = _value(expected, field)
        if wanted is not None and str(_value(actual, field, '') or '') != str(wanted):
            raise RuntimeError(
                f'Refusing to use GCP {label}: its {field} does not match '
                'this benchmark job.'
            )
    expected_labels = dict(_value(expected, 'labels', {}) or {})
    actual_labels = dict(_value(actual, 'labels', {}) or {})
    if expected_labels and any(
        actual_labels.get(key) != value
        for key, value in expected_labels.items()
    ):
        raise RuntimeError(
            f'Refusing to use GCP {label}: required ownership labels are missing.'
        )
    for field in ('network', 'subnetwork', 'machine_type', 'type_'):
        wanted = _value(expected, field)
        if wanted and not _link_equal(_value(actual, field), wanted):
            raise RuntimeError(
                f'Refusing to use GCP {label}: its {field} relationship differs.'
            )
    for field in ('ip_cidr_range', 'size_gb'):
        wanted = _value(expected, field)
        if wanted is not None and str(_value(actual, field, '') or '') != str(wanted):
            raise RuntimeError(
                f'Refusing to use GCP {label}: its {field} differs.'
            )
    for field in ('provisioned_iops', 'provisioned_throughput'):
        wanted = _value(expected, field)
        # Proto-plus exposes an unset integer scalar as zero.  Zero is never a
        # provisioned performance request in this provider; treat it as absent
        # so GCE's reported baseline pd-ssd capability is not mistaken for
        # drift. Positive Hyperdisk values remain exact and fail closed.
        if int(wanted or 0) and int(_value(actual, field, 0) or 0) != int(
            wanted
        ):
            raise RuntimeError(
                f'Refusing to use GCP {label}: its {field} differs.'
            )
    for field in ('source_ranges', 'source_tags', 'target_tags'):
        wanted = _value(expected, field)
        if wanted is not None and _string_set(_value(actual, field)) != _string_set(wanted):
            raise RuntimeError(
                f'Refusing to use GCP {label}: its {field} differs.'
            )
    wanted_allowed = _value(expected, 'allowed')
    if wanted_allowed is not None and _allowed_set(
        _value(actual, 'allowed')
    ) != _allowed_set(wanted_allowed):
        raise RuntimeError(
            f'Refusing to use GCP {label}: its allowed ports differ.'
        )
    if prefix in {'gcp_network', 'gcp_dsb_network'}:
        actual_routing = _value(actual, 'routing_config', {}) or {}
        expected_routing = _value(expected, 'routing_config', {}) or {}
        if (
            bool(_value(actual, 'auto_create_subnetworks', True))
            != bool(_value(expected, 'auto_create_subnetworks', False))
            or str(_value(actual_routing, 'routing_mode', '') or '')
            != str(_value(expected_routing, 'routing_mode', '') or '')
        ):
            raise RuntimeError(
                f'Refusing to use GCP {label}: custom network mode differs.'
            )
    if prefix in {
        'gcp_subnet',
        'gcp_dsb_cluster_subnet',
        'gcp_dsb_loadgen_subnet',
    }:
        for field in ('private_ip_google_access', 'stack_type'):
            if str(_value(actual, field, '') or '') != str(
                _value(expected, field, '') or ''
            ):
                raise RuntimeError(
                    f'Refusing to use GCP {label}: its {field} differs.'
                )
    if prefix in {
        'gcp_ssh_firewall',
        'gcp_iperf_firewall',
        'gcp_web_firewall',
        *DISTRIBUTED_FIREWALL_PREFIXES,
    }:
        for field in ('direction', 'priority', 'disabled'):
            if str(_value(actual, field, '') or '') != str(
                _value(expected, field, '') or ''
            ):
                raise RuntimeError(
                    f'Refusing to use GCP {label}: its {field} differs.'
                )
    if prefix in {
        'gcp_instance',
        'gcp_peer_instance',
        'gcp_loadgen_instance',
        *DISTRIBUTED_NODE_PREFIXES.values(),
    }:
        actual_tags = _string_set(
            _value(_value(actual, 'tags', {}) or {}, 'items', ())
        )
        expected_tags = _string_set(
            _value(_value(expected, 'tags', {}) or {}, 'items', ())
        )
        if actual_tags != expected_tags:
            raise RuntimeError(f'Refusing to use GCP {label}: tags differ.')
        actual_metadata = {
            str(_value(item, 'key', '')): str(_value(item, 'value', '') or '')
            for item in (_value(_value(actual, 'metadata', {}) or {}, 'items', ()) or ())
        }
        expected_metadata = {
            str(_value(item, 'key', '')): str(_value(item, 'value', '') or '')
            for item in (_value(_value(expected, 'metadata', {}) or {}, 'items', ()) or ())
        }
        if actual_metadata != expected_metadata:
            raise RuntimeError(f'Refusing to use GCP {label}: metadata differs.')
        if (_value(actual, 'service_accounts', ()) or ()) != (
            _value(expected, 'service_accounts', ()) or ()
        ):
            raise RuntimeError(
                f'Refusing to use GCP {label}: service accounts differ.'
            )
        actual_nics = list(_value(actual, 'network_interfaces', ()) or ())
        expected_nics = list(_value(expected, 'network_interfaces', ()) or ())
        if len(actual_nics) != 1 or len(expected_nics) != 1:
            raise RuntimeError(
                f'Refusing to use GCP {label}: network interfaces differ.'
            )
        for field in ('network', 'subnetwork'):
            if not _link_equal(
                _value(actual_nics[0], field), _value(expected_nics[0], field)
            ):
                raise RuntimeError(
                    f'Refusing to use GCP {label}: NIC {field} differs.'
                )
        if str(_value(actual_nics[0], 'stack_type', '') or '') != str(
            _value(expected_nics[0], 'stack_type', '') or ''
        ):
            raise RuntimeError(
                f'Refusing to use GCP {label}: NIC stack_type differs.'
            )
        expected_private_ip = (
            _value(expected_nics[0], 'network_i_p')
            or _value(expected_nics[0], 'network_ip')
        )
        actual_private_ip = (
            _value(actual_nics[0], 'network_i_p')
            or _value(actual_nics[0], 'network_ip')
        )
        if expected_private_ip and str(actual_private_ip or '') != str(
            expected_private_ip
        ):
            raise RuntimeError(
                f'Refusing to use GCP {label}: private IP address differs.'
            )
        expected_nic_type = str(
            _value(expected_nics[0], 'nic_type', '') or ''
        )
        actual_nic_type = str(_value(actual_nics[0], 'nic_type', '') or '')
        if expected_nic_type and actual_nic_type != expected_nic_type:
            raise RuntimeError(
                f'Refusing to use GCP {label}: NIC nic_type differs.'
            )
        actual_access = list(_value(actual_nics[0], 'access_configs', ()) or ())
        expected_access = list(_value(expected_nics[0], 'access_configs', ()) or ())
        if len(actual_access) != len(expected_access):
            raise RuntimeError(
                f'Refusing to use GCP {label}: external access differs.'
            )
        for actual_config, expected_config in zip(actual_access, expected_access):
            for field in ('name', 'type_', 'network_tier'):
                if str(_value(actual_config, field, '') or '') != str(
                    _value(expected_config, field, '') or ''
                ):
                    raise RuntimeError(
                        f'Refusing to use GCP {label}: access config differs.'
                    )
        actual_disks = {
            str(_value(item, 'device_name', '') or ''): item
            for item in (_value(actual, 'disks', ()) or ())
        }
        expected_disks = {
            str(_value(item, 'device_name', '') or ''): item
            for item in (_value(expected, 'disks', ()) or ())
        }
        if set(actual_disks) != set(expected_disks):
            raise RuntimeError(
                f'Refusing to use GCP {label}: attached disk set differs.'
            )
        for device_name, expected_disk in expected_disks.items():
            actual_disk = actual_disks[device_name]
            for field in ('boot', 'auto_delete', 'mode', 'type_'):
                if str(_value(actual_disk, field, '') or '') != str(
                    _value(expected_disk, field, '') or ''
                ):
                    raise RuntimeError(
                        f'Refusing to use GCP {label}: disk {device_name} '
                        f'{field} differs.'
                    )
            expected_interface = str(
                _value(expected_disk, 'interface', '') or ''
            )
            if expected_interface and str(
                _value(actual_disk, 'interface', '') or ''
            ) != expected_interface:
                raise RuntimeError(
                    f'Refusing to use GCP {label}: disk {device_name} '
                    'interface differs.'
                )
            expected_source = _value(expected_disk, 'source')
            if not expected_source:
                expected_disk_name = str(_value(
                    _value(expected_disk, 'initialize_params', {}) or {},
                    'disk_name',
                ) or '')
                # Instance inserts specify an initializeParams.diskName while
                # the returned instance exposes a fully scoped disk source.
                # Never collapse that relationship to a basename: adopting an
                # exact-name VM with a same-named foreign-project boot disk can
                # make its auto-delete disk destructive during cleanup.
                expected_source = _zonal_resource_link(
                    project_id,
                    zone,
                    'disks',
                    expected_disk_name,
                )
            if not _scoped_link_equal(
                _value(actual_disk, 'source'), expected_source
            ):
                raise RuntimeError(
                    f'Refusing to use GCP {label}: disk {device_name} source differs.'
                )
    if prefix == 'gcp_dsb_router':
        expected_bgp = _value(expected, 'bgp', {}) or {}
        actual_bgp = _value(actual, 'bgp', {}) or {}
        if int(_value(actual_bgp, 'asn', 0) or 0) != int(
            _value(expected_bgp, 'asn', 0) or 0
        ):
            raise RuntimeError(
                'Refusing to use GCP distributed router: BGP ASN differs.'
            )
        expected_nats = list(_value(expected, 'nats', ()) or ())
        actual_nats = list(_value(actual, 'nats', ()) or ())
        if len(actual_nats) != len(expected_nats):
            raise RuntimeError(
                'Refusing to use GCP distributed router: NAT count differs.'
            )
        if not expected_nats:
            return
        for field in (
            'name',
            'nat_ip_allocate_option',
            'source_subnetwork_ip_ranges_to_nat',
            'enable_endpoint_independent_mapping',
        ):
            if str(_value(actual_nats[0], field, '') or '') != str(
                _value(expected_nats[0], field, '') or ''
            ):
                raise RuntimeError(
                    'Refusing to use GCP distributed router: '
                    f'NAT {field} differs.'
                )
        expected_subnets = list(
            _value(expected_nats[0], 'subnetworks', ()) or ()
        )
        actual_subnets = list(
            _value(actual_nats[0], 'subnetworks', ()) or ()
        )
        subnet_matches = (
            len(actual_subnets) == 1
            and len(expected_subnets) == 1
            and _link_equal(
                _value(actual_subnets[0], 'name'),
                _value(expected_subnets[0], 'name'),
            )
            and _string_set(
                _value(actual_subnets[0], 'source_ip_ranges_to_nat')
            ) == _string_set(
                _value(expected_subnets[0], 'source_ip_ranges_to_nat')
            )
        )
        if not subnet_matches:
            raise RuntimeError(
                'Refusing to use GCP distributed router: NAT subnet differs.'
            )


def _machine_type_details(
    project_id: str,
    zone: str,
    machine_type: str,
    clients: Mapping[str, Any],
    *,
    missing_architecture_fallback: str | None = None,
):
    item = _call(
        clients['machine_types'],
        'get',
        clients,
        'GetMachineTypeRequest',
        project=project_id,
        zone=zone,
        machine_type=machine_type,
    )
    if _deprecated_state(item) in {'OBSOLETE', 'DELETED'}:
        raise ValueError(f'GCP machine type {machine_type} is no longer launchable.')
    profile = _machine_storage_profile(
        item,
        missing_architecture_fallback=missing_architecture_fallback,
    )
    if not profile:
        raise ValueError(
            f'GCP machine type {machine_type} is not supported by the '
            'implemented benchmark storage profiles. Choose a supported '
            'machine without bundled Local SSD, metal, or accelerators.'
        )
    details = _machine_summary(
        item,
        missing_architecture_fallback=missing_architecture_fallback,
    )
    if details['vcpu'] < 1 or details['memory_gb'] < 1:
        raise ValueError(f'GCP machine type {machine_type} has invalid capacity metadata.')
    details['self_link'] = str(_value(item, 'self_link', '') or '')
    return details


def _resolve_zone_and_machine(
    project_id: str,
    region: str,
    machine_type: str,
    requested_zone: str | None,
    clients: Mapping[str, Any],
):
    zone_items = placement(project_id, region, clients=clients)['availability_zones']
    zones = [item['name'] for item in zone_items]
    if requested_zone:
        if requested_zone not in zones:
            raise ValueError(
                f'GCP zone {requested_zone} is not UP in region {region}. '
                'Refresh placement and choose another zone.'
            )
        candidates = [requested_zone]
    else:
        candidates = zones
    for zone in candidates:
        try:
            return zone, _machine_type_details(
                project_id, zone, machine_type, clients
            )
        except Exception as exc:
            if _not_found(exc):
                continue
            raise
    if requested_zone:
        raise ValueError(
            f'GCP machine type {machine_type} is not available in {requested_zone}.'
        )
    raise ValueError(
        f'GCP machine type {machine_type} is not available in an UP zone in '
        f'{region}. Refresh the machine-type list and try again.'
    )


def _validate_plan_capacity(plan: Any, details: Mapping[str, Any]):
    claimed_cpus = float(_value(plan, 'ocpus', details['vcpu']))
    claimed_memory = float(_value(plan, 'memory_gb', details['memory_gb']))
    if claimed_cpus != float(details['vcpu']) or abs(
        claimed_memory - float(details['memory_gb'])
    ) > 0.001:
        raise ValueError(
            'The submitted GCP vCPU or memory value does not match the '
            'selected fixed machine type. Refresh discovery and try again.'
        )
    benchmarks = set(_value(plan, 'benchmarks', ()) or ())
    phoronix = _value(plan, 'phoronix', {}) or {}
    profiles = set(_value(phoronix, 'profiles', ()) or ())
    if (
        'phoronix' in benchmarks
        and 'build_linux_kernel' in profiles
        and float(details['memory_gb']) < 4
    ):
        raise ValueError(
            'Phoronix Linux Kernel Compilation on GCP requires a machine '
            f'with at least 4 GiB of memory; {details["machine_type"]} has '
            f'{details["memory_gb"]:g} GiB.'
        )


def _loadgen_machine_details(
    project_id: str,
    zone: str,
    clients: Mapping[str, Any],
) -> dict[str, Any]:
    """Resolve and validate the fixed, architecture-neutral web load shape."""

    try:
        details = _machine_type_details(
            project_id,
            zone,
            LOADGEN_MACHINE_TYPE,
            clients,
            # The Compute API omits architecture for N2 in some projects. N2
            # is a fixed x86 family, so infer x86 only when this field is
            # absent. Explicit, contradictory metadata remains authoritative
            # and fails the compatibility check below.
            missing_architecture_fallback=LOADGEN_ARCHITECTURE,
        )
    except Exception as exc:
        if _not_found(exc):
            raise ValueError(
                f'GCP web benchmarks require {LOADGEN_MACHINE_TYPE}, but it '
                f'is unavailable in {zone}. Choose another zone.'
            ) from exc
        raise RuntimeError(
            f'Unable to validate the required GCP web load generator '
            f'{LOADGEN_MACHINE_TYPE} in {zone}: {exc}'
        ) from exc
    compatible = (
        details.get('machine_type') == LOADGEN_MACHINE_TYPE
        and details.get('architecture') == LOADGEN_ARCHITECTURE
        and int(details.get('vcpu', 0)) == LOADGEN_VCPUS
        and float(details.get('memory_gb', 0)) == LOADGEN_MEMORY_GB
        and details.get('disk_type') == LOADGEN_DISK_TYPE
        and not bool(details.get('shared_cpu'))
    )
    if not compatible:
        raise ValueError(
            f'GCP web benchmarks require an x86_64 {LOADGEN_MACHINE_TYPE} '
            f'with {LOADGEN_VCPUS} vCPUs, {LOADGEN_MEMORY_GB} GiB, and '
            f'{LOADGEN_DISK_TYPE} in {zone}; the returned machine metadata '
            'is incompatible.'
        )
    return details


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


def _validate_manifest_capacity(node: Any, details: Mapping[str, Any]):
    if node.selected_shape and details['machine_type'] != node.selected_shape:
        raise ValueError(
            f'GCP role {node.key} resolved a machine type outside its '
            'topology contract.'
        )
    if (
        node.capacity.minimum_vcpus is not None
        and int(details['vcpu']) < node.capacity.minimum_vcpus
    ):
        raise ValueError(
            f'GCP machine type {details["machine_type"]} does not satisfy '
            f'the {node.key} vCPU capacity contract.'
        )
    if (
        node.capacity.minimum_memory_gib is not None
        and float(details['memory_gb']) < node.capacity.minimum_memory_gib
    ):
        raise ValueError(
            f'GCP machine type {details["machine_type"]} does not satisfy '
            f'the {node.key} memory capacity contract.'
        )
    required_architecture = node.capacity.required_architecture
    if (
        required_architecture is not None
        and details['architecture'] != required_architecture
    ):
        raise ValueError(
            f'GCP machine type {details["machine_type"]} does not satisfy '
            f'the {node.key} architecture contract.'
        )
    if node.architecture and details['architecture'] != node.architecture:
        raise ValueError(
            f'GCP machine type {details["machine_type"]} architecture '
            f'conflicts with the {node.key} topology contract.'
        )


def _distributed_role_details(
    manifest: DeathStarBenchTopologyManifest,
    *,
    project_id: str,
    zone: str,
    application_details: Mapping[str, Any],
    clients: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    details_by_key: dict[str, dict[str, Any]] = {}
    for node in manifest.nodes:
        details = (
            dict(application_details)
            if node.key == 'application'
            else _machine_type_details(
                project_id,
                zone,
                DISTRIBUTED_ROLE_MACHINE_TYPES[node.key],
                clients,
                missing_architecture_fallback='x86_64',
            )
        )
        expected_machine = (
            application_details['machine_type']
            if node.key == 'application'
            else DISTRIBUTED_ROLE_MACHINE_TYPES[node.key]
        )
        if details['machine_type'] != expected_machine:
            raise ValueError(
                f'GCP distributed role {node.key} resolved an unexpected '
                'machine type.'
            )
        _validate_manifest_capacity(node, details)
        details_by_key[node.key] = details
    return details_by_key


def _distributed_candidate_image(
    resources: Mapping[str, Any],
    *,
    key_prefix: str,
    architecture: str,
    clients: Mapping[str, Any],
) -> dict[str, str]:
    architecture = normalize_architecture(architecture)
    keys = {
        'image_id': f'{key_prefix}_id',
        'name': f'{key_prefix}_name',
        'self_link': f'{key_prefix}_self_link',
        'family': f'{key_prefix}_family',
        'architecture': f'{key_prefix}_architecture',
    }
    present = [key for key in keys.values() if key in resources]
    if present:
        if len(present) != len(keys):
            raise ResourceInventoryError(
                f'GCP distributed image contract {key_prefix} is incomplete.'
            )
        image = {field: str(resources[key]) for field, key in keys.items()}
        if image['architecture'] != architecture:
            raise ResourceInventoryError(
                f'GCP distributed image contract {key_prefix} has a '
                'conflicting architecture.'
            )
        if image['family'] != IMAGE_FAMILIES[architecture]:
            raise ResourceInventoryError(
                f'GCP distributed image contract {key_prefix} has a '
                'conflicting family.'
            )
        expected_link = f'projects/{IMAGE_PROJECT}/global/images/{image["name"]}'
        if not _scoped_link_equal(image['self_link'], expected_link):
            raise ResourceInventoryError(
                f'GCP distributed image contract {key_prefix} has an '
                'unexpected project or image.'
            )
        return image
    return latest_rocky_linux_9_image(architecture, clients=clients)


def _planned_gcp_inventory(
    manifest: DeathStarBenchTopologyManifest,
    *,
    zone: str,
    names: Mapping[str, str],
    details_by_key: Mapping[str, Mapping[str, Any]],
    database_disk_name: str,
    database_disk_device: str,
) -> RoleNodeInventory:
    inventory = manifest.planned_inventory('gcp')
    for node in inventory.nodes:
        storage = node.storage
        if node.key == 'database':
            storage_contract = next(
                item for item in manifest.nodes
                if item.key == 'database'
            ).storage[0]
            storage = (StorageResource(
                key=storage_contract.key,
                kind='persistent_block_volume',
                provider_resource_name=database_disk_name,
                device=database_disk_device,
                mount_point=storage_contract.mount_point,
                filesystem=storage_contract.filesystem,
                size_gb=storage_contract.size_gib,
                provisioned_iops=storage_contract.minimum_iops,
                provisioned_throughput_mibps=(
                    storage_contract.minimum_throughput_mibps
                ),
                ephemeral=False,
                lifecycle_status='planned',
            ),)
        details = details_by_key[node.key]
        inventory = inventory.upsert(RoleNode(
            key=node.key,
            role=node.role,
            provider_resource_name=names[node.key],
            private_addresses=(DISTRIBUTED_PRIVATE_ADDRESSES[node.key],),
            zone=zone,
            shape=str(details['machine_type']),
            architecture=str(details['architecture']),
            capacity_class=node.capacity_class,
            storage=storage,
            lifecycle_status='planned',
        ))
    return inventory


def _distributed_firewall_specs(
    manifest: DeathStarBenchTopologyManifest,
    *,
    job_id: str,
    network: str,
    tags: Mapping[str, str],
    clients: Mapping[str, Any],
) -> tuple[tuple[str, str, Any], ...]:
    channels = {
        channel.key: channel
        for channel in manifest.network_policy.channels
    }

    def allowed(channel_key: str) -> list[dict[str, Any]]:
        channel = channels[channel_key]
        return [{
            'I_p_protocol': channel.protocol,
            'ports': [str(port) for port in channel.ports],
        }]

    cluster_roles = ('control', 'database', 'cache', 'application')
    definitions = (
        (
            'gcp_dsb_public_ssh_firewall',
            'dsb-public-ssh',
            ('control', 'application', 'load-generator'),
            (SSH_SOURCE_CIDR,),
            (),
            [{'I_p_protocol': 'tcp', 'ports': ['22']}],
        ),
        (
            'gcp_dsb_private_ssh_firewall',
            'dsb-private-ssh',
            ('database', 'cache'),
            (),
            ('control', 'application'),
            [{'I_p_protocol': 'tcp', 'ports': ['22']}],
        ),
        (
            'gcp_dsb_k3s_api_firewall',
            'dsb-k3s-api',
            ('control',),
            (),
            ('database', 'cache', 'application'),
            allowed(K3S_CONTROL_PLANE_CHANNEL),
        ),
        (
            'gcp_dsb_k3s_overlay_firewall',
            'dsb-k3s-overlay',
            cluster_roles,
            (),
            cluster_roles,
            allowed(K3S_OVERLAY_CHANNEL),
        ),
        (
            'gcp_dsb_benchmark_firewall',
            'dsb-benchmark',
            ('application',),
            (),
            ('load-generator',),
            allowed(BENCHMARK_INGRESS_CHANNEL),
        ),
    )
    result = []
    for prefix, suffix, targets, source_ranges, sources, rules in definitions:
        name = _resource_name(job_id, suffix)
        body = _distributed_firewall_resource(
            clients,
            job_id,
            name,
            network,
            role=suffix,
            target_tags=(tags[key] for key in targets),
            source_ranges=source_ranges,
            source_tags=(tags[key] for key in sources),
            allowed=rules,
        )
        result.append((prefix, name, body))
    return tuple(result)


def _network_resource(
    clients: Mapping[str, Any],
    job_id: str,
    name: str,
    *,
    role: str = 'network',
):
    return _message(clients, 'Network', {
        'name': name,
        'description': _description(job_id, role),
        'auto_create_subnetworks': False,
        'routing_config': {'routing_mode': 'REGIONAL'},
    })


def _subnet_resource(
    clients: Mapping[str, Any],
    job_id: str,
    name: str,
    network: str,
    *,
    role: str = 'subnet',
    cidr: str = SUBNET_CIDR,
):
    return _message(clients, 'Subnetwork', {
        'name': name,
        'description': _description(job_id, role),
        'network': network,
        'ip_cidr_range': cidr,
        'private_ip_google_access': False,
        'stack_type': 'IPV4_ONLY',
    })


def _router_resource(
    clients: Mapping[str, Any],
    job_id: str,
    name: str,
    network: str,
    cluster_subnet: str,
    *,
    include_nat: bool = True,
):
    """Build the regional router, optionally with its Cloud NAT feature.

    Compute Engine refuses a router insert that enables BGP and Cloud NAT in
    the same create operation.  Provisioning therefore inserts the base
    router first and applies this exact NAT body with a separate idempotent
    patch.
    """

    values = {
        'name': name,
        'description': _description(job_id, 'distributed-router'),
        'network': network,
        'bgp': {'asn': DISTRIBUTED_ROUTER_ASN},
    }
    if include_nat:
        values['nats'] = [{
            'name': f'{name}-nat',
            'nat_ip_allocate_option': 'AUTO_ONLY',
            'source_subnetwork_ip_ranges_to_nat': 'LIST_OF_SUBNETWORKS',
            'subnetworks': [{
                'name': cluster_subnet,
                'source_ip_ranges_to_nat': ['ALL_IP_RANGES'],
            }],
            'enable_endpoint_independent_mapping': True,
        }]
    return _message(clients, 'Router', values)


def _ensure_distributed_router(
    job: dict[str, Any],
    persist: PersistCallback | None,
    *,
    clients: Mapping[str, Any],
    project_id: str,
    region: str,
    name: str,
    network: str,
    cluster_subnet: str,
):
    """Create the base router, then idempotently enable its exact NAT."""

    prefix = 'gcp_dsb_router'
    resources = job['resources']
    base = _router_resource(
        clients,
        str(job['id']).lower(),
        name,
        network,
        cluster_subnet,
        include_nat=False,
    )
    complete = _router_resource(
        clients,
        str(job['id']).lower(),
        name,
        network,
        cluster_subnet,
    )
    get_values = {
        'project': project_id,
        'region': region,
        'router': name,
    }

    router = None
    if resources.get(f'{prefix}_name') == name:
        router = _get_or_none(
            clients['routers'],
            clients,
            'GetRouterRequest',
            **get_values,
        )
        if router is not None:
            actual_nats = list(_value(router, 'nats', ()) or ())
            _assert_insert_resource_matches(
                prefix,
                router,
                complete if actual_nats else base,
                project_id=project_id,
            )
            _assert_saved_identity(prefix, router, resources)
            _persist_recovered_identity(job, persist, prefix, router)
    if router is None:
        router = _create_named_resource(
            job,
            persist,
            prefix=prefix,
            name=name,
            client=clients['routers'],
            clients=clients,
            insert_request_type='InsertRouterRequest',
            insert_values={
                'project': project_id,
                'region': region,
                'router_resource': base,
            },
            get_request_type='GetRouterRequest',
            get_values=get_values,
        )

    if list(_value(router, 'nats', ()) or ()):
        _assert_insert_resource_matches(
            prefix,
            router,
            complete,
            project_id=project_id,
        )
        _record(
            job,
            persist,
            gcp_dsb_router_nat_configured=True,
            gcp_dsb_router_nat_update_ambiguous=False,
        )
        return router
    if resources.get('gcp_dsb_router_nat_configured') is True:
        raise RuntimeError(
            'The GCP distributed router lost its recorded Cloud NAT feature.'
        )
    _assert_insert_resource_matches(
        prefix,
        router,
        base,
        project_id=project_id,
    )

    request_key = 'gcp_dsb_router_nat_request_id'
    ambiguous_key = 'gcp_dsb_router_nat_update_ambiguous'
    request_id = str(resources.get(request_key) or uuid.uuid4())
    if resources.get(request_key) is not None and not _valid_request_id(
        request_id
    ):
        raise ResourceInventoryError(
            'The GCP distributed router NAT request ID is invalid.'
        )
    _record(
        job,
        persist,
        **{
            request_key: request_id,
            ambiguous_key: True,
            'gcp_dsb_router_nat_configured': False,
        },
    )
    try:
        operation = _call(
            clients['routers'],
            'patch',
            clients,
            'PatchRouterRequest',
            project=project_id,
            region=region,
            router=name,
            router_resource=complete,
            request_id=request_id,
        )
    except Exception as exc:
        if _confirmed_rejection(exc):
            _forget(job, persist, request_key, ambiguous_key)
        raise
    try:
        _wait(operation)
    except Exception:
        if _operation_completed_with_error(operation):
            _forget(job, persist, request_key, ambiguous_key)
        raise
    router = _call(
        clients['routers'],
        'get',
        clients,
        'GetRouterRequest',
        **get_values,
    )
    _assert_saved_identity(prefix, router, resources)
    _assert_insert_resource_matches(
        prefix,
        router,
        complete,
        project_id=project_id,
    )
    _record(
        job,
        persist,
        gcp_dsb_router_nat_configured=True,
        gcp_dsb_router_nat_update_ambiguous=False,
    )
    return router


def _firewall_resource(
    clients: Mapping[str, Any],
    job_id: str,
    name: str,
    network: str,
    *,
    role: str,
    target_tag: str | None = None,
    target_tags: Iterable[str] = (),
    source_ranges: Iterable[str] = (),
    source_tags: Iterable[str] = (),
    protocols: Iterable[str] = (),
    web_benchmarks: Iterable[str] = (),
):
    allowed = []
    protocols = tuple(protocols)
    targets = tuple(dict.fromkeys(
        str(tag)
        for tag in (tuple(target_tags) or ((target_tag,) if target_tag else ()))
        if str(tag)
    ))
    if not targets:
        raise ValueError('A GCP firewall target tag is required.')
    if role == 'ssh-firewall':
        allowed.append({'I_p_protocol': 'tcp', 'ports': ['22']})
    elif role == 'web-firewall':
        selected = set(str(item) for item in web_benchmarks)
        ports = []
        if 'apachebench' in selected:
            ports.append('80')
        if 'deathstarbench' in selected:
            ports.extend(('5000', '8080'))
        if not ports:
            raise ValueError(
                'A GCP web firewall requires ApacheBench or DeathStarBench.'
            )
        allowed.append({'I_p_protocol': 'tcp', 'ports': ports})
    else:
        # Every iperf3 run needs its TCP control/data listener. UDP/5201 is
        # opened only when the UDP workload is selected.
        allowed.append({'I_p_protocol': 'tcp', 'ports': ['5201']})
        if 'udp' in protocols:
            allowed.append({'I_p_protocol': 'udp', 'ports': ['5201']})
        if 'sctp' in protocols:
            # GCE accepts SCTP by name or protocol number, but port ranges are
            # defined only for TCP and UDP. Keep this source/target-tag scoped
            # and omit a misleading port field.
            allowed.append({'I_p_protocol': '132'})
    return _message(clients, 'Firewall', {
        'name': name,
        'description': _description(job_id, role),
        'network': network,
        'direction': 'INGRESS',
        'priority': 1000,
        'source_ranges': list(source_ranges),
        'source_tags': list(source_tags),
        'target_tags': list(targets),
        'allowed': allowed,
        'disabled': False,
    })


def _distributed_firewall_resource(
    clients: Mapping[str, Any],
    job_id: str,
    name: str,
    network: str,
    *,
    role: str,
    target_tags: Iterable[str],
    allowed: Iterable[Mapping[str, Any]],
    source_ranges: Iterable[str] = (),
    source_tags: Iterable[str] = (),
):
    targets = tuple(dict.fromkeys(str(item) for item in target_tags if item))
    sources = tuple(dict.fromkeys(str(item) for item in source_ranges if item))
    source_role_tags = tuple(dict.fromkeys(
        str(item) for item in source_tags if item
    ))
    rules = [dict(item) for item in allowed]
    if not targets or not rules or (not sources and not source_role_tags):
        raise ValueError(
            'A distributed GCP firewall requires targets, sources, and rules.'
        )
    return _message(clients, 'Firewall', {
        'name': name,
        'description': _description(job_id, role),
        'network': network,
        'direction': 'INGRESS',
        'priority': 1000,
        'source_ranges': list(sources),
        'source_tags': list(source_role_tags),
        'target_tags': list(targets),
        'allowed': rules,
        'disabled': False,
    })


def _data_disk_resource(
    clients: Mapping[str, Any],
    job_id: str,
    name: str,
    project_id: str,
    zone: str,
    size_gb: int,
    *,
    role: str = 'data',
    disk_type: str = DEFAULT_DISK_TYPE,
    provisioned_iops: int | None = None,
    provisioned_throughput_mibps: int | None = None,
):
    body = {
        'name': name,
        'description': _description(job_id, role),
        'size_gb': int(size_gb),
        'type_': f'projects/{project_id}/zones/{zone}/diskTypes/{disk_type}',
        'labels': _labels(job_id, role),
        # No encryption key is supplied: Compute Engine encrypts the disk at
        # rest with Google-owned and Google-managed keys by default.
    }
    if provisioned_iops is not None:
        body['provisioned_iops'] = int(provisioned_iops)
    if provisioned_throughput_mibps is not None:
        body['provisioned_throughput'] = int(provisioned_throughput_mibps)
    return _message(clients, 'Disk', body)


def _instance_resource(
    clients: Mapping[str, Any],
    *,
    job_id: str,
    name: str,
    role: str,
    machine_type: str,
    image: Mapping[str, Any],
    project_id: str,
    zone: str,
    network: str,
    subnet: str,
    network_tag: str | None = None,
    network_tags: Iterable[str] = (),
    boot_size_gb: int,
    disk_type: str = DEFAULT_DISK_TYPE,
    disk_interface: str | None = None,
    network_interface_type: str | None = None,
    disk_provisioned_iops: int | None = None,
    disk_provisioned_throughput_mibps: int | None = None,
    public_key: str | None = None,
    iperf3_protocols: Iterable[str] = (),
    data_disk_self_link: str | None = None,
    data_device_name: str | None = None,
    data_disk_interface: str | None = None,
    private_ip_address: str | None = None,
    assign_public_ip: bool = True,
):
    metadata = [
        {'key': 'block-project-ssh-keys', 'value': 'TRUE'},
        {'key': 'enable-oslogin', 'value': 'FALSE'},
        {'key': 'serial-port-enable', 'value': 'FALSE'},
    ]
    if public_key:
        ssh_items = rocky_linux.ssh_metadata_items(
            public_key, username=SSH_USER
        )
        metadata = [
            item for item in ssh_items
            if item['key'] not in {'block-project-ssh-keys', 'enable-oslogin'}
        ] + metadata
    elif role in {'runner', 'load-generator'}:
        raise ValueError(
            f'An SSH public key is required for the GCP {role}.'
        )
    else:
        metadata.append({
            'key': 'startup-script',
            'value': rocky_linux.iperf3_peer_startup_script(iperf3_protocols),
        })

    boot_name = f'{name}-boot'
    boot_initialize_params = {
        'disk_name': boot_name,
        'disk_size_gb': int(boot_size_gb),
        'disk_type': (
            f'projects/{project_id}/zones/{zone}/diskTypes/{disk_type}'
        ),
        'source_image': image['self_link'],
        'labels': _labels(job_id, f'{role}-boot'),
        'description': _description(job_id, f'{role}-boot'),
    }
    if disk_provisioned_iops is not None:
        boot_initialize_params['provisioned_iops'] = int(
            disk_provisioned_iops
        )
    if disk_provisioned_throughput_mibps is not None:
        boot_initialize_params['provisioned_throughput'] = int(
            disk_provisioned_throughput_mibps
        )
    boot_disk = {
        'boot': True,
        'auto_delete': True,
        'type_': 'PERSISTENT',
        'mode': 'READ_WRITE',
        'device_name': boot_name,
        'initialize_params': boot_initialize_params,
    }
    if disk_interface:
        boot_disk['interface'] = disk_interface
    disks = [boot_disk]
    if data_disk_self_link:
        data_attachment = {
            'boot': False,
            'auto_delete': False,
            'type_': 'PERSISTENT',
            'mode': 'READ_WRITE',
            'source': data_disk_self_link,
            'device_name': data_device_name,
        }
        attachment_interface = data_disk_interface or disk_interface
        if attachment_interface:
            data_attachment['interface'] = attachment_interface
        disks.append(data_attachment)

    network_interface = {
        'network': network,
        'subnetwork': subnet,
        'stack_type': 'IPV4_ONLY',
        'access_configs': ([{
            'name': 'External NAT',
            'type_': 'ONE_TO_ONE_NAT',
            'network_tier': 'PREMIUM',
        }] if assign_public_ip else []),
    }
    if private_ip_address:
        # Proto-plus preserves Compute's historical ``networkIP`` field as
        # ``network_i_p``.  The dictionary-only provider fakes accept either
        # spelling, but the real NetworkInterface constructor rejects
        # ``network_ip`` as unknown.
        network_interface['network_i_p'] = private_ip_address
    family = _machine_family(_basename(machine_type))
    h3_machine = family == 'h3'
    requested_nic_type = network_interface_type or (
        'GVNIC' if family in EXPLICIT_GVNIC_FAMILIES else None
    )
    if requested_nic_type:
        # H3 and C4A require Google Virtual NIC. Rocky Linux 9's Google image
        # includes the gVNIC driver for both supported architectures.
        network_interface['nic_type'] = requested_nic_type

    tags = tuple(dict.fromkeys(
        str(tag)
        for tag in ((*network_tags, network_tag) if network_tag else network_tags)
        if str(tag)
    ))
    if not tags:
        raise ValueError('At least one GCP instance network tag is required.')

    return _message(clients, 'Instance', {
        'name': name,
        'description': _description(job_id, role),
        'machine_type': machine_type,
        'can_ip_forward': False,
        'deletion_protection': False,
        'labels': _labels(job_id, role),
        'tags': {'items': list(tags)},
        'metadata': {'items': metadata},
        'disks': disks,
        'network_interfaces': [network_interface],
        # Explicitly attach no service account so guest workloads cannot mint
        # Google Cloud access tokens from the metadata server.
        'service_accounts': [],
        'scheduling': {
            'automatic_restart': True,
            'on_host_maintenance': 'TERMINATE' if h3_machine else 'MIGRATE',
            'preemptible': False,
            'provisioning_model': 'STANDARD',
        },
        'shielded_instance_config': {
            'enable_secure_boot': False,
            'enable_vtpm': True,
            'enable_integrity_monitoring': True,
        },
    })


def _instance_ips(instance: Any) -> tuple[str | None, str | None]:
    interfaces = list(_value(instance, 'network_interfaces', ()) or ())
    if not interfaces:
        return None, None
    interface = interfaces[0]
    private_ip = _value(interface, 'network_i_p') or _value(interface, 'network_ip')
    access_configs = list(_value(interface, 'access_configs', ()) or ())
    public_ip = None
    if access_configs:
        public_ip = (
            _value(access_configs[0], 'nat_i_p')
            or _value(access_configs[0], 'nat_ip')
        )
    return (
        str(public_ip) if public_ip else None,
        str(private_ip) if private_ip else None,
    )


def _expected_disk_profile(
    resources: Mapping[str, Any], prefix: str
) -> tuple[str, int | None, int | None, int | None]:
    if prefix in {'gcp_data_disk', 'gcp_dsb_database_data_disk'}:
        key_stem = (
            'gcp_data_disk'
            if prefix == 'gcp_data_disk'
            else 'gcp_dsb_database_data_disk'
        )
        return (
            str(resources.get(f'{key_stem}_type') or DEFAULT_DISK_TYPE),
            resources.get(f'{key_stem}_size_gb'),
            resources.get(f'{key_stem}_provisioned_iops'),
            resources.get(f'{key_stem}_provisioned_throughput_mibps'),
        )
    if prefix == 'gcp_runner_boot_disk':
        fallback_size_gb = resources.get('gcp_boot_disk_size_gb')
    elif prefix == 'gcp_peer_boot_disk':
        fallback_size_gb = resources.get('gcp_peer_boot_disk_size_gb')
    elif prefix == 'gcp_loadgen_boot_disk':
        fallback_size_gb = resources.get('gcp_loadgen_boot_disk_size_gb')
    elif prefix in DISTRIBUTED_BOOT_PREFIXES.values():
        return (
            str(resources.get(f'{prefix}_type') or DEFAULT_DISK_TYPE),
            resources.get(f'{prefix}_size_gb'),
            resources.get(f'{prefix}_provisioned_iops'),
            resources.get(f'{prefix}_provisioned_throughput_mibps'),
        )
    else:  # pragma: no cover - internal invariant
        raise KeyError(prefix)
    return (
        str(
            resources.get(f'{prefix}_type')
            or (
                resources.get('gcp_loadgen_boot_disk_type')
                if prefix == 'gcp_loadgen_boot_disk'
                else resources.get('gcp_boot_disk_type')
            )
            or DEFAULT_DISK_TYPE
        ),
        resources.get(f'{prefix}_size_gb') or fallback_size_gb,
        (
            resources.get(f'{prefix}_provisioned_iops')
            if f'{prefix}_provisioned_iops' in resources
            else (
                None
                if prefix == 'gcp_loadgen_boot_disk'
                else resources.get('gcp_boot_disk_provisioned_iops')
            )
        ),
        (
            resources.get(f'{prefix}_provisioned_throughput_mibps')
            if f'{prefix}_provisioned_throughput_mibps' in resources
            else (
                None
                if prefix == 'gcp_loadgen_boot_disk'
                else resources.get(
                    'gcp_boot_disk_provisioned_throughput_mibps'
                )
            )
        ),
    )


def _verify_disk_profile(
    prefix: str,
    disk: Any,
    resources: Mapping[str, Any],
):
    disk_type, size_gb, iops, throughput = _expected_disk_profile(
        resources, prefix
    )
    label = prefix.removeprefix('gcp_').replace('_', ' ')
    if _basename(_value(disk, 'type_')) != disk_type:
        raise RuntimeError(
            f'Refusing GCP cleanup because the recorded {label} type changed. '
            'No resources were changed.'
        )
    comparisons = (
        ('size', size_gb, _value(disk, 'size_gb')),
        ('provisioned IOPS', iops, _value(disk, 'provisioned_iops')),
        (
            'provisioned throughput',
            throughput,
            _value(disk, 'provisioned_throughput'),
        ),
    )
    for field, expected, actual in comparisons:
        if expected is not None and int(actual or 0) != int(expected):
            raise RuntimeError(
                f'Refusing GCP cleanup because the recorded {label} {field} '
                'changed. No resources were changed.'
            )


def _expected_disk_link(
    prefix: str,
    resources: Mapping[str, Any],
) -> str:
    return _zonal_resource_link(
        str(resources.get('gcp_project_id') or ''),
        str(resources.get('gcp_zone') or resources.get('availability_zone') or ''),
        'disks',
        str(resources.get(f'{prefix}_name') or ''),
    )


def _verify_disk_relationship(
    prefix: str,
    disk: Any,
    resources: Mapping[str, Any],
):
    """Verify a disk's project/zone and immutable source-image relationship."""
    label = prefix.removeprefix('gcp_').replace('_', ' ')
    expected_self_link = _expected_disk_link(prefix, resources)
    if not _scoped_link_equal(_value(disk, 'self_link'), expected_self_link):
        raise RuntimeError(
            f'Refusing GCP cleanup because the recorded {label} belongs to a '
            'different project or zone. No resources were changed.'
        )
    compact_boot_prefixes = {
        'gcp_runner_boot_disk',
        'gcp_peer_boot_disk',
        'gcp_loadgen_boot_disk',
    }
    distributed_boot_prefixes = set(DISTRIBUTED_BOOT_PREFIXES.values())
    if prefix not in compact_boot_prefixes | distributed_boot_prefixes:
        return
    if prefix in distributed_boot_prefixes:
        role = next(
            key for key, value in DISTRIBUTED_BOOT_PREFIXES.items()
            if value == prefix
        )
        image_prefix = (
            'gcp_dsb_application_image'
            if role == 'application'
            else 'gcp_dsb_support_image'
        )
        expected_image_id = str(resources.get(f'{image_prefix}_id') or '')
        expected_image_link = str(
            resources.get(f'{image_prefix}_self_link') or ''
        )
    elif prefix == 'gcp_loadgen_boot_disk':
        expected_image_id = str(resources.get('gcp_loadgen_image_id') or '')
        expected_image_link = str(
            resources.get('gcp_loadgen_image_self_link') or ''
        )
    else:
        expected_image_id = str(resources.get('image_id') or '')
        expected_image_link = str(resources.get('gcp_image_self_link') or '')
    if expected_image_id:
        actual_image_id = str(_value(disk, 'source_image_id', '') or '')
        if actual_image_id != expected_image_id:
            raise RuntimeError(
                f'Refusing GCP cleanup because the recorded {label} source '
                'image ID differs from the pinned image contract. No '
                'resources were changed.'
            )
    actual_image_link = str(_value(disk, 'source_image', '') or '')
    if (
        expected_image_link
        and actual_image_link
        and not _scoped_link_equal(actual_image_link, expected_image_link)
    ):
        raise RuntimeError(
            f'Refusing GCP cleanup because the recorded {label} source image '
            'belongs to a different project or image. No resources were changed.'
        )


def _capture_boot_disk_identity(
    job: dict[str, Any],
    persist: PersistCallback | None,
    prefix: str,
    clients: Mapping[str, Any],
):
    resources = job['resources']
    disk = _bounded_cleanup_lookup(
        (prefix,),
        resources,
        clients,
    ).get(prefix)
    if disk is None:
        raise RuntimeError(
            f'GCP did not expose the exact {prefix.removeprefix("gcp_").replace("_", " ")} '
            'within the bounded reconciliation window. Its response-loss '
            'contract was retained and provisioning cannot continue until '
            'immutable disk identity is verified.'
        )
    _assert_saved_identity(prefix, disk, resources)
    _verify_disk_relationship(prefix, disk, resources)
    _required_labels(
        str(job['id']).lower(),
        _label_role(prefix) or '',
        disk,
        prefix.removeprefix('gcp_').replace('_', ' '),
    )
    _verify_disk_profile(prefix, disk, resources)
    _persist_recovered_identity(job, persist, prefix, disk)
    _record(
        job,
        persist,
        **{
            f'{prefix}_type': _basename(_value(disk, 'type_')),
            f'{prefix}_size_gb': int(_value(disk, 'size_gb', 0) or 0),
            f'{prefix}_provisioned_iops': (
                int(_value(disk, 'provisioned_iops', 0) or 0) or None
            ),
            f'{prefix}_provisioned_throughput_mibps': (
                int(_value(disk, 'provisioned_throughput', 0) or 0) or None
            ),
        },
    )
    return disk


def provision(
    job: dict[str, Any],
    plan: Any,
    *,
    public_key: str | None = None,
    emit: EventCallback | None = None,
    persist: PersistCallback | None = None,
    credentials=None,
    clients=None,
):
    """Create one isolated GCP benchmark stack and persist every exact name."""
    benchmarks = tuple(_value(plan, 'benchmarks', ()) or ())
    if 'deathstarbench' in benchmarks:
        topology_id, runtime_id = _deathstarbench_contract_ids(plan)
        require_released_runtime(topology_id, runtime_id)
    requested_project = _project_id(plan)
    project_id, _, clients = _runtime(requested_project or None, credentials, clients)
    region = _region(plan)
    machine_type = _machine_type_name(plan)
    if not machine_type:
        raise ValueError('Select a GCP machine type.')
    public_key = str(public_key or job.get('_public_key') or '').strip()
    if not public_key:
        raise ValueError('An SSH public key is required to launch GCP instances.')
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
    )
    # Validate key syntax before any cloud write even for direct provider use.
    rocky_linux.ssh_metadata_items(public_key, username=SSH_USER)
    additional_volume = bool(_storage_value(plan, 'additional_volume', False))
    boot_size_gb = int(_storage_value(plan, 'boot_size_gb', 100))
    additional_size_gb = int(_storage_value(plan, 'additional_size_gb', 1024))
    if not 50 <= boot_size_gb <= 32768:
        raise ValueError('GCP boot disk size must be between 50 and 32768 GiB.')
    if additional_volume and not 50 <= additional_size_gb <= 32768:
        raise ValueError(
            'GCP benchmark data disk size must be between 50 and 32768 GiB.'
        )
    fileio_selected = 'fio' in benchmarks or (
        'sysbench' in benchmarks
        and 'fileio' in set(_value(sysbench, 'workloads', ()) or ())
    )
    if fileio_selected and not additional_volume:
        raise ValueError(
            'GCP fio and Sysbench File I/O require the additional '
            'pd-balanced data disk.'
        )

    project = _call(
        clients['projects'], 'get', clients, 'GetProjectRequest', project=project_id
    )
    compute_project_id = str(_value(project, 'id', '') or '')
    if not compute_project_id:
        raise RuntimeError(f'Compute Engine did not return an ID for project {project_id}.')
    resources = job.setdefault('resources', {})
    requested_zone = str(
        resources.get('gcp_zone')
        or _value(plan, 'gcp_zone')
        or _value(plan, 'availability_zone')
        or _value(plan, 'availability_domain')
        or ''
    ).strip() or None
    zone, details = _resolve_zone_and_machine(
        project_id,
        region,
        machine_type,
        requested_zone,
        clients,
    )
    _validate_plan_capacity(plan, details)
    disk_type = str(details['disk_type'])
    disk_interface = details.get('disk_interface')
    network_interface_type = details.get('network_interface_type')
    disk_provisioned_iops = details.get('disk_provisioned_iops')
    disk_provisioned_throughput_mibps = details.get(
        'disk_provisioned_throughput_mibps'
    )
    image = _persisted_image_contract(
        resources,
        details['architecture'],
    ) or latest_rocky_linux_9_image(
        details['architecture'], clients=clients
    )
    loadgen_details = None
    loadgen_image = None
    if web_benchmarks:
        # Resolve capacity and image compatibility before the first cloud
        # write. A C4A target can therefore use an independent x86 N2 load
        # generator without inheriting the target's Hyperdisk/NVMe profile.
        loadgen_details = _loadgen_machine_details(project_id, zone, clients)
        loadgen_image = _persisted_image_contract(
            resources,
            LOADGEN_ARCHITECTURE,
            load_generator=True,
        )
        if loadgen_image is None:
            loadgen_image = (
                image
                if details['architecture'] == LOADGEN_ARCHITECTURE
                else latest_rocky_linux_9_image(
                    LOADGEN_ARCHITECTURE,
                    clients=clients,
                )
            )

    job_id = str(job.get('id') or '').lower()
    prefix = _resource_name(job_id)
    runner_name = _resource_name(job_id, 'runner')
    peer_name = _resource_name(job_id, 'iperf-peer')
    loadgen_name = _resource_name(job_id, 'loadgen')
    data_disk_name = _resource_name(job_id, 'data')
    data_device_name = data_disk_name
    runner_tag = _resource_name(job_id, 'runner')
    peer_tag = _resource_name(job_id, 'iperf-peer')
    loadgen_tag = _resource_name(job_id, 'loadgen')

    # This ownership anchor is durable before the first cloud create. It makes
    # an interrupted response-lost run discoverable by the local history UI.
    _record(
        job,
        persist,
        provider='gcp',
        gcp_project_id=project_id,
        gcp_compute_project_id=compute_project_id,
        gcp_resource_prefix=prefix,
        region=region,
        gcp_zone=zone,
        availability_zone=zone,
        gcp_machine_type=machine_type,
        instance_type=machine_type,
        architecture=details['architecture'],
        ocpus=details['vcpu'],
        memory_gb=details['memory_gb'],
        image_id=image['image_id'],
        image_name=image['name'],
        gcp_image_family=image['family'],
        gcp_image_self_link=image['self_link'],
        ssh_user=SSH_USER,
        gcp_iperf3_protocols=list(protocols),
        gcp_web_benchmarks=list(web_benchmarks),
        gcp_runner_tag=runner_tag,
        gcp_peer_tag=peer_tag if protocols else None,
        gcp_loadgen_tag=loadgen_tag if web_benchmarks else None,
        gcp_boot_disk_type=disk_type,
        gcp_boot_disk_size_gb=boot_size_gb,
        gcp_peer_boot_disk_size_gb=PEER_BOOT_SIZE_GB if protocols else None,
        gcp_boot_disk_provisioned_iops=disk_provisioned_iops,
        gcp_boot_disk_provisioned_throughput_mibps=(
            disk_provisioned_throughput_mibps
        ),
        gcp_disk_interface=disk_interface,
        gcp_network_interface_type=network_interface_type,
        gcp_loadgen_machine_type=(
            LOADGEN_MACHINE_TYPE if web_benchmarks else None
        ),
        gcp_loadgen_boot_disk_type=(
            LOADGEN_DISK_TYPE if web_benchmarks else None
        ),
        gcp_loadgen_boot_disk_size_gb=(
            LOADGEN_BOOT_SIZE_GB if web_benchmarks else None
        ),
        loadgen_shape=LOADGEN_MACHINE_TYPE if web_benchmarks else None,
        loadgen_vcpus=LOADGEN_VCPUS if web_benchmarks else None,
        loadgen_memory_gb=LOADGEN_MEMORY_GB if web_benchmarks else None,
        loadgen_architecture=(
            LOADGEN_ARCHITECTURE if web_benchmarks else None
        ),
        loadgen_image_id=(
            loadgen_image['image_id'] if loadgen_image else None
        ),
        loadgen_image_name=(loadgen_image['name'] if loadgen_image else None),
        loadgen_network_bandwidth_gbps=(
            LOADGEN_NETWORK_BANDWIDTH_GBPS if web_benchmarks else None
        ),
        loadgen_network_capacity_kind=(
            'maximum' if web_benchmarks else None
        ),
        loadgen_network_peak_bandwidth_gbps=(
            LOADGEN_NETWORK_BANDWIDTH_GBPS if web_benchmarks else None
        ),
        gcp_loadgen_image_id=(
            loadgen_image['image_id'] if loadgen_image else None
        ),
        gcp_loadgen_image_name=(loadgen_image['name'] if loadgen_image else None),
        gcp_loadgen_image_self_link=(
            loadgen_image['self_link'] if loadgen_image else None
        ),
        gcp_loadgen_image_family=(
            loadgen_image['family'] if loadgen_image else None
        ),
        gcp_loadgen_network_bandwidth_gbps=(
            LOADGEN_NETWORK_BANDWIDTH_GBPS if web_benchmarks else None
        ),
    )

    network_name = prefix
    network_body = _network_resource(clients, job_id, network_name)
    network = _create_named_resource(
        job,
        persist,
        prefix='gcp_network',
        name=network_name,
        client=clients['networks'],
        clients=clients,
        insert_request_type='InsertNetworkRequest',
        insert_values={'project': project_id, 'network_resource': network_body},
        get_request_type='GetNetworkRequest',
        get_values={'project': project_id, 'network': network_name},
    )
    network_link = str(_value(network, 'self_link', '') or '')
    _emit(job, emit, 'Provision', f'Created GCP VPC {network_name}.')

    subnet_name = prefix
    subnet_body = _subnet_resource(
        clients, job_id, subnet_name, network_link
    )
    subnet = _create_named_resource(
        job,
        persist,
        prefix='gcp_subnet',
        name=subnet_name,
        client=clients['subnetworks'],
        clients=clients,
        insert_request_type='InsertSubnetworkRequest',
        insert_values={
            'project': project_id,
            'region': region,
            'subnetwork_resource': subnet_body,
        },
        get_request_type='GetSubnetworkRequest',
        get_values={'project': project_id, 'region': region, 'subnetwork': subnet_name},
    )
    subnet_link = str(_value(subnet, 'self_link', '') or '')
    _emit(job, emit, 'Provision', f'Created GCP subnet {subnet_name} in {region}.')

    ssh_firewall_name = _resource_name(job_id, 'ssh')
    ssh_firewall = _firewall_resource(
        clients,
        job_id,
        ssh_firewall_name,
        network_link,
        role='ssh-firewall',
        target_tags=(
            (runner_tag, loadgen_tag)
            if web_benchmarks
            else (runner_tag,)
        ),
        source_ranges=(SSH_SOURCE_CIDR,),
    )
    _create_named_resource(
        job,
        persist,
        prefix='gcp_ssh_firewall',
        name=ssh_firewall_name,
        client=clients['firewalls'],
        clients=clients,
        insert_request_type='InsertFirewallRequest',
        insert_values={'project': project_id, 'firewall_resource': ssh_firewall},
        get_request_type='GetFirewallRequest',
        get_values={'project': project_id, 'firewall': ssh_firewall_name},
    )

    if protocols:
        iperf_firewall_name = _resource_name(job_id, 'iperf')
        iperf_firewall = _firewall_resource(
            clients,
            job_id,
            iperf_firewall_name,
            network_link,
            role='iperf-firewall',
            target_tag=peer_tag,
            source_tags=(runner_tag,),
            protocols=protocols,
        )
        _create_named_resource(
            job,
            persist,
            prefix='gcp_iperf_firewall',
            name=iperf_firewall_name,
            client=clients['firewalls'],
            clients=clients,
            insert_request_type='InsertFirewallRequest',
            insert_values={'project': project_id, 'firewall_resource': iperf_firewall},
            get_request_type='GetFirewallRequest',
            get_values={'project': project_id, 'firewall': iperf_firewall_name},
        )

    if web_benchmarks:
        web_firewall_name = _resource_name(job_id, 'web')
        web_firewall = _firewall_resource(
            clients,
            job_id,
            web_firewall_name,
            network_link,
            role='web-firewall',
            target_tag=runner_tag,
            source_tags=(loadgen_tag,),
            web_benchmarks=web_benchmarks,
        )
        _create_named_resource(
            job,
            persist,
            prefix='gcp_web_firewall',
            name=web_firewall_name,
            client=clients['firewalls'],
            clients=clients,
            insert_request_type='InsertFirewallRequest',
            insert_values={
                'project': project_id,
                'firewall_resource': web_firewall,
            },
            get_request_type='GetFirewallRequest',
            get_values={'project': project_id, 'firewall': web_firewall_name},
        )

    data_disk = None
    if additional_volume:
        size_gb = additional_size_gb
        disk_body = _data_disk_resource(
            clients,
            job_id,
            data_disk_name,
            project_id,
            zone,
            size_gb,
            disk_type=disk_type,
            provisioned_iops=disk_provisioned_iops,
            provisioned_throughput_mibps=(
                disk_provisioned_throughput_mibps
            ),
        )
        _record(
            job,
            persist,
            gcp_data_disk_type=disk_type,
            gcp_data_disk_size_gb=size_gb,
            gcp_data_device_name=data_device_name,
            gcp_data_disk_provisioned_iops=disk_provisioned_iops,
            gcp_data_disk_provisioned_throughput_mibps=(
                disk_provisioned_throughput_mibps
            ),
        )
        data_disk = _create_named_resource(
            job,
            persist,
            prefix='gcp_data_disk',
            name=data_disk_name,
            client=clients['disks'],
            clients=clients,
            insert_request_type='InsertDiskRequest',
            insert_values={
                'project': project_id,
                'zone': zone,
                'disk_resource': disk_body,
            },
            get_request_type='GetDiskRequest',
            get_values={'project': project_id, 'zone': zone, 'disk': data_disk_name},
        )
        _emit(
            job,
            emit,
            'Provision',
            f'Created encrypted GCP {disk_type} data disk '
            f'{data_disk_name} ({size_gb} GiB).',
        )

    runner_body = _instance_resource(
        clients,
        job_id=job_id,
        name=runner_name,
        role='runner',
        machine_type=details['self_link'] or (
            f'projects/{project_id}/zones/{zone}/machineTypes/{machine_type}'
        ),
        image=image,
        project_id=project_id,
        zone=zone,
        network=network_link,
        subnet=subnet_link,
        network_tag=runner_tag,
        boot_size_gb=boot_size_gb,
        disk_type=disk_type,
        disk_interface=disk_interface,
        network_interface_type=network_interface_type,
        disk_provisioned_iops=disk_provisioned_iops,
        disk_provisioned_throughput_mibps=(
            disk_provisioned_throughput_mibps
        ),
        public_key=public_key,
        data_disk_self_link=(
            str(_value(data_disk, 'self_link', '') or '') if data_disk else None
        ),
        data_device_name=data_device_name if data_disk else None,
    )
    runner_had_contract = any(
        key in resources
        for key in (
            'gcp_instance_name',
            'gcp_instance_id',
            'gcp_instance_self_link',
            'gcp_instance_request_id',
            'gcp_instance_create_ambiguous',
        )
    )
    runner_request_id = str(
        resources.get('gcp_instance_request_id') or uuid.uuid4()
    )
    _record(
        job,
        persist,
        gcp_runner_boot_disk_name=f'{runner_name}-boot',
        # The boot disk is created atomically by the instance insert and must
        # retain the parent's UUID after the parent contract is cleared.
        gcp_runner_boot_disk_request_id=runner_request_id,
        gcp_runner_boot_disk_create_ambiguous=True,
        gcp_instance_name=runner_name,
        gcp_instance_request_id=runner_request_id,
        gcp_instance_create_ambiguous=True,
    )
    try:
        runner = _create_named_resource(
            job,
            persist,
            prefix='gcp_instance',
            name=runner_name,
            client=clients['instances'],
            clients=clients,
            insert_request_type='InsertInstanceRequest',
            insert_values={
                'project': project_id,
                'zone': zone,
                'instance_resource': runner_body,
            },
            get_request_type='GetInstanceRequest',
            get_values={
                'project': project_id,
                'zone': zone,
                'instance': runner_name,
            },
            clear_contract_on_confirmed_rejection=not runner_had_contract,
        )
    except Exception:
        if not resources.get('gcp_instance_name'):
            _forget(
                job,
                persist,
                *_contract_keys('gcp_runner_boot_disk'),
            )
        raise
    runner_id = str(_value(runner, 'id', '') or '')
    public_ip, private_ip = _instance_ips(runner)
    _record(
        job,
        persist,
        instance_id=runner_id,
        public_ip=public_ip,
        private_ip=private_ip,
    )
    _capture_boot_disk_identity(
        job, persist, 'gcp_runner_boot_disk', clients
    )
    if not public_ip or not private_ip:
        raise RuntimeError(
            f'GCP runner {runner_name} became available without both public '
            'and private IPv4 addresses.'
        )
    _emit(
        job,
        emit,
        'Provision',
        f'Launched GCP {machine_type} runner in {zone}; public IP {public_ip}.',
    )

    if web_benchmarks:
        assert loadgen_details is not None  # validated before cloud writes
        assert loadgen_image is not None
        loadgen_body = _instance_resource(
            clients,
            job_id=job_id,
            name=loadgen_name,
            role='load-generator',
            machine_type=loadgen_details['self_link'] or (
                f'projects/{project_id}/zones/{zone}/machineTypes/'
                f'{LOADGEN_MACHINE_TYPE}'
            ),
            image=loadgen_image,
            project_id=project_id,
            zone=zone,
            network=network_link,
            subnet=subnet_link,
            network_tag=loadgen_tag,
            boot_size_gb=LOADGEN_BOOT_SIZE_GB,
            disk_type=LOADGEN_DISK_TYPE,
            public_key=public_key,
        )
        loadgen_had_contract = any(
            key in resources
            for key in (
                'gcp_loadgen_instance_name',
                'gcp_loadgen_instance_id',
                'gcp_loadgen_instance_self_link',
                'gcp_loadgen_instance_request_id',
                'gcp_loadgen_instance_create_ambiguous',
            )
        )
        loadgen_request_id = str(
            resources.get('gcp_loadgen_instance_request_id') or uuid.uuid4()
        )
        _record(
            job,
            persist,
            gcp_loadgen_boot_disk_name=f'{loadgen_name}-boot',
            # The boot disk is created atomically by the instance insert, so
            # its response-loss identity is the same idempotency UUID.
            gcp_loadgen_boot_disk_request_id=loadgen_request_id,
            gcp_loadgen_boot_disk_create_ambiguous=True,
            gcp_loadgen_instance_name=loadgen_name,
            gcp_loadgen_instance_request_id=loadgen_request_id,
            gcp_loadgen_instance_create_ambiguous=True,
        )
        try:
            loadgen = _create_named_resource(
                job,
                persist,
                prefix='gcp_loadgen_instance',
                name=loadgen_name,
                client=clients['instances'],
                clients=clients,
                insert_request_type='InsertInstanceRequest',
                insert_values={
                    'project': project_id,
                    'zone': zone,
                    'instance_resource': loadgen_body,
                },
                get_request_type='GetInstanceRequest',
                get_values={
                    'project': project_id,
                    'zone': zone,
                    'instance': loadgen_name,
                },
                clear_contract_on_confirmed_rejection=(
                    not loadgen_had_contract
                ),
            )
        except Exception:
            if not resources.get('gcp_loadgen_instance_name'):
                _forget(
                    job,
                    persist,
                    *_contract_keys('gcp_loadgen_boot_disk'),
                )
            raise
        loadgen_id = str(_value(loadgen, 'id', '') or '')
        loadgen_public_ip, loadgen_private_ip = _instance_ips(loadgen)
        _record(
            job,
            persist,
            loadgen_instance_id=loadgen_id,
            loadgen_public_ip=loadgen_public_ip,
            loadgen_private_ip=loadgen_private_ip,
            loadgen_shape=LOADGEN_MACHINE_TYPE,
            loadgen_vcpus=LOADGEN_VCPUS,
            loadgen_memory_gb=LOADGEN_MEMORY_GB,
            loadgen_architecture=LOADGEN_ARCHITECTURE,
            loadgen_image_id=loadgen_image['image_id'],
            loadgen_image_name=loadgen_image['name'],
            loadgen_network_bandwidth_gbps=(
                LOADGEN_NETWORK_BANDWIDTH_GBPS
            ),
            loadgen_network_capacity_kind='maximum',
            loadgen_network_peak_bandwidth_gbps=(
                LOADGEN_NETWORK_BANDWIDTH_GBPS
            ),
            gcp_loadgen_image_id=loadgen_image['image_id'],
            gcp_loadgen_image_name=loadgen_image['name'],
            gcp_loadgen_image_self_link=loadgen_image['self_link'],
            gcp_loadgen_network_bandwidth_gbps=(
                LOADGEN_NETWORK_BANDWIDTH_GBPS
            ),
        )
        _capture_boot_disk_identity(
            job,
            persist,
            'gcp_loadgen_boot_disk',
            clients,
        )
        if not loadgen_public_ip or not loadgen_private_ip:
            raise RuntimeError(
                f'GCP load generator {loadgen_name} became available '
                'without both public and private IPv4 addresses.'
            )
        _emit(
            job,
            emit,
            'Provision',
            f'Launched GCP {LOADGEN_MACHINE_TYPE} web load generator in '
            f'{zone}; public IP {loadgen_public_ip}.',
        )

    if protocols:
        peer_body = _instance_resource(
            clients,
            job_id=job_id,
            name=peer_name,
            role='iperf-peer',
            machine_type=details['self_link'] or (
                f'projects/{project_id}/zones/{zone}/machineTypes/{machine_type}'
            ),
            image=image,
            project_id=project_id,
            zone=zone,
            network=network_link,
            subnet=subnet_link,
            network_tag=peer_tag,
            boot_size_gb=PEER_BOOT_SIZE_GB,
            disk_type=disk_type,
            disk_interface=disk_interface,
            network_interface_type=network_interface_type,
            disk_provisioned_iops=disk_provisioned_iops,
            disk_provisioned_throughput_mibps=(
                disk_provisioned_throughput_mibps
            ),
            iperf3_protocols=protocols,
        )
        peer_had_contract = any(
            key in resources
            for key in (
                'gcp_peer_instance_name',
                'gcp_peer_instance_id',
                'gcp_peer_instance_self_link',
                'gcp_peer_instance_request_id',
                'gcp_peer_instance_create_ambiguous',
            )
        )
        peer_request_id = str(
            resources.get('gcp_peer_instance_request_id') or uuid.uuid4()
        )
        _record(
            job,
            persist,
            gcp_peer_boot_disk_name=f'{peer_name}-boot',
            gcp_peer_boot_disk_request_id=peer_request_id,
            gcp_peer_boot_disk_create_ambiguous=True,
            gcp_peer_instance_name=peer_name,
            gcp_peer_instance_request_id=peer_request_id,
            gcp_peer_instance_create_ambiguous=True,
        )
        try:
            peer = _create_named_resource(
                job,
                persist,
                prefix='gcp_peer_instance',
                name=peer_name,
                client=clients['instances'],
                clients=clients,
                insert_request_type='InsertInstanceRequest',
                insert_values={
                    'project': project_id,
                    'zone': zone,
                    'instance_resource': peer_body,
                },
                get_request_type='GetInstanceRequest',
                get_values={
                    'project': project_id,
                    'zone': zone,
                    'instance': peer_name,
                },
                clear_contract_on_confirmed_rejection=not peer_had_contract,
            )
        except Exception:
            if not resources.get('gcp_peer_instance_name'):
                _forget(
                    job,
                    persist,
                    *_contract_keys('gcp_peer_boot_disk'),
                )
            raise
        _, peer_private_ip = _instance_ips(peer)
        _record(
            job,
            persist,
            gcp_peer_machine_type=machine_type,
            gcp_peer_image_id=image['image_id'],
            peer_private_ip=peer_private_ip,
        )
        _capture_boot_disk_identity(
            job, persist, 'gcp_peer_boot_disk', clients
        )
        if not peer_private_ip:
            raise RuntimeError(
                f'GCP iperf3 peer {peer_name} became available without a private IPv4 address.'
            )
        _emit(
            job,
            emit,
            'Provision',
            f'Launched same-type GCP iperf3 peer in {zone}; benchmark '
            f'traffic targets {peer_private_ip}.',
        )

    return resources


def provision_distributed_deathstarbench_candidate(
    job: dict[str, Any],
    plan: Any,
    *,
    public_key: str | None = None,
    emit: EventCallback | None = None,
    persist: PersistCallback | None = None,
    credentials=None,
    clients=None,
):
    """Create the unreleased five-node GCP DeathStarBench candidate.

    The ordinary provider dispatcher never calls this entry point while the
    distributed runtime profile is unreleased.  It exists for explicit,
    operator-driven qualification of the same K3s workload used on Azure.
    """

    plan_provider = str(_value(plan, 'provider', '') or '').strip().casefold()
    if plan_provider and plan_provider != 'gcp':
        raise ValueError('The GCP distributed candidate requires a GCP plan.')
    if tuple(_value(plan, 'benchmarks', ()) or ()) != ('deathstarbench',):
        raise ValueError(
            'The GCP distributed candidate requires DeathStarBench as its '
            'only selected benchmark.'
        )
    deathstarbench = _value(plan, 'deathstarbench', {}) or {}
    if _value(deathstarbench, 'workload', None) != 'social_network':
        raise ValueError(
            'The GCP distributed candidate currently requires the exact '
            'DeathStarBench Social Network workload.'
        )
    topology_id, runtime_id = _deathstarbench_contract_ids(plan)
    if (
        topology_id != DISTRIBUTED_TIERED_TOPOLOGY_ID
        or runtime_id != K3S_RUNTIME_ID
    ):
        raise ValueError(
            'The GCP distributed candidate requires the exact '
            'distributed_tiered_v1/k3s_v1 contract.'
        )
    resources = job.setdefault('resources', {})
    saved_provider = str(resources.get('provider') or '').strip().casefold()
    if (
        (saved_provider and saved_provider != 'gcp')
        or (resources and not saved_provider)
    ):
        raise ResourceInventoryError(
            'The GCP distributed candidate refuses pre-existing resource '
            'state without an exact GCP provider identity.'
        )
    requested_project = _project_id(plan)
    project_id, _, clients = _runtime(
        requested_project or None, credentials, clients
    )
    if 'routers' not in clients:
        raise RuntimeError(
            'The GCP distributed candidate requires the Compute Routers API '
            'client for private cluster egress.'
        )
    region = _region(plan)
    application_machine = _machine_type_name(plan)
    if not application_machine:
        raise ValueError(
            'Select a GCP machine type for the distributed application role.'
        )
    public_key = str(public_key or job.get('_public_key') or '').strip()
    if not public_key:
        raise ValueError('An SSH public key is required to launch GCP instances.')
    rocky_linux.ssh_metadata_items(public_key, username=SSH_USER)

    project = _call(
        clients['projects'],
        'get',
        clients,
        'GetProjectRequest',
        project=project_id,
    )
    compute_project_id = str(_value(project, 'id', '') or '')
    if not compute_project_id:
        raise RuntimeError(
            f'Compute Engine did not return an ID for project {project_id}.'
        )
    requested_zone = str(
        resources.get('gcp_zone')
        or _value(plan, 'gcp_zone')
        or _value(plan, 'availability_zone')
        or _value(plan, 'availability_domain')
        or ''
    ).strip() or None
    zone, application_details = _resolve_zone_and_machine(
        project_id,
        region,
        application_machine,
        requested_zone,
        clients,
    )
    _validate_plan_capacity(plan, application_details)
    manifest = build_topology_manifest(
        topology_id,
        runtime_id,
        selected_shape=application_machine,
        selected_architecture=application_details['architecture'],
    )
    role_details = _distributed_role_details(
        manifest,
        project_id=project_id,
        zone=zone,
        application_details=application_details,
        clients=clients,
    )
    application_image = _distributed_candidate_image(
        resources,
        key_prefix='gcp_dsb_application_image',
        architecture=application_details['architecture'],
        clients=clients,
    )
    support_image = _distributed_candidate_image(
        resources,
        key_prefix='gcp_dsb_support_image',
        architecture='x86_64',
        clients=clients,
    )

    job_id = str(job.get('id') or '').lower()
    base_name = _resource_name(job_id, 'dsb')
    network_name = base_name
    cluster_subnet_name = _resource_name(job_id, 'dsb-cluster')
    loadgen_subnet_name = _resource_name(job_id, 'dsb-loadgen')
    router_name = _resource_name(job_id, 'dsb-router')
    node_names = {
        role: _resource_name(job_id, f'dsb-{role}')
        for role in DISTRIBUTED_PRIVATE_ADDRESSES
    }
    role_tags = dict(node_names)
    database_disk_name = _resource_name(job_id, 'dsb-database-data')
    database_disk_device = (
        f'/dev/disk/by-id/google-{database_disk_name}'
    )
    firewall_contracts = _distributed_firewall_specs(
        manifest,
        job_id=job_id,
        network=(
            f'projects/{project_id}/global/networks/{network_name}'
        ),
        tags=role_tags,
        clients=clients,
    )
    expected_names = {
        'gcp_dsb_network': network_name,
        'gcp_dsb_cluster_subnet': cluster_subnet_name,
        'gcp_dsb_loadgen_subnet': loadgen_subnet_name,
        'gcp_dsb_router': router_name,
        'gcp_dsb_database_data_disk': database_disk_name,
        **{
            prefix: name
            for prefix, name, _ in firewall_contracts
        },
    }
    for role, name in node_names.items():
        expected_names[DISTRIBUTED_NODE_PREFIXES[role]] = name
        expected_names[DISTRIBUTED_BOOT_PREFIXES[role]] = f'{name}-boot'
    expected_inventory = _planned_gcp_inventory(
        manifest,
        zone=zone,
        names=node_names,
        details_by_key=role_details,
        database_disk_name=database_disk_name,
        database_disk_device=database_disk_device,
    )

    def image_values(prefix: str, image: Mapping[str, Any]) -> dict[str, Any]:
        return {
            f'{prefix}_id': image['image_id'],
            f'{prefix}_name': image['name'],
            f'{prefix}_self_link': image['self_link'],
            f'{prefix}_family': image['family'],
            f'{prefix}_architecture': image['architecture'],
        }

    contract_values: dict[str, Any] = {
        'provider': 'gcp',
        'gcp_distributed_candidate': True,
        'gcp_project_id': project_id,
        'gcp_compute_project_id': compute_project_id,
        'gcp_resource_prefix': base_name,
        'gcp_dsb_expected_names': expected_names,
        'region': region,
        'gcp_zone': zone,
        'availability_zone': zone,
        'gcp_machine_type': application_machine,
        'instance_type': application_machine,
        'architecture': application_details['architecture'],
        'ocpus': application_details['vcpu'],
        'memory_gb': application_details['memory_gb'],
        'image_id': application_image['image_id'],
        'image_name': application_image['name'],
        'gcp_image_family': application_image['family'],
        'gcp_image_self_link': application_image['self_link'],
        'ssh_user': SSH_USER,
        'gcp_web_benchmarks': ['deathstarbench'],
        'gcp_dsb_network_cidr': DISTRIBUTED_NETWORK_CIDR,
        'gcp_dsb_cluster_subnet_cidr': DISTRIBUTED_CLUSTER_SUBNET_CIDR,
        'gcp_dsb_loadgen_subnet_cidr': DISTRIBUTED_LOADGEN_SUBNET_CIDR,
        'gcp_dsb_database_disk_device': database_disk_device,
        'gcp_dsb_database_data_disk_type': (
            DISTRIBUTED_DATABASE_DISK_TYPE
        ),
        'gcp_dsb_database_data_disk_size_gb': int(
            next(
                node for node in manifest.nodes if node.key == 'database'
            ).storage[0].size_gib
        ),
        # pd-ssd performance is baseline, not provisioned. The topology and
        # nested inventory retain the required 3000 IOPS / 125 MiB/s minima.
        'gcp_dsb_database_data_disk_provisioned_iops': None,
        'gcp_dsb_database_data_disk_provisioned_throughput_mibps': None,
        **image_values('gcp_dsb_application_image', application_image),
        **image_values('gcp_dsb_support_image', support_image),
    }
    for role, details in role_details.items():
        normalized = role.replace('-', '_')
        contract_values.update({
            f'gcp_dsb_{normalized}_machine_type': details['machine_type'],
            f'gcp_dsb_{normalized}_architecture': details['architecture'],
            f'gcp_dsb_{normalized}_private_ip': (
                DISTRIBUTED_PRIVATE_ADDRESSES[role]
            ),
            f'gcp_dsb_{normalized}_tag': role_tags[role],
        })
    _initialize_distributed_candidate_contract(
        job,
        persist,
        manifest,
        expected_inventory,
        contract_values,
    )

    network = _create_named_resource(
        job,
        persist,
        prefix='gcp_dsb_network',
        name=network_name,
        client=clients['networks'],
        clients=clients,
        insert_request_type='InsertNetworkRequest',
        insert_values={
            'project': project_id,
            'network_resource': _network_resource(
                clients,
                job_id,
                network_name,
                role='distributed-network',
            ),
        },
        get_request_type='GetNetworkRequest',
        get_values={'project': project_id, 'network': network_name},
    )
    network_link = str(_value(network, 'self_link', '') or '')
    _emit(job, emit, 'Provision', f'Created GCP distributed VPC {network_name}.')

    subnet_specs = (
        (
            'gcp_dsb_cluster_subnet',
            cluster_subnet_name,
            'distributed-cluster-subnet',
            DISTRIBUTED_CLUSTER_SUBNET_CIDR,
        ),
        (
            'gcp_dsb_loadgen_subnet',
            loadgen_subnet_name,
            'distributed-loadgen-subnet',
            DISTRIBUTED_LOADGEN_SUBNET_CIDR,
        ),
    )
    subnet_links: dict[str, str] = {}
    for prefix, name, role, cidr in subnet_specs:
        subnet = _create_named_resource(
            job,
            persist,
            prefix=prefix,
            name=name,
            client=clients['subnetworks'],
            clients=clients,
            insert_request_type='InsertSubnetworkRequest',
            insert_values={
                'project': project_id,
                'region': region,
                'subnetwork_resource': _subnet_resource(
                    clients,
                    job_id,
                    name,
                    network_link,
                    role=role,
                    cidr=cidr,
                ),
            },
            get_request_type='GetSubnetworkRequest',
            get_values={
                'project': project_id,
                'region': region,
                'subnetwork': name,
            },
        )
        subnet_links[prefix] = str(_value(subnet, 'self_link', '') or '')

    _ensure_distributed_router(
        job,
        persist,
        clients=clients,
        project_id=project_id,
        region=region,
        name=router_name,
        network=network_link,
        cluster_subnet=subnet_links['gcp_dsb_cluster_subnet'],
    )

    # Rebuild bodies with the authoritative returned network link before each
    # insert; the deterministic expected-name version above was contract-only.
    firewall_contracts = _distributed_firewall_specs(
        manifest,
        job_id=job_id,
        network=network_link,
        tags=role_tags,
        clients=clients,
    )
    for prefix, name, body in firewall_contracts:
        _create_named_resource(
            job,
            persist,
            prefix=prefix,
            name=name,
            client=clients['firewalls'],
            clients=clients,
            insert_request_type='InsertFirewallRequest',
            insert_values={
                'project': project_id,
                'firewall_resource': body,
            },
            get_request_type='GetFirewallRequest',
            get_values={'project': project_id, 'firewall': name},
        )

    database_storage = next(
        node for node in manifest.nodes if node.key == 'database'
    ).storage[0]
    _set_inventory_storage_status(
        job,
        persist,
        'database',
        database_storage.key,
        'creating',
    )
    try:
        database_disk = _create_named_resource(
            job,
            persist,
            prefix='gcp_dsb_database_data_disk',
            name=database_disk_name,
            client=clients['disks'],
            clients=clients,
            insert_request_type='InsertDiskRequest',
            insert_values={
                'project': project_id,
                'zone': zone,
                'disk_resource': _data_disk_resource(
                    clients,
                    job_id,
                    database_disk_name,
                    project_id,
                    zone,
                    int(database_storage.size_gib),
                    role='database-data',
                    disk_type=DISTRIBUTED_DATABASE_DISK_TYPE,
                ),
            },
            get_request_type='GetDiskRequest',
            get_values={
                'project': project_id,
                'zone': zone,
                'disk': database_disk_name,
            },
        )
    except Exception:
        _set_inventory_storage_status(
            job,
            persist,
            'database',
            database_storage.key,
            (
                'create_ambiguous'
                if resources.get('gcp_dsb_database_data_disk_name')
                else 'failed'
            ),
        )
        raise
    database_disk_id = str(_value(database_disk, 'id', '') or '')
    _set_inventory_storage_status(
        job,
        persist,
        'database',
        database_storage.key,
        'running',
        provider_resource_id=database_disk_id,
    )

    for node_key in manifest.creation_order:
        details = role_details[node_key]
        name = node_names[node_key]
        prefix = DISTRIBUTED_NODE_PREFIXES[node_key]
        boot_prefix = DISTRIBUTED_BOOT_PREFIXES[node_key]
        image = application_image if node_key == 'application' else support_image
        subnet_prefix = (
            'gcp_dsb_loadgen_subnet'
            if node_key == 'load-generator'
            else 'gcp_dsb_cluster_subnet'
        )
        body = _instance_resource(
            clients,
            job_id=job_id,
            name=name,
            role=node_key,
            machine_type=details['self_link'] or (
                f'projects/{project_id}/zones/{zone}/machineTypes/'
                f'{details["machine_type"]}'
            ),
            image=image,
            project_id=project_id,
            zone=zone,
            network=network_link,
            subnet=subnet_links[subnet_prefix],
            network_tags=(role_tags[node_key],),
            boot_size_gb=DISTRIBUTED_OS_DISK_SIZE_GB,
            disk_type=details['disk_type'],
            disk_interface=details.get('disk_interface'),
            network_interface_type=details.get('network_interface_type'),
            disk_provisioned_iops=details.get('disk_provisioned_iops'),
            disk_provisioned_throughput_mibps=details.get(
                'disk_provisioned_throughput_mibps'
            ),
            public_key=public_key,
            data_disk_self_link=(
                str(_value(database_disk, 'self_link', '') or '')
                if node_key == 'database'
                else None
            ),
            data_device_name=(
                database_disk_name if node_key == 'database' else None
            ),
            data_disk_interface=(
                DISTRIBUTED_DATABASE_DISK_INTERFACE
                if node_key == 'database'
                else None
            ),
            private_ip_address=DISTRIBUTED_PRIVATE_ADDRESSES[node_key],
            assign_public_ip=node_key in DISTRIBUTED_PUBLIC_IP_NODES,
        )
        instance_had_contract = any(
            key in resources
            for key in (
                f'{prefix}_name',
                f'{prefix}_id',
                f'{prefix}_self_link',
                f'{prefix}_request_id',
                f'{prefix}_create_ambiguous',
            )
        )
        request_id = str(
            resources.get(f'{prefix}_request_id') or uuid.uuid4()
        )
        _record(
            job,
            persist,
            **{
                f'{boot_prefix}_name': f'{name}-boot',
                f'{boot_prefix}_request_id': request_id,
                f'{boot_prefix}_create_ambiguous': True,
                f'{boot_prefix}_type': details['disk_type'],
                f'{boot_prefix}_size_gb': DISTRIBUTED_OS_DISK_SIZE_GB,
                f'{boot_prefix}_provisioned_iops': details.get(
                    'disk_provisioned_iops'
                ),
                f'{boot_prefix}_provisioned_throughput_mibps': details.get(
                    'disk_provisioned_throughput_mibps'
                ),
                f'{prefix}_name': name,
                f'{prefix}_request_id': request_id,
                f'{prefix}_create_ambiguous': True,
            },
        )
        _set_inventory_node_status(
            job, persist, node_key, 'creating'
        )
        try:
            instance = _create_named_resource(
                job,
                persist,
                prefix=prefix,
                name=name,
                client=clients['instances'],
                clients=clients,
                insert_request_type='InsertInstanceRequest',
                insert_values={
                    'project': project_id,
                    'zone': zone,
                    'instance_resource': body,
                },
                get_request_type='GetInstanceRequest',
                get_values={
                    'project': project_id,
                    'zone': zone,
                    'instance': name,
                },
                clear_contract_on_confirmed_rejection=(
                    not instance_had_contract
                ),
            )
        except Exception:
            if not resources.get(f'{prefix}_name'):
                _forget(job, persist, *_contract_keys(boot_prefix))
            _set_inventory_node_status(
                job,
                persist,
                node_key,
                (
                    'failed'
                    if not resources.get(f'{prefix}_name')
                    else 'create_ambiguous'
                ),
            )
            raise
        instance_id = str(_value(instance, 'id', '') or '')
        public_ip, private_ip = _instance_ips(instance)
        if private_ip != DISTRIBUTED_PRIVATE_ADDRESSES[node_key]:
            raise RuntimeError(
                f'GCP distributed {node_key} received private address '
                f'{private_ip or "none"}, expected '
                f'{DISTRIBUTED_PRIVATE_ADDRESSES[node_key]}.'
            )
        if node_key in DISTRIBUTED_PUBLIC_IP_NODES and not public_ip:
            raise RuntimeError(
                f'GCP distributed {node_key} did not receive its required '
                'public management address.'
            )
        if node_key not in DISTRIBUTED_PUBLIC_IP_NODES and public_ip:
            raise RuntimeError(
                f'GCP distributed {node_key} unexpectedly received a public '
                'address.'
            )
        normalized = node_key.replace('-', '_')
        values: dict[str, Any] = {
            f'gcp_dsb_{normalized}_instance_id': instance_id,
            f'gcp_dsb_{normalized}_public_ip': public_ip,
            f'gcp_dsb_{normalized}_private_ip': private_ip,
            f'{prefix}_public_ip': public_ip,
            f'{prefix}_private_ip': private_ip,
        }
        if node_key == 'application':
            values.update({
                'instance_id': instance_id,
                'public_ip': public_ip,
                'private_ip': private_ip,
            })
        elif node_key == 'load-generator':
            values.update({
                'loadgen_instance_id': instance_id,
                'loadgen_public_ip': public_ip,
                'loadgen_private_ip': private_ip,
                'loadgen_shape': details['machine_type'],
                'loadgen_vcpus': details['vcpu'],
                'loadgen_memory_gb': details['memory_gb'],
                'loadgen_architecture': details['architecture'],
                'loadgen_image_id': support_image['image_id'],
                'loadgen_image_name': support_image['name'],
                'loadgen_network_bandwidth_gbps': (
                    LOADGEN_NETWORK_BANDWIDTH_GBPS
                ),
                'loadgen_network_capacity_kind': 'maximum',
            })
        _record(job, persist, **values)
        _capture_boot_disk_identity(job, persist, boot_prefix, clients)
        _set_inventory_node_status(
            job,
            persist,
            node_key,
            'running',
            provider_resource_id=instance_id,
            public_address=public_ip,
            private_address=private_ip,
        )
        _emit(
            job,
            emit,
            'Provision',
            f'Launched GCP distributed {node_key} role on '
            f'{details["machine_type"]} at {private_ip}.',
        )

    _emit(
        job,
        emit,
        'Provision',
        'GCP distributed candidate infrastructure reached the five-node '
        'lifecycle boundary; no benchmark workload was installed.',
    )
    return resources


RESOURCE_PREFIXES = (
    'gcp_loadgen_instance',
    'gcp_peer_instance',
    'gcp_instance',
    'gcp_data_disk',
    'gcp_loadgen_boot_disk',
    'gcp_peer_boot_disk',
    'gcp_runner_boot_disk',
    'gcp_web_firewall',
    'gcp_iperf_firewall',
    'gcp_ssh_firewall',
    'gcp_subnet',
    'gcp_network',
)

DISTRIBUTED_RESOURCE_PREFIXES = (
    *DISTRIBUTED_NODE_PREFIXES.values(),
    'gcp_dsb_database_data_disk',
    *DISTRIBUTED_BOOT_PREFIXES.values(),
    *DISTRIBUTED_FIREWALL_PREFIXES,
    'gcp_dsb_router',
    'gcp_dsb_loadgen_subnet',
    'gcp_dsb_cluster_subnet',
    'gcp_dsb_network',
)


def _contract_keys(prefix: str) -> tuple[str, ...]:
    keys = [
        f'{prefix}_name',
        f'{prefix}_id',
        f'{prefix}_self_link',
        f'{prefix}_request_id',
        f'{prefix}_create_ambiguous',
        f'{prefix}_reconciliation_error',
        f'{prefix}_delete_request_id',
    ]
    if prefix == 'gcp_instance':
        keys.extend(('instance_id', 'public_ip', 'private_ip'))
    elif prefix == 'gcp_loadgen_instance':
        keys.extend((
            'loadgen_instance_id',
            'loadgen_public_ip',
            'loadgen_private_ip',
            'loadgen_shape',
            'loadgen_vcpus',
            'loadgen_memory_gb',
            'loadgen_architecture',
            'loadgen_image_id',
            'loadgen_image_name',
            'loadgen_network_bandwidth_gbps',
            'loadgen_network_capacity_kind',
            'loadgen_network_peak_bandwidth_gbps',
            'gcp_loadgen_machine_type',
            'gcp_loadgen_network_bandwidth_gbps',
        ))
    elif prefix == 'gcp_peer_instance':
        keys.extend((
            'peer_private_ip',
            'gcp_peer_machine_type',
            'gcp_peer_image_id',
        ))
    elif prefix == 'gcp_data_disk':
        keys.extend((
            'gcp_data_disk_type',
            'gcp_data_disk_size_gb',
            'gcp_data_device_name',
            'gcp_data_disk_provisioned_iops',
            'gcp_data_disk_provisioned_throughput_mibps',
        ))
    elif prefix in {
        'gcp_runner_boot_disk',
        'gcp_peer_boot_disk',
        'gcp_loadgen_boot_disk',
    }:
        keys.extend((
            f'{prefix}_type',
            f'{prefix}_size_gb',
            f'{prefix}_provisioned_iops',
            f'{prefix}_provisioned_throughput_mibps',
        ))
    return tuple(keys)


def _resource_get_spec(
    prefix: str,
    resources: Mapping[str, Any],
    clients: Mapping[str, Any],
) -> tuple[Any, str, dict[str, Any]]:
    project_id = resources['gcp_project_id']
    region = resources.get('region') or DEFAULT_REGION
    zone = resources.get('gcp_zone') or resources.get('availability_zone')
    name = resources.get(f'{prefix}_name')
    if prefix in {
        'gcp_instance',
        'gcp_peer_instance',
        'gcp_loadgen_instance',
        *DISTRIBUTED_NODE_PREFIXES.values(),
    }:
        return clients['instances'], 'GetInstanceRequest', {
            'project': project_id,
            'zone': zone,
            'instance': name,
        }
    if prefix in {
        'gcp_data_disk',
        'gcp_runner_boot_disk',
        'gcp_peer_boot_disk',
        'gcp_loadgen_boot_disk',
        'gcp_dsb_database_data_disk',
        *DISTRIBUTED_BOOT_PREFIXES.values(),
    }:
        return clients['disks'], 'GetDiskRequest', {
            'project': project_id,
            'zone': zone,
            'disk': name,
        }
    if prefix in {
        'gcp_ssh_firewall',
        'gcp_iperf_firewall',
        'gcp_web_firewall',
        *DISTRIBUTED_FIREWALL_PREFIXES,
    }:
        return clients['firewalls'], 'GetFirewallRequest', {
            'project': project_id,
            'firewall': name,
        }
    if prefix in {
        'gcp_subnet',
        'gcp_dsb_cluster_subnet',
        'gcp_dsb_loadgen_subnet',
    }:
        return clients['subnetworks'], 'GetSubnetworkRequest', {
            'project': project_id,
            'region': region,
            'subnetwork': name,
        }
    if prefix == 'gcp_dsb_router':
        return clients['routers'], 'GetRouterRequest', {
            'project': project_id,
            'region': region,
            'router': name,
        }
    if prefix in {'gcp_network', 'gcp_dsb_network'}:
        return clients['networks'], 'GetNetworkRequest', {
            'project': project_id,
            'network': name,
        }
    raise KeyError(prefix)


def _get_cleanup_resource(
    prefix: str,
    resources: Mapping[str, Any],
    clients: Mapping[str, Any],
):
    client, request_type, values = _resource_get_spec(prefix, resources, clients)
    return _get_or_none(client, clients, request_type, **values)


def _bounded_cleanup_lookup(
    prefixes: Iterable[str],
    resources: Mapping[str, Any],
    clients: Mapping[str, Any],
    *,
    attempts: int = RECONCILIATION_ATTEMPTS,
) -> dict[str, Any]:
    """Poll response-lost exact names together within one bounded window."""
    pending = set(prefixes)
    found: dict[str, Any] = {}
    for attempt in range(max(1, attempts)):
        for prefix in tuple(pending):
            resource = _get_cleanup_resource(prefix, resources, clients)
            if resource is not None:
                found[prefix] = resource
                pending.remove(prefix)
        if not pending or attempt + 1 >= max(1, attempts):
            break
        time.sleep(RECONCILIATION_DELAY_SECONDS)
    for prefix in pending:
        found[prefix] = None
    return found


def _required_labels(job_id: str, role: str, resource: Any, label: str):
    labels = dict(_value(resource, 'labels', {}) or {})
    expected = _labels(job_id, role)
    if any(labels.get(key) != value for key, value in expected.items()):
        raise RuntimeError(
            f'Refusing GCP cleanup because the recorded {label} lacks this '
            'job\'s required ownership labels. No resources were changed.'
        )


def _assert_saved_identity(
    prefix: str,
    resource: Any,
    resources: Mapping[str, Any],
):
    label = prefix.removeprefix('gcp_').replace('_', ' ')
    name = str(resources.get(f'{prefix}_name') or '')
    if str(_value(resource, 'name', '') or '') != name:
        raise RuntimeError(
            f'Refusing GCP cleanup because the recorded {label} name changed. '
            'No resources were changed.'
        )
    actual_id, self_link = _required_resource_identity(prefix, resource)
    saved_id = str(resources.get(f'{prefix}_id') or '')
    saved_link = str(resources.get(f'{prefix}_self_link') or '')
    if saved_id and saved_id != actual_id:
        raise RuntimeError(
            f'Refusing GCP cleanup because {label} {name} now has numeric ID '
            f'{actual_id}, not recorded ID {saved_id}. No resources were changed.'
        )
    if saved_link and saved_link != self_link:
        raise RuntimeError(
            f'Refusing GCP cleanup because {label} {name} has a different '
            'selfLink. No resources were changed.'
        )


def _description_role(prefix: str) -> str | None:
    return {
        'gcp_network': 'network',
        'gcp_subnet': 'subnet',
        'gcp_ssh_firewall': 'ssh-firewall',
        'gcp_iperf_firewall': 'iperf-firewall',
        'gcp_web_firewall': 'web-firewall',
        'gcp_instance': 'runner',
        'gcp_peer_instance': 'iperf-peer',
        'gcp_loadgen_instance': 'load-generator',
        'gcp_data_disk': 'data',
        'gcp_runner_boot_disk': 'runner-boot',
        'gcp_peer_boot_disk': 'iperf-peer-boot',
        'gcp_loadgen_boot_disk': 'load-generator-boot',
    }.get(prefix)


def _label_role(prefix: str) -> str | None:
    role = {
        'gcp_instance': 'runner',
        'gcp_peer_instance': 'iperf-peer',
        'gcp_loadgen_instance': 'load-generator',
        'gcp_data_disk': 'data',
        'gcp_runner_boot_disk': 'runner-boot',
        'gcp_peer_boot_disk': 'iperf-peer-boot',
        'gcp_loadgen_boot_disk': 'load-generator-boot',
        'gcp_dsb_database_data_disk': 'database-data',
    }.get(prefix)
    if role:
        return role
    for node_role, boot_prefix in DISTRIBUTED_BOOT_PREFIXES.items():
        if prefix == boot_prefix:
            return f'{node_role}-boot'
    return None


def _verify_cleanup_resources(
    job: dict[str, Any],
    found: Mapping[str, Any],
):
    """Validate identity, ownership, and every managed parent relationship."""
    resources = job['resources']
    job_id = str(job['id']).lower()
    for prefix, resource in found.items():
        if resource is None:
            continue
        _assert_saved_identity(prefix, resource, resources)
        role = _description_role(prefix)
        if role and str(_value(resource, 'description', '') or '') != _description(
            job_id, role
        ):
            raise RuntimeError(
                f'Refusing GCP cleanup because the recorded '
                f'{prefix.removeprefix("gcp_").replace("_", " ")} has an '
                'unexpected ownership description. No resources were changed.'
            )
        label_role = _label_role(prefix)
        if label_role:
            _required_labels(
                job_id,
                label_role,
                resource,
                prefix.removeprefix('gcp_').replace('_', ' '),
            )

    network = found.get('gcp_network')
    network_link = (
        _value(network, 'self_link')
        if network is not None
        else resources.get('gcp_network_self_link')
    )
    network_name = resources.get('gcp_network_name')
    if not network_link and network_name:
        network_link = (
            f'projects/{resources["gcp_project_id"]}/global/networks/'
            f'{network_name}'
        )
    subnet = found.get('gcp_subnet')
    subnet_link = (
        _value(subnet, 'self_link')
        if subnet is not None
        else resources.get('gcp_subnet_self_link')
    )
    subnet_name = resources.get('gcp_subnet_name')
    if not subnet_link and subnet_name:
        subnet_link = (
            f'projects/{resources["gcp_project_id"]}/regions/'
            f'{resources.get("region") or DEFAULT_REGION}/subnetworks/'
            f'{subnet_name}'
        )

    if network is not None:
        if bool(_value(network, 'auto_create_subnetworks', True)):
            raise RuntimeError(
                'Refusing GCP cleanup because the recorded VPC is no longer '
                'an isolated custom-mode network. No resources were changed.'
            )
    if subnet is not None:
        if not _link_equal(_value(subnet, 'network'), network_link or network_name):
            raise RuntimeError(
                'Refusing GCP cleanup because the recorded subnet belongs to '
                'a different VPC. No resources were changed.'
            )
        if str(_value(subnet, 'ip_cidr_range', '') or '') != SUBNET_CIDR:
            raise RuntimeError(
                'Refusing GCP cleanup because the recorded subnet CIDR '
                'changed. No resources were changed.'
            )

    runner_tag = str(resources.get('gcp_runner_tag') or '')
    peer_tag = str(resources.get('gcp_peer_tag') or '')
    loadgen_tag = str(resources.get('gcp_loadgen_tag') or '')
    protocols = tuple(resources.get('gcp_iperf3_protocols') or ())
    web_benchmarks = set(resources.get('gcp_web_benchmarks') or ())
    web_ports = set()
    if 'apachebench' in web_benchmarks:
        web_ports.add('80')
    if 'deathstarbench' in web_benchmarks:
        web_ports.update(('5000', '8080'))
    firewall_contracts = (
        (
            'gcp_ssh_firewall',
            {runner_tag} | ({loadgen_tag} if loadgen_tag else set()),
            {SSH_SOURCE_CIDR},
            set(),
            {('tcp', ('22',))},
        ),
        (
            'gcp_iperf_firewall',
            {peer_tag},
            set(),
            {runner_tag},
            {('tcp', ('5201',))}
            | ({('udp', ('5201',))} if 'udp' in protocols else set())
            | ({('132', ())} if 'sctp' in protocols else set()),
        ),
        (
            'gcp_web_firewall',
            {runner_tag},
            set(),
            {loadgen_tag},
            {('tcp', tuple(sorted(web_ports)))},
        ),
    )
    for (
        prefix,
        expected_targets,
        expected_source_ranges,
        expected_source_tags,
        expected_allowed,
    ) in firewall_contracts:
        firewall = found.get(prefix)
        if firewall is None:
            continue
        if not _link_equal(_value(firewall, 'network'), network_link or network_name):
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} belongs to a '
                'different VPC. No resources were changed.'
            )
        if _string_set(_value(firewall, 'target_tags')) != expected_targets:
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} targets a different '
                'instance set. No resources were changed.'
            )
        if _string_set(
            _value(firewall, 'source_ranges')
        ) != expected_source_ranges or _string_set(
            _value(firewall, 'source_tags')
        ) != expected_source_tags:
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} source scope changed. '
                'No resources were changed.'
            )
        if (
            _allowed_set(_value(firewall, 'allowed')) != expected_allowed
            or str(_value(firewall, 'direction', '') or '') != 'INGRESS'
            or int(_value(firewall, 'priority', 0) or 0) != 1000
            or bool(_value(firewall, 'disabled', False))
        ):
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} allowed ports changed. '
                'No resources were changed.'
            )

    for prefix, expected_tag, expected_machine, boot_prefix in (
        (
            'gcp_instance',
            runner_tag,
            str(resources.get('gcp_machine_type') or ''),
            'gcp_runner_boot_disk',
        ),
        (
            'gcp_peer_instance',
            peer_tag,
            str(resources.get('gcp_machine_type') or ''),
            'gcp_peer_boot_disk',
        ),
        (
            'gcp_loadgen_instance',
            loadgen_tag,
            str(
                resources.get('gcp_loadgen_machine_type')
                or LOADGEN_MACHINE_TYPE
            ),
            'gcp_loadgen_boot_disk',
        ),
    ):
        instance = found.get(prefix)
        if instance is None:
            continue
        if _basename(_value(instance, 'machine_type')) != expected_machine:
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} has a different '
                'machine type. No resources were changed.'
            )
        tags = _string_set(_value(_value(instance, 'tags', {}), 'items', ()))
        if tags != {expected_tag}:
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} has a different '
                'network tag. No resources were changed.'
            )
        if _value(instance, 'service_accounts', ()) or ():
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} now has a service '
                'account attached. No resources were changed.'
            )
        interfaces = list(_value(instance, 'network_interfaces', ()) or ())
        if len(interfaces) != 1 or not _link_equal(
            _value(interfaces[0], 'network'), network_link or network_name
        ) or not _link_equal(
            _value(interfaces[0], 'subnetwork'), subnet_link or subnet_name
        ):
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} is attached to an '
                'unexpected network or subnet. No resources were changed.'
            )
        expected_nic_type = str(
            (
                None
                if prefix == 'gcp_loadgen_instance'
                else resources.get('gcp_network_interface_type')
            )
            or ''
        )
        if expected_nic_type and str(
            _value(interfaces[0], 'nic_type', '') or ''
        ) != expected_nic_type:
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} has a different '
                'network interface type. No resources were changed.'
            )
        boot_name = str(resources.get(f'{boot_prefix}_name') or '')
        expected_devices = {boot_name}
        data_name = str(resources.get('gcp_data_device_name') or '')
        if prefix == 'gcp_instance' and data_name:
            expected_devices.add(data_name)
        disks = list(_value(instance, 'disks', ()) or ())
        actual_devices = {
            str(_value(disk, 'device_name', '') or '') for disk in disks
        }
        if not boot_name or actual_devices != expected_devices:
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} has an unexpected '
                'attached disk set. No resources were changed.'
            )
        for disk in disks:
            device_name = str(_value(disk, 'device_name', '') or '')
            expected_disk_interface = str(
                (
                    None
                    if prefix == 'gcp_loadgen_instance'
                    else resources.get('gcp_disk_interface')
                )
                or ''
            )
            if expected_disk_interface and str(
                _value(disk, 'interface', '') or ''
            ) != expected_disk_interface:
                raise RuntimeError(
                    f'Refusing GCP cleanup because {prefix} disk '
                    f'{device_name or "unknown"} has a different disk '
                    'interface. No resources were changed.'
                )
            if device_name == boot_name:
                expected_boot_link = _zonal_resource_link(
                    str(resources.get('gcp_project_id') or ''),
                    str(
                        resources.get('gcp_zone')
                        or resources.get('availability_zone')
                        or ''
                    ),
                    'disks',
                    boot_name,
                )
                valid = (
                    bool(_value(disk, 'boot', False))
                    and bool(_value(disk, 'auto_delete', False))
                    and _scoped_link_equal(
                        _value(disk, 'source'), expected_boot_link
                    )
                )
            else:
                expected_data_name = str(
                    resources.get('gcp_data_disk_name') or data_name
                )
                expected_data_link = _zonal_resource_link(
                    str(resources.get('gcp_project_id') or ''),
                    str(
                        resources.get('gcp_zone')
                        or resources.get('availability_zone')
                        or ''
                    ),
                    'disks',
                    expected_data_name,
                )
                valid = (
                    not bool(_value(disk, 'boot', False))
                    and not bool(_value(disk, 'auto_delete', True))
                    and device_name == data_name
                    and _scoped_link_equal(
                        _value(disk, 'source'), expected_data_link
                    )
                )
            if not valid:
                raise RuntimeError(
                    f'Refusing GCP cleanup because {prefix} disk '
                    f'{device_name or "unknown"} has unsafe attachment '
                    'semantics. No resources were changed.'
                )

    for disk_prefix in (
        'gcp_data_disk',
        'gcp_runner_boot_disk',
        'gcp_peer_boot_disk',
        'gcp_loadgen_boot_disk',
    ):
        disk = found.get(disk_prefix)
        if disk is not None:
            _verify_disk_relationship(disk_prefix, disk, resources)
            _verify_disk_profile(disk_prefix, disk, resources)

    disk_parent_roles = {
        'gcp_data_disk': 'runner',
        'gcp_runner_boot_disk': 'runner',
        'gcp_peer_boot_disk': 'iperf-peer',
        'gcp_loadgen_boot_disk': 'loadgen',
    }
    for disk_prefix, parent_role in disk_parent_roles.items():
        disk = found.get(disk_prefix)
        if disk is None:
            continue
        users = list(_value(disk, 'users', ()) or ())
        expected_parent_link = _zonal_resource_link(
            str(resources.get('gcp_project_id') or ''),
            str(
                resources.get('gcp_zone')
                or resources.get('availability_zone')
                or ''
            ),
            'instances',
            _resource_name(job_id, parent_role),
        )
        if users and (
            len(users) != 1
            or not _scoped_link_equal(users[0], expected_parent_link)
        ):
            raise RuntimeError(
                f'Refusing GCP cleanup because {disk_prefix} is attached to '
                'an unexpected instance. No resources were changed.'
            )


def _persist_recovered_identity(
    job: dict[str, Any],
    persist: PersistCallback | None,
    prefix: str,
    resource: Any,
):
    resource_id, self_link = _required_resource_identity(prefix, resource)
    _record(
        job,
        persist,
        **{
            f'{prefix}_id': resource_id,
            f'{prefix}_self_link': self_link,
            f'{prefix}_create_ambiguous': False,
            f'{prefix}_reconciliation_error': None,
        },
    )


def _replay_ambiguous_create(
    job: dict[str, Any],
    persist: PersistCallback | None,
    prefix: str,
    found: Mapping[str, Any],
    clients: Mapping[str, Any],
):
    """Replay response-lost non-instance creates with their original UUID."""
    resources = job['resources']
    job_id = str(job['id']).lower()
    project_id = resources['gcp_project_id']
    region = resources.get('region') or DEFAULT_REGION
    zone = resources.get('gcp_zone') or resources.get('availability_zone')
    name = resources[f'{prefix}_name']
    network = found.get('gcp_network')
    network_link = (
        _value(network, 'self_link')
        if network is not None
        else resources.get('gcp_network_self_link')
    )
    if prefix == 'gcp_network':
        return _create_named_resource(
            job,
            persist,
            prefix=prefix,
            name=name,
            client=clients['networks'],
            clients=clients,
            insert_request_type='InsertNetworkRequest',
            insert_values={
                'project': project_id,
                'network_resource': _network_resource(clients, job_id, name),
            },
            get_request_type='GetNetworkRequest',
            get_values={'project': project_id, 'network': name},
        )
    if prefix == 'gcp_subnet':
        if not network_link:
            raise RuntimeError('the recorded GCP VPC is not available')
        body = _subnet_resource(clients, job_id, name, network_link)
        return _create_named_resource(
            job,
            persist,
            prefix=prefix,
            name=name,
            client=clients['subnetworks'],
            clients=clients,
            insert_request_type='InsertSubnetworkRequest',
            insert_values={
                'project': project_id,
                'region': region,
                'subnetwork_resource': body,
            },
            get_request_type='GetSubnetworkRequest',
            get_values={'project': project_id, 'region': region, 'subnetwork': name},
        )
    if prefix in {
        'gcp_ssh_firewall',
        'gcp_iperf_firewall',
        'gcp_web_firewall',
    }:
        if not network_link:
            raise RuntimeError('the recorded GCP VPC is not available')
        iperf = prefix == 'gcp_iperf_firewall'
        web = prefix == 'gcp_web_firewall'
        if iperf:
            role = 'iperf-firewall'
            target_tags = (resources.get('gcp_peer_tag'),)
            source_ranges = ()
            source_tags = (resources.get('gcp_runner_tag'),)
        elif web:
            role = 'web-firewall'
            target_tags = (resources.get('gcp_runner_tag'),)
            source_ranges = ()
            source_tags = (resources.get('gcp_loadgen_tag'),)
        else:
            role = 'ssh-firewall'
            target_tags = tuple(filter(None, (
                resources.get('gcp_runner_tag'),
                resources.get('gcp_loadgen_tag'),
            )))
            source_ranges = (SSH_SOURCE_CIDR,)
            source_tags = ()
        body = _firewall_resource(
            clients,
            job_id,
            name,
            network_link,
            role=role,
            target_tags=target_tags,
            source_ranges=source_ranges,
            source_tags=source_tags,
            protocols=resources.get('gcp_iperf3_protocols') or (),
            web_benchmarks=resources.get('gcp_web_benchmarks') or (),
        )
        return _create_named_resource(
            job,
            persist,
            prefix=prefix,
            name=name,
            client=clients['firewalls'],
            clients=clients,
            insert_request_type='InsertFirewallRequest',
            insert_values={'project': project_id, 'firewall_resource': body},
            get_request_type='GetFirewallRequest',
            get_values={'project': project_id, 'firewall': name},
        )
    if prefix == 'gcp_data_disk':
        size_gb = int(resources['gcp_data_disk_size_gb'])
        body = _data_disk_resource(
            clients,
            job_id,
            name,
            project_id,
            zone,
            size_gb,
            disk_type=str(
                resources.get('gcp_data_disk_type') or DEFAULT_DISK_TYPE
            ),
            provisioned_iops=resources.get(
                'gcp_data_disk_provisioned_iops'
            ),
            provisioned_throughput_mibps=resources.get(
                'gcp_data_disk_provisioned_throughput_mibps'
            ),
        )
        return _create_named_resource(
            job,
            persist,
            prefix=prefix,
            name=name,
            client=clients['disks'],
            clients=clients,
            insert_request_type='InsertDiskRequest',
            insert_values={
                'project': project_id,
                'zone': zone,
                'disk_resource': body,
            },
            get_request_type='GetDiskRequest',
            get_values={'project': project_id, 'zone': zone, 'disk': name},
        )
    raise RuntimeError(f'{prefix} cannot be safely replayed without secret guest data')


def _structured_error_codes(value: Any) -> set[str]:
    """Collect stable API error codes/reasons without inspecting prose."""
    codes: set[str] = set()

    def add(candidate: Any):
        if candidate is None or callable(candidate):
            return
        name = getattr(candidate, 'name', None)
        if name:
            codes.add(str(name).strip().upper())
        elif isinstance(candidate, str):
            codes.add(candidate.strip().upper())

    add(_value(value, 'reason'))
    add(_value(value, 'error_code'))
    code = _value(value, 'code')
    if callable(code):
        try:
            add(code())
        except Exception:
            pass
    else:
        add(code)
    for collection_name in (
        'errors',
        'error_details',
        'errorDetails',
        'details',
    ):
        collection = _value(value, collection_name) or ()
        if isinstance(collection, Mapping):
            collection = (collection,)
        for item in collection:
            add(_value(item, 'code'))
            add(_value(item, 'reason'))
            error_info = _value(item, 'error_info') or _value(item, 'errorInfo')
            add(_value(error_info, 'reason'))
    error = _value(value, 'error')
    if error and error is not value:
        for item in (_value(error, 'errors', ()) or ()):
            add(_value(item, 'code'))
            add(_value(item, 'reason'))
        for detail in (_value(error, 'error_details', ()) or ()):
            add(_value(_value(detail, 'error_info'), 'reason'))
    return {code for code in codes if code}


def _delete_retryable(
    exc: BaseException,
    operation: Any | None = None,
) -> bool:
    """Classify a delete failure using canonical status and reason fields."""
    status = _http_status(exc)
    if status in {408, 409, 429, 500, 502, 503, 504}:
        return True
    reasons = _structured_error_codes(exc)
    if operation is not None:
        reasons.update(_structured_error_codes(operation))
    if reasons & {
        'ABORTED',
        'BACKEND_ERROR',
        'DEADLINE_EXCEEDED',
        'INTERNAL',
        'INTERNAL_ERROR',
        'QUOTA_EXCEEDED',
        'RATE_LIMIT_EXCEEDED',
        'RESOURCE_EXHAUSTED',
        'RESOURCE_IN_USE_BY_ANOTHER_RESOURCE',
        'RESOURCE_NOT_READY',
        'SERVICE_UNAVAILABLE',
        'UNAVAILABLE',
    }:
        return True
    return exc.__class__.__name__ in {
        'Aborted',
        'DeadlineExceeded',
        'GatewayTimeout',
        'InternalServerError',
        'ResourceExhausted',
        'ServiceUnavailable',
        'TooManyRequests',
    }


def _operation_completed_with_error(operation: Any) -> bool:
    """Distinguish a terminal LRO error from an ambiguous polling failure."""
    if _value(operation, 'error_code'):
        return True
    if _value(operation, 'error'):
        return True
    done = _value(operation, 'done')
    if callable(done):
        try:
            return bool(done())
        except Exception:
            return False
    return False


def _delete_resource(
    job: dict[str, Any],
    persist: PersistCallback | None,
    prefix: str,
    clients: Mapping[str, Any],
):
    resources = job['resources']
    name = resources[f'{prefix}_name']
    project_id = resources['gcp_project_id']
    region = resources.get('region') or DEFAULT_REGION
    zone = resources.get('gcp_zone') or resources.get('availability_zone')
    request_key = f'{prefix}_delete_request_id'
    request_id = str(resources.get(request_key) or uuid.uuid4())
    _record(job, persist, **{request_key: request_id})
    if prefix in {
        'gcp_instance',
        'gcp_peer_instance',
        'gcp_loadgen_instance',
        *DISTRIBUTED_NODE_PREFIXES.values(),
    }:
        client, request_type, values = (
            clients['instances'],
            'DeleteInstanceRequest',
            {'project': project_id, 'zone': zone, 'instance': name},
        )
    elif prefix in {
        'gcp_data_disk',
        'gcp_runner_boot_disk',
        'gcp_peer_boot_disk',
        'gcp_loadgen_boot_disk',
        'gcp_dsb_database_data_disk',
        *DISTRIBUTED_BOOT_PREFIXES.values(),
    }:
        client, request_type, values = (
            clients['disks'],
            'DeleteDiskRequest',
            {'project': project_id, 'zone': zone, 'disk': name},
        )
    elif prefix in {
        'gcp_ssh_firewall',
        'gcp_iperf_firewall',
        'gcp_web_firewall',
        *DISTRIBUTED_FIREWALL_PREFIXES,
    }:
        client, request_type, values = (
            clients['firewalls'],
            'DeleteFirewallRequest',
            {'project': project_id, 'firewall': name},
        )
    elif prefix in {
        'gcp_subnet',
        'gcp_dsb_cluster_subnet',
        'gcp_dsb_loadgen_subnet',
    }:
        client, request_type, values = (
            clients['subnetworks'],
            'DeleteSubnetworkRequest',
            {'project': project_id, 'region': region, 'subnetwork': name},
        )
    elif prefix == 'gcp_dsb_router':
        client, request_type, values = (
            clients['routers'],
            'DeleteRouterRequest',
            {'project': project_id, 'region': region, 'router': name},
        )
    elif prefix in {'gcp_network', 'gcp_dsb_network'}:
        client, request_type, values = (
            clients['networks'],
            'DeleteNetworkRequest',
            {'project': project_id, 'network': name},
        )
    else:  # pragma: no cover - internal invariant
        raise KeyError(prefix)
    for attempt in range(8):
        try:
            values['request_id'] = request_id
            operation = _call(client, 'delete', clients, request_type, **values)
        except Exception as exc:
            if _not_found(exc):
                return
            # A transport/API-call failure before an operation is returned is
            # response-ambiguous. Replay the same UUID so Compute deduplicates
            # a request that may already have reached the service.
            if attempt < 7 and _delete_retryable(exc):
                time.sleep(RECONCILIATION_DELAY_SECONDS)
                continue
            raise
        try:
            _wait(operation)
            return
        except Exception as exc:
            if _not_found(exc):
                return
            completed_error = _operation_completed_with_error(operation)
            if completed_error:
                # A returned LRO that is conclusively DONE-with-error has
                # consumed this requestId. Replaying it can only replay the
                # same failed operation, so clear/rotate before a semantic
                # retry or a later cleanup invocation.
                _forget(job, persist, request_key)
            if attempt < 7 and _delete_retryable(exc, operation):
                if completed_error:
                    # A fresh UUID is a new name-only delete. Re-read and
                    # re-verify immutable identity/ownership before issuing it
                    # so a same-name replacement cannot be deleted between
                    # semantic retries.
                    current = _get_cleanup_resource(
                        prefix, resources, clients
                    )
                    if current is None:
                        return
                    if resources.get('gcp_distributed_candidate') is True:
                        _verify_distributed_cleanup_resources(
                            job, {prefix: current}
                        )
                    else:
                        _verify_cleanup_resources(job, {prefix: current})
                    request_id = str(uuid.uuid4())
                    _record(job, persist, **{request_key: request_id})
                time.sleep(RECONCILIATION_DELAY_SECONDS)
                continue
            raise


def _clear_contract(
    job: dict[str, Any],
    persist: PersistCallback | None,
    prefix: str,
):
    _forget(job, persist, *_contract_keys(prefix))


def _managed_resources_remain(resources: Mapping[str, Any]) -> bool:
    return bool(resources.get('gcp_resource_prefix')) or any(
        any(resources.get(key) is not None for key in _contract_keys(prefix))
        for prefix in RESOURCE_PREFIXES
    )


def _remaining_resource_contracts(resources: Mapping[str, Any]) -> list[str]:
    return [
        prefix
        for prefix in RESOURCE_PREFIXES
        if any(
            resources.get(key) is not None
            for key in _contract_keys(prefix)
        )
    ]


def _valid_request_id(value: Any) -> bool:
    try:
        parsed = uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        return False
    return parsed.int != 0


def _load_distributed_cleanup_contract(
    job: Mapping[str, Any],
) -> tuple[DeathStarBenchTopologyManifest, RoleNodeInventory, dict[str, str]]:
    resources = job.get('resources', {})
    if resources.get('gcp_distributed_candidate') is not True:
        raise ResourceInventoryError(
            'GCP distributed cleanup marker is missing or invalid.'
        )
    required = (
        DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
        DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY,
        ROLE_NODE_INVENTORY_KEY,
        'gcp_project_id',
        'gcp_compute_project_id',
        'gcp_zone',
        'gcp_dsb_expected_names',
    )
    missing = [key for key in required if key not in resources]
    if missing:
        raise ResourceInventoryError(
            'GCP distributed cleanup contract is incomplete: '
            + ', '.join(missing)
            + '.'
        )
    manifest = DeathStarBenchTopologyManifest.from_dict(
        resources[DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY]
    )
    if (
        manifest.topology_id != DISTRIBUTED_TIERED_TOPOLOGY_ID
        or manifest.runtime_id != K3S_RUNTIME_ID
        or resources[DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY]
        != manifest.fingerprint
    ):
        raise ResourceInventoryError(
            'GCP distributed cleanup topology is inconsistent.'
        )
    inventory = load_role_node_inventory(resources)
    if (
        inventory.provider != 'gcp'
        or inventory.topology_fingerprint != manifest.fingerprint
        or tuple(node.key for node in inventory.nodes)
        != tuple(sorted(DISTRIBUTED_PRIVATE_ADDRESSES))
    ):
        raise ResourceInventoryError(
            'GCP distributed cleanup inventory is inconsistent.'
        )
    raw_names = resources['gcp_dsb_expected_names']
    if not isinstance(raw_names, Mapping):
        raise ResourceInventoryError(
            'GCP distributed expected-name allowlist must be an object.'
        )
    expected_names = {
        str(key): str(value) for key, value in raw_names.items()
    }
    if set(expected_names) != set(DISTRIBUTED_RESOURCE_PREFIXES):
        raise ResourceInventoryError(
            'GCP distributed expected-name allowlist is incomplete or unknown.'
        )
    job_id = str(job.get('id') or '').lower()
    base_name = _resource_name(job_id, 'dsb')
    deterministic = {
        'gcp_dsb_network': base_name,
        'gcp_dsb_cluster_subnet': _resource_name(job_id, 'dsb-cluster'),
        'gcp_dsb_loadgen_subnet': _resource_name(job_id, 'dsb-loadgen'),
        'gcp_dsb_router': _resource_name(job_id, 'dsb-router'),
        'gcp_dsb_database_data_disk': _resource_name(
            job_id, 'dsb-database-data'
        ),
    }
    firewall_suffixes = {
        'gcp_dsb_public_ssh_firewall': 'dsb-public-ssh',
        'gcp_dsb_private_ssh_firewall': 'dsb-private-ssh',
        'gcp_dsb_k3s_api_firewall': 'dsb-k3s-api',
        'gcp_dsb_k3s_overlay_firewall': 'dsb-k3s-overlay',
        'gcp_dsb_benchmark_firewall': 'dsb-benchmark',
    }
    deterministic.update({
        prefix: _resource_name(job_id, suffix)
        for prefix, suffix in firewall_suffixes.items()
    })
    for role in DISTRIBUTED_PRIVATE_ADDRESSES:
        name = _resource_name(job_id, f'dsb-{role}')
        deterministic[DISTRIBUTED_NODE_PREFIXES[role]] = name
        deterministic[DISTRIBUTED_BOOT_PREFIXES[role]] = f'{name}-boot'
    if expected_names != deterministic:
        raise ResourceInventoryError(
            'GCP distributed expected-name allowlist conflicts with the job ID.'
        )
    if resources.get('gcp_resource_prefix') != base_name:
        raise ResourceInventoryError(
            'GCP distributed resource prefix conflicts with the job ID.'
        )
    for prefix, expected_name in expected_names.items():
        saved_name = resources.get(f'{prefix}_name')
        if saved_name is not None and saved_name != expected_name:
            raise ResourceInventoryError(
                f'GCP distributed {prefix} name conflicts with the allowlist.'
            )
    for node in inventory.nodes:
        role = node.key
        if (
            node.provider_resource_name != expected_names[
                DISTRIBUTED_NODE_PREFIXES[role]
            ]
            or node.private_addresses
            != (DISTRIBUTED_PRIVATE_ADDRESSES[role],)
            or node.zone != resources['gcp_zone']
            or node.shape != resources.get(
                f'gcp_dsb_{role.replace("-", "_")}_machine_type'
            )
            or node.architecture != resources.get(
                f'gcp_dsb_{role.replace("-", "_")}_architecture'
            )
        ):
            raise ResourceInventoryError(
                f'GCP distributed inventory role {role} conflicts with the '
                'provider contract.'
            )
        prefix = DISTRIBUTED_NODE_PREFIXES[role]
        flat_id = resources.get(f'{prefix}_id')
        if flat_id and node.provider_resource_id != str(flat_id):
            raise ResourceInventoryError(
                f'GCP distributed inventory role {role} has a conflicting ID.'
            )
    database = inventory.node('database')
    storage = database.storage_resource('database-data') if database else None
    disk_prefix = 'gcp_dsb_database_data_disk'
    if (
        storage is None
        or storage.provider_resource_name != expected_names[disk_prefix]
        or storage.device != resources.get('gcp_dsb_database_disk_device')
    ):
        raise ResourceInventoryError(
            'GCP distributed database storage inventory is inconsistent.'
        )
    flat_disk_id = resources.get(f'{disk_prefix}_id')
    if flat_disk_id and storage.provider_resource_id != str(flat_disk_id):
        raise ResourceInventoryError(
            'GCP distributed database storage has a conflicting ID.'
        )
    return manifest, inventory, expected_names


def _distributed_description_role(prefix: str) -> str:
    direct = {
        'gcp_dsb_network': 'distributed-network',
        'gcp_dsb_cluster_subnet': 'distributed-cluster-subnet',
        'gcp_dsb_loadgen_subnet': 'distributed-loadgen-subnet',
        'gcp_dsb_router': 'distributed-router',
        'gcp_dsb_database_data_disk': 'database-data',
        'gcp_dsb_public_ssh_firewall': 'dsb-public-ssh',
        'gcp_dsb_private_ssh_firewall': 'dsb-private-ssh',
        'gcp_dsb_k3s_api_firewall': 'dsb-k3s-api',
        'gcp_dsb_k3s_overlay_firewall': 'dsb-k3s-overlay',
        'gcp_dsb_benchmark_firewall': 'dsb-benchmark',
    }
    if prefix in direct:
        return direct[prefix]
    for role, node_prefix in DISTRIBUTED_NODE_PREFIXES.items():
        if prefix == node_prefix:
            return role
    for role, boot_prefix in DISTRIBUTED_BOOT_PREFIXES.items():
        if prefix == boot_prefix:
            return f'{role}-boot'
    raise KeyError(prefix)


def _verify_distributed_cleanup_resources(
    job: dict[str, Any],
    found: Mapping[str, Any],
    *,
    manifest: DeathStarBenchTopologyManifest | None = None,
):
    resources = job['resources']
    if manifest is None:
        manifest, _, expected_names = _load_distributed_cleanup_contract(job)
    else:
        expected_names = dict(resources['gcp_dsb_expected_names'])
    job_id = str(job['id']).lower()
    project_id = str(resources['gcp_project_id'])
    zone = str(resources['gcp_zone'])
    region = str(resources.get('region') or DEFAULT_REGION)
    for prefix, resource in found.items():
        if resource is None:
            continue
        _assert_saved_identity(prefix, resource, resources)
        role = _distributed_description_role(prefix)
        if str(_value(resource, 'description', '') or '') != _description(
            job_id, role
        ):
            raise RuntimeError(
                f'Refusing GCP distributed cleanup because {prefix} has an '
                'unexpected ownership description. No resources were changed.'
            )
        if prefix in {
            *DISTRIBUTED_NODE_PREFIXES.values(),
            *DISTRIBUTED_BOOT_PREFIXES.values(),
            'gcp_dsb_database_data_disk',
        }:
            _required_labels(job_id, role, resource, prefix)

    network = found.get('gcp_dsb_network')
    network_link = (
        _value(network, 'self_link')
        if network is not None
        else resources.get('gcp_dsb_network_self_link')
    ) or (
        f'projects/{project_id}/global/networks/'
        f'{expected_names["gcp_dsb_network"]}'
    )
    if network is not None:
        _assert_insert_resource_matches(
            'gcp_dsb_network',
            network,
            _network_resource(
                {}, job_id, resources['gcp_dsb_network_name'],
                role='distributed-network',
            ),
            project_id=project_id,
            zone=zone,
        )

    subnet_specs = {
        'gcp_dsb_cluster_subnet': (
            'distributed-cluster-subnet', DISTRIBUTED_CLUSTER_SUBNET_CIDR
        ),
        'gcp_dsb_loadgen_subnet': (
            'distributed-loadgen-subnet', DISTRIBUTED_LOADGEN_SUBNET_CIDR
        ),
    }
    subnet_links: dict[str, str] = {}
    for prefix, (role, cidr) in subnet_specs.items():
        subnet = found.get(prefix)
        expected_name = str(
            resources.get(f'{prefix}_name') or expected_names[prefix]
        )
        expected_link = (
            f'projects/{project_id}/regions/{region}/subnetworks/{expected_name}'
        )
        subnet_links[prefix] = str(
            _value(subnet, 'self_link') if subnet is not None else (
                resources.get(f'{prefix}_self_link') or expected_link
            )
        )
        if subnet is not None:
            _assert_insert_resource_matches(
                prefix,
                subnet,
                _subnet_resource(
                    {}, job_id, expected_name, network_link,
                    role=role, cidr=cidr,
                ),
                project_id=project_id,
                zone=zone,
            )

    router = found.get('gcp_dsb_router')
    if router is not None:
        actual_nats = list(_value(router, 'nats', ()) or ())
        if (
            resources.get('gcp_dsb_router_nat_configured') is True
            and not actual_nats
        ):
            raise RuntimeError(
                'Refusing GCP distributed cleanup because the router lost '
                'its recorded Cloud NAT feature. No resources were changed.'
            )
        _assert_insert_resource_matches(
            'gcp_dsb_router',
            router,
            _router_resource(
                {},
                job_id,
                resources['gcp_dsb_router_name'],
                network_link,
                subnet_links['gcp_dsb_cluster_subnet'],
                include_nat=bool(actual_nats),
            ),
            project_id=project_id,
            zone=zone,
        )

    role_tags = {
        role: str(resources[f'gcp_dsb_{role.replace("-", "_")}_tag'])
        for role in DISTRIBUTED_PRIVATE_ADDRESSES
    }
    expected_firewalls = {
        prefix: body
        for prefix, _, body in _distributed_firewall_specs(
            manifest,
            job_id=job_id,
            network=network_link,
            tags=role_tags,
            clients={},
        )
    }
    for prefix in DISTRIBUTED_FIREWALL_PREFIXES:
        firewall = found.get(prefix)
        if firewall is not None:
            _assert_insert_resource_matches(
                prefix,
                firewall,
                expected_firewalls[prefix],
                project_id=project_id,
                zone=zone,
            )

    for role, prefix in DISTRIBUTED_NODE_PREFIXES.items():
        instance = found.get(prefix)
        if instance is None:
            continue
        expected_machine = resources[
            f'gcp_dsb_{role.replace("-", "_")}_machine_type'
        ]
        if _basename(_value(instance, 'machine_type')) != expected_machine:
            raise RuntimeError(
                f'Refusing GCP distributed cleanup because {role} has a '
                'different machine type. No resources were changed.'
            )
        tags = _string_set(_value(_value(instance, 'tags', {}), 'items', ()))
        if tags != {role_tags[role]}:
            raise RuntimeError(
                f'Refusing GCP distributed cleanup because {role} tags '
                'changed. No resources were changed.'
            )
        if _value(instance, 'service_accounts', ()) or ():
            raise RuntimeError(
                f'Refusing GCP distributed cleanup because {role} has a '
                'service account. No resources were changed.'
            )
        metadata = {
            str(_value(item, 'key', '')): str(_value(item, 'value', '') or '')
            for item in (
                _value(_value(instance, 'metadata', {}) or {}, 'items', ()) or ()
            )
        }
        if (
            metadata.get('block-project-ssh-keys') != 'TRUE'
            or metadata.get('enable-oslogin') != 'FALSE'
            or not metadata.get('ssh-keys', '').startswith(f'{SSH_USER}:')
        ):
            raise RuntimeError(
                f'Refusing GCP distributed cleanup because {role} SSH '
                'metadata changed. No resources were changed.'
            )
        interfaces = list(_value(instance, 'network_interfaces', ()) or ())
        subnet_prefix = (
            'gcp_dsb_loadgen_subnet'
            if role == 'load-generator'
            else 'gcp_dsb_cluster_subnet'
        )
        if len(interfaces) != 1 or not _link_equal(
            _value(interfaces[0], 'network'), network_link
        ) or not _link_equal(
            _value(interfaces[0], 'subnetwork'), subnet_links[subnet_prefix]
        ):
            raise RuntimeError(
                f'Refusing GCP distributed cleanup because {role} network '
                'relationships changed. No resources were changed.'
            )
        private_ip = (
            _value(interfaces[0], 'network_i_p')
            or _value(interfaces[0], 'network_ip')
        )
        if str(private_ip or '') != DISTRIBUTED_PRIVATE_ADDRESSES[role]:
            raise RuntimeError(
                f'Refusing GCP distributed cleanup because {role} private '
                'address changed. No resources were changed.'
            )
        access = list(_value(interfaces[0], 'access_configs', ()) or ())
        expected_public = role in DISTRIBUTED_PUBLIC_IP_NODES
        if bool(access) != expected_public or len(access) > 1:
            raise RuntimeError(
                f'Refusing GCP distributed cleanup because {role} public '
                'access changed. No resources were changed.'
            )
        boot_name = resources[f'{DISTRIBUTED_BOOT_PREFIXES[role]}_name']
        expected_devices = {boot_name}
        if role == 'database':
            expected_devices.add(resources['gcp_dsb_database_data_disk_name'])
        disks = list(_value(instance, 'disks', ()) or ())
        if {
            str(_value(item, 'device_name', '') or '') for item in disks
        } != expected_devices:
            raise RuntimeError(
                f'Refusing GCP distributed cleanup because {role} disk set '
                'changed. No resources were changed.'
            )
        for disk in disks:
            device_name = str(_value(disk, 'device_name', '') or '')
            expected_link = _zonal_resource_link(
                project_id, zone, 'disks', device_name
            )
            if not _scoped_link_equal(_value(disk, 'source'), expected_link):
                raise RuntimeError(
                    f'Refusing GCP distributed cleanup because {role} disk '
                    'source changed. No resources were changed.'
                )
            if device_name == boot_name:
                safe = bool(_value(disk, 'boot', False)) and bool(
                    _value(disk, 'auto_delete', False)
                )
            else:
                safe = (
                    not bool(_value(disk, 'boot', False))
                    and not bool(_value(disk, 'auto_delete', True))
                    and str(_value(disk, 'interface', '') or '')
                    == DISTRIBUTED_DATABASE_DISK_INTERFACE
                )
            if not safe:
                raise RuntimeError(
                    f'Refusing GCP distributed cleanup because {role} disk '
                    'attachment semantics changed. No resources were changed.'
                )

    for prefix in (
        'gcp_dsb_database_data_disk',
        *DISTRIBUTED_BOOT_PREFIXES.values(),
    ):
        disk = found.get(prefix)
        if disk is None:
            continue
        _verify_disk_relationship(prefix, disk, resources)
        _verify_disk_profile(prefix, disk, resources)
        users = list(_value(disk, 'users', ()) or ())
        role = (
            'database'
            if prefix == 'gcp_dsb_database_data_disk'
            else next(
                key for key, value in DISTRIBUTED_BOOT_PREFIXES.items()
                if value == prefix
            )
        )
        if users:
            parent_name = resources.get(
                f'{DISTRIBUTED_NODE_PREFIXES[role]}_name'
            )
            if not parent_name:
                raise RuntimeError(
                    f'Refusing GCP distributed cleanup because {prefix} is '
                    'attached but its parent VM contract is absent. No '
                    'resources were changed.'
                )
            expected_parent = _zonal_resource_link(
                project_id,
                zone,
                'instances',
                parent_name,
            )
            if (
                len(users) != 1
                or not _scoped_link_equal(users[0], expected_parent)
            ):
                raise RuntimeError(
                    f'Refusing GCP distributed cleanup because {prefix} is '
                    'attached to another VM. No resources were changed.'
                )


def _destroy_distributed_deathstarbench_candidate(
    job: dict[str, Any],
    *,
    emit: EventCallback | None,
    persist: PersistCallback | None,
    credentials: Any,
    clients: Mapping[str, Any] | None,
    preserve_status: bool,
):
    resources = job.setdefault('resources', {})
    try:
        manifest, inventory, expected_names = (
            _load_distributed_cleanup_contract(job)
        )
    except (ResourceInventoryError, TopologyManifestError, ValueError) as exc:
        raise RuntimeError(
            'Refusing GCP distributed cleanup because the persisted '
            f'ownership inventory is invalid: {exc}'
        ) from exc
    project_id, _, clients = _runtime(
        str(resources['gcp_project_id']), credentials, clients
    )
    project = _call(
        clients['projects'], 'get', clients, 'GetProjectRequest',
        project=project_id,
    )
    current_project_id = str(_value(project, 'id', '') or '')
    if current_project_id != str(resources['gcp_compute_project_id']):
        raise RuntimeError(
            'Refusing GCP distributed cleanup because the immutable Compute '
            'project ID changed. No resources were changed.'
        )

    for prefix in DISTRIBUTED_RESOURCE_PREFIXES:
        if not resources.get(f'{prefix}_name'):
            continue
        ambiguous = resources.get(f'{prefix}_create_ambiguous') is True
        request_id = resources.get(f'{prefix}_request_id')
        if prefix in DISTRIBUTED_BOOT_PREFIXES.values():
            role = next(
                key for key, value in DISTRIBUTED_BOOT_PREFIXES.items()
                if value == prefix
            )
            parent_request = resources.get(
                f'{DISTRIBUTED_NODE_PREFIXES[role]}_request_id'
            )
            if request_id and parent_request and request_id != parent_request:
                raise RuntimeError(
                    f'Refusing GCP distributed cleanup because {prefix} has '
                    'a request ID different from its parent VM.'
                )
            request_id = request_id or parent_request
        if ambiguous and not _valid_request_id(request_id):
            raise RuntimeError(
                f'Refusing GCP distributed cleanup because {prefix} has an '
                'invalid response-loss request ID. No resources were changed.'
            )
        if not ambiguous and (
            not resources.get(f'{prefix}_id')
            or not resources.get(f'{prefix}_self_link')
        ):
            raise RuntimeError(
                f'Refusing GCP distributed cleanup because {prefix} lacks '
                'immutable identity. No resources were changed.'
            )

    found = {
        prefix: _get_cleanup_resource(prefix, resources, clients)
        for prefix in DISTRIBUTED_RESOURCE_PREFIXES
        if resources.get(f'{prefix}_name')
    }
    missing = [prefix for prefix, resource in found.items() if resource is None]
    if missing:
        found.update(_bounded_cleanup_lookup(
            missing,
            resources,
            clients,
        ))
    unresolved = [
        prefix for prefix, resource in found.items()
        if resource is None
        and resources.get(f'{prefix}_create_ambiguous') is True
    ]
    if unresolved:
        raise RuntimeError(
            'GCP distributed cleanup cannot yet prove whether response-lost '
            'creates exist for: '
            + ', '.join(unresolved)
            + '. No resources were changed; retry cleanup.'
        )
    _verify_distributed_cleanup_resources(
        job, found, manifest=manifest
    )
    for prefix, resource in found.items():
        if resource is not None:
            _persist_recovered_identity(job, persist, prefix, resource)

    if not preserve_status:
        job['status'] = 'destroying'
        _persist(job, persist)
    _emit(
        job, emit, 'Destroy',
        'Removing exact GCP distributed benchmark infrastructure.',
    )
    delete_order = (
        *(DISTRIBUTED_NODE_PREFIXES[key] for key in manifest.deletion_order),
        'gcp_dsb_database_data_disk',
        *(DISTRIBUTED_BOOT_PREFIXES[key] for key in manifest.deletion_order),
        *DISTRIBUTED_FIREWALL_PREFIXES,
        'gcp_dsb_router',
        'gcp_dsb_loadgen_subnet',
        'gcp_dsb_cluster_subnet',
        'gcp_dsb_network',
    )
    errors: list[str] = []
    for prefix in delete_order:
        if not resources.get(f'{prefix}_name') or found.get(prefix) is None:
            continue
        role = next((
            key for key, value in DISTRIBUTED_NODE_PREFIXES.items()
            if value == prefix
        ), None)
        try:
            if role:
                _set_inventory_node_status(
                    job, persist, role, 'deleting'
                )
            elif prefix == 'gcp_dsb_database_data_disk':
                _set_inventory_storage_status(
                    job, persist, 'database', 'database-data', 'deleting'
                )
            current = _get_cleanup_resource(prefix, resources, clients)
            if current is not None:
                check_found = dict(found)
                check_found[prefix] = current
                _verify_distributed_cleanup_resources(
                    job, check_found, manifest=manifest
                )
                _delete_resource(job, persist, prefix, clients)
            if role:
                _set_inventory_node_status(
                    job, persist, role, 'deleted'
                )
            elif prefix == 'gcp_dsb_database_data_disk':
                _set_inventory_storage_status(
                    job, persist, 'database', 'database-data', 'deleted'
                )
            _emit(
                job,
                emit,
                'Destroy',
                f'Deleted GCP distributed resource '
                f'{resources.get(f"{prefix}_name")}.',
            )
        except Exception as exc:
            errors.append(
                f'Unable to delete GCP distributed resource '
                f'{resources.get(f"{prefix}_name")}: {exc}'
            )

    final_resources = dict(resources)
    for prefix, name in expected_names.items():
        final_resources[f'{prefix}_name'] = name
    remaining = []
    for prefix in DISTRIBUTED_RESOURCE_PREFIXES:
        try:
            if _get_cleanup_resource(prefix, final_resources, clients) is not None:
                remaining.append(prefix)
        except Exception as exc:
            errors.append(f'Unable to prove {prefix} absent: {exc}')
    if remaining:
        errors.append(
            'GCP distributed resources remain: ' + ', '.join(remaining)
        )
    if errors:
        raise RuntimeError('; '.join(dict.fromkeys(errors)))

    # Every GCP-prefixed value in this operator-only contract is either a
    # cloud ownership anchor or provider metadata derived from that graph.
    # Clear the complete provider contract only after the independent final
    # absence sweep above succeeds.  Leaving generic values such as the
    # project/zone or pinned-image fields behind would make the run appear
    # recoverable even though no GCE resources remain.
    cleanup_keys = [
        key for key in tuple(resources)
        if key.startswith('gcp_')
    ]
    cleanup_keys.extend((
        'gcp_distributed_candidate',
        'gcp_resource_prefix',
        'gcp_web_benchmarks',
        'instance_id',
        'public_ip',
        'private_ip',
        'loadgen_instance_id',
        'loadgen_public_ip',
        'loadgen_private_ip',
        'loadgen_shape',
        'loadgen_vcpus',
        'loadgen_memory_gb',
        'loadgen_architecture',
        'loadgen_image_id',
        'loadgen_image_name',
        'loadgen_network_bandwidth_gbps',
        'loadgen_network_capacity_kind',
        ROLE_NODE_INVENTORY_KEY,
        DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
        DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY,
        K3S_RUNTIME_JOURNAL_KEY,
        DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
        DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    ))
    _forget(job, persist, *cleanup_keys)
    if not preserve_status:
        job['status'] = 'destroyed'
        job['cleanup_error'] = None
        _persist(job, persist)
        _emit(
            job, emit, 'Complete',
            'GCP distributed infrastructure was destroyed.',
        )
    return resources


def destroy_resources(
    job: dict[str, Any],
    *,
    emit: EventCallback | None = None,
    persist: PersistCallback | None = None,
    credentials=None,
    clients=None,
    preserve_status: bool = False,
):
    """Delete the exact, ownership-verified GCP resource graph for one job."""
    resources = job.setdefault('resources', {})
    if resources.get('gcp_distributed_candidate') is True:
        return _destroy_distributed_deathstarbench_candidate(
            job,
            emit=emit,
            persist=persist,
            credentials=credentials,
            clients=clients,
            preserve_status=preserve_status,
        )
    if job.get('status') == 'destroyed' and not _managed_resources_remain(resources):
        return resources
    project_id = str(
        resources.get('gcp_project_id')
        or _value(job.get('plan', {}), 'gcp_project_id')
        or ''
    ).strip()
    if not project_id:
        if not _managed_resources_remain(resources):
            return resources
        raise RuntimeError(
            'Refusing GCP cleanup because the recorded project ID is missing.'
        )
    project_id, _, clients = _runtime(project_id, credentials, clients)
    resources['gcp_project_id'] = project_id
    project = _call(
        clients['projects'], 'get', clients, 'GetProjectRequest', project=project_id
    )
    current_compute_id = str(_value(project, 'id', '') or '')
    expected_compute_id = str(resources.get('gcp_compute_project_id') or '')
    if _managed_resources_remain(resources) and not expected_compute_id:
        raise RuntimeError(
            'Refusing GCP cleanup because the immutable Compute project ID is '
            'missing from this managed-resource contract. No resources were changed.'
        )
    if expected_compute_id and current_compute_id != expected_compute_id:
        raise RuntimeError(
            'Refusing GCP cleanup because the selected project now has Compute '
            f'project ID {current_compute_id or "unknown"}, not recorded ID '
            f'{expected_compute_id}. Restore access to the original project.'
        )
    expected_prefix = _resource_name(str(job['id']).lower())
    saved_prefix = str(resources.get('gcp_resource_prefix') or expected_prefix)
    if saved_prefix != expected_prefix:
        raise RuntimeError(
            'Refusing GCP cleanup because the saved resource prefix does not '
            'match this job ID. No resources were changed.'
        )

    expected_names = {
        'gcp_network': expected_prefix,
        'gcp_subnet': expected_prefix,
        'gcp_ssh_firewall': f'{expected_prefix}-ssh',
        'gcp_iperf_firewall': f'{expected_prefix}-iperf',
        'gcp_web_firewall': f'{expected_prefix}-web',
        'gcp_instance': f'{expected_prefix}-runner',
        'gcp_peer_instance': f'{expected_prefix}-iperf-peer',
        'gcp_loadgen_instance': f'{expected_prefix}-loadgen',
        'gcp_data_disk': f'{expected_prefix}-data',
        'gcp_runner_boot_disk': f'{expected_prefix}-runner-boot',
        'gcp_peer_boot_disk': f'{expected_prefix}-iperf-peer-boot',
        'gcp_loadgen_boot_disk': f'{expected_prefix}-loadgen-boot',
    }
    for prefix, expected_name in expected_names.items():
        saved_name = resources.get(f'{prefix}_name')
        if saved_name and saved_name != expected_name:
            raise RuntimeError(
                f'Refusing GCP cleanup because the recorded '
                f'{prefix.removeprefix("gcp_").replace("_", " ")} name '
                'does not match this job. No resources were changed.'
            )

    # These descriptive keys are persisted before the data-disk create
    # contract. If the process stops before the name/requestId write (or the
    # initial insert is definitively rejected), no cloud resource could exist.
    if not resources.get('gcp_data_disk_name') and any(
        resources.get(key) is not None
        for key in (
            'gcp_data_disk_type',
            'gcp_data_disk_size_gb',
            'gcp_data_device_name',
            'gcp_data_disk_provisioned_iops',
            'gcp_data_disk_provisioned_throughput_mibps',
        )
    ):
        _forget(
            job,
            persist,
            'gcp_data_disk_type',
            'gcp_data_disk_size_gb',
            'gcp_data_device_name',
            'gcp_data_disk_provisioned_iops',
            'gcp_data_disk_provisioned_throughput_mibps',
        )

    # Fill deterministic boot names for interrupted instance inserts created
    # by earlier versions of the manifest contract.
    inferred = {}
    if resources.get('gcp_instance_name'):
        inferred['gcp_runner_boot_disk_name'] = (
            resources.get('gcp_runner_boot_disk_name')
            or f'{resources["gcp_instance_name"]}-boot'
        )
        inferred.setdefault(
            'gcp_runner_boot_disk_create_ambiguous',
            resources.get('gcp_runner_boot_disk_create_ambiguous', True),
        )
        if (
            not resources.get('gcp_runner_boot_disk_request_id')
            and resources.get('gcp_instance_request_id')
        ):
            inferred['gcp_runner_boot_disk_request_id'] = resources[
                'gcp_instance_request_id'
            ]
    if resources.get('gcp_peer_instance_name'):
        inferred['gcp_peer_boot_disk_name'] = (
            resources.get('gcp_peer_boot_disk_name')
            or f'{resources["gcp_peer_instance_name"]}-boot'
        )
        inferred.setdefault(
            'gcp_peer_boot_disk_create_ambiguous',
            resources.get('gcp_peer_boot_disk_create_ambiguous', True),
        )
        if (
            not resources.get('gcp_peer_boot_disk_request_id')
            and resources.get('gcp_peer_instance_request_id')
        ):
            inferred['gcp_peer_boot_disk_request_id'] = resources[
                'gcp_peer_instance_request_id'
            ]
    if resources.get('gcp_loadgen_instance_name'):
        inferred['gcp_loadgen_boot_disk_name'] = (
            resources.get('gcp_loadgen_boot_disk_name')
            or f'{resources["gcp_loadgen_instance_name"]}-boot'
        )
        inferred.setdefault(
            'gcp_loadgen_boot_disk_create_ambiguous',
            resources.get('gcp_loadgen_boot_disk_create_ambiguous', True),
        )
        if (
            not resources.get('gcp_loadgen_boot_disk_request_id')
            and resources.get('gcp_loadgen_instance_request_id')
        ):
            inferred['gcp_loadgen_boot_disk_request_id'] = resources[
                'gcp_loadgen_instance_request_id'
            ]
    if inferred:
        _record(job, persist, **inferred)

    for prefix in RESOURCE_PREFIXES:
        if not resources.get(f'{prefix}_name'):
            continue
        ambiguous = resources.get(f'{prefix}_create_ambiguous') is True
        if prefix in {
            'gcp_runner_boot_disk',
            'gcp_peer_boot_disk',
            'gcp_loadgen_boot_disk',
        }:
            parent = {
                'gcp_runner_boot_disk': 'gcp_instance',
                'gcp_peer_boot_disk': 'gcp_peer_instance',
                'gcp_loadgen_boot_disk': 'gcp_loadgen_instance',
            }[prefix]
            parent_request_id = resources.get(f'{parent}_request_id')
            boot_request_id = resources.get(f'{prefix}_request_id')
            if (
                boot_request_id
                and parent_request_id
                and str(boot_request_id) != str(parent_request_id)
            ):
                raise RuntimeError(
                    f'Refusing GCP cleanup because {prefix} has a response-loss '
                    'request ID different from its parent instance. No '
                    'resources were changed.'
                )
            request_id = boot_request_id or parent_request_id
        else:
            request_id = resources.get(f'{prefix}_request_id')
        boot_absence_proven = (
            prefix in {
                'gcp_runner_boot_disk',
                'gcp_peer_boot_disk',
                'gcp_loadgen_boot_disk',
            }
            and resources.get('gcp_network_absence_confirmed') is True
        )
        if (
            ambiguous
            and not boot_absence_proven
            and not _valid_request_id(request_id)
        ):
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} has an invalid or '
                'missing response-loss request ID. No resources were changed.'
            )
        if not ambiguous and (
            not resources.get(f'{prefix}_id')
            or not resources.get(f'{prefix}_self_link')
        ):
            raise RuntimeError(
                f'Refusing GCP cleanup because {prefix} lacks its immutable '
                'numeric ID or selfLink. No resources were changed.'
            )

    found: dict[str, Any] = {}
    for prefix in RESOURCE_PREFIXES:
        if resources.get(f'{prefix}_name'):
            found[prefix] = _get_cleanup_resource(prefix, resources, clients)
    poll_prefixes = [
        prefix
        for prefix in (
            'gcp_instance',
            'gcp_peer_instance',
            'gcp_loadgen_instance',
            'gcp_runner_boot_disk',
            'gcp_peer_boot_disk',
            'gcp_loadgen_boot_disk',
        )
        if (
            resources.get(f'{prefix}_name')
            and found.get(prefix) is None
            and resources.get(f'{prefix}_create_ambiguous') is True
        )
    ]
    if poll_prefixes:
        found.update(
            _bounded_cleanup_lookup(
                poll_prefixes,
                resources,
                clients,
                attempts=max(1, RECONCILIATION_ATTEMPTS - 1),
            )
        )

    # No cloud mutation is allowed until every currently visible exact-name
    # resource has passed identity, ownership, and relationship verification.
    _verify_cleanup_resources(job, found)
    for prefix, resource in found.items():
        if resource is not None:
            _persist_recovered_identity(job, persist, prefix, resource)

    if not preserve_status:
        job['status'] = 'destroying'
        _persist(job, persist)
    _emit(job, emit, 'Destroy', 'Removing GCP benchmark infrastructure.')

    # Reconcile response-lost creates after the read-only verification phase.
    # Instances are intentionally not replayed because SSH public keys are not
    # persisted; parent deletion later proves any unresolved instance absent.
    for prefix in (
        'gcp_network',
        'gcp_subnet',
        'gcp_ssh_firewall',
        'gcp_iperf_firewall',
        'gcp_web_firewall',
        'gcp_data_disk',
    ):
        if (
            resources.get(f'{prefix}_name')
            and found.get(prefix) is None
            and resources.get(f'{prefix}_create_ambiguous') is True
        ):
            try:
                recovered = _replay_ambiguous_create(
                    job, persist, prefix, found, clients
                )
                found[prefix] = recovered
                _verify_cleanup_resources(job, found)
            except Exception as exc:
                if resources.get(f'{prefix}_name'):
                    _record(
                        job,
                        persist,
                        **{
                            f'{prefix}_reconciliation_error': (
                                f'Unable to reconcile response-lost GCP '
                                f'{prefix.removeprefix("gcp_").replace("_", " ")}: '
                                f'{exc}'
                            ),
                        },
                    )
                else:
                    _clear_contract(job, persist, prefix)

    for prefix in (
        'gcp_instance',
        'gcp_peer_instance',
        'gcp_loadgen_instance',
    ):
        if (
            resources.get(f'{prefix}_name')
            and found.get(prefix) is None
            and resources.get(f'{prefix}_create_ambiguous') is True
        ):
            _record(
                job,
                persist,
                **{
                    f'{prefix}_reconciliation_error': (
                        'The response-lost GCP instance is not visible yet; '
                        'cleanup will remove its recorded dependencies and '
                        'retain this contract unless VPC deletion proves it absent.'
                    )
                },
            )

    errors: list[str] = []
    delete_order = (
        'gcp_loadgen_instance',
        'gcp_peer_instance',
        'gcp_instance',
        'gcp_data_disk',
        'gcp_loadgen_boot_disk',
        'gcp_peer_boot_disk',
        'gcp_runner_boot_disk',
        'gcp_web_firewall',
        'gcp_iperf_firewall',
        'gcp_ssh_firewall',
        'gcp_subnet',
        'gcp_network',
    )
    network_absent = resources.get('gcp_network_absence_confirmed') is True
    for prefix in delete_order:
        name = resources.get(f'{prefix}_name')
        if not name:
            continue
        resource = found.get(prefix)
        unresolved = (
            resource is None
            and resources.get(f'{prefix}_create_ambiguous') is True
        )
        if unresolved:
            continue
        if resource is None:
            # The read-only preflight proved this exact name absent and there
            # is no outstanding create. Do not issue a name-only delete that
            # could race with an unrelated same-name resource created later.
            if prefix == 'gcp_network':
                network_absent = True
                _record(
                    job,
                    persist,
                    gcp_network_absence_confirmed=True,
                )
            _clear_contract(job, persist, prefix)
            continue
        try:
            # Re-read immediately before a name-only Compute delete. Numeric ID
            # and ownership verification catches replacement after preflight.
            current = _get_cleanup_resource(prefix, resources, clients)
            if current is None:
                if prefix == 'gcp_network':
                    network_absent = True
                    _record(
                        job,
                        persist,
                        gcp_network_absence_confirmed=True,
                    )
                _clear_contract(job, persist, prefix)
                continue
            _verify_cleanup_resources(job, {prefix: current})
            _delete_resource(job, persist, prefix, clients)
            if prefix == 'gcp_network':
                network_absent = True
                _record(
                    job,
                    persist,
                    gcp_network_absence_confirmed=True,
                )
            _clear_contract(job, persist, prefix)
            _emit(
                job,
                emit,
                'Destroy',
                f'Deleted GCP {prefix.removeprefix("gcp_").replace("_", " ")} '
                f'{name}.',
            )
        except Exception as exc:
            errors.append(
                f'Unable to delete GCP '
                f'{prefix.removeprefix("gcp_").replace("_", " ")} {name}: {exc}'
            )

    if network_absent:
        # No instance/subnet/firewall tied to this custom VPC can survive its
        # successful deletion. Clear missing response-lost child contracts,
        # but keep independent disk names until an exact final sweep.
        for prefix in (
            'gcp_loadgen_instance',
            'gcp_peer_instance',
            'gcp_instance',
            'gcp_web_firewall',
            'gcp_iperf_firewall',
            'gcp_ssh_firewall',
            'gcp_subnet',
        ):
            if found.get(prefix) is None:
                _clear_contract(job, persist, prefix)
        boot_prefixes = [
            prefix
            for prefix in (
                'gcp_loadgen_boot_disk',
                'gcp_peer_boot_disk',
                'gcp_runner_boot_disk',
            )
            if resources.get(f'{prefix}_name')
        ]
        final_boots = _bounded_cleanup_lookup(
            boot_prefixes, resources, clients
        ) if boot_prefixes else {}
        for prefix in boot_prefixes:
            try:
                disk = final_boots.get(prefix)
                if disk is not None:
                    _verify_cleanup_resources(job, {prefix: disk})
                    _persist_recovered_identity(job, persist, prefix, disk)
                    current = _get_cleanup_resource(
                        prefix, resources, clients
                    )
                    if current is not None:
                        _verify_cleanup_resources(job, {prefix: current})
                        _delete_resource(job, persist, prefix, clients)
                _clear_contract(job, persist, prefix)
            except Exception as exc:
                errors.append(
                    f'Unable to reconcile GCP orphan boot disk '
                    f'{resources.get(f"{prefix}_name")}: {exc}'
                )

    # Surface reconciliation problems only after all independent recorded
    # resources have had a cleanup attempt. Keeping their contracts makes the
    # next cleanup invocation retryable instead of silently leaking resources.
    for prefix in RESOURCE_PREFIXES:
        error = resources.get(f'{prefix}_reconciliation_error')
        if error:
            errors.append(str(error))
    remaining = _remaining_resource_contracts(resources)
    if remaining:
        errors.append(
            'GCP cleanup retained unresolved managed-resource contracts for: '
            + ', '.join(prefix.removeprefix('gcp_') for prefix in remaining)
            + '. Retry cleanup.'
        )
    if errors:
        raise RuntimeError('; '.join(dict.fromkeys(errors)))

    _forget(
        job,
        persist,
        'gcp_resource_prefix',
        'gcp_runner_tag',
        'gcp_peer_tag',
        'gcp_loadgen_tag',
        'gcp_iperf3_protocols',
        'gcp_web_benchmarks',
        'gcp_network_absence_confirmed',
    )
    if not preserve_status:
        job['status'] = 'destroyed'
        job['cleanup_error'] = None
        _persist(job, persist)
        _emit(job, emit, 'Complete', 'GCP infrastructure was destroyed.')
    return resources


cleanup = destroy_resources


__all__ = [
    'DEFAULT_DISK_TYPE',
    'DEFAULT_REGION',
    'DISTRIBUTED_PRIVATE_ADDRESSES',
    'DISTRIBUTED_ROLE_MACHINE_TYPES',
    'HYPERDISK_BALANCED_IOPS',
    'HYPERDISK_BALANCED_THROUGHPUT_MIBPS',
    'HYPERDISK_BALANCED_TYPE',
    'IMAGE_FAMILIES',
    'IMAGE_PROJECT',
    'LOADGEN_ARCHITECTURE',
    'LOADGEN_BOOT_SIZE_GB',
    'LOADGEN_DISK_TYPE',
    'LOADGEN_MACHINE_TYPE',
    'LOADGEN_MEMORY_GB',
    'LOADGEN_NETWORK_BANDWIDTH_GBPS',
    'LOADGEN_VCPUS',
    'MANAGED_BY',
    'SSH_SOURCE_CIDR',
    'SSH_USER',
    'bootstrap',
    'cleanup',
    'create_clients',
    'latest_rocky_linux_9_image',
    'machine_types',
    'normalize_architecture',
    'placement',
    'provision',
    'provision_distributed_deathstarbench_candidate',
    'destroy_resources',
]
