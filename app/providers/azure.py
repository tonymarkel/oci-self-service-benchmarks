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
import json
import math
import re
import subprocess
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ..guests import rocky_linux


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

    def show(self, urn: str):
        return _AzureCliSubscriptions._run(
            'vm', 'image', 'terms', 'show', '--urn', urn
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
        'marketplace_terms': _AzureCliMarketplaceTerms(),
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
    del subscription_id
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
    del subscription_id
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


latest_rocky_linux_9_image = resolve_rocky_linux_9_image


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


def _get_or_none(service: Any, *args):
    try:
        return service.get(*args)
    except Exception as exc:
        if _not_found(exc):
            return None
        raise


def _verify_group(
    group: Any,
    *,
    subscription_id: str,
    name: str,
    region: str,
    required_tags: Mapping[str, str],
    saved_id: str,
):
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
    for key, expected in (
        (f'{prefix}_name', name),
        (f'{prefix}_expected_id', expected_id),
    ):
        current = resources.get(key)
        if current and _normalize_id(current) != _normalize_id(expected):
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
            f'{prefix}_create_ambiguous': True,
        },
    )
    operation = service.begin_create_or_update(resource_group, name, dict(body))
    resource = _wait(operation)
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
    _record(
        job,
        persist,
        **{
            f'{prefix}_id': actual_id,
            f'{prefix}_create_ambiguous': False,
        },
    )
    return resource


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


def _create_vm_role(
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
):
    job_id = str(job['id']).lower()
    pip_name = f'{name}-pip'
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
                        'privateIPAllocationMethod': 'Dynamic',
                        'privateIPAddressVersion': 'IPv4',
                        'primary': True,
                        'subnet': {'id': subnet_id},
                        'publicIPAddress': {'id': pip_id},
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
    refreshed_pip = _get_or_none(pip_service, resource_group, pip_name) or public_ip
    refreshed_nic = _get_or_none(nic_service, resource_group, nic_name) or nic
    public, private = _ip_values(refreshed_nic, refreshed_pip)
    if not public or not private:
        raise RuntimeError(
            f'Azure {role} became available without both public and private '
            'IPv4 addresses; the resource-group cleanup contract was retained.'
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
    benchmarks = tuple(_value(plan, 'benchmarks', ()) or ())
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
    _record(
        job,
        persist,
        azure_subnet_name=subnet_name,
        azure_subnet_expected_id=subnet_id,
        azure_subnet_create_ambiguous=True,
    )
    subnet = _wait(subnet_service.begin_create_or_update(
        group_name,
        vnet_name,
        subnet_name,
        {
            'properties': {
                'addressPrefix': SUBNET_CIDR,
                'networkSecurityGroup': {'id': nsg_id},
                'privateEndpointNetworkPolicies': 'Disabled',
            },
        },
    ))
    if _normalize_id(_value(subnet, 'id')) != _normalize_id(subnet_id):
        raise RuntimeError(
            'Azure returned a conflicting subnet identity; cleanup contract retained.'
        )
    _record(
        job,
        persist,
        azure_subnet_id=str(_value(subnet, 'id')),
        azure_subnet_create_ambiguous=False,
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
        _ensure_child(
            job, persist,
            prefix='azure_data_disk', name=data_disk_name,
            expected_id=data_disk_id,
            service=_require_service(
                _compute_service(clients, 'disks'), 'managed disks'
            ),
            resource_group=group_name, body=disk_body,
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


def _azure_contract_keys(resources: Mapping[str, Any]) -> list[str]:
    cloud_keys = [key for key in resources if key.startswith('azure_')]
    canonical = [
        'instance_id', 'public_ip', 'private_ip', 'peer_private_ip',
        'loadgen_instance_id', 'loadgen_public_ip', 'loadgen_private_ip',
        'data_volume_device',
    ]
    return cloud_keys + [key for key in canonical if key in resources]


def _verify_resource_group_inventory(
    clients: Mapping[str, Any],
    resources: Mapping[str, Any],
    *,
    subscription_id: str,
    resource_group: str,
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
    allowed = set()
    for key, value in resources.items():
        if key == 'azure_resource_group_expected_id':
            continue
        if not key.startswith('azure_') or not key.endswith('_expected_id'):
            continue
        normalized = _normalize_id(value)
        if not normalized.startswith(group_root + '/providers/'):
            raise RuntimeError(
                'Refusing Azure cleanup because a persisted managed-resource '
                'identity falls outside the exact benchmark resource group.'
            )
        allowed.add(normalized)
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
):
    """Delete one exact owned resource group after fail-closed verification."""
    resources = job.setdefault('resources', {})
    group_name = str(resources.get('azure_resource_group_name') or '')
    has_azure_contract = any(key.startswith('azure_') for key in resources)
    if not group_name:
        if has_azure_contract:
            raise RuntimeError(
                'Refusing Azure cleanup because managed resource metadata '
                'exists without the resource-group ownership anchor.'
            )
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
    subscription_id, _, clients = _runtime(
        subscription_id, credential, clients
    )
    groups = _require_service(_resource_groups(clients), 'resource groups')
    group = _get_or_none(groups, group_name)
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
            group = None
        else:
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
            )
            _record(job, persist, azure_resource_group_delete_ambiguous=True)
            try:
                _wait(groups.begin_delete(group_name))
            except Exception:
                # A lost polling response is successful only when exact-name
                # absence can be proven. Otherwise retain the entire contract.
                if _get_or_none(groups, group_name) is not None:
                    raise
            remaining = _get_or_none(groups, group_name)
            if remaining is not None:
                raise RuntimeError(
                    'Azure resource-group deletion completed without proving '
                    'absence; the cleanup contract was retained for retry.'
                )
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
