"""Provider descriptors and fail-closed runtime dispatch.

The registry is deliberately independent of the cloud SDK implementations.
That keeps provider discovery and capability metadata in one place while the
orchestrator supplies the operation handlers that are appropriate for each
call.  A provider must be both registered here and explicitly wired for an
operation before that operation can run.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, TypeVar


DEFAULT_PROVIDER_ID = 'oci'


class UnsupportedProviderError(ValueError):
    """Raised before an unknown provider can reach a cloud operation."""


class ProviderOperationUnavailableError(RuntimeError):
    """Raised when a registered provider has no explicit operation handler."""


@dataclass(frozen=True, slots=True)
class ProviderCapabilities:
    benchmark_ids: tuple[str, ...]
    llm_benchmark_ids: tuple[str, ...]
    sysbench_workloads: tuple[str, ...]
    iperf3_protocols: tuple[str, ...]
    additional_volume: bool
    private_peer: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            'benchmarks': list(self.benchmark_ids),
            'llm_benchmarks': list(self.llm_benchmark_ids),
            'sysbench_workloads': list(self.sysbench_workloads),
            'iperf3_protocols': list(self.iperf3_protocols),
            'additional_volume': self.additional_volume,
            'private_peer': self.private_peer,
        }


@dataclass(frozen=True, slots=True)
class ProviderAdapter:
    id: str
    name: str
    short_name: str
    ssh_user: str
    provisioning_message: str
    capabilities: ProviderCapabilities

    def as_dict(self) -> dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'short_name': self.short_name,
            'ssh_user': self.ssh_user,
            'capabilities': self.capabilities.as_dict(),
        }


_PROVIDERS = (
    ProviderAdapter(
        id='oci',
        name='Oracle Cloud Infrastructure',
        short_name='OCI',
        ssh_user='opc',
        provisioning_message=(
            'Creating VCN, gateway, subnet, and security rules.'
        ),
        capabilities=ProviderCapabilities(
            benchmark_ids=(
                'deathstarbench',
                'apachebench',
                'sysbench',
                'phoronix',
                'stream',
                'iperf3',
                'fio',
            ),
            llm_benchmark_ids=('llama_bench',),
            sysbench_workloads=('cpu', 'memory', 'fileio'),
            iperf3_protocols=('tcp', 'udp', 'sctp'),
            additional_volume=True,
            private_peer=True,
        ),
    ),
    ProviderAdapter(
        id='aws',
        name='Amazon Web Services',
        short_name='AWS',
        ssh_user='ec2-user',
        provisioning_message=(
            'Creating an AWS VPC, public subnet, internet gateway, and '
            'security group.'
        ),
        capabilities=ProviderCapabilities(
            benchmark_ids=(
                'deathstarbench',
                'apachebench',
                'sysbench',
                'stream',
                'phoronix',
                'iperf3',
                'fio',
            ),
            llm_benchmark_ids=('llama_bench',),
            sysbench_workloads=('cpu', 'memory', 'fileio'),
            iperf3_protocols=('tcp', 'udp', 'sctp'),
            additional_volume=True,
            private_peer=True,
        ),
    ),
    ProviderAdapter(
        id='gcp',
        name='Google Cloud',
        short_name='GCP',
        ssh_user='benchmark',
        provisioning_message=(
            'Creating a Google Cloud VPC network, subnet, firewall rules, '
            'and Compute Engine instances.'
        ),
        capabilities=ProviderCapabilities(
            benchmark_ids=(
                'deathstarbench',
                'apachebench',
                'sysbench',
                'stream',
                'phoronix',
                'iperf3',
                'fio',
            ),
            llm_benchmark_ids=('llama_bench',),
            sysbench_workloads=('cpu', 'memory', 'fileio'),
            iperf3_protocols=('tcp', 'udp', 'sctp'),
            additional_volume=True,
            private_peer=True,
        ),
    ),
    ProviderAdapter(
        id='azure',
        name='Microsoft Azure',
        short_name='Azure',
        ssh_user='benchmark',
        provisioning_message=(
            'Creating an Azure resource group, virtual network, subnet, '
            'network security group, public IPs, and virtual machines.'
        ),
        capabilities=ProviderCapabilities(
            benchmark_ids=(
                'deathstarbench',
                'apachebench',
                'sysbench',
                'stream',
                'phoronix',
                'iperf3',
                'fio',
            ),
            llm_benchmark_ids=('llama_bench',),
            sysbench_workloads=('cpu', 'memory', 'fileio'),
            iperf3_protocols=('tcp', 'udp'),
            additional_volume=True,
            private_peer=True,
        ),
    ),
)
_PROVIDERS_BY_ID = {provider.id: provider for provider in _PROVIDERS}


def providers() -> tuple[ProviderAdapter, ...]:
    """Return registered providers in their stable presentation order."""
    return _PROVIDERS


def provider_id(value: Any) -> str:
    """Resolve a provider ID from a plan, saved plan mapping, or raw ID.

    Plans written before provider selection existed omit the field and remain
    OCI plans.  Explicit null, blank, and unknown values are rejected instead
    of falling through to OCI.
    """
    if isinstance(value, str):
        raw = value
    elif isinstance(value, Mapping):
        raw = value.get('provider', DEFAULT_PROVIDER_ID)
    else:
        raw = getattr(value, 'provider', DEFAULT_PROVIDER_ID)

    normalized = str(raw).strip().lower() if raw is not None else ''
    if normalized not in _PROVIDERS_BY_ID:
        supported = ', '.join(_PROVIDERS_BY_ID)
        label = normalized or '<empty>'
        raise UnsupportedProviderError(
            f'Unsupported cloud provider {label!r}; supported providers: '
            f'{supported}.'
        )
    return normalized


def provider_adapter(value: Any) -> ProviderAdapter:
    return _PROVIDERS_BY_ID[provider_id(value)]


_Result = TypeVar('_Result')


def dispatch_provider_operation(
    value: Any,
    operation: str,
    handlers: Mapping[str, Callable[[], _Result]],
) -> _Result:
    """Invoke only an explicitly wired handler for a registered provider."""
    adapter = provider_adapter(value)
    handler = handlers.get(adapter.id)
    if handler is None:
        raise ProviderOperationUnavailableError(
            f'{adapter.name} is registered but does not implement the '
            f'{operation} operation.'
        )
    return handler()
