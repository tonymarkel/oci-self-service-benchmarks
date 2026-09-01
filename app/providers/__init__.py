"""Cloud-provider adapters used by the benchmark orchestrator."""

from . import aws, azure, gcp
from .registry import (
    ProviderAdapter,
    ProviderCapabilities,
    ProviderOperationUnavailableError,
    UnsupportedProviderError,
    dispatch_provider_operation,
    provider_adapter,
    provider_id,
    providers,
)

__all__ = [
    'ProviderAdapter',
    'ProviderCapabilities',
    'ProviderOperationUnavailableError',
    'UnsupportedProviderError',
    'aws',
    'azure',
    'gcp',
    'dispatch_provider_operation',
    'provider_adapter',
    'provider_id',
    'providers',
]
