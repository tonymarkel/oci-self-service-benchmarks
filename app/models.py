from pydantic import BaseModel, Field, StrictBool, model_validator
from typing import Literal

from .deathstarbench_contract import (
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    K3S_RUNTIME_ID,
    PODMAN_COMPOSE_RUNTIME_ID,
    RUNTIME_PROFILES,
    SINGLE_HOST_TOPOLOGY_ID,
)


LEGACY_SYSBENCH_WORKLOADS = {
    'sysbench_cpu': 'cpu',
    'sysbench_memory': 'memory',
    'sysbench_fileio': 'fileio',
}
SYSBENCH_WORKLOAD_ORDER = ('cpu', 'memory', 'fileio')
LEGACY_IPERF3_PROTOCOLS = {
    'iperf_tcp': 'tcp',
    'iperf_udp': 'udp',
    'iperf_sctp': 'sctp',
}
IPERF3_PROTOCOL_ORDER = ('tcp', 'udp', 'sctp')
LEGACY_BASELINE_COMPONENTS = ('sysbench', 'stream', 'fio')
SUPPORTED_BENCHMARKS = frozenset({
    'deathstarbench',
    'apachebench',
    'sysbench',
    'stream',
    'fio',
    'iperf3',
    'phoronix',
})
SUPPORTED_LLM_BENCHMARKS = frozenset({'llama_bench'})

DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID = SINGLE_HOST_TOPOLOGY_ID
DEATHSTARBENCH_DISTRIBUTED_TIERED_TOPOLOGY_ID = (
    DISTRIBUTED_TIERED_TOPOLOGY_ID
)
DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID = PODMAN_COMPOSE_RUNTIME_ID
DEATHSTARBENCH_K3S_RUNTIME_ID = K3S_RUNTIME_ID
DEATHSTARBENCH_DEFAULT_TOPOLOGY_ID = DEATHSTARBENCH_SINGLE_HOST_TOPOLOGY_ID
DEATHSTARBENCH_DEFAULT_RUNTIME_ID = DEATHSTARBENCH_PODMAN_COMPOSE_RUNTIME_ID
DEATHSTARBENCH_TOPOLOGY_RUNTIME_PAIRS = frozenset(RUNTIME_PROFILES)


class ClearSavedRunsRequest(BaseModel):
    """Explicit confirmation required before deleting local run history."""

    confirmed: StrictBool

    @model_validator(mode='after')
    def require_confirmation(self):
        if not self.confirmed:
            raise ValueError('Explicit confirmation is required.')
        return self


def canonicalize_baseline_plan(values):
    if not isinstance(values, dict):
        return values
    benchmarks = list(values.get('benchmarks') or [])
    if 'baseline' not in benchmarks:
        return values

    migrated = dict(values)
    canonical_benchmarks = []
    for benchmark in benchmarks:
        replacements = (
            LEGACY_BASELINE_COMPONENTS
            if benchmark == 'baseline'
            else (benchmark,)
        )
        for replacement in replacements:
            if replacement not in canonical_benchmarks:
                canonical_benchmarks.append(replacement)
    migrated['benchmarks'] = canonical_benchmarks

    options = dict(values.get('sysbench') or {})
    selected = set(options.get('workloads') or [])
    selected.update(('cpu', 'memory'))
    options['workloads'] = [
        workload for workload in SYSBENCH_WORKLOAD_ORDER
        if workload in selected
    ]
    migrated['sysbench'] = options
    return migrated


def canonicalize_sysbench_plan(values):
    if not isinstance(values, dict):
        return values
    benchmarks = list(values.get('benchmarks') or [])
    legacy = {
        LEGACY_SYSBENCH_WORKLOADS[item]
        for item in benchmarks
        if item in LEGACY_SYSBENCH_WORKLOADS
    }
    if not legacy:
        return values

    migrated = dict(values)
    canonical_benchmarks = []
    sysbench_added = False
    for benchmark in benchmarks:
        if benchmark in LEGACY_SYSBENCH_WORKLOADS or benchmark == 'sysbench':
            if not sysbench_added:
                canonical_benchmarks.append('sysbench')
                sysbench_added = True
            continue
        canonical_benchmarks.append(benchmark)
    migrated['benchmarks'] = canonical_benchmarks

    options = dict(values.get('sysbench') or {})
    selected = set(options.get('workloads') or []) | legacy
    options['workloads'] = [
        workload for workload in SYSBENCH_WORKLOAD_ORDER
        if workload in selected
    ]
    migrated['sysbench'] = options
    return migrated


def canonicalize_iperf3_plan(values):
    if not isinstance(values, dict):
        return values
    benchmarks = list(values.get('benchmarks') or [])
    legacy = {
        LEGACY_IPERF3_PROTOCOLS[item]
        for item in benchmarks
        if item in LEGACY_IPERF3_PROTOCOLS
    }
    if not legacy:
        return values

    migrated = dict(values)
    canonical_benchmarks = []
    iperf3_added = False
    for benchmark in benchmarks:
        if benchmark in LEGACY_IPERF3_PROTOCOLS or benchmark == 'iperf3':
            if not iperf3_added:
                canonical_benchmarks.append('iperf3')
                iperf3_added = True
            continue
        canonical_benchmarks.append(benchmark)
    migrated['benchmarks'] = canonical_benchmarks

    options = dict(values.get('iperf3') or {})
    selected = set(options.get('protocols') or []) | legacy
    options['protocols'] = [
        protocol for protocol in IPERF3_PROTOCOL_ORDER
        if protocol in selected
    ]
    migrated['iperf3'] = options
    return migrated


def canonicalize_benchmark_plan(values):
    values = canonicalize_baseline_plan(values)
    values = canonicalize_sysbench_plan(values)
    return canonicalize_iperf3_plan(values)


def canonicalize_provider_plan(values):
    """Supply safe provider defaults while keeping legacy OCI plans valid."""
    if not isinstance(values, dict):
        return values
    provider = values.get('provider', 'oci')
    if provider not in {'aws', 'gcp', 'azure'}:
        return values

    migrated = dict(values)
    compatibility_type = {
        'aws': migrated.get('instance_type'),
        'gcp': migrated.get('machine_type'),
        'azure': migrated.get('vm_size'),
    }[provider]
    if not migrated.get('shape') and compatibility_type:
        migrated['shape'] = compatibility_type
    if 'storage' not in migrated:
        migrated['storage'] = {'additional_volume': False}
    return migrated


class SecurityOptions(BaseModel):
    mode: Literal['none', 'shielded', 'confidential'] = 'none'
    secure_boot: bool = False
    measured_boot: bool = False
    trusted_platform_module: bool = False

class StorageOptions(BaseModel):
    boot_size_gb: int = Field(100, ge=50, le=32768)
    boot_performance: int = Field(10, ge=10, le=120)
    additional_volume: bool = True
    additional_size_gb: int = Field(1024, ge=50, le=32768)
    additional_performance: int = Field(10, ge=10, le=120)
    mount_style: Literal['paravirtualized', 'iscsi'] = 'paravirtualized'

    @model_validator(mode='before')
    @classmethod
    def migrate_legacy_nvme_attachment(cls, values):
        """Normalize the former, non-functional NVMe Block Volume option."""

        if isinstance(values, dict) and values.get('mount_style') == 'nvme':
            migrated = dict(values)
            migrated['mount_style'] = 'paravirtualized'
            return migrated
        return values


class DeathStarBenchOptions(BaseModel):
    topology_id: Literal[
        'single_host_v1',
        'distributed_tiered_v1',
    ] = Field(DEATHSTARBENCH_DEFAULT_TOPOLOGY_ID, frozen=True)
    runtime_id: Literal[
        'podman_compose_v1',
        'k3s_v1',
    ] = Field(DEATHSTARBENCH_DEFAULT_RUNTIME_ID, frozen=True)
    workload: Literal[
        'media_microservices',
        'hotel_reservation',
        'social_network',
    ] = 'media_microservices'
    warmup_seconds: int = Field(30, ge=0, le=600)
    duration_seconds: int = Field(60, ge=10, le=1800)
    threads: int = Field(4, ge=1, le=64)
    connections: int = Field(64, ge=1, le=4096)
    request_rate: int = Field(100, ge=1, le=100000)

    @model_validator(mode='after')
    def validate_topology_runtime_pair(self):
        pair = (self.topology_id, self.runtime_id)
        if pair not in DEATHSTARBENCH_TOPOLOGY_RUNTIME_PAIRS:
            raise ValueError(
                'DeathStarBench topology and runtime must be a supported '
                'versioned pair.'
            )
        return self


class ApacheBenchOptions(BaseModel):
    workloads: list[Literal['new_connections', 'keep_alive']] = Field(
        default_factory=lambda: ['new_connections', 'keep_alive']
    )
    request_count: int = Field(500000, ge=1, le=100000000)
    concurrency: int = Field(100, ge=1, le=10000)
    response_size_kib: int = Field(64, ge=1, le=1024)
    warmup_requests: int = Field(10000, ge=0, le=10000000)
    trials: int = Field(3, ge=1, le=10)

    @model_validator(mode='after')
    def validate_options(self):
        if len(self.workloads) != len(set(self.workloads)):
            raise ValueError('ApacheBench connection modes must be unique.')
        if self.concurrency > self.request_count:
            raise ValueError(
                'ApacheBench concurrency cannot exceed the measured request '
                'count.'
            )
        return self


class SysbenchOptions(BaseModel):
    workloads: list[Literal['cpu', 'memory', 'fileio']] = Field(
        default_factory=lambda: ['cpu']
    )

    @model_validator(mode='after')
    def validate_unique_workloads(self):
        if len(self.workloads) != len(set(self.workloads)):
            raise ValueError('Sysbench workloads must be unique.')
        return self


class Iperf3Options(BaseModel):
    protocols: list[Literal['tcp', 'udp', 'sctp']] = Field(
        default_factory=lambda: ['tcp']
    )

    @model_validator(mode='after')
    def validate_unique_protocols(self):
        if len(self.protocols) != len(set(self.protocols)):
            raise ValueError('iperf3 protocols must be unique.')
        return self


class PhoronixOptions(BaseModel):
    profiles: list[Literal[
        'compress_7zip',
        'openssl',
        'build_linux_kernel',
        'tinymembench',
    ]] = Field(default_factory=lambda: ['compress_7zip'])

    @model_validator(mode='after')
    def validate_unique_profiles(self):
        if len(self.profiles) != len(set(self.profiles)):
            raise ValueError('Phoronix profiles must be unique.')
        return self


class BenchmarkPlan(BaseModel):
    provider: Literal['oci', 'aws', 'gcp', 'azure'] = 'oci'
    aws_profile: str = Field('default', min_length=1)
    gcp_project_id: str | None = None
    gcp_zone: str | None = None
    azure_subscription_id: str | None = None
    azure_zone: str | None = None
    region: str
    compartment_id: str | None = None
    availability_domain: str | None = None
    fault_domain: str | None = None
    shape: str
    ocpus: float = Field(8, ge=1)
    memory_gb: float = Field(32, ge=1)
    ssh_private_key: str = Field(min_length=1)
    ssh_public_key: str = Field(min_length=1)
    ssh_key_passphrase: str | None = None
    security: SecurityOptions = Field(default_factory=SecurityOptions)
    networking: Literal['paravirtualized', 'sriov'] = 'paravirtualized'
    storage: StorageOptions = Field(default_factory=StorageOptions)
    deathstarbench: DeathStarBenchOptions = Field(
        default_factory=DeathStarBenchOptions
    )
    apachebench: ApacheBenchOptions = Field(
        default_factory=ApacheBenchOptions
    )
    sysbench: SysbenchOptions = Field(default_factory=SysbenchOptions)
    iperf3: Iperf3Options = Field(default_factory=Iperf3Options)
    phoronix: PhoronixOptions = Field(default_factory=PhoronixOptions)
    destroy_after_completion: bool = True
    benchmarks: list[str] = Field(default_factory=list)
    llm_benchmarks: list[str] = Field(default_factory=list)

    @model_validator(mode='before')
    @classmethod
    def migrate_legacy_benchmark_options(cls, values):
        return canonicalize_benchmark_plan(canonicalize_provider_plan(values))

    @model_validator(mode='after')
    def validate_plan(self):
        if not self.benchmarks and not self.llm_benchmarks:
            raise ValueError('Select at least one benchmark.')
        unsupported = sorted(set(self.benchmarks) - SUPPORTED_BENCHMARKS)
        if unsupported:
            raise ValueError(
                f'{self.provider.upper()} does not support the selected '
                'benchmark: ' + ', '.join(unsupported)
            )
        unsupported_llm = sorted(
            set(self.llm_benchmarks) - SUPPORTED_LLM_BENCHMARKS
        )
        if unsupported_llm:
            raise ValueError(
                f'{self.provider.upper()} does not support the selected LLM '
                'benchmark: ' + ', '.join(unsupported_llm)
            )
        if self.security.mode != 'shielded' and any([self.security.secure_boot, self.security.measured_boot, self.security.trusted_platform_module]):
            raise ValueError('Shielded options require Shielded security mode.')
        if 'sysbench' in self.benchmarks and not self.sysbench.workloads:
            raise ValueError('Select at least one Sysbench workload.')
        if 'iperf3' in self.benchmarks and not self.iperf3.protocols:
            raise ValueError('Select at least one iperf3 protocol.')
        if 'phoronix' in self.benchmarks and not self.phoronix.profiles:
            raise ValueError('Select at least one Phoronix profile.')
        if 'apachebench' in self.benchmarks and not self.apachebench.workloads:
            raise ValueError(
                'Select at least one ApacheBench connection mode.'
            )
        sysbench_fileio_selected = (
            'sysbench_fileio' in self.benchmarks
            or (
                'sysbench' in self.benchmarks
                and 'fileio' in self.sysbench.workloads
            )
        )
        storage_benchmark_selected = (
            sysbench_fileio_selected or 'fio' in self.benchmarks
        )
        if storage_benchmark_selected and not self.storage.additional_volume:
            raise ValueError(
                'fio and Sysbench file I/O require the additional /data '
                'volume.'
            )
        if 'deathstarbench' in self.benchmarks:
            if self.deathstarbench.connections < self.deathstarbench.threads:
                raise ValueError(
                    'DeathStarBench connections must be greater than or equal '
                    'to its worker threads.'
                )
            if self.deathstarbench.request_rate < self.deathstarbench.threads:
                raise ValueError(
                    'DeathStarBench request rate must be greater than or equal '
                    'to its worker threads.'
                )
            if self.deathstarbench.connections % self.deathstarbench.threads:
                raise ValueError(
                    'DeathStarBench connections must be evenly divisible by '
                    'its worker threads.'
                )
            if self.deathstarbench.request_rate % self.deathstarbench.threads:
                raise ValueError(
                    'DeathStarBench request rate must be evenly divisible by '
                    'its worker threads.'
                )
            if self.memory_gb < 16:
                raise ValueError(
                    'DeathStarBench requires at least 16 GB of memory for its '
                    'microservice containers.'
                )
        if self.provider == 'aws':
            if self.security.mode != 'none':
                raise ValueError(
                    'OCI security options are not applicable to AWS plans.'
                )
            if self.networking != 'paravirtualized':
                raise ValueError(
                    'OCI networking options are not applicable to AWS plans.'
                )
        if self.provider == 'gcp':
            if not (self.gcp_project_id or '').strip():
                raise ValueError(
                    'A Google Cloud project ID is required for GCP plans.'
                )
            if not (self.gcp_zone or '').strip():
                raise ValueError(
                    'A Google Cloud zone is required for GCP plans.'
                )
            if self.security.mode != 'none':
                raise ValueError(
                    'OCI security options are not applicable to GCP plans.'
                )
            if self.networking != 'paravirtualized':
                raise ValueError(
                    'OCI networking options are not applicable to GCP plans.'
                )
        if self.provider == 'azure':
            if not (self.azure_subscription_id or '').strip():
                raise ValueError(
                    'An Azure subscription ID is required for Azure plans.'
                )
            if not (self.azure_zone or '').strip():
                raise ValueError(
                    'An Azure availability zone is required for Azure plans.'
                )
            if self.security.mode != 'none':
                raise ValueError(
                    'OCI security options are not applicable to Azure plans.'
                )
            if self.networking != 'paravirtualized':
                raise ValueError(
                    'OCI networking options are not applicable to Azure plans.'
                )
            if 'sctp' in self.iperf3.protocols:
                raise ValueError(
                    'Azure Virtual Network does not support the SCTP iperf3 '
                    'protocol; select TCP or UDP.'
                )
        return self
