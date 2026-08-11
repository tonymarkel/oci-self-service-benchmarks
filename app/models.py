from pydantic import BaseModel, Field, model_validator
from typing import Literal

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
    mount_style: Literal['paravirtualized', 'iscsi', 'nvme'] = 'paravirtualized'


class DeathStarBenchOptions(BaseModel):
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


class BenchmarkPlan(BaseModel):
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
    destroy_after_completion: bool = True
    benchmarks: list[str] = Field(default_factory=list)
    llm_benchmarks: list[str] = Field(default_factory=list)

    @model_validator(mode='after')
    def validate_plan(self):
        if not self.benchmarks and not self.llm_benchmarks:
            raise ValueError('Select at least one benchmark.')
        if self.storage.mount_style == 'nvme' and not self.shape.startswith('BM.'):
            raise ValueError('NVMe data volume attachment is available only for bare metal shapes.')
        if self.security.mode != 'shielded' and any([self.security.secure_boot, self.security.measured_boot, self.security.trusted_platform_module]):
            raise ValueError('Shielded options require Shielded security mode.')
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
        return self
