"""Versioned DeathStarBench topology and orchestration-runtime contracts.

Benchmark runs must never resolve a moving Kubernetes release channel.  A
released profile names every topology and runtime component that can affect
the result, and historical profiles remain readable after the default moves
forward.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType


SINGLE_HOST_TOPOLOGY_ID = 'single_host_v1'
DISTRIBUTED_TIERED_TOPOLOGY_ID = 'distributed_tiered_v1'
PODMAN_COMPOSE_RUNTIME_ID = 'podman_compose_v1'
K3S_RUNTIME_ID = 'k3s_v1'
K3S_RUNTIME_JOURNAL_KEY = 'k3s_runtime_state_v1'
DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY = 'deathstarbench_workload_state_v1'

# The current Podman implementation intentionally uses provider-packaged
# Podman on Oracle/Rocky Linux and a separately qualified package set on Amazon
# Linux.  This marker identifies the portable benchmark method, rather than
# pretending those provider packages are one identical binary build.
SINGLE_HOST_RUNTIME_REVISION = 'legacy-podman-compose-runtime-v1'

# Candidate distributed runtime.  It is deliberately exact and is not
# selected through K3s's moving ``stable`` channel.  ``released=False`` keeps
# it out of cloud provisioning until the multi-node lifecycle is complete and
# qualified on every provider.
K3S_VERSION = 'v1.36.4+k3s1'
DISTRIBUTED_RUNTIME_REVISION = 'k3s-v1.36.4-k3s1-tiered-runtime-v5'
DISTRIBUTED_WORKLOAD_REVISION = 'social-network-6ecb097-workload-v1'
DISTRIBUTED_IMAGE_SET_REVISION = 'social-network-images-v1'


@dataclass(frozen=True)
class DeathStarBenchRuntimeProfile:
    """One immutable, auditable way to deploy a benchmark topology."""

    topology_id: str
    topology_revision: str
    runtime_id: str
    runtime_revision: str
    label: str
    roles: tuple[str, ...]
    node_count: int
    creation_order: tuple[str, ...]
    deletion_order: tuple[str, ...]
    orchestrator: str
    orchestrator_version: str | None
    placement_revision: str
    released: bool

    def metadata(self) -> dict[str, object]:
        """Return stable result metadata without mutable implementation state."""

        return {
            'topology_id': self.topology_id,
            'topology_revision': self.topology_revision,
            'runtime_id': self.runtime_id,
            'runtime_revision': self.runtime_revision,
            'topology_label': self.label,
            'topology_node_count': self.node_count,
            'topology_roles': ', '.join(self.roles),
            'topology_creation_order': ', '.join(self.creation_order),
            'topology_deletion_order': ', '.join(self.deletion_order),
            'service_placement_revision': self.placement_revision,
            'orchestrator': self.orchestrator,
            'orchestrator_version': self.orchestrator_version or 'provider-qualified',
        }


SINGLE_HOST_PROFILE = DeathStarBenchRuntimeProfile(
    topology_id=SINGLE_HOST_TOPOLOGY_ID,
    topology_revision='single-host-placement-v1',
    runtime_id=PODMAN_COMPOSE_RUNTIME_ID,
    runtime_revision=SINGLE_HOST_RUNTIME_REVISION,
    label='Compact / single-host',
    roles=('runner', 'load_generator'),
    node_count=2,
    creation_order=('runner', 'load-generator'),
    deletion_order=('load-generator', 'runner'),
    orchestrator='Podman with podman-compose',
    orchestrator_version=None,
    placement_revision='all-workload-services-on-runner-v1',
    released=True,
)

DISTRIBUTED_TIERED_PROFILE = DeathStarBenchRuntimeProfile(
    topology_id=DISTRIBUTED_TIERED_TOPOLOGY_ID,
    topology_revision='distributed-tiered-placement-v1',
    runtime_id=K3S_RUNTIME_ID,
    runtime_revision=DISTRIBUTED_RUNTIME_REVISION,
    label='Distributed tiered',
    roles=('control', 'database', 'cache', 'application', 'load_generator'),
    node_count=5,
    creation_order=(
        'control',
        'database',
        'cache',
        'application',
        'load-generator',
    ),
    deletion_order=(
        'load-generator',
        'application',
        'cache',
        'database',
        'control',
    ),
    orchestrator='K3s/containerd',
    orchestrator_version=K3S_VERSION,
    placement_revision='frontend-app-cache-database-v1',
    released=False,
)

RUNTIME_PROFILES = MappingProxyType({
    (profile.topology_id, profile.runtime_id): profile
    for profile in (SINGLE_HOST_PROFILE, DISTRIBUTED_TIERED_PROFILE)
})


def runtime_profile(
    topology_id: str,
    runtime_id: str,
) -> DeathStarBenchRuntimeProfile:
    """Resolve an exact profile or fail closed on an unknown pair."""

    try:
        return RUNTIME_PROFILES[(str(topology_id), str(runtime_id))]
    except KeyError:
        raise ValueError(
            'Unsupported DeathStarBench topology/runtime contract: '
            f'{topology_id}/{runtime_id}.'
        ) from None


def require_released_runtime(topology_id: str, runtime_id: str):
    """Reject a modeled profile until its complete cloud lifecycle is ready."""

    profile = runtime_profile(topology_id, runtime_id)
    if not profile.released:
        raise ValueError(
            f'DeathStarBench {profile.label} is not released yet; its '
            'multi-node provisioning and cleanup path is still being '
            'qualified. Use Compact / single-host for runnable plans.'
        )
    return profile
