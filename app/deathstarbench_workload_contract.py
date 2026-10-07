"""Immutable workload identities for distributed DeathStarBench.

This registry is data, not a deployment or public-release gate. Social Network
retains its existing qualified identity. Hotel Reservation and Media
Microservices describe reviewed candidates only: no asset release flag can
make either candidate selectable. Executable bundle preflight remains required
for the existing Social Network lifecycle.

Resolve and pass a profile explicitly. There is deliberately no mutable
"current workload", so concurrent runs cannot exchange workload identities.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from .deathstarbench_contract import (
    DISTRIBUTED_DATASET_REVISION,
    DISTRIBUTED_IMAGE_SET_REVISION,
    DISTRIBUTED_LOAD_DRIVER_REVISION,
    DISTRIBUTED_MEASUREMENT_REVISION,
    DISTRIBUTED_WORKLOAD_REVISION,
)


ASSET_ROOT = Path(__file__).resolve().parent / 'manifests' / 'deathstarbench'
UPSTREAM_REVISION = '6ecb09706140f8730b5385c08f1386c654c3c526'


@dataclass(frozen=True, slots=True)
class DistributedDeathStarBenchWorkloadProfile:
    """One versioned workload, independent of cloud and application imports."""

    workload_id: str
    name: str
    namespace: str
    workload_label: str
    workload_revision: str
    image_set_revision: str
    dataset_revision: str
    load_driver_revision: str
    measurement_revision: str
    frontend_component: str
    frontend_container_port: int
    frontend_service_port: int
    frontend_node_port: int
    required_image_keys: tuple[str, ...]
    request_script_path: str
    request_script_sha256: str
    asset_directory: Path
    component_asset_sha256: str
    network_policy_asset_sha256: str
    expected_component_placement: Mapping[str, str]
    released: bool

    def __post_init__(self):
        # Copy before freezing: retaining a caller-owned mutable dictionary
        # would make a frozen dataclass only superficially immutable.
        object.__setattr__(
            self,
            'expected_component_placement',
            MappingProxyType(dict(sorted(self.expected_component_placement.items()))),
        )
        object.__setattr__(self, 'required_image_keys', tuple(self.required_image_keys))

    def metadata(self) -> dict[str, str | int]:
        """Return portable identity data without local paths or release state."""

        return {
            'workload_id': self.workload_id,
            'workload_name': self.name,
            'workload_namespace': self.namespace,
            'workload_label': self.workload_label,
            'workload_revision': self.workload_revision,
            'image_set_revision': self.image_set_revision,
            'dataset_revision': self.dataset_revision,
            'load_driver_revision': self.load_driver_revision,
            'measurement_revision': self.measurement_revision,
            'upstream_revision': UPSTREAM_REVISION,
            'request_script_path': self.request_script_path,
            'request_script_sha256': self.request_script_sha256,
            'component_asset_sha256': self.component_asset_sha256,
            'network_policy_asset_sha256': self.network_policy_asset_sha256,
            'frontend_component': self.frontend_component,
            'frontend_container_port': self.frontend_container_port,
            'frontend_service_port': self.frontend_service_port,
            'frontend_node_port': self.frontend_node_port,
        }


def _placement(**roles: tuple[str, ...]) -> Mapping[str, str]:
    return MappingProxyType({
        name: role
        for role, names in roles.items()
        for name in names
    })


SOCIAL_NETWORK_PROFILE = DistributedDeathStarBenchWorkloadProfile(
    workload_id='social_network',
    name='Social Network',
    namespace='deathstarbench-social',
    workload_label='social-network-v1',
    workload_revision=DISTRIBUTED_WORKLOAD_REVISION,
    image_set_revision=DISTRIBUTED_IMAGE_SET_REVISION,
    dataset_revision=DISTRIBUTED_DATASET_REVISION,
    load_driver_revision=DISTRIBUTED_LOAD_DRIVER_REVISION,
    measurement_revision=DISTRIBUTED_MEASUREMENT_REVISION,
    frontend_component='nginx-thrift',
    frontend_container_port=8080,
    frontend_service_port=8080,
    frontend_node_port=8080,
    required_image_keys=(
        'jaeger', 'media-frontend', 'memcached', 'mongodb', 'nginx-thrift',
        'redis', 'social-network-microservices',
    ),
    request_script_path='socialNetwork/wrk2/scripts/social-network/mixed-workload.lua',
    request_script_sha256=(
        'ab2cd04b6cffb53beaf27efd8dfb5eae7dcd6c8abecbb70623fda93139b3dd32'
    ),
    asset_directory=ASSET_ROOT / 'social-network-v1',
    component_asset_sha256=(
        '72e913cc5f638ca4c36fccbdb7458f58064f47749854429e8a1d171d9199de75'
    ),
    network_policy_asset_sha256=(
        '7d9647c5a96884863bb03cdcc446e66a5523d0a5a473cc33bd9a581fbebed86c'
    ),
    expected_component_placement=_placement(
        application=(
            'compose-post-service', 'home-timeline-service', 'media-frontend',
            'media-service', 'nginx-thrift', 'post-storage-service',
            'social-graph-service', 'text-service', 'unique-id-service',
            'url-shorten-service', 'user-mention-service', 'user-service',
            'user-timeline-service',
        ),
        cache=(
            'home-timeline-redis', 'media-memcached', 'post-storage-memcached',
            'social-graph-redis', 'url-shorten-memcached', 'user-memcached',
            'user-timeline-redis',
        ),
        database=(
            'media-mongodb', 'post-storage-mongodb', 'social-graph-mongodb',
            'url-shorten-mongodb', 'user-mongodb', 'user-timeline-mongodb',
        ),
        control=('jaeger-agent',),
    ),
    released=True,
)

HOTEL_RESERVATION_PROFILE = DistributedDeathStarBenchWorkloadProfile(
    workload_id='hotel_reservation',
    name='Hotel Reservation',
    namespace='deathstarbench-hotel',
    workload_label='hotel-reservation-v1',
    workload_revision='hotel-reservation-6ecb097-workload-v1',
    image_set_revision='hotel-reservation-images-v1',
    dataset_revision='hotel-reservation-bundled-seed-candidate-v1',
    load_driver_revision='wrk2-6ecb097-hotel-oci-amd64-candidate-v1',
    measurement_revision='hotel-reservation-distributed-measurement-candidate-v1',
    frontend_component='frontend',
    frontend_container_port=5000,
    frontend_service_port=8080,
    frontend_node_port=8080,
    required_image_keys=('consul', 'hotel-reservation', 'jaeger', 'memcached', 'mongodb'),
    request_script_path=(
        'hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua'
    ),
    request_script_sha256=(
        'af6230aaaf220fceaef6208d416c5a9d75f15f66d70e3cde00b6e441957bf352'
    ),
    asset_directory=ASSET_ROOT / 'hotel-reservation-v1',
    component_asset_sha256=(
        'bc71e76636b394012689e5a54d3d265ae0c69b4ef13daf0c98ff2e0ef74ed531'
    ),
    network_policy_asset_sha256=(
        'c5b00bde4a2e86236ca1b97ecc9a32a98cbea12e8eee667e83c7345715422c38'
    ),
    expected_component_placement=_placement(
        application=(
            'attractions', 'frontend', 'geo', 'profile', 'rate',
            'recommendation', 'reservation', 'review', 'search', 'user',
        ),
        cache=(
            'memcached-profile', 'memcached-rate', 'memcached-reserve',
            'memcached-review',
        ),
        database=(
            'mongodb-attractions', 'mongodb-geo', 'mongodb-profile',
            'mongodb-rate', 'mongodb-recommendation', 'mongodb-reservation',
            'mongodb-review', 'mongodb-user',
        ),
        control=('consul', 'jaeger'),
    ),
    released=False,
)

MEDIA_MICROSERVICES_PROFILE = DistributedDeathStarBenchWorkloadProfile(
    workload_id='media_microservices',
    name='Media Microservices',
    namespace='deathstarbench-media',
    workload_label='media-microservices-v1',
    workload_revision='media-microservices-6ecb097-workload-v1',
    image_set_revision='media-microservices-images-v1',
    dataset_revision='media-microservices-tmdb-users-movies-candidate-v1',
    load_driver_revision='wrk2-6ecb097-media-oci-amd64-candidate-v1',
    measurement_revision='media-microservices-distributed-measurement-candidate-v1',
    frontend_component='nginx-web-server',
    frontend_container_port=8080,
    frontend_service_port=8080,
    frontend_node_port=8080,
    required_image_keys=(
        'jaeger', 'media-microservices', 'memcached', 'mongodb',
        'nginx-web-server', 'redis',
    ),
    request_script_path=(
        'mediaMicroservices/wrk2/scripts/media-microservices/compose-review.lua'
    ),
    request_script_sha256=(
        '147fdc983955c46a06c80ab41c188e0d946b37eb45e019a1f0a9e3073017bbaa'
    ),
    asset_directory=ASSET_ROOT / 'media-microservices-v1',
    component_asset_sha256=(
        '692a0e86908dabe4600cbc9b2656ab2f6ffdbaa01f38b634811ecbebb5ce0016'
    ),
    network_policy_asset_sha256=(
        '50ea77adfaae027ffd050352d6a3f0afec767c8cc3bbd9d6652eeb8e7343ee6e'
    ),
    expected_component_placement=_placement(
        application=(
            'cast-info-service', 'compose-review-service', 'movie-id-service',
            'movie-info-service', 'movie-review-service', 'nginx-web-server',
            'plot-service', 'rating-service', 'review-storage-service',
            'text-service', 'unique-id-service', 'user-review-service',
            'user-service',
        ),
        cache=(
            'cast-info-memcached', 'compose-review-memcached',
            'movie-id-memcached', 'movie-info-memcached', 'movie-review-redis',
            'plot-memcached', 'rating-redis', 'review-storage-memcached',
            'user-memcached', 'user-review-redis',
        ),
        database=(
            'cast-info-mongodb', 'movie-id-mongodb', 'movie-info-mongodb',
            'movie-review-mongodb', 'plot-mongodb', 'review-storage-mongodb',
            'user-mongodb', 'user-review-mongodb',
        ),
        control=('jaeger',),
    ),
    released=False,
)

DISTRIBUTED_WORKLOAD_PROFILES = MappingProxyType({
    profile.workload_id: profile
    for profile in (
        SOCIAL_NETWORK_PROFILE,
        HOTEL_RESERVATION_PROFILE,
        MEDIA_MICROSERVICES_PROFILE,
    )
})


def distributed_workload_profile(
    workload_id: str,
) -> DistributedDeathStarBenchWorkloadProfile:
    """Resolve an exact known ID, never an alias, default, or mutable state."""

    if not isinstance(workload_id, str) or workload_id not in DISTRIBUTED_WORKLOAD_PROFILES:
        raise ValueError(f'Unsupported distributed DeathStarBench workload: {workload_id!r}.')
    return DISTRIBUTED_WORKLOAD_PROFILES[workload_id]


def require_released_distributed_workload(
    workload_id: str,
) -> DistributedDeathStarBenchWorkloadProfile:
    """Reject candidates even if a component asset claims it is released."""

    profile = distributed_workload_profile(workload_id)
    if not profile.released:
        raise ValueError(
            f'Distributed DeathStarBench {profile.name} is not released; '
            'its immutable images, dataset, load driver, measurement, '
            'network policy, and cleanup qualification are not complete. '
            'Use the released Social Network workload for new runs.'
        )
    return profile


def supported_distributed_workloads() -> tuple[DistributedDeathStarBenchWorkloadProfile, ...]:
    """Return modeled contracts, including explicit non-runnable candidates."""

    return tuple(DISTRIBUTED_WORKLOAD_PROFILES.values())


def selectable_distributed_workloads() -> tuple[DistributedDeathStarBenchWorkloadProfile, ...]:
    """Return reviewed releases; callers must still preflight their bundles."""

    return tuple(profile for profile in supported_distributed_workloads() if profile.released)
