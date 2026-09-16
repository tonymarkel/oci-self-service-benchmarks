"""Deterministic K3s workload bundle for distributed DeathStarBench.

This module intentionally does not discover images, clone source code, or
render a Helm chart at run time.  Static workload and policy specifications
are checked into ``app/manifests``.  A render may vary only by selecting one
of the two platform-specific immutable image sets and by inserting the load
generator's single-host CIDR into the ingress policy.

The bundle is a release candidate.  Its image lock must explicitly say that
it is unreleased; changing that state belongs to the wider runtime release
gate, after the images and live-cloud lifecycle have been qualified.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import shlex
from types import MappingProxyType
from typing import Any

from .deathstarbench_contract import (
    DISTRIBUTED_IMAGE_SET_REVISION,
    DISTRIBUTED_WORKLOAD_REVISION,
)
from .k3s_runtime import K3S_BINARY


NAMESPACE = 'deathstarbench-social'
UPSTREAM_REVISION = '6ecb09706140f8730b5385c08f1386c654c3c526'
IMAGE_LOCK_SCHEMA_VERSION = 1
WORKLOAD_ASSET_SCHEMA_VERSION = 1
WORKLOAD_LABEL = 'social-network-v1'
DATABASE_ROOT = '/var/lib/deathstarbench/database/mongodb'
FRONTEND_COMPONENT = 'nginx-thrift'
FRONTEND_NODE_PORT = 8080
READINESS_TIMEOUT_SECONDS = 900

ASSET_DIRECTORY = (
    Path(__file__).resolve().parent
    / 'manifests'
    / 'deathstarbench'
    / 'social-network-v1'
)
COMPONENT_ASSET = ASSET_DIRECTORY / 'components.json'
NETWORK_POLICY_ASSET = ASSET_DIRECTORY / 'network-policies.json'

PLATFORMS = MappingProxyType({
    'x86_64': 'linux/amd64',
    'amd64': 'linux/amd64',
    'aarch64': 'linux/arm64',
    'arm64': 'linux/arm64',
})
CANONICAL_ARCHITECTURES = MappingProxyType({
    'linux/amd64': 'x86_64',
    'linux/arm64': 'aarch64',
})

CPP_ENTRYPOINTS = MappingProxyType({
    'compose-post-service': 'ComposePostService',
    'home-timeline-service': 'HomeTimelineService',
    'media-service': 'MediaService',
    'post-storage-service': 'PostStorageService',
    'social-graph-service': 'SocialGraphService',
    'text-service': 'TextService',
    'unique-id-service': 'UniqueIdService',
    'url-shorten-service': 'UrlShortenService',
    'user-mention-service': 'UserMentionService',
    'user-service': 'UserService',
    'user-timeline-service': 'UserTimelineService',
})
REDIS_COMPONENTS = frozenset({
    'home-timeline-redis',
    'social-graph-redis',
    'user-timeline-redis',
})
MEMCACHED_COMPONENTS = frozenset({
    'media-memcached',
    'post-storage-memcached',
    'url-shorten-memcached',
    'user-memcached',
})
MONGODB_COMPONENTS = frozenset({
    'media-mongodb',
    'post-storage-mongodb',
    'social-graph-mongodb',
    'url-shorten-mongodb',
    'user-mongodb',
    'user-timeline-mongodb',
})
FRONTEND_COMPONENTS = frozenset({'nginx-thrift', 'media-frontend'})
CONTROL_COMPONENTS = frozenset({'jaeger-agent'})
EXPECTED_COMPONENTS = frozenset({
    *CPP_ENTRYPOINTS,
    *REDIS_COMPONENTS,
    *MEMCACHED_COMPONENTS,
    *MONGODB_COMPONENTS,
    *FRONTEND_COMPONENTS,
    *CONTROL_COMPONENTS,
})
REQUIRED_IMAGE_KEYS = (
    'jaeger',
    'media-frontend',
    'memcached',
    'mongodb',
    'nginx-thrift',
    'redis',
    'social-network-microservices',
)
AUDITED_COMPONENT_EDGES = frozenset({
    ('compose-post-service', 'home-timeline-service', 'TCP', 9090),
    ('compose-post-service', 'media-service', 'TCP', 9090),
    ('compose-post-service', 'post-storage-service', 'TCP', 9090),
    ('compose-post-service', 'text-service', 'TCP', 9090),
    ('compose-post-service', 'unique-id-service', 'TCP', 9090),
    ('compose-post-service', 'user-service', 'TCP', 9090),
    ('compose-post-service', 'user-timeline-service', 'TCP', 9090),
    ('home-timeline-service', 'home-timeline-redis', 'TCP', 6379),
    ('home-timeline-service', 'post-storage-service', 'TCP', 9090),
    ('home-timeline-service', 'social-graph-service', 'TCP', 9090),
    ('media-frontend', 'media-mongodb', 'TCP', 27017),
    ('nginx-thrift', 'compose-post-service', 'TCP', 9090),
    ('nginx-thrift', 'home-timeline-service', 'TCP', 9090),
    ('nginx-thrift', 'social-graph-service', 'TCP', 9090),
    ('nginx-thrift', 'user-service', 'TCP', 9090),
    ('nginx-thrift', 'user-timeline-service', 'TCP', 9090),
    ('post-storage-service', 'post-storage-memcached', 'TCP', 11211),
    ('post-storage-service', 'post-storage-mongodb', 'TCP', 27017),
    ('social-graph-service', 'social-graph-mongodb', 'TCP', 27017),
    ('social-graph-service', 'social-graph-redis', 'TCP', 6379),
    ('social-graph-service', 'user-service', 'TCP', 9090),
    ('text-service', 'url-shorten-service', 'TCP', 9090),
    ('text-service', 'user-mention-service', 'TCP', 9090),
    ('url-shorten-service', 'url-shorten-memcached', 'TCP', 11211),
    ('url-shorten-service', 'url-shorten-mongodb', 'TCP', 27017),
    ('user-mention-service', 'user-memcached', 'TCP', 11211),
    ('user-mention-service', 'user-mongodb', 'TCP', 27017),
    ('user-service', 'social-graph-service', 'TCP', 9090),
    ('user-service', 'user-memcached', 'TCP', 11211),
    ('user-service', 'user-mongodb', 'TCP', 27017),
    ('user-timeline-service', 'post-storage-service', 'TCP', 9090),
    ('user-timeline-service', 'user-timeline-mongodb', 'TCP', 27017),
    ('user-timeline-service', 'user-timeline-redis', 'TCP', 6379),
})
TRACING_SOURCES = frozenset({*CPP_ENTRYPOINTS, *FRONTEND_COMPONENTS})
_EDGE_SOURCES = frozenset(edge[0] for edge in AUDITED_COMPONENT_EDGES)
_EDGE_DESTINATIONS = frozenset(edge[1] for edge in AUDITED_COMPONENT_EDGES)
EXPECTED_NETWORK_POLICIES = frozenset({
    *(f'allow-egress-from-{name}' for name in _EDGE_SOURCES),
    *(f'allow-ingress-to-{name}' for name in _EDGE_DESTINATIONS),
    'allow-dns-egress',
    'allow-jaeger-ingress',
    'allow-load-generator-frontend',
    'allow-tracing-egress',
    'default-deny-all',
})

_IMAGE_REFERENCE_RE = re.compile(
    r'^(?=.{1,512}$)'
    r'(?:[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/)+'
    r'[a-z0-9]+(?:[._/-][a-z0-9]+)*'
    r'@sha256:[0-9a-f]{64}$'
)
_DNS_LABEL_RE = re.compile(
    r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$'
)


class WorkloadBundleError(ValueError):
    """Raised when an image lock, checked-in asset, or attestation drifts."""


def _exact_fields(value: Mapping[str, Any], expected: set[str], label: str):
    actual = set(value)
    if actual == expected:
        return
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    details = []
    if missing:
        details.append(f'missing {", ".join(missing)}')
    if extra:
        details.append(f'unknown {", ".join(extra)}')
    raise WorkloadBundleError(
        f'{label} has an invalid schema ({"; ".join(details)}).'
    )


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WorkloadBundleError(f'{label} must be an object.')
    return value


def _sequence(value: Any, label: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise WorkloadBundleError(f'{label} must be an array.')
    return value


def _dns_label(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _DNS_LABEL_RE.fullmatch(value):
        raise WorkloadBundleError(f'{label} must be a normalized DNS label.')
    return value


def _canonical_architecture(value: Any) -> tuple[str, str]:
    if not isinstance(value, str):
        raise WorkloadBundleError('Architecture must be a string.')
    normalized = value.strip().lower()
    try:
        platform = PLATFORMS[normalized]
    except KeyError:
        raise WorkloadBundleError(
            f'Unsupported workload architecture {value!r}.'
        ) from None
    return CANONICAL_ARCHITECTURES[platform], platform


def _load_generator_cidr(value: Any) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or '/' not in value
    ):
        raise WorkloadBundleError(
            'The load-generator address must be an exact IPv4 /32 CIDR.'
        )
    try:
        network = ipaddress.ip_network(value, strict=True)
    except ValueError:
        raise WorkloadBundleError(
            'The load-generator address must be an exact IPv4 /32 CIDR.'
        ) from None
    if network.version != 4 or network.prefixlen != 32:
        raise WorkloadBundleError(
            'The load-generator address must be an exact IPv4 /32 CIDR.'
        )
    return str(network)


def _immutable_image_reference(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _IMAGE_REFERENCE_RE.fullmatch(value):
        raise WorkloadBundleError(
            f'{label} must be a lower-case registry reference pinned directly '
            'to one sha256 platform-manifest digest.'
        )
    if ':latest' in value or value.endswith(':latest'):
        raise WorkloadBundleError(f'{label} must not use a latest tag.')
    return value


@dataclass(frozen=True, slots=True)
class WorkloadImageLock:
    """Validated, immutable image references for both supported platforms."""

    platforms: Mapping[str, Mapping[str, str]]
    fingerprint: str

    def image(self, architecture: str, image_key: str) -> str:
        _, platform = _canonical_architecture(architecture)
        if image_key not in REQUIRED_IMAGE_KEYS:
            raise WorkloadBundleError(f'Unknown workload image key {image_key!r}.')
        return self.platforms[platform][image_key]


def validate_image_lock(value: Mapping[str, Any]) -> WorkloadImageLock:
    """Validate the exact candidate image-lock schema.

    Both platforms are mandatory even when one platform is selected for a
    particular run.  This prevents a nominal image-set revision from meaning
    different things on x86 and Arm.  The references must point directly to
    platform manifest digests; tags and moving multi-architecture tags are
    never accepted.
    """

    lock = _mapping(value, 'Workload image lock')
    expected_fields = {
        'schema_version',
        'workload_revision',
        'image_set_revision',
        'upstream_revision',
        'released',
        'platforms',
    }
    _exact_fields(lock, expected_fields, 'Workload image lock')
    if lock['schema_version'] != IMAGE_LOCK_SCHEMA_VERSION:
        raise WorkloadBundleError('Unsupported workload image-lock schema.')
    if lock['workload_revision'] != DISTRIBUTED_WORKLOAD_REVISION:
        raise WorkloadBundleError('The workload image lock has the wrong revision.')
    if lock['image_set_revision'] != DISTRIBUTED_IMAGE_SET_REVISION:
        raise WorkloadBundleError('The workload image lock has the wrong image set.')
    if lock['upstream_revision'] != UPSTREAM_REVISION:
        raise WorkloadBundleError('The workload image lock has the wrong upstream commit.')
    if lock['released'] is not False:
        raise WorkloadBundleError(
            'The distributed workload image set is still an unreleased candidate.'
        )

    platform_values = _mapping(lock['platforms'], 'Image-lock platforms')
    required_platforms = {'linux/amd64', 'linux/arm64'}
    _exact_fields(platform_values, required_platforms, 'Image-lock platforms')
    normalized: dict[str, Mapping[str, str]] = {}
    seen_references: set[str] = set()
    for platform in sorted(required_platforms):
        platform_value = _mapping(
            platform_values[platform],
            f'Image-lock platform {platform}',
        )
        _exact_fields(
            platform_value,
            {'images'},
            f'Image-lock platform {platform}',
        )
        images = _mapping(
            platform_value['images'],
            f'Images for {platform}',
        )
        _exact_fields(images, set(REQUIRED_IMAGE_KEYS), f'Images for {platform}')
        selected: dict[str, str] = {}
        for image_key in REQUIRED_IMAGE_KEYS:
            reference = _immutable_image_reference(
                images[image_key],
                f'Image {image_key} for {platform}',
            )
            if reference in seen_references:
                raise WorkloadBundleError(
                    'Every workload component/platform entry must have a '
                    'distinct immutable image reference.'
                )
            seen_references.add(reference)
            selected[image_key] = reference
        normalized[platform] = MappingProxyType(selected)

    for image_key in REQUIRED_IMAGE_KEYS:
        amd64_repository = normalized['linux/amd64'][image_key].split('@', 1)[0]
        arm64_repository = normalized['linux/arm64'][image_key].split('@', 1)[0]
        if amd64_repository != arm64_repository:
            raise WorkloadBundleError(
                f'Image {image_key} must use the same repository on both platforms.'
            )

    canonical = json.dumps(lock, sort_keys=True, separators=(',', ':'))
    fingerprint = 'sha256:' + hashlib.sha256(canonical.encode('utf-8')).hexdigest()
    return WorkloadImageLock(
        platforms=MappingProxyType(normalized),
        fingerprint=fingerprint,
    )


def _read_json_asset(path: Path, label: str) -> Any:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise WorkloadBundleError(f'Unable to read {label}.') from exc
    try:
        return json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WorkloadBundleError(f'{label} is not valid UTF-8 JSON.') from exc


def _validate_port(value: Any, label: str) -> dict[str, Any]:
    port = _mapping(value, label)
    _exact_fields(
        port,
        {'name', 'container_port', 'service_port', 'protocol', 'node_port'},
        label,
    )
    name = _dns_label(port['name'], f'{label} name')
    protocol = port['protocol']
    if protocol not in {'TCP', 'UDP'}:
        raise WorkloadBundleError(f'{label} protocol must be TCP or UDP.')
    for field in ('container_port', 'service_port'):
        number = port[field]
        if type(number) is not int or not 1 <= number <= 65535:
            raise WorkloadBundleError(f'{label} {field} is invalid.')
    node_port = port['node_port']
    if node_port is not None and (
        type(node_port) is not int or not 1 <= node_port <= 65535
    ):
        raise WorkloadBundleError(f'{label} node_port is invalid.')
    return {
        'name': name,
        'container_port': port['container_port'],
        'service_port': port['service_port'],
        'protocol': protocol,
        'node_port': node_port,
    }


def _component_specs() -> tuple[dict[str, Any], ...]:
    asset = _mapping(
        _read_json_asset(COMPONENT_ASSET, 'workload component asset'),
        'Workload component asset',
    )
    _exact_fields(
        asset,
        {
            'schema_version',
            'namespace',
            'upstream_revision',
            'workload_revision',
            'released',
            'components',
        },
        'Workload component asset',
    )
    if asset['schema_version'] != WORKLOAD_ASSET_SCHEMA_VERSION:
        raise WorkloadBundleError('Unsupported workload component asset schema.')
    if asset['namespace'] != NAMESPACE:
        raise WorkloadBundleError('The component asset has the wrong namespace.')
    if asset['upstream_revision'] != UPSTREAM_REVISION:
        raise WorkloadBundleError('The component asset has the wrong upstream commit.')
    if asset['workload_revision'] != DISTRIBUTED_WORKLOAD_REVISION:
        raise WorkloadBundleError('The component asset has the wrong workload revision.')
    if asset['released'] is not False:
        raise WorkloadBundleError('The component asset must remain unreleased.')

    raw_components = _sequence(asset['components'], 'Workload components')
    components: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_components):
        label = f'Workload component {index}'
        component = _mapping(raw, label)
        _exact_fields(
            component,
            {
                'name',
                'image_key',
                'role',
                'entrypoint',
                'environment',
                'ports',
                'service_type',
                'external_traffic_policy',
                'storage_subdir',
            },
            label,
        )
        name = _dns_label(component['name'], f'{label} name')
        image_key = component['image_key']
        if image_key not in REQUIRED_IMAGE_KEYS:
            raise WorkloadBundleError(f'{label} has an unknown image key.')
        role = component['role']
        if role not in {'application', 'cache', 'database', 'control'}:
            raise WorkloadBundleError(f'{label} has an invalid node role.')
        entrypoint = component['entrypoint']
        if entrypoint is not None and (
            not isinstance(entrypoint, str)
            or not re.fullmatch(r'[A-Za-z][A-Za-z0-9]+', entrypoint)
        ):
            raise WorkloadBundleError(f'{label} has an invalid entrypoint.')
        environment = _mapping(component['environment'], f'{label} environment')
        normalized_environment: dict[str, str] = {}
        for key, value in environment.items():
            if (
                not isinstance(key, str)
                or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', key)
                or not isinstance(value, str)
            ):
                raise WorkloadBundleError(f'{label} has invalid environment data.')
            normalized_environment[key] = value
        ports = tuple(
            _validate_port(value, f'{label} port {port_index}')
            for port_index, value in enumerate(
                _sequence(component['ports'], f'{label} ports')
            )
        )
        if not ports:
            raise WorkloadBundleError(f'{label} must expose at least one service port.')
        if len({port['name'] for port in ports}) != len(ports):
            raise WorkloadBundleError(f'{label} has duplicate port names.')
        service_type = component['service_type']
        if service_type not in {'ClusterIP', 'NodePort'}:
            raise WorkloadBundleError(f'{label} has an invalid service type.')
        external_policy = component['external_traffic_policy']
        if external_policy not in {None, 'Local'}:
            raise WorkloadBundleError(f'{label} has an invalid external traffic policy.')
        storage_subdir = component['storage_subdir']
        if storage_subdir is not None:
            storage_subdir = _dns_label(storage_subdir, f'{label} storage subdirectory')
        components.append({
            'name': name,
            'image_key': image_key,
            'role': role,
            'entrypoint': entrypoint,
            'environment': normalized_environment,
            'ports': ports,
            'service_type': service_type,
            'external_traffic_policy': external_policy,
            'storage_subdir': storage_subdir,
        })

    names = [component['name'] for component in components]
    if len(names) != len(set(names)) or set(names) != EXPECTED_COMPONENTS:
        raise WorkloadBundleError('The component asset does not contain the exact workload.')
    if names != sorted(names):
        raise WorkloadBundleError('The component asset must be sorted by component name.')

    by_name = {component['name']: component for component in components}
    for name, entrypoint in CPP_ENTRYPOINTS.items():
        component = by_name[name]
        if (
            component['image_key'] != 'social-network-microservices'
            or component['role'] != 'application'
            or component['entrypoint'] != entrypoint
            or _port_pairs(component) != {('TCP', 9090, 9090, None)}
            or component['storage_subdir'] is not None
        ):
            raise WorkloadBundleError(f'C++ component {name} drifted from upstream.')
    _validate_component_group(by_name, REDIS_COMPONENTS, 'redis', 'cache', 6379)
    _validate_component_group(
        by_name, MEMCACHED_COMPONENTS, 'memcached', 'cache', 11211
    )
    _validate_component_group(
        by_name, MONGODB_COMPONENTS, 'mongodb', 'database', 27017,
        requires_storage=True,
    )
    nginx = by_name[FRONTEND_COMPONENT]
    if (
        nginx['image_key'] != 'nginx-thrift'
        or nginx['role'] != 'application'
        or nginx['service_type'] != 'NodePort'
        or nginx['external_traffic_policy'] != 'Local'
        or _port_pairs(nginx) != {('TCP', 8080, 8080, FRONTEND_NODE_PORT)}
    ):
        raise WorkloadBundleError('The nginx frontend service contract drifted.')
    media = by_name['media-frontend']
    if (
        media['image_key'] != 'media-frontend'
        or media['role'] != 'application'
        or _port_pairs(media) != {('TCP', 8080, 8081, None)}
    ):
        raise WorkloadBundleError('The media frontend port contract drifted.')
    jaeger = by_name['jaeger-agent']
    if (
        jaeger['image_key'] != 'jaeger'
        or jaeger['role'] != 'control'
        or _port_pairs(jaeger)
        != {('UDP', 6831, 6831, None), ('TCP', 16686, 16686, None)}
    ):
        raise WorkloadBundleError('The Jaeger component contract drifted.')
    return tuple(components)


def _port_pairs(component: Mapping[str, Any]) -> set[tuple[Any, ...]]:
    return {
        (
            port['protocol'],
            port['container_port'],
            port['service_port'],
            port['node_port'],
        )
        for port in component['ports']
    }


def _validate_component_group(
    by_name: Mapping[str, Mapping[str, Any]],
    names: frozenset[str],
    image_key: str,
    role: str,
    port: int,
    *,
    requires_storage: bool = False,
):
    for name in names:
        component = by_name[name]
        expected_storage = name if requires_storage else None
        if (
            component['image_key'] != image_key
            or component['role'] != role
            or component['entrypoint'] is not None
            or _port_pairs(component) != {('TCP', port, port, None)}
            or component['storage_subdir'] != expected_storage
            or component['service_type'] != 'ClusterIP'
            or component['external_traffic_policy'] is not None
        ):
            raise WorkloadBundleError(f'Component {name} drifted from its tier contract.')


def _labels(component: Mapping[str, Any]) -> dict[str, str]:
    return {
        'app.kubernetes.io/component': component['name'],
        'app.kubernetes.io/name': 'deathstarbench-social-network',
        'deathstarbench.io/role': component['role'],
        'deathstarbench.io/workload': WORKLOAD_LABEL,
    }


def _metadata(name: str, *, component: Mapping[str, Any] | None = None) -> dict[str, Any]:
    labels = (
        _labels(component)
        if component is not None
        else {
            'app.kubernetes.io/name': 'deathstarbench-social-network',
            'deathstarbench.io/workload': WORKLOAD_LABEL,
        }
    )
    return {
        'annotations': {
            'deathstarbench.io/image-set-revision': DISTRIBUTED_IMAGE_SET_REVISION,
            'deathstarbench.io/upstream-revision': UPSTREAM_REVISION,
            'deathstarbench.io/workload-revision': DISTRIBUTED_WORKLOAD_REVISION,
        },
        'labels': labels,
        'name': name,
        'namespace': NAMESPACE,
    }


def _persistent_volume(component: Mapping[str, Any]) -> dict[str, Any]:
    name = component['name']
    metadata = _metadata(f'dsb-{name}')
    metadata.pop('namespace')
    return {
        'apiVersion': 'v1',
        'kind': 'PersistentVolume',
        'metadata': metadata,
        'spec': {
            'accessModes': ['ReadWriteOnce'],
            'capacity': {'storage': '10Gi'},
            'claimRef': {'name': name, 'namespace': NAMESPACE},
            'hostPath': {
                'path': f'{DATABASE_ROOT}/{component["storage_subdir"]}',
                'type': 'Directory',
            },
            'nodeAffinity': {
                'required': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            'key': 'deathstarbench.io/role',
                            'operator': 'In',
                            'values': ['database'],
                        }],
                    }],
                },
            },
            'persistentVolumeReclaimPolicy': 'Retain',
            'storageClassName': '',
            'volumeMode': 'Filesystem',
        },
    }


def _persistent_volume_claim(component: Mapping[str, Any]) -> dict[str, Any]:
    return {
        'apiVersion': 'v1',
        'kind': 'PersistentVolumeClaim',
        'metadata': _metadata(component['name'], component=component),
        'spec': {
            'accessModes': ['ReadWriteOnce'],
            'resources': {'requests': {'storage': '10Gi'}},
            'storageClassName': '',
            'volumeMode': 'Filesystem',
            'volumeName': f'dsb-{component["name"]}',
        },
    }


def _probe_port(component: Mapping[str, Any]) -> int:
    tcp_ports = [
        port['container_port']
        for port in component['ports']
        if port['protocol'] == 'TCP'
    ]
    if not tcp_ports:
        raise WorkloadBundleError(
            f'Component {component["name"]} has no TCP readiness port.'
        )
    return min(tcp_ports)


def _deployment(
    component: Mapping[str, Any],
    image_reference: str,
    architecture: str,
) -> dict[str, Any]:
    labels = _labels(component)
    container: dict[str, Any] = {
        'env': [
            {'name': key, 'value': value}
            for key, value in sorted(component['environment'].items())
        ],
        'image': image_reference,
        'imagePullPolicy': 'IfNotPresent',
        'name': component['name'],
        'ports': [
            {
                'containerPort': port['container_port'],
                'name': port['name'],
                'protocol': port['protocol'],
            }
            for port in component['ports']
        ],
        'readinessProbe': {
            'failureThreshold': 12,
            'periodSeconds': 5,
            'tcpSocket': {'port': _probe_port(component)},
            'timeoutSeconds': 2,
        },
    }
    if component['entrypoint'] is not None:
        container['command'] = [component['entrypoint']]
    pod_spec: dict[str, Any] = {
        'automountServiceAccountToken': False,
        'containers': [container],
        'enableServiceLinks': False,
        'nodeSelector': {'deathstarbench.io/role': component['role']},
        'terminationGracePeriodSeconds': 30,
    }
    if component['role'] == 'control':
        pod_spec['tolerations'] = [{
            'effect': 'NoSchedule',
            'key': 'node-role.kubernetes.io/control-plane',
            'operator': 'Equal',
            'value': 'true',
        }]
    if component['storage_subdir'] is not None:
        container['volumeMounts'] = [{
            'mountPath': '/data/db',
            'name': 'database-data',
        }]
        pod_spec['volumes'] = [{
            'name': 'database-data',
            'persistentVolumeClaim': {'claimName': component['name']},
        }]
    metadata = _metadata(component['name'], component=component)
    metadata['annotations']['deathstarbench.io/architecture'] = architecture
    return {
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'metadata': metadata,
        'spec': {
            'replicas': 1,
            'revisionHistoryLimit': 0,
            'selector': {'matchLabels': labels},
            'strategy': {'type': 'Recreate'},
            'template': {
                'metadata': {
                    'annotations': {
                        'deathstarbench.io/architecture': architecture,
                        'deathstarbench.io/image-reference': image_reference,
                    },
                    'labels': labels,
                },
                'spec': pod_spec,
            },
        },
    }


def _service(component: Mapping[str, Any]) -> dict[str, Any]:
    service_ports = []
    for port in component['ports']:
        value: dict[str, Any] = {
            'name': port['name'],
            'port': port['service_port'],
            'protocol': port['protocol'],
            'targetPort': port['container_port'],
        }
        if port['node_port'] is not None:
            value['nodePort'] = port['node_port']
        service_ports.append(value)
    spec: dict[str, Any] = {
        'ports': service_ports,
        'selector': _labels(component),
        'type': component['service_type'],
    }
    if component['external_traffic_policy'] is not None:
        spec['externalTrafficPolicy'] = component['external_traffic_policy']
    return {
        'apiVersion': 'v1',
        'kind': 'Service',
        'metadata': _metadata(component['name'], component=component),
        'spec': spec,
    }


def _network_policy_contract() -> tuple[tuple[str, str, str, int], ...]:
    asset = _mapping(
        _read_json_asset(NETWORK_POLICY_ASSET, 'network-policy asset'),
        'Network-policy asset',
    )
    _exact_fields(
        asset,
        {
            'schema_version',
            'namespace',
            'upstream_revision',
            'workload_revision',
            'released',
            'component_edges',
            'application_tracing',
            'dns',
            'load_generator_ingress',
        },
        'Network-policy asset',
    )
    if (
        asset['schema_version'] != WORKLOAD_ASSET_SCHEMA_VERSION
        or asset['namespace'] != NAMESPACE
        or asset['upstream_revision'] != UPSTREAM_REVISION
        or asset['workload_revision'] != DISTRIBUTED_WORKLOAD_REVISION
        or asset['released'] is not False
    ):
        raise WorkloadBundleError('The network-policy asset identity drifted.')
    edges: list[tuple[str, str, str, int]] = []
    for index, raw in enumerate(
        _sequence(asset['component_edges'], 'Network-policy component edges')
    ):
        edge = _mapping(raw, f'Network-policy edge {index}')
        _exact_fields(
            edge,
            {'source', 'destination', 'protocol', 'port'},
            f'Network-policy edge {index}',
        )
        source = _dns_label(edge['source'], f'Network-policy edge {index} source')
        destination = _dns_label(
            edge['destination'],
            f'Network-policy edge {index} destination',
        )
        protocol = edge['protocol']
        port = edge['port']
        if (
            source not in EXPECTED_COMPONENTS
            or destination not in EXPECTED_COMPONENTS
            or protocol not in {'TCP', 'UDP'}
            or type(port) is not int
            or not 1 <= port <= 65535
        ):
            raise WorkloadBundleError(f'Network-policy edge {index} is invalid.')
        edges.append((source, destination, protocol, port))
    if edges != sorted(edges) or len(edges) != len(set(edges)):
        raise WorkloadBundleError(
            'Network-policy component edges must be unique and sorted.'
        )
    if set(edges) != AUDITED_COMPONENT_EDGES:
        raise WorkloadBundleError('The audited component dependency graph drifted.')

    tracing = _mapping(asset['application_tracing'], 'Tracing policy contract')
    _exact_fields(
        tracing,
        {'sources', 'destination', 'protocol', 'port'},
        'Tracing policy contract',
    )
    sources = _sequence(tracing['sources'], 'Tracing policy sources')
    if (
        list(sources) != sorted(sources)
        or set(sources) != TRACING_SOURCES
        or tracing['destination'] != 'jaeger-agent'
        or tracing['protocol'] != 'UDP'
        or tracing['port'] != 6831
    ):
        raise WorkloadBundleError('The tracing policy contract drifted.')
    if asset['dns'] != {
        'namespace': 'kube-system',
        'pod_labels': {'k8s-app': 'kube-dns'},
        'ports': [
            {'port': 53, 'protocol': 'TCP'},
            {'port': 53, 'protocol': 'UDP'},
        ],
    }:
        raise WorkloadBundleError('The DNS policy contract drifted.')
    if asset['load_generator_ingress'] != {
        'destination': FRONTEND_COMPONENT,
        'port': 8080,
        'protocol': 'TCP',
    }:
        raise WorkloadBundleError('The load-generator policy contract drifted.')
    return tuple(edges)


def _policy_metadata(name: str) -> dict[str, Any]:
    return {
        'annotations': {
            'deathstarbench.io/upstream-revision': UPSTREAM_REVISION,
            'deathstarbench.io/workload-revision': DISTRIBUTED_WORKLOAD_REVISION,
        },
        'labels': {'deathstarbench.io/workload': WORKLOAD_LABEL},
        'name': name,
        'namespace': NAMESPACE,
    }


def _component_selector(name: str) -> dict[str, Any]:
    return {
        'matchLabels': {'app.kubernetes.io/component': name},
    }


def _network_policy(name: str, spec: Mapping[str, Any]) -> dict[str, Any]:
    return {
        'apiVersion': 'networking.k8s.io/v1',
        'kind': 'NetworkPolicy',
        'metadata': _policy_metadata(name),
        'spec': dict(spec),
    }


def _network_policy_documents(load_generator_cidr: str) -> tuple[dict[str, Any], ...]:
    edges = _network_policy_contract()
    rendered: list[dict[str, Any]] = []
    for source in sorted(_EDGE_SOURCES):
        rules = []
        for _, destination, protocol, port in (
            edge for edge in edges if edge[0] == source
        ):
            rules.append({
                'ports': [{'port': port, 'protocol': protocol}],
                'to': [{'podSelector': _component_selector(destination)}],
            })
        rendered.append(_network_policy(
            f'allow-egress-from-{source}',
            {
                'egress': rules,
                'podSelector': _component_selector(source),
                'policyTypes': ['Egress'],
            },
        ))
    for destination in sorted(_EDGE_DESTINATIONS):
        rules = []
        for source, _, protocol, port in (
            edge for edge in edges if edge[1] == destination
        ):
            rules.append({
                'from': [{'podSelector': _component_selector(source)}],
                'ports': [{'port': port, 'protocol': protocol}],
            })
        rendered.append(_network_policy(
            f'allow-ingress-to-{destination}',
            {
                'ingress': rules,
                'podSelector': _component_selector(destination),
                'policyTypes': ['Ingress'],
            },
        ))
    rendered.extend((
        _network_policy('allow-dns-egress', {
            'egress': [{
                'ports': [
                    {'port': 53, 'protocol': 'TCP'},
                    {'port': 53, 'protocol': 'UDP'},
                ],
                'to': [{
                    'namespaceSelector': {
                        'matchLabels': {
                            'kubernetes.io/metadata.name': 'kube-system'
                        },
                    },
                    'podSelector': {'matchLabels': {'k8s-app': 'kube-dns'}},
                }],
            }],
            'podSelector': {},
            'policyTypes': ['Egress'],
        }),
        _network_policy('allow-jaeger-ingress', {
            'ingress': [{
                'from': [{
                    'podSelector': {
                        'matchLabels': {'deathstarbench.io/role': 'application'}
                    },
                }],
                'ports': [{'port': 6831, 'protocol': 'UDP'}],
            }],
            'podSelector': _component_selector('jaeger-agent'),
            'policyTypes': ['Ingress'],
        }),
        _network_policy('allow-load-generator-frontend', {
            'ingress': [{
                'from': [{'ipBlock': {'cidr': load_generator_cidr}}],
                'ports': [{'port': 8080, 'protocol': 'TCP'}],
            }],
            'podSelector': _component_selector(FRONTEND_COMPONENT),
            'policyTypes': ['Ingress'],
        }),
        _network_policy('allow-tracing-egress', {
            'egress': [{
                'ports': [{'port': 6831, 'protocol': 'UDP'}],
                'to': [{
                    'podSelector': _component_selector('jaeger-agent'),
                }],
            }],
            'podSelector': {
                'matchLabels': {'deathstarbench.io/role': 'application'}
            },
            'policyTypes': ['Egress'],
        }),
        _network_policy('default-deny-all', {
            'egress': [],
            'ingress': [],
            'podSelector': {},
            'policyTypes': ['Ingress', 'Egress'],
        }),
    ))
    rendered.sort(key=lambda policy: policy['metadata']['name'])
    names = [value['metadata']['name'] for value in rendered]
    if len(names) != len(set(names)) or set(names) != EXPECTED_NETWORK_POLICIES:
        raise WorkloadBundleError('The NetworkPolicy set is incomplete or contains extras.')
    _attest_network_policy_contract(rendered, load_generator_cidr)
    return tuple(rendered)


def _attest_network_policy_contract(
    policies: Sequence[Mapping[str, Any]],
    load_generator_cidr: str,
):
    by_name = {policy['metadata']['name']: policy for policy in policies}
    default = by_name['default-deny-all']['spec']
    if (
        default != {
            'podSelector': {},
            'policyTypes': ['Ingress', 'Egress'],
            'ingress': [],
            'egress': [],
        }
    ):
        raise WorkloadBundleError('The default-deny NetworkPolicy drifted.')
    observed_egress = set()
    for source in _EDGE_SOURCES:
        policy = by_name[f'allow-egress-from-{source}']
        for rule in policy['spec']['egress']:
            destination = rule['to'][0]['podSelector']['matchLabels'][
                'app.kubernetes.io/component'
            ]
            port = rule['ports'][0]
            observed_egress.add((
                source, destination, port['protocol'], port['port']
            ))
    observed_ingress = set()
    for destination in _EDGE_DESTINATIONS:
        policy = by_name[f'allow-ingress-to-{destination}']
        for rule in policy['spec']['ingress']:
            source = rule['from'][0]['podSelector']['matchLabels'][
                'app.kubernetes.io/component'
            ]
            port = rule['ports'][0]
            observed_ingress.add((
                source, destination, port['protocol'], port['port']
            ))
    if (
        observed_egress != AUDITED_COMPONENT_EDGES
        or observed_ingress != AUDITED_COMPONENT_EDGES
    ):
        raise WorkloadBundleError('Rendered component policy edges drifted.')
    frontend = by_name['allow-load-generator-frontend']
    if (
        frontend['spec'].get('podSelector', {}).get('matchLabels', {}).get(
            'app.kubernetes.io/component'
        ) != FRONTEND_COMPONENT
        or frontend['spec']['ingress'][0]['from']
        != [{'ipBlock': {'cidr': load_generator_cidr}}]
    ):
        raise WorkloadBundleError('The load-generator ingress policy drifted.')
    dns = by_name['allow-dns-egress']['spec']['egress']
    if len(dns) != 1 or dns[0].get('to') != [{
        'namespaceSelector': {
            'matchLabels': {'kubernetes.io/metadata.name': 'kube-system'}
        },
        'podSelector': {'matchLabels': {'k8s-app': 'kube-dns'}},
    }]:
        raise WorkloadBundleError('The DNS egress policy is not scoped to CoreDNS.')
    tracing_egress = by_name['allow-tracing-egress']['spec']
    tracing_ingress = by_name['allow-jaeger-ingress']['spec']
    if (
        tracing_egress['podSelector']
        != {'matchLabels': {'deathstarbench.io/role': 'application'}}
        or tracing_egress['egress'] != [{
            'ports': [{'port': 6831, 'protocol': 'UDP'}],
            'to': [{'podSelector': _component_selector('jaeger-agent')}],
        }]
        or tracing_ingress['podSelector']
        != _component_selector('jaeger-agent')
        or tracing_ingress['ingress'] != [{
            'from': [{
                'podSelector': {
                    'matchLabels': {'deathstarbench.io/role': 'application'}
                },
            }],
            'ports': [{'port': 6831, 'protocol': 'UDP'}],
        }]
    ):
        raise WorkloadBundleError('The rendered tracing policies drifted.')


@dataclass(frozen=True, slots=True)
class RenderedSocialNetworkBundle:
    """Canonical phase payloads plus provenance used by orchestration."""

    manifest_source_sha256: str
    rendered_manifest_sha256: str
    image_set_revision: str
    application_architecture: str
    namespace: str
    namespace_manifest: str
    storage: str
    policies: str
    services: str
    workloads: str
    expected_component_placement: Mapping[str, str]
    load_generator_cidr: str
    image_lock_fingerprint: str
    canonical_json: str
    document_json: tuple[str, ...]

    @property
    def architecture(self) -> str:
        """Compatibility name for the selected application architecture."""

        return self.application_architecture

    @property
    def platform(self) -> str:
        return PLATFORMS[self.application_architecture]

    @property
    def bundle_fingerprint(self) -> str:
        return self.rendered_manifest_sha256

    @property
    def phase_payloads(self) -> tuple[str, str, str, str, str]:
        """Return phases in their only valid application order."""

        return (
            self.namespace_manifest,
            self.storage,
            self.policies,
            self.services,
            self.workloads,
        )

    @property
    def phase_names(self) -> tuple[str, str, str, str, str]:
        return ('namespace', 'storage', 'policies', 'services', 'workloads')

    @property
    def phases(self) -> tuple[tuple[str, str], ...]:
        """Pair every phase name with its payload in deterministic order."""

        return tuple(zip(self.phase_names, self.phase_payloads, strict=True))

    def phase_payload(self, phase: str) -> str:
        """Resolve a named phase without confusing it with the namespace ID."""

        fields = {
            'namespace': 'namespace_manifest',
            'storage': 'storage',
            'policies': 'policies',
            'services': 'services',
            'workloads': 'workloads',
        }
        try:
            return getattr(self, fields[phase])
        except (KeyError, TypeError):
            raise WorkloadBundleError(f'Unknown workload phase {phase!r}.') from None

    def phase_sha256(self, phase: str) -> str:
        """Return the bare SHA-256 expected by ``workload_apply_command``."""

        payload = self.phase_payload(phase)
        return hashlib.sha256(payload.encode('utf-8')).hexdigest()


# Compatibility name retained for callers written while this candidate layer
# was being developed.
RenderedWorkloadBundle = RenderedSocialNetworkBundle


def _canonical_list_json(documents: Sequence[Mapping[str, Any]]) -> str:
    return json.dumps(
        {'apiVersion': 'v1', 'items': list(documents), 'kind': 'List'},
        sort_keys=True,
        separators=(',', ':'),
    ) + '\n'


def _manifest_source_sha256() -> str:
    digest = hashlib.sha256()
    for path in (COMPONENT_ASSET, NETWORK_POLICY_ASSET):
        digest.update(path.name.encode('ascii'))
        digest.update(b'\0')
        try:
            digest.update(path.read_bytes())
        except OSError as exc:
            raise WorkloadBundleError('Unable to fingerprint workload assets.') from exc
        digest.update(b'\0')
    return 'sha256:' + digest.hexdigest()


def _component_architecture(name: str, application_architecture: str) -> str:
    return (
        application_architecture
        if _component_role(name) == 'application'
        else 'x86_64'
    )


def _component_image(
    image_lock: WorkloadImageLock,
    name: str,
    application_architecture: str,
) -> str:
    component_architecture = _component_architecture(
        name,
        application_architecture,
    )
    platform = PLATFORMS[component_architecture]
    return image_lock.platforms[platform][_component_image_key(name)]


def render_workload_bundle(
    image_lock: Mapping[str, Any],
    architecture: str,
    load_generator_cidr: str,
) -> RenderedWorkloadBundle:
    """Render the exact Kubernetes object set for one benchmark cluster."""

    validated_lock = validate_image_lock(image_lock)
    canonical_architecture, platform = _canonical_architecture(architecture)
    load_cidr = _load_generator_cidr(load_generator_cidr)
    components = _component_specs()

    namespace = {
        'apiVersion': 'v1',
        'kind': 'Namespace',
        'metadata': {
            'annotations': {
                'deathstarbench.io/image-set-revision': DISTRIBUTED_IMAGE_SET_REVISION,
                'deathstarbench.io/upstream-revision': UPSTREAM_REVISION,
                'deathstarbench.io/workload-revision': DISTRIBUTED_WORKLOAD_REVISION,
            },
            'labels': {
                'kubernetes.io/metadata.name': NAMESPACE,
                'deathstarbench.io/workload': WORKLOAD_LABEL,
            },
            'name': NAMESPACE,
        },
    }
    documents: list[dict[str, Any]] = [namespace]
    database_components = [
        component
        for component in components
        if component['storage_subdir'] is not None
    ]
    documents.extend(_persistent_volume(component) for component in database_components)
    documents.extend(
        _persistent_volume_claim(component) for component in database_components
    )
    documents.extend(
        _deployment(
            component,
            _component_image(
                validated_lock,
                component['name'],
                canonical_architecture,
            ),
            _component_architecture(
                component['name'],
                canonical_architecture,
            ),
        )
        for component in components
    )
    documents.extend(_service(component) for component in components)
    documents.extend(_network_policy_documents(load_cidr))

    _validate_rendered_documents(
        documents,
        validated_lock,
        canonical_architecture,
        load_cidr,
    )
    document_json = tuple(
        json.dumps(document, sort_keys=True, separators=(',', ':'))
        for document in documents
    )
    canonical_json = _canonical_list_json(documents)
    bundle_fingerprint = (
        'sha256:' + hashlib.sha256(canonical_json.encode('utf-8')).hexdigest()
    )
    namespace_documents = [
        document
        for document in documents
        if document['kind'] == 'Namespace'
    ]
    storage_documents = [
        document
        for document in documents
        if document['kind'] in {
            'PersistentVolume', 'PersistentVolumeClaim'
        }
    ]
    policy_documents = [
        document for document in documents if document['kind'] == 'NetworkPolicy'
    ]
    service_documents = [
        document for document in documents if document['kind'] == 'Service'
    ]
    workload_documents = [
        document for document in documents if document['kind'] == 'Deployment'
    ]
    return RenderedSocialNetworkBundle(
        manifest_source_sha256=_manifest_source_sha256(),
        rendered_manifest_sha256=bundle_fingerprint,
        image_set_revision=DISTRIBUTED_IMAGE_SET_REVISION,
        application_architecture=canonical_architecture,
        namespace=NAMESPACE,
        namespace_manifest=_canonical_list_json(namespace_documents),
        storage=_canonical_list_json(storage_documents),
        policies=_canonical_list_json(policy_documents),
        services=_canonical_list_json(service_documents),
        workloads=_canonical_list_json(workload_documents),
        expected_component_placement=MappingProxyType({
            component['name']: component['role']
            for component in components
        }),
        load_generator_cidr=load_cidr,
        image_lock_fingerprint=validated_lock.fingerprint,
        canonical_json=canonical_json,
        document_json=document_json,
    )


def render_social_network_bundle(
    image_lock: Mapping[str, Any],
    application_architecture: str,
    load_generator_ip: str,
) -> RenderedSocialNetworkBundle:
    """Render from the provider inventory's exact load-generator address."""

    if not isinstance(load_generator_ip, str) or load_generator_ip != load_generator_ip.strip():
        raise WorkloadBundleError('Load-generator IP must be an exact IPv4 address.')
    try:
        address = ipaddress.ip_address(load_generator_ip)
    except ValueError:
        raise WorkloadBundleError('Load-generator IP must be an exact IPv4 address.') from None
    if address.version != 4:
        raise WorkloadBundleError('Load-generator IP must be an exact IPv4 address.')
    return render_workload_bundle(
        image_lock,
        application_architecture,
        f'{address}/32',
    )


def render_workload_documents(
    image_lock: Mapping[str, Any],
    architecture: str,
    load_generator_cidr: str,
) -> tuple[str, ...]:
    """Return individually canonicalized Kubernetes JSON documents."""

    return render_workload_bundle(
        image_lock, architecture, load_generator_cidr
    ).document_json


def render_workload_json(
    image_lock: Mapping[str, Any],
    architecture: str,
    load_generator_cidr: str,
) -> str:
    """Return one canonical Kubernetes List JSON document."""

    return render_workload_bundle(
        image_lock, architecture, load_generator_cidr
    ).canonical_json


def _validate_rendered_documents(
    documents: Sequence[Mapping[str, Any]],
    image_lock: WorkloadImageLock,
    architecture: str,
    load_generator_cidr: str,
):
    identities: list[tuple[str, str, str | None]] = []
    for document in documents:
        metadata = _mapping(document.get('metadata'), 'Rendered object metadata')
        identities.append((
            str(document.get('kind')),
            str(metadata.get('name')),
            metadata.get('namespace'),
        ))
    if len(identities) != len(set(identities)):
        raise WorkloadBundleError('The rendered bundle contains duplicate identities.')
    counts = Counter(kind for kind, _, _ in identities)
    expected_counts = {
        'Namespace': 1,
        'PersistentVolume': 6,
        'PersistentVolumeClaim': 6,
        'Deployment': len(EXPECTED_COMPONENTS),
        'Service': len(EXPECTED_COMPONENTS),
        'NetworkPolicy': len(EXPECTED_NETWORK_POLICIES),
    }
    if dict(counts) != expected_counts:
        raise WorkloadBundleError('The rendered workload object inventory drifted.')
    expected_images = {
        name: _component_image(image_lock, name, architecture)
        for name in EXPECTED_COMPONENTS
    }
    deployments = {
        document['metadata']['name']: document
        for document in documents
        if document['kind'] == 'Deployment'
    }
    if set(deployments) != EXPECTED_COMPONENTS:
        raise WorkloadBundleError('The rendered deployment inventory drifted.')
    for name, deployment in deployments.items():
        containers = deployment['spec']['template']['spec']['containers']
        if len(containers) != 1 or containers[0]['image'] != expected_images[name]:
            raise WorkloadBundleError(f'Deployment {name} selected the wrong image.')
    serialized = json.dumps(documents, sort_keys=True, separators=(',', ':'))
    if ':latest' in serialized:
        raise WorkloadBundleError('The rendered bundle contains a mutable placeholder.')
    if serialized.count(load_generator_cidr) != 1:
        raise WorkloadBundleError('The load-generator CIDR was not rendered exactly once.')


def _component_image_key(name: str) -> str:
    if name in CPP_ENTRYPOINTS:
        return 'social-network-microservices'
    if name in REDIS_COMPONENTS:
        return 'redis'
    if name in MEMCACHED_COMPONENTS:
        return 'memcached'
    if name in MONGODB_COMPONENTS:
        return 'mongodb'
    if name == 'nginx-thrift':
        return 'nginx-thrift'
    if name == 'media-frontend':
        return 'media-frontend'
    if name == 'jaeger-agent':
        return 'jaeger'
    raise WorkloadBundleError(f'Unknown component {name!r}.')


def workload_apply_command(expected_sha256: str) -> str:
    """Read one canonical phase from stdin, verify it, then apply it.

    Keeping the payload on stdin avoids putting a large manifest (including
    registry coordinates) into the SSH command line, process table, or job
    event text.  The orchestrator obtains the expected digest from
    ``RenderedSocialNetworkBundle.phase_sha256``.
    """

    if (
        not isinstance(expected_sha256, str)
        or not re.fullmatch(r'[0-9a-f]{64}', expected_sha256)
    ):
        raise WorkloadBundleError('Expected phase SHA-256 must be 64 lower-case hex.')
    kubectl = f'sudo {shlex.quote(K3S_BINARY)} kubectl'
    return (
        'set -euo pipefail; umask 077; '
        f'EXPECTED_SHA256={shlex.quote(expected_sha256)}; '
        'MANIFEST=$(mktemp /tmp/deathstarbench-workload.XXXXXX.json); '
        'trap \'rm -f -- "$MANIFEST"\' EXIT; '
        'cat > "$MANIFEST"; test -s "$MANIFEST"; '
        'ACTUAL_SHA256=$(sha256sum "$MANIFEST" | awk \'{print $1}\'); '
        'test "$ACTUAL_SHA256" = "$EXPECTED_SHA256" || '
        '{ echo "Workload phase SHA-256 mismatch." >&2; exit 1; }; '
        f'{kubectl} apply --dry-run=server -f "$MANIFEST" >/dev/null; '
        f'{kubectl} apply --field-manager=deathstarbench-benchmark '
        '-f "$MANIFEST" >/dev/null'
    )


def workload_readiness_command(
    bundle: RenderedSocialNetworkBundle,
    timeout_seconds: int = READINESS_TIMEOUT_SECONDS,
) -> str:
    """Wait for the exact workload and emit one Kubernetes List attestation."""

    if not isinstance(bundle, RenderedSocialNetworkBundle):
        raise WorkloadBundleError('Workload readiness requires a rendered bundle.')
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 3600:
        raise WorkloadBundleError('Workload readiness timeout must be 1-3600 seconds.')
    kubectl = f'sudo {shlex.quote(K3S_BINARY)} kubectl'
    resources = (
        'deployments.apps,services,persistentvolumes,persistentvolumeclaims,'
        'networkpolicies.networking.k8s.io,pods'
    )
    return (
        'set -euo pipefail; '
        f'{kubectl} -n {shlex.quote(NAMESPACE)} wait '
        '--for=condition=Available deployment --all '
        f'--timeout={timeout_seconds}s >/dev/null; '
        f'{kubectl} -n {shlex.quote(NAMESPACE)} wait '
        '--for=condition=Ready pod '
        f'-l deathstarbench.io/workload={shlex.quote(WORKLOAD_LABEL)} '
        f'--timeout={timeout_seconds}s >/dev/null; '
        f'{kubectl} -n {shlex.quote(NAMESPACE)} get {resources} '
        '-o json'
    )


def _attestation_items(value: str | bytes) -> list[Mapping[str, Any]]:
    if isinstance(value, bytes):
        try:
            value = value.decode('utf-8')
        except UnicodeDecodeError as exc:
            raise WorkloadBundleError('Workload attestation is not UTF-8.') from exc
    if not isinstance(value, str):
        raise WorkloadBundleError('Workload attestation must be JSON text.')
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise WorkloadBundleError('Workload attestation is not valid JSON.') from exc
    document = _mapping(payload, 'Workload attestation')
    allowed = {'apiVersion', 'kind', 'items', 'metadata'}
    if not set(document).issubset(allowed) or not {'apiVersion', 'kind', 'items'} <= set(document):
        raise WorkloadBundleError('Workload attestation has an invalid schema.')
    if document['apiVersion'] != 'v1' or document['kind'] != 'List':
        raise WorkloadBundleError('Workload attestation must be a Kubernetes List.')
    items = _sequence(document['items'], 'Workload attestation items')
    result = []
    for index, item in enumerate(items):
        result.append(_mapping(item, f'Workload attestation item {index}'))
    return result


def _require_expected_subset(actual: Any, expected: Any, label: str):
    """Compare submitted fields while allowing Kubernetes API defaults."""

    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping):
            raise WorkloadBundleError(f'{label} is not an object.')
        for key, expected_value in expected.items():
            if key not in actual:
                raise WorkloadBundleError(f'{label} is missing {key}.')
            _require_expected_subset(
                actual[key],
                expected_value,
                f'{label}.{key}',
            )
        return
    if isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) != len(expected):
            raise WorkloadBundleError(f'{label} array drifted.')
        for index, (actual_value, expected_value) in enumerate(
            zip(actual, expected, strict=True)
        ):
            _require_expected_subset(
                actual_value,
                expected_value,
                f'{label}[{index}]',
            )
        return
    if actual != expected:
        raise WorkloadBundleError(f'{label} drifted.')


def parse_workload_attestation(
    value: str | bytes,
    bundle: RenderedSocialNetworkBundle,
) -> Mapping[str, Any]:
    """Fail closed unless live Kubernetes state matches the rendered bundle."""

    if not isinstance(bundle, RenderedSocialNetworkBundle):
        raise WorkloadBundleError('Attestation requires a rendered workload bundle.')
    items = _attestation_items(value)
    by_kind: dict[str, dict[str, Mapping[str, Any]]] = {}
    for item in items:
        kind = item.get('kind')
        metadata = _mapping(item.get('metadata'), 'Attested resource metadata')
        name = metadata.get('name')
        namespace = metadata.get('namespace')
        if not isinstance(kind, str) or not isinstance(name, str):
            raise WorkloadBundleError('Attested resource identity is invalid.')
        if kind != 'PersistentVolume' and namespace != NAMESPACE:
            raise WorkloadBundleError(f'Attested {kind}/{name} is in the wrong namespace.')
        kind_values = by_kind.setdefault(kind, {})
        if name in kind_values:
            raise WorkloadBundleError(f'Attestation repeats {kind}/{name}.')
        kind_values[name] = item

    expected_static = {
        'Deployment': set(EXPECTED_COMPONENTS),
        'Service': set(EXPECTED_COMPONENTS),
        'PersistentVolume': {f'dsb-{name}' for name in MONGODB_COMPONENTS},
        'PersistentVolumeClaim': set(MONGODB_COMPONENTS),
        'NetworkPolicy': set(EXPECTED_NETWORK_POLICIES),
    }
    if set(by_kind) != {*expected_static, 'Pod'}:
        raise WorkloadBundleError('Attestation contains an unexpected resource kind.')
    for kind, names in expected_static.items():
        if set(by_kind.get(kind, {})) != names:
            raise WorkloadBundleError(f'Attested {kind} inventory drifted.')

    expected_by_kind: dict[str, dict[str, Mapping[str, Any]]] = {}
    for expected in json.loads(bundle.canonical_json)['items']:
        kind = expected['kind']
        if kind == 'Namespace':
            continue
        expected_by_kind.setdefault(kind, {})[
            expected['metadata']['name']
        ] = expected
    for kind, names in expected_static.items():
        for name in names:
            observed = by_kind[kind][name]
            expected = expected_by_kind[kind][name]
            _require_expected_subset(
                observed.get('metadata'),
                expected['metadata'],
                f'{kind}/{name} metadata',
            )
            _require_expected_subset(
                observed.get('spec'),
                expected['spec'],
                f'{kind}/{name} spec',
            )

    workload_payload = json.loads(bundle.workloads)
    expected_images = {
        item['metadata']['name']:
        item['spec']['template']['spec']['containers'][0]['image']
        for item in workload_payload['items']
    }
    for name, deployment in by_kind['Deployment'].items():
        spec = _mapping(deployment.get('spec'), f'Deployment {name} spec')
        status = _mapping(deployment.get('status'), f'Deployment {name} status')
        if any(status.get(field) != 1 for field in (
            'availableReplicas', 'readyReplicas', 'updatedReplicas'
        )):
            raise WorkloadBundleError(f'Deployment {name} is not exactly ready.')
        template_spec = _mapping(
            spec.get('template', {}).get('spec'),
            f'Deployment {name} pod spec',
        )
        if template_spec.get('nodeSelector') != {
            'deathstarbench.io/role': _component_role(name)
        }:
            raise WorkloadBundleError(f'Deployment {name} placement drifted.')
        containers = _sequence(
            template_spec.get('containers'),
            f'Deployment {name} containers',
        )
        expected_image = expected_images[name]
        if len(containers) != 1 or containers[0].get('image') != expected_image:
            raise WorkloadBundleError(f'Deployment {name} image drifted.')
        resources = containers[0].get('resources', {})
        if resources not in ({}, None):
            raise WorkloadBundleError(
                f'Deployment {name} unexpectedly constrains benchmark resources.'
            )
        if template_spec.get('initContainers') not in (None, []):
            raise WorkloadBundleError(
                f'Deployment {name} contains an unapproved init container.'
            )

    frontend = by_kind['Service'][FRONTEND_COMPONENT]
    frontend_spec = _mapping(frontend.get('spec'), 'Frontend Service spec')
    if (
        frontend_spec.get('type') != 'NodePort'
        or frontend_spec.get('externalTrafficPolicy') != 'Local'
        or not any(
            port.get('port') == 8080
            and port.get('targetPort') == 8080
            and port.get('nodePort') == 8080
            and port.get('protocol', 'TCP') == 'TCP'
            for port in _sequence(frontend_spec.get('ports'), 'Frontend ports')
        )
    ):
        raise WorkloadBundleError('The live frontend Service drifted.')

    for name in MONGODB_COMPONENTS:
        pv = by_kind['PersistentVolume'][f'dsb-{name}']
        pvc = by_kind['PersistentVolumeClaim'][name]
        if pv.get('status', {}).get('phase') != 'Bound':
            raise WorkloadBundleError(f'PersistentVolume dsb-{name} is not Bound.')
        if pvc.get('status', {}).get('phase') != 'Bound':
            raise WorkloadBundleError(f'PersistentVolumeClaim {name} is not Bound.')
        if pv.get('spec', {}).get('hostPath') != {
            'path': f'{DATABASE_ROOT}/{name}',
            'type': 'Directory',
        }:
            raise WorkloadBundleError(f'PersistentVolume dsb-{name} path drifted.')
        if pvc.get('spec', {}).get('volumeName') != f'dsb-{name}':
            raise WorkloadBundleError(f'PersistentVolumeClaim {name} binding drifted.')

    pods_by_component: dict[str, Mapping[str, Any]] = {}
    for pod in by_kind['Pod'].values():
        metadata = _mapping(pod.get('metadata'), 'Pod metadata')
        labels = _mapping(metadata.get('labels'), 'Pod labels')
        component = labels.get('app.kubernetes.io/component')
        if component not in EXPECTED_COMPONENTS or component in pods_by_component:
            raise WorkloadBundleError('Pod component inventory drifted.')
        spec = _mapping(pod.get('spec'), f'Pod {metadata.get("name")} spec')
        status = _mapping(pod.get('status'), f'Pod {metadata.get("name")} status')
        if (
            status.get('phase') != 'Running'
            or not isinstance(spec.get('nodeName'), str)
            or not spec.get('nodeName')
            or spec.get('nodeSelector')
            != {'deathstarbench.io/role': _component_role(component)}
        ):
            raise WorkloadBundleError(f'Pod for {component} is not correctly placed.')
        conditions = _sequence(status.get('conditions'), f'Pod {component} conditions')
        ready = [
            condition
            for condition in conditions
            if isinstance(condition, Mapping) and condition.get('type') == 'Ready'
        ]
        container_statuses = _sequence(
            status.get('containerStatuses'),
            f'Pod {component} container statuses',
        )
        expected_image = expected_images[component]
        expected_digest = expected_image.rsplit('@', 1)[1]
        expected_container = expected_by_kind['Deployment'][component][
            'spec'
        ]['template']['spec']['containers'][0]
        pod_containers = _sequence(
            spec.get('containers'),
            f'Pod {component} containers',
        )
        if len(pod_containers) != 1:
            raise WorkloadBundleError(f'Pod for {component} has extra containers.')
        _require_expected_subset(
            pod_containers[0],
            expected_container,
            f'Pod {component} container',
        )
        if pod_containers[0].get('resources', {}) not in ({}, None):
            raise WorkloadBundleError(
                f'Pod for {component} unexpectedly constrains benchmark resources.'
            )
        if spec.get('initContainers') not in (None, []):
            raise WorkloadBundleError(
                f'Pod for {component} contains an unapproved init container.'
            )
        if (
            len(ready) != 1
            or ready[0].get('status') != 'True'
            or len(container_statuses) != 1
            or container_statuses[0].get('ready') is not True
            or container_statuses[0].get('image') != expected_image
            or not str(container_statuses[0].get('imageID', '')).endswith(
                '@' + expected_digest
            )
        ):
            raise WorkloadBundleError(f'Pod image/readiness for {component} drifted.')
        pods_by_component[component] = pod
    if set(pods_by_component) != EXPECTED_COMPONENTS:
        raise WorkloadBundleError('The live pod inventory is incomplete.')

    return MappingProxyType({
        'schema_version': 1,
        'namespace': NAMESPACE,
        'workload_revision': DISTRIBUTED_WORKLOAD_REVISION,
        'image_set_revision': DISTRIBUTED_IMAGE_SET_REVISION,
        'architecture': bundle.application_architecture,
        'platform': bundle.platform,
        'load_generator_cidr': bundle.load_generator_cidr,
        'image_lock_fingerprint': bundle.image_lock_fingerprint,
        'bundle_fingerprint': bundle.rendered_manifest_sha256,
        'component_count': len(EXPECTED_COMPONENTS),
        'pod_nodes': tuple(sorted({
            pod['spec']['nodeName'] for pod in pods_by_component.values()
        })),
    })


def _component_role(name: str) -> str:
    if name in CPP_ENTRYPOINTS or name in FRONTEND_COMPONENTS:
        return 'application'
    if name in REDIS_COMPONENTS or name in MEMCACHED_COMPONENTS:
        return 'cache'
    if name in MONGODB_COMPONENTS:
        return 'database'
    if name in CONTROL_COMPONENTS:
        return 'control'
    raise WorkloadBundleError(f'Unknown component {name!r}.')


# Concise aliases for orchestrators that already scope their operation to the
# DeathStarBench workload phase.
apply_command = workload_apply_command
readiness_command = workload_readiness_command
parse_readiness_attestation = parse_workload_attestation
