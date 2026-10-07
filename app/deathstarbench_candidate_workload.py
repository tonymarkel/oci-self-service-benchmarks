"""Offline, fail-closed manifest foundation for new distributed workloads.

Hotel and Media are candidates, not publicly runnable benchmarks. Rendering
does not provision, apply manifests, initialize data, or open their release
gates. The released Social Network renderer and its historical fingerprints
remain unchanged. All selection is explicit so concurrent previews cannot
change another workload's namespace or image identities.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import ipaddress
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any

from .deathstarbench_workload_contract import (
    DistributedDeathStarBenchWorkloadProfile,
    distributed_workload_profile,
)


ASSET_SCHEMA_VERSION = 2
CANDIDATE_IMAGE_SCHEMA_VERSION = 1
DATABASE_ROOT = '/var/lib/deathstarbench/database/mongodb'
UPSTREAM_REVISION = '6ecb09706140f8730b5385c08f1386c654c3c526'
_ARCHITECTURES = {'x86_64': 'linux/amd64', 'aarch64': 'linux/arm64'}
_ARCHITECTURE_ALIASES = {'amd64': 'x86_64', 'arm64': 'aarch64'}
_PRIVATE_NETWORKS = tuple(ipaddress.ip_network(cidr) for cidr in (
    '10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16',
))
_DNS_LABEL = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$')
_IMAGE_REFERENCE = re.compile(
    r'^(?=.{1,512}$)(?:[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/)+'
    r'[a-z0-9]+(?:[._/-][a-z0-9]+)*@sha256:[0-9a-f]{64}$'
)


class CandidateWorkloadError(ValueError):
    """A candidate asset or preview input does not match its audited contract."""


def _fields(value: Any, expected: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise CandidateWorkloadError(f'{label} has an invalid schema.')
    return value


def _array(value: Any, label: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise CandidateWorkloadError(f'{label} must be an array.')
    return value


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n'


def _fingerprint(value: Any) -> str:
    return 'sha256:' + hashlib.sha256(_canonical(value).encode()).hexdigest()


def _candidate_profile(workload_id: str) -> DistributedDeathStarBenchWorkloadProfile:
    try:
        profile = distributed_workload_profile(workload_id)
    except ValueError as exc:
        raise CandidateWorkloadError(str(exc)) from exc
    if profile.released or workload_id == 'social_network':
        raise CandidateWorkloadError(
            'Use the existing released renderer for Social Network; this '
            'foundation accepts only unreleased Hotel and Media candidates.'
        )
    return profile


def _read_asset(path: Path, expected_sha256: str) -> Mapping[str, Any]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise CandidateWorkloadError(f'Unable to read candidate asset {path.name}.') from exc
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise CandidateWorkloadError(f'Audited candidate asset {path.name} changed.')
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CandidateWorkloadError('Candidate asset must be UTF-8 JSON.') from exc
    if not isinstance(value, Mapping):
        raise CandidateWorkloadError('Candidate asset must be an object.')
    return value


def _asset_identity(asset: Mapping[str, Any], profile: Any, section: str):
    _fields(asset, {
        'schema_version', 'namespace', 'upstream_revision',
        'workload_revision', 'released', section,
    }, 'Candidate asset')
    if (
        type(asset['schema_version']) is not int
        or asset['schema_version'] != ASSET_SCHEMA_VERSION
        or asset['namespace'] != profile.namespace
        or asset['upstream_revision'] != UPSTREAM_REVISION
        or asset['workload_revision'] != profile.workload_revision
        or asset['released'] is not False
    ):
        raise CandidateWorkloadError('Candidate asset identity or release state drifted.')


def validate_candidate_assets(
    workload_id: str,
) -> tuple[tuple[Mapping[str, Any], ...], tuple[tuple[str, str, str, int], ...]]:
    """Validate checked-in inventories and edges against immutable audit hashes."""

    profile = _candidate_profile(workload_id)
    asset = _read_asset(
        profile.asset_directory / 'components.json', profile.component_asset_sha256,
    )
    _asset_identity(asset, profile, 'components')
    components = _array(asset['components'], 'Candidate components')
    names = []
    by_name = {}
    for component in components:
        _fields(component, {
            'name', 'image_key', 'role', 'command', 'environment', 'ports',
            'service_type', 'external_traffic_policy', 'storage_subdir',
        }, 'Candidate component')
        name = component['name']
        if not isinstance(name, str) or not _DNS_LABEL.fullmatch(name):
            raise CandidateWorkloadError('Candidate component name is invalid.')
        if (
            name not in profile.expected_component_placement
            or component['role'] != profile.expected_component_placement[name]
            or component['image_key'] not in profile.required_image_keys
        ):
            raise CandidateWorkloadError(f'Candidate component {name} identity drifted.')
        command = component['command']
        if command is not None:
            values = _array(command, f'{name} command')
            if not 1 <= len(values) <= 32 or any(
                not isinstance(item, str) or not item or len(item) > 256
                or any(ord(character) < 32 or ord(character) > 126 for character in item)
                for item in values
            ):
                raise CandidateWorkloadError(f'{name} command must contain bounded argv strings.')
            if Path(values[0]).name in {'sh', 'bash', 'dash', 'sudo'}:
                raise CandidateWorkloadError('Candidate commands must not invoke a shell.')
        environment = component['environment']
        if not isinstance(environment, Mapping) or any(
            not isinstance(key, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', key)
            or not isinstance(value, str) or len(value) > 2048 or '\0' in value
            for key, value in environment.items()
        ):
            raise CandidateWorkloadError(f'{name} environment is invalid.')
        ports = _array(component['ports'], f'{name} ports')
        if not ports:
            raise CandidateWorkloadError(f'{name} requires a readiness port.')
        port_names = []
        for port in ports:
            _fields(port, {
                'name', 'container_port', 'service_port', 'protocol', 'node_port',
            }, f'{name} port')
            if not isinstance(port['name'], str) or not _DNS_LABEL.fullmatch(port['name']):
                raise CandidateWorkloadError(f'{name} port name is invalid.')
            if port['protocol'] not in {'TCP', 'UDP'} or any(
                type(port[field]) is not int or not 1 <= port[field] <= 65535
                for field in ('container_port', 'service_port')
            ):
                raise CandidateWorkloadError(f'{name} port is invalid.')
            port_names.append(port['name'])
            if port['node_port'] is not None and (
                name != profile.frontend_component
                or type(port['node_port']) is not int
                or port['node_port'] != profile.frontend_node_port
            ):
                raise CandidateWorkloadError('Only the audited frontend may expose a NodePort.')
        if len(port_names) != len(set(port_names)):
            raise CandidateWorkloadError(f'{name} repeats a port name.')
        if not any(port['protocol'] == 'TCP' for port in ports):
            raise CandidateWorkloadError(f'{name} requires a TCP readiness port.')
        frontend = name == profile.frontend_component
        expected_type = 'NodePort' if frontend else 'ClusterIP'
        expected_policy = 'Local' if frontend else None
        if (
            component['service_type'] != expected_type
            or component['external_traffic_policy'] != expected_policy
        ):
            raise CandidateWorkloadError(f'{name} service boundary drifted.')
        if frontend and ports != [{
            'name': 'http', 'container_port': profile.frontend_container_port,
            'service_port': profile.frontend_service_port, 'protocol': 'TCP',
            'node_port': profile.frontend_node_port,
        }]:
            raise CandidateWorkloadError('Candidate frontend port mapping drifted.')
        expected_storage = name if component['role'] == 'database' else None
        if component['storage_subdir'] != expected_storage or (
            expected_storage is not None and component['image_key'] != 'mongodb'
        ):
            raise CandidateWorkloadError(f'{name} database storage binding drifted.')
        names.append(name)
        by_name[name] = component
    if (
        names != sorted(profile.expected_component_placement)
        or len(names) != len(set(names))
    ):
        raise CandidateWorkloadError('Candidate component inventory drifted.')
    if sum(component['role'] == 'database' for component in components) * 10 > 100:
        raise CandidateWorkloadError('Candidate database claims exceed the dedicated disk.')

    policy_asset = _read_asset(
        profile.asset_directory / 'network-policies.json', profile.network_policy_asset_sha256,
    )
    _asset_identity(policy_asset, profile, 'edges')
    edges = []
    for edge in _array(policy_asset['edges'], 'Candidate dependency edges'):
        _fields(edge, {'source', 'destination', 'protocol', 'port'}, 'Candidate edge')
        if edge['source'] not in by_name or edge['destination'] not in by_name:
            raise CandidateWorkloadError('Candidate edge references an unknown component.')
        if (edge['protocol'], edge['port']) not in {
            (port['protocol'], port['container_port'])
            for port in by_name[edge['destination']]['ports']
        }:
            raise CandidateWorkloadError('Candidate edge does not match its destination port.')
        if type(edge['port']) is not int:
            raise CandidateWorkloadError('Candidate edge port must be an integer.')
        edges.append((edge['source'], edge['destination'], edge['protocol'], edge['port']))
    if not edges or edges != sorted(set(edges)):
        raise CandidateWorkloadError('Candidate dependency edges must be sorted and unique.')
    # Return copies. Neither callers nor parallel renders can alter checked-in data.
    return tuple(json.loads(_canonical(components))), tuple(edges)


def validate_candidate_images(workload_id: str, value: Any) -> Mapping[str, Mapping[str, str]]:
    """Validate preview references, not publication or a public release receipt.

    No example/placeholder image manifest is saved as qualification evidence.
    Real publication, driver attestation, and cloud qualification are separate
    release gates and cannot be satisfied by this syntactic validation.
    """

    profile = _candidate_profile(workload_id)
    manifest = _fields(value, {
        'schema_version', 'workload_id', 'workload_revision',
        'image_set_revision', 'upstream_revision', 'released', 'platforms',
    }, 'Candidate image manifest')
    if (
        type(manifest['schema_version']) is not int
        or manifest['schema_version'] != CANDIDATE_IMAGE_SCHEMA_VERSION
        or manifest['workload_id'] != workload_id
        or manifest['workload_revision'] != profile.workload_revision
        or manifest['image_set_revision'] != profile.image_set_revision
        or manifest['upstream_revision'] != UPSTREAM_REVISION
        or manifest['released'] is not False
    ):
        raise CandidateWorkloadError('Candidate image manifest identity or release state drifted.')
    platforms = _fields(manifest['platforms'], set(_ARCHITECTURES.values()), 'Candidate platforms')
    selected = {}
    seen = set()
    for platform in sorted(platforms):
        platform_value = _fields(platforms[platform], {'images'}, 'Candidate platform')
        # Validate and retain the same snapshot, never a caller-owned mapping
        # that could change between validation, rendering, and fingerprinting.
        images = dict(_fields(
            platform_value['images'], set(profile.required_image_keys), 'Candidate images',
        ))
        for reference in images.values():
            if (
                not isinstance(reference, str) or not _IMAGE_REFERENCE.fullmatch(reference)
                or reference.endswith('0' * 64) or reference in seen
            ):
                raise CandidateWorkloadError('Candidate images require distinct nonzero manifest digests.')
            seen.add(reference)
        selected[platform] = MappingProxyType(images)
    for key in profile.required_image_keys:
        if selected['linux/amd64'][key].split('@', 1)[0] != selected['linux/arm64'][key].split('@', 1)[0]:
            raise CandidateWorkloadError('Both platforms must use the same image repository.')
    return MappingProxyType(selected)


@dataclass(frozen=True, slots=True)
class CandidateWorkloadBundle:
    workload_id: str
    namespace: str
    application_architecture: str
    manifest_source_sha256: str
    rendered_manifest_sha256: str
    image_manifest_sha256: str
    load_generator_cidr: str
    canonical_json: str
    phase_payloads: tuple[str, ...]
    expected_component_placement: Mapping[str, str]
    released: bool = False

    @property
    def phases(self) -> tuple[tuple[str, str], ...]:
        return tuple(zip(
            ('namespace', 'storage', 'policies', 'services', 'workloads'),
            self.phase_payloads, strict=True,
        ))


def render_candidate_workload_bundle(
    workload_id: str,
    image_manifest: Mapping[str, Any],
    application_architecture: str,
    load_generator_ip: str,
) -> CandidateWorkloadBundle:
    """Render audited Hotel/Media manifests offline without enabling execution."""

    profile = _candidate_profile(workload_id)
    components, edges = validate_candidate_assets(workload_id)
    images = validate_candidate_images(workload_id, image_manifest)
    image_snapshot = {
        'schema_version': CANDIDATE_IMAGE_SCHEMA_VERSION,
        'workload_id': profile.workload_id,
        'workload_revision': profile.workload_revision,
        'image_set_revision': profile.image_set_revision,
        'upstream_revision': UPSTREAM_REVISION,
        'released': False,
        'platforms': {
            platform: {'images': dict(references)}
            for platform, references in images.items()
        },
    }
    if not isinstance(application_architecture, str):
        raise CandidateWorkloadError('Candidate application architecture must be x86_64 or aarch64.')
    architecture = _ARCHITECTURE_ALIASES.get(application_architecture, application_architecture)
    if architecture not in _ARCHITECTURES:
        raise CandidateWorkloadError('Candidate application architecture must be x86_64 or aarch64.')
    try:
        address = ipaddress.ip_address(load_generator_ip)
    except (TypeError, ValueError) as exc:
        raise CandidateWorkloadError('Candidate load-generator IP must be an exact private IPv4 address.') from exc
    if (
        not isinstance(load_generator_ip, str) or str(address) != load_generator_ip
        or address.version != 4
        or not any(address in network for network in _PRIVATE_NETWORKS)
    ):
        raise CandidateWorkloadError('Candidate load-generator IP must be an exact private IPv4 address.')
    cidr = f'{address}/32'
    annotations = {
        'deathstarbench.io/workload-revision': profile.workload_revision,
        'deathstarbench.io/image-set-revision': profile.image_set_revision,
        'deathstarbench.io/upstream-revision': UPSTREAM_REVISION,
        'deathstarbench.io/release-state': 'candidate',
    }

    def labels(component=None):
        values = {'deathstarbench.io/workload': profile.workload_label}
        if component is not None:
            values.update({
                'app.kubernetes.io/component': component['name'],
                'deathstarbench.io/role': component['role'],
            })
        return values

    def metadata(name, component=None, *, cluster=False):
        values = {'name': name, 'labels': labels(component), 'annotations': dict(annotations)}
        if not cluster:
            values['namespace'] = profile.namespace
        return values

    def selector(name):
        return {'matchLabels': {'app.kubernetes.io/component': name, **labels()}}

    namespace = [{
        'apiVersion': 'v1', 'kind': 'Namespace',
        'metadata': {
            **metadata(profile.namespace, cluster=True),
            'labels': {'kubernetes.io/metadata.name': profile.namespace, **labels()},
        },
    }]
    storage, services, workloads, policies = [], [], [], []
    for component in components:
        name, role = component['name'], component['role']
        component_arch = architecture if role == 'application' else 'x86_64'
        platform = _ARCHITECTURES[component_arch]
        image = images[platform][component['image_key']]
        container = {
            'name': name, 'image': image, 'imagePullPolicy': 'IfNotPresent',
            'env': [{'name': key, 'value': value} for key, value in sorted(component['environment'].items())],
            'ports': [{
                'name': port['name'], 'containerPort': port['container_port'], 'protocol': port['protocol'],
            } for port in component['ports']],
            'readinessProbe': {
                'tcpSocket': {'port': min(port['container_port'] for port in component['ports'] if port['protocol'] == 'TCP')},
                'failureThreshold': 12, 'periodSeconds': 5, 'timeoutSeconds': 2,
            },
        }
        if component['command'] is not None:
            container['command'] = list(component['command'])
        pod_spec = {
            'automountServiceAccountToken': False, 'enableServiceLinks': False,
            'containers': [container], 'terminationGracePeriodSeconds': 30,
            'nodeSelector': {
                'deathstarbench.io/role': role,
                'kubernetes.io/arch': 'amd64' if component_arch == 'x86_64' else 'arm64',
            },
        }
        if role == 'control':
            pod_spec['tolerations'] = [{
                'key': 'node-role.kubernetes.io/control-plane', 'operator': 'Equal',
                'value': 'true', 'effect': 'NoSchedule',
            }]
        if component['storage_subdir'] is not None:
            volume_name = f'{profile.namespace}-{name}'
            storage.extend(({
                'apiVersion': 'v1', 'kind': 'PersistentVolume',
                'metadata': metadata(volume_name, component, cluster=True),
                'spec': {
                    'accessModes': ['ReadWriteOnce'], 'capacity': {'storage': '10Gi'},
                    'claimRef': {'name': name, 'namespace': profile.namespace},
                    'hostPath': {'path': f'{DATABASE_ROOT}/{name}', 'type': 'Directory'},
                    'nodeAffinity': {'required': {'nodeSelectorTerms': [{'matchExpressions': [{
                        'key': 'deathstarbench.io/role', 'operator': 'In', 'values': ['database'],
                    }]}]}},
                    'persistentVolumeReclaimPolicy': 'Retain', 'storageClassName': '', 'volumeMode': 'Filesystem',
                },
            }, {
                'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim', 'metadata': metadata(name, component),
                'spec': {
                    'accessModes': ['ReadWriteOnce'], 'resources': {'requests': {'storage': '10Gi'}},
                    'storageClassName': '', 'volumeMode': 'Filesystem', 'volumeName': volume_name,
                },
            }))
            container['volumeMounts'] = [{'name': 'database-data', 'mountPath': '/data/db'}]
            pod_spec['volumes'] = [{'name': 'database-data', 'persistentVolumeClaim': {'claimName': name}}]
        workloads.append({
            'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': metadata(name, component),
            'spec': {
                'replicas': 1, 'revisionHistoryLimit': 0, 'strategy': {'type': 'Recreate'},
                'selector': {'matchLabels': labels(component)},
                'template': {
                    'metadata': {'labels': labels(component), 'annotations': {
                        'deathstarbench.io/architecture': component_arch,
                        'deathstarbench.io/image-reference': image,
                    }},
                    'spec': pod_spec,
                },
            },
        })
        service_spec = {
            'type': component['service_type'], 'selector': labels(component),
            'ports': [{
                'name': port['name'], 'port': port['service_port'],
                'targetPort': port['container_port'], 'protocol': port['protocol'],
                **({'nodePort': port['node_port']} if port['node_port'] is not None else {}),
            } for port in component['ports']],
        }
        if component['external_traffic_policy'] is not None:
            service_spec['externalTrafficPolicy'] = component['external_traffic_policy']
        services.append({'apiVersion': 'v1', 'kind': 'Service', 'metadata': metadata(name, component), 'spec': service_spec})

    def policy(name, spec):
        policies.append({
            'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
            'metadata': metadata(name), 'spec': spec,
        })

    for source in sorted({edge[0] for edge in edges}):
        policy(f'allow-egress-from-{source}', {
            'podSelector': selector(source), 'policyTypes': ['Egress'],
            'egress': [{
                'to': [{'podSelector': selector(destination)}], 'ports': [{'port': port, 'protocol': protocol}],
            } for origin, destination, protocol, port in edges if origin == source],
        })
    for destination in sorted({edge[1] for edge in edges}):
        policy(f'allow-ingress-to-{destination}', {
            'podSelector': selector(destination), 'policyTypes': ['Ingress'],
            'ingress': [{
                'from': [{'podSelector': selector(source)}], 'ports': [{'port': port, 'protocol': protocol}],
            } for source, target, protocol, port in edges if target == destination],
        })
    policy('default-deny-all', {'podSelector': {}, 'policyTypes': ['Ingress', 'Egress'], 'ingress': [], 'egress': []})
    policy('allow-dns-egress', {
        'podSelector': {}, 'policyTypes': ['Egress'], 'egress': [{
            'to': [{
                'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'kube-system'}},
                'podSelector': {'matchLabels': {'k8s-app': 'kube-dns'}},
            }],
            'ports': [{'port': 53, 'protocol': 'TCP'}, {'port': 53, 'protocol': 'UDP'}],
        }],
    })
    policy('allow-load-generator-frontend', {
        'podSelector': selector(profile.frontend_component), 'policyTypes': ['Ingress'],
        'ingress': [{
            'from': [{'ipBlock': {'cidr': cidr}}],
            'ports': [{'port': profile.frontend_container_port, 'protocol': 'TCP'}],
        }],
    })
    for documents in (storage, policies, services, workloads):
        documents.sort(key=lambda item: (item['kind'], item['metadata']['name']))
    phases = tuple(
        _canonical({'apiVersion': 'v1', 'kind': 'List', 'items': values})
        for values in (namespace, storage, policies, services, workloads)
    )
    canonical_json = _canonical({
        'apiVersion': 'v1', 'kind': 'List',
        'items': [item for values in (namespace, storage, policies, services, workloads) for item in values],
    })
    return CandidateWorkloadBundle(
        workload_id=workload_id, namespace=profile.namespace, application_architecture=architecture,
        manifest_source_sha256=_fingerprint({
            'components': profile.component_asset_sha256, 'policies': profile.network_policy_asset_sha256,
        }),
        rendered_manifest_sha256='sha256:' + hashlib.sha256(canonical_json.encode()).hexdigest(),
        image_manifest_sha256=_fingerprint(image_snapshot), load_generator_cidr=cidr,
        canonical_json=canonical_json, phase_payloads=phases,
        expected_component_placement=MappingProxyType(dict(profile.expected_component_placement)),
    )
