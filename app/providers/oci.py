"""Operator-only OCI distributed infrastructure candidate; no runtime release.

This adapter deliberately does not participate in the application dispatcher.
It owns a dedicated five-node graph, never compact-run or pre-existing cloud
resources. Callers must hold the run lease and supply durable persistence.
Credentials are supplied through SDK clients and are never persisted.
"""

from __future__ import annotations

import copy
from dataclasses import replace
from datetime import datetime, timezone
from ipaddress import ip_address, ip_network
import math
import re
import time
import uuid

import oci as sdk

from ..deathstarbench_contract import (
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY, DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DISTRIBUTED_TIERED_TOPOLOGY_ID, K3S_RUNTIME_ID, K3S_RUNTIME_JOURNAL_KEY,
)
from ..deathstarbench_topology import DeathStarBenchTopologyManifest, build_topology_manifest
from ..resource_inventory import ROLE_NODE_INVENTORY_KEY, RoleNodeInventory, StorageResource, persist_role_node_inventory


CONTRACT_KEY = 'oci_dsb_infrastructure'
SCHEMA_VERSION = 2
MANAGED_BY = 'oci-self-service-benchmarks'
SSH_USER = 'opc'
SUPPORT_SHAPE = 'VM.Standard.E5.Flex'
PRIVATE_ADDRESSES = {
    'control': '10.240.1.10', 'application': '10.240.1.13',
    'database': '10.240.3.11', 'cache': '10.240.3.12',
    'load-generator': '10.240.2.10',
}
CAPACITIES = {'control': (1, 4), 'database': (2, 16), 'cache': (1, 8), 'load-generator': (1, 8)}
PUBLIC_ROLES = frozenset({'control', 'application', 'load-generator'})
DATABASE_DEVICE = '/dev/oracleoci/oraclevdb'
DATABASE_MOUNT_POINT = '/var/lib/deathstarbench/database'
TOPOLOGY_MANIFEST_KEY = 'deathstarbench_topology_manifest'
TOPOLOGY_FINGERPRINT_KEY = 'deathstarbench_topology_fingerprint'
RUNTIME_ALIAS_KEYS = frozenset({
    'provider', 'ssh_user', 'oci_distributed_candidate',
    TOPOLOGY_MANIFEST_KEY, TOPOLOGY_FINGERPRINT_KEY,
    'oci_compartment_id', 'oci_availability_domain', 'region',
    'oci_dsb_database_volume_id', 'oci_dsb_database_attachment_id',
    'oci_dsb_database_device', 'oci_dsb_database_mount_point',
    *(f'oci_dsb_{role.replace("-", "_")}_{kind}'
      for role in PRIVATE_ADDRESSES for kind in ('instance_id', 'private_ip', 'public_ip')),
})
_NON_CLOUD_EVIDENCE_KEYS = frozenset({
    K3S_RUNTIME_JOURNAL_KEY, DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    'deathstarbench_distributed_network_qualification',
})
WAIT_SECONDS = 1800
ETAG_DELETE_ATTEMPTS = 3
READ_THROTTLE_RETRY_DELAYS = (1, 2, 4, 8, 16)


class LifecycleError(RuntimeError):
    """Ownership, mutation ambiguity, or immutable graph validation failed."""


def _dict(value):
    value = copy.deepcopy(value) if isinstance(value, dict) else sdk.util.to_dict(value)
    def normalize(item):
        if isinstance(item, datetime):
            return item.isoformat()
        if isinstance(item, dict):
            return {key: normalize(val) for key, val in item.items()}
        if isinstance(item, list):
            return [normalize(val) for val in item]
        return item
    return normalize(value)


def _model(name, value):
    """Build SDK models recursively so snake-case fields serialize correctly."""
    if name == 'InstanceSourceDetails':
        name = 'InstanceSourceViaImageDetails'
    model = getattr(sdk.core.models, name)()
    for key, item in value.items():
        if key not in model.swagger_types:
            raise LifecycleError(f'Unknown SDK {name} field: {key}')
        kind = model.swagger_types[key]
        if isinstance(item, dict) and not kind.startswith('dict('):
            item = _model(kind, item)
        elif isinstance(item, list) and kind.startswith('list['):
            inner = kind[5:-1]
            item = [_model(inner, entry) if isinstance(entry, dict) else entry for entry in item]
        setattr(model, key, item)
    return model


def _now():
    return datetime.now(timezone.utc).isoformat()


def _read(client, method, *args, **kwargs):
    """Run one idempotent OCI read with bounded tenant-throttle retries."""

    for attempt in range(len(READ_THROTTLE_RETRY_DELAYS) + 1):
        try:
            return getattr(client, method)(*args, **kwargs)
        except sdk.exceptions.ServiceError as exc:
            throttled = exc.status == 429 and exc.code == 'TooManyRequests'
            if not throttled or attempt == len(READ_THROTTLE_RETRY_DELAYS):
                raise
            time.sleep(READ_THROTTLE_RETRY_DELAYS[attempt])


def _list(client, method, **kwargs):
    result = []
    page = None
    while True:
        response = _read(
            client,
            method,
            **kwargs,
            **({'page': page} if page else {}),
        )
        result.extend(_dict(item) for item in response.data)
        page = response.headers.get('opc-next-page')
        if not page:
            return result


def _get(client, method, resource_id):
    try:
        response = _read(client, method, resource_id)
    except sdk.exceptions.ServiceError as exc:
        if exc.status == 404:
            return None
        raise
    return _dict(response.data), response.headers.get('etag')


def _assert_subset(expected, actual, label):
    for key, value in expected.items():
        if isinstance(value, dict):
            _assert_subset(value, actual.get(key) or {}, f'{label}.{key}')
        elif isinstance(value, list) and all(isinstance(item, dict) for item in value):
            observed = actual.get(key)
            if not isinstance(observed, list) or len(value) != len(observed):
                raise LifecycleError(f'{label}.{key} list membership changed.')
            for wanted, found in zip(value, observed):
                _assert_subset(wanted, found, f'{label}.{key}')
        elif actual.get(key) != value:
            raise LifecycleError(f'{label}.{key} no longer matches the saved contract.')


def _validate_inputs(inputs):
    expected = {'compartment_id', 'availability_domain', 'region', 'shape', 'architecture',
                'ocpus', 'memory_gb', 'application_image_id', 'support_image_id', 'public_key',
                'defined_tags'}
    if set(inputs) != expected:
        raise LifecycleError('OCI candidate requires an exact explicit input contract.')
    for key in ('compartment_id', 'application_image_id', 'support_image_id'):
        prefix = 'compartment' if key == 'compartment_id' else 'image'
        if not re.fullmatch(rf'ocid1\.{prefix}\.[a-zA-Z0-9._-]+', str(inputs[key])):
            raise LifecycleError(f'Invalid pinned {key}.')
    for key in ('availability_domain', 'region', 'shape'):
        if not isinstance(inputs[key], str) or not inputs[key].strip():
            raise LifecycleError(f'Missing {key}.')
    if inputs['architecture'] not in {'arm64', 'x86_64'}:
        raise LifecycleError('Unsupported application architecture.')
    if not re.fullmatch(r'VM\.Standard\.(?:E[456]|A[124])\.Flex', inputs['shape']):
        raise LifecycleError('Candidate requires a supported Standard Flex shape.')
    expected_arch = 'arm64' if '.A' in inputs['shape'] else 'x86_64'
    if inputs['architecture'] != expected_arch:
        raise LifecycleError('Shape architecture conflicts with the candidate contract.')
    for key in ('ocpus', 'memory_gb'):
        if type(inputs[key]) not in (int, float) or not math.isfinite(inputs[key]) or inputs[key] <= 0:
            raise LifecycleError(f'Invalid {key}.')
    if not re.fullmatch(r'(?:ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp256) [A-Za-z0-9+/=]+(?: [^\r\n]+)?', inputs['public_key']):
        raise LifecycleError('A single SSH public key is required.')
    _validate_defined_tags(inputs['defined_tags'])


def _validate_defined_tags(value):
    """Require a small deterministic OCI defined-tag map.

    Some tenancies reject creates unless compartment tag defaults are supplied
    explicitly. Keeping the exact namespace/key/value map inside the durable
    candidate contract makes retries reproducible and prevents an operator
    resume from silently inheriting a changed tag policy.
    """
    if not isinstance(value, dict) or len(value) > 64:
        raise LifecycleError('OCI defined_tags must be an object with at most 64 namespaces.')
    count = 0
    for namespace, tags in value.items():
        if (not _valid_tag_name(namespace) or not isinstance(tags, dict)
                or not tags or len(tags) > 64):
            raise LifecycleError('OCI defined_tags contains an invalid namespace or tag map.')
        for key, tag_value in tags.items():
            count += 1
            if (not _valid_tag_name(key) or not isinstance(tag_value, str)
                    or len(tag_value.encode('utf-8')) > 256
                    or '\x00' in tag_value):
                raise LifecycleError('OCI defined_tags contains an invalid key or value.')
    if count > 64:
        raise LifecycleError('OCI defined_tags exceeds the 64-tag resource limit.')


def _valid_tag_name(value):
    return (isinstance(value, str) and 0 < len(value) <= 100
            and all(33 <= ord(char) <= 126 and char != '.' for char in value))


def _manifest(inputs):
    return build_topology_manifest(DISTRIBUTED_TIERED_TOPOLOGY_ID, K3S_RUNTIME_ID,
                                   selected_shape=inputs['shape'], selected_architecture=inputs['architecture'])


def _ref(key):
    return {'$ref': key}


def _resolve(value, entries):
    if isinstance(value, dict):
        if set(value) == {'$ref'}:
            key, _, implicit = value['$ref'].partition('/')
            identity = (entries[key].get('implicit', {}).get(implicit, {}).get('id')
                        if implicit else entries[key].get('id'))
            if not identity:
                raise LifecycleError(f'Unresolved dependency {value["$ref"]}.')
            return identity
        return {key: _resolve(item, entries) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve(item, entries) for item in value]
    return value


def _specs(job_id, inputs):
    """All creates and their immutable request bodies, before any mutation."""
    specs = {}
    def add(key, kind, model, body, client='network'):
        if kind != 'volume_attachment':
            body = {'compartment_id': inputs['compartment_id'], **body,
                    'freeform_tags': {'managed-by': MANAGED_BY, 'benchmark-job': job_id, 'benchmark-role': key},
                    'defined_tags': copy.deepcopy(inputs['defined_tags'])}
        body['display_name'] = f'bench-{job_id}-dsb-{key}'
        specs[key] = {'kind': kind, 'client': client, 'model': model, 'body': body}
    add('vcn', 'vcn', 'CreateVcnDetails', {'cidr_block': '10.240.0.0/16'})
    add('igw', 'internet_gateway', 'CreateInternetGatewayDetails', {'vcn_id': _ref('vcn'), 'is_enabled': True})
    add('nat', 'nat_gateway', 'CreateNatGatewayDetails', {'vcn_id': _ref('vcn'), 'block_traffic': False})
    for key, gateway in (('public-route', 'igw'), ('private-route', 'nat')):
        add(key, 'route_table', 'CreateRouteTableDetails', {'vcn_id': _ref('vcn'), 'route_rules': [
            {'destination': '0.0.0.0/0', 'destination_type': 'CIDR_BLOCK', 'network_entity_id': _ref(gateway)}]})
    add('empty-security', 'security_list', 'CreateSecurityListDetails', {'vcn_id': _ref('vcn'),
        'ingress_security_rules': [], 'egress_security_rules': []})
    for key, cidr, private in (('management', '10.240.1.0/24', False), ('loadgen', '10.240.2.0/24', False), ('data', '10.240.3.0/24', True)):
        add(key, 'subnet', 'CreateSubnetDetails', {'vcn_id': _ref('vcn'), 'cidr_block': cidr,
            'availability_domain': inputs['availability_domain'], 'prohibit_public_ip_on_vnic': private,
            'dhcp_options_id': _ref('vcn/dhcp_options'),
            'route_table_id': _ref('private-route' if private else 'public-route'),
            'security_list_ids': [_ref('empty-security')]})
    for role in PRIVATE_ADDRESSES:
        add(f'nsg-{role}', 'network_security_group', 'CreateNetworkSecurityGroupDetails', {'vcn_id': _ref('vcn')})
    add('database-data', 'volume', 'CreateVolumeDetails', {'availability_domain': inputs['availability_domain'],
        'size_in_gbs': 256, 'vpus_per_gb': 20, 'is_auto_tune_enabled': False, 'autotune_policies': []}, 'block')
    for role in _manifest(inputs).creation_order:
        ocpus, memory = (inputs['ocpus'], inputs['memory_gb']) if role == 'application' else CAPACITIES[role]
        subnet = 'data' if role in {'database', 'cache'} else 'loadgen' if role == 'load-generator' else 'management'
        add(role, 'instance', 'LaunchInstanceDetails', {'availability_domain': inputs['availability_domain'],
            'shape': inputs['shape'] if role == 'application' else SUPPORT_SHAPE,
            'shape_config': {'ocpus': ocpus, 'memory_in_gbs': memory},
            'source_details': {'source_type': 'image', 'image_id': inputs['application_image_id'] if role == 'application' else inputs['support_image_id'], 'boot_volume_size_in_gbs': 50},
            'metadata': {'ssh_authorized_keys': inputs['public_key']},
            'create_vnic_details': {'subnet_id': _ref(subnet), 'private_ip': PRIVATE_ADDRESSES[role],
                'assign_public_ip': role in PUBLIC_ROLES, 'nsg_ids': [_ref(f'nsg-{role}')], 'skip_source_dest_check': False}}, 'compute')
    add('database-attachment', 'volume_attachment', 'AttachParavirtualizedVolumeDetails', {
        'instance_id': _ref('database'), 'volume_id': _ref('database-data'), 'type': 'paravirtualized',
        'device': DATABASE_DEVICE, 'is_read_only': False, 'is_shareable': False}, 'compute')
    return specs


def _rules(role, manifest):
    """Render the persisted reachability contract plus explicit SSH management."""
    topology = DeathStarBenchTopologyManifest.from_dict(manifest)
    if topology.topology_id != DISTRIBUTED_TIERED_TOPOLOGY_ID or role not in PRIVATE_ADDRESSES:
        raise LifecycleError('NSG rules require an exact distributed role manifest.')
    channels = {channel.key: channel for channel in topology.network_policy.channels}
    rules = [{'direction': 'EGRESS', 'protocol': 'all', 'destination': '0.0.0.0/0', 'destination_type': 'CIDR_BLOCK', 'is_stateless': False}]
    def ingress(source, port, protocol='6'):
        rules.append({'direction': 'INGRESS', 'protocol': protocol, 'source': source,
                      'source_type': 'CIDR_BLOCK', 'is_stateless': False,
                      ('tcp_options' if protocol == '6' else 'udp_options'): {'destination_port_range': {'min': port, 'max': port}}})
    if role in PUBLIC_ROLES:
        ingress('0.0.0.0/0', 22)
    else:
        for source in ('control', 'application'):
            ingress(PRIVATE_ADDRESSES[source] + '/32', 22)
    for reachability in topology.network_policy.rules:
        if reachability.destination != role:
            continue
        for channel_key in reachability.channels:
            channel = channels[channel_key]
            protocol = {'tcp': '6', 'udp': '17'}[channel.protocol]
            for port in channel.ports:
                ingress(PRIVATE_ADDRESSES[reachability.source] + '/32', port, protocol)
    return rules


def _confirmed_create_rejection(exc):
    """Only explicit non-acceptance codes; never infer absence from a 404.

    Creates disable SDK retries so this cannot be a rejection of an automatic
    replay following an earlier response-lost acceptance. Unknown errors stay
    ambiguous and retain the original creating intent for reconciliation.
    """
    definite_codes = {
        400: {'CannotParseRequest', 'InvalidParameter', 'LimitExceeded', 'MissingParameter', 'QuotaExceeded'},
        401: {'NotAuthenticated'},
        403: {'NotAllowed', 'NotAuthorized', 'SignUpRequired'},
        405: {'MethodNotAllowed'},
        413: {'RequestEntityTooLarge'},
        422: {'UnprocessableEntity'},
        429: {'TooManyRequests'},
    }
    return isinstance(exc, sdk.exceptions.ServiceError) and exc.code in definite_codes.get(exc.status, set())


def _canonical_rule(rule):
    return {key: _canonical_rule(value) if isinstance(value, dict) else value
            for key, value in rule.items() if value is not None and key not in {'id', 'time_created', 'is_valid'}}


def _check_rules(client, nsg_id, expected, *, allow_empty=False):
    actual = [_canonical_rule(item) for item in _list(client, 'list_network_security_group_security_rules', network_security_group_id=nsg_id)]
    if allow_empty and not actual:
        return False
    if len(actual) != len(expected) or any(item not in actual for item in expected):
        raise LifecycleError('NSG rules differ from the exact role allowlist; refusing mutation.')
    return True


def _preflight(inputs, clients):
    images = {}
    for role, image_id, arch, shape in (
        ('application', inputs['application_image_id'], inputs['architecture'], inputs['shape']),
        ('support', inputs['support_image_id'], 'x86_64', SUPPORT_SHAPE),
    ):
        response = _read(clients['compute'], 'get_image', image_id)
        image = _dict(response.data)
        name = image.get('display_name', '')
        if (image.get('id') != image_id or image.get('operating_system') != 'Oracle Linux'
                or not str(image.get('operating_system_version', '')).startswith('9')
                or not name.startswith('Oracle-Linux-9') or image.get('compartment_id') is not None
                or image.get('lifecycle_state') != 'AVAILABLE'
                or ('aarch64' in name.lower()) != (arch == 'arm64')):
            raise LifecycleError(f'{role} image is not a compatible pinned Oracle Linux 9 platform image.')
        compatible = _list(clients['compute'], 'list_image_shape_compatibility_entries', image_id=image_id)
        if shape not in {item.get('shape') for item in compatible}:
            raise LifecycleError(f'{role} image does not support {shape}.')
        images[role] = image
    shapes = _list(clients['compute'], 'list_shapes', compartment_id=inputs['compartment_id'], availability_domain=inputs['availability_domain'])
    for role in PRIVATE_ADDRESSES:
        shape_name = inputs['shape'] if role == 'application' else SUPPORT_SHAPE
        ocpus, memory = (inputs['ocpus'], inputs['memory_gb']) if role == 'application' else CAPACITIES[role]
        matches = [item for item in shapes if item.get('shape') == shape_name]
        if len(matches) != 1 or matches[0].get('is_flexible') is not True:
            raise LifecycleError(f'{role} shape is not available in the selected AD.')
        shape = matches[0]
        cpu_range, mem_range = shape.get('ocpu_options') or {}, shape.get('memory_options') or {}
        if not (cpu_range.get('min', math.inf) <= ocpus <= cpu_range.get('max', 0)
                and mem_range.get('min_in_g_bs', math.inf) <= memory <= mem_range.get('max_in_g_bs', 0)
                and mem_range.get('min_per_ocpu_in_gbs', math.inf) <= memory / ocpus <= mem_range.get('max_per_ocpu_in_gbs', 0)):
            raise LifecycleError(f'{role} capacity is outside the provider shape range.')
    return images


def _inventory(contract):
    manifest = _manifest(contract['inputs'])
    nodes = []
    for node in manifest.planned_inventory('oci').nodes:
        entry = contract['entries'][node.key]
        implicit = entry.get('implicit', {})
        vnic = implicit.get('vnic', {}).get('snapshot', {})
        storage = ()
        if node.key == 'database':
            disk = contract['entries']['database-data']
            storage = (StorageResource(key='database-data', kind='block_volume', provider_resource_id=disk.get('id'),
                provider_resource_name=disk['spec']['body']['display_name'], model='oci_higher_performance_20_vpus',
                size_gb=256, provisioned_iops=19200, provisioned_throughput_mibps=146.484375,
                device=DATABASE_DEVICE, mount_point=DATABASE_MOUNT_POINT, ephemeral=False,
                lifecycle_status='deleted' if disk['status'] == 'deleted' else 'running' if disk.get('id') else 'planned'),)
        nodes.append(replace(node, provider_resource_id=entry.get('id'), provider_resource_name=entry['spec']['body']['display_name'],
            zone=contract['inputs']['availability_domain'], shape=entry['spec']['body']['shape'],
            architecture=contract['inputs']['architecture'] if node.key == 'application' else 'x86_64',
            public_addresses=(vnic['public_ip'],) if vnic.get('public_ip') else (), private_addresses=(PRIVATE_ADDRESSES[node.key],),
            storage=storage, lifecycle_status='deleted' if entry['status'] == 'deleted' else 'running' if entry['status'] == 'ready' else 'planned'))
    return RoleNodeInventory(provider='oci', topology_fingerprint=manifest.fingerprint, nodes=tuple(nodes))


def _save(job, persist):
    persist_role_node_inventory(job['resources'], _inventory(job['resources'][CONTRACT_KEY]))
    persist(job)


def _load(job):
    contract = job.get('resources', {}).get(CONTRACT_KEY)
    if (not contract or type(contract.get('schema_version')) is not int
            or contract.get('schema_version') != SCHEMA_VERSION or contract.get('job_id') != job.get('id')
            or contract.get('status') not in {'planned', 'ready', 'deleting', 'deleted'}):
        raise LifecycleError('Missing or incompatible OCI candidate contract.')
    _validate_inputs(contract['inputs'])
    expected = _specs(job['id'], contract['inputs'])
    if set(contract['entries']) != set(expected) or contract['manifest'] != _manifest(contract['inputs']).as_dict():
        raise LifecycleError('OCI candidate topology or resource allowlist changed.')
    ids = []
    for key, entry in contract['entries'].items():
        if entry['spec'] != expected[key]:
            raise LifecycleError(f'OCI {key} create contract changed.')
        if entry.get('status') not in {'planned', 'creating', 'ready', 'deleting', 'deleted'}:
            raise LifecycleError('Unrecognized OCI resource lifecycle status.')
        if entry['status'] == 'planned' and entry.get('id'):
            raise LifecycleError('Planned OCI resource unexpectedly has a provider identity.')
        try:
            uuid.UUID(entry['retry_token'])
        except (ValueError, TypeError, KeyError) as exc:
            raise LifecycleError('Invalid persisted OCI retry token.') from exc
        if entry.get('id'):
            ids.append(entry['id'])
    if len(ids) != len(set(ids)):
        raise LifecycleError('OCI resource identities are not unique.')
    if job['resources'].get(ROLE_NODE_INVENTORY_KEY) != _inventory(contract).as_dict():
        raise LifecycleError('OCI role inventory conflicts with the resource graph.')
    return contract


def _require_saved_implicit_boundaries(contract, *, require_complete):
    """Validate captured identities without trusting flattened runtime aliases."""
    inputs = contract['inputs']
    entries = contract['entries']
    seen = {entry['id'] for entry in entries.values() if entry.get('id')}
    for key, entry in entries.items():
        kind = entry['spec']['kind']
        if kind not in {'vcn', 'instance'}:
            continue
        implicit = entry.get('implicit')
        if not implicit and not require_complete:
            continue
        expected_keys = {'route_table', 'security_list', 'dhcp_options'} if kind == 'vcn' else {'vnic', 'boot_volume', 'attachments'}
        if not isinstance(implicit, dict) or set(implicit) != expected_keys:
            raise LifecycleError(f'OCI {key} implicit inventory is incomplete or unknown.')
        for child_kind in expected_keys - {'attachments'}:
            child = implicit[child_kind]
            child_id = child.get('id')
            client = 'block' if child_kind == 'boot_volume' else 'network'
            if not isinstance(child_id, str) or not child_id or child_id in seen:
                raise LifecycleError('OCI implicit resource identity is missing or duplicated.')
            seen.add(child_id)
            _assert_subset({'kind': child_kind, 'client': client}, child, f'{key} implicit descriptor')
            expected = {'id': child_id, 'compartment_id': inputs['compartment_id']}
            if kind == 'vcn':
                expected['vcn_id'] = entry['id']
            else:
                expected['availability_domain'] = inputs['availability_domain']
                if child_kind == 'vnic':
                    nic = _resolve(entry['spec']['body']['create_vnic_details'], entries)
                    public = nic.pop('assign_public_ip')
                    expected.update(nic, is_primary=True)
                    address = child.get('snapshot', {}).get('public_ip')
                    if bool(address) != public:
                        raise LifecycleError(f'OCI {key} captured public address violates its role contract.')
                    if address:
                        _public_ipv4(address)
                else:
                    expected.update(image_id=entry['spec']['body']['source_details']['image_id'], size_in_gbs=50)
            _assert_subset(expected, child.get('snapshot') or {}, f'{key} captured {child_kind}')
            if not child.get('etag'):
                raise LifecycleError('OCI captured child ETag is missing.')
        if kind == 'instance':
            attachments = implicit['attachments']
            if not isinstance(attachments, dict) or set(attachments) != {'vnic', 'boot'}:
                raise LifecycleError('OCI captured attachment inventory is incomplete.')
            for attachment_key, child_kind, target in (('vnic', 'vnic', 'vnic_id'), ('boot', 'boot_volume', 'boot_volume_id')):
                attachment = attachments[attachment_key]
                attachment_id = attachment.get('id')
                # OCI paravirtualized boot attachments use the owning instance
                # OCID as their API identity. That one exact kind-scoped alias
                # is valid; every other implicit attachment remains globally
                # unique and cannot collide with a saved resource identity.
                parent_id_alias = attachment_key == 'boot' and attachment_id == entry['id']
                if (not isinstance(attachment_id, str) or not attachment_id
                        or (attachment_id in seen and not parent_id_alias)):
                    raise LifecycleError('OCI captured attachment identity is missing or duplicated.')
                if not parent_id_alias:
                    seen.add(attachment_id)
                expected = {'instance_id': entry['id'], 'compartment_id': inputs['compartment_id'],
                    'availability_domain': inputs['availability_domain'], target: implicit[child_kind]['id'],
                    'lifecycle_state': 'ATTACHED'}
                if attachment_key == 'vnic':
                    expected['subnet_id'] = implicit['vnic']['snapshot']['subnet_id']
                _assert_subset(expected, attachment, f'{key} captured attachment')


def _public_ipv4(value):
    try:
        address = ip_address(value)
    except (ValueError, TypeError) as exc:
        raise LifecycleError('OCI public SSH address is invalid.') from exc
    # Documentation ranges remain usable in deterministic tests, but private
    # underlay addresses, wildcard/loopback/link-local/multicast are not routes.
    if (not isinstance(value, str) or str(address) != value or address.version != 4
            or address.is_loopback or address.is_link_local or address.is_multicast or address.is_unspecified or address.is_reserved
            or any(address in ip_network(cidr) for cidr in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'))):
        raise LifecycleError('OCI public SSH address is not an exact public IPv4 route.')
    return value


def _runtime_projection(contract):
    entries = contract['entries']
    inputs = contract['inputs']
    aliases = {
        'provider': 'oci', 'ssh_user': SSH_USER, 'oci_distributed_candidate': True,
        TOPOLOGY_MANIFEST_KEY: copy.deepcopy(contract['manifest']),
        TOPOLOGY_FINGERPRINT_KEY: contract['manifest']['fingerprint'],
        'oci_compartment_id': inputs['compartment_id'], 'oci_availability_domain': inputs['availability_domain'],
        'region': inputs['region'], 'oci_dsb_database_volume_id': entries['database-data']['id'],
        'oci_dsb_database_attachment_id': entries['database-attachment']['id'],
        'oci_dsb_database_device': DATABASE_DEVICE, 'oci_dsb_database_mount_point': DATABASE_MOUNT_POINT,
    }
    for role, address in PRIVATE_ADDRESSES.items():
        entry = entries[role]
        snapshot = entry['implicit']['vnic']['snapshot']
        prefix = f'oci_dsb_{role.replace("-", "_")}'
        aliases.update({f'{prefix}_instance_id': entry['id'], f'{prefix}_private_ip': address,
                        f'{prefix}_public_ip': snapshot.get('public_ip') or None})
    public = [aliases[f'oci_dsb_{role.replace("-", "_")}_public_ip'] for role in PUBLIC_ROLES]
    if len(set(public)) != len(public):
        raise LifecycleError('OCI runtime roles have duplicate public addresses.')
    return aliases


def _check_runtime_aliases(resources, contract):
    present = set(resources) & RUNTIME_ALIAS_KEYS
    unexpected = {key for key in resources if key.startswith('oci_dsb_') and key != CONTRACT_KEY} - RUNTIME_ALIAS_KEYS
    if unexpected:
        raise LifecycleError('Unknown OCI runtime aliases cannot override candidate ownership.')
    if not present:
        return
    expected = _runtime_projection(contract)
    for key in present:
        if resources[key] != expected[key] or type(resources[key]) is not type(expected[key]):
            raise LifecycleError(f'OCI runtime alias {key} conflicts with the authoritative candidate contract.')


def validate_distributed_deathstarbench_candidate(job, *, clients=None, require_ready=False):
    """Return a detached exact contract; optional cloud checks are read-only.

    No missing identity is recovered, no alias is trusted or published, and no
    caller-owned journal/snapshot is modified. A caller must still own the run
    lease before using this validation as a precursor to remote mutations.
    """
    try:
        if not isinstance(job, dict) or not re.fullmatch(r'[0-9a-f]{12}', str(job.get('id', ''))):
            raise LifecycleError('An exact OCI candidate job ID is required.')
        contract = copy.deepcopy(_load(job))
        for entry in contract['entries'].values():
            if entry.get('id') and (not isinstance(entry['id'], str) or not re.fullmatch(r'ocid1\.[a-zA-Z0-9._-]+', entry['id'])):
                raise LifecycleError('OCI candidate has an invalid provider OCID.')
        images = contract.get('images')
        if not isinstance(images, dict) or set(images) != {'application', 'support'}:
            raise LifecycleError('OCI candidate has no exact pinned platform-image evidence.')
        for role, architecture in (('application', contract['inputs']['architecture']), ('support', 'x86_64')):
            image = images[role]
            name = image.get('display_name', '')
            if (image.get('id') != contract['inputs'][role + '_image_id'] or image.get('compartment_id') is not None
                    or image.get('operating_system') != 'Oracle Linux' or not re.fullmatch(r'9(?:\.\d+)*', str(image.get('operating_system_version', '')))
                    or not name.startswith('Oracle-Linux-9') or ('aarch64' in name.lower()) != (architecture == 'arm64')
                    or image.get('lifecycle_state') != 'AVAILABLE'):
                raise LifecycleError('OCI candidate pinned platform-image evidence conflicts with its inputs.')
        if require_ready and (contract['status'] != 'ready' or any(
                entry['status'] != 'ready' or not entry.get('id') for entry in contract['entries'].values())):
            raise LifecycleError('OCI runtime requires the complete ready candidate graph.')
        _require_saved_implicit_boundaries(contract, require_complete=require_ready)
        _check_runtime_aliases(job['resources'], contract)
        if clients is not None:
            for name in ('compute', 'network', 'block'):
                if getattr(clients[name], '_config', {}).get('region') != contract['inputs']['region']:
                    raise LifecycleError('OCI validation client region conflicts with the candidate contract.')
            _verify_graph(contract, clients)
            if require_ready:
                for entry in contract['entries'].values():
                    expected_state = {'instance': 'RUNNING', 'volume_attachment': 'ATTACHED'}.get(entry['spec']['kind'], 'AVAILABLE')
                    observed = _verify(entry, contract, clients)
                    if observed is None or observed[0].get('lifecycle_state') != expected_state:
                        raise LifecycleError('OCI runtime requires live ready resources, not just saved status.')
        return contract
    except (KeyError, TypeError, AttributeError, ValueError) as exc:
        raise LifecycleError(f'Malformed OCI candidate validation contract: {exc}') from exc


def distributed_runtime_projection(job, *, clients=None):
    """Derive exact SSH/topology/storage aliases without editing persisted state."""
    contract = validate_distributed_deathstarbench_candidate(job, clients=clients, require_ready=True)
    return _runtime_projection(contract)


def distributed_deathstarbench_candidate_projection(resources):
    """Strict local runtime plan projection after live-validated publication.

    No SDK operation occurs here. Initial/resumed operators must publish through
    ``publish_distributed_runtime_projection`` under the run lease first.
    """
    if not isinstance(resources, dict) or not isinstance(resources.get(CONTRACT_KEY), dict):
        raise LifecycleError('OCI runtime projection requires exact candidate resources.')
    job = {'id': resources[CONTRACT_KEY].get('job_id'), 'resources': resources}
    contract = validate_distributed_deathstarbench_candidate(job, require_ready=True)
    if not RUNTIME_ALIAS_KEYS <= set(resources):
        raise LifecycleError('OCI runtime aliases have not been completely cloud-validated and published.')
    aliases = _runtime_projection(contract)
    inventory = _inventory(contract)
    nodes = {}
    for node in inventory.nodes:
        prefix = f'oci_dsb_{node.key.replace("-", "_")}'
        direct = node.key in {'control', 'load-generator'}
        nodes[node.key] = {
            'key': node.key, 'role': node.role, 'node_name': node.provider_resource_name,
            'instance_id': node.provider_resource_id, 'shape': node.shape, 'architecture': node.architecture,
            'private_ip': node.private_addresses[0],
            'public_ip': node.public_addresses[0] if node.public_addresses else None,
            'host_key': prefix + ('_public_ip' if direct else '_private_ip'),
            'jump_host_key': None if direct else 'oci_dsb_control_public_ip',
        }
    public = [node['public_ip'] for node in nodes.values() if node['public_ip']]
    if len(set(public)) != len(public):
        raise LifecycleError('OCI runtime roles have duplicate public addresses.')
    return {
        'provider': 'oci', 'job_id': contract['job_id'], 'compartment_id': contract['inputs']['compartment_id'],
        'region': contract['inputs']['region'], 'zone': contract['inputs']['availability_domain'], 'ssh_user': SSH_USER,
        'topology_fingerprint': inventory.topology_fingerprint,
        'database_volume_id': aliases['oci_dsb_database_volume_id'],
        'database_attachment_id': aliases['oci_dsb_database_attachment_id'],
        'database_device': DATABASE_DEVICE, 'database_mount_point': DATABASE_MOUNT_POINT,
        'aliases': aliases, 'nodes': nodes,
        'support_image': {**copy.deepcopy(contract['images']['support']), 'architecture': 'x86_64'},
        'application_image': {**copy.deepcopy(contract['images']['application']), 'architecture': contract['inputs']['architecture']},
    }


def publish_distributed_runtime_projection(job, *, clients, persist):
    """Live-validate and durably publish derived aliases; never overwrite drift."""
    if clients is None or not callable(persist):
        raise LifecycleError('Live OCI clients and durable persistence are required to publish runtime aliases.')
    aliases = distributed_runtime_projection(job, clients=clients)
    job['resources'].update(copy.deepcopy(aliases))
    persist(job)
    return aliases


def _valid_timestamp(value):
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _ordered_timestamps(*values):
    if not all(_valid_timestamp(value) for value in values):
        return False
    parsed = [datetime.fromisoformat(value) for value in values]
    return parsed == sorted(parsed)


def distributed_candidate_is_deleted(job):
    """Fail-closed local cleanup evidence predicate, not a new cloud absence probe.

    Retained IDs/addresses are tombstones only when every accepted create has
    timestamped, confirmed deletion (including boot disks). Never classify an
    arbitrary deleted status, malformed inventory, or foreign resource key as
    safe. Fresh independent cloud absence checks remain an operator task.
    """
    try:
        contract = validate_distributed_deathstarbench_candidate(job)
        if contract['status'] != 'deleted' or not _valid_timestamp(contract.get('deletion_confirmed_at')):
            return False
        allowed = {CONTRACT_KEY, ROLE_NODE_INVENTORY_KEY} | RUNTIME_ALIAS_KEYS | _NON_CLOUD_EVIDENCE_KEYS
        if set(job['resources']) - allowed:
            return False
        for entry in contract['entries'].values():
            if entry['status'] == 'planned':
                if entry.get('id') or entry.get('implicit'):
                    return False
                if entry.get('attempted_at'):
                    rejection = entry.get('last_create_rejection') or {}
                    exc = sdk.exceptions.ServiceError(rejection.get('status'), rejection.get('code'), {}, '')
                    if (not _confirmed_create_rejection(exc) or rejection.get('attempted_at') != entry['attempted_at']
                            or not _valid_timestamp(rejection.get('recorded_at'))):
                        return False
            elif (entry['status'] != 'deleted' or not entry.get('id')
                    or not _ordered_timestamps(entry.get('attempted_at'), entry.get('delete_attempted_at'),
                                               entry.get('deletion_confirmed_at'), contract['deletion_confirmed_at'])):
                return False
            elif entry['spec']['kind'] == 'instance':
                boot = entry.get('implicit', {}).get('boot_volume', {})
                if not boot.get('id') or not _ordered_timestamps(entry['delete_attempted_at'],
                        boot.get('deletion_confirmed_at'), entry['deletion_confirmed_at']):
                    return False
            elif entry['spec']['kind'] == 'vcn' and not entry.get('implicit'):
                return False
        return True
    except (LifecycleError, KeyError, TypeError, AttributeError, ValueError):
        return False


def _lookup_kwargs(kind, contract, entries):
    values = {'compartment_id': contract['inputs']['compartment_id']}
    if kind in {'volume_attachment', 'volume'}:
        values['availability_domain'] = contract['inputs']['availability_domain']
    if kind not in {'vcn', 'instance', 'volume', 'volume_attachment'}:
        values['vcn_id'] = entries['vcn'].get('id')
    return values


def _plural(kind):
    return {'vnic': 'vnics', 'dhcp_options': 'dhcp_options'}.get(kind, kind + 's')


def _recover(entry, contract, clients):
    spec = entry['spec']
    kwargs = _lookup_kwargs(spec['kind'], contract, contract['entries'])
    if 'vcn_id' in kwargs and not kwargs['vcn_id']:
        raise LifecycleError('Cannot reconcile a child without its VCN identity.')
    matches = [item for item in _list(clients[spec['client']], 'list_' + _plural(spec['kind']), **kwargs)
               if item.get('display_name') == spec['body']['display_name'] and item.get('lifecycle_state') not in {'TERMINATED', 'DETACHED'}]
    if len(matches) != 1:
        raise LifecycleError('Create outcome remains ambiguous; retain state and reconcile later, never replay.')
    try:
        created = datetime.fromisoformat(str(matches[0]['time_created']).replace('Z', '+00:00'))
        attempted = datetime.fromisoformat(entry['attempted_at'])
        if not -300 <= (created - attempted).total_seconds() <= 86400:
            raise ValueError('Create timestamp outside retry-token window')
    except (KeyError, ValueError, TypeError) as exc:
        raise LifecycleError('Recovered resource creation time does not match the saved intent.') from exc
    return matches[0]['id']


def _verify(entry, contract, clients):
    spec = entry['spec']
    observed = _get(clients[spec['client']], 'get_' + spec['kind'], entry['id'])
    if observed is None:
        # OCI deliberately uses NotAuthorizedOrNotFound for both cases. A
        # successful scoped list must independently corroborate disappearance.
        listed = _list(clients[spec['client']], 'list_' + _plural(spec['kind']),
                       **_lookup_kwargs(spec['kind'], contract, contract['entries']))
        if any(item['id'] == entry['id'] and item.get('lifecycle_state') not in {'TERMINATED', 'DETACHED'} for item in listed):
            raise LifecycleError('OCI GET returned 404 but scoped listing still contains the resource.')
        return None
    data, etag = observed
    expected = _resolve(spec['body'], contract['entries'])
    if spec['kind'] == 'instance':
        expected = {key: value for key, value in expected.items() if key not in {'create_vnic_details', 'source_details'}}
        expected['image_id'] = spec['body']['source_details']['image_id']
    elif spec['kind'] == 'volume_attachment':
        expected['attachment_type'] = expected.pop('type')
    _assert_subset(expected, data, spec['kind'])
    if data.get('id') != entry['id'] or not etag:
        raise LifecycleError('Resource identity or ETag missing.')
    return data, etag


def _wait(entry, contract, clients, *, deleted=False, timeout=WAIT_SECONDS):
    end = time.monotonic() + timeout
    while True:
        observed = _verify(entry, contract, clients)
        state = observed[0].get('lifecycle_state') if observed else None
        if deleted and (observed is None or state in {'TERMINATED', 'DETACHED'}):
            return None
        if not deleted and state in {'AVAILABLE', 'RUNNING', 'ATTACHED'}:
            return observed
        if state in {'FAULTY', 'FAILED', 'TERMINATED'} or time.monotonic() >= end:
            raise LifecycleError(f'OCI resource waiter stopped in {state}.')
        time.sleep(2)


def _implicit(entry, contract, clients):
    """Discover implicit children by exact immutable parent, not name guessing."""
    spec = entry['spec']
    compartment = contract['inputs']['compartment_id']
    availability_domain = contract['inputs']['availability_domain']
    found = {}
    if spec['kind'] == 'vcn':
        data = _verify(entry, contract, clients)[0]
        for field, kind in (('default_route_table_id', 'route_table'), ('default_security_list_id', 'security_list'), ('default_dhcp_options_id', 'dhcp_options')):
            child_id = data.get(field)
            if not child_id:
                raise LifecycleError('VCN default child identity is missing.')
            child, etag = _get(clients['network'], 'get_' + kind, child_id) or ({}, None)
            _assert_subset({'id': child_id, 'vcn_id': entry['id'], 'compartment_id': compartment}, child, 'VCN implicit child')
            if not etag:
                raise LifecycleError('VCN implicit child ETag is invalid.')
            found[kind] = {'id': child_id, 'kind': kind, 'client': 'network', 'snapshot': child, 'etag': etag}
    elif spec['kind'] == 'instance':
        args = {'compartment_id': contract['inputs']['compartment_id'], 'instance_id': entry['id']}
        vnics = [item for item in _list(clients['compute'], 'list_vnic_attachments', **args) if item.get('lifecycle_state') != 'DETACHED']
        boots = [item for item in _list(clients['compute'], 'list_boot_volume_attachments', availability_domain=contract['inputs']['availability_domain'], **args) if item.get('lifecycle_state') != 'DETACHED']
        if len(vnics) != 1 or len(boots) != 1:
            raise LifecycleError('Instance must have exactly one VNIC and boot attachment.')
        for attachment, kind, target_field in ((vnics[0], 'vnic_attachment', 'vnic_id'),
                                                (boots[0], 'boot_volume_attachment', 'boot_volume_id')):
            attachment_id = attachment.get('id')
            target_id = attachment.get(target_field)
            if not isinstance(attachment_id, str) or not attachment_id or not isinstance(target_id, str) or not target_id:
                raise LifecycleError('Implicit attachment identity is missing.')
            expected_attachment = {'id': attachment_id, 'instance_id': entry['id'],
                'compartment_id': compartment, 'availability_domain': availability_domain,
                target_field: target_id, 'lifecycle_state': 'ATTACHED'}
            _assert_subset(expected_attachment, attachment, 'Implicit attachment')
            actual_attachment, attachment_etag = _get(clients['compute'], 'get_' + kind, attachment_id) or ({}, None)
            _assert_subset(expected_attachment, actual_attachment, 'Implicit attachment readback')
            if not attachment_etag:
                raise LifecycleError('Implicit attachment readback ETag is missing.')
        vnic, etag = _get(clients['network'], 'get_vnic', vnics[0]['vnic_id']) or ({}, None)
        expected = _resolve(spec['body']['create_vnic_details'], contract['entries'])
        public = expected.pop('assign_public_ip')
        expected.update(id=vnics[0]['vnic_id'], compartment_id=compartment, availability_domain=availability_domain)
        _assert_subset(expected, vnic, 'primary-vnic')
        if vnics[0].get('subnet_id') != expected['subnet_id']:
            raise LifecycleError('Implicit VNIC attachment subnet differs from the launch contract.')
        if bool(vnic.get('public_ip')) != public or not etag or vnic.get('is_primary') is not True:
            raise LifecycleError('Unexpected primary VNIC/public addressing.')
        found['vnic'] = {'id': vnic['id'], 'kind': 'vnic', 'client': 'network', 'snapshot': vnic, 'etag': etag}
        boot, etag = _get(clients['block'], 'get_boot_volume', boots[0]['boot_volume_id']) or ({}, None)
        _assert_subset({'id': boots[0]['boot_volume_id'], 'compartment_id': compartment,
                        'availability_domain': availability_domain}, boot, 'boot-volume')
        if not etag or boot.get('image_id') != spec['body']['source_details']['image_id'] or boot.get('size_in_gbs') != 50:
            raise LifecycleError('Unexpected boot volume image/size/ETag.')
        found['boot_volume'] = {'id': boot['id'], 'kind': 'boot_volume', 'client': 'block', 'snapshot': boot, 'etag': etag}
        found['attachments'] = {'vnic': vnics[0], 'boot': boots[0]}
    previous = entry.get('implicit')
    if previous:
        for key in found:
            if key == 'attachments':
                if (set(previous[key]) != set(found[key]) or any(
                        _stable_implicit(previous[key][name])
                        != _stable_implicit(found[key][name])
                        for name in found[key])):
                    raise LifecycleError('Implicit attachment identity changed.')
            elif previous[key]['id'] != found[key]['id'] or _stable_implicit(previous[key]['snapshot']) != _stable_implicit(found[key]['snapshot']):
                raise LifecycleError('Implicit resource changed after inventory capture.')
    entry['implicit'] = found


def _stable_implicit(snapshot):
    # Hydration, lifecycle state, and provider-maintained update timestamps can
    # change during normal VM start/stop and attachment transitions. They do
    # not change ownership, attachment IDs, parent identities, or disk data.
    return {
        key: value
        for key, value in snapshot.items()
        if key not in {'lifecycle_state', 'is_hydrated', 'time_updated'}
    }


def _terminal_states(kind):
    if kind in {
        'boot_volume_attachment',
        'vnic_attachment',
        'volume_attachment',
    }:
        return {'DETACHED'}
    return {'TERMINATED'}


def _assert_terminal_identity(*, identity, kind, expected, observed, label):
    if observed.get('id') != identity:
        raise LifecycleError(f'OCI terminal audit read the wrong {label} identity.')
    _assert_subset(expected, observed, f'terminal {label}')
    if observed.get('lifecycle_state') not in _terminal_states(kind):
        raise LifecycleError(f'OCI terminal audit found {label} still live.')


def _attest_identity(
    *, client, get_method, list_method, identity, kind, expected,
    list_kwargs, label,
):
    """Corroborate one retained identity with independent GET and list reads."""

    observed = _get(client, get_method, identity)
    if observed is not None:
        _assert_terminal_identity(
            identity=identity,
            kind=kind,
            expected=expected,
            observed=observed[0],
            label=label,
        )
    listed = _list(client, list_method, **list_kwargs)
    for item in listed:
        if item.get('id') != identity:
            continue
        _assert_terminal_identity(
            identity=identity,
            kind=kind,
            expected=expected,
            observed=item,
            label=label,
        )


def _terminal_explicit_expected(entry, contract):
    spec = entry['spec']
    expected = _resolve(spec['body'], contract['entries'])
    if spec['kind'] == 'instance':
        expected = {
            key: value
            for key, value in expected.items()
            if key not in {'create_vnic_details', 'source_details'}
        }
        expected['image_id'] = spec['body']['source_details']['image_id']
    elif spec['kind'] == 'volume_attachment':
        expected['attachment_type'] = expected.pop('type')
    return expected


def _terminal_implicit_expected(kind, snapshot):
    """Project an implicit resource down to deletion-stable ownership fields.

    OCI advances lifecycle metadata such as ``time_updated`` while detaching a
    boot attachment. Other operational fields can disappear once a VNIC or
    boot volume becomes terminal. The retained OCID, compartment, parent
    identities, and creation-time ownership/configuration fields remain the
    fail-closed boundary for terminal readback.
    """

    required = {
        'route_table': {'id', 'compartment_id', 'vcn_id'},
        'security_list': {'id', 'compartment_id', 'vcn_id'},
        'dhcp_options': {'id', 'compartment_id', 'vcn_id'},
        'vnic': {
            'id', 'compartment_id', 'availability_domain', 'subnet_id',
            'is_primary',
        },
        'boot_volume': {
            'id', 'compartment_id', 'availability_domain', 'image_id',
            'size_in_gbs',
        },
        'vnic_attachment': {
            'id', 'compartment_id', 'availability_domain', 'instance_id',
            'vnic_id', 'subnet_id',
        },
        'boot_volume_attachment': {
            'id', 'compartment_id', 'availability_domain', 'instance_id',
            'boot_volume_id',
        },
    }.get(kind)
    if required is None or not required <= set(snapshot):
        raise LifecycleError(
            f'OCI terminal audit lacks immutable {kind} ownership evidence.'
        )
    expected = {key: copy.deepcopy(snapshot[key]) for key in required}
    # Tags and names are provider ownership evidence for implicitly-created
    # resources when OCI returns them. They are deliberately not required for
    # attachment/default-child types that do not support these attributes.
    for key in ('display_name', 'freeform_tags', 'defined_tags'):
        if key in snapshot:
            expected[key] = copy.deepcopy(snapshot[key])
    return expected


def attest_distributed_candidate_terminal_deletion(job, *, clients):
    """Freshly prove that every accepted OCI resource is absent or terminal.

    Local deletion timestamps remain necessary but are not cloud evidence.
    This independent post-cleanup gate reads every retained explicit identity,
    corroborates it with a fresh compartment-scoped list, and
    also checks the implicit instance/VCN children captured before mutation.
    """

    if not distributed_candidate_is_deleted(job):
        raise LifecycleError(
            'OCI terminal audit requires complete local deletion evidence.'
        )
    contract = validate_distributed_deathstarbench_candidate(job)
    for name in ('compute', 'network', 'block'):
        client = clients.get(name) if isinstance(clients, dict) else None
        if client is None or getattr(client, '_config', {}).get('region') != (
            contract['inputs']['region']
        ):
            raise LifecycleError(
                'OCI terminal audit client region differs from the saved contract.'
            )

    entries = contract['entries']
    compartment = contract['inputs']['compartment_id']
    availability_domain = contract['inputs']['availability_domain']
    for key, entry in entries.items():
        if entry['status'] == 'planned':
            continue
        spec = entry['spec']
        list_kwargs = _lookup_kwargs(spec['kind'], contract, entries)
        # Deleted VCN IDs are not a reliable list scope. A fresh compartment
        # list is broader and can still prove whether the exact retained OCID
        # is absent or terminal without depending on a deleted parent.
        list_kwargs.pop('vcn_id', None)
        _attest_identity(
            client=clients[spec['client']],
            get_method='get_' + spec['kind'],
            list_method='list_' + _plural(spec['kind']),
            identity=entry['id'],
            kind=spec['kind'],
            expected=_terminal_explicit_expected(entry, contract),
            list_kwargs=list_kwargs,
            label=f'explicit {key}',
        )

    vcn = entries['vcn']
    for child_kind, child in vcn.get('implicit', {}).items():
        _attest_identity(
            client=clients[child['client']],
            get_method='get_' + child['kind'],
            list_method='list_' + _plural(child['kind']),
            identity=child['id'],
            kind=child['kind'],
            expected=_terminal_implicit_expected(
                child['kind'],
                child['snapshot'],
            ),
            list_kwargs={'compartment_id': compartment},
            label=f'implicit VCN {child_kind}',
        )

    for role in PRIVATE_ADDRESSES:
        entry = entries[role]
        if entry['status'] == 'planned':
            continue
        implicit = entry.get('implicit', {})
        vnic = implicit.get('vnic') or {}
        boot = implicit.get('boot_volume') or {}
        if not vnic.get('id') or not boot.get('id'):
            raise LifecycleError(
                'OCI terminal audit lacks captured instance child identities.'
            )
        # VirtualNetworkClient has no list_vnics operation. The primary VNIC's
        # captured attachment provides the independent compartment-list proof.
        observed_vnic = _get(clients['network'], 'get_vnic', vnic['id'])
        if observed_vnic is not None:
            _assert_terminal_identity(
                identity=vnic['id'],
                kind='vnic',
                expected=_terminal_implicit_expected('vnic', vnic['snapshot']),
                observed=observed_vnic[0],
                label=f'{role} VNIC',
            )
        _attest_identity(
            client=clients['block'],
            get_method='get_boot_volume',
            list_method='list_boot_volumes',
            identity=boot['id'],
            kind='boot_volume',
            expected=_terminal_implicit_expected(
                'boot_volume',
                boot['snapshot'],
            ),
            list_kwargs={
                'availability_domain': availability_domain,
                'compartment_id': compartment,
            },
            label=f'{role} boot volume',
        )
        for attachment_key, kind in (
            ('vnic', 'vnic_attachment'),
            ('boot', 'boot_volume_attachment'),
        ):
            attachment = implicit.get('attachments', {}).get(attachment_key)
            if not isinstance(attachment, dict) or not attachment.get('id'):
                raise LifecycleError(
                    'OCI terminal audit lacks captured instance attachment identities.'
                )
            list_kwargs = {'compartment_id': compartment}
            if kind == 'boot_volume_attachment':
                list_kwargs['availability_domain'] = availability_domain
            _attest_identity(
                client=clients['compute'],
                get_method='get_' + kind,
                list_method='list_' + _plural(kind),
                identity=attachment['id'],
                kind=kind,
                expected=_terminal_implicit_expected(kind, attachment),
                list_kwargs=list_kwargs,
                label=f'{role} {attachment_key} attachment',
            )
    return True


def provision_distributed_deathstarbench_candidate(job, *, inputs, clients, persist, emit=None):
    """Create only candidate infrastructure. Requires an exclusive caller lease.

    ``inputs`` contains explicit compartment/AD/region, shape/capacity/arch,
    application/support platform image OCIDs, and an SSH *public* key.
    ``clients`` supplies authenticated ``compute``, ``network``, ``block`` SDK
    clients already configured for the requested region. No benchmark is run.
    """
    if not callable(persist) or not re.fullmatch(r'[0-9a-f]{12}', str(job.get('id', ''))):
        raise LifecycleError('Durable persistence and a canonical job ID are required.')
    _validate_inputs(inputs)
    for client in clients.values():
        region = getattr(client, '_config', {}).get('region')
        if region != inputs['region']:
            raise LifecycleError('OCI SDK client region does not match the pinned region.')
    resources = job.setdefault('resources', {})
    if CONTRACT_KEY not in resources:
        if resources:
            raise LifecycleError('Cannot mix candidate infrastructure with another resource inventory.')
        images = _preflight(inputs, clients)
        resources[CONTRACT_KEY] = {'schema_version': SCHEMA_VERSION, 'job_id': job['id'], 'inputs': copy.deepcopy(inputs),
            'manifest': _manifest(inputs).as_dict(), 'images': images, 'created_at': _now(), 'status': 'planned',
            'entries': {key: {'spec': spec, 'retry_token': str(uuid.uuid4()), 'status': 'planned'} for key, spec in _specs(job['id'], inputs).items()}}
        _save(job, persist)
    contract = _load(job)
    if inputs != contract['inputs'] or contract['status'] in {'deleting', 'deleted'}:
        raise LifecycleError('Cannot change or recreate an existing candidate contract.')
    for key, entry in contract['entries'].items():
        spec = entry['spec']
        if not entry.get('id'):
            if entry['status'] == 'planned':
                body = _resolve(spec['body'], contract['entries'])
                request_model = _model(spec['model'], body)
                entry.update(status='creating', attempted_at=_now())
                _save(job, persist)
                method = 'launch_instance' if spec['kind'] == 'instance' else 'attach_volume' if spec['kind'] == 'volume_attachment' else 'create_' + spec['kind']
                try:
                    response = getattr(clients[spec['client']], method)(request_model,
                        opc_retry_token=entry['retry_token'], retry_strategy=sdk.retry.NoneRetryStrategy())
                except sdk.exceptions.ServiceError as exc:
                    if _confirmed_create_rejection(exc):
                        entry['status'] = 'planned'
                        entry['last_create_rejection'] = {'status': exc.status, 'code': exc.code,
                            'attempted_at': entry['attempted_at'], 'recorded_at': _now()}
                        _save(job, persist)
                    raise
                entry['id'] = _dict(response.data)['id']
                _save(job, persist)
            else:
                entry['id'] = _recover(entry, contract, clients)
                _save(job, persist)
        _wait(entry, contract, clients)
        _implicit(entry, contract, clients)
        entry['status'] = 'ready'
        _save(job, persist)
        if spec['kind'] == 'network_security_group':
            expected = _rules(key[4:], contract['manifest'])
            if not _check_rules(clients['network'], entry['id'], expected, allow_empty=True):
                if entry.get('rules_attempted_at'):
                    raise LifecycleError('NSG rule mutation remains ambiguous; never replay blindly.')
                entry['rules_attempted_at'] = _now()
                _save(job, persist)
                clients['network'].add_network_security_group_security_rules(entry['id'],
                    _model('AddNetworkSecurityGroupSecurityRulesDetails', {'security_rules': expected}),
                    retry_strategy=sdk.retry.NoneRetryStrategy())
                _check_rules(clients['network'], entry['id'], expected)
            entry['rules_ready'] = True
            _save(job, persist)
        if emit:
            emit(job, 'Provision', f'OCI distributed candidate {key} ready.')
    contract['status'] = 'ready'
    _save(job, persist)
    return _inventory(contract)


def _owned_delete_intent(entry):
    if entry.get('status') != 'deleting':
        return False
    try:
        attempted = datetime.fromisoformat(entry['delete_attempted_at'])
    except (KeyError, TypeError, ValueError):
        return False
    return attempted.tzinfo is not None and attempted.utcoffset() is not None


def _verify_graph(contract, clients):
    """Read-only full graph gate, run before every destructive operation."""
    entries = contract['entries']
    for entry in entries.values():
        if not entry.get('id') or entry['status'] == 'deleted':
            continue
        observed = _verify(entry, contract, clients)
        if observed is None:
            if not _owned_delete_intent(entry):
                raise LifecycleError('Owned resource disappeared without a recorded delete intent.')
            continue
        cloud_state = observed[0].get('lifecycle_state')
        if cloud_state in {'TERMINATING', 'TERMINATED', 'DETACHING', 'DETACHED'} and not _owned_delete_intent(entry):
            # Gate the whole graph before any sibling resource can be changed.
            # In particular, do not detach the DB disk and only then discover
            # an instance was terminated outside this lifecycle controller.
            raise LifecycleError('Cloud deletion started without an owned delete intent.')
        if entry['spec']['kind'] == 'network_security_group':
            _check_rules(clients['network'], entry['id'], _rules(entry['spec']['body']['freeform_tags']['benchmark-role'][4:], contract['manifest']),
                         # An exact empty set is safe to delete when the rule
                         # mutation was never confirmed. Exact intended rules
                         # are also accepted; partial/extra rules still fail.
                         allow_empty=not entry.get('rules_ready'))
        # A saved delete intent is not proof that the API accepted termination.
        # A RUNNING/STOPPED instance still needs its complete attachment graph
        # checked before a retry can terminate it (including foreign VNICs).
        deleting_in_cloud = cloud_state in {'TERMINATING', 'TERMINATED'}
        if not deleting_in_cloud:
            _implicit(entry, contract, clients)
    vcn = entries['vcn'].get('id')
    if not vcn or entries['vcn']['status'] == 'deleted':
        return
    if _verify(entries['vcn'], contract, clients) is None:
        if any(entry['status'] not in {'planned', 'deleted'} for key, entry in entries.items() if key != 'vcn'):
            raise LifecycleError('VCN disappeared while child resource intents remain active.')
        return
    defaults = entries['vcn'].get('implicit', {})
    for kind in ('subnet', 'route_table', 'security_list', 'dhcp_options', 'network_security_group', 'internet_gateway', 'nat_gateway', 'local_peering_gateway', 'service_gateway', 'vlan', 'drg_attachment', 'vtap'):
        allowed = {entry['id'] for entry in entries.values() if entry.get('id') and entry['spec']['kind'] == kind}
        allowed.update(item['id'] for item in defaults.values() if item['kind'] == kind)
        actual = _list(clients['network'], 'list_' + _plural(kind), compartment_id=contract['inputs']['compartment_id'], vcn_id=vcn)
        if any(item['id'] not in allowed for item in actual if item.get('lifecycle_state') != 'TERMINATED'):
            raise LifecycleError(f'Foreign {kind} in the candidate VCN; refusing cleanup.')
    for key in ('database-data',):
        volume = entries[key].get('id')
        if volume:
            attachments = _list(clients['compute'], 'list_volume_attachments', compartment_id=contract['inputs']['compartment_id'], volume_id=volume)
            allowed = entries['database-attachment'].get('id')
            if any(item['id'] != allowed for item in attachments if item.get('lifecycle_state') != 'DETACHED'):
                raise LifecycleError('Foreign database volume attachment; refusing cleanup.')
    for role in PRIVATE_ADDRESSES:
        entry = entries[role]
        if not entry.get('id') or entry['status'] == 'deleted':
            continue
        args = {'compartment_id': contract['inputs']['compartment_id'], 'instance_id': entry['id']}
        attachments = _list(clients['compute'], 'list_volume_attachments', **args)
        allowed = entries['database-attachment'].get('id') if role == 'database' else None
        if any(item['id'] != allowed for item in attachments if item.get('lifecycle_state') != 'DETACHED'):
            raise LifecycleError('Instance has a foreign data-volume attachment.')
        boot = entry.get('implicit', {}).get('boot_volume')
        if boot:
            attachments = _list(clients['compute'], 'list_boot_volume_attachments',
                compartment_id=contract['inputs']['compartment_id'], availability_domain=contract['inputs']['availability_domain'], boot_volume_id=boot['id'])
            if any(item.get('instance_id') != entry['id'] for item in attachments if item.get('lifecycle_state') != 'DETACHED'):
                raise LifecycleError('Boot volume has a foreign attachment.')
    for role in PRIVATE_ADDRESSES:
        nsg = entries[f'nsg-{role}']
        if not nsg.get('id') or nsg['status'] == 'deleted':
            continue
        allowed = entries[role].get('implicit', {}).get('vnic', {}).get('id')
        members = _list(clients['network'], 'list_network_security_group_vnics', network_security_group_id=nsg['id'])
        if any(item['vnic_id'] != allowed for item in members):
            raise LifecycleError('NSG contains a foreign VNIC.')
    for subnet in ('management', 'data', 'loadgen'):
        entry = entries[subnet]
        if not entry.get('id') or entry['status'] == 'deleted':
            continue
        allowed = {item.get('implicit', {}).get('vnic', {}).get('id') for key, item in entries.items() if key in PRIVATE_ADDRESSES}
        addresses = _list(clients['network'], 'list_private_ips', subnet_id=entry['id'])
        if any(item.get('vnic_id') not in allowed or item.get('is_primary') is not True for item in addresses):
            raise LifecycleError('Subnet contains an untracked private IP or VNIC.')


def destroy_distributed_deathstarbench_candidate(job, *, clients, persist, emit=None):
    """Delete only the fully verified candidate graph, retaining recovery state."""
    if not callable(persist):
        raise LifecycleError('Durable persistence is required for cleanup.')
    contract = _load(job)
    for client in clients.values():
        if getattr(client, '_config', {}).get('region') != contract['inputs']['region']:
            raise LifecycleError('OCI cleanup client region differs from the saved contract.')
    if contract['status'] == 'deleted':
        if not distributed_candidate_is_deleted(job):
            raise LifecycleError('OCI deleted marker lacks complete, consistent cleanup evidence.')
        return
    for entry in contract['entries'].values():
        if not entry.get('id') and entry['status'] != 'planned':
            entry['id'] = _recover(entry, contract, clients)
            _save(job, persist)
    _verify_graph(contract, clients)
    contract['status'] = 'deleting'
    _save(job, persist)
    # Reverse insertion order gives attachment -> nodes -> data disk -> NSGs
    # -> subnets -> security/route tables -> gateways -> VCN/default children.
    for key, entry in reversed(list(contract['entries'].items())):
        if entry['status'] in {'planned', 'deleted'}:
            continue
        _verify_graph(contract, clients)
        spec = entry['spec']
        observed = _verify(entry, contract, clients)
        if observed is not None and observed[0].get('lifecycle_state') not in {'TERMINATED', 'DETACHED'}:
            in_progress = observed[0].get('lifecycle_state') in {'TERMINATING', 'DETACHING'}
            if in_progress and entry['status'] != 'deleting':
                raise LifecycleError('Cloud deletion started without an owned delete intent.')
            entry['status'] = 'deleting'
            entry['delete_attempted_at'] = _now()
            _save(job, persist)
            method = 'terminate_instance' if spec['kind'] == 'instance' else 'detach_volume' if spec['kind'] == 'volume_attachment' else 'delete_' + spec['kind']
            kwargs = {
                'if_match': observed[1],
                'retry_strategy': sdk.retry.NoneRetryStrategy(),
            }
            if spec['kind'] == 'instance':
                kwargs['preserve_boot_volume'] = True
            if not in_progress:
                for attempt in range(ETAG_DELETE_ATTEMPTS):
                    try:
                        getattr(clients[spec['client']], method)(entry['id'], **kwargs)
                        break
                    except sdk.exceptions.ServiceError as exc:
                        if exc.status != 412 or exc.code != 'NoEtagMatch':
                            raise
                        if attempt + 1 == ETAG_DELETE_ATTEMPTS:
                            if key != 'igw' or spec['kind'] != 'internet_gateway':
                                raise LifecycleError(
                                    'OCI ETag fallback is permitted only for '
                                    'the qualified internet gateway.'
                                ) from exc
                            # OCI can return changing ETags from GET while its
                            # mutation endpoint repeatedly computes another
                            # value (observed with tag defaults on the IGW).
                            # Only bypass the optional optimistic header for
                            # that exact resource after two identical,
                            # full-graph-validated payload snapshots.
                            _verify_graph(contract, clients)
                            first = _verify(entry, contract, clients)
                            time.sleep(2)
                            _verify_graph(contract, clients)
                            second = _verify(entry, contract, clients)
                            if (first is None or second is None or not first[1]
                                    or not second[1]
                                    or first[0] != second[0]
                                    or first[0].get('lifecycle_state') in {
                                        'TERMINATING', 'TERMINATED',
                                        'DETACHING', 'DETACHED'}):
                                raise LifecycleError(
                                    'OCI ETag fallback could not prove two '
                                    'identical exact resource snapshots.'
                                ) from exc
                            entry['etag_fallback_attempted_at'] = _now()
                            entry['etag_fallback_observed'] = first[1]
                            entry['etag_fallback_confirmed'] = second[1]
                            _save(job, persist)
                            fallback = {key: value for key, value in kwargs.items()
                                        if key != 'if_match'}
                            getattr(clients[spec['client']], method)(entry['id'], **fallback)
                            break
                        # Deleting a route/subnet can legitimately advance a
                        # parent gateway/VCN ETag. Re-audit the entire remaining
                        # graph and exact target before retrying; configuration
                        # or ownership drift still fails closed in verification.
                        _verify_graph(contract, clients)
                        refreshed = _verify(entry, contract, clients)
                        if refreshed is None:
                            break
                        if refreshed[0].get('lifecycle_state') in {
                                'TERMINATING', 'TERMINATED', 'DETACHING', 'DETACHED'}:
                            break
                        kwargs['if_match'] = refreshed[1]
            _wait(entry, contract, clients, deleted=True)
        if spec['kind'] == 'instance':
            boot = entry.get('implicit', {}).get('boot_volume')
            if not boot:
                raise LifecycleError('Cannot clean instance without its boot volume identity.')
            observed_boot = _get(clients['block'], 'get_boot_volume', boot['id'])
            if observed_boot and observed_boot[0].get('lifecycle_state') != 'TERMINATED':
                data, etag = observed_boot
                _assert_subset(_stable_implicit(boot['snapshot']), data, 'boot-volume')
                attachments = _list(clients['compute'], 'list_boot_volume_attachments', availability_domain=contract['inputs']['availability_domain'], compartment_id=contract['inputs']['compartment_id'], boot_volume_id=boot['id'])
                if any(item.get('lifecycle_state') != 'DETACHED' for item in attachments):
                    raise LifecycleError('Boot volume is still attached; retain cleanup state.')
                if not etag:
                    raise LifecycleError('Missing boot volume ETag.')
                in_progress = data.get('lifecycle_state') == 'TERMINATING'
                if in_progress and not boot.get('delete_attempted_at'):
                    raise LifecycleError('Boot volume deletion has no saved intent.')
                if not in_progress:
                    boot['delete_attempted_at'] = _now()
                    _save(job, persist)
                    clients['block'].delete_boot_volume(
                        boot['id'],
                        if_match=etag,
                        retry_strategy=sdk.retry.NoneRetryStrategy(),
                    )
                end = time.monotonic() + WAIT_SECONDS
                while True:
                    remaining = _get(clients['block'], 'get_boot_volume', boot['id'])
                    if remaining is None or remaining[0].get('lifecycle_state') == 'TERMINATED':
                        break
                    if time.monotonic() >= end:
                        raise LifecycleError('Boot volume deletion not confirmed.')
                    time.sleep(2)
            remaining_boots = _list(clients['block'], 'list_boot_volumes',
                availability_domain=contract['inputs']['availability_domain'], compartment_id=contract['inputs']['compartment_id'])
            if any(item['id'] == boot['id'] and item.get('lifecycle_state') != 'TERMINATED' for item in remaining_boots):
                raise LifecycleError('Boot volume absence was not corroborated by a scoped list.')
            boot['deletion_confirmed_at'] = _now()
        entry['status'] = 'deleted'
        entry['deletion_confirmed_at'] = _now()
        _save(job, persist)
        if emit:
            emit(job, 'Destroy', f'Deleted OCI distributed candidate {key}.')
    contract['status'] = 'deleted'
    contract['deletion_confirmed_at'] = _now()
    _save(job, persist)
