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
import math
import re
import time
import uuid

import oci as sdk

from ..deathstarbench_contract import DISTRIBUTED_TIERED_TOPOLOGY_ID, K3S_RUNTIME_ID
from ..deathstarbench_topology import DeathStarBenchTopologyManifest, build_topology_manifest
from ..resource_inventory import ROLE_NODE_INVENTORY_KEY, RoleNodeInventory, StorageResource, persist_role_node_inventory


CONTRACT_KEY = 'oci_dsb_infrastructure'
SCHEMA_VERSION = 1
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
WAIT_SECONDS = 1800


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


def _list(client, method, **kwargs):
    result = []
    page = None
    while True:
        response = getattr(client, method)(**kwargs, **({'page': page} if page else {}))
        result.extend(_dict(item) for item in response.data)
        page = response.headers.get('opc-next-page')
        if not page:
            return result


def _get(client, method, resource_id):
    try:
        response = getattr(client, method)(resource_id)
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
                'ocpus', 'memory_gb', 'application_image_id', 'support_image_id', 'public_key'}
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
                    'freeform_tags': {'managed-by': MANAGED_BY, 'benchmark-job': job_id, 'benchmark-role': key}}
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
        response = clients['compute'].get_image(image_id)
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
                device=DATABASE_DEVICE, mount_point='/var/lib/deathstarbench', ephemeral=False,
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
                if previous[key] != found[key]:
                    raise LifecycleError('Implicit attachment identity changed.')
            elif previous[key]['id'] != found[key]['id'] or _stable_implicit(previous[key]['snapshot']) != _stable_implicit(found[key]['snapshot']):
                raise LifecycleError('Implicit resource changed after inventory capture.')
    entry['implicit'] = found


def _stable_implicit(snapshot):
    # Hydration and attachment lifecycle transitions are expected during normal
    # VM start/stop. They do not change ownership, attachment IDs, or disk data.
    return {key: value for key, value in snapshot.items() if key not in {'lifecycle_state', 'is_hydrated'}}


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
                         allow_empty=not entry.get('rules_attempted_at'))
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
            kwargs = {'if_match': observed[1]}
            if spec['kind'] == 'instance':
                kwargs['preserve_boot_volume'] = True
            if not in_progress:
                getattr(clients[spec['client']], method)(entry['id'], **kwargs)
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
                    clients['block'].delete_boot_volume(boot['id'], if_match=etag)
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
        entry['status'] = 'deleted'
        _save(job, persist)
        if emit:
            emit(job, 'Destroy', f'Deleted OCI distributed candidate {key}.')
    contract['status'] = 'deleted'
    _save(job, persist)
