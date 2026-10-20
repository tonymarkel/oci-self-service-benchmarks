"""Cloud wiring for private support-stage operators, never public dispatch."""

from __future__ import annotations

import copy
import hashlib
import re

from app import main as application
from app.providers import aws, azure, gcp, oci
from app.run_lease import require_acquired_run_lease
from scripts import qualify_aws_oci_deathstarbench_distributed as legacy_pins
from scripts.deathstarbench_candidate_operator_state import canonical, fingerprint


CLEANUP_SOURCE_KEY = 'candidate_cloud_cleanup_source'
_OPERATOR_JOURNALS = frozenset({'candidate_support_operator', 'hotel_initialization_operator', 'hotel_startup_operator'})


def _startup_require(condition, message='Invalid independent startup provider ownership.'):
    if not condition:
        raise ValueError(message)


def _startup_target(kind, resource_id, name, scope, parent=None, link=None, incarnation=None):
    return {'candidate_schema': 'hotel-startup-owned-target-v1', 'kind': kind, 'id': resource_id,
            'name': name, 'scope': copy.deepcopy(scope), 'parent_id': parent,
            'self_link': link, 'incarnation_id': incarnation}


def _startup_aws_native(job):
    """Finite native ownership only, never SDK responses or application data."""
    excluded = {aws.K3S_RUNTIME_JOURNAL_KEY, aws.DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
                aws.DEATHSTARBENCH_EXECUTION_JOURNAL_KEY, aws.DISTRIBUTED_NETWORK_QUALIFICATION_KEY}
    allowed = aws._AWS_DSB_TERMINAL_RESOURCE_KEYS - excluded
    resources = {key: copy.deepcopy(value) for key, value in job['resources'].items() if key in allowed}
    _startup_require(not any(key.startswith('aws_') and key not in allowed for key in job['resources']))
    state = aws._dsb_load({**job, 'resources': resources}, allow_quiesced_load_generator=True)
    contract, graph = state['contract'], state['graph']
    _startup_require(set(contract) == {'job_id', 'account_id', 'profile', 'region', 'zone', 'shapes',
                                      'public_key', 'support_image', 'application_image'})
    _startup_require(re.fullmatch(r'[0-9]{12}', contract['account_id']) is not None)
    _startup_require(all(type(contract[key]) is str and re.fullmatch(r'[A-Za-z0-9_.-]{1,255}', contract[key])
                         for key in ('profile', 'region', 'zone')))
    _startup_require(contract['profile'] == aws._profile(job['plan']) and contract['region'] == aws._region(job['plan'])
        and contract['shapes']['application'] == aws._instance_type(job['plan']))
    requested_zone = job['plan'].get('availability_zone') or job['plan'].get('availability_domain')
    _startup_require(not requested_zone or contract['zone'] == requested_zone)
    _startup_require(hashlib.sha256(contract['public_key'].encode()).hexdigest() == job['candidate_support_pin']['ssh_public_key_sha256'])
    aws._dsb_public_key_identity(contract['public_key'])
    pin = job['candidate_support_pin']['provider_pin']
    for kind in ('support', 'application'):
        image = contract[kind + '_image']
        _startup_require(set(image) == {'image_id', 'owner_id', 'name', 'creation_date', 'product_code', 'architecture', 'root_device_name'})
        _startup_require(image == pin[kind + '_image'])
        _startup_require(all(type(value) is str and re.fullmatch(r'[A-Za-z0-9_.:/+-]{1,255}', value) for value in image.values()))
    for key, row in graph.items():
        _startup_require(set(row) <= ({'status', 'id', 'public_addresses'} if key.startswith('instance-') else {'status', 'id'}))
        _startup_require(row.get('status') in {'planned', 'creating', 'create_ambiguous', 'running', 'deleting', 'delete_ambiguous', 'deleted'})
        if row.get('id') is not None:
            _startup_require(row['status'] != 'planned')
            kind = aws._dsb_specs(contract, graph)[key][0]
            prefix = {'instance': 'i', 'route': 'rtb', 'eip': 'eipalloc', 'volume': 'vol'}.get(kind, kind)
            _startup_require(type(row['id']) is str and re.fullmatch(prefix + r'-[0-9a-f]+', row['id']))
        if 'public_addresses' in row:
            _startup_require(type(row['public_addresses']) is list and len(row['public_addresses']) <= 1)
            for address in row['public_addresses']:
                import ipaddress
                _startup_require(str(ipaddress.IPv4Address(address)) == address)
    return resources, state


def _startup_aws_projection(job, previous, *, transition=None, finalizing=False, verified_implicit=None):
    from app import deathstarbench_hotel_startup_ownership as ownership
    resources, state = _startup_aws_native(job)
    contract, graph = state['contract'], state['graph']
    scope = {'account_id': contract['account_id'], 'region': contract['region']}
    known, absent, attempts = {}, {}, {}
    unresolved = set(previous['unresolved_creates'])
    old_graph = previous['resources'].get(aws.AWS_DSB_GRAPH_KEY, {}).get('graph', {})
    implicit_kinds = {'vpc-default-route': ('route', 'rtb', 'vpc'), 'vpc-default-sg': ('sg', 'sg', 'vpc'),
                      'vpc-default-acl': ('network_acl', 'acl', 'vpc')}
    for role in aws.AWS_DSB_ADDRESSES:
        for suffix, kind, prefix in (('boot-volume', 'volume', 'vol'), ('primary-eni', 'network_interface', 'eni'),
                                     ('eni-attachment', 'eni_attachment', 'eni-attach')):
            implicit_kinds[role + '-' + suffix] = (kind, prefix, 'instance-' + role)
    def implicit_target(key, row):
        if key.startswith('nat-eni-'):
            _startup_require(key == 'nat-eni-' + row['id'])
            expected = ('network_interface', 'eni', 'nat')
        else:
            _startup_require(key in implicit_kinds)
            expected = implicit_kinds[key]
        kind, prefix, parent = expected
        target = _startup_target('aws_' + kind, row['id'], row['id'], scope, graph[parent].get('id'))
        _startup_require(re.fullmatch(prefix + r'-[0-9a-f]+', row['id']) and row == target)
        return target
    for semantic, row in previous['known_identities'].items():
        key = semantic.removeprefix('aws-')
        if key in graph:
            continue
        implicit_target(key, row)
        known[semantic] = row
        if row['kind'] != 'aws_eni_attachment':
            absent[semantic] = row
    _startup_require(set(previous['create_attempts']) <= {'aws-' + key for key in graph})
    if transition is not None:
        _startup_require(type(transition) is dict and set(transition) == {'resource_key', 'request_identity', 'result', 'classification'})
        key = transition['resource_key']
        _startup_require(key in graph and 'aws-' + key in unresolved and old_graph.get(key, {}).get('status') == 'creating')
        _startup_require(transition['result'] == 'rejected-before-allocation'
                         and transition['classification'] in {'UnauthorizedOperation', 'AuthFailure', 'OptInRequired',
                             'InvalidParameterValue', 'InvalidParameterCombination', 'InsufficientInstanceCapacity',
                             'InstanceLimitExceeded', 'VcpuLimitExceeded', 'AddressLimitExceeded', 'InsufficientVolumeCapacity'})
        _startup_require(transition['request_identity'] == aws._dsb_specs(contract, graph)[key][1].get('ClientToken')
                         and graph[key] == {'status': 'planned'})
        unresolved.remove('aws-' + key)
    for key, row in graph.items():
        semantic = 'aws-' + key
        kind, request = aws._dsb_specs(contract, graph)[key]
        name = request.get('KeyName') or request.get('GroupName') or next(tag['Value'] for tag in aws._dsb_tags(job['id'], key) if tag['Key'] == 'Name')
        parent = graph['vpc'].get('id') if kind in ('igw', 'subnet', 'sg', 'route', 'nat', 'instance') else None
        target = _startup_target('aws_' + kind, row.get('id'), name, scope, parent)
        if semantic in previous['create_attempts']:
            _startup_require(previous['create_attempts'][semantic] == {'candidate_schema': ownership.ATTEMPT_SCHEMA,
                'kind': 'aws_' + kind, 'name': name, 'scope': scope, 'request_id': None})
        old = old_graph.get(key, {})
        if old.get('id'):
            _startup_require(row.get('id') == old['id'], 'Original AWS allocation identity was removed or replaced.')
        if row.get('id'):
            _startup_require(old.get('id') == row['id'] or old.get('status') in ('creating', 'create_ambiguous'),
                             'AWS identity has no prior original create intent.')
            known[semantic] = target
            absent[semantic] = target
            unresolved.discard(semantic)
        if row['status'] in ('creating', 'create_ambiguous'):
            attempts[semantic] = {'candidate_schema': ownership.ATTEMPT_SCHEMA, 'kind': 'aws_' + kind,
                                 'name': name, 'scope': scope, 'request_id': None}
            if not row.get('id'):
                unresolved.add(semantic)
        if finalizing and not row.get('id') and semantic not in previous['create_attempts'] and semantic not in attempts:
            absent[semantic] = target
        if old.get('status') in ('deleting', 'delete_ambiguous', 'deleted'):
            _startup_require(row['status'] in ('deleting', 'delete_ambiguous', 'deleted'))
    supplied = job[ownership.CHECKPOINT_KEY]
    # Only the exact-parent read-only preflight may introduce implicit rows.
    if verified_implicit is not None:
        _startup_require(finalizing and previous['seal_fingerprint'] is None)
        for key, row in verified_implicit.items():
            _startup_require(set(row) == {'kind', 'id', 'parent_id'} and row['kind'] in {
                'volume', 'network_interface', 'eni_attachment', 'sg', 'route', 'network_acl'})
            _startup_require(row['parent_id'] in {entry.get('id') for entry in graph.values()} and row['id'])
            target = _startup_target('aws_' + row['kind'], row['id'], row['id'], scope, row['parent_id'])
            implicit_target(key, target)
            known['aws-' + key] = target
            if row['kind'] != 'eni_attachment':
                absent['aws-' + key] = target
    audits = supplied['audits']
    for envelope in audits:
        identity = envelope['cloud_identity']
        _startup_require(set(identity) == {'run_id', 'provider', 'account_id', 'contract', 'projection', 'resource_ids'})
        _startup_require(identity['run_id'] == job['id'] and identity['provider'] == 'aws'
                         and identity['account_id'] == scope['account_id'] and identity['contract'] == contract
                         and identity['resource_ids'] == {key: entry.get('id') for key, entry in graph.items()})
        projection = identity['projection']
        _startup_require(set(projection) == {'provider', 'job_id', 'account_id', 'region', 'zone', 'ssh_user',
            'topology_fingerprint', 'database_volume_id', 'aliases', 'nodes', 'support_image', 'application_image'})
        _startup_require(projection['provider'] == 'aws' and projection['job_id'] == job['id']
                         and projection['account_id'] == contract['account_id']
                         and projection['region'] == contract['region'] and projection['zone'] == contract['zone']
                         and projection['ssh_user'] == 'rocky'
                         and projection['support_image'] == contract['support_image']
                         and projection['application_image'] == contract['application_image']
                         and projection['database_volume_id'] == graph['database-data']['id']
                         and projection['topology_fingerprint'] == resources[aws.TOPOLOGY_FINGERPRINT_KEY]
                         and projection['aliases'] == aws._dsb_runtime_aliases(contract, graph)
                         and set(projection['nodes']) == set(aws.AWS_DSB_ADDRESSES))
        for role, node in projection['nodes'].items():
            _startup_require(set(node) == {'key', 'role', 'node_name', 'instance_id', 'shape', 'architecture',
                                           'private_ip', 'public_ip', 'host_key', 'jump_host_key'})
            inventory = aws._dsb_inventory(contract, graph).node(role)
            prefix = 'aws_dsb_' + role.replace('-', '_')
            direct = role in ('control', 'load-generator')
            _startup_require(node == {'key': role, 'role': inventory.role, 'node_name': inventory.provider_resource_name,
                'instance_id': inventory.provider_resource_id, 'shape': inventory.shape, 'architecture': inventory.architecture,
                'private_ip': inventory.private_addresses[0], 'public_ip': inventory.public_addresses[0] if inventory.public_addresses else None,
                'host_key': prefix + ('_public_ip' if direct else '_private_ip'), 'jump_host_key': None if direct else 'aws_dsb_control_public_ip'})
    return {'provider_scope': scope, 'create_contract': copy.deepcopy(contract), 'resources': resources,
            'known_identities': known, 'absence_targets': absent, 'create_attempts': attempts,
            'unresolved_creates': sorted(unresolved), 'audits': copy.deepcopy(audits)}


def project_startup_provider_ownership(job, previous, *, transition=None, finalizing=False, verified_implicit=None):
    if job['plan']['provider'] == 'aws':
        return _startup_aws_projection(job, previous, transition=transition, finalizing=finalizing, verified_implicit=verified_implicit)
    if job['plan']['provider'] == 'oci':
        return _startup_oci_projection(job, previous, finalizing=finalizing)
    if job['plan']['provider'] in ('gcp', 'azure'):
        return _startup_flat_projection(job, previous, transition=transition, finalizing=finalizing)
    raise ValueError('Startup native ownership bridge is not available for this provider.')


def validate_startup_provider_checkpoint(checkpoint, roots):
    from app import deathstarbench_hotel_startup_ownership as ownership
    carrier = {**roots, 'resources': copy.deepcopy(checkpoint['resources']), ownership.CHECKPOINT_KEY: checkpoint}
    projection = project_startup_provider_ownership(carrier, checkpoint)
    if checkpoint['provider'] in ('oci', 'azure') or (checkpoint['provider'] == 'gcp' and checkpoint['seal_fingerprint'] is None):
        _startup_require(set(projection['known_identities']) == set(checkpoint['known_identities']),
                         'Startup checkpoint contains unsupported native known identities.')
    checked = ownership.update_hotel_startup_ownership_checkpoint(checkpoint, projection)
    _startup_require(canonical(checked) == canonical(checkpoint), 'Independent native checkpoint facts are inconsistent.')


def _startup_oci_projection(job, previous, *, finalizing=False):
    from app import deathstarbench_hotel_startup_ownership as ownership
    allowed = {oci.CONTRACT_KEY, oci.ROLE_NODE_INVENTORY_KEY} | oci.RUNTIME_ALIAS_KEYS
    resources = {key: copy.deepcopy(value) for key, value in job['resources'].items() if key in allowed}
    contract = oci.validate_distributed_deathstarbench_candidate({**job, 'resources': resources}, ownership_snapshot='startup-v1')
    _startup_require(set(contract) <= {'schema_version', 'job_id', 'inputs', 'manifest', 'images', 'created_at', 'status', 'entries', 'deletion_confirmed_at'})
    _startup_require(oci._valid_timestamp(contract['created_at']))
    _startup_require(hashlib.sha256(contract['inputs']['public_key'].encode()).hexdigest() == job['candidate_support_pin']['ssh_public_key_sha256'])
    _startup_require(contract['inputs'] == legacy_pins._oci_inputs(job['candidate_support_pin']['provider_pin'],
        {'public_key': contract['inputs']['public_key']}))
    for image in contract['images'].values():
        _startup_require(set(image) == {'id', 'display_name', 'compartment_id', 'operating_system', 'operating_system_version', 'lifecycle_state'})
    scope = {key: contract['inputs'][key] for key in ('compartment_id', 'region')}
    create = {key: contract[key] for key in ('schema_version', 'job_id', 'inputs', 'manifest', 'images', 'created_at')}
    create['entries'] = {key: {field: row[field] for field in ('spec', 'retry_token')} for key, row in contract['entries'].items()}
    known, absent, attempts, unresolved = {}, {}, {}, []
    old_entries = previous['resources'].get(oci.CONTRACT_KEY, {}).get('entries', {})
    for key, row in contract['entries'].items():
        _startup_require(set(row) <= {'spec', 'retry_token', 'status', 'id', 'attempted_at', 'last_create_rejection', 'implicit',
            'rules_attempted_at', 'rules_ready', 'delete_attempted_at', 'deletion_confirmed_at', 'etag_fallback_attempted_at',
            'etag_fallback_observed', 'etag_fallback_confirmed'})
        for field in ('attempted_at', 'rules_attempted_at', 'delete_attempted_at', 'deletion_confirmed_at', 'etag_fallback_attempted_at'):
            _startup_require(field not in row or oci._valid_timestamp(row[field]))
        for field in ('rules_ready',):
            _startup_require(field not in row or type(row[field]) is bool)
        for field in ('etag_fallback_observed', 'etag_fallback_confirmed'):
            _startup_require(field not in row or (type(row[field]) is str and re.fullmatch(r'[\x20-\x7e]{1,512}', row[field])))
        _startup_require(not any(field in row for field in ('rules_ready', 'rules_attempted_at')) or row['spec']['kind'] == 'network_security_group')
        _startup_require(not any(field in row for field in ('etag_fallback_attempted_at', 'etag_fallback_observed', 'etag_fallback_confirmed')) or row['spec']['kind'] == 'internet_gateway')
        semantic = 'oci-' + key
        target_scope = {**scope, **({'availability_domain': contract['inputs']['availability_domain']} if 'availability_domain' in row['spec']['body'] else {})}
        parent = (contract['entries']['database'].get('id') if key == 'database-attachment' else
                  contract['entries']['vcn'].get('id') if 'vcn_id' in row['spec']['body'] else None)
        target = _startup_target('oci_' + row['spec']['kind'], row.get('id'), row['spec']['body']['display_name'], target_scope, parent)
        old = old_entries.get(key, {})
        if old.get('id'):
            _startup_require(row.get('id') == old['id'])
        if row.get('id'):
            _startup_require(old.get('id') == row['id'] or old.get('status') == 'creating')
            oci._startup_ocid(row['id'], row['spec']['kind'])
            known[semantic] = absent[semantic] = target
        if row.get('attempted_at'):
            attempts[semantic] = {'candidate_schema': ownership.ATTEMPT_SCHEMA, 'kind': target['kind'],
                'name': target['name'], 'scope': target_scope, 'request_id': row['retry_token']}
            rejection = row.get('last_create_rejection')
            rejected = False
            if rejection is not None:
                _startup_require(set(rejection) == {'status', 'code', 'attempted_at', 'recorded_at'}
                    and rejection['attempted_at'] == row['attempted_at']
                    and oci._ordered_timestamps(rejection['attempted_at'], rejection['recorded_at']))
                rejected = oci._confirmed_create_rejection(oci.sdk.exceptions.ServiceError(rejection['status'], rejection['code'], {}, ''))
                _startup_require(rejected and row['status'] in ('planned', 'deleted'))
            if not row.get('id') and not rejected:
                unresolved.append(semantic)
        elif finalizing and not row.get('id'):
            absent[semantic] = target
        implicit = row.get('implicit', {})
        _startup_require(not old.get('implicit') or set(implicit) == set(old['implicit']), 'OCI original implicit ownership was removed.')
        for child_kind, child in implicit.items():
            old_child = old.get('implicit', {}).get(child_kind)
            if old_child is not None:
                _startup_require(oci._stable_implicit(child if child_kind == 'attachments' else child['snapshot'])
                    == oci._stable_implicit(old_child if child_kind == 'attachments' else old_child['snapshot']),
                    'OCI original implicit configuration changed.')
            children = ([(name + '_attachment', data, None) for name, data in child.items()] if child_kind == 'attachments'
                        else [(child_kind, child['snapshot'], child)])
            for name, data, wrapper in children:
                kind = 'boot_volume_attachment' if name == 'boot_attachment' else name
                _startup_require(oci._startup_implicit_snapshot(kind, data, contract=contract, entry=row) == data,
                                 'OCI persisted implicit snapshot is not its approved finite projection.')
                if wrapper is not None:
                    _startup_require(set(wrapper) <= {'id', 'kind', 'client', 'snapshot', 'etag', 'delete_attempted_at', 'deletion_confirmed_at'}
                        and wrapper['id'] == data['id'] and wrapper['kind'] == kind
                        and wrapper['client'] == ('block' if kind == 'boot_volume' else 'network')
                        and type(wrapper['etag']) is str and re.fullmatch(r'[\x20-\x7e]{1,512}', wrapper['etag']))
                    for field in ('delete_attempted_at', 'deletion_confirmed_at'):
                        _startup_require(field not in wrapper or (kind == 'boot_volume' and oci._valid_timestamp(wrapper[field])))
                suffix = {'route_table': 'default-route', 'security_list': 'default-security', 'dhcp_options': 'default-dhcp',
                    'boot_volume': 'boot-volume', 'boot_volume_attachment': 'boot-attachment', 'vnic_attachment': 'vnic-attachment'}.get(kind, kind)
                child_scope = {**scope, **({'availability_domain': data['availability_domain']} if 'availability_domain' in data else {})}
                typed = _startup_target('oci_' + kind, data['id'], data['id'], child_scope, row['id'])
                known['oci-' + key + '-' + suffix] = absent['oci-' + key + '-' + suffix] = typed
    resources[oci.CONTRACT_KEY] = contract
    audits = job[ownership.CHECKPOINT_KEY]['audits']
    for envelope in audits:
        identity = envelope['cloud_identity']
        _startup_require(set(identity) == {'run_id', 'provider', 'inputs', 'projection', 'resource_ids'}
            and identity['run_id'] == job['id'] and identity['provider'] == 'oci' and identity['inputs'] == contract['inputs']
            and identity['resource_ids'] == {key: row.get('id') for key, row in contract['entries'].items()}
            and identity['projection'] == oci._runtime_projection(contract))
    return {'provider_scope': scope, 'create_contract': create, 'resources': resources, 'known_identities': known,
            'absence_targets': absent, 'create_attempts': attempts, 'unresolved_creates': unresolved, 'audits': audits}


_STARTUP_FLAT_COMMON = frozenset({'provider', 'region', 'availability_zone', 'instance_type', 'architecture', 'vcpu',
    'ocpus', 'memory_gb', 'image_id', 'image_name', 'ssh_user', 'instance_id', 'public_ip', 'private_ip', 'data_volume_device',
    'loadgen_instance_id', 'loadgen_public_ip', 'loadgen_private_ip', 'loadgen_shape', 'loadgen_vcpus', 'loadgen_memory_gb',
    'loadgen_architecture', 'loadgen_image_id', 'loadgen_image_name', 'loadgen_network_bandwidth_gbps', 'loadgen_network_capacity_kind'})


def _startup_flat_resources(job, previous):
    from scripts import deathstarbench_candidate_cloud_audit as audit
    from dataclasses import replace
    from app.resource_inventory import load_role_node_inventory, persist_role_node_inventory
    module = gcp if job['plan']['provider'] == 'gcp' else azure
    audit.require_known_candidate_resource_keys(job)
    allowed = (audit._GCP_PROVIDER_KEYS if module is gcp else audit._AZURE_PROVIDER_KEYS) | _STARTUP_FLAT_COMMON | {
        module.ROLE_NODE_INVENTORY_KEY, module.DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
        module.DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY, module.DISTRIBUTED_PROVIDER_TERMINAL_KEY}
    resources = {key: copy.deepcopy(value) for key, value in job['resources'].items()
                 if key in allowed and key != 'azure_subscription_name' and not key.endswith('_reconciliation_error')}
    complex_keys = {module.ROLE_NODE_INVENTORY_KEY, module.DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
                    module.DISTRIBUTED_PROVIDER_TERMINAL_KEY, 'gcp_dsb_expected_names', 'azure_resource_group_tags'}
    for key, value in resources.items():
        if key in complex_keys:
            continue
        if key in ('gcp_web_benchmarks', 'azure_web_benchmarks'):
            _startup_require(value == ['deathstarbench'])
        elif type(value) is str:
            _startup_require(re.fullmatch(r'[A-Za-z0-9_.:/+\-]{1,2048}', value) is not None,
                             'Unsafe native startup scalar field.')
        else:
            _startup_require(value is None or type(value) in (bool, int, float))
            canonical(value)
    if module.DISTRIBUTED_PROVIDER_TERMINAL_KEY in resources:
        from app.deathstarbench_contract import distributed_provider_terminal_marker
        _startup_require(previous['seal_fingerprint'] is not None
            and resources[module.DISTRIBUTED_PROVIDER_TERMINAL_KEY] == distributed_provider_terminal_marker(job['plan']['provider'], job['id'])
            and not any(key.startswith(job['plan']['provider'] + '_') for key in resources))
        return resources, None, None
    inventory = load_role_node_inventory(resources)
    _startup_require(resources[module.ROLE_NODE_INVENTORY_KEY] == inventory.as_dict())
    if module is gcp:
        # The writer saves positive create IDs before its later inventory
        # readiness update. Normalize only that proven identity, never readiness.
        nodes = []
        for node in inventory.nodes:
            rid = resources.get(gcp.DISTRIBUTED_NODE_PREFIXES[node.key] + '_id')
            _startup_require(not rid or node.provider_resource_id in (None, rid))
            storage = []
            for disk in node.storage:
                disk_id = resources.get('gcp_dsb_database_data_disk_id') if disk.key == 'database-data' else None
                _startup_require(not disk_id or disk.provider_resource_id in (None, disk_id))
                storage.append(replace(disk, provider_resource_id=disk_id or disk.provider_resource_id))
            nodes.append(replace(node, provider_resource_id=rid or node.provider_resource_id, storage=tuple(storage)))
        inventory = replace(inventory, nodes=tuple(nodes))
        persist_role_node_inventory(resources, inventory)
        manifest, inventory, names = gcp._load_distributed_cleanup_contract({**job, 'resources': resources})
    else:
        manifest, inventory = azure._load_topology_inventory_contract(resources)
        names = azure._validate_managed_expected_ids(resources, subscription_id=resources['azure_subscription_id'],
            resource_group=resources['azure_resource_group_name'], manifest=manifest)
        _startup_require(resources['azure_resource_group_tags'] == azure._tags(job['id'], 'resource-group'))
    _startup_require(resources[module.DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY] == manifest.as_dict())
    from app.models import BenchmarkPlan
    plan = BenchmarkPlan.model_validate({**job['plan'], 'ssh_private_key': 'cleanup-only', 'ssh_public_key': 'cleanup-only'})
    if module is gcp:
        _startup_require(resources['gcp_project_id'] == plan.gcp_project_id)
        audit._gcp_contract({**job, 'resources': resources}, plan, resources)
    else:
        _startup_require(resources['azure_subscription_id'] == plan.azure_subscription_id)
        audit._azure_contract(job, plan, resources)
    return resources, inventory, names


def _startup_flat_projection(job, previous, *, transition=None, finalizing=False):
    from app import deathstarbench_hotel_startup_ownership as ownership
    from app.resource_inventory import RoleNodeInventory
    from scripts import deathstarbench_candidate_cloud_audit as audit
    kind = job['plan']['provider']
    module = gcp if kind == 'gcp' else azure
    resources, inventory, names = _startup_flat_resources(job, previous)
    if inventory is None:
        return {**{key: copy.deepcopy(previous[key]) for key in ownership._PROJECTION}, 'resources': resources}
    if kind == 'gcp':
        scope = {'project_id': resources['gcp_project_id'], 'compute_project_id': resources['gcp_compute_project_id']}
        dynamic = {prefix + '_' + field for prefix in gcp.DISTRIBUTED_RESOURCE_PREFIXES
                   for field in ('name', 'id', 'self_link', 'request_id', 'create_ambiguous', 'reconciliation_error', 'delete_request_id')}
        dynamic |= {prefix + '_' + field for prefix in gcp.DISTRIBUTED_BOOT_PREFIXES.values()
                    for field in ('type', 'size_gb', 'provisioned_iops', 'provisioned_throughput_mibps')}
        dynamic |= {'gcp_dsb_router_nat_configured', 'gcp_dsb_router_nat_request_id', 'gcp_dsb_router_nat_update_ambiguous'}
        dynamic |= {'gcp_dsb_' + role.replace('-', '_') + suffix for role in gcp.DISTRIBUTED_PRIVATE_ADDRESSES
                    for suffix in ('_public_ip', '_instance_public_ip', '_instance_private_ip')}
        static_keys = (audit._GCP_PROVIDER_KEYS - dynamic) | {'provider', 'region', 'availability_zone', 'instance_type',
            'architecture', 'ocpus', 'memory_gb', 'image_id', 'image_name', 'ssh_user', module.DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
            module.DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY}
    else:
        scope = {'subscription_id': resources['azure_subscription_id'], 'tenant_id': resources['azure_tenant_id']}
        static_keys = {'provider', 'region', 'availability_zone', 'instance_type', 'architecture', 'vcpu', 'ocpus', 'memory_gb',
            'image_id', 'image_name', 'ssh_user', 'azure_subscription_id', 'azure_tenant_id', 'azure_zone', 'azure_vm_size',
            'azure_architecture', 'azure_image_id', 'azure_image_urn', 'azure_image_version', 'azure_web_benchmarks',
            'azure_distributed_candidate', 'azure_database_disk_type', 'azure_database_disk_size_gb', 'azure_database_disk_iops',
            'azure_database_disk_throughput_mibps', 'azure_database_disk_device', 'azure_resource_group_name',
            'azure_resource_group_expected_id', 'azure_resource_group_tags', module.DEATHSTARBENCH_TOPOLOGY_MANIFEST_KEY,
            module.DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY}
        static_keys |= {key for key in audit._AZURE_PROVIDER_KEYS if key.endswith('_expected_id') or key.startswith(('azure_dsb_application_image_', 'azure_dsb_support_image_'))}
        static_keys |= {key.removesuffix('_expected_id') + '_name' for key in names}
    values = {key: resources[key] for key in static_keys if key in resources}
    create = {'values': values, 'planned_inventory': previous['create_contract']['planned_inventory'] if previous['contract_recorded'] else inventory.as_dict()}
    if previous['contract_recorded']:
        expected_inventory = RoleNodeInventory.from_dict(create['planned_inventory'])
        module._validate_expected_role_inventory(inventory, expected_inventory)
    known, absent, attempts = {}, {}, {}
    unresolved = set(previous['unresolved_creates'])
    old_resources = previous['resources']
    if kind == 'gcp':
        resolved = set()
        if transition is not None:
            _startup_require(type(transition) is dict and set(transition) == {'resource_key', 'request_identity', 'result', 'classification'})
            prefix = transition['resource_key']
            _startup_require(prefix in names and prefix in unresolved and old_resources.get(prefix + '_request_id') == transition['request_identity'])
            _startup_require((transition['result'] == 'insert-rejected-before-allocation' and transition['classification'] in {'400', '401', '403', '404', '405', '410', '411', '413', '414', '415', '422'})
                or (transition['result'] == 'terminal-operation-failed-and-exact-target-absent' and transition['classification'] == 'completed-error-exact-absence'))
            resolved.add(prefix)
            for role, parent in gcp.DISTRIBUTED_NODE_PREFIXES.items():
                if prefix == parent:
                    boot = gcp.DISTRIBUTED_BOOT_PREFIXES[role]
                    _startup_require(old_resources.get(boot + '_request_id') == transition['request_identity'])
                    resolved.add(boot)
            _startup_require(all(not resources.get(prefix + '_name') and not resources.get(prefix + '_id') for prefix in resolved))
            unresolved -= resolved
        for prefix, name in names.items():
            rid, link, request = (resources.get(prefix + '_' + field) for field in ('id', 'self_link', 'request_id'))
            clients = dict.fromkeys(('instances', 'disks', 'firewalls', 'subnetworks', 'routers', 'networks'))
            _, request_type, arguments = gcp._resource_get_spec(prefix, resources, clients)
            endpoint = {'GetInstanceRequest': 'instances', 'GetDiskRequest': 'disks', 'GetFirewallRequest': 'firewalls',
                'GetSubnetworkRequest': 'subnetworks', 'GetRouterRequest': 'routers', 'GetNetworkRequest': 'networks'}[request_type]
            placement = ('zones/' + arguments['zone'] if 'zone' in arguments else
                         'regions/' + arguments['region'] if 'region' in arguments else 'global')
            target_scope = {**scope, **{field: arguments[field] for field in ('zone', 'region') if field in arguments}}
            expected_link = f"projects/{scope['project_id']}/{placement}/{endpoint}/{name}"
            parent = None
            if prefix in gcp.DISTRIBUTED_BOOT_PREFIXES.values():
                role = next(role for role, value in gcp.DISTRIBUTED_BOOT_PREFIXES.items() if value == prefix)
                parent = resources.get(gcp.DISTRIBUTED_NODE_PREFIXES[role] + '_id') or previous['known_identities'].get(prefix, {}).get('parent_id')
            elif endpoint in ('subnetworks', 'routers', 'firewalls'):
                parent = resources.get('gcp_dsb_network_id') or previous['known_identities'].get(prefix, {}).get('parent_id')
            old_id = old_resources.get(prefix + '_id')
            if old_id and rid is None:
                _startup_require(previous['seal_fingerprint'] is not None and old_resources.get(prefix + '_delete_request_id'), 'GCP original identity disappeared without owned delete intent.')
            if rid:
                _startup_require(type(rid) is str and re.fullmatch(r'[1-9][0-9]*', rid) and type(link) is str
                    and (old_id == rid or prefix in previous['create_attempts']))
                _startup_require(not old_id or old_id == rid)
                _startup_require(re.fullmatch(r'(?:https://(?:compute|www)\.googleapis\.com/compute/v1/)?projects/[A-Za-z0-9_./-]+', link)
                                 and gcp._scoped_link_equal(link, expected_link))
                target = _startup_target('gcp_' + endpoint.removesuffix('s'), rid, name, target_scope, parent, link=link)
                known[prefix] = absent[prefix] = target
                unresolved.discard(prefix)
            if request:
                _startup_require(gcp._valid_request_id(request) and resources.get(prefix + '_name') == name)
                attempts[prefix] = {'candidate_schema': ownership.ATTEMPT_SCHEMA, 'kind': 'gcp_create', 'name': name, 'scope': target_scope, 'request_id': request}
                if not rid:
                    unresolved.add(prefix)
            if finalizing and not rid and prefix not in previous['create_attempts'] and prefix not in attempts:
                absent[prefix] = _startup_target('gcp_expected_name', None, name, target_scope)
    else:
        group = resources['azure_resource_group_name']
        expected_group = azure._expected_id(scope['subscription_id'], group)
        _startup_require(resources['azure_resource_group_expected_id'] == expected_group)
        group_target = _startup_target('azure_resource_group', expected_group, group, scope)
        absent['azure_resource_group'] = group_target
        if resources.get('azure_resource_group_id'):
            _startup_require(azure._normalize_id(resources['azure_resource_group_id']) == azure._normalize_id(expected_group))
            known['azure_resource_group'] = group_target
        if resources.get('azure_resource_group_create_ambiguous') is True or 'azure_resource_group' in previous['create_attempts']:
            attempts['azure_resource_group'] = {'candidate_schema': ownership.ATTEMPT_SCHEMA, 'kind': 'azure_resource_group',
                                               'name': group, 'scope': scope, 'request_id': None}
        for field, (name, expected_id) in names.items():
            prefix = field.removesuffix('_expected_id')
            rid = resources.get(prefix + '_id')
            if rid:
                _startup_require(azure._normalize_id(rid) == azure._normalize_id(expected_id))
                known[prefix] = _startup_target('azure_arm_child', expected_id, name, scope, expected_group)
            if resources.get(prefix + '_create_ambiguous') is True or resources.get(prefix + '_spec_fingerprint'):
                attempts[prefix] = {'candidate_schema': ownership.ATTEMPT_SCHEMA, 'kind': 'azure_arm_child',
                                    'name': name, 'scope': scope, 'request_id': None}
    audits = job[ownership.CHECKPOINT_KEY]['audits']
    for envelope in audits:
        known.update(_startup_flat_audit(job, resources, inventory, names, scope,
            {**previous['known_identities'], **known}, envelope['cloud_identity']))
    return {'provider_scope': scope, 'create_contract': create, 'resources': resources, 'known_identities': known,
            'absence_targets': absent, 'create_attempts': attempts, 'unresolved_creates': sorted(unresolved), 'audits': audits}


def _startup_flat_audit(job, resources, inventory, names, scope, known, identity):
    """Finite actual audit evidence, with UUID facts distinct from ARM targets."""
    import ipaddress
    import math
    from scripts import deathstarbench_candidate_cloud_audit as audit
    kind = job['plan']['provider']
    module = gcp if kind == 'gcp' else azure
    _startup_require(type(identity) is dict and set(identity) == {'revision', 'provider', 'run_id', 'plan_sha256',
        'topology_fingerprint', 'account', 'roles', 'images', 'resources', 'profiles'})
    _startup_require(identity['revision'] == audit.REVISION and identity['provider'] == kind
        and identity['run_id'] == job['id'] and identity['plan_sha256'] == audit._sha(job['plan'])
        and identity['topology_fingerprint'] == resources[module.DEATHSTARBENCH_TOPOLOGY_FINGERPRINT_KEY]
        and identity['account'] == scope)
    prefixes = set(names) if kind == 'gcp' else {key.removesuffix('_expected_id') for key in names}
    rows = identity['resources']
    fields = {'prefix', 'id', 'self_link' if kind == 'gcp' else 'unique_id'}
    _startup_require(type(rows) is list and len(rows) == len(prefixes)
        and all(type(row) is dict and set(row) == fields for row in rows)
        and {row['prefix'] for row in rows} == prefixes)
    extra, by_prefix = {}, {row['prefix']: row for row in rows}
    for prefix, row in by_prefix.items():
        if kind == 'gcp':
            _startup_require(prefix in known and row['id'] == known[prefix]['id']
                and row['self_link'] == known[prefix]['self_link'])
        else:
            name, arm = names[prefix + '_expected_id']
            _startup_require(row['id'] == arm)
            if prefix.endswith(('_instance', '_disk')):
                unique = audit._uuid(row['unique_id'], 'Invalid startup Azure incarnation.')
                _startup_require(unique == row['unique_id'])
                # The actual GET-backed audit establishes implicit OS disk ARM
                # identity too; expected-name-only entries are not made known.
                extra[prefix] = _startup_target('azure_arm_child', arm, name, scope,
                    resources['azure_resource_group_expected_id'])
                extra[prefix + '_incarnation'] = _startup_target(
                    'azure_vm_incarnation' if prefix.endswith('_instance') else 'azure_disk_incarnation',
                    unique, name, scope, arm, incarnation=unique)
            else:
                _startup_require(row['unique_id'] is None and prefix in known and known[prefix]['id'] == arm)
    images = identity['images']
    _startup_require(type(images) is dict and set(images) == {'application', 'support'})
    for image_kind, image in images.items():
        prefix = kind + '_dsb_' + image_kind + '_image_'
        expected = ({field: resources[prefix + ('id' if field == 'image_id' else field)]
                     for field in ('image_id', 'name', 'self_link', 'family', 'architecture')} if kind == 'gcp' else
                    {field: resources[prefix + field] for field in ('id', 'urn')})
        _startup_require(image == expected)
    roles = identity['roles']
    role_names = {node.key for node in inventory.nodes}
    _startup_require(type(roles) is dict and set(roles) == role_names)
    for node in inventory.nodes:
        role = node.key
        prefix = kind + '_dsb_' + role.replace('-', '_') + '_instance'
        expected = {'id': by_prefix[prefix]['id'], 'name': node.provider_resource_name,
            'shape': node.shape, 'architecture': node.architecture,
            'private_ip': module.DISTRIBUTED_PRIVATE_ADDRESSES[role],
            'public_ip': roles[role].get('public_ip'),
            'image_id': images['application' if role == 'application' else 'support']['image_id' if kind == 'gcp' else 'id']}
        if kind == 'azure':
            expected['unique_id'] = by_prefix[prefix]['unique_id']
        _startup_require(roles[role] == expected)
        public = expected['public_ip']
        _startup_require(public in (None, '') or type(public) is str)
        _startup_require(bool(public) == (role in module.DISTRIBUTED_PUBLIC_IP_NODES))
        if public:
            _startup_require(type(public) is str and str(ipaddress.IPv4Address(public)) == public)
        alias = kind + '_dsb_' + role.replace('-', '_') + '_public_ip'
        _startup_require(alias not in resources or resources[alias] == public)
    profiles = identity['profiles']
    _startup_require(type(profiles) is dict and set(profiles) == ({'roles', 'disks'} if kind == 'gcp' else {'roles', 'database', 'boot'})
        and type(profiles['roles']) is dict and set(profiles['roles']) == role_names)
    for role, profile in profiles['roles'].items():
        _startup_require(type(profile) is dict and set(profile) == {'shape', 'vcpus', 'memory_gib', 'architecture'}
            and profile['shape'] == roles[role]['shape'] and profile['architecture'] == roles[role]['architecture']
            and type(profile['vcpus']) is int and 0 < profile['vcpus'] <= 65536
            and type(profile['memory_gib']) in (int, float) and math.isfinite(profile['memory_gib'])
            and 0 < profile['memory_gib'] <= 1048576)
    if kind == 'gcp':
        _startup_require(profiles['disks'] == {prefix: list(gcp._expected_disk_profile(resources, prefix))
            for prefix in ('gcp_dsb_database_data_disk', *gcp.DISTRIBUTED_BOOT_PREFIXES.values())})
    else:
        _startup_require(profiles['database'] == {'type': resources['azure_database_disk_type'],
            'size_gib': resources['azure_database_disk_size_gb'], 'iops': resources['azure_database_disk_iops'],
            'throughput_mibps': resources['azure_database_disk_throughput_mibps']}
            and profiles['boot'] == {'type': 'Premium_LRS', 'size_gib': azure.DISTRIBUTED_OS_DISK_SIZE_GB})
    for key, row in extra.items():
        _startup_require(key not in known or known[key] == row, 'Original Azure audited incarnation changed.')
    return extra


class CandidateCloud:
    def __init__(self, plan, provider_pin, *, persist, emit, oci_config_file=None,
                 operator_journal_key='candidate_support_operator'):
        if type(operator_journal_key) is not str or operator_journal_key not in _OPERATOR_JOURNALS:
            raise ValueError('An explicit supported private operator journal is required.')
        self.plan = plan
        self.pin = copy.deepcopy(provider_pin)
        self.persist, self.emit = persist, emit
        self.oci_config_file = oci_config_file
        self.operator_journal_key = operator_journal_key
        self._clients = None

    def _cleanup_identity(self, job):
        """Select exact retained evidence; never fall back to another lifecycle.

        Legacy support behavior remains unchanged. The separate Hotel caller
        additionally revalidates both immutable pins, its lifecycle record and
        every retained fresh audit envelope before cleanup source publication.
        A failed partial provision may legitimately have no completed audit.
        """
        if type(self.operator_journal_key) is not str or self.operator_journal_key not in _OPERATOR_JOURNALS:
            raise ValueError('The private operator journal selector changed outside its reviewed allowlist.')
        record = job.get(self.operator_journal_key)
        if self.operator_journal_key == 'candidate_support_operator':
            return copy.deepcopy((record or {}).get('cloud_identity'))
        from app import deathstarbench_candidate_operator as support
        from app.deathstarbench_hotel_initialization_operator import (
            PIN_KEY, validate_hotel_initialization_pin,
            validate_hotel_initialization_operator_record,
        )
        pin = job.get('candidate_support_pin')
        support._validate_pin(job, pin, None, cleanup_only=True)
        hotel_pin = validate_hotel_initialization_pin(job.get(PIN_KEY), pin, cleanup_only=True)
        record = validate_hotel_initialization_operator_record(record, hotel_pin, pin)
        identity = record['cloud_identity']
        audits = record['audits']
        if identity is None:
            if audits:
                raise ValueError('Hotel cleanup has audit evidence without its exact cloud identity.')
            return None
        if not isinstance(identity, dict) or not identity or not audits:
            raise ValueError('Hotel cleanup requires its retained read-only cloud identity audits.')
        if identity.get('run_id') != job['id'] or identity.get('provider') != self.plan.provider:
            raise ValueError('Hotel cleanup cloud identity belongs to another run or provider.')
        for value in audits:
            audited = support._audit(value, pin)
            if canonical(audited['cloud_identity']) != canonical(identity):
                raise ValueError('Hotel cleanup cloud create identities drifted across retained audits.')
        if self.plan.provider in ('azure', 'gcp'):
            from scripts import deathstarbench_candidate_cloud_audit as audit
            if (identity.get('revision') != audit.REVISION
                    or identity.get('run_id') != job['id'] or identity.get('provider') != self.plan.provider
                    or identity.get('plan_sha256') != audit._sha(job['plan'])
                    or not isinstance(identity.get('account'), dict)
                    or not isinstance(identity.get('resources'), list)):
                raise ValueError('Hotel cleanup audit belongs to a different run/provider/plan.')
        return copy.deepcopy(identity)

    def _require_gcp_audited_graph(self, resources, expected, *, complete):
        """Bind mutable GCP deletion input to independent audited numeric IDs."""
        from scripts import deathstarbench_candidate_cloud_audit as audit
        rows = expected['resources']
        if (not all(isinstance(row, dict) and set(row) == {'prefix', 'id', 'self_link'} for row in rows)
                or len({row['prefix'] for row in rows}) != len(rows)
                or {row['prefix'] for row in rows} != set(gcp.DISTRIBUTED_RESOURCE_PREFIXES)):
            raise ValueError('Hotel GCP cleanup audit has a different exact resource graph.')
        for row in rows:
            prefix = row['prefix']
            identity_key, link_key = prefix + '_id', prefix + '_self_link'
            if complete or identity_key in resources:
                if audit._numeric_id(resources.get(identity_key), 'Hotel GCP cleanup numeric identity missing') != row['id']:
                    raise ValueError('Hotel GCP cleanup resource numeric identity drifted from its audit.')
            if complete or link_key in resources:
                if not gcp._scoped_link_equal(resources.get(link_key), row['self_link']):
                    raise ValueError('Hotel GCP cleanup resource self-link drifted from its audit.')

    def _guard(self, job, lease):
        require_acquired_run_lease(lease, job['id'])
        saved = self.plan.model_dump(exclude={'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'})
        if canonical(saved) != canonical(job['plan']):
            raise ValueError('Infrastructure plan changed at the provider boundary.')
        if canonical(self.pin) != canonical(job['candidate_support_pin']['provider_pin']):
            raise ValueError('Provider selector changed at the provider boundary.')
        if self.plan.provider in ('azure', 'gcp'):
            from scripts.deathstarbench_candidate_cloud_audit import require_known_candidate_resource_keys
            require_known_candidate_resource_keys(job)

    def clients(self):
        if self.plan.provider == 'oci' and self._clients is None:
            from argparse import Namespace
            self._clients = legacy_pins._oci_clients(
                Namespace(oci_config_file=self.oci_config_file, oci_profile=self.pin['profile']), self.pin,
            )
        return self._clients

    def provision(self, job, lease):
        self._guard(job, lease)
        common = {'persist': self.persist, 'emit': self.emit}
        startup = self.operator_journal_key == 'hotel_startup_operator'
        if startup:
            store = self._startup_store(job, lease)
            checkpoint = store.checkpoint(job['id'], lease)
            _startup_require(not checkpoint['contract_recorded'] and not job['resources'], 'Startup provision cannot resume or adopt resources.')
            def resolution(value, **transition):
                store.save_cleanup_progress(value, lease, transition=transition)
                self.persist(value)
            if self.plan.provider in ('aws', 'gcp'):
                common.update(strict_create_identity=True, persist_create_resolution=resolution)
            elif self.plan.provider == 'oci':
                common['ownership_snapshot'] = 'startup-v1'
        if self.plan.provider == 'aws':
            aws.provision_distributed_deathstarbench_candidate(
                job, self.plan, public_key=job['_public_key'],
                support_image=self.pin['support_image'], application_image=self.pin['application_image'], **common,
            )
            aws.validate_distributed_deathstarbench_candidate(job, persist=self.persist, strict_cleanup=startup)
        elif self.plan.provider == 'oci':
            oci.provision_distributed_deathstarbench_candidate(
                job, inputs=legacy_pins._oci_inputs(self.pin, {'public_key': job['_public_key']}),
                clients=self.clients(), **common,
            )
            oci.publish_distributed_runtime_projection(job, clients=self.clients(), persist=self.persist,
                ownership_snapshot='startup-v1' if startup else 'legacy')
        else:
            provider = azure if self.plan.provider == 'azure' else gcp
            provider.provision_distributed_deathstarbench_candidate(
                job, self.plan, public_key=job['_public_key'], **common,
            )
        self._guard(job, lease)

    def audit(self, job, lease):
        self._guard(job, lease)
        from scripts import deathstarbench_candidate_cloud_audit as audit
        startup = self.operator_journal_key == 'hotel_startup_operator'
        if self.plan.provider == 'azure':
            receipt = audit.audit_azure_ready(job, self.plan)
            return self._startup_audit(job, lease, receipt) if startup else receipt
        if self.plan.provider == 'gcp':
            receipt = audit.audit_gcp_ready(job, self.plan)
            return self._startup_audit(job, lease, receipt) if startup else receipt
        if self.plan.provider == 'aws':
            # Existing AWS SDK audit also publishes local aliases. Isolate that
            # publication and require the real persisted aliases already match.
            probe = {**job, 'resources': copy.deepcopy(job['resources'])}
            projection = aws.validate_distributed_deathstarbench_candidate(probe, strict_cleanup=startup)
            if canonical(probe['resources']) != canonical(job['resources']):
                raise ValueError('AWS audit would adopt or repair persisted identities/aliases.')
            state = aws._dsb_load(probe)
            identity = {
                'run_id': job['id'], 'provider': 'aws', 'account_id': state['contract']['account_id'],
                'contract': state['contract'], 'projection': projection,
                'resource_ids': {key: entry['id'] for key, entry in state['graph'].items()},
            }
        else:
            mode = 'startup-v1' if startup else 'legacy'
            contract = oci.validate_distributed_deathstarbench_candidate(job, clients=self.clients(), require_ready=True, ownership_snapshot=mode)
            projection = oci.distributed_runtime_projection(job, clients=self.clients(), ownership_snapshot=mode)
            identity = {
                'run_id': job['id'], 'provider': 'oci', 'inputs': contract['inputs'],
                'projection': projection,
                'resource_ids': {key: entry['id'] for key, entry in contract['entries'].items()},
            }
        self._guard(job, lease)
        receipt = {
            'schema_version': 1, 'candidate_schema': 'candidate-support-cloud-ready-v1',
            'candidate_only': True, 'read_only': True, 'run_id': job['id'],
            'provider': self.plan.provider, 'cloud_identity': identity,
            'cloud_identity_sha256': fingerprint(identity),
        }
        return self._startup_audit(job, lease, receipt) if startup else receipt

    def _startup_audit(self, job, lease, receipt):
        """Validate the fresh native body, enrich known facts before Task 5 records it.

        The caller next records this receipt and saves once; no previous audit
        snapshot is rewritten, and no extra cloud mutation/callback is needed.
        """
        from app import deathstarbench_hotel_startup_ownership as ownership
        store = self._startup_store(job, lease)
        previous = store.checkpoint(job['id'], lease)
        _startup_require(previous['seal_fingerprint'] is None)
        projection = project_startup_provider_ownership(job, previous)
        if self.plan.provider in ('gcp', 'azure'):
            resources, inventory, names = _startup_flat_resources(job, previous)
            projection['known_identities'].update(_startup_flat_audit(job, resources, inventory, names,
                projection['provider_scope'], {**previous['known_identities'], **projection['known_identities']}, receipt['cloud_identity']))
        enriched = ownership.update_hotel_startup_ownership_checkpoint(previous, projection)
        # Validate the prospective envelope through the same native save gate.
        probe = {**job, ownership.CHECKPOINT_KEY: ownership.record_hotel_startup_ownership_audit(enriched, receipt)}
        project_startup_provider_ownership(probe, previous)
        job[ownership.CHECKPOINT_KEY] = enriched
        return receipt

    def reconcile_aws_capacity_rejection(self, job, lease, *, event_id):
        """Explicit cleanup-only bridge; never part of default destroy/resume."""
        self._guard(job, lease)
        if self.plan.provider != 'aws':
            raise ValueError('Capacity-rejection reconciliation is AWS-only.')
        result = aws.reconcile_distributed_capacity_rejection_for_cleanup(
            job, lease=lease, event_id=event_id, persist=self.persist,
        )
        self._guard(job, lease)
        return result

    def destroy_and_prove(self, job, lease):
        self._guard(job, lease)
        if self.operator_journal_key == 'hotel_startup_operator':
            return self._destroy_startup(job, lease)
        selected_identity = self._cleanup_identity(job)
        from scripts import deathstarbench_candidate_cloud_audit as audit
        source = job.get(CLEANUP_SOURCE_KEY)
        if source is None:
            if (self.operator_journal_key == 'hotel_initialization_operator'
                    and self.plan.provider == 'gcp' and selected_identity is not None):
                self._require_gcp_audited_graph(job['resources'], selected_identity, complete=True)
            value = {
                'schema_version': 1, 'candidate_schema': 'candidate-support-cleanup-source-v1',
                'run_id': job['id'], 'provider': self.plan.provider,
                'pin_fingerprint': job['candidate_support_pin']['fingerprint'],
                'plan_sha256': fingerprint(job['plan']), 'resources': copy.deepcopy(job['resources']),
                'cloud_identity': selected_identity,
            }
            source = {**value, 'fingerprint': fingerprint(value)}
            job[CLEANUP_SOURCE_KEY] = copy.deepcopy(source)
            # Provider cleanup may durably clear its entire graph. Keep its
            # exact original GET targets before the first delete, so a crash
            # after graph clearing cannot strand independent absence checks.
            self.persist(job)
            self._guard(job, lease)
            if canonical(job.get(CLEANUP_SOURCE_KEY)) != canonical(source):
                raise ValueError('Cleanup source changed during durable publication.')
        required = {'schema_version', 'candidate_schema', 'run_id', 'provider', 'pin_fingerprint',
                    'plan_sha256', 'resources', 'cloud_identity', 'fingerprint'}
        if (not isinstance(source, dict) or set(source) != required
                or type(source['schema_version']) is not int or source['schema_version'] != 1
                or source['candidate_schema'] != 'candidate-support-cleanup-source-v1'
                or source['run_id'] != job['id'] or source['provider'] != self.plan.provider
                or source['pin_fingerprint'] != job['candidate_support_pin']['fingerprint']
                or source['plan_sha256'] != fingerprint(job['plan'])
                or not isinstance(source['resources'], dict)
                or source['fingerprint'] != fingerprint({key: source[key] for key in source if key != 'fingerprint'})):
            raise ValueError('Retained cleanup source differs from its exact run/provider pin.')
        if self.operator_journal_key == 'hotel_initialization_operator':
            if canonical(self._cleanup_identity(job)) != canonical(selected_identity):
                raise ValueError('Hotel audited cloud create identity changed during cleanup source publication.')
            if canonical(source['cloud_identity']) != canonical(selected_identity):
                raise ValueError('Retained Hotel cleanup source crossed its audited cloud create identity.')
            if self.plan.provider == 'gcp' and selected_identity is not None:
                self._require_gcp_audited_graph(source['resources'], selected_identity, complete=True)
                self._require_gcp_audited_graph(job['resources'], selected_identity, complete=False)
        original = {**job, 'resources': copy.deepcopy(source['resources'])}
        common = {'persist': self.persist, 'emit': self.emit}
        provider = self.plan.provider
        if provider == 'aws':
            aws.destroy_distributed_deathstarbench_candidate(job, **common)
            aws.recover_distributed_deathstarbench_candidate(job, persist=self.persist, wait_for_attachments=False)
            complete = aws.distributed_deathstarbench_candidate_deleted(job)
            absence = {'retained_tombstones_reconciled': True}
        elif provider == 'oci':
            oci.destroy_distributed_deathstarbench_candidate(job, clients=self.clients(), **common)
            oci.attest_distributed_candidate_terminal_deletion(job, clients=self.clients())
            complete = oci.distributed_candidate_is_deleted(job)
            absence = {'retained_tombstones_reconciled': True}
        else:
            if provider == 'azure':
                expected = source['cloud_identity']
                if expected is not None:
                    audit.require_azure_cleanup_identity(original, expected)
                azure.destroy_resources(job, **common)
                absence = audit.audit_azure_absent(original)
            else:
                gcp.destroy_resources(job, **common)
                absence = audit.audit_gcp_absent(original)
            complete = application._cleared_distributed_provider_contract(job, provider)
        self._guard(job, lease)
        if not complete:
            raise ValueError('Provider cleanup lacks its strict terminal deletion contract.')
        return {
            'schema_version': 1, 'candidate_schema': 'candidate-support-cleanup-proof-v1',
            'candidate_only': True, 'run_id': job['id'], 'provider': provider,
            'cleanup_complete': True, 'independent_absence_proved': True,
            'provider_absence': absence,
        }

    def _startup_store(self, job, lease):
        from scripts.deathstarbench_hotel_startup_operator_state import HotelStartupStateStore
        store = HotelStartupStateStore(lease.path.parent.parent)
        store._check_job(job, lease)
        return store

    def _destroy_startup(self, job, lease):
        from app import deathstarbench_hotel_startup_ownership as ownership
        from scripts.deathstarbench_hotel_startup_operator_state import OWNERSHIP_KEY
        store = self._startup_store(job, lease)
        # Never consult a journal, stale live graph, source or executable pin.
        current = store.load_for_destroy(job['id'], lease)
        job['resources'] = current['resources']
        job[ownership.CHECKPOINT_KEY] = current[ownership.CHECKPOINT_KEY]
        checkpoint = job[ownership.CHECKPOINT_KEY]
        if not checkpoint['contract_recorded']:
            return ownership.make_hotel_startup_cleanup_not_required(job,
                support_pin=job['candidate_support_pin'], initialization_pin=job['hotel_initialization_pin'],
                startup_pin=job['hotel_startup_pin'])
        _startup_require(not checkpoint['unresolved_creates'], 'Unresolved original create identity blocks strict cleanup.')
        from scripts import deathstarbench_candidate_cloud_audit as audit
        source = current.get(OWNERSHIP_KEY)
        if source is None:
            implicit = None
            if self.plan.provider == 'aws':
                boundary = aws.prepare_distributed_candidate_cleanup(job)
                implicit = boundary['implicit']
            elif self.plan.provider == 'oci':
                contract = oci.prepare_distributed_candidate_cleanup(job, clients=self.clients())
                job['resources'][oci.CONTRACT_KEY] = contract
                oci.persist_role_node_inventory(job['resources'], oci._inventory(contract))
            elif self.plan.provider == 'gcp':
                gcp.prepare_distributed_candidate_cleanup(job)
            else:
                audit._azure_scope(job['resources'], self.plan)
                azure.prepare_distributed_candidate_cleanup(job)
                for envelope in checkpoint['audits']:
                    audit.require_azure_cleanup_identity(job, envelope['cloud_identity'])
            store.save_cleanup_progress(job, lease, finalizing=True, verified_implicit=implicit)
            source = ownership.make_hotel_startup_cleanup_ownership(job,
                support_pin=job['candidate_support_pin'], initialization_pin=job['hotel_initialization_pin'],
                startup_pin=job['hotel_startup_pin'])
            store.publish_cleanup_ownership(job['id'], source, lease)
            job[ownership.CHECKPOINT_KEY] = store.checkpoint(job['id'], lease)
        # Readback and directory fsync finished before entering the real engine.
        source = store.cleanup_ownership(job['id'], lease)
        persist = lambda value: store.save_cleanup_progress(value, lease)
        original = {**job, 'resources': copy.deepcopy(source['resources'])}
        if self.plan.provider == 'aws':
            boundary = self._startup_aws_boundary(source)
            aws.destroy_distributed_deathstarbench_candidate(job, persist=persist, emit=self.emit,
                strict_cleanup=True, expected_cleanup_identity=boundary)
            aws.recover_distributed_deathstarbench_candidate(job, persist=persist, wait_for_attachments=False,
                allow_quiesced_load_generator=True, strict_cleanup=True, expected_cleanup_identity=boundary)
            absence = aws.attest_startup_candidate_absence(job, boundary)
            complete = aws.distributed_deathstarbench_candidate_deleted(job)
        elif self.plan.provider == 'oci':
            oci.destroy_distributed_deathstarbench_candidate(job, clients=self.clients(), persist=persist, emit=self.emit,
                strict_cleanup=True, ownership_snapshot='startup-v1')
            oci.attest_distributed_candidate_terminal_deletion(job, clients=self.clients())
            absence = {'retained_explicit_and_implicit_terminal_attestation': True}
            complete = oci.distributed_candidate_is_deleted(job)
        else:
            complete = application._cleared_distributed_provider_contract(job, self.plan.provider)
            if not complete:
                if self.plan.provider == 'gcp':
                    gcp.destroy_resources(job, persist=persist, emit=self.emit, strict_cleanup=True)
                else:
                    audit._azure_scope(original['resources'], self.plan)
                    azure.prepare_distributed_candidate_cleanup(job)
                    for envelope in source['audits']:
                        audit.require_azure_cleanup_identity(original, envelope['cloud_identity'])
                    azure.destroy_resources(job, persist=persist, emit=self.emit)
                complete = application._cleared_distributed_provider_contract(job, self.plan.provider)
            absence = audit.audit_gcp_absent(original) if self.plan.provider == 'gcp' else audit.audit_azure_absent(original)
        _startup_require(complete, 'Strict provider terminal proof is incomplete.')
        self._guard(job, lease)
        return {'schema_version': 1, 'candidate_schema': 'candidate-support-cleanup-proof-v1',
                'candidate_only': True, 'run_id': job['id'], 'provider': self.plan.provider,
                'cleanup_complete': True, 'independent_absence_proved': True, 'provider_absence': absence}

    @staticmethod
    def _startup_aws_boundary(source):
        graph = source['resources'][aws.AWS_DSB_GRAPH_KEY]['graph']
        return {'explicit': {key: entry.get('id') for key, entry in graph.items()},
                'implicit': {key.removeprefix('aws-'): {'kind': row['kind'].removeprefix('aws_'),
                    'id': row['id'], 'parent_id': row['parent_id']}
                    for key, row in source['known_identities'].items() if key.removeprefix('aws-') not in graph}}
