"""Fresh, read-only infrastructure proofs for the private candidate operator.

These adapters intentionally reuse the *Social Network infrastructure-only*
plan; that plan is never a request to install or execute Social Network. Hotel
and Media identity stays in the private operator's separate candidate plan.
Only GET/list APIs are used here. Receipts are observations, not authorization
to start applications, initialize datasets, or promote any release gate.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from app.deathstarbench_contract import (
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    K3S_RUNTIME_ID,
)
from app.deathstarbench_topology import build_topology_manifest
from app.models import BenchmarkPlan
from app.providers import azure, gcp


REVISION = 'candidate-cloud-audit-v1'
_SSH_FIELDS = {'ssh_private_key', 'ssh_public_key', 'ssh_key_passphrase'}


def _known_provider_keys():
    """Finite writer schemas, not a startswith authorization rule.

    These cover only the distributed candidate writers and their failure/
    cleanup paths, not the provider's compact/peer/general-volume contracts.
    A new writer field deliberately requires review before this operator can
    let a provider's broad final metadata clear erase it.
    """
    azure_keys = {
        'azure_subscription_id', 'azure_subscription_name', 'azure_tenant_id',
        'azure_zone', 'azure_vm_size', 'azure_architecture', 'azure_image_id',
        'azure_image_urn', 'azure_image_version', 'azure_web_benchmarks',
        'azure_distributed_candidate', 'azure_database_disk_type',
        'azure_database_disk_size_gb', 'azure_database_disk_iops',
        'azure_database_disk_throughput_mibps', 'azure_database_disk_device',
        'azure_resource_group_name', 'azure_resource_group_expected_id',
        'azure_resource_group_id', 'azure_resource_group_tags',
        'azure_resource_group_create_ambiguous', 'azure_resource_group_delete_ambiguous',
    }
    direct = {
        'azure_dsb_cluster_nsg', 'azure_dsb_loadgen_nsg', 'azure_dsb_vnet',
        'azure_dsb_cluster_nat_public_ip', 'azure_dsb_cluster_nat_gateway',
        'azure_dsb_cluster_subnet', 'azure_dsb_loadgen_subnet', 'azure_dsb_database_data_disk',
    }
    for role in azure.DISTRIBUTED_PRIVATE_ADDRESSES:
        normalized = role.replace('-', '_')
        prefix = f'azure_dsb_{normalized}'
        azure_keys.update(prefix + '_' + field for field in ('architecture', 'vm_size', 'private_ip', 'public_ip'))
        direct.update((prefix + '_instance', prefix + '_instance_nic'))
        if role in azure.DISTRIBUTED_PUBLIC_IP_NODES:
            direct.add(prefix + '_instance_public_ip')
        # OS disks are created by the VM. The writer records only these three
        # ownership/response-loss fields, not arbitrary child lifecycle fields.
        azure_keys.update(prefix + '_instance_os_disk_' + field
                          for field in ('name', 'expected_id', 'create_ambiguous'))
    azure_keys.update(prefix + '_' + field for prefix in direct for field in (
        'name', 'expected_id', 'id', 'spec_fingerprint', 'create_ambiguous'))
    azure_keys.update(f'azure_dsb_{kind}_image_{field}' for kind in ('application', 'support')
                      for field in ('id', 'urn', 'version', 'architecture'))

    gcp_keys = {
        'gcp_project_id', 'gcp_compute_project_id', 'gcp_resource_prefix', 'gcp_zone',
        'gcp_machine_type', 'gcp_image_family', 'gcp_image_self_link',
        'gcp_web_benchmarks', 'gcp_distributed_candidate', 'gcp_dsb_expected_names',
        'gcp_dsb_network_cidr', 'gcp_dsb_cluster_subnet_cidr', 'gcp_dsb_loadgen_subnet_cidr',
        'gcp_dsb_database_disk_device', 'gcp_dsb_router_nat_configured',
        'gcp_dsb_router_nat_request_id', 'gcp_dsb_router_nat_update_ambiguous',
    }
    # _create_named_resource, _persist_recovered_identity and _delete_resource
    # use this exact finite lifecycle set for the twenty distributed objects.
    gcp_keys.update(prefix + '_' + field for prefix in gcp.DISTRIBUTED_RESOURCE_PREFIXES
                    for field in ('name', 'id', 'self_link', 'request_id', 'create_ambiguous',
                                  'reconciliation_error', 'delete_request_id'))
    for role in gcp.DISTRIBUTED_PRIVATE_ADDRESSES:
        prefix = f'gcp_dsb_{role.replace("-", "_")}'
        gcp_keys.update(prefix + '_' + field for field in (
            'machine_type', 'architecture', 'private_ip', 'public_ip', 'tag'))
        gcp_keys.update(prefix + '_instance_' + field for field in ('private_ip', 'public_ip'))
    gcp_keys.update(prefix + '_' + field for prefix in (
        'gcp_dsb_database_data_disk', *gcp.DISTRIBUTED_BOOT_PREFIXES.values())
        for field in ('type', 'size_gb', 'provisioned_iops', 'provisioned_throughput_mibps'))
    gcp_keys.update(f'gcp_dsb_{kind}_image_{field}' for kind in ('application', 'support')
                    for field in ('id', 'name', 'self_link', 'family', 'architecture'))
    return frozenset(azure_keys), frozenset(gcp_keys)


_AZURE_PROVIDER_KEYS, _GCP_PROVIDER_KEYS = _known_provider_keys()


def require_known_candidate_resource_keys(job) -> None:
    """Refuse unknown Azure/GCP metadata before any SDK call or broad clear.

    Missing keys are allowed for partial provision/cleanup. Non-provider keys
    are neither erased nor approved here; the strict terminal predicate still
    rejects unknown recovery data. Cross-provider cloud keys are also refused.
    """
    _require(isinstance(job, Mapping) and isinstance(job.get('resources'), Mapping),
             'candidate resource mapping missing')
    plan = job.get('plan')
    provider = plan.get('provider') if isinstance(plan, Mapping) else getattr(plan, 'provider', None)
    if provider not in {'azure', 'gcp'}:
        return
    allowed = _AZURE_PROVIDER_KEYS if provider == 'azure' else _GCP_PROVIDER_KEYS
    _require(all(isinstance(key, str) for key in job['resources']), 'resource field names must be strings')
    unknown = sorted(key for key in job['resources'] if key.startswith(('azure_', 'gcp_')) and key not in allowed)
    _require(not unknown, 'unknown provider resource metadata is not owned by this candidate: ' + ', '.join(unknown))


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode('utf-8')).hexdigest()


def _require(condition: Any, message: str):
    if not condition:
        raise ValueError('Candidate cloud audit refused: ' + message)


def _text(value: Any, label: str) -> str:
    _require(isinstance(value, str) and value and value == value.strip(), label)
    return value


def _numeric_id(value: Any, label: str) -> str:
    text = str(value) if isinstance(value, int) and not isinstance(value, bool) else value
    _require(isinstance(text, str) and re.fullmatch(r'[1-9][0-9]*', text), label)
    return text


def _uuid(value: Any, label: str) -> str:
    text = _text(value, label)
    try:
        parsed = UUID(text)
    except ValueError as exc:
        raise ValueError('Candidate cloud audit refused: ' + label) from exc
    _require(str(parsed) == text.lower() and parsed.int != 0, label)
    return str(parsed)


def _source(job: Mapping[str, Any], provider: str, plan: BenchmarkPlan | None = None):
    _require(isinstance(job, Mapping), 'job must be an object')
    require_known_candidate_resource_keys(job)
    run_id = job.get('id')
    _require(isinstance(run_id, str) and re.fullmatch(r'[0-9a-f]{12}', run_id)
             and run_id != '000000000000', 'invalid run ID')
    saved = job.get('plan')
    _require(isinstance(saved, Mapping), 'missing original persisted infrastructure plan')
    if plan is None:
        # Required model fields are excluded from all identity comparisons.
        # Placeholders are not credentials and cannot be passed to a guest.
        plan = BenchmarkPlan.model_validate({**saved, 'ssh_private_key': 'audit-only',
                                            'ssh_public_key': 'audit-only'})
    _require(isinstance(plan, BenchmarkPlan), 'expected a BenchmarkPlan')
    _require(_canonical(plan.model_dump(exclude=_SSH_FIELDS)) == _canonical(saved),
             'supplied plan differs from the exact saved plan')
    _require(plan.provider == provider and plan.benchmarks == ['deathstarbench']
             and not plan.llm_benchmarks
             and plan.deathstarbench.workload == 'social_network'
             and plan.deathstarbench.topology_id == DISTRIBUTED_TIERED_TOPOLOGY_ID
             and plan.deathstarbench.runtime_id == K3S_RUNTIME_ID,
             'not the exact infrastructure-only five-role plan')
    resources = job.get('resources')
    _require(isinstance(resources, Mapping) and resources.get('provider') == provider
             and resources.get(f'{provider}_distributed_candidate') is True,
             'missing provider-bound candidate ownership contract')
    _require(not job.get('results') and not resources.get(DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY)
             and not resources.get(DEATHSTARBENCH_EXECUTION_JOURNAL_KEY),
             'a workload/execution journal or results cannot be adopted as infrastructure-only')
    return plan, resources


def _receipt(job, plan, manifest, *, state: str, account: dict, **observed):
    value = {
        'schema_version': 1, 'candidate_only': True,
        'revision': REVISION, 'provider': plan.provider, 'run_id': job['id'],
        'state': state, 'read_only': True,
        'plan_sha256': _sha(plan.model_dump(exclude=_SSH_FIELDS)),
        'topology_fingerprint': manifest.fingerprint,
        'account': account,
        'applications_qualified': False, 'dataset_qualified': False,
        'measurement_qualified': False, 'public_ready': False,
        **observed,
    }
    if state == 'ready':
        identity = {key: value[key] for key in (
            'revision', 'provider', 'run_id', 'plan_sha256',
            'topology_fingerprint', 'account', 'roles', 'images', 'resources', 'profiles')}
        value.update(cloud_identity=identity, cloud_identity_sha256=_sha(identity))
    return {**value, 'observation_sha256': _sha(value)}


def _azure_scope(resources, plan, *, credential=None, clients=None):
    subscription = _uuid(resources.get('azure_subscription_id'), 'invalid saved subscription ID')
    _require(plan.azure_subscription_id is not None
             and plan.azure_subscription_id.lower() == subscription,
             'subscription differs from original plan')
    injected = clients is not None
    resolved, account, clients = azure._runtime(subscription, credential, clients)
    # _resolve_subscription has a permissive fallback for empty account lists;
    # an audit must establish a real accessible, enabled tenant instead.
    tenant = _text(azure._value(account, 'tenant_id', azure._value(account, 'tenantId')),
                   'accessible Azure tenant evidence missing')
    _require(resolved.lower() == subscription
             and str(azure._value(account, 'state', '')).casefold() == 'enabled'
             and tenant == resources.get('azure_tenant_id'),
             'Azure subscription/tenant is inaccessible, disabled, or changed')
    # CLI account list is cached. Independently GET the current subscription
    # before interpreting any child 404 as absence. The installed legacy
    # subscription model drops tenantId; request the documented 2022 response
    # through the SDK and retain only its decoded public metadata.
    # https://learn.microsoft.com/rest/api/resources/subscriptions/get
    service = azure._subscription_operations(clients.get('subscription_audit') or clients.get('subscriptions'))
    owned_client = None
    if not callable(getattr(service, 'get', None)):
        _require(not injected, 'fresh Azure subscription GET client missing')
        from azure.identity import AzureCliCredential
        from azure.mgmt.subscription import SubscriptionClient
        owned_client = SubscriptionClient(clients.get('credential') or credential or
                                         AzureCliCredential(subscription=subscription))
        service = owned_client.subscriptions
    try:
        live = service.get(subscription, api_version='2022-12-01',
                           cls=lambda response, *_: json.loads(response.http_response.text()))
    finally:
        if owned_client is not None:
            owned_client.close()
    _require(azure._account_id(live).lower() == subscription
             and str(azure._value(live, 'state', '')).casefold() == 'enabled'
             and azure._value(live, 'tenant_id', azure._value(live, 'tenantId')) == tenant,
             'fresh Azure subscription/tenant identity changed or is disabled')
    return clients, {'subscription_id': subscription, 'tenant_id': tenant}


def _azure_contract(job, plan, resources):
    contract = azure._load_topology_inventory_contract(resources)
    _require(contract is not None, 'Azure topology contract missing')
    manifest, inventory = contract
    _require(manifest.topology_id == DISTRIBUTED_TIERED_TOPOLOGY_ID
             and manifest.runtime_id == K3S_RUNTIME_ID,
             'Azure is not the five-role K3s topology')
    expected = build_topology_manifest(
        plan.deathstarbench.topology_id, plan.deathstarbench.runtime_id,
        selected_shape=plan.shape, selected_architecture=resources.get('architecture'))
    _require(expected.fingerprint == manifest.fingerprint,
             'Azure manifest differs from the original plan')
    group = _text(resources.get('azure_resource_group_name'), 'missing Azure group')
    _require(group == azure._job_name(job['id']), 'Azure group differs from deterministic run name')
    _require(resources.get('region') == plan.region
             and resources.get('azure_zone') == plan.azure_zone
             and resources.get('availability_zone') == plan.azure_zone,
             'Azure placement differs from original plan')
    expected_group_id = azure._expected_id(resources['azure_subscription_id'], group)
    _require(azure._normalize_id(resources.get('azure_resource_group_expected_id'))
             == azure._normalize_id(expected_group_id)
             and resources.get('azure_resource_group_tags') == azure._tags(job['id'], 'resource-group'),
             'Azure original group ownership anchor changed')
    specs = azure._active_managed_identity_specs(
        resources, subscription_id=resources['azure_subscription_id'],
        resource_group=group, manifest=manifest)
    return manifest, inventory, group, specs


def _az(value, *path):
    for key in path:
        found = azure._observed_field(value, key)
        if found is azure._MISSING and key == 'properties':
            continue  # Azure Python SDK flattens wire-level properties.
        if found is azure._MISSING:
            return None
        value = found
    return value


def _az_match(actual, expected, label):
    _require(azure._matches_observed_spec(actual, expected), f'Azure {label} spec changed')
    # Python equality equates 0/1 with False/True; require actual API booleans.
    def booleans(value, contract):
        if isinstance(contract, Mapping):
            return all(booleans(_az(value, key), item) for key, item in contract.items())
        if isinstance(contract, list):
            items = list(value or ())
            if contract and all(isinstance(item, Mapping) and 'name' in item for item in contract):
                values = {_az(item, 'name'): item for item in items}
                return all(booleans(values.get(item['name']), item) for item in contract)
            return all(booleans(a, b) for a, b in zip(items, contract))
        return type(value) is bool and value is contract if type(contract) is bool else True
    _require(booleans(actual, expected), f'Azure {label} boolean fields changed')


def _azure_get(clients, group, prefix, name, vnet):
    if prefix.endswith('_subnet'):
        return azure._get_or_none(azure._network_service(clients, 'subnets'), group, vnet, name)
    kind = ('virtual_machines' if prefix.endswith('_instance') else
            'disks' if prefix.endswith('_disk') else
            'network_interfaces' if prefix.endswith('_nic') else
            'network_security_groups' if prefix.endswith('_nsg') else
            'virtual_networks' if prefix.endswith('_vnet') else
            'nat_gateways' if prefix.endswith('_gateway') else 'public_ip_addresses')
    service = (azure._compute_service(clients, kind) if kind in {'virtual_machines', 'disks'}
               else azure._network_service(clients, kind))
    _require(service is not None, f'Azure read client {kind} unavailable')
    if kind == 'virtual_machines':
        # A plain VM GET reports provisioning, not whether its guest is on.
        return service.get(group, name, expand='instanceView')
    return azure._get_or_none(service, group, name)


def _check_addresses(resources, node, role, provider, public, private):
    _require(private == node.private_addresses[0]
             and tuple(node.public_addresses) == ((public,) if public else ()),
             f'{provider} {role} live addresses differ from role inventory')
    normalized = role.replace('-', '_')
    for field, value in (('private_ip', private), ('public_ip', public)):
        _require(resources.get(f'{provider}_dsb_{normalized}_{field}') == value,
                 f'{provider} {role} saved address alias differs')
    if role in {'application', 'load-generator'}:
        prefix = '' if role == 'application' else 'loadgen_'
        for field, value in (('private_ip', private), ('public_ip', public),
                             ('instance_id', node.provider_resource_id)):
            _require(str(resources.get(prefix + field) or '') == str(value or ''),
                     f'{provider} canonical {role} alias differs')
    for address in (private, public):
        if address is not None:
            _require(isinstance(ipaddress.ip_address(address), ipaddress.IPv4Address),
                     f'{provider} non-IPv4 address')


def audit_azure_ready(job, plan, *, credential=None, clients=None) -> dict:
    """Audit the complete owned five-role Azure graph without changing it."""
    plan, resources = _source(job, 'azure', plan)
    manifest, inventory, group, specs = _azure_contract(job, plan, resources)
    clients, account = _azure_scope(resources, plan, credential=credential, clients=clients)
    subscription, region, zone = account['subscription_id'], plan.region, plan.azure_zone
    group_value = azure._get_or_none(azure._resource_groups(clients), group)
    _require(group_value is not None, 'Azure group absent')
    azure._verify_group(group_value, subscription_id=subscription, name=group,
                        region=region, required_tags=azure._tags(job['id'], 'resource-group'),
                        saved_id=resources.get('azure_resource_group_id', ''))
    _require(azure._provisioning_state(group_value) == 'succeeded', 'Azure group not ready')
    for key, (name, identity) in specs.items():
        _require(resources.get(key) == identity and resources.get(key.removesuffix('_expected_id') + '_name') == name,
                 f'Azure incomplete deterministic resource contract: {key}')
    azure._verify_resource_group_inventory(clients, resources, subscription_id=subscription,
                                           resource_group=group, manifest=manifest)
    listed = list(azure._resource_inventory(clients).list_by_resource_group(group))
    expected_top = {azure._normalize_id(identity) for key, (_, identity) in specs.items()
                    if not key.endswith('_subnet_expected_id')}
    listed_ids = [azure._normalize_id(azure._value(item, 'id')) for item in listed]
    _require(len(listed_ids) == len(set(listed_ids)) and set(listed_ids) == expected_top,
             'Azure top-level inventory incomplete or duplicated')
    found = {}
    for key, (name, identity) in specs.items():
        prefix = key.removesuffix('_expected_id')
        value = _azure_get(clients, group, prefix, name, resources['azure_dsb_vnet_name'])
        _require(value is not None and azure._normalize_id(_az(value, 'id')) == azure._normalize_id(identity)
                 and _az(value, 'name') == name
                 and azure._provisioning_state(value) == 'succeeded', f'Azure {prefix} missing, replaced, or not ready')
        found[prefix] = value
    listed_subnets = list(azure._network_service(clients, 'subnets').list(group, resources['azure_dsb_vnet_name']))
    subnet_ids = [azure._normalize_id(_az(value, 'id')) for value in listed_subnets]
    _require(len(subnet_ids) == 2 and set(subnet_ids) == {
        azure._normalize_id(resources[f'azure_dsb_{kind}_subnet_expected_id']) for kind in ('cluster', 'loadgen')},
        'Azure virtual network has unexpected or missing child subnets')
    _require(not _az(found['azure_dsb_vnet'], 'properties', 'virtualNetworkPeerings'),
             'Azure virtual network has unexpected peerings')
    application = azure._find_size(subscription, region, zone, plan.shape, clients)
    azure._validate_capacity(plan, application)
    capacities = azure._distributed_role_details(manifest, subscription_id=subscription,
                region=region, zone=zone, application_details=application, clients=clients)
    images = {}
    for kind, architecture in (('application', application['architecture']), ('support', 'x86_64')):
        prefix = f'azure_dsb_{kind}_image'
        _require(all(resources.get(f'{prefix}_{field}') for field in ('version', 'urn', 'id', 'architecture')),
                 'Azure exact immutable image pin missing')
        images[kind] = azure._distributed_candidate_image(resources, key_prefix=prefix,
                      subscription_id=subscription, region=region, architecture=architecture, clients=clients)
    expected = {}
    def body(prefix, role, properties, **extra):
        expected[prefix] = {'location': region, 'tags': azure._tags(job['id'], role),
                            'properties': properties, **extra}
    def identity(prefix):
        return resources[prefix + '_expected_id']
    rules = azure._distributed_security_rules(manifest)
    for kind, role in (('cluster', 'cluster-network-security-group'), ('loadgen', 'loadgen-network-security-group')):
        body(f'azure_dsb_{kind}_nsg', role, {'securityRules': rules['load-generator' if kind == 'loadgen' else kind]})
    body('azure_dsb_vnet', 'distributed-virtual-network',
         {'addressSpace': {'addressPrefixes': [azure.DISTRIBUTED_VNET_CIDR]}})
    pip_properties = {'publicIPAllocationMethod': 'Static', 'publicIPAddressVersion': 'IPv4'}
    zonal = {'zones': [zone]} if zone else {}
    body('azure_dsb_cluster_nat_public_ip', 'cluster-nat-public-ip', pip_properties,
         sku={'name': 'Standard', 'tier': 'Regional'}, **zonal)
    body('azure_dsb_cluster_nat_gateway', 'cluster-nat-gateway',
         {'idleTimeoutInMinutes': 4, 'publicIpAddresses': [{'id': identity('azure_dsb_cluster_nat_public_ip')}]},
         sku={'name': 'Standard'}, **zonal)
    for kind, cidr in (('cluster', azure.DISTRIBUTED_CLUSTER_SUBNET_CIDR), ('loadgen', azure.DISTRIBUTED_LOADGEN_SUBNET_CIDR)):
        properties = {'addressPrefix': cidr, 'networkSecurityGroup': {'id': identity(f'azure_dsb_{kind}_nsg')},
                      'privateEndpointNetworkPolicies': 'Enabled', 'defaultOutboundAccess': False}
        if kind == 'cluster':
            properties['natGateway'] = {'id': identity('azure_dsb_cluster_nat_gateway')}
        else:
            _require(not _az(found[f'azure_dsb_{kind}_subnet'], 'properties', 'natGateway'), 'Azure loadgen unexpected NAT')
        expected[f'azure_dsb_{kind}_subnet'] = {'properties': properties}
        _require(not any(_az(found[f'azure_dsb_{kind}_subnet'], 'properties', field)
                         for field in ('routeTable', 'serviceEndpoints', 'delegations')),
                 'Azure subnet has an unexpected routing/delegation relationship')
    storage = next(node for node in manifest.nodes if node.key == 'database').storage[0]
    body('azure_dsb_database_data_disk', 'database-data-disk',
         {'creationData': {'createOption': 'Empty'}, 'diskSizeGB': storage.size_gib,
          'diskIOPSReadWrite': storage.minimum_iops, 'diskMBpsReadWrite': storage.minimum_throughput_mibps,
          'networkAccessPolicy': 'DenyAll', 'publicNetworkAccess': 'Disabled'},
         sku={'name': azure.PREMIUM_V2_DISK_TYPE}, **zonal)
    roles, opaque = {}, {}
    for node in inventory.nodes:
        role, normalized = node.key, node.key.replace('-', '_')
        prefix = f'azure_dsb_{normalized}_instance'
        vm, nic = found[prefix], found[prefix + '_nic']
        details = capacities[role]
        for zonal_prefix in (prefix, prefix + '_os_disk') + ((prefix + '_public_ip',) if prefix + '_public_ip' in found else ()):
            _require(list(_az(found[zonal_prefix], 'zones') or ()) == ([zone] if zone else []),
                     'Azure live zonal placement changed')
        _require(node.lifecycle_status == 'running' and node.zone == zone
                 and node.architecture == details['architecture']
                 and resources.get(f'azure_dsb_{normalized}_architecture') == details['architecture']
                 and resources.get(f'azure_dsb_{normalized}_vm_size') == details['vm_size'],
                 f'Azure {role} role/capacity inventory changed')
        statuses = list(_az(vm, 'properties', 'instanceView', 'statuses') or ())
        power = [str(_az(status, 'code')) for status in statuses if str(_az(status, 'code')).startswith('PowerState/')]
        _require(power == ['PowerState/running'], f'Azure {role} is not running')
        opaque[prefix] = _uuid(_az(vm, 'properties', 'vmId'), f'Azure {role} missing unique vmId')
        image = images['application' if role == 'application' else 'support']
        data_id = identity('azure_dsb_database_data_disk') if role == 'database' else None
        expected[prefix] = azure._vm_body(region=region, zone=zone, name=node.provider_resource_name,
            role=role, job_id=job['id'], vm_size=details['vm_size'], image=image,
            nic_id=identity(prefix + '_nic'), public_key=None,
            os_disk_size_gb=azure.DISTRIBUTED_OS_DISK_SIZE_GB, data_disk_id=data_id)
        pip = found.get(prefix + '_public_ip')
        if pip is not None:
            body(prefix + '_public_ip', role + '-public-ip', pip_properties,
                 sku={'name': 'Standard', 'tier': 'Regional'}, **zonal)
        ip_properties = {'privateIPAllocationMethod': 'Static', 'privateIPAddressVersion': 'IPv4',
                         'primary': True, 'privateIPAddress': azure.DISTRIBUTED_PRIVATE_ADDRESSES[role],
                         'subnet': {'id': identity('azure_dsb_' + ('loadgen' if role == 'load-generator' else 'cluster') + '_subnet')}}
        if pip is not None:
            ip_properties['publicIPAddress'] = {'id': identity(prefix + '_public_ip')}
        else:
            configs = list(_az(nic, 'properties', 'ipConfigurations') or ())
            _require(len(configs) == 1 and not _az(configs[0], 'properties', 'publicIPAddress'), 'Azure unexpected public access')
        body(prefix + '_nic', role + '-nic', {'enableAcceleratedNetworking': bool(details['accelerated_networking']),
             'enableIPForwarding': False, 'ipConfigurations': [{'name': 'primary', 'properties': ip_properties}]})
        _require(azure._normalize_id(_az(nic, 'properties', 'virtualMachine', 'id')) == azure._normalize_id(identity(prefix)),
                 'Azure NIC parent VM changed')
        _require(not _az(nic, 'properties', 'networkSecurityGroup'), 'Azure unexpected NIC-level NSG')
        public, private = azure._ip_values(nic, pip)
        _check_addresses(resources, node, role, 'azure', public, private)
        os_disk = found[prefix + '_os_disk']
        _az_match(os_disk, {'location': region, 'sku': {'name': 'Premium_LRS'}, 'properties': {
            'diskSizeGB': azure.DISTRIBUTED_OS_DISK_SIZE_GB, 'osType': 'Linux', 'diskState': 'Attached'}, **zonal}, role + ' OS disk')
        _require(azure._normalize_id(_az(vm, 'properties', 'storageProfile', 'osDisk', 'managedDisk', 'id'))
                 == azure._normalize_id(identity(prefix + '_os_disk')), 'Azure OS disk attachment changed')
        for disk_prefix in (prefix + '_os_disk',) + (('azure_dsb_database_data_disk',) if role == 'database' else ()):
            disk = found[disk_prefix]
            _require(azure._normalize_id(_az(disk, 'managedBy')) == azure._normalize_id(identity(prefix))
                     and _az(disk, 'properties', 'diskState') == 'Attached', 'Azure disk is not exclusively attached to its role')
            opaque[disk_prefix] = _uuid(_az(disk, 'properties', 'uniqueId'), 'Azure disk missing uniqueId')
        roles[role] = {'id': _az(vm, 'id'), 'name': node.provider_resource_name, 'unique_id': opaque[prefix],
                       'shape': details['vm_size'], 'architecture': details['architecture'],
                       'private_ip': private, 'public_ip': public, 'image_id': image['image_id']}
    for prefix, body_value in expected.items():
        _az_match(found[prefix], body_value, prefix)
    return _receipt(job, plan, manifest, state='ready', account=account, roles=roles,
        images={key: {'id': value['image_id'], 'urn': value['urn']} for key, value in images.items()},
        profiles={'roles': {role: {'shape': item['vm_size'], 'vcpus': item['vcpu'],
                                  'memory_gib': item['memory_gb'], 'architecture': item['architecture']}
                            for role, item in sorted(capacities.items())},
                  'database': {'type': azure.PREMIUM_V2_DISK_TYPE, 'size_gib': storage.size_gib,
                               'iops': storage.minimum_iops, 'throughput_mibps': storage.minimum_throughput_mibps},
                  'boot': {'type': 'Premium_LRS', 'size_gib': azure.DISTRIBUTED_OS_DISK_SIZE_GB}},
        resources=[{'prefix': prefix, 'id': _az(value, 'id'), 'unique_id': opaque.get(prefix)}
                   for prefix, value in sorted(found.items())])


def _gcp_scope(resources, plan, *, credentials=None, clients=None):
    project = _text(resources.get('gcp_project_id'), 'GCP project missing')
    _require(project == plan.gcp_project_id, 'GCP project differs from original plan')
    resolved, _, clients = gcp._runtime(project, credentials, clients)
    actual = gcp._call(clients['projects'], 'get', clients, 'GetProjectRequest', project=project)
    numeric = _numeric_id(gcp._value(actual, 'id'), 'GCP live Compute project ID missing')
    _require(resolved == project and numeric == resources.get('gcp_compute_project_id'),
             'GCP immutable Compute project ID changed')
    return clients, {'project_id': project, 'compute_project_id': numeric}


def _gcp_contract(job, plan, resources):
    manifest, inventory, names = gcp._load_distributed_cleanup_contract(job)
    expected = build_topology_manifest(plan.deathstarbench.topology_id, plan.deathstarbench.runtime_id,
                selected_shape=plan.shape, selected_architecture=resources.get('architecture'))
    _require(expected.fingerprint == manifest.fingerprint, 'GCP manifest differs from original plan')
    _require(resources.get('region') == plan.region and resources.get('gcp_zone') == plan.gcp_zone
             and resources.get('availability_zone') == plan.gcp_zone,
             'GCP placement differs from original plan')
    for planned_node in manifest.planned_inventory('gcp').nodes:
        node = inventory.node(planned_node.key)
        _require(node.role == planned_node.role and node.capacity_class == planned_node.capacity_class,
                 'GCP inventory role/capacity contract changed')
        planned_storage = {item.key: item for item in planned_node.storage}
        actual_storage = {item.key: item for item in node.storage}
        _require(set(actual_storage) == set(planned_storage), 'GCP inventory storage set changed')
        for key, item in planned_storage.items():
            _require(all(getattr(actual_storage[key], field) == getattr(item, field) for field in (
                'kind', 'mount_point', 'filesystem', 'size_gb', 'provisioned_iops',
                'provisioned_throughput_mibps', 'ephemeral')), 'GCP inventory storage contract changed')
    return manifest, inventory, names


def audit_gcp_ready(job, plan, *, credentials=None, clients=None) -> dict:
    """Audit every named resource and immutable GCP identity using GET only."""
    plan, resources = _source(job, 'gcp', plan)
    manifest, inventory, names = _gcp_contract(job, plan, resources)
    clients, account = _gcp_scope(resources, plan, credentials=credentials, clients=clients)
    for prefix in names:
        _require(all(resources.get(f'{prefix}_{field}') for field in ('name', 'id', 'self_link')),
                 f'GCP persisted identity incomplete: {prefix}')
    found = {prefix: gcp._get_cleanup_resource(prefix, resources, clients) for prefix in names}
    _require(all(value is not None for value in found.values()), 'GCP complete graph is not present')
    gcp._verify_distributed_cleanup_resources(job, found, manifest=manifest)
    _require(resources.get('gcp_dsb_router_nat_configured') is True, 'GCP NAT qualification missing')
    application = gcp._machine_type_details(plan.gcp_project_id, plan.gcp_zone, plan.shape, clients)
    gcp._validate_plan_capacity(plan, application)
    capacities = gcp._distributed_role_details(manifest, project_id=plan.gcp_project_id,
                    zone=plan.gcp_zone, application_details=application, clients=clients)
    database_storage = next(node for node in manifest.nodes if node.key == 'database').storage[0]
    _require(gcp._expected_disk_profile(resources, 'gcp_dsb_database_data_disk') == (
        gcp.DISTRIBUTED_DATABASE_DISK_TYPE, database_storage.size_gib, None, None),
        'GCP database disk profile differs from topology')
    _require(inventory.node('database').storage_resource('database-data').lifecycle_status == 'running',
             'GCP database storage inventory is not running')
    for role, details in capacities.items():
        _require(gcp._expected_disk_profile(resources, gcp.DISTRIBUTED_BOOT_PREFIXES[role]) == (
            details['disk_type'], gcp.DISTRIBUTED_OS_DISK_SIZE_GB,
            details['disk_provisioned_iops'], details['disk_provisioned_throughput_mibps']),
            f'GCP {role} boot disk profile differs from its machine family')
        _require(resources.get(f'gcp_dsb_{role.replace("-", "_")}_tag') == inventory.node(role).provider_resource_name,
                 'GCP role tag differs from deterministic identity')
    images = {}
    for kind, architecture in (('application', application['architecture']), ('support', 'x86_64')):
        prefix = 'gcp_dsb_' + kind + '_image'
        _require(all(resources.get(f'{prefix}_{field}') for field in ('id', 'name', 'self_link', 'family', 'architecture')),
                 'GCP exact immutable image pin missing')
        image = gcp._distributed_candidate_image(resources, key_prefix=prefix, architecture=architecture, clients=clients)
        live = gcp._call(clients['images'], 'get', clients, 'GetImageRequest', project=gcp.IMAGE_PROJECT, image=image['name'])
        _require(_numeric_id(gcp._value(live, 'id'), 'GCP image numeric ID missing') == image['image_id']
                 and gcp._value(live, 'name') == image['name']
                 and gcp._scoped_link_equal(gcp._value(live, 'self_link'), image['self_link'])
                 and gcp.normalize_architecture(gcp._value(live, 'architecture')) == architecture
                 and gcp._value(live, 'status') == 'READY', 'GCP pinned image no longer matches')
        images[kind] = image
    roles = {}
    for node in inventory.nodes:
        role = node.key
        prefix = gcp.DISTRIBUTED_NODE_PREFIXES[role]
        instance = found[prefix]
        details = capacities[role]
        _require(node.lifecycle_status == 'running' and gcp._value(instance, 'status') == 'RUNNING'
                 and details['machine_type'] == node.shape and details['architecture'] == node.architecture,
                 f'GCP {role} not running or role capacity changed')
        public, private = gcp._instance_ips(instance)
        _check_addresses(resources, node, role, 'gcp', public, private)
        roles[role] = {'id': _numeric_id(gcp._value(instance, 'id'), 'GCP instance numeric ID missing'),
                       'name': node.provider_resource_name, 'shape': details['machine_type'],
                       'architecture': details['architecture'], 'private_ip': private, 'public_ip': public,
                       'image_id': images['application' if role == 'application' else 'support']['image_id']}
    for prefix in ('gcp_dsb_database_data_disk', *gcp.DISTRIBUTED_BOOT_PREFIXES.values()):
        disk = found[prefix]
        role = 'database' if prefix == 'gcp_dsb_database_data_disk' else next(
            role for role, name in gcp.DISTRIBUTED_BOOT_PREFIXES.items() if name == prefix)
        users = list(gcp._value(disk, 'users', ()) or ())
        parent = found[gcp.DISTRIBUTED_NODE_PREFIXES[role]]
        _require(gcp._value(disk, 'status') == 'READY' and len(users) == 1
                 and gcp._scoped_link_equal(users[0], gcp._value(parent, 'self_link')),
                 'GCP disk is not ready and exclusively attached')
        if prefix in gcp.DISTRIBUTED_BOOT_PREFIXES.values():
            _require(gcp._value(disk, 'source_image') and gcp._value(disk, 'source_image_id'),
                     'GCP boot disk missing immutable image provenance')
    return _receipt(job, plan, manifest, state='ready', account=account, roles=roles,
        images=images,
        profiles={'roles': {role: {'shape': item['machine_type'], 'vcpus': item['vcpu'],
                                  'memory_gib': item['memory_gb'], 'architecture': item['architecture']}
                            for role, item in sorted(capacities.items())},
                  'disks': {prefix: list(gcp._expected_disk_profile(resources, prefix))
                            for prefix in sorted(('gcp_dsb_database_data_disk', *gcp.DISTRIBUTED_BOOT_PREFIXES.values()))}},
        resources=[{'prefix': prefix, 'id': _numeric_id(gcp._value(value, 'id'), 'GCP resource numeric ID missing'),
                                  'self_link': gcp._value(value, 'self_link')} for prefix, value in sorted(found.items())])


def audit_azure_absent(original_job, *, credential=None, clients=None) -> dict:
    """Prove the original exact Azure group is absent, never delete/adopt it.

    The caller must first run the provider's verified exact-child cleanup and
    check its cleared contract. This independent audit uses the *original*
    pre-cleanup snapshot; an empty or newly rewritten job cannot prove absence.
    """
    plan, resources = _source(original_job, 'azure')
    manifest, _, group, _ = _azure_contract(original_job, plan, resources)
    clients, account = _azure_scope(resources, plan, credential=credential, clients=clients)
    service = azure._resource_groups(clients)
    for _ in range(2):
        _require(azure._get_or_none(service, group) is None, 'Azure original group still exists (owned or foreign replacement)')
    return _receipt(original_job, plan, manifest, state='absent', account=account,
                    absence_passes=2, resource_group=group,
                    resource_group_id=resources['azure_resource_group_expected_id'])


def audit_gcp_absent(original_job, *, credentials=None, clients=None) -> dict:
    """GET every original deterministic name twice; any presence/error fails."""
    plan, resources = _source(original_job, 'gcp')
    manifest, _, names = _gcp_contract(original_job, plan, resources)
    clients, account = _gcp_scope(resources, plan, credentials=credentials, clients=clients)
    # Local request-only projection handles a lost first-create response: names
    # were persisted atomically in the allowlist before any cloud request, but
    # the corresponding flat name/ID might not have been recorded yet.
    lookup = dict(resources)
    for prefix, name in names.items():
        lookup[prefix + '_name'] = name
    for _ in range(2):
        for prefix in sorted(names):
            _require(gcp._get_cleanup_resource(prefix, lookup, clients) is None,
                     f'GCP original {prefix} still exists (owned or foreign replacement)')
    return _receipt(original_job, plan, manifest, state='absent', account=account,
                    absence_passes=2, expected_names=dict(sorted(names.items())))


def require_azure_cleanup_identity(original_job, expected_cloud_identity, *, credential=None, clients=None) -> dict:
    """Check pinned opaque VM/disk IDs before provider cleanup, not readiness.

    Missing children and powered-off VMs are allowed. Present same-name
    replacements or API errors are not. The provider still must independently
    verify its complete ownership/relationship graph before deleting anything.
    """
    plan, resources = _source(original_job, 'azure')
    manifest, _, group, specs = _azure_contract(original_job, plan, resources)
    clients, account = _azure_scope(resources, plan, credential=credential, clients=clients)
    expected = expected_cloud_identity
    _require(isinstance(expected, Mapping) and expected.get('revision') == REVISION
             and expected.get('provider') == 'azure' and expected.get('run_id') == original_job['id']
             and expected.get('account') == account
             and expected.get('plan_sha256') == _sha(plan.model_dump(exclude=_SSH_FIELDS))
             and expected.get('topology_fingerprint') == manifest.fingerprint,
             'Azure pinned cleanup identity belongs to another run/account/plan')
    pinned = expected.get('resources')
    _require(isinstance(pinned, list) and all(isinstance(item, Mapping) for item in pinned),
             'Azure pinned cleanup resource identities missing')
    by_prefix = {item.get('prefix'): item for item in pinned}
    _require(len(by_prefix) == len(pinned) and set(by_prefix) == {
        key.removesuffix('_expected_id') for key in specs}, 'Azure pinned graph inventory incomplete')
    observations = []
    for key, (name, identity) in sorted(specs.items()):
        prefix = key.removesuffix('_expected_id')
        _require(azure._normalize_id(by_prefix[prefix].get('id')) == azure._normalize_id(identity),
                 'Azure pinned ARM identity changed')
        if not (prefix.endswith('_instance') or prefix.endswith('_disk')):
            continue
        unique = _uuid(by_prefix[prefix].get('unique_id'), 'Azure opaque cleanup identity missing')
        service = azure._compute_service(clients, 'virtual_machines' if prefix.endswith('_instance') else 'disks')
        _require(service is not None, 'Azure cleanup identity read client missing')
        live = azure._get_or_none(service, group, name)
        if live is not None:
            _require(azure._normalize_id(_az(live, 'id')) == azure._normalize_id(identity)
                     and _uuid(_az(live, 'properties', 'vmId' if prefix.endswith('_instance') else 'uniqueId'),
                               'Azure present child missing opaque ID') == unique,
                     f'Azure {prefix} was replaced; cleanup is not authorized')
        observations.append({'prefix': prefix, 'id': identity, 'unique_id': unique, 'present': live is not None})
    return {'schema_version': 1, 'candidate_only': True,
            'revision': REVISION, 'read_only': True, 'provider': 'azure', 'run_id': original_job['id'],
            'cleanup_identity_matches': True, 'cloud_identity_sha256': _sha(expected), 'resources': observations}
