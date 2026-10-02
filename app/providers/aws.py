"""AWS discovery, provisioning, and cleanup for the benchmark MVP.

The adapter deliberately creates a fresh boto3 ``Session`` for each public
operation.  Only the profile name is carried in a plan; AWS credentials remain
in the user's normal local AWS configuration and are never copied into job
state.
"""

from __future__ import annotations

import math
import base64
import binascii
import copy
import hashlib
import re
import time
import uuid
from dataclasses import replace
from collections.abc import Callable, Mapping
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, WaiterError

from ..guests import amazon_linux
from ..guests.amazon_linux import SSH_USER, ami_parameter_name
from ..deathstarbench_contract import (
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DISTRIBUTED_TIERED_TOPOLOGY_ID,
    K3S_RUNTIME_ID,
    K3S_RUNTIME_JOURNAL_KEY,
    require_released_runtime,
)
from ..deathstarbench_distributed import (
    DISTRIBUTED_NETWORK_QUALIFICATION_KEY,
    TOPOLOGY_FINGERPRINT_KEY,
    TOPOLOGY_MANIFEST_KEY,
)
from ..deathstarbench_topology import build_topology_manifest
from ..resource_inventory import (
    ROLE_NODE_INVENTORY_KEY,
    ResourceInventoryError,
    RoleNodeInventory,
    load_role_node_inventory,
    persist_role_node_inventory,
)


DEFAULT_PROFILE = 'default'
DEFAULT_REGION = 'us-east-2'
MANAGED_BY = 'oci-self-service-benchmarks'
VPC_CIDR = '10.42.0.0/16'
SUBNET_CIDR = '10.42.1.0/24'
SSH_SOURCE_CIDR = '0.0.0.0/0'
DATA_VOLUME_DEVICE = '/dev/sdf'
DEFAULT_GP3_IOPS = 3000
DEFAULT_GP3_THROUGHPUT_MIBPS = 125
AMBIGUOUS_TAG_LOOKUP_ATTEMPTS = 6
AMBIGUOUS_TAG_LOOKUP_DELAY_SECONDS = 2
_DSB_NO_RETRY_CONFIG = Config(
    retries={'mode': 'standard', 'total_max_attempts': 1},
)
VPC_CONTRACT_KEYS = (
    'aws_vpc_create_ambiguous',
    'aws_vpc_reconciliation_error',
)
INTERNET_GATEWAY_CONTRACT_KEYS = (
    'aws_internet_gateway_create_ambiguous',
    'aws_internet_gateway_reconciliation_error',
)
ROUTE_TABLE_CONTRACT_KEYS = (
    'aws_route_table_client_token',
    'aws_route_table_create_ambiguous',
    'aws_route_table_reconciliation_error',
    'aws_route_table_vpc_id',
)
KEY_PAIR_CONTRACT_KEYS = (
    'aws_key_pair_import_ambiguous',
    'aws_key_pair_reconciliation_error',
)
LOADGEN_SECURITY_GROUP_CONTRACT_KEYS = (
    'aws_loadgen_security_group_create_ambiguous',
    'aws_loadgen_security_group_reconciliation_error',
)
CREATE_RECONCILIATION_ERROR_KEYS = (
    'aws_vpc_reconciliation_error',
    'aws_internet_gateway_reconciliation_error',
    'aws_route_table_reconciliation_error',
    'aws_key_pair_reconciliation_error',
    'aws_loadgen_security_group_reconciliation_error',
)
DATA_VOLUME_CONTRACT_KEYS = (
    'aws_data_volume_client_token',
    'aws_data_volume_create_ambiguous',
    'aws_data_volume_reconciliation_error',
    'aws_data_volume_attached_instance_id',
    'aws_data_volume_device',
    'aws_data_volume_availability_zone',
    'aws_data_volume_size_gb',
    'aws_data_volume_type',
    'aws_data_volume_iops',
    'aws_data_volume_throughput_mibps',
)
PEER_BOOT_SIZE_GB = 20
WEB_BENCHMARKS = frozenset({'apachebench', 'deathstarbench'})
LOAD_GENERATOR_INSTANCE_TYPES = (
    'm7i.large',
    'm6i.large',
    'm5.large',
)
LOAD_GENERATOR_VCPUS = 2
LOAD_GENERATOR_MEMORY_GB = 8
LOAD_GENERATOR_BOOT_SIZE_GB = 50
LOAD_GENERATOR_INSTANCE_CONTRACT_KEYS = (
    'aws_loadgen_instance_id',
    'aws_loadgen_instance_client_token',
    'aws_loadgen_instance_type',
    'aws_loadgen_image_id',
    'aws_loadgen_image_name',
    'aws_loadgen_architecture',
    'aws_loadgen_vcpus',
    'aws_loadgen_memory_gb',
    'aws_loadgen_availability_zone',
    'aws_loadgen_network_baseline_gbps',
    'aws_loadgen_network_peak_gbps',
    'aws_loadgen_public_ip',
    'aws_loadgen_private_ip',
    'loadgen_instance_id',
    'loadgen_shape',
    'loadgen_image_id',
    'loadgen_image_name',
    'loadgen_architecture',
    'loadgen_vcpus',
    'loadgen_memory_gb',
    'loadgen_network_bandwidth_gbps',
    'loadgen_network_peak_bandwidth_gbps',
    'loadgen_network_capacity_kind',
    'loadgen_public_ip',
    'loadgen_private_ip',
)
BARE_METAL_WAITER_DELAY_SECONDS = 15
BARE_METAL_WAITER_MAX_ATTEMPTS = 160
AMI_PARAMETER_PREFIX = (
    '/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-'
)
SUPPORTED_ARCHITECTURES = ('x86_64', 'arm64')

EventCallback = Callable[[dict[str, Any], str, str], None]
PersistCallback = Callable[[dict[str, Any]], None]


def create_session(profile: str = DEFAULT_PROFILE, region: str | None = None):
    """Create a non-shared boto3 Session from a named local AWS profile."""

    return boto3.Session(
        profile_name=(profile or DEFAULT_PROFILE),
        region_name=region,
    )


def _session(profile, region, aws_session=None):
    return aws_session or create_session(profile, region)


def _emit(job, emit, stage, message):
    if emit:
        emit(job, stage, message)


def _persist(job, persist):
    if persist:
        persist(job)


def _record(job, persist, **values):
    """Record non-secret resource metadata and persist it immediately."""

    job.setdefault('resources', {}).update(values)
    _persist(job, persist)


def _forget(job, persist, *keys):
    resources = job.setdefault('resources', {})
    for key in keys:
        resources.pop(key, None)
    _persist(job, persist)


def _confirmed_client_rejection(error):
    if not isinstance(error, ClientError):
        return False
    status = error.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
    return isinstance(status, int) and 400 <= status < 500


def _value(value, name, default=None):
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _profile(plan):
    return str(_value(plan, 'aws_profile', DEFAULT_PROFILE) or DEFAULT_PROFILE)


def _region(plan):
    return str(_value(plan, 'region', DEFAULT_REGION) or DEFAULT_REGION)


def _instance_type(plan):
    return str(
        _value(plan, 'instance_type')
        or _value(plan, 'shape')
        or ''
    ).strip()


def _iperf3_protocols(plan):
    benchmarks = set(_value(plan, 'benchmarks', ()) or ())
    if 'iperf3' not in benchmarks:
        return ()
    options = _value(plan, 'iperf3', {}) or {}
    protocols = tuple(dict.fromkeys(
        str(item) for item in (_value(options, 'protocols', ()) or ())
    ))
    if not protocols:
        raise ValueError('Select at least one iperf3 protocol for AWS.')
    unsupported = sorted(
        set(protocols) - amazon_linux.SUPPORTED_IPERF3_PROTOCOLS
    )
    if unsupported:
        raise ValueError(
            'The selected iperf3 protocol is not supported on AWS; '
            f'unsupported selection: {", ".join(unsupported)}.'
        )
    return protocols


def _web_benchmarks(plan):
    """Return selected two-instance web workloads in stable port-rule order."""

    selected = set(_value(plan, 'benchmarks', ()) or ())
    return tuple(
        benchmark
        for benchmark in ('apachebench', 'deathstarbench')
        if benchmark in selected
    )


def _tags(job_id):
    return [
        {'Key': 'Name', 'Value': f'benchmark-{job_id}'},
        {'Key': 'benchmark-job', 'Value': job_id},
        {'Key': 'managed-by', 'Value': MANAGED_BY},
    ]


def _data_volume_create_request(job_id, availability_zone, size_gb, token):
    """Build the exact idempotent request used for gp3 create/recovery."""
    return {
        'AvailabilityZone': str(availability_zone),
        'Size': int(size_gb),
        'VolumeType': 'gp3',
        'Iops': DEFAULT_GP3_IOPS,
        'Throughput': DEFAULT_GP3_THROUGHPUT_MIBPS,
        'Encrypted': True,
        'ClientToken': str(token),
        'TagSpecifications': [{
            'ResourceType': 'volume',
            'Tags': [
                *_tags(job_id),
                {'Key': 'benchmark-role', 'Value': 'data'},
            ],
        }],
    }


def _route_table_create_request(job_id, vpc_id, token):
    """Build the exact idempotent request used for route-table recovery."""
    return {
        'VpcId': str(vpc_id),
        'ClientToken': str(token),
        'TagSpecifications': [{
            'ResourceType': 'route-table',
            'Tags': _tags(job_id),
        }],
    }


def _paginate(client, operation_name, result_key, **kwargs):
    paginator = client.get_paginator(operation_name)
    for page in paginator.paginate(**kwargs):
        yield from page.get(result_key, [])


def bootstrap(
    profile: str = DEFAULT_PROFILE,
    default_region: str = DEFAULT_REGION,
    aws_session=None,
):
    """Validate a profile and return its caller identity and enabled regions."""

    profile = profile or DEFAULT_PROFILE
    default_region = default_region or DEFAULT_REGION
    session = _session(profile, default_region, aws_session)
    identity = session.client('sts').get_caller_identity()
    ec2 = session.client('ec2', region_name=default_region)
    response = ec2.describe_regions(AllRegions=False)
    regions = [
        item['RegionName']
        for item in response.get('Regions', [])
        if item.get('OptInStatus') in (None, 'opt-in-not-required', 'opted-in')
    ]
    return {
        'provider': 'aws',
        'profile': profile,
        'default_region': default_region,
        'regions': sorted(set(regions)),
        'account_id': identity.get('Account'),
        'principal_arn': identity.get('Arn'),
        'user_id': identity.get('UserId'),
    }


def placement(
    profile: str = DEFAULT_PROFILE,
    region: str = DEFAULT_REGION,
    aws_session=None,
):
    """List Availability Zones available to the selected account."""

    session = _session(profile, region, aws_session)
    ec2 = session.client('ec2', region_name=region)
    response = ec2.describe_availability_zones(
        Filters=[{'Name': 'state', 'Values': ['available']}],
    )
    zones = [
        {
            'name': zone['ZoneName'],
            'zone_id': zone.get('ZoneId'),
            'state': zone.get('State'),
        }
        for zone in response.get('AvailabilityZones', [])
    ]
    zones.sort(key=lambda item: item['name'])
    return {'availability_zones': zones}


def _supported_architectures(instance_type):
    architectures = instance_type.get('ProcessorInfo', {}).get(
        'SupportedArchitectures',
        [],
    )
    return [item for item in SUPPORTED_ARCHITECTURES if item in architectures]


def _local_nvme_storage_summary(instance_type):
    """Return a fail-closed local-NVMe profile from EC2 type metadata.

    ``/dev/nvme*`` is not sufficient evidence of instance-local storage on
    Nitro because EBS volumes use the same guest interface.  Only advertise a
    profile when EC2 positively reports instance storage, NVMe support, a
    uniform all-SSD disk layout, and an internally consistent total size.
    """
    unavailable = {
        'local_nvme_supported': False,
        'local_nvme_disk_count': 0,
        'local_nvme_disk_size_gb': 0,
        'local_nvme_total_size_gb': 0,
    }
    if instance_type.get('InstanceStorageSupported') is not True:
        return unavailable
    storage = instance_type.get('InstanceStorageInfo') or {}
    if str(storage.get('NvmeSupport') or '').casefold() not in {
        'supported',
        'required',
    }:
        return unavailable
    total_size_gb = storage.get('TotalSizeInGB')
    if (
        isinstance(total_size_gb, bool)
        or not isinstance(total_size_gb, int)
        or total_size_gb <= 0
    ):
        return unavailable
    disk_count = 0
    disk_sizes = set()
    for disk in storage.get('Disks') or ():
        count = disk.get('Count')
        size_gb = disk.get('SizeInGB')
        if (
            str(disk.get('Type') or '').casefold() != 'ssd'
            or isinstance(count, bool)
            or not isinstance(count, int)
            or count <= 0
            or isinstance(size_gb, bool)
            or not isinstance(size_gb, int)
            or size_gb <= 0
        ):
            return unavailable
        disk_count += count
        disk_sizes.add(size_gb)
    if len(disk_sizes) != 1 or disk_count <= 0:
        return unavailable
    disk_size_gb = disk_sizes.pop()
    if disk_count * disk_size_gb != total_size_gb:
        return unavailable
    return {
        'local_nvme_supported': True,
        'local_nvme_disk_count': disk_count,
        'local_nvme_disk_size_gb': disk_size_gb,
        'local_nvme_total_size_gb': total_size_gb,
    }


def _al2023_incompatibility(instance_type):
    """Explain EC2 families whose advertised ISA cannot boot this AL2023 AMI."""
    name = str(instance_type.get('InstanceType') or '').casefold()
    family = name.partition('.')[0]
    if family == 'a1':
        return (
            f'{name or "The selected instance type"} uses first-generation '
            'Graviton (A1), which Amazon Linux 2023 does not support. Choose '
            'a Graviton2-or-later Arm instance type.'
        )
    if family.startswith('mac'):
        return (
            f'{name or "The selected instance type"} is an EC2 Mac instance; '
            'Mac instances require a macOS AMI on a Dedicated Host and cannot '
            'run this standard Amazon Linux 2023 launch flow.'
        )
    return None


def _instance_type_summary(item):
    architectures = _supported_architectures(item)
    return {
        'instance_type': item['InstanceType'],
        # ``shape`` keeps the generic UI/result contract compatible with OCI.
        'shape': item['InstanceType'],
        'vcpu': item.get('VCpuInfo', {}).get('DefaultVCpus'),
        'ocpus': item.get('VCpuInfo', {}).get('DefaultVCpus'),
        'memory_gb': round(
            item.get('MemoryInfo', {}).get('SizeInMiB', 0) / 1024,
            3,
        ),
        'architecture': architectures[0] if len(architectures) == 1 else None,
        'architectures': architectures,
        'network_performance': item.get('NetworkInfo', {}).get(
            'NetworkPerformance'
        ),
        'current_generation': bool(item.get('CurrentGeneration', False)),
        'bare_metal': bool(item.get('BareMetal', False)),
        'burstable': bool(item.get('BurstablePerformanceSupported', False)),
        'hypervisor': item.get('Hypervisor'),
        **_local_nvme_storage_summary(item),
    }


def _supports_plan_capacity(item):
    """Keep discovery aligned with the plan model's minimum capacities."""

    return (
        not _al2023_incompatibility(item)
        and bool(_supported_architectures(item))
        and (item.get('VCpuInfo', {}).get('DefaultVCpus') or 0) >= 1
        and (item.get('MemoryInfo', {}).get('SizeInMiB') or 0) >= 1024
    )


def instance_types(
    profile: str = DEFAULT_PROFILE,
    region: str = DEFAULT_REGION,
    availability_zone: str | None = None,
    aws_session=None,
):
    """List launchable x86-64/Arm instance types and normalized capacities."""

    session = _session(profile, region, aws_session)
    ec2 = session.client('ec2', region_name=region)
    offering_kwargs = {
        'LocationType': 'availability-zone' if availability_zone else 'region',
    }
    if availability_zone:
        offering_kwargs['Filters'] = [
            {'Name': 'location', 'Values': [availability_zone]}
        ]
    names = sorted({
        item['InstanceType']
        for item in _paginate(
            ec2,
            'describe_instance_type_offerings',
            'InstanceTypeOfferings',
            **offering_kwargs,
        )
    })
    items = []
    for offset in range(0, len(names), 100):
        response = ec2.describe_instance_types(
            InstanceTypes=names[offset:offset + 100]
        )
        items.extend(response.get('InstanceTypes', []))
    summaries = [
        _instance_type_summary(item)
        for item in items
        if _supports_plan_capacity(item)
    ]
    summaries.sort(key=lambda item: item['instance_type'].casefold())
    return {'items': summaries}


def _normalize_ami_architecture(architecture):
    aliases = {
        'amd64': 'x86_64',
        'x86-64': 'x86_64',
        'x86_64': 'x86_64',
        'aarch64': 'arm64',
        'arm64': 'arm64',
    }
    try:
        return aliases[str(architecture).lower()]
    except KeyError as exc:
        raise ValueError(
            f'Amazon Linux 2023 is not supported for architecture '
            f'{architecture!r}.'
        ) from exc


def latest_amazon_linux_2023_ami(
    profile: str = DEFAULT_PROFILE,
    region: str = DEFAULT_REGION,
    architecture: str = 'x86_64',
    aws_session=None,
):
    """Resolve and verify AWS's latest standard Amazon Linux 2023 AMI."""

    architecture = _normalize_ami_architecture(architecture)
    session = _session(profile, region, aws_session)
    parameter_name = ami_parameter_name(architecture)
    parameter = session.client('ssm', region_name=region).get_parameter(
        Name=parameter_name,
    )['Parameter']
    image_id = parameter['Value']
    images = session.client('ec2', region_name=region).describe_images(
        ImageIds=[image_id],
        Owners=['amazon'],
    ).get('Images', [])
    if len(images) != 1:
        raise RuntimeError(
            f'AWS public parameter {parameter_name} did not resolve to one '
            'available Amazon-owned AMI.'
        )
    image = images[0]
    if image.get('State') != 'available':
        raise RuntimeError(f'Amazon Linux AMI {image_id} is not available.')
    if image.get('Architecture') != architecture:
        raise RuntimeError(
            f'Amazon Linux AMI {image_id} has architecture '
            f'{image.get("Architecture")!r}, expected {architecture!r}.'
        )
    return {
        'image_id': image_id,
        'name': image.get('Name'),
        'architecture': architecture,
        'root_device_name': image.get('RootDeviceName') or '/dev/xvda',
        'parameter_name': parameter_name,
        'parameter_version': parameter.get('Version'),
    }


def _instance_type_details(ec2, instance_type):
    response = ec2.describe_instance_types(InstanceTypes=[instance_type])
    items = response.get('InstanceTypes', [])
    if len(items) != 1:
        raise RuntimeError(f'AWS did not describe instance type {instance_type}.')
    item = items[0]
    incompatibility = _al2023_incompatibility(item)
    if incompatibility:
        raise ValueError(incompatibility)
    supported = _supported_architectures(item)
    if not supported:
        raise RuntimeError(
            f'{instance_type} does not support an Amazon Linux 2023 '
            'x86-64 or Arm image.'
        )
    # Prefer x86-64 when an instance family advertises more than one option;
    # it has the broadest compatibility with the current benchmark set.
    return {
        'architecture': 'x86_64' if 'x86_64' in supported else supported[0],
        'vcpu': item.get('VCpuInfo', {}).get('DefaultVCpus'),
        'memory_gb': item.get('MemoryInfo', {}).get('SizeInMiB', 0) / 1024,
        'bare_metal': bool(item.get('BareMetal', False)),
        'hypervisor': item.get('Hypervisor'),
        **_local_nvme_storage_summary(item),
    }


def _load_generator_type_details(ec2, availability_zone):
    """Select the first compatible fixed x86 load generator in one AZ.

    AWS reports baseline and peak bandwidth per network card.  The supported
    fixed-size candidates expose one primary card, so record card zero's exact
    API values rather than inferring capacity from marketing text.
    """

    availability_zone = str(availability_zone)
    filters = [
        {
            'Name': 'instance-type',
            'Values': list(LOAD_GENERATOR_INSTANCE_TYPES),
        },
        {'Name': 'location', 'Values': [availability_zone]},
    ]
    offered = set()
    request = {
        'LocationType': 'availability-zone',
        'Filters': filters,
    }
    while True:
        response = ec2.describe_instance_type_offerings(**request)
        offered.update(
            item.get('InstanceType')
            for item in response.get('InstanceTypeOfferings', [])
            if (
                item.get('InstanceType') in LOAD_GENERATOR_INSTANCE_TYPES
                and item.get('Location') == availability_zone
            )
        )
        next_token = response.get('NextToken')
        if not next_token:
            break
        request['NextToken'] = next_token

    ordered = [
        instance_type
        for instance_type in LOAD_GENERATOR_INSTANCE_TYPES
        if instance_type in offered
    ]
    if not ordered:
        raise RuntimeError(
            'The selected web benchmark needs a fixed x86 AWS load generator, '
            f'but none of {", ".join(LOAD_GENERATOR_INSTANCE_TYPES)} is '
            f'offered in Availability Zone {availability_zone}.'
        )

    response = ec2.describe_instance_types(InstanceTypes=ordered)
    by_name = {
        item.get('InstanceType'): item
        for item in response.get('InstanceTypes', [])
        if item.get('InstanceType')
    }
    rejected = []
    for instance_type in ordered:
        item = by_name.get(instance_type)
        if not item:
            rejected.append(f'{instance_type}: metadata missing')
            continue
        architectures = item.get('ProcessorInfo', {}).get(
            'SupportedArchitectures',
            (),
        )
        vcpus = item.get('VCpuInfo', {}).get('DefaultVCpus')
        memory_mib = item.get('MemoryInfo', {}).get('SizeInMiB')
        network_cards = item.get('NetworkInfo', {}).get('NetworkCards') or ()
        primary_card = next(
            (
                card
                for card in network_cards
                if card.get('NetworkCardIndex') == 0
            ),
            network_cards[0] if network_cards else None,
        )
        try:
            baseline_gbps = float(
                primary_card.get('BaselineBandwidthInGbps')
            )
            peak_gbps = float(primary_card.get('PeakBandwidthInGbps'))
        except (AttributeError, TypeError, ValueError):
            baseline_gbps = 0.0
            peak_gbps = 0.0
        if (
            'x86_64' not in architectures
            or vcpus != LOAD_GENERATOR_VCPUS
            or memory_mib != LOAD_GENERATOR_MEMORY_GB * 1024
            or not math.isfinite(baseline_gbps)
            or not math.isfinite(peak_gbps)
            or baseline_gbps <= 0
            or peak_gbps < baseline_gbps
        ):
            rejected.append(
                f'{instance_type}: expected x86_64, '
                f'{LOAD_GENERATOR_VCPUS} vCPUs, '
                f'{LOAD_GENERATOR_MEMORY_GB} GiB, and positive baseline/peak '
                'bandwidth metadata'
            )
            continue
        return {
            'instance_type': instance_type,
            'architecture': 'x86_64',
            'vcpu': LOAD_GENERATOR_VCPUS,
            'memory_gb': LOAD_GENERATOR_MEMORY_GB,
            'network_baseline_gbps': baseline_gbps,
            'network_peak_gbps': peak_gbps,
        }

    raise RuntimeError(
        'AWS did not report a compatible 2-vCPU, 8-GiB x86 load generator '
        f'with usable network-capacity metadata in {availability_zone}: '
        + '; '.join(rejected)
        + '.'
    )


def _validate_benchmark_capacity(plan, type_details):
    """Reject selected source builds that predictably exceed guest capacity."""
    benchmarks = set(_value(plan, 'benchmarks', ()) or ())
    phoronix = _value(plan, 'phoronix', {}) or {}
    profiles = set(_value(phoronix, 'profiles', ()) or ())
    memory_gb = float(type_details.get('memory_gb') or 0)
    if (
        'phoronix' in benchmarks
        and 'build_linux_kernel' in profiles
        and memory_gb < 4
    ):
        raise ValueError(
            'Phoronix Linux Kernel Compilation on AWS requires an instance '
            f'with at least 4 GiB of memory; the selected instance has '
            f'{memory_gb:g} GiB. Choose a larger instance type or deselect '
            'that profile.'
        )


def _resolve_availability_zone(ec2, instance_type, requested_zone=None):
    """Choose an available AZ that explicitly offers the instance type."""

    zones = ec2.describe_availability_zones(
        Filters=[{'Name': 'state', 'Values': ['available']}],
    ).get('AvailabilityZones', [])
    available = {
        item.get('ZoneName')
        for item in zones
        if item.get('ZoneName')
    }
    if requested_zone and requested_zone not in available:
        raise ValueError(
            f'AWS Availability Zone {requested_zone} is not available to the '
            'local account in the selected region. Refresh placement and try '
            'again.'
        )

    filters = [{'Name': 'instance-type', 'Values': [instance_type]}]
    if requested_zone:
        filters.append({'Name': 'location', 'Values': [requested_zone]})
    response = ec2.describe_instance_type_offerings(
        LocationType='availability-zone',
        Filters=filters,
    )
    offered_zones = {
        item.get('Location')
        for item in response.get('InstanceTypeOfferings', [])
        if item.get('InstanceType') == instance_type and item.get('Location')
    }
    candidates = sorted(available & offered_zones)
    if requested_zone:
        if requested_zone in candidates:
            return requested_zone
        raise ValueError(
            f'{instance_type} is not currently offered in AWS Availability '
            f'Zone {requested_zone}. Refresh the instance-type list and '
            'choose another type or zone.'
        )
    if not candidates:
        raise ValueError(
            f'{instance_type} is not currently offered in an available AWS '
            'Availability Zone in the selected region. Refresh the '
            'instance-type list and choose another type.'
        )
    return candidates[0]


def provision(
    job: dict[str, Any],
    plan,
    *,
    public_key: str | None = None,
    emit: EventCallback | None = None,
    persist: PersistCallback | None = None,
    aws_session=None,
):
    """Create an isolated public AWS benchmark target.

    Each returned resource identifier is recorded and persisted immediately,
    before the next dependent operation starts.  That makes cleanup possible
    even when provisioning fails part-way through.
    """

    dsb = _value(plan, 'deathstarbench', {}) or {}
    if 'deathstarbench' in (_value(plan, 'benchmarks', ()) or ()):
        topology = _value(dsb, 'topology_id', 'single_host_v1')
        runtime = _value(dsb, 'runtime_id', 'podman_compose_v1')
        if (
            topology == DISTRIBUTED_TIERED_TOPOLOGY_ID
            and runtime == K3S_RUNTIME_ID
        ):
            raise ValueError(
                'The compact AWS provision entry point cannot create '
                'distributed DeathStarBench; use the public distributed '
                'lifecycle.'
            )
        require_released_runtime(topology, runtime)
    profile = _profile(plan)
    region = _region(plan)
    instance_type = _instance_type(plan)
    if not instance_type:
        raise ValueError('Select an AWS instance type.')
    public_key = (public_key or job.get('_public_key') or '').strip()
    if not public_key:
        raise ValueError('An SSH public key is required to launch AWS instances.')
    iperf3_protocols = _iperf3_protocols(plan)
    web_benchmarks = _web_benchmarks(plan)

    session = _session(profile, region, aws_session)
    ec2 = session.client('ec2', region_name=region)
    resources = job.setdefault('resources', {})
    suffix = str(job['id'])
    tags = _tags(suffix)
    tag_specifications = lambda resource_type: [
        {'ResourceType': resource_type, 'Tags': tags}
    ]

    identity = session.client('sts', region_name=region).get_caller_identity()
    type_details = _instance_type_details(ec2, instance_type)
    _validate_benchmark_capacity(plan, type_details)
    requested_zone = (
        resources.get('availability_zone')
        or _value(plan, 'availability_zone')
        or _value(plan, 'availability_domain')
    )
    availability_zone = _resolve_availability_zone(
        ec2,
        instance_type,
        requested_zone,
    )
    loadgen_type_details = (
        _load_generator_type_details(ec2, availability_zone)
        if web_benchmarks
        else None
    )
    architecture = type_details['architecture']
    requested_vcpus = _value(plan, 'ocpus')
    requested_memory_gb = _value(plan, 'memory_gb')
    if (
        requested_vcpus is not None
        and float(requested_vcpus) != float(type_details['vcpu'])
    ):
        raise ValueError(
            f'{instance_type} has {type_details["vcpu"]} vCPUs, but the plan '
            f'requested {requested_vcpus}. Refresh the AWS instance-type '
            'selection and try again.'
        )
    if (
        requested_memory_gb is not None
        and float(requested_memory_gb) != float(type_details['memory_gb'])
    ):
        raise ValueError(
            f'{instance_type} has {type_details["memory_gb"]:g} GiB of memory, '
            f'but the plan requested {requested_memory_gb}. Refresh the AWS '
            'instance-type selection and try again.'
        )
    image = latest_amazon_linux_2023_ami(
        profile,
        region,
        architecture,
        aws_session=session,
    )
    loadgen_image = None
    if loadgen_type_details:
        loadgen_image = (
            image
            if architecture == 'x86_64'
            else latest_amazon_linux_2023_ami(
                profile,
                region,
                'x86_64',
                aws_session=session,
            )
        )
    _record(
        job,
        persist,
        provider='aws',
        aws_profile=profile,
        region=region,
        instance_type=instance_type,
        ssh_user=SSH_USER,
        aws_account_id=identity.get('Account'),
        aws_principal_arn=identity.get('Arn'),
        image_id=image['image_id'],
        image_name=image.get('name'),
        architecture=architecture,
        vcpu=type_details['vcpu'],
        ocpus=type_details['vcpu'],
        memory_gb=type_details['memory_gb'],
        bare_metal=type_details['bare_metal'],
        hypervisor=type_details.get('hypervisor'),
        local_nvme_supported=bool(
            type_details.get('local_nvme_supported', False)
        ),
        local_nvme_disk_count=int(
            type_details.get('local_nvme_disk_count', 0) or 0
        ),
        local_nvme_disk_size_gb=int(
            type_details.get('local_nvme_disk_size_gb', 0) or 0
        ),
        local_nvme_total_size_gb=int(
            type_details.get('local_nvme_total_size_gb', 0) or 0
        ),
        availability_zone=availability_zone,
        ami_parameter=image['parameter_name'],
        ami_parameter_version=image.get('parameter_version'),
    )
    if loadgen_type_details and loadgen_image:
        _record(
            job,
            persist,
            aws_loadgen_instance_type=(
                loadgen_type_details['instance_type']
            ),
            aws_loadgen_image_id=loadgen_image['image_id'],
            aws_loadgen_image_name=loadgen_image.get('name'),
            aws_loadgen_architecture=loadgen_type_details['architecture'],
            aws_loadgen_vcpus=loadgen_type_details['vcpu'],
            aws_loadgen_memory_gb=loadgen_type_details['memory_gb'],
            aws_loadgen_availability_zone=availability_zone,
            aws_loadgen_network_baseline_gbps=(
                loadgen_type_details['network_baseline_gbps']
            ),
            aws_loadgen_network_peak_gbps=(
                loadgen_type_details['network_peak_gbps']
            ),
            loadgen_shape=loadgen_type_details['instance_type'],
            loadgen_image_id=loadgen_image['image_id'],
            loadgen_image_name=loadgen_image.get('name'),
            loadgen_architecture=loadgen_type_details['architecture'],
            loadgen_vcpus=loadgen_type_details['vcpu'],
            loadgen_memory_gb=loadgen_type_details['memory_gb'],
            loadgen_network_bandwidth_gbps=(
                loadgen_type_details['network_baseline_gbps']
            ),
            loadgen_network_peak_bandwidth_gbps=(
                loadgen_type_details['network_peak_gbps']
            ),
            loadgen_network_capacity_kind=(
                'aws_primary_network_card_baseline'
            ),
        )

    _forget(job, persist, 'aws_vpc_reconciliation_error')
    _record(job, persist, aws_vpc_create_ambiguous=True)
    try:
        vpc = ec2.create_vpc(
            CidrBlock=VPC_CIDR,
            TagSpecifications=tag_specifications('vpc'),
        )['Vpc']
    except Exception as exc:
        if _confirmed_client_rejection(exc):
            _forget(job, persist, *VPC_CONTRACT_KEYS)
        raise
    _record(
        job,
        persist,
        aws_vpc_id=vpc['VpcId'],
        aws_vpc_create_ambiguous=False,
    )
    _emit(job, emit, 'Provision', f'Created AWS VPC {vpc["VpcId"]}.')
    ec2.get_waiter('vpc_available').wait(VpcIds=[vpc['VpcId']])
    ec2.modify_vpc_attribute(
        VpcId=vpc['VpcId'],
        EnableDnsSupport={'Value': True},
    )
    ec2.modify_vpc_attribute(
        VpcId=vpc['VpcId'],
        EnableDnsHostnames={'Value': True},
    )

    _forget(job, persist, 'aws_internet_gateway_reconciliation_error')
    _record(job, persist, aws_internet_gateway_create_ambiguous=True)
    try:
        gateway = ec2.create_internet_gateway(
            TagSpecifications=tag_specifications('internet-gateway'),
        )['InternetGateway']
    except Exception as exc:
        if _confirmed_client_rejection(exc):
            _forget(job, persist, *INTERNET_GATEWAY_CONTRACT_KEYS)
        raise
    gateway_id = gateway['InternetGatewayId']
    _record(
        job,
        persist,
        aws_internet_gateway_id=gateway_id,
        aws_internet_gateway_create_ambiguous=False,
    )
    ec2.attach_internet_gateway(
        InternetGatewayId=gateway_id,
        VpcId=vpc['VpcId'],
    )
    _record(job, persist, aws_internet_gateway_attached=True)

    subnet_request = {
        'VpcId': vpc['VpcId'],
        'CidrBlock': SUBNET_CIDR,
        'AvailabilityZone': availability_zone,
        'TagSpecifications': tag_specifications('subnet'),
    }
    subnet = ec2.create_subnet(**subnet_request)['Subnet']
    subnet_id = subnet['SubnetId']
    created_zone = subnet.get('AvailabilityZone') or availability_zone
    _record(
        job,
        persist,
        aws_subnet_id=subnet_id,
        availability_zone=created_zone,
    )
    if created_zone != availability_zone:
        raise RuntimeError(
            f'AWS created subnet {subnet_id} in {created_zone}, expected '
            f'{availability_zone}.'
        )
    ec2.get_waiter('subnet_available').wait(SubnetIds=[subnet_id])
    ec2.modify_subnet_attribute(
        SubnetId=subnet_id,
        MapPublicIpOnLaunch={'Value': True},
    )

    route_table_token = (
        resources.get('aws_route_table_client_token') or uuid.uuid4().hex
    )
    _forget(job, persist, 'aws_route_table_reconciliation_error')
    _record(
        job,
        persist,
        aws_route_table_client_token=route_table_token,
        aws_route_table_create_ambiguous=True,
        aws_route_table_vpc_id=vpc['VpcId'],
    )
    try:
        route_table = ec2.create_route_table(
            **_route_table_create_request(
                suffix,
                vpc['VpcId'],
                route_table_token,
            )
        )['RouteTable']
    except Exception as exc:
        if _confirmed_client_rejection(exc):
            _forget(job, persist, *ROUTE_TABLE_CONTRACT_KEYS)
        raise
    route_table_id = route_table['RouteTableId']
    _record(
        job,
        persist,
        aws_route_table_id=route_table_id,
        aws_route_table_create_ambiguous=False,
    )
    association = ec2.associate_route_table(
        RouteTableId=route_table_id,
        SubnetId=subnet_id,
    )
    association_id = association['AssociationId']
    _record(job, persist, aws_route_table_association_id=association_id)
    ec2.create_route(
        RouteTableId=route_table_id,
        DestinationCidrBlock='0.0.0.0/0',
        GatewayId=gateway_id,
    )

    group = ec2.create_security_group(
        GroupName=f'benchmark-{suffix}',
        Description=f'Benchmark runner SSH access for job {suffix}',
        VpcId=vpc['VpcId'],
        TagSpecifications=tag_specifications('security-group'),
    )
    group_id = group['GroupId']
    _record(job, persist, aws_security_group_id=group_id)
    ec2.authorize_security_group_ingress(
        GroupId=group_id,
        IpPermissions=[{
            'IpProtocol': 'tcp',
            'FromPort': 22,
            'ToPort': 22,
            'IpRanges': [{
                'CidrIp': SSH_SOURCE_CIDR,
                'Description': 'Benchmark runner SSH access',
            }],
        }],
    )

    peer_group_id = None
    if iperf3_protocols:
        peer_group = ec2.create_security_group(
            GroupName=f'benchmark-peer-{suffix}',
            Description=f'Private iperf3 peer for benchmark job {suffix}',
            VpcId=vpc['VpcId'],
            TagSpecifications=[{
                'ResourceType': 'security-group',
                'Tags': [
                    *tags,
                    {'Key': 'benchmark-role', 'Value': 'iperf-peer'},
                ],
            }],
        )
        peer_group_id = peer_group['GroupId']
        _record(job, persist, aws_peer_security_group_id=peer_group_id)
        permissions = [{
            'IpProtocol': 'tcp',
            'FromPort': 5201,
            'ToPort': 5201,
            'UserIdGroupPairs': [{
                'GroupId': group_id,
                'Description': 'iperf3 control and TCP data from runner',
            }],
        }]
        if 'udp' in iperf3_protocols:
            permissions.append({
                'IpProtocol': 'udp',
                'FromPort': 5201,
                'ToPort': 5201,
                'UserIdGroupPairs': [{
                    'GroupId': group_id,
                    'Description': 'iperf3 UDP data from runner',
                }],
            })
        if 'sctp' in iperf3_protocols:
            # EC2 security groups represent SCTP as IP protocol 132. For a
            # non-TCP/UDP protocol AWS applies the rule to the whole protocol,
            # so deliberately omit FromPort/ToPort and scope it to the exact
            # runner security group instead.
            permissions.append({
                'IpProtocol': '132',
                'UserIdGroupPairs': [{
                    'GroupId': group_id,
                    'Description': 'iperf3 SCTP data from runner',
                }],
            })
        ec2.authorize_security_group_ingress(
            GroupId=peer_group_id,
            IpPermissions=permissions,
        )
        _emit(
            job,
            emit,
            'Provision',
            'Created a peer security group that accepts iperf3 traffic only '
            'from the benchmark runner security group.',
        )

    loadgen_group_id = None
    if web_benchmarks:
        loadgen_group_tags = [
            *tags,
            {'Key': 'benchmark-role', 'Value': 'load-generator'},
        ]
        _forget(
            job,
            persist,
            'aws_loadgen_security_group_reconciliation_error',
        )
        _record(
            job,
            persist,
            aws_loadgen_security_group_create_ambiguous=True,
        )
        try:
            loadgen_group = ec2.create_security_group(
                GroupName=f'benchmark-loadgen-{suffix}',
                Description=(
                    f'Web benchmark load generator for job {suffix}'
                ),
                VpcId=vpc['VpcId'],
                TagSpecifications=[{
                    'ResourceType': 'security-group',
                    'Tags': loadgen_group_tags,
                }],
            )
        except Exception as exc:
            if _confirmed_client_rejection(exc):
                _forget(
                    job,
                    persist,
                    *LOADGEN_SECURITY_GROUP_CONTRACT_KEYS,
                )
            raise
        loadgen_group_id = loadgen_group['GroupId']
        _record(
            job,
            persist,
            aws_loadgen_security_group_id=loadgen_group_id,
            aws_loadgen_security_group_create_ambiguous=False,
        )
        ec2.authorize_security_group_ingress(
            GroupId=loadgen_group_id,
            IpPermissions=[{
                'IpProtocol': 'tcp',
                'FromPort': 22,
                'ToPort': 22,
                'IpRanges': [{
                    'CidrIp': SSH_SOURCE_CIDR,
                    'Description': 'Web load-generator SSH access',
                }],
            }],
        )
        web_permissions = []
        if 'apachebench' in web_benchmarks:
            web_permissions.append({
                'IpProtocol': 'tcp',
                'FromPort': 80,
                'ToPort': 80,
                'UserIdGroupPairs': [{
                    'GroupId': loadgen_group_id,
                    'Description': 'ApacheBench traffic from load generator',
                }],
            })
        if 'deathstarbench' in web_benchmarks:
            for port in (5000, 8080):
                web_permissions.append({
                    'IpProtocol': 'tcp',
                    'FromPort': port,
                    'ToPort': port,
                    'UserIdGroupPairs': [{
                        'GroupId': loadgen_group_id,
                        'Description': (
                            'DeathStarBench traffic from load generator'
                        ),
                    }],
                })
        ec2.authorize_security_group_ingress(
            GroupId=group_id,
            IpPermissions=web_permissions,
        )
        _emit(
            job,
            emit,
            'Provision',
            'Created a dedicated load-generator security group; web service '
            'ports accept traffic only from that group.',
        )

    key_name = f'benchmark-{suffix}'
    _forget(job, persist, 'aws_key_pair_reconciliation_error')
    _record(
        job,
        persist,
        aws_key_pair_name=key_name,
        aws_key_pair_import_ambiguous=True,
    )
    try:
        imported_key = ec2.import_key_pair(
            KeyName=key_name,
            PublicKeyMaterial=public_key.encode('utf-8'),
            TagSpecifications=tag_specifications('key-pair'),
        )
    except Exception as exc:
        if _confirmed_client_rejection(exc):
            _forget(
                job,
                persist,
                'aws_key_pair_name',
                *KEY_PAIR_CONTRACT_KEYS,
            )
        raise
    key_values = {'aws_key_pair_name': key_name}
    if imported_key.get('KeyPairId'):
        key_values['aws_key_pair_id'] = imported_key['KeyPairId']
    key_values['aws_key_pair_import_ambiguous'] = False
    _record(job, persist, **key_values)

    storage = _value(plan, 'storage', {}) or {}
    boot_size_gb = int(_value(storage, 'boot_size_gb', 100) or 100)
    if bool(_value(storage, 'additional_volume', False)):
        data_size_gb = int(_value(storage, 'additional_size_gb', 1024) or 1024)
        volume_token = (
            resources.get('aws_data_volume_client_token') or uuid.uuid4().hex
        )
        # Persist the requested volume contract before CreateVolume. If its
        # response is lost, exact ownership tags allow cleanup to recover the
        # volume without relying on a broad account search.
        _record(
            job,
            persist,
            aws_data_volume_client_token=volume_token,
            aws_data_volume_size_gb=data_size_gb,
            aws_data_volume_type='gp3',
            aws_data_volume_iops=DEFAULT_GP3_IOPS,
            aws_data_volume_throughput_mibps=(
                DEFAULT_GP3_THROUGHPUT_MIBPS
            ),
            aws_data_volume_device=DATA_VOLUME_DEVICE,
            aws_data_volume_availability_zone=availability_zone,
            aws_data_volume_create_ambiguous=True,
        )
        try:
            data_volume = ec2.create_volume(
                **_data_volume_create_request(
                    suffix,
                    availability_zone,
                    data_size_gb,
                    volume_token,
                )
            )
        except Exception as exc:
            if _confirmed_client_rejection(exc):
                # A definite 4xx rejection cannot have created a volume. Do
                # not make cleanup replay a request that AWS already rejected.
                _forget(job, persist, *DATA_VOLUME_CONTRACT_KEYS)
            raise
        data_volume_id = data_volume['VolumeId']
        _record(
            job,
            persist,
            aws_data_volume_id=data_volume_id,
            aws_data_volume_create_ambiguous=False,
        )
        _emit(
            job,
            emit,
            'Provision',
            f'Created {data_size_gb} GiB gp3 /data volume '
            f'{data_volume_id} at {DEFAULT_GP3_IOPS} IOPS and '
            f'{DEFAULT_GP3_THROUGHPUT_MIBPS} MiB/s.',
        )
        ec2.get_waiter('volume_available').wait(
            VolumeIds=[data_volume_id]
        )
    # Persist our own token before RunInstances.  If AWS accepts the request
    # but the response is lost, cleanup can reconcile the exact instance
    # without relying on a broad tag-only search.  Reuse a recorded token if
    # provisioning is retried in-process.
    client_token = (
        resources.get('aws_instance_client_token') or uuid.uuid4().hex
    )
    _record(job, persist, aws_instance_client_token=client_token)
    launch = ec2.run_instances(
        ClientToken=client_token,
        ImageId=image['image_id'],
        InstanceType=instance_type,
        KeyName=key_name,
        MinCount=1,
        MaxCount=1,
        NetworkInterfaces=[{
            'DeviceIndex': 0,
            'SubnetId': subnet_id,
            'Groups': [group_id],
            'AssociatePublicIpAddress': True,
            'DeleteOnTermination': True,
        }],
        BlockDeviceMappings=[{
            'DeviceName': image['root_device_name'],
            'Ebs': {
                'DeleteOnTermination': True,
                'Encrypted': True,
                'VolumeSize': boot_size_gb,
                'VolumeType': 'gp3',
            },
        }],
        MetadataOptions={
            'HttpEndpoint': 'enabled',
            'HttpTokens': 'required',
        },
        TagSpecifications=[
            *tag_specifications('instance'),
            *tag_specifications('volume'),
        ],
    )
    instance = launch['Instances'][0]
    instance_id = instance['InstanceId']
    _record(
        job,
        persist,
        aws_instance_id=instance_id,
        instance_id=instance_id,
    )
    _emit(
        job,
        emit,
        'Provision',
        f'Launched AWS {instance_type}; waiting for it to become ready.',
    )
    peer_instance_id = None
    if iperf3_protocols:
        peer_client_token = (
            resources.get('aws_peer_instance_client_token')
            or uuid.uuid4().hex
        )
        _record(
            job,
            persist,
            aws_peer_instance_client_token=peer_client_token,
            aws_peer_instance_type=instance_type,
            aws_peer_image_id=image['image_id'],
        )
        peer_tags = [
            *tags,
            {'Key': 'benchmark-role', 'Value': 'iperf-peer'},
        ]
        peer_launch = ec2.run_instances(
            ClientToken=peer_client_token,
            ImageId=image['image_id'],
            InstanceType=instance_type,
            MinCount=1,
            MaxCount=1,
            UserData=amazon_linux.iperf3_peer_cloud_init(iperf3_protocols),
            NetworkInterfaces=[{
                'DeviceIndex': 0,
                'SubnetId': subnet_id,
                'Groups': [peer_group_id],
                # The address is used only for outbound AL2023 package setup;
                # no public ingress is allowed. Benchmark traffic targets the
                # private address recorded below.
                'AssociatePublicIpAddress': True,
                'DeleteOnTermination': True,
            }],
            BlockDeviceMappings=[{
                'DeviceName': image['root_device_name'],
                'Ebs': {
                    'DeleteOnTermination': True,
                    'Encrypted': True,
                    'VolumeSize': PEER_BOOT_SIZE_GB,
                    'VolumeType': 'gp3',
                },
            }],
            MetadataOptions={
                'HttpEndpoint': 'enabled',
                'HttpTokens': 'required',
            },
            TagSpecifications=[
                {'ResourceType': 'instance', 'Tags': peer_tags},
                {'ResourceType': 'volume', 'Tags': peer_tags},
            ],
        )
        peer_instance_id = peer_launch['Instances'][0]['InstanceId']
        _record(job, persist, aws_peer_instance_id=peer_instance_id)
        _emit(
            job,
            emit,
            'Provision',
            f'Launched same-type AWS iperf3 peer {instance_type} in '
            f'{availability_zone}; waiting for it to become ready.',
        )
    loadgen_instance_id = None
    if web_benchmarks:
        loadgen_client_token = (
            resources.get('aws_loadgen_instance_client_token')
            or uuid.uuid4().hex
        )
        _record(
            job,
            persist,
            aws_loadgen_instance_client_token=loadgen_client_token,
        )
        loadgen_tags = [
            *tags,
            {'Key': 'benchmark-role', 'Value': 'load-generator'},
        ]
        loadgen_launch = ec2.run_instances(
            ClientToken=loadgen_client_token,
            ImageId=loadgen_image['image_id'],
            InstanceType=loadgen_type_details['instance_type'],
            KeyName=key_name,
            MinCount=1,
            MaxCount=1,
            NetworkInterfaces=[{
                'DeviceIndex': 0,
                'SubnetId': subnet_id,
                'Groups': [loadgen_group_id],
                'AssociatePublicIpAddress': True,
                'DeleteOnTermination': True,
            }],
            BlockDeviceMappings=[{
                'DeviceName': loadgen_image['root_device_name'],
                'Ebs': {
                    'DeleteOnTermination': True,
                    'Encrypted': True,
                    'VolumeSize': LOAD_GENERATOR_BOOT_SIZE_GB,
                    'VolumeType': 'gp3',
                },
            }],
            MetadataOptions={
                'HttpEndpoint': 'enabled',
                'HttpTokens': 'required',
            },
            TagSpecifications=[
                {'ResourceType': 'instance', 'Tags': loadgen_tags},
                {'ResourceType': 'volume', 'Tags': loadgen_tags},
            ],
        )
        loadgen_instance_id = loadgen_launch['Instances'][0]['InstanceId']
        _record(
            job,
            persist,
            aws_loadgen_instance_id=loadgen_instance_id,
            loadgen_instance_id=loadgen_instance_id,
        )
        _emit(
            job,
            emit,
            'Provision',
            f'Launched separate AWS '
            f'{loadgen_type_details["instance_type"]} x86 load generator in '
            f'{availability_zone}; waiting for it to become ready.',
        )
    waiter_kwargs = {'InstanceIds': [instance_id]}
    if type_details['bare_metal']:
        # Bare-metal hosts can require 20 minutes or more to complete their
        # hardware/firmware boot checks. Keep this bounded, but do not apply the
        # ordinary botocore 10-minute waiter ceiling to those selectable types.
        waiter_kwargs['WaiterConfig'] = {
            'Delay': BARE_METAL_WAITER_DELAY_SECONDS,
            'MaxAttempts': BARE_METAL_WAITER_MAX_ATTEMPTS,
        }
    ec2.get_waiter('instance_running').wait(**waiter_kwargs)
    peer_waiter_kwargs = None
    if peer_instance_id:
        peer_waiter_kwargs = {'InstanceIds': [peer_instance_id]}
        if type_details['bare_metal']:
            peer_waiter_kwargs['WaiterConfig'] = {
                'Delay': BARE_METAL_WAITER_DELAY_SECONDS,
                'MaxAttempts': BARE_METAL_WAITER_MAX_ATTEMPTS,
            }
        ec2.get_waiter('instance_running').wait(**peer_waiter_kwargs)
    loadgen_waiter_kwargs = None
    if loadgen_instance_id:
        loadgen_waiter_kwargs = {'InstanceIds': [loadgen_instance_id]}
        ec2.get_waiter('instance_running').wait(**loadgen_waiter_kwargs)
    data_volume_id = resources.get('aws_data_volume_id')
    if data_volume_id:
        ec2.attach_volume(
            Device=DATA_VOLUME_DEVICE,
            InstanceId=instance_id,
            VolumeId=data_volume_id,
        )
        _record(
            job,
            persist,
            aws_data_volume_attached_instance_id=instance_id,
        )
        ec2.get_waiter('volume_in_use').wait(VolumeIds=[data_volume_id])
        _emit(
            job,
            emit,
            'Provision',
            f'Attached gp3 /data volume {data_volume_id} to {instance_id}.',
        )
    ec2.get_waiter('instance_status_ok').wait(**waiter_kwargs)
    if peer_waiter_kwargs:
        ec2.get_waiter('instance_status_ok').wait(**peer_waiter_kwargs)
    if loadgen_waiter_kwargs:
        ec2.get_waiter('instance_status_ok').wait(**loadgen_waiter_kwargs)
    description = ec2.describe_instances(InstanceIds=[instance_id])
    ready = description['Reservations'][0]['Instances'][0]
    _record(
        job,
        persist,
        public_ip=ready.get('PublicIpAddress'),
        private_ip=ready.get('PrivateIpAddress'),
    )
    if not resources.get('public_ip'):
        raise RuntimeError(
            f'AWS instance {instance_id} reached running state without a public IP.'
        )
    if peer_instance_id:
        peer_description = ec2.describe_instances(
            InstanceIds=[peer_instance_id]
        )
        peer_ready = peer_description['Reservations'][0]['Instances'][0]
        _record(
            job,
            persist,
            peer_private_ip=peer_ready.get('PrivateIpAddress'),
        )
        if not resources.get('peer_private_ip'):
            raise RuntimeError(
                f'AWS iperf3 peer {peer_instance_id} reached running state '
                'without a private IP.'
            )
    if loadgen_instance_id:
        loadgen_description = ec2.describe_instances(
            InstanceIds=[loadgen_instance_id]
        )
        loadgen_ready = loadgen_description['Reservations'][0]['Instances'][0]
        loadgen_public_ip = loadgen_ready.get('PublicIpAddress')
        loadgen_private_ip = loadgen_ready.get('PrivateIpAddress')
        _record(
            job,
            persist,
            aws_loadgen_public_ip=loadgen_public_ip,
            aws_loadgen_private_ip=loadgen_private_ip,
            loadgen_public_ip=loadgen_public_ip,
            loadgen_private_ip=loadgen_private_ip,
        )
        if not loadgen_public_ip or not loadgen_private_ip:
            raise RuntimeError(
                f'AWS load generator {loadgen_instance_id} reached running '
                'state without both public and private IP addresses.'
            )
    _emit(
        job,
        emit,
        'Provision',
        f'AWS infrastructure is ready. Public IP: {resources["public_ip"]}',
    )
    return resources


def _error_code(error):
    if isinstance(error, ClientError):
        return error.response.get('Error', {}).get('Code')
    if isinstance(error, WaiterError):
        return (error.last_response or {}).get('Error', {}).get('Code')
    return None


def _delete_with_retry(
    operation,
    *,
    gone_codes,
    retry_codes=('DependencyViolation', 'InvalidGroup.InUse'),
    attempts=6,
    delay_seconds=2,
):
    for attempt in range(1, attempts + 1):
        try:
            operation()
            return
        except ClientError as exc:
            code = _error_code(exc)
            if code in gone_codes:
                return
            if code in retry_codes and attempt < attempts:
                time.sleep(delay_seconds * attempt)
                continue
            raise


def _instances_for_client_token(ec2, client_token, job_id, *, role=None):
    """Resolve one ambiguous RunInstances response without a broad search."""

    filters = [
        {'Name': 'client-token', 'Values': [client_token]},
        {'Name': 'tag:benchmark-job', 'Values': [str(job_id)]},
        {'Name': 'tag:managed-by', 'Values': [MANAGED_BY]},
    ]
    if role:
        filters.append({'Name': 'tag:benchmark-role', 'Values': [role]})
    instances = {}
    request = {'Filters': filters}
    while True:
        response = ec2.describe_instances(**request)
        for reservation in response.get('Reservations', []):
            for instance in reservation.get('Instances', []):
                instance_id = instance.get('InstanceId')
                if instance_id:
                    instances[instance_id] = instance
        next_token = response.get('NextToken')
        if not next_token:
            break
        request['NextToken'] = next_token
    if len(instances) > 1:
        raise RuntimeError(
            'Refusing AWS cleanup because the recorded EC2 client token and '
            f'benchmark tags matched {len(instances)} instances. No resources '
            'were changed; inspect the AWS account before retrying.'
        )
    return list(instances.values())


def _ownership_filters(job_id):
    return [
        {'Name': 'tag:benchmark-job', 'Values': [str(job_id)]},
        {'Name': 'tag:managed-by', 'Values': [MANAGED_BY]},
    ]


def _tagged_candidates(
    ec2,
    operation_name,
    result_key,
    id_key,
    job_id,
    *,
    gone_codes=(),
    **kwargs,
):
    """Describe every resource carrying both exact ownership tags."""

    operation = getattr(ec2, operation_name)
    request = {
        **kwargs,
        'Filters': [
            *kwargs.get('Filters', []),
            *_ownership_filters(job_id),
        ],
    }
    candidates = {}
    while True:
        try:
            response = operation(**request)
        except ClientError as exc:
            if _error_code(exc) in gone_codes:
                return []
            raise
        if not isinstance(response, Mapping):
            break
        items = response.get(result_key, [])
        if not isinstance(items, (list, tuple)):
            items = []
        for item in items:
            resource_id = item.get(id_key)
            if not resource_id:
                raise RuntimeError(
                    f'AWS returned a tagged {result_key} item without {id_key}; '
                    'refusing cleanup before changing any resources.'
                )
            candidates[resource_id] = item
        next_token = response.get('NextToken')
        if not isinstance(next_token, str) or not next_token:
            break
        request['NextToken'] = next_token
    return list(candidates.values())


def _single_tagged_candidate(
    ec2,
    operation_name,
    result_key,
    id_key,
    job_id,
    label,
    *,
    gone_codes=(),
    **kwargs,
):
    candidates = _tagged_candidates(
        ec2,
        operation_name,
        result_key,
        id_key,
        job_id,
        gone_codes=gone_codes,
        **kwargs,
    )
    if len(candidates) > 1:
        identifiers = ', '.join(
            sorted(str(item[id_key]) for item in candidates)
        )
        raise RuntimeError(
            f'Refusing AWS cleanup because this run\'s exact ownership tags '
            f'matched {len(candidates)} {label} resources ({identifiers}). '
            'No resources were changed; inspect the AWS account before retrying.'
        )
    return candidates[0] if candidates else None


def _bounded_ambiguous_tag_lookup(
    ec2,
    operation_name,
    result_key,
    id_key,
    job_id,
    label,
    *,
    gone_codes=(),
    attempts=AMBIGUOUS_TAG_LOOKUP_ATTEMPTS,
    delay_seconds=AMBIGUOUS_TAG_LOOKUP_DELAY_SECONDS,
    **kwargs,
):
    """Retry one exact dual-tag lookup across EC2's consistency window."""

    for attempt in range(1, attempts + 1):
        candidate = _single_tagged_candidate(
            ec2,
            operation_name,
            result_key,
            id_key,
            job_id,
            label,
            gone_codes=gone_codes,
            **kwargs,
        )
        if candidate is not None:
            return candidate
        if attempt < attempts:
            time.sleep(delay_seconds * attempt)
    return None


def _reconcile_cleanup_manifest(ec2, job, persist):
    """Recover exact IDs from response-lost tagged create operations.

    Searches are performed only when an ID is absent and require both the
    random job tag and the application ownership tag.  All candidate
    relationships are checked before the recovered IDs are persisted.
    """

    resources = job.setdefault('resources', {})
    job_id = str(job['id'])
    candidates = {}
    specifications = [
        (
            'aws_vpc_id',
            'VPC',
            'describe_vpcs',
            'Vpcs',
            'VpcId',
            {},
            'aws_vpc_create_ambiguous',
            'aws_vpc_reconciliation_error',
        ),
        (
            'aws_internet_gateway_id',
            'internet gateway',
            'describe_internet_gateways',
            'InternetGateways',
            'InternetGatewayId',
            {},
            'aws_internet_gateway_create_ambiguous',
            'aws_internet_gateway_reconciliation_error',
        ),
        (
            'aws_subnet_id',
            'subnet',
            'describe_subnets',
            'Subnets',
            'SubnetId',
            {},
            None,
            None,
        ),
        (
            'aws_route_table_id',
            'route table',
            'describe_route_tables',
            'RouteTables',
            'RouteTableId',
            {},
            'aws_route_table_create_ambiguous',
            'aws_route_table_reconciliation_error',
        ),
        (
            'aws_security_group_id',
            'security group',
            'describe_security_groups',
            'SecurityGroups',
            'GroupId',
            {
                'Filters': [{
                    'Name': 'group-name',
                    'Values': [f'benchmark-{job_id}'],
                }],
            },
            None,
            None,
        ),
    ]
    plan = job.get('plan', {}) or {}
    peer_expected = bool(
        resources.get('aws_peer_instance_client_token')
        or resources.get('aws_peer_instance_id')
        or resources.get('aws_peer_security_group_id')
        or 'iperf3' in set(_value(plan, 'benchmarks', ()) or ())
    )
    if peer_expected:
        specifications.append((
            'aws_peer_security_group_id',
            'iperf3 peer security group',
            'describe_security_groups',
            'SecurityGroups',
            'GroupId',
            {
                'Filters': [{
                    'Name': 'tag:benchmark-role',
                    'Values': ['iperf-peer'],
                }],
            },
            None,
            None,
        ))
    loadgen_expected = bool(
        resources.get('aws_loadgen_instance_client_token')
        or resources.get('aws_loadgen_instance_id')
        or resources.get('aws_loadgen_security_group_id')
        or resources.get('aws_loadgen_security_group_create_ambiguous') is True
        or _web_benchmarks(plan)
    )
    if loadgen_expected:
        specifications.append((
            'aws_loadgen_security_group_id',
            'web load-generator security group',
            'describe_security_groups',
            'SecurityGroups',
            'GroupId',
            {
                'Filters': [
                    {
                        'Name': 'group-name',
                        'Values': [f'benchmark-loadgen-{job_id}'],
                    },
                    {
                        'Name': 'tag:benchmark-role',
                        'Values': ['load-generator'],
                    },
                ],
            },
            'aws_loadgen_security_group_create_ambiguous',
            'aws_loadgen_security_group_reconciliation_error',
        ))
    for (
        resource_key,
        label,
        operation,
        result_key,
        id_key,
        query,
        ambiguity_key,
        reconciliation_error_key,
    ) in specifications:
        if resources.get(resource_key):
            continue
        lookup = (
            _bounded_ambiguous_tag_lookup
            if ambiguity_key and resources.get(ambiguity_key) is True
            else _single_tagged_candidate
        )
        candidate = lookup(
            ec2,
            operation,
            result_key,
            id_key,
            job_id,
            label,
            **query,
        )
        if candidate:
            if resource_key == 'aws_loadgen_security_group_id':
                expected_name = f'benchmark-loadgen-{job_id}'
                if candidate.get('GroupName') != expected_name:
                    raise RuntimeError(
                        'Refusing AWS cleanup because the response-lost web '
                        'load-generator security group does not match the '
                        f'deterministic name {expected_name}. No resources were '
                        'changed; inspect the AWS account before retrying.'
                    )
                _assert_owned_resource(
                    job_id,
                    label,
                    candidate.get(id_key),
                    candidate,
                    expected_role='load-generator',
                )
            candidates[resource_key] = candidate
        elif reconciliation_error_key and resources.get(ambiguity_key) is True:
            # Route-table requests have an idempotent replay below. The other
            # creates deliberately remain retryable rather than treating an
            # eventually-consistent zero match as proof that no resource exists.
            if resource_key != 'aws_route_table_id':
                _record(
                    job,
                    persist,
                    **{
                        reconciliation_error_key: (
                            'Unable to reconcile the response-lost AWS '
                            f'{label}: no exact ownership-tag match was visible '
                            f'after {AMBIGUOUS_TAG_LOOKUP_ATTEMPTS} bounded '
                            'attempts. Retry cleanup.'
                        )
                    },
                )

    if (
        resources.get('aws_route_table_client_token')
        and not resources.get('aws_route_table_id')
        and resources.get('aws_route_table_create_ambiguous') is True
        and not candidates.get('aws_route_table_id')
    ):
        try:
            contract_vpc_id = resources.get('aws_route_table_vpc_id')
            recovered_vpc = candidates.get('aws_vpc_id')
            current_vpc_id = resources.get('aws_vpc_id') or (
                recovered_vpc.get('VpcId') if recovered_vpc else None
            )
            if not contract_vpc_id or not current_vpc_id:
                raise RuntimeError(
                    'its owning VPC is missing from the saved manifest'
                )
            if str(contract_vpc_id) != str(current_vpc_id):
                raise RuntimeError(
                    'its saved VPC contract does not match the recorded or '
                    'reconciled VPC'
                )
            replayed = ec2.create_route_table(
                **_route_table_create_request(
                    job_id,
                    contract_vpc_id,
                    resources['aws_route_table_client_token'],
                )
            )
            route_table = replayed.get('RouteTable')
            if not isinstance(route_table, Mapping) or not route_table.get(
                'RouteTableId'
            ):
                raise RuntimeError('AWS did not return a route-table ID')
        except Exception as exc:
            _record(
                job,
                persist,
                aws_route_table_reconciliation_error=(
                    'Unable to reconcile the response-lost AWS route table: '
                    f'{exc}'
                ),
            )
        else:
            candidates['aws_route_table_id'] = route_table

    if (
        resources.get('aws_data_volume_client_token')
        and not resources.get('aws_data_volume_id')
        and resources.get('aws_data_volume_create_ambiguous') is True
    ):
        candidate = _single_tagged_candidate(
            ec2,
            'describe_volumes',
            'Volumes',
            'VolumeId',
            job_id,
            'gp3 data volume',
            Filters=[{
                'Name': 'tag:benchmark-role',
                'Values': ['data'],
            }],
        )
        if candidate:
            candidates['aws_data_volume_id'] = candidate
        else:
            # CreateVolume is idempotent for an identical ClientToken request.
            # Replaying the exact persisted contract closes the eventual-tag-
            # visibility gap after a lost response: AWS returns the original
            # volume, or creates one that this cleanup immediately owns and
            # deletes. A mismatched/tampered contract fails closed in AWS.
            try:
                availability_zone = (
                    resources.get('aws_data_volume_availability_zone')
                    or resources.get('availability_zone')
                )
                size_gb = resources.get('aws_data_volume_size_gb')
                if not availability_zone or not size_gb:
                    raise RuntimeError(
                        'its Availability Zone or size is missing from the '
                        'saved manifest'
                    )
                if (
                    resources.get('aws_data_volume_type', 'gp3') != 'gp3'
                    or int(resources.get(
                        'aws_data_volume_iops', DEFAULT_GP3_IOPS
                    )) != DEFAULT_GP3_IOPS
                    or int(resources.get(
                        'aws_data_volume_throughput_mibps',
                        DEFAULT_GP3_THROUGHPUT_MIBPS,
                    )) != DEFAULT_GP3_THROUGHPUT_MIBPS
                ):
                    raise RuntimeError(
                        'its saved gp3 performance contract is invalid'
                    )
                replayed = ec2.create_volume(
                    **_data_volume_create_request(
                        job_id,
                        availability_zone,
                        size_gb,
                        resources['aws_data_volume_client_token'],
                    )
                )
                volume_id = replayed.get('VolumeId')
                if not volume_id:
                    raise RuntimeError('AWS did not return a volume ID')
            except Exception as exc:
                _record(
                    job,
                    persist,
                    aws_data_volume_reconciliation_error=(
                        'Unable to reconcile the response-lost AWS gp3 data '
                        f'volume: {exc}'
                    ),
                )
            else:
                candidates['aws_data_volume_id'] = replayed
                _record(
                    job,
                    persist,
                    aws_data_volume_id=volume_id,
                    aws_data_volume_create_ambiguous=False,
                )

        if candidates.get('aws_data_volume_id'):
            _forget(job, persist, 'aws_data_volume_reconciliation_error')

    if not resources.get('aws_key_pair_id'):
        key_name = resources.get('aws_key_pair_name') or f'benchmark-{job_id}'
        lookup = (
            _bounded_ambiguous_tag_lookup
            if resources.get('aws_key_pair_import_ambiguous') is True
            else _single_tagged_candidate
        )
        candidate = lookup(
            ec2,
            'describe_key_pairs',
            'KeyPairs',
            'KeyPairId',
            job_id,
            'key pair',
            gone_codes=('InvalidKeyPair.NotFound',),
            Filters=[{'Name': 'key-name', 'Values': [key_name]}],
        )
        if candidate:
            candidates['aws_key_pair_id'] = candidate
        elif resources.get('aws_key_pair_import_ambiguous') is True:
            _record(
                job,
                persist,
                aws_key_pair_reconciliation_error=(
                    'Unable to reconcile the response-lost AWS key pair: no '
                    'exact ownership-tag and key-name match was visible after '
                    f'{AMBIGUOUS_TAG_LOOKUP_ATTEMPTS} bounded attempts. Retry '
                    'cleanup.'
                ),
            )

    recovered_vpc = candidates.get('aws_vpc_id')
    vpc_id = resources.get('aws_vpc_id') or (
        recovered_vpc.get('VpcId') if recovered_vpc else None
    )
    child_relationships = (
        ('aws_subnet_id', 'subnet', 'SubnetId'),
        ('aws_route_table_id', 'route table', 'RouteTableId'),
        ('aws_security_group_id', 'security group', 'GroupId'),
        (
            'aws_peer_security_group_id',
            'iperf3 peer security group',
            'GroupId',
        ),
        (
            'aws_loadgen_security_group_id',
            'web load-generator security group',
            'GroupId',
        ),
    )
    for resource_key, label, id_key in child_relationships:
        candidate = candidates.get(resource_key)
        if not candidate:
            continue
        if not vpc_id or candidate.get('VpcId') != vpc_id:
            raise RuntimeError(
                f'Refusing AWS cleanup because tagged {label} '
                f'{candidate.get(id_key, "unknown")} '
                'does not belong to the recorded or reconciled VPC. No '
                'resources were changed; inspect the AWS account before retrying.'
            )

    gateway = candidates.get('aws_internet_gateway_id')
    if gateway:
        attachments = gateway.get('Attachments') or []
        if attachments and (
            not vpc_id
            or any(
                attachment.get('VpcId') != vpc_id
                for attachment in attachments
            )
        ):
            raise RuntimeError(
                'Refusing AWS cleanup because the tagged internet gateway does '
                'not belong to the recorded or reconciled VPC. No resources '
                'were changed; inspect the AWS account before retrying.'
            )

    recovered = {}
    recovered_contracts = {
        'aws_vpc_id': (
            'aws_vpc_create_ambiguous',
            'aws_vpc_reconciliation_error',
        ),
        'aws_internet_gateway_id': (
            'aws_internet_gateway_create_ambiguous',
            'aws_internet_gateway_reconciliation_error',
        ),
        'aws_route_table_id': (
            'aws_route_table_create_ambiguous',
            'aws_route_table_reconciliation_error',
        ),
        'aws_key_pair_id': (
            'aws_key_pair_import_ambiguous',
            'aws_key_pair_reconciliation_error',
        ),
        'aws_loadgen_security_group_id': (
            'aws_loadgen_security_group_create_ambiguous',
            'aws_loadgen_security_group_reconciliation_error',
        ),
    }
    for resource_key, candidate in candidates.items():
        if resource_key == 'aws_key_pair_id':
            recovered['aws_key_pair_id'] = candidate['KeyPairId']
            recovered['aws_key_pair_name'] = candidate.get(
                'KeyName',
                resources.get('aws_key_pair_name') or f'benchmark-{job_id}',
            )
        else:
            id_keys = {
                'aws_vpc_id': 'VpcId',
                'aws_internet_gateway_id': 'InternetGatewayId',
                'aws_subnet_id': 'SubnetId',
                'aws_route_table_id': 'RouteTableId',
                'aws_security_group_id': 'GroupId',
                'aws_peer_security_group_id': 'GroupId',
                'aws_loadgen_security_group_id': 'GroupId',
                'aws_data_volume_id': 'VolumeId',
            }
            recovered[resource_key] = candidate[id_keys[resource_key]]
        contract = recovered_contracts.get(resource_key)
        if contract and resources.get(contract[0]) is True:
            # Persist the recovered ID and close its response-loss ambiguity in
            # the same state write so a restart can never observe false/no-ID.
            recovered[contract[0]] = False
            recovered[contract[1]] = None
    if recovered:
        _record(job, persist, **recovered)
    return recovered


def _describe_for_ownership(operation, gone_codes):
    try:
        return operation()
    except ClientError as exc:
        if _error_code(exc) in gone_codes:
            return None
        raise


def _first_item(response, key, *, item_id=None, id_key=None):
    if not response:
        return None
    items = response.get(key, [])
    if not isinstance(items, (list, tuple)):
        return None
    if item_id is None or id_key is None:
        return items[0] if items else None
    return next(
        (item for item in items if item.get(id_key) == item_id),
        None,
    )


def _assert_owned_resource(
    job_id,
    label,
    resource_id,
    item,
    *,
    expected_role=None,
):
    if item is None:
        # Exact-ID describe operations return NotFound for resources that are
        # already gone.  The later idempotent delete handles the same case.
        return
    tag_values = {
        tag.get('Key'): tag.get('Value')
        for tag in (item.get('Tags') or [])
        if tag.get('Key')
    }
    expected = {
        'benchmark-job': str(job_id),
        'managed-by': MANAGED_BY,
    }
    if expected_role:
        expected['benchmark-role'] = expected_role
    if any(tag_values.get(key) != value for key, value in expected.items()):
        raise RuntimeError(
            f'Refusing AWS cleanup because recorded {label} {resource_id} '
            'does not carry this run\'s required ownership and role tags. '
            'No resources were changed; inspect the saved run manifest and '
            'AWS account before retrying.'
        )


def _assert_reconciled_loadgen_instance(
    job_id,
    resources,
    instance,
):
    """Validate the complete persisted launch contract before adoption."""

    instance_id = instance.get('InstanceId')
    _assert_owned_resource(
        job_id,
        'response-lost web load-generator instance',
        instance_id or 'unknown',
        instance,
        expected_role='load-generator',
    )
    expected = {
        'client token': resources.get('aws_loadgen_instance_client_token'),
        'instance type': resources.get('aws_loadgen_instance_type'),
        'AMI': resources.get('aws_loadgen_image_id'),
        'Availability Zone': resources.get('aws_loadgen_availability_zone'),
        'VPC': resources.get('aws_vpc_id'),
        'subnet': resources.get('aws_subnet_id'),
        'security group': resources.get('aws_loadgen_security_group_id'),
    }
    missing = [label for label, value in expected.items() if not value]
    if missing:
        raise RuntimeError(
            'Refusing AWS cleanup because the response-lost web load-generator '
            'manifest is missing its exact launch contract '
            f'({", ".join(missing)}). No resources were changed; inspect the '
            'saved run manifest before retrying.'
        )

    actual_security_groups = {
        group.get('GroupId')
        for group in (instance.get('SecurityGroups') or [])
        if group.get('GroupId')
    }
    actual = {
        'client token': instance.get('ClientToken'),
        'instance type': instance.get('InstanceType'),
        'AMI': instance.get('ImageId'),
        'Availability Zone': (instance.get('Placement') or {}).get(
            'AvailabilityZone'
        ),
        'VPC': instance.get('VpcId'),
        'subnet': instance.get('SubnetId'),
        'security group': actual_security_groups,
    }
    expected_security_groups = {expected['security group']}
    mismatches = [
        label
        for label in (
            'client token',
            'instance type',
            'AMI',
            'Availability Zone',
            'VPC',
            'subnet',
        )
        if actual[label] != expected[label]
    ]
    if actual_security_groups != expected_security_groups:
        mismatches.append('security group')
    if mismatches:
        raise RuntimeError(
            'Refusing AWS cleanup because the client-token-matched web '
            'load-generator instance does not match its exact launch contract '
            f'({", ".join(mismatches)} differ). No resources were changed; '
            'inspect the saved run manifest and AWS account before retrying.'
        )


def _verify_cleanup_ownership(
    ec2,
    job_id,
    resources,
    *,
    verified_instance_ids=(),
):
    """Fail closed if an exact ID or dependency relationship is unexpected.

    The returned association ID is inferred only from the owned route table's
    explicit association to the owned subnet.
    """

    verified_instance_ids = set(verified_instance_ids)
    expected_roles = {
        'aws_peer_instance_id': 'iperf-peer',
        'aws_loadgen_instance_id': 'load-generator',
        'aws_peer_security_group_id': 'iperf-peer',
        'aws_loadgen_security_group_id': 'load-generator',
    }
    described_instances = {}
    for instance_key, label in (
        ('aws_instance_id', 'instance'),
        ('aws_peer_instance_id', 'iperf3 peer instance'),
        ('aws_loadgen_instance_id', 'web load-generator instance'),
    ):
        instance_id = resources.get(instance_key)
        if not instance_id or instance_id in verified_instance_ids:
            continue
        response = _describe_for_ownership(
            lambda value=instance_id: ec2.describe_instances(
                InstanceIds=[value]
            ),
            ('InvalidInstanceID.NotFound',),
        )
        instance = None
        if response:
            for reservation in response.get('Reservations', []):
                for candidate in reservation.get('Instances', []):
                    if candidate.get('InstanceId') == instance_id:
                        instance = candidate
                        break
                if instance:
                    break
        _assert_owned_resource(
            job_id,
            label,
            instance_id,
            instance,
            expected_role=expected_roles.get(instance_key),
        )
        described_instances[instance_key] = instance

    key_pair_id = resources.get('aws_key_pair_id')
    key_pair_name = resources.get('aws_key_pair_name')
    if key_pair_id or key_pair_name:
        kwargs = (
            {'KeyPairIds': [key_pair_id]}
            if key_pair_id
            else {'KeyNames': [key_pair_name]}
        )
        response = _describe_for_ownership(
            lambda: ec2.describe_key_pairs(**kwargs),
            ('InvalidKeyPair.NotFound',),
        )
        key_pair = _first_item(
            response,
            'KeyPairs',
            item_id=key_pair_id or key_pair_name,
            id_key='KeyPairId' if key_pair_id else 'KeyName',
        )
        _assert_owned_resource(
            job_id,
            'key pair',
            key_pair_id or key_pair_name,
            key_pair,
        )

    exact_resources = (
        (
            'aws_data_volume_id',
            'gp3 data volume',
            'describe_volumes',
            'VolumeIds',
            'Volumes',
            'VolumeId',
            ('InvalidVolume.NotFound',),
        ),
        (
            'aws_subnet_id',
            'subnet',
            'describe_subnets',
            'SubnetIds',
            'Subnets',
            'SubnetId',
            ('InvalidSubnetID.NotFound',),
        ),
        (
            'aws_route_table_id',
            'route table',
            'describe_route_tables',
            'RouteTableIds',
            'RouteTables',
            'RouteTableId',
            ('InvalidRouteTableID.NotFound',),
        ),
        (
            'aws_security_group_id',
            'security group',
            'describe_security_groups',
            'GroupIds',
            'SecurityGroups',
            'GroupId',
            ('InvalidGroup.NotFound',),
        ),
        (
            'aws_peer_security_group_id',
            'iperf3 peer security group',
            'describe_security_groups',
            'GroupIds',
            'SecurityGroups',
            'GroupId',
            ('InvalidGroup.NotFound',),
        ),
        (
            'aws_loadgen_security_group_id',
            'web load-generator security group',
            'describe_security_groups',
            'GroupIds',
            'SecurityGroups',
            'GroupId',
            ('InvalidGroup.NotFound',),
        ),
        (
            'aws_internet_gateway_id',
            'internet gateway',
            'describe_internet_gateways',
            'InternetGatewayIds',
            'InternetGateways',
            'InternetGatewayId',
            ('InvalidInternetGatewayID.NotFound',),
        ),
        (
            'aws_vpc_id',
            'VPC',
            'describe_vpcs',
            'VpcIds',
            'Vpcs',
            'VpcId',
            ('InvalidVpcID.NotFound',),
        ),
    )
    described = {}
    for (
        resource_key,
        label,
        operation_name,
        argument_name,
        result_key,
        id_key,
        gone_codes,
    ) in exact_resources:
        resource_id = resources.get(resource_key)
        if not resource_id:
            continue
        operation = getattr(ec2, operation_name)
        response = _describe_for_ownership(
            lambda op=operation, arg=argument_name, value=resource_id: op(
                **{arg: [value]}
            ),
            gone_codes,
        )
        item = _first_item(
            response,
            result_key,
            item_id=resource_id,
            id_key=id_key,
        )
        _assert_owned_resource(
            job_id,
            label,
            resource_id,
            item,
            expected_role=expected_roles.get(resource_key),
        )
        described[resource_key] = item

    data_volume = described.get('aws_data_volume_id')
    if data_volume is not None:
        volume_tags = {
            tag.get('Key'): tag.get('Value')
            for tag in (data_volume.get('Tags') or [])
            if tag.get('Key')
        }
        if volume_tags.get('benchmark-role') != 'data':
            raise RuntimeError(
                'Refusing AWS cleanup because the recorded gp3 data volume '
                'does not carry the benchmark-role=data tag. No resources '
                'were changed; inspect the saved run manifest and AWS account '
                'before retrying.'
            )
        instance_id = (
            resources.get('aws_instance_id')
            or resources.get('aws_data_volume_attached_instance_id')
        )
        foreign_attachments = [
            attachment
            for attachment in (data_volume.get('Attachments') or [])
            if (
                attachment.get('State') != 'detached'
                and (
                    not instance_id
                    or attachment.get('InstanceId') != instance_id
                )
            )
        ]
        if foreign_attachments:
            raise RuntimeError(
                'Refusing AWS cleanup because the recorded gp3 data volume '
                'is attached to an instance other than the recorded benchmark '
                'instance. No resources were changed; inspect the saved run '
                'manifest and AWS account before retrying.'
            )

    if (
        resources.get('aws_route_table_association_id')
        and not resources.get('aws_route_table_id')
    ):
        raise RuntimeError(
            'Refusing AWS cleanup because a route-table association is '
            'recorded without its owning route table. No resources were '
            'changed; inspect the saved run manifest before retrying.'
        )

    vpc_id = resources.get('aws_vpc_id')
    subnet_id = resources.get('aws_subnet_id')
    for instance_key, instance in described_instances.items():
        if instance is None:
            continue
        if (
            not vpc_id
            or not subnet_id
            or instance.get('VpcId') != vpc_id
            or instance.get('SubnetId') != subnet_id
        ):
            raise RuntimeError(
                'Refusing AWS cleanup because recorded '
                f'{instance_key} does not belong to the recorded VPC and '
                'subnet. No resources were changed; inspect the saved run '
                'manifest before retrying.'
            )
    for resource_key, label in (
        ('aws_subnet_id', 'subnet'),
        ('aws_route_table_id', 'route table'),
        ('aws_security_group_id', 'security group'),
        ('aws_peer_security_group_id', 'iperf3 peer security group'),
        (
            'aws_loadgen_security_group_id',
            'web load-generator security group',
        ),
    ):
        item = described.get(resource_key)
        if item is None:
            continue
        if not vpc_id or item.get('VpcId') != vpc_id:
            raise RuntimeError(
                f'Refusing AWS cleanup because recorded {label} '
                f'{resources.get(resource_key)} does not belong to recorded '
                f'VPC {vpc_id or "unknown"}. No resources were changed; '
                'inspect the saved run manifest before retrying.'
            )

    gateway = described.get('aws_internet_gateway_id')
    if gateway is not None:
        attachments = gateway.get('Attachments') or []
        if attachments and (
            not vpc_id
            or any(
                attachment.get('VpcId') != vpc_id
                for attachment in attachments
            )
        ):
            raise RuntimeError(
                'Refusing AWS cleanup because the recorded internet gateway is '
                'attached to a VPC other than the recorded VPC. No resources '
                'were changed; inspect the saved run manifest before retrying.'
            )

    route_table = described.get('aws_route_table_id')
    if route_table is None:
        return None
    subnet_id = resources.get('aws_subnet_id')
    association_id = resources.get('aws_route_table_association_id')
    explicit_associations = [
        association
        for association in (route_table.get('Associations') or [])
        if not association.get('Main')
    ]
    if not subnet_id:
        if explicit_associations:
            raise RuntimeError(
                'Refusing AWS cleanup because the recorded route table has an '
                'explicit association but no owned subnet is recorded. No '
                'resources were changed; inspect the saved run manifest before '
                'retrying.'
            )
        return None

    matching_subnet = [
        association
        for association in explicit_associations
        if association.get('SubnetId') == subnet_id
    ]
    foreign = [
        association
        for association in explicit_associations
        if association.get('SubnetId') != subnet_id
    ]
    if foreign or len(matching_subnet) > 1:
        raise RuntimeError(
            'Refusing AWS cleanup because the recorded route table has '
            'ambiguous or foreign explicit subnet associations. No resources '
            'were changed; inspect the saved run manifest before retrying.'
        )
    if association_id:
        matching_id = next(
            (
                association
                for association in explicit_associations
                if association.get('RouteTableAssociationId') == association_id
            ),
            None,
        )
        if matching_id is not None and matching_id.get('SubnetId') != subnet_id:
            raise RuntimeError(
                'Refusing AWS cleanup because the recorded route-table '
                f'association {association_id} does not belong to recorded '
                f'subnet {subnet_id}. No resources were changed; inspect the '
                'saved run manifest before retrying.'
            )
        if matching_id is None and explicit_associations:
            raise RuntimeError(
                'Refusing AWS cleanup because the recorded route-table '
                f'association {association_id} was replaced by a different '
                'explicit association. No resources were changed; inspect the '
                'saved run manifest before retrying.'
            )
        # No explicit associations means a prior disassociate request likely
        # succeeded before its local manifest update; the later NotFound-safe
        # delete remains idempotent.
        return None
    if matching_subnet:
        inferred = matching_subnet[0].get('RouteTableAssociationId')
        if not inferred:
            raise RuntimeError(
                'Refusing AWS cleanup because the matching route-table '
                'association has no ID. No resources were changed; inspect the '
                'AWS account before retrying.'
            )
        return inferred
    return None


def destroy_resources(
    job: dict[str, Any],
    *,
    emit: EventCallback | None = None,
    persist: PersistCallback | None = None,
    aws_session=None,
    preserve_status: bool = False,
):
    """Delete only the exact AWS resource IDs recorded for this job."""

    if AWS_DSB_GRAPH_KEY in job.get('resources', {}):
        return destroy_distributed_deathstarbench_candidate(
            job, emit=emit, persist=persist, aws_session=aws_session,
            preserve_status=preserve_status,
        )
    plan = job.get('plan', {})
    resources = job.setdefault('resources', {})
    profile = str(
        resources.get('aws_profile')
        or _value(plan, 'aws_profile', DEFAULT_PROFILE)
        or DEFAULT_PROFILE
    )
    region = str(
        resources.get('region')
        or _value(plan, 'region', DEFAULT_REGION)
        or DEFAULT_REGION
    )
    session = _session(profile, region, aws_session)
    ec2 = session.client('ec2', region_name=region)
    errors = []
    managed_resource_keys = (
        'aws_instance_id',
        'aws_instance_client_token',
        'aws_peer_instance_id',
        'aws_peer_instance_client_token',
        'aws_loadgen_instance_id',
        'aws_loadgen_instance_client_token',
        'aws_data_volume_id',
        'aws_data_volume_client_token',
        'aws_vpc_create_ambiguous',
        'aws_internet_gateway_create_ambiguous',
        'aws_route_table_client_token',
        'aws_route_table_create_ambiguous',
        'aws_key_pair_import_ambiguous',
        'aws_key_pair_id',
        'aws_key_pair_name',
        'aws_route_table_association_id',
        'aws_subnet_id',
        'aws_route_table_id',
        'aws_security_group_id',
        'aws_peer_security_group_id',
        'aws_loadgen_security_group_id',
        'aws_loadgen_security_group_create_ambiguous',
        'aws_internet_gateway_id',
        'aws_vpc_id',
    )
    if job.get('status') == 'destroyed' and not any(
        resources.get(key) for key in managed_resource_keys
    ):
        stale_loadgen_keys = [
            key
            for key in LOAD_GENERATOR_INSTANCE_CONTRACT_KEYS
            if key in resources
        ]
        if stale_loadgen_keys:
            _forget(job, persist, *stale_loadgen_keys)
        return resources
    expected_account = resources.get('aws_account_id')
    if expected_account:
        identity = session.client(
            'sts',
            region_name=region,
        ).get_caller_identity()
        current_account = identity.get('Account')
        if str(current_account or '') != str(expected_account):
            raise RuntimeError(
                'Refusing AWS cleanup because the local profile now resolves '
                f'to account {current_account or "unknown"}, but this run '
                f'created resources in account {expected_account}. Restore '
                'access to the original account and retry cleanup.'
            )

    # A tagged create may have succeeded even if its response never reached
    # the app. Reconcile only absent IDs and validate every recovered child
    # relationship before any cloud mutation.
    _reconcile_cleanup_manifest(ec2, job, persist)

    # RunInstances may have succeeded even when its response never reached the
    # app.  Resolve that narrow ambiguity before changing the job status or
    # deleting any dependency.  Never search when an exact instance ID exists.
    reconciled_instance_ids = set()
    launch_records = (
        (
            'aws_instance_id',
            'aws_instance_client_token',
            {'instance_id': None},
            None,
        ),
        (
            'aws_peer_instance_id',
            'aws_peer_instance_client_token',
            {},
            'iperf-peer',
        ),
        (
            'aws_loadgen_instance_id',
            'aws_loadgen_instance_client_token',
            {'loadgen_instance_id': None},
            'load-generator',
        ),
    )
    for instance_key, token_key, aliases, role in launch_records:
        client_token = resources.get(token_key)
        if resources.get(instance_key) or not client_token:
            continue
        matches = _instances_for_client_token(
            ec2,
            client_token,
            job['id'],
            role=role,
        )
        if matches:
            matched_instance = matches[0]
            instance_id = matched_instance['InstanceId']
            if instance_key == 'aws_loadgen_instance_id':
                _assert_reconciled_loadgen_instance(
                    job['id'],
                    resources,
                    matched_instance,
                )
            else:
                expected_vpc = resources.get('aws_vpc_id')
                expected_subnet = resources.get('aws_subnet_id')
                if (
                    expected_vpc
                    and matched_instance.get('VpcId') is not None
                    and matched_instance.get('VpcId') != expected_vpc
                ) or (
                    expected_subnet
                    and matched_instance.get('SubnetId') is not None
                    and matched_instance.get('SubnetId') != expected_subnet
                ):
                    raise RuntimeError(
                        'Refusing AWS cleanup because the client-token-matched '
                        'instance does not belong to the recorded VPC and '
                        'subnet. No resources were changed; inspect the AWS '
                        'account before retrying.'
                    )
            reconciled_instance_ids.add(instance_id)
            values = {instance_key: instance_id}
            values.update({key: instance_id for key in aliases})
            _record(
                job,
                persist,
                **values,
            )
    inferred_association_id = _verify_cleanup_ownership(
        ec2,
        job['id'],
        resources,
        verified_instance_ids=reconciled_instance_ids,
    )
    if inferred_association_id and not resources.get(
        'aws_route_table_association_id'
    ):
        _record(
            job,
            persist,
            aws_route_table_association_id=inferred_association_id,
        )
    if not preserve_status:
        job['status'] = 'destroying'
    _persist(job, persist)
    _emit(job, emit, 'Destroy', 'Removing AWS benchmark infrastructure.')

    loadgen_instance_id = resources.get('aws_loadgen_instance_id')
    if loadgen_instance_id:
        try:
            try:
                ec2.terminate_instances(InstanceIds=[loadgen_instance_id])
                ec2.get_waiter('instance_terminated').wait(
                    InstanceIds=[loadgen_instance_id]
                )
            except ClientError as exc:
                if _error_code(exc) != 'InvalidInstanceID.NotFound':
                    raise
            except WaiterError as exc:
                if _error_code(exc) != 'InvalidInstanceID.NotFound':
                    raise
            _forget(
                job,
                persist,
                *LOAD_GENERATOR_INSTANCE_CONTRACT_KEYS,
            )
            _emit(
                job,
                emit,
                'Destroy',
                f'Terminated AWS web load generator {loadgen_instance_id}.',
            )
        except Exception as exc:
            errors.append(
                'Unable to terminate AWS web load generator '
                f'{loadgen_instance_id}: {exc}'
            )

    peer_instance_id = resources.get('aws_peer_instance_id')
    if peer_instance_id:
        try:
            try:
                ec2.terminate_instances(InstanceIds=[peer_instance_id])
                ec2.get_waiter('instance_terminated').wait(
                    InstanceIds=[peer_instance_id]
                )
            except ClientError as exc:
                if _error_code(exc) != 'InvalidInstanceID.NotFound':
                    raise
            except WaiterError as exc:
                if _error_code(exc) != 'InvalidInstanceID.NotFound':
                    raise
            _forget(
                job,
                persist,
                'aws_peer_instance_id',
                'aws_peer_instance_client_token',
                'aws_peer_instance_type',
                'aws_peer_image_id',
                'peer_private_ip',
            )
            _emit(
                job,
                emit,
                'Destroy',
                f'Terminated AWS iperf3 peer {peer_instance_id}.',
            )
        except Exception as exc:
            errors.append(
                f'Unable to terminate AWS iperf3 peer '
                f'{peer_instance_id}: {exc}'
            )

    instance_id = resources.get('aws_instance_id')
    if instance_id:
        try:
            try:
                ec2.terminate_instances(InstanceIds=[instance_id])
                ec2.get_waiter('instance_terminated').wait(
                    InstanceIds=[instance_id]
                )
            except ClientError as exc:
                if _error_code(exc) != 'InvalidInstanceID.NotFound':
                    raise
            except WaiterError as exc:
                # A terminated instance can age out of DescribeInstances while
                # a waiter is polling; verify before treating that as failure.
                if _error_code(exc) != 'InvalidInstanceID.NotFound':
                    raise
            _forget(
                job,
                persist,
                'aws_instance_id',
                'instance_id',
                'public_ip',
                'private_ip',
                'aws_instance_client_token',
            )
            _emit(job, emit, 'Destroy', f'Terminated AWS instance {instance_id}.')
        except Exception as exc:  # continue so independent resources are retried
            errors.append(f'Unable to terminate AWS instance {instance_id}: {exc}')

    data_volume_id = resources.get('aws_data_volume_id')
    if data_volume_id:
        try:
            _delete_with_retry(
                lambda: ec2.delete_volume(VolumeId=data_volume_id),
                gone_codes=('InvalidVolume.NotFound',),
                retry_codes=('VolumeInUse',),
                attempts=10,
            )
            _forget(
                job,
                persist,
                'aws_data_volume_id',
                *DATA_VOLUME_CONTRACT_KEYS,
            )
            _emit(
                job,
                emit,
                'Destroy',
                f'Deleted AWS gp3 data volume {data_volume_id}.',
            )
        except Exception as exc:
            errors.append(
                f'Unable to delete AWS gp3 data volume '
                f'{data_volume_id}: {exc}'
            )

    key_pair_id = resources.get('aws_key_pair_id')
    key_pair_name = resources.get('aws_key_pair_name')
    unresolved_key_import = (
        resources.get('aws_key_pair_import_ambiguous') is True
        and not key_pair_id
    )
    if (key_pair_id or key_pair_name) and not unresolved_key_import:
        try:
            kwargs = (
                {'KeyPairId': key_pair_id}
                if key_pair_id
                else {'KeyName': key_pair_name}
            )
            _delete_with_retry(
                lambda: ec2.delete_key_pair(**kwargs),
                gone_codes=('InvalidKeyPair.NotFound',),
            )
            _forget(
                job,
                persist,
                'aws_key_pair_id',
                'aws_key_pair_name',
                *KEY_PAIR_CONTRACT_KEYS,
            )
        except Exception as exc:
            errors.append(f'Unable to delete AWS key pair: {exc}')

    association_id = resources.get('aws_route_table_association_id')
    if association_id:
        try:
            _delete_with_retry(
                lambda: ec2.disassociate_route_table(
                    AssociationId=association_id
                ),
                gone_codes=('InvalidAssociationID.NotFound',),
            )
            _forget(job, persist, 'aws_route_table_association_id')
        except Exception as exc:
            errors.append(f'Unable to disassociate AWS route table: {exc}')

    subnet_id = resources.get('aws_subnet_id')
    if subnet_id:
        try:
            _delete_with_retry(
                lambda: ec2.delete_subnet(SubnetId=subnet_id),
                gone_codes=('InvalidSubnetID.NotFound',),
            )
            _forget(job, persist, 'aws_subnet_id', 'availability_zone')
        except Exception as exc:
            errors.append(f'Unable to delete AWS subnet {subnet_id}: {exc}')

    route_table_id = resources.get('aws_route_table_id')
    if route_table_id:
        try:
            _delete_with_retry(
                lambda: ec2.delete_route_table(RouteTableId=route_table_id),
                gone_codes=('InvalidRouteTableID.NotFound',),
            )
            _forget(
                job,
                persist,
                'aws_route_table_id',
                *ROUTE_TABLE_CONTRACT_KEYS,
            )
        except Exception as exc:
            errors.append(
                f'Unable to delete AWS route table {route_table_id}: {exc}'
            )

    peer_group_id = resources.get('aws_peer_security_group_id')
    if peer_group_id:
        try:
            _delete_with_retry(
                lambda: ec2.delete_security_group(GroupId=peer_group_id),
                gone_codes=('InvalidGroup.NotFound',),
            )
            _forget(job, persist, 'aws_peer_security_group_id')
        except Exception as exc:
            errors.append(
                f'Unable to delete AWS iperf3 peer security group '
                f'{peer_group_id}: {exc}'
            )

    group_id = resources.get('aws_security_group_id')
    if group_id:
        try:
            _delete_with_retry(
                lambda: ec2.delete_security_group(GroupId=group_id),
                gone_codes=('InvalidGroup.NotFound',),
            )
            _forget(job, persist, 'aws_security_group_id')
        except Exception as exc:
            errors.append(f'Unable to delete AWS security group {group_id}: {exc}')

    loadgen_group_id = resources.get('aws_loadgen_security_group_id')
    if loadgen_group_id:
        try:
            _delete_with_retry(
                lambda: ec2.delete_security_group(GroupId=loadgen_group_id),
                gone_codes=('InvalidGroup.NotFound',),
            )
            _forget(
                job,
                persist,
                'aws_loadgen_security_group_id',
                *LOADGEN_SECURITY_GROUP_CONTRACT_KEYS,
            )
        except Exception as exc:
            errors.append(
                'Unable to delete AWS web load-generator security group '
                f'{loadgen_group_id}: {exc}'
            )

    gateway_id = resources.get('aws_internet_gateway_id')
    vpc_id = resources.get('aws_vpc_id')
    if gateway_id:
        try:
            # Attempt the exact recorded gateway/VPC pair even if a process
            # stopped between AttachInternetGateway and persisting the flag.
            if vpc_id:
                _delete_with_retry(
                    lambda: ec2.detach_internet_gateway(
                        InternetGatewayId=gateway_id,
                        VpcId=vpc_id,
                    ),
                    gone_codes=(
                        'Gateway.NotAttached',
                        'InvalidInternetGatewayID.NotFound',
                        'InvalidVpcID.NotFound',
                    ),
                )
                _forget(job, persist, 'aws_internet_gateway_attached')
            _delete_with_retry(
                lambda: ec2.delete_internet_gateway(
                    InternetGatewayId=gateway_id
                ),
                gone_codes=('InvalidInternetGatewayID.NotFound',),
            )
            _forget(
                job,
                persist,
                'aws_internet_gateway_id',
                'aws_internet_gateway_attached',
                *INTERNET_GATEWAY_CONTRACT_KEYS,
            )
        except Exception as exc:
            errors.append(
                f'Unable to delete AWS internet gateway {gateway_id}: {exc}'
            )

    vpc_absence_confirmed = False
    if vpc_id:
        try:
            _delete_with_retry(
                lambda: ec2.delete_vpc(VpcId=vpc_id),
                gone_codes=('InvalidVpcID.NotFound',),
            )
            _forget(
                job,
                persist,
                'aws_vpc_id',
                *VPC_CONTRACT_KEYS,
            )
            vpc_absence_confirmed = True
        except Exception as exc:
            errors.append(f'Unable to delete AWS VPC {vpc_id}: {exc}')

    if (
        vpc_absence_confirmed
        and not resources.get('aws_route_table_id')
        and resources.get('aws_route_table_create_ambiguous') is True
    ):
        # A non-main route table cannot outlive its VPC. If deleting the exact
        # owned VPC succeeded (or returned NotFound), an earlier zero-match plus
        # failed replay is now conclusively absent rather than still ambiguous.
        _forget(job, persist, *ROUTE_TABLE_CONTRACT_KEYS)

    if (
        vpc_absence_confirmed
        and not resources.get('aws_loadgen_security_group_id')
        and resources.get(
            'aws_loadgen_security_group_create_ambiguous'
        ) is True
    ):
        # A security group cannot outlive its VPC, so successful deletion of
        # the exact owned VPC conclusively closes a prior lost-create response.
        _forget(job, persist, *LOADGEN_SECURITY_GROUP_CONTRACT_KEYS)

    # Reconciliation failures do not prevent deletion of independent recorded
    # resources. Add them only after that work, and only if their durable
    # contracts remain unresolved.
    for error_key in (
        *CREATE_RECONCILIATION_ERROR_KEYS,
        'aws_data_volume_reconciliation_error',
    ):
        if resources.get(error_key):
            errors.append(resources[error_key])

    if errors:
        raise RuntimeError('; '.join(errors))
    # A zero-match reconciliation remains durable until all recorded launch
    # dependencies have been removed successfully.  At that point an accepted
    # instance could not still be attached to the recorded subnet/VPC.
    if resources.get('aws_instance_client_token'):
        _forget(job, persist, 'aws_instance_client_token')
    if resources.get('aws_peer_instance_client_token'):
        _forget(
            job,
            persist,
            'aws_peer_instance_client_token',
            'aws_peer_instance_type',
            'aws_peer_image_id',
            'peer_private_ip',
        )
    if any(
        key in resources for key in LOAD_GENERATOR_INSTANCE_CONTRACT_KEYS
    ):
        _forget(
            job,
            persist,
            *LOAD_GENERATOR_INSTANCE_CONTRACT_KEYS,
        )
    if resources.get('aws_data_volume_client_token'):
        _forget(job, persist, *DATA_VOLUME_CONTRACT_KEYS)
    for contract in (
        VPC_CONTRACT_KEYS,
        INTERNET_GATEWAY_CONTRACT_KEYS,
        ROUTE_TABLE_CONTRACT_KEYS,
        KEY_PAIR_CONTRACT_KEYS,
    ):
        if any(resources.get(key) is not None for key in contract):
            _forget(job, persist, *contract)
    if not preserve_status:
        job['status'] = 'destroyed'
        job['cleanup_error'] = None
        _persist(job, persist)
        _emit(job, emit, 'Complete', 'AWS infrastructure was destroyed.')
    return resources


# A shorter alias is convenient for provider registries.
cleanup = destroy_resources


__all__ = [
    'AMI_PARAMETER_PREFIX',
    'DATA_VOLUME_DEVICE',
    'DEFAULT_PROFILE',
    'DEFAULT_GP3_IOPS',
    'DEFAULT_GP3_THROUGHPUT_MIBPS',
    'DEFAULT_REGION',
    'SSH_SOURCE_CIDR',
    'bootstrap',
    'cleanup',
    'create_session',
    'destroy_resources',
    'instance_types',
    'latest_amazon_linux_2023_ami',
    'placement',
    'provision',
]


# Operator-only distributed infrastructure. This graph deliberately does not
# share the compact runner's flat ownership verifier or cleanup inventory.
AWS_DSB_GRAPH_KEY = 'aws_deathstarbench_candidate_graph'
AWS_DSB_SCHEMA_VERSION = 1
AWS_DSB_ADDRESSES = {
    'control': '10.240.1.10', 'application': '10.240.1.13',
    'load-generator': '10.240.2.10', 'database': '10.240.3.11',
    'cache': '10.240.3.12',
}
AWS_DSB_SUBNETS = {
    'management': '10.240.1.0/24', 'loadgen': '10.240.2.0/24',
    'data': '10.240.3.0/24',
}
AWS_DSB_SHAPES = {
    'control': 'm7i.large', 'database': 'm7i.xlarge',
    'cache': 'm7i.large', 'load-generator': 'm7i.large',
}
_AWS_DSB_TERMINAL_RESOURCE_KEYS = frozenset({
    'provider',
    'aws_distributed_candidate',
    'aws_profile',
    'region',
    'aws_account_id',
    'availability_zone',
    AWS_DSB_GRAPH_KEY,
    TOPOLOGY_MANIFEST_KEY,
    TOPOLOGY_FINGERPRINT_KEY,
    ROLE_NODE_INVENTORY_KEY,
    'ssh_user',
    'aws_dsb_database_volume_id',
    K3S_RUNTIME_JOURNAL_KEY,
    DEATHSTARBENCH_WORKLOAD_JOURNAL_KEY,
    DEATHSTARBENCH_EXECUTION_JOURNAL_KEY,
    DISTRIBUTED_NETWORK_QUALIFICATION_KEY,
    *(f'aws_dsb_{role.replace("-", "_")}_{address}'
      for role in AWS_DSB_ADDRESSES
      for address in ('private_ip', 'public_ip')),
})
AWS_ROCKY_OFFICIAL_OWNER_ID = '792107900819'
_DSB_KINDS = {
    'vpc': ('describe_vpcs', 'Vpcs', 'VpcId', 'create_vpc', 'Vpc'),
    'igw': ('describe_internet_gateways', 'InternetGateways', 'InternetGatewayId',
            'create_internet_gateway', 'InternetGateway'),
    'subnet': ('describe_subnets', 'Subnets', 'SubnetId', 'create_subnet', 'Subnet'),
    'route': ('describe_route_tables', 'RouteTables', 'RouteTableId',
              'create_route_table', 'RouteTable'),
    'eip': ('describe_addresses', 'Addresses', 'AllocationId', 'allocate_address', None),
    'nat': ('describe_nat_gateways', 'NatGateways', 'NatGatewayId',
            'create_nat_gateway', 'NatGateway'),
    'sg': ('describe_security_groups', 'SecurityGroups', 'GroupId',
           'create_security_group', None),
    'key': ('describe_key_pairs', 'KeyPairs', 'KeyPairId', 'import_key_pair', None),
    'volume': ('describe_volumes', 'Volumes', 'VolumeId', 'create_volume', None),
    'instance': ('describe_instances', 'Reservations', 'InstanceId',
                 'run_instances', 'Instances'),
}


def _dsb_fail(message):
    raise ResourceInventoryError(f'AWS distributed candidate: {message}')


def _dsb_ec2_client(session, region):
    """Use one-attempt EC2 calls for the replay-safe candidate controller.

    Several EC2 create APIs in this graph do not accept an idempotency token.
    The lifecycle controller, rather than botocore, must observe an uncertain
    response and reconcile the durable intent before any operator retry.
    """
    return session.client(
        'ec2',
        region_name=region,
        config=_DSB_NO_RETRY_CONFIG,
    )


def _dsb_tags(job_id, key):
    return _tags(job_id) + [{'Key': 'benchmark-role', 'Value': key}]


def _dsb_list(ec2, kind, filters):
    method, result, _, _, _ = _DSB_KINDS[kind]
    request = {'Filter' if kind == 'nat' else 'Filters': filters}
    if kind == 'key':
        request['IncludePublicKey'] = True
    items = []
    while True:
        response = getattr(ec2, method)(**request)
        page = response.get(result, [])
        if kind == 'instance':
            page = [i for r in page for i in r.get('Instances', [])]
            page = [i for i in page if i.get('State', {}).get('Name') != 'terminated']
        if kind == 'nat':
            page = [i for i in page if i.get('State') != 'deleted']
        items.extend(page)
        token = response.get('NextToken')
        if not token:
            return items
        request['NextToken'] = token


def _dsb_read_children(ec2, method, result_key, filters):
    request = {'Filters': filters}
    result = []
    while True:
        response = getattr(ec2, method)(**request)
        result.extend(response.get(result_key, []))
        if not response.get('NextToken'):
            return result
        request['NextToken'] = response['NextToken']


def _dsb_filters(job_id, key=None):
    filters = [
        {'Name': 'tag:benchmark-job', 'Values': [job_id]},
        {'Name': 'tag:managed-by', 'Values': [MANAGED_BY]},
    ]
    if key is not None:
        filters.append({'Name': 'tag:benchmark-role', 'Values': [key]})
    return filters


def _dsb_require_tags(item, job_id, key):
    tags = {tag['Key']: tag['Value'] for tag in item.get('Tags', [])}
    if any(tags.get(tag['Key']) != tag['Value'] for tag in _dsb_tags(job_id, key)):
        _dsb_fail(f'{key} ownership tags differ; refusing adoption/deletion.')


def _dsb_public_key_identity(value):
    """Compare SSH algorithm and wire-format key bytes, ignoring only comments."""
    if not isinstance(value, str):
        _dsb_fail('imported SSH public key is missing or malformed.')
    parts = value.split()
    if len(parts) < 2 or parts[0] not in ('ssh-rsa', 'ssh-ed25519'):
        _dsb_fail('imported SSH public key is missing or malformed.')
    try:
        wire = base64.b64decode(parts[1], validate=True)
    except (binascii.Error, ValueError):
        _dsb_fail('imported SSH public key is missing or malformed.')
    size = int.from_bytes(wire[:4], 'big')
    if len(wire) < 4 + size or wire[4:4 + size] != parts[0].encode('ascii'):
        _dsb_fail('imported SSH public key algorithm differs from its wire format.')
    return parts[0], wire


def _dsb_pin_image(ec2, pin, architecture):
    """Pin either Rocky's public AMI or its Marketplace copy exactly.

    The Rocky project-owned channel avoids a Marketplace entitlement mutation;
    the Marketplace channel remains supported when the account already has an
    agreement.  Neither path discovers a "latest" image or accepts terms.
    """
    required = {'image_id', 'owner_id', 'name', 'creation_date', 'product_code',
                'architecture', 'root_device_name'}
    if not isinstance(pin, dict) or set(pin) != required:
        raise ValueError('Supply an exact Rocky Linux 9 AMI pin: '
                         + ', '.join(sorted(required)))
    official_public = (
        pin.get('owner_id') == AWS_ROCKY_OFFICIAL_OWNER_ID
        and pin.get('product_code') is None
    )
    marketplace = (
        isinstance(pin.get('product_code'), str)
        and bool(pin['product_code'])
    )
    if (not re.fullmatch(r'ami-[0-9a-f]{8,17}', pin['image_id'])
            or not re.fullmatch(r'[0-9]{12}', pin['owner_id'])
            or pin['architecture'] != architecture
            or not (official_public or marketplace)
            or not re.search(r'rocky[-_ ]?9|rocky[-_ ]?linux[-_ ]?9', pin['name'], re.I)):
        raise ValueError('The explicit Rocky Linux 9 AMI pin is invalid.')
    images = ec2.describe_images(ImageIds=[pin['image_id']], Owners=[pin['owner_id']]).get('Images', [])
    if len(images) != 1:
        raise ValueError('The pinned Rocky Linux 9 AMI is unavailable.')
    image = images[0]
    fields = {'image_id': 'ImageId', 'owner_id': 'OwnerId', 'name': 'Name',
              'creation_date': 'CreationDate', 'architecture': 'Architecture',
              'root_device_name': 'RootDeviceName'}
    expected_product_codes = (
        []
        if official_public
        else [{
            'ProductCodeId': pin['product_code'],
            'ProductCodeType': 'marketplace',
        }]
    )
    if (any(image.get(api) != pin[key] for key, api in fields.items())
            or image.get('State') != 'available'
            or image.get('RootDeviceType') != 'ebs'
            or image.get('VirtualizationType') != 'hvm'
            or image.get('EnaSupport') is not True
            or (official_public and image.get('Public') is not True)
            or (image.get('ProductCodes') or []) != expected_product_codes):
        raise ValueError(
            'The Rocky Linux AMI does not match its complete approved pin.'
        )
    return copy.deepcopy(pin)


def _latest_official_rocky_linux_9_pin(ec2, architecture):
    """Resolve one immutable Rocky-owned public AMI using read-only calls.

    The normal application path deliberately excludes Marketplace images: it
    must never accept terms or depend on an account-specific subscription as
    part of provisioning.  The complete returned identity is re-read and
    persisted by the candidate controller before that controller performs its
    first mutation.
    """

    if architecture not in {'x86_64', 'arm64'}:
        raise ValueError('Rocky Linux 9 AMI resolution requires x86_64 or arm64.')
    request = {
        'Owners': [AWS_ROCKY_OFFICIAL_OWNER_ID],
        'Filters': [
            {'Name': 'state', 'Values': ['available']},
            {'Name': 'architecture', 'Values': [architecture]},
            {'Name': 'root-device-type', 'Values': ['ebs']},
            {'Name': 'virtualization-type', 'Values': ['hvm']},
            {'Name': 'is-public', 'Values': ['true']},
            {'Name': 'image-type', 'Values': ['machine']},
        ],
    }
    candidates = []
    while True:
        response = ec2.describe_images(**request)
        for image in response.get('Images', []):
            if (
                re.fullmatch(r'ami-[0-9a-f]{8,17}', str(image.get('ImageId', '')))
                and image.get('OwnerId') == AWS_ROCKY_OFFICIAL_OWNER_ID
                and image.get('State') == 'available'
                and image.get('Architecture') == architecture
                and image.get('RootDeviceType') == 'ebs'
                and image.get('VirtualizationType') == 'hvm'
                and image.get('ImageType') == 'machine'
                and image.get('EnaSupport') is True
                and image.get('Public') is True
                and not (image.get('ProductCodes') or [])
                and re.search(
                    r'rocky[-_ ]?9|rocky[-_ ]?linux[-_ ]?9',
                    str(image.get('Name', '')),
                    re.I,
                )
                and isinstance(image.get('CreationDate'), str)
                and bool(image['CreationDate'])
                and isinstance(image.get('RootDeviceName'), str)
                and image['RootDeviceName'].startswith('/dev/')
            ):
                candidates.append(image)
        token = response.get('NextToken')
        if not token:
            break
        request['NextToken'] = token
    if not candidates:
        raise RuntimeError(
            'No approved public Rocky Linux 9 AMI is available for '
            f'{architecture} in this AWS region.'
        )
    selected = max(
        candidates,
        key=lambda image: (image['CreationDate'], image['ImageId']),
    )
    return {
        'image_id': selected['ImageId'],
        'owner_id': selected['OwnerId'],
        'name': selected['Name'],
        'creation_date': selected['CreationDate'],
        'product_code': None,
        'architecture': selected['Architecture'],
        'root_device_name': selected['RootDeviceName'],
    }


def resolve_distributed_deathstarbench_image_pins(plan, *, aws_session=None):
    """Resolve complete public image pins without mutating the AWS account."""

    session = _session(_profile(plan), _region(plan), aws_session)
    ec2 = _dsb_ec2_client(session, _region(plan))
    application_architecture = _instance_type_details(
        ec2,
        _instance_type(plan),
    )['architecture']
    support = _latest_official_rocky_linux_9_pin(ec2, 'x86_64')
    application = (
        copy.deepcopy(support)
        if application_architecture == 'x86_64'
        else _latest_official_rocky_linux_9_pin(ec2, application_architecture)
    )
    return {
        'support_image': support,
        'application_image': application,
    }


def _dsb_role_subnet(role):
    return ('data' if role in ('database', 'cache') else
            'loadgen' if role == 'load-generator' else 'management')


def _dsb_contract_manifest(contract):
    return build_topology_manifest(
        DISTRIBUTED_TIERED_TOPOLOGY_ID, K3S_RUNTIME_ID,
        selected_shape=contract['shapes']['application'],
        selected_architecture=contract['application_image']['architecture'],
    )


def _dsb_inventory(contract, graph):
    manifest = _dsb_contract_manifest(contract)
    nodes = []
    for node in manifest.planned_inventory('aws').nodes:
        entry = graph.get(f'instance-{node.key}', {})
        data = graph.get('database-data', {})
        storage = tuple(replace(s, model='gp3', size_gb=100,
            provisioned_iops=3000, provisioned_throughput_mibps=125,
            device=None, provider_resource_id=data.get('id'),
            provider_resource_name=f"benchmark-{contract['job_id']}-database-data",
            lifecycle_status=data.get('status', 'planned')) for s in node.storage)
        nodes.append(replace(node,
            provider_resource_id=entry.get('id'),
            provider_resource_name=f"benchmark-{contract['job_id']}-{node.key}",
            private_addresses=(AWS_DSB_ADDRESSES[node.key],),
            public_addresses=tuple(entry.get('public_addresses', [])),
            zone=contract['zone'], shape=contract['shapes'][node.key],
            architecture=(contract['application_image']['architecture']
                if node.key == 'application' else 'x86_64'),
            storage=storage, lifecycle_status=entry.get('status', 'planned')))
    return RoleNodeInventory(provider='aws', topology_fingerprint=manifest.fingerprint,
                             nodes=tuple(nodes))


def _dsb_save(job, persist):
    state = job['resources'][AWS_DSB_GRAPH_KEY]
    persist_role_node_inventory(job['resources'], _dsb_inventory(state['contract'], state['graph']))
    _persist(job, persist)


def _dsb_load(job, *, allow_quiesced_load_generator=False):
    resources = job.get('resources', {})
    state = resources.get(AWS_DSB_GRAPH_KEY)
    if (resources.get('provider') != 'aws' or not isinstance(state, dict)
            or set(state) != {'schema_version', 'contract', 'graph'}
            or type(state.get('schema_version')) is not int
            or state.get('schema_version') != AWS_DSB_SCHEMA_VERSION):
        _dsb_fail('missing or unsupported candidate graph.')
    contract = state['contract']
    if (contract.get('job_id') != job.get('id')
            or contract.get('shapes') != {**AWS_DSB_SHAPES,
                'application': contract.get('shapes', {}).get('application')}
            or resources.get('aws_account_id') != contract.get('account_id')
            or resources.get('region') != contract.get('region')):
        _dsb_fail('saved provider identity or role contract differs.')
    manifest = _dsb_contract_manifest(contract)
    if (resources.get('deathstarbench_topology_manifest') != manifest.as_dict()
            or resources.get('deathstarbench_topology_fingerprint') != manifest.fingerprint
            or load_role_node_inventory(resources) != _dsb_inventory(contract, state['graph'])):
        _dsb_fail('topology or inventory differs from the candidate contract.')
    allowed = {'vpc', 'igw', 'public-route', 'private-route', 'eip', 'nat', 'key',
               'database-data', *('subnet-' + k for k in AWS_DSB_SUBNETS),
               *('sg-' + k for k in AWS_DSB_ADDRESSES),
               *('instance-' + k for k in AWS_DSB_ADDRESSES)}
    if set(state['graph']) != allowed:
        _dsb_fail('graph resource allowlist differs.')
    _dsb_validate_published_aliases(
        resources,
        contract,
        state['graph'],
        allow_quiesced_load_generator=allow_quiesced_load_generator,
    )
    return state


def _dsb_runtime_aliases(contract, graph):
    """Derive convenience addresses only from authoritative role inventory."""
    inventory = _dsb_inventory(contract, graph)
    aliases = {'ssh_user': 'rocky',
               'aws_dsb_database_volume_id': graph['database-data'].get('id')}
    for node in inventory.nodes:
        if len(node.private_addresses) != 1 or len(node.public_addresses) > 1:
            _dsb_fail(f'{node.key} has ambiguous runtime addresses.')
        prefix = 'aws_dsb_' + node.key.replace('-', '_')
        aliases[prefix + '_private_ip'] = node.private_addresses[0]
        aliases[prefix + '_public_ip'] = node.public_addresses[0] if node.public_addresses else None
    return aliases


def _dsb_validate_published_aliases(
    resources,
    contract,
    graph,
    *,
    required=False,
    allow_quiesced_load_generator=False,
):
    expected = _dsb_runtime_aliases(contract, graph)
    published = set(expected).intersection(resources)
    aliases_match = (
        published == set(expected)
        and all(resources[key] == value for key, value in expected.items())
    )
    if published and not aliases_match:
        loadgen_public_key = 'aws_dsb_load_generator_public_ip'
        loadgen_entry = graph.get('instance-load-generator', {})
        quiesced_ephemeral_address = (
            allow_quiesced_load_generator
            and DEATHSTARBENCH_EXECUTION_JOURNAL_KEY in resources
            and published == set(expected)
            and isinstance(loadgen_entry, Mapping)
            and loadgen_entry.get('status') in {
                'running', 'deleting', 'delete_ambiguous', 'deleted',
            }
            and loadgen_entry.get('id')
            and not loadgen_entry.get('public_addresses')
            and isinstance(resources.get(loadgen_public_key), str)
            and bool(resources[loadgen_public_key])
            and all(
                resources[key] == value
                for key, value in expected.items()
                if key != loadgen_public_key
            )
        )
        if not quiesced_ephemeral_address:
            _dsb_fail(
                'published runtime SSH/storage aliases differ from the '
                'authoritative graph.'
            )
    if required and published != set(expected):
        _dsb_fail('runtime SSH/storage aliases have not been cloud-validated and published.')
    return expected


def _dsb_candidate_projection(resources, *, require_aliases):
    if not isinstance(resources, Mapping) or resources.get('aws_distributed_candidate') is not True:
        _dsb_fail('runtime projection requires exact AWS candidate resource state.')
    graph_state = resources.get(AWS_DSB_GRAPH_KEY)
    if not isinstance(graph_state, dict) or not isinstance(graph_state.get('contract'), dict):
        _dsb_fail('runtime projection requires the complete candidate contract.')
    state = _dsb_load({'id': graph_state['contract'].get('job_id'), 'resources': resources})
    contract, graph = state['contract'], state['graph']
    if any(not isinstance(entry, dict) or entry.get('status') != 'running'
           or not isinstance(entry.get('id'), str) or not entry['id'] for entry in graph.values()):
        _dsb_fail('runtime projection requires all candidate resources in running state.')
    aliases = _dsb_validate_published_aliases(resources, contract, graph, required=require_aliases)
    inventory = load_role_node_inventory(resources)
    nodes = {}
    for node in inventory.nodes:
        public_required = node.key not in ('database', 'cache')
        if bool(node.public_addresses) != public_required:
            _dsb_fail(f'{node.key} runtime public-address policy differs.')
        prefix = 'aws_dsb_' + node.key.replace('-', '_')
        direct = node.key in ('control', 'load-generator')
        nodes[node.key] = {
            'key': node.key, 'role': node.role,
            'node_name': node.provider_resource_name,
            'instance_id': node.provider_resource_id,
            'shape': node.shape, 'architecture': node.architecture,
            'private_ip': node.private_addresses[0],
            'public_ip': node.public_addresses[0] if node.public_addresses else None,
            'host_key': prefix + ('_public_ip' if direct else '_private_ip'),
            'jump_host_key': None if direct else 'aws_dsb_control_public_ip',
        }
    if len({node['instance_id'] for node in nodes.values()}) != len(nodes):
        _dsb_fail('runtime projection has duplicate role instance identities.')
    return {
        'provider': 'aws', 'job_id': contract['job_id'], 'account_id': contract['account_id'],
        'region': contract['region'], 'zone': contract['zone'], 'ssh_user': 'rocky',
        'topology_fingerprint': inventory.topology_fingerprint,
        'database_volume_id': graph['database-data']['id'],
        'aliases': aliases, 'nodes': nodes,
        'support_image': copy.deepcopy(contract['support_image']),
        'application_image': copy.deepcopy(contract['application_image']),
    }


def distributed_deathstarbench_candidate_projection(resources):
    """Read-only local projection; require previously cloud-validated aliases.

    This performs no cloud calls and cannot itself prove live cloud ownership.
    The operator must call ``validate_distributed_deathstarbench_candidate``
    before initial or resumed SSH work. Changes to any published alias fail
    closed instead of silently redirecting a connection or disk operation.
    """
    return _dsb_candidate_projection(resources, require_aliases=True)


def validate_distributed_deathstarbench_candidate(job, *, persist=None, aws_session=None):
    """Reconcile/audit AWS read-only, then publish exact nonsecret SSH aliases."""
    objects = recover_distributed_deathstarbench_candidate(job, persist=persist, aws_session=aws_session)
    state = _dsb_load(job)
    contract, graph = state['contract'], state['graph']
    if set(objects) != set(graph):
        _dsb_fail('runtime validation requires the complete live cloud graph.')
    for role in AWS_DSB_ADDRESSES:
        if objects['instance-' + role].get('State', {}).get('Name') != 'running':
            _dsb_fail(f'{role} instance is not running for runtime validation.')
        expected = _dsb_rule_set(_dsb_permissions(contract, graph, role))
        if _dsb_rule_set(objects['sg-' + role].get('IpPermissions', [])) != expected:
            _dsb_fail(f'{role} required runtime ingress paths are incomplete.')
    if (objects['nat'].get('State') != 'available'
            or len(objects['nat'].get('NatGatewayAddresses', [])) != 1
            or len(objects['igw'].get('Attachments', [])) != 1):
        _dsb_fail('runtime egress gateway attachments are incomplete.')
    for key, names in (('public-route', ('management', 'loadgen')), ('private-route', ('data',))):
        table = objects[key]
        expected_subnets = {graph['subnet-' + name]['id'] for name in names}
        if ({a.get('SubnetId') for a in table.get('Associations', [])} != expected_subnets
                or len(table.get('Associations', [])) != len(expected_subnets)
                or len(table.get('Routes', [])) != 2):
            _dsb_fail(f'{key} runtime routes or associations are incomplete.')
    attachments = objects['database-data'].get('Attachments', [])
    if (len(attachments) != 1 or attachments[0].get('InstanceId') != graph['instance-database']['id']
            or attachments[0].get('Device') != DATA_VOLUME_DEVICE
            or attachments[0].get('State') != 'attached'):
        _dsb_fail('runtime database EBS attachment is missing or not ready.')
    database_disks = objects['instance-database'].get('BlockDeviceMappings', [])
    if not any(d.get('DeviceName') == DATA_VOLUME_DEVICE
               and d.get('Ebs', {}).get('VolumeId') == graph['database-data']['id']
               and d.get('Ebs', {}).get('DeleteOnTermination') is False for d in database_disks):
        _dsb_fail('runtime database EBS attachment is missing from the instance.')
    projection = _dsb_candidate_projection(job['resources'], require_aliases=False)
    # Publication is one local state write, only after the whole cloud graph
    # and its reconstructed inventory passed validation. Never repair drift.
    job['resources'].update(projection['aliases'])
    _persist(job, persist)
    return copy.deepcopy(projection)


def distributed_deathstarbench_candidate_deleted(job):
    """Strict local terminal predicate; retained IDs are deletion tombstones.

    This is not an independent cloud absence proof. An operator cleanup gate
    must first run read-only recovery/graph verification against the original
    AWS account, then require this predicate. Malformed or partial state is
    never advertised as safe to discard.
    """
    try:
        resources = job.get('resources') if isinstance(job, Mapping) else None
        if (not isinstance(resources, Mapping)
                or job.get('status') != 'destroyed'
                or job.get('cleanup_error')
                or resources.get('aws_distributed_candidate') is not True
                or not set(resources) <= _AWS_DSB_TERMINAL_RESOURCE_KEYS):
            return False
        state = _dsb_load(job)
        for key, entry in state['graph'].items():
            allowed = {'status', 'id', 'public_addresses'} if key.startswith('instance-') else {'status', 'id'}
            if (not isinstance(entry, dict) or not set(entry) <= allowed
                    or entry.get('status') != 'deleted'
                    or ('id' in entry and (not isinstance(entry['id'], str) or not entry['id']))):
                return False
        return True
    except (ResourceInventoryError, ValueError, KeyError, TypeError, AttributeError):
        return False


def _dsb_specs(contract, graph):
    """Derive requests from one immutable contract; missing dependencies stay None."""
    def ident(key):
        return graph[key].get('id')
    def spec(key, kind, body):
        resource_type = {'igw': 'internet-gateway', 'route': 'route-table',
            'eip': 'elastic-ip', 'nat': 'natgateway', 'sg': 'security-group',
            'key': 'key-pair'}.get(kind, kind)
        tags = _dsb_tags(contract['job_id'], key)
        body['TagSpecifications'] = [{'ResourceType': resource_type, 'Tags': tags}]
        if kind in ('route', 'nat', 'volume', 'instance'):
            body['ClientToken'] = hashlib.sha256(
                f"{contract['job_id']}:{contract['account_id']}:{contract['region']}:{key}".encode()
            ).hexdigest()
        return key, (kind, body)
    specs = [spec('vpc', 'vpc', {'CidrBlock': '10.240.0.0/16'}),
             spec('igw', 'igw', {})]
    for name, cidr in AWS_DSB_SUBNETS.items():
        specs.append(spec('subnet-' + name, 'subnet', {'VpcId': ident('vpc'),
            'CidrBlock': cidr, 'AvailabilityZone': contract['zone']}))
    for key in ('public-route', 'private-route'):
        specs.append(spec(key, 'route', {'VpcId': ident('vpc')}))
    specs.append(spec('eip', 'eip', {'Domain': 'vpc'}))
    specs.append(spec('nat', 'nat', {'SubnetId': ident('subnet-management'),
        'AllocationId': ident('eip'), 'ConnectivityType': 'public'}))
    for role in AWS_DSB_ADDRESSES:
        specs.append(spec('sg-' + role, 'sg', {'VpcId': ident('vpc'),
            'GroupName': f"benchmark-{contract['job_id']}-dsb-{role}",
            'Description': f'DeathStarBench distributed {role}'}))
    specs.append(spec('key', 'key', {'KeyName': f"benchmark-{contract['job_id']}-dsb",
        'PublicKeyMaterial': contract['public_key']}))
    specs.append(spec('database-data', 'volume', {'AvailabilityZone': contract['zone'],
        'Encrypted': True, 'Size': 100, 'VolumeType': 'gp3', 'Iops': 3000, 'Throughput': 125}))
    for role in _dsb_contract_manifest(contract).creation_order:
        pin = contract['application_image'] if role == 'application' else contract['support_image']
        body = {'ImageId': pin['image_id'], 'InstanceType': contract['shapes'][role],
            'MinCount': 1, 'MaxCount': 1, 'KeyName': f"benchmark-{contract['job_id']}-dsb",
            'Placement': {'AvailabilityZone': contract['zone']},
            'MetadataOptions': {'HttpTokens': 'required', 'HttpEndpoint': 'enabled'},
            'NetworkInterfaces': [{'DeviceIndex': 0,
                'SubnetId': ident('subnet-' + _dsb_role_subnet(role)),
                'PrivateIpAddress': AWS_DSB_ADDRESSES[role],
                'AssociatePublicIpAddress': role not in ('database', 'cache'),
                'Groups': [ident('sg-' + role)], 'DeleteOnTermination': True}],
            'BlockDeviceMappings': [{'DeviceName': pin['root_device_name'], 'Ebs': {
                'VolumeSize': 50, 'VolumeType': 'gp3', 'Encrypted': True,
                'Iops': 3000, 'Throughput': 125,
                'DeleteOnTermination': True}}]}
        key, value = spec('instance-' + role, 'instance', body)
        body['TagSpecifications'].append({'ResourceType': 'volume',
            'Tags': _dsb_tags(contract['job_id'], 'boot-' + role)})
        specs.append((key, value))
    return dict(specs)


def _dsb_verify_item(
    contract,
    graph,
    key,
    item,
    *,
    allow_attaching=False,
    allow_quiesced_load_generator=False,
):
    kind, request = _dsb_specs(contract, graph)[key]
    _dsb_require_tags(item, contract['job_id'], key)
    identifier = _DSB_KINDS[kind][2]
    if graph[key].get('id') and item.get(identifier) != graph[key]['id']:
        _dsb_fail(f'{key} identity changed.')
    fields = {
        'vpc': ('CidrBlock',), 'subnet': ('VpcId', 'CidrBlock', 'AvailabilityZone'),
        'route': ('VpcId',), 'eip': ('Domain',),
        'nat': ('SubnetId', 'ConnectivityType'),
        'sg': ('VpcId', 'GroupName', 'Description'), 'key': ('KeyName',),
        'volume': ('AvailabilityZone', 'Encrypted', 'Size', 'VolumeType', 'Iops', 'Throughput'),
        'instance': ('ImageId', 'InstanceType', 'KeyName', 'ClientToken'), 'igw': (),
    }[kind]
    if any(item.get(field) != request[field] for field in fields):
        _dsb_fail(f'{key} resource configuration differs.')
    if kind == 'key' and _dsb_public_key_identity(item.get('PublicKey')) != _dsb_public_key_identity(contract['public_key']):
        _dsb_fail('imported SSH public key differs from the immutable launch contract.')
    if kind == 'vpc' and (item.get('Ipv6CidrBlockAssociationSet') or any(
            a.get('CidrBlock') != '10.240.0.0/16'
            for a in item.get('CidrBlockAssociationSet', []))):
        _dsb_fail('VPC has an unexpected CIDR association.')
    if kind == 'nat' and any(a.get('AllocationId') != graph['eip'].get('id')
                             for a in item.get('NatGatewayAddresses', [])):
        _dsb_fail('NAT gateway allocation differs.')
    if kind == 'igw' and any(a.get('VpcId') != graph['vpc'].get('id')
                             for a in item.get('Attachments', [])):
        _dsb_fail('internet gateway is attached to another VPC.')
    if kind == 'instance':
        role = key.removeprefix('instance-')
        nic = request['NetworkInterfaces'][0]
        pin = contract['application_image'] if role == 'application' else contract['support_image']
        if (item.get('VpcId') != graph['vpc'].get('id')
                or item.get('SubnetId') != nic['SubnetId']
                or item.get('PrivateIpAddress') != nic['PrivateIpAddress']
                or item.get('Placement', {}).get('AvailabilityZone') != contract['zone']
                or item.get('RootDeviceName') != pin['root_device_name']
                or {s.get('GroupId') for s in item.get('SecurityGroups', [])} != set(nic['Groups'])
                or len(item.get('NetworkInterfaces', [])) != 1
                or item.get('MetadataOptions', {}).get('HttpTokens') != 'required'):
            _dsb_fail(f'{key} network placement differs.')
        primary = item['NetworkInterfaces'][0]
        public_ip = item.get('PublicIpAddress')
        association = primary.get('Association', {})
        if nic['AssociatePublicIpAddress']:
            quiesced_address_release = (
                allow_quiesced_load_generator
                and key == 'instance-load-generator'
                and item.get('State', {}).get('Name') in {
                    'stopping', 'stopped', 'shutting-down', 'terminated',
                }
                and not public_ip
                and not association
            )
            if (
                not quiesced_address_release
                and (
                    (
                        public_ip
                        and association.get('PublicIp')
                        and association['PublicIp'] != public_ip
                    )
                    or (
                        not allow_attaching
                        and not _dsb_public_address_ready(key, item)
                    )
                )
            ):
                _dsb_fail(f'{key} required public address/primary ENI association differs.')
        elif public_ip or association:
            _dsb_fail(f'{key} has an unexpected public address/primary ENI association.')
        attachment = primary.get('Attachment', {})
        allowed_statuses = ('attached', 'attaching') if allow_attaching else ('attached',)
        if (not primary.get('NetworkInterfaceId') or not attachment.get('AttachmentId')
                or attachment.get('DeviceIndex') != 0
                or attachment.get('Status') not in allowed_statuses
                or attachment.get('DeleteOnTermination') is not True):
            _dsb_fail(f'{key} primary ENI attachment/deletion contract differs.')
        devices = item.get('BlockDeviceMappings', [])
        root = [d for d in devices if d.get('DeviceName') == pin['root_device_name']]
        # EC2 can expose an accepted instance before its block-device mapping
        # is populated (the RunInstances response itself commonly does this).
        # Only the completely absent, still-converging view is tolerated, and
        # only on a path that will wait and then perform strict verification.
        boot_mapping_pending = allow_attaching and not devices
        if not boot_mapping_pending and (
                len(root) != 1
                or root[0].get('Ebs', {}).get('DeleteOnTermination') is not True
                or not root[0].get('Ebs', {}).get('VolumeId')):
            _dsb_fail(f'{key} boot disk deletion contract differs.')
        for device in devices:
            if device in root:
                continue
            if (role != 'database' or device.get('DeviceName') != DATA_VOLUME_DEVICE
                    or device.get('Ebs', {}).get('VolumeId') != graph['database-data'].get('id')
                    or device.get('Ebs', {}).get('DeleteOnTermination') is not False):
                _dsb_fail(f'{key} has an unexpected attached disk.')


def _dsb_discover(
    ec2,
    contract,
    graph,
    key,
    *,
    allow_attaching=False,
    allow_quiesced_load_generator=False,
):
    kind, _ = _dsb_specs(contract, graph)[key]
    if graph[key].get('id'):
        id_filter = {'igw': 'internet-gateway-id', 'route': 'route-table-id',
            'eip': 'allocation-id', 'nat': 'nat-gateway-id', 'sg': 'group-id',
            'key': 'key-pair-id'}.get(kind, kind + '-id')
        filters = [{'Name': id_filter, 'Values': [graph[key]['id']]}]
    else:
        filters = _dsb_filters(contract['job_id'], key)
    found = _dsb_list(ec2, kind, filters)
    if len(found) > 1:
        _dsb_fail(f'{key} reconciliation found multiple resources.')
    if not found:
        return None
    _dsb_verify_item(
        contract,
        graph,
        key,
        found[0],
        allow_attaching=allow_attaching,
        allow_quiesced_load_generator=allow_quiesced_load_generator,
    )
    return found[0]


def _dsb_exact_tags(item, expected):
    """Require one exact, duplicate-free tag set from an atomic create."""
    tags = item.get('Tags')
    if not isinstance(tags, list) or any(
            not isinstance(tag, Mapping)
            or set(tag) != {'Key', 'Value'}
            or not isinstance(tag['Key'], str)
            or not isinstance(tag['Value'], str)
            for tag in tags):
        return False
    pairs = [(tag['Key'], tag['Value']) for tag in tags]
    expected_pairs = [(tag['Key'], tag['Value']) for tag in expected]
    return len({key for key, _ in pairs}) == len(pairs) and sorted(pairs) == sorted(expected_pairs)


def _dsb_accept_igw_create_response(contract, graph, response):
    """Validate the complete atomic CreateInternetGateway result.

    Persisting this accepted identity before any follow-up Describe call avoids
    depending on EC2's eventually-consistent tag-filter index. A malformed or
    incomplete response remains ambiguous and is reconciled read-only.
    """
    if not isinstance(response, Mapping) or set(response) - {
            'InternetGateway', 'ResponseMetadata'}:
        return None
    gateway = response.get('InternetGateway')
    if (not isinstance(gateway, Mapping)
            or not isinstance(gateway.get('InternetGatewayId'), str)
            or not re.fullmatch(r'igw-[0-9a-f]+', gateway['InternetGatewayId'])
            or gateway.get('OwnerId') != contract['account_id']
            or gateway.get('Attachments') != []
            or not _dsb_exact_tags(gateway, _dsb_tags(contract['job_id'], 'igw'))):
        return None
    _dsb_verify_item(contract, graph, 'igw', gateway)
    return gateway


def _dsb_wait_for_primary_attachment(ec2, job, key, found, persist):
    """Re-observe an accepted instance; never re-dispatch RunInstances."""
    state = job['resources'][AWS_DSB_GRAPH_KEY]
    contract, graph = state['contract'], state['graph']
    entry = graph[key]
    role = key.removeprefix('instance-')
    pin = contract['application_image'] if role == 'application' else contract['support_image']
    def boot_mapping_ready(item):
        root = [d for d in item.get('BlockDeviceMappings', [])
                if d.get('DeviceName') == pin['root_device_name']]
        return (len(root) == 1
                and root[0].get('Ebs', {}).get('DeleteOnTermination') is True
                and bool(root[0].get('Ebs', {}).get('VolumeId')))
    if (found.get('State', {}).get('Name') == 'pending'
            or found['NetworkInterfaces'][0]['Attachment']['Status'] == 'attaching'
            or not _dsb_public_address_ready(key, found)
            or not boot_mapping_ready(found)):
        # Retain the accepted identity before a waiter can time out or the
        # operator process can be interrupted. Only attachment/address readiness is lax;
        # all ownership, placement, device-index and deletion checks ran first.
        entry['id'] = found['InstanceId']
        entry['status'] = 'creating'
        _dsb_save(job, persist)
        ec2.get_waiter('instance_running').wait(InstanceIds=[entry['id']])
        for _ in range(AMBIGUOUS_TAG_LOOKUP_ATTEMPTS):
            observed = _dsb_discover(ec2, contract, graph, key, allow_attaching=True)
            if (observed is not None and observed.get('State', {}).get('Name') == 'running'
                    and observed['NetworkInterfaces'][0]['Attachment']['Status'] == 'attached'
                    and _dsb_public_address_ready(key, observed)
                    and boot_mapping_ready(observed)):
                found = observed
                break
            time.sleep(AMBIGUOUS_TAG_LOOKUP_DELAY_SECONDS)
        else:
            _dsb_fail(f'{key} primary ENI is not yet attached, or required public addressing/boot mapping is not yet ready; retain identity and retry reconciliation.')
    _dsb_verify_item(contract, graph, key, found)
    return found


def _dsb_public_address_ready(key, item):
    public_ip = item.get('PublicIpAddress')
    association = item['NetworkInterfaces'][0].get('Association', {})
    if key.removeprefix('instance-') in ('database', 'cache'):
        return not public_ip and not association
    return bool(public_ip) and association.get('PublicIp') == public_ip


def _dsb_ensure(ec2, job, key, persist):
    state = job['resources'][AWS_DSB_GRAPH_KEY]
    contract, graph = state['contract'], state['graph']
    entry = graph[key]
    kind, request = _dsb_specs(contract, graph)[key]
    if entry['status'] == 'deleted':
        _dsb_fail('a cleanup-started graph cannot be provisioned again.')
    found = _dsb_discover(ec2, contract, graph, key, allow_attaching=True)
    if found is None:
        if entry.get('id') or entry['status'] != 'planned':
            # Even tokenized creates are not blindly replayed: operators can
            # re-run read-only recovery after EC2's eventual-consistency window.
            _dsb_fail(f'{key} create is unresolved; retain the inventory and reconcile again.')
        entry['status'] = 'creating'
        _dsb_save(job, persist)
        try:
            response = getattr(ec2, _DSB_KINDS[kind][3])(**request)
            if kind == 'igw':
                found = _dsb_accept_igw_create_response(contract, graph, response)
                if found is not None:
                    # The service response is the first authoritative identity.
                    # Retain it before any eventually-consistent read can fail.
                    entry['id'] = found['InternetGatewayId']
                    entry['status'] = 'creating'
                    _dsb_save(job, persist)
        except Exception as exc:
            # These explicit service rejections guarantee no resource was
            # accepted (not timeouts/5xx). Do not strand an empty create intent.
            if isinstance(exc, ClientError) and exc.response.get('Error', {}).get('Code') in {
                'UnauthorizedOperation', 'AuthFailure', 'OptInRequired',
                'InvalidParameterValue', 'InvalidParameterCombination',
                'InsufficientInstanceCapacity', 'InstanceLimitExceeded',
                'VcpuLimitExceeded', 'AddressLimitExceeded',
            }:
                entry['status'] = 'planned'
                _dsb_save(job, persist)
                raise
            entry['status'] = 'create_ambiguous'
            _dsb_save(job, persist)
            # An accepted request can lose its response; never create twice.
            found = _dsb_discover(ec2, contract, graph, key, allow_attaching=True)
            if found is None:
                raise
        for _ in range(AMBIGUOUS_TAG_LOOKUP_ATTEMPTS):
            found = _dsb_discover(ec2, contract, graph, key, allow_attaching=True)
            if found:
                break
            time.sleep(AMBIGUOUS_TAG_LOOKUP_DELAY_SECONDS)
        if found is None:
            entry['status'] = 'create_ambiguous'
            _dsb_save(job, persist)
            _dsb_fail(f'{key} create cannot yet be reconciled.')
    if kind == 'instance':
        found = _dsb_wait_for_primary_attachment(ec2, job, key, found, persist)
    entry['id'] = found[_DSB_KINDS[kind][2]]
    entry['status'] = (
        'creating' if kind == 'instance' and found.get('State', {}).get('Name') == 'pending'
        else 'running'
    )
    if kind == 'instance':
        entry['public_addresses'] = [found['PublicIpAddress']] if found.get('PublicIpAddress') else []
    _dsb_save(job, persist)
    return found


def _dsb_permissions(contract, graph, role):
    permissions = []
    def add(protocol, port, source=None, cidr=None):
        rule = {'IpProtocol': protocol, 'FromPort': port, 'ToPort': port}
        if source:
            source_id = graph['sg-' + source].get('id')
            if not source_id:
                return
            rule['UserIdGroupPairs'] = [{'GroupId': source_id}]
        else:
            rule['IpRanges'] = [{'CidrIp': cidr}]
        permissions.append(rule)
    if role in ('control', 'application', 'load-generator'):
        add('tcp', 22, cidr=SSH_SOURCE_CIDR)
    else:
        for source in ('control', 'application'):
            add('tcp', 22, source=source)
    manifest = _dsb_contract_manifest(contract)
    channels = {c.key: c for c in manifest.network_policy.channels}
    for rule in manifest.network_policy.rules:
        if rule.destination == role:
            for channel in rule.channels:
                for port in channels[channel].ports:
                    add(channels[channel].protocol, port, source=rule.source)
    # The overlay must also allow pods whose endpoints share a role node.
    if role != 'load-generator':
        add('udp', 8472, source=role)
    return permissions


def _dsb_rule_set(permissions):
    values = set()
    for p in permissions:
        base = (p['IpProtocol'], p.get('FromPort'), p.get('ToPort'))
        for key, field in (('IpRanges', 'CidrIp'), ('Ipv6Ranges', 'CidrIpv6'),
                           ('UserIdGroupPairs', 'GroupId'), ('PrefixListIds', 'PrefixListId')):
            for item in p.get(key, []):
                values.add((*base, key, item[field]))
    return values


def _dsb_connect(ec2, job, persist):
    """Reconcile child edges from observed state, allowing only exact subsets."""
    state = job['resources'][AWS_DSB_GRAPH_KEY]
    c, g = state['contract'], state['graph']
    igw = _dsb_discover(ec2, c, g, 'igw')
    if not igw.get('Attachments'):
        ec2.attach_internet_gateway(InternetGatewayId=g['igw']['id'], VpcId=g['vpc']['id'])
    for key, target, target_key in (
        ('public-route', 'GatewayId', 'igw'), ('private-route', 'NatGatewayId', 'nat')):
        table = _dsb_discover(ec2, c, g, key)
        default = [r for r in table.get('Routes', []) if r.get('DestinationCidrBlock') == '0.0.0.0/0']
        if not default:
            ec2.create_route(RouteTableId=g[key]['id'], DestinationCidrBlock='0.0.0.0/0',
                             **{target: g[target_key]['id']})
        elif len(default) != 1 or default[0].get(target) != g[target_key]['id']:
            _dsb_fail(f'{key} default route differs.')
        expected_subnets = ('data',) if key == 'private-route' else ('management', 'loadgen')
        for name in expected_subnets:
            subnet_id = g['subnet-' + name]['id']
            attached = [a for a in table.get('Associations', []) if a.get('SubnetId') == subnet_id]
            if not attached:
                ec2.associate_route_table(RouteTableId=g[key]['id'], SubnetId=subnet_id)
    for role in AWS_DSB_ADDRESSES:
        group = _dsb_discover(ec2, c, g, 'sg-' + role)
        expected = _dsb_permissions(c, g, role)
        observed = _dsb_rule_set(group.get('IpPermissions', []))
        if not observed <= _dsb_rule_set(expected):
            _dsb_fail(f'{role} security group has unexpected ingress.')
        for permission in expected:
            if not _dsb_rule_set([permission]) <= observed:
                ec2.authorize_security_group_ingress(GroupId=g['sg-' + role]['id'],
                                                     IpPermissions=[permission])
    _dsb_save(job, persist)


def provision_distributed_deathstarbench_candidate(
    job, plan, *, support_image, application_image, public_key=None,
    emit=None, persist=None, aws_session=None,
):
    """Explicit operator entrypoint, not reachable through normal released plans.

    Both image pins are mandatory and Marketplace terms must already have been
    accepted by the operator. No subscription or agreement mutation is made.
    No guest commands, K3s setup, benchmark, or public API release is included.
    """
    dsb = _value(plan, 'deathstarbench', {}) or {}
    if (_value(plan, 'provider') != 'aws'
            or tuple(_value(plan, 'benchmarks', ())) != ('deathstarbench',)
            or _value(dsb, 'workload') != 'social_network'
            or _value(dsb, 'topology_id') != DISTRIBUTED_TIERED_TOPOLOGY_ID
            or _value(dsb, 'runtime_id') != K3S_RUNTIME_ID):
        raise ValueError('AWS candidate requires only distributed_tiered_v1/k3s_v1 Social Network.')
    job_id = str(job.get('id', ''))
    if not re.fullmatch(r'[a-z0-9]{8,32}', job_id):
        raise ValueError('AWS candidate job ID must be 8–32 lowercase alphanumeric characters.')
    public_key = str(public_key or job.get('_public_key') or '').strip()
    if not re.fullmatch(r'ssh-(?:rsa|ed25519) [A-Za-z0-9+/=]+(?: [^\r\n]+)?', public_key):
        raise ValueError('An OpenSSH RSA or Ed25519 public key is required.')
    _dsb_public_key_identity(public_key)
    resources = job.setdefault('resources', {})
    if resources and AWS_DSB_GRAPH_KEY not in resources:
        _dsb_fail('refusing pre-existing compact or unrelated resource state.')
    session = _session(_profile(plan), _region(plan), aws_session)
    ec2 = _dsb_ec2_client(session, _region(plan))
    account = session.client('sts', region_name=_region(plan)).get_caller_identity()['Account']
    shapes = {**AWS_DSB_SHAPES, 'application': _instance_type(plan)}
    details = {shape: _instance_type_details(ec2, shape) for shape in set(shapes.values())}
    zone = _resolve_availability_zone(ec2, shapes['application'],
        _value(plan, 'availability_zone') or _value(plan, 'availability_domain'))
    for shape in set(shapes.values()):
        _resolve_availability_zone(ec2, shape, zone)
    if any(details[shape]['architecture'] != 'x86_64' for shape in AWS_DSB_SHAPES.values()):
        raise ValueError('Supporting AWS candidate roles require x86-64.')
    for role, shape in AWS_DSB_SHAPES.items():
        expected_cpu, expected_memory = (4, 16) if role == 'database' else (2, 8)
        if details[shape]['vcpu'] != expected_cpu or details[shape]['memory_gb'] != expected_memory:
            raise ValueError(f'The fixed {role} AWS role capacity differs from its contract.')
    application_capacity = details[shapes['application']]
    if (_value(plan, 'ocpus') != application_capacity['vcpu']
            or _value(plan, 'memory_gb') != application_capacity['memory_gb']):
        raise ValueError(
            'AWS application vCPU/memory metadata must exactly match the '
            'selected instance type.'
        )
    contract = {
        'job_id': job_id, 'account_id': account, 'profile': _profile(plan),
        'region': _region(plan), 'zone': zone, 'shapes': shapes, 'public_key': public_key,
        'support_image': _dsb_pin_image(ec2, support_image, 'x86_64'),
        'application_image': _dsb_pin_image(ec2, application_image,
            details[shapes['application']]['architecture']),
    }
    manifest = _dsb_contract_manifest(contract)
    if AWS_DSB_GRAPH_KEY in resources:
        state = _dsb_load(job)
        if state['contract'] != contract:
            _dsb_fail('resume inputs differ from the immutable launch contract.')
    else:
        keys = ('vpc', 'igw', *('subnet-' + k for k in AWS_DSB_SUBNETS),
                'public-route', 'private-route', 'eip', 'nat',
                *('sg-' + k for k in AWS_DSB_ADDRESSES), 'key', 'database-data',
                *('instance-' + k for k in manifest.creation_order))
        resources.update(provider='aws', aws_distributed_candidate=True,
            aws_profile=_profile(plan), region=_region(plan),
            aws_account_id=account, availability_zone=zone,
            deathstarbench_topology_manifest=manifest.as_dict(),
            deathstarbench_topology_fingerprint=manifest.fingerprint)
        resources[AWS_DSB_GRAPH_KEY] = {'schema_version': AWS_DSB_SCHEMA_VERSION,
            'contract': contract, 'graph': {key: {'status': 'planned'} for key in keys}}
        _dsb_save(job, persist)  # Complete topology + role/storage plan before ANY mutation.
    state = resources[AWS_DSB_GRAPH_KEY]
    g = state['graph']
    for key in _dsb_specs(contract, g):
        if key.startswith('instance-'):
            continue
        if key == 'nat':
            # Public NAT requires its public subnet's IGW and route first.
            igw = _dsb_discover(ec2, contract, g, 'igw')
            if not igw.get('Attachments'):
                ec2.attach_internet_gateway(InternetGatewayId=g['igw']['id'], VpcId=g['vpc']['id'])
            table = _dsb_discover(ec2, contract, g, 'public-route')
            if not any(r.get('DestinationCidrBlock') == '0.0.0.0/0' for r in table.get('Routes', [])):
                ec2.create_route(RouteTableId=g['public-route']['id'],
                    DestinationCidrBlock='0.0.0.0/0', GatewayId=g['igw']['id'])
            if not any(a.get('SubnetId') == g['subnet-management']['id'] for a in table.get('Associations', [])):
                ec2.associate_route_table(RouteTableId=g['public-route']['id'], SubnetId=g['subnet-management']['id'])
        _dsb_ensure(ec2, job, key, persist)
        if key == 'vpc':
            ec2.get_waiter('vpc_available').wait(VpcIds=[g[key]['id']])
        if key.startswith('subnet-'):
            ec2.get_waiter('subnet_available').wait(SubnetIds=[g[key]['id']])
        if key == 'nat':
            ec2.get_waiter('nat_gateway_available').wait(NatGatewayIds=[g[key]['id']])
    # Audit all existing children before adding role rules or associations.
    recover_distributed_deathstarbench_candidate(job, persist=persist, aws_session=session)
    _dsb_connect(ec2, job, persist)
    for role in manifest.creation_order:
        key = 'instance-' + role
        _dsb_ensure(ec2, job, key, persist)
        ec2.get_waiter('instance_running').wait(InstanceIds=[g[key]['id']])
        _dsb_ensure(ec2, job, key, persist)
        _emit(job, emit, 'Provision', f'AWS distributed {role} instance is running.')
    volume = _dsb_discover(ec2, contract, g, 'database-data')
    expected_attachment = {'InstanceId': g['instance-database']['id'], 'Device': DATA_VOLUME_DEVICE}
    attachments = volume.get('Attachments', [])
    if not attachments:
        ec2.get_waiter('volume_available').wait(VolumeIds=[g['database-data']['id']])
        ec2.attach_volume(VolumeId=g['database-data']['id'], **expected_attachment)
    elif len(attachments) != 1 or any(attachments[0].get(k) != v for k, v in expected_attachment.items()):
        _dsb_fail('database volume attachment differs.')
    ec2.get_waiter('volume_in_use').wait(VolumeIds=[g['database-data']['id']])
    # AttachVolume defaults to false; verify rather than silently changing a disk contract.
    recover_distributed_deathstarbench_candidate(job, persist=persist, aws_session=session)
    _emit(job, emit, 'Provision', 'AWS five-node infrastructure candidate is ready; runtime remains unreleased.')
    return resources


def _dsb_verify_graph(ec2, contract, graph, objects):
    """Read the whole child graph before permitting the first delete.

    Fresh VPC default route/security/ACL objects are provider-owned defaults;
    all other children must match this exact graph. Foreign users/attachments
    stop cleanup, even when the top-level VPC still carries our tags.
    """
    vpc_id = graph['vpc'].get('id')
    for kind in _DSB_KINDS:
        known = {item[_DSB_KINDS[kind][2]] for key, item in objects.items()
                 if _dsb_specs(contract, graph)[key][0] == kind}
        if kind == 'volume':
            known.update(d['Ebs']['VolumeId'] for key, item in objects.items()
                         if key.startswith('instance-')
                         for d in item.get('BlockDeviceMappings', [])
                         if d.get('Ebs', {}).get('DeleteOnTermination') is True)
        discovered = _dsb_list(ec2, kind, _dsb_filters(contract['job_id']))
        if any(item[_DSB_KINDS[kind][2]] not in known for item in discovered):
            _dsb_fail(f'unknown job-tagged {kind} resource; cleanup is blocked.')
        if not vpc_id or kind not in ('subnet', 'sg', 'route', 'instance', 'nat', 'igw'):
            continue
        name = 'attachment.vpc-id' if kind == 'igw' else 'vpc-id'
        children = _dsb_list(ec2, kind, [{'Name': name, 'Values': [vpc_id]}])
        if kind == 'sg' and 'vpc' in objects and sum(c.get('GroupName') == 'default' for c in children) != 1:
            _dsb_fail('default VPC security group is missing or ambiguous.')
        for child in children:
            if child[_DSB_KINDS[kind][2]] in known:
                continue
            if kind == 'sg' and child.get('GroupName') == 'default':
                expected_ingress = {('-1', None, None, 'UserIdGroupPairs', child['GroupId'])}
                expected_egress = {('-1', None, None, 'IpRanges', '0.0.0.0/0')}
                if (child.get('VpcId') != vpc_id
                        or _dsb_rule_set(child.get('IpPermissions', [])) != expected_ingress
                        or _dsb_rule_set(child.get('IpPermissionsEgress', [])) != expected_egress
                        or any(p.get('UserId') != contract['account_id']
                               for rule in child.get('IpPermissions', [])
                               for p in rule.get('UserIdGroupPairs', []))):
                    _dsb_fail('default VPC security group has foreign configuration.')
                continue
            if kind == 'route' and any(a.get('Main') for a in child.get('Associations', [])):
                if (any(not a.get('Main') for a in child.get('Associations', []))
                        or any(r.get('GatewayId') != 'local' for r in child.get('Routes', []))):
                    _dsb_fail('default VPC route table has foreign configuration.')
                continue
            _dsb_fail(f'foreign {kind} child in the candidate VPC.')
    allowed_nics = set()
    instance_nics = {}
    for key, item in objects.items():
        kind, _ = _dsb_specs(contract, graph)[key]
        if kind == 'instance':
            allowed_nics.update(n['NetworkInterfaceId'] for n in item.get('NetworkInterfaces', []))
            instance_nics[item['NetworkInterfaces'][0]['NetworkInterfaceId']] = item
            role = key.removeprefix('instance-')
            for device in item.get('BlockDeviceMappings', []):
                if not device.get('Ebs', {}).get('DeleteOnTermination'):
                    continue
                volume_id = device['Ebs']['VolumeId']
                volumes = _dsb_list(ec2, 'volume', [{'Name': 'volume-id', 'Values': [volume_id]}])
                if len(volumes) != 1:
                    _dsb_fail(f'{key} boot volume cannot be verified.')
                volume = volumes[0]
                _dsb_require_tags(volume, contract['job_id'], 'boot-' + role)
                attachments = volume.get('Attachments', [])
                if (volume.get('Encrypted') is not True or volume.get('Size') != 50
                        or volume.get('VolumeType') != 'gp3'
                        or volume.get('Iops') != 3000 or volume.get('Throughput') != 125
                        or len(attachments) != 1
                        or attachments[0].get('InstanceId') != item['InstanceId']):
                    _dsb_fail(f'{key} boot volume configuration/attachment differs.')
        elif kind == 'nat':
            allowed_nics.update(a['NetworkInterfaceId'] for a in item.get('NatGatewayAddresses', [])
                                if a.get('NetworkInterfaceId'))
        elif kind == 'volume':
            for attachment in item.get('Attachments', []):
                if (attachment.get('InstanceId') != graph['instance-database'].get('id')
                        or attachment.get('Device') != DATA_VOLUME_DEVICE):
                    _dsb_fail('database volume has a foreign attachment.')
        elif kind == 'eip':
            nat = objects.get('nat', {})
            allowed = {a.get('NetworkInterfaceId') for a in nat.get('NatGatewayAddresses', [])}
            if item.get('NetworkInterfaceId') and item['NetworkInterfaceId'] not in allowed:
                _dsb_fail('elastic IP has a foreign attachment.')
        elif kind == 'sg':
            expected = _dsb_permissions(contract, graph, key.removeprefix('sg-'))
            if not _dsb_rule_set(item.get('IpPermissions', [])) <= _dsb_rule_set(expected):
                _dsb_fail(f'{key} ingress differs.')
            allowed_egress = {('-1', None, None, 'IpRanges', '0.0.0.0/0')}
            if _dsb_rule_set(item.get('IpPermissionsEgress', [])) != allowed_egress:
                _dsb_fail(f'{key} egress differs.')
        elif kind == 'route':
            names = ('data',) if key == 'private-route' else ('management', 'loadgen')
            expected_subnets = {graph['subnet-' + name].get('id') for name in names}
            if any(a.get('Main') or a.get('SubnetId') not in expected_subnets
                   for a in item.get('Associations', [])):
                _dsb_fail(f'{key} has a foreign association.')
            for route in item.get('Routes', []):
                if route.get('DestinationCidrBlock') == '10.240.0.0/16' and route.get('GatewayId') == 'local':
                    continue
                field, target = ('NatGatewayId', 'nat') if key == 'private-route' else ('GatewayId', 'igw')
                if (route.get('DestinationCidrBlock') != '0.0.0.0/0'
                        or route.get(field) != graph[target].get('id')):
                    _dsb_fail(f'{key} has a foreign route.')
    if vpc_id:
        request = {'Filters': [{'Name': 'vpc-id', 'Values': [vpc_id]}]}
        observed_nics = set()
        while True:
            response = ec2.describe_network_interfaces(**request)
            observed_nics.update(n['NetworkInterfaceId'] for n in response.get('NetworkInterfaces', []))
            if any(n['NetworkInterfaceId'] not in allowed_nics for n in response.get('NetworkInterfaces', [])):
                _dsb_fail('candidate VPC has a foreign network interface.')
            for nic in response.get('NetworkInterfaces', []):
                instance = instance_nics.get(nic['NetworkInterfaceId'])
                if instance is None:
                    continue
                attachment = nic.get('Attachment', {})
                expected = instance['NetworkInterfaces'][0]['Attachment']
                public_ip = instance.get('PublicIpAddress')
                association = nic.get('Association', {})
                if ((public_ip and association.get('PublicIp') != public_ip)
                        or (not public_ip and association)):
                    _dsb_fail('primary ENI public association differs from the role address policy.')
                if (attachment.get('AttachmentId') != expected['AttachmentId']
                        or attachment.get('InstanceId') != instance['InstanceId']
                        or attachment.get('InstanceOwnerId') != contract['account_id']
                        or attachment.get('DeviceIndex') != 0
                        or attachment.get('Status') != 'attached'
                        or attachment.get('DeleteOnTermination') is not True
                        or nic.get('VpcId') != vpc_id
                        or nic.get('SubnetId') != instance['SubnetId']
                        or nic.get('PrivateIpAddress') != instance['PrivateIpAddress']
                        or {g.get('GroupId') for g in nic.get('Groups', [])}
                        != {g['GroupId'] for g in instance['SecurityGroups']}):
                    _dsb_fail('primary ENI attachment identity or deletion contract differs.')
            if not response.get('NextToken'):
                break
            request['NextToken'] = response['NextToken']
        if not set(instance_nics) <= observed_nics:
            _dsb_fail('primary ENI attachment cannot be independently verified.')
        filters = [{'Name': 'vpc-id', 'Values': [vpc_id]}]
        acls = _dsb_read_children(ec2, 'describe_network_acls', 'NetworkAcls', filters)
        if any(a.get('IsDefault') is not True for a in acls):
            _dsb_fail('candidate VPC has a foreign network ACL.')
        if 'vpc' in objects and len(acls) != 1:
            _dsb_fail('default VPC network ACL is missing or ambiguous.')
        expected_entries = [
            {'RuleNumber': number, 'Protocol': '-1', 'RuleAction': action,
             'Egress': egress, 'CidrBlock': '0.0.0.0/0'}
            for egress in (False, True) for number, action in ((100, 'allow'), (32767, 'deny'))
        ]
        expected_subnets = {obj['SubnetId'] for key, obj in objects.items() if key.startswith('subnet-')}
        for acl in acls:
            associations = acl.get('Associations', [])
            entries = acl.get('Entries', [])
            if (acl.get('VpcId') != vpc_id
                    or len(entries) != len(expected_entries)
                    or any(e not in expected_entries for e in entries)
                    or any(e not in entries for e in expected_entries)
                    or len(associations) != len(expected_subnets)
                    or {a.get('SubnetId') for a in associations} != expected_subnets
                    or any(a.get('NetworkAclId') != acl.get('NetworkAclId')
                           or not a.get('NetworkAclAssociationId') for a in associations)
                    or len({a['NetworkAclAssociationId'] for a in associations}) != len(associations)):
                _dsb_fail('default VPC network ACL has foreign entries or associations.')
        endpoints = _dsb_read_children(ec2, 'describe_vpc_endpoints', 'VpcEndpoints', filters)
        if endpoints:
            _dsb_fail('candidate VPC has foreign endpoints.')
        for side in ('requester', 'accepter'):
            peers = _dsb_read_children(ec2, 'describe_vpc_peering_connections',
                'VpcPeeringConnections', [{'Name': f'{side}-vpc-info.vpc-id', 'Values': [vpc_id]}])
            if any(p.get('Status', {}).get('Code') not in ('deleted', 'rejected', 'expired', 'failed') for p in peers):
                _dsb_fail('candidate VPC has a foreign peering connection.')
        transit = _dsb_read_children(ec2, 'describe_transit_gateway_vpc_attachments',
                                     'TransitGatewayVpcAttachments', filters)
        if any(a.get('State') not in ('deleted', 'rejected', 'failed') for a in transit):
            _dsb_fail('candidate VPC has a foreign transit gateway attachment.')
        vpn = _dsb_read_children(ec2, 'describe_vpn_gateways', 'VpnGateways',
                                 [{'Name': 'attachment.vpc-id', 'Values': [vpc_id]}])
        if vpn:
            _dsb_fail('candidate VPC has a foreign VPN gateway.')


def recover_distributed_deathstarbench_candidate(
    job,
    *,
    persist=None,
    aws_session=None,
    wait_for_attachments=True,
    allow_quiesced_load_generator=False,
):
    """Read-only cloud reconciliation; persist accepted IDs, never re-create."""
    state = _dsb_load(
        job,
        allow_quiesced_load_generator=allow_quiesced_load_generator,
    )
    contract, graph = state['contract'], state['graph']
    session = _session(contract['profile'], contract['region'], aws_session)
    if session.client('sts', region_name=contract['region']).get_caller_identity()['Account'] != contract['account_id']:
        _dsb_fail('current AWS account differs from the launch account.')
    ec2 = _dsb_ec2_client(session, contract['region'])
    objects = {}
    for key in _dsb_specs(contract, graph):
        entry = graph[key]
        may_wait = wait_for_attachments and entry['status'] not in ('deleting', 'delete_ambiguous', 'deleted')
        found = _dsb_discover(
            ec2,
            contract,
            graph,
            key,
            allow_attaching=may_wait,
            allow_quiesced_load_generator=allow_quiesced_load_generator,
        )
        if found:
            if entry['status'] == 'deleted':
                _dsb_fail(f'{key} reappeared after confirmed deletion.')
            if may_wait and key.startswith('instance-'):
                found = _dsb_wait_for_primary_attachment(ec2, job, key, found, persist)
            entry['id'] = found[_DSB_KINDS[_dsb_specs(contract, graph)[key][0]][2]]
            if key.startswith('instance-'):
                observed_public_ip = found.get('PublicIpAddress')
                if not (
                    allow_quiesced_load_generator
                    and key == 'instance-load-generator'
                    and not observed_public_ip
                    and found.get('State', {}).get('Name') in {
                        'stopping', 'stopped', 'shutting-down', 'terminated',
                    }
                ):
                    entry['public_addresses'] = (
                        [observed_public_ip] if observed_public_ip else []
                    )
            if entry['status'] not in ('deleting', 'delete_ambiguous'):
                entry['status'] = ('creating' if key.startswith('instance-')
                    and found.get('State', {}).get('Name') == 'pending' else 'running')
            objects[key] = found
            _dsb_save(job, persist)
        elif entry['status'] in ('creating', 'create_ambiguous'):
            _dsb_fail(f'{key} create remains ambiguous; do not delete its dependencies.')
        elif entry.get('id'):
            if entry['status'] not in ('deleting', 'delete_ambiguous', 'deleted'):
                _dsb_fail(f'{key} disappeared outside cleanup; ownership cannot be established.')
            entry['status'] = 'deleted'
            _dsb_save(job, persist)
    _dsb_verify_graph(ec2, contract, graph, objects)
    return objects


def quiesce_distributed_deathstarbench_load_generator(
    job, *, aws_session=None,
):
    """Stop only the exact owned load generator before graph-wide cleanup.

    A durable execution journal means a remote benchmark unit may still be
    producing traffic even after the controller lost SSH.  Validate the local
    candidate contract and the load-generator instance itself before using the
    EC2 control plane.  This intentionally does not audit unrelated siblings:
    the full cleanup gate below may reject those, but it must do so only after
    measured traffic has been quiesced.
    """
    resources = job.get('resources', {})
    if DEATHSTARBENCH_EXECUTION_JOURNAL_KEY not in resources:
        return False
    state = _dsb_load(job, allow_quiesced_load_generator=True)
    contract, graph = state['contract'], state['graph']
    entry = graph['instance-load-generator']
    if entry.get('status') in ('planned', 'deleted'):
        return False
    session = _session(
        contract['profile'], contract['region'], aws_session,
    )
    account = session.client(
        'sts', region_name=contract['region'],
    ).get_caller_identity().get('Account')
    if account != contract['account_id']:
        _dsb_fail('current AWS account differs from the launch account.')
    ec2 = _dsb_ec2_client(session, contract['region'])
    instance = _dsb_discover(
        ec2,
        contract,
        graph,
        'instance-load-generator',
        allow_quiesced_load_generator=True,
    )
    if instance is None:
        return False
    state_name = instance.get('State', {}).get('Name')
    quiet_states = {
        'stopping', 'stopped', 'shutting-down', 'terminated',
    }
    if state_name in quiet_states:
        return False
    if state_name not in {'pending', 'running'}:
        _dsb_fail(
            'load-generator instance has an unsupported lifecycle state; '
            'refusing stop.'
        )
    instance_id = entry.get('id')
    if instance_id != instance.get('InstanceId'):
        _dsb_fail('load-generator instance identity changed before stop.')
    try:
        ec2.stop_instances(InstanceIds=[instance_id])
        ec2.get_waiter('instance_stopped').wait(InstanceIds=[instance_id])
    except Exception:
        # A response or waiter can be lost after EC2 accepted the idempotent
        # stop.  Accept only a fresh exact-instance read proving a quiet state;
        # otherwise surface the failure so an operator can retry safely.
        observed = _dsb_discover(
            ec2,
            contract,
            graph,
            'instance-load-generator',
            allow_quiesced_load_generator=True,
        )
        if observed is None or observed.get('State', {}).get('Name') in quiet_states:
            return True
        raise
    observed = _dsb_discover(
        ec2,
        contract,
        graph,
        'instance-load-generator',
        allow_quiesced_load_generator=True,
    )
    if observed is not None and observed.get('State', {}).get('Name') not in quiet_states:
        _dsb_fail('load-generator stop was not confirmed.')
    return True


def destroy_distributed_deathstarbench_candidate(
    job, *, emit=None, persist=None, aws_session=None, preserve_status=False,
):
    """Quiesce exact load traffic, then verify and delete the whole graph.

    On any ambiguity or foreign resource the remaining graph is retained. The
    targeted stop occurs first only when an execution journal exists, so a
    later sibling refusal cannot strand active traffic. This entrypoint never
    force-detaches a disk or deletes a dependency after an unconfirmed child
    deletion. Durable tombstones remain as recovery evidence.
    """
    state = _dsb_load(
        job,
        allow_quiesced_load_generator=(
            DEATHSTARBENCH_EXECUTION_JOURNAL_KEY
            in job.get('resources', {})
        ),
    )
    c, g = state['contract'], state['graph']
    session = _session(c['profile'], c['region'], aws_session)
    ec2 = _dsb_ec2_client(session, c['region'])
    quiesce_distributed_deathstarbench_load_generator(
        job, aws_session=session,
    )
    objects = recover_distributed_deathstarbench_candidate(
        job,
        persist=persist,
        aws_session=session,
        wait_for_attachments=False,
        allow_quiesced_load_generator=True,
    )
    order = [*('instance-' + r for r in _dsb_contract_manifest(c).deletion_order),
             'database-data', 'nat', 'eip', *('sg-' + r for r in AWS_DSB_ADDRESSES),
             'key', 'private-route', 'public-route',
             *('subnet-' + n for n in reversed(AWS_DSB_SUBNETS)), 'igw', 'vpc']
    # Remove only the exact verified internal SG edges before deleting groups.
    # This is after the whole graph audit and before any instance is deleted.
    for role in AWS_DSB_ADDRESSES:
        item = objects.get('sg-' + role)
        if item and item.get('IpPermissions'):
            ec2.revoke_security_group_ingress(GroupId=item['GroupId'], IpPermissions=item['IpPermissions'])
    for key in order:
        item = _dsb_discover(
            ec2,
            c,
            g,
            key,
            allow_quiesced_load_generator=True,
        )
        if item is None:
            if g[key]['status'] == 'planned':
                g[key]['status'] = 'deleted'
                _dsb_save(job, persist)
            continue
        g[key]['status'] = 'deleting'
        _dsb_save(job, persist)
        try:
            kind = _dsb_specs(c, g)[key][0]
            resource_id = g[key]['id']
            if kind == 'instance':
                ec2.terminate_instances(InstanceIds=[resource_id])
                ec2.get_waiter('instance_terminated').wait(InstanceIds=[resource_id])
            elif kind == 'volume':
                ec2.get_waiter('volume_available').wait(VolumeIds=[resource_id])
                ec2.delete_volume(VolumeId=resource_id)
            elif kind == 'nat':
                ec2.delete_nat_gateway(NatGatewayId=resource_id)
                ec2.get_waiter('nat_gateway_deleted').wait(NatGatewayIds=[resource_id])
            elif kind == 'eip':
                ec2.release_address(AllocationId=resource_id)
            elif kind == 'sg':
                ec2.delete_security_group(GroupId=resource_id)
            elif kind == 'key':
                ec2.delete_key_pair(KeyPairId=resource_id)
            elif kind == 'route':
                for association in item.get('Associations', []):
                    ec2.disassociate_route_table(AssociationId=association['RouteTableAssociationId'])
                ec2.delete_route_table(RouteTableId=resource_id)
            elif kind == 'subnet':
                ec2.delete_subnet(SubnetId=resource_id)
            elif kind == 'igw':
                if item.get('Attachments'):
                    ec2.detach_internet_gateway(InternetGatewayId=resource_id, VpcId=g['vpc']['id'])
                ec2.delete_internet_gateway(InternetGatewayId=resource_id)
            elif kind == 'vpc':
                # DeleteVpc also removes provider-created defaults. Re-audit
                # them immediately before that implicit destructive action.
                recover_distributed_deathstarbench_candidate(
                    job,
                    persist=persist,
                    aws_session=session,
                    wait_for_attachments=False,
                    allow_quiesced_load_generator=True,
                )
                ec2.delete_vpc(VpcId=resource_id)
            for _ in range(AMBIGUOUS_TAG_LOOKUP_ATTEMPTS):
                if _dsb_discover(
                    ec2,
                    c,
                    g,
                    key,
                    allow_quiesced_load_generator=True,
                ) is None:
                    break
                time.sleep(AMBIGUOUS_TAG_LOOKUP_DELAY_SECONDS)
            else:
                _dsb_fail(f'{key} deletion has not been confirmed.')
        except Exception:
            g[key]['status'] = 'delete_ambiguous'
            _dsb_save(job, persist)
            raise
        g[key]['status'] = 'deleted'
        _dsb_save(job, persist)
        _emit(job, emit, 'Cleanup', f'Deleted AWS distributed {key}.')
    if not all(entry.get('status') == 'deleted' for entry in g.values()):
        _dsb_fail('AWS distributed cleanup ended without complete tombstones.')
    for alias in _dsb_runtime_aliases(c, g):
        job['resources'].pop(alias, None)
    if not preserve_status:
        job['status'] = 'destroyed'
    job.pop('cleanup_error', None)
    _dsb_save(job, persist)
    return job['resources']
