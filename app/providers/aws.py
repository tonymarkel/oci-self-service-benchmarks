"""AWS discovery, provisioning, and cleanup for the benchmark MVP.

The adapter deliberately creates a fresh boto3 ``Session`` for each public
operation.  Only the profile name is carried in a plan; AWS credentials remain
in the user's normal local AWS configuration and are never copied into job
state.
"""

from __future__ import annotations

import math
import time
import uuid
from collections.abc import Callable, Mapping
from typing import Any

import boto3
from botocore.exceptions import ClientError, WaiterError

from ..guests import amazon_linux
from ..guests.amazon_linux import SSH_USER, ami_parameter_name


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
