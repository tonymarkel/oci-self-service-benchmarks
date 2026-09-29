# AWS distributed DeathStarBench infrastructure candidate

This is an **operator-only infrastructure slice**, not a released benchmark.
The normal provider entrypoint rejects the unreleased
`distributed_tiered_v1/k3s_v1` profile. There is no new UI, public API, K3s
bootstrap, workload, or measurement support in this slice.

`app.providers.aws.provision_distributed_deathstarbench_candidate` accepts the
normal AWS plan (DeathStarBench Social Network only), an SSH public key, and two
mandatory image-pin dictionaries, `support_image` and `application_image`.
Each pin has exactly these fields:

```python
{
    "image_id": "ami-<explicit regional image ID>",
    "owner_id": "<approved 12-digit publisher account>",
    "name": "<exact Rocky Linux 9 image name>",
    "creation_date": "<exact AWS CreationDate>",
    "product_code": "<approved Marketplace product code>",
    "architecture": "x86_64",  # or arm64 for the selected application
    "root_device_name": "<exact AMI root device name>",
}
```

The caller must approve the publisher/product and accept Marketplace terms
before running the candidate. The adapter verifies the complete supplied pin,
HVM/EBS/ENA support, and available state. It never discovers a moving "latest"
image, subscribes to a product, or accepts an agreement. DescribeImages does
not prove subscription entitlement; a launch can still be rejected by AWS.

## Infrastructure contract

One availability zone, with all roles and storage recorded before the first
mutation:

| Role | Shape | Private address | Public IP |
| --- | --- | --- | --- |
| Control | m7i.large | 10.240.1.10 | Yes |
| Application | Selected shape | 10.240.1.13 | Yes |
| Load generator | m7i.large | 10.240.2.10 | Yes |
| Database | m7i.xlarge | 10.240.3.11 | No |
| Cache | m7i.large | 10.240.3.12 | No |

The VPC is `10.240.0.0/16`. Management, load-generator, and data subnets use
the corresponding `/24` prefixes. A zonal NAT gateway provides private data
egress; public subnets use an internet gateway. Separate security groups
implement the versioned topology's role paths. Public SSH remains
`0.0.0.0/0`, consistent with the existing product policy.

The database uses an independently created, encrypted 100-GiB gp3 volume with
3,000 IOPS and 125 MiB/s, attached with `DeleteOnTermination=false`. It is not
formatted or mounted. `/dev/sdf` is an EC2 attachment name only, never a guest
device identity. The subsequent runtime slice must resolve Nitro EBS by its
volume ID/serial before formatting. Boot disks are encrypted 50-GiB gp3 and
deleted with their instances.

## Recovery and cleanup

Supply the existing job dictionary and persistence callback to
`recover_distributed_deathstarbench_candidate` for read-only cloud discovery.
Create intents are persisted first; tokenized resources have deterministic
client tokens and every resource has a job/role ownership tag. Accepted
response loss is reconciled without another create. An unresolved create
blocks deletion of its dependencies. Retry read-only recovery after EC2
eventual consistency settles; an ambiguous absence is never treated as proof
that no resource exists.

An accepted instance with a temporarily `attaching` primary ENI or pending
public address assignment retains its identity before waiting and re-observing
the attachment and address policy. Provisioning and
read-only recovery never replay `RunInstances` for that instance. Cleanup
continues to require a fully verified `attached` ENI and fails closed on a
transient or mismatched attachment.

`destroy_distributed_deathstarbench_candidate` (also dispatched by the normal
AWS cleanup entrypoint) audits the whole graph before deleting any resources.
Unexpected IDs, tags, attachments, children, network rules, or topology drift
block cleanup. This includes the provider-created default security group and
network ACL, the primary ENI's attachment/deletion policy, and the imported
SSH public key's algorithm and decoded key material. Default VPC resources
are checked again immediately before `DeleteVpc` implicitly removes them.
Public roles must have matching instance/primary-ENI public addresses; database
and cache must have neither. Independent boot-volume checks also verify the
explicit gp3 3,000-IOPS/125-MiB/s performance contract.
Deletion is dependency ordered, confirmed, and replayable.
It preserves durable deleted-resource tombstones and the role inventory.
No force detach is used. AWS permissions must include the read-only network
inventory APIs used by the audit, including VPC peering, VPN, and transit
gateway attachment discovery.

This candidate is covered by stateful API-schema-validated unit tests. **It
has not yet been live-qualified on AWS.** Operator execution must use the
existing run lease/persistence orchestration; these provider helpers do not
create or manage a process lease themselves. Keep the release gate closed
until live create/recovery/cleanup qualification and the separate runtime
integration are complete.
