# AWS distributed DeathStarBench infrastructure candidate

This document describes the **operator qualification surface** for the
released `distributed_tiered_v1/k3s_v1` benchmark. The compact provider
entrypoint still rejects direct distributed calls so callers cannot bypass the
coordinated public lifecycle gate; the normal UI/API uses that guarded public
dispatcher. The separate operator wrapper connects this infrastructure adapter to the
shared K3s bootstrap, Social Network workload, network qualification, optional
measurement path, resume, and cleanup-only machinery. See
`docs/deathstarbench-distributed.md` for the complete current contract and live
qualification evidence.

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
    "product_code": "<approved Marketplace product code or null>",
    "architecture": "x86_64",  # or arm64 for the selected application
    "root_device_name": "<exact AMI root device name>",
}
```

For Marketplace images, the caller must approve the publisher/product and
accept terms before running the candidate. The approved Rocky project public
publisher is also supported with a null product code and requires no
Marketplace mutation. The adapter verifies the complete supplied pin,
HVM/EBS/ENA support, public status where required, and available state. It
never discovers a moving "latest" image, subscribes to a product, or accepts an
agreement. DescribeImages does not prove Marketplace subscription entitlement;
a launch can still be rejected by AWS.

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

Before any mutation, live instance-type metadata must exactly match every fixed
support-role capacity and the application's explicitly supplied vCPU and memory
values. A stale or contradictory shape selection fails before a cloud write.

The VPC is `10.240.0.0/16`. Management, load-generator, and data subnets use
the corresponding `/24` prefixes. A zonal NAT gateway provides private data
egress; public subnets use an internet gateway. Separate security groups
implement the versioned topology's role paths. Public SSH remains
`0.0.0.0/0`, consistent with the existing product policy.

The database uses an independently created, encrypted 100-GiB gp3 volume with
3,000 IOPS and 125 MiB/s, attached with `DeleteOnTermination=false`.
`/dev/sdf` is an EC2 attachment name only, never a guest device identity. The
runtime resolves Nitro EBS by the exact volume ID/serial, rejects the boot
ancestry and any nonblank foreign content, and mounts only the resulting XFS
filesystem at `/var/lib/deathstarbench/database`. Boot disks are encrypted
50-GiB gp3 and deleted with their instances.

## Recovery and cleanup

Supply the existing job dictionary and persistence callback to
`recover_distributed_deathstarbench_candidate` for read-only cloud discovery.
Create intents are persisted first; tokenized resources have deterministic
client tokens and every resource has a job/role ownership tag. Accepted
response loss is reconciled without another create. An unresolved create
blocks deletion of its dependencies. Retry read-only recovery after EC2
eventual consistency settles; an ambiguous absence is never treated as proof
that no resource exists.

All candidate EC2 clients use `total_max_attempts=1`, so botocore cannot replay
an uncertain non-idempotent create behind the lifecycle controller. A complete
atomic create response is strictly checked before its identity is accepted. In
particular, an Internet Gateway response must contain the exact owner, empty
attachment state, and duplicate-free ownership tags; its ID is persisted before
any eventually consistent tag-filter lookup, and subsequent reads use that
exact ID.

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

This candidate is covered by stateful API-schema-validated tests. Operator
wrapper job `675d8b6ba5ae` passed live AWS provisioning, Rocky Linux guest
preparation, exact database mounting, K3s readiness, Social Network workload
readiness, required and forbidden network probes, and terminal cleanup on
2026-09-29. It did not use `--measure` and is not a benchmark result.

The immediately preceding `--measure` invocation, job `d7b5e018e6b1`, exposed
an EC2 IGW tag-index consistency window before runtime work began: the create
response contained the accepted identity, but the old path discarded it and an
immediate tag-filter lookup could not yet see the resource. The run failed
closed. After adding the strict response acceptance and exact-ID persistence
described above, its partial VPC/IGW graph was reconciled and fully destroyed
without a cleanup error.

Measured job `98a16f2c3e8a` then passed the complete live path in
`us-east-1`/`us-east-1a` with a 4-vCPU/16-GiB `m7i.xlarge` Rocky Linux 9
application, the exact four-node K3s cluster, and a separate x86_64
`m7i.large` load generator. It proved 962 Reed98 users, 37,624 follow edges,
and 9,424 posts; completed a 30-second warm-up and all 5,994 measured requests
at the target 100 requests/second (99.893931 observed); recorded p50/p95/p99
latency of 3.711/6.555/8.527 ms with zero errors, timeouts, or uncompleted
requests; and proved all 27 Pod identities unchanged with zero restarts. Strict
result, comparison, `results.json`, and report gates passed. Exact cleanup
deleted the full graph, and a post-restart cleanup-only audit also passed with
final `destroyed` state and no error or cleanup error.

Fresh artifact-backed measurement-v2 job `46fa02564ca2` passed on 2026-09-30
with the final published driver. It completed all 5,994 requests at 99.892941
requests/second with p50/p95/p99 latency of 3.707/6.295/7.791 ms, 100 percent
completion, zero errors, and unchanged Pod identity. Automatic cleanup passed.
Independent tag-scoped inventory at `2026-09-30T23:20:44Z` covered the new run
and all three formal AWS failure-matrix runs and found zero live resources in
every graph category. EC2's retained `terminated` instance and `deleted` NAT
records are terminal provider history, not live ownership.

The representative checkpoint failure matrix passed on 2026-09-30:

- Safe pre-initialization job `3249984b8909` hard-exited with status 86 after
  durably reaching `load_generator_ready`. Its interruption evidence recorded
  `resume_allowed` and `not_attempted_process_exit`, and the run lease was
  unheld. A fresh process resumed the same graph, produced the exact Reed98
  dataset, and completed all 5,994 measured requests at 99.890920 requests per
  second with p50/p95/p99 latency of 3.793/6.515/8.131 ms. All error and
  timeout counters were zero, and all observed Pods retained their identities
  with zero restarts. Cleanup deleted every resource. A subsequent
  cleanup-only terminal audit left the final evidence as
  `cleanup_completed`/`completed`.
- Initializer-response-loss job `26c5d0701087` reconciled the durable systemd
  initializer invocation `ba21b4c678834d5296d476eb5a95f5a5` without replay.
  It proved the exact Reed98 dataset and completed all 5,994 requests at
  99.894819 requests per second with p50/p95/p99 latency of
  4.103/6.963/8.679 ms, zero errors, zero restarts, and unchanged Pod
  identities. Its full 21-resource graph and all five role nodes were deleted.
- Unsafe post-initialization job `4c2e0c6feafd` hard-exited with status 86 at
  `warmup_started`. It retained the initialized Reed98 database, no result
  artifact, an unheld lease, and the fail-closed `cleanup_only_required`
  decision. A fresh `--resume` refused with status 2 and changed only the
  recovery outcome to `resume_refused`; the contract, inventory, and execution
  graph hash remained
  `9962627111076e87934995bedc1da0e5a4e176a4483235fdaf5b2e676fb937da`.
  Explicit `--cleanup-only` then deleted the 21-resource graph and all five
  role nodes and finalized `cleanup_completed`/`completed` evidence.

This is a representative checkpoint failure matrix, not exhaustive failure
coverage. AWS does not yet claim live active-signal or cleanup-interruption
qualification. Those paths remain synthetically covered but are outside this
matrix.

Operator execution must use
`scripts/qualify_aws_oci_deathstarbench_distributed.py`; the provider helpers
do not create or manage a process lease themselves. The operator-path
prerequisite gates, public UI/API lifecycle integration, and coordinated
release-flag transition are complete.
