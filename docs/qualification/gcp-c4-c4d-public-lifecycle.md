# GCP C4/C4D qualification

Date: 2026-10-05. Compact C4/C4D and distributed C4 measurements completed.
Distributed C4D failed during dataset initialization on two fresh clusters;
that release gate remains unresolved. This is a candidate qualification
record, not a claim that every live gate passed.

## Supported profile

Normal C4 and C4D standard/highmem/highcpu machine types use the existing
Hyperdisk Balanced profile: 3,000 IOPS, 140 MiB/s, NVMe attachments, gVNIC, and
the x86_64 Rocky Linux 9 image. C4A keeps the corresponding Arm64 profile.
Architecture comes from the API, not a guess based on the machine name.
Local-SSD, bare-metal, accelerator, contradictory-architecture, and other
unimplemented Hyperdisk-only variants remain excluded before provisioning.
C3 retains its existing catalog, disk profile, and UI defaults.

These requirements follow Google's [machine-series documentation](https://docs.cloud.google.com/compute/docs/general-purpose-machines).
The [Hyperdisk Balanced baseline](https://docs.cloud.google.com/compute/docs/disks/hd-types/hyperdisk-balanced#baseline_performance)
does not consume provisioned IOPS/throughput quota; disk capacity and CPU quotas
still apply.

## Public lifecycle runs

Every canary uses the normal `create_job` lifecycle in `us-east1-b`, with
automatic destruction enabled and `rocky-linux-9-v20260910` pinned by the
provider after image-family resolution.

| Mode | Run | Application/runner shape | vCPUs / GiB | Results |
| --- | --- | --- | --- | --- |
| Compact | `b99d898a2619` | `c4-standard-4` | 4 / 15 | All six completed |
| Compact | `a0cf0b75ae1c` | `c4d-standard-8` | 8 / 31 | All six completed |
| Distributed | `770b0915d3d1` | `c4-highmem-4` | 4 / 31 | 218/218 requests; 21.792516 req/s |
| Distributed | `15d1a20c89dd` | `c4d-standard-8` | 8 / 31 | Dataset initialization failed; no measurement |

The compact selections are Sysbench CPU, fio, Sysbench file I/O, iperf TCP,
ApacheBench new connections, and ApacheBench keep-alive. Storage results report
zero errors and the exact provisioned `/data` target, Hyperdisk Balanced,
3,000 IOPS, and 140 MiB/s. Independent live API snapshots confirmed the runner
and same-size peer use gVNIC/NVMe; runner boot/data are 100 GiB and peer boot is
20 GiB. The fixed web load generator remains `n2-standard-2`, with a 20-GiB
`pd-balanced` SCSI boot disk.

Distributed runs use the released `distributed_tiered_v1` / `k3s_v1` Social
Network contract, no warm-up, a ten-second measurement, two threads, four
connections, and an offered rate of 20 requests/second. The selected application
uses a 50-GiB Hyperdisk boot disk at the same baseline. Control, database,
cache, and load-generator roles retain their fixed N2 shapes and 50-GiB
`pd-balanced` boots; database data remains 100-GiB `pd-ssd` attached over SCSI.
Independent live C4 and C4D snapshots confirmed this separation. The C4
measurement reported 100% completion with zero HTTP, connection, read, write,
timeout, socket, or uncompleted-request errors. It generated HTML/JSON artifacts
and completed automatic cleanup.

## Cleanup and failure evidence

Both compact runs and the successful C4 distributed run reached `destroyed`
with complete benchmark status, no run or cleanup error, HTML/JSON artifacts
present, and no recoverable resources when loaded in a fresh process.
Independent CLI inventories found no matching instances, disks, networks,
subnets, firewalls, routers, or addresses.

Two preliminary rejection probes also have independent zero-resource receipts:
`21b9ee5bc139` rejected an incorrect submitted memory value before creation;
`e958efed6ce0` hit the project's `CPUS_ALL_REGIONS` limit of 32 while adding
the peer for overlapping eight-vCPU compact graphs, then automatically removed
the partial graph. Neither rejection produced benchmark results.

Initial distributed attempt `74d52d1faca5` exceeded the regional
`SSD_TOTAL_GB` quota of 500 while overlapping another cluster; fixed support
boot disks and database data consume 300 GiB per complete graph. Attempt
`fe91d4331a53` passed guest/K3s, workload, and required/forbidden network-path
qualification but failed during follow uploads in dataset initialization with
`aiohttp.ServerDisconnectedError`. Both were automatically destroyed, have no
recoverable resources in fresh-process checks, and have independent zero-live
resource receipts. No measurements were produced. Fresh sequential clusters
were used instead of replaying partially initialized datasets; the pinned
initializer and reproducibility contract remain unchanged.

The fresh C4D retry `15d1a20c89dd` also passed provisioning, guest/K3s,
workload-readiness, and required/forbidden network-path checks, but stopped in
the initializer after reporting 962 successful user registrations. Follow
uploads returned HTTP 500 with a Thrift transport exception. Bounded read-only
diagnostics captured before destruction showed all workload pods running with
zero restarts, frontend socket read timeouts, and Social Graph's
`getaddrinfo(user-service:9090)` name-resolution failure. CoreDNS was Ready
with zero restarts on the fixed N2 control node, the expected `10.43.0.10`
Service and live DNS endpoints; its logs did not establish the failed DNS
path's cause. This is not evidence of a quota rejection or a machine/storage
restriction. The exact backend DNS/transport failure remains unresolved.
Automatic cleanup completed with no cleanup error and no recoverable
resources in a fresh-process check. An independent final inventory for all
eight runs in this record found zero matching resources across all seven
resource types. All eight are terminal `destroyed`; the five failed runs
retain failed benchmark status and no measurement artifacts, rather than
being relabeled successful because cleanup completed.

Before marking this candidate ready for release, reproduce and diagnose the
application-to-DNS/backend path on C4D, apply a reviewed fix with any required
runtime/workload revision updates, and obtain a successful fresh measurement
plus independent terminal cleanup evidence. Do not conceal the failure with
an initializer replay, relaxed validation, or silent workload changes.

The canaries exposed a provenance-only bookkeeping issue: a retained
`gcp_loadgen_image_id` was mistaken for owned infrastructure. It is now
explicitly recognized as image provenance. A regression verifies persisted
status/history becomes terminal after cleanup while real instance identities,
ownership anchors, and response-lost request IDs still require recovery.

Synthetic tests cover C4/C4D discovery and direct rejection, compact full-graph
provisioning, fixed N2 helpers, C3 regression, JSON restart/replay, and complete
cleanup. Distributed cleanup reconstructs required interfaces from validated
saved role shape/architecture and rejects NIC, boot-interface, IOPS, or
throughput drift before deletion. Local repository checks ran 1,259 tests with
one existing host-specific skip; GitHub CI passed on Python 3.10 and 3.14.
