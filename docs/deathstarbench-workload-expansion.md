# Distributed Hotel Reservation and Media Microservices

Status, 2026-10-07: **foundation candidates, not public benchmarks**. Social
Network is still the only released distributed workload. No new image
publication, cloud provisioning, dataset initialization, measurement, or
cleanup qualification is claimed by this slice.

## Completed foundation

The existing five-role cloud topology and pinned K3s runtime are reusable.
The new workload registry resolves immutable identities explicitly, with no
process-global current workload. Parallel renders cannot exchange namespaces,
image selections, dependency graphs, or metadata. Social Network retains its
original revision strings, assets, renderer, public dispatch, and comparison
identity.

| Candidate | Application processes | Database processes | Cache processes | Control processes | Frontend |
| --- | ---: | ---: | ---: | ---: | --- |
| Hotel Reservation | 10 | 8 MongoDB | 4 memcached | Consul and Jaeger | NodePort/service 8080 to pod 5000 |
| Media Microservices | 13 | 8 MongoDB | 7 memcached and 3 Redis | Jaeger | NodePort/service/pod 8080 |

These are pod processes distributed across **five VMs**, not one VM per
component. The fifth VM is the separate load generator. Only the application
role follows the selected shape's x86/Arm architecture; supporting roles
retain the existing fixed x86 contracts.

The pinned-source audits and schema-v2 candidate assets live in:

- [Hotel Reservation audit](../app/manifests/deathstarbench/hotel-reservation-v1/upstream-audit.md).
- [Media Microservices audit](../app/manifests/deathstarbench/media-microservices-v1/upstream-audit.md).

Each component and network asset is bound to a literal audited SHA-256 in
`app/deathstarbench_workload_contract.py`. The candidate validator rejects
byte drift, crossed workload/revision identities, unexpected component
placement, storage or frontend mappings, and incomplete platform image
inventories. The original upstream request scripts have separate pinned
hashes. Candidate dataset, driver, and measurement revisions explicitly use
`candidate` markers: a future correctness-patched method needs a reviewed
identity, not an unrecorded change to these names.

`app/deathstarbench_candidate_workload.py` can render deterministic **offline
previews** from operator-supplied platform-manifest references. It performs
no network requests or writes and has no apply, deploy, or measurement entry
point. Its preview image schema is not the public release-lock schema. A
syntactically valid digest does not prove registry publication, platform
content, pull access, or a working benchmark. Example digests exist only in
test fixtures, never as checked-in qualification evidence.

The renderer produces namespace, storage, policies, services, and deployment
phases. It uses exact role/architecture selectors, one replica per component,
no service-account token, separately pre-bound 10-GiB claims on the database
disk, paired ingress/egress rules for every audited dependency, default deny,
CoreDNS-only DNS access, and one private load-generator `/32` frontend rule.
The eight database claims total 80 GiB within the current 100-GiB disk;
guest free-space/storage attestation is still a later runtime requirement.
Hotel's pod policy uses port **5000**, not the external NodePort 8080. No
cloud ingress or K3s NodePort-range expansion is needed.

Both new profiles and assets remain unreleased. The existing public model,
catalog, lifecycle, and released bundle validator continue to reject them;
an asset flag or candidate preview cannot open their gates. Historical
compact API clients, reports, comparisons, and cleanup remain supported.

## Next review slices and release prerequisites

1. **Reproducible images and load drivers.** Prepare drift-checked native
   amd64/arm64 application/frontend contexts and immutable backing-image
   references. Adopt and test required source corrections. Publish exact
   platform manifests and workload-specific x86 load drivers, with source,
   patched script, binary, context, license, and anonymous-pull evidence.
2. **Workload-aware execution and measurement.** Pass the selected immutable
   profile through deployment/readiness, network qualification, database
   preparation, run-bound lock snapshots, journals, recovery, and cleanup.
   Add exact dataset and semantic response verification, durable one-shot
   initialization, unchanged pre/post pod identities, and workload-specific
   comparison cohorts. Do not populate Hotel/Media metadata with fabricated
   Social Network Reed98 hashes.
3. **Live qualification and public release.** Verify permitted and denied
   network paths, semantic transactions, warmup/measurement accounting,
   interruption/response loss, stop/destroy/restart recovery, and independently
   absent resources on AWS, OCI, GCP, and Azure. Advertise a workload only
   after its own coherent artifact/runtime/measurement gates pass. Releasing
   one candidate must not disable the already released Social Network path.

The Hotel audit records upstream request/data correctness issues, including
reservation coordinates, seeded username formatting, Consul dial targets,
cache-miss behavior, and restart-sensitive seeding. HTTP 200 alone is not a
successful login or reservation. Media needs the reviewed initialization and
frontend adaptations, deterministic input identity, and proof that a composed
review reaches persistent storage and its user/movie references. These fixes
are explicitly **not** implemented or qualified by the foundation assets.

Maintainers review each slice before merge. No automatic merge or release-flag
promotion is part of this workflow.
