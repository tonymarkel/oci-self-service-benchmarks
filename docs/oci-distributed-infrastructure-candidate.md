# OCI distributed infrastructure candidate

This is an **operator-only candidate**, not a released benchmark mode. The
normal UI/API/provider dispatch and release gate are unchanged. A separate
operator wrapper now connects the infrastructure adapter to Oracle Linux guest
preparation, the shared K3s bootstrap, Social Network workload, network
qualification, optional measurement path, resume, and cleanup-only machinery.
See `docs/deathstarbench-distributed.md` for the complete current contract and
live qualification evidence.

`app.providers.oci` is separate from the existing compact OCI implementation.
Its two entrypoints are `provision_distributed_deathstarbench_candidate` and
`destroy_distributed_deathstarbench_candidate`. They require an exclusive run
lease held by the caller, a durable `persist(job)` callback, and authenticated
OCI `compute`, `network`, and `block` SDK clients pinned to the input region.
Direct use still requires the caller-held lease; the supported operator
entrypoint is `scripts/qualify_aws_oci_deathstarbench_distributed.py`.

## Explicit inputs and layout

The input dictionary requires `compartment_id`, `availability_domain`, `region`,
`shape`, `architecture`, `ocpus`, `memory_gb`, `application_image_id`,
`support_image_id`, exact `defined_tags`, and a single SSH `public_key`.
Credentials and private keys are never plan values. Image OCIDs must refer to
compatible Oracle Linux 9 platform images. Image details and compatibility are
read before cloud writes; there is no moving image lookup or Marketplace
agreement acceptance. The guest login contract is `opc`. The wrapper accepts
the tag map from `--oci-defined-tags-json`, persists it in the immutable
provider pin, applies it to every taggable resource, and rejects drift during
resume and cleanup. An empty object is valid only when tenancy policy permits
it.

The five nodes occupy one AD in a dedicated `10.240.0.0/16` VCN:

| Role | Private address | Shape/capacity | Public IP |
| --- | --- | --- | --- |
| Control | `10.240.1.10` | E5 Flex, 1 OCPU / 4 GiB | Yes |
| Application | `10.240.1.13` | Selected supported Standard Flex shape | Yes |
| Load generator | `10.240.2.10` | E5 Flex, 1 OCPU / 8 GiB | Yes |
| Database | `10.240.3.11` | E5 Flex, 2 OCPU / 16 GiB | No |
| Cache | `10.240.3.12` | E5 Flex, 1 OCPU / 8 GiB | No |

Public management/load-generator subnets use an internet gateway. The private
data subnet uses a NAT gateway. Each subnet explicitly uses the empty managed
security list and the VCN's captured default DHCP options. Five role NSGs derive
all workload reachability rules from the persisted topology's channels: cluster
UDP/8472, worker-to-control TCP/6443, application-to-cache TCP/6379 and TCP/11211,
application-to-database TCP/27017, and load-generator-to-application TCP/8080.
Explicit management rules add SSH to public roles and control/application SSH
to database/cache. Other inbound connections have no allow rule. Public SSH
ingress remains `0.0.0.0/0`, matching the
existing operator choice. These rules are not substitutes for Kubernetes
NetworkPolicies or live negative-path qualification.

The database owns a separate 256-GiB block volume at 20 VPUs/GB, represented as
19,200 IOPS and 146.484375 MiB/s in inventory. Attachment is explicitly
paravirtualized, nonshareable, writable, at `/dev/oracleoci/oraclevdb`. It is
independently tracked and deleted after detachment/instance termination. Every
boot volume is captured and explicitly deleted after termination rather than
depending on implicit deletion.

## Failure and cleanup behavior

- Before the first mutation, persist the topology, role inventory, full
  deterministic create graph, and UUID retry tokens. Immediately before each
  request, persist its attempt timestamp. Immediately after acceptance, persist
  its OCID.
- A lost create response is reconciled by exact name, ownership tags, parent,
  immutable request fields, and the saved creation-time window. An ambiguous
  absent create is **not replayed**, even if a retry token has expired. It
  retains state and needs later reconciliation/operator investigation.
- Explicit allowlisted OCI 4xx non-acceptance responses return only the rejected
  create intent to `planned`, recording its status/code and attempt timestamp.
  Already-created resources remain cleanable. Unknown 4xx codes, ambiguous
  `NotAuthorizedOrNotFound`, connections/timeouts, and 5xx outcomes never use
  that shortcut. SDK automatic retries are disabled for creates and NSG rule
  additions, so a final rejection cannot hide an earlier ambiguous SDK replay.
- NSG rule additions have no retry token. A response-lost addition is accepted
  only when the exact expected rule set is observed; partial/extra rules and
  uncertain empty results fail closed.
- Capture default VCN children, VNICs, boot volumes, and their attachments.
  First capture checks exact GET/list identities, expected compartment and AD
  where applicable, and each parent/subnet relationship before adoption.
  Before cleanup and each delete, recheck ownership, relationships, inventory,
  NSG membership/rules, and child graph. Use current ETags for deletes, preserve
  delete intent after interruption, and confirm absence before advancing.
  A saved delete intent does not suppress attachment checks while the cloud
  instance remains live; only observed termination allows those checks to stop.
  Observed deletion/termination requires an owned, timestamped delete intent
  in the whole-graph preflight; otherwise cleanup refuses before any mutation.
- Retry a delete after `NoEtagMatch` only after refreshing and re-auditing the
  full graph. If three conditioned deletes fail while OCI returns an unchanged
  exact target payload with a fresh nonempty ETag on every GET, the optional
  no-If-Match form is allowed only after two more full-graph audits and two
  identical nonterminal target payloads. Both nonempty ETags are recorded and
  may differ. The one fallback delete disables SDK retries; any unstable
  payload still fails closed.
- Cleanup enumerates child resources in the configured compartment. Operators
  must not share the dedicated VCN or move its children across compartments.
  This is not tenant-wide discovery; OCI dependency checks still prevent parent
  deletion when unseen cross-compartment children remain. A failed cleanup is
  retained, never reported as complete. Broader recovery tooling is a remaining
  qualification item.
- The completed contract and deleted role inventory remain as recovery/audit
  evidence. Repeated cleanup is idempotent; provisioning cannot recreate a
  deleting/deleted contract.

## Validation and remaining gates

The stateful fake suite exercises normal lifecycle, implicit resources,
response loss, partial creation, foreign dependencies, immutable-contract
drift, required defined tags, and guarded ETag deletion. Operator wrapper job
`b610d368e781` passed live OCI provisioning, Oracle Linux 9 DNS/swap/SELinux
preparation, exact database mounting, K3s readiness, Social Network workload
readiness, required and forbidden network probes, and terminal cleanup on
2026-09-29. It did not use `--measure` and is not a benchmark result.

Measured job `52fd33e7a66f` then passed the complete live path in
`us-ashburn-1`, `DfpY:US-ASHBURN-AD-1`, with a 4-OCPU/32-GiB
`VM.Standard.E5.Flex` application node, the exact four-node K3s cluster, and a
separate fixed load generator. It proved 962 Reed98 users, 37,624 follow edges,
and 9,424 posts; completed a 30-second warm-up and all 5,994 measured requests
at the target 100 requests/second (99.894964 observed); recorded p50/p95/p99
latency of 3.329/5.663/7.191 ms with zero errors, timeouts, or uncompleted
requests; and proved all 27 Pod identities unchanged with zero restarts. Strict
result, comparison, `results.json`, and report gates passed.

After process restart, cleanup-only initially refused its terminal audit because
OCI had legitimately advanced boot-attachment `time_updated`. Terminal
validation was corrected to retain only genuinely immutable boot-attachment
identity and relationship fields. The rerun proved every resource absent,
finalized `destroyed`, and cleared `cleanup_error`.

Safe pre-initialization recovery and initializer response loss were then
live-qualified together on 2026-09-30 as job `327749bd33cb`. The first process
hard-exited with status 86 after durably reaching `load_generator_ready`; the
evidence recorded `resume_allowed` and `not_attempted_process_exit`, and the
lease was unheld. A fresh process resumed the same graph and reconciled durable
initializer invocation `edbb209e98244812aec91af251fcef0c` after injected
response loss without replay. It proved the exact Reed98 dataset, completed all
5,994 measured requests at 99.894792 requests per second with p50/p95/p99
latency of 3.089/4.831/5.991 ms, and recorded zero errors, zero restarts, and
unchanged Pod identities. Exact cleanup deleted all 21 OCI graph entries and
all five role nodes; evidence finalized as `resume_completed` and `completed`,
with no remaining recoverable ownership.

Unsafe post-initialization recovery was live-qualified on 2026-09-30 as job
`c69567748b43`. The first process hard-exited with status 86 at
`warmup_started` after proving the exact Reed98 dataset and capturing all 27
pre-measurement Pod identities. It retained no result or report artifact,
released its lease, and recorded
`cleanup_only_required`/`not_attempted_process_exit`. A fresh `--resume`
refused with status 2; only the evidence recovery outcome changed to
`resume_refused`, while the contract, inventory, and execution graph hash
remained
`312c480bc1006ebebfb03d461231d5d1d0276f4a20a602dd72a9f1765220ccd6`.
Explicit `--cleanup-only` deleted and audited all 21 graph entries and all five
role nodes, finalized `cleanup_completed`/`completed`, released the lease, and
projected no recoverable ownership.

This completes OCI's representative checkpoint failure matrix. OCI does not
claim live active-signal or cleanup-interruption qualification; those paths
remain synthetically covered but are outside this matrix. The candidate remains
unreleased. The broader remaining release gates are representative Azure
failure paths, publication or equivalent qualification of an immutable load
driver, and final cross-provider cleanup review.

SDK reference: [OCI Core clients](https://docs.oracle.com/en-us/iaas/tools/python/latest/api/core.html).
