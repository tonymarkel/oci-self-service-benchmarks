# OCI distributed infrastructure candidate

This is an **operator-only, infrastructure-only candidate**, not a released
benchmark mode. The normal UI/API/provider dispatch and release gate are
unchanged. No guest preparation, K3s bootstrap, workload installation,
measurement, or cloud qualification is included in this slice.

`app.providers.oci` is separate from the existing compact OCI implementation.
Its two entrypoints are `provision_distributed_deathstarbench_candidate` and
`destroy_distributed_deathstarbench_candidate`. They require an exclusive run
lease held by the caller, a durable `persist(job)` callback, and authenticated
OCI `compute`, `network`, and `block` SDK clients pinned to the input region.
There is intentionally no public CLI until the operator lease/recovery wrapper
and live qualification are implemented.

## Explicit inputs and layout

The input dictionary requires `compartment_id`, `availability_domain`, `region`,
`shape`, `architecture`, `ocpus`, `memory_gb`, `application_image_id`,
`support_image_id`, and a single SSH `public_key`. Credentials and private keys
are never plan values. Image OCIDs must refer to compatible Oracle Linux 9
platform images. Image details and compatibility are read before cloud writes;
there is no moving image lookup or Marketplace agreement acceptance. The guest
login contract is `opc`; guest/kernel qualification is a subsequent slice.

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

The stateful fake suite exercises normal lifecycle, implicit resources, response
loss, partial creation, foreign dependencies, immutable-contract drift, and
ETag-conditioned deletion. It makes no cloud requests. Live OCI provisioning,
network reachability, failure injection, Oracle Linux/UEK guest preparation,
and K3s/workload execution are still required before any release decision.

SDK reference: [OCI Core clients](https://docs.oracle.com/en-us/iaas/tools/python/latest/api/core.html).
