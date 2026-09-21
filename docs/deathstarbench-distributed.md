# Distributed DeathStarBench design

Status: the foundation, Azure five-node infrastructure, the unreleased GCP
five-node Compute Engine candidate, provider-neutral four-node K3s bootstrap,
deterministic Social Network deployment, candidate-image publication,
deterministic Reed98 initialization, bounded warm-up, measured traffic,
reporting, exact provider cleanup, shared cross-process ownership, and
failure-injection machinery are implemented. One safe pre-initialization Azure
hard-exit/reacquisition/resume/cleanup path has also passed live qualification.
The GCP candidate has synthetic lifecycle and operator-harness coverage, but
has not yet passed live qualification. `distributed_tiered_v1` is still
unreleased for normal cloud provisioning. The UI does not offer it, and the
normal API/provider path rejects it before creating a run or making a cloud
write. Representative negative network probes, live post-initialization
cleanup-only and signal paths, immutable load-driver publication or equivalent
qualification, GCP live qualification, and qualification on AWS and OCI remain
release gates.

## Benchmark modes

`single_host_v1` remains the backward-compatible default. It measures the
selected compute shape as a consolidated Podman host while a separate fixed
x86 load generator sends private-network traffic.

`distributed_tiered_v1` will use this fixed five-node graph:

```text
load_generator -> application/frontend -> cache
                              |
                              +---------> database + persistent storage

control: K3s API, scheduling, discovery, and lightweight support services
```

The first release keeps the frontend and stateless application services on one
node. A later topology revision can split those services and add replicas; it
must not silently change `distributed_tiered_v1`.

## Reproducibility contract

Every result records:

- topology ID and topology revision;
- runtime ID and exact runtime revision;
- service-placement revision and logical roles;
- DeathStarBench upstream revision;
- workload, warm-up, duration, threads, connections, and offered rate; and
- provider-observed hardware and storage as provenance.

The shared Azure/GCP candidate pipeline additionally pins dataset revision
`social-network-socfb-reed98-compose-seed1-v1`, load-driver revision
`wrk2-6ecb097-native-v1`, and measurement revision
`social-network-distributed-measurement-v1`. It records the exact hashes of
the source graph, patched initializer, request script, built load-driver
binary, compiler version, rendered workload, pod execution identity, raw
output, and normalized metrics. Distributed comparison fails closed unless
all workload, image-lock, rendered-manifest, dataset, driver, measurement,
initializer, graph-input, and request-script identities are present and
valid; a changed identity creates a separate comparison cohort.

The distributed runtime uses an exact K3s release. It must never resolve the
moving `stable` or `latest` channels during a run. A runtime cannot be marked
released until its binary checksums, images, manifests, CNI configuration, and
component versions are pinned and qualified. Runtime revisions form separate
automatic comparison cohorts.

The current candidate pins K3s `v1.36.4+k3s1`, its architecture-specific
binary, and its matching architecture-specific air-gap system-image bundle by
SHA-256. The independent EL9 `k3s-selinux` 1.6-1 RPM is downloaded from its
exact release URL and verified against a source-recorded SHA-256. Workload
revision `social-network-6ecb097-workload-v1` is generated from checked-in
component and policy assets audited against DeathStarBench commit
`6ecb09706140f8730b5385c08f1386c654c3c526`. Its image-lock schema requires an
exact per-platform digest for every custom and supporting image; the candidate
lock was published and qualified in Azure, but deliberately remains marked
unreleased. The exact candidate is checked in at
`docs/qualification/deathstarbench-social-network-images-v1.json`.
Provider guest preparation is an explicit qualification item: SELinux must be
enforcing with the expected policies and labels, cgroup v2 and swap state must
match the contract, and the required kernel modules and sysctls must pass the
same preflight on every supported image.
There is no in-place upgrade during a benchmark. A later Kubernetes release is
a new runtime revision, is qualified in CI and cloud smoke tests, and forms a
new comparison cohort; older report metadata remains readable.

Historical results without execution markers normalize only to the original
`single_host_v1` / `podman_compose_v1` contract. Partially marked results and
distributed results without an exact runtime revision fail closed.

## Provider-neutral resource inventory

Multi-node resources are represented by the versioned
`resources.role_node_inventory` contract. Each node has a stable logical key,
role, provider identity, addresses, placement, shape, architecture, storage,
and lifecycle status. Serialization is deterministic, and an unknown schema
version fails closed so cleanup cannot silently lose a resource.

The target roles are:

- `load_generator`
- `control`
- `application`
- `cache`
- `database`

Providers must persist the planned logical node before the create request,
then persist the provider request/idempotency contract and accepted resource
identity immediately. Existing flat runner and load-generator fields remain
readable during migration.

Inventory updates preserve previously accepted VM, address, disk, and device
identities and reject replacement. A node can be forgotten only if it was
never created or its deletion is confirmed. The inventory also carries the
topology fingerprint and each role's versioned capacity class so recovery
cannot lose the plan it is cleaning up.

## Azure synthetic infrastructure pilot

Azure is the first provider with an internal, unreleased five-node
infrastructure candidate. It is deliberately separate from the normal UI and
API provisioning route. The infrastructure hook stops after creating and
attesting the cloud boundary; separate internal hooks bootstrap K3s and deploy
the workload from the persisted inventory. The existing compact Azure
DeathStarBench path now dual-writes its runner and load-generator state to both
the legacy flat fields and the versioned role inventory before its first
resource-group write.

The candidate uses these role contracts:

- `control`, `cache`, and `load_generator`: `Standard_D2as_v7`;
- `database`: `Standard_D4as_v7` plus a 100-GiB Premium SSD v2 data disk at
  3,000 IOPS and 125 MiB/s; and
- `application`: the benchmark shape selected by the user.

The Azure underlay is `10.240.0.0/16`, split into a `10.240.1.0/24` cluster
subnet and a `10.240.2.0/24` load-generator subnet. This intentionally avoids
the K3s defaults used by the candidate runtime (`10.42.0.0/16` for pods and
`10.43.0.0/16` for services). Private addresses are deterministic, and only
the control, application, and load-generator nodes receive public addresses.
Separate NSGs admit only the required management, K3s underlay, and
load-generator-to-application paths, followed by an explicit deny for other
VNet inbound traffic. These cloud rules do not replace the checked-in
Kubernetes NetworkPolicies; those policies remain a gate for the workload
deployment slice.

Both subnets explicitly disable Azure's implicit default outbound access. A
run-owned zonal Standard NAT Gateway and Standard public IP provide stable
outbound access for the cluster subnet, including the private database and
cache nodes. The load-generator subnet uses its load generator's explicitly
assigned Standard public IP. This keeps package, runtime-artifact, and image
downloads independent of Azure's changing default-outbound behavior.

Before the first cloud write, the candidate persists the complete topology,
resource inventory, deterministic identities, and exact application/support
image IDs, URNs, versions, and architectures. The run-owned resource group is
the atomic cleanup boundary. Retry and cleanup validate the persisted
contract, ownership tags, observed resource specifications, and response-loss
reconciliation before reusing or deleting resources.

SKU and disk capability discovery validates the requested per-resource
placement, but it does not guarantee aggregate subscription quota or live
regional capacity. Those conditions can still fail after partial creation;
the persisted contract makes that state recoverable. In addition to synthetic
lifecycle and failure-injection coverage, runtime v5 has now completed the
internal live Azure qualification described below.

## GCP synthetic infrastructure candidate

GCP now has an internal, unreleased five-node Compute Engine candidate. Like
the Azure pilot, it is callable only from the operator qualification harness
and internal hooks; the normal UI, API, and provider dispatcher remain closed.
It persists the provider-neutral topology manifest, fingerprint, role-node
inventory, deterministic resource names, request UUIDs, and pinned Rocky Linux
9 image identities before the corresponding cloud writes. The same topology,
K3s runtime, rendered Social Network workload, dataset, measurement, and result
contracts used by Azure are then selected through provider-neutral dispatch.
GCP-specific code owns only the cloud lifecycle and guest device discovery.

All five instances are placed in one selected zone. The exact role contract is:

- `control`, `cache`, and `load_generator`: `n2-standard-2`, x86_64;
- `database`: `n2-standard-4`, x86_64, plus the database disk described below;
  and
- `application`: the supported machine type and architecture selected by the
  operator. The qualifier defaults to Arm64 `c4a-standard-8` with 8 vCPUs and
  32 GiB, but this default is not a release promise for every region.

Every node receives a 50-GiB boot disk and the Rocky Linux 9 image pinned for
its architecture. N2 support nodes use `pd-balanced`; the default C4A
application uses its required Hyperdisk Balanced, NVMe, and gVNIC profile with
3,000 provisioned IOPS and 140 MiB/s. A different selected application shape
must pass the provider's exact architecture, capacity, disk-interface, and NIC
profile validation before the first instance write. Support roles remain x86_64
even when the application role is Arm64.

The database receives a separate 100-GiB zonal `pd-ssd` attached over SCSI with
`autoDelete` disabled. Persistent Disk performance is service-derived rather
than provisioned per disk, so the flat GCP fields deliberately record no
provisioned IOPS or throughput while the provider-neutral inventory retains the
topology minimum of 3,000 IOPS and 125 MiB/s. The attachment has a deterministic
device name, `benchmark-<job-id>-dsb-database-data`, and the guest resolves only
its exact `/dev/disk/by-id/google-<device-name>` link. It never enumerates or
guesses among unused disks. The preparation hook rejects the boot-device
ancestry, partitions or child devices, foreign mounts and fstab entries, and
non-XFS filesystems. It formats only a blank whole disk, mounts it by UUID at
`/var/lib/deathstarbench/database`, and persists the device name, link,
filesystem, UUID, and mount point as a non-secret attestation. Every later
workload retry re-attests those values read-only before touching MongoDB paths.

The custom-mode VPC is `10.240.0.0/16`, with a `10.240.1.0/24` cluster subnet
and `10.240.2.0/24` load-generator subnet. As on Azure, neither range overlaps
the pinned K3s pod CIDR `10.42.0.0/16` or service CIDR `10.43.0.0/16`. The
deterministic addresses are:

| Role | Private address | Public address |
| --- | --- | --- |
| `control` | `10.240.1.10` | ephemeral Premium-tier IPv4 |
| `database` | `10.240.1.11` | none |
| `cache` | `10.240.1.12` | none |
| `application` | `10.240.1.13` | ephemeral Premium-tier IPv4 |
| `load_generator` | `10.240.2.10` | ephemeral Premium-tier IPv4 |

A regional Cloud Router and auto-allocated Cloud NAT serve only the cluster
subnet, so private database and cache nodes can retrieve the pinned runtime and
images without public addresses. The load generator uses its own public
address for outbound access. Five run-owned ingress firewall rules use exact
role tags: public SSH reaches control, application, and load generator from the
currently configured `0.0.0.0/0` management scope; private SSH reaches database
and cache only from control and application; workers reach control on TCP/6443;
cluster nodes exchange Flannel traffic on UDP/8472; and only the load generator
reaches the application NodePort on TCP/8080. Kubernetes NetworkPolicies remain
the independent pod-level boundary.

Unlike Azure, GCP has no run-owned resource group to use as one atomic deletion
boundary. The cleanup boundary is therefore the exact persisted graph: five
instances, five auto-delete boot disks, the non-auto-delete database disk, five
firewall rules, Cloud Router/NAT, two subnets, and the VPC. Cleanup first proves
the immutable project identity, topology and inventory fingerprints,
deterministic names, resource IDs and self-links, ownership labels and
descriptions, network relationships, machine types, tags, addresses, public-IP
policy, disk attachments, and response-loss request UUIDs. A missing resource
with an unresolved create is ambiguous and causes cleanup to retain ownership
for retry instead of guessing. After revalidation, cleanup deletes nodes in the
manifest's frontend-first order, then the database disk, boot disks, firewall
rules, router, subnets, and VPC. It clears the GCP ownership and runtime journals
only after independent lookups prove every expected resource absent.

## Internal K3s bootstrap (Azure and GCP candidates)

The internal runtime hook reloads and validates the persisted topology,
fingerprint, five-node role inventory, fixed provider role identities, private
addresses, shapes, architectures, and database disk before its first SSH
process. Control is reached directly; database, cache, and application are
reached at their private addresses through a local OpenSSH proxy on the exact
control public address. The GCP application has a public address for its cloud
contract, but runtime and workload management deliberately use its private
address through control so the execution path matches Azure. The private key
never leaves the orchestrator, and the jump and destination share the run-owned
known-hosts database and cancellation boundary.

The four cluster hosts are prepared in manifest order. Rocky Linux 9 and its
exact SELinux policy are verified, and firewalld is disabled inside the strict
provider firewall boundary: the existing Azure NSGs or the role-tagged GCP
rules. Required modules/sysctls are persisted, and the architecture-specific
K3s binary plus air-gap bundle are checksum-verified.
On Azure the database disk is resolved only through LUN 0's fixed NVMe/SCSI
links. On GCP it is resolved only through the persisted
`/dev/disk/by-id/google-*` link described above. In both cases it is formatted
only when blank, mounted by XFS UUID at
`/var/lib/deathstarbench/database`, and recorded as a non-secret attestation.

The control plane uses exact pod/service CIDRs and Flannel VXLAN. Traefik,
ServiceLB, metrics-server, and local-storage are disabled for this runtime
slice. The control node retains `deathstarbench.io/role=control` and uses the
standard `node-role.kubernetes.io/control-plane=true:NoSchedule` taint so the
CoreDNS deployment can tolerate it. Before workers join, the orchestrator
patches CoreDNS with an exact control-host selector and verifies that selector,
so replacement replicas also remain off measured workers. Runtime revision
`k3s-v1.36.4-k3s1-tiered-runtime-v5` also fixes K3s's service NodePort range to
the smallest valid range containing the required NodePort, `8080-8081`.
Only 8080 is assigned by the rendered Service and allowed through the
provider firewall and NetworkPolicy; 8081 is not exposed. This avoids K3s's
rejection of a range whose endpoints are equal without enabling the default
broad NodePort range.
The workload attestor accepts only the Kubernetes API's known `omitempty`
round trips: empty container environment lists, the default-deny policy's
empty ingress/egress lists, and an empty PersistentVolume storage class. It
still rejects non-empty replacements and environment sources, requires the
PVC's explicit empty storage class, rejects legacy storage-class annotations,
and requires security-significant empty mappings such as the default-deny
policy's `podSelector: {}`.
K3s's pinned containerd can report a node-local OCI config digest, rather than
the registry platform-manifest reference, in the Pod status `image`
presentation field. Kubernetes separately maps the runtime's pullable
repository digest into public Pod `imageID`. The attestor therefore requires a
non-empty presentation value and binds `imageID` exactly to the locked
platform-manifest reference, allowing only the historical
`docker-pullable://` prefix. The Pod and Deployment specs remain independently
bound to that exact image-lock reference, and the Pod must still be Running
and Ready.
Readiness verifies exactly four nodes and no extras, including each node's
name, private IP, architecture, K3s version, role label, control label/taint,
and Ready condition; it separately proves CoreDNS is ready only on control.

Server and agent credentials are distinct, root-owned, and mode 0600. The
control credentials are created together from `/dev/urandom` only on a proven
pristine host. An exact owned retry is accepted; foreign markers, partial
credential state, malformed files, and any attempted token change fail closed,
so credentials are never silently rotated. Workers receive only K3s's CA-bound
secure agent token over SSH standard input. Persisted runtime state contains
only phase progress, the server CA SHA-256, and database-volume attestation—no
credential. The one SSH call whose stdout contains that token uses sensitive
output handling, which redacts partial stdout and stderr on timeout or failure.
The bootstrap stops at `cluster_ready`; it neither deploys a workload nor
produces a benchmark result. The separate workload hook described below
requires that exact boundary before it makes any workload change.

## Internal Social Network workload candidate

The upstream Social Network Helm chart was audited at the pinned
DeathStarBench commit and is not applied directly. It contains mutable image
tags, runtime Git clones, incomplete placement, no persistent database
volumes, and no NetworkPolicies. The candidate instead renders a deterministic
Kubernetes object set from checked-in, drift-checked component and dependency
assets. Its 27 components are placed exactly: 13 frontend/application
components on `application`, seven Redis/Memcached components on `cache`, six
MongoDB components on `database`, and Jaeger on `control`. For an Arm
application run, only application-role components use `linux/arm64`; the
fixed support roles continue to select `linux/amd64` images for their x86
nodes. Readiness attests each Deployment, Pod, Service, image ID, architecture,
and node placement and rejects missing or extra workload objects.

The six MongoDB instances each receive a static PersistentVolume and
PersistentVolumeClaim rooted at
`/var/lib/deathstarbench/database/mongodb/<component>`. Before applying them,
the provider-specific database guest hook re-attests the Azure LUN 0 or exact
GCP device-name XFS mount and UUID. The shared path then creates only the owned
MongoDB directories, persists their SELinux `container_file_t` labeling, and
fails closed on an unowned or mismatched path. Live readiness requires every
volume and claim to be bound to the expected host path on the database node.

The bundle applies namespace, storage, policy, service, and workload phases in
that fixed order. A default-deny baseline is paired with allow rules generated
from the audited component dependency graph, exact destination ports, scoped
CoreDNS TCP/UDP 53 access, and application tracing to Jaeger UDP/6831. External
workload ingress is limited to the one persisted load-generator IPv4 `/32`
address reaching the nginx-thrift NodePort on TCP/8080. The NodePort uses
`externalTrafficPolicy: Local` so the application node remains the only
frontend ingress target.

A separate versioned workload journal records runtime, topology, source and
rendered-manifest hashes, image-lock fingerprint, architecture, load-generator
CIDR, database-storage identity, completed apply phases, and final
attestation. It is written before work and after every monotonic phase
transition. A retry first re-attests the recorded K3s server-CA identity and
the database disk through a strictly read-only UUID, mount, and fstab check,
validates the entire stored contract, skips every durably completed phase,
and sends only an incomplete phase's exact hash-checked payload over SSH
standard input. Any changed topology,
volume, asset, phase order, image lock, or live placement fails closed. The
hook stops at `workload_ready`; it does not initialize data or emit a benchmark
result.

The manual `deathstarbench-social-images.yml` GitHub Actions workflow prepares
drift-checked build contexts, builds the three custom Social Network images
for `linux/amd64` and `linux/arm64`, resolves exact platform digests for those
and the pinned supporting images, and uploads the candidate image lock as a
workflow artifact. Workflow run 35257311111 published the three candidate
indexes, produced the exact checked-in lock, and completed successfully. The
three GHCR packages are public, and all six locked custom platform manifests
were anonymously resolvable for the Azure qualification. Publication and
public pulls are qualification evidence, not permission to open the UI release
gate.

## Internal dataset and measurement candidate

The Azure and GCP candidates expose the same provider-neutral operator-only
execution hook outside the normal run path. Provider adapters supply only the
validated inventory and SSH routes; the initialization, warm-up, wrk2 command,
parsing, evidence, result, and comparison logic is shared. Its cleanup-owned
journal advances monotonically through
`preparing_load_generator`, `load_generator_ready`,
`initialization_started`, `dataset_ready`, `warmup_started`,
`warmup_complete`, `measurement_started`, and `measurement_complete`.
Before initialization it requires an exact HTTP 200 GET response with the
empty JSON object `{}` from the user-timeline endpoint and proves that every
relevant MongoDB collection is empty. The GET probe cannot mutate the
workload.

Initialization uses the pinned Reed98 graph and deterministic seed-one
Social Network initializer. A root-owned transient systemd oneshot on the
load generator claims the run exactly once, executes the payload as the
unprivileged `benchmark` user, bounds its runtime, and atomically finalizes a
size-limited transcript. Every loaded-unit status observation verifies the
root-owned initializer script SHA-256, systemd service user, and exact
ExecStart argv; terminal observations additionally require an invocation
identity and monotonic timestamps. An ambiguous SSH response is reconciled
against that durable identity without dispatching the initializer again. Only
pre-initialization journal states are safely resumable; any interruption at
`initialization_started` or later requires cleanup and fresh infrastructure
because partially mutated databases cannot be distinguished from the
requested dataset.

After initialization, independent MongoDB queries require the exact Reed98
cardinalities. The fixed x86 load generator then builds the pinned wrk2
revision, performs a bounded warm-up, and performs one bounded measured run
over the private endpoint. Transport retries are disabled for non-idempotent
execution boundaries. The result retains workload and input identities,
warm-up and measured counters, latency and throughput, process resource
observations, raw-output and metrics hashes, and exact pre/post identities for
all 27 workload Pods. Cleanup removes the execution journal as cloud ownership
state, while the completed result and report retain the safe comparison and
qualification evidence.

## Interruption and cross-process recovery

Every normal web run, operator qualification run, persisted manual destroy,
and history deletion now uses the same nonblocking per-run POSIX lease. The
stable empty `.benchmark-run.lease` inode is paired with a canonical 0600
`.benchmark-run.lease.owner` sidecar. Each owner also acquires the historical
`.deathstarbench-azure-qualification.lock` first as a migration guard, so an
older qualifier and a current process cannot split ownership during rollout.
The normal supervisor retains its lease through final persistence and cleanup,
including any mutating worker thread that outlives asyncio cancellation.
Persisted destroy reloads state after taking the lease. History deletion
revalidates under the lease, atomically moves the visible run directory to a
hidden quarantine name, and only then removes it. Held or ambiguous ownership
fails closed; an active persisted run is classified as interrupted only when
its lease is definitively unheld.

The operator harness supports failure injection at `load_generator_ready`,
`initialization_started`, `warmup_started`, and `measurement_started`. A
checkpoint callback runs only after the named execution-journal state has been
durably persisted and before the next remote operation. A `started` checkpoint
therefore identifies a replay boundary; it does not by itself prove that an
already-running remote process was killed. Graceful injection raises the same
cooperative cancellation used by the application and attempts cleanup. Hard
injection first atomically writes `qualification-interruption.json`, then exits
the process with status 86 without cleanup. SIGINT and SIGTERM interrupt
synchronous provider waits; evidence is then recorded during normal unwinding,
including a separate immutable record of the first later signal that arrives
during recovery or cleanup.

The interruption artifact preserves its origin immutably: job, source,
checkpoint or signal, mode, durable execution state, replay decision, and
timestamp cannot be replaced. Cleanup and recovery outcomes may advance, and
the first subsequent signal has its own immutable fields. A recovery with
existing evidence cannot inject a second checkpoint. Only an absent execution
journal, `preparing_load_generator`, or `load_generator_ready` may resume.
`initialization_started` and every later state require cleanup-only followed by
fresh infrastructure because partial database mutation is not safely
distinguishable from the requested dataset. Damaged retained evidence never
blocks cloud cleanup, but it keeps the qualification exit nonzero.

Synthetic coverage exercises every checkpoint and replay decision, both
injection modes, real subprocess exit-86 and fresh-process reacquisition,
SIGINT/SIGTERM during blocked waits, historical/current lock exclusion, and
the normal web-run, destroy, and history-deletion ownership races.

## GCP operator qualification

The GCP harness requires Application Default Credentials with Compute Engine
access, an enabled Compute API, an accessible project, enough regional quota,
and the same checked-in candidate image lock used by Azure. A new run always
creates a persisted job and holds the shared per-run lease. In the normal path
the harness always attempts exact cleanup, whether qualification succeeds or
fails; a zero exit requires both the requested readiness/result boundary and a
proof that every GCP ownership key and expected cloud resource is gone.

This workload-only example provisions the complete GCP graph, reaches exact
`cluster_ready` and `workload_ready`, and then destroys it without initializing
the dataset or producing a benchmark result:

```bash
.venv/bin/python scripts/qualify_gcp_deathstarbench_distributed.py \
  --image-lock docs/qualification/deathstarbench-social-network-images-v1.json \
  --project-id <project-id> \
  --region us-east1 \
  --zone us-east1-b \
  --application-machine-type c4a-standard-8 \
  --application-vcpus 8 \
  --application-memory-gib 32
```

Add `--measure` to initialize Reed98 exactly once, warm up, measure, require the
strict distributed result and saved report, and then destroy the graph:

```bash
.venv/bin/python scripts/qualify_gcp_deathstarbench_distributed.py \
  --image-lock docs/qualification/deathstarbench-social-network-images-v1.json \
  --project-id <project-id> \
  --region us-east1 \
  --zone us-east1-b \
  --application-machine-type c4a-standard-8 \
  --application-vcpus 8 \
  --application-memory-gib 32 \
  --measure
```

Unless explicitly overridden, the shared measurement contract uses a
30-second warm-up, 60-second measured interval, four threads, four connections,
and an offered rate of 100 requests/second.

An intentionally interrupted or transport-failed run retains its job ID and
ownership contract. Resume reloads the saved plan, image-lock copy, topology,
runtime, workload, and execution journals instead of constructing a new run:

```bash
.venv/bin/python scripts/qualify_gcp_deathstarbench_distributed.py \
  --resume <job-id> \
  --measure
```

Omit `--measure` only when resuming a workload-only run that has no execution
journal. If the original run used non-default warm-up, duration, thread,
connection, or request-rate values, repeat those exact options on resume.
Measurement recovery is allowed only before dataset mutation: no execution
journal, `preparing_load_generator`, or `load_generator_ready`. Once
`initialization_started` is durable, replay is unsafe and the harness requires
cleanup-only followed by fresh infrastructure.

Cleanup-only never needs SSH credentials or a live guest. It reloads and
validates the exact cloud ownership graph and uses the Compute control plane:

```bash
.venv/bin/python scripts/qualify_gcp_deathstarbench_distributed.py \
  --cleanup-only <job-id>
```

If cloud state is ambiguous, foreign, or temporarily unavailable, cleanup
fails closed and retains the journal for another cleanup-only attempt. It does
not discard ownership merely because a delete request or lookup failed.

## GCP live qualification evidence (pending)

No GCP live qualification has been accepted yet. The implementation and
synthetic tests are not deployment or benchmark evidence.

> **Update after live qualification:** record the implementation commit,
> project-neutral region and zone, job ID, exact five role machine types and
> architectures, image-lock fingerprint, runtime/workload readiness hashes,
> database-device and filesystem attestation, and—when `--measure` is used—the
> dataset cardinalities, workload settings, normalized metrics and evidence
> hashes. Also record automatic cleanup state plus independent absence checks
> for instances, disks, firewall rules, router/NAT, subnets, and VPC. Remove
> this pending marker only after all of those claims are supported by retained
> or external evidence.

## Azure live qualification evidence

The first exact `workload_ready` qualification completed on 2026-09-18. This
is deployment evidence, not a benchmark result.

- Image publication used workflow run
  [35257311111](https://github.com/tonymarkel/oci-self-service-benchmarks/actions/runs/35257311111),
  attempt 1, from commit `4f6832eca9881acfbd4b46aa4041ad88a1e9436e`.
  The custom images used candidate tag
  `dsb-6ecb09706140-35257311111-1` before the workflow resolved their exact
  per-platform manifests.
- The checked-in lock is byte-for-byte the workflow artifact: file SHA-256
  `663e0bfaefa3d7ae19eae2430e0cc5bdcf1ccfe6c858927c2b0d4b1f5ab472e5`
  and validated lock fingerprint
  `sha256:e5435057d7813e563f6c4f40e7d877660326d83a87ce1df805f9440e161487d4`.
  Its `released` field remains `false`.
- Qualification job `25cad0331731` ran merged runtime-v5 commit
  `ecf5e03474c5b5df2541d9a21f5e2516da94923f` in Azure `eastus2`, zone 1.
  Control, cache, and load generator used `Standard_D2as_v7`; database used
  `Standard_D4as_v7` plus the contracted data disk; and the Arm application
  role used `Standard_D8ps_v6` with 8 vCPUs and 32 GiB.
- The candidate proved the exact four-node K3s membership, architectures,
  role labels, control taint, CoreDNS placement, and Ready state. It then
  applied all five workload phases and attested all 27 Deployments and Pods,
  six bound MongoDB PV/PVC pairs, exact images, placements, Services, and
  NetworkPolicies at `workload_ready`.
- The qualification intentionally stopped before dataset initialization and
  emitted no performance result. Automatic cleanup reached persisted
  `destroyed` state with no recoverable ownership keys, and an independent
  Azure resource-group lookup returned `false`.

The first complete measured qualification also passed on 2026-09-18:

- The operator qualification record associates job `73213d3a298b` with branch
  `codex/azure-dsb-dataset-load`; the reviewed slice is frozen at implementation
  commit `aab1d81649b2a3fc7d1963f9c36ca2ac806df84d`. This source association is
  external provenance because the retained result does not yet embed Git
  revision and dirty-tree identity. The live run preceded two review-driven
  fail-closed checks—live VM ownership before deallocation and exact installed
  initializer/unit identity during ambiguous reconciliation—which are included
  in that commit and covered by focused synthetic tests.
- The run used Azure `eastus2`, zone 1. Control, cache, and load generator used
  `Standard_D2as_v7`; database used `Standard_D4as_v7` plus the contracted
  disk; and the Arm application role used `Standard_D8ps_v6` with 8 vCPUs and
  32 GiB. Warm-up was 30 seconds and measurement was 60 seconds, both with
  four threads, four connections, and an offered rate of 100 requests/second.
- Workload provenance bound upstream revision
  `6ecb09706140f8730b5385c08f1386c654c3c526`, image-lock fingerprint
  `sha256:e5435057d7813e563f6c4f40e7d877660326d83a87ce1df805f9440e161487d4`,
  manifest-source hash
  `sha256:6d397ff6d633d627bb2ae115b6bb031e6db0715bdb13eb3885adce2f74151df6`,
  rendered-manifest hash
  `sha256:a2dfff62061af974a14688ffa698faa77b6b03995914293c0382d95da8615ef8`,
  and topology hash
  `sha256:b572496ed3a0e48bad17528dd5b89edaf1b2e9b682eb75988c197f00b3106583`.
- Dataset inputs bound initializer-source SHA-256
  `1b7dce9e14b82b3fb8b4ecdfe90b6fda9b7797da849840ca1a4521ef706e0482`,
  patched-initializer SHA-256
  `ff504a03311c1d6da4e5ba031b49ec824541fa78b898cda029a60e364edcadaf`,
  Reed98 node SHA-256
  `084917af148384c1e8396addcec2fca2a9f2c3918cad9676e12cdaad7dc7dfb2`,
  and edge SHA-256
  `ad6861fc9c27cfa77a865614454e5836988277a84889232acdb1fd1e0f557300`.
  The pinned input contains 18,812 undirected edges. Independent
  post-initialization database queries proved 962 users, 962 social-graph
  users, 37,624 followers, 37,624 followees, 9,424 posts, 908 user-timeline
  documents, and 9,424 timeline post references.
- The durable initializer ran as unit
  `benchmark-deathstarbench-init-73213d3a298b.service`, with invocation ID
  `eef48e065be64b288eeebb401d7dc0e0`, payload SHA-256
  `550ed626f9455d9ffc032a20876a18d528b52f94e045920e226c2e5b4db69080`,
  and a 1,000-byte finalized transcript whose SHA-256 was
  `877f8435b78df72b325f176beb3332519e31dedd64fdb6043c2421cd92d259a9`.
- The load driver bound Lua request-script SHA-256
  `ab2cd04b6cffb53beaf27efd8dfb5eae7dcd6c8abecbb70623fda93139b3dd32`,
  wrk2-binary SHA-256
  `f017e4e7a462b525c0b1fc5d23e883a6390f3ea09a9982d79e50a13f8c4bcd65`,
  and compiler-version SHA-256
  `798b37549d22cbbac6287c535cfc6dd1212f426ab26c45505cd64bdde58be27d`.
- Warm-up completed 3,000 of 3,000 requests in 30.002117 seconds at
  99.992944 requests/second, with p50/p95/p99 latencies of
  4.295/13.783/20.879 ms and zero HTTP, socket, connect, read, write, timeout,
  or incomplete-request errors. The measured interval completed and sent
  5,994 of 5,994 successful requests in 60.005865 seconds at 99.890236
  requests/second, with p50/p95/p99 latencies of 4.219/13.823/20.015 ms and
  the same all-zero error counters.
- The measured wrk2 process observed 9,344 KiB peak RSS, two logical CPUs,
  and rounded CPU/capacity values of 0.0 percent with no capacity warning.
  Zero is a valid integer-rounded observation at this offered load, not
  missing telemetry. The warm-up observed 9,088 KiB peak RSS with the same
  logical-CPU and rounded CPU/capacity values.
- All 27 pre-measurement Pod identities were unchanged after measurement,
  including UID, node, image ID, and zero restarts. The pod execution
  attestation hash was
  `sha256:0d147bcd1e81d2e3580785de12acf066001e4d62555e6bf75ea8f18e0d3445f9`;
  measurement evidence, normalized metrics, and full-output hashes were
  `sha256:7097ca018c07d84b4ee6c718ed64aedd854f2152d2038d89b6240b958a855c82`,
  `sha256:b8555bf63b60a50f2415a5918c8afd0d52580e4a8af554bf98fe7448c0c0fba0`,
  and
  `sha256:2358a12e79bfd0a8e6886ddd8cc89fd5df48f993985adfd58e49cbde8cdd1fd1`.
- Both `results.json` and `report.html` were generated, and comparison accepted
  the result as known contract fingerprint
  `ef61988d881204d765e8ffe698f2f88d880b83f19534f4ede3b7b3ad17bb3898`.
  Retained state proves automatic cleanup reached `destroyed` and removed every
  recoverable ownership key. The qualification console additionally observed
  `measurement_complete` before cleanup and an independent Azure resource-group
  lookup returned `false`; those two observations are external evidence rather
  than fields retained in the final artifacts.

The first complete safe-replay qualification passed on 2026-09-20:

- Job `f6131578cb93` ran failure-gate implementation commit
  `94ed686cec728cb95f0cad678637f805641c13f9`, including the shared-lease base
  from `b470c5b4f968ca5ce0d33279ed5df649d74e619f`. This remains operator/external
  source association because the retained result does not yet embed Git
  revision and dirty-tree identity.
- The first process used the qualifier's no-override workload contract: a
  30-second warm-up, 60-second measurement, four threads, four connections,
  and 100 requests/second. It durably reached `load_generator_ready`, wrote
  immutable hard-interruption evidence at
  `2026-09-20T21:45:48.145104+00:00`, then exited with status 86 without
  cleanup. The evidence recorded `resume_allowed`. A fresh recovery process
  acquired both the current and migration-guard locks before making its first
  recovered cloud operation.
- Recovery reused and re-attested the original resource group, five role
  identities, K3s cluster, database disk, and Social Network workload. It ran
  one Reed98 initializer, proved 962 users, 37,624 follows, and 9,424 posts,
  and reached `measurement_complete`. The retained pre/post-measurement
  attestation showed all 27 Pod identities unchanged with zero restarts.
- Warm-up completed 2,999 requests with one sent request recorded as
  uncompleted at the fixed cutoff, 99.966667 percent completion, and zero HTTP,
  socket, connect, read, write, or timeout errors. Measurement completed all
  5,994 sent requests in 60.004685 seconds at 99.8922 requests/second, with
  p50/p95/p99 latencies of 5.891/24.639/36.799 ms and the same all-zero error
  counters.
- `results.json` and `report.html` were generated. Comparison retained the
  known contract fingerprint
  `ef61988d881204d765e8ffe698f2f88d880b83f19534f4ede3b7b3ad17bb3898`;
  measurement evidence and normalized metrics hashes were
  `sha256:b36dbfa502a74c10e6ebaf62f8fa5d9c4d768b064f1bcb072d99a22b2f16b8df`
  and
  `sha256:46d8e29c2151746f2488eec945a26ff77e4f70ca9561e6ad39681c2d230c0b3f`.
- Retained evidence finalized as `recovery_outcome: resume_completed` and
  `cleanup_outcome: completed`. State finalized `destroyed` with no error or
  recoverable ownership, the lease was unheld, and an independent Azure lookup
  confirmed resource group `benchmark-f6131578cb93` was absent.

This qualification proves crash-boundary persistence, cross-process
reacquisition, safe pre-initialization replay, strict result acceptance, and
exact cleanup. It does not claim that a remote process was killed after a
`started` checkpoint. Two earlier attempts supplied additional negative-path
evidence without being accepted as successful qualifications: job
`f94788204d1c` used a stale 64-connection harness default and was correctly
rejected for wrk2 timeout events, which led to the default being aligned with
the already-qualified four-connection contract; job `3234bc8ac487` then hit an
independent SSH transport timeout during the measured wrk2 command, after
initialization and warm-up. Both attempts failed closed, released their leases,
completed cleanup, and had absent resource groups afterward.

The release and UI gates remain closed. Positive Azure deployment,
measurement, and safe pre-initialization recovery do not replace representative
forbidden-path probes, live post-initialization cleanup-only and signal paths,
publication or equivalent qualification of an immutable load-driver artifact,
or the same qualification on AWS, GCP, and OCI.

## Network and access policy

- All measured traffic uses private addresses in one selected zone or
  availability domain.
- Only the load generator may reach the application frontend port.
- Only the application role may reach cache and database workload ports.
- Backend roles do not receive public IP addresses.
- Guest management reaches backend roles through the control/application
  bastion path or immutable bootstrap data.
- K3s API access is limited to cluster members and the local orchestrator path.
- UDP/8472 (the Flannel overlay) is a private all-cluster-node underlay path
  and is never publicly reachable. Workers reach the K3s API only on TCP/6443.
  Metrics-server is disabled, so the profile does not open kubelet TCP/10250.

Cloud firewall rules enforce the private underlay boundary, but they cannot
enforce pod-tier isolation inside encapsulated overlay traffic. The checked-in
Kubernetes default-deny NetworkPolicies separately allow DNS to CoreDNS on
TCP/UDP 53, only the exact audited component-to-component edges and ports,
application tracing to Jaeger on UDP/6831, and only the persisted
load-generator `/32` address to frontend TCP/8080. Both layers are release
requirements; provider adapters must not translate the logical workload
matrix into broad subnet rules. Live qualification must positively prove the
required paths and negatively prove representative forbidden paths before
release.

The server and workers use separate root-only credentials. The server token is
never copied to workers; secret material travels only through SSH standard
input and never through argv, environment variables, events, or reports.

## Lifecycle and cleanup invariants

The lifecycle is backend-first for startup and frontend-first for shutdown:

1. Persist the topology and every planned role.
2. Create network/security resources.
3. Create control, database, cache, application, then load-generator nodes.
4. Install the exact runtime, label nodes, and verify placement.
5. Prepare persistent storage, deploy the digest-locked workload, enforce its
   exact NetworkPolicies, and attest readiness and placement.
6. Initialize a deterministic dataset, warm up, measure, and gather telemetry.
7. Stop the load generator before deleting workload resources.
8. Delete load generator, application, cache, database, then control; delete
   role disks before parent network resources.

Cancellation and cleanup must not depend on guest access. Every create step
needs response-loss reconciliation, ownership verification, idempotent delete,
and a failure-injection test. A distributed topology remains unreleased until
create, cancel, application restart, manual stop/destroy, and automatic cleanup
are proven for AWS, GCP, Azure, and OCI.

For an Azure execution journal, cleanup first reads the deterministic load
generator through the Azure control plane and verifies its exact resource ID
and run ownership tags. It then attempts a best-effort deallocation to reduce
the risk of leaving measured traffic running when SSH is unavailable. The
run-owned resource group remains the authoritative cleanup boundary: its
complete top-level inventory is re-read and verified immediately before
deletion, and an unexpected sibling still fails closed without deleting the
group.

For GCP, cleanup uses no guest dependency and has no resource-group shortcut.
It reconciles every deterministic resource in the persisted graph, recovers
immutable IDs after response loss only when exact ownership and configuration
match, and rejects an unexpected project, relationship, label, description,
shape, address, tag, disk, or service account. It then deletes in dependency
order and performs fresh absence lookups across the complete graph before
forgetting any ownership metadata. Partial deletion leaves the remaining
contract recoverable by `--cleanup-only`.

## Release sequence

1. **Implemented:** versioned model, result fingerprint, immutable runtime
   profile, and generic role-node inventory.
2. **In progress:** dual-write the existing compact lifecycle into the role
   inventory and add failure-injection coverage. Azure compact dual-write is
   implemented; the other providers remain.
3. **In progress:** exercise an unreleased five-node lifecycle on Azure first,
   using its run-owned resource group as the cleanup boundary, then qualify the
   same lifecycle on every provider. Azure synthetic coverage, positive live
   deployment and measurement, and the safe pre-initialization hard-exit,
   reacquisition, resume, and cleanup path are complete. The GCP explicit-graph
   lifecycle and operator harness are implemented with synthetic coverage, but
   GCP live qualification, AWS and OCI implementations, and additional live
   failure paths remain.
4. **Implemented for the Azure and GCP candidates:** the provider-neutral K3s
   bootstrap, OS preparation, provider-specific exact database mount, secure
   node joining, and cluster-placement attestation are covered synthetically.
   The Azure path passed live qualification; the GCP path has not yet been run
   live. Runtime v5 uses the smallest valid NodePort range, normalizes only
   audited Kubernetes API round trips, and attests the runtime's exact public
   image identities.
5. **In progress:** the checked-in, pinned Social Network manifest and
   component-policy bundle, MongoDB storage preparation, role- and
   architecture-aware image selection, phased retry journal, and exact live
   attestation are implemented with synthetic coverage. The manual GHCR image
   workflow published the candidate, the exact digest lock is checked in, and
   positive Azure qualification passed. GCP uses the same renderer and
   attestor, with live qualification pending. Representative negative live
   network probes and AWS/OCI remain. The release and UI gates stay closed.
6. **Implemented for the Azure and GCP candidates, with only the Azure core
   path live-qualified:** deterministic Reed98 initialization, durable
   at-most-once dispatch and reconciliation, exact database cardinality checks,
   bounded warm-up and measurement from the dedicated x86 load generator,
   result/report
   generation, comparison fingerprinting, and cleanup. Shared run ownership,
   all four checkpoint/replay decisions, hard and graceful injection, and
   signal unwinding have synthetic coverage. Live job `f6131578cb93` also
   passed the safe `load_generator_ready` hard-exit, cross-process resume,
   strict measurement, report, and cleanup path. The two post-run
   ownership/identity hardenings, post-initialization cleanup-only behavior,
   and signal paths still require live failure-path qualification.
   Initialization is not resumable after partial dataset mutation: an
   interrupted initialization fails closed and requires fresh infrastructure
   rather than continuing from an unknown database state. Those remaining
   representative paths remain release gates.
7. **In progress:** run and retain the GCP workload-only, measured, recovery,
   cleanup, and representative negative-path evidence; then implement and
   qualify the equivalent provider lifecycle on AWS and OCI.
8. **Planned:** per-role CPU, memory, network, disk, restart, and readiness
   telemetry.
9. **Planned:** offered-load sweeps, repeated trials, and
   sustainable-throughput reporting.
10. **Planned:** advanced per-role shape selection and later topology
   revisions.

The UI exposes only released profiles. Until GCP, AWS, and OCI qualification
and all network, interruption, and cleanup release gates pass, the existing
compact mode remains the only runnable option.
