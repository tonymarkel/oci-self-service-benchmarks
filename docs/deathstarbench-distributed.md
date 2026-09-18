# Distributed DeathStarBench design

Status: the foundation, Azure synthetic-infrastructure pilot, internal
four-node K3s bootstrap, and deterministic Social Network workload deployment
are implemented. `distributed_tiered_v1` is modeled but is not yet released
for cloud provisioning. The UI does not offer it, and the normal API/provider
path rejects it before creating a run or making a cloud write. The K3s and
workload paths have synthetic orchestration coverage but have not yet been
exercised against billable Azure resources. The image publication workflow
has not been run, and no candidate images have been published by this work.

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
exact per-platform digest for every custom and supporting image; a real lock
still must be produced and qualified before release.
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
the persisted contract makes that state recoverable. The candidate currently
has synthetic lifecycle and failure-injection coverage only. It has not yet
been qualified by provisioning resources in a live Azure subscription.

## Internal Azure K3s bootstrap

The internal runtime hook reloads and validates the persisted topology,
fingerprint, five-node role inventory, fixed Azure role identities, private
addresses, shapes, architectures, and database disk before its first SSH
process. Control is reached directly; database, cache, and application are
reached at their private addresses through a local OpenSSH proxy on the exact
control public address. The private key never leaves the orchestrator, and the
jump and destination share the run-owned known-hosts database and cancellation
boundary.

The four cluster hosts are prepared in manifest order. Rocky Linux 9 and its
exact SELinux policy are verified, firewalld is disabled inside the existing
strict Azure NSG boundary, required modules/sysctls are persisted, and the
architecture-specific K3s binary plus air-gap bundle are checksum-verified.
The database disk is resolved only through Azure LUN 0's fixed NVMe/SCSI links,
formatted only when blank, mounted by XFS UUID at
`/var/lib/deathstarbench/database`, and recorded as a non-secret attestation.

The control plane uses exact pod/service CIDRs and Flannel VXLAN. Traefik,
ServiceLB, metrics-server, and local-storage are disabled for this runtime
slice. The control node retains `deathstarbench.io/role=control` and uses the
standard `node-role.kubernetes.io/control-plane=true:NoSchedule` taint so the
CoreDNS deployment can tolerate it. Before workers join, the orchestrator
patches CoreDNS with an exact control-host selector and verifies that selector,
so replacement replicas also remain off measured workers. Runtime revision
`k3s-v1.36.4-k3s1-tiered-runtime-v3` also fixes K3s's service NodePort range to
the smallest valid range containing the required NodePort, `8080-8081`.
Only 8080 is assigned by the rendered Service and allowed through the
provider firewall and NetworkPolicy; 8081 is not exposed. This avoids K3s's
rejection of a range whose endpoints are equal without enabling the default
broad NodePort range.
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
the database guest hook re-attests the Azure LUN 0 XFS mount and UUID, creates
only the owned MongoDB directories, persists their SELinux `container_file_t`
labeling, and fails closed on an unowned or mismatched path. Live readiness
requires every volume and claim to be bound to the expected host path on the
database node.

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
workflow artifact. The workflow and lock-generation validation are
implemented, but the workflow has not been run and no images were published
or tested in Azure as part of this phase. New GHCR packages are private by
default; all three `deathstarbench-social-*` packages must be made public
before the anonymous Azure K3s pulls used by this candidate can succeed. A
workflow run and public pulls are qualification inputs, not permission to open
the UI release gate.

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

## Release sequence

1. **Implemented:** versioned model, result fingerprint, immutable runtime
   profile, and generic role-node inventory.
2. **In progress:** dual-write the existing compact lifecycle into the role
   inventory and add failure-injection coverage. Azure compact dual-write is
   implemented; the other providers remain.
3. **In progress:** exercise an unreleased synthetic five-node lifecycle on
   Azure first, using its run-owned resource group as the cleanup boundary,
   then qualify the same lifecycle on every provider. The Azure candidate and
   synthetic tests are implemented; live-cloud qualification and the other
   providers remain.
4. **In progress:** the internal Azure K3s bootstrap, OS preparation, exact
   database mount, secure node joining, and cluster placement attestation are
   implemented with synthetic coverage. Runtime v2 restricts NodePort to the
   exact frontend port. Live Azure qualification remains.
5. **In progress:** the checked-in, pinned Social Network manifest and
   component-policy bundle, MongoDB storage preparation, role- and
   architecture-aware image selection, phased retry journal, and exact live
   attestation are implemented with synthetic coverage. The manual GHCR image
   workflow is implemented, but it has not published images; public package
   access, a real digest lock, and positive/negative live Azure qualification
   remain. The release and UI gates stay closed.
6. **Next:** deterministically initialize the Social Network dataset, warm up,
   drive and measure load, and gather the first result. Initialization is not
   resumable after partial dataset mutation: an interrupted initialization
   must fail closed and use fresh infrastructure rather than continue from an
   unknown database state.
7. **Planned:** equivalent deployment and qualification on the other three
   providers.
8. **Planned:** per-role CPU, memory, network, disk, restart, and readiness
   telemetry.
9. **Planned:** offered-load sweeps, repeated trials, and
   sustainable-throughput reporting.
10. **Planned:** advanced per-role shape selection and later topology
   revisions.

The UI exposes only released profiles. Until workload measurement, live-cloud
qualification, and all cleanup gates pass, the existing compact mode remains
the only runnable option.
