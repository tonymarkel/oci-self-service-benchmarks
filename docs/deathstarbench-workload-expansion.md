# Distributed Hotel Reservation and Media Microservices

Status, 2026-10-15: **published artifacts, support-only K3s staging, guarded Hotel image access and offline initialization-control foundations; not public benchmarks**.
Social Network is still the only released distributed workload. The audited
foundations now have drift-checked application/frontend, initializer, and
load-driver preparation tools, plus an explicit native publication pipeline.
No cloud provisioning, dataset initialization, live measurement, or cleanup
qualification is claimed by the artifact pipeline.

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
The eight database claims total 80 GiB within the current 100-GiB disk.
The candidate-only guest storage adapter below now checks that full budget;
executing it against actual cloud guests is still a runtime qualification gate.
Hotel's pod policy uses port **5000**, not the external NodePort 8080. No
cloud ingress or K3s NodePort-range expansion is needed.

Both new profiles and assets remain unreleased. The existing public model,
catalog, lifecycle, and released bundle validator continue to reject them;
an asset flag or candidate preview cannot open their gates. Historical
compact API clients, reports, comparisons, and cleanup remain supported.

## Artifact preparation checkpoint

The following tools only generate new local candidate directories. They do not
change the input checkout, call a provider, publish images, initialize a dataset,
or enable a benchmark. They require the exact audited upstream revision and
reject dirty sources, ignored files in copied trees, unsafe symlinks, changed
Git blob bytes or executable modes, and patch-anchor drift. Existing output
paths and paths overlapping protected inputs are refused. On failure only the
fresh partial output owned by the tool is removed.

- `scripts/prepare_deathstarbench_hotel_images.py` prepares a native
  amd64/arm64 Go application recipe from vendored dependencies. The official
  Go 1.21.13 multi-platform builder index, platform manifests, configurations,
  and Go version were verified; the runtime uses scratch, static binaries,
  baked configuration, licenses, and an unprivileged user. Build-time tests and
  compilation are offline. Candidate corrections cover decimal seeded user
  names, Consul targets for review/attractions, cold and partial `GetMulti`
  cache misses, the MongoDB `hotelId` capacity query, and consistent nightly
  reservation cache keys. Executable Go tests use fake cache/store interfaces
  rather than claiming a live MongoDB transaction was tested.
- `scripts/prepare_deathstarbench_media_images.py` prepares the Media C++ app,
  OpenResty frontend, and a separate hashed initializer/dataset bundle. It
  retains Media's own mongo-c-driver 1.14.0 dependency recipe, bakes generated
  Lua and handlers, renders the K3s resolver/namespace, fixes the Jaeger flag,
  corrects `charactor` to `character`, and increases the request-body buffer.
  Preparation v3 omits the three unused, obsolete global-API Lua clients for
  ComposeReview, Rating, and ReviewStorage. Exact original hashes and a
  versioned exclusion receipt are retained; copied Lua references and aliases
  fail validation. Their C++ backend services remain unchanged.
  Prepared initializer scripts normalize null posters, retain the first
  mapping for duplicate movie titles, and propagate HTTP errors. The shell
  registration scripts require an explicit target; the Python uploader retains
  its upstream CLI default and still needs a validated runtime wrapper. They
  have not been executed against a deployed workload.
- `scripts/prepare_deathstarbench_candidate_load_driver.py` prepares distinct
  amd64 Hotel and Media driver contexts without changing Social Network's
  released driver. Both pin wrk2/LuaJIT/LuaSocket inputs and licenses; Hotel's
  reservation coordinates and port are corrected; Media uses a base URL without
  duplicating its compose endpoint. Per-worker seeds, original/prepared request
  hashes, metric/entrypoint hashes, and hardened runtime requirements are
  explicit. HTTP/socket errors and response-body failures are separate counters.
  The candidate callback schema is not accepted by production measurement.

`scripts/validate_deathstarbench_candidate_artifacts.py` prepares all four
artifacts independently twice and compares their manifest receipts. Run it
against a clean recursive checkout and the checksum-pinned LuaSocket source
rock (the same inputs required by the released Social driver):

```bash
.venv/bin/python scripts/validate_deathstarbench_candidate_artifacts.py \
  --upstream /absolute/path/to/pinned-deathstarbench \
  --luasocket-rock /absolute/path/to/luasocket-3.1.0-1.src.rock \
  --output /absolute/path/to/new-candidate-output
```

The required, credential-free CI job repeats this against actual upstream
sources, executes Hotel correctness tests using the pinned native Go builder
with no network and read-only inputs, and runs LuaJIT harness tests against
the actual prepared driver scripts. Uploaded receipts attest source preparation
only—not image builds, public pulls, live endpoints, or release readiness.

Deterministic context preparation is **not byte-reproducible image rebuilding**.
Media now pins its Xenial base index/platform manifests and individually
checksum-verifies dependency archives before extraction. Hidden Hunter and
`resty.string/master` downloads are removed. Xenial apt repositories and driver
RPM repositories remain mutable; these legacy dependencies are not a security
support or reproducible-rebuild claim. Hotel still needs
guarded one-shot seeding, restart/duplicate handling, atomic capacity semantics,
and a post-warmup dataset policy: reservations change persistent inventory.
The Lua harness simulates response callbacks and worker aggregation; wrk2
does not expose request identity to its response callback, so body checks are
not complete endpoint-specific or persistent-transaction proof.

## Native image publication checkpoint

The existing manual image workflow has three additional publication choices:
`hotel-images`, `media-images`, and `hotel-media-images`. They call
`.github/workflows/deathstarbench-candidate-images.yml` at the caller's exact
commit. PR and push CI do not publish images or require cloud credentials.

```bash
gh workflow run deathstarbench-social-images.yml \
  --ref YOUR_REVIEWED_BRANCH --field publication=hotel-media-images
```

The workflow uses separate native `ubuntu-24.04` and `ubuntu-24.04-arm`
workers, checks host and Docker-daemon architectures, and does not install QEMU.
Hotel and Media applications, plus Media's frontend, build for both platforms;
the two workload-specific drivers and backing roles remain x86-only. Unique
run/attempt tags use separate `deathstarbench-hotel-*` and
`deathstarbench-media-*` packages, without changing Social Network artifacts.
SBOM and BuildKit provenance accompany original pushed indexes; operator/CI
receipts are not cryptographically signed native-build proof.

Image checks pull exact platform digests and inspect stopped-container exports
without extracting untrusted paths onto the host. Every ELF is checked for the
expected architecture, and baked configuration, scripts, and licenses are
hashed against prepared inputs. Hotel's ten scratch binaries must be static;
they are not started because upstream startup mutates MongoDB. Media's thirteen
installed C++ binaries (including the undeployed `PageService`) and frontend
libraries receive native linkage checks. A versioned linkage scope records the
sorted workload/custom-library targets; their recursive loader dependencies
must resolve. Unrelated OS tools are not executed or promoted to workload
requirements, but still receive the same all-ELF architecture and unexpected-key
screening. The frontend also verifies excluded clients are absent from the
image, including dangling or cyclic leaf links and aliased parent directories.
It parses its actual baked nginx configuration and loads Lua/Thrift modules,
the binary protocol, and all three native Thrift helpers in OpenResty's Lua
host with explicit mock DNS, not a
substitute configuration or live dependencies. Executed checks use user 65532,
a read-only filesystem, no network, no capabilities, and no-new-privileges.
Driver checks bind binary/request/metric/entrypoint identities to their offline
attestation. None of these checks is a transaction or dataset-readiness test.

`scripts/publish_deathstarbench_candidate_images.py` independently regenerates
preparation on the assembly worker, validates both original native receipts
before index mutation, and retains original native indexes when producing a
dual-platform index. `scripts/create_deathstarbench_candidate_image_lock.py`
verifies raw index, runtime-manifest, and configuration bytes against registry
SHA-256 descriptors. The resulting `candidate-artifact-lock-v1` records exact
source/context/recipe, patch, binary, initializer, license, and x86 backing-image
identities. It is deliberately **not** the deployable public runtime-lock schema.

New GHCR packages may be private. Publishing with `GITHUB_TOKEN`, registry
manifest inspection, and authenticated pulls do not prove anonymous layer
access. Package owners must make candidates public before isolated empty-auth
layer-pull and smoke qualification. This gate remains explicitly false until
independently completed. Every generated candidate lock also keeps `released`,
runtime, measurement, dataset, denied-edge, and cleanup qualification false.
Failure artifacts retain available preparation/build identities without
inventing successful smoke results.

### Retained native publication evidence

Both workload pipelines completed successfully on 2026-10-13:

- Hotel Reservation: [run 38414696606](https://github.com/tonymarkel/oci-self-service-benchmarks/actions/runs/38414696606),
  source commit `1dbc03ca4b104a2fba4fc6a4c3c0bb7e22690ca7`.
- Media Microservices: [run 38416925449](https://github.com/tonymarkel/oci-self-service-benchmarks/actions/runs/38416925449),
  source commit `86f0869e4bbd940b52c3eaa3b4ac360be8b7e96a`.

The [retained summary](qualification/deathstarbench-hotel-media-candidate-images-v1.json)
records exact app/frontend/driver and backing-image references, platform/config
digests, native smoke observations, preparation identities, workflow source
commits, and downloaded lock/publication-file hashes. Complete receipts remain
in each workflow's `candidate-lock-*` artifact for its 90-day retention window;
the summary is not a substitute runtime lock. Archive digests reported by
GitHub are distinguished from hashes computed for the extracted JSON files.

Every public/runtime/measurement/dataset/network/cleanup gate remains closed.
The application and UI still offer Social Network only. Candidate artifact
adoption, per-run snapshots, and profile-specific storage preparation are now
implemented below. Workload readiness, initialization, measurement, recovery,
and live qualification must follow before exposing either new UI choice.
Media's TMDB dataset also needs documented rights clearance or a reviewed,
versioned synthetic replacement; artifact hashes do not establish data rights.

## Candidate runtime preflight checkpoint

`app/deathstarbench_candidate_artifacts.py` accepts the **complete** downloaded
candidate lock and publication, plus regenerated workload and driver preparation
trees. It reruns the publication validator against those trees and freshly
retrieved raw registry index, platform-manifest, and configuration bytes. The
entire regenerated lock must match, including native receipts, licenses,
prepared contexts, backing images, and driver attestation. A retained summary,
serialized adoption metadata, or a syntactically valid digest is insufficient.
This is read-only registry inspection, not layer-pull or deployed-service proof.

The adoption result is immutable and architecture-neutral. Runtime image schema
v2 records both application platforms, but **only amd64 backing-role images**;
it does not fabricate Arm MongoDB, Consul, cache, or tracing manifests. The
renderer still selects application architecture explicitly. Legacy schema-v1
offline previews keep their previous validation/fingerprints. Social Network's
released image validator, renderer, guest commands, providers, and public
dispatch are unchanged.

`app/deathstarbench_candidate_runtime.py` binds the adoption to an exact run ID,
provider, supplied topology fingerprint, application architecture, private load
generator `/32`, filesystem UUID, namespace, profile revisions, and rendered
bundle hashes. Snapshots are portable copies, not reconstructed execution
authority: restoring one requires fresh artifact adoption and an exact match.
Both bindings and plans check their internal relationships, including copies
made with `dataclasses.replace`. The provider/topology values are **identity
bindings only**; this offline layer does not attest existing cloud resources.
No candidate is dispatched by the public API or provider qualification wrappers.

The read-only operator preflight emits a snapshot on stdout:

```bash
.venv/bin/python scripts/preflight_deathstarbench_candidate_runtime.py \
  --workload hotel_reservation \
  --image-lock /absolute/path/to/candidate-image-lock.json \
  --publication /absolute/path/to/publication.json \
  --workload-preparation /absolute/path/to/prepared/first/hotel \
  --driver-preparation /absolute/path/to/prepared/first/hotel-driver \
  --run-id "$CANDIDATE_RUN_ID" --provider azure \
  --topology-fingerprint "$VALIDATED_TOPOLOGY_FINGERPRINT" \
  --application-architecture aarch64 \
  --load-generator-ip "$VALIDATED_LOAD_GENERATOR_IP" \
  --database-filesystem-uuid "$VALIDATED_DATABASE_FILESYSTEM_UUID"
```

Use `media_microservices`, `first/media`, and `first/media-driver` for Media.
The preparation-validation tool above generates these source trees; locks and
publications come from their reviewed native workflow artifacts. The command
does not create files, deploy infrastructure, run SSH/Docker, or initialize data.
`--verify-snapshot /absolute/path/to/snapshot.json` additionally checks an earlier
snapshot against fresh preflight. It refuses links/nonregular files, swapped
file identities, duplicate JSON keys, non-UTF-8/nonfinite JSON, oversized or
excessively nested inputs, crossed run identities, or any promoted gate.
Use a trusted CA bundle if the local Python certificate store cannot verify a
registry redirect; never disable TLS verification. Explicit GHCR authentication
is optional and, if used, cannot qualify anonymous access.

The existing manual workflow also has a read-only `candidate-runtime-preflight`
operation. It downloads the two exact retained publication artifacts, verifies
their extracted file hashes, independently regenerates preparation twice, and
performs full fresh registry adoption with a read-only `GITHUB_TOKEN`. It renders
eight provider/architecture snapshots per workload using **explicitly synthetic
offline** run/topology/disk inputs. The resulting artifact includes `scope.json`;
these are not cloud runs or anonymous-access/guest-storage qualification. No
registry writes, image builds, cloud credentials, dataset execution, or provider
commands occur. Required PR CI remains credential-free and does not dispatch it.

```bash
gh workflow run deathstarbench-social-images.yml \
  --ref YOUR_REVIEWED_BRANCH --field publication=candidate-runtime-preflight
```

Both workloads passed this full read-only adoption on 2026-10-13 in
[run 38426311217](https://github.com/tonymarkel/oci-self-service-benchmarks/actions/runs/38426311217),
source commit `c09b878dd942b8f5353c5cad12357ba89961fe45`. The
[retained validation summary](qualification/deathstarbench-candidate-runtime-preflight-v1.json)
binds downloaded adoption/scope/snapshot file hashes, artifact IDs and archive
hashes, exact lock/preparation/driver identities, and all closed-gate scope
statements. It is not an authority to deploy or restore from summary metadata.
The local macOS registry attempt failed closed at a Docker blob redirect's
certificate-chain verification, including a trusted CA-bundle retry; no TLS
bypass was used. The clean Linux runner completed registry revalidation.

`app/deathstarbench_candidate_storage.py` derives eight exact database paths
and 80 GiB of claims from the hash-audited profile assets. Its **predeployment**
guest command requires an already mounted, canonical, root-owned XFS filesystem
with the expected UUID and the full claim budget available. It performs no
format, mount, or data deletion. An atomic, root-only ownership marker binds
run/workload/namespace/disk/inventory; a Social marker, foreign marker, unowned
filesystem entries, unknown database paths, nested mounts, unsafe links/special
files, insufficient capacity, or incorrect SELinux labels fails closed. Exact
same-owner partial directory preparation can retry while preserving regular
data. Concurrent claims cannot overwrite another owner's marker. The 10-GiB
hostPath PVC requests are not per-directory filesystem quotas or reservations.

The storage journal records only `preflight_only` or an exact, parsed
`storage_prepared` receipt. A restored receipt is historical predeployment
evidence, not current mount/free-space/readiness proof. Re-recording storage is
refused by this storage-only API; the separate read-only boundary below is used
for support-stage retry. Preparation must not be replayed as a live
populated-database recovery command. The preflight slice does
not apply pods, seed either dataset, run a benchmark, claim denied-edge or
cleanup success, or enable UI selection. Portable shell tests use real temporary
files with explicit privileged-tool stubs, not live cloud/storage attestation.

## Support-only K3s staging checkpoint

`app/deathstarbench_candidate_stage.py` derives a separate run-bound stage,
without changing the final offline bundle or Social Network's released path.
Its six phases are namespace, storage, policies, services, support workloads,
and blocked applications. All fourteen Hotel or nineteen Media support
Deployments request one replica. Every application/frontend requests **zero**:
ten for Hotel, thirteen for Media. No initializer, driver, Job, autoscaler, or
application-start permission is introduced. This is necessary because eight
Hotel programs seed MongoDB on startup; Media initialization/data rights also
remain unqualified. Zero replicas are the desired/current observed state, not a
permanent admission policy against an external operator.

All object/pod-template metadata binds run, runtime snapshot, stage revision,
non-circular stage identity, and denied application startup. Exact payload hashes
are separate. Crossed runs/workloads/architectures, changed replicas, altered
payloads, or substitution of the final application-starting bundle fail closed.

`app/deathstarbench_candidate_k3s.py` generates apply, read-only reconciliation,
and support-readiness commands. Apply hashes stdin, acquires a root-only
namespace lock, verifies owned namespace/related-PV inventory, and performs
strict server-side dry-run before apply. Namespace creation is atomic; conflicts
are not ownership proof. All observed object UIDs must remain unchanged across
dry-run/apply; normal controller/Pod replacement also requires a new explicit
qualification attempt rather than transparent adoption.
Unknown/foreign objects, orphaned PVs, application Pods, nonzero application
controllers, or incompatible specs are refused, never adopted/deleted/scaled
down. There is no prune, forced field conflict, or repair. Multi-object apply is
not a transaction; its caller must journal and reconcile partial completion.

Attestation checks exact role nodes/architectures, static inventory, PVC/PV UID
binding/paths, frontend ports (including Hotel pod 5000), network policies,
support Pod/ReplicaSet/Deployment ownership, locked image IDs, readiness and
container/restart identities, with zero observed application Pods. Only support
services are waited on; missing support Pods cannot become successful readiness.
Narrow API-default equivalences cover omitted empty PV class/container env/deny
arrays and controller fields. ReplicaSet metadata follows Deployment annotations
while its template follows pod-template annotations, matching the
[pinned Kubernetes controller](https://github.com/kubernetes/kubernetes/blob/v1.36.4/pkg/controller/deployment/util/deployment_util.go).
TCP readiness is not a semantic transaction test. Current paused observations
do not prove that another actor never started applications or seeded data.

`app/deathstarbench_candidate_deployment.py` is an **internal leased facade**,
not public dispatch or a live provisioning wrapper. It requires durable
persistence, the exact run lease, and strict provider-persisted K3s inventory in
`cluster_ready`. It binds run separately from the run-independent topology hash,
plus inventory hash, SSH routes, role nodes, server CA and provider disk identity.
An additive acquired-lease validator verifies the original factory capability,
retained descriptors/inodes, canonical paths and owner metadata, plus both held
current/legacy locks. Fabricated/copied, released, unlocked or changed leases
cannot authorize staging; observational lease status is not permission.
Those bindings and the exact stage journal are checked before/after every
remote/persist callback. Erasing or substituting persisted intent prevents
subsequent dispatch. Live K3s nodes/system services, CA and disk are re-attested
before mutation; existing
Social execution/results cannot be adopted. The caller still owns independent
fresh cloud ownership checks, provisioning, signals and exact cleanup.

A namespace guard precedes initial storage mutation. Preparation is permitted
only before the stage namespace exists. Once the initial full-budget receipt is
journaled, retries use `app/deathstarbench_candidate_storage_observation.py`:
read-only checks of marker, UUID/XFS, exact inventory, unsafe trees, SELinux and
capacity/free bytes. Observed free space may now be below 80 GiB (even zero),
because MongoDB can consume the initially attested budget; total capacity must
still cover the claims. No directories, labels, modes or data are repaired.

The journal persists ordered intent before dispatch, pairs pending phases with
before-apply UID receipts, and reconciles after transport completion/loss.
Complete owned state advances without replay; partial owned state retries only
with unchanged observed UIDs. Completed phases and namespace continuity are
rechecked before further mutation, and every completed static UID must survive
the final readiness observation. Saved `support_ready_applications_paused` is
historical and revoked before the first retry SSH, including cancellation or
transport failure. All public/runtime/dataset/measurement/network/cleanup gates
remain false; this is support readiness only.

Tests execute generated remote Python against a **fake** Kubernetes API, portable
filesystem harnesses, simulated SSH/persist failures, and full-parser journal
integration. They are not live API/cloud qualification. The manual read-only
artifact workflow additionally derives staged manifests and syntax-checks
commands against actual image locks with synthetic provider/architecture inputs;
its scope explicitly says Kubernetes/guest/cloud operations were not executed.
Hotel and Media remain unavailable in the UI.

## Private support-stage operator

`scripts/qualify_deathstarbench_candidate_support.py` joins the reviewed stage
to the existing five-role provider lifecycle without using public dispatch or
the Social Network deployment/measurement wrappers. It creates **five billable
VMs** per new invocation. The separate Hotel/Media pin identifies the candidate;
the sanitized `BenchmarkPlan` is only an infrastructure carrier for the existing
`distributed_tiered_v1` / `k3s_v1` role, capacity, network and storage contract.
Its `deathstarbench.workload` remains `social_network` solely because that
existing infrastructure contract accepts only that value. No Social Network
application is deployed and no Social result/report is fabricated.

Example (paths identify actual full reviewed artifacts/preparations, not the
checked-in qualification summaries):

```sh
python scripts/qualify_deathstarbench_candidate_support.py \
  --plan /secure/azure-five-role-plan.json \
  --workload hotel_reservation \
  --image-lock /secure/hotel/candidate-image-lock.json \
  --publication /secure/hotel/publication.json \
  --workload-preparation /secure/prepared/hotel \
  --driver-preparation /secure/prepared/hotel-driver
```

The plan must select only distributed DeathStarBench, have
`destroy_after_completion: true`, disable the general additional storage volume,
and contain no SSH credentials/key material. SSH defaults come from the existing
environment configuration or `--ssh-env-file`. AWS additionally requires the
existing exact Rocky AMI `--aws-image-pin`; OCI requires its pinned support and
application image OCIDs, architecture, and profile selectors. OCI defined tags
come from the exact carrier plan; an optional tag file must agree with it.
No Marketplace agreement/subscription is accepted by this operator.

State lives under `app/runs/.candidate-support/<run-id>/`, outside public run
history. Pins and state are private 0600 JSON in 0700 directories, written with
atomic replace and file/directory fsync while the original acquired run lease is
held. Exact original lock/publication bytes are retained. Private SSH keys and
passphrases stay process-local; public authorized keys can legitimately occur
inside provider ownership contracts. Private SSH known-host state uses the same
lease-bound directory, not a public run-shaped directory. Validation failures
do not print injected private key/passphrase inputs.

New runs provision once, take a fresh SDK ownership/readiness audit, prepare the
pinned K3s runtime, repeat the cloud audit, verify support-image access, and
execute only the support stage. The cloud identity pins account/project,
topology, addresses, profiles, image/disk/network identities and immutable
creation IDs. Azure audits include opaque VM/disk IDs as well as reusable ARM
paths. Azure's original provider did not retain those opaque IDs during create;
their first successful complete audit establishes the additional pins. Before
that audit, partial-provision cleanup retains the existing exact ARM-ID/tag/
graph contract. Azure cleanup checks any established opaque IDs before delete,
including stopped VMs; observed same-name replacements are refused.

Support-image access is deliberately narrower than the application release
gate. It anonymously streams each exact active amd64 backing manifest, config
and **all compressed layer bodies**, checking descriptor sizes and SHA-256.
There are four distinct backing images per candidate. The client uses verified
TLS, fixed Docker Hub repository-scoped anonymous token exchange, bounded
streaming and reviewed HTTPS CDN destinations without forwarding auth. It does
not use Docker config, environment registry/proxy credentials, or local image
caches; it stores no layer files and executes no image. Each invocation/resume
must obtain a fresh process-local factory capability. Saved JSON is historical
evidence, never an access authority. Application/frontend/driver pulls, native
binary/version smoke, and global `anonymous_layer_pull_qualified` remain outside
this proof; the existing global release gates stay false.

After taking cleanup ownership, normal returns/failures and cooperative signals
attempt exact destruction in `finally`. Repeated SIGINT/SIGTERM are deferred
through cleanup. The exact known support journal is durably archived before
removing that one non-cloud resource key. The original cloud cleanup targets
are also persisted **before** deletion, so a crash after a provider clears its
graph cannot erase the independent absence targets. Unknown provider-prefixed
keys are refused using finite writer schemas; unknown non-provider recovery
data is not discarded to make a terminal predicate pass. Azure/GCP require
fresh independent SDK absence reads after provider terminal proof; AWS/OCI
re-read their retained tombstones. Inaccessible resources are not absence.

Recovery commands use the printed private run ID:

```sh
# Fresh SDK audit and artifact adoption; never re-create missing roles.
python scripts/qualify_deathstarbench_candidate_support.py \
  --resume <run-id> \
  --workload-preparation /secure/prepared/hotel \
  --driver-preparation /secure/prepared/hotel-driver

# No registry, prepared trees, private SSH key, or guest command is needed.
python scripts/qualify_deathstarbench_candidate_support.py \
  --cleanup-only <run-id>
```

Resume is refused without cleanup if its initial read-only ownership audit
fails or its immutable create identities changed; use explicit cleanup-only to
request destruction of the still-verifiable owned graph. Resume never calls
provisioning. Once teardown begins only cleanup-only is permitted. Provider
selector/image/shape overrides are not accepted during recovery. The original
SDK authentication and exact ownership evidence are still required for cleanup.

An older AWS partial launch can retain a no-ID ambiguous intent when EC2
explicitly rejected its root EBS allocation with
[`InsufficientVolumeCapacity`](https://docs.aws.amazon.com/ec2/latest/devguide/errors-overview.html).
New launches recognize that exact fresh service rejection. Existing ambiguous
records are **not** resolved from a saved error message or a missing tag lookup.
For this one case an operator can explicitly supply the authoritative event:

```sh
python scripts/qualify_deathstarbench_candidate_support.py \
  --cleanup-only <run-id> \
  --reconcile-aws-capacity-rejection <cloudtrail-event-uuid>
```

This optional AWS-only mode additionally requires `cloudtrail:LookupEvents`.
It takes a fresh, bounded CloudTrail lookup and requires exactly one matching
immutable RunInstances request, account/region/time, the exact capacity error,
and a null response. Two fresh instance and boot-volume absence passes must
also succeed. Full private evidence is persisted before the sole no-ID intent
becomes a deletion tombstone; it never becomes provisionable or replays a
create. The original cleanup-source snapshot is unchanged, and ordinary
whole-graph destruction plus independent terminal proof must still pass.
Uncertain, replaced, duplicate, late, or inaccessible evidence fails closed.
This is not a general override for ambiguous launches.

SIGKILL, power loss, or the explicit qualification-only
`--interrupt-after cluster-ready|support-ready --interrupt-mode hard` cannot run
`finally`; these deliberately retain billable resources and require recovery.
Graceful injected interruptions use the same checkpoints and cleanup path.
Exit 0 means the requested support-only operation and independent cleanup proof
succeeded (or cleanup-only succeeded). Exit 1 is a support-stage failure with
cleanup proved complete; exit 2 means refusal/incomplete cleanup. Cooperative
signals return nonzero signal status after cleanup; cleanup failure takes
precedence. No exit code authorizes application startup or public release.

Unit tests use synthetic SDK/SSH/registry transports and are not live cloud
qualification. Any live evidence is retained separately with its exact scope;
neither a support-ready Pod nor a TCP probe is a semantic application test.

### Support-only live observations, 15 October 2026

The private operator completed the following bounded live checks. Each row is
one candidate/provider observation, not qualification of both workloads or
both application architectures on every provider. Support roles use amd64;
the application column describes the prepared node, not application execution.

| Provider | Candidate | Application node | Ready support Pods | Application Pods | Independent cleanup |
| --- | --- | --- | --- | --- | --- |
| Azure | Hotel | Arm64 | 14 | 0 | Passed |
| AWS | Hotel | x86-64 | 14 | 0 | Passed |
| GCP | Media | Arm64 | 19 | 0 | Passed |
| OCI | Media | x86-64 | 19 | 0 | Passed |

All six guarded phases completed in each successful run. Hotel's ten and
Media's thirteen application deployments remained at zero replicas. No
application, initializer, load driver, or benchmark measurement was dispatched.
Fresh cloud ownership audits ran before SSH and after K3s preparation; fresh
support-image byte access preceded staging. Every attempt, including the four
earlier failures, finished with independently proved destruction.

The failed attempts remain in the evidence rather than being counted as
support successes. A hard GCP cluster-ready interruption recovered without
reprovisioning, then exposed a Python 3.9 `zip` compatibility issue. Source edits
overlapping other early processes exposed dynamic source-extraction drift;
embedded helpers are now frozen when imported. Fresh processes validated that
repair. OCI's earlier process overlapped source evolution, so its summary does
not claim execution of one exact final commit. AWS's rejected EBS allocation
was reconciled only through the explicit CloudTrail/absence boundary above; a
new run in another zone then passed support staging and cleanup.

The [retained observation summary](qualification/deathstarbench-candidate-support-operator-v1.json)
contains counts and hashes, not cloud account/resource IDs, IP addresses,
hostnames, keys, or raw CloudTrail events. It records source-cohort limitations
and cannot reconstruct fresh artifact, cloud, or cleanup authority. Exact
private records remain outside web history.

Local `scripts/check.sh` passed 1,681 tests (one existing macOS GNU-sed/time
skip). GitHub's Python 3.10/3.14 CI and the
[read-only artifact/support-byte workflow](https://github.com/tonymarkel/oci-self-service-benchmarks/actions/runs/38749728738)
also passed on the tested code commit. All global application/runtime, dataset,
measurement, network-denial, full-anonymous-image, and public release gates
remain closed. Hotel and Media are still hidden in the UI; support readiness
does not enable their public benchmark dispatch.

## Additive Hotel initialization source checkpoint

The retained Hotel v1 image is not safe to start yet: all eight database-backed
commands call `InsertMany` at startup, before normal service initialization.
Restarting a Pod can therefore duplicate or partly seed its store. A Kubernetes
`restartPolicy: Never` or a per-process lock does not solve response loss or
cross-database ownership. The published v1 recipe, its preparation API, image
lock adoption and support-only operator remain unchanged.

`scripts/prepare_deathstarbench_hotel_initialization.py` adds a **separate
recipe, not accepted by the original v1 publication/adoption or runtime APIs**:
`hotel-reservation-guarded-init-native-go-scratch-candidate-v1`. It first
validates the complete original preparation, retains that input receipt, then
extracts the eight exact typed Go seed constructors into a pure catalog. Nested
types and decimal user names are preserved; six audited coordinate expressions
gain explicit typed rounding steps. Python does not evaluate or approximate the
row data. The generated source fingerprint binds the extraction revision,
eight full source hashes, all eleven collection/row mappings, and the complete
arithmetic policy and its exact original/replacement anchors.

The new Go library constructs exactly **873 baseline documents in eleven
collections across eight stores**, rejects duplicate/foreign logical keys and
incorrect BSON types, assigns deterministic logical-key `_id` values, and
computes canonical typed content and logical-key hashes. Its complete metadata
fingerprint includes the source binding. These are actual catalog-construction
hashes, not Social Network dataset identifiers.

The initial golden check caught a genuine Arm/x86 difference: Go permits
fused multiply-add to skip an intermediate rounding step. The same Go compiler
emitted FMA on Arm and separate division/multiplication/addition on x86; geo
and recommendation records consequently had different content hashes despite
identical logical keys. The generator's versioned rounding policy now forces
each operation's original float32/float64 precision, as specified by the
[Go floating-point rules](https://go.dev/ref/spec#Floating_point_operators).
The dataset uses the distinct
`hotel-reservation-deterministic-rounded-baseline-candidate-v1` revision.
Canonicalization and MongoDB comparisons do not round away stored-data drift.

The distinct `hotel-initialize` command supports only explicit
`initialize-empty` and read-only `verify` modes. Identity, dataset fingerprint,
the exact baked configuration and all eight fixed service endpoints are
mandatory. The source-level initialization protocol:

1. Observes every store and refuses any preexisting collection or marker.
2. Creates an atomic ownership claim in each of the eight stores before
   inserting any application data. Claim conflicts, cancellation, partial writes and
   lost responses fail closed; initialization is never automatically replayed,
   including after an apparently completed attempt.
3. Verifies the complete typed baseline across all stores before completing
   any marker, then verifies the completed baseline again. No reset, delete,
   repair or upsert path exists.

Each normal database-backed service now performs **read-only verification of
its own store** before returning the client to the original service. It never
seeds data or expands its service-to-database network permissions. All ten
application commands and the initializer expose the sole
`--candidate-initialization-attestation` option before configuration, tuning,
identity or database access; it emits offline catalog metadata only.

This is a source/compiled-protocol checkpoint, **not MongoDB, Pod restart or
application-runtime qualification**. Tests use an in-memory store model; even
a fully completed set of per-store markers is not an atomic eight-database
transaction. The new recipe is deliberately not accepted by the original image
lock factory or support controller, and no web dispatch path calls it. Its
Python metadata validator grants no runtime authority, and self-reported JSON
alone cannot prove that a particular binary ran.

The [local offline observation](qualification/deathstarbench-hotel-initialization-offline-v1.json)
retains the actual native Go 1.24.2/darwin-arm64 catalog metadata and context
receipts: all sixteen initialization test functions passed, all eleven commands
built and attested identical metadata, and all eleven refused unbound startup.
The observed source/dataset fingerprints are pinned in a Go golden regression.
This record does not claim Linux Go 1.21.13 CI results, MongoDB execution,
image publication or cluster qualification.

Required CI prepares the new recipe twice from the audited checkout, tests and
builds all eleven commands using the pinned native Go 1.21.13 builder with no
network, then executes their metadata-only option outside the configuration
directory with no run identity. It checks that ordinary unbound startup fails,
and that every command reports the same source-bound dataset fingerprint. Only
bounded preparation/native-metadata receipts are uploaded; no database or
initializer write is executed in CI. Example source-only preparation:

```bash
.venv/bin/python scripts/validate_deathstarbench_hotel_initialization.py prepare \
  --upstream /absolute/path/to/pinned-deathstarbench \
  --output /absolute/path/to/new-hotel-initialization-output
```

The separate image checkpoint below explicitly selects, publishes, smoke-tests
and adopts the **new** recipe rather than rewriting the v1 lock. Subsequent execution
needs a run/cluster/namespace/storage/Pod-bound controller, durable response-loss
reconciliation and global prestart verification. Consul registration/restart
health, ignored preload errors, atomic reservation capacity/authentication,
semantic transactions and a separate post-warmup dataset epoch remain open.
Reservations mutate the baseline: this verifier intentionally rejects those
changes and must not be advertised as general healthy restart recovery.
Media's dataset rights and runtime gates remain separate and unresolved.

## Separate guarded Hotel image checkpoint

The existing manual publication wrapper has a new explicit
`hotel-initialization-images` choice; its `load-driver` default and all original
routes are unchanged. The reusable guarded workflow builds only the new Hotel
app/initializer image, natively on `ubuntu-24.04` and `ubuntu-24.04-arm`, without
QEMU. Unique `guard-init-6ecb097-<run>-<attempt>` tags use the existing public
Hotel app repository; old tags, digests and v1 locks are not rewritten. Media
and Social Network are not rebuilt.

Before registry observation, the source boundary independently reconstructs
the complete guarded preparation from the audited Git checkout. It compares
the full tree, modes, licenses, retained v1 receipt and manifest while holding
no-follow file descriptors and detecting input changes. Historical receipt
hashes alone are not a substitute for this reconstruction. The exact reviewed
Hotel v1 lock/publication from run `38414696606` are freshly adopted through the
original factory; its fixed x86 support and load-driver images are reused, not
rebuilt or silently replaced.

Each native runner pulls the exact app platform manifest, validates its raw
registry config and local config identity, and inspects a stopped container's
filesystem without extracting it onto the host. All eleven binaries must be
static native ELF64 executables, with exact baked config/license inventories
and no embedded private keys or mutable runtime clone commands. Hardened
containers then execute only the pure metadata option and ordinary unbound
startup refusal: no network, read-only filesystem, non-root user, no added
capabilities, bounded memory/processes/output/deadlines and owned-container
cleanup. The upstream TLS-disabled informational event is accepted narrowly;
other unexpected logs fail the check. Every command on both architectures
must attest identical complete source-bound dataset metadata.

Assembly validates both original native receipts and fresh registry bytes
before publishing a multi-platform index. The new
`GuardedHotelArtifactBinding` is a separate immutable artifact-only type: it
cannot be converted to the existing candidate runtime plan and has no runtime
manifest API. Adoption rechecks source reconstruction, native receipts, exact
index membership/config bytes and the original baseline. Serialized evidence
and CI smoke are observations, not signed binary-build provenance or permission
to initialize a database.

Separate fresh native jobs repeat the exact app pull and offline smoke with
an empty Docker client configuration and no supplied registry credentials.
This is **Docker-client isolation only**, not independent full-layer download,
proxy independence or proof of absent daemon/host credentials. The general
`anonymous_pull_qualified` gate remains false; support/driver layers are not
reexecuted by these jobs. No cloud resources, MongoDB processes, K3s workloads,
dataset writes, application services or benchmark traffic are started by this
pipeline. All initialization, runtime, measurement and public-release gates
remain closed. It does not change support-stage behavior or the public form.

The [retained image observation](qualification/deathstarbench-hotel-guarded-images-v1.json)
records successful [native workflow run 38767705438](https://github.com/tonymarkel/oci-self-service-benchmarks/actions/runs/38767705438):
both native builds, index assembly and both fresh empty-client native jobs
passed. Every smoke inspected eleven static native executables, observed eleven
pure attestations and eleven unbound startup refusals. Complete dataset metadata
matched between x86 and Arm and the reviewed source golden: eight stores,
eleven collections and 873 documents. Both fresh native artifact adoptions
produced the same fingerprint. The record binds exact index/platform/config
digests and downloaded receipt hashes without treating them as future authority.
The first publication attempt correctly stopped before assembly on a strict
BuildKit-default config mismatch; its failed candidate tags/evidence remain
separate. Two additional local adoption attempts later failed during registry
reads of the original baseline and were not counted as successful guarded
adoption. The successful native jobs did not bypass those registry checks.

## Guarded Hotel image access and initialization-control foundations

The explicit manual `hotel-runtime-preflight` choice adds a **read-only image
qualification**, not a runtime operator. It selects the exact reviewed original
and guarded lock/publication files, regenerates source preparations and checks
complete source reconstruction before registry observations. All original
publication choices and their default remain unchanged.

`app/deathstarbench_hotel_image_access.py` then reads every exact index,
platform manifest, config and compressed layer body for **seven image
platforms**: both guarded application architectures, four distinct fixed-x86
support images and the fixed-x86 driver. It checks descriptor SHA-256s and
sizes, config architecture and rootfs inventory, bounded layer/aggregate bytes
and deadlines. Shared layers are downloaded again rather than accepted from a
cache. There is no Docker, file cache, netrc, cookie store, inherited proxy
handler or registry credential input. TLS verification and repository-scoped
anonymous challenges are mandatory; tokens are never forwarded to redirects.
Only reviewed exact Docker Hub and GHCR/CDN HTTPS hosts are permitted, not
wildcard hosts or manifest-supplied download URLs. The
[GitHub container registry documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
and [hosted-runner endpoint list](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)
document public anonymous access and the two allowed GitHub registry/CDN hosts.

The resulting immutable access object belongs to this process/PID and the
original guarded artifact proof. Copies, pickle/JSON, changed fields and
crossed artifacts cannot restore authority. Each future attempt must obtain a
fresh object. Its narrow `anonymous_all_bound_image_bytes_verified` observation
does **not** qualify a CRI pull, native execution, live initialization, dataset,
benchmark or public release; those gates remain false. The old support-only
access capability and support operator are unchanged.

`app/deathstarbench_hotel_runtime.py` separately binds the guarded artifact to
the **exact original paused support plan and stage**. It requires the same
baseline artifact fingerprint and every reused image, and derives the run,
provider, topology, application architecture and database storage identity
from that plan rather than accepting replacements. It retains the reviewed
recipe/source/dataset identity and an explicit nonzero attempt UUID. Initializer
and verifier names derive from that attempt. Rich cluster observations retain
explicit CA, kube-system Namespace UID (there is no Kubernetes Cluster object
UID), workload Namespace UID, database Node UID, eight PVC/PV/database-Pod
identities and initializer Pod UID. These are **caller-supplied, unverified
observations**, not a live API check or permission. Historical observation
validation returns only a detached dictionary with freshness/execution false.

`app/deathstarbench_hotel_initialization_stage.py` derives two offline
standalone Pods, `initialize-empty` and `verify`, with nonempty
[`schedulingGates`](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-scheduling-readiness/),
`restartPolicy: Never`, exact guarded platform images, explicit run/dataset
environment and bounded rootless/read-only/no-token/no-volume security settings.
There is no Job controller, backoff or automatic replay. Nine additive policies
limit these Pods to the eight MongoDB services and CoreDNS and permit only the
matching MongoDB ingress; original application permissions are unchanged.
The offline JSON Patch template tests the Pod UID, name, namespace, exact
labels/annotations and entire spec before removing its gate. It deliberately
fails on unhandled admission defaults or mutation. It is **not** a runnable
controller or authorization API.

`app/deathstarbench_hotel_initialization_journal.py` provides real private
local persistence under the original acquired run lease. Canonical bounded
events are exclusive single-link, no-follow, fsynced files chained to the
exact run/plan/stage/attempt. Each dispatch intent first persists a separate
exclusive **run-wide latch**. Latch and event names cannot be reused by a new
attempt; there is no delete, reset or repair path. Partial writes, fsync/response
loss, orphan/competing latches, history changes, crossed resource UIDs and lost
leases fail closed. Reopening any latched attempt is cleanup-only even after
an apparently successful initializer or verifier. Local command-output records
must match the exact run/dataset/mode/Pod UID and still retain false start gates.
They are unverified observations, not fresh database or Pod execution proof.

These foundations alone implement no executor. The separate live controller
described below consumes them; their historical JSON and closed factory flags
are not promoted into execution authority. Old support-operator success/resume
and per-store Go markers cannot substitute for fresh live checks. Service
discovery/preload/restart, authentication/atomic reservation semantics,
measurement epochs and live-cloud release qualification remain separate
prerequisites before UI release.

The [retained full-byte observation](qualification/deathstarbench-hotel-image-access-v1.json)
records successful [run 38777055813](https://github.com/tonymarkel/oci-self-service-benchmarks/actions/runs/38777055813):
six exact indexes, seven platform manifests/configs and all 45 compressed layer
bodies were verified, including repeated downloads of shared layers. Layer
bodies totalled 623,098,966 bytes; total downloads including metadata and
anonymous token responses were 623,184,724 bytes. The artifact fingerprint
matches the reviewed guarded publication. The record retains the exact receipt
hash and per-platform images/counts, without treating that historical JSON as
a capability or as live workload qualification.

## Private live Hotel initialization controller

`scripts/qualify_deathstarbench_hotel_initialization.py` is a separate,
cleanup-owning private workflow. It does not change the old support operator,
Social Network runtime, public form, benchmark dispatch or comparison cohorts.
It creates the same five-role billable infrastructure, freshly adopts both
original and guarded artifacts, prepares K3s and validates the original
support-only stage before adding initialization resources. All ten application
Deployments remain at zero throughout this workflow.

`app/deathstarbench_hotel_live_controller.py` joins fresh cloud ownership,
provider volume and read-only filesystem checks with the strict live API
validator in `app/deathstarbench_hotel_live_k3s.py`. Each observation validates
the unchanged complete support graph, CA, kube-system/workload Namespace UIDs,
role Node UIDs, database PV/PVC/Pod identities and container executions.
Services and controller-owned EndpointSlices must resolve to the exact ready
support Pod UIDs and private IPs; matching labels alone do not suffice.

Nine additive policies and two standalone, scheduling-gated Pods are created
once. The live Pod validator accepts only specifically audited Kubernetes
defaults, rejecting injected containers, credentials, storage, placement,
environment, command, image or security changes. Discovery must prove that
every served dynamic mutating-webhook, policy and binding collection is empty;
unknown mutators, discovery failures and denied reads fail closed. The check is
repeated immediately before each write, because a pre-admission JSON Patch
test alone cannot prevent an UPDATE admission mutator from changing an image.
This is not protection against a concurrent privileged cluster administrator.
Immediately before dispatch,
the controller rechecks ownership/storage/API continuity and durably records
the exclusive local latch. One atomic JSON Patch tests the freshly admitted
Pod identity and entire spec before removing its gate. There is no Job,
restart, backoff, reset, repair, apply reconciliation or dispatch retry.

The controller accepts only the exact native image, zero restarts, pinned Pod
UID, successful terminal exit and bounded run/dataset/mode-specific output.
It then releases the separate global read-only verifier using the same
durable-intent policy. Narrow successful initialization evidence does not
grant application-start, runtime, measurement, all-cloud or public-release
qualification. Every normal or failed invocation attempts exact owned
infrastructure destruction and independent absence proof.

An interrupted or uncertain attempt is **cleanup-only**, even if initialization
finished remotely. The private immutable attempt pin, journal and latch files
are retained. `--cleanup-only RUN_ID` needs no source adoption, registry access,
SSH or Kubernetes dispatch; it uses the original provider identity. A fresh
run with fresh infrastructure is required to try initialization again. The
qualification-only interruption checkpoints include loss of the accepted
initializer response before the client parses its acknowledgement.

### Validation status

The controller, admitted-API and cleanup-wrapper tests exercise simulated
success, response loss, identity drift and denied admission discovery. They do
not substitute for live MongoDB/K3s initialization qualification.

The first Azure Arm invocation on 2026-10-15 stopped during fresh baseline
artifact inspection, before allocating a run or provisioning cloud resources.
The Docker CDN certificate was reissued by the local corporate web gateway,
but neither Python's default CA bundle, Mozilla's CA bundle nor the existing
OS-managed trust store validated it. TLS and hostname verification were not
disabled; no trust anchors were imported or system trust settings changed.
That initial invocation did not allocate a run or create cloud resources. The
original support-only evidence is not promoted to satisfy the new controller's
initialization or verifier gates.

The operator subsequently confirmed that Docker URLs are intentionally blocked
on the local corporate network and authorized temporary cloud compute for
registry access. The private Hotel CLI therefore supports an explicit
`--registry-egress` mapping for operator-owned SSH TCP forwards. Only the seven
reviewed registry, anonymous-authentication and CDN HTTPS hosts on port 443 use
this context-bound connector. URLs, HTTP Host, TLS server names, certificate and
hostname validation, anonymous token scoping, redirect restrictions and full
byte/digest validation remain unchanged. Global sockets, cloud SDK traffic,
system proxy settings and system trust are not modified. A failed forward
never falls back to the blocked direct route.

The mapping must be a private owned regular file; its immutable one-use
process-local context cannot be restored from a saved observation. The
separate helper owner must attest the VM, strict SSH host key and restricted
forwarding configuration and must destroy the helper independently of each
benchmark's cleanup. The mapping validator does not infer cloud ownership or
SSH authenticity merely from a listening loopback port. No cloud credentials
or private SSH keys are copied onto the relay, no VM cloud identity is needed,
and the helper does not cache or terminate registry HTTPS. Explicit egress
observations are retained outside public history; they never substitute for
fresh artifact or image-access capabilities. Cleanup-only recovery refuses the
mapping option and has no registry/SSH dependency.

The authorized Azure relay subsequently verified the same public Docker config
through all three registry clients with certificate, hostname and digest checks
enabled. Its dedicated VM has no managed identity, receives no local cloud
credentials or private SSH keys, and permits only the seven reviewed SSH TCP
forwarding destinations. A quota-blocked partial deployment was independently
proved absent in two Azure checks before the helper was moved to a different
VM family; its VM, disk, network and SSH host-key identities were preserved.

Azure Arm attempt `4101173fd91d`, using source commit `17acb335`, then passed
fresh artifact adoption, both full-byte image-access factories, the exact
four-node K3s readiness checks and backing-service staging with all ten
application Deployments paused. Initializer preflight stopped before object
creation or any dispatch intent with an EndpointSlice port-validation error.
Review found that the validator rejected legitimate empty paused-Service
placeholders. Kubernetes explicitly creates these
[zero-Pod placeholders](https://github.com/kubernetes/kubernetes/blob/v1.36.4/staging/src/k8s.io/endpointslice/reconciler.go#L241);
the API can represent their empty lists as
[JSON null](https://github.com/kubernetes/kubernetes/issues/135968).
The run retained its initial journal event, no dispatch latches and a successful
two-pass independent Azure absence proof. This is useful failure-path evidence,
not successful initialization or separate global read-only verification.

The validator now accepts only empty, null or omitted ports/endpoints for these
paused-Service placeholders. Active support Services still require their full
typed port maps, exact Pod/address/node/UID bindings and ready endpoints; only
port-list order is ignored. Extra, duplicate, missing or changed active ports
and any published paused-application endpoint remain failures. Regression
fixtures include all ten paused placeholders rather than omitting them. This
normalization does not grant application-start or release authority, and a
fresh live attempt is required to qualify the changed controller.

With source `a22db227`, GCP Arm attempt `e88f16cf1871` passed the live preflight
and created both gated Pods. The requested `initialize-response-returned`
interruption fired after the remote gate-release command returned but before
the controller parsed its acknowledgement. Exit 130 was expected: the journal
retained three events and one initializer latch, no verifier latch or exit
acknowledgement, and an explicitly unknown initialization outcome. There was no
replay or verifier dispatch. Cleanup completed with two independent absence
passes covering the twenty expected GCP resources. This was an injected client
acknowledgement-loss test, not an actual network outage or successful dataset
initialization.

Azure Arm attempt `35d8be5e471a`, also at `a22db227`, accepted one initializer
release but stopped during a later status observation because the terminated
container and Pod phase did not agree. The mismatching phase and exit code were
not retained, so neither the exact cause nor initialization success is claimed.
Its one initializer latch remained, no verifier ran, and two independent Azure
absence passes confirmed cleanup. The pinned Kubernetes implementation can
[retain a nonterminal Pod phase while container termination is reported](https://github.com/kubernetes/kubernetes/blob/v1.36.4/pkg/kubelet/status/status_manager.go#L1299-L1313).
The observer now treats only `Pending` or `Running` in this situation as pending
evidence, after all existing identity, timestamp, reason, signal and container
checks. It accepts no exit code until the Pod phase is coherent. The existing
bounded read-only wait continues; dispatch is never repeated. Contradictory
terminal phases remain failures. Fresh positive qualification is still pending.

## Next review slices and release prerequisites

1. **Immutable artifacts and public access.** Hotel now has native guarded
   publication and complete anonymous byte-access observations for all seven
   bound image platforms. A later runtime attempt must freshly obtain its own
   artifact/access capabilities and qualify actual guest/CRI behavior; this
   historical observation cannot replace those checks. Media remains separate.
   Read-only candidate adoption alone is not the access gate.
   Version tags/configuration metadata alone are not executed
   backing-image version or CPU-compatibility proof. Preparation receipts alone
   cannot satisfy this gate.
2. **Workload-aware execution and measurement.** Pass the selected immutable
   profile through deployment/readiness, network qualification, database
   preparation, run-bound lock snapshots, journals, recovery, and cleanup.
   Candidate artifact/storage preflight and support-only K3s apply/readiness now
   supply the first boundaries. The private cleanup-owning operator now joins
   those boundaries; live support-stage evidence must be evaluated separately.
   Guarded application startup and semantic initialization/measurement remain
   required before release.
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

The audits remain immutable records of the original upstream source. Candidate
patch receipts identify the corrections adopted after that audit; the audit
itself is not rewritten to imply qualification. HTTP 200 alone is not a
successful login or reservation. Media still needs deployed dataset readiness
and proof that a composed review reaches persistent storage and its user/movie
references. Neither candidate is qualified for public execution yet.

Maintainers review each slice before merge. No automatic merge or release-flag
promotion is part of this workflow.
