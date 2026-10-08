# Distributed Hotel Reservation and Media Microservices

Status, 2026-10-08: **native artifact candidates, not public benchmarks**.
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
The eight database claims total 80 GiB within the current 100-GiB disk;
guest free-space/storage attestation is still a later runtime requirement.
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
libraries receive native linkage checks. The frontend parses its actual baked
nginx configuration and loads Lua/Thrift modules with explicit mock DNS, not a
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

## Next review slices and release prerequisites

1. **Immutable artifacts and public access.** Complete native publication and
   retain the generated locks, build receipts, and offline smoke evidence.
   Independently qualify anonymous pulls of every exact platform digest before
   runtime adoption. Version tags/configuration metadata alone are not executed
   backing-image version or CPU-compatibility proof. Preparation receipts alone
   cannot satisfy this gate.
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

The audits remain immutable records of the original upstream source. Candidate
patch receipts identify the corrections adopted after that audit; the audit
itself is not rewritten to imply qualification. HTTP 200 alone is not a
successful login or reservation. Media still needs deployed dataset readiness
and proof that a composed review reaches persistent storage and its user/movie
references. Neither candidate is qualified for public execution yet.

Maintainers review each slice before merge. No automatic merge or release-flag
promotion is part of this workflow.
