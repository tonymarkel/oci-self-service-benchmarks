# Hotel Reservation distributed candidate: pinned-source audit

This is a **foundation candidate**, not a runnable or released Hotel
Reservation benchmark. Both JSON assets have `released: false`. There is no
adopted Hotel image lock, image publication, dataset executor, immutable load
driver, semantic measurement attestation, or live-cloud qualification in this
slice. The existing released Social Network bundle is unchanged. Neither
these files nor the workload registry authorize provisioning Hotel resources.

The source is [DeathStarBench commit
`6ecb09706140f8730b5385c08f1386c654c3c526`](https://github.com/delimitrou/DeathStarBench/tree/6ecb09706140f8730b5385c08f1386c654c3c526/hotelReservation).
The audited checkout's `HEAD` matched that commit. The SHA-256 values below
were computed from its exact file bytes, including their original line
endings; they are not hashes of API- or Markdown-rendered text.

## Source anchors

All paths are relative to that upstream repository. These are audit anchors,
not an assertion that a future image preparation step already validates them.
The future build context must verify the complete tracked source inventory,
vendored dependencies, and strict patch anchors before publication.

| Upstream path | Original file SHA-256 |
| --- | --- |
| `hotelReservation/Dockerfile` | `99651371ef0722a273bbef9c496608593257f4a2a13ec1f3246bdd1b7c048d4a` |
| `hotelReservation/config.json` | `758febb12cf66301e91755fb4123c4dfca985a6f4a9a461c18b6a9a38bcfddaa` |
| `hotelReservation/docker-compose.yml` | `988b3e3d4c0c01c5032f47d6ff69db56a8245966ddb7dcaaef1b726ff641bc12` |
| `hotelReservation/go.mod` | `a5a886b6b67cea384f09f4497cc273b1d710dbe719d9904bd9258446fa38ce90` |
| `hotelReservation/go.sum` | `347d12f01fb8f6c442a5b5cfb26d3377e702531f6b4a193c6d3c81829a3dd435` |
| `hotelReservation/registry/registry.go` | `267ae21fc993be6689e0fdcceddad79afd6efffef9d1cd4de57f0c45800f7788` |
| `hotelReservation/dialer/dialer.go` | `fbf886d052dc4a5f3c66b548c866a2e21c76b00234ccbe6f13eb11f16d8b42ab` |
| `hotelReservation/services/frontend/server.go` | `453d3efeeb27c28896434cd1e7fa18f1810678b82835be909094bc629fe7764c` |
| `hotelReservation/services/reservation/server.go` | `cc24f3bd38b845b0cb9fe591f811dc33abe36d02c53bc1c28f426739d26c1c0e` |
| `hotelReservation/tracing/tracer.go` | `a50f0d9ddff8bf5fd1d2c4001f4396322b9a2de6113668591bfc006b12331a0c` |
| `hotelReservation/tune/setting.go` | `c2993b74753e865fe3182faf991b2218a40d7c73d3932c12a2fbebf7ca724c51` |
| `hotelReservation/vendor/github.com/bradfitz/gomemcache/memcache/memcache.go` | `ec73b17dffd561d75edc9d509b85acf340957e237b79f6ed2d4f6c8c75e26810` |
| `hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua` | `af6230aaaf220fceaef6208d416c5a9d75f15f66d70e3cde00b6e441957bf352` |
| `hotelReservation/cmd/attractions/db.go` | `be72ded0c1cadc8c515457fda790aa936e7cde307aca2fbf4dcf4d28d0015de2` |
| `hotelReservation/cmd/geo/db.go` | `1080bc0fe779b9d2523d2289bc52ef7a90fbe9be98da2a730875617003c49219` |
| `hotelReservation/cmd/profile/db.go` | `cbfa8112e162e62696a28a2bdf23245368ac8f7165dd5ce5bdcd939b23da7191` |
| `hotelReservation/cmd/rate/db.go` | `ca75806fddf2d6119321de18854672abfd60a988e3de9b7949db6db5c8d66be2` |
| `hotelReservation/cmd/recommendation/db.go` | `c921b415b63e0ed69d1ccecfdfde8d8671cb268419508df95b827fd9c65899b7` |
| `hotelReservation/cmd/reservation/db.go` | `c4c8858fd1a6101c65ac898835da9a4a731861c8b9369100f2177e28414a013e` |
| `hotelReservation/cmd/review/db.go` | `49bc45dd35e5caf27cdb8dcc3b7422e78f2d1db0aa0a7cef98bea17dd80cd740` |
| `hotelReservation/cmd/user/db.go` | `aced9202d319f1da37060e39513520ff073993572dc12b0869fc24d9dd668d1b` |

## Frozen candidate placement and networking

The candidate reuses the existing five roles: control, database, cache,
application, and a separate load generator. Its 24 component processes do not
mean 24 cloud VMs. There are ten application processes, eight MongoDB
instances, four memcached instances, and two control components. Review and
attractions are included because the pinned Compose graph and frontend
construct both clients, even though the selected mixed workload does not
exercise their HTTP routes.

| Component group | Candidate placement and private port |
| --- | --- |
| `frontend` | Application; container TCP 5000; service and NodePort 8080; `externalTrafficPolicy: Local` |
| `profile`, `search`, `geo`, `rate`, `recommendation`, `user`, `reservation`, `review`, `attractions` | Application; TCP 8081, 8082, 8083, 8084, 8085, 8086, 8087, 8088, 8089 respectively |
| `memcached-profile`, `memcached-rate`, `memcached-reserve`, `memcached-review` | Cache; TCP 11211 |
| `mongodb-attractions`, `mongodb-geo`, `mongodb-profile`, `mongodb-rate`, `mongodb-recommendation`, `mongodb-reservation`, `mongodb-review`, `mongodb-user` | Database; TCP 27017; independent persistent-volume subdirectory equal to component name |
| `consul` | Control; TCP 8500; run-scoped, single-agent service discovery |
| `jaeger` | Control; UDP 6831 for application traces; private TCP 16686 for readiness/administration |

Application commands are exact `/go/bin/<component>` executables, matching
the pinned Go build's installation layout. The future image must preserve
the `/workspace` working directory and bake the audited `config.json`; these
assets do not introduce a mutable runtime checkout or Helm chart. Explicit
application environment fixes TLS off, GC at 100, tracing sample ratio 0.01,
memcached timeout 2 seconds, and log level INFO. The candidate Consul command
is `consul agent -dev -client=0.0.0.0`: its discovery state is per run, not a
durable database. Exact image permissions, entrypoint behavior, and readiness
must be qualified before this command is adopted. Other backing images retain
their locked-image entrypoint/command defaults; no candidate image lock exists
yet. No memcached sizing or thread-count claim is inferred from the upstream
Compose environment variables.

The 41 component dependency edges in `network-policies.json` are the audited
runtime graph: frontend to its seven downstream gRPC clients; search to geo
and rate; each database-backed service to its own MongoDB; four cache clients
to their respective memcached; every application process to Consul TCP 8500
and Jaeger UDP 6831. The pinned tracer uses a local probabilistic sampler and
UDP reporter, not a required remote sampling HTTP edge. Consul registrations
have no application health-check callbacks. Unused upstream externally
published Consul, tracing, and database ports are not allowed workload edges.

DNS to the fixed cluster CoreDNS selector, default-deny ingress/egress, and
load-generator `/32` ingress are derived by the foundation renderer. Frontend
pod ingress must use **TCP 5000**, while the cloud/application-node ingress
remains **TCP 8080**. No additional cloud frontend port or K3s NodePort range
is needed. Live qualification still needs to prove the Consul-discovered Pod
IP traffic traverses the audited component policies and cannot reach denied
roles or public tracing/discovery endpoints.

## Source corrections required before building a runnable image

No correction listed here has been implemented by these candidate assets.
Each adopted correction needs exact original-hash/occurrence checks, a
recorded patched SHA-256, fixtures demonstrating semantics, and inclusion in
the build context fingerprint and workload/dataset/driver revisions.

1. The mixed Lua request script's reservation function references undefined
   `lat` and `lon`. Supply explicit valid coordinates and bind the original
   `http://localhost:5000` target to the attested private NodePort endpoint.
   Preserve the 60% search, 39% recommendation, 0.5% user-login, and 0.5%
   reservation mix. A declared per-worker deterministic random seed, rather
   than the source's wall-clock seed, needs its own driver identity and tests.
2. `cmd/user/db.go` formats a decimal **string** suffix with `%x`, producing
   ASCII-hex usernames instead of the Lua driver's `Cornell_<decimal>` names.
   Reconcile seed and driver deliberately; correcting the seeded name format
   changes the dataset identity. HTTP 200 does not prove login succeeded.
3. Frontend review and attractions dial bare `srv-review` and
   `srv-attractions`, unlike the Consul URLs used by other gRPC clients.
   Correct both call sites to use the existing shared Consul resolution path,
   or explicitly qualify a separately versioned DNS-alias design. The
   current `WithBalancer` only supplies round-robin configuration and does
   not turn a bare name into a Consul resolver.
4. Reservation `CheckAvailability` assumes memcached `GetMulti` signals
   missing keys with `ErrCacheMiss`. Its vendored implementation documents
   absent keys in a non-nil map with nil error. Determine misses by comparing
   requested and returned keys, for both capacity and reservation-count
   reads. The miss Mongo query also needs the `hotelId` field around `$in`.
   Completely cold cache can skip required database reads without an HTTP
   failure; partially warm cache can produce different behavior. Correct
   and test cache-miss/hit paths, date cache keys, and controlled capacity
   exhaustion before declaring the search/reservation test representative.
5. All eight database-backed startup processes unconditionally `InsertMany`
   their seed data. A restarted application can duplicate seed rows. Freeze
   a non-replayable seed contract or strict idempotent seeding design, reject
   duplicate initial state, and attest restart behavior. Merely observing
   that each process started is insufficient.

## Dataset and semantic readiness requirements

The pinned source generates its dataset in the Go startup programs rather
than using Social Network's external Reed98 HTTP initializer. Its `data/*.json`
files are not the authoritative seed used by those programs. Exact baseline
cardinalities from the original seeders are:

| MongoDB component | Database / collection | Initial documents |
| --- | --- | ---: |
| `mongodb-geo` | `geo-db / geo` | 80 |
| `mongodb-profile` | `profile-db / hotels` | 80 |
| `mongodb-rate` | `rate-db / inventory` | 27 |
| `mongodb-recommendation` | `recommendation-db / recommendation` | 80 |
| `mongodb-user` | `user-db / user` | 501 |
| `mongodb-reservation` | `reservation-db / number` | 80 |
| `mongodb-reservation` | `reservation-db / reservation` | 1 |
| `mongodb-review` | `review-db / reviews` | 6 |
| `mongodb-attractions` | `attractions-db / hotels` | 6 |
| `mongodb-attractions` | `attractions-db / restaurants` | 6 |
| `mongodb-attractions` | `attractions-db / museums` | 6 |

Counts alone are not a dataset fingerprint. Verify hotel/user IDs, date
inventory, room capacities, expected passwords, no duplicates, and versioned
seed source/patch hashes directly from the correct database pods. K3s
attestation must check one replica per component, exact node roles and
architectures, images, services, PVC binding/node affinity, policies, and
stable restart counts. Consul must register exactly the expected gRPC service
names, live Pod addresses, and ports; reject stale/duplicate registrations.

Frontend readiness must check seeded search/recommendation responses and
successful login body semantics. Live **qualification** must exercise a
controlled reservation followed by availability/capacity checks, including
cold-cache behavior; a successful `/` or empty JSON response is not enough.
Any per-run mutating probe must be journaled and have an explicitly declared
post-probe dataset identity, not replayed silently after disconnects.

Warmup and measured reservations change persistent state and consume finite
room inventory. The measurement contract must declare whether warmup changes
are retained or reset through a guarded, attested phase. Record successful,
rejected, HTTP-error, and socket-error requests separately; the application
can return login/reservation failure messages with HTTP 200. The driver must
not count these bodies as successful transactions or confuse an expected
full-capacity rejection with a transport failure. A frozen request mix and
dataset/driver/measurement revision must separate incompatible comparisons.

## Image, architecture, lifecycle, and release blockers

The required runtime image keys are exactly `consul`, `hotel-reservation`,
`jaeger`, `memcached`, and `mongodb`. The pinned Go Dockerfile starts from
mutable `golang:1.21`, uses vendored dependencies and `CGO_ENABLED=0`, and
provides no immutable production image artifact. Prepare native amd64/arm64
application candidates, validate the tracked source inventory, bake the
checked configuration and strict patches, and pin exact platform manifests.
Freeze all backing-image versions/digests as well; the upstream graph uses
MongoDB 5.0, so adopting Social's MongoDB 4.4.6 digest without an explicit
qualified software/dataset revision would change the test contract.

Only application-role components use the selected shape's architecture.
Control, cache, database, and load-generator roles remain the existing fixed
x86 support-node contracts. Qualification must check the chosen MongoDB
image's CPU-feature requirements on those database shapes and the exact
Consul/Jaeger/container entrypoints on support nodes. The existence of an Arm
application image does not justify running supporting MongoDB containers on
an unqualified Arm host.

The Social-only wrk2 image binds a different request-script SHA and a fixed
user-index argument. It cannot attest Hotel requests. Publish a separate
immutable Hotel driver with its source/patch hashes, exact wrk2/LuaJIT and
LuaSocket identities, protected runtime, semantic-error accounting, and
anonymous digest-pull proof. Verify application and backing-image registry
access and publication evidence independently; tags or a successful build
are not release evidence.

Before public catalog/API selection, add workload-aware release preflight,
run-bound image snapshots, deployment/measurement journals, recovery,
namespace/PV/guest cleanup, comparison metadata, and fail-closed dispatch.
Do not reuse a Social journal, Reed98 initializer, database attestation,
request driver, or namespace for Hotel. Recovery must use the saved workload
identity, and guarded cleanup must reconcile all eight database paths and
the separate control discovery process without broadening its targets.
Cross-cloud live workload, semantic measurement, denied-edge, interruption,
and resource-deletion qualification remains required. None is claimed by
this foundation slice.
