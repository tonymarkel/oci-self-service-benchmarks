# Media Microservices candidate audit

This is a foundation contract, not a runnable or publicly released benchmark.
Both candidate JSON assets deliberately have `released: false`. They describe
the component inventory and allowed service graph only; they do not attest a
published image, a successfully initialized dataset, or a live-cloud run.

The source inspected is the exact DeathStarBench commit
[`6ecb09706140f8730b5385c08f1386c654c3c526`](https://github.com/delimitrou/DeathStarBench/tree/6ecb09706140f8730b5385c08f1386c654c3c526/mediaMicroservices).
No moving upstream branch or chart may supply runtime files.

## Workload scope and placement

The selected request workload is
`mediaMicroservices/wrk2/scripts/media-microservices/compose-review.lua`.
The pinned Compose deployment, excluding its DNS helper, has 32 components:
12 C++ applications, an Nginx/OpenResty frontend, eight MongoDB instances,
seven Memcached instances, three Redis instances and Jaeger.

`components.json` places all application services and the frontend on the
selected application VM, all MongoDB instances on the fixed database VM,
Memcached and Redis on the fixed cache VM, and Jaeger on the control VM.
The fifth VM is the external, fixed x86_64 load generator, not a workload pod.
MongoDB requires eight separately named persistent subdirectories on the
dedicated database filesystem; cache contents are ephemeral. The current
provider-neutral 100-GiB database volume is not a substitute for checking the
future rendered claims, free capacity and initialized database contents.

Only `nginx-web-server` is a NodePort service: TCP 8080 with
`externalTrafficPolicy: Local`. All other services are ClusterIP. The frontend
uses `fqdn_suffix=.deathstarbench-media.svc.cluster.local`. Jaeger keeps its
upstream DNS name `jaeger`, which the pinned C++ and frontend configuration
already references.

`PageService` is compiled upstream, but the pinned Compose deployment does not
start it and compose-review does not call it. It is intentionally absent from
this workload, rather than silently introducing a different read workload.
The `dns-media` helper is excluded: it mounts the host Docker socket and
resolver files, and Kubernetes provides the required DNS itself.

## Network review

The 49 exact edges in `network-policies.json` comprise 36 dependency edges
and 13 tracing edges. They are derived from these pinned source paths:

- `mediaMicroservices/config/service-config.json` defines service endpoints.
- `mediaMicroservices/src/*/*Service.cpp` constructs the Thrift, Redis,
  MongoDB and Memcached client pools.
- `mediaMicroservices/nginx-web-server/lua-scripts/wrk2-api/review/compose.lua`
  fans a request out to user, text, movie-id and unique-id services.
- The frontend's user/movie registration and cast/movie/plot write handlers
  provide the initialization edges.
- `mediaMicroservices/config/jaeger-config.yml` and
  `mediaMicroservices/nginx-web-server/jaeger-config.json` select Jaeger.

Application RPCs use TCP 9090, MongoDB TCP 27017, Memcached TCP 11211 and
Redis TCP 6379. All 13 application components may send tracing to Jaeger
UDP 6831. In particular, user, text, unique-id, movie-id and rating services
call back into compose-review; a frontend-to-compose-only policy would not
permit the real request path.

The common renderer must add namespace default-deny, narrowly scoped
Kubernetes DNS TCP/UDP 53, and the inventory's single private load-generator
`/32` ingress to frontend TCP 8080. These rules are not additional unchecked
component edges. Jaeger TCP 16686 is internal administration/readiness, not
a public endpoint. Zipkin collector configuration is retained from upstream,
but no application edge grants TCP 9411 access.

## Exact source anchors

All hashes below are SHA-256 of the original, unmodified pinned files. They
are source-review evidence, not hashes of hypothetical patched outputs.
Paths in this table are relative to `mediaMicroservices/`.

| Path | Original SHA-256 |
| --- | --- |
| `docker-compose.yml` | `f566fa02214e1b1d887a29a2a5f850b1e1bc579f196a89ce2320f21c3f37ad2b` |
| `Dockerfile` | `0322b8e036091c887cab74d3b49cb84a469f1d26f732c22fc3c0bedb979c9563` |
| `docker/thrift-microservice-deps/cpp/Dockerfile` | `25114f38329d54e11a6b3782b8332153b37d9b6df9725a636c9d433502244016` |
| `docker/openresty-thrift/xenial/Dockerfile` | `5319eb130887b29a020503c686f10e35f5e9bcc7cd6ead95237e57567dc4f18a` |
| `config/service-config.json` | `558a19ce892498f716988209627da08d2f5e7a5c8a8f1afa24f268040c5c95dc` |
| `config/jaeger-config.yml` | `cff14dc9d8ced8c0bfafad247b05f0a4fb8c744e4c404fcdf54acece452e6ee1` |
| `nginx-web-server/conf/nginx.conf` | `ec0080858f8d10a277ad55e94675f91aa112f41fb845cb597744f885c490dec7` |
| `nginx-web-server/jaeger-config.json` | `a92a36fc877c8b3e1967c5c543cc7030f72c0659320ab7108a70a0f2e0b19703` |
| `nginx-web-server/lua-scripts/wrk2-api/movie-info/write.lua` | `8872cc5830aedb4c3641eb31fadb3661e9e0816d6f8c0bef692a586e9cee50ce` |
| `nginx-web-server/lua-scripts/wrk2-api/review/compose.lua` | `0cd3b938ebc485e7a80ea2dec6212b5172165c85d4d42838b33869798fb2d119` |
| `scripts/write_movie_info.py` | `de6f06f7e809361b00cfa33f40365a0d5ae7f84a69bd5d2120d63eadcd91b938` |
| `scripts/register_users.sh` | `0de03dd5bfb1e786dd0fb766d137dd20b4b505694c17be3b793206b360519c56` |
| `scripts/register_movies.sh` | `911554a01a5af5fe5a8358cdfa5473aa8662058e66627dac29d028328162c0b5` |
| `datasets/tmdb/casts.json` | `d6de92717fcdaf9e16e7c499a47ae9925a0c3b32d1459ce6c992d7ac8d0e968d` |
| `datasets/tmdb/movies.json` | `b9085236dc7d3b04e07b72a9d4b77cf447dde93ce263a00c1e2dfd82ff2b8fb5` |
| `wrk2/scripts/media-microservices/compose-review.lua` | `147fdc983955c46a06c80ab41c188e0d946b37eb45e019a1f0a9e3073017bbaa` |

## Required image and source adaptations

The required image keys are `jaeger`, `media-microservices`, `memcached`,
`mongodb`, `nginx-web-server` and `redis`. No candidate Media application,
frontend or load-driver image is published or selected by this slice.

The existing third-party image versions can be reused only through validated
platform-manifest digests: MongoDB 4.4.6, Redis 7.2.4, Memcached 1.6.26 and
Jaeger 1.57.0. Application/frontend builds must produce independently
verified amd64 and arm64 artifacts; infrastructure support for an Arm shape
is not proof that the Media images run on it. Support roles and the load
generator remain x86_64 under the current topology contract.

Media's C++ dependency recipe uses mongo-c-driver 1.14.0, whereas the pinned
Social Network recipe uses 1.15.0 and additional dependencies. A Social
application image cannot stand in for the Media service image. The OpenResty
Dockerfile is byte-identical to Social Network's at this commit, so reviewed
legacy-download and pinned LuaRocks corrections are reusable, but the
Media `gen-lua`, handlers, configuration and licenses must be baked into its
own immutable frontend image. No pod may clone or download them at startup.

Before a Media image/initializer can be qualified, preserve and verify the
existing compact runner's relevant adaptations, rather than assuming these
candidate assets have already fixed upstream:

- Correct the `charactor` field spelling to `character` in the movie-info
  write handler so it matches the Thrift definition and input JSON.
- Use `client_body_buffer_size 64k` for the pinned handlers that read an
  in-memory request body; review the fixed ingress worker/file-limit tuning.
- Turn a missing poster into an empty thumbnail array, not `[null]`.
- Register each TMDB title once, retaining its first movie mapping; the
  upstream MovieId service requires unique titles.
- Make all four aiohttp upload/registration response handlers fail on HTTP
  errors and make both registration shell scripts fail on HTTP/command errors.
- Render the K3s resolver address and namespace suffix exactly; the Compose
  resolver is Docker-specific. Correct the misspelled frontend Jaeger
  `diabled` flag and adopt a reviewed, fixed tracing configuration.

Adaptations must have drift-checked source anchors, exact patched hashes and
revisions in the eventual build/initializer evidence. Build provenance,
licenses/SBOM, runtime smoke checks and anonymous digest pulls are required
before accepting an image lock. Mutable Xenial package repositories and
upstream downloads are rebuild risks even after a particular runtime digest
is immutable.

## Dataset and semantic readiness

The pinned dataset contains 18,194 distinct cast IDs and 1,000 distinct movie
IDs, but only 995 distinct TMDB titles. All cast records have the required
ID/name/gender/biography keys. Three movie poster values are null. The two
registration scripts add 1,000 synthetic users and 1,000 synthetic movie
titles. With the reviewed first-title deduplication, the intended initial
state therefore has:

- 18,194 cast-info documents.
- 1,000 movie-info documents and 1,000 plots.
- 1,995 movie-id title mappings: 995 TMDB plus 1,000 synthetic mappings.
- 1,000 synthetic users, excluding any separately journaled readiness fixture.
- No preexisting benchmark reviews or user/movie review-list entries.

Exact collection counts and key ranges must be checked on the database VM;
initializer console progress and a listening port are not sufficient. Keep
the durable, one-shot initializer journal and response-loss recovery semantics
used by Social Network, binding it to these Media sources and dataset hashes.
No ambiguous initialization or measured request phase may be replayed.

After initialization, a bounded, explicitly accounted compose-review canary
must prove that a successful HTTP response corresponds to a stored review
and matching user/movie review references. This is a mutating probe and must
be journaled and included in, or removed through a qualified process from,
the initial-state contract. Do not retry it blindly after response loss.
Warmup and measurement must use the same immutable driver and verify that
database state is meaningful, not only that Kubernetes pods are Ready.

The pinned compose-review driver selects from 1,000 users and movie titles,
with 256-character text and ratings from 0 through 10. It appends
`/wrk2-api/review/compose` to the wrk base URL, so the base URL must be `/`;
passing that endpoint to wrk again duplicates the route. Its original
wall-clock random seed is not a deterministic trace. Any fixed-seed
adaptation needs its own declared request-script hash and measurement
revision. The existing Social driver hardcodes a different script, three
Social target anchors and a 962-user range, and must not be reused unchanged.

## Upstream Helm hazards and remaining gate

The pinned `helm-chart/mediamicroservices/Chart.yaml` declares `page-service`,
but that subchart directory is absent. It omits the required
`review-storage-memcached` dependency even though its subchart and C++ client
configuration exist. The frontend subchart's init container clones a moving
third-party fork using a mutable Git image. These charts are source-review
inputs only, not a deployment method.

The shared static renderer must validate exact component names, roles,
commands, images, persistence, complete ingress/egress policy pairs and
runtime image identities. Workload-specific journals, database storage
counts, readiness/dataset markers and comparison fingerprints must remain
separate from the released Social Network identities. Until immutable image
publication, guest integration, failure-path qualification and live provider
qualification are complete, Media stays unavailable for public provisioning.
