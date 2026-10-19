# Hotel live startup controller and cleanup/recovery

Date: 2026-10-19

Status: proposed written specification; conversational design approved, written-spec review pending.

Foundation: merged PR #42, commit `444f42641ebac9b7314484ec50aca54e24e18512`.

## Intent and success criteria

Provide a private, cleanup-owning qualification path that initializes a fresh
Hotel dataset, starts the ten startup-safe application processes under exact
identity and dispatch controls, records their execution and service identities,
and independently proves destruction of its owned cloud infrastructure.

A successful invocation means all ten exact processes were observed running
without restart, the nine backend Consul registrations and all application
Service endpoints matched those processes, and cleanup completed. It does not
mean that the application is semantically ready or a benchmark is released.
The frontend is the tenth application and does not register in Consul.

The user approved a separate private operator, one retained lease across the
whole lifecycle, durable intent before mutations, no uncertain replay, exact
owned-resource cleanup, failure-path validation, and closed public gates.

Non-goals are public UI/dispatch, benchmark traffic or measurement, functional
query/authentication/reservation qualification, database reset/repair/upsert,
application restart/replacement, Kubernetes version upgrades, new image recipes,
and changing the existing support-only or initialization operators' contracts.
No implementation or live qualification is authorized by this document alone.

## Architecture and alternatives

Use a new startup operator and startup-specific observer, controller, journal,
immutable lifecycle pin and private state/CLI boundary. Reuse proven cloud,
support, initialization and cleanup primitives under their existing contracts.

Extending the current initialization operator would reduce orchestration code,
but would change its promise that all applications stay paused and teardown
occurs before return. Calling it and then starting applications cannot work:
its `finally` block has already destroyed the infrastructure. Combining this
slice with public benchmark release would additionally require semantic,
measurement and multi-cloud release qualification. The separate operator keeps
those responsibilities independent.

The startup operator owns this complete lifecycle:

```text
fresh inputs and immutable intent -> provision -> support readiness
  -> guarded initialization and global read-only verification
  -> startup create intent -> ten gated Pods pinned
  -> per-component intent, single release, bounded observation
  -> final execution/identity observation -> destroy -> independent absence
```

Any failure after cleanup ownership is accepted enters cleanup. There is no
execution-resume path. Cleanup-only recovery is a separate invocation.

## Boundaries and retained identities

1. The operator requires an original factory-acquired private run lease and
   durably accepted lifecycle intent before provisioning. Hold the lease through
   cleanup and final persistence. Preserve the existing private state namespace;
   public history/results are not operator state.
2. Freshly adopt the original baseline, guarded initialization artifacts and
   startup artifacts. Obtain their required fresh image-access capabilities.
   Both new artifact paths must bind the same original baseline and reused
   support/driver images. The guarded initializer and startup artifacts must
   additionally agree on the exact compiled deterministic dataset identity;
   do not invent a dataset attestation on the original baseline. Never coerce
   one artifact, plan, stage or capability type into another.
3. A new immutable startup pin binds the support and initialization pin
   fingerprints, startup artifact/source/config/dataset identities and distinct
   initialization/startup UUIDs. Independently revalidate all pin files on every
   private state load/save. An attempt UUID is not completion or dispatch proof.
4. Reuse the lower-level guarded initialization controller within the new
   lifecycle, not its cleanup-owning operator. It completes one initializer and
   the separate global read-only verifier in this fresh leased invocation.
   Before startup, independently observe the exact completed Pod UIDs, native
   image identities, exit-zero results and bounded run/dataset/mode-specific
   output. Saved success JSON or an old verifier is insufficient.
5. Bind fresh provider account/create identities, CA, kube-system and workload
   Namespace UIDs, role Node identities, provider database volume, filesystem
   UUID, all eight PVC/PV/database Pod identities, support container executions,
   and support Service/EndpointSlice identities. Reobserve continuity before
   create, before each release, and before final startup success.

The existing immutable startup plan/stage retain their false authority and
qualification fields. Live observations use separate versioned schemas; they
never mutate or promote an offline foundation object.

## Complete live inventory and admission checks

The startup observer accepts only exact startup plan/stage types. Inventory
coverage is versioned and explicit, not a claim to discover every possible
Kubernetes object. Retain complete raw results and account for every object
within that coverage. Collect Nodes, bound PersistentVolumes, the workload and
kube-system Namespace identities, and these workload-namespace collections:
Deployments, ReplicaSets, Pods, Services, EndpointSlices, PVCs, NetworkPolicies,
ServiceAccounts, Secrets and ConfigMaps. Also inspect Jobs, CronJobs,
DaemonSets, StatefulSets, ReplicationControllers, PodTemplates, HPAs, Roles,
RoleBindings, ResourceQuotas and LimitRanges; require absence except exact
system defaults explicitly audited for the pinned cluster profile. Required
API discovery, collection reads and pagination/completeness checks must all
succeed; do not treat an inaccessible collection as empty. Admission discovery
has the separate complete served-version coverage described below.

Partition only exact known initialization objects, exact startup Pods, and the
original static/support graph. Unknown, duplicate, missing or crossed objects
within the audited coverage prevent startup. Do not silently project out
objects or claim absence of unrelated API kinds outside this coverage.

Use unchanged lower-level support validation for the original graph. Do not
reuse the old full initialization observer unchanged: it rejects application
Pods and requires empty application EndpointSlices. Validate every real
EndpointSlice in the new boundary, including populated startup endpoints;
never manufacture empty slices or filter arbitrary application labels to make
an old validator pass. Preserve the ten original application Deployments at
zero replicas and prohibit additional controllers or resources.

Before any release, pin all ten admitted Pod names, UIDs, metadata and exact
specs, retaining their initially observed resource versions as evidence.
Immediately before each atomic patch, reobserve its current resourceVersion
and unchanged identity/spec; status-only version changes are not replacement
or permission to accept spec changes. After release, admit only enumerated
controller-owned transitions such as binding nodeName; arbitrary drift fails.
Admit only individually audited API defaults. Reject unexpected ServiceAccount
credentials, pull secrets, projected token/other
volumes, init/ephemeral containers, probes, lifecycle hooks, environment,
commands, image, ports, resources, placement or security changes. Keep the
existing rootless/read-only, six identity fields plus `TLS=false`, Downward API,
`Never` restart, scheduling gate and 900-second qualification deadline.

Prove complete discovery and successful empty inventories for every served
dynamic admission mutator collection immediately before create and release.
Denied, incomplete or unknown discovery fails closed. This is not protection
against a concurrent privileged administrator; record that limitation.

Observed execution must bind Pod UID/private IPv4/node, exact container ID,
zero restarts, native platform manifest and config digest. Retain raw Kubernetes
image IDs and node-local CRI evidence mapping the exact running container to
those immutable identities. Do not relax existing image-ID validators or
substitute compressed-byte access for runtime image proof.

For each application Service, verify EndpointSlice manager and owner Service
UID, target Pod UID/name/node/address, exact ports and conditions, with no extra
or duplicate endpoints. Continue validating support endpoints independently.
Kubernetes Ready/Running or a published endpoint is not semantic readiness.

## Durable one-shot startup and observation

Create a separate append-only startup journal; leave the initializer's six
events and two run-wide latches unchanged. Journal records are private bounded
canonical JSON with exact run/attempt/plan/stage identity, ordered hashes,
single-link/no-follow checks, exclusive creation, file fsync and directory
fsync. Partial or malformed records are retained, never repaired or removed.

Required ordering is:

1. Fresh prerequisites and an observation proving no startup Pods or stale
   application registrations exist.
2. Persist an exclusive run-wide create latch and create-intent event binding
   the complete manifest before one create request. A partial List create or
   lost acknowledgement is cleanup-only; do not reconcile it into a retry.
3. Observe and durably pin the complete set of ten exact gated admitted Pods.
   No release occurs unless the complete set is intact and blocked.
4. Release deterministically: `attractions`, `geo`, `profile`, `rate`,
   `recommendation`, `reservation`, `review`, `user`, then `search`, then
   `frontend`. The first eight have no application-service dependencies;
   search follows geo/rate, and frontend follows its backend dependencies.
5. Before each release, revalidate lease, immutable inputs, callback-owned
   evidence, fresh cloud/storage/cluster/support/verifier continuity and
   admission-mutator absence. Exclusively fsync a run-wide component latch
   and dispatch intent binding its exact admitted Pod UID/spec and image-access
   fingerprint. A failed or uncertain persistence response permits no mutation.
6. Issue at most one conditional gate-removal request per component. Test UID,
   resourceVersion, name, namespace, labels, annotations and complete admitted
   spec; remove only the scheduling gate. Do not scale, apply, upsert or retry.
7. Bounded read-only observations may wait for exact execution and identity
   evidence. A terminal failure, timeout, restart, replacement, unexpected
   registration or uncertain mutation stops all remaining releases.

Before starting a component, require the expected registrations of already
started backend components and no stale, duplicate or unknown application
registrations. After each backend starts, inspect complete agent and catalog
state. Require exactly one expected entry with service `srv-{component}`, ID
`hotel-{run}-{startup_attempt}-{component}-{pod_uid}`, exact Pod IP/port and
metadata `{run_id,startup_attempt_id,pod_uid,contract_sha256,component}`.
The nine backend entries must agree across the agent/catalog and pinned Pods.
The two API surfaces are not interchangeable: separately allow the intrinsic
`consul` server catalog entry only when its server/node/datacenter and address
are bound to the exact pinned Consul execution and built-in server port 8300.
The pinned-image observation must confirm that intrinsic identity before it is
allowed; documentation examples are not live proof.
Reject every other extra entry; do not ignore arbitrary non-`srv-*` names or
require byte-equality between complete agent and catalog responses. The
[official catalog API](https://developer.hashicorp.com/consul/api-docs/catalog)
documents these distinct surfaces and the intrinsic server service. The
frontend has no registration. Do not deregister, overwrite or repair.

The source registration guard is a bounded read-before-write check, not an
atomic distributed claim or continuously valid health proof. Retained outcomes
describe observations at stated times, not exactly-once remote execution or
continued service availability.

The new controller uses one 900-second startup window from the first create
request through final observation; polls and components cannot reset it.
Each read-only observation is additionally bounded to 60 seconds, and each API
or Consul request to 15 seconds or the shorter remaining deadline. Reject raw
observations over 4 MiB and startup journal records over 1 MiB. Keep these
qualification bounds as versioned constants, not caller-supplied overrides;
they are not benchmark durations or performance claims.

## Cleanup, interruption and recovery

The new operator always attempts whole-run destruction after provisioning,
including after successful startup. Preserve initialization and startup
evidence separately before graph clearing. Archive/startup-history persistence
failure prevents overall success but must not suppress otherwise safe cleanup
when exact deletion and absence-check targets were already durably published,
or can still be durably published. Failure of that mandatory pre-delete
cleanup-source publication must prevent deletion and retain a cleanup-failed
outcome; do not destroy targets that cannot be recovered for absence checks.

Add an explicit startup lifecycle selector to provider cleanup. Validate its
support/initialization/startup pins and independently retained cleanup ownership
projection, including run/provider/account, infrastructure plan fingerprint,
persisted provider allocation identities and matching audit envelopes when
provisioning reached the audit stage. Partial provisioning still requires the
existing strict provider create-identity contract; do not fabricate readiness.
This projection excludes startup dispatch events, service output and success
claims. Do not require full startup-history deserialization to perform cleanup,
and do not reconstruct ownership from corrupt startup history. Never fall back
to the legacy support selector. Preserve exact deletion targets durably before
the first provider delete, even if the provider
later clears its mutable graph. Carry forward provider-specific strict identity
checks, including GCP numeric IDs/self-links and Azure scoped identities.

Only delete the exact owned provider graph. Unknown Kubernetes objects block
startup; they are not deleted through labels, prefixes or namespace sweeps.
Whole-run provider teardown may still proceed when that ephemeral infrastructure
is independently proven owned. Unknown or conflicting cloud deletion targets
require refusal and retained recovery evidence, not broadened deletion scope.

Any reopened run is cleanup-only, even if it has no dispatch latch. Recovery
must not adopt sources, access registries, establish an SSH relay, contact
Kubernetes, initialize, release gates or restart applications. It loads only
the original immutable provider/ownership inputs, acquires a fresh lease and
performs strict provider cleanup and independent absence checks. Corrupt startup
history never grants execution rights; cleanup requires independently intact
ownership pins and provider evidence, otherwise it refuses without repair.

Defer repeated interrupt signals during cleanup using the established operator
pattern. SIGINT/SIGTERM, hard-exit restart, lost responses, or callback mutation
cannot clear dispatch latches or create another attempt on the same resources.
An execution failure remains a nonzero outcome even if teardown succeeds.
Cleanup-only may return zero only when strict destruction and independent
absence are proven; it never rewrites the original execution outcome.

## Outcomes and release boundary

Retain separate startup execution, service-identity and cleanup observations,
both attempt IDs, immutable fingerprints, admitted Pod/container/CRI identities,
endpoint/catalog evidence, journal/latch state and sanitized failures. Bound
all evidence; never retain credentials, kubeconfigs, private SSH keys, tokens
or signed registry URLs in public or historical output.

`startup_execution_observed` and `service_identity_observed` may become true
only for the exact complete live observation. `semantic_readiness_verified`,
runtime/cloud/measurement/public-release qualification and public dispatch stay
false. HTTP 200, static frontend content, TCP success, catalog membership and
Kubernetes conditions cannot promote those gates. Functional query,
authentication/reservation correctness and measurement are subsequent work.

## Validation and delivery

Automated tests must cover successful sequencing plus failure injection at
create intent, partial writes/fsync, persistence, create responses, and each of
the ten component intents/releases/responses. Include conditional-patch
failure, response applied but lost, stale/crossed catalog and endpoint entries,
admission mutation/discovery denial, CRI/image drift, restart/replacement,
cluster/storage/support/verifier drift, copied/forked/lost leases, callback
mutation, signal/hard-exit recovery, malformed history and cleanup-source or
absence-proof failures. Assert create at most once, each release at most once,
no later release after failure, and zero registry/SSH/startup calls in recovery.

Preserve all existing guarded/support/Social Network behavior. Run focused
security tests, the full repository checks, source/Go/Lua CI gates and
independent code review before a review-only PR. Do not auto-merge.

After implementation tests and independent code review pass, live qualification
starts with one fresh GCP Arm success attempt and a separate injected
gate-release acknowledgement-loss attempt. Require exact retained evidence,
non-replay and independent cloud absence after each. Any registry-egress helper
is separately owned and cleaned up without changing TLS/proxy trust or copying
cloud credentials. No all-cloud claim follows from these attempts; AWS, OCI
and Azure require their own later live observations before such claims.

The implementation plan must keep observer, journal, controller, operator/state
and provider cleanup integration as explicit testable units. This is one
private lifecycle feature, not a public release. Written-spec approval is
required before writing the implementation plan; plan review and execution
method selection precede product-code changes or deployment.
