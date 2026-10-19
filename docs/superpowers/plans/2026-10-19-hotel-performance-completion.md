# Hotel Performance Benchmark Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a selectable, cross-cloud distributed Hotel performance benchmark with correct application behavior, one uninterrupted measurement, trustworthy comparisons, and exact resource teardown.

**Architecture:** Compose the existing support and lower-level guarded initializer with a separate one-shot startup controller and Hotel-specific correctness/measurement executor. Keep private candidate contracts immutable; expose a separately qualified public Hotel release through an explicit lifecycle adapter. A terminal failure permanently invalidates the whole Hotel run, while independent ownership evidence permits resource destruction without execution recovery.

**Tech Stack:** Python 3.10–3.14, unittest, existing FastAPI/Pydantic app, vanilla JavaScript, pinned K3s, native Go Hotel artifacts, amd64 wrk2/LuaJIT/LuaSocket driver, existing AWS/OCI/GCP/Azure adapters and GitHub Actions.

**Spec:** [Approved Hotel performance-only design](../specs/2026-10-19-hotel-live-startup-design.md).

Status: proposed implementation plan; user review and execution-method selection pending. This document authorizes no code execution or cloud provisioning by itself.

## Global Constraints

- “A runner or application failure fails the entire run.” No execution recovery or resume, deliberate application failure, resilience score, or time-to-recovery measurement.
- “Cleanup success never changes execution failure into benchmark success.” Failed/interrupted/cancelled attempts retain diagnostic output only; completed rows do not override run failure.
- “A new benchmark requires a new run identity, fresh infrastructure and fresh dataset.” Never replay initialization, traffic or uncertain mutations on an old invocation.
- “Closing or reloading the UI does not itself fail an otherwise healthy runner.” Do not equate browser connectivity with worker health.
- Keep `distributed_tiered_v1` / `k3s_v1`, existing five-node role placement, and pinned `v1.36.4+k3s1`; do not upgrade Kubernetes in this work.
- Application architectures are `x86_64` / `aarch64`; support and load-generator roles remain fixed x86. The startup namespace is `deathstarbench-hotel`.
- Preserve existing support-only, guarded initialization, immutable startup foundation, Social Network and Media contracts. No mutable module-global “current workload”.
- The startup window is 900 seconds total; each observation is at most 60 seconds; each API/Consul request is at most 15 seconds or the shorter remaining deadline. Raw startup evidence is at most 4 MiB; a startup journal record is at most 1 MiB.
- Gate-removal order: `attractions`, `geo`, `profile`, `rate`, `recommendation`, `reservation`, `review`, `user`, `search`, `frontend`.
- Use the original acquired private execution lease from intent through teardown; public history cannot restore a capability or authorize dispatch.
- Preserve rootless/read-only application containers, `Never` restart, seven exact environment entries including `TLS=false`, no service-account credentials and no added probes/sidecars.
- Keep exact-owned deletion and independent absence checks. Never use labels/prefixes/namespace sweeps to infer deletion authority; missing/conflicting ownership refuses deletion.
- Never retain credentials, kubeconfigs, private keys, tokens or signed registry URLs in public/historical evidence. Keep registry helpers separately owned and cleaned up; do not change TLS/proxy trust.
- Use simulated offline failures to test fail-fast handling, not a dedicated live failure/recovery matrix. Normal network isolation probes happen outside timed traffic.
- Every delivery PR is review-only: never merge or enable auto-merge. Leave unrelated user changes, including `app/.DS_Store`, untouched.

## Review Focus

1. Completed-looking Hotel rows followed by runner failure must remain ineligible after save/reopen/report fallback; Task 11 tests the complete round trip.
2. Valid provider ownership with corrupt execution history must still permit exact teardown, while missing ownership must refuse; Task 6 tests both cases.
3. Admission defaults, EndpointSlice updates and node-local CRI can disagree without obvious Pod failure; Tasks 1–2 test allowed status churn versus identity/spec drift.
4. HTTP 200 can mean failed login or rejected reservation, and warmup consumes room inventory; Tasks 8–10 test body classification, persisted effects and retained dataset epochs.
5. Later components can exhaust the shared deadline, and applied mutations can lose their responses; Task 4 tests a single shrinking deadline and permanent failure without redispatch.

## Delivery Shape and Decisions

This is one Hotel completion plan, not a promise that startup alone completes Hotel. Reviewable milestones are A (Tasks 1–7: private live startup), B (8–10: functional performance execution), C (11–13: results and public integration behind closed gates), and D (14: normal live validation and release). Each task has its own red/green cycle and commit; a milestone can be proposed as a PR without opening public execution early.

- Keep the existing upstream mixed-workload distribution: 60% search, 39% recommendation, 0.5% login and 0.5% reservation; deterministic per-worker seed base `20261008`, exponential interarrival distribution. Bind both original and prepared script hashes.
- Reuse `DeathStarBenchOptions` bounds/defaults: warmup 30 seconds (`0..600`), duration 60 (`10..1800`), threads 4 (`1..64`), connections 64 (`1..4096`), rate 100 (`1..100000`). Require connections and rate divisible by threads. Zero warmup is explicitly recorded as skipped, not run for one second.
- The existing private startup stage's `activeDeadlineSeconds=900` cannot support a maximum-length performance run. Preserve that v1 stage unchanged. Task 10 adds a separately versioned performance runtime/stage, with the application deadline fixed before creation at `900 + warmup_seconds + duration_seconds + 540` seconds; the startup controller still has only its original 900-second startup window. The extra 540 seconds bounds correctness, epoch inspection and post-check overhead; exceeding any phase budget fails, never extends a deadline or patches a running Pod.
- Retain correctness-probe and warmup dataset effects. Record baseline, post-correctness, post-warmup and post-measurement epochs; never reset/repair between them. Changing epoch semantics creates a new measurement revision.
- Throughput means completed HTTP responses per measured second, not durable successful reservations. Report HTTP, transport, uncompleted, recognized business rejection and unrecognized-body counts separately. `wrk2.response()` lacks endpoint correlation: never promote candidate body checks into per-endpoint transaction-success claims.
- A failed control/SSH operation or application execution failure invalidates the run. Ordinary per-request errors are measured outcomes, not an automatic runner failure; no hidden threshold, retry or discarded slow interval. A failed correctness/state invariant or inconsistent metrics transcript is terminal.
- Results remain pending until execution, post-checks, persistence and exact teardown/absence all succeed. Hotel attempts owned teardown even on success. The initial Hotel public release does not offer infrastructure retention or resume; Social Network's existing options remain unchanged.
- The candidate `HOTEL_RESERVATION_PROFILE` stays unreleased and unchanged. Add a distinct qualified public profile/bundle resolver; do not flip the candidate flag and break historical adoption.
- Normal live validation covers each provider/application-architecture pair advertised by the release. Unavailable capacity/quota is an unqualified cell, never an inferred pass.

## File and Interface Map

New modules are intentionally narrow; do not turn `app/main.py` into a second Hotel executor.

| Unit | New files | Responsibility |
| --- | --- | --- |
| Startup evidence | `app/deathstarbench_hotel_startup_identity.py`, `app/deathstarbench_hotel_startup_k3s.py` | Exact Pods/endpoints/Consul/CRI and bounded collection |
| Startup execution | `app/deathstarbench_hotel_startup_journal.py`, `app/deathstarbench_hotel_startup_controller.py` | Durable intent and one-shot dispatch |
| Ownership/lifecycle | `app/deathstarbench_hotel_startup_ownership.py`, `app/deathstarbench_hotel_startup_operator.py` | Independently validated deletion authority and cleanup-owning startup |
| Private integration | `scripts/deathstarbench_hotel_startup_operator_state.py`, `scripts/qualify_deathstarbench_hotel_startup.py` | Pin/state persistence and fresh/destroy-only CLI |
| Functional execution | `app/deathstarbench_hotel_correctness.py`, `app/deathstarbench_hotel_execution_stage.py`, `scripts/prepare_deathstarbench_hotel_verifier.py`, `scripts/assets/deathstarbench/hotel-performance-v1/` | Versioned read-only epoch inspection and bounded semantic probes |
| Driver/artifacts | `scripts/prepare_deathstarbench_hotel_performance_driver.py`, `scripts/assets/deathstarbench/driver/hotel-performance-*`, `app/deathstarbench_hotel_performance_artifacts.py`, `app/deathstarbench_hotel_performance_image_access.py` | Distinct metric recipe, helper/driver adoption and fresh byte-access capability |
| Measurement | `app/deathstarbench_hotel_performance_runtime.py`, `app/deathstarbench_hotel_performance_journal.py`, `app/deathstarbench_hotel_measurement.py` | Correctness, warmup, one contiguous measurement, continuity |
| Validity/results | `app/deathstarbench_hotel_validity.py` | Sticky whole-run validity and chart eligibility |
| Public release | `app/deathstarbench_hotel_release.py`, `app/deathstarbench_hotel_public.py` | Separate release profile, snapshots and public-to-private ownership adapter |

Each new Python unit receives a correspondingly named `tests/test_*.py` file. Existing files modified by integration tasks are named below. Existing source pipelines remain pinned; new artifact workflows use their repository patterns rather than rewriting old receipts.

### Task 1: Exact startup Pod, endpoint, Consul and CRI identity

**Files:** Create `app/deathstarbench_hotel_startup_identity.py`, `tests/test_deathstarbench_hotel_startup_identity.py`. Reuse `StartupRuntimeFixtureMixin` from `tests/test_deathstarbench_hotel_startup_runtime.py` and support fixtures, without forging factory proofs.

**Interfaces:** Consume `StartupHotelRuntimePlan`, exact approved stage Pod dictionaries and role-node map. Produce:

```python
validate_startup_hotel_admitted_pod(pod: dict, approved: dict, role_nodes: dict[str, str], *, pinned: dict | None = None, released: bool = False) -> dict
validate_startup_hotel_endpoints(slices: list[dict], services: list[dict], support_pods: list[dict], startup_pods: dict[str, dict]) -> dict
validate_startup_hotel_cri_evidence(evidence: dict, plan: StartupHotelRuntimePlan, pods: dict[str, dict]) -> dict
validate_startup_hotel_consul_identity(evidence: dict, plan: StartupHotelRuntimePlan, pods: dict[str, dict], started_components: tuple[str, ...], consul_execution: dict) -> dict
```

- [ ] Write `StartupIdentityTests.test_status_only_version_change_preserves_pinned_spec`: assert a fresh resourceVersion is accepted with unchanged UID/spec, original version remains retained, injected env/volume/sidecar/token/image/placement fails, and only enumerated post-release `nodeName` binding is accepted. Add `test_endpoint_owner_target_and_native_cri_must_agree` and `test_consul_intrinsic_entry_is_not_a_blanket_exception`; reject extra/duplicate/stale entries and require zero frontend registration.
  Core assertions: `self.assertEqual(observed['spec'], pinned['spec'])`; `self.assertEqual(observed['pod_uid'], pinned['pod_uid'])`; `self.assertEqual(len(backend_registrations), 9)`.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_startup_identity -v`; expect missing-module/API failure before implementation.
- [ ] Implement the four functions with `StartupHotelIdentityError(ValueError)`. Pin complete admitted metadata/spec, exact Pod UID/IP/node/container execution and manifest/config digest; keep the existing strict Pod image-ID matcher unchanged. Nine IDs use `hotel-{run}-{startup_attempt}-{component}-{pod_uid}`; separately bind the intrinsic `consul` server entry/port 8300.
- [ ] Rerun the new suite plus `python3 -m unittest tests.test_deathstarbench_hotel_live_k3s tests.test_deathstarbench_hotel_startup_stage -v`; expect all pass, with old validators still rejecting application Pods.
- [ ] Commit only this unit and tests: `git commit -m "feat: validate exact Hotel startup execution identities"`.

### Task 2: Bounded complete startup observation and node-local CRI collection

**Files:** Create `app/deathstarbench_hotel_startup_k3s.py`, `tests/test_deathstarbench_hotel_startup_k3s.py`. Do not generalize the old full initialization observer.

**Interfaces:** Consume Task 1 functions and existing `GuardedHotelRuntimePlan`, `GuardedHotelInitializationStage`, `StartupHotelRuntimePlan`, `StartupHotelStartupStage`. Produce:

```python
startup_hotel_observation_command(plan, stage, initialization_plan, initialization_stage, role_nodes: dict[str, str], expected_ca_sha256: str, *, deadline_remaining_seconds: float) -> str
parse_startup_hotel_kubernetes_observation(output: str | bytes, plan, stage, initialization_plan, initialization_stage, role_nodes: dict[str, str], expected_ca_sha256: str, *, expected_identity: dict | None = None) -> dict
startup_hotel_cri_observation_command(plan, pods: dict[str, dict], *, deadline_remaining_seconds: float) -> str
validate_startup_hotel_live_observation(kubernetes_observation: dict, cri_output: str | bytes, plan, *, started_components: tuple[str, ...]) -> dict
```

- [ ] Write `StartupObservationTests.test_every_required_collection_has_discovery_and_complete_list_evidence`: include PodTemplates/ResourceQuotas/LimitRanges, all Nodes, both namespace identities, all spec-listed collections, per-list pagination proof and every served mutator API version. Assert denied/missing/truncated/duplicate collections and >4-MiB raw output fail, rather than becoming empty inventories.
- [ ] Add `test_real_application_endpoints_are_validated_not_filtered` and `test_cri_is_collected_on_application_node_and_rejoined`: changed container identity between Kubernetes/CRI/final Kubernetes reads fails; a Kubernetes-only intermediate observation cannot claim complete execution proof.
  Core assertions: `self.assertFalse(intermediate['startup_execution_observed'])`; `self.assertEqual(cri_host, role_nodes['application'])`; deny incomplete collection fixtures with `self.assertRaises(ValueError)`.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_startup_k3s -v`; expect failure.
- [ ] Implement bounded command/parse/join functions, reusing only strict lower-level support/admission helpers. Partition exact initializer/verifier/startup objects; pass the genuinely original support graph to unchanged support validation. Collect complete agent/catalog evidence from the pinned Consul execution under the same request/output bounds; validate it through Task 1, not an unbound URL. Never fabricate empty application slices.
- [ ] Rerun that command and the Task 1 suite; expect pass. Assert command timeouts are `min(15, remaining)` per request and `min(60, remaining)` per observation, and CRI command targets the application host rather than control.
- [ ] Commit: `git commit -m "feat: collect bounded Hotel startup observations"`.

### Task 3: Separate append-only startup journal

**Files:** Create `app/deathstarbench_hotel_startup_journal.py`, `tests/test_deathstarbench_hotel_startup_journal.py`. Leave initializer `EVENTS`, `LATCHES`, six events and two latches unchanged.

**Interfaces:** `StartupHotelStartupJournal(plan, stage, *, lease)` exposes `record_create_intent(observation: dict) -> dict`, `record_admitted_pods(observation: dict) -> dict`, `record_dispatch_intent(component: str, observation: dict, *, image_access) -> dict`, `record_component_observation(component: str, observation: dict) -> dict`, `record_final_observation(observation: dict) -> dict`, `metadata() -> dict`.

- [ ] Write `StartupJournalTests.test_create_and_each_component_have_exclusive_durable_intent`: one create latch and ten component latches, exact release order, UID/spec/access fingerprints, maximum 1-MiB records, ordered hash chain. Assert out-of-order/duplicate dispatch and any reopened invocation refuse, including an invocation with only a created event.
- [ ] Add `test_partial_fsync_or_unsafe_file_never_grants_dispatch`: simulate partial write, file/directory fsync failure, orphan latch, symlink/hardlink/FIFO/wrong mode, copied/forked/released lease. Retain partial evidence without repair or deletion.
  Core assertions: `self.assertEqual(len(component_latches), 10)`; `self.assertEqual(len(create_latches), 1)`; second dispatch uses `self.assertRaises(ValueError)` and the damaged record bytes stay unchanged.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_startup_journal -v`; expect failure.
- [ ] Implement exclusive no-follow, single-link private publication and canonical bounded JSON using the existing initializer journal's mechanics as a pattern, not its event schema.
- [ ] Rerun the new suite and `python3 -m unittest tests.test_deathstarbench_hotel_initialization_journal tests.test_deathstarbench_hotel_initialization_journal_review -v`; expect pass.
- [ ] Commit: `git commit -m "feat: journal one-shot Hotel startup intent"`.

### Task 4: Fail-fast one-shot application startup controller

**Files:** Create `app/deathstarbench_hotel_startup_controller.py`, `tests/test_deathstarbench_hotel_startup_controller.py`.

**Interfaces:** Produce `prepare_startup_hotel_applications(job, startup_artifacts, *, baseline_artifacts, initialization_plan, initialization_stage, lease, execute, persist, image_access, fresh_audit, expected_cloud_identity: dict, startup_attempt_id: str, emit=None, monotonic=time.monotonic) -> dict`. `execute(host, command)` retains the existing SSH callback shape; no injected live-failure/checkpoint API is exposed.

- [ ] Write `StartupControllerTests.test_fresh_initializer_and_all_ten_blocked_pods_precede_first_release`: reuse the lower-level initializer's real outcomes as evidence, then independently check exact completed initializer/verifier Pod UIDs/images/exit-zero/run/dataset logs; one List create, pin all ten Pods before one release, observe nine backend identities and frontend endpoints.
- [ ] Write `test_shared_deadline_and_lost_response_are_terminal` and `test_each_boundary_failure_stops_later_dispatch`: table-test all ten intents/releases/responses, persistence, source/capability/lease/cloud/storage/support/namespace drift. Assert create count <=1, each release <=1, no later releases and elapsed budget never resets per component. Simulate these offline only.
  Core assertions: `self.assertLessEqual(create_calls, 1)`; `self.assertLessEqual(release_calls[component], 1)`; `self.assertEqual(released_after_failure, [])`; `self.assertLessEqual(startup_elapsed_seconds, 900)`.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_startup_controller -v`; expect failure.
- [ ] Implement callback-before/after binding guards, original factory capability checks and a single monotonic deadline. Conditional JSON Patch tests current resourceVersion, UID, name/namespace, all admitted metadata/spec before only removing the scheduling gate. No retry/apply/scale/upsert/repair.
- [ ] Rerun Tasks 1–4 suites plus `tests.test_deathstarbench_hotel_live_controller`; expect pass. Success sets only `startup_execution_observed` / `service_identity_observed`; semantic/measurement/public gates stay closed.
- [ ] Commit: `git commit -m "feat: start Hotel services once with terminal failures"`.

### Task 5: Immutable startup ownership and cleanup-owning lifecycle

**Files:** Create `app/deathstarbench_hotel_startup_ownership.py`, `app/deathstarbench_hotel_startup_operator.py` and matching tests.

**Interfaces:** Ownership exposes `make_hotel_startup_cleanup_ownership(job: dict, *, support_pin: dict, initialization_pin: dict, startup_pin: dict) -> dict` and `validate_hotel_startup_cleanup_ownership(value: dict, *, run_id: str, provider: str) -> dict`. Operator exposes `make_hotel_startup_pin(run_id: str, support_pin: dict, initialization_pin: dict, artifacts, *, startup_attempt_id: str) -> dict`, `validate_hotel_startup_pin(value: dict, support_pin: dict, initialization_pin: dict, artifacts=None) -> dict`, and `run_hotel_startup_operator(job, *, lease, pin, initialization_pin, startup_pin, persist, provision, fresh_audit, prepare_runtime, support_image_access, stage, initialize, startup, destroy_and_prove, artifacts, guarded_artifacts, startup_artifacts, cleanup_signal_context=None) -> HotelStartupOperatorOutcome`.

- [ ] Write `StartupOperatorTests.test_one_original_execution_lease_covers_every_phase_and_teardown`: exact order intent/provision/support/initialize/startup/destroy/absence; initializer callback is `prepare_guarded_hotel_initialization`, not its cleanup-owning operator. Assert all three pins and distinct nonzero attempt UUIDs remain bound.
- [ ] Write `test_failure_is_sticky_even_if_destroy_succeeds` and `test_archive_failure_does_not_suppress_independently_owned_teardown`; unsuccessful evidence publication cannot produce success. Unknown ownership or mandatory pre-delete target-publication failure must block deletion.
  Core assertions: `self.assertEqual(outcome.execution_outcome, 'failed')`; `self.assertEqual(outcome.cleanup_outcome, 'completed')`; `self.assertEqual(startup_calls_after_failure, 0)`.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_startup_ownership tests.test_deathstarbench_hotel_startup_operator -v`; expect failure.
- [ ] Implement `HotelStartupOperatorOutcome` with independent `execution_outcome`, `cleanup_outcome`, `error` and `cleanup_error`. Define an ownership-only schema `hotel-startup-cleanup-ownership-v1` binding run/provider/account, plan and three pin fingerprints, persisted create/allocation identities and audits when present. Exclude service logs/dispatch history/success claims from deletion authority.
- [ ] After the lower-level initializer returns its retained outcome dictionary, rederive `GuardedHotelRuntimePlan` / `GuardedHotelInitializationStage` from the original guarded artifacts, same support plan/stage and pinned initialization UUID for Task 4. Do not expect the existing initializer to return those objects or infer completion from rederivation; Task 4 still independently observes completion.
- [ ] Rerun new suites and initialization-operator regressions; expect pass. Keep original startup artifact source/config/dataset proof and all old immutable gates unchanged.
- [ ] Commit: `git commit -m "feat: own Hotel startup lifecycle and exact teardown"`.

### Task 6: Private state and provider destruction integration

**Files:** Create `scripts/deathstarbench_hotel_startup_operator_state.py`, `tests/test_deathstarbench_hotel_startup_operator_state.py`; modify `scripts/deathstarbench_candidate_provider.py`, `tests/test_deathstarbench_hotel_candidate_provider.py`.

**Interfaces:** `HotelStartupStateStore(HotelInitializationStateStore)` adds `publish_startup_pin(pin: dict, lease) -> None`, `startup_pin(run_id: str, lease) -> dict`, `publish_cleanup_ownership(run_id: str, ownership: dict, lease) -> None`, `load_for_destroy(run_id: str, lease) -> dict`; normal `save/load` recheck all three pin files. `CandidateCloud` gains only explicit `operator_journal_key='hotel_startup_operator'` handling and consumes Task 5 ownership for teardown.

- [ ] Write `StartupStateTests.test_normal_load_checks_three_pins_without_restoring_capabilities` and `test_destroy_load_ignores_corrupt_startup_history_not_corrupt_ownership`. Assert source, private byte-access capability and executable callbacks are never deserialized into authority. Immutable files are exclusive, no-follow and bounded.
- [ ] Extend provider tests with `test_startup_destroy_publishes_exact_targets_before_delete`, `test_missing_or_crossed_ownership_refuses`, `test_partial_provision_uses_create_identity_not_fake_readiness`. Assert graph clearing does not lose original absence-check targets; decoy legacy selector is never used.
  Core assertions: `self.assertLess(events.index('publish-cleanup-source'), events.index('delete'))`; `self.assertEqual(absence_targets, original_targets)`; `self.assertEqual(delete_calls_with_invalid_ownership, 0)`.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_startup_operator_state tests.test_deathstarbench_hotel_candidate_provider -v`; expect failure.
- [ ] Implement distinct startup cleanup-source validation rather than opportunistically adding fields to old exact-key schemas. Preserve GCP numeric IDs/scoped self-links and Azure exact scope/ID/tag checks, AWS/OCI strict graph/tombstone checks, and strict absence auditing after deletion.
- [ ] Rerun new suites and `python3 -m unittest tests.test_deathstarbench_hotel_initialization_operator tests.test_deathstarbench_cleanup_review -v`; expect pass. A later destroy operation may succeed but cannot restart or reclassify the Hotel execution.
- [ ] Commit: `git commit -m "feat: persist independent Hotel cleanup ownership"`.

### Task 7: Fresh-only private startup CLI and milestone A verification

**Files:** Create `scripts/qualify_deathstarbench_hotel_startup.py`, `tests/test_deathstarbench_hotel_startup_cli.py`; update `docs/deathstarbench-workload-expansion.md`.

**Interfaces:** `main(argv=None) -> int` has fresh-run mode and explicit `--destroy-only --run-id ID`. Reuse existing provider/plan/source argument patterns and registry-egress transport. Fresh mode accepts original/guarded/startup lock/publication/preparation inputs; destroy-only rejects all artifact/registry/SSH overrides. No `--resume`, `--recover` or `--interrupt-after` option.

- [ ] Write `StartupCliTests.test_fresh_adoption_and_same_baseline_bind_all_artifacts`, `test_existing_run_cannot_execute`, `test_destroy_only_has_zero_registry_ssh_kubernetes_calls`, `test_destroy_preserves_failed_execution`. Assert old malformed inputs fail before provision and no stored receipt recreates an access capability.
  Core assertions: `self.assertNotIn('--resume', help_text)`; `self.assertEqual((registry_calls, ssh_calls, startup_calls), (0, 0, 0))` in destroy-only mode; `self.assertEqual(saved_execution_outcome, 'failed')` after successful destruction.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_startup_cli -v`; expect failure.
- [ ] Wire original adoption/access factories and Tasks 4–6 under the `.candidate-support` store's acquired execution lease. Retain independent startup/init observations; catch/defer cleanup signals using existing patterns.
- [ ] Rerun the suite, startup-focused suites, existing registry-egress CLI suites and `PYTHON_BIN=python3 scripts/check.sh`; require no failures and record actual skip reasons. Native Go/Lua source tests remain required in CI; local inability to run them is not a pass.
- [ ] Obtain independent code review; fix/retest findings, then create a review-only milestone A PR with exact test evidence. Do not merge. Only after code/CI review, a normal uninterrupted GCP Arm startup attempt may validate actual startup and exact absence; no deliberately lost acknowledgement attempt.
- [ ] Commit documentation/evidence only after observing actual outcomes: `git commit -m "feat: expose private fail-fast Hotel startup qualification"`.

### Task 8: Functional correctness and retained dataset epochs

**Files:** Create `app/deathstarbench_hotel_correctness.py`, `app/deathstarbench_hotel_execution_stage.py`, corresponding tests, `scripts/prepare_deathstarbench_hotel_verifier.py`, `tests/test_deathstarbench_hotel_verifier.py`, `scripts/assets/deathstarbench/hotel-performance-v1/qualification/{contract,checks,epoch}.go`, matching `_test.go` files and `cmd/hotel-qualify/main.go`. Add `.github/workflows/deathstarbench-hotel-performance-verifier.yml`, modify `.github/workflows/ci.yml` using existing pinned artifact patterns; keep all old image recipes/locks immutable.

**Interfaces:** `HotelCorrectnessContract` frozen fields bind schema `hotel-correctness-v1`, baseline dataset/source fingerprint, probe script/fixture fingerprints and epoch policy `hotel-retain-probes-and-warmup-v1`. `HotelCorrectnessObservation` binds run/attempt/helper Pod execution plus `verified: bool`, request results, durable effects and epoch fingerprint. Produce `run_hotel_correctness_checks(context: dict, contract: HotelCorrectnessContract, *, execute, observe, persist) -> HotelCorrectnessObservation` and `validate_hotel_dataset_epoch(value: dict, contract: HotelCorrectnessContract, *, phase: str, previous: dict | None) -> dict`.

`prepare_context(upstream: Path, startup_preparation: Path, output: Path) -> dict` in the verifier script creates the separate helper recipe. `build_hotel_execution_support_stage(context: dict, contract: HotelCorrectnessContract, *, helper_image: str) -> HotelExecutionSupportStage` in the stage module derives exact helper Pods/policies; `validate_hotel_execution_support_stage(stage: HotelExecutionSupportStage, context: dict) -> HotelExecutionSupportStage` rederives them. `validate_hotel_execution_observation(startup_observation: dict, stage: HotelExecutionSupportStage, helper_inventory: dict) -> dict` admits only the exact helper executions/policies and unchanged support/application identities. These are offline types and pure validators, never execution authority.

- [ ] Write `HotelCorrectnessTests.test_http_200_is_not_authentication_or_reservation_proof`: verify pinned Cornell fixture credentials, meaningful nonempty fixture search/rates/profile/recommendation results, one controlled successful reservation, expected availability change and its persisted record/quantity. Use deterministic IDs/dates from the generated seed catalog, not hand-retyped architecture-rounded coordinates. An endpoint cannot satisfy another endpoint's assertion.
- [ ] Write `test_epoch_retains_only_declared_reservation_effects`: inspect all eleven collections across eight databases. Non-reservation collections/markers must match the deterministic baseline; capacities/IDs/types/dates remain valid; reservation changes have valid user/hotel references, positive quantities and no over-capacity overlap. Retain a canonical fingerprint/counts of observed mutable records at each phase without pretending a post-warmup hash is fixed across different rates/durations.
  Core assertions: `self.assertTrue(observation.verified)` only after all checks; `self.assertEqual(epoch['immutable_content_sha256'], baseline['immutable_content_sha256'])`; failed-reservation body or missing persisted effect uses `self.assertRaises(ValueError)`.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_correctness tests.test_deathstarbench_hotel_execution_stage tests.test_deathstarbench_hotel_verifier -v`; expect missing implementation failure.
- [ ] Implement a separately versioned static native helper from the existing vendored startup preparation: `hotel-qualify check` performs the bounded semantic probes once, `hotel-qualify inspect --phase PHASE` only reads state. No seed/reset/repair/upsert mode. Build amd64/arm64 helper artifacts with independent digests/receipts and bounded run/dataset/phase JSON output. Bound correctness to 180 seconds, each inspection to 60 seconds, each HTTP/Mongo call to 15 seconds or remaining time, and emitted JSON to 4 MiB; stream database rows into canonical fingerprints rather than dumping unbounded records.
- [ ] Render exact one-shot helper Pods/policies with `HotelExecutionSupportStage`; rootless/read-only, no token, one `Never` execution, application role. New performance observer explicitly admits only these known stage objects. Do not weaken the v1 startup observer or reuse baseline verification as if mutated data were still baseline. Helper output and Mongo read/HTTP deadlines are bounded; acquire dispatch latches via Task 10.
- [ ] Rerun the Python command and `go test -mod=vendor -vet=off ./qualification/... ./cmd/hotel-qualify` inside the pinned Go 1.21.13 CI container against the prepared helper context; verify failed login bodies, wrong hotel IDs, duplicates, altered static data, missing persistence and excess capacity all fail. Native CI uses no external network/databases; real semantic transactions wait for normal live validation.
- [ ] Commit: `git commit -m "feat: verify Hotel transactions and retained dataset epochs"`.

### Task 9: Separate Hotel performance driver and honest metric accounting

**Files:** Create `scripts/prepare_deathstarbench_hotel_performance_driver.py`, `scripts/assets/deathstarbench/driver/hotel-performance-entrypoint`, `scripts/assets/deathstarbench/driver/hotel-performance-metrics.lua`, `app/deathstarbench_hotel_performance_artifacts.py`, `app/deathstarbench_hotel_performance_image_access.py` and corresponding tests; add `.github/workflows/deathstarbench-hotel-performance-driver.yml`, extend `.github/workflows/ci.yml`. Reuse shared pinned compiler/source/license recipes, not mutable Social globals.

**Interfaces:** `prepare_context(upstream: Path, luasocket_rock: Path, output: Path) -> dict` yields distinct `hotel-performance-driver-v1` identity; entrypoint accepts exact `attest` or `run --target PRIVATE_IPV4 --port 8080 --threads N --connections N --rate N --duration N --phase warmup|measurement`. Emits one `HOTEL_DSB_METRICS schema=hotel-performance-v1` record; records original/prepared request, entrypoint, metrics, wrk binary/context/source, seed/mix and measurement-method identities.

Metric fields are `duration_seconds`, `sent_requests`, `completed_requests`, `uncompleted_requests`, `completed_requests_per_second`, `http_errors`, `http_status_errors`, `connect_errors`, `read_errors`, `write_errors`, `timeout_errors`, `business_rejections`, `unrecognized_bodies`, `recognized_bodies`, `p50_ms`, `p95_ms`, `p99_ms`. Response classes partition completed requests as `http_errors + business_rejections + unrecognized_bodies + recognized_bodies`; HTTP status-error counts and transport counters are additional breakdowns, not added twice. `recognized_bodies` is structural recognition, not durable transaction success.

`adopt_hotel_performance_artifacts(verifier_lock: Path, driver_lock: Path, *, verifier_publication: Path, driver_publication: Path, verifier_preparation: Path, driver_preparation: Path, upstream: Path, startup: StartupHotelArtifactBinding, inspector=None, blob_inspector=None) -> HotelPerformanceArtifactBinding` reconstructs both separate recipes, source/license/native attestations and immutable manifests/configs. `qualify_hotel_performance_image_access(artifacts: HotelPerformanceArtifactBinding, *, transport) -> HotelPerformanceImageAccess` proves anonymous complete compressed bytes for both native helper platforms and the amd64 driver; `require_hotel_performance_image_access(access: HotelPerformanceImageAccess, artifacts: HotelPerformanceArtifactBinding) -> None` rechecks original process-local identity. Saved JSON and a checked-in release flag cannot construct these factory-only types.

- [ ] Write `HotelDriverTests.test_original_mix_seed_target_and_compiler_recipe_are_preserved`; assert 60/39/0.5/0.5 distribution logic, seed base `20261008`, private target canonicalization, fixed port, rate/thread divisibility and phase binding. Candidate and Social preparation receipts remain byte-identical.
- [ ] Write executable Lua/Bash tests `test_http_200_rejections_are_not_transaction_successes`, `test_one_marker_all_counts_reconcile`, `test_missing_or_nonfinite_metrics_fail`: callbacks distinguish recognized login/reservation rejection messages, HTTP/status errors, malformed bodies and structural search bodies. Reject unknown shapes rather than silently count success; keep expected capacity rejection distinct from transport error.
  Core assertions: `self.assertEqual(metrics['completed_requests_per_second'], metrics['completed_requests'] / metrics['duration_seconds'])` within emitted decimal precision; `self.assertEqual(metrics['uncompleted_requests'], metrics['sent_requests'] - metrics['completed_requests'])`; no durable-success field exists.
- [ ] Write artifact/access tests for changed prepared sources, native binary/config digest, incomplete blob bytes, copied/forked proof and saved JSON rejection. Assert a marker with `measurement_qualified=1` cannot fabricate either factory capability.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_performance_driver tests.test_deathstarbench_hotel_performance_artifacts tests.test_deathstarbench_hotel_performance_image_access -v`; expect failure.
- [ ] Implement read-only nonroot driver with pinned runtime/binary/script checks and one direct wrk2 execution. Emit completed requests/sec, sent/completed/uncompleted, HTTP/transport breakdown, business/body classifications and p50/p95/p99; no “durable successful transactions/sec” field or endpoint-correlated claim.
- [ ] Implement separate artifact adoption/access using the existing startup factory patterns. Keep old baseline/startup access requirements as well; they do not cover the new helper/driver images.
- [ ] Rerun executable tests against actual prepared scripts with `DSB_REQUIRE_LUA=1`, existing candidate Lua gates, deterministic twice-preparation and no-network artifact attestation. Publish/adopt distinct platform digests using review-approved workflows; retain full anonymous-byte and native runtime evidence, not tags or old candidate markers as qualification.
- [ ] Commit: `git commit -m "feat: add immutable Hotel performance driver accounting"`.

### Task 10: One correctness/warmup/measurement execution under one lease

**Files:** Create `app/deathstarbench_hotel_performance_runtime.py`, `app/deathstarbench_hotel_performance_journal.py`, `app/deathstarbench_hotel_measurement.py` and corresponding tests; extend only explicit version dispatch in the new startup controller/identity/observer/journal from Tasks 1–4.

**Interfaces:** `HotelPerformanceJournal(context: dict, *, lease)` exposes `record_intent(phase: str, evidence: dict) -> dict`, `record_completion(phase: str, evidence: dict) -> dict`, `metadata() -> dict`; fixed phases `correctness`, `warmup`, `measurement`, `postcheck`, with separate one-shot helper inspection dispatch identities. `HotelMeasurementContract` freezes options and artifact/epoch/mix semantics; `HotelMeasurementObservation` holds pending metrics and complete provenance. Produce `parse_hotel_measurement_output(value: str | bytes, *, expected_contract: HotelMeasurementContract, expected_phase: str) -> dict` and `run_hotel_performance_execution(job, context: dict, contract: HotelMeasurementContract, *, lease, execute, observe, persist, monotonic=time.monotonic, timestamp) -> HotelMeasurementObservation`.

The runtime module produces exact new `HotelPerformanceRuntimePlan` and `HotelPerformanceStartupStage` via `create_hotel_performance_runtime_plan(startup_plan: StartupHotelRuntimePlan, contract: HotelMeasurementContract, *, artifacts: HotelPerformanceArtifactBinding, access: HotelPerformanceImageAccess) -> HotelPerformanceRuntimePlan` and `build_hotel_performance_startup_stage(plan: HotelPerformanceRuntimePlan) -> HotelPerformanceStartupStage`. Compose and revalidate original factory inputs; never cast a saved v1 plan/stage or mutate its snapshot. Explicitly accept only the two audited startup/performance type pairs in new execution code. The performance stage differs only through version-bound execution identity/deadline and exact later helper admission; retain all application image/env/security/placement constraints.

- [ ] Write `HotelMeasurementTests.test_exactly_one_contiguous_interval_and_retained_warmup`: order fresh startup proof → semantic checks → epoch → optional one warmup → epoch → one measurement → final state/continuity. Assert warmup=0 is explicitly skipped; no reset/delete/reseed and no measurements from multiple invocations are combined.
- [ ] Write `test_response_loss_or_persist_failure_permanently_stops_traffic`, `test_restart_during_interval_invalidates_pending_result`, `test_transport_request_errors_remain_visible_without_control_retry`. Assert latches predate every command, terminal failure prevents later commands, and transcript counts/durations/finiteness match contract and summary. Ambiguous helper response fails, never repeat a mutating correctness probe.
- [ ] Add `test_maximum_duration_has_a_precreated_bounded_stage_not_a_deadline_patch`: at warmup=600/duration=1800, assert the separate performance Pod deadline is 3840 and the v1 stage remains 900. The startup window remains 900; no callback can extend it. Assert every helper is terminated before measurement and no correctness/network/CRI polling runs inside the timed interval.
  Core assertions: `self.assertEqual(phase_dispatches.count('measurement'), 1)`; `self.assertEqual(reset_calls, 0)`; `self.assertEqual(performance_pod['spec']['activeDeadlineSeconds'], 3840)`; `self.assertEqual(v1_pod['spec']['activeDeadlineSeconds'], 900)` for the maximum-options fixture.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_performance_runtime tests.test_deathstarbench_hotel_performance_journal tests.test_deathstarbench_hotel_measurement -v`; expect failure.
- [ ] Implement bounded durable phase intents and exact image/Pod identity continuity. Retain Kubernetes/CRI execution IDs before/after the timed interval; `Never` restart plus unchanged exact execution identities detect exits/replacements without running diagnostic probes inside the measurement. Do not rerun old initialization/driver/setup after control failure.
- [ ] Rerun Task 8–10 suites plus Social measurement regressions; expect pass. Metrics stay pending, not a completed chart-eligible result, until the owning lifecycle finalizes after post-check and teardown. Invariant failure, application exit, malformed metrics or timeout is a failed whole run.
- [ ] Commit: `git commit -m "feat: execute uninterrupted Hotel performance measurements"`.

### Task 11: Sticky Hotel validity, reports, history and comparison cohorts

**Files:** Create `app/deathstarbench_hotel_validity.py`, `tests/test_deathstarbench_hotel_validity.py`; modify `app/main.py`, `app/comparison.py`, `tests/test_deathstarbench_comparison.py`, `tests/test_comparison_api.py`, `tests/test_result_reporting.py`, `tests/test_history_comparison_ui.py`.

**Interfaces:** `fail_hotel_run(job: dict, *, phase: str, reason: str, occurred_at: str) -> None` appends/retains a monotonic terminal latch. `complete_hotel_run(job: dict, observation: HotelMeasurementObservation, *, cleanup_evidence: dict, completed_at: str) -> None` requires complete uninterrupted execution and absence proof. `hotel_run_comparison_eligibility(run: dict, result: dict, plan: dict) -> dict` returns `{'eligible': bool, 'reason': str}`; missing/invalid/crossed provenance is ineligible. Use schema `hotel-run-validity-v1`, no authority in a caller-supplied `eligible=True` field.

- [ ] Write a full round-trip `HotelValidityTests.test_completed_row_then_runner_failure_never_returns_to_charts` covering state/results/report fallback, save/reload, regenerated report and comparison API. Also assert cancellation, malformed/missing validity, crossed run IDs, interrupted post-persistence and destroy success cannot clear failure. UI refresh with a healthy worker must not create failure.
- [ ] Write `HotelComparisonTests.test_dataset_epoch_method_not_per_run_ids_defines_cohort`: bind workload/dataset baseline identity, epoch policy, helper/driver/metric method, mix/seed/options, runtime/topology/source/config/images. Keep actual run/Pod UUIDs and observed epoch hashes as provenance, not equality keys that would prevent valid cross-cloud comparisons. No fake Reed98 nodes/edges/initializer hashes.
  Core assertions: `self.assertFalse(eligibility['eligible'])` after late runner failure; `self.assertEqual(eligible_chart_metrics, [])`; `self.assertEqual(aws_cohort, oci_cohort)` for identical qualified contracts with different run/Pod IDs.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_validity tests.test_deathstarbench_comparison tests.test_comparison_api tests.test_result_reporting tests.test_history_comparison_ui -v`; expect failure.
- [ ] Integrate validity into `benchmark_status`, `require_complete_benchmark_results`, `results_artifact_job`, persistence/reopen, summaries and report generation. Add fields to comparison `_RUN_FIELDS`; branch `_deathstarbench_execution_context` explicitly by workload and enforce Hotel run eligibility before any chart extraction or comparison selection. Preserve non-Hotel historical behavior.
- [ ] Rerun result/comparison/history suites and Social fingerprints; expect pass. Raw failed outputs stay visible with reason but cannot be presented as valid throughput bars, completed benchmark summaries or comparison candidates.
- [ ] Commit: `git commit -m "fix: keep failed Hotel runs out of performance comparisons"`.

### Task 12: Qualified Hotel release bundle and normal lifecycle dispatch

**Files:** Create `app/deathstarbench_hotel_release.py`, `app/deathstarbench_hotel_public.py`, `tests/test_deathstarbench_hotel_release.py`, `tests/test_deathstarbench_hotel_public.py`; modify `app/deathstarbench_workload_contract.py`, `app/deathstarbench_public.py`, `app/models.py`, `app/catalog.py`, `app/main.py`, `app/providers/{aws,azure,gcp,oci}.py` and provider/public lifecycle tests.

**Interfaces:** `require_released_hotel_bundle(*, source: Path) -> dict`, `snapshot_hotel_release_bundle(run_directory: Path, *, source: Path) -> Path`, `released_hotel_profile(bundle: dict) -> DistributedDeathStarBenchWorkloadProfile`, `run_public_hotel_performance_lifecycle(job, plan, bundle: dict, *, execute, persist, emit, public_key, oci_context_factory) -> None`. Public bundle schema `hotel-performance-release-v1` binds original support, guarded initializer, startup, verifier and driver artifacts, semantic/epoch/measurement/network contracts and actual qualification records.

- [ ] Write `HotelReleaseTests.test_missing_gate_or_mixed_bundle_fails_before_provision`, `test_candidate_profile_stays_unreleased_after_public_release` and `test_saved_bundle_is_exclusive_and_run_bound`. Tampering one image/recipe/epoch/driver/provider architecture qualification fails closed; public flags alone do not qualify a bundle.
- [ ] Write `HotelPublicTests.test_one_owned_lifecycle_not_double_provision_or_early_destroy`: public job is a status/report facade over the private execution context. Acquire a fresh original private ownership lease once and hold it through provision, support, initialization, startup, measurement and teardown; a parent public state lock is never accepted as dispatch authority. Compose lower-level startup and measurement, not the startup-only operator that destroys immediately after startup.
  Core assertions: `self.assertFalse(HOTEL_RESERVATION_PROFILE.released)`; `self.assertEqual(provision_calls, 1)`; `self.assertLess(events.index('postcheck'), events.index('destroy'))`; public snapshot from another run uses `self.assertRaises(ValueError)`.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_release tests.test_deathstarbench_hotel_public tests.test_provider_plan -v`; expect failure.
- [ ] Add explicit Hotel dispatch and independently versioned public profile resolution without changing `distributed_workload_profile('hotel_reservation')` candidate semantics. Reuse exact five-node provider allocation/storage contracts, but authorize only known Hotel/Social selectors; never broaden provider validators to arbitrary strings. Package the validated local preparation/source inputs and licenses referenced by each artifact with the release distribution so fresh public factory validation does not depend on a developer's checkout path or user-supplied source override; bounded cache reuse is revalidated, never authority by filename.
- [ ] Extend normal stop/destroy routing to consume independent Hotel ownership. Any unfinished execution found after process restart becomes failed and offers destroy only. No resume/recover route, no Social initializer/measurement or private source adoption from public saved JSON. Finalize validity only after exact teardown and absence; reject Hotel retention option explicitly rather than silently ignoring it.
- [ ] Rerun private candidate/init/startup regressions and four provider/public lifecycle suites; expect pass. Keep Hotel's checked-in public gate false until Task 14; test the complete path with an explicitly injected reviewed test bundle, never a production bypass flag.
- [ ] Commit: `git commit -m "feat: route qualified Hotel performance lifecycles explicitly"`.

### Task 13: Public selector, progress, history and charts

**Files:** Modify `app/static/app.js`, `app/static/index.html`, `app/static/history.js`, `app/static/comparison-page.js`, `app/static/comparison-view.js` only where necessary; add `tests/test_deathstarbench_hotel_public_ui.py`, extend `tests/test_deathstarbench_public_ui.py`, `tests/test_comparison_page_ui.py`, `tests/test_live_stop_ui.py`.

**Interfaces:** Existing `/api/catalog` exposes only bundle-qualified selectable workloads; Hotel uses the existing `workload='hotel_reservation'` value and distributed topology/runtime fields. Backend rejects an unqualified manual request even if a client fabricates an option.

- [ ] Write executable JS/API tests `test_hotel_available_only_from_qualified_catalog`, `test_hotel_form_roundtrip_and_stop_destroy_without_resume`, `test_failed_hotel_row_is_diagnostic_not_comparable`. Assert Firefox-safe native option labels, current shape auto-fill protections and existing Social selection survive.
  Core assertions: `self.assertNotIn('media_microservices', selectable_workload_ids)`; `self.assertEqual(restored_options['workload'], 'hotel_reservation')`; `self.assertFalse(failed_card['comparison_selectable'])` in the executable DOM fixture.
- [ ] Run `python3 -m unittest tests.test_deathstarbench_hotel_public_ui tests.test_live_stop_ui tests.test_comparison_page_ui -v`; expect failure.
- [ ] Replace Social-only force-selection in `enforceDeathstarTopologyValues`, `setDeathstarValues`, `restoreDeathstarSelection` with explicit catalog-qualified workload resolution. Keep Media hidden. Render correctness/warmup/measurement/cleanup stages and terminal reasons without resilience metrics or recovery controls.
- [ ] Rerun JS-backed suites and `node --check` for changed scripts; expect pass. Verify normal UI refresh preserves healthy execution and that failed/partial diagnostics cannot add a compare selection through URL parameters or chart API calls.
- [ ] Inspect the actual local UI in a browser before claiming UI completion: qualified fixture catalog, closed production gate, both workload restore flows, failed diagnostics, valid comparison layout and stop/destroy confirmation. Do not start cloud work merely by opening a page.
- [ ] Commit: `git commit -m "feat: present qualified Hotel benchmark results in the UI"`.

### Task 14: Normal live matrix, final release evidence and review-only PR

**Files:** Create `scripts/qualify_deathstarbench_hotel_performance.py`, `tests/test_deathstarbench_hotel_performance_cli.py`, `docs/qualification/deathstarbench-hotel-performance-v1.json`; update `docs/deathstarbench-workload-expansion.md`, `README.md` and the checked-in Hotel release bundle. Do not overwrite earlier candidate/publication/audit evidence.

**Interfaces:** Fresh-only qualification CLI composes the normal Hotel performance lifecycle with explicit provider/shape/architecture and immutable artifact inputs. `--destroy-only` uses retained ownership; no fault-injection/resume options. Evidence schema binds source commit, exact artifacts/settings, provider/account/region/shape/architecture, semantic/epoch/network/runtime/measurement observations, validity and independent absence.

- [ ] Write `HotelPerformanceCliTests.test_normal_run_has_no_failure_injection_or_resume_switch`, `test_unqualified_matrix_cell_cannot_open_release`, `test_evidence_omits_secrets_and_does_not_rewrite_old_outcomes`; run red/green while implementing CLI/evidence validation.
  Core assertions: `self.assertNotIn('--interrupt-after', help_text)`; `self.assertNotIn('--resume', help_text)`; bundle promotion with a missing advertised provider/architecture cell uses `self.assertRaises(ValueError)`.
- [ ] Run `PYTHON_BIN=python3 scripts/check.sh`; require zero test failures. Run required Python 3.10/3.14 and pinned source/Go/Lua CI jobs and independent whole-branch review. Do not substitute earlier test counts, skips or successful image builds for these fresh gates.
- [ ] Start with the reviewed GCP Arm startup path, then complete normal end-to-end runs for advertised AWS/OCI/GCP/Azure × amd64/arm64 application cells. Use default 30-second warmup, 60-second measured interval, four threads, 64 connections, 100 req/s; keep support/loadgen x86. Capacity/quota failure is retained failure with exact cleanup, not a pass or a reason to resume that invocation.
- [ ] Before timed traffic, prove allowed and denied Hotel network edges with positive controls; remove only exact owned probe artifacts. Never deliberately kill/disconnect a runner/service/node. Check meaningful operations, immutable executions, all epochs, one measured interval, complete error accounting and no application restart. Confirm every owned VM/network/storage/helper graph absent afterward using provider-specific identity checks.
- [ ] Run a normal public-lifecycle smoke per provider with reviewed release-candidate inputs while general selection stays closed; verify actual report/history/comparison provenance. Compare matching successful Hotel runs across clouds: distinct execution IDs must not block matching cohort, incompatible settings must explain exclusion, and every failed attempt must remain diagnostic-only.
- [ ] Publish only real sanitized observations and mark only qualified cells supported. Open the Hotel public gate in the coordinated immutable release bundle/profile/catalog tests after all required evidence passes. Missing evidence stays closed; no manufactured gate booleans or blanket all-cloud claims.
- [ ] Rerun full checks/required CI after final evidence/release changes. Create and attach a review-only PR summarizing the entire Hotel outcome, actual cloud matrix, tests and cleanup status; wait for the user to merge. No auto-merge and no unreported retained resources.

## Completion Checklist and Plan Self-Review

- Startup/inventory/admission/CRI/Consul requirements: Tasks 1–4; original offline snapshots never become authority.
- Durable ownership, exact teardown, interrupted runs terminal: Tasks 5–7; no execution reentry or dedicated live failure gate.
- Real Hotel operation/persistence correctness and retained state: Task 8; separate helper artifacts and explicit stage admission.
- Immutable driver and honest non-correlated metric accounting: Task 9; candidate callback markers never treated as qualified semantics.
- One uninterrupted execution, no replay/reset/merged intervals: Task 10; original lease retained and latches consumed before dispatch.
- Sticky whole-run validity and compatible cross-cloud cohorts: Task 11; five review-focus classes have named tests in their owning tasks.
- Explicit release/profile/provider/public ownership adapter and UI: Tasks 12–13; candidate history and Social/Media behavior remain intact.
- Normal supported-cloud/architecture evidence and coordinated public release: Task 14; startup-only success is never Hotel completion.
- All referenced new production APIs are defined in their owning task; reuse existing factory types rather than fabricated capabilities. Each task's red/green checks precede its commit; stage only its named files.
- No product code, dependency installation, worktree setup or live deployment occurs until this plan is reviewed and an execution method is chosen. At execution time use the worktree skill, reuse a suitable existing attached checkout when available, and preserve this branch's approved spec/plan commits.
