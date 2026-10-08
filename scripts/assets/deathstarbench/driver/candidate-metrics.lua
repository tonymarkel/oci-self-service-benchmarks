-- Candidate callback accounting only: this is NOT the released Social method.
-- wrk2 does not give response() a request/connection identifier. Hotel body
-- checks therefore recognize the pinned frontend's messages/GeoJSON shape;
-- they cannot establish endpoint-level semantics or reservation durability.
local candidate_threads = {}
local worker_count = 0

setup = function(thread)
  worker_count = worker_count + 1
  thread:set("dsb_candidate_worker_index", worker_count)
  table.insert(candidate_threads, thread)
end

init = function(args)
  assert(type(dsb_candidate_worker_index) == "number", "worker index missing")
  -- Scheduling still changes which worker issues a request at a given time;
  -- this fixes each worker's random stream, not wall-clock ordering.
  math.randomseed(candidate_seed_base + dsb_candidate_worker_index)
  math.random(); math.random(); math.random()
  dsb_candidate_completed = 0
  dsb_candidate_http_errors = 0
  dsb_candidate_http_status_errors = 0
  dsb_candidate_semantic_failures = 0
  dsb_candidate_semantic_successes = 0
end

local function candidate_body_ok(body)
  if type(body) ~= "string" then return false end
  if candidate_workload == "media_microservices" then
    -- The pinned compose endpoint ngx.exit(200) has an empty response body.
    return body == ""
  end
  local message = body:match('^%s*{%s*"message"%s*:%s*"([^"]*)"%s*}%s*$')
  if message then
    return message == "Login successfully!" or message == "Reserve successfully!"
  end
  -- Search/recommendation use GeoJSON. This deliberately modest structural
  -- check is NOT a full JSON decoder or an endpoint-specific assertion.
  return body:match('^%s*{') ~= nil and body:match('}%s*$') ~= nil and
    body:match('"type"%s*:%s*"FeatureCollection"') ~= nil and
    body:match('"features"%s*:%s*%[') ~= nil
end

response = function(status, headers, body)
  dsb_candidate_completed = dsb_candidate_completed + 1
  if status > 399 then
    dsb_candidate_http_status_errors = dsb_candidate_http_status_errors + 1
  end
  if status ~= 200 then
    dsb_candidate_http_errors = dsb_candidate_http_errors + 1
  elseif not candidate_body_ok(body) then
    -- In particular, HTTP-200 failed login / exhausted reservations are not
    -- successful requests. They remain separate from transport/HTTP errors.
    dsb_candidate_semantic_failures = dsb_candidate_semantic_failures + 1
  else
    dsb_candidate_semantic_successes = dsb_candidate_semantic_successes + 1
  end
end

done = function(summary, latency, requests)
  local completed, http_errors, status_errors, semantic_failures, successes = 0, 0, 0, 0, 0
  for _, thread in ipairs(candidate_threads) do
    completed = completed + assert(thread:get("dsb_candidate_completed"))
    http_errors = http_errors + assert(thread:get("dsb_candidate_http_errors"))
    status_errors = status_errors + assert(thread:get("dsb_candidate_http_status_errors"))
    semantic_failures = semantic_failures + assert(thread:get("dsb_candidate_semantic_failures"))
    successes = successes + assert(thread:get("dsb_candidate_semantic_successes"))
  end
  local seconds = summary.duration / 1000000.0
  local socket_errors = summary.errors.connect + summary.errors.read +
    summary.errors.write + summary.errors.timeout
  local consistent = 0
  if completed == summary.requests and status_errors == summary.errors.status and
     completed == http_errors + semantic_failures + successes then consistent = 1 end
  local throughput, failures_percent = 0.0, 0.0
  if seconds > 0 then throughput = successes / seconds end
  if completed > 0 then failures_percent = (http_errors + semantic_failures) * 100.0 / completed end
  io.write(string.format(
    "CANDIDATE_DSB_METRICS schema=candidate-body-v1 measurement_qualified=0 " ..
    "workload=%s seed_base=%d workers=%d completed_responses=%d summary_requests=%d " ..
    "body_check_successes=%d http_errors=%d http_status_errors=%d semantic_failures=%d " ..
    "socket_errors=%d connect_errors=%d read_errors=%d write_errors=%d timeout_errors=%d " ..
    "counter_consistent=%d duration_seconds=%.6f " ..
    "body_check_successes_per_second=%.6f completed_failure_percent=%.6f " ..
    "p50_ms=%.6f p95_ms=%.6f p99_ms=%.6f\n",
    candidate_workload, candidate_seed_base, #candidate_threads, completed, summary.requests,
    successes, http_errors, status_errors, semantic_failures,
    socket_errors, summary.errors.connect, summary.errors.read, summary.errors.write,
    summary.errors.timeout, consistent, seconds, throughput, failures_percent,
    latency:percentile(50.0) / 1000.0, latency:percentile(95.0) / 1000.0,
    latency:percentile(99.0) / 1000.0))
end
