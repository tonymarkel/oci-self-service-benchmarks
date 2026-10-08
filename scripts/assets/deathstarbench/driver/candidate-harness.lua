-- Executable Lua 5.1/LuaJIT smoke harness for the actual prepared assets.
-- Separate environments model wrk's worker globals/get/set; this does NOT
-- exercise wrk's C threads, HTTP parser, sockets, or live service semantics.
assert(_VERSION == "Lua 5.1", "candidate smoke requires Lua 5.1/LuaJIT")
local workload, request_path, metrics_path = arg[1], arg[2], arg[3]
assert(workload == "hotel_reservation" or workload == "media_microservices")
assert(request_path and metrics_path)
package.preload.socket = function() return {gettime = function() return 0 end} end

local output = {}
local master = setmetatable({io = {write = function(value) table.insert(output, value) end}}, {__index = _G})
local function load_into(path, environment)
  local chunk = assert(loadfile(path))
  setfenv(chunk, environment)
  chunk()
end
load_into(metrics_path, master)

local function worker()
  local state = setmetatable({}, {__index = _G})
  state.wrk = {format = function(method, path, headers, body)
    return {method = method, path = path, body = body, headers = headers}
  end}
  local thread = {
    set = function(self, key, value) state[key] = value end,
    get = function(self, key) return rawget(state, key) end,
  }
  master.setup(thread)
  load_into(request_path, state)
  load_into(metrics_path, state)
  state.init({})
  return state
end
local first, second = worker(), worker()
assert(first.dsb_candidate_worker_index == 1 and second.dsb_candidate_worker_index == 2)

-- Reinitializing the same worker reproduces its first request random stream.
first.init({})
local replay_a = first.request()
first.init({})
local replay_b = first.request()
assert(replay_a.path == replay_b.path and replay_a.body == replay_b.body, "seed replay failed")
second.init({})
local other = second.request()
assert(replay_a.path ~= other.path or replay_a.body ~= other.body, "workers reused a random stream")

local endpoints = {}
first.init({})
for i = 1, 6000 do
  local request = first.request()
  assert(type(request.path) == "string" and request.path:find("http://localhost:8080/", 1, true) == 1)
  assert(not request.path:find("nil", 1, true), "undefined coordinate leaked into request")
  if workload == "hotel_reservation" then
    local endpoint = assert(request.path:match("http://localhost:8080(/[^?]+)"))
    endpoints[endpoint] = (endpoints[endpoint] or 0) + 1
    if endpoint ~= "/user" then
      assert(tonumber(request.path:match("[?&]lat=([^&]+)")), "latitude missing")
      assert(tonumber(request.path:match("[?&]lon=([^&]+)")), "longitude missing")
    end
  else
    assert(request.path == "http://localhost:8080/wrk2-api/review/compose", "duplicated compose endpoint")
    assert(request.method == "POST" and request.headers["Content-Type"] == "application/x-www-form-urlencoded")
    assert(request.body:match("^username=username_%d+&password=password_%d+&title=.+&rating=%d+&text=.+$"))
  end
end
if workload == "hotel_reservation" then
  for _, endpoint in ipairs({"/hotels", "/recommendations", "/user", "/reservation"}) do
    assert(endpoints[endpoint] and endpoints[endpoint] > 0, "mixed endpoint not exercised: " .. endpoint)
  end
end

first.init({}); second.init({})
local completed, successes, semantic, http, status_errors
if workload == "hotel_reservation" then
  first.response(200, {}, '{"message":"Login successfully!"}\n')
  first.response(200, {}, '{"message":"Failed. Please check your username and password. "}\n')
  first.response(200, {}, '{"message":"Failed. Already reserved. "}\n')
  first.response(200, {}, '{"features":[],"type":"FeatureCollection"}\n')
  first.response(500, {}, 'internal failure')
  first.response(302, {}, '')
  second.response(200, {}, '{"message":"Reserve successfully!"}\n')
  second.response(200, {}, '{"message":"unexpected"}\n')
  second.response(404, {}, 'missing')
  completed, successes, semantic, http, status_errors = 9, 3, 3, 3, 2
else
  first.response(200, {}, '')
  first.response(200, {}, '{"error":"compose failed"}')
  first.response(500, {}, 'failure')
  second.response(200, {}, '')
  second.response(200, {}, ' ')
  second.response(302, {}, '')
  completed, successes, semantic, http, status_errors = 6, 2, 2, 2, 1
end
local summary = {duration = 2000000, requests = completed,
  errors = {connect = 1, read = 2, write = 3, timeout = 4, status = status_errors}}
local latency = {percentile = function(self, value) return value * 1000 end}
master.done(summary, latency, {})
local function metric(line, name) return tonumber(line:match(name .. "=(%S+)")) end
local line = assert(output[#output])
assert(line:find("CANDIDATE_DSB_METRICS schema=candidate-body-v1 measurement_qualified=0", 1, true))
assert(not line:find("OCI_DSB_METRICS", 1, true))
assert(metric(line, "workers") == 2)
assert(metric(line, "completed_responses") == completed)
assert(metric(line, "body_check_successes") == successes)
assert(metric(line, "semantic_failures") == semantic)
assert(metric(line, "http_errors") == http)
assert(metric(line, "http_status_errors") == status_errors)
assert(metric(line, "socket_errors") == 10)
assert(metric(line, "counter_consistent") == 1)
summary.requests = completed + 1
master.done(summary, latency, {})
assert(metric(output[#output], "counter_consistent") == 0, "counter mismatch was hidden")
summary.requests = completed
summary.errors.status = status_errors + 1
master.done(summary, latency, {})
assert(metric(output[#output], "counter_consistent") == 0, "HTTP status mismatch was hidden")
io.write("candidate Lua harness passed: " .. workload .. "\n")
