package registry

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/initialization"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/initialization/catalog"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/startup"
	consul "github.com/hashicorp/consul/api"
)

func candidateRegistryIdentity(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(key)
		if strings.HasPrefix(upper, "CONSUL_") || strings.HasPrefix(upper, "JAEGER_") ||
			strings.HasPrefix(upper, "MONGO") || strings.Contains(upper, "PROXY") ||
			strings.HasPrefix(upper, "DSB_") || strings.HasPrefix(upper, "TLS_") || strings.HasPrefix(upper, "HOTEL_") {
			t.Setenv(key, "")
			if os.Unsetenv(key) != nil {
				t.Fatal("test environment cleanup refused")
			}
		}
	}
	dataset, err := initialization.BuildDataset(catalog.Collections(), catalog.SourceFingerprint)
	if err != nil {
		t.Fatal("compiled dataset unavailable")
	}
	for key, value := range map[string]string{
		"TLS": "false", "HOTEL_CANDIDATE_RUN_ID": "0123456789ab",
		"HOTEL_CANDIDATE_CONTRACT_SHA256":    dataset.Fingerprint(),
		"HOTEL_CANDIDATE_STARTUP_ATTEMPT_ID": "11111111-1111-4111-8111-111111111111",
		"HOTEL_CANDIDATE_COMPONENT":          "geo",
		"HOTEL_CANDIDATE_POD_UID":            "22222222-2222-4222-8222-222222222222",
		"HOTEL_CANDIDATE_POD_IP":             "10.42.1.7",
	} {
		t.Setenv(key, value)
	}
	if _, err := startup.CurrentIdentity(); err != nil {
		t.Fatal("test startup identity refused")
	}
}

// The URL remains the exact production agent. Only this package-private test
// transport dials the loopback httptest listener; there is no external network.
func candidateFakeConsul(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	transport := candidateTransport()
	t.Cleanup(transport.CloseIdleConnections)
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != consulAddress {
			t.Errorf("nonfixed Consul address requested")
		}
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	client, err := newCandidateClient(transport)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func candidateRegistration(t *testing.T) (string, string, string, int) {
	t.Helper()
	id, err := ServiceID("srv-geo")
	if err != nil {
		t.Fatal(err)
	}
	return "srv-geo", id, "10.42.1.7", 8083
}

func TestCandidateConsulFixedConfigurationAndNoAmbientOverrides(t *testing.T) {
	candidateRegistryIdentity(t)
	for _, address := range []string{"", "http://consul:8500", "consul:8501", "localhost:8500", "secret@consul:8500"} {
		if client, err := NewClient(address); err == nil || client != nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("nonfixed endpoint accepted or exposed")
		}
	}
	for _, key := range []string{"CONSUL_HTTP_ADDR", "CONSUL_HTTP_TOKEN", "CONSUL_HTTP_TOKEN_FILE",
		"CONSUL_HTTP_AUTH", "CONSUL_NAMESPACE", "CONSUL_CACERT", "CONSUL_CLIENT_KEY",
		"HTTP_PROXY", "https_proxy", "ALL_PROXY", "NO_PROXY"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "candidate-secret-must-not-be-read-or-printed")
			client, err := NewClient(consulAddress)
			if err == nil || client != nil || strings.Contains(err.Error(), "candidate-secret") {
				t.Fatal("ambient Consul setting accepted or exposed")
			}
		})
	}
	client, err := NewClient(consulAddress)
	if err != nil || client == nil {
		t.Fatal("fixed client could not be constructed offline")
	}
}

func TestCandidateConsulOneReadOneRegistrationAndExactMetadata(t *testing.T) {
	candidateRegistryIdentity(t)
	var reads, writes int32
	client := candidateFakeConsul(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Host != consulAddress || r.Header.Get("Authorization") != "" || r.Header.Get("X-Consul-Token") != "" {
			t.Error("Consul host or credential header differs")
		}
		switch r.Method + " " + r.URL.Path {
		case "GET /v1/agent/services":
			atomic.AddInt32(&reads, 1)
			w.Write([]byte(`{}`))
		case "PUT /v1/agent/service/register":
			atomic.AddInt32(&writes, 1)
			var registration consul.AgentServiceRegistration
			if json.NewDecoder(r.Body).Decode(&registration) != nil {
				t.Error("invalid registration body")
			}
			identity, _ := startup.CurrentIdentity()
			_, id, ip, port := candidateRegistration(t)
			if registration.ID != id || registration.Name != "srv-geo" || registration.Address != ip || registration.Port != port {
				t.Error("service identity differs")
			}
			metadata := registration.Meta
			if len(metadata) != 5 || metadata["run_id"] != identity.RunID ||
				metadata["startup_attempt_id"] != identity.StartupAttemptID || metadata["pod_uid"] != identity.PodUID ||
				metadata["contract_sha256"] != identity.ContractSHA256 || metadata["component"] != "geo" {
				t.Error("service metadata differs")
			}
			if registration.Check != nil || len(registration.Checks) != 0 || len(registration.Tags) != 0 || r.URL.RawQuery != "" {
				t.Error("unexpected health callback, tag or query")
			}
		default:
			t.Error("unexpected Consul API request")
			w.WriteHeader(http.StatusNotFound)
		}
	})
	name, id, ip, port := candidateRegistration(t)
	if err := client.Register(name, id, ip, port); err != nil {
		t.Fatal(err)
	}
	// Even if the fake agent still reports an empty catalog (as after a dev-agent
	// restart), this process cannot re-register or retry the consumed attempt.
	if client.Register(name, id, ip, port) == nil || client.Deregister(id) == nil {
		t.Fatal("registration replay or deregistration accepted")
	}
	if atomic.LoadInt32(&reads) != 1 || atomic.LoadInt32(&writes) != 1 {
		t.Fatal("expected exactly one preflight and one registration")
	}
}

func TestCandidateConsulWrongIdentityAndUnboundClientsCannotMutate(t *testing.T) {
	candidateRegistryIdentity(t)
	var requests int32
	client := candidateFakeConsul(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Write([]byte(`{}`))
	})
	name, id, ip, port := candidateRegistration(t)
	for _, test := range []struct {
		name, id, ip string
		port         int
	}{
		{"srv-user", id, ip, port}, {name, "foreign-secret", ip, port},
		{name, id, "", port}, {name, id, "10.42.1.8", port}, {name, id, ip, 8084},
	} {
		err := client.Register(test.name, test.id, test.ip, test.port)
		if err == nil || strings.Contains(err.Error(), "foreign-secret") {
			t.Fatal("wrong service identity accepted or exposed")
		}
	}
	if (&Client{Client: client.Client}).Register(name, id, ip, port) == nil {
		t.Fatal("unbound client accepted")
	}
	client.AddHeader("X-Consul-Token", "candidate-secret")
	if err := client.Register(name, id, ip, port); err == nil || strings.Contains(err.Error(), "candidate-secret") {
		t.Fatal("caller header override accepted or exposed")
	}
	if atomic.LoadInt32(&requests) != 0 {
		t.Fatal("identity rejection made a remote request")
	}
}

func TestCandidateConsulDuplicateCatalogsQueryFailuresAndOversizeRefuseWrites(t *testing.T) {
	candidateRegistryIdentity(t)
	name, id, ip, port := candidateRegistration(t)
	duplicate := func(key, observedID, observedName string) string {
		raw, _ := json.Marshal(map[string]interface{}{key: map[string]string{"ID": observedID, "Service": observedName}})
		return string(raw)
	}
	for _, test := range []struct {
		label, body string
		status      int
	}{
		{"same-name", duplicate("old-id", "old-id", name), 200},
		{"same-id", duplicate(id, id, "foreign-name"), 200},
		{"exact-duplicate", duplicate(id, id, name), 200},
		{"malformed-entry", duplicate("wrong-key", "another-id", "srv-user"), 200},
		{"null-catalog", `null`, 200}, {"bad-json", `candidate-secret`, 200},
		{"duplicate-catalog-key", `{"old-id":{"ID":"old-id","Service":"srv-geo"},"old-id":{"ID":"old-id","Service":"srv-user"}}`, 200},
		{"duplicate-service-field", `{"old-id":{"ID":"old-id","Service":"srv-geo","Service":"srv-user"}}`, 200},
		{"trailing-json", `{} {"old-id":{"ID":"old-id","Service":"srv-geo"}}`, 200},
		{"query-error", `candidate-secret`, 500},
		{"oversize", `{"` + strings.Repeat("x", maxCatalogBytes) + `":{}}`, 200},
	} {
		t.Run(test.label, func(t *testing.T) {
			var reads, writes int32
			client := candidateFakeConsul(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					atomic.AddInt32(&writes, 1)
				}
				atomic.AddInt32(&reads, 1)
				w.WriteHeader(test.status)
				w.Write([]byte(test.body))
			})
			err := client.Register(name, id, ip, port)
			if err == nil || strings.Contains(err.Error(), "candidate-secret") || atomic.LoadInt32(&writes) != 0 || atomic.LoadInt32(&reads) != 1 {
				t.Fatal("rejected preflight mutated, retried, succeeded or exposed server body")
			}
			if client.Register(name, id, ip, port) == nil || atomic.LoadInt32(&reads) != 1 {
				t.Fatal("failed preflight was replayed")
			}
		})
	}
}

func TestCandidateConsulConcurrentAndCopiedClientShareOneAttempt(t *testing.T) {
	candidateRegistryIdentity(t)
	name, id, ip, port := candidateRegistration(t)
	var reads, writes, successes int32
	client := candidateFakeConsul(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			atomic.AddInt32(&reads, 1)
			w.Write([]byte(`{}`))
			return
		}
		atomic.AddInt32(&writes, 1)
	})
	copied := *client
	var running sync.WaitGroup
	for index := 0; index < 8; index++ {
		running.Add(1)
		go func(index int) {
			defer running.Done()
			candidate := client
			if index%2 == 0 {
				candidate = &copied
			}
			if candidate.Register(name, id, ip, port) == nil {
				atomic.AddInt32(&successes, 1)
			}
		}(index)
	}
	running.Wait()
	if atomic.LoadInt32(&reads) != 1 || atomic.LoadInt32(&writes) != 1 || atomic.LoadInt32(&successes) != 1 {
		t.Fatal("copied or concurrent client bypassed the shared local attempt")
	}
}

func TestCandidateConsulTimeoutRedirectAndUncertainWriteNeverRetry(t *testing.T) {
	candidateRegistryIdentity(t)
	name, id, ip, port := candidateRegistration(t)
	t.Run("timeout", func(t *testing.T) {
		var requests int32
		client := candidateFakeConsul(t, func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requests, 1)
			<-r.Context().Done()
		})
		began := time.Now()
		if err := client.Register(name, id, ip, port); err == nil || time.Since(began) > registrationTimeout+time.Second {
			t.Fatal("Consul request did not fail within bound")
		}
		if atomic.LoadInt32(&requests) != 1 || client.Register(name, id, ip, port) == nil {
			t.Fatal("timed-out preflight replayed")
		}
	})
	t.Run("redirect", func(t *testing.T) {
		var requests int32
		client := candidateFakeConsul(t, func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requests, 1)
			http.Redirect(w, r, "http://candidate-secret.invalid/", http.StatusTemporaryRedirect)
		})
		if err := client.Register(name, id, ip, port); err == nil || strings.Contains(err.Error(), "candidate-secret") || atomic.LoadInt32(&requests) != 1 {
			t.Fatal("redirect followed or exposed")
		}
	})
	t.Run("uncertain-write", func(t *testing.T) {
		var reads, writes int32
		client := candidateFakeConsul(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				atomic.AddInt32(&reads, 1)
				w.Write([]byte(`{}`))
				return
			}
			atomic.AddInt32(&writes, 1)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("candidate-secret"))
		})
		err := client.Register(name, id, ip, port)
		if err == nil || strings.Contains(err.Error(), "candidate-secret") || atomic.LoadInt32(&reads) != 1 || atomic.LoadInt32(&writes) != 1 {
			t.Fatal("uncertain registration retried or exposed")
		}
		if client.Register(name, id, ip, port) == nil || atomic.LoadInt32(&writes) != 1 || atomic.LoadInt32(&reads) != 1 {
			t.Fatal("uncertain registration replayed")
		}
	})
}
