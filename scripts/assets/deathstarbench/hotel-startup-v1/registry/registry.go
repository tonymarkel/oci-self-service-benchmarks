// Package registry is a separate candidate startup source checkpoint. It does
// not qualify Consul health, catalog continuity, restart recovery or application
// readiness. A future observer must independently bind the complete live catalog.
package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/startup"
	consul "github.com/hashicorp/consul/api"
)

const consulAddress = "consul:8500"
const registrationTimeout = 2 * time.Second
const maxCatalogBytes = 65536

type clientGuard struct {
	client    *consul.Client
	mutex     sync.Mutex
	attempted bool
}

// Client keeps the upstream embedded Consul client API for existing read-only
// balancer call sites. Registrations also require an unexported factory binding.
type Client struct {
	*consul.Client
	guard *clientGuard
}

// consulEnvironment refuses overrides before the vendored constructor can read
// CONSUL_HTTP_TOKEN_FILE or inherit a namespace, credential or TLS setting.
// Values are never included in errors. The candidate has no ambient proxy path.
func consulEnvironment() error {
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		lower := strings.ToLower(key)
		if value != "" && (strings.HasPrefix(key, "CONSUL_") ||
			lower == "http_proxy" || lower == "https_proxy" || lower == "all_proxy" || lower == "no_proxy") {
			return fmt.Errorf("candidate Hotel Consul environment override refused")
		}
	}
	return nil
}

func candidateTransport() *http.Transport {
	return &http.Transport{
		Proxy:                  nil,
		DialContext:            (&net.Dialer{Timeout: registrationTimeout}).DialContext,
		DisableKeepAlives:      true,
		ResponseHeaderTimeout:  registrationTimeout,
		TLSHandshakeTimeout:    registrationTimeout,
		MaxResponseHeaderBytes: 16384,
	}
}

type boundedTransport struct{ transport *http.Transport }

// The vendored map decoder silently accepts duplicate JSON keys. Reject them
// before decoding so a later duplicate key cannot conceal an existing service
// or change the service name used by the duplicate preflight.
func validateCatalogJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value func(int) error
	value = func(depth int) error {
		if depth > 16 {
			return fmt.Errorf("candidate Hotel Consul catalog nesting exceeds bound")
		}
		token, err := decoder.Token()
		if err != nil || depth == 0 && token != json.Delim('{') {
			return fmt.Errorf("candidate Hotel Consul catalog JSON refused")
		}
		switch token {
		case json.Delim('{'):
			seen := map[string]bool{}
			for decoder.More() {
				key, err := decoder.Token()
				name, ok := key.(string)
				if err != nil || !ok || seen[name] {
					return fmt.Errorf("candidate Hotel Consul catalog duplicate or invalid key")
				}
				seen[name] = true
				if err = value(depth + 1); err != nil {
					return err
				}
			}
			if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
				return fmt.Errorf("candidate Hotel Consul catalog object incomplete")
			}
		case json.Delim('['):
			for decoder.More() {
				if err = value(depth + 1); err != nil {
					return err
				}
			}
			if token, err = decoder.Token(); err != nil || token != json.Delim(']') {
				return fmt.Errorf("candidate Hotel Consul catalog array incomplete")
			}
		case json.Delim('}'), json.Delim(']'):
			return fmt.Errorf("candidate Hotel Consul catalog delimiter refused")
		}
		return nil
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("candidate Hotel Consul catalog trailing data refused")
	}
	return nil
}

func (b boundedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	// No caller, redirect or ambient configuration may change the fixed agent.
	if request.URL.Scheme != "http" || request.URL.Host != consulAddress || request.URL.User != nil ||
		request.Header.Get("Authorization") != "" || request.Header.Get("X-Consul-Token") != "" {
		return nil, fmt.Errorf("candidate Hotel Consul request identity refused")
	}
	response, err := b.transport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxCatalogBytes+1))
	if err != nil || len(raw) > maxCatalogBytes {
		return nil, fmt.Errorf("candidate Hotel Consul response exceeds bound or is incomplete")
	}
	if request.Method == "GET" && request.URL.Path == "/v1/agent/services" && response.StatusCode == http.StatusOK {
		if err := validateCatalogJSON(raw); err != nil {
			return nil, err
		}
	}
	response.Body = io.NopCloser(bytes.NewReader(raw))
	return response, nil
}

func newCandidateClient(transport *http.Transport) (*Client, error) {
	if consulEnvironment() != nil || transport == nil {
		return nil, fmt.Errorf("candidate Hotel Consul configuration refused")
	}
	httpClient := &http.Client{
		Transport: boundedTransport{transport: transport},
		Timeout:   registrationTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return fmt.Errorf("candidate Hotel Consul redirect refused")
		},
	}
	// The vendored client reads defaults even for a non-nil config; environment
	// denial above is therefore required as well as these explicit fixed fields.
	client, err := consul.NewClient(&consul.Config{
		Address: consulAddress, Scheme: "http", Transport: transport, HttpClient: httpClient,
	})
	if err != nil {
		return nil, fmt.Errorf("candidate Hotel Consul configuration refused")
	}
	return &Client{Client: client, guard: &clientGuard{client: client}}, nil
}

// NewClient creates a bounded fixed-agent client without opening a connection.
// No credential, target, namespace, TLS or proxy environment fallback is used.
func NewClient(address string) (*Client, error) {
	if address != consulAddress {
		return nil, fmt.Errorf("candidate Hotel Consul endpoint refused")
	}
	return newCandidateClient(candidateTransport())
}

func ServiceID(serviceName string) (string, error) { return startup.ServiceID(serviceName) }

func servicePort(component string) int {
	return map[string]int{
		"profile": 8081, "search": 8082, "geo": 8083, "rate": 8084,
		"recommendation": 8085, "user": 8086, "reservation": 8087,
		"review": 8088, "attractions": 8089,
	}[component]
}

// Register performs a bounded read-before-write duplicate preflight followed by
// at most one registration request. It is NOT an atomic claim, distributed lock,
// exactly-once guarantee, health check or proof of a continuously live catalog.
// Existing same-name or same-ID entries (even exact duplicates) are never adopted,
// repaired, removed or overwritten intentionally. Uncertainty is cleanup-only.
func (c *Client) Register(name, id, ip string, port int) error {
	if c == nil || c.Client == nil || c.guard == nil || c.guard.client != c.Client ||
		len(c.Client.Headers()) != 0 || consulEnvironment() != nil {
		return fmt.Errorf("candidate Hotel Consul client refused")
	}
	identity, err := startup.CurrentIdentity()
	if err != nil {
		return fmt.Errorf("candidate Hotel Consul startup identity refused")
	}
	expectedID, err := startup.ServiceID(name)
	expectedPort := servicePort(identity.Component)
	if err != nil || expectedPort == 0 || name != "srv-"+identity.Component || id != expectedID ||
		ip != identity.PodIP || port != expectedPort {
		return fmt.Errorf("candidate Hotel Consul service identity refused")
	}
	c.guard.mutex.Lock()
	if c.guard.attempted {
		c.guard.mutex.Unlock()
		return fmt.Errorf("candidate Hotel Consul registration replay refused")
	}
	// Consume the local attempt before the first remote request. This is not a
	// durable or cross-client latch and cannot establish controller authority.
	c.guard.attempted = true
	c.guard.mutex.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), registrationTimeout)
	defer cancel()
	services, err := c.Agent().ServicesWithFilterOpts("", (&consul.QueryOptions{}).WithContext(ctx))
	if err != nil || services == nil || len(services) > 9 {
		return fmt.Errorf("candidate Hotel Consul duplicate preflight refused")
	}
	for key, service := range services {
		if service == nil || key == "" || service.ID != key || service.ID == id || service.Service == name {
			return fmt.Errorf("candidate Hotel Consul existing or invalid service refused")
		}
	}
	registration := &consul.AgentServiceRegistration{
		ID: id, Name: name, Address: ip, Port: port,
		Meta: map[string]string{
			"run_id": identity.RunID, "startup_attempt_id": identity.StartupAttemptID,
			"pod_uid": identity.PodUID, "contract_sha256": identity.ContractSHA256,
			"component": identity.Component,
		},
	}
	if c.Agent().ServiceRegisterOpts(registration, (consul.ServiceRegisterOpts{}).WithContext(ctx)) != nil {
		// A failed/lost acknowledgement cannot establish whether this one PUT
		// took effect. Never retry, repair or register again automatically.
		return fmt.Errorf("candidate Hotel Consul registration outcome uncertain")
	}
	return nil
}

// Deregister deliberately refuses in this baseline-only checkpoint. Whole-run
// guarded infrastructure cleanup is required; graceful restart, replacement and
// application-driven catalog repair are not implemented or qualified here.
func (c *Client) Deregister(id string) error {
	return fmt.Errorf("candidate Hotel Consul deregistration refused; cleanup-only")
}
