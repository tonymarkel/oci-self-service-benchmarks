// Package startup is a source-level fail-closed contract, not live readiness,
// global verification, controller authority or a release capability.
package startup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/initialization"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/initialization/catalog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const RecipeRevision = "hotel-reservation-guarded-startup-native-go-scratch-candidate-v1"
const PolicyRevision = "hotel-guarded-startup-source-policy-v1"
const maxConfigBytes = 65536

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var componentPorts = map[string]int{"attractions": 8089, "frontend": 5000, "geo": 8083,
	"profile": 8081, "rate": 8084, "recommendation": 8085, "reservation": 8087,
	"review": 8088, "search": 8082, "user": 8086}

func refused(message string) error { return errors.New("hotel candidate startup: " + message) }
func validUUID(value string) bool {
	return uuidPattern.MatchString(value) && value != "00000000-0000-0000-0000-000000000000"
}
func configDigest(value string) (string, error) {
	value = strings.TrimPrefix(value, "sha256:")
	if !digestPattern.MatchString(value) || value == strings.Repeat("0", 64) {
		return "", refused("invalid compiled config identity")
	}
	return value, nil
}
func compiledDataset() (*initialization.Dataset, error) {
	return initialization.BuildDataset(catalog.Collections(), catalog.SourceFingerprint)
}

// Identity contains explicit caller assertions only. It does not prove Pod,
// namespace, cluster, storage, controller ownership or freshness.
type Identity struct {
	RunID, ContractSHA256, StartupAttemptID, Component, PodUID, PodIP string
}

func rejectOverrides() error {
	allowed := map[string]bool{"HOTEL_CANDIDATE_RUN_ID": true, "HOTEL_CANDIDATE_CONTRACT_SHA256": true,
		"HOTEL_CANDIDATE_STARTUP_ATTEMPT_ID": true, "HOTEL_CANDIDATE_COMPONENT": true,
		"HOTEL_CANDIDATE_POD_UID": true, "HOTEL_CANDIDATE_POD_IP": true}
	for _, item := range os.Environ() {
		pair := strings.SplitN(item, "=", 2)
		name := pair[0]
		upper := strings.ToUpper(name)
		fixedSampling := name == "JAEGER_SAMPLE_RATIO" && len(pair) == 2 && pair[1] == "0.01"
		if strings.HasPrefix(upper, "CONSUL_") || (strings.HasPrefix(upper, "JAEGER_") && !fixedSampling) ||
			strings.HasPrefix(upper, "MONGO") || strings.HasSuffix(upper, "_PROXY") || upper == "PROXY" ||
			strings.HasPrefix(upper, "DSB_") || strings.HasPrefix(upper, "TLS_") ||
			(strings.HasPrefix(upper, "HOTEL_") && !allowed[name]) {
			return refused("endpoint, proxy, registry or TLS environment override is forbidden")
		}
	}
	if os.Getenv("TLS") != "false" {
		return refused("runtime requires explicit TLS=false")
	}
	return nil
}

// CurrentIdentity validates all mandatory identity before any I/O. Run IDs
// retain the original guarded twelve-hex contract; attempt and Pod IDs are UUIDs.
func CurrentIdentity() (Identity, error) {
	value := Identity{os.Getenv("HOTEL_CANDIDATE_RUN_ID"), os.Getenv("HOTEL_CANDIDATE_CONTRACT_SHA256"),
		os.Getenv("HOTEL_CANDIDATE_STARTUP_ATTEMPT_ID"), os.Getenv("HOTEL_CANDIDATE_COMPONENT"),
		os.Getenv("HOTEL_CANDIDATE_POD_UID"), os.Getenv("HOTEL_CANDIDATE_POD_IP")}
	if err := rejectOverrides(); err != nil {
		return Identity{}, err
	}
	base, err := initialization.IdentityFromEnv()
	if err != nil || !validUUID(value.StartupAttemptID) || !validUUID(value.PodUID) || value.StartupAttemptID == value.PodUID {
		return Identity{}, refused("mandatory run, attempt or Pod identity is invalid")
	}
	if _, ok := componentPorts[value.Component]; !ok {
		return Identity{}, refused("mandatory component identity is invalid")
	}
	ip := net.ParseIP(value.PodIP)
	if ip == nil || ip.To4() == nil || ip.String() != value.PodIP || !ip.IsPrivate() {
		return Identity{}, refused("Pod IP must be a canonical private IPv4 address")
	}
	dataset, err := compiledDataset()
	if err != nil || base.ContractSHA256 != dataset.Fingerprint() {
		return Identity{}, refused("identity differs from the compiled dataset")
	}
	return value, nil
}

// ServiceID binds an exact upstream service name to the explicit attempt/Pod.
func ServiceID(serviceName string) (string, error) {
	identity, err := CurrentIdentity()
	if err != nil {
		return "", err
	}
	if serviceName != "srv-"+identity.Component || identity.Component == "frontend" {
		return "", refused("service name differs from its exact component")
	}
	return "hotel-" + identity.RunID + "-" + identity.StartupAttemptID + "-" + identity.Component + "-" + identity.PodUID, nil
}

// ServicePort is a fixed component mapping, never a configurable endpoint.
func ServicePort(component string) (int, error) {
	port, ok := componentPorts[component]
	if !ok {
		return 0, refused("unknown service component")
	}
	return port, nil
}

func expectedConfig() map[string]string {
	return map[string]string{"consulAddress": "consul:8500", "jaegerAddress": "jaeger:6831",
		"FrontendPort": "5000", "GeoPort": "8083", "GeoMongoAddress": "mongodb-geo:27017",
		"ProfilePort": "8081", "ProfileMongoAddress": "mongodb-profile:27017", "ProfileMemcAddress": "memcached-profile:11211",
		"ReviewPort": "8088", "ReviewMongoAddress": "mongodb-review:27017", "ReviewMemcAddress": "memcached-review:11211",
		"AttractionsPort": "8089", "AttractionsMongoAddress": "mongodb-attractions:27017",
		"RatePort": "8084", "RateMongoAddress": "mongodb-rate:27017", "RateMemcAddress": "memcached-rate:11211",
		"RecommendPort": "8085", "RecommendMongoAddress": "mongodb-recommendation:27017",
		"ReservePort": "8087", "ReserveMongoAddress": "mongodb-reservation:27017", "ReserveMemcAddress": "memcached-reserve:11211",
		"SearchPort": "8082", "UserPort": "8086", "UserMongoAddress": "mongodb-user:27017", "KnativeDomainName": ""}
}

func parseConfig(raw []byte) (map[string]string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, refused("config must be one exact object")
	}
	result, wanted := map[string]string{}, expectedConfig()
	for decoder.More() {
		key, err := decoder.Token()
		name, valid := key.(string)
		if err != nil || !valid {
			return nil, refused("config field is invalid")
		}
		if _, duplicate := result[name]; duplicate {
			return nil, refused("config repeats a field")
		}
		var value string
		if decoder.Decode(&value) != nil {
			return nil, refused("config values must be strings")
		}
		if expected, ok := wanted[name]; !ok || value != expected {
			return nil, refused("config differs from fixed startup endpoints")
		}
		result[name] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || len(result) != len(wanted) {
		return nil, refused("config inventory is incomplete")
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, refused("config contains trailing data")
	}
	return result, nil
}

func readConfig(expected string) (map[string]string, error) {
	wanted, err := configDigest(expected)
	if err != nil {
		return nil, err
	}
	before, err := os.Lstat("config.json")
	if err != nil || !before.Mode().IsRegular() || before.Size() < 1 || before.Size() > maxConfigBytes {
		return nil, refused("bounded regular baked config is required")
	}
	fd, err := syscall.Open("config.json", syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, refused("baked config cannot be opened safely")
	}
	file := os.NewFile(uintptr(fd), "config.json")
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) || opened.Size() != before.Size() || !opened.ModTime().Equal(before.ModTime()) {
		return nil, refused("baked config changed before reading")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil || len(raw) != int(opened.Size()) || len(raw) > maxConfigBytes {
		return nil, refused("baked config read is incomplete or oversized")
	}
	after, err := file.Stat()
	named, namedErr := os.Lstat("config.json")
	if err != nil || namedErr != nil || !os.SameFile(opened, after) || !os.SameFile(after, named) || !named.Mode().IsRegular() ||
		after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) || !named.ModTime().Equal(after.ModTime()) {
		return nil, refused("baked config changed while reading")
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != wanted {
		return nil, refused("baked config digest differs")
	}
	return parseConfig(raw)
}

// Preflight rejects every runtime flag and all overrides before config or
// network I/O. The resulting detached map is not live startup authority.
func Preflight(component, expectedConfigSHA string, args []string) (map[string]string, error) {
	if len(args) != 0 {
		return nil, refused("runtime arguments are forbidden")
	}
	identity, err := CurrentIdentity()
	if err != nil || identity.Component != component {
		return nil, refused("runtime component or identity differs")
	}
	return readConfig(expectedConfigSHA)
}

// Attest emits pure metadata before identity, TLS, config or network checks.
// It explicitly does not assert that an application was started or is ready.
func Attest(writer io.Writer, args []string, component, expectedConfigSHA string) (bool, error) {
	found := false
	for _, arg := range args {
		found = found || arg == "--candidate-startup-attestation" || strings.HasPrefix(arg, "--candidate-startup-attestation=")
	}
	if !found {
		return false, nil
	}
	digest, err := configDigest(expectedConfigSHA)
	_, known := componentPorts[component]
	if err != nil || !known || writer == nil || len(args) != 1 || args[0] != "--candidate-startup-attestation" {
		return true, refused("invalid metadata attestation request")
	}
	dataset, err := compiledDataset()
	if err != nil {
		return true, refused("compiled dataset is invalid")
	}
	value := map[string]interface{}{"schema_version": 1, "candidate_schema": "hotel-guarded-startup-source-attestation-v1",
		"recipe_revision": RecipeRevision, "policy_revision": PolicyRevision, "component": component,
		"config_sha256": "sha256:" + digest, "dataset_fingerprint": dataset.Fingerprint(),
		"source_fingerprint": catalog.SourceFingerprint, "candidate_only": true,
		"scope": "offline-source-contract-only-no-live-readiness-or-execution-authority"}
	for _, name := range []string{"released", "initialization_executed", "database_executed", "application_started",
		"application_start_allowed", "application_ready", "global_readonly_verification_completed", "dataset_qualified",
		"initializer_authorized", "runtime_authorized", "qualification_authority", "database_execution_verified",
		"runtime_qualified", "measurement_qualified", "cloud_qualified", "anonymous_pull_qualified", "public_release"} {
		value[name] = false
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return true, refused("metadata encoding failed")
	}
	if _, err = fmt.Fprintln(writer, string(raw)); err != nil {
		return true, refused("metadata output failed")
	}
	return true, nil
}

func decodeRows(rows []bson.Raw, out interface{}) error {
	value := reflect.ValueOf(out)
	if !value.IsValid() || value.Kind() != reflect.Ptr || value.IsNil() || value.Elem().Kind() != reflect.Slice {
		return refused("preload output must be a nonnil slice pointer")
	}
	result := reflect.MakeSlice(value.Elem().Type(), 0, len(rows))
	for _, raw := range rows {
		item := reflect.New(result.Type().Elem())
		if bson.Unmarshal(raw, item.Interface()) != nil {
			return refused("verified preload row decode failed")
		}
		result = reflect.Append(result, item.Elem())
	}
	value.Elem().Set(result)
	return nil
}

// LoadRows only decodes the detached rows from the same bounded snapshot that
// passed exact completed own-store verification. There is no fallback read/write.
func LoadRows(client *mongo.Client, component, collection string, out interface{}) error {
	output := reflect.ValueOf(out)
	if !output.IsValid() || output.Kind() != reflect.Ptr || output.IsNil() || output.Elem().Kind() != reflect.Slice {
		return refused("preload output must be a nonnil slice pointer")
	}
	identity, err := CurrentIdentity()
	if err != nil || identity.Component != component || client == nil {
		return refused("preload requires exact component identity and client")
	}
	dataset, err := compiledDataset()
	if err != nil {
		return refused("compiled preload dataset is invalid")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rows, err := initialization.VerifiedMongoStartupRows(ctx, client, component, collection,
		initialization.Identity{RunID: identity.RunID, ContractSHA256: identity.ContractSHA256}, dataset)
	if err != nil {
		return refused("bounded verified preload failed")
	}
	if ctx.Err() != nil {
		return refused("preload deadline expired")
	}
	return decodeRows(rows, out)
}
