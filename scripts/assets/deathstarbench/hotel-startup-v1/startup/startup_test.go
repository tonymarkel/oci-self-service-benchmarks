package startup

// Source fixtures only. No image, service, MongoDB or cloud is executed here.
import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func cleanStartupEnvironment(t *testing.T) {
	t.Helper()
	for _, item := range os.Environ() {
		name := strings.SplitN(item, "=", 2)[0]
		upper := strings.ToUpper(name)
		if strings.HasPrefix(upper, "HOTEL_") || strings.HasPrefix(upper, "CONSUL_") || strings.HasPrefix(upper, "JAEGER_") ||
			strings.HasPrefix(upper, "MONGO") || strings.Contains(upper, "PROXY") || strings.HasPrefix(upper, "TLS") || strings.HasPrefix(upper, "DSB_") {
			t.Setenv(name, "")
			if err := os.Unsetenv(name); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func startupEnvironment(t *testing.T) {
	t.Helper()
	cleanStartupEnvironment(t)
	dataset, err := compiledDataset()
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"TLS": "false", "HOTEL_CANDIDATE_RUN_ID": "abcdef123456",
		"HOTEL_CANDIDATE_CONTRACT_SHA256":    dataset.Fingerprint(),
		"HOTEL_CANDIDATE_STARTUP_ATTEMPT_ID": "11111111-1111-4111-8111-111111111111",
		"HOTEL_CANDIDATE_COMPONENT":          "geo", "HOTEL_CANDIDATE_POD_UID": "22222222-2222-4222-8222-222222222222",
		"HOTEL_CANDIDATE_POD_IP": "10.42.1.7"} {
		t.Setenv(name, value)
	}
}

func startupDirectory(t *testing.T) string {
	t.Helper()
	before, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if err = os.Chdir(directory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(before) })
	return directory
}

func fixtureDigest(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func writeStartupConfig(t *testing.T, raw []byte) string {
	t.Helper()
	if err := os.WriteFile("config.json", raw, 0600); err != nil {
		t.Fatal(err)
	}
	return fixtureDigest(raw)
}

func TestStartupPreflightRequiresExactIdentityAndDetachedFixedConfig(t *testing.T) {
	startupEnvironment(t)
	startupDirectory(t)
	raw, _ := json.Marshal(expectedConfig())
	digest := writeStartupConfig(t, raw)
	result, err := Preflight("geo", digest, nil)
	if err != nil || result["GeoMongoAddress"] != "mongodb-geo:27017" {
		t.Fatal("valid source preflight failed", err)
	}
	result["GeoMongoAddress"] = "foreign:27017"
	again, err := Preflight("geo", "sha256:"+digest, nil)
	if err != nil || again["GeoMongoAddress"] != "mongodb-geo:27017" {
		t.Fatal("config map was not detached", err)
	}
	id, err := ServiceID("srv-geo")
	if err != nil || !strings.Contains(id, os.Getenv("HOTEL_CANDIDATE_POD_UID")) || !strings.Contains(id, os.Getenv("HOTEL_CANDIDATE_STARTUP_ATTEMPT_ID")) {
		t.Fatal("registration ID is not exact Pod/attempt bound", err)
	}
	if _, err = ServiceID("srv-user"); err == nil {
		t.Fatal("crossed service name accepted")
	}
	if _, err = Preflight("user", digest, nil); err == nil {
		t.Fatal("crossed runtime component accepted")
	}
}

func TestStartupRejectsAllArgumentsIdentityAndEnvironmentOverridesBeforeConfig(t *testing.T) {
	startupEnvironment(t)
	startupDirectory(t) // Deliberately contains no config.
	for _, args := range [][]string{{"--consuladdr=foreign:8500"}, {"--jaegerAddr", "foreign:6831"}, {"positional"}, {"--candidate-startup-attestation=1"}} {
		if _, err := Preflight("geo", strings.Repeat("a", 64), args); err == nil || !strings.Contains(err.Error(), "arguments") {
			t.Fatal("runtime arguments did not fail before file I/O", args, err)
		}
	}
	for name, value := range map[string]string{"HOTEL_CANDIDATE_RUN_ID": "000000000000", "HOTEL_CANDIDATE_CONTRACT_SHA256": "sha256:" + strings.Repeat("a", 64),
		"HOTEL_CANDIDATE_STARTUP_ATTEMPT_ID": "00000000-0000-0000-0000-000000000000", "HOTEL_CANDIDATE_POD_UID": "NOT-A-UUID",
		"HOTEL_CANDIDATE_COMPONENT": "mongodb-geo", "HOTEL_CANDIDATE_POD_IP": "8.8.8.8", "TLS": "true"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, value)
			if _, err := CurrentIdentity(); err == nil {
				t.Fatal("invalid runtime identity accepted", name)
			}
		})
	}
	for _, name := range []string{"CONSUL_HTTP_ADDR", "CONSUL_HTTP_TOKEN", "http_proxy", "NO_PROXY", "JAEGER_AGENT_HOST", "MONGODB_URI", "TLS_CONFIG", "DSB_GRPC_NETWORK", "HOTEL_ENDPOINT"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "") // Presence alone is forbidden, even an empty override.
			if _, err := CurrentIdentity(); err == nil {
				t.Fatal("environment override accepted", name)
			}
		})
	}
	for _, value := range []string{"10.42.01.7", "127.0.0.1", "::1", "172.16.0.1:8080", "169.254.1.1"} {
		t.Setenv("HOTEL_CANDIDATE_POD_IP", value)
		if _, err := CurrentIdentity(); err == nil {
			t.Fatal("noncanonical/nonprivate Pod IP accepted", value)
		}
	}
}

func TestStartupTracingRatioIsUnsetOrExactlyPinnedOnly(t *testing.T) {
	startupEnvironment(t)
	if _, err := CurrentIdentity(); err != nil {
		t.Fatal("unset sampling ratio must retain pinned tracer default", err)
	}
	t.Setenv("JAEGER_SAMPLE_RATIO", "0.01")
	if _, err := CurrentIdentity(); err != nil {
		t.Fatal("exact pinned sampling ratio was refused", err)
	}
	for _, value := range []string{"", "0", "0.010", "0.1", "NaN", "1"} {
		t.Setenv("JAEGER_SAMPLE_RATIO", value)
		if _, err := CurrentIdentity(); err == nil {
			t.Fatal("changed sampling ratio accepted", value)
		}
	}
}

func TestStartupConfigRejectsRehashedDuplicateUnknownMissingAndTypedContent(t *testing.T) {
	startupEnvironment(t)
	startupDirectory(t)
	valid, _ := json.Marshal(expectedConfig())
	for _, raw := range [][]byte{[]byte(`{"consulAddress":"consul:8500","consulAddress":"consul:8500"}`),
		[]byte(`{"unknown":"field"}`), []byte(`{"FrontendPort":5000}`), []byte(`{}`), append(valid, []byte("{}")...), []byte("null")} {
		digest := writeStartupConfig(t, raw)
		if _, err := Preflight("geo", digest, nil); err == nil {
			t.Fatal("rehashed invalid config accepted", string(raw))
		}
	}
	digest := writeStartupConfig(t, valid)
	if _, err := Preflight("geo", strings.Repeat("a", 64), nil); err == nil {
		t.Fatal("config SHA mismatch accepted")
	}
	if err := os.WriteFile("config.json", bytes.Repeat([]byte("x"), maxConfigBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Preflight("geo", digest, nil); err == nil {
		t.Fatal("oversized config accepted")
	}
}

func TestStartupConfigRejectsMissingDirectorySymlinkAndFIFOWithoutBlocking(t *testing.T) {
	startupEnvironment(t)
	directory := startupDirectory(t)
	digest := strings.Repeat("a", 64)
	if _, err := Preflight("geo", digest, nil); err == nil {
		t.Fatal("missing config accepted")
	}
	if err := os.Mkdir("config.json", 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := Preflight("geo", digest, nil); err == nil {
		t.Fatal("directory config accepted")
	}
	if err := os.Remove("config.json"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("target.json", []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(directory, "target.json"), "config.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := Preflight("geo", digest, nil); err == nil {
		t.Fatal("linked config accepted")
	}
	if err := os.Remove("config.json"); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo("config.json", 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Preflight("geo", digest, nil); err == nil {
		t.Fatal("FIFO config accepted")
	}
}

func TestStartupAttestationIsPureMetadataWithEveryGateClosed(t *testing.T) {
	cleanStartupEnvironment(t)
	startupDirectory(t)
	t.Setenv("TLS", "true")
	t.Setenv("HTTP_PROXY", "ignored-for-pure-metadata")
	var output bytes.Buffer
	handled, err := Attest(&output, []string{"--candidate-startup-attestation"}, "geo", strings.Repeat("a", 64))
	if err != nil || !handled {
		t.Fatal("metadata required runtime environment/config", err)
	}
	var value map[string]interface{}
	if json.Unmarshal(output.Bytes(), &value) != nil || value["recipe_revision"] != RecipeRevision || value["policy_revision"] != PolicyRevision {
		t.Fatal("metadata identity differs")
	}
	for _, name := range []string{"released", "initialization_executed", "database_executed", "application_started", "application_start_allowed", "application_ready",
		"global_readonly_verification_completed", "dataset_qualified", "runtime_qualified", "measurement_qualified", "cloud_qualified", "public_release",
		"initializer_authorized", "runtime_authorized", "qualification_authority", "database_execution_verified", "anonymous_pull_qualified"} {
		if value[name] != false {
			t.Fatal("metadata promoted gate", name)
		}
	}
	for _, args := range [][]string{{"--candidate-startup-attestation=1"}, {"--candidate-startup-attestation", "extra"}} {
		if _, err = Attest(&output, args, "geo", strings.Repeat("a", 64)); err == nil {
			t.Fatal("malformed reserved attestation accepted")
		}
	}
}

func TestStartupDecodeAndDisconnectedPreloadsFailWithoutPartialResults(t *testing.T) {
	startupEnvironment(t)
	row, _ := bson.Marshal(bson.D{{Key: "hotelId", Value: "1"}})
	var out []struct {
		HotelID string `bson:"hotelId"`
	}
	if err := decodeRows([]bson.Raw{row}, &out); err != nil || len(out) != 1 || out[0].HotelID != "1" {
		t.Fatal("verified typed row decode failed", err)
	}
	if err := decodeRows([]bson.Raw{}, &out); err != nil || out == nil || len(out) != 0 {
		t.Fatal("verified empty rows must clear stale output", err)
	}
	var incompatible []struct {
		HotelID int `bson:"hotelId"`
	}
	if err := decodeRows([]bson.Raw{row}, &incompatible); err == nil || len(incompatible) != 0 {
		t.Fatal("typed decode error was ignored")
	}
	if err := decodeRows([]bson.Raw{row}, nil); err == nil {
		t.Fatal("nil output accepted")
	}
	if err := LoadRows(nil, "geo", "geo", &out); err == nil {
		t.Fatal("nil preload client accepted")
	}
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://mongodb-geo:27017"))
	if err != nil {
		t.Fatal(err)
	}
	// The client was never connected, so this exercises no network operation.
	if err = LoadRows(client, "geo", "geo", &out); err == nil {
		t.Fatal("disconnected preload client accepted")
	}
}
