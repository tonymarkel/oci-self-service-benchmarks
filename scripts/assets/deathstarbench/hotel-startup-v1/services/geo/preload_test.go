package geo

import (
	"os"
	"strings"
	"testing"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/initialization"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/initialization/catalog"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/startup"
	"github.com/hailocab/go-geoindex"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func candidatePreloadIdentity(t *testing.T) {
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
		"HOTEL_CANDIDATE_COMPONENT":          "geo", "HOTEL_CANDIDATE_POD_UID": "22222222-2222-4222-8222-222222222222",
		"HOTEL_CANDIDATE_POD_IP": "10.42.1.7",
	} {
		t.Setenv(key, value)
	}
	if _, err := startup.CurrentIdentity(); err != nil {
		t.Fatal("test startup identity refused")
	}
}

func TestCandidateGeoPreloadFailurePrecedesListenerAndRegistration(t *testing.T) {
	candidatePreloadIdentity(t)
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://mongodb-geo:27017"))
	if err != nil {
		t.Fatal(err)
	}
	// A nonzero invalid port would fail at net.Listen. A nil Registry would panic
	// at registration. Even a pre-populated index cannot bypass baseline preload.
	for _, candidate := range []*mongo.Client{nil, client} {
		server := &Server{Port: -1, MongoClient: candidate, index: geoindex.NewClusteringIndex()}
		if err := server.Run(); err == nil || err.Error() != "candidate Hotel geo preload refused" {
			t.Fatal("preload failure did not stop before listener/registration")
		}
	}
}

func TestCandidateGeoNilAndDisconnectedClientsDoNotLoadPartialIndex(t *testing.T) {
	candidatePreloadIdentity(t)
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://mongodb-geo:27017"))
	if err != nil {
		t.Fatal(err)
	}
	// NewClient does not Connect: this tests local disconnected-client failure,
	// without contacting a MongoDB server or opening any external socket.
	for _, candidate := range []*mongo.Client{nil, client} {
		if index, err := newGeoIndex(candidate); err == nil || index != nil {
			t.Fatal("failed preload produced an index")
		}
	}
}
