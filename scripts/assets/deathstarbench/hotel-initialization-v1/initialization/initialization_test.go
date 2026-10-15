package initialization_test

// These tests model storage semantics in memory. They do not contact MongoDB
// or qualify deployed databases, Consul health, Pod restart or applications.
import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

	hotelinit "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/initialization"
	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/initialization/catalog"
	"go.mongodb.org/mongo-driver/bson"
)

type fakeCluster struct {
	mu     sync.Mutex
	stores map[string]*fakeStore
	log    []string
	hook   func(string, string) error
}
type fakeStore struct {
	cluster   *fakeCluster
	component string
	rows      map[string][]bson.Raw
}

func copyRows(rows []bson.Raw) []bson.Raw {
	out := make([]bson.Raw, len(rows))
	for i, row := range rows {
		out[i] = append(bson.Raw(nil), row...)
	}
	return out
}
func newCluster(dataset *hotelinit.Dataset) (*fakeCluster, hotelinit.Stores) {
	c := &fakeCluster{stores: map[string]*fakeStore{}}
	stores := hotelinit.Stores{}
	for _, spec := range dataset.Metadata().Collections {
		if c.stores[spec.Component] == nil {
			store := &fakeStore{cluster: c, component: spec.Component, rows: map[string][]bson.Raw{}}
			c.stores[spec.Component] = store
			stores[spec.Component] = store
		}
	}
	return c, stores
}
func (s *fakeStore) Observe(ctx context.Context, database string, limits map[string]int) (hotelinit.Snapshot, error) {
	s.cluster.mu.Lock()
	defer s.cluster.mu.Unlock()
	if ctx.Err() != nil {
		return hotelinit.Snapshot{}, ctx.Err()
	}
	s.cluster.log = append(s.cluster.log, "observe:"+s.component)
	out := hotelinit.Snapshot{Collections: map[string][]bson.Raw{}}
	for name, rows := range s.rows {
		out.Collections[name] = copyRows(rows)
	}
	return out, nil
}
func (s *fakeStore) Claim(ctx context.Context, database string, row bson.Raw) error {
	s.cluster.mu.Lock()
	defer s.cluster.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.cluster.log = append(s.cluster.log, "claim:"+s.component)
	if _, exists := s.rows[hotelinit.MarkerCollection]; exists {
		return errors.New("claim conflict")
	}
	s.rows[hotelinit.MarkerCollection] = copyRows([]bson.Raw{row})
	if s.cluster.hook != nil {
		return s.cluster.hook("claim", s.component)
	}
	return nil
}
func (s *fakeStore) Insert(ctx context.Context, database, name string, rows []bson.Raw) error {
	s.cluster.mu.Lock()
	defer s.cluster.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.cluster.log = append(s.cluster.log, "insert:"+s.component+":"+name)
	if _, exists := s.rows[name]; exists {
		return errors.New("collection already exists")
	}
	s.rows[name] = copyRows(rows)
	if s.cluster.hook != nil {
		return s.cluster.hook("insert", s.component)
	}
	return nil
}
func (s *fakeStore) Complete(ctx context.Context, database string, before, after bson.Raw) error {
	s.cluster.mu.Lock()
	defer s.cluster.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.cluster.log = append(s.cluster.log, "complete:"+s.component)
	rows := s.rows[hotelinit.MarkerCollection]
	if len(rows) != 1 || !bytes.Equal(rows[0], before) {
		return errors.New("exact marker CAS conflict")
	}
	s.rows[hotelinit.MarkerCollection] = copyRows([]bson.Raw{after})
	if s.cluster.hook != nil {
		return s.cluster.hook("complete", s.component)
	}
	return nil
}
func dataset(t *testing.T) *hotelinit.Dataset {
	t.Helper()
	d, err := hotelinit.BuildDataset(catalog.Collections(), catalog.SourceFingerprint)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func identity(d *hotelinit.Dataset) hotelinit.Identity {
	return hotelinit.Identity{RunID: "0123456789ab", ContractSHA256: d.Fingerprint()}
}
func writeCount(c *fakeCluster) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, value := range c.log {
		if !strings.HasPrefix(value, "observe:") {
			n++
		}
	}
	return n
}
func cloneCatalog() []hotelinit.Collection {
	values := catalog.Collections()
	for i := range values {
		documents := []interface{}{}
		for _, value := range values[i].Documents {
			raw, _ := bson.Marshal(value)
			var doc bson.D
			_ = bson.Unmarshal(raw, &doc)
			documents = append(documents, doc)
		}
		values[i].Documents = documents
	}
	return values
}
func replace(doc bson.D, key string, value interface{}) bson.D {
	for i := range doc {
		if doc[i].Key == key {
			doc[i].Value = value
			return doc
		}
	}
	return append(doc, bson.E{Key: key, Value: value})
}
func mutateRow(s *fakeStore, name string, index int, callback func(bson.D) bson.D) {
	var doc bson.D
	_ = bson.Unmarshal(s.rows[name][index], &doc)
	raw, _ := bson.Marshal(callback(doc))
	s.rows[name][index] = raw
}

func TestDatasetExactInventoryDeterminismAndCopyIsolation(t *testing.T) {
	d := dataset(t)
	m := d.Metadata()
	if m.DocumentCount != 873 || m.CollectionCount != 11 || m.DatabaseCount != 8 || len(m.Collections) != 11 {
		t.Fatal("incomplete compiled baseline")
	}
	values := cloneCatalog()
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
	for i := range values {
		rows := values[i].Documents
		for left, right := 0, len(rows)-1; left < right; left, right = left+1, right-1 {
			rows[left], rows[right] = rows[right], rows[left]
		}
	}
	other, err := hotelinit.BuildDataset(values, catalog.SourceFingerprint)
	if err != nil || other.Fingerprint() != d.Fingerprint() {
		t.Fatal("canonical dataset depends on input order", err)
	}
	m.Collections[0].LogicalKeyFields[0] = "changed"
	m.Collections[0].ContentSHA256 = "changed"
	if d.Metadata().Collections[0].LogicalKeyFields[0] == "changed" || d.Metadata().Collections[0].ContentSHA256 == "changed" {
		t.Fatal("metadata aliases factory state")
	}
	if m.SourceFingerprint != catalog.SourceFingerprint || m.InitializationExecuted || m.DatasetQualified || m.RuntimeQualified || m.MeasurementQualified || m.ApplicationStartAllowed || m.PublicRelease {
		t.Fatal("offline metadata promoted execution")
	}
}

func TestOfflineCompiledCatalogGoldenIdentities(t *testing.T) {
	// Observed from the offline native catalog, not MongoDB execution, dataset
	// qualification or runtime qualification. The pinned builder must agree.
	const source = "sha256:40c76ecf1623f6e098d3839efc0d2341c416817c94e1c36a6b36a87d9647be49"
	const fingerprint = "sha256:76974701cecf121db01b511e34d1f543adb76b430146ca4022c1eb81f61d04ee"
	d := dataset(t)
	if catalog.SourceFingerprint != source || d.Metadata().SourceFingerprint != source || d.Fingerprint() != fingerprint {
		t.Fatal("offline compiled catalog identity differs from the reviewed golden baseline")
	}
}

func TestDatasetRejectsMissingDuplicateForeignKeysTypesAndFields(t *testing.T) {
	for _, kind := range []string{"missing-collection", "missing-row", "duplicate-logical", "foreign-key", "wrong-type", "extra-field", "nan", "wrong-password", "zero-source"} {
		t.Run(kind, func(t *testing.T) {
			values := cloneCatalog()
			source := catalog.SourceFingerprint
			switch kind {
			case "missing-collection":
				values = values[:10]
			case "missing-row":
				values[0].Documents = values[0].Documents[:5]
			case "duplicate-logical":
				doc := values[0].Documents[0].(bson.D)
				values[0].Documents[1] = replace(doc, "lat", float64(99))
			case "foreign-key":
				values[0].Documents[0] = replace(values[0].Documents[0].(bson.D), "hotelId", "foreign")
			case "wrong-type":
				values[0].Documents[0] = replace(values[0].Documents[0].(bson.D), "lat", "37")
			case "extra-field":
				values[0].Documents[0] = append(values[0].Documents[0].(bson.D), bson.E{Key: "foreign", Value: true})
			case "nan":
				values[0].Documents[0] = replace(values[0].Documents[0].(bson.D), "lat", math.NaN())
			case "wrong-password":
				for i := range values {
					if values[i].Component == "user" {
						values[i].Documents[0] = replace(values[i].Documents[0].(bson.D), "password", "wrong")
					}
				}
			case "zero-source":
				source = "sha256:" + strings.Repeat("0", 64)
			}
			if _, err := hotelinit.BuildDataset(values, source); err == nil {
				t.Fatal("invalid dataset accepted")
			}
		})
	}
}

func TestInitializeAllClaimsAndFullGlobalVerificationBeforeCompletion(t *testing.T) {
	d := dataset(t)
	c, stores := newCluster(d)
	claimed := map[string]bool{}
	inserted := 0
	c.hook = func(operation, component string) error {
		switch operation {
		case "claim":
			claimed[component] = true
		case "insert":
			if len(claimed) != 8 {
				t.Fatal("data write before all durable claims")
			}
			inserted++
		case "complete":
			if inserted != 11 {
				t.Fatal("completion before complete data inventory")
			}
			for _, spec := range d.Metadata().Collections {
				if len(c.stores[spec.Component].rows[spec.Name]) != spec.Count {
					t.Fatal("completion before global contents")
				}
			}
		}
		return nil
	}
	if err := hotelinit.Initialize(context.Background(), stores, identity(d), d); err != nil {
		t.Fatal(err)
	}
	before := writeCount(c)
	if err := hotelinit.Verify(context.Background(), stores, identity(d), d); err != nil {
		t.Fatal(err)
	}
	if writeCount(c) != before {
		t.Fatal("Verify performed writes")
	}
	if err := hotelinit.Initialize(context.Background(), stores, identity(d), d); err == nil {
		t.Fatal("completed initialization replayed")
	}
	if writeCount(c) != before {
		t.Fatal("replay changed complete state")
	}
}

func TestConcurrentInitializersHaveOnlyOneAtomicClaimWinner(t *testing.T) {
	d := dataset(t)
	_, stores := newCluster(d)
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { <-start; results <- hotelinit.Initialize(context.Background(), stores, identity(d), d) }()
	}
	close(start)
	a, b := <-results, <-results
	if (a == nil) == (b == nil) {
		t.Fatalf("expected exactly one winner, got %v / %v", a, b)
	}
	if err := hotelinit.Verify(context.Background(), stores, identity(d), d); err != nil {
		t.Fatal(err)
	}
}

func TestCancellationAndLostClaimOrDataResponseNeverReplay(t *testing.T) {
	for _, failure := range []string{"cancel", "claim", "insert"} {
		t.Run(failure, func(t *testing.T) {
			d := dataset(t)
			c, stores := newCluster(d)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fired := false
			c.hook = func(operation, component string) error {
				if !fired && ((failure == "cancel" && operation == "claim") || operation == failure) {
					fired = true
					if failure == "cancel" {
						cancel()
						return nil
					}
					return errors.New("SECRET mongodb://credentials response lost")
				}
				return nil
			}
			if err := hotelinit.Initialize(ctx, stores, identity(d), d); err == nil || strings.Contains(err.Error(), "SECRET") {
				t.Fatal("failure not safely surfaced")
			}
			before := writeCount(c)
			c.hook = nil
			if err := hotelinit.Initialize(context.Background(), stores, identity(d), d); err == nil {
				t.Fatal("pending/partial state replayed")
			}
			if err := hotelinit.Verify(context.Background(), stores, identity(d), d); err == nil {
				t.Fatal("partial state qualified")
			}
			if writeCount(c) != before {
				t.Fatal("partial-state recovery wrote data")
			}
		})
	}
}

func TestLostLastCompletionResponseRecognizesOnlyReadOnlyFullCompletion(t *testing.T) {
	d := dataset(t)
	c, stores := newCluster(d)
	c.hook = func(operation, component string) error {
		if operation == "complete" && component == "user" {
			return errors.New("response lost")
		}
		return nil
	}
	if err := hotelinit.Initialize(context.Background(), stores, identity(d), d); err == nil {
		t.Fatal("lost response reported as success")
	}
	before := writeCount(c)
	c.hook = nil
	if err := hotelinit.Verify(context.Background(), stores, identity(d), d); err != nil {
		t.Fatal("read-only completion recognition failed", err)
	}
	if err := hotelinit.Initialize(context.Background(), stores, identity(d), d); err == nil {
		t.Fatal("complete state replayed")
	}
	if writeCount(c) != before {
		t.Fatal("recognition performed a write")
	}
}

func TestPreexistingDataEmptyCollectionAndForeignMarkerRefuseBeforeAnyClaim(t *testing.T) {
	for _, kind := range []string{"data", "empty-collection", "marker", "unexpected-collection"} {
		t.Run(kind, func(t *testing.T) {
			d := dataset(t)
			c, stores := newCluster(d)
			s := c.stores["user"]
			name := "user"
			if kind == "marker" {
				name = hotelinit.MarkerCollection
			}
			if kind == "unexpected-collection" {
				name = "foreign"
			}
			s.rows[name] = []bson.Raw{}
			if kind == "data" || kind == "marker" {
				row, _ := bson.Marshal(bson.D{{Key: "_id", Value: "foreign"}})
				s.rows[name] = []bson.Raw{row}
			}
			if err := hotelinit.Initialize(context.Background(), stores, identity(d), d); err == nil {
				t.Fatal("foreign/preexisting state adopted")
			}
			if writeCount(c) != 0 {
				t.Fatal("claimed an unowned store")
			}
		})
	}
}

func TestVerifyRejectsMissingDuplicateChangedTypedRowsAndMarkerFields(t *testing.T) {
	for _, kind := range []string{"missing", "duplicate", "changed", "extra-field", "typed-number", "marker-foreign", "marker-extra", "marker-pending", "unexpected-collection"} {
		t.Run(kind, func(t *testing.T) {
			d := dataset(t)
			c, stores := newCluster(d)
			if err := hotelinit.Initialize(context.Background(), stores, identity(d), d); err != nil {
				t.Fatal(err)
			}
			before := writeCount(c)
			s := c.stores["user"]
			switch kind {
			case "missing":
				delete(s.rows, "user")
			case "duplicate":
				s.rows["user"][1] = append(bson.Raw(nil), s.rows["user"][0]...)
			case "changed":
				mutateRow(s, "user", 0, func(doc bson.D) bson.D { return replace(doc, "password", "changed") })
			case "extra-field":
				mutateRow(s, "user", 0, func(doc bson.D) bson.D { return append(doc, bson.E{Key: "foreign", Value: true}) })
			case "typed-number":
				mutateRow(c.stores["reservation"], "reservation", 0, func(doc bson.D) bson.D { return replace(doc, "number", int64(1)) })
			case "marker-foreign":
				mutateRow(s, hotelinit.MarkerCollection, 0, func(doc bson.D) bson.D { return replace(doc, "run_id", "abcdef123456") })
			case "marker-extra":
				mutateRow(s, hotelinit.MarkerCollection, 0, func(doc bson.D) bson.D { return append(doc, bson.E{Key: "foreign", Value: true}) })
			case "marker-pending":
				mutateRow(s, hotelinit.MarkerCollection, 0, func(doc bson.D) bson.D { return replace(doc, "state", "claimed") })
			case "unexpected-collection":
				s.rows["unknown"] = []bson.Raw{}
			}
			if err := hotelinit.Verify(context.Background(), stores, identity(d), d); err == nil {
				t.Fatal("changed baseline accepted")
			}
			if writeCount(c) != before {
				t.Fatal("Verify repaired state")
			}
		})
	}
}

func TestOwnStoreVerificationIsReadOnlyAndDoesNotInspectOtherServices(t *testing.T) {
	d := dataset(t)
	c, stores := newCluster(d)
	if err := hotelinit.Initialize(context.Background(), stores, identity(d), d); err != nil {
		t.Fatal(err)
	}
	before := writeCount(c)
	c.log = nil
	delete(c.stores["geo"].rows, "geo")
	if err := hotelinit.VerifyComponent(context.Background(), stores["user"], "user", identity(d), d); err != nil {
		t.Fatal(err)
	}
	if len(c.log) != 1 || c.log[0] != "observe:user" {
		t.Fatal("own-store verifier crossed service boundary")
	}
	if writeCount(c) != 0 {
		t.Fatal("service verification writes")
	}
	_ = before
}

func TestCrossedDatasetIdentityRefusesBeforeAnyIO(t *testing.T) {
	d := dataset(t)
	c, stores := newCluster(d)
	other := identity(d)
	other.ContractSHA256 = "sha256:" + strings.Repeat("c", 64)
	if other.ContractSHA256 == d.Fingerprint() {
		t.Fatal("test must supply a distinct valid digest")
	}
	if err := hotelinit.Initialize(context.Background(), stores, other, d); err == nil {
		t.Fatal("initializer accepted another dataset identity")
	}
	if err := hotelinit.Verify(context.Background(), stores, other, d); err == nil {
		t.Fatal("global verifier accepted another dataset identity")
	}
	if err := hotelinit.VerifyComponent(context.Background(), stores["user"], "user", other, d); err == nil {
		t.Fatal("service verifier accepted another dataset identity")
	}
	client, closeClient, err := hotelinit.ConnectAndVerify(context.Background(), "user", "mongodb-user:27017", other, d)
	if err == nil || client != nil || closeClient != nil {
		t.Fatal("startup connected for another dataset identity")
	}
	if len(c.log) != 0 {
		t.Fatal("crossed identity performed store reads or writes")
	}
}

func TestInitializationStopsOnClaimOrEarlierContentDriftBeforeFurtherWrites(t *testing.T) {
	for _, kind := range []string{"later-claim", "earlier-collection"} {
		t.Run(kind, func(t *testing.T) {
			d := dataset(t)
			c, stores := newCluster(d)
			fired := false
			c.hook = func(operation, component string) error {
				if !fired && operation == "insert" {
					fired = true
					if kind == "later-claim" {
						mutateRow(c.stores["geo"], hotelinit.MarkerCollection, 0, func(doc bson.D) bson.D { return replace(doc, "run_id", "abcdef123456") })
					} else {
						mutateRow(c.stores[component], "hotels", 0, func(doc bson.D) bson.D { return replace(doc, "lat", float64(99)) })
					}
				}
				return nil
			}
			if err := hotelinit.Initialize(context.Background(), stores, identity(d), d); err == nil {
				t.Fatal("initialization continued through observed drift")
			}
			if !fired {
				t.Fatal("drift hook did not execute")
			}
			for _, entry := range c.log {
				if strings.HasPrefix(entry, "complete:") || (kind == "later-claim" && strings.HasPrefix(entry, "insert:geo:")) || (kind == "earlier-collection" && entry == "insert:attractions:museums") {
					t.Fatal("write followed observed foreign marker/content", entry)
				}
			}
		})
	}
}

func TestContextIdentityAndOfflineAttestationHaveNoFallback(t *testing.T) {
	d := dataset(t)
	c, stores := newCluster(d)
	bad := []hotelinit.Identity{{}, {RunID: "000000000000", ContractSHA256: identity(d).ContractSHA256}, {RunID: "0123456789ab", ContractSHA256: "sha256:" + strings.Repeat("0", 64)}}
	for _, value := range bad {
		if err := hotelinit.Initialize(context.Background(), stores, value, d); err == nil {
			t.Fatal("bad identity accepted")
		}
		client, closeClient, err := hotelinit.ConnectAndVerify(context.Background(), "user", "mongodb-user:27017", value, d)
		if err == nil || client != nil || closeClient != nil {
			t.Fatal("startup connected without mandatory identity")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := hotelinit.Initialize(ctx, stores, identity(d), d); err == nil {
		t.Fatal("canceled operation ran")
	}
	if writeCount(c) != 0 {
		t.Fatal("invalid input performed writes")
	}
	var output bytes.Buffer
	handled, err := hotelinit.Attest(&output, []string{"--candidate-initialization-attestation"}, d)
	if !handled || err != nil || !strings.Contains(output.String(), d.Fingerprint()) {
		t.Fatal("offline attestation failed", err)
	}
	for _, args := range [][]string{{"--candidate-initialization-attestation", "extra"}, {"--candidate-initialization-attestation=true"}} {
		if handled, err := hotelinit.Attest(&output, args, d); !handled || err == nil {
			t.Fatal("malformed reserved attestation flag fell through")
		}
	}
	if _, _, err := hotelinit.OpenMongoStores(context.Background(), map[string]string{}); err == nil {
		t.Fatal("incomplete endpoints accepted")
	}
	client, closeClient, err := hotelinit.ConnectAndVerify(context.Background(), "user", "mongodb://SECRET@foreign", identity(d), d)
	if err == nil || client != nil || closeClient != nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatal("arbitrary endpoint accepted or leaked")
	}
}
