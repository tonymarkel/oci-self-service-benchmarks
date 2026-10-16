package initialization_test

// In-memory fixtures only: these do not run MongoDB or qualify an application.
import (
	"context"
	"errors"
	"testing"

	hotelinit "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/initialization"
	"go.mongodb.org/mongo-driver/bson"
)

type startupReadStore struct {
	hotelinit.Store // Any attempted write panics; only Observe is implemented.
	snapshot        hotelinit.Snapshot
	err             error
	calls           int
}

func (store *startupReadStore) Observe(ctx context.Context, database string, limits map[string]int) (hotelinit.Snapshot, error) {
	store.calls++
	if ctx == nil || ctx.Err() != nil {
		return hotelinit.Snapshot{}, errors.New("cancelled observation")
	}
	if _, ok := ctx.Deadline(); !ok || len(limits) == 0 {
		return hotelinit.Snapshot{}, errors.New("unbounded observation")
	}
	return store.snapshot, store.err
}

func startupSnapshotFixture(t *testing.T, component string) (*hotelinit.Dataset, hotelinit.Identity, *startupReadStore) {
	t.Helper()
	d := dataset(t)
	identity := hotelinit.Identity{RunID: "abcdef123456", ContractSHA256: d.Fingerprint()}
	cluster, stores := newCluster(d)
	if err := hotelinit.Initialize(context.Background(), stores, identity, d); err != nil {
		t.Fatal(err)
	}
	rows := map[string][]bson.Raw{}
	for name, values := range cluster.stores[component].rows {
		rows[name] = copyRows(values)
	}
	return d, identity, &startupReadStore{snapshot: hotelinit.Snapshot{Collections: rows}}
}

func TestStartupRowsUseExactlyOneVerifiedDetachedSnapshot(t *testing.T) {
	d, identity, store := startupSnapshotFixture(t, "geo")
	rows, err := hotelinit.VerifiedStartupRows(context.Background(), store, "geo", "geo", identity, d)
	if err != nil || len(rows) != 80 || store.calls != 1 {
		t.Fatal("same-snapshot preload failed", err, len(rows), store.calls)
	}
	original := append(bson.Raw(nil), store.snapshot.Collections["geo"][0]...)
	rows[0][5] ^= 1
	if string(original) != string(store.snapshot.Collections["geo"][0]) {
		t.Fatal("returned rows alias observed store")
	}
	second, err := hotelinit.VerifiedStartupRows(context.Background(), store, "geo", "geo", identity, d)
	if err != nil || string(second[0]) != string(original) {
		t.Fatal("caller mutation changed later verified rows", err)
	}
	store.snapshot.Collections["geo"][0][5] ^= 1
	if string(second[0]) != string(original) {
		t.Fatal("retained observation aliases returned rows")
	}
}

func TestStartupCinemaEmptyRequiresExactOwnStoreBaseline(t *testing.T) {
	d, identity, store := startupSnapshotFixture(t, "attractions")
	rows, err := hotelinit.VerifiedStartupRows(context.Background(), store, "attractions", "cinemas", identity, d)
	if err != nil || rows == nil || len(rows) != 0 || store.calls != 1 {
		t.Fatal("verified absent cinema collection did not return explicit empty", err)
	}
	store.snapshot.Collections["cinemas"] = []bson.Raw{}
	if _, err = hotelinit.VerifiedStartupRows(context.Background(), store, "attractions", "cinemas", identity, d); err == nil {
		t.Fatal("unexpected empty cinema collection was adopted")
	}
	delete(store.snapshot.Collections, "cinemas")
	delete(store.snapshot.Collections, hotelinit.MarkerCollection)
	if _, err = hotelinit.VerifiedStartupRows(context.Background(), store, "attractions", "cinemas", identity, d); err == nil {
		t.Fatal("empty cinema bypassed baseline ownership proof")
	}
}

func TestStartupSnapshotRejectsIdentityCardinalityTypedDriftAndErrors(t *testing.T) {
	for _, change := range []string{"foreign-run", "foreign-dataset", "missing-marker", "missing-row", "duplicate-row", "typed-row", "observe-error", "cancelled"} {
		t.Run(change, func(t *testing.T) {
			d, identity, store := startupSnapshotFixture(t, "geo")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch change {
			case "foreign-run":
				identity.RunID = "fedcba654321"
			case "foreign-dataset":
				identity.ContractSHA256 = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			case "missing-marker":
				delete(store.snapshot.Collections, hotelinit.MarkerCollection)
			case "missing-row":
				store.snapshot.Collections["geo"] = store.snapshot.Collections["geo"][:79]
			case "duplicate-row":
				store.snapshot.Collections["geo"][1] = store.snapshot.Collections["geo"][0]
			case "typed-row":
				var row bson.D
				if bson.Unmarshal(store.snapshot.Collections["geo"][0], &row) != nil {
					t.Fatal("fixture decode")
				}
				for i := range row {
					if row[i].Key == "lat" {
						row[i].Value = int32(42)
					}
				}
				encoded, _ := bson.Marshal(row)
				store.snapshot.Collections["geo"][0] = encoded
			case "observe-error":
				store.err = errors.New("incomplete cursor")
			case "cancelled":
				cancel()
			}
			if rows, err := hotelinit.VerifiedStartupRows(ctx, store, "geo", "geo", identity, d); err == nil || rows != nil {
				t.Fatal("invalid snapshot produced rows", change)
			}
		})
	}
}

func TestStartupSnapshotRejectsNilAndCrossStoreBeforeObservation(t *testing.T) {
	d, identity, store := startupSnapshotFixture(t, "geo")
	if _, err := hotelinit.VerifiedStartupRows(context.Background(), nil, "geo", "geo", identity, d); err == nil {
		t.Fatal("nil store accepted")
	}
	if _, err := hotelinit.VerifiedMongoStartupRows(context.Background(), nil, "geo", "geo", identity, d); err == nil {
		t.Fatal("nil Mongo client accepted")
	}
	for _, collection := range []string{"hotels", hotelinit.MarkerCollection, "cinemas", ""} {
		if _, err := hotelinit.VerifiedStartupRows(context.Background(), store, "geo", collection, identity, d); err == nil {
			t.Fatal("crossed collection accepted", collection)
		}
	}
	if store.calls != 0 {
		t.Fatal("invalid collection performed an observation")
	}
}
