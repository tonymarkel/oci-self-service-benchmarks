package initialization

import (
	"context"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// Snapshot contains only the named application database. Collection presence
// matters: even an empty unowned collection is not an empty-store preflight.
// Store implementations must bound counts/scans and return detached raw BSON.
type Snapshot struct{ Collections map[string][]bson.Raw }
type Store interface {
	Observe(context.Context, string, map[string]int) (Snapshot, error)
	Claim(context.Context, string, bson.Raw) error
	Insert(context.Context, string, string, []bson.Raw) error
	Complete(context.Context, string, bson.Raw, bson.Raw) error
}
type Stores map[string]Store

const operationTimeout = 15 * time.Second
const initializationTimeout = 3 * time.Minute

func components(dataset *Dataset) []string {
	values := []string{}
	seen := map[string]bool{}
	for _, item := range dataset.collections {
		if !seen[item.spec.component] {
			seen[item.spec.component] = true
			values = append(values, item.spec.component)
		}
	}
	sort.Strings(values)
	return values
}
func componentCollections(dataset *Dataset, component string) []preparedCollection {
	values := []preparedCollection{}
	for _, item := range dataset.collections {
		if item.spec.component == component {
			values = append(values, item)
		}
	}
	return values
}
func checkContext(ctx context.Context) error {
	if ctx == nil {
		return failure("context is required")
	}
	return ctx.Err()
}
func checkInputs(ctx context.Context, stores Stores, identity Identity, dataset *Dataset) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := identity.validate(); err != nil {
		return err
	}
	if !validDataset(dataset) || identity.ContractSHA256 != dataset.Fingerprint() || len(stores) != 8 {
		return failure("complete compiled dataset and exact eight stores are required")
	}
	for _, component := range components(dataset) {
		if stores[component] == nil {
			return failure("missing required store")
		}
	}
	return nil
}
func marker(dataset *Dataset, identity Identity, component, state string) bson.Raw {
	value := bson.D{{Key: "_id", Value: "owner"}, {Key: "schema_version", Value: int32(1)}, {Key: "contract_revision", Value: Revision},
		{Key: "dataset_revision", Value: DatasetRevision}, {Key: "component", Value: component}, {Key: "database", Value: component + "-db"},
		{Key: "run_id", Value: identity.RunID}, {Key: "contract_sha256", Value: identity.ContractSHA256},
		{Key: "source_fingerprint", Value: dataset.metadata.SourceFingerprint}, {Key: "dataset_fingerprint", Value: dataset.Fingerprint()}, {Key: "state", Value: state}}
	canonical, _ := canonicalValue(value)
	raw, _ := bson.Marshal(canonical)
	return raw
}
func observe(ctx context.Context, store Store, dataset *Dataset, component string) (Snapshot, error) {
	if err := checkContext(ctx); err != nil {
		return Snapshot{}, err
	}
	limits := map[string]int{MarkerCollection: 1}
	for _, item := range componentCollections(dataset, component) {
		limits[item.spec.name] = item.spec.count
	}
	operation, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	value, err := store.Observe(operation, component+"-db", limits)
	if err != nil {
		return Snapshot{}, failure("bounded store observation failed")
	}
	if err = checkContext(operation); err != nil {
		return Snapshot{}, err
	}
	if value.Collections == nil {
		return Snapshot{}, failure("store inventory observation is missing")
	}
	if len(value.Collections) > len(limits) {
		return Snapshot{}, failure("unexpected application collections")
	}
	for name, rows := range value.Collections {
		limit, ok := limits[name]
		if !ok || len(rows) > limit {
			return Snapshot{}, failure("unexpected collection or excess document count")
		}
		for _, row := range rows {
			if len(row) == 0 || len(row) > 65536 {
				return Snapshot{}, failure("observed document exceeds bound")
			}
		}
	}
	return value, nil
}
func equalRaw(left, right bson.Raw) bool {
	a, e := document(left)
	if e != nil {
		return false
	}
	b, e := document(right)
	if e != nil {
		return false
	}
	x, e := bson.MarshalExtJSON(a, true, false)
	if e != nil {
		return false
	}
	y, e := bson.MarshalExtJSON(b, true, false)
	return e == nil && string(x) == string(y)
}
func requireMarker(snapshot Snapshot, wanted bson.Raw) error {
	rows, ok := snapshot.Collections[MarkerCollection]
	if !ok || len(rows) != 1 || !equalRaw(rows[0], wanted) {
		return failure("foreign, incomplete or malformed ownership marker")
	}
	return nil
}
func verifySnapshot(snapshot Snapshot, dataset *Dataset, identity Identity, component, state string) error {
	if err := requireMarker(snapshot, marker(dataset, identity, component, state)); err != nil {
		return err
	}
	return verifyCollections(snapshot, componentCollections(dataset, component))
}

func verifyCollections(snapshot Snapshot, collections []preparedCollection) error {
	if len(snapshot.Collections) != len(collections)+1 {
		return failure("dataset collection inventory is incomplete")
	}
	for _, item := range collections {
		rows, ok := snapshot.Collections[item.spec.name]
		if !ok || len(rows) != len(item.rows) {
			return failure("baseline document cardinality differs")
		}
		wanted := map[string]bool{}
		for _, value := range item.canonical {
			wanted[value] = true
		}
		for _, raw := range rows {
			doc, err := document(raw)
			if err != nil {
				return err
			}
			value, err := bson.MarshalExtJSON(doc, true, false)
			if err != nil || !wanted[string(value)] {
				return failure("missing, duplicate or changed typed baseline document")
			}
			delete(wanted, string(value))
		}
		if len(wanted) != 0 {
			return failure("baseline content is incomplete")
		}
	}
	return nil
}
func writeOperation(ctx context.Context, callback func(context.Context) error) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	operation, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	if callback(operation) != nil {
		return failure("one-shot write failed or its response is uncertain; do not replay")
	}
	return checkContext(operation)
}
func detachedRows(rows []bson.Raw) []bson.Raw {
	result := make([]bson.Raw, len(rows))
	for i, row := range rows {
		result[i] = append(bson.Raw(nil), row...)
	}
	return result
}

// Initialize never repairs or replays any existing state, even exact complete
// state. All eight atomic claims must be durable and re-observed before data
// writes; all eight full datasets must verify before the first complete CAS.
// There is no cross-server transaction. A failed/partial attempt stays owned
// and incomplete for explicit read-only diagnosis and infrastructure cleanup.
func Initialize(ctx context.Context, stores Stores, identity Identity, dataset *Dataset) error {
	if err := checkInputs(ctx, stores, identity, dataset); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, initializationTimeout)
	defer cancel()
	names := components(dataset)
	for _, component := range names {
		snapshot, err := observe(ctx, stores[component], dataset, component)
		if err != nil {
			return err
		}
		if len(snapshot.Collections) != 0 {
			return failure("initialization requires every store absent/empty and unclaimed; never replay")
		}
	}
	for _, component := range names {
		claim := marker(dataset, identity, component, "claimed")
		if err := writeOperation(ctx, func(ctx context.Context) error { return stores[component].Claim(ctx, component+"-db", claim) }); err != nil {
			return err
		}
	}
	for _, component := range names {
		snapshot, err := observe(ctx, stores[component], dataset, component)
		if err != nil {
			return err
		}
		if len(snapshot.Collections) != 1 {
			return failure("data appeared before all claims were established")
		}
		if err = requireMarker(snapshot, marker(dataset, identity, component, "claimed")); err != nil {
			return err
		}
	}
	progress := map[string][]preparedCollection{}
	for _, item := range dataset.collections {
		item := item
		snapshot, err := observe(ctx, stores[item.spec.component], dataset, item.spec.component)
		if err != nil {
			return err
		}
		if err = requireMarker(snapshot, marker(dataset, identity, item.spec.component, "claimed")); err != nil {
			return err
		}
		if err = verifyCollections(snapshot, progress[item.spec.component]); err != nil {
			return err
		}
		if err := writeOperation(ctx, func(ctx context.Context) error {
			return stores[item.spec.component].Insert(ctx, item.spec.database, item.spec.name, detachedRows(item.rows))
		}); err != nil {
			return err
		}
		progress[item.spec.component] = append(progress[item.spec.component], item)
	}
	for _, component := range names {
		snapshot, err := observe(ctx, stores[component], dataset, component)
		if err != nil {
			return err
		}
		if err = verifySnapshot(snapshot, dataset, identity, component, "claimed"); err != nil {
			return err
		}
	}
	for _, component := range names {
		snapshot, err := observe(ctx, stores[component], dataset, component)
		if err != nil {
			return err
		}
		if err = verifySnapshot(snapshot, dataset, identity, component, "claimed"); err != nil {
			return err
		}
		before, after := marker(dataset, identity, component, "claimed"), marker(dataset, identity, component, "complete")
		if err := writeOperation(ctx, func(ctx context.Context) error {
			return stores[component].Complete(ctx, component+"-db", before, after)
		}); err != nil {
			return err
		}
	}
	return Verify(ctx, stores, identity, dataset)
}

// Verify only reads exact completed ownership and baseline contents. Historical
// success is not authority and reservation mutations intentionally fail this
// baseline contract; this does not qualify healthy Consul/Pod restart behavior.
func Verify(ctx context.Context, stores Stores, identity Identity, dataset *Dataset) error {
	if err := checkInputs(ctx, stores, identity, dataset); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, initializationTimeout)
	defer cancel()
	for _, component := range components(dataset) {
		if err := VerifyComponent(ctx, stores[component], component, identity, dataset); err != nil {
			return err
		}
	}
	return nil
}

// VerifyComponent only observes the service's own store. The future controller
// must separately finish global verification before authorizing applications.
func VerifyComponent(ctx context.Context, store Store, component string, identity Identity, dataset *Dataset) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := identity.validate(); err != nil {
		return err
	}
	if !validDataset(dataset) || identity.ContractSHA256 != dataset.Fingerprint() || store == nil || len(componentCollections(dataset, component)) == 0 {
		return failure("invalid own-store verification input")
	}
	snapshot, err := observe(ctx, store, dataset, component)
	if err != nil {
		return err
	}
	return verifySnapshot(snapshot, dataset, identity, component, "complete")
}
