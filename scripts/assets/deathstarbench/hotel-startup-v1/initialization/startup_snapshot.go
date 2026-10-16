package initialization

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// VerifiedStartupRows performs one bounded own-store observation, verifies its
// exact completed baseline, and returns only detached rows from THAT snapshot.
// It never claims, writes, repairs, seeds or recognizes restart authority.
func VerifiedStartupRows(ctx context.Context, store Store, component, collection string, identity Identity, dataset *Dataset) ([]bson.Raw, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := identity.validate(); err != nil {
		return nil, err
	}
	if store == nil || !validDataset(dataset) || identity.ContractSHA256 != dataset.Fingerprint() || len(componentCollections(dataset, component)) == 0 {
		return nil, failure("invalid startup own-store snapshot input")
	}
	known := component == "attractions" && collection == "cinemas"
	for _, item := range componentCollections(dataset, component) {
		known = known || item.spec.name == collection
	}
	if !known {
		return nil, failure("startup collection is outside its compiled own-store baseline")
	}
	snapshot, err := observe(ctx, store, dataset, component)
	if err != nil {
		return nil, err
	}
	// Detach before verification too: a Store cannot later mutate the rows
	// that were checked into a different in-memory preload result.
	detached := Snapshot{Collections: map[string][]bson.Raw{}}
	for name, rows := range snapshot.Collections {
		detached.Collections[name] = detachedRows(rows)
	}
	if err = verifySnapshot(detached, dataset, identity, component, "complete"); err != nil {
		return nil, err
	}
	if err = checkContext(ctx); err != nil {
		return nil, err
	}
	// Cinema data is deliberately absent from this revision. Only a verified
	// exact attractions baseline can produce this explicit empty preload.
	if component == "attractions" && collection == "cinemas" {
		return []bson.Raw{}, nil
	}
	return detachedRows(detached.Collections[collection]), nil
}

// VerifiedMongoStartupRows adapts an existing client to the strict bounded
// store reader. No second collection Find or unchecked Cursor.All is allowed.
func VerifiedMongoStartupRows(ctx context.Context, client *mongo.Client, component, collection string, identity Identity, dataset *Dataset) ([]bson.Raw, error) {
	if client == nil {
		return nil, failure("startup Mongo client is required")
	}
	return VerifiedStartupRows(ctx, &mongoStore{client: client, component: component}, component, collection, identity, dataset)
}
