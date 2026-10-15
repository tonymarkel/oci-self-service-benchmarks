package initialization

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type mongoStore struct {
	client    *mongo.Client
	component string
}

func knownComponent(component string) bool {
	for _, spec := range specifications() {
		if spec.component == component {
			return true
		}
	}
	return false
}
func mongoOptions(component, address string) (*options.ClientOptions, error) {
	if !knownComponent(component) || address != "mongodb-"+component+":27017" {
		return nil, failure("Mongo endpoint is not the fixed own-store address")
	}
	return options.Client().ApplyURI("mongodb://" + address).SetRetryWrites(false).SetRetryReads(false).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority(), writeconcern.J(true), writeconcern.WTimeout(operationTimeout))).
		SetConnectTimeout(operationTimeout).SetServerSelectionTimeout(operationTimeout).SetSocketTimeout(operationTimeout).SetMaxPoolSize(2), nil
}
func openMongo(ctx context.Context, component, address string) (*mongoStore, func(), error) {
	if err := checkContext(ctx); err != nil {
		return nil, nil, err
	}
	opts, err := mongoOptions(component, address)
	if err != nil {
		return nil, nil, err
	}
	operation, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	client, err := mongo.Connect(operation, opts)
	if err != nil {
		return nil, nil, failure("Mongo connection setup failed")
	}
	closeClient := func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Disconnect(cleanup)
	}
	if client.Ping(operation, readpref.Primary()) != nil {
		closeClient()
		return nil, nil, failure("Mongo ping failed")
	}
	if err = checkContext(operation); err != nil {
		closeClient()
		return nil, nil, err
	}
	return &mongoStore{client: client, component: component}, closeClient, nil
}

// OpenMongoStores only connects/pings; it performs no writes. Address selectors
// are fixed service names, never arbitrary URIs, credentials or query options.
func OpenMongoStores(ctx context.Context, addresses map[string]string) (Stores, func(), error) {
	if err := checkContext(ctx); err != nil {
		return nil, nil, err
	}
	if len(addresses) != 8 {
		return nil, nil, failure("exact eight Mongo endpoints are required")
	}
	for _, spec := range specifications() {
		if _, err := mongoOptions(spec.component, addresses[spec.component]); err != nil {
			return nil, nil, err
		}
	}
	stores := Stores{}
	closers := []func(){}
	closeAll := func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}
	for _, spec := range specifications() {
		if stores[spec.component] != nil {
			continue
		}
		store, closeClient, err := openMongo(ctx, spec.component, addresses[spec.component])
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		stores[spec.component] = store
		closers = append(closers, closeClient)
	}
	return stores, closeAll, nil
}

// ConnectAndVerify is startup read-only: no claims, inserts, index creation,
// repair or fallback seeding. It only verifies its own completed baseline.
func ConnectAndVerify(ctx context.Context, component, address string, identity Identity, dataset *Dataset) (*mongo.Client, func(), error) {
	if err := identity.validate(); err != nil {
		return nil, nil, err
	}
	if !validDataset(dataset) || identity.ContractSHA256 != dataset.Fingerprint() {
		return nil, nil, failure("compiled dataset is required")
	}
	store, closeClient, err := openMongo(ctx, component, address)
	if err != nil {
		return nil, nil, err
	}
	if err = VerifyComponent(ctx, store, component, identity, dataset); err != nil {
		closeClient()
		return nil, nil, err
	}
	return store.client, closeClient, nil
}

func (store *mongoStore) validate(database string, limits map[string]int) error {
	if database != store.component+"-db" {
		return failure("Mongo operation crossed its own database")
	}
	if limits != nil {
		expected := map[string]int{MarkerCollection: 1}
		for _, spec := range specifications() {
			if spec.component == store.component {
				expected[spec.name] = spec.count
			}
		}
		if len(limits) != len(expected) {
			return failure("Mongo scan limits differ from the exact dataset")
		}
		for key, value := range expected {
			if limits[key] != value {
				return failure("Mongo scan limits differ from the exact dataset")
			}
		}
	}
	return nil
}
func (store *mongoStore) Observe(ctx context.Context, database string, limits map[string]int) (Snapshot, error) {
	if err := store.validate(database, limits); err != nil {
		return Snapshot{}, err
	}
	// Mongo-owned administrative databases are not application dataset stores.
	names, err := store.client.ListDatabaseNames(ctx, bson.D{}, options.ListDatabases().SetNameOnly(true))
	if err != nil || len(names) > 4 {
		return Snapshot{}, failure("Mongo database inventory read failed or exceeds bound")
	}
	for _, name := range names {
		if name != "admin" && name != "config" && name != "local" && name != database {
			return Snapshot{}, failure("unexpected application database")
		}
	}
	db := store.client.Database(database)
	cursor, err := db.ListCollections(ctx, bson.D{}, options.ListCollections().SetBatchSize(int32(len(limits)+1)))
	if err != nil {
		return Snapshot{}, failure("Mongo collection inventory read failed")
	}
	defer cursor.Close(ctx)
	result := Snapshot{Collections: map[string][]bson.Raw{}}
	for cursor.Next(ctx) {
		var entry struct {
			Name    string `bson:"name"`
			Type    string `bson:"type"`
			Options bson.D `bson:"options"`
		}
		if cursor.Decode(&entry) != nil || entry.Type != "collection" || len(entry.Options) != 0 {
			return Snapshot{}, failure("unexpected collection kind/options")
		}
		limit, ok := limits[entry.Name]
		if !ok || len(result.Collections) >= len(limits) {
			return Snapshot{}, failure("unexpected or excess collections")
		}
		if _, duplicate := result.Collections[entry.Name]; duplicate {
			return Snapshot{}, failure("duplicate collection observation")
		}
		collection := db.Collection(entry.Name)
		count, err := collection.CountDocuments(ctx, bson.D{}, options.Count().SetLimit(int64(limit+1)).SetMaxTime(operationTimeout))
		if err != nil || count < 0 || count > int64(limit) {
			return Snapshot{}, failure("Mongo bounded document count failed")
		}
		rows, err := collection.Find(ctx, bson.D{}, options.Find().SetLimit(int64(limit+1)).SetBatchSize(int32(limit+1)).SetMaxTime(operationTimeout))
		if err != nil {
			return Snapshot{}, failure("Mongo bounded content scan failed")
		}
		observed := []bson.Raw{}
		for rows.Next(ctx) {
			if len(observed) >= limit || len(rows.Current) > 65536 {
				_ = rows.Close(ctx)
				return Snapshot{}, failure("Mongo content exceeds scan bound")
			}
			observed = append(observed, append(bson.Raw(nil), rows.Current...))
		}
		readErr := rows.Err()
		_ = rows.Close(ctx)
		if readErr != nil || len(observed) != int(count) {
			return Snapshot{}, failure("Mongo content scan is incomplete or changed")
		}
		result.Collections[entry.Name] = observed
	}
	if cursor.Err() != nil {
		return Snapshot{}, failure("Mongo collection inventory is incomplete")
	}
	return result, nil
}
func (store *mongoStore) Claim(ctx context.Context, database string, claim bson.Raw) error {
	if err := store.validate(database, nil); err != nil {
		return err
	}
	_, err := store.client.Database(database).Collection(MarkerCollection).InsertOne(ctx, claim)
	if err != nil {
		return failure("atomic claim failed or response uncertain")
	}
	return nil
}
func (store *mongoStore) Insert(ctx context.Context, database, name string, documents []bson.Raw) error {
	if err := store.validate(database, nil); err != nil {
		return err
	}
	expected := 0
	for _, spec := range specifications() {
		if spec.component == store.component && spec.name == name {
			expected = spec.count
		}
	}
	if expected == 0 || len(documents) != expected {
		return failure("write collection/count differs from compiled dataset")
	}
	values := make([]interface{}, len(documents))
	for i, row := range documents {
		values[i] = append(bson.Raw(nil), row...)
	}
	result, err := store.client.Database(database).Collection(name).InsertMany(ctx, values, options.InsertMany().SetOrdered(true))
	if err != nil || result == nil || len(result.InsertedIDs) != len(values) {
		return failure("one-shot data write failed or response uncertain")
	}
	return nil
}
func completionFilter(before bson.Raw) (bson.D, error) {
	var expected bson.D
	if bson.Unmarshal(before, &expected) != nil {
		return nil, failure("invalid claim CAS")
	}
	fields := map[string]bool{"_id": true, "schema_version": true, "contract_revision": true, "dataset_revision": true,
		"component": true, "database": true, "run_id": true, "contract_sha256": true, "source_fingerprint": true, "dataset_fingerprint": true, "state": true}
	if len(expected) != len(fields) {
		return nil, failure("invalid claim CAS fields")
	}
	values := map[string]string{}
	for _, field := range expected {
		if !fields[field.Key] {
			return nil, failure("invalid claim CAS fields")
		}
		delete(fields, field.Key)
		if field.Key == "schema_version" {
			if number, ok := field.Value.(int32); !ok || number != 1 {
				return nil, failure("invalid claim CAS BSON type")
			}
			continue
		}
		value, ok := field.Value.(string)
		if !ok || value == "" {
			return nil, failure("invalid claim CAS BSON type")
		}
		values[field.Key] = value
	}
	if values["_id"] != "owner" || values["contract_revision"] != Revision || values["dataset_revision"] != DatasetRevision || values["state"] != "claimed" ||
		!knownComponent(values["component"]) || values["database"] != values["component"]+"-db" ||
		(Identity{RunID: values["run_id"], ContractSHA256: values["contract_sha256"]}).validate() != nil ||
		values["contract_sha256"] != values["dataset_fingerprint"] || !validDigest(values["source_fingerprint"]) {
		return nil, failure("invalid claim CAS identity")
	}
	// Whole-document equality is atomic and rejects added/altered fields rather
	// than replacing/repairing a foreign marker matched by only a field subset.
	// Mongo equality equates numeric BSON types (and string/symbol values), so
	// explicit aggregation $type equality is also required for every field.
	conditions := bson.A{bson.D{{Key: "$eq", Value: bson.A{"$$ROOT", bson.D{{Key: "$literal", Value: expected}}}}}}
	for _, field := range expected {
		kind := "string"
		if field.Key == "schema_version" {
			kind = "int"
		}
		conditions = append(conditions, bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$" + field.Key}}, kind}}})
	}
	return bson.D{{Key: "_id", Value: "owner"}, {Key: "$expr", Value: bson.D{{Key: "$and", Value: conditions}}}}, nil
}

func (store *mongoStore) Complete(ctx context.Context, database string, before, after bson.Raw) error {
	if err := store.validate(database, nil); err != nil {
		return err
	}
	filter, err := completionFilter(before)
	if err != nil {
		return err
	}
	var expected bson.D
	_ = bson.Unmarshal(before, &expected)
	for i := range expected {
		if expected[i].Key == "state" {
			expected[i].Value = "complete"
		}
	}
	wanted, _ := bson.Marshal(expected)
	if !equalRaw(after, wanted) {
		return failure("claim completion replacement differs from the exact state transition")
	}
	result, err := store.client.Database(database).Collection(MarkerCollection).ReplaceOne(ctx, filter, after, options.Replace().SetUpsert(false))
	if err != nil || result == nil || result.MatchedCount != 1 || result.ModifiedCount != 1 {
		return failure("exact claim completion CAS failed or response uncertain")
	}
	return nil
}
