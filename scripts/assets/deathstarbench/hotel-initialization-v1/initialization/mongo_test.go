package initialization

// These tests inspect native driver options and command filter construction.
// They do not execute MongoDB commands or qualify server CAS/durability behavior.
import (
	"reflect"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func claimDocument(t *testing.T) bson.D {
	t.Helper()
	dataset := &Dataset{metadata: Metadata{SourceFingerprint: "sha256:" + strings.Repeat("a", 64), DatasetFingerprint: "sha256:" + strings.Repeat("b", 64)}}
	identity := Identity{RunID: "0123456789ab", ContractSHA256: dataset.Fingerprint()}
	var result bson.D
	if err := bson.Unmarshal(marker(dataset, identity, "user", "claimed"), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func rawDocument(t *testing.T, value bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func changeField(document bson.D, key string, value interface{}) bson.D {
	for i := range document {
		if document[i].Key == key {
			document[i].Value = value
			return document
		}
	}
	return append(document, bson.E{Key: key, Value: value})
}

func TestCompletionFilterRequiresWholeDocumentAndEveryExactBSONType(t *testing.T) {
	before := claimDocument(t)
	filter, err := completionFilter(rawDocument(t, before))
	if err != nil {
		t.Fatal(err)
	}
	if len(filter) != 2 || filter[0].Key != "_id" || filter[0].Value != "owner" || filter[1].Key != "$expr" {
		t.Fatal("completion filter lost exact marker selector")
	}
	expression := filter[1].Value.(bson.D)
	if len(expression) != 1 || expression[0].Key != "$and" {
		t.Fatal("completion filter must conjoin root equality and type guards")
	}
	conditions := expression[0].Value.(bson.A)
	if len(conditions) != len(before)+1 {
		t.Fatal("completion filter missing exact type guards")
	}
	wantedRoot := bson.D{{Key: "$eq", Value: bson.A{"$$ROOT", bson.D{{Key: "$literal", Value: before}}}}}
	if !reflect.DeepEqual(conditions[0], wantedRoot) {
		t.Fatal("completion CAS does not compare the entire literal marker")
	}
	for i, field := range before {
		kind := "string"
		if field.Key == "schema_version" {
			kind = "int"
		}
		wanted := bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$" + field.Key}}, kind}}}
		if !reflect.DeepEqual(conditions[i+1], wanted) {
			t.Fatalf("completion CAS lacks BSON type guard for %s", field.Key)
		}
	}
}

func TestCompletionFilterRejectsMalformedForeignAndNonExactExpectedMarkers(t *testing.T) {
	for _, kind := range []string{"missing", "extra", "duplicate", "int64-schema", "double-schema", "symbol-string", "wrong-version", "complete", "foreign-component", "foreign-database", "zero-run", "crossed-dataset", "zero-source"} {
		t.Run(kind, func(t *testing.T) {
			document := claimDocument(t)
			switch kind {
			case "missing":
				document = document[1:]
			case "extra":
				document = append(document, bson.E{Key: "foreign", Value: "foreign"})
			case "duplicate":
				document[len(document)-1] = document[0]
			case "int64-schema":
				document = changeField(document, "schema_version", int64(1))
			case "double-schema":
				document = changeField(document, "schema_version", float64(1))
			case "symbol-string":
				document = changeField(document, "state", primitive.Symbol("claimed"))
			case "wrong-version":
				document = changeField(document, "schema_version", int32(2))
			case "complete":
				document = changeField(document, "state", "complete")
			case "foreign-component":
				document = changeField(document, "component", "foreign")
			case "foreign-database":
				document = changeField(document, "database", "profile-db")
			case "zero-run":
				document = changeField(document, "run_id", "000000000000")
			case "crossed-dataset":
				document = changeField(document, "dataset_fingerprint", "sha256:"+strings.Repeat("c", 64))
			case "zero-source":
				document = changeField(document, "source_fingerprint", "sha256:"+strings.Repeat("0", 64))
			}
			if _, err := completionFilter(rawDocument(t, document)); err == nil {
				t.Fatal("malformed/foreign expected marker accepted")
			}
		})
	}
	if _, err := completionFilter(bson.Raw{0, 0, 0}); err == nil {
		t.Fatal("malformed BSON accepted")
	}
}

func TestMongoOptionsAreBoundedNonRetryingAndDurable(t *testing.T) {
	opts, err := mongoOptions("user", "mongodb-user:27017")
	if err != nil {
		t.Fatal(err)
	}
	if opts.RetryWrites == nil || *opts.RetryWrites || opts.RetryReads == nil || *opts.RetryReads {
		t.Fatal("automatic driver replay enabled")
	}
	if opts.WriteConcern == nil || opts.WriteConcern.GetW() != "majority" || !opts.WriteConcern.GetJ() || opts.WriteConcern.GetWTimeout() != operationTimeout {
		t.Fatal("writes lack bounded majority+journal acknowledgement")
	}
	if opts.ConnectTimeout == nil || *opts.ConnectTimeout != operationTimeout || opts.ServerSelectionTimeout == nil || *opts.ServerSelectionTimeout != operationTimeout || opts.SocketTimeout == nil || *opts.SocketTimeout != operationTimeout || opts.MaxPoolSize == nil || *opts.MaxPoolSize != 2 {
		t.Fatal("native client bounds differ from contract")
	}
	for _, pair := range [][2]string{{"foreign", "mongodb-foreign:27017"}, {"user", "mongodb-profile:27017"}, {"user", "mongodb://SECRET@mongodb-user:27017"}, {"user", "mongodb-user:27017?retryWrites=true"}} {
		if _, err := mongoOptions(pair[0], pair[1]); err == nil || strings.Contains(err.Error(), "SECRET") {
			t.Fatal("arbitrary endpoint accepted or disclosed")
		}
	}
}
