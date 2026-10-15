// Package initialization is an unreleased, baseline-only Hotel initializer.
// Offline construction and mocked tests are not MongoDB or restart qualification.
package initialization

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

const Revision = "hotel-separate-initialization-candidate-v1"
const RecipeRevision = "hotel-reservation-guarded-init-native-go-scratch-candidate-v1"
const DatasetRevision = "hotel-reservation-deterministic-rounded-baseline-candidate-v1"
const MarkerCollection = "_hotel_candidate_initialization"

type Collection struct {
	Component, Database, Name string
	Documents                 []interface{}
}

type CollectionMetadata struct {
	Component         string   `json:"component"`
	Database          string   `json:"database"`
	Name              string   `json:"name"`
	Count             int      `json:"count"`
	LogicalKeyFields  []string `json:"logical_key_fields"`
	ContentSHA256     string   `json:"content_sha256"`
	LogicalKeysSHA256 string   `json:"logical_keys_sha256"`
}

type Metadata struct {
	SchemaVersion           int                  `json:"schema_version"`
	CandidateSchema         string               `json:"candidate_schema"`
	CandidateOnly           bool                 `json:"candidate_only"`
	ContractRevision        string               `json:"contract_revision"`
	RecipeRevision          string               `json:"recipe_revision"`
	DatasetRevision         string               `json:"dataset_revision"`
	SourceFingerprint       string               `json:"source_fingerprint"`
	DatasetFingerprint      string               `json:"dataset_fingerprint"`
	Canonicalization        string               `json:"canonicalization"`
	DeterministicID         string               `json:"deterministic_id"`
	DatabaseCount           int                  `json:"database_count"`
	CollectionCount         int                  `json:"collection_count"`
	DocumentCount           int                  `json:"document_count"`
	Collections             []CollectionMetadata `json:"collections"`
	Scope                   string               `json:"scope"`
	InitializationExecuted  bool                 `json:"initialization_executed"`
	DatasetQualified        bool                 `json:"dataset_qualified"`
	RuntimeQualified        bool                 `json:"runtime_qualified"`
	MeasurementQualified    bool                 `json:"measurement_qualified"`
	ApplicationStartAllowed bool                 `json:"application_start_allowed"`
	PublicRelease           bool                 `json:"public_release"`
}

type collectionSpec struct {
	component, database, name string
	count                     int
	keys                      []string
	shape                     map[string]interface{}
}

func fields(items ...string) map[string]interface{} {
	value := map[string]interface{}{}
	for _, item := range items {
		parts := strings.SplitN(item, ":", 2)
		value[parts[0]] = parts[1]
	}
	return value
}

func specifications() []collectionSpec {
	point := fields("hotelId:string", "lat:double", "lon:double")
	profile := fields("id:string", "name:string", "phoneNumber:string", "description:string")
	profile["address"] = fields("streetNumber:string", "streetName:string", "city:string", "state:string", "country:string", "postalCode:string", "lat:double", "lon:double")
	rate := fields("hotelId:string", "code:string", "inDate:string", "outDate:string")
	rate["roomType"] = fields("bookableRate:double", "code:string", "roomDescription:string", "totalRate:double", "totalRateInclusive:double")
	review := fields("reviewId:string", "hotelId:string", "name:string", "rating:double", "description:string")
	review["images"] = fields("url:string", "default:bool")
	return []collectionSpec{
		{"attractions", "attractions-db", "hotels", 6, []string{"hotelId"}, point},
		{"attractions", "attractions-db", "museums", 6, []string{"museumId"}, fields("museumId:string", "lat:double", "lon:double", "museumName:string", "type:string")},
		{"attractions", "attractions-db", "restaurants", 6, []string{"restaurantId"}, fields("restaurantId:string", "lat:double", "lon:double", "restaurantName:string", "rating:double", "type:string")},
		{"geo", "geo-db", "geo", 80, []string{"hotelId"}, point},
		{"profile", "profile-db", "hotels", 80, []string{"id"}, profile},
		{"rate", "rate-db", "inventory", 27, []string{"hotelId", "code", "inDate", "outDate", "roomType.code"}, rate},
		{"recommendation", "recommendation-db", "recommendation", 80, []string{"hotelId"}, fields("hotelId:string", "lat:double", "lon:double", "rate:double", "price:double")},
		{"reservation", "reservation-db", "number", 80, []string{"hotelId"}, fields("hotelId:string", "numberOfRoom:int")},
		{"reservation", "reservation-db", "reservation", 1, []string{"hotelId", "customerName", "inDate", "outDate"}, fields("hotelId:string", "customerName:string", "inDate:string", "outDate:string", "number:int")},
		{"review", "review-db", "reviews", 6, []string{"reviewId"}, review},
		{"user", "user-db", "user", 501, []string{"username"}, fields("username:string", "password:string")},
	}
}

type preparedCollection struct {
	spec      collectionSpec
	rows      []bson.Raw
	canonical []string
}
type Dataset struct {
	collections []preparedCollection
	metadata    Metadata
}

func failure(message string) error { return errors.New("hotel candidate initialization: " + message) }
func digest(raw []byte) string {
	value := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(value[:])
}

var exactDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func validDigest(value string) bool {
	return exactDigest.MatchString(value) && value != "sha256:"+strings.Repeat("0", 64)
}

// canonicalJSON sorts all object keys, preserves JSON integer tokens, emits UTF-8,
// disables HTML escaping, and has no terminal newline.
func canonicalJSON(value interface{}) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, failure("invalid canonical JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded interface{}
	if decoder.Decode(&decoded) != nil {
		return nil, failure("invalid canonical JSON")
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if encoder.Encode(decoded) != nil {
		return nil, failure("invalid canonical JSON")
	}
	return bytes.TrimSuffix(output.Bytes(), []byte("\n")), nil
}

func canonicalValue(value interface{}) (interface{}, error) {
	switch typed := value.(type) {
	case bson.D:
		result := make(bson.D, len(typed))
		seen := map[string]bool{}
		for index, item := range typed {
			if item.Key == "" || seen[item.Key] {
				return nil, failure("duplicate or empty BSON field")
			}
			seen[item.Key] = true
			child, err := canonicalValue(item.Value)
			if err != nil {
				return nil, err
			}
			result[index] = bson.E{Key: item.Key, Value: child}
		}
		sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
		return result, nil
	case bson.A:
		result := make(bson.A, len(typed))
		for index, item := range typed {
			child, err := canonicalValue(item)
			if err != nil {
				return nil, err
			}
			result[index] = child
		}
		return result, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return nil, failure("nonfinite BSON number")
		}
		return typed, nil
	case string, bool, int32, int64:
		return value, nil
	default:
		return nil, failure("unsupported BSON value")
	}
}

func document(raw []byte) (bson.D, error) {
	if len(raw) == 0 || len(raw) > 65536 {
		return nil, failure("document size exceeds bound")
	}
	var decoded bson.D
	if bson.Unmarshal(raw, &decoded) != nil {
		return nil, failure("invalid BSON document")
	}
	value, err := canonicalValue(decoded)
	if err != nil {
		return nil, err
	}
	return value.(bson.D), nil
}

func lookup(doc bson.D, path string) (interface{}, bool) {
	parts := strings.SplitN(path, ".", 2)
	for _, item := range doc {
		if item.Key == parts[0] {
			if len(parts) == 1 {
				return item.Value, true
			}
			nested, ok := item.Value.(bson.D)
			if !ok {
				return nil, false
			}
			return lookup(nested, parts[1])
		}
	}
	return nil, false
}

func matchesShape(doc bson.D, shape map[string]interface{}) bool {
	if len(doc) != len(shape) {
		return false
	}
	for _, item := range doc {
		expected, ok := shape[item.Key]
		if !ok {
			return false
		}
		if nested, ok := expected.(map[string]interface{}); ok {
			value, ok := item.Value.(bson.D)
			if !ok || !matchesShape(value, nested) {
				return false
			}
			continue
		}
		switch expected {
		case "string":
			if value, ok := item.Value.(string); !ok || value == "" {
				return false
			}
		case "double":
			if _, ok := item.Value.(float64); !ok {
				return false
			}
		case "bool":
			if _, ok := item.Value.(bool); !ok {
				return false
			}
		case "int":
			// The pinned seed structs use Go int values fitting BSON int32.
			// Do not silently equate a foreign int64/double with that type.
			if _, ok := item.Value.(int32); !ok {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func expectedLogicalKeys(spec collectionSpec) map[string]bool {
	result := map[string]bool{}
	add := func(values ...string) { encoded, _ := canonicalJSON(values); result[string(encoded)] = true }
	if spec.component == "rate" {
		for i := 1; i <= 80; i++ {
			if i > 3 && (i < 7 || i%3 != 0) {
				continue
			}
			out, room := "2015-04-10", "KNG"
			if i == 2 {
				room = "QN"
			}
			if i > 6 {
				if i%2 == 0 {
					out = "2015-04-17"
				} else {
					out = "2015-04-24"
				}
			}
			add(strconv.Itoa(i), "RACK", "2015-04-09", out, room)
		}
		return result
	}
	if spec.component == "reservation" && spec.name == "reservation" {
		add("4", "Alice", "2015-04-09", "2015-04-10")
		return result
	}
	if spec.component == "user" {
		for i := 0; i <= 500; i++ {
			add("Cornell_" + strconv.Itoa(i))
		}
		return result
	}
	for i := 1; i <= spec.count; i++ {
		add(strconv.Itoa(i))
	}
	return result
}

// BuildDataset accepts the exact eleven typed seed collections. The source
// fingerprint is a compiled preparation binding, not provenance or authority.
// _id hashes canonical JSON {component,database,collection,key:[logical strings]}.
func BuildDataset(collections []Collection, sourceFingerprint string) (*Dataset, error) {
	if !validDigest(sourceFingerprint) || len(collections) != 11 {
		return nil, failure("invalid source binding or collection inventory")
	}
	byName := map[string]Collection{}
	for _, value := range collections {
		key := value.Component + "/" + value.Database + "/" + value.Name
		if _, exists := byName[key]; exists {
			return nil, failure("duplicate collection")
		}
		byName[key] = value
	}
	dataset := &Dataset{}
	metadata := Metadata{SchemaVersion: 1, CandidateSchema: "hotel-initialization-dataset-v1", CandidateOnly: true,
		ContractRevision: Revision, RecipeRevision: RecipeRevision, DatasetRevision: DatasetRevision, SourceFingerprint: sourceFingerprint,
		Canonicalization: "bson-canonical-extjson-recursive-sorted-v1", DeterministicID: "sha256-canonical-logical-key-v1",
		DatabaseCount: 8, CollectionCount: 11, DocumentCount: 873, Scope: "offline-dataset-construction-only-no-mongodb-execution"}
	for _, spec := range specifications() {
		value, ok := byName[spec.component+"/"+spec.database+"/"+spec.name]
		if !ok || len(value.Documents) != spec.count {
			return nil, failure("incomplete collection cardinality")
		}
		expected := expectedLogicalKeys(spec)
		rows := map[string]bson.Raw{}
		canonicalRows := map[string]string{}
		for _, input := range value.Documents {
			raw, err := bson.Marshal(input)
			if err != nil {
				return nil, failure("invalid typed seed document")
			}
			doc, err := document(raw)
			if err != nil {
				return nil, err
			}
			if !matchesShape(doc, spec.shape) {
				return nil, failure("unexpected seed fields or BSON types")
			}
			keys := []string{}
			for _, field := range spec.keys {
				item, ok := lookup(doc, field)
				if !ok {
					return nil, failure("missing logical key")
				}
				text, ok := item.(string)
				if !ok {
					return nil, failure("invalid logical key type")
				}
				keys = append(keys, text)
			}
			keyBytes, _ := canonicalJSON(keys)
			key := string(keyBytes)
			if !expected[key] || rows[key] != nil {
				return nil, failure("missing, duplicate or foreign logical seed row")
			}
			if spec.component == "user" {
				username, _ := lookup(doc, "username")
				password, _ := lookup(doc, "password")
				suffix := strings.TrimPrefix(username.(string), "Cornell_")
				sum := sha256.Sum256([]byte(strings.Repeat(suffix, 10)))
				if password != hex.EncodeToString(sum[:]) {
					return nil, failure("seeded password mismatch")
				}
			}
			if spec.component == "reservation" {
				if spec.name == "reservation" {
					number, _ := lookup(doc, "number")
					if number != int32(1) {
						return nil, failure("seed reservation value mismatch")
					}
				} else {
					hotel, _ := lookup(doc, "hotelId")
					index, _ := strconv.Atoi(hotel.(string))
					capacity := int32(200)
					if index > 6 && index%3 == 1 {
						capacity = 300
					} else if index > 6 && index%3 == 2 {
						capacity = 250
					}
					number, _ := lookup(doc, "numberOfRoom")
					if number != capacity {
						return nil, failure("seed room-capacity value mismatch")
					}
				}
			}
			idBytes, _ := canonicalJSON(map[string]interface{}{"component": spec.component, "database": spec.database, "collection": spec.name, "key": keys})
			doc = append(doc, bson.E{Key: "_id", Value: digest(idBytes)})
			normalized, err := canonicalValue(doc)
			if err != nil {
				return nil, err
			}
			doc = normalized.(bson.D)
			raw, err = bson.Marshal(doc)
			if err != nil {
				return nil, failure("invalid canonical seed BSON")
			}
			ejson, err := bson.MarshalExtJSON(doc, true, false)
			if err != nil {
				return nil, failure("invalid canonical seed EJSON")
			}
			rows[key] = append(bson.Raw(nil), raw...)
			canonicalRows[key] = string(ejson)
		}
		keys := make([]string, 0, len(rows))
		for key := range rows {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		prepared := preparedCollection{spec: spec}
		content := []json.RawMessage{}
		for _, key := range keys {
			prepared.rows = append(prepared.rows, rows[key])
			prepared.canonical = append(prepared.canonical, canonicalRows[key])
			content = append(content, json.RawMessage(canonicalRows[key]))
		}
		encoded, _ := canonicalJSON(content)
		keyBytes, _ := canonicalJSON(keys)
		metadata.Collections = append(metadata.Collections, CollectionMetadata{Component: spec.component, Database: spec.database, Name: spec.name, Count: spec.count, LogicalKeyFields: append([]string(nil), spec.keys...), ContentSHA256: digest(encoded), LogicalKeysSHA256: digest(keyBytes)})
		dataset.collections = append(dataset.collections, prepared)
	}
	encoded, _ := canonicalJSON(metadata)
	var base map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	_ = decoder.Decode(&base)
	delete(base, "dataset_fingerprint")
	encoded, _ = canonicalJSON(base)
	metadata.DatasetFingerprint = digest(encoded)
	dataset.metadata = metadata
	return dataset, nil
}

func (dataset *Dataset) Fingerprint() string {
	if dataset == nil {
		return ""
	}
	return dataset.metadata.DatasetFingerprint
}
func (dataset *Dataset) Metadata() Metadata {
	if dataset == nil {
		return Metadata{}
	}
	value := dataset.metadata
	value.Collections = append([]CollectionMetadata(nil), value.Collections...)
	for i := range value.Collections {
		value.Collections[i].LogicalKeyFields = append([]string(nil), value.Collections[i].LogicalKeyFields...)
	}
	return value
}
func validDataset(dataset *Dataset) bool {
	return dataset != nil && len(dataset.collections) == 11 && validDigest(dataset.Fingerprint())
}

type Identity struct {
	RunID string
	// In this private source contract this is exactly Dataset.Fingerprint(),
	// not a bearer capability or a future controller authorization token.
	ContractSHA256 string
}

var runPattern = regexp.MustCompile(`^[0-9a-f]{12}$`)

func (identity Identity) validate() error {
	if !runPattern.MatchString(identity.RunID) || identity.RunID == "000000000000" || !validDigest(identity.ContractSHA256) {
		return failure("mandatory run/contract identity is invalid")
	}
	return nil
}
func IdentityFromEnv() (Identity, error) {
	value := Identity{os.Getenv("HOTEL_CANDIDATE_RUN_ID"), os.Getenv("HOTEL_CANDIDATE_CONTRACT_SHA256")}
	return value, value.validate()
}

// Attest must run before configuration, tuning or connection in every main.
// It performs no MongoDB I/O and intentionally advertises no execution gate.
func Attest(writer io.Writer, args []string, dataset *Dataset) (bool, error) {
	found := false
	for _, arg := range args {
		if arg == "--candidate-initialization-attestation" || strings.HasPrefix(arg, "--candidate-initialization-attestation=") {
			found = true
		}
	}
	if !found {
		return false, nil
	}
	if len(args) != 1 || args[0] != "--candidate-initialization-attestation" || !validDataset(dataset) || writer == nil {
		return true, failure("invalid offline attestation request")
	}
	raw, err := canonicalJSON(dataset.Metadata())
	if err != nil {
		return true, err
	}
	if _, err = fmt.Fprintln(writer, string(raw)); err != nil {
		return true, failure("offline attestation output failed")
	}
	return true, nil
}
