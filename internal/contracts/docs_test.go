package contracts_test

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"iot/internal/contracts"
)

// TestOpenAPISpecIsTheEmbeddedPublishedFile is the consistency assertion for the
// management API contract. The spec served at /openapi.json used to be a second,
// hand-written copy of docs/openapi.json; it is now embedded from that file, and
// this test fails if a Go-side copy is ever reintroduced or the published
// document stops matching what the code serves.
func TestOpenAPISpecIsTheEmbeddedPublishedFile(t *testing.T) {
	published := readPublishedJSON(t, "../../docs/openapi.json")

	served := contracts.OpenAPISpec()
	if served == nil {
		t.Fatal("contracts.OpenAPISpec() returned nil; the embedded document does not decode")
	}
	if !reflect.DeepEqual(served, published) {
		t.Fatalf("OpenAPISpec() differs from docs/openapi.json; the published file is the single source of truth and the served document must match it")
	}

	assertRequiredKeys(t, "docs/openapi.json", published, "openapi", "info", "paths")
}

// TestMQTTEnvelopeSchemaIsTheEmbeddedPublishedFile does the same for the MQTT
// envelope schema.
func TestMQTTEnvelopeSchemaIsTheEmbeddedPublishedFile(t *testing.T) {
	published := readPublishedJSON(t, "../../docs/mqtt-envelope.schema.json")

	served := contracts.MQTTEnvelopeSchema()
	if served == nil {
		t.Fatal("contracts.MQTTEnvelopeSchema() returned nil; the embedded document does not decode")
	}
	if !reflect.DeepEqual(served, published) {
		t.Fatalf("MQTTEnvelopeSchema() differs from docs/mqtt-envelope.schema.json; the published file is the single source of truth and the served document must match it")
	}

	assertRequiredKeys(t, "docs/mqtt-envelope.schema.json", published, "$schema", "title", "required", "properties")
}

// TestPublishedOpenAPIDocumentsEveryManagementRoutePath checks that the
// published spec still declares the documented management API surface. It does
// not enumerate internal/adminapi routes (that would need a second copy of the
// route table); it guards against a wholesale loss of paths from the contract.
func TestPublishedOpenAPIDocumentsEveryManagementRoutePath(t *testing.T) {
	published := readPublishedJSON(t, "../../docs/openapi.json")

	paths, ok := published["paths"].(map[string]any)
	if !ok {
		t.Fatalf("docs/openapi.json has no paths object")
	}
	for _, path := range []string{
		"/api/v1/tenants",
		"/api/v1/devices",
		"/api/v1/devices/{tenantId}/{deviceId}",
		"/api/v1/devices/{tenantId}/{deviceId}/status",
		"/api/v1/devices/{tenantId}/{deviceId}/telemetry",
		"/api/v1/telemetry",
		"/api/v1/commands",
		"/api/v1/commands/{id}",
		"/api/v1/commands/{id}/ack",
		"/healthz",
	} {
		if _, ok := paths[path]; !ok {
			t.Errorf("docs/openapi.json is missing path %q", path)
		}
	}
}

// TestEmbeddedContractAccessorsDoNotShareState makes sure a caller mutating the
// decoded document cannot corrupt the embedded copy for every other caller.
func TestEmbeddedContractAccessorsDoNotShareState(t *testing.T) {
	first := contracts.OpenAPISpec()
	if first == nil {
		t.Fatal("contracts.OpenAPISpec() returned nil")
	}
	first["openapi"] = "mutated"

	if second := contracts.OpenAPISpec(); second["openapi"] == "mutated" {
		t.Fatal("OpenAPISpec() returned a shared mutable document")
	}

	schema := contracts.MQTTEnvelopeSchema()
	if schema == nil {
		t.Fatal("contracts.MQTTEnvelopeSchema() returned nil")
	}
	schema["title"] = "mutated"
	if again := contracts.MQTTEnvelopeSchema(); again["title"] == "mutated" {
		t.Fatal("MQTTEnvelopeSchema() returned a shared mutable document")
	}
}

func readPublishedJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s is not valid JSON: %v", path, err)
	}
	return doc
}

func assertRequiredKeys(t *testing.T, name string, doc map[string]any, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if _, ok := doc[key]; !ok {
			t.Errorf("%s is missing required key %q", name, key)
		}
	}
}
