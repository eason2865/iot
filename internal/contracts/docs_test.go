package contracts_test

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"iot/internal/contracts"
)

// TestMQTTEnvelopeSchemaIsTheEmbeddedPublishedFile is the consistency assertion
// for the MQTT envelope contract. The schema served at
// /schemas/mqtt-envelope.json is embedded from docs/mqtt-envelope.schema.json,
// which is the single hand-maintained source; this test fails if a Go-side copy
// is ever reintroduced or the published document stops matching what the code
// serves.
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

// TestEmbeddedContractAccessorDoesNotShareState makes sure a caller mutating the
// decoded document cannot corrupt the embedded copy for every other caller.
func TestEmbeddedContractAccessorDoesNotShareState(t *testing.T) {
	first := contracts.MQTTEnvelopeSchema()
	if first == nil {
		t.Fatal("contracts.MQTTEnvelopeSchema() returned nil")
	}
	first["title"] = "mutated"

	if second := contracts.MQTTEnvelopeSchema(); second["title"] == "mutated" {
		t.Fatal("MQTTEnvelopeSchema() returned a shared mutable document")
	}
}

// TestCommandResponseHidesInternalDispatchFields pins the management API shape
// for commands. platform.Command carries dispatcher bookkeeping for the Kafka
// event and the dispatch loop; serialising it directly in a REST response
// leaked dispatchAttempts and deadlineAt to clients as always-zero values.
func TestCommandResponseHidesInternalDispatchFields(t *testing.T) {
	encoded, err := json.Marshal(contracts.CommandResponse{
		ID:       "cmd-1",
		TenantID: "tenant-a",
		DeviceID: "device-1",
		Status:   contracts.CommandStatusSent,
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	for _, field := range []string{"id", "tenantId", "deviceId", "status", "payload", "createdAt", "updatedAt"} {
		if _, ok := got[field]; !ok {
			t.Errorf("CommandResponse is missing public field %q", field)
		}
	}
	for _, field := range []string{"dispatchAttempts", "deadlineAt"} {
		if _, ok := got[field]; ok {
			t.Errorf("CommandResponse must not expose internal dispatch field %q, got %s", field, encoded)
		}
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
