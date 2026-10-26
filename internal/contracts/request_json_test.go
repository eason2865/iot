package contracts

import (
	"strings"
	"testing"
)

func TestRequestJSONRejectsAmbiguousInput(t *testing.T) {
	for _, raw := range []string{`null`, `{"tenantId":"a","tenantId":"b"}`, `{"tenantId":"a","TenantID":"b"}`, `{"payload":{"x":1,"x":2}}`, `{"tenantId":"a","TenantId":"b"}`, `{} {}`, `{}garbage`} {
		var dst struct {
			TenantID string         `json:"tenantId"`
			Payload  map[string]any `json:"payload"`
		}
		if err := DecodeRequestJSON(strings.NewReader(raw), &dst); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}

func TestBusinessFieldContracts(t *testing.T) {
	for _, name := range []string{"", "  ", "bad\nname", strings.Repeat("a", 257)} {
		if ValidateTenantName(name) == nil {
			t.Errorf("accepted name %q", name)
		}
	}
	for _, id := range []string{"", " ", "p/x", "_p", strings.Repeat("p", 65)} {
		if ValidateProductID(id) == nil {
			t.Errorf("accepted product %q", id)
		}
	}
	for _, payload := range []string{"null", "[]", "1", "true"} {
		if ValidateCommandPayload([]byte(payload)) == nil {
			t.Errorf("accepted command %s", payload)
		}
	}
	if err := ValidateCommandPayload([]byte(`{"switch":"on"}`)); err != nil {
		t.Fatal(err)
	}
}

func TestRequestFieldMatchingRejectsUnicodeCaseAliases(t *testing.T) {
	var dst struct {
		Status string `json:"status"`
	}
	if DecodeRequestJSON(strings.NewReader(`{"status":"acked","ſtatus":"failed"}`), &dst) == nil {
		t.Fatal("accepted duplicate struct field alias")
	}
	var body struct {
		Payload map[string]int `json:"payload"`
	}
	if err := DecodeRequestJSON(strings.NewReader(`{"payload":{"x":1,"X":2}}`), &body); err != nil {
		t.Fatalf("case-sensitive payload keys rejected: %v", err)
	}
}
