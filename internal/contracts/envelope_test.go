package contracts_test

import (
	"testing"

	"iot/internal/contracts"
)

func TestParseEnvelope(t *testing.T) {
	raw := []byte(`{
  "msgId": "b7a0d8c8-4f8b-4b1e-9d7d-3ad4d7fe1d2a",
  "tenantId": "t1",
  "deviceId": "d1",
  "ts": 1717670000000,
  "type": "telemetry",
  "version": "v1",
  "traceId": "trace-001",
  "payload": {
    "temp": 23.4,
    "humi": 60.1
  }
}`)

	got, err := contracts.ParseEnvelope(raw)
	if err != nil {
		t.Fatalf("ParseEnvelope() error = %v", err)
	}

	if got.MsgID != "b7a0d8c8-4f8b-4b1e-9d7d-3ad4d7fe1d2a" {
		t.Fatalf("ParseEnvelope().MsgID = %q, want %q", got.MsgID, "b7a0d8c8-4f8b-4b1e-9d7d-3ad4d7fe1d2a")
	}
	if got.TenantID != "t1" {
		t.Fatalf("ParseEnvelope().TenantID = %q, want %q", got.TenantID, "t1")
	}
	if got.DeviceID != "d1" {
		t.Fatalf("ParseEnvelope().DeviceID = %q, want %q", got.DeviceID, "d1")
	}
	if got.Type != "telemetry" {
		t.Fatalf("ParseEnvelope().Type = %q, want %q", got.Type, "telemetry")
	}
	if got.Version != "v1" {
		t.Fatalf("ParseEnvelope().Version = %q, want %q", got.Version, "v1")
	}
	if got.TraceID != "trace-001" {
		t.Fatalf("ParseEnvelope().TraceID = %q, want %q", got.TraceID, "trace-001")
	}
}

func TestParseEnvelopeRejectsInvalidTopicIdentifiers(t *testing.T) {
	raw := []byte(`{
  "msgId": "msg-1",
  "tenantId": "tenant/a",
  "deviceId": "device-42",
  "ts": 1717670000000,
  "type": "telemetry",
  "version": "v1",
  "payload": {}
}`)

	if _, err := contracts.ParseEnvelope(raw); err == nil {
		t.Fatal("ParseEnvelope() error = nil, want error")
	}
}
