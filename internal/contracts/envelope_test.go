package contracts_test

import (
	"errors"
	"testing"
	"time"

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

// TestValidateEnvelopeTimestampBounds covers the timestamp range check. Without
// it a broken or hostile device clock writes timestamps that sort incorrectly
// forever: a far-future ts stays the "latest" point, and a seconds-as-
// milliseconds mix-up silently lands in 1970.
func TestValidateEnvelopeTimestampBounds(t *testing.T) {
	now := time.Now()
	valid := func(ts int64) contracts.Envelope {
		return contracts.Envelope{
			MsgID:    "msg-1",
			TenantID: "tenant-a",
			DeviceID: "device-1",
			Type:     "telemetry",
			Version:  "v1",
			Ts:       ts,
		}
	}

	for _, tc := range []struct {
		name    string
		ts      int64
		wantErr bool
	}{
		{"current time", now.UnixMilli(), false},
		{"slightly behind", now.Add(-24 * time.Hour).UnixMilli(), false},
		{"inside the clock skew", now.Add(4 * time.Minute).UnixMilli(), false},
		{"exactly the 2000-01-01 floor", 946684800000, false},
		{"just below the floor", 946684799999, true},
		{"seconds sent as milliseconds (1970)", 1717670000, true},
		{"well beyond the clock skew", now.Add(time.Hour).UnixMilli(), true},
		{"zero", 0, true},
		{"negative", -1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := contracts.ValidateEnvelope(valid(tc.ts))
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("ValidateEnvelope(ts=%d) error = %v, want nil", tc.ts, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateEnvelope(ts=%d) error = nil, want an error", tc.ts)
			}
			// Callers and the DLQ rely on the generic sentinel, so the specific
			// timestamp error must still satisfy errors.Is(ErrInvalidEnvelope).
			if !errors.Is(err, contracts.ErrInvalidEnvelope) {
				t.Fatalf("ValidateEnvelope(ts=%d) error = %v, want it to wrap ErrInvalidEnvelope", tc.ts, err)
			}
			if tc.ts == 0 || tc.ts == -1 {
				return // handled by the required-field check, not the range check
			}
			if !errors.Is(err, contracts.ErrEnvelopeTimestampOutOfRange) {
				t.Fatalf("ValidateEnvelope(ts=%d) error = %v, want ErrEnvelopeTimestampOutOfRange", tc.ts, err)
			}
		})
	}
}
