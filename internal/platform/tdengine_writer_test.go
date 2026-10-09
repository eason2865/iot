package platform

import (
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
)

func TestTDengineWriterWriteAfterCloseDoesNotPanic(t *testing.T) {
	db, err := sql.Open("taosRestful", "")
	if err != nil {
		t.Fatalf("open db handle: %v", err)
	}
	writer := &TDengineWriter{db: db}

	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := writer.WriteTelemetry(TelemetryRecord{}); !errors.Is(err, errTDengineWriterClosed) {
		t.Fatalf("expected closed writer error, got %v", err)
	}
}

func TestValidateTDField(t *testing.T) {
	for _, bad := range []string{"a\nb", "a\rb", "a\x00b", "\x7f", "tab\there"} {
		if err := validateTDField(bad); !errors.Is(err, errTDFieldRejected) {
			t.Fatalf("validateTDField(%q) = %v, want errTDFieldRejected", bad, err)
		}
	}
	// Quotes and backslashes stay legal: escapeTD neutralizes them.
	for _, ok := range []string{"tenant-1", "msg'id", `back\slash`, "unicode ✓"} {
		if err := validateTDField(ok); err != nil {
			t.Fatalf("validateTDField(%q) = %v, want nil", ok, err)
		}
	}
}

// The db handle is never dialed here: validation must reject before Exec.
func TestWriteRecordRejectsControlCharactersBeforeExec(t *testing.T) {
	db, err := sql.Open("taosRestful", "")
	if err != nil {
		t.Fatalf("open db handle: %v", err)
	}
	defer db.Close()
	writer := &TDengineWriter{db: db, database: "iot", table: "telemetry_v2"}

	rec := TelemetryRecord{
		TenantID: "t1", DeviceID: "d1", MsgID: "m\n1",
		Type: "temp", Version: "v1", Ts: 1730000000000, Payload: json.RawMessage(`{"v":1}`),
	}
	if err := writer.writeRecord(rec); !errors.Is(err, errTDFieldRejected) {
		t.Fatalf("writeRecord with control char in msgId = %v, want errTDFieldRejected", err)
	}
}
