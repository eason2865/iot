package platform

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestTDengineWriterWriteAfterCloseDoesNotPanic(t *testing.T) {
	db, err := sql.Open("taosRestful", "root:unused@http(127.0.0.1:1)/")
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

func TestWriteRecordRejectsOversizedFieldsBeforeExec(t *testing.T) {
	db, err := sql.Open("taosRestful", "root:unused@http(127.0.0.1:1)/")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := &TDengineWriter{db: db}
	for _, field := range []string{"tenant", "device", "message", "type", "version"} {
		rec := TelemetryRecord{TenantID: "t", DeviceID: "d", MsgID: "m", Type: "temp", Version: "v1"}
		value := strings.Repeat("界", 22) // 66 bytes, although only 22 characters.
		switch field {
		case "tenant":
			rec.TenantID = value
		case "device":
			rec.DeviceID = value
		case "message":
			rec.MsgID = value
		case "type":
			rec.Type = value
		case "version":
			rec.Version = value
		}
		if err = w.writeRecord(rec); !errors.Is(err, errTDFieldRejected) {
			t.Errorf("%s: got %v, want permanent field rejection", field, err)
		}
	}
}

// The db handle is never dialed here: validation must reject before Exec.
func TestWriteRecordRejectsControlCharactersBeforeExec(t *testing.T) {
	db, err := sql.Open("taosRestful", "root:unused@http(127.0.0.1:1)/")
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
