package platform

import (
	"database/sql"
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
