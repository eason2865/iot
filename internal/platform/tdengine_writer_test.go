package platform

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestCompensationCancellationInterruptsTDengineRequest(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release }))
	defer server.Close()
	defer unblock()
	db, err := sql.Open("taosRestful", "root:password@http("+strings.TrimPrefix(server.URL, "http://")+")/")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	worker := &Worker{tdengine: &TDengineWriter{db: db, database: "test", table: "telemetry_v2"}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	marked := recordingTelemetryMarker{}
	done := make(chan bool, 1)
	go func() {
		done <- worker.compensateTelemetryRows(ctx, []TelemetryRecord{{TenantID: "t", DeviceID: "d", MsgID: "m", Type: "temp", Version: "v1", Ts: time.Now().UnixMilli()}}, marked)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("TDengine request not reached")
	}
	cancel()
	select {
	case advance := <-done:
		if advance || len(marked) != 0 {
			t.Fatal("cancelled write was marked complete")
		}
	case <-time.After(300 * time.Millisecond):
		// Release the old implementation before DB.Close waits for its Exec.
		unblock()
		<-done
		t.Fatal("cancelled compensation is stuck in TDengine HTTP request")
	}
}

func TestTDengineWriteAndInitializationHaveTimeouts(t *testing.T) {
	for _, initialization := range []bool{false, true} {
		t.Run(fmt.Sprint("initialization=", initialization), func(t *testing.T) {
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			defer server.Close()
			defer unblock()
			dsn := "root:password@http(" + strings.TrimPrefix(server.URL, "http://") + ")/"
			done := make(chan error, 1)
			go func() {
				if initialization {
					w, err := NewTDengineWriter(TDengineConfig{DSN: dsn, WriteTimeout: 50 * time.Millisecond}, nil)
					if w != nil {
						_ = w.Close()
					}
					done <- err
				} else {
					db, err := sql.Open("taosRestful", dsn)
					if err != nil {
						done <- err
						return
					}
					defer db.Close()
					w := &TDengineWriter{db: db, database: "test", table: "telemetry_v2", writeTimeout: 50 * time.Millisecond}
					done <- w.WriteTelemetry(TelemetryRecord{TenantID: "t", DeviceID: "d", MsgID: "m", Type: "temp", Version: "v1", Ts: time.Now().UnixMilli()})
				}
			}()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected write deadline, got %v", err)
				}
			case <-time.After(500 * time.Millisecond):
				unblock()
				<-done
				t.Fatal("TDengine operation has no total timeout")
			}
		})
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
		if err = w.writeRecord(t.Context(), rec); !errors.Is(err, errTDFieldRejected) {
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
	if err := writer.writeRecord(t.Context(), rec); !errors.Is(err, errTDFieldRejected) {
		t.Fatalf("writeRecord with control char in msgId = %v, want errTDFieldRejected", err)
	}
}
