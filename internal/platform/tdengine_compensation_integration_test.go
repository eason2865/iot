package platform

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"iot/internal/contracts"
)

func TestTimestampRangeRejectionDoesNotBlockCompensation(t *testing.T) {
	dsn := os.Getenv("IOT_TDENGINE_TEST_DSN")
	if dsn == "" {
		t.Skip("set IOT_TDENGINE_TEST_DSN")
	}
	s := isolatedPostgresStore(t)
	database := fmt.Sprintf("range_%d", time.Now().UnixNano())
	admin, err := sql.Open("taosRestful", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE IF EXISTS " + database); _ = admin.Close() })
	// Pin retention rather than depending on the integration server's defaults.
	if _, err := admin.Exec("CREATE DATABASE " + database + " PRECISION 'ms' DURATION 1 KEEP 3"); err != nil {
		t.Fatal(err)
	}
	w, err := NewTDengineWriter(TDengineConfig{DSN: dsn, Database: database}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	for i, msg := range []string{"out-of-retention", "healthy"} {
		ts := time.Now().UnixMilli()
		if i == 0 {
			ts = 946684800000
		}
		env := contracts.Envelope{MsgID: msg, TenantID: "t", DeviceID: "d", Ts: ts, Type: "temp", Version: "v1", Payload: []byte(`{}`)}
		if err := contracts.ValidateEnvelope(env); err != nil {
			t.Fatal(err)
		}
		if _, err := s.RecordTelemetry(env); err != nil {
			t.Fatal(err)
		}
	}
	rows, next, err := s.ListUnwrittenTelemetry(t.Context(), time.Now().Add(time.Hour), 0, nil, 1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("scan: %v %v", rows, err)
	}
	if err = w.WriteTelemetry(rows[0]); err == nil || !strings.Contains(err.Error(), "0x60b") {
		t.Fatalf("expected real retention error, got %v", err)
	}
	worker := &Worker{store: s, tdengine: w}
	if !worker.compensateTelemetryRows(t.Context(), rows, s) {
		t.Fatal("permanent timestamp rejection prevented cursor advance")
	}
	rows, _, err = s.ListUnwrittenTelemetry(t.Context(), time.Now().Add(time.Hour), next, nil, 1)
	if err != nil || len(rows) != 1 || rows[0].MsgID != "healthy" {
		t.Fatalf("later batch: %v %v", rows, err)
	}
	if !worker.compensateTelemetryRows(t.Context(), rows, s) {
		t.Fatal("healthy later batch failed")
	}
	var poison, healthy bool
	if err = s.db.QueryRow(`SELECT BOOL_OR(tdengine_written) FILTER(WHERE msg_id='out-of-retention'), BOOL_OR(tdengine_written) FILTER(WHERE msg_id='healthy') FROM telemetry_records`).Scan(&poison, &healthy); err != nil || poison || !healthy {
		t.Fatalf("completion poison=%v healthy=%v err=%v", poison, healthy, err)
	}
}

func TestTDengineRejectsUnsupportedDatabasePrecision(t *testing.T) {
	dsn := os.Getenv("IOT_TDENGINE_TEST_DSN")
	if dsn == "" {
		t.Skip("set IOT_TDENGINE_TEST_DSN")
	}
	db, err := sql.Open("taosRestful", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, precision := range []string{"us", "ns"} {
		t.Run(precision, func(t *testing.T) {
			name := fmt.Sprintf("precision_%d", time.Now().UnixNano())
			if _, err := db.Exec("CREATE DATABASE " + name + " PRECISION '" + precision + "'"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = db.Exec("DROP DATABASE IF EXISTS " + name) })
			w, err := NewTDengineWriter(TDengineConfig{DSN: dsn, Database: name}, nil)
			if w != nil {
				_ = w.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "unsupported precision \""+precision+"\": require ms") {
				t.Fatalf("unsupported %s precision accepted: %v", precision, err)
			}
		})
	}
}

func TestHistoricalOversizedRowDoesNotBlockRealTDengine(t *testing.T) {
	dsn := os.Getenv("IOT_TDENGINE_TEST_DSN")
	if dsn == "" {
		t.Skip("set IOT_TDENGINE_TEST_DSN to run TDengine regressions")
	}
	s := isolatedPostgresStore(t)
	database := fmt.Sprintf("regression_%d", time.Now().UnixNano())
	td, err := NewTDengineWriter(TDengineConfig{DSN: dsn, Database: database}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := td.db.Exec("DROP DATABASE IF EXISTS " + database)
		if err != nil {
			t.Error(err)
		}
		_ = td.Close()
	})
	for _, msg := range []string{"historical", "healthy"} {
		_, err = s.RecordTelemetry(contracts.Envelope{MsgID: msg, TenantID: "t", DeviceID: "d", Ts: time.Now().UnixMilli(), Type: "temp", Version: "v1", Payload: []byte(`{"v":1}`)})
		if err != nil {
			t.Fatal(err)
		}
	}
	poison := strings.Repeat("x", 65)
	if _, err = s.db.Exec(`UPDATE telemetry_records SET msg_id=$1 WHERE msg_id='historical'`, poison); err != nil {
		t.Fatal(err)
	}
	rows, next, err := s.ListUnwrittenTelemetry(t.Context(), time.Now().Add(time.Hour), 0, nil, 500)
	if err != nil || len(rows) != 2 || next == 0 {
		t.Fatalf("scan=%+v next=%d err=%v", rows, next, err)
	}
	worker := &Worker{store: s, tdengine: td}
	if !worker.compensateTelemetryRows(t.Context(), rows, s) {
		t.Fatal("permanent row prevented cursor advance")
	}
	var healthyWritten, poisonWritten bool
	if err = s.db.QueryRow(`SELECT BOOL_OR(tdengine_written) FILTER(WHERE msg_id='healthy'),BOOL_OR(tdengine_written) FILTER(WHERE msg_id=$1) FROM telemetry_records`, poison).Scan(&healthyWritten, &poisonWritten); err != nil {
		t.Fatal(err)
	}
	if !healthyWritten || poisonWritten {
		t.Fatalf("completion: healthy=%v poison=%v", healthyWritten, poisonWritten)
	}
	var count int64
	if err = td.db.QueryRow("SELECT COUNT(*) FROM " + database + ".telemetry_v2").Scan(&count); err != nil || count != 1 {
		t.Fatalf("TDengine count=%d err=%v", count, err)
	}
}
