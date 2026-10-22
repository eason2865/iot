package platform

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"iot/internal/contracts"
)

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
