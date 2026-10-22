package platform

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTDengineTimestampRoundTripIgnoresSessionTimezone(t *testing.T) {
	dsn := os.Getenv("IOT_TDENGINE_TEST_DSN")
	if dsn == "" {
		t.Skip("set IOT_TDENGINE_TEST_DSN")
	}
	for _, zone := range []string{"UTC", "Asia%2FShanghai"} {
		t.Run(zone, func(t *testing.T) {
			sep := "?"
			if strings.Contains(dsn, "?") {
				sep = "&"
			}
			database := fmt.Sprintf("timezone_%d", time.Now().UnixNano())
			w, err := NewTDengineWriter(TDengineConfig{DSN: dsn + sep + "timezone=" + zone, Database: database}, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = w.db.Exec("DROP DATABASE IF EXISTS " + database); _ = w.Close() })
			rec := TelemetryRecord{TenantID: "t", DeviceID: "d", MsgID: "timezone", Ts: time.Now().UnixMilli(), Type: "temp", Version: "v1", Payload: []byte(`{}`)}
			if err = w.WriteTelemetry(rec); err != nil {
				t.Fatal(err)
			}
			var got time.Time
			if err = w.db.QueryRow("SELECT ts FROM " + database + ".telemetry_v2").Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got.UnixMilli() != rec.Ts {
				t.Fatalf("time shifted: got=%d want=%d", got.UnixMilli(), rec.Ts)
			}
		})
	}
}
