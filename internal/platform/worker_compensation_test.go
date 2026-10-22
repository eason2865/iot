package platform

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type recordingTelemetryMarker map[string]bool

func (m recordingTelemetryMarker) MarkTelemetryTDengineWritten(msgID, _, _ string) error {
	m[msgID] = true
	return nil
}

func TestCompensationSkipsPermanentFieldsButRetriesOutages(t *testing.T) {
	for _, outage := range []bool{false, true} {
		t.Run(fmt.Sprint("outage=", outage), func(t *testing.T) {
			var inserts []string
			var mu sync.Mutex
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				inserts = append(inserts, string(body))
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				if outage {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				fmt.Fprint(w, `{"code":0,"column_meta":[["affected_rows","INT",4]],"data":[[1]],"rows":1}`)
			}))
			defer server.Close()
			db, err := sql.Open("taosRestful", "root:password@http("+strings.TrimPrefix(server.URL, "http://")+")/")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			worker := &Worker{tdengine: &TDengineWriter{db: db, database: "test", table: "telemetry_v2"}}
			poison := TelemetryRecord{TenantID: "t", DeviceID: "d", MsgID: strings.Repeat("x", 65), Type: "temp", Version: "v1", Ts: 1730000000000}
			good := poison
			good.MsgID = "healthy"
			marked := recordingTelemetryMarker{}
			advance := worker.compensateTelemetryRows(t.Context(), []TelemetryRecord{poison, good}, marked)
			if advance == outage {
				t.Fatalf("advance=%v outage=%v", advance, outage)
			}
			if marked[poison.MsgID] || marked[good.MsgID] == outage {
				t.Fatalf("incorrect completion markers: %v", marked)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(inserts) != 1 || !strings.Contains(inserts[0], "healthy") {
				t.Fatalf("expected only healthy row sent to sink: %v", inserts)
			}
		})
	}
}
