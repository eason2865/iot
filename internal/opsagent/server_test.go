package opsagent

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const tenantConfig = `{"mine":{"operatorToken":"operator-token","managementToken":"read-token"}}`
const diagnosticPath = "/api/v1/tenants/mine/commands/test-command/diagnoses"

func testAgent(t *testing.T, backend http.HandlerFunc) *Server {
	t.Helper()
	upstream := httptest.NewServer(backend)
	t.Cleanup(upstream.Close)
	for _, key := range []string{"OPS_MODEL_KEY", "OPS_MODEL", "OPS_MODEL_URL", "OPS_PROMETHEUS_URL", "OPS_TEMPO_URL"} {
		t.Setenv(key, "")
	}
	t.Setenv("OPS_TENANTS", tenantConfig)
	t.Setenv("OPS_MANAGEMENT_URL", upstream.URL)
	s, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func diagnosis(s *Server, path, token, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

const statusJSON = `{"id":"test-command","tenantId":"mine","status":"timeout","createdAt":"2026-11-27T00:00:00Z","updatedAt":"2026-11-27T00:00:30Z","deviceId":"sensitive-device","payload":{"secret":"sensitive-command"},"result":{"payload":{"secret":"sensitive-result"}}}`

func TestDiagnosisToolsAndPrivacy(t *testing.T) {
	var calls atomic.Int32
	s := testAgent(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/api/v1/tenants/mine/commands/test-command":
			if r.Method != "GET" || r.Header.Get("Authorization") != "Bearer read-token" {
				t.Error("wrong management request")
			}
			io.WriteString(w, statusJSON)
		case "/api/v1/query":
			if r.Header.Get("Authorization") != "" || r.URL.Query().Get("time") != "2026-11-27T00:00:30Z" || r.URL.Query().Get("query") != `min(up{job="device-worker"})` {
				t.Error("wrong metrics request")
			}
			io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[123,"0"]}]}}`)
		case "/api/search":
			if r.Header.Get("Authorization") != "" || !strings.Contains(r.URL.Query().Get("q"), `span.iot.command.id = "test-command"`) || r.URL.Query().Get("start") == "" {
				t.Error("wrong trace request")
			}
			io.WriteString(w, `{"traces":[{"spanSets":[{"matched":2,"spans":[{"name":"command.timeout","startTimeUnixNano":"1000000000","durationNanos":"2000000","attributes":[{"key":"iot.command.id","value":{"stringValue":"test-command"}},{"key":"status","value":{"stringValue":"error"}}]},{"name":"command.ack","startTimeUnixNano":"3000000000","durationNanos":"100","attributes":[{"key":"iot.command.id","value":{"stringValue":"unrelated"}}]}]}]}]}`)
		case "/v1/chat/completions":
			raw, _ := io.ReadAll(r.Body)
			body := string(raw)
			if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer model-key" || !strings.Contains(body, "排查超时") {
				t.Error("wrong model request")
			}
			for _, secret := range []string{"sensitive", "test-command", "read-token", "operator-token", "model-key", "unrelated"} {
				if strings.Contains(body, secret) {
					t.Errorf("model received %s", secret)
				}
			}
			io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"检查 ACK 订阅。"}}]}`)
		default:
			t.Errorf("unexpected tool path %s", r.URL.Path)
			w.WriteHeader(500)
		}
	})
	s.prometheus = s.management
	s.tempo = s.management
	s.modelURL = s.management + "/v1"
	s.model = "test"
	s.modelKey = "model-key"
	w := diagnosis(s, diagnosticPath, "operator-token", `{"question":"排查超时"}`)
	var got report
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != 200 || calls.Load() != 4 {
		t.Fatalf("status %d, calls %d, %v", w.Code, calls.Load(), err)
	}
	if got.Trace.State != "ok" || len(got.Trace.Stages) != 1 || got.Trace.Stages[0].DurationMS != 2 || got.Trace.Stages[0].Status != "error" || got.Metrics.AllWorkerTargetsUp == nil || *got.Metrics.AllWorkerTargetsUp || got.ModelStatus != "ok" || strings.Contains(w.Body.String(), "sensitive") {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestFailClosedBeforeObservation(t *testing.T) {
	var calls atomic.Int32
	s := testAgent(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(404) })
	s.tempo = s.management
	s.prometheus = s.management
	s.modelURL = s.management
	for _, tc := range []struct {
		path, token, body string
		code              int
	}{
		{diagnosticPath, "wrong", "", 401}, {strings.Replace(diagnosticPath, "mine", "other", 1), "operator-token", "", 401},
		{diagnosticPath, "read-token", "", 401}, {diagnosticPath + "?query=up", "operator-token", "", 400}, {diagnosticPath, "operator-token", `{"url":"http://untrusted"}`, 400},
		{diagnosticPath, "operator-token", `{"question":"a","Question":"b"}`, 400}, {diagnosticPath, "operator-token", "", 404},
	} {
		if got := diagnosis(s, tc.path, tc.token, tc.body); got.Code != tc.code {
			t.Errorf("status %d want %d", got.Code, tc.code)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("unexpected upstream calls: %d", calls.Load())
	}
}

func TestEvidenceFallback(t *testing.T) {
	s := testAgent(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/tenants/mine/commands/test-command":
			io.WriteString(w, statusJSON)
		case "/api/search":
			io.WriteString(w, `{"traces":[]}`)
		default:
			w.WriteHeader(503)
		}
	})
	for _, enabled := range []bool{false, true} {
		if enabled {
			s.prometheus = s.management
			s.tempo = s.management
			s.modelURL = s.management
		}
		w := diagnosis(s, diagnosticPath, "operator-token", "")
		var r report
		if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil || w.Code != 200 || r.Finding == "" {
			t.Fatal(w.Body.String())
		}
		if enabled && (r.Trace.State != "no_data" || r.Metrics.State != "unavailable" || r.ModelStatus != "unavailable") {
			t.Fatal(r)
		}
		if !enabled && (r.Trace.State != "disabled" || r.Metrics.State != "disabled" || r.ModelStatus != "disabled") {
			t.Fatal(r)
		}
	}
}

func TestManagementRedirectKeepsCredentialsLocal(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("redirect followed") }))
	defer target.Close()
	s := testAgent(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	})
	if got := diagnosis(s, diagnosticPath, "operator-token", ""); got.Code != 502 {
		t.Fatal(got.Code)
	}
}

func TestConfigurationRejectsCredentialReuse(t *testing.T) {
	testAgent(t, func(http.ResponseWriter, *http.Request) {})
	t.Setenv("OPS_TENANTS", `{"a":{"operatorToken":"shared","managementToken":"reader-a"},"b":{"operatorToken":"op-b","managementToken":"shared"}}`)
	if _, err := FromEnv(); err == nil {
		t.Fatal("accepted shared credentials")
	}
}
