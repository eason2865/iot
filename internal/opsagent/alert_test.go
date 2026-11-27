package opsagent

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func queueTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("IOT_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set IOT_POSTGRES_TEST_DSN for durable webhook tests")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	schema := fmt.Sprintf("ops_alert_test_%d", time.Now().UnixNano())
	if _, err = admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Exec("DROP SCHEMA " + schema + " CASCADE") })
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	args := u.Query()
	args.Set("search_path", schema)
	u.RawQuery = args.Encode()
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ddl, err := os.ReadFile("../../deploy/ops-agent/alert-queue.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(ddl)); err != nil {
		t.Fatal(err)
	}
	return u.String()
}

func TestGrafanaSupplement(t *testing.T) {
	dsn := queueTestDSN(t)
	var prom, tempo, model, notify atomic.Int32
	modelStarted := make(chan struct{}, 1)
	releaseModel := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/query":
			wantTime := "2026-11-27T08:00:00Z"
			if prom.Add(1) == 2 {
				wantTime = "2026-11-27T08:30:00Z"
			}
			if r.URL.Query().Get("time") != wantTime || r.URL.Query().Get("query") != `sum(rate(iot_dlq_publish_total{job="device-worker",result="error",stage="command.ack"}[5m]))` {
				t.Error(r.URL.Query())
			}
			io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"2.5"]}]}}`)
		case "/api/search":
			start, end := "1795766100", "1795766400"
			if tempo.Add(1) == 2 {
				start, end = "1795767900", "1795768200"
			}
			if r.URL.Query().Get("q") != `{ resource.service.name = "device-worker" && status = error }` || r.URL.Query().Get("start") != start || r.URL.Query().Get("end") != end {
				t.Error(r.URL.Query())
			}
			w.WriteHeader(503)
		case "/chat/completions":
			model.Add(1)
			data, _ := io.ReadAll(r.Body)
			if strings.Contains(string(data), "private-label") || strings.Contains(string(data), "secret-access") {
				t.Error("raw webhook or credentials leaked")
			}
			modelStarted <- struct{}{}
			<-releaseModel
			w.WriteHeader(503)
		case "/robot":
			n := notify.Add(1)
			b, _ := io.ReadAll(r.Body)
			stamp := r.URL.Query().Get("timestamp")
			mac := hmac.New(sha256.New, []byte("sign-key"))
			fmt.Fprint(mac, stamp+"\nsign-key")
			if r.URL.Query().Get("sign") != base64.StdEncoding.EncodeToString(mac.Sum(nil)) || r.URL.Query().Get("access_token") != "secret-access" {
				t.Error("invalid signature or missing access token")
			}
			if !strings.Contains(string(b), "2.5 次/秒") || !strings.Contains(string(b), "暂不可用") || strings.Contains(string(b), "private-label") {
				t.Error("invalid supplement", string(b))
			}
			if n == 3 && !strings.Contains(string(b), "补充分析 · 恢复") {
				t.Error("missing recovery notification")
			}
			if n == 1 {
				io.WriteString(w, `{"errcode":310000}`)
			} else {
				io.WriteString(w, `{"errcode":0}`)
			}
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer up.Close()
	for k, v := range map[string]string{"OPS_TENANTS": "", "OPS_ALERT_DSN": dsn, "OPS_GRAFANA_TOKEN": "grafana-test", "OPS_DINGTALK_URL": up.URL + "/robot?access_token=secret-access", "OPS_DINGTALK_SECRET": "sign-key", "OPS_PROMETHEUS_URL": up.URL, "OPS_TEMPO_URL": up.URL, "OPS_MODEL_URL": up.URL, "OPS_MODEL": "test"} {
		t.Setenv(k, v)
	}
	newServer := func() *Server {
		s, err := FromEnv()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.alerts.db.Close() })
		return s
	}
	s := newServer()
	post := func(s *Server, token, body string) int {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/api/v1/webhooks/grafana", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		s.Handler().ServeHTTP(w, r)
		return w.Code
	}
	body := `{"alerts":[{"status":"firing","startsAt":"2026-11-27T08:00:00Z","fingerprint":"f1","labels":{"rule_uid":"iot-dlq-write-failures","job":"device-worker","stage":"command.ack","other":"private-label"},"annotations":{"summary":"private-label"}}]}`
	if post(s, "wrong", body) != 401 || post(s, "grafana-test", strings.Replace(body, "firing", "bad", 1)) != 400 || post(s, "grafana-test", strings.Replace(body, "iot-dlq-write-failures", "unrelated", 1)) != 204 {
		t.Fatal("webhook validation failed")
	}
	for _, name := range []string{"DatasourceError", "DatasourceNoData"} {
		synthetic := strings.Replace(body, `"rule_uid":`, `"alertname":"`+name+`","rule_uid":`, 1)
		if post(s, "grafana-test", synthetic) != 204 {
			t.Fatal("classified datasource health as a business alert", name)
		}
	}
	if post(s, "grafana-test", body) != 202 || model.Load() != 0 {
		t.Fatal("webhook did not enqueue asynchronously")
	}
	// Another replica can pick up the committed event after an intake restart.
	peer := newServer()
	done := make(chan error, 1)
	go func() { _, err := peer.processAlert(context.Background()); done <- err }()
	select {
	case <-modelStarted:
	case <-time.After(5 * time.Second):
		close(releaseModel)
		t.Fatal("model did not start")
	}
	code := post(s, "grafana-test", body)
	worked, err := s.processAlert(t.Context())
	close(releaseModel)
	if jobErr := <-done; jobErr != nil {
		t.Fatal(jobErr)
	}
	if code != 202 || worked || err != nil {
		t.Fatalf("duplicate or lease failed: %d %t %v", code, worked, err)
	}
	if _, err = s.alerts.db.Exec("UPDATE ops_alert_jobs SET available_at=now() WHERE finished_at IS NULL"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.processAlert(t.Context()); err != nil {
		t.Fatal(err)
	}
	if post(peer, "grafana-test", body) != 202 {
		t.Fatal("repeat failed")
	}
	var delivered bool
	var attempts int
	if err = s.alerts.db.QueryRow("SELECT delivered,attempts FROM ops_alert_jobs").Scan(&delivered, &attempts); err != nil || !delivered || attempts != 2 || model.Load() != 1 || notify.Load() != 2 || prom.Load() != 1 || tempo.Load() != 1 {
		t.Fatal("delivery state or retry incorrect", err, delivered, attempts)
	}
	recovery := strings.Replace(strings.Replace(body, "firing", "resolved", 1), `"fingerprint"`, `"endsAt":"2026-11-27T08:30:00Z","fingerprint"`, 1)
	if post(s, "grafana-test", recovery) != 202 {
		t.Fatal("recovery rejected")
	}
	if worked, err := peer.processAlert(t.Context()); err != nil || !worked || notify.Load() != 3 {
		t.Fatal("recovery delivery failed", err)
	}
	s.alerts.db.Close()
	if post(s, "grafana-test", body) != 503 {
		t.Fatal("acknowledged alert while queue unavailable")
	}
}

func TestShippedRulesMatchAlertTools(t *testing.T) {
	for id, spec := range alertRules {
		b, err := os.ReadFile("../../monitoring/grafana/alerts/" + strings.TrimPrefix(id, "iot-") + ".json")
		if err != nil {
			t.Fatal(err)
		}
		var rule struct {
			Title  string
			Labels map[string]string
		}
		if err = json.Unmarshal(b, &rule); err != nil || rule.Title != spec.title || rule.Labels["rule_uid"] != id {
			t.Fatal(id, err)
		}
	}
}
