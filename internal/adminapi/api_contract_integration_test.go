package adminapi

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/rest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"iot/internal/core"
	"iot/internal/platform"
	corev1 "iot/proto/core/v1"
)

// Exercise the production router/middleware -> real gRPC -> isolated PostgreSQL.
func contractIntegrationServer(t *testing.T) (http.Handler, *sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("IOT_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set IOT_POSTGRES_TEST_DSN")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("api_contract_%d", time.Now().UnixNano())
	if _, err = admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE"); _ = admin.Close() })
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	store, err := platform.NewPostgresStore(parsed.String(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rpc := grpc.NewServer()
	corev1.RegisterCoreServiceServer(rpc, core.NewService(store, nil))
	go func() { _ = rpc.Serve(listener) }()
	t.Cleanup(rpc.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	api := &Server{rpc: corev1.NewCoreServiceClient(conn)}
	server := newManagementHTTPServer()
	server.Use(rest.ToMiddleware(limitRequestBodyMiddleware))
	server.Use(rest.ToMiddleware(platform.BearerTokenMiddlewareWithTenants("admin", map[string]string{"tenant-t": "t"})))
	server.AddRoutes(api.routes())
	var handler http.Handler
	type bound struct{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(bound); !ok {
					panic(r)
				}
			}
		}()
		server.StartWithOpts(func(s *http.Server) { handler = s.Handler; panic(bound{}) })
	}()
	if handler == nil {
		t.Fatal("router not bound")
	}
	return handler, admin, schema
}
func contractRequest(t *testing.T, h http.Handler, method, path, token, body string, want int) map[string]json.RawMessage {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != want {
		t.Fatalf("%s %s: HTTP %d want %d: %s", method, path, rec.Code, want, rec.Body.String())
	}
	var response map[string]json.RawMessage
	if want != 404 {
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
	}
	return response
}
func TestCanonicalAPIContract(t *testing.T) {
	h, db, schema := contractIntegrationServer(t)
	for _, tenant := range []string{"t", "u"} {
		contractRequest(t, h, "POST", "/api/v1/tenants", "admin", fmt.Sprintf(`{"id":%q,"name":"Tenant"}`, tenant), 201)
	}
	for _, scope := range []string{"t", "u"} {
		for _, device := range []string{"d", "other"} {
			contractRequest(t, h, "POST", "/api/v1/tenants/"+scope+"/devices", "admin", fmt.Sprintf(`{"deviceId":%q,"productId":"p","secret":"test"}`, device), 201)
		}
	}
	list := contractRequest(t, h, "GET", "/api/v1/tenants/t/devices?pageSize=1", "tenant-t", "", 200)
	var devices []platform.Device
	_ = json.Unmarshal(list["items"], &devices)
	if len(devices) != 1 || devices[0].TenantID != "t" {
		t.Fatalf("tenant listing: %s", list["items"])
	}
	var cursor string
	_ = json.Unmarshal(list["nextCursor"], &cursor)
	if cursor == "" {
		t.Fatal("device cursor missing")
	}
	contractRequest(t, h, "GET", "/api/v1/tenants/u/devices?cursor="+url.QueryEscape(cursor), "admin", "", 400)
	contractRequest(t, h, "GET", "/api/v1/tenants/u/devices", "tenant-t", "", 403)
	for _, query := range []string{"tenantId=u", "pageSize=0", "pageSize=1&pageSize=2", "cursor=", "bad=%zz", "unknown=value"} {
		contractRequest(t, h, "GET", "/api/v1/tenants/t/devices?"+query, "tenant-t", "", 400)
	}
	contractRequest(t, h, "POST", "/api/v1/tenants/t/devices", "tenant-t", `{"tenantId":"t","deviceId":"new","productId":"p","secret":"x"}`, 400)
	for _, body := range []string{`{"deviceId":"x","deviceId":"y","productId":"p","secret":"x"}`, `{"deviceId":"x","productId":"   ","secret":"x"}`} {
		contractRequest(t, h, "POST", "/api/v1/tenants/t/devices", "tenant-t", body, 400)
	}
	contractRequest(t, h, "POST", "/api/v1/tenants", "admin", `{"id":"bad-name","name":"   "}`, 400)
	for _, old := range []string{"/api/v1/devices", "/api/v1/commands", "/api/v1/telemetry", "/api/v1/devices/t/d"} {
		contractRequest(t, h, "GET", old, "admin", "", 404)
	}

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	for i := 0; i < 6; i++ {
		kind := "temp"
		if i%2 != 0 {
			kind = "voltage"
		}
		contractRequest(t, h, "POST", "/api/v1/tenants/t/devices/d/telemetry", "tenant-t", fmt.Sprintf(`{"msgId":"m%d","ts":%d,"type":%q,"version":"v1","payload":{"value":%d}}`, i, base.Add(time.Duration(i)*time.Minute).UnixMilli(), kind, i), 202)
	}
	filter := url.Values{"startTime": {base.Add(time.Minute).Format(time.RFC3339Nano)}, "endTime": {base.Add(5 * time.Minute).Format(time.RFC3339Nano)}, "type": {"temp"}, "pageSize": {"1"}, "sort": {"desc"}}
	var seen []string
	for {
		body := contractRequest(t, h, "GET", "/api/v1/tenants/t/devices/d/telemetry?"+filter.Encode(), "tenant-t", "", 200)
		var rows []platform.TelemetryRecord
		_ = json.Unmarshal(body["items"], &rows)
		for _, row := range rows {
			seen = append(seen, row.MsgID)
		}
		_ = json.Unmarshal(body["nextCursor"], &cursor)
		if cursor == "" {
			break
		}
		filter.Set("cursor", cursor)
		changed, _ := url.ParseQuery(filter.Encode())
		changed.Set("type", "voltage")
		contractRequest(t, h, "GET", "/api/v1/tenants/t/devices/d/telemetry?"+changed.Encode(), "tenant-t", "", 400)
	}
	if strings.Join(seen, ",") != "m4,m2" {
		t.Fatalf("event-time/type/pagination %v", seen)
	}
	boundQuery := url.Values{"startTime": {base.Add(2 * time.Minute).Format(time.RFC3339Nano)}, "endTime": {base.Add(4 * time.Minute).Format(time.RFC3339Nano)}, "type": {"temp"}}
	bounded := contractRequest(t, h, "GET", "/api/v1/tenants/t/devices/d/telemetry?"+boundQuery.Encode(), "tenant-t", "", 200)
	var boundaryRows []platform.TelemetryRecord
	_ = json.Unmarshal(bounded["items"], &boundaryRows)
	if len(boundaryRows) != 1 || boundaryRows[0].MsgID != "m2" {
		t.Fatalf("half-open range failed: %s", bounded["items"])
	}
	for _, query := range []string{"startTime=bad", "startTime=" + url.QueryEscape(base.Add(time.Hour).Format(time.RFC3339Nano)) + "&endTime=" + url.QueryEscape(base.Format(time.RFC3339Nano)), "sort=wrong", "type="} {
		contractRequest(t, h, "GET", "/api/v1/tenants/t/devices/d/telemetry?"+query, "tenant-t", "", 400)
	}

	created := contractRequest(t, h, "POST", "/api/v1/tenants/t/devices/d/commands", "tenant-t", `{"payload":{"switch":"on"}}`, 201)
	var id string
	_ = json.Unmarshal(created["id"], &id)
	ackPath := "/api/v1/tenants/t/devices/d/commands/" + id + "/acknowledgements"
	result := `{"status":"failed","errorCode":"MOTOR_BLOCKED","payload":{"reason":"jammed"}}`
	ack := contractRequest(t, h, "POST", ackPath, "tenant-t", result, 200)
	var state string
	_ = json.Unmarshal(ack["status"], &state)
	if state != "failed" || !strings.Contains(string(ack["result"]), "MOTOR_BLOCKED") {
		t.Fatalf("result lost: %s", ack["result"])
	}
	contractRequest(t, h, "POST", ackPath, "tenant-t", result, 200)
	contractRequest(t, h, "POST", ackPath, "tenant-t", `{"status":"acked"}`, 409)
	detail := contractRequest(t, h, "GET", "/api/v1/tenants/t/commands/"+id, "tenant-t", "", 200)
	if !strings.Contains(string(detail["result"]), "jammed") {
		t.Fatal("stored result not exposed")
	}
	body := contractRequest(t, h, "GET", "/api/v1/tenants/t/commands?deviceId=d&status=failed&sort=asc", "tenant-t", "", 200)
	var cmds []platform.Command
	_ = json.Unmarshal(body["items"], &cmds)
	if len(cmds) != 1 || cmds[0].ID != id || cmds[0].Result == nil {
		t.Fatalf("command filter/result: %s", body["items"])
	}
	contractRequest(t, h, "POST", "/api/v1/tenants/t/devices/other/commands", "tenant-t", `{"payload":{}}`, 201)
	deviceCommands := contractRequest(t, h, "GET", "/api/v1/tenants/t/devices/d/commands", "tenant-t", "", 200)
	var deviceRows []platform.Command
	_ = json.Unmarshal(deviceCommands["items"], &deviceRows)
	if len(deviceRows) != 1 || deviceRows[0].ID != id {
		t.Fatalf("device-scoped commands leaked: %s", deviceCommands["items"])
	}
	oldest := contractRequest(t, h, "GET", "/api/v1/tenants/t/commands?endTime=0001-01-01T00:00:00Z", "tenant-t", "", 200)
	var oldestRows []platform.Command
	_ = json.Unmarshal(oldest["items"], &oldestRows)
	if len(oldestRows) != 0 {
		t.Fatal("explicit minimum endTime was treated as absent")
	}
	contractRequest(t, h, "GET", "/api/v1/tenants/t/commands?status=unknown", "tenant-t", "", 400)
	for _, payload := range []string{"null", "[]", "true", "123"} {
		contractRequest(t, h, "POST", "/api/v1/tenants/t/devices/d/commands", "tenant-t", `{"payload":`+payload+`}`, 400)
	}
	var acknowledgements, events int
	if err := db.QueryRow("SELECT count(*) FROM "+schema+".command_ack WHERE command_id=$1", id).Scan(&acknowledgements); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT count(*) FROM "+schema+".command_events WHERE command_id=$1 AND event_type='failed'", id).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if acknowledgements != 1 || events != 1 {
		t.Fatalf("duplicate audit: %d/%d", acknowledgements, events)
	}
}
