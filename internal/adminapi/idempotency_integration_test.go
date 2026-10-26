package adminapi

import (
	"bytes"
	"context"
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"iot/internal/contracts"
	"iot/internal/core"
	"iot/internal/platform"
	corev1 "iot/proto/core/v1"
)

// Exercise the production REST -> protobuf/gRPC -> PostgreSQL boundary.
func TestCommandIdempotencyThroughRESTAndGRPC(t *testing.T) {
	dsn := os.Getenv("IOT_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set IOT_POSTGRES_TEST_DSN")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("api_regression_%d", time.Now().UnixNano())
	if _, err = admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE"); _ = admin.Close() })
	scoped, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := scoped.Query()
	query.Set("search_path", schema)
	scoped.RawQuery = query.Encode()
	store, err := platform.NewPostgresStore(scoped.String(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err = store.CreateTenant(platform.Tenant{ID: "t", Name: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.CreateDevice(platform.Device{TenantID: "t", DeviceID: "d", ProductID: "p", Secret: "secret"}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rpcServer := grpc.NewServer()
	corev1.RegisterCoreServiceServer(rpcServer, core.NewService(store, nil))
	go func() { _ = rpcServer.Serve(listener) }()
	t.Cleanup(rpcServer.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	server := &Server{rpc: corev1.NewCoreServiceClient(conn)}
	for _, payload := range []string{"", "null", "[]", "42", `"text"`, `{"values":[` + strings.TrimSuffix(strings.Repeat("1e131071,", 40), ",") + `]}`} {
		for _, recordRPC := range []bool{false, true} {
			var err error
			if recordRPC {
				_, err = server.rpc.RecordTelemetry(t.Context(), &corev1.RecordTelemetryRequest{Telemetry: &corev1.TelemetryRecord{MsgId: "invalid-payload", TenantId: "t", DeviceId: "d", Ts: time.Now().UnixMilli(), Type: "temp", Version: "v1", Payload: []byte(payload)}})
			} else {
				_, err = server.rpc.IngestTelemetry(t.Context(), &corev1.IngestTelemetryRequest{MsgId: "invalid-payload", TenantId: "t", DeviceId: "d", Ts: time.Now().UnixMilli(), Type: "temp", Version: "v1", Payload: []byte(payload)})
			}
			if httpStatusFromGRPC(err) != 400 {
				t.Errorf("payload %q record=%v status=%d error=%v", payload, recordRPC, httpStatusFromGRPC(err), err)
			}
		}
	}
	for _, table := range []string{"tenants", "devices", "commands", "telemetry_records"} {
		t.Run("cancel-list-"+table, func(t *testing.T) {
			lock, err := admin.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Rollback()
			if _, err = lock.Exec("LOCK TABLE " + schema + "." + table + " IN ACCESS EXCLUSIVE MODE"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				switch table {
				case "tenants":
					_, err = server.rpc.ListTenants(ctx, &corev1.ListTenantsRequest{})
				case "devices":
					_, err = server.rpc.ListDevices(ctx, &corev1.ListDevicesRequest{TenantId: "t"})
				case "commands":
					_, err = server.rpc.ListCommands(ctx, &corev1.ListCommandsRequest{TenantId: "t"})
				case "telemetry_records":
					_, err = server.rpc.ListTelemetry(ctx, &corev1.ListTelemetryRequest{TenantId: "t", DeviceId: "d"})
				}
				done <- err
			}()
			waiting := func() int {
				t.Helper()
				var n int
				if err = admin.QueryRow(`SELECT COUNT(*) FROM pg_locks WHERE relation=$1::regclass AND NOT granted`, schema+"."+table).Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}
			limit := time.Now().Add(time.Second)
			for waiting() == 0 && time.Now().Before(limit) {
				time.Sleep(5 * time.Millisecond)
			}
			if waiting() == 0 {
				t.Fatal("list query did not reach held table lock")
			}
			cancel()
			if err = <-done; err == nil {
				t.Fatal("cancelled list succeeded")
			}
			limit = time.Now().Add(400 * time.Millisecond)
			for waiting() != 0 && time.Now().Before(limit) {
				time.Sleep(5 * time.Millisecond)
			}
			if waiting() != 0 {
				t.Fatal("server query remained blocked after RPC cancellation")
			}
		})
	}
	for name, handler := range map[string]http.HandlerFunc{"tenants": server.listTenantsHandler, "tenants/t/devices": server.listDevicesHandler, "tenants/t/commands": server.listCommandsHandler, "tenants/t/devices/d/telemetry": server.listTelemetryHandler} {
		for _, query := range []string{"cursor=" + url.QueryEscape("%%%"), "cursor=%%%", "cursor=_w", "cursor=AA", "pageSize=101", "pageSize=10abc", "pageSize=1.5"} {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/"+name+"?"+query, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("%s invalid page %s status=%d body=%s", name, query, rec.Code, rec.Body.String())
			}
		}
	}
	handler := platform.BearerTokenMiddlewareWithTenants("admin", map[string]string{"tenant-token": "t"})(http.HandlerFunc(server.createCommandHandler))
	post := func(key, payload, tenant string) (int, string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants/"+tenant+"/devices/d/commands", bytes.NewBufferString(fmt.Sprintf(`{"payload":%s}`, payload)))
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Authorization", "Bearer tenant-token")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		var body struct {
			ID string `json:"id"`
		}
		if rec.Code == http.StatusCreated {
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
		}
		return rec.Code, body.ID
	}
	code, first := post("request-1", `{"switch":"on"}`, "t")
	if code != 201 || first == "" {
		t.Fatalf("create: status=%d id=%s", code, first)
	}
	if code, id := post("request-1", `{ "switch": "on" }`, "t"); code != 201 || id != first {
		t.Fatalf("retry: status=%d id=%s", code, id)
	}
	if code, _ := post("request-1", `{"switch":"off"}`, "t"); code != 409 {
		t.Fatalf("changed payload status=%d", code)
	}
	if code, _ := post("request-1", `{}`, "foreign"); code != 403 {
		t.Fatalf("cross-tenant status=%d", code)
	}
	if code, _ := post("with space", `{}`, "t"); code != 400 {
		t.Fatalf("invalid key status=%d", code)
	}

	// Exercise the real gateway -> gRPC -> PG boundary with both authorization paths.
	for _, token := range []string{"admin", "tenant-token"} {
		for _, tc := range []struct {
			path, body string
			handler    http.HandlerFunc
		}{
			{"/api/v1/tenants/t/devices", `{"deviceId":"long-secret","productId":"p","secret":"` + strings.Repeat("x", 73) + `"}`, server.createDeviceHandler},
			{"/api/v1/tenants/t/devices/d/commands", `{"payload":{"x":"\u0000"}}`, server.createCommandHandler},
			{"/api/v1/tenants/t/devices/d/commands", `{"payload":{"x":1e1000000}}`, server.createCommandHandler},
			{"/api/v1/tenants/t/devices/d/commands", `{"payload":{"x":"` + strings.Repeat("x", 1048400) + `"}}`, server.createCommandHandler},
			{"/api/v1/tenants/t/devices/d/commands", `{"payload":{}} garbage`, server.createCommandHandler},
			{"/api/v1/tenants/t/devices/d/commands", `{"payload":{}} {}`, server.createCommandHandler},
		} {
			req := httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()
			h := limitRequestBodyMiddleware(platform.BearerTokenMiddlewareWithTenants("admin", map[string]string{"tenant-token": "t"})(tc.handler))
			h.ServeHTTP(rec, req)
			if rec.Code != 400 {
				t.Errorf("token=%s path=%s invalid input status=%d", token, tc.path, rec.Code)
			}
		}
	}
	testByteBoundedRPCPages(t, store, server, admin, schema)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	response, err := server.rpc.GetDevice(t.Context(), &corev1.GetDeviceRequest{TenantId: "t", DeviceId: "d"})
	if response != nil || httpStatusFromGRPC(err) != 500 {
		t.Fatalf("database failure should not become 404: %+v %v", response, err)
	}
}

func testByteBoundedRPCPages(t *testing.T, store *platform.PostgresStore, server *Server, admin *sql.DB, schema string) {
	large := strings.Repeat("x", 900000)
	for i := 0; i < 6; i++ {
		if _, err := store.CreateCommand("t", "d", json.RawMessage(`{"x":"`+large+`"}`)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordTelemetry(contracts.Envelope{MsgID: fmt.Sprintf("large-%d", i), TenantID: "t", DeviceID: "d", Ts: time.Now().UnixMilli(), Type: "temp", Version: "v1", Payload: json.RawMessage(`{"x":"` + large + `"}`)}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateTenant(platform.Tenant{ID: fmt.Sprintf("page-%d", i), Name: "historical tenant"}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateDevice(platform.Device{TenantID: "t", DeviceID: fmt.Sprintf("large-%d", i), ProductID: "historical-product", Secret: "secret"}); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec("UPDATE "+schema+".tenants SET name=$1 WHERE id=$2", large, fmt.Sprintf("page-%d", i)); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec("UPDATE "+schema+".devices SET product_id=$1 WHERE tenant_id='t' AND device_id=$2", large, fmt.Sprintf("large-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	check := func(name string, want int, list func(string) ([]string, string, proto.Message, error)) {
		t.Helper()
		cursor := ""
		seen := map[string]bool{}
		for page := 0; page < 20; page++ {
			ids, next, message, err := list(cursor)
			if err != nil {
				t.Fatalf("%s RPC failed: %v", name, err)
			}
			if proto.Size(message) > platform.MaxListResponseBytes {
				t.Fatalf("%s response exceeds byte budget: %d", name, proto.Size(message))
			}
			for _, id := range ids {
				if seen[id] {
					t.Fatalf("%s duplicate ID", name)
				}
				seen[id] = true
			}
			if next == "" {
				if len(seen) != want {
					t.Fatalf("%s lost rows: %d want %d", name, len(seen), want)
				}
				return
			}
			if len(ids) == 0 || next == cursor {
				t.Fatal("pagination cannot advance")
			}
			cursor = next
		}
		t.Fatal("pagination never finished")
	}
	check("commands", 7, func(c string) ([]string, string, proto.Message, error) {
		r, e := server.rpc.ListCommands(t.Context(), &corev1.ListCommandsRequest{TenantId: "t", Cursor: c})
		var ids []string
		for _, v := range r.GetCommands() {
			ids = append(ids, v.GetId())
		}
		return ids, r.GetNextCursor(), r, e
	})
	check("telemetry", 6, func(c string) ([]string, string, proto.Message, error) {
		r, e := server.rpc.ListTelemetry(t.Context(), &corev1.ListTelemetryRequest{TenantId: "t", DeviceId: "d", Cursor: c})
		var ids []string
		for _, v := range r.GetRecords() {
			ids = append(ids, v.GetMsgId())
		}
		return ids, r.GetNextCursor(), r, e
	})
	check("tenants", 7, func(c string) ([]string, string, proto.Message, error) {
		r, e := server.rpc.ListTenants(t.Context(), &corev1.ListTenantsRequest{Cursor: c})
		var ids []string
		for _, v := range r.GetTenants() {
			ids = append(ids, v.GetId())
		}
		return ids, r.GetNextCursor(), r, e
	})
	check("devices", 7, func(c string) ([]string, string, proto.Message, error) {
		r, e := server.rpc.ListDevices(t.Context(), &corev1.ListDevicesRequest{TenantId: "t", Cursor: c})
		var ids []string
		for _, v := range r.GetDevices() {
			ids = append(ids, v.GetDeviceId())
		}
		return ids, r.GetNextCursor(), r, e
	})
	rec := httptest.NewRecorder()
	server.listCommandsHandler(rec, httptest.NewRequest("GET", "/api/v1/tenants/t/commands", nil))
	if rec.Code != 200 {
		t.Fatalf("large command HTTP list status %d", rec.Code)
	}
}
