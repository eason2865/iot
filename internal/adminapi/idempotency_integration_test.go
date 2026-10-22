package adminapi

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	for name, handler := range map[string]http.HandlerFunc{"tenants": server.listTenantsHandler, "devices": server.listDevicesHandler, "commands": server.listCommandsHandler, "devices/t/d/telemetry": server.listTelemetryHandler} {
		for _, query := range []string{"cursor=" + url.QueryEscape("%%%"), "cursor=%%%", "cursor=_w", "cursor=AA", "pageSize=101"} {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/"+name+"?tenantId=t&"+query, nil)
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
		req := httptest.NewRequest(http.MethodPost, "/api/v1/commands", bytes.NewBufferString(fmt.Sprintf(`{"tenantId":%q,"deviceId":"d","payload":%s}`, tenant, payload)))
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
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	response, err := server.rpc.GetDevice(t.Context(), &corev1.GetDeviceRequest{TenantId: "t", DeviceId: "d"})
	if response != nil || httpStatusFromGRPC(err) != 500 {
		t.Fatalf("database failure should not become 404: %+v %v", response, err)
	}
}
