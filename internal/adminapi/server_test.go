package adminapi

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"iot/internal/contracts"
	"iot/internal/core"
	"iot/internal/platform"
	corev1 "iot/proto/core/v1"
)

func TestRPCClientConfUsesDirectEndpoints(t *testing.T) {
	t.Setenv("IOT_CORE_ENDPOINTS", "iot-core:9001,iot-core-2:9001")

	conf := rpcClientConf()
	if len(conf.Etcd.Hosts) != 0 || conf.Etcd.Key != "" {
		t.Fatalf("rpc client still configures etcd: %+v", conf.Etcd)
	}
	if len(conf.Endpoints) != 2 || conf.Endpoints[0] != "iot-core:9001" || conf.Endpoints[1] != "iot-core-2:9001" {
		t.Fatalf("direct endpoints = %v", conf.Endpoints)
	}
}

// TestHTTPStatusFromGRPC pins the code-to-status mapping that replaced message
// substring matching. The old mapping turned "tenantId contains invalid MQTT
// topic characters" into 502 (no pattern matched) and "command does not belong
// to device" into 502 as well.
func TestHTTPStatusFromGRPC(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		want int
	}{
		{codes.InvalidArgument, http.StatusBadRequest},
		{codes.FailedPrecondition, http.StatusBadRequest},
		{codes.NotFound, http.StatusNotFound},
		{codes.AlreadyExists, http.StatusConflict},
		{codes.Aborted, http.StatusConflict},
		{codes.Unauthenticated, http.StatusUnauthorized},
		{codes.PermissionDenied, http.StatusForbidden},
		{codes.ResourceExhausted, http.StatusTooManyRequests},
		{codes.DeadlineExceeded, http.StatusGatewayTimeout},
		{codes.Unavailable, http.StatusBadGateway},
		{codes.Internal, http.StatusInternalServerError},
		{codes.Unknown, http.StatusInternalServerError},
	} {
		if got := httpStatusFromGRPC(status.Error(tc.code, "boom")); got != tc.want {
			t.Errorf("httpStatusFromGRPC(%v) = %d, want %d", tc.code, got, tc.want)
		}
	}

	// A nil error is codes.OK, so it must map to success rather than 500.
	if got := httpStatusFromGRPC(nil); got != http.StatusOK {
		t.Errorf("httpStatusFromGRPC(nil) = %d, want %d", got, http.StatusOK)
	}

	// A non-status error must not panic and must fall back to 500.
	if got := httpStatusFromGRPC(context.Canceled); got != http.StatusInternalServerError {
		t.Errorf("httpStatusFromGRPC(context.Canceled) = %d, want %d", got, http.StatusInternalServerError)
	}
}

// TestZRPCClientPreservesStatusCode is the reason this mapping is safe: the
// gateway reaches iot-core through a go-zero zrpc client, and if that client
// rewrapped errors such that status.FromError stopped recognising them, every
// failure would silently collapse into the default branch. This drives a real
// gRPC server through a real zrpc client to prove the code survives.
func TestZRPCClientPreservesStatusCode(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	server := grpc.NewServer()
	corev1.RegisterCoreServiceServer(server, statusRepoServices{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	conf := zrpc.NewDirectClientConf([]string{listener.Addr().String()}, "management-api", "")
	conf.Timeout = 5000
	client, err := zrpc.NewClient(conf, zrpc.WithUnaryClientInterceptor(platform.UnaryClientRequestIDInterceptor()))
	if err != nil {
		t.Fatalf("zrpc.NewClient: %v", err)
	}
	defer client.Conn().Close()

	rpc := corev1.NewCoreServiceClient(client.Conn())

	_, err = rpc.GetCommand(context.Background(), &corev1.GetCommandRequest{Id: "cmd-1", TenantId: "tenant-a"})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("GetCommand() over zrpc: code = %v, want NotFound (err=%v)", got, err)
	}
	if httpStatusFromGRPC(err) != http.StatusNotFound {
		t.Fatalf("GetCommand() over zrpc mapped to %d, want %d", httpStatusFromGRPC(err), http.StatusNotFound)
	}

	_, err = rpc.CreateTenant(context.Background(), &corev1.CreateTenantRequest{Id: "tenant/#", Name: "bad"})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("CreateTenant() over zrpc: code = %v, want InvalidArgument (err=%v)", got, err)
	}
	if httpStatusFromGRPC(err) != http.StatusBadRequest {
		t.Fatalf("CreateTenant() over zrpc mapped to %d, want %d", httpStatusFromGRPC(err), http.StatusBadRequest)
	}
}

// TestWriteRPCErrorBodyOmitsTransportPrefix keeps the response body to the
// message iot-core produced, rather than the "rpc error: code = ... desc = ..."
// framing.
func TestWriteRPCErrorBodyOmitsTransportPrefix(t *testing.T) {
	rec := httptest.NewRecorder()
	writeRPCError(rec, status.Error(codes.NotFound, "command not found"))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if body := rec.Body.String(); body != "{\"error\":\"command not found\"}\n" {
		t.Fatalf("body = %q, want the bare iot-core message", body)
	}
}

// statusRepoServices returns fixed status errors so the client round trip can be
// asserted without a database.
type statusRepoServices struct {
	corev1.UnimplementedCoreServiceServer
}

func (statusRepoServices) GetCommand(context.Context, *corev1.GetCommandRequest) (*corev1.GetCommandResponse, error) {
	return nil, status.Error(codes.NotFound, "command not found")
}

func (statusRepoServices) CreateTenant(context.Context, *corev1.CreateTenantRequest) (*corev1.Tenant, error) {
	return nil, status.Error(codes.InvalidArgument, "tenantId contains invalid MQTT topic characters")
}

// TestRealCoreStatusCodesThroughGateway drives the real iot-core service, with a
// stub repository, through a real zrpc client. The stub-server test above proves
// the transport preserves codes; this proves the handlers actually produce them,
// including the case that used to surface as 502 Bad Gateway.
func TestRealCoreStatusCodesThroughGateway(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	server := grpc.NewServer()
	corev1.RegisterCoreServiceServer(server, core.NewService(commandLookupRepo{}, nil))
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	conf := zrpc.NewDirectClientConf([]string{listener.Addr().String()}, "management-api", "")
	conf.Timeout = 5000
	client, err := zrpc.NewClient(conf, zrpc.WithUnaryClientInterceptor(platform.UnaryClientRequestIDInterceptor()))
	if err != nil {
		t.Fatalf("zrpc.NewClient: %v", err)
	}
	defer client.Conn().Close()
	rpc := corev1.NewCoreServiceClient(client.Conn())

	for _, tc := range []struct {
		name string
		req  *corev1.GetCommandRequest
		want int
	}{
		{"owning tenant", &corev1.GetCommandRequest{Id: "cmd-a", TenantId: "tenant-a"}, http.StatusOK},
		{"missing tenant", &corev1.GetCommandRequest{Id: "cmd-a"}, http.StatusBadRequest},
		{"foreign tenant", &corev1.GetCommandRequest{Id: "cmd-a", TenantId: "tenant-b"}, http.StatusNotFound},
		{"unknown id", &corev1.GetCommandRequest{Id: "missing", TenantId: "tenant-a"}, http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := rpc.GetCommand(context.Background(), tc.req)
			if got := httpStatusFromGRPC(err); got != tc.want {
				t.Fatalf("GetCommand(%+v) mapped to %d, want %d (err=%v)", tc.req, got, tc.want, err)
			}
		})
	}

	// "tenantId contains invalid MQTT topic characters" matched no substring in
	// the old gateway mapping and surfaced as 502; it must be a 400.
	_, err = rpc.CreateTenant(context.Background(), &corev1.CreateTenantRequest{Id: "tenant/#", Name: "bad"})
	if got := httpStatusFromGRPC(err); got != http.StatusBadRequest {
		t.Fatalf("CreateTenant() invalid topic mapped to %d, want %d (err=%v)", got, http.StatusBadRequest, err)
	}
}

// commandLookupRepo serves one command owned by tenant-a.
type commandLookupRepo struct{}

func (commandLookupRepo) CreateTenant(platform.Tenant) (platform.Tenant, error) {
	return platform.Tenant{}, nil
}
func (commandLookupRepo) ListTenants() []platform.Tenant { return nil }
func (commandLookupRepo) CreateDevice(platform.Device) (platform.Device, error) {
	return platform.Device{}, nil
}
func (commandLookupRepo) ListDevices() []platform.Device { return nil }
func (commandLookupRepo) GetDevice(string, string) (platform.Device, bool) {
	return platform.Device{}, false
}
func (commandLookupRepo) RecordTelemetry(contracts.Envelope) (platform.TelemetryRecord, error) {
	return platform.TelemetryRecord{}, nil
}
func (commandLookupRepo) ListTelemetry(string, string) []platform.TelemetryRecord { return nil }
func (commandLookupRepo) GetDeviceStatus(string, string) (platform.DeviceStatus, bool) {
	return platform.DeviceStatus{}, false
}
func (commandLookupRepo) CreateCommand(string, string, json.RawMessage) (platform.Command, error) {
	return platform.Command{}, nil
}
func (commandLookupRepo) AckCommand(string, string, string) (platform.Command, error) {
	return platform.Command{}, nil
}
func (commandLookupRepo) ListCommands() []platform.Command { return nil }
func (commandLookupRepo) GetCommand(id string) (platform.Command, bool) {
	if id != "cmd-a" {
		return platform.Command{}, false
	}
	return platform.Command{ID: "cmd-a", TenantID: "tenant-a", DeviceID: "device-a"}, true
}
