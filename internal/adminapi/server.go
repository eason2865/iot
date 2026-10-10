package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"iot/internal/contracts"
	"iot/internal/platform"
	"iot/internal/runtimeconfig"
	corev1 "iot/proto/core/v1"
)

// maxRequestBodyBytes caps every management API request body at 1 MiB. All
// bodies here are small JSON documents (tenant/device/command/telemetry
// envelopes); without a cap a client could force unbounded memory use while
// the handler decodes.
const maxRequestBodyBytes = 1 << 20

type Server struct {
	rpc     corev1.CoreServiceClient
	metrics *platform.Metrics
}

// limitRequestBodyMiddleware enforces maxRequestBodyBytes centrally, so no
// handler can forget to bound its decode. Handlers see an error from
// MaxBytesReader when the limit is exceeded.
func limitRequestBodyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		}
		next.ServeHTTP(w, r)
	})
}

func Run() error {
	client, err := newRPCClient()
	if err != nil {
		return err
	}
	defer client.Conn().Close()

	platform.ConfigureStdLogger("management-api")
	metrics := platform.NewMetrics()
	httpServer := rest.MustNewServer(rest.RestConf{
		ServiceConf: service.ServiceConf{
			Name:      "management-api",
			Telemetry: platform.TraceConfig("management-api"),
		},
		Host:    listenHost(),
		Port:    listenPort(),
		Timeout: 3000,
		Middlewares: rest.MiddlewaresConf{
			Trace:      true,
			Log:        true,
			Prometheus: true,
			Recover:    true,
			Metrics:    true,
			Timeout:    true,
		},
	})
	httpServer.Use(rest.ToMiddleware(limitRequestBodyMiddleware))
	httpServer.Use(rest.ToMiddleware(platform.RequestIDHTTPMiddleware))
	httpServer.Use(rest.ToMiddleware(metrics.HTTPMiddleware()))
	// MANAGEMENT_API_TOKEN is the global service token. MANAGEMENT_API_TOKENS
	// optionally adds tenant-bound tokens as a JSON map {"token":"tenantId"};
	// such a token may only touch requests naming its own tenant.
	tenantTokens, err := platform.ParseTenantBoundTokens(runtimeconfig.EnvOrDefault("MANAGEMENT_API_TOKENS", ""))
	if err != nil {
		return fmt.Errorf("invalid MANAGEMENT_API_TOKENS configuration")
	}
	httpServer.Use(rest.ToMiddleware(platform.BearerTokenMiddlewareWithTenants(runtimeconfig.EnvOrDefault("MANAGEMENT_API_TOKEN", ""), tenantTokens)))
	defer httpServer.Stop()

	go serveManagementAPIMetrics(metrics.Handler(), managementAPIMetricsHost(), managementAPIMetricsPort(), runtimeconfig.EnvOrDefault("MANAGEMENT_API_METRICS_PATH", "/metrics"))

	api := &Server{
		rpc:     corev1.NewCoreServiceClient(client.Conn()),
		metrics: metrics,
	}
	httpServer.AddRoutes(api.routes())
	httpServer.Start()
	return nil
}

func newRPCClient() (zrpc.Client, error) {
	conf := rpcClientConf()
	conf.Timeout = 5000
	tlsCreds, err := platform.GRPCClientTLSCredentials()
	if err != nil {
		return nil, err
	}
	clientOpts := []zrpc.ClientOption{zrpc.WithUnaryClientInterceptor(platform.UnaryClientRequestIDInterceptor())}
	if tlsCreds != nil {
		clientOpts = append(clientOpts, zrpc.WithTransportCredentials(tlsCreds))
	}
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		client, err := zrpc.NewClient(conf, clientOpts...)
		if err == nil {
			return client, nil
		}
		lastErr = err
		time.Sleep(1 * time.Second)
	}
	return nil, lastErr
}

func rpcClientConf() zrpc.RpcClientConf {
	return zrpc.NewDirectClientConf(
		runtimeconfig.SplitCSV(runtimeconfig.EnvOrDefault("IOT_CORE_ENDPOINTS", "127.0.0.1:9001")),
		"management-api",
		"",
	)
}

func (s *Server) routes() []rest.Route {
	return []rest.Route{
		{Method: http.MethodGet, Path: "/healthz", Handler: s.healthHandler},
		{Method: http.MethodGet, Path: "/readyz", Handler: s.readyHandler},
		{Method: http.MethodGet, Path: "/schemas/mqtt-envelope.json", Handler: s.mqttEnvelopeSchemaHandler},
		{Method: http.MethodPost, Path: "/api/v1/tenants", Handler: s.createTenantHandler},
		{Method: http.MethodGet, Path: "/api/v1/tenants", Handler: s.listTenantsHandler},
		{Method: http.MethodPost, Path: "/api/v1/devices", Handler: s.createDeviceHandler},
		{Method: http.MethodGet, Path: "/api/v1/devices", Handler: s.listDevicesHandler},
		{Method: http.MethodGet, Path: "/api/v1/devices/:tenantId/:deviceId", Handler: s.getDeviceHandler},
		{Method: http.MethodGet, Path: "/api/v1/devices/:tenantId/:deviceId/status", Handler: s.getDeviceStatusHandler},
		{Method: http.MethodGet, Path: "/api/v1/devices/:tenantId/:deviceId/telemetry", Handler: s.listTelemetryHandler},
		{Method: http.MethodPost, Path: "/api/v1/telemetry", Handler: s.ingestTelemetryHandler},
		{Method: http.MethodPost, Path: "/api/v1/commands", Handler: s.createCommandHandler},
		{Method: http.MethodGet, Path: "/api/v1/commands", Handler: s.listCommandsHandler},
		{Method: http.MethodGet, Path: "/api/v1/commands/:id", Handler: s.getCommandHandler},
		{Method: http.MethodPost, Path: "/api/v1/commands/:id/ack", Handler: s.ackCommandHandler},
	}
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ok",
		"serviceName": "management-api",
	})
}

// readyHandler is the deep readiness probe: management-api is useless without
// its iot-core backend, so it verifies the gRPC path (which in turn reaches
// PostgreSQL) with a bounded ListTenants call. A dead dependency must pull the
// pod out of the Service endpoints instead of letting it black-hole requests
// behind a static "ok".
func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := s.rpc.ListTenants(ctx, &corev1.ListTenantsRequest{PageSize: 1}); err != nil {
		writeError(w, http.StatusServiceUnavailable, "iot-core unreachable: "+status.Convert(err).Message())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ready",
		"serviceName": "management-api",
	})
}

func (s *Server) mqttEnvelopeSchemaHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, contracts.MQTTEnvelopeSchema())
}

func (s *Server) createTenantHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.rpc.CreateTenant(r.Context(), &corev1.CreateTenantRequest{Id: req.ID, Name: req.Name})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, platform.Tenant{ID: resp.GetId(), Name: resp.GetName()})
}

func (s *Server) listTenantsHandler(w http.ResponseWriter, r *http.Request) {
	pageSize, cursor, err := pageFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.rpc.ListTenants(r.Context(), &corev1.ListTenantsRequest{PageSize: int32(pageSize), Cursor: cursor})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	tenants := make([]platform.Tenant, 0, len(resp.GetTenants()))
	for _, tenant := range resp.GetTenants() {
		tenants = append(tenants, platform.Tenant{ID: tenant.GetId(), Name: tenant.GetName()})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": tenants, "nextCursor": resp.GetNextCursor()})
}

func (s *Server) createDeviceHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TenantID  string `json:"tenantId"`
		DeviceID  string `json:"deviceId"`
		ProductID string `json:"productId"`
		Secret    string `json:"secret"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.rpc.CreateDevice(r.Context(), &corev1.CreateDeviceRequest{
		TenantId:  req.TenantID,
		DeviceId:  req.DeviceID,
		ProductId: req.ProductID,
		Secret:    req.Secret,
	})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, deviceFromPB(resp))
}

func (s *Server) listDevicesHandler(w http.ResponseWriter, r *http.Request) {
	pageSize, cursor, err := pageFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.rpc.ListDevices(r.Context(), &corev1.ListDevicesRequest{PageSize: int32(pageSize), Cursor: cursor})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	devices := make([]platform.Device, 0, len(resp.GetDevices()))
	for _, device := range resp.GetDevices() {
		devices = append(devices, deviceFromPB(device))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": devices, "nextCursor": resp.GetNextCursor()})
}

func (s *Server) getDeviceHandler(w http.ResponseWriter, r *http.Request) {
	tenantID, deviceID, ok := splitDevicePath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	resp, err := s.rpc.GetDevice(r.Context(), &corev1.GetDeviceRequest{TenantId: tenantID, DeviceId: deviceID})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, deviceFromPB(resp.GetDevice()))
}

func (s *Server) getDeviceStatusHandler(w http.ResponseWriter, r *http.Request) {
	tenantID, deviceID, ok := splitDevicePath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	resp, err := s.rpc.GetDeviceStatus(r.Context(), &corev1.GetDeviceStatusRequest{TenantId: tenantID, DeviceId: deviceID})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, deviceStatusFromPB(resp.GetStatus()))
}

func (s *Server) listTelemetryHandler(w http.ResponseWriter, r *http.Request) {
	tenantID, deviceID, ok := splitDevicePath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	pageSize, cursor, err := pageFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.rpc.ListTelemetry(r.Context(), &corev1.ListTelemetryRequest{TenantId: tenantID, DeviceId: deviceID, PageSize: int32(pageSize), Cursor: cursor})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	records := make([]platform.TelemetryRecord, 0, len(resp.GetRecords()))
	for _, record := range resp.GetRecords() {
		records = append(records, telemetryFromPB(record))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": records, "nextCursor": resp.GetNextCursor()})
}

func (s *Server) ingestTelemetryHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MsgID    string          `json:"msgId"`
		TenantID string          `json:"tenantId"`
		DeviceID string          `json:"deviceId"`
		Ts       int64           `json:"ts"`
		Type     string          `json:"type"`
		Version  string          `json:"version"`
		Payload  json.RawMessage `json:"payload"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.rpc.IngestTelemetry(r.Context(), &corev1.IngestTelemetryRequest{
		MsgId:    req.MsgID,
		TenantId: req.TenantID,
		DeviceId: req.DeviceID,
		Ts:       req.Ts,
		Type:     req.Type,
		Version:  req.Version,
		Payload:  []byte(req.Payload),
	})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, telemetryFromPB(resp.GetRecord()))
}

func (s *Server) createCommandHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TenantID string          `json:"tenantId"`
		DeviceID string          `json:"deviceId"`
		Payload  json.RawMessage `json:"payload"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.rpc.CreateCommand(r.Context(), &corev1.CreateCommandRequest{
		TenantId: req.TenantID,
		DeviceId: req.DeviceID,
		Payload:  []byte(req.Payload),
	})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, commandFromPB(resp.GetCommand()))
}

func (s *Server) listCommandsHandler(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenantId")
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "tenantId is required")
		return
	}
	if !contracts.IsValidTopicPart(tenantID) {
		writeError(w, http.StatusBadRequest, "tenantId contains invalid MQTT topic characters")
		return
	}
	pageSize, cursor, err := pageFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.rpc.ListCommands(r.Context(), &corev1.ListCommandsRequest{PageSize: int32(pageSize), Cursor: cursor, TenantId: tenantID})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	commands := make([]contracts.CommandResponse, 0, len(resp.GetCommands()))
	for _, command := range resp.GetCommands() {
		commands = append(commands, commandFromPB(command))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": commands, "nextCursor": resp.GetNextCursor()})
}

func pageFromRequest(r *http.Request) (int, string, error) {
	pageSize := 0
	if raw := r.URL.Query().Get("pageSize"); raw != "" {
		if _, err := fmt.Sscan(raw, &pageSize); err != nil {
			return 0, "", fmt.Errorf("pageSize must be an integer")
		}
	}
	page, err := platform.NormalizePageRequest(pageSize, r.URL.Query().Get("cursor"))
	return page.Size, page.Cursor, err
}

func (s *Server) getCommandHandler(w http.ResponseWriter, r *http.Request) {
	id, ok := splitCommandPath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	// tenantId is required: iot-core answers InvalidArgument when it is missing
	// and NotFound when the command belongs to another tenant, so the gateway
	// does not duplicate either check.
	resp, err := s.rpc.GetCommand(r.Context(), &corev1.GetCommandRequest{
		Id:       id,
		TenantId: r.URL.Query().Get("tenantId"),
	})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, commandFromPB(resp.GetCommand()))
}

func (s *Server) ackCommandHandler(w http.ResponseWriter, r *http.Request) {
	id, ok := splitCommandPath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	var req struct {
		TenantID string `json:"tenantId"`
		DeviceID string `json:"deviceId"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.rpc.AckCommand(r.Context(), &corev1.AckCommandRequest{
		Id:       id,
		TenantId: req.TenantID,
		DeviceId: req.DeviceID,
	})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, commandFromPB(resp.GetCommand()))
}

func managementAPIMetricsHost() string {
	return runtimeconfig.EnvOrDefault("MANAGEMENT_API_METRICS_HOST", "0.0.0.0")
}

func managementAPIMetricsPort() int {
	return runtimeconfig.Int("MANAGEMENT_API_METRICS_PORT", 9100)
}

func serveManagementAPIMetrics(handler http.Handler, host string, port int, path string) {
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("starting management-api metrics server at %s%s", addr, path)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("management-api metrics server stopped: %v", err)
	}
}

func splitDevicePath(path string) (string, string, bool) {
	path = strings.TrimPrefix(path, "/api/v1/devices/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func splitCommandPath(path string) (string, bool) {
	path = strings.TrimPrefix(path, "/api/v1/commands/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return "", false
	}
	return parts[0], true
}

func deviceFromPB(device *corev1.Device) platform.Device {
	if device == nil {
		return platform.Device{}
	}
	return platform.Device{
		TenantID:  device.GetTenantId(),
		DeviceID:  device.GetDeviceId(),
		ProductID: device.GetProductId(),
		CreatedAt: timestampToTime(device.GetCreatedAt()),
	}
}

func deviceStatusFromPB(status *corev1.DeviceStatus) platform.DeviceStatus {
	if status == nil {
		return platform.DeviceStatus{}
	}
	return platform.DeviceStatus{
		TenantID:   status.GetTenantId(),
		DeviceID:   status.GetDeviceId(),
		Online:     status.GetOnline(),
		LastSeenAt: timestampToTime(status.GetLastSeenAt()),
	}
}

func telemetryFromPB(record *corev1.TelemetryRecord) platform.TelemetryRecord {
	if record == nil {
		return platform.TelemetryRecord{}
	}
	return platform.TelemetryRecord{
		MsgID:      record.GetMsgId(),
		TenantID:   record.GetTenantId(),
		DeviceID:   record.GetDeviceId(),
		Ts:         record.GetTs(),
		Type:       record.GetType(),
		Version:    record.GetVersion(),
		Payload:    json.RawMessage(record.GetPayload()),
		ReceivedAt: timestampToTime(record.GetReceivedAt()),
	}
}

// commandFromPB maps the gRPC command to the REST representation. It must return
// contracts.CommandResponse rather than platform.Command: the latter also
// carries dispatcher bookkeeping (dispatchAttempts, deadlineAt) which is not
// part of the management API contract and only ever reached clients as
// always-zero values.
func commandFromPB(command *corev1.Command) contracts.CommandResponse {
	if command == nil {
		return contracts.CommandResponse{}
	}
	return contracts.CommandResponse{
		ID:        command.GetId(),
		TenantID:  command.GetTenantId(),
		DeviceID:  command.GetDeviceId(),
		Status:    contracts.CommandStatus(command.GetStatus()),
		Payload:   json.RawMessage(command.GetPayload()),
		CreatedAt: timestampToTime(command.GetCreatedAt()),
		UpdatedAt: timestampToTime(command.GetUpdatedAt()),
	}
}

func timestampToTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// writeRPCError maps an iot-core gRPC error onto the REST surface. The mapping
// is by status code, never by message text: matching on substrings previously
// turned "tenantId contains invalid MQTT topic characters" into 502 Bad Gateway
// (no pattern matched) and "command does not belong to device" into 502 as well.
func writeRPCError(w http.ResponseWriter, err error) {
	writeError(w, httpStatusFromGRPC(err), status.Convert(err).Message())
}

func httpStatusFromGRPC(err error) int {
	switch status.Code(err) {
	case codes.OK:
		// status.Code(nil) is codes.OK, so a nil error maps to success rather
		// than falling through to 500.
		return http.StatusOK
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.Unavailable:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func listenHost() string {
	return runtimeconfig.ListenHost("LISTEN_ADDR", "0.0.0.0")
}

func listenPort() int {
	return runtimeconfig.ListenPort("LISTEN_ADDR", "PORT", 8080)
}
