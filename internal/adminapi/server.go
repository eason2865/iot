package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"iot/internal/contracts"
	"iot/internal/platform"
	"iot/internal/runtimeconfig"
	corev1 "iot/proto/core/v1"
)

type Server struct {
	rpc     corev1.CoreServiceClient
	metrics *platform.Metrics
}

func Run() error {
	client, err := newRPCClient()
	if err != nil {
		return err
	}
	defer client.Conn().Close()

	platform.ConfigureStdLogger("admin-api")
	metrics := platform.NewMetrics()
	httpServer := rest.MustNewServer(rest.RestConf{
		ServiceConf: service.ServiceConf{
			Name:      "admin-api",
			Telemetry: platform.TraceConfig("admin-api"),
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
	httpServer.Use(rest.ToMiddleware(platform.RequestIDHTTPMiddleware))
	httpServer.Use(rest.ToMiddleware(metrics.HTTPMiddleware()))
	defer httpServer.Stop()

	go serveAdminMetrics(metrics.Handler(), adminMetricsHost(), adminMetricsPort(), runtimeconfig.EnvOrDefault("ADMIN_METRICS_PATH", "/metrics"))

	api := &Server{
		rpc:     corev1.NewCoreServiceClient(client.Conn()),
		metrics: metrics,
	}
	httpServer.AddRoutes(api.routes())
	httpServer.Start()
	return nil
}

func newRPCClient() (zrpc.Client, error) {
	conf := zrpc.NewEtcdClientConf(
		runtimeconfig.SplitCSV(runtimeconfig.EnvOrDefault("CORE_RPC_ETCD_HOSTS", "localhost:2379")),
		runtimeconfig.EnvOrDefault("CORE_RPC_ETCD_KEY", "iot/core-rpc"),
		"admin-api",
		"",
	)
	conf.Timeout = 5000
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		client, err := zrpc.NewClient(conf,
			zrpc.WithUnaryClientInterceptor(platform.UnaryClientRequestIDInterceptor()))
		if err == nil {
			return client, nil
		}
		lastErr = err
		time.Sleep(1 * time.Second)
	}
	return nil, lastErr
}

func (s *Server) routes() []rest.Route {
	return []rest.Route{
		{Method: http.MethodGet, Path: "/healthz", Handler: s.healthHandler},
		{Method: http.MethodGet, Path: "/openapi.json", Handler: s.openapiHandler},
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
		"serviceName": "admin-api",
	})
}

func (s *Server) openapiHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, contracts.OpenAPISpec())
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
	resp, err := s.rpc.ListTenants(r.Context(), &corev1.ListTenantsRequest{})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	tenants := make([]platform.Tenant, 0, len(resp.GetTenants()))
	for _, tenant := range resp.GetTenants() {
		tenants = append(tenants, platform.Tenant{ID: tenant.GetId(), Name: tenant.GetName()})
	}
	writeJSON(w, http.StatusOK, tenants)
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
	resp, err := s.rpc.ListDevices(r.Context(), &corev1.ListDevicesRequest{})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	devices := make([]platform.Device, 0, len(resp.GetDevices()))
	for _, device := range resp.GetDevices() {
		devices = append(devices, deviceFromPB(device))
	}
	writeJSON(w, http.StatusOK, devices)
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
	resp, err := s.rpc.ListTelemetry(r.Context(), &corev1.ListTelemetryRequest{TenantId: tenantID, DeviceId: deviceID})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	records := make([]platform.TelemetryRecord, 0, len(resp.GetRecords()))
	for _, record := range resp.GetRecords() {
		records = append(records, telemetryFromPB(record))
	}
	writeJSON(w, http.StatusOK, records)
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
	resp, err := s.rpc.ListCommands(r.Context(), &corev1.ListCommandsRequest{})
	if err != nil {
		writeRPCError(w, err)
		return
	}
	commands := make([]platform.Command, 0, len(resp.GetCommands()))
	for _, command := range resp.GetCommands() {
		commands = append(commands, commandFromPB(command))
	}
	writeJSON(w, http.StatusOK, commands)
}

func (s *Server) getCommandHandler(w http.ResponseWriter, r *http.Request) {
	id, ok := splitCommandPath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	resp, err := s.rpc.GetCommand(r.Context(), &corev1.GetCommandRequest{Id: id})
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

func adminMetricsHost() string {
	return runtimeconfig.EnvOrDefault("ADMIN_METRICS_HOST", "0.0.0.0")
}

func adminMetricsPort() int {
	return runtimeconfig.Int("ADMIN_METRICS_PORT", 9100)
}

func serveAdminMetrics(handler http.Handler, host string, port int, path string) {
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("starting admin metrics server at %s%s", addr, path)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("admin metrics server stopped: %v", err)
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
		Secret:    device.GetSecret(),
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

func commandFromPB(command *corev1.Command) platform.Command {
	if command == nil {
		return platform.Command{}
	}
	return platform.Command{
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

func writeRPCError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	case strings.Contains(err.Error(), "not found"):
		status = http.StatusNotFound
	case strings.Contains(err.Error(), "already exists"):
		status = http.StatusConflict
	case strings.Contains(err.Error(), "required"):
		status = http.StatusBadRequest
	}
	writeError(w, status, err.Error())
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
