package platform

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"iot/internal/contracts"
)

type Config struct {
	ServiceName        string
	DeviceHeartbeatTTL time.Duration
	Store              Repository
	Publisher          MessagePublisher
	Metrics            *Metrics
}

type App struct {
	serviceName string
	store       Repository
	publisher   MessagePublisher
	metrics     *Metrics
	ttl         time.Duration
	router      http.Handler
}

func New(cfg Config) *App {
	ttl := cfg.DeviceHeartbeatTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	app := &App{
		serviceName: cfg.ServiceName,
		store:       cfg.Store,
		publisher:   cfg.Publisher,
		ttl:         ttl,
	}
	if app.store == nil {
		app.store = newMemoryStore(ttl)
	}
	if app.publisher == nil {
		app.publisher = noopPublisher{}
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NewMetrics()
	}
	app.metrics = cfg.Metrics
	app.router = app.routes()
	return app
}

func (a *App) Router() http.Handler {
	return a.router
}

type Tenant struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Device struct {
	TenantID  string    `json:"tenantId"`
	DeviceID  string    `json:"deviceId"`
	ProductID string    `json:"productId"`
	Secret    string    `json:"secret,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

type DeviceStatus struct {
	TenantID   string    `json:"tenantId"`
	DeviceID   string    `json:"deviceId"`
	Online     bool      `json:"online"`
	LastSeenAt time.Time `json:"lastSeenAt"`
}

type TelemetryRecord struct {
	MsgID      string          `json:"msgId"`
	TenantID   string          `json:"tenantId"`
	DeviceID   string          `json:"deviceId"`
	Ts         int64           `json:"ts"`
	Type       string          `json:"type"`
	Version    string          `json:"version"`
	Payload    json.RawMessage `json:"payload"`
	ReceivedAt time.Time       `json:"receivedAt"`
}

type Command struct {
	ID        string                  `json:"id"`
	TenantID  string                  `json:"tenantId"`
	DeviceID  string                  `json:"deviceId"`
	Status    contracts.CommandStatus `json:"status"`
	Payload   json.RawMessage         `json:"payload"`
	CreatedAt time.Time               `json:"createdAt"`
	UpdatedAt time.Time               `json:"updatedAt"`
}

const (
	CommandStatusCreated = contracts.CommandStatusCreated
	CommandStatusSent    = contracts.CommandStatusSent
	CommandStatusAcked   = contracts.CommandStatusAcked
	CommandStatusFailed  = contracts.CommandStatusFailed
	CommandStatusTimeout = contracts.CommandStatusTimeout
)

type apiError struct {
	Error string `json:"error"`
}

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.healthHandler)
	mux.Handle("/metrics", a.metrics.Handler())
	mux.HandleFunc("/openapi.json", a.openapiHandler)
	mux.HandleFunc("/schemas/mqtt-envelope.json", a.mqttEnvelopeSchemaHandler)
	mux.HandleFunc("/api/v1/tenants", a.handleTenants)
	mux.HandleFunc("/api/v1/devices", a.handleDevices)
	mux.HandleFunc("/api/v1/telemetry", a.handleTelemetry)
	mux.HandleFunc("/api/v1/commands", a.handleCommands)
	mux.HandleFunc("/api/v1/commands/", a.handleCommandByID)
	mux.HandleFunc("/api/v1/devices/", a.handleDeviceByID)
	return a.observeHTTP(mux)
}

func (a *App) observeHTTP(next http.Handler) http.Handler {
	return RequestIDHTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rw := &observedResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		if a.metrics != nil {
			a.metrics.ObserveHTTPRequest(routeLabel(r.URL.Path), r.Method, rw.status, time.Since(start))
		}
	}))
}

func (a *App) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ok",
		"serviceName": a.serviceName,
	})
}

func (a *App) openapiHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, contracts.OpenAPISpec())
}

func (a *App) mqttEnvelopeSchemaHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, contracts.MQTTEnvelopeSchema())
}

func (a *App) handleTenants(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req Tenant
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.ID == "" || req.Name == "" {
			if a.metrics != nil {
				a.metrics.IncTenant("error")
			}
			writeError(w, http.StatusBadRequest, "id and name are required")
			return
		}
		if !contracts.IsValidTopicPart(req.ID) {
			if a.metrics != nil {
				a.metrics.IncTenant("error")
			}
			writeError(w, http.StatusBadRequest, "tenantId contains invalid MQTT topic characters")
			return
		}
		tenant, err := a.store.CreateTenant(req)
		if err != nil {
			if a.metrics != nil {
				a.metrics.IncTenant("error")
			}
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		if a.metrics != nil {
			a.metrics.IncTenant("ok")
		}
		writeJSON(w, http.StatusCreated, tenant)
	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.store.ListTenants())
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *App) handleDevices(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
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
		if req.TenantID == "" || req.DeviceID == "" || req.ProductID == "" || req.Secret == "" {
			if a.metrics != nil {
				a.metrics.IncDevice("error")
			}
			writeError(w, http.StatusBadRequest, "tenantId, deviceId, productId and secret are required")
			return
		}
		if !contracts.IsValidTopicPart(req.TenantID) || !contracts.IsValidTopicPart(req.DeviceID) {
			if a.metrics != nil {
				a.metrics.IncDevice("error")
			}
			writeError(w, http.StatusBadRequest, "tenantId or deviceId contains invalid MQTT topic characters")
			return
		}
		device, err := a.store.CreateDevice(Device{
			TenantID:  req.TenantID,
			DeviceID:  req.DeviceID,
			ProductID: req.ProductID,
			Secret:    req.Secret,
		})
		if err != nil {
			if a.metrics != nil {
				a.metrics.IncDevice("error")
			}
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		if a.metrics != nil {
			a.metrics.IncDevice("ok")
		}
		writeJSON(w, http.StatusCreated, device)
	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.store.ListDevices())
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *App) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	env, err := contracts.ParseEnvelope(mustReadBody(r))
	if err != nil {
		if a.metrics != nil {
			a.metrics.IncTelemetry("error")
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	record, err := a.store.RecordTelemetry(env)
	if err != nil {
		if a.metrics != nil {
			a.metrics.IncTelemetry("error")
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := a.publisher.PublishTelemetry(record); err != nil {
		if a.metrics != nil {
			a.metrics.IncTelemetry("error")
		}
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if a.metrics != nil {
		a.metrics.IncTelemetry("ok")
	}
	writeJSON(w, http.StatusAccepted, record)
}

func (a *App) handleCommands(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			TenantID string          `json:"tenantId"`
			DeviceID string          `json:"deviceId"`
			Payload  json.RawMessage `json:"payload"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.TenantID == "" || req.DeviceID == "" {
			if a.metrics != nil {
				a.metrics.IncCommand("created", "error")
			}
			writeError(w, http.StatusBadRequest, "tenantId and deviceId are required")
			return
		}
		if !contracts.IsValidTopicPart(req.TenantID) || !contracts.IsValidTopicPart(req.DeviceID) {
			if a.metrics != nil {
				a.metrics.IncCommand("created", "error")
			}
			writeError(w, http.StatusBadRequest, "tenantId or deviceId contains invalid MQTT topic characters")
			return
		}
		cmd, err := a.store.CreateCommand(req.TenantID, req.DeviceID, req.Payload)
		if err != nil {
			if a.metrics != nil {
				a.metrics.IncCommand("created", "error")
			}
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := a.publisher.PublishCommand(cmd); err != nil {
			if a.metrics != nil {
				a.metrics.IncCommand("created", "error")
			}
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		if a.metrics != nil {
			a.metrics.IncCommand("created", "ok")
		}
		writeJSON(w, http.StatusCreated, cmd)
	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.store.ListCommands())
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *App) handleCommandByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/commands/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		cmd, ok := a.store.GetCommand(id)
		if !ok {
			writeError(w, http.StatusNotFound, "command not found")
			return
		}
		writeJSON(w, http.StatusOK, cmd)
		return
	}
	if len(parts) == 2 && parts[1] == "ack" && r.Method == http.MethodPost {
		var req struct {
			TenantID string `json:"tenantId"`
			DeviceID string `json:"deviceId"`
		}
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		cmd, err := a.store.AckCommand(id, req.TenantID, req.DeviceID)
		if err != nil {
			if a.metrics != nil {
				a.metrics.IncCommand("acked", "error")
			}
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if a.metrics != nil {
			a.metrics.IncCommand("acked", "ok")
		}
		writeJSON(w, http.StatusOK, cmd)
		return
	}
	writeError(w, http.StatusNotFound, "not found")
}

func (a *App) handleDeviceByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/devices/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	tenantID, deviceID := parts[0], parts[1]
	if len(parts) == 2 && r.Method == http.MethodGet {
		device, ok := a.store.GetDevice(tenantID, deviceID)
		if !ok {
			writeError(w, http.StatusNotFound, "device not found")
			return
		}
		writeJSON(w, http.StatusOK, device)
		return
	}
	if len(parts) < 3 {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	action := parts[2]
	switch action {
	case "status":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		status, ok := a.store.GetDeviceStatus(tenantID, deviceID)
		if !ok {
			writeError(w, http.StatusNotFound, "device not found")
			return
		}
		writeJSON(w, http.StatusOK, status)
	case "telemetry":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		records := a.store.ListTelemetry(tenantID, deviceID)
		writeJSON(w, http.StatusOK, records)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func mustReadBody(r *http.Request) []byte {
	defer r.Body.Close()
	data, _ := io.ReadAll(r.Body)
	return data
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, apiError{Error: msg})
}

type observedResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *observedResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

type memoryStore struct {
	mu sync.RWMutex

	ttl time.Duration

	tenants    map[string]Tenant
	devices    map[string]Device
	statuses   map[string]DeviceStatus
	telemetry  map[string][]TelemetryRecord
	commands   map[string]Command
	commandSeq int64
}

func newMemoryStore(ttl time.Duration) *memoryStore {
	return &memoryStore{
		ttl:       ttl,
		tenants:   map[string]Tenant{},
		devices:   map[string]Device{},
		statuses:  map[string]DeviceStatus{},
		telemetry: map[string][]TelemetryRecord{},
		commands:  map[string]Command{},
	}
}

func deviceKey(tenantID, deviceID string) string {
	return tenantID + ":" + deviceID
}

func (s *memoryStore) CreateTenant(t Tenant) (Tenant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[t.ID]; exists {
		return Tenant{}, fmt.Errorf("tenant already exists")
	}
	s.tenants[t.ID] = t
	return t, nil
}

func (s *memoryStore) ListTenants() []Tenant {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Tenant, 0, len(s.tenants))
	for _, tenant := range s.tenants {
		out = append(out, tenant)
	}
	return out
}

func (s *memoryStore) CreateDevice(d Device) (Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[d.TenantID]; !exists {
		return Device{}, fmt.Errorf("tenant not found")
	}
	key := deviceKey(d.TenantID, d.DeviceID)
	if _, exists := s.devices[key]; exists {
		return Device{}, fmt.Errorf("device already exists")
	}
	d.CreatedAt = time.Now().UTC()
	s.devices[key] = d
	s.statuses[key] = DeviceStatus{TenantID: d.TenantID, DeviceID: d.DeviceID, Online: false}
	return d, nil
}

func (s *memoryStore) ListDevices() []Device {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Device, 0, len(s.devices))
	for _, device := range s.devices {
		out = append(out, device)
	}
	return out
}

func (s *memoryStore) GetDevice(tenantID, deviceID string) (Device, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	device, ok := s.devices[deviceKey(tenantID, deviceID)]
	return device, ok
}

func (s *memoryStore) RecordTelemetry(env contracts.Envelope) (TelemetryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := deviceKey(env.TenantID, env.DeviceID)
	if _, exists := s.devices[key]; !exists {
		return TelemetryRecord{}, fmt.Errorf("device not found")
	}
	rec := TelemetryRecord{
		MsgID:      env.MsgID,
		TenantID:   env.TenantID,
		DeviceID:   env.DeviceID,
		Ts:         env.Ts,
		Type:       env.Type,
		Version:    env.Version,
		Payload:    env.Payload,
		ReceivedAt: time.Now().UTC(),
	}
	s.telemetry[key] = append(s.telemetry[key], rec)
	s.statuses[key] = DeviceStatus{
		TenantID:   env.TenantID,
		DeviceID:   env.DeviceID,
		Online:     true,
		LastSeenAt: time.Now().UTC(),
	}
	return rec, nil
}

func (s *memoryStore) CreateCommand(tenantID, deviceID string, payload json.RawMessage) (Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := deviceKey(tenantID, deviceID)
	if _, exists := s.devices[key]; !exists {
		return Command{}, fmt.Errorf("device not found")
	}
	s.commandSeq++
	id := fmt.Sprintf("cmd-%d", s.commandSeq)
	now := time.Now().UTC()
	cmd := Command{
		ID:        id,
		TenantID:  tenantID,
		DeviceID:  deviceID,
		Status:    contracts.CommandStatusCreated,
		Payload:   payload,
		CreatedAt: now,
		UpdatedAt: now,
	}
	next, err := contracts.AdvanceCommandStatus(cmd.Status, contracts.CommandEventPublished)
	if err != nil {
		return Command{}, err
	}
	cmd.Status = next
	cmd.UpdatedAt = time.Now().UTC()
	s.commands[id] = cmd
	return cmd, nil
}

func (s *memoryStore) AckCommand(id, tenantID, deviceID string) (Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd, exists := s.commands[id]
	if !exists {
		return Command{}, fmt.Errorf("command not found")
	}
	if cmd.TenantID != tenantID || cmd.DeviceID != deviceID {
		return Command{}, fmt.Errorf("command does not belong to device")
	}
	next, err := contracts.AdvanceCommandStatus(cmd.Status, contracts.CommandEventAcked)
	if err != nil {
		return Command{}, err
	}
	cmd.Status = next
	cmd.UpdatedAt = time.Now().UTC()
	s.commands[id] = cmd
	return cmd, nil
}

func (s *memoryStore) ListCommands() []Command {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Command, 0, len(s.commands))
	for _, command := range s.commands {
		out = append(out, command)
	}
	return out
}

func (s *memoryStore) GetCommand(id string) (Command, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cmd, ok := s.commands[id]
	return cmd, ok
}

func (s *memoryStore) GetDeviceStatus(tenantID, deviceID string) (DeviceStatus, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := deviceKey(tenantID, deviceID)
	status, ok := s.statuses[key]
	if !ok {
		return DeviceStatus{}, false
	}
	if !status.LastSeenAt.IsZero() && time.Since(status.LastSeenAt) > s.ttl {
		status.Online = false
	}
	return status, true
}

func (s *memoryStore) ListTelemetry(tenantID, deviceID string) []TelemetryRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := deviceKey(tenantID, deviceID)
	src := s.telemetry[key]
	out := make([]TelemetryRecord, len(src))
	copy(out, src)
	return out
}
