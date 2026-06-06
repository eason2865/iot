package platform

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"mqtt/internal/contracts"
)

type Config struct {
	ServiceName      string
	DeviceHeartbeatTTL time.Duration
}

type App struct {
	serviceName string
	store       *memoryStore
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
		store:       newMemoryStore(ttl),
		ttl:         ttl,
	}
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
	MsgID     string          `json:"msgId"`
	TenantID  string          `json:"tenantId"`
	DeviceID  string          `json:"deviceId"`
	Ts        int64           `json:"ts"`
	Type      string          `json:"type"`
	Version   string          `json:"version"`
	Payload   json.RawMessage `json:"payload"`
	ReceivedAt time.Time      `json:"receivedAt"`
}

type Command struct {
	ID        string          `json:"id"`
	TenantID  string          `json:"tenantId"`
	DeviceID  string          `json:"deviceId"`
	Status    contracts.CommandStatus `json:"status"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
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
	mux.HandleFunc("/openapi.json", a.openapiHandler)
	mux.HandleFunc("/schemas/mqtt-envelope.json", a.mqttEnvelopeSchemaHandler)
	mux.HandleFunc("/api/v1/tenants", a.handleTenants)
	mux.HandleFunc("/api/v1/devices", a.handleDevices)
	mux.HandleFunc("/api/v1/telemetry", a.handleTelemetry)
	mux.HandleFunc("/api/v1/commands", a.handleCommands)
	mux.HandleFunc("/api/v1/commands/", a.handleCommandByID)
	mux.HandleFunc("/api/v1/devices/", a.handleDeviceByID)
	return mux
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
	writeJSON(w, http.StatusOK, openAPISpec())
}

func (a *App) mqttEnvelopeSchemaHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, mqttEnvelopeSchema())
}

func openAPISpec() map[string]any {
	return map[string]any{
		"openapi": "3.1.0",
		"info": map[string]any{
			"title":   "MQTT IoT Platform API",
			"version": "v1",
		},
		"paths": map[string]any{
			"/healthz": map[string]any{
				"get": map[string]any{"responses": map[string]any{"200": map[string]any{"description": "ok"}}},
			},
			"/api/v1/tenants": map[string]any{
				"get":  map[string]any{"responses": map[string]any{"200": map[string]any{"description": "list tenants"}}},
				"post": map[string]any{"responses": map[string]any{"201": map[string]any{"description": "create tenant"}}},
			},
			"/api/v1/devices": map[string]any{
				"get":  map[string]any{"responses": map[string]any{"200": map[string]any{"description": "list devices"}}},
				"post": map[string]any{"responses": map[string]any{"201": map[string]any{"description": "create device"}}},
			},
			"/api/v1/devices/{tenantId}/{deviceId}": map[string]any{
				"get": map[string]any{"responses": map[string]any{"200": map[string]any{"description": "device detail"}}},
			},
			"/api/v1/devices/{tenantId}/{deviceId}/status": map[string]any{
				"get": map[string]any{"responses": map[string]any{"200": map[string]any{"description": "device status"}}},
			},
			"/api/v1/devices/{tenantId}/{deviceId}/telemetry": map[string]any{
				"get": map[string]any{"responses": map[string]any{"200": map[string]any{"description": "device telemetry"}}},
			},
			"/api/v1/telemetry": map[string]any{
				"post": map[string]any{"responses": map[string]any{"202": map[string]any{"description": "ingest telemetry"}}},
			},
			"/api/v1/commands": map[string]any{
				"get":  map[string]any{"responses": map[string]any{"200": map[string]any{"description": "list commands"}}},
				"post": map[string]any{"responses": map[string]any{"201": map[string]any{"description": "create command"}}},
			},
			"/api/v1/commands/{id}": map[string]any{
				"get": map[string]any{"responses": map[string]any{"200": map[string]any{"description": "command detail"}}},
			},
			"/api/v1/commands/{id}/ack": map[string]any{
				"post": map[string]any{"responses": map[string]any{"200": map[string]any{"description": "ack command"}}},
			},
		},
		"components": map[string]any{
			"schemas": map[string]any{
				"ApiError": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"error": map[string]any{"type": "string"},
					},
				},
				"TelemetryEnvelope": map[string]any{
					"type": "object",
					"required": []string{"msgId", "tenantId", "deviceId", "ts", "type", "version", "payload"},
				},
			},
		},
	}
}

func mqttEnvelopeSchema() map[string]any {
	return map[string]any{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"title":   "MQTT Telemetry Envelope",
		"type":    "object",
		"required": []string{"msgId", "tenantId", "deviceId", "ts", "type", "version", "payload"},
		"properties": map[string]any{
			"msgId":    map[string]any{"type": "string"},
			"tenantId": map[string]any{"type": "string"},
			"deviceId": map[string]any{"type": "string"},
			"ts":       map[string]any{"type": "integer"},
			"type":     map[string]any{"type": "string"},
			"version":  map[string]any{"type": "string"},
			"traceId":  map[string]any{"type": "string"},
			"productId": map[string]any{"type": "string"},
			"region":   map[string]any{"type": "string"},
			"seq":      map[string]any{"type": "integer"},
			"payload":  map[string]any{"type": "object"},
		},
	}
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
			writeError(w, http.StatusBadRequest, "id and name are required")
			return
		}
		tenant, err := a.store.createTenant(req)
		if err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, tenant)
	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.store.listTenants())
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
			writeError(w, http.StatusBadRequest, "tenantId, deviceId, productId and secret are required")
			return
		}
		device, err := a.store.createDevice(Device{
			TenantID:  req.TenantID,
			DeviceID:  req.DeviceID,
			ProductID: req.ProductID,
			Secret:    req.Secret,
		})
		if err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, device)
	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.store.listDevices())
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
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	record, err := a.store.recordTelemetry(env)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
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
			writeError(w, http.StatusBadRequest, "tenantId and deviceId are required")
			return
		}
		cmd, err := a.store.createCommand(req.TenantID, req.DeviceID, req.Payload)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, cmd)
	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.store.listCommands())
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
		cmd, ok := a.store.getCommand(id)
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
		cmd, err := a.store.ackCommand(id, req.TenantID, req.DeviceID)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
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
		device, ok := a.store.getDevice(tenantID, deviceID)
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
		status, ok := a.store.getDeviceStatus(tenantID, deviceID)
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
		records := a.store.listTelemetry(tenantID, deviceID)
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

type memoryStore struct {
	mu sync.RWMutex

	ttl time.Duration

	tenants  map[string]Tenant
	devices  map[string]Device
	statuses map[string]DeviceStatus
	telemetry map[string][]TelemetryRecord
	commands map[string]Command
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

func (s *memoryStore) createTenant(t Tenant) (Tenant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[t.ID]; exists {
		return Tenant{}, fmt.Errorf("tenant already exists")
	}
	s.tenants[t.ID] = t
	return t, nil
}

func (s *memoryStore) listTenants() []Tenant {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Tenant, 0, len(s.tenants))
	for _, tenant := range s.tenants {
		out = append(out, tenant)
	}
	return out
}

func (s *memoryStore) createDevice(d Device) (Device, error) {
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

func (s *memoryStore) listDevices() []Device {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Device, 0, len(s.devices))
	for _, device := range s.devices {
		out = append(out, device)
	}
	return out
}

func (s *memoryStore) getDevice(tenantID, deviceID string) (Device, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	device, ok := s.devices[deviceKey(tenantID, deviceID)]
	return device, ok
}

func (s *memoryStore) recordTelemetry(env contracts.Envelope) (TelemetryRecord, error) {
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

func (s *memoryStore) createCommand(tenantID, deviceID string, payload json.RawMessage) (Command, error) {
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

func (s *memoryStore) ackCommand(id, tenantID, deviceID string) (Command, error) {
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

func (s *memoryStore) listCommands() []Command {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Command, 0, len(s.commands))
	for _, command := range s.commands {
		out = append(out, command)
	}
	return out
}

func (s *memoryStore) getCommand(id string) (Command, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cmd, ok := s.commands[id]
	return cmd, ok
}

func (s *memoryStore) getDeviceStatus(tenantID, deviceID string) (DeviceStatus, bool) {
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

func (s *memoryStore) listTelemetry(tenantID, deviceID string) []TelemetryRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := deviceKey(tenantID, deviceID)
	src := s.telemetry[key]
	out := make([]TelemetryRecord, len(src))
	copy(out, src)
	return out
}
