package platform

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"iot/internal/contracts"
)

// NOTE: this file implements the business REST endpoints of platform.App
// (/api/v1/*). Production does NOT serve them, and they carry no auth
// middleware, so App only registers them when Config.EnableBusinessAPI is set
// explicitly — never inferred from the service name.
//
// management-api runs its own go-zero REST gateway behind a bearer token in
// internal/adminapi and talks to iot-core over gRPC (see
// docs/adr/0001-iot-core-management-api-split.md); bootstrap instantiates
// platform.App only for telemetry-ingestor and device-worker, and leaves
// EnableBusinessAPI false. That makes these handlers reachable only from the
// platform unit tests and the live E2E tests in e2e_test.go /
// e2e_load_test.go. Do not add new production behaviour here — new REST
// surface belongs in internal/adminapi.

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

// readyHandler is the deep readiness probe: unlike /healthz (process liveness
// only) it checks the configured runtime dependencies and backing store.
// Production MQTT services supply a connection/subscription check; the
// in-memory test harness can omit it.
func (a *App) readyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if a.readinessCheck != nil {
		if err := a.readinessCheck(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "runtime is not ready")
			return
		}
	}
	if pinger, ok := a.store.(interface {
		PingContext(context.Context) error
	}); ok {
		if err := pinger.PingContext(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "store unreachable: "+err.Error())
			return
		}
	}
	// A dependency may disconnect while the store ping was in flight.
	if a.readinessCheck != nil && a.readinessCheck(ctx) != nil {
		writeError(w, http.StatusServiceUnavailable, "runtime is not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ready",
		"serviceName": a.serviceName,
	})
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
		if !contracts.IsValidTenantID(req.ID) {
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
	if r.Method == http.MethodPost {
		var done func()
		w, done = a.observeBusinessOutcome(w, "device", "created")
		defer done()
	}
	p, _ := ManagementRequest(r)
	if r.Method == http.MethodGet {
		var devices []Device
		for _, d := range a.store.ListDevices() {
			if d.TenantID == p.TenantID {
				d.Secret = ""
				devices = append(devices, d)
			}
		}
		writeJSON(w, 200, devices)
		return
	}
	var req struct {
		DeviceID  string `json:"deviceId"`
		ProductID string `json:"productId"`
		Secret    string `json:"secret"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	d, err := a.store.CreateDevice(Device{TenantID: p.TenantID, DeviceID: req.DeviceID, ProductID: req.ProductID, Secret: req.Secret})
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	d.Secret = ""
	writeJSON(w, 201, d)
}

func (a *App) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var done func()
		w, done = a.observeBusinessOutcome(w, "telemetry", "")
		defer done()
	}
	p, _ := ManagementRequest(r)
	var req struct {
		MsgID   string          `json:"msgId"`
		Ts      int64           `json:"ts"`
		Type    string          `json:"type"`
		Version string          `json:"version"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	env := contracts.Envelope{MsgID: req.MsgID, TenantID: p.TenantID, DeviceID: p.DeviceID, Ts: req.Ts, Type: req.Type, Version: req.Version, Payload: req.Payload}
	if err := contracts.ValidateEnvelope(env); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	rec, err := a.store.RecordTelemetry(env)
	if err != nil && !IsTelemetryDuplicate(err) {
		writeError(w, 400, err.Error())
		return
	}
	if err = a.publisher.PublishTelemetry(r.Context(), rec); err != nil {
		writeError(w, 502, err.Error())
		return
	}
	writeJSON(w, 202, rec)
}

func (a *App) handleCommands(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var done func()
		w, done = a.observeBusinessOutcome(w, "command", "created")
		defer done()
	}
	p, _ := ManagementRequest(r)
	if r.Method == http.MethodGet {
		var cmds []Command
		for _, c := range a.store.ListCommands() {
			if c.TenantID == p.TenantID {
				cmds = append(cmds, c)
			}
		}
		writeJSON(w, 200, commandResponses(cmds))
		return
	}
	var req struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err := ValidateCommandInput(p.TenantID, p.DeviceID, req.Payload); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if err := contracts.ValidateIdempotencyKey(key); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	var cmd Command
	var err error
	if store, ok := a.store.(CommandCreator); ok {
		cmd, err = store.CreateCommandWithKey(r.Context(), p.TenantID, p.DeviceID, key, req.Payload)
	} else {
		cmd, err = a.store.CreateCommand(p.TenantID, p.DeviceID, req.Payload)
	}
	if err != nil {
		code := 400
		if errors.Is(err, ErrIdempotencyConflict) {
			code = 409
		}
		writeError(w, code, err.Error())
		return
	}
	if err = a.publisher.PublishCommand(r.Context(), cmd); err != nil {
		writeError(w, 502, err.Error())
		return
	}
	writeJSON(w, 201, commandResponse(cmd))
}

func (a *App) handleCommandByID(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var done func()
		w, done = a.observeBusinessOutcome(w, "command", "acked")
		defer done()
	}
	p, _ := ManagementRequest(r)
	if r.Method == http.MethodGet {
		cmd, ok := a.store.GetCommand(p.CommandID)
		if !ok || cmd.TenantID != p.TenantID {
			writeError(w, 404, "command not found")
			return
		}
		writeJSON(w, 200, commandResponse(cmd))
		return
	}
	var result contracts.CommandResult
	if err := decodeJSON(r, &result); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	store, ok := a.store.(CommandResultStore)
	if !ok {
		writeError(w, 400, "result storage unavailable")
		return
	}
	cmd, err := store.AckCommandResultContext(r.Context(), p.CommandID, p.TenantID, p.DeviceID, result)
	if err != nil {
		code := 400
		if errors.Is(err, ErrAcknowledgementConflict) {
			code = 409
		}
		writeError(w, code, err.Error())
		return
	}
	writeJSON(w, 200, commandResponse(cmd))
}

func commandResponse(cmd Command) contracts.CommandResponse {
	return contracts.CommandResponse{ID: cmd.ID, TenantID: cmd.TenantID, DeviceID: cmd.DeviceID, Status: cmd.Status, Payload: cmd.Payload, CreatedAt: cmd.CreatedAt, UpdatedAt: cmd.UpdatedAt, Result: cmd.Result}
}
func commandResponses(cmds []Command) []contracts.CommandResponse {
	out := make([]contracts.CommandResponse, 0, len(cmds))
	for _, cmd := range cmds {
		out = append(out, commandResponse(cmd))
	}
	return out
}

func (a *App) handleDeviceByID(w http.ResponseWriter, r *http.Request) {
	p, _ := ManagementRequest(r)
	switch p.Resource {
	case "device":
		d, ok := a.store.GetDevice(p.TenantID, p.DeviceID)
		if !ok {
			writeError(w, 404, "device not found")
			return
		}
		d.Secret = ""
		writeJSON(w, 200, d)
	case "status":
		state, ok := a.store.GetDeviceStatus(p.TenantID, p.DeviceID)
		if !ok {
			writeError(w, 404, "device not found")
			return
		}
		writeJSON(w, 200, state)
	case "telemetry":
		writeJSON(w, 200, a.store.ListTelemetry(p.TenantID, p.DeviceID))
	default:
		writeError(w, 404, "not found")
	}
}

func (a *App) observeBusinessOutcome(w http.ResponseWriter, kind, operation string) (http.ResponseWriter, func()) {
	rw := &observedResponseWriter{ResponseWriter: w, status: http.StatusOK}
	return rw, func() {
		if a.metrics == nil {
			return
		}
		result := "ok"
		if rw.status >= 400 {
			result = "error"
		}
		switch kind {
		case "device":
			a.metrics.IncDevice(result)
		case "telemetry":
			a.metrics.IncTelemetry(result)
		case "command":
			a.metrics.IncCommand(operation, result)
		}
	}
}
