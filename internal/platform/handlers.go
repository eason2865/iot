package platform

import (
	"encoding/json"
	"net/http"
	"strings"

	"iot/internal/contracts"
)

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
