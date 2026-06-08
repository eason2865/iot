package contracts

func OpenAPISpec() map[string]any {
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
					"type":     "object",
					"required": []string{"msgId", "tenantId", "deviceId", "ts", "type", "version", "payload"},
				},
			},
		},
	}
}

func MQTTEnvelopeSchema() map[string]any {
	return map[string]any{
		"$schema":  "https://json-schema.org/draft/2020-12/schema",
		"title":    "MQTT Telemetry Envelope",
		"type":     "object",
		"required": []string{"msgId", "tenantId", "deviceId", "ts", "type", "version", "payload"},
		"properties": map[string]any{
			"msgId":     map[string]any{"type": "string"},
			"tenantId":  map[string]any{"type": "string"},
			"deviceId":  map[string]any{"type": "string"},
			"ts":        map[string]any{"type": "integer"},
			"type":      map[string]any{"type": "string"},
			"version":   map[string]any{"type": "string"},
			"traceId":   map[string]any{"type": "string"},
			"productId": map[string]any{"type": "string"},
			"region":    map[string]any{"type": "string"},
			"seq":       map[string]any{"type": "integer"},
			"payload":   map[string]any{"type": "object"},
		},
	}
}
