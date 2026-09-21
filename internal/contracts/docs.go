package contracts

// OpenAPISpec returns the OpenAPI 3.1 specification for the management REST API.
// It is served at /openapi.json and must stay in sync with docs/openapi.json.
func OpenAPISpec() map[string]any {
	bearer := []any{map[string]any{"bearerAuth": []any{}}}
	paginationParams := []any{
		map[string]any{"name": "pageSize", "in": "query", "required": false, "schema": map[string]any{"type": "integer", "minimum": 1, "maximum": 100, "default": 50}, "description": "items per page (max 100)"},
		map[string]any{"name": "cursor", "in": "query", "required": false, "schema": map[string]any{"type": "string"}, "description": "opaque keyset cursor from a previous response's nextCursor"},
	}
	pagedResponse := func(itemRef, description string) map[string]any {
		return map[string]any{
			"description": description,
			"content": map[string]any{"application/json": map[string]any{"schema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"items":      map[string]any{"type": "array", "items": map[string]any{"$ref": itemRef}},
					"nextCursor": map[string]any{"type": "string", "description": "pass as cursor for the next page; empty when no more pages"},
				},
			}}},
		}
	}
	errorResponses := func(codes ...string) map[string]any {
		out := map[string]any{}
		desc := map[string]string{"400": "bad request", "401": "missing or invalid bearer token", "404": "not found", "409": "conflict"}
		for _, code := range codes {
			out[code] = map[string]any{
				"description": desc[code],
				"content":     map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/ApiError"}}},
			}
		}
		return out
	}
	merge := func(ok map[string]any, errs map[string]any) map[string]any {
		out := map[string]any{"200": ok}
		for k, v := range errs {
			out[k] = v
		}
		return out
	}

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
				"get": map[string]any{
					"security":   bearer,
					"parameters": paginationParams,
					"responses":  merge(pagedResponse("#/components/schemas/Tenant", "list tenants (keyset-paginated)"), errorResponses("400", "401")),
				},
				"post": map[string]any{
					"security":    bearer,
					"requestBody": map[string]any{"required": true, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/TenantInput"}}}},
					"responses": map[string]any{
						"201": map[string]any{"description": "tenant created", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Tenant"}}}},
						"400": errorResponses("400")["400"], "401": errorResponses("401")["401"], "409": errorResponses("409")["409"],
					},
				},
			},
			"/api/v1/devices": map[string]any{
				"get": map[string]any{
					"security":   bearer,
					"parameters": paginationParams,
					"responses":  merge(pagedResponse("#/components/schemas/Device", "list devices (keyset-paginated)"), errorResponses("400", "401")),
				},
				"post": map[string]any{
					"security":    bearer,
					"requestBody": map[string]any{"required": true, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/DeviceInput"}}}},
					"responses": map[string]any{
						"201": map[string]any{"description": "device created", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Device"}}}},
						"400": errorResponses("400")["400"], "401": errorResponses("401")["401"], "409": errorResponses("409")["409"],
					},
				},
			},
			"/api/v1/devices/{tenantId}/{deviceId}": map[string]any{
				"get": map[string]any{
					"security":   bearer,
					"parameters": devicePathParams(),
					"responses":  merge(map[string]any{"description": "device detail", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Device"}}}}, errorResponses("401", "404")),
				},
			},
			"/api/v1/devices/{tenantId}/{deviceId}/status": map[string]any{
				"get": map[string]any{
					"security":   bearer,
					"parameters": devicePathParams(),
					"responses":  merge(map[string]any{"description": "device status", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/DeviceStatus"}}}}, errorResponses("401", "404")),
				},
			},
			"/api/v1/devices/{tenantId}/{deviceId}/telemetry": map[string]any{
				"get": map[string]any{
					"security":   bearer,
					"parameters": append(devicePathParams(), paginationParams...),
					"responses":  merge(pagedResponse("#/components/schemas/TelemetryRecord", "device telemetry (keyset-paginated, ascending by received_at)"), errorResponses("400", "401", "404")),
				},
			},
			"/api/v1/telemetry": map[string]any{
				"post": map[string]any{
					"security":    bearer,
					"requestBody": map[string]any{"required": true, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/TelemetryEnvelope"}}}},
					"responses": map[string]any{
						"202": map[string]any{"description": "telemetry accepted"},
						"400": errorResponses("400")["400"], "401": errorResponses("401")["401"],
					},
				},
			},
			"/api/v1/commands": map[string]any{
				"get": map[string]any{
					"security":    bearer,
					"description": "List commands for one tenant; tenantId is required.",
					"parameters": append([]any{
						map[string]any{"name": "tenantId", "in": "query", "required": true, "schema": map[string]any{"type": "string"}},
					}, paginationParams...),
					"responses": merge(pagedResponse("#/components/schemas/Command", "list commands (keyset-paginated)"), errorResponses("400", "401")),
				},
				"post": map[string]any{
					"security":    bearer,
					"requestBody": map[string]any{"required": true, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/CommandInput"}}}},
					"responses": map[string]any{
						"201": map[string]any{"description": "command created", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Command"}}}},
						"400": errorResponses("400")["400"], "401": errorResponses("401")["401"], "404": errorResponses("404")["404"],
					},
				},
			},
			"/api/v1/commands/{id}": map[string]any{
				"get": map[string]any{
					"security":   bearer,
					"parameters": []any{map[string]any{"name": "id", "in": "path", "required": true, "schema": map[string]any{"type": "string"}}},
					"responses":  merge(map[string]any{"description": "command detail", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Command"}}}}, errorResponses("401", "404")),
				},
			},
			"/api/v1/commands/{id}/ack": map[string]any{
				"post": map[string]any{
					"security":    bearer,
					"description": "Manually acknowledge a command (used by integrations; device ACKs normally arrive over MQTT).",
					"parameters":  []any{map[string]any{"name": "id", "in": "path", "required": true, "schema": map[string]any{"type": "string"}}},
					"requestBody": map[string]any{"required": true, "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/CommandAckInput"}}}},
					"responses": map[string]any{
						"200": map[string]any{"description": "command acknowledged", "content": map[string]any{"application/json": map[string]any{"schema": map[string]any{"$ref": "#/components/schemas/Command"}}}},
						"400": errorResponses("400")["400"], "401": errorResponses("401")["401"], "404": errorResponses("404")["404"],
					},
				},
			},
		},
		"components": map[string]any{
			"securitySchemes": map[string]any{
				"bearerAuth": map[string]any{
					"type":         "http",
					"scheme":       "bearer",
					"bearerFormat": "token",
					"description":  "Service token from MANAGEMENT_API_TOKEN, sent as Authorization: Bearer <token>",
				},
			},
			"schemas": map[string]any{
				"ApiError": map[string]any{
					"type":       "object",
					"properties": map[string]any{"error": map[string]any{"type": "string"}},
				},
				"Tenant": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":   map[string]any{"type": "string"},
						"name": map[string]any{"type": "string"},
					},
				},
				"TenantInput": map[string]any{
					"type":     "object",
					"required": []string{"id", "name"},
					"properties": map[string]any{
						"id":   map[string]any{"type": "string"},
						"name": map[string]any{"type": "string"},
					},
				},
				"Device": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"tenantId":  map[string]any{"type": "string"},
						"deviceId":  map[string]any{"type": "string"},
						"productId": map[string]any{"type": "string"},
						"createdAt": map[string]any{"type": "string", "format": "date-time"},
					},
				},
				"DeviceInput": map[string]any{
					"type":     "object",
					"required": []string{"tenantId", "deviceId", "productId", "secret"},
					"properties": map[string]any{
						"tenantId":  map[string]any{"type": "string"},
						"deviceId":  map[string]any{"type": "string"},
						"productId": map[string]any{"type": "string"},
						"secret":    map[string]any{"type": "string", "description": "device MQTT password; stored as bcrypt hash"},
					},
				},
				"DeviceStatus": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"tenantId":   map[string]any{"type": "string"},
						"deviceId":   map[string]any{"type": "string"},
						"online":     map[string]any{"type": "boolean"},
						"lastSeenAt": map[string]any{"type": "string", "format": "date-time"},
					},
				},
				"TelemetryRecord": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"msgId":      map[string]any{"type": "string"},
						"tenantId":   map[string]any{"type": "string"},
						"deviceId":   map[string]any{"type": "string"},
						"ts":         map[string]any{"type": "integer"},
						"type":       map[string]any{"type": "string"},
						"version":    map[string]any{"type": "string"},
						"payload":    map[string]any{"type": "object"},
						"receivedAt": map[string]any{"type": "string", "format": "date-time"},
					},
				},
				"TelemetryEnvelope": map[string]any{
					"type":     "object",
					"required": []string{"msgId", "tenantId", "deviceId", "ts", "type", "version", "payload"},
					"properties": map[string]any{
						"msgId":    map[string]any{"type": "string"},
						"tenantId": map[string]any{"type": "string"},
						"deviceId": map[string]any{"type": "string"},
						"ts":       map[string]any{"type": "integer"},
						"type":     map[string]any{"type": "string"},
						"version":  map[string]any{"type": "string"},
						"payload":  map[string]any{"type": "object"},
					},
				},
				"Command": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":        map[string]any{"type": "string"},
						"tenantId":  map[string]any{"type": "string"},
						"deviceId":  map[string]any{"type": "string"},
						"status":    map[string]any{"type": "string", "enum": []string{"created", "published", "sent", "acked", "failed", "timeout"}},
						"payload":   map[string]any{"type": "object"},
						"createdAt": map[string]any{"type": "string", "format": "date-time"},
						"updatedAt": map[string]any{"type": "string", "format": "date-time"},
					},
				},
				"CommandInput": map[string]any{
					"type":     "object",
					"required": []string{"tenantId", "deviceId", "payload"},
					"properties": map[string]any{
						"tenantId": map[string]any{"type": "string"},
						"deviceId": map[string]any{"type": "string"},
						"payload":  map[string]any{"type": "object"},
					},
				},
				"CommandAckInput": map[string]any{
					"type":     "object",
					"required": []string{"tenantId", "deviceId"},
					"properties": map[string]any{
						"tenantId": map[string]any{"type": "string"},
						"deviceId": map[string]any{"type": "string"},
					},
				},
			},
		},
	}
}

func devicePathParams() []any {
	return []any{
		map[string]any{"name": "tenantId", "in": "path", "required": true, "schema": map[string]any{"type": "string"}},
		map[string]any{"name": "deviceId", "in": "path", "required": true, "schema": map[string]any{"type": "string"}},
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
