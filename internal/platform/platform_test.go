package platform_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"mqtt/internal/platform"
)

func TestDeviceTelemetryAndStatusFlow(t *testing.T) {
	app := platform.New(platform.Config{ServiceName: "admin"})
	ts := httptest.NewServer(app.Router())
	defer ts.Close()

	createTenant(t, ts.URL, "tenant-a", "Tenant A")
	createDevice(t, ts.URL, "tenant-a", "device-42", "product-x")

	postJSON(t, ts.URL+"/api/v1/telemetry", map[string]any{
		"msgId":    "msg-1",
		"tenantId": "tenant-a",
		"deviceId": "device-42",
		"ts":       1717670000000,
		"type":     "telemetry",
		"version":  "v1",
		"payload": map[string]any{
			"temp": 23.4,
		},
	})

	var status platform.DeviceStatus
	getJSON(t, ts.URL+"/api/v1/devices/tenant-a/device-42/status", &status)

	if !status.Online {
		t.Fatalf("device should be online after telemetry")
	}
	if status.LastSeenAt.IsZero() {
		t.Fatalf("LastSeenAt should be set")
	}

	var records []platform.TelemetryRecord
	getJSON(t, ts.URL+"/api/v1/devices/tenant-a/device-42/telemetry", &records)
	if len(records) != 1 {
		t.Fatalf("telemetry records = %d, want 1", len(records))
	}
}

func TestCommandAckFlow(t *testing.T) {
	app := platform.New(platform.Config{ServiceName: "admin"})
	ts := httptest.NewServer(app.Router())
	defer ts.Close()

	createTenant(t, ts.URL, "tenant-a", "Tenant A")
	createDevice(t, ts.URL, "tenant-a", "device-42", "product-x")

	var created platform.Command
	postJSON(t, ts.URL+"/api/v1/commands", map[string]any{
		"tenantId": "tenant-a",
		"deviceId": "device-42",
		"payload": map[string]any{
			"switch": "on",
		},
	}, &created)

	if created.Status != platform.CommandStatusSent {
		t.Fatalf("created status = %q, want %q", created.Status, platform.CommandStatusSent)
	}

	postJSON(t, ts.URL+"/api/v1/commands/"+created.ID+"/ack", map[string]any{
		"tenantId": "tenant-a",
		"deviceId": "device-42",
	})

	var got platform.Command
	getJSON(t, ts.URL+"/api/v1/commands/"+created.ID, &got)
	if got.Status != platform.CommandStatusAcked {
		t.Fatalf("command status = %q, want %q", got.Status, platform.CommandStatusAcked)
	}
}

func createTenant(t *testing.T, baseURL, id, name string) {
	t.Helper()
	postJSON(t, baseURL+"/api/v1/tenants", map[string]any{
		"id":   id,
		"name": name,
	})
}

func createDevice(t *testing.T, baseURL, tenantID, deviceID, productID string) {
	t.Helper()
	postJSON(t, baseURL+"/api/v1/devices", map[string]any{
		"tenantId":  tenantID,
		"deviceId":  deviceID,
		"productId": productID,
		"secret":    "secret-1",
	})
}

func postJSON(t *testing.T, url string, body map[string]any, out ...any) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("http.Post() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("status code = %d", resp.StatusCode)
	}
	if len(out) > 0 {
		if err := json.NewDecoder(resp.Body).Decode(out[0]); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
	}
}

func getJSON(t *testing.T, url string, out any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("http.Get() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("status code = %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
}
