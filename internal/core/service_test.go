package core

import (
	"encoding/json"
	"strings"
	"testing"

	"iot/internal/contracts"
	"iot/internal/platform"
	corev1 "iot/proto/core/v1"
)

func TestServiceRejectsInvalidMQTTTopicIdentifiers(t *testing.T) {
	svc := NewService(fakeRepo{}, nil)

	if _, err := svc.CreateTenant(t.Context(), &corev1.CreateTenantRequest{Id: "tenant/#", Name: "bad"}); err == nil || !strings.Contains(err.Error(), "invalid MQTT topic") {
		t.Fatalf("CreateTenant() error = %v, want invalid MQTT topic", err)
	}
	if _, err := svc.CreateDevice(t.Context(), &corev1.CreateDeviceRequest{
		TenantId:  "tenant-a",
		DeviceId:  "device/+",
		ProductId: "product-x",
		Secret:    "secret-1",
	}); err == nil || !strings.Contains(err.Error(), "invalid MQTT topic") {
		t.Fatalf("CreateDevice() error = %v, want invalid MQTT topic", err)
	}
	if _, err := svc.CreateCommand(t.Context(), &corev1.CreateCommandRequest{
		TenantId: "tenant-a",
		DeviceId: "device/42",
		Payload:  json.RawMessage(`{"switch":"on"}`),
	}); err == nil || !strings.Contains(err.Error(), "invalid MQTT topic") {
		t.Fatalf("CreateCommand() error = %v, want invalid MQTT topic", err)
	}
}

type fakeRepo struct{}

func (fakeRepo) CreateTenant(platform.Tenant) (platform.Tenant, error) { return platform.Tenant{}, nil }
func (fakeRepo) ListTenants() []platform.Tenant                        { return nil }
func (fakeRepo) CreateDevice(platform.Device) (platform.Device, error) { return platform.Device{}, nil }
func (fakeRepo) ListDevices() []platform.Device                        { return nil }
func (fakeRepo) GetDevice(string, string) (platform.Device, bool)      { return platform.Device{}, false }
func (fakeRepo) RecordTelemetry(contracts.Envelope) (platform.TelemetryRecord, error) {
	return platform.TelemetryRecord{}, nil
}
func (fakeRepo) ListTelemetry(string, string) []platform.TelemetryRecord { return nil }
func (fakeRepo) GetDeviceStatus(string, string) (platform.DeviceStatus, bool) {
	return platform.DeviceStatus{}, false
}
func (fakeRepo) CreateCommand(string, string, json.RawMessage) (platform.Command, error) {
	return platform.Command{}, nil
}
func (fakeRepo) AckCommand(string, string, string) (platform.Command, error) {
	return platform.Command{}, nil
}
func (fakeRepo) ListCommands() []platform.Command           { return nil }
func (fakeRepo) GetCommand(string) (platform.Command, bool) { return platform.Command{}, false }
