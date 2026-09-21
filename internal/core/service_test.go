package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

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

func TestListCommandsScopesToTenant(t *testing.T) {
	svc := NewService(pagedFakeRepo{}, nil)
	resp, err := svc.ListCommands(t.Context(), &corev1.ListCommandsRequest{
		TenantId: "tenant-a",
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("ListCommands() error = %v", err)
	}
	if len(resp.GetCommands()) != 1 || resp.GetCommands()[0].GetTenantId() != "tenant-a" {
		t.Fatalf("ListCommands() returned %+v, want tenant-a only", resp.GetCommands())
	}
}

func TestListCommandsRequiresTenant(t *testing.T) {
	svc := NewService(pagedFakeRepo{}, nil)
	if _, err := svc.ListCommands(t.Context(), &corev1.ListCommandsRequest{}); err == nil || !strings.Contains(err.Error(), "tenantId is required") {
		t.Fatalf("ListCommands() error = %v, want tenantId required", err)
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

type pagedFakeRepo struct{ fakeRepo }

func (pagedFakeRepo) ListCommandsPage(page platform.PageRequest) ([]platform.Command, string, error) {
	if page.TenantID != "tenant-a" {
		return nil, "", fmt.Errorf("unexpected tenant filter %q", page.TenantID)
	}
	return []platform.Command{{ID: "cmd-a", TenantID: "tenant-a", DeviceID: "device-a", CreatedAt: time.Unix(1, 0).UTC()}}, "next", nil
}

// dupRepo always reports the telemetry as a duplicate, simulating a retry after
// a previous attempt wrote PostgreSQL but failed to publish to Kafka.
type dupRepo struct{ fakeRepo }

func (dupRepo) RecordTelemetry(env contracts.Envelope) (platform.TelemetryRecord, error) {
	return platform.TelemetryRecord{MsgID: env.MsgID, TenantID: env.TenantID, DeviceID: env.DeviceID}, platform.ErrDuplicateTelemetry
}

// countingPublisher records how many times telemetry was published.
type countingPublisher struct{ telemetry int }

func (p *countingPublisher) PublishTelemetry(platform.TelemetryRecord) error {
	p.telemetry++
	return nil
}
func (p *countingPublisher) PublishCommand(platform.Command) error { return nil }

// TestIngestTelemetryRepublishesOnDuplicate pins the fix for the Kafka-loss
// race: when the first IngestTelemetry wrote PostgreSQL but failed to publish,
// a retry hits the duplicate branch. The duplicate branch must STILL publish to
// Kafka so the event reaches the worker/TDengine; otherwise the row exists but
// is never delivered downstream.
func TestIngestTelemetryRepublishesOnDuplicate(t *testing.T) {
	pub := &countingPublisher{}
	svc := NewService(dupRepo{}, pub)
	_, err := svc.IngestTelemetry(context.Background(), &corev1.IngestTelemetryRequest{
		MsgId: "m1", TenantId: "t1", DeviceId: "d1",
	})
	if err != nil {
		t.Fatalf("IngestTelemetry duplicate: %v", err)
	}
	if pub.telemetry != 1 {
		t.Fatalf("expected Kafka publish on duplicate path, got %d publishes", pub.telemetry)
	}
}
