package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// TestGetCommandScopesToTenant pins the tenant contract of the command detail
// endpoint. Before this, ListCommands and AckCommand required a tenant while
// GetCommand accepted a bare ID, so a caller could read any tenant's command and
// the three endpoints disagreed about their contract.
func TestGetCommandScopesToTenant(t *testing.T) {
	svc := NewService(commandRepo{}, nil)

	if _, err := svc.GetCommand(t.Context(), &corev1.GetCommandRequest{Id: "cmd-a"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetCommand() without tenantId: code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}

	resp, err := svc.GetCommand(t.Context(), &corev1.GetCommandRequest{Id: "cmd-a", TenantId: "tenant-a"})
	if err != nil {
		t.Fatalf("GetCommand() for the owning tenant: %v", err)
	}
	if resp.GetCommand().GetId() != "cmd-a" {
		t.Fatalf("GetCommand() returned %+v", resp.GetCommand())
	}

	// A foreign tenant and a nonexistent command must be indistinguishable, so
	// the response cannot confirm that another tenant's command exists.
	foreign, err := svc.GetCommand(t.Context(), &corev1.GetCommandRequest{Id: "cmd-a", TenantId: "tenant-b"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetCommand() cross-tenant: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
	if foreign != nil {
		t.Fatal("GetCommand() returned a command for a foreign tenant")
	}
	if _, err := svc.GetCommand(t.Context(), &corev1.GetCommandRequest{Id: "missing", TenantId: "tenant-a"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetCommand() unknown id: code = %v, want NotFound", status.Code(err))
	}
}

// TestValidationErrorsUseInvalidArgument guards the API status codes. iot-core
// used to return plain errors, and the gateway matched on message substrings, so
// "tenantId contains invalid MQTT topic characters" matched no pattern and
// surfaced as 502 Bad Gateway instead of 400.
func TestValidationErrorsUseInvalidArgument(t *testing.T) {
	svc := NewService(fakeRepo{}, nil)

	if _, err := svc.CreateTenant(t.Context(), &corev1.CreateTenantRequest{Id: "tenant/#", Name: "bad"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateTenant() invalid id: code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
	if _, err := svc.ListCommands(t.Context(), &corev1.ListCommandsRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListCommands() without tenant: code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := svc.GetDevice(t.Context(), &corev1.GetDeviceRequest{TenantId: "t", DeviceId: "d"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetDevice() missing: code = %v, want NotFound", status.Code(err))
	}
}

// TestRepoErrorsMapToStatusCodes pins the repository-to-status translation that
// replaced substring matching in the gateway.
func TestRepoErrorsMapToStatusCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"not found", fmt.Errorf("tenant %w", platform.ErrNotFound), codes.NotFound},
		{"already exists", fmt.Errorf("tenant %w", platform.ErrAlreadyExists), codes.AlreadyExists},
		{"unexpected", errors.New("connection reset"), codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.Code(mapRepoError(tc.err)); got != tc.want {
				t.Fatalf("mapRepoError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
	if err := mapRepoError(nil); err != nil {
		t.Fatalf("mapRepoError(nil) = %v, want nil", err)
	}

	svc := NewService(conflictRepo{}, nil)
	if _, err := svc.CreateTenant(t.Context(), &corev1.CreateTenantRequest{Id: "tenant-a", Name: "A"}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateTenant() duplicate: code = %v, want AlreadyExists (err=%v)", status.Code(err), err)
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

func (p *countingPublisher) PublishTelemetry(context.Context, platform.TelemetryRecord) error {
	p.telemetry++
	return nil
}
func (p *countingPublisher) PublishCommand(context.Context, platform.Command) error { return nil }

// TestIngestTelemetryRepublishesOnDuplicate pins the fix for the Kafka-loss
// race: when the first IngestTelemetry wrote PostgreSQL but failed to publish,
// a retry hits the duplicate branch. The duplicate branch must STILL publish to
// Kafka so the event reaches the worker/TDengine; otherwise the row exists but
// is never delivered downstream.
func TestIngestTelemetryRepublishesOnDuplicate(t *testing.T) {
	pub := &countingPublisher{}
	svc := NewService(dupRepo{}, pub)
	_, err := svc.IngestTelemetry(context.Background(), &corev1.IngestTelemetryRequest{
		MsgId: "m1", TenantId: "t1", DeviceId: "d1", Type: "telemetry", Version: "v1",
		Ts: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("IngestTelemetry duplicate: %v", err)
	}
	if pub.telemetry != 1 {
		t.Fatalf("expected Kafka publish on duplicate path, got %d publishes", pub.telemetry)
	}
}

// commandRepo serves a single command owned by tenant-a.
type commandRepo struct{ fakeRepo }

func (commandRepo) GetCommand(id string) (platform.Command, bool) {
	if id != "cmd-a" {
		return platform.Command{}, false
	}
	return platform.Command{ID: "cmd-a", TenantID: "tenant-a", DeviceID: "device-a"}, true
}

// conflictRepo always reports a uniqueness conflict.
type conflictRepo struct{ fakeRepo }

func (conflictRepo) CreateTenant(platform.Tenant) (platform.Tenant, error) {
	return platform.Tenant{}, fmt.Errorf("tenant %w", platform.ErrAlreadyExists)
}
