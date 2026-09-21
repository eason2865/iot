package platform

import (
	"encoding/json"
	"errors"

	"iot/internal/contracts"
)

// ErrDuplicateTelemetry marks a telemetry envelope already stored for the
// same (msg_id, tenant_id, device_id), so downstream sinks stay idempotent
// when a DLQ replay re-publishes the message.
var ErrDuplicateTelemetry = errors.New("duplicate telemetry message")

// errIdentityMismatch marks a message whose envelope tenant/device identity
// does not match the MQTT topic it arrived on (identity spoofing attempt).
var errIdentityMismatch = errors.New("envelope identity does not match mqtt topic")

// IsTelemetryDuplicate reports whether err wraps ErrDuplicateTelemetry.
func IsTelemetryDuplicate(err error) bool {
	return errors.Is(err, ErrDuplicateTelemetry)
}

type Repository interface {
	CreateTenant(Tenant) (Tenant, error)
	ListTenants() []Tenant
	CreateDevice(Device) (Device, error)
	ListDevices() []Device
	GetDevice(tenantID, deviceID string) (Device, bool)
	RecordTelemetry(env contracts.Envelope) (TelemetryRecord, error)
	ListTelemetry(tenantID, deviceID string) []TelemetryRecord
	GetDeviceStatus(tenantID, deviceID string) (DeviceStatus, bool)
	CreateCommand(tenantID, deviceID string, payload json.RawMessage) (Command, error)
	AckCommand(id, tenantID, deviceID string) (Command, error)
	ListCommands() []Command
	GetCommand(id string) (Command, bool)
}

type MessagePublisher interface {
	PublishTelemetry(TelemetryRecord) error
	PublishCommand(Command) error
}

type noopPublisher struct{}

func (noopPublisher) PublishTelemetry(TelemetryRecord) error { return nil }
func (noopPublisher) PublishCommand(Command) error           { return nil }
