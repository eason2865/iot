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

// ErrNotFound marks a repository lookup that matched no row, or a row that
// belongs to a different tenant or device. Both cases are deliberately the same
// error: telling a caller that a command exists but belongs to someone else
// leaks the existence of another tenant's data. Callers map it to a NotFound API
// error, so the API surface never has to match on message text.
var ErrNotFound = errors.New("not found")

// ErrAlreadyExists marks a uniqueness conflict on create (tenant or device),
// mapped to an AlreadyExists API error.
var ErrAlreadyExists = errors.New("already exists")

// IsNotFound reports whether err wraps ErrNotFound.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsAlreadyExists reports whether err wraps ErrAlreadyExists.
func IsAlreadyExists(err error) bool {
	return errors.Is(err, ErrAlreadyExists)
}

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
