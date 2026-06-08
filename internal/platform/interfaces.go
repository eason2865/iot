package platform

import (
	"encoding/json"

	"iot/internal/contracts"
)

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
