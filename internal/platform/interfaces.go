package platform

import (
	"encoding/json"

	"iot/internal/contracts"
)

type Store interface {
	createTenant(Tenant) (Tenant, error)
	listTenants() []Tenant
	createDevice(Device) (Device, error)
	listDevices() []Device
	getDevice(tenantID, deviceID string) (Device, bool)
	recordTelemetry(env contracts.Envelope) (TelemetryRecord, error)
	listTelemetry(tenantID, deviceID string) []TelemetryRecord
	getDeviceStatus(tenantID, deviceID string) (DeviceStatus, bool)
	createCommand(tenantID, deviceID string, payload json.RawMessage) (Command, error)
	ackCommand(id, tenantID, deviceID string) (Command, error)
	listCommands() []Command
	getCommand(id string) (Command, bool)
}

type Publisher interface {
	publishTelemetry(TelemetryRecord) error
	publishCommand(Command) error
}

type noopPublisher struct{}

func (noopPublisher) publishTelemetry(TelemetryRecord) error { return nil }
func (noopPublisher) publishCommand(Command) error           { return nil }
