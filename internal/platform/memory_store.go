package platform

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"iot/internal/contracts"
)

type memoryStore struct {
	mu sync.RWMutex

	ttl time.Duration

	tenants    map[string]Tenant
	devices    map[string]Device
	statuses   map[string]DeviceStatus
	telemetry  map[string][]TelemetryRecord
	commands   map[string]Command
	commandSeq int64
}

func newMemoryStore(ttl time.Duration) *memoryStore {
	return &memoryStore{
		ttl:       ttl,
		tenants:   map[string]Tenant{},
		devices:   map[string]Device{},
		statuses:  map[string]DeviceStatus{},
		telemetry: map[string][]TelemetryRecord{},
		commands:  map[string]Command{},
	}
}

func deviceKey(tenantID, deviceID string) string {
	return tenantID + ":" + deviceID
}

func (s *memoryStore) CreateTenant(t Tenant) (Tenant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[t.ID]; exists {
		return Tenant{}, fmt.Errorf("tenant already exists")
	}
	s.tenants[t.ID] = t
	return t, nil
}

func (s *memoryStore) ListTenants() []Tenant {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Tenant, 0, len(s.tenants))
	for _, tenant := range s.tenants {
		out = append(out, tenant)
	}
	return out
}

func (s *memoryStore) CreateDevice(d Device) (Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[d.TenantID]; !exists {
		return Device{}, fmt.Errorf("tenant not found")
	}
	key := deviceKey(d.TenantID, d.DeviceID)
	if _, exists := s.devices[key]; exists {
		return Device{}, fmt.Errorf("device already exists")
	}
	d.CreatedAt = time.Now().UTC()
	s.devices[key] = d
	s.statuses[key] = DeviceStatus{TenantID: d.TenantID, DeviceID: d.DeviceID, Online: false}
	return d, nil
}

func (s *memoryStore) ListDevices() []Device {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Device, 0, len(s.devices))
	for _, device := range s.devices {
		out = append(out, device)
	}
	return out
}

func (s *memoryStore) GetDevice(tenantID, deviceID string) (Device, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	device, ok := s.devices[deviceKey(tenantID, deviceID)]
	return device, ok
}

func (s *memoryStore) RecordTelemetry(env contracts.Envelope) (TelemetryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := deviceKey(env.TenantID, env.DeviceID)
	if _, exists := s.devices[key]; !exists {
		return TelemetryRecord{}, fmt.Errorf("device not found")
	}
	rec := TelemetryRecord{
		MsgID:      env.MsgID,
		TenantID:   env.TenantID,
		DeviceID:   env.DeviceID,
		Ts:         env.Ts,
		Type:       env.Type,
		Version:    env.Version,
		Payload:    env.Payload,
		ReceivedAt: time.Now().UTC(),
	}
	s.telemetry[key] = append(s.telemetry[key], rec)
	s.statuses[key] = DeviceStatus{
		TenantID:   env.TenantID,
		DeviceID:   env.DeviceID,
		Online:     true,
		LastSeenAt: time.Now().UTC(),
	}
	return rec, nil
}

func (s *memoryStore) CreateCommand(tenantID, deviceID string, payload json.RawMessage) (Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := deviceKey(tenantID, deviceID)
	if _, exists := s.devices[key]; !exists {
		return Command{}, fmt.Errorf("device not found")
	}
	s.commandSeq++
	id := fmt.Sprintf("cmd-%d", s.commandSeq)
	now := time.Now().UTC()
	cmd := Command{
		ID:        id,
		TenantID:  tenantID,
		DeviceID:  deviceID,
		Status:    contracts.CommandStatusCreated,
		Payload:   payload,
		CreatedAt: now,
		UpdatedAt: now,
	}
	next, err := contracts.AdvanceCommandStatus(cmd.Status, contracts.CommandEventPublished)
	if err != nil {
		return Command{}, err
	}
	cmd.Status = next
	cmd.UpdatedAt = time.Now().UTC()
	s.commands[id] = cmd
	return cmd, nil
}

func (s *memoryStore) AckCommand(id, tenantID, deviceID string) (Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd, exists := s.commands[id]
	if !exists {
		return Command{}, fmt.Errorf("command not found")
	}
	if cmd.TenantID != tenantID || cmd.DeviceID != deviceID {
		return Command{}, fmt.Errorf("command does not belong to device")
	}
	next, err := contracts.AdvanceCommandStatus(cmd.Status, contracts.CommandEventAcked)
	if err != nil {
		return Command{}, err
	}
	cmd.Status = next
	cmd.UpdatedAt = time.Now().UTC()
	s.commands[id] = cmd
	return cmd, nil
}

func (s *memoryStore) ListCommands() []Command {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Command, 0, len(s.commands))
	for _, command := range s.commands {
		out = append(out, command)
	}
	return out
}

func (s *memoryStore) GetCommand(id string) (Command, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cmd, ok := s.commands[id]
	return cmd, ok
}

func (s *memoryStore) GetDeviceStatus(tenantID, deviceID string) (DeviceStatus, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := deviceKey(tenantID, deviceID)
	status, ok := s.statuses[key]
	if !ok {
		return DeviceStatus{}, false
	}
	if !status.LastSeenAt.IsZero() && time.Since(status.LastSeenAt) > s.ttl {
		status.Online = false
	}
	return status, true
}

func (s *memoryStore) ListTelemetry(tenantID, deviceID string) []TelemetryRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := deviceKey(tenantID, deviceID)
	src := s.telemetry[key]
	out := make([]TelemetryRecord, len(src))
	copy(out, src)
	return out
}
