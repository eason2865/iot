package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"iot/internal/contracts"
)

// memoryStore is the in-memory Repository used only when New is given no Store.
// Production always injects the PostgreSQL store (see internal/bootstrap and
// internal/core); this type exists so the platform unit tests and the E2E
// harness can run without a database. Keep it behaviourally aligned with
// PostgresStore, but do not rely on it for durability or concurrency semantics.
type memoryStore struct {
	mu sync.RWMutex

	ttl time.Duration

	tenants     map[string]Tenant
	devices     map[string]Device
	statuses    map[string]DeviceStatus
	telemetry   map[string][]TelemetryRecord
	commandKeys map[string]string
	commands    map[string]Command
	commandSeq  int64
}

func newMemoryStore(ttl time.Duration) *memoryStore {
	return &memoryStore{
		ttl:         ttl,
		tenants:     map[string]Tenant{},
		devices:     map[string]Device{},
		statuses:    map[string]DeviceStatus{},
		telemetry:   map[string][]TelemetryRecord{},
		commands:    map[string]Command{},
		commandKeys: map[string]string{},
	}
}

func deviceKey(tenantID, deviceID string) string {
	return tenantID + ":" + deviceID
}

func (s *memoryStore) CreateTenant(t Tenant) (Tenant, error) {
	if !contracts.IsValidTenantID(t.ID) {
		return Tenant{}, contracts.ErrInvalidTopicPart
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[t.ID]; exists {
		return Tenant{}, fmt.Errorf("tenant %w", ErrAlreadyExists)
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
	if !contracts.IsValidTenantID(d.TenantID) || !contracts.IsValidTopicPart(d.DeviceID) {
		return Device{}, contracts.ErrInvalidTopicPart
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[d.TenantID]; !exists {
		return Device{}, fmt.Errorf("tenant %w", ErrNotFound)
	}
	key := deviceKey(d.TenantID, d.DeviceID)
	if _, exists := s.devices[key]; exists {
		return Device{}, fmt.Errorf("device %w", ErrAlreadyExists)
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
	return s.RestoreTelemetryContext(context.Background(), TelemetryRecord{
		MsgID: env.MsgID, TenantID: env.TenantID, DeviceID: env.DeviceID, Ts: env.Ts,
		Type: env.Type, Version: env.Version, Payload: env.Payload, ReceivedAt: time.Now().UTC(),
	})
}

func (s *memoryStore) RestoreTelemetryContext(ctx context.Context, rec TelemetryRecord) (TelemetryRecord, error) {
	if err := ctx.Err(); err != nil {
		return TelemetryRecord{}, err
	}
	if err := contracts.ValidateTelemetryPayload(rec.Payload); err != nil {
		return TelemetryRecord{}, err
	}
	if rec.ReceivedAt.IsZero() {
		return TelemetryRecord{}, fmt.Errorf("%w: original receive time is required", contracts.ErrInvalidEnvelope)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := deviceKey(rec.TenantID, rec.DeviceID)
	if _, exists := s.devices[key]; !exists {
		return TelemetryRecord{}, fmt.Errorf("device %w", ErrNotFound)
	}
	s.telemetry[key] = append(s.telemetry[key], rec)
	if previous := s.statuses[key]; previous.LastSeenAt.IsZero() || previous.LastSeenAt.Before(rec.ReceivedAt) {
		s.statuses[key] = DeviceStatus{TenantID: rec.TenantID, DeviceID: rec.DeviceID, Online: true, LastSeenAt: rec.ReceivedAt}
	}
	return rec, nil
}

func (s *memoryStore) CreateCommand(tenantID, deviceID string, payload json.RawMessage) (Command, error) {
	return s.CreateCommandWithKey(context.Background(), tenantID, deviceID, "", payload)
}

func (s *memoryStore) createCommand(tenantID, deviceID string, payload json.RawMessage) (Command, error) {
	key := deviceKey(tenantID, deviceID)
	if _, exists := s.devices[key]; !exists {
		return Command{}, fmt.Errorf("device %w", ErrNotFound)
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
		return Command{}, fmt.Errorf("command %w", ErrNotFound)
	}
	if cmd.TenantID != tenantID || cmd.DeviceID != deviceID {
		return Command{}, fmt.Errorf("command %w", ErrNotFound)
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

// The memory implementation is only a test harness; PostgreSQL owns production
// atomicity. Reuse the same lock for key lookup and command allocation.
func (s *memoryStore) CreateCommandWithKey(ctx context.Context, tenantID, deviceID, key string, payload json.RawMessage) (Command, error) {
	if err := ctx.Err(); err != nil {
		return Command{}, err
	}
	if err := contracts.ValidateIdempotencyKey(key); err != nil {
		return Command{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return Command{}, err
	}
	if key != "" {
		for _, cmd := range s.commands {
			if cmd.TenantID == tenantID && s.commandKeys[cmd.ID] == key {
				var old any
				_ = json.Unmarshal(cmd.Payload, &old)
				if cmd.DeviceID != deviceID || !reflect.DeepEqual(old, value) {
					return Command{}, ErrIdempotencyConflict
				}
				return cmd, nil
			}
		}
	}
	cmd, err := s.createCommand(tenantID, deviceID, payload)
	if err == nil && key != "" {
		s.commandKeys[cmd.ID] = key
	}
	return cmd, err
}
