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
	if err := contracts.ValidateTenantName(t.Name); err != nil {
		return Tenant{}, err
	}
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

func (s *memoryStore) CreateDevice(d Device) (Device, error) {
	if err := contracts.ValidateProductID(d.ProductID); err != nil {
		return Device{}, err
	}
	if err := contracts.ValidateDeviceSecret(d.Secret); err != nil {
		return Device{}, err
	}
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

func (s *memoryStore) AckCommandResultContext(ctx context.Context, id, tenantID, deviceID string, input contracts.CommandResult) (Command, error) {
	if err := ctx.Err(); err != nil {
		return Command{}, err
	}
	if err := contracts.ValidateDeviceIdentity(tenantID, deviceID); err != nil {
		return Command{}, err
	}
	if err := contracts.ValidateCommandID(id); err != nil {
		return Command{}, err
	}
	result, err := contracts.NormalizeCommandResult(input)
	if err != nil {
		return Command{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd, ok := s.commands[id]
	if !ok || cmd.TenantID != tenantID || cmd.DeviceID != deviceID {
		return Command{}, ErrNotFound
	}
	if commandAlreadyDelivered(cmd.Status) && cmd.Status != contracts.CommandStatusSent {
		if cmd.Result != nil {
			a, _ := json.Marshal(cmd.Result)
			b, _ := json.Marshal(result)
			var av, bv any
			_ = json.Unmarshal(a, &av)
			_ = json.Unmarshal(b, &bv)
			if !reflect.DeepEqual(av, bv) {
				return Command{}, ErrAcknowledgementConflict
			}
		}
		return cmd, nil
	}
	next, err := contracts.AdvanceCommandStatus(cmd.Status, contracts.CommandEvent(result.Status))
	if err != nil {
		return Command{}, err
	}
	cmd.Status = next
	cmd.UpdatedAt = time.Now().UTC()
	cmd.Result = &result
	s.commands[id] = cmd
	return cmd, nil
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

// The memory implementation is only a test harness; PostgreSQL owns production
// atomicity. Reuse the same lock for key lookup and command allocation.
func (s *memoryStore) CreateCommandWithKey(ctx context.Context, tenantID, deviceID, key string, payload json.RawMessage) (Command, error) {
	if err := ValidateCommandInput(tenantID, deviceID, payload); err != nil {
		return Command{}, err
	}
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

type noopPublisher struct{}

func (noopPublisher) PublishTelemetry(context.Context, TelemetryRecord) error { return nil }
func (noopPublisher) PublishCommand(context.Context, Command) error           { return nil }
