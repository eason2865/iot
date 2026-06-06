package platform

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"iot/internal/contracts"
)

type PostgresStore struct {
	db  *sql.DB
	ttl time.Duration
}

func NewPostgresStore(dsn string, ttl time.Duration) (*PostgresStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &PostgresStore{db: db, ttl: ttl}
	if err := store.ensureSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *PostgresStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *PostgresStore) ensureSchema(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS tenants (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS devices (
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  secret TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, device_id),
  CONSTRAINT fk_devices_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS device_state (
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  connected BOOLEAN NOT NULL DEFAULT FALSE,
  last_seen_at TIMESTAMPTZ,
  last_msg_id TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, device_id)
);

CREATE TABLE IF NOT EXISTS telemetry_records (
  id BIGSERIAL PRIMARY KEY,
  msg_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  ts BIGINT NOT NULL,
  type TEXT NOT NULL,
  version TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (msg_id, tenant_id, device_id)
);

CREATE TABLE IF NOT EXISTS commands (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  status TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_devices_tenant ON devices(tenant_id);
CREATE INDEX IF NOT EXISTS idx_commands_tenant_device ON commands(tenant_id, device_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_tenant_device ON telemetry_records(tenant_id, device_id, received_at DESC);
`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *PostgresStore) createTenant(t Tenant) (Tenant, error) {
	_, err := s.db.Exec(`INSERT INTO tenants (id, name) VALUES ($1, $2)`, t.ID, t.Name)
	if err != nil {
		return Tenant{}, translateSQLError(err, "tenant")
	}
	return t, nil
}

func (s *PostgresStore) CreateTenant(t Tenant) (Tenant, error) { return s.createTenant(t) }

func (s *PostgresStore) listTenants() []Tenant {
	rows, err := s.db.Query(`SELECT id, name FROM tenants ORDER BY id`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.ID, &t.Name); err == nil {
			out = append(out, t)
		}
	}
	return out
}

func (s *PostgresStore) ListTenants() []Tenant { return s.listTenants() }

func (s *PostgresStore) createDevice(d Device) (Device, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Device{}, err
	}
	defer tx.Rollback()
	var exists bool
	if err := tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM tenants WHERE id = $1)`, d.TenantID).Scan(&exists); err != nil {
		return Device{}, err
	}
	if !exists {
		return Device{}, fmt.Errorf("tenant not found")
	}
	d.CreatedAt = time.Now().UTC()
	_, err = tx.Exec(`INSERT INTO devices (tenant_id, device_id, product_id, secret, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
		d.TenantID, d.DeviceID, d.ProductID, d.Secret, d.CreatedAt)
	if err != nil {
		return Device{}, translateSQLError(err, "device")
	}
	_, err = tx.Exec(`INSERT INTO device_state (tenant_id, device_id, connected, updated_at) VALUES ($1, $2, false, $3)
		ON CONFLICT (tenant_id, device_id) DO UPDATE SET updated_at = EXCLUDED.updated_at`,
		d.TenantID, d.DeviceID, d.CreatedAt)
	if err != nil {
		return Device{}, err
	}
	if err := tx.Commit(); err != nil {
		return Device{}, err
	}
	return d, nil
}

func (s *PostgresStore) CreateDevice(d Device) (Device, error) { return s.createDevice(d) }

func (s *PostgresStore) listDevices() []Device {
	rows, err := s.db.Query(`SELECT tenant_id, device_id, product_id, secret, created_at FROM devices ORDER BY tenant_id, device_id`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.TenantID, &d.DeviceID, &d.ProductID, &d.Secret, &d.CreatedAt); err == nil {
			out = append(out, d)
		}
	}
	return out
}

func (s *PostgresStore) ListDevices() []Device { return s.listDevices() }

func (s *PostgresStore) getDevice(tenantID, deviceID string) (Device, bool) {
	var d Device
	err := s.db.QueryRow(`SELECT tenant_id, device_id, product_id, secret, created_at FROM devices WHERE tenant_id = $1 AND device_id = $2`,
		tenantID, deviceID).Scan(&d.TenantID, &d.DeviceID, &d.ProductID, &d.Secret, &d.CreatedAt)
	if err != nil {
		return Device{}, false
	}
	return d, true
}

func (s *PostgresStore) GetDevice(tenantID, deviceID string) (Device, bool) {
	return s.getDevice(tenantID, deviceID)
}

func (s *PostgresStore) recordTelemetry(env contracts.Envelope) (TelemetryRecord, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return TelemetryRecord{}, err
	}
	defer tx.Rollback()
	if _, ok := s.getDevice(env.TenantID, env.DeviceID); !ok {
		return TelemetryRecord{}, fmt.Errorf("device not found")
	}
	payloadBytes, err := json.Marshal(env.Payload)
	if err != nil {
		return TelemetryRecord{}, err
	}
	now := time.Now().UTC()
	rec := TelemetryRecord{
		MsgID:      env.MsgID,
		TenantID:   env.TenantID,
		DeviceID:   env.DeviceID,
		Ts:         env.Ts,
		Type:       env.Type,
		Version:    env.Version,
		Payload:    env.Payload,
		ReceivedAt: now,
	}
	_, err = tx.Exec(`INSERT INTO telemetry_records (msg_id, tenant_id, device_id, ts, type, version, payload, received_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (msg_id, tenant_id, device_id) DO NOTHING`,
		rec.MsgID, rec.TenantID, rec.DeviceID, rec.Ts, rec.Type, rec.Version, payloadBytes, rec.ReceivedAt)
	if err != nil {
		return TelemetryRecord{}, err
	}
	_, err = tx.Exec(`INSERT INTO device_state (tenant_id, device_id, connected, last_seen_at, last_msg_id, updated_at)
		VALUES ($1,$2,true,$3,$4,$3)
		ON CONFLICT (tenant_id, device_id) DO UPDATE SET connected = true, last_seen_at = EXCLUDED.last_seen_at, last_msg_id = EXCLUDED.last_msg_id, updated_at = EXCLUDED.updated_at`,
		rec.TenantID, rec.DeviceID, now, rec.MsgID)
	if err != nil {
		return TelemetryRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return TelemetryRecord{}, err
	}
	return rec, nil
}

func (s *PostgresStore) RecordTelemetry(env contracts.Envelope) (TelemetryRecord, error) {
	return s.recordTelemetry(env)
}

func (s *PostgresStore) listTelemetry(tenantID, deviceID string) []TelemetryRecord {
	rows, err := s.db.Query(`SELECT msg_id, tenant_id, device_id, ts, type, version, payload, received_at
		FROM telemetry_records WHERE tenant_id = $1 AND device_id = $2 ORDER BY received_at ASC`, tenantID, deviceID)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []TelemetryRecord
	for rows.Next() {
		var rec TelemetryRecord
		var payload []byte
		if err := rows.Scan(&rec.MsgID, &rec.TenantID, &rec.DeviceID, &rec.Ts, &rec.Type, &rec.Version, &payload, &rec.ReceivedAt); err == nil {
			rec.Payload = json.RawMessage(payload)
			out = append(out, rec)
		}
	}
	return out
}

func (s *PostgresStore) ListTelemetry(tenantID, deviceID string) []TelemetryRecord {
	return s.listTelemetry(tenantID, deviceID)
}

func (s *PostgresStore) getDeviceStatus(tenantID, deviceID string) (DeviceStatus, bool) {
	var status DeviceStatus
	var lastSeen sql.NullTime
	err := s.db.QueryRow(`SELECT tenant_id, device_id, connected, last_seen_at FROM device_state WHERE tenant_id = $1 AND device_id = $2`,
		tenantID, deviceID).Scan(&status.TenantID, &status.DeviceID, &status.Online, &lastSeen)
	if err != nil {
		return DeviceStatus{}, false
	}
	if lastSeen.Valid {
		status.LastSeenAt = lastSeen.Time
		if s.ttl > 0 && time.Since(lastSeen.Time) > s.ttl {
			status.Online = false
		}
	}
	return status, true
}

func (s *PostgresStore) GetDeviceStatus(tenantID, deviceID string) (DeviceStatus, bool) {
	return s.getDeviceStatus(tenantID, deviceID)
}

func (s *PostgresStore) createCommand(tenantID, deviceID string, payload json.RawMessage) (Command, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Command{}, err
	}
	defer tx.Rollback()
	if _, ok := s.getDevice(tenantID, deviceID); !ok {
		return Command{}, fmt.Errorf("device not found")
	}
	id := fmt.Sprintf("cmd-%d", time.Now().UTC().UnixNano())
	now := time.Now().UTC()
	cmd := Command{
		ID:        id,
		TenantID:  tenantID,
		DeviceID:  deviceID,
		Status:    contracts.CommandStatusSent,
		Payload:   payload,
		CreatedAt: now,
		UpdatedAt: now,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Command{}, err
	}
	_, err = tx.Exec(`INSERT INTO commands (id, tenant_id, device_id, status, payload, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		cmd.ID, cmd.TenantID, cmd.DeviceID, cmd.Status, payloadBytes, cmd.CreatedAt, cmd.UpdatedAt)
	if err != nil {
		return Command{}, err
	}
	if err := tx.Commit(); err != nil {
		return Command{}, err
	}
	return cmd, nil
}

func (s *PostgresStore) CreateCommand(tenantID, deviceID string, payload json.RawMessage) (Command, error) {
	return s.createCommand(tenantID, deviceID, payload)
}

func (s *PostgresStore) ackCommand(id, tenantID, deviceID string) (Command, error) {
	cmd, exists := s.getCommand(id)
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
	now := time.Now().UTC()
	_, err = s.db.Exec(`UPDATE commands SET status = $1, updated_at = $2 WHERE id = $3`, next, now, id)
	if err != nil {
		return Command{}, err
	}
	cmd.Status = next
	cmd.UpdatedAt = now
	return cmd, nil
}

func (s *PostgresStore) AckCommand(id, tenantID, deviceID string) (Command, error) {
	return s.ackCommand(id, tenantID, deviceID)
}

func (s *PostgresStore) listCommands() []Command {
	rows, err := s.db.Query(`SELECT id, tenant_id, device_id, status, payload, created_at, updated_at FROM commands ORDER BY created_at DESC`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Command
	for rows.Next() {
		var cmd Command
		var payload []byte
		if err := rows.Scan(&cmd.ID, &cmd.TenantID, &cmd.DeviceID, &cmd.Status, &payload, &cmd.CreatedAt, &cmd.UpdatedAt); err == nil {
			cmd.Payload = json.RawMessage(payload)
			out = append(out, cmd)
		}
	}
	return out
}

func (s *PostgresStore) ListCommands() []Command { return s.listCommands() }

func (s *PostgresStore) getCommand(id string) (Command, bool) {
	var cmd Command
	var payload []byte
	err := s.db.QueryRow(`SELECT id, tenant_id, device_id, status, payload, created_at, updated_at FROM commands WHERE id = $1`, id).
		Scan(&cmd.ID, &cmd.TenantID, &cmd.DeviceID, &cmd.Status, &payload, &cmd.CreatedAt, &cmd.UpdatedAt)
	if err != nil {
		return Command{}, false
	}
	cmd.Payload = json.RawMessage(payload)
	return cmd, true
}

func (s *PostgresStore) GetCommand(id string) (Command, bool) { return s.getCommand(id) }

func translateSQLError(err error, kind string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "duplicate key value") {
		return fmt.Errorf("%s already exists", kind)
	}
	return err
}
