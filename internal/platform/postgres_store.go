package platform

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
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
  secret_hash TEXT NOT NULL,
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
  tdengine_written BOOLEAN NOT NULL DEFAULT FALSE,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (msg_id, tenant_id, device_id)
);

CREATE TABLE IF NOT EXISTS commands (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  status TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  next_dispatch_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  dispatch_attempts INTEGER NOT NULL DEFAULT 0,
  deadline_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS command_events (
  id BIGSERIAL PRIMARY KEY,
  command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  detail JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS command_ack (
	id BIGSERIAL PRIMARY KEY,
	command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
	tenant_id TEXT NOT NULL,
	device_id TEXT NOT NULL,
	ack_status TEXT NOT NULL,
	ack_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_devices_tenant ON devices(tenant_id);
CREATE INDEX IF NOT EXISTS idx_commands_tenant_device ON commands(tenant_id, device_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_tenant_device ON telemetry_records(tenant_id, device_id, received_at DESC);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return err
	}
	// Existing local environments used a plaintext secret column. Preserve the
	// device identities while replacing every credential with a bcrypt hash.
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE devices ADD COLUMN IF NOT EXISTS secret_hash TEXT`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE devices ALTER COLUMN secret DROP NOT NULL`); err != nil && !strings.Contains(err.Error(), "column \"secret\" does not exist") {
		return err
	}
	for _, stmt := range []string{
		`ALTER TABLE commands ADD COLUMN IF NOT EXISTS next_dispatch_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`ALTER TABLE commands ADD COLUMN IF NOT EXISTS dispatch_attempts INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE commands ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ`,
		`ALTER TABLE command_ack ADD COLUMN IF NOT EXISTS ack_status TEXT NOT NULL DEFAULT 'acked'`,
		`ALTER TABLE command_ack ADD COLUMN IF NOT EXISTS ack_payload JSONB NOT NULL DEFAULT '{}'::jsonb`,
		`ALTER TABLE command_ack ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`ALTER TABLE telemetry_records ADD COLUMN IF NOT EXISTS tdengine_written BOOLEAN NOT NULL DEFAULT FALSE`,
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_commands_dispatch ON commands(status, next_dispatch_at)`); err != nil {
		return err
	}
	return s.migrateLegacyDeviceSecrets(ctx)
}

func (s *PostgresStore) migrateLegacyDeviceSecrets(ctx context.Context) error {
	var hasLegacy bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'devices' AND column_name = 'secret')`).Scan(&hasLegacy); err != nil || !hasLegacy {
		return err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id, device_id, secret FROM devices WHERE secret_hash IS NULL AND secret IS NOT NULL AND secret <> ''`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tenantID, deviceID, secret string
		if err := rows.Scan(&tenantID, &deviceID, &secret); err != nil {
			return err
		}
		hash, err := HashDeviceSecret(secret)
		if err != nil {
			return err
		}
		if _, err = s.db.ExecContext(ctx, `UPDATE devices SET secret_hash = $1, secret = NULL WHERE tenant_id = $2 AND device_id = $3`, hash, tenantID, deviceID); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *PostgresStore) CreateTenant(t Tenant) (Tenant, error) {
	_, err := s.db.Exec(`INSERT INTO tenants (id, name) VALUES ($1, $2)`, t.ID, t.Name)
	if err != nil {
		return Tenant{}, translateSQLError(err, "tenant")
	}
	return t, nil
}

func (s *PostgresStore) ListTenants() []Tenant {
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

func (s *PostgresStore) ListTenantsPage(page PageRequest) ([]Tenant, string, error) {
	page, err := NormalizePageRequest(page.Size, page.Cursor)
	if err != nil {
		return nil, "", err
	}
	parts, err := decodeCursor(page.Cursor, 1)
	if err != nil {
		return nil, "", err
	}
	after := ""
	if len(parts) == 1 {
		after = parts[0]
	}
	rows, err := s.db.Query(`SELECT id, name FROM tenants WHERE id > $1 ORDER BY id LIMIT $2`, after, page.Size+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []Tenant
	for rows.Next() {
		var item Tenant
		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			return nil, "", err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > page.Size {
		next = encodeCursor(out[page.Size-1].ID)
		out = out[:page.Size]
	}
	return out, next, nil
}

func (s *PostgresStore) ListDevicesPage(page PageRequest) ([]Device, string, error) {
	page, err := NormalizePageRequest(page.Size, page.Cursor)
	if err != nil {
		return nil, "", err
	}
	afterTenant, afterDevice, err := decodeDeviceCursor(page.Cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := s.db.Query(`SELECT tenant_id, device_id, product_id, created_at FROM devices
		WHERE (tenant_id > $1) OR (tenant_id = $1 AND device_id > $2)
		ORDER BY tenant_id, device_id LIMIT $3`, afterTenant, afterDevice, page.Size+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var item Device
		if err := rows.Scan(&item.TenantID, &item.DeviceID, &item.ProductID, &item.CreatedAt); err != nil {
			return nil, "", err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > page.Size {
		next = encodeDeviceCursor(out[page.Size-1].TenantID, out[page.Size-1].DeviceID)
		out = out[:page.Size]
	}
	return out, next, nil
}

func (s *PostgresStore) CreateDevice(d Device) (Device, error) {
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
	hash, err := HashDeviceSecret(d.Secret)
	if err != nil {
		return Device{}, err
	}
	_, err = tx.Exec(`INSERT INTO devices (tenant_id, device_id, product_id, secret_hash, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
		d.TenantID, d.DeviceID, d.ProductID, hash, d.CreatedAt)
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
	d.Secret = ""
	return d, nil
}

func (s *PostgresStore) ListDevices() []Device {
	rows, err := s.db.Query(`SELECT tenant_id, device_id, product_id, created_at FROM devices ORDER BY tenant_id, device_id`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.TenantID, &d.DeviceID, &d.ProductID, &d.CreatedAt); err == nil {
			out = append(out, d)
		}
	}
	return out
}

func (s *PostgresStore) GetDevice(tenantID, deviceID string) (Device, bool) {
	var d Device
	err := s.db.QueryRow(`SELECT tenant_id, device_id, product_id, created_at FROM devices WHERE tenant_id = $1 AND device_id = $2`,
		tenantID, deviceID).Scan(&d.TenantID, &d.DeviceID, &d.ProductID, &d.CreatedAt)
	if err != nil {
		return Device{}, false
	}
	return d, true
}

func (s *PostgresStore) AuthenticateDevice(tenantID, deviceID, secret string) bool {
	var hash string
	if err := s.db.QueryRow(`SELECT secret_hash FROM devices WHERE tenant_id = $1 AND device_id = $2`, tenantID, deviceID).Scan(&hash); err != nil {
		return false
	}
	return VerifyDeviceSecret(hash, secret)
}

func (s *PostgresStore) RecordTelemetry(env contracts.Envelope) (TelemetryRecord, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return TelemetryRecord{}, err
	}
	defer tx.Rollback()
	if _, ok := s.GetDevice(env.TenantID, env.DeviceID); !ok {
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
	res, err := tx.Exec(`INSERT INTO telemetry_records (msg_id, tenant_id, device_id, ts, type, version, payload, received_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (msg_id, tenant_id, device_id) DO NOTHING`,
		rec.MsgID, rec.TenantID, rec.DeviceID, rec.Ts, rec.Type, rec.Version, payloadBytes, rec.ReceivedAt)
	if err != nil {
		return TelemetryRecord{}, err
	}
	inserted, _ := res.RowsAffected()
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
	if inserted == 0 {
		// Duplicate (e.g. DLQ replay). Return the stored row's TDengine completion
		// state so the caller can compensate the TDengine write if it never landed.
		var written bool
		_ = s.db.QueryRow(`SELECT tdengine_written FROM telemetry_records WHERE msg_id = $1 AND tenant_id = $2 AND device_id = $3`,
			rec.MsgID, rec.TenantID, rec.DeviceID).Scan(&written)
		rec.TDengineWritten = written
		return rec, ErrDuplicateTelemetry
	}
	return rec, nil
}

// MarkTelemetryTDengineWritten records that the telemetry row was persisted to
// TDengine, so a later DLQ replay does not write it twice.
func (s *PostgresStore) MarkTelemetryTDengineWritten(msgID, tenantID, deviceID string) error {
	_, err := s.db.Exec(`UPDATE telemetry_records SET tdengine_written = TRUE WHERE msg_id = $1 AND tenant_id = $2 AND device_id = $3`,
		msgID, tenantID, deviceID)
	return err
}

func (s *PostgresStore) ListTelemetry(tenantID, deviceID string) []TelemetryRecord {
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

func (s *PostgresStore) ListTelemetryPage(page PageRequest) ([]TelemetryRecord, string, error) {
	page, err := NormalizePageRequest(page.Size, page.Cursor)
	if err != nil {
		return nil, "", err
	}
	afterAt, afterMsgID, err := decodeTelemetryCursor(page.Cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := s.db.Query(`SELECT msg_id, tenant_id, device_id, ts, type, version, payload, received_at
		FROM telemetry_records
		WHERE tenant_id = $1 AND device_id = $2
		  AND (received_at > $3 OR (received_at = $3 AND msg_id > $4))
		ORDER BY received_at ASC, msg_id ASC LIMIT $5`,
		page.TenantID, page.DeviceID, afterAt, afterMsgID, page.Size+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []TelemetryRecord
	for rows.Next() {
		var rec TelemetryRecord
		var payload []byte
		if err := rows.Scan(&rec.MsgID, &rec.TenantID, &rec.DeviceID, &rec.Ts, &rec.Type, &rec.Version, &payload, &rec.ReceivedAt); err != nil {
			return nil, "", err
		}
		rec.Payload = json.RawMessage(payload)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > page.Size {
		next = encodeTelemetryCursor(out[page.Size-1].ReceivedAt, out[page.Size-1].MsgID)
		out = out[:page.Size]
	}
	return out, next, nil
}

func (s *PostgresStore) GetDeviceStatus(tenantID, deviceID string) (DeviceStatus, bool) {
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

func (s *PostgresStore) CreateCommand(tenantID, deviceID string, payload json.RawMessage) (Command, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Command{}, err
	}
	defer tx.Rollback()
	if _, ok := s.GetDevice(tenantID, deviceID); !ok {
		return Command{}, fmt.Errorf("device not found")
	}
	id := uuid.Must(uuid.NewV7()).String()
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
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return Command{}, err
	}
	_, err = tx.Exec(`INSERT INTO commands (id, tenant_id, device_id, status, payload, next_dispatch_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$6,$7)`,
		cmd.ID, cmd.TenantID, cmd.DeviceID, cmd.Status, payloadBytes, cmd.CreatedAt, cmd.UpdatedAt)
	if err != nil {
		return Command{}, err
	}
	if _, err = tx.Exec(`INSERT INTO command_events (command_id, event_type) VALUES ($1, 'created')`, cmd.ID); err != nil {
		return Command{}, err
	}
	if err := tx.Commit(); err != nil {
		return Command{}, err
	}
	return cmd, nil
}

func (s *PostgresStore) AckCommand(id, tenantID, deviceID string) (Command, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Command{}, err
	}
	defer tx.Rollback()
	var cmd Command
	var payload []byte
	var deadline sql.NullTime
	err = tx.QueryRow(`SELECT id, tenant_id, device_id, status, payload, created_at, updated_at, dispatch_attempts, deadline_at FROM commands WHERE id = $1 FOR UPDATE`, id).Scan(&cmd.ID, &cmd.TenantID, &cmd.DeviceID, &cmd.Status, &payload, &cmd.CreatedAt, &cmd.UpdatedAt, &cmd.DispatchAttempts, &deadline)
	if err == sql.ErrNoRows {
		return Command{}, fmt.Errorf("command not found")
	}
	if err != nil {
		return Command{}, err
	}
	cmd.Payload = json.RawMessage(payload)
	if deadline.Valid {
		cmd.DeadlineAt = deadline.Time
	}
	if cmd.TenantID != tenantID || cmd.DeviceID != deviceID {
		return Command{}, fmt.Errorf("command does not belong to device")
	}
	// ACK delivery is at-least-once. Duplicate or late ACKs are harmless and
	// must not turn a healthy consumer into an error loop.
	if cmd.Status == contracts.CommandStatusAcked || cmd.Status == contracts.CommandStatusTimeout || cmd.Status == contracts.CommandStatusFailed {
		if err := tx.Commit(); err != nil {
			return Command{}, err
		}
		return cmd, nil
	}
	next, err := contracts.AdvanceCommandStatus(cmd.Status, contracts.CommandEventAcked)
	if err != nil {
		return Command{}, err
	}
	now := time.Now().UTC()
	_, err = tx.Exec(`UPDATE commands SET status = $1, updated_at = $2 WHERE id = $3`, next, now, id)
	if err != nil {
		return Command{}, err
	}
	cmd.Status = next
	cmd.UpdatedAt = now
	if _, err = tx.Exec(`INSERT INTO command_ack (command_id, tenant_id, device_id, ack_status, created_at)
		SELECT $1, $2, $3, 'acked', $4 WHERE NOT EXISTS (SELECT 1 FROM command_ack WHERE command_id = $1 AND ack_status = 'acked')`, id, tenantID, deviceID, now); err != nil {
		return Command{}, err
	}
	if _, err = tx.Exec(`INSERT INTO command_events (command_id, event_type) VALUES ($1, 'acked')`, id); err != nil {
		return Command{}, err
	}
	if err = tx.Commit(); err != nil {
		return Command{}, err
	}
	return cmd, nil
}

func (s *PostgresStore) ClaimCommandsForDispatch(limit int, lease time.Duration) ([]Command, error) {
	if limit < 1 {
		return nil, nil
	}
	rows, err := s.db.Query(`WITH claimed AS (
  SELECT id FROM commands WHERE status = 'created' AND next_dispatch_at <= NOW()
  ORDER BY created_at, id FOR UPDATE SKIP LOCKED LIMIT $1
) UPDATE commands c SET dispatch_attempts = c.dispatch_attempts + 1, next_dispatch_at = NOW() + $2::interval, updated_at = NOW()
FROM claimed WHERE c.id = claimed.id
RETURNING c.id, c.tenant_id, c.device_id, c.status, c.payload, c.created_at, c.updated_at, c.dispatch_attempts, c.deadline_at`, limit, lease.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCommands(rows)
}

// MarkCommandPublished transitions a command from created to published once the
// dispatcher wrote it to Kafka. The deadline is NOT started here; it begins
// only when device-worker confirms the MQTT downlink via MarkCommandSent.
func (s *PostgresStore) MarkCommandPublished(id string) error {
	result, err := s.db.Exec(`UPDATE commands SET status = 'published', updated_at = NOW() WHERE id = $1 AND status = 'created'`, id)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 1 {
		_, err = s.db.Exec(`INSERT INTO command_events (command_id, event_type) VALUES ($1, 'published')`, id)
	}
	return err
}

// MarkCommandSent transitions a command from published to sent once the MQTT
// downlink succeeded, and starts the ACK deadline.
func (s *PostgresStore) MarkCommandSent(id string, deadline time.Time) error {
	result, err := s.db.Exec(`UPDATE commands SET status = 'sent', deadline_at = $2, updated_at = NOW() WHERE id = $1 AND status = 'published'`, id, deadline)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 1 {
		_, err = s.db.Exec(`INSERT INTO command_events (command_id, event_type) VALUES ($1, 'delivered')`, id)
	}
	return err
}

func (s *PostgresStore) RescheduleCommand(id string, retryAfter time.Duration) error {
	_, err := s.db.Exec(`UPDATE commands SET next_dispatch_at = NOW() + $2::interval, updated_at = NOW() WHERE id = $1 AND status = 'created'`, id, retryAfter.String())
	return err
}

func (s *PostgresStore) ExpireCommands(now time.Time) (int64, error) {
	rows, err := s.db.Query(`UPDATE commands SET status = 'timeout', updated_at = $1 WHERE status = 'sent' AND deadline_at <= $1 RETURNING id`, now)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var count int64
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		if _, err := s.db.Exec(`INSERT INTO command_events (command_id, event_type) VALUES ($1, 'timeout')`, id); err != nil {
			return 0, err
		}
		count++
	}
	return count, rows.Err()
}

func (s *PostgresStore) ListCommands() []Command {
	rows, err := s.db.Query(`SELECT id, tenant_id, device_id, status, payload, created_at, updated_at, dispatch_attempts, deadline_at FROM commands ORDER BY created_at DESC, id DESC`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Command
	for rows.Next() {
		var cmd Command
		var payload []byte
		var deadline sql.NullTime
		if err := rows.Scan(&cmd.ID, &cmd.TenantID, &cmd.DeviceID, &cmd.Status, &payload, &cmd.CreatedAt, &cmd.UpdatedAt, &cmd.DispatchAttempts, &deadline); err == nil {
			cmd.Payload = json.RawMessage(payload)
			if deadline.Valid {
				cmd.DeadlineAt = deadline.Time
			}
			out = append(out, cmd)
		}
	}
	return out
}

func (s *PostgresStore) ListCommandsPage(page PageRequest) ([]Command, string, error) {
	tenantID := page.TenantID
	page, err := NormalizePageRequest(page.Size, page.Cursor)
	if err != nil {
		return nil, "", err
	}
	page.TenantID = tenantID
	createdAt, id, err := decodeCommandCursor(page.Cursor)
	if err != nil {
		return nil, "", err
	}
	if page.TenantID == "" {
		return nil, "", fmt.Errorf("tenantId is required")
	}
	rows, err := s.db.Query(`SELECT id, tenant_id, device_id, status, payload, created_at, updated_at, dispatch_attempts, deadline_at
FROM commands WHERE tenant_id = $1 AND ($2::timestamptz IS NULL OR created_at < $2 OR (created_at = $2 AND id < $3))
ORDER BY created_at DESC, id DESC LIMIT $4`, page.TenantID, nullTime(createdAt), id, page.Size+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out, err := scanCommands(rows)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > page.Size {
		last := out[page.Size-1]
		next = encodeCommandCursor(last.CreatedAt, last.ID)
		out = out[:page.Size]
	}
	return out, next, nil
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func (s *PostgresStore) GetCommand(id string) (Command, bool) {
	var cmd Command
	var payload []byte
	var deadline sql.NullTime
	err := s.db.QueryRow(`SELECT id, tenant_id, device_id, status, payload, created_at, updated_at, dispatch_attempts, deadline_at FROM commands WHERE id = $1`, id).
		Scan(&cmd.ID, &cmd.TenantID, &cmd.DeviceID, &cmd.Status, &payload, &cmd.CreatedAt, &cmd.UpdatedAt, &cmd.DispatchAttempts, &deadline)
	if err != nil {
		return Command{}, false
	}
	cmd.Payload = json.RawMessage(payload)
	if deadline.Valid {
		cmd.DeadlineAt = deadline.Time
	}
	return cmd, true
}

type commandRows interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func scanCommands(rows commandRows) ([]Command, error) {
	var out []Command
	for rows.Next() {
		var cmd Command
		var payload []byte
		var deadline sql.NullTime
		if err := rows.Scan(&cmd.ID, &cmd.TenantID, &cmd.DeviceID, &cmd.Status, &payload, &cmd.CreatedAt, &cmd.UpdatedAt, &cmd.DispatchAttempts, &deadline); err != nil {
			return nil, err
		}
		cmd.Payload = json.RawMessage(payload)
		if deadline.Valid {
			cmd.DeadlineAt = deadline.Time
		}
		out = append(out, cmd)
	}
	return out, rows.Err()
}

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
