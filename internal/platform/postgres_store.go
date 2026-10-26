package platform

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"

	"iot/internal/contracts"
	"iot/migrations"
)

const defaultPostgresOperationTimeout = 5 * time.Second

func postgresOperationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, defaultPostgresOperationTimeout)
}

type PostgresStore struct {
	db  *sql.DB
	ttl time.Duration
}

// PingContext exposes the underlying connection health check so the /readyz
// probe can fail closed when PostgreSQL is unreachable.
func (s *PostgresStore) PingContext(ctx context.Context) error {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	return s.db.PingContext(ctx)
}

func NewPostgresStore(dsn string, ttl time.Duration) (*PostgresStore, error) {
	return OpenPostgresStore(dsn, ttl, true)
}

// OpenPostgresStore can skip DDL when deployments migrate with a separate role.
func OpenPostgresStore(dsn string, ttl time.Duration, autoMigrate bool) (*PostgresStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &PostgresStore{db: db, ttl: ttl}
	if autoMigrate {
		if err := store.ensureSchema(ctx); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return store, nil
}

func (s *PostgresStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// MigrateSchema applies the repository SQL under a database/schema-scoped lock.
// Multiple replicas cannot race DDL; a failed upgrade rolls back as one unit.
func (s *PostgresStore) MigrateSchema(ctx context.Context) error { return s.ensureSchema(ctx) }

func (s *PostgresStore) ensureSchema(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext(current_database()),hashtext(current_schema() || ':iot-schema'))`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, migrations.UpgradeSQL); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, migrations.InitialSQL); err != nil {
		return err
	}
	if err = migrateLegacyDeviceSecrets(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func migrateLegacyDeviceSecrets(ctx context.Context, tx *sql.Tx) error {
	var legacy bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='devices' AND column_name='secret')`).Scan(&legacy); err != nil || !legacy {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT tenant_id,device_id,secret,COALESCE(secret_hash,'') FROM devices WHERE secret IS NOT NULL`)
	if err != nil {
		return err
	}
	type credential struct{ tenant, device, secret, hash string }
	var credentials []credential
	for rows.Next() {
		var c credential
		if err = rows.Scan(&c.tenant, &c.device, &c.secret, &c.hash); err != nil {
			_ = rows.Close()
			return err
		}
		credentials = append(credentials, c)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}
	for _, c := range credentials {
		hash := c.hash
		if hash == "" {
			hash, err = HashDeviceSecret(c.secret)
			if err != nil {
				return fmt.Errorf("legacy device credential is invalid: %w", err)
			}
		} else if !validBcryptEncoding(hash) {
			return fmt.Errorf("legacy device hash is invalid; migration rolled back")
		}
		if _, err = tx.ExecContext(ctx, `UPDATE devices SET secret_hash=$1,secret=NULL WHERE tenant_id=$2 AND device_id=$3`, hash, c.tenant, c.device); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) CreateTenant(t Tenant) (Tenant, error) {
	return s.CreateTenantContext(context.Background(), t)
}

func (s *PostgresStore) CreateTenantContext(ctx context.Context, t Tenant) (Tenant, error) {
	if err := contracts.ValidateTenantName(t.Name); err != nil {
		return Tenant{}, err
	}
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	if !contracts.IsValidTenantID(t.ID) {
		return Tenant{}, contracts.ErrInvalidTopicPart
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO tenants (id, name) VALUES ($1, $2)`, t.ID, t.Name)
	if err != nil {
		return Tenant{}, translateSQLError(err, "tenant")
	}
	return t, nil
}

func (s *PostgresStore) ListTenants() []Tenant {
	rows, _ := s.ListTenantsContext(context.Background())
	return rows
}

func (s *PostgresStore) ListTenantsContext(ctx context.Context) ([]Tenant, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT id,name FROM tenants ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Tenant
	for rows.Next() {
		var item Tenant

		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			return nil, err
		}

		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PostgresStore) ListTenantsPage(page PageRequest) ([]Tenant, string, error) {
	return s.ListTenantsPageContext(context.Background(), page)
}

func (s *PostgresStore) ListTenantsPageContext(ctx context.Context, page PageRequest) ([]Tenant, string, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
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
	rows, err := s.db.QueryContext(ctx, `SELECT id, name FROM tenants WHERE id > $1 ORDER BY id LIMIT $2`, after, page.Size+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	next := ""
	var budget responsePageBudget
	var out []Tenant
	for rows.Next() {
		var item Tenant
		if err := rows.Scan(&item.ID, &item.Name); err != nil {
			return nil, "", err
		}
		accepted, err := budget.accept(tenantListBytes(item), len(out), page.Size)
		if err != nil {
			return nil, "", err
		}
		if !accepted {
			last := out[len(out)-1]
			next = encodeCursor(last.ID)
			break
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	return out, next, nil
}

func (s *PostgresStore) ListDevicesPage(page PageRequest) ([]Device, string, error) {
	return s.ListDevicesPageContext(context.Background(), page)
}

func (s *PostgresStore) ListDevicesPageContext(ctx context.Context, page PageRequest) ([]Device, string, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	page, err := NormalizeListRequest(page, "devices")
	if err != nil {
		return nil, "", err
	}
	afterTenant, afterDevice, err := decodeDeviceCursor(page.Cursor)
	if err != nil {
		return nil, "", err
	}
	if afterTenant != "" && afterTenant != page.TenantID {
		return nil, "", errInvalidCursor
	}
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id,device_id,product_id,created_at FROM devices
 WHERE tenant_id=$1 AND device_id>$2 ORDER BY device_id ASC LIMIT $3`, page.TenantID, afterDevice, page.Size+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	next := ""
	var budget responsePageBudget
	var out []Device
	for rows.Next() {
		var item Device
		if err := rows.Scan(&item.TenantID, &item.DeviceID, &item.ProductID, &item.CreatedAt); err != nil {
			return nil, "", err
		}
		accepted, err := budget.accept(deviceListBytes(item), len(out), page.Size)
		if err != nil {
			return nil, "", err
		}
		if !accepted {
			last := out[len(out)-1]
			next = encodeDeviceCursor(last.TenantID, last.DeviceID)
			break
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	return out, next, nil
}

func (s *PostgresStore) CreateDevice(d Device) (Device, error) {
	return s.CreateDeviceContext(context.Background(), d)
}

func (s *PostgresStore) CreateDeviceContext(ctx context.Context, d Device) (Device, error) {
	if err := contracts.ValidateProductID(d.ProductID); err != nil {
		return Device{}, err
	}
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	if !contracts.IsValidTenantID(d.TenantID) || !contracts.IsValidTopicPart(d.DeviceID) {
		return Device{}, contracts.ErrInvalidTopicPart
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Device{}, err
	}
	defer tx.Rollback()
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM tenants WHERE id = $1)`, d.TenantID).Scan(&exists); err != nil {
		return Device{}, err
	}
	if !exists {
		return Device{}, fmt.Errorf("tenant %w", ErrNotFound)
	}
	d.CreatedAt = time.Now().UTC()
	hash, err := HashDeviceSecret(d.Secret)
	if err != nil {
		return Device{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO devices (tenant_id, device_id, product_id, secret_hash, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
		d.TenantID, d.DeviceID, d.ProductID, hash, d.CreatedAt)
	if err != nil {
		return Device{}, translateSQLError(err, "device")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO device_state (tenant_id, device_id, connected, updated_at) VALUES ($1, $2, false, $3)
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
	rows, _ := s.ListDevicesContext(context.Background())
	return rows
}

func (s *PostgresStore) ListDevicesContext(ctx context.Context) ([]Device, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id,device_id,product_id,created_at FROM devices ORDER BY tenant_id,device_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var item Device

		if err := rows.Scan(&item.TenantID, &item.DeviceID, &item.ProductID, &item.CreatedAt); err != nil {
			return nil, err
		}

		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PostgresStore) GetDevice(tenantID, deviceID string) (Device, bool) {
	value, err := s.GetDeviceContext(context.Background(), tenantID, deviceID)
	return value, err == nil
}

func (s *PostgresStore) GetDeviceContext(ctx context.Context, tenantID, deviceID string) (Device, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	var d Device
	err := s.db.QueryRowContext(ctx, `SELECT tenant_id, device_id, product_id, created_at FROM devices WHERE tenant_id = $1 AND device_id = $2`,
		tenantID, deviceID).Scan(&d.TenantID, &d.DeviceID, &d.ProductID, &d.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return Device{}, ErrNotFound
		}
		return Device{}, err
	}
	return d, nil
}

func (s *PostgresStore) AuthenticateDevice(tenantID, deviceID, secret string) bool {
	return s.AuthenticateDeviceContext(context.Background(), tenantID, deviceID, secret)
}

func (s *PostgresStore) AuthenticateDeviceContext(ctx context.Context, tenantID, deviceID, secret string) bool {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	var hash string
	if err := s.db.QueryRowContext(ctx, `SELECT secret_hash FROM devices WHERE tenant_id = $1 AND device_id = $2`, tenantID, deviceID).Scan(&hash); err != nil {
		return false
	}
	return VerifyDeviceSecret(hash, secret)
}

func (s *PostgresStore) RecordTelemetry(env contracts.Envelope) (TelemetryRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.RecordTelemetryContext(ctx, env)
}

func (s *PostgresStore) RecordTelemetryContext(ctx context.Context, env contracts.Envelope) (TelemetryRecord, error) {
	return s.RestoreTelemetryContext(ctx, TelemetryRecord{
		MsgID: env.MsgID, TenantID: env.TenantID, DeviceID: env.DeviceID, Ts: env.Ts,
		Type: env.Type, Version: env.Version, Payload: env.Payload, ReceivedAt: time.Now().UTC(),
	})
}

// RestoreTelemetryContext preserves the server receive time from a trusted
// Kafka record. Public ingress stamps its own time via RecordTelemetryContext.
func (s *PostgresStore) RestoreTelemetryContext(ctx context.Context, rec TelemetryRecord) (TelemetryRecord, error) {
	if err := contracts.ValidateTelemetryPayload(rec.Payload); err != nil {
		return TelemetryRecord{}, err
	}
	if rec.ReceivedAt.IsZero() {
		return TelemetryRecord{}, fmt.Errorf("%w: original receive time is required", contracts.ErrInvalidEnvelope)
	}
	rec.TDengineWritten = false
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return TelemetryRecord{}, err
	}
	defer tx.Rollback()
	if err := requireDevice(ctx, tx, rec.TenantID, rec.DeviceID); err != nil {
		return TelemetryRecord{}, err
	}
	// Recheck the actual normalized bytes as a defense against differences in
	// PostgreSQL representation; the preflight estimate already bounds DB work.
	var normalized []byte
	if err := tx.QueryRowContext(ctx, `SELECT $1::jsonb`, []byte(rec.Payload)).Scan(&normalized); err != nil {
		return TelemetryRecord{}, err
	}
	if err := contracts.ValidateTelemetryPayload(normalized); err != nil {
		return TelemetryRecord{}, err
	}
	payloadBytes, err := json.Marshal(rec.Payload)
	if err != nil {
		return TelemetryRecord{}, err
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `INSERT INTO telemetry_records (msg_id, tenant_id, device_id, ts, type, version, payload, received_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (msg_id, tenant_id, device_id) DO NOTHING`,
		rec.MsgID, rec.TenantID, rec.DeviceID, rec.Ts, rec.Type, rec.Version, payloadBytes, rec.ReceivedAt)
	if err != nil {
		return TelemetryRecord{}, err
	}
	inserted, _ := res.RowsAffected()
	if inserted == 0 {
		// Duplicate (e.g. DLQ replay). Do NOT touch device_state here: a
		// replayed message must not mark the device online or refresh
		// last_seen_at, otherwise a DLQ replay lies about liveness. Return the
		// stored row so the caller can compensate the TDengine write if it
		// never landed.
		var stored TelemetryRecord
		var payload []byte
		if err := tx.QueryRowContext(ctx, `SELECT msg_id, tenant_id, device_id, ts, type, version, payload, received_at, tdengine_written
			FROM telemetry_records WHERE msg_id = $1 AND tenant_id = $2 AND device_id = $3`,
			rec.MsgID, rec.TenantID, rec.DeviceID).
			Scan(&stored.MsgID, &stored.TenantID, &stored.DeviceID, &stored.Ts, &stored.Type, &stored.Version, &payload, &stored.ReceivedAt, &stored.TDengineWritten); err != nil {
			return TelemetryRecord{}, err
		}
		stored.Payload = json.RawMessage(payload)
		return stored, ErrDuplicateTelemetry
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO device_state (tenant_id, device_id, connected, last_seen_at, last_msg_id, updated_at)
		VALUES ($1,$2,true,$3,$4,$5)
		ON CONFLICT (tenant_id, device_id) DO UPDATE SET connected = true,
		last_seen_at = EXCLUDED.last_seen_at, last_msg_id = EXCLUDED.last_msg_id,
		updated_at = GREATEST(device_state.updated_at, EXCLUDED.updated_at)
		WHERE device_state.last_seen_at IS NULL OR device_state.last_seen_at < EXCLUDED.last_seen_at`,
		rec.TenantID, rec.DeviceID, rec.ReceivedAt, rec.MsgID, now)
	if err != nil {
		return TelemetryRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return TelemetryRecord{}, err
	}
	return rec, nil
}

// MarkTelemetryTDengineWritten records that the telemetry row was persisted to
// TDengine, so a later DLQ replay does not write it twice.
func (s *PostgresStore) MarkTelemetryTDengineWritten(msgID, tenantID, deviceID string) error {
	return s.MarkTelemetryTDengineWrittenContext(context.Background(), msgID, tenantID, deviceID)
}

func (s *PostgresStore) MarkTelemetryTDengineWrittenContext(ctx context.Context, msgID, tenantID, deviceID string) error {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	_, err := s.db.ExecContext(ctx, `UPDATE telemetry_records SET tdengine_written = TRUE WHERE msg_id = $1 AND tenant_id = $2 AND device_id = $3`,
		msgID, tenantID, deviceID)
	return err
}

// ListUnwrittenTelemetry returns rows whose TDengine write never completed
// (tdengine_written = FALSE), oldest first, for the device-worker's background
// compensation scan. olderThan excludes freshly ingested rows so the scan does
// not race the consumer that is about to write them.
func (s *PostgresStore) ListUnwrittenTelemetry(ctx context.Context, olderThan time.Time, afterID int64, tenantIDs []string, limit int) ([]TelemetryRecord, int64, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	if limit < 1 {
		return nil, afterID, nil
	}
	query := `SELECT id,msg_id,tenant_id,device_id,ts,type,version,payload,received_at FROM telemetry_records WHERE tdengine_written=FALSE AND received_at <= $1 AND id>$2`
	args := []any{olderThan, afterID, limit}
	if len(tenantIDs) > 0 {
		query += ` AND tenant_id = ANY($4)`
		args = append(args, tenantIDs)
	}
	query += ` ORDER BY id LIMIT $3`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, afterID, err
	}
	defer rows.Close()
	var out []TelemetryRecord
	cursor := afterID
	for rows.Next() {
		var rec TelemetryRecord
		var payload []byte
		if err := rows.Scan(&cursor, &rec.MsgID, &rec.TenantID, &rec.DeviceID, &rec.Ts, &rec.Type, &rec.Version, &payload, &rec.ReceivedAt); err != nil {
			return nil, afterID, err
		}
		rec.Payload = json.RawMessage(payload)
		out = append(out, rec)
	}
	return out, cursor, rows.Err()
}

func (s *PostgresStore) ListTelemetry(tenantID, deviceID string) []TelemetryRecord {
	rows, _ := s.ListTelemetryContext(context.Background(), tenantID, deviceID)
	return rows
}

func (s *PostgresStore) ListTelemetryContext(ctx context.Context, tenantID, deviceID string) ([]TelemetryRecord, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT msg_id,tenant_id,device_id,ts,type,version,payload,received_at FROM telemetry_records WHERE tenant_id=$1 AND device_id=$2 ORDER BY received_at ASC`, tenantID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TelemetryRecord
	for rows.Next() {
		var item TelemetryRecord
		var payload []byte
		if err := rows.Scan(&item.MsgID, &item.TenantID, &item.DeviceID, &item.Ts, &item.Type, &item.Version, &payload, &item.ReceivedAt); err != nil {
			return nil, err
		}
		item.Payload = json.RawMessage(payload)
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PostgresStore) ListTelemetryPage(page PageRequest) ([]TelemetryRecord, string, error) {
	return s.ListTelemetryPageContext(context.Background(), page)
}

func (s *PostgresStore) ListTelemetryPageContext(ctx context.Context, page PageRequest) ([]TelemetryRecord, string, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	page, err := NormalizeListRequest(page, "telemetry")
	if err != nil {
		return nil, "", err
	}
	afterAt, afterID, err := decodePageCursor(page, "telemetry")
	if err != nil {
		return nil, "", err
	}
	direction, operator := "DESC", "<"
	if page.Sort == "asc" {
		direction, operator = "ASC", ">"
	}
	var afterMillis any
	if page.Cursor != "" {
		afterMillis = afterAt.UnixMilli()
	}
	var startMillis, endMillis any
	if page.HasStartTime {
		startMillis = page.StartTime.UnixMilli()
	}
	if page.HasEndTime {
		endMillis = page.EndTime.UnixMilli()
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT msg_id,tenant_id,device_id,ts,type,version,payload,received_at
 FROM telemetry_records WHERE tenant_id=$1 AND device_id=$2
 AND ($3::bigint IS NULL OR ts >= $3) AND ($4::bigint IS NULL OR ts < $4)
 AND ($5='' OR type=$5) AND ($6::bigint IS NULL OR (ts,msg_id) %s ($6,$7))
 ORDER BY ts %s,msg_id %s LIMIT $8`, operator, direction, direction), page.TenantID, page.DeviceID, startMillis, endMillis, page.Type, afterMillis, afterID, page.Size+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	next := ""
	var budget responsePageBudget
	var out []TelemetryRecord
	for rows.Next() {
		var rec TelemetryRecord
		var payload []byte
		if err := rows.Scan(&rec.MsgID, &rec.TenantID, &rec.DeviceID, &rec.Ts, &rec.Type, &rec.Version, &payload, &rec.ReceivedAt); err != nil {
			return nil, "", err
		}
		rec.Payload = json.RawMessage(payload)
		accepted, err := budget.accept(telemetryListBytes(rec), len(out), page.Size)
		if err != nil {
			return nil, "", err
		}
		if !accepted {
			last := out[len(out)-1]
			next = encodePageCursor(page, "telemetry", time.UnixMilli(last.Ts), last.MsgID)
			break
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	return out, next, nil
}

func (s *PostgresStore) GetDeviceStatus(tenantID, deviceID string) (DeviceStatus, bool) {
	value, err := s.GetDeviceStatusContext(context.Background(), tenantID, deviceID)
	return value, err == nil
}

func (s *PostgresStore) GetDeviceStatusContext(ctx context.Context, tenantID, deviceID string) (DeviceStatus, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	var status DeviceStatus
	var lastSeen sql.NullTime
	err := s.db.QueryRowContext(ctx, `SELECT tenant_id, device_id, connected, last_seen_at FROM device_state WHERE tenant_id = $1 AND device_id = $2`,
		tenantID, deviceID).Scan(&status.TenantID, &status.DeviceID, &status.Online, &lastSeen)
	if err != nil {
		if err == sql.ErrNoRows {
			return DeviceStatus{}, ErrNotFound
		}
		return DeviceStatus{}, err
	}
	if lastSeen.Valid {
		status.LastSeenAt = lastSeen.Time
		if s.ttl > 0 && time.Since(lastSeen.Time) > s.ttl {
			status.Online = false
		}
	}
	return status, nil
}

// CreateCommand preserves the unkeyed API for internal callers.
func (s *PostgresStore) CreateCommand(tenantID, deviceID string, payload json.RawMessage) (Command, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.CreateCommandWithKey(ctx, tenantID, deviceID, "", payload)
}

func requireDevice(ctx context.Context, tx *sql.Tx, tenantID, deviceID string) error {
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM devices WHERE tenant_id=$1 AND device_id=$2)`, tenantID, deviceID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("device %w", ErrNotFound)
	}
	return nil
}

// CreateCommandWithKey atomically reuses a tenant-scoped request key. A changed
// device or payload conflicts instead of executing a second device action.
func (s *PostgresStore) CreateCommandWithKey(ctx context.Context, tenantID, deviceID, key string, payload json.RawMessage) (Command, error) {
	if err := ValidateCommandInput(tenantID, deviceID, payload); err != nil {
		return Command{}, err
	}
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	if err := contracts.ValidateIdempotencyKey(key); err != nil {
		return Command{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Command{}, err
	}
	defer tx.Rollback()
	if err := requireDevice(ctx, tx, tenantID, deviceID); err != nil {
		return Command{}, err
	}
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	// PostgreSQL expands numeric exponents in JSONB. Check that exact stored
	// representation before accepting a command the dispatcher cannot deliver.
	var normalized []byte
	if err := tx.QueryRowContext(ctx, `SELECT $1::jsonb`, []byte(payload)).Scan(&normalized); err != nil {
		return Command{}, err
	}
	if err := ValidateCommandInput(tenantID, deviceID, normalized); err != nil {
		return Command{}, err
	}
	payload = normalized
	id, err := uuid.NewV7()
	if err != nil {
		return Command{}, err
	}
	now := time.Now().UTC()
	cmd := Command{ID: id.String(), TenantID: tenantID, DeviceID: deviceID, Status: contracts.CommandStatusCreated, Payload: payload, CreatedAt: now, UpdatedAt: now}
	var storedID string
	err = tx.QueryRowContext(ctx, `INSERT INTO commands (id,tenant_id,device_id,status,payload,next_dispatch_at,created_at,updated_at,idempotency_key)
 VALUES ($1,$2,$3,$4,$5,$6,$6,$6,NULLIF($7,''))
 ON CONFLICT (tenant_id,idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING id`, cmd.ID, tenantID, deviceID, cmd.Status, []byte(payload), now, key).Scan(&storedID)
	if err == sql.ErrNoRows {
		var raw []byte
		var deadline sql.NullTime
		var matches bool
		var result []byte
		err = tx.QueryRowContext(ctx, `SELECT id,tenant_id,device_id,status,payload,created_at,updated_at,dispatch_attempts,deadline_at,result,(device_id=$3 AND payload=$4::jsonb)
  FROM commands WHERE tenant_id=$1 AND idempotency_key=$2`, tenantID, key, deviceID, []byte(payload)).Scan(&cmd.ID, &cmd.TenantID, &cmd.DeviceID, &cmd.Status, &raw, &cmd.CreatedAt, &cmd.UpdatedAt, &cmd.DispatchAttempts, &deadline, &result, &matches)
		if err != nil {
			return Command{}, err
		}
		if !matches {
			return Command{}, ErrIdempotencyConflict
		}
		cmd.Payload = json.RawMessage(raw)
		if len(result) > 0 {
			if err = json.Unmarshal(result, &cmd.Result); err != nil {
				return Command{}, err
			}
		}
		if deadline.Valid {
			cmd.DeadlineAt = deadline.Time
		}
	} else if err != nil {
		return Command{}, err
	} else if _, err = tx.ExecContext(ctx, `INSERT INTO command_events (command_id,event_type) VALUES ($1,'created')`, cmd.ID); err != nil {
		return Command{}, err
	}
	if err = tx.Commit(); err != nil {
		return Command{}, err
	}
	return cmd, nil
}

func (s *PostgresStore) AckCommand(id, tenantID, deviceID string) (Command, error) {
	return s.AckCommandContext(context.Background(), id, tenantID, deviceID)
}

func (s *PostgresStore) AckCommandContext(ctx context.Context, id, tenantID, deviceID string) (Command, error) {
	return s.AckCommandResultContext(ctx, id, tenantID, deviceID, contracts.CommandResult{Status: contracts.CommandStatusAcked})
}

func (s *PostgresStore) AckCommandResultContext(ctx context.Context, id, tenantID, deviceID string, input contracts.CommandResult) (Command, error) {
	if err := contracts.ValidateDeviceIdentity(tenantID, deviceID); err != nil {
		return Command{}, err
	}
	if err := contracts.ValidateCommandID(id); err != nil {
		return Command{}, err
	}
	normalized, err := contracts.NormalizeCommandResult(input)
	if err != nil {
		return Command{}, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return Command{}, err
	}
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Command{}, err
	}
	defer tx.Rollback()
	var cmd Command
	var payload []byte
	var deadline sql.NullTime
	var result []byte
	err = tx.QueryRowContext(ctx, `SELECT id, tenant_id, device_id, status, payload, created_at, updated_at, dispatch_attempts, deadline_at, result FROM commands WHERE id = $1 FOR UPDATE`, id).Scan(&cmd.ID, &cmd.TenantID, &cmd.DeviceID, &cmd.Status, &payload, &cmd.CreatedAt, &cmd.UpdatedAt, &cmd.DispatchAttempts, &deadline, &result)
	if err == sql.ErrNoRows {
		return Command{}, fmt.Errorf("command %w", ErrNotFound)
	}
	if err != nil {
		return Command{}, err
	}
	cmd.Payload = json.RawMessage(payload)
	if len(result) > 0 {
		if err := json.Unmarshal(result, &cmd.Result); err != nil {
			return Command{}, err
		}
	}
	if deadline.Valid {
		cmd.DeadlineAt = deadline.Time
	}
	if cmd.TenantID != tenantID || cmd.DeviceID != deviceID {
		// Deliberately indistinguishable from a command that does not exist:
		// confirming that the ID belongs to another tenant or device would leak
		// that tenant's data to the caller.
		return Command{}, fmt.Errorf("command %w", ErrNotFound)
	}
	// ACK delivery is at-least-once. Duplicate or late ACKs are harmless and
	// must not turn a healthy consumer into an error loop.
	if cmd.Status == contracts.CommandStatusAcked || cmd.Status == contracts.CommandStatusTimeout || cmd.Status == contracts.CommandStatusFailed {
		if cmd.Result != nil {
			var equal bool
			if err = tx.QueryRowContext(ctx, `SELECT result=$2::jsonb FROM commands WHERE id=$1`, id, encoded).Scan(&equal); err != nil {
				return Command{}, err
			}
			if !equal {
				return Command{}, ErrAcknowledgementConflict
			}
		}
		if err := tx.Commit(); err != nil {
			return Command{}, err
		}
		return cmd, nil
	}
	next, err := contracts.AdvanceCommandStatus(cmd.Status, contracts.CommandEvent(normalized.Status))
	if err != nil {
		return Command{}, err
	}
	now := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `UPDATE commands SET status=$1,updated_at=$2,result=$4::jsonb WHERE id=$3`, next, now, id, encoded)
	if err != nil {
		return Command{}, err
	}
	cmd.Status = next
	cmd.UpdatedAt = now
	cmd.Result = &normalized
	if _, err = tx.ExecContext(ctx, `INSERT INTO command_ack(command_id,tenant_id,device_id,ack_status,ack_payload,created_at)
 VALUES($1,$2,$3,$4,$5::jsonb,$6)`, id, tenantID, deviceID, normalized.Status, encoded, now); err != nil {
		return Command{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO command_events(command_id,event_type,detail) VALUES($1,$2,$3::jsonb)`, id, normalized.Status, encoded); err != nil {
		return Command{}, err
	}
	if err = tx.Commit(); err != nil {
		return Command{}, err
	}
	return cmd, nil
}

func (s *PostgresStore) ClaimCommandsForDispatch(limit int, lease time.Duration) ([]Command, error) {
	return s.ClaimCommandsForDispatchContext(context.Background(), limit, lease)
}

func (s *PostgresStore) ClaimCommandsForDispatchContext(ctx context.Context, limit int, lease time.Duration) ([]Command, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	if limit < 1 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `WITH claimed AS (
  SELECT id FROM commands WHERE status = 'created' AND next_dispatch_at <= NOW() AND dispatch_attempts < $3
  ORDER BY created_at, id FOR UPDATE SKIP LOCKED LIMIT $1
) UPDATE commands c SET dispatch_attempts = c.dispatch_attempts + 1, next_dispatch_at = NOW() + $2::interval, updated_at = NOW()
FROM claimed WHERE c.id = claimed.id
RETURNING c.id, c.tenant_id, c.device_id, c.status, c.payload, c.created_at, c.updated_at, c.dispatch_attempts, c.deadline_at, c.result`, limit, lease.String(), maxDispatchAttempts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCommands(rows)
}

// MarkCommandPublished transitions a command from created to published once the
// dispatcher wrote it to Kafka. The deadline is NOT started here; it begins
// only when device-worker confirms the MQTT downlink via MarkCommandSent.
// The claimed attempt and lease fence a delayed completion from an older owner.
func (s *PostgresStore) MarkCommandPublished(id string, attempt int) error {
	return s.MarkCommandPublishedContext(context.Background(), id, attempt)
}

func (s *PostgresStore) MarkCommandPublishedContext(ctx context.Context, id string, attempt int) error {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	_, err := s.updateCommandsWithEvent(ctx, `UPDATE commands SET status = 'published', updated_at = NOW()
 WHERE id = $1 AND status = 'created' AND dispatch_attempts = $2 AND next_dispatch_at > NOW() RETURNING id`, "published", id, attempt)
	return err
}

// MarkCommandSent transitions a command to sent once the MQTT downlink
// succeeded, and starts the ACK deadline. It accepts both 'created' and
// 'published' as the prior state: the worker can confirm the MQTT downlink
// before the dispatcher's MarkCommandPublished commit lands (dispatcher writes
// Kafka first, then updates the row), so requiring strictly 'published' would
// race and leave the command stuck in published with no deadline.
func (s *PostgresStore) MarkCommandSent(id string, deadline time.Time) error {
	return s.MarkCommandSentContext(context.Background(), id, deadline)
}

func (s *PostgresStore) MarkCommandSentContext(ctx context.Context, id string, deadline time.Time) error {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	_, err := s.updateCommandsWithEvent(ctx, `UPDATE commands SET status = 'sent', deadline_at = $2, updated_at = NOW()
 WHERE id = $1 AND status IN ('created', 'published') RETURNING id`, "delivered", id, deadline)
	return err
}

func (s *PostgresStore) RescheduleCommand(id string, attempt int, retryAfter time.Duration) error {
	return s.RescheduleCommandContext(context.Background(), id, attempt, retryAfter)
}

func (s *PostgresStore) RescheduleCommandContext(ctx context.Context, id string, attempt int, retryAfter time.Duration) error {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	_, err := s.db.ExecContext(ctx, `UPDATE commands SET next_dispatch_at = NOW() + $3::interval, updated_at = NOW()
 WHERE id = $1 AND status = 'created' AND dispatch_attempts = $2 AND next_dispatch_at > NOW()`, id, attempt, retryAfter.String())
	return err
}

// RequeueCommand returns a 'published' command to 'created' so the dispatcher
// redelivers it on the next poll instead of waiting up to 5 minutes for the
// stale-command recovery scan. The worker calls this when the MQTT downlink
// fails. It only matches 'published': if the dispatcher's MarkCommandPublished
// has not landed yet (it writes Kafka first, then updates the row), the row is
// still 'created' and will be re-claimed on its own — matching that state here
// would only open an overwrite race with MarkCommandPublished. Attempts stay
// bounded: each redelivery goes through ClaimCommandsForDispatch, which
// increments dispatch_attempts, and RecoverStaleCommands marks the command
// failed once the cap is hit.
func (s *PostgresStore) RequeueCommand(id string, retryAfter time.Duration) error {
	return s.RequeueCommandContext(context.Background(), id, retryAfter)
}

func (s *PostgresStore) RequeueCommandContext(ctx context.Context, id string, retryAfter time.Duration) error {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	_, err := s.updateCommandsWithEvent(ctx,
		`UPDATE commands SET status = 'created', next_dispatch_at = NOW() + $2::interval, updated_at = NOW()
		 WHERE id = $1 AND status = 'published' RETURNING id`,
		"requeued", id, retryAfter.String())
	return err
}

// ExpireCommands marks 'sent' commands whose ACK deadline passed as timeout.
// The NULL-deadline branch is a defensive backstop: MarkCommandSent always
// writes deadline_at, but rows written by older versions (or by a manual
// status flip) can sit in 'sent' with no deadline and would otherwise never
// converge. nullDeadlineGrace <= 0 disables that backstop.
func (s *PostgresStore) ExpireCommands(now time.Time, nullDeadlineGrace time.Duration) (int64, error) {
	return s.ExpireCommandsContext(context.Background(), now, nullDeadlineGrace)
}

func (s *PostgresStore) ExpireCommandsContext(ctx context.Context, now time.Time, nullDeadlineGrace time.Duration) (int64, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	predicate := `status='sent' AND deadline_at <= $1`
	args := []any{now}
	if nullDeadlineGrace > 0 {
		predicate = `status='sent' AND (deadline_at <= $1 OR (deadline_at IS NULL AND updated_at + $2::interval <= $1))`
		args = append(args, nullDeadlineGrace.String())
	}
	return s.updateCommandBatchWithEvent(ctx, `status='timeout',updated_at=$1`, predicate, "timeout", args...)
}

// RecoverStaleCommands repairs commands stranded by dispatcher/worker outages:
//   - 'published' commands whose Kafka event was never confirmed via
//     MarkCommandSent (worker downtime, skipped offsets) are requeued to
//     'created' for redelivery while attempts remain, or marked 'failed' once
//     dispatch attempts are exhausted.
//   - 'created' commands that exhausted dispatch attempts (persistent Kafka
//     publish failures) are marked 'failed' after the active lease/backoff ends.
//
// Redelivery is safe: MarkCommandSent and AckCommand state guards keep
// duplicate MQTT downlinks and ACKs idempotent.
func (s *PostgresStore) RecoverStaleCommands(staleBefore time.Time, maxAttempts int) (int64, int64, error) {
	return s.RecoverStaleCommandsContext(context.Background(), staleBefore, maxAttempts)
}

func (s *PostgresStore) RecoverStaleCommandsContext(ctx context.Context, staleBefore time.Time, maxAttempts int) (int64, int64, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	requeued, err := s.updateCommandBatchWithEvent(ctx, `status='created',next_dispatch_at=NOW(),updated_at=NOW()`,
		`status='published' AND updated_at <= $1 AND dispatch_attempts < $2`, "requeued", staleBefore, maxAttempts)
	if err != nil {
		return 0, 0, err
	}
	failed, err := s.updateCommandBatchWithEvent(ctx, `status='failed',updated_at=NOW()`,
		`dispatch_attempts >= $2 AND ((status='created' AND next_dispatch_at <= NOW()) OR (status='published' AND updated_at <= $1))`, "failed", staleBefore, maxAttempts)
	return requeued, failed, err
}

// Skip locked commands and bound each sweep. State and audit remain one atomic
// statement, so a slow ACK cannot hold up dispatch of an unrelated command.
func (s *PostgresStore) updateCommandBatchWithEvent(ctx context.Context, assignment, predicate, event string, args ...any) (int64, error) {
	return s.updateCommandsWithEvent(ctx, `UPDATE commands SET `+assignment+` WHERE id IN (
 SELECT id FROM commands WHERE `+predicate+` ORDER BY created_at,id LIMIT 500 FOR UPDATE SKIP LOCKED
 ) RETURNING id`, event, args...)
}

func (s *PostgresStore) updateCommandsWithEvent(ctx context.Context, query, event string, args ...any) (int64, error) {
	// One statement makes both writes atomic and uses just one pool connection.
	args = append(args, event)
	result, err := s.db.ExecContext(ctx, `WITH changed AS (`+query+`)
 INSERT INTO command_events (command_id, event_type) SELECT id, $`+strconv.Itoa(len(args))+` FROM changed`, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *PostgresStore) ListCommands() []Command {
	rows, _ := s.ListCommandsContext(context.Background())
	return rows
}

func (s *PostgresStore) ListCommandsContext(ctx context.Context) ([]Command, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT id,tenant_id,device_id,status,payload,created_at,updated_at,dispatch_attempts,deadline_at,result FROM commands ORDER BY created_at DESC,id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCommands(rows)
}

func (s *PostgresStore) ListCommandsPage(page PageRequest) ([]Command, string, error) {
	return s.ListCommandsPageContext(context.Background(), page)
}

func (s *PostgresStore) ListCommandsPageContext(ctx context.Context, page PageRequest) ([]Command, string, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	page, err := NormalizeListRequest(page, "commands")
	if err != nil {
		return nil, "", err
	}
	createdAt, id, err := decodePageCursor(page, "commands")
	if err != nil {
		return nil, "", err
	}
	direction, operator := "DESC", "<"
	if page.Sort == "asc" {
		direction, operator = "ASC", ">"
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT id,tenant_id,device_id,status,payload,created_at,updated_at,dispatch_attempts,deadline_at,result
 FROM commands WHERE tenant_id=$1 AND ($2::timestamptz IS NULL OR (created_at,id) %s ($2,$3))
 AND ($4='' OR device_id=$4) AND ($5='' OR status=$5)
 AND ($6::timestamptz IS NULL OR created_at >= $6) AND ($7::timestamptz IS NULL OR created_at < $7)
 ORDER BY created_at %s,id %s LIMIT $8`, operator, direction, direction), page.TenantID, nullTime(createdAt), id, page.DeviceID, page.Status, listBoundTime(page.StartTime, page.HasStartTime), listBoundTime(page.EndTime, page.HasEndTime), page.Size+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []Command
	next := ""
	var budget responsePageBudget
	for rows.Next() {
		cmd, err := scanCommandRow(rows)
		if err != nil {
			return nil, "", err
		}
		accepted, err := budget.accept(commandListBytes(cmd), len(out), page.Size)
		if err != nil {
			return nil, "", err
		}
		if !accepted {
			last := out[len(out)-1]
			next = encodePageCursor(page, "commands", last.CreatedAt, last.ID)
			break
		}
		out = append(out, cmd)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	return out, next, nil
}

func listBoundTime(stamp time.Time, present bool) any {
	if !present {
		return nil
	}
	return stamp
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func (s *PostgresStore) GetCommand(id string) (Command, bool) {
	value, err := s.GetCommandContext(context.Background(), id)
	return value, err == nil
}

type commandRows interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func scanCommands(rows commandRows) ([]Command, error) {
	var out []Command
	for rows.Next() {
		cmd, err := scanCommandRow(rows)
		if err != nil {
			return nil, err
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
		return fmt.Errorf("%s %w", kind, ErrAlreadyExists)
	}
	return err
}

// GetCommandForDelivery fails closed on database errors before an MQTT publish.
func (s *PostgresStore) GetCommandForDelivery(ctx context.Context, id string) (Command, error) {
	return s.GetCommandContext(ctx, id)
}

func (s *PostgresStore) GetCommandContext(ctx context.Context, id string) (Command, error) {
	ctx, cancel := postgresOperationContext(ctx)
	defer cancel()
	var cmd Command
	var payload []byte
	var deadline sql.NullTime
	var result []byte
	err := s.db.QueryRowContext(ctx, `SELECT id,tenant_id,device_id,status,payload,created_at,updated_at,dispatch_attempts,deadline_at,result FROM commands WHERE id=$1`, id).Scan(&cmd.ID, &cmd.TenantID, &cmd.DeviceID, &cmd.Status, &payload, &cmd.CreatedAt, &cmd.UpdatedAt, &cmd.DispatchAttempts, &deadline, &result)
	if err == sql.ErrNoRows {
		return Command{}, ErrNotFound
	}
	if err != nil {
		return Command{}, err
	}
	cmd.Payload = json.RawMessage(payload)
	if len(result) > 0 {
		if err := json.Unmarshal(result, &cmd.Result); err != nil {
			return Command{}, err
		}
	}
	if deadline.Valid {
		cmd.DeadlineAt = deadline.Time
	}
	return cmd, nil
}

func scanCommandRow(rows interface{ Scan(...any) error }) (Command, error) {
	var cmd Command
	var payload []byte
	var deadline sql.NullTime
	var result []byte
	if err := rows.Scan(&cmd.ID, &cmd.TenantID, &cmd.DeviceID, &cmd.Status, &payload, &cmd.CreatedAt, &cmd.UpdatedAt, &cmd.DispatchAttempts, &deadline, &result); err != nil {
		return Command{}, err
	}
	cmd.Payload = json.RawMessage(payload)
	if len(result) > 0 {
		if err := json.Unmarshal(result, &cmd.Result); err != nil {
			return Command{}, err
		}
	}
	if deadline.Valid {
		cmd.DeadlineAt = deadline.Time
	}
	return cmd, nil
}
