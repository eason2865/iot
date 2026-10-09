package platform

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

// TDengineWriter writes one telemetry record per statement. It deliberately does
// not buffer records in memory: device-worker commits a Kafka offset only after
// the write returns, so a background batch would turn "write succeeded" into
// "enqueued" and open a new data-loss window. Throughput is scaled on the
// TDengine side (connection pool / server capacity), not by batching here.
type TDengineWriter struct {
	db        *sql.DB
	database  string
	table     string
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
	metrics   *Metrics
}

type TDengineConfig struct {
	DSN      string
	Database string
	Table    string
}

var errTDengineWriterClosed = errors.New("tdengine writer closed")

func NewTDengineWriter(cfg TDengineConfig, metrics *Metrics) (*TDengineWriter, error) {
	if cfg.DSN == "" {
		return nil, nil
	}
	table := cfg.Table
	if table == "" {
		table = "telemetry_v2"
	}
	database := cfg.Database
	if database == "" {
		database = "iot"
	}
	db, err := sql.Open("taosRestful", cfg.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	w := &TDengineWriter{
		db:       db,
		database: database,
		table:    table,
		metrics:  metrics,
	}
	if err := w.ensureSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return w, nil
}

func (w *TDengineWriter) Close() error {
	if w == nil || w.db == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
	})
	return w.db.Close()
}

func (w *TDengineWriter) ensureSchema() error {
	stmts := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", w.database),
		fmt.Sprintf(`CREATE STABLE IF NOT EXISTS %s.%s (
  ts TIMESTAMP,
  msg_id VARCHAR(64),
  type VARCHAR(64),
  version VARCHAR(32),
  payload_hash BINARY(64),
  payload_bytes INT
) TAGS (
  tenant_id VARCHAR(64),
  device_id VARCHAR(64)
)`, w.database, w.table),
	}
	for _, stmt := range stmts {
		if _, err := w.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (w *TDengineWriter) WriteTelemetry(rec TelemetryRecord) error {
	if w == nil || w.db == nil {
		return nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return errTDengineWriterClosed
	}
	w.mu.Unlock()
	// A Kafka offset is committed only after this call returns successfully, so
	// TDengine failures are routed to the DLQ instead of being logged and lost.
	return w.writeRecord(rec)
}

// escapeTD escapes a value for a TDengine string literal. Backslashes must be
// escaped first: TDengine treats '\' as an escape character, so a raw
// backslash would corrupt the statement (or break out of it).
func escapeTD(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `'`, `''`)
}

func (w *TDengineWriter) writeRecord(rec TelemetryRecord) error {
	hash := fmt.Sprintf("%x", sha256.Sum256(rec.Payload))
	// One subtable per device makes tenant/device dimensions TDengine tags.
	// Raw JSON remains in PostgreSQL JSONB, eliminating a second bounded copy.
	tableHash := fmt.Sprintf("%x", sha256.Sum256([]byte(rec.TenantID+"\x00"+rec.DeviceID)))[:24]
	stable := w.database + "." + w.table
	childTable := stable + "_" + tableHash
	statement := fmt.Sprintf(
		"INSERT INTO %s USING %s TAGS ('%s', '%s') VALUES ('%s', '%s', '%s', '%s', '%s', %d)",
		childTable, stable, escapeTD(rec.TenantID), escapeTD(rec.DeviceID),
		time.UnixMilli(rec.Ts).UTC().Format("2006-01-02 15:04:05.000"), escapeTD(rec.MsgID),
		escapeTD(rec.Type), escapeTD(rec.Version), hash, len(rec.Payload),
	)
	if _, err := w.db.Exec(statement); err != nil {
		if w.metrics != nil {
			w.metrics.IncTDengineWrite("error")
		}
		return err
	}
	if w.metrics != nil {
		w.metrics.IncTDengineWrite("ok")
	}
	return nil
}
