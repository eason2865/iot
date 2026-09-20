package platform

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

type TDengineWriter struct {
	db            *sql.DB
	table         string
	pendingCh     chan TelemetryRecord
	closedCh      chan struct{}
	mu            sync.Mutex
	closed        bool
	closeOnce     sync.Once
	wg            sync.WaitGroup
	flushInterval time.Duration
	batchSize     int
	metrics       *Metrics
}

type TDengineConfig struct {
	DSN   string
	Table string
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
		db:            db,
		table:         table,
		pendingCh:     make(chan TelemetryRecord, 1024),
		closedCh:      make(chan struct{}),
		flushInterval: 100 * time.Millisecond,
		batchSize:     50,
		metrics:       metrics,
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
		if w.closedCh == nil {
			w.closedCh = make(chan struct{})
		}
		w.closed = true
		close(w.closedCh)
		close(w.pendingCh)
		w.mu.Unlock()
	})
	w.wg.Wait()
	return w.db.Close()
}

func (w *TDengineWriter) ensureSchema() error {
	stmts := []string{
		"CREATE DATABASE IF NOT EXISTS iot",
		fmt.Sprintf(`CREATE STABLE IF NOT EXISTS %s (
  ts TIMESTAMP,
  msg_id VARCHAR(64),
  type VARCHAR(64),
  version VARCHAR(32),
  payload_hash BINARY(64),
  payload_bytes INT
) TAGS (
  tenant_id VARCHAR(64),
  device_id VARCHAR(64)
)`, w.table),
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
	return w.writeBatch([]TelemetryRecord{rec})
}

func escapeTD(s string) string {
	return strings.ReplaceAll(s, `'`, `''`)
}

func (w *TDengineWriter) run() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	batch := make([]TelemetryRecord, 0, w.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := w.writeBatch(batch); err != nil {
			log.Printf("tdengine batch write error: %v", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case rec, ok := <-w.pendingCh:
			if !ok {
				flush()
				return
			}
			batch = append(batch, rec)
			if len(batch) >= w.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (w *TDengineWriter) writeBatch(records []TelemetryRecord) error {
	if len(records) == 0 {
		return nil
	}

	for _, rec := range records {
		hash := fmt.Sprintf("%x", sha256.Sum256(rec.Payload))
		// One subtable per device makes tenant/device dimensions TDengine tags.
		// Raw JSON remains in PostgreSQL JSONB, eliminating a second bounded copy.
		tableHash := fmt.Sprintf("%x", sha256.Sum256([]byte(rec.TenantID+"\x00"+rec.DeviceID)))[:24]
		childTable := w.table + "_" + tableHash
		statement := fmt.Sprintf(
			"INSERT INTO %s USING %s TAGS ('%s', '%s') VALUES ('%s', '%s', '%s', '%s', '%s', %d)",
			childTable, w.table, escapeTD(rec.TenantID), escapeTD(rec.DeviceID),
			time.UnixMilli(rec.Ts).UTC().Format("2006-01-02 15:04:05.000"), escapeTD(rec.MsgID),
			escapeTD(rec.Type), escapeTD(rec.Version), hash, len(rec.Payload),
		)
		if _, err := w.db.Exec(statement); err != nil {
			if w.metrics != nil {
				w.metrics.IncTDengineWrite("error")
			}
			return err
		}
	}
	if w.metrics != nil {
		w.metrics.IncTDengineWrite("ok")
	}
	return nil
}
