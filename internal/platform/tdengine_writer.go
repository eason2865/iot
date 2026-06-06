package platform

import (
	"database/sql"
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

func NewTDengineWriter(cfg TDengineConfig, metrics *Metrics) (*TDengineWriter, error) {
	if cfg.DSN == "" {
		return nil, nil
	}
	table := cfg.Table
	if table == "" {
		table = "telemetry"
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
		flushInterval: 100 * time.Millisecond,
		batchSize:     50,
		metrics:       metrics,
	}
	if err := w.ensureSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	w.wg.Add(1)
	go w.run()
	return w, nil
}

func (w *TDengineWriter) Close() error {
	if w == nil || w.db == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		close(w.pendingCh)
	})
	w.wg.Wait()
	return w.db.Close()
}

func (w *TDengineWriter) ensureSchema() error {
	stmts := []string{
		"CREATE DATABASE IF NOT EXISTS iot",
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  ts TIMESTAMP,
  tenant_id NCHAR(64),
  device_id NCHAR(64),
  msg_id NCHAR(64),
  type NCHAR(64),
  version NCHAR(32),
  payload NCHAR(4096)
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
	select {
	case w.pendingCh <- rec:
		return nil
	default:
		// Backpressure is preferable to dropping telemetry.
		w.pendingCh <- rec
		return nil
	}
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

	var b strings.Builder
	b.Grow(len(records) * 256)
	b.WriteString("INSERT INTO ")
	b.WriteString(w.table)
	b.WriteString(" VALUES ")

	for i, rec := range records {
		if i > 0 {
			b.WriteString(", ")
		}
		payload := escapeTD(string(rec.Payload))
		b.WriteString(fmt.Sprintf(
			"('%s', '%s', '%s', '%s', '%s', '%s', '%s')",
			time.UnixMilli(rec.Ts).UTC().Format("2006-01-02 15:04:05.000"),
			escapeTD(rec.TenantID),
			escapeTD(rec.DeviceID),
			escapeTD(rec.MsgID),
			escapeTD(rec.Type),
			escapeTD(rec.Version),
			escapeTD(payload),
		))
	}

	_, err := w.db.Exec(b.String())
	if err != nil {
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
