package platform

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"iot/internal/contracts"
)

// Every test owns a schema; the application database and running workers are untouched.
func isolatedPostgresStore(t *testing.T) *PostgresStore {
	t.Helper()
	dsn := os.Getenv("IOT_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set IOT_POSTGRES_TEST_DSN to run PostgreSQL regressions")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*cfg)
	schema := fmt.Sprintf("regression_%d", time.Now().UnixNano())
	if _, err = admin.Exec("CREATE SCHEMA " + schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE"); _ = admin.Close() })
	cfg.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cfg)
	s := &PostgresStore{db: db, ttl: 5 * time.Minute}
	t.Cleanup(func() { _ = db.Close() })
	if err = s.ensureSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateTenant(Tenant{ID: "t", Name: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateDevice(Device{TenantID: "t", DeviceID: "d", ProductID: "p", Secret: "test-secret"}); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestFinalDispatchAttemptKeepsActiveLease(t *testing.T) {
	s := isolatedPostgresStore(t)
	cmd, err := s.CreateCommand("t", "d", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE commands SET dispatch_attempts=9, next_dispatch_at=NOW()-INTERVAL '1 second' WHERE id=$1`, cmd.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimCommandsForDispatch(1, time.Minute)
	if err != nil || len(claimed) != 1 || claimed[0].DispatchAttempts != 10 {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	_, failed, err := s.RecoverStaleCommands(time.Now().Add(-time.Minute), 10)
	if err != nil || failed != 0 {
		t.Fatalf("active last attempt failed: count=%d err=%v", failed, err)
	}
	if _, err = s.db.Exec(`UPDATE commands SET next_dispatch_at=NOW()-INTERVAL '1 second' WHERE id=$1`, cmd.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err = s.ClaimCommandsForDispatch(1, time.Minute)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("exhausted attempt reclaimed: %v %v", claimed, err)
	}
	_, failed, err = s.RecoverStaleCommands(time.Now().Add(-time.Minute), 10)
	if err != nil || failed != 1 {
		t.Fatalf("expired final lease not failed: %d %v", failed, err)
	}
}

func TestStaleDispatcherCannotOverwriteNewLease(t *testing.T) {
	s := isolatedPostgresStore(t)
	cmd, err := s.CreateCommand("t", "d", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE commands SET dispatch_attempts=2,next_dispatch_at=NOW()+INTERVAL '1 minute' WHERE id=$1`, cmd.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.MarkCommandPublished(cmd.ID, 1); err != nil {
		t.Fatal(err)
	}
	if err = s.RescheduleCommand(cmd.ID, 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	var untouched bool
	if err = s.db.QueryRow(`SELECT status='created' AND next_dispatch_at < NOW()+INTERVAL '2 minutes' AND NOT EXISTS(SELECT 1 FROM command_events WHERE command_id=$1 AND event_type='published') FROM commands WHERE id=$1`, cmd.ID).Scan(&untouched); err != nil || !untouched {
		t.Fatalf("stale dispatcher changed current claim: %v %v", untouched, err)
	}
	if err = s.MarkCommandPublished(cmd.ID, 2); err != nil {
		t.Fatal(err)
	}
	stored, ok := s.GetCommand(cmd.ID)
	if !ok || stored.Status != contracts.CommandStatusPublished {
		t.Fatalf("current claim not published: %+v", stored)
	}
}

func TestCommandTransitionAuditAtomicity(t *testing.T) {
	for _, event := range []string{"published", "delivered", "timeout", "requeued", "failed"} {
		t.Run(event, func(t *testing.T) {
			s := isolatedPostgresStore(t)
			cmd, err := s.CreateCommand("t", "d", nil)
			if err != nil {
				t.Fatal(err)
			}
			prior, want := "created", event
			switch event {
			case "delivered":
				want = "sent"
			case "timeout":
				prior = "sent"
			case "requeued":
				prior = "published"
				want = "created"
			}
			if _, err = s.db.Exec(`UPDATE commands SET status=$2, dispatch_attempts=10, next_dispatch_at=NOW()-INTERVAL '1 minute', updated_at=NOW()-INTERVAL '1 hour', deadline_at=NOW()-INTERVAL '1 minute' WHERE id=$1`, cmd.ID, prior); err != nil {
				t.Fatal(err)
			}
			// Force the audit insert to fail after the state update has run.
			if _, err = s.db.Exec(`ALTER TABLE command_events ADD CONSTRAINT reject_event CHECK(event_type <> '` + event + `')`); err != nil {
				t.Fatal(err)
			}
			if event == "published" {
				if _, err = s.db.Exec(`UPDATE commands SET next_dispatch_at=NOW()+INTERVAL '1 hour' WHERE id=$1`, cmd.ID); err != nil {
					t.Fatal(err)
				}
			}
			transition := func() error {
				switch event {
				case "published":
					return s.MarkCommandPublished(cmd.ID, 10)
				case "delivered":
					return s.MarkCommandSent(cmd.ID, time.Now().Add(time.Minute))
				case "timeout":
					_, err = s.ExpireCommands(time.Now(), time.Minute)
				case "requeued":
					err = s.RequeueCommand(cmd.ID, time.Second)
				case "failed":
					_, _, err = s.RecoverStaleCommands(time.Now().Add(-time.Minute), 10)
				}
				return err
			}
			if err = transition(); err == nil {
				t.Fatal("expected audit failure")
			}
			var status string
			if err = s.db.QueryRow(`SELECT status FROM commands WHERE id=$1`, cmd.ID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != prior {
				t.Fatalf("state committed without audit: got %s want %s", status, prior)
			}
			if _, err = s.db.Exec(`ALTER TABLE command_events DROP CONSTRAINT reject_event`); err != nil {
				t.Fatal(err)
			}
			s.db.SetMaxOpenConns(1)
			done := make(chan error, 1)
			go func() { done <- transition() }()
			select {
			case err = <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				s.db.SetMaxOpenConns(2)
				<-done
				t.Fatal("transition needs a second pool connection")
			}
			if err = s.db.QueryRow(`SELECT status FROM commands WHERE id=$1`, cmd.ID).Scan(&status); err != nil || status != want {
				t.Fatalf("status=%s want=%s err=%v", status, want, err)
			}
			if err = transition(); err != nil {
				t.Fatal(err)
			}
			var count int
			if err = s.db.QueryRow(`SELECT COUNT(*) FROM command_events WHERE command_id=$1 AND event_type=$2`, cmd.ID, event).Scan(&count); err != nil || count != 1 {
				t.Fatalf("audit count=%d err=%v", count, err)
			}
		})
	}
}

func TestPostgresWritesWithOneConnection(t *testing.T) {
	for _, op := range []string{"telemetry", "command", "duplicate"} {
		t.Run(op, func(t *testing.T) {
			s := isolatedPostgresStore(t)
			env := contracts.Envelope{MsgID: "m", TenantID: "t", DeviceID: "d", Ts: time.Now().UnixMilli(), Type: "telemetry", Version: "v1"}
			if op == "duplicate" {
				if _, err := s.RecordTelemetry(env); err != nil {
					t.Fatal(err)
				}
			}
			s.db.SetMaxOpenConns(1)
			done := make(chan error, 1)
			go func() {
				var err error
				if op == "command" {
					_, err = s.CreateCommand("t", "d", nil)
				} else {
					_, err = s.RecordTelemetry(env)
				}
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil && !(op == "duplicate" && IsTelemetryDuplicate(err)) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				s.db.SetMaxOpenConns(2) // Release the old implementation before reporting the regression.
				<-done
				t.Fatal("write held the only transaction connection while waiting for another pool connection")
			}
		})
	}
}

func TestPostgresIdempotentCommandCreation(t *testing.T) {
	s := isolatedPostgresStore(t)
	const callers = 12
	ids := make(chan string, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd, err := s.CreateCommandWithKey(t.Context(), "t", "d", "request-1", []byte(`{"switch":"on","value":1}`))
			if err != nil {
				errs <- err
				return
			}
			ids <- cmd.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var id string
	for got := range ids {
		if id == "" {
			id = got
		}
		if got != id {
			t.Errorf("retry created another command: %s != %s", got, id)
		}
	}
	var commands, events int
	if err := s.db.QueryRow(`SELECT count(*) FROM commands`).Scan(&commands); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM command_events WHERE event_type='created'`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if commands != 1 || events != 1 {
		t.Fatalf("commands=%d events=%d, want one of each", commands, events)
	}
	// JSONB comparison ignores object key ordering and whitespace.
	same, err := s.CreateCommandWithKey(t.Context(), "t", "d", "request-1", []byte(`{ "value":1, "switch":"on" }`))
	if err != nil || same.ID != id {
		t.Fatalf("equivalent retry: %+v %v", same, err)
	}
	if _, err = s.CreateCommandWithKey(t.Context(), "t", "d", "request-1", []byte(`{"switch":"off"}`)); err != ErrIdempotencyConflict {
		t.Fatalf("changed request error=%v", err)
	}
	if _, err = s.CreateDevice(Device{TenantID: "t", DeviceID: "d2", ProductID: "p", Secret: "test-secret"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateCommandWithKey(t.Context(), "t", "d2", "request-1", []byte(`{"switch":"on","value":1}`)); err != ErrIdempotencyConflict {
		t.Fatalf("changed device error=%v", err)
	}
	if _, err = s.CreateTenant(Tenant{ID: "t2", Name: "other"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateDevice(Device{TenantID: "t2", DeviceID: "d", ProductID: "p", Secret: "test-secret"}); err != nil {
		t.Fatal(err)
	}
	other, err := s.CreateCommandWithKey(t.Context(), "t2", "d", "request-1", []byte(`{}`))
	if err != nil || other.ID == id {
		t.Fatalf("tenant-scoped key: %+v %v", other, err)
	}
}

func TestPostgresContextWritesAndLookups(t *testing.T) {
	s := isolatedPostgresStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.GetDeviceContext(ctx, "t", "d"); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetDeviceContext: %v", err)
	}
	if _, err := s.RecordTelemetryContext(ctx, contracts.Envelope{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RecordTelemetryContext: %v", err)
	}
	if _, err := s.CreateCommandWithKey(ctx, "t", "d", "key", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateCommandWithKey: %v", err)
	}
	if _, err := s.GetDeviceContext(t.Context(), "t", "missing"); !IsNotFound(err) {
		t.Fatalf("missing device: %v", err)
	}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetDeviceContext(t.Context(), "t", "d"); err == nil || IsNotFound(err) {
		t.Fatalf("database error hidden as missing device: %v", err)
	}
}

func TestPostgresSchemaUpgradeIsSerializedAndPreservesCredentials(t *testing.T) {
	s := isolatedPostgresStore(t)
	_, err := s.db.Exec(`ALTER TABLE devices ADD COLUMN secret TEXT NOT NULL DEFAULT 'legacy-secret'; ALTER TABLE devices ALTER COLUMN secret_hash DROP NOT NULL; UPDATE devices SET secret_hash=NULL; ALTER TABLE commands DROP COLUMN idempotency_key CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	s.db.SetMaxOpenConns(2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { errs <- s.MigrateSchema(t.Context()) }()
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if !s.AuthenticateDevice("t", "d", "legacy-secret") {
		t.Fatal("legacy credential not preserved")
	}
	var plaintext *string
	if err := s.db.QueryRow(`SELECT secret FROM devices WHERE tenant_id='t' AND device_id='d'`).Scan(&plaintext); err != nil || plaintext != nil {
		t.Fatalf("plaintext secret retained: %v", err)
	}
	if _, err := s.CreateCommandWithKey(t.Context(), "t", "d", "upgraded-key", nil); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStartupCanSkipDDL(t *testing.T) {
	s := isolatedPostgresStore(t)
	var schema string
	if err := s.db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	scoped, err := url.Parse(os.Getenv("IOT_POSTGRES_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	query := scoped.Query()
	query.Set("search_path", schema)
	query.Set("default_transaction_read_only", "on")
	scoped.RawQuery = query.Encode()
	readOnly, err := OpenPostgresStore(scoped.String(), time.Minute, false)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if _, err := readOnly.GetDeviceContext(t.Context(), "t", "d"); err != nil {
		t.Fatal(err)
	}
	if err := readOnly.MigrateSchema(t.Context()); err == nil {
		t.Fatal("read-only connection unexpectedly permitted DDL")
	}
}
