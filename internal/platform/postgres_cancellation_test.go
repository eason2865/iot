package platform

import (
	"context"
	"testing"
	"time"
)

func TestLockedExpiryDoesNotBlockDispatcher(t *testing.T) {
	for _, status := range []string{"sent", "published", "created"} {
		t.Run(status, func(t *testing.T) { lockedCommandDoesNotBlockDispatcher(t, status) })
	}
}

func lockedCommandDoesNotBlockDispatcher(t *testing.T, status string) {
	t.Helper()
	s := isolatedPostgresStore(t)
	stale, err := s.CreateCommand("t", "d", nil)
	if err != nil {
		t.Fatal(err)
	}
	attempts := 1
	if status == "created" {
		attempts = maxDispatchAttempts
	}
	if _, err = s.db.Exec(`UPDATE commands SET status=$2, dispatch_attempts=$3,
 deadline_at=NOW()-INTERVAL '1 hour', updated_at=NOW()-INTERVAL '1 hour',
 next_dispatch_at=NOW()-INTERVAL '1 second' WHERE id=$1`, stale.ID, status, attempts); err != nil {
		t.Fatal(err)
	}
	_, err = s.CreateCommand("t", "d", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE commands SET next_dispatch_at=NOW()-INTERVAL '1 second' WHERE status='created'`); err != nil {
		t.Fatal(err)
	}
	lock, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err = lock.Exec(`SELECT id FROM commands WHERE id=$1 FOR UPDATE`, stale.ID); err != nil {
		t.Fatal(err)
	}
	published := make(chan struct{}, 1)
	pub := dispatchPublisher{publish: func(context.Context, Command) error {
		select {
		case published <- struct{}{}:
		default:
		}
		return nil
	}}
	dispatcher := NewCommandDispatcher(s, pub, time.Minute)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { dispatcher.Run(ctx); close(done) }()
	select {
	case <-published:
	case <-time.After(400 * time.Millisecond):
		cancel()
		_ = lock.Rollback()
		<-done
		t.Fatal("locked stale row blocked unrelated command dispatch")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(400 * time.Millisecond):
		_ = lock.Rollback()
		<-done
		t.Fatal("dispatcher ignored cancellation")
	}
}

func TestDispatcherCancellationInterruptsDatabaseWait(t *testing.T) {
	s := isolatedPostgresStore(t)
	lock, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err = lock.ExecContext(t.Context(), `LOCK TABLE commands IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	dispatcher := NewCommandDispatcher(s, dispatchPublisher{publish: func(context.Context, Command) error {
		t.Error("published while commands table was locked")
		return nil
	}}, time.Minute)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { dispatcher.Run(ctx); close(done) }()
	waiting := func() bool {
		t.Helper()
		var count int
		if err := s.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM pg_locks
 WHERE relation='commands'::regclass AND NOT granted`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count > 0
	}
	deadline := time.Now().Add(time.Second)
	for !waiting() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !waiting() {
		t.Fatal("dispatcher did not reach held database lock")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(400 * time.Millisecond):
		_ = lock.Rollback()
		<-done
		t.Fatal("dispatcher did not exit after cancellation")
	}
	deadline = time.Now().Add(400 * time.Millisecond)
	for waiting() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if waiting() {
		t.Fatal("database query remained blocked after dispatcher cancellation")
	}
}
