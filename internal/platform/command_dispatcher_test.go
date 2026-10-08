package platform

import (
	"context"
	"testing"
	"time"
)

type stubDispatchStore struct {
	claims int
}

func (s *stubDispatchStore) ClaimCommandsForDispatch(int, time.Duration) ([]Command, error) {
	s.claims++
	return nil, nil
}

func (s *stubDispatchStore) MarkCommandPublished(string) error             { return nil }
func (s *stubDispatchStore) RescheduleCommand(string, time.Duration) error { return nil }
func (s *stubDispatchStore) ExpireCommands(time.Time) (int64, error)       { return 0, nil }
func (s *stubDispatchStore) RecoverStaleCommands(time.Time, int) (int64, int64, error) {
	return 0, 0, nil
}

// TestCommandDispatcherRunStopsOnContextCancel pins the shutdown contract of the
// dispatch loop: iot-core ties it to SIGINT/SIGTERM, so Run must return once the
// context is cancelled instead of polling forever.
func TestCommandDispatcherRunStopsOnContextCancel(t *testing.T) {
	store := &stubDispatchStore{}
	dispatcher := NewCommandDispatcher(store, noopPublisher{}, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		dispatcher.Run(ctx)
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("CommandDispatcher.Run kept running after the context was cancelled")
	}

	if store.claims == 0 {
		t.Fatal("CommandDispatcher.Run never polled the store")
	}
}
