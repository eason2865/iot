package platform

import (
	"context"
	"testing"
	"time"
)

type stubDispatchStore struct {
	claims   int
	commands []Command
	limits   []int
}

func (s *stubDispatchStore) ClaimCommandsForDispatch(limit int, _ time.Duration) ([]Command, error) {
	s.claims++
	s.limits = append(s.limits, limit)
	if len(s.commands) > 0 {
		commands := s.commands[:1]
		s.commands = s.commands[1:]
		return commands, nil
	}
	return nil, nil
}

type dispatchPublisher struct {
	noopPublisher
	publish func(context.Context, Command) error
}

func (p dispatchPublisher) PublishCommand(ctx context.Context, c Command) error {
	return p.publish(ctx, c)
}

func TestDispatcherClaimsJustBeforeEachBoundedPublish(t *testing.T) {
	s := &stubDispatchStore{commands: []Command{{ID: "a", DispatchAttempts: 1}, {ID: "b", DispatchAttempts: 10}}}
	count := 0
	p := dispatchPublisher{publish: func(ctx context.Context, c Command) error {
		count++
		if s.claims != count || s.limits[count-1] != 1 {
			t.Fatalf("batch preclaimed before publish: claims=%d limits=%v", s.claims, s.limits)
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second/2 {
			t.Fatal("publish is not bounded within its lease")
		}
		return nil
	}}
	d := NewCommandDispatcher(s, p, time.Minute)
	d.batchSize = 2
	d.lease = time.Second
	d.dispatchOnce(t.Context())
	if count != 2 {
		t.Fatalf("published=%d", count)
	}
}

func (s *stubDispatchStore) MarkCommandPublished(string, int) error             { return nil }
func (s *stubDispatchStore) RescheduleCommand(string, int, time.Duration) error { return nil }
func (s *stubDispatchStore) ExpireCommands(time.Time, time.Duration) (int64, error) {
	return 0, nil
}
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

}
