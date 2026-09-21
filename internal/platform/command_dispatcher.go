package platform

import (
	"context"
	"log"
	"time"
)

// maxDispatchAttempts bounds redelivery of a command; once exhausted the
// command is marked failed instead of looping forever.
const maxDispatchAttempts = 10

type CommandDispatchStore interface {
	ClaimCommandsForDispatch(limit int, lease time.Duration) ([]Command, error)
	MarkCommandPublished(id string) error
	RescheduleCommand(id string, retryAfter time.Duration) error
	ExpireCommands(now time.Time) (int64, error)
	RecoverStaleCommands(staleBefore time.Time, maxAttempts int) (int64, int64, error)
}

type CommandDispatcher struct {
	store     CommandDispatchStore
	publisher MessagePublisher
	timeout   time.Duration
	pollEvery time.Duration
	lease     time.Duration
	batchSize int
}

func NewCommandDispatcher(store CommandDispatchStore, publisher MessagePublisher, timeout time.Duration) *CommandDispatcher {
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	return &CommandDispatcher{store: store, publisher: publisher, timeout: timeout, pollEvery: time.Second, lease: 30 * time.Second, batchSize: 100}
}

func (d *CommandDispatcher) Run(ctx context.Context) {
	if d == nil || d.store == nil || d.publisher == nil {
		return
	}
	ticker := time.NewTicker(d.pollEvery)
	defer ticker.Stop()
	for {
		d.dispatchOnce()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *CommandDispatcher) dispatchOnce() {
	// Requeue commands stranded in 'published' (Kafka event never confirmed by
	// the worker) and fail commands that exhausted dispatch attempts.
	staleBefore := time.Now().UTC().Add(-d.timeout)
	if requeued, failed, err := d.store.RecoverStaleCommands(staleBefore, maxDispatchAttempts); err != nil {
		log.Printf("command recovery scan error: %v", err)
	} else if requeued > 0 || failed > 0 {
		log.Printf("command recovery scan requeued %d, failed %d commands", requeued, failed)
	}
	if expired, err := d.store.ExpireCommands(time.Now().UTC()); err != nil {
		log.Printf("command timeout scan error: %v", err)
	} else if expired > 0 {
		log.Printf("command timeout scan marked %d commands", expired)
	}
	commands, err := d.store.ClaimCommandsForDispatch(d.batchSize, d.lease)
	if err != nil {
		log.Printf("command dispatch claim error: %v", err)
		return
	}
	for _, command := range commands {
		if err := d.publisher.PublishCommand(command); err != nil {
			backoff := time.Second * time.Duration(1<<min(command.DispatchAttempts, 6))
			log.Printf("command dispatch error: id=%s err=%v", command.ID, err)
			if retryErr := d.store.RescheduleCommand(command.ID, backoff); retryErr != nil {
				log.Printf("command reschedule error: id=%s err=%v", command.ID, retryErr)
			}
			continue
		}
		if err := d.store.MarkCommandPublished(command.ID); err != nil {
			log.Printf("command mark published error: id=%s err=%v", command.ID, err)
		}
	}
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
