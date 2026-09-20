package platform

import (
	"context"
	"log"
	"time"
)

type CommandDispatchStore interface {
	ClaimCommandsForDispatch(limit int, lease time.Duration) ([]Command, error)
	MarkCommandSent(id string, deadline time.Time) error
	RescheduleCommand(id string, retryAfter time.Duration) error
	ExpireCommands(now time.Time) (int64, error)
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
		if err := d.store.MarkCommandSent(command.ID, time.Now().UTC().Add(d.timeout)); err != nil {
			log.Printf("command mark sent error: id=%s err=%v", command.ID, err)
		}
	}
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
