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
	ClaimCommandsForDispatchContext(ctx context.Context, limit int, lease time.Duration) ([]Command, error)
	MarkCommandPublishedContext(ctx context.Context, id string, attempt int) error
	RescheduleCommandContext(ctx context.Context, id string, attempt int, retryAfter time.Duration) error
	ExpireCommandsContext(ctx context.Context, now time.Time, nullDeadlineGrace time.Duration) (int64, error)
	RecoverStaleCommandsContext(ctx context.Context, staleBefore time.Time, maxAttempts int) (int64, int64, error)
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
	ctx, cancel := context.WithCancel(ctx)
	scansDone := make(chan struct{})
	go func() {
		defer close(scansDone)
		ticker := time.NewTicker(d.pollEvery)
		defer ticker.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			d.scanCommands(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() { cancel(); <-scansDone }()
	ticker := time.NewTicker(d.pollEvery)
	defer ticker.Stop()
	for {
		d.dispatchPending(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *CommandDispatcher) dispatchOnce(ctx context.Context) {
	d.scanCommands(ctx)
	d.dispatchPending(ctx)
}

func (d *CommandDispatcher) scanCommands(ctx context.Context) {
	// Requeue commands stranded in 'published' (Kafka event never confirmed by
	// the worker) and fail commands that exhausted dispatch attempts.
	staleBefore := time.Now().UTC().Add(-d.timeout)
	if requeued, failed, err := d.store.RecoverStaleCommandsContext(ctx, staleBefore, maxDispatchAttempts); err != nil {
		log.Printf("command recovery scan error: %v", err)
	} else if requeued > 0 || failed > 0 {
		log.Printf("command recovery scan requeued %d, failed %d commands", requeued, failed)
	}
	// The ACK timeout doubles as the grace period for legacy 'sent' rows that
	// predate deadline_at: they are given one full timeout window from their
	// last update before being expired.
	if expired, err := d.store.ExpireCommandsContext(ctx, time.Now().UTC(), d.timeout); err != nil {
		log.Printf("command timeout scan error: %v", err)
	} else if expired > 0 {
		log.Printf("command timeout scan marked %d commands", expired)
	}
}

func (d *CommandDispatcher) dispatchPending(ctx context.Context) {
	// Claim immediately before publishing, so queued commands do not spend
	// their lease waiting for earlier Kafka calls in the batch.
	for i := 0; i < d.batchSize && ctx.Err() == nil; i++ {
		commands, err := d.store.ClaimCommandsForDispatchContext(ctx, 1, d.lease)
		if err != nil {
			log.Printf("command dispatch claim error: %v", err)
			return
		}
		if len(commands) == 0 {
			return
		}
		command := commands[0]
		publishCtx, cancel := context.WithTimeout(ctx, d.lease/2)
		err = d.publisher.PublishCommand(publishCtx, command)
		cancel()
		if err != nil {
			backoff := commandRetryBackoff(command.DispatchAttempts)
			log.Printf("command dispatch error: id=%s err=%v", command.ID, err)
			if retryErr := d.store.RescheduleCommandContext(ctx, command.ID, command.DispatchAttempts, backoff); retryErr != nil {
				log.Printf("command reschedule error: id=%s err=%v", command.ID, retryErr)
			}
			continue
		}
		if err := d.store.MarkCommandPublishedContext(ctx, command.ID, command.DispatchAttempts); err != nil {
			log.Printf("command mark published error: id=%s err=%v", command.ID, err)
		}
	}
}
