package platform

import (
	"context"
	"time"
)

// Production stores support caller cancellation; legacy in-memory adapters
// retain the original methods for compatibility with the test harness.
func markTelemetryWritten(ctx context.Context, store telemetryCompletionMarker, msg, tenant, device string) error {
	if scoped, ok := store.(interface {
		MarkTelemetryTDengineWrittenContext(context.Context, string, string, string) error
	}); ok {
		return scoped.MarkTelemetryTDengineWrittenContext(ctx, msg, tenant, device)
	}
	return store.MarkTelemetryTDengineWritten(msg, tenant, device)
}

func markStoredCommandSent(ctx context.Context, store interface{ MarkCommandSent(string, time.Time) error }, id string, deadline time.Time) error {
	if scoped, ok := store.(interface {
		MarkCommandSentContext(context.Context, string, time.Time) error
	}); ok {
		return scoped.MarkCommandSentContext(ctx, id, deadline)
	}
	return store.MarkCommandSent(id, deadline)
}

func requeueStoredCommand(ctx context.Context, store interface {
	RequeueCommand(string, time.Duration) error
}, id string, backoff time.Duration) error {
	if scoped, ok := store.(interface {
		RequeueCommandContext(context.Context, string, time.Duration) error
	}); ok {
		return scoped.RequeueCommandContext(ctx, id, backoff)
	}
	return store.RequeueCommand(id, backoff)
}
