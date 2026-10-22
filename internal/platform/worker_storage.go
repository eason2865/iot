package platform

import (
	"context"
	"time"

	"iot/internal/contracts"
)

// Defensive bounds also protect maintenance callers from a manually corrupted
// database counter; untrusted Kafka counters are rejected before lookup.
func commandRetryBackoff(attempt int) time.Duration {
	return time.Second * time.Duration(1<<min(max(attempt, 0), 6))
}

func restoreStoredTelemetry(ctx context.Context, store Repository, rec TelemetryRecord) (TelemetryRecord, error) {
	if restorer, ok := store.(interface {
		RestoreTelemetryContext(context.Context, TelemetryRecord) (TelemetryRecord, error)
	}); ok {
		return restorer.RestoreTelemetryContext(ctx, rec)
	}
	env := contracts.Envelope{MsgID: rec.MsgID, TenantID: rec.TenantID, DeviceID: rec.DeviceID, Ts: rec.Ts, Type: rec.Type, Version: rec.Version, Payload: rec.Payload}
	if scoped, ok := store.(TelemetryInbox); ok {
		return scoped.RecordTelemetryContext(ctx, env)
	}
	return store.RecordTelemetry(env)
}

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
