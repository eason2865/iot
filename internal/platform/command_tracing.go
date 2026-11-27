package platform

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Commands can outlive a pod. Carry only the W3C parent, never HTTP baggage,
// tenant identifiers, payloads or credentials, across durable boundaries.
func commandTraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

func withCommandParent(ctx context.Context, parent string) context.Context {
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": parent})
}

func startCommandSpan(ctx context.Context, operation, id string, kind trace.SpanKind, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	opts = append(opts, trace.WithSpanKind(kind))
	if id != "" {
		opts = append(opts, trace.WithAttributes(attribute.String("iot.command.id", id)))
	}
	return otel.Tracer("iot/commands").Start(ctx, operation, opts...)
}

func endCommandSpan(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "operation failed")
	}
	span.End()
}

func commandDeliveryContext(ctx context.Context, headers []kafka.Header, storedParent string) context.Context {
	ctx = withCommandParent(ctx, storedParent)
	origin := trace.SpanContextFromContext(ctx)
	for _, header := range headers {
		if header.Key == "traceparent" {
			candidate := withCommandParent(ctx, string(header.Value))
			producer := trace.SpanContextFromContext(candidate)
			if !origin.IsValid() || (producer.TraceID() == origin.TraceID() && producer.IsSampled() == origin.IsSampled()) {
				return candidate
			}
			break
		}
	}
	// A manually replayed notification may lack Kafka headers.
	return ctx
}

func commandElapsed(span trace.Span, key string, from, to time.Time) {
	if !from.IsZero() && !to.Before(from) {
		span.SetAttributes(attribute.Int64(key, to.Sub(from).Milliseconds()))
	}
}
