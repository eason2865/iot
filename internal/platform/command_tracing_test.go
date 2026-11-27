package platform

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type acceptedCommandPublish struct{ mqtt.Token }

func (acceptedCommandPublish) WaitTimeout(time.Duration) bool { return true }
func (acceptedCommandPublish) Error() error                   { return nil }

type commandTraceMQTT struct{ mqtt.Client }

func (commandTraceMQTT) Publish(string, byte, bool, interface{}) mqtt.Token {
	return acceptedCommandPublish{}
}

func TestCommandTraceAcrossDispatchDeliveryAckAndTimeout(t *testing.T) {
	store := isolatedPostgresStore(t)
	brokers, topic := regressionKafkaTopic(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer func() { otel.SetTracerProvider(old); _ = provider.Shutdown(context.Background()) }()

	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, RequiredAcks: kafka.RequireAll, BatchSize: 1}
	defer writer.Close()
	publisher := &KafkaPublisher{commandWriter: writer}
	dispatcher := NewCommandDispatcher(store, publisher, time.Minute)
	dispatcher.pollEvery = 50 * time.Millisecond
	done := make(chan struct{})
	go func() { defer close(done); dispatcher.Run(ctx) }()
	defer func() { cancel(); <-done }()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: topic, StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 2 << 20})
	defer reader.Close()
	worker := &Worker{store: store, commandReader: reader, mqtt: commandTraceMQTT{}, acks: newWorkerAckState(), ackTimeout: time.Minute}
	defer worker.acks.cancel()

	for _, terminal := range []string{"ack", "timeout"} {
		parentCtx, root := provider.Tracer("test").Start(ctx, "request")
		cmd, err := store.CreateCommandWithKey(parentCtx, "t", "d", terminal, nil)
		root.End()
		if err != nil {
			t.Fatal(err)
		}
		// A retry under an independent request must preserve the first causal parent.
		retry, err := store.CreateCommandWithKey(ctx, "t", "d", terminal, nil)
		if err != nil || retry.ID != cmd.ID {
			t.Fatalf("idempotency: %v %s", err, retry.ID)
		}
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := store.GetCommandForDelivery(ctx, cmd.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(msg.Headers) != 1 || msg.Headers[0].Key != "traceparent" {
			t.Fatal("missing Kafka traceparent")
		}
		if err = worker.deliverCommand(ctx, msg, stored, time.Now()); err != nil {
			t.Fatal(err)
		}

		if terminal == "ack" {
			payload, _ := json.Marshal(CommandAckMessage{CommandID: cmd.ID, TenantID: "t", DeviceID: "d", Status: "acked"})
			ack := &countedAckMessage{fakeMQTTMessage: fakeMQTTMessage{topic: "tenant/t/device/d/ack", payload: payload}}
			worker.handleAckMessage(nil, ack)
			if ack.acks.Load() != 1 {
				t.Fatal("ACK not persisted")
			}
		} else if expired, err := store.ExpireCommandsContext(ctx, time.Now().Add(2*time.Minute)); err != nil || expired != 1 {
			t.Fatalf("expired=%d err=%v", expired, err)
		}

		spans := map[string]tracetest.SpanStub{}
		for _, span := range exporter.GetSpans() {
			if span.SpanContext.TraceID() == root.SpanContext().TraceID() {
				spans[span.Name] = span
			}
		}
		parents := map[string]string{
			"command.create":        "request",
			"command.dispatch":      "command.create",
			"command.kafka.publish": "command.dispatch",
			"command.kafka.consume": "command.kafka.publish",
			"command.mqtt.publish":  "command.kafka.consume",
			"command." + terminal:   "command.create",
		}
		for child, parent := range parents {
			s, ok := spans[child]
			if !ok || s.Parent.SpanID() != spans[parent].SpanContext.SpanID() {
				t.Fatalf("%s missing or wrong parent %s", child, parent)
			}
			attrs := attribute.NewSet(s.Attributes...)
			id, _ := attrs.Value("iot.command.id")
			if id.AsString() != cmd.ID {
				t.Fatalf("%s command ID=%v", child, id)
			}
		}
		last := spans["command."+terminal]
		attrs := attribute.NewSet(last.Attributes...)
		if _, ok := attrs.Value("iot.command.ack_wait_ms"); !ok {
			t.Fatal("ACK wait missing")
		}
		if terminal == "timeout" && last.Status.Code != codes.Error {
			t.Fatal("timeout not marked failed")
		}
	}
}

func TestCommandDeliveryKeepsStoredTraceOnReplay(t *testing.T) {
	const parent = "00-11111111111111111111111111111111-2222222222222222-00"
	for _, v := range []string{"", "invalid", "00-33333333333333333333333333333333-4444444444444444-01", "00-11111111111111111111111111111111-4444444444444444-01"} {
		ctx := commandDeliveryContext(t.Context(), []kafka.Header{{Key: "traceparent", Value: []byte(v)}}, parent)
		if got := commandTraceParent(ctx); got != parent || trace.SpanContextFromContext(ctx).IsSampled() {
			t.Fatalf("stored parent changed to %q", got)
		}
	}
}
