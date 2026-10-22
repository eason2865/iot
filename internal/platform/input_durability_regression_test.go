package platform

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"iot/internal/contracts"
)

func TestSecretAndJSONBInputBounds(t *testing.T) {
	if _, err := HashDeviceSecret(strings.Repeat("x", 73)); !errors.Is(err, contracts.ErrInvalidDeviceSecret) {
		t.Fatal("accepted overlong bcrypt secret")
	}
	for _, raw := range []string{`{"x":"\u0000"}`, `{"\u0000":1}`, `{"x":1e1000000}`, `{"x":"\ud800"}`} {
		if err := contracts.ValidateTelemetryPayload(json.RawMessage(raw)); err == nil {
			t.Errorf("accepted JSONB-incompatible payload %s", raw)
		}
	}
}

func TestMixedLegacySecretsAreErased(t *testing.T) {
	s := isolatedPostgresStore(t)
	if _, err := s.db.Exec(`ALTER TABLE devices ADD COLUMN secret TEXT DEFAULT 'stale-plaintext'; UPDATE devices SET secret='stale-plaintext'`); err != nil {
		t.Fatal(err)
	}
	if err := s.MigrateSchema(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateDevice(Device{TenantID: "t", DeviceID: "new", ProductID: "p", Secret: "new-secret"}); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM devices WHERE secret IS NOT NULL`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("plaintext remains count=%d err=%v", n, err)
	}
	if !s.AuthenticateDevice("t", "d", "test-secret") {
		t.Fatal("existing hash changed")
	}
}

func TestOversizeCommandRejectedBeforeAcceptance(t *testing.T) {
	s := isolatedPostgresStore(t)
	raw := json.RawMessage(`{"v":"` + strings.Repeat("x", 1048430) + `"}`)
	_, err := s.CreateCommandWithKey(t.Context(), "t", "d", "oversize", raw)
	if err == nil {
		t.Fatal("oversize command committed")
	}
	var n int
	if err = s.db.QueryRow(`SELECT count(*) FROM commands`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("rejected command persisted n=%d err=%v", n, err)
	}
}

func TestKafkaRetryReturnsInitializationError(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := ensureKafkaTopicsUntil(ctx, []string{"127.0.0.1:1"}, KafkaTopicConfig{}, "private-init-test"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing setup error: %v", err)
	}

}

func TestKafkaStartupFailureStopsBusiness(t *testing.T) {
	cfg := KafkaTopicConfig{SetupTimeout: 30 * time.Millisecond}
	p, err := NewKafkaPublisher(KafkaPublisherConfig{Brokers: []string{"127.0.0.1:1"}, TopicConfig: cfg}, nil)
	if err == nil || p != nil {
		t.Fatalf("publisher started without Kafka topic guarantees: %v", err)
	}
	w := NewWorker(WorkerConfig{KafkaBrokers: []string{"127.0.0.1:1"}, TopicConfig: cfg}, nil, nil, nil)
	if err = w.Run(t.Context()); err == nil || w.telemetryReader != nil || w.commandReader != nil {
		t.Fatalf("worker started despite initialization failure: %v", err)
	}
	b := NewMQTTBridge(MQTTBridgeConfig{BrokerURL: "tcp://127.0.0.1:1", KafkaBrokers: []string{"127.0.0.1:1"}, DLQTopicConfig: cfg}, noopPublisher{}, nil)
	if err = b.Run(t.Context()); err == nil || b.client != nil {
		t.Fatalf("bridge started despite DLQ initialization failure: %v", err)
	}
}

func TestNearLimitAcceptedCommandPublishesToRealKafka(t *testing.T) {
	s := isolatedPostgresStore(t)
	brokers, topic := regressionKafkaTopic(t)
	_, telemetry := regressionKafkaTopic(t)
	p, err := NewKafkaPublisher(KafkaPublisherConfig{Brokers: brokers, TelemetryTopic: telemetry, CommandTopic: topic}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.commandWriter.AllowAutoTopicCreation || p.telemetryWriter.AllowAutoTopicCreation {
		t.Fatal("producer may auto-create unmanaged topics")
	}
	payload := json.RawMessage(`{"v":"` + strings.Repeat("x", maxCommandValueBytes-1024) + `"}`)
	c, err := s.CreateCommandWithKey(t.Context(), "t", "d", "near-limit", payload)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimCommandsForDispatch(1, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err = p.PublishCommand(ctx, claimed[0]); err != nil {
		t.Fatalf("accepted command undeliverable: %v", err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 2 << 20})
	defer reader.Close()
	m, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got Command
	if err = json.Unmarshal(m.Value, &got); err != nil || got.ID != c.ID || len(got.Payload) != len(payload) {
		t.Fatalf("command changed: err=%v", err)
	}
	// A short exponent form can expand past capacity when read back from JSONB.
	expanding := json.RawMessage(`[` + strings.TrimSuffix(strings.Repeat("1e131071,", 9), ",") + `]`)
	if _, err = s.CreateCommandWithKey(t.Context(), "t", "d", "expansion", expanding); err == nil {
		t.Fatal("JSONB expansion bypassed capacity gate")
	}
	var n int
	if err = s.db.QueryRow(`SELECT count(*) FROM commands WHERE idempotency_key='expansion'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("expanding command committed: %d %v", n, err)
	}
}

func TestJSONBValidationMatchesStorageLimits(t *testing.T) {
	s := isolatedPostgresStore(t)
	for _, raw := range []string{`{"v":"\\u0000"}`, `{"v":"\ud83d\ude00"}`, `{"v":1e131071}`, `{"v":1e-16383}`, `{"v":1e131072}`, `{"v":1e-16384}`, `{"v":"\ud800"}`, `{"v":"\udc00"}`, `{"v":"\u0000"}`, `{"v":"\"\\"}`, `{"v":123456789012345678901234567890.123456789}`} {
		var stored []byte
		pgErr := s.db.QueryRow(`SELECT $1::jsonb`, raw).Scan(&stored)
		err := contracts.ValidateJSONB(json.RawMessage(raw))
		if (err == nil) != (pgErr == nil) {
			t.Errorf("raw=%q validator=%v PG=%v", raw, err, pgErr)
		}
	}
}
