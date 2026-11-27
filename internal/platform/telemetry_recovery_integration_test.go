package platform

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"iot/internal/contracts"

	"github.com/segmentio/kafka-go"
)

func TestKafkaTelemetryRequiresOriginalReceiveTime(t *testing.T) {
	s := isolatedPostgresStore(t)
	brokers, topic := regressionKafkaTopic(t)
	_, dlqTopic := regressionKafkaTopic(t)
	received := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Microsecond)
	rec := TelemetryRecord{MsgID: "historical", TenantID: "t", DeviceID: "d", Ts: received.UnixMilli(), Type: "temp", Version: "v1", Payload: []byte(`{}`), ReceivedAt: received}
	value, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, RequiredAcks: kafka.RequireAll, BatchSize: 1}
	defer writer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	missing := []byte(`{"msgId":"missing","tenantId":"t","deviceId":"d","ts":1,"type":"temp","version":"v1","payload":{}}`)
	if err = writer.WriteMessages(ctx, kafka.Message{Value: missing}, kafka.Message{Value: value}); err != nil {
		t.Fatal(err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: topic, StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 10e6, MaxWait: 100 * time.Millisecond})
	defer func() {
		_ = reader.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := &kafka.Client{Addr: kafka.TCP(brokers...)}
		_, _ = client.DeleteGroups(cleanup, &kafka.DeleteGroupsRequest{GroupIDs: []string{topic}})
	}()
	dlqWriter := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: dlqTopic, RequiredAcks: kafka.RequireAll, BatchSize: 1}
	defer dlqWriter.Close()
	w := &Worker{store: s, telemetryReader: reader, dlqWriter: dlqWriter}
	done := make(chan error, 1)
	go func() { done <- w.consumeTelemetry(ctx) }()
	defer func() { cancel(); <-done }()
	dlq := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 10e6})
	defer dlq.Close()
	msg, err := dlq.ReadMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var item DeadLetter
	if err := json.Unmarshal(msg.Value, &item); err != nil {
		t.Fatal(err)
	}
	if item.Stage != StageTelemetryDecode || item.SourceTopic != topic || item.ValueBase64 != base64.StdEncoding.EncodeToString(missing) {
		t.Fatalf("record missing receivedAt was not retained in the decode DLQ: %+v", item)
	}
	var stored []TelemetryRecord
	for ctx.Err() == nil {
		stored, _, err = s.ListTelemetryPageContext(ctx, PageRequest{TenantID: "t", DeviceID: "d"})
		if err != nil {
			t.Fatal(err)
		}
		if len(stored) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(stored) != 1 || stored[0].MsgID != rec.MsgID || !stored[0].ReceivedAt.Equal(received) {
		t.Fatalf("historical receive time was replaced: %+v, want %s", stored, received)
	}
	status, err := s.GetDeviceStatusContext(ctx, "t", "d")
	if err != nil || status.Online || !status.LastSeenAt.Equal(received) {
		t.Fatalf("historical recovery refreshed device liveness: %+v, err=%v", status, err)
	}
}

func TestConcurrentTelemetryCannotRegressDeviceState(t *testing.T) {
	s := isolatedPostgresStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	key := time.Now().UnixNano()
	// Block the older transaction after its receive time has been captured but
	// before it reaches device_state. The newer transaction can commit first.
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`CREATE FUNCTION delay_old() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF NEW.msg_id='old' THEN PERFORM pg_advisory_xact_lock(%d); END IF; RETURN NEW; END $$;
 CREATE TRIGGER delay_old BEFORE INSERT ON telemetry_records FOR EACH ROW EXECUTE FUNCTION delay_old()`, key))
	if err != nil {
		t.Fatal(err)
	}
	lock, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err = lock.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, key); err != nil {
		t.Fatal(err)
	}
	env := contracts.Envelope{MsgID: "old", TenantID: "t", DeviceID: "d", Ts: time.Now().UnixMilli(), Type: "temp", Version: "v1", Payload: []byte(`{}`)}
	done := make(chan error, 1)
	go func() { _, err := s.RecordTelemetryContext(ctx, env); done <- err }()
	var waiting bool
	for ctx.Err() == nil {
		if err = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks
 WHERE locktype='advisory' AND classid=($1::bigint >> 32)::oid
 AND objid=($1::bigint & 4294967295)::oid AND NOT granted)`, key).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !waiting {
		t.Fatal("older transaction did not reach delay trigger")
	}
	newEnv := env
	newEnv.MsgID = "new"
	newer, err := s.RecordTelemetryContext(ctx, newEnv)
	if err != nil {
		t.Fatal(err)
	}
	if err = lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	var seen time.Time
	var msg string
	if err = s.db.QueryRowContext(ctx, `SELECT last_seen_at,last_msg_id FROM device_state WHERE tenant_id='t' AND device_id='d'`).Scan(&seen, &msg); err != nil {
		t.Fatal(err)
	}
	if msg != "new" || !seen.Equal(newer.ReceivedAt.Truncate(time.Microsecond)) {
		t.Fatalf("late older transaction regressed state: time=%s msg=%s, newer=%s", seen, msg, newer.ReceivedAt)
	}
}
