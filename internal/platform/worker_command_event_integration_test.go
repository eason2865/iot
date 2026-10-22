package platform

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
)

type rejectedPublishToken struct{ mqtt.Token }

func (rejectedPublishToken) WaitTimeout(time.Duration) bool { return true }
func (rejectedPublishToken) Error() error                   { return errors.New("injected MQTT outage") }

type rejectedPublishClient struct {
	mqtt.Client
	calls atomic.Int64
}

func (c *rejectedPublishClient) Publish(string, byte, bool, interface{}) mqtt.Token {
	c.calls.Add(1)
	return rejectedPublishToken{}
}

func TestCommandEventAttemptsCannotCrashOrControlBackoff(t *testing.T) {
	for _, attempt := range []int{-1, 99} {
		t.Run(fmt.Sprint(attempt), func(t *testing.T) {
			s := isolatedPostgresStore(t)
			brokers, topic := regressionKafkaTopic(t)
			_, dlq := regressionKafkaTopic(t)
			cmd, err := s.CreateCommand("t", "d", json.RawMessage(`{"switch":"on"}`))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec(`UPDATE commands SET status='published',dispatch_attempts=2 WHERE id=$1`, cmd.ID); err != nil {
				t.Fatal(err)
			}
			cmd.DispatchAttempts = attempt
			cmd.Status = CommandStatusPublished
			raw, err := json.Marshal(cmd)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			writer := newDeadLetterWriter(brokers, topic)
			defer writer.Close()
			if err = writer.WriteMessages(ctx, kafka.Message{Value: raw}); err != nil {
				t.Fatal(err)
			}
			reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: topic + "-worker", StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 2 << 20})
			defer reader.Close()
			defer cancel()
			dlqWriter := newDeadLetterWriter(brokers, dlq)
			defer dlqWriter.Close()
			mqttClient := &rejectedPublishClient{}
			worker := &Worker{store: s, mqtt: mqttClient, commandReader: reader, dlqWriter: dlqWriter}
			done := make(chan error, 1)
			go func() {
				defer func() {
					if p := recover(); p != nil {
						done <- fmt.Errorf("consumer panic: %v", p)
					}
				}()
				done <- worker.consumeCommands(ctx)
			}()
			dlqReader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: dlq, Partition: 0, MinBytes: 1, MaxBytes: 2 << 20})
			defer dlqReader.Close()
			defer cancel()
			type result struct {
				message kafka.Message
				err     error
			}
			received := make(chan result, 1)
			go func() { m, e := dlqReader.ReadMessage(ctx); received <- result{m, e} }()
			select {
			case err := <-done:
				t.Fatalf("consumer stopped before quarantining event: %v", err)
			case r := <-received:
				if r.err != nil {
					t.Fatal(r.err)
				}
				var letter DeadLetter
				if err = json.Unmarshal(r.message.Value, &letter); err != nil {
					t.Fatal(err)
				}
				wantStage := StageCommandMQTT
				if attempt < 0 {
					wantStage = StageCommandDecode
				}
				if letter.Stage != wantStage || letter.ValueBase64 != base64.StdEncoding.EncodeToString(raw) {
					t.Fatalf("incorrect DLQ: %+v", letter)
				}
			}
			var state string
			var delay float64
			if err = s.db.QueryRow(`SELECT status,EXTRACT(EPOCH FROM (next_dispatch_at-updated_at)) FROM commands WHERE id=$1`, cmd.ID).Scan(&state, &delay); err != nil {
				t.Fatal(err)
			}
			if attempt < 0 {
				if mqttClient.calls.Load() != 0 || state != "published" {
					t.Fatalf("invalid event delivered: calls=%d state=%s", mqttClient.calls.Load(), state)
				}
			} else if mqttClient.calls.Load() != 1 || state != "created" || delay < 3.9 || delay > 4.1 {
				t.Fatalf("backoff must use DB attempt=2: calls=%d state=%s seconds=%f", mqttClient.calls.Load(), state, delay)
			}
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("consumer did not stop")
			}
		})
	}
}
