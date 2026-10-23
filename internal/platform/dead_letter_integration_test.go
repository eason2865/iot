package platform

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func regressionKafkaTopic(t *testing.T) ([]string, string) {
	t.Helper()
	brokers := strings.Split(os.Getenv("IOT_KAFKA_TEST_BROKERS"), ",")
	if brokers[0] == "" {
		t.Skip("set IOT_KAFKA_TEST_BROKERS to run Kafka regressions")
	}
	topic := fmt.Sprintf("iot-regression-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	controller, err := discoverKafkaController(ctx, brokers)
	if err != nil {
		t.Fatal(err)
	}
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	response, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Errors[topic]; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{topic}}); err != nil {
			t.Error(err)
		}
	})
	if err := waitForKafkaTopicMetadata(ctx, client, topic); err != nil {
		t.Fatal(err)
	}
	// Writers use the original bootstrap pool, while production reconciliation
	// uses the advertised Controller pool. IP/hostname aliases have independent
	// DefaultTransport caches: both must be ready before this fixture returns.
	controllerAddress := kafka.TCP(net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if controllerAddress.String() != client.Addr.String() {
		if err := waitForKafkaTopicMetadata(ctx, &kafka.Client{Addr: controllerAddress}, topic); err != nil {
			t.Fatal(err)
		}
	}
	return brokers, topic
}

func TestDeadLetterPreservesLargeOriginal(t *testing.T) {
	for _, kind := range []string{"ingestor", "worker"} {
		t.Run(kind, func(t *testing.T) {
			brokers, topic := regressionKafkaTopic(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			raw := make([]byte, 800<<10)
			if _, err := rand.Read(raw); err != nil {
				t.Fatal(err)
			}
			// The original fits the normal Kafka limit before DLQ base64 wrapping.
			ordinary := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, RequiredAcks: kafka.RequireAll, BatchSize: 1}
			if err := ordinary.WriteMessages(ctx, kafka.Message{Value: raw}); err != nil {
				t.Fatal(err)
			}
			_ = ordinary.Close()
			var writer *kafka.Writer
			var bridge *MQTTBridge
			if kind == "ingestor" {
				bridge = NewMQTTBridge(MQTTBridgeConfig{BrokerURL: "tcp://127.0.0.1:1", KafkaBrokers: brokers, DLQTopic: topic}, noopPublisher{}, nil)
				defer bridge.cancel()
				writer = bridge.dlqWriter
			} else {
				_, commandTopic := regressionKafkaTopic(t)
				worker := NewWorker(WorkerConfig{KafkaBrokers: brokers, TelemetryTopic: topic, CommandTopic: commandTopic, DLQTopic: topic}, nil, nil, nil)
				defer worker.acks.cancel()
				defer worker.telemetryReader.Close()
				defer worker.commandReader.Close()
				writer = worker.dlqWriter
			}
			defer writer.Close()
			original := kafka.Message{Topic: "tenant/t/device/d/telemetry", Key: []byte("d"), Value: raw, Partition: 2, Offset: 17}
			if err := publishDeadLetter(ctx, writer, original, StageMQTTDecode, errors.New("invalid JSON"), nil); err != nil {
				t.Fatalf("DLQ lost a Kafka-sized original after wrapping: %v", err)
			}
			reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 10e6, MaxWait: 100 * time.Millisecond})
			defer reader.Close()
			if _, err := reader.ReadMessage(ctx); err != nil {
				t.Fatal(err)
			}
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var item DeadLetter
			if err := json.Unmarshal(msg.Value, &item); err != nil {
				t.Fatal(err)
			}
			restored, err := base64.StdEncoding.DecodeString(item.ValueBase64)
			if err != nil || !bytes.Equal(restored, raw) || item.Stage != StageMQTTDecode || item.SourceOffset != 17 || item.SourcePartition != 2 || item.SourceTopic != original.Topic || item.Key != "d" {
				t.Fatalf("DLQ did not preserve complete original and metadata: decode=%v bytes=%d", err, len(restored))
			}
			if bridge != nil {
				// Exercise the actual invalid-MQTT path, which has no PG inbox copy.
				message := &bridgeMessage{payload: raw}
				bridge.processMessage(message)
				if message.acked.Load() != 1 {
					t.Fatal("durable invalid MQTT message was not acknowledged")
				}
				select {
				case err := <-bridge.errCh:
					t.Fatalf("large invalid message failed ingestor: %v", err)
				default:
				}
				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err = json.Unmarshal(msg.Value, &item); err != nil {
					t.Fatal(err)
				}
				restored, err := base64.StdEncoding.DecodeString(item.ValueBase64)
				if err != nil || !bytes.Equal(restored, raw) {
					t.Fatal("invalid MQTT original was not preserved")
				}
			}
		})
	}
}
