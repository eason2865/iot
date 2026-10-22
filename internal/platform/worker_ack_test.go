package platform

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/produce"
)

type countedAckMessage struct {
	fakeMQTTMessage
	acks atomic.Int32
}

func (m *countedAckMessage) Ack() { m.acks.Add(1) }
func validAckMessage() *countedAckMessage {
	return &countedAckMessage{fakeMQTTMessage: fakeMQTTMessage{topic: "tenant/t/device/d/ack", payload: []byte(`{"commandId":"cmd","tenantId":"t","deviceId":"d"}`)}}
}

type ackKafkaTransport struct {
	entered, release chan struct{}
	fail             bool
}

func (s *ackKafkaTransport) RoundTrip(ctx context.Context, _ net.Addr, request kafka.Request) (kafka.Response, error) {
	switch request := request.(type) {
	case *metadata.Request:
		return &metadata.Response{Brokers: []metadata.ResponseBroker{{NodeID: 1, Host: "unused", Port: 9092}}, Topics: []metadata.ResponseTopic{{Name: "ack-test", Partitions: []metadata.ResponsePartition{{PartitionIndex: 0, LeaderID: 1, ReplicaNodes: []int32{1}, IsrNodes: []int32{1}}}}}}, nil
	case *produce.Request:
		select {
		case s.entered <- struct{}{}:
		default:
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if s.fail {
			return nil, fmt.Errorf("Kafka unavailable")
		}
		return &produce.Response{Topics: []produce.ResponseTopic{{Topic: request.Topics[0].Topic, Partitions: []produce.ResponsePartition{{Partition: 0}}}}}, nil
	default:
		return nil, fmt.Errorf("unexpected Kafka request: %T", request)
	}
}

func TestWorkerConfirmsOnlyAfterDLQPersistence(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("fail=", fail), func(t *testing.T) {
			transport := &ackKafkaTransport{entered: make(chan struct{}, 1), release: make(chan struct{}), fail: fail}
			worker := NewWorker(WorkerConfig{}, &failingAckStore{}, nil, nil)
			worker.dlqWriter = &kafka.Writer{Addr: kafka.TCP("unused:9092"), Topic: "ack-test", BatchSize: 1, RequiredAcks: kafka.RequireAll, Transport: transport, MaxAttempts: 1}
			defer worker.dlqWriter.Close()
			defer worker.acks.cancel()
			msg := validAckMessage()
			done := make(chan struct{})
			go func() { worker.handleAckMessage(nil, msg); close(done) }()
			select {
			case <-transport.entered:
			case <-time.After(2 * time.Second):
				close(transport.release)
				<-done
				t.Fatal("DLQ publish not reached")
			}
			if msg.acks.Load() != 0 {
				t.Error("MQTT acknowledged before Kafka persisted the DLQ record")
			}
			close(transport.release)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("ACK callback did not complete")
			}
			if fail {
				if msg.acks.Load() != 0 {
					t.Fatal("failed DLQ was acknowledged")
				}
				select {
				case <-worker.acks.fatal:
				default:
					t.Fatal("double failure did not fail worker")
				}
			} else if msg.acks.Load() != 1 {
				t.Fatal("durable DLQ was not acknowledged exactly once")
			}
		})
	}
}

func TestWorkerConfirmsRejectedAckWithoutRetryLoop(t *testing.T) {
	for _, test := range []struct{ topic, payload string }{
		{"tenant/t/device/d/ack", "invalid"},
		{"tenant/foreign/device/d/ack", `{"commandId":"cmd","tenantId":"t","deviceId":"d"}`},
		{"tenant/t/device/d/ack", `{"tenantId":"t","deviceId":"d"}`},
		{"tenant/t/device/d/telemetry", `{"commandId":"cmd","tenantId":"t","deviceId":"d"}`},
	} {
		w := NewWorker(WorkerConfig{}, nil, nil, nil)
		msg := &countedAckMessage{fakeMQTTMessage: fakeMQTTMessage{topic: test.topic, payload: []byte(test.payload)}}
		w.handleAckMessage(nil, msg)
		w.acks.cancel()
		if msg.acks.Load() != 1 {
			t.Errorf("permanent invalid ACK not rejected: %+v", test)
		}
	}
}
