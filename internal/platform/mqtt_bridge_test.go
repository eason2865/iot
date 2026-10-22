package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type blockingBridgePublisher struct{ entered, release chan struct{} }

func (p *blockingBridgePublisher) PublishTelemetry(context.Context, TelemetryRecord) error {
	close(p.entered)
	<-p.release
	return nil
}
func (*blockingBridgePublisher) PublishCommand(context.Context, Command) error { return nil }

func TestBridgeDoesNotAcknowledgeBeforeKafkaPersistence(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	pub := &blockingBridgePublisher{entered: make(chan struct{}), release: make(chan struct{})}
	bridge := NewMQTTBridge(MQTTBridgeConfig{BrokerURL: "tcp://" + listener.Addr().String(), ClientID: "bridge-test"}, pub, nil)
	brokerResult := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			brokerResult <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err = packets.ReadPacket(conn); err == nil {
			err = packets.NewControlPacket(packets.Connack).Write(conn)
		}
		if err != nil {
			brokerResult <- err
			return
		}
		packet, err := packets.ReadPacket(conn)
		if err != nil {
			brokerResult <- err
			return
		}
		suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
		suback.MessageID = packet.(*packets.SubscribePacket).MessageID
		suback.ReturnCodes = []byte{1}
		if err = suback.Write(conn); err != nil {
			brokerResult <- err
			return
		}
		message := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		message.Qos = 1
		message.MessageID = 42
		message.TopicName = "tenant/t/device/d/telemetry"
		message.Payload, _ = json.Marshal(map[string]any{"msgId": "m", "tenantId": "t", "deviceId": "d", "ts": time.Now().UnixMilli(), "type": "telemetry", "version": "v1", "payload": map[string]any{}})
		if err = message.Write(conn); err != nil {
			brokerResult <- err
			return
		}
		<-pub.entered
		_ = conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
		packet, err = packets.ReadPacket(conn)
		if err == nil {
			brokerResult <- fmt.Errorf("received %T before Kafka persisted the telemetry", packet)
			close(pub.release)
			return
		}
		if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
			brokerResult <- err
			close(pub.release)
			return
		}
		close(pub.release)
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		packet, err = packets.ReadPacket(conn)
		if err == nil {
			if ack, ok := packet.(*packets.PubackPacket); !ok || ack.MessageID != 42 {
				err = fmt.Errorf("expected PUBACK 42, got %v", packet)
			}
		}
		brokerResult <- err
		_, _ = packets.ReadPacket(conn)
	}()
	token := bridge.client.Connect()
	if !token.WaitTimeout(time.Second) || token.Error() != nil {
		t.Fatalf("connect: %v", token.Error())
	}
	defer bridge.client.Disconnect(100)
	if err := <-brokerResult; err != nil {
		t.Fatal(err)
	}
}

func TestBridgeRejectsQoSZeroSubscription(t *testing.T) {
	for _, prefix := range []string{"", "$share/iot-telemetry/"} {
		t.Run(prefix, func(t *testing.T) { testBridgeRejectsQoSZeroSubscription(t, prefix) })
	}
}

func testBridgeRejectsQoSZeroSubscription(t *testing.T, prefix string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err = packets.ReadPacket(conn); err != nil {
			return
		}
		if err = packets.NewControlPacket(packets.Connack).Write(conn); err != nil {
			return
		}
		packet, err := packets.ReadPacket(conn)
		if err != nil {
			return
		}
		ack := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
		ack.MessageID = packet.(*packets.SubscribePacket).MessageID
		ack.ReturnCodes = []byte{0}
		_ = ack.Write(conn)
		_, _ = packets.ReadPacket(conn)
	}()
	bridge := NewMQTTBridge(MQTTBridgeConfig{BrokerURL: "tcp://" + listener.Addr().String(), TopicFilter: prefix + "tenant/+/device/+/telemetry"}, noopPublisher{}, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if err = bridge.Run(ctx); err == nil || !strings.Contains(err.Error(), "requires QoS 1") {
		t.Fatalf("expected fatal QoS 0 rejection, got %v", err)
	}
}

type bridgeMessage struct {
	payload []byte
	acked   atomic.Int32
}

func (*bridgeMessage) Duplicate() bool   { return false }
func (*bridgeMessage) Qos() byte         { return 1 }
func (*bridgeMessage) Retained() bool    { return false }
func (*bridgeMessage) Topic() string     { return "tenant/t/device/d/telemetry" }
func (*bridgeMessage) MessageID() uint16 { return 42 }
func (m *bridgeMessage) Payload() []byte { return m.payload }
func (m *bridgeMessage) Ack()            { m.acked.Add(1) }
func telemetryBridgeMessage(t *testing.T) *bridgeMessage {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"msgId": "durable-m", "tenantId": "t", "deviceId": "d", "ts": time.Now().UnixMilli(), "type": "telemetry", "version": "v1", "payload": map[string]any{"temperature": 23}})
	if err != nil {
		t.Fatal(err)
	}
	return &bridgeMessage{payload: payload}
}

type failedBridgePublisher struct{}

func (failedBridgePublisher) PublishTelemetry(context.Context, TelemetryRecord) error {
	return errors.New("Kafka unavailable")
}
func (failedBridgePublisher) PublishCommand(context.Context, Command) error {
	return errors.New("Kafka unavailable")
}

func TestBridgeInboxSurvivesKafkaAndDLQFailure(t *testing.T) {
	store := isolatedPostgresStore(t)
	bridge := NewMQTTBridge(MQTTBridgeConfig{BrokerURL: "tcp://127.0.0.1:1", Inbox: store}, failedBridgePublisher{}, nil)
	defer bridge.stop()
	msg := telemetryBridgeMessage(t)
	bridge.processMessage(msg)
	if msg.acked.Load() != 1 {
		t.Fatalf("durable inbox was not acknowledged: %d", msg.acked.Load())
	}
	rows, _, err := store.ListUnwrittenTelemetry(t.Context(), time.Now().Add(time.Minute), 0, []string{"t"}, 10)
	if err != nil || len(rows) != 1 || rows[0].MsgID != "durable-m" {
		t.Fatalf("message not available for compensation: %+v %v", rows, err)
	}
	select {
	case err := <-bridge.errCh:
		t.Fatalf("durable message incorrectly failed ingestor: %v", err)
	default:
	}
}

func TestBridgeDoesNotAckWhenNoSinkIsDurable(t *testing.T) {
	bridge := NewMQTTBridge(MQTTBridgeConfig{BrokerURL: "tcp://127.0.0.1:1"}, failedBridgePublisher{}, nil)
	defer bridge.stop()
	msg := telemetryBridgeMessage(t)
	bridge.processMessage(msg)
	if msg.acked.Load() != 0 {
		t.Fatal("message acknowledged with neither Kafka nor DLQ persistence")
	}
	select {
	case <-bridge.errCh:
	default:
		t.Fatal("undurable delivery did not fail ingestor")
	}
}

func TestBridgeShutdownDrainsPendingPublish(t *testing.T) {
	pub := &blockingBridgePublisher{entered: make(chan struct{}), release: make(chan struct{})}
	bridge := NewMQTTBridge(MQTTBridgeConfig{BrokerURL: "tcp://127.0.0.1:1"}, pub, nil)
	msg := telemetryBridgeMessage(t)
	bridge.pending.Add(1)
	go func() { defer bridge.pending.Done(); bridge.processMessage(msg) }()
	<-pub.entered
	stopped := make(chan struct{})
	go func() { bridge.stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("stop returned before pending publish completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(pub.release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("drain never completed")
	}
	if msg.acked.Load() != 1 {
		t.Fatal("drained message was not acknowledged")
	}
}
