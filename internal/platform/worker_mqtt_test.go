package platform

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type blockedAckStore struct {
	Repository
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockedAckStore) AckCommandContext(ctx context.Context, _, _, _ string) (Command, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return Command{}, nil
	case <-ctx.Done():
		return Command{}, ctx.Err()
	}
}

// A slow ACK database call must not block processing PUBACK for an unrelated
// downlink on the same MQTT connection. The broker below speaks real MQTT so
// this exercises the callback/router interaction, rather than option values.
func TestWorkerSlowAckDoesNotBlockMQTTPublish(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	brokerErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			brokerErr <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err = packets.ReadPacket(conn); err == nil {
			err = packets.NewControlPacket(packets.Connack).Write(conn)
		}
		if err != nil {
			brokerErr <- err
			return
		}
		packet, err := packets.ReadPacket(conn)
		if err != nil {
			brokerErr <- err
			return
		}
		suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
		suback.MessageID = packet.(*packets.SubscribePacket).MessageID
		suback.ReturnCodes = []byte{1}
		if err = suback.Write(conn); err != nil {
			brokerErr <- err
			return
		}
		ack := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		ack.TopicName = "tenant/t1/device/d1/ack"
		ack.Payload = []byte(`{"commandId":"blocked","tenantId":"t1","deviceId":"d1","status":"acked"}`)
		if err = ack.Write(conn); err != nil {
			brokerErr <- err
			return
		}
		packet, err = packets.ReadPacket(conn)
		if err != nil {
			brokerErr <- err
			return
		}
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.(*packets.PublishPacket).MessageID
		// More incoming ACKs arrive while the first callback is blocked. This
		// fills the router pipeline before the outgoing command's PUBACK.
		for i := 0; i < 4; i++ {
			if err = ack.Write(conn); err != nil {
				brokerErr <- err
				return
			}
		}
		brokerErr <- puback.Write(conn)
		// Keep the connection alive until the client completes the test.
		_, _ = packets.ReadPacket(conn)
	}()
	store := &blockedAckStore{entered: make(chan struct{}), release: make(chan struct{})}
	worker := NewWorker(WorkerConfig{MQTTBrokerURL: "tcp://" + listener.Addr().String()}, store, nil, nil)
	defer worker.mqtt.Disconnect(100)
	defer close(store.release)
	connect := worker.mqtt.Connect()
	if !connect.WaitTimeout(2*time.Second) || connect.Error() != nil {
		t.Fatalf("connect: %v", connect.Error())
	}
	select {
	case <-store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("ACK callback did not reach store")
	}
	publish := worker.mqtt.Publish("tenant/t1/device/d2/command", 1, false, []byte(`{"id":"other"}`))
	if !publish.WaitTimeout(time.Second) {
		t.Fatal("slow ACK blocked MQTT PUBACK for another command")
	}
	if err := publish.Error(); err != nil {
		t.Fatal(err)
	}
	if err := <-brokerErr; err != nil {
		t.Fatal(err)
	}
}
