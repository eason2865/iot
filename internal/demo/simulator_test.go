package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"iot/internal/contracts"
	"iot/internal/platform"
)

func TestServiceSeedsTopologyAndEmitsTraffic(t *testing.T) {
	ctx := context.Background()
	admin := newRecordingAdmin()
	buses := newRecordingBusFactory()

	svc := NewService(Config{
		TenantCount:      2,
		DevicesPerTenant: 2,
		TenantPrefix:     "demo",
		ProductID:        "product-demo",
	}, admin, buses, rand.New(rand.NewSource(7)))

	if err := svc.EnsureTopology(ctx); err != nil {
		t.Fatalf("EnsureTopology() error = %v", err)
	}

	if got := len(admin.tenants); got != 2 {
		t.Fatalf("tenant seed count = %d, want 2", got)
	}
	if got := len(admin.devices); got != 4 {
		t.Fatalf("device seed count = %d, want 4", got)
	}

	if err := svc.EmitTelemetry(ctx); err != nil {
		t.Fatalf("EmitTelemetry() error = %v", err)
	}
	if err := svc.EmitCommand(ctx); err != nil {
		t.Fatalf("EmitCommand() error = %v", err)
	}

	if got := len(buses.publishCalls); got < 1 {
		t.Fatalf("publish calls = %d, want >= 1", got)
	}
	if got := len(admin.commands); got != 1 {
		t.Fatalf("command create count = %d, want 1", got)
	}

	tenantID := admin.tenants[0].id
	deviceID := admin.devices[0].deviceID
	downlinkTopic, err := contracts.BuildCommandTopic(tenantID, deviceID)
	if err != nil {
		t.Fatalf("BuildCommandTopic() error = %v", err)
	}
	agent := buses.busForTenant(tenantID)
	if agent == nil {
		t.Fatal("tenant bus not created")
	}

	downlink := platform.CommandDownlink{
		ID:       "cmd-123",
		TenantID: tenantID,
		DeviceID: deviceID,
	}
	payload, err := json.Marshal(downlink)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	agent.dispatch(downlinkTopic, payload)

	ackTopic, err := contracts.BuildAckTopic(tenantID, deviceID)
	if err != nil {
		t.Fatalf("BuildAckTopic() error = %v", err)
	}
	if !buses.hasPublish(ackTopic) {
		t.Fatalf("expected ACK publish on %s", ackTopic)
	}
	if !strings.Contains(string(buses.lastPublishPayload(ackTopic)), `"commandId":"cmd-123"`) {
		t.Fatalf("ack payload = %s, want commandId", string(buses.lastPublishPayload(ackTopic)))
	}
}

type recordingAdmin struct {
	tenants  []struct{ id, name string }
	devices  []struct{ tenantID, deviceID, productID string }
	commands []platform.Command
}

func newRecordingAdmin() *recordingAdmin { return &recordingAdmin{} }

func (r *recordingAdmin) CreateTenant(ctx context.Context, id, name string) error {
	r.tenants = append(r.tenants, struct{ id, name string }{id: id, name: name})
	return nil
}

func (r *recordingAdmin) CreateDevice(ctx context.Context, tenantID, deviceID, productID string) error {
	r.devices = append(r.devices, struct{ tenantID, deviceID, productID string }{tenantID: tenantID, deviceID: deviceID, productID: productID})
	return nil
}

func (r *recordingAdmin) CreateCommand(ctx context.Context, tenantID, deviceID string, payload json.RawMessage) (platform.Command, error) {
	cmd := platform.Command{ID: fmt.Sprintf("cmd-%d", len(r.commands)+1), TenantID: tenantID, DeviceID: deviceID, Payload: payload}
	r.commands = append(r.commands, cmd)
	return cmd, nil
}

type recordingBusFactory struct {
	buses        map[string]*recordingBus
	publishCalls []struct {
		topic   string
		payload []byte
	}
}

func newRecordingBusFactory() *recordingBusFactory {
	return &recordingBusFactory{buses: map[string]*recordingBus{}}
}

func (f *recordingBusFactory) NewClient(ctx context.Context, tenantID string) (Bus, error) {
	bus := &recordingBus{subscriptions: map[string]func(string, []byte){}}
	bus.onPublish = func(topic string, payload []byte) {
		f.publishCalls = append(f.publishCalls, struct {
			topic   string
			payload []byte
		}{topic: topic, payload: append([]byte(nil), payload...)})
	}
	f.buses[tenantID] = bus
	return bus, nil
}

func (f *recordingBusFactory) busForTenant(tenantID string) *recordingBus {
	return f.buses[tenantID]
}

func (f *recordingBusFactory) hasPublish(topic string) bool {
	for _, call := range f.publishCalls {
		if call.topic == topic {
			return true
		}
	}
	return false
}

func (f *recordingBusFactory) lastPublishPayload(topic string) []byte {
	for i := len(f.publishCalls) - 1; i >= 0; i-- {
		if f.publishCalls[i].topic == topic {
			return f.publishCalls[i].payload
		}
	}
	return nil
}

type recordingBus struct {
	subscriptions map[string]func(string, []byte)
	onPublish     func(topic string, payload []byte)
}

func (r *recordingBus) Publish(topic string, payload []byte) error {
	if r.onPublish != nil {
		r.onPublish(topic, payload)
	}
	return nil
}

func (r *recordingBus) Subscribe(topic string, handler func(string, []byte)) error {
	r.subscriptions[topic] = handler
	return nil
}

func (r *recordingBus) Close() error { return nil }

func (r *recordingBus) dispatch(topic string, payload []byte) {
	for pattern, handler := range r.subscriptions {
		if topicMatches(pattern, topic) {
			handler(topic, payload)
		}
	}
}
