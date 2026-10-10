package main

import (
	"context"
	"encoding/json"
	"fmt"
	"iot/internal/contracts"
	"os"
	"strings"
	"testing"
	"time"

	"iot/internal/platform"
)

// TestReplayGroupIDIsPerStage pins the property that makes targeted replays safe.
// With one shared group, committing any matching record moves the group position
// past every earlier record, so a later run for a different stage would never see
// them; kafka-go only applies StartOffset when the group has no committed offset.
func TestReplayGroupIDIsPerStage(t *testing.T) {
	if got := replayGroupID(""); got != "iot-dlq-replay-all" {
		t.Fatalf("replayGroupID(\"\") = %q, want the all-stages group", got)
	}

	seen := map[string]string{}
	for _, stage := range platform.DeadLetterStages() {
		group := replayGroupID(stage)
		if group == "iot-dlq-replay-all" {
			t.Fatalf("stage %q must not share the all-stages group", stage)
		}
		if !strings.Contains(group, stage) {
			t.Fatalf("group %q should name its stage %q", group, stage)
		}
		if other, ok := seen[group]; ok {
			t.Fatalf("stages %q and %q share the group %q", other, stage, group)
		}
		seen[group] = stage
	}
}

func TestMQTTDeadLetterReplayTargetsKafkaTelemetry(t *testing.T) {
	item := platform.DeadLetter{Stage: platform.StageMQTTKafka, SourceTopic: "tenant/t/device/d/telemetry"}
	value := []byte(`{"msgId":"m","tenantId":"t","deviceId":"d","ts":1700000000000,"type":"telemetry","version":"v1","payload":{"x":1}}`)
	topic, payload, err := replayKafkaPayload(item, value)
	if err != nil || topic != "iot.telemetry" {
		t.Fatalf("topic=%s err=%v", topic, err)
	}
	var rec platform.TelemetryRecord
	if err := json.Unmarshal(payload, &rec); err != nil || rec.MsgID != "m" {
		t.Fatalf("converted record: %s err=%v", payload, err)
	}
	item.SourceTopic = "tenant/other/device/d/telemetry"
	if _, _, err := replayKafkaPayload(item, value); err == nil {
		t.Fatal("foreign identity was replayed")
	}
}

type replayAckRecorder struct{ id, tenant, device string }

func (s *replayAckRecorder) AckCommandContext(_ context.Context, id, tenant, device string) (platform.Command, error) {
	s.id, s.tenant, s.device = id, tenant, device
	return platform.Command{}, nil
}
func TestAckReplayAppliesStateWithoutCommandPublish(t *testing.T) {
	store := &replayAckRecorder{}
	item := platform.DeadLetter{Stage: platform.StageCommandAck, SourceTopic: "tenant/t/device/d/ack"}
	value := []byte(`{"commandId":"cmd","tenantId":"t","deviceId":"d"}`)
	if err := replayAck(context.Background(), item, value, store); err != nil {
		t.Fatal(err)
	}
	if store.id != "cmd" || store.tenant != "t" || store.device != "d" {
		t.Fatalf("wrong ACK: %+v", store)
	}
	item.SourceTopic = "tenant/other/device/d/ack"
	if err := replayAck(context.Background(), item, value, store); err == nil {
		t.Fatal("foreign ACK was replayed")
	}
}

func TestE2EAckReplayPostgres(t *testing.T) {
	if os.Getenv("IOT_E2E") == "" {
		t.Skip("set IOT_E2E=1")
	}
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://iot:iot123@localhost:5432/iot?sslmode=disable"
	}
	store, err := platform.NewPostgresStore(dsn, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tenant := fmt.Sprintf("ack-replay-%d", time.Now().UnixNano())
	if _, err := store.CreateTenant(platform.Tenant{ID: tenant, Name: tenant}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateDevice(platform.Device{TenantID: tenant, DeviceID: "d", ProductID: "p", Secret: "s"}); err != nil {
		t.Fatal(err)
	}
	cmd, err := store.CreateCommand(tenant, "d", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkCommandSent(cmd.ID, time.Now().Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	item := platform.DeadLetter{Stage: platform.StageCommandAck, SourceTopic: "tenant/" + tenant + "/device/d/ack"}
	value, _ := json.Marshal(platform.CommandAckMessage{CommandID: cmd.ID, TenantID: tenant, DeviceID: "d"})
	for i := 0; i < 2; i++ {
		if err := replayAck(context.Background(), item, value, store); err != nil {
			t.Fatal(err)
		}
	}
	got, ok := store.GetCommand(cmd.ID)
	if !ok || got.Status != contracts.CommandStatusAcked {
		t.Fatalf("ACK replay did not persist: %+v", got)
	}
}
