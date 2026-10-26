package platform

import (
	"encoding/json"
	"errors"
	"testing"

	"iot/internal/contracts"
)

func TestMQTTFailedResultIsDurableAndIdempotent(t *testing.T) {
	s := isolatedPostgresStore(t)
	cmd, err := s.CreateCommand("t", "d", nil)
	if err != nil {
		t.Fatal(err)
	}
	worker := NewWorker(WorkerConfig{}, s, nil, nil)
	defer worker.acks.cancel()
	raw := []byte(`{"commandId":"` + cmd.ID + `","tenantId":"t","deviceId":"d","status":"failed","errorCode":"DEVICE_ERROR","payload":{"reason":"blocked"}}`)
	for i := 0; i < 2; i++ {
		msg := &countedAckMessage{fakeMQTTMessage: fakeMQTTMessage{topic: "tenant/t/device/d/ack", payload: raw}}
		worker.handleAckMessage(nil, msg)
		if msg.acks.Load() != 1 {
			t.Fatal("durable result not acknowledged")
		}
	}
	stored, err := s.GetCommandContext(t.Context(), cmd.ID)
	if err != nil || stored.Status != contracts.CommandStatusFailed || stored.Result == nil || stored.Result.ErrorCode != "DEVICE_ERROR" {
		t.Fatalf("result lost: %+v %v", stored, err)
	}
	var count int
	if err = s.db.QueryRow(`SELECT count(*) FROM command_ack WHERE command_id=$1 AND ack_status='failed' AND ack_payload->>'errorCode'='DEVICE_ERROR'`, cmd.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("ACK rows=%d err=%v", count, err)
	}
	_, err = s.AckCommandResultContext(t.Context(), cmd.ID, "t", "d", contracts.CommandResult{Status: contracts.CommandStatusAcked})
	if !errors.Is(err, ErrAcknowledgementConflict) {
		t.Fatalf("conflicting result accepted: %v", err)
	}
}

func TestDeviceResultAndAuditRollbackTogether(t *testing.T) {
	s := isolatedPostgresStore(t)
	cmd, err := s.CreateCommand("t", "d", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`ALTER TABLE command_events ADD CONSTRAINT refuse_result CHECK(event_type <> 'failed')`); err != nil {
		t.Fatal(err)
	}
	_, err = s.AckCommandResultContext(t.Context(), cmd.ID, "t", "d", contracts.CommandResult{Status: contracts.CommandStatusFailed, ErrorCode: "FAULT", Payload: json.RawMessage(`{"reason":"failure"}`)})
	if err == nil {
		t.Fatal("audit failure accepted")
	}
	stored, err := s.GetCommandContext(t.Context(), cmd.ID)
	if err != nil || stored.Status != contracts.CommandStatusCreated || stored.Result != nil {
		t.Fatalf("partial state committed: %+v %v", stored, err)
	}
	var count int
	if err = s.db.QueryRow(`SELECT count(*) FROM command_ack WHERE command_id=$1`, cmd.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial ACK committed: %d %v", count, err)
	}
}
