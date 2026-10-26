package platform

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"iot/internal/contracts"
)

// Business records share a 1 MiB writer/topic floor. Reserve transport framing,
// the key and future metadata; measure the full command, not only its payload.
const businessBatchBytes = 1 << 20
const maxCommandValueBytes = businessBatchBytes - 1024

func ValidateCommandInput(tenant, device string, payload json.RawMessage) error {
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if err := contracts.ValidateCommandPayload(payload); err != nil {
		return err
	}
	// Reserve the longest dispatcher state, timestamp precision and attempt value.
	stamp := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	value, err := json.Marshal(Command{ID: strings.Repeat("x", 36), TenantID: tenant, DeviceID: device, Payload: payload, Status: CommandStatusPublished, CreatedAt: stamp, UpdatedAt: stamp, DeadlineAt: stamp, DispatchAttempts: 2147483647})
	if err != nil {
		return err
	}
	if len(value) > maxCommandValueBytes {
		return fmt.Errorf("%w: encoded command exceeds %d bytes", contracts.ErrInvalidPayload, maxCommandValueBytes)
	}
	return nil
}

func businessTopicConfig(cfg KafkaTopicConfig) KafkaTopicConfig {
	cfg.MinMessageBytes = max(cfg.MinMessageBytes, businessBatchBytes)
	return cfg
}
