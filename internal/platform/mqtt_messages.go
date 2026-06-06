package platform

import (
	"encoding/json"
	"time"

	"iot/internal/contracts"
)

type CommandDownlink struct {
	ID        string                  `json:"id"`
	TenantID  string                  `json:"tenantId"`
	DeviceID  string                  `json:"deviceId"`
	Status    contracts.CommandStatus `json:"status"`
	Payload   json.RawMessage         `json:"payload"`
	CreatedAt time.Time               `json:"createdAt"`
	UpdatedAt time.Time               `json:"updatedAt"`
}

type CommandAckMessage struct {
	CommandID string          `json:"commandId"`
	TenantID  string          `json:"tenantId"`
	DeviceID  string          `json:"deviceId"`
	Status    string          `json:"status,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}
