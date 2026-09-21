package platform

import (
	"encoding/json"
	"time"

	"iot/internal/contracts"
)

type Tenant struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Device struct {
	TenantID  string    `json:"tenantId"`
	DeviceID  string    `json:"deviceId"`
	ProductID string    `json:"productId"`
	Secret    string    `json:"secret,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

type DeviceStatus struct {
	TenantID   string    `json:"tenantId"`
	DeviceID   string    `json:"deviceId"`
	Online     bool      `json:"online"`
	LastSeenAt time.Time `json:"lastSeenAt"`
}

type TelemetryRecord struct {
	MsgID    string          `json:"msgId"`
	TenantID string          `json:"tenantId"`
	DeviceID string          `json:"deviceId"`
	Ts       int64           `json:"ts"`
	Type     string          `json:"type"`
	Version  string          `json:"version"`
	Payload  json.RawMessage `json:"payload"`
	// TDengineWritten is an internal sink-completion marker used only for DLQ
	// replay compensation; it must not be exposed on the management API.
	TDengineWritten bool      `json:"-"`
	ReceivedAt      time.Time `json:"receivedAt"`
}

type Command struct {
	ID               string                  `json:"id"`
	TenantID         string                  `json:"tenantId"`
	DeviceID         string                  `json:"deviceId"`
	Status           contracts.CommandStatus `json:"status"`
	Payload          json.RawMessage         `json:"payload"`
	CreatedAt        time.Time               `json:"createdAt"`
	UpdatedAt        time.Time               `json:"updatedAt"`
	DispatchAttempts int                     `json:"dispatchAttempts"`
	DeadlineAt       time.Time               `json:"deadlineAt,omitempty"`
}

const (
	CommandStatusCreated   = contracts.CommandStatusCreated
	CommandStatusPublished = contracts.CommandStatusPublished
	CommandStatusSent      = contracts.CommandStatusSent
	CommandStatusAcked     = contracts.CommandStatusAcked
	CommandStatusFailed    = contracts.CommandStatusFailed
	CommandStatusTimeout   = contracts.CommandStatusTimeout
)
