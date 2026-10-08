package contracts

import (
	"encoding/json"
	"time"
)

// CommandResponse is the REST representation of a command.
//
// It exists because platform.Command is not a wire type: that struct also
// carries dispatcher bookkeeping (DispatchAttempts, DeadlineAt) which the Kafka
// event and the dispatch loop need, but which are not part of the management
// API contract. Marshalling platform.Command directly in a response leaked
// those fields to clients as always-zero values (dispatchAttempts: 0,
// deadlineAt: year 1), because the protobuf message the gateway maps from does
// not populate them. Keeping one definition here means both REST surfaces
// (internal/adminapi and the platform.App test harness) expose the same shape.
type CommandResponse struct {
	ID        string          `json:"id"`
	TenantID  string          `json:"tenantId"`
	DeviceID  string          `json:"deviceId"`
	Status    CommandStatus   `json:"status"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
}
