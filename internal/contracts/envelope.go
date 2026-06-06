package contracts

import (
	"encoding/json"
	"errors"
	"fmt"
)

var ErrInvalidEnvelope = errors.New("invalid envelope")

type Envelope struct {
	MsgID     string          `json:"msgId"`
	TenantID  string          `json:"tenantId"`
	DeviceID  string          `json:"deviceId"`
	Ts        int64           `json:"ts"`
	Type      string          `json:"type"`
	Version   string          `json:"version"`
	TraceID   string          `json:"traceId,omitempty"`
	ProductID string          `json:"productId,omitempty"`
	Region    string          `json:"region,omitempty"`
	Seq       int64           `json:"seq,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

func ParseEnvelope(raw []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrInvalidEnvelope, err)
	}
	if err := ValidateEnvelope(env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

func ValidateEnvelope(env Envelope) error {
	if env.MsgID == "" || env.TenantID == "" || env.DeviceID == "" || env.Type == "" || env.Version == "" || env.Ts <= 0 {
		return ErrInvalidEnvelope
	}
	return nil
}
