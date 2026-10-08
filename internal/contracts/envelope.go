package contracts

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var ErrInvalidEnvelope = errors.New("invalid envelope")

// ErrEnvelopeTimestampOutOfRange marks a telemetry timestamp that cannot be
// real: before 2000-01-01, or further ahead than the tolerated device clock
// skew. It wraps ErrInvalidEnvelope, so existing errors.Is checks and DLQ
// handling keep working while the recorded DLQ error says what was wrong.
var ErrEnvelopeTimestampOutOfRange = fmt.Errorf("%w: ts out of range", ErrInvalidEnvelope)

const (
	// minEnvelopeTsMillis is 2000-01-01T00:00:00Z in milliseconds. Besides
	// rejecting garbage, it turns the common "device sent seconds instead of
	// milliseconds" bug into an explicit rejection instead of silently writing
	// 1970 timestamps into the time-series store.
	minEnvelopeTsMillis int64 = 946684800000
	// maxEnvelopeClockSkew tolerates devices whose clock runs slightly ahead of
	// the server, without letting a badly set clock pin a device to the top of
	// every time-ordered query indefinitely.
	maxEnvelopeClockSkew = 5 * time.Minute
)

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

// ValidateEnvelope checks the envelope contract at ingest time (MQTT bridge and
// REST ingestion). It is intentionally not applied again when a message is
// consumed from Kafka or replayed from the DLQ, so a historic message stays
// acceptable no matter how old it is by then.
func ValidateEnvelope(env Envelope) error {
	return validateEnvelopeAt(env, time.Now())
}

// validateEnvelopeAt is ValidateEnvelope with an explicit clock, so the
// timestamp bounds can be exercised without depending on wall-clock time.
func validateEnvelopeAt(env Envelope, now time.Time) error {
	if env.MsgID == "" || env.TenantID == "" || env.DeviceID == "" || env.Type == "" || env.Version == "" || env.Ts <= 0 {
		return ErrInvalidEnvelope
	}
	if !IsValidTopicPart(env.TenantID) || !IsValidTopicPart(env.DeviceID) {
		return ErrInvalidEnvelope
	}
	if env.Ts < minEnvelopeTsMillis || env.Ts > now.Add(maxEnvelopeClockSkew).UnixMilli() {
		return ErrEnvelopeTimestampOutOfRange
	}
	return nil
}
