package contracts

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

// Limits are UTF-8 bytes, matching TDengine VARCHAR storage, not rune counts.
const (
	TelemetryFieldMaxBytes   = 64
	TelemetryVersionMaxBytes = 32
)

func ValidateStorageField(name, value string, maxBytes int) error {
	if value == "" || len(value) > maxBytes || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s must contain 1..%d UTF-8 bytes", ErrInvalidEnvelope, name, maxBytes)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: %s contains control characters", ErrInvalidEnvelope, name)
		}
	}
	return nil
}

// ValidateTelemetryFields also applies to persisted records during compensation.
// It deliberately has no wall-clock check, so replay does not age out records.
func ValidateTelemetryFields(env Envelope) error {
	for _, field := range []struct {
		name, value string
		limit       int
	}{
		{"msgId", env.MsgID, TelemetryFieldMaxBytes},
		{"tenantId", env.TenantID, TelemetryFieldMaxBytes},
		{"deviceId", env.DeviceID, TelemetryFieldMaxBytes},
		{"type", env.Type, TelemetryFieldMaxBytes},
		{"version", env.Version, TelemetryVersionMaxBytes},
	} {
		if err := ValidateStorageField(field.name, field.value, field.limit); err != nil {
			return err
		}
	}
	if !IsValidTopicPart(env.TenantID) || !IsValidTopicPart(env.DeviceID) {
		return ErrInvalidEnvelope
	}
	return nil
}
