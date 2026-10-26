package contracts

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

func ValidateTenantName(value string) error {
	if strings.TrimSpace(value) == "" || len(value) > 256 || !utf8.ValidString(value) {
		return fmt.Errorf("%w: name must contain 1..256 UTF-8 bytes and not be blank", ErrInvalidPayload)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: name contains control characters", ErrInvalidPayload)
		}
	}
	return nil
}

func ValidateProductID(value string) error {
	if len(value) == 0 || len(value) > 64 {
		return fmt.Errorf("%w: productId must contain 1..64 ASCII identifier characters", ErrInvalidPayload)
	}
	for i, c := range []byte(value) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || i > 0 && (c == '.' || c == '_' || c == '-')) {
			return fmt.Errorf("%w: invalid productId", ErrInvalidPayload)
		}
	}
	return nil
}

func ValidateDeviceIdentity(tenant, device string) error {
	if !IsValidTenantID(tenant) || !IsValidTopicPart(device) {
		return fmt.Errorf("%w: valid tenantId and deviceId are required", ErrInvalidEnvelope)
	}
	return nil
}

func ValidateCommandID(id string) error {
	if !IsValidTopicPart(id) {
		return fmt.Errorf("%w: valid commandId is required", ErrInvalidEnvelope)
	}
	return nil
}

func ValidateCommandPayload(payload []byte) error {
	if len(payload) == 0 {
		return nil
	} // An omitted argument object is explicitly empty.
	if err := ValidateJSONB(payload); err != nil {
		return err
	}
	if err := ValidateUniqueJSON(payload); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	if trimmed := strings.TrimSpace(string(payload)); len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("%w: command payload must be an object", ErrInvalidPayload)
	}
	return nil
}
