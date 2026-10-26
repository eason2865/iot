package contracts

import (
	"encoding/json"
	"fmt"
	"strings"
)

type CommandResult struct {
	Status    CommandStatus   `json:"status"`
	ErrorCode string          `json:"errorCode,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

func NormalizeCommandResult(result CommandResult) (CommandResult, error) {
	if result.Status != CommandStatusAcked && result.Status != CommandStatusFailed {
		return result, fmt.Errorf("%w: acknowledgement status must be acked or failed", ErrInvalidPayload)
	}
	if len(result.ErrorCode) > 64 || strings.TrimSpace(result.ErrorCode) != result.ErrorCode {
		return result, fmt.Errorf("%w: invalid errorCode", ErrInvalidPayload)
	}
	if result.ErrorCode != "" {
		if err := ValidateStorageField("errorCode", result.ErrorCode, 64); err != nil {
			return result, fmt.Errorf("%w: invalid errorCode", ErrInvalidPayload)
		}
	}
	if result.Status == CommandStatusAcked && result.ErrorCode != "" {
		return result, fmt.Errorf("%w: successful acknowledgement cannot have an errorCode", ErrInvalidPayload)
	}
	if len(result.Payload) == 0 {
		result.Payload = json.RawMessage(`{}`)
	}
	if len(result.Payload) > 64<<10 {
		return result, fmt.Errorf("%w: acknowledgement payload exceeds 64KiB", ErrInvalidPayload)
	}
	if err := validateJSONBBudget(result.Payload, 64<<10); err != nil {
		return result, err
	}
	if err := ValidateCommandPayload(result.Payload); err != nil {
		return result, err
	}
	return result, nil
}
