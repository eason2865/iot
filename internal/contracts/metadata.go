package contracts

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// A single metadata field must also fit a bounded list page. SQL TEXT itself
// has a much larger limit and cannot represent NUL characters.
func ValidateMetadataText(value string) error {
	if len(value) > MaxJSONBBytes || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return fmt.Errorf("%w: metadata must be UTF-8 without NUL and at most %d bytes", ErrInvalidPayload, MaxJSONBBytes)
	}
	return nil
}
