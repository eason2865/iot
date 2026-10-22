package contracts

import (
	"strings"
	"testing"
)

func TestValidateIdempotencyKey(t *testing.T) {
	for _, key := range []string{"", "retry-1", strings.Repeat("a", 128)} {
		if err := ValidateIdempotencyKey(key); err != nil {
			t.Errorf("valid key rejected: %v", err)
		}
	}
	for _, key := range []string{"has space", "line\nbreak", strings.Repeat("a", 129), "请求"} {
		if err := ValidateIdempotencyKey(key); err == nil {
			t.Errorf("invalid key accepted: %q", key)
		}
	}
}
