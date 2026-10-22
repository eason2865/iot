package contracts

import "errors"

var ErrInvalidIdempotencyKey = errors.New("idempotency key must contain at most 128 printable ASCII characters without spaces")

// Empty keeps existing clients compatible; callers opt into safe request retries.
func ValidateIdempotencyKey(key string) error {
	if len(key) > 128 {
		return ErrInvalidIdempotencyKey
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7e {
			return ErrInvalidIdempotencyKey
		}
	}
	return nil
}
