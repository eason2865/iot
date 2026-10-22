package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

var ErrInvalidPayload = errors.New("invalid payload")
var ErrInvalidDeviceSecret = errors.New("secret must contain 1..72 UTF-8 bytes")

func ValidateDeviceSecret(secret string) error {
	if len(secret) == 0 || len(secret) > 72 || !utf8.ValidString(secret) {
		return ErrInvalidDeviceSecret
	}
	return nil
}

// ValidateJSONB preserves JSON numbers without float64 conversion, while enforcing
// PostgreSQL's numeric and Unicode representation limits before any database write.
func ValidateJSONB(raw json.RawMessage) error {
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return ErrInvalidPayload
	}
	// encoding/json replaces unpaired UTF-16 surrogates. JSONB rejects them, so
	// check escapes before decoding can erase that distinction.
	for i := 0; i < len(raw); i++ {
		if raw[i] != '"' {
			continue
		}
		i++
		for i < len(raw) && raw[i] != '"' {
			if raw[i] == '\\' {
				i++
				if raw[i] == 'u' {
					u, _ := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
					i += 4
					if u >= 0xd800 && u <= 0xdbff {
						if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
							return ErrInvalidPayload
						}
						low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
						if err != nil || low < 0xdc00 || low > 0xdfff {
							return ErrInvalidPayload
						}
						i += 6
					} else if u >= 0xdc00 && u <= 0xdfff {
						return ErrInvalidPayload
					}
				}
			}
			i++
		}
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	for {
		token, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return ErrInvalidPayload
		}
		switch v := token.(type) {
		case string:
			if strings.ContainsRune(v, 0) {
				return ErrInvalidPayload
			}
		case json.Number:
			number := strings.TrimPrefix(string(v), "-")
			mantissa, exp := number, int64(0)
			if at := strings.IndexAny(number, "eE"); at >= 0 {
				mantissa = number[:at]
				exp, err = strconv.ParseInt(number[at+1:], 10, 32)
				if err != nil {
					return ErrInvalidPayload
				}
			}
			integer, fraction, _ := strings.Cut(mantissa, ".")
			digits := integer + fraction
			leading := len(digits) - len(strings.TrimLeft(digits, "0"))
			if int64(len(integer)-leading)+exp > 131072 || int64(len(fraction))-exp > 16383 || exp > 131072 || exp < -16383-int64(len(integer)) {
				return fmt.Errorf("%w: numeric range exceeded", ErrInvalidPayload)
			}
		}
	}
}
