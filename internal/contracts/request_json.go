package contracts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"
)

// DecodeRequestJSON rejects ambiguous object keys before typed decoding. A
// case-insensitive check matches encoding/json's struct field matching rules.
func DecodeRequestJSON(r io.Reader, dst any) error {
	raw, err := io.ReadAll(io.LimitReader(r, MaxJSONBBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > MaxJSONBBytes {
		return fmt.Errorf("request body exceeds %d bytes", MaxJSONBBytes)
	}
	if !utf8.Valid(raw) {
		return fmt.Errorf("request body must contain valid UTF-8")
	}
	if err = validateUniqueJSON(raw, true); err != nil {
		return err
	}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("request body must be a JSON object")
	}
	if err := ValidateJSONB(raw); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func ValidateUniqueJSON(raw []byte) error { return validateUniqueJSON(raw, false) }

func validateUniqueJSON(raw []byte, foldRoot bool) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value func(int) error
	value = func(depth int) error {
		if depth > 128 {
			return fmt.Errorf("JSON nesting exceeds 128 levels")
		}
		token, err := dec.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		if delim == '{' {
			seen := map[string]bool{}
			for dec.More() {
				key, err := dec.Token()
				if err != nil {
					return err
				}
				name := key.(string)
				if depth == 0 && foldRoot {
					name = foldJSONField(name)
				}
				if seen[name] {
					return fmt.Errorf("duplicate JSON field")
				}
				seen[name] = true
				if err = value(depth + 1); err != nil {
					return err
				}
			}
		} else if delim == '[' {
			for dec.More() {
				if err = value(depth + 1); err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("invalid JSON delimiter")
		}
		_, err = dec.Token()
		return err
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return fmt.Errorf("request body must contain exactly one JSON value")
	}
	return nil
}

func foldJSONField(name string) string {
	runes := []rune(name)
	for i, r := range runes {
		smallest := r
		for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
			if next < smallest {
				smallest = next
			}
		}
		runes[i] = smallest
	}
	return string(runes)
}
