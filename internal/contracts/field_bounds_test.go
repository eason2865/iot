package contracts

import (
	"strings"
	"testing"
	"time"
)

func TestEnvelopeStorageFieldBounds(t *testing.T) {
	for _, field := range []string{"msgId", "tenantId", "deviceId", "type", "version"} {
		for _, multibyte := range []bool{false, true} {
			limit := 64
			if field == "version" {
				limit = 32
			}
			for _, overflow := range []bool{false, true} {
				env := Envelope{MsgID: "m", TenantID: "t", DeviceID: "d", Type: "temp", Version: "v1", Ts: time.Now().UnixMilli()}
				value := strings.Repeat("x", limit)
				if multibyte {
					value = strings.Repeat("界", limit/3) + strings.Repeat("x", limit%3)
				}
				if overflow {
					value += "x"
				}
				switch field {
				case "msgId":
					env.MsgID = value
				case "tenantId":
					env.TenantID = value
				case "deviceId":
					env.DeviceID = value
				case "type":
					env.Type = value
				case "version":
					env.Version = value
				}
				if err := ValidateEnvelope(env); (err != nil) != overflow {
					t.Errorf("%s bytes=%d overflow=%v: %v", field, len(value), overflow, err)
				}
			}
		}
	}
}

func TestStorageFieldsRejectInvalidText(t *testing.T) {
	for _, bad := range []string{"", "a\nb", "a\rb", "a\x00b", "\x7f", "tab\there", "\u0085", string([]byte{0xff})} {
		if err := ValidateStorageField("test", bad, 64); err == nil {
			t.Errorf("accepted invalid text %q", bad)
		}
	}
	for _, valid := range []string{"msg'id", `back\slash`, "unicode ✓"} {
		if err := ValidateStorageField("test", valid, 64); err != nil {
			t.Errorf("rejected %q: %v", valid, err)
		}
	}
}
