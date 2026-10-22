package contracts

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestEnvelopeRequiresObjectPayload(t *testing.T) {
	for _, payload := range []string{"", "null", "[]", "42", `"text"`, "true", "{", "{} {}", "\u00a0{}", "{\"x\":\"\xff\"}", "{}", ` {"values":[1,null,{"x":true}]} `} {
		valid := payload == "{}" || payload == ` {"values":[1,null,{"x":true}]} `
		env := Envelope{MsgID: "m", TenantID: "t", DeviceID: "d", Ts: time.Now().UnixMilli(), Type: "telemetry", Version: "v1", Payload: json.RawMessage(payload)}
		if err := ValidateEnvelope(env); (err == nil) != valid {
			t.Errorf("payload %q: %v", payload, err)
		}
		member := ""
		if payload != "" {
			member = `,"payload":` + payload
		}
		raw := fmt.Sprintf(`{"msgId":"m","tenantId":"t","deviceId":"d","ts":%d,"type":"temp","version":"v1"%s}`, time.Now().UnixMilli(), member)
		if _, err := ParseEnvelope([]byte(raw)); (err == nil) != valid {
			t.Errorf("MQTT payload %q: %v", payload, err)
		}
	}
}
