package platform

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"iot/internal/contracts"
)

type exactDeviceAuthenticator struct{}

func (exactDeviceAuthenticator) AuthenticateDevice(tenant, device, secret string) bool {
	return tenant == "factory" && device == "line1:d" && secret == "secret"
}

func TestMQTTAuthenticationPreservesDeviceColons(t *testing.T) {
	username, err := contracts.BuildDeviceUsername("factory", "line1:d")
	if err != nil {
		t.Fatal(err)
	}
	handler := MQTTAuthenticationHandler(exactDeviceAuthenticator{}, "internal", "callback")
	request := httptest.NewRequest("POST", "/authenticate", strings.NewReader(`{"username":"`+username+`","password":"secret"}`))
	request.Header.Set(AuthCallbackTokenHeader, "callback")
	response := httptest.NewRecorder()
	handler(response, request)
	var result MQTTAuthResponse
	if err = json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Result != "allow" || result.ClientAttrs["tenant_id"] != "factory" || result.ClientAttrs["device_id"] != "line1:d" {
		t.Fatalf("identity changed: %+v", result)
	}
	if result.ACL[0].Topic != "eq tenant/factory/device/line1:d/telemetry" {
		t.Fatalf("wrong ACL: %+v", result.ACL)
	}
}
