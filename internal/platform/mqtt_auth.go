package platform

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"

	"golang.org/x/crypto/bcrypt"

	"iot/internal/contracts"
)

const internalMQTTUsername = "iot-service"

// AuthCallbackTokenHeader carries the shared secret EMQX must present when
// invoking the internal authentication callback, so arbitrary in-cluster
// callers cannot probe device credentials.
const AuthCallbackTokenHeader = "X-Iot-Auth-Token"

type DeviceAuthenticator interface {
	AuthenticateDevice(tenantID, deviceID, secret string) bool
}

type MQTTAuthRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	ClientID string `json:"clientid"`
}

type MQTTACLRule struct {
	Permission string `json:"permission"`
	Action     string `json:"action"`
	Topic      string `json:"topic"`
}

type MQTTAuthResponse struct {
	Result      string            `json:"result"`
	ClientAttrs map[string]string `json:"client_attrs,omitempty"`
	ACL         []MQTTACLRule     `json:"acl,omitempty"`
}

// MQTTAuthenticationHandler validates EMQX authentication callbacks. When
// callbackToken is non-empty, EMQX must present it via AuthCallbackTokenHeader,
// blocking arbitrary in-cluster callers from probing device credentials.
func MQTTAuthenticationHandler(authenticator DeviceAuthenticator, internalPassword, callbackToken string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if callbackToken != "" && !secureEqual(r.Header.Get(AuthCallbackTokenHeader), callbackToken) {
			writeError(w, http.StatusUnauthorized, "invalid callback token")
			return
		}
		var req MQTTAuthRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&req); err != nil {
			writeJSON(w, http.StatusOK, MQTTAuthResponse{Result: "deny"})
			return
		}
		if req.Username == internalMQTTUsername && secureEqual(req.Password, internalPassword) {
			if role, acl := internalMQTTRole(req.ClientID); role != "" {
				writeJSON(w, http.StatusOK, MQTTAuthResponse{Result: "allow", ClientAttrs: map[string]string{"role": role}, ACL: acl})
				return
			}
		}
		tenantID, deviceID, ok := strings.Cut(req.Username, ":")
		if !ok || tenantID == "" || deviceID == "" || authenticator == nil || !authenticator.AuthenticateDevice(tenantID, deviceID, req.Password) {
			writeJSON(w, http.StatusOK, MQTTAuthResponse{Result: "deny"})
			return
		}
		writeJSON(w, http.StatusOK, MQTTAuthResponse{
			Result:      "allow",
			ClientAttrs: map[string]string{"role": "device", "tenant_id": tenantID, "device_id": deviceID},
			ACL: []MQTTACLRule{
				{Permission: "allow", Action: "publish", Topic: "eq tenant/" + tenantID + "/device/" + deviceID + "/telemetry"},
				{Permission: "allow", Action: "publish", Topic: "eq tenant/" + tenantID + "/device/" + deviceID + "/ack"},
				{Permission: "allow", Action: "subscribe", Topic: "eq tenant/" + tenantID + "/device/" + deviceID + "/command"},
			},
		})
	}
}

func internalMQTTRole(clientID string) (string, []MQTTACLRule) {
	switch {
	case strings.HasPrefix(clientID, "iot-telemetry-ingestor-"):
		// Cover both the shared-subscription form used by multi-replica Helm
		// deployments and the plain filter used by local/E2E single instances.
		return "telemetry-ingestor", []MQTTACLRule{
			{Permission: "allow", Action: "subscribe", Topic: "eq $share/iot-telemetry/tenant/+/device/+/telemetry"},
			{Permission: "allow", Action: "subscribe", Topic: contracts.TelemetryTopicFilter},
		}
	case strings.HasPrefix(clientID, "iot-device-worker-"):
		return "device-worker", []MQTTACLRule{
			{Permission: "allow", Action: "subscribe", Topic: "eq $share/iot-device-worker/tenant/+/device/+/ack"},
			{Permission: "allow", Action: "subscribe", Topic: contracts.AckTopicFilter},
			{Permission: "allow", Action: "publish", Topic: "match tenant/+/device/+/command"},
		}
	case strings.HasPrefix(clientID, "iot-demo-"):
		// The simulator is a trusted local-only test workload. Production does
		// not deploy it or issue this shared service credential.
		return "demo", []MQTTACLRule{
			{Permission: "allow", Action: "publish", Topic: "match tenant/+/device/+/telemetry"},
			{Permission: "allow", Action: "publish", Topic: "match tenant/+/device/+/ack"},
			{Permission: "allow", Action: "subscribe", Topic: "match tenant/+/device/+/command"},
		}
	default:
		return "", nil
	}
}

func secureEqual(left, right string) bool {
	if left == "" || right == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func HashDeviceSecret(secret string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(secret), bcrypt.DefaultCost)
	return string(hash), err
}

func VerifyDeviceSecret(hash, secret string) bool {
	return hash != "" && secret != "" && bcrypt.CompareHashAndPassword([]byte(hash), []byte(secret)) == nil
}
