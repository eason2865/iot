package platform

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

const internalMQTTUsername = "iot-service"

// AuthCallbackTokenHeader carries the shared secret EMQX must present when
// invoking the internal authentication callback, so arbitrary in-cluster
// callers cannot probe device credentials.
// NOTE: the name intentionally uses underscores instead of hyphens. EMQX 6
// does not resolve `${VAR}` placeholders inside authn HTTP templates, so the
// token is injected via an EMQX_AUTHENTICATION__1__HEADERS__* env override,
// and EMQX only accepts [A-Z0-9_] segments in such override names.
const AuthCallbackTokenHeader = "X_Iot_Auth_Token"

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

// aclDenyAll terminates every client ACL. EMQX falls through to the
// broker-wide authorization chain when no client ACL rule matches, so without
// a terminal deny a misconfigured chain rule (e.g. the legacy security-profile
// allow-all in the default acl.conf) would silently grant full access.
var aclDenyAll = MQTTACLRule{Permission: "deny", Action: "all", Topic: "#"}

func withACLGuard(rules ...MQTTACLRule) []MQTTACLRule {
	return append(rules, aclDenyAll)
}

// MQTTAuthenticationHandler validates EMQX authentication callbacks. EMQX must
// present the shared callbackToken via AuthCallbackTokenHeader. The check is
// fail-closed: an empty configured token rejects every callback, so the
// endpoint is never left unauthenticated by a missing env var.
func MQTTAuthenticationHandler(authenticator DeviceAuthenticator, internalPassword, callbackToken string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if callbackToken == "" || !secureEqual(r.Header.Get(AuthCallbackTokenHeader), callbackToken) {
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
			ACL: withACLGuard(
				MQTTACLRule{Permission: "allow", Action: "publish", Topic: "eq tenant/" + tenantID + "/device/" + deviceID + "/telemetry"},
				MQTTACLRule{Permission: "allow", Action: "publish", Topic: "eq tenant/" + tenantID + "/device/" + deviceID + "/ack"},
				MQTTACLRule{Permission: "allow", Action: "subscribe", Topic: "eq tenant/" + tenantID + "/device/" + deviceID + "/command"},
			),
		})
	}
}

func internalMQTTRole(clientID string) (string, []MQTTACLRule) {
	switch {
	case strings.HasPrefix(clientID, "iot-telemetry-ingestor-"):
		// EMQX unwraps shared subscriptions before authorization, so the rule
		// must match the bare filter; shared vs plain cannot be distinguished
		// at the authz layer. Note: EMQX 6.3 ACL topics only support the "eq "
		// prefix — a legacy "match " prefix would be treated as a literal
		// topic level and never match.
		return "telemetry-ingestor", withACLGuard(
			MQTTACLRule{Permission: "allow", Action: "subscribe", Topic: "tenant/+/device/+/telemetry"},
		)
	case strings.HasPrefix(clientID, "iot-device-worker-"):
		return "device-worker", withACLGuard(
			MQTTACLRule{Permission: "allow", Action: "subscribe", Topic: "tenant/+/device/+/ack"},
			MQTTACLRule{Permission: "allow", Action: "publish", Topic: "tenant/+/device/+/command"},
		)
	case strings.HasPrefix(clientID, "iot-demo-"):
		// The simulator is a trusted local-only test workload, but a leaked
		// service credential must not grant broker-wide access: scope its ACL
		// to the single tenant encoded in the client ID.
		tenantID := demoTenantFromClientID(clientID)
		if tenantID == "" {
			return "", nil
		}
		return "demo", withACLGuard(
			MQTTACLRule{Permission: "allow", Action: "publish", Topic: "tenant/" + tenantID + "/device/+/telemetry"},
			MQTTACLRule{Permission: "allow", Action: "publish", Topic: "tenant/" + tenantID + "/device/+/ack"},
			MQTTACLRule{Permission: "allow", Action: "subscribe", Topic: "tenant/" + tenantID + "/device/+/command"},
		)
	default:
		return "", nil
	}
}

// demoTenantFromClientID extracts the tenant embedded in a demo simulator
// client ID of the form "iot-demo-<tenant>-<unix-nano>" (see demo runtime).
func demoTenantFromClientID(clientID string) string {
	rest, ok := strings.CutPrefix(clientID, "iot-demo-")
	if !ok {
		return ""
	}
	idx := strings.LastIndex(rest, "-")
	if idx <= 0 {
		return ""
	}
	suffix := rest[idx+1:]
	if suffix == "" {
		return ""
	}
	for _, r := range suffix {
		if r < '0' || r > '9' {
			return ""
		}
	}
	return rest[:idx]
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
