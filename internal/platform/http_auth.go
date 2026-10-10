package platform

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"iot/internal/contracts"
)

// BearerTokenMiddleware keeps operational endpoints public for Kubernetes and
// Prometheus while requiring a service token for every management API route.
// The MQTT envelope schema stays public because device vendors integrate
// against it; the previous /openapi.json exemption was removed along with the
// endpoint, so the full REST route map is no longer readable without a token.
func BearerTokenMiddleware(token string) func(http.Handler) http.Handler {
	return BearerTokenMiddlewareWithTenants(token, nil)
}

// ParseTenantBoundTokens parses the MANAGEMENT_API_TOKENS value: a JSON object
// mapping a bearer token to the tenant ID it is restricted to, e.g.
// {"token-a":"tenant-1"}. An empty input yields an empty map.
func ParseTenantBoundTokens(raw string) (map[string]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var tokens map[string]string
	if err := json.Unmarshal([]byte(raw), &tokens); err != nil {
		return nil, err
	}
	for token, tenantID := range tokens {
		if strings.TrimSpace(token) == "" || !contracts.IsValidTopicPart(tenantID) {
			return nil, errInvalidTenantTokenEntry
		}
	}
	return tokens, nil
}

var errInvalidTenantTokenEntry = &authError{"tenant-bound tokens require non-empty token and tenantId"}

type authError struct{ msg string }

func (e *authError) Error() string { return e.msg }

// BearerTokenMiddlewareWithTenants extends BearerTokenMiddleware with optional
// per-tenant tokens: a token present in tenantTokens authenticates only
// requests that name that exact tenant (in the device path, the tenantId query
// parameter, or the JSON body). Cross-tenant endpoints (tenant CRUD, device
// list) are rejected for tenant-bound tokens. The global token keeps full
// access. An empty tenantTokens map behaves exactly like
// BearerTokenMiddleware.
func BearerTokenMiddlewareWithTenants(token string, tenantTokens map[string]string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Probes and the public schema must stay unauthenticated: the
			// kubelet does not carry a token.
			if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/schemas/mqtt-envelope.json" {
				next.ServeHTTP(w, r)
				return
			}
			provided, hasBearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !hasBearer || provided == "" {
				writeError(w, http.StatusUnauthorized, "valid bearer token is required")
				return
			}
			if token != "" && len(provided) == len(token) && subtle.ConstantTimeCompare([]byte(provided), []byte(token)) == 1 {
				next.ServeHTTP(w, r)
				return
			}
			boundTenant, isTenantToken := tenantTokens[provided]
			if !isTenantToken {
				// Log rejections so token misuse and brute-force attempts are
				// visible; without this a 401 leaves no trace anywhere.
				log.Printf("bearer auth rejected: method=%s path=%s remote=%s", r.Method, r.URL.Path, r.RemoteAddr)
				writeError(w, http.StatusUnauthorized, "valid bearer token is required")
				return
			}
			requestTenant, err := requestTenantID(r)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			if requestTenant == "" {
				// A tenant-bound token must never reach cross-tenant endpoints
				// (tenant CRUD, unscoped device listing): they would expose
				// other tenants' data.
				log.Printf("tenant-bound token rejected for cross-tenant route: tenant=%s method=%s path=%s", boundTenant, r.Method, r.URL.Path)
				writeError(w, http.StatusForbidden, "tenant-bound token requires a tenant-scoped request")
				return
			}
			if requestTenant != boundTenant {
				log.Printf("tenant-bound token tenant mismatch: token tenant=%s request tenant=%s path=%s", boundTenant, requestTenant, r.URL.Path)
				writeError(w, http.StatusForbidden, "token is not authorized for this tenant")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// requestTenantID extracts the tenant a request targets: the device path
// segment, the tenantId query parameter, or the JSON body's tenantId field.
// A consumed body is restored so downstream handlers decode it unchanged.
func requestTenantID(r *http.Request) (string, error) {
	// Authorize against exactly the tenant source used by each handler. A
	// query parameter on a write or global list must never grant access.
	if r.Method == http.MethodGet {
		if rest, ok := strings.CutPrefix(r.URL.Path, "/api/v1/devices/"); ok {
			parts := strings.Split(rest, "/")
			if len(parts) >= 2 && parts[0] != "" && parts[1] != "" {
				return parts[0], nil
			}
		}
		if r.URL.Path == "/api/v1/commands" || strings.HasPrefix(r.URL.Path, "/api/v1/commands/") {
			return r.URL.Query().Get("tenantId"), nil
		}
		return "", nil
	}
	isWrite := r.URL.Path == "/api/v1/devices" || r.URL.Path == "/api/v1/telemetry" || r.URL.Path == "/api/v1/commands" ||
		(strings.HasPrefix(r.URL.Path, "/api/v1/commands/") && strings.HasSuffix(r.URL.Path, "/ack"))
	if r.Method != http.MethodPost || !isWrite || r.Body == nil {
		return "", nil
	}
	// Also bound direct middleware callers, which may not use the gateway's
	// MaxBytesReader. Restore the body for the real handler.
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20+1))
	if err != nil {
		return "", err
	}
	if len(body) > 1<<20 {
		return "", errInvalidTenantTokenBody
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var fields struct {
		TenantID string `json:"tenantId"`
	}
	if err := json.Unmarshal(body, &fields); err != nil {
		return "", errInvalidTenantTokenBody
	}
	return fields.TenantID, nil
}

var errInvalidTenantTokenBody = &authError{"request body is not valid JSON"}
