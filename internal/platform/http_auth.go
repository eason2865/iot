package platform

import (
	"crypto/subtle"
	"encoding/json"
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
		if strings.TrimSpace(token) == "" || !contracts.IsValidTenantID(tenantID) {
			return nil, errInvalidTenantTokenEntry
		}
	}
	return tokens, nil
}

var errInvalidTenantTokenEntry = &authError{"tenant-bound tokens require non-empty token and tenantId"}

type authError struct{ msg string }

func (e *authError) Error() string { return e.msg }

// BearerTokenMiddlewareWithTenants authorizes a single parsed path scope. The
// global token can manage tenants; bound tokens require the matching tenant path.
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
			global := token != "" && len(provided) == len(token) && subtle.ConstantTimeCompare([]byte(provided), []byte(token)) == 1
			boundTenant, isTenantToken := tenantTokens[provided]
			if !global && !isTenantToken {
				log.Printf("bearer auth rejected: method=%s", r.Method)
				writeError(w, http.StatusUnauthorized, "valid bearer token is required")
				return
			}
			params, err := ManagementRequest(r)
			if err != nil {
				if IsNotFound(err) {
					writeError(w, http.StatusNotFound, "not found")
				} else {
					writeError(w, http.StatusBadRequest, err.Error())
				}
				return
			}
			if !global && (params.TenantID == "" || params.TenantID != boundTenant) {
				writeError(w, http.StatusForbidden, "token is not authorized for this tenant")
				return
			}
			next.ServeHTTP(w, withManagementParameters(r, params))
		})
	}
}
