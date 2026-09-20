package platform

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// BearerTokenMiddleware keeps operational endpoints public for Kubernetes and
// Prometheus while requiring a service token for every management API route.
func BearerTokenMiddleware(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthz" || r.URL.Path == "/openapi.json" || r.URL.Path == "/schemas/mqtt-envelope.json" {
				next.ServeHTTP(w, r)
				return
			}
			provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if token == "" || len(provided) != len(token) || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
				writeError(w, http.StatusUnauthorized, "valid bearer token is required")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
