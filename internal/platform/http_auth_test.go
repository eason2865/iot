package platform

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseTenantBoundTokens(t *testing.T) {
	if tokens, err := ParseTenantBoundTokens(""); err != nil || tokens != nil {
		t.Fatalf("ParseTenantBoundTokens(\"\") = %v, %v; want nil, nil", tokens, err)
	}
	if _, err := ParseTenantBoundTokens("{not-json"); err == nil {
		t.Fatal("ParseTenantBoundTokens accepted invalid JSON")
	}
	if _, err := ParseTenantBoundTokens(`{"tok":""}`); err == nil {
		t.Fatal("ParseTenantBoundTokens accepted an empty tenantId")
	}
	tokens, err := ParseTenantBoundTokens(`{"tok-a":"tenant-a","tok-b":"tenant-b"}`)
	if err != nil {
		t.Fatalf("ParseTenantBoundTokens() error = %v", err)
	}
	if tokens["tok-a"] != "tenant-a" || tokens["tok-b"] != "tenant-b" {
		t.Fatalf("ParseTenantBoundTokens() = %v", tokens)
	}
}

func runAuthRequest(t *testing.T, mw func(http.Handler) http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)
	return rec
}

func TestTenantBoundTokenScoping(t *testing.T) {
	mw := BearerTokenMiddlewareWithTenants("global-token", map[string]string{"tok-a": "tenant-a"})

	// Public endpoints stay open.
	if rec := runAuthRequest(t, mw, http.MethodGet, "/healthz", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", rec.Code)
	}
	if rec := runAuthRequest(t, mw, http.MethodGet, "/readyz", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("readyz status = %d, want 200 (kubelet probes carry no token)", rec.Code)
	}
	// Unknown token is rejected.
	if rec := runAuthRequest(t, mw, http.MethodGet, "/api/v1/tenants", "nope", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unknown token status = %d, want 401", rec.Code)
	}
	// Global token keeps full access, including cross-tenant routes.
	if rec := runAuthRequest(t, mw, http.MethodGet, "/api/v1/tenants", "global-token", ""); rec.Code != http.StatusOK {
		t.Fatalf("global token status = %d, want 200", rec.Code)
	}
	// Tenant-bound token is accepted on its own tenant: path, query, body.
	if rec := runAuthRequest(t, mw, http.MethodGet, "/api/v1/devices/tenant-a/dev-1", "tok-a", ""); rec.Code != http.StatusOK {
		t.Fatalf("path tenant status = %d, want 200", rec.Code)
	}
	if rec := runAuthRequest(t, mw, http.MethodGet, "/api/v1/commands?tenantId=tenant-a", "tok-a", ""); rec.Code != http.StatusOK {
		t.Fatalf("query tenant status = %d, want 200", rec.Code)
	}
	if rec := runAuthRequest(t, mw, http.MethodPost, "/api/v1/commands", "tok-a", `{"tenantId":"tenant-a","deviceId":"d","payload":{}}`); rec.Code != http.StatusOK {
		t.Fatalf("body tenant status = %d, want 200", rec.Code)
	}
	// Foreign tenants are forbidden, whichever way they are named.
	if rec := runAuthRequest(t, mw, http.MethodGet, "/api/v1/devices/tenant-b/dev-1", "tok-a", ""); rec.Code != http.StatusForbidden {
		t.Fatalf("foreign path tenant status = %d, want 403", rec.Code)
	}
	if rec := runAuthRequest(t, mw, http.MethodGet, "/api/v1/commands?tenantId=tenant-b", "tok-a", ""); rec.Code != http.StatusForbidden {
		t.Fatalf("foreign query tenant status = %d, want 403", rec.Code)
	}
	if rec := runAuthRequest(t, mw, http.MethodPost, "/api/v1/commands", "tok-a", `{"tenantId":"tenant-b"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("foreign body tenant status = %d, want 403", rec.Code)
	}
	// Cross-tenant routes are forbidden for tenant-bound tokens.
	if rec := runAuthRequest(t, mw, http.MethodGet, "/api/v1/tenants", "tok-a", ""); rec.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant route status = %d, want 403", rec.Code)
	}
}

func TestTenantBoundTokenPreservesRequestBody(t *testing.T) {
	mw := BearerTokenMiddlewareWithTenants("", map[string]string{"tok-a": "tenant-a"})
	body := `{"tenantId":"tenant-a","deviceId":"d"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/commands", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer tok-a")
	rec := httptest.NewRecorder()
	var got string
	mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, len(body))
		n, _ := r.Body.Read(buf)
		got = string(buf[:n])
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got != body {
		t.Fatalf("handler body = %q, want %q", got, body)
	}
}

func TestTenantTokenCannotOverrideHandlerTenant(t *testing.T) {
	mw := BearerTokenMiddlewareWithTenants("global", map[string]string{"a": "tenant-a"})
	for _, tc := range []struct{ method, path, body string }{
		{"GET", "/api/v1/tenants?tenantId=tenant-a", ""},
		{"POST", "/api/v1/tenants?tenantId=tenant-a", `{"id":"tenant-b"}`},
		{"GET", "/api/v1/devices?tenantId=tenant-a", ""},
		{"POST", "/api/v1/commands?tenantId=tenant-a", `{"tenantId":"tenant-b"}`},
		{"POST", "/api/v1/commands/cmd/ack?tenantId=tenant-a", `{"tenantId":"tenant-b"}`},
		{"POST", "/api/v1/telemetry?tenantId=tenant-a", `{"tenantId":"tenant-b"}`},
	} {
		if got := runAuthRequest(t, mw, tc.method, tc.path, "a", tc.body).Code; got != http.StatusForbidden {
			t.Errorf("%s %s: got %d, want 403", tc.method, tc.path, got)
		}
	}
}

func TestTokenRequiresBearerScheme(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/commands?tenantId=tenant-a", nil)
	req.Header.Set("Authorization", "a")
	rec := httptest.NewRecorder()
	BearerTokenMiddlewareWithTenants("a", nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("raw token status = %d, want 401", rec.Code)
	}
}
