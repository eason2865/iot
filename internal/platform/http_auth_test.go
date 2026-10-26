package platform

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func runAuthRequest(t *testing.T, mw func(http.Handler) http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })).ServeHTTP(rec, r)
	return rec
}
func TestTenantBoundTokenScoping(t *testing.T) {
	mw := BearerTokenMiddlewareWithTenants("global-token", map[string]string{"tok-a": "tenant-a"})
	for _, tc := range []struct {
		method, path, token, body string
		want                      int
	}{
		{"GET", "/healthz", "", "", 200}, {"GET", "/readyz", "", "", 200},
		{"GET", "/api/v1/tenants", "nope", "", 401}, {"GET", "/api/v1/tenants", "global-token", "", 200},
		{"GET", "/api/v1/tenants/tenant-a/devices", "tok-a", "", 200},
		{"GET", "/api/v1/tenants/tenant-a/devices/dev-1", "tok-a", "", 200},
		{"GET", "/api/v1/tenants/tenant-a/commands", "tok-a", "", 200},
		{"POST", "/api/v1/tenants/tenant-a/devices/d/commands", "tok-a", `{"payload":{}}`, 200},
		{"GET", "/api/v1/tenants/tenant-b/devices/dev-1", "tok-a", "", 403},
		{"GET", "/api/v1/tenants/tenant-b/commands", "tok-a", "", 403},
		{"POST", "/api/v1/tenants/tenant-b/devices/d/commands", "tok-a", `{"tenantId":"tenant-a"}`, 403},
		{"GET", "/api/v1/tenants", "tok-a", "", 403},
	} {
		if rec := runAuthRequest(t, mw, tc.method, tc.path, tc.token, tc.body); rec.Code != tc.want {
			t.Errorf("%s %s HTTP=%d want %d", tc.method, tc.path, rec.Code, tc.want)
		}
	}
}
func TestTenantBoundTokenPreservesRequestBody(t *testing.T) {
	body := `{"payload":{"switch":"on"}}`
	r := httptest.NewRequest("POST", "/api/v1/tenants/tenant-a/devices/d/commands", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer tok-a")
	rec := httptest.NewRecorder()
	BearerTokenMiddlewareWithTenants("", map[string]string{"tok-a": "tenant-a"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		if string(got) != body {
			t.Error("body changed")
		}
		p, err := ManagementRequest(r)
		if err != nil || p.TenantID != "tenant-a" || p.DeviceID != "d" {
			t.Fatalf("shared parameters %+v %v", p, err)
		}
		w.WriteHeader(200)
	})).ServeHTTP(rec, r)
	if rec.Code != 200 {
		t.Fatalf("HTTP %d", rec.Code)
	}
}
func TestTenantTokenCannotOverrideHandlerTenant(t *testing.T) {
	mw := BearerTokenMiddlewareWithTenants("admin", map[string]string{"tok-a": "tenant-a"})
	for _, path := range []string{"/api/v1/tenants?tenantId=tenant-a", "/api/v1/tenants/tenant-b/commands?tenantId=tenant-a", "/api/v1/tenants/tenant-a/commands?status=failed&status=acked", "/api/v1/tenants/tenant-a/commands?bad=%zz", "/api/v1/tenants/tenant-a/commands?unknown=x"} {
		if rec := runAuthRequest(t, mw, "GET", path, "tok-a", ""); rec.Code != 400 {
			t.Errorf("%s HTTP %d", path, rec.Code)
		}
	}
}
func TestBearerSchemeRequiredForTenantToken(t *testing.T) {
	r := httptest.NewRequest("GET", "/api/v1/tenants/tenant-a/commands", nil)
	r.Header.Set("Authorization", "tok-a")
	rec := httptest.NewRecorder()
	BearerTokenMiddlewareWithTenants("", map[string]string{"tok-a": "tenant-a"})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("unauthorized handler called") })).ServeHTTP(rec, r)
	if rec.Code != 401 {
		t.Fatal(rec.Code)
	}
}
