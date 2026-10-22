package adminapi

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zeromicro/go-zero/rest"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"iot/internal/platform"
)

// Bind the real go-zero middleware chain, then stop immediately before Listen.
func boundManagementHandler(t *testing.T) http.Handler {
	t.Helper()
	s := newManagementHTTPServer()
	s.Use(rest.ToMiddleware(platform.BearerTokenMiddlewareWithTenants("fictional-admin-token", nil)))
	s.AddRoute(rest.Route{Method: "GET", Path: "/api/v1/tenants", Handler: func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("secret") != "fictional-query-secret" {
			t.Error("original query changed")
		}
		w.WriteHeader(200)
	}})
	var h http.Handler
	type stopBeforeListen struct{}
	func() {
		defer func() {
			if p := recover(); p != nil {
				if _, ok := p.(stopBeforeListen); !ok {
					panic(p)
				}
			}
		}()
		s.StartWithOpts(func(server *http.Server) { h = server.Handler; panic(stopBeforeListen{}) })
	}()
	if h == nil {
		t.Fatal("middleware chain not bound")
	}
	return h
}

func TestManagementTracingNeverExportsRawURLSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.log")
	t.Setenv("OTEL_DISABLED", "false")
	t.Setenv("OTEL_BATCHER", "file")
	t.Setenv("OTEL_ENDPOINT", path)
	t.Setenv("OTEL_SAMPLER", "1")
	old := otel.GetTracerProvider()
	defer otel.SetTracerProvider(old)
	platform.StartTracing("management-api")
	defer platform.StopTracing()
	h := boundManagementHandler(t)
	for _, route := range []string{"/api/v1/tenants", "/unknown/fictional-path-secret"} {
		for _, authorized := range []bool{false, true} {
			r := httptest.NewRequest("GET", route+"?secret=fictional-query-secret&token=fictional-query-token", nil)
			r.Header.Set("Cookie", "fictional-cookie-secret")
			r.Header.Set("User-Agent", "fictional-agent-secret")
			if authorized {
				r.Header.Set("Authorization", "Bearer fictional-admin-token")
			}
			response := httptest.NewRecorder()
			h.ServeHTTP(response, r)
			want := 401
			if authorized {
				want = 200
			}
			if strings.HasPrefix(route, "/unknown/") {
				want = 404
			}
			if response.Code != want {
				t.Fatalf("HTTP %d want %d", response.Code, want)
			}
		}
	}
	provider := otel.GetTracerProvider().(*sdktrace.TracerProvider)
	if err := provider.ForceFlush(t.Context()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 {
		t.Fatal("business tracing was disabled")
	}
	for _, secret := range []string{"fictional-query-secret", "fictional-query-token", "fictional-path-secret", "fictional-cookie-secret", "fictional-agent-secret", "fictional-admin-token"} {
		if strings.Contains(string(raw), secret) {
			t.Errorf("trace exported %s", secret)
		}
	}
}
