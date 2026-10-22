package platform

import (
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// SafeHTTPTracingMiddleware uses an attribute allowlist for every exporter.
// URL queries, path identifiers, headers, bodies, peer addresses and user agents
// are never span attributes. The request itself remains intact for auth/RPC.
func SafeHTTPTracingMiddleware(serviceName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route := routeLabel(r.URL.Path)
			if route == "/healthz" || route == "/readyz" || route == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}
			method := r.Method
			switch method {
			case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "CONNECT", "OPTIONS", "TRACE":
			default:
				method = "_OTHER"
			}
			propagator := otel.GetTextMapPropagator()
			ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			ctx, span := otel.Tracer(serviceName).Start(ctx, route, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(
				attribute.String("http.method", method), attribute.String("http.route", route), attribute.String("http.target", route),
			))
			defer span.End()
			propagator.Inject(ctx, propagation.HeaderCarrier(w.Header()))
			response := &accessLogResponse{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(response, r.WithContext(ctx))
			span.SetAttributes(attribute.Int("http.status_code", response.status))
			if response.status >= 500 {
				span.SetStatus(codes.Error, "")
			}
		})
	}
}
