package adminapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"iot/internal/platform"
	corev1 "iot/proto/core/v1"
)

type probeRPC struct{ corev1.CoreServiceClient }

func (probeRPC) ListTenants(ctx context.Context, _ *corev1.ListTenantsRequest, _ ...grpc.CallOption) (*corev1.ListTenantsResponse, error) {
	// Recreate the core's metadata extraction even when client tracing is off.
	carrier := propagation.MapCarrier{}
	md, _ := metadata.FromOutgoingContext(ctx)
	for key, values := range md {
		if len(values) > 0 {
			carrier.Set(key, values[0])
		}
	}
	remote := propagation.TraceContext{}.Extract(context.Background(), carrier)
	_, span := otel.Tracer("test-core").Start(remote, "ListTenants")
	span.End()
	return &corev1.ListTenantsResponse{}, nil
}

func TestProbesDoNotProduceTraces(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(old)
	defer provider.Shutdown(t.Context())
	api := &Server{rpc: probeRPC{}}
	for path, endpoint := range map[string]http.HandlerFunc{"/healthz": api.healthHandler, "/readyz": api.readyHandler} {
		h := platform.SafeHTTPTracingMiddleware("management-api")(endpoint)
		for i := 0; i < 128; i++ {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set("traceparent", "00-11111111111111111111111111111111-1111111111111111-01")
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if res.Code != 200 {
				t.Fatal(res.Code)
			}
		}
	}
	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("probes exported %d spans", len(spans))
	}
	h := platform.SafeHTTPTracingMiddleware("management-api")(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/business", nil))
	if len(exporter.GetSpans()) != 1 {
		t.Fatal("business tracing was disabled")
	}
}
