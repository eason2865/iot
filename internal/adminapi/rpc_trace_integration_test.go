package adminapi

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zeromicro/go-zero/rest/handler"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"iot/internal/platform"
	corev1 "iot/proto/core/v1"
)

type traceCaptureCore struct {
	corev1.UnimplementedCoreServiceServer
	received chan metadata.MD
}

func (s *traceCaptureCore) ListTenants(ctx context.Context, _ *corev1.ListTenantsRequest) (*corev1.ListTenantsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.received <- md.Copy()
	return &corev1.ListTenantsResponse{}, nil
}

func TestHTTPTracePropagatesThroughProductionRPCClient(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())))
	oldProvider, oldPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldPropagator)
		_ = provider.Shutdown(t.Context())
	}()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	captured := &traceCaptureCore{received: make(chan metadata.MD, 4)}
	rpcServer := grpc.NewServer()
	corev1.RegisterCoreServiceServer(rpcServer, captured)
	go rpcServer.Serve(listener)
	defer rpcServer.Stop()
	t.Setenv("IOT_CORE_ENDPOINTS", listener.Addr().String())
	t.Setenv("IOT_CORE_TLS_CA", "")
	t.Setenv("IOT_CORE_TLS_CERT", "")
	t.Setenv("IOT_CORE_TLS_KEY", "")
	t.Setenv("IOT_CORE_TLS_SERVER_NAME", "")
	client, err := newRPCClient()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Conn().Close()
	api := &Server{rpc: corev1.NewCoreServiceClient(client.Conn())}
	conf := managementHTTPConf()
	for _, probe := range []bool{false, true} {
		path := "/api/v1/tenants"
		endpoint := http.HandlerFunc(api.listTenantsHandler)
		if probe {
			path = "/readyz"
			endpoint = api.readyHandler
			exporter.Reset()
		}
		h := platform.RequestIDHTTPMiddleware(handler.TraceHandler(conf.Name, path, handler.WithTraceIgnorePaths(conf.TraceIgnorePaths))(endpoint))
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("X-Request-Id", "trace-regression")
		req.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		if res.Code != 200 {
			t.Fatalf("HTTP %d", res.Code)
		}
		md := <-captured.received
		if len(md.Get("x-request-id")) == 0 {
			t.Fatal("request ID missing")
		}
		carrier := propagation.MapCarrier{}
		for k, v := range md {
			if len(v) > 0 {
				carrier.Set(k, v[0])
			}
		}
		sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), carrier))
		if !sc.IsValid() || sc.IsSampled() == probe {
			t.Fatalf("probe=%v missing/wrong sampled context: traceparent=%v", probe, md.Get("traceparent"))
		}
		if !probe && sc.TraceID().String() != "11111111111111111111111111111111" {
			t.Fatal("trace ID changed across RPC")
		}
		if probe && len(exporter.GetSpans()) != 0 {
			t.Fatal("probe emitted tracing spans")
		}
	}
}
