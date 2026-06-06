package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestRequestIDHTTPMiddleware(t *testing.T) {
	var gotRequestID string
	h := RequestIDHTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRequestID = RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "http://example.com/healthz", nil)
	req.Header.Set(requestIDHeader, "req-123")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusNoContent)
	}
	if gotRequestID != "req-123" {
		t.Fatalf("unexpected request id in context: got %q want %q", gotRequestID, "req-123")
	}
	if got := rec.Header().Get(requestIDHeader); got != "req-123" {
		t.Fatalf("unexpected request id header: got %q want %q", got, "req-123")
	}
}

func TestUnaryClientRequestIDInterceptor(t *testing.T) {
	ctx := ContextWithRequestID(context.Background(), "req-client")
	var gotRequestID string

	err := UnaryClientRequestIDInterceptor()(ctx, "/core.v1.CoreService/CreateCommand", nil, nil, nil,
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			md, ok := metadata.FromOutgoingContext(ctx)
			if !ok {
				t.Fatalf("expected outgoing metadata")
			}
			values := md.Get("x-request-id")
			if len(values) == 0 {
				t.Fatalf("expected request id metadata")
			}
			gotRequestID = values[0]
			return nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotRequestID != "req-client" {
		t.Fatalf("unexpected request id metadata: got %q want %q", gotRequestID, "req-client")
	}
}

func TestUnaryServerRequestIDInterceptor(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "req-server"))
	var gotRequestID string

	_, err := UnaryServerRequestIDInterceptor()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/core.v1.CoreService/CreateCommand"},
		func(ctx context.Context, req any) (any, error) {
			gotRequestID = RequestIDFromContext(ctx)
			return nil, nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotRequestID != "req-server" {
		t.Fatalf("unexpected request id in handler context: got %q want %q", gotRequestID, "req-server")
	}
}
