package platform

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const requestIDHeader = "X-Request-Id"

type structuredLogWriter struct {
	service string
	out     io.Writer
	mu      sync.Mutex
}

func (w *structuredLogWriter) Write(p []byte) (int, error) {
	line := strings.TrimSpace(string(p))
	if line == "" {
		return len(p), nil
	}
	record := map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"service": w.service,
		"level":   "info",
		"msg":     line,
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return 0, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.out.Write(append(payload, '\n')); err != nil {
		return 0, err
	}
	return len(p), nil
}

func ConfigureStdLogger(serviceName string) {
	log.SetFlags(0)
	logx.AddGlobalFields(logx.Field("service", serviceName))
	log.SetOutput(&structuredLogWriter{
		service: serviceName,
		out:     os.Stdout,
	})
}

func TraceConfig(serviceName string) trace.Config {
	if disabled := strings.EqualFold(os.Getenv("OTEL_DISABLED"), "true"); disabled {
		return trace.Config{
			Name:     serviceName,
			Disabled: true,
		}
	}

	batcher := strings.TrimSpace(os.Getenv("OTEL_BATCHER"))
	if batcher == "" {
		batcher = "file"
	}

	endpoint := strings.TrimSpace(os.Getenv("OTEL_ENDPOINT"))
	if endpoint == "" && batcher == "file" {
		endpoint = fmt.Sprintf("/tmp/%s-traces.log", strings.ReplaceAll(serviceName, "/", "-"))
	}
	if batcher == "file" && endpoint != "" {
		ensureTraceEndpointDir(endpoint)
	}

	cfg := trace.Config{
		Name:     serviceName,
		Endpoint: endpoint,
		Batcher:  batcher,
		Sampler:  1.0,
	}
	if v := strings.TrimSpace(os.Getenv("OTEL_SAMPLER")); v != "" {
		if sample, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.Sampler = sample
		}
	}
	if v := strings.TrimSpace(os.Getenv("OTEL_HTTP_PATH")); v != "" {
		cfg.OtlpHttpPath = v
	}
	if v := strings.TrimSpace(os.Getenv("OTEL_HTTP_SECURE")); strings.EqualFold(v, "true") {
		cfg.OtlpHttpSecure = true
	}
	return cfg
}

func ensureTraceEndpointDir(endpoint string) {
	dir := filepath.Dir(endpoint)
	if dir == "." || dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("trace endpoint directory create error: endpoint=%q error=%v", endpoint, err)
	}
}

func StartTracing(serviceName string) {
	trace.StartAgent(TraceConfig(serviceName))
}

type requestIDContextKey struct{}

func NewRequestID() string {
	var buf [16]byte
	if _, err := crand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}

func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(requestIDContextKey{}).(string); ok {
		return v
	}
	return ""
}

func ContextWithRequestID(ctx context.Context, requestID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		requestID = NewRequestID()
	}
	ctx = context.WithValue(ctx, requestIDContextKey{}, requestID)
	return logx.ContextWithFields(ctx, logx.Field("requestId", requestID))
}

func EnsureRequestID(ctx context.Context, requestID string) (context.Context, string) {
	if existing := RequestIDFromContext(ctx); existing != "" {
		return ContextWithRequestID(ctx, existing), existing
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		requestID = NewRequestID()
	}
	return ContextWithRequestID(ctx, requestID), requestID
}

func RequestIDHTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, requestID := EnsureRequestID(r.Context(), r.Header.Get(requestIDHeader))
		w.Header().Set(requestIDHeader, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func UnaryClientRequestIDInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) error {
		ctx, requestID := EnsureRequestID(ctx, "")
		ctx = metadata.AppendToOutgoingContext(ctx, strings.ToLower(requestIDHeader), requestID)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func UnaryServerRequestIDInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		requestID := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if values := md.Get(strings.ToLower(requestIDHeader)); len(values) > 0 {
				requestID = values[0]
			}
		}
		ctx, requestID = EnsureRequestID(ctx, requestID)
		_ = grpc.SetHeader(ctx, metadata.Pairs(strings.ToLower(requestIDHeader), requestID))
		return handler(ctx, req)
	}
}
