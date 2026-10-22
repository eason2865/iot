package platform

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	ztrace "github.com/zeromicro/go-zero/core/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

const traceFileMaxBytes int64 = 8 << 20
const traceFileBackups = 2

var fileTracing struct {
	sync.Mutex
	provider *sdktrace.TracerProvider
}

func StartTracing(serviceName string) {
	cfg := traceSettings(serviceName)
	if cfg.Disabled {
		return
	}
	if cfg.Batcher != "file" {
		ztrace.StartAgent(cfg)
		return
	}
	fileTracing.Lock()
	defer fileTracing.Unlock()
	if fileTracing.provider != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Endpoint), 0o755); err != nil {
		log.Printf("trace directory unavailable: %v", err)
		return
	}
	writer := &traceFileWriter{path: cfg.Endpoint, maxBytes: traceFileMaxBytes, backups: traceFileBackups}
	// Discard oversized files from the old unbounded exporter on restart.
	if err := writer.cleanOversized(); err != nil {
		log.Printf("trace retention setup failed: %v", err)
		return
	}
	exporter, err := stdouttrace.New(stdouttrace.WithWriter(writer))
	if err != nil {
		log.Printf("trace exporter unavailable: %v", err)
		return
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", cfg.Name))),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.Sampler))),
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second), sdktrace.WithMaxQueueSize(256), sdktrace.WithMaxExportBatchSize(64)),
	)
	fileTracing.provider = provider
	otel.SetTracerProvider(provider)
}

func StopTracing() {
	fileTracing.Lock()
	provider := fileTracing.provider
	fileTracing.provider = nil
	fileTracing.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if provider != nil {
		_ = provider.Shutdown(ctx)
		return
	}
	done := make(chan struct{})
	go func() { ztrace.StopAgent(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// A valid unsampled parent suppresses readiness RPC spans on both sides of
// the gateway/core boundary, including when root sampling is configured as 1.
func ProbeContext(ctx context.Context) context.Context {
	sc := oteltrace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		id := NewRequestID()
		traceID, _ := oteltrace.TraceIDFromHex(id)
		spanID, _ := oteltrace.SpanIDFromHex(id[:16])
		sc = oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: traceID, SpanID: spanID})
	}
	ctx = oteltrace.ContextWithSpanContext(ctx, sc.WithTraceFlags(sc.TraceFlags()&^oteltrace.FlagsSampled))
	// Readiness must also suppress the core span when the RPC client has no
	// tracing interceptor configured; overwrite any inherited sampled header.
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	for key, value := range carrier {
		md.Set(key, value)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// This writer fails a rotation/write instead of appending beyond its budget.
// File operations are serialized; no descriptor remains open across rotations.
type traceFileWriter struct {
	mu       sync.Mutex
	path     string
	maxBytes int64
	backups  int
}

func (w *traceFileWriter) cleanOversized() error {
	for i := 0; i <= w.backups; i++ {
		path := w.path
		if i > 0 {
			path = fmt.Sprintf("%s.%d", w.path, i)
		}
		stat, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if stat.Size() > w.maxBytes {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *traceFileWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if int64(len(p)) > w.maxBytes {
		return 0, fmt.Errorf("trace record exceeds file size budget")
	}
	stat, err := os.Stat(w.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	if stat != nil && stat.Size()+int64(len(p)) > w.maxBytes {
		for i := w.backups; i > 0; i-- {
			from := w.path
			if i > 1 {
				from = fmt.Sprintf("%s.%d", w.path, i-1)
			}
			if err := os.Rename(from, fmt.Sprintf("%s.%d", w.path, i)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return 0, err
			}
		}
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	n, err := file.Write(p)
	return n, errors.Join(err, file.Close())
}
