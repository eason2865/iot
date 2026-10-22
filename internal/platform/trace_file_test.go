package platform

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestTraceFileStaysBounded(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("OTEL_DISABLED", "false")
	t.Setenv("OTEL_BATCHER", "file")
	t.Setenv("OTEL_ENDPOINT", filepath.Join(dir, "trace.log"))
	t.Setenv("OTEL_SAMPLER", "1")
	old := otel.GetTracerProvider()
	defer otel.SetTracerProvider(old)
	defer StopTracing()
	StartTracing("bounded-regression")
	provider, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider)
	if !ok {
		t.Fatal("file tracer provider was not installed")
	}
	tracer := provider.Tracer("regression")
	for i := 0; i < 512; i++ {
		_, span := tracer.Start(context.Background(), "business-operation")
		span.SetAttributes(attribute.String("sample", strings.Repeat("x", 64<<10)))
		span.End()
		if i%16 == 0 {
			if err := provider.ForceFlush(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := provider.ForceFlush(t.Context()); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) > 3 || len(files) < 2 {
		t.Fatalf("expected active trace plus up to two rotated files, got %d", len(files))
	}
	var total int64
	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
		if info.Size() > 8<<20 {
			t.Errorf("trace file %s grew beyond 8 MiB: %d", file.Name(), info.Size())
		}
	}
	if total > 24<<20 {
		t.Fatalf("trace retention grew beyond 24 MiB: %d", total)
	}
}

func TestTraceWriterKeepsBudgetUnderConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.log")
	writer := &traceFileWriter{path: path, maxBytes: 128, backups: 2}
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 20; j++ {
				if _, err := writer.Write([]byte("a complete span record\n")); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	group.Wait()
	files, _ := filepath.Glob(path + "*")
	if len(files) != 3 {
		t.Fatalf("files=%v", files)
	}
	for _, file := range files {
		stat, err := os.Stat(file)
		if err != nil || stat.Size() > 128 {
			t.Fatalf("file exceeded budget: %s %v", file, err)
		}
	}
	if _, err := writer.Write([]byte(strings.Repeat("x", 129))); err == nil {
		t.Fatal("oversized trace should be dropped")
	}
}

func TestTraceRotationFailureDoesNotAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trace.log")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 60)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path+".1", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".1/blocked", []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	writer := &traceFileWriter{path: path, maxBytes: 64, backups: 1}
	if _, err := writer.Write([]byte("too much\n")); err == nil {
		t.Fatal("rotation failure was ignored")
	}
	stat, err := os.Stat(path)
	if err != nil || stat.Size() != 60 {
		t.Fatalf("appended despite failed rotation: %v %v", stat, err)
	}
	if err := os.RemoveAll(path + ".1"); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("recovered\n")); err != nil {
		t.Fatal(err)
	}
}

func TestFileTraceSettingsHaveBoundedDefaults(t *testing.T) {
	t.Setenv("OTEL_DISABLED", "false")
	t.Setenv("OTEL_BATCHER", "file")
	t.Setenv("OTEL_ENDPOINT", filepath.Join(t.TempDir(), "trace.log"))
	for _, sample := range []string{"", "NaN", "+Inf", "-1", "2"} {
		t.Setenv("OTEL_SAMPLER", sample)
		if cfg := traceSettings("test"); cfg.Sampler != 0.1 {
			t.Fatalf("sampler %q = %v", sample, cfg.Sampler)
		}
	}
	if !TraceConfig("test").Disabled {
		t.Fatal("go-zero can replace the bounded file exporter")
	}
	writer := &traceFileWriter{path: os.Getenv("OTEL_ENDPOINT"), maxBytes: 64, backups: 2}
	if err := os.WriteFile(writer.path, []byte(strings.Repeat("x", 128)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writer.cleanOversized(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(writer.path); !os.IsNotExist(err) {
		t.Fatal("unbounded legacy file survived retention setup")
	}
}
