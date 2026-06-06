package platform

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/trace"
)

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

func StartTracing(serviceName string) {
	trace.StartAgent(TraceConfig(serviceName))
}
