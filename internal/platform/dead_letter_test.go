package platform

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

// TestPublishDeadLetterCountsEveryOutcome pins the dead-letter visibility
// metric. A failed dead-letter write is not an ordinary error: the message was
// not preserved, so the consumer stays blocked or the worker restarts, and that
// state has to be countable rather than only logged.
func TestPublishDeadLetterCountsEveryOutcome(t *testing.T) {
	metrics := NewMetrics()

	// No writer configured: the message is explicitly NOT dead-lettered.
	if err := publishDeadLetter(context.Background(), nil, kafka.Message{Topic: "iot.telemetry", Value: []byte("x")}, StageTelemetryTDengine, errors.New("tdengine down"), metrics); err == nil {
		t.Fatal("publishDeadLetter() without a writer returned nil")
	}

	body := scrapeMetrics(t, metrics)
	if want := `iot_dlq_publish_total{result="error",stage="telemetry.tdengine"} 1`; !strings.Contains(body, want) {
		t.Fatalf("metrics missing %q\n%s", want, body)
	}
	// The sibling series is seeded so an alert can be written against a series
	// that exists before the first failure.
	if want := `iot_dlq_publish_total{result="ok",stage="telemetry.tdengine"} 0`; !strings.Contains(body, want) {
		t.Fatalf("metrics missing seeded series %q", want)
	}
	// Every stage is seeded, so a new stage cannot be missed when alerting.
	for _, stage := range DeadLetterStages() {
		if !strings.Contains(body, `stage="`+stage+`"`) {
			t.Fatalf("stage %q is not present in the DLQ metric", stage)
		}
	}
}

func scrapeMetrics(t *testing.T, metrics *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	return string(body)
}
