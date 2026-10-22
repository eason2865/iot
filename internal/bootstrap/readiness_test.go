package bootstrap

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"iot/internal/platform"
)

type unusedPublisher struct{}

func (*unusedPublisher) PublishTelemetry(context.Context, platform.TelemetryRecord) error { return nil }
func (*unusedPublisher) PublishCommand(context.Context, platform.Command) error           { return nil }

func TestRuntimeNotReadyBeforeMQTTSubscription(t *testing.T) {
	for _, name := range []string{"telemetry-ingestor", "device-worker"} {
		t.Run(name, func(t *testing.T) {
			resources := &runtimeResources{metrics: platform.NewMetrics()}
			if name == "telemetry-ingestor" {
				resources.bridge = platform.NewMQTTBridge(platform.MQTTBridgeConfig{BrokerURL: "tcp://127.0.0.1:1"}, &unusedPublisher{}, nil)
			} else {
				resources.worker = platform.NewWorker(platform.WorkerConfig{MQTTBrokerURL: "tcp://127.0.0.1:1"}, nil, nil, nil)
			}
			app := newRuntimeApp(name, resources)
			for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable} {
				res := httptest.NewRecorder()
				app.Router().ServeHTTP(res, httptest.NewRequest(http.MethodGet, path, nil))
				if res.Code != want {
					t.Errorf("%s=%d want=%d", path, res.Code, want)
				}
			}
		})
	}
}
