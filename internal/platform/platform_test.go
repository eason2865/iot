package platform_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"iot/internal/platform"
)

func TestOperationalRouterExcludesBusinessAPI(t *testing.T) {
	for _, serviceName := range []string{"management-api", "device-worker", "telemetry-ingestor", "iot-core"} {
		app := platform.New(platform.Config{ServiceName: serviceName})
		ts := httptest.NewServer(app.Router())

		resp, err := http.Get(ts.URL + "/api/v1/tenants")
		if err != nil {
			ts.Close()
			t.Fatalf("http.Get() error = %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			ts.Close()
			t.Fatalf("service %q: GET /api/v1/tenants status = %d, want %d", serviceName, resp.StatusCode, http.StatusNotFound)
		}

		health, err := http.Get(ts.URL + "/healthz")
		if err != nil {
			ts.Close()
			t.Fatalf("http.Get() error = %v", err)
		}
		_ = health.Body.Close()
		ts.Close()
		if health.StatusCode != http.StatusOK {
			t.Fatalf("service %q: GET /healthz status = %d, want %d", serviceName, health.StatusCode, http.StatusOK)
		}
	}
}

// failingPingStore wraps a Repository with a PingContext that always fails,
// emulating a PostgreSQL connection that is down while the process is alive.
type failingPingStore struct {
	platform.Repository
}

func (failingPingStore) PingContext(context.Context) error {
	return errPingFailed
}

var errPingFailed = errors.New("connection refused")

// TestReadyzFailsClosedOnStoreOutage pins the deep readiness probe: /healthz
// reports process liveness only, so /readyz must turn 503 when the backing
// store is unreachable — otherwise a pod with a dead PostgreSQL link keeps
// receiving traffic while reporting "ok".
func TestReadyzFailsClosedOnStoreOutage(t *testing.T) {
	healthy := platform.New(platform.Config{ServiceName: "device-worker"})
	tsHealthy := httptest.NewServer(healthy.Router())
	defer tsHealthy.Close()
	resp, err := http.Get(tsHealthy.URL + "/readyz")
	if err != nil {
		t.Fatalf("http.Get() error = %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthy: GET /readyz status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	degraded := platform.New(platform.Config{ServiceName: "device-worker", Store: failingPingStore{}})
	tsDegraded := httptest.NewServer(degraded.Router())
	defer tsDegraded.Close()
	resp, err = http.Get(tsDegraded.URL + "/readyz")
	if err != nil {
		t.Fatalf("http.Get() error = %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("failing store: GET /readyz status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	// Liveness must stay shallow: a dependency outage is not a reason to
	// restart the process.
	resp, err = http.Get(tsDegraded.URL + "/healthz")
	if err != nil {
		t.Fatalf("http.Get() error = %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("failing store: GET /healthz status = %d, want %d (liveness stays shallow)", resp.StatusCode, http.StatusOK)
	}
}
