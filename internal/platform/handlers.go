package platform

import (
	"context"
	"net/http"
	"time"
)

func (a *App) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ok",
		"serviceName": a.serviceName,
	})
}

// readyHandler is the deep readiness probe: unlike /healthz (process liveness
// only) it checks the configured runtime dependencies and backing store.
// Production MQTT services supply a connection/subscription check; the
// in-memory test harness can omit it.
func (a *App) readyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if a.readinessCheck != nil {
		if err := a.readinessCheck(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "runtime is not ready")
			return
		}
	}
	if pinger, ok := a.store.(interface {
		PingContext(context.Context) error
	}); ok {
		if err := pinger.PingContext(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "store unreachable: "+err.Error())
			return
		}
	}
	// A dependency may disconnect while the store ping was in flight.
	if a.readinessCheck != nil && a.readinessCheck(ctx) != nil {
		writeError(w, http.StatusServiceUnavailable, "runtime is not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ready",
		"serviceName": a.serviceName,
	})
}
