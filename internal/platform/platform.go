package platform

import (
	"net/http"
	"time"
)

type Config struct {
	ServiceName        string
	DeviceHeartbeatTTL time.Duration
	Store              Repository
	Publisher          MessagePublisher
	Metrics            *Metrics
}

type App struct {
	serviceName string
	store       Repository
	publisher   MessagePublisher
	metrics     *Metrics
	ttl         time.Duration
	router      http.Handler
}

func New(cfg Config) *App {
	ttl := cfg.DeviceHeartbeatTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	app := &App{
		serviceName: cfg.ServiceName,
		store:       cfg.Store,
		publisher:   cfg.Publisher,
		ttl:         ttl,
	}
	if app.store == nil {
		app.store = newMemoryStore(ttl)
	}
	if app.publisher == nil {
		app.publisher = noopPublisher{}
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NewMetrics()
	}
	app.metrics = cfg.Metrics
	app.router = app.routes()
	return app
}

func (a *App) Router() http.Handler {
	return a.router
}

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.healthHandler)
	mux.Handle("/metrics", a.metrics.Handler())
	mux.HandleFunc("/openapi.json", a.openapiHandler)
	mux.HandleFunc("/schemas/mqtt-envelope.json", a.mqttEnvelopeSchemaHandler)
	mux.HandleFunc("/api/v1/tenants", a.handleTenants)
	mux.HandleFunc("/api/v1/devices", a.handleDevices)
	mux.HandleFunc("/api/v1/telemetry", a.handleTelemetry)
	mux.HandleFunc("/api/v1/commands", a.handleCommands)
	mux.HandleFunc("/api/v1/commands/", a.handleCommandByID)
	mux.HandleFunc("/api/v1/devices/", a.handleDeviceByID)
	return a.observeHTTP(mux)
}

func (a *App) observeHTTP(next http.Handler) http.Handler {
	return RequestIDHTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rw := &observedResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		if a.metrics != nil {
			a.metrics.ObserveHTTPRequest(routeLabel(r.URL.Path), r.Method, rw.status, time.Since(start))
		}
	}))
}
