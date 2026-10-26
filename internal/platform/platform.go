package platform

import (
	"context"
	"net/http"
	"time"
)

type Config struct {
	ServiceName        string
	DeviceHeartbeatTTL time.Duration
	Store              Repository
	Publisher          MessagePublisher
	Metrics            *Metrics
	ReadinessCheck     func(context.Context) error
	// EnableBusinessAPI registers the tenant/device/telemetry/command REST
	// endpoints. It is opt-in and defaults to false for every service: those
	// endpoints carry no authentication (see handlers.go), so enabling them by
	// service name would expose unauthenticated writes to any process that
	// happens to be called "management-api". Production REST is served by
	// internal/adminapi behind a bearer token; this flag exists for the test
	// harness and for a deliberately gated in-process HTTP surface.
	EnableBusinessAPI bool
}

type App struct {
	serviceName    string
	store          Repository
	publisher      MessagePublisher
	metrics        *Metrics
	ttl            time.Duration
	enableAPI      bool
	router         http.Handler
	readinessCheck func(context.Context) error
}

func New(cfg Config) *App {
	ttl := cfg.DeviceHeartbeatTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	app := &App{
		serviceName:    cfg.ServiceName,
		store:          cfg.Store,
		publisher:      cfg.Publisher,
		readinessCheck: cfg.ReadinessCheck,
		ttl:            ttl,
		// Business REST is strictly opt-in: never inferred from ServiceName,
		// because these routes have no auth middleware.
		enableAPI: cfg.EnableBusinessAPI,
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
	mux.HandleFunc("/readyz", a.readyHandler)
	mux.Handle("/metrics", a.metrics.Handler())
	if a.enableAPI {
		mux.HandleFunc("/schemas/mqtt-envelope.json", a.mqttEnvelopeSchemaHandler)
		for pattern, handler := range map[string]http.HandlerFunc{
			"GET /api/v1/tenants": a.handleTenants, "POST /api/v1/tenants": a.handleTenants,
			"GET /api/v1/tenants/{tenantId}/devices": a.handleDevices, "POST /api/v1/tenants/{tenantId}/devices": a.handleDevices,
			"GET /api/v1/tenants/{tenantId}/devices/{deviceId}":                                        a.handleDeviceByID,
			"GET /api/v1/tenants/{tenantId}/devices/{deviceId}/status":                                 a.handleDeviceByID,
			"GET /api/v1/tenants/{tenantId}/devices/{deviceId}/telemetry":                              a.handleDeviceByID,
			"POST /api/v1/tenants/{tenantId}/devices/{deviceId}/telemetry":                             a.handleTelemetry,
			"POST /api/v1/tenants/{tenantId}/devices/{deviceId}/commands":                              a.handleCommands,
			"GET /api/v1/tenants/{tenantId}/commands":                                                  a.handleCommands,
			"GET /api/v1/tenants/{tenantId}/commands/{commandId}":                                      a.handleCommandByID,
			"POST /api/v1/tenants/{tenantId}/devices/{deviceId}/commands/{commandId}/acknowledgements": a.handleCommandByID,
		} {
			mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
				p, err := ManagementRequest(r)
				if err != nil {
					writeError(w, 400, err.Error())
					return
				}
				handler(w, withManagementParameters(r, p))
			})
		}
	}
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
