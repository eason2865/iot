package platform

import (
	"context"
	"net/http"
)

type Config struct {
	ServiceName    string
	Store          Repository
	Metrics        *Metrics
	ReadinessCheck func(context.Context) error
}

type App struct {
	serviceName    string
	store          Repository
	metrics        *Metrics
	router         http.Handler
	readinessCheck func(context.Context) error
}

func New(cfg Config) *App {
	app := &App{
		serviceName:    cfg.ServiceName,
		store:          cfg.Store,
		readinessCheck: cfg.ReadinessCheck,
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
	return RequestIDHTTPMiddleware(a.metrics.HTTPMiddleware()(mux))
}
