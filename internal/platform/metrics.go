package platform

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Metrics struct {
	registry               *prometheus.Registry
	httpRequestsTotal      *prometheus.CounterVec
	httpRequestDuration    *prometheus.HistogramVec
	grpcRequestsTotal      *prometheus.CounterVec
	grpcRequestDuration    *prometheus.HistogramVec
	tenantsTotal           *prometheus.CounterVec
	devicesTotal           *prometheus.CounterVec
	telemetryIngestedTotal *prometheus.CounterVec
	commandsTotal          *prometheus.CounterVec
	kafkaPublishTotal      *prometheus.CounterVec
	mqttBridgeTotal        *prometheus.CounterVec
	workerTotal            *prometheus.CounterVec
	tdengineWriteTotal     *prometheus.CounterVec
	demoEventsTotal        *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		registry: registry,
		httpRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iot_http_requests_total",
			Help: "Total number of HTTP requests handled by the service.",
		}, []string{"route", "method", "status"}),
		httpRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "iot_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method", "status"}),
		grpcRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iot_grpc_requests_total",
			Help: "Total number of gRPC requests handled by the service.",
		}, []string{"method", "code"}),
		grpcRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "iot_grpc_request_duration_seconds",
			Help:    "gRPC request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "code"}),
		tenantsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iot_tenants_total",
			Help: "Tenant operation counts.",
		}, []string{"result"}),
		devicesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iot_devices_total",
			Help: "Device operation counts.",
		}, []string{"result"}),
		telemetryIngestedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iot_telemetry_ingested_total",
			Help: "Telemetry operation counts.",
		}, []string{"result"}),
		commandsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iot_commands_total",
			Help: "Command operation counts.",
		}, []string{"kind", "result"}),
		kafkaPublishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iot_kafka_publish_total",
			Help: "Kafka publish counts.",
		}, []string{"kind", "result"}),
		mqttBridgeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iot_mqtt_bridge_total",
			Help: "MQTT bridge message counts.",
		}, []string{"result"}),
		workerTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iot_worker_total",
			Help: "Worker pipeline event counts.",
		}, []string{"kind", "result"}),
		tdengineWriteTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iot_tdengine_write_total",
			Help: "TDengine write counts.",
		}, []string{"result"}),
		demoEventsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "iot_demo_events_total",
			Help: "Demo simulator event counts.",
		}, []string{"kind", "result"}),
	}

	registry.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
		m.httpRequestsTotal,
		m.httpRequestDuration,
		m.grpcRequestsTotal,
		m.grpcRequestDuration,
		m.tenantsTotal,
		m.devicesTotal,
		m.telemetryIngestedTotal,
		m.commandsTotal,
		m.kafkaPublishTotal,
		m.mqttBridgeTotal,
		m.workerTotal,
		m.tdengineWriteTotal,
		m.demoEventsTotal,
	)
	return m
}

func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.registry
}

func (m *Metrics) Handler() http.Handler {
	if m == nil || m.registry == nil {
		return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) HTTPMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}
			start := time.Now()
			rw := &observedResponseWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)
			m.ObserveHTTPRequest(routeLabel(r.URL.Path), r.Method, rw.status, time.Since(start))
		})
	}
}

func (m *Metrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err).String()
		if m != nil {
			m.grpcRequestsTotal.WithLabelValues(info.FullMethod, code).Inc()
			m.grpcRequestDuration.WithLabelValues(info.FullMethod, code).Observe(time.Since(start).Seconds())
		}
		return resp, err
	}
}

func (m *Metrics) ObserveHTTPRequest(route, method string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	labels := prometheus.Labels{
		"route":  route,
		"method": method,
		"status": statusLabel(status),
	}
	m.httpRequestsTotal.With(labels).Inc()
	m.httpRequestDuration.With(labels).Observe(duration.Seconds())
}

func (m *Metrics) IncTenant(result string) {
	incCounterVec(m.tenantsTotal, result)
}

func (m *Metrics) IncDevice(result string) {
	incCounterVec(m.devicesTotal, result)
}

func (m *Metrics) IncTelemetry(result string) {
	incCounterVec(m.telemetryIngestedTotal, result)
}

func (m *Metrics) IncCommand(kind, result string) {
	incCounterVec(m.commandsTotal, kind, result)
}

func (m *Metrics) IncKafkaPublish(kind, result string) {
	incCounterVec(m.kafkaPublishTotal, kind, result)
}

func (m *Metrics) IncMQTTBridge(result string) {
	incCounterVec(m.mqttBridgeTotal, result)
}

func (m *Metrics) IncWorker(kind, result string) {
	incCounterVec(m.workerTotal, kind, result)
}

func (m *Metrics) IncTDengineWrite(result string) {
	incCounterVec(m.tdengineWriteTotal, result)
}

func (m *Metrics) IncDemo(kind, result string) {
	incCounterVec(m.demoEventsTotal, kind, result)
}

func incCounterVec(vec *prometheus.CounterVec, labels ...string) {
	if vec == nil {
		return
	}
	vec.WithLabelValues(labels...).Inc()
}

func statusLabel(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "2xx"
	case status >= 300 && status < 400:
		return "3xx"
	case status >= 400 && status < 500:
		return "4xx"
	case status >= 500:
		return "5xx"
	default:
		return "unknown"
	}
}

func routeLabel(path string) string {
	switch {
	case path == "/healthz":
		return "/healthz"
	case path == "/metrics":
		return "/metrics"
	case path == "/openapi.json":
		return "/openapi.json"
	case path == "/schemas/mqtt-envelope.json":
		return "/schemas/mqtt-envelope.json"
	case path == "/api/v1/tenants":
		return "/api/v1/tenants"
	case path == "/api/v1/devices":
		return "/api/v1/devices"
	case path == "/api/v1/telemetry":
		return "/api/v1/telemetry"
	case path == "/api/v1/commands":
		return "/api/v1/commands"
	case strings.HasPrefix(path, "/api/v1/commands/") && strings.HasSuffix(path, "/ack"):
		return "/api/v1/commands/{id}/ack"
	case strings.HasPrefix(path, "/api/v1/commands/"):
		return "/api/v1/commands/{id}"
	case strings.HasPrefix(path, "/api/v1/devices/") && strings.HasSuffix(path, "/status"):
		return "/api/v1/devices/{tenantId}/{deviceId}/status"
	case strings.HasPrefix(path, "/api/v1/devices/") && strings.HasSuffix(path, "/telemetry"):
		return "/api/v1/devices/{tenantId}/{deviceId}/telemetry"
	case strings.HasPrefix(path, "/api/v1/devices/"):
		return "/api/v1/devices/{tenantId}/{deviceId}"
	default:
		return "other"
	}
}
