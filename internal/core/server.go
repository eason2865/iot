package core

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"

	"iot/internal/platform"
	"iot/internal/runtimeconfig"
	corev1 "iot/proto/core/v1"
)

// auxShutdownTimeout bounds how long the auxiliary HTTP endpoints drain. go-zero
// force-quits the process 5.5s after SIGTERM, so this has to finish well inside
// that window to be graceful rather than merely faster than the kill.
const auxShutdownTimeout = 2 * time.Second

func Run() error {
	platform.ConfigureStdLogger("iot-core")
	metrics := platform.NewMetrics()

	// Background workers, not request handlers, are tied to SIGINT/SIGTERM so
	// they stop between iterations instead of mid-cycle.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, closer, err := buildStore(5 * time.Minute)
	if err != nil {
		return err
	}
	defer func() {
		if closer != nil {
			_ = closer()
		}
	}()
	publisher, closer, err := buildPublisher()
	if err != nil {
		return err
	}
	defer func() {
		if closer != nil {
			_ = closer()
		}
	}()
	if dispatchStore, ok := store.(platform.CommandDispatchStore); ok && publisher != nil {
		go platform.NewCommandDispatcher(dispatchStore, publisher, runtimeconfig.Duration("COMMAND_ACK_TIMEOUT", 5*time.Minute)).Run(ctx)
	}

	server := zrpc.MustNewServer(rpcServerConf(), func(grpcServer *grpc.Server) {
		corev1.RegisterCoreServiceServer(grpcServer, NewService(store, publisher))
	})
	server.AddUnaryInterceptors(platform.UnaryServerRequestIDInterceptor(), metrics.UnaryServerInterceptor())
	tlsCreds, err := platform.GRPCServerTLSCredentials()
	if err != nil {
		return err
	}
	if tlsCreds != nil {
		log.Printf("iot-core gRPC mTLS enabled")
		server.AddOptions(grpc.Creds(tlsCreds))
	}

	// The auxiliary HTTP endpoints are bound here, before the gRPC server starts
	// serving, so that a port conflict fails startup instead of leaving the
	// endpoint silently down. That matters most for the MQTT authentication
	// callback: it is fail-closed, so an unreachable one rejects every device
	// while the pod still reports Ready.
	auxServers := make([]*auxHTTPServer, 0, 2)
	if authenticator, ok := store.(platform.DeviceAuthenticator); ok {
		mqttAuth, err := newAuxHTTPServer(
			"mqtt-auth",
			runtimeconfig.EnvOrDefault("IOT_CORE_MQTT_AUTH_LISTEN", ":9090"),
			mqttAuthMux(
				authenticator,
				runtimeconfig.EnvOrDefault("EMQX_INTERNAL_PASSWORD", ""),
				runtimeconfig.EnvOrDefault("IOT_CORE_MQTT_AUTH_TOKEN", ""),
			),
		)
		if err != nil {
			return err
		}
		auxServers = append(auxServers, mqttAuth)
	}
	metricsServer, err := newAuxHTTPServer(
		"metrics",
		fmt.Sprintf("%s:%d", iotCorePrometheusHost(), iotCorePrometheusPort()),
		metricsMux(metrics.Handler(), runtimeconfig.EnvOrDefault("IOT_CORE_PROMETHEUS_PATH", "/metrics")),
	)
	if err != nil {
		closeAuxServers(auxServers)
		return err
	}
	auxServers = append(auxServers, metricsServer)

	for _, aux := range auxServers {
		go aux.Serve()
	}
	// Drain the auxiliary endpoints on the same signal that stops the dispatcher:
	// in-flight scrapes and auth callbacks finish, and the listeners close, rather
	// than the process dying with connections half-served.
	auxDrained := make(chan struct{})
	go func() {
		defer close(auxDrained)
		<-ctx.Done()
		shutdownAuxServers(auxServers)
	}()

	server.Start()

	// Wait for that drain before returning. go-zero's gRPC GracefulStop can finish
	// in about a second (it runs one second after the signal), while the drain is
	// allowed auxShutdownTimeout, so returning immediately would exit the process
	// underneath an in-flight request. The wait is bounded in case the signal path
	// never ran, which is what would make it block forever.
	select {
	case <-auxDrained:
	case <-time.After(auxShutdownTimeout + time.Second):
		log.Printf("iot-core auxiliary servers did not finish draining in time")
	}
	return nil
}

// auxHTTPServer is one of iot-core's auxiliary HTTP endpoints: the EMQX
// authentication callback and the metrics endpoint. Both are plain HTTP servers
// created through newAuxHTTPServer so they can be bound up front and shut down
// with the process.
type auxHTTPServer struct {
	name     string
	listener net.Listener
	server   *http.Server
}

func newAuxHTTPServer(name, addr string, handler http.Handler) (*auxHTTPServer, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("bind iot-core %s listener on %s: %w", name, addr, err)
	}
	return &auxHTTPServer{
		name:     name,
		listener: listener,
		server: &http.Server{
			Handler: handler,
			// The MQTT auth endpoint is reachable from the emqx namespace, so a
			// client that opens a connection and stalls must not pin it.
			ReadHeaderTimeout: 5 * time.Second,
		},
	}, nil
}

// Serve blocks until the server stops. ErrServerClosed is the expected shutdown
// path, not a failure.
func (a *auxHTTPServer) Serve() {
	log.Printf("starting iot-core %s server at %s", a.name, a.listener.Addr())
	if err := a.server.Serve(a.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("iot-core %s server stopped: %v", a.name, err)
	}
}

func shutdownAuxServers(servers []*auxHTTPServer) {
	ctx, cancel := context.WithTimeout(context.Background(), auxShutdownTimeout)
	defer cancel()
	for _, aux := range servers {
		if err := aux.server.Shutdown(ctx); err != nil {
			log.Printf("iot-core %s server shutdown: %v", aux.name, err)
			continue
		}
		log.Printf("iot-core %s server stopped gracefully", aux.name)
	}
}

// closeAuxServers releases listeners bound before a later startup step failed.
func closeAuxServers(servers []*auxHTTPServer) {
	for _, aux := range servers {
		_ = aux.listener.Close()
	}
}

func metricsMux(handler http.Handler, path string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	return mux
}

func mqttAuthMux(authenticator platform.DeviceAuthenticator, internalPassword, callbackToken string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/internal/mqtt/authenticate", platform.MQTTAuthenticationHandler(authenticator, internalPassword, callbackToken))
	return mux
}

func rpcServerConf() zrpc.RpcServerConf {
	return zrpc.RpcServerConf{
		ServiceConf: service.ServiceConf{
			Name:      "iot-core",
			Telemetry: platform.TraceConfig("iot-core"),
		},
		ListenOn: rpcListenOn(),
		Middlewares: zrpc.ServerMiddlewaresConf{
			Trace:      true,
			Recover:    true,
			Stat:       true,
			Prometheus: true,
			Breaker:    true,
		},
	}
}

func buildStore(ttl time.Duration) (platform.Repository, func() error, error) {
	dsn := runtimeconfig.EnvOrDefault("POSTGRES_DSN", "postgres://iot:iot123@localhost:5432/iot?sslmode=disable")
	store, err := platform.NewPostgresStore(dsn, ttl)
	if err != nil {
		return nil, nil, err
	}
	return store, store.Close, nil
}

func buildPublisher() (platform.MessagePublisher, func() error, error) {
	brokers := runtimeconfig.SplitCSV(runtimeconfig.EnvOrDefault("KAFKA_BROKERS", "localhost:9092"))
	publisher := platform.NewKafkaPublisher(platform.KafkaPublisherConfig{
		Brokers:        brokers,
		TelemetryTopic: runtimeconfig.EnvOrDefault("KAFKA_TELEMETRY_TOPIC", "iot.telemetry"),
		CommandTopic:   runtimeconfig.EnvOrDefault("KAFKA_COMMAND_TOPIC", "iot.command"),
		TopicConfig:    topicConfigFromEnv(),
	}, nil)
	if publisher == nil {
		return nil, nil, nil
	}
	return publisher, publisher.Close, nil
}

// topicConfigFromEnv carries the topic durability settings every service shares.
// They only apply when this service creates a missing topic; an existing topic
// keeps its configuration, so raising these on a live cluster is an operational
// change (see docs/adr/0004).
func topicConfigFromEnv() platform.KafkaTopicConfig {
	return platform.KafkaTopicConfig{
		ReplicationFactor: runtimeconfig.KafkaTopicReplicationFactor(),
		MinInsyncReplicas: runtimeconfig.KafkaTopicMinInsyncReplicas(),
	}
}

func rpcListenOn() string {
	if addr := os.Getenv("IOT_CORE_LISTEN_ON"); addr != "" {
		return addr
	}
	if addr := os.Getenv("LISTEN_ADDR"); addr != "" && strings.HasPrefix(addr, ":") {
		if _, err := strconv.Atoi(strings.TrimPrefix(addr, ":")); err == nil {
			return addr
		}
	}
	return ":9001"
}

func iotCorePrometheusHost() string {
	return runtimeconfig.EnvOrDefault("IOT_CORE_PROMETHEUS_HOST", "0.0.0.0")
}

func iotCorePrometheusPort() int {
	return runtimeconfig.Int("IOT_CORE_PROMETHEUS_PORT", 9101)
}
