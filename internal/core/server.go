package core

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"

	"iot/internal/platform"
	"iot/internal/runtimeconfig"
	corev1 "iot/proto/core/v1"
)

func Run() error {
	platform.ConfigureStdLogger("iot-core")
	metrics := platform.NewMetrics()
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
		go platform.NewCommandDispatcher(dispatchStore, publisher, runtimeconfig.Duration("COMMAND_ACK_TIMEOUT", 5*time.Minute)).Run(context.Background())
	}
	if authenticator, ok := store.(platform.DeviceAuthenticator); ok {
		go serveMQTTAuthentication(
			authenticator,
			runtimeconfig.EnvOrDefault("EMQX_INTERNAL_PASSWORD", ""),
			runtimeconfig.EnvOrDefault("IOT_CORE_MQTT_AUTH_TOKEN", ""),
		)
	}

	server := zrpc.MustNewServer(rpcServerConf(), func(grpcServer *grpc.Server) {
		corev1.RegisterCoreServiceServer(grpcServer, NewService(store, publisher))
	})
	server.AddUnaryInterceptors(platform.UnaryServerRequestIDInterceptor(), metrics.UnaryServerInterceptor())

	go serveIotCoreMetrics(metrics.Handler(), iotCorePrometheusHost(), iotCorePrometheusPort(), runtimeconfig.EnvOrDefault("IOT_CORE_PROMETHEUS_PATH", "/metrics"))

	server.Start()
	return nil
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
	}, nil)
	if publisher == nil {
		return nil, nil, nil
	}
	return publisher, publisher.Close, nil
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

func serveIotCoreMetrics(handler http.Handler, host string, port int, path string) {
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("starting iot-core metrics server at %s%s", addr, path)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("iot-core metrics server stopped: %v", err)
	}
}

func serveMQTTAuthentication(authenticator platform.DeviceAuthenticator, internalPassword, callbackToken string) {
	mux := http.NewServeMux()
	mux.Handle("/internal/mqtt/authenticate", platform.MQTTAuthenticationHandler(authenticator, internalPassword, callbackToken))
	addr := runtimeconfig.EnvOrDefault("IOT_CORE_MQTT_AUTH_LISTEN", ":9090")
	log.Printf("starting iot-core MQTT authentication server at %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("iot-core MQTT authentication server stopped: %v", err)
	}
}
