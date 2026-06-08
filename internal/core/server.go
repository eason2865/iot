package core

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"

	"iot/internal/platform"
	"iot/internal/runtimeconfig"
	corev1 "iot/proto/core/v1"
)

func Run() error {
	platform.ConfigureStdLogger("core-rpc")
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

	server := zrpc.MustNewServer(zrpc.RpcServerConf{
		ServiceConf: service.ServiceConf{
			Name:      "core-rpc",
			Telemetry: platform.TraceConfig("core-rpc"),
		},
		ListenOn: rpcListenOn(),
		Etcd: discov.EtcdConf{
			Hosts: runtimeconfig.SplitCSV(runtimeconfig.EnvOrDefault("CORE_RPC_ETCD_HOSTS", "localhost:2379")),
			Key:   runtimeconfig.EnvOrDefault("CORE_RPC_ETCD_KEY", "iot/core-rpc"),
		},
		Middlewares: zrpc.ServerMiddlewaresConf{
			Trace:      true,
			Recover:    true,
			Stat:       true,
			Prometheus: true,
			Breaker:    true,
		},
	}, func(grpcServer *grpc.Server) {
		corev1.RegisterCoreServiceServer(grpcServer, NewService(store, publisher))
	})
	server.AddUnaryInterceptors(platform.UnaryServerRequestIDInterceptor(), metrics.UnaryServerInterceptor())

	go serveCoreRPCMetrics(metrics.Handler(), coreRPCPrometheusHost(), coreRPCPrometheusPort(), runtimeconfig.EnvOrDefault("CORE_RPC_PROMETHEUS_PATH", "/metrics"))

	server.Start()
	return nil
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
	if addr := os.Getenv("CORE_RPC_LISTEN_ON"); addr != "" {
		return addr
	}
	if addr := os.Getenv("LISTEN_ADDR"); addr != "" && strings.HasPrefix(addr, ":") {
		if _, err := strconv.Atoi(strings.TrimPrefix(addr, ":")); err == nil {
			return addr
		}
	}
	return ":9001"
}

func coreRPCPrometheusHost() string {
	return runtimeconfig.EnvOrDefault("CORE_RPC_PROMETHEUS_HOST", "0.0.0.0")
}

func coreRPCPrometheusPort() int {
	return runtimeconfig.Int("CORE_RPC_PROMETHEUS_PORT", 9101)
}

func serveCoreRPCMetrics(handler http.Handler, host string, port int, path string) {
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("starting core-rpc metrics server at %s%s", addr, path)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("core-rpc metrics server stopped: %v", err)
	}
}
