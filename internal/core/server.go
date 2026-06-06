package core

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"

	"iot/internal/platform"
	corev1 "iot/proto/core/v1"
)

func Run() error {
	platform.ConfigureStdLogger("core-rpc")
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
			Hosts: splitCSV(envOrDefault("CORE_RPC_ETCD_HOSTS", "localhost:2379")),
			Key:   envOrDefault("CORE_RPC_ETCD_KEY", "iot/core-rpc"),
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
	server.AddUnaryInterceptors(platform.UnaryServerRequestIDInterceptor())

	server.Start()
	return nil
}

func buildStore(ttl time.Duration) (platform.Repository, func() error, error) {
	dsn := envOrDefault("POSTGRES_DSN", "postgres://iot:iot123@localhost:5432/iot?sslmode=disable")
	store, err := platform.NewPostgresStore(dsn, ttl)
	if err != nil {
		return nil, nil, err
	}
	return store, store.Close, nil
}

func buildPublisher() (platform.MessagePublisher, func() error, error) {
	brokers := splitCSV(envOrDefault("KAFKA_BROKERS", "localhost:9092"))
	publisher := platform.NewKafkaPublisher(platform.KafkaPublisherConfig{
		Brokers:        brokers,
		TelemetryTopic: envOrDefault("KAFKA_TELEMETRY_TOPIC", "iot.telemetry"),
		CommandTopic:   envOrDefault("KAFKA_COMMAND_TOPIC", "iot.command"),
	}, nil)
	if publisher == nil {
		return nil, nil, nil
	}
	return publisher, publisher.Close, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
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
