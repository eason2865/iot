package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"iot/internal/contracts"
	"iot/internal/platform"
	"iot/internal/runtimeconfig"
)

type runtimeResources struct {
	store     platform.Repository
	publisher platform.MessagePublisher
	worker    *platform.Worker
	bridge    *platform.MQTTBridge
	metrics   *platform.Metrics
	closers   []func() error
}

func Run(serviceName string) error {
	platform.ConfigureStdLogger(serviceName)
	platform.StartTracing(serviceName)

	resources, err := buildRuntime(serviceName)
	if err != nil {
		return err
	}
	for _, closer := range resources.closers {
		if closer != nil {
			defer closer()
		}
	}

	app := platform.New(platform.Config{
		ServiceName:        serviceName,
		DeviceHeartbeatTTL: 5 * time.Minute,
		Store:              resources.store,
		Publisher:          resources.publisher,
		Metrics:            resources.metrics,
	})
	srv := &http.Server{
		Addr:    listenAddr(),
		Handler: app.Router(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 4)
	if resources.bridge != nil {
		go func() {
			errCh <- resources.bridge.Run(ctx)
		}()
	}
	if resources.worker != nil {
		go func() {
			errCh <- resources.worker.Run(ctx)
		}()
	}
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(shutdownCh)

	select {
	case sig := <-shutdownCh:
		cancel()
		ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelShutdown()
		if err := srv.Shutdown(ctxShutdown); err != nil {
			return fmt.Errorf("shutdown after %s: %w", sig, err)
		}
		return nil
	case err := <-errCh:
		cancel()
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func buildRuntime(serviceName string) (*runtimeResources, error) {
	ttl := 5 * time.Minute
	res := &runtimeResources{
		metrics: platform.NewMetrics(),
	}

	store, closer, err := buildStore(ttl)
	if err != nil {
		return nil, err
	}
	res.store = store
	if closer != nil {
		res.closers = append(res.closers, closer)
	}

	publisher, closer, err := buildPublisher(res.metrics)
	if err != nil {
		return nil, err
	}
	res.publisher = publisher
	if closer != nil {
		res.closers = append(res.closers, closer)
	}

	switch serviceName {
	case "ingress":
		bridge := platform.NewMQTTBridge(platform.MQTTBridgeConfig{
			BrokerURL:   runtimeconfig.EnvOrDefault("EMQX_URL", "tcp://127.0.0.1:1883"),
			ClientID:    runtimeconfig.EnvOrDefault("EMQX_INGRESS_CLIENT_ID", "iot-ingress"),
			Username:    os.Getenv("EMQX_USERNAME"),
			Password:    os.Getenv("EMQX_PASSWORD"),
			TopicFilter: runtimeconfig.EnvOrDefault("EMQX_TOPIC_FILTER", contracts.TelemetryTopicFilter),
		}, publisher, res.metrics)
		res.bridge = bridge
	case "worker":
		tdWriter, closer, err := buildTDengineWriter(res.metrics)
		if err != nil {
			return nil, err
		}
		if closer != nil {
			res.closers = append(res.closers, closer)
		}
		res.worker = platform.NewWorker(platform.WorkerConfig{
			KafkaBrokers:   runtimeconfig.SplitCSV(runtimeconfig.EnvOrDefault("KAFKA_BROKERS", "localhost:9092")),
			TelemetryTopic: runtimeconfig.EnvOrDefault("KAFKA_TELEMETRY_TOPIC", "iot.telemetry"),
			CommandTopic:   runtimeconfig.EnvOrDefault("KAFKA_COMMAND_TOPIC", "iot.command"),
			MQTTBrokerURL:  runtimeconfig.EnvOrDefault("EMQX_URL", "tcp://127.0.0.1:1883"),
			MQTTClientID:   runtimeconfig.EnvOrDefault("EMQX_WORKER_CLIENT_ID", "iot-worker"),
			MQTTUsername:   os.Getenv("EMQX_USERNAME"),
			MQTTPassword:   os.Getenv("EMQX_PASSWORD"),
		}, store, tdWriter, res.metrics)
	}

	return res, nil
}

func buildStore(ttl time.Duration) (platform.Repository, func() error, error) {
	dsn := runtimeconfig.EnvOrDefault("POSTGRES_DSN", "postgres://iot:iot123@localhost:5432/iot?sslmode=disable")
	store, err := platform.NewPostgresStore(dsn, ttl)
	if err != nil {
		return nil, nil, err
	}
	return store, store.Close, nil
}

func buildPublisher(metrics *platform.Metrics) (platform.MessagePublisher, func() error, error) {
	brokers := runtimeconfig.SplitCSV(runtimeconfig.EnvOrDefault("KAFKA_BROKERS", "localhost:9092"))
	publisher := platform.NewKafkaPublisher(platform.KafkaPublisherConfig{
		Brokers:        brokers,
		TelemetryTopic: runtimeconfig.EnvOrDefault("KAFKA_TELEMETRY_TOPIC", "iot.telemetry"),
		CommandTopic:   runtimeconfig.EnvOrDefault("KAFKA_COMMAND_TOPIC", "iot.command"),
	}, metrics)
	if publisher == nil {
		return nil, nil, nil
	}
	return publisher, publisher.Close, nil
}

func buildTDengineWriter(metrics *platform.Metrics) (*platform.TDengineWriter, func() error, error) {
	dsn := runtimeconfig.EnvOrDefault("TDENGINE_DSN", "root:taosdata@http(127.0.0.1:6041)/iot")
	writer, err := platform.NewTDengineWriter(platform.TDengineConfig{
		DSN:   dsn,
		Table: runtimeconfig.EnvOrDefault("TDENGINE_TABLE", "telemetry"),
	}, metrics)
	if err != nil {
		return nil, nil, err
	}
	return writer, writer.Close, nil
}

func listenAddr() string {
	return runtimeconfig.ListenAddr("LISTEN_ADDR", "PORT", ":8080")
}
