package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	"iot/internal/platform"
	"iot/internal/runtimeconfig"
)

func Run() error {
	platform.ConfigureStdLogger("demo")
	platform.StartTracing("demo")

	metrics := platform.NewMetrics()
	cfg := Config{
		TenantCount:       runtimeconfig.Int("DEMO_TENANT_COUNT", 5),
		DevicesPerTenant:  runtimeconfig.Int("DEMO_DEVICES_PER_TENANT", 10),
		TenantPrefix:      runtimeconfig.EnvOrDefault("DEMO_TENANT_PREFIX", "demo"),
		ProductID:         runtimeconfig.EnvOrDefault("DEMO_PRODUCT_ID", "product-demo"),
		TickInterval:      runtimeconfig.Duration("DEMO_TICK_INTERVAL", 250*time.Millisecond),
		TelemetryBurstMin: runtimeconfig.Int("DEMO_TELEMETRY_BURST_MIN", 1),
		TelemetryBurstMax: runtimeconfig.Int("DEMO_TELEMETRY_BURST_MAX", 5),
		CommandBurstMin:   runtimeconfig.Int("DEMO_COMMAND_BURST_MIN", 1),
		CommandBurstMax:   runtimeconfig.Int("DEMO_COMMAND_BURST_MAX", 3),
		Metrics:           metrics,
	}

	admin, err := newHTTPAdminClient(runtimeconfig.EnvOrDefault("DEMO_ADMIN_URL", "http://127.0.0.1:8080"))
	if err != nil {
		return err
	}
	defer admin.Close()

	factory, err := newMQTTBusFactory(MQTTBusFactoryConfig{
		BrokerURL:  runtimeconfig.EnvOrDefault("DEMO_MQTT_URL", "tcp://127.0.0.1:1883"),
		Username:   os.Getenv("DEMO_MQTT_USERNAME"),
		Password:   os.Getenv("DEMO_MQTT_PASSWORD"),
		ClientPref: runtimeconfig.EnvOrDefault("DEMO_CLIENT_ID_PREFIX", "iot-demo"),
	})
	if err != nil {
		return err
	}
	defer factory.Close()

	service := NewService(cfg, admin, factory, randSource())

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	addr := listenAddr()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- runHealthServer(ctx, addr, metrics)
	}()

	runErr := make(chan error, 1)
	go func() {
		runErr <- service.Run(ctx)
	}()

	select {
	case err := <-runErr:
		cancel()
		if err != nil {
			return err
		}
		return nil
	case err := <-serverErr:
		cancel()
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		return nil
	}
}

type httpAdminClient struct {
	baseURL string
	client  *http.Client
}

func newHTTPAdminClient(baseURL string) (*httpAdminClient, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	if baseURL == "" {
		return nil, fmt.Errorf("DEMO_ADMIN_URL is required")
	}
	return &httpAdminClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (c *httpAdminClient) Close() error { return nil }

func (c *httpAdminClient) CreateTenant(ctx context.Context, id, name string) error {
	return c.postJSON(ctx, c.baseURL+"/api/v1/tenants", map[string]any{"id": id, "name": name}, nil, http.StatusConflict)
}

func (c *httpAdminClient) CreateDevice(ctx context.Context, tenantID, deviceID, productID string) error {
	return c.postJSON(ctx, c.baseURL+"/api/v1/devices", map[string]any{
		"tenantId":  tenantID,
		"deviceId":  deviceID,
		"productId": productID,
		"secret":    "demo-secret",
	}, nil, http.StatusConflict)
}

func (c *httpAdminClient) CreateCommand(ctx context.Context, tenantID, deviceID string, payload json.RawMessage) (platform.Command, error) {
	var created platform.Command
	err := c.postJSON(ctx, c.baseURL+"/api/v1/commands", map[string]any{
		"tenantId": tenantID,
		"deviceId": deviceID,
		"payload":  json.RawMessage(payload),
	}, &created)
	return created, err
}

func (c *httpAdminClient) postJSON(ctx context.Context, url string, body any, out any, okStatuses ...int) error {
	ctx, requestID := platform.EnsureRequestID(ctx, "")
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(payload)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", requestID)

	ctx, span := otel.Tracer("demo-http").Start(req.Context(), "admin-api POST "+req.URL.Path,
		oteltrace.WithSpanKind(oteltrace.SpanKindClient))
	defer span.End()
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	req = req.WithContext(ctx)

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	for _, status := range okStatuses {
		if resp.StatusCode == status {
			return nil
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type MQTTBusFactoryConfig struct {
	BrokerURL  string
	Username   string
	Password   string
	ClientPref string
}

type mqttBusFactory struct {
	cfg    MQTTBusFactoryConfig
	mu     sync.Mutex
	closed bool
	buses  []*mqttBus
}

func newMQTTBusFactory(cfg MQTTBusFactoryConfig) (*mqttBusFactory, error) {
	if cfg.BrokerURL == "" {
		return nil, fmt.Errorf("DEMO_MQTT_URL is required")
	}
	if cfg.ClientPref == "" {
		cfg.ClientPref = "iot-demo"
	}
	return &mqttBusFactory{cfg: cfg}, nil
}

func (f *mqttBusFactory) NewClient(ctx context.Context, tenantID string) (Bus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, fmt.Errorf("mqtt factory closed")
	}
	bus, err := newMQTTBus(f.cfg, tenantID)
	if err != nil {
		return nil, err
	}
	f.buses = append(f.buses, bus)
	return bus, nil
}

func (f *mqttBusFactory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	var err error
	for _, bus := range f.buses {
		if busErr := bus.Close(); busErr != nil && err == nil {
			err = busErr
		}
	}
	return err
}

type mqttBus struct {
	client mqtt.Client
	mu     sync.Mutex
	subs   map[string]func(string, []byte)
}

func newMQTTBus(cfg MQTTBusFactoryConfig, tenantID string) (*mqttBus, error) {
	bus := &mqttBus{subs: map[string]func(string, []byte){}}
	opts := mqtt.NewClientOptions().AddBroker(cfg.BrokerURL)
	opts.SetClientID(fmt.Sprintf("%s-%s-%d", cfg.ClientPref, strings.ReplaceAll(tenantID, "/", "-"), time.Now().UnixNano()))
	opts.SetOrderMatters(false)
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
	}
	if cfg.Password != "" {
		opts.SetPassword(cfg.Password)
	}
	opts.SetAutoReconnect(true)
	opts.OnConnect = func(client mqtt.Client) {
		bus.mu.Lock()
		defer bus.mu.Unlock()
		for topic := range bus.subs {
			token := client.Subscribe(topic, 1, func(_ mqtt.Client, msg mqtt.Message) {
				bus.handle(msg.Topic(), msg.Payload())
			})
			token.Wait()
		}
	}
	bus.client = mqtt.NewClient(opts)
	token := bus.client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return nil, err
	}
	return bus, nil
}

func (b *mqttBus) Publish(topic string, payload []byte) error {
	token := b.client.Publish(topic, 1, false, payload)
	token.Wait()
	return token.Error()
}

func (b *mqttBus) Subscribe(topic string, handler func(topic string, payload []byte)) error {
	b.mu.Lock()
	b.subs[topic] = handler
	b.mu.Unlock()

	token := b.client.Subscribe(topic, 1, func(_ mqtt.Client, msg mqtt.Message) {
		b.handle(msg.Topic(), msg.Payload())
	})
	token.Wait()
	return token.Error()
}

func (b *mqttBus) handle(topic string, payload []byte) {
	b.mu.Lock()
	handlers := make([]func(string, []byte), 0, len(b.subs))
	for pattern, handler := range b.subs {
		if topicMatches(pattern, topic) {
			handlers = append(handlers, handler)
		}
	}
	b.mu.Unlock()
	for _, handler := range handlers {
		handler(topic, payload)
	}
}

func (b *mqttBus) Close() error {
	if b == nil || b.client == nil {
		return nil
	}
	b.client.Disconnect(250)
	return nil
}

func runHealthServer(ctx context.Context, addr string, metrics *platform.Metrics) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","serviceName":"demo"}`))
	})
	mux.Handle("/metrics", metrics.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	err := srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func listenAddr() string {
	return runtimeconfig.ListenAddr("LISTEN_ADDR", "PORT", ":8080")
}

func randSource() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}
