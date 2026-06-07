package platform_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
	_ "github.com/taosdata/driver-go/v3/taosRestful"

	"iot/internal/contracts"
	"iot/internal/platform"
)

func TestE2ESchemeTelemetryCommandAck(t *testing.T) {
	if os.Getenv("IOT_E2E") == "" {
		t.Skip("set IOT_E2E=1 to run the end-to-end local stack test")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics := platform.NewMetrics()

	postgresDSN := envOr("POSTGRES_DSN", "postgres://iot:iot123@localhost:5432/iot?sslmode=disable")
	kafkaBrokers := splitCSV(envOr("KAFKA_BROKERS", "localhost:9092"))
	emqxURL := envOr("EMQX_URL", "tcp://127.0.0.1:1883")
	tdengineDSN := envOr("TDENGINE_DSN", "root:taosdata@http(127.0.0.1:6041)/iot")

	store, err := platform.NewPostgresStore(postgresDSN, 5*time.Minute)
	if err != nil {
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	defer store.Close()

	publisher := platform.NewKafkaPublisher(platform.KafkaPublisherConfig{
		Brokers:        kafkaBrokers,
		TelemetryTopic: "iot.telemetry",
		CommandTopic:   "iot.command",
	}, metrics)
	if publisher == nil {
		t.Fatal("NewKafkaPublisher() returned nil")
	}
	defer publisher.Close()

	tdWriter, err := platform.NewTDengineWriter(platform.TDengineConfig{
		DSN:   tdengineDSN,
		Table: "telemetry",
	}, metrics)
	if err != nil {
		t.Fatalf("NewTDengineWriter() error = %v", err)
	}
	if tdWriter == nil {
		t.Fatal("NewTDengineWriter() returned nil")
	}
	defer tdWriter.Close()

	app := platform.New(platform.Config{
		ServiceName: "admin",
		Store:       store,
		Publisher:   publisher,
		Metrics:     metrics,
	})
	ts := httptest.NewServer(app.Router())
	defer ts.Close()

	tenantID := fmt.Sprintf("tenant-%d", time.Now().UnixNano())
	deviceID := fmt.Sprintf("device-%d", time.Now().UnixNano())
	clientIDPrefix := fmt.Sprintf("iot-e2e-%d", time.Now().UnixNano())
	ackTopicFilter := fmt.Sprintf("tenant/%s/device/+/ack", tenantID)

	bridge := platform.NewMQTTBridge(platform.MQTTBridgeConfig{
		BrokerURL:   emqxURL,
		ClientID:    clientIDPrefix + "-bridge",
		TopicFilter: contracts.TelemetryTopicFilter,
	}, publisher, metrics)
	if bridge == nil {
		t.Fatal("NewMQTTBridge() returned nil")
	}
	worker := platform.NewWorker(platform.WorkerConfig{
		KafkaBrokers:     kafkaBrokers,
		KafkaGroupID:     fmt.Sprintf("iot-e2e-%d", time.Now().UnixNano()),
		KafkaStartOffset: kafka.LastOffset,
		TelemetryTopic:   "iot.telemetry",
		CommandTopic:     "iot.command",
		AckTopicFilter:   ackTopicFilter,
		TenantIDs:        []string{tenantID},
		MQTTBrokerURL:    emqxURL,
		MQTTClientID:     clientIDPrefix + "-worker",
	}, store, tdWriter, metrics)
	if worker == nil {
		t.Fatal("NewWorker() returned nil")
	}

	runErr := make(chan error, 2)
	go func() {
		if err := bridge.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			runErr <- fmt.Errorf("bridge: %w", err)
		}
	}()
	go func() {
		if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			runErr <- fmt.Errorf("worker: %w", err)
		}
	}()
	defer func() {
		cancel()
		select {
		case err := <-runErr:
			if err != nil {
				t.Fatalf("background service error: %v", err)
			}
		default:
		}
	}()

	waitFor(t, 10*time.Second, func() bool {
		return mqttReachable(emqxURL)
	})

	e2eCreateTenant(t, ts.URL, tenantID, "Tenant E2E")
	e2eCreateDevice(t, ts.URL, tenantID, deviceID, "product-x")

	telemetryClient := mustMQTTClient(t, emqxURL, clientIDPrefix+"-telemetry")
	defer telemetryClient.Disconnect(250)
	telemetryTopic := contractsMustTopic(t, tenantID, deviceID, contracts.TopicSuffixTelemetry)
	telemetryPayload := map[string]any{
		"msgId":    fmt.Sprintf("msg-%d", time.Now().UnixNano()),
		"tenantId": tenantID,
		"deviceId": deviceID,
		"ts":       time.Now().UnixMilli(),
		"type":     "telemetry",
		"version":  "v1",
		"payload": map[string]any{
			"temp": 21.5,
		},
	}
	publishMQTTJSON(t, telemetryClient, telemetryTopic, telemetryPayload)

	waitFor(t, 60*time.Second, func() bool {
		var status platform.DeviceStatus
		if err := e2eGetJSON(ts.URL+"/api/v1/devices/"+tenantID+"/"+deviceID+"/status", &status); err != nil {
			return false
		}
		return status.Online && !status.LastSeenAt.IsZero()
	})

	waitFor(t, 60*time.Second, func() bool {
		var records []platform.TelemetryRecord
		if err := e2eGetJSON(ts.URL+"/api/v1/devices/"+tenantID+"/"+deviceID+"/telemetry", &records); err != nil {
			return false
		}
		return len(records) >= 1
	})

	waitFor(t, 60*time.Second, func() bool {
		got, err := tdengineTelemetryCount(tdengineDSN, tenantID, deviceID)
		return err == nil && got >= 1
	})

	commandClient := mustMQTTClient(t, emqxURL, clientIDPrefix+"-device")
	defer commandClient.Disconnect(250)
	commandTopic := contractsMustTopic(t, tenantID, deviceID, contracts.TopicSuffixCommand)
	commandCh := make(chan commandEnvelope, 1)
	commandToken := commandClient.Subscribe(commandTopic, 1, func(_ mqtt.Client, msg mqtt.Message) {
		var env commandEnvelope
		_ = json.Unmarshal(msg.Payload(), &env)
		select {
		case commandCh <- env:
		default:
		}
	})
	commandToken.Wait()
	if err := commandToken.Error(); err != nil {
		t.Fatalf("subscribe command topic error = %v", err)
	}

	var created platform.Command
	e2ePostJSON(t, ts.URL+"/api/v1/commands", map[string]any{
		"tenantId": tenantID,
		"deviceId": deviceID,
		"payload": map[string]any{
			"switch": "on",
		},
	}, &created)

	gotCommand := waitForCommandEnvelope(t, commandCh)
	if gotCommand.ID == "" {
		t.Fatalf("command downlink missing id: %+v", gotCommand)
	}
	if gotCommand.ID != created.ID {
		t.Fatalf("command downlink id = %q, want %q", gotCommand.ID, created.ID)
	}
	if gotCommand.TenantID != tenantID || gotCommand.DeviceID != deviceID {
		t.Fatalf("command downlink context mismatch: %+v", gotCommand)
	}

	ackClient := mustMQTTClient(t, emqxURL, clientIDPrefix+"-ack")
	defer ackClient.Disconnect(250)
	ackTopic := contractsMustTopic(t, tenantID, deviceID, contracts.TopicSuffixAck)
	publishMQTTJSON(t, ackClient, ackTopic, map[string]any{
		"commandId": gotCommand.ID,
		"tenantId":  tenantID,
		"deviceId":  deviceID,
		"status":    "acked",
	})

	waitFor(t, 60*time.Second, func() bool {
		var got platform.Command
		if err := e2eGetJSON(ts.URL+"/api/v1/commands/"+created.ID, &got); err != nil {
			return false
		}
		return got.Status == platform.CommandStatusAcked
	})
}

type commandEnvelope struct {
	ID        string          `json:"id"`
	TenantID  string          `json:"tenantId"`
	DeviceID  string          `json:"deviceId"`
	Status    string          `json:"status"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"createdAt"`
}

func waitForCommandEnvelope(t *testing.T, ch <-chan commandEnvelope) commandEnvelope {
	t.Helper()
	select {
	case env := <-ch:
		return env
	case <-time.After(60 * time.Second):
		t.Fatal("timed out waiting for command envelope")
	}
	return commandEnvelope{}
}

func tdengineTelemetryCount(dsn, tenantID, deviceID string) (int, error) {
	db, err := sql.Open("taosRestful", dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var count int
	query := fmt.Sprintf("select count(*) from telemetry where tenant_id='%s' and device_id='%s';", tenantID, deviceID)
	if err := db.QueryRow(query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func mqttReachable(url string) bool {
	opts := mqtt.NewClientOptions().AddBroker(url)
	opts.SetClientID(fmt.Sprintf("probe-%d", time.Now().UnixNano()))
	opts.SetConnectTimeout(3 * time.Second)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return false
	}
	client.Disconnect(250)
	return true
}

func mustMQTTClient(t *testing.T, brokerURL, clientID string) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions().AddBroker(brokerURL)
	opts.SetClientID(clientID)
	opts.SetConnectTimeout(5 * time.Second)
	opts.SetAutoReconnect(false)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		t.Fatalf("MQTT connect error = %v", err)
	}
	return client
}

func publishMQTTJSON(t *testing.T, client mqtt.Client, topic string, body map[string]any) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	token := client.Publish(topic, 1, false, payload)
	token.Wait()
	if err := token.Error(); err != nil {
		t.Fatalf("publish error = %v", err)
	}
}

func contractsMustTopic(t *testing.T, tenantID, deviceID, suffix string) string {
	t.Helper()
	var (
		topic string
		err   error
	)
	switch suffix {
	case contracts.TopicSuffixTelemetry:
		topic, err = contracts.BuildTelemetryTopic(tenantID, deviceID)
	case contracts.TopicSuffixCommand:
		topic, err = contracts.BuildCommandTopic(tenantID, deviceID)
	case contracts.TopicSuffixAck:
		topic, err = contracts.BuildAckTopic(tenantID, deviceID)
	default:
		t.Fatalf("unsupported canonical topic suffix %q", suffix)
	}
	if err != nil {
		t.Fatalf("%s topic error = %v", suffix, err)
	}
	return topic
}

func e2eCreateTenant(t *testing.T, baseURL, id, name string) {
	t.Helper()
	e2ePostJSON(t, baseURL+"/api/v1/tenants", map[string]any{
		"id":   id,
		"name": name,
	})
}

func e2eCreateDevice(t *testing.T, baseURL, tenantID, deviceID, productID string) {
	t.Helper()
	e2ePostJSON(t, baseURL+"/api/v1/devices", map[string]any{
		"tenantId":  tenantID,
		"deviceId":  deviceID,
		"productId": productID,
		"secret":    "secret-1",
	})
}

func e2ePostJSON(t *testing.T, url string, body map[string]any, out ...any) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	resp, err := http.Post(url, "application/json", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatalf("http.Post() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("status code = %d", resp.StatusCode)
	}
	if len(out) > 0 {
		if err := json.NewDecoder(resp.Body).Decode(out[0]); err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
	}
}

func e2eGetJSON(url string, out any) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status code = %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timeout after %s", timeout)
}

func envOr(key, fallback string) string {
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
