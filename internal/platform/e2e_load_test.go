package platform_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"

	"iot/internal/contracts"
	"iot/internal/platform"
)

func TestE2ELoadMultiTenantMultiDevice(t *testing.T) {
	if os.Getenv("IOT_E2E_LOAD") == "" {
		t.Skip("set IOT_E2E_LOAD=1 to run the local multi-tenant load test")
	}

	const (
		tenantCount        = 5
		devicesPerTenant   = 10
		telemetryPerDevice = 6
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics := platform.NewMetrics()

	postgresDSN := envOr("POSTGRES_DSN", "postgres://iot:iot123@localhost:5432/iot?sslmode=disable")
	kafkaBrokers := splitCSV(envOr("KAFKA_BROKERS", "localhost:9092"))
	emqxURL := envOr("EMQX_URL", "tcp://127.0.0.1:1883")
	tdengineDSN := envOr("TDENGINE_DSN", "root:taosdata@http(127.0.0.1:6041)/iot")
	runPrefix := fmt.Sprintf("load-%d", time.Now().UnixNano())

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

	bridge := platform.NewMQTTBridge(platform.MQTTBridgeConfig{
		BrokerURL:   emqxURL,
		ClientID:    "iot-load-bridge",
		TopicFilter: contracts.TelemetryTopicFilter,
	}, publisher, metrics)
	worker := platform.NewWorker(platform.WorkerConfig{
		KafkaBrokers:     kafkaBrokers,
		KafkaGroupID:     fmt.Sprintf("iot-load-%d", time.Now().UnixNano()),
		KafkaStartOffset: kafka.LastOffset,
		TelemetryTopic:   "iot.telemetry",
		CommandTopic:     "iot.command",
		MQTTBrokerURL:    emqxURL,
		MQTTClientID:     "iot-load-worker",
	}, store, tdWriter, metrics)

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

	telemetryClient := mustMQTTClient(t, emqxURL, "iot-load-telemetry")
	defer telemetryClient.Disconnect(250)
	agents := make([]func(), 0, tenantCount)

	tenantIDs := make([]string, 0, tenantCount)
	deviceIDs := make([]string, 0, tenantCount*devicesPerTenant)
	for ti := 0; ti < tenantCount; ti++ {
		tenantID := fmt.Sprintf("%s-tenant-%d", runPrefix, ti)
		tenantIDs = append(tenantIDs, tenantID)
		e2eCreateTenant(t, ts.URL, tenantID, "Load Tenant "+strconv.Itoa(ti))
		for di := 0; di < devicesPerTenant; di++ {
			deviceID := fmt.Sprintf("%s-device-%d-%d", runPrefix, ti, di)
			deviceIDs = append(deviceIDs, deviceID)
			e2eCreateDevice(t, ts.URL, tenantID, deviceID, "product-load")
		}
	}

	for _, tenantID := range tenantIDs {
		tenantID := tenantID
		client := mustMQTTClient(t, emqxURL, "iot-load-agent-"+tenantID)
		commandTopic, err := contracts.BuildTenantCommandTopicFilter(tenantID)
		if err != nil {
			t.Fatalf("BuildTenantCommandTopicFilter() error = %v", err)
		}
		ackCh := make(chan commandEnvelope, 16)
		sub := client.Subscribe(commandTopic, 1, func(c mqtt.Client, msg mqtt.Message) {
			var env commandEnvelope
			if err := json.Unmarshal(msg.Payload(), &env); err != nil {
				return
			}
			ackTopic, err := contracts.BuildAckTopic(env.TenantID, env.DeviceID)
			if err != nil {
				return
			}
			go func() {
				ackPayload := map[string]any{
					"commandId": env.ID,
					"tenantId":  env.TenantID,
					"deviceId":  env.DeviceID,
					"status":    "acked",
				}
				if err := publishMQTTJSONNoT(c, ackTopic, ackPayload); err != nil {
					return
				}
				select {
				case ackCh <- env:
				default:
				}
			}()
		})
		sub.Wait()
		if err := sub.Error(); err != nil {
			t.Fatalf("subscribe command topic error = %v", err)
		}
		agents = append(agents, func() { client.Disconnect(250) })
	}
	defer func() {
		for _, closeFn := range agents {
			closeFn()
		}
	}()

	start := time.Now()
	baseTelemetryTs := time.Now().UnixMilli()
	telemetryIndex := int64(0)

	telemetrySent := 0
	for ti, tenantID := range tenantIDs {
		for di := 0; di < devicesPerTenant; di++ {
			deviceID := deviceIDs[ti*devicesPerTenant+di]
			for seq := 0; seq < telemetryPerDevice; seq++ {
				currentTs := baseTelemetryTs + telemetryIndex
				telemetryIndex++
				payload := map[string]any{
					"msgId":    fmt.Sprintf("loadmsg-%d-%d-%d-%d", ti, di, seq, time.Now().UnixNano()),
					"tenantId": tenantID,
					"deviceId": deviceID,
					"ts":       currentTs,
					"type":     "telemetry",
					"version":  "v1",
					"payload": map[string]any{
						"seq":  seq,
						"temp": 20 + seq,
					},
				}
				telemetryTopic := contractsMustTopic(t, tenantID, deviceID, contracts.TopicSuffixTelemetry)
				publishMQTTJSON(t, telemetryClient, telemetryTopic, payload)
				telemetrySent++
			}
		}
	}

	expectedTelemetry := tenantCount * devicesPerTenant * telemetryPerDevice
	waitFor(t, 45*time.Second, func() bool {
		gotPG, err := pgTelemetryCount(postgresDSN, runPrefix+"%")
		if err != nil || gotPG < expectedTelemetry {
			return false
		}
		for ti, tenantID := range tenantIDs {
			deviceID := deviceIDs[ti*devicesPerTenant]
			gotTD, err := tdengineTelemetryCountExact(tdengineDSN, tenantID, deviceID)
			if err != nil || gotTD < telemetryPerDevice {
				return false
			}
		}
		return true
	})

	for ti, tenantID := range tenantIDs {
		for di := 0; di < devicesPerTenant; di++ {
			deviceID := deviceIDs[ti*devicesPerTenant+di]
			var cmd platform.Command
			e2ePostJSON(t, ts.URL+"/api/v1/commands", map[string]any{
				"tenantId": tenantID,
				"deviceId": deviceID,
				"payload": map[string]any{
					"switch": "toggle",
					"idx":    di,
				},
			}, &cmd)
		}
	}

	expectedCommands := tenantCount * devicesPerTenant
	waitFor(t, 45*time.Second, func() bool {
		gotAcked, err := pgAckedCommandCount(postgresDSN, runPrefix+"%")
		if err != nil || gotAcked < expectedCommands {
			return false
		}
		gotOnline, err := pgOnlineDeviceCount(postgresDSN, runPrefix+"%")
		if err != nil || gotOnline < expectedCommands {
			return false
		}
		return true
	})

	waitFor(t, 20*time.Second, func() bool {
		_, err := e2eGetJSONBody(ts.URL + "/api/v1/devices/" + tenantIDs[0] + "/" + deviceIDs[0] + "/status")
		return err == nil
	})

	t.Logf("load test completed in %s: telemetry=%d commands=%d", time.Since(start).Round(time.Millisecond), telemetrySent, expectedCommands)
}

func pgTelemetryCount(dsn, tenantPrefix string) (int, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM telemetry_records WHERE tenant_id LIKE $1`, tenantPrefix).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func pgAckedCommandCount(dsn, tenantPrefix string) (int, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM commands WHERE tenant_id LIKE $1 AND status = 'acked'`, tenantPrefix).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func pgOnlineDeviceCount(dsn, tenantPrefix string) (int, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM device_state WHERE tenant_id LIKE $1 AND connected = true`, tenantPrefix).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func tdengineTelemetryCountExact(dsn, tenantID, deviceID string) (int, error) {
	db, err := sql.Open("taosRestful", dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var count int
	query := fmt.Sprintf(
		"select count(*) from telemetry where tenant_id='%s' and device_id='%s';",
		strings.ReplaceAll(tenantID, "'", "''"),
		strings.ReplaceAll(deviceID, "'", "''"),
	)
	if err := db.QueryRow(query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func e2eGetJSONBody(url string) (map[string]any, error) {
	var out map[string]any
	if err := e2eGetJSON(url, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func publishMQTTJSONNoT(client mqtt.Client, topic string, body map[string]any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	token := client.Publish(topic, 1, false, payload)
	token.Wait()
	return token.Error()
}
