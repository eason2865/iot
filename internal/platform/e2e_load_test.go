package platform_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/jackc/pgx/v5/stdlib"

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

	baseURL := e2eManagementURL()
	postgresDSN := envOr("POSTGRES_DSN", "postgres://iot:iot123@localhost:5432/iot?sslmode=disable")
	emqxURL := envOr("EMQX_URL", "tcp://127.0.0.1:1883")
	tdengineDSN := envOr("TDENGINE_DSN", "root:taosdata@http(127.0.0.1:6041)/iot")
	runPrefix := fmt.Sprintf("load-%d", time.Now().UnixNano())
	clientIDPrefix := "iot-" + runPrefix
	tenantIDs := make([]string, 0, tenantCount)
	for ti := 0; ti < tenantCount; ti++ {
		tenantID := fmt.Sprintf("%s-tenant-%d", runPrefix, ti)
		tenantIDs = append(tenantIDs, tenantID)
	}

	devices := make(map[string]mqtt.Client, tenantCount*devicesPerTenant)
	for ti, tenant := range tenantIDs {
		e2eCreateTenant(t, baseURL, tenant, "Load Tenant "+strconv.Itoa(ti))
		for di := 0; di < devicesPerTenant; di++ {
			id := fmt.Sprintf("%s-device-%d-%d", runPrefix, ti, di)
			e2eCreateDevice(t, baseURL, tenant, id, "product-load")
			client := mustMQTTDeviceClient(t, emqxURL, clientIDPrefix+"-"+id, tenant, id)
			defer client.Disconnect(250)
			devices[id] = client
			ackTopic := contractsMustTopic(t, tenant, id, contracts.TopicSuffixAck)
			token := client.Subscribe(contractsMustTopic(t, tenant, id, contracts.TopicSuffixCommand), 1, func(c mqtt.Client, msg mqtt.Message) {
				var env commandEnvelope
				if err := json.Unmarshal(msg.Payload(), &env); err != nil {
					return
				}
				go publishMQTTJSONNoT(c, ackTopic, map[string]any{"commandId": env.ID, "tenantId": tenant, "deviceId": id, "status": "acked"})
			})
			token.Wait()
			if err := token.Error(); err != nil {
				t.Fatal(err)
			}
		}
	}

	start := time.Now()
	baseTelemetryTs := time.Now().UnixMilli()
	telemetryIndex := int64(0)

	telemetrySent := 0
	for ti, tenantID := range tenantIDs {
		for di := 0; di < devicesPerTenant; di++ {
			deviceID := fmt.Sprintf("%s-device-%d-%d", runPrefix, ti, di)
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
				publishMQTTJSON(t, devices[deviceID], telemetryTopic, payload)
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
			deviceID := fmt.Sprintf("%s-device-%d-0", runPrefix, ti)
			gotTD, err := tdengineTelemetryCount(tdengineDSN, tenantID, deviceID)
			if err != nil || gotTD < telemetryPerDevice {
				return false
			}
		}
		return true
	})

	for ti, tenantID := range tenantIDs {
		for di := 0; di < devicesPerTenant; di++ {
			deviceID := fmt.Sprintf("%s-device-%d-%d", runPrefix, ti, di)
			var cmd platform.Command
			e2ePostJSON(t, baseURL+"/api/v1/tenants/"+tenantID+"/devices/"+deviceID+"/commands", map[string]any{
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
		_, err := e2eGetJSONBody(baseURL + "/api/v1/tenants/" + tenantIDs[0] + "/devices/" + fmt.Sprintf("%s-device-0-0", runPrefix) + "/status")
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
