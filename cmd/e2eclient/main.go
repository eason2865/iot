// Command e2eclient is a throwaway E2E harness: it connects to EMQX as a
// provisioned device, publishes a telemetry envelope, and reports whether the
// broker accepted it. Used to verify the full MQTT -> ingestor -> Kafka ->
// worker -> PG/TDengine chain after deployment.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	broker := getenv("E2E_BROKER", "tcp://localhost:11883")
	tenantID := getenv("E2E_TENANT", "e2e-tenant")
	deviceID := getenv("E2E_DEVICE", "e2e-device-1")
	secret := getenv("E2E_SECRET", "e2e-secret-123")

	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID(fmt.Sprintf("iot-e2e-%d", time.Now().UnixNano()))
	opts.SetUsername(tenantID + ":" + deviceID)
	opts.SetPassword(secret)
	opts.SetConnectTimeout(10 * time.Second)

	c := mqtt.NewClient(opts)
	tok := c.Connect()
	tok.Wait()
	if err := tok.Error(); err != nil {
		fmt.Printf("CONNECT-FAIL: %v\n", err)
		os.Exit(1)
	}
	defer c.Disconnect(500)
	fmt.Println("CONNECT-OK: device authenticated via EMQX -> iot-core callback")

	envelope := map[string]any{
		"msgId":    fmt.Sprintf("e2e-msg-%d", time.Now().UnixNano()),
		"tenantId": tenantID,
		"deviceId": deviceID,
		"ts":       time.Now().UnixMilli(),
		"type":     "temperature",
		"version":  "1.0",
		"payload":  map[string]any{"value": 42.5},
	}
	body, _ := json.Marshal(envelope)
	topic := fmt.Sprintf("tenant/%s/device/%s/telemetry", tenantID, deviceID)
	ptok := c.Publish(topic, 1, false, body)
	ptok.Wait()
	if err := ptok.Error(); err != nil {
		fmt.Printf("PUBLISH-FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PUBLISH-OK: topic=%s msgId=%s\n", topic, envelope["msgId"])
	fmt.Printf("ENVELOPE-MSGID: %s\n", envelope["msgId"])
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
