// Command e2espoof tests P0: publish a telemetry whose envelope deviceId does
// NOT match the MQTT topic device. The worker must reject it (DLQ), not ingest.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	broker := "tcp://localhost:11883"
	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID(fmt.Sprintf("iot-e2e-spoof-%d", time.Now().UnixNano()))
	opts.SetUsername("e2e-tenant:e2e-device-1")
	opts.SetPassword("e2e-secret-123")
	opts.SetConnectTimeout(10 * time.Second)

	c := mqtt.NewClient(opts)
	tok := c.Connect()
	tok.Wait()
	if err := tok.Error(); err != nil {
		fmt.Printf("CONNECT-FAIL: %v\n", err)
		os.Exit(1)
	}
	defer c.Disconnect(500)

	// Topic says device e2e-device-1, but envelope claims a DIFFERENT device.
	envelope := map[string]any{
		"msgId":    fmt.Sprintf("spoof-%d", time.Now().UnixNano()),
		"tenantId": "e2e-tenant",
		"deviceId": "e2e-device-999", // mismatch!
		"ts":       time.Now().UnixMilli(),
		"type":     "temperature",
		"version":  "1.0",
		"payload":  map[string]any{"value": 99},
	}
	body, _ := json.Marshal(envelope)
	// Publish on the device's own topic but with forged envelope identity.
	ptok := c.Publish("tenant/e2e-tenant/device/e2e-device-1/telemetry", 1, false, body)
	ptok.Wait()
	if err := ptok.Error(); err != nil {
		fmt.Printf("PUBLISH-FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("SPOOF-SENT: msgId=%s (envelope deviceId=e2e-device-999 on topic device=e2e-device-1)\n", envelope["msgId"])
}
