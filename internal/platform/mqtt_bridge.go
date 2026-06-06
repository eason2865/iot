package platform

import (
	"context"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"iot/internal/contracts"
)

type MQTTBridgeConfig struct {
	BrokerURL   string
	ClientID    string
	Username    string
	Password    string
	TopicFilter string
}

type MQTTBridge struct {
	client  mqtt.Client
	filter  string
	metrics *Metrics
}

func NewMQTTBridge(cfg MQTTBridgeConfig, publisher Publisher, metrics *Metrics) *MQTTBridge {
	if cfg.BrokerURL == "" || publisher == nil {
		return nil
	}
	filter := cfg.TopicFilter
	if filter == "" {
		filter = "tenant/+/device/+/telemetry"
	}
	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.BrokerURL)
	opts.SetClientID(cfg.ClientID)
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
	}
	if cfg.Password != "" {
		opts.SetPassword(cfg.Password)
	}
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(2 * time.Second)
	bridge := &MQTTBridge{filter: filter, metrics: metrics}
	opts.OnConnect = func(_ mqtt.Client) {
		token := bridge.client.Subscribe(filter, 0, func(_ mqtt.Client, msg mqtt.Message) {
			env, err := contracts.ParseEnvelope(msg.Payload())
			if err != nil {
				if bridge.metrics != nil {
					bridge.metrics.IncMQTTBridge("error")
				}
				return
			}
			rec := TelemetryRecord{
				MsgID:      env.MsgID,
				TenantID:   env.TenantID,
				DeviceID:   env.DeviceID,
				Ts:         env.Ts,
				Type:       env.Type,
				Version:    env.Version,
				Payload:    env.Payload,
				ReceivedAt: time.Now().UTC(),
			}
			if err := publisher.publishTelemetry(rec); err != nil {
				if bridge.metrics != nil {
					bridge.metrics.IncMQTTBridge("error")
				}
				return
			}
			if bridge.metrics != nil {
				bridge.metrics.IncMQTTBridge("ok")
			}
		})
		token.Wait()
	}
	bridge.client = mqtt.NewClient(opts)
	return bridge
}

func (b *MQTTBridge) Run(ctx context.Context) error {
	if b == nil || b.client == nil {
		return nil
	}
	token := b.client.Connect()
	token.Wait()
	if err := token.Error(); err != nil {
		return err
	}
	<-ctx.Done()
	b.client.Disconnect(250)
	return nil
}
