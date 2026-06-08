package platform

import (
	"context"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"iot/internal/contracts"
)

type MQTTBridgeConfig struct {
	BrokerURL          string
	ClientID           string
	Username           string
	Password           string
	TopicFilter        string
	PublishConcurrency int
	PublishSlotTimeout time.Duration
}

type MQTTBridge struct {
	client       mqtt.Client
	filter       string
	metrics      *Metrics
	publishSlots chan struct{}
}

func NewMQTTBridge(cfg MQTTBridgeConfig, publisher MessagePublisher, metrics *Metrics) *MQTTBridge {
	if cfg.BrokerURL == "" || publisher == nil {
		return nil
	}
	filter := cfg.TopicFilter
	if filter == "" {
		filter = contracts.TelemetryTopicFilter
	}
	publishConcurrency := cfg.PublishConcurrency
	if publishConcurrency <= 0 {
		publishConcurrency = 64
	}
	publishSlotTimeout := cfg.PublishSlotTimeout
	if publishSlotTimeout <= 0 {
		publishSlotTimeout = 30 * time.Second
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
	bridge := &MQTTBridge{filter: filter, metrics: metrics, publishSlots: make(chan struct{}, publishConcurrency)}
	opts.OnConnect = func(_ mqtt.Client) {
		log.Printf("mqtt bridge connected: filter=%s", filter)
		token := bridge.client.Subscribe(filter, 1, func(_ mqtt.Client, msg mqtt.Message) {
			env, err := contracts.ParseEnvelope(msg.Payload())
			if err != nil {
				log.Printf("mqtt bridge parse error: topic=%s err=%v", msg.Topic(), err)
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
			if acquirePublishSlot(bridge.publishSlots, publishSlotTimeout) {
				go func() {
					defer func() { <-bridge.publishSlots }()
					if err := publisher.PublishTelemetry(rec); err != nil {
						log.Printf("mqtt bridge publish telemetry error: tenant=%s device=%s msg=%s err=%v", rec.TenantID, rec.DeviceID, rec.MsgID, err)
						if bridge.metrics != nil {
							bridge.metrics.IncMQTTBridge("error")
						}
						return
					}
					if bridge.metrics != nil {
						bridge.metrics.IncMQTTBridge("ok")
					}
				}()
			} else {
				log.Printf("mqtt bridge publish backlog full: tenant=%s device=%s msg=%s", rec.TenantID, rec.DeviceID, rec.MsgID)
				if bridge.metrics != nil {
					bridge.metrics.IncMQTTBridge("error")
				}
				return
			}
		})
		token.Wait()
		if err := token.Error(); err != nil {
			log.Printf("mqtt bridge subscribe error: filter=%s err=%v", filter, err)
			if bridge.metrics != nil {
				bridge.metrics.IncMQTTBridge("error")
			}
		}
	}
	bridge.client = mqtt.NewClient(opts)
	return bridge
}

func acquirePublishSlot(slots chan struct{}, timeout time.Duration) bool {
	if timeout <= 0 {
		select {
		case slots <- struct{}{}:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case slots <- struct{}{}:
		return true
	case <-timer.C:
		return false
	}
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
