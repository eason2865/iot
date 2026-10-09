package platform

import (
	"context"
	"errors"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"

	"iot/internal/contracts"
)

// errPublishBacklogFull marks a message that could not get a publish slot
// before the timeout; it is dead-lettered with this cause so the DLQ record
// explains the drop.
var errPublishBacklogFull = errors.New("mqtt bridge publish backlog full")

type MQTTBridgeConfig struct {
	BrokerURL          string
	ClientID           string
	Username           string
	Password           string
	TopicFilter        string
	PublishConcurrency int
	PublishSlotTimeout time.Duration
	KafkaBrokers       []string
	DLQTopic           string
	TopicConfig        KafkaTopicConfig
	DLQTopicConfig     KafkaTopicConfig
}

type MQTTBridge struct {
	client       mqtt.Client
	filter       string
	metrics      *Metrics
	publishSlots chan struct{}
	dlqWriter    *kafka.Writer
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
	dlqTopic := cfg.DLQTopic
	if dlqTopic == "" {
		dlqTopic = "iot.dlq"
	}
	if len(cfg.KafkaBrokers) > 0 {
		ensureKafkaTopicsBestEffort(cfg.KafkaBrokers, cfg.DLQTopicConfig, dlqTopic)
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
	if len(cfg.KafkaBrokers) > 0 {
		bridge.dlqWriter = &kafka.Writer{Addr: kafka.TCP(cfg.KafkaBrokers...), Topic: dlqTopic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchSize: 1, AllowAutoTopicCreation: true}
	}
	opts.OnConnect = func(_ mqtt.Client) {
		log.Printf("mqtt bridge connected: filter=%s", filter)
		token := bridge.client.Subscribe(filter, 1, func(_ mqtt.Client, msg mqtt.Message) {
			env, err := contracts.ParseEnvelope(msg.Payload())
			if err != nil {
				log.Printf("mqtt bridge parse error: topic=%s err=%v", msg.Topic(), err)
				if bridge.metrics != nil {
					bridge.metrics.IncMQTTBridge("error")
				}
				_ = publishDeadLetter(context.Background(), bridge.dlqWriter, kafka.Message{Topic: msg.Topic(), Value: msg.Payload()}, StageMQTTDecode, err, bridge.metrics)
				return
			}
			// Reject identity spoofing: the envelope tenant/device must match the
			// actual MQTT topic the message arrived on. ACL limits which topic a
			// device can publish to, but not what identity it claims in the body.
			topicTenant, topicDevice, _, ok := contracts.ParseDeviceTopic(msg.Topic())
			if !ok || topicTenant != env.TenantID || topicDevice != env.DeviceID {
				log.Printf("mqtt bridge identity mismatch: topic=%s envelope tenant=%s device=%s", msg.Topic(), env.TenantID, env.DeviceID)
				if bridge.metrics != nil {
					bridge.metrics.IncMQTTBridge("error")
				}
				_ = publishDeadLetter(context.Background(), bridge.dlqWriter, kafka.Message{Topic: msg.Topic(), Value: msg.Payload()}, StageMQTTIdentity, errIdentityMismatch, bridge.metrics)
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
					if err := publisher.PublishTelemetry(context.Background(), rec); err != nil {
						log.Printf("mqtt bridge publish telemetry error: tenant=%s device=%s msg=%s err=%v", rec.TenantID, rec.DeviceID, rec.MsgID, err)
						if bridge.metrics != nil {
							bridge.metrics.IncMQTTBridge("error")
						}
						if dlqErr := publishDeadLetter(context.Background(), bridge.dlqWriter, kafka.Message{Topic: msg.Topic(), Key: []byte(rec.DeviceID), Value: msg.Payload()}, StageMQTTKafka, err, bridge.metrics); dlqErr != nil {
							// The message is now lost for good: the publish failed
							// and the dead-letter copy failed too. This must be
							// visible in logs, not silently dropped.
							log.Printf("mqtt bridge dead-letter error: tenant=%s device=%s msg=%s err=%v", rec.TenantID, rec.DeviceID, rec.MsgID, dlqErr)
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
				// Backpressure must not silently drop the message: dead-letter it
				// on the same stage/path as a failed publish so it can be replayed.
				if dlqErr := publishDeadLetter(context.Background(), bridge.dlqWriter, kafka.Message{Topic: msg.Topic(), Key: []byte(rec.DeviceID), Value: msg.Payload()}, StageMQTTKafka, errPublishBacklogFull, bridge.metrics); dlqErr != nil {
					log.Printf("mqtt bridge dead-letter error: tenant=%s device=%s msg=%s err=%v", rec.TenantID, rec.DeviceID, rec.MsgID, dlqErr)
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
	if b.dlqWriter != nil {
		_ = b.dlqWriter.Close()
	}
	return nil
}
