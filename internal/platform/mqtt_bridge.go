package platform

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
	"iot/internal/contracts"
)

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
	// Production injects PostgreSQL as a durable inbox. Kafka failures are then
	// recoverable by the worker even if the ingestor is replaced during publishing.
	Inbox TelemetryInbox
}

type MQTTBridge struct {
	client       mqtt.Client
	filter       string
	metrics      *Metrics
	publishSlots chan struct{}
	dlqWriter    *kafka.Writer
	publisher    MessagePublisher
	inbox        TelemetryInbox
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	stopping     bool
	pending      sync.WaitGroup
	errCh        chan error
}

func NewMQTTBridge(cfg MQTTBridgeConfig, publisher MessagePublisher, metrics *Metrics) *MQTTBridge {
	if cfg.BrokerURL == "" || publisher == nil {
		return nil
	}
	filter := cfg.TopicFilter
	if filter == "" {
		filter = contracts.TelemetryTopicFilter
	}
	concurrency := cfg.PublishConcurrency
	if concurrency <= 0 {
		concurrency = 64
	}
	slotTimeout := cfg.PublishSlotTimeout
	if slotTimeout <= 0 {
		slotTimeout = 30 * time.Second
	}
	topic := cfg.DLQTopic
	if topic == "" {
		topic = "iot.dlq"
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &MQTTBridge{filter: filter, metrics: metrics, publishSlots: make(chan struct{}, concurrency), publisher: publisher, inbox: cfg.Inbox, ctx: ctx, cancel: cancel, errCh: make(chan error, 1)}
	if len(cfg.KafkaBrokers) > 0 {
		ensureKafkaTopicsBestEffort(cfg.KafkaBrokers, deadLetterTopicConfig(cfg.DLQTopicConfig), topic)
		b.dlqWriter = newDeadLetterWriter(cfg.KafkaBrokers, topic)
	}
	opts := mqtt.NewClientOptions().AddBroker(cfg.BrokerURL).SetClientID(cfg.ClientID)
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
	}
	if cfg.Password != "" {
		opts.SetPassword(cfg.Password)
	}
	opts.SetAutoReconnect(true).SetConnectRetry(true).SetConnectRetryInterval(2 * time.Second)
	// A callback only schedules work. Its return must never acknowledge a message
	// still held solely in process memory.
	opts.SetAutoAckDisabled(true)
	opts.OnConnect = func(client mqtt.Client) {
		log.Printf("mqtt bridge connected: filter=%s", filter)
		token := client.Subscribe(filter, 1, func(_ mqtt.Client, msg mqtt.Message) {
			b.mu.Lock()
			if b.stopping {
				b.mu.Unlock()
				return
			}
			b.pending.Add(1)
			b.mu.Unlock()
			if !acquirePublishSlotContext(b.ctx, b.publishSlots, slotTimeout) {
				defer b.pending.Done()
				b.deadLetter(msg, StageMQTTKafka, errPublishBacklogFull, false)
				return
			}
			go func() { defer b.pending.Done(); defer func() { <-b.publishSlots }(); b.processMessage(msg) }()
		})
		if !token.WaitTimeout(5 * time.Second) {
			b.fail(errors.New("mqtt bridge subscribe timeout"))
			return
		}
		if err := token.Error(); err != nil {
			b.fail(err)
			return
		}
		sub, ok := token.(*mqtt.SubscribeToken)
		if !ok {
			b.fail(fmt.Errorf("mqtt bridge subscription missing SUBACK: %s", filter))
			return
		}
		granted, present := sub.Result()[filter]
		if !present || granted != 1 {
			b.fail(fmt.Errorf("mqtt bridge subscription requires QoS 1: filter=%s SUBACK=%#x present=%t", filter, granted, present))
		}
	}
	b.client = mqtt.NewClient(opts)
	return b
}

func (b *MQTTBridge) processMessage(msg mqtt.Message) {
	env, err := contracts.ParseEnvelope(msg.Payload())
	if err != nil {
		b.deadLetter(msg, StageMQTTDecode, err, false)
		return
	}
	tenant, device, _, ok := contracts.ParseDeviceTopic(msg.Topic())
	if !ok || tenant != env.TenantID || device != env.DeviceID {
		b.deadLetter(msg, StageMQTTIdentity, errIdentityMismatch, false)
		return
	}
	rec := TelemetryRecord{MsgID: env.MsgID, TenantID: env.TenantID, DeviceID: env.DeviceID, Ts: env.Ts, Type: env.Type, Version: env.Version, Payload: env.Payload, ReceivedAt: time.Now().UTC()}
	durable := false
	if b.inbox != nil {
		ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
		stored, storeErr := b.inbox.RecordTelemetryContext(ctx, env)
		cancel()
		if storeErr == nil || IsTelemetryDuplicate(storeErr) {
			rec = stored
			durable = true
			msg.Ack()
		} else {
			log.Printf("mqtt bridge inbox write error: msg=%s err=%v", env.MsgID, storeErr)
		}
	}
	if err := b.publisher.PublishTelemetry(b.ctx, rec); err != nil {
		b.deadLetter(msg, StageMQTTKafka, err, durable)
		return
	}
	if !durable {
		msg.Ack()
	}
	if b.metrics != nil {
		b.metrics.IncMQTTBridge("ok")
	}
}

func (b *MQTTBridge) deadLetter(msg mqtt.Message, stage string, cause error, durable bool) {
	if b.metrics != nil {
		b.metrics.IncMQTTBridge("error")
	}
	log.Printf("mqtt bridge error: topic=%s stage=%s err=%v", msg.Topic(), stage, cause)
	if err := publishDeadLetter(b.ctx, b.dlqWriter, kafka.Message{Topic: msg.Topic(), Value: msg.Payload()}, stage, cause, b.metrics); err != nil {
		// PostgreSQL inbox rows are retried by the worker's compensation scan.
		// Otherwise fail the ingestor and leave this MQTT delivery unacknowledged.
		if !durable {
			b.fail(fmt.Errorf("mqtt message not persisted: %w", err))
		}
		log.Printf("mqtt bridge dead-letter error: %v", err)
		return
	}
	if !durable {
		msg.Ack()
	}
}

func (b *MQTTBridge) fail(err error) {
	select {
	case b.errCh <- err:
	default:
	}
}

func acquirePublishSlot(slots chan struct{}, timeout time.Duration) bool {
	return acquirePublishSlotContext(context.Background(), slots, timeout)
}
func acquirePublishSlotContext(ctx context.Context, slots chan struct{}, timeout time.Duration) bool {
	if timeout <= 0 {
		select {
		case <-ctx.Done():
			return false
		case slots <- struct{}{}:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case slots <- struct{}{}:
		return true
	case <-timer.C:
		return false
	}
}

// stop closes admission before waiting so no callback can race WaitGroup.Add.
// A bounded drain precedes cancellation; persisted inbox rows survive a forced stop.
func (b *MQTTBridge) stop() {
	b.mu.Lock()
	b.stopping = true
	b.mu.Unlock()
	done := make(chan struct{})
	go func() { b.pending.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		b.cancel()
		<-done
	}
	b.cancel()
	b.client.Disconnect(250)
	if b.dlqWriter != nil {
		_ = b.dlqWriter.Close()
	}
}

func (b *MQTTBridge) Run(ctx context.Context) error {
	if b == nil || b.client == nil {
		return nil
	}
	defer b.stop()
	token := b.client.Connect()
	for !token.WaitTimeout(100 * time.Millisecond) {
		select {
		case <-ctx.Done():
			return nil
		case err := <-b.errCh:
			return err
		default:
		}
	}
	if err := token.Error(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-b.errCh:
		return err
	}
}
