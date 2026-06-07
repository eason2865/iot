package platform

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"

	"iot/internal/contracts"
)

type WorkerConfig struct {
	KafkaBrokers     []string
	KafkaGroupID     string
	KafkaStartOffset int64
	TelemetryTopic   string
	CommandTopic     string
	AckTopicFilter   string
	AckTopicFilters  []string
	TenantIDs        []string
	MQTTBrokerURL    string
	MQTTClientID     string
	MQTTUsername     string
	MQTTPassword     string
}

type Worker struct {
	store           Store
	tdengine        *TDengineWriter
	mqtt            mqtt.Client
	telemetryReader *kafka.Reader
	commandReader   *kafka.Reader
	metrics         *Metrics
	tenantAllowlist map[string]struct{}
}

func NewWorker(cfg WorkerConfig, store Store, tdengine *TDengineWriter, metrics *Metrics) *Worker {
	w := &Worker{
		store:           store,
		tdengine:        tdengine,
		metrics:         metrics,
		tenantAllowlist: tenantAllowlist(cfg.TenantIDs),
	}
	if len(cfg.KafkaBrokers) > 0 {
		telemetryTopic := cfg.TelemetryTopic
		if telemetryTopic == "" {
			telemetryTopic = "iot.telemetry"
		}
		commandTopic := cfg.CommandTopic
		if commandTopic == "" {
			commandTopic = "iot.command"
		}
		ensureKafkaTopicsBestEffort(cfg.KafkaBrokers, telemetryTopic, commandTopic)
		groupID := cfg.KafkaGroupID
		if groupID == "" {
			groupID = "iot-worker"
		}
		startOffset := cfg.KafkaStartOffset
		if startOffset == 0 {
			startOffset = kafka.FirstOffset
		}
		w.telemetryReader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:     cfg.KafkaBrokers,
			Topic:       telemetryTopic,
			GroupID:     groupID + "-telemetry",
			StartOffset: startOffset,
			MinBytes:    1,
			MaxBytes:    10e6,
		})
		w.commandReader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:     cfg.KafkaBrokers,
			Topic:       commandTopic,
			GroupID:     groupID + "-command",
			StartOffset: startOffset,
			MinBytes:    1,
			MaxBytes:    10e6,
		})
	}
	if cfg.MQTTBrokerURL != "" {
		opts := mqtt.NewClientOptions().AddBroker(cfg.MQTTBrokerURL)
		if cfg.MQTTClientID != "" {
			opts.SetClientID(cfg.MQTTClientID)
		} else {
			opts.SetClientID("iot-worker")
		}
		if cfg.MQTTUsername != "" {
			opts.SetUsername(cfg.MQTTUsername)
		}
		if cfg.MQTTPassword != "" {
			opts.SetPassword(cfg.MQTTPassword)
		}
		opts.SetAutoReconnect(true)
		ackTopics := normalizeAckTopicFilters(cfg)
		opts.OnConnect = func(client mqtt.Client) {
			for _, ackTopic := range ackTopics {
				token := client.Subscribe(ackTopic, 1, w.handleAckMessage)
				token.Wait()
				if err := token.Error(); err != nil {
					log.Printf("command ack subscribe error: topic=%s err=%v", ackTopic, err)
				}
			}
		}
		w.mqtt = mqtt.NewClient(opts)
	}
	return w
}

func normalizeAckTopicFilters(cfg WorkerConfig) []string {
	if len(cfg.AckTopicFilters) > 0 {
		topics := make([]string, 0, len(cfg.AckTopicFilters))
		for _, topic := range cfg.AckTopicFilters {
			if topic != "" {
				topics = append(topics, topic)
			}
		}
		if len(topics) > 0 {
			return topics
		}
	}
	if cfg.AckTopicFilter != "" {
		return []string{cfg.AckTopicFilter}
	}
	return []string{contracts.AckTopicFilter}
}

func (w *Worker) handleAckMessage(_ mqtt.Client, msg mqtt.Message) {
	var ack CommandAckMessage
	if err := json.Unmarshal(msg.Payload(), &ack); err != nil {
		log.Printf("command ack unmarshal error: %v", err)
		if w.metrics != nil {
			w.metrics.IncWorker("ack", "error")
		}
		return
	}
	if ack.CommandID == "" {
		ack.CommandID = string(msg.Payload())
	}
	if w.store != nil {
		if _, err := w.store.ackCommand(ack.CommandID, ack.TenantID, ack.DeviceID); err != nil {
			log.Printf("command ack store error: %v", err)
			if w.metrics != nil {
				w.metrics.IncWorker("ack", "error")
			}
		} else {
			log.Printf("command ack consumed: tenant=%s device=%s id=%s", ack.TenantID, ack.DeviceID, ack.CommandID)
			if w.metrics != nil {
				w.metrics.IncWorker("ack", "ok")
			}
		}
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if w == nil {
		return nil
	}
	log.Printf("worker starting: telemetryReader=%t commandReader=%t mqtt=%t", w.telemetryReader != nil, w.commandReader != nil, w.mqtt != nil)
	if w.mqtt != nil {
		token := w.mqtt.Connect()
		token.Wait()
		if err := token.Error(); err != nil {
			return err
		}
		defer w.mqtt.Disconnect(250)
	}
	errCh := make(chan error, 2)
	if w.telemetryReader != nil {
		go func() { errCh <- w.consumeTelemetry(ctx) }()
	}
	if w.commandReader != nil {
		go func() { errCh <- w.consumeCommands(ctx) }()
	}
	if w.telemetryReader == nil && w.commandReader == nil {
		<-ctx.Done()
		return nil
	}
	select {
	case <-ctx.Done():
		if w.telemetryReader != nil {
			_ = w.telemetryReader.Close()
		}
		if w.commandReader != nil {
			_ = w.commandReader.Close()
		}
		return nil
	case err := <-errCh:
		if err != nil {
			return err
		}
		return nil
	}
}

func (w *Worker) consumeTelemetry(ctx context.Context) error {
	for {
		msg, err := w.telemetryReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			log.Printf("telemetry fetch error: %v", err)
			if w.metrics != nil {
				w.metrics.IncWorker("telemetry", "error")
			}
			if isRetriableKafkaError(err) {
				sleepBeforeKafkaRetry(ctx)
				continue
			}
			return err
		}
		var rec TelemetryRecord
		if err := json.Unmarshal(msg.Value, &rec); err != nil {
			log.Printf("telemetry unmarshal error: %v", err)
			if w.metrics != nil {
				w.metrics.IncWorker("telemetry", "error")
			}
			_ = w.telemetryReader.CommitMessages(ctx, msg)
			continue
		}
		if !w.tenantAllowed(rec.TenantID) {
			_ = w.telemetryReader.CommitMessages(ctx, msg)
			continue
		}
		log.Printf("telemetry consumed: tenant=%s device=%s msg=%s", rec.TenantID, rec.DeviceID, rec.MsgID)
		env := contracts.Envelope{
			MsgID:    rec.MsgID,
			TenantID: rec.TenantID,
			DeviceID: rec.DeviceID,
			Ts:       rec.Ts,
			Type:     rec.Type,
			Version:  rec.Version,
			Payload:  rec.Payload,
		}
		if w.store != nil {
			if _, err := w.store.recordTelemetry(env); err != nil {
				log.Printf("telemetry store error: %v", err)
				if w.metrics != nil {
					w.metrics.IncWorker("telemetry", "error")
				}
				_ = w.telemetryReader.CommitMessages(ctx, msg)
				continue
			}
		}
		if w.tdengine != nil {
			if err := w.tdengine.WriteTelemetry(rec); err != nil {
				log.Printf("tdengine write error: %v", err)
				if w.metrics != nil {
					w.metrics.IncWorker("telemetry", "error")
				}
				_ = w.telemetryReader.CommitMessages(ctx, msg)
				continue
			}
		}
		if w.metrics != nil {
			w.metrics.IncWorker("telemetry", "ok")
		}
		_ = w.telemetryReader.CommitMessages(ctx, msg)
	}
}

func (w *Worker) consumeCommands(ctx context.Context) error {
	for {
		msg, err := w.commandReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			log.Printf("command fetch error: %v", err)
			if w.metrics != nil {
				w.metrics.IncWorker("command", "error")
			}
			if isRetriableKafkaError(err) {
				sleepBeforeKafkaRetry(ctx)
				continue
			}
			return err
		}
		var cmd Command
		if err := json.Unmarshal(msg.Value, &cmd); err != nil {
			log.Printf("command unmarshal error: %v", err)
			if w.metrics != nil {
				w.metrics.IncWorker("command", "error")
			}
			_ = w.commandReader.CommitMessages(ctx, msg)
			continue
		}
		if !w.tenantAllowed(cmd.TenantID) {
			_ = w.commandReader.CommitMessages(ctx, msg)
			continue
		}
		log.Printf("command consumed: tenant=%s device=%s id=%s", cmd.TenantID, cmd.DeviceID, cmd.ID)
		if w.mqtt != nil {
			topic, err := contracts.BuildCommandTopic(cmd.TenantID, cmd.DeviceID)
			if err != nil {
				log.Printf("command topic error: tenant=%s device=%s id=%s err=%v", cmd.TenantID, cmd.DeviceID, cmd.ID, err)
				if w.metrics != nil {
					w.metrics.IncWorker("command", "error")
				}
				_ = w.commandReader.CommitMessages(ctx, msg)
				continue
			}
			payload, err := json.Marshal(CommandDownlink{
				ID:        cmd.ID,
				TenantID:  cmd.TenantID,
				DeviceID:  cmd.DeviceID,
				Status:    cmd.Status,
				Payload:   cmd.Payload,
				CreatedAt: cmd.CreatedAt,
				UpdatedAt: cmd.UpdatedAt,
			})
			if err != nil {
				log.Printf("command marshal error: %v", err)
				if w.metrics != nil {
					w.metrics.IncWorker("command", "error")
				}
				_ = w.commandReader.CommitMessages(ctx, msg)
				continue
			}
			token := w.mqtt.Publish(topic, 1, false, payload)
			token.Wait()
			if err := token.Error(); err != nil {
				log.Printf("mqtt publish error: %v", err)
				if w.metrics != nil {
					w.metrics.IncWorker("command", "error")
				}
				_ = w.commandReader.CommitMessages(ctx, msg)
				continue
			}
		}
		if w.metrics != nil {
			w.metrics.IncWorker("command", "ok")
		}
		_ = w.commandReader.CommitMessages(ctx, msg)
	}
}

func tenantAllowlist(tenantIDs []string) map[string]struct{} {
	if len(tenantIDs) == 0 {
		return nil
	}
	allowlist := make(map[string]struct{}, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		if tenantID != "" {
			allowlist[tenantID] = struct{}{}
		}
	}
	if len(allowlist) == 0 {
		return nil
	}
	return allowlist
}

func (w *Worker) tenantAllowed(tenantID string) bool {
	if w == nil || len(w.tenantAllowlist) == 0 {
		return true
	}
	_, ok := w.tenantAllowlist[tenantID]
	return ok
}

func sleepBeforeKafkaRetry(ctx context.Context) {
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
