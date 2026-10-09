package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaPublisher struct {
	telemetryWriter *kafka.Writer
	commandWriter   *kafka.Writer
	metrics         *Metrics
}

type KafkaPublisherConfig struct {
	Brokers        []string
	TelemetryTopic string
	CommandTopic   string
	TopicConfig    KafkaTopicConfig
}

func NewKafkaPublisher(cfg KafkaPublisherConfig, metrics *Metrics) *KafkaPublisher {
	if len(cfg.Brokers) == 0 {
		return nil
	}
	telemetryTopic := cfg.TelemetryTopic
	if telemetryTopic == "" {
		telemetryTopic = "iot.telemetry"
	}
	commandTopic := cfg.CommandTopic
	if commandTopic == "" {
		commandTopic = "iot.command"
	}
	ensureKafkaTopicsBestEffort(cfg.Brokers, cfg.TopicConfig, telemetryTopic, commandTopic)
	return &KafkaPublisher{
		telemetryWriter: &kafka.Writer{
			Addr:     kafka.TCP(cfg.Brokers...),
			Topic:    telemetryTopic,
			Balancer: &kafka.Hash{},
			// RequireAll: this producer carries events that PostgreSQL already
			// considers committed, so a leader crash after an un-replicated ack
			// would lose an event the database believes it published — the DLQ
			// cannot cover that, it only covers consumer-side failures. BatchSize
			// is 1, so each message already pays a full round trip; RequireAll
			// only adds waiting for the slowest in-sync replica.
			RequiredAcks:           kafka.RequireAll,
			BatchSize:              1,
			BatchTimeout:           10 * time.Millisecond,
			AllowAutoTopicCreation: true,
		},
		commandWriter: &kafka.Writer{
			Addr:     kafka.TCP(cfg.Brokers...),
			Topic:    commandTopic,
			Balancer: &kafka.Hash{},
			// RequireAll: this producer carries events that PostgreSQL already
			// considers committed, so a leader crash after an un-replicated ack
			// would lose an event the database believes it published — the DLQ
			// cannot cover that, it only covers consumer-side failures. BatchSize
			// is 1, so each message already pays a full round trip; RequireAll
			// only adds waiting for the slowest in-sync replica.
			RequiredAcks:           kafka.RequireAll,
			BatchSize:              1,
			BatchTimeout:           10 * time.Millisecond,
			AllowAutoTopicCreation: true,
		},
		metrics: metrics,
	}
}

func (p *KafkaPublisher) Close() error {
	if p == nil {
		return nil
	}
	var errs []string
	if p.telemetryWriter != nil {
		if err := p.telemetryWriter.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if p.commandWriter != nil {
		if err := p.commandWriter.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func (p *KafkaPublisher) PublishTelemetry(ctx context.Context, record TelemetryRecord) error {
	if p == nil || p.telemetryWriter == nil {
		return nil
	}
	value, err := json.Marshal(record)
	if err != nil {
		if p.metrics != nil {
			p.metrics.IncKafkaPublish("telemetry", "error")
		}
		return err
	}
	err = writeKafkaMessageWithRetry(ctx, p.telemetryWriter, kafka.Message{
		Key:   []byte(record.DeviceID),
		Value: value,
	})
	if err != nil {
		if p.metrics != nil {
			p.metrics.IncKafkaPublish("telemetry", "error")
		}
		return err
	}
	if p.metrics != nil {
		p.metrics.IncKafkaPublish("telemetry", "ok")
	}
	return nil
}

func (p *KafkaPublisher) PublishCommand(ctx context.Context, cmd Command) error {
	if p == nil || p.commandWriter == nil {
		return nil
	}
	value, err := json.Marshal(cmd)
	if err != nil {
		if p.metrics != nil {
			p.metrics.IncKafkaPublish("command", "error")
		}
		return err
	}
	err = writeKafkaMessageWithRetry(ctx, p.commandWriter, kafka.Message{
		Key:   []byte(cmd.DeviceID),
		Value: value,
	})
	if err != nil {
		if p.metrics != nil {
			p.metrics.IncKafkaPublish("command", "error")
		}
		return err
	}
	if p.metrics != nil {
		p.metrics.IncKafkaPublish("command", "ok")
	}
	return nil
}

func writeKafkaMessageWithRetry(ctx context.Context, writer *kafka.Writer, msg kafka.Message) error {
	var err error
	for _, delay := range []time.Duration{
		0,
		200 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
	} {
		if delay > 0 {
			// The backoff wait is interruptible: without this a shutdown could
			// be stalled by the full ~32s retry schedule.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = writer.WriteMessages(attemptCtx, msg)
		cancel()
		if err == nil {
			return nil
		}
		if !isRetriableKafkaError(err) {
			return err
		}
	}
	return err
}

func isRetriableKafkaError(err error) bool {
	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) && kafkaErr.Temporary() {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Temporary() {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection refused")
}
