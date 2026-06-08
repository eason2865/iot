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
	ensureKafkaTopicsBestEffort(cfg.Brokers, telemetryTopic, commandTopic)
	return &KafkaPublisher{
		telemetryWriter: &kafka.Writer{
			Addr:                   kafka.TCP(cfg.Brokers...),
			Topic:                  telemetryTopic,
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireOne,
			BatchSize:              1,
			BatchTimeout:           10 * time.Millisecond,
			AllowAutoTopicCreation: true,
		},
		commandWriter: &kafka.Writer{
			Addr:                   kafka.TCP(cfg.Brokers...),
			Topic:                  commandTopic,
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireOne,
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

func (p *KafkaPublisher) PublishTelemetry(record TelemetryRecord) error {
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
	err = writeKafkaMessageWithRetry(p.telemetryWriter, kafka.Message{
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

func (p *KafkaPublisher) PublishCommand(cmd Command) error {
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
	err = writeKafkaMessageWithRetry(p.commandWriter, kafka.Message{
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

func writeKafkaMessageWithRetry(writer *kafka.Writer, msg kafka.Message) error {
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
			time.Sleep(delay)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = writer.WriteMessages(ctx, msg)
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
