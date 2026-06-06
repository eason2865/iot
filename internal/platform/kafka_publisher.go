package platform

import (
	"context"
	"encoding/json"
	"fmt"
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
	return &KafkaPublisher{
		telemetryWriter: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        telemetryTopic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireOne,
			BatchSize:    1,
			BatchTimeout: 10 * time.Millisecond,
		},
		commandWriter: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        commandTopic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireOne,
			BatchSize:    1,
			BatchTimeout: 10 * time.Millisecond,
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

func (p *KafkaPublisher) publishTelemetry(record TelemetryRecord) error {
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
	err = p.telemetryWriter.WriteMessages(context.Background(), kafka.Message{
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

func (p *KafkaPublisher) PublishTelemetry(record TelemetryRecord) error {
	return p.publishTelemetry(record)
}

func (p *KafkaPublisher) publishCommand(cmd Command) error {
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
	err = p.commandWriter.WriteMessages(context.Background(), kafka.Message{
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

func (p *KafkaPublisher) PublishCommand(cmd Command) error { return p.publishCommand(cmd) }
