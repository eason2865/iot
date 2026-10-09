package platform

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Dead-letter stages. These strings are operational identifiers: they appear in
// iot.dlq records, in the `-stage` filter of cmd/dlq-replay, and as the `stage`
// label of iot_dlq_publish_total. They are constants so the label cannot grow an
// accidental value through a typo in a call site.
const (
	StageMQTTDecode        = "mqtt.decode"
	StageMQTTIdentity      = "mqtt.identity"
	StageMQTTKafka         = "mqtt.kafka"
	StageTelemetryDecode   = "telemetry.decode"
	StageTelemetryPostgres = "telemetry.postgres"
	StageTelemetryTDengine = "telemetry.tdengine"
	StageCommandDecode     = "command.decode"
	StageCommandTopic      = "command.topic"
	StageCommandEncode     = "command.encode"
	StageCommandMQTT       = "command.mqtt"
)

// DeadLetterStages lists every stage that can appear in iot.dlq. cmd/dlq-replay
// and the metrics seeding use it so a new stage cannot be left unregistered.
func DeadLetterStages() []string {
	return []string{
		StageMQTTDecode,
		StageMQTTIdentity,
		StageMQTTKafka,
		StageTelemetryDecode,
		StageTelemetryPostgres,
		StageTelemetryTDengine,
		StageCommandDecode,
		StageCommandTopic,
		StageCommandEncode,
		StageCommandMQTT,
	}
}

type DeadLetter struct {
	SourceTopic     string `json:"sourceTopic"`
	SourcePartition int    `json:"sourcePartition"`
	SourceOffset    int64  `json:"sourceOffset"`
	Key             string `json:"key,omitempty"`
	ValueBase64     string `json:"valueBase64"`
	Stage           string `json:"stage"`
	Error           string `json:"error"`
	OccurredAt      string `json:"occurredAt"`
}

// publishDeadLetter records the message in the dead-letter topic. Every outcome
// is counted in iot_dlq_publish_total: a failure here means the message was NOT
// dead-lettered, which is the state that turns a bad record into a stuck
// consumer or a crash loop, so it must be visible rather than only logged.
func publishDeadLetter(writer *kafka.Writer, msg kafka.Message, stage string, cause error, metrics *Metrics) error {
	if writer == nil {
		if metrics != nil {
			metrics.IncDLQPublish(stage, "error")
		}
		return fmt.Errorf("dead-letter writer is not configured")
	}
	payload, err := json.Marshal(DeadLetter{SourceTopic: msg.Topic, SourcePartition: msg.Partition, SourceOffset: msg.Offset, Key: string(msg.Key), ValueBase64: base64.StdEncoding.EncodeToString(msg.Value), Stage: stage, Error: cause.Error(), OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		if metrics != nil {
			metrics.IncDLQPublish(stage, "error")
		}
		return err
	}
	if err := writeKafkaMessageWithRetry(writer, kafka.Message{Key: []byte(msg.Topic), Value: payload}); err != nil {
		if metrics != nil {
			metrics.IncDLQPublish(stage, "error")
		}
		return err
	}
	if metrics != nil {
		metrics.IncDLQPublish(stage, "ok")
	}
	return nil
}

func commitAfterDeadLetter(ctx context.Context, writer *kafka.Writer, reader *kafka.Reader, msg kafka.Message, stage string, cause error, metrics *Metrics) error {
	if err := publishDeadLetter(writer, msg, stage, cause, metrics); err != nil {
		return err
	}
	return reader.CommitMessages(ctx, msg)
}
