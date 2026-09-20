package platform

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

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

func publishDeadLetter(writer *kafka.Writer, msg kafka.Message, stage string, cause error) error {
	if writer == nil {
		return fmt.Errorf("dead-letter writer is not configured")
	}
	payload, err := json.Marshal(DeadLetter{SourceTopic: msg.Topic, SourcePartition: msg.Partition, SourceOffset: msg.Offset, Key: string(msg.Key), ValueBase64: base64.StdEncoding.EncodeToString(msg.Value), Stage: stage, Error: cause.Error(), OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}
	return writeKafkaMessageWithRetry(writer, kafka.Message{Key: []byte(msg.Topic), Value: payload})
}

func commitAfterDeadLetter(ctx context.Context, writer *kafka.Writer, reader *kafka.Reader, msg kafka.Message, stage string, cause error) error {
	if err := publishDeadLetter(writer, msg, stage, cause); err != nil {
		return err
	}
	return reader.CommitMessages(ctx, msg)
}
