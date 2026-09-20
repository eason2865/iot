package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"iot/internal/platform"
)

// dlq-replay republishes each dead-letter record to its original topic. Run it
// as a bounded batch after inspecting the stage/error fields.
func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	dlqTopic := flag.String("topic", "iot.dlq", "dead-letter topic")
	limit := flag.Int("limit", 100, "maximum records to replay")
	flag.Parse()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: splitCSV(*brokers), Topic: *dlqTopic, GroupID: "iot-dlq-replay", StartOffset: kafka.FirstOffset, MaxBytes: 10e6})
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	count := 0
	for count < *limit {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatal(err)
		}
		var item platform.DeadLetter
		if err := json.Unmarshal(msg.Value, &item); err != nil {
			log.Printf("skip malformed DLQ record: %v", err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}
		value, err := base64.StdEncoding.DecodeString(item.ValueBase64)
		if err != nil {
			log.Printf("skip invalid payload: %v", err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}
		writer := &kafka.Writer{Addr: kafka.TCP(splitCSV(*brokers)...), Topic: item.SourceTopic, RequiredAcks: kafka.RequireAll, BatchSize: 1}
		err = writer.WriteMessages(ctx, kafka.Message{Key: []byte(item.Key), Value: value, Headers: []kafka.Header{{Key: "x-replayed-from-dlq", Value: []byte("true")}}})
		_ = writer.Close()
		if err != nil {
			log.Fatal(err)
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Fatal(err)
		}
		count++
		log.Printf("replayed stage=%s topic=%s offset=%d", item.Stage, item.SourceTopic, item.SourceOffset)
	}
}

func splitCSV(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if strings.TrimSpace(item) != "" {
			out = append(out, strings.TrimSpace(item))
		}
	}
	return out
}
