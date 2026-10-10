package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"iot/internal/contracts"
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"iot/internal/platform"
)

// dlq-replay republishes dead-letter records to their original topic, after the
// operator has fixed the root cause and inspected what is there.
//
// It is deliberately a bounded, operator-driven batch rather than a background
// consumer; see docs/adr/0006 for why. Two properties matter for that decision to
// be safe:
//
//   - A record is only republished once the operator asked for it, and only
//     after the failing stage was actually fixed. A record replayed into a still
//     broken path simply lands back in the DLQ, and a replayed command re-sends
//     its MQTT downlink to the device.
//   - A run that does not ask to mutate anything cannot move the consumer group,
//     so -dry-run is safe to repeat.
func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	dlqTopic := flag.String("topic", "iot.dlq", "dead-letter topic")
	limit := flag.Int("limit", 100, "maximum records to replay")
	stage := flag.String("stage", "", "only replay records from this stage; empty replays all stages. Known stages: "+strings.Join(platform.DeadLetterStages(), ", "))
	group := flag.String("group", "", "consumer group to resume from; defaults to one group per stage")
	dryRun := flag.Bool("dry-run", false, "inspect matching records without republishing or committing anything")
	idleTimeout := flag.Duration("idle-timeout", 5*time.Second, "stop successfully when no record arrives within this window")
	flag.Parse()

	groupID := *group
	if groupID == "" {
		groupID = replayGroupID(*stage)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     splitCSV(*brokers),
		Topic:       *dlqTopic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	var ackStore *platform.PostgresStore
	defer func() {
		if ackStore != nil {
			_ = ackStore.Close()
		}
	}()
	var scanned, replayed, skipped int
	for replayed < *limit {
		// Bound each fetch so reaching the end of the topic ends the run
		// successfully, instead of blocking until the overall timeout and then
		// exiting non-zero as if something had failed.
		fetchCtx, cancelFetch := context.WithTimeout(ctx, *idleTimeout)
		msg, err := reader.FetchMessage(fetchCtx)
		cancelFetch()
		if err != nil {
			if ctx.Err() != nil {
				log.Fatalf("replay stopped after the overall timeout: %v", err)
			}
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			log.Fatal(err)
		}
		scanned++

		var item platform.DeadLetter
		if err := json.Unmarshal(msg.Value, &item); err != nil {
			log.Printf("skip malformed DLQ record: %v", err)
			// A malformed record can never be replayed, so move past it rather
			// than reporting it on every future run.
			commitReplay(ctx, reader, msg, *dryRun)
			skipped++
			continue
		}
		if *stage != "" && item.Stage != *stage {
			// Never commit a record from another stage. Committing any record
			// advances this group past everything before it, and a later run
			// targeting that stage would never see those records again.
			skipped++
			continue
		}
		if *dryRun {
			log.Printf("dry-run stage=%s topic=%s offset=%d key=%s err=%s", item.Stage, item.SourceTopic, item.SourceOffset, item.Key, item.Error)
			replayed++
			continue
		}
		value, err := base64.StdEncoding.DecodeString(item.ValueBase64)
		if err != nil {
			log.Printf("skip invalid payload: %v", err)
			commitReplay(ctx, reader, msg, false)
			skipped++
			continue
		}
		if item.Stage == platform.StageCommandAck {
			if ackStore == nil {
				dsn := os.Getenv("POSTGRES_DSN")
				if dsn == "" {
					log.Fatal("command.ack replay requires POSTGRES_DSN")
				}
				ackStore, err = platform.NewPostgresStore(dsn, 5*time.Minute)
				if err != nil {
					log.Fatal("cannot open ACK replay store")
				}
			}
			err = replayAck(ctx, item, value, ackStore)
		} else {
			topic, payload, planErr := replayKafkaPayload(item, value)
			if planErr != nil {
				log.Fatalf("cannot replay stage=%s: %v", item.Stage, planErr)
			}
			writer := &kafka.Writer{Addr: kafka.TCP(splitCSV(*brokers)...), Topic: topic, RequiredAcks: kafka.RequireAll, BatchSize: 1}
			err = writer.WriteMessages(ctx, kafka.Message{Key: []byte(item.Key), Value: payload, Headers: []kafka.Header{{Key: "x-replayed-from-dlq", Value: []byte("true")}}})
			_ = writer.Close()
		}
		if err != nil {
			log.Fatalf("replay stage=%s offset=%d failed: %v", item.Stage, item.SourceOffset, err)
		}

		commitReplay(ctx, reader, msg, false)
		replayed++
		log.Printf("replayed stage=%s topic=%s offset=%d", item.Stage, item.SourceTopic, item.SourceOffset)
	}

	log.Printf("dlq-replay done: group=%s stage=%q scanned=%d replayed=%d skipped=%d dryRun=%t",
		groupID, *stage, scanned, replayed, skipped, *dryRun)
}

// replayGroupID keeps one consumer group per stage. A single shared group loses
// records: committing any matching record moves the group position past every
// earlier record, so a later run targeting a different stage would never see
// them. Per-stage groups also keep "already replayed" durable within a stage,
// since a run only ever commits records of its own stage.
func replayGroupID(stage string) string {
	if stage == "" {
		return "iot-dlq-replay-all"
	}
	return "iot-dlq-replay-" + stage
}

// commitReplay advances the group position, unless this is a dry run: a run that
// promises not to change anything must not move the offset either.
func commitReplay(ctx context.Context, reader *kafka.Reader, msg kafka.Message, dryRun bool) {
	if dryRun {
		return
	}
	if err := reader.CommitMessages(ctx, msg); err != nil {
		log.Fatalf("commit offset %d: %v", msg.Offset, err)
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

type ackReplayStore interface {
	AckCommandContext(context.Context, string, string, string) (platform.Command, error)
}

func replayAck(ctx context.Context, item platform.DeadLetter, value []byte, store ackReplayStore) error {
	var ack platform.CommandAckMessage
	if err := json.Unmarshal(value, &ack); err != nil {
		return err
	}
	tenant, device, suffix, ok := contracts.ParseDeviceTopic(item.SourceTopic)
	if !ok || suffix != contracts.TopicSuffixAck || tenant != ack.TenantID || device != ack.DeviceID || ack.CommandID == "" {
		return fmt.Errorf("ACK identity does not match source topic")
	}
	_, err := store.AckCommandContext(ctx, ack.CommandID, ack.TenantID, ack.DeviceID)
	return err
}

func replayKafkaPayload(item platform.DeadLetter, value []byte) (string, []byte, error) {
	if item.Stage == platform.StageMQTTDecode || item.Stage == platform.StageMQTTIdentity {
		return "", nil, fmt.Errorf("invalid MQTT messages must be corrected at their source")
	}
	if item.Stage != platform.StageMQTTKafka {
		return item.SourceTopic, value, nil
	}
	var env contracts.Envelope
	if err := json.Unmarshal(value, &env); err != nil {
		return "", nil, err
	}
	tenant, device, suffix, ok := contracts.ParseDeviceTopic(item.SourceTopic)
	if !ok || suffix != contracts.TopicSuffixTelemetry || tenant != env.TenantID || device != env.DeviceID {
		return "", nil, fmt.Errorf("telemetry identity does not match source topic")
	}
	topic := os.Getenv("KAFKA_TELEMETRY_TOPIC")
	if topic == "" {
		topic = "iot.telemetry"
	}
	payload, err := json.Marshal(platform.TelemetryRecord{MsgID: env.MsgID, TenantID: env.TenantID, DeviceID: env.DeviceID, Ts: env.Ts, Type: env.Type, Version: env.Version, Payload: env.Payload, ReceivedAt: time.Now().UTC()})
	return topic, payload, err
}
