package platform

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol/describeconfigs"
	"github.com/segmentio/kafka-go/protocol/incrementalalterconfigs"
	"github.com/segmentio/kafka-go/protocol/metadata"
)

// The controller may acknowledge a config change before a read observes it.
// Initialization must wait rather than expose a writer with the old 1 MiB cap.
type delayedKafkaConfig struct {
	altered    bool
	reads      int
	neverReady bool
	limit      string
	alterCalls int
}

func (d *delayedKafkaConfig) RoundTrip(_ context.Context, _ net.Addr, req kafka.Request) (kafka.Response, error) {
	switch req.(type) {
	case *metadata.Request:
		return &metadata.Response{Brokers: []metadata.ResponseBroker{{NodeID: 1, Host: "broker", Port: 9092}}, Topics: []metadata.ResponseTopic{{Name: "dlq", Partitions: []metadata.ResponsePartition{{LeaderID: 1, ReplicaNodes: []int32{1}, IsrNodes: []int32{1}}}}}}, nil
	case *incrementalalterconfigs.Request:
		d.alterCalls++
		d.altered = true
		return &incrementalalterconfigs.Response{Responses: []incrementalalterconfigs.ResponseAlterResponse{{ResourceType: 2, ResourceName: "dlq"}}}, nil
	case *describeconfigs.Request:
		limit := "1048576"
		if d.limit != "" {
			limit = d.limit
		}
		if d.altered {
			d.reads++
			if !d.neverReady && d.reads >= 3 {
				limit = "2097152"
			}
		}
		return &describeconfigs.Response{Resources: []describeconfigs.ResponseResource{{ResourceType: 2, ResourceName: "dlq", ConfigEntries: []describeconfigs.ResponseConfigEntry{{ConfigName: "max.message.bytes", ConfigValue: limit}}}}}, nil
	default:
		return nil, errors.New("unexpected Kafka API")
	}
}

func TestKafkaReconciliationWaitsForConfigVisibility(t *testing.T) {
	for _, tc := range []struct {
		name       string
		neverReady bool
	}{
		{name: "delayed"},
		{name: "deadline", neverReady: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &delayedKafkaConfig{neverReady: tc.neverReady}
			client := &kafka.Client{Addr: kafka.TCP("broker:9092"), Transport: transport}
			timeout := 3 * time.Second
			if tc.neverReady {
				timeout = 80 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			desired := []kafka.TopicConfig{{Topic: "dlq", NumPartitions: 1, ReplicationFactor: 1, ConfigEntries: []kafka.ConfigEntry{{ConfigName: "max.message.bytes", ConfigValue: "2097152"}}}}
			err := reconcileKafkaTopicsWithClient(ctx, client, desired)
			if tc.neverReady {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("unapplied config reported ready: %v", err)
				}
			} else if err != nil || transport.reads < 3 {
				t.Fatalf("returned before config was visible: reads=%d err=%v", transport.reads, err)
			}
			if transport.alterCalls != 1 {
				t.Fatalf("config rewritten while waiting: %d alterations", transport.alterCalls)
			}
		})
	}
}

func TestKafkaReconciliationKeepsLargerMessageLimit(t *testing.T) {
	transport := &delayedKafkaConfig{limit: "4194304"}
	client := &kafka.Client{Addr: kafka.TCP("broker:9092"), Transport: transport}
	desired := []kafka.TopicConfig{{Topic: "dlq", NumPartitions: 1, ReplicationFactor: 1, ConfigEntries: []kafka.ConfigEntry{{ConfigName: "max.message.bytes", ConfigValue: "2097152"}}}}
	if err := reconcileKafkaTopicsWithClient(t.Context(), client, desired); err != nil {
		t.Fatal(err)
	}
	if transport.alterCalls != 0 {
		t.Fatal("larger operator message limit was overwritten")
	}
}
