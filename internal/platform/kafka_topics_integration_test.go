package platform

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
)

func TestKafkaTopicFixturePrimesReconciliationCache(t *testing.T) {
	brokers := strings.Split(os.Getenv("IOT_KAFKA_TEST_BROKERS"), ",")
	if brokers[0] == "" {
		t.Skip("set IOT_KAFKA_TEST_BROKERS")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	controller, err := discoverKafkaController(ctx, brokers)
	if err != nil {
		t.Fatal(err)
	}
	address := kafka.TCP(net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	// Prime the same default-transport pool that production reconciliation uses
	// before the new topic exists. A bootstrap alias has a different cache key.
	client := &kafka.Client{Addr: address}
	if _, err := client.Metadata(ctx, &kafka.MetadataRequest{}); err != nil {
		t.Fatal(err)
	}
	bootstrapClient := &kafka.Client{Addr: kafka.TCP(brokers...)}
	if _, err := bootstrapClient.Metadata(ctx, &kafka.MetadataRequest{}); err != nil {
		t.Fatal(err)
	}
	original, topic := regressionKafkaTopic(t)
	if strings.Join(original, ",") != strings.Join(brokers, ",") {
		t.Fatal("fixture changed bootstrap addresses and bypassed the test case")
	}
	for _, cached := range []*kafka.Client{client, bootstrapClient} {
		metadata, err := cached.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
		if err != nil {
			t.Fatal(err)
		}
		if len(metadata.Topics) != 1 || metadata.Topics[0].Error != nil || len(metadata.Topics[0].Partitions) != 1 {
			t.Fatalf("fixture left cache stale at %s: %+v", cached.Addr, metadata.Topics)
		}
	}
	// This is deliberately one-shot: retries here would hide broken setup.
	if err := reconcileKafkaTopics(ctx, address, []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileKafkaTopicsPreservesUnmanagedConfigs(t *testing.T) {
	brokers := strings.Split(os.Getenv("IOT_KAFKA_TEST_BROKERS"), ",")
	if brokers[0] == "" {
		t.Skip("set IOT_KAFKA_TEST_BROKERS to run Kafka regressions")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		t.Fatal(err)
	}
	client := &kafka.Client{Addr: kafka.TCP(net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))}
	topic := fmt.Sprintf("iot-regression-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.DeleteTopics(cleanup, &kafka.DeleteTopicsRequest{Topics: []string{topic}})
		if err != nil {
			t.Error(err)
		}
	})
	created, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1, ConfigEntries: []kafka.ConfigEntry{{ConfigName: "retention.ms", ConfigValue: "100000"}, {ConfigName: "min.insync.replicas", ConfigValue: "1"}, {ConfigName: "max.message.bytes", ConfigValue: "2097152"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = created.Errors[topic]; err != nil {
		t.Fatal(err)
	}
	if err = waitForKafkaTopicMetadata(ctx, client, topic); err != nil {
		t.Fatal(err)
	}
	desired := []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1, ConfigEntries: []kafka.ConfigEntry{{ConfigName: "retention.ms", ConfigValue: "200000"}, {ConfigName: "min.insync.replicas", ConfigValue: "1"}}}}
	for i := 0; i < 2; i++ {
		if err = reconcileKafkaTopics(ctx, client.Addr, desired); err != nil {
			t.Fatal(err)
		}
		response, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{Resources: []kafka.DescribeConfigRequestResource{{ResourceType: kafka.ResourceTypeTopic, ResourceName: topic, ConfigNames: []string{"retention.ms", "min.insync.replicas", "max.message.bytes"}}}})
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Resources) != 1 || response.Resources[0].Error != nil {
			t.Fatalf("describe: %+v", response.Resources)
		}
		values := map[string]string{}
		for _, entry := range response.Resources[0].ConfigEntries {
			values[entry.ConfigName] = entry.ConfigValue
		}
		for name, want := range map[string]string{"retention.ms": "200000", "min.insync.replicas": "1", "max.message.bytes": "2097152"} {
			if values[name] != want {
				t.Errorf("%s=%s want=%s", name, values[name], want)
			}
		}
	}
}

// CreateTopics success does not imply topic/leader metadata has reached every
// broker. Establish that prerequisite before testing one-shot reconciliation.
func waitForKafkaTopicMetadata(ctx context.Context, client *kafka.Client, topic string) error {
	var last error
	for {
		metadata, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
		if err == nil {
			err = fmt.Errorf("topic %s metadata is not ready", topic)
			for _, item := range metadata.Topics {
				if item.Name != topic {
					continue
				}
				if item.Error != nil {
					err = item.Error
					break
				}
				ready := len(item.Partitions) > 0
				for _, partition := range item.Partitions {
					ready = ready && partition.Error == nil && partition.Leader.Host != "" && len(partition.Replicas) > 0 && len(partition.Isr) > 0
				}
				if ready {
					return nil
				}
			}
		}
		last = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s metadata (%v): %w", topic, last, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

type delayedTopicMetadata struct {
	calls       int
	unavailable bool
}

func (d *delayedTopicMetadata) RoundTrip(_ context.Context, _ net.Addr, _ kafka.Request) (kafka.Response, error) {
	d.calls++
	topic := metadataAPI.ResponseTopic{Name: "new-topic", ErrorCode: int16(kafka.UnknownTopicOrPartition)}
	if !d.unavailable && d.calls > 1 {
		topic.ErrorCode = 0
		topic.Partitions = []metadataAPI.ResponsePartition{{LeaderID: 1, ReplicaNodes: []int32{1}}}
		if d.calls > 2 {
			topic.Partitions[0].IsrNodes = []int32{1}
		}
	}
	return &metadataAPI.Response{Brokers: []metadataAPI.ResponseBroker{{NodeID: 1, Host: "broker", Port: 9092}}, Topics: []metadataAPI.ResponseTopic{topic}}, nil
}

func TestKafkaMetadataSetupWaitsForTopicAndLeaderReadiness(t *testing.T) {
	transport := &delayedTopicMetadata{}
	client := &kafka.Client{Addr: kafka.TCP("broker:9092"), Transport: transport}
	if err := waitForKafkaTopicMetadata(t.Context(), client, "new-topic"); err != nil {
		t.Fatal(err)
	}
	if transport.calls != 3 {
		t.Fatalf("metadata checked %d times, want unknown topic -> no ISR -> ready", transport.calls)
	}
	transport.unavailable = true
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	if err := waitForKafkaTopicMetadata(ctx, client, "new-topic"); err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("missing setup deadline: %v", err)
	}
}

func TestKafkaInitializationRejectsReplicaAndPartitionDrift(t *testing.T) {
	brokers, topic := regressionKafkaTopic(t)
	for _, cfg := range []KafkaTopicConfig{{ReplicationFactor: 2, MinInsyncReplicas: 1, NumPartitions: 1}, {ReplicationFactor: 1, MinInsyncReplicas: 1, NumPartitions: 2}} {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		err := ensureKafkaTopics(ctx, brokers, cfg, topic)
		cancel()
		if err == nil || !strings.Contains(err.Error(), "operational change required") {
			t.Fatalf("drift did not block startup: cfg=%+v err=%v", cfg, err)
		}
	}
}

func TestKafkaBootstrapFallback(t *testing.T) {
	for _, stalled := range []bool{false, true} {
		t.Run(fmt.Sprint(stalled), func(t *testing.T) {
			brokers, topic := regressionKafkaTopic(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			defer listener.Close()
			if stalled {
				done := make(chan struct{})
				go func() {
					defer close(done)
					c, e := listener.Accept()
					if e == nil {
						defer c.Close()
						_, _ = io.Copy(io.Discard, c)
					}
				}()
				defer func() { listener.Close(); <-done }()
			} else {
				listener.Close()
			}
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			if err = ensureKafkaTopics(ctx, append([]string{address}, brokers...), KafkaTopicConfig{}, topic); err != nil {
				t.Fatalf("healthy second bootstrap was ignored: %v", err)
			}
		})
	}
}
