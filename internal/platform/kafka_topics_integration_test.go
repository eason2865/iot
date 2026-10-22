package platform

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

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
	client := &kafka.Client{Addr: kafka.TCP(fmt.Sprintf("%s:%d", controller.Host, controller.Port))}
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
