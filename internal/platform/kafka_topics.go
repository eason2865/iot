package platform

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaTopicConfig describes how this service creates its topics when they are
// missing. Local development runs a single broker, so the defaults stay at
// replication 1; production must raise both values (see docs/adr/0004). With
// min.insync.replicas=1 a producer using acks=all is no more durable than
// acks=1, because the ISR can shrink to the leader alone and writes still
// succeed there.
type KafkaTopicConfig struct {
	// SetupTimeout bounds startup reconciliation; zero uses 60 seconds.
	SetupTimeout      time.Duration
	ReplicationFactor int
	MinInsyncReplicas int
	// NumPartitions is the partition count for topics this service creates.
	// Single-partition topics serialize all consumption onto one consumer per
	// group, so production deployments that need horizontal scale must raise
	// it. Like the replication factor it only applies at creation time.
	NumPartitions int
	// RetentionMs sets retention.ms on the topics created with this config. Zero
	// or negative leaves the broker default in place, which is not necessarily
	// bounded: a broker configured with -1 never deletes. The dead-letter topic
	// uses this so an unreplayed backlog cannot grow without limit.
	RetentionMs int
	// MinMessageBytes raises max.message.bytes when necessary. Larger existing
	// operator limits are preserved. Zero leaves this entry unmanaged.
	MinMessageBytes int
}

func (c KafkaTopicConfig) normalized() KafkaTopicConfig {
	if c.ReplicationFactor <= 0 {
		c.ReplicationFactor = 1
	}
	if c.MinInsyncReplicas <= 0 {
		c.MinInsyncReplicas = 1
	}
	if c.NumPartitions <= 0 {
		c.NumPartitions = 1
	}
	return c
}

// buildTopicConfigs is pure so the replication and in-sync-replica rules can be
// unit tested without a broker. CreateTopics is idempotent for an existing
// topic (kafka-go skips TopicAlreadyExists); ensureKafkaTopics follows it with
// a reconciliation pass that incrementally SETs drifted config entries. Partition
// count and replication factor still only take effect on first creation —
// changing them on a live topic is an operational task, not something this
// service can do, so such drift fails startup until an operator fixes it.
func buildTopicConfigs(topics []string, cfg KafkaTopicConfig) []kafka.TopicConfig {
	cfg = cfg.normalized()
	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		topicConfig := kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     cfg.NumPartitions,
			ReplicationFactor: cfg.ReplicationFactor,
		}
		// The broker rejects min.insync.replicas above the replication factor.
		// Report the misconfiguration instead of creating a topic that silently
		// lacks the durability setting the deployment asked for.
		if cfg.MinInsyncReplicas > cfg.ReplicationFactor {
			log.Printf("kafka topic %s: min.insync.replicas=%d exceeds replicationFactor=%d, leaving it at the broker default",
				topic, cfg.MinInsyncReplicas, cfg.ReplicationFactor)
		} else {
			topicConfig.ConfigEntries = append(topicConfig.ConfigEntries, kafka.ConfigEntry{
				ConfigName:  "min.insync.replicas",
				ConfigValue: strconv.Itoa(cfg.MinInsyncReplicas),
			})
		}
		if cfg.RetentionMs > 0 {
			topicConfig.ConfigEntries = append(topicConfig.ConfigEntries, kafka.ConfigEntry{
				ConfigName:  "retention.ms",
				ConfigValue: strconv.Itoa(cfg.RetentionMs),
			})
		}
		if cfg.MinMessageBytes > 0 {
			topicConfig.ConfigEntries = append(topicConfig.ConfigEntries, kafka.ConfigEntry{
				ConfigName: "max.message.bytes", ConfigValue: strconv.Itoa(cfg.MinMessageBytes),
			})
		}
		configs = append(configs, topicConfig)
	}
	return configs
}

func initializeKafkaTopics(brokers []string, cfg KafkaTopicConfig, topics ...string) error {
	timeout := cfg.SetupTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return ensureKafkaTopicsUntil(ctx, brokers, cfg, topics...)
}

func ensureKafkaTopicsUntil(ctx context.Context, brokers []string, cfg KafkaTopicConfig, topics ...string) error {
	if cfg.normalized().MinInsyncReplicas > cfg.normalized().ReplicationFactor {
		return fmt.Errorf("min.insync.replicas exceeds replication factor")
	}
	var last error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		last = ensureKafkaTopics(attemptCtx, brokers, cfg, topics...)
		cancel()
		if last == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("Kafka topic initialization failed (%v): %w", last, ctx.Err())
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("Kafka topic initialization failed (%v): %w", last, ctx.Err())
		case <-timer.C:
		}
	}
}

func ensureKafkaTopics(ctx context.Context, brokers []string, cfg KafkaTopicConfig, topics ...string) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	defer func() {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	if len(brokers) == 0 || len(topics) == 0 {
		return nil
	}
	if cfg.normalized().MinInsyncReplicas > cfg.normalized().ReplicationFactor {
		return fmt.Errorf("min.insync.replicas exceeds replication factor")
	}
	configs := buildTopicConfigs(topics, cfg)
	if len(configs) == 0 {
		return nil
	}

	controller, err := discoverKafkaController(ctx, brokers)
	if err != nil {
		return err
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, closeController, err := dialKafkaSetup(ctx, controllerAddr)
	if err != nil {
		return err
	}
	defer closeController()

	if err := controllerConn.CreateTopics(configs...); err != nil {
		return err
	}
	return reconcileKafkaTopics(ctx, kafka.TCP(controllerAddr), configs)
}

// kafka.Conn protocol methods lack context parameters. Bound both I/O and
// explicit cancellation, including a caller that cancels before its deadline.
func dialKafkaSetup(ctx context.Context, address string) (*kafka.Conn, func(), error) {
	conn, err := kafka.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err = conn.SetDeadline(deadline); err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	return conn, func() { stop(); _ = conn.Close() }, nil
}

// reconcileKafkaTopics brings pre-existing topics (broker auto.create, or a
// previous deployment with different settings) in line with the desired
// config. Config entries are compared and corrected via IncrementalAlterConfigs; drift in
// partition count or replication factor cannot be fixed safely here, so it is
// returned as an error until an operator changes the assignment.
func reconcileKafkaTopics(ctx context.Context, addr net.Addr, desired []kafka.TopicConfig) error {
	client := &kafka.Client{Addr: addr}

	names := make([]string, 0, len(desired))
	for _, tc := range desired {
		names = append(names, tc.Topic)
	}
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: names})
	if err != nil {
		return err
	}
	topics := make(map[string]kafka.Topic, len(meta.Topics))
	for _, topic := range meta.Topics {
		topics[topic.Name] = topic
	}

	for _, tc := range desired {
		live, ok := topics[tc.Topic]
		if !ok {
			return fmt.Errorf("kafka topic %s: missing metadata", tc.Topic)
		}
		if live.Error != nil {
			return fmt.Errorf("kafka topic %s: metadata: %w", tc.Topic, live.Error)
		}
		if len(live.Partitions) != tc.NumPartitions {
			return fmt.Errorf("kafka topic %s: partition count is %d, desired %d; operational change required", tc.Topic, len(live.Partitions), tc.NumPartitions)
		}
		for _, partition := range live.Partitions {
			if partition.Error != nil {
				return fmt.Errorf("kafka topic %s partition metadata: %w", tc.Topic, partition.Error)
			}
			if len(partition.Replicas) != tc.ReplicationFactor {
				return fmt.Errorf("kafka topic %s: replication factor is %d, desired %d; operational change required", tc.Topic, len(partition.Replicas), tc.ReplicationFactor)
			}
		}

		if len(tc.ConfigEntries) == 0 {
			continue
		}
		entryNames := make([]string, 0, len(tc.ConfigEntries))
		for _, entry := range tc.ConfigEntries {
			entryNames = append(entryNames, entry.ConfigName)
		}
		desc, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
			Resources: []kafka.DescribeConfigRequestResource{{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: tc.Topic,
				ConfigNames:  entryNames,
			}},
		})
		if err != nil {
			return err
		}
		if len(desc.Resources) == 0 {
			return fmt.Errorf("kafka topic %s: missing config response", tc.Topic)
		}
		resource := desc.Resources[0]
		if resource.Error != nil {
			return fmt.Errorf("kafka topic %s: describe configs: %w", tc.Topic, resource.Error)
		}
		drift := driftedConfigEntries(resource.ConfigEntries, tc.ConfigEntries)
		if len(drift) == 0 {
			continue
		}
		configs := make([]kafka.IncrementalAlterConfigsRequestConfig, 0, len(drift))
		for _, entry := range drift {
			configs = append(configs, kafka.IncrementalAlterConfigsRequestConfig{Name: entry.ConfigName, Value: entry.ConfigValue, ConfigOperation: kafka.ConfigOperationSet})
		}
		// AlterConfigs replaces ALL topic overrides, including operator-managed
		// settings. Incremental SET preserves every unrelated override.
		alter, err := client.IncrementalAlterConfigs(ctx, &kafka.IncrementalAlterConfigsRequest{
			Resources: []kafka.IncrementalAlterConfigsRequestResource{{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: tc.Topic,
				Configs:      configs,
			}},
		})
		if err != nil {
			return err
		}
		if len(alter.Resources) != 1 {
			return fmt.Errorf("kafka topic %s: missing incremental config response", tc.Topic)
		}
		if failure := alter.Resources[0].Error; failure != nil {
			return fmt.Errorf("kafka topic %s: incremental alter configs: %w", tc.Topic, failure)
		}
		for _, entry := range drift {
			log.Printf("kafka topic %s: reconciled %s=%s", tc.Topic, entry.ConfigName, entry.ConfigValue)
		}
	}
	return nil
}

// driftedConfigEntries returns the desired entries whose live value differs or
// is absent — exactly the set IncrementalAlterConfigs must apply. Pure so the comparison
// rules are unit-testable without a broker.
func driftedConfigEntries(live []kafka.DescribeConfigResponseConfigEntry, desired []kafka.ConfigEntry) []kafka.ConfigEntry {
	values := make(map[string]string, len(live))
	for _, entry := range live {
		values[entry.ConfigName] = entry.ConfigValue
	}
	var drift []kafka.ConfigEntry
	for _, entry := range desired {
		// max.message.bytes is a capacity floor, never a request to shrink.
		if entry.ConfigName == "max.message.bytes" {
			current, liveErr := strconv.Atoi(values[entry.ConfigName])
			minimum, desiredErr := strconv.Atoi(entry.ConfigValue)
			if liveErr == nil && desiredErr == nil && current >= minimum {
				continue
			}
		}
		if current, ok := values[entry.ConfigName]; !ok || current != entry.ConfigValue {
			drift = append(drift, entry)
		}
	}
	return drift
}

// Share the attempt deadline between bootstrap addresses and reserve time for
// controller operations. One unresponsive address cannot consume every retry.
func discoverKafkaController(ctx context.Context, brokers []string) (kafka.Broker, error) {
	var failures []error
	for i, address := range brokers {
		if err := ctx.Err(); err != nil {
			return kafka.Broker{}, err
		}
		timeout := time.Second
		if deadline, ok := ctx.Deadline(); ok {
			timeout = time.Until(deadline) / time.Duration(len(brokers)-i+1)
		}
		probe, cancel := context.WithTimeout(ctx, timeout)
		conn, closeConn, err := dialKafkaSetup(probe, address)
		if err == nil {
			controller, lookupErr := conn.Controller()
			closeConn()
			cancel()
			if lookupErr == nil {
				return controller, nil
			}
			err = lookupErr
		} else {
			cancel()
		}
		failures = append(failures, fmt.Errorf("bootstrap %s: %w", address, err))
	}
	return kafka.Broker{}, errors.Join(failures...)
}
