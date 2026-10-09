package platform

import (
	"context"
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
	ReplicationFactor int
	MinInsyncReplicas int
	// RetentionMs sets retention.ms on the topics created with this config. Zero
	// or negative leaves the broker default in place, which is not necessarily
	// bounded: a broker configured with -1 never deletes. The dead-letter topic
	// uses this so an unreplayed backlog cannot grow without limit.
	RetentionMs int
}

func (c KafkaTopicConfig) normalized() KafkaTopicConfig {
	if c.ReplicationFactor <= 0 {
		c.ReplicationFactor = 1
	}
	if c.MinInsyncReplicas <= 0 {
		c.MinInsyncReplicas = 1
	}
	return c
}

// buildTopicConfigs is pure so the replication and in-sync-replica rules can be
// unit tested without a broker. CreateTopics is idempotent for an existing
// topic (kafka-go skips TopicAlreadyExists); ensureKafkaTopics follows it with
// a reconciliation pass that AlterConfigs any drifted config entries. Partition
// count and replication factor still only take effect on first creation —
// changing them on a live topic is an operational task, not something this
// service can do, so drift there is logged instead.
func buildTopicConfigs(topics []string, cfg KafkaTopicConfig) []kafka.TopicConfig {
	cfg = cfg.normalized()
	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		topicConfig := kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
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
		configs = append(configs, topicConfig)
	}
	return configs
}

func ensureKafkaTopicsBestEffort(brokers []string, cfg KafkaTopicConfig, topics ...string) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := ensureKafkaTopics(ctx, brokers, cfg, topics...)
		cancel()
		if err == nil || time.Now().After(deadline) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func ensureKafkaTopics(ctx context.Context, brokers []string, cfg KafkaTopicConfig, topics ...string) error {
	if len(brokers) == 0 || len(topics) == 0 {
		return nil
	}
	configs := buildTopicConfigs(topics, cfg)
	if len(configs) == 0 {
		return nil
	}

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	controller, err := conn.Controller()
	_ = conn.Close()
	if err != nil {
		return err
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafka.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(configs...); err != nil {
		return err
	}
	return reconcileKafkaTopics(ctx, kafka.TCP(controllerAddr), configs)
}

// reconcileKafkaTopics brings pre-existing topics (broker auto.create, or a
// previous deployment with different settings) in line with the desired
// config. Config entries are compared and corrected via AlterConfigs; drift in
// partition count or replication factor cannot be fixed safely here, so it is
// logged loudly instead of staying silent.
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
			continue
		}
		if len(live.Partitions) != tc.NumPartitions {
			log.Printf("kafka topic %s: partition count is %d, desired %d; not auto-correcting, this needs an operational change",
				tc.Topic, len(live.Partitions), tc.NumPartitions)
		}
		if len(live.Partitions) > 0 && len(live.Partitions[0].Replicas) != tc.ReplicationFactor {
			log.Printf("kafka topic %s: replication factor is %d, desired %d; not auto-correcting, this needs an operational change",
				tc.Topic, len(live.Partitions[0].Replicas), tc.ReplicationFactor)
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
			continue
		}
		resource := desc.Resources[0]
		if resource.Error != nil {
			log.Printf("kafka topic %s: describe configs failed: %v", tc.Topic, resource.Error)
			continue
		}
		drift := driftedConfigEntries(resource.ConfigEntries, tc.ConfigEntries)
		if len(drift) == 0 {
			continue
		}
		configs := make([]kafka.AlterConfigRequestConfig, 0, len(drift))
		for _, entry := range drift {
			configs = append(configs, kafka.AlterConfigRequestConfig{Name: entry.ConfigName, Value: entry.ConfigValue})
		}
		alter, err := client.AlterConfigs(ctx, &kafka.AlterConfigsRequest{
			Resources: []kafka.AlterConfigRequestResource{{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: tc.Topic,
				Configs:      configs,
			}},
		})
		if err != nil {
			return err
		}
		if failure := alter.Errors[kafka.AlterConfigsResponseResource{Type: int8(kafka.ResourceTypeTopic), Name: tc.Topic}]; failure != nil {
			log.Printf("kafka topic %s: alter configs failed: %v", tc.Topic, failure)
			continue
		}
		for _, entry := range drift {
			log.Printf("kafka topic %s: reconciled %s=%s", tc.Topic, entry.ConfigName, entry.ConfigValue)
		}
	}
	return nil
}

// driftedConfigEntries returns the desired entries whose live value differs or
// is absent — exactly the set AlterConfigs must apply. Pure so the comparison
// rules are unit-testable without a broker.
func driftedConfigEntries(live []kafka.DescribeConfigResponseConfigEntry, desired []kafka.ConfigEntry) []kafka.ConfigEntry {
	values := make(map[string]string, len(live))
	for _, entry := range live {
		values[entry.ConfigName] = entry.ConfigValue
	}
	var drift []kafka.ConfigEntry
	for _, entry := range desired {
		if current, ok := values[entry.ConfigName]; !ok || current != entry.ConfigValue {
			drift = append(drift, entry)
		}
	}
	return drift
}
