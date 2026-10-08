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
// unit tested without a broker. Note that CreateTopics is idempotent for an
// existing topic (kafka-go skips TopicAlreadyExists), so these settings only
// take effect on first creation: changing the replication factor or
// min.insync.replicas of a live topic is an operational task, not something this
// service can do.
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

	return controllerConn.CreateTopics(configs...)
}
