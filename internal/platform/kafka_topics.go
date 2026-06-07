package platform

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func ensureKafkaTopicsBestEffort(brokers []string, topics ...string) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := ensureKafkaTopics(ctx, brokers, topics...)
		cancel()
		if err == nil || time.Now().After(deadline) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func ensureKafkaTopics(ctx context.Context, brokers []string, topics ...string) error {
	if len(brokers) == 0 || len(topics) == 0 {
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

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		configs = append(configs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}
	if len(configs) == 0 {
		return nil
	}
	return controllerConn.CreateTopics(configs...)
}
