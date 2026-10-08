package runtimeconfig

import (
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func EnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func SplitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func Int(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func Duration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		if ms, err := strconv.Atoi(v); err == nil {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return fallback
}

func ListenAddr(addrKey, portKey, fallback string) string {
	if addr := os.Getenv(addrKey); addr != "" {
		return addr
	}
	if port := os.Getenv(portKey); port != "" {
		if _, err := strconv.Atoi(port); err == nil {
			return ":" + port
		}
	}
	return fallback
}

func ListenHost(addrKey, fallback string) string {
	if addr := os.Getenv(addrKey); addr != "" {
		host, _, err := net.SplitHostPort(addr)
		if err == nil && host != "" {
			return host
		}
		if strings.HasPrefix(addr, ":") {
			return fallback
		}
	}
	return fallback
}

func ListenPort(addrKey, portKey string, fallback int) int {
	if addr := os.Getenv(addrKey); addr != "" {
		_, port, err := net.SplitHostPort(addr)
		if err == nil {
			if p, err := strconv.Atoi(port); err == nil {
				return p
			}
		}
	}
	return Int(portKey, fallback)
}

// KafkaTopicReplicationFactor is the replication factor this service asks for
// when it creates a topic. Default 1 suits the single-broker local stack; a
// production deployment must raise it together with the ISR minimum below.
func KafkaTopicReplicationFactor() int {
	return Int("KAFKA_TOPIC_REPLICATION_FACTOR", 1)
}

// KafkaTopicMinInsyncReplicas is the min.insync.replicas this service sets on
// the topics it creates. With the default of 1, a producer using acks=all is no
// more durable than acks=1: the ISR can shrink to the leader alone and writes
// still succeed there. Production must set it to at least 2.
func KafkaTopicMinInsyncReplicas() int {
	return Int("KAFKA_TOPIC_MIN_INSYNC_REPLICAS", 1)
}
