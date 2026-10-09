package runtimeconfig

import "testing"

// TestIntHandlesValidUnparseableAndUnset covers the three cases that decide
// whether a configured value is actually in effect. The unparseable case is not
// hypothetical: Helm renders a bare large YAML integer as "6.048e+08", and
// silently keeping the default is how a "configured" retention policy would ship
// without ever being applied.
func TestIntHandlesValidUnparseableAndUnset(t *testing.T) {
	t.Setenv("IOT_TEST_INT", "604800000")
	if got := Int("IOT_TEST_INT", 1); got != 604800000 {
		t.Fatalf("Int() = %d, want 604800000", got)
	}

	t.Setenv("IOT_TEST_INT", "6.048e+08")
	if got := Int("IOT_TEST_INT", 7); got != 7 {
		t.Fatalf("Int() on a float-rendered value = %d, want the fallback 7", got)
	}

	t.Setenv("IOT_TEST_INT", "not-a-number")
	if got := Int("IOT_TEST_INT", 9); got != 9 {
		t.Fatalf("Int() on garbage = %d, want the fallback 9", got)
	}

	t.Setenv("IOT_TEST_INT", "")
	if got := Int("IOT_TEST_INT", 3); got != 3 {
		t.Fatalf("Int() on an unset variable = %d, want the fallback 3", got)
	}
}

func TestKafkaTopicDurabilityDefaults(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_REPLICATION_FACTOR", "")
	t.Setenv("KAFKA_TOPIC_MIN_INSYNC_REPLICAS", "")
	t.Setenv("KAFKA_DLQ_RETENTION_MS", "")

	if got := KafkaTopicReplicationFactor(); got != 1 {
		t.Fatalf("KafkaTopicReplicationFactor() = %d, want the single-broker default 1", got)
	}
	if got := KafkaTopicMinInsyncReplicas(); got != 1 {
		t.Fatalf("KafkaTopicMinInsyncReplicas() = %d, want 1", got)
	}
	if got := KafkaDLQRetentionMs(); got != 7*24*60*60*1000 {
		t.Fatalf("KafkaDLQRetentionMs() = %d, want 7 days", got)
	}

	t.Setenv("KAFKA_TOPIC_REPLICATION_FACTOR", "3")
	t.Setenv("KAFKA_TOPIC_MIN_INSYNC_REPLICAS", "2")
	t.Setenv("KAFKA_DLQ_RETENTION_MS", "3600000")

	if got := KafkaTopicReplicationFactor(); got != 3 {
		t.Fatalf("KafkaTopicReplicationFactor() = %d, want 3", got)
	}
	if got := KafkaTopicMinInsyncReplicas(); got != 2 {
		t.Fatalf("KafkaTopicMinInsyncReplicas() = %d, want 2", got)
	}
	if got := KafkaDLQRetentionMs(); got != 3600000 {
		t.Fatalf("KafkaDLQRetentionMs() = %d, want 3600000", got)
	}
}
