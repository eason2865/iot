package bootstrap

import "testing"

func TestMQTTClientIDUsesPodSuffixWhenPresent(t *testing.T) {
	t.Setenv("EMQX_TELEMETRY_INGESTOR_CLIENT_ID", "ingestor")
	t.Setenv("EMQX_CLIENT_ID_SUFFIX", "telemetry-ingestor-abc123")

	if got, want := mqttClientID("EMQX_TELEMETRY_INGESTOR_CLIENT_ID", "fallback"), "ingestor-telemetry-ingestor-abc123"; got != want {
		t.Fatalf("mqttClientID() = %q, want %q", got, want)
	}
}

func TestMQTTClientIDKeepsBaseIDWithoutSuffix(t *testing.T) {
	t.Setenv("EMQX_CLIENT_ID_SUFFIX", "")

	if got, want := mqttClientID("EMQX_DEVICE_WORKER_CLIENT_ID", "iot-device-worker"), "iot-device-worker"; got != want {
		t.Fatalf("mqttClientID() = %q, want %q", got, want)
	}
}

func TestPublisherInitializationFailurePropagates(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("KAFKA_TOPIC_REPLICATION_FACTOR", "1")
	t.Setenv("KAFKA_TOPIC_MIN_INSYNC_REPLICAS", "2")
	p, close, err := buildPublisher(nil)
	if err == nil || p != nil || close != nil {
		t.Fatalf("startup ignored invalid durability config: %v", err)
	}
}
