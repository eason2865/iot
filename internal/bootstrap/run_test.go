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
