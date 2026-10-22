package contracts_test

import (
	"testing"

	"iot/internal/contracts"
)

func TestBuildTelemetryTopic(t *testing.T) {
	got, err := contracts.BuildTelemetryTopic("tenant-a", "device-42")
	if err != nil {
		t.Fatalf("BuildTelemetryTopic() error = %v", err)
	}

	want := "tenant/tenant-a/device/device-42/telemetry"
	if got != want {
		t.Fatalf("BuildTelemetryTopic() = %q, want %q", got, want)
	}
}

func TestBuildCommandTopic(t *testing.T) {
	got, err := contracts.BuildCommandTopic("tenant-a", "device-42")
	if err != nil {
		t.Fatalf("BuildCommandTopic() error = %v", err)
	}

	want := "tenant/tenant-a/device/device-42/command"
	if got != want {
		t.Fatalf("BuildCommandTopic() = %q, want %q", got, want)
	}
}

func TestBuildAckTopic(t *testing.T) {
	got, err := contracts.BuildAckTopic("tenant-a", "device-42")
	if err != nil {
		t.Fatalf("BuildAckTopic() error = %v", err)
	}

	want := "tenant/tenant-a/device/device-42/ack"
	if got != want {
		t.Fatalf("BuildAckTopic() = %q, want %q", got, want)
	}
}

func TestTopicFilters(t *testing.T) {
	if contracts.TelemetryTopicFilter != "tenant/+/device/+/telemetry" {
		t.Fatalf("TelemetryTopicFilter = %q", contracts.TelemetryTopicFilter)
	}
	if contracts.AckTopicFilter != "tenant/+/device/+/ack" {
		t.Fatalf("AckTopicFilter = %q", contracts.AckTopicFilter)
	}

	got, err := contracts.BuildTenantCommandTopicFilter("tenant-a")
	if err != nil {
		t.Fatalf("BuildTenantCommandTopicFilter() error = %v", err)
	}
	want := "tenant/tenant-a/device/+/command"
	if got != want {
		t.Fatalf("BuildTenantCommandTopicFilter() = %q, want %q", got, want)
	}
}

func TestTopicPartValidationRejectsMQTTWildcardsAndSeparators(t *testing.T) {
	for _, value := range []string{"", " ", " tenant-a", "tenant-a ", "tenant/a", "tenant+a", "tenant#a", "tenant\x00a", "tenant\na", string([]byte{0xff})} {
		if contracts.IsValidTopicPart(value) {
			t.Fatalf("IsValidTopicPart(%q) = true, want false", value)
		}
		if _, err := contracts.BuildTelemetryTopic(value, "device-42"); err == nil {
			t.Fatalf("BuildTelemetryTopic(%q, device-42) error = nil, want error", value)
		}
	}
}

func TestTopicBuildersPreserveCanonicalIdentity(t *testing.T) {
	for _, value := range []string{" tenant-a", "tenant-a "} {
		if _, err := contracts.BuildCommandTopic("t", value); err == nil {
			t.Errorf("device %q was silently normalized", value)
		}
		if _, err := contracts.BuildTenantCommandTopicFilter(value); err == nil {
			t.Errorf("tenant %q was silently normalized", value)
		}
	}
	for _, topic := range []string{"/tenant/t/device/d/telemetry", "tenant/t/device/d/telemetry/", "tenant/ t/device/d/telemetry"} {
		if _, _, _, ok := contracts.ParseDeviceTopic(topic); ok {
			t.Errorf("noncanonical topic accepted: %q", topic)
		}
	}
}

func TestTenantIDAndDeviceUsernameHaveUnambiguousEncoding(t *testing.T) {
	if contracts.IsValidTenantID("factory:line1") {
		t.Fatal("tenant contains reserved username separator")
	}
	if _, err := contracts.BuildDeviceUsername("factory:line1", "d"); err == nil {
		t.Fatal("ambiguous username accepted")
	}
	if _, err := contracts.BuildTelemetryTopic("factory:line1", "d"); err == nil {
		t.Fatal("ambiguous topic identity accepted")
	}
	if _, _, _, ok := contracts.ParseDeviceTopic("tenant/factory:line1/device/d/ack"); ok {
		t.Fatal("ambiguous tenant parsed")
	}
	for _, device := range []string{"d", "line1:d"} {
		username, err := contracts.BuildDeviceUsername("factory", device)
		if err != nil {
			t.Fatal(err)
		}
		tenant, parsed, ok := contracts.ParseDeviceUsername(username)
		if !ok || tenant != "factory" || parsed != device {
			t.Fatalf("username roundtrip: %s -> %s/%s", username, tenant, parsed)
		}
	}
	for _, username := range []string{"factory", ":d", "factory:", " factory:d", "factory:d/1"} {
		if _, _, ok := contracts.ParseDeviceUsername(username); ok {
			t.Errorf("invalid username accepted: %q", username)
		}
	}
}
