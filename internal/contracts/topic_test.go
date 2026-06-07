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
	for _, value := range []string{"", " ", "tenant/a", "tenant+a", "tenant#a"} {
		if contracts.IsValidTopicPart(value) {
			t.Fatalf("IsValidTopicPart(%q) = true, want false", value)
		}
		if _, err := contracts.BuildTelemetryTopic(value, "device-42"); err == nil {
			t.Fatalf("BuildTelemetryTopic(%q, device-42) error = nil, want error", value)
		}
	}
}
