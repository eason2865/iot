package contracts_test

import (
	"testing"

	"iot/internal/contracts"
)

func TestBuildDeviceTopic(t *testing.T) {
	got, err := contracts.BuildDeviceTopic("tenant-a", "device-42", "up")
	if err != nil {
		t.Fatalf("BuildDeviceTopic() error = %v", err)
	}

	want := "tenant/tenant-a/device/device-42/up"
	if got != want {
		t.Fatalf("BuildDeviceTopic() = %q, want %q", got, want)
	}
}
