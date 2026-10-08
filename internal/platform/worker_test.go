package platform

import (
	"fmt"
	"reflect"
	"testing"

	"iot/internal/contracts"
	"iot/internal/runtimeconfig"
)

func TestNormalizeAckTopicFiltersDefaultsToCanonicalFilter(t *testing.T) {
	got := normalizeAckTopicFilters(WorkerConfig{})
	want := []string{contracts.AckTopicFilter}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalizeAckTopicFilters() = %#v, want %#v", got, want)
	}
}

func TestNormalizeAckTopicFiltersKeepsSingleFilter(t *testing.T) {
	got := normalizeAckTopicFilters(WorkerConfig{
		AckTopicFilter: "tenant/t1/device/+/ack",
	})
	want := []string{"tenant/t1/device/+/ack"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalizeAckTopicFilters() = %#v, want %#v", got, want)
	}
}

func TestNormalizeAckTopicFiltersPrefersNonEmptyMultipleFilters(t *testing.T) {
	got := normalizeAckTopicFilters(WorkerConfig{
		AckTopicFilter:  "tenant/legacy/device/+/ack",
		AckTopicFilters: []string{"tenant/t1/device/+/ack", "", "tenant/t2/device/+/ack"},
	})
	want := []string{"tenant/t1/device/+/ack", "tenant/t2/device/+/ack"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalizeAckTopicFilters() = %#v, want %#v", got, want)
	}
}

func TestWorkerTenantAllowedDefaultsToAllTenants(t *testing.T) {
	worker := NewWorker(WorkerConfig{}, nil, nil, nil)
	if !worker.tenantAllowed("tenant-a") {
		t.Fatal("tenantAllowed() rejected tenant-a without an allowlist")
	}
}

func TestWorkerTenantAllowedRestrictsConfiguredTenants(t *testing.T) {
	worker := NewWorker(WorkerConfig{TenantIDs: []string{"tenant-a", "", "tenant-b"}}, nil, nil, nil)
	if !worker.tenantAllowed("tenant-a") {
		t.Fatal("tenantAllowed() rejected configured tenant-a")
	}
	if worker.tenantAllowed("tenant-c") {
		t.Fatal("tenantAllowed() accepted tenant-c outside the allowlist")
	}
}

// TestWorkerTenantAllowlistFromEnv pins the DEVICE_WORKER_TENANT_IDS wiring: an
// unset or blank value must not restrict anything, so deployments that predate
// the allowlist keep processing every tenant.
func TestWorkerTenantAllowlistFromEnv(t *testing.T) {
	for _, raw := range []string{"", " ", ",", " , "} {
		worker := NewWorker(WorkerConfig{TenantIDs: runtimeconfig.SplitCSV(raw)}, nil, nil, nil)
		if !worker.tenantAllowed("any-tenant") {
			t.Fatalf("SplitCSV(%q) restricted tenants; an empty allowlist must mean all tenants", raw)
		}
	}

	worker := NewWorker(WorkerConfig{TenantIDs: runtimeconfig.SplitCSV("tenant-a, tenant-b")}, nil, nil, nil)
	if !worker.tenantAllowed("tenant-b") {
		t.Fatal("tenantAllowed() rejected tenant-b from the parsed CSV allowlist")
	}
	if worker.tenantAllowed("tenant-c") {
		t.Fatal("tenantAllowed() accepted tenant-c outside the parsed CSV allowlist")
	}
}

func TestIsTelemetryDuplicate(t *testing.T) {
	if IsTelemetryDuplicate(nil) {
		t.Fatal("IsTelemetryDuplicate(nil) = true")
	}
	if IsTelemetryDuplicate(fmt.Errorf("other error")) {
		t.Fatal("IsTelemetryDuplicate() matched unrelated error")
	}
	wrapped := fmt.Errorf("record telemetry: %w", ErrDuplicateTelemetry)
	if !IsTelemetryDuplicate(wrapped) {
		t.Fatal("IsTelemetryDuplicate() missed wrapped ErrDuplicateTelemetry")
	}
}
