package platform

import (
	"reflect"
	"testing"

	"iot/internal/contracts"
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
