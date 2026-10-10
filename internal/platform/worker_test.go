package platform

import (
	"context"
	"encoding/json"
	"errors"
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

func TestCommandAlreadyDelivered(t *testing.T) {
	for _, status := range []contracts.CommandStatus{
		contracts.CommandStatusSent,
		contracts.CommandStatusAcked,
		contracts.CommandStatusTimeout,
		contracts.CommandStatusFailed,
	} {
		if !commandAlreadyDelivered(status) {
			t.Fatalf("commandAlreadyDelivered(%q) = false, want true", status)
		}
	}
	for _, status := range []contracts.CommandStatus{
		contracts.CommandStatusCreated,
		contracts.CommandStatusPublished,
	} {
		if commandAlreadyDelivered(status) {
			t.Fatalf("commandAlreadyDelivered(%q) = true, want false: a %q command still needs the downlink", status, status)
		}
	}
}

func TestTenantSetHashIsOrderIndependentAndStable(t *testing.T) {
	a := tenantSetHash([]string{"tenant-b", "tenant-a", "tenant-a", ""})
	b := tenantSetHash([]string{"tenant-a", "tenant-b"})
	if a != b {
		t.Fatalf("tenantSetHash differs for the same set: %q vs %q", a, b)
	}
	if tenantSetHash([]string{"tenant-a"}) == tenantSetHash([]string{"tenant-c"}) {
		t.Fatal("tenantSetHash collided for distinct sets")
	}
}

// flakyAckStore injects transient AckCommand failures to exercise the worker's
// retry-and-dead-letter path.
type flakyAckStore struct {
	*memoryStore
	failures int
	calls    int
}

func (s *flakyAckStore) AckCommand(id, tenantID, deviceID string) (Command, error) {
	s.calls++
	if s.calls <= s.failures {
		return Command{}, fmt.Errorf("transient store error")
	}
	return s.memoryStore.AckCommand(id, tenantID, deviceID)
}

type fakeMQTTMessage struct {
	topic   string
	payload []byte
}

func (m fakeMQTTMessage) Duplicate() bool   { return false }
func (m fakeMQTTMessage) Qos() byte         { return 1 }
func (m fakeMQTTMessage) Retained() bool    { return false }
func (m fakeMQTTMessage) Topic() string     { return m.topic }
func (m fakeMQTTMessage) MessageID() uint16 { return 0 }
func (m fakeMQTTMessage) Payload() []byte   { return m.payload }
func (m fakeMQTTMessage) Ack()              {}

func mustSeedCommand(t *testing.T, store *memoryStore, tenantID, deviceID string) Command {
	t.Helper()
	if _, err := store.CreateTenant(Tenant{ID: tenantID, Name: tenantID}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	if _, err := store.CreateDevice(Device{TenantID: tenantID, DeviceID: deviceID, ProductID: "p", Secret: "s"}); err != nil {
		t.Fatalf("CreateDevice() error = %v", err)
	}
	cmd, err := store.CreateCommand(tenantID, deviceID, json.RawMessage(`{"switch":"on"}`))
	if err != nil {
		t.Fatalf("CreateCommand() error = %v", err)
	}
	return cmd
}

func TestHandleAckMessageRetriesTransientStoreError(t *testing.T) {
	base := newMemoryStore(0)
	cmd := mustSeedCommand(t, base, "tenant-a", "device-1")
	store := &flakyAckStore{memoryStore: base, failures: 1}
	worker := NewWorker(WorkerConfig{}, store, nil, nil)

	payload, _ := json.Marshal(CommandAckMessage{CommandID: cmd.ID, TenantID: "tenant-a", DeviceID: "device-1"})
	topic, err := contracts.BuildAckTopic("tenant-a", "device-1")
	if err != nil {
		t.Fatalf("BuildAckTopic() error = %v", err)
	}
	worker.handleAckMessage(nil, fakeMQTTMessage{topic: topic, payload: payload})

	if store.calls != 2 {
		t.Fatalf("AckCommand calls = %d, want 2 (one transient failure then success)", store.calls)
	}
	got, ok := base.GetCommand(cmd.ID)
	if !ok || got.Status != contracts.CommandStatusAcked {
		t.Fatalf("command status = %q, want acked", got.Status)
	}
}

func TestHandleAckMessageDoesNotRetryUnknownCommand(t *testing.T) {
	base := newMemoryStore(0)
	store := &flakyAckStore{memoryStore: base}
	worker := NewWorker(WorkerConfig{}, store, nil, nil)

	payload, _ := json.Marshal(CommandAckMessage{CommandID: "missing", TenantID: "tenant-a", DeviceID: "device-1"})
	topic, err := contracts.BuildAckTopic("tenant-a", "device-1")
	if err != nil {
		t.Fatalf("BuildAckTopic() error = %v", err)
	}
	worker.handleAckMessage(nil, fakeMQTTMessage{topic: topic, payload: payload})

	if store.calls != 1 {
		t.Fatalf("AckCommand calls = %d, want 1: a not-found command must not be retried", store.calls)
	}
}

func TestHandleAckMessageDeadLettersAfterRetriesExhausted(t *testing.T) {
	base := newMemoryStore(0)
	cmd := mustSeedCommand(t, base, "tenant-a", "device-1")
	store := &flakyAckStore{memoryStore: base, failures: ackStoreAttempts}
	// No DLQ writer configured: publishDeadLetter reports the misconfiguration,
	// and the handler must log it instead of panicking.
	worker := NewWorker(WorkerConfig{}, store, nil, nil)

	payload, _ := json.Marshal(CommandAckMessage{CommandID: cmd.ID, TenantID: "tenant-a", DeviceID: "device-1"})
	topic, err := contracts.BuildAckTopic("tenant-a", "device-1")
	if err != nil {
		t.Fatalf("BuildAckTopic() error = %v", err)
	}
	worker.handleAckMessage(nil, fakeMQTTMessage{topic: topic, payload: payload})

	if store.calls != ackStoreAttempts {
		t.Fatalf("AckCommand calls = %d, want %d", store.calls, ackStoreAttempts)
	}
	got, _ := base.GetCommand(cmd.ID)
	if got.Status == contracts.CommandStatusAcked {
		t.Fatal("command reached acked despite persistent store failures")
	}
}

type unavailableDeliveryStore struct{ *memoryStore }

func (s unavailableDeliveryStore) GetCommandForDelivery(context.Context, string) (Command, error) {
	return Command{}, errors.New("database unavailable")
}
func TestCommandDeliveryFailsClosed(t *testing.T) {
	base := newMemoryStore(0)
	cmd := mustSeedCommand(t, base, "tenant-a", "device-1")
	worker := NewWorker(WorkerConfig{}, unavailableDeliveryStore{base}, nil, nil)
	if deliver, err := worker.commandForDelivery(context.Background(), cmd); deliver || err == nil {
		t.Fatalf("unavailable database allowed publish: %t %v", deliver, err)
	}
	worker = NewWorker(WorkerConfig{}, base, nil, nil)
	foreign := cmd
	foreign.TenantID = "tenant-b"
	if deliver, err := worker.commandForDelivery(context.Background(), foreign); deliver || err != nil {
		t.Fatalf("foreign identity: %t %v", deliver, err)
	}
	if deliver, err := worker.commandForDelivery(context.Background(), Command{ID: "missing"}); deliver || err != nil {
		t.Fatalf("missing command: %t %v", deliver, err)
	}
	if deliver, err := worker.commandForDelivery(context.Background(), cmd); !deliver || err != nil {
		t.Fatalf("created command: %t %v", deliver, err)
	}
	if _, err := base.AckCommand(cmd.ID, cmd.TenantID, cmd.DeviceID); err != nil {
		t.Fatal(err)
	}
	if deliver, err := worker.commandForDelivery(context.Background(), cmd); deliver || err != nil {
		t.Fatalf("acked replay: %t %v", deliver, err)
	}
}
