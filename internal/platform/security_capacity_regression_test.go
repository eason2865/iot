package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	"iot/internal/contracts"
)

func TestDevicesCannotUseServiceClientIDs(t *testing.T) {
	h := MQTTAuthenticationHandler(exactDeviceAuthenticator{}, "internal", "callback")
	for _, id := range []string{"iot-device-worker-test", "iot-telemetry-ingestor-test", "iot-demo-t-1", "iot-device-worker", "iot-telemetry-ingestor"} {
		raw, _ := json.Marshal(MQTTAuthRequest{Username: "factory:line1:d", Password: "secret", ClientID: id})
		req := httptest.NewRequest("POST", "/authenticate", bytes.NewReader(raw))
		req.Header.Set(AuthCallbackTokenHeader, "callback")
		rec := httptest.NewRecorder()
		h(rec, req)
		var result MQTTAuthResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.Result != "deny" {
			t.Errorf("device accepted reserved client ID %q", id)
		}
	}
}

func TestTelemetryExpansionIsRejectedBeforePersistence(t *testing.T) {
	s := isolatedPostgresStore(t)
	raw := json.RawMessage(`{"values":[` + strings.TrimSuffix(strings.Repeat("1e131071,", 40), ",") + `]}`)
	_, err := s.RecordTelemetryContext(t.Context(), contracts.Envelope{MsgID: "expanded", TenantID: "t", DeviceID: "d", Ts: time.Now().UnixMilli(), Type: "temp", Version: "v1", Payload: raw})
	if err == nil {
		t.Fatal("multi-megabyte JSONB telemetry accepted")
	}
	var n int
	if err = s.db.QueryRow(`SELECT count(*) FROM telemetry_records`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("rejected telemetry persisted: %d %v", n, err)
	}
}

func TestPaginationHasByteBudgetAndNoSkippedCommands(t *testing.T) {
	s := isolatedPostgresStore(t)
	for i := 0; i < 6; i++ {
		if _, err := s.CreateCommand("t", "d", json.RawMessage(`{"x":"`+strings.Repeat("x", 900000)+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	cursor := ""
	for page := 0; page < 10; page++ {
		rows, next, err := s.ListCommandsPageContext(t.Context(), PageRequest{TenantID: "t", Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		size := 0
		for _, c := range rows {
			size += len(c.Payload) + 256
			if seen[c.ID] {
				t.Fatal("duplicate page item")
			}
			seen[c.ID] = true
		}
		if size > 2<<20 {
			t.Fatalf("page exceeds byte budget: %d", size)
		}
		if next == "" {
			break
		}
		if len(rows) == 0 || next == cursor {
			t.Fatal("cursor failed to advance")
		}
		cursor = next
	}
	if len(seen) != 6 {
		t.Fatalf("pagination lost records: %d", len(seen))
	}
}

func TestLegacyMigrationRejectsCorruptedBcryptEncoding(t *testing.T) {
	s := isolatedPostgresStore(t)
	hash, err := HashDeviceSecret("test-secret")
	if err != nil {
		t.Fatal(err)
	}
	damaged := hash[:7] + "!" + hash[8:]
	if _, err = bcrypt.Cost([]byte(damaged)); err != nil {
		t.Fatal("fixture must pass Cost")
	}
	if _, err = s.db.Exec(`ALTER TABLE devices ADD COLUMN secret TEXT`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE devices SET secret='retained-plaintext',secret_hash=$1`, damaged); err != nil {
		t.Fatal(err)
	}
	if err = s.MigrateSchema(t.Context()); err == nil {
		t.Fatal("migration accepted invalid salt encoding")
	}
	var secret, stored string
	if err = s.db.QueryRow(`SELECT secret,secret_hash FROM devices WHERE tenant_id='t' AND device_id='d'`).Scan(&secret, &stored); err != nil || secret != "retained-plaintext" || stored != damaged {
		t.Fatal("failed migration did not preserve credentials")
	}
}

// A pending publish must not prevent the independent expiry loop from running.
type independentScanStore struct {
	stubDispatchStore
	scans chan struct{}
}

func (s *independentScanStore) ExpireCommandsContext(context.Context, time.Time, time.Duration) (int64, error) {
	select {
	case s.scans <- struct{}{}:
	default:
	}
	return 0, nil
}
func TestTimeoutScanContinuesDuringSlowPublish(t *testing.T) {
	s := &independentScanStore{stubDispatchStore: stubDispatchStore{commands: []Command{{ID: "slow", DispatchAttempts: 1}}}, scans: make(chan struct{}, 32)}
	entered := make(chan struct{})
	release := make(chan struct{})
	p := dispatchPublisher{publish: func(ctx context.Context, _ Command) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	d := NewCommandDispatcher(s, p, time.Minute)
	d.pollEvery = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()
	defer func() { cancel(); close(release); <-done }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("publish not reached")
	}
	// Drain initial scan and require a later scan while the publish is still blocked.
	for len(s.scans) > 0 {
		<-s.scans
	}
	select {
	case <-s.scans:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("expiry stalled behind pending publish")
	}
}

func TestExpiryCommitsWhileRealPostgresPublisherIsBlocked(t *testing.T) {
	s := isolatedPostgresStore(t)
	exp, err := s.CreateCommand("t", "d", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.MarkCommandSent(exp.ID, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateCommand("t", "d", nil); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	p := dispatchPublisher{publish: func(ctx context.Context, _ Command) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	d := NewCommandDispatcher(s, p, time.Minute)
	d.pollEvery = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()
	defer func() { cancel(); close(release); <-done }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("publish not reached")
	}
	if _, err = s.db.Exec(`UPDATE commands SET deadline_at=NOW()-INTERVAL '1 second' WHERE id=$1`, exp.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c, err := s.GetCommandContext(t.Context(), exp.ID)
		if err != nil {
			t.Fatal(err)
		}
		if c.Status == CommandStatusTimeout {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expired command remained sent while publisher was blocked")
}

func TestOversizedHistoricalRowFailsExplicitly(t *testing.T) {
	s := isolatedPostgresStore(t)
	raw := `{"v":"` + strings.Repeat("x", 5<<20) + `"}`
	if _, err := s.db.Exec(`INSERT INTO telemetry_records(msg_id,tenant_id,device_id,ts,type,version,payload,received_at) VALUES('historical','t','d',1,'temp','v1',$1,NOW())`, raw); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ListTelemetryPageContext(t.Context(), PageRequest{TenantID: "t", DeviceID: "d", Size: 1}); !errors.Is(err, ErrListItemTooLarge) {
		t.Fatalf("oversized history did not fail explicitly: %v", err)
	}
}

func TestBcryptEncodingChecksSaltAndChecksum(t *testing.T) {
	valid, err := HashDeviceSecret("secret")
	if err != nil {
		t.Fatal(err)
	}
	if !validBcryptEncoding(valid) {
		t.Fatal("valid encoding rejected")
	}
	for _, bad := range []string{valid[:7] + "!" + valid[8:], valid[:29] + "!" + valid[30:], valid[:28] + "/" + valid[29:], valid[:59] + "/", valid + "x"} {
		if validBcryptEncoding(bad) {
			t.Fatal("corrupt/noncanonical bcrypt encoding accepted")
		}
	}
}
