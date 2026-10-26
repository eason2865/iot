package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"iot/internal/contracts"
	"iot/internal/platform"
)

func TestAckReplayWithDMLOnlyRole(t *testing.T) {
	dsn := os.Getenv("IOT_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set IOT_POSTGRES_TEST_DSN")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*cfg)
	defer admin.Close()
	schema := fmt.Sprintf("replay_%d", time.Now().UnixNano())
	role := schema + "_dml"
	if _, err = admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE") }()
	if _, err = admin.Exec("CREATE ROLE " + role + " LOGIN PASSWORD 'regression-only'"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec("DROP OWNED BY " + role); _, _ = admin.Exec("DROP ROLE " + role) }()
	ownerURL, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := ownerURL.Query()
	query.Set("search_path", schema)
	ownerURL.RawQuery = query.Encode()
	owner, err := platform.OpenPostgresStore(ownerURL.String(), time.Minute, true)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if _, err = owner.CreateTenant(platform.Tenant{ID: "t", Name: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err = owner.CreateDevice(platform.Device{TenantID: "t", DeviceID: "d", ProductID: "p", Secret: "s"}); err != nil {
		t.Fatal(err)
	}
	command, err := owner.CreateCommand("t", "d", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"GRANT USAGE ON SCHEMA " + schema + " TO " + role, "GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA " + schema + " TO " + role, "GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA " + schema + " TO " + role} {
		if _, err = admin.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	ownerURL.User = url.UserPassword(role, "regression-only")
	businessDSN := ownerURL.String()
	t.Setenv("POSTGRES_AUTO_MIGRATE", "true")
	if store, err := openAckReplayStore(businessDSN); err == nil {
		store.Close()
		t.Fatal("DML-only role unexpectedly allowed migration")
	}
	t.Setenv("POSTGRES_AUTO_MIGRATE", "false")
	store, err := openAckReplayStore(businessDSN)
	if err != nil {
		t.Fatalf("POSTGRES_AUTO_MIGRATE=false still attempted DDL: %v", err)
	}
	defer store.Close()
	item := platform.DeadLetter{SourceTopic: "tenant/t/device/d/ack", Stage: platform.StageCommandAck}
	payload := []byte(fmt.Sprintf(`{"status":"acked","commandId":%q,"tenantId":"t","deviceId":"d"}`, command.ID))
	for i := 0; i < 2; i++ {
		if err = replayAck(t.Context(), item, payload, store); err != nil {
			t.Fatal(err)
		}
	}
	failedCommand, err := owner.CreateCommand("t", "d", nil)
	if err != nil {
		t.Fatal(err)
	}
	failedPayload := []byte(fmt.Sprintf(`{"status":"failed","commandId":%q,"tenantId":"t","deviceId":"d","errorCode":"BLOCKED","payload":{"reason":"jammed"}}`, failedCommand.ID))
	for i := 0; i < 2; i++ {
		if err = replayAck(t.Context(), item, failedPayload, store); err != nil {
			t.Fatal(err)
		}
	}
	recovered, ok := owner.GetCommand(failedCommand.ID)
	if !ok || recovered.Status != contracts.CommandStatusFailed || recovered.Result == nil || recovered.Result.ErrorCode != "BLOCKED" {
		t.Fatalf("failed ACK replay lost result: %+v", recovered)
	}
	got, ok := owner.GetCommand(command.ID)
	if !ok || got.Status != contracts.CommandStatusAcked {
		t.Fatalf("ACK not recovered: %+v", got)
	}
	businessCfg, err := pgx.ParseConfig(businessDSN)
	if err != nil {
		t.Fatal(err)
	}
	check := stdlib.OpenDB(*businessCfg)
	defer check.Close()
	if _, err = check.Exec("CREATE TABLE " + schema + ".forbidden (id int)"); err == nil {
		t.Fatal("test role has DDL permission")
	}
	var count int
	if err = check.QueryRow("SELECT COUNT(*) FROM "+schema+".command_events WHERE event_type='acked' AND command_id=$1", command.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("audit count=%d err=%v", count, err)
	}
}

func TestAckReplayRejectsInvalidAutoMigrationFlag(t *testing.T) {
	t.Setenv("POSTGRES_AUTO_MIGRATE", "invalid")
	_, err := openAckReplayStore("postgres://unused:unused@127.0.0.1:1/iot?sslmode=disable")
	if err == nil || !strings.Contains(err.Error(), "POSTGRES_AUTO_MIGRATE") {
		t.Fatalf("invalid flag not rejected before connecting: %v", err)
	}
}
