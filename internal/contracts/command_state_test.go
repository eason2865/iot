package contracts_test

import (
	"testing"

	"iot/internal/contracts"
)

func TestAdvanceCommandStatus(t *testing.T) {
	got, err := contracts.AdvanceCommandStatus(contracts.CommandStatusCreated, contracts.CommandEventPublished)
	if err != nil {
		t.Fatalf("AdvanceCommandStatus(created, published) error = %v", err)
	}
	if got != contracts.CommandStatusSent {
		t.Fatalf("AdvanceCommandStatus(created, published) = %q, want %q", got, contracts.CommandStatusSent)
	}

	got, err = contracts.AdvanceCommandStatus(got, contracts.CommandEventAcked)
	if err != nil {
		t.Fatalf("AdvanceCommandStatus(sent, acked) error = %v", err)
	}
	if got != contracts.CommandStatusAcked {
		t.Fatalf("AdvanceCommandStatus(sent, acked) = %q, want %q", got, contracts.CommandStatusAcked)
	}
}
