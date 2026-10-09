package main

import (
	"strings"
	"testing"

	"iot/internal/platform"
)

// TestReplayGroupIDIsPerStage pins the property that makes targeted replays safe.
// With one shared group, committing any matching record moves the group position
// past every earlier record, so a later run for a different stage would never see
// them; kafka-go only applies StartOffset when the group has no committed offset.
func TestReplayGroupIDIsPerStage(t *testing.T) {
	if got := replayGroupID(""); got != "iot-dlq-replay-all" {
		t.Fatalf("replayGroupID(\"\") = %q, want the all-stages group", got)
	}

	seen := map[string]string{}
	for _, stage := range platform.DeadLetterStages() {
		group := replayGroupID(stage)
		if group == "iot-dlq-replay-all" {
			t.Fatalf("stage %q must not share the all-stages group", stage)
		}
		if !strings.Contains(group, stage) {
			t.Fatalf("group %q should name its stage %q", group, stage)
		}
		if other, ok := seen[group]; ok {
			t.Fatalf("stages %q and %q share the group %q", other, stage, group)
		}
		seen[group] = stage
	}
}
