package platform

import "testing"

// TestBuildTopicConfigs pins how replication factor and min.insync.replicas are
// applied when this service creates a missing topic. Getting this wrong makes
// acks=all a no-op: with min.insync.replicas=1 the ISR can shrink to the leader
// alone and writes still succeed there, which is exactly the silent-loss window
// RequireAll is meant to close (see docs/adr/0004).
func TestBuildTopicConfigs(t *testing.T) {
	minISR := func(cfg []configEntryView) string {
		for _, entry := range cfg {
			if entry.name == "min.insync.replicas" {
				return entry.value
			}
		}
		return ""
	}

	for _, tc := range []struct {
		name          string
		config        KafkaTopicConfig
		wantRF        int
		wantMinISR    string
		wantRetention string
		wantConfigs   int
	}{
		{
			name:        "defaults stay single-broker friendly",
			config:      KafkaTopicConfig{},
			wantRF:      1,
			wantMinISR:  "1",
			wantConfigs: 1,
		},
		{
			name:        "production settings are applied",
			config:      KafkaTopicConfig{ReplicationFactor: 3, MinInsyncReplicas: 2},
			wantRF:      3,
			wantMinISR:  "2",
			wantConfigs: 1,
		},
		{
			name: "min.insync.replicas above the replication factor is dropped",
			// The broker rejects this combination, so the topic must be created
			// without the entry rather than failing creation or silently
			// pretending the durability setting was applied.
			config:      KafkaTopicConfig{ReplicationFactor: 1, MinInsyncReplicas: 2},
			wantRF:      1,
			wantMinISR:  "",
			wantConfigs: 1,
		},
		{
			name: "the dead-letter retention policy is applied",
			// The DLQ is the only topic with a bounded retention: an unreplayed
			// backlog must not grow forever.
			config:        KafkaTopicConfig{ReplicationFactor: 1, MinInsyncReplicas: 1, RetentionMs: 604800000},
			wantRF:        1,
			wantMinISR:    "1",
			wantRetention: "604800000",
			wantConfigs:   1,
		},
		{
			name:        "blank topic names are skipped",
			config:      KafkaTopicConfig{ReplicationFactor: 2, MinInsyncReplicas: 2},
			wantRF:      2,
			wantMinISR:  "2",
			wantConfigs: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topics := []string{"iot.telemetry"}
			if tc.name == "blank topic names are skipped" {
				topics = []string{"", "iot.telemetry"}
			}
			configs := buildTopicConfigs(topics, tc.config)
			if len(configs) != tc.wantConfigs {
				t.Fatalf("buildTopicConfigs() returned %d configs, want %d", len(configs), tc.wantConfigs)
			}
			got := configs[0]
			if got.Topic != "iot.telemetry" {
				t.Fatalf("topic = %q", got.Topic)
			}
			if got.ReplicationFactor != tc.wantRF {
				t.Fatalf("replicationFactor = %d, want %d", got.ReplicationFactor, tc.wantRF)
			}
			if got.NumPartitions != 1 {
				t.Fatalf("numPartitions = %d, want 1", got.NumPartitions)
			}
			views := make([]configEntryView, 0, len(got.ConfigEntries))
			for _, entry := range got.ConfigEntries {
				views = append(views, configEntryView{name: entry.ConfigName, value: entry.ConfigValue})
			}
			if isr := minISR(views); isr != tc.wantMinISR {
				t.Fatalf("min.insync.replicas = %q, want %q", isr, tc.wantMinISR)
			}
			gotRetention := ""
			for _, view := range views {
				if view.name == "retention.ms" {
					gotRetention = view.value
				}
			}
			if gotRetention != tc.wantRetention {
				t.Fatalf("retention.ms = %q, want %q", gotRetention, tc.wantRetention)
			}
		})
	}
}

type configEntryView struct {
	name  string
	value string
}
