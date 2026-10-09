package platform_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// appMetricPattern finds the application metric names inside a PromQL
// expression. Only iot_* names are matched: every other token in these
// expressions is a PromQL function, a label value, or a metric this project does
// not register (up, go_*, process_*).
var appMetricPattern = regexp.MustCompile(`\biot_[a-z0-9_]+`)

// metricDeclPattern finds the metric names this package declares.
var metricDeclPattern = regexp.MustCompile(`Name:\s*"(iot_[a-z0-9_]+)"`)

// TestMonitoringAssetsReferenceRealMetrics guards the Grafana files against drift
// from the code. A typo in a dashboard expression yields a permanently empty
// panel, and a typo in an alert expression yields a rule that can never fire.
// Both fail silently, which is the worst way for observability to break: the
// reason you look at the panel is that something is already wrong.
func TestMonitoringAssetsReferenceRealMetrics(t *testing.T) {
	exported := declaredAppMetrics(t)
	if len(exported) == 0 {
		t.Fatal("no iot_* metric declarations found")
	}

	for _, file := range monitoringFiles(t) {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, name := range appMetricPattern.FindAllString(string(raw), -1) {
			if !metricExists(name, exported) {
				t.Errorf("%s references %q, which no service registers", filepath.Base(file), name)
			}
		}
	}
}

// TestAlertPanelLinksPointAtRealPanels catches an alert annotation pointing at a
// dashboard or panel that does not exist, which silently degrades the notification
// link an operator is supposed to follow.
func TestAlertPanelLinksPointAtRealPanels(t *testing.T) {
	// dashboard uid -> set of panel ids
	panels := map[string]map[string]struct{}{}
	for _, file := range glob(t, "dashboards", "*.json") {
		var dashboard struct {
			UID    string `json:"uid"`
			Panels []struct {
				ID int `json:"id"`
			} `json:"panels"`
		}
		readJSON(t, file, &dashboard)
		if dashboard.UID == "" {
			t.Fatalf("%s has no uid", filepath.Base(file))
		}
		ids := map[string]struct{}{}
		for _, panel := range dashboard.Panels {
			ids[strconv.Itoa(panel.ID)] = struct{}{}
		}
		panels[dashboard.UID] = ids
	}

	for _, file := range glob(t, "alerts", "*.json") {
		var rule struct {
			UID         string `json:"uid"`
			Title       string `json:"title"`
			Condition   string `json:"condition"`
			Data        []any  `json:"data"`
			Annotations struct {
				DashboardUID string `json:"__dashboardUid__"`
				PanelID      string `json:"__panelId__"`
			} `json:"annotations"`
		}
		readJSON(t, file, &rule)

		if rule.UID == "" || rule.Title == "" || rule.Condition == "" || len(rule.Data) == 0 {
			t.Errorf("%s is missing a required field (uid/title/condition/data)", filepath.Base(file))
		}
		if rule.Annotations.DashboardUID == "" {
			continue
		}
		ids, ok := panels[rule.Annotations.DashboardUID]
		if !ok {
			t.Errorf("%s links to dashboard %q, which does not exist", filepath.Base(file), rule.Annotations.DashboardUID)
			continue
		}
		if rule.Annotations.PanelID == "" {
			continue
		}
		if _, ok := ids[rule.Annotations.PanelID]; !ok {
			t.Errorf("%s links to panel %s of dashboard %q, which does not exist",
				filepath.Base(file), rule.Annotations.PanelID, rule.Annotations.DashboardUID)
		}
	}
}

// TestDashboardsParse keeps a malformed dashboard from reaching Grafana, where a
// provisioned file that fails to parse simply does not appear.
func TestDashboardsParse(t *testing.T) {
	for _, file := range glob(t, "dashboards", "*.json") {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		var dashboard map[string]any
		if err := json.Unmarshal(raw, &dashboard); err != nil {
			t.Errorf("%s is not valid JSON: %v", filepath.Base(file), err)
			continue
		}
		for _, key := range []string{"uid", "title", "panels", "schemaVersion"} {
			if _, ok := dashboard[key]; !ok {
				t.Errorf("%s is missing %q", filepath.Base(file), key)
			}
		}
	}
}

// declaredAppMetrics reads the metric names this package declares, from the
// source rather than from Registry().Gather().
//
// Gather only reports families that have at least one series, so a lazily created
// vector is invisible until it is first observed — iot_http_requests_total is
// registered and exported at runtime, but absent from Gather until the first HTTP
// request. Using Gather here would report a false mismatch for every metric
// seedSeries does not pre-create.
func declaredAppMetrics(t *testing.T) map[string]struct{} {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob package sources: %v", err)
	}
	names := map[string]struct{}{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, match := range metricDeclPattern.FindAllStringSubmatch(string(raw), -1) {
			names[match[1]] = struct{}{}
		}
	}
	return names
}

// metricExists accounts for histograms: a dashboard queries
// <name>_bucket / _sum / _count while the registry family is named <name>.
func metricExists(referenced string, exported map[string]struct{}) bool {
	if _, ok := exported[referenced]; ok {
		return true
	}
	for _, suffix := range []string{"_bucket", "_sum", "_count"} {
		if base, found := strings.CutSuffix(referenced, suffix); found {
			if _, ok := exported[base]; ok {
				return true
			}
		}
	}
	return false
}

func monitoringFiles(t *testing.T) []string {
	t.Helper()
	return append(glob(t, "dashboards", "*.json"), glob(t, "alerts", "*.json")...)
}

func glob(t *testing.T, dir, pattern string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "monitoring", "grafana", dir, pattern))
	if err != nil {
		t.Fatalf("glob %s/%s: %v", dir, pattern, err)
	}
	if len(files) == 0 {
		t.Fatalf("no files matched monitoring/grafana/%s/%s", dir, pattern)
	}
	return files
}

func readJSON(t *testing.T, file string, dst any) {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		t.Fatalf("%s is not valid JSON: %v", filepath.Base(file), err)
	}
}
