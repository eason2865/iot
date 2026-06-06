package platform_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"iot/internal/platform"
)

func TestDocsEndpoints(t *testing.T) {
	app := platform.New(platform.Config{ServiceName: "admin"})
	ts := httptest.NewServer(app.Router())
	defer ts.Close()

	checkJSONEndpoint(t, ts.URL+"/openapi.json", "openapi")
	checkJSONEndpoint(t, ts.URL+"/schemas/mqtt-envelope.json", "title")
}

func checkJSONEndpoint(t *testing.T, url, requiredKey string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("http.Get() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if _, ok := got[requiredKey]; !ok {
		t.Fatalf("response missing key %q", requiredKey)
	}
}
