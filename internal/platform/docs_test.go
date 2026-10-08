package platform_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
)

func TestDocsEndpoints(t *testing.T) {
	app := newBusinessAPIApp(t, nil)
	ts := httptest.NewServer(app.Router())
	defer ts.Close()

	// The served contract must be the published document on disk: docs/*.json is
	// the single hand-maintained source and is embedded at build time, so this
	// catches a handler that returns anything other than that file.
	checkJSONEndpoint(t, ts.URL+"/openapi.json", "openapi", "../../docs/openapi.json")
	checkJSONEndpoint(t, ts.URL+"/schemas/mqtt-envelope.json", "title", "../../docs/mqtt-envelope.schema.json")
}

func checkJSONEndpoint(t *testing.T, url, requiredKey, publishedPath string) {
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

	raw, err := os.ReadFile(publishedPath)
	if err != nil {
		t.Fatalf("read %s: %v", publishedPath, err)
	}
	var published map[string]any
	if err := json.Unmarshal(raw, &published); err != nil {
		t.Fatalf("%s is not valid JSON: %v", publishedPath, err)
	}
	if !reflect.DeepEqual(got, published) {
		t.Fatalf("GET %s does not match %s", url, publishedPath)
	}
}
