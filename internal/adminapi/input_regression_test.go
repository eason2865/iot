package adminapi

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStrictRequestParsing(t *testing.T) {
	for _, raw := range []string{"10abc", "1.5", "10%20junk", "999999999999999999999999"} {
		if _, _, err := pageFromRequest(httptest.NewRequest("GET", "/api/v1/tenants?pageSize="+raw, nil)); err == nil {
			t.Errorf("accepted pageSize=%s", raw)
		}
	}
	for _, body := range []string{`{"tenantId":"t"}junk`, `{"tenantId":"t"} {"tenantId":"u"}`} {
		var dst struct {
			TenantID string `json:"tenantId"`
		}
		if err := decodeJSON(httptest.NewRequest("POST", "/", strings.NewReader(body)), &dst); err == nil {
			t.Errorf("accepted trailing body %q", body)
		}
	}
}
