package adminapi

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest/handler"
	"iot/internal/platform"
)

type lockedLogBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}
func (b *lockedLogBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.Buffer.String() }
func (b *lockedLogBuffer) Len() int       { b.mu.Lock(); defer b.mu.Unlock(); return b.Buffer.Len() }

func TestFailedRequestDoesNotLogCredentials(t *testing.T) {
	for _, mode := range []string{"failure", "success", "unauthorized", "panic", "slow"} {
		t.Run(mode, func(t *testing.T) { testRequestLogging(t, mode) })
	}
}

func testRequestLogging(t *testing.T, mode string) {
	var output lockedLogBuffer
	old := logx.Reset()
	logx.SetWriter(logx.NewWriter(&output))
	oldStd := log.Writer()
	log.SetOutput(&output)
	defer func() {
		log.SetOutput(oldStd)
		logx.Reset()
		if old != nil {
			logx.SetWriter(old)
		}
	}()
	endpoint := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "fictional-device-secret") || r.Header.Get("Authorization") != "Bearer fictional-admin-token" {
			t.Error("request credentials were altered before reaching handler")
		}
		switch mode {
		case "success":
			w.WriteHeader(http.StatusCreated)
		case "unauthorized":
			w.WriteHeader(http.StatusUnauthorized)
		case "panic":
			panic("fictional-panic-secret")
		case "slow":
			time.Sleep(510 * time.Millisecond)
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	if managementHTTPConf().Middlewares.Log {
		endpoint = handler.LogHandler(endpoint)
	} else {
		endpoint = platform.SafeHTTPLoggingMiddleware(endpoint)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/devices?token=fictional-query-token", strings.NewReader(`{"tenantId":"t","deviceId":"d","productId":"p","secret":"fictional-device-secret"}`))
	req.Header.Set("Authorization", "Bearer fictional-admin-token")
	req.Header.Set("Cookie", "session=fictional-cookie")
	endpoint.ServeHTTP(httptest.NewRecorder(), req)
	for _, secret := range []string{"fictional-admin-token", "fictional-device-secret", "fictional-query-token", "fictional-cookie", "fictional-panic-secret"} {
		if strings.Contains(output.String(), secret) {
			t.Errorf("request log leaked %s", secret)
		}
	}
	if output.Len() == 0 {
		t.Fatal("request failure was not logged")
	}
}
