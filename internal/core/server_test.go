package core

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRPCServerConfDoesNotRegisterWithEtcd(t *testing.T) {
	conf := rpcServerConf()
	if conf.HasEtcd() {
		t.Fatalf("rpc server still configures etcd: %+v", conf.Etcd)
	}
}

// TestNewAuxHTTPServerFailsOnPortConflict pins the startup property that replaced
// a fire-and-forget goroutine: the auxiliary endpoints are bound before the
// service claims to be serving. If the MQTT authentication callback cannot bind,
// fail-closed authentication rejects every device, so the process must not come
// up looking healthy.
func TestNewAuxHTTPServerFailsOnPortConflict(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer taken.Close()

	_, err = newAuxHTTPServer("mqtt-auth", taken.Addr().String(), mqttAuthMux(nil, "internal", "token"))
	if err == nil {
		t.Fatal("newAuxHTTPServer() on a taken port returned nil error")
	}
	if !strings.Contains(err.Error(), "mqtt-auth") || !strings.Contains(err.Error(), taken.Addr().String()) {
		t.Fatalf("error should name the endpoint and address, got %v", err)
	}
}

// TestAuxHTTPServerShutdownDrainsInflightRequests is the graceful part: a request
// already being served must complete, and only then may the listener close.
func TestAuxHTTPServerShutdownDrainsInflightRequests(t *testing.T) {
	started := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		close(started)
		time.Sleep(250 * time.Millisecond)
		_, _ = w.Write([]byte("drained"))
	})

	aux, err := newAuxHTTPServer("metrics", "127.0.0.1:0", mux)
	if err != nil {
		t.Fatalf("newAuxHTTPServer: %v", err)
	}
	go aux.Serve()
	addr := aux.listener.Addr().String()

	type result struct {
		body string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := http.Get("http://" + addr + "/slow")
		if err != nil {
			done <- result{err: err}
			return
		}
		defer resp.Body.Close()
		body, readErr := io.ReadAll(resp.Body)
		done <- result{body: string(body), err: readErr}
	}()

	// Shut down while the request is in flight: Shutdown waits for it.
	<-started
	shutdownAuxServers([]*auxHTTPServer{aux})

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("in-flight request failed during shutdown: %v", got.err)
		}
		if got.body != "drained" {
			t.Fatalf("in-flight response body = %q, want %q", got.body, "drained")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request never completed")
	}
}

// TestShutdownAuxServersReleasesListeners checks the other half: after a graceful
// shutdown the port is free, so a restart is not blocked by a lingering listener.
func TestShutdownAuxServersReleasesListeners(t *testing.T) {
	aux, err := newAuxHTTPServer("metrics", "127.0.0.1:0", metricsMux(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}), "/metrics"))
	if err != nil {
		t.Fatalf("newAuxHTTPServer: %v", err)
	}
	addr := aux.listener.Addr().String()
	go aux.Serve()

	// The endpoint really serves before we take it away.
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("http.Get before shutdown: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	shutdownAuxServers([]*auxHTTPServer{aux})

	reused, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listener was not released after shutdown: %v", err)
	}
	_ = reused.Close()
}

// TestCloseAuxServersReleasesListeners covers the failed-startup path: a listener
// bound before a later step failed must not be leaked.
func TestCloseAuxServersReleasesListeners(t *testing.T) {
	aux, err := newAuxHTTPServer("mqtt-auth", "127.0.0.1:0", mqttAuthMux(nil, "internal", "token"))
	if err != nil {
		t.Fatalf("newAuxHTTPServer: %v", err)
	}
	addr := aux.listener.Addr().String()

	closeAuxServers([]*auxHTTPServer{aux})

	reused, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listener was not released: %v", err)
	}
	_ = reused.Close()
}

// TestAuxMuxRoutes pins the paths the two auxiliary servers expose, since the
// handlers are now built by helpers rather than inline.
func TestAuxMuxRoutes(t *testing.T) {
	authMux := mqttAuthMux(nil, "internal-password", "callback-token")
	rec := httptest.NewRecorder()
	// A GET must be rejected by the handler as method not allowed, which proves
	// the route is wired (an unwired path answers 404).
	authMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/mqtt/authenticate", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("auth route status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}

	metricsMux := metricsMux(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("metrics-body"))
	}), "/metrics")
	rec = httptest.NewRecorder()
	metricsMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Body.String() != "metrics-body" {
		t.Fatalf("metrics route body = %q", rec.Body.String())
	}
}
