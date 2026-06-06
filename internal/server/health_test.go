package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"mqtt/internal/server"
)

func TestHealthHandler(t *testing.T) {
	handler := server.NewHealthHandler("ingress")
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", rec.Code, http.StatusOK)
	}

	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if got := string(body); got != "ok" {
		t.Fatalf("body = %q, want %q", got, "ok")
	}
}
