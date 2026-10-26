package platform

import (
	"encoding/json"
	"iot/internal/contracts"
	"net/http"
)

type apiError struct {
	Error string `json:"error"`
}

func decodeJSON(r *http.Request, dst any) error { return contracts.DecodeRequestJSON(r.Body, dst) }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, apiError{Error: msg})
}

type observedResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *observedResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
