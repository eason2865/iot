// Package opsagent serves tenant-scoped, read-only command diagnoses.
package opsagent

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"iot/internal/contracts"
)

type tenantAccess struct {
	OperatorToken   string `json:"operatorToken"`
	ManagementToken string `json:"managementToken"`
}

type Server struct {
	tenants    map[string]tenantAccess
	management string
	prometheus string
	tempo      string
	modelURL   string
	model      string
	modelKey   string
	client     *http.Client
	slots      chan struct{}
}

func FromEnv() (*Server, error) {
	s := &Server{
		management: os.Getenv("OPS_MANAGEMENT_URL"),
		prometheus: os.Getenv("OPS_PROMETHEUS_URL"),
		tempo:      os.Getenv("OPS_TEMPO_URL"),
		modelURL:   os.Getenv("OPS_MODEL_URL"),
		model:      os.Getenv("OPS_MODEL"),
		modelKey:   os.Getenv("OPS_MODEL_KEY"),
		slots:      make(chan struct{}, 8),
		client:     &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
	if s.management == "" {
		s.management = "http://management-api:8080"
	}
	for _, raw := range []string{s.management, s.prometheus, s.tempo, s.modelURL} {
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("invalid ops-agent upstream URL")
		}
	}
	if (s.modelURL == "") != (s.model == "") || (s.modelURL == "" && s.modelKey != "") {
		return nil, errors.New("configure OPS_MODEL_URL and OPS_MODEL together")
	}
	if err := contracts.DecodeRequestJSON(strings.NewReader(os.Getenv("OPS_TENANTS")), &s.tenants); err != nil || len(s.tenants) == 0 {
		return nil, errors.New("OPS_TENANTS must contain tenant credentials")
	}
	used := map[string]bool{}
	for id, keys := range s.tenants {
		if !contracts.IsValidTenantID(id) {
			return nil, errors.New("invalid OPS_TENANTS tenant")
		}
		for _, key := range []string{keys.OperatorToken, keys.ManagementToken} {
			if key == "" || strings.ContainsAny(key, " \t\r\n") || used[key] {
				return nil, errors.New("OPS_TENANTS requires distinct nonempty bearer credentials")
			}
			used[key] = true
		}
	}
	return s, nil
}

type command struct {
	ID      string                  `json:"id"`
	Tenant  string                  `json:"tenantId"`
	Status  contracts.CommandStatus `json:"status"`
	Created time.Time               `json:"createdAt"`
	Updated time.Time               `json:"updatedAt"`
}

type report struct {
	Command     command         `json:"command"`
	Finding     string          `json:"finding"`
	Metrics     metricsEvidence `json:"metrics"`
	Trace       traceEvidence   `json:"trace"`
	ModelStatus string          `json:"modelStatus"`
	ModelAdvice string          `json:"modelAdvice,omitempty"`
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /api/v1/tenants/{tenant}/commands/{id}/diagnoses", s.diagnose)
	return mux
}

func (s *Server) diagnose(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	tenant, id := r.PathValue("tenant"), r.PathValue("id")
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	keys := s.tenants[tenant]
	if !ok || token == "" || len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare([]byte(token), []byte(keys.OperatorToken)) != 1 {
		http.Error(w, "invalid tenant credentials", http.StatusUnauthorized)
		return
	}
	if r.URL.RawQuery != "" || contracts.ValidateCommandID(id) != nil || id == "." || id == ".." {
		http.Error(w, "invalid diagnosis parameters", 400)
		return
	}
	var input struct {
		Question string `json:"question"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := contracts.DecodeRequestJSON(http.MaxBytesReader(w, r.Body, 8192), &input); err != nil || utf8.RuneCountInString(input.Question) > 512 {
			http.Error(w, "invalid diagnosis request", 400)
			return
		}
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		http.Error(w, "agent busy", 429)
		return
	}
	path := "/api/v1/tenants/" + url.PathEscape(tenant) + "/commands/" + url.PathEscape(id)
	var out report
	status, err := s.fetch(r.Context(), http.MethodGet, s.management, path, keys.ManagementToken, nil, &out.Command, 4*time.Second)
	if status == 404 {
		http.Error(w, "command not found", 404)
		return
	}
	if err != nil || out.Command.ID != id || out.Command.Tenant != tenant || out.Command.Created.IsZero() || out.Command.Updated.IsZero() {
		http.Error(w, "command lookup unavailable", 502)
		return
	}
	out.Finding = findings[out.Command.Status]
	if out.Finding == "" {
		http.Error(w, "unknown command state", 502)
		return
	}
	// A successful, tenant-bound command lookup gates both infrastructure tools.
	metrics := make(chan metricsEvidence, 1)
	go func() { metrics <- s.metrics(r.Context(), out.Command.Updated) }()
	out.Trace = s.trace(r.Context(), out.Command)
	out.Metrics = <-metrics
	out.ModelStatus, out.ModelAdvice = s.advice(r.Context(), input.Question, out)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

var findings = map[contracts.CommandStatus]string{
	contracts.CommandStatusCreated:   "命令已创建，平台尚未记录 Kafka 发布成功。",
	contracts.CommandStatusPublished: "平台已记录 Kafka 发布，尚未记录 MQTT 下发确认。",
	contracts.CommandStatusSent:      "平台已记录 MQTT 下发，正在等待设备回执。",
	contracts.CommandStatusAcked:     "平台已持久化设备成功回执。",
	contracts.CommandStatusFailed:    "命令已失败。可结合回执或派发阶段的追踪区分失败来源。",
	contracts.CommandStatusTimeout:   "平台已记录 MQTT 下发；在 ACK 期限内未记录设备回执。",
}

func (s *Server) fetch(ctx context.Context, method, base, path, token string, body []byte, dest any, timeout time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return res.StatusCode, errors.New("upstream request failed")
	}
	const limit = 2 << 20
	b, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil || len(b) > limit {
		return res.StatusCode, errors.New("invalid upstream response")
	}
	return res.StatusCode, json.Unmarshal(b, dest)
}
