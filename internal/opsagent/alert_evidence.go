package opsagent

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

type ruleSpec struct{ title, service, metric, filters, measure string }

var alertRules = map[string]ruleSpec{
	"iot-management-api-http-5xx":    {"Management API HTTP 5xx 持续出现", "management-api", "iot_http_requests_total", `status="5xx"`, "HTTP 5xx"},
	"iot-management-api-healthz-qps": {"Management API healthz QPS 超过 1", "management-api", "iot_http_requests_total", `route="/healthz",status="2xx"`, "healthz 2xx"},
	"iot-dlq-writes":                 {"DLQ 正在产生死信", "iot-pipeline", "iot_dlq_publish_total", `result="ok"`, "成功写入死信"},
	"iot-dlq-write-failures":         {"死信写入失败（消息未保全）", "iot-pipeline", "iot_dlq_publish_total", `result="error"`, "写入死信失败"},
}
var ruleByTitle = func() map[string]string {
	m := make(map[string]string)
	for id, spec := range alertRules {
		m[spec.title] = id
	}
	return m
}()
var pipelineJobs = []string{"device-worker", "telemetry-ingestor"}
var pipelineStages = []string{"mqtt.decode", "mqtt.identity", "mqtt.kafka", "telemetry.decode", "telemetry.postgres", "telemetry.tdengine", "command.decode", "command.topic", "command.encode", "command.mqtt", "command.ack"}

func makeEvent(rule string, a grafanaAlert) alertEvent {
	e := alertEvent{Rule: rule, Service: alertRules[rule].service, Status: a.Status, At: a.StartsAt.UTC()}
	if e.Status == "resolved" {
		e.At = a.EndsAt.UTC()
	}
	if e.Service == "iot-pipeline" {
		if job := a.Labels["job"]; slices.Contains(pipelineJobs, job) {
			e.Service = job
		}
		if stage := a.Labels["stage"]; slices.Contains(pipelineStages, stage) {
			e.Stage = stage
		}
	}
	return e
}

func (e alertEvent) selectors() (prom, trace string) {
	if e.Service == "iot-pipeline" {
		prom = `job=~"device-worker|telemetry-ingestor"`
		trace = `resource.service.name =~ "device-worker|telemetry-ingestor"`
	} else {
		prom = "job=" + strconv.Quote(e.Service)
		trace = "resource.service.name = " + strconv.Quote(e.Service)
	}
	spec := alertRules[e.Rule]
	filters := prom + "," + spec.filters
	if e.Stage != "" {
		filters += ",stage=" + strconv.Quote(e.Stage)
	}
	return "sum(rate(" + spec.metric + "{" + filters + "}[5m]))", "{ " + trace + " && status = error }"
}

func (s *Server) alertRate(ctx context.Context, query string, at time.Time) string {
	v := url.Values{"query": {query}, "time": {at.Format(time.RFC3339Nano)}}
	var result struct {
		Status string
		Data   struct {
			ResultType string
			Result     []struct{ Value []json.RawMessage }
		}
	}
	_, err := s.fetch(ctx, http.MethodGet, s.prometheus, "/api/v1/query?"+v.Encode(), "", nil, &result, 4*time.Second)
	if err != nil || result.Status != "success" || result.Data.ResultType != "vector" {
		return "查询失败"
	}
	if len(result.Data.Result) == 0 {
		return "无样本"
	}
	if len(result.Data.Result) != 1 || len(result.Data.Result[0].Value) != 2 {
		return "响应无效"
	}
	var raw string
	if json.Unmarshal(result.Data.Result[0].Value[1], &raw) != nil {
		return "响应无效"
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
		return "无有效样本"
	}
	return fmt.Sprintf("%.4g 次/秒", n)
}

func (s *Server) alertTraces(ctx context.Context, query string, start, end time.Time) string {
	if s.tempo == "" {
		return "未配置"
	}
	const limit = 5
	v := url.Values{"q": {query}, "start": {strconv.FormatInt(start.Unix(), 10)}, "end": {strconv.FormatInt(end.Unix(), 10)}, "limit": {strconv.Itoa(limit)}, "spss": {"1"}}
	var result struct{ Traces []struct{ TraceID string } }
	_, err := s.fetch(ctx, http.MethodGet, s.tempo, "/api/search?"+v.Encode(), "", nil, &result, 4*time.Second)
	if err != nil {
		return "查询失败"
	}
	count := len(result.Traces)
	if count >= limit {
		return "至少 5 条已采样错误链路"
	}
	return fmt.Sprintf("%d 条已采样错误链路", count)
}

func (s *Server) alertMessage(ctx context.Context, e alertEvent) string {
	start := e.At.Add(-5 * time.Minute)
	prom, trace := e.selectors()
	metric := s.alertRate(ctx, prom, e.At)
	traces := s.alertTraces(ctx, trace, start, e.At)
	scope := e.Service
	if e.Stage != "" {
		scope += " / " + e.Stage
	}
	phase := "触发"
	if e.Status == "resolved" {
		phase = "恢复"
	}
	spec := alertRules[e.Rule]
	evidence := fmt.Sprintf("IoT 告警补充分析 · %s\n%s\n范围：%s\n窗口：%s 至 %s (UTC)\nPrometheus · %s：%s\nTempo：%s", phase, spec.title, scope, start.Format("2006-01-02 15:04:05"), e.At.Format("2006-01-02 15:04:05"), spec.measure, metric, traces)
	status, advice := s.modelSummary(ctx, "用中文提出一个可验证的可能原因及一个具体的只读核查步骤。原因必须标为假设。未配置、无样本或查询失败的来源不能证明服务健康或异常；零个已采样错误链路不能排除故障。仅使用给定的服务和时间窗。", evidence)
	if status == "ok" {
		runes := []rune(advice)
		if len(runes) > 900 {
			advice = string(runes[:900]) + "…"
		}
		return evidence + "\n模型建议（待核实）：" + strings.TrimSpace(advice)
	}
	if status == "disabled" {
		return evidence + "\n模型：未配置"
	}
	return evidence + "\n模型：暂不可用"
}
