package opsagent

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type alertQueue struct {
	db                          *sql.DB
	token, robotURL, signingKey string
	wake                        chan struct{}
}

func newAlertQueue(s *Server) (*alertQueue, error) {
	dsn, token, robot, sign := os.Getenv("OPS_ALERT_DSN"), os.Getenv("OPS_GRAFANA_TOKEN"), os.Getenv("OPS_DINGTALK_URL"), os.Getenv("OPS_DINGTALK_SECRET")
	if dsn == "" && token == "" && robot == "" && sign == "" {
		return nil, nil
	}
	u, err := url.Parse(robot)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Fragment != "" || dsn == "" || token == "" || strings.ContainsAny(token, " \t\r\n") || s.prometheus == "" {
		return nil, errors.New("alert analysis requires a queue DSN, Grafana bearer token, robot URL and Prometheus URL")
	}
	for _, cred := range s.tenants {
		if token == cred.OperatorToken || token == cred.ManagementToken {
			return nil, errors.New("Grafana credential must be distinct from tenant credentials")
		}
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, errors.New("invalid alert queue connection settings")
	}
	db.SetMaxOpenConns(5)
	return &alertQueue{db: db, token: token, robotURL: robot, signingKey: sign, wake: make(chan struct{}, 1)}, nil
}

type grafanaAlert struct {
	Status, Fingerprint string
	StartsAt, EndsAt    time.Time
	Labels              map[string]string
}
type alertEvent struct {
	Rule    string    `json:"rule"`
	Service string    `json:"service"`
	Stage   string    `json:"stage,omitempty"`
	Status  string    `json:"status"`
	At      time.Time `json:"at"`
}
type queuedEvent struct {
	id    string
	event alertEvent
}

func (q *alertQueue) receive(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare([]byte(token), []byte(q.token)) != 1 {
		http.Error(w, "invalid Grafana credential", 401)
		return
	}
	var payload struct {
		Alerts          []grafanaAlert
		TruncatedAlerts int
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&payload); err != nil || dec.Decode(new(any)) != io.EOF || len(payload.Alerts) == 0 || len(payload.Alerts) > 128 || payload.TruncatedAlerts != 0 {
		http.Error(w, "invalid or truncated Grafana alert group", 400)
		return
	}
	var events []queuedEvent
	for _, a := range payload.Alerts {
		// Grafana can copy rule labels onto its own datasource-health alerts.
		name := a.Labels["alertname"]
		if name == "DatasourceNoData" || name == "DatasourceError" {
			continue
		}
		rule := a.Labels["rule_uid"]
		if rule == "" {
			rule = ruleByTitle[name]
		}
		if _, supported := alertRules[rule]; !supported {
			continue
		}
		if a.Fingerprint == "" || len(a.Fingerprint) > 128 || a.StartsAt.IsZero() || (a.Status != "firing" && a.Status != "resolved") {
			http.Error(w, "invalid alert identity", 400)
			return
		}
		e := makeEvent(rule, a)
		if e.At.IsZero() || e.At.Before(a.StartsAt) {
			http.Error(w, "invalid alert time", 400)
			return
		}
		sum := sha256.Sum256([]byte(rule + "\x00" + a.Fingerprint + "\x00" + a.StartsAt.UTC().Format(time.RFC3339Nano) + "\x00" + a.Status))
		events = append(events, queuedEvent{hex.EncodeToString(sum[:]), e})
	}
	if len(events) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := q.enqueue(ctx, events); err != nil {
		http.Error(w, "queue unavailable", 503)
		return
	}
	select {
	case q.wake <- struct{}{}:
	default:
	}
	w.WriteHeader(http.StatusAccepted)
}

func (q *alertQueue) enqueue(ctx context.Context, events []queuedEvent) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, j := range events {
		data, err := json.Marshal(j.event)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO ops_alert_jobs(event_id,event) VALUES ($1,$2) ON CONFLICT DO NOTHING", j.id, data); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// RunAlerts processes only committed webhook deliveries. Leases and event IDs
// are shared across replicas. Cancellation leaves interrupted work retryable.
func (s *Server) RunAlerts(ctx context.Context) {
	q := s.alerts
	if q == nil {
		return
	}
	defer q.db.Close()
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	sweep := time.NewTicker(time.Hour)
	defer sweep.Stop()
	for ctx.Err() == nil {
		select {
		case <-sweep.C:
			clean, cancel := context.WithTimeout(ctx, 4*time.Second)
			_, err := q.db.ExecContext(clean, "DELETE FROM ops_alert_jobs WHERE finished_at < now() - interval '7 days'")
			cancel()
			if err != nil {
				log.Print("alert queue retention cleanup failed")
			}
		default:
		}
		worked, err := s.processAlert(ctx)
		if err != nil && ctx.Err() == nil {
			log.Print("alert queue processing unavailable")
		}
		if worked && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-q.wake:
		case <-poll.C:
		}
	}
}
func (s *Server) processAlert(parent context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, 65*time.Second)
	defer cancel()
	q := s.alerts
	var id, message string
	var raw []byte
	var attempt int
	err := q.db.QueryRowContext(ctx, `WITH picked AS (
 SELECT event_id FROM ops_alert_jobs WHERE finished_at IS NULL AND available_at<=now()
 ORDER BY available_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED)
 UPDATE ops_alert_jobs j SET attempts=j.attempts+1,available_at=now()+interval '90 seconds'
 FROM picked WHERE j.event_id=picked.event_id RETURNING j.event_id,j.event,j.message,j.attempts`).Scan(&id, &raw, &message, &attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if message == "" {
		var event alertEvent
		if err = json.Unmarshal(raw, &event); err != nil {
			return true, err
		}
		message = s.alertMessage(ctx, event)
		result, err := q.db.ExecContext(ctx, "UPDATE ops_alert_jobs SET message=$3 WHERE event_id=$1 AND attempts=$2", id, attempt, message)
		if err != nil {
			return true, err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return true, nil
		}
	}
	delivered := q.notify(ctx, s, message) == nil
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	// A remote acceptance followed by a lost response can result in a duplicate.
	_, err = q.db.ExecContext(ctx, `UPDATE ops_alert_jobs SET delivered=$3,
 finished_at=CASE WHEN $3 OR attempts>=6 THEN now() END,
 available_at=now()+$4 * interval '1 second' WHERE event_id=$1 AND attempts=$2`, id, attempt, delivered, 10<<min(attempt-1, 5))
	if !delivered {
		log.Printf("alert supplement delivery failed: event=%s attempt=%d", id, attempt)
	}
	return true, err
}
func (q *alertQueue) notify(ctx context.Context, s *Server, message string) error {
	target, err := url.Parse(q.robotURL)
	if err != nil {
		return err
	}
	if q.signingKey != "" {
		stamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
		mac := hmac.New(sha256.New, []byte(q.signingKey))
		fmt.Fprint(mac, stamp+"\n"+q.signingKey)
		v := target.Query()
		v.Set("timestamp", stamp)
		v.Set("sign", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
		target.RawQuery = v.Encode()
	}
	body, _ := json.Marshal(map[string]any{"msgtype": "text", "text": map[string]string{"content": message}})
	var reply struct {
		Code *int `json:"errcode"`
	}
	_, err = s.fetch(ctx, http.MethodPost, target.String(), "", "", body, &reply, 5*time.Second)
	if err != nil || reply.Code == nil || *reply.Code != 0 {
		return errors.New("robot did not confirm delivery")
	}
	return nil
}
