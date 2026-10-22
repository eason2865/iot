package platform

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestMQTTReadinessTracksSubscriptionsAndReconnect(t *testing.T) {
	for _, kind := range []string{"ingestor", "worker"} {
		t.Run(kind, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			connections := make(chan net.Conn, 2)
			subscribed := make(chan chan struct{})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			brokerDone := make(chan struct{})
			subCount := 1
			if kind == "worker" {
				subCount = 2
			}
			go func() {
				defer close(brokerDone)
				for generation := 0; generation < 2; generation++ {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					connections <- conn
					_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
					if _, err = packets.ReadPacket(conn); err != nil {
						conn.Close()
						return
					}
					if err = packets.NewControlPacket(packets.Connack).Write(conn); err != nil {
						conn.Close()
						return
					}
					for i := 0; i < subCount; i++ {
						packet, err := packets.ReadPacket(conn)
						if err != nil {
							conn.Close()
							return
						}
						sub, ok := packet.(*packets.SubscribePacket)
						if !ok {
							conn.Close()
							return
						}
						release := make(chan struct{})
						select {
						case subscribed <- release:
						case <-ctx.Done():
							conn.Close()
							return
						}
						select {
						case <-release:
						case <-ctx.Done():
							conn.Close()
							return
						}
						ack := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
						ack.MessageID = sub.MessageID
						ack.ReturnCodes = []byte{1}
						if err = ack.Write(conn); err != nil {
							conn.Close()
							return
						}
					}
					_, _ = packets.ReadPacket(conn)
					_ = conn.Close()
				}
			}()
			url := "tcp://" + listener.Addr().String()
			var run func(context.Context) error
			var check func(context.Context) error
			if kind == "ingestor" {
				bridge := NewMQTTBridge(MQTTBridgeConfig{BrokerURL: url, ClientID: "readiness-ingestor"}, noopPublisher{}, nil)
				run, check = bridge.Run, bridge.CheckReady
			} else {
				worker := NewWorker(WorkerConfig{MQTTBrokerURL: url, AckTopicFilters: []string{"tenant/t/device/d/ack", "tenant/t/device/d2/ack"}}, nil, nil, nil)
				run, check = worker.Run, worker.CheckReady
			}
			app := New(Config{ReadinessCheck: check})
			status := func(path string) int {
				res := httptest.NewRecorder()
				app.Router().ServeHTTP(res, httptest.NewRequest(http.MethodGet, path, nil))
				return res.Code
			}
			await := func(want int) {
				t.Helper()
				limit := time.Now().Add(2 * time.Second)
				for time.Now().Before(limit) {
					if status("/readyz") == want {
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
				t.Fatalf("/readyz=%d want=%d", status("/readyz"), want)
			}
			await(503)
			done := make(chan struct{})
			var runErr error
			go func() { runErr = run(ctx); close(done) }()
			defer func() {
				cancel()
				listener.Close()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("MQTT runner did not stop")
				}
				select {
				case <-brokerDone:
				case <-time.After(time.Second):
					t.Error("broker did not stop")
				}
			}()
			for generation := 0; generation < 2; generation++ {
				var conn net.Conn
				select {
				case conn = <-connections:
				case <-ctx.Done():
					t.Fatal("MQTT did not connect")
				}
				for i := 0; i < subCount; i++ {
					var release chan struct{}
					select {
					case release = <-subscribed:
					case <-ctx.Done():
						t.Fatal("MQTT did not subscribe")
					}
					if got := status("/readyz"); got != 503 {
						t.Fatalf("generation=%d pending subscription=%d /readyz=%d", generation, i, got)
					}
					if got := status("/healthz"); got != 200 {
						t.Fatal("liveness followed MQTT readiness")
					}
					close(release)
				}
				await(200)
				if generation == 0 {
					_ = conn.Close()
					await(503)
				}
			}
			cancel()
			select {
			case <-done:
				if runErr != nil {
					t.Fatal(runErr)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("runner did not stop")
			}
			await(503)
		})
	}
}

func TestOldSUBACKCannotRestoreReadiness(t *testing.T) {
	var state mqttReadiness
	old := state.reset()
	current := state.reset()
	if state.confirm(old) {
		t.Fatal("old SUBACK passed the startup gate")
	}
	if state.ready() {
		t.Fatal("old connection became ready")
	}
	if !state.confirm(current) {
		t.Fatal("current SUBACK failed the startup gate")
	}
	if !state.ready() {
		t.Fatal("current connection did not become ready")
	}
	state.stop()
	state.confirm(current)
	if state.ready() {
		t.Fatal("stopped connection became ready")
	}
}
