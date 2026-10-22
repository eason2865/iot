package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
	"iot/internal/contracts"
)

type workerAckState struct {
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	stopping  bool
	pending   sync.WaitGroup
	fatal     chan error
	ready     chan struct{}
	readyOnce sync.Once
}

func newWorkerAckState() *workerAckState {
	ctx, cancel := context.WithCancel(context.Background())
	return &workerAckState{ctx: ctx, cancel: cancel, fatal: make(chan error, 1), ready: make(chan struct{})}
}

func (w *Worker) failAck(err error) {
	w.readiness.stop()
	log.Print(err)
	select {
	case w.acks.fatal <- err:
	default:
	}
}

func (w *Worker) subscribeAckTopics(client mqtt.Client, topics []string) {
	generation := w.readiness.reset()
	ctx, cancel := context.WithTimeout(w.acks.ctx, 5*time.Second)
	defer cancel()
	for _, topic := range topics {
		token := client.Subscribe(topic, 1, w.handleAckMessage)
		select {
		case <-ctx.Done():
			if w.acks.ctx.Err() != nil {
				return
			} // Normal worker shutdown.
			w.failAck(fmt.Errorf("command ACK subscription incomplete: topic=%s: %w", topic, ctx.Err()))
			return
		case <-token.Done():
		}
		if err := token.Error(); err != nil {
			w.failAck(fmt.Errorf("command ACK subscribe: topic=%s: %w", topic, err))
			return
		}
		sub, ok := token.(*mqtt.SubscribeToken)
		if !ok {
			w.failAck(fmt.Errorf("command ACK subscribe: topic=%s: missing SUBACK result", topic))
			return
		}
		granted, ok := sub.Result()[topic]
		if !ok || granted != 1 {
			w.failAck(fmt.Errorf("command ACK subscribe: topic=%s: expected QoS 1, SUBACK=%#x present=%t", topic, granted, ok))
			return
		}
	}
	if w.readiness.confirm(generation) {
		w.acks.readyOnce.Do(func() { close(w.acks.ready) })
	}
}

func (w *Worker) handleAckMessage(_ mqtt.Client, msg mqtt.Message) {
	w.acks.mu.Lock()
	if w.acks.stopping {
		w.acks.mu.Unlock()
		return
	}
	w.acks.pending.Add(1)
	w.acks.mu.Unlock()
	defer w.acks.pending.Done()
	var ack CommandAckMessage
	if err := json.Unmarshal(msg.Payload(), &ack); err != nil {
		w.rejectAck(msg, "invalid JSON")
		return
	}
	tenant, device, suffix, ok := contracts.ParseDeviceTopic(msg.Topic())
	if !ok || suffix != contracts.TopicSuffixAck || tenant != ack.TenantID || device != ack.DeviceID || ack.CommandID == "" {
		w.rejectAck(msg, "invalid identity or commandId")
		return
	}
	err := errors.New("ACK store is not configured")
	if w.store != nil {
		for attempt := 0; attempt < ackStoreAttempts; attempt++ {
			err = w.applyAck(ack)
			if err == nil || IsNotFound(err) {
				break
			}
			if attempt+1 < ackStoreAttempts {
				timer := time.NewTimer(ackStoreBackoff * time.Duration(attempt+1))
				select {
				case <-w.acks.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
	}
	if IsNotFound(err) {
		// Unknown/foreign commands are intentionally rejected, not retried.
		w.rejectAck(msg, "unknown command")
		return
	}
	if err != nil {
		log.Printf("command ack store error after retries: %v", err)
		if w.metrics != nil {
			w.metrics.IncDeviceWorker("ack", "error")
		}
		dlqCtx, cancel := context.WithTimeout(w.acks.ctx, 5*time.Second)
		dlqErr := publishDeadLetter(dlqCtx, w.dlqWriter, kafka.Message{Topic: msg.Topic(), Key: []byte(ack.CommandID), Value: msg.Payload()}, StageCommandAck, err, w.metrics)
		cancel()
		if dlqErr != nil {
			failure := fmt.Errorf("command ACK not persisted: store: %v; DLQ: %w", err, dlqErr)
			w.failAck(failure)
			return // No MQTT PUBACK without a durable copy.
		}
	} else if w.metrics != nil {
		w.metrics.IncDeviceWorker("ack", "ok")
	}
	msg.Ack()
}

func (w *Worker) rejectAck(msg mqtt.Message, reason string) {
	log.Printf("command ack rejected: %s", reason)
	if w.metrics != nil {
		w.metrics.IncDeviceWorker("ack", "error")
	}
	msg.Ack() // Permanent invalid input must not become a redelivery loop.
}

func (w *Worker) applyAck(ack CommandAckMessage) error {
	if store, ok := w.store.(interface {
		AckCommandContext(context.Context, string, string, string) (Command, error)
	}); ok {
		ctx, cancel := context.WithTimeout(w.acks.ctx, 2*time.Second)
		defer cancel()
		_, err := store.AckCommandContext(ctx, ack.CommandID, ack.TenantID, ack.DeviceID)
		return err
	}
	_, err := w.store.AckCommand(ack.CommandID, ack.TenantID, ack.DeviceID)
	return err
}

// Stop admission before Wait so callbacks cannot race WaitGroup.Add. Keep MQTT
// and the DLQ writer alive while admitted ACKs finish their persistence attempt.
func (w *Worker) stopAckHandling() {
	w.readiness.stop()
	w.acks.mu.Lock()
	w.acks.stopping = true
	w.acks.mu.Unlock()
	done := make(chan struct{})
	go func() { w.acks.pending.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		w.acks.cancel()
		<-done
	}
	w.acks.cancel()
	if w.mqtt != nil {
		w.mqtt.Disconnect(250)
	}
}
