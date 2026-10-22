package platform

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
	"github.com/segmentio/kafka-go/protocol/createtopics"
	"github.com/segmentio/kafka-go/protocol/metadata"
)

func TestKafkaSetupHonorsContextDuringProtocolRequests(t *testing.T) {
	for _, stage := range []string{"controller", "create"} {
		for _, deadline := range []bool{false, true} {
			t.Run(stage+"/deadline="+strconv.FormatBool(deadline), func(t *testing.T) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				entered := make(chan struct{}, 1)
				var connections sync.WaitGroup
				stop := make(chan struct{})
				var stopOnce sync.Once
				stopAll := func() { stopOnce.Do(func() { close(stop) }) }
				acceptDone := make(chan struct{})
				go func() {
					defer close(acceptDone)
					for {
						conn, err := listener.Accept()
						if err != nil {
							return
						}
						connections.Add(1)
						go func() {
							defer connections.Done()
							defer conn.Close()
							go func() { <-stop; _ = conn.Close() }()
							for {
								version, id, _, request, err := protocol.ReadRequest(conn)
								if err != nil {
									return
								}
								var response protocol.Message
								switch request.(type) {
								case *apiversions.Request:
									response = &apiversions.Response{ApiKeys: []apiversions.ApiKeyResponse{{ApiKey: int16(protocol.Metadata), MinVersion: 0, MaxVersion: 1}, {ApiKey: int16(protocol.CreateTopics), MinVersion: 0, MaxVersion: 0}}}
								case *metadata.Request:
									if stage == "controller" {
										entered <- struct{}{}
										_, _ = io.Copy(io.Discard, conn)
										return
									}
									address := listener.Addr().(*net.TCPAddr)
									response = &metadata.Response{ControllerID: 1, Brokers: []metadata.ResponseBroker{{NodeID: 1, Host: "127.0.0.1", Port: int32(address.Port)}}}
								case *createtopics.Request:
									entered <- struct{}{}
									_, _ = io.Copy(io.Discard, conn)
									return
								default:
									return
								}
								if err = protocol.WriteResponse(conn, version, id, response); err != nil {
									return
								}
							}
						}()
					}
				}()
				defer func() { listener.Close(); stopAll(); <-acceptDone; connections.Wait() }()
				var ctx context.Context
				var cancel context.CancelFunc
				if deadline {
					ctx, cancel = context.WithTimeout(t.Context(), 150*time.Millisecond)
				} else {
					ctx, cancel = context.WithCancel(t.Context())
				}
				defer cancel()
				done := make(chan error, 1)
				go func() {
					done <- ensureKafkaTopics(ctx, []string{listener.Addr().String()}, KafkaTopicConfig{}, "private-test")
				}()
				select {
				case <-entered:
				case <-time.After(time.Second):
					cancel()
					stopAll()
					<-done
					t.Fatal("target request not reached")
				}
				if !deadline {
					cancel()
				}
				select {
				case err = <-done:
					if err == nil {
						t.Fatal("stalled broker reported success")
					}
				case <-time.After(350 * time.Millisecond):
					cancel()
					stopAll()
					<-done
					t.Fatal("context did not interrupt Kafka request")
				}
			})
		}
	}
}

func TestKafkaSetupRetryBackoffHonorsTotalBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		ensureKafkaTopicsUntil(ctx, []string{"127.0.0.1:1"}, KafkaTopicConfig{}, "private-test")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(350 * time.Millisecond):
		t.Fatal("retry backoff exceeded the total startup budget")
	}
}
