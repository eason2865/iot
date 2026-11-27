package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"iot/internal/opsagent"
)

func main() {
	agent, err := opsagent.FromEnv()
	if err != nil {
		log.Fatal(err)
	}
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8084"
	}
	server := &http.Server{Addr: addr, Handler: agent.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); agent.RunAlerts(ctx) }()
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 50*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
		close(done)
	}()
	if err = server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	<-done
	<-workerDone
}
