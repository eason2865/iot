package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"iot/internal/platform"
)

func main() {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		log.Fatal("POSTGRES_DSN is required")
	}
	store, err := platform.OpenPostgresStore(dsn, 0, false)
	if err != nil {
		log.Fatal("cannot connect to PostgreSQL for migration")
	}
	defer store.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := store.MigrateSchema(ctx); err != nil {
		log.Fatalf("PostgreSQL migration failed: %v", err)
	}
	log.Print("PostgreSQL migration complete")
}
