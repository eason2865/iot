package main

import (
	"log"

	"iot/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("telemetry-ingestor"); err != nil {
		log.Fatal(err)
	}
}
