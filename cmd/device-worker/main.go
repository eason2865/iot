package main

import (
	"log"

	"iot/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("device-worker"); err != nil {
		log.Fatal(err)
	}
}
