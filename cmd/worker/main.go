package main

import (
	"log"

	"mqtt/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("worker"); err != nil {
		log.Fatal(err)
	}
}
