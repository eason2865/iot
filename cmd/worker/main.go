package main

import (
	"log"

	"iot/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("worker"); err != nil {
		log.Fatal(err)
	}
}
