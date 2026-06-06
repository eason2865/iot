package main

import (
	"log"

	"mqtt/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("ingress"); err != nil {
		log.Fatal(err)
	}
}
