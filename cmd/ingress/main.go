package main

import (
	"log"

	"iot/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("ingress"); err != nil {
		log.Fatal(err)
	}
}
