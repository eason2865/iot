package main

import (
	"log"

	"mqtt/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("admin"); err != nil {
		log.Fatal(err)
	}
}
