package main

import (
	"log"

	"iot/internal/bootstrap"
)

func main() {
	if err := bootstrap.Run("admin"); err != nil {
		log.Fatal(err)
	}
}
