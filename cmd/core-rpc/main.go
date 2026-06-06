package main

import (
	"log"

	"iot/internal/core"
)

func main() {
	if err := core.Run(); err != nil {
		log.Fatal(err)
	}
}
