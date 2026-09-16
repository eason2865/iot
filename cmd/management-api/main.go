package main

import (
	"log"

	"iot/internal/adminapi"
)

func main() {
	if err := adminapi.Run(); err != nil {
		log.Fatal(err)
	}
}
