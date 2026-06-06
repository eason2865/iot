package main

import (
	"log"

	"iot/internal/demo"
)

func main() {
	if err := demo.Run(); err != nil {
		log.Fatal(err)
	}
}
