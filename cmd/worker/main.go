package main

import (
	"log"
	"time"

	"github.com/capstone-b4/capstone-go/internal/config"
)

func main() {
	config.LoadConfig()
	log.Println("Worker starting... Kafka consumer placeholder")

	log.Println("Worker ready (heartbeat every 10 seconds)")

	for {
		log.Println("Worker heartbeat - still alive at", time.Now().Format(time.RFC3339))
		time.Sleep(10 * time.Second)
	}
}
