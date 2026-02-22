package main

import (
	"log"

	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/database"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/queue"
)

func main() {
	config.LoadConfig()

	database.ConnectPostgres()
	defer database.ClosePostgres()

	database.ConnectMongo()
	defer database.CloseMongo()

	// Start consumer
	go queue.StartKafkaConsumer()
	defer queue.CloseKafkaConsumer()

	log.Println("Worker running... Kafka consumer active")

	select {} // keep alive
}
