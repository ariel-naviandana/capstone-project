package main

import (
	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/database"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/logging"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/queue"

	"github.com/rs/zerolog/log"
)

func main() {
	logging.InitLogger()

	config.LoadConfig()

	database.ConnectPostgres()
	defer database.ClosePostgres()

	database.ConnectMongo()
	defer database.CloseMongo()

	cache.ConnectRedis()
	defer cache.CloseRedis()

	go queue.StartKafkaConsumer()
	defer queue.CloseKafkaConsumer()

	log.Info().Msg("Worker running... Kafka consumer active")

	select {}
}
