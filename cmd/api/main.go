package main

import (
	"log"
	"net/http"

	"github.com/capstone-b4/capstone-go/internal/application"
	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/delivery/handler"
	"github.com/capstone-b4/capstone-go/internal/delivery/middleware"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/database"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/queue"

	"github.com/gin-gonic/gin"
)

func main() {
	config.LoadConfig()
	database.ConnectPostgres()
	defer database.ClosePostgres()

	cache.ConnectRedis()
	defer cache.CloseRedis()

	queue.InitKafkaProducer()
	defer queue.CloseKafkaProducer()

	txRepo := database.NewTransactionRepository(database.PostgresPool)
	txService := application.NewTransactionService(txRepo)
	txHandler := handler.NewTransactionHandler(txService)

	r := gin.Default()

	apiGroup := r.Group("/")
	apiGroup.Use(middleware.RateLimiter())
	{
		apiGroup.POST("/transactions", txHandler.Create)
		apiGroup.GET("/transactions/:txId", txHandler.GetByTxID)
		apiGroup.GET("/users/:id/balance", txHandler.GetUserBalance)
	}

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	port := config.AppConfig.ServerPort
	if port == "" {
		port = "8000"
		log.Println("Warning: Port empty, fallback to 8000")
	}

	log.Printf("Server starting on :%s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
