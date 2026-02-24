package main

import (
	"net/http"

	"github.com/capstone-b4/capstone-go/internal/application"
	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/delivery/handler"
	"github.com/capstone-b4/capstone-go/internal/delivery/middleware"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/database"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/logging"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/queue"
	"github.com/google/uuid"

	"github.com/gin-contrib/requestid"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

func main() {
	logging.InitLogger()

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

	r.Use(requestid.New())

	r.Use(func(c *gin.Context) {
		reqID := requestid.Get(c)
		if reqID == "" {
			reqID = uuid.New().String()
		}

		log.Debug().
			Str("generated_trace_id", reqID).
			Str("path", c.Request.URL.Path).
			Msg("Trace ID generated for request")

		requestLogger := log.With().
			Str("trace_id", reqID).
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
			Logger()

		c.Set("logger", requestLogger)
		c.Set("trace_id", reqID)

		c.Next()
	})

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
		log.Info().Msg("Warning: Port empty, fallback to 8000")
	}

	log.Info().
		Str("port", port).
		Msg("Server starting on :" + port)

	if err := r.Run(":" + port); err != nil {
		log.Fatal().Err(err).Msg("Failed to start server")
	}
}
