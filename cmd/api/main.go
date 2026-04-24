package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/capstone-b4/capstone-go/internal/application"
	"github.com/capstone-b4/capstone-go/internal/config"
	"github.com/capstone-b4/capstone-go/internal/delivery/handler"
	"github.com/capstone-b4/capstone-go/internal/delivery/middleware"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/database"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/logging"
	_ "github.com/capstone-b4/capstone-go/internal/infrastructure/observability"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/queue"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/gin-contrib/requestid"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

var (
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)

	requestTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)
)

func init() {
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(requestTotal)
}

func main() {
	logging.InitLogger()

	config.LoadConfig()

	database.ConnectPostgres()
	defer database.ClosePostgres()

	database.ConnectMongo()
	defer database.CloseMongo()

	cache.ConnectRedis()
	defer cache.CloseRedis()

	queue.InitKafkaProducer()
	defer queue.CloseKafkaProducer()

	txRepo := database.NewTransactionRepository(database.WritePool, database.ReadPool)
	txService := application.NewTransactionService(txRepo)
	txHandler := handler.NewTransactionHandler(txService)

	// Menggunakan gin.New() tanpa Logger bawaan untuk meminimalisasi CPU blocking I/O di terminal
	r := gin.New()
	r.Use(middleware.CustomRecovery())

	// Melindungi container dari goroutine leak saat DDOS (Limit 100 request concurrent / fail-fast)
	middleware.InitDDosShield(100)
	r.Use(middleware.DDosShield())

	r.Use(func(c *gin.Context) {
		start := time.Now()
		method := c.Request.Method
		path := c.FullPath()

		c.Next()

		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())

		requestDuration.WithLabelValues(method, path, status).Observe(duration)
		requestTotal.WithLabelValues(method, path, status).Inc()
	})

	r.Use(requestid.New())

	// Inisialisasi Asynchronous UUID Pre-Generator Pool untuk ultra-low latency
	uuidPool := make(chan string, 10000)
	for i := 0; i < 3; i++ { // 3 Worker mempercepat pengisian
		go func() {
			for {
				uuidPool <- uuid.New().String()
			}
		}()
	}

	r.Use(func(c *gin.Context) {
		reqID := requestid.Get(c)
		if reqID == "" {
			select {
			case reqID = <-uuidPool:
			default:
				// Fallback jika semua worker uuid sibuk dan antrean pool kosong
				reqID = uuid.New().String()
			}
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
		apiGroup.GET("/users/:id/transactions", txHandler.GetUserTransactions)
	}

	r.GET("/health", func(c *gin.Context) {
		logger := logging.GetLogger(c)

		components := make(map[string]string)
		overallStatus := "healthy"
		var errMsg string

		pgCtx, pgCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer pgCancel()
		if err := database.WritePool.Ping(pgCtx); err != nil {
			components["postgres_primary"] = "down"
			overallStatus = "unhealthy"
			errMsg += fmt.Sprintf("Postgres Primary down: %v; ", err)
			logger.Warn().Err(err).Msg("Health check: Postgres Primary down")
		} else {
			components["postgres_primary"] = "up"
		}

		pgRepCtx, pgRepCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer pgRepCancel()
		if err := database.ReadPool.Ping(pgRepCtx); err != nil {
			components["postgres_replica"] = "down"
			overallStatus = "unhealthy"
			errMsg += fmt.Sprintf("Postgres Replica down: %v; ", err)
			logger.Warn().Err(err).Msg("Health check: Postgres Replica down")
		} else {
			components["postgres_replica"] = "up"
		}

		redisCtx, redisCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer redisCancel()
		if _, err := cache.RedisClient.Ping(redisCtx).Result(); err != nil {
			components["redis"] = "down"
			overallStatus = "unhealthy"
			errMsg += fmt.Sprintf("Redis down: %v; ", err)
			logger.Warn().Err(err).Msg("Health check: Redis down")
		} else {
			components["redis"] = "up"
		}

		kafkaStatus := "up"
		if !queue.IsKafkaProducerReady() {
			kafkaStatus = "down"
			overallStatus = "unhealthy"
			errMsg += "Kafka producer nil; "
			logger.Warn().Msg("Health check: Kafka producer nil")
		}
		components["kafka"] = kafkaStatus

		mongoCtx, mongoCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer mongoCancel()
		if err := database.MongoClient.Ping(mongoCtx, nil); err != nil {
			components["mongo"] = "down"
			overallStatus = "unhealthy"
			errMsg += fmt.Sprintf("Mongo down: %v; ", err)
			logger.Warn().Err(err).Msg("Health check: Mongo down")
		} else {
			components["mongo"] = "up"
		}

		response := gin.H{
			"status":     overallStatus,
			"components": components,
		}
		if errMsg != "" {
			response["message"] = errMsg
		}

		logger.Info().
			Str("overall_status", overallStatus).
			Interface("components", components).
			Msg("Health check performed")

		statusCode := http.StatusOK
		switch overallStatus {
		case "unhealthy":
			statusCode = http.StatusServiceUnavailable
		case "degraded":
			statusCode = http.StatusOK
		}

		c.JSON(statusCode, response)
	})

	port := config.AppConfig.ServerPort
	if port == "" {
		port = "8000"
		log.Info().Msg("Warning: Port empty, fallback to 8000")
	}

	log.Info().
		Str("port", port).
		Msg("Server starting on :" + port)

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           r,
		ReadHeaderTimeout: 3 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal().Err(err).Msg("Failed to start server")
	}
}
