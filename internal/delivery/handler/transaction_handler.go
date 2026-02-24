package handler

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/capstone-b4/capstone-go/internal/application"
	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/logging"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/queue"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/resilience"
	"github.com/google/uuid"

	"github.com/gin-gonic/gin"
)

type TransactionHandler struct {
	service *application.TransactionService
}

func NewTransactionHandler(service *application.TransactionService) *TransactionHandler {
	return &TransactionHandler{service: service}
}

func (h *TransactionHandler) Create(c *gin.Context) {
	logger := logging.GetLogger(c)

	var input domain.TransactionCreate
	if err := c.ShouldBindJSON(&input); err != nil {
		logger.Warn().
			Err(err).
			Msg("Invalid input on Create transaction")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input", "detail": err.Error()})
		return
	}

	if input.Type == "transfer" && input.RecipientID == 0 {
		logger.Warn().Msg("Missing recipient_id for transfer")
		c.JSON(http.StatusBadRequest, gin.H{"error": "recipient_id wajib untuk transfer"})
		return
	}

	txID := uuid.New().String()

	traceID := c.GetString("trace_id")
	_, err := resilience.ExecuteWithBreaker[struct{}](
		c.Request.Context(),
		resilience.KafkaProducerBreaker,
		"KafkaPublish",
		func() (struct{}, error) {
			return struct{}{}, queue.PublishTransactionEvent(txID, input.UserID, input.RecipientID, input.Amount, input.Type, traceID)
		},
	)
	if err != nil {
		logger.Warn().
			Err(err).
			Str("tx_id", txID).
			Str("type", input.Type).
			Msg("Kafka publish ditolak breaker")
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":   "Sistem sedang overload atau Kafka tidak tersedia",
			"message": "Coba lagi dalam beberapa detik",
		})
		return
	}

	logger.Info().
		Str("tx_id", txID).
		Str("type", input.Type).
		Int64("user_id", input.UserID).
		Float64("amount", input.Amount).
		Msg("Transaction accepted and published to Kafka")

	c.JSON(http.StatusAccepted, gin.H{
		"id":      txID,
		"message": "Transaksi diterima dan akan diproses async (status pending)",
	})
}

func (h *TransactionHandler) GetByTxID(c *gin.Context) {
	logger := logging.GetLogger(c)

	txID := c.Param("txId")
	cacheKey := "tx:" + txID

	if cached, found := cache.GetCached[domain.TransactionDetail](c.Request.Context(), cacheKey); found {
		logger.Info().
			Str("tx_id", txID).
			Str("status", cached.Status).
			Msg("GET /transactions → CACHE HIT (Redis)")
		c.JSON(http.StatusOK, cached)
		return
	}

	logger.Info().
		Str("tx_id", txID).
		Msg("GET /transactions → CACHE MISS, cek ke PostgreSQL")

	detail, err := h.service.GetByTxID(c.Request.Context(), txID)
	if err != nil {
		if strings.Contains(err.Error(), "circuit breaker") || strings.Contains(err.Error(), "rejected") {
			logger.Warn().
				Err(err).
				Str("tx_id", txID).
				Msg("Breaker reject di GetByTxID")
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":   "Sistem sedang overload atau database sibuk",
				"message": "Transaksi sedang diproses, coba lagi dalam beberapa detik",
				"status":  "processing",
			})
			return
		}

		if err.Error() == "transaction not found" || strings.Contains(err.Error(), "no rows") {
			logger.Info().
				Str("tx_id", txID).
				Msg("Transaction belum ada di DB, masih processing")
			c.JSON(http.StatusOK, gin.H{
				"tx_id":   txID,
				"status":  "processing",
				"message": "Transaksi sedang diproses, coba lagi dalam beberapa detik",
			})
			return
		}

		logger.Error().
			Err(err).
			Str("tx_id", txID).
			Msg("Gagal query DB untuk transaction")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get transaction", "detail": err.Error()})
		return
	}

	cache.SetCache(c.Request.Context(), cacheKey, detail, 5*time.Minute)

	logger.Info().
		Str("tx_id", txID).
		Str("status", detail.Status).
		Msg("GET /transactions → success from PostgreSQL")

	c.JSON(http.StatusOK, detail)
}

func (h *TransactionHandler) GetUserBalance(c *gin.Context) {
	logger := logging.GetLogger(c)

	userIDStr := c.Param("id")
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		logger.Warn().
			Err(err).
			Str("user_id_str", userIDStr).
			Msg("Invalid user ID")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid user ID"})
		return
	}

	cacheKey := "user_balance:" + userIDStr

	if cached, found := cache.GetCached[domain.UserBalance](c.Request.Context(), cacheKey); found {
		logger.Info().
			Int64("user_id", userID).
			Float64("balance", cached.Balance).
			Msg("GET /users/:id/balance → CACHE HIT (Redis)")
		c.JSON(http.StatusOK, cached)
		return
	}

	logger.Info().
		Int64("user_id", userID).
		Msg("GET /users/:id/balance → CACHE MISS, ambil dari PostgreSQL")

	balance, err := h.service.GetUserBalance(c.Request.Context(), userID)
	if err != nil {
		if strings.Contains(err.Error(), "circuit breaker") || strings.Contains(err.Error(), "rejected") {
			logger.Warn().
				Err(err).
				Int64("user_id", userID).
				Msg("Breaker reject di GetUserBalance")
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":   "Sistem sedang overload atau database sibuk",
				"message": "Saldo sedang diproses, coba lagi dalam beberapa detik",
			})
			return
		}

		if err.Error() == "user not found" {
			logger.Info().
				Int64("user_id", userID).
				Msg("User not found")
			c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
			return
		}

		logger.Error().
			Err(err).
			Int64("user_id", userID).
			Msg("Gagal get balance user")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get balance", "detail": err.Error()})
		return
	}

	cache.SetCache(c.Request.Context(), cacheKey, balance, 1*time.Minute)

	logger.Info().
		Int64("user_id", userID).
		Float64("balance", balance.Balance).
		Msg("GET /users/:id/balance → success from PostgreSQL")

	c.JSON(http.StatusOK, balance)
}
