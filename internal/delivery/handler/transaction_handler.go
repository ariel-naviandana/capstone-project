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
	"github.com/capstone-b4/capstone-go/internal/pkg/response"
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
		c.JSON(http.StatusBadRequest, response.ErrorJSON(response.ErrInvalidInput, "Invalid input", err.Error()))
		return
	}

	if input.Type == "transfer" && input.RecipientID == 0 {
		logger.Warn().Msg("Missing recipient_id for transfer")
		c.JSON(http.StatusBadRequest, response.ErrorJSON(response.ErrInvalidInput, "recipient_id wajib untuk transfer", ""))
		return
	}

	if input.Type == "transfer" && input.UserID == input.RecipientID {
		logger.Warn().Msg("Self-transfer not allowed")
		c.JSON(http.StatusBadRequest, response.ErrorJSON(response.ErrInvalidInput, "tidak bisa transfer ke diri sendiri", ""))
		return
	}

	txID := uuid.New().String()

	traceID := c.GetString("trace_id")
	_, err := resilience.ExecuteWithBreaker(
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
		c.JSON(http.StatusServiceUnavailable, response.ErrorJSON(
			response.ErrServiceUnavailable,
			"Sistem sedang overload atau Kafka tidak tersedia",
			"Coba lagi dalam beberapa detik",
		))
		return
	}

	logger.Info().
		Str("tx_id", txID).
		Str("type", input.Type).
		Int64("user_id", input.UserID).
		Float64("amount", input.Amount).
		Msg("Transaction accepted and published to Kafka")

	c.JSON(http.StatusAccepted, response.SuccessJSON("Transaksi diterima dan akan diproses async (status pending)", gin.H{
		"id": txID,
	}))
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
		c.JSON(http.StatusOK, response.SuccessJSON("Succeed", cached))
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
			c.JSON(http.StatusServiceUnavailable, response.ErrorJSON(
				response.ErrServiceUnavailable,
				"Sistem sedang overload atau database sibuk (transaksi sedang diproses)",
				err.Error(),
			))
			return
		}

		if strings.Contains(err.Error(), "transaction not found") || strings.Contains(err.Error(), "no rows") {
			logger.Info().
				Str("tx_id", txID).
				Msg("Transaction belum ada di DB, masih processing")
			c.JSON(http.StatusOK, response.SuccessJSON("Transaksi sedang diproses, coba lagi dalam beberapa detik", gin.H{
				"tx_id":  txID,
				"status": "processing",
			}))
			return
		}

		logger.Error().
			Err(err).
			Str("tx_id", txID).
			Msg("Gagal query DB untuk transaction")
		c.JSON(http.StatusInternalServerError, response.ErrorJSON(response.ErrInternalError, "Failed to get transaction", err.Error()))
		return
	}

	if err := cache.SetCache(c.Request.Context(), cacheKey, detail, 5*time.Minute); err != nil {
		logger.Warn().Err(err).Str("tx_id", txID).Msg("Failed to set transaction cache")
	}

	logger.Info().
		Str("tx_id", txID).
		Str("status", detail.Status).
		Msg("GET /transactions → success from PostgreSQL")

	c.JSON(http.StatusOK, response.SuccessJSON("Succeed", detail))
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
		c.JSON(http.StatusBadRequest, response.ErrorJSON(response.ErrInvalidInput, "Invalid user ID", ""))
		return
	}

	cacheKey := "user_balance:" + userIDStr

	if cached, found := cache.GetCached[domain.UserBalance](c.Request.Context(), cacheKey); found {
		logger.Info().
			Int64("user_id", userID).
			Float64("balance", cached.Balance).
			Msg("GET /users/:id/balance → CACHE HIT (Redis)")
		c.JSON(http.StatusOK, response.SuccessJSON("Succeed", cached))
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
			c.JSON(http.StatusServiceUnavailable, response.ErrorJSON(
				response.ErrServiceUnavailable,
				"Sistem sedang overload atau database sibuk (Saldo sedang diproses)",
				err.Error(),
			))
			return
		}

		if strings.Contains(err.Error(), "user not found") {
			logger.Info().
				Int64("user_id", userID).
				Msg("User not found")
			c.JSON(http.StatusNotFound, response.ErrorJSON(response.ErrNotFound, "User not found", ""))
			return
		}

		logger.Error().
			Err(err).
			Int64("user_id", userID).
			Msg("Gagal get balance user")
		c.JSON(http.StatusInternalServerError, response.ErrorJSON(response.ErrInternalError, "Failed to get balance", err.Error()))
		return
	}

	if err := cache.SetCache(c.Request.Context(), cacheKey, balance, 10*time.Minute); err != nil {
		logger.Warn().Err(err).Int64("user_id", userID).Msg("Failed to set balance cache")
	}

	logger.Info().
		Int64("user_id", userID).
		Float64("balance", balance.Balance).
		Msg("GET /users/:id/balance → success from PostgreSQL")

	c.JSON(http.StatusOK, response.SuccessJSON("Succeed", balance))
}

func (h *TransactionHandler) GetUserTransactions(c *gin.Context) {
	logger := logging.GetLogger(c)

	userIDStr := c.Param("id")
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		logger.Warn().Err(err).Str("user_id_str", userIDStr).Msg("Invalid user ID")
		c.JSON(http.StatusBadRequest, response.ErrorJSON(response.ErrInvalidInput, "Invalid user ID", ""))
		return
	}

	limitStr := c.DefaultQuery("limit", "10")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 10
	}

	offsetStr := c.DefaultQuery("offset", "0")
	offset, err := strconv.Atoi(offsetStr)
	if err != nil || offset < 0 {
		offset = 0
	}

	// Cache short-lived (opsional)
	cacheKey := "list_tx:" + userIDStr + "?limit=" + strconv.Itoa(limit) + "&offset=" + strconv.Itoa(offset)
	if cached, found := cache.GetCached[[]*domain.TransactionDetail](c.Request.Context(), cacheKey); found {
		logger.Info().Int64("user_id", userID).Msg("GET /users/:id/transactions → CACHE HIT")
		c.JSON(http.StatusOK, response.SuccessJSON("Succeed", cached))
		return
	}

	logger.Info().Int64("user_id", userID).Msg("GET /users/:id/transactions → CACHE MISS")
	transactions, err := h.service.GetUserTransactions(c.Request.Context(), userID, limit, offset)
	if err != nil {
		if strings.Contains(err.Error(), "circuit breaker") || strings.Contains(err.Error(), "rejected") {
			c.JSON(http.StatusServiceUnavailable, response.ErrorJSON(response.ErrServiceUnavailable, "Sistem overload", err.Error()))
			return
		}
		logger.Error().Err(err).Int64("user_id", userID).Msg("Failed to list user transactions")
		c.JSON(http.StatusInternalServerError, response.ErrorJSON(response.ErrInternalError, "Gagal mengambil daftar transaksi", err.Error()))
		return
	}

	// Set cache dgn TTL sangat pendek agar tdk terlalu basi, tapi melindung DB dari refresh-spam user
	if err := cache.SetCache(c.Request.Context(), cacheKey, transactions, 15*time.Second); err != nil {
		logger.Warn().Err(err).Int64("user_id", userID).Msg("Failed to set transactions list cache")
	}

	c.JSON(http.StatusOK, response.SuccessJSON("Succeed", transactions))
}
