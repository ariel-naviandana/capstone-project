package handler

import (
	"errors"
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
	"github.com/redis/go-redis/v9"

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

	if input.Type == "transfer" && input.RecipientNo == "" {
		logger.Warn().Msg("Missing recipient_no for transfer")
		c.JSON(http.StatusBadRequest, response.ErrorJSON(response.ErrInvalidInput, "recipient_no wajib untuk transfer", ""))
		return
	}

	if input.Type == "transfer" && input.AccountNo == input.RecipientNo {
		logger.Warn().Msg("Self-transfer not allowed")
		c.JSON(http.StatusBadRequest, response.ErrorJSON(response.ErrInvalidInput, "tidak bisa transfer ke rekening sendiri", ""))
		return
	}

	txID := "TRX-" + time.Now().Format("20060102150405") + "-" + uuid.NewString()[:6]

	// Idempotency-by-ref_no: when the client supplies ref_no, claim
	// idem:<ref_no> -> trx_id in Redis with SETNX. A retry of the same ref_no
	// returns the previously-issued trx_id instead of producing a duplicate
	// transaction. Empty ref_no means the client is opting out of dedup.
	idemClaimed := false
	idemKey := ""
	if input.RefNo != "" {
		idemKey = "idem:tx:" + input.RefNo
		// SetArgs with Mode "NX" replaces deprecated SetNX. On miss (key
		// already set), Redis replies nil and the client surfaces redis.Nil.
		_, claimErr := cache.RedisClient.SetArgs(c.Request.Context(), idemKey, txID, redis.SetArgs{
			Mode: "NX",
			TTL:  24 * time.Hour,
		}).Result()
		switch {
		case claimErr == nil:
			idemClaimed = true
		case errors.Is(claimErr, redis.Nil):
			existing, getErr := cache.RedisClient.Get(c.Request.Context(), idemKey).Result()
			if getErr == nil && existing != "" {
				logger.Info().Str("ref_no", input.RefNo).Str("existing_tx_id", existing).Msg("Duplicate ref_no; returning existing trx_id")
				c.JSON(http.StatusOK, response.SuccessJSON("Transaksi dengan ref_no ini sudah pernah diterima", gin.H{
					"trx_id":     existing,
					"idempotent": true,
				}))
				return
			}
			// Race: claim disappeared between SET NX and GET. Treat as new.
		default:
			logger.Warn().Err(claimErr).Str("ref_no", input.RefNo).Msg("Idempotency SET NX failed; proceeding without claim")
		}
	}

	traceID := c.GetString("trace_id")
	_, err := resilience.ExecuteWithBreaker(
		c.Request.Context(),
		resilience.KafkaProducerBreaker,
		"KafkaPublish",
		func() (struct{}, error) {
			return struct{}{}, queue.PublishTransactionEvent(txID, input.AccountNo, input.RecipientNo, input.Amount, input.Type, input.RefNo, traceID)
		},
	)
	if err != nil {
		// Roll back the idempotency claim so the client can retry without
		// being told their tx is "already processed" when nothing was queued.
		if idemClaimed {
			if delErr := cache.RedisClient.Del(c.Request.Context(), idemKey).Err(); delErr != nil {
				logger.Warn().Err(delErr).Str("ref_no", input.RefNo).Msg("Failed to release idempotency claim after publish failure")
			}
		}
		logger.Warn().
			Err(err).
			Str("tx_id", txID).
			Str("type", input.Type).
			Msg("Kafka publish gagal")
		c.JSON(http.StatusServiceUnavailable, response.ErrorJSON(
			response.ErrServiceUnavailable,
			"Sistem sedang overload atau Kafka tidak tersedia",
			"Coba lagi dalam beberapa detik",
		))
		return
	}

	c.JSON(http.StatusAccepted, response.SuccessJSON("Transaksi diterima dan akan diproses async (status pending)", gin.H{
		"trx_id": txID,
	}))
}

func (h *TransactionHandler) GetByTxID(c *gin.Context) {
	logger := logging.GetLogger(c)

	txID := c.Param("txId")

	detail, err := h.service.GetByTxID(c.Request.Context(), txID)

	if err != nil {
		if strings.Contains(err.Error(), "transaction not found") || strings.Contains(err.Error(), "no rows") {
			c.JSON(http.StatusOK, response.SuccessJSON("Transaksi sedang diproses, coba lagi dalam beberapa detik", gin.H{
				"trx_id": txID,
				"status": "processing",
			}))
			return
		}
		logger.Error().Err(err).Str("tx_id", txID).Msg("Gagal query DB untuk transaction")
		c.JSON(http.StatusInternalServerError, response.ErrorJSON(response.ErrInternalError, "Failed to get transaction", err.Error()))
		return
	}

	c.JSON(http.StatusOK, response.SuccessJSON("Succeed", detail))
}

func (h *TransactionHandler) GetAccountBalance(c *gin.Context) {
	logger := logging.GetLogger(c)

	accountNo := c.Param("accountNo")
	if accountNo == "" {
		c.JSON(http.StatusBadRequest, response.ErrorJSON(response.ErrInvalidInput, "Invalid account NO", ""))
		return
	}

	balance, err := h.service.GetAccountBalance(c.Request.Context(), accountNo)

	if err != nil {
		if strings.Contains(err.Error(), "account not found") {
			c.JSON(http.StatusNotFound, response.ErrorJSON(response.ErrNotFound, "Account not found", ""))
			return
		}
		logger.Error().Err(err).Str("account_no", accountNo).Msg("Gagal get balance account")
		c.JSON(http.StatusInternalServerError, response.ErrorJSON(response.ErrInternalError, "Failed to get balance", err.Error()))
		return
	}

	if balance == nil {
		c.JSON(http.StatusNotFound, response.ErrorJSON(response.ErrNotFound, "Account not found", ""))
		return
	}

	c.JSON(http.StatusOK, response.SuccessJSON("Succeed", balance))
}

func (h *TransactionHandler) GetAccountTransactions(c *gin.Context) {
	logger := logging.GetLogger(c)

	accountNo := c.Param("accountNo")
	if accountNo == "" {
		c.JSON(http.StatusBadRequest, response.ErrorJSON(response.ErrInvalidInput, "Invalid account NO", ""))
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

	transactions, err := h.service.GetAccountTransactions(c.Request.Context(), accountNo, limit, offset)
	if err != nil {
		logger.Error().Err(err).Str("account_no", accountNo).Msg("Failed to list account transactions")
		c.JSON(http.StatusInternalServerError, response.ErrorJSON(response.ErrInternalError, "Gagal mengambil daftar transaksi", err.Error()))
		return
	}

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
	cache.SetCache(c.Request.Context(), cacheKey, transactions, 15*time.Second)

	c.JSON(http.StatusOK, response.SuccessJSON("Succeed", transactions))
}
	c.JSON(http.StatusOK, response.SuccessJSON("Succeed", transactions))
}
