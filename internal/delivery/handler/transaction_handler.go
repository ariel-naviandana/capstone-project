package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/capstone-b4/capstone-go/internal/application"
	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/logging"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/observability"
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
		observability.RequestsRejectedTotal.WithLabelValues("breaker", "/transactions").Inc()
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

	c.JSON(http.StatusAccepted, response.SuccessJSON("Transaksi diterima dan akan diproses async (status pending)", gin.H{
		"trx_id": txID,
	}))
}

func (h *TransactionHandler) GetByTxID(c *gin.Context) {
	logger := logging.GetLogger(c)

	txID := c.Param("txId")

	detail, bytes, err := cache.TxLayer.GetOrFetch(c.Request.Context(), txID,
		func(ctx context.Context) (*domain.TransactionDetail, error) {
			d, ferr := h.service.GetByTxID(ctx, txID)
			if ferr != nil && (strings.Contains(ferr.Error(), "transaction not found") || strings.Contains(ferr.Error(), "no rows")) {
				return nil, cache.ErrNotFound
			}
			return d, ferr
		},
	)

	if err != nil {
		if errors.Is(err, cache.ErrNotFound) {
			// Tx hasn't landed in DB yet — worker may still be processing.
			c.JSON(http.StatusOK, response.SuccessJSON("Transaksi sedang diproses, coba lagi dalam beberapa detik", gin.H{
				"trx_id": txID,
				"status": "processing",
			}))
			return
		}
		if strings.Contains(err.Error(), "circuit breaker") || strings.Contains(err.Error(), "rejected") {
			observability.RequestsRejectedTotal.WithLabelValues("breaker", "/transactions/:txId").Inc()
			logger.Warn().Err(err).Str("tx_id", txID).Msg("Breaker reject di GetByTxID")
			c.JSON(http.StatusServiceUnavailable, response.ErrorJSON(
				response.ErrServiceUnavailable,
				"Sistem sedang overload atau database sibuk (transaksi sedang diproses)",
				err.Error(),
			))
			return
		}
		logger.Error().Err(err).Str("tx_id", txID).Msg("Gagal query DB untuk transaction")
		c.JSON(http.StatusInternalServerError, response.ErrorJSON(response.ErrInternalError, "Failed to get transaction", err.Error()))
		return
	}

	if bytes != nil {
		c.Data(http.StatusOK, "application/json; charset=utf-8", buildSuccessBody(bytes))
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

	balance, bytes, err := cache.BalanceLayer.GetOrFetch(c.Request.Context(), accountNo,
		func(ctx context.Context) (*domain.AccountBalance, error) {
			b, ferr := h.service.GetAccountBalance(ctx, accountNo)
			if ferr != nil && strings.Contains(ferr.Error(), "account not found") {
				return nil, cache.ErrNotFound
			}
			return b, ferr
		},
	)

	if err != nil {
		if errors.Is(err, cache.ErrNotFound) {
			c.JSON(http.StatusNotFound, response.ErrorJSON(response.ErrNotFound, "Account not found", ""))
			return
		}
		if strings.Contains(err.Error(), "circuit breaker") || strings.Contains(err.Error(), "rejected") {
			observability.RequestsRejectedTotal.WithLabelValues("breaker", "/accounts/:accountNo/balance").Inc()
			logger.Warn().Err(err).Str("account_no", accountNo).Msg("Breaker reject di GetAccountBalance")
			c.JSON(http.StatusServiceUnavailable, response.ErrorJSON(
				response.ErrServiceUnavailable,
				"Sistem sedang overload atau database sibuk (Saldo sedang diproses)",
				err.Error(),
			))
			return
		}
		logger.Error().Err(err).Str("account_no", accountNo).Msg("Gagal get balance account")
		c.JSON(http.StatusInternalServerError, response.ErrorJSON(response.ErrInternalError, "Failed to get balance", err.Error()))
		return
	}

	if bytes != nil {
		// Fast path: pre-marshaled balance body is already in cache, so we
		// only have to splice it into the success envelope. Skips the
		// per-request domain.AccountBalance -> JSON encode entirely.
		c.Data(http.StatusOK, "application/json; charset=utf-8", buildSuccessBody(bytes))
		return
	}
	c.JSON(http.StatusOK, response.SuccessJSON("Succeed", balance))
}

// successPrefix / successSuffix surround the cached data bytes to produce the
// same wire format as response.SuccessResponse{Message: "Succeed", Data: ...}.
var (
	successPrefix = []byte(`{"message":"Succeed","data":`)
	successSuffix = []byte(`}`)
)

func buildSuccessBody(data []byte) []byte {
	out := make([]byte, 0, len(successPrefix)+len(data)+len(successSuffix))
	out = append(out, successPrefix...)
	out = append(out, data...)
	out = append(out, successSuffix...)
	return out
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

	cacheKey := "list_tx:" + accountNo + "?limit=" + strconv.Itoa(limit) + "&offset=" + strconv.Itoa(offset)
	if cached, found := cache.GetCached[[]*domain.TransactionDetail](c.Request.Context(), cacheKey); found {
		c.JSON(http.StatusOK, response.SuccessJSON("Succeed", cached))
		return
	}

	transactions, err := h.service.GetAccountTransactions(c.Request.Context(), accountNo, limit, offset)
	if err != nil {
		if strings.Contains(err.Error(), "circuit breaker") || strings.Contains(err.Error(), "rejected") {
			observability.RequestsRejectedTotal.WithLabelValues("breaker", "/accounts/:accountNo/transactions").Inc()
			c.JSON(http.StatusServiceUnavailable, response.ErrorJSON(response.ErrServiceUnavailable, "Sistem overload", err.Error()))
			return
		}
		logger.Error().Err(err).Str("account_no", accountNo).Msg("Failed to list account transactions")
		c.JSON(http.StatusInternalServerError, response.ErrorJSON(response.ErrInternalError, "Gagal mengambil daftar transaksi", err.Error()))
		return
	}

	if err := cache.SetCache(c.Request.Context(), cacheKey, transactions, 15*time.Second); err != nil {
		logger.Warn().Err(err).Str("account_no", accountNo).Msg("Failed to set transactions list cache")
	}

	c.JSON(http.StatusOK, response.SuccessJSON("Succeed", transactions))
}