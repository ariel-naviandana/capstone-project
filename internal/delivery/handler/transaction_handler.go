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
	"github.com/capstone-b4/capstone-go/internal/infrastructure/observability"
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
	if input.RefNo == "" {
		input.RefNo = "REF-" + uuid.NewString()[:8]
	}

	traceID := c.GetString("trace_id")
	_, err := resilience.ExecuteWithBreaker(
		c.Request.Context(),
		resilience.KafkaProducerBreaker,
		"KafkaPublish",
		func() (struct{}, error) {
			return struct{}{}, queue.PublishTransactionEvent(txID, input.AccountNo, input.RecipientNo, input.Amount, input.Type, traceID)
		},
	)
	if err != nil {
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
	cacheKey := "tx:" + txID

	if cached, found := cache.GetCached[domain.TransactionDetail](c.Request.Context(), cacheKey); found {
		c.JSON(http.StatusOK, response.SuccessJSON("Succeed", cached))
		return
	}

	detail, err := h.service.GetByTxID(c.Request.Context(), txID)
	if err != nil {
		if strings.Contains(err.Error(), "circuit breaker") || strings.Contains(err.Error(), "rejected") {
			observability.RequestsRejectedTotal.WithLabelValues("breaker", "/transactions/:txId").Inc()
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
			c.JSON(http.StatusOK, response.SuccessJSON("Transaksi sedang diproses, coba lagi dalam beberapa detik", gin.H{
				"trx_id": txID,
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

	c.JSON(http.StatusOK, response.SuccessJSON("Succeed", detail))
}

func (h *TransactionHandler) GetAccountBalance(c *gin.Context) {
	logger := logging.GetLogger(c)

	accountNo := c.Param("accountNo")
	if accountNo == "" {
		c.JSON(http.StatusBadRequest, response.ErrorJSON(response.ErrInvalidInput, "Invalid account NO", ""))
		return
	}

	cacheKey := "account_balance:" + accountNo

	if cached, found := cache.GetCached[domain.AccountBalance](c.Request.Context(), cacheKey); found {
		c.JSON(http.StatusOK, response.SuccessJSON("Succeed", cached))
		return
	}

	balance, err := h.service.GetAccountBalance(c.Request.Context(), accountNo)
	if err != nil {
		if strings.Contains(err.Error(), "circuit breaker") || strings.Contains(err.Error(), "rejected") {
			observability.RequestsRejectedTotal.WithLabelValues("breaker", "/accounts/:accountNo/balance").Inc()
			logger.Warn().
				Err(err).
				Str("account_no", accountNo).
				Msg("Breaker reject di GetAccountBalance")
			c.JSON(http.StatusServiceUnavailable, response.ErrorJSON(
				response.ErrServiceUnavailable,
				"Sistem sedang overload atau database sibuk (Saldo sedang diproses)",
				err.Error(),
			))
			return
		}

		if strings.Contains(err.Error(), "account not found") {
			c.JSON(http.StatusNotFound, response.ErrorJSON(response.ErrNotFound, "Account not found", ""))
			return
		}

		logger.Error().
			Err(err).
			Str("account_no", accountNo).
			Msg("Gagal get balance account")
		c.JSON(http.StatusInternalServerError, response.ErrorJSON(response.ErrInternalError, "Failed to get balance", err.Error()))
		return
	}

	if err := cache.SetCache(c.Request.Context(), cacheKey, balance, 10*time.Minute); err != nil {
		logger.Warn().Err(err).Str("account_no", accountNo).Msg("Failed to set balance cache")
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
