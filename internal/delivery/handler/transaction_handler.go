package handler

import (
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/capstone-b4/capstone-go/internal/application"
	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/queue"
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
	var input domain.TransactionCreate
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input", "detail": err.Error()})
		return
	}

	if input.Type == "transfer" && input.RecipientID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "recipient_id wajib untuk transfer"})
		return
	}

	txID := uuid.New().String()

	err := queue.PublishTransactionEvent(txID, input.UserID, input.RecipientID, input.Amount, input.Type)
	if err != nil {
		log.Printf("Gagal publish Kafka: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Sistem sibuk, coba lagi nanti"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"id":      txID,
		"message": "Transaksi diterima dan akan diproses async (status pending)",
	})
}

func (h *TransactionHandler) GetByTxID(c *gin.Context) {
	txID := c.Param("txId")

	cacheKey := "tx:" + txID

	if cached, found := cache.GetCached[domain.TransactionDetail](c.Request.Context(), cacheKey); found {
		log.Printf("GET /transactions/%s → CACHE HIT (Redis), status: %s", txID, cached.Status)
		c.JSON(http.StatusOK, cached)
		return
	}

	log.Printf("GET /transactions/%s → CACHE MISS, cek ke PostgreSQL", txID)

	detail, err := h.service.GetByTxID(c.Request.Context(), txID)
	if err != nil {
		if err.Error() == "transaction not found" || strings.Contains(err.Error(), "no rows") {
			log.Printf("Transaction %s belum ada di DB, masih processing", txID)
			c.JSON(http.StatusOK, gin.H{
				"tx_id":   txID,
				"status":  "pending",
				"message": "Transaksi sedang diproses, coba lagi dalam beberapa detik",
			})
			return
		}

		log.Printf("Gagal query DB untuk %s: %v", txID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get transaction", "detail": err.Error()})
		return
	}

	cache.SetCache(c.Request.Context(), cacheKey, detail, 5*time.Minute)

	c.JSON(http.StatusOK, detail)
}

func (h *TransactionHandler) GetUserBalance(c *gin.Context) {
	userIDStr := c.Param("id")
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid user ID"})
		return
	}

	cacheKey := "user_balance:" + userIDStr

	if cached, found := cache.GetCached[domain.UserBalance](c.Request.Context(), cacheKey); found {
		log.Printf("GET /users/%d/balance → CACHE HIT (Redis), balance: %.2f", userID, cached.Balance)
		c.JSON(http.StatusOK, cached)
		return
	}

	log.Printf("GET /users/%d/balance → CACHE MISS, ambil dari PostgreSQL", userID)

	balance, err := h.service.GetUserBalance(c.Request.Context(), userID)
	if err != nil {
		if err.Error() == "user not found" {
			c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get balance"})
		}
		return
	}

	cache.SetCache(c.Request.Context(), cacheKey, balance, 1*time.Minute)

	c.JSON(http.StatusOK, balance)
}
