package handler

import (
	"log"
	"net/http"
	"strings"

	"github.com/capstone-b4/capstone-go/internal/application"
	"github.com/capstone-b4/capstone-go/internal/domain"
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

	// Validasi transfer
	if input.Type == "transfer" && input.RecipientID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "recipient_id wajib untuk transfer"})
		return
	}

	// Generate tx_id UUID (unik tanpa DB)
	txID := uuid.New().String()

	// Publish ke Kafka dulu (fire-and-forget)
	err := queue.PublishTransactionEvent(txID, input.UserID, input.RecipientID, input.Amount, input.Type)
	if err != nil {
		log.Printf("Gagal publish Kafka: %v", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Sistem sedang sibuk, coba lagi nanti"})
		return
	}

	// Return cepat ke user (Accepted 202 lebih tepat untuk async processing)
	c.JSON(http.StatusAccepted, gin.H{
		"id":      txID,
		"message": "Transaksi diterima dan akan diproses async (status pending)",
	})
}

func (h *TransactionHandler) GetByTxID(c *gin.Context) {
	txID := c.Param("id")
	if txID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tx_id required"})
		return
	}

	detail, err := h.service.GetByTxID(c.Request.Context(), txID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			c.JSON(http.StatusNotFound, gin.H{"error": "Transaction not found"})
		} else {
			log.Printf("Gagal get transaction %s: %v", txID, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get transaction"})
		}
		return
	}

	c.JSON(http.StatusOK, detail)
}
