package handler

import (
	"log"
	"net/http"

	"github.com/capstone-b4/capstone-go/internal/application"
	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/queue"

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

	// Validasi tambahan: transfer wajib recipient_id
	if input.Type == "transfer" && input.RecipientID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "recipient_id wajib untuk type transfer"})
		return
	}

	// Insert ke DB (asumsi service return txID)
	txID, err := h.service.CreateTransaction(c.Request.Context(), &input)
	if err != nil {
		log.Printf("Gagal create transaction: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal membuat transaksi", "detail": err.Error()})
		return
	}

	// Publish ke Kafka (non-blocking, tidak gagalkan response kalau Kafka down)
	err = queue.PublishTransactionEvent(txID, input.UserID, input.RecipientID, input.Amount, input.Type)
	if err != nil {
		log.Printf("Kafka publish gagal (transaksi tetap dibuat): %v", err)
		// Tidak return error ke client, biar API tetap responsif
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":      txID,
		"message": "Transaksi dibuat berhasil (pending, diproses async)",
	})
}
