package handler

import (
	"log"
	"net/http"

	"github.com/capstone-b4/capstone-go/internal/application"
	"github.com/capstone-b4/capstone-go/internal/domain"

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
		log.Printf("Binding error: %v", err) // tambah log
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	id, err := h.service.CreateTransaction(c.Request.Context(), &input)
	if err != nil {
		log.Printf("Create transaction error: %v", err) // log detail error
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create transaction", "detail": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":      id,
		"message": "Transaction created successfully (pending)",
	})
}
