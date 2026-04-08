package handler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/capstone-b4/capstone-go/internal/application"
	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/domain/mocks"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/cache"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func setupTestServer() (*gin.Engine, *mocks.MockTransactionRepository, *miniredis.Miniredis) {
	mr, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	cache.RedisClient = redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	mockRepo := new(mocks.MockTransactionRepository)
	service := application.NewTransactionService(mockRepo)
	txHandler := NewTransactionHandler(service)

	gin.SetMode(gin.TestMode)
	r := gin.New()

	api := r.Group("/")
	{
		api.POST("/transactions", txHandler.Create)
		api.GET("/transactions/:txId", txHandler.GetByTxID)
		api.GET("/users/:id/balance", txHandler.GetUserBalance)
		api.GET("/users/:id/transactions", txHandler.GetUserTransactions)
	}

	return r, mockRepo, mr
}

func TestTransactionHandler_GetUserBalance(t *testing.T) {
	router, mockRepo, mr := setupTestServer()
	defer mr.Close()

	t.Run("Success - Cache Miss", func(t *testing.T) {
		mr.FlushAll()
		
		expectedBalance := &domain.UserBalance{
			ID:       1,
			Username: "user1",
			Balance:  100000,
		}

		mockRepo.On("GetUserBalance", mock.Anything, int64(1)).Return(expectedBalance, nil).Once()

		req, _ := http.NewRequest(http.MethodGet, "/users/1/balance", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, resp.Body.String(), "100000")
		mockRepo.AssertExpectations(t)
	})

	t.Run("Success - Cache Hit", func(t *testing.T) {
		mr.FlushAll()
		cachedData := &domain.UserBalance{
			ID:       1,
			Username: "user1",
			Balance:  50000,
		}
		err := cache.SetCache(context.Background(), "user_balance:1", cachedData, time.Minute)
		assert.NoError(t, err)

		req, _ := http.NewRequest(http.MethodGet, "/users/1/balance", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, resp.Body.String(), "50000")
		// Pastikan Mock Repo tidak dipanggil karena di stop oleh Redis Cache
		mockRepo.AssertNotCalled(t, "GetUserBalance") 
	})

	t.Run("Error - Invalid Format", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "/users/abc/balance", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, resp.Body.String(), "ERR_INVALID_INPUT")
	})
}

func TestTransactionHandler_GetUserTransactions(t *testing.T) {
	router, mockRepo, mr := setupTestServer()
	defer mr.Close()

	t.Run("Success - Query with Default Pagination", func(t *testing.T) {
		mr.FlushAll()
		expectedTx := []*domain.TransactionDetail{
			{ID: 1, Amount: 100},
			{ID: 2, Amount: 200},
		}

		// default limit=10, offset=0
		mockRepo.On("GetUserTransactions", mock.Anything, int64(2), 10, 0).Return(expectedTx, nil).Once()

		req, _ := http.NewRequest(http.MethodGet, "/users/2/transactions", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, resp.Body.String(), "Succeed")
		mockRepo.AssertExpectations(t)
	})
}

func TestTransactionHandler_CreateTransaction(t *testing.T) {
	router, _, mr := setupTestServer()
	defer mr.Close()

	t.Run("Error - Missing Recipient for Transfer", func(t *testing.T) {
		body := []byte(`{"user_id":1,"amount":50000,"type":"transfer"}`)
		req, _ := http.NewRequest(http.MethodPost, "/transactions", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, resp.Body.String(), "recipient_id wajib")
	})
	
	// Kita tidak mengetes sukses create karena Handler men-trigger Kafka (gobreaker.ExecuteWithBreaker), 
	// Di mana mock Kafka cukup rumit. Sehingga cukup memastikan request validation berjalan.
}
