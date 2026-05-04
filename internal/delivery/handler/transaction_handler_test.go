package handler

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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
	cache.InitLayers()

	mockRepo := new(mocks.MockTransactionRepository)
	service := application.NewTransactionService(mockRepo)
	txHandler := NewTransactionHandler(service)

	gin.SetMode(gin.TestMode)
	r := gin.New()

	api := r.Group("/")
	{
		api.POST("/transactions", txHandler.Create)
		api.GET("/transactions/:txId", txHandler.GetByTxID)
		api.GET("/accounts/:accountNo/balance", txHandler.GetAccountBalance)
		api.GET("/accounts/:accountNo/transactions", txHandler.GetAccountTransactions)
	}

	return r, mockRepo, mr
}

func TestTransactionHandler_GetAccountBalance(t *testing.T) {
	router, mockRepo, mr := setupTestServer()
	defer mr.Close()

	t.Run("Success - Cache Miss", func(t *testing.T) {
		mr.FlushAll()

		expectedBalance := &domain.AccountBalance{
			AccountNo: "ACC-1",
			Balance:   100000,
		}

		mockRepo.On("GetAccountBalance", mock.Anything, "ACC-1").Return(expectedBalance, nil).Once()

		req, _ := http.NewRequest(http.MethodGet, "/accounts/ACC-1/balance", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, resp.Body.String(), "100000")
		mockRepo.AssertExpectations(t)
	})

	t.Run("Success - Cache Hit", func(t *testing.T) {
		mr.FlushAll()
		cachedData := &domain.AccountBalance{
			AccountNo: "ACC-1",
			Balance:   50000,
		}
		cache.BalanceLayer.WriteThrough(context.Background(), "ACC-1", cachedData)

		req, _ := http.NewRequest(http.MethodGet, "/accounts/ACC-1/balance", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, resp.Body.String(), "50000")
		// Pastikan Mock Repo tidak dipanggil karena di stop oleh Redis Cache
		mockRepo.AssertNotCalled(t, "GetAccountBalance")
	})

	// Di versi baru accountNo string bebas. Kita bs tes param kosong / malformed
	// t.Run("Error - Invalid Format", ...)
}

func TestTransactionHandler_GetAccountTransactions(t *testing.T) {
	router, mockRepo, mr := setupTestServer()
	defer mr.Close()

	t.Run("Success - Query with Default Pagination", func(t *testing.T) {
		mr.FlushAll()
		expectedTx := []*domain.TransactionDetail{
			{TrxID: "trx-1", Amount: 100},
			{TrxID: "trx-2", Amount: 200},
		}

		// default limit=10, offset=0
		mockRepo.On("GetAccountTransactions", mock.Anything, "ACC-2", 10, 0).Return(expectedTx, nil).Once()

		req, _ := http.NewRequest(http.MethodGet, "/accounts/ACC-2/transactions", nil)
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
		body := []byte(`{"account_no":"ACC-1","amount":50000,"type":"transfer"}`)
		req, _ := http.NewRequest(http.MethodPost, "/transactions", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, resp.Body.String(), "recipient_no wajib")
	})

	// Kita tidak mengetes sukses create karena Handler men-trigger Kafka (gobreaker.ExecuteWithBreaker),
	// Di mana mock Kafka cukup rumit. Sehingga cukup memastikan request validation berjalan.
	// ==================== TAMBAHAN ====================

	t.Run("Error - Self Transfer Not Allowed", func(t *testing.T) {
		body := []byte(`{"user_id":1,"recipient_id":1,"amount":50000,"type":"transfer"}`)
		req, _ := http.NewRequest(http.MethodPost, "/transactions", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, resp.Body.String(), "tidak bisa transfer ke diri sendiri")
	})

	t.Run("Error - Invalid JSON Format", func(t *testing.T) {
		body := []byte(`{"user_id":1, "amount":}`) // Malformed JSON
		req, _ := http.NewRequest(http.MethodPost, "/transactions", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusBadRequest, resp.Code)
		assert.Contains(t, resp.Body.String(), "ERR_INVALID_INPUT")
	})

	// Catatan: Skenario happy path (deposit/transfer), breaker reject, dan Kafka error
	// tidak dapat diuji karena memerlukan mocking fungsi global `ExecuteWithBreaker[T]`
	// yang menggunakan type parameter (generics).
}

// ==================== GET USER BALANCE - ADDITIONAL TESTS ====================

func TestTransactionHandler_GetUserBalance_UserNotFound(t *testing.T) {
	router, mockRepo, mr := setupTestServer()
	defer mr.Close()

	// Ganti assert.AnError dengan error yang mengandung "user not found"
	mockRepo.On("GetUserBalance", mock.Anything, int64(999)).Return(nil, errors.New("user not found")).Once()

	req, _ := http.NewRequest(http.MethodGet, "/users/999/balance", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusNotFound, resp.Code)
	assert.Contains(t, resp.Body.String(), "User not found")
	mockRepo.AssertExpectations(t)
}

// ==================== GET TRANSACTION BY ID TESTS ====================

func TestTransactionHandler_GetByTxID_CacheHit(t *testing.T) {
	router, mockRepo, mr := setupTestServer()
	defer mr.Close()

	txID := "tx-123"
	cacheKey := "tx:" + txID

	cachedTx := &domain.TransactionDetail{
		TxID:   txID,
		Status: "pending",
		Amount: 50000,
	}

	err := cache.SetCache(context.Background(), cacheKey, cachedTx, time.Minute)
	assert.NoError(t, err)

	req, _ := http.NewRequest(http.MethodGet, "/transactions/"+txID, nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Contains(t, resp.Body.String(), "pending")
	// Mock repo tidak boleh dipanggil karena cache hit
	mockRepo.AssertNotCalled(t, "GetByTxID")
}

func TestTransactionHandler_GetByTxID_CacheMiss_TransactionFound(t *testing.T) {
	router, mockRepo, mr := setupTestServer()
	defer mr.Close()

	txID := "tx-456"
	expectedTx := &domain.TransactionDetail{
		TxID:   txID,
		Status: "success",
		Amount: 100000,
	}

	mockRepo.On("GetByTxID", mock.Anything, txID).Return(expectedTx, nil).Once()

	req, _ := http.NewRequest(http.MethodGet, "/transactions/"+txID, nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Contains(t, resp.Body.String(), "success")
	mockRepo.AssertExpectations(t)
}

func TestTransactionHandler_GetByTxID_TransactionNotFound(t *testing.T) {
	router, mockRepo, mr := setupTestServer()
	defer mr.Close()

	txID := "tx-notfound"

	// Ganti assert.AnError dengan error yang mengandung "not found"
	mockRepo.On("GetByTxID", mock.Anything, txID).Return(nil, errors.New("transaction not found")).Once()

	req, _ := http.NewRequest(http.MethodGet, "/transactions/"+txID, nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	// Handler mengembalikan 200 dengan status "processing" untuk transaksi yang belum ada
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Contains(t, resp.Body.String(), "processing")
	mockRepo.AssertExpectations(t)
}

// ==================== GET USER TRANSACTIONS - ADDITIONAL TESTS ====================

func TestTransactionHandler_GetUserTransactions_CustomPagination(t *testing.T) {
	router, mockRepo, mr := setupTestServer()
	defer mr.Close()

	expectedTx := []*domain.TransactionDetail{
		{ID: 1, Amount: 100},
	}

	// Custom pagination: limit=5, offset=10
	mockRepo.On("GetUserTransactions", mock.Anything, int64(2), 5, 10).Return(expectedTx, nil).Once()

	req, _ := http.NewRequest(http.MethodGet, "/users/2/transactions?limit=5&offset=10", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
	mockRepo.AssertExpectations(t)
}

func TestTransactionHandler_GetUserTransactions_InvalidPagination(t *testing.T) {
	router, mockRepo, mr := setupTestServer()
	defer mr.Close()

	expectedTx := []*domain.TransactionDetail{} // empty

	// Invalid limit/offset -> fallback ke default (limit=10, offset=0)
	mockRepo.On("GetUserTransactions", mock.Anything, int64(2), 10, 0).Return(expectedTx, nil).Once()

	req, _ := http.NewRequest(http.MethodGet, "/users/2/transactions?limit=-5&offset=abc", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
	mockRepo.AssertExpectations(t)
}
