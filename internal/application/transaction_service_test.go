package application

import (
	"context"
	"errors"
	"testing"

	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/domain/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTransactionService_CreateTransaction(t *testing.T) {
	mockRepo := new(mocks.MockTransactionRepository)
	service := NewTransactionService(mockRepo)
	ctx := context.Background()

	input := &domain.TransactionCreate{
		UserID:      1,
		RecipientID: 2,
		Amount:      50000,
		Type:        "transfer",
	}

	t.Run("Success", func(t *testing.T) {
		mockRepo.On("Create", ctx, input).Return(int64(100), nil).Once()

		id, err := service.CreateTransaction(ctx, input)

		assert.NoError(t, err)
		assert.Equal(t, int64(100), id)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Error", func(t *testing.T) {
		mockRepo.On("Create", ctx, input).Return(int64(0), errors.New("db error")).Once()

		id, err := service.CreateTransaction(ctx, input)

		assert.Error(t, err)
		assert.Equal(t, int64(0), id)
		mockRepo.AssertExpectations(t)
	})
}

func TestTransactionService_GetByTxID(t *testing.T) {
	mockRepo := new(mocks.MockTransactionRepository)
	service := NewTransactionService(mockRepo)
	ctx := context.Background()

	txID := "tx-123456"
	expectedDetail := &domain.TransactionDetail{
		ID:     1,
		TxID:   txID,
		Amount: 50000,
	}

	t.Run("Success", func(t *testing.T) {
		mockRepo.On("GetByTxID", ctx, txID).Return(expectedDetail, nil).Once()

		result, err := service.GetByTxID(ctx, txID)

		assert.NoError(t, err)
		assert.Equal(t, expectedDetail, result)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Not Found", func(t *testing.T) {
		mockRepo.On("GetByTxID", ctx, mock.Anything).Return((*domain.TransactionDetail)(nil), errors.New("not found")).Once()

		result, err := service.GetByTxID(ctx, "invalid-tx")

		assert.Error(t, err)
		assert.Nil(t, result)
		mockRepo.AssertExpectations(t)
	})
}

func TestTransactionService_GetUserBalance(t *testing.T) {
	mockRepo := new(mocks.MockTransactionRepository)
	service := NewTransactionService(mockRepo)
	ctx := context.Background()

	userID := int64(1)
	expectedBalance := &domain.UserBalance{
		ID:       1,
		Username: "user1",
		Balance:  100000,
	}

	t.Run("Success", func(t *testing.T) {
		mockRepo.On("GetUserBalance", ctx, userID).Return(expectedBalance, nil).Once()

		result, err := service.GetUserBalance(ctx, userID)

		assert.NoError(t, err)
		assert.Equal(t, expectedBalance, result)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Not Found", func(t *testing.T) {
		mockRepo.On("GetUserBalance", ctx, userID).Return((*domain.UserBalance)(nil), errors.New("not found")).Once()

		result, err := service.GetUserBalance(ctx, userID)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockRepo.AssertExpectations(t)
	})
}

func TestTransactionService_GetUserTransactions(t *testing.T) {
	mockRepo := new(mocks.MockTransactionRepository)
	service := NewTransactionService(mockRepo)
	ctx := context.Background()

	userID := int64(1)
	limit := 10
	offset := 0
	expectedList := []*domain.TransactionDetail{
		{ID: 1, Amount: 50000},
		{ID: 2, Amount: 20000},
	}

	t.Run("Success", func(t *testing.T) {
		mockRepo.On("GetUserTransactions", ctx, userID, limit, offset).Return(expectedList, nil).Once()

		results, err := service.GetUserTransactions(ctx, userID, limit, offset)

		assert.NoError(t, err)
		assert.Len(t, results, 2)
		assert.Equal(t, expectedList, results)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Error", func(t *testing.T) {
		mockRepo.On("GetUserTransactions", ctx, userID, limit, offset).Return(([]*domain.TransactionDetail)(nil), errors.New("db error")).Once()

		results, err := service.GetUserTransactions(ctx, userID, limit, offset)

		assert.Error(t, err)
		assert.Nil(t, results)
		mockRepo.AssertExpectations(t)
	})
}
