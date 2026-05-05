package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/domain/mocks"
	"github.com/stretchr/testify/assert"
)

func TestTransactionService_CreateTransaction(t *testing.T) {
	mockRepo := new(mocks.MockTransactionRepository)
	service := NewTransactionService(mockRepo)

	ctx := context.Background()
	input := &domain.TransactionCreate{
		AccountNo: "ACC-1",
		Amount:    50000,
		Type:      "deposit",
	}

	t.Run("Success", func(t *testing.T) {
		mockRepo.On("Create", ctx, input).Return("TRX-1", nil).Once()

		id, err := service.CreateTransaction(ctx, input)

		assert.NoError(t, err)
		assert.Equal(t, "TRX-1", id)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Error", func(t *testing.T) {
		expectedErr := errors.New("database error")
		mockRepo.On("Create", ctx, input).Return("", expectedErr).Once()

		id, err := service.CreateTransaction(ctx, input)

		assert.ErrorIs(t, err, expectedErr)
		assert.Equal(t, "", id)
		mockRepo.AssertExpectations(t)
	})
}

func TestTransactionService_GetByTxID(t *testing.T) {
	mockRepo := new(mocks.MockTransactionRepository)
	service := NewTransactionService(mockRepo)

	ctx := context.Background()
	txID := "tx-123"
	expectedDetail := &domain.TransactionDetail{
		TrxID:     txID,
		AccountNo: "ACC-1",
		Amount:    10000,
		Type:      "deposit",
		Status:    "success",
	}

	t.Run("Success", func(t *testing.T) {
		mockRepo.On("GetByTxID", ctx, txID).Return(expectedDetail, nil).Once()

		detail, err := service.GetByTxID(ctx, txID)

		assert.NoError(t, err)
		assert.Equal(t, expectedDetail, detail)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Not Found", func(t *testing.T) {
		expectedErr := errors.New("not found")
		mockRepo.On("GetByTxID", ctx, txID).Return((*domain.TransactionDetail)(nil), expectedErr).Once()

		detail, err := service.GetByTxID(ctx, txID)

		assert.ErrorIs(t, err, expectedErr)
		assert.Nil(t, detail)
		mockRepo.AssertExpectations(t)
	})
}

func TestTransactionService_GetAccountBalance(t *testing.T) {
	mockRepo := new(mocks.MockTransactionRepository)
	service := NewTransactionService(mockRepo)

	ctx := context.Background()
	accountNo := "ACC-1"
	expectedBalance := &domain.AccountBalance{
		AccountNo: accountNo,
		Balance:   150000,
	}

	t.Run("Success", func(t *testing.T) {
		mockRepo.On("GetAccountBalance", ctx, accountNo).Return(expectedBalance, nil).Once()

		balance, err := service.GetAccountBalance(ctx, accountNo)

		assert.NoError(t, err)
		assert.Equal(t, expectedBalance, balance)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Error", func(t *testing.T) {
		expectedErr := errors.New("user not found")
		mockRepo.On("GetAccountBalance", ctx, accountNo).Return((*domain.AccountBalance)(nil), expectedErr).Once()

		balance, err := service.GetAccountBalance(ctx, accountNo)

		assert.ErrorIs(t, err, expectedErr)
		assert.Nil(t, balance)
		mockRepo.AssertExpectations(t)
	})
}

func TestTransactionService_GetAccountTransactions(t *testing.T) {
	mockRepo := new(mocks.MockTransactionRepository)
	service := NewTransactionService(mockRepo)

	ctx := context.Background()
	accountNo := "ACC-1"
	limit, offset := 10, 0

	expectedTx := []*domain.TransactionDetail{
		{TrxID: "tx-1", AccountNo: accountNo, Amount: 1000, CreatedAt: time.Now()},
		{TrxID: "tx-2", AccountNo: accountNo, Amount: 2000, CreatedAt: time.Now()},
	}

	t.Run("Success", func(t *testing.T) {
		mockRepo.On("GetAccountTransactions", ctx, accountNo, limit, offset).Return(expectedTx, nil).Once()

		transactions, err := service.GetAccountTransactions(ctx, accountNo, limit, offset)

		assert.NoError(t, err)
		assert.Len(t, transactions, 2)
		assert.Equal(t, expectedTx, transactions)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Error", func(t *testing.T) {
		expectedErr := errors.New("db error")
		mockRepo.On("GetAccountTransactions", ctx, accountNo, limit, offset).Return(([]*domain.TransactionDetail)(nil), expectedErr).Once()

		transactions, err := service.GetAccountTransactions(ctx, accountNo, limit, offset)

		assert.ErrorIs(t, err, expectedErr)
		assert.Nil(t, transactions)
		mockRepo.AssertExpectations(t)
	})
}
