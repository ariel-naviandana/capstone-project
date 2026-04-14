package mocks

import (
	"context"

	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/stretchr/testify/mock"
)

type MockTransactionRepository struct {
	mock.Mock
}

func (m *MockTransactionRepository) Create(ctx context.Context, tx *domain.TransactionCreate) (int64, error) {
	args := m.Called(ctx, tx)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockTransactionRepository) GetByTxID(ctx context.Context, txID string) (*domain.TransactionDetail, error) {
	args := m.Called(ctx, txID)
	if args.Get(0) != nil {
		return args.Get(0).(*domain.TransactionDetail), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockTransactionRepository) GetUserBalance(ctx context.Context, userID int64) (*domain.UserBalance, error) {
	args := m.Called(ctx, userID)
	if args.Get(0) != nil {
		return args.Get(0).(*domain.UserBalance), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockTransactionRepository) GetUserTransactions(ctx context.Context, userID int64, limit int, offset int) ([]*domain.TransactionDetail, error) {
	args := m.Called(ctx, userID, limit, offset)
	if args.Get(0) != nil {
		return args.Get(0).([]*domain.TransactionDetail), args.Error(1)
	}
	return nil, args.Error(1)
}
