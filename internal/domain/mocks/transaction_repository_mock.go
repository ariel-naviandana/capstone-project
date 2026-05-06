package mocks

import (
	"context"

	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/stretchr/testify/mock"
)

type MockTransactionRepository struct {
	mock.Mock
}

func (m *MockTransactionRepository) Create(ctx context.Context, tx *domain.TransactionCreate) (string, error) {
	args := m.Called(ctx, tx)
	return args.String(0), args.Error(1)
}

func (m *MockTransactionRepository) GetByTxID(ctx context.Context, txID string) (*domain.TransactionDetail, error) {
	args := m.Called(ctx, txID)
	if args.Get(0) != nil {
		return args.Get(0).(*domain.TransactionDetail), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockTransactionRepository) GetAccountBalance(ctx context.Context, accountNo string) (*domain.AccountBalance, error) {
	args := m.Called(ctx, accountNo)
	if args.Get(0) != nil {
		return args.Get(0).(*domain.AccountBalance), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockTransactionRepository) GetAccountTransactions(ctx context.Context, accountNo string, limit int, offset int) ([]*domain.TransactionDetail, error) {
	args := m.Called(ctx, accountNo, limit, offset)
	if args.Get(0) != nil {
		return args.Get(0).([]*domain.TransactionDetail), args.Error(1)
	}
	return nil, args.Error(1)
}
