package application

import (
	"context"
	"testing"
	"time"
	"time"

	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockTransactionRepository struct {
	createFunc                  func(ctx context.Context, input *domain.TransactionCreate) (string, error)
	getByTxIDFunc               func(ctx context.Context, txID string) (*domain.TransactionDetail, error)
	getAccountBalanceFunc       func(ctx context.Context, accountNo string) (*domain.AccountBalance, error)
	getAccountTransactionsFunc  func(ctx context.Context, accountNo string, limit int, offset int) ([]*domain.TransactionDetail, error)
}

func (m *mockTransactionRepository) Create(ctx context.Context, input *domain.TransactionCreate) (string, error) {
	return m.createFunc(ctx, input)
}

func (m *mockTransactionRepository) GetByTxID(ctx context.Context, txID string) (*domain.TransactionDetail, error) {
	return m.getByTxIDFunc(ctx, txID)
}

func (m *mockTransactionRepository) GetAccountBalance(ctx context.Context, accountNo string) (*domain.AccountBalance, error) {
	return m.getAccountBalanceFunc(ctx, accountNo)
}

func (m *mockTransactionRepository) GetAccountTransactions(ctx context.Context, accountNo string, limit int, offset int) ([]*domain.TransactionDetail, error) {
	return m.getAccountTransactionsFunc(ctx, accountNo, limit, offset)
}

func TestCreateTransaction(t *testing.T) {
	mockRepo := &mockTransactionRepository{
		createFunc: func(ctx context.Context, input *domain.TransactionCreate) (string, error) {
			return "TRX-20260514-abc123", nil
		},
	}

	service := NewTransactionService(mockRepo)

	ctx := context.Background()
	input := &domain.TransactionCreate{
		AccountNo: "123-456-000001",
		Amount:    100000,
		Type:      "deposit",
		RefNo:     "REF001",
	}

	trxID, err := service.CreateTransaction(ctx, input)

	require.NoError(t, err)
	assert.Equal(t, "TRX-20260514-abc123", trxID)
}

func TestGetByTxID(t *testing.T) {
	mockRepo := &mockTransactionRepository{
		getByTxIDFunc: func(ctx context.Context, txID string) (*domain.TransactionDetail, error) {
			return &domain.TransactionDetail{
				TrxID:       "TRX-20260514-abc123",
				AccountNo:   "123-456-000001",
				Amount:      100000,
				Type:        "deposit",
				Status:      "pending",
				RefNo:       "REF001",
				RecipientNo: "",
				CreatedAt:   time.Now(),
				UpdatedAt:   time.Now(),
			}, nil
		},
	}

	service := NewTransactionService(mockRepo)

	ctx := context.Background()

	detail, err := service.GetByTxID(ctx, "TRX-20260514-abc123")

	require.NoError(t, err)
	assert.Equal(t, "TRX-20260514-abc123", detail.TrxID)
	assert.Equal(t, "123-456-000001", detail.AccountNo)
	assert.Equal(t, 100000.0, detail.Amount)
}

func TestGetAccountBalance(t *testing.T) {
	mockRepo := &mockTransactionRepository{
		getAccountBalanceFunc: func(ctx context.Context, accountNo string) (*domain.AccountBalance, error) {
			return &domain.AccountBalance{
				AccountNo: "123-456-000001",
				Balance:   5000000,
			}, nil
		},
	}

	service := NewTransactionService(mockRepo)

	ctx := context.Background()

	balance, err := service.GetAccountBalance(ctx, "123-456-000001")

	require.NoError(t, err)
	assert.Equal(t, "123-456-000001", balance.AccountNo)
	assert.Equal(t, 5000000.0, balance.Balance)
}

func TestGetAccountTransactions(t *testing.T) {
	mockRepo := &mockTransactionRepository{
		getAccountTransactionsFunc: func(ctx context.Context, accountNo string, limit int, offset int) ([]*domain.TransactionDetail, error) {
			return []*domain.TransactionDetail{
				{
					TrxID:       "TRX-20260514-abc123",
					AccountNo:   "123-456-000001",
					Amount:      100000,
					Type:        "deposit",
					Status:      "completed",
					RefNo:       "REF001",
					RecipientNo: "",
					CreatedAt:   time.Now(),
					UpdatedAt:   time.Now(),
				},
				{
					TrxID:       "TRX-20260513-def456",
					AccountNo:   "123-456-000001",
					Amount:      50000,
					Type:        "withdraw",
					Status:      "completed",
					RefNo:       "REF002",
					RecipientNo: "",
					CreatedAt:   time.Now().Add(-24 * time.Hour),
					UpdatedAt:   time.Now().Add(-24 * time.Hour),
				},
			}, nil
		},
	}

	service := NewTransactionService(mockRepo)

	ctx := context.Background()

	transactions, err := service.GetAccountTransactions(ctx, "123-456-000001", 10, 0)

	require.NoError(t, err)
	assert.Len(t, transactions, 2)
	assert.Equal(t, "TRX-20260514-abc123", transactions[0].TrxID)
	assert.Equal(t, "123-456-000001", transactions[0].AccountNo)
}
