package application

import (
	"context"

	"github.com/capstone-b4/capstone-go/internal/domain"
)

type TransactionService struct {
	repo domain.TransactionRepository
}

func NewTransactionService(repo domain.TransactionRepository) *TransactionService {
	return &TransactionService{repo: repo}
}

func (s *TransactionService) CreateTransaction(ctx context.Context, input *domain.TransactionCreate) (string, error) {
	return s.repo.Create(ctx, input)
}

func (s *TransactionService) GetByTxID(ctx context.Context, txID string) (*domain.TransactionDetail, error) {
	return s.repo.GetByTxID(ctx, txID)
}

func (s *TransactionService) GetAccountBalance(ctx context.Context, accountNo string) (*domain.AccountBalance, error) {
	return s.repo.GetAccountBalance(ctx, accountNo)
}
func (s *TransactionService) GetAccountTransactions(ctx context.Context, accountNo string, limit int, offset int) ([]*domain.TransactionDetail, error) {
	return s.repo.GetAccountTransactions(ctx, accountNo, limit, offset)
}