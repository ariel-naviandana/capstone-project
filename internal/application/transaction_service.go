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

func (s *TransactionService) CreateTransaction(ctx context.Context, input *domain.TransactionCreate) (int64, error) {
	return s.repo.Create(ctx, input)
}

func (s *TransactionService) GetByTxID(ctx context.Context, txID string) (*domain.TransactionDetail, error) {
	return s.repo.GetByTxID(ctx, txID)
}

func (s *TransactionService) GetUserBalance(ctx context.Context, userID int64) (*domain.UserBalance, error) {
	return s.repo.GetUserBalance(ctx, userID)
}

func (s *TransactionService) GetUserTransactions(ctx context.Context, userID int64, limit int, offset int) ([]*domain.TransactionDetail, error) {
	return s.repo.GetUserTransactions(ctx, userID, limit, offset)
}
