package domain

import "context"

type TransactionRepository interface {
	Create(ctx context.Context, tx *TransactionCreate) (int64, error)
	GetByTxID(ctx context.Context, txID string) (*TransactionDetail, error)
	GetUserBalance(ctx context.Context, userID int64) (*UserBalance, error)
	GetUserTransactions(ctx context.Context, userID int64, limit int, offset int) ([]*TransactionDetail, error)
}
