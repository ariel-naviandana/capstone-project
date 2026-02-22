package domain

import "context"

type TransactionRepository interface {
	Create(ctx context.Context, tx *TransactionCreate) (int64, error)
	GetByTxID(ctx context.Context, txID string) (*TransactionDetail, error)
}
