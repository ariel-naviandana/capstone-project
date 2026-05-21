package domain

import "context"

type TransactionRepository interface {
	Create(ctx context.Context, tx *TransactionCreate) (string, error)
	GetByTxID(ctx context.Context, txID string) (*TransactionDetail, error)
	GetAccountBalance(ctx context.Context, accountNo string) (*AccountBalance, error)
	GetAccountTransactions(ctx context.Context, accountNo string, limit int, offset int) ([]*TransactionDetail, error)
}