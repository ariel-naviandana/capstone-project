package domain

import "context"

type TransactionRepository interface {
	Create(ctx context.Context, tx *TransactionCreate) (int64, error)
	// Nanti tambah: FindByID, UpdateStatus, dll
}
