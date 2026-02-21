package database

import (
	"context"
	"fmt"

	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type transactionRepository struct {
	db *pgxpool.Pool
}

func NewTransactionRepository(db *pgxpool.Pool) domain.TransactionRepository {
	return &transactionRepository{db: db}
}

func (r *transactionRepository) Create(ctx context.Context, input *domain.TransactionCreate) (int64, error) {
	var id int64
	query := `
		INSERT INTO transactions (user_id, amount, status, created_at, updated_at)
		VALUES ($1, $2, 'pending', NOW(), NOW())
		RETURNING id
	`

	err := r.db.QueryRow(ctx, query, input.UserID, input.Amount).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to insert transaction: %w", err)
	}

	return id, nil
}
