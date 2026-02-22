package database

import (
	"context"
	"fmt"

	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/jackc/pgx"
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
		INSERT INTO transactions (
			user_id,
			recipient_id,
			amount,
			type,
			status,
			description,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, 'pending', $5, NOW(), NOW())
		RETURNING id
	`

	// Kalau type bukan transfer, kirim 0 (boleh di DB sekarang)
	recipientID := input.RecipientID
	if input.Type != "transfer" {
		recipientID = 0
	}

	err := r.db.QueryRow(ctx, query,
		input.UserID,
		recipientID,
		input.Amount,
		input.Type,
		input.Description,
	).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("failed to insert transaction: %w", err)
	}

	return id, nil
}

func (r *transactionRepository) GetByTxID(ctx context.Context, txID string) (*domain.TransactionDetail, error) {
	var detail domain.TransactionDetail
	query := `
		SELECT tx_id, id, user_id, recipient_id, amount, type, status, created_at, updated_at
		FROM transactions
		WHERE tx_id = $1
	`
	err := r.db.QueryRow(ctx, query, txID).Scan(
		&detail.TxID, &detail.ID, &detail.UserID, &detail.RecipientID,
		&detail.Amount, &detail.Type, &detail.Status,
		&detail.CreatedAt, &detail.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("transaction not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}
	return &detail, nil
}
