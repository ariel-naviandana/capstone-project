package database

import (
	"context"
	"fmt"

	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/resilience"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
)

type DBQueryInterface interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type transactionRepository struct {
	writeDb DBQueryInterface
	readDb  DBQueryInterface
}

func NewTransactionRepository(writeDb DBQueryInterface, readDb DBQueryInterface) domain.TransactionRepository {
	return &transactionRepository{writeDb: writeDb, readDb: readDb}
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

	recipientID := input.RecipientID
	if input.Type != "transfer" {
		recipientID = 0
	}

	_, err := resilience.ExecuteWithBreaker(ctx, resilience.PostgresBreaker, "PostgresCreateTx", func() (int64, error) {
		err := r.writeDb.QueryRow(ctx, query,
			input.UserID,
			recipientID,
			input.Amount,
			input.Type,
			input.Description,
		).Scan(&id)
		if err != nil {
			return 0, fmt.Errorf("failed to insert transaction (user_id=%d, type=%s): %w", input.UserID, input.Type, err)
		}
		return id, nil
	})
	if err != nil {
		log.Warn().Err(err).Int64("user_id", input.UserID).Str("type", input.Type).Msg("Create transaction failed")
		return 0, err
	}

	return id, nil
}

func (r *transactionRepository) GetByTxID(ctx context.Context, txID string) (*domain.TransactionDetail, error) {
	var detail domain.TransactionDetail
	query := `
		SELECT tx_id, id, user_id, COALESCE(recipient_id, 0) as recipient_id, amount, type, status, created_at, updated_at
		FROM transactions
		WHERE tx_id = $1
	`

	result, err := resilience.ExecuteWithBreaker(ctx, resilience.PostgresBreaker, "PostgresGetByTxID", func() (*domain.TransactionDetail, error) {
		err := r.readDb.QueryRow(ctx, query, txID).Scan(
			&detail.TxID, &detail.ID, &detail.UserID, &detail.RecipientID,
			&detail.Amount, &detail.Type, &detail.Status,
			&detail.CreatedAt, &detail.UpdatedAt,
		)
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("transaction not found for tx_id=%s", txID)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get transaction (tx_id=%s): %w", txID, err)
		}
		return &detail, nil
	})
	if err != nil {
		log.Warn().Err(err).Str("tx_id", txID).Msg("GetByTxID failed")
		return nil, err
	}
	return result, nil
}

func (r *transactionRepository) GetUserBalance(ctx context.Context, userID int64) (*domain.UserBalance, error) {
	var ub domain.UserBalance
	query := `
		SELECT id, username, balance
		FROM users
		WHERE id = $1
	`

	result, err := resilience.ExecuteWithBreaker(ctx, resilience.PostgresBreaker, "PostgresGetUserBalance", func() (*domain.UserBalance, error) {
		err := r.readDb.QueryRow(ctx, query, userID).Scan(&ub.ID, &ub.Username, &ub.Balance)
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("user not found for user_id=%d", userID)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get user balance (user_id=%d): %w", userID, err)
		}
		return &ub, nil
	})
	if err != nil {
		log.Warn().Err(err).Int64("user_id", userID).Msg("GetUserBalance failed")
		return nil, err
	}
	return result, nil
}

func (r *transactionRepository) GetUserTransactions(ctx context.Context, userID int64, limit int, offset int) ([]*domain.TransactionDetail, error) {
	// Manual Sharding Simulation:
	// Jika sistem di-shard, kita dapat memilih db connection berdasarkan userID
	// e.g., shardID := userID % 2
	// dbPool := getShardDB(shardID)
	// Namun di implementasi ini, kita mengambil langsung dari readDb (Replika).

	query := `
		SELECT tx_id, id, user_id, COALESCE(recipient_id, 0) as recipient_id, amount, type, status, created_at, updated_at
		FROM transactions
		WHERE user_id = $1 OR recipient_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	result, err := resilience.ExecuteWithBreaker(ctx, resilience.PostgresBreaker, "PostgresGetUserTransactions", func() ([]*domain.TransactionDetail, error) {
		rows, err := r.readDb.Query(ctx, query, userID, limit, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query get user transactions (user_id=%d): %w", userID, err)
		}
		defer rows.Close()

		var transactions []*domain.TransactionDetail
		for rows.Next() {
			var detail domain.TransactionDetail
			if err := rows.Scan(
				&detail.TxID, &detail.ID, &detail.UserID, &detail.RecipientID,
				&detail.Amount, &detail.Type, &detail.Status,
				&detail.CreatedAt, &detail.UpdatedAt,
			); err != nil {
				return nil, fmt.Errorf("failed to scan transaction row (user_id=%d): %w", userID, err)
			}
			transactions = append(transactions, &detail)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("rows iteration error (user_id=%d): %w", userID, err)
		}

		return transactions, nil
	})

	if err != nil {
		log.Warn().Err(err).Int64("user_id", userID).Msg("GetUserTransactions failed")
		return nil, err
	}

	return result, nil
}

