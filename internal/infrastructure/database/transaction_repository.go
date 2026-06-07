package database

import (
	"context"
	"fmt"
	"time"

	"github.com/capstone-b4/capstone-go/internal/domain"
	"github.com/capstone-b4/capstone-go/internal/infrastructure/resilience"
	"github.com/google/uuid"
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
	shards  []DBQueryInterface
}

func NewTransactionRepository(writeDb DBQueryInterface, readDb DBQueryInterface) domain.TransactionRepository {
	shards := make([]DBQueryInterface, 0, len(ShardWritePools))
	for _, pool := range ShardWritePools {
		shards = append(shards, pool)
	}
	if len(shards) == 0 {
		shards = []DBQueryInterface{writeDb, writeDb}
	}
	return &transactionRepository{writeDb: writeDb, readDb: readDb, shards: shards}
}

func (r *transactionRepository) Create(ctx context.Context, input *domain.TransactionCreate) (string, error) {
	trxID := "TRX-" + time.Now().Format("20060102150405") + "-" + uuid.NewString()[:6]

	query := `
		INSERT INTO transactions (
			trx_id,
			account_no,
			recipient_no,
			type,
			amount,
			status,
			ref_no,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, 'pending', $6, NOW(), NOW())
		RETURNING trx_id
	`

	returnedID, err := resilience.ExecuteWithBreaker(ctx, resilience.PostgresBreaker, "PostgresCreateTx", func() (string, error) {
		shard, shardID, shardErr := r.shardForAccount(input.AccountNo)
		if shardErr != nil {
			return "", shardErr
		}

		var returnedTrxID string
		err := shard.QueryRow(ctx, query,
			trxID,
			input.AccountNo,
			input.RecipientNo,
			input.Type,
			input.Amount,
			input.RefNo,
		).Scan(&returnedTrxID)
		if err != nil {
			return "", fmt.Errorf("failed to insert transaction (account_no=%s, type=%s): %w", input.AccountNo, input.Type, err)
		}
		log.Debug().Int("shard_id", shardID).Str("account_no", input.AccountNo).Str("trx_id", returnedTrxID).Msg("Transaction routed to shard")
		return returnedTrxID, nil
	})
	if err != nil {
		log.Warn().Err(err).Str("account_no", input.AccountNo).Str("type", input.Type).Msg("Create transaction failed")
		return "", err
	}
	return returnedID, nil
}

func (r *transactionRepository) GetByTxID(ctx context.Context, txID string) (*domain.TransactionDetail, error) {
	var detail domain.TransactionDetail
	var refNo *string
	var recipientNo *string
	query := `
		SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at
		FROM transactions
		WHERE trx_id = $1
	`

	result, err := resilience.ExecuteWithBreaker(ctx, resilience.PostgresBreaker, "PostgresGetByTxID", func() (*domain.TransactionDetail, error) {
		for shardID, shard := range r.shards {
			detail = domain.TransactionDetail{}
			refNo = nil
			recipientNo = nil
			err := shard.QueryRow(ctx, query, txID).Scan(
				&detail.TrxID, &detail.AccountNo, &recipientNo,
				&detail.Amount, &detail.Type, &detail.Status,
				&refNo, &detail.CreatedAt, &detail.UpdatedAt,
			)
			if err == pgx.ErrNoRows {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("failed to get transaction (trx_id=%s, shard=%d): %w", txID, shardID, err)
			}
			if refNo != nil {
				detail.RefNo = *refNo
			}
			if recipientNo != nil {
				detail.RecipientNo = *recipientNo
			}
			log.Debug().Int("shard_id", shardID).Str("trx_id", txID).Msg("Transaction found in shard")
			return &detail, nil
		}
		return nil, fmt.Errorf("transaction not found for trx_id=%s", txID)
	})
	if err != nil {
		log.Warn().Err(err).Str("trx_id", txID).Msg("GetByTxID failed")
		return nil, err
	}
	return result, nil
}

func (r *transactionRepository) GetAccountBalance(ctx context.Context, accountNo string) (*domain.AccountBalance, error) {
	var ab domain.AccountBalance
	query := `
		SELECT account_no, balance
		FROM accounts
		WHERE account_no = $1
	`

	result, err := resilience.ExecuteWithBreaker(ctx, resilience.PostgresBreaker, "PostgresGetAccountBalance", func() (*domain.AccountBalance, error) {
		err := r.readDb.QueryRow(ctx, query, accountNo).Scan(&ab.AccountNo, &ab.Balance)
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("account not found for account_no=%s", accountNo)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get account balance (account_no=%s): %w", accountNo, err)
		}
		return &ab, nil
	})
	if err != nil {
		log.Warn().Err(err).Str("account_no", accountNo).Msg("GetAccountBalance failed")
		return nil, err
	}
	return result, nil
}

func (r *transactionRepository) GetAccountTransactions(ctx context.Context, accountNo string, limit int, offset int) ([]*domain.TransactionDetail, error) {
	query := `
		SELECT trx_id, account_no, recipient_no, amount, type, status, ref_no, created_at, updated_at
		FROM transactions
		WHERE account_no = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	result, err := resilience.ExecuteWithBreaker(ctx, resilience.PostgresBreaker, "PostgresGetAccountTransactions", func() ([]*domain.TransactionDetail, error) {
		shard, shardID, shardErr := r.shardForAccount(accountNo)
		if shardErr != nil {
			return nil, shardErr
		}

		rows, err := shard.Query(ctx, query, accountNo, limit, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query get account transactions (account_no=%s): %w", accountNo, err)
		}
		defer rows.Close()

		var transactions []*domain.TransactionDetail
		for rows.Next() {
			var detail domain.TransactionDetail
			var refNo *string
			var recipientNo *string
			if err := rows.Scan(
				&detail.TrxID, &detail.AccountNo, &recipientNo,
				&detail.Amount, &detail.Type, &detail.Status,
				&refNo, &detail.CreatedAt, &detail.UpdatedAt,
			); err != nil {
				return nil, fmt.Errorf("failed to scan transaction row (account_no=%s): %w", accountNo, err)
			}
			if recipientNo != nil {
				detail.RecipientNo = *recipientNo
			}
			if refNo != nil {
				detail.RefNo = *refNo
			}
			transactions = append(transactions, &detail)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("rows iteration error (account_no=%s): %w", accountNo, err)
		}

		log.Debug().Int("shard_id", shardID).Str("account_no", accountNo).Int("count", len(transactions)).Msg("Account transactions read from shard")
		return transactions, nil
	})

	if err != nil {
		log.Warn().Err(err).Str("account_no", accountNo).Msg("GetAccountTransactions failed")
		return nil, err
	}

	return result, nil
}

func (r *transactionRepository) shardForAccount(accountNo string) (DBQueryInterface, int, error) {
	shardID, err := ShardIDForAccount(accountNo)
	if err != nil {
		return nil, 0, err
	}
	if shardID >= len(r.shards) || r.shards[shardID] == nil {
		return nil, shardID, fmt.Errorf("transaction shard %d is not initialized", shardID)
	}
	return r.shards[shardID], shardID, nil
}
